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
 *
 * Facility:
 *
 *  MSFS/bs
 *
 * Abstract:
 *
 *  bs_sbm.c
 *  This module contains routines related to storage bitmap manipulation.
 *
 *
 * Date:
 *
 *  Thu Jul 12 18:29:30 1990
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_sbm.c,v $ $Revision: 1.1.112.3 $ (DEC) $Date: 2008/02/12 13:07:01 $"

#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_stg.h>
#include <msfs/vfast.h>

#define VD_MAX_DESC 50

#define ADVFS_MODULE BS_SBM

/*
 * Local function prototypes.
 */

static
statusT
alloc_bits_page (
                 bfAccessT *sbmbfap,  /* in */
                 uint32T pageOffset,  /* in */
                 int startBit,  /* in */
                 int endBit,  /* in */
                 ftxHT ftx  /* in */
                 );

static
void
dealloc_bits_page (
                   bfAccessT *sbmbfap,  /* in */
                   uint32T pageOffset,  /* in */
                   int startBit,  /* in */
                   int endBit,  /* in */
                   ftxHT ftx  /* in */
                   );

static
stgDescT *
load_x_cache(
             vdT *vd,  /* in */
             uint32T req_clust,  /* in */
             uint32T max_clust, /* in */
             uint32T fillCache, /* in */
             bsAllocHintT alloc_hint, /* in */
             uint64T dstBlkOffset
             );

static
void
remove_desc( 
            vdT *vdp,   /* in */
            stgDescT *stg_desc  /* in */
            );

static
stgDescT *
add_cache (
           vdT *vdp,  /* in */
           const uint32T blk,  /* in */
           const uint32T blks, /* in */
           bsAllocHintT alloc_hint  /* in */
           );

static
void
remove_cache (
              vdT *vdp,  /* in */
              const uint32T clustOffset,  /* in */
              const uint32T clustCnt,  /* in */
              stgDescT *stgDesc,  /* in */
              uint32T flags  /* in */
              );

static statusT 
sbm_verify_xor(bsStgBmT *sbmPagep);  /* in - SBM page to verify */

uint32T
find_bmt_end (
             vdT *vdp    /* in */
             );

#define IS_FREE(bit, wd) (!((wd) & (1 << (bit))))
#define IS_INUSE(bit, wd) (((wd) & (1 << (bit))))

#define ONDISK_CLUST(pg, wd, bit) \
     (((pg*SBM_BITS_PG)+(wd*SBM_BITS_LONG)+bit) )

#define MIG_RSVD_BIT( pg, wd, startBit) \
    (  (mig_rsvdStartClust <= (ONDISK_CLUST(pg,wd,startBit))) && \
       (ONDISK_CLUST(pg,wd,startBit) < mig_rsvdEndClust) )

#define MIG_RSVD_WORD( pg, wd)  \
   (  ( mig_rsvdStartClust <= ONDISK_CLUST(pg,wd,0) && \
        ONDISK_CLUST(pg,wd,0) < mig_rsvdEndClust ) \
       || \
      ( mig_rsvdStartClust <= ONDISK_CLUST(pg,wd,SBM_BITS_LONG) && \
        ONDISK_CLUST(pg,wd,SBM_BITS_LONG) < mig_rsvdEndClust ) )

int AdvfsFixUpSBM = 0;

void CANT_SET_TWICE( 
    unsigned mask,
    unsigned pageOffset,
    unsigned wordOffset,
    bfAccessT *sbmbfap
    )
{
    domainT *dmnP;

    dmnP =  sbmbfap->dmnP;

    printf( "ADVFS INTERNAL ERROR: alloc_bits_page: can't set a bit twice!\n" );

    printf( "ADVFS: dmnId = %08x.%08x, vd = %d, pg = %u, wd = %u, mask = %08x\n",
            dmnP->domainId.tv_sec,
            dmnP->domainId.tv_usec,
            sbmbfap->primVdIndex,
            pageOffset,
            wordOffset,
            mask
          );

    domain_panic( dmnP, "alloc_bits_page: can't set a bit twice!");
}

statusT
CANT_CLEAR_TWICE( 
    unsigned mask,
    unsigned pageOffset,
    unsigned wordOffset,
    bfAccessT *sbmbfap
    )
{
    domainT *dmnP;

    dmnP = sbmbfap->dmnP;

    printf( "ADVFS INTERNAL ERROR: dealloc_bits_page: can't clear a bit twice!\n" );

    printf( "ADVFS: dmnId = %08x.%08x, vd = %d, pg = %u, wd = %u, mask = %08x\n",
            dmnP->domainId.tv_sec,
            dmnP->domainId.tv_usec,
            sbmbfap->primVdIndex,
            pageOffset,
            wordOffset,
            mask
            );

    if (!AdvfsFixUpSBM) {
        set_recovery_failed(dmnP, 1);
        domain_panic(dmnP,
            "dealloc_bits_page: can't clear a bit twice!  dmnId = %08x.%08x, vd = %d, pg = %u, wd = %u, mask = %08x",
            sbmbfap->dmnP->domainId.tv_sec,
            sbmbfap->dmnP->domainId.tv_usec,
            sbmbfap->primVdIndex,
            pageOffset,
            wordOffset,
            mask
        );

        ms_uaprintf("Warning: AdvFS has detected an inconsistency - can't clear a bit twice!:\n");
        ms_uaprintf("AdvFS is attempting to work around this problem but this message may indicate\n");
        ms_uaprintf("metadata inconsistency in the '%s' domain.\n", dmnP->domainName);
        ms_uaprintf("Unmount the domain at the earliest convenient time.  Now invoke\n");
        ms_uaprintf("  dbx -k /vmunix\n");
        ms_uaprintf("to assign AdvfsFixUpSBM the value of 1.\n");
        ms_uaprintf("Exit dbx and run the command:\n");
        ms_uaprintf("  /sbin/advfs/verify %s\n", dmnP->domainName);
        ms_uaprintf("to check the integrity of the domain's metadata.\n\n\n");

        return (E_DOMAIN_PANIC);
    }

    return (EOK);
}


#define DEALLOC_BITS 0
#define ALLOC_BITS 1

typedef struct bitmapUndoRec
{
    int type;  /* ALLOC_BITS, DEALLOC_BITS */
    uint16T vdIndex;
    uint32T pageOffset;
    int startBit;
    int endBit;
} bitmapUndoRecT;

opxT bitmap_undo_opx;


/*
 * bitmap_undo_opx
 *
 * This function is the bitmap code "undo" function.  It is called by ftx
 * when a bitmap transaction must be backed out.  The transactions that were
 * completed before this transaction have not been backed out while the
 * transactions after this transaction have been backed out.
 *
 * This function assumes that no locks are held when it is called.
 */

void
bitmap_undo_opx (
                 ftxHT ftxH,  /* in */
                 int undoRecSz,  /* in */
                 void* undoRec  /* in */
                 )
{

    statusT sts;
    uint32T clustCnt;
    domainT *dmnP;
    vdT *vdp;
    unLkActionT unlock_action;
    bitmapUndoRecT *undoRecp;


    undoRecp = (bitmapUndoRecT *)undoRec;

    dmnP = ftxH.dmnP;
    vdp = VD_HTOP(undoRecp->vdIndex, dmnP);

    clustCnt = (undoRecp->endBit - undoRecp->startBit) + 1;

    FTX_LOCKWRITE( &vdp->stgMap_lk, ftxH )

    /*
     * Note: The vd and domain count adjustments are accurate only during
     * a run-time undo.  During recovery, they have no effect as these
     * fields are not yet initialized.
     */

    switch (undoRecp->type) {

      case ALLOC_BITS:

        dealloc_bits_page (
                           vdp->sbmp, 
                           undoRecp->pageOffset,
                           undoRecp->startBit,
                           undoRecp->endBit,
                           ftxH
                           );

        /*
         * Each bit represents a cluster of blocks.
         */
        vdp->freeClust = vdp->freeClust + clustCnt;

        mutex_lock( &dmnP->mutex );
        UINT64T_ADD (dmnP->freeBlks, clustCnt * vdp->stgCluster);
        mutex_unlock( &dmnP->mutex );

        break;

      case DEALLOC_BITS:

        alloc_bits_page (
                         vdp->sbmp, 
                         undoRecp->pageOffset,
                         undoRecp->startBit,
                         undoRecp->endBit,
                         ftxH
                         );

        /*
         * Each bit represents a cluster of blocks.
         */
        vdp->freeClust = vdp->freeClust - clustCnt;

        mutex_lock( &dmnP->mutex );
        UINT64T_SUB (dmnP->freeBlks, clustCnt * vdp->stgCluster);
        mutex_unlock( &dmnP->mutex );

        break;

      default:
            ADVFS_SAD1 ("bitmap_undo_opx: bad record type", undoRecp->type);

    }

    return;

} /* end bitmap_undo_opx */


opxT bitmap_rtdn_opx;

/*
 * bitmap_rtdn_opx
 *
 * This function is the bitmap code "routine done" function.  It is
 * called by ftx when a transaction is completed.
 */

void
bitmap_rtdn_opx (
                 ftxHT ftxH,  /* in */
                 int rtdnRecSz,  /* in */
                 void* opRec  /* in */
                 )
{
    ADVFS_SAD0("bitmap_rtdn_opx not coded");
}


/*
 * init_bs_bitmap_opx
 *
 * This function registers the bitmap code as a transaction agent.
 */

init_bs_bitmap_opx ()
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_SBM_ALLOC_BITS_V1,
                             &bitmap_undo_opx,
                             &bitmap_rtdn_opx
                             );

    if (sts != EOK) {
        ADVFS_SAD1("init_bs_bitmap_opx:register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_SBM_DEALLOC_BITS_V1,
                             &bitmap_undo_opx,
                             &bitmap_rtdn_opx
                             );

    if (sts != EOK) {
        ADVFS_SAD1("init_bs_bitmap_opx:register failure", sts);
    }

} /* end init_bs_bitmap_opx */


/*
 * sbm_set_pg_bits - Sets the storage bitmap bits that correspond to the
 * clusters "bitOffset" thru "bitOffset+bitCount-1".  The caller must
 * ensure that "bitOffset" thru "bitOffset+bitCount-1" is fully contained
 * within the page "sbmPg" since this routine only modifies one page.
 * The routine is NOT the general "set bitmap bits" routine.  It is
 * meant for disk initialization only.  Use sbm_alloc_bits().
 */
void
sbm_set_pg_bits (
                 int bitOffset,     /* in - bit offset into page */
                 int bitCount,      /* in - number of bits to set */
                 struct bsStgBm* sbmPg  /* in - ptr to sbm buffer */
                 )
{
    int endBit;
    int firstInt;
    int lastInt;
    int wordBit;
    int i;
    const uint32T allSet = 0xffffffff;
    uint32T mask;


    endBit = bitOffset + bitCount - 1;
    firstInt = bitOffset/32;
    lastInt = endBit/32;

    wordBit = firstInt*32;
    mask = allSet<<(bitOffset - wordBit);
    if (firstInt == lastInt) {
        mask &= allSet>>((wordBit + 31) - endBit);
        MS_DBG_ASSERT((sbmPg->mapInt[firstInt] & mask) == 0);
        sbmPg->mapInt[firstInt] |= mask;
        sbmPg->xor ^= mask;
        return;
    }

    MS_DBG_ASSERT((sbmPg->mapInt[firstInt] & mask) == 0);
    sbmPg->mapInt[firstInt] |= mask;
    sbmPg->xor ^= mask;

    for (i = firstInt + 1; i < lastInt; i++) {
        MS_DBG_ASSERT((sbmPg->mapInt[i]) == 0);
        sbmPg->mapInt[i] = allSet;
        sbmPg->xor ^= allSet;
    }
    wordBit = lastInt * 32;  /* first bit of last word */
    mask = allSet>>((wordBit + 31) - endBit);
    MS_DBG_ASSERT((sbmPg->mapInt[lastInt]) == 0);
    sbmPg->mapInt[lastInt] |= mask;
    sbmPg->xor ^= mask;
}


/*
 * sbm_alloc_bits - Allocate the storage bitmap bits that correspond to the
 * clusters "bitOffset" thru "bitOffset+bitCount-1".  
 */
statusT
sbm_alloc_bits (
            vdT *vdp,  /* in */
            int bitOffset,  /* in */
            int bitCount,  /* in */
            ftxHT parentFtx  /* in */
            )
{
    statusT sts;
    uint32T sbm_pg, start_sbm_pg, end_sbm_pg;
    int cur_bit, endBit, bits_to_set, set_bits;
    ftxHT ftx;
    bitmapUndoRecT undoRec;


    undoRec.type = ALLOC_BITS;
    undoRec.vdIndex = vdp->vdIndex;

    start_sbm_pg = bitOffset / SBM_BITS_PG;
    end_sbm_pg   = (bitOffset + (bitCount - 1)) / SBM_BITS_PG;
    cur_bit      = bitOffset % SBM_BITS_PG;
    bits_to_set  = bitCount;
    
    for (sbm_pg = start_sbm_pg; sbm_pg <= end_sbm_pg; sbm_pg++) {

        sts = FTX_START_META (
                          FTA_BS_SBM_ALLOC_BITS_V1,
                          &ftx,
                          parentFtx,
                          vdp->dmnP,
                          1
                          );
        if (sts != EOK) {
            /*
             * FIX - What is the correct action instead of abort?  If we return
             * to our caller, what do we do with bits that have already been set?
             */
            ADVFS_SAD1 ("can't start transaction", sts);
        }

        set_bits = MIN( bits_to_set, SBM_BITS_PG - cur_bit );
        endBit   = cur_bit + set_bits - 1;

        sts = alloc_bits_page (vdp->sbmp, sbm_pg, cur_bit, endBit, ftx);
        if (sts != EOK) { RAISE_EXCEPTION( sts ); }

        undoRec.pageOffset = sbm_pg;
        undoRec.startBit = cur_bit;
        undoRec.endBit = endBit;
        ftx_done_u (
                        ftx,
                        FTA_BS_SBM_ALLOC_BITS_V1,
                        sizeof (undoRec),
                        &undoRec
                        );

        cur_bit = (cur_bit + set_bits) % SBM_BITS_PG;
        bits_to_set -= set_bits;
        
    }

    return EOK;

HANDLE_EXCEPTION:
    ftx_fail(ftx);
    return sts;
}


/*
 * alloc_bits_page - Allocates the storage bitmap bits on the specified page
 * that correspond to the clusters "startBit" thru "endBit".
 */
statusT
alloc_bits_page (
                 bfAccessT *sbmbfap,  /* in */
                 uint32T pageOffset,  /* in */
                 int startBit,  /* in */
                 int endBit,  /* in */
                 ftxHT ftx  /* in */
                 )

{

    statusT sts;
    rbfPgRefHT pgRef;
    bsStgBmT *sbmp;
    int firstInt;
    int lastInt;
    int wordBit;
    uint32T mask;
    const uint32T allSet = 0xffffffff;
    int i;


    sts = rbf_pinpg( &pgRef, (void *) &sbmp, sbmbfap, pageOffset, BS_NIL, ftx );

    if (sts != EOK) {
        domain_panic(sbmbfap->dmnP,"alloc_bits_page: rbf_pinpg on sbm failed");
        sts = E_DOMAIN_PANIC;
        return sts;
    }

    firstInt = startBit/32;
    lastInt  = endBit/32;
    wordBit  = firstInt*32;
    mask     = allSet << (startBit - wordBit);

    rbf_pin_record (
                    pgRef,
                    &(sbmp->mapInt[firstInt]),
                    sizeof (sbmp->mapInt[0]) *
                    (&(sbmp->mapInt[lastInt]) - &(sbmp->mapInt[firstInt]) + 1)
                    );

    rbf_pin_record (pgRef, &(sbmp->xor), sizeof(sbmp->xor));

    if (firstInt == lastInt) {
        mask &= allSet>>((wordBit + 31) - endBit);
        if (sbmp->mapInt[firstInt] & mask) {
            CANT_SET_TWICE( (sbmp->mapInt[firstInt] & mask), pageOffset,
                firstInt, sbmbfap );
        }
        sbmp->mapInt[firstInt] |= mask;
        sbmp->xor ^= mask;
    } else {
        if (sbmp->mapInt[firstInt] & mask) {
            CANT_SET_TWICE( (sbmp->mapInt[firstInt] & mask), pageOffset,
                firstInt, sbmbfap );
        }
        sbmp->mapInt[firstInt] |= mask;
        sbmp->xor ^= mask;
        
        for (i = firstInt + 1; i < lastInt; i++) {
            if (sbmp->mapInt[i] != 0) {
                CANT_SET_TWICE( (sbmp->mapInt[i] & mask), pageOffset, i,
                        sbmbfap );
            }
            sbmp->mapInt[i] = allSet;
            sbmp->xor ^= allSet;
        }
        wordBit = lastInt * 32;  /* first bit of last word */
        mask = allSet>>((wordBit + 31) - endBit);
        if (sbmp->mapInt[lastInt] & mask) {
            CANT_SET_TWICE( (sbmp->mapInt[lastInt] & mask), pageOffset,
                        lastInt, sbmbfap );
        }
        sbmp->mapInt[lastInt] |= mask;
        sbmp->xor ^= mask;
    }

    return EOK;

} /* end alloc_bits_page */


/*
 * dealloc_bits_no_sub_ftx - Deallocates the storage bitmap bits that correspond 
 * to the clusters "bitOffset" thru "bitOffset+bitCount-1". 
 * It and the routines it calls MUST NOT START A SUB TRANSACTION.
 */
static
statusT
dealloc_bits_no_sub_ftx (
              vdT *vdp,  /* in */
              int bitOffset,  /* in */
              int bitCount,  /* in */
              ftxHT parentFtx  /* in */
              )
{
    statusT sts;
    uint32T sbm_pg, start_sbm_pg, end_sbm_pg;
    int cur_bit, endBit, bits_to_clr, clr_bits;


    start_sbm_pg = bitOffset / SBM_BITS_PG;
    end_sbm_pg   = (bitOffset + (bitCount - 1)) / SBM_BITS_PG;
    cur_bit      = bitOffset % SBM_BITS_PG;
    bits_to_clr  = bitCount;
    
    for (sbm_pg = start_sbm_pg; sbm_pg <= end_sbm_pg; sbm_pg++) {
        
        clr_bits = MIN( bits_to_clr, SBM_BITS_PG - cur_bit );
        endBit   = cur_bit + clr_bits - 1;

        dealloc_bits_page (vdp->sbmp, 
                           sbm_pg, 
                           cur_bit, 
                           endBit, 
                           parentFtx);

        cur_bit = (cur_bit + clr_bits) % SBM_BITS_PG;
        bits_to_clr -= clr_bits;
        
    }

    vdp->spaceReturned = 1;
    return EOK;
}


/*
 * dealloc_bits_page - Deallocates the storage bitmap bits on the specified page
 * that correspond to the clusters "startBit" thru "endBit".
 */
static
void
dealloc_bits_page (
                   bfAccessT *sbmbfap,  /* in */
                   uint32T pageOffset,  /* in */
                   int startBit,  /* in */
                   int endBit,  /* in */
                   ftxHT ftx  /* in */
                   )

{

    statusT sts;
    rbfPgRefHT pgRef;
    bsStgBmT *sbmp;
    int firstInt;
    int lastInt;
    int wordBit;
    uint32T mask;
    const uint32T allSet = 0xffffffff;
    int i;


    sts = rbf_pinpg( &pgRef, (void *) &sbmp, sbmbfap, pageOffset, BS_NIL, ftx );

    if (sts != EOK) {
        domain_panic(sbmbfap->dmnP,"dealloc_bits_page: rbf_pinpg on sbm failed");
        return;
    }

    firstInt = startBit/32;
    lastInt  = endBit/32;
    wordBit  = firstInt*32;
    mask     = allSet<<(startBit - wordBit);

    rbf_pin_record (
                    pgRef,
                    &(sbmp->mapInt[firstInt]),
                    sizeof (sbmp->mapInt[0]) *
                    (&(sbmp->mapInt[lastInt]) - &(sbmp->mapInt[firstInt]) + 1)
                    );

    rbf_pin_record (pgRef, &(sbmp->xor), sizeof(sbmp->xor));

    if (firstInt == lastInt) {
        mask &= allSet>>((wordBit + 31) - endBit);
        if ((sbmp->mapInt[firstInt] & mask) != mask) {
            sts = CANT_CLEAR_TWICE( (sbmp->mapInt[firstInt] & mask), pageOffset,
                        firstInt, sbmbfap );
            if (sts == E_DOMAIN_PANIC) {
                return;
            }
        }
        sbmp->mapInt[firstInt] &= ~mask;
        sbmp->xor ^= mask;
    } else {
        if ((sbmp->mapInt[firstInt] & mask) != mask) {
            sts = CANT_CLEAR_TWICE( (sbmp->mapInt[firstInt] & mask), pageOffset,
                        firstInt, sbmbfap );
            if (sts == E_DOMAIN_PANIC) {
                return;
            }
        }
        sbmp->mapInt[firstInt] &= ~mask;
        sbmp->xor ^= mask;

        for (i = firstInt + 1; i < lastInt; i++) {
            if (sbmp->mapInt[i] != allSet) {
                sts = CANT_CLEAR_TWICE( sbmp->mapInt[i], pageOffset, i, sbmbfap );
                if (sts == E_DOMAIN_PANIC) {
                    return;
                }
            }
            sbmp->mapInt[i] = 0;
            sbmp->xor ^= allSet;
        }
        wordBit = lastInt * 32;  /* first bit of last word */
        mask = allSet>>((wordBit + 31) - endBit);
        if ((sbmp->mapInt[lastInt] & mask) != mask) {
            sts = CANT_CLEAR_TWICE( (sbmp->mapInt[lastInt] & mask), pageOffset,
                        lastInt, sbmbfap );
            if (sts == E_DOMAIN_PANIC) {
                return;
            }
        }
        sbmp->mapInt[lastInt] &= ~mask;
        sbmp->xor ^= mask;
    }

    return;

} /* end dealloc_bits_page */


/*
 * sbm_find_space
 *
 * This function now reserves space for reserved files. 
 * When the vdp->freeRsvdStg descriptor is empty, the function
 * calls load_x_cache(), looking for a minimum of one-fifth of
 * the volume.
 *
 * If a non-reserved file needs space, and no free space is left
 * outside of the reserved space, one-half of the reserved space
 * (tail end) is freed up for non-reserved file usage.  
 *
 * (Non-reserved files:)
 * If the request is greater than the AdvfsNotPickyBlkCnt, this routine
 * scans the free space list looking for either a set of contiguous
 * blocks that is at least "requested_blks" long or the largest set
 * of contiguous blocks.  The routine returns a pointer to the 
 * storage descriptor of the set of contiguous blocks found and it
 * returns in "found_blks" either "requested_blks" or the number of
 * blocks in the largest set of contiguous blocks found.  In other words,
 * this routine will attempt to find a set of contiguous blocks that 
 * will fill the request; if it can't find such a set of blocks then
 * it will find the largest set of contiguous blocks.  In the latter
 * case the caller will need to call this routine multiple times to
 * fully allocate (or find) the requested number of blocks (in which
 * case the space will be found in chunks (extents)).  Note that if
 * a suitable set of blocks is not found in the free list then the
 * storage bitmap is used to continue the search.
 *
 * If the request is equal to or less than AdvfsNotPickyBlkCnt, simply
 * take space from the first thing on the extent list cache,
 * regardless of whether it completely satisfies the request or not.
 *
 * After this function returns, the caller can call sbm_remove_space()
 * to allocate the disk space described by the storage descriptor.
 *
 * The allocation of storage is a two step process so that the caller
 * can determine if its requested storage is available before allocating it.
 * If, after calling sbm_find_space(), the caller does not want the space
 * described by the storage descriptor, it does not have to back out
 * the allocation.
 *
 * NOTE:  The pointer returned by sbm_find_space() is not valid 
 * once sbm_remove_space() is called or if the SBM lock is released.
 *
 */

/*
 * Storage allocation requests less than or equal to this limit are
 * considered "not picky" and this determines whether the first
 * storage available is used, or the storage bitmap is exhaustively
 * scanned looking for a best match.
 */

/* The default is 8 pages - it should be less than the preallocation
 * size for large sequential files, otherwise we tend to get extreme
 * fragmentation in certain cases.
 */
#define DEFAULT_NOT_PICKY_BLOCK_COUNT  (8 * ADVFS_PGSZ_IN_BLKS)

uint32T AdvfsNotPickyBlkCnt = DEFAULT_NOT_PICKY_BLOCK_COUNT;
uint32T AdvfsSbmFindSpace;

void *
sbm_find_space (
                vdT *vdp,  /* in */
                uint32T requested_blks,  /* in */
                uint64T dstBlkOffset,  /* in, if alloc_hint==BS_ALLOC_MIG_RSVD */
                bsAllocHintT alloc_hint, /* in */
                uint32T *start_blk,  /* in/out */
                uint32T *found_blks  /* out - always is <= requested_blks */
                )
{
    uint32T requested_clust = howmany( requested_blks, vdp->stgCluster );
    stgDescT *cur_desc;
    uint32T requested_start;
    uint32T num_found_clust, start_found_clust;
    stgDescT *reserved_desc;
    int free_rsvd, fillCache=0;


    ++AdvfsSbmFindSpace;

    *found_blks = 0;
#ifndef ADVFS_RBMT_STRESS

    if (vdp->freeRsvdStg.num_clust == 0) {

        vdp->freeRsvdStg.start_clust = 0;

        /* Reserve 20% of the volume for the BMT */
        free_rsvd = howmany(
                            rounddown(vdp->vdSize/5, vdp->bmtp->bfPageSz),
                            vdp->stgCluster);

        reserved_desc = load_x_cache(
                        vdp,
                        free_rsvd,
                        free_rsvd,
                        fillCache,
                        0,
                        0
                        );

        if (reserved_desc == NULL)  {
            return (NULL);
        }

        MS_SMP_ASSERT(reserved_desc->start_clust < vdp->vdClusters);
        MS_SMP_ASSERT(reserved_desc->start_clust + reserved_desc->num_clust <=
                      vdp->vdClusters);

        vdp->freeRsvdStg.start_clust = reserved_desc->start_clust;
        if (reserved_desc->num_clust > free_rsvd) {
            /*
             * The order of the following two lines
             * (freeRsvdStg assignment and add_cache()) must
             * not be reversed, because add_cache() checks
             * because add_cache() checks the freeRsvdStg
             * descriptor in CHECK_RSVD_OVERLAP.
             * add_cache() is added
             */
            vdp->freeRsvdStg.num_clust = free_rsvd;
            add_cache(vdp, reserved_desc->start_clust + free_rsvd,
                      reserved_desc->num_clust - free_rsvd,
                      alloc_hint);
        } else {
            vdp->freeRsvdStg.num_clust = reserved_desc->num_clust;
        }
        remove_desc (vdp, reserved_desc);
    }

    if (alloc_hint == BS_ALLOC_RSVD) {
        cur_desc = &(vdp->freeRsvdStg);

        if (cur_desc->num_clust >= requested_clust) {
            goto use_cur_desc;
        }
    }
#endif

    if (alloc_hint == BS_ALLOC_MIG_RSVD) {
        /* overRide the reserved migrate portion of the sbm on this vd
         * and return it because this allocation is for moving the
         * file being migrated into the reserved blks.
         */
        cur_desc = load_x_cache(
                              vdp,
                              requested_clust,
                              requested_clust,
                              fillCache,
                              BS_ALLOC_MIG_RSVD,
                              dstBlkOffset
                              );
        if ((cur_desc == NULL) ||
           (dstBlkOffset != (cur_desc->start_clust * vdp->stgCluster)) ||
           ( cur_desc->num_clust < requested_clust )) {
            /* Problem is that the cleared space is not free for some reason,
             * probably because we are running out of space and the system
             * has unlocked the smartstore lock range.  We return a NULL
             * if we haven't got the storage exactly where we wanted it.
             * Caller will handle the condition of not enough of clusts.
             */
            return (NULL);
        }

        /* reduce cur_desc size to requested amount */
        cur_desc->num_clust  = requested_clust;
        SS_TRACE(vdp,dstBlkOffset,requested_clust,
                 cur_desc->start_clust,cur_desc->num_clust);
        goto use_cur_desc;
    }

    cur_desc = vdp->freeStgLst;

    /*
     * An alloc_hint of BS_ALLOC_NFS will attempt to accommodate
     * for the situation of multiple nfsd's growing the same file.
     * The start_blk will be a guess as to the optimal location for
     * this allocation request such that the extent count can be
     * minimized.
     *
     * Must be able to satisfy requested_blks; o.w., will still result
     * in fragmentation.
     *
     * Note: this logic depends on the current implementation which
     * keeps the freestorage list ordered by disk block #.
     */
    if ( cur_desc && alloc_hint == BS_ALLOC_NFS ) {
        requested_start = howmany( *start_blk, vdp->stgCluster );
        do {

            MS_SMP_ASSERT(cur_desc->start_clust < vdp->vdClusters);
            MS_SMP_ASSERT(cur_desc->start_clust + cur_desc->num_clust <=
                          vdp->vdClusters);

            if (requested_start >= cur_desc->start_clust
            && requested_start + requested_clust
                    <= cur_desc->start_clust + cur_desc->num_clust) {
                *start_blk = requested_start * vdp->stgCluster;
                *found_blks = requested_blks;
                return cur_desc;
            }
            cur_desc = cur_desc->nextp;
        } while ( cur_desc != vdp->freeStgLst );
    }        


    /*
     * If this request is less than or equal to the NotPicky block
     * count, simply take the first thing off the list, regardless of
     * its size, if there is any.
     *
     * if alloc_hint indicates a file being migrated clear
     * ignore the AdvfsNotPickyBlkCnt check and try to get enough
     * blks to hold the whole extent, after all, we are trying to
     * reduce extents, not increase them!
     */

    if (( cur_desc && (requested_blks <= AdvfsNotPickyBlkCnt) ) &&
        (alloc_hint != BS_ALLOC_MIG_SINGLEXTNT ) ) {
        goto use_cur_desc;
    }

    /*
     * The free space list contains a cache of descriptors.  Each descriptor
     * describes a set of free, unreserved contiguous clusters on disk.
     *
     * Scan the free list for a set of contiguous clusters that is at
     * least "requested_clust" long or the largest set of contiguous clusters.
     */

    while ((cur_desc != NULL) ) {

        MS_SMP_ASSERT(cur_desc->start_clust < vdp->vdClusters);
        MS_SMP_ASSERT(cur_desc->start_clust + cur_desc->num_clust <=
                      vdp->vdClusters);
        if ( cur_desc->num_clust >= requested_clust ) {
            /*
             * A set of contiguous clusters of at least
             * "requested_clust" long for picky allocators, or
             * exactly request_clust for not picky allocators, was
             * found.
             */
            *start_blk  = cur_desc->start_clust * vdp->stgCluster;
            *found_blks = requested_blks;
            return cur_desc;
        }

        cur_desc = cur_desc->nextp;

        if (cur_desc == vdp->freeStgLst) {
            /*
             * We've scanned the entire free list so quit.
             */
            cur_desc = NULL;
        }
    }

    /*
     * If here, there was either no extents on the extent list
     * already, or we didn't like what was on it.
     *
     * Search the bitmap for a set of contiguous clusters (try to get
     * a contiguous set at least "requested_clust" long).
     *
     * If there is any free, on-disk storage, and we're being picky,
     * the descriptor location returned by load_x_cache() points to
     * the descriptor describing the largest set of contiguous free,
     * unreserved clusters on the free list, which could be one of
     * those already rejected by the scan above.
     *
     * If load_x_cache() returns a NULL descriptor the first time,
     * it could be because it returned free space that overlapped
     * with reserved space (this is checked in add_cache()).
     *
     * So we try again, setting vdp->scanStartClust to the end of
     * the bmt.
     */

    if (alloc_hint == BS_ALLOC_MIG_SINGLEXTNT) {
        fillCache = 0;  /* force attempt to get requested_clust blks */
    } else if ((requested_clust*vdp->stgCluster) <= AdvfsNotPickyBlkCnt ) {
        fillCache = 1;
    }

    cur_desc = load_x_cache(
                              vdp,
                              requested_clust,
                              requested_clust,
                              fillCache,
                              0,
                              0
                              );
    if (cur_desc == NULL) {
        vdp->scanStartClust = vdp->freeRsvdStg.start_clust
                              + vdp->freeRsvdStg.num_clust;

        if ( vdp->scanStartClust >= vdp->vdClusters ) {
            vdp->scanStartClust = 0;
        }

        cur_desc = load_x_cache(
                              vdp,
                              requested_clust,
                              requested_clust,
                              fillCache,
                              0,
                              0
                              );

    }

    if ( alloc_hint == BS_ALLOC_RSVD ) {
        /* For reserved files use the best extent: cur_desc or freeRsvdStg. */
        if ( cur_desc == NULL ) {
            if ( vdp->freeRsvdStg.num_clust > 0 ) {
                cur_desc = &vdp->freeRsvdStg;
            }
        } else if ( vdp->freeRsvdStg.num_clust > cur_desc->num_clust ){
            remove_desc( vdp, cur_desc );
            cur_desc = &vdp->freeRsvdStg;
        }
    }

    /*
     * Return the location of the descriptor which describes the free space.
     *
     * If "cur_desc" is not NULL, it points to a descriptor on the
     * free list that describes a set of contiguous clusters.
     * Note that this set may not be as large as "requested_clust".
     *
     * If "cur_desc" is NULL, then take space from the reserved descriptor.
     */

    if (cur_desc == NULL) {

        if (alloc_hint == BS_ALLOC_RSVD) {
            cur_desc = &(vdp->freeRsvdStg);
            goto use_cur_desc;
        }

        if (requested_clust <= vdp->freeRsvdStg.num_clust) {

            /*
             * If the request is for less than half the space reserved for the
             * BMT then split the BMT space in half to increase the chance that
             * the next request for storage will be contiguous with this one.
             * If the requested size is for more than half the reserved BMT
             * space, give back the requested size.
             */

            num_found_clust = MAX(rounddown(vdp->freeRsvdStg.num_clust / 2,
                                        vdp->bmtp->bfPageSz / vdp->stgCluster ),
                                  requested_clust);
            
            *found_blks = requested_clust * vdp->stgCluster;
        } else {
            num_found_clust = vdp->freeRsvdStg.num_clust;
            *found_blks = num_found_clust * vdp->stgCluster;
        }
            
        vdp->freeRsvdStg.num_clust =
            vdp->freeRsvdStg.num_clust - num_found_clust;

        start_found_clust = (vdp->freeRsvdStg.start_clust +
                             vdp->freeRsvdStg.num_clust);
            
        *start_blk = start_found_clust * vdp->stgCluster;
        
        return add_cache(
                         vdp,
                         start_found_clust,
                         num_found_clust,
                         0
                        );
    }


    if (cur_desc != NULL) {
use_cur_desc:
        MS_SMP_ASSERT(cur_desc->start_clust < vdp->vdClusters);
        MS_SMP_ASSERT(cur_desc->start_clust + cur_desc->num_clust <=
                      vdp->vdClusters);
        *start_blk  = cur_desc->start_clust * vdp->stgCluster;
        if ( cur_desc->num_clust >= requested_clust ) {
            *found_blks = requested_blks;
        } else {
            *found_blks = cur_desc->num_clust * vdp->stgCluster;
        }
    }
    return cur_desc;

}  /* end sbm_find_space */


/*
 * Given a virtual disk, starting block, and maximum number of pages
 * available for pinning:
 *
 * (A) If the proposed blkCount blocks can be freed in the SBM without
 *     exceeding pinPages, then blkCount is unchanged and pinPages is
 *     decremented to reflect the number of pages that would be pinned.
 *
 * (B) If the proposed blkCount is too large, then it is reduced so that
 *     the SBM pages can be freed in the SBM without exceeding pinPages.
 *     In this case, pinPages is not changed.
 *
 * Caution:  this function makes the assumption that exactly one pin
 * is used to free bits in an SBM page.  Thus it has implicit knowledge
 * how the SBM management is implemented.
 */
void
sbm_howmany_blks(
    uint32T blkOffset,
    uint32T *blkCount,          /* in/out , might be reduced */
    int *pinPages,              /* in/out */
    vdT *vdp,
    int pgSz
    )
{
    int firstSbmPage, lastSbmPage;
    int bitOffset;
    int bitCount;
    int sc = vdp->stgCluster;
    int residue;
    int lastPg;
    int bc;

    MS_SMP_ASSERT((*blkCount % pgSz) == 0);

    if (*pinPages == 0) {
        *blkCount = 0;
    }
    if (*blkCount == 0) {
        return;
    }

    bitOffset = blkOffset / sc;
    bitCount = *blkCount / sc;
    /*
     * The interval [firstSbmPage, lastSbmPage] represents the
     * maximum range of SBM pages that may be pinned without
     * exceeding the limit on pinned pages.
     */
    firstSbmPage = bitOffset / SBM_BITS_PG;
    lastSbmPage = firstSbmPage + *pinPages - 1;
    /*
     * The interval [firstSbmPage, lastPg] represents the pages in the
     * SBM having bits we would like to clear.
     */
    lastPg = (bitOffset + bitCount - 1) / SBM_BITS_PG;
    /*
     * If the SBM interval we want to clear is smaller than
     * what we could clear, then the entire SBM interval can be
     * cleared in one transaction.  Decrement pinPages to reflect
     * the number of pages that would be pinned.
     */
    if (lastPg <= lastSbmPage) {
        MS_SMP_ASSERT(*pinPages >= lastPg - firstSbmPage + 1);
        *pinPages -= lastPg - firstSbmPage + 1;
        return;
    }
    /*
     * There are too many SBM bits to be cleared without exceeding
     * the limit on pinned pages.  Indicate that all the available
     * pages will be pinned and reduce the block count.
     */
    *pinPages = 0;      /* Use up all the available pages */

    /*
     * Compute number of bits that could be cleared on first page.
     */
    residue = SBM_BITS_PG - (bitOffset % SBM_BITS_PG);

    /*
     * Compute the total number of bits that could be cleared.
     */
    bc = residue + (lastSbmPage - firstSbmPage) * SBM_BITS_PG;

    MS_SMP_ASSERT(bc <= bitCount);

    /*
     * Make sure that the returned value is an integral number of pages.
     */
    *blkCount = ((bc * sc) / pgSz) * pgSz;
}


/*
 * sbm_remove_space - Removes from the free list "blks" blocks. The blocks
 * are removed from the storage descriptor pointed to by "stg_desc" (which
 * generally was found by sbm_find_space() or sbm_find_space_in_range()).  If
 * all of the blocks in the descriptor are removed then the descriptor is
 * removed from the free list.
 */
statusT
sbm_remove_space ( 
                  vdT *vdp,    /* in */
                  uint32T startBlk,  /* in */
                  uint32T blks,  /* in */
                  stgDescT *stg_desc,  /* in */
                  ftxHT parentFtx,  /* in */
                  uint32T flags     /* in */
                  )
{
    uint32T clust = howmany( blks, vdp->stgCluster );
    uint32T startClust = startBlk / vdp->stgCluster;
    stgDescT *newStgDesc;
    domainT *domain;
    statusT sts;


    /* FIX - Sanity check.  Is this check needed? */
    if ((startClust * vdp->stgCluster) != startBlk) {
        ADVFS_SAD0 ("block offset not aligned on cluster boundary");
    }

    if (stg_desc == NULL) {
        sts = EBAD_STG_DESC;
        goto EXIT_SBM_REMOVE_SPACE;
    }
    if ( (startClust < stg_desc->start_clust) ||
         ((startClust + clust) > (stg_desc->start_clust + stg_desc->num_clust)) ) {
        sts = EBAD_STG_DESC;
        goto EXIT_SBM_REMOVE_SPACE;
    }

    /*
     * Set the corresponding bits in the bitmap and update
     * the total number of free clusters and the counters
     * in the storage descriptor.
     */

    sts = sbm_alloc_bits( vdp, startClust, clust, parentFtx );
    if (sts != EOK) {
      goto EXIT_SBM_REMOVE_SPACE;
    }
    vdp->freeClust -= clust;

    domain = vdp->dmnP;
    mutex_lock (&(domain->mutex));
    UINT64T_SUB (domain->freeBlks, clust * vdp->stgCluster);
    mutex_unlock (&(domain->mutex));

    remove_cache (
                  vdp,
                  startClust,
                  clust,
                  stg_desc,
                  flags
                  );

    sts = EOK;

EXIT_SBM_REMOVE_SPACE:

    return sts;
}


/*
 * sbm_return_space_no_sub_ftx
 *
 * This function returns the specified blocks to the on-disk free space pool.
 * It and the routines it calls MUST NOT START A SUB TRANSACTION.
 */
statusT
sbm_return_space_no_sub_ftx ( 
                  vdT *virtualDiskp,  /* in */
                  uint32T blkOffset,  /* in */
                  uint32T blkCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  )

{
    uint32T clusterCnt;
    uint32T clusterOffset;
    domainT *domain;
    statusT sts;


    clusterCnt = howmany (blkCnt, virtualDiskp->stgCluster);
    clusterOffset = blkOffset / virtualDiskp->stgCluster;

    /*
     * FIX - Sanity check.  Is this check needed?
     */

    if ((clusterOffset * virtualDiskp->stgCluster) != blkOffset) {
        ADVFS_SAD0 ("block offset not aligned on cluster boundary");
    }

    sts = dealloc_bits_no_sub_ftx (
                        virtualDiskp,
                        clusterOffset,
                        clusterCnt,
                        parentFtx
                        );
    if (sts != EOK) {
        return sts;
    }

    virtualDiskp->freeClust = virtualDiskp->freeClust + clusterCnt;

    domain = virtualDiskp->dmnP;
    mutex_lock (&(domain->mutex));
    UINT64T_ADD (domain->freeBlks, clusterCnt * virtualDiskp->stgCluster);
    mutex_unlock (&(domain->mutex));

    return EOK;

} /* sbm_return_space_no_sub_ftx */


/*
 * remove_desc - This function removes the specified descriptor from the
 * space cache.
 */
void
remove_desc ( 
             vdT *vdp,           /* in */
             stgDescT *stg_desc  /* in */
             )
{

    if (stg_desc == &(vdp->freeRsvdStg)) {
        return;
    }

    stg_desc->prevp->nextp = stg_desc->nextp;
    stg_desc->nextp->prevp = stg_desc->prevp;

    if (vdp->freeStgLst == stg_desc) {
        /*
         * The descriptor is first on the list so we need to
         * update the list head pointer.
         */
        if (stg_desc->nextp == stg_desc) {
            /*
             * This is the only descriptor in the list so make the
             * list empty.
             */
            vdp->freeStgLst = NULL;
        } else {
            /*
             * Set the list head pointer to the next descriptor.
             */
            vdp->freeStgLst = stg_desc->nextp;
        }
    }

    ms_free( stg_desc );
    --vdp->numFreeDesc;

    return;

} /* remove_desc */


/*
 * CHECK_MIG_RSVD_OVERLAP
 *
 * This macro is called in add_cache().  It checks the space
 * passed into add_cache(), to see if it overlaps with the
 * freeMigRsvdStg descriptor (reservation for migrating files).
 * If so, it resets start_clust and num_clust to the
 * non-overlapping part, or else returns NULL.
 *
 */

#define CHECK_MIG_RSVD_OVERLAP {                                        \
                                                                        \
    mig_rsvd_end = mig_rsvd_start + mig_rsvd_clust;                     \
    cur_end = start_clust + num_clust;                                  \
    cur_start = start_clust;                                            \
                                                                        \
                                                                        \
    if (cur_start >= mig_rsvd_start) {                                  \
                                                                        \
        if ((cur_start < mig_rsvd_end) && (cur_end > mig_rsvd_end)) {   \
            /* cur_start starts in rsvd range and cur_end ends beyond rsvd range */ \
            start_clust = mig_rsvd_end;                                 \
            num_clust = cur_end - mig_rsvd_end;                         \
                                                                        \
        } else if (cur_end <= mig_rsvd_end) {                           \
            /* cur_start and cur_end begin and end within mig rsvd range */ \
            return NULL;                                                \
        }                                                               \
                                                                        \
    } else {                                                            \
        if ((cur_end > mig_rsvd_start) && (cur_end <= mig_rsvd_end)) {  \
            /* cur_start < mig start and cur_end ends in mig rsvd range */ \
            num_clust = mig_rsvd_start - cur_start;                     \
                                                                        \
        } else if (cur_end > mig_rsvd_end) {                            \
            /* cur_start < mig start and cur_end ends beyond mig rsvd range */ \
            /* chop off extent at boundary */                           \
            num_clust = mig_rsvd_start - cur_start;                     \
        }                                                               \
    }                                                                   \
}

/*
 * CHECK_RSVD_OVERLAP
 *
 * This macro is called in add_cache().  It checks the space
 * passed into add_cache(), to see if it overlaps with the
 * freeRsvdStg descriptor (reservation for reserved files).
 * If so, it resets start_clust and num_clust to the
 * non-overlapping part, or else returns NULL.
 *
 */

#define CHECK_RSVD_OVERLAP {                                    \
                                                                \
    rsvd_end = rsvd_start + rsvd_clust;                         \
    cur_end = start_clust + num_clust;                          \
    cur_start = start_clust;                                    \
                                                                \
                                                                \
    if (cur_start >= rsvd_start) {                              \
                                                                \
        if ((cur_start < rsvd_end) && (cur_end > rsvd_end)) {   \
            start_clust = rsvd_end;                             \
            num_clust = cur_end - rsvd_end;                     \
                                                                \
        } else if (cur_end <= rsvd_end) {                       \
            return NULL;                                        \
        }                                                       \
                                                                \
    } else {                                                    \
        if ((cur_end > rsvd_start) && (cur_end <= rsvd_end)) {  \
            num_clust = rsvd_start - cur_start;                 \
                                                                \
        } else if (cur_end > rsvd_end) {                        \
                                                                \
            num_clust = rsvd_start - cur_start;                 \
            start_clust2 = rsvd_end;                            \
            num_clust2 = cur_end - rsvd_end;                    \
                                                                \
            chunk++;                                            \
                                                                \
        }                                                       \
    }                                                           \
}                                                               \


/*
 * add_cache - This function adds space information into the space
 * cache.
 *
 * A space cache may be limited by the number storage descriptors already
 * in the cache.  If the descriptor limit has been reached, the function 
 * removes a descriptor before adding a new one.
 *
 * This routine simply adds each element to the tail of the list, with
 * no tests for overlap with existing elements.  Hence, the caller
 * must guarantee that elements added to the extent list do not
 * overlap with each other.
 *
 * However, there is a test for overlap with the freeRsvdStg
 * descriptor (reserved space for reserved files).  This descriptor
 * is per-vd and does not reside in the cache.
 *
 * Normally, a pointer to a valid free space cache entry is returned.
 * If something goes wrong (like running out of memory) then NULL is
 * returned.
 */

uint32T AdvfsAddCache = 0;

static stgDescT *
add_cache (
           vdT *vdp,  /* in */
           const uint32T begin_clust,  /* in */
           const uint32T add_num_clust,  /* in */
           bsAllocHintT alloc_hint  /* in */
           )
{
    stgDescT *new_desc;
    stgDescT *last_desc;
    uint32T start_clust = begin_clust, num_clust = add_num_clust;
    uint32T start_clust2, num_clust2;

    uint32T rsvd_start = vdp->freeRsvdStg.start_clust;
    uint32T rsvd_clust = vdp->freeRsvdStg.num_clust;
    uint32T rsvd_end, cur_end, cur_start, mig_rsvd_end;
    uint32T mig_rsvd_start = vdp->freeMigRsvdStg.start_clust;
    uint32T mig_rsvd_clust = vdp->freeMigRsvdStg.num_clust;
    int i, chunk=1;


    ++AdvfsAddCache;

    /*
     * We know that allocations are always done in 8kbyte
     * units, so mask off any odd bits to make sure we don't
     * end up with a useless number of clusters.
     */
    num_clust = ((num_clust*vdp->stgCluster) & ~0xf)/vdp->stgCluster;

    if ( num_clust == 0 ) {
        /* 
         * normally this would be an error.  however, due to a bug
         * in V1.0 we really need to just ignore the odd-size free
         * space chunks on the disk.  they will eventually merge with
         * surrounding space when it is freed.
         */

        return NULL;
    }

    if ( vdp->numFreeDesc >= VD_MAX_DESC ) {
        /*
         * There are too many space descriptors; get rid of  the
         * last one.
         */
        
        remove_desc( vdp, vdp->freeStgLst->prevp );
    }

    CHECK_RSVD_OVERLAP;

    if ( alloc_hint != BS_ALLOC_MIG_RSVD) {
        /* check first chunk */
        CHECK_MIG_RSVD_OVERLAP;
    } else
        chunk = 1; /* vfast's own request - discard second chunk */

    for (i=1; i<=chunk; i++) {

        if (i==2) {
            start_clust = start_clust2;
            num_clust = num_clust2;
            if ( alloc_hint != BS_ALLOC_MIG_RSVD)
                CHECK_MIG_RSVD_OVERLAP; /* check second chunk */
        }

        new_desc = (stgDescT *) ms_malloc( sizeof( stgDescT ) );
        if (new_desc == NULL) {
            return NULL;
        }

        new_desc->start_clust = start_clust;
        new_desc->num_clust  = num_clust;

        if ( vdp->freeStgLst ) {
            /*
             * There is already something on the list, so add to the end
             * of the existing list.
             */
            last_desc = vdp->freeStgLst->prevp;

            new_desc->nextp         = last_desc->nextp;
            last_desc->nextp->prevp = new_desc;
            last_desc->nextp        = new_desc;
            new_desc->prevp         = last_desc;

            (vdp->numFreeDesc)++;

        } else {
            /*
             * The list was empty, so make this the first element on it.
             */
            new_desc->prevp = new_desc;
            new_desc->nextp = new_desc;
            vdp->numFreeDesc = 1;
            vdp->freeStgLst = new_desc;
        }
    }

    return new_desc;

} /* end add_cache */


/*
 * remove_cache
 *
 * This function removes the space information from the specified space
 * cache.  If the storage descriptor describes only the specified info,
 * it removes that descriptor from the cache.  If only part of the
 * info is removed, it simply reduces the count in the descriptor.
 */

uint32T AdvfsRemoveCache;
uint32T AdvfsUpdateCache;

static
void
remove_cache (
              vdT *vdp,  /* in */
              const uint32T clustOffset,  /* in */
              const uint32T clustCnt,  /* in */
              stgDescT *stgDesc,  /* in */
              uint32T flags  /* in */
              )

{
    stgDescT *desc1, *desc2;


    ++AdvfsRemoveCache;

    if (clustOffset == stgDesc->start_clust) {
        stgDesc->start_clust = stgDesc->start_clust + clustCnt;
        if (stgDesc->num_clust >= clustCnt) {
            stgDesc->num_clust = stgDesc->num_clust - clustCnt;
            /* remove the whole descriptor or trim it */
            if (stgDesc->num_clust == 0) {
                /* remove the whole descriptor */
                remove_desc (
                             vdp,
                             stgDesc
                             );
            }
        } else {
            /* remove the whole descriptor */
            remove_desc (
                         vdp,
                         stgDesc
                         );
        }

    /*
     * Handle the case of the allocation request being located at the
     * end of the space described by the storage descriptor.
     */
    } else if ((clustOffset >stgDesc->start_clust) &&
           (clustOffset + clustCnt == stgDesc->start_clust + stgDesc->num_clust)) {
        stgDesc->num_clust -= clustCnt;

    /*
     * For the case of alloc_hint == BS_ALLOC_NFS, the goal is to avoid
     * the excessive fragmentation by guessing the optimal location for
     * an allocation request.  When honoring that request, it's necessary
     * to not needlessly remove the cache storage descriptor describing 
     * clusters in that region of the disk.  Instead, try to replace the
     * used descriptor with 2 descriptors which describe the remaining
     * free clusters.
     *
     * "flags" is currently used only to indicate the request for the
     * behavior of not removing the complete storage descriptor.  This
     * is currently used only when using the BS_ALLOC_NFS allocation
     * algorithm.
     */
    } else if (flags) {
        ++AdvfsUpdateCache;

        /* insert stgDesc for the first part of the replacement descriptor */
        if (desc1 = (stgDescT *) ms_malloc(sizeof(stgDescT))) {
            desc1->start_clust = stgDesc->start_clust;
            desc1->num_clust = clustOffset - stgDesc->start_clust;
            desc1->prevp = stgDesc;
            desc1->nextp = stgDesc->nextp;
            desc1->prevp->nextp = desc1;
            desc1->nextp->prevp = desc1;
            vdp->numFreeDesc++;
        }

        /* insert stgDesc for the second part of the replacement descriptor */
        if (desc1 && (desc2 = (stgDescT *) ms_malloc(sizeof(stgDescT)))) {
            desc2->start_clust = clustOffset + clustCnt;
            desc2->num_clust = stgDesc->num_clust - clustCnt - desc1->num_clust;
            desc2->prevp = desc1;
            desc2->nextp = desc1->nextp;
            desc2->prevp->nextp = desc2;
            desc2->nextp->prevp = desc2;
            vdp->numFreeDesc++;
        }

        remove_desc (vdp, stgDesc);

    } else {
        remove_desc (
                     vdp,
                     stgDesc
                     );
    }

    return;

}  /* end remove_cache */


#define PREV_CLUST(clustOffset) \
    (((clustOffset + vdp->vdClusters) - 1) % vdp->vdClusters)

/*
 * HANDLE_FREE_CLUST -
 *
 * This macro handles the case where the scan rountine found 
 * 'free_clusters' free clusters. 
 */

#define HANDLE_FREE_CLUST( free_clusters ) { \
    if (freeClustCnt == 0) { \
        /* \
         * The previous cluster was not free so save \
         * the current cluster number as the start of \
         * a new set of contiguous free clusters. \
         */ \
        freeClustStart = curClust % vdp->vdClusters; \
    } \
    freeClustCnt += (free_clusters); /* increase size of contig set */ \
    curClust += (free_clusters); \
}

/*
 * HANDLE_INUSE_CLUST -
 *
 * This macro handles the case where the scan routine found 
 * 'inuse_clusters' allocated clusters. 
 */

#define HANDLE_INUSE_CLUST( inuse_clusters ) { \
    if ( freeClustCnt ) { \
        if ( (freeClustCnt >= req_clust) && \
             (!fillCache) && in_use == 0) { \
                in_use = 1;     \
                goto _PAGE_FINISHED;    \
        }       \
        if ( (freeClustCnt >= req_clust) && \
             (freeClustCnt >= max_clust) && \
             (!fillCache) && in_use >= 1) { \
            /* \
             * The previous cluster was free and we are transitioning \
             * to an allocated cluster.  Therefore, this terminates \
             * the current set of contiguous free clusters.  If \
             * the current set fullfills the requested number \
             * of free clusters then we are done. \
             */ \
            goto _PAGE_FINISHED; \
        } \
        if ( (freeClustCnt*vdp->stgCluster) >= 16 ) { \
            if (freeClustCnt > bestFreeClustCnt) { \
                /* \ 
                 * Same comment as above but the current set does not \ 
                 * fullfill the requested number of free clusters.  However, \ 
                 * the current set is bigger than the other sets we've \ 
                 * encountered so save this start cluster number and cluster \ 
                 * count if not filling cache. \
                 */ \
                if ( !fillCache ) { \
                    bestFreeClustCnt = freeClustCnt; \
                    bestFreeClustStart = freeClustStart; \
                } \
            } \
            if ( fillCache ) { \
                if ( vdp->numFreeDesc < VD_MAX_DESC ) { \
                    (void)add_cache( \
                                    vdp, \
                                    freeClustStart, \
                                    freeClustCnt, \
                                    0 \
                                    ); \
                } else { \
                    /* \
                     * The cache is full so force an end of the bitmap scan. \
                     */ \
                    endPageVisits = reqEndPageVisits; \
                    goto _PAGE_FINISHED; \
                } \
            } \
        } \
        freeClustCnt = 0; \
    } \
    in_use++; \
    curClust += (inuse_clusters); \
}


/*
 * load_x_cache - Scans the storage bitmap in a forward direction
 * (ascending cluster numbers), searching for a contiguous set of free
 * clusters that is at least 'req_clust' in length.  If possible,
 * it returns a set that is also at least 'max_clust' in length.
 * If it cannot find such a set at the offset, then it continues
 * looking for a set that is at least 'max_clust' in length.  The set's
 * starting cluster number and number of clusters in the set are returned if
 * such a set of clusters is found; otherwise, the start cluster and
 * cluster count of the largest contiguous set of free blocks is returned.
 */

uint32T AdvfsLoadXCache;

static
stgDescT *
load_x_cache(
             vdT* vdp,          /* in - vd ptr */
             uint32T req_clust, /* in - req contig free clusters */
             uint32T max_clust, /* in - max contig free clusters */
             uint32T fillCache, /* in - flag to fill cache */
             bsAllocHintT alloc_hint,  /* in */
             uint64T dstBlkOffset /* in, used if hint==BS_ALLOC_MIG_RSVD */
             )
{
    bfPageRefHT pgref;
    struct bsStgBm* sbmp; /* pointer to storage bitmap page */
    statusT sts;
    uint32T 
        pg,               /* current bitmap page number */
        wd,               /* current bitmap word number (within cur page) */
        bit,              /* current bitmap bit (within cur word) */
        freeClustCnt = 0,         /* current number of free clusters */
        freeClustStart = 0,       /* current start of free clusters */
        bestFreeClustCnt = 0,     /* best number of free clusters */
        bestFreeClustStart = 0,   /* best start of free clusters */
        end_clust,
        startWd,
        startBit,
        wholePgs,         /* number of whole pages */
        lastPgWds,        /* number of whole words in last page */
        lastPgBits,       /* number of bits in last word */
        curClust,         /* current cluster in storage bitmap */
        bitCnt,
        wordCnt,
        endPage,          /* last page of the search */
        endPageWholeWordCnt,  /* number of whole words in last page of search */
        endPageBitCnt;    /* number of bits in last word of last page of search */
    int endPageVisits,    /* # of times we have examined the last page of search */
        reqEndPageVisits; /* # of times we must examine the last page of search */
    int check_xor_fields; /* TRUE if we should validate the xor field */
    logDescT *ldP;
    int in_use=0;
    uint32T mig_rsvdStartClust = vdp->freeMigRsvdStg.start_clust,
            mig_rsvdEndClust = mig_rsvdStartClust + vdp->freeMigRsvdStg.num_clust;

    /*
     * fillCache:
     * If the request is for less than 32 pages (16 blocks each) then
     * don't be picky about trying to exactly satisfy the request, but
     * fill the extent cache with whatever comes along.
     */

    ++AdvfsLoadXCache;

    MS_SMP_ASSERT(lock_holder(&vdp->stgMap_lk.lock));

    /*
     * The list is simply built in scan order, and the add_cache
     * routine no longer checks for overlap, so it is crucial that any
     * existing elements be tossed before the bitmap is scanned.
     */

    (void)sbm_clear_cache( vdp );

    /*
     * If any blocks have been freed by remove
     * storage, make sure that action is committed in the log.
     */
    if ( vdp->spaceReturned && (ldP = vdp->dmnP->ftxLogP)) {
        lgr_flush(ldP);
        vdp->spaceReturned = 0;
    }

    wholePgs   = vdp->vdClusters / SBM_BITS_PG;
    lastPgWds  = vdp->vdClusters / SBM_BITS_LONG - wholePgs * SBM_LONGS_PG;
    lastPgBits = vdp->vdClusters - wholePgs * SBM_BITS_PG - 
                      lastPgWds * SBM_BITS_LONG;

    if (alloc_hint == BS_ALLOC_MIG_RSVD) {
        curClust = dstBlkOffset / vdp->stgCluster;
    } else {
        curClust = vdp->scanStartClust;
    }

    freeClustStart   = curClust;
    end_clust = PREV_CLUST( curClust );

    /*
     * Figure out where to start the scan.
     */
    pg       = curClust / SBM_BITS_PG;
    startWd  = (curClust % SBM_BITS_PG) / SBM_BITS_LONG;
    startBit = curClust % SBM_BITS_LONG;

    /*
     * Figure out where to end the scan.
     */
    endPage = end_clust / SBM_BITS_PG;
    endPageWholeWordCnt = ((end_clust + 1) / SBM_BITS_LONG) -
      (endPage * SBM_LONGS_PG);
    endPageBitCnt = (end_clust + 1) - (endPage * SBM_BITS_PG) -
      (endPageWholeWordCnt * SBM_BITS_LONG);

    /*
     * Figure out how many times we must examine the last page of the
     * search.
     */
    endPageVisits = 0;
    if (endPage == pg) {
        if (end_clust >= curClust) {  /* curClust still starting clust */
            /*
             * We must examine the clusters from the scan's starting cluster
             * to the scan's ending cluster.
             */
            reqEndPageVisits = 1;
        } else {
            /*
             * We must examine the clusters from the scan's starting cluster
             * to the page's last cluster.  Then, after wrapping and coming
             * to the end page again, we must examine the clusters from the
             * page's first cluster to the scan's ending cluster.
             */
            reqEndPageVisits = 2;
        }
    } else {
        reqEndPageVisits = 1;
    }

    /*
     * If this domain consistently maintains the 'xor' fields at
     * the beginning of the SBM pages, check the validity of 
     * that value, unless AdvfsFixUpSBM is turned on.
     */
    check_xor_fields = (((vdp->sbmp->dmnP->dmnVersion >= 
                        FIRST_VALID_SBM_XOR_VERSION) &&
                         (!AdvfsFixUpSBM)) ? TRUE : FALSE);

    /*
     * Scan the pages.
     */

    while (TRUE) {
        /*
         * Read the page.
         */
        sts = bs_refpg( &pgref, (void *)&sbmp, vdp->sbmp, pg, BS_SEQ_AHEAD );

        if (sts != EOK) {
            domain_panic(vdp->dmnP,"load_x_cache: bad status from bs_refpg of sbm");
            return NULL;
        }
        
        if ((check_xor_fields) && (sbm_verify_xor(sbmp) != EOK)) {
            bs_derefpg(pgref, BS_CACHE_IT);
            domain_panic(vdp->dmnP,
                "Found bad xor in load_x_cache!  Corrupted SBM metadata file!");
            return NULL;
        }

        if ((pg == (vdp->bitMapPgs - 1)) && (wholePgs < vdp->bitMapPgs)) {
            /*
             * Scan the last bitmap page.  We special case it if it does 
             * not have a full set of words.
             *
             * If this page is the last page of the scan, examine at bits
             * up to and including the last bit of the scan.  Otherwise,
             * examine the remaining bits in the page.
             *
             * Process all the whole words in the page.
             */

            wordCnt = lastPgWds;
            bitCnt = lastPgBits;
            if (pg == endPage) {
                ++endPageVisits;
                if (endPageVisits == reqEndPageVisits) {
                    wordCnt = endPageWholeWordCnt;
                    bitCnt = endPageBitCnt;
                }
            }
            
            /* continue using MIG_RSVD_WORD macro so that if
             * a cluster is found that is big enough here, it won't
             * be truncated by add_cache when it is found that it overlaps
             * the reserved migrate space. (in add_cache)
             */
            for (wd = startWd; wd < wordCnt; (wd)++) {
                uint32T curWord = sbmp->mapInt[wd];
                if ( (startBit == 0) && ( curWord == 0)
                   && (( alloc_hint == BS_ALLOC_MIG_RSVD) ||
                       (!(MIG_RSVD_WORD( pg, wd))))) {

                    HANDLE_FREE_CLUST( SBM_BITS_LONG );

                } else if ((startBit == 0) && (curWord == 0xffffffff)) {

                    HANDLE_INUSE_CLUST( SBM_BITS_LONG );

                } else {
                    for (bit = startBit; bit < SBM_BITS_LONG; bit++) {
                        if (IS_FREE( bit, curWord )
                             && (( alloc_hint == BS_ALLOC_MIG_RSVD) ||
                                 (!(MIG_RSVD_BIT( pg, wd, bit))))) {

                            HANDLE_FREE_CLUST( 1 );

                        } else {
                            HANDLE_INUSE_CLUST( 1 );
                        }
                    }
                    startBit = 0;
                }
            }

            /*
             * Process the last word.  Again, it is special cased because it
             * it may not have a full set of bits.
             */

            for (bit = startBit; bit < bitCnt; bit++) {
                if (IS_FREE( bit, sbmp->mapInt[wd] )
                    && (( alloc_hint == BS_ALLOC_MIG_RSVD) ||
                        (!(MIG_RSVD_BIT( pg, wd, bit))))) {

                    HANDLE_FREE_CLUST( 1 );

                } else {
                    HANDLE_INUSE_CLUST( 1 );
                }
            }

            /*
             * We've hit the end of the bitmap.  This is virtually
             * like hitting an inuse cluster, so do that.
             */

            HANDLE_INUSE_CLUST( 0 );

        } else {
            /*
             ** This is not that last bitmap page.
             ** Process all the words in the page.
             */

            wordCnt = SBM_LONGS_PG;
            bitCnt = 0;
            if (pg == endPage) {
                endPageVisits++;
                if (endPageVisits == reqEndPageVisits) {
                    wordCnt = endPageWholeWordCnt;
                    bitCnt = endPageBitCnt;
                }
            }
            
            for (wd = startWd; wd < wordCnt; (wd)++) {
                uint32T curWord = sbmp->mapInt[wd];
                if ( (startBit == 0) && ( curWord == 0)
                     && (!(MIG_RSVD_WORD( pg, wd)))) {

                    HANDLE_FREE_CLUST( SBM_BITS_LONG );

                } else if ((startBit == 0) && (curWord == 0xffffffff)) {

                    HANDLE_INUSE_CLUST( SBM_BITS_LONG );

                } else {
                    for (bit = startBit; bit < SBM_BITS_LONG; bit++) {
                        if (IS_FREE( bit, curWord )
                            && (( alloc_hint == BS_ALLOC_MIG_RSVD) ||
                                (!(MIG_RSVD_BIT( pg, wd, bit)))) ) {

                            HANDLE_FREE_CLUST( 1 );

                        } else {
                            HANDLE_INUSE_CLUST( 1 );
                        }
                    }
                    startBit = 0;
                }
            }

            for (bit = startBit; bit < bitCnt; bit++) {
                if (  IS_FREE( bit, sbmp->mapInt[wd] )
                   && (( alloc_hint == BS_ALLOC_MIG_RSVD) ||
                       (!(MIG_RSVD_BIT( pg, wd, bit)))) ) {

                    HANDLE_FREE_CLUST( 1 );

                } else {
                    HANDLE_INUSE_CLUST( 1 );
                }
            }
        }

_PAGE_FINISHED:
        if (bs_derefpg(pgref, BS_CACHE_IT) != EOK) {
            domain_panic(vdp->dmnP,"load_x_cache: bad status from bs_derefpg on sbm");
            return NULL;
        }

        startWd = 0;
        startBit = 0;

        /*
         * Set the scan start point in case we decide to return
         * without scanning further.
         */

        vdp->scanStartClust = curClust % vdp->vdClusters;

        if ( (freeClustCnt >= req_clust) && !fillCache
             && (freeClustCnt >= max_clust || in_use == 1)) {

            /*
             * The current free cluster is greater than the number of requested
             * clusters.  The free cluster may continue into the next page but
             * we don't care because it satisfies the minimum cluster request.
             */

            return add_cache(
                             vdp,
                             freeClustStart,
                             freeClustCnt,
                             alloc_hint
                             );
        }

        /*
         * If this was a "fillCache" call, we are not picky about
         * what to return, so simply return the first descriptor
         * on the just loaded free list.  This may not have even
         * filled the extent cache.
         */

        if ( fillCache && vdp->freeStgLst ) {
            return vdp->freeStgLst;
        }

        if (endPageVisits == reqEndPageVisits) {
            /*
             * The scan ended without finding a free cluster that is
             * greater than the number of requested clusters if we
             * were being picky (not fillCache), or the scan ended and
             * the last bit examined was free in fillCache mode, so
             * nothing has been added to the extent list cache yet.
             */

            if ( bestFreeClustCnt ) {
                freeClustCnt = bestFreeClustCnt;
                freeClustStart = bestFreeClustStart;
            }

            if ( freeClustCnt ) {
                return add_cache(
                                 vdp,
                                 freeClustStart,
                                 freeClustCnt,
                                 alloc_hint
                                 );
            } else {
                return NULL;
            }
        }

        pg = (pg + 1) % vdp->bitMapPgs; /* go on to the next page */

    } /* end while(TRUE) */

    /** NEVER REACHED **/
}


/*
 * sbm_lock_range -
 *
 * Wrapper to translate blocks to clusters and call sbm_lock_unlock_range.
 *
 * STGMAP lock must be held by caller.
 */
statusT
sbm_lock_range (
          vdT *vdp,           /* in */
          uint32T blkOffset, /* in  */
          uint32T blkCnt    /* in */
          )
{
    statusT sts = EOK;
    uint32T startClust= blkOffset / vdp->stgCluster;
    uint32T numClust = blkCnt / vdp->stgCluster;

    /* lock the sbm location we are migrating src extents into */
    sts = sbm_lock_unlock_range (vdp,
                                 startClust,
                                 numClust );
    if (sts != EOK) {
        return (sts);
    }
    return EOK;
}


/*
 * sbm_lock_unlock_range -
 *   1. Sets or unsets (based on flag) the range to ignore and not
 *      allocate in the sbm for the given vd.
 *      (Only one lock allowed at a time!)
 *   2. Clear the free stg cache of overlapping entries because
 *      it might contain entries in the requested reserved range.
 *
 * SMP
 *  if setting start_clust or num_clust to non-zero values, enter
 *  with vdp->stgMap_lk locked.
 */
statusT
sbm_lock_unlock_range (
          vdT *vdp,           /* in */
          uint32T startClust, /* in -  zero unlocks */
          uint32T numClust    /* in -  zero unlocks */
          )
{
    stgDescT *cur_desc,*next_desc;
    uint64T cur_desc_start=0,cur_desc_end=0;
    uint64T end_clust = startClust + numClust;

    /* lock/unlock(0,0) the migrate reserved range */

    /*
     * If setting this range lock, clear the cache of any entries already
     * in the cache that overlap the new reserved range.
     */
    if (startClust || numClust) {
        MS_SMP_ASSERT(lock_holder(&vdp->stgMap_lk.lock));
        next_desc = cur_desc = vdp->freeStgLst;
        while ((cur_desc != NULL) ) {
            cur_desc_end = cur_desc->start_clust+cur_desc->num_clust;
            cur_desc_start = cur_desc->start_clust;
            next_desc = cur_desc->nextp;
            if ( ((startClust >= cur_desc_start) &&
                 (startClust <  cur_desc_end)) ||
                ((end_clust  >  cur_desc_start) &&
                 (end_clust  <= cur_desc_end)) ||
                ((startClust <= cur_desc_start) &&
                 (end_clust  >  cur_desc_end)) ) {
                remove_cache (
                              vdp,
                              startClust,
                              numClust,
                              cur_desc,
                              0
                              );
            }

            cur_desc = next_desc;

            if ((!vdp->freeStgLst) ||
               (cur_desc == vdp->freeStgLst)){
                cur_desc = NULL;
            }
        }
    }

    vdp->freeMigRsvdStg.start_clust = startClust;
    vdp->freeMigRsvdStg.num_clust = numClust;
    return EOK;
}


/*
 * sbm_init - Initializes the free storage list.
 */
statusT
sbm_init (
          vdT *vdp  /* in */
          )
{
    stgDescT *reserved_desc;
    int free_rsvd;

    /* Not needed since no one races this. Used for assert in load_x_cache. */
    STGMAP_LOCK_WRITE(&vdp->stgMap_lk);

    vdp->freeStgLst = NULL;
    vdp->freeClust = 0;
    vdp->numFreeDesc = 0;
    vdp->bitMapPgs = howmany( vdp->vdClusters, SBM_BITS_PG );
    vdp->freeClust = sbm_total_free_space( vdp ); 
    vdp->spaceReturned = 0;
    vdp->scanStartClust = find_bmt_end(vdp);

    /* The two lines below needed due to checking overlap
        in add_cache() from load_x_cache() */

    vdp->freeRsvdStg.start_clust = 0;
    vdp->freeRsvdStg.num_clust = 0;
    vdp->freeRsvdStg.prevp = &(vdp->freeRsvdStg);
    vdp->freeRsvdStg.nextp = &(vdp->freeRsvdStg);
    vdp->freeMigRsvdStg.start_clust = 0;
    vdp->freeMigRsvdStg.num_clust = 0;

#ifndef ADVFS_RBMT_STRESS
    free_rsvd = howmany
                (vdp->vdSize/ 5 / vdp->bmtp->bfPageSz * vdp->bmtp->bfPageSz,
                 vdp->stgCluster);
#else
    free_rsvd = 0;
#endif

    reserved_desc = load_x_cache(
                        vdp,
                        vdp->bmtp->bfPageSz/vdp->stgCluster,
                        free_rsvd,
                        0,
                        0,
                        0
                        );

    /*
     * If the SBM is invalid, the domain may now be panic'ed.
     */
    if (vdp->dmnP->dmn_panic) {
        STGMAP_UNLOCK(&vdp->stgMap_lk);
        return (E_DOMAIN_PANIC);
    }

    if (reserved_desc != NULL) {
        vdp->freeRsvdStg.start_clust = reserved_desc->start_clust;
        if (reserved_desc->num_clust > free_rsvd) {

            /*
             * The order of the following two lines
             * (freeRsvdStg assignment and add_cache()) must
             * not be reversed, because add_cache() checks the freeRsvdStg
             * descriptor in CHECK_RSVD_OVERLAP.
             * add_cache() is added
             */
            vdp->freeRsvdStg.num_clust = free_rsvd;
            add_cache(vdp, reserved_desc->start_clust + free_rsvd,
                      reserved_desc->num_clust - free_rsvd, BS_ALLOC_DEFAULT);
        } else {
            vdp->freeRsvdStg.num_clust = reserved_desc->num_clust;
        }
        remove_desc (vdp, reserved_desc);
    }

    /*
     * V4 domains and above place their BMT around the middle of the
     * volume for better performance.  V3 domains place their BMT
     * at the beginning of the volume.  Typically, in a single disk
     * volume (i.e., not a RAID device), the best-performing
     * tracks are at the low (outer edge) end of the disk.  So for V3
     * domains, start writing the first bytes of user data right after
     * the BMT.  But for V4 and above, start writing the first bytes of
     * user data at the lowest available blocks, not after the BMT.
     */
    if (RBMT_THERE(vdp->dmnP)) {
        vdp->scanStartClust = 0;
        sbm_clear_cache(vdp);
    }

    STGMAP_UNLOCK(&vdp->stgMap_lk);

    return EOK;
}


/*
 * sbm_clear_cache - Scans through the free and reserved storage list and removes
 * the storage descriptors.
 */
statusT
sbm_clear_cache (
                 vdT *vdp  /* in */
                 )
{
    stgDescT *cur_desc;

    while ( cur_desc = vdp->freeStgLst ) {
        remove_desc(vdp, cur_desc);
    }

    return EOK;
}


/*
 * sbm_total_free_space - Returns the total number of free clusters on 
 * the disk.
 */
uint32T
sbm_total_free_space (
                      vdT *vdp  /* in */
                      )
{
    bfPageRefHT pgref;
    struct bsStgBm* sbmp; /* pointer to storage bitmap page */
    statusT sts;
    int bm_pgs,           /* number of whole pages */
        last_bm_pg_wds,   /* number of words in last page */
        last_bm_wd_bits,  /* number of bits in last word */
        cur_clust = 0;    /* current cluster (bit) in storage bitmap */
    int pg, wd, bit;      /* indices */
    int free_clust = 0;
    int check_xor_fields; /* TRUE if we should validate the xor field */


    bm_pgs          = vdp->vdClusters / SBM_BITS_PG;
    last_bm_pg_wds  = vdp->vdClusters / SBM_BITS_LONG - bm_pgs * SBM_LONGS_PG;
    last_bm_wd_bits = (vdp->vdClusters) - (bm_pgs * SBM_BITS_PG) - 
                      (last_bm_pg_wds * SBM_BITS_LONG);

    /*
     * If this domain consistently maintains the 'xor' fields at
     * the beginning of the SBM pages, check the validity of 
     * that value, unless AdvfsFixUpSBM is turned on.
     */
    check_xor_fields = (((vdp->sbmp->dmnP->dmnVersion >= 
                        FIRST_VALID_SBM_XOR_VERSION) &&
                         (!AdvfsFixUpSBM)) ? TRUE : FALSE);

    /*
     * Process all whole pages.
     */
    for (pg = 0; pg < bm_pgs; pg++) {
        /*
         * Read the page.  If we cannot read a page, panic the domain.
         */
        sts = bs_refpg( &pgref, (void *)&sbmp, vdp->sbmp, pg, BS_SEQ_AHEAD );
        if (sts != EOK) {
            domain_panic(vdp->dmnP,
                     "sbm_total_free_space: bad status from bs_refpg on SBM");
            return 0;
        }

        if ((check_xor_fields) && (sbm_verify_xor(sbmp) != EOK)) {
            bs_derefpg(pgref, BS_CACHE_IT);
            domain_panic(vdp->dmnP,
                "Found bad xor in sbm_total_free_space!  Corrupted SBM metadata file!");
            return 0;
        }

        /*
         * Process all the words in the page.
         */
        for (wd = 0; wd < SBM_LONGS_PG; wd++) {
            if (sbmp->mapInt[wd] == 0xffffffff) {
                cur_clust += SBM_BITS_LONG;
            } else if (sbmp->mapInt[wd] == 0) {
                free_clust += SBM_BITS_LONG;
                cur_clust   += SBM_BITS_LONG;
            } else {
                for (bit = 0; bit < SBM_BITS_LONG; bit++) {
                    if (IS_FREE( bit, sbmp->mapInt[wd] )) {
                        free_clust++;
                    }
                cur_clust++;
                }
            }
        }
        if (bs_derefpg(pgref, BS_CACHE_IT) != EOK) {
            ADVFS_SAD1( "sbm_total_free_space: bad status from bs_derefpg",
                      sts );
        }
    }

    if ((last_bm_pg_wds > 0) || (last_bm_wd_bits > 0)) {
        /*
         * Now process the last page.  We special case it since it may
         * not have a full set of words.  If we can't read it in,
         * panic the domain.
         */
        sts = bs_refpg( &pgref, (void *)&sbmp, vdp->sbmp, pg, BS_NIL );
        if (sts != EOK) {
            domain_panic(vdp->dmnP,
                     "sbm_total_free_space: bad status from bs_refpg on SBM");
            return 0;
        }

        if ((check_xor_fields) && (sbm_verify_xor(sbmp) != EOK)) {
            bs_derefpg(pgref, BS_CACHE_IT);
            domain_panic(vdp->dmnP,
                         "Found bad xor in sbm_total_free_space!  Corrupted SBM metadata file!");
            return 0;
        }

        /*
         * Process all the words in the page except the last word.
         */
        for (wd = 0; wd < last_bm_pg_wds; wd++) {
            if (sbmp->mapInt[wd] == 0xffffffff) {
                cur_clust += SBM_BITS_LONG;
            } else if (sbmp->mapInt[wd] == 0) {
                free_clust += SBM_BITS_LONG;
                cur_clust   += SBM_BITS_LONG;
            } else {
                for (bit = 0; bit < SBM_BITS_LONG; bit++) {
                    if (IS_FREE( bit, sbmp->mapInt[wd] )) {
                        free_clust++;
                    }
                    cur_clust++;
                }
            }
        }
        /*
         * Now process the last word.  Again, it is special cased because it
         * it may not have a full set of bits.
         */
        for (bit = 0; bit < last_bm_wd_bits; bit++) {
            if (IS_FREE( bit, sbmp->mapInt[wd] )) {
                free_clust++;
            }
            cur_clust++;
        }
        if (bs_derefpg(pgref, BS_CACHE_IT) != EOK) {
            ADVFS_SAD1( "sbm_total_free_space: bad status from bs_derefpg", 
                      sts );
        }
    }

    return free_clust;
}


/*
 * sbm_dump - Prints the contents of a disk's free and reserved storage list.
 */
void
sbm_dump (
          vdT *vdp  /* in */
          )
{
    stgDescT *cur_desc = vdp->freeStgLst;
    statusT sts;


    printf( "freeStgLst = 0x%x\n", cur_desc );
    printf( "  freeClust      = %d\n", vdp->freeClust );
    printf( "  bitMapPgs      = %d\n", vdp->bitMapPgs );
    printf( "  numFreeDesc    = %d\n", vdp->numFreeDesc );
    printf( "  scanStartClust = %d\n\n", vdp->scanStartClust );

    while (cur_desc != NULL) {
        printf( "  desc = 0x%x\n", cur_desc );
        printf( "    start_clust = %d\n", cur_desc->start_clust );
        printf( "    num_clust   = %d\n", cur_desc->num_clust );
        printf( "    nextp       = 0x%x\n", cur_desc->nextp );
        printf( "    prevp       = 0x%x\n\n\n", cur_desc->prevp );

        cur_desc = cur_desc->nextp;

        if (cur_desc == vdp->freeStgLst) {
            /*
             * We've scanned the entire free list so quit.
             */
            cur_desc = NULL;
        }
    }

    return;
}

#define ADD_BITS(num_bits) { \
    inuseBitCnt += (num_bits); \
}


/*
 * sbm_scan - Starting at the provided startBlk, scan to the endBlk
 * and return the number of pages in use.
 *
 * caller must ensure block range is != reserved bmt blocks.
 */

statusT
sbm_scan (
           vdT *vdp, /* in */
           uint32T startBlk,  /* in */
           uint32T endBlk,  /* in */
           uint32T *cnt  /* out */
          )
{
    bfPageRefHT pgref;
    struct bsStgBm* sbmp; /* pointer to storage bitmap page */
    statusT sts;
    logDescT *ldP;
    uint32T
        curClust,         /* starting  cluster in range input*/
        end_clust,        /* ending cluster in range input*/
        pg,               /* current bitmap page number */
        wd,               /* current bitmap word number (within cur page) */
        bit,              /* current bitmap bit (within cur word) */
        inuseBitCnt = 0,        /* current number of inuse clusters */
        startWd,          /* starting wd to scan from */
        startBit,         /* starting bit to scan from */
        sbmLastPg,        /* number of whole pages in sbm */
        sbmlastPgWds,     /* number of whole words in last page */
        sbmlastPgBits,       /* number of bits in last word */
        bitCnt,           /* current bitCnt at this page start */
        wordCnt,          /* current word Cnt at this page start */
        rangeEndPage,     /* ending page in range entered */
        rangeEndPageWholeWordCnt,  /* ending page word cnt for range entered*/
        rangeEndWordBitCnt;   /* ending word bit cnt for range entered */

    /*
     * If any blocks have been freed by remove
     * storage, make sure that action is committed in the log.
     */
    if ( vdp->spaceReturned && (ldP = vdp->dmnP->ftxLogP)) {
        lgr_flush(ldP);
        vdp->spaceReturned = 0;
    }

    /*
     * Figure out where to start the scan.
     */
    curClust = startBlk / vdp->stgCluster;
    pg       = curClust / SBM_BITS_PG;
    startWd  = (curClust % SBM_BITS_PG) / SBM_BITS_LONG;
    startBit = curClust % SBM_BITS_LONG;

    /* get the sbm dimensions */
    sbmLastPg   = vdp->vdClusters / SBM_BITS_PG;
    sbmlastPgWds  = vdp->vdClusters / SBM_BITS_LONG - sbmLastPg * SBM_LONGS_PG;
    sbmlastPgBits = vdp->vdClusters - sbmLastPg * SBM_BITS_PG -
                      sbmlastPgWds * SBM_BITS_LONG;

    /*
     * Figure out where to end the scan.
     */

    end_clust = endBlk / vdp->stgCluster;
    rangeEndPage = end_clust / SBM_BITS_PG;
    rangeEndPageWholeWordCnt = (end_clust / SBM_BITS_LONG) -
      (rangeEndPage * SBM_LONGS_PG);
    rangeEndWordBitCnt =  end_clust - (rangeEndPage * SBM_BITS_PG) -
      (rangeEndPageWholeWordCnt * SBM_BITS_LONG);

    /* check for not enough space in sbm for range - if not, return ENO_SPACE */
    if (rangeEndPage > sbmLastPg) {
        return ENOSPC;
    } else if (rangeEndPage == sbmLastPg) {
        if (rangeEndPageWholeWordCnt > sbmlastPgWds) {
            return ENOSPC;
        } else if (rangeEndPageWholeWordCnt == sbmlastPgWds) {
            if (rangeEndWordBitCnt > sbmlastPgBits) {
                return ENOSPC;
            }
        }
    }

    /*
     * Scan the pages up to the end of the sbm or cnt, whichever comes first.
     */

    while (TRUE) {
        /*
         * Read the page.
         */

        sts = bs_refpg( &pgref, (void *)&sbmp, vdp->sbmp, pg, BS_SEQ_AHEAD );
        if (sts != EOK) {
            domain_panic(vdp->dmnP,
                     "sbm_scan: bad status (sts==) from bs_refpg on SBM");
            return E_DOMAIN_PANIC;
        }
        if (pg == rangeEndPage) {
            /*
             * Last range page.  We special case it if it does
             * not have a full set of words. Examine the remaining bits
             * in the page.
             */
                wordCnt = rangeEndPageWholeWordCnt;
                bitCnt = rangeEndWordBitCnt;

        } else {
            /*
             ** It is not the very last page(possibly partial) in the SBM.
             ** Process all the words in the page starting from startWd.
             ** startWd will be set to middle of first page for first time
             ** through.
             */

            wordCnt = SBM_LONGS_PG;
            bitCnt = 0;

        }

        for (wd = startWd; wd < wordCnt; (wd)++) {
            uint32T curWord = sbmp->mapInt[wd];
            if ((startBit == 0) && (curWord == 0xffffffff)) {
                ADD_BITS(SBM_BITS_LONG);
            } else {
                for (bit = startBit; bit < SBM_BITS_LONG; bit++) {
                    if (IS_INUSE( bit, curWord )) {
                        ADD_BITS(1);
                    }
                }
                startBit = 0;
            }
        }

            /*
             * Process the last word.  Again, it is special cased because it
             * it may not have a full set of bits and/or we may have started in it.
             */

        for (bit = startBit; bit < bitCnt; bit++) {
            if (IS_INUSE( bit, sbmp->mapInt[wd] )) {
                ADD_BITS(1);
            }
        }

_PAGE_FINISHED:
        sts = bs_derefpg(pgref, BS_CACHE_IT);
        if (sts != EOK) {
            domain_panic(vdp->dmnP,"sbm_scan: bad status from bs_derefpg on sbm");
            return E_DOMAIN_PANIC;
        }

        /* test for end of scan range OR done with all bitmap pages */
        if ((pg == rangeEndPage) || (pg == (vdp->bitMapPgs - 1))) {
            *cnt = inuseBitCnt;
            return(EOK);
        }

        startWd = 0;
        startBit = 0;
        pg++;     /* go on to the next page */

    } /* end while(TRUE) */

    /** NEVER REACHED **/
}

#define PROCESS_BIT {                                                          \
    if (whoami==SS_CHILD) curr_clust_cnt++;                                     \
                                                                               \
    if (IS_INUSE( bit, sbmp->mapInt[wd] )) {                                    \
        last_bit = 1;                                                          \
        continue;                                                              \
    }                                                                          \
                                                                               \
    cur_clust = ((pg * SBM_BITS_PG)  + (wd * SBM_BITS_LONG) + bit) ;           \
                                                                               \
    if (whoami==SS_PARENT) {                                                    \
                                                                               \
        /* skip to first zero bit after next sequence of 1's */                \
        if (!last_bit) {                                                        \
            last_bit = 0;                                                      \
            continue;                                                          \
        }                                                                      \
        last_bit = 0;                                                          \
                                                                               \
        /* check for current bit overlapping reserved stg */                   \
        if ((cur_clust >= bmt_rsvd_start ) && (cur_clust <= bmt_rsvd_end ))     \
            continue;                                                          \
                                                                               \
        /* call child to scan ahead and determine how many bits                \
         * before enough free bits are found that satisfy reqClustSize         \
         */                                                                    \
        *clustRunCnt = 0;                                                      \
        sts = sbm_scan_v3_v4(vdp,                                              \
                             reqClustSize,                                     \
                             pg,wd,bit,                                        \
                             clustRunCnt,                                      \
                             NULL,                                             \
                             SS_CHILD);                                        \
        if (sts != EOK) {                                                       \
            pg = (vdp->bitMapPgs - 1);                                         \
            goto _PAGE_FINISHED;                                               \
        }                                                                      \
                                                                               \
        if ((*clustRunCnt) &&                                                   \
           ((!bestClustCnt) || (*clustRunCnt <= bestClustCnt))) {              \
            /* This is the shortest range with enough free bits so far,        \
             * save it and keep looking                                        \
             */                                                                \
            bestClustCnt = *clustRunCnt;                                       \
            savedStartClust = cur_clust;                                       \
            sbm_lock_unlock_range (vdp,                                        \
                                   savedStartClust,                            \
                                   bestClustCnt );                             \
            sbm_range_locked = TRUE;                                           \
        }                                                                      \
                                                                               \
                                                                               \
    } else {                                                                   \
        /* SS_CHILD */                                                         \
        /* check for current bit overlapping reserved stg */                   \
        if ((cur_clust >= bmt_rsvd_start ) && (cur_clust < bmt_rsvd_end )) {    \
            /* current bit overlaps reserved, treat as end of a run */         \
            overlaps=TRUE;                                                     \
            pg = (vdp->bitMapPgs - 1);                                         \
            goto _PAGE_FINISHED;   /* fixed here  */                           \
        }                                                                      \
                                                                               \
        free_clust++;                                                          \
                                                                               \
        if (free_clust == reqClustSize) {                                       \
            /* found enough cleared bits;                                      \
             * fake ending to force exit though page deref code,               \
             * even though we are not at end of sbm.                           \
             */                                                                \
            pg = (vdp->bitMapPgs - 1);                                         \
            goto _PAGE_FINISHED;                                               \
        }                                                                      \
    }                                                                          \
}


/*
 * sbm_scan_v3_v4 - Starting at block zero, walk through the sbm
 * and return the fewest number of bits that contain reqClustSize of
 * free bits (0).
 * This is accomplished using one pointer walking through the sbm free bit
 * by free bit, recursively calling itself to have the child run ahead
 * and determine the absolute number of bits that it would take to satisfy
 * the requested number of clusters.  The child call with the fewest
 * absolute bits is the winner and the block offset at that point is
 * returned to the caller.
 *
 * NOTE:that the amount of free space found is always limited to the
 *      amount of free space between the metadata at the start of the
 *      volume and the first reserved bmt storage. -OR- between the last
 *      of the reserved bmt storage and the end of the volume.
 *
 * This is a single level recursive call only!
 * If not enough free blocks in the sbm return ENOSPC.
 *
 * Caller holds stgmap lock.
 */

statusT
sbm_scan_v3_v4 (
       vdT *vdp,             /* in */
       uint64T reqClustSize, /* in */
       uint32T startPg,      /* in */
       uint32T startWd,      /* in */
       uint32T startBit,     /* in */
       uint64T *clustRunCnt,/* out, length in clusters(currently pages) found */
       uint64T *blkOffset,  /* out, location found */
       int whoami           /* in , parent, child */
       )
{
    bfPageRefHT pgref;
    struct bsStgBm* sbmp; /* pointer to storage bitmap page */
    statusT sts;
    logDescT *ldP;
    uint64T
        curr_clust_cnt=0, /* running number of clusters processed */
        free_clust=0,     /* count of the free bits for the curr_clust_cnt */
        pg,               /* current bitmap page number */
        wd,               /* current bitmap word number (within cur page) */
        bit,              /* current bitmap bit (within cur word) */
        sbmLastPg,        /* number of whole pages in sbm */
        sbmLastPgWholeWds,   /* number of whole words in last page */
        sbmLastPgBits,       /* number of bits in last word */
        bitCnt,           /* current bitCnt at this page start */
        wordCnt,          /* current word Cnt at this page start */
        bestClustCnt=0,        /* current least clustRunCnt */
        savedStartClust,  /* current location of bestClustCnt */
        cur_clust;      /* current cluster being processed */
    uint64T bmt_rsvd_start = vdp->freeRsvdStg.start_clust,
            bmt_rsvd_end = vdp->freeRsvdStg.start_clust +
                           vdp->freeRsvdStg.num_clust;
    uint16T last_bit=1;
    extern REPLICATED int SS_is_running;
    int pg_refed = FALSE;
    int sbm_locked = FALSE;
    int sbm_range_locked = FALSE;
    int overlaps=FALSE;  /* flag for when child begins overlapping rsvd storage */
    uint32T jumpPg=0, jumpWd=0, jumpBit=0;

    /* check to see if vd is being deactivated or
     * smartstore is being stopped before parking.
     */
    if ((vdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (vdp->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ||
       (SS_is_running == FALSE)) {
        return(E_WOULD_BLOCK);
    }

    /* FIX - UPDATE to CHECK for rsvd tags in the way when version of sbm is
     * v5.  Return ENOSPC if not enough space because of them.
     */

    if ( whoami == SS_PARENT ) {
        STGMAP_LOCK_WRITE(&vdp->stgMap_lk)   /* protects vdClusters */
        sbm_locked = TRUE;
    }

    /* get the sbm last page dimensions */
    sbmLastPg   = vdp->vdClusters / SBM_BITS_PG;
    sbmLastPgWholeWds  = vdp->vdClusters / SBM_BITS_LONG -
                         sbmLastPg * SBM_LONGS_PG;
    sbmLastPgBits = vdp->vdClusters - sbmLastPg * SBM_BITS_PG -
                      sbmLastPgWholeWds * SBM_BITS_LONG;

    MS_SMP_ASSERT(startPg <= sbmLastPg);
    MS_SMP_ASSERT(startWd <= SBM_LONGS_PG);
    MS_SMP_ASSERT(startBit < SBM_BITS_LONG);

    /*
     * Scan the pages up to the end of the sbm
     */

    for (pg = startPg; pg <= sbmLastPg; ) {

        if ((whoami == SS_PARENT) && (sbm_locked == TRUE)) {
            STGMAP_UNLOCK(&vdp->stgMap_lk)
            sbm_locked = FALSE;
        }

        thread_block(); /* another thread can take away stg at this point */

        if (whoami == SS_PARENT) {
            STGMAP_LOCK_WRITE(&vdp->stgMap_lk)
            sbm_locked = TRUE;
        }

        /* Need to recompute after dropping the lock. vdClusters may change. */
        sbmLastPg   = vdp->vdClusters / SBM_BITS_PG;
        sbmLastPgWholeWds  = vdp->vdClusters / SBM_BITS_LONG -
                             sbmLastPg * SBM_LONGS_PG;
        sbmLastPgBits = vdp->vdClusters - sbmLastPg * SBM_BITS_PG -
                        sbmLastPgWholeWds * SBM_BITS_LONG;

        /*
         * Read the page.
         */
        sts = bs_refpg( &pgref, (void *)&sbmp, vdp->sbmp, pg, BS_SEQ_AHEAD );
        if (sts != EOK) {
            if ((whoami == SS_PARENT) && (sbm_locked == TRUE)) {
                STGMAP_UNLOCK(&vdp->stgMap_lk)
                sbm_locked = FALSE;
            }
            domain_panic(vdp->dmnP,
                 "sbm_scan_v3_v4: bad status (sts==) from bs_refpg on SBM");
            return E_DOMAIN_PANIC;
        }
        pg_refed = TRUE;

        if (pg == sbmLastPg) {
            /*
             * Last range page.  We special case it if it does
             * not have a full set of words. Examine the remaining bits
             * in the page.
             */
                wordCnt = sbmLastPgWholeWds;
                bitCnt = sbmLastPgBits;
        } else {
            /*
             ** It is not the very last page(possibly partial) in the SBM.
             ** Process all the words in the page starting from startWd.
             */

            wordCnt = SBM_LONGS_PG;
            bitCnt = 0;
        }

        /*
         * Process all the words in the page.
         */
        for (wd = startWd; wd < wordCnt; (wd)++) {
            for (bit = startBit; bit < SBM_BITS_LONG; bit++) {

                PROCESS_BIT;

                if (whoami == SS_PARENT) {
                    if (*clustRunCnt != 0) {
                        /* Found a valid run. Jump to the start of the next page */
                        wd=wordCnt;
                        break;
                    } else  /* no Run ended on this page, running out of space */
                    if (bmt_rsvd_end > 0) {  /* still some rsvd space, jump it */

                        /* There is a bmt reserved range in the way. */
                        /* Jump to the start of the first bit after the rsvd range */
                        jumpPg  =  bmt_rsvd_end / SBM_BITS_PG;
                        jumpWd  =  bmt_rsvd_end / SBM_BITS_LONG - jumpPg * SBM_LONGS_PG;
                        jumpBit =  bmt_rsvd_end - jumpPg * SBM_BITS_PG -
                                                   jumpWd * SBM_BITS_LONG;
                        MS_SMP_ASSERT(jumpPg <= sbmLastPg);
                        MS_SMP_ASSERT(jumpWd <= SBM_LONGS_PG);
                        MS_SMP_ASSERT(jumpBit < SBM_BITS_LONG);

                        if (jumpPg > pg) {
                            /* rsvd section ends on next page of sbm - go there */
                            goto _PAGE_FINISHED;
                        } else if ((jumpWd > wd) && (startPg == pg)) {
                            /* Jump to end of rsvd section in current page
                             * if we are not at least at end of rsvd section
                             */
                            wd=jumpWd;
                            bit=jumpBit;
                            jumpPg = jumpWd = jumpBit = 0;
                            break;
                        }
                    }
                }

            } /* end bits in current word */
            startBit = 0;
        }  /* end words in current page */

        /*
         * Process the last word in the current page.  It is special cased
         * because it may not have a full set of bits and/or we may have
         * started in it.
         */

        for (bit = startBit; bit < bitCnt; bit++) {

            PROCESS_BIT;

        }

_PAGE_FINISHED:

        if ((whoami == SS_PARENT) && (sbm_locked == TRUE)) {
            STGMAP_UNLOCK(&vdp->stgMap_lk)
            sbm_locked = FALSE;
        }

        if (pg_refed == TRUE) {
            statusT sts2 = EOK;
            sts2 = bs_derefpg(pgref, BS_CACHE_IT);
            pg_refed = FALSE;
            if (sts2 != EOK) {
                domain_panic(vdp->dmnP,
                         "sbm_scan_v3_v4: bad status from bs_derefpg on sbm");
                return E_DOMAIN_PANIC;
            }
        }

        if ((sts != EOK) && (sts != ENOSPC)) {
            return sts;
        }

        if (vdp->ssVolInfo.ssVdMigState == SS_ABORT) {
            return E_WOULD_BLOCK;
        }

        /* test for done with all bitmap pages */
        if (pg == (vdp->bitMapPgs - 1)) {
            if (whoami == SS_PARENT) {
                if (bestClustCnt) {
                    *clustRunCnt = bestClustCnt;
                    *blkOffset = savedStartClust * vdp->stgCluster;
                    return(EOK);
                } else {
                    /* not enough space was found, return ENOSPC */
                    *clustRunCnt = 0;
                    *blkOffset = 0;
                    return(ENOSPC);
                }
            } else {
                if (free_clust >= reqClustSize) {
                    *clustRunCnt = curr_clust_cnt;
                    return(EOK);
                } else {
                    /* not enough space was found,
                     * return EOK if we need to keep going cause we ran into rsvd,
                     * return ENOSPC if we ran to the end of sbm and don't have enough
                     */
                    *clustRunCnt = 0;
                    if (overlaps == TRUE)
                       sts = EOK;
                    else
                       sts = ENOSPC;
                    return(sts);
                }
            }
        }

        if (jumpPg) {
            pg = (jumpPg ? jumpPg : (pg+1));
            startWd  = (jumpWd ? jumpWd : 0);
            startBit = (jumpBit ? jumpBit : 0);
            jumpPg = jumpWd = jumpBit = 0;
        } else {
            pg++;
            startWd = 0;
            startBit = 0;
        }
     } /* end while pages in sbm */

    /** NEVER REACHED **/
    MS_SMP_ASSERT(0);
    return(ENOSPC);
}


static statusT 
sbm_verify_xor(bsStgBmT *sbmPagep)
{
    uint32T currWd,                 /* Current word we are examining */
            calcXor;                /* Calculated 'xor' value */

    for (currWd = calcXor = 0;
         currWd < SBM_LONGS_PG;
         calcXor ^= sbmPagep->mapInt[currWd], currWd++);
    return (calcXor == sbmPagep->xor ? EOK : EIO);
}

/* find_bmt_end()
 *
 * This routine finds the end of the bmt, and returns the
 * cluster offset.
 */

uint32T
find_bmt_end(vdT *vdp)
{
    uint32T vdBlk, prevpage, lastpage, blk_offset;
    bsInMemXtntMapT *xMap;
    bsInMemSubXtntMapT *subXMap;

    xMap = vdp->bmtp->xtnts.xtntMap;
    MS_SMP_ASSERT(xMap != NULL);
    subXMap = &(xMap->subXtntMap[xMap->cnt-1]);

    vdBlk = subXMap->bsXA[subXMap->cnt-2].vdBlk;
    prevpage = subXMap->bsXA[subXMap->cnt-2].bsPage;
    lastpage = subXMap->bsXA[subXMap->cnt-1].bsPage;

    blk_offset = vdBlk + (lastpage-prevpage)*vdp->bmtp->bfPageSz;
    return howmany( blk_offset, vdp->stgCluster );
}
