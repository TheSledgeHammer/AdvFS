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
 *
 *
 * Facility:
 *
 *      AdvFS
 *
 * Abstract:
 *
 *      Bitfile Set management routines.
 *
 */

#define ADVFS_MODULE BS_BITFILE_SETS

#include <sys/stat.h>
#include <sys/vnode.h>                /* struct vnode */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <ms_osf.h>
#include <bs_msg_queue.h>
#include <bs_extents.h>
#include <tcr/clu.h>
#include <advfs_evm.h>
#include <bs_bitfile_sets.h>
#include <bs_ims.h>
#include <bs_snapshot.h>

#define RBF_PIN_FIELD( h, f ) rbf_pin_record( h, (void*)&(f), (int32_t)sizeof( f ) )

extern int advfs_fstype;

#ifdef SNAPSHOT
/* This is temporary until we get ESS hooks in */
#endif
int advfs_eio_on_bfs_access = 0;

/*
 * Private prototypes
 */
static int64_t
bs_get_avail_mcells(
    domainT *dmnP               /* in - The domain pointer */
    );

static statusT
bs_bfs_find_set(
    char *setName,             /* in - name of set to find */
    domainT *dmnP,             /* in - domain pointer */
    uint64_t doingRoot,        /* in - flag */
    bfSetParamsT *setParams    /* out - the bitfile-set's parameters */
        );


/****************************************************************************
 * bf set mgt
 ****************************************************************************/

/* This is the dynamic hashtable for BfSet access structures */
void * BfSetHashTbl;


opxT bs_bfs_create_undo;
void
bs_bfs_create_undo(
    ftxHT ftxH,      /* in - ftx handle */
    int opRecSz,     /* in - size of opx record */
    void* opRec      /* in - ptr to opx record */
    )
{
   /*
    * There doesn't appear to be anything to do.  The bitfile will
    * be removed by the bf_create undo opx and it is not necessary to
    * erase the fs context record in the mcell.
    */
}

/*
 * bs_bfs_init
 *
 * Initializes bitfile-set global structures.
 */

void
bs_bfs_init(
    void
    )
{
    statusT sts;
    int e;
    void del_list_remove_undo( ftxHT ftxH, int opRecSz, void* opRec );

    sts = ftx_register_agent( FTA_BS_BFS_CREATE_V1, &bs_bfs_create_undo, NIL );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: ftx register agent failed 1", sts );
    }

    sts = ftx_register_agent( FTA_BS_BFS_DEL_LIST_REMOVE_V1,
                              &del_list_remove_undo,
                              NIL );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: ftx register agent failed 3", sts );
    }

    /*
     * Initialize the BfSetHashTbl
     */
    BfSetHashTbl = dyn_hashtable_init(
		       ((uint64_t)BFSET_HASH_INITIAL_SIZE),
                       ((uint64_t)BFSET_HASH_CHAIN_LENGTH),
                       BFSET_HASH_ELEMENTS_TO_BUCKETS,
                       ((uint64_t)BFSET_HASH_USECS_BETWEEN_SPLITS),
                       advfs_offsetof(bfSetT,hashlinks),
                       NULL);
    if (BfSetHashTbl == NULL) {
        ADVFS_SAD0("bs_bfs_init: can't get space for hash table");
    }
}

/*
 * bfs_lookup
 *
 * Return a pointer to the requested bitfile-set's descriptor.
 * Returns NULL if the bitfile-set was not found.
 *
 * SYNCHRONIZATION NOTES
 *
 * Assumes the bitfile-set table mutex is locked by the caller.  This
 * lock synchronizes lookups and inserts/deletes into the table.
 *
 */

bfSetT *
bfs_lookup(
    bfSetIdT bfSetId    /* in - bitfile set's ID */
    )
{
    bfSetT *bfSetp_return = NULL;
    bfSetT *bfSetp;
    bfSetT *bfSetp_start;
    uint64_t key;

    /*
     * We should assert we hold the domain's bitfile-set table lock, but we
     * don't have the domain pointer, so we can't do that.
     */

    /*
     * calculate the hash key using the bitfile-set ID as input to the hash
     * algorithm.
     */
    key = BFSET_GET_HASH_KEY( bfSetId );

    /*
     * Lookup up the bitfile-set ID in the dynamic hash table
     */
    bfSetp_start = BFSET_HASH_LOCK( key, NULL );

    if ( BFSET_VALID(bfSetp_start) ) {
        /*
         * We found a bucket with something in it.  Check each entry
         * chained to this bucket.
         */
        bfSetp = bfSetp_start;
        do {
            if ( BS_BFS_EQL(bfSetp->bfSetId, bfSetId ) ) {
                /*
                 * Bitfile-set was found; return a pointer to it's descriptor.
                 */
                bfSetp_return = bfSetp;
                break;
            }
            bfSetp = bfSetp->hashlinks.dh_links.dh_next;
        } while ( bfSetp != bfSetp_start );
    }

    /*
     * Unlock the bucket.
     */
    BFSET_HASH_UNLOCK( key );

    /*
     * If the fileset ID was not found, then NULL will be returned
     */
    return bfSetp_return;
}


/*
 * bfs_alloc
 *
 * Creates a new bitfile-set descriptor and adds it to the global
 * dynamic hash table (BfSetHashTbl).
 *
 * Assumes the bitfile-set table is locked by the caller.
 *
 * Returns EOK, ENO_MORE_MEMORY or E_TOO_MANY_BF_SETS;
 */
static statusT
bfs_alloc(
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    domainT *dmnP,         /* in - BF-set's domain's struct pointer */
    bfSetT **retBfSetp     /* out - ptr to BF-set's descriptor */
    )
{
    bfSetT *bfSetp;
    statusT sts;

    bfSetp = bfs_lookup( bfSetId );

    if ( BFSET_VALID(bfSetp) ) {

        /*
         * Set the dmnP to the current domain
         */
        MS_SMP_ASSERT( bfSetp->dmnP == dmnP );
        
        BFSET_TRACE( bfSetp, 0 );

        /*
         * This set's descriptor is already cached in the table so
         * reuse the desc.
         */
        *retBfSetp = bfSetp;

        sts = EOK;
        goto EXIT_BFS_ALLOC;
    }


    /* Allocate and zero a bitfile set descriptor. */
    bfSetp = (bfSetT *) ms_malloc( sizeof( bfSetT ) );

    bfSetp->bfSetId    = bfSetId;
    bfSetp->dirTag     = bfSetId.dirTag;
    bfSetp->dmnP       = dmnP;
    bfSetp->state      = BFS_INVALID;
    bfSetp->bfSetMagic = SETMAGIC;
    bfSetp->accessFwd  =
    bfSetp->accessBwd  = (bfAccessT *)(&bfSetp->accessFwd);
    bfSetp->fsnp       = NULL;

    /* Setup snapshot fields */
    bfSetp->bfsParentSnapSet = NULL;
    bfSetp->bfsFirstChildSnapSet = NULL;
    bfSetp->bfsNextSiblingSnapSet = NULL;
    bfSetp->bfsSnapLevel = 0;
    bfSetp->bfsSnapRefs = 0;


    ADVMTX_BFSSET_INIT(&bfSetp->setMutex);
    ADVSMP_SETACCESSCHAIN_INIT(&bfSetp->accessChainLock);
    ADVFTM_SETTAGDIR_INIT(&bfSetp->dirLock, &bfSetp->setMutex);
    
    ADVSMP_BFSET_INIT(&bfSetp->bfSetMutex);

    bfSetp->ssTagArraySz = 0;
    bfSetp->ssTagArray   = NULL;
    ADVSMP_SS_TAG_ARRAY_INIT(&bfSetp->ssTagMutex);

    cv_init(&bfSetp->bfsSnapCv, "AdvFS snap create CV", NULL, CV_WAITOK);

    /*
     * return the bitfile-set structure pointer
     */
    *retBfSetp = bfSetp;

    /*
     * Link the bitfile-set descriptor into the domain's list of
     * bitfile-sets.
     */
    ADVMTX_DOMAIN_LOCK( &bfSetp->dmnP->dmnMutex );
    BFSET_DMN_INSQ(bfSetp->dmnP, &bfSetp->dmnP->bfSetHead, &bfSetp->bfSetList);
    ADVMTX_DOMAIN_UNLOCK( &bfSetp->dmnP->dmnMutex );

    /*
     * Place the new bitfile-set descriptor in the hash table.
     */
    bfSetp->hashlinks.dh_key = BFSET_GET_HASH_KEY( bfSetId );
    BFSET_HASH_INSERT( bfSetp, TRUE );

    sts = EOK;

EXIT_BFS_ALLOC:
    return sts;
}

/*
 * bfs_dealloc
 *
 * Puts the bitfile set's desc on the LRU free list.
 *
 * Assumes the bitfile-set table is locked by the caller.
 */

void
bfs_dealloc(
    bfSetT *bfSetp, /* in - bitfile set descriptor pointer */
    int deallocate  /* in - flag indicates to remove the set from existence */
    )
{

    statusT sts;

    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt == 0 );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&bfSetp->dmnP->BfSetTblLock.lock.rw, 
                                          RWL_UNLOCKED ));

    /*
     * Invalidate all buffers and access structs associated with
     * this set and deallocate the set's desc.
     */

    if (deallocate) {

        /*
         * Flush and invalidate all pages for all files in the fileset.
         * If this is the root bitfile set, do not do the flush.  The
         * metadata is flushed in other paths.
         */
        if (!BS_BFTAG_EQL(bfSetp->dirTag, staticRootTagDirTag)) {
            advfs_bs_bfs_flush(bfSetp, TRUE, FLUSH_NOFLAGS);
        }

        /*
         * Invalidate all access structs associated with this set descriptor.
         */
        access_invalidate( bfSetp );

        /*
         * Let's make sure it did what it's supposed to do.
         */
        MS_SMP_ASSERT(bfSetp->accessFwd ==  (bfAccessT *)(&bfSetp->accessFwd));

        /*
         * Remove this bitfile-set from the list in the domain structure.
         */
        ADVMTX_DOMAIN_LOCK( &bfSetp->dmnP->dmnMutex );
        BFSET_DMN_REMQ( bfSetp->dmnP, &bfSetp->bfSetList );
        ADVMTX_DOMAIN_UNLOCK( &bfSetp->dmnP->dmnMutex );

        /*
         * Remove the bfSetT from the dyn_hashtable.
         */
        BFSET_HASH_REMOVE( bfSetp, TRUE );

        ADVMTX_BFSSET_DESTROY( &bfSetp->setMutex );


        ADVSMP_SETACCESSCHAIN_DESTROY( &bfSetp->accessChainLock );

        ADVSMP_BFSET_DESTROY( &bfSetp->bfSetMutex );
        if (bfSetp->ssTagArraySz > 0) {
            ms_free (bfSetp->ssTagArray);
        }
        ADVSMP_SS_TAG_ARRAY_DESTROY( &bfSetp->ssTagMutex );

        cv_destroy(&bfSetp->bfsSnapCv);

        bfSetp->bfSetMagic |= MAGIC_DEALLOC;
        ms_free( bfSetp );
    }
    return;
}

/*
 * bs_bfs_lookup_desc
 *
 * Return a pointer to the requested bitfile-set descriptor.
 * Returns 0 if the set is not found.
 *
 * RACE CONDITION
 *
 * See bfs_lookup().
 */

bfSetT *
bs_bfs_lookup_desc(
    bfSetIdT bfSetId    /* in - bitfile set's ID */
    )
{
    bfSetT *bfSetp;

    bfSetp = bfs_lookup( bfSetId );

    return bfSetp;
}

/*
 * bfs_create
 *
 * Creates a bitfile-set in the specified domain.  This entails creating
 * the bitfile-set's tag directory and initializing the bitfile-set's
 * client "FS context" area (kept in the tag directories primary mcell).
 *
 * Assumes the bitfile-set table is locked by the caller.
 *
 * Returns EOK, EBAD_DOMAIN_POINTER, and errors from bs_create().
 */

statusT
bfs_create(
    domainT *dmnP,          /* in - domain pointer */
    serviceClassT reqServ,  /* in - required service class */
    char *setName,          /* in - the new set's name */
    uint32_t fsetOptions,   /* in - fileset options */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    bfParamsT *tagDirParamsp = NULL;
    bfTagT dirTag;
    statusT sts;
    bfMCIdT tagDirMCId;
    int ftxStarted = FALSE, tagDirOpen = FALSE;
    bsBfSetAttrT *bfsAttrp = NULL;
    bsQuotaAttrT *quotaAttrp = NULL;
    ftxHT ftxH;
    bfAccessT *dirbfap;
    bfSetParamsT *setParamsp = NULL;
    int i;
    int err;
    struct vnode *nullvp = NULL;
    struct timeval ltime;

    /*
     * Check validity of domain pointer.
     */

    if (dmnP == NULL) {
        RAISE_EXCEPTION( EBAD_DOMAIN_POINTER );
    }

    /*
     * Minimize stack usage.
     * TODO:  We could do one ms_malloc and manage it ourselves.
     */
    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    bfsAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));
    tagDirParamsp = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));
    quotaAttrp = (bsQuotaAttrT *) ms_malloc( sizeof( bsQuotaAttrT ));

    ltime = get_system_time();

    sts = bs_bfs_find_set( setName, dmnP, 0UL, setParamsp );
    if (sts == EOK) {
        RAISE_EXCEPTION( E_DUPLICATE_SET );
    }

    sts = FTX_START_N( FTA_BFS_CREATE, &ftxH, parentFtxH, dmnP );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    ftxStarted = TRUE;

    tagDirParamsp->bfpPageSize = dmnP->metaAllocSz;
    tagDirParamsp->cl.reqServices = reqServ;

    sts = rbf_create( &dirTag, dmnP->bfSetDirp, tagDirParamsp, ftxH, CRT_INTERNAL,
                      BFD_METADATA);
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    bfSetId->dirTag = dirTag;
    bfSetId->domainId = dmnP->domainId;

    sts = tagdir_lookup( dmnP->bfSetDirp,
                         &dirTag,
                         &tagDirMCId
                         );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_create: can't find Tag Dir tag", sts );
    }

    /*
     * Initialize the BF-set attributes in the BF-set's primary mcell.
     */

    sts = bs_access( &dirbfap, dirTag, dmnP->bfSetDirp, ftxH, BF_OP_INTERNAL, 
                     &nullvp );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_create: can't open tag dir", sts );
    }

    tagDirOpen = TRUE;

    bfsAttrp->bfSetId = *bfSetId;
    bfsAttrp->state = BFS_ODS_VALID;
    bfsAttrp->mode = S_IRWXU | S_IRGRP| S_IROTH;

    bfsAttrp->uid = CR_EUID(TNC_CRED());
    bfsAttrp->gid = CR_EGID(TNC_CRED());
    bfsAttrp->flags = BFS_OD_ROOT_SNAPSHOT;

    bfsAttrp->bfsaFilesetCreate = ltime.tv_sec;

    flmemcpy( setName, bfsAttrp->setName, (size_t) BS_SET_NAME_SZ );

    bzero( &bfsAttrp->fsetThreshold, sizeof(adv_threshold_ods_t));

    sts = bmtr_put_rec( dirbfap,
                        BSR_BFS_ATTR,
                        bfsAttrp,
                        (uint16_t) sizeof( bsBfSetAttrT ),
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    /* put a zero-filled quota attributes record in mcell */

    sts = bmtr_put_rec( dirbfap,
                        BSR_BFS_QUOTA_ATTR,
                        quotaAttrp,
                        (uint16_t) sizeof( bsQuotaAttrT ),
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    bs_close(dirbfap, MSFS_CLOSE_NONE);

    ftx_done_n( ftxH, FTA_BFS_CREATE );

    ms_free( quotaAttrp );
    ms_free( tagDirParamsp );
    ms_free( bfsAttrp );
    ms_free( setParamsp );

    return EOK;

HANDLE_EXCEPTION:

    if (tagDirOpen) {
        bs_close(dirbfap, MSFS_CLOSE_NONE);
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (quotaAttrp != NULL) {
        ms_free( quotaAttrp );
    }
    if (tagDirParamsp != NULL) {
        ms_free( tagDirParamsp );
    }
    if (bfsAttrp != NULL) {
        ms_free( bfsAttrp );
    }
    if (setParamsp != NULL) {
        ms_free( setParamsp );
    }

    return sts;
}

/*
 * rbf_bfs_create
 *
 * Higher level interface to bfs_create().  This routine calls
 * bfs_create() to create the bitfile set.
 */

statusT
rbf_bfs_create(
    domainT *dmnP,          /* in - domain pointer */
    serviceClassT reqServ,  /* in - required service class */
    char *setName,          /* in - the new set's name */
    uint32_t fsetOptions,   /* in - fileset options */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    bfSetT *bfSetp = NULL;
    bsBfSetAttrT *setAttrp;
    statusT sts;
    int ftxStarted = FALSE, lkLocked = FALSE;
    ftxHT ftxH;
    rbfPgRefHT pinPgH;
    bfs_op_flags_t bfs_op_flag = BFS_OP_DEF;

    sts = FTX_START_N( FTA_BFS_CREATE_2, &ftxH, parentFtxH, dmnP );
    if ( sts != EOK ) {
        RAISE_EXCEPTION( sts );
    }

    ftxStarted = TRUE;

    ADVRWL_BFSETTBL_WRITE( dmnP );
    lkLocked = TRUE;

    sts = bfs_create( dmnP,
                      reqServ,
                      setName,
                      fsetOptions,
                      ftxH,
                      bfSetId );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bfs_access( &bfSetp, *bfSetId, &bfs_op_flag, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bmtr_get_rec_ptr( bfSetp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            (uint16_t) sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &pinPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(dmnP, "rbf_bfs_create: bmtr_get_rec_ptr failed, return code = %d.", sts );
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    bfs_close( bfSetp, ftxH );
    ADVRWL_BFSETTBL_UNLOCK( dmnP );

    ftx_done_n( ftxH, FTA_BFS_CREATE_2 );

    return EOK;

HANDLE_EXCEPTION:
     
    if (bfSetp) {
        bfs_close( bfSetp, ftxH );
    } 

    /*
     * a transaction failure may release BfSetTblLock
     */
    if (lkLocked && (ADVRWL_BFSETTBL_ISWRLOCKED( dmnP )) ) {
        /*
         * Make sure we don't own it at all if we don't own it for
         * write... If a transaction unlocked it, no one else should have
         * relocked it for read...
         */
        MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&dmnP->BfSetTblLock.lock.rw,
		       RWL_UNLOCKED));
        ADVRWL_BFSETTBL_UNLOCK( dmnP );
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    return sts;
}

/*
 * bfs_close
 *
 * This routine is called to close a bitfile-set.
 *
 * Assumes the bitfile-set table is locked by the caller.
 */

void
bfs_close(
    bfSetT *bfSetp,     /* in - bitfile-set desc pointer */
    ftxHT   ftxH	/* in - transaction handle */
    )
{
    domainT *dmnP = bfSetp->dmnP;
        
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    bfSetp->fsRefCnt--;

    if (bfSetp->fsRefCnt == 0) { 
        (void) bs_close(bfSetp->dirBfAp, MSFS_CLOSE_NONE);
        bfSetp->dirBfAp = NULL;
        
        /*
         * for active domains, remove all in-memory traces of set structure, 
         * flushes any remaining dirty buffers.  For recovering domains, 
         * this is delayed until advfs_bs_dmn_flush().
         */
#       ifdef Tru64_to_HPUX_Port_Comments
        /*
         *                        ----> The call to bfs_dealloc when
         *                              running recovery needs to be
         *                              investigated as bfs_dealloc is
         *                              a noop when FALSE is passed in.
         */
#       endif /* Tru64_to_HPUX_Port_Comments */
        if((dmnP->state == BFD_RECOVER_REDO) ||
           (dmnP->state == BFD_RECOVER_FTX) ||
           (dmnP->state == BFD_RECOVER_CONTINUATIONS))
            bfs_dealloc( bfSetp, FALSE );
        else
            bfs_dealloc( bfSetp, TRUE );
             
    } else if ( bfSetp->fsRefCnt < 0 ) {
        ADVFS_SAD0("bfs_close: bitfile set ref cnt went negative");
    }

    /* Close the bitfile domain */

    bs_domain_close( dmnP );
}

/*
 * bs_bfs_close
 *
 * This routine is called to close the specified bitfile-set.
 *
 * All sets in the snapset tree are also closed.  
 *
 * Returns EOK.  Should be changed to return void.
 */

statusT
bs_bfs_close(
    bfSetT *bfSetp,	/* in - pointer to open bitfile-set */
    ftxHT   ftxH,	/* in - transaction handle */
    bfs_op_flags_t options	/* in - options flags */
    )
{
    bfSetT *setp, *nextSetp;
    domainT *dmnP = bfSetp->dmnP;
    unLkActionT unLkAction = UNLK_NEITHER;
    int unlock_tbl_lock = FALSE;
    int bfap_scan_again = FALSE;

    /* Flush stats if requested by caller.  This is done by any callers
     * wanting to close the fileset opened externally.  We used to 
     * always flush dirty stats when the fileset gets deallocated, but
     * this had to be changed because updating stats requires the
     * snapset tree to be opened (if it exists).  By the time bfs_dealloc()
     * has been called, the snapset tree has been closed. We still
     * flush the fileset when it gets deallocated, but we don't update
     * stats.  That happens here.  We can't be holding the bfset
     * tbl lock when updating stats.
     */
    if (BFSET_VALID(bfSetp) && (options & BFS_OP_FLUSH_STATS) && 
            !BS_BFTAG_EQL(bfSetp->dirTag, staticRootTagDirTag)) 
    {
        MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_EQL( &dmnP->BfSetTblLock.lock.rw, 
                    RWL_UNLOCKED));
        advfs_bs_bfs_flush(bfSetp, TRUE, FLUSH_UPDATE_STATS);
    }

    /*
     * Here is where we grab BfSetTblLock.  Now we have to call 
     * lock_holder() first to see if we already have this locked 
     * since some undo and rtdn routines now acquire this lock 
     * early to prevent a hierarchy violation.
     */

    if (!ADVRWL_BFSETTBL_ISWRLOCKED( dmnP )) {
        /*
         * A more careful analysis is required to determine if this is
         * correct assert.  We cannot check to see if we hold this lock for
         * anything other than write, but I believe that we should only be
         * calling bs_bfs_close if we hold the write lock or do not hold the
         * lock at all.  If this is hit, we will need to pass some flag
         * indicating the lock state.
         */
        MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &dmnP->BfSetTblLock.lock.rw, 
                                                 RWL_UNLOCKED) );
        ADVRWL_BFSETTBL_WRITE( dmnP );
        unlock_tbl_lock = TRUE;
    }
    
    MS_SMP_ASSERT( BFSET_VALID(bfSetp));
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    if (options & BFS_OP_UNMOUNT) {
        struct fsContext *file_context = NULL;
        bfAccessT *bfap = NULL;
        bfAccessT *next_bfap;
        bfAccessT *bfap_list_marker;

        bfap_list_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
        bfap_list_marker->accMagic = ACCMAGIC_MARKER;
        bfap_list_marker->bfSetp = bfSetp;

        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);

        bfap = bfSetp->accessFwd;
        while (bfap != (bfAccessT *)(&bfSetp->accessFwd)) {
            if (bfap->accMagic == ACCMAGIC_MARKER) {
                bfap = bfap->setFwd;
                continue;
            }
            MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
            file_context = VTOC(&bfap->bfVnode);
            if (file_context == NULL) {
                bfap = bfap->setFwd;
                continue;
            }

            file_context->fsc_fsnp = (fileSetNodeT *)0xdeadbeefdeadbeef;
            if (file_context->quotaInitialized == TRUE) {
                /* If we have to detach quota, release the setaccesschain
                 * lock to avoid a hierarchy problem. Then re-lock and
                 * start scan again.
                 */
                INS_ACC_SETLIST(bfap_list_marker, bfap, FALSE);
                ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);
                advfs_fs_detach_quota(file_context);
                ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);
                bfap = bfap_list_marker->setFwd;
                RM_ACC_SETLIST(bfap_list_marker, FALSE);
                file_context->quotaInitialized = FALSE;
            } else bfap = bfap->setFwd;

            ADVSMP_FILESTATS_LOCK(&file_context->fsContext_lock);
            file_context->fs_flag = 0;
            MS_SMP_ASSERT( file_context->dirty_stats == FALSE );
            file_context->dirty_stats = FALSE;
            ADVSMP_FILESTATS_UNLOCK(&file_context->fsContext_lock);

            file_context->initialized = FALSE;

        }
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);
        ms_free(bfap_list_marker);
    }

    (void) advfs_snapset_close(bfSetp, ftxH);

    bfs_close( bfSetp, ftxH );

    /*
     * Unlock the table.
     */
    if ( !(options & BFS_CL_COND_UNLOCK) || (unlock_tbl_lock == TRUE) ) {
        ADVRWL_BFSETTBL_UNLOCK( dmnP );
    } 

    return EOK;
}


/*
 * BEGIN_IMS
 *
 * bfs_access - This routine opens/accesses a single fileset and does not do
 * any snapshot processing.  This routine should generally only be called by
 * bfs_open or bs_bfs_close.  Care must be taken when calling through
 * routine directly to not corrupt the snapset tree.
 *
 *
 * Parameters:
 * refBfSetp - A returned pointer to the opened bitfile set (on EOK)
 * bfSetId - The bfSetId of the fileset to open.
 * options - options
 * ftxH - the parent transaction handle.
 * 
 * Return Values:
 *      EOK - The requested fileset and relatives are open.
 *      E_NO_SUCH_BF_SET -  
 *      Errors from called routines.
 *
 * Entry/Exit Conditions:
 * On Entry, the bfSetTbl lock should be held for write or not held at all.  
 *
 * If the BFS_OP_LOCK_TBL_LOCK flag is set in the options, then the table
 * lock will be acquired if it is not already held.  Additionally, if the
 * BFS_OP_LOCK_TBL_LOCK flag is set, this routine will return with the lock
 * held and will set the BFS_OP_TBL_LOCK_LOCKED flag to indcate that the
 * lock was acquired and needs to be unlocked.
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
bfs_access(
    bfSetT**        retBfSetp, /* out - pointer to open bitfile-set desc */
    bfSetIdT        bfSetId,   /* in - bitfile-set id */
    bfs_op_flags_t* options,   /* in/out - options flags */
    ftxHT           ftxH       /* in - transaction handle */
    )
{
    domainT *dmnP;
    statusT sts;
    bfSetT *bfSetp;
    bfAccessT *tagDirBfap;
    int i;
    int dmnOpen = FALSE, tagDirOpen = FALSE, newSet = FALSE;
    int unlock_tbl_lock = FALSE;
    bsBfSetAttrT setAttr;
    struct vnode *nullvp;
    bfTagT dir_tag;
    bfMCIdT mcid;

    if (advfs_eio_on_bfs_access) {
        return EIO;
    }
    /*
     * Open the bitfile-set's domain.
     */
    sts = bs_domain_access( &dmnP,
                            bfSetId.domainId,
                            (*options & BFS_OP_DEACTIVATED_OK) );
    if (sts != EOK) { 
        goto HANDLE_EXCEPTION; 
    }
    dmnOpen = TRUE;


    /*
     * Lock the domain's bitfile-set lock if not already locked.
     */

    if (!ADVRWL_BFSETTBL_ISWRLOCKED( dmnP )) {
        /*
         * A more careful analysis is required to determine if this is
         * correct assert.  We cannot check to see if we hold this lock for
         * anything other than write, but I believe that we should only be
         * calling bs_bfs_close if we hold the write lock or do not hold the
         * lock at all.  If this is hit, we will need to pass some flag
         * indicating the lock state.
         */
        MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL(&dmnP->BfSetTblLock.lock.rw, 
                                              RWL_UNLOCKED));
        ADVRWL_BFSETTBL_WRITE( dmnP );
        if (*options & BFS_OP_LOCK_TBL_LOCK) {
            *options |= BFS_OP_TBL_LOCK_LOCKED;
        } else {
            unlock_tbl_lock = TRUE;
        }
    }


    if (BS_BFTAG_SEQ( bfSetId.dirTag ) == 0) {
        /* wild card bitfile set id (tag) */
        bfMCIdT bfMCId; /* Just dummy params to tagdir lookup */

        sts = tagdir_lookup(dmnP->bfSetDirp, &bfSetId.dirTag, &bfMCId);
        if (sts == E_OUT_OF_SYNC_SNAPSHOT && 
                (*options & BFS_OP_IGNORE_OUT_OF_SYNC)) {
            /* mask the error if instructed to ignore hard out-of-sync
             * snapset.
             */
            sts = EOK;
        }
        if (sts != EOK) {
            if (!BS_BFTAG_EQL(bfSetId.dirTag, staticRootTagDirTag)) {
                RAISE_EXCEPTION(ENO_SUCH_TAG);
            }
        }
    }

    /*
     * Get a pointer to the bitfile-set's descriptor.
     */
    bfSetp = bfs_lookup( bfSetId );

    if ( !BFSET_VALID(bfSetp) ) {
        /*
         * The fileset is not currently open, so Open the 
         * fileset's tag directory. The BF_OP_OVERRIDE_SMAX flag is passed
         * in to allow us to open a fileset even if we are at the maximum
         * allow number of access structures.  We don't want to fail an open
         * of a fileset because of this since it may be a dependant
         * (snapshot) fileset and would have be to marked out of sync if we
         * fail.
         */
        nullvp = NULL;
        sts = bs_access( &tagDirBfap, 
                        bfSetId.dirTag, 
                        dmnP->bfSetDirp, 
                        ftxH, 
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX, 
                        &nullvp );

        if (sts == ENO_SUCH_TAG) {
            RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
        } else if (sts != EOK) {
           if (sts == EHANDLE_OVF) {
                RAISE_EXCEPTION( sts );
            }
            ADVFS_SAD1( "bfs_access: can't access bitfile-set's tag dir", sts );
        }
        tagDirOpen = TRUE;

        /*
         * Call bfs_alloc() create a descriptor for the bitfile-set.
         */
        sts = bfs_alloc( bfSetId, dmnP, &bfSetp );
        if (sts != EOK) { 
            RAISE_EXCEPTION( sts ); 
        }
        newSet = TRUE;

        bfSetp->dirBfAp = tagDirBfap;

        sts = bmtr_get_rec(tagDirBfap,
                           BSR_BFS_ATTR,
                           (void *) &setAttr,
                           (uint16_t) sizeof( setAttr ) );
        if (sts != EOK) {
            domain_panic( dmnP, "bfs_access: get orig bf set attr failed", sts );
            goto HANDLE_EXCEPTION;
        }

        /*
         * We lookup the tag entry to see if the fileset is hard out of sync.
         */
        dir_tag = bfSetId.dirTag;
        sts = tagdir_lookup(dmnP->bfSetDirp, &dir_tag, &mcid);
        if (sts == E_OUT_OF_SYNC_SNAPSHOT && 
                (*options & BFS_OP_IGNORE_OUT_OF_SYNC)) {
            sts = EOK;
        }
        if (sts != EOK) { 
            if (sts != E_OUT_OF_SYNC_SNAPSHOT) {
                domain_panic(dmnP, "bfs_access: tagdir_lookup failed. [%d] %s", sts,
                        BSERRMSG(sts));
            }
            RAISE_EXCEPTION(sts);
        }

        strncpy( bfSetp->bfSetName, setAttr.setName, (size_t) BS_SET_NAME_SZ );
        bfSetp->bfs_dev = setAttr.fsDev;


        bfSetp->fsetThreshold.threshold_upper_limit =
            setAttr.fsetThreshold.threshold_upper_limit;

        bfSetp->fsetThreshold.threshold_lower_limit =
            setAttr.fsetThreshold.threshold_lower_limit;

        bfSetp->fsetThreshold.upper_event_interval  =
            setAttr.fsetThreshold.upper_event_interval;

        bfSetp->fsetThreshold.lower_event_interval  =
            setAttr.fsetThreshold.lower_event_interval;

        bfSetp->fsetThreshold.time_of_last_upper_event = 0;
        bfSetp->fsetThreshold.time_of_last_lower_event = 0;
        

        /*
         *  setAttr.flags maps to the low 16 bits of BfSetFlags
         */
        bfSetp->bfSetFlags = (uint32_t) setAttr.flags;

        /* Since the hard out of sync flag is associated with
         * the tagdir file for this fileset (via tag_flags for the tag entry),
         * we need to set the hard out of sync flag here explicitly.
         */
        if (dir_tag.tag_flags & BS_TD_OUT_OF_SYNC_SNAP) {
            if (*options & BFS_OP_IGNORE_OUT_OF_SYNC) {
                bfSetp->bfSetFlags |= BFS_OD_HARD_OUT_OF_SYNC;
            } else {
                RAISE_EXCEPTION(E_OUT_OF_SYNC_SNAPSHOT);
            }
        }
        
        if (setAttr.state == BFS_ODS_VALID) {
            bfSetp->state = BFS_READY;

        } else if (setAttr.state == BFS_ODS_DELETED) {
            bfSetp->state = BFS_DELETING;
        }


        sts = tagdir_get_info(bfSetp, &bfSetp->tagUnInPg,
                              &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                              &bfSetp->bfCnt );
        if (sts != EOK) {
            domain_panic(bfSetp->dmnP,
                         "bfs_access: can't get tag info");
            RAISE_EXCEPTION( sts );
        }

        if ( dmnP->state != BFD_ACTIVATED ) {
            /*
             * crash recovery may alter the on-disk version of the
             * info we just loaded so we need to reload it next time
             * the set is accessed (it will be accessed again during mount)
             */
            bfSetp->infoLoaded = FALSE;
        } else {
            bfSetp->infoLoaded = TRUE;
        }

    } else if (bfSetp->fsRefCnt == 0) {
        /*
         * This bitfile-set is not currently open but was open at some time.
         * Open the bitfile-set's tag directory.
         */

        nullvp = NULL;
        sts = bs_access(&tagDirBfap, 
                        bfSetId.dirTag, 
                        dmnP->bfSetDirp, 
                        ftxH,
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX,
                        &nullvp);
        if (sts == ENO_SUCH_TAG) {
            RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
        }
        if (sts != EOK) {
            if (sts == EHANDLE_OVF) {
                RAISE_EXCEPTION( sts );
            }
            ADVFS_SAD1( "bfs_access: can't access bitfile-set's tag dir", sts );
        }

        bfSetp->dirBfAp = tagDirBfap;

        tagDirOpen = TRUE;

        sts = bfs_alloc( bfSetId, dmnP, &bfSetp );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_access: bfs_alloc failed", sts );
        }
    }

    if ((*options & BFS_OP_IGNORE_DEL) == 0) {
        /*
         * When the set is deleting, treat as if it is deleted.
         */
        if (bfSetp->state == BFS_DELETING) {
            RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
        }
    }

    if (!bfSetp->infoLoaded && (dmnP->state == BFD_ACTIVATED)) {
        /*
         * When bitfile set is activated for the first time during
         * recovery, the bitfile counts can be wrong.  So reload now.
         */

        sts = tagdir_get_info( bfSetp, &bfSetp->tagUnInPg,
                               &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                               &bfSetp->bfCnt );
        if (sts != EOK) {
            domain_panic(bfSetp->dmnP,
                         "bfs_access: can't get tag info");
            RAISE_EXCEPTION( sts );
        }

        bfSetp->infoLoaded = TRUE;
    }

    bfSetp->fsRefCnt++;

    if (unlock_tbl_lock) {
        ADVRWL_BFSETTBL_UNLOCK( dmnP );
    }
    *retBfSetp = bfSetp;

    return EOK;

HANDLE_EXCEPTION:

    if (newSet) {
        bfs_dealloc( bfSetp, TRUE );
    }

    if (tagDirOpen) {
        (void) bs_close(tagDirBfap, MSFS_CLOSE_NONE);
    }

    /*
     * Unlock if we locked it.
     */
    if ( (unlock_tbl_lock) || 
         ( (*options & BFS_OP_LOCK_TBL_LOCK) && (*options & BFS_OP_TBL_LOCK_LOCKED) ) ) {
        ADVRWL_BFSETTBL_UNLOCK( dmnP );
        *options &= ~BFS_OP_TBL_LOCK_LOCKED;
    }

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (sts == E_OUT_OF_SYNC_SNAPSHOT) {
        advfs_snap_print_hard_out_of_sync(bfSetId);
    }
    return sts;
}


/*
 * BEGIN_IMS
 *
 * bfs_open - This routine opens a fileset and all related snapsets (every
 * one in the snapset tree).  This routine must be called to acquire a
 * pointer to the bfSetT before accessing or creating files in a fileset.  
 *
 * This routine uses bfs_access to open individual filesets, but will open
 * all associated sets.  Care must be taken when opening on a single fileset
 * with bfs_access since that will not open dependencies.
 *
 * Parameters:
 * refBfSetp - A returned pointer to the opened bitfile set (on EOK)
 * bfSetId - The bfSetId of the fileset to open.
 * options - options
 * ftxH - the parent transaction handle.
 * 
 * Return Values:
 *      EOK - The requested fileset and relatives are open.
 *      E_NO_SUCH_BF_SET -  
 *      Errors from called routines.
 *
 * Entry/Exit Conditions:
 * On Entry, the bfSetTbl lock should be held for write or not held at all.  
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
bfs_open(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    uint32_t options,      /* in - options flags */
    ftxHT ftxH             /* in - transacton handle */
    )
{
    statusT sts;
    bfSetT *bfSetp, *setp, *prevSetp;
    bfSetIdT setId;
    bfSetT *origSetp;
    bsBfSetAttrT bfSetAttr;
    bfTagT setDirTag;
    domainT *dmn_p;
    bfs_op_flags_t      bfs_op_flag = options;


    /*
     * Access the domain
     */
    sts = bs_domain_access(&dmn_p, bfSetId.domainId, FALSE);
    if (sts != EOK) {
        return sts; 
    }

    /*
     *  Open the specified bitfile set.
     */
    bfs_op_flag |= BFS_OP_LOCK_TBL_LOCK;
    sts = bfs_access( &bfSetp, 
                        bfSetId, 
                        &bfs_op_flag, 
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }
    
    /*
     * The table lock should have been acquired by bfs_access if it was
     * required.  If it was acquired, BFS_OP_TBL_LOCK_LOCKED was set in the
     * bfs_op_flag so we need to unlock it.
     */

    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL(&dmn_p->BfSetTblLock.lock.rw, 
                                              RWL_WRITELOCKED));


    /*
     * If this fileset is not the root bitfileset, see if any snapshots need
     * to be accessed
     */
    if (BS_BFTAG_REG( bfSetp->dirTag )) {
        snap_flags_t snap_flags = SF_IGNORE_BFS_DEL;

        if (bfs_op_flag & BFS_OP_IGNORE_OUT_OF_SYNC) {
            snap_flags |= SF_IGNORE_BFS_OUT_OF_SYNC;
        }
        sts = advfs_snapset_access( bfSetp, snap_flags, ftxH );

        if (sts != EOK) {
            bfs_close( bfSetp, ftxH );
            goto HANDLE_EXCEPTION;
        }
    }

    if (bfSetp->bfSetFlags & BFS_OD_OUT_OF_SYNC) {
        (void) advfs_snap_print_out_of_sync( NilBfTag, bfSetp );
    }

    /*
     * Unlock the table (if locked here).
     */

    if (bfs_op_flag & BFS_OP_TBL_LOCK_LOCKED) {
      ADVRWL_BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    bs_domain_close(dmn_p);

    *retBfSetp = bfSetp;

    return EOK;

HANDLE_EXCEPTION:

    if (bfs_op_flag & BFS_OP_TBL_LOCK_LOCKED) {
        ADVRWL_BFSETTBL_UNLOCK( dmn_p );
    }

    bs_domain_close(dmn_p);

    return sts;
}


/*
 * BEGIN_IMS
 *
 * advfs_bs_delete_fileset_tags -
 *
 * Parameters:
 *
 * Return Values:
 *
 * END_IMS
 */

static statusT
advfs_bs_delete_fileset_tags(
        bfSetT*     bf_set_p    /* in - pointer to fileset to cleanup */
        )
{
    bfMCIdT mcid;
    bfAccessT *bfap;

    bfTagT cur_tag = NilBfTag;
    statusT sts    = EOK;
    int file_count = 0;
    ftxHT ftxH     = FtxNilFtxH;
    struct vnode *vp = NULL;

    MS_SMP_ASSERT(bf_set_p->state == BFS_DELETING);
    
    while (TRUE) {
        bfap = NULL;
        sts = tagdir_lookup_next(bf_set_p, &cur_tag, &mcid);
        if (sts == ENO_SUCH_TAG) {
            /* Normal loop exit */
            break;
        } else if (sts != EOK) {
            domain_panic(bf_set_p->dmnP, "advfs_bs_delete_fileset_tags: "
                    "tagdir_lookup_next returned %s", BSERRMSG(sts));
            return E_DOMAIN_PANIC;
        }



        sts = FTX_START_N( FTA_BFS_DELETE_SNAP, &ftxH, FtxNilFtxH, 
                bf_set_p->dmnP);
        if (sts != EOK) {
            domain_panic(bf_set_p->dmnP, "advfs_bs_delete_fileset_tags: "
                    "failed to start transaction. %s", BSERRMSG(sts));
            ftx_fail( ftxH );
            return E_DOMAIN_PANIC;
        }


        if (cur_tag.tag_flags & BS_TD_VIRGIN_SNAP) {
            /* Tag does not have it's own metadata */
            sts = bs_access_one(&bfap, cur_tag, 
                    bf_set_p, 
                    ftxH,
                    BF_OP_INMEM_ONLY|BF_OP_IGNORE_BFS_DELETING|BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
            if (sts != EOK && sts != EINVALID_HANDLE && sts != EHANDLE_OVF) {
                /* There was some error besides not finding the access
                 * structure ACC_VALID.
                 */
#               ifdef SNAPSHOT
                /* print error message? */
#               endif
                SNAP_STATS( delete_fileset_tags_access_err_1 );
                goto _loop_end;
            }
            tagdir_remove_tag(bf_set_p, &cur_tag, ftxH);
            if (bfap != NULL) {
                /* snapshot access structure was found in cache */
                MS_SMP_ASSERT(bfap->bfaFlags & BFA_VIRGIN_SNAP);
                advfs_unlink_snapshot(bfap);
                ADVSMP_BFAP_LOCK(&bfap->bfaLock);
                lk_set_state(&bfap->stateLk, ACC_INVALID);
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
            } 
        } else {
            sts = bs_access_one(&bfap, cur_tag, bf_set_p, ftxH,
                    BF_OP_IGNORE_BFS_DELETING|BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
            if (sts != EOK) {
#ifdef SNAPSHOT
                /* Is this the right behavior? */
#endif
                SNAP_STATS( delete_fileset_tags_access_err_2 );
                goto _loop_end;
            }
            advfs_unlink_snapshot(bfap);
            rbf_delete(bfap, ftxH);
        }
_loop_end:
        ftx_done_n(ftxH, FTA_BFS_DELETE_SNAP);

        /* 
         * If bf_set_p has a parent, then we want to clean the state of the
         * parent snapshot file.  advfs_snap_clean_state will adjust the
         * BFA_SNAP_CHANGE flag to make sure it is correct.
         */
        if (bf_set_p->bfsParentSnapSet) {
            sts = advfs_snap_clean_state(bf_set_p, cur_tag);
        }

        if (bfap != NULL) {

            MS_SMP_ASSERT(bfap->refCnt == 1);
            bs_close_one(bfap, MSFS_BFSET_DEL, FtxNilFtxH);
        }
        file_count++;
        if (file_count && 
                file_count % ADVFS_FILES_BEFORE_PREEMPTION_POINT == 0)
        {
            delay(1);
        }
    }

    return EOK;
}


/*
 * bfs_delete_pending_list_add
 *
 * Adds a bitfile set to the "bf set delete pending" list.
 *
 * Caller must hold the BfSetTblLock locked via an FTX lock.
 *
 * Returns 1 if the bf set was successfully added to the delete pending list.
 * Returns 0 if the bf set was already on the delete pending list.
 *
 * Note that this routine cannot start its own sub transaction because
 * it is called from a ftx undo routine in addition to being a general
 * "add to the list" routine.  Therefore, the caller is responsible for
 * starting the transaction.
 */

static int
bfs_delete_pending_list_add(
    bfAccessT *dirBfAp,         /* in - set's tagdir bf struct */
    domainT *dmnP,              /* in - set's domain pointer */
    ftxHT ftxH                  /* in - transaction handle */
    )
{
    bsBfSetAttrT *setAttrp;
    bsDmnMAttrT *dmnMAttrp;
    statusT sts;
    vdT *logVdp = NULL;
    rbfPgRefHT fsetPgH, dmnPgH;

    sts = bmtr_get_rec_ptr( dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            (uint16_t) sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_add: bmtr_get_rec_ptr failed return code = %d.", sts );
        return 0;    /* E_DOMAIN_PANIC */
    }

    if (setAttrp->state == BFS_ODS_DELETED) {
        /*
         * The set is already on the delete pending list.
         * deref the page returned by bmtr_get_rec_ptr()
         */

        rbf_deref_page( fsetPgH, BS_CACHE_IT );

        return 0;
    }

    logVdp = VD_HTOP((vdIndexT) BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    sts = bmtr_get_rec_ptr( logVdp->rbmtp,
                            ftxH,
                            BSR_DMN_MATTR,
                            (uint16_t) sizeof( *dmnMAttrp ),
                            1 /* pin pg */,
                            (void *) &dmnMAttrp,
                            &dmnPgH,
                            NULL );
    if (sts != EOK){
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_add: bmtr_get_rec_ptr failed return code = %d.", sts );
        return 0;    /* E_DOMAIN_PANIC */
    }

    /* mark bitfile set 'deleted' */
    RBF_PIN_FIELD( fsetPgH, setAttrp->state );
    setAttrp->state = BFS_ODS_DELETED;

    /* add the bitfile set to the 'delete pending list' */
    RBF_PIN_FIELD( fsetPgH, setAttrp->nxtDelPendingBfSet );
    setAttrp->nxtDelPendingBfSet = dmnMAttrp->delPendingBfSet;

    RBF_PIN_FIELD( dmnPgH, dmnMAttrp->delPendingBfSet );
    dmnMAttrp->delPendingBfSet = setAttrp->bfSetId.dirTag;

    return 1;
}

/*
 * del_list_remove_undo
 *
 * Puts the bitfile set back on the 'bf set delete pending' list.
 */

typedef struct {
    bfSetIdT setId;
} delPendUndoRecT ;

opxT del_list_remove_undo;

void
del_list_remove_undo(
    ftxHT ftxH,      /* in - ftx handle */
    int opRecSz,     /* in - size of opx record */
    void* opRec      /* in - ptr to opx record */
    )
{
    delPendUndoRecT undoRec;
    bfAccessT *tagDirBfap;
    domainT *dmnP;
    statusT sts;
    struct vnode *nullvp = NULL;

    dmnP =  ftxH.dmnP;

    bcopy(opRec, &undoRec, sizeof(delPendUndoRecT));

    /*
     * Open the orig set's tag dir.
     */

    sts = bs_access(&tagDirBfap, undoRec.setId.dirTag, dmnP->bfSetDirp,
                    FtxNilFtxH, BF_OP_INTERNAL, &nullvp);
    if (sts != EOK) {
        ADVFS_SAD1( "del_list_remove_undo: bs_access failed", sts );
    }

    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH );

    bfs_delete_pending_list_add(tagDirBfap, dmnP, ftxH );

    bs_close(tagDirBfap, MSFS_CLOSE_NONE);
}

/*
 * bfs_delete_pending_list_remove
 *
 * Removes a bitfile set from the 'bf set delete pending' list.
 *
 * This routine uses a subtransaction to do its work.  It also
 * has an operational undo which simply puts the set back on the
 * delete pending list.
 */

static void
bfs_delete_pending_list_remove(
    bfAccessT *dirBfAp,         /* in - set's tagdir bf struct */
    ftxHT parentFtxH            /* in - parent transaction handle */
    )
{
    ftxHT ftxH;
    bsBfSetAttrT *setAttrp, *nextSetAttrp;
    bsDmnMAttrT *dmnMAttrp;
    statusT sts;
    domainT *dmnP;
    vdT *logVdp = NULL;
    delPendUndoRecT undoRec;
    bfAccessT *nextTagDirBfap;
    bfTagT nextTag;
    rbfPgRefHT fsetPgH, nextFsetPgH, dmnPgH;
    struct vnode *nullvp;

    dmnP = dirBfAp->dmnP;

    sts = FTX_START_N( FTA_BS_BFS_DEL_LIST_REMOVE_V1, &ftxH,
                       parentFtxH, dmnP );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_delete_pending_list_remove: ftx_start failed", sts );
    }

    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH );

    sts = bmtr_get_rec_ptr( dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            (uint16_t) sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (1)  failed, return code = %d", sts );
        ftx_fail(ftxH);
        return;
    }

    /*
     * Get the 'head' of the 'bf set delete pending list' from
     * the domain attributes record.
     */

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    sts = bmtr_get_rec_ptr( logVdp->rbmtp,
                            ftxH,
                            BSR_DMN_MATTR,
                            (uint16_t) sizeof( *dmnMAttrp ),
                            1 /* pin pg */,
                            (void *) &dmnMAttrp,
                            &dmnPgH,
                            NULL );
    if (sts != EOK){
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (2)  failed, return code = %d", sts );
        ftx_fail(ftxH);
        return;
    }

    /*
     * Now scan the list until we find the set that preceeds the set
     * we are deleting.  This is a typical remove an element from
     * a singly-linked list where you need the element that preceeds
     * the to-be-deleted element so that you can fix up the pointers
     * properly.
     */

    nextTag = dmnMAttrp->delPendingBfSet;

    while (!BS_BFTAG_EQL( nextTag, setAttrp->bfSetId.dirTag )) {

        nullvp = NULL;
        sts = bs_access( &nextTagDirBfap, nextTag, dmnP->bfSetDirp,
                              FtxNilFtxH, BF_OP_INTERNAL, &nullvp );
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bs_access failed, return code = %d", sts ); 
            ftx_fail(ftxH);
            return;
        }

        sts = bmtr_get_rec_ptr( nextTagDirBfap,
                                ftxH,
                                BSR_BFS_ATTR,
                                (uint16_t) sizeof( *nextSetAttrp ),
                                1 /* pin pg */,
                                (void *) &nextSetAttrp,
                                &nextFsetPgH,
                                NULL );
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (3) failed, return code = %d", sts );
            ftx_fail(ftxH);
            return;
        }

        nextTag = nextSetAttrp->nxtDelPendingBfSet;

        if (!BS_BFTAG_EQL( nextTag, setAttrp->bfSetId.dirTag )) {
            rbf_deref_page( nextFsetPgH, BS_CACHE_IT );
            bs_close(nextTagDirBfap, MSFS_CLOSE_NONE);
        }
    }

    /*
     * Mark bitfile set 'invalid'
     */

    RBF_PIN_FIELD( fsetPgH, setAttrp->state );
    setAttrp->state = BFS_ODS_INVALID;

    /*
     * remove the bitfile set from the 'delete pending list'
     */

    if (BS_BFTAG_EQL( dmnMAttrp->delPendingBfSet, setAttrp->bfSetId.dirTag )) {

        RBF_PIN_FIELD( dmnPgH, dmnMAttrp->delPendingBfSet );
        dmnMAttrp->delPendingBfSet = setAttrp->nxtDelPendingBfSet;

    } else {
        RBF_PIN_FIELD( nextFsetPgH, nextSetAttrp->nxtDelPendingBfSet );
        nextSetAttrp->nxtDelPendingBfSet = setAttrp->nxtDelPendingBfSet;

        bs_close(nextTagDirBfap, MSFS_CLOSE_NONE);
    }

    RBF_PIN_FIELD( fsetPgH, setAttrp->nxtDelPendingBfSet );
    setAttrp->nxtDelPendingBfSet = NilBfTag;

    undoRec.setId = setAttrp->bfSetId;

    ftx_done_u(ftxH, FTA_BS_BFS_DEL_LIST_REMOVE_V1, (int) sizeof(undoRec), 
            &undoRec);
}

/*
 * bfs_delete_pending_list_finish_all
 *
 * This routine scans the 'bf set delete pending' list and finishes
 * deleting all the bitfile sets which are on the list.
 *
 * Note:  Since this routine is only called during domain activation it
 *        assumes that no other thread is mucking with the 'bf set
 *        delete pending' list.  Therefore, no locking is done.
 */

void
bfs_delete_pending_list_finish_all(
    domainT *dmnP                       /* in */
    )
{
    statusT sts;
    bfSetIdT setId;
    bsDmnMAttrT dmnMAttr;
    bfTagT nextTag;
    bfAccessT *tagDirBfap;
    bsBfSetAttrT setAttr;
    vdT *logVdp = NULL;
    struct vnode *nullvp;

    /*
     * Get the 'head' of the 'bf set delete pending list' from
     * the domain attributes record.
     */

    logVdp = VD_HTOP((vdIndexT) BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    sts = bmtr_get_rec(logVdp->rbmtp,
                       BSR_DMN_MATTR,
                       &dmnMAttr,
                       (uint16_t) sizeof( dmnMAttr ) );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_delete_pending_list_finish_all: get dmn attr failed", sts );
    }

    /*
     * Now scan the list and call bs_bfs_delete() for each set in the list.
     * Note that bs_bfs_delete() will remove the deleted set from the
     * list so this routine does not have to deal with that issue.
     */

    setId.domainId = dmnP->domainId;
    nextTag = dmnMAttr.delPendingBfSet;

    while (!BS_BFTAG_EQL( nextTag, NilBfTag )) {
        /*
         * Get and save the pointer (tag) to the next set in the list
         * before deleting the set.  This is because the pointer to the
         * the next set is kept in the set's tag dir's mcells; these
         * deleted when the set is deleted.
         */

        nullvp = NULL;
        sts = bs_access( &tagDirBfap,
                         nextTag,
                         dmnP->bfSetDirp,
                         FtxNilFtxH,
                         BF_OP_INTERNAL,
                         &nullvp );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_delete_pending_list_finish_all: bs_access failed", sts );
        }

        sts = bmtr_get_rec( tagDirBfap,
                           BSR_BFS_ATTR,
                           (void *) &setAttr,
                           (uint16_t) sizeof( setAttr ) );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_delete_pending_list_finish_all: get orig set attr failed", sts );
        }

        bs_close(tagDirBfap, MSFS_CLOSE_NONE);

        setId.dirTag = nextTag;
        nextTag = setAttr.nxtDelPendingBfSet;

        bs_bfs_delete( setId, (ftxIdT) 0 );
    }
}



/* 
 * BEGIN_IMS
 *
 * advfs_can_remove_fileset - will open all filesets related to the 
 * fileset specified by bf_set_id and will verify if the fileset cannot
 * be removed.
 *
 * Parameters:
 * bf_set_id - bitfile set id to be deleted
 * dmn_p - pointer to domain in-memory structure
 * bf_set_p - address of pointer to bfSet structure returned on successful
 * completion.
 *
 * Return Values:
 * EOK
 * E_ACCESS_DENIED
 * E_HAS_SNAPSHOT
 * E_TOO_MANY_ACCESSORS
 * other return values from bfs_open
 * 
 * END_IMS
 */

static statusT
advfs_can_remove_fileset(
        bfSetIdT    bf_set_id,  /* in - bitfile set id */
        bfSetT      **bf_set_p  /* out - pointer to bitfile set to delete.
                                 * Valid only on EOK. */
        )
{
    ftxHT ftxH = FtxNilFtxH;
    statusT sts = EOK;
    bfDmnParamsT *dmn_params_p = NULL;
    bfSetParamsT *set_params_p = NULL;
    bfSetT       *ret_set_p = NULL;

    *bf_set_p = NULL;

    /*
     * This may be called while processing the bfs_delete__pending list
     * during recovery in which case the fileset may already be deleted and
     * need final cleanup.  Allow the openeing of deleted filesets in this
     * case.
     */
    sts = bfs_open(&ret_set_p, 
            bf_set_id, 
            BFS_OP_IGNORE_DEL|BFS_OP_IGNORE_OUT_OF_SYNC, 
            ftxH);
    if (sts != EOK) {
        return sts;
    }

    dmn_params_p = (bfDmnParamsT *)ms_malloc_no_bzero(sizeof(bfDmnParamsT));
    sts = bs_get_dmn_params(ret_set_p->dmnP, dmn_params_p, 0);
    if (sts != EOK) {
        goto _error;
    }

    set_params_p = (bfSetParamsT *)ms_malloc_no_bzero(sizeof(bfSetParamsT));
    sts = bs_get_bfset_params(ret_set_p, set_params_p, 0);
    if (sts != EOK) {
        goto _error;
    }
    
    if (!bs_accessible(BS_ACC_WRITE, dmn_params_p->mode, dmn_params_p->uid, 
                dmn_params_p->gid) ||
            !bs_accessible(BS_ACC_WRITE, set_params_p->mode, 
                set_params_p->uid, set_params_p->gid)) 
    {
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    if (ret_set_p->bfsNumSnapChildren > 0) {
        sts = E_HAS_SNAPSHOT;
        goto _error;
    }

    /* The only ref on the fileset should be the one we just placed
     * on it with the call to bfs_open.
     */
    if (ret_set_p->fsRefCnt - ret_set_p->bfsSnapRefs != 1) {
        sts = E_TOO_MANY_ACCESSORS;
        goto _error;
    }

    ms_free(set_params_p);
    ms_free(dmn_params_p);

    *bf_set_p = ret_set_p;
    return EOK;

_error:

    bs_bfs_close(ret_set_p, ftxH, BFS_OP_DEF);
    if (dmn_params_p) {
        ms_free(dmn_params_p);
    }
    
    if (set_params_p) {
        ms_free(set_params_p);
    }
    return sts;
}

/* 
 * BEGIN_IMS
 * 
 * advfs_can_remove_fileset_second_check - is a lightweight verson of 
 * advfs_can_remove_fileset.  It is to be called in the context of an
 * exclusive transaction in which the fileset state will be set to 
 * BFS_DELETING.  This routine will verify that the fileset was not
 * transitioned to a sate from which it cannot be deleted while the
 * exclusive transaction was starting. 
 * 
 * Parameters:
 * bf_set_p - pointer to bitfile set
 *
 * Return Values:
 * E_HAS_SNAPSHOT
 * E_TOO_MANY_ACCESSORS
 *
 * END_IMS
 */

static statusT
advfs_can_remove_fileset_second_check(
        bfSetT  *bf_set_p   /* in - pointer to bitfile set to delete */
        )
{
    if (bf_set_p->bfsNumSnapChildren > 0) {
        return E_HAS_SNAPSHOT;
    }

    if (bf_set_p->fsRefCnt - bf_set_p->bfsSnapRefs != 1) {
        return E_TOO_MANY_ACCESSORS;
    }

    return EOK;
}

/*
 * bs_bfs_delete
 *
 * Deletes a bitfile set.   This basically means:
 *      - delete all bitfiles in the set
 *      - delete the set's tagdir
 *
 * Note:  This routine must be idempotent.  In other words, if the system
 *        crashes during a bs_bfs_delete() it must be possible for
 *        domain activation to call this routine again to complete
 *        the deletetion.  Therefore, this routine can't make any
 *        assumptions about being called only once.
 */


statusT
bs_bfs_delete(
    bfSetIdT bf_set_id, /* in - bitfile set id */
    ftxIdT  xid         /* in - CFS transaction id */
    )
{
    int setOpen    = FALSE;
    int ftxStarted = FALSE;
    int has_parent = FALSE;
    bfSetT *set_p, *unused_set_p;
    statusT sts;
    bfAccessT *dirBfAp;
    ftxHT ftxH = FtxNilFtxH;
    int ss_was_activated = FALSE, oldState;
    bfs_op_flags_t bfs_options;
    domainT *dmn_p;
    bfSetT      *parent_set;
    int16_t     cleanup_parent_bfaps = FALSE;

    sts = advfs_can_remove_fileset(bf_set_id, &set_p);
    if (sts != EOK) {
        goto _error;
    }
    setOpen = TRUE;

    dmn_p = set_p->dmnP;
    if(dmn_p->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        /* deactivate/drain vfast operations */
        (void)advfs_ss_change_state(dmn_p->domainName,
				    SS_SUSPEND, &oldState, 0);
        ss_was_activated = TRUE;
    }

    sts = FTX_START_EXC_XID( FTA_BFS_DEL_PENDING_ADD, &ftxH, dmn_p, xid );
    if (sts != EOK) {
        goto _error;
    }
    ftxStarted = TRUE;

    sts = advfs_can_remove_fileset_second_check(set_p);
    if (sts != EOK) {
        goto _error;
    }

    FTX_LOCKWRITE( &dmn_p->BfSetTblLock, ftxH );

    set_p->state = BFS_DELETING;

    if (set_p->bfsParentSnapSet != NULL) {
        has_parent = TRUE;
        parent_set = set_p->bfsParentSnapSet;
    }
    /*
     * We need to add the set to the delete pending list.
     * This way if the system crashes during the delete we'll know
     * at domain activation time that we need to finish deleting this set.
     * We will issue a LOGSYNC with this ftx_done so that we KNOW the
     * fileset will never again come back into existance.  If the system
     * crashes, then recovery will finish the removal.  This is necessary
     * because advfs_bs_delete_fielset_tags will begin to unlink files from
     * there parents which means they will become out of sync.  By issuing
     * the log sync, we can unlink the files from parents and not worry
     * about out of sync issues.
     */
    if (bfs_delete_pending_list_add(set_p->dirBfAp, dmn_p, ftxH)) {
        ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );
        ftx_done_n( ftxH, FTA_BFS_DEL_PENDING_ADD );
    } else {
        /* bitfile set was already on the delete pending list */
        ftx_fail( ftxH );
    }

    ftxStarted = FALSE;

    sts = advfs_bs_delete_fileset_tags(set_p);

    /*
     * Now, really delete the set.
     */

    sts = FTX_START_EXC( FTA_BFS_DELETE, &ftxH, dmn_p );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_delete: start ftx failed", sts );
    }

    ftxStarted = TRUE;

    bfs_options = BFS_OP_IGNORE_DEL|BFS_OP_IGNORE_OUT_OF_SYNC;
    sts = bfs_access(&unused_set_p, set_p->bfSetId, &bfs_options, ftxH);

    if ( sts != EOK ) {
        sts = EOK;
        goto _error;
    }

    ADVRWL_BFSETTBL_WRITE( dmn_p );

    /* Close the sets. */


    if (parent_set && (parent_set->fsRefCnt > 1)) {
        /* 
         * If the call to bs_bfs_close will not be the last close of the
         * parent, then we want to restore the page permissions after the
         * call to bfs_bfs_close.  If this is the last close of the parent,
         * then the parent will be deallocated adn it is not necessary to
         * restore the permissions.  The bfSetTbl lock will protect against
         * new snapsets coming into existence or the parent being mounted.
         */
        cleanup_parent_bfaps = TRUE;
    }


    (void) bs_bfs_close(set_p, ftxH, BFS_CL_COND_UNLOCK);

    sts = advfs_unlink_snapset(set_p, 
            (has_parent) ? SF_HAD_PARENT : SF_SNAP_NOFLAGS, 
            ftxH);

    if (sts != EOK) {
        sts = EOK;
        ADVRWL_BFSETTBL_UNLOCK( dmn_p );
        goto _error;
    }

    if (cleanup_parent_bfaps && (parent_set->bfsFirstChildSnapSet == NULL)) {
        /* The parent is still open and/or mounted and has no remaining
         * snapshot children.  The bfSetTbl lock protects us from newly
         * created snapshot coming into existence, so restore the cache
         * permissions on all open files in the parent set.
         */
        sts = advfs_snap_restore_cache_protections( parent_set );
        MS_SMP_ASSERT( sts == EOK );
        if (sts != EOK) {
            /*
             * There is currently no error case that can be returned from
             * advfs_snap_restore_cache_protectoins.  This is purely
             * defensive in case someone accidently modifies it to return an
             * error in the future.  If the error is not handled, pages
             * could be left in cache and read only, thereby causing
             * infinite faults on any writes.
             */
            domain_panic( dmn_p, "bs_bfs_delete: Failed to restore snap cache protections");
            sts = E_DOMAIN_PANIC;
            goto _error;
        }
    }


    while (set_p->fsRefCnt > 0) {
        set_p->fsRefCnt--;
        /* Now close the access on the domain that was added from the
         * bfs_access call earlier.
         */
        bs_domain_close( dmn_p );
    }
    ADVRWL_BFSETTBL_UNLOCK( dmn_p );
    
    dirBfAp = set_p->dirBfAp;

    bfs_delete_pending_list_remove(dirBfAp, ftxH);

    /*
     * Delete the set's tag dir.  This has to be done after the above
     * unlinking since the links are maintained in the tag dir's
     * mcells.  Hence, we can't delete the tag dir before we deal with
     * links.
     */

    sts = rbf_delete( dirBfAp, ftxH );
    if (sts != EOK) {
        domain_panic(dmn_p, "bs_bfs_delete: rbf_delete returned: %d", sts);
        sts = E_DOMAIN_PANIC;
        goto _error;
    }



    /*
     * Now that the on-disk representation of the fileset has been
     * invalidated, invalidate the in-memory representation.  It is
     * important to do this after the on-disk representation has been
     * invalidated because if the in-memory representation were
     * invalidated (and removed, via ms_free), the fileset could still
     * be accessed afresh, since there would be no in-memory warning
     * that the fileset is going away.
     */

    ADVRWL_BFSETTBL_WRITE( dmn_p );
    bfs_dealloc( set_p, TRUE );
    ADVRWL_BFSETTBL_UNLOCK( dmn_p );

    /* in a cluster ensure removing a snapshot fset cannot be undone if 
     * the server fails because we are about to notify all cluster
     * nodes that they can write directly to storage if they hold the
     * appropriate tokens.  Because this bypasses AdvFS, no COWs will
     * be performed.
     */
    if (clu_is_ready() && has_parent) {
	ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );
    } 
    
    ftx_done_n( ftxH, FTA_BFS_DELETE );
    
    ftxStarted = FALSE;
   
    bs_close(dirBfAp, MSFS_BFSET_DEL);

    if (ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        (void)advfs_ss_change_state(dmn_p->domainName,
				    SS_ACTIVATED, &oldState, 0);
        ss_was_activated = FALSE;
    }




    return EOK;

_error:


    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (setOpen) {
        (void) bs_bfs_close(set_p, ftxH, BFS_OP_DEF);
    }

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        (void)advfs_ss_change_state(dmn_p->domainName,
				    SS_ACTIVATED, &oldState, 0);
    }

    return sts;
}



/*
 * bs_bfs_add_root
 *
 * Special interface to add the root bitfile-set to the bitfile-set
 * table.
 *
 * Returns status of bfs_alloc().
 */

statusT
bs_bfs_add_root(
    bfSetIdT bfSetDirId,   /* in - bitfile-set id */
    domainT *dmnP,         /* in - BF-set's domain's pointer */
    bfSetT **retBfSetDirp  /* out - pointer to BF-set's descriptor */
    )
{
    statusT sts;
    bfSetT *bfSetp;

    ADVRWL_BFSETTBL_WRITE( dmnP );

    sts = bfs_alloc( bfSetDirId, dmnP, &bfSetp );

    if (sts == EOK) {
        bfSetp->state = BFS_READY;
        *retBfSetDirp = bfSetp;
    }

    ADVRWL_BFSETTBL_UNLOCK( dmnP );
    return sts;
}

/*
 * bs_bfs_switch_root_tagdir
 *
 * Special interface to switch the root bitfile-set's tag directory.  This
 * function closes the old tag directory and opens the new tag directory.
 *
 * This function assumes that the caller has exclusive access to the bitfile
 * set descriptor.
 */

void
bs_bfs_switch_root_tagdir (
                           domainT *domain,  /* in */
                           bfTagT newTag  /* in */
                           )
{
    bfSetT *bfSetp;
    statusT sts;
    bfAccessT *dirbfap;

    bfSetp = domain->bfSetDirp;

    bs_invalidate_rsvd_access_struct(domain,
                                     domain->bfSetDirTag,
                                     bfSetp->dirBfAp);

    sts = bfm_open_ms(&dirbfap,
                      domain,
                      BS_BFTAG_VDI(newTag),
                      BFM_BFSDIR);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_switch_root_tagdir: bfm_open_ms() failed", sts );
    }
    bfSetp->dirBfAp = dirbfap;

    return;
}

/*
 * bs_bfset_activate
 *
 * Activates the bitfile set specified by bfSetName.
 *
 * Returns EOK, E_BAD_BF_SET_TBL, or the status returned by the
 * called subroutines.
 */

statusT
bs_bfset_activate(
    char *bfDmnName,    /* in - bitfile-set's domain file name */
    char *bfSetName,    /* in - bitfile-set name */
    uint64_t flags,     /* in - advfs related activation flags (bs_domain.h)*/
    bfSetIdT *bfSetId)  /* out - bitfile-set's ID */
{
    bfDomainIdT dmnId;
    domainT *dmnP;
    bfSetParamsT setParams;
    int dmnActive = FALSE, dmnOpen = FALSE;
    statusT sts;
    bfsQueueT *entry;
    bfSetT *bfSetP, *temp;

    sts = bs_bfdmn_tbl_activate( bfDmnName, flags, &dmnId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, dmnId, FALSE );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    /* 
     * Try the fast path to the in memory bfSetIdT structure if the bfSet is
     * mounted. We need to look through the associated filesets in the active
     * domain structure.  If we find the matching fileset, then we can grab 
     * the bfSetId from there. Otherwise find the bfSetIdT on the disk.
     */
    
    ADVRWL_BFSETTBL_READ(dmnP);
    entry = dmnP->bfSetHead.bfsQfwd;
    bfSetP = NULL;
    while (entry != &dmnP->bfSetHead) {
        temp = BFSET_QUEUE_TO_BFSETP(entry);
        if (!strcmp(temp->bfSetName, bfSetName) 
                && (temp->fsRefCnt > 0) && (temp->fsnp != NULL)) {
            bfSetP = temp;
            break;
        }
        entry = entry->bfsQfwd;
    }
    ADVRWL_BFSETTBL_UNLOCK(dmnP);

    if (bfSetP != NULL) {
        /* Found the bfSetId in memory already.  Let's make a copy
           of it for ourselves. */

        *bfSetId = bfSetP->bfSetId;
    } else { 
        /*
         * Get the bfSetId from the disk.
         */
        
        sts = bs_bfs_find_set(bfSetName,
                              dmnP, 
                              flags & (DMNA_LOCAL_ROOT | DMNA_GLOBAL_ROOT),
                              &setParams);

        if (sts != EOK) {
            /* The set doesn't exist! */
            goto _error;
        }

        *bfSetId = setParams.bfSetId;
    }

    bs_domain_close( dmnP );

    return EOK;

_error:

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, flags );
    }

    return( sts );
}

/*
 * bs_bfs_get_info
 *
 * This routine may be used to scan thru the bitfile sets in a
 * domain and get each set's attributes.
 *
 * The routine uses the parameter 'nextSetIdx' to determin which
 * set's info is desired.  If it is 0 (zero) then the routine starts
 * with the set whose tag is 1.  Otherwise, it uses the nextSetIdx
 * as the set's tag (which means that the caller can ask for the
 * info for a specific set by passing the correct tag; note that if
 * an invalid tag is passed this routine will most likely return
 * the attributes of the next valid set).
 *
 * Whenever the routine is successful it will update 'nextSetIdx' to
 * the next tag.  This way the caller can initialize 'nextSetIdx' to
 * zero and keep calling this routine to get the attributes of all
 * sets in the domain.
 */

statusT
bs_bfs_get_info(
    uint64_t *nextSetIdx,      /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    domainT *dmnP,             /* in - domain pointer */
    uint32_t *userId           /* out - bfset user id */
    )
{
    bfTagT setTag;
    statusT sts;
    bfMCIdT mcid;
    bfSetT *setp;
    bfSetT *bfSetp;
    bfAccessT *bfap;
    bsBfSetAttrT *setAttrp = NULL;
    bsQuotaAttrT quotaAttr;
    struct vnode *nullvp = NULL;

    if (dmnP == NULL){
        RAISE_EXCEPTION(EBAD_DOMAIN_POINTER);
    }

    setAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));

    bfSetp = dmnP->bfSetDirp;

    setTag.tag_seq = 0;
    setTag.tag_num = (*nextSetIdx == 0) ? 1 : *nextSetIdx;

    sts = tagdir_lookup2( dmnP->bfSetDirp, &setTag, &mcid );
    *nextSetIdx = setTag.tag_num + 1;

    if (sts == ENO_SUCH_TAG) {
        RAISE_EXCEPTION(E_NO_MORE_SETS);

    } else if (sts != EOK && (sts != E_OUT_OF_SYNC_SNAPSHOT)) {
        RAISE_EXCEPTION(sts);
    }

    /*
     * The set exists. Open it and get its attributes.
     */

    sts = bs_access( &bfap, 
            setTag, 
            dmnP->bfSetDirp, 
            FtxNilFtxH, 
            BF_OP_INTERNAL,
            &nullvp );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    sts = bmtr_get_rec(bfap,
                       BSR_BFS_ATTR,
                       (void *) setAttrp,
                       (uint16_t) sizeof( bsBfSetAttrT ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if ( !BS_UID_EQL(setAttrp->bfSetId.domainId, dmnP->domainId) ) {
        if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
            if ( BS_UID_EQL(setAttrp->bfSetId.domainId, dmnP->dualMountId) ) {
                setAttrp->bfSetId.domainId = dmnP->domainId;
            }
            else {
                RAISE_EXCEPTION(ENO_SUCH_DOMAIN);
            }
        }
        else {
            RAISE_EXCEPTION(ENO_SUCH_DOMAIN);
        }
    }

    sts = bmtr_get_rec(bfap,
                       BSR_BFS_QUOTA_ATTR,
                       (void *) &quotaAttr,
                       (uint16_t) sizeof( quotaAttr ) );
    if (sts != EOK) {
        bzero( &quotaAttr, sizeof( quotaAttr ) );
    }

    *bfSetParams                 = bsNilBfSetParams;
    bfSetParams->bfSetId         = setAttrp->bfSetId;
    bfSetParams->fsDev           = setAttrp->fsDev;

    bfSetParams->bfspParentSnapSet   = setAttrp->bfsaParentSnapSet;
    bfSetParams->bfspFirstChildSnap  = setAttrp->bfsaFirstChildSnapSet;
    bfSetParams->bfspNextSiblingSnap = setAttrp->bfsaNextSiblingSnap;
    bfSetParams->bfspSnapLevel       = setAttrp->bfsaSnapLevel;
    bfSetParams->bfspCreateTime      = setAttrp->bfsaFilesetCreate;

    bfSetParams->mode            = setAttrp->mode;
    bfSetParams->uid             = setAttrp->uid;
    bfSetParams->gid             = setAttrp->gid;
    bfSetParams->bfSetFlags      = setAttrp->flags;
    if (setTag.tag_flags & BS_TD_OUT_OF_SYNC_SNAP) {
        bfSetParams->bfSetFlags |= BFS_OD_HARD_OUT_OF_SYNC;
    }
    bfSetParams->quotaStatus     = quotaAttr.quotaStatus;
    bfSetParams->blkHLimit       = quotaAttr.blkHLimit;
    bfSetParams->blkSLimit       = quotaAttr.blkSLimit;
    bfSetParams->fileHLimit      = quotaAttr.fileHLimit;
    bfSetParams->fileSLimit      = quotaAttr.fileSLimit;
    bfSetParams->blkTLimit       = quotaAttr.blkTLimit;
    bfSetParams->fileTLimit      = quotaAttr.fileTLimit;
    bfSetParams->filesUsed       = 0;
    bfSetParams->blksUsed        = 0;
    bfSetParams->dmnAvailFiles   = bs_get_avail_mcells( dmnP );
    bfSetParams->dmnTotalBlks    = dmnP->totalBlks;
    bfSetParams->dmnAvailBlks    = dmnP->freeBlks;

    setp = bs_bfs_lookup_desc( setAttrp->bfSetId );

    if ( BFSET_VALID(setp) ) {
        if (setp->fsnp != NULL) {
            struct fileSetNode *fsnp = setp->fsnp;

           /*
            * If bfSetT for a fileset is valid,
            * override the first value from disk with
            * in-memory value because the changed value
            * in-memory should be applied immediately
            * for a proper operation(s).
            */
            bfSetParams->bfSetFlags = setp->bfSetFlags;

            bfSetParams->quotaStatus = fsnp->quotaStatus;


            if ((fsnp->quotaStatus & QSTS_QUOTA_ON) &&
                (fsnp->quotaStatus & QSTS_FS_QUOTA_MT)) {
                bfSetParams->filesUsed = fsnp->filesUsed;
                bfSetParams->blksUsed  = fsnp->blksUsed;

            } else {
                bfSetParams->filesUsed = setp->bfCnt;
            }

        } else {
            bfSetParams->filesUsed = setp->bfCnt;
        }
    }

    bcopy( setAttrp->setName, bfSetParams->setName, (size_t) BS_SET_NAME_SZ );

    *userId = 0;

    bs_close(bfap, MSFS_CLOSE_NONE);

    ms_free( setAttrp );
    return EOK;

HANDLE_EXCEPTION:
    if (setAttrp != NULL) {
        ms_free( setAttrp );
    }
    return sts;
}

/*
 * bs_bfs_find_set
 *
 * Given a bitfile set name this routine will determine if a
 * set by that name exists in the domain.
 */

static statusT
bs_bfs_find_set(
    char *setName,             /* in - name of set to find */
    domainT *dmnP,             /* in - domain pointer */
    uint64_t doingRoot,        /* in - flag */
    bfSetParamsT *setParams    /* out - the bitfile-set's parameters */
    )
{
    statusT sts;
    uint64_t setIdx;
    int done = FALSE;
    uint32_t t1;                /* unused output param */

    setIdx = 0;

    do {
        sts = bs_bfs_get_info( &setIdx, setParams, dmnP, &t1 );
        if (sts == EOK) {
            if (strcmp( setName, setParams->setName ) == 0 ||
                doingRoot) {
                done = TRUE;
            }
        }
    } while (!done && (sts == EOK));

    if (sts != EOK) {
        return E_NO_SUCH_BF_SET;
    } else {
        return EOK;
    }
}

/****************************************************************************
 **  Bitfile cloning and copy on write routines
 ***************************************************************************/


/*
 * bs_get_bfset_params
 *
 * Get the parameters for a bitfile-set
 *
 * Returns E_BAD_BF_SET_POINTER, possible status
 * from bmtr_scan or bs_derefpg
 */

statusT
bs_get_bfset_params(
    bfSetT *bfSetp,            /* in - the bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    int lock                   /* in - not used */
    )
{
    statusT sts = EOK;
    bsBfSetAttrT bfsAttr;
    bsQuotaAttrT quotaAttr;

    /*
     * Check validity of descriptor pointer.
     */
    if ( ! BFSET_VALID(bfSetp) ) {
        RAISE_EXCEPTION(E_BAD_BF_SET_POINTER);
    }

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &bfsAttr,
                       (uint16_t) sizeof( bfsAttr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_QUOTA_ATTR,
                       &quotaAttr,
                       (uint16_t) sizeof( quotaAttr ) );
    if (sts == EBMTR_NOT_FOUND) {
        bzero( &quotaAttr, sizeof( quotaAttr ) );
    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    *bfSetParams                 = bsNilBfSetParams;
    bfSetParams->bfSetId         = bfsAttr.bfSetId;

    bfSetParams->bfspParentSnapSet      = bfsAttr.bfsaParentSnapSet;
    bfSetParams->bfspFirstChildSnap     = bfsAttr.bfsaFirstChildSnapSet;
    bfSetParams->bfspNextSiblingSnap    = bfsAttr.bfsaNextSiblingSnap;
    bfSetParams->bfspSnapLevel          = bfsAttr.bfsaSnapLevel;
    bfSetParams->bfspCreateTime         = bfsAttr.bfsaFilesetCreate;

    bfSetParams->mode            = bfsAttr.mode;
    bfSetParams->uid             = bfsAttr.uid;
    bfSetParams->gid             = bfsAttr.gid;
    bfSetParams->bfSetFlags      = bfsAttr.flags;
    if (bfSetp->bfSetFlags & BFS_OD_HARD_OUT_OF_SYNC) {
        bfSetParams->bfSetFlags |= BFS_OD_HARD_OUT_OF_SYNC;
    }
    bfSetParams->quotaStatus     = quotaAttr.quotaStatus;

    bfSetParams->blkHLimit       = quotaAttr.blkHLimit;
    bfSetParams->blkSLimit       = quotaAttr.blkSLimit;
    bfSetParams->fileHLimit      = quotaAttr.fileHLimit;
    bfSetParams->fileSLimit      = quotaAttr.fileSLimit;
    bfSetParams->blkTLimit       = quotaAttr.blkTLimit;
    bfSetParams->fileTLimit      = quotaAttr.fileTLimit;
    bfSetParams->filesUsed       = bfSetp->bfCnt;
    bfSetParams->blksUsed        = 0;
    bfSetParams->dmnAvailFiles   = bs_get_avail_mcells( bfSetp->dmnP );
    bfSetParams->dmnTotalBlks    = bfSetp->dmnP->totalBlks;
    bfSetParams->dmnAvailBlks    = bfSetp->dmnP->freeBlks;
    if ( (bfSetp->fsnp != NULL) && !(bfSetp->fsnp->quotaStatus & QSTS_INACTIVE) )  {
        struct fileSetNode *fsnp = bfSetp->fsnp;

        /*
         * If bfSetT for a fileset is valid,
         * override the first value from disk with 
         * in-memory value because the changed value
         * in-memory should be applied immediately
         * for a proper operation(s).
         */
        bfSetParams->bfSetFlags = bfSetp->bfSetFlags;

        bfSetParams->quotaStatus = fsnp->quotaStatus;

        if ((fsnp->quotaStatus & QSTS_QUOTA_ON) &&
            (fsnp->quotaStatus & QSTS_FS_QUOTA_MT)) {
            bfSetParams->filesUsed = fsnp->filesUsed;
            bfSetParams->blksUsed  = fsnp->blksUsed;
        }
    }

    bcopy( bfsAttr.setName, bfSetParams->setName, (size_t) BS_SET_NAME_SZ );

HANDLE_EXCEPTION:
    return sts;
}

statusT
bs_set_bfset_params(
    bfSetT *bfSetp,            /* in - bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams, /* in - bitfile-set's params */
    ftxIdT xid                 /* in - CFS transaction id */
    )
{
    return rbf_set_bfset_params( bfSetp, bfSetParams, FtxNilFtxH, xid );
}

/*
 * rbf_set_bfset_params
 *
 * This routine sets the writable bitfile-set parameters in the bitfile-set
 * metadata.
 *
 * NOTE: The file system context struct is written, and it is
 * only written to the stable metadata - any "in-memory" copy of it
 * will not be affected.
 *
 * Returns E_BAD_BF_SET_POINTER, possible error return from
 * bmtr_scan, bs_deref_pg, bs_pinpg, or bs_unpinpg.
 */

statusT
rbf_set_bfset_params(
    bfSetT *bfSetp,             /* in - bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams,  /* in - bitfile-set's params */
    ftxHT parentFtxH,
    ftxIdT  xid                 /* in - CFS transaction id */
    )
{
    statusT sts = EOK;
    bsBfSetAttrT bfsAttr;
    bsQuotaAttrT quotaAttr;
    int ftxStarted = FALSE,
        bfSetTblLocked = FALSE,
        mcellListLocked = FALSE,
        exclusiveFtx = FALSE;
    ftxHT ftxH;

    /*
     * Check validity of descriptor pointer.
     */
    if ( ! BFSET_VALID(bfSetp) ) {
        RAISE_EXCEPTION(E_BAD_BF_SET_POINTER);
    }

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        /* Probably a read-only snapshot */
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    if ((bfSetParams->blkSLimit < 0) ||
        (bfSetParams->blkHLimit < 0)) {
        /* only support 63-bit block limits */
        RAISE_EXCEPTION(EINVAL);
    }

    if (parentFtxH.hndl == 0) {
        /* valid xids (xid>0) only come from bs_set_bfset_params(), *
         * which always starts a new transaction.                   */
        sts = FTX_START_EXC_XID( FTA_BS_SET_BFSET_PARAMS,
                                 &ftxH, bfSetp->dmnP, xid );
        exclusiveFtx = TRUE;
    } else {
        sts = FTX_START_N( FTA_BS_SET_BFSET_PARAMS,
                           &ftxH, parentFtxH, bfSetp->dmnP );
    }

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    ftxStarted = TRUE;

    if (!ADVRWL_BFSETTBL_ISWRLOCKED( (bfSetp->dmnP) )) {
        /*
         * fs_fset_name_change() calls this routine with the
         * BfSetTblLock locked so we don't need to lock it here.
         */
        /*
         * A more careful analysis is required to determine if this is
         * correct assert.  We cannot check to see if we hold this lock for
         * anything other than write, but I believe that we should only be
         * calling bs_bfs_close if we hold the write lock or do not hold the
         * lock at all.  If this is hit, we will need to pass some flag
         * indicating the lock state.
         */
        MS_SMP_ASSERT(
	    ADVFS_SMP_RW_LOCK_EQL(&bfSetp->dmnP->BfSetTblLock.lock.rw, 
                                   RWL_UNLOCKED));
        ADVRWL_BFSETTBL_WRITE( bfSetp->dmnP );
        bfSetTblLocked = TRUE;
    }

    /* basic fileset attributes */

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            (void *) &bfsAttr,
                            (uint16_t) sizeof( bfsAttr ),
                            BMTR_LOCK );
    if (sts != EOK) {
        sts = EBMTR_NOT_FOUND;
        goto the_end;
    }
    mcellListLocked = TRUE;

    bfsAttr.uid = bfSetParams->uid;
    bfsAttr.gid = bfSetParams->gid;
    bfsAttr.mode = bfSetParams->mode;
    bfsAttr.flags = bfSetParams->bfSetFlags;

    bcopy( bfSetParams->setName, bfsAttr.setName, (size_t) BS_SET_NAME_SZ );

    sts = bmtr_put_rec_n_unlk( bfSetp->dirBfAp,
                               BSR_BFS_ATTR,
                               &bfsAttr,
                               (uint16_t) sizeof( bfsAttr ),
                               ftxH,
                               BMTR_UNLOCK,
                               (ftxIdT) 0 );
    if (sts != EOK) {
        goto the_end;
    }

    /*
     * Take a simple lock to modify bfSetFlags.
     * Locate statement to change bfSetp->bfSetFlags here so
     * if bmtr_put_rec_n_unlk() above fails, bfSetp->bfSetFlags
     * can still hold old information. If not, the value will
     * be changed.
     */
    ADVSMP_FILESET_LOCK(&bfSetp->bfSetMutex);
    bfSetp->bfSetFlags = bfSetParams->bfSetFlags;
    ADVSMP_FILESET_UNLOCK(&bfSetp->bfSetMutex);

    mcellListLocked = FALSE;

    /* now deal with the quota attributes */

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_QUOTA_ATTR,
                            (void *) &quotaAttr,
                            (uint16_t) sizeof( quotaAttr ),
                            BMTR_LOCK );
    if (sts != EOK) {
        sts = EBMTR_NOT_FOUND;
        goto the_end;
    }
    else {
        mcellListLocked = TRUE;
    }

    /* set the time limits now; they may be overridden below */
    quotaAttr.blkTLimit = bfSetParams->blkTLimit;
    quotaAttr.fileTLimit = bfSetParams->fileTLimit;

    if (bfSetp->fsnp != NULL) {
        time_t sec;
        struct timeval tv;
        struct fileSetNode *fsnp = bfSetp->fsnp;
        int32_t ncur;

        tv = get_system_time();
        sec = tv.tv_sec;

        /*
         * rules for updating the grace period - update if new
         * soft limit is exceeded and there either wasn't a softlimit
         * before or the soft limit was not exceeded before.  basically,
         * the grace period should not be changed if it is already in affect.
         */

        if (bfSetParams->blkSLimit &&
           (fsnp->blksUsed >= bfSetParams->blkSLimit) &&
           (!(fsnp->blkSLimit) ||
            fsnp->blksUsed < fsnp->blkSLimit)) {

            fsnp->blkTLimit     = sec + fsnp->qi[GRPQUOTA].qiBlkTime;
            quotaAttr.blkTLimit = fsnp->blkTLimit;
        }

        if (bfSetParams->fileSLimit &&
           (fsnp->filesUsed >= bfSetParams->fileSLimit) &&
           (!(fsnp->fileSLimit) ||
            fsnp->filesUsed < fsnp->fileSLimit)) {

            fsnp->fileTLimit     = sec + fsnp->qi[GRPQUOTA].qiFileTime;
            quotaAttr.fileTLimit = fsnp->fileTLimit;
        }

        /*
         * BMT quota attributes
         * no longer have the QSTS_QUOTA_ON, QSTS_USR_QUOTA_EN, or
         * QSTS_GRP_QUOTA_EN flags set.  So when we compare the
         * in-memory and on-disk quota stati here, we ignore
         * discrepancies due only to these flags.
         * We must have an EXCL Ftx to change quotaStatus.
         */
        if (quotaAttr.quotaStatus &&
            ((quotaAttr.quotaStatus & 
              ~(QSTS_QUOTA_ON | QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN)) != 
             (bfSetParams->quotaStatus & 
              ~(QSTS_QUOTA_ON | QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN)))) 
          {
            if (!exclusiveFtx) {
              sts = EBAD_FTXH;
              goto the_end;
            }
            fsnp->quotaStatus = bfSetParams->quotaStatus;
          }
        
        fsnp->fileHLimit  = bfSetParams->fileHLimit;
        fsnp->blkHLimit   = bfSetParams->blkHLimit;
        fsnp->fileSLimit  = bfSetParams->fileSLimit;
        fsnp->blkSLimit   = bfSetParams->blkSLimit;

        if (fsnp->blksUsed < fsnp->blkSLimit) {
            ATOMIC_FETCH_BIT_AND( &fsnp->fsFlags, ~FS_DQUOT_BLKS, &ncur );
        }

        if (fsnp->filesUsed < fsnp->fileSLimit) {
            ATOMIC_FETCH_BIT_AND( &fsnp->fsFlags, ~FS_DQUOT_FILES, &ncur );
        }
    }

    quotaAttr.blkHLimit = bfSetParams->blkHLimit;
    quotaAttr.blkSLimit = bfSetParams->blkSLimit;

    quotaAttr.fileHLimit = bfSetParams->fileHLimit;
    quotaAttr.fileSLimit = bfSetParams->fileSLimit;

    /*
     * we do not write
     * quota enforcement flags or "quotas are on" flags to the BMT.
     */
    quotaAttr.quotaStatus  = bfSetParams->quotaStatus &
                            ~(QSTS_QUOTA_ON |
                              QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);

    sts = bmtr_put_rec_n_unlk( bfSetp->dirBfAp,
                               BSR_BFS_QUOTA_ATTR,
                               &quotaAttr,
                               (uint16_t) sizeof( quotaAttr ),
                               ftxH,
                               mcellListLocked ? BMTR_UNLOCK : BMTR_NO_LOCK,
                               (ftxIdT) 0 );
    if (sts != EOK) {
        goto the_end;
    }

    if (bfSetTblLocked) {
        ADVRWL_BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    ftx_done_n( ftxH, FTA_BS_SET_BFSET_PARAMS );
    return EOK;

the_end:

    if (bfSetTblLocked) {
        ADVRWL_BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    if (mcellListLocked) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfSetp->dirBfAp->mcellList_lk) );
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

HANDLE_EXCEPTION:
    return sts;
}


static int64_t
bs_get_avail_mcells(
    domainT *dmnP               /* in - The domain pointer */
    )
{
    vdIndexT vdi;
    int vdcnt;
    bf_vd_blk_t freeBlks;
    long potentialMcells;  /* number of mcells that could be allocated */
    long availMcells = 0;
    struct vd* vdp;
    bfAccessT *bmtap;

    /*
     * For each disk in the domain we calculate the number of mcells and
     * roll it up into a per domain number of mcells.
     */

    vdcnt = vdi = 1;

    while ((vdi <= BS_MAX_VDI) && (vdcnt <= dmnP->vdCnt )) {

        if ( vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE) ) {
            /*
             * NOTE:  I don't acquire the storage bitmap lock because
             * vdp->freeClust could change as soon as I release the lock.
             * In addition, vdp->freeClust is the only changeable vd field
             * that I use.  If I grabbed multiple changeable fields from
             * the vd, I would acquire the appropriate locks so that the
             * fields would be in sync relative to each other.
             */
            freeBlks = vdp->freeClust * vdp->stgCluster; 

            /* total mcells (number of pages times mcells per page) */
            bmtap = vdp->bmtp;

            ADVRWL_XTNTMAP_READ( &(bmtap->xtntMap_lk) );

            /*
             * The number of potential bitfiles is limited by the
             * space available for tags and the space available for
             * mcells.  Good luck trying to understand the following
             * computation.  It amounts to multiplying the number of 
             * pages by the harmonic mean of BS_TD_TAGS_PG and BSPG_CELLS.
             */
            potentialMcells = ((freeBlks / bmtap->bfPageSz) * BS_TD_TAGS_PG
                               * BSPG_CELLS) / (BS_TD_TAGS_PG + BSPG_CELLS);

            availMcells += (bmtap->bfaNextFob / ADVFS_METADATA_PGSZ_IN_FOBS) * 
                BSPG_CELLS + potentialMcells;

            ADVRWL_XTNTMAP_UNLOCK( &(bmtap->xtntMap_lk) );

            /* decrement vdRefCnt on vdp before going on to next disk */
            vd_dec_refcnt( vdp );
            vdcnt++;
        }

        vdi++; 
    }

    /*
     * only report up to the current max our tag files can handle.
     */
    if (availMcells > MAX_ADVFS_TAGS) {
    
        availMcells = MAX_ADVFS_TAGS;
    }

    return availMcells;
}


/* end bs_bitfile_sets.c */
