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
 *    Advfs Storage System
 *
 * Abstract:
 *
 *    bs_snapshot.c 
 *    This file contains the primary routines for dealing with snapshots on
 *    AdvFS.
 *
 * Date:
 *
 *    November 24, 2003
 */


#define ADVFS_MODULE BS_SNAPSHOT 
/* Includes */
#include <bs_public.h>

#include <sys/buf.h>
#include <sys/spin.h>
#include <sys/mutex.h>
#include <sys/condvar.h>
#include <sys/fcache.h>
#include <sys/param.h>

#include <ms_generic_locks.h>
#include <ms_logger.h>
#include <ftx_public.h>
#include <bs_ods.h>
#include <bs_ims.h>
#include <bs_extents.h>
#include <vfast.h>
#include <bs_access.h>
#include <bs_inmem_map.h>
#include <bs_migrate.h>
#include <bs_ims.h>
#include <fs_dir_routines.h>
#include <bs_snapshot.h>
#include <tcr/clu.h>
#include <fs/vfs_ifaces.h>            /* struct nameidata,  NAMEIDATA macro */
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

/* Type defs */


struct snap_lock_list {
    int32_t     sll_size;
    int32_t     sll_cnt;
    bfAccessT** sll_bfaps;
};

typedef struct snap_lock_list snap_lock_list_t;

/* Prototypes */

static
statusT
advfs_get_first_extent_mcell(
        bfTagT          bf_set_tag,
        bfAccessT*      bfap,
        bf_fob_t        fob_cnt,
        ftxHT           parent_ftx,
        bfMCIdT*        return_mcell_id);


static
statusT
advfs_append_cow_hole( 
        bfAccessT*       bfap,
        bsInMemXtntMapT* xtnt_map,
        bf_fob_t         start_fob,
        bf_fob_t         end_fob,
        ftxHT            parent_ftx);


static
statusT
advfs_insert_cow_hole( 
        bfAccessT*         bfap,
        bsInMemXtntMapT*   xtnt_map, 
        bsInMemXtntDescIdT xtnt_desc_id,
        bf_fob_t           start_fob,
        bf_fob_t           end_fob,
        ftxHT              parent_ftx);

static
statusT
advfs_snap_migrate_setup_recursive(
        bfAccessT*        bfap,
        bfSetT*           bf_set_ptr,
        snap_lock_list_t* snap_list);


static
statusT
advfs_add_cow_hole_common(
        bfAccessT*         bfap, 
        bsInMemXtntMapT*   xtnt_map, 
        bf_fob_t           start_fob,
        bf_fob_t           end_fob,
        ftxHT              parent_ftx);


static
statusT
advfs_get_new_sub_xtnt(
        bfAccessT*         bfap,
        bsInMemXtntMapT*   xtnt_map,
        ftxHT              parent_ftx );


static
statusT
advfs_snap_io_wait_error_helper( 
        struct ioanchor*        io_anchor,
        bfAccessT*              bfap,
        off_t*                  error_offset,
        extent_blk_desc_t**     snap_maps,
        ftxHT                   parent_ftx);


static
statusT
advfs_snapset_access_recursive(
        bfSetT*         bf_set_p,
        bfSetT*         parent_set_p,
        bfSetIdT        cur_bf_set_id,
        int32_t*        num_children,
        snap_flags_t*   snap_flags,
        ftxHT           parent_ftx);


static
statusT
advfs_new_snap_mcell(
        bfMCIdT*        new_mcell,
        domainT*        domain_p,
        ftxHT           parent_ftx,
        bsBfAttrT*      bf_attr,
        bfSetT          *bf_set_p,
        bfTagT          tag);

static
statusT
advfs_snap_mcell(
        bsMRT           *rec_prt,
        vdT             *cur_vd_p,
        bfMCIdT         *child_prim_mcell,
        ftxHT           parent_ftx,
        int             first_call);

static
statusT
advfs_link_snapsets(
        bfSetT*         parent_set_p,
        bfSetT*         child_set_p);

static
advfs_check_snap_perms(
        char   *parent_dmn_name,
        char   *parent_fset_name,
        bfSetT **parent_bf_set);

static
statusT
advfs_copy_tagdir(
        bfSetT *parent_bf_set_p,        /* bfSetT pointer of parent fileset */
        bfSetT *snap_bf_set_p,          /* bfSetT pointer of snap set */
        ftxHT   parent_ftx);            /* Transaction of parent_ftx */

static
statusT
advfs_link_snapsets_full(
        bfSetT *parent_bf_set_p,    /* bfSetT pointer of parent fileset */
        bfSetT *child_bf_set_p,     /* bfSetT pointer of child fileset */
        snap_flags_t snap_flags,    /* Flags to indicate read or write snapshot*/
        ftxHT parent_ftx);          /* Transaction to use for updates */

static
statusT
advfs_snap_protect_cache(
        bfSetT *parent_bf_set_p);   /* bfSetT pointer of parent fileset */


static
statusT
advfs_snap_drain_writes(
        bfSetT *parent_bf_set_p);   /* bfSetT pointer of parent fileset */

static
statusT
advfs_close_snap_parents(
        bfAccessT*  bfap);

static
statusT
advfs_snapset_close_recursive(
        bfSetT* bf_set_p,
        bfSetT* cur_set_p,
        ftxHT   parent_ftx);

opxT advfs_bs_bfs_snap_link_undo_opx;
void
advfs_bs_bfs_snap_link_undo_opx(
        ftxHT       ftxH,
        int         opRecSz,
        void*       opRecp);


/* Structures */
struct undoSnapLinkRec {
    bfSetIdT usp_bf_set_id;
    bfSetIdT usp_snap_set_id;
};

typedef struct undoSnapLinkRec undoSnapLinkRecT;


/* Globals */
uint64_t        advfs_cow_alloc_units = ADVFS_COW_ALLOC_UNIT_DEFAULT;

/* Used for assertions */
#define VM_PAGE_SZ (uint64_t)NBPG

/*
 * Required for BS_GET_FSID
 */
extern int advfs_fstype;


/*
 * Snapshot stats
 */
#ifdef ADVFS_SNAP_STATS
advfs_snap_stats_t advfs_snap_stats;
#endif

/* Function Definitions */


/*
 * BEGIN_IMS
 *
 * advfs_access_snap_parents - This routine access all of the parent
 * snapshots of bfap.
 *
 * Parameters:
 * bfap - the access structure of the file to have its children opened.
 * bfSetp - the fileset of bfap.
 * parent_ftx - the parent transaction under which to perform the access.
 * 
 * Return Values:
 *      EIO - Accessing a parent failed 
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and valid
 *      On entry, no locks are held. 
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_access_snap_parents(
        bfAccessT*      bfap,
        bfSetT*         bf_set_ptr,
        ftxHT           parent_ftx)
{
    statusT     sts = EOK;
    bfAccessT*  child_bfap;
    bfAccessT*  parent_bfap;
    bf_fob_t    logical_next_fob = 0; /* What next fob would be for bfap if 
                                       * it were fully populated by it's
                                       * parent's extents. */
    int32_t     track_next_fob = TRUE;


    /* This is a racy check */
    if ( (bfap->bfaFlags & BFA_PARENT_SNAP_OPEN) ||
         (bfap->bfaFlags & BFA_ROOT_SNAPSHOT) ) {
        /* The parents are already opened... nothing to do */
        SNAP_STATS( access_snap_parent_already_open );
        return EOK;
    }

    ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );

    /* We check now under the snap lock.  This is due to racing threads 
     * and one thread dropping the snap lock at the end of this routine. 
     * The other thread could have already gone past the racy check and 
     * is now waiting on the snap lock (just dropped by the first thread).
     */
    if ( (bfap->bfaFlags & BFA_PARENT_SNAP_OPEN) ||
         (bfap->bfaFlags & BFA_ROOT_SNAPSHOT) ) {
        /* The parents are already opened... nothing to do */
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        SNAP_STATS( access_snap_parent_already_open );
        return EOK;
    }

    /*
     * Set the BFA_OPENING_PARENTS flag to lock out any other threads also
     * trying to access the parents.  We don't want to hold any locks while
     * accessing the parents.
     */
    if (bfap->bfaFlags & BFA_OPENING_PARENTS) {
        SNAP_STATS( access_snap_parent_raced_another_open );
        while (bfap->bfaFlags & BFA_OPENING_PARENTS) {
            cv_wait( &bfap->bfaSnapCv,
                     &bfap->bfaSnapLock,
                     CV_RWLOCK_WR,
                     CV_DFLT_FLG );
            if (bfap->bfaFlags & BFA_PARENT_SNAP_OPEN) {
                /* Another thread must have succeeded... Yay!
                 * Just unlock the snap lock and return success.*/
                MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_OPENING_PARENTS) );
                ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                return EOK;
            }
        } 

        /* 
         * The BFA_OPENING_PARENTS flag is now not set but the parent wasn't
         * opened.  THis means another thread tried to open the parents and
         * failed.  That isn't a good sign that this thread will succeed,
         * but try anyway.
         */
        SNAP_STATS( access_snap_parent_racer_failed );
    }


    /*
     * Set the BFA_OPENING_PARENTS flag to fend off racing threads.
     */
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    bfap->bfaFlags |= BFA_OPENING_PARENTS;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    ADVRWL_BFAP_SNAP_UNLOCK( bfap );


    /*
     * Begin accessing the parents 
     */
    child_bfap = bfap;
    while ( HAS_SNAPSHOT_PARENT( child_bfap ) ) {
        sts = bs_access_one( &parent_bfap,
                                child_bfap->tag,
                                child_bfap->bfSetp->bfsParentSnapSet,
                                parent_ftx,
                                BF_OP_INTERNAL|BF_OP_SNAP_REF|BF_OP_OVERRIDE_SMAX);
        if (sts == ENO_SUCH_TAG) {
            SNAP_STATS( access_snap_parent_e_no_such_tag );
            /*
             * child_bfap should be a root bfap if it has no parent. 
             */
            MS_SMP_ASSERT( child_bfap->bfaFlags & BFA_ROOT_SNAPSHOT );
            /* 
             * If the parent doesn't exist, it must have been deleted.  If
             * it was deleted, all the data was COWed.  Now we can stop
             * opening parents.
             */
            sts = EOK;
            break;
        }
        if (sts != EOK) {
            /* We got a real error.  We need to close all the parents we've
             * opened so far and return the error.
             */
            bfAccessT* next_parent = NULL;

            SNAP_STATS( access_snap_parent_error_1 );
           
            parent_bfap = bfap->bfaParentSnap;
            while (parent_bfap != NULL) {
                next_parent = parent_bfap->bfaParentSnap;
                (void)bs_close_one( parent_bfap,
                        MSFS_SNAP_DEREF,
                        parent_ftx );
                parent_bfap = next_parent;

                SNAP_STATS( access_snap_parent_closing_parents );
               
            }

            /* Clear the BFA_OPENING_PARENTS flag */
            ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags &= ~BFA_OPENING_PARENTS;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
            ADVRWL_BFAP_SNAP_UNLOCK( bfap );

            /* Let people know we are done trying to open the parents */
            cv_broadcast( &bfap->bfaSnapCv, NULL, CV_NULL_LOCK );

            return sts;
        }

        MS_SMP_ASSERT(!(bfap->bfaFlags & BFA_PARENT_SNAP_OPEN));
        ADVFS_ACCESS_LOGIT( parent_bfap, 
                "advfs_access_snap_parents accessed bfap as a parent" );


        /*
         * The bfaNextFob field needs to be correctly set from the parents 
         * so that metadata can have the orig_file_size set.  We want to get
         * the bfaNextFob of the first non-virign snapshot.  If child_bfap
         * has already been cowed, then it's orig_file_size will be store
         * the value we need.
         */
        if (track_next_fob) {
            if (child_bfap->bfaFlags & BFA_VIRGIN_SNAP) { 
                if (parent_bfap->bfaFlags & BFA_ROOT_SNAPSHOT) {
                    logical_next_fob = parent_bfap->bfaNextFob;
                } else {
                    /* The parent isn't fully populated by has it's own
                     * metadata so it's bfa_orig_file_size must be correct.
                     * This will be used to set the bfaNextFob. */
                    logical_next_fob = roundup( 
                            ADVFS_OFFSET_TO_FOB_UP( parent_bfap->bfa_orig_file_size),
                            parent_bfap->bfPageSz );
                } 
                track_next_fob = FALSE;
            }
        }

        /*
         * Link the child to the parent.  Do not link the parent to the
         * child until necessary (a write occurs)
         */
        ADVRWL_BFAP_SNAP_WRITE_LOCK( child_bfap );
        child_bfap->bfaParentSnap = parent_bfap;
        ADVRWL_BFAP_SNAP_UNLOCK( child_bfap );

        /* Move up the chain.  If we hit a root snapshot, break. */
        child_bfap = parent_bfap;
        if (child_bfap->bfaFlags & BFA_ROOT_SNAPSHOT) {
            break;
        }
    }

    ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    bfap->bfaFlags |= BFA_PARENT_SNAP_OPEN;
    bfap->bfaFlags &= ~BFA_OPENING_PARENTS;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );


    /* 
     * If bfap is a metadata file, then it's file_size and bfa_orig_file_size
     * are probably not correctly set if it hasn't been COWed yet.  Set them now.  
     */
    if ( IS_COWABLE_METADATA( bfap ) && (bfap->bfaFlags & BFA_VIRGIN_SNAP) ) {
        bfap->bfa_orig_file_size = logical_next_fob * ADVFS_FOB_SZ;
    }

    ADVRWL_BFAP_SNAP_UNLOCK( bfap );

    cv_broadcast( &bfap->bfaSnapCv, NULL, CV_NULL_LOCK );


    return sts;

}

/*
 * BEGIN_IMS
 *
 * advfs_access_snap_children - This routine is used to access all child
 * snapshots of a file.  This routine does not access beyond the immediate
 * children. 
 *
 * Parameters:
 * bfap - the access structure of the file to have its children opened.
 * parent_ftx - the parent transaction under which to perform the access.
 * 
 * Return Values:
 *      EIO - Accessing a child failed and so did setting the "OUT OF SYNC"
 *      flag.  The domain has paniced.
 *
 *      
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and valid and the parents are open if they
 *      exist. 
 *      On entry, the bfaSnapLock of bfap is not held
 *      On entry, the BFA_SNAP_CHANGE flag is set if the bfap may have
 *      children to be accessed.
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_access_snap_children(
        bfAccessT*      bfap,
        ftxHT           parent_ftx,
        snap_flags_t*   snap_flags)
{
    bfSetT      *cur_bf_set = NULL;
    bfAccessT   *cur_bfap = NULL;
    bfAccessT   *prev_bfap = NULL;
    statusT     sts = EOK;
    int32_t     ftx_started = FALSE;
    int32_t     snap_lock_locked = FALSE;

    MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_OPENING_PARENTS) );
    

    /*
     * If BFA_NO_SNAP_CHILD is set we have already looked on disk and know
     * there is nothing to do.
     */
    if (bfap->bfaFlags & BFA_NO_SNAP_CHILD) {
        return EOK;
    }

    if ( FTX_EQ( parent_ftx, FtxNilFtxH ) )  {
        /* 
         * If no transaction was passed in, then start one so that we don't
         * deadlock with the bfaSnapLock.  
         */
        sts = FTX_START_N( FTA_NULL, &parent_ftx, FtxNilFtxH, bfap->dmnP );
        ftx_started = TRUE;
    }


    ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
    snap_lock_locked = TRUE;
    if (!(bfap->bfaFlags & BFA_SNAP_CHANGE) && 
          (bfap->bfaFirstChildSnap != NULL) ) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        SNAP_STATS( access_snap_children_no_work );
        if (ftx_started) {
            ftx_quit( parent_ftx );
        }
        return EOK;
    }

    /*
     * We flush stats at snap create time but reads or
     * other operations may have changed them.  In order to make sure the
     * st_size is up to date when we COW the metadata, make sure to flush
     * any dirty stats to disk now before doing the COW
     */
    if (bfap->bfFsContext.dirty_stats) {
        /*
         * fs_update_stats will start a transaction so if our parent is nil,
         * then drop the bfaSnapLock to prevent a deadlock and hierarchy
         * inversion.
         */
        SNAP_STATS( access_snap_children_flushed_dirty_stats );

        /*
         * We should not have a dirty file size since we flushed the stats at
         * create, we may have a dirty a_time 
         */
        
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_36);
        
        sts = fs_update_stats( &bfap->bfVnode, 
                bfap,
                parent_ftx,
                ADVFS_UPDATE_NO_COW );
        
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_37);

        if (sts != EOK) {
            domain_panic(bfap->bfSetp->dmnP, "advfs_access_snap_children: Failed"
                    " to update stats. Error code = %d", sts);
            if (ftx_started) {
                ftx_quit( parent_ftx );
            }
            return sts;
        }
    }


    cur_bf_set = bfap->bfSetp->bfsFirstChildSnapSet;
    cur_bfap = bfap->bfaFirstChildSnap;

    /* Loop opening each child or verifying that it is already open */
    while (cur_bf_set != NULL) {
        if (cur_bfap && cur_bfap->bfSetp == cur_bf_set) {
            /* The child in cur_bf_set is already open */
            SNAP_STATS( access_snap_children_child_already_open );
            prev_bfap = cur_bfap;
            cur_bfap = cur_bfap->bfaNextSiblingSnap;
        } else {
            /* If we are opening a new child, it should be at the end
             * of the list. This assumes that the children are chained
             * in the same order as the snapsets. If this is the first
             * child, then cur_bfap = NULL */
            MS_SMP_ASSERT (cur_bfap == NULL || 
                    cur_bfap->bfaNextSiblingSnap == NULL ||
                    cur_bf_set->state == BFS_DELETING ||
                    (cur_bf_set->bfSetFlags & BFS_OD_OUT_OF_SYNC));

            if (cur_bf_set->state == BFS_DELETING) {
                /* The fileset is being deleted, skip to the next set */
                cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;
                SNAP_STATS( access_snap_children_child_bf_set_deleting );
                continue;
            } else {
                sts = bs_access_one( &cur_bfap,
                                        bfap->tag,
                                        cur_bf_set,
                                        parent_ftx,
                                        BF_OP_INTERNAL|
                                        BF_OP_OVERRIDE_SMAX|
                                        BF_OP_IGNORE_DEL
                                        );
                if (sts == ENO_SUCH_TAG) {
                    /*
                     * This is probably the case of a writeable snapshot in
                     * which the child was deleted.  Alternately, the parent
                     * was created after the child snapset was created and
                     * we are now writing to the parent for the first time.
                     */
                    cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;
                    SNAP_STATS( access_snap_children_child_doesnt_exist );
                    continue;
                } else if (sts != EOK ) {
                    SNAP_STATS( access_snap_children_out_of_sync_call );
                    sts = advfs_snap_out_of_sync( bfap->tag, 
                                                NULL,
                                                cur_bf_set,
                                                parent_ftx);
                    /* If the status is not EOK, the domain should have
                     * paniced.  If the domain is paniced, just return the
                     * status. */
                    if (sts != EOK) {
                        MS_SMP_ASSERT( bfap->dmnP->dmn_panic );
                        SNAP_STATS( access_snap_children_out_of_sync_failed);
                        if (ftx_started) {
                            ftx_done_n( parent_ftx, FTA_NULL );
                        }
                        if (snap_lock_locked) {
                            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                        }
                        return sts;
                    }
                    cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;
                    continue;
                }
                
                /* 
                 * This child may have already been cleved off from its
                 * parent.  In that case, just close it again.
                 */
                if (cur_bfap->bfaFlags & BFA_ROOT_SNAPSHOT) {
                    ADVFS_ACCESS_LOGIT( cur_bfap, "advfs_access_snap_children skipping child");
                    ADVFS_ACCESS_LOGIT( bfap, "advfs_access_snap_children skipped child");
                    (void)bs_close_one( cur_bfap, MSFS_CLOSE_NONE, parent_ftx );
                    cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;
                    cur_bfap = prev_bfap ? prev_bfap->bfaNextSiblingSnap : NULL; 
                    continue;
                }

                ADVFS_ACCESS_LOGIT( cur_bfap, "advfs_access_snap_children accessed bfap as child" );


                ADVSMP_BFAP_LOCK( &cur_bfap->bfaLock );
                cur_bfap->bfaFlags |= BFA_OPENED_BY_PARENT; 
                ADVSMP_BFAP_UNLOCK( &cur_bfap->bfaLock );

                *snap_flags |= SF_ACCESSED_CHILDREN;
                if ( prev_bfap == NULL) {
                    MS_SMP_ASSERT( bfap->bfaFirstChildSnap == NULL );
                    bfap->bfaFirstChildSnap = cur_bfap; 
                } else {
                    MS_SMP_ASSERT ( prev_bfap->bfaNextSiblingSnap == NULL );
                    prev_bfap->bfaNextSiblingSnap = cur_bfap;
                }

                if ( cur_bfap->bfaFlags & BFA_VIRGIN_SNAP) {
                    ADVRWL_BFAP_SNAP_WRITE_LOCK( cur_bfap );
                    SNAP_STATS( access_snap_children_calling_setup_cow );

                    sts = advfs_setup_cow( bfap, 
                                        cur_bfap, 
                                        SF_SNAP_NOFLAGS,
                                        parent_ftx);
                    ADVRWL_BFAP_SNAP_UNLOCK( cur_bfap );
                    if (sts != EOK) {
                        SNAP_STATS( access_snap_children_setup_failed );

                        sts = advfs_snap_out_of_sync( bfap->tag, 
                                                        bfap,
                                                        cur_bf_set,
                                                        parent_ftx);
                        if (sts != EOK) {
                            MS_SMP_ASSERT( bfap->dmnP->dmn_panic );
                            SNAP_STATS( access_snap_out_of_sync_failed_2 );
                            if (ftx_started) {
                                ftx_done_n( parent_ftx, FTA_NULL );
                            }
                            if (snap_lock_locked) {
                                ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                            }
                            return sts;
                        }
                        cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;
                        prev_bfap = cur_bfap;
                        cur_bfap = cur_bfap->bfaNextSiblingSnap;
                        continue;
                    }
                }

                MS_SMP_ASSERT ( cur_bfap->bfa_orig_file_size != -1 );
                prev_bfap = cur_bfap;
                cur_bfap = cur_bfap->bfaNextSiblingSnap;
            }
      
        }
        cur_bf_set = cur_bf_set->bfsNextSiblingSnapSet;

    }    

    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    if ((bfap->bfaFirstChildSnap == NULL) &&
        (!bfap->bfSetp->bfsFirstChildSnapSet) || 
        ((!bfap->bfSetp->bfsFirstChildSnapSet) &&
        (bfap->bfSetp->bfsFirstChildSnapSet->state != BFS_DELETING) ) ) {
        /*
         * Don't set BFA_NO_SNAP_CHILD flag if the first snapshot child is
         * BFS_DELETING.  This may be the last child and it is going away.
         * We want to not optimize this bfap until the snapset child is
         * completely gone otherwise we could loop trying to fault in a read
         * only page while advfs_bs_delete_fileset_tags is blocked trying to
         * start a transaction.
         */
        bfap->bfaFlags |= BFA_NO_SNAP_CHILD;
    }
    bfap->bfaFlags &= ~BFA_SNAP_CHANGE;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    ADVRWL_BFAP_SNAP_UNLOCK( bfap );

    /*
     * If parent_ftx was passed in, then a transaction was started by this
     * routine.  Finish it now.
     */
    if (ftx_started) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_38);
        ftx_done_n( parent_ftx, FTA_NULL );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_39);
    }


    return EOK;


}

/* BEGIN_IMS
 *
 * advfs_close_snaps - closes any snapshot parents of children that were
 * opened or referenced as a result of a call to advfs_access_snap_parents
 * or advfs_access_snap_children.
 *
 * Parameters:
 * bfap - the access structure that has the associated snapshots to be 
 * closed.
 *
 * Return Values:
 * EOK - All associated snapshots have been closed.
 * 
 * END_IMS
 */
statusT
advfs_close_snaps(
        bfAccessT *bfap,  /* bfap to have associated snapshots closed */
        snap_flags_t snap_flags)  
{

    if (!HAS_RELATED_SNAPSHOT(bfap)) {
        /* Nothing to do ... */
        return EOK;
    }

    ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
    if (bfap->refCnt - 
            (bfap->bfaRefsFromChildSnaps +
             ((bfap->bfaFlags & BFA_OPENED_BY_PARENT) ? 1 : 0)) != 1)
    {

        /* 
         * The above check was to make sure we weren't the last closer.
         * Therefore, the result should not be 0...
         */
        MS_SMP_ASSERT( bfap->refCnt - 
            (bfap->bfaRefsFromChildSnaps +
             ((bfap->bfaFlags & BFA_OPENED_BY_PARENT) ? 1 : 0)) != 0)

        /* This is not the last close of the file */
        SNAP_STATS( close_snaps_not_last_close );
        ADVRWL_BFAP_SNAP_UNLOCK(bfap);
        return EOK;
    }

    ADVFS_ACCESS_LOGIT( bfap, "close_snaps: Doing parent and child close");
    
    if ((bfap->bfaParentSnap) && !(snap_flags & SF_SNAP_SKIP_PARENTS) ) {
        SNAP_STATS( close_snaps_closing_parents );
        (void)advfs_close_snap_parents(bfap);
        bfap->bfaParentSnap = NULL;
    }

    if (bfap->bfaFirstChildSnap) {
        SNAP_STATS( close_snaps_closing_children );
        (void)advfs_close_snap_children(bfap);
        bfap->bfaFirstChildSnap = NULL;
    }
    ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    return EOK;
}

/* BEGIN_IMS
 *
 * advfs_close_snap_parents - traverses up the parent chain and closes each
 * parent file that was opened by advfs_access_snap_parents.
 *
 * Parameters:
 * bfap - access structure whose parent chain will be closed.
 *
 * Return Values:
 * EOK - the parent chain has been closed.
 * 
 * END_IMS
 */
static
statusT
advfs_close_snap_parents(
        bfAccessT *bfap)    /* bfap to have parent snapshots closed */
{
    bfAccessT *cur_parent_bfap = NULL, *next_parent_bfap = NULL;

    MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_EQL(&bfap->bfaSnapLock, RWL_WRITELOCKED));
    if (!(bfap->bfaFlags & BFA_PARENT_SNAP_OPEN)) {
        SNAP_STATS( close_parents_no_work );
        return EOK;
    }
    cur_parent_bfap = bfap->bfaParentSnap;
    while (cur_parent_bfap != NULL) {
        next_parent_bfap = cur_parent_bfap->bfaParentSnap;
        ADVFS_ACCESS_LOGIT(cur_parent_bfap, 
                "close from snapshot");
        (void) bs_close_one( cur_parent_bfap, 
                             MSFS_SNAP_DEREF,
                             FtxNilFtxH);
        cur_parent_bfap = next_parent_bfap;
        SNAP_STATS( close_parents_closed_parent );

    }
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);
    bfap->bfaFlags &= ~BFA_PARENT_SNAP_OPEN;
    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    return EOK;
}

/* BEGIN_IMS
 * advfs_close_snap_children - closes the children of the access structure
 * that were opened by advfs_access_snap_children.
 *
 * Parameters:
 * bfap - access structure whose children will be closed.
 *
 * Return Values:
 * EOK - The children have been closed.
 *
 * END_IMS
 */
statusT
advfs_close_snap_children(
        bfAccessT *bfap)    /* bfap to have children snapshots closed */
{
    bfAccessT *cur_child_bfap = NULL, *next_child_bfap = NULL;

    MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_NEQL(&bfap->bfaSnapLock, RWL_UNLOCKED));
    if (bfap->bfaFirstChildSnap == NULL) {
        SNAP_STATS( close_children_no_work );
        return EOK;
    }

    cur_child_bfap = bfap->bfaFirstChildSnap;
    while (cur_child_bfap != NULL) {
        next_child_bfap = cur_child_bfap->bfaNextSiblingSnap;
        cur_child_bfap->bfaNextSiblingSnap = NULL;
        MS_SMP_ASSERT( cur_child_bfap->refCnt > 0 );
        ADVFS_ACCESS_LOGIT( cur_child_bfap, "close_snap_children calling close");
        (void)bs_close_one(cur_child_bfap, 
                MSFS_SNAP_PARENT_CLOSE, 
                FtxNilFtxH);
        cur_child_bfap = next_child_bfap;
        SNAP_STATS( close_children_closed_children );
    }
    bfap->bfaFirstChildSnap = NULL;

    return EOK;
}

/*
 * BEGIN_IMS
 *
 * advfs_acquire_snap_locks - acquires the bfaSnapLock and migStgLk for
 * parents and children of bfap according to the requirements passed in by
 * the snap_flags parameter (reads and writes are handled differently).
 *
 * Parameters:
 * bfap - the access structure of the file to be locked.
 * snap_flags - Flags indicating whether a read or write locking scheme is
 * to be used.  
 * 
 * Return Values:
 * EOK - Locks were acquired.
 *
 *      
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and its parents and immediate children are
 *      accessed and linked to bfap.
 *
 * Algorithm:
 * For SF_SNAP_READ - the migStgLk of each parent will be acquired for read
 * mode.
 * For SF_SNAP_WRITE - the bfaSnapLock of each child will be acquired in
 * write mode.  The migStgLk of each parent will be acquired for
 * read mode and finally the migStgLk of each child will be acquired for
 * read mode.  
 * 
 * Locks are always acquired from parent to child and all locks of one order
 * are acquired before locks of the next order (bfaSnapLock before
 * migStgLk).
 *
 * END_IMS
 */
statusT
advfs_acquire_snap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx)
{

    bfAccessT   *parent_snaps[ADVFS_MAX_SNAP_DEPTH];
    bfAccessT   *cur_parent = NULL;
    int16_t     high_parent_idx = 0;
    int16_t     cur_idx = 0;

    /* Make sure only SF_SNAP_WRITE or SF_SNAP_READ is set and not both */
    MS_SMP_ASSERT( ((snap_flags & SF_SNAP_WRITE) || (snap_flags & SF_SNAP_READ)) &&
            (!((snap_flags & SF_SNAP_WRITE) && (snap_flags & SF_SNAP_READ))) )


    /* if the bfap is reserved or is a tag directory (in the root fileset,
     * just return as these files aren't cowed anyways */
    if ( (BS_BFTAG_RSVD(bfap->tag)) || 
          (bfap->bfSetp == bfap->dmnP->bfSetDirp) ) {
        SNAP_STATS( acquire_snap_locks_no_work );
        return EOK;
    }

    bzero( parent_snaps, sizeof( parent_snaps ) );

    /* build an array of parents for locking in order */
    cur_parent = bfap->bfaParentSnap;
    high_parent_idx = -1;
    while (cur_parent != NULL) {
        parent_snaps[++high_parent_idx] = cur_parent;
        cur_parent = cur_parent->bfaParentSnap;
    }


    /* 
     * Oh woeful complexity.  We need to hold the bfaSnapLock to walk the
     * list of children in order to take out the migStgLk.  Unfortuantely,
     * the migStgLk needs to be above the bfaSnapLock because of metadata.
     * So, here's what we're gonna do... 
     *
     * Step 1: Acquire parent's bfaSnapLocks since that's the locking order
     * (parents before children)
     * 
     * Step 2: Acquire bfap's bfaSnapLock since that what we want to hold to
     * walk the list of child snapshots. 
     *
     * Step 3: TRY to acquire all the migStgLks.  If we fail, it could only
     * mean that a migrate was in progress on the child.  That is
     * unfortunate since we would prefer to give this write priority over
     * the migrate, but c'est la vie, non?  If we fail to acquire the
     * migStgLk for any child, drop all the locks acquired so far and goto
     * step 1.
     */

lock_the_parents:

    /* lock the parent's bfa snap locks */
    for (cur_idx = high_parent_idx; cur_idx >= 0; cur_idx--) {
        /* this assert may not be valid for children of writeable snapshots */
        MS_SMP_ASSERT( !(parent_snaps[cur_idx]->bfaFlags & BFA_OUT_OF_SYNC) ); 

        /*
         * We dont' want to lock a parent of a BFA_ROOT_SNAPSHOT. That would
         * be distinctly negative.  If we hit that case, we probably have a
         * race condition with advfs_force_cow_and_unlink.
         */
        MS_SMP_ASSERT( !(parent_snaps[cur_idx]->bfaFlags & BFA_ROOT_SNAPSHOT) ||
                        (cur_idx == high_parent_idx) );
        ADVRWL_BFAP_SNAP_READ_LOCK( parent_snaps[cur_idx] );
    }

    /*
     * lock bfap's snap lock
     */
    ADVRWL_BFAP_SNAP_READ_LOCK( bfap );

    /* 
     * acquire the migstglk of any children for adding storage.  For bfap,
     * if this is userdata, the migStgLk was acquired in getpage 
     * (for writes) if it is metadata it should already be held if necessary. 
     */
    if (snap_flags & SF_SNAP_WRITE) {
        /* acquire bfasnaplocks for children. */
        bfAccessT *cur_child = bfap->bfaFirstChildSnap;
        while (cur_child != NULL) {
            if (cur_child->bfaFlags & BFA_OUT_OF_SYNC) {
                /* 
                 * Keep stats that this happend but don't treat it
                 * differently
                 */
                SNAP_STATS( acquire_snap_locks_skipped_out_of_sync_child );
            }
            if ( ADVRWL_MIGSTG_READ_TRY( cur_child ) ) {
                /* 
                 * If we successfully acquired the migStgLk, and we are a
                 * metadata file, then we need to make sure we aren't racing
                 * a migrate since migrate may need to start a transaction
                 * and get the migStgLk. If we are racing a migrate, we will
                 * try to finish the migrate in our transaction so that
                 * migrate doesn't have to.
                 */
                if (cur_child->bfaFlags & BFA_SWITCH_STG) {
                    /* 
                     * Make sure that the parent is a COWable metadata file,
                     * otherwise we shouldnt' be in this case.
                     */
                    MS_SMP_ASSERT( IS_COWABLE_METADATA( bfap ) );
                    /* We now need to try to get the lock for WRITE mode.
                     * If we fail, then we will drop all locks and try to
                     * get it in WRITE mode again.  We may spin trying to
                     * get the lock in WRITE mode, but it is not a deadlock
                     * since migrate will either get a transaction and take
                     * the lock for write, or block waiting for the
                     * transaction that we are in.
                     */
                    if (ADVRWL_MIGSTG_UPGRADE( cur_child ) ) {
                        /* We got the lock easily. Now finish the migrate.
                         * This will let migrate make progress once the
                         * BFA_SWITCH_STG flag is cleared.  Success or
                         * failure of this routine is irrelevant, it will be
                         * dealt with by the racing migrate thread.
                         */
                         (void) advfs_migrate_switch_stg_full( cur_child,
                                                parent_ftx );
                        /* 
                         * Now downgrade the lock and continue 
                         */
                        ADVRWL_MIGSTG_DOWNGRADE( cur_child );

                    } else {
                        /* We couldn't upgrade.  Drop the lock and try to
                         * get the lock for WRITE.  If we still can't get it, 
                         * drop all the locks and try again.  Any one trying
                         * to get the migStgLk for READ will also be able to
                         * finish the migrate which will make progress for
                         * the migrate thread and for this thread.
                         */
                        ADVRWL_MIGSTG_UNLOCK( cur_child );
                        if (ADVRWL_MIGSTG_WRITE_TRY( cur_child ) ) {
                            /* We got the lock, now do the migrate switch.
                             */
                            (void) advfs_migrate_switch_stg_full( cur_child,
                                                parent_ftx );
                            /* 
                             * Now downgrade the lock and continue 
                             */
                            ADVRWL_MIGSTG_DOWNGRADE( cur_child );
                        } else {
                            /* Failed to acquire migStgLk of some child, drop the lock
                             * of each child up to this one */
                            bfAccessT* last_child = cur_child;

                            SNAP_STATS( acquire_snap_locks_migstg_write_failed );
                            cur_child = bfap->bfaFirstChildSnap;
                            while (cur_child != last_child) {
                                ADVRWL_MIGSTG_UNLOCK( cur_child );
                            }

                            /* Now unlock the parents and bfap */
                            for (cur_idx = high_parent_idx; cur_idx >= 0; cur_idx--) {
                                /* this assert may not be valid for 
                                 * children of writeable snapshots */
                                MS_SMP_ASSERT( !(parent_snaps[cur_idx]->bfaFlags & 
                                            BFA_OUT_OF_SYNC) ); 
                                ADVRWL_BFAP_SNAP_UNLOCK( parent_snaps[cur_idx] );
                            }

                            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                            goto lock_the_parents;
                        }
                    }
                }

                cur_child = cur_child->bfaNextSiblingSnap;

            } else {
                /* Failed to acquire migStgLk of some child, drop the lock
                 * of each child up to this one */
                bfAccessT* last_child = cur_child;

                SNAP_STATS( acquire_snap_locks_migstg_read_failed );
                cur_child = bfap->bfaFirstChildSnap;
                while (cur_child != last_child) {
                    ADVRWL_MIGSTG_UNLOCK( cur_child );
                }

                /* Now unlock the parents and bfap */
                for (cur_idx = high_parent_idx; cur_idx >= 0; cur_idx--) {
                    /* this assert may not be valid for children of writeable snapshots */
                    MS_SMP_ASSERT( !(parent_snaps[cur_idx]->bfaFlags & BFA_OUT_OF_SYNC) ); 
                    ADVRWL_BFAP_SNAP_UNLOCK( parent_snaps[cur_idx] );
                }

                ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                /*
                 * Now a special trick.  Since last_child's migStg_lk was
                 * the one we failed to acquire, last_child must be in the
                 * middle of a migrate.  We will now try to acquire that
                 * lock for READ.  This will wait until last_child is done
                 * migrating then wake us up.  This is done as opposed to
                 * just going straight back to lock_the_parents so we don't
                 * spin during a long-lived migrate.
                 */
                ADVRWL_MIGSTG_READ( last_child );
                ADVRWL_MIGSTG_UNLOCK( last_child );
                return EAGAIN;
            }
        }
     }


    /*
     * And now for grand finale, lock the children's bfaSnapLocks
     */
    

    /* parent_snaps now has parent chain from root to bfap's fileset.  all
     * locks will be acquired from high_parent_idx down to 0. */
    if (snap_flags & SF_SNAP_WRITE) {
        /* acquire bfasnaplocks for children. */
        bfAccessT *cur_child = bfap->bfaFirstChildSnap;
        while (cur_child != NULL) {
            if (cur_child->bfaFlags & BFA_OUT_OF_SYNC) {
                /* Keep stats but don't treat it differently */
                SNAP_STATS( acquire_snap_locks_skipped_out_of_sync_child_2 );
            }
            ADVRWL_BFAP_SNAP_WRITE_LOCK( cur_child );
            cur_child = cur_child->bfaNextSiblingSnap;
        }
    }


    return EOK;
}

/*
 * begin_ims
 *
 * advfs_drop_snap_locks - this routine will drop the migstglk of all
 * parents and children of bfap and will drop the bfasnaplock of each child.
 * this routine assumes that all children that are not bfa_out_of_sync are
 * locked according to the lock scheme in advfs_acquire_snap_locks.  if any
 * of the child have had their locks dropped, this routine cannot be used.
 *
 * any children that were marked as out of sync while the locks were held
 * must have had their locks dropped since they will not be dropped in this
 * routine.
 *
 * parameters:
 * bfap - the access structure of the file to be locked.
 * snap_flags - flags indicating whether a read or write locking scheme is
 * to be used.  
 * 
 * return values:
 * eok - locks were acquired.
 *
 *      
 *
 * entry/exit conditions:
 *      on entry, bfap is open and its parents and immediate children are
 *      accessed and linked to bfap.
 *      on entry, the bfasnaplock for bfap is already held.
 *      on entry, the migstglk of all parents is held for read and if
 *      snap_flags & sf_snap_write, the bfasnaplock and migstglk of all
 *      non-bfa_out_of_sync children is locked. 
 *
 * algorithm:
 *
 * end_ims
 */
statusT
advfs_drop_snap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags)
{
    bfAccessT   *cur_parent = NULL;
    bfAccessT   *cur_child = NULL;


    /* make sure only sf_snap_write or sf_snap_read is set and not both */
    MS_SMP_ASSERT( ((snap_flags & SF_SNAP_WRITE) || (snap_flags & SF_SNAP_READ)) &&
            (!((snap_flags & SF_SNAP_WRITE) && (snap_flags & SF_SNAP_READ))) )

    /* assert that the bfasnaplock is not unlocked */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL( &bfap->bfaSnapLock, RWL_UNLOCKED));

    SNAP_STATS( acquire_drop_locks_called );

    /* unlock the bfasnaplock of each parent */
    cur_parent = bfap->bfaParentSnap;
    while (cur_parent != NULL) {
        ADVRWL_BFAP_SNAP_UNLOCK( cur_parent );
        cur_parent = cur_parent->bfaParentSnap;
    }


    if (snap_flags & SF_SNAP_READ) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        return EOK;
    }

    /*
     * if we were doing a write, unlock the children now.
     */
    cur_child = bfap->bfaFirstChildSnap;
    while (cur_child != NULL) {

        if (cur_child->bfaFlags & BFA_OUT_OF_SYNC) {
            /* Keep stats but don't treat it differently */
            SNAP_STATS( acquire_drop_locks_skipped_out_of_sync_child );
        } else {
            MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL( &cur_child->bfaSnapLock, RWL_UNLOCKED));
        }
        ADVRWL_BFAP_SNAP_UNLOCK( cur_child );
        ADVRWL_MIGSTG_UNLOCK( cur_child );
        cur_child = cur_child->bfaNextSiblingSnap;
    }

    ADVRWL_BFAP_SNAP_UNLOCK( bfap );

    return EOK;

}

/*
 * begin_ims
 *
 * advfs_acquire_xtntmap_locks - this routine will acquire the xtntmap_lks for
 * bfap and each parent of bfap. the routine is the snapshot version of
 * x_lock_inmem_xtnt_map but only deals with the x_load_reference case.
 *
 * 
 * parameters:
 * bfap - the access structure of the file to be locked.
 * xtnt_flags - flags to be passed to x_lock_inmem_xtnt_maps.  not all flags
 * are valid.  this routine only supports the x_load_reference and not the
 * x_load_update or x_load_locksowned. this is primarily for the
 * xtnt_no_wait flag.
 * snap_flags - flags to be passed in related to snaps.  not all flags are
 * valid.  this routine only supports the sf_local_blkmap flag.  this is
 * used for the snap lock assert in the beginning.
 * 
 * Return Values:
 * EOK - Locks were acquired.
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and its parents and immediate children are
 *      accessed and linked to bfap.
 *      On entry, the bfaSnapLock for bfap is held.
 *
 * Algorithm:
 * The xtntMap_lk for each parent is acquired via a call to
 * x_lock_inmem_xtnt_map.  The parents' maps are locked from root to child.
 *
 * END_IMS
 */
statusT
advfs_acquire_xtntMap_locks(
        bfAccessT*      bfap,
        xtntMapFlagsT   xtnt_flags,
        snap_flags_t    snap_flags)
{
    bfAccessT*  parent_snaps[ADVFS_MAX_SNAP_DEPTH];
    bfAccessT*  cur_parent      = NULL;
    int16_t high_parent_idx     = -1;
    int16_t cur_idx             = 0;
    statusT sts                 = EOK;

    /* We need to assert that the bfaSnapLock is NOT unlocked except
     * for this one case: 
     *    The caller is trying to to see if some offset is just mapped.
     *     AND 
     *    if the caller is interested in the full logical map 
     *    (including parent).  This is indicated with the XTNT_NO_MAPS flag 
     *    being set and the SF_LOCAL_BLKMAP flag not being set.
     */
    if (!(xtnt_flags & XTNT_NO_MAPS) || (snap_flags & SF_LOCAL_BLKMAP)) {
        MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL (&bfap->bfaSnapLock, 
                    RWL_UNLOCKED));
    }
    MS_SMP_ASSERT( !(xtnt_flags & X_LOAD_UPDATE ));

    bzero( parent_snaps, sizeof( parent_snaps ) );

    cur_parent = bfap->bfaParentSnap;
    while (cur_parent != NULL) {
        parent_snaps[++high_parent_idx] = cur_parent;
        cur_parent = cur_parent->bfaParentSnap;
    }

    /* parent_snaps now has parent chain from root to bfap.  All locks will
     * be acquired from high_parent_idx down to zero. */
    for (cur_idx = high_parent_idx; cur_idx >= 0; cur_idx--) {
        sts = x_lock_inmem_xtnt_map( parent_snaps[cur_idx], xtnt_flags);
        if (sts != EOK) {
            for (cur_idx = cur_idx + 1; cur_idx <= high_parent_idx; cur_idx++) {
                ADVRWL_XTNTMAP_UNLOCK( &parent_snaps[cur_idx]->xtntMap_lk );
            }
            return sts;
        }
    }

    /* 
     * The parents are all locked, now lock bfap
     */
    sts = x_lock_inmem_xtnt_map( bfap, xtnt_flags );
    if ( sts != EOK ) {
        for (cur_idx = 0; cur_idx <= high_parent_idx; cur_idx++) {
            ADVRWL_XTNTMAP_UNLOCK( &parent_snaps[cur_idx]->xtntMap_lk );
        }
        return sts;
    }

    /*
     * Finally, if this is a read, lock the xtntMaps for the last child
     * snapshot so that the pages that have already been COWed can be
     * unprotected if they are read into cache.  Children snaps are always
     * time ordered in the list of children snapshots.  That is to say, the
     * first child in the list of children was always created first.  That
     * means that any COWing to be done in the first child is a subset of
     * the COWing to be done in the second child is a subset of the COWing
     * to be done in the third... Therefore, by looking at the last child,
     * we get a description of the most that could possibly need to be
     * COWed.  If a range has been COWed to the last child, it does not need
     * to be protect in cache since there is no work to be done on it.
     */
    if ((snap_flags & SF_SNAP_READ) && (bfap->bfaFirstChildSnap != NULL)) {
        bfAccessT* child_bfap = bfap->bfaFirstChildSnap;
        while (child_bfap->bfaNextSiblingSnap) {
            child_bfap = child_bfap->bfaNextSiblingSnap;
        }

        sts = x_lock_inmem_xtnt_map( child_bfap, xtnt_flags );
        if (sts != EOK) {
            SNAP_STATS( acquire_xtntMap_locks_x_lock_failed );
            for (cur_idx = 0; cur_idx <= high_parent_idx; cur_idx++) {
                ADVRWL_XTNTMAP_UNLOCK( &parent_snaps[cur_idx]->xtntMap_lk );
            }
            ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
            return sts;
        }
    }

    return EOK;
}



/*
 * BEGIN_IMS
 *
 * advfs_drop_xtntMap_locks - This routine will drop the xtntMap_lks for
 * bfap and each parent of bfap. The routine undoes
 * advfs_acquire_xtntMap_locks.
 *
 * 
 * Parameters:
 * bfap - the access structure of the file to be locked.
 * 
 * Return Values:
 * EOK - Locks were acquired.
 *
 *      
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and its parents and immediate children are
 *      accessed and linked to bfap.
 *      On entry, the bfaSnapLock for bfap is held.
 *
 * Algorithm:
 * The xtntMap_lk for each parent is acquired via a call to
 * x_lock_inmem_xtnt_map.  The parents' maps are locked from root to child.
 *
 * END_IMS
 */
statusT
advfs_drop_xtntMap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags)
{
    bfAccessT   *cur_parent = NULL;
    bfAccessT   *last_child = NULL;

    ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );

    cur_parent = bfap->bfaParentSnap;
    while (cur_parent != NULL) {
        ADVRWL_XTNTMAP_UNLOCK( &cur_parent->xtntMap_lk );
        cur_parent = cur_parent->bfaParentSnap;
    }

    if ((bfap->bfaFirstChildSnap != NULL) && (snap_flags & SF_SNAP_READ)) {
        last_child = bfap->bfaFirstChildSnap;
        while (last_child->bfaNextSiblingSnap) {
            last_child = last_child->bfaNextSiblingSnap;
        }

        ADVRWL_XTNTMAP_UNLOCK( &last_child->xtntMap_lk );
    }

    return EOK;

}


/*
 * BEGIN_IMS
 *
 * advfs_snap_migrate_setup- This routine will acquire the bfaSnapLock and
 * the mcellList lock for each descendant of bfap that shares a primary
 * mcell.  This routine will first recursively acquire the bfasnapLocks for
 * each descendant by walking the tree downwards starting at bfap.  Next,
 * the routine will acqire the mcellList lock for any descendants for while
 * the bfaSnapLock was acquired.
 *
 * Parameters:
 * bfap - the access structure of the file to be locked.
 * unlock_hdl - A handle to pass back to advfs_snap_migrate_done when
 * done.
 * 
 * Return Values:
 * EOK - Locks were acquired.
 * EINVAL - Called on a snapshot that doens't have it's own metadata.
 *
 *      
 *
 * Entry/Exit Conditions:
 *
 *
 * END_IMS
 */
statusT
advfs_snap_migrate_setup(
        bfAccessT*      bfap,
        void**          unlock_hdl,
        ftxHT           parent_ftx)
{
    snap_lock_list_t*   snap_list;
    statusT             sts = EOK;
    int32_t             i;

    if (bfap->bfaFlags & BFA_VIRGIN_SNAP) {
        SNAP_STATS( snap_migrate_setup_found_virgin_snap );
        return EINVAL;
    }
   
    snap_list = 
        (snap_lock_list_t*)ms_malloc_no_bzero(sizeof(snap_lock_list_t));


    /*
     * Add bfap to the array of locked bfaps.
     */
    snap_list->sll_bfaps = (bfAccessT**)ms_malloc( sizeof( bfAccessT*) );
    snap_list->sll_size = 1;
    snap_list->sll_cnt = 1;
    snap_list->sll_bfaps[0] = bfap;

    ADVRWL_BFAP_SNAP_READ_LOCK( bfap );


    sts = advfs_snap_migrate_setup_recursive( 
            bfap,
            bfap->bfSetp,
            snap_list );


    /*
     * Now we have the BFA_SNAP_LOCK held for every decendent that is in
     * cache and uses the same mcell as bfap.  Now lock all the
     * mcellList_lks for write access to prevent any reads. 
     */
    for (i = 0; i < snap_list->sll_cnt; i++) {
        FTX_LOCKWRITE( &(snap_list->sll_bfaps[i]->mcellList_lk), parent_ftx );
    }

    *unlock_hdl = snap_list;
    return sts;

}

/*
 * BEGIN_IMS
 *
 * advfs_snap_migrate_setup_recursive - A recursive helper routine to
 * access bfaps sharing a common primMCId and acquire the
 * bfaSnapLock for read.  This routine assumes "bfap" is already accessed on
 * entry.  All descendents sharing a common primMCId will be accessed.
 *
 * Parameters:
 *
 * Return Values:
 * EOK - Locks were acquired.
 *
 *      
 * Entry/Exit Conditions:
 * On entry, bfap already has its bfaSnapLock locked.
 *
 * END_IMS
 */

static
statusT
advfs_snap_migrate_setup_recursive(
        bfAccessT*        bfap,
        bfSetT*           bf_set_ptr,
        snap_lock_list_t* snap_list)
{
    bfAccessT** bfap_array;
    int32_t     array_size;
    statusT     sts = EOK;
    bfAccessT*  child_bfap;
    bfAccessT*  sibling_bfap;

    if (snap_list->sll_size <= snap_list->sll_cnt) {
        SNAP_STATS( snap_migrate_setup_recur_modified_array );

        MS_SMP_ASSERT( snap_list->sll_size > 0 );
        array_size = 2 * snap_list->sll_size;
        
        bfap_array = (bfAccessT**)ms_malloc( array_size * sizeof( bfAccessT*) );

        if (snap_list->sll_bfaps != NULL) {
            /*
             * We use sll_size and not array_size since sll_size is the size
             * of the smaller (original) array.
             */
            bcopy( snap_list->sll_bfaps, bfap_array, 
                    snap_list->sll_size * sizeof( bfAccessT* ) );
            ms_free(snap_list->sll_bfaps);
        }
        snap_list->sll_bfaps = bfap_array;
        snap_list->sll_size = array_size;
    }



    if (bf_set_ptr->bfsFirstChildSnapSet) {
        SNAP_STATS( snap_migrate_setup_recur_to_children);

        /*
         * There may be a child of bfap.  Access it and see if it shares an
         * mcell.  If not, end the recursion.  If it does share a common
         * mcell, recurse to the child.
         */
        sts = bs_access_one( &child_bfap,
                        bfap->tag,
                        bf_set_ptr->bfsFirstChildSnapSet,
                        FtxNilFtxH,
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);

        if (sts == EOK) {
            /* Well, we got a child snapshot.  Now see if it shares a
             * primary mcell.*/
                
            ADVRWL_BFAP_SNAP_READ_LOCK( child_bfap );
            if (child_bfap->bfaFlags & BFA_VIRGIN_SNAP) {
                /*
                 * Add child_bfap to the list of locked snapshots 
                 */
                snap_list->sll_bfaps[snap_list->sll_cnt] = child_bfap;
                snap_list->sll_cnt++;

                advfs_snap_migrate_setup_recursive( child_bfap,
                                        bf_set_ptr->bfsFirstChildSnapSet,
                                        snap_list );
            } else {
                ADVRWL_BFAP_SNAP_UNLOCK( child_bfap );
                /* The child is independent and doesn't need to be affected
                 * by this parent's metadta migrate. Just close it.
                 * NOTE: We call bs_close here to close any related
                 * snapshots despite having done an bs_access_one above.
                 * The logic behind this is simply that if the thread that
                 * would have been responsible for closing related snapshots
                 * did the close between the bs_access_one and the following
                 * close, the closing of related snapshots would not occur.
                 * So we will do a bs_close of the file and cleanup any
                 * releated snapshots appropriately. */
                (void)bs_close( child_bfap,
                        MSFS_CLOSE_NONE );
            }

        } else if (sts != ENO_SUCH_TAG) {
            /* If status were ENO_SUCH_TAG we would have nothing to do here.
             * Unfortunately, it this case, we couldn't access the child so
             * we wont' be able to update it's primary mcell and we won't be
             * able to lock it.  Mark the fileset out of sync along with the
             * file.
             */
            advfs_snap_out_of_sync( bfap->tag,
                                NULL,
                                bf_set_ptr->bfsFirstChildSnapSet,
                                FtxNilFtxH );


        }
    }

    /*
     * Now see if there are sibling snapshots to access.
     */
    if (bf_set_ptr->bfsNextSiblingSnapSet) {
        SNAP_STATS( snap_migrate_setup_recur_to_siblings);

        /*
         * There may be a sibling of bfap.  Access it and see if it shares an
         * mcell.  If not, end the recursion.  If it does share a common
         * mcell, recurse to the child.
         */
        sts = bs_access_one( &sibling_bfap,
                        bfap->tag,
                        bf_set_ptr->bfsNextSiblingSnapSet,
                        FtxNilFtxH,
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);

        if (sts == EOK) {
            /* Well, we got a child snapshot.  Now see if it shares a
             * primary mcell.*/
                
            ADVRWL_BFAP_SNAP_READ_LOCK( sibling_bfap );
            if (sibling_bfap->bfaFlags & BFA_VIRGIN_SNAP) {
                /*
                 * Add bfap to the list of locked snapshots 
                 */
                snap_list->sll_bfaps[snap_list->sll_cnt] = sibling_bfap;
                snap_list->sll_cnt++;

                advfs_snap_migrate_setup_recursive( sibling_bfap,
                                        bf_set_ptr->bfsNextSiblingSnapSet,
                                        snap_list );
            } else {
                ADVRWL_BFAP_SNAP_UNLOCK( sibling_bfap );
                /* The child is independent and doesn't need to be affected
                 * by this parent's metadta migrate. Just close it.
                 * NOTE: See comment above the bs_close call above.  */
                (void)bs_close( sibling_bfap,
                        MSFS_CLOSE_NONE);
            }

        } else if (sts != ENO_SUCH_TAG) {
            /* If status were ENO_SUCH_TAG we would have nothing to do here.
             * Unfortunately, it this case, we couldn't access the child so
             * we wont' be able to update it's primary mcell and we won't be
             * able to lock it.  Mark the fileset out of sync along with the
             * file.
             */
            advfs_snap_out_of_sync( bfap->tag,
                                NULL,
                                bf_set_ptr->bfsNextSiblingSnapSet,
                                FtxNilFtxH );


        }

    }


    return EOK;
    
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_migrate_done- This routine will drop all the
 * bfaSnapLocks held by bfaps in the unlock_hdl.  The unlock_hdl is a
 * pointer to a snap_lock_list_t structure that was passed back by 
 * advfs_snap_migrate_setup.  While this routine is unlocking the
 * snapshots in the list of locked snapshots, it will also update the
 * primMCId field to the passed in parameter since the parent's metadata was
 * migrated. 
 *
 * This routine will update the children's primMCId and tag directory when
 * with the new primary mcell Id.
 *
 * NOTE: Although advfs_acquire_snap_migrate_lock took out the mcellList_lk,
 * the lock was already dropped when the transaction associated with the
 * migrate completed.
 *
 * Parameters:
 *
 * Return Values:
 * EOK - Locks were acquired.
 *
 * Entry/Exit Conditions:
 *
 * END_IMS
 */


statusT
advfs_snap_migrate_done(
        bfAccessT*      bfap,
        void*           unlock_hdl,
        bfMCIdT         new_prim_mcell,
        ftxHT           parent_ftx,
        int32_t         error) /* IN - 0 normal done
                                *      1 failure case cleanup 
                                */

{

    int32_t i;
    snap_lock_list_t*   snap_list = (snap_lock_list_t*)unlock_hdl;
    bfAccessT*  cur_bfap;
    tagInfoT    tagInfo;
    statusT     sts = EOK;

    for (i = 0; i < snap_list->sll_cnt; i++) {
        cur_bfap = snap_list->sll_bfaps[i];
        MS_SMP_ASSERT( (cur_bfap == bfap) || 
            (cur_bfap->bfaFlags & BFA_VIRGIN_SNAP) );
        

        if (cur_bfap != bfap && !error) {
            SNAP_STATS( snap_migrate_done_unlocking );

            /* 
             * Update the child's primMCId.
             */
            ADVSMP_BFAP_LOCK( &cur_bfap->bfaLock );
            cur_bfap->primMCId = new_prim_mcell;
            ADVSMP_BFAP_UNLOCK( &cur_bfap->bfaLock );

            /* 
             * Update the tag directory for this child.
             * */
            sts = advfs_bs_tagdir_update_tagmap(
                    cur_bfap->tag,
                    cur_bfap->bfSetp,
                    BS_TD_NO_FLAGS,
                    new_prim_mcell,
                    parent_ftx,
                    TDU_SET_MCELL);

            if (sts != EOK) {
                advfs_snap_out_of_sync( cur_bfap->tag,
                                        cur_bfap,
                                        cur_bfap->bfSetp,
                                        parent_ftx );
                SNAP_STATS( snap_migrate_done_tag_dir_update_failed );

            }
               
        }

        ADVRWL_BFAP_SNAP_UNLOCK( cur_bfap );
        if (i > 0) {
            /*
             * Element 0 is set to the passed in bfap which gets closed
             * at a higher level.  We only need to close child snapshot
             * files here.
             * NOTE: We call bs_close here to close any related snapshots 
             * despite having opened it with bs_access_one in 
             * advfs_snap_migrate_setup_recursive.
             * The logic behind this is simply that if the thread that
             * would have been responsible for closing related snapshots
             * did the close between the bs_access_one and the following
             * close, the closing of related snapshots would not occur.
             * So we will do a bs_close of the file and cleanup any
             * releated snapshots appropriately.
             */
            (void)bs_close( cur_bfap, MSFS_CLOSE_NONE );
        }
    }

    ms_free( snap_list->sll_bfaps );
    ms_free( snap_list );

    return EOK;
            
}


/*
 * BEGIN_IMS
 *
 * advfs_get_snap_xtnt_desc - This routine is a snapshot version of
 * imm_get_xtnt_desc which will find the extent descriptor that a fob lies
 * in when considering the parent snapshots. 
 * 
 * Parameters:
 * 
 * Return Values:
 *      
 *
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_get_snap_xtnt_desc(
        bf_fob_t                        fob_offset,
        bfAccessT*                      bfap,
        bsInMemXtntDescIdT*             xtnt_desc_id,
        bsXtntDescT*                    xtnt_desc,
        bfAccessT**                     source_bfap ) 
{

    bf_fob_t            max_fob_from_parent = 
                ADVFS_OFFSET_TO_FOB_UP(bfap->bfa_orig_file_size);
    bsInMemXtntDescIdT  bfap_xtnt_desc_id;
    bsXtntDescT         bfap_xtnt_desc;
    bsXtntDescT         cur_xtnt_desc;
    bsInMemXtntDescIdT  cur_xtnt_desc_id;
    bsXtntDescT         prev_xtnt_desc;
    bf_fob_t            clip_fob_offset;
    bf_fob_t            clip_fob_cnt;

    int32_t             child_xtnt_not_mapped;
    bfAccessT*          cur_parent_bfap;
    statusT             sts = EOK;


    /* 
     * Start by trying to get the extent in the child snapshot.  First see
     * if the fob is mapped in bfap extents before looking to parents.
     */
    sts = imm_get_xtnt_desc( fob_offset,
                        bfap->xtnts.xtntMap,
                        FALSE,
                        &bfap_xtnt_desc_id,
                        &bfap_xtnt_desc);

    if ( (bfap->bfaParentSnap == NULL) ||
         ( (sts == EOK) && 
         (bfap_xtnt_desc.bsxdVdBlk != XTNT_TERM) ) ) {
        *xtnt_desc_id = bfap_xtnt_desc_id;
        *xtnt_desc = bfap_xtnt_desc;
        SNAP_STATS( snap_get_xtnt_desc_quick_return );

        return sts;
    }


    if ( (sts == E_RANGE_NOT_MAPPED) && 
         ((off_t)(fob_offset * ADVFS_FOB_SZ)  > max( bfap->bfa_orig_file_size,
                                                     bfap->file_size) ) ) {
        SNAP_STATS( snap_get_xtnt_desc_return_cond_1 );
        return sts;
    } else if ( (sts != EOK) && (sts != E_RANGE_NOT_MAPPED) ) {
        SNAP_STATS( snap_get_xtnt_desc_return_cond_2 );
        return sts;
    }

    child_xtnt_not_mapped = (sts == E_RANGE_NOT_MAPPED);
    cur_xtnt_desc = bfap_xtnt_desc;
    prev_xtnt_desc = bfap_xtnt_desc;
    cur_parent_bfap = bfap->bfaParentSnap;

    while ( ( (sts == E_RANGE_NOT_MAPPED) || 
              (cur_xtnt_desc.bsxdVdBlk == XTNT_TERM) ) &&
            (cur_parent_bfap != NULL) ) {

        /* 
         * Get the extent descriptor from the parent to see if it is mapped.
         */
        sts = imm_get_xtnt_desc( fob_offset,
                                cur_parent_bfap->xtnts.xtntMap,
                                FALSE,
                                &cur_xtnt_desc_id,
                                &cur_xtnt_desc);

        /*
         * Check for errors and see if we should continue or return 
         */
        if ( (cur_parent_bfap->bfaParentSnap != NULL) &&
             (sts == E_RANGE_NOT_MAPPED) ) {
            /* 
             * If we still have a parent and got back E_RANGE_NOT_MAPPED,
             * then we need to keep looking to more parents 
             */
            cur_parent_bfap = cur_parent_bfap->bfaParentSnap;
            SNAP_STATS( snap_get_xtnt_desc_skipping_desc );
            continue;
        } else if ( ( (sts != EOK) && (sts != E_RANGE_NOT_MAPPED) ) ||
                    (cur_parent_bfap->bfaParentSnap) ) {
            /*
             * If we got a real error, or if we got any error and are at the
             * root parent, then just return.
             */
            *xtnt_desc = cur_xtnt_desc;
            *xtnt_desc_id = cur_xtnt_desc_id;
            SNAP_STATS( snap_get_xtnt_desc_return_cond_3 );
            return sts;
        }

        if ( (cur_parent_bfap->bfaParentSnap == NULL) ||
             (cur_xtnt_desc.bsxdVdBlk != XTNT_TERM) ) {
            /* We found an extent that maps the desired fob */
            if (child_xtnt_not_mapped) {
                /* There was no "unmapped" extent in the child snapshot
                 * with which to clip the parent's extent.  As a result, we
                 * will use the bfa_orig_file_size to clip the extent.*/
                cur_xtnt_desc.bsxdFobCnt = min( 
                        (roundup(ADVFS_OFFSET_TO_FOB_UP(bfap->bfa_orig_file_size), 
                                 bfap->bfPageSz)) - 
                          cur_xtnt_desc.bsxdFobOffset,
                        cur_xtnt_desc.bsxdFobCnt);
                *xtnt_desc = cur_xtnt_desc;
                SNAP_STATS( snap_get_xtnt_desc_good_return );
                return EOK;
            } else {
                /* The child has an "unmapped" extent that must clip the
                 * parent's extent maps.  Adjacent to the "unmapped"
                 * descriptor may be mapped extents. This concept deserves a
                 * picture. */

                clip_fob_offset = max( 
                        prev_xtnt_desc.bsxdFobOffset,
                        cur_xtnt_desc.bsxdFobOffset );

                clip_fob_cnt = min(
                        prev_xtnt_desc.bsxdFobOffset+
                          prev_xtnt_desc.bsxdFobCnt,
                        cur_xtnt_desc.bsxdFobOffset+
                          cur_xtnt_desc.bsxdFobCnt) -
                    clip_fob_offset;

                cur_xtnt_desc.bsxdFobOffset = clip_fob_offset;
                cur_xtnt_desc.bsxdFobCnt = clip_fob_cnt;
                *xtnt_desc = cur_xtnt_desc;
                SNAP_STATS( snap_get_xtnt_desc_good_return_2 );
                return EOK;
            }
        } else {
            /* cur_parent_bfap is still unmapped, need to go up another level */
            cur_parent_bfap = cur_parent_bfap->bfaParentSnap;

            /* The child xtntdesc fob count will be clipped since the
             * parent's unmapped range may be smaller than the child's */
            if (child_xtnt_not_mapped) {
                clip_fob_offset = cur_xtnt_desc.bsxdFobOffset;
                clip_fob_cnt = roundup(
                        ADVFS_OFFSET_TO_FOB_UP( bfap->bfa_orig_file_size ),
                        bfap->bfPageSz) -
                    cur_xtnt_desc.bsxdFobOffset;
                child_xtnt_not_mapped = FALSE;
                SNAP_STATS( snap_get_xtnt_desc_clip_fob );
            } else {
                /* prev_xtnt_desc should be a valid extent to use for
                 * clipping */
                clip_fob_offset = max( 
                        prev_xtnt_desc.bsxdFobOffset,
                        cur_xtnt_desc.bsxdFobOffset );

                clip_fob_cnt = min(
                        prev_xtnt_desc.bsxdFobOffset+
                          prev_xtnt_desc.bsxdFobCnt,
                        cur_xtnt_desc.bsxdFobOffset+
                          cur_xtnt_desc.bsxdFobCnt) -
                    clip_fob_offset;
                SNAP_STATS( snap_get_xtnt_desc_clip_fob_2 );

            }
            prev_xtnt_desc.bsxdFobCnt = clip_fob_cnt;
            prev_xtnt_desc.bsxdFobOffset = clip_fob_offset;
            SNAP_STATS( snap_get_xtnt_desc_continued );

            continue;
        }
    }

    return sts;

}



/*
 * BEGIN_IMS
 *
 * advfs_get_next_snap_xtnt_desc - This routine is similar to
 * imm_get_next_xtnt_desc but works for snapshots.  This currently just 
 * uses advfs_get_snap_xtnt_desc but could be made more efficient later.
 * 
 * Parameters:
 * 
 * Return Values:
 *      
 *
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_get_next_snap_xtnt_desc(
        bfAccessT*               bfap,
        bsInMemXtntDescIdT*      xtnt_desc_id,
        bsXtntDescT*             xtnt_desc,
        bfAccessT**              source_bfap)
{
    bf_fob_t    next_fob;

    next_fob = xtnt_desc->bsxdFobOffset + xtnt_desc->bsxdFobCnt + 1;
    return advfs_get_snap_xtnt_desc( next_fob,
                                bfap,
                                xtnt_desc_id,
                                xtnt_desc,
                                source_bfap);

}


/*
 * BEGIN_IMS
 *
 * advfs_add_snap_stg - This routine will examine each child of bfap (that
 * is not out of sync) and add storage to any unmapped ranges in any chilren
 * in the range [offset..offset+size].  The storage that is added will be
 * returned in the snap_maps.  Storage added to a single child snapshot will
 * be linked via the ebd_next_desc field.  The snap_maps will link between
 * children snapshots via the ebd_snap_fwd pointers.
 * 
 * Parameters:
 * bfap - the access structure that will be written and whose children need
 * storage.
 *
 * offset - the starting offset to have storage added. (in bytes)
 *
 * size - the length of storage to be added. (in bytes)
 *
 * snap_maps - a two-dimensional list of storage added to children.  Any
 * children in the snap_maps returned are still locked.  Any children that
 * are not represented in the snap_maps have had the migStgLk and the
 * bfaSnapLock dropped.
 *
 * parent_ftx - The parent transaction to contain the adding of storage.
 * 
 * Return Values:
 * EOK - All storage was added.
 * EIO -
 *
 *      
 *
 * Entry/Exit Conditions:
 *      On entry, bfap is open and its parents and immediate children are
 *      accessed and linked to bfap.
 *      On entry, the bfaSnapLock for bfap is held.
 *      On entry, the bfaSnapLock is held for write on all children of bfap
 *      and the migStgLk is held for read on all children of bfap.
 *      On exit, any snapshot children without an entry in the snap_maps
 *      (any that didn't have storage added) have had the bfaSnapLock and
 *      migStgLk dropped.
 *
 * Algorithm:
 * This routine will first acquire the extent maps for bfap then acquire the
 * extent maps for each child snapshot.  Any holes in bfap will be inserted
 * as COWED_HOLES in the children and any storage in bfap will be added as
 * storage to the children.  If ENO_SPACE is encountered while adding
 * storage to children snapshots, all remaining children are marked as out
 * of sync.  The out of sync transaction will be ftx_done'd with a special
 * done mode so that even if the parent transaction fails, the out of sync
 * state will make it to disk.
 *
 * END_IMS
 */
statusT
advfs_add_snap_stg(
        bfAccessT*              bfap,
        off_t                   offset,
        size_t                  size,
        off_t*                  min_child_stg,
        off_t*                  max_child_stg,
        extent_blk_desc_t**     snap_maps,
        snap_flags_t            snap_flags,
        ftxHT                   parent_ftx)
{
    extent_blk_desc_t *bfap_extents   = NULL;
    extent_blk_desc_t *child_extents  = NULL;
    extent_blk_desc_t *cur_bfap_desc  = NULL;
    extent_blk_desc_t *cur_child_desc = NULL;
    extent_blk_desc_t *snap_map_head  = NULL;
    extent_blk_desc_t *snap_map_tail  = NULL;
    bfAccessT   *cur_child      = NULL;
    statusT     sts             = EOK;
    off_t       start_off       = offset;
    off_t       child_start_off = 0;
    off_t       child_end_off   = 0;
    uint64_t    bfap_xtnt_cnt   = 0;
    uint64_t    child_xtnt_cnt  = 0;
    extent_blk_desc_t *child_extent_head = NULL;
    extent_blk_desc_t *child_extent_tail = NULL;
    uint64_t    start_fob       = 0;
    uint64_t    end_fob         = 0;
    off_t       temp_off        = 0;

    *min_child_stg = offset+size;
    *max_child_stg = offset;

    /*
     * Get extents maps of bfap to be used to determine where storage must
     * be added to children.  These are logical extent maps so they will
     * include extents inherited from (mapped-in) parent snapshots.  Passing
     * in the RND_ENTIRE_HOLE flag will cause the extents returned to be
     * rounded out to encompass an entire hole if the start or end offsets
     * land in a hole.  THis allows us to COW entire holes rather than parts
     * of holes.
     */
    sts = advfs_get_blkmap_in_range( bfap,
                               bfap->xtnts.xtntMap,
                               &start_off,
                               size,
                               &bfap_extents,
                               &bfap_xtnt_cnt,
                               RND_ENTIRE_HOLE,
                               EXB_COMPLETE,
                               (snap_flags&SF_NO_WAIT)?
                                XTNT_LOCKS_HELD|XTNT_NO_WAIT:
                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);
    if (sts != EOK) {
        /* Do all snapshot children need to be marked out of sync in this
         * case?  Or will the caller be able to fail without doing a write?
         */
        SNAP_STATS( add_snap_stg_get_blkamp_failed );
        return sts;
    }


    /* Walk each child bfap and add storage or COWED_HOLES */
    cur_child = bfap->bfaFirstChildSnap;
    while (cur_child != NULL) {
        
        /* 
         * If the child is out of sync, we can skip it.  But we should still
         * unlock it since it will not be in the snap_maps to be cleaned up
         * later.
         */
        if (cur_child->bfaFlags & BFA_OUT_OF_SYNC) {
            ADVRWL_MIGSTG_UNLOCK( cur_child );
            ADVRWL_BFAP_SNAP_UNLOCK( cur_child );

            cur_child = cur_child->bfaNextSiblingSnap;
            SNAP_STATS( add_snap_stg_out_of_sync_child );
            continue;
        }

        MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL (&cur_child->bfaSnapLock, RWL_WRITELOCKED));

        /* Get the child's extent maps. child_extents will be a list of
         * COWED_HOLES (-2) and unmapped holes (-1) that are directly mapped
         * in the child bfap.  No extents from parents will be returned. We
         * will not look at extents beyond the child's bfa_orig_file_size
         * rounded up to an allocation unit. */
        child_start_off = start_off;

        child_end_off   = offset+size;
        child_end_off = min(child_end_off, cur_child->bfa_orig_file_size);
        child_end_off = roundup(child_end_off,
                                ADVFS_ALLOCATION_SZ(cur_child) );

        child_extent_head = child_extent_tail = NULL;
        child_extents = NULL;
        sts = advfs_get_blkmap_in_range( cur_child,
                                cur_child->xtnts.xtntMap,
                                &child_start_off,
                                child_end_off - child_start_off,
                                &child_extents,
                                &child_xtnt_cnt,
                                RND_ENTIRE_HOLE,
                                EXB_ONLY_HOLES|EXB_DO_NOT_INHERIT,
                                (snap_flags&SF_NO_WAIT)? 
                                        XTNT_NO_WAIT:
                                        XTNT_WAIT_OK);
        if (sts == EWOULDBLOCK) {
            goto err_free_resources;
        }

        if (sts != EOK) {
            SNAP_STATS( add_snap_stg_get_blkmap_failed_2 );
            sts = advfs_snap_out_of_sync( cur_child->tag,
                                        cur_child,
                                        cur_child->bfSetp,
                                        parent_ftx);


            if ( sts != EOK) {
                SNAP_STATS( add_snap_stg_out_of_sync_failed );
                MS_SMP_ASSERT( cur_child->dmnP->dmn_panic );
                goto err_free_resources; 
            }

            /*
             * Unlock this child before continuing since it will not be in
             * the snap maps for unlocking.
             */
            ADVRWL_MIGSTG_UNLOCK( cur_child );
            ADVRWL_BFAP_SNAP_UNLOCK( cur_child );

            cur_child = cur_child->bfaNextSiblingSnap;
            continue;
        }


        /* Walk the extents of bfap and add storage or COWED_HOLES to
         * unmapped regions of cur_child */
        cur_child_desc = child_extents;
        for( cur_bfap_desc = bfap_extents; 
                cur_bfap_desc != NULL;
                cur_bfap_desc = cur_bfap_desc->ebd_next_desc) {

            /* Walk the children that overlap cur_bfap_desc */
            while (cur_child_desc != NULL) {

                /* We are only concerned with unmapped holes (XTNT_TERM) in
                 * the child, not with COWED_HOLES */
                if (cur_child_desc->ebd_vd_blk != XTNT_TERM) {
                    cur_child_desc = cur_child_desc->ebd_next_desc;
                    SNAP_STATS( add_snap_stg_skipping_stg_extent );
                    continue;
                }

                /*
                 * The child's hole is XTNT_TERM.  If we are SF_NO_WAIT,
                 * then we either need to add a perm hole or stg so we will
                 * just return EWOULDBLOCK since neither of those can be
                 * done without blocking.
                 */
                if (snap_flags & SF_NO_WAIT) {
                    sts = EWOULDBLOCK;
                    goto err_free_resources;
                }

                if (cur_child_desc->ebd_offset >=
                    (off_t)(cur_bfap_desc->ebd_offset +
                            cur_bfap_desc->ebd_byte_cnt)) {
                    /* Passed the end of cur_bfap_desc */
                    SNAP_STATS( add_snap_stg_done_with_cur_bfap_desc );
                    break;
                }

                /* Make sure cur_child_desc overlaps cur_bfap_desc */
                MS_SMP_ASSERT( (cur_child_desc->ebd_offset >= 
                                cur_bfap_desc->ebd_offset && 
                                cur_child_desc->ebd_offset <=
                                (off_t)(cur_bfap_desc->ebd_offset + 
                                        cur_bfap_desc->ebd_byte_cnt)) ||
                               (cur_bfap_desc->ebd_offset >=
                                cur_child_desc->ebd_offset &&
                                cur_bfap_desc->ebd_offset <=
                                (off_t)(cur_child_desc->ebd_offset +
                                        cur_child_desc->ebd_byte_cnt)) );

                /* Calculate the start and end of the hole.  We want to 
                 * COW the entire hole, not just part of it.
                 */
                 start_fob = ADVFS_OFFSET_TO_FOB_DOWN( 
                            max( cur_child_desc->ebd_offset,
                                cur_bfap_desc->ebd_offset) );
                 end_fob = ADVFS_OFFSET_TO_FOB_DOWN(
                            min( cur_child_desc->ebd_offset +
                                cur_child_desc->ebd_byte_cnt,
                                cur_bfap_desc->ebd_offset +
                                cur_bfap_desc->ebd_byte_cnt ) );

                if ( (cur_bfap_desc->ebd_vd_blk == XTNT_TERM) || 
                     (cur_bfap_desc->ebd_vd_blk == COWED_HOLE) ) {


                    /* The range is a hole in the parent.  Add a COWED hole to
                     * the child */
                    SNAP_STATS( add_snap_stg_trying_to_add_cow_hole);
                    sts = advfs_make_cow_hole( cur_child,
                                        start_fob,
                                        end_fob - start_fob,
                                        parent_ftx );
                    if (sts != EOK) {
                        SNAP_STATS( add_snap_stg_add_cow_hole_failed );
                        sts = advfs_snap_out_of_sync( cur_child->tag,
                                cur_child,
                                cur_child->bfSetp,
                                parent_ftx);

                        if ( sts != EOK) {
                            SNAP_STATS( add_snap_stg_out_of_sync_failed_2);

                            MS_SMP_ASSERT( cur_child->dmnP->dmn_panic );
                            goto err_free_resources;
                        }
                        /* This will break out of the loop and advance to the
                         * * next child snapshot */
                        advfs_free_blkmaps( &child_extents );
                        advfs_free_blkmaps( &child_extent_head );
                        cur_child_desc = NULL;
                        SNAP_STATS( add_snap_stg_continuing );

                        continue;
                    }
                } else {
                    extent_blk_desc_t *new_storage_desc = NULL;


                    /* The range has storage in the parent. Add storage to
                     * the child and link the new storage into the snap_maps
                     * to be COWed.  Pass in a flag to indicate that no COW
                     * processing should be done on the child. */
                    SNAP_STATS( add_snap_stg_adding_stg );

                    sts = rbf_add_stg( cur_child,
                                        start_fob,
                                        end_fob - start_fob,
                                        parent_ftx,
                                        STG_MIG_STG_HELD|STG_NO_COW);
                    /* If adding storage fails, mark the file out of sync.
                     * If marking out of sync fails, then we should have
                     * domain paniced.  So just free the extent maps and
                     * return. */
                    if (sts != EOK) {
                        SNAP_STATS( add_snap_stg_add_stg_failed );

                        sts = advfs_snap_out_of_sync( cur_child->tag,
                                cur_child,
                                cur_child->bfSetp,
                                parent_ftx);

                        if ( sts != EOK) {
                            SNAP_STATS( add_snap_stg_out_of_sync_failed_3 );
                            MS_SMP_ASSERT( cur_child->dmnP->dmn_panic );
                            advfs_free_blkmaps( &child_extent_head );
                            goto err_free_resources;
                        }
                        /* This will break out of the loop and advance to the
                         * * next child snapshot */
                        advfs_free_blkmaps( &child_extents );
                        advfs_free_blkmaps( &child_extent_head );
                        cur_child_desc = NULL;
                        SNAP_STATS( add_snap_stg_continuing_3 );
                        continue;
                    }


                    /*
                     * Figure out what the min and max storage to added is 
                     */
                    *min_child_stg = min( (off_t)(start_fob * ADVFS_FOB_SZ),
                                          *min_child_stg );
                    *max_child_stg = max( (off_t)(end_fob * ADVFS_FOB_SZ),
                                          *max_child_stg );
                

                    temp_off = start_fob * ADVFS_FOB_SZ;
                    /* Get the extent maps for the storage just added */
                    sts = advfs_get_blkmap_in_range( cur_child,
                                        cur_child->xtnts.xtntMap,
                                        &temp_off,
                                        (end_fob - start_fob) * ADVFS_FOB_SZ,
                                        &new_storage_desc,
                                        NULL,
                                        RND_NONE,
                                        EXB_ONLY_STG|EXB_DO_NOT_INHERIT,
                                        XTNT_WAIT_OK);
                    if (sts != EOK) {
                        SNAP_STATS( add_snap_stg_get_blkmap_failed_3);
                        sts = advfs_snap_out_of_sync( cur_child->tag,
                                cur_child,
                                cur_child->bfSetp,
                                parent_ftx);


                        if ( sts != EOK) {
                            SNAP_STATS( add_snap_stg_out_of_sync_failed_4 );
                            MS_SMP_ASSERT( cur_child->dmnP->dmn_panic );
                            advfs_free_blkmaps( &child_extent_head );
                            goto err_free_resources;
                        }
                        /* This will break out of the loop and advance to the
                         * * next child snapshot */
                        advfs_free_blkmaps( &child_extents );
                        advfs_free_blkmaps( &child_extent_head );
                        cur_child_desc = NULL;
                        SNAP_STATS( add_snap_stg_continuing_2 );

                        continue;
                    }
#ifdef ADVFS_SMP_ASSERT
                    /* Verify that the range covered by the extent(s)
                     * returned covers the entire storage request that was
                     * made 
                     */
                    MS_SMP_ASSERT( new_storage_desc != NULL );
                    MS_SMP_ASSERT( new_storage_desc->ebd_offset == temp_off );
                    {
                        extent_blk_desc_t *temp_desc = new_storage_desc;
                        while (temp_desc->ebd_next_desc != NULL) {
                            temp_desc = temp_desc->ebd_next_desc;
                        }
                        MS_SMP_ASSERT( 
                                (temp_desc->ebd_offset + temp_desc->ebd_byte_cnt) ==
                                (end_fob * ADVFS_FOB_SZ) );
                    }
#endif


                    /* Chain this storage to any storage already added. More
                     * than one extent may have been added, but it should
                     * all be mapped in the new_storage_desc list through
                     * ebd_next_desc. */
                    if (child_extent_head == NULL) {
                        child_extent_head = new_storage_desc;
                    }

                    /* Link the set of new extents to the existing set of
                     * extents */
                    if (child_extent_tail != NULL) {
                        child_extent_tail->ebd_next_desc = new_storage_desc;
                    }
                    
                    /* Set child_extent_tail to the last storage desc */
                    for (child_extent_tail = new_storage_desc;
                            child_extent_tail->ebd_next_desc != NULL;
                            child_extent_tail = child_extent_tail->ebd_next_desc) {}


                    
                }

                MS_SMP_ASSERT( sts == EOK );
                /* If we added storage through the end of cur_child_desc,
                 * then we can advance to the next child descriptor.  If we
                 * didn't add all the storage (or COWED_HOLE) for
                 * cur_child_desc, then The cur_child_desc spanned two or
                 * more parent extents and we need to advance to the next
                 * parent extent. */
                if (end_fob >= 
                        ADVFS_OFFSET_TO_FOB_UP(cur_child_desc->ebd_offset +
                                                cur_child_desc->ebd_byte_cnt ) ) {
                    cur_child_desc = cur_child_desc->ebd_next_desc;
                    SNAP_STATS( add_snap_stg_advnace_desc );

                } else {
                    /* We are finished with the cur_bfap_desc. Break out of
                     * the while loop to go to the next parent's descriptor.*/
                    SNAP_STATS( add_snap_stg_break_inner );
                    break; 
                }
            }

            if (sts != EOK) {
                SNAP_STATS( add_snap_stg_status_error );

                /* An error occured while walking this child snapshots
                 * extents.  Break out of the for loop for the parent
                 * extents to advance to the next child.  The cur_child's
                 * extents have been freed */
                sts = EOK;
                break;
            }
        }

        /* Free the unmapped extents for cur_child */
        advfs_free_blkmaps( &child_extents );

        if (child_extent_head != NULL) {
            /* We added storage, add the extents of added storage to the
             * snap_maps to be returned for COWing */
            if (snap_map_head == NULL) {
                snap_map_head = child_extent_head;
            }
            if (snap_map_tail != NULL) {
                snap_map_tail->ebd_snap_fwd = child_extent_head;
            }
            snap_map_tail = child_extent_head;
            ADVFS_ACCESS_LOGIT_2( cur_child, "advfs_add_snap_stg: Added storage to child",
                        child_extent_head->ebd_offset, child_extent_head->ebd_byte_cnt);
        } else {
            ADVFS_ACCESS_LOGIT( cur_child, "advfs_add_snap_stg: Added no storage to child" );
            /* No storage was added, so drop child's migStgLk and
             * bfaSnapLock */
            ADVRWL_MIGSTG_UNLOCK( cur_child );
            ADVRWL_BFAP_SNAP_UNLOCK( cur_child );
        }
        cur_child = cur_child->bfaNextSiblingSnap;
    } /* while (cur_child != NULL) */ 

    advfs_free_blkmaps( &bfap_extents );
    *snap_maps = snap_map_head;
    return EOK;
   


err_free_resources:
    SNAP_STATS( add_snap_stg_free_resrouces_error );


    ADVFS_ACCESS_LOGIT( bfap, "advfs_add_snap_stg: Error condition" );
    MS_SMP_ASSERT( sts != EOK );
    /* This is a fatal error path (probably a domain panic).  
     * All children will be marked out of sync but only in memory
     * and all resources will be freed including extent maps (snap_maps) and
     * locks.  cur_child was being processed when this error path was
     * called, so any children after cur_child need the locks dropped.
     * Before cur_child, only bfaps in the snap_maps need locks to be
     * dropped. */
    advfs_free_blkmaps ( &bfap_extents );
    advfs_free_blkmaps ( &child_extents );

    /* Unlock each child after cur_child.  Include cur_child if cur_child
     * isn't in the snap_maps.  If cur_child is in the snap_maps, it will be
     * freed with the other children in the snap maps */
    while (cur_child != NULL) {
        if (cur_child == snap_map_tail->ebd_bfap) {
            cur_child = cur_child->bfaNextSiblingSnap;
            SNAP_STATS( add_snap_stg_continue_4 );
            continue;
        }
        
        /* Mark the child out of sync in memory if not already out of sync. 
         * We shoud have domain paniced, so in memory is the best we can do.
         * This will prevent any reads from the file */
        if ( !(cur_child->bfaFlags & BFA_OUT_OF_SYNC) ) { 
            ADVSMP_BFAP_LOCK( &cur_child->bfaLock);
            cur_child->bfaFlags |= BFA_OUT_OF_SYNC;
            ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock);
            SNAP_STATS( add_snap_stg_skipping_out_of_sync_child );
        }
            
        ADVRWL_MIGSTG_UNLOCK( cur_child );
        ADVRWL_BFAP_SNAP_UNLOCK( cur_child );
    }

    cur_child_desc = snap_map_head;
    while (cur_child_desc != NULL) {
        SNAP_STATS( add_snap_stg_unlocking_children );

        /* Walk the snap maps freeing maps and unlocking children */
        cur_child = cur_child_desc->ebd_bfap;
        snap_map_head = cur_child_desc->ebd_snap_fwd;

        /* Mark the child out of sync in memory if not already out of sync. 
         * We shoud have domain paniced, so in memory is the best we can do.
         * This will prevent any reads from the file */
        if ( !(cur_child->bfaFlags & BFA_OUT_OF_SYNC) ) { 
            ADVSMP_BFAP_LOCK( &cur_child->bfaLock);
            cur_child->bfaFlags |= BFA_OUT_OF_SYNC;
            ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock);
            SNAP_STATS( add_snap_stg_skipping_out_of_sync_child_2 );
        }

        ADVRWL_MIGSTG_UNLOCK( cur_child );
        ADVRWL_BFAP_SNAP_UNLOCK( cur_child );
        cur_child_desc = snap_map_head;
    }

    return sts;
}



/*
 * BEGIN_IMS
 *
 * advfs_issue_snap_io - This routine will take an ioanchor and a set of
 * snap maps and issue IO to all regions in the snap maps that overlap the
 * buf structure in the ioanchor.  
 * 
 * Parameters:
 * io_anchor - an ioanchor_t structure that describes the IO to be written
 * to snapshot chilren.
 *
 * snap_maps - a two dimensional list of uninitialized storage in snapshot
 * children that needs to be initialized.  
 * 
 * Return Values:
 * EOK - IOs were issued successfully. 
 *
 * Entry/Exit Conditions:
 *      On exit, all extents in the snap maps that overlap with the IO
 *      described by the io_anchor parameter have been issued and the
 *      iocounter of the io_anchor has been incremented for each additional
 *      IO.
 *
 * Algorithm:
 * This routine will walk the snap_maps and issue IOs to each extent that
 * overlaps the ioanchor's buf structure. The routine assumes the snap maps
 * are ordered by increasing file offset.
 *
 * END_IMS
 */
statusT
advfs_issue_snap_io(
        ioanchor_t*             io_anchor,
        extent_blk_desc_t*      snap_maps,
        snap_flags_t            snap_flags,
        bfAccessT*              bfap,
        ftxHT                   parent_ftx)
{
    struct buf* io_buf  = io_anchor->anchr_buf_copy;
    struct buf* cur_buf = NULL;
    off_t       io_start = 0;
    off_t       io_end  = 0;
    off_t       cur_io_start = 0;
    off_t       offset_into_extent = 0;
    off_t       blks_into_extent = 0;
    off_t       cur_write_end = 0;
    extent_blk_desc_t*  cur_extent_maps = NULL;
    extent_blk_desc_t*  prev_extent_maps = NULL;
    extent_blk_desc_t*  cur_desc = NULL;
    bfAccessT*          cur_bfap = NULL;
    statusT     sts = EOK;


    MS_SMP_ASSERT( io_buf != NULL );
    MS_SMP_ASSERT( !(BGETFLAGS(io_buf) & B_DONE) );
    MS_SMP_ASSERT( !(BGETFLAGS(io_buf) & B_ERROR) );

    
    io_start = io_buf->b_foffset;
    io_end = io_start + io_buf->b_bcount;

    /* Walk the extent maps of each file in the snap maps (following
     * ebd_snap_fwd). 
     */
    cur_extent_maps = snap_maps;
    while (cur_extent_maps != NULL) {
        cur_desc = cur_extent_maps;
        cur_bfap = cur_extent_maps->ebd_bfap;
        /* Walk each descriptor in this snapshots extent maps */
        while ( cur_desc != NULL ) {
            if (io_end <= cur_desc->ebd_offset) {
                /* The cur_desc is beyond this IO request.  Nothing to do */
                SNAP_STATS( issue_snap_io_advancing_beyond_desc );
                break;
            } else if ( io_start >= 
                   (cur_desc->ebd_offset + cur_desc->ebd_byte_cnt) ) {
                /* Descriptor is before the start of the IO, advance to
                 * the next descriptor */
                cur_desc = cur_desc->ebd_next_desc;
                SNAP_STATS( issue_snap_io_advancing_before_desc );
                continue;
            }


            /*
             * Figure out the io start relative to the descriptor start
             */
            if (io_start <= cur_desc->ebd_offset) {
                /* 
                 * The IO will encompass the start of the descriptor.
                 */
                offset_into_extent = 0;
                blks_into_extent = 0;
            } else {
                /* The IO will start some offset into the current
                 * descriptor.
                 */
                offset_into_extent = io_start - cur_desc->ebd_offset;
                blks_into_extent = offset_into_extent / 
                    (ADVFS_FOBS_PER_DEV_BSIZE * ADVFS_FOB_SZ);
            }

            MS_SMP_ASSERT( offset_into_extent % DEV_BSIZE == 0 );
            MS_SMP_ASSERT( offset_into_extent % ADVFS_FOB_SZ == 0 );

            /* Figure out the end of the write to be issued on this extent.
             * Either the end of the extent or the end of the IO request. */
            if (io_end > cur_desc->ebd_offset + cur_desc->ebd_byte_cnt) {
                cur_write_end = cur_desc->ebd_offset + cur_desc->ebd_byte_cnt;
                SNAP_STATS( issue_snap_io_io_end_greater_than_desc );
            } else {
                SNAP_STATS( issue_snap_io_io_end_less_than_desc );
                cur_write_end = io_end;
            }
            cur_io_start = MAX( io_start, cur_desc->ebd_offset );

            /* Malloc a new struct buf for use in the IO to cur_desc.
             * And initailize it with the original IO buf. */
            cur_buf = (struct buf*)ms_malloc_no_bzero( sizeof( struct buf ) );
            bcopy( io_buf, cur_buf, sizeof( struct buf ) );

            BRESETFLAGS(cur_buf,(bufflags_t)B_READ);
            BSETFLAGS(cur_buf, (bufflags_t)(B_WRITE|B_PHYS|B_CALL));
            cur_buf->b_foffset = cur_io_start; 
            cur_buf->b_blkno = cur_desc->ebd_vd_blk + blks_into_extent;
            /* Offset the virtual address by the amount of the IO we aren't
             * doing for this extent descriptor */
            cur_buf->b_un.b_addr += cur_io_start - io_start;
            cur_buf->b_bcount = cur_write_end - cur_io_start;

            /* Icrement the IO counter for the IO about to be issued */
            spin_lock( &io_anchor->anchr_lock );
            io_anchor->anchr_iocounter++;
            spin_unlock( &io_anchor->anchr_lock );

            advfs_bs_startio( cur_buf, 
                                io_anchor, 
                                NULL,
                                cur_bfap,
                                VD_HTOP( cur_desc->ebd_vd_index,
                                        cur_bfap->dmnP ),
                                0);
            cur_desc = cur_desc->ebd_next_desc;
            SNAP_STATS( issue_snap_io_called_bsstartio );

        }
        cur_extent_maps = cur_extent_maps->ebd_snap_fwd;
    }

    /* 
     * Issue the fake read so that the writes are syncrhonized.  This is
     * either a fake read of the original read, or a fake IO to cleanup the
     * extra io counter if a real read was issued.
     */

    if (snap_flags & SF_FAKE_ORIG) {
        io_buf = io_anchor->anchr_origbuf;
    } else {
        io_buf = (struct buf*)ms_malloc_no_bzero( sizeof( struct buf ) );
        bcopy( io_anchor->anchr_buf_copy, io_buf, sizeof( struct buf ) );
    }

    advfs_bs_startio( io_buf,
            io_anchor,
            NULL, /* No bsBuf for a fake IO */
            bfap,
            NULL, /* No vd for a fake IO */
            ADVIOFLG_FAKEIO );

    return sts;
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_revoke_cfs_tokens - This routine is responsible for revoking
 * the CFS tokens of every descendant snapshot of bfap.  If a descendant is
 * not in cache, it does not need to be revoke.  If bfap has a parent
 * snapshot (is a child snapshot) it's token also must be revoked.  This
 * routine will set the BFA_XTNTS_IN_USE and BFA_IN_COW_MODE flags before
 * returning. 
 *
 * NOTE: This routine will currently only revoke one level down.  It will
 * assert that no further descendants are in cache... 
 *
 * Parameters:
 * bfap - the file whose children must be revoked.
 *
 * Return Values:
 * EOK -  Tokens were successfully revoked and not further clients can
 * acquire xtnt maps.
 * 
 * Entry/Exit Conditions:
 *      On entry, it is assumed that the file lock is held for read unless
 *      SF_NO_LOCK is set in snap_flags in which case the lock is not held.  
 *      The lock cannot be held while revoking the CFS client tokens, so it
 *      must be dropped.  We cannot safely drop the file lock for write.
 *
 *      On entry, assumes the bfaSnapLock of bfap is held for read.
 *
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_snap_revoke_cfs_tokens(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags 
)
{
    MS_SMP_ASSERT( clu_is_ready() );


    if ( HAS_RELATED_SNAPSHOT( bfap ) && !(snap_flags & SF_NO_LOCK) ) {
        ADVRWL_FILECONTEXT_UNLOCK( &(bfap->bfFsContext) );
        SNAP_STATS( revoke_cfs_tokens_droped_file_lock );
    }

    if (HAS_SNAPSHOT_CHILD( bfap ) ) {
        bfAccessT*      cur_child = bfap->bfaFirstChildSnap;
        bfAccessT*      next_child = NULL;
        fsid_t          fsid;

        SNAP_STATS( revoke_cfs_tokens_had_child_to_revoke );


        ADVRWL_BFAP_SNAP_READ_LOCK( bfap );


        /* Need to revoke all descendant children. First mark use bfap as
         * BFA_XTNTS_IN_USE to block any new access to the extents from cfs
         * clients. */
        while (cur_child != NULL) {
            ADVRWL_BFAP_SNAP_WRITE_LOCK( cur_child );
            /*
             * Wait for other COWers to finish.  Only one at a time may
             * proceed...
             */
            while (cur_child->bfaFlags & BFA_IN_COW_MODE) {
                cv_wait( &cur_child->bfaSnapCv,
                         &cur_child->bfaSnapLock,
                         CV_RWLOCK_WR,
                         CV_DFLT_FLG);
                SNAP_STATS( revoke_cfs_tokens_cv_waited );

            }
            ADVSMP_BFAP_LOCK( &cur_child->bfaLock );
            MS_SMP_ASSERT( !(cur_child->bfaFlags & BFA_IN_COW_MODE) &&
                           !(cur_child->bfaFlags & BFA_XTNTS_IN_USE) );
            cur_child->bfaFlags |= BFA_IN_COW_MODE|BFA_XTNTS_IN_USE;
            ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock );
            MS_SMP_ASSERT( cur_child->bfaFirstChildSnap == NULL );
            next_child = cur_child->bfaNextSiblingSnap;
            ADVRWL_BFAP_SNAP_UNLOCK( cur_child );
            cur_child = next_child;
        }
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        /* Now no cfs clients can get a copy of the extents, but they may
         * already have access. We will now walk the list of children
         * without any locks held so that we can call
         * CLU_CFS_COW_MODE_ENTER.  We can only walk this list without a
         * lock because the fileset removal code will block on
         * BFA_IN_COW_MODE.  That way, no children can disappear out of the
         * list while we walk it. */
        cur_child = bfap->bfaFirstChildSnap;
        while (cur_child != NULL) {
            BS_GET_FSID( cur_child->bfSetp, fsid );
            if (!CLU_CFS_COW_MODE_ENTER( fsid, cur_child->tag )) { 
                SNAP_STATS( revoke_cfs_tokens_cow_mode_enter_failed );

                /* COW_MODE_ENTER failed... cnode must not have exists,
                 * nothing to be done. */
                ADVSMP_BFAP_LOCK( &cur_child->bfaLock );
                MS_SMP_ASSERT( !(cur_child->bfaFlags & BFA_CFS_HAS_XTNTS) );
                cur_child->bfaFlags &= ~BFA_IN_COW_MODE;
                next_child = cur_child->bfaNextSiblingSnap;
                ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock );
                cur_child = next_child;
            }
        }

    }

    /* 
     * Now see if we need to revoke bfap's token.
     */
    if ( HAS_SNAPSHOT_PARENT( bfap ) ) {
        fsid_t  fsid;
        /* 
         * First set flags to block new cfs clients from getting extents 
         */
        SNAP_STATS( revoke_cfs_tokens_revoking_bfap_token );

        ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_IN_COW_MODE) &&
                           !(bfap->bfaFlags & BFA_XTNTS_IN_USE) );
        bfap->bfaFlags |= BFA_IN_COW_MODE|BFA_XTNTS_IN_USE;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        BS_GET_FSID( bfap->bfSetp, fsid );
        if (!CLU_CFS_COW_MODE_ENTER( fsid, bfap->tag )) { 
            SNAP_STATS( revoke_cfs_tokens_cow_mode_enter_failed_2 );

            /* COW_MODE_ENTER failed... cnode must not have exists,
             * nothing to be done. */
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_CFS_HAS_XTNTS) );
            bfap->bfaFlags &= ~BFA_IN_COW_MODE;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }

    }

    if ( HAS_RELATED_SNAPSHOT( bfap ) && !(snap_flags & SF_NO_LOCK) ) {
        ADVRWL_FILECONTEXT_READ_LOCK( &(bfap->bfFsContext) );
    }


    return EOK;

}

/*
 * BEGIN_IMS
 *
 * advfs_snap_cleanup_and_unlock - This routine is used to cleanup after a
 * fault.  It will unlock any locks that were acqiured in advfs_getpage and
 * will undo anything that was done by advfs_snap_revoke_cfs_tokens.  
 *
 * NOTE: This routine will currently only allow one level down.  It will
 * assert that no further descendants are in cache... 
 *
 * Parameters:
 *
 * Return Values:
 * EOK -  
 * 
 * Entry/Exit Conditions:
 * On entry, assumes the bfaSnapLock of bfap is held for read and that
 * snap_maps represents files that need to be unlocked.
 *
 * On exit, memory allocated to snap_maps will be freed.
 *
 * Algorithm:
 *
 * END_IMS
 */

statusT
advfs_snap_cleanup_and_unlock(
        bfAccessT*      bfap,
        extent_blk_desc_t**     snap_maps,
        snap_flags_t    snap_flags)
{
    bfAccessT*  cur_child;
    bfAccessT*  cur_parent;
    extent_blk_desc_t*  cur_desc;


    /*
     * Start by unlocking all the children in the snap maps.  Any children
     * not in the snap maps were unlocked earlier on.
     */
    cur_desc = *snap_maps;
    while (cur_desc != NULL) {
        if (cur_desc->ebd_bfap != bfap) {
            SNAP_STATS( snap_cleanup_unlocked_from_snap_maps );
            ADVRWL_MIGSTG_UNLOCK( cur_desc->ebd_bfap );
            ADVRWL_BFAP_SNAP_UNLOCK( cur_desc->ebd_bfap );
        }
        cur_desc = cur_desc->ebd_snap_fwd;
    }


    /* 
     * If we are in a cluster, we need to take any file in cow mode out of
     * cow mode.  We also need to clear the BFA_XTNTS_IN_USE flag.
     */
    if ( (bfap->bfVnode.v_vfsp->vfs_flag & VFS_CFSONTOP) && 
	 				(snap_flags & SF_SNAP_WRITE) ) {
        MS_SMP_ASSERT( bfap->bfaFirstChildSnap );

        SNAP_STATS( snap_cleanup_unlocked_for_write_case );

        /*
         * Step one is to clear the BFA_XTNTS_IN_USE flag. Once this flag is
         * cleared, some children may be in cow mode and some may not.
         * Those not in cow mode didn't have a cnode when
         * advfs_snap_revoke_cfs_tokens was called.  Those children could
         * now have their extents accesses.  The children still in cow mode
         * are still protected, but we will get to them shortly.
         */
        cur_child = bfap->bfaFirstChildSnap;
        while (cur_child != NULL) {
            ADVSMP_BFAP_LOCK( &cur_child->bfaLock );
            cur_child->bfaFlags &= ~ BFA_XTNTS_IN_USE;
            ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock );
            cur_child = cur_child->bfaNextSiblingSnap;
            SNAP_STATS( snap_cleanup_unlocked_for_XTNTS_loop );

        }

        if (bfap->bfaFlags & BFA_IN_COW_MODE) {
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags &= ~ BFA_IN_COW_MODE;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }
    }

    /* 
     * It's now safe to unlock bfap.  We need to do this to finish cluster
     * processing too since we can't call COW_MODE_LEAVE holding this lock.
     */
    ADVRWL_BFAP_SNAP_UNLOCK( bfap );

    /* 
     * Unlock any parents.   
     */
    cur_parent = bfap->bfaParentSnap;
    while (cur_parent != NULL) {
        ADVRWL_BFAP_SNAP_UNLOCK( cur_parent);
        cur_parent = cur_parent->bfaParentSnap;
        SNAP_STATS( snap_cleanup_unlocked_parents );
    }

    /*
     * Now finish the CFS processing as appropriate.  In this (step 2)
     * block, we will clear the BFA_IN_COW_MODE flag and call
     * CLU_CFS_COW_MODE_LEAVE.  This will allow the extents to be acquired
     * by CFS clients.  By processing bfap last, we synchronize with
     * deleting a fileset which will block on the BFA_IN_COW_MODE and
     * BFA_XTNTS_IN_USE flags.
     */
    if ( (bfap->bfVnode.v_vfsp->vfs_flag & VFS_CFSONTOP) && 
         					(snap_flags & SF_SNAP_WRITE) ) {
        fsid_t  fsid; 
        cur_child = bfap->bfaFirstChildSnap;
        while (cur_child != NULL) {
            
            /*
             * If cur_child is not BFA_IN_COW_MODE there is nothing to be
             * done.  
             */
            if (cur_child->bfaFlags & BFA_IN_COW_MODE) {
                BS_GET_FSID( cur_child->bfSetp, fsid );
                CLU_CFS_COW_MODE_LEAVE( fsid, cur_child->tag );
                SNAP_STATS( snap_cleanup_left_cow_mode_for_child );

                ADVSMP_BFAP_LOCK( &cur_child->bfaLock );
                cur_child->bfaFlags &= ~BFA_IN_COW_MODE;
                ADVSMP_BFAP_UNLOCK( &cur_child->bfaLock );
                
            }
            
            /* 
             * Let everyone know this bfap is ready to be used.
             */
            cv_broadcast( &cur_child->bfaSnapCv, NULL, CV_NULL_LOCK );
            cur_child = cur_child->bfaNextSiblingSnap;
        }

        /* 
         * Now process bfap.
         */
        if (bfap->bfaFlags & BFA_IN_COW_MODE) {
            BS_GET_FSID( bfap->bfSetp, fsid );
            CLU_CFS_COW_MODE_LEAVE( fsid, bfap->tag );
            SNAP_STATS( snap_cleanup_left_cow_mode_for_bfap );

            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags &= ~BFA_IN_COW_MODE;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }
        /* 
         * Let everyone know this bfap is ready to be used.
         */
        cv_broadcast( &bfap->bfaSnapCv, NULL, CV_NULL_LOCK );

    }


    /* Free up the snap maps */
    cur_desc = *snap_maps;
    while (cur_desc != NULL) {
        *snap_maps = (*snap_maps)->ebd_snap_fwd;
        advfs_free_blkmaps( &cur_desc );
        cur_desc = *snap_maps;
    }

    MS_SMP_ASSERT( *snap_maps == NULL );

    return EOK;
}

/*
 * BEGIN_IMS
 *
 * advfs_setup_cow -  This routine will copy the metadata from parent_bfap
 * to child_bfap, thereby preparing child_bfap for future copy-on-write's of
 * extent data.  When advfs_setup_cow is complete, child_bfap has its own
 * copy of metadata and is no longer BFA_VIRGIN_SNAP or BS_TD_VIRGIN_SNAP.
 * 
 * Parameters:
 * parent_bfap - the parent of child_bfap (and source of the metadata to be
 * copied).
 * child_bfap - the snapshot child to recevieve its own metadata.
 * snap_flags - flags
 * parent_ftx - the transaction under which to perform the metadata copy.
 *
 * 
 * Return Values:
 * EOK - child_bfap has its own metadata.
 * Not-EOK - child_bfap is out of sync on disk, or domain has paniced and
 * child_bfap is out of sync in memory only.
 *
 * Entry/Exit Conditions:
 *      On Entry, parent_bfap already has its own metadata.  If parent bfap
 *      does not yet have metadata, it must have advfs_setup_cow called on
 *      it first.
 *      On entry, the bfaSnapLock is held in write mode for child_bfap.
 *      
 *
 * Algorithm:
 * This routine will start a transaction under which to copy the metadata.
 * If the transaction succeeds, it will not be undone.  It will be finished
 * with a special done mode to allow the changes to remain even if the
 * higher level transaction is later failed. 
 *
 * END_IMS
 */
statusT
advfs_setup_cow(
        bfAccessT*      parent_bfap,
        bfAccessT*      child_bfap,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx)
{
    bfMCIdT     child_prim_mcell;
    bfMCIdT     parent_prim_mcell;
    bsBfAttrT   parent_attr;
    ftxHT       cow_ftx;
    lkStatesT   prev_state      = LKW_NONE;
    statusT     sts             = EOK;

    /* Assert that child_bfap->bfaSnapLock is held for write */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL (&child_bfap->bfaSnapLock, RWL_WRITELOCKED));
    MS_SMP_ASSERT( !(parent_bfap->bfaFlags & BFA_VIRGIN_SNAP) );

    if (!(child_bfap->bfaFlags & BFA_VIRGIN_SNAP)) {
        /* The child already has metadata */
        SNAP_STATS( setup_cow_no_work );
        return EOK;
    }

    /* Start a transaction so that it can be ftx_done'd in a special done
     * mode. This also synchronizes with fileset removal which starts an
     * exclusive transaction */
    sts = FTX_START_N( FTA_META_SNAP, &cow_ftx, parent_ftx, child_bfap->dmnP );

    MS_SMP_ASSERT( sts == EOK );

    /* Acquire the attributse from the parent */
    sts = bmtr_get_rec( parent_bfap,
                        BSR_ATTR,
                        &parent_attr,
                        sizeof(parent_attr));
    if (sts != EOK) {
        SNAP_STATS( setup_cow_get_rec_error );
        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_428);
        ftx_fail( cow_ftx );
        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_429);
        return sts;
    }

    parent_attr.state = BSRA_VALID;
    parent_attr.transitionId = get_ftx_id( cow_ftx );
    
    ADVRWL_MCELL_LIST_READ( &parent_bfap->mcellList_lk );
    ADVRWL_MCELL_LIST_WRITE( &child_bfap->mcellList_lk );


    /*
     * If the child hasn't been COWed yet, it's bfa_orig_file_size may not
     * be correctly initialized (if it hasn't opened it's parents yet).
     * Initialize it here.
     */
    if (parent_bfap->bfaFlags & ADVFS_ROOT_SNAPSHOT) {
        if (IS_COWABLE_METADATA( parent_bfap ) ) {
            /* For metadata, calculate an approrpriate value for
             * bfa_orig_file_size */
            if (parent_bfap->bfa_orig_file_size == ADVFS_ROOT_SNAPSHOT) {
                SNAP_STATS( setup_cow_meta_parent_is_root );
                child_bfap->bfa_orig_file_size = parent_bfap->bfaNextFob * ADVFS_FOB_SZ;
            } else {
                SNAP_STATS( setup_cow_meta_parent_is_not_root );
                child_bfap->bfa_orig_file_size = 
                    max(parent_bfap->bfa_orig_file_size,
                        (off_t)(parent_bfap->bfaNextFob * ADVFS_FOB_SZ));
                MS_SMP_ASSERT( child_bfap->bfa_orig_file_size % 
                        (child_bfap->bfPageSz * ADVFS_FOB_SZ) == 0 );
            }
        } else {
            /* For userdata... use the parent's file_size. */
            SNAP_STATS( setup_cow_set_user_size );

            MS_SMP_ASSERT( child_bfap->dataSafety == BFD_USERDATA );
            child_bfap->bfa_orig_file_size = parent_bfap->file_size;
        }
    }

    MS_SMP_ASSERT( child_bfap->bfaFlags & BFA_VIRGIN_SNAP );

    /*
     * Setup the original file size before writing the BSR_ATTR record to
     * the new snapshot metadata.
     */
    parent_attr.bfat_orig_file_size = child_bfap->bfa_orig_file_size;

    /* Get a new mcell for the snapshot */
    sts = advfs_new_snap_mcell( &child_prim_mcell,
                                child_bfap->dmnP,
                                cow_ftx,
                                &parent_attr,
                                child_bfap->bfSetp,
                                child_bfap->tag );
    if (sts != EOK) {
        SNAP_STATS( setup_cow_new_snap_mcell_failed );

        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_430);
        ftx_fail( cow_ftx );
        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_431);
        if (advfs_snap_out_of_sync( child_bfap->tag,
                                        child_bfap,
                                        child_bfap->bfSetp,
                                        parent_ftx) != EOK) {
            SNAP_STATS( setup_cow_out_of_sync_failed );

            MS_SMP_ASSERT( child_bfap->dmnP->dmn_panic );
        }
        ADVRWL_MCELL_LIST_UNLOCK( &parent_bfap->mcellList_lk );
        ADVRWL_MCELL_LIST_UNLOCK( &child_bfap->mcellList_lk );
        return sts;
    }


    parent_prim_mcell = parent_bfap->primMCId;
    /* 
     * Copy the records in the mcell chain of the parent.  
     */
    sts = advfs_snap_copy_mcell_chain( parent_prim_mcell,
                                child_prim_mcell,
                                child_bfap->dmnP,
                                cow_ftx );
    if (sts != EOK) {
        SNAP_STATS( setup_cow_snap_copy_mcell_failed );

        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_432);
        ftx_fail( cow_ftx );
        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_433);
        if (advfs_snap_out_of_sync( child_bfap->tag,
                                        child_bfap,
                                        child_bfap->bfSetp,
                                        parent_ftx) != EOK) {
            SNAP_STATS( setup_cow_out_of_sync_failed_2 );

            MS_SMP_ASSERT( child_bfap->dmnP->dmn_panic );
        }
        ADVRWL_MCELL_LIST_UNLOCK( &parent_bfap->mcellList_lk );
        ADVRWL_MCELL_LIST_UNLOCK( &child_bfap->mcellList_lk );
        return sts;
    }

    /* Reset the child's primary mcell Id */
    child_bfap->primMCId = child_prim_mcell;

    /* Reset the mcell Id in the xtnt maps */
    child_bfap->xtnts.xtntMap->hdrMcellId = child_prim_mcell;
    child_bfap->xtnts.xtntMap->subXtntMap[0].mcellId = child_prim_mcell;
    
    /* update the tag entry with the new mcell id and remove the 
     * BS_TD_VIRGIN_SNAP flag */
    sts = advfs_bs_tagdir_update_tagmap(
            child_bfap->tag,
            child_bfap->bfSetp,
            BS_TD_VIRGIN_SNAP,
            child_prim_mcell,
            cow_ftx,
            TDU_UNSET_FLAGS|TDU_SET_MCELL|TDU_SKIP_FTX_UNDO);

    if ( sts != EOK ) {
        SNAP_STATS( setup_cow_tag_update_failed );

        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_434);
        ftx_fail( cow_ftx );
        ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_435);
        if (advfs_snap_out_of_sync( child_bfap->tag,
                    child_bfap,
                    child_bfap->bfSetp,
                    parent_ftx) != EOK) {
            SNAP_STATS( setup_cow_out_of_sync_failed_3 );

            MS_SMP_ASSERT( child_bfap->dmnP->dmn_panic );
        }
        return sts;
    }

    /* We won't both remapping the child because we don't have any extents
     * to map (yet) */
    
    ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_40);
    
    ftx_special_done_mode( cow_ftx, FTXDONE_SKIP_SUBFTX_UNDO );
    
    ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_41);
    
    ftx_done_n( cow_ftx, FTA_META_SNAP );
    
    ADVFS_CRASH_RECOVERY_TEST(parent_bfap->dmnP, AdvfsCrash_42);

    ADVRWL_MCELL_LIST_UNLOCK( &child_bfap->mcellList_lk ); 
    ADVRWL_MCELL_LIST_UNLOCK( &parent_bfap->mcellList_lk );

    ADVSMP_BFAP_LOCK( &child_bfap->bfaLock );
    child_bfap->bfaFlags &= ~BFA_VIRGIN_SNAP;
    ADVSMP_BFAP_UNLOCK( &child_bfap->bfaLock );

    return EOK;
}


/*
 * BEGIN_IMS
 *
 * advfs_new_snap_mcell - This routine will get a new primary mcell for a
 * snapshot.  It is expected that the mcellList_lk is held for write mode.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 * EOK - A new mcell has been created an initailized.
 * ENOSPC - There is no storage left to create a new mcell.
 * EIO - An IO Error occured trying to get the new mcell.
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */

static statusT
advfs_new_snap_mcell(
               bfMCIdT*   new_prim_mcell, /* out - ptr to mcell id */
               domainT*   domain_p,       /* in - domain ptr */
               ftxHT      parent_ftx,     /* in - parent ftx */
               bsBfAttrT* bf_attr,        /* in - bitfile attributes ptr */
               bfSetT     *bf_set_p,      /* in - bitfile's bf set desc ptr */
               bfTagT     tag)            /* in - tag of new bitfile */
{
    statusT sts;
    ftxHT ftx;
    mcellUIdT mcelluid;

    /* Start a transaction to create the new mcell */
    sts = FTX_START_META( FTA_SNAP_NEW_MCELL, &ftx, parent_ftx, domain_p );
    if (sts != EOK) {
        ADVFS_SAD1("advfs_new_snap_mcell: ftx_start failed", sts );
    }

    mcelluid.ut.bfsid = bf_set_p->bfSetId;

    mcelluid.ut.tag = tag;

    sts = new_mcell( ftx, 
                &mcelluid, 
                bf_attr, 
                domain_p);

    if ( sts != EOK ) {
        SNAP_STATS( new_snap_mcell_new_mcell_failed );

        ADVFS_CRASH_RECOVERY_TEST(domain_p, AdvfsCrash_436);
        ftx_fail( ftx );
        ADVFS_CRASH_RECOVERY_TEST(domain_p, AdvfsCrash_437);
        return sts;
    }

    *new_prim_mcell = mcelluid.mcell;

    ADVFS_CRASH_RECOVERY_TEST(domain_p, AdvfsCrash_43);
    
    ftx_done_u( ftx, FTA_SNAP_NEW_MCELL, sizeof(mcelluid), &mcelluid );

    ADVFS_CRASH_RECOVERY_TEST(domain_p, AdvfsCrash_44);

    return EOK;
}

/*
 * BEGIN_IMS
 *
 * advfs_new_snap_mcell_undo_opx - This routine is the undo for
 * advfs_new_snap_mcell.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */

opxT advfs_new_snap_mcell_undo_opx;
void
advfs_new_snap_mcell_undo_opx(
                    ftxHT       ftxH,
                    int         opRecSz,
                    void*       opRecp
                    )
{
    mcellUIdT undoR;
    domainT *dmnP;
    vdT* vdp;
    rbfPgRefHT pgref;
    bsMPgT* bmtpgp;
    statusT sts;
    bsMCT* mcp;
    bsBfAttrT* odattrp;

    bcopy(opRecp, &undoR, sizeof(mcellUIdT));
    SNAP_STATS( new_snap_mcell_undo_opx );

    if (opRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("advfs_new_snap_mcell_undo_opx: bad root done record size");
    }

    dmnP = ftxH.dmnP;

    if ((FETCH_MC_VOLUME(undoR.mcell) > BS_MAX_VDI) ||
        ((vdp = VD_HTOP(FETCH_MC_VOLUME( undoR.mcell ), dmnP)) == 0 )) {
        ADVFS_SAD1( "advfs_new_snap_mcell_undo_opx: vd index no good", 
                FETCH_MC_VOLUME( undoR.mcell ) );
    }

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vdp->bmtp,
                    undoR.mcell.page, BS_NIL, ftxH, MF_NO_VERIFY );
    if (sts != EOK) {
        domain_panic(dmnP,
            "advfs_new_snap_mcell_undo_opx: failed to access bmt pg %ld, err %d",
            undoR.mcell.page, sts );
        return;
    }

    if ( bmtpgp->bmtPageId != FETCH_MC_PAGE(undoR.mcell) ) {
        domain_panic(dmnP, "advfs_new_snap_mcell_undo_opx: got bmt page %ld instead of %ld",
                   bmtpgp->bmtPageId, undoR.mcell.page );
        return;
    }

    mcp = &bmtpgp->bsMCA[undoR.mcell.cell];

    /* find attributes, set state invalid */

    odattrp = bmtr_find( mcp, BSR_ATTR, dmnP);
    if ( !odattrp ) {
        ADVFS_SAD0("advfs_new_snap_mcell_undo_opx: can't find bf attributes");
    }

    rbf_pin_record( pgref, 
                        &odattrp->state,
                        sizeof( odattrp->state ) );
    odattrp->state = BSRA_INVALID;

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_45);

    sts = dealloc_mcells (dmnP, undoR.mcell, ftxH);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_46);

    if (sts != EOK) {
        domain_panic(dmnP, "advfs_new_snap_mcell_undo_opx: dealloc_mcells() failed - sts %d", sts);
        return;
    }
}



/*
 * BEGIN_IMS
 *
 * advfs_get_first_extent_mcell - This routine will attempt to get a
 * snapshots first extent mcell.  The first attempt at getting an mcell will
 * be on the same volume as the primary mcell.  If that fails, the mcell
 * will be acquired from another volume.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */
static
statusT
advfs_get_first_extent_mcell(
        bfTagT          bf_set_tag,
        bfAccessT*      bfap,
        bf_fob_t        fob_cnt,
        ftxHT           parent_ftx,
        bfMCIdT*        return_mcell_id)
{

    bfMCIdT mcellId;
    statusT sts;
    int32_t     skip_cnt        = 0;
    int32_t     max_skip_cnt    = 0;
    vdIndexT    *skip_vd_idx    = NULL;
    vdIndexT    vd_idx;
    vdT         *vdp;

    /*
     * FIX - check if disk is still in the service class??
     */
    vd_idx = FETCH_MC_VOLUME(bfap->primMCId);
    sts = stg_alloc_new_mcell( bfap,
                               bf_set_tag,
                               bfap->tag,
                               vd_idx,
                               parent_ftx,
                               &mcellId,
                               FALSE);
    if (sts == EOK) {
        SNAP_STATS( get_first_extent_mcell_stg_alloc_succeeded );
        MS_SMP_ASSERT( vd_idx == FETCH_MC_VOLUME(mcellId));
        *return_mcell_id = mcellId;
        return sts;
    }

    while (1) {

        /*
         * Add the previous disk to the skip list.
         */

        if (skip_cnt== max_skip_cnt) {
            sts = extend_skip (&max_skip_cnt, &skip_vd_idx);
            if (sts != EOK) {
                SNAP_STATS( get_first_extent_mcell_extent_skip_failed );
                RAISE_EXCEPTION (sts);
            }
        }
        skip_vd_idx[skip_cnt] = vd_idx;
        skip_cnt++;

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    bfap->reqServices,
                                    skip_cnt,
                                    skip_vd_idx,
                                    fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE,
                                    bfap->bfPageSz / ADVFS_FOBS_PER_DEV_BSIZE,
                                    FALSE); /** return stgMap_lk unlocked **/
        if (sts != EOK) {
            SNAP_STATS( get_first_extent_mcell_selec_vd_failed );
            RAISE_EXCEPTION (sts);
        }

        sts = stg_alloc_new_mcell( bfap,
                                   bf_set_tag,
                                   bfap->tag,
                                   vdp->vdIndex,
                                   parent_ftx,
                                   &mcellId,
                                   FALSE);
        vd_dec_refcnt(vdp);
        if (sts == EOK) {
            SNAP_STATS( get_first_extent_mcell_stg_alloc_failed_2 );
            MS_SMP_ASSERT(vdp->vdIndex == FETCH_MC_VOLUME(mcellId));
            ms_free (skip_vd_idx);
            *return_mcell_id = mcellId;
            return sts;
        }
        /* ignore bad sts - just try to allocate mcell from another disk */

    }  /* end while */

HANDLE_EXCEPTION:
    if (skip_vd_idx!= NULL) {
        ms_free (skip_vd_idx);
    }

    return sts;

}


/*
 * BEGIN_IMS
 *
 * advfs_init_snapshots - This routine registers ftx agents for the
 * snapshot subsystem 
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */

void
advfs_init_snap_ftx()
{
    statusT sts;

    sts = ftx_register_agent_n(FTA_SNAP_NEW_MCELL,
                             &advfs_new_snap_mcell_undo_opx,
                             0,
                             0
                             );
    if (sts != EOK) {
        ADVFS_SAD1( "advfs_init_snap_ftx: register failure", sts );
    }

    sts = ftx_register_agent_n(FTA_BFS_LINK_SNAP,
                             &advfs_bs_bfs_snap_link_undo_opx,
                             0,
                             0
                             );
    if (sts != EOK) {
        ADVFS_SAD1( "advfs_init_snap_ftx: register failure", sts );
    }
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_copy_mcell_chain - This routine will copy all the records in
 * the mcell chain starting at parent_prim_mcell into a chain of mcells
 * starting at child_prim_mcell. This does not affect extent mcells.  The
 * caller is responsible for protecting the mcell chain from modification.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 * EOK - A new mcell has been created an initailized.
 * ENOSPC - There is no storage left to create a new mcell.
 * EIO - An IO Error occured trying to get the new mcell.
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_snap_copy_mcell_chain(
    bfMCIdT     parent_prim_mcell,  /* in - parent primary mcell's id */
    bfMCIdT     child_prim_mcell,   /* in - snapshot primary mcell's id */
    domainT     *dmnP,              /* in - ptr to domain struct */
    ftxHT       parent_ftx          /* in - parent ftx handle */
    )
{
    vdT *oVdp, *cVdp;
    bfPageRefHT pgRef;
    bsMPgT *obmtp;
    bsMCT *omcp;
    bfMCIdT mcid, cmcid;
    bsMRT *rp;
    char *orp, *crp;
    ftxHT ftxH;
    int pgRefed = FALSE, ftxStarted = FALSE, first = TRUE;
    vdIndexT vdIndex;
    statusT sts;

    mcid = parent_prim_mcell;
    cmcid = child_prim_mcell;

    oVdp = VD_HTOP(FETCH_MC_VOLUME(parent_prim_mcell), dmnP);
    cVdp = VD_HTOP(FETCH_MC_VOLUME(child_prim_mcell), dmnP);

    sts = FTX_START_N( FTA_SNAP_RECS_V1, &ftxH, parent_ftx,
                       cVdp->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    ftxStarted = TRUE;

    /*
     * For each mcell in the original bitfile...
     */

    do {
        sts = bs_refpg(&pgRef,
                       (void*)&obmtp,
                       oVdp->bmtp,
                       mcid.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            SNAP_STATS( snap_copy_mcell_chain_refpg_failed );
            RAISE_EXCEPTION( sts );
        }

        pgRefed = TRUE;

        /* get a pointer to the next mcell */
        omcp = &obmtp->bsMCA[ mcid.cell ];

        /* get a pointer to the first mcell record in the current mcell */
        rp = (struct bsMR *) omcp->bsMR0;

        /* copy contents of mcell */
        sts = advfs_snap_mcell(rp, cVdp, &child_prim_mcell, ftxH, first);
        first = FALSE;
        if (sts != EOK) {
            SNAP_STATS( snap_copy_mcell_chain_snap_mcell_failed );
            RAISE_EXCEPTION( sts );
        }

        /*
         * Move on to the next mcell in the orig bitfile's mcell list.
         */

        mcid = omcp->mcNextMCId;

        (void) bs_derefpg( pgRef, BS_CACHE_IT );
        pgRefed = FALSE;

    } while ((FETCH_MC_VOLUME(mcid) == FETCH_MC_VOLUME( parent_prim_mcell ) ) &&
             !((mcid.page == 0) && (mcid.cell == 0)));

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_47);
    
    ftx_done_n( ftxH, FTA_SNAP_RECS_V1 );

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_48);
    
    return EOK;

HANDLE_EXCEPTION:

    SNAP_STATS( snap_copy_mcell_chain_error_case );

    if (pgRefed) {
        (void) bs_derefpg( pgRef, BS_CACHE_IT );
    }

    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_438);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_439);
    }

    return sts;
}



/*
 * BEGIN_IMS
 *
 * advfs_snap_mcell - This routine will copy a single parent mcell into a
 * child mcell.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 * EOK - A new mcell has been created an initailized.
 * ENOSPC - There is no storage left to create a new mcell.
 * EIO - An IO Error occured trying to get the new mcell.
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */
static statusT
advfs_snap_mcell(
        bsMRT           *first_rec_ptr,
        vdT             *cur_vd_p,
        bfMCIdT         *child_mcell_id, /* Modified! */
        ftxHT           parent_ftx,
        int             is_first_call)
{
    statusT sts;
    ftxHT ftxH;
    int ftxStarted = FALSE, i;
    bfMCIdT cur_mcell_id;
    char *parent_rec_ptr, *child_rec_ptr;
    bsMPgT *child_bmtp;
    bsMCT *child_mcell_p;
    rbfPgRefHT cPgRef;

    sts = FTX_START_N(FTA_SNAP_MCELL_V1, &ftxH, parent_ftx,
                      cur_vd_p->dmnP);
    if (sts != EOK) {
        SNAP_STATS( snap_mcell_failed_ftx_start );
        RAISE_EXCEPTION( sts );
    }
    ftxStarted = TRUE;

    /*
     * Select an mcell.  If is_first_call, then use the primary mcell rather
     * than allocating a new one.  The primary mcell is already allocated. 
     */
    if (is_first_call) {
        cur_mcell_id= *child_mcell_id;
        SNAP_STATS( snap_mcell_is_first_call );
    } else {
        SNAP_STATS( snap_mcell_is_not_first_call );
        sts = bmt_alloc_link_mcell (
                                    cur_vd_p->dmnP,
                                    cur_vd_p->vdIndex,
                                    BMT_NORMAL_MCELL,
                                    *child_mcell_id,
                                    ftxH,
                                    &cur_mcell_id,
                                    NULL
                                    );
        if (sts != EOK) {
            SNAP_STATS( snap_mcell_alloc_link_failed );
            RAISE_EXCEPTION( sts );
        }
    }

    /*
     * Advance the mcell pointer 
     */
    *child_mcell_id = cur_mcell_id;

    /*
     * pin down whole mcell record area
     */
    sts = rbf_pinpg( &cPgRef, 
                        (void*)&child_bmtp,
                        cur_vd_p->bmtp, 
                        cur_mcell_id.page, 
                        BS_NIL, ftxH, MF_VERIFY_PAGE );
    if (sts != EOK) {
        SNAP_STATS( snap_mcell_rbf_pinpg_failed );
        RAISE_EXCEPTION( sts );
    }

    /* Get the pointer to the mcell and pin the records */
    child_mcell_p = &child_bmtp->bsMCA[ cur_mcell_id.cell ];
    rbf_pin_record(cPgRef,
                   child_mcell_p->bsMR0,
                   BSC_R_SZ
                   );

    /*
     * For each record in the mcell...
     */
    while (first_rec_ptr->type != BSR_NIL) {

        if ((first_rec_ptr->type != BSR_ATTR) && (first_rec_ptr->type != BSR_XTNTS)) {
            SNAP_STATS( snap_mcell_copying_record );

            /*
             * Reached a record that we want to copy to the child snapshot.
             * Allocating a record should not fail.
             */
            child_rec_ptr = bmtr_assign( 
                    first_rec_ptr->type, 
                    first_rec_ptr->bCnt - sizeof(bsMRT), 
                    child_mcell_p);
            if (child_rec_ptr == NULL) {
                ADVFS_SAD0( "advfs_snap_mcell: assign failed" );
            }

            /*
             * Copy the record from the orig bitfile to the snapshot.
             */
            parent_rec_ptr = (char *)first_rec_ptr+ sizeof( bsMRT );

            for (i = 0; i < first_rec_ptr->bCnt - sizeof( bsMRT ); i++) {
                child_rec_ptr[i] = parent_rec_ptr[i];
                ADVFS_CRASH_RECOVERY_TEST(cur_vd_p->dmnP, AdvfsCrash_49);
            }
        }

        first_rec_ptr = (bsMRT *) ((char *)first_rec_ptr + 
                roundup( first_rec_ptr->bCnt, sizeof( uint64_t )));
    }  /* end while */

    ADVFS_CRASH_RECOVERY_TEST(cur_vd_p->dmnP, AdvfsCrash_50);
    
    ftx_done_n(ftxH, FTA_SNAP_MCELL_V1);

    ADVFS_CRASH_RECOVERY_TEST(cur_vd_p->dmnP, AdvfsCrash_51);

    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        SNAP_STATS( snap_mcell_ftx_failed );
        ADVFS_CRASH_RECOVERY_TEST(cur_vd_p->dmnP, AdvfsCrash_440);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(cur_vd_p->dmnP, AdvfsCrash_441);
    }

    return sts;
}



/*
 * BEGIN_IMS
 *
 * advfs_sync_cow_metadata - This routine is used when writing to an
 * unmapped region of a metadata file in a writeable snapshot.  Currently
 * unsupported. 
 *
 * Parameters:
 *  
 * 
 * Return Values:
 * EOK - A new mcell has been created an initailized.
 * ENOSPC - There is no storage left to create a new mcell.
 * EIO - An IO Error occured trying to get the new mcell.
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */

statusT
advfs_sync_cow_metadata(
        bfAccessT*      bfap,
        off_t           offset,
        size_t          size,
        ftxHT           parent_ftx)
{
    return ENOT_SUPPORTED;
}



/*
 * BEGIN_IMS
 *
 * advfs_snap_hard_out_of_sync - This routine will set the BS_TD_OUT_OF_SYNC_SNAP
 * flag in the tag entry for the fileset specified.  A user can not mount
 * this fileset after being marked hard out-of-sync.
 *
 * Parameters:
 * dmn_p - Domain pointer
 * bf_set_id - Id of fileset we are marking hard out-of-sync
 * parent_ftx - Passed in transaction handle.
 *  
 *
 * Return Values:
 * ENO_SUCH_TAG
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 * 
 *
 * END_IMS
 */

static
statusT
advfs_snap_hard_out_of_sync(
        domainT*        dmn_p,
        bfSetIdT        bf_set_id,
        ftxHT           parent_ftx)
{
    bfSetT*             root_bfset_p;
    bfAccessT*          dir_acc_p;
    bsBfSetAttrT        set_attr;
    bsBfSetAttrT*       update_set_attr_p;
    rbfPgRefHT          pg_ref;
    bfTagT              dir_tag;
    bfTagFlagsT         tag_flags;
    bfMCIdT             mcid;
    bfSetT*             set_p;
    bfAccessT*          tag_dir_bfap;
    statusT             sts = EOK;

    MS_SMP_ASSERT(dmn_p != NULL);

    root_bfset_p = dmn_p->bfSetDirp;

    dir_tag = bf_set_id.dirTag;

    sts = tagdir_lookup_full(root_bfset_p, &dir_tag, &mcid, &tag_flags);
    if (sts != EOK && (sts != E_OUT_OF_SYNC_SNAPSHOT)) {
        return sts;
    }
    
    if (tag_flags & BS_TD_OUT_OF_SYNC_SNAP) {
        /* Nothing to do */
        return EOK;
    }

    /* Now set the tagFlags in the tag entry for the fileset to 
     * out of sync.  This is a hard out-of-sync.  The fileset is out
     * of commission and it can't be mounted.
     */
    sts = advfs_bs_tagdir_update_tagmap(
            bf_set_id.dirTag,
            root_bfset_p,
            BS_TD_OUT_OF_SYNC_SNAP,
            bsNilMCId,
            parent_ftx,
            TDU_SET_FLAGS|TDU_SKIP_FTX_UNDO);

    if (sts != EOK) {
        domain_panic(dmn_p, 
                "advfs_snap_hard_out_of_sync failed to update tagflags, return "
                "code = %d", 
                sts);
        return sts;
    }

    set_p = bfs_lookup(bf_set_id);
    if (BFSET_VALID(set_p)) {
        ADVSMP_BFSET_LOCK(&set_p->bfSetMutex);
        set_p->bfSetFlags |= BFS_OD_HARD_OUT_OF_SYNC;
        ADVSMP_BFSET_UNLOCK(&set_p->bfSetMutex);
    }

    /* TODO: Generate an event? */
    return EOK;
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_hard_out_of_sync_recursive - 
 * This routine will set the BS_TD_OUT_OF_SYNC_SNAP
 * flag in the tag entry for the fileset specified and it's children and siblings
 * (recursively).
 *
 * Parameters:
 * dmn_p - Domain pointer
 * cur_bf_set_id - Id of fileset we are marking hard out-of-sync
 * parent_ftx - Passed in transaction handle.
 *  
 *
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 * + mark current fileset hard out of sync
 * + recurse to children
 * + recurse to siblings
 * 
 *
 * END_IMS
 */

static
statusT
advfs_snap_hard_out_of_sync_recursive(
        domainT*        dmn_p,
        bfSetIdT        cur_bf_set_id,
        ftxHT           parent_ftx)
{
    statusT      sts;
    bfAccessT   *tag_dir_bfap;
    bsBfSetAttrT bf_set_attr;

    if (BS_BFS_EQL(cur_bf_set_id, nilBfSetId)) {
        return EOK;
    }

    sts = advfs_snap_hard_out_of_sync(dmn_p, cur_bf_set_id, parent_ftx);
    if (sts) {
        return sts;
    }

    sts = bs_access_one( &tag_dir_bfap,
                        cur_bf_set_id.dirTag,
                        dmn_p->bfSetDirp,
                        parent_ftx,
                        BF_OP_INTERNAL);
    if (sts != EOK) {
        return sts;
    }

    sts = bmtr_get_rec(tag_dir_bfap,
                        BSR_BFS_ATTR,
                        (void *) &bf_set_attr,
                        sizeof( bf_set_attr) );
    if (sts != EOK) {
        (void) bs_close_one( tag_dir_bfap, MSFS_CLOSE_NONE, parent_ftx );
        return sts;
    }

    (void) bs_close_one( tag_dir_bfap, MSFS_CLOSE_NONE, parent_ftx );

    /* recurse to children */
    sts = advfs_snap_hard_out_of_sync_recursive(dmn_p,
            bf_set_attr.bfsaFirstChildSnapSet,
            parent_ftx);
    if (sts != EOK) {
        return sts;
    }

    /* recurse to siblings */
    sts = advfs_snap_hard_out_of_sync_recursive(dmn_p,
            bf_set_attr.bfsaNextSiblingSnap,
            parent_ftx);
    if (sts) {
        return sts;
    }

    return EOK;
}



/*
 * BEGIN_IMS
 *
 * advfs_snap_out_of_sync - This routine marks a snapshot and a fileset as
 * out of sync.  The routine takes the tag and bfap of the file to be marked
 * out of sync.  If bfap is NULL, the file is only marked out of sync on
 * disk (in the tag directory, tag flags) in bf_set_p.  If bfap is non-NULL,
 * then bfap is marked as out of sync by setting BFA_OUT_OF_SYNC.  If the
 * passed in bfap is already marked BFA_OUT_OF_SYNC, it is assumed that it
 * is already marked out of sync on disk. 
 *
 * Parameters:
 * tag - specifies the tag of the file to mark as out of sync
 * bfap - specifies the access structure for tag.  Can be NULL.
 * bf_set_p - specifies the fileset of tag. Cannot be NULL.
 *  
 * 
 * Return Values:
 * EOK - The requested fileset is marked out of sync.
 * non-EOK - The domain has paniced because a write to metadata failed.
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 * This routine will start a transaction under which to mark the bfSet and
 * the file out of sync.  The transaction will be completed with a special
 * done mode to prevent undoing in the case of a failed higher level
 * transaction.  When a file is marked as out of sync, all its UFC pages
 * will be invalidated to prevent further access to the pages.
 *
 * END_IMS
 */

statusT
advfs_snap_out_of_sync(
        bfTagT          tag,
        bfAccessT*      bfap,
        bfSetT*         bf_set_p,
        ftxHT           parent_ftx)
{
    ftxHT   cur_ftx;
    rbfPgRefHT          pg_ref;
    int16_t             pin_bf_set = FALSE;
    bsBfSetAttrT*       set_attr_p = NULL;
    statusT             sts        = EOK;


    MS_SMP_ASSERT( bf_set_p != NULL );

    if ( (bfap) && (bfap->bfaFlags & BFA_OUT_OF_SYNC) &&
         (bf_set_p->bfSetFlags & BFS_OD_OUT_OF_SYNC) ) {
        SNAP_STATS( snap_out_of_sync_no_work );
        return EOK;
    }

    sts = FTX_START_N( FTA_SNAP_OUT_OF_SYNC, 
                        &cur_ftx, 
                        parent_ftx, 
                        bf_set_p->dmnP);
    if (sts != EOK ) {
        ADVFS_SAD0(" Transaction start failed in advfs_snap_out_of_sync");
    }


    /* Start with marking the fileset out of sync as appropriate */
    if (!(bf_set_p->bfSetFlags & BFS_OD_OUT_OF_SYNC) ) {
        SNAP_STATS( snap_out_of_sync_setting_bfset_out_of_sync );
        /* Fileset is not already out of sync, so mark it out of sync */
        sts = bmtr_get_rec_ptr( bf_set_p->dirBfAp,
                                cur_ftx,
                                BSR_BFS_ATTR,
                                sizeof( bsBfSetAttrT ),
                                TRUE, /* Pin the page */
                                (void **)&set_attr_p,
                                &pg_ref,
                                NULL);
        if (sts != EOK) {
            SNAP_STATS( snap_out_of_sync_get_rec_ptr_failed );

            domain_panic( bf_set_p->dmnP, "advfs_snap_out_of_sync: bmtr_get_rec_ptr failed. Return code = %d", sts);
            goto err_fail_ftx;
        }

        /* Once a record is pinned, the transaction cannot be failed.  We
         * may need to do IO to get the tag directory entry for tag, so we
         * will postpone pinning a page until all IO is done. */
        pin_bf_set = TRUE;

    }


    /* Now mark the file as out of sync.  To mark the file out of sync, the 
     * tagFlag BS_TD_OUT_OF_SYNC will be set in the tag directory of the
     * fileset of the tag passed in.  */
    if  ( (!BS_BFTAG_EQL(tag, NilBfTag)) && 
          ( (bfap == NULL) || 
            ( (bfap != NULL) && !(bfap->bfaFlags & BFA_OUT_OF_SYNC)) ) ) {
        
        sts = advfs_bs_tagdir_update_tagmap(
                tag, 
                bf_set_p,
                BS_TD_OUT_OF_SYNC_SNAP,
                bsNilMCId,
                cur_ftx,
                TDU_SET_FLAGS|TDU_SKIP_FTX_UNDO);

        if ( sts != EOK ) {
            SNAP_STATS( snap_out_of_sync_update_tagmap_failed );

            domain_panic( bf_set_p->dmnP, 
                    "advfs_snap_out_of_sync: update tagmap failed. "
                    "Return code = %d", sts);
            goto err_fail_ftx;
        }

        /* Now mark the bfap as out of sync */
        if (bfap != NULL) {
            SNAP_STATS( snap_out_of_sync_setting_fail_out_of_sync_2 );

            ADVFS_ACCESS_LOGIT( bfap, "advfs_snap_out_of_sync setting BFA_OUT_OF_SYNC");
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags |= BFA_OUT_OF_SYNC;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

            fcache_vn_invalidate( &bfap->bfVnode,
                                0, 0, /* The entire file */
                                NULL,
                                (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL );

        }
    }


    if (pin_bf_set) {
        SNAP_STATS( snap_out_of_sync_pinning_bfset );

        /* Cannot fail cur_ftx after this */
        rbf_pin_record( pg_ref, 
                        &set_attr_p->flags,
                        sizeof( set_attr_p->flags ) );
        set_attr_p->flags |= BFS_OD_OUT_OF_SYNC;

        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_52);

        /* Now it is out of sync on disk (or in the log at least).  Next,
         * mark it out of sync in memory */
        ADVSMP_BFSET_LOCK( &bf_set_p->bfSetMutex );
        bf_set_p->bfSetFlags |= BFS_OD_OUT_OF_SYNC;
        ADVSMP_BFSET_UNLOCK( &bf_set_p->bfSetMutex );
    }


    /* Finish the transaction and prevent undos from removing the out of
     * sync state on this tag/bfSet */
    
    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_53);
    
    ftx_special_done_mode( cur_ftx, FTXDONE_SKIP_SUBFTX_UNDO );
    
    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_54);
    
    ftx_done_n( cur_ftx, FTA_SNAP_OUT_OF_SYNC);

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_55);

    /*
     * We need to make sure this log record is on disk and will be redone
     * otherwise we open ourselves up to the possibility of modifications to
     * the parent being written to the disk and having the child lose it's
     * out of sync state after a crash.
     */
    advfs_lgr_flush( bf_set_p->dmnP->ftxLogP, nilLSN, FALSE );

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_56);

    return sts;


err_fail_ftx:
    SNAP_STATS( snap_out_of_sync_error_case );

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_442);
    ftx_fail( cur_ftx );
    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_443);
    return sts;

}


/*
 * BEGIN_IMS
 *
 * advfs_snap_children_out_of_sync - This routine marks all children of bfap
 * as out of sync.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_snap_children_out_of_sync(
        bfAccessT*      bfap,
        ftxHT           parent_ftx)
{
    bfAccessT   *cur_bfap;
    bfSetT      *cur_bfsetp;
    statusT      sts = EOK;

    if (bfap == NULL) {
        SNAP_STATS( snap_children_out_of_sync_no_work );
        return EOK;
    }
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
    SNAP_STATS( snap_children_out_of_sync_called );


    /* Mark the the children out of sync.  
     * Loop through the children already loaded in memory
     */
    cur_bfap = bfap->bfaFirstChildSnap;
    while (cur_bfap != NULL) {
        advfs_snap_out_of_sync( cur_bfap->tag,
                cur_bfap,
                cur_bfap->bfSetp,
                parent_ftx);
        cur_bfap = cur_bfap->bfaNextSiblingSnap;
    }

    /* Loop through all children (on-disk). */
    cur_bfsetp = bfap->bfSetp->bfsFirstChildSnapSet;
    while (cur_bfsetp != NULL) {
        advfs_snap_out_of_sync( bfap->tag,
                NULL,
                cur_bfsetp,
                parent_ftx);
        cur_bfsetp = cur_bfsetp->bfsNextSiblingSnapSet;
    }

    return sts;
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_print_hard_out_of_sync - This routine is used to inform the user
 * that a fileset is hard out-of-sync 
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

void
advfs_snap_print_hard_out_of_sync(
        bfSetIdT         bf_set_id)
{
    uprintf(" A snapshot filesystem");
    uprintf(" is out of sync and inconsistent with its parent filesystem.\n");
    uprintf(" Access to files in the snapshot filesystem has been disabled.\n");
    uprintf(" Snapshot Filesystem ID = %08x.%08x.%016lx.%04x\n", 
            (uint32_t)bf_set_id.domainId.id_sec,
            (uint32_t)bf_set_id.domainId.id_usec,
            bf_set_id.dirTag.tag_num,
            bf_set_id.dirTag.tag_seq);
    return;
}
/*
 * BEGIN_IMS
 *
 * advfs_snap_print_out_of_sync - This routine is used to inform the user
 * that a file and fileset are out of sync.  If tag is NilBfTag, then only
 * the fileset will be out of sync.  If tag is not Nil, then the user will
 * be informed that all access to the file is now prohibited.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_snap_print_out_of_sync(
        bfTagT          tag,
        bfSetT*         bf_set_p)
{
    if ( !BS_BFTAG_EQL(tag, NilBfTag ) ) {
        ms_uaprintf( "WARNING: AdvFS cannot copy-on-write data to a snapshot file.\n");
        ms_uaprintf( "Further access to the file is prohibited. ");
    }

    ms_uaprintf(" The snapshot filesystem : %s\n", bf_set_p->bfSetName);
    ms_uaprintf(" is out of sync with its parent filesystem. \n");
    ms_uaprintf(" Access to files in the snapshot filesystem will be restricted\n");
    ms_uaprintf(" to files that are still in sync with the parent filesystem. ");
    ms_uaprintf(" Continued use of the snapshot filesystem is discouraged. ");
    ms_uaprintf(" Snapshot Filesystem ID = %08x.%08x.%016lx.%04x\n", 
            bf_set_p->bfSetId.domainId.id_sec,
            bf_set_p->bfSetId.domainId.id_usec,
            bf_set_p->bfSetId.dirTag.tag_num,
            bf_set_p->bfSetId.dirTag.tag_seq);
    ms_uaprintf(" Parent Filesystem ID = %08x.%08x.%016lx.%04x\n", 
            bf_set_p->bfsParentSnapSet->bfSetId.domainId.id_sec,
            bf_set_p->bfsParentSnapSet->bfSetId.domainId.id_usec,
            bf_set_p->bfsParentSnapSet->bfSetId.dirTag.tag_num,
            bf_set_p->bfsParentSnapSet->bfSetId.dirTag.tag_seq);

    if ( !(BS_BFTAG_EQL(tag, NilBfTag) ) ) {
        ms_uaprintf("WARNING: An AdvFS snapshot file is out of sync with its\n");
        ms_uaprintf("parent file.  Further access is prohibited\n");
        ms_uaprintf("File ID = %016lx.%04x\n", tag.tag_num, tag.tag_seq);

    }
    return EOK;

}


/*
 * BEGIN_IMS
 *
 * advfs_make_cow_hole -This routine inserts a cowed hole into a bfap's
 * extent maps.  A COWED_HOLE is a hole that starts with a vd_blk of -2.  It
 * marks that an extent was a hole in a parent snapshot and the hole has
 * been modified in the parent causing a COW to occur on the hole.  The
 * start_fob and num_fobs describe the entire hole to be COWed.  
 *
 * Parameters:
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      On entry, it is assumed that the xtntMap_lk and the mcellList_lk of
 *      the file to have a COWED_HOLE inserted can be acquired for write. 
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
statusT
advfs_make_cow_hole( 
        bfAccessT*      bfap,
        bf_fob_t        start_fob,
        bf_fob_t        num_fobs,
        ftxHT           parent_ftx )
{
    bsInMemXtntT        *snap_xtnts;
    bsInMemXtntDescIdT  xtnt_desc_id;
    bsXtntDescT         xtnt_desc;
    ftxHT               cur_ftx;
    statusT     sts         = EOK;
    bf_fob_t    end_fob     = start_fob + num_fobs;


    /* 
     * Acquire the mcellList_lk and xtntMap_lk for bfap.
     */
    sts = FTX_START_N( FTA_NULL, &cur_ftx, parent_ftx, bfap->dmnP );
    if (sts != EOK) {
        /* This should mark the child as out of sync and return gracefully
         * */
        ADVFS_SAD0( "Failed to start a transaction in advfs_make_cow_hole");
    }


    ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );

    /* 
     * The hole will be inserted from [start_fob..start_fob+num_fobs].
     * Make sure that the end_fob is not beyond the COW-able region.
     */
    MS_SMP_ASSERT( end_fob < roundup( bfap->bfa_orig_file_size, VM_PAGE_SZ ) );

    snap_xtnts= &bfap->xtnts;
    MS_SMP_ASSERT( snap_xtnts->validFlag == XVT_VALID );

    /* The update flag to imm_get_xtnt_desc is FALSE here.  Assert that that
     * is the correct decision to make */
    MS_SMP_ASSERT( snap_xtnts->xtntMap->updateStart >= snap_xtnts->xtntMap->cnt );

    sts = imm_get_xtnt_desc( start_fob,
                       snap_xtnts->xtntMap,
                       FALSE,
                       &xtnt_desc_id,
                       &xtnt_desc);

    if ( xtnt_desc.volIndex == bsNilVdIndex ) {
        /* The range is not described in the extent maps of the snapshot.
         * It is neither storage, a COWED_HOLE, nor an unmapped hole (-1). */

        /* Add a COWED_HOLE descriptor after the last descriptor. */
        SNAP_STATS( make_cow_hole_calling_append_cow_hole );
        sts = advfs_append_cow_hole( bfap,
                                        snap_xtnts->xtntMap,
                                        start_fob,
                                        end_fob,
                                        cur_ftx);
    } else {
        /* There are descriptors before and after the range to become a
         * COWED_HOLE. */
        MS_SMP_ASSERT(xtnt_desc.bsxdFobCnt != 0);
        if ( xtnt_desc.bsxdVdBlk == COWED_HOLE) {
            /* The snapshot already has a COWED hole here. Nothing to do. */
            SNAP_STATS( make_cow_hole_found_cow_hole );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_444);
            ftx_fail( cur_ftx );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_445);
            ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
            return EOK;
        }

        if ( xtnt_desc.bsxdVdBlk != XTNT_TERM ) {
            /* The snapshot already has extents here. Nothing to do. */
            SNAP_STATS( make_cow_hole_found_stg );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_446);
            ftx_fail( cur_ftx );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_447);
            ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
            return EOK;
        }

        /* Insert the COWED_HOLE into the child's extent maps */
        sts = advfs_insert_cow_hole( bfap,
                                snap_xtnts->xtntMap,
                                xtnt_desc_id,
                                start_fob,
                                end_fob,
                                cur_ftx);
    }

    if (sts == EOK) {
        SNAP_STATS( make_cow_hole_updating_on_disk_xtnts );
        sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                        bfap,
                                        snap_xtnts->xtntMap,
                                        cur_ftx );

        ADVRWL_XTNTMAP_WRITE( &bfap->xtntMap_lk );

        if (sts != EOK) {
            SNAP_STATS( make_cow_hole_update_failed );
            unload_sub_xtnt_maps (snap_xtnts->xtntMap);
        } else {
            /*
             * * Merge extent maps will reset the updateStart value of the xtnts.
             * */
            SNAP_STATS( make_cow_hole_merging_maps );
            merge_xtnt_maps( snap_xtnts);
        }
        
        ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
    }



    if (sts != EOK) {
        SNAP_STATS( make_cow_hole_sts_not_ok );

        /* Something happened while updating the extent maps. Invalidate the 
         * in memory copies to make sure everything is valid for use by
         * other readers (who may need to go to disk to get the maps). */
#ifdef SNAPSHOTS
        /* SHOULD PROBABLY MARK BFAP AS OUT OF SYNC */
#endif
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_448);
        ftx_fail( cur_ftx );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_449);
    } else {
        SNAP_STATS( make_cow_hole_finished_ftx );

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_57);
        
        ftx_done_n( cur_ftx, FTA_NULL );

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_58);
    }

    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );

    return sts;


}



/*
 * BEGIN_IMS
 *
 * advfs_append_cow_hole - This is a helper routine to advfs_make_cow_hole.
 * This routine adds a new COWED_HOLE to a range of a snapshots extent maps
 * that has an implied unmapped region. (The range is not a hole that starts
 * with a vd_blk of -1, it is just not mentioned at all).
 *
 *
 * Parameters:
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_append_cow_hole( 
        bfAccessT*       bfap,
        bsInMemXtntMapT* xtnt_map,
        bf_fob_t         start_fob,
        bf_fob_t         end_fob,
        ftxHT            parent_ftx)
{
    bfSetT *bfSetp;
    uint32_t last_sub_xtnt_idx;         /* last subextent index */
    uint32_t update_sub_xtnt_idx;       /* update subextent index */
    statusT sts;
    bsInMemSubXtntMapT *sub_xtnt_map;


    bfSetp = bfap->bfSetp;

    /* This check should be true unless we attempt to insert a COWED_HOLE
     * while actively changing extents.  This shouldn't happen.  If it does,
     * see the updateFlg in make_perm_hole on Tru64. */
    MS_SMP_ASSERT(xtnt_map->cnt == xtnt_map->validCnt);

    last_sub_xtnt_idx = xtnt_map->validCnt - 1;
    /* start_fob is after this subextent. */
    MS_SMP_ASSERT(start_fob >= xtnt_map->subXtntMap[last_sub_xtnt_idx].bssxmFobOffset+
            xtnt_map->subXtntMap[last_sub_xtnt_idx].bssxmFobCnt);

    xtnt_map->origStart = last_sub_xtnt_idx;
    xtnt_map->origEnd = last_sub_xtnt_idx;

    update_sub_xtnt_idx = xtnt_map->cnt;
    xtnt_map->updateStart = update_sub_xtnt_idx;
    if ( update_sub_xtnt_idx >= xtnt_map->maxCnt ) {
        SNAP_STATS( append_cow_hole_extending_map );

        /* One more decriptor needed to hold the COWED_HOLE start. */
        sts = imm_extend_xtnt_map( bfap,
                                xtnt_map,
                                XTNT_ORIG_XTNTMAP);
        if ( sts != EOK ) {
            SNAP_STATS( append_cow_hole_extend_failed );
            return sts;
        }
    }

    sts = imm_copy_sub_xtnt_map( bfap,
            xtnt_map,
            &xtnt_map->subXtntMap[last_sub_xtnt_idx],
            &xtnt_map->subXtntMap[update_sub_xtnt_idx]);
    if ( sts != EOK ) {
        SNAP_STATS( append_cow_hole_copy_failed );
        return sts;
    }
    xtnt_map->cnt++;

    sub_xtnt_map = &xtnt_map->subXtntMap[update_sub_xtnt_idx];

    if ( (sub_xtnt_map->cnt == 1) && (update_sub_xtnt_idx == 1) ) {
        MS_SMP_ASSERT(sub_xtnt_map->bsXA[0].bsx_vd_blk == XTNT_TERM);
        SNAP_STATS( append_cow_hole_case_1 );

        /*
         * This file has one subextent with one descriptor and it does not 
         * have any storage allocated to it.  If needed, allocate an 
         * mcell for the sub extent map before allocating storage.
         */
        if ( (sub_xtnt_map->mcellId.page == bsNilMCId.page) &&
                (sub_xtnt_map->mcellId.cell == bsNilMCId.cell) )
        {
            SNAP_STATS( append_cow_hole_case_2 );

            /* 
             * advfs_get_first_extent_mcell will allocate the first extent mcell for the
             * snapshot.  It will try to get an mcell on the same volume as
             * the snapshot's primary mcell but if that fails will get one
             * on another volume. 
             */
            sts = advfs_get_first_extent_mcell( bfSetp->dirTag,
                    bfap,
                    0,               /* 0 bytes of stg */
                    parent_ftx,
                    &sub_xtnt_map->mcellId);
            if ( sts != EOK ) {
                SNAP_STATS( append_cow_hole_get_first_failed );
                RAISE_EXCEPTION(sts);
            }

            sub_xtnt_map->mcellState = ADV_MCS_NEW_MCELL;
            xtnt_map->hdrMcellId = sub_xtnt_map->mcellId;
        }
    }

    sts = advfs_add_cow_hole_common(bfap, 
                                xtnt_map, 
                                start_fob, 
                                end_fob, 
                                parent_ftx);

    if ( sts != EOK ) {
        SNAP_STATS( append_cow_hole_add_cow_common_failed );
        RAISE_EXCEPTION(sts);
    }

    xtnt_map->updateEnd = xtnt_map->cnt - 1;

    return EOK;

HANDLE_EXCEPTION:
    SNAP_STATS( append_cow_hole_error_case );

    xtnt_map->updateEnd = xtnt_map->cnt - 1;
    unload_sub_xtnt_maps(xtnt_map);

    return sts;

}



/*
 * BEGIN_IMS
 *
 * advfs_insert_cow_hole - This is a helper routine to advfs_make_cow_hole.
 * This routine inserts a COWED_HOLE where an unmapped range (hole of -1)
 * already exists in a child's extent maps.  If an error occurs, the extent
 * maps will not be modified.
 *
 *
 * Parameters:
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_insert_cow_hole( 
        bfAccessT*         bfap,
        bsInMemXtntMapT*   xtnt_map, 
        bsInMemXtntDescIdT xtnt_desc_id,
        bf_fob_t           start_fob,
        bf_fob_t           end_fob,
        ftxHT              parent_ftx)
{
    bfSetT      *bfSetp;
    bsXtntT     bsXA[2];
    bsXtntT     desc;
    uint32_t    i;
    bsInMemSubXtntMapT *new_sub_xtnt;
    bsInMemSubXtntMapT *old_sub_xtnt;
    bf_fob_t    fob_cnt;
    statusT     sts;

    MS_SMP_ASSERT( HAS_SNAPSHOT_PARENT( bfap ) );
    bfSetp = bfap->bfSetp;

    MS_SMP_ASSERT(xtnt_desc_id.subXtntMapIndex < xtnt_map->cnt);
    
    /* 
     * No update extents should exist.  If this assert is hit, see Tru64's
     * make_perm_hole for guidance 
     */
    MS_SMP_ASSERT( bfap->xtnts.xtntMap->updateStart >= 
                        bfap->xtnts.xtntMap->cnt );

    xtnt_map->updateStart = xtnt_map->cnt;
    xtnt_map->origStart = xtnt_desc_id.subXtntMapIndex;
    xtnt_map->origEnd = xtnt_desc_id.subXtntMapIndex;

    /*
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */
    MS_SMP_ASSERT(xtnt_map->cnt <= xtnt_map->maxCnt);
    if ( xtnt_map->cnt == xtnt_map->maxCnt ) {
        SNAP_STATS( insert_cow_hole_extending_map );
        sts = imm_extend_xtnt_map(bfap,
                        xtnt_map,
                        XTNT_ORIG_XTNTMAP);
        if ( sts != EOK ) {
            SNAP_STATS( insert_cow_hole_extend_failed );
            return sts;
        }
    }

    old_sub_xtnt= &xtnt_map->subXtntMap[xtnt_desc_id.subXtntMapIndex];
    new_sub_xtnt= &xtnt_map->subXtntMap[xtnt_map->cnt];

    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtnt_map,
                                 old_sub_xtnt,
                                 new_sub_xtnt);
    if ( sts != EOK ) {
        SNAP_STATS( insert_cow_hole_copy_failed );
        return sts;
    }

    xtnt_map->cnt++;

    MS_SMP_ASSERT(xtnt_desc_id.xtntDescIndex + 1 < new_sub_xtnt->cnt);
    new_sub_xtnt->cnt = xtnt_desc_id.xtntDescIndex + 1;
    MS_SMP_ASSERT(new_sub_xtnt->cnt <= new_sub_xtnt->maxCnt);

    /* The hole to be inserted overlaps a normal (unmapped) hole extent. */
    MS_SMP_ASSERT(start_fob >=
                  new_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex].bsx_fob_offset);
    MS_SMP_ASSERT(new_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex].bsx_vd_blk == XTNT_TERM);
    MS_SMP_ASSERT(end_fob <= new_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex + 1].bsx_fob_offset);

    sts = advfs_add_cow_hole_common(bfap, 
                                xtnt_map, 
                                start_fob, 
                                end_fob, 
                                parent_ftx);


    if ( sts != EOK ) {
        SNAP_STATS( insert_cow_hole_add_cow_common_failed );
        RAISE_EXCEPTION(sts);
    }

    xtnt_map->updateEnd = xtnt_map->cnt - 1;

    /* sub_xtnt_map array may have changed due to adding a new subextent. */
    /* Recompute old_sub_xtnt & new_sub_xtnt. */
    old_sub_xtnt = &xtnt_map->subXtntMap[xtnt_desc_id.subXtntMapIndex];
    new_sub_xtnt= &xtnt_map->subXtntMap[xtnt_map->updateEnd];

    /*
     * We added the COWED_HOLE in an (unmapped) hole mapped by the extent array.
     * Now, copy the extent descriptors positioned after the hole descriptor.
     */
    if (new_sub_xtnt->bsXA[new_sub_xtnt->cnt - 1].bsx_fob_offset<
        old_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex + 1].bsx_fob_offset) {

        SNAP_STATS( insert_cow_hole_case_1 );

        /*
         * The last new COWED_HOLE fob allocated and the next page after the hole are
         * not contiguous, we need to create a (non COWED) hole descriptor
         * and copy it to the end of the extent map.  This will terminate
         * the COWED hole.  
         */
        imm_split_desc( new_sub_xtnt->bsXA[new_sub_xtnt->cnt - 1].bsx_fob_offset,
                        &old_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex],/* orig*/
                        &desc,                            /* first part */
                        &bsXA[0]);                        /* last part */

        bsXA[1].bsx_fob_offset = 
            old_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex+1].bsx_fob_offset;
        bsXA[1].bsx_vd_blk = XTNT_TERM;

        /* copy bsXA to xtnt_map[cnt-1].bsXA[cnt-1] */
        sts = imm_copy_xtnt_descs( bfap,
                                   FETCH_MC_VOLUME( new_sub_xtnt->mcellId ),
                                   &bsXA[0],
                                   1,
                                   TRUE,
                                   xtnt_map,
                                   XTNT_ORIG_XTNTMAP);

        if ( sts != EOK ) {
            SNAP_STATS( insert_cow_hole_copy_failed_2 );
            RAISE_EXCEPTION (sts);
        }
        old_sub_xtnt = &xtnt_map->subXtntMap[xtnt_desc_id.subXtntMapIndex];

    } else if (old_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex+1].bsx_vd_blk ==
              COWED_HOLE) {
        SNAP_STATS( insert_cow_hole_impossible );

        /* 
         * On HPUX, a hole in a parent is COWed all at once.  Therefore, it
         * should never be the case that a hole just created is found to be
         * adjacent to another COWED_HOLE.  If this is hit, see Tru64 for
         * guidance.
         */
        MS_SMP_ASSERT( FALSE );
    }
    xtnt_desc_id.xtntDescIndex++;

    /*
     * Copy the remaining entries from the split sub extent map.
     */
    sts = imm_copy_xtnt_descs( bfap,
                               FETCH_MC_VOLUME( old_sub_xtnt->mcellId ),
                               &old_sub_xtnt->bsXA[xtnt_desc_id.xtntDescIndex],
                               old_sub_xtnt->cnt-1 - xtnt_desc_id.xtntDescIndex,
                               TRUE,
                               xtnt_map,
                               XTNT_ORIG_XTNTMAP);

    if ( sts != EOK ) {
        SNAP_STATS( insert_cow_hole_copy_failed_3 );
        RAISE_EXCEPTION (sts);
    }

    /*
     * Set the last update sub extent map's "updateEnd" field as
     * imm_copy_xtnt_descs() may have modified the map.
     */
    new_sub_xtnt= &xtnt_map->subXtntMap[xtnt_map->updateEnd];
    new_sub_xtnt->updateEnd = new_sub_xtnt->cnt - 1;
    new_sub_xtnt->bssxmFobCnt =
        new_sub_xtnt->bsXA[new_sub_xtnt->updateEnd].bsx_fob_offset -
        new_sub_xtnt->bssxmFobOffset;


    /* 
     * At this point, the in memory map's update range reflect the COWED_HOLE having been
     * added.  The on disk extents must now be updated.  A failure to
     * acquire a new extent map will cause the in memory update maps to be
     * invalidated.
     */

    /*
     * Allocate new mcells for any new sub extent maps that were added
     * by imm_copy_xtnt_descs().
     */

    for ( i = xtnt_map->updateEnd + 1; i < xtnt_map->cnt; i++ ) {
        new_sub_xtnt = &xtnt_map->subXtntMap[i];
        sts = stg_alloc_new_mcell( bfap,
                                   bfSetp->dirTag,
                                   bfap->tag,
                                   FETCH_MC_VOLUME (new_sub_xtnt->mcellId ),
                                   parent_ftx,
                                   &new_sub_xtnt->mcellId,
                                   TRUE);
        if ( sts != EOK ) {
            SNAP_STATS( insert_cow_hole_mcell_alloc_failed );
            RAISE_EXCEPTION (sts);
        }

        new_sub_xtnt->updateStart = 0;
        new_sub_xtnt->updateEnd = new_sub_xtnt->cnt - 1;
        new_sub_xtnt->mcellState = ADV_MCS_NEW_MCELL;
    }


    xtnt_map->updateEnd = xtnt_map->cnt - 1;

    return sts;

HANDLE_EXCEPTION:
    SNAP_STATS( insert_cow_hole_error_case );

    xtnt_map->updateEnd = xtnt_map->cnt - 1;
    unload_sub_xtnt_maps (xtnt_map);

    return sts;

}


/*
 * BEGIN_IMS
 *
 * advfs_add_cow_hole_common - This routine is a common routine used by
 * advfs_append_cow_hole and advfs_insert_cow_hole.  This routine will add
 * an extent after the last fob in the last subextent map (the update
 * subextent map).  This routine will acquire a new mcell if required.  The
 * only failure possible is the failure to get a new mcell.  If this routine
 * cannot acquire a new mcell, the sub extent is not modified.
 *
 * Parameters:
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_add_cow_hole_common(
        bfAccessT*         bfap, 
        bsInMemXtntMapT*   xtnt_map, 
        bf_fob_t           start_fob,
        bf_fob_t           end_fob,
        ftxHT              parent_ftx)
{
    bsInMemSubXtntMapT *sub_xtnt_map;
    uint32_t bfPageOffset;
    uint32_t xI;           /* extent index of last descriptor */
    statusT sts = EOK;
    uint32_t last_fob;


    sub_xtnt_map = &xtnt_map->subXtntMap[xtnt_map->cnt - 1];
    MS_SMP_ASSERT(sub_xtnt_map->cnt > 0);

    xI = sub_xtnt_map->cnt - 1;
    sub_xtnt_map->updateStart = xI;

    if ( start_fob == sub_xtnt_map->bsXA[xI].bsx_fob_offset ) {
        SNAP_STATS( add_cow_common_case_1 );
        /* Add hole adjacent to the last extent in this subextent. */
        /* The start of the hole will replace the original extent terminator. */
        /* We only need one more descriptor to terminate the hole. */
        if ( xI == sub_xtnt_map->maxCnt - 1 ) {
            SNAP_STATS( add_cow_common_extend_xtnt_map );
            /* We need one more to terminate the COWED_HOLE. */
            sts = imm_extend_sub_xtnt_map(sub_xtnt_map);
            if ( sts == EXTND_FAILURE ) {
                SNAP_STATS( add_cow_common_extend_failed_new_sub );

                /* sub_xtnt_map is full. Get a new one. */
                MS_SMP_ASSERT(sub_xtnt_map->bsXA[sub_xtnt_map->cnt - 1].bsx_vd_blk ==
                              XTNT_TERM);

                sub_xtnt_map->updateEnd = sub_xtnt_map->updateStart;

                /* The COWED_HOLE we are about to create should not 
                 * fill a hole that lives at the end of an extent
                 * map (this is the RULE THAT HOLES MUST STICK TO THE
                 * STORAGE FOLLOWING IT).  Get a new sub xtnt and mcell for
                 * the COWED_HOLE.
                 *
                 */
                sts = advfs_get_new_sub_xtnt(bfap, 
                        xtnt_map, 
                        parent_ftx);

                /* The sub_xtnt_map array has changed. Recalculate sub_xtnt_map. */
                sub_xtnt_map = &xtnt_map->subXtntMap[xtnt_map->cnt - 1];
                xI = 0;
            }
            if ( sts != EOK ) {
                SNAP_STATS( add_cow_common_failure_case_1 );
                RAISE_EXCEPTION(sts);
            }
        }
    } else {
        SNAP_STATS( add_cow_common_failure_case_2 );

        MS_SMP_ASSERT(start_fob > sub_xtnt_map->bsXA[xI].bsx_fob_offset);
        /* The hole starts beyond the last extent. We need 2 more descriptors */
        /* to describe the hole, a COWED_HOLE start and a terminator. */
        xI++;
        if ( sub_xtnt_map->maxCnt < 2 || xI > sub_xtnt_map->maxCnt - 2 ) {
            SNAP_STATS( add_cow_common_failure_check_extend );

            /* There are not enough free descriptors. We must get more. */
            if ( xI + 1 < sub_xtnt_map->onDiskMaxCnt ) {
                SNAP_STATS( add_cow_common_extend_xtnts_2);

                /* There are at least 2 descs left in this subextent (mcell). */
                /* Extend it knowing we will get at least 2 more. */
                sts = imm_extend_sub_xtnt_map(sub_xtnt_map);
                MS_SMP_ASSERT(xI + 1 < sub_xtnt_map->maxCnt);
            } else {
                SNAP_STATS( add_cow_common_extend_on_disk );

                /* There is not enough room in the subextent (mcell), get */
                /* a new one. */
                MS_SMP_ASSERT(sub_xtnt_map->bsXA[sub_xtnt_map->cnt - 1].bsx_vd_blk ==
                              XTNT_TERM);
                last_fob = sub_xtnt_map->bsXA[sub_xtnt_map->cnt - 1].bsx_fob_offset;

                sub_xtnt_map->updateEnd = sub_xtnt_map->updateStart;


                sts = advfs_get_new_sub_xtnt(bfap, 
                                        xtnt_map, 
                                        parent_ftx);

                sub_xtnt_map = &xtnt_map->subXtntMap[xtnt_map->cnt - 1];

                /* Start the new extent where the last subextent left off. */
                sub_xtnt_map->bsXA[0].bsx_fob_offset= last_fob;
                sub_xtnt_map->bsXA[0].bsx_vd_blk = XTNT_TERM;
                /* Since the permanent hole is not adjacent to the last */
                /* extent, this subextent starts with a normal hole. */
                xI = 1;
            }
            if ( sts != EOK ) {
                SNAP_STATS( add_cow_common_failure_case_3 );
                RAISE_EXCEPTION(sts);
            }
        }
    }

    /* Start the COWED_HOLE. */
    sub_xtnt_map->bsXA[xI].bsx_fob_offset = start_fob;
    sub_xtnt_map->bsXA[xI].bsx_vd_blk = COWED_HOLE;
    xI++;

    /* Terminate the COWED_HOLE. */
    sub_xtnt_map->bsXA[xI].bsx_fob_offset = end_fob;
    sub_xtnt_map->bsXA[xI].bsx_vd_blk = XTNT_TERM;
    sub_xtnt_map->updateEnd = xI;
    sub_xtnt_map->cnt = xI + 1;
    MS_SMP_ASSERT(sub_xtnt_map->cnt <= sub_xtnt_map->maxCnt);
    sub_xtnt_map->bssxmFobCnt = sub_xtnt_map->bsXA[sub_xtnt_map->cnt - 1].bsx_fob_offset -
                          sub_xtnt_map->bsXA[0].bsx_fob_offset;

HANDLE_EXCEPTION:
    return sts;


}


/*
 * BEGIN_IMS
 *
 * advfs_get_new_sub_xtnt - This routine is a helper routine to
 * advfs_add_cow_hole_common that will allocate a new sub extent map and a
 * corresponding mcell.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_get_new_sub_xtnt(
        bfAccessT*         bfap,
        bsInMemXtntMapT*   xtnt_map,
        ftxHT              parent_ftx )
{
    bfSetT *bfSetp;
    bsInMemSubXtntMapT *sub_xtnt_map;
    bsInMemSubXtntMapT *prev_sub_xtnt;
    statusT sts;

    bfSetp = bfap->bfSetp;

    /*
     * If we are at the maximum number of sub extent maps, grow the maximum.
     */
    if ( xtnt_map->cnt >= xtnt_map->maxCnt ) {
        SNAP_STATS( advfs_get_new_sub_extend_case );
        sts = imm_extend_xtnt_map(bfap,
                                xtnt_map,
                                XTNT_ORIG_XTNTMAP);
        if (sts != EOK) {
            SNAP_STATS( advfs_get_new_sub_extend_failed );
            RAISE_EXCEPTION (sts);
        }
    }

    prev_sub_xtnt = &xtnt_map->subXtntMap[xtnt_map->cnt - 1];
    sub_xtnt_map = &xtnt_map->subXtntMap[xtnt_map->cnt];
    /* Get a new extent mcell.  Try first on the same volume as the primary
     * mcell, then any other volume.  advfs_get_first_extent_mcell handles getting a new
     * one
     */
    sts = advfs_get_first_extent_mcell( bfSetp->dirTag,
                               bfap,
                               0,                   /* 0 bytes of stg */
                               parent_ftx,
                               &sub_xtnt_map->mcellId);
    if ( sts != EOK ) {
        SNAP_STATS( advfs_get_new_sub_get_first_failed );
        RAISE_EXCEPTION(sts);
    }

    sts = imm_init_sub_xtnt_map( sub_xtnt_map,
                                 (prev_sub_xtnt->bssxmFobOffset+
                                  prev_sub_xtnt->bssxmFobCnt),
                                 0,  
                                 sub_xtnt_map->mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 0);  /* force default value for maxCnt */
    if ( sts != EOK ) {
        SNAP_STATS( advfs_get_new_sub_init_sub_failed );
        RAISE_EXCEPTION(sts);
    }

    sub_xtnt_map->mcellState = ADV_MCS_NEW_MCELL;
    xtnt_map->cnt++;

HANDLE_EXCEPTION:
    return sts;

}


/*
 * BEGIN_IMS
 *
 * advfs_force_cow_and_unlink - This routine will force a COW in a range of
 * a file and potentially unlink the children from the parents.  For
 * userdata, this routine will acquire an active range over the largest amount 
 * of data that advfs_getpage may COW.  This will synchronize with direct IO 
 * although the locking is probably not required.
 *
 * Parameters:
 *
 * bfap - The bfap to have it's data COWed to the child.
 *
 * start_of_cow - The offset in bfap to begin the COW.
 *
 * bytes_to_cow - The number of bytes to cow.
 *
 * snap_flags - SF_NO_UNLINK can be passed in to COW only a portion of
 * a file without unlinking it from the parent.  This will leave the files
 * linked but partly COWed.  Not passing in the SF_NO_UNLINK flag will make
 * the children independent of the parent.
 *
 * parent_ftx - The parent transaction.  This must be used carefully since
 * trying to COW too much data may cause a log half full problem.  The
 * caller must insure the amount of data to be COWed will not cause a log
 * half full problem if a parent_ftx is passed in.
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * On entry, it is assumed that the fileContext lock is held for bfap.  This
 * is only necessary so that the active range lock can be acquired.
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
statusT
advfs_force_cow_and_unlink(
        bfAccessT*      bfap,
        off_t           start_of_cow,
        size_t          bytes_to_cow,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx) /* Be careful passing in a parent_ftx */
{
    uint64_t            bfap_xtnt_cnt = 0;
    fcache_map_dsc_t*   fcmap;
    fcache_as_hdl_t     vas;
    int                 error = EOK, i;
    statusT             sts = EOK, sts2 = EOK;
    statusT             in_sync = EOK;
    size_t              bytes_left; 
    size_t              bytes_to_map;
    size_t              bytes_to_fault = 0;
    faf_status_t        fault_status = 0;
    struct advfs_pvt_param      priv_param;
    off_t               end_of_cow;
    struct              nameidata*      ndp;
    ni_nameiop_t        saved_ndp_flags;
    ftxHT               ftx_handle;
    actRangeT           *arp = NULL;
    actRangeT           *found_arp = NULL;
    bfAccessT           *last_child = NULL;
    extent_blk_desc_t   *last_child_holes= NULL;
    extent_blk_desc_t   *cur_extent;
    uint64_t            last_child_cnt;
    bf_fob_t            ar_start_fob;
    bf_fob_t            ar_end_fob;

    struct      vnode*          saved_vnode;
    struct      bfAccess       *cur_bfap = NULL, *next_bfap = NULL;
    bfSetT                     *cur_bfsetp = NULL;

    ndp = NAMEIDATA();
    saved_ndp_flags = ndp->ni_nameiop;
    saved_vnode = ndp->ni_vp;

    ADVFS_ACCESS_LOGIT_2(bfap, "force_cow called", start_of_cow, bytes_to_cow);

    MS_SMP_ASSERT( (bfap->bfaWriteCnt > 0) || (!FTX_EQ(parent_ftx, FtxNilFtxH)) );

    /*
     * We need to round start_of_cow down to an allocation unit so that
     * checks for beyond EOF don't accidently miss adjancent storage.
     * Example: file_size = 8096 and start_of_cow == 8096.  If bfaNextFob
     * were 16 (16k allocated storage) and we don't fault now then we won't
     * COW that FOB before it is modified.  This would cause directIO to
     * think it needs to do a COW when it should not have to.
     */
    end_of_cow = start_of_cow + bytes_to_cow;
    start_of_cow = rounddown( start_of_cow, bfap->bfPageSz * ADVFS_FOB_SZ );

    if (bytes_to_cow == 0) {
        if (bfap->dataSafety == BFD_USERDATA) {
            bytes_to_cow = bfap->file_size - start_of_cow;
        } else {
            if (bfap->bfa_orig_file_size != ADVFS_ROOT_SNAPSHOT) {
                bytes_to_cow = max(bfap->bfa_orig_file_size,
                                   (off_t)(bfap->bfaNextFob * ADVFS_FOB_SZ))
                             - start_of_cow;
            } else {
                bytes_to_cow = bfap->bfaNextFob * ADVFS_FOB_SZ;
            }
        }
        end_of_cow = start_of_cow + bytes_to_cow;
    } 

    if (bfap->dataSafety == BFD_USERDATA) {
        /* Don't force a cow beyond EOF */
        end_of_cow = min( end_of_cow, bfap->file_size );
    } else {
        MS_SMP_ASSERT( bfap->dataSafety != BFD_LOG );
        end_of_cow = min( end_of_cow, (off_t)(bfap->bfaNextFob * ADVFS_FOB_SZ));
    }
    bytes_to_cow = end_of_cow - start_of_cow;


    /* If we are in a truncate out situation, the range passed in to COW will
     * be past file_size (start offset will be past file_size).  
     * There is no storage that needs to be COWed.
     */
    if (bytes_to_cow < 0) {
        bytes_to_cow = 0;
    }

    /* Access the snapshot children here.  This enables the check for
     * any valid children (not out-of-sync) for COWing.
     * Any metadata for the file will be cowed at this point.
     * This will prevent needing to call access_children if
     * advfs_getpage is called.
     */
    if ( (bfap->bfaFlags & BFA_SNAP_CHANGE) ||
            (HAS_SNAPSHOT_CHILD( bfap ) && 
             (bfap->bfaFirstChildSnap == NULL) ) ) {
        sts = advfs_access_snap_children( bfap,
                parent_ftx,
                &snap_flags );
        in_sync = sts;
    }

    /*
     * There may be nothing to do after all.  Check to see if start_of_cow
     * is greater than file_size of the file to be cowed. Just jump and see
     * if the child needs unlinking.   If there is nothing to cow, 
     * then just check to see if child needs unlinking.
     */
    if ( ( (bfap->dataSafety == BFD_USERDATA) && 
           (start_of_cow >= bfap->file_size) ) || 
         ( (bfap->dataSafety == BFD_METADATA) &&
           (start_of_cow >= (off_t)(bfap->bfaNextFob * ADVFS_FOB_SZ)) ) || 
         (  bytes_to_cow == 0 ) ) {
        goto unlink_child;
    }

    MS_SMP_ASSERT( sts == EOK );
    bytes_left = bytes_to_cow;


    /* Indicate to getpage this not an mmaper */

    ndp->ni_nameiop |= NI_RW;
    ndp->ni_vp = &bfap->bfVnode;


    /* This is temporary and sloppy.  Just fault each file completely */
    if (bfap->dataSafety == BFD_USERDATA) {
        vas = bfap->dmnP->userdataVas;
    } else {
        vas = bfap->dmnP->metadataVas;
    }

    /*
     * Setup an active range for userdata to synchronize with direct IO.
     * The worry is that COWing may bring data into cache under
     * a directIO's active range.  Although this shouldn't be an issue since
     * the data could not be modified and all writers would serialize on the
     * COW completing, we will be safe rather than sorry.
     * We will round out to the largest possible amount advfs_getpage will
     * COW.
     */
    if (bfap->dataSafety == BFD_USERDATA) {
        ar_start_fob = rounddown( ADVFS_OFFSET_TO_FOB_DOWN(start_of_cow),
                                advfs_cow_alloc_units * bfap->bfPageSz );
        ar_end_fob   = roundup( ADVFS_OFFSET_TO_FOB_UP(start_of_cow + 
                                                        bytes_to_cow),
                                advfs_cow_alloc_units * bfap->bfPageSz ) - 1;
        if (snap_flags & SF_DIRECT_IO_APPEND) {
            /*
             * Make sure we don't overlap with the active range inserted
             * by advfs_fs_write_direct() that protects EOF.
             */
            if (advfs_bs_actRange_spans(bfap, bfap->file_size, &found_arp) && 
                    (ar_start_fob < found_arp->arStartFob)) {
                arp = advfs_bs_get_actRange(TRUE);
                arp->arStartFob = ar_start_fob;
                arp->arEndFob = MIN(ar_end_fob, (found_arp->arStartFob-1));
                arp->arState = AR_FORCE_COW;
                arp->arDebug = SET_LINE_AND_THREAD( __LINE__ );
                (void)insert_actRange_onto_list( bfap, arp, &bfap->bfFsContext );
            }
        } else {
            arp = advfs_bs_get_actRange(TRUE);
            arp->arStartFob = ar_start_fob;
            arp->arEndFob = ar_end_fob;
            arp->arState = AR_FORCE_COW;
            arp->arDebug = SET_LINE_AND_THREAD( __LINE__ );
            (void)insert_actRange_onto_list( bfap, arp, &bfap->bfFsContext );
        }
    } 

    /* 
     * Find the last child (in sync) snapshot to use for determining what needs 
     * to be COWed.  Assume that the first child is not out of sync but validate
     * it later.
     */
    MS_SMP_ASSERT(!(bfap->bfaFlags & BFA_SNAP_CHANGE));
    last_child = bfap->bfaFirstChildSnap;
    cur_bfap = last_child;
    if (last_child == NULL) {
        /*
         * Nothing to do!
         */
        goto cleanup_and_exit;
    }

    while (cur_bfap->bfaNextSiblingSnap) {
        if (!(cur_bfap->bfaNextSiblingSnap->bfaFlags & BFA_OUT_OF_SYNC)) {
            last_child = cur_bfap->bfaNextSiblingSnap; 
        }
        cur_bfap = cur_bfap->bfaNextSiblingSnap;
    }

    if (last_child->bfaFlags & BFA_OUT_OF_SYNC) {
        /*
         * There are no child that are not out of sync.  No COWing to be
         * done.
         */
        goto unlink_child;
    }

    /*
     * Find all the holes in the last_child.  We only need to COW those
     * holes that are XTNT_TERM.
     */
    sts = advfs_get_blkmap_in_range( last_child,
                                        last_child->xtnts.xtntMap,
                                        &start_of_cow,
                                        bytes_left,
                                        &last_child_holes,
                                        &last_child_cnt,
                                        RND_VM_PAGE,
                                        EXB_DO_NOT_INHERIT|EXB_ONLY_HOLES,
                                        XTNT_LOCKS_HELD|XTNT_WAIT_OK);
    if (sts != EOK) {
        /*
         * This really should never happen since the child is already open.
         * If it does happen, fall back and fault the entire region.  It's
         * inefficient, but necessary to prevent other snapshots from going
         * out of sync without being noticed.
         *
         * If advfs_getpage can't get at the extents, then it will mark all
         * children as out of sync.
         */
        last_child_holes = (extent_blk_desc_t*)ms_malloc_no_bzero(
                        sizeof( extent_blk_desc_t ) );
        last_child_holes->ebd_next_desc = NULL;
        last_child_holes->ebd_offset = start_of_cow;
        last_child_holes->ebd_byte_cnt = bytes_left;
        last_child_holes->ebd_vd_blk = XTNT_TERM;
        last_child_cnt = 1;
    }

                                
    priv_param = Nil_advfs_pvt_param;
    priv_param.app_starting_offset = start_of_cow;
    priv_param.app_total_bytes = bytes_left;
    priv_param.app_flags = APP_FORCE_COW;
    priv_param.app_parent_ftx = parent_ftx;  /* Usually FtxNilFtxH */
    
    cur_extent = last_child_holes;
    
    while (cur_extent) {

        if (cur_extent->ebd_vd_blk == COWED_HOLE) {
            /*
             * If this isn't an unmapped range, skip it, it's already COWed.
             */
            cur_extent = cur_extent->ebd_next_desc;
            if (cur_extent == NULL) {
                goto unlink_child;
            }
        }

        start_of_cow = cur_extent->ebd_offset;
        bytes_left = cur_extent->ebd_byte_cnt;

        while ( bytes_left > 0 ) {

            if (bfap->dataSafety != BFD_USERDATA) {
                /* Must be metadata.  We want to limit the size we map to the 
                 * address space map size.
                 */
                bytes_to_map = bfap->bfPageSz * ADVFS_FOB_SZ; 
            } else {
                /*
                 * Otherwise map as much as VM will let us.
                 */ 
                bytes_to_map = bytes_left;
            }

            if (!(fcmap = fcache_as_map( vas,
                        &bfap->bfVnode,
                        start_of_cow,
                        bytes_to_map,
                        0UL,      /* To the end of the file */
                        FAM_WRITE,
                        &error) )) {
                if (error) {
                    sts = error;    
                    in_sync = sts;
                    goto unlink_child;
                }
            }
            
            bytes_to_fault = 0;


            MS_SMP_ASSERT( !((bfap->dataSafety == BFD_USERDATA) &&
                         (start_of_cow >= bfap->file_size) ) );
        

            if ( error = fcache_as_fault( fcmap,
                                        NULL,
                                        &bytes_to_fault,
                                        &fault_status,
                                        (uintptr_t)&priv_param,
                                        (FAF_WRITE | FAF_SYNC ))) {

                if (error) {
#ifdef SNAPSHOT
                /* THis shouldn't reqire the FAU_ASYNC flag!!! */
#endif
                    (void)fcache_as_unmap( fcmap, NULL, 0UL, FAU_FREE|FAU_ASYNC);
                    sts = error;
                    in_sync = sts;
                    goto unlink_child;
                }
            }

#ifdef SNAPSHOT
                /* THis shouldn't reqire the FAU_ASYNC flag!!! */
#endif
       
            if (error = fcache_as_unmap( fcmap, NULL, 0UL, FAU_FREE|FAU_ASYNC)) {
                sts = error;
                in_sync = sts;
                goto unlink_child;
            }

            bytes_left -= bytes_to_fault;
            start_of_cow += bytes_to_fault;

        }
        
        cur_extent = cur_extent->ebd_next_desc;
    }


unlink_child:
    /*
     * Unlink the children from the parent required
     */
    if (!(snap_flags & SF_NO_UNLINK) ) {
        /* Remove any refs set by snapshot children from the parents.
         * Do this all the way up to the root parent. */
        cur_bfap = bfap->bfaParentSnap;
        while (cur_bfap != NULL) {
            next_bfap = cur_bfap->bfaParentSnap;
            for (i = 0; i < bfap->bfaRefsFromChildSnaps; i++) {
                (void)bs_close_one(cur_bfap, MSFS_SNAP_DEREF, parent_ftx);
            }
            cur_bfap = next_bfap;
        }

        /*
         * Start a transaction before acquiring the snap lock to avoid a
         * deadlock when updating the tag directories for children.
         */
        sts = FTX_START_N( FTA_NULL, &ftx_handle, parent_ftx, bfap->dmnP );
        MS_SMP_ASSERT(sts == EOK);
        ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
        /* Now unlink all children from this file */
        cur_bfap = bfap->bfaFirstChildSnap;
        while (cur_bfap != NULL) {
            int16_t close_child = FALSE;
            bfAccessT *next_bfap = cur_bfap->bfaNextSiblingSnap;

            ADVFS_ACCESS_LOGIT(cur_bfap, "force_cow next child in parent list");
            /* in memory */
            cur_bfap->bfaParentSnap = NULL;
            ADVSMP_BFAP_LOCK(&cur_bfap->bfaLock);
            cur_bfap->bfaFlags |= BFA_ROOT_SNAPSHOT;
            cur_bfap->bfaFlags &= ~BFA_PARENT_SNAP_OPEN;
            close_child = (cur_bfap->bfaFlags & BFA_OPENED_BY_PARENT);
            ADVSMP_BFAP_UNLOCK(&cur_bfap->bfaLock);

            /* and on disk.  If the on disk update fails, that's a bummer,
             * but what can we do.  We can't let one naughty child prevent
             * us from unlinking the rest (at least in memory).  So, we will
             * just ignore any disk errors, let the domain panic if it has
             * to, and carry on. */

            sts = advfs_bs_tagdir_update_tagmap(
                    cur_bfap->tag, 
                    cur_bfap->bfSetp, 
                    BS_TD_ROOT_SNAPSHOT,
                    bsNilMCId,
                    ftx_handle,
                    TDU_SET_FLAGS);
            if (sts != EOK) {
                /* Oh well! */
                in_sync = sts;
                sts == EOK;
            }
            
            /*
             * If this child was ref'ed by the parent, close that ref.  This
             * may deallocate the child or it may not.  To us it doesn't
             * really matter.
             */
            if (close_child) {
                ADVFS_ACCESS_LOGIT( cur_bfap, "force_cow calling close");
                (void)bs_close_one( cur_bfap, MSFS_SNAP_PARENT_CLOSE, parent_ftx);
            }
            ADVFS_ACCESS_LOGIT( cur_bfap, "force_cow unlinked this child ");
            cur_bfap = next_bfap;
        }
        bfap->bfaFirstChildSnap = NULL;

        ADVRWL_BFAP_SNAP_UNLOCK(bfap);

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_59);
        
        ftx_done_n( ftx_handle, FTA_NULL );

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_60);

        ADVSMP_BFAP_LOCK(&bfap->bfaLock);
        bfap->refCnt -= bfap->bfaRefsFromChildSnaps;
        bfap->bfaRefsFromChildSnaps = 0;
        ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

        /*
         * Now we will restore the permissions on all in-cache pages of the
         * parent file.   We also need to synchronize with the creation of new
         * snapshots but we have a writeCnt on parent so we should be safe.
         */
        if (bfap->bfaFirstChildSnap == NULL) {
            struct advfs_pvt_param pvt_param;
            /*
             * A new snapshot cannot be created so checking for children is
             * sufficient to check if this is the last snapshot.
             */
            bzero(&pvt_param, sizeof( struct advfs_pvt_param) );
            pvt_param.app_flags |= APP_RESTORE_PG_PROTECTION;
            sts = fcache_vn_flush( &bfap->bfVnode, 0L, 0UL,
                    (uintptr_t) &pvt_param, FVF_WRITE | FVF_SYNC );

            if (sts != EOK) {
                in_sync = sts;
            }
            /*
             * If we fail here, we've got problems since the parents pages
             * may be protected and have no way of being unprotected.  Could
             * anything cause us to fail this simple operation?
             */
            MS_SMP_ASSERT( sts == EOK );

        }

        ADVFS_ACCESS_LOGIT(bfap, "unlinked children from this file");


    }

cleanup_and_exit:

    if (last_child_holes) {
        (void)advfs_free_blkmaps( &last_child_holes );
    }

    if (in_sync != EOK) {
        ADVRWL_BFAP_SNAP_READ_LOCK( bfap );
        sts2 = advfs_snap_children_out_of_sync(bfap, parent_ftx);
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        if (sts2 != EOK) {
            domain_panic(bfap->bfSetp->dmnP, "advfs_force_cow_and_unlink: "
                    "Failed to mark snapshots out of sync. Error code = %d",
                    sts2);
        }
    }

    /*
     * Direct IO needs all pages that may have been broght into cache to be
     * invalidated.  Call fcache_vn_invalidate on the range we had 
     * an active range lock over.  If we don't have an active range, then we
     * didn't fault anything in so we don't need to invalidate anything.
     */
    if ( (snap_flags & SF_INVAL_ALL) && (arp) ) {
        off_t  offset;
        size_t size;

        offset = ADVFS_FOB_TO_OFFSET(arp->arStartFob);
        size   = ADVFS_FOB_TO_OFFSET(arp->arEndFob - arp->arStartFob + 1 );
        MS_SMP_ASSERT( offset  % NBPG == 0 &&
                       size    % NBPG == 0 &&
                       size           != 0 );

        sts2 = fcache_vn_invalidate( &bfap->bfVnode,
                                      offset,
                                      size,
                                      NULL,
                                      FVI_INVAL );
        MS_SMP_ASSERT( sts2 == EOK );
    }

    if (arp) {
        ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );
        spin_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        spin_unlock(&bfap->actRangeLock);
        advfs_bs_free_actRange(arp, TRUE);
    }


    ndp->ni_vp = saved_vnode;
    ndp->ni_nameiop = saved_ndp_flags;

    return sts;
}


/*
 * BEGIN_IMS
 *
 * advfs_wait_for_snap_io - This routine waits for IO related to snapshots
 * and issues any necessary remaining IOs.  On an error, on a file other
 * than bfap (one of bfap's children) the child is marked out of sync.  On
 * an error on bfap, the error is returned, all children are marked out of
 * sync, and error_offset contains the lowest offset for an error. *
 *
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_wait_for_snap_io(
        struct ioanchor**       io_anchor_head,
        bfAccessT*              bfap,
        off_t*                  error_offset,
        extent_blk_desc_t**     snap_maps,
        ftxHT                   parent_ftx)
{


    ioanchor_t* io_anchor = *io_anchor_head;
    ioanchor_t* next_io_anchor = NULL;
    statusT     return_sts = EOK;
    statusT     sts = EOK;

    while (io_anchor != NULL) {
        /* We have probably done some for of COWing.  We need to do
         * special processing on all the IO */
        spin_lock( &io_anchor->anchr_lock );
        if (io_anchor->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) {
            SNAP_STATS( wait_for_snap_io_wakeup_on_all_case );

            /* This was a read and we need to issue the associated
             * writes */
            if (io_anchor->anchr_iocounter == 1) {
                SNAP_STATS( wait_for_snap_io_wakeeup_iocounter_one );

                /* The read has completed and the writes can be
                 * issued. Here we only need to be awaken on the last
                 * io.
                 */
                io_anchor->anchr_flags &= ~IOANCHORFLG_WAKEUP_ON_COW_READ;
                io_anchor->anchr_flags |= IOANCHORFLG_WAKEUP_ON_LAST_IO;
                spin_unlock( &io_anchor->anchr_lock );

                /*
                 * Check for errors.  Each iodesc on the list was an error.
                 * If bfap is on the list, mark all children out of sync.
                 * Otherwise, just mark the single errant child out of sync.
                 */
                if (io_anchor->anchr_error_ios != NULL) {
                    SNAP_STATS( wait_for_snap_io_wakeeup_io_error );

                    /*
                     * Deal with marking children out of sync and updating
                     * error offset ass appropriate.
                     */
                    return_sts = advfs_snap_io_wait_error_helper( 
                                        io_anchor,
                                        bfap,
                                        error_offset,
                                        snap_maps,
                                        parent_ftx );
                    /* 
                     * Finish the fake IO.
                     */
                    biodone( io_anchor->anchr_origbuf );
                    next_io_anchor = io_anchor->anchr_listfwd;

                    ms_free( io_anchor->anchr_buf_copy );
                    advfs_bs_free_ioanchor( io_anchor, M_WAITOK );
                    io_anchor = next_io_anchor;

                    continue;
                } 

                /*
                 * advfs_issue_snap_io will issue a fake IO to decrement the
                 * final io_counter and to synchronize with io completion.
                 * If no io to snapshot children is started, the fake IO
                 * will still occur.
                 */
                sts = advfs_issue_snap_io( io_anchor,
                                *snap_maps,
                                SF_SNAP_NOFLAGS,
                                bfap,
                                parent_ftx );
                MS_SMP_ASSERT( sts == EOK );

                SNAP_STATS( wait_for_snap_io_issue_snap_io );

                /* 
                 * The writes and/or fake IOs issued in advfs_issue_snap_io
                 * will complete at some point.  We will just keep waiting.
                 */
               
            } else {
                MS_SMP_ASSERT( io_anchor->anchr_iocounter != 0 );
                /* The read has not yet completed.  Wait for it. */
                SNAP_STATS( wait_for_snap_io_read_wait );

                /* One of these better be set or your not waking up !*/
                MS_SMP_ASSERT(
                   (io_anchor->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                   (io_anchor->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));

                cv_wait( &io_anchor->anchr_cvwait,
                         &io_anchor->anchr_lock,
                         CV_SPIN,
                         CV_DFLT_FLG );
                spin_unlock( &io_anchor->anchr_lock );

            }
        } else {
            SNAP_STATS( wait_for_snap_io_write_case );

            /* We need to wait for outstanding writes to children */
            if (io_anchor->anchr_flags & IOANCHORFLG_IODONE) {
                SNAP_STATS( wait_for_snap_io_iodone );

                MS_SMP_ASSERT( io_anchor->anchr_iocounter == 0 );
                spin_unlock( &io_anchor->anchr_lock );

                if (io_anchor->anchr_error_ios != NULL) {
                    SNAP_STATS( wait_for_snap_io_write_io_errors );

                    /*
                     * Deal with marking children out of sync and updating
                     * error offset ass appropriate.
                     */
                    return_sts = advfs_snap_io_wait_error_helper( 
                                        io_anchor,
                                        bfap,
                                        error_offset,
                                        snap_maps,
                                        parent_ftx );

                    next_io_anchor = io_anchor->anchr_listfwd;
                    advfs_bs_free_ioanchor( io_anchor, M_WAITOK );
                    io_anchor = next_io_anchor;

                    continue;
                }

                /* Ah, finally, the read is completed and all children
                 * snapshots have been written to. */
                next_io_anchor = io_anchor->anchr_listfwd;

                ms_free( io_anchor->anchr_buf_copy );
                advfs_bs_free_ioanchor( io_anchor, M_WAITOK );
                io_anchor = next_io_anchor;
            } else {
                MS_SMP_ASSERT( !(io_anchor->anchr_flags & IOANCHORFLG_IODONE)); 

                SNAP_STATS( wait_for_snap_io_write_wait );

                /* The writes are not yet completed.  Wait for them... wait
                 * for them!!! */
                /* One of these better be set or your not waking up !*/
                MS_SMP_ASSERT(
                   (io_anchor->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                   (io_anchor->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));

                cv_wait( &io_anchor->anchr_cvwait,
                         &io_anchor->anchr_lock,
                         CV_SPIN,
                         CV_DFLT_FLG );
                spin_unlock( &io_anchor->anchr_lock );

            }
        }
    }
    MS_SMP_ASSERT( io_anchor == NULL );

    *io_anchor_head = io_anchor;


    /*
     * return_sts is only an error if an IO error occured on bfap.  For
     * children, the error is not returned but the children are marked out
     * of sync.
     */
    return return_sts;
}

/*
 * BEGIN_IMS
 *
 * advfs_snap_io_wait_error_helper - This routine will take an io_anchor
 * with some number of errant io's and process the errors.  The adviodesc
 * structures will be freed during processing and child snapshots marked as
 * out of sync as appropriate.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_snap_io_wait_error_helper( 
        ioanchor_t*     io_anchor,
        bfAccessT*      bfap,
        off_t*          error_offset,
        extent_blk_desc_t**     snap_maps,
        ftxHT           parent_ftx)
{

    statusT     return_sts = EOK;

    adviodesc_t* iodesc  = io_anchor->anchr_error_ios;
    adviodesc_t* next_iodesc = NULL;
    while (iodesc != NULL ) {
        if (iodesc->advio_bfaccess == bfap ) {
            extent_blk_desc_t*  cur_snap = *snap_maps;

            /* 
             * We don't need to mark this bfap as
             * out of sync... but there was an IO
             * error.  And that's bad.
             */
                           
            *error_offset = min(*error_offset, 
                    io_anchor->anchr_origbuf->b_foffset);
            return_sts = EIO;

            /* Mark each child that may have required IO
             * as out of sync */
            while (cur_snap != NULL) {
                if (cur_snap->ebd_bfap == bfap) {
                    cur_snap = cur_snap->ebd_snap_fwd;
                } else {
                    /* A child is now out of sync.  This could be
                     * finer granularity if we only marked out of
                     * sync those children that overlapped in t
                     * snap maps and not just any hcildren in the
                     * snap maps. */
                    advfs_snap_out_of_sync( cur_snap->ebd_bfap->tag,
                            cur_snap->ebd_bfap,
                            cur_snap->ebd_bfap->bfSetp,
                            parent_ftx );
                }
                cur_snap = cur_snap->ebd_snap_fwd;
            }
        } else {
            /* 
             * A child had an IO error. Try to mark it out
             * of sync.
             */
            advfs_snap_out_of_sync( iodesc->advio_bfaccess->tag,
                    iodesc->advio_bfaccess,
                    iodesc->advio_bfaccess->bfSetp,
                    parent_ftx );
        }

        next_iodesc = iodesc->advio_fwd;
        advfs_bs_free_adviodesc( iodesc );
        iodesc = next_iodesc;
        io_anchor->anchr_error_ios = iodesc;
    }

    MS_SMP_ASSERT( io_anchor->anchr_error_ios == NULL );

    return return_sts;

}


/*
 * BEGIN_IMS
 *
 * advfs_snap_protect_datafill_plist - This routine is called when pages are
 * being brought into cache in a snapshot environment.  This routine will
 * read protect any pages that may need COWing and make sure pages that
 * could not need COWing are not read protected.  The last snapshot child's
 * extents are examine to determine whether any COWing may be required.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *
 * Assumes that the extent map locks are already held for bfap and for the
 * last_child of bfap.
 *
 * Assumes that the plist pages are not read protected already.
 * 
 *
 * Algorithm:
 *
 *
 *
 * END_IMS
 */

statusT
advfs_snap_protect_datafill_plist( 
        bfAccessT*      bfap, 
        page_fsdata_t*  plist,
        off_t           offset,
        size_t          size)
{
    bfAccessT*  last_child;
    extent_blk_desc_t*  last_child_holes;
    extent_blk_desc_t*  last_child_xtnt;
    uint64_t            last_child_cnt;
    page_fsdata_t*      pfdat;
    statusT     sts;


    /* 
     * If the children are not open yet (bfaFirstChildSnap == NULL) then we
     * need to protect all the pages since we can't tell what has and has
     * not been COWed without accessing the child.
     */
    if (bfap->bfaFirstChildSnap == NULL) {
        pfdat = plist;
        while (pfdat) {
            pfdat->pfs_info.pi_pg_ro = 1;
            pfdat = pfdat->pfs_next;
        }
        return EOK;
    }

    /*
     * Find the last child 
     */
    last_child = bfap->bfaFirstChildSnap;

    while (last_child->bfaNextSiblingSnap) {
        last_child = last_child->bfaNextSiblingSnap;
    }


    sts = advfs_get_blkmap_in_range( last_child,
                                        last_child->xtnts.xtntMap,
                                        &offset,
                                        size,
                                        &last_child_holes,
                                        &last_child_cnt,
                                        RND_VM_PAGE,
                                        EXB_ONLY_HOLES,
                                        XTNT_LOCKS_HELD|XTNT_WAIT_OK);

    if (sts != EOK) {
        MS_SMP_ASSERT( FALSE );
        return sts;
    }

    if (last_child_cnt == 0) {
        return EOK;
    }

    
    last_child_xtnt = last_child_holes;
    pfdat = plist;
    
    while (last_child_xtnt) {
        if (last_child_xtnt->ebd_vd_blk != XTNT_TERM) {
            /* We only want unmapped holes, not COWed holes */
            MS_SMP_ASSERT( last_child_xtnt->ebd_vd_blk == COWED_HOLE );
            last_child_xtnt = last_child_xtnt->ebd_next_desc;
            continue;
        }

        /* Mark all the pfdats in the range covered by this hole as pi_pg_ro
         * (read only) 
         */
        while (pfdat) {
            if (pfdat->pfs_off < last_child_xtnt->ebd_offset) {
                MS_SMP_ASSERT( pfdat->pfs_info.pi_pg_ro == 0 );
                pfdat = pfdat->pfs_next;
            } else {
                MS_SMP_ASSERT((pfdat->pfs_off >= last_child_xtnt->ebd_offset) &&
                              (pfdat->pfs_off < 
                               (off_t)(last_child_xtnt->ebd_offset +
                                       last_child_xtnt->ebd_byte_cnt) ));
                pfdat->pfs_info.pi_pg_ro = 1;
                pfdat = pfdat->pfs_next;
            }

            if (pfdat == NULL) {
                break; 
            }
            
            /* See if we need to break out of the plist to go to the next
             * extent desc.
             */
            if (pfdat->pfs_off >= (off_t)(last_child_xtnt->ebd_offset +
                                          last_child_xtnt->ebd_byte_cnt) ) {
                break;
            }

        }

        last_child_xtnt = last_child_xtnt->ebd_next_desc;
    }


    advfs_free_blkmaps( &last_child_holes );

}


/*
 * BEGIN_IMS
 *
 * advfs_snap_unprotect_incache_plist - This routine will unprotect pages
 * in a plist which are backed by storage.  This is necessary to prevent
 * advfs_getpage from infinitely looping when doing COWs on pages that
 * are in cache.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *
 * Assumes that the extent map locks are already held for bfap.
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_snap_unprotect_incache_plist(
        bfAccessT*      bfap,
        page_fsdata_t*  plist,
        off_t           off,
        size_t          size,
        int32_t         *found_stg_backing,
        xtntMapFlagsT   blkmap_flags)
{

    statusT     sts = EOK;
    extent_blk_desc_t*  xtnt_descs;
    extent_blk_desc_t*  cur_desc;
    page_fsdata_t*      pfdat;


    *found_stg_backing = FALSE;

    sts = advfs_get_blkmap_in_range( bfap,
                        bfap->xtnts.xtntMap,
                        &off,
                        size,
                        &xtnt_descs,
                        NULL,
                        RND_NONE,
                        EXB_ONLY_STG,
                        (blkmap_flags) ? 
                        blkmap_flags : XTNT_WAIT_OK|XTNT_LOCKS_HELD);

    if (sts != EOK) {
        return sts;
    }



    pfdat = plist;
    cur_desc = xtnt_descs;
    while (pfdat && cur_desc) {
        MS_SMP_ASSERT( (cur_desc->ebd_vd_blk != XTNT_TERM) &&
                       (cur_desc->ebd_vd_blk != COWED_HOLE) );

        *found_stg_backing = TRUE;

        /* 
         * Check to see if the pfdat is in the current desc.  If it is
         * passed the current desc, advance the current desc.
         */
        if (pfdat->pfs_off >= cur_desc->ebd_offset) { 
            if (pfdat->pfs_off < (off_t)(cur_desc->ebd_offset + 
                                         cur_desc->ebd_byte_cnt)) {
                /* pfdat is in the range of cur_desc, unprotect it. */
                pfdat->pfs_info.pi_pg_ro = 0;
                pfdat = pfdat->pfs_next;
            } else {
                /* pfdat must be beyond cur_desc, advance cur_desc */
                cur_desc = cur_desc->ebd_next_desc;
            }
        } else {
            pfdat = pfdat->pfs_next;
        }
    }

    advfs_free_blkmaps( &xtnt_descs );


    return sts;
}



/*
 * BEGIN_IMS
 *
 * advfs_snapset_access - This routine will access every related file in the
 * snapset tree putting a sympathetic references count (fsRefCnt) on each
 * related fileset.  The accessing is done recursively and in a pre-order
 * traversal of the entire snapset tree so that parents are opened and
 * refereced before the children.  It is expected that bf_set_p is already
 * opened and a reference will not be placed on it.
 *
 *
 * Parameters:
 * bf_set_p - the already opened fileset whose related snapset must be
 * accessed.
 * snap_flags - snap flags.  If SF_IGNORE_BFS_DEL is set, then a related
 * snapset will be opened even if it is marked DELETING.
 * parent_ftx - parent transaction handle.
 * 
 *
 * Return Values:
 *
 *
 * Entry/Exit Conditions:
 *
 * Upon entry, the BfSetTblLock must me held for write.
 * 
 *
 * Algorithm:
 *
 * Find the root of the snapset tree that bf_set_p belongs to.
 * Recursively walk the tree access a parent and then all its children.
 * If bf_set_p is encountered, it will be skipped.
 *
 *
 * END_IMS
 */
statusT
advfs_snapset_access(
        bfSetT*         bf_set_p,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx)
{

    bsBfSetAttrT        bf_set_attr;
    bfSetT*             root_bitfileset;
    bfSetIdT            parent_set_id;
    bfSetIdT            top_level_id;
    bfAccessT*          tag_dir_bfap;
    statusT             sts = EOK;
    int32_t             num_children = 0;

    MS_SMP_ASSERT( rwlock_wrowned(&bf_set_p->dmnP->BfSetTblLock.lock.rw) );

    root_bitfileset = bf_set_p->dmnP->bfSetDirp;

    /*
     * Start by finding the root of the snapset tree.  We will keep reading
     * the BSR_BFS_ATTR record and walking up the tree to the root.
     */
    sts = bmtr_get_rec(bf_set_p->dirBfAp,
                        BSR_BFS_ATTR,
                        (void *) &bf_set_attr,
                        sizeof( bf_set_attr) );
    if (sts != EOK) {
        /* 
         * We couldn't read the attributes record for the fileset we want to
         * open.  This is a problem... We don't know if it is a child or
         * not, so return a hard error to fail the entire open.
         */
        return sts;
    }

    /*
     * If there is no parent and no child, then we are done.  
     */
    if (BS_BFS_EQL( bf_set_attr.bfsaParentSnapSet, nilBfSetId ) &&
        BS_BFS_EQL( bf_set_attr.bfsaFirstChildSnapSet, nilBfSetId ) ) {
        return EOK;
    }

    parent_set_id = bf_set_attr.bfsaParentSnapSet;
    if (!BS_BFS_EQL( parent_set_id, nilBfSetId)) {
        top_level_id = parent_set_id;
    } else {
        top_level_id = bf_set_p->bfSetId;
    }

    /* Now walk up the tree */
    while ( !BS_BFS_EQL( parent_set_id, nilBfSetId )) {
        top_level_id = parent_set_id;

        /* 
         * Try to access the tag file for the parent fileset so we can get
         * its BSR_BFS_ATTR record.
         */
        sts = bs_access_one( &tag_dir_bfap,
                        parent_set_id.dirTag,
                        root_bitfileset,
                        parent_ftx,
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX );
        if (sts != EOK) {
            return sts;
        }

        /*
         * Get the fileset attributes
         */
        sts = bmtr_get_rec(tag_dir_bfap,
                        BSR_BFS_ATTR,
                        (void *) &bf_set_attr,
                        sizeof( bf_set_attr ) );

        (void) bs_close_one( tag_dir_bfap,
                        MSFS_CLOSE_NONE,
                        parent_ftx );

        if (sts != EOK) {
            /* 
             * We couldn't read the attributes record for a parent fileset we need to
             * open.  This is a problem...  We don't know if we are the root
             * of the fileset tree or not.
             */
            return sts;
        }
        parent_set_id = bf_set_attr.bfsaParentSnapSet;
    }


    /*
     * top_level_id is now the bfSetId of the root fileset in the snapset
     * tree.
     */
    sts = advfs_snapset_access_recursive( bf_set_p,
                                        NULL,
                                        top_level_id,
                                        &num_children,
                                        &snap_flags,
                                        parent_ftx);


    /*
     * If an error occured, advfs_snapset_access_recursive should have
     * cleaned up after itself.
     */
    return sts;

}



/*
 * BEGIN_IMS
 *
 * advfs_snapset_access_recursive - This routine is a helper to
 * advfs_snapset_access.  This routine will take as parameters the fileset
 * being accessed and the root of the snapset tree that the fileset to be
 * accessed belongs to.  This routine will traverse the entire snapset tree
 * accessing each related fileset and skipping the fileset being accessed.
 * (it was already accessed).  This routine traverses the tree in a
 * pre-order traversal accessing first the parent, then the children.
 *
 *
 * Parameters:
 * bf_set_p - the fileset being accessed.
 * parent_set_p - The parent of cur_bf_set_id, used for recursion.
 * cur_bf_set_id - the current fileset id to access.  Should be the root of
 * the snapset tree on first call.
 *
 * num_children - recursive counter used to count children.
 * snap_flags - used to track progress as the tree is traversed.
 * parent_ftx - parent transaction handle.
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_snapset_access_recursive(
        bfSetT*         bf_set_p,
        bfSetT*         parent_set_p,
        bfSetIdT        cur_bf_set_id,
        int32_t*        kid_counter,
        snap_flags_t*   snap_flags,
        ftxHT           parent_ftx)
{

    bsBfSetAttrT        bf_set_attr;
    int32_t     cur_bf_set_open = FALSE;
    int32_t     recursed_to_children = FALSE;
    int32_t     recursed_to_siblings = FALSE;
    int32_t     clear_out_of_sync = FALSE;
    int32_t     hard_out_of_sync = FALSE;
    int32_t     snap_refs_was_incremented = FALSE;
    bfAccessT   *tag_dir_bfap = NULL;
    bfSetT*     root_bitfileset = bf_set_p->dmnP->bfSetDirp;
    bfSetT*     cur_set_p      = NULL;
    bfSetIdT    child_set_id;
    bfSetIdT    sibling_set_id;
    int32_t     num_children = 0;
    statusT     sts, hard_error = EOK;
    enum acc_open_flags acc_options;

    
    if (BS_BFS_EQL( nilBfSetId, cur_bf_set_id )) {
        /* End the recursion */
        return EOK;
    }

    *kid_counter = *kid_counter + 1;

    /*
     * First get the fileset attributes for cur_bf_set_id so we know the
     * children and siblings.
     */
    acc_options = BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX;
    sts = bs_access_one( &tag_dir_bfap,
                        cur_bf_set_id.dirTag,
                        root_bitfileset,
                        parent_ftx,
                        acc_options );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    sts = bmtr_get_rec(tag_dir_bfap,
                        BSR_BFS_ATTR,
                        (void *) &bf_set_attr,
                        sizeof( bf_set_attr) );
    if (sts != EOK) {
        /* 
         * We couldn't read the attributes record for the fileset we want to
         * open.  This is a problem... We don't know if it is a child or
         * not.
         */
        (void) bs_close_one( tag_dir_bfap, MSFS_CLOSE_NONE, parent_ftx );
        if (*snap_flags & SF_FOUND_SNAPSET) {
            sts = advfs_snap_hard_out_of_sync(bf_set_p->dmnP,
                    cur_bf_set_id,
                    parent_ftx);
            if (sts != EOK) {
                domain_panic(bf_set_p->dmnP, "Failed to mark fileset hard out-of-sync, return code = %d", sts);
                goto HANDLE_EXCEPTION;
            }
            return EOK;
        } else {
            /* This is a hard error.  We need to fail the entire open */
            goto HANDLE_EXCEPTION;
        }
    }

    (void) bs_close_one( tag_dir_bfap, MSFS_CLOSE_NONE, parent_ftx );

    if (*snap_flags & SF_HARD_OUT_OF_SYNC) {
        sts = advfs_snap_hard_out_of_sync(bf_set_p->dmnP,
                cur_bf_set_id,
                parent_ftx);
        if (sts != EOK) {
            domain_panic(bf_set_p->dmnP, "Failed to mark fileset hard out-of-sync, return code = %d", sts);
            goto HANDLE_EXCEPTION;
        }
    } 
    
    /* 
     * Access cur_bf_set_id if it isn't bf_set_p
     */
    if (!BS_BFS_EQL( cur_bf_set_id, bf_set_p->bfSetId ) ) {
        bfs_op_flags_t bfs_op_flag = BFS_OP_DEF;

        if (*snap_flags & SF_IGNORE_BFS_DEL) {
            bfs_op_flag |= BFS_OP_IGNORE_DEL;
        }

        if (*snap_flags & SF_IGNORE_BFS_OUT_OF_SYNC) {
            bfs_op_flag |= BFS_OP_IGNORE_OUT_OF_SYNC;
        }

        sts = bfs_access( &cur_set_p,
                cur_bf_set_id,
                &bfs_op_flag,
                parent_ftx );
        if (sts != EOK)  {
            if (*snap_flags & SF_FOUND_SNAPSET) {
                /* We already opened the target fileset and all of it's
                 * dependencies.  The current fileset is "expendable".
                 * We will mark it and all of it's related filesets
                 * (children and siblings) hard out-of-sync.  They will
                 * not be available to the user via mount after this.
                 */
                sts = advfs_snap_hard_out_of_sync_recursive(bf_set_p->dmnP, 
                        cur_bf_set_id,
                        parent_ftx);
                if (sts) {
                    goto HANDLE_EXCEPTION;
                }
                return EOK;
            } else {
                /* Fail the entire open */
                if (*snap_flags & SF_HARD_OUT_OF_SYNC || 
                        (sts == E_OUT_OF_SYNC_SNAPSHOT)) {
                    /* This is a hard error.  We will first propogate the
                     * hard out-of-sync to this fileset's siblings and 
                     * children.  Then we will retun the error to the caller.
                     */
                    sts = advfs_snap_hard_out_of_sync_recursive(bf_set_p->dmnP, 
                            cur_bf_set_id,
                            parent_ftx);
                }
                goto HANDLE_EXCEPTION;
            }
        } 

        cur_bf_set_open = TRUE;
    } else {
        cur_set_p = bf_set_p;
        *snap_flags |= SF_FOUND_SNAPSET;
    }

    if (*snap_flags & SF_OUT_OF_SYNC) {
        /* An out of sync parent of this fileset is out of sync.  
         * Make sure this one is too. 
         */
        sts = advfs_snap_out_of_sync( NilBfTag,
                NULL,
                cur_set_p,
                parent_ftx );

    }

    if (bf_set_attr.flags & BFS_OD_OUT_OF_SYNC && 
            !(*snap_flags & SF_OUT_OF_SYNC)) {
        *snap_flags |= SF_OUT_OF_SYNC;
        clear_out_of_sync = TRUE;
    }

    if (cur_set_p->bfSetFlags & BFS_OD_HARD_OUT_OF_SYNC && 
            !(*snap_flags & SF_HARD_OUT_OF_SYNC)) {
        /* This fileset is hard out-of-sync.  Make sure
         * we propogate this to it's children and siblings. 
         */
        *snap_flags |= SF_HARD_OUT_OF_SYNC;
        hard_out_of_sync = TRUE;
    }

    if (cur_set_p != bf_set_p) {
        /* 
         * This isn't the fileset we want to open, so note that the fsRefCnt
         * is from a sympathetic access.
         */
        MS_SMP_ASSERT( !BS_BFS_EQL( cur_set_p->bfSetId,
                                    bf_set_p->bfSetId) );
        cur_set_p->bfsSnapRefs++;
        snap_refs_was_incremented = TRUE;
    }

    (void) advfs_link_snapsets( parent_set_p, cur_set_p );

    if (cur_set_p->bfSetFlags & BFS_OD_OUT_OF_SYNC) {
        MS_SMP_ASSERT( cur_set_p->bfsParentSnapSet != NULL );
        (void) advfs_snap_print_out_of_sync( NilBfTag, cur_set_p );
    }

    /* Recurse to first child if it exists. */
    if (!BS_BFS_EQL( bf_set_attr.bfsaFirstChildSnapSet,
                        nilBfSetId ) ) {
        sts = advfs_snapset_access_recursive( bf_set_p,
                                cur_set_p,
                                bf_set_attr.bfsaFirstChildSnapSet,
                                &num_children,
                                snap_flags,
                                parent_ftx );

        if (sts != EOK) {
            goto HANDLE_EXCEPTION;
        }
        recursed_to_children = TRUE;

        if (cur_bf_set_open == TRUE) {
            cur_set_p->bfsNumSnapChildren = num_children;
        }

    }

    /*
     * We want to clear the SF_OUT_OF_SYNC flag before going to our siblings
     * if we set the flag.  If we don't clear this flag, then we may end up
     * marking our siblings out of sync which isn't valid since they have no
     * dependence on this file set.
     */
    if (clear_out_of_sync) {
        *snap_flags &= ~SF_OUT_OF_SYNC;
    }


    /*
     * Recurse to siblings.
     */
    if (!BS_BFS_EQL( bf_set_attr.bfsaNextSiblingSnap,
                        nilBfSetId ) ) {
        sts = advfs_snapset_access_recursive( bf_set_p,
                                parent_set_p,
                                bf_set_attr.bfsaNextSiblingSnap,
                                kid_counter,
                                snap_flags,
                                parent_ftx );
        if (sts != EOK) {
            goto HANDLE_EXCEPTION;
        }
        recursed_to_siblings = TRUE;
    }

    if (hard_out_of_sync) {
        *snap_flags &= ~SF_HARD_OUT_OF_SYNC;
    }

    return EOK;


HANDLE_EXCEPTION:

    MS_SMP_ASSERT( sts != EOK );
    /*
     * Something went wrong.  
     */
    if (cur_bf_set_open) {
        /* This is a hard failure...Undo any fileset accesses that we have done
         * in this instance of the recursion.
         */
        if (recursed_to_siblings) {
            /* For each sibling in the list we need to call 
             * advfs_snapset_close_recursive().  This will close the sibling
             * and all of it's children.
             */
            bfSetT *sibling_ptr = NULL;
            MS_SMP_ASSERT(cur_set_p != NULL);

            sibling_ptr = cur_set_p->bfsNextSiblingSnapSet;
            while (sibling_ptr != NULL) {
                /* close the siblings and any of their children */
                advfs_snapset_close_recursive(cur_set_p, sibling_ptr, parent_ftx);
                sibling_ptr = sibling_ptr->bfsNextSiblingSnapSet;
            }
        }

        if (recursed_to_children) {
            /* advfs_snapset_close_recursive() will close the children of
             * cur_set_p.  It will not close cur_set_p itself.  That will
             * be taken care below in a bit...
             */
            MS_SMP_ASSERT(cur_set_p != NULL);
            advfs_snapset_close_recursive(cur_set_p, cur_set_p, parent_ftx);
        }
        /*
         * If we are returning a hard error, make sure we don't leave the
         * bfsSnapRefs elevated.
         */
        if (snap_refs_was_incremented) {
            cur_set_p->bfsSnapRefs--;
        }

        bfs_close( cur_set_p, parent_ftx );
    }

    if (clear_out_of_sync) {
        *snap_flags &= ~SF_OUT_OF_SYNC;
    } 

    if (hard_out_of_sync) {
        *snap_flags &= ~SF_HARD_OUT_OF_SYNC;
    }

    return sts;
}




/*
 * BEGIN_IMS
 *
 * advfs_link_snapsets - This routine will link a child fileset to the end
 * of its parent's child snapset list.  The linking is done only in memory
 * since it is already linked on disk.
 *
 *
 * Parameters:
 * parent_set_p - The parent snapset (could be NULL)
 * child_set_p  - The child to link to the parent.
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
static
statusT
advfs_link_snapsets(
        bfSetT*         parent_set_p,
        bfSetT*         child_set_p)
{


    if (parent_set_p == NULL || child_set_p->bfsParentSnapSet != NULL) {
        /* Nothing to do... */
        return EOK;
    }

    child_set_p->bfsParentSnapSet = parent_set_p;

    if (parent_set_p->bfsFirstChildSnapSet == NULL) {
        parent_set_p->bfsFirstChildSnapSet = child_set_p;
    } else {
        bfSetT* prev_set = parent_set_p->bfsFirstChildSnapSet;
        bfSetT* next_set = prev_set->bfsNextSiblingSnapSet;

        /* 
         * Walk to the end of the list
         */
        while (next_set != NULL) {
            prev_set = next_set;
            next_set = next_set->bfsNextSiblingSnapSet;
        }

        prev_set->bfsNextSiblingSnapSet = child_set_p;
    }

    return EOK;
}


/*
 * BEGIN_IMS
 *
 * advfs_create_snapset - This is the top-level routine to create a snapset.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * E_ACCESS_DENIED
 * E_NOT_SUPPORTED
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_create_snapset(
        char *parent_dmn_name,  /* Name of parent fileset's domain */
        char *parent_set_name,  /* Name of parent fileset */
        char *snap_dmn_name,    /* Name of snap's domain */
        char *snap_fset_name,   /* Name of snap's fileset */
        bfSetIdT *snap_set_id,  /* bf set id of the new snap set */
        snap_flags_t snap_flags,/* snap related flags */
        ftxIdT cfs_xid)         /* CFS transaction ID */
{
    ftxHT ftxH;
    domainT *dmn_p;
    bfDomainIdT dmn_id;
    statusT sts;
    bfs_op_flags_t bfs_op_flags;

    bfSetT *snap_set_p   = NULL;
    bfSetT *parent_set_p = NULL;
    int bfset_tbl_locked = FALSE;
    int ftx_started = FALSE;
    int first_snapset_in_fileset = FALSE;
    int clear_snap_flag = FALSE;
    int close_snapset = FALSE;

    /* Check if the parent fileset can be snapped.  If so, then it will be open
     * when the routine returns.
     */
    if (advfs_check_snap_perms(parent_dmn_name, parent_set_name,
                &parent_set_p))
    {
        return E_ACCESS_DENIED;
    }

    /* The parent fileset is now open */
    dmn_p = parent_set_p->dmnP;
    MS_SMP_ASSERT(dmn_p != NULL);
    dmn_id = dmn_p->domainId;

    if (parent_set_p->bfsParentSnapSet != NULL) {
        /* For now, we only allow single level snap trees */
        sts = E_CANT_SNAP_A_SNAPSHOT;
        goto _error;
    }
#ifdef CLU_LATER
    if (parent_set_p->fsnp && 
            (parent_set_p->fsnp->vfsp->vfs_flag & VFS_CFSONTOP)) {
        CLU_CFS_SNAP_NOTIFY(SNAP_CREATE);
    }
#endif

    /* Determine if this snapset we are creating is the first in the fileset */
    if (parent_set_p->bfsFirstChildSnapSet == NULL) {
        first_snapset_in_fileset = TRUE;
    }

    ADVRWL_BFSETTBL_WRITE(dmn_p);
    bfset_tbl_locked = TRUE;

    /* Another thread might have snuck in and started to delete the parent 
     * fileset.  If this is the case, then we bail out.
     */
    if (parent_set_p->state == BFS_DELETING) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    /* The following flag will prevent another thread from accessing the 
     * parent fileset when we drop the bfSetTbl lock for advfs_drain_writes().
     */
    ADVSMP_BFSET_LOCK(&parent_set_p->bfSetMutex);
    parent_set_p->bfSetFlags |= BFS_IM_SNAP_IN_PROGRESS;
    ADVSMP_BFSET_UNLOCK(&parent_set_p->bfSetMutex);
    clear_snap_flag = TRUE;

    /* Drop the bfset table lock and wait for all outstanding writes to
     * complete. 
     */
    ADVRWL_BFSETTBL_UNLOCK(dmn_p);
    bfset_tbl_locked = FALSE;

    sts = advfs_snap_drain_writes(parent_set_p);
    if (sts != EOK) {
        goto _error;
    }

    /* We flush updated stats now before we start the exclusive 
     * transaction
     */
    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_61);
    
    advfs_bs_dmn_flush_meta( dmn_p, FLUSH_NOFLAGS );
    
    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_62);
    
    advfs_bs_bfs_flush( parent_set_p, FALSE, FLUSH_UPDATE_STATS );

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_63);

    /* We start the exclusive transction for synchronization issues.  This
     * will block any storage additions.
     */
    sts = FTX_START_EXC_XID(FTA_BFS_SNAP, &ftxH, dmn_p, cfs_xid);
    if (sts != EOK) {
        goto _error;
    }
    ftx_started = TRUE;

    /* Acquire the bfset table lock so we can create the snapset */
    ADVRWL_BFSETTBL_WRITE(dmn_p);
    bfset_tbl_locked = TRUE;
    if (sts = bfs_create(dmn_p, nilServiceClass, snap_fset_name, 0, 
                ftxH, snap_set_id)) 
    {
        goto _error;
    }

    /* Assumption: This access can be undo'ed by failing the transaction */
    bfs_op_flags = BFS_OP_DEF;
    if (sts = bfs_access(&snap_set_p, *snap_set_id, &bfs_op_flags, ftxH)) {
        goto _error;
    }

    close_snapset = TRUE;
    ADVSMP_BFSET_LOCK(&snap_set_p->bfSetMutex);
    snap_set_p->bfSetFlags |= BFS_IM_SNAP_IN_PROGRESS;
    snap_set_p->bfSetFlags &= ~BFS_OD_ROOT_SNAPSHOT;
    ADVSMP_BFSET_UNLOCK(&snap_set_p->bfSetMutex);

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_64);
    
    advfs_bs_dmn_flush(dmn_p, FLUSH_NO_TBL_LOCK);

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_65);

    if (sts = advfs_copy_tagdir(parent_set_p, snap_set_p, ftxH)) {
        goto _error;
    }

    /* This routine will link the newly created snapset in the snapset tree
     * both in-memory (bfSetT) and on-disk (bfBfsAttrT).
     */
    if (sts = advfs_link_snapsets_full(parent_set_p, snap_set_p, snap_flags, 
                ftxH)) 
    {
        goto _error;
    }

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_66);
    
    if (sts = advfs_snap_protect_cache(parent_set_p)) {
        goto _error;
    }

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_67);
    
    /* No need to close the snapset separately.  It will be closed with the
     * whole snapset tree.
     */
    close_snapset = FALSE;

    /* Bump the ref counts of the snapset to match the parent fileset */
    while (snap_set_p->fsRefCnt < parent_set_p->fsRefCnt) {
        bfs_op_flags = BFS_OP_DEF;
        sts = bfs_access(&snap_set_p, *snap_set_id, &bfs_op_flags, ftxH);
        MS_SMP_ASSERT(sts == EOK);
        if (sts != EOK) {
            goto _error;
        }
    }
    /* Since the snapset can't be mounted yet all of it's references
     * are sympathetic to the parent.
     */
    snap_set_p->bfsSnapRefs = parent_set_p->fsRefCnt;

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_68);
    
    ftx_done_n(ftxH, FTA_BFS_SNAP);
    
    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_69);

    ftx_started = FALSE;

    ADVSMP_BFSET_LOCK(&snap_set_p->bfSetMutex);
    snap_set_p->bfSetFlags &= ~BFS_IM_SNAP_IN_PROGRESS;
    ADVSMP_BFSET_UNLOCK(&snap_set_p->bfSetMutex);

    ADVSMP_BFSET_LOCK(&parent_set_p->bfSetMutex);
    parent_set_p->bfSetFlags &= ~BFS_IM_SNAP_IN_PROGRESS;
    ADVSMP_BFSET_UNLOCK(&parent_set_p->bfSetMutex);

    cv_broadcast(&parent_set_p->bfsSnapCv, NULL, CV_NULL_LOCK);
    cv_broadcast(&snap_set_p->bfsSnapCv, NULL, CV_NULL_LOCK);

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_70);
    
    bs_bfs_close(parent_set_p, FtxNilFtxH, BFS_CL_COND_UNLOCK);

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_71);
    
    ADVRWL_BFSETTBL_UNLOCK(dmn_p);
    
    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_72);
    
    bs_bfdmn_deactivate(dmn_id, 0);

    ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_73);

    return EOK;

_error:
    if (clear_snap_flag) {
        ADVSMP_BFSET_LOCK(&parent_set_p->bfSetMutex);
        parent_set_p->bfSetFlags &= ~BFS_IM_SNAP_IN_PROGRESS;
        ADVSMP_BFSET_UNLOCK(&parent_set_p->bfSetMutex);

        cv_broadcast(&parent_set_p->bfsSnapCv, NULL, CV_NULL_LOCK);
    }

    if (bfset_tbl_locked) {
        /* 
         * The ftx_fail may acquire the bSetTbl lock, so drop it before
         * failing
         */
        ADVRWL_BFSETTBL_UNLOCK(dmn_p);
    }


    if (parent_set_p->fsnp && 
            (parent_set_p->fsnp->vfsp->vfs_flag & VFS_CFSONTOP) && 
            first_snapset_in_fileset) {
#ifdef CLU_LATER
        CLU_CFS_SNAP_NOTIFY(SNAP_DELETE);
#endif
    }

    if (ftx_started) {
        /* This will back out (possibly) the snapset creation, the on-disk
         * linking of the snapset into the snapset tree, the copy of the 
         * tagdir from parent to snapset, and the 
         * bfs_access done on the snapset to match the parent fsRefCnt.
         */
        ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_450);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(dmn_p, AdvfsCrash_451);
    }


    ADVRWL_BFSETTBL_WRITE(dmn_p);

    if (close_snapset) {
        bfs_close(snap_set_p, FtxNilFtxH);
    }
    ADVRWL_BFSETTBL_UNLOCK(dmn_p);


    bs_bfs_close(parent_set_p, FtxNilFtxH, BFS_OP_DEF);


    bs_bfdmn_deactivate(dmn_id, 0);

        
    return sts;
}

/*
 * BEGIN_IMS
 *
 * advfs_check_snap_perms - This routine will validate that the parent
 * filset can be snapped.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * E_ACCESS_DENIED
 * E_NOT_SUPPORTED
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

static
statusT
advfs_check_snap_perms(
        char   *parent_dmn_name,  /* Name of parent fileset's domain */
        char   *parent_fset_name, /* Name of parent fileset */
        bfSetT **parent_bf_set_p) /* Returns bf set struct of parent fileset */
{
    bfSetIdT parent_set_id;
    domainT *dmn_p;
    bsBfSetAttrT bfs_attr;

    bfDmnParamsT *dmn_params_p = NULL;
    bfSetParamsT *set_params_p = NULL;
    statusT sts = EOK;
    int dmn_active = 0;
    int fset_open = 0;

    if (sts = bs_bfset_activate(parent_dmn_name, parent_fset_name, 0, 
                &parent_set_id))
    {
        return sts;
    }
    dmn_active = 1;

    if (sts = bfs_open(parent_bf_set_p, parent_set_id, BFS_OP_DEF, FtxNilFtxH )) 
    {
        goto _error;
    }
    fset_open = 1;
    dmn_p = (*parent_bf_set_p)->dmnP;
    
    dmn_params_p = (bfDmnParamsT *) ms_malloc_no_bzero(sizeof(bfDmnParamsT));

    if (sts = bs_get_dmn_params(dmn_p, dmn_params_p, 0)) {
        goto _error;
    }

    if (!bs_accessible(BS_ACC_WRITE,
                dmn_params_p->mode, dmn_params_p->uid, dmn_params_p->gid)) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    ms_free(dmn_params_p);

    set_params_p = (bfSetParamsT *) ms_malloc_no_bzero(sizeof(bfSetParamsT));

    if (sts = bs_get_bfset_params(*parent_bf_set_p, set_params_p, 0)) {
        goto _error;
    }

    if (!bs_accessible(BS_ACC_READ,
               set_params_p->mode, set_params_p->uid, set_params_p->gid)) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    if (set_params_p->bfspSnapLevel == ADVFS_MAX_SNAP_DEPTH) {
        sts = ENOT_SUPPORTED;
        goto _error;
    }
    ms_free(set_params_p);

    return EOK;

_error:
    if (dmn_params_p) {
        ms_free(dmn_params_p);
    }

    if (set_params_p) {
        ms_free(set_params_p);
    }

    if (fset_open) {
        bs_bfs_close(*parent_bf_set_p, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmn_active) {
        bs_bfdmn_deactivate(parent_set_id.domainId, 0);
    }

    return sts;
}

/*
 * BEGIN_IMS
 *
 * advfs_copy_tagdir - This routine will copy the parent tagdir to the child.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

static
statusT
advfs_copy_tagdir(
        bfSetT *parent_bf_set_p,        /* bfSetT pointer of parent fileset */
        bfSetT *snap_bf_set_p,          /* bfSetT pointer of snap set */
        ftxHT   parent_ftx)             /* Transaction of parent_ftx */
{
    statusT        sts;
    bs_meta_page_t pg, next_pg;
    bfPageRefHT    parent_pg_ref, snap_pg_ref;
    bsTDirPgT     *parent_pg_p, *snap_pg_p;
    bsTMapT       *tag_map_ptr;
    int            tag_entry;

    bfAccessT *parent_tag_bfap = parent_bf_set_p->dirBfAp;
    bfAccessT *snap_tag_bfap   = snap_bf_set_p->dirBfAp;
    
    if (parent_tag_bfap->bfaNextFob > snap_tag_bfap->bfaNextFob) {
        sts = rbf_add_stg(snap_tag_bfap, 
                snap_tag_bfap->bfaNextFob,
                (parent_tag_bfap->bfaNextFob - snap_tag_bfap->bfaNextFob),
                parent_ftx,
                0);
        if (sts != EOK) {
            return sts;
        }
    }

    /*
     * This loop will probably consume the vast majority of the time it
     * takes to create a snapshot.  This may be optimized in the future by
     * doing a raw read into a buffer, patching the buffer and doing a
     * raw write back out again without doing pin pages. */
    next_pg = parent_tag_bfap->bfaNextFob / parent_tag_bfap->bfPageSz;
    for (pg = 0; pg < next_pg; pg++) {
        sts = bs_refpg(&parent_pg_ref,
                (void *) &parent_pg_p,
                parent_tag_bfap,
                pg,
                parent_ftx,
                MF_VERIFY_PAGE);
        if (sts != EOK) {
            return sts;
        }

        sts = bs_pinpg(&snap_pg_ref,
                (void *) &snap_pg_p,
                snap_tag_bfap,
                pg,
                parent_ftx,
                MF_NO_VERIFY);
        if (sts != EOK) {
            bs_derefpg(parent_pg_ref, BS_CACHE_IT);
            return sts;
        }

        bcopy(parent_pg_p, snap_pg_p, parent_tag_bfap->bfPageSz * ADVFS_FOB_SZ);

        if (pg == 0) {
            /* 
             * For page 0 of the tag directory, the tagmap at index 0 is used 
             * for the free list.  Skip this entry in the loop below. 
             */
            tag_entry = 1;
        } else {
            tag_entry = 0;
        }

        for (; tag_entry < BS_TD_TAGS_PG; tag_entry++) {
            tag_map_ptr = &snap_pg_p->tMapA[tag_entry];
            tag_map_ptr->tmFlags |= BS_TD_VIRGIN_SNAP;
            tag_map_ptr->tmFlags &= ~BS_TD_ROOT_SNAPSHOT;
        }
        bs_unpinpg(snap_pg_ref, logNilRecord, BS_DIRTY);
        bs_derefpg(parent_pg_ref, BS_CACHE_IT);
    }
    sts = fcache_vn_flush(&(snap_tag_bfap->bfVnode), 0, 0, NULL, 
            FVF_WRITE | FVF_SYNC);
    if (sts != EOK) {
        return sts;
    }

    sts = tagdir_get_info(snap_bf_set_p, &snap_bf_set_p->tagUnInPg, 
            &snap_bf_set_p->tagUnMpPg, &snap_bf_set_p->tagFrLst, 
            &snap_bf_set_p->bfCnt);
    if (sts != EOK) {
        return sts;
    }
    
    return EOK;
}

/*
 * BEGIN_IMS
 *
 * advfs_link_snapsets_full - This routine will link the child bf set into the 
 *                            snapshot tree.  It also copies bfset records
 *                            from the parent to the child.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

static
statusT
advfs_link_snapsets_full(
        bfSetT *parent_bf_set_p,    /* bfSetT pointer of parent fileset */
        bfSetT *child_bf_set_p,     /* bfSetT pointer of child fileset */
        snap_flags_t snap_flags,    /* Flags to indicate read or write snapshot*/
        ftxHT parent_ftx)           /* Transaction to use for updates */
{
    bsBfSetAttrT *bfs_attr_p;
    bsQuotaAttrT *quota_attr_p;
    statusT sts;
    bfSetT *prev_child_p = NULL, *next_child_p = NULL;
    int parent_linked = FALSE;
    undoSnapLinkRecT *undo_rec_p;
    ftxHT cur_ftx;
    int ftx_started = FALSE;

    MS_SMP_ASSERT(rwlock_wrowned(&parent_bf_set_p->dmnP->BfSetTblLock.lock.rw));

    bfs_attr_p = (bsBfSetAttrT *)ms_malloc(sizeof(bsBfSetAttrT));
    quota_attr_p = (bsQuotaAttrT *)ms_malloc(sizeof(bsQuotaAttrT));
    undo_rec_p = (undoSnapLinkRecT *)ms_malloc(sizeof(undoSnapLinkRecT));

    sts = FTX_START_N(FTA_BFS_LINK_SNAP, &cur_ftx, parent_ftx, 
            parent_bf_set_p->dmnP);
    if (sts != EOK) {
        domain_panic(parent_bf_set_p->dmnP, 
                "failed to start transaction in advfs_link_snapsets_full");
        goto _error;
    }

    ftx_started = TRUE;

    /* link parent to child */
    sts = bmtr_get_rec_n_lk(parent_bf_set_p->dirBfAp,
            BSR_BFS_ATTR,
            (void *) bfs_attr_p,
            (int16_t)sizeof(bsBfSetAttrT),
            BMTR_LOCK);
    if (sts != EOK) {
        goto _error;
    }

    if (!BS_BFS_EQL(bfs_attr_p->bfsaFirstChildSnapSet, nilBfSetId)) {
        /* This is not the first child.  We need to link in using the
         * NextSiblingSnapSet pointer. */
        ADVRWL_MCELL_LIST_UNLOCK(&(parent_bf_set_p->dirBfAp->mcellList_lk));

        prev_child_p = parent_bf_set_p->bfsFirstChildSnapSet;
        next_child_p = prev_child_p->bfsNextSiblingSnapSet;

        MS_SMP_ASSERT(prev_child_p != NULL);

        while (next_child_p != NULL) {
            prev_child_p = next_child_p;
            next_child_p = next_child_p->bfsNextSiblingSnapSet;
        }

        sts = bmtr_get_rec_n_lk(prev_child_p->dirBfAp,
                BSR_BFS_ATTR,
                (void *) bfs_attr_p,
                (uint16_t)sizeof(bsBfSetAttrT),
                BMTR_LOCK);
        if (sts != EOK) {
            goto _error;
        }
        /* Set up the undo record */
        undo_rec_p->usp_bf_set_id = prev_child_p->bfSetId;
        undo_rec_p->usp_snap_set_id = child_bf_set_p->bfSetId;

        bfs_attr_p->bfsaNextSiblingSnap = child_bf_set_p->bfSetId;
        sts = bmtr_update_rec_n_unlk(prev_child_p->dirBfAp,
                BSR_BFS_ATTR,
                (void *) bfs_attr_p,
                (uint16_t)sizeof(bsBfSetAttrT),
                cur_ftx,
                BMTR_UNLOCK);
        if (sts != EOK) {
            goto _error;
        }
        prev_child_p->bfsNextSiblingSnapSet = child_bf_set_p;
    } else {
        /* Since this is the first child we just set the FirstChildSnapSet
         * pointer */
        
        /* Set up the undo record */
        undo_rec_p->usp_bf_set_id = parent_bf_set_p->bfSetId;
        undo_rec_p->usp_snap_set_id = child_bf_set_p->bfSetId;
        
        bfs_attr_p->bfsaFirstChildSnapSet = child_bf_set_p->bfSetId;
        sts = bmtr_update_rec_n_unlk(parent_bf_set_p->dirBfAp,
                BSR_BFS_ATTR,
                (void *) bfs_attr_p,
                (uint16_t)sizeof(bsBfSetAttrT),
                cur_ftx,
                BMTR_UNLOCK);
        if (sts != EOK) {
            goto _error;
        }
        parent_bf_set_p->bfsFirstChildSnapSet = child_bf_set_p;
    }
    
    ftx_done_u(cur_ftx, FTA_BFS_LINK_SNAP, sizeof(undoSnapLinkRecT), undo_rec_p);
    ftx_started = FALSE;

    parent_bf_set_p->bfsNumSnapChildren++;
    parent_linked = TRUE;

    /* link child to parent */
    sts = FTX_START_N(FTA_NULL, &cur_ftx, parent_ftx, parent_bf_set_p->dmnP);
    if (sts != EOK) {
        domain_panic(parent_bf_set_p->dmnP, "advfs_link_snapset_full failed to"
                " start a transaction");
        goto _error;
    }
    ftx_started = TRUE;

    sts = bmtr_get_rec_n_lk(child_bf_set_p->dirBfAp,
            BSR_BFS_ATTR,
            (void *) bfs_attr_p,
            (uint16_t)sizeof(bsBfSetAttrT),
            BMTR_LOCK);
    if (sts != EOK) {
        goto _error;
    }
    bfs_attr_p->bfsaParentSnapSet = parent_bf_set_p->bfSetId;
    bfs_attr_p->bfsaSnapLevel = parent_bf_set_p->bfsSnapLevel + 1;
    bfs_attr_p->state |= BFS_ODS_VALID;

    if (snap_flags & SF_SNAP_READ) {
        bfs_attr_p->flags |= BFS_OD_READ_ONLY;
    } 
#ifdef ADVFS_SMP_ASSERT
    else {
        MS_SMP_ASSERT(snap_flags & SF_SNAP_WRITE);

    }
#endif
    
    sts = bmtr_update_rec_n_unlk(child_bf_set_p->dirBfAp,
            BSR_BFS_ATTR,
            (void *) bfs_attr_p,
            (uint16_t)sizeof(bsBfSetAttrT),
            cur_ftx,
            BMTR_UNLOCK);
    if (sts != EOK) {
        goto _error;
    }
    /* No need for an undo, because the snapset will be deleted w/ any failure */
    ftx_done_n(cur_ftx, FTA_NULL);
    ftx_started = FALSE;

    child_bf_set_p->bfsParentSnapSet = parent_bf_set_p;
    child_bf_set_p->bfsSnapLevel     = parent_bf_set_p->bfsSnapLevel + 1;
    if (snap_flags & SF_SNAP_READ) {
        ADVSMP_BFSET_LOCK(&child_bf_set_p->bfSetMutex);
        child_bf_set_p->bfSetFlags |= BFS_OD_READ_ONLY;
        ADVSMP_BFSET_UNLOCK(&child_bf_set_p->bfSetMutex);
    } 

    sts = FTX_START_N(FTA_NULL, &cur_ftx, parent_ftx, parent_bf_set_p->dmnP);
    if (sts != EOK) {
        domain_panic(parent_bf_set_p->dmnP, "advfs_link_snapset_full failed to"
                " start a transaction");
        goto _error;
    }
    ftx_started = TRUE;

    /* Copy the BSR_BFS_QUOTA_ATTR from the parent to the child */
    sts = bmtr_get_rec_n_lk(parent_bf_set_p->dirBfAp,
            BSR_BFS_QUOTA_ATTR,
            (void *) quota_attr_p,
            sizeof(bsQuotaAttrT),
            BMTR_NO_LOCK);
    if (sts != EOK) {
        goto _error;
    }

    sts = bmtr_update_rec_n_unlk(child_bf_set_p->dirBfAp,
            BSR_BFS_QUOTA_ATTR,
            (void *) quota_attr_p,
            sizeof(bsQuotaAttrT),
            cur_ftx,
            BMTR_NO_LOCK);
    if (sts != EOK) {
        goto _error;
    }
    /* No need for an undo, because the snapset will be deleted w/ any failure */
    ftx_done_n(cur_ftx, FTA_NULL);
    ftx_started = FALSE;

    ms_free(bfs_attr_p);
    ms_free(quota_attr_p);
    ms_free(undo_rec_p);
    return EOK;

_error:
    ms_free(bfs_attr_p);
    ms_free(quota_attr_p);
    ms_free(undo_rec_p);

    if (ftx_started) {
        ftx_fail(cur_ftx);
    }

    /* Back out any in-memory linking on the parent bfset */
    if (parent_linked) {
        if (prev_child_p) {
            prev_child_p->bfsNextSiblingSnapSet = NULL;
        } else {
            parent_bf_set_p->bfsFirstChildSnapSet = NULL;
        }
        parent_bf_set_p->bfsNumSnapChildren--;
    }

    return sts;
}

/*
 * BEGIN_IMS
 *
 * advfs_bs_bfs_snap_link_undo_opx - This routine is the undo for
 * advfs_link_snapsets_full.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 * Algorithm:
 *
 * END_IMS
 */

void
advfs_bs_bfs_snap_link_undo_opx(
                    ftxHT       ftxH,
                    int         opRecSz,
                    void*       opRecp
                    )
{
    statusT sts;
    bfAccessT *dir_bfap;
    bfSetT *bf_set_p;
    undoSnapLinkRecT undo_rec;
    bsBfSetAttrT *bfs_attr_p;
    int opened_tagdir = FALSE;

    MS_SMP_ASSERT(sizeof(undoSnapLinkRecT) == opRecSz);

    bcopy(opRecp, &undo_rec, opRecSz);

    bfs_attr_p = (bsBfSetAttrT *)ms_malloc(sizeof(bsBfSetAttrT));

    bf_set_p = bfs_lookup(undo_rec.usp_bf_set_id);

    if (bf_set_p) {
        dir_bfap = bf_set_p->dirBfAp;
    } else {
        /* access this fileset's tag directory */
        sts = bs_access_one(&dir_bfap,
                undo_rec.usp_bf_set_id.dirTag,
                ftxH.dmnP->bfSetDirp,
                ftxH,
                BF_OP_IGNORE_BFS_DELETING|BF_OP_IGNORE_DEL|BF_OP_INTERNAL|
                BF_OP_OVERRIDE_SMAX);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, "bs_access failed in advfs_bs_bfs_snap_link_undo_opx");
            ms_free(bfs_attr_p);
            return;
        }
        opened_tagdir = TRUE;
    }

    sts = bmtr_get_rec_n_lk(dir_bfap,
            BSR_BFS_ATTR,
            (void *)bfs_attr_p,
            sizeof(bsBfSetAttrT),
            BMTR_LOCK);
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bmtr_get_rec_n_lk failed in advfs_bs_bfs_snap_link_undo_opx");
        ms_free(bfs_attr_p);
        return;
    }

    /* Since the snapset is always linked in at the end of the list. We just
     * need to set either the first child snapset or next sibling snap set
     * to nilBfSetId.
     */
    if (BS_BFS_EQL(bfs_attr_p->bfsaFirstChildSnapSet, undo_rec.usp_snap_set_id)) {
        /* The snapset was the first child of the parent */
        bfs_attr_p->bfsaFirstChildSnapSet = nilBfSetId;
    } else {
        bfs_attr_p->bfsaNextSiblingSnap = nilBfSetId;
    }

    sts = bmtr_update_rec_n_unlk(dir_bfap,
            BSR_BFS_ATTR,
            (void *)bfs_attr_p,
            sizeof(bsBfSetAttrT),
            ftxH,
            BMTR_UNLOCK);
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bmtr_update_rec failed in advfs_bs_bfs_snap_link_undo_opx");
        ms_free(bfs_attr_p);
        return;
    }
    
    if (bf_set_p) {
        if (bf_set_p->bfsFirstChildSnapSet &&
                BS_BFS_EQL(bf_set_p->bfsFirstChildSnapSet->bfSetId, 
                    undo_rec.usp_snap_set_id)) {
            /* This fileset is the parent of the snapset we linked in. */
            bf_set_p->bfsFirstChildSnapSet = NULL;
            bf_set_p->bfsNumSnapChildren--;
        } else if (bf_set_p->bfsNextSiblingSnapSet){
            /* This fileset is the sibling of the snapset we linked in. */
            bf_set_p->bfsNextSiblingSnapSet = NULL;
            bf_set_p->bfsParentSnapSet->bfsNumSnapChildren--;
        }
    }

    if (opened_tagdir == TRUE) {
        (void) bs_close_one(dir_bfap, MSFS_CLOSE_NONE, ftxH);
    }

    ms_free(bfs_attr_p);
}

/*
 * BEGIN_IMS
 *
 * advfs_snap_protect_cache - This routine sets all of open files in the parent
 *                            fileset to read_only.  This will enable the 
 *                            copy-on-write processing.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

static
statusT
advfs_snap_protect_cache(
        bfSetT *parent_bf_set_p)    /* bfSetT pointer of parent fileset */
{
    bfAccessT *bfap_list_marker;
    bfAccessT *curr_bfap;
    int size;
    struct advfs_pvt_param pvt_param;
    statusT sts = EOK;

    bfap_list_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
    bfap_list_marker->accMagic = ACCMAGIC_MARKER;
    bfap_list_marker->bfSetp = parent_bf_set_p;

    pvt_param = Nil_advfs_pvt_param;
    pvt_param.app_flags |= APP_MARK_READ_ONLY;

    ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
    ADD_ACC_SETLIST(bfap_list_marker, FALSE, FALSE);
    while (bfap_list_marker->setFwd != 
            (bfAccessT *)(&parent_bf_set_p->accessFwd)) 
    {
        curr_bfap = bfap_list_marker->setFwd;
        if (lk_get_state(curr_bfap->stateLk) == ACC_INVALID ||
                lk_get_state(curr_bfap->stateLk) == ACC_DEALLOC ||
                lk_get_state(curr_bfap->stateLk) == ACC_RECYCLE ||
                curr_bfap->accMagic == ACCMAGIC_MARKER) 
        {
            goto _move_marker;
        }

        if (!ADVRWL_BFAP_FLUSH_READ_TRY(curr_bfap)) {
            /* The only contention for this lock is when the access
             * structure is being deallocatedor recycled. We know
             * therefore that there is no dirty data to flush so we
             * can just skip it.
             */
            goto _move_marker;
        }

        ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);
        if (IS_COWABLE_METADATA(curr_bfap)) {
            size = curr_bfap->bfaNextFob * ADVFS_FOB_SZ;
        } else {
            size = curr_bfap->file_size;
        }
        sts = fcache_vn_flush(&curr_bfap->bfVnode, 0, 0, 
                (uintptr_t) &pvt_param, FVF_WRITE | FVF_SYNC);

        if (sts != EOK) {
            ADVRWL_BFAP_FLUSH_UNLOCK(curr_bfap);
            ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
            goto _exit;
        }
        ADVSMP_BFAP_LOCK(&curr_bfap->bfaLock);
        curr_bfap->bfaFlags |= BFA_SNAP_CHANGE;
        curr_bfap->bfaFlags &= ~BFA_NO_SNAP_CHILD;
        ADVSMP_BFAP_UNLOCK(&curr_bfap->bfaLock);
        ADVRWL_BFAP_FLUSH_UNLOCK(curr_bfap);

        ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);

_move_marker:
        if (bfap_list_marker->setFwd == curr_bfap) {
            /* First remove the marker from the list */
            RM_ACC_SETLIST(bfap_list_marker, FALSE);

            /* Now add the marker after curr_bfap (to the right) */
            INS_ACC_SETLIST(bfap_list_marker, curr_bfap, FALSE);
        }
    }

_exit:
    RM_ACC_SETLIST(bfap_list_marker, FALSE);
    ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);

    ms_free(bfap_list_marker);
    return sts;
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_restore_cache_protectiosn - This routine sets all of open files 
 * in the parent fileset to only have holes be read-only.   *
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

statusT
advfs_snap_restore_cache_protections(
        bfSetT *parent_bf_set_p)    /* bfSetT pointer of parent fileset */
{
    bfAccessT *bfap_list_marker;
    bfAccessT *curr_bfap;
    struct advfs_pvt_param pvt_param;
    statusT sts = EOK;

    bfap_list_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
    bfap_list_marker->accMagic = ACCMAGIC_MARKER;
    bfap_list_marker->bfSetp = parent_bf_set_p;

    bzero(&pvt_param, sizeof(struct advfs_pvt_param));
    pvt_param.app_flags |= APP_RESTORE_PG_PROTECTION;

    ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
    ADD_ACC_SETLIST(bfap_list_marker, FALSE, FALSE);
    while (bfap_list_marker->setFwd != 
            (bfAccessT *)(&parent_bf_set_p->accessFwd)) 
    {
        curr_bfap = bfap_list_marker->setFwd;
        if (lk_get_state(curr_bfap->stateLk) == ACC_INVALID ||
                curr_bfap->accMagic == ACCMAGIC_MARKER) 
        {
            ADVFS_ACCESS_LOGIT( curr_bfap, "restore_cache_protections: moving past");
            goto _move_marker;
        }

        if (!ADVRWL_BFAP_FLUSH_READ_TRY(curr_bfap)) {
            /* The only contention for this lock is when the access
             * structure is being deallocatedor recycled. We know
             * therefore that there is no dirty data to flush so we
             * can just skip it.
             */
            ADVFS_ACCESS_LOGIT( curr_bfap, "restore_cache_protections: lock failed");
            goto _move_marker;
        }

        ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);

        /*
         * This will synchronize with any getpage callers that have already
         * made the decision to protect new pages that are brought into
         * cache.
         */
        ADVRWL_BFAP_SNAP_WRITE_LOCK( curr_bfap );


        ADVFS_ACCESS_LOGIT( curr_bfap, "restore_cache_protections: flushing");

        /*
         * Now we will restore the permissions on all in-cache pages of the
         * parent file.  
         */
        sts = fcache_vn_flush(&curr_bfap->bfVnode, 0, 0, 
                (uintptr_t) &pvt_param, FVF_WRITE | FVF_SYNC);

        MS_SMP_ASSERT( sts == EOK );
        if (sts != EOK) {
            ADVRWL_BFAP_FLUSH_UNLOCK(curr_bfap);
            ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
            goto _exit;
        }

        ADVRWL_BFAP_SNAP_UNLOCK( curr_bfap );

        ADVRWL_BFAP_FLUSH_UNLOCK(curr_bfap);

        ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);

_move_marker:
        if (bfap_list_marker->setFwd == curr_bfap) {
            /* First remove the marker from the list */
            RM_ACC_SETLIST(bfap_list_marker, FALSE);

            /* Now add the marker after curr_bfap (to the right) */
            INS_ACC_SETLIST(bfap_list_marker, curr_bfap, FALSE);
        }
    }

_exit:
    RM_ACC_SETLIST(bfap_list_marker, FALSE);
    ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);

    ms_free(bfap_list_marker);
    return sts;
}

/*
 * BEGIN_IMS
 *
 * advfs_snap_drain_writes - This routine will synchronize snapset creation 
 *                           with any open files that have the file lock
 *                           held for write.
 *
 * Parameters:
 *  
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */

int32_t exited_snap_drain = 0;

static
statusT
advfs_snap_drain_writes(
        bfSetT *parent_bf_set_p)    /* bfSetT pointer of parent fileset */
{
    bfAccessT *bfap_list_marker;
    bfAccessT *curr_bfap;
    statusT sts = EOK;

    bfap_list_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
    bfap_list_marker->accMagic = ACCMAGIC_MARKER;
    bfap_list_marker->bfSetp = parent_bf_set_p;

    ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
    ADD_ACC_SETLIST(bfap_list_marker, FALSE, FALSE);
    while (bfap_list_marker->setFwd != 
            (bfAccessT *)(&parent_bf_set_p->accessFwd)) 
    {
        curr_bfap = bfap_list_marker->setFwd;
        if (lk_get_state(curr_bfap->stateLk) == ACC_INVALID ||
                lk_get_state(curr_bfap->stateLk) == ACC_DEALLOC ||
                lk_get_state(curr_bfap->stateLk) == ACC_RECYCLE ||
                curr_bfap->accMagic == ACCMAGIC_MARKER) 
        {
            goto _move_marker;
        }

        ADVFS_ACCESS_LOGIT( curr_bfap, "advfs_snap_drain_writes processing");
        /* If there are no writes (via advfs_fswrite) we can skip this
         * file.   See advfs_fswrite.
         */
        if (curr_bfap->bfaWriteCnt == 0) {
            goto _move_marker;
        }
        if (!ADVSMP_BFAP_TRYLOCK(&curr_bfap->bfaLock)) {
            ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);
            delay(1);
            ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);

            continue;
        }
        ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);
        /* 
         * We need to wait for all outstanding writes to complete.  Once we
         * go to sleep and wake up, the bfap may be gone.  When we wake up,
         * we will just go back and re-process this bfap.
         */
        if (curr_bfap->bfaWriteCnt) {
            cv_wait(&curr_bfap->bfaSnapCv, &curr_bfap->bfaLock, 
                    CV_SPIN, CV_NO_RELOCK);

            ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);
            continue;
        }
        ADVSMP_BFAP_UNLOCK(&curr_bfap->bfaLock);
        ADVSMP_SETACCESSCHAIN_LOCK(&parent_bf_set_p->accessChainLock);

_move_marker:

        if (curr_bfap->accMagic == ACCMAGIC) {
            ADVFS_ACCESS_LOGIT( curr_bfap, 
                    "advfs_snap_drain_writes moving past");
        }

        if (bfap_list_marker->setFwd == curr_bfap) {
            /* First remove the marker from the list */
            RM_ACC_SETLIST(bfap_list_marker, FALSE);

            /* Now add the marker after curr_bfap (to the right) */
            INS_ACC_SETLIST(bfap_list_marker, curr_bfap, FALSE);
        }
    } /* end while(bfap_list_marker->setFwd != .... */
    RM_ACC_SETLIST(bfap_list_marker, FALSE);
    ADVSMP_SETACCESSCHAIN_UNLOCK(&parent_bf_set_p->accessChainLock);
    exited_snap_drain++;

    return sts;
}

/* 
 * BEGIN_IMS
 *
 * advfs_snapset_close_recursive - is a helper function for 
 * advfs_snapset_close.  It will close the snapshot tree in a post-traversal
 * order.  It uses recursion to traverse the tree. 
 *
 * Parameters:
 * bf_set_p - the bfSet to have all related snapsets closed
 * cur_set_p - current parent bfSet
 * parent_ftx - parent transaction handle
 *
 * Return Values:
 * EOK - Successfully closed bfSet in this iteration
 *
 * END_IMS
 */
static
statusT
advfs_snapset_close_recursive(
        bfSetT  *bf_set_p,  /* bfSet to have all related snapsets closed */
        bfSetT  *cur_set_p, /* Parent of cur_set_p */
        ftxHT   parent_ftx) /* Parent FTX */
{
    bfSetT *child_set_p = NULL, *next_set_p = NULL;
    statusT sts = EOK;

    MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_EQL(&bf_set_p->dmnP->BfSetTblLock.lock.rw, 
                RWL_WRITELOCKED));
    child_set_p = cur_set_p->bfsFirstChildSnapSet;
    while (child_set_p != NULL) {
        next_set_p = child_set_p->bfsNextSiblingSnapSet;
        if (cur_set_p->fsRefCnt == 1) {
            /* This is the last ref on the current fileset.
             * Disconnect the child from the parent.  The child may hang
             * around after the parent goes away. 
             */
            if (child_set_p->bfsParentSnapSet->bfsFirstChildSnapSet ==
                    child_set_p ) {
                child_set_p->bfsParentSnapSet->bfsFirstChildSnapSet = NULL;
            }
            child_set_p->bfsParentSnapSet = NULL;
            child_set_p->bfsNextSiblingSnapSet = NULL;
        }
        sts = advfs_snapset_close_recursive(bf_set_p, child_set_p, parent_ftx);
        MS_SMP_ASSERT(sts == EOK);
        child_set_p = next_set_p;
    }
    if (bf_set_p != cur_set_p) {
        cur_set_p->bfsSnapRefs--;
        bfs_close(cur_set_p, parent_ftx);
    }
    return EOK;
}

/* 
 * BEGIN_IMS
 *
 * advfs_snapset_close - closes all related snapsets opened via
 * advfs_snapset_access.
 *
 * Parameters:
 * bf_set_p - bfSet to have all related snapsets closed 
 * parent_ftx - parent transaction handle
 *
 * Return Value:
 * EOK - All related snapsets are closed
 *
 * END_IMS
 */
statusT
advfs_snapset_close(
        bfSetT  *bf_set_p,  /* bfSet to have all related snapsets closed */
        ftxHT   parent_ftx) /* Parent FTX */
{
    bfSetT *root_set_p, *next_parent_set_p;
    statusT sts = EOK;

    MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_EQL(&bf_set_p->dmnP->BfSetTblLock.lock.rw,
                RWL_WRITELOCKED));
    root_set_p = bf_set_p;
    next_parent_set_p = bf_set_p->bfsParentSnapSet;

    while (next_parent_set_p) {
        root_set_p = next_parent_set_p;
        next_parent_set_p = next_parent_set_p->bfsParentSnapSet;
    }
    sts = advfs_snapset_close_recursive(bf_set_p, root_set_p, parent_ftx);
    if (sts != EOK) {
        domain_panic( bf_set_p->dmnP, "advfs_snapset_close: recursive call error" );
    }
    return EOK;
}

/*
 * BEGIN_IMS
 * 
 * advfs_unlink_snapshot - 
 *
 * Parameters:
 *
 * Return Values:
 *
 */

void
advfs_unlink_snapshot(
        bfAccessT* bfap         /* in - bfap to unlink from parents */
        )
{
    statusT sts = EOK;
    int accessed_parent = FALSE;
    bfAccessT *parent_bfap = NULL;
    bfAccessT *cur_child = NULL, *prev_child = NULL;

    MS_SMP_ASSERT(bfap->bfaFirstChildSnap == NULL);

    if (!(bfap->bfaFlags & BFA_OPENED_BY_PARENT)) {
        return;
    }
    if (bfap->bfaParentSnap == NULL) {
        sts = bs_access_one(&parent_bfap, 
                bfap->tag,
                bfap->bfSetp->bfsParentSnapSet,
                FtxNilFtxH,
                BF_OP_INMEM_ONLY|BF_OP_IGNORE_BFS_DELETING|BF_OP_IGNORE_DEL|
                BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
        MS_SMP_ASSERT(parent_bfap != NULL);
        accessed_parent = TRUE;
    } else {
        parent_bfap = bfap->bfaParentSnap;
    }
    ADVRWL_BFAP_SNAP_WRITE_LOCK(parent_bfap);
    ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
    while (bfap->bfaFlags & BFA_XTNTS_IN_USE || 
            (bfap->bfaFlags & BFA_IN_COW_MODE))
    {
        SNAP_STATS( unlink_snapshot_wait_on_xtnts );
        cv_wait(&bfap->bfaSnapCv, &bfap->bfaSnapLock, 
                CV_RWLOCK_WR, CV_DFLT_FLG);
    }
    ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    if (parent_bfap->bfaFirstChildSnap == bfap) {
        parent_bfap->bfaFirstChildSnap = bfap->bfaNextSiblingSnap;
    } else {
        prev_child = parent_bfap->bfaFirstChildSnap;
        cur_child = prev_child->bfaNextSiblingSnap;
        while (cur_child != NULL && cur_child != bfap) {
            prev_child = cur_child;
            cur_child = cur_child->bfaNextSiblingSnap;
        }
        MS_SMP_ASSERT(cur_child != NULL);
        prev_child->bfaNextSiblingSnap = bfap->bfaNextSiblingSnap;
    }
    ADVRWL_BFAP_SNAP_UNLOCK(parent_bfap);
    MS_SMP_ASSERT(bfap->refCnt == 2);
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);
    bfap->refCnt = 1;
    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
    MS_SMP_ASSERT(bfap->bfaFlags & BFA_OPENED_BY_PARENT);
    MS_SMP_ASSERT(!(bfap->bfaFlags & BFA_EXT_OPEN));

    /*
     * Make sure we aren't leaving a ref on the parent when unlinking.
     * This is only valid if only 1 snapshot exists.  This is to help track
     * down a specific problem.
     */
#ifndef MULTIPLE_SNAPS
    MS_SMP_ASSERT( parent_bfap->bfaRefsFromChildSnaps == 0 );
#endif
    if (accessed_parent) {
        sts = bs_close_one(parent_bfap, MSFS_CLOSE_NONE, FtxNilFtxH);
        MS_SMP_ASSERT(sts == EOK);
    }
    return;
}

/*
 * BEGIN_IMS
 * 
 * advfs_unlink_snapset - Unlinks the given fileset from it's parent
 * and any siblings on-disk (and in-memory if necessary).
 *
 * Parameters:
 * bf_set_p - fileset to unlink
 * snap_flags - if SF_HAD_PARENT is set, then we exit.
 * parent_exc_ftx - parent transaction (may be FtxNilFtxH)
 *
 * Algorithm:
 * Find the fileset record (on-disk) that points to this fileset
 * we are deleting.  Remove the fileset entry from this list.
 * We also clear out the snapset tree pointers in the deleting fileset's
 * pointer.  If this routine returns EOK, then the fileset has been 
 * completely unlinked from the snapset tree (both on-disk and in memory).
 * 
 * We don't have any undo routines here because we have placed this fileset
 * on the bfs_del_pending_list and have flushed the log.  Therefore,
 * we will always try to redo the delete.  There is no point in undoing
 * the changes (if any) when we are about to do it again.
 *
 * Return Values:
 *
 */

statusT
advfs_unlink_snapset(
        bfSetT          *bf_set_p,
        snap_flags_t    snap_flags,
        ftxHT           parent_exc_ftx
        )
{
    bfSetIdT parent_set_id, cur_set_id, next_sibling_set_id;
    ftxHT ftxH;
    rbfPgRefHT fset_pg_h;

    statusT    sts                 = EOK;
    int        on_disk_update_only = FALSE;
    bfSetT    *root_bfset          = bf_set_p->dmnP->bfSetDirp;
    bfSetT    *prev_set_p          = NULL, *cur_set_p           = NULL;
    bfAccessT *tag_dir_bfap        = NULL, *prev_tag_dir_bfap   = NULL;
    int        is_first_child      = FALSE;
    int        close_tag_dir_bfap  = FALSE;
    bsBfSetAttrT *set_attr_p       = NULL, *prev_bfs_attr_p     = NULL;

    /* If the SF_HAD_PARENT flag is set, then there is no parent for this
     * fileset.  We have nothing to unlink from.
     */
    if (!(snap_flags & SF_HAD_PARENT)) {
        return EOK;
    }

    prev_bfs_attr_p = (bsBfSetAttrT *)ms_malloc(sizeof(bsBfSetAttrT));

    /* Get the parent_set_id & next_set_id of the snapset */
    sts = bmtr_get_rec(bf_set_p->dirBfAp,
            BSR_BFS_ATTR,
            (void *) prev_bfs_attr_p,
            sizeof(bsBfSetAttrT));
    if (sts != EOK) {
        domain_panic(bf_set_p->dmnP, 
                "advfs_unlink_snapset: Error reading BSR_BFS_ATTR, return code = %d", 
                sts);
        goto _exit;
    }
    next_sibling_set_id = prev_bfs_attr_p->bfsaNextSiblingSnap;
    parent_set_id       = prev_bfs_attr_p->bfsaParentSnapSet;
    MS_SMP_ASSERT(!BS_BFS_EQL(parent_set_id, nilBfSetId));

    if (bf_set_p->bfsParentSnapSet == NULL) {
        on_disk_update_only = TRUE;

        /* Now get the on-disk bfs attr record for the parent */
        sts = bs_access_one(&prev_tag_dir_bfap, /* parent snapshot */
                parent_set_id.dirTag,
                root_bfset,
                parent_exc_ftx,
                BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
        MS_SMP_ASSERT(sts == EOK);
        if (sts != EOK) {
           /* We domain panic here b/c if asserts are not enabled, we need to
            * stop IO on this domain because we are unable to complete the unlink
            * after the delete has been started.
            */
            domain_panic(bf_set_p->dmnP, 
                    "advfs_unlink_snapset: Error opening parent tag_dir, return code = %d", 
                    sts);
            goto _exit;
        }
        close_tag_dir_bfap = TRUE;
        sts = bmtr_get_rec(prev_tag_dir_bfap, /* parent snapshot */
                BSR_BFS_ATTR,
                (void *) prev_bfs_attr_p,
                sizeof(bsBfSetAttrT));
        if (sts != EOK) {
            domain_panic(bf_set_p->dmnP, 
                    "advfs_unlink_snapset: Error reading parent BSR_BFS_ATTR, return code = %d", 
                    sts);
            goto _exit;
        }

        if (BS_BFS_EQL(prev_bfs_attr_p->bfsaFirstChildSnapSet, 
                    bf_set_p->bfSetId)) 
        {
            /* This snapset is the first child in the snapset list */
            is_first_child = TRUE;
        } else if (BS_BFS_EQL(prev_bfs_attr_p->bfsaFirstChildSnapSet, 
                    nilBfSetId)) 
        {
            /* The parent's children list is empty.  This fileset is not
             * linked in.  Just return.  This may be the case if we
             * have already done the unlink of this snapset, but the parent
             * transaction did not complete/or this may be a redo.
             */
            sts = EOK;
            goto _exit;
        } else {
            /* We need to find the fileset record in the list that points
             * to the fileset that we are deleting.
             */
            (void)bs_close_one(prev_tag_dir_bfap, MSFS_CLOSE_NONE, parent_exc_ftx);
            close_tag_dir_bfap = FALSE;

            /* Load the bfs attr for the first child */
            sts = bs_access_one(&prev_tag_dir_bfap, /* first child */
                    prev_bfs_attr_p->bfsaFirstChildSnapSet.dirTag,
                    root_bfset,
                    parent_exc_ftx,
                    BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
            MS_SMP_ASSERT(sts == EOK);
            if (sts != EOK) {
                domain_panic(bf_set_p->dmnP, 
                        "advfs_unlink_snapset: Error opening first child tag_dir, return code = %d", 
                        sts);
                goto _exit;
            }
            close_tag_dir_bfap = TRUE;
            sts = bmtr_get_rec(prev_tag_dir_bfap, /* first child */
                    BSR_BFS_ATTR,
                    (void *) prev_bfs_attr_p,
                    sizeof(bsBfSetAttrT));
            if (sts != EOK) {
                domain_panic(bf_set_p->dmnP, 
                        "advfs_unlink_snapset: Error reading prev BSR_BFS_ATTR, return code = %s", 
                        sts);
                goto _exit;
            }

            cur_set_id = prev_bfs_attr_p->bfsaNextSiblingSnap;
            /* loop till we find the fileset we are deleting or we hit the
             * end of the list 
             */
            while ( !BS_BFTAG_EQL(cur_set_id.dirTag, bf_set_p->dirBfAp->tag) &&
                    (!BS_BFS_EQL(cur_set_id, nilBfSetId)) ) {
                (void)bs_close_one(prev_tag_dir_bfap, MSFS_CLOSE_NONE, 
                        parent_exc_ftx);
                close_tag_dir_bfap = FALSE;
                sts = bs_access_one(&prev_tag_dir_bfap,
                        cur_set_id.dirTag,
                        root_bfset,
                        parent_exc_ftx,
                        BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
                MS_SMP_ASSERT(sts == EOK);
                if (sts != EOK) {
                    domain_panic(bf_set_p->dmnP, 
                            "advfs_unlink_snapset: Error opening child tag_dir, return code = %d", 
                            sts);
                    goto _exit;
                }
                close_tag_dir_bfap = TRUE;
                sts = bmtr_get_rec(prev_tag_dir_bfap,
                        BSR_BFS_ATTR,
                        (void *) prev_bfs_attr_p,
                        sizeof(bsBfSetAttrT));
                if (sts != EOK) {
                    domain_panic(bf_set_p->dmnP, 
                            "advfs_unlink_snapset: Error reading prev BSR_BFS_ATTR, return code = %d", 
                            sts);
                    goto _exit;
                }
                cur_set_id = prev_bfs_attr_p->bfsaNextSiblingSnap;
            }

            if (BS_BFS_EQL(cur_set_id, nilBfSetId)) {
                /* This fileset is not in the parent's children list.
                 * Just return since there is no work to do.
                 */
                sts = EOK;
                goto _exit;
            }
            /* At this point, prev_tag_dir_bfap is the tag dir that needs
             * to be updated on disk and next_sibling_set_id is what
             * the tag dir's BSR_BFS_ATTR next field needs to point to.
             */
        } /* end else */
    } else {
        /* Fast path. In memory snapset structure is still setup so we
         * can take advantage.
         */
        on_disk_update_only = FALSE;
        if (bf_set_p->bfsParentSnapSet->bfsFirstChildSnapSet == bf_set_p) {
            /* The first child is the one we are deleting */
            is_first_child = TRUE;
            prev_tag_dir_bfap = bf_set_p->bfsParentSnapSet->dirBfAp;
        } else {
            /* Need to search for previous sibling for the update */
            prev_set_p = bf_set_p->bfsParentSnapSet->bfsFirstChildSnapSet;
            cur_set_p = prev_set_p->bfsNextSiblingSnapSet;
            while (cur_set_p != bf_set_p && (cur_set_p != NULL)) {
                prev_set_p = cur_set_p;
                cur_set_p = cur_set_p->bfsNextSiblingSnapSet;
            }
            MS_SMP_ASSERT(cur_set_p != NULL);
            prev_tag_dir_bfap = prev_set_p->dirBfAp;
        }
        sts = bmtr_get_rec(prev_tag_dir_bfap,
                BSR_BFS_ATTR,
                (void *) prev_bfs_attr_p,
                sizeof(bsBfSetAttrT));
        if (sts != EOK) {
            domain_panic(bf_set_p->dmnP, 
                    "advfs_unlink_snapset: Error reading in-mem BSR_BFS_ATTR for");
            goto _exit;
        }
    }


    /* At this point, the prev_bfs_attr is the BSR_BFS_ATTR record for
     * the fileset that needs to have the child list pointer updated.
     * Update child list to point to next sibling of bf_set_p.
     * Since we an exclusive transaction, no one else could be modifying
     * the ODS. 
     */

    sts = FTX_START_N( FTA_BS_BFS_UNLINK_SNAP_V1, &ftxH, parent_exc_ftx,
            bf_set_p->dmnP);
    if (sts != EOK) {
        domain_panic(bf_set_p->dmnP, 
                "advfs_unlink_snapset: failed to start transaction, return code = %d", 
                sts);
        goto _exit;
    }

    if (is_first_child == TRUE) {
        prev_bfs_attr_p->bfsaFirstChildSnapSet = next_sibling_set_id;
    } else {
        prev_bfs_attr_p->bfsaNextSiblingSnap = next_sibling_set_id;
    }

    /* Update the previous fileset in the child list pointing to the snapset 
     * we are unlinking.  
     */
    sts = bmtr_update_rec(prev_tag_dir_bfap,
            BSR_BFS_ATTR,
            (void *) prev_bfs_attr_p,
            sizeof(bsBfSetAttrT),
            ftxH,
            0);
    if (sts != EOK) {
        domain_panic(bf_set_p->dmnP, 
                "advfs_unlink_snapset: Error updating BSR_BFS_ATTR, return code = %d", 
                sts);
        ftx_fail(ftxH);
        goto _exit;
    }

    /* We can't fail the transaction (at least without a domain panic) after 
     * this point.  Now unlink (on-disk) the snapset we are deleting.  We remove
     * references to the parent and the next snap sibling. 
     */
    sts = bmtr_get_rec_ptr( bf_set_p->dirBfAp,
            ftxH,
            BSR_BFS_ATTR,
            sizeof(bsBfSetAttrT),
            1,      /* pin page */
            (void **) &set_attr_p,
            &fset_pg_h,
            NULL );

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_74);

    if (sts != EOK) {
        domain_panic(bf_set_p->dmnP, 
                "advfs_unlink_snapset: Error updating BSR_BFS_ATTR, return code = %d", 
                sts);
        /* This will domain panic also.  The ftx_fail will
         * clean up the transaction and release the ftx slot.
         */
        ftx_fail(ftxH);  
        goto _exit;
    } else {
        rbf_pin_record(fset_pg_h, &set_attr_p->bfsaParentSnapSet, 
                sizeof(set_attr_p->bfsaParentSnapSet));
        set_attr_p->bfsaParentSnapSet = nilBfSetId;
        
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_75);
        
        rbf_pin_record(fset_pg_h, &set_attr_p->bfsaNextSiblingSnap,
                sizeof(set_attr_p->bfsaNextSiblingSnap));
        set_attr_p->bfsaNextSiblingSnap = nilBfSetId;

        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_76);
    }

    if (!on_disk_update_only) {
        bfSetT *parent_set_p = bf_set_p->bfsParentSnapSet;

        ADVSMP_BFSET_LOCK(&parent_set_p->bfSetMutex);
        if (is_first_child == TRUE) {
            bf_set_p->bfsParentSnapSet->bfsFirstChildSnapSet = 
                bf_set_p->bfsNextSiblingSnapSet;
        } else {
            prev_set_p->bfsNextSiblingSnapSet = 
                bf_set_p->bfsNextSiblingSnapSet;
        }
        bf_set_p->bfsParentSnapSet = NULL;
        bf_set_p->bfsNextSiblingSnapSet = NULL;
        ADVSMP_BFSET_UNLOCK( &parent_set_p->bfSetMutex );    
    }

    /* Now that the snapset is unlinked from it's parents, we need to
     * remove any refs on the snapset from related filesets.  
     * These closes won't happen otherwise for each snap ref
     * because we have just removed this fileset from the in-memory and on-disk
     * list of the snapset tree.  A bs_bfs_close() on the related fileset 
     * wouldn't know about this snapset at that point. 
     */
    while (bf_set_p->bfsSnapRefs-- > 0) {
        bfs_close(bf_set_p, ftxH);
    }

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_77);
    
    ftx_done_n(ftxH, FTA_BS_BFS_UNLINK_SNAP_V1);

    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_78);

    /* TADA! We are done.  This fileset has been unlinked from it's parent
     * and any siblings.  We don't have an undo routine....so if the 
     * changes get to disk we are all set.  Even if the parent caller fails
     * the upper level transaction, the changes will remain on disk.  
     * The reasoning...we are deleting the fileset, it WILL get deleted
     * (see reasons in the comment header for this routine.  There is no
     * point in undoing the changes just to redo them again.  Since the fileset 
     * has been unlinked, it will be treated just like a "normal" fileset for 
     * deletion...'nuff said.
     */
_exit:
    if (close_tag_dir_bfap) {
        (void)bs_close_one(prev_tag_dir_bfap, MSFS_CLOSE_NONE, parent_exc_ftx);
    }

    if (prev_bfs_attr_p) {
        ms_free(prev_bfs_attr_p);
    }
    return sts;
}


/*
 * BEGIN_IMS
 * 
 * advfs_verify_snap_page_protections -  This routine is intended to be
 * called by advfs_getpage during SMP_ASSERT to validate that pages in cache
 * are correctly marked for pi_pg_ro.  For now, it validates that the range
 * [offset..offset+range] is marked pi_pg_ro over any holes in the file.
 * Later, it can validate that if the file has snapshot children, pages that
 * are not yet COWed are actually marked pi_pg_ro.
 *
 * Parameters:
 *
 * Return Values:
 *
 */
statusT
advfs_verify_snap_page_protections( struct vnode* vp,
                        off_t offset,
                        off_t size,
                        fcache_vminfo_t *fc_vminfo )
{

    page_fsdata_t *pfdat, *plist;
    fcache_pflags_t pflags;
    int err=0;
    size_t alloc_size;
    off_t  alloc_offset;
    off_t  req_off;
    size_t req_size;
    off_t ending_request_offset;
    statusT sts;
    fpa_status_t alloc_status;
    bfAccessT* bfap = VTOA( vp );
    extent_blk_desc_t *sparseness_map;
    extent_blk_desc_t *smap_entry;
    uint64_t           xtnt_cnt;
    
    alloc_offset = offset;
    alloc_size = size;
    ending_request_offset = alloc_offset + alloc_size;
    req_off = offset;
    req_size = size;

    if ( bfap->bfaParentSnap ) {
        sts = advfs_acquire_xtntMap_locks( bfap, 
                    X_LOAD_REFERENCE|XTNT_WAIT_OK,
                    SF_SNAP_NOFLAGS );
    } else {
        x_lock_inmem_xtnt_map( bfap,
                    X_LOAD_REFERENCE|XTNT_WAIT_OK );
    }


    while (req_off < ending_request_offset) {

        alloc_offset = req_off;
        alloc_size = req_size;

        plist = fcache_page_alloc( fc_vminfo,
                                        vp,
                                        &alloc_offset,
                                        &alloc_size,
                                        alloc_offset,
                                        ending_request_offset,
                                        &alloc_status,
                                        FPA_PGCACHE,
                                        FP_DEFAULT );
#ifdef ADVFS_SMP_ASSERT
        /* If the file is opened for Direct I/O, make sure the pages being
         * put into the UFC have an active range associated with them,
         * otherwise stale data could be returned or written to the file,
         * causing data corruption or the appearence of corruption.
         */
        if ( plist && bfap->dioCnt ) {
            ASSERT_ACTRANGE_HELD( bfap, alloc_offset, alloc_size );
        }
#endif /* ADVFS_SMP_ASSERT */
        
        if (plist == NULL) {
            break;
        }

        sts = advfs_get_blkmap_in_range( bfap,
                                bfap->xtnts.xtntMap,
                                &alloc_offset,
                                alloc_size,
                                &sparseness_map,
                                &xtnt_cnt,
                                RND_VM_PAGE,
                                EXB_ONLY_HOLES,
                                XTNT_WAIT_OK|XTNT_LOCKS_HELD );

        MS_SMP_ASSERT( sts == EOK );

        if (sts != EOK) {
            fcache_page_release( fc_vminfo,
                                alloc_size,
                                &plist,
                                FP_DEFAULT);
        }



        /* 
         * Verify that every page over a hole is marked pi_pg_ro
         */
        smap_entry = sparseness_map;
        pfdat = plist;
        while (smap_entry && pfdat) {
            if (pfdat->pfs_off < smap_entry->ebd_offset) {
                /* This page is over storage. */
                if (!HAS_SNAPSHOT_CHILD(bfap)) {
                    MS_SMP_ASSERT( pfdat->pfs_info.pi_pg_ro == 0 );
                }
                pfdat = pfdat->pfs_next;
                continue;
            }

            /* pfdat offset is greater than start of smap_entry, now check
             * to see that it is in the range. 
             */
            if (pfdat->pfs_off < (off_t)(smap_entry->ebd_offset + 
                                         smap_entry->ebd_byte_cnt)) {
                MS_SMP_ASSERT( pfdat->pfs_info.pi_pg_ro );
                pfdat = pfdat->pfs_next;
                continue;
            } else {
                /* pfdat is past smap_entry, advance smap */
                smap_entry = smap_entry->ebd_next_desc;
                continue;
            }

        }

        fcache_page_release( fc_vminfo,
                                alloc_size,
                                &plist,
                                FP_DEFAULT);



        advfs_free_blkmaps( &sparseness_map );


        req_off += alloc_size;
        req_size -= alloc_size; 

    }

    if ( bfap->bfaParentSnap ) {
        sts = advfs_drop_xtntMap_locks( bfap, 
                    SF_SNAP_NOFLAGS );
    } else {
        ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
    }

                                
    return EOK;
    
}


/*
 * BEGIN_IMS
 *
 * advfs_snap_clean_state -  
 * 
 * Parameters:
 *
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 * It is assumed the caller has synchronized with snapset creation so that
 * the check to determine if this is the last snapshot is not racing with
 * creation.
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_snap_clean_state(bfSetT *bf_set_p,
        bfTagT del_tag)
{
    bfSetT *parent_bf_set_p;

    bfAccessT *parent_bfap = NULL;
    int        sts         = EOK;
    struct advfs_pvt_param pvt_param;

    if (!BFSET_VALID(bf_set_p)) {
        return sts;
    }


    parent_bf_set_p = bf_set_p->bfsParentSnapSet;
    if (!parent_bf_set_p) {
        /* There is no parent for this fileset.  Nothing to do */
        return sts;
    }

    /*
     * This will sync with snapset creation and access.
     */
    ADVRWL_BFSETTBL_READ( bf_set_p->dmnP);

    if ( (parent_bf_set_p->bfsFirstChildSnapSet == bf_set_p) && 
         (bf_set_p->bfsNextSiblingSnapSet == NULL) ) {
        /* This fileset is the last child snapset of the parent.
         * We will need to modify this parent file only if it's already
         * in-memory
         */
        sts = bs_access_one(&parent_bfap,
                del_tag,
                bf_set_p->bfsParentSnapSet,
                FtxNilFtxH,
                BF_OP_INMEM_ONLY|BF_OP_IGNORE_BFS_DELETING|BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX);
        if (sts != EOK) {
            goto _exit;
        }



        ADVSMP_BFAP_LOCK(&parent_bfap->bfaLock);
        parent_bfap->bfaFlags &= ~BFA_SNAP_CHANGE;
        ADVSMP_BFAP_UNLOCK(&parent_bfap->bfaLock);


        /* We call bs_close here because the access on the parent may have 
         * caused another racing close to not do last close processing 
         * in regards to closing any snapshot children.
         */
        bs_close(parent_bfap, MSFS_CLOSE_NONE);
    }

_exit:

    ADVRWL_BFSETTBL_UNLOCK( bf_set_p->dmnP);


    return sts;
}




/*
 * BEGIN_IMS
 *
 * advfs_snap_split_extents_for_cow - This routine will take a list of
 * extents and split those extents based on what has an has not been COWed
 * to the last child of the bfap passed in.  If the single extent in the
 * parent has been half COWed in the child, the extent will be split into
 * two separate extents with the COWed extents having the high-order bit of
 * the fob set to indicate it has not been COWed.
 * 
 * Parameters:
 *
 * 
 * Return Values:
 *
 * bfap_extent_cnt will be incremeented by one for each split that occurs.
 *
 * Entry/Exit Conditions:
 * It is assumed that the bfaSnapLock of the bfap passed in is held.
 *
 * Algorithm:
 *
 * Find the last snap child.
 *
 * Get the extents for the last snap child.
 *
 * Find all UNMAPPED holes in the child and subdivide extents in the
 * parents' extents to indicate those ranges need to be COWed.
 *
 * END_IMS
 */

statusT
advfs_snap_split_extents_for_cow( bfAccessT *bfap,
                                extent_blk_desc_t *extent_list,
                                uint64_t        *extent_cnt)
{

    bfAccessT           *last_child;
    extent_blk_desc_t   *child_extents;

    return ENOT_SUPPORTED;

}

/*
 * BEGIN_IMS
 *
 * advfs_assert_no_cow - This routine is used to assert that the specified
 *                       range does not need snapshot copy-on-write.
 * 
 * Parameters:
 *
 *      bfap    - IN - bfAccessT for the file to check if range was COW'ed
 *      offset  - IN - range starting byte offset
 *      size    - IN - range byte length
 * 
 * Return Values:
 *
 *      It either returns, or asserts.
 *
 * Entry/Exit Conditions:
 *
 * Algorithm:
 *
 * END_IMS
 */

#ifdef ADVFS_SMP_ASSERT
void
advfs_assert_no_cow( bfAccessT *bfap, off_t offset, size_t size )
{
    statusT            sts;
    int                snap_locked      = FALSE;
    off_t              tmp_offset;
    size_t             tmp_size;
    uint64_t           xtnt_count       = 0;
    extent_blk_desc_t *storage_blkmap   = NULL;
    extent_blk_desc_t *blkp;
    bfAccessT         *snap;
    bfAccessT         *snap_to_check;
    
    ADVRWL_BFAP_SNAP_READ_LOCK( bfap );

    snap = bfap->bfaFirstChildSnap;
    if (snap == NULL) {
        goto _done;      /* no snapshots */
    } else {
        /*
         * Assume the first child is not out of sync, but later verify this.
         */
        snap_to_check = snap;
    }

    /* 
     * find last child snapshot that is not out of sync.
     */
    while( snap->bfaNextSiblingSnap != NULL ) {
        if ( !(snap->bfaFlags & BFA_OUT_OF_SYNC) ) {
            snap_to_check = snap;       /* ignore out of sync snaps */
        }
        snap = snap->bfaNextSiblingSnap;
    }

    if (snap_to_check->bfaFlags & BFA_OUT_OF_SYNC) {
        /*
         * Verify we didn't pick the first child and it was out of sync.
         */
        goto _done;
    }

    ADVRWL_BFAP_SNAP_READ_LOCK( snap_to_check );
    snap_locked = TRUE;

    /*
     * Bound the request based on the original file size when the file was
     * snapped.  That is to say, this request might be for something beyond
     * the range the snapshot is responsible for.
     */
    tmp_offset = offset;
    tmp_size   = size;
    if ( tmp_offset >= (off_t)snap_to_check->bfa_orig_file_size ) {
        goto _done;     /* snapshot is not this big */
    }
    if ( (off_t)(tmp_offset + tmp_size) > snap_to_check->bfa_orig_file_size ) {
        tmp_size = snap_to_check->bfa_orig_file_size - tmp_offset;
    }

    /* 
     * Get the extent map for this range 
     */
    sts = advfs_get_blkmap_in_range( snap_to_check, 
                                     snap_to_check->xtnts.xtntMap,
                                    &tmp_offset, 
                                     tmp_size,
                                    &storage_blkmap,
                                    &xtnt_count,
                                     RND_ALLOC_UNIT,
                                     EXB_ONLY_HOLES|EXB_DO_NOT_INHERIT, 
                                     XTNT_WAIT_OK );
    MS_SMP_ASSERT(sts == EOK);

    /* 
     * see if any of these extents need to be COW'ed 
     */
    for( blkp=storage_blkmap; blkp != NULL; blkp=blkp->ebd_next_desc ) {
        MS_SMP_ASSERT( blkp->ebd_vd_blk != XTNT_TERM );
    }

_done:
    if ( storage_blkmap != NULL ) {
        (void)advfs_free_blkmaps( &storage_blkmap );
    }
    if ( snap_locked ) {
        ADVRWL_BFAP_SNAP_UNLOCK(snap_to_check);
    }
    ADVRWL_BFAP_SNAP_UNLOCK(bfap);
}
#endif /* ADVFS_SMP_ASSERT */
