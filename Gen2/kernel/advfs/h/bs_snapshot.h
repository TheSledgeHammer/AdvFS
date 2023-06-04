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
/*
 *
 * Facility:
 *
 *    Advfs Storage System
 *
 * Abstract:
 *
 *    bs_snapshot.h 
 *    This file contains the primary routine protoypes for dealing with 
 *    snapshots on AdvFS.
 *
 * Date:
 *
 *    December 4, 2003
 */

#ifndef _ADVFS_BS_SNAPSHOT_
#define _ADVFS_BS_SNAPSHOT_

#include <ms_privates.h>

/**************************************
 * snapshot stats macros
 *************************************/
#ifdef ADVFS_SMP_ASSERT
#define ADVFS_SNAP_STATS
#endif 

#ifdef ADVFS_SNAP_STATS
struct advfs_snap_stats_struct {
    /* Advfs_access_snap_parents */
    uint32_t access_snap_parent_already_open;
    uint32_t access_snap_parent_raced_another_open;
    uint32_t access_snap_parent_racer_failed;
    uint32_t access_snap_parent_e_no_such_tag;
    uint32_t access_snap_parent_error_1;
    uint32_t access_snap_parent_closing_parents;
    /* Advfs_access_snap_children */
    uint32_t access_snap_children_no_work;
    uint32_t access_snap_children_flushed_dirty_stats;
    uint32_t access_snap_children_dirty_stats_start_over;
    uint32_t access_snap_children_child_already_open;
    uint32_t access_snap_children_child_bf_set_deleting;
    uint32_t access_snap_children_child_doesnt_exist;
    uint32_t access_snap_children_out_of_sync_call;
    uint32_t access_snap_children_out_of_sync_failed;
    uint32_t access_snap_children_calling_setup_cow;
    uint32_t access_snap_children_setup_failed;
    uint32_t access_snap_out_of_sync_failed_2;
    /* advfs_close_snaps */
    uint32_t close_snaps_not_last_close;
    uint32_t close_snaps_closing_parents;
    uint32_t close_snaps_closing_children;
    /* advfs_close_snap_parents */
    uint32_t close_parents_no_work;
    uint32_t close_parents_closed_parent;
    /* advfs_close_snap_children */
    uint32_t close_children_no_work;
    uint32_t close_children_closed_children;
    /* advfs_acquire_snap_locks */
    uint32_t acquire_snap_locks_no_work;
    uint32_t acquire_snap_locks_migstg_write_failed;
    uint32_t acquire_snap_locks_migstg_read_failed;
    uint32_t acquire_snap_locks_skipped_out_of_sync_child;
    uint32_t acquire_snap_locks_skipped_out_of_sync_child_2;
    /* advfs_drop_snap_locks */
    uint32_t acquire_drop_locks_called;
    uint32_t acquire_drop_locks_skipped_out_of_sync_child;
    /* advfs_acquire_xtntMap_locks */
    uint32_t acquire_xtntMap_locks_x_lock_failed;
    /* advfs_drop_xtntMap_locks */
    /* advfs_snap_migrate_setup */
    uint32_t snap_migrate_setup_found_virgin_snap;
    /* advfs_snap_migrate_setup_recursive */
    uint32_t snap_migrate_setup_recur_modified_array;
    uint32_t snap_migrate_setup_recur_to_children;
    uint32_t snap_migrate_setup_recur_to_siblings;
    /* advfs_snap_migrate_done */
    uint32_t snap_migrate_done_unlocking;
    uint32_t snap_migrate_done_tag_dir_update_failed;
    /* advfs_get_snap_xtnt_desc */
    uint32_t snap_get_xtnt_desc_quick_return;
    uint32_t snap_get_xtnt_desc_return_cond_1;
    uint32_t snap_get_xtnt_desc_return_cond_2;
    uint32_t snap_get_xtnt_desc_skipping_desc;
    uint32_t snap_get_xtnt_desc_return_cond_3;
    uint32_t snap_get_xtnt_desc_good_return;
    uint32_t snap_get_xtnt_desc_good_return_2;
    uint32_t snap_get_xtnt_desc_clip_fob;
    uint32_t snap_get_xtnt_desc_clip_fob_2;
    uint32_t snap_get_xtnt_desc_continued;
    /* advfs_add_snap_stg */
    uint32_t add_snap_stg_get_blkamp_failed;
    uint32_t add_snap_stg_out_of_sync_child;
    uint32_t add_snap_stg_get_blkmap_failed_2;
    uint32_t add_snap_stg_out_of_sync_failed;
    uint32_t add_snap_stg_skipping_stg_extent;
    uint32_t add_snap_stg_done_with_cur_bfap_desc;
    uint32_t add_snap_stg_trying_to_add_cow_hole;
    uint32_t add_snap_stg_add_cow_hole_failed;
    uint32_t add_snap_stg_out_of_sync_failed_2;
    uint32_t add_snap_stg_continuing;
    uint32_t add_snap_stg_adding_stg;
    uint32_t add_snap_stg_add_stg_failed;
    uint32_t add_snap_stg_out_of_sync_failed_3;
    uint32_t add_snap_stg_continuing_3;
    uint32_t add_snap_stg_get_blkmap_failed_3;
    uint32_t add_snap_stg_out_of_sync_failed_4;
    uint32_t add_snap_stg_continuing_2;
    uint32_t add_snap_stg_advnace_desc;
    uint32_t add_snap_stg_break_inner;
    uint32_t add_snap_stg_status_error;
    uint32_t add_snap_stg_free_resrouces_error;
    uint32_t add_snap_stg_continue_4;
    uint32_t add_snap_stg_skipping_out_of_sync_child;
    uint32_t add_snap_stg_skipping_out_of_sync_child_2;
    uint32_t add_snap_stg_unlocking_children;
    /* advfs_issue_snap_io */
    uint32_t issue_snap_io_advancing_beyond_desc;
    uint32_t issue_snap_io_advancing_before_desc;
    uint32_t issue_snap_io_io_end_greater_than_desc;
    uint32_t issue_snap_io_io_end_less_than_desc;
    uint32_t issue_snap_io_called_bsstartio;
    /* advfs_snap_revoke_cfs_tokens */
    uint32_t revoke_cfs_tokens_droped_file_lock;
    uint32_t revoke_cfs_tokens_had_child_to_revoke;
    uint32_t revoke_cfs_tokens_cv_waited;
    uint32_t revoke_cfs_tokens_cow_mode_enter_failed;
    uint32_t revoke_cfs_tokens_revoking_bfap_token;
    uint32_t revoke_cfs_tokens_cow_mode_enter_failed_2;
    /* advfs_snap_cleanup_and_unlock */
    uint32_t snap_cleanup_unlocked_from_snap_maps;
    uint32_t snap_cleanup_unlocked_for_write_case;
    uint32_t snap_cleanup_unlocked_for_XTNTS_loop;
    uint32_t snap_cleanup_unlocked_parents;
    uint32_t snap_cleanup_left_cow_mode_for_child;
    uint32_t snap_cleanup_left_cow_mode_for_bfap;
    /* advfs_setup_cow */
    uint32_t setup_cow_no_work;
    uint32_t setup_cow_get_rec_error;
    uint32_t setup_cow_meta_parent_is_root;
    uint32_t setup_cow_meta_parent_is_not_root;
    uint32_t setup_cow_set_user_size;
    uint32_t setup_cow_new_snap_mcell_failed;
    uint32_t setup_cow_out_of_sync_failed;
    uint32_t setup_cow_snap_copy_mcell_failed;
    uint32_t setup_cow_out_of_sync_failed_2;
    uint32_t setup_cow_tag_update_failed;
    uint32_t setup_cow_out_of_sync_failed_3;
    /* advfs_new_snap_mcell */
    uint32_t new_snap_mcell_new_mcell_failed;
    /* advfs_new_snap_mcell_undo_opx */
    uint32_t new_snap_mcell_undo_opx;
    /* advfs_get_first_extent_mcell */
    uint32_t get_first_extent_mcell_stg_alloc_succeeded;
    uint32_t get_first_extent_mcell_extent_skip_failed;
    uint32_t get_first_extent_mcell_selec_vd_failed;
    uint32_t get_first_extent_mcell_stg_alloc_failed_2;
    /* advfs_snap_copy_mcell_chain */
    uint32_t snap_copy_mcell_chain_refpg_failed;
    uint32_t snap_copy_mcell_chain_snap_mcell_failed;
    uint32_t snap_copy_mcell_chain_error_case;
    /* advfs_snap_mcell */
    uint32_t snap_mcell_failed_ftx_start;
    uint32_t snap_mcell_is_first_call;
    uint32_t snap_mcell_is_not_first_call;
    uint32_t snap_mcell_alloc_link_failed;
    uint32_t snap_mcell_rbf_pinpg_failed;
    uint32_t snap_mcell_copying_record;
    uint32_t snap_mcell_ftx_failed;
    /* advfs_snap_out_of_sync */
    uint32_t snap_out_of_sync_no_work;
    uint32_t snap_out_of_sync_setting_bfset_out_of_sync;
    uint32_t snap_out_of_sync_get_rec_ptr_failed;
    uint32_t snap_out_of_sync_update_tagmap_failed;
    uint32_t snap_out_of_sync_setting_fail_out_of_sync;
    uint32_t snap_out_of_sync_setting_fail_out_of_sync_2;
    uint32_t snap_out_of_sync_pinning_bfset;
    uint32_t snap_out_of_sync_error_case;
    /* advfs_snap_children_out_of_sync */
    uint32_t snap_children_out_of_sync_no_work;
    uint32_t snap_children_out_of_sync_called;
    /* advfs_make_cow_hole */
    uint32_t make_cow_hole_calling_append_cow_hole;
    uint32_t make_cow_hole_found_cow_hole;
    uint32_t make_cow_hole_found_stg;
    uint32_t make_cow_hole_updating_on_disk_xtnts;
    uint32_t make_cow_hole_update_failed;
    uint32_t make_cow_hole_merging_maps;
    uint32_t make_cow_hole_sts_not_ok;
    uint32_t make_cow_hole_finished_ftx;
    /* advfs_append_cow_hole */
    uint32_t append_cow_hole_extending_map;
    uint32_t append_cow_hole_extend_failed;
    uint32_t append_cow_hole_copy_failed;
    uint32_t append_cow_hole_case_1;
    uint32_t append_cow_hole_case_2;
    uint32_t append_cow_hole_get_first_failed;
    uint32_t append_cow_hole_add_cow_common_failed;
    uint32_t append_cow_hole_error_case;
    /* advfs_insert_cow_hole */
    uint32_t insert_cow_hole_extending_map;
    uint32_t insert_cow_hole_extend_failed;
    uint32_t insert_cow_hole_copy_failed;
    uint32_t insert_cow_hole_add_cow_common_failed;
    uint32_t insert_cow_hole_case_1;
    uint32_t insert_cow_hole_copy_failed_2;
    uint32_t insert_cow_hole_impossible;
    uint32_t insert_cow_hole_copy_failed_3;
    uint32_t insert_cow_hole_mcell_alloc_failed;
    uint32_t insert_cow_hole_error_case;
    /* advfs_add_cow_hole_common */
    uint32_t add_cow_common_case_1;
    uint32_t add_cow_common_extend_xtnt_map;
    uint32_t add_cow_common_extend_failed_new_sub;
    uint32_t add_cow_common_failure_case_1;
    uint32_t add_cow_common_failure_case_2;
    uint32_t add_cow_common_failure_check_extend;
    uint32_t add_cow_common_extend_xtnts_2;
    uint32_t add_cow_common_extend_on_disk;
    uint32_t add_cow_common_failure_case_3;
    /* advfs_get_new_sub_xtnt */
    uint32_t advfs_get_new_sub_extend_case;
    uint32_t advfs_get_new_sub_extend_failed;
    uint32_t advfs_get_new_sub_get_first_failed;
    uint32_t advfs_get_new_sub_init_sub_failed;
    /* advfs_force_cow_and_unlink */ 
    /* advfs_wait_for_snap_io */
    uint32_t wait_for_snap_io_wakeup_on_all_case;
    uint32_t wait_for_snap_io_wakeeup_iocounter_one;
    uint32_t wait_for_snap_io_wakeeup_io_error;
    uint32_t wait_for_snap_io_iocounter_zero;
    uint32_t wait_for_snap_io_read_wait;
    uint32_t wait_for_snap_io_write_case;
    uint32_t wait_for_snap_io_iodone;
    uint32_t wait_for_snap_io_write_io_errors;
    uint32_t wait_for_snap_io_write_wait;
    uint32_t wait_for_snap_io_issue_snap_io;
    /* advfs_fs_write_direct */
    uint32_t direct_write_force_cow_failed; 
    /* advfs_unlink_snapshot */
    uint32_t unlink_snapshot_wait_on_xtnts;
    /* advfs_bs_delete_fileset_tags */
    uint32_t delete_fileset_tags_access_err_1;
    uint32_t delete_fileset_tags_access_err_2;
    /* advfs_bs_add_stg */
    uint32_t bs_add_stg_returning_delList;
    /* advfs_getpage */
    uint32_t getpage_process_snap_status_changed;
    uint32_t getpage_eagain_getting_snap_locks;
    uint32_t getpage_buf_create_failure;
    /* advfs_prealloc */
    uint32_t prealloc_failed_force_cow;
    /* dir_trunc_start */
    uint32_t dir_trunc_start_forcing_cow;
    uint32_t dir_trunc_start_cow_failed;
    uint32_t dir_trunc_start_limiting_truncate;
    /* idx_trunc_limited_truncate */
    uint32_t idx_trunc_limited_truncate;
};
typedef struct advfs_snap_stats_struct advfs_snap_stats_t;

extern advfs_snap_stats_t advfs_snap_stats;

#define SNAP_STATS( __stat_name ) advfs_snap_stats.__stat_name++
#else
#define SNAP_STATS( __stat_name )
#endif

/******************************************
 * Constants 
 *****************************************/

/*
 * This defines the block size that COWs will generally occur in.
 * This value indicates the number of allocation units to be COWed together.
 * This should not be used in code except to initialize advfs_cow_alloc_unit 
 * (defined in bs_snapshot.c).
 */
#define ADVFS_COW_ALLOC_UNIT_DEFAULT 8

/* 
 * Defines the maximum number of allocation units to fault in at one time
 * when performing a forces COW. 
 */
#define ADVFS_FORCE_COW_MAX_ALLOC_UNITS 64


/* Maximum depth of snapshots. (Maximum number of parents) */
#define  ADVFS_MAX_SNAP_DEPTH 5

/* The high order bit is set in the extent maps returned to CFS to indicate
 * that the extent has already been COWed */
#define ADVFS_CFS_COW_IS_COMPLETE 1<<63


/* 
 * This is used as a marker for bfa_orig_file_size to indicate that the file
 * should ignore this value.  
 */
#define ADVFS_ROOT_SNAPSHOT ((uint64_t)-1)


/*
 * This flag is passed to fs_update_stats and bmtr_update_rec to indicate
 * that the stats should be flushed to disk WITHOUT trying to do any COWing.
 * The other flags to fs_update_stats are defined all over, so be careful
 * about this value.
 */
#define ADVFS_UPDATE_NO_COW 0x1000


/*****************************************
 * Macros
 ****************************************/


/* Macros to checking snapshot state */
#define HAS_SNAPSHOT_CHILD( _bfap )                             \
        ( ((_bfap)->bfSetp->bfsFirstChildSnapSet != NULL) &&    \
          !((_bfap)->bfaFlags & BFA_NO_SNAP_CHILD) )

#define HAS_SNAPSHOT_PARENT( _bfap )                    \
        ( ((_bfap)->bfSetp->bfsParentSnapSet != NULL) &&        \
          !((_bfap)->bfaFlags & BFA_ROOT_SNAPSHOT) )

#define HAS_RELATED_SNAPSHOT( _bfap )                   \
        (HAS_SNAPSHOT_CHILD( _bfap ) || HAS_SNAPSHOT_PARENT( _bfap )) 

/*
 * Is the bfap metadata and is it not reserved and not a tag directory file
 */
#define IS_COWABLE_METADATA( _bfap )                    \
        ( (_bfap->dataSafety == BFD_METADATA) &&        \
          (!(((int64_t)_bfap->tag.tag_num) < 0)) &&     \
          (!(BS_BFTAG_EQL(_bfap->bfSetp->dirTag, staticRootTagDirTag))) )

#define IS_COW_CANDIDATE( _bfap )                       \
        ( HAS_SNAPSHOT_PARENT( _bfap ) &&               \
          ( IS_COWABLE_METADATA( _bfap ) ||             \
            ((_bfap)->dataSafety == BFD_USERDATA) ))

#define ADVFS_SNAPSHOT_CREATE_SYNC( _bfap )                             \
{                                                                       \
    uint32_t dummy;                                                     \
    ADVFS_ATOMIC_FETCH_INCR( &(_bfap)->bfaWriteCnt, &dummy );           \
    while ((_bfap)->bfSetp->bfSetFlags & BFS_IM_SNAP_IN_PROGRESS) {     \
        ADVFS_ATOMIC_FETCH_DECR( &(_bfap)->bfaWriteCnt, &dummy );       \
        cv_broadcast( &(_bfap)->bfaSnapCv, NULL, CV_NULL_LOCK);         \
        ADVSMP_BFSET_LOCK( &(_bfap)->bfSetp->bfSetMutex );              \
        if ((_bfap)->bfSetp->bfSetFlags & BFS_IM_SNAP_IN_PROGRESS) {    \
            cv_wait( &(_bfap)->bfSetp->bfsSnapCv,                       \
                    &(_bfap)->bfSetp->bfSetMutex,                       \
                    CV_SPIN,                                            \
                    CV_NO_RELOCK );                                     \
        } else {                                                        \
            ADVSMP_BFSET_UNLOCK( &(_bfap)->bfSetp->bfSetMutex );        \
        }                                                               \
        ADVFS_ATOMIC_FETCH_INCR( &(_bfap)->bfaWriteCnt, &dummy );       \
    }                                                                   \
}

#define ADVFS_SNAPSHOT_DONE_SYNC( _bfap )                               \
{                                                                       \
    int32_t dummy;                                                      \
    ADVFS_ATOMIC_FETCH_DECR( &(_bfap)->bfaWriteCnt, &dummy );           \
    if ((_bfap)->bfaWriteCnt == 0 &&                                    \
            ((_bfap)->bfSetp->bfSetFlags & BFS_IM_SNAP_IN_PROGRESS)) {  \
        cv_broadcast( &(_bfap)->bfaSnapCv, NULL, CV_NULL_LOCK );        \
    }                                                                   \
}

/*****************************************
 * Forward declarations 
 *****************************************/
struct bsInMemXtntDescId;
struct bsXtntDesc;



/******************************************
 * Types
 *****************************************/
typedef enum {
    SF_SNAP_NOFLAGS     = 0x0,  /* No Flags */
    SF_SNAP_READ        = 0x1,  /* Indicates a read operation is occuring on a snapshot */
    SF_SNAP_WRITE       = 0x2,  /* Indicates a write operation is occuring */
    SF_HAD_PARENT       = 0x4,  /* Used to indicate to advfs_unlink_snapset that a parent 
                                 * DID exist, but may have been closed */
    SF_FOUND_SNAPSET    = 0x10, /* Indicates that the fileset being opened has been 
                                 * traversed while accessing other related snapsets
                                 * * see advfs_access_snapset_recursive */
    SF_OUT_OF_SYNC      = 0x20, /* Indicates a fileset that is out of sync has 
                                 * been found and children must be marked out of sync */
    SF_NO_UNLINK        = 0x40, /* Indicates to advfs_force_cow_and_unlink that the
                                 * unlink is not desired */
    SF_NO_LOCK          = 0x80, /* Used to indicate to advfs_snap_revoke_cfs_tokens that
                                 * the file lock is not held. */
    SF_LOCAL_BLKMAP     = 0x100, /* Used to indicate to 
                                  * advfs_acquire_xtntMap_locks that the caller
                                  * is only interested in a local block map.  */
    SF_ACCESSED_CHILDREN = 0x200, /* Used to indicate that 
                                   * advfs_access_snap_children opened
                                   * a child snapshot */
    SF_FAKE_ORIG        = 0x400, /* Indicates to advfs_issue_snap_io that the fake IO
                                  * should be on the anchr_origbug. */
    SF_INVAL_ALL        = 0x800, /* Invalidate pages after COWing.  Used by direct IO */
    SF_SNAP_SKIP_PARENTS= 0x1000, /* Don't try closing the parent snapshots. */
    SF_IGNORE_BFS_DEL   = 0x2000, /* Ignore the deleted flag for bfs_access */
    SF_SYNC_REQUIRED    = 0x4000, /* Synchronize with snapset create is required */
    SF_NO_WAIT          = 0x8000, /* Don't wait for locks or storage addition */
    SF_HARD_OUT_OF_SYNC = 0x10000, /* A fileset has been marked hard out-of-sync 
                                    * and it's children and siblings need to 
                                    * be marked hard out-of-sync as well. */
    SF_IGNORE_BFS_OUT_OF_SYNC   = 0x20000,/* Ignore the out-of-sync tagflag for
                                           * bfs_access */
    SF_DIRECT_IO_APPEND = 0x40000 /* Indicates that an active range has been
                                   * added for EOF. Used by 
                                   * advfs_force_cow_and_unlink */

} snap_flags_t;


/********************************************
 * Function prototypes 
 *******************************************/
statusT
advfs_access_snap_parents(
        bfAccessT*      bfap,
        bfSetT*         bf_set_ptr,
        ftxHT           parent_ftx);


statusT
advfs_access_snap_children(
        bfAccessT*      bfap,
        ftxHT           parent_ftx,
        snap_flags_t*   snap_flags);


statusT
advfs_close_snaps(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags);

statusT
advfs_acquire_snap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx);

statusT
advfs_drop_snap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags);

statusT
advfs_acquire_xtntMap_locks(
        bfAccessT*      bfap,
        xtntMapFlagsT   xtnt_flags,
        snap_flags_t    snap_flags);

statusT
advfs_drop_xtntMap_locks(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags);

statusT
advfs_snap_migrate_setup(
        bfAccessT*      bfap,
        void**          unlock_hdl,
        ftxHT           parent_ftx);


statusT
advfs_snap_migrate_done(
        bfAccessT*      bfap,
        void*           unlock_hdl,
        bfMCIdT         new_prim_mcell,
        ftxHT           parent_ftx,
        int32_t         error);


statusT
advfs_get_snap_xtnt_desc(
        bf_fob_t                        fob_offset,
        bfAccessT*                      bfap,
        struct bsInMemXtntDescId*       xtnt_desc_id,
        struct bsXtntDesc*              xtnt_desc,
        bfAccessT**                     source_bfap );

statusT
advfs_get_next_snap_xtnt_desc(
        bfAccessT*                      bfap,
        struct bsInMemXtntDescId*       xtnt_desc_id,
        struct bsXtntDesc*              xtnt_desc,
        bfAccessT**                     source_bfap);


statusT
advfs_add_snap_stg(
        bfAccessT*              bfap,
        off_t                   offset,
        size_t                  size,
        off_t*                  min_child_stg,
        off_t*                  max_child_stg,
        extent_blk_desc_t**     snap_maps,
        snap_flags_t            snap_flags,
        ftxHT                   parent_ftx);

statusT
advfs_issue_snap_io(
        ioanchor_t*             io_anchor,
        extent_blk_desc_t*      snap_maps,
        snap_flags_t            snap_flags,
        bfAccessT*              bfap,
        ftxHT                   parent_ftx);

statusT
advfs_snap_revoke_cfs_tokens(
        bfAccessT*      bfap,
        snap_flags_t    snap_flags );

statusT
advfs_snap_cleanup_and_unlock(
        bfAccessT*      bfap,
        extent_blk_desc_t**     snap_maps,
        snap_flags_t    snap_flags);


statusT
advfs_setup_cow(
        bfAccessT*      parent_bfap,
        bfAccessT*      child_bfap,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx);


void
advfs_new_snap_mcell_undo_opx(
        ftxHT           ftxH,
        int             opRecSz,
        void*           opRecp);

statusT
advfs_snap_copy_mcell_chain(
        bfMCIdT         parent_prim_mcell,  
        bfMCIdT         child_prim_mcell,  
        domainT         *dmnP,            
        ftxHT           parent_ftx);       

statusT
advfs_sync_cow_metadata(
        bfAccessT*      bfap,
        off_t           offset,
        size_t          size,
        ftxHT           parent_ftx);
                

statusT
advfs_snap_out_of_sync(
        bfTagT          tag,
        bfAccessT*      bfap,
        bfSetT*         bf_set_p,
        ftxHT           parent_ftx);

statusT
advfs_snap_children_out_of_sync(
        bfAccessT*      bfap,
        ftxHT           parent_ftx);


void
advfs_snap_print_hard_out_of_sync(
        bfSetIdT        bf_set_id);

statusT
advfs_snap_print_out_of_sync(
        bfTagT          tag,
        bfSetT*         bf_set_p);


statusT
advfs_make_cow_hole( 
        bfAccessT*      bfap,
        bf_fob_t        start_fob,
        bf_fob_t        num_fobs,
        ftxHT           parent_ftx );

statusT
advfs_force_cow_and_unlink(
        bfAccessT*      bfap,
        off_t           start_of_cow,
        size_t          bytes_to_cow,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx);


statusT
advfs_wait_for_snap_io(
        struct ioanchor**       io_anchor_head,
        bfAccessT*      bfap,
        off_t*          error_offset,
        extent_blk_desc_t**     snap_maps,
        ftxHT           parent_ftx);

statusT
advfs_snap_protect_datafill_plist( 
        bfAccessT*      bfap, 
        page_fsdata_t*  plist,
        off_t           offset,
        size_t          size);

statusT
advfs_snap_unprotect_incache_plist(
        bfAccessT*              bfap,
        struct page_fsdata*     plist,
        off_t                   off,
        size_t                  size,
        int32_t                 *found_stg_backing,
        xtntMapFlagsT           blkmap_flags);

statusT
advfs_snapset_access(
        bfSetT*         bf_set_p,
        snap_flags_t    snap_flags,
        ftxHT           parent_ftx);

statusT
advfs_create_snapset(
        char *parent_dmn_name,
        char *parent_set_name,
        char *snap_dmn_name,
        char *snap_fset_name,
        bfSetIdT *snap_set_id,
        snap_flags_t snap_flags,
        ftxIdT cfs_xid);

statusT
advfs_snap_restore_cache_protections(
        bfSetT *parent_bf_set_p);   /* bfSetT pointer of parent fileset */

statusT
advfs_snap_clean_state(bfSetT *bf_set_p,
        bfTagT del_tag);

statusT
advfs_snapset_close(
        bfSetT  *bf_set_p,      /* bfSet to have all related snapsets closed */
        ftxHT   parent_ftx);    /* Parent FTX */

statusT
advfs_close_snap_children(
        bfAccessT*  bfap);      /* bfap to have all child snapshots closed */

void
advfs_unlink_snapshot(
        bfAccessT*  bfap);      /* bfap to unlink from parent */

statusT
advfs_unlink_snapset(
        bfSetT*         bf_set_p,        /* fileset pointer to unlink */
        snap_flags_t    snap_flags,      /* SF_HAD_PARENT if parent exists */
        ftxHT           parent_exc_ftx); /* exclusive transaction handle */


statusT
advfs_snap_clean_perms(bfSetT *bf_set_p,
        bfTagT del_tag);

void
advfs_assert_no_cow( bfAccessT *bfap,   /* IN - bfAccessT for the file to
                                         *      check if range was COW'ed */
                     off_t offset,      /* IN - range starting byte offset */
                     size_t size );     /* IN - range byte length */

/* End function prototypes */

#endif /* _ADVFS_BS_SNAPSHOT_ */
