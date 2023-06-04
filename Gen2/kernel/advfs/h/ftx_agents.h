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
 *  AdvFS
 *
 * Abstract:
 *
 *  ftx_agents.h
 *
 *  This header file agent identifiers and agent description strings.
 *
 */

/*
 *  Agent identifiers are kept in this file and only in this file.
 */

#ifndef FTX_AGENTS_H
#define FTX_AGENTS_H

typedef enum {
    FTA_NULL =                          0,

                   /**** Bitfile sets or snapshot agents ****/
    FTA_BS_BFS_CREATE_V1 =              1,
    FTA_BS_BFS_UNLINK_SNAP_V1 =         2,
    FTA_BS_BFS_DEL_LIST_REMOVE_V1 =     3,
    FTA_BS_BFS_SNAP_V1 =                4,

                          /**** Bmt operations ****/
    FTA_BS_BMT_PUT_REC_V1 =             5,
    FTA_BS_BMT_FREE_MCELL_V1 =          6,
    FTA_BS_BMT_ALLOC_MCELL_V1 =         7,
    FTA_BS_BMT_ALLOC_LINK_MCELL_V1 =    8,
    FTA_BS_BMT_LINK_MCELLS_V1 =         9,
    FTA_BS_BMT_UNLINK_MCELLS_V1 =       10,

                         /**** bitfile creation ****/
    FTA_BS_BF_CREATE_V1 =               11,
    FTA_FREE_SLOT_12    =               12,     /* free slot */

                          /**** bitfile deletion */
    FTA_BS_DEL_DELETE_V1 =              13,
    FTA_BS_DEL_DEFERRED_V1 =            14,
    FTA_BS_BMT_FREE_BF_MCELLS_V1 =      15,

                      /**** extent map operations ****/
    FTA_BS_XTNT_UPD_MCELL_CNT_V1 =      16,
    FTA_BS_XTNT_UPD_REC_V1 =            17,
    FTA_BS_XTNT_CRE_REC =               18,
    FTA_BS_XTNT_CRE_XTRA_REC_V1 =       19,
    FTA_BS_XTNT_CRE_SHADOW_REC_V1 =     20,
    FTA_BS_XTNT_INSERT_CHAIN_V1 =       21,
    FTA_BS_XTNT_REMOVE_CHAIN_V1 =       22,
    FTA_BS_XTNT_ZERO_MAP_V1_UNUSED =    23, /* unused */

                        /*** migrate operations ****/
    FTA_BS_MIG_MIGRATE_V1 =             24,
    FTA_BS_MIG_ALLOC_V1 =               25,
    FTA_BS_MIG_CNT_V1 =                 26,
    FTA_BS_MIG_INSERT_HEAD_V1 =         27,
    FTA_BS_MIG_REMOVE_HEAD_V1 =         28,
    FTA_BS_MIG_REMOVE_MIDDLE_V1 =       29,
    FTA_BS_MIG_ALLOC_LINK_V1 =          30,

                        /**** shadow operations ****/
    FTA_BS_SHA_SHADOW_V1 =              31,
    FTA_BS_SHA_BITFILE_TYPE_V1 =        32,

                          /**** storage bitmap ****/
    FTA_BS_SBM_ALLOC_BITS_V1 =          33,

                        /**** storage management ****/
    FTA_BS_STG_ADD_V1 =                 34,
    FTA_BS_STG_REMOVE_V1 =              35,
    FTA_BS_SBM_DEALLOC_BITS_V1 =        36,
    FTA_BS_STG_COPY_V1 =                37,

                        /**** striped bitfile management ****/
    FTA_BS_STR_STRIPE_V1 =              38,
    FTA_BS_STR_UPD_XTNT_REC_V1 =        39,

                          /**** tag directory ****/
    FTA_BS_TAG_WRITE_V1 =               40,
    FTA_BS_TAG_EXTEND_TAGDIR_V1 =       41,
    FTA_BS_TAG_PATCH_FREELIST_V1 =      42,

                           /**** file system ****/
    FTA_FS_REMOVE_V1 =                  43,
    FTA_FS_UPDATE_V1 =                  44,
    FTA_FS_CREATE_V1  =                 45,
    FTA_FS_INSERT_V1 =                  46,
    FTA_FS_LINK_V1 =                    47,
    FTA_FS_RENAME_V1 =                  48,

                              /**** quotas ****/
    FTA_FS_DQUOT_SYNC_V1 =              49,

    FTA_TER_CRE_TER_NULL =              50,
    FTA_TER_CRE_XTRA_NULL =             51,
    FTA_TER_UPDATE_PTR_NULL =           52,
    FTA_TER_APPEND_TO_TER_NULL =        53,
    FTA_TER_APPEND_TO_XTRA_NULL =       54,
    FTA_TER_UPDATE_TOTAL_NULL =         55,
    FTA_TER_FLAGS_TER_NULL =            56,
    FTA_TER_FLAGS_XTRA_NULL =           57,
    FTA_TER_SET_MEDIA_NULL =            58,
    FTA_TER_SET_CLEAN_NULL =            59,
    FTA_TER_ADD_STG_NULL =              60,
    FTA_TER_ZERO_XTNT_MAP_NULL =        61,
    FTA_TER_TRUNC_MAP_NULL =            62,
    FTA_TER_SET_ACCESS_NULL =           63,
    FTA_TER_SET_DIRTY_NULL =            64,

    FTA_BS_CLOSE_V1 =                   65,
    FTA_BS_XTNT_REWRITE_MAP_V1 =        66,
    FTA_BS_STG_ALLOC_MCELL_V1 =         67,
    FTA_BS_COW_PG =                     68,
    FTA_BS_COW =                        69,
    FTA_BFS_CREATE =                    70,
    FTA_BFS_DEL_PENDING_ADD =           71,
    FTA_BFS_DELETE =                    72,
    FTA_BFS_DELETE_SNAP =               73,
    FTA_BFS_SNAP =                      74,
    FTA_BS_RBMT_ALLOC_MCELL_V1 =        75,
    FTA_BS_BMT_UPDATE_REC_V1 =          76,
    FTA_BS_BMT_EXTEND_V1 =              77,
    FTA_BS_RBMT_EXTEND_V1 =             78,
    FTA_PREALLOC =                      79,
    FTA_BS_DEL_ADD_LIST_V1 =            80,
    FTA_BS_DEL_REM_LIST_V1 =            81,
    FTA_BS_DEL_FREE_PRIMARY_MCELL_V1  = 82,
    FTA_BS_DEL_FREE_MCELL_CHAIN_V1 =    83,
    FTA_BS_DEL_FTX_START_V1 =           84,
    FTA_BS_DELETE_6 =                   85,     /* temp used by rmvol bug fix */
    FTA_BS_XTNT_RELOAD_ORIG_XTNTMAP =   86,
    FTA_SET_BFSET_FLAG =                87,     
    FTA_PUNCH_HOLE =                    88,     
    FTA_BS_TAG_SWITCH_ROOT_TAGDIR_V1 =  89,
    FTA_BS_SET_NEXT_TAG_V1 =            90,
    FTA_FS_CREATE_1 =                   91, /* note cfs use in ftx_done_fs() */
    FTA_FS_CREATE_2 =                   92,
    FTA_FS_DIR_INIT_1 =                 93,
    FTA_FS_DIR_LOOKUP_1 =               94,
    FTA_FS_DIR_LOOKUP_2 =               95,
    FTA_FS_FILE_SETS_1 =                96,
    FTA_FS_INIT_QUOTAS_V1 =             97,
    FTA_FS_WRITE_ADD_STG_V1 =           98,
    FTA_FS_WRITE_V1 =                   99,
    FTA_OSF_SETATTR_1 =                 100,
    FTA_OSF_FSYNC_V1 =                  101,
    FTA_OSF_REMOVE_V1 =                 102,
    FTA_OSF_LINK_V1 =                   103,
    FTA_OSF_RENAME_V1 =                 104,
    FTA_OSF_RMDIR_V1 =                  105,
    FTA_FTX_LOG_DATA =                  106,
    FTA_FS_SYSCALLS_1 =                 107,
    FTA_FS_SYSCALLS_2 =                 108,
    FTA_MSS_COMMON_1 =                  109,
    FTA_BS_MIG_MOVE_METADATA_V1 =       110,
    FTA_LGR_SWITCH_VOL =                111,
    FTA_BS_XTNT_INSERT_CHAIN_LOCK_V1 =  112,
    FTA_BS_XTNT_REMOVE_CHAIN_LOCK_V1 =  113,
    FTA_BS_STR_UPD_XTNT_REC_LOCK_V1 =   114,
    FTA_FREE_SLOT_115 =                 115,     /* free slot */
    FTA_FTX_SWITCH_LOG =                116,
    FTA_FTX_CHECKPOINT_LOG =            117,
    FTA_BS_BMT_CREATE_REC =             118,
    FTA_FS_QUOTA_OFF_V1 =               119,
    FTA_BS_STG_SET_ALLOC_DISK_V1 =      120,
    FTA_BS_SET_BF_PARAMS =              121,
    FTA_BS_SET_BF_IPARAMS =             122,
    FTA_BS_SET_BFSET_PARAMS =           123,
    FTA_BS_MIG_MOVE_METADATA_EXC_V1 =   124,
    FTA_FS_CREATE_ROOT_FILE =           125,
    FTA_BS_SWITCH_ROOT_TAGDIR_V1 =      126,
    FTA_FS_GET_QUOTA_V1 =               127,
    FTA_FS_SET_QUOTA_V1 =               128,
    FTA_FS_SET_USE_V1 =                 129,
    FTA_FS_QUOTA_ON_V1 =                130,
    FTA_FS_DETACH_QUOTA_V1 =            131,
    FTA_FS_ATTACH_QUOTA_V1 =            132,
    FTA_FS_GET_QUOTA =                  133,
    FTA_BFS_LINK_SNAP =                 134,
    FTA_FREE_SLOT_135 =                 135,     /* free slot */
    FTA_BFS_CREATE_2 =                  136,
    FTA_OSF_SETATTR_2 =                 137,
    FTA_OSF_SYNCDATA_V1 =               138,
    FTA_FS_DQUOT_SYNC_V2 =              139,
    FTA_FREE_SLOT_140 =                 140,	 /* free slot */
    FTA_FREE_SLOT_141 =                 141,	 /* free slot */
    FTA_FREE_SLOT_142 =                 142,     /* fre slot  */
    FTA_FREE_SLOT_143 =                 143,     /* free slot */
    FTA_FREE_SLOT_144 =                 144,     /* free slot */
    FTA_BS_SET_VD_PARAMS =		145,
    FTA_ADVFS_ALLOC_MCELL =		146,
    FTA_BS_BMT_DEFERRED_MCELL_FREE =    147,
    FTA_DATA_LOGGING_WRITE =            148,
    FTA_TAG_TO_FREELIST =               149,
    FTA_FS_WRITE_TRUNC =                150,
    FTA_IDX_UNDO_V1 =                   151,
    FTA_BS_DEL_TRUNCATE_DDL =           152,
    FTA_BS_FREEZE =                     153,
    FTA_BS_VD_EXTEND =                  154,
    FTA_SS_DMN_DATA_WRITE =             155,
				/**** AdvFS ACLS ****/
    FTA_ACL_UPDATE =                    156,
    FTA_ACL_UPDATE_WITHOUT_UNDO =       157,
    FTA_ACL_UPDATE_WITH_UNDO =          158,
    /* Snapshot transaction */
    FTA_SNAP_OUT_OF_SYNC =              159,
    FTA_SNAP_MCELL_V1 =                 160,
    FTA_SNAP_RECS_V1 =                  161,
    FTA_SNAP_NEW_MCELL =                162,
    FTA_META_SNAP =                     163,
    FTA_GETPAGE_COW =                   164,

    FTA_ADVFS_THRESHOLD_EVENT =         165,

/***********************************************
 *                                             *
 * PLEASE REMEMBER TO UPDATE FTX_MAX_AGENTS    *
 *                                             *
 ***********************************************/

    FTX_MAX_AGENTS =                    166
} ftxAgentIdT;

#endif  /* FTX_AGENTS_H */

