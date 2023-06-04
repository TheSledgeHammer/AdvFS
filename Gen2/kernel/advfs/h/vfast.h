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
 *    vfast.h
 *    Header file for vfast(aka smartstore) functionality.
 *
 */

#ifndef _SS_H_
#define _SS_H_

/* include header files */
#include <advfs/bs_ods.h>
#include <advfs/bs_msg_queue.h>
#include <advfs/bs_public.h>
#include <advfs/ms_generic_locks.h>

extern  uint32_t   SS_is_running;    /* mostly read */


#define SECHOUR (60*60)

/* 
 * The epoch 1/1/1970 was a Thursday - add 4 days
 */
#define GETCURRWDAY(tv_sec, newday)                    \
{                                                      \
    newday = (tv_sec / SECDAY + 4) % 7;                \
}

/* Defaults */
#define SS_INIT_PCT_MSGQ_MORE_THDS 80 /* % of msg Q filled before more thds */
#define SS_INIT_MSGQ_SIZE         128 /* message queues sizes */
#define SS_INIT_LIST_THDS         2 /* initial # of thds in list pool */
#define SS_INIT_WORK_THDS         2 /* initial # of thds in work pool */
#define SS_INIT_PCT_IOS_WHEN_BUSY 1 /* percent IOS allowed when busy */
#define SS_INIT_HOT_LST_GROW_SZ   75 /* in list entries/volume */
#define SS_INIT_FRAG_LST_SZ       200 /* in list entries/volume */
#define SS_INIT_PACK_LST_SZ     300 /* in list entries/volume */
#define SS_INIT_FILE_ACC_THRESH  1000 /* number of IOs before hot list bump */
#define SS_INIT_STEADY_STATE    24 /* in hours before hot list operational */
#define SS_INIT_MINUTES_HOT_LST 30 /* minutes before hot files distrib */
#define SS_INIT_HOT_ERROR_PENALTY 60 /* minutes before hot files distrib again*/

/* Minimum values */
#define SS_MIN_PCT_MSGQ_MORE_THDS 50 /* % of msg Q filled before more thds */
#define SS_MIN_MSGQ_SIZE          64 /* message queues sizes */
#define SS_MIN_LIST_THDS         2 /* min number of thds per list worker pool */
#define SS_MIN_WORK_THDS         2 /* min number of thds per work pool */
#define SS_MIN_PCT_IOS_WHEN_BUSY 0 /* percent IOS when busy */
#define SS_MIN_FILE_ACC_THRESH 200 /* in number of IOs before hot list bump */
#define SS_MIN_STEADY_STATE      0 /* in hours before hot list operational */
#define SS_MIN_MINUTES_HOT_LST  5 /* minutes before files on hot list distrib */
#define SS_MIN_HOT_ERROR_PENALTY 30 /* minutes before hot files distrib again*/

/* Maximum values */
#define SS_MAX_PCT_MSGQ_MORE_THDS 100 /* % of msg Q filled before more thds */
#define SS_MAX_MSGQ_SIZE          256 /* message queues sizes */
#define SS_MAX_LIST_THDS          6 /* max thds per list worker pool */
#define SS_MAX_WORK_THDS          6 /* max thds per work pool */
#define SS_MAX_PCT_IOS_WHEN_BUSY  50 /* percent IOS when otherwise busy */
#define SS_MAX_FILE_ACC_THRESH 10000 /* number of IOs before hot list bump */
#define SS_MAX_STEADY_STATE      518 /* hours before hot list operational */
#define SS_MAX_MINUTES_HOT_LST  1500 /* minutes before files hot list distrib */
#define SS_MAX_HOT_ERROR_PENALTY 1440 /* minutes before hot files distrib again*/

/* IO control */
#define SS_FOBCNT       64  /* number of 1k file offset blocks to migrate for 
                             * each work cycle.  With a default page size of 8k,
                             * this if 8 pages per work cycle */

/* monitor thread constants, all of these intervals are in seconds */
#define SS_VD_SAMPLE_INTERVAL     1
#define SS_HOT_LIST_CHK_REVS      600 /* interval to check hot list */
#define SS_MSG_CHK_REVS           10  /* interval to check hot list msg input */
#define SS_DEFRAG_ACTIVE_CHK_REVS 40  /* interval to check for no IO. */
#define SS_DEFRAG_ALL_CHK_REVS    300 /* interval to check any frag. files */

/* messages rate constants */
#define SS_MSGS_PER_SEC_GOAL   20 /* desired msg rate is adjusted here */
#define SS_MAX_MSGS_ALLOWED  (SS_MSGS_PER_SEC_GOAL*(SS_VD_SAMPLE_INTERVAL*SS_MSG_CHK_REVS))
#define SS_MAX_MSGS_IN_WORKQ   2  /* Maximum msgs allowed in work queue before adding */
                                  /* another thread */

/* other constants */
#define SS_MAX_DEFRAG_ALL_MSGS  (SS_INIT_MSGQ_SIZE / 2) 
                      /* maximum DEFRAG_ALL files to send to fragment 
                       * list per domain, per cycle.  */
#define SS_MAX_DEFRAG_ALL_PAGES  8
                      /* maximum tag pages to read per cycle per domain */
#define SS_HOT_LST_CROSS_PCT  5 /* 5% of top hot files distributed(not incl rsvd) */
#define SS_HOT_TRACKING_DAYS  7 /* 7 days worth of hot count hits is tracked */
#define SS_INIT_BMT_PAGES_PER_PASS 32 /* in pages before blocking again */
#define SS_PACK_RANGE_PCT_FREE      5 /* Pack when largest free chunk is LT 5% 
                                       * of the total free space. */
#define SS_PACK_RANGE_PCT_MAX_SIZE 25 /* pack when clear size not more than 25%
                                       * of total vol size */
#define SS_BALANCE_GOAL             2 /* volumes shall be kept within 4%
                                       * capacity of each other */
#define SS_PARENT 0
#define SS_CHILD  1
#define SS_UPDATE 1
#define SS_READ   2

/* vfast macros */

#define EXTENTS_FRAG_RATIO(pageCnt,xtntCnt) \
                 ((uint64_t) ((uint64_t)pageCnt*32L) / (uint64_t) xtntCnt) 
#define BITS_PER_ARRAY_INDEX      (64)
#define PG_TO_ARRAY_INDEX(page)   ((uint64_t) page / BITS_PER_ARRAY_INDEX)
#define PG_TO_BIT(page)           ((uint64_t) 1<<(page % BITS_PER_ARRAY_INDEX))
#define TAG_PG_TO_ARRAY_INDEX(page) ((page+BITS_PER_ARRAY_INDEX)/BITS_PER_ARRAY_INDEX)

#define SET_FRAGMENTED_TAG_PAGE(array,page) \
          (array[PG_TO_ARRAY_INDEX(page)] |= PG_TO_BIT(page))
#define CLEAR_FRAGMENTED_TAG_PAGE(array,page) \
          (array[PG_TO_ARRAY_INDEX(page)] &= ~(PG_TO_BIT(page)))


/* thread pool stuff */

typedef struct tpool {
     mutexT       plock;     /* locks next field */
     int          threadCnt; /* thread count for this pool */
     int          shutdown;  /* flag used in vfast shutdown */
} tpoolT;

/* vfast messaging stuff */

typedef enum {
    SS_HOT_LIST_CHK,       /* check hot list for file to move */
    SS_DEFRAG_ACTIVE_CHK,  /* send off a ok to run msg to vds that are bored */
    SS_DEFRAG_ALL_CHK      /* check for files needed to be defragmented,
                            * if there are no pending messages.
                            */
} ssPeriodicMsgTypeT;

typedef enum {
    FRAG_UPDATE_ADD,  /* add or update entry in frag list */
    HOT_ADD_UPDATE_MISS,    /* page not found in memory, causing IO */
    HOT_ADD_UPDATE_WRITE,   /* wrote a new page causing IO*/
    HOT_REMOVE, /* remove list entry from both hot and frag lists */
    SS_LIST_STOP
} ssListMsgTypeT;

typedef enum {
    SS_VD_RUN,    /* msg sent due to vfast turn to run */
    SS_WORK_STOP
} ssWorkMsgTypeT;

typedef struct {
    vdIndexT vdi;
    uint64_t fragRatio; /* primary sort in list */
    uint32_t xtntCnt;   /* secondary sort in list */
} ssFragAddMsgT;

typedef struct {
    vdIndexT vdi;      /* primary mcell vdi */
    uint32_t  cnt[SS_HOT_TRACKING_DAYS];      /* counter */
    uint64_t  size;     /* file size */
} ssHotMsgT;

typedef struct {
    ssListMsgTypeT msgType;  /* msg type for future expansion  */
    bfSetIdT bfSetId;
    bfTagT tag;
    ssFragAddMsgT  frag;
    ssHotMsgT  hot;
} ssListMsgT;

typedef struct {
    ssWorkMsgTypeT msgType;    /* msg type for future expansion  */
    bfDomainIdT domainId;      /* domain with list to change */
    vdIndexT vdIndex;          /* vd with list to change */
} ssWorkMsgT;


/* basic vfast structures */

typedef enum
{
    SS_MSG_IDLE,   /* vd is idle and no msgs are pending */
    SS_POSTED, /* vd has a work ok msg pending processing */
    SS_STOP    /* thd is notified that it should abort due to dmn deactivate */
} ssMsgOpT;

typedef uint64_t ssTagArrayT;  /* used to implement -o defrag_all */

typedef enum
{
    SS_MIG_IDLE, /* vd is idle and no migrates are pending */
    SS_PROCESSING,  /* thd is currently in a migrate cycle */
    SS_PARKED, /* thd is parked awaiting continuation broadcast from io thd */
    SS_ABORT   /* thd is notified that it should abort due to dmn deactivate */
} ssMigOpT;

typedef struct ssHotEntry {
    struct ssHotEntry *ssHotFwd; /* must stay in first position */
    struct ssHotEntry *ssHotBwd;  /* must stay in second position */
    int ssHotPlaced;  /* vd where file was placed, set by work thd  */
                      /* whenever ss first migrates this file, also an error */
                     /* can go here. */
    vdIndexT ssHotVdi;     /* current vd of primary mcell;
                       * if ssHotPlaced, should be same */
    bfTagT   ssHotTag;     /* tag of file */
    bfSetIdT ssHotBfSetId; /* settag of file */
    uint32_t  ssHotIOCnt[7]; /* cnt of IOs on this file by day of week */
    uint64_t  ssHotFileSize;/* size of file */
    adv_time_t ssHotLastUpdTime; /* last time one of the Cnts above was updated */
    adv_time_t ssHotEntryTime; /* time entry was placed on list */
    adv_time_t ssHotErrTime; /* time entry tried and failed to place */
} ssHotLLT;

typedef struct ssHotHdr {      
    struct ssHotEntry *ssHotFwd; /* must stay in first position */
    struct ssHotEntry *ssHotBwd; /* must stay in second position */
    uint16_t        ssHotListCnt; /* number of hot list entries for this dmn */
} ssHotHdrT;

typedef struct ssFragEntry {
    struct ssFragEntry  *ssFragRatFwd;  /* must stay in first position */
    struct ssFragEntry  *ssFragRatBwd;  /* must stay in second position */
    uint32_t  ssFragRatio;
    uint32_t  ssXtntCnt;
    bfTagT   ssFragTag;
    bfSetIdT ssFragBfSetId;
} ssFragLLT;

typedef struct ssFragHdr {
    struct ssFragEntry *ssFragRatFwd;  /* must stay in first position */
    struct ssFragEntry *ssFragRatBwd;  /* must stay in second position */
    uint32_t             ssFragListCnt;
    uint64_t             ssFragHdrThresh;
} ssFragHdrT;

typedef struct ssPackEntry {
    struct ssPackEntry  *ssPackFwd;
    struct ssPackEntry  *ssPackBwd;
    bfTagT               ssPackTag;
    bfSetIdT             ssPackBfSetId;
    bf_fob_t    ssPackFobOffset;        /* File offset block to pack at */
    bf_fob_t    ssPackFobCount;         /* File offset block count to pack */
    bf_vd_blk_t ssPackStartXtBlock;     /* block at ssPackFobOffset */
    bf_vd_blk_t ssPackEndXtBlock;       /* block at PackFobOffset + PackFobCnt */
} ssPackLLT;

typedef struct ssPackHdr {
    ssPackLLT       *ssPackFwd;
    ssPackLLT       *ssPackBwd;
    uint32_t          ssPackListCnt;
    uint64_t          ssPackZoneStartBlk;
    uint64_t          ssPackZoneEndBlk;
    bs_meta_page_t   ssPackLastBMTPage;
} ssPackHdrT;

typedef struct ssDmnInfo {
    mutexT     ssDmnLk; /* protects ssDmnHotWorking, Msg Counts,
                         * output variables, ssDmnSSThdCnt, ssDmnSSWaiters
                         */
    int        ssDmnSSThdCnt;  /* current number of vfast threads in dmn */
    int        ssDmnSSWaiters; /* number of waiters on ssDmnSSThdCnt */
    cv_t       ssDmnSSWaitersCV;/* Condition Variable for ssDmnSSWaiters */
    ssDmnOpT   ssDmnState;  /* processing state that domain is in */
    /* message flow rate to lists stats */
    uint32_t    ssDmnListMsgSendCnt; /* cnt of list msgs send attempts */
    uint32_t    ssDmnListMsgDropCnt;       /* cnt of list msgs dropped */

    /* internal used params */
    adv_time_t ssFirstMountTime;   /* time domain first had a WRITE mount */
    int        ssIoInterval;       /* Interval for ss when IO queues busy */
    mutexT     ssDmnHotLk;     /* protects the domain hot list */
    ssHotHdrT  ssDmnHotHdr;    /* header for hot list */
    int        ssDmnHotWorking;  /* TRUE,FALSE indicates that a thread is 
                                  * processing this domain's hot file.  Other 
                                  * threads must not also process this same 
                                  * hot file 
                                  */
    int        ssDmnHotPlaced;  /* TRUE,FALSE indicates that a hot file migrate 
                                 * is pending for domain, Prevents monitor from 
                                 * sending multiple hot files to vds in the same
                                 * domain, thus not getting true balance.
                                 */
    uint16_t    ssDmnHotCurrDay;  /* current index into ssHotIOCnt[7] - by day of week */

    /* UI visable configurable options */
    uint16_t    ssDmnDefragment; /* defragment active enable/disable */
    uint16_t    ssDmnDefragAll;  /* defragment all enable/disable */
    uint16_t    ssDmnSmartPlace; /* IO load balance enable/disable */
    uint16_t    ssDmnBalance;    /* volume free space balance enable/disable */
    uint16_t    ssDmnVerbosity;  /* enable/disable comments to evm */
    uint16_t    ssDmnDirectIo;  /* vfast on files with directio allowed? */
    uint16_t    ssMaxPercentOfIoWhenBusy; /* % of IO vfast is allowed to 
                                          * use when system is busy 
                                          */
    /* output variables */
    uint64_t    ssFilesDefraged; /* count of files vfast has defragmented */
    
    uint64_t    ssFobsDefraged;    /* count of fobs vfast has defragmented */
    uint64_t    ssFobsBalanced;    /* count of fobs vfast has 
                                * moved to other vols for free space balance 
                                */
    uint64_t    ssFilesIOBal; /* count of files vfast has 
                              * moved to support IO load bal
                              */
    uint64_t    ssExtentsConsol; /* count of extents vfast has 
                                 * combined for file defragmentation 
                                 */
    uint64_t    ssPagesConsol ;/* count of pages packed in support 
                               * of free space consolidation 
                               */
    /* UI hidden configurable options */
    uint16_t    ssAccessThreshHits; /* count of hits required on a file 
                                    * before the hot list enry is updated. 
                                    */
    uint16_t    ssSteadyState; /* minutes before IO is stable enough so 
                               * IO load balancing can be performed.
                               */
    uint16_t    ssMinutesInHotList; /* length of time file must be in hot 
                                    * list before moving. Also length of 
                                    * time since file was last updated, 
                                    * else file is cold 
                                    */
    uint32_t    ssReserved1;  
    uint32_t    ssReserved2;
    uint64_t    ssReserved3;
    uint64_t    ssReserved4;
} ssDmnInfoT;

typedef struct ssVolInfo {
    /* Temporarily move this in order to avoid PA alignment issues */
#ifdef _KERNEL
    spin_t     ssVdMsgLk;    /* locks ssVdMsgState field */
#endif
    mutexT     ssVdMigLk; /* locks the following 5 fields */
    ssMigOpT   ssVdMigState;  /* state the migrate is in for this volume */
    cv_t        ssContMigCV;  /* cv used to manage single thd/volume migrates */
    int        ssVdSSThdCnt;  /* current number of vfast threads in vd */
    int        ssVdSSWaiters; /* number of waiters on ssVdSSThdCnt */
    cv_t       ssVdSSWaitersCV; /* Cond Var for ssVdSSWaiters */

    ssMsgOpT   ssVdMsgState; /* state of the vd with regard to work messages 
                              * protected by ssVdMsgLk */

    ssPackHdrT ssPackHdr;  /* not-locked, single work thread access */
    mutexT     ssFragLk;   /* protects frag list from collisions by 
                            * work and list thds */
    ssFragHdrT ssFragHdr;  /* list of ssFragLLT structs */
    bfTagT     ssVdHotTag; /* tag of file to move to this volume 
                            * in support of hot files */
    bfSetIdT   ssVdHotBfSetId; /* Settag of file to move to this volume 
                                * in support of hot files. */
    uint16_t    ssIOCnt;
    uint64_t    ssLastIOCnt; /* last IO value since last lull check */
} ssVolInfoT;

/* UI structures */
typedef enum {
    SS_NOOP,
    SS_UI_RESTART,
    SS_UI_STOP,
    SS_UI_ACTIVATE,
    SS_UI_DEACTIVATE,
    SS_UI_SUSPEND,
    SS_UI_STATUS,
    SS_UI_SHOWHIDDEN,
    SS_UI_RESET
} ssUIDmnOpsT;


#ifdef _KERNEL

/****** global prototypes ******/

/* forward declarations */
struct bfAccess;
struct domain;
struct vd;
struct extent_blk_desc;

statusT
ss_open_file(
    bfSetIdT  bfSetId,
    bfTagT    filetag,
    bfSetT    **retbfSetp,
    struct    bfAccess **retbfap,
    int32_t   *fsetMounted );

statusT
ss_close_file(
    bfSetT    *bfSetp,
    struct    bfAccess *bfap,
    int32_t   fsetmounted );

void
ss_rmvol_from_hotlst(
    struct   domain *dmnP,
    vdIndexT delVdi );

void
ss_startios(
    struct vd *vdp );

statusT
advfs_ss_change_state(
    char *domain_name,
    ssDmnOpT state,
    ssDmnOpT *oldState,
    int32_t  write_out_record );

statusT
ss_put_rec(
    struct domain *dmnP ) ;

statusT
ss_chk_fragratio (
    struct bfAccess *bfap );

void
ss_snd_hot (
    ssListMsgTypeT msgType,
    struct         bfAccess *bfap );

void
ss_dmn_activate(
    struct domain *dmnP );

void
ss_dealloc_dmn(
    struct domain *dmnP );

void
ss_kern_stop();

statusT
ss_kern_start();

void
ss_kern_init();

void
ss_init_vd(
    struct vd *vdp );

void
ss_dealloc_vd(
    struct vd *vdp );

void
ss_dmn_deactivate(
    struct  domain *dmnP,
    int     flag );

void
ss_dealloc_pack_list(
    struct vd *vdp );

statusT
ss_block_and_wait(
    struct vd *vdp );

void
ss_set_fragmented_tag_page(
    bfTagT  tag,
    bfSetT *bfSetp );

int
ss_bfap_fragmented(
    struct bfAccess *bfap );

int ss_xtnt_counter(
    struct bfAccess *bfap );




/****** local module prototypes ******/
static
int
ss_chk_hot_list(
    struct   domain *dmnP,
    bfTagT   tag,
    bfSetIdT bfSetId,
    vdIndexT vdIndex,
    statusT  sts,
    int32_t  flag );

static
statusT
ss_find_space(
    struct      vd *vdp,
    bf_vd_blk_t requestedBlkCnt,
    bf_vd_blk_t *allocBlkOffp,
    bf_vd_blk_t *alloc_fob_cnt,
    int32_t     flags );

static
statusT
ss_get_n_lk_free_space(
    struct      vd *vdp,
    bf_fob_t    reqPageCnt,
    bf_vd_blk_t *allocBlkOffp,
    bf_fob_t    *alloc_fob_cnt,
    int32_t     alloc_hint );

static
statusT
ss_find_hot_target_vd(
    struct   domain  *dmnP,
    vdIndexT *targVdIndex,
    bfTagT   *tag,
    bfSetIdT *setId );

static
statusT
ss_do_periodic_tasks(
    ssPeriodicMsgTypeT task );

static
int
ss_find_target_vd(
    struct bfAccess *bfap );

static
statusT
ss_vd_migrate(
    bfTagT         filetag,
    bfSetIdT       bfSetId,
    vdIndexT       srcVdIndex,
    ssWorkMsgTypeT msgType,
    uint32_t       xtntCnt,
    int32_t        alloc_hint,
    int32_t        hotfile );

static
void
ss_get_most_xtnts(
    struct            bfAccess *bfap,      
    struct            extent_blk_desc *headBd,
    bf_fob_t          req_fob_cnt,     
    struct            extent_blk_desc *startBd,
    bf_fob_t          *fobs_to_migrate,
    uint64_t          *extent_cnt );

static
statusT
ss_get_vd_most_free(
    struct bfAccess *bfap,
    vdIndexT *newVdIndex );

static
ssFragLLT *
ss_select_frag_file(
    struct vd *vdp );

static
void 
ss_adj_msgs_flow_rate();

static
void
ss_boss_thread( void );

static
void
ss_monitor_thread ( void );

static
void
ss_work_thd_pool ( void );

static
void
ss_list_thd_pool ( void );

static
void
ss_copy_rec_from_disk(
    bsSSDmnAttrT *ssAttr, 
    struct       domain *dmnP ); 

static
void
ss_copy_rec_to_disk(
    struct       domain *dmnP, 
    bsSSDmnAttrT *ssAttr );

static
void
print_ssDmnInfo(
    struct domain *dmnP );

static
void
print_ssVolInfo(
    struct vd *vdp );

static
void
ss_insert_frag_onto_list(
    vdIndexT vdi,
    bfTagT tag,
    bfSetIdT bfSetId,
    uint64_t fragRatio, /* primary sort in list */
    uint32_t xtntCnt );  /* secondary sort in list */

static
void
ss_dealloc_frag_list(
    struct vd *vdp );

static
void
ss_boss_init( void );

static
void
ss_init_dmnInfo(
    struct domain *dmnP );

static
void
ss_queues_create( void );

static
void
ss_dealloc_hot(
    struct domain *dmnP );

static
void
ss_insert_hot_list(
    ssListMsgTypeT msgType,
    ssHotLLT *hp );

static
void
ss_del_from_hot_list(
    ssHotLLT *hp );

static
void
ss_delete_from_frag_list(
    struct   vd *vdp,
    bfTagT   tag,
    bfSetIdT bfSetId );

static void
ss_move_file(
    vdIndexT       vdi,
    bfDomainIdT    domainId,
    ssWorkMsgTypeT msgType );

/*
 * autotune stats
 */
#ifdef ADVFS_SMP_ASSERT
#define ADVFS_VFAST_STATS
#define SS_DEBUG
extern int ss_debug;
  /* level of output 0 - none 
   *                 1 - minimal information and error conditions
   *                 2 - detailed information and error conditions
   */

#endif


#ifdef ADVFS_VFAST_STATS
struct advfs_vfast_stats_struct {
    uint32_t ss_kern_start_init_err;
    uint32_t ss_kern_stop_not_yet_inited;
    uint32_t ss_queues_create_msgq_err;
    uint32_t ss_queues_create_listq_err;
    uint32_t ss_queues_create_workq_err;
    uint32_t ss_boss_init_err;
    uint32_t ss_boss_thread_init_err;
    uint32_t ss_boss_thread_worker_init_err;
    uint32_t ss_boss_thread_monitor_init_err;
    uint32_t ss_boss_thread_count_err;
    uint32_t ss_adj_msgs_flow_rate_no_dmn;
    uint32_t ss_do_periodic_tasks_no_dmn;
    uint32_t ss_dmn_activate_put_rec_err;
    uint32_t ss_dmn_activate_get_rec_err;
    uint32_t ss_change_state_dmn_access_err;
    uint32_t ss_change_state_state_err;
    uint32_t ss_change_state_put_rec_err;
    uint32_t ss_put_rec_bad_vdi;
    uint32_t ss_put_rec_bad_ftx;
    uint32_t ss_put_rec_bad_bmtr_put_rec;
    uint32_t ss_chk_fragratio_dio_err;
    uint32_t ss_chk_fragratio_bfap_closed_with_err;
    uint32_t ss_chk_fragratio_sanity_err;
    uint32_t ss_chk_fragratio_lock_xtnt_map_err;
    uint32_t ss_chk_fragratio_vd_ok;
    uint32_t ss_chk_fragratio_vd_not_in_svcclass;
    uint32_t ss_insert_frag_onto_list_rbf_bfs_open_err;
    uint32_t ss_insert_frag_onto_list_fileset_not_acc;
    uint32_t ss_insert_frag_onto_list_sanity_err;
    uint32_t ss_insert_frag_onto_list_rmvol_in_progress;
    uint32_t ss_insert_frag_onto_list_malloc_err;
    uint32_t ss_delete_from_frag_list_cnt_zero;
    uint32_t ss_select_frag_file_list_cnt_zero;
    uint32_t ss_dealloc_frag_list_cnt_zero;
    uint32_t ss_print_frag_list_list_cnt_zero;
    uint32_t ss_dealloc_pack_list_cnt_zero;
    uint32_t ss_print_pack_list_cnt_zero;
    uint32_t ss_snd_hot_dio;
    uint32_t ss_insert_hot_list_bfs_valid_err;
    uint32_t ss_insert_hot_list_rbf_bfs_open_err;
    uint32_t ss_insert_hot_list_sanity_err;
    uint32_t ss_rmvol_from_hotlist_list_zero;
    uint32_t ss_del_from_hot_list_bfset_invalid;
    uint32_t ss_del_from_hot_list_rbf_bfs_open_err;
    uint32_t ss_chk_hot_list_cnt_zero;
    uint32_t ss_chk_hot_list_not_active_hot_io;
    uint32_t ss_chk_hot_list_no_entry;
    uint32_t ss_dealloc_hot_list_cnt_zero;
    uint32_t ss_block_and_wait_no_block;
    uint32_t ss_block_and_wait_would_block;
    uint32_t ss_block_and_wait_no_block_2;
    uint32_t ss_open_file_rbf_bfs_open_err;
    uint32_t ss_open_file_bfset_invalid;
    uint32_t ss_open_file_bf_get_err;
    uint32_t ss_open_file_bs_access_err;
    uint32_t ss_move_file_dmn_not_active;
    uint32_t ss_move_file_src_vd_not_valid;
    uint32_t ss_move_file_would_block;
    uint32_t ss_move_file_rmvol_inprog;
    uint32_t ss_find_space_no_space;
    uint32_t ss_find_space_sbm_lock_range_err;
    uint32_t ss_get_n_lk_free_space_ss_find_space_err;
    uint32_t ss_get_n_lk_free_space_ss_find_space_err2;
    uint32_t ss_get_n_lk_free_space_mig_pack_vd_range_err;
    uint32_t ss_get_n_lk_free_space_ss_find_space_err3;
    uint32_t ss_get_n_lk_free_space_enospc;
    uint32_t ss_get_n_lk_free_space_sbm_lock_range_err;
    uint32_t ss_get_n_lk_free_space_ss_find_space_err4;
    uint32_t ss_get_vd_most_free_lock_xtnt_err;
    uint32_t ss_get_vd_most_free_not_enough_xtnts;
    uint32_t ss_get_vd_most_free_eno_more_blocks;
    uint32_t ss_vd_migrate_ss_open_file_err;
    uint32_t ss_vd_migrate_ignore_dio;
    uint32_t ss_vd_migrate_get_vd_most_free_err;
    uint32_t ss_vd_migrate_bad_vdi;
    uint32_t ss_vd_migrate_err;
    uint32_t ss_vd_migrate_lock_xtnt_err;
    uint32_t ss_vd_migrate_no_stg_to_mig;
    uint32_t ss_vd_migrate_get_n_lk_free_space_err;
    uint32_t ss_vd_migrate_no_free_space_found_or_created;
    uint32_t ss_vd_migrate_lock_xtnt_map_err;
    uint32_t ss_vd_migrate_no_space;
    uint32_t ss_vd_migrate_get_blkmap_err;
    uint32_t ss_vd_migrate_enospc2;
    uint32_t ss_vd_migrate_bs_migrate_err;
    uint32_t ss_vd_migrate_move_metadata_err;
    uint32_t ss_vd_migrate_close_file_err;
    uint32_t ss_vd_migrate_block_and_wait_err;
    uint32_t ss_vd_migrate_open_file_err;
    uint32_t ss_vd_migrate_open_file_err2;
    uint32_t ss_blks_on_vd_lock_xtnt_err;
    uint32_t ss_find_target_vd_err;
    uint32_t ss_find_target_vd_enomem;
    uint32_t ss_find_target_vd_info_not_out_of_balance;
    uint32_t ss_find_target_vd_info_not_out_of_balance2;
    uint32_t ss_find_hot_target_vd_err;
    uint32_t ss_find_hot_target_vd_enomem;
    uint32_t mig_pack_vd_range_bmt_get_vd_bf_inway_err;
    uint32_t mig_pack_vd_range_no_more_mcells;
    uint32_t mig_pack_vd_range_del_find_del_entry_err;
    uint32_t mig_pack_vd_range_del_find_del_entry_err2;
    uint32_t mig_pack_vd_range_ss_open_file_err;
    uint32_t mig_pack_vd_range_range_not_mapped;
    uint32_t mig_pack_vd_range_ss_close_file_err;
    uint32_t mig_pack_vd_range_ss_close_file_err2;
    uint32_t mig_pack_vd_range_ss_block_and_wait_err;
    uint32_t mig_pack_vd_range_no_more_mcells2;
    uint32_t mig_pack_vd_range_ss_open_file_err2;
    uint32_t mig_pack_vd_range_small_file_force_vmpagesz;
    uint32_t mig_pack_vd_range_temp_migrate;
    uint32_t mig_pack_vd_range_temp_migrate_nospace;
    uint32_t mig_pack_vd_range_temp_migrate_normal_err;
    uint32_t mig_pack_vd_range_temp_migrate_normal_err2;
    uint32_t mig_pack_vd_range_sbm_scan_err;
    uint32_t mig_pack_vd_range_sbm_scan_err2;
    uint32_t mig_pack_vd_range_space_stolen;
    uint32_t mig_pack_vd_range_space_stolen2;
    uint32_t mig_pack_vd_range_temp_migrate_nolock;
    uint32_t ss_vd_migrate_xtnt_map_err;
    uint32_t ss_vd_migrate_frag_unexpected_mismatch;
    uint32_t ss_grow_tag_array_cnt;
};

typedef struct advfs_vfast_stats_struct advfs_vfast_stats_t;

extern advfs_vfast_stats_t advfs_vfast_stats;
#define VFAST_STATS( __stat_name ) advfs_vfast_stats.__stat_name++
#else
#define VFAST_STATS( __stat_name )
#endif /* ADVFS_VFAST_STATS */

#endif /* _KERNEL */

#endif /* _SS_H_ */
