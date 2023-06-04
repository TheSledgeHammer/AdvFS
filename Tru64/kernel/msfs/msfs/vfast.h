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
 * @(#)$RCSfile: vfast.h,v $ $Revision: 1.1.3.1 $ (DEC) $Date: 2001/12/14 16:51:12 $
 */

#ifndef _SS_H_
#define _SS_H_

/* include header files */
#include <msfs/bs_ods.h>
#include <msfs/bs_msg_queue.h>

#define GETCURRWDAY(tv_sec, newday)                    \
{                                                      \
    /* The epoch was on a Thursday.  Jan 1, 1900 was   \
     * on a Monday.  This will not be correct if YRREF \
     * changes!                                        \
     */                                                \
    MS_SMP_ASSERT(YRREF == 1970);                      \
    newday = (tv_sec / SECDAY + 4) % 7;                \
}

/*
 * Macros for the vfast SS_is_running lock.
 */
extern lock_data_t SSLock;
#define SS_WRITE_LOCK(ssl) \
    lock_write( ssl );

#define SS_UNLOCK(ssl) \
    lock_done( ssl );

/* Lock initialization for SSLock. */
#define SMARTSTORE_LOCK_INIT(ssl) \
    lock_setup( ssl, ADV_SSLock_lk_info, TRUE );

/* Lock destruction for SSLock. */
#define SMARTSTORE_LOCK_DESTROY(ssl) \
    lock_terminate( ssl );

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
#define SS_PAGECNT       4  /* number of pages to migrate for each work cycle */

/* monitor thread constants */
#define SS_VD_SAMPLE_INTERVAL    1
#define SS_HOT_LIST_CHK_REVS  600 /* interval to check hot list (in secs) */
#define SS_MSG_CHK_REVS       10 /* interval to check hot list msg input, secs*/
#define SS_LULL_CHK_REVS      40 /* interval to check for no IO. seconds */
#define SS_FRAG_CHK_REVS      300 /* interval to check for fragmented frag file */

/* messages rate constants */
#define SS_MSGS_PER_SEC_GOAL   20 /* desired msg rate is adjusted here */
#define SS_MAX_MSGS_ALLOWED  (SS_MSGS_PER_SEC_GOAL*(SS_VD_SAMPLE_INTERVAL*SS_MSG_CHK_REVS))
#define SS_MAX_MSGS_IN_WORKQ   2  /* Maximum msgs allowed in work queue before adding */
                                  /* another thread */

/* other constants */
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
                 ((ulong) ((ulong)pageCnt*32L) / (ulong) xtntCnt) 

/* thread pool stuff */

typedef struct tpool {
     mutexT       plock;     /* locks next field */
     int          threadCnt; /* thread count for this pool */
     int          shutdown;  /* flag used in vfast shutdown */
} tpoolT;

/* vfast messaging stuff */

typedef enum {
    SS_HOT_LIST_CHK, /* check hot list for file to move */
    SS_LULL_CHK, /* send off a ok to run msg to vds that are bored */
    SS_FRAG_CHK  /* check frag file for need to defrag */
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
    uint64T fragRatio; /* primary sort in list */
    uint32T xtntCnt;   /* secondary sort in list */
} ssFragAddMsgT;

typedef struct {
    vdIndexT vdi;      /* primary mcell vdi */
    uint32T  cnt[SS_HOT_TRACKING_DAYS];      /* counter */
    uint64T  size;     /* file size */
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
                      /* whenever ss first migrates this file, also an error
                     /* can go here. */
    int ssHotVdi;     /* current vd of primary mcell;
                       * if ssHotPlaced, should be same */
    bfTagT   ssHotTag;     /* tag of file */
    bfSetIdT ssHotBfSetId; /* settag of file */
    uint32T  ssHotIOCnt[7]; /* cnt of IOs on this file by day of week */
    uint64T  ssHotFileSize;/* size of file */
    time_t   ssHotLastUpdTime; /* last time one of the Cnts above was updated */
    time_t   ssHotEntryTime; /* time entry was placed on list */
    time_t   ssHotErrTime; /* time entry tried and failed to place */
} ssHotLLT;

typedef struct ssHotHdr {      
    struct ssHotEntry *ssHotFwd; /* must stay in first position */
    struct ssHotEntry *ssHotBwd; /* must stay in second position */
    uint16T        ssHotListCnt; /* number of hot list entries for this dmn */
} ssHotHdrT;

typedef struct ssFragEntry {
    struct ssFragEntry  *ssFragRatFwd;  /* must stay in first position */
    struct ssFragEntry  *ssFragRatBwd;  /* must stay in second position */
    uint32T  ssFragRatio;
    uint32T  ssXtntCnt;
    bfTagT   ssFragTag;
    bfSetIdT ssFragBfSetId;
} ssFragLLT;

typedef struct ssFragHdr {
    struct ssFragEntry *ssFragRatFwd;  /* must stay in first position */
    struct ssFragEntry *ssFragRatBwd;  /* must stay in second position */
    uint32T             ssFragListCnt;
    uint64T             ssFragHdrThresh;
} ssFragHdrT;

typedef struct ssPackEntry {
    struct ssPackEntry  *ssPackFwd;
    struct ssPackEntry  *ssPackBwd;
    bfTagT               ssPackTag;
    bfSetIdT             ssPackBfSetId;
    uint32T        ssPackPageOffset; 
    uint32T        ssPackPageCnt; 
    uint64T        ssPackStartXtBlock; /* block at ssPackPageOffset */
    uint64T        ssPackEndXtBlock; /* block at PackPageOffset + PackPageCnt */
} ssPackLLT;

typedef struct ssPackHdr {
    ssPackLLT       *ssPackFwd;
    ssPackLLT       *ssPackBwd;
    uint32T          ssPackListCnt;
    uint64T          ssPackZoneStartBlk;
    uint64T          ssPackZoneEndBlk;
    uint32T          ssPackLastBMTPage;
} ssPackHdrT;

typedef struct ssDmnInfo {
    mutexT     ssDmnLk; /* protects ssDmnHotWorking, Msg Counts,
                         * output variables, ssDmnSSThdCnt, ssDmnSSWaiters
                         */
    int        ssDmnSSThdCnt;  /* current number of vfast threads in dmn */
    int        ssDmnSSWaiters; /* number of waiters on ssDmnSSThdCnt */
    ssDmnOpT   ssDmnState;  /* processing state that domain is in */
    /* message flow rate to lists stats */
    uint32T    ssDmnListMsgSendCnt; /* cnt of list msgs send attempts */
    uint32T    ssDmnListMsgDropCnt;       /* cnt of list msgs dropped */

    /* internal used params */
    time_t     ssFirstMountTime;   /* time domain first had a WRITE mount */
    int        ssPctIOs;       /* pct of ios allowed to ss when IO queues busy */
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
    uint16T    ssDmnHotCurrDay;  /* current index into ssHotIOCnt[7] - by day of week */

    /* UI visable configurable options */
    uint16T    ssDmnDefragment; /* defragmenting enable/disable */
    uint16T    ssDmnSmartPlace; /* IO load balance enable/disable */
    uint16T    ssDmnBalance;    /* volume free space balance enable/disable */
    uint16T    ssDmnVerbosity;  /* enable/disable comments to evm */
    uint16T    ssDmnDirectIo;  /* vfast on files with directio allowed? */
    uint16T    ssMaxPercentOfIoWhenBusy; /* % of IO vfast is allowed to 
                                          * use when system is busy 
                                          */
    /* output variables */
    uint32T    ssFilesDefraged; /* count of files vfast has defragmented */
    uint32T    ssPagesDefraged; /* count of pages vfast has defragmented */
    uint32T    ssPagesBalanced;/* count of pages vfast has 
                                * moved to other vols for free space balance 
                                */
    uint32T    ssFilesIOBal; /* count of files vfast has 
                              * moved to support IO load bal
                              */
    uint32T    ssExtentsConsol; /* count of extents vfast has 
                                 * combined for file defragmentation 
                                 */
    uint32T    ssPagesConsol ;/* count of pages packed in support 
                               * of free space consolidation 
                               */
    /* UI hidden configurable options */
    uint16T    ssAccessThreshHits; /* count of hits required on a file 
                                    * before the hot list enry is updated. 
                                    */
    uint16T    ssSteadyState; /* minutes before IO is stable enough so 
                               * IO load balancing can be performed.
                               */
    uint16T    ssMinutesInHotList; /* length of time file must be in hot 
                                    * list before moving. Also length of 
                                    * time since file was last updated, 
                                    * else file is cold 
                                    */
    uint16T    ssReserved0;
    uint32T    ssReserved1;  
    uint32T    ssReserved2;
} ssDmnInfoT;

typedef struct ssVolInfo {
    mutexT     ssVdMigLk; /* locks the following 5 fields */
    ssMigOpT   ssVdMigState;  /* state the migrate is in for this volume */
    cvT        ssContMig_cv;  /* cv used to manage single thd/volume migrates */
    int        ssVdSSThdCnt;  /* current number of vfast threads in vd */
    int        ssVdSSWaiters; /* number of waiters on ssVdSSThdCnt */

    mutexT     ssVdMsgLk;     /* locks the following 1 field */
    ssMsgOpT   ssVdMsgState;  /* state of the vd with regard to work messages */

    ssPackHdrT ssPackHdr;  /* not-locked, single work thread access */
    mutexT     ssFragLk;   /* protects frag list from collisions by 
                            * work and list thds */
    ssFragHdrT ssFragHdr;  /* list of ssFragLLT structs */
    bfTagT     ssVdHotTag; /* tag of file to move to this volume 
                            * in support of hot files */
    bfSetIdT   ssVdHotBfSetId; /* Settag of file to move to this volume 
                                * in support of hot files. */
    uint16T    ssIOCnt;
    uint64T    ssLastIOCnt; /* last IO value since last lull check */
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
    SS_UI_SHOWHIDDEN
} ssUIDmnOpsT;

#endif /* _SS_H_ */
