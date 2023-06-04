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
 *    vfast.c
 *    Primary vfast(aka smartstore) functionality.
 *
 * Date:
 *
 *    December 11, 2001
 */


#define ADVFS_MODULE VFAST

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <tcr/dlm.h>
#include <machine/sys/save_state.h>
#include <machine/sys/clock.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#include <fs_dir_routines.h>
#include <ms_public.h>
#include <ftx_public.h>
#include <fs_dir.h>
#include <fs_quota.h>
#include <ms_privates.h>
#include <bs_access.h>
#include <bs_domain.h>
#include <ms_osf.h>
#include <vfast.h>
#include <bs_extents.h>
#include <bs_inmem_map.h>
#include <bs_vd.h>
#include <bs_stg.h>
#include <bs_migrate.h>
#include <bs_msg_queue.h>
#include <tcr/clu.h>

msgQHT ssBossQH;       /* bosses msg queue for adding threads */
msgQHT ssListQH;       /* list processing msg queue */
msgQHT ssWorkQH;       /* ss work msg queue */
static tpoolT ssListTpool; /* pool of list worker threads */
static tpoolT ssWorkTpool; /* pool of ss general worker threads */

mutexT SSLock;      /* mutex lock for SS_is_running */

#ifndef REPLICATED
#define REPLICATED const
#endif

mutexT ssStoppedMutex; /* global simple lock for ssStoppedCV */
cv_t   ssStoppedCV;    /* condition variable used when shutting ss system down */

#define _ REPLICATED

/* 64-byte-aligned section of vfast mostly read data */
/* For PA read only variables cannot be modified, hence the removal of the const
 * declaration for SS_is_inited and SS_is_running.  This should be revisited once
 * hpux supports replicated data for better NUMA support.
 */
uint32_t   SS_is_inited          = FALSE;                     /* rarely read, once written */
uint32_t   SS_is_running         = FALSE;                     /* mostly read */
_ bf_fob_t SSFobCnt              = SS_FOBCNT;                 /* Number of 1k units to migrate
                                                               * in a work cycle */
_ uint32_t SS_pct_msgq_more_thds = SS_INIT_PCT_MSGQ_MORE_THDS;/* const */
_ uint32_t SS_max_list_thds      = SS_MAX_LIST_THDS;          /* const */
_ uint32_t SS_max_work_thds      = SS_MAX_WORK_THDS;          /* const */
_ uint32_t SSHotLstCrossPct      = SS_HOT_LST_CROSS_PCT;      /* const */
_ uint32_t SSPackRangePctFree    = 100/SS_PACK_RANGE_PCT_FREE;     /* const */
_ uint32_t SSPackRangePctMaxSize = 100/SS_PACK_RANGE_PCT_MAX_SIZE; /* const */
_ uint32_t SSBalanceGoal         = SS_BALANCE_GOAL;            /* const */
_ uint32_t SSHotPenaltyTime      = SS_INIT_HOT_ERROR_PENALTY;  /* const */
_ uint32_t SS_const_data_pad_1[4] = {0};

typedef enum {
    SS_ADD_LIST_THREAD,
    SS_ADD_WORK_THREAD,
    SS_SHUTDOWN,
    SS_POOL_STOPPED,
    SS_MONITOR_STOPPED,
    SS_ADJ_MSG_RATE 
} ssBossMsgTypeT;

typedef struct {
    ssBossMsgTypeT msgType;   /* msg type for future expansion  */
} ssBossMsgT;

typedef struct {
    uint64_t    volHotIO;
    bf_vd_blk_t volFreeBlks;
    vdIndexT    vdi;
} volDataT;

/*
 * Macros for tracking unique vfast structure conditions
 */
#ifdef ADVFS_VFAST_STATS
advfs_vfast_stats_t advfs_vfast_stats;
#endif
#ifdef SS_DEBUG
int ss_debug=0;
#endif

/*
 * local prototypes
 */
void
ss_grow_tag_array(bfSetT *, size_t);

int
ss_setup_fragmented_tags(bfSetT         *bfSetp,
                         bs_meta_page_t  tagPg,
                         uint32_t       *page_throttle,
                         uint32_t       *msg_throttle);

/*********************************************************************
 *
 * This routine initializes global data structures for vfast,
 * including vfast queues.  Does not initialize the thread pools.
 *
 ********************************************************************/
void
ss_kern_init()
{
    int32_t false=FALSE,
            true =TRUE;

    ADVMTX_SMARTSTOREGBL_INIT( &ssStoppedMutex );

    (void) cv_init( &ssStoppedCV, "ssStopped CV", NULL, CV_WAITOK);

    ADVMTX_SS_INIT(&SSLock);
    ADVMTX_SS_LOCK(&SSLock );
    write_const_data(&false, &SS_is_running, sizeof(int));
    write_const_data(&true,  &SS_is_inited,  sizeof(int));
    ADVMTX_SS_UNLOCK(&SSLock );

    /* Create the vfast thread queues.
     * These live until sys halt.
     */
    ss_queues_create();
    (void) ss_kern_start();

#ifdef ADVFS_VFAST_STATS
    bzero( &advfs_vfast_stats, sizeof( advfs_vfast_stats_t ) );
#endif

    return;
}

/*********************************************************************
 *
 * This routine starts the vfast thread pools.  Called when
 * system is booted via "../init.d/vfast restart".
 *
 ********************************************************************/
statusT
ss_kern_start()
{
    if(SS_is_inited == FALSE) {
        /* Advfs has not been initialized yet! */
        /* User needs to mount at least one Advfs fileset */
        VFAST_STATS( ss_kern_start_init_err );
        return E_DOMAIN_NOT_ACTIVATED;
    }

    ADVMTX_SS_LOCK(&SSLock );
    if(SS_is_running == FALSE) {
        ss_boss_init();  /* starts a thread to set SS_is_running = TRUE */
    }
    ADVMTX_SS_UNLOCK(&SSLock );
    return EOK;
}

/*********************************************************************
 *
 * This routine stops the vfast thread pools.  Called when
 * system is shutdown via "../init.d/vfast stop".
 *
 ********************************************************************/
void
ss_kern_stop()
{
    ssBossMsgT *msg;
    struct fileSetNode *fsp = NULL;
    ssHotHdrT *hhp;   /* pointer to hot list header */
    vdT* vdp;
    int32_t vdi=0, 
            vdCnt=0,
            false=FALSE,
            ss_map_locked = FALSE;

    if(SS_is_inited == FALSE) {
        /* Advfs has not been initialized yet! */
        /* User needs to mount at least one Advfs fileset */
        VFAST_STATS( ss_kern_stop_not_yet_inited );
        return ;
    }

    ADVMTX_SS_LOCK(&SSLock );
    ss_map_locked = TRUE;
    if(SS_is_running == TRUE) {

        /* stop messages from being placed on queues */
        write_const_data(&false, &SS_is_running, sizeof(int));
        ADVMTX_SS_UNLOCK(&SSLock );
        ss_map_locked = FALSE;

        /* Notify any threads working on files or waiting to work 
         * on a file of shutdown.  
         */
        ADVRWL_FILESETLIST_READ(&FilesetLock );
        for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
            for ( vdi = 1; vdCnt < fsp->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, fsp->dmnP, TRUE, FALSE)) ) {
                    continue;
                }

                cv_broadcast (&vdp->ssVolInfo.ssContMigCV, NULL, CV_NULL_LOCK);

                vdCnt++;
                vd_dec_refcnt( vdp );
            }
        }
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

        /* send message to boss thread to shutdown, then wait 
         * for it to do so.
         */
        if((msg = (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
            msg->msgType = SS_SHUTDOWN;
            msgq_send_msg(ssBossQH, msg);
        }

        ADVMTX_SMARTSTOREGBL_LOCK(&ssStoppedMutex );
        cv_wait( &ssStoppedCV, &ssStoppedMutex, CV_MUTEX, CV_NO_RELOCK );
    }

    /* Now free the vfast lists */
    ADVRWL_FILESETLIST_READ(&FilesetLock );
    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
        ss_dealloc_hot(fsp->dmnP);
        for (vdi=1; vdCnt<fsp->dmnP->vdCnt && vdi<=BS_MAX_VDI; vdi++) {
            if ( !(vdp = vd_htop_if_valid(vdi, fsp->dmnP, TRUE, FALSE)) )
                continue;
            ss_dealloc_frag_list(vdp);
            vd_dec_refcnt(vdp);
            vdCnt++;
        }
    }
    ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );


    if(ss_map_locked == TRUE)
        ADVMTX_SS_UNLOCK(&SSLock );
    return;
}

/*********************************************************************
 *
 * This routine creates the vfast thread message queues. Done at
 * boot as part of kernel initialization of Advfs.
 *
 ********************************************************************/

static void
ss_queues_create(void)
{
    statusT sts;

    /* Create a message queue to send messages to the boss thread.  */
    sts = msgq_create(&ssBossQH,           /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,   /* max messages in queue */
                      (int)sizeof( ssBossMsgT ),/* max size of each msg */
                      TRUE,           /* q size growth is allowed */
                                      /* can't miss shutdown msgs */
                      current_rad_id()     /* RAD to create on */
                     );
    if (sts != EOK) {
        ms_printf("AdvFS vfast boss msg Q was not spawned.\n");
        VFAST_STATS( ss_queues_create_msgq_err );
        return;
    }

    /* Create a message queue to send messages to the list 
     * worker thread pool.  
     */
    sts = msgq_create( &ssListQH,          /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,   /* max messages in queue */
                      (int)sizeof( ssListMsgT ),/* max size of each msg */
                      FALSE,               /* q size is fixed */
                      current_rad_id()     /* RAD to create on */
                      );
    if (sts != EOK) {
        ms_printf("AdvFS vfast List msg Q was not spawned.\n");
        VFAST_STATS( ss_queues_create_listq_err );
        return;
    }

    /* Create a message queue to handle messages to the worker thread pool.  */
    sts = msgq_create(&ssWorkQH,         /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,/* max messages in queue */
                      (int)sizeof(ssWorkMsgT),/* max size of each msg */
                      FALSE,                  /* q size is fixed */
                      current_rad_id()       /* RAD to create on */
                      );
    if (sts != EOK) {
        ms_printf("AdvFS vfast Work msg Q was not spawned.\n");
        VFAST_STATS( ss_queues_create_workq_err );
        return;
    }
    return;
}


/*********************************************************************
 *
 * This routine initializes the mutexes for the thread pools.  It
 * also starts the main boss thread which in turn will create the
 * two thread pools for vfast.  It is called as part of 
 * "ss restart" from user mode, usually at boot time.
 *
 ********************************************************************/
static void
ss_boss_init( void )
{
    extern proc_t *Advfsd;
    tid_t tid;

    ADVMTX_SSLIST_TPOOL_INIT(&ssListTpool.plock);
    ADVMTX_SSLIST_WORKTPOOL_INIT(&ssWorkTpool.plock);

    /* Create and start the boss thread.  */
    if (kdaemon_thread_create((void (*)(void *))ss_boss_thread,
                              NULL,
                              Advfsd,
                              &tid,
                              KDAEMON_THREAD_CREATE_DETACHED)) {
        ms_printf("AdvFS list boss thread was not spawned.\n");
        VFAST_STATS( ss_boss_init_err );
        return;
    }
    return;
}


/*********************************************************************
 *
 * This routine creates the main boss thread which in turn creates
 * the thread pools for vfast.
 *
 ********************************************************************/

static void
ss_boss_thread( void )
{
    extern proc_t *Advfsd;
    tid_t tid;
    ssBossMsgT *bmsg;
    ssListMsgT *listmsg;
    ssWorkMsgT *workmsg;
    int32_t i = 0,
            monitor_stopped = TRUE,
            true =TRUE;

    /* Purge the message queues because vfast may have been 
     * running previously.  Even though we purge the messages on
     * shutdown, it is a good idea to do it again when bringing
     * the system up because messages may have snuck into the queue
     * as we were shutting down.
     */
    msgq_purge_msgs(ssBossQH);
    msgq_purge_msgs(ssWorkQH);
    msgq_purge_msgs(ssListQH);

    /* create list thread pool workers */
    ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
    ssListTpool.threadCnt =0;
    ssListTpool.shutdown  =FALSE;
    ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
    for(i=0; i< SS_INIT_LIST_THDS; i++) {
        if (kdaemon_thread_create((void (*)(void *))ss_list_thd_pool,
                                  NULL,
                                  Advfsd,
                                  &tid,
                                  KDAEMON_THREAD_CREATE_DETACHED)) {
            ms_printf("AdvFS list worker thread #(%d) was not spawned.\n",i);
            VFAST_STATS( ss_boss_thread_init_err );
            return;
        } else {
            ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
            ssListTpool.threadCnt++;
            ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
        }
    }

    /* create vfast work thread pool workers */
    ADVMTX_SSLIST_WORKTPOOL_LOCK(&ssWorkTpool.plock);
    ssWorkTpool.threadCnt =0;
    ssWorkTpool.shutdown  =FALSE;
    ADVMTX_SSLIST_WORKTPOOL_UNLOCK(&ssWorkTpool.plock);
    for(i=0; i< SS_INIT_WORK_THDS; i++) {
        if (kdaemon_thread_create((void (*)(void *))ss_work_thd_pool,
                                  NULL,
                                  Advfsd,
                                  &tid,
                                  KDAEMON_THREAD_CREATE_DETACHED)) {
            ms_printf("AdvFS worker thread number (%d) was not spawned.\n",i);
            VFAST_STATS( ss_boss_thread_worker_init_err );
            return;
        } else {
            ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
            ssWorkTpool.threadCnt++;
            ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
        }
    }

    /* Create and start the monitor thread.  */
    if (kdaemon_thread_create((void (*)(void *))ss_monitor_thread,
                              NULL,
                              Advfsd,
                              &tid,
                              KDAEMON_THREAD_CREATE_DETACHED)) {
        ms_printf("AdvFS vfast monitor thread was not spawned.\n");
        VFAST_STATS( ss_boss_thread_monitor_init_err );
        return;
    } else {
        monitor_stopped = FALSE;
    }

    if((ssWorkTpool.threadCnt) && (ssListTpool.threadCnt)) {
        ADVMTX_SS_LOCK(&SSLock );
        write_const_data(&true, &SS_is_running, sizeof(int));
        ADVMTX_SS_UNLOCK(&SSLock );
    } else {
        ms_printf("AdvFS vfast not started.\n");
        VFAST_STATS( ss_boss_thread_count_err );
        return;
    }

    /* now hang around and wait to manage the worker threads */
    while (TRUE) {
        /* Wait for something to do */
        bmsg = (ssBossMsgT *)msgq_recv_msg( ssBossQH );
        switch ( bmsg->msgType ) {
            case SS_ADD_LIST_THREAD:
                ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
                if(ssListTpool.threadCnt >= SS_max_list_thds) {
                    ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
                    break;
                }
                ssListTpool.threadCnt++;
                ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
                /* Can't hold lock over kernel_thread call */
                if (kdaemon_thread_create((void (*)(void *))ss_list_thd_pool,
                                          NULL,
                                          Advfsd,
                                          &tid,
                                          KDAEMON_THREAD_CREATE_DETACHED)) {
                    ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
                    ssListTpool.threadCnt--;
                    ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
                }
                break;
            case SS_ADD_WORK_THREAD:
                ADVMTX_SSLIST_WORKTPOOL_LOCK(&ssWorkTpool.plock);
                if(ssWorkTpool.threadCnt >= SS_max_work_thds) {
                    ADVMTX_SSLIST_WORKTPOOL_UNLOCK(&ssWorkTpool.plock);
                    break;
                }
                ssWorkTpool.threadCnt++;
                ADVMTX_SSLIST_WORKTPOOL_UNLOCK(&ssWorkTpool.plock);
                /* Can't hold lock over kernel_thread call */
                if (kdaemon_thread_create((void (*)(void *))ss_work_thd_pool,
                                          NULL,
                                          Advfsd,
                                          &tid,
                                          KDAEMON_THREAD_CREATE_DETACHED)) {
                    ADVMTX_SSLIST_WORKTPOOL_LOCK(&ssWorkTpool.plock);
                    ssWorkTpool.threadCnt--;
                    ADVMTX_SSLIST_WORKTPOOL_UNLOCK(&ssWorkTpool.plock);
                }
                break;
            case SS_SHUTDOWN:
                ssWorkTpool.shutdown = TRUE;
                ssListTpool.shutdown = TRUE;

                /* Send a msg to force one of the threads to detect 
                 * the shutdown. This is done just in case there are 
                 * no messages currently on the queue.  If there is 
                 * messages, threads will pick up the shutdown flag 
                 * the next time they begin to process the next msg.
                 */
                if((workmsg = 
                   (ssWorkMsgT *)msgq_alloc_msg(ssWorkQH)) != NULL) {
                    workmsg->msgType = SS_WORK_STOP;
                    msgq_send_msg(ssWorkQH, workmsg);
                }
                if((listmsg = 
                   (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
                    listmsg->msgType = SS_LIST_STOP;
                    msgq_send_msg(ssListQH, listmsg);
                }

                break;
            case SS_POOL_STOPPED:
            case SS_MONITOR_STOPPED:
                if( bmsg->msgType == SS_MONITOR_STOPPED ) {
                    monitor_stopped = TRUE;
                }

                /* check to see if I, the boss, should die */
                if((SS_is_running == FALSE) && 
                   (monitor_stopped == TRUE) && 
                   (ssWorkTpool.threadCnt == 0) && 
                   (ssListTpool.threadCnt == 0) &&
                   (ssWorkTpool.shutdown == TRUE) &&
                   (ssListTpool.shutdown == TRUE)) {

                    /* cleanup message queues */
                    msgq_purge_msgs(ssBossQH);
                    msgq_purge_msgs(ssListQH);
                    msgq_purge_msgs(ssWorkQH);

                    /* notify message sender that shutdown is completed */
                    cv_broadcast (&ssStoppedCV, NULL, CV_NULL_LOCK);

                    /* hari-kari */
                    (void) kdaemon_thread_exit();
                    /* NOT REACHED */
                }
                break;
            case SS_ADJ_MSG_RATE:
                ss_adj_msgs_flow_rate();
                break;
            default:
                MS_SMP_ASSERT(0);
                break;
        }

        msgq_free_msg( ssBossQH, bmsg );

    } /* end while processing msg queue */

    /* NOT REACHED */
}

/*********************************************************************
 * ss_monitor_thread
 * 
 * This single thread runs continuously while vfast is running. 
 * Each sleep loop it does three things:  
 * 1. Perform hot list msg queue throttle algorithm adjustments by
 *    sending a msg to the boss thread to tell it to adjust the hot 
 *    list frequency of update messages.
 * 2. Checks the message queues of the list and work thread pools to 
 *    see if more threads need to be created to handle the number of 
 *    msgs in the message queue.
 * 3. Periodically scan hot list to check IO load balance and see if
 *    any files need moved to support this functionality.
 *
 ********************************************************************/
static void
ss_monitor_thread (void)
{
    ssBossMsgT *bmsg;
    struct   fileSetNode *fsp = NULL;
    vdT*     vdp;
    uint64_t curBlkIO;  /* current IO count on volume */
    int32_t  vdi,
             msg_rev_cnt = 0,
             hot_chk_rev_cnt = 0, 
             zero_io_rev_cnt = 0,
             defragall_chk_rev_cnt = 0,
             short_q_depth = (SS_pct_msgq_more_thds * SS_INIT_MSGQ_SIZE)/100;
    statusT sts=EOK;

    while(TRUE) {

        delay(HZ * SS_VD_SAMPLE_INTERVAL);
 
        if(SS_is_running == FALSE) {
            /* send message to boss thread letting it know 
             * we are all done 
             */
            if((bmsg = 
               (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                bmsg->msgType = SS_MONITOR_STOPPED;
                msgq_send_msg(ssBossQH, bmsg);
            }
            kdaemon_thread_exit();
            /* NOT REACHED */
        }

        /* check for the need to create another list thread */
        if((ssListTpool.threadCnt < SS_max_list_thds) &&
           (msgq_get_length(ssListQH) > short_q_depth)) {

            if((bmsg = 
               (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                bmsg->msgType = SS_ADD_LIST_THREAD;
                msgq_send_msg(ssBossQH, bmsg);
            }
        }

        /* check for the need to create more work threads */
        if((ssWorkTpool.threadCnt < SS_max_work_thds) &&
           (msgq_get_length(ssWorkQH) > short_q_depth)) {
            if((bmsg = 
               (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                bmsg->msgType = SS_ADD_WORK_THREAD;
                msgq_send_msg(ssBossQH, bmsg);
            }
        }

        /* hot list msg queues throttle algorithm - 
         * We need to vary the number of messages produced in order to
         * prevent message flooding.  The sample period determines how
         * many messages were dropped as a result of too many messages 
         * on q.  Thresholds for producing messages are adjusted here
         * to either reduce or increase the number of messages for 
         * each domain with vfast enabled.
         */
        if(msg_rev_cnt++ >= SS_MSG_CHK_REVS) { 

           /* The sampling period has ended. Based on the # of 
            * messages dropped averaged throughout the sampling
            * period for each domain, adjust the value of 
            * ssDmnInfo.ssAccessThreshHits to allow message delivery 
            * that does not exceed the global list msgq depth.
            * Send message to boss thread instead of doing it here 
            * to avoid lock hierarchy issues
            */
            if((bmsg = 
               (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                bmsg->msgType = SS_ADJ_MSG_RATE;
                msgq_send_msg(ssBossQH, bmsg);
            }
            bmsg = NULL;
            msg_rev_cnt=0;
        } 

        if(++hot_chk_rev_cnt >= SS_HOT_LIST_CHK_REVS) {
            (void) ss_do_periodic_tasks(SS_HOT_LIST_CHK);
            hot_chk_rev_cnt=0;
        }

        if(++zero_io_rev_cnt >= SS_DEFRAG_ACTIVE_CHK_REVS) {
            (void) ss_do_periodic_tasks(SS_DEFRAG_ACTIVE_CHK);
            zero_io_rev_cnt=0;
        }

        if(++defragall_chk_rev_cnt >= SS_DEFRAG_ALL_CHK_REVS) {
            (void) ss_do_periodic_tasks(SS_DEFRAG_ALL_CHK);
            defragall_chk_rev_cnt=0;
        }
    } /* end forever loop */

    /* NOT REACHED */
    return;
}

/*********************************************************************
 *
 * This routine is the main routine for worker threads.  Threads wait
 * for a volume run message and then call ss_move_file on the volume.
 *
 ********************************************************************/

static void
ss_work_thd_pool ( void )
{
    ssWorkMsgT *msg;
    statusT    sts;

    while (TRUE) {
        /* Wait for something to do */
        msg = (ssWorkMsgT *)msgq_recv_msg( ssWorkQH );

        /* check for a shutdown */
        if(ssWorkTpool.shutdown == TRUE) {
            ssBossMsgT *bmsg=NULL;
            msgq_free_msg( ssWorkQH, msg );
            ADVMTX_SSLIST_WORKTPOOL_LOCK(&ssWorkTpool.plock);
            ssWorkTpool.threadCnt--;
            ADVMTX_SSLIST_WORKTPOOL_UNLOCK(&ssWorkTpool.plock);
            if(ssWorkTpool.threadCnt > 0) {
                /* send message to next thread in pool */
                /* only needed if no messages currently on q */
                if((msg = 
                   (ssWorkMsgT *)msgq_alloc_msg(ssWorkQH)) != NULL) {
                    msg->msgType = SS_WORK_STOP;
                    msgq_send_msg(ssWorkQH, msg);
                }
            } else {
                /* send message to boss thread letting it know we 
                 * the last thread in the pool, then die 
                 */
                if((bmsg = (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                    bmsg->msgType = SS_POOL_STOPPED;
                    msgq_send_msg(ssBossQH, bmsg);
                }
            }
            kdaemon_thread_exit();
            /* NOT REACHED */
        }

        /* vdp->ssVolInfo.ssVdMsgState already SS_POSTED */
        ss_move_file(msg->vdIndex, msg->domainId, msg->msgType);

        msgq_free_msg( ssWorkQH, msg );
    }
    /* NOT REACHED */
}


/*********************************************************************
 *
 * This routine is the main routine for list threads.  Threads wait
 * for a message to update a list, then the thread will call the 
 * appropriate routine to update the correct list.
 *
 ********************************************************************/

static void
ss_list_thd_pool ( void )
{
    statusT    sts;
    ssListMsgT *msg;
    ssFragLLT  *fp;
    ssHotLLT   *hp;
    ssBossMsgT *bmsg;
    int32_t    wday;

    while (TRUE) {
        /* Wait for something to do */
        msg = (ssListMsgT *)msgq_recv_msg( ssListQH );

        /* check for a shutdown */
        if(ssListTpool.shutdown == TRUE) {

            msgq_free_msg( ssListQH, msg );

            ADVMTX_SSLIST_TPOOL_LOCK(&ssListTpool.plock);
            ssListTpool.threadCnt--;
            ADVMTX_SSLIST_TPOOL_UNLOCK(&ssListTpool.plock);
            if(ssListTpool.threadCnt > 0) {
                /* Send message to next thread in pool */
                /* only needed if no messages currently on q. */
                if((msg = 
                   (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
                    msg->msgType = SS_WORK_STOP;
                    msgq_send_msg(ssListQH, msg);
                }
            } else {
                /* Send message to boss thread letting it know we 
                 * are the last thread and we are going to die.
                 */
                if((bmsg = 
                   (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                    bmsg->msgType = SS_POOL_STOPPED;
                    msgq_send_msg(ssBossQH, bmsg);
                }
            }
            kdaemon_thread_exit();
            /* NOT REACHED */
        }

        switch ( msg->msgType ) {
          case FRAG_UPDATE_ADD:

              /* Put this fragmented file onto the list so that it can 
               * be defragmented later by vfast.
               */
              ss_insert_frag_onto_list(msg->frag.vdi,
                                       msg->tag,
                                       msg->bfSetId,
                                       msg->frag.fragRatio,
                                       msg->frag.xtntCnt );

              break;

          case HOT_ADD_UPDATE_WRITE:
          case HOT_ADD_UPDATE_MISS:

              /* Allocate a list entry for the hot file */
              hp = (ssHotLLT *)(ms_malloc( sizeof(ssHotLLT) ));

              /* Initialize fields;   */
              hp->ssHotTag          = msg->tag;
              hp->ssHotBfSetId      = msg->bfSetId;
              hp->ssHotVdi          = msg->hot.vdi;
              hp->ssHotFileSize     = msg->hot.size;
              hp->ssHotPlaced       = 0;
              hp->ssHotErrTime      = 0;
              hp->ssHotLastUpdTime  = 0;
              hp->ssHotEntryTime    = 0;
              for(wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                  hp->ssHotIOCnt[wday] = msg->hot.cnt[wday];
              }

              /* Put this hot file onto the list so that it can be 
               * balanced later by vfast.
               */

              ss_insert_hot_list( msg->msgType, hp );
              break;

          case HOT_REMOVE:
              /* Allocate a list entry for the hot file */
              hp = (ssHotLLT *)(ms_malloc( sizeof(ssHotLLT) ));

              /* Initialize fields;   */
              hp->ssHotTag     = msg->tag;
              hp->ssHotBfSetId = msg->bfSetId;

              /* Remove this hot file from the hot list if its there */
              ss_del_from_hot_list(hp);
              break;

          default:
              MS_SMP_ASSERT(0);
              break;
        }

        msgq_free_msg( ssListQH, msg );
    }
    /* NEVER REACHED */
}

/*********************************************************************
 *
 * This routine adjusts the list work thread msg send rate to the 
 * list message queue by increasing or decreasing the value(threshold) 
 * at which the hot messages are sent.
 *
 ********************************************************************/

static void
ss_adj_msgs_flow_rate()
{
    domainT *dmnP;
    int32_t activeDmnCnt      = 0,
            desiredAveMsgCnt  = 0,
            totListMsgSendCnt = 0,
            totListMsgDropCnt = 0;
    extern domainT *DmnSentinelP;


    /* first count up the number of active domains that also have 
     * vfast activated.  Also total up the number of msgs sent 
     * to all the active dmns.
     */
    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );

    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        VFAST_STATS ( ss_adj_msgs_flow_rate_no_dmn );
        return;
    }

    dmnP = DmnSentinelP;        /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;  /* get reference so domain can't dissappear */
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    do
    {
        /* adjust the msg rate only active domains */
        if( (SS_is_running == TRUE) &&
            (dmnP->ssDmnInfo.ssFirstMountTime != 0) &&
            (dmnP->state == BFD_ACTIVATED) &&
            (dmnP->dmnFlag != BFD_DEACTIVATE_IN_PROGRESS) &&
            (dmnP->dmnFlag != BFD_DEACTIVATE_PENDING) &&
            (dmnP->mountCnt > 0) &&
            (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) )    {

            /* get the total active domains and total msgs sent and
             * total messages dropped so we can determine the average
             * target message rate per domain and adjust individual 
             * domains flow rates based on this average below.
             */
            activeDmnCnt++;
            totListMsgSendCnt += dmnP->ssDmnInfo.ssDmnListMsgSendCnt;
            totListMsgDropCnt += dmnP->ssDmnInfo.ssDmnListMsgDropCnt;

            /* reset for next sample period */
            dmnP->ssDmnInfo.ssDmnListMsgDropCnt = 0;
        }

        ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            cv_broadcast(&dmnP->dmnRefWaitersCV, NULL, CV_NULL_LOCK);
        }

        dmnP = dmnP->dmnFwd;       /* get next ... */
        if (dmnP != DmnSentinelP)  { 
            ++dmnP->dmnAccCnt;     /* add our reference */
        }
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    } while (dmnP != DmnSentinelP);

    /* calculate the ave msgs that should be sent to each domain. */
    if(activeDmnCnt)
        desiredAveMsgCnt = SS_MAX_MSGS_ALLOWED/activeDmnCnt;
    else {
        return;
    }

    /* Now adjust the hot msgs threshold level on each domain.  This 
     * will cause those domains with fewer than the average to 
     * increase their msgs, and those domains with more than the 
     * average to decrease their msg output.
     */
    /* Regrab the starting point and get a reference to it. */
    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        return;
    }

    dmnP = DmnSentinelP; /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;   /* get our reference for the adjustment */
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    do
    {
        
        if((dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE) &&
            (dmnP->ssDmnInfo.ssFirstMountTime != 0)  &&
            (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND))  {

            /* adjust each active dmn's hot msgs to achieve average
             * raising hot msgs threshold(hit cnts) reduces msgs
             * lowering hot msgs threshold reduces msgs
             * adjust thresholds no lower than SS_INIT_FILE_ACC_THRESH 
             */

            if ( totListMsgDropCnt > 0 ) {
                /* list is dropping some - too many msgs */
                if(dmnP->ssDmnInfo.ssDmnListMsgSendCnt > 
                                           desiredAveMsgCnt ) {
                    /* reduce the hot msgs on the hottest domains  
                     * by increasing the threshold 
                     */
                    dmnP->ssDmnInfo.ssAccessThreshHits = 
                        dmnP->ssDmnInfo.ssAccessThreshHits + 
                        (dmnP->ssDmnInfo.ssAccessThreshHits / 8);
                }
            } else if( totListMsgDropCnt < 1 ) {
                /* list is not dropping msgs - balance out msg flows 
                 * from domains 
                 */
                if((totListMsgSendCnt > SS_MAX_MSGS_ALLOWED) &&
                    (dmnP->ssDmnInfo.ssDmnListMsgSendCnt > 
                                                desiredAveMsgCnt) ) {
                    /* reduce the hot msgs 12.5% (raise threshold)
                     * for domains over the ave -or-
                     * whenever a domain exceeds the per domain max msg 
                     * rate allowed
                     */
                    dmnP->ssDmnInfo.ssAccessThreshHits = 
                        dmnP->ssDmnInfo.ssAccessThreshHits + 
                        (dmnP->ssDmnInfo.ssAccessThreshHits / 8);
                } else {
                    /* Increase the hot msgs 12.5% (lower theshold)
                     * for domains equal to or under the ave, not less 
                     * than SS_MIN_FILE_ACC_THRESH though 
                     */
                    dmnP->ssDmnInfo.ssAccessThreshHits = 
                      MAX(SS_MIN_FILE_ACC_THRESH, 
                        (dmnP->ssDmnInfo.ssAccessThreshHits - 
                        (dmnP->ssDmnInfo.ssAccessThreshHits / 8)));
                }
            }
        } /* end if a relevant dmn */

        /* reset for next sample period */
        dmnP->ssDmnInfo.ssDmnListMsgSendCnt = 0; 

        ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            cv_broadcast(&dmnP->dmnRefWaitersCV, NULL, CV_NULL_LOCK);
        }

        dmnP = dmnP->dmnFwd;       /* get next ... */
        if (dmnP != DmnSentinelP)  {
            ++dmnP->dmnAccCnt;          /* add our reference */
        }
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    } while (dmnP != DmnSentinelP);
    return;
}

/*********************************************************************
 *
 * This routine calls a subroutine for each domain that has hotfiles
 * activated.  The subroutine determines what file should be moved
 * to give better IO load balance among volumes in the domain.
 *
 ********************************************************************/

static
statusT
ss_do_periodic_tasks(ssPeriodicMsgTypeT task)
{
    extern   domainT *DmnSentinelP;
    domainT  *dmnP;
    vdIndexT dstVdIndex = -1;
    bfTagT   tag=NilBfTag;
    bfSetIdT setId=nilBfSetId;
    vdT      *dvdp=NULL;      /* destination vd */
    vdT      *vdp=NULL;       /* possible vd needs vfast work */
    uint64_t ioBlkCnt;
    statusT  sts=EOK;
    int32_t  vdi, 
             vdCnt,
             pending_count,
             vdRefed = FALSE;

    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        VFAST_STATS ( ss_do_periodic_tasks_no_dmn );
        return(EOK);
    }

    dmnP = DmnSentinelP; /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;  /* get our ref so domain can't dissappear */
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );

    do
    {
        /* Balance IO on domains that are active and have 
         * ssDmnSmartPlace enabled.
         */

        if( (SS_is_running == TRUE) &&
            (dmnP->ssDmnInfo.ssFirstMountTime != 0) &&
            (dmnP->state == BFD_ACTIVATED) &&
            (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
            (dmnP->dmnFlag != BFD_DEACTIVATE_IN_PROGRESS) &&
            (dmnP->dmnFlag != BFD_DEACTIVATE_PENDING) &&
            (dmnP->mountCnt > 0)) {

            switch(task) {
              case SS_HOT_LIST_CHK:
                if((dmnP->ssDmnInfo.ssDmnHotHdr.ssHotListCnt > 0) &&
                   (dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE) &&
                   (dmnP->ssDmnInfo.ssDmnHotPlaced == FALSE))  {
                    sts = ss_find_hot_target_vd(dmnP,
                                                &dstVdIndex,
                                                &tag,
                                                &setId);
                    vdRefed = FALSE;
                    if((sts==EOK) && (dstVdIndex != -1)) {
                        /* Bump the ref count on this vd so it cant be
                         * removed.
                         */
                        dvdp = vd_htop_if_valid(dstVdIndex, dmnP, TRUE,FALSE);
                        if ( dvdp != NULL ) {
                            vdRefed = TRUE;
                        }
                        /* else - must be an rmvol going on - skip on thru */
    
                        /* Check for a hot file to be migrated on this vol
                         * already.
                         */
                        if((vdRefed == TRUE) &&
                           (BS_BFTAG_EQL
                              (dvdp->ssVolInfo.ssVdHotTag, NilBfTag ))) {
                            /* save off the file for later movement */
                            dvdp->ssVolInfo.ssVdHotTag = tag;
                            dvdp->ssVolInfo.ssVdHotBfSetId = setId;
                            dmnP->ssDmnInfo.ssDmnHotPlaced = TRUE;
                        }

                        if(vdRefed == TRUE)
                            vd_dec_refcnt(dvdp);
                    }
                }
                break;

              case SS_DEFRAG_ACTIVE_CHK:
                /* Search for inactivity on any vds. If detected, signal
                 * vfast to start work on that vd.
                 */
                 
                for (vdi=1, vdCnt=0; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) )
                        continue;

                    ioBlkCnt = vdp->dStat.readblk+vdp->dStat.writeblk;


                    /* This used to check Io Queue lengths before starting
                     * IO.  vdIoOut should track outstanding IOs of any type
                     * of a vd and will therefore replace the IO Queues */
                    if(!(vdp->vdIoOut) &&
                        (vdp->ssVolInfo.ssLastIOCnt == ioBlkCnt) ) {
                        ss_startios(vdp);
                    }
                    vdp->ssVolInfo.ssLastIOCnt = ioBlkCnt;
                    vd_dec_refcnt( vdp );
                    vdCnt++;
                }
                break;

              case SS_DEFRAG_ALL_CHK:
                /* Check for any fragmented files */
                {
                bfsQueueT  *bfSet_queue_entry;
                bfSetT     *bfSetp;
                uint32_t   min_size,
                           page_throttle,
                           msg_throttle,
                           next_fileset,
                           index,
                           set_start_pg,
                           start_index;
                uint64_t   bit,
                           start_bit,
                           tagPg,
                           tagPages;

                if (dmnP->ssDmnInfo.ssDmnDefragAll == 0) {
                    break;
                }
                pending_count = 0;
                for (vdi=1, vdCnt=0; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) )
                        continue;
                    /*
                     * We are not taking out the vdp->ssVolInfo.ssFragLk, 
                     * because we can be off by 1 with little impact to the 
                     * algorithm.  We simply want to see if the # of pending
                     * messages queued for defragmentation is low.
                     */
                    pending_count += vdp->ssVolInfo.ssFragHdr.ssFragListCnt;
                    vd_dec_refcnt( vdp );
                    vdCnt++;
                }
                /* 
                 * a low pending_count allows defrag_all to potentially
                 * add some files to be defragmented 
                 */
                if (pending_count > 1)
                    break;

                ADVRWL_BFSETTBL_READ(dmnP);  
                bfSet_queue_entry = dmnP->bfSetHead.bfsQfwd;
                while ( bfSet_queue_entry != &dmnP->bfSetHead ) {
                    bfSetp = BFSET_QUEUE_TO_BFSETP(bfSet_queue_entry);
                    /* 
                     * handle case of either non initialized array,
                     * or the array needs to grow.
                     */
                    min_size = (uint32_t) TAG_PG_TO_ARRAY_INDEX(bfSetp->tagUnInPg);
                    if (bfSetp->ssTagArraySz < min_size) {
                        /*
                         * grow 4 more array elements than we need 
                         * this allows for approximately an additional
                         * 100k files without needing to grow the array
                         */
                        ss_grow_tag_array(bfSetp, (size_t)min_size+4);
                    }
                    if (bfSetp->ssStartTagPg < bfSetp->tagUnInPg &&
                        bfSetp->ssStartTagPg > 0) {
                        start_index = PG_TO_ARRAY_INDEX(bfSetp->ssStartTagPg);
                        start_bit   = bfSetp->ssStartTagPg % BITS_PER_ARRAY_INDEX;
                    } else {
                        start_index = 0;
                        start_bit   = 0;
                    }

                    page_throttle = msg_throttle = 0;
                    next_fileset = 0;
                    set_start_pg = 1;
                    
                    for (index=start_index; index<bfSetp->ssTagArraySz; index++) {
#ifdef SS_DEBUG
                        if (ss_debug) {
                            if (bfSetp->ssTagArray[index]) {
                                printf("vfast-%s before: index=%5d,  mask=0x%lx\n",
                                       dmnP->domainName, index,bfSetp->ssTagArray[index]);
                            }
                        }
#endif
                        if ((tagPages=bfSetp->ssTagArray[index]) != 0) {
                            for (bit=start_bit; bit<BITS_PER_ARRAY_INDEX; bit++) {
                                /* 
                                 * Only read in a page if its corresponding
                                 * bit is set.
                                 */
                                if (tagPages & (1L << bit)) {
                                    tagPg = bit+(index*BITS_PER_ARRAY_INDEX);
                                    CLEAR_FRAGMENTED_TAG_PAGE(bfSetp->ssTagArray, tagPg);
                                    /* 
                                     * Find any fragmented files on this 
                                     * metadata tag page and send defrag message
                                     * for any found.
                                     */
                                    sts=ss_setup_fragmented_tags(bfSetp, 
                                                                 tagPg,
                                                                 &page_throttle,
                                                                 &msg_throttle);
                                    if (sts != EOK) {
                                        /* We want to start at this page 
                                         * next time interval */
                                        if (set_start_pg) {
                                            bfSetp->ssStartTagPg = (uint32_t)tagPg;
                                            set_start_pg = 0;
                                        }
                                        /* 
                                         * We found a fragmented tag, keep
                                         * the bit set.
                                         */
                                        SET_FRAGMENTED_TAG_PAGE(bfSetp->ssTagArray, tagPg);
                                        /*
                                         * We have sent as many messages as
                                         * autotune can currently handle.
                                         */
                                        if (sts == ENOSPC)
                                            break;
                                    } else {
#ifdef SS_DEBUG
                                        if (ss_debug)
                                            printf("vfast-%s CLR tagPg %ld (0x%lx)\n",
                                                dmnP->domainName, tagPg, (1L << (tagPg%BITS_PER_ARRAY_INDEX)));
#endif
                                    }
                                    /*
                                     * We will throttle our defrag_all setup
                                     * activity if either too many pages are
                                     * read, or we have sent to many messages
                                     * to autotune.
                                     */
                                    if (page_throttle >= SS_MAX_DEFRAG_ALL_PAGES ||
                                        msg_throttle >= SS_MAX_DEFRAG_ALL_MSGS) {
                                        if ((page_throttle >= SS_MAX_DEFRAG_ALL_PAGES) &&
                                            msg_throttle == 0) {
                                            bfSetp->ssStartTagPg = (uint32_t)(tagPg + 1);
                                        }
#ifdef SS_DEBUG
                                        if (ss_debug) {
                                            printf("vfast-%s BREAK: page_throttle(%d), msg_throttle(%d), tagPg=%ld, 0x%lx\n",
                                                    dmnP->domainName, page_throttle,msg_throttle,
                                                    tagPg, 1L<<(tagPg%BITS_PER_ARRAY_INDEX));
                                        }
#endif
                                       
                                        next_fileset = 1;
                                        break;
                                    }
                                 
                                } 
                            } /* for bit */
                            start_bit = 0;
                        }
#ifdef SS_DEBUG
                        if (ss_debug) {
                            if (bfSetp->ssTagArray[index]) {
                                printf("vfast-%s after:  index=%5d,  mask=0x%lx\n",
                                        dmnP->domainName, index, bfSetp->ssTagArray[index]);
                            }
                        }
#endif
                        if (next_fileset)
                            break;
                    } /* for index */
                    if (((page_throttle < SS_MAX_DEFRAG_ALL_PAGES) && 
                         (msg_throttle == 0)) || 
                        (index == bfSetp->ssTagArraySz)) {
                        /* start from the beginning */
                        bfSetp->ssStartTagPg = 0;
                    }
                    bfSet_queue_entry = bfSet_queue_entry->bfsQfwd;

                } /* while file sets */
                ADVRWL_BFSETTBL_UNLOCK(dmnP);
                break;
                }
              default:
                break;

            } /* end switch */
        }

        /* MUST complete this section for each domain! */
        ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            cv_broadcast(&dmnP->dmnRefWaitersCV, NULL, CV_NULL_LOCK);
        }

        dmnP = dmnP->dmnFwd;       /* get next domain... */
        if(dmnP != DmnSentinelP) { /* if we've wrapped - don't bump */
            ++dmnP->dmnAccCnt;     /* add our reference */
        }
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    } while (dmnP != DmnSentinelP);

    return(sts);

} /* end ss_do_periodic_tasks */

/********  end of thread pool routines  ************/


/********  start of domain activate/deactivate routines  *******/


/*********************************************************************
 *  ss_dmn_activate
 *
 *  DmnTblLock is held by caller to prevent racing addvol and rmvol.
 *  Will not race with umount because only last activator(umount) does
 *  the deactivation of the domain and this is called before that final
 *  deactivation.
 *
 *  1. create ss mutexes for domain passed in.
 *  2. load/initialize ss domain structure.
 *
 ********************************************************************/

void
ss_dmn_activate(domainT *dmnP)
{
    vdT          *logVdp = NULL;
    statusT      sts;
    bsSSDmnAttrT ssAttr;
    int32_t      oldState;

    /*
     * Check validity of pointer.
     */
    MS_SMP_ASSERT(dmnP);

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    /* fetch ss record from vd where the log is, if there */
    sts = bmtr_get_rec(logVdp->rbmtp,
                       BSR_DMN_SS_ATTR,
                       &ssAttr,
                       sizeof( ssAttr ) );
    if (sts == EBMTR_NOT_FOUND) {
        /* init and write out a new default record */
        ss_init_dmnInfo(dmnP);

        sts = ss_put_rec (dmnP);
        if(sts != EOK) {
            VFAST_STATS ( ss_dmn_activate_put_rec_err );
	    goto HANDLE_EXCEPTION;
        }
        return;

    } else if (sts != EOK) {
        VFAST_STATS ( ss_dmn_activate_get_rec_err );
	goto HANDLE_EXCEPTION;
    }

    ss_init_dmnInfo(dmnP); /* init mutexes and set default values */

    /* fill in dmnP->ssDmnInfo from on-disk ssAttr and return */
    ss_copy_rec_from_disk(&ssAttr, dmnP); 
    return;

HANDLE_EXCEPTION:

    advfs_ss_change_state(dmnP->domainName, SS_ERROR, &oldState, 1);
    return;
}


/*********************************************************************
 * ss_dmn_deactivate
 * called any time the domain is deactivated.
 *
 * See also ss_dealloc_vd which is called via vd_remove
 *
 * dmnP->ssDmnInfo.ssDmnLk is held by caller or a flag is passed in to get it.
 * dmnP->ssDmnInfo.ssDmnLk is always released in here.
 *
 ********************************************************************/

void
ss_dmn_deactivate(domainT *dmnP, int flag)
{
    statusT sts;
    int     vdi, 
            vdCnt=0;
    vdT*    vdp;
 
    MS_SMP_ASSERT(dmnP);

    if(flag == TRUE) {
        ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
    }

    /* Notify any threads working on a files of shutdown. This is 
     * required so that we do not block the deactivation process
     * by keeping the domain open during a vfast migrate.
     * This broadcast is also needed in vd_remove so that a rmvol
     * forces vfast to release the vol so that vd_remove may
     * proceed.
     */
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        }

        ADVSPL_VOLINFO_MSG_LOCK( &vdp->ssVolInfo.ssVdMsgLk );
        vdp->ssVolInfo.ssVdMsgState = SS_STOP;
        ADVSPL_VOLINFO_MSG_UNLOCK( &vdp->ssVolInfo.ssVdMsgLk );

        ADVMTX_SSVDMIG_LOCK( &vdp->ssVolInfo.ssVdMigLk );
        vdp->ssVolInfo.ssVdMigState = SS_ABORT;
        ADVMTX_SSVDMIG_UNLOCK( &vdp->ssVolInfo.ssVdMigLk );

        cv_broadcast (&vdp->ssVolInfo.ssContMigCV, NULL, CV_NULL_LOCK);
        SS_TRACE(vdp,0,0,0,0);
        vd_dec_refcnt( vdp );
        vdCnt++;
    }

    /* Wait for any working vfast threads to finish their current
     * work cycle and notice we are deactivating this domain.
     */
    if (dmnP->ssDmnInfo.ssDmnSSThdCnt > 0) {
        dmnP->ssDmnInfo.ssDmnSSWaiters++;
        SS_TRACE(vdp,0,0,0,0);
        while (TRUE) {
            cv_wait(&dmnP->ssDmnInfo.ssDmnSSWaitersCV,
                            &dmnP->ssDmnInfo.ssDmnLk, CV_MUTEX, CV_NO_RELOCK);
            if (!dmnP->ssDmnInfo.ssDmnSSWaiters) {
                    break;
            }
            ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
        }
    } else {
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );
    }
    SS_TRACE(vdp,0,0,0,0);
    return;
}

/*********************************************************************
 * ss_dealloc_dmn
 * called any time the domain is deactivated because of last umount
 *
 * 1. cleans up the domain structure vfast linked lists
 * 2. destroys vfast mutexes
 *
 * See also ss_dealloc_vd which is called via vd_remove
 ********************************************************************/

void
ss_dealloc_dmn(domainT *dmnP)
{
    ss_dealloc_hot(dmnP);
    ADVMTX_SSDOMAIN_DESTROY( &dmnP->ssDmnInfo.ssDmnLk );
    ADVMTX_SSDOMAIN_HOT_DESTROY( &dmnP->ssDmnInfo.ssDmnHotLk );
    cv_destroy ( &dmnP->ssDmnInfo.ssDmnSSWaitersCV);
    return;
}



/*********************************************************************
 *
 * deallocate the vd lists 
 *
 ********************************************************************/

void
ss_dealloc_vd(vdT *vdp)  /* in - pointer to header for list */
{
    /* remove frag list from vd */
    ss_dealloc_frag_list(vdp); 
    mutex_destroy( &vdp->ssVolInfo.ssVdMigLk );
    spin_destroy ( &vdp->ssVolInfo.ssVdMsgLk );
    mutex_destroy( &vdp->ssVolInfo.ssFragLk );
    cv_destroy ( &vdp->ssVolInfo.ssContMigCV);
    return;
}


/*********************************************************************
 *
 * This routine changes the activation state of a domain.
 *
 * During cfs mount/umounts, disable vfast to prevent
 * vfast migrates from occuring during the mount/umount.
 * If we didn't do this, vfast may race with 
 * the mount/umount and make/not make a callout to 
 * CC_CFS_CONDIO_EXCL_MODE_ENTER in migrate.  This could
 * lead to a panic because the pvnode may not be available
 * yet, even though, from advfs it looks like the fset is mounted.
 *
 ********************************************************************/

statusT
advfs_ss_change_state(char     *domain_name,      /* in */
                      ssDmnOpT  state,            /* in */
                      ssDmnOpT *oldState,         /* out */
                      int       write_out_record) /* in */
{
    statusT sts;
    struct  timeval new_time;
    vdT*    vdp;
    domainT *dmnP;
    bfDomainIdT domainId;
    int32_t vdi, 
            vdCnt = 0,
            dmnActive   = FALSE, 
            dmnOpen     = FALSE,
            ssDmnLocked = FALSE;

    sts = bs_bfdmn_tbl_activate( domain_name,
                                 0,
                                 &domainId );
    if (sts != EOK) {
	goto HANDLE_EXCEPTION;
    }
    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );
    if (sts != EOK) {
        VFAST_STATS ( ss_change_state_dmn_access_err );
	goto HANDLE_EXCEPTION;
    }
    dmnOpen = TRUE;

    MS_SMP_ASSERT(dmnP);
    if(dmnP->ssDmnInfo.ssDmnState == state) {
        VFAST_STATS ( ss_change_state_state_err );
        /* already set to this new state-abort*/
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
    ssDmnLocked = TRUE;

    /* if this is first time activating, record the First mount time*/
    if((dmnP->ssDmnInfo.ssFirstMountTime == 0) &&
       (state == SS_ACTIVATED || state == SS_SUSPEND)) {
        new_time = get_system_time();
        dmnP->ssDmnInfo.ssFirstMountTime = new_time.tv_sec;
    }

    *oldState = dmnP->ssDmnInfo.ssDmnState;

    switch(state) {
        case SS_CFS_RELOC:   /* special cluster srvr reloc case */
        case SS_CFS_UMOUNT:  /* special cluster umount case */
        case SS_CFS_MOUNT:   /* special cluster mount case */
            if(dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED)  {
                /* Set state to SS_CFS_RELOC only if currently SS_ACTIVATED.
                 * vfast state will be restored to SS_ACTIVATED in ss_put_rec
                 * after domain is deactivated if case == SS_CFS_RELOC.
                 * vfast state will be restored to SS_ACTIVATED by caller
                 * after fset is done being umounted if case == SS_CFS_UMOUNT.
                 * If vfast is already in some deactivated state, nothing to do.
                 */
                dmnP->ssDmnInfo.ssDmnState = state;
                ss_dmn_deactivate(dmnP, FALSE);
                ssDmnLocked = FALSE;
            }
            break;
        case SS_ACTIVATED:
            if(dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) {
                /* Not already SS_ACTIVATED, set to SS_ACTIVATED and return
                 * thread states to SS_IDLE
                 * DO NOT set ssVdMigState = SS_IDLE if theres a chance a thread is
                 * already in a volume.  To do so would allow two threads to work a
                 * volume at the same time and that is a big no-no.
                 */
                dmnP->ssDmnInfo.ssDmnState = state;
                for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
                        continue;
                    }
                    vdp->ssVolInfo.ssVdMigState = SS_MIG_IDLE;
                    /*
		     * Turn ssVdMsgState back on after an rmvol.
		     * If it is left as SS_STOP, no further autotune
		     * processing would occur on this vd.
                     */
                    vdp->ssVolInfo.ssVdMsgState = SS_MSG_IDLE;
                    vd_dec_refcnt( vdp );
                    vdCnt++;
                }
            }
            break;
        default:
            /* vfast state is either SS_DEACTIVATED or SS_SUSPENDED.
             * Wait for any working vfast threads to finish their current
             * work cycle and notice we are deactivating this domain.
             */
            dmnP->ssDmnInfo.ssDmnState = state;
            ss_dmn_deactivate(dmnP, FALSE);  /* lock released in ss_dmn_deactivate */
            ssDmnLocked = FALSE;
    }
            
    if(ssDmnLocked == TRUE) {
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );
        ssDmnLocked = FALSE;
    }

    if (dmnP->ssDmnInfo.ssDmnState == SS_DEACTIVATED) {
        ss_dealloc_hot(dmnP);
        for (vdi=1; vdCnt<dmnP->vdCnt && vdi<=BS_MAX_VDI; vdi++) {
            if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) )
                continue;
            ss_dealloc_frag_list(vdp);
            vd_dec_refcnt(vdp);
            vdCnt++;
        }
    }

    if(write_out_record == 1) {
        /* write out on-disk vfast record here. */
        sts = ss_put_rec (dmnP);
        if(sts != EOK) {
            VFAST_STATS ( ss_change_state_put_rec_err );
	    goto HANDLE_EXCEPTION;
        }
    }

HANDLE_EXCEPTION:

    if (dmnOpen) {
        if(ssDmnLocked == TRUE) {
            ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );
        }
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        bs_bfdmn_deactivate( domainId, 0 );
    }

    return(sts);
}

/********   end of domain activate/deactivate routines  *******/


/********   start of structure and record init routines  *******/

/*********************************************************************
 *
 * This routine initializes the vfast in memory record tied
 * to each domain.
 *
 ********************************************************************/
static void
ss_init_dmnInfo(domainT *dmnP)
{
    bzero( (char *)&dmnP->ssDmnInfo, sizeof( struct ssDmnInfo ) );
    ADVMTX_SSDOMAIN_INIT(&dmnP->ssDmnInfo.ssDmnLk);
    ADVMTX_SSDOMAIN_HOT_INIT(&dmnP->ssDmnInfo.ssDmnHotLk);

    (void) cv_init(&dmnP->ssDmnInfo.ssDmnSSWaitersCV, "ssDmnSSWaiters CV",
                    NULL, CV_WAITOK);

    dmnP->ssDmnInfo.ssDmnState                  = SS_DEACTIVATED;
    dmnP->ssDmnInfo.ssFirstMountTime            = 0;
    dmnP->ssDmnInfo.ssDmnHotWorking             = FALSE;
    dmnP->ssDmnInfo.ssDmnHotPlaced              = FALSE;
    dmnP->ssDmnInfo.ssDmnDirectIo               = TRUE;
    dmnP->ssDmnInfo.ssDmnDefragment             = FALSE;
    dmnP->ssDmnInfo.ssDmnDefragAll              = FALSE;
    dmnP->ssDmnInfo.ssDmnSmartPlace             = FALSE;
    dmnP->ssDmnInfo.ssDmnBalance                = FALSE;

    /* UI configurable options */
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = SS_INIT_PCT_IOS_WHEN_BUSY;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssIoInterval = 100 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssIoInterval = 0;
    dmnP->ssDmnInfo.ssAccessThreshHits       = SS_INIT_FILE_ACC_THRESH;
    dmnP->ssDmnInfo.ssFilesDefraged          = 0;
    dmnP->ssDmnInfo.ssFobsDefraged           = 0;
    dmnP->ssDmnInfo.ssFobsBalanced           = 0;
    dmnP->ssDmnInfo.ssFilesIOBal             = 0;
    dmnP->ssDmnInfo.ssExtentsConsol          = 0;
    dmnP->ssDmnInfo.ssPagesConsol            = 0;
    dmnP->ssDmnInfo.ssSteadyState            = SS_INIT_STEADY_STATE;
    dmnP->ssDmnInfo.ssMinutesInHotList       = SS_INIT_MINUTES_HOT_LST;
    dmnP->ssDmnInfo.ssReserved1              = 0;
    dmnP->ssDmnInfo.ssReserved2              = 0;

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotFwd =
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr);
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotBwd =
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr);
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotListCnt = 0;
    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    return ;

} /* end ss_init_dmnInfo */



/*********************************************************************
 *  ss_init_vd
 * This routine initializes the vfast in memory record tied
 * to each volume.
 *
 *  1. create ss mutexes for volume passed in.
 *  2. initialize ss volume structure.
 *
 ********************************************************************/

void
ss_init_vd(vdT *vdp)
{
    ADVSPL_VOLINFO_MSG_INIT(&vdp->ssVolInfo.ssVdMsgLk);
    ADVMTX_SSVDMIG_INIT(&vdp->ssVolInfo.ssVdMigLk);
    ADVMTX_VOLINFO_FRAG_INIT(&vdp->ssVolInfo.ssFragLk);
    ADVMTX_SSVDMIG_LOCK( &vdp->ssVolInfo.ssVdMigLk );
    (void) cv_init( &vdp->ssVolInfo.ssContMigCV, "ssContMig CV",
                    NULL, CV_WAITOK );
    vdp->ssVolInfo.ssVdMigState = SS_MIG_IDLE;
    ADVMTX_SSVDMIG_UNLOCK( &vdp->ssVolInfo.ssVdMigLk );
    vdp->ssVolInfo.ssVdMsgState = SS_MSG_IDLE;

    /* initialize the frag list */
    vdp->ssVolInfo.ssFragHdr.ssFragRatFwd  = 
          (struct ssFragEntry *)(&vdp->ssVolInfo.ssFragHdr);
    vdp->ssVolInfo.ssFragHdr.ssFragRatBwd  = 
          (struct ssFragEntry *)(&vdp->ssVolInfo.ssFragHdr);
    vdp->ssVolInfo.ssFragHdr.ssFragListCnt = 0;
    vdp->ssVolInfo.ssFragHdr.ssFragHdrThresh = 0x7fffffffffffffff;

    /* initialize the pack list */
    vdp->ssVolInfo.ssPackHdr.ssPackFwd  = 
          (struct ssPackEntry *)(&vdp->ssVolInfo.ssPackHdr);
    vdp->ssVolInfo.ssPackHdr.ssPackBwd  = 
          (struct ssPackEntry *)(&vdp->ssVolInfo.ssPackHdr);
    vdp->ssVolInfo.ssPackHdr.ssPackListCnt = 0;
    vdp->ssVolInfo.ssPackHdr.ssPackZoneStartBlk = 0;
    vdp->ssVolInfo.ssPackHdr.ssPackZoneEndBlk = 0;
    vdp->ssVolInfo.ssPackHdr.ssPackLastBMTPage = 0;
    return;
}

/*********************************************************************
 *
 * This routine copies the on disk domain structure to in memory 
 * structure.
 *
 ********************************************************************/

static void
ss_copy_rec_from_disk(bsSSDmnAttrT *ssAttr,        /* in */ 
                      domainT *dmnP)               /* out */
{
    dmnP->ssDmnInfo.ssDmnState = ssAttr->ssDmnState;
    /* reset vfast state if we have been relocated by cfs - precaution */
    if((dmnP->ssDmnInfo.ssDmnState == SS_CFS_RELOC) ||
       (dmnP->ssDmnInfo.ssDmnState == SS_CFS_UMOUNT) ||
       (dmnP->ssDmnInfo.ssDmnState == SS_CFS_MOUNT)) {
        dmnP->ssDmnInfo.ssDmnState = SS_ACTIVATED;
    }
    dmnP->ssDmnInfo.ssDmnDefragment = ssAttr->ssDmnDefragment;
    dmnP->ssDmnInfo.ssDmnDefragAll  = ssAttr->ssDmnDefragAll;
    dmnP->ssDmnInfo.ssDmnSmartPlace = ssAttr->ssDmnSmartPlace;
    dmnP->ssDmnInfo.ssDmnBalance    = ssAttr->ssDmnBalance;
    dmnP->ssDmnInfo.ssDmnVerbosity  = ssAttr->ssDmnVerbosity;
    dmnP->ssDmnInfo.ssDmnDirectIo   = ssAttr->ssDmnDirectIo;
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = 
                            ssAttr->ssMaxPercentOfIoWhenBusy;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssIoInterval = 100 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssIoInterval = 0;
    dmnP->ssDmnInfo.ssAccessThreshHits = ssAttr->ssAccessThreshHits;
    dmnP->ssDmnInfo.ssFilesDefraged    = ssAttr->ssFilesDefraged;
    dmnP->ssDmnInfo.ssFobsDefraged     = ssAttr->ssFobsDefraged;
    dmnP->ssDmnInfo.ssFobsBalanced     = ssAttr->ssFobsBalanced;
    dmnP->ssDmnInfo.ssFilesIOBal       = ssAttr->ssFilesIOBal;
    dmnP->ssDmnInfo.ssExtentsConsol    = ssAttr->ssExtentsConsol;
    dmnP->ssDmnInfo.ssPagesConsol      = ssAttr->ssPagesConsol;
    dmnP->ssDmnInfo.ssSteadyState      = (uint32_t)ssAttr->ssSteadyState;
    dmnP->ssDmnInfo.ssMinutesInHotList = ssAttr->ssMinutesInHotList;
    dmnP->ssDmnInfo.ssReserved1        = (uint32_t)ssAttr->ssReserved1;
    dmnP->ssDmnInfo.ssReserved2        = (uint32_t)ssAttr->ssReserved2;
    return;
}

/*********************************************************************
 *
 * This routine copies the in memory domain structure to disk.
 *
 ********************************************************************/

static void
ss_copy_rec_to_disk(domainT      *dmnP,    /* in */
                    bsSSDmnAttrT *ssAttr)  /* out */
                    
{
    /* user configurable by domain */
    ssAttr->ssDmnState      = dmnP->ssDmnInfo.ssDmnState;
    ssAttr->ssDmnDefragment = dmnP->ssDmnInfo.ssDmnDefragment;
    ssAttr->ssDmnDefragAll  = dmnP->ssDmnInfo.ssDmnDefragAll;
    ssAttr->ssDmnSmartPlace = dmnP->ssDmnInfo.ssDmnSmartPlace;
    ssAttr->ssDmnBalance    = dmnP->ssDmnInfo.ssDmnBalance;
    ssAttr->ssDmnVerbosity  = dmnP->ssDmnInfo.ssDmnVerbosity;
    ssAttr->ssDmnDirectIo   = dmnP->ssDmnInfo.ssDmnDirectIo;

    /* user hidden configurable by domain */
    ssAttr->ssMinutesInHotList = dmnP->ssDmnInfo.ssMinutesInHotList;
    ssAttr->ssMaxPercentOfIoWhenBusy = 
                      dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    ssAttr->ssAccessThreshHits = dmnP->ssDmnInfo.ssAccessThreshHits;
    ssAttr->ssSteadyState = (uint16_t)dmnP->ssDmnInfo.ssSteadyState;

    /* statistics */
    ssAttr->ssFilesDefraged = dmnP->ssDmnInfo.ssFilesDefraged;
    ssAttr->ssFobsDefraged  = dmnP->ssDmnInfo.ssFobsDefraged;
    ssAttr->ssFobsBalanced  = dmnP->ssDmnInfo.ssFobsBalanced;
    ssAttr->ssFilesIOBal    = dmnP->ssDmnInfo.ssFilesIOBal;
    ssAttr->ssExtentsConsol = dmnP->ssDmnInfo.ssExtentsConsol;
    ssAttr->ssPagesConsol   = dmnP->ssDmnInfo.ssPagesConsol;

    /* extras on-disk */
    ssAttr->ssReserved1 = (uint16_t)dmnP->ssDmnInfo.ssReserved1;
    ssAttr->ssReserved2 = (uint16_t)dmnP->ssDmnInfo.ssReserved2;
    return;
}


/*********************************************************************
 *
 * This routine writes out the in memory vfast record to disk.
 * It overwrites record if already there, creates if not there.
 *
 ********************************************************************/

/* overwrite if there, creates if not there */
statusT
ss_put_rec( domainT *dmnP )

{
    vdT *logVdp = NULL;
    ftxHT ftx;
    statusT sts;
    bsSSDmnAttrT ssAttr;
    int32_t ftxStarted = FALSE,
            vd_refed = FALSE;

    MS_SMP_ASSERT(dmnP);

    /*
     * If there has been a domain_panic, do not start another root
     * transaction.
     */
    if ( dmnP->dmn_panic ) {
	sts = E_DOMAIN_PANIC;
	goto HANDLE_EXCEPTION;
    }

    /* Be sure to bump the refCnt until we are done */
    if ( !(logVdp = 
         vd_htop_if_valid(BS_BFTAG_VDI(dmnP->ftxLogTag), 
                                     dmnP, TRUE, FALSE)) ) {
        VFAST_STATS ( ss_put_rec_bad_vdi );
	sts = EBAD_VDI;
	goto HANDLE_EXCEPTION;
    }
    vd_refed = TRUE;

    /* reset vfast state if we have been relocated by cfs */
    if(dmnP->ssDmnInfo.ssDmnState == SS_CFS_RELOC) {
        /* don't set update on-disk flag or we would recall this routine! */
        dmnP->ssDmnInfo.ssDmnState = SS_ACTIVATED;
    }

    /* write the record to disk structure */

    ss_copy_rec_to_disk(dmnP, &ssAttr);

    sts = FTX_START_N(FTA_NULL, &ftx, FtxNilFtxH, dmnP);
    if (sts != EOK) {
        VFAST_STATS ( ss_put_rec_bad_ftx );
	sts = EBAD_FTXH;
	goto HANDLE_EXCEPTION;
    }
    ftxStarted = TRUE;

    /*
     * Install OR Update ss record in the BMT (V3) or RBMT (V4).
     */
    sts = bmtr_put_rec(
        logVdp->rbmtp,
        BSR_DMN_SS_ATTR,
        &ssAttr,
        sizeof( bsSSDmnAttrT ),
        ftx );
    if (sts != EOK) {
        VFAST_STATS ( ss_put_rec_bad_bmtr_put_rec );
	goto HANDLE_EXCEPTION;
    }

    ftx_done_n( ftx, FTA_SS_DMN_DATA_WRITE );
    ftxStarted = FALSE;

    if(vd_refed)
        vd_dec_refcnt( logVdp );
    return EOK;

HANDLE_EXCEPTION:

    if(vd_refed)
        vd_dec_refcnt( logVdp );

    if(ftxStarted == TRUE) {
       ftx_fail (ftx); 
    }
    return sts;
}


/********   end of structure and record init routines  *******/

/*********** start  debug routines  ****************/

#ifdef ADVFS_SS_TRACE
void
ss_trace( vdT      *vdp,
          uint16_t module,
          uint16_t line,
          int64_t  value1,
          int64_t  value2,
          int64_t  value3,
          int64_t  value4
        )
{
     ssTraceElmtT *te;
    extern mutexT TraceLock;
    extern int TraceSequence;

    ADVMTX_TRACE_LOCK(&TraceLock);

    vdp->ss_trace_ptr = (vdp->ss_trace_ptr + 1) % SS_TRACE_HISTORY;
    te = &vdp->ss_trace_buf[vdp->ss_trace_ptr];
    te->thd = (struct thread *)(((int64_t)current_cpu() << 36) |
                                 (int64_t)u.u_kthreadp & 0xffffffff);
    te->seq  = TraceSequence++;
    te->mod  = module;
    te->ln   = line;
    te->val1 = value1; /* tag */
    te->val2 = value2; /* offset */
    te->val3 = value3; /* cnt */
    te->val4 = value4; /* destinationBlk */

    ADVMTX_TRACE_UNLOCK(&TraceLock);
}
#endif /* ADVFS_SS_TRACE */

/********   start of frag list routines  *******/

/*********************************************************************
 *
 * Use the block map routine to determine a count of the number of extents
 * that are not contiguous to each other.
 *
 * lock bfap->xtntMap_lk is held and released by callers!
 *
 ********************************************************************/
int
ss_xtnt_counter(bfAccessT *bfap)
{
    statusT  sts;
    uint64_t count = 0;
    off_t    offset = 0;
    bsInMemXtntMapT   *xtntMap = bfap->xtnts.xtntMap;
    extent_blk_desc_t *not_used=NULL;

    /*
     * Passing XTNT_NO_MAPS should guarantee that we don't need to free
     * any pointers after the call.  When RND_MIGRATE is passed, the count
     * returned should consider 2 extents that are logically separated by
     * a hole, yet physically contiguous on disk, as a single extent.
     * 
     */
    sts = advfs_get_blkmap_in_range(bfap,
                                    xtntMap,
                                    &offset,  /* starting offset */
                                    (xtntMap->bsxmNextFob - 1L) * ADVFS_FOB_SZ,
                                              /* length in bytes */
                                    &not_used,
                                    &count,
                                    RND_MIGRATE,
                                    EXB_ONLY_STG, /* only storage */
                                    XTNT_NO_WAIT | XTNT_NO_MAPS | XTNT_LOCKS_HELD);
    MS_SMP_ASSERT(not_used == NULL);
    return (int) ((sts != EOK) ? sts : count);
}


/*********************************************************************
 * ss_chk_fragratio
 *
 * This routine checks the file's degree of fragmentation against the
 * frag list threshold.  If it is worse, then a message is sent to the 
 * frag list pool to add the file.  If it is not worse, does nothing.
 *
 * lock bfap->xtntMap_lk is held by caller!
 *
 ********************************************************************/

statusT
ss_chk_fragratio (
                 bfAccessT *bfap  /* in */
                 )
{
    uint32_t totXtnts    = 0,
             allocFobCnt = 0;
    int32_t  vd_refed    = FALSE;
    ssListMsgT *listmsg;
    vdT        *vdp;
    statusT    sts=EOK;

    MS_SMP_ASSERT(bfap);

    /*
     * This is a direct io enabled file, and vfast is not scheduled to perform
     * on direct io enabled files.
     */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
       (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO) ||
        (bfap->dioCnt)) {
        VFAST_STATS ( ss_chk_fragratio_dio_err );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    if(bfap->ssMigrating==SS_STOP) {
        /* vfast closed this file with an error, reset flag
         * to allow next closer to check for placement on 
         * frag list.
         */
        bfap->ssMigrating = SS_MSG_IDLE; /* allows to be placed again */
        VFAST_STATS ( ss_chk_fragratio_bfap_closed_with_err );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* check for ss up and activated on this domain */
    if ((SS_is_running == FALSE) ||
        (bfap->ssMigrating != SS_MSG_IDLE) ||
        (bfap->dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (bfap->dmnP->state != BFD_ACTIVATED) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (bfap->dmnP->mountCnt == 0) ||
        ((bfap->dmnP->ssDmnInfo.ssDmnDefragment == FALSE) &&
         (bfap->dmnP->ssDmnInfo.ssDmnDefragAll  == FALSE)) ||
         !(bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_CFS_MOUNT ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_CFS_UMOUNT) ) 
    {
        VFAST_STATS ( ss_chk_fragratio_sanity_err );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }
    if ((sts = sc_valid_vd(bfap->dmnP, defServiceClass, bfap->primMCId.volume)) != EOK) {
        VFAST_STATS ( ss_chk_fragratio_vd_not_in_svcclass );
        /* 
         * Assume a rmvol is in process and don't perform any vfast ops.
         */
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }
    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
    if (sts != EOK) {
        VFAST_STATS ( ss_chk_fragratio_lock_xtnt_map_err );
	goto HANDLE_EXCEPTION;
    }
    allocFobCnt = bfap->xtnts.bimxAllocFobCnt;
    /* go get the number of not contiguous extents in the file */
    totXtnts = ss_xtnt_counter(bfap);
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    if (totXtnts <= 1) {  /* cant do any better than 1, eh? */
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* bump the ref count on the vd */
    vdp = vd_htop_if_valid(bfap->primMCId.volume, bfap->dmnP, TRUE, FALSE);
    if ( vdp == NULL ) {
        /* only this vd is activated, abort */
        VFAST_STATS ( ss_chk_fragratio_vd_ok );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }
    vd_refed = TRUE;


    if ( (EXTENTS_FRAG_RATIO(allocFobCnt,totXtnts) <
           vdp->ssVolInfo.ssFragHdr.ssFragHdrThresh) ||
         ( vdp->ssVolInfo.ssFragHdr.ssFragListCnt < 
           SS_INIT_FRAG_LST_SZ ) )    {

        if((listmsg = 
           (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
            listmsg->msgType = FRAG_UPDATE_ADD;
            listmsg->tag = bfap->tag;
            listmsg->bfSetId = bfap->bfSetp->bfSetId;
            listmsg->frag.vdi = vdp->vdIndex;
            listmsg->frag.fragRatio = (uint64_t)
                  EXTENTS_FRAG_RATIO(allocFobCnt,totXtnts);
            listmsg->frag.xtntCnt = totXtnts;
            msgq_send_msg(ssListQH, listmsg);
        } else {
            ADVMTX_SSDOMAIN_LOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
            bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
            ADVMTX_SSDOMAIN_UNLOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
        }
    }

    ADVMTX_SSDOMAIN_LOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
    bfap->dmnP->ssDmnInfo.ssDmnListMsgSendCnt++;
#ifdef DEBUG
    bfap->dmnP->ssDmnInfo.ssDmnMsgSendAttmptCntFrag++;
#endif
    ADVMTX_SSDOMAIN_UNLOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );

HANDLE_EXCEPTION:

    if (vd_refed) {
        vd_dec_refcnt(vdp);
    }
    return(sts);
}



/*********************************************************************
 * ss_insert_frag_onto_list
 *
 * This routine places an ssFragLLT struct onto the list for a given
 * domain and volume(vdIndex).
 *
 * The vdp->ssVolInfo.ssFragLk lock will be seized and 
 * released inside this routine.
 *
 ********************************************************************/

static void
ss_insert_frag_onto_list( vdIndexT vdi,
                       bfTagT tag,
                       bfSetIdT bfSetId,
                       uint64_t fragRatio, /* primary sort in list */
                       uint32_t xtntCnt)  /* secondary sort in list */
{
    domainT *dmnP;
    bfSetT *bfSetp;
    vdT* vdp=NULL;
    ssFragLLT *currp, /* list pointer */
              *nextp, /* list pointer */
              *fp = NULL;
    ssFragHdrT *fhp;
    int32_t inserted = FALSE,
            setOpen  = FALSE, 
            vd_refed = FALSE;
    statusT sts;

    /* Now open the set */
    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            VFAST_STATS ( ss_insert_frag_onto_list_rbf_bfs_open_err );
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
        VFAST_STATS ( ss_insert_frag_onto_list_fileset_not_acc );
        goto _cleanup;
    } 

    /* Check dmnState here again, discard msg if state has changed since 
     * this message was sent.
     */
    if( (SS_is_running == FALSE) ||
        (dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (dmnP->mountCnt == 0) ||
          ! (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND ||
             dmnP->ssDmnInfo.ssDmnState == SS_CFS_MOUNT ||
             dmnP->ssDmnInfo.ssDmnState == SS_CFS_UMOUNT))  {
        VFAST_STATS ( ss_insert_frag_onto_list_sanity_err );
        goto _cleanup;
    }

    /* bump the ref count on the vd */
    vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE);
    if ( vdp == NULL ) {
        /* must be an rmvol going on - just delete this msg */
        VFAST_STATS ( ss_insert_frag_onto_list_rmvol_in_progress );
        goto _cleanup;
    }
    vd_refed = TRUE;

    /* Allocate a list entry for the fragmented file */
    fp = (ssFragLLT *)(ms_malloc( sizeof(ssFragLLT) ));

    /* Initialize fields; ms_malloc() inited struct to zeros.  */
    fp->ssFragTag     = tag;
    fp->ssFragBfSetId = bfSetId;
    fp->ssFragRatio   = fragRatio;
    fp->ssXtntCnt     = xtntCnt;

    ADVMTX_VOLINFO_FRAG_LOCK(&vdp->ssVolInfo.ssFragLk);
    ss_delete_from_frag_list(vdp, tag, bfSetId);

    /* insert this new entry into correct order */
    fhp = &vdp->ssVolInfo.ssFragHdr;
    currp = fhp->ssFragRatFwd;
    while ( currp != (ssFragLLT *)fhp )  {
        if (fp->ssFragRatio <= currp->ssFragRatio ) {

            while((currp != (ssFragLLT *)fhp) &&
                  (fp->ssFragRatio == currp->ssFragRatio ) &&
                  (fp->ssXtntCnt <= currp->ssXtntCnt) ) {
             /* keep going until we find correct place to put entry */
                  currp = currp->ssFragRatFwd;  
            }
            if(currp == (ssFragLLT *)fhp) break; /* at list end */

            /* inserting in middle of list, insert before currp */
            fp->ssFragRatFwd = currp;
            fp->ssFragRatBwd = currp->ssFragRatBwd;
            currp->ssFragRatBwd->ssFragRatFwd = fp;
            currp->ssFragRatBwd = fp;
            fhp->ssFragListCnt++;
            inserted = TRUE;
            break;
        }
        currp = currp->ssFragRatFwd;  /* Keep going! */
    }

    /* if not inserted above, put it on end of list */
    if(!inserted) {
        /* goes on end of list */
        fp->ssFragRatFwd = (ssFragLLT *) fhp;
        fp->ssFragRatBwd = fhp->ssFragRatBwd;
        fhp->ssFragRatBwd->ssFragRatFwd = fp;
        fhp->ssFragRatBwd = fp;
        fhp->ssFragListCnt++;
    }


    if ( fhp->ssFragListCnt > SS_INIT_FRAG_LST_SZ ) {
        /* too many entries in list, delete the last one in list */
        currp = fhp->ssFragRatBwd;
        currp->ssFragRatFwd->ssFragRatBwd = currp->ssFragRatBwd;
        currp->ssFragRatBwd->ssFragRatFwd = currp->ssFragRatFwd;
        currp->ssFragRatFwd = currp->ssFragRatBwd = 0;
        ms_free(currp); fhp->ssFragListCnt--;

        /* reset threshold */
        fhp->ssFragHdrThresh = fhp->ssFragRatBwd->ssFragRatio;
    }

    ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);

_cleanup:

    if (vd_refed) {
        vd_dec_refcnt(vdp);
    }
    if (setOpen) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        setOpen = FALSE;
    }
    return;
}



/*********************************************************************
 *
 * ss_delete_from_frag_list
 *
 * This routine deletes an entry on the frag list on the given volume
 * if the entry matching the tag can be found.
 *
 * lock vdp->ssVolInfo.ssFragLk must be taken by caller
 ********************************************************************/

static void
ss_delete_from_frag_list(vdT *vdp,          /* in */
                         bfTagT tag,        /* in */
                         bfSetIdT bfSetId)  /* in */
{
    ssFragLLT *currp, *nextp; /* curr, next entries */
    ssFragHdrT *fhp;   /* pointer to frag list header */
    statusT sts;

    MS_SMP_ASSERT(vdp);
    MS_SMP_ASSERT(ADVMTX_VOLINFO_FRAG_OWNED(&vdp->ssVolInfo.ssFragLk));
    fhp = &vdp->ssVolInfo.ssFragHdr;

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        VFAST_STATS ( ss_delete_from_frag_list_cnt_zero );
        return;
    }

    currp = fhp->ssFragRatFwd;

    /* search list for file
     * if found, delete it
     */

    while ( currp != (ssFragLLT *)fhp ) {
        nextp = currp->ssFragRatFwd;
        /* see if file is anywhere on list  delete it */
        if((BS_BFTAG_EQL( tag, currp->ssFragTag )) &&
           (BS_BFS_EQL( bfSetId, currp->ssFragBfSetId))) {
            currp->ssFragRatFwd->ssFragRatBwd = currp->ssFragRatBwd;
            currp->ssFragRatBwd->ssFragRatFwd = currp->ssFragRatFwd;
            currp->ssFragRatFwd = currp->ssFragRatBwd = 0;
            ms_free(currp); fhp->ssFragListCnt--;
        }
        currp = nextp;  /* Keep going! */
    }

    if ( fhp->ssFragListCnt < SS_INIT_FRAG_LST_SZ ) {
        /* less than maximum entries in list, reset threshhold to something large*/
        fhp->ssFragHdrThresh = 0x7fffffffffffffff;
    }
    return;
}


/*********************************************************************
 *
 * This routine returns the worst fragmented file in the list(first
 * one).  If none in list, returns NULL.
 *
 * lock vdp->ssVolInfo.ssFragLk protects this routine.
 ********************************************************************/

static ssFragLLT *
ss_select_frag_file(vdT *vdp)           /* in - vd */
{
    ssFragLLT    *fp;  /* local entry pointer */
    ssFragHdrT   *fhp;

    MS_SMP_ASSERT(vdp);
    ADVMTX_VOLINFO_FRAG_LOCK(&vdp->ssVolInfo.ssFragLk);
    fhp = &vdp->ssVolInfo.ssFragHdr;
    MS_SMP_ASSERT(fhp);

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);
        VFAST_STATS ( ss_select_frag_file_list_cnt_zero );
        return(NULL);
    }

    fp = fhp->ssFragRatFwd;

    if((fp != (ssFragLLT *)fhp ) &&
        (BS_BFTAG_REG(fp->ssFragTag))) {
        /* found the worst one in list */
        ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);
        return(fp);
    }

    ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);
    return(NULL);
}


/*********************************************************************
 *
 * This routine deallocates the frag list for the given vd.
 *
 ********************************************************************/

static void
ss_dealloc_frag_list(vdT *vdp)           /* in */
{
    ssFragHdrT *fhp;
    ssFragLLT *currp, *nextp;  /* entry pointer */

    MS_SMP_ASSERT(vdp);
    ADVMTX_VOLINFO_FRAG_LOCK(&vdp->ssVolInfo.ssFragLk);
    fhp = &vdp->ssVolInfo.ssFragHdr;
    MS_SMP_ASSERT(fhp);

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        VFAST_STATS ( ss_dealloc_frag_list_cnt_zero );
        ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);
        return;
    }

    currp = fhp->ssFragRatFwd;

    while (currp != (ssFragLLT *)fhp ) {
        nextp = currp->ssFragRatFwd;
        currp->ssFragRatFwd->ssFragRatBwd = currp->ssFragRatBwd;
        currp->ssFragRatBwd->ssFragRatFwd = currp->ssFragRatFwd;
        if(currp)  ms_free(currp);  
        fhp->ssFragListCnt--;
        currp = nextp;
    }
    MS_SMP_ASSERT(vdp->ssVolInfo.ssFragHdr.ssFragRatFwd  ==
          (struct ssFragEntry *)(&vdp->ssVolInfo.ssFragHdr));
    MS_SMP_ASSERT(vdp->ssVolInfo.ssFragHdr.ssFragRatBwd  == 
          (struct ssFragEntry *)(&vdp->ssVolInfo.ssFragHdr));

    ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);
    return;
}

/********   end of frag list routines  *******/

/********   start of pack list routines  *******/

/*********************************************************************
 *
 * This routine deallocates the complete pack list.
 *
 ********************************************************************/

/* deletes the pack list */
void
ss_dealloc_pack_list(vdT *vdp)           /* in */
{
    ssPackHdrT *php;
    ssPackLLT *currp, *nextp;  /* entry pointer */

    MS_SMP_ASSERT(vdp);
    php = &vdp->ssVolInfo.ssPackHdr;
    MS_SMP_ASSERT(php);

    /* check for a null list first! */
    if(php->ssPackListCnt == 0) {
        VFAST_STATS ( ss_dealloc_pack_list_cnt_zero );
        return;
    }
    currp = php->ssPackFwd;

    while (currp != (ssPackLLT *)php ) {
        nextp = currp->ssPackFwd;
        currp->ssPackFwd->ssPackBwd = currp->ssPackBwd;
        currp->ssPackBwd->ssPackFwd = currp->ssPackFwd;
        if(currp)  ms_free(currp);
        php->ssPackListCnt--;
        currp = nextp;
    }
    MS_SMP_ASSERT( vdp->ssVolInfo.ssPackHdr.ssPackFwd  == 
          (struct ssPackEntry *)(&vdp->ssVolInfo.ssPackHdr));
    MS_SMP_ASSERT( vdp->ssVolInfo.ssPackHdr.ssPackBwd  ==
          (struct ssPackEntry *)(&vdp->ssVolInfo.ssPackHdr));
    return;
}

/********   end of pack list routines  *******/

/********   start of hot list routines  *******/


/*********************************************************************
 *
 * This routine sends messages to the list threads.  The messages
 * tell the list threads to update the hot list items with the data
 * contained in the message.
 *
 ********************************************************************/

void
ss_snd_hot (
          ssListMsgTypeT msgType, /* in - miss or write i/o counter */
          bfAccessT *bfap         /* in */
           )
{
    int32_t wday;
    ssListMsgT *listmsg;

    MS_SMP_ASSERT(bfap);

    /*
     * This is a direct io enabled file, and vfast is not scheduled to perform
     * on direct io enabled files.
     */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
       (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO) ||
        (bfap->dioCnt)) {
        VFAST_STATS ( ss_snd_hot_dio );
        return;
    }

    switch ( msgType ) {

        case HOT_REMOVE:

            /* send a message to the vfast hot list */
           if((listmsg = (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
               listmsg->msgType = HOT_REMOVE;
               listmsg->tag = bfap->tag;
               listmsg->bfSetId = bfap->bfSetp->bfSetId;
               msgq_send_msg(ssListQH, listmsg);
           } else {
               ADVMTX_SSDOMAIN_LOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
               bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
               ADVMTX_SSDOMAIN_UNLOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
           }
           break;

        case HOT_ADD_UPDATE_MISS:
        case HOT_ADD_UPDATE_WRITE:
            /* send a message to the vfast hot list */
            if((listmsg = 
               (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
                listmsg->msgType = HOT_ADD_UPDATE_MISS;
                listmsg->tag = bfap->tag;
                listmsg->bfSetId = bfap->bfSetp->bfSetId;
                listmsg->hot.vdi = FETCH_MC_VOLUME(bfap->primMCId);
                for(wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                    listmsg->hot.cnt[wday]  = bfap->ssHotCnt[wday];
                    /* reset counter */
                    bfap->ssHotCnt[wday] = 0;
                }
                listmsg->hot.size  = bfap->file_size;

                msgq_send_msg(ssListQH, listmsg);

            } else {
                ADVMTX_SSDOMAIN_LOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
                bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
                ADVMTX_SSDOMAIN_UNLOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
            }
            break;

        default:
            break;
    }

    /* now update the messages sent counter on the domain.  This
     * counter is used to determine which dmn is sending too 
     * many messages so that the number can be reduced on the correct
     * domain so that the end result is less messages sent to the q.
     */
    ADVMTX_SSDOMAIN_LOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
    bfap->dmnP->ssDmnInfo.ssDmnListMsgSendCnt++;
    ADVMTX_SSDOMAIN_UNLOCK( &bfap->dmnP->ssDmnInfo.ssDmnLk );
    return;
}

/*********************************************************************
 *
 * This routine swaps two entries in the linked list.
 *
 ********************************************************************/
void
ss_swap_entries(ssHotHdrT *hhp,
                ssHotLLT *left,
                ssHotLLT *right)
{
    ssHotLLT *templeftBwd;
    ssHotLLT *templeftFwd;

    if(left->ssHotFwd == right) {
        /* entries are adjacent to each other */
        left->ssHotBwd->ssHotFwd = right;
        right->ssHotFwd->ssHotBwd = left;

        left->ssHotFwd = right->ssHotFwd;
        right->ssHotBwd = left->ssHotBwd;

        left->ssHotBwd = right;
        right->ssHotFwd = left;
    } else {
        /* entries are not adjacent to each other in the list */
        templeftBwd = left->ssHotBwd;
        templeftFwd = left->ssHotFwd;

        left->ssHotBwd->ssHotFwd = right;
        right->ssHotBwd->ssHotFwd = left;

        left->ssHotFwd->ssHotBwd = right;
        right->ssHotFwd->ssHotBwd = left;

        left->ssHotBwd = right->ssHotBwd;
        right->ssHotBwd = templeftBwd;

        left->ssHotFwd = right->ssHotFwd;
        right->ssHotFwd = templeftFwd;
    }

    return;
}

/*********************************************************************
 *
 * This routine reorders the linked list passed in by descending values.
 * ---------------------------------------------------
 * |  ...   | left  |  right  |  rightnext  | .....  |
 * ---------------------------------------------------
 ********************************************************************/
void
ss_insertsort( ssHotHdrT *hhp)
{
    ssHotLLT *left,*right,*rightnext;
    int64_t leftTotal, leftBwdTotal;
    int32_t wday;

    right = hhp->ssHotFwd;

    while(1) {
        rightnext = right->ssHotFwd;
        left = right;

        while(1) {

            if(left == hhp->ssHotFwd)
                break;

            leftTotal = leftBwdTotal =0;
            for(wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                /* count may span days, so update all days */
                leftTotal += left->ssHotIOCnt[wday];
                leftBwdTotal += left->ssHotBwd->ssHotIOCnt[wday];
            }

            if(leftTotal > leftBwdTotal)
                ss_swap_entries(hhp,left->ssHotBwd,left);
            else
                break;
        }

        right = rightnext;
        if(rightnext == (ssHotLLT *)hhp)
            break;       /* done */
    }

    return;
}

/*********************************************************************
 *
 * This routine inserts an entry in the hot list.  The hot list for
 * each activated domain is sorted by the number of reads and writes
 * combined.
 *
 ********************************************************************/

static void
ss_insert_hot_list(
         ssListMsgTypeT msgType, /* in */
         ssHotLLT *hp)           /* in  - entry */
{
    domainT *dmnP;
    bfSetT *bfSetp;
    ssHotLLT *currp, *nextp; /* curr entry ptr */
    ssHotHdrT *hhp;   /* pointer to hot list header */
    struct timeval new_time;
    statusT sts;
    int32_t updated  = FALSE, 
            inserted = FALSE,
            setOpen  = FALSE,
            wday,
            newday;
    uint64_t newhpCnt, currhpCnt;

    /* Now open the set */
    sts = bfs_open( &bfSetp, hp->ssHotBfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            VFAST_STATS ( ss_insert_hot_list_bfs_valid_err );
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
        if(hp) ms_free(hp);
        VFAST_STATS ( ss_insert_hot_list_rbf_bfs_open_err );
        goto _cleanup;
    } 

    /* Check dmnState here again, discard msg if state has changed 
     * since this message was sent.
     */
    if( (SS_is_running == FALSE) ||
        (dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (dmnP->mountCnt == 0) ||
          ! (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND  ||
             dmnP->ssDmnInfo.ssDmnState == SS_CFS_MOUNT  ||
             dmnP->ssDmnInfo.ssDmnState == SS_CFS_UMOUNT)) {
        if(hp) ms_free(hp);
        VFAST_STATS ( ss_insert_hot_list_sanity_err );
        goto _cleanup;
    }

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    currp = hhp->ssHotFwd;


    /* check for a new day of the week */
    new_time = get_system_time();
    GETCURRWDAY(new_time.tv_sec,newday);
    if(dmnP->ssDmnInfo.ssDmnHotCurrDay != newday) {
        /* if day has flipped to new day, clear new day
         * for all entries, reorder list
         */
        while( currp != (ssHotLLT *)hhp )  {
            currp->ssHotIOCnt[newday] = 0;
            nextp = currp->ssHotFwd;

            /* if the flip caused there to be a zero count,
             * delete this entry.
             */
            for(currhpCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                currhpCnt += currp->ssHotIOCnt[wday];
            }

            if(currhpCnt == 0) {
                currp->ssHotFwd->ssHotBwd = currp->ssHotBwd;
                currp->ssHotBwd->ssHotFwd = currp->ssHotFwd;
                currp->ssHotFwd = currp->ssHotBwd = 0;
                ms_free(currp); hhp->ssHotListCnt--;
            }
            currp = nextp;  /* Keep going! */
        }
        dmnP->ssDmnInfo.ssDmnHotCurrDay = newday;
        ss_insertsort(hhp); /* reorder list */
    }

    /* Search list for a duplicate which may be on a diff vdi.
     * If found, update it for the new vd and hit counts and then
     * remove it.  List is ordered by combined counts.
     */
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    currp = hhp->ssHotFwd;
    while(( hhp->ssHotListCnt != 0) &&
          ( currp != (ssHotLLT *)hhp ))  {

        /* if file is anywhere on list, remove it here and reinsert
         * below in its proper sorted order.
         */
        if((BS_BFTAG_EQL(hp->ssHotTag,currp->ssHotTag)) &&
           (BS_BFS_EQL( bfSetp->bfSetId, currp->ssHotBfSetId))) {

            /* found one in list that is a match, update it */
            for(wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                /* count may span days, so update all days */
                currp->ssHotIOCnt[wday] += hp->ssHotIOCnt[wday];
            }
            currp->ssHotVdi = hp->ssHotVdi;
            currp->ssHotFileSize = hp->ssHotFileSize;

            /* now remove it from the list, free one passed in */
            currp->ssHotFwd->ssHotBwd = currp->ssHotBwd;
            currp->ssHotBwd->ssHotFwd = currp->ssHotFwd;
            currp->ssHotFwd = currp->ssHotBwd = 0;
            ms_free(hp); /* free the one passed in */
            hhp->ssHotListCnt--;
            hp = currp;
            updated = TRUE;
            break;
        }
        currp = currp->ssHotFwd;  /* Keep going! */
    }
    
    hp->ssHotLastUpdTime = new_time.tv_sec;
    if(!updated)
        hp->ssHotEntryTime = new_time.tv_sec;

    /* insert hp back in order */
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    currp = hhp->ssHotFwd;
    for(newhpCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
            newhpCnt += hp->ssHotIOCnt[wday];
    }
    while ( currp != (ssHotLLT *)hhp )  {

        for(currhpCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
            currhpCnt += currp->ssHotIOCnt[wday];
        }

        if (newhpCnt > currhpCnt) {
            /* insert before currp */
            hp->ssHotFwd = currp;
            hp->ssHotBwd = currp->ssHotBwd;
            currp->ssHotBwd->ssHotFwd = hp;
            currp->ssHotBwd = hp;

            hhp->ssHotListCnt++;
            inserted = TRUE;
            break;
        }
        currp = currp->ssHotFwd;  /* Keep going! */
    }

    if(!inserted) {
        /* goes on end of list */
        hp->ssHotFwd = (ssHotLLT *) hhp;
        hp->ssHotBwd = hhp->ssHotBwd;
        hhp->ssHotBwd->ssHotFwd = hp;
        hhp->ssHotBwd = hp;
        hhp->ssHotListCnt++;
    }

    if ( hhp->ssHotListCnt > 
            (SS_INIT_HOT_LST_GROW_SZ * dmnP->vdCnt) ) {
        /* too many entries in list, 
         * delete the one that has the oldest update time 
         */
        ssHotLLT *delp;
        time_t saved_t ;
        saved_t = 0x7fffffff;

        currp = hhp->ssHotFwd;
        
        /* loop through list and find the oldest */
        while( ( hhp->ssHotListCnt != 0) &&
               ( currp != (ssHotLLT *)hhp ) ) {
            if(currp->ssHotLastUpdTime < saved_t) {
                saved_t  = currp->ssHotLastUpdTime;
                delp = currp;
            }
            currp = currp->ssHotFwd;  /* Keep going! */
        }

        delp->ssHotFwd->ssHotBwd = delp->ssHotBwd;
        delp->ssHotBwd->ssHotFwd = delp->ssHotFwd;
        delp->ssHotFwd = delp->ssHotBwd = 0;
        ms_free(delp); hhp->ssHotListCnt--;

    }

    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );

_cleanup:

    if (setOpen) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        setOpen = FALSE;
    }
    return;
}



/*********************************************************************
 * ss_rmvol_from_hotlst - reduces hot list entries on domain where vol
 *                        is being removed.
 *
 * 1. remove entries from hot list that belong to vd being removed.
 * 2. reduce number of entries in list to new number allowed.
 *
 * Domain table lock is held from caller via the DmnTblLock.
 ********************************************************************/

void
ss_rmvol_from_hotlst( domainT *dmnP,   /* in */
                      vdIndexT delVdi )/* in */
{
    bfSetT *bfSetp;
    statusT sts;
    ssHotLLT *currp, *nextp; /* curr hot list entry ptr */
    ssHotHdrT *hhp=NULL;   /* pointer to hot list header */
    ssHotLLT *delp;  /* entry to delete */
    time_t saved_t;

    /* domain state is irrelavant, cleanup list if domain is in
     * any state.
     */
    MS_SMP_ASSERT(dmnP);
    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;

    /* check for a null list first! */
    if(hhp->ssHotListCnt == 0) {
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
        VFAST_STATS ( ss_rmvol_from_hotlist_list_zero );
        return;
    }
    currp = hhp->ssHotFwd;

    /* search list for file located on volume
     * if found, delete it
     */
    while(currp != (ssHotLLT *)hhp) {
        nextp = currp->ssHotFwd;
        if(delVdi == currp->ssHotVdi) {
            /* delete vdi match entries */
            currp->ssHotFwd->ssHotBwd = currp->ssHotBwd;
            currp->ssHotBwd->ssHotFwd = currp->ssHotFwd;
            currp->ssHotFwd = currp->ssHotBwd = 0;
            ms_free(currp); hhp->ssHotListCnt--;
        }
        currp = nextp;  /* Keep going! */
    }

    /* now, if there is still too many entries on list, 
     * remove the oldest entries 
     */
    while ( hhp->ssHotListCnt > 
            (SS_INIT_HOT_LST_GROW_SZ * dmnP->vdCnt) ) {
        /* too many entries in list, 
         * delete the oldest update time entries 
         */
        hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
        currp = hhp->ssHotFwd;
        saved_t = 0x7fffffff;
        delp = NULL;
        
        /* loop through list and find the oldest */
        while( ( hhp->ssHotListCnt != 0) &&
               ( currp != (ssHotLLT *)hhp ) ) {
            if(currp->ssHotLastUpdTime < saved_t) {
                saved_t  = currp->ssHotLastUpdTime;
                delp = currp;
            }
            currp = currp->ssHotFwd;  /* Keep going! */
        }
        MS_SMP_ASSERT(delp!=NULL);
        delp->ssHotFwd->ssHotBwd = delp->ssHotBwd;
        delp->ssHotBwd->ssHotFwd = delp->ssHotFwd;
        delp->ssHotFwd = delp->ssHotBwd = 0;
        ms_free(delp); hhp->ssHotListCnt--;
    }
    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    return;
} /* end ss_rmvol_from_hotlst */



/*********************************************************************
 *
 * This routine deletes the entry passed in from the hot list if the
 * entries tag is on the list.
 *
 ********************************************************************/

static void
ss_del_from_hot_list(ssHotLLT *hp)
{
    domainT *dmnP;
    bfSetT *bfSetp;
    ssHotLLT *currp, *nextp; /* curr, next entry ptrs */
    ssHotHdrT *hhp;   /* pointer to hot list header */
    int32_t setOpen = FALSE;
    statusT sts;

    /* open the set */
    sts = bfs_open( &bfSetp, 
                        hp->ssHotBfSetId,  
                        BFS_OP_DEF, 
                        FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            VFAST_STATS ( ss_del_from_hot_list_bfset_invalid );
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
        VFAST_STATS ( ss_del_from_hot_list_rbf_bfs_open_err );
        goto _cleanup;
    } 

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    currp = hhp->ssHotFwd;

    /* search list for file
     * if found, delete it
     */

    while( ( hhp->ssHotListCnt != 0) &&
           ( currp != (ssHotLLT *)hhp ) ) {
        nextp = currp->ssHotFwd;
        /* see if file is anywhere on list  delete it */
        if((BS_BFTAG_EQL(hp->ssHotTag,currp->ssHotTag)) &&
           (BS_BFS_EQL( bfSetp->bfSetId,currp->ssHotBfSetId))) {

            currp->ssHotFwd->ssHotBwd = currp->ssHotBwd;
            currp->ssHotBwd->ssHotFwd = currp->ssHotFwd;
            currp->ssHotFwd = currp->ssHotBwd = 0;
            ms_free(currp); hhp->ssHotListCnt--;
        }
        currp = nextp;  /* Keep going! */
    }

    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );

_cleanup:

    if(hp) ms_free(hp);

    if (setOpen) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        setOpen = FALSE;
    }
    return;
}

/*********************************************************************
 *
 * This routine performs two different tasks, depending upon the flag.
 * First it searches for the domain's hot list for tag.
 *
 * Next, if flag:
 * SS_READ - If tag in hot file list and has been placed already, 
 *           defragment the file on same vol as its been placed by 
 *           prior hot list processing.
 * SS_UPDATE - update ssPlaced in hot list to vd passed in.
 *             vd < 0 : srcVdIndex contains the error from migrate.
 *
 ********************************************************************/

static 
int
ss_chk_hot_list ( domainT  *dmnP,       /* in */
                  bfTagT   tag,         /* in */
                  bfSetIdT bfSetId,     /* in */
                  vdIndexT srcVdIndex,  /* in */
                  statusT  err,         /* in */
                  int32_t  flag )       /* in */
{
    ssHotHdrT *hhp;
    ssHotLLT  *hp=NULL;
    struct timeval new_time;

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    MS_SMP_ASSERT(hhp);
    MS_SMP_ASSERT(flag != 0);

    if ( hhp->ssHotListCnt == 0) {
        ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
        VFAST_STATS ( ss_chk_hot_list_cnt_zero );
        return (-1);
    }

    hp = hhp->ssHotFwd;
    while( hp != (ssHotLLT *)hhp ) {
        if((BS_BFTAG_EQL( tag, hp->ssHotTag )) &&
           (BS_BFS_EQL( bfSetId, hp->ssHotBfSetId))) {
            /* found the entry */

            if(flag==SS_UPDATE) {
                if(err != EOK) {
                    if(err == ENOSPC) err = ENO_MORE_BLKS;
                    hp->ssHotPlaced = err;
                    new_time = get_system_time();
                    hp->ssHotErrTime = new_time.tv_sec;
                } else
                    hp->ssHotVdi = hp->ssHotPlaced = srcVdIndex;
                ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
                return(srcVdIndex);
            } 

            if(flag==SS_READ) {
                if(hp->ssHotPlaced > 0) {
                    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
                    return(hp->ssHotPlaced);
                } else {
                    /* not a currently active hot IO placed entry */
                    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
                    VFAST_STATS ( ss_chk_hot_list_not_active_hot_io );
                    return (-1);
                }
            }

        } /* end processing of matched entry */
        hp = hp->ssHotFwd;
    } /* end loop entries in list */

    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    VFAST_STATS ( ss_chk_hot_list_no_entry );
    return(-1);
}


/*********************************************************************
 *
 * This routine deallocates all the entries on the hot list for the
 * given domain.
 *
 ********************************************************************/

static void
ss_dealloc_hot(domainT *dmnP)  /* in - pointer to header for list */
{
    ssHotHdrT *hhp;
    ssHotLLT *currp, *nextp;  /* entry pointer */

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    MS_SMP_ASSERT(hhp);
    currp = hhp->ssHotFwd;

    /* check for a null list first! */
    if(hhp->ssHotListCnt == 0) {
        ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
        VFAST_STATS ( ss_dealloc_hot_list_cnt_zero );
        return;
    }

    while( currp != (ssHotLLT *)hhp ) {
        nextp = currp->ssHotFwd;
        currp->ssHotFwd->ssHotBwd = currp->ssHotBwd;
        currp->ssHotBwd->ssHotFwd = currp->ssHotFwd;
        if(currp)  ms_free(currp);
        hhp->ssHotListCnt--;
        currp = nextp;
    }
    MS_SMP_ASSERT(dmnP->ssDmnInfo.ssDmnHotHdr.ssHotFwd ==
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr));
    MS_SMP_ASSERT(dmnP->ssDmnInfo.ssDmnHotHdr.ssHotBwd ==
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr));

    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    return;
}

/********   end of hot list routines  *******/


/********   start of work routines  *******/



/*********************************************************************
 *
 * This routine sends a start message to a volume if one has not
 * already been posted and there is work to be done.
 * It also unparks a migrate on the volume if one has been parked.
 *
 ********************************************************************/

/* check for vfast enabled for processing on this domain */
void
ss_startios(vdT *vdp)
{
    ssWorkMsgT *workmsg=NULL;
    int idle;

    if( (SS_is_running == TRUE) &&
        (vdp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
        (vdp->dmnP->state == BFD_ACTIVATED) &&
        (vdp->dmnP->dmnFlag != BFD_DEACTIVATE_IN_PROGRESS) &&
        (vdp->dmnP->dmnFlag != BFD_DEACTIVATE_PENDING) &&
        (vdp->dmnP->mountCnt > 0) &&
        (vdp->dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE ||
         vdp->dmnP->ssDmnInfo.ssDmnDefragment == TRUE ||
         vdp->dmnP->ssDmnInfo.ssDmnDefragAll  == TRUE ||
         vdp->dmnP->ssDmnInfo.ssDmnBalance == TRUE) ) {

        ADVSPL_VOLINFO_MSG_LOCK( &vdp->ssVolInfo.ssVdMsgLk );
        idle = (vdp->ssVolInfo.ssVdMsgState == SS_MSG_IDLE) &&
               (vdp->ssVolInfo.ssFragHdr.ssFragListCnt ||
                vdp->ssVolInfo.ssVdHotTag.tag_num);
        ADVSPL_VOLINFO_MSG_UNLOCK( &vdp->ssVolInfo.ssVdMsgLk );

        if (idle) {
            if((workmsg = (ssWorkMsgT *)msgq_alloc_msg(ssWorkQH)) != NULL) {
                ADVSPL_VOLINFO_MSG_LOCK( &vdp->ssVolInfo.ssVdMsgLk );
                idle = (vdp->ssVolInfo.ssVdMsgState == SS_MSG_IDLE);
                /* 
                 * If multiple threads successfully allocate a message,
                 * the one that takes the spin lock first will send it,
                 * any others would simply free it.
                 */
                vdp->ssVolInfo.ssVdMsgState = SS_POSTED;
                ADVSPL_VOLINFO_MSG_UNLOCK( &vdp->ssVolInfo.ssVdMsgLk );
                if (idle) {
                    workmsg->msgType = SS_VD_RUN;
                    workmsg->vdIndex = vdp->vdIndex;
                    workmsg->domainId = vdp->dmnP->domainId;
                    msgq_send_msg(ssWorkQH, workmsg);
                } else {
                    msgq_free_msg(ssWorkQH, workmsg);
                }
            } /* else just drop it - queue is full */
        }

        if (vdp->ssVolInfo.ssVdMigState == SS_PARKED) {
            cv_broadcast (&vdp->ssVolInfo.ssContMigCV, NULL, CV_NULL_LOCK);
        }

    } /* end if vfast is enabled */
    return;
}

/*********************************************************************
 *
 * This routine is called after a vfast work cycle if there is 
 * some other outstanding IOs.   The thread blocks and waits to be
 * awoken by bs_osf_complete->ss_startios call sequence.  The purpose
 * of this routine is to keep vfast from doing too much work at
 * once, thus using up too much IO bandwidth.  Smaller slices of work
 * time == less impact upon performance.
 *
 ********************************************************************/

statusT
ss_block_and_wait(vdT *vdp)
{
    /* allow any other threads to run */
    preemption_point();

    /* The use of vdIoOut replaced the checking of Io Queue lengths */
    if((vdp->ssVolInfo.ssVdMigState != SS_ABORT) &&
       (SS_is_running == TRUE) &&
       (vdp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
       !(vdp->vdIoOut)) {
        /* keep going, no other work is being done */
        vdp->ssVolInfo.ssIOCnt=0;
        VFAST_STATS ( ss_block_and_wait_no_block );
        return(EOK);
    }

    /* check to see if vd is being deactivated or
     * vfast is being stopped before parking. 
     */
    ADVMTX_SS_LOCK(&SSLock );
    ADVMTX_SSVDMIG_LOCK( &vdp->ssVolInfo.ssVdMigLk );
    if((vdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (vdp->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        ADVMTX_SSVDMIG_UNLOCK( &vdp->ssVolInfo.ssVdMigLk );
        ADVMTX_SS_UNLOCK(&SSLock );
        VFAST_STATS ( ss_block_and_wait_would_block );
        return(E_WOULD_BLOCK);
    }

    /* Smartstore work thread blocks and waits for next vd inactivity 
     * broadcast from io thd. 
     */
    vdp->ssVolInfo.ssVdMigState = SS_PARKED;
    cv_wait( &vdp->ssVolInfo.ssContMigCV, &vdp->ssVolInfo.ssVdMigLk,
                    CV_MUTEX, CV_DFLT_FLG );
    ADVMTX_SS_UNLOCK(&SSLock );
    SS_TRACE(vdp,0,0,0,0);

    /* check vd for deactivation before continuing to migrate */
    if((vdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (vdp->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        ADVMTX_SSVDMIG_UNLOCK( &vdp->ssVolInfo.ssVdMigLk );
        VFAST_STATS ( ss_block_and_wait_no_block_2 );
        return(E_WOULD_BLOCK);
    }

    vdp->ssVolInfo.ssVdMigState = SS_PROCESSING;
    ADVMTX_SSVDMIG_UNLOCK( &vdp->ssVolInfo.ssVdMigLk );
    return(EOK);
}

/*********************************************************************
 *
 * This routine is called to open a fileset and access a files access
 * structure.  Either both work or error is returned.
 *
 ********************************************************************/

statusT
ss_open_file(
            bfSetIdT bfSetId,     /* in */
            bfTagT   filetag,     /* in */
            bfSetT    **retbfSetp,  /* out */
            bfAccessT **retbfap,    /* out */
            int *retmounted         /* out */
            )
{
    statusT sts=EOK;
    struct vnode *vp;
    bfAccessT *bfap=NULL;
    bfSetT *bfSetp=NULL;
    struct fileSetNode *fsnp=NULL;
    int32_t closeBfSetFlag   = FALSE, 
            closeBitfileFlag = FALSE,
            do_vrele         = FALSE;

    /* open the fileset */
    sts = bfs_open( &bfSetp,
                        bfSetId,
                        BFS_OP_DEF,
                        FtxNilFtxH );
    if (sts != EOK) {
        VFAST_STATS ( ss_open_file_rbf_bfs_open_err );
	goto HANDLE_EXCEPTION;
    }
    closeBfSetFlag = TRUE;
    if ( ! BFSET_VALID(bfSetp) ) {
        VFAST_STATS ( ss_open_file_bfset_invalid );
	sts = E_NO_SUCH_BF_SET;
	goto HANDLE_EXCEPTION;
    }

    /* Access the bitfile, if its being deleted, sts != EOK 
     * ALWAYS get a vnode, mounted or not! 
     * Get it a different way depending on whether fset is
     * mounted or not.
     */
    vp = NULL;
    fsnp = (struct fileSetNode *)bfSetp->fsnp;
    if(fsnp) {
        /* fset mounted - get vnode with mount, do vrele */
        sts = bf_get(
                 filetag,
                 0,
                 DONT_UNLOCK,
                 fsnp,
                 &vp,
                 NULL,
                 NULL
                 );
        if (sts != EOK) {
            /* file was deleted, lets just stop and cleanup */
            VFAST_STATS ( ss_open_file_bf_get_err );
	    goto HANDLE_EXCEPTION;
        }
        do_vrele = TRUE;
        bfap = VTOA((struct vnode *) (vp));
        bfSetp = bfap->bfSetp;
    } else {
        /* fset unmounted - get vnode without mount, do bs_close */
        sts = bs_access( &bfap,
                     filetag,
                     bfSetp,
                     FtxNilFtxH,
                     BF_OP_INTERNAL, /* no vnode when not mounted */
                     &vp);
        if (sts != EOK) {
            /* file was deleted, lets just stop and cleanup */
            VFAST_STATS ( ss_open_file_bs_access_err );
	    goto HANDLE_EXCEPTION;
        }
        do_vrele = FALSE;
    }
    closeBitfileFlag = TRUE;

    *retbfap   = bfap;
    *retbfSetp = bfSetp;
    *retmounted = do_vrele;
    return(EOK);

HANDLE_EXCEPTION:

    if (closeBitfileFlag == TRUE) {
        if(do_vrele == TRUE) {
            VN_RELE(&bfap->bfVnode);
        } else {
            (void) bs_close(bfap, MSFS_SS_NOCALL);
        }
    }

    if (closeBfSetFlag == TRUE) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }
    return(sts);
}


/*********************************************************************
 *
 * This routine is called to close a fileset and a file's access
 * structure.
 *
 ********************************************************************/

statusT
ss_close_file(
            bfSetT    *bfSetp,  /* in */
            bfAccessT *bfap,     /* in */
            int fsetMounted
            )
{
    MS_SMP_ASSERT(bfap);

    if(fsetMounted) {
        VN_RELE(&bfap->bfVnode);
    } else{
        (void) bs_close(bfap, MSFS_SS_NOCALL);
    }

    MS_SMP_ASSERT(bfSetp);

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    return(EOK);
}


/*********************************************************************
 *
 * This routine determines the file to move for this volume. It calls
 * ss_vd_migrate to actually move the file.
 *
 ********************************************************************/

static void
ss_move_file( vdIndexT vdi,      /* in */
              bfDomainIdT domainId,  /* in */
              ssWorkMsgTypeT msgType
            )
{
    vdT *svdp=NULL;
    domainT *dmnP=NULL;
    statusT sts=EOK;
    ssFragLLT *fp=NULL;  /* frag file ptr */
    bfTagT  filetag=NilBfTag;
    bfSetIdT bfSetId=nilBfSetId;
    int32_t alloc_hint  = BS_ALLOC_DEFAULT,
            domain_open = FALSE, 
            vdRefed     = FALSE,
            vdiPlaced;
    vdIndexT dstVdIndex;

    /* bump the access count on the domain so it can't go away
     * while we are working in it
     */
    if ( sts = bs_domain_access(&dmnP, domainId, FALSE)) {
        /* domain can not be activated, just drop msg */
        VFAST_STATS ( ss_move_file_dmn_not_active );
	goto HANDLE_EXCEPTION;
    }
    domain_open = TRUE;

    /*
     * If the source vd is not valid, we are in the middle of a rmvol
     * and we want the rmvol to take precedence over vfast.  If the target
     * vd ends up being the same as the source vd (most common case), we
     * will fail later anyway.  
     */
    if ((sts = sc_valid_vd(dmnP, defServiceClass, vdi)) != EOK) {
        VFAST_STATS ( ss_move_file_src_vd_not_valid );
        /* bump the ref count on this vd so it cant be removed */
        svdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE);
        if ( svdp == NULL ) {
            /* must be an rmvol going on - just drop msg */
            VFAST_STATS ( ss_move_file_rmvol_inprog );
	    sts = EBAD_VDI;
	    goto HANDLE_EXCEPTION;
        }
        vdRefed = TRUE;
	goto HANDLE_EXCEPTION;
    }


    /* increment the dmn vfast thread count */
    ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
    dmnP->ssDmnInfo.ssDmnSSThdCnt++;
    ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );

    if ((SS_is_running == FALSE) ||
        (dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (dmnP->mountCnt == 0) ||
        (dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED))
    {
        VFAST_STATS ( ss_move_file_would_block );
	sts = E_WOULD_BLOCK;
	goto HANDLE_EXCEPTION;
    }

    /* bump the ref count on this vd so it cant be removed */
    svdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE);
    if ( svdp == NULL ) {
        /* must be an rmvol going on - just drop msg */
        VFAST_STATS ( ss_move_file_rmvol_inprog );
	sts = EBAD_VDI;
	goto HANDLE_EXCEPTION;
    }
    vdRefed = TRUE;

    /* Potentially the thread works on vd for a long time,
     * Increment the volume vfast thread count
     */
    ADVMTX_SSVDMIG_LOCK( &svdp->ssVolInfo.ssVdMigLk );
    svdp->ssVolInfo.ssVdSSThdCnt++;
    ADVMTX_SSVDMIG_UNLOCK( &svdp->ssVolInfo.ssVdMigLk );

    /* First check for a hot file move on svdp here.  Select a 
     * file to move by first checking to see if any hot files 
     * need moved. If no hot file is targeted at this vd default 
     * to the frag list check below.
     */
    ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
    if((dmnP->ssDmnInfo.ssDmnSmartPlace) &&
       (dmnP->ssDmnInfo.ssDmnHotWorking == FALSE) &&
       (!BS_BFTAG_EQL( svdp->ssVolInfo.ssVdHotTag, NilBfTag ))) {

        /* hot file to be moved to this volume. move it. */
        filetag = svdp->ssVolInfo.ssVdHotTag;
        bfSetId = svdp->ssVolInfo.ssVdHotBfSetId;
        /* block out any other threads */
        dmnP->ssDmnInfo.ssDmnHotWorking = TRUE;  
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );

        sts = ss_vd_migrate( filetag,
                             bfSetId,
                             svdp->vdIndex,
                             msgType,
                             0,
                             BS_ALLOC_MIG_SINGLEXTNT,
                             1  /* hot file */
                             );
#ifdef SS_DEBUG
        if (ss_debug > 1)
            printf("ss_vd_migrate:(hotfile) vd(%d) tag(%ld.%ld) returns sts=%d\n",
                   svdp->vdIndex, bfSetId.dirTag.tag_num, filetag.tag_num, sts);
#endif
        /* ENO_SUCH_TAG indicates file was deleted
         * E_NO_SUCH_BF_SET indicates fileset is being deleted by another thread
         * ENO_MORE_BLKS indicates not enough space to finish migrate command
         * E_WOULD_BLOCK indicates vfast was stopped in middle of a migrate
         * E_RSVD_FILE_INWAY - vfast unable to pack cause rsvd files inway.
         * ENOSPC - vfast unable to find or create enough space
         * E_RANGE_NOT_CLEARED real error, range was not cleared by mig_pack_vd_range
         * E_RANGE_NOT_MAPPED - file was truncated, but still defragmented
         *                     successfully  -or-  During a pack operation, an extent
         *                     of a file being packed was truncated and pack aborted.
         * ENO_MORE_MCELLS - File being packed is on DDL, abort since we cant move it
         */

        /* Update ssPlaced in hot list.
         * If there is an error moving file, set ssHotPlaced == -error 
         * so we don't try again and again in an infinite loop.
         */
        vdiPlaced = ss_chk_hot_list(svdp->dmnP,
                                    filetag, 
                                    bfSetId, 
                                    svdp->vdIndex,
                                    sts,
                                    SS_UPDATE );
        /* Even if migrate fails, clear out hot file so later 
         * processing of the hot list by the monitor thread may 
         * see a different target or conditions on the target 
         * may change.
         */
        svdp->ssVolInfo.ssVdHotTag = NilBfTag;
        svdp->ssVolInfo.ssVdHotBfSetId = nilBfSetId;
        ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk ); 
        dmnP->ssDmnInfo.ssDmnHotWorking = FALSE;
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );
        dmnP->ssDmnInfo.ssDmnHotPlaced = FALSE;
	sts = EOK;
	goto HANDLE_EXCEPTION;
    } 
    ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );

    if (dmnP->ssDmnInfo.ssDmnDefragment || dmnP->ssDmnInfo.ssDmnDefragAll) {

        /* select a fragmented file to move */
        if((fp = ss_select_frag_file(svdp)) == NULL) {
            /* simply return if there is nothing in list */
	    sts = EOK;
	    goto HANDLE_EXCEPTION;
        }
        filetag = fp->ssFragTag;
        bfSetId = fp->ssFragBfSetId;

        /* Delete entry from list.
         * If not done below the bs_access above, the bs_access call 
         * will simply cause the list threads to put the entry back 
         * on the list.
         */
        ADVMTX_VOLINFO_FRAG_LOCK(&svdp->ssVolInfo.ssFragLk);
        ss_delete_from_frag_list(svdp, filetag, bfSetId);
        ADVMTX_VOLINFO_FRAG_UNLOCK(&svdp->ssVolInfo.ssFragLk);

        /* if hot file is about to be placed as a hot file, skip it */
        if((BS_BFTAG_EQL( svdp->ssVolInfo.ssVdHotTag, filetag ))  &&
           (BS_BFS_EQL( svdp->ssVolInfo.ssVdHotBfSetId, bfSetId))) {

            sts = EOK;
	    goto HANDLE_EXCEPTION;
        }

        /* If tag in hot file list and has been placed already, 
         * defragment the file on same vol as its been placed by 
         * prior hot list processing.
         */
        dstVdIndex = svdp->vdIndex;
        vdiPlaced = ss_chk_hot_list (svdp->dmnP,
                                     filetag, 
                                     bfSetId, 
                                     svdp->vdIndex, 
                                     EOK,
                                     SS_READ );
        if(vdiPlaced != -1) {
            /* if hot file has been placed, keep it on same vd */
            alloc_hint = BS_ALLOC_MIG_SAMEVD;
            dstVdIndex = vdiPlaced;
        } else {
            alloc_hint = BS_ALLOC_DEFAULT;
        }
        sts = ss_vd_migrate( filetag,
                             bfSetId,
                             dstVdIndex,
                             msgType,
                             fp->ssXtntCnt,
                             alloc_hint,
                             0            /* fragmented file */
                             );
#ifdef SS_DEBUG
        if (ss_debug > 1)
            printf("ss_vd_migrate: vd(%d) tag(%ld.%ld) returns sts=%d\n",
                    svdp->vdIndex, bfSetId.dirTag.tag_num, filetag.tag_num, sts);
#endif
        /* ENO_SUCH_TAG indicates file was deleted
         * E_NO_SUCH_BF_SET indicates fileset is being deleted by another thread
         * ENO_MORE_BLKS indicates not enough space to finish migrate command
         * E_WOULD_BLOCK indicates vfast was stopped in middle of a migrate
         * E_RSVD_FILE_INWAY - vfast unable to pack cause rsvd files inway.
         * ENOSPC - vfast unable to find or create enough space
         * E_RANGE_NOT_CLEARED real error, range was not cleared by mig_pack_vd_range
         * E_RANGE_NOT_MAPPED - file was truncated, but still defragmented
         *                     successfully  -or-  During a pack operation, an extent
         *                     of a file being packed was truncated and pack aborted.
         * ENO_MORE_MCELLS - File being packed is on DDL, abort since we cant move it
         */
    } /* end defrag and balance */

HANDLE_EXCEPTION:

    if(vdRefed) {
        /* reset State to IDLE so that more messages can wake
         * up threads
         */
        ADVSPL_VOLINFO_MSG_LOCK( &svdp->ssVolInfo.ssVdMsgLk );
        if(svdp->ssVolInfo.ssVdMsgState != SS_STOP)
            svdp->ssVolInfo.ssVdMsgState = SS_MSG_IDLE;
        ADVSPL_VOLINFO_MSG_UNLOCK( &svdp->ssVolInfo.ssVdMsgLk );

        if((sts != EOK) &&  /*  !already stopped */
           (svdp->ssVolInfo.ssVdMsgState != SS_STOP) &&  /* !rmvol or vfast deactivate */
           (svdp->ssVolInfo.ssVdMigState != SS_ABORT) && /*        or dmn deactivate   */
           !(svdp->vdIoOut)) { /* vdIoOut replaces checking Io Queue lengths.  */
            /* no ios on normal queues, run vfast. */
            preemption_point();
            ss_startios(svdp);
            svdp->ssVolInfo.ssIOCnt=0;
        }

        /* decrement the volume vfast thread count */
        ADVMTX_SSVDMIG_LOCK( &svdp->ssVolInfo.ssVdMigLk );
        svdp->ssVolInfo.ssVdSSThdCnt--;
        if (svdp->ssVolInfo.ssVdSSThdCnt == 0 &&
            svdp->ssVolInfo.ssVdSSWaiters) {
            svdp->ssVolInfo.ssVdSSWaiters = 0;
            cv_broadcast(&svdp->ssVolInfo.ssVdSSWaitersCV, NULL, CV_NULL_LOCK);
        }
        ADVMTX_SSVDMIG_UNLOCK( &svdp->ssVolInfo.ssVdMigLk );

        vd_dec_refcnt(svdp);
    }

    if(domain_open) {
        /* decrement the dmn vfast thread count */
        ADVMTX_SSDOMAIN_LOCK( &dmnP->ssDmnInfo.ssDmnLk );
        dmnP->ssDmnInfo.ssDmnSSThdCnt--;
        if (dmnP->ssDmnInfo.ssDmnSSThdCnt == 0 &&
            dmnP->ssDmnInfo.ssDmnSSWaiters) {
            dmnP->ssDmnInfo.ssDmnSSWaiters = 0;
            cv_broadcast(&dmnP->ssDmnInfo.ssDmnSSWaitersCV, NULL, CV_NULL_LOCK);
        }
        ADVMTX_SSDOMAIN_UNLOCK( &dmnP->ssDmnInfo.ssDmnLk );

        bs_domain_close(dmnP);
    }
    return;
}


/*********************************************************************
 * Find largest chunk of contiguously free disk space.  Lock the
 * disk space and return.
 ********************************************************************/

static statusT
ss_find_space(
    vdT *vdp,                /* in - vd on which to find the free blocks */
    bf_vd_blk_t requestedBlkCnt, /* in - number of DEV_BSIZE blocks requested */
    bf_vd_blk_t *blkOffset,   /* out - ptr to new, clear sbm locked range */
    bf_vd_blk_t *blkCnt,   /* out - count of pages locked */
    int32_t flags)
{
    int32_t sbm_locked = FALSE;
    void *stgDesc=NULL;
    statusT sts=EOK;
    bf_vd_blk_t locblkOffset,
                locblkCnt;

    ADVFTM_VDT_STGMAP_LOCK(&vdp->stgMap_lk);
    sbm_locked = TRUE;

    /*
     * We don't have a specific file to get storage for, so make the minimum
     * value for sbm_find_space equal to the less of the user and meta
     * allocation units.
     */
    stgDesc = sbm_find_space( vdp,
                              requestedBlkCnt,
                              MIN(vdp->dmnP->userAllocSz, 
                                  vdp->dmnP->metaAllocSz),
                              0,
                              BS_ALLOC_MIG_SINGLEXTNT, /* try for all, get most */
                              &locblkOffset,
                              &locblkCnt);

    if(stgDesc == NULL)  {
        /* volume has NO free space */
        VFAST_STATS ( ss_find_space_no_space );
	sts = ENOSPC;
	goto HANDLE_EXCEPTION;
    }

    if(flags==TRUE) {
        /* lock the sbm location we may migrate src extents into */
        SS_TRACE(vdp,0,0,locblkOffset,locblkCnt);
        sts = sbm_lock_range (vdp,
                              locblkOffset,
                              locblkCnt );
        if(sts != EOK) {
            VFAST_STATS ( ss_find_space_sbm_lock_range_err );
	    goto HANDLE_EXCEPTION;
        }
    }

    ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
    sbm_locked = FALSE;

    *blkOffset = locblkOffset;
    *blkCnt = locblkCnt;
    return sts;

HANDLE_EXCEPTION:

    if(sbm_locked) {
        ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
    }
    return sts;
}


/*********************************************************************
 *
 * This routine gets the free space required to move a file.
 * 1. Try to find a single extent to hold requested fob count. 
 * 2. If amount found is less than amount needed, try to pack 
 *    files together in an effort to free a contiguous free chunk.
 * 3. If we can't pack it because the range is either too big,
 *    or there is a reserved file in the way, return as big a 
 *    free chunk as found on the  volume.
 * If not any free space, or the flag is set to BS_ALLOC_MIG_SINGLEXTNT
 * and we couldn't get all that was requested, return ENOSPC to caller.
 *
 ********************************************************************/

static statusT
ss_get_n_lk_free_space(
    vdT *vdp, /* in - vd on which to find the free blocks */
    bf_fob_t req_fob_cnt,    /* in - number of fobs requested */
    bf_vd_blk_t *allocBlkOffp,/* out - ptr to new, clear sbm locked range */
    bf_fob_t *alloc_fob_cnt, /* out - count of fobs locked */
    int32_t alloc_hint)  /* in - hint for type of allocation */

{
    bf_vd_blk_t newBlkCnt       = 0,
                scanBlkCnt      = 0,
                requestedBlkCnt = 0,
                scanBlkOffset   = 0,
                blkCnt          = 0,
                blkOffset       = 0,
                newBlkOffset    = 0,
                totFreeBlks     = 0,
                startClust      = 0,
                numClust        = 0,
                clustCnt        = 0;

    statusT sts = EOK;

    int sbm_range_locked = FALSE;

    /* (WITHOUT new Storage Reservation)
     * Try to find space for the requested number of fobs.
     * sbm_find_space() returns NULL if there is not enough
     * space on the virtual disk.  Otherwise, it returns,
     * in "blkCnt", the largest contiguous block that can be
     * allocated.  This value may or may not be large enough
     * to satisfy the request.
     */

    requestedBlkCnt = req_fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE;
    sts = ss_find_space(vdp,
                        requestedBlkCnt,
                        &blkOffset,
                        &blkCnt,
                        TRUE);
    if(sts != EOK) {
        VFAST_STATS ( ss_get_n_lk_free_space_ss_find_space_err );
	goto HANDLE_EXCEPTION;
    }

    totFreeBlks = vdp->freeClust*vdp->stgCluster;
    if( (blkCnt) &&
        (blkCnt < requestedBlkCnt) &&
        ((alloc_hint & BS_ALLOC_MIG_SINGLEXTNT) ||
        ( (totFreeBlks > (2*requestedBlkCnt)) &&
          ((totFreeBlks/SSPackRangePctFree) > blkCnt ))) ) {
        /*
         * We didn't find enough free space to hold the entire file.
         * We now try to create a large enough space by packing extents.
         * First we must scan the sbm to find a range of the sbm that
         * contains enough free space to hold the entire file.
         *
         * RULE 1: If this is a hot file, try to create it
         *         regardless of how much needs to be cleared.
         * RULE 2: Scan/Pack only if there is free space that is twice the
         *         size of the entire file.  The chances of being
         *         successful of packing anything less are not very good
         *         because:
         *         a. more likely to be stolen by system.
         *         b. more likely to encounter a rsvd file.
         *         c. becomes less efficent than combining extents.
         * RULE 3: Scan/Pack ranges when largest free extent found above is
         *         SSPackRangePctFree% or less of the total volume free space.
         * Note: SSPackRangePctFree is already a reciprocal percentage.
         */
        sts = EOK;
        MS_SMP_ASSERT(vdp->stgCluster);
        clustCnt = scanBlkOffset = 0;
        /*
         * sbm_scan_v3_v4 returns enough free blk in a range, or ENOSPC.
         * A new range can be re-locked in sbm_scan_v3_v4.
         */
        sts = sbm_scan_v3_v4 (
                              vdp,
                              (requestedBlkCnt / vdp->stgCluster),
                              0,
                              0,
                              0,
                              &clustCnt,
                              &scanBlkOffset,
                              0 /* parent */
                              );
        if(sts != EOK) {
            /* legit error, find what we can and return */
            sts = ss_find_space(vdp,
                                requestedBlkCnt,
                                &blkOffset,
                                &blkCnt,
                                TRUE);
            if(sts != EOK) {
                VFAST_STATS ( ss_get_n_lk_free_space_ss_find_space_err2 );
		goto HANDLE_EXCEPTION;
            } else {
		sts = EOK;
		goto HANDLE_EXCEPTION;
            }
        }

        if(((alloc_hint & BS_ALLOC_MIG_SINGLEXTNT) ||
             (clustCnt <= (vdp->vdClusters/SSPackRangePctMaxSize)))) {
            /* Note: SBM has a range re-locked by sbm_scan_v3_v4.  It will be reduced
             *       in size below, after packing it.
             * RULE 1: if this is a hot file, try to create it
             *         regardless of how much needs to be cleared.
             *         We do this because hot files are accessed frequently
             *         and we want to do the extra work to defragment them.
             * RULE 2: Never pack a range size that is
             *         SS_PACK_RANGE_PCT_MAX_SIZE percent of the
             *         total volume size.
             *         a. more likely to be stolen by system.
             *         b. more likely to encounter a rsvd file.
             *         c. becomes less efficent than combining extents.
             * If we pass these rules then (1)reset the stgDesc
             * so we fall into the pack block below. and (2) free the
             * stg returned from sbm_find_space below AFTER we lock the
             * new pack range.
             *
             * Note: SSPackRangePctMaxSize is already a reciprocal percentage.
             */
            scanBlkCnt = clustCnt * vdp->stgCluster;

            sts = mig_pack_vd_range (
                               vdp,
                               scanBlkOffset,
                               (scanBlkOffset + scanBlkCnt),
                               &newBlkOffset,
                               1 );

            if ( sts != EOK ) {
                VFAST_STATS ( ss_get_n_lk_free_space_mig_pack_vd_range_err );
                /* E_RSVD_FILE_INWAY: There may have been enough space but
                 *          it isn't uninterupted by a non-movable reserved file.
                 * ENO_MORE_MCELLS - File being packed is on DDL, abort
                 *                   since we cant move it.
                 * ENO_MORE_BLKS indicates the reserved
                 *               space was stolen by a needy
                 *               user application file.
                 * E_RANGE_NOT_MAPPED indicates that the file
                 *               range being migrated was
                 *               truncated in whole or part before
                 *               we could migrate it.
                 */
                sts = ss_find_space(vdp,
                                    requestedBlkCnt,
                                    &blkOffset,
                                    &blkCnt,
                                    TRUE);
                if(sts != EOK) {
                    VFAST_STATS ( ss_get_n_lk_free_space_ss_find_space_err3 );
		    goto HANDLE_EXCEPTION;
                } else {
		    sts = EOK;
		    goto HANDLE_EXCEPTION;
                }
            }

            /* Relock the new sbm location at the new clear blk
             * offset.
             */
            newBlkCnt = scanBlkCnt - (newBlkOffset - scanBlkOffset);

            if(newBlkCnt == 0) {
                VFAST_STATS ( ss_get_n_lk_free_space_enospc );
		sts = ENOSPC;
		goto HANDLE_EXCEPTION;
            }

            ADVFTM_VDT_STGMAP_LOCK(&vdp->stgMap_lk);
            sts = sbm_lock_range (vdp,
                                  newBlkOffset,
                                  newBlkCnt );
            ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
            if(sts != EOK) {
                VFAST_STATS ( ss_get_n_lk_free_space_sbm_lock_range_err );
		goto HANDLE_EXCEPTION;
            }
            sbm_range_locked = TRUE;

            blkOffset = newBlkOffset;
            blkCnt    = newBlkCnt;
        } else {
            /* range would take too long to pack, return largest extent */
            sts = ss_find_space(vdp,
                                requestedBlkCnt,
                                &blkOffset,
                                &blkCnt,
                                TRUE);
            if(sts != EOK) {
                VFAST_STATS ( ss_get_n_lk_free_space_ss_find_space_err4 );
		goto HANDLE_EXCEPTION;
            } else {
		sts = EOK;
		goto HANDLE_EXCEPTION;
            }
        }
    }

HANDLE_EXCEPTION:

    if(sts != EOK) {
        *allocBlkOffp = 0;
        *alloc_fob_cnt= 0;
    } else {
        *allocBlkOffp = blkOffset;
        *alloc_fob_cnt = blkCnt * ADVFS_FOBS_PER_DEV_BSIZE;
    }

    if((sts != EOK) && (sbm_range_locked)) {
        /* if there was an unrecoverable error, clear the lock */
        sbm_lock_unlock_range (vdp, 0, 0);
        SS_TRACE(vdp,0,0,0,sts);
    }

    SS_TRACE(vdp,0,*allocBlkOffp,*alloc_fob_cnt,sts);
    return sts;

}
/*
 * Routine to find the range that covers the most extents in the file
 * for req_fob_cnt number of fobs.  Holes are checked to verify the fobs 
 * on either side of the hole are contiguous.  If they are contiguous, 
 * the two extents are considered to be one extent since we could not 
 * improve upon them.  If they are not contiguous, the two extents are 
 * counted as 2, since we could move them to an adjacent position on disk.
 */
static void
ss_get_most_xtnts(bfAccessT *bfap,           /* in  - file */
                  extent_blk_desc_t *headBd, /* in  - head of block descriptor list */
                  bf_fob_t req_fob_cnt,      /* in  - requested fob count */
                  extent_blk_desc_t *startBd,/* out - new start fob */
                  bf_fob_t *fobs_to_migrate, /* out - number of fobs to migrate */
                  uint64_t *extent_cnt)
{
    statusT sts;
    extent_blk_desc_t *walkBd, *currBd, *prevBd, *bestStart;
    uint64_t reqSz, bestSz, bestCnt, currSz, currCnt;

    bestCnt = 0;
    reqSz = ADVFS_FOB_TO_OFFSET(req_fob_cnt);

    for (walkBd=headBd; walkBd!=NULL; walkBd=walkBd->ebd_next_desc) {
        prevBd = NULL;
        currSz = 0;
        for (currBd=walkBd; currBd!=NULL; currBd=currBd->ebd_next_desc) {
            if (prevBd == NULL) {
                currCnt = 1;
            } else {
                /* 
                 * 2 contiguous extents separated by a hole should
                 * not increment the extent count 
                 */
                if ((prevBd->ebd_offset + prevBd->ebd_byte_cnt) !=
                    currBd->ebd_offset) {
                    currCnt++;
                }
            }
            if (currSz + currBd->ebd_byte_cnt <= reqSz) {
                currSz += currBd->ebd_byte_cnt;
            } else {
                break;
            }
            if (currCnt > bestCnt) { /* best seen so far */
                bestStart = walkBd;
                bestCnt   = currCnt;
                bestSz    = currSz;
            }

            prevBd = currBd;
        }
    }

    startBd          = bestStart;
    *extent_cnt      = bestCnt;
    *fobs_to_migrate = ADVFS_OFFSET_TO_FOB_DOWN(bestSz);

}


/*
 * finds 
 */
static
statusT
ss_find_space_in_vd(bfAccessT   *bfap,
                    vdIndexT    vdi,              /* in  */
                    bf_vd_blk_t requestedBlkCnt,  /* in */
                    bf_vd_blk_t *blkCnt)          /* out */
{

    statusT sts=EOK;
    vdT    *vdp=NULL;
    bf_vd_blk_t blkOffset;

    if (vdp = vd_htop_if_valid(vdi, bfap->dmnP, TRUE, FALSE)) {

        sts = sc_valid_vd (bfap->dmnP,
                           bfap->reqServices,
                           vdp->vdIndex);
        if (sts != EOK) {
            sts = EOK;
        } else {
            *blkCnt = blkOffset = 0;
            sts = ss_find_space(vdp,
                                requestedBlkCnt,
                                &blkOffset,
                                blkCnt,
                                FALSE);   /* no lock range */
            if(sts==ENOSPC) 
                sts = EOK;
        }
        vd_dec_refcnt( vdp );
    }

    return(sts);
}

/*
 * Find the volume which has enough space to handle the size of the
 * file to be moved.  Give preference to the volume containing the
 * primary mcell, otherwise, return the first volume encountered that
 * has enough space.  If no volume satisfies the requested size, return
 * the volume with the most free space.
 */
static
statusT
ss_get_vd_most_free(bfAccessT *bfap,
                    vdIndexT *newVdIndex)
{
    int32_t vdi;
    statusT sts=EOK;
    bf_vd_blk_t blkCnt,
                savedtotFreeBlks = 0,
                requestedBlkCnt  = 0;

    *newVdIndex = 0;

    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
    if (sts != EOK) {
        VFAST_STATS ( ss_get_vd_most_free_lock_xtnt_err );
        return(sts);
    }

    requestedBlkCnt = bfap->xtnts.bimxAllocFobCnt / ADVFS_FOBS_PER_DEV_BSIZE; 

    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
    if (requestedBlkCnt < 1) {
        VFAST_STATS ( ss_get_vd_most_free_not_enough_xtnts );
        return(E_NOT_ENOUGH_XTNTS);
    }

    /*
     * First search for space in the vd containing the primary mcell.
     */
    if (ss_find_space_in_vd(bfap, 
                            bfap->primMCId.volume, 
                            requestedBlkCnt,
                            &blkCnt) == EOK) {
        if(blkCnt) {
            *newVdIndex = bfap->primMCId.volume;
            if (blkCnt == requestedBlkCnt) {
                return (sts); /* success */
            }
            savedtotFreeBlks = blkCnt;
        }
    }

    /*
     * Since we didn't find enough contiguous space in the primary mcell's vd, 
     * check all other vd's.
     */
    for (vdi = 1; vdi <= bfap->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if (bfap->primMCId.volume == vdi) {
            continue; /* skip, since we already did this one */
        }
        if (ss_find_space_in_vd(bfap, vdi, requestedBlkCnt, &blkCnt) == EOK) {
            /*
             * Record a new vd index for any volume that has storage
             * larger than the biggest recorded size we've seen thus far.
             */
            if(blkCnt && 
               ((!savedtotFreeBlks) || (blkCnt > savedtotFreeBlks))) {
                *newVdIndex = vdi;
                if (blkCnt == requestedBlkCnt) {
                    return (sts); /* success */
                }
                savedtotFreeBlks = blkCnt;
            }
        }
    }
    if(*newVdIndex == 0) {
        VFAST_STATS ( ss_get_vd_most_free_eno_more_blocks );
        sts=ENO_MORE_BLKS;
    }

    return(sts);

}

/*********************************************************************
 *
 * This routine is the main routine for moving files around.  The rule
 * is that there is only one thread allowed to be moving a file on a
 * particular volume at a time.  The other threads queue up in here,
 * blocking on dvdp->ssVolInfo.ssVdMigState > SS_IDLE.
 *
 ********************************************************************/

static statusT
ss_vd_migrate( bfTagT   filetag,
               bfSetIdT bfSetId,
               vdIndexT srcVdIndex,
               ssWorkMsgTypeT msgType,
               uint32_t xtntCnt,
               int32_t  alloc_hint,
               int32_t  hotfile
             )
{
    statusT   sts=EOK, 
              sts2;
    bfAccessT *bfap   = NULL;
    bfSetT    *bfSetp = NULL;
    vdT       *dvdp   = NULL;

    extent_blk_desc_t *headBd=NULL, *startBd=NULL, *currBd;
    bsInMemXtntMapT *xtntMap;

    bf_fob_t fobs_migrated   = 0,
             fobs_to_migrate = 0,
             req_fob_cnt     = 0, /* number fobs requested to defragment complete file */
             alloc_fob_cnt   = 0, /* sbm is free for this many fobs.*/
             fob_offset      = 0, 
             fob_end         = 0, 
             pageSize,
             mig_fob_cnt; 

    bf_vd_blk_t allocBlkOffset = 0,/* sbm is free starting at this location.*/
                newBlkOffset   = 0;

    int32_t wholeFile      = TRUE,
            xtntmap_locked = FALSE,
            closeFileFlag  = FALSE,
            vdRefed        = FALSE,
            targVol        = -1,
            fsetMounted;

    uint32_t totXtnts = 0;
    uint64_t extentCnt=0;
    vdIndexT dstVdIndex=0, newDstVdIndex=0;
    off_t  offset = 0;
    int fragmented;

    sts = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
    if (sts != EOK) {
        VFAST_STATS ( ss_vd_migrate_ss_open_file_err );
	goto HANDLE_EXCEPTION;
    }

    closeFileFlag = TRUE;
    /*
     * Check to see if bfaFlags indicates we are fragmented, yet
     * ss_xtnt_counter() indicates we are not. ss_xtnt_counter()
     * takes precedence.
     */
    if ((bfap->bfaFlags & BFA_FRAGMENTED)) {
        sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
        if(sts != EOK) {
            VFAST_STATS ( ss_vd_migrate_xtnt_map_err );
            goto HANDLE_EXCEPTION;
        }
        if (ss_xtnt_counter(bfap) == 1) {

            sts = advfs_bs_tagdir_update_tagmap(bfap->tag,
                                                bfap->bfSetp,
                                                BS_TD_FRAGMENTED,
                                                bsNilMCId,
                                                FtxNilFtxH,
                                                TDU_UNSET_FLAGS);
     
            MS_SMP_ASSERT(sts == EOK);

            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags &= ~BFA_FRAGMENTED;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
            ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
            VFAST_STATS ( ss_vd_migrate_frag_unexpected_mismatch );
            goto HANDLE_EXCEPTION;
        }
        ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    }

    ADVSMP_BFAP_BFA_SS_MIG_LOCK(&bfap->bfa_ss_migrate_lock);
    if (bfap->ssMigrating == SS_POSTED) {
        /* another thread is already migrating this file:
         * this is either a defrag migrate or a hotfile migrate
         * in either case, simply return as if this is a hotfile
         * and we are defragmenting, it will potentially get moved
         * later during hotfile processing.  If it is a hotfile migrate,
         * it will become defragmented during the hotfile migrate.
         */
        ADVSMP_BFAP_BFA_SS_MIG_UNLOCK(&bfap->bfa_ss_migrate_lock);
        sts = ss_close_file( bfSetp, bfap, fsetMounted );
        return (E_MIGRATE_IN_PROGRESS);
    }
    bfap->ssMigrating = SS_POSTED;
    ADVSMP_BFAP_BFA_SS_MIG_UNLOCK(&bfap->bfa_ss_migrate_lock);
    closeFileFlag = TRUE;
    pageSize        = bfap->bfPageSz;

    /*
     * This is a direct io enabled file, and vfast is not scheduled to perform
     * on direct io enabled files.
     *
     * This check should have been done before the file was put on the list, but
     * it doesn't hurt to check again.
     */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
       (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO) ||
        (bfap->dioCnt)) {
        VFAST_STATS ( ss_vd_migrate_ignore_dio );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    dstVdIndex = srcVdIndex;  /* default is primary mcell vol */
    if(( !(alloc_hint & BS_ALLOC_MIG_SAMEVD) ) &&
       ( !(alloc_hint & BS_ALLOC_MIG_SINGLEXTNT) ) &&
       (bfap->dmnP->ssDmnInfo.ssDmnBalance) &&
       (bfap->dmnP->vdCnt > 1)) {
        /* balance is enabled - go find best volume */
        if((targVol = ss_find_target_vd(bfap)) != -1) {
            dstVdIndex = targVol;
        }
    } 
    if((targVol == -1) &&
       ( !(alloc_hint & BS_ALLOC_MIG_SINGLEXTNT) ) &&
       ( !(alloc_hint & BS_ALLOC_MIG_SAMEVD) )) {
        /* If no need to explicitly balance and this is not
         * an explicit placement for a hotfile, choose the volume 
         * with the most free space. The space may be stolen by 
         * system, but then we would be running out of space anyways.
         */
        sts = ss_get_vd_most_free(bfap,&newDstVdIndex);
        if(sts == EOK) {
            dstVdIndex = newDstVdIndex;
        } else if(sts == ENO_MORE_BLKS) {
            dstVdIndex = srcVdIndex; /* try orig cause may want to pack */
        } else if(sts != EOK) {
            VFAST_STATS ( ss_vd_migrate_get_vd_most_free_err );
	    goto HANDLE_EXCEPTION;
        }
    }

    /* bump the ref count on this selected vd so it cant be removed */
    dvdp = vd_htop_if_valid(dstVdIndex, bfap->dmnP, TRUE, FALSE);
    if ( dvdp == NULL ) {
        /* must be an rmvol going on - just drop msg */
        VFAST_STATS ( ss_vd_migrate_bad_vdi );
	sts = EBAD_VDI;
	goto HANDLE_EXCEPTION;
    }
    vdRefed = TRUE;

    /* Wait for any other threads to complete before continuing */
    ADVMTX_SSVDMIG_LOCK( &dvdp->ssVolInfo.ssVdMigLk );
    dvdp->ssVolInfo.ssVdSSThdCnt++;    /* Potentially held for a long time */
    while ((dvdp->ssVolInfo.ssVdMigState == SS_PROCESSING) ||
           (dvdp->ssVolInfo.ssVdMigState == SS_PARKED)) {
        /* must be another thread got here first, simply wait for my turn */
        cv_wait( &dvdp->ssVolInfo.ssContMigCV, &dvdp->ssVolInfo.ssVdMigLk,
                        CV_MUTEX, CV_DFLT_FLG );
    }

    if((dvdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (bfap->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        ADVMTX_SSVDMIG_UNLOCK( &dvdp->ssVolInfo.ssVdMigLk );
        /* re-broadcast in case there are more threads waiting */
        cv_broadcast (&dvdp->ssVolInfo.ssContMigCV, NULL, CV_NULL_LOCK);
        VFAST_STATS ( ss_vd_migrate_err );
	sts = E_WOULD_BLOCK;
	goto HANDLE_EXCEPTION;
    }

    /* Set state to SS_PROCESSING so that no more messages are 
     * sent to work thread while we are working on this destination volume.
     */
    dvdp->ssVolInfo.ssVdMigState = SS_PROCESSING;
    ADVMTX_SSVDMIG_UNLOCK( &dvdp->ssVolInfo.ssVdMigLk );

    /* determine number of fobs of free space needed 
     * Make sure that the in-memory extent maps are valid.
     * Returns with xtntMap_lk read-locked.
     */
    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
    if (sts != EOK) {
        VFAST_STATS ( ss_vd_migrate_lock_xtnt_err );
	goto HANDLE_EXCEPTION;
    }

    /*
     * Attempt to get contiguous storage for all allocated space.
     * This could be larger than the actual file size.
     */
    req_fob_cnt = bfap->xtnts.bimxAllocFobCnt;
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    if (!req_fob_cnt) {
        /* no storage to migrate in this file */
        VFAST_STATS ( ss_vd_migrate_no_stg_to_mig );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* 
     * Try to get free space for storage to be migrated.
     * lock it with sbm_lock_unlock_range
     */
    sts = ss_get_n_lk_free_space(dvdp, 
                           req_fob_cnt,
                           &allocBlkOffset,
                           &alloc_fob_cnt,
                           alloc_hint);
#ifdef SS_DEBUG
    if (req_fob_cnt != alloc_fob_cnt) {
        if (ss_debug > 0)
            printf("ss_get_n_lk_free_space:vd(%d) tag(%ld.%ld) ret req_fobs(%ld) alloc_fobs(%ld) sts=%d\n",
                dvdp->vdIndex, ((bfSetT *)bfap->bfSetp)->dirTag.tag_num,
                bfap->tag.tag_num, req_fob_cnt, alloc_fob_cnt, sts);
    }
#endif
    if(sts != EOK) {
        VFAST_STATS ( ss_vd_migrate_get_n_lk_free_space_err );
	goto HANDLE_EXCEPTION;
    }
    if(alloc_fob_cnt == 0) {
        /* no free space found or created! */
        VFAST_STATS ( ss_vd_migrate_no_free_space_found_or_created );
	sts = ENOSPC;
	goto HANDLE_EXCEPTION;
    }


    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
    if(sts != EOK) {
        VFAST_STATS ( ss_vd_migrate_lock_xtnt_map_err );
	goto HANDLE_EXCEPTION;
    }

    xtntmap_locked = TRUE;
    xtntMap = bfap->xtnts.xtntMap;

    sts = advfs_get_blkmap_in_range(bfap,
                                    xtntMap,
                                    &offset,  /* starting offset */
                                    (xtntMap->bsxmNextFob - 1L) * ADVFS_FOB_SZ,
                                              /* length in bytes */
                                    &headBd,
                                    &extentCnt,
                                    RND_NONE,
                                    EXB_ONLY_STG, /* no holes */
                                    XTNT_NO_WAIT | XTNT_LOCKS_HELD);
    if (sts == EOK && hotfile) {
        if(bfap->primMCId.volume== dstVdIndex && extentCnt < 2) {
            /* Claim success as we can't improve on fragmentation and
             * this hotfile is being placed on the same volume
             */
	    goto HANDLE_EXCEPTION;
        } 
    } else  {
        if((extentCnt < 2) || (sts != EOK)) {
            if(extentCnt < 2) {
                VFAST_STATS ( ss_vd_migrate_no_space );
                sts = ENOSPC;
            }
            VFAST_STATS ( ss_vd_migrate_get_blkmap_err );
	    goto HANDLE_EXCEPTION;
        }
    }

    /* May have less than requested here because ss_get_n_lk_free_space returns 
     * as much as it can even if it was unable to find or create what was 
     * requested.  In this case we will combine the most non-contiguous
     * extents in the file into the free space we did get.
     *
     * Note: it may be possible that even though we got less than requested, it
     *       is still enough to hold the entire file.  This should be considered
     *       at a future time.
     */

    if(alloc_fob_cnt < req_fob_cnt) {
        ss_get_most_xtnts(bfap, headBd, req_fob_cnt, startBd, 
                          &fobs_to_migrate, &extentCnt);      
        if (extentCnt < 2) {
            sts = ENOSPC;
            VFAST_STATS ( ss_vd_migrate_enospc2 );
	    goto HANDLE_EXCEPTION;
        }
        wholeFile = FALSE;
    } else {
        fobs_to_migrate = alloc_fob_cnt;
        startBd = headBd;
    }

    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
    xtntmap_locked = FALSE;

    newBlkOffset = allocBlkOffset;
    fobs_migrated = 0;

#ifdef SS_DEBUG
    if (ss_debug > 0) {
        if (bfap && bfap->primMCId.volume != dstVdIndex) {
            printf("vfast-%s: vd %ld to %d, ",
                   dvdp->dmnP->domainName, bfap->primMCId.volume, dstVdIndex) ;
        } else {
            printf("vfast-%s: vd %d     , ", 
                   dvdp->dmnP->domainName, dstVdIndex);
        }
        printf("xtnt %3ld, %6ld fobs, tag(%ld) ",
               extentCnt,
               fobs_to_migrate, 
               bfap->tag.tag_num);
    
        if (req_fob_cnt != alloc_fob_cnt) {
            printf(", alloc_fob %ld\n",alloc_fob_cnt);
        }
        if (sts != EOK) {
            printf(", sts=%d",sts);
        }
        printf("\n");
    }
#endif

    for (currBd=startBd; currBd!=NULL && fobs_migrated<fobs_to_migrate;
         currBd=currBd->ebd_next_desc) {

        fob_offset = ADVFS_OFFSET_TO_FOB_DOWN(currBd->ebd_offset);
        fob_end = fob_offset + (((ADVFS_OFFSET_TO_FOB_UP(currBd->ebd_byte_cnt) +
                                pageSize -1)/pageSize)*pageSize);


        while (fob_offset < fob_end) {
            mig_fob_cnt = MIN((fob_end - fob_offset), SSFobCnt);

            sts = bs_migrate(
                    bfap,         /* in */
                    -1,           /* -1 indicates from any disk */
                    fob_offset,   /* src fob_offset- in */
                    mig_fob_cnt,  /* srcFobCnt - in */
                    dstVdIndex,   /* dstVdIndex - in */
                    newBlkOffset, /* dstBlkOffset - in */
                    1,            /* forceFlag, ignore pending truncations - in */
                    BS_ALLOC_MIG_RSVD
                  );

            if(sts != EOK) {
#ifdef SS_DEBUG
                if (ss_debug > 0)
                    printf("bs_migrate: vd(%d) tag(%ld.%ld) returns sts=%d\n",
                           dstVdIndex, bfap->bfSetp->bfSetId.dirTag.tag_num,
                           bfap->tag.tag_num, sts);
#endif
                VFAST_STATS ( ss_vd_migrate_bs_migrate_err );
		goto HANDLE_EXCEPTION;
            }
#ifdef SS_DEBUG
            if (ss_debug > 1)
                printf("    mig tag(%ld.%ld), offset(%ld)\n",
                        bfap->bfSetp->bfSetId.dirTag.tag_num,
                        bfap->tag.tag_num,
                        fob_offset);
#endif

            fob_offset    += mig_fob_cnt;
            fobs_migrated += mig_fob_cnt;
            newBlkOffset  += mig_fob_cnt;

            /* update the fob stats before parking or exiting */
            dvdp->dmnP->ssDmnInfo.ssFobsDefraged += mig_fob_cnt;
            if((dstVdIndex != srcVdIndex) && (targVol != -1))
                dvdp->dmnP->ssDmnInfo.ssFobsBalanced += mig_fob_cnt;
            if (fobs_migrated >= fobs_to_migrate) {
                /* we are done with the file */
                if(wholeFile) {
                    ADVMTX_SSDOMAIN_LOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                    dvdp->dmnP->ssDmnInfo.ssFilesDefraged++;
                    ADVMTX_SSDOMAIN_UNLOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );

                    if(bfap->primMCId.volume != dstVdIndex) {
                        /* since we are done, we need to ensure that the primary 
                         * mcell is moved to the destination volume.
                         */
                        sts = bs_move_metadata (bfap, dvdp);
                        if (sts != EOK) {
                            VFAST_STATS ( ss_vd_migrate_move_metadata_err );
			    goto HANDLE_EXCEPTION;
                        }
                    }
    
                    if(alloc_hint & BS_ALLOC_MIG_SINGLEXTNT) {
                        /* record hot file move stat here */
                        ADVMTX_SSDOMAIN_LOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                        dvdp->dmnP->ssDmnInfo.ssFilesIOBal++;
                        ADVMTX_SSDOMAIN_UNLOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                    }
                }

                ADVMTX_SSDOMAIN_LOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                dvdp->dmnP->ssDmnInfo.ssExtentsConsol += extentCnt;
                ADVMTX_SSDOMAIN_UNLOCK( &dvdp->dmnP->ssDmnInfo.ssDmnLk );

                break;

            }

            /* close the fileset and bfap before parking  so that the fileset can
             * be deleted if needed. We don't want to hold up the delete while
             * waiting for some idle time to run
             */
            sts = ss_close_file( bfSetp, bfap, fsetMounted );
            if (sts != EOK) {
                VFAST_STATS ( ss_vd_migrate_close_file_err );
		goto HANDLE_EXCEPTION;
            }
            closeFileFlag = FALSE;
    
            /* wait for next run approval to continue */
            sts = ss_block_and_wait(dvdp);
            if(sts != EOK) {
                VFAST_STATS ( ss_vd_migrate_block_and_wait_err );
		goto HANDLE_EXCEPTION;
            }
    
            /* reopen the file */
            sts = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
            if (sts != EOK) {
                VFAST_STATS ( ss_vd_migrate_open_file_err2 );
		goto HANDLE_EXCEPTION;
            }
            closeFileFlag = TRUE;
            ADVSMP_BFAP_BFA_SS_MIG_LOCK(&bfap->bfa_ss_migrate_lock);
            bfap->ssMigrating = SS_POSTED;
            ADVSMP_BFAP_BFA_SS_MIG_UNLOCK(&bfap->bfa_ss_migrate_lock);
        }
    }

HANDLE_EXCEPTION:


    if(xtntmap_locked)
        ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    if (headBd)
        advfs_free_blkmaps(&headBd);

    if((closeFileFlag == TRUE) &&
       (sts == EOK) &&
       (vdRefed)) {

        SS_TRACE(dvdp,0,0,0,sts);

        /* delete entry from list, if there */
        if(!BS_BFS_EQL(bfSetId, nilBfSetId)) {
            ADVMTX_VOLINFO_FRAG_LOCK(&dvdp->ssVolInfo.ssFragLk);
            ss_delete_from_frag_list(dvdp, filetag, bfSetId);
            ADVMTX_VOLINFO_FRAG_UNLOCK(&dvdp->ssVolInfo.ssFragLk);
        }
        /* since we may have only combined extents or
         * this file could have been modified by user,
         * recheck for fragmentation.
         */
        /* go get the number of not contiguous extents in the file */
        sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
        if(sts != EOK) {
	    goto HANDLE_EXCEPTION;
        }
        totXtnts = ss_xtnt_counter(bfap);
        ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

        if((totXtnts > 1) &&   /* cant do any better than 1, eh? */
           (!BS_BFS_EQL(bfSetId, nilBfSetId)) &&
           ((EXTENTS_FRAG_RATIO(alloc_fob_cnt,totXtnts) <
            dvdp->ssVolInfo.ssFragHdr.ssFragHdrThresh) ||
           (dvdp->ssVolInfo.ssFragHdr.ssFragListCnt < SS_INIT_FRAG_LST_SZ))) {
            /* insert file at new priority on possibly new vd */
            ss_insert_frag_onto_list((vdIndexT) bfap->primMCId.volume,
                                     bfap->tag,
                                     bfSetId,
                             (uint64_t) EXTENTS_FRAG_RATIO(bfap->bfaNextFob,totXtnts),
                                     totXtnts );
        }
    }

    SS_TRACE(dvdp,0,0,0,sts);

    /* Must reset flag away from SS_POSTED */
    if(closeFileFlag == TRUE) {
        ADVSMP_BFAP_BFA_SS_MIG_LOCK(&bfap->bfa_ss_migrate_lock);
        if(sts==EOK)
            bfap->ssMigrating = SS_MSG_IDLE; /* allow to be placed again */
        else
            bfap->ssMigrating = SS_STOP; /* will be cleared next ss_chk_fragratio */
        ADVSMP_BFAP_BFA_SS_MIG_UNLOCK(&bfap->bfa_ss_migrate_lock);
        (void) ss_close_file( bfSetp, bfap, fsetMounted );
    } else {
        /* reopen the file - reset msg gate flag */
        sts2 = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
        if (sts2== EOK) {
            ADVSMP_BFAP_BFA_SS_MIG_LOCK(&bfap->bfa_ss_migrate_lock);
            bfap->ssMigrating = SS_STOP;
            ADVSMP_BFAP_BFA_SS_MIG_UNLOCK(&bfap->bfa_ss_migrate_lock);
            (void) ss_close_file( bfSetp, bfap, fsetMounted );
        }
    }

    if(vdRefed) {

        SS_TRACE(dvdp,0,0,0,sts);

        /* unlock free space */
        sbm_lock_unlock_range (dvdp, 0, 0);

        ADVMTX_SSVDMIG_LOCK( &dvdp->ssVolInfo.ssVdMigLk );
        /* reset State to IDLE and wake up waiting threads */
        if(dvdp->ssVolInfo.ssVdMigState != SS_ABORT)
                dvdp->ssVolInfo.ssVdMigState = SS_MIG_IDLE;
        ADVMTX_SSVDMIG_UNLOCK( &dvdp->ssVolInfo.ssVdMigLk );

        cv_broadcast (&dvdp->ssVolInfo.ssContMigCV, NULL, CV_NULL_LOCK);

        /* decrement the vd vfast thread count */
        ADVMTX_SSVDMIG_LOCK( &dvdp->ssVolInfo.ssVdMigLk );
        dvdp->ssVolInfo.ssVdSSThdCnt--;
        if (dvdp->ssVolInfo.ssVdSSThdCnt == 0 &&
            dvdp->ssVolInfo.ssVdSSWaiters) {
            dvdp->ssVolInfo.ssVdSSWaiters = 0;
            cv_broadcast(&dvdp->ssVolInfo.ssVdSSWaitersCV, NULL, CV_NULL_LOCK);
        }
        ADVMTX_SSVDMIG_UNLOCK( &dvdp->ssVolInfo.ssVdMigLk );

        vd_dec_refcnt(dvdp);
    }
    return(sts);
} /* end ss_vd_migrate */


/*********************************************************************
 *
 * This routine counts the number of disk blocks on the given volume.
 *
 ********************************************************************/

static
uint64_t
ss_blks_on_vd(bfAccessT *bfap,       /* in */ 
              vdIndexT vdi           /* in */
             )
{
    bf_vd_blk_t tot_blks_on_vd=0;
    uint32_t i;
    bsInMemXtntMapT *xtntMap=NULL;
    bsInMemSubXtntMapT *subXtntMap=NULL;
    statusT sts=EOK;

    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_REFERENCE|XTNT_WAIT_OK);
    if (sts != EOK) {
        VFAST_STATS ( ss_blks_on_vd_lock_xtnt_err );
        return(0);
    }

    xtntMap = bfap->xtnts.xtntMap;

    /* determine total number of blocks on this volume */
    for ( i = 0; i < xtntMap->validCnt; i++ ) {
        subXtntMap = &(xtntMap->subXtntMap[i]);
        MS_SMP_ASSERT(subXtntMap);
        if (FETCH_MC_VOLUME(subXtntMap->mcellId) == vdi ) {
            tot_blks_on_vd += subXtntMap->bssxmFobCnt / ADVFS_FOBS_PER_DEV_BSIZE;
        }
    }  /* end for */

    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
    return(tot_blks_on_vd);
}


/*********************************************************************
 *
 * This routine determines the file to move to a target volume to end
 * up with a better balanced domain. 
 *
 ********************************************************************/

static
int
ss_find_target_vd( bfAccessT *bfap)
{
    statusT sts;
    vdT    *vdp=NULL;
    uint64_t fileBlockCount,
             dmnSizeBlks,
             dmnFreeSizeBlks,
             newVolFreeSizeBlks,
             blksOnTarg,         /* file blocks on target volume */
             blksOnVd,           /* file blocks on non-target volume */
             dmnFreeSizePct, 
             targFreePct, 
             srcFreePct,
             bestTargetFileBlks, /* best file blocks on target volume */
             precision=100000000; /* enough for a terabyte of volsize */
    int64_t  volDeviation, 
             dmnBalanceDeviation,
             newBalanceDeviation,
             bestNewBalanceDeviation;
    int32_t  i,
             vdi,
             vdCnt,
             validVd,
             targ,
             bestTargetVdi = -1,
             bestTargetIdx = 0,
             targetVdi= -1,
             vdRefed = FALSE,
             found_bad_vol = FALSE;
    typedef struct {
        uint64_t volSizeBlks;
        uint64_t volFreeBlks;
        int vdi;
    } volDataT;
    volDataT *volData = NULL;

    if ((TEST_DMNP(bfap->dmnP) != EOK) ||
        (bfap->dmnP->state != BFD_ACTIVATED) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_PENDING)) {
        VFAST_STATS ( ss_find_target_vd_err );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    dmnSizeBlks = 0;
    dmnFreeSizeBlks = 0;
    fileBlockCount = bfap->xtnts.bimxAllocFobCnt / ADVFS_FOBS_PER_DEV_BSIZE;

    /* malloc this structure each time routine is called, 
     * otherwise we would have to reference and dereference the vdp 
     * many times in the following code.
     */
    volData = (volDataT *)ms_malloc(bfap->dmnP->vdCnt * sizeof(volDataT));

    /*
     * Determine how much free space there is on each volume.
     * Save it off for use later.
     */
    for (vdCnt = 0,vdi = 1, validVd=0; 
         vdCnt < bfap->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( !(vdp = vd_htop_if_valid(vdi, bfap->dmnP, TRUE, FALSE)) ) {
            continue;
        }
        sts = sc_valid_vd (
                           bfap->dmnP,
                           bfap->reqServices,
                           vdp->vdIndex
                           );
        if (sts != EOK) {
            /* This volume is not a candidate for our "normal" file.
             * Skip this volume in our processing.
             */
            vdCnt++;
            vd_dec_refcnt( vdp );
            continue;
        }

        volData[validVd].volSizeBlks = vdp->vdSize;
        volData[validVd].volFreeBlks = vdp->freeClust * vdp->stgCluster;
        volData[validVd].vdi = vdi;

        dmnSizeBlks += volData[validVd].volSizeBlks;
        dmnFreeSizeBlks += volData[validVd].volFreeBlks;

        validVd++;
        vdCnt++;
        vd_dec_refcnt(vdp);
    }

    dmnFreeSizePct = (dmnFreeSizeBlks * precision) / dmnSizeBlks;

    /*
     * Calculate the total deviation of the domain from perfect balance
     */
    dmnBalanceDeviation = 0;
    for(i=0; i < validVd; i++) {
        volDeviation = ((volData[i].volFreeBlks * precision) / 
                         volData[i].volSizeBlks) - dmnFreeSizePct;
        dmnBalanceDeviation += abs(volDeviation);
        if((abs(volDeviation)) > (SSBalanceGoal * (precision/100)))
            found_bad_vol = TRUE;  /* at least one of the vols worse than threshold */
    }

    if(!found_bad_vol) {
        /* if not too out of balance 
         * return -1 indicating same vol as src.is ok
         */
        targetVdi = -1;
        VFAST_STATS ( ss_find_target_vd_info_not_out_of_balance );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    bestNewBalanceDeviation = dmnBalanceDeviation;

    for(targ=0; targ < validVd; targ++) {

        blksOnTarg = ss_blks_on_vd(bfap,
                                   (volData[targ].vdi));

        /* Don't consider moving the file to this volume if either the volume
         * doesn't have enough free space to hold the whole file (the whole
         * file must be allocated afresh before any of the old space occupied
         * by the file can be freed, so the presently occupied space cannot be
         * counted). */

        if(fileBlockCount >= volData[targ].volFreeBlks) {
            continue;
        }

        newBalanceDeviation = 0;

        /* simulate moving file to target volume above, add up all the other
         * volumes new free space and arrive at a new domain balance deviation.
         */
        for (i = 0; i < validVd; i++) {
            
            blksOnVd = ss_blks_on_vd(bfap,
                                     (volData[i].vdi));

            /* increase the free size of the volume if file is to be moved
             * from this volume.
             */
            newVolFreeSizeBlks = volData[i].volFreeBlks + blksOnVd;

            /* decrement the free size of the target volume by the
             * whole file amount since the whole file is moved 
             */
            if (i == targ) {
                newVolFreeSizeBlks -= fileBlockCount;
            }

            /* Calculate delta for this volume from the domain average free
             * space.  Keep a running total for all volumes in domain.
             */
            volDeviation = ((newVolFreeSizeBlks * precision) / 
                                     volData[i].volSizeBlks) - dmnFreeSizePct;

            newBalanceDeviation += abs(volDeviation);
        }

        /* if domains deviation is better than last, set to new best  */
        if (newBalanceDeviation < bestNewBalanceDeviation) {
            bestNewBalanceDeviation = newBalanceDeviation;
            /* this should be the new target volume */
            bestTargetVdi = volData[targ].vdi;
            bestTargetIdx = targ;
            bestTargetFileBlks = blksOnTarg;
        }
    }

    /* is new best deviation from all volumes above better
     * than the existing domain deviation? If so, perform a final check.
     */
    if (bestNewBalanceDeviation < dmnBalanceDeviation) {
        /*
         * This code checks to see that the new free area in the target volume
         * is less than the free area that will remain on each of the volumes 
         * where the file used to be.  This prevents a very large file from being
         * moved to a small volume, whereby the small volume would then show
         * a possibly very large percentage of its disk suddenly consumed.
        */
        for (i = 0; i < validVd; i++) {

            /* skip if volume is the same as the target volume */
            if (volData[i].vdi == bestTargetVdi) continue;

            blksOnVd = ss_blks_on_vd(bfap,
                                     (volData[i].vdi));

            /* process only if some blocks on the source volumes */
            if (blksOnVd) {
                targFreePct  =  ((volData[bestTargetIdx].volFreeBlks + 
                                         bestTargetFileBlks - fileBlockCount) *
                                          precision) / 
                                         volData[bestTargetIdx].volSizeBlks;
                srcFreePct =    ((volData[i].volFreeBlks + blksOnVd) * precision)
                                    / volData[i].volSizeBlks;

                if ( targFreePct < srcFreePct ) {
                    targetVdi = -1;
                    VFAST_STATS ( ss_find_target_vd_info_not_out_of_balance2 );
		    sts = EOK;
		    goto HANDLE_EXCEPTION;
                }
             }
        }

        /* set target volume to the volume that will help  */
        targetVdi = bestTargetVdi;
    } else {
        targetVdi = -1;
    }

HANDLE_EXCEPTION:

    if(volData)  ms_free(volData);
    return targetVdi;

} /* end ss_find_target_vd */


/*********************************************************************
 *
 * This routine determines which target volume ends
 * up with a better balanced domain. 
 *
 ********************************************************************/
static void
ss_sim_file_on_vols( volDataT *volData,       /* in */
                     int32_t validVd,         /* in */
                     int64_t currDmnIOBalance,/* in */
                     ssHotLLT *try_hp,        /* in */
                     int64_t *newDeviation,   /* out */
                     vdIndexT *targVdi)       /* out */
{
    int64_t  volDeviation,
             newBalanceDeviation,
             bestNewBalanceDeviation;
    uint64_t newVolIO, currHotIOCnt;
    int32_t  i, wday, targ;
    vdIndexT targetVolumeIndex= -1;


    bestNewBalanceDeviation = 0;
    for(targ=0; targ < validVd; targ++) {

        if((try_hp->ssHotFileSize / (ADVFS_FOB_SZ / ADVFS_FOBS_PER_DEV_BSIZE)) >=
            volData[targ].volFreeBlks) {
            /* not enough room on this target */
            continue;
        }

        /* simulate moving file to target volume above, add up all the other
         * volumes hot IOs and arrive at a new domain balance deviation.
         */
        for(currHotIOCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++)
            currHotIOCnt += try_hp->ssHotIOCnt[wday];

        for(newBalanceDeviation = 0,
            newVolIO=0, volDeviation=0, i = 0; i < validVd; i++) {

            if (i == targ) {
                /* increase the IO count of the volume if file is to be moved
                 * to this volume.
                 */
                newVolIO = volData[i].volHotIO + currHotIOCnt;
            } else
                newVolIO = volData[i].volHotIO ;

            /* Calculate delta for this volume from the domain best hot
             * file balance, keep a running total for all volumes in domain
             */
            volDeviation = newVolIO - currDmnIOBalance;
            newBalanceDeviation += abs(volDeviation);
        }

        /* if domains IO balance is better than last, set to new best  */
        if ((bestNewBalanceDeviation == 0) ||
            (newBalanceDeviation < bestNewBalanceDeviation)) {
            bestNewBalanceDeviation = newBalanceDeviation;
            /* this should be the new target volume */
            targetVolumeIndex = volData[targ].vdi;
        }
    }

    *newDeviation = bestNewBalanceDeviation;
    *targVdi = targetVolumeIndex;

    return ;
}


/*********************************************************************
 *
 * This routine finds the hot file distribution that creates the 
 * most balance for the most active hot files.
 *
 ********************************************************************/

static
statusT
ss_find_hot_target_vd(domainT  *dmnP,         /* in */
                      vdIndexT *targVdIndex,  /* out - selected vd */
                      bfTagT   *tag,          /* out - selected file */
                      bfSetIdT *setId)        /* out - selected fset */
{
    statusT sts=EOK;
    struct timeval curr_time;
    ssHotHdrT *hhp;
    ssHotLLT *currp, *hp, *new_hp=NULL;  /* entry pointer */
    vdT *vdp=NULL,*vdp2=NULL;
    int64_t  volDeviation, 
             newBalanceDeviation,
             bestNewBalanceDeviation,
             currDmnIOBalance ;
    uint64_t dmnHotIO=0, newVolIO, currHotIOCnt;
    int32_t  hot_lock_taken = FALSE,
             vdRefed        = FALSE,
             i, 
             wday,
             targ,
             vdCnt, 
             validVd;
    vdIndexT targetVolumeIndex= -1,
             bestVolumeIndex,
             vdi;
    volDataT *volData = NULL;
    uint16_t topHotLstCnt=0;

    if ((TEST_DMNP(dmnP) != EOK) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING)) {
        VFAST_STATS ( ss_find_hot_target_vd_err );
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* malloc this structure each time routine is called, 
     * otherwise we would have to reference and dereference the vdp 
     * many times in the following code to store it on the vdp.
     * malloc the most vds that can be valid even though we may
     * use a subset of them.
     */
    volData = (volDataT *)ms_malloc(dmnP->vdCnt * sizeof(volDataT));

    /* I. load the volume information */
    for (vdCnt = 0,vdi = 1, validVd=0; 
         vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        }
        sts = sc_valid_vd (
                           vdp->dmnP,
                           defServiceClass,
                           vdp->vdIndex
                           );
        if (sts != EOK) {
            /* This volume is not a candidate for our "normal" file. 
             * Skip this volume in our processing.
             */
            vdCnt++;
            vd_dec_refcnt( vdp );
            continue;
        }

        volData[validVd].volFreeBlks = vdp->freeClust * vdp->stgCluster;
        volData[validVd].vdi = vdp->vdIndex;

        validVd++;
        vdCnt++;
        vd_dec_refcnt( vdp );
    }

    /* see if there is any work - keep this below sc_valid_vd for lock hier */
    curr_time = get_system_time();
    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    hot_lock_taken = TRUE;
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    if (( hhp->ssHotListCnt == 0)  || 
        ( validVd == 1 ) ||
        (((curr_time.tv_sec - dmnP->ssDmnInfo.ssFirstMountTime)/SECHOUR) <
          dmnP->ssDmnInfo.ssSteadyState)) {
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* II. Count up the hottest HOT file IO on each volume.
     *      Find first hot file in top percent that has not been
     *      ssHotPlaced on a volume yet.
     */
    i = 0;
    hp = hhp->ssHotFwd;

    topHotLstCnt = hhp->ssHotListCnt / (100 / SSHotLstCrossPct);
    if ( dmnP->vdCnt > topHotLstCnt )
        topHotLstCnt = dmnP->vdCnt;

    while( hp != (ssHotLLT *)hhp ) {

        if ( i >= topHotLstCnt ) {
            /* Place only a percentage of the list  */
            break;  /*quit scan*/
        }

        if ((hp->ssHotPlaced < 0)  &&
            ((curr_time.tv_sec - hp->ssHotErrTime) > SSHotPenaltyTime)) {
            /* If penalty time for this file's vfast error is up,
             * reset HotPlaced so file gets considered
             */
            hp->ssHotPlaced = 0;
            hp->ssHotErrTime = 0;
        }

        /* Make sure the entry has been in the list long enough and that it
         * is still being used.
         */
        if((curr_time.tv_sec - hp->ssHotEntryTime) <
                  dmnP->ssDmnInfo.ssMinutesInHotList) {
            /* not on list long enough, skip it */
            hp = hp->ssHotFwd;
            continue;
        }

        if((hp->ssHotPlaced == 0) && 
           (!new_hp) &&
           (!BS_BFTAG_RSVD(hp->ssHotTag))) {
            /* if it hasn't been placed on a volume yet */
            /* don't move reserved files as part of hot files balancing */
            for (targ = 0; targ < validVd; targ++) {
                /* Place first file found that will fit on at least one 
                 * of the volumes.
                 */
                if((hp->ssHotFileSize / 
                            (ADVFS_FOB_SZ / ADVFS_FOBS_PER_DEV_BSIZE)) < 
                        volData[targ].volFreeBlks) {
                    new_hp = hp;  /* found the file to place! */
                    break;
                }
            } 
        }

        /* count reserved files where they lay - 
         * ignore hot files that have errored or that have not been placed yet. */
        if ((hp->ssHotPlaced > 0) ||
            (BS_BFTAG_RSVD(hp->ssHotTag))) {
            for(currHotIOCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                currHotIOCnt += hp->ssHotIOCnt[wday];
            }
            /* match correct vd since there can be vdi that are not consecutive */
            for(targ=0; targ < validVd; targ++) {
                if(volData[targ].vdi == hp->ssHotVdi) {
                    volData[targ].volHotIO += currHotIOCnt;
                    dmnHotIO += currHotIOCnt;
                }
            }
        }

        if(!BS_BFTAG_RSVD(hp->ssHotTag)) i++;  
        hp = hp->ssHotFwd;
    }

    if((dmnHotIO == 0) && (new_hp != NULL)) {
        /* No files have been placed yet,
         * keep file on same volume
         */
        targetVolumeIndex = new_hp->ssHotVdi;
	sts = EOK;
	goto HANDLE_EXCEPTION;
    }

    /* III. No new files ready to be placed.
     *      Clear placement field for files that have fallen
     *      so the next time they are one of hottest they can
     *      be placed according to the conditions at that time.
     */
    for(; hp != (ssHotLLT *)hhp; hp = hp->ssHotFwd) {

        /* skip reserved files, files not on list long enough,
         * and files with move errors
         */
        if(BS_BFTAG_RSVD(hp->ssHotTag))
            continue;

        if(hp->ssHotPlaced != 0) {
            hp->ssHotPlaced = 0;
        }

    } /* end for */

    if(new_hp == NULL) {
         /* no files ready to be placed.  */
        targetVolumeIndex = -1;
	sts = EOK;
	goto HANDLE_EXCEPTION;
    } else {

        /* IV. New file ready to be placed.
         *     Determine best volume to place it on.
         */

        currDmnIOBalance = dmnHotIO / validVd;

        ss_sim_file_on_vols( volData,
                         validVd,
                         currDmnIOBalance,
                         new_hp,
                         &newBalanceDeviation,
                         &targetVolumeIndex);
    }


HANDLE_EXCEPTION:

    if(hot_lock_taken) {
        ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );
    }

    if(volData)  ms_free(volData);

    if(new_hp)  {
        *targVdIndex = targetVolumeIndex;
        *tag = new_hp->ssHotTag;
        *setId = new_hp->ssHotBfSetId;
    } else {
        *targVdIndex = -1;
    }
    return sts;

} /* end ss_find_hot_target_vd */


/*
 * This routine locks the tag array and then sets the bit for the tag 
 * page that this tag belongs to.
 *
 * no return value
 */
void
ss_set_fragmented_tag_page(bfTagT  tag,
                           bfSetT *bfSetp
                          ) 
{
    bs_meta_page_t tagPg      = TAGTOPG(&tag);
    uint64_t       arraySz    = bfSetp->ssTagArraySz;
    uint64_t       arrayIndex = PG_TO_ARRAY_INDEX(tagPg);
    /* 
     * The array will grow in the future if defrag_all is enabled.
     * Make sure our array is large enough to set the corresponding
     * bit in our mapping.
     */
    if (arraySz == 0 || arrayIndex > (arraySz-1)) {
        return;
    }

    /*
     * Set the bit.
     */
    ADVSMP_SS_TAG_ARRAY_LOCK(&bfSetp->ssTagMutex);
    SET_FRAGMENTED_TAG_PAGE(bfSetp->ssTagArray, tagPg); 
    ADVSMP_SS_TAG_ARRAY_UNLOCK(&bfSetp->ssTagMutex);
}

/*
 * This routine allocates space for a larger tag array.  If the
 * tag array is growing, the existing entries are copied into
 * the larger array.  All previously uninitialized entries are
 * marked as "dirty" so that these tag pages will be examined for
 * fragmented files.
 *
 * no return value
 */
void
ss_grow_tag_array(bfSetT  *bfSetp,
                  size_t  new_size)
{
    size_t   i;
    size_t   old_size  = bfSetp->ssTagArraySz;
    uint64_t *arrayPtr = (uint64_t *) 
                         ms_malloc_no_bzero(sizeof(uint64_t) * new_size);
    int      lock_taken=0;
    uint64_t *tmpArray;

    VFAST_STATS ( ss_grow_tag_array_cnt );

    /*
     * Copy existing entries to new array and free old array.
     */
    if (old_size) {
        ADVSMP_SS_TAG_ARRAY_LOCK(&bfSetp->ssTagMutex);
        lock_taken = 1;
        bcopy(bfSetp->ssTagArray, arrayPtr, old_size);
        tmpArray = bfSetp->ssTagArray;
    }
    bfSetp->ssTagArray = arrayPtr;
    if (lock_taken) {
        ADVSMP_SS_TAG_ARRAY_UNLOCK(&bfSetp->ssTagMutex);
        ms_free(tmpArray);
    }
    /* 
     * Initial setup assumes all pages have a dirty tag on them.
     * If we are growing the array here, we will only initialize
     * the new portion.
     */
    for (i=old_size; i < new_size; i++) {
        bfSetp->ssTagArray[i] = 0xffffffffffffffff;
    }
    bfSetp->ssTagArraySz = (uint32_t) new_size;
}

/*
 * Reads in a tag page, then sends messages to autotune for any tags
 * which have the fragmented bit set.  
 *
 * returns ENOSPC if message queue is full.
 */
int
ss_setup_fragmented_tags(bfSetT         *bfSetp,
                         bs_meta_page_t  tagPg,
                         uint32_t       *page_throttle,
                         uint32_t       *msg_throttle
                        )
{
    int32_t      slot, 
                 start_slot = (tagPg == 0) ? 1 : 0; /* page 0 slot 0 special */
    int          sts = 0;
    bfPageCacheHintT  cache_hint = BS_RECYCLE_IT;
    bfPageRefHT  pgRef;
    bsTDirPgT   *tdpgp;
    bsTMapT     *tdmap;
    ssListMsgT  *listmsg;

    /* ssTagArray is larger than initialized pages */
    if (tagPg >= bfSetp->tagUnInPg) {
        return sts;
    }

    if (bs_refpg(&pgRef,
                 (void **)&tdpgp,
                 bfSetp->dirBfAp,
                 tagPg,
                 FtxNilFtxH,
                 MF_VERIFY_PAGE) !=  EOK) {
        return sts;
    }

    MS_SMP_ASSERT(tdpgp->tpCurrPage == tagPg);

    for (slot = start_slot; slot < (int32_t)BS_TD_TAGS_PG; slot++) {
        if (*msg_throttle >= SS_MAX_DEFRAG_ALL_MSGS) {
            cache_hint = BS_CACHE_IT;  /* will ref this page again */
            break;
        }
        tdmap = &tdpgp->tMapA[slot];

        if ((tdmap->tmFlags & BS_TD_IN_USE) &&
            (tdmap->tmFlags & BS_TD_FRAGMENTED)) {
            sts = 1;

            if ((listmsg = (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
                (*msg_throttle)++;
                listmsg->msgType       = FRAG_UPDATE_ADD;
                listmsg->tag.tag_seq   = tdmap->tmSeqNo;
                listmsg->tag.tag_num   = MKTAGNUM(tagPg, slot);
                listmsg->tag.tag_flags = tdmap->tmFlags;
                listmsg->bfSetId       = bfSetp->bfSetId;
                listmsg->frag.vdi      = tdmap->tmBfMCId.volume;
                /* Artifically set both fragRation and xtntCnt to
                 * be a low value as we don't have an open file handle
                 * to get the real information.  This will effectively
                 * place it on the message queue with a lower 
                 * priority than those put on the queue via
                 * the active path and matches the desired 
                 * behavior that active files have precedence
                 * over inactive files.
                 */
                listmsg->frag.fragRatio = 0;
                listmsg->frag.xtntCnt   = 2;
                msgq_send_msg(ssListQH, listmsg);
#ifdef SS_DEBUG
                if (ss_debug) {
                     printf("vfast-%s add tag (%ld) to list\n", 
                               bfSetp->dmnP->domainName, listmsg->tag.tag_num);
                }
#endif
            } else {
                sts = ENOSPC;
                cache_hint = BS_CACHE_IT;  /* will ref this page again */
                break;
            }
        }
        
    } /* for */
    (*page_throttle)++;
    (void) bs_derefpg(pgRef, cache_hint);
    return sts;
}

/*
 * Quick defragment check.  May return a false positive for
 * files which have holes, yet their storage is adjacent on disk.
 * This is okay, as autotune will figure it out later via ss_xtnt_counter()
 * and won't attempt the migrate.
 *
 * Assumes the extent map is locked.
 *
 * returns 1 (True) if file is fragmented
 *         0 (False) if file is contiguous
 */
int
ss_bfap_fragmented(struct bfAccess *bfap)
{
    int bfap_fragmented = 1;
    bsInMemSubXtntMapT *subXtntMap;
    
    if (bfap->xtnts.xtntMap->validCnt == 1) {
        bfap_fragmented = 0;
    } else if (bfap->xtnts.xtntMap->validCnt == 2) {
        subXtntMap = &(bfap->xtnts.xtntMap->subXtntMap[1]);
        switch (subXtntMap->cnt) {
          case 2: if (subXtntMap->bsXA[0].bsx_fob_offset == 0) {
                      /* no primary info */
                      bfap_fragmented = 0;
                  }
                  break;
          case 3: if (subXtntMap->bsXA[0].bsx_vd_blk == XTNT_TERM) {
                      /* file starts with a hole and has one add'l extent */
                      bfap_fragmented = 0;
                  }
                  break;
          default:
             break;
        } /* switch */
    }
#ifdef HANDLE_HOLES
    if (bfap_fragmented) {
        (bfap->xtnts.xtntMap->subXtntMap[1]->bsXA[0].bsx_vd_blk == XTNT_TERM) ?
                start_val=0 : start_val=1;
       
        for (i=1; i<bfap->xtnts.xtntMap->validCnt; i++) {
            subXtntMap = &(bfap->xtnts.xtntMap->subXtntMap[i]);
            
            for (i=start_val; i < subXtntMap[i].cnt; i++) {
                /* check for hole storage hole storage, where
                 * storage is always contiguous on disk */
            }
            start_val = 0;
        }
    }
#endif /* HANDLE_HOLES */
    return bfap_fragmented;
}
