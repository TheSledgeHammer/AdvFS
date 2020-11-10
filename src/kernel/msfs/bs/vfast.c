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
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vfast.c,v $ $Revision: 1.1.16.11 $ (DEC) $Date: 2006/04/06 13:48:55 $ "

#define ADVFS_MODULE VFAST

#include <sys/lock_probe.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/clu.h>
#include <sys/dlm.h>
#include <machine/clock.h>

#include <msfs/fs_dir_routines.h>
#include <msfs/ms_public.h>
#include <msfs/ftx_public.h>
#include <msfs/fs_dir.h>
#include <msfs/fs_quota.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_access.h>
#include <msfs/ms_osf.h>
#include <msfs/vfast.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_vd.h>
#include <msfs/bs_stg.h>

msgQHT ssBossQH;       /* bosses msg queue for adding threads */
msgQHT ssListQH;       /* list processing msg queue */
msgQHT ssWorkQH;       /* ss work msg queue */
static tpoolT ssListTpool; /* pool of list worker threads */
static tpoolT ssWorkTpool; /* pool of ss general worker threads */

lock_data_t SSLock;   /* complex lock for SS_is_running */

#ifndef REPLICATED
#define REPLICATED const
#endif

mutexT ssStoppedMutex; /* global simple lock for ss_stopped_cv */
cvT    ss_stopped_cv;  /* condition variable used when shutting ss system down */

/* NUMA constants */
#pragma extern_model save
#pragma extern_model cache_aligned_packed_data(vfast)

#define _ REPLICATED

/* 64-byte-aligned section of vfast mostly read data */
_ int SS_is_inited          = FALSE;                     /* rarely read, once written */
_ int SS_is_running         = FALSE;                     /* mostly read */
_ int SSPageCnt             = SS_PAGECNT;                 /* const */
_ int SSLicenseStatus       = FALSE;                      /* const */
_ int SS_pct_msgq_more_thds = SS_INIT_PCT_MSGQ_MORE_THDS; /* const */
_ int SS_max_list_thds      = SS_MAX_LIST_THDS;          /* const */
_ int SS_max_work_thds      = SS_MAX_WORK_THDS;          /* const */
_ int SSHotLstCrossPct      = SS_HOT_LST_CROSS_PCT;       /* const */
_ int SSPackRangePctFree    = 100/SS_PACK_RANGE_PCT_FREE;     /* const */
_ int SSPackRangePctMaxSize = 100/SS_PACK_RANGE_PCT_MAX_SIZE; /* const */
_ int SSBalanceGoal         = SS_BALANCE_GOAL;            /* const */
_ int SSHotPenaltyTime      = SS_INIT_HOT_ERROR_PENALTY;  /* const */
_ int SS_const_data_pad_1[4] = {0};

#pragma extern_model restore


struct lockinfo * ADV_SSLock_lk_info;
decl_simple_lock_info(, ADVssStoppedMutex_lockinfo )
decl_simple_lock_info(, ADVssListTpool_lockinfo )
decl_simple_lock_info(, ADVssWorkTpool_lockinfo )
decl_simple_lock_info(, ADVvdT_ssVdMsgLk_lockinfo )
decl_simple_lock_info(, ADVvdT_ssVdMigLk_lockinfo )
decl_simple_lock_info(, ADVvdT_ssFragLk_lockinfo )
decl_simple_lock_info(, ADVdomainT_ssDmnLk_lockinfo )
decl_simple_lock_info(, ADVdomainT_ssDmnHotLk_lockinfo )

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
    uint64T volHotIO;
    uint64T volFreeBlks;
    int     vdi;
} volDataT;

/****** global prototypes ******/

statusT
ss_open_file(
            bfSetIdT bfSetId,
            bfTagT   filetag,
            bfSetT    **retbfSetp,
            bfAccessT **retbfap,
            int *fsetMounted
            );

statusT
ss_close_file(
            bfSetT    *bfSetp,
            bfAccessT *bfap,
            int fsetmounted
            );

void
ss_startios(vdT *vdp);

void
ss_rmvol_from_hotlst( domainT *dmnP,
                      vdIndexT delVdi );

statusT
ss_change_state(char *domain_name,
                ssDmnOpT state,
                u_long *dmnState,
                int write_out_record);

statusT
ss_put_rec( domainT *dmnP ) ;

statusT
ss_chk_fragratio (
                 bfAccessT *bfap
                 );
void
ss_snd_hot (
           ssListMsgTypeT msgType, /* in-miss or write i/o counter */
           bfAccessT *bfap         /* in */
           );

void
ss_dmn_activate(domainT *dmnP, u_long flag);

void
ss_dealloc_dmn(domainT *dmnP);

void
ss_kern_stop();

statusT
ss_kern_start();

void
ss_kern_init();

void
ss_init_vd(vdT *vdp);

void
ss_dealloc_vd(vdT *vdp);

void
ss_dmn_deactivate(domainT *dmnP, int flag);

void
ss_dealloc_pack_list(vdT *vdp) ;

#ifdef SS_DEBUG
void
print_pack_list(ssPackHdrT   *php, 
                       vdT   *vdp) ;
#endif

statusT
ss_block_and_wait(vdT *vdp);

/****** local module prototypes ******/
static int
ss_chk_hot_list ( domainT  *dmnP,
                  bfTagT   tag,
                  bfSetIdT bfSetId,
                  int vdIndex,
                  statusT sts,
                  int      flag );

static statusT
ss_find_space(
    vdT *vdp,
    uint64T requestedBlkCnt,
    uint32T *allocBlkOffp,
    uint32T *allocPageCnt,
    int flags);

static statusT
ss_get_n_lk_free_space(vdT *vdp,
                 uint64T bfPageSize,
                 uint64T reqPageCnt,
                 uint32T *allocBlkOffp,
                 uint64T *allocPageCnt,
                 int alloc_hint);

static statusT
ss_find_hot_target_vd(domainT *dmnP,
                      vdIndexT *targVdIndex,
                      bfTagT *tag,
                      bfSetIdT *setId);

static statusT
ss_do_periodic_tasks(ssPeriodicMsgTypeT task);

static int
ss_find_target_vd( bfAccessT *bfap);

static statusT
ss_vd_migrate( bfTagT   filetag,
               bfSetIdT bfSetId,
               vdIndexT srcVdIndex,
               ssWorkMsgTypeT msgType,
               int alloc_hint
             );

static statusT
ss_get_most_xtnts (bfAccessT *bfap,
                  int       *xtntmap_locked,
                  uint64T   reqPageCnt,
                  uint64T   *startPg,
                  uint64T   *runPageCnt,
                  uint64T   *xmRunPageCnt,
                  uint64T   *extentRunCnt,
                  int       whoami,
                  uint64T   pageOffset,
                  uint64T   prevPageCnt,
                  uint64T   prevBlk);

static statusT
ss_get_vd_most_free(bfAccessT *bfap,
                    vdIndexT *newVdIndex);

static ssFragLLT *
ss_select_frag_file(vdT *vdp);

static void 
ss_adj_msgs_flow_rate();

static void
ss_boss_thread( void ) ;

static void
ss_monitor_thread ( void );

static void
ss_work_thd_pool ( void ) ;

static void
ss_list_thd_pool ( void ) ;

static void
ss_copy_rec_from_disk(bsSSDmnAttrT *ssAttr,        /* in */
                      domainT *dmnP) ;             /* out */

static void
ss_copy_rec_to_disk(domainT      *dmnP,  /* in */
                    bsSSDmnAttrT *ssAttr /* out */
                   ) ;

static void
print_ssDmnInfo(domainT *dmnP) ;

static void
print_ssVolInfo(vdT *vdp) ;

static void
ss_insert_frag_onto_list( vdIndexT vdi,
                       bfTagT tag,
                       bfSetIdT bfSetId,
                       uint64T fragRatio, /* primary sort in list */
                       uint32T xtntCnt);  /* secondary sort in list */

#ifdef SS_DEBUG
static void
print_frag_list(vdT *vdp) ;
#endif

static void
ss_dealloc_frag_list(vdT *vdp) ;

static void
ss_boss_init( void );

static void
ss_init_dmnInfo(domainT *dmnP);

static void
ss_queues_create(void);

static void
ss_dealloc_hot(domainT *dmnP);

static void
ss_insert_hot_list(ssListMsgTypeT msgType,
                     ssHotLLT *hp) ;

#ifdef SS_DEBUG
static void
ss_sleep(int retry) ;

static void
print_hot_list(ssHotHdrT   *hhp) ;
#endif

static void
ss_del_from_hot_list(ssHotLLT *hp);

static void
ss_delete_from_frag_list(vdT *vdp, bfTagT tag, bfSetIdT bfSetId);

static void
ss_move_file(
               vdIndexT vdi,      /* in */
               bfDomainIdT domainId,  /* in */
               ssWorkMsgTypeT msgType /* in */
              );

#define INC_SSTHDCNT( _dmnP )  \
    /* increment the dmn vfast thread count */  \
    mutex_lock( &(_dmnP)->ssDmnInfo.ssDmnLk );  \
    (_dmnP)->ssDmnInfo.ssDmnSSThdCnt++;  \
    mutex_unlock( &(_dmnP)->ssDmnInfo.ssDmnLk );

#define DEC_SSTHDCNT( _dmnP )  \
        /* decrement the dmn vfast thread count */  \
        mutex_lock( &(_dmnP)->ssDmnInfo.ssDmnLk );  \
        (_dmnP)->ssDmnInfo.ssDmnSSThdCnt--;  \
        if ((_dmnP)->ssDmnInfo.ssDmnSSThdCnt == 0 &&  \
            (_dmnP)->ssDmnInfo.ssDmnSSWaiters)  \
        {  \
            (_dmnP)->ssDmnInfo.ssDmnSSWaiters = 0;  \
            thread_wakeup((vm_offset_t)&(_dmnP)->ssDmnInfo.ssDmnSSWaiters);  \
        }  \
        mutex_unlock( &(_dmnP)->ssDmnInfo.ssDmnLk );

/*********************************************************************
 *
 * This routine initializes global data structures for vfast,
 * including vfast queues.  Does not initialize the thread pools.
 *
 ********************************************************************/
void
ss_kern_init()
{
    int false=FALSE;
    int true =TRUE;

    mutex_init3( &ssStoppedMutex, 0, "Smartstore global Mutex", 
                 ADVssStoppedMutex_lockinfo );
    cv_init( &ss_stopped_cv );

    SMARTSTORE_LOCK_INIT(&SSLock);
    SS_WRITE_LOCK(&SSLock );
    write_const_data(&false, &SS_is_running, sizeof(int));
    write_const_data(&true, &SS_is_inited, sizeof(int));
    SS_UNLOCK(&SSLock );

    ss_kern_start();
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
        return E_DOMAIN_NOT_ACTIVATED;
    }

    SS_WRITE_LOCK(&SSLock );
    if(SS_is_running == FALSE) {
        ss_boss_init();  /* starts a thread to set SS_is_running = TRUE */
    }
    SS_UNLOCK(&SSLock );
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
    int vdi=0, vdCnt=0;
    int false=FALSE;
    int ss_map_locked = FALSE;

    if(SS_is_inited == FALSE) {
        /* Advfs has not been initialized yet! */
        /* User needs to mount at least one Advfs fileset */
        return ;
    }

    SS_WRITE_LOCK(&SSLock );
    ss_map_locked = TRUE;
    if(SS_is_running == TRUE) {

        /* stop messages from being placed on queues */
        write_const_data(&false, &SS_is_running, sizeof(int));
        SS_UNLOCK(&SSLock );
        ss_map_locked = FALSE;

        /* Notify any threads working on files or waiting to work 
         * on a file of shutdown.  
         */
        FILESET_READ_LOCK(&FilesetLock );
        for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
            for ( vdi = 1; vdCnt < fsp->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, fsp->dmnP, TRUE, FALSE)) ) {
                    continue;
                }

                cond_broadcast (&vdp->ssVolInfo.ssContMig_cv);

                vdCnt++;
                vd_dec_refcnt( vdp );
            }
        }
        FILESET_UNLOCK(&FilesetLock );

        /* send message to boss thread to shutdown, then wait 
         * for it to do so.
         */
        if((msg = (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
            msg->msgType = SS_SHUTDOWN;
            msgq_send_msg(ssBossQH, msg);
        }

        mutex_lock(&ssStoppedMutex );
        cond_wait( &ss_stopped_cv, &ssStoppedMutex );
        mutex_unlock(&ssStoppedMutex );

        /* Now free the vfast lists */
        FILESET_READ_LOCK(&FilesetLock );
        for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
            ss_dealloc_hot(fsp->dmnP);
            for ( vdi = 1; vdCnt < fsp->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, fsp->dmnP, TRUE, FALSE)) )
                    continue;

                ss_dealloc_frag_list(vdp);
                vd_dec_refcnt( vdp );
                vdCnt++;
            }
        }
        FILESET_UNLOCK(&FilesetLock );
    }
    if(ss_map_locked == TRUE)
        SS_UNLOCK(&SSLock );
    return;
}

/*********************************************************************
 *
 * This routine creates the vfast thread message queues. Done at
 * boot as part of kernel initialization of Advfs.
 *
 ********************************************************************/

void
ss_queues_create(void)
{
    statusT sts;

    /* Create a message queue to send messages to the boss thread.  */
    sts = msgq_create(&ssBossQH,           /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,   /* max messages in queue */
                      sizeof( ssBossMsgT ),/* max size of each msg */
                      TRUE,           /* q size growth is allowed */
                                      /* can't miss shutdown msgs */
                      current_rad_id()     /* RAD to create on */
                      );
    if (sts != EOK) {
        ms_printf("AdvFS vfast boss msg Q was not spawned.\n");
        return;
    }

    /* Create a message queue to send messages to the list 
     * worker thread pool.  
     */
    sts = msgq_create( &ssListQH,          /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,   /* max messages in queue */
                      sizeof( ssListMsgT ),/* max size of each msg */
                      FALSE,               /* q size is fixed */
                      current_rad_id()     /* RAD to create on */
                      );
    if (sts != EOK) {
        ms_printf("AdvFS vfast List msg Q was not spawned.\n");
        return;
    }

    /* Create a message queue to handle messages to the worker thread pool.  */
    sts = msgq_create(&ssWorkQH,         /* returned handle to q */
                      SS_INIT_MSGQ_SIZE,/* max messages in queue */
                      sizeof(ssWorkMsgT),/* max size of each msg */
                      FALSE,                  /* q size is fixed */
                      current_rad_id()       /* RAD to create on */
                      );
    if (sts != EOK) {
        ms_printf("AdvFS vfast Work msg Q was not spawned.\n");
        return;
    }
    return;
}

/*********************************************************************
 *
 * This routine wipes out the vfast message queues when vfast is stopped.
 ********************************************************************/

void
ss_queues_destroy()
{
    int ret=0;

    msgq_purge_msgs(ssBossQH);
    msgq_purge_msgs(ssListQH);
    msgq_purge_msgs(ssWorkQH);

    if((ret=msgq_destroy(ssBossQH)) != 0)
        ms_printf("AdvFS vfast boss queue was not cleaned up completely.\n");
    if((ret=msgq_destroy(ssListQH)) != 0)
        ms_printf("AdvFS vfast list queue was not cleaned up completely.\n");
    if((ret=msgq_destroy(ssWorkQH)) != 0)
        ms_printf("AdvFS vfast work queue was not cleaned up completely.\n");

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
void
ss_boss_init( void )
{
    extern task_t first_task;

    mutex_init3(&ssListTpool.plock, 0, 
                "ssListPoolLk", ADVssListTpool_lockinfo);
    mutex_init3(&ssWorkTpool.plock, 0, 
                "ssWorkPoolLk", ADVssWorkTpool_lockinfo);

    /* Create and start the boss thread.  */
    if (!kernel_thread( first_task, ss_boss_thread )) {
        ms_printf("AdvFS list boss thread was not spawned.\n");
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
    extern task_t first_task;
    ssBossMsgT *bmsg;
    int i=0;
    ssListMsgT *listmsg;
    ssWorkMsgT *workmsg;
    int monitor_stopped = TRUE;
    int true=TRUE;

    ss_queues_create();

    /* create list thread pool workers */
    mutex_lock(&ssListTpool.plock);
    ssListTpool.threadCnt =0;
    ssListTpool.shutdown  =FALSE;
    mutex_unlock(&ssListTpool.plock);
    for(i=0; i< SS_INIT_LIST_THDS; i++) {
        if (!kernel_thread(first_task,ss_list_thd_pool)) {
            ms_printf("AdvFS list worker thread #(%d) was not spawned.\n",i);
            ss_queues_destroy();
            return;
        } else {
            mutex_lock(&ssListTpool.plock);
            ssListTpool.threadCnt++;
            mutex_unlock(&ssListTpool.plock);
        }
    }

    /* create vfast work thread pool workers */
    mutex_lock(&ssWorkTpool.plock);
    ssWorkTpool.threadCnt =0;
    ssWorkTpool.shutdown  =FALSE;
    mutex_unlock(&ssWorkTpool.plock);
    for(i=0; i< SS_INIT_WORK_THDS; i++) {
        if (!kernel_thread( first_task, ss_work_thd_pool )) {
            ms_printf("AdvFS worker thread number (%d) was not spawned.\n",i);
            ss_queues_destroy();
            return;
        } else {
            mutex_lock(&ssListTpool.plock);
            ssWorkTpool.threadCnt++;
            mutex_unlock(&ssListTpool.plock);
        }
    }

    /* Create and start the monitor thread.  */
    if (!kernel_thread( first_task, ss_monitor_thread )) {
        ms_printf("AdvFS vfast monitor thread was not spawned.\n");
        ss_queues_destroy();
        return;
    } else {
        monitor_stopped = FALSE;
    }

    if((ssWorkTpool.threadCnt) && (ssListTpool.threadCnt)) {
        SS_WRITE_LOCK(&SSLock );
        write_const_data(&true, &SS_is_running, sizeof(int));
        SS_UNLOCK(&SSLock );
    } else {
        ms_printf("AdvFS vfast not started.\n");
        ss_queues_destroy();
        return;
    }

    /* now hang around and wait to manage the worker threads */
    while (TRUE) {
        /* Wait for something to do */
        bmsg = (ssBossMsgT *)msgq_recv_msg( ssBossQH );
        switch ( bmsg->msgType ) {
            case SS_ADD_LIST_THREAD:
                mutex_lock(&ssListTpool.plock);
                if(ssListTpool.threadCnt >= SS_max_list_thds) {
                    mutex_unlock(&ssListTpool.plock);
                    break;
                }
                ssListTpool.threadCnt++;
                mutex_unlock(&ssListTpool.plock);
                /* Can't hold lock over kernel_thread call */
                if (!kernel_thread(first_task,ss_list_thd_pool)) {
                    mutex_lock(&ssListTpool.plock);
                    ssListTpool.threadCnt--;
                    mutex_unlock(&ssListTpool.plock);
                }
                break;
            case SS_ADD_WORK_THREAD:
                mutex_lock(&ssWorkTpool.plock);
                if(ssWorkTpool.threadCnt >= SS_max_work_thds) {
                    mutex_unlock(&ssWorkTpool.plock);
                    break;
                }
                ssWorkTpool.threadCnt++;
                mutex_unlock(&ssWorkTpool.plock);
                /* Can't hold lock over kernel_thread call */
                if (!kernel_thread(first_task,ss_work_thd_pool)) {
                    mutex_lock(&ssWorkTpool.plock);
                    ssWorkTpool.threadCnt--;
                    mutex_unlock(&ssWorkTpool.plock);
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
                    msgq_free_msg( ssBossQH, bmsg );
                    ss_queues_destroy();

                    /* notify message sender that shutdown is completed */
                    cond_broadcast (&ss_stopped_cv);

                    /* hari-kari */
                    (void) thread_terminate(current_thread());
                    thread_halt_self();
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
    struct fileSetNode *fsp = NULL;
    int vdi ;
    vdT* vdp;
    unsigned long curBlkIO;  /* current IO count on volume */
    int msg_rev_cnt = 0;
    int hot_chk_rev_cnt = 0, zero_io_rev_cnt = 0;
    int frag_chk_rev_cnt=0;
    statusT sts=EOK;
    int short_q_depth = (SS_pct_msgq_more_thds * SS_INIT_MSGQ_SIZE)/100;

    while(TRUE) {

        assert_wait(NULL, FALSE);
        thread_set_timeout(hz * SS_VD_SAMPLE_INTERVAL);
        thread_block();
 
        if(SS_is_running == FALSE) {
            /* send message to boss thread letting it know 
             * we are all done 
             */
            if((bmsg = 
               (ssBossMsgT *)msgq_alloc_msg(ssBossQH)) != NULL) {
                bmsg->msgType = SS_MONITOR_STOPPED;
                msgq_send_msg(ssBossQH, bmsg);
            }
            (void) thread_terminate(current_thread());
            thread_halt_self();
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
            ss_do_periodic_tasks(SS_HOT_LIST_CHK);
            hot_chk_rev_cnt=0;
        }

        if(++zero_io_rev_cnt >= SS_LULL_CHK_REVS) {
            ss_do_periodic_tasks(SS_LULL_CHK);
            zero_io_rev_cnt=0;
        }

        if(++frag_chk_rev_cnt >= SS_FRAG_CHK_REVS) {
            ss_do_periodic_tasks(SS_FRAG_CHK);
            frag_chk_rev_cnt=0;
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
    statusT sts;

    while (TRUE) {
        /* Wait for something to do */
        msg = (ssWorkMsgT *)msgq_recv_msg( ssWorkQH );

        /* check for a shutdown */
        if(ssWorkTpool.shutdown == TRUE) {
            ssBossMsgT *bmsg=NULL;
            msgq_free_msg( ssWorkQH, msg );
            mutex_lock(&ssWorkTpool.plock);
            ssWorkTpool.threadCnt--;
            mutex_unlock(&ssWorkTpool.plock);
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
            (void) thread_terminate(current_thread());
            thread_halt_self();
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
    ssListMsgT *msg;
    statusT sts;
    ssFragLLT *fp;
    ssHotLLT *hp;
    ssBossMsgT *bmsg;
    int wday;

    while (TRUE) {
        /* Wait for something to do */
        msg = (ssListMsgT *)msgq_recv_msg( ssListQH );

        /* check for a shutdown */
        if(ssListTpool.shutdown == TRUE) {

            msgq_free_msg( ssListQH, msg );

            mutex_lock(&ssListTpool.plock);
            ssListTpool.threadCnt--;
            mutex_unlock(&ssListTpool.plock);
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
            (void) thread_terminate(current_thread());
            thread_halt_self();
            /* NOT REACHED */
        }

        switch ( msg->msgType ) {
          case FRAG_UPDATE_ADD:

              /* Put this fragmented file onto the list so that it can 
               * be defragmented later by vfast.
               */
              ss_insert_frag_onto_list( msg->frag.vdi,
                                       msg->tag,
                                       msg->bfSetId,
                                       msg->frag.fragRatio,
                                       msg->frag.xtntCnt );

              break;

          case HOT_ADD_UPDATE_WRITE:
          case HOT_ADD_UPDATE_MISS:

              /* Allocate a list entry for the hot file */
              hp = (ssHotLLT *)(ms_malloc( sizeof(ssHotLLT) ));
              if ( !hp ) {
                  break;
              }

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
              if ( !hp ) {
                  break;
              }

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
    int activeDmnCnt = 0;
    int desiredAveMsgCnt = 0;
    int totListMsgSendCnt = 0;
    int totListMsgDropCnt = 0;
    extern mutexT DmnTblMutex;
    extern domainT *DmnSentinelP;


    /* first count up the number of active domains that also have 
     * vfast activated.  Also total up the number of msgs sent 
     * to all the active dmns.
     */
    mutex_lock( &DmnTblMutex );

    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        mutex_unlock( &DmnTblMutex );
        return;
    }

    dmnP = DmnSentinelP;        /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;  /* get reference so domain can't dissappear */
    mutex_unlock( &DmnTblMutex );
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

        mutex_lock( &DmnTblMutex );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            thread_wakeup((vm_offset_t)&dmnP->dmnRefWaiters);
        }

        dmnP = dmnP->dmnFwd;       /* get next ... */
        if (dmnP != DmnSentinelP)  { 
            ++dmnP->dmnAccCnt;     /* add our reference */
        }
        mutex_unlock( &DmnTblMutex );
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
    mutex_lock( &DmnTblMutex );
    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        mutex_unlock( &DmnTblMutex );
        return;
    }

    dmnP = DmnSentinelP; /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;   /* get our reference for the adjustment */
    mutex_unlock( &DmnTblMutex );
    do
    {
        
        if((dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE) &&
            (dmnP->ssDmnInfo.ssFirstMountTime != 0)  &&
            (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND))  {

            /* adjust each active dmn's hot msgs to achieve average
             * raising hot msgs threshold(hit cnts) reduces msgs
             * lowerint hot msgs threshold reduces msgs
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

        mutex_lock( &DmnTblMutex );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            thread_wakeup((vm_offset_t)&dmnP->dmnRefWaiters);
        }

        dmnP = dmnP->dmnFwd;       /* get next ... */
        if (dmnP != DmnSentinelP)  {
            ++dmnP->dmnAccCnt;          /* add our reference */
        }
        mutex_unlock( &DmnTblMutex );
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
    extern mutexT DmnTblMutex;
    extern domainT *DmnSentinelP;
    domainT *dmnP;
    vdIndexT dstVdIndex = -1;
    bfTagT  tag=NilBfTag;
    bfSetIdT setId=nilBfSetId;
    int vdRefed = FALSE;
    vdT *dvdp=NULL;      /* destination vd */
    vdT *vdp=NULL;       /* possible vd needs vfast work */
    int vdi, vdCnt=0;
    unsigned long ioBlkCnt;
    statusT sts;
    int fsetMounted=0;
    bfSetT *bfSetp=NULL;
    bfAccessT *bfap=NULL;
    struct fileSetNode *fsp;
    struct fragfiles {
        struct fragfiles *fsNext;
        bfSetIdT bfSetId;
        bfTagT fragBfTag;
    };
    struct fragfiles *ffilep=NULL;
    struct fragfiles *ffileHeadp=NULL;
    struct fragfiles *ffileNextp=NULL;

    mutex_lock( &DmnTblMutex );
    if(DmnSentinelP == NULL) {
        /* test for if all domains are deactivated */
        mutex_unlock( &DmnTblMutex );
        return(EOK);
    }

    dmnP = DmnSentinelP; /* grab starting point */
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt;  /* get our ref so domain can't dissappear */
    mutex_unlock( &DmnTblMutex );

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

              case SS_LULL_CHK:
                /* Search for inactivity on any vds. If detected, signal
                 * vfast to start work on that vd.
                 */
                for ( vdi = 1;
                      vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) )
                        continue;

                    ioBlkCnt = vdp->dStat.readblk+vdp->dStat.writeblk;

                    if(!(vdp->blockingQ.ioQLen ||
                         vdp->flushQ.ioQLen || vdp->ubcReqQ.ioQLen ||
                         vdp->consolQ.ioQLen || vdp->devQ.ioQLen) &&
                        (vdp->ssVolInfo.ssLastIOCnt == ioBlkCnt) ) {
                        ss_startios(vdp);
                    }
                    vdp->ssVolInfo.ssLastIOCnt = ioBlkCnt;
                    vd_dec_refcnt( vdp );
                    vdCnt++;
                }
                break;

              case SS_FRAG_CHK:
                /* check for a fragmented frag file */
                if(dmnP->ssDmnInfo.ssDmnDefragment == TRUE) {

                    FILESET_READ_LOCK(&FilesetLock );

                    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext ) {
                        if ( BFSET_VALID(fsp->bfSetp) &&
                             fsp->bfSetp->dmnP == dmnP )
                        {
                            /* create a list of this domains frag files */

                            ffilep = (struct fragfiles *)ms_malloc(sizeof(struct fragfiles));
                            if ( ffilep == NULL ) {
                                ms_printf("ss_do_periodic_tasks; no memory available.\n");
                                sts = ENOMEM;
                                break;
                            }

                            ffilep->bfSetId = fsp->bfSetp->bfSetId;
                            ffilep->fragBfTag = fsp->bfSetp->fragBfTag;

                            /*
                             * Link this fragfile into the list of domain
                             * frag files.  Insert always at top of list.
                             */
                            if ( ffileHeadp != NULL ) {
                                ffilep->fsNext = ffileHeadp;
                            } else {
                                ffilep->fsNext = NULL;  /* first time through */
                            }
                            ffileHeadp = ffilep;

                        }
                    }

                    FILESET_UNLOCK(&FilesetLock);

                    ffilep = ffileHeadp;
                    while ( ffilep != NULL ) {
                        sts = ss_open_file( ffilep->bfSetId,
                                            ffilep->fragBfTag,
                                            &bfSetp,
                                            &bfap,
                                            &fsetMounted);
                        if ( sts == EOK ) {
                            if ( bfap->ssMigrating == SS_STOP ) {
                                bfap->ssMigrating = SS_MSG_IDLE;
                            }
                            ss_chk_fragratio (bfap);
                            sts = ss_close_file( bfSetp, bfap, fsetMounted );
                            fsetMounted = 0;
                        }
                        /* ignore errors - rechecks next time in routine */
                        ffilep = ffilep->fsNext;
                    }
                }
                break;

              default:
                      break;

            } /* end switch */
        }

        /* MUST complete this section for each domain! */
        mutex_lock( &DmnTblMutex );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            thread_wakeup((vm_offset_t)&dmnP->dmnRefWaiters);
        }

        ffilep = ffileHeadp;
        while ( ffilep != NULL ) {
            ffileNextp = ffilep->fsNext;
            ms_free(ffilep);
            ffilep = ffileNextp;
        }
        ffileHeadp = NULL;

        dmnP = dmnP->dmnFwd;       /* get next domain... */
        if(dmnP != DmnSentinelP) { /* if we've wrapped - don't bump */
            ++dmnP->dmnAccCnt;     /* add our reference */
        }
        mutex_unlock( &DmnTblMutex );
    } while (dmnP != DmnSentinelP);

HANDLE_EXCEPTION:

    ffilep = ffileHeadp;
    while ( ffilep != NULL ) {
        ffileNextp = ffilep->fsNext;
        ms_free(ffilep);
        ffilep = ffileNextp;
    }

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
ss_dmn_activate(domainT *dmnP,u_long flag)
{
    vdT *logVdp = NULL;
    bfAccessT *mdap;
    statusT sts;
    bsSSDmnAttrT ssAttr;
    u_long dmnState;

    /*
     * Check validity of pointer.
     */
    MS_SMP_ASSERT(dmnP);

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = logVdp->rbmtp;
    } else {
        mdap = logVdp->bmtp;
    }

    /* fetch ss record from vd where the log is, if there */
    sts = bmtr_get_rec(mdap,
                       BSR_DMN_SS_ATTR,
                       &ssAttr,
                       sizeof( ssAttr ) );
    if (sts == EBMTR_NOT_FOUND) {
        /* init and write out a new default record */
        ss_init_dmnInfo(dmnP);

        sts = ss_put_rec (dmnP);
        if(sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        return;

    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    ss_init_dmnInfo(dmnP); /* init mutexes and set default values */

    /* fill in dmnP->ssDmnInfo from on-disk ssAttr and return */
    ss_copy_rec_from_disk(&ssAttr, dmnP); 
    return;

HANDLE_EXCEPTION:
    if ( ( flag & M_FAILOVER ) && ( flag & M_GLOBAL_ROOT ) )
	dmnState = M_GLOBAL_ROOT;
    else
	dmnState = 0;		
    ss_change_state(dmnP->domainName, SS_ERROR, &dmnState, 1);
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
    int vdi, vdCnt=0;
    vdT* vdp;
 
    MS_SMP_ASSERT(dmnP);

    if(flag == TRUE) {
        mutex_lock( &dmnP->ssDmnInfo.ssDmnLk );
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

        mutex_lock( &vdp->ssVolInfo.ssVdMsgLk );
        vdp->ssVolInfo.ssVdMsgState = SS_STOP;
        mutex_unlock( &vdp->ssVolInfo.ssVdMsgLk );

        mutex_lock( &vdp->ssVolInfo.ssVdMigLk );
        vdp->ssVolInfo.ssVdMigState = SS_ABORT;
        mutex_unlock( &vdp->ssVolInfo.ssVdMigLk );

        cond_broadcast (&vdp->ssVolInfo.ssContMig_cv);
        SS_TRACE(vdp,0,0,0,0);
        vd_dec_refcnt( vdp );
        vdCnt++;
    }

    /* Wait for any working vfast threads to finish their current
     * work cycle and notice we are deactivating this domain.
     */
    if (dmnP->ssDmnInfo.ssDmnSSThdCnt > 0) {
        dmnP->ssDmnInfo.ssDmnSSWaiters++;
        assert_wait_mesg( (vm_offset_t)&dmnP->ssDmnInfo.ssDmnSSWaiters, FALSE,
                          "ssDmnSSThdCnt" );
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );
        SS_TRACE(vdp,0,0,0,0);
        thread_block();
    } else {
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );
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
    mutex_destroy( &dmnP->ssDmnInfo.ssDmnLk );
    mutex_destroy( &dmnP->ssDmnInfo.ssDmnHotLk );
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
    mutex_destroy( &vdp->ssVolInfo.ssVdMsgLk );
    mutex_destroy( &vdp->ssVolInfo.ssFragLk );
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
 * The dmnState flag when used as an input may contain the values 0 0r
 * M_GLOBAL_ROOT. If dmnState carries the M_GLOBAL_ROOT it indicates a
 * failover of the cluster_root domain and the DmnTblLock need not be
 * taken and this controls the call to bs_bfdmn_tbl_activate accordingly.
 * As an ouput,dmnState holds the value of previous state of vfast. 
 * 
 ********************************************************************/

statusT
ss_change_state(char *domain_name,    /* in */
                ssDmnOpT state,       /* in */
                u_long  *dmnState,    /* in & out */
                int write_out_record) /* in */
{
    statusT sts;
    struct timeval new_time;
    int vdi, vdCnt=0;
    vdT* vdp;
    domainT *dmnP;
    bfDomainIdT domainId;
    int dmnActive=FALSE, dmnOpen = FALSE;
    int ssDmnLocked = FALSE;
    u_long recoveryFlag = *dmnState;		
    
    if ( recoveryFlag & M_GLOBAL_ROOT )
        sts = bs_bfdmn_tbl_activate( domain_name,
                                     M_GLOBAL_ROOT,
                                     &domainId );
    else   
        sts = bs_bfdmn_tbl_activate( domain_name,
                                     0,
                                     &domainId );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmnOpen = TRUE;

    MS_SMP_ASSERT(dmnP);
    if(dmnP->ssDmnInfo.ssDmnState == state) {
        /* already set to this new state-abort*/
        RAISE_EXCEPTION(EOK);
    }

    mutex_lock( &dmnP->ssDmnInfo.ssDmnLk );
    ssDmnLocked = TRUE;

    /* if this is first time activating, record the First mount time*/
    if((dmnP->ssDmnInfo.ssFirstMountTime == 0) &&
       (state == SS_ACTIVATED || state == SS_SUSPEND)) {
        TIME_READ(new_time);
        dmnP->ssDmnInfo.ssFirstMountTime = new_time.tv_sec;
    }

    *dmnState = dmnP->ssDmnInfo.ssDmnState;

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
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );
        ssDmnLocked = FALSE;
    }

    if(dmnP->ssDmnInfo.ssDmnState == SS_DEACTIVATED) {
        ss_dealloc_hot(dmnP);
        for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
            if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) )
                continue;

            ss_dealloc_frag_list(vdp);
            vd_dec_refcnt( vdp );
            vdCnt++;
        }
    }

    if(write_out_record == 1) {
        /* write out on-disk vfast record here. */
        sts = ss_put_rec (dmnP);
        if(sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
    }

HANDLE_EXCEPTION:

    if (dmnOpen) {
        if(ssDmnLocked == TRUE) {
            mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );
            ssDmnLocked = FALSE;
        }
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
	if ( recoveryFlag == M_GLOBAL_ROOT )
        	bs_bfdmn_deactivate( domainId,M_GLOBAL_ROOT );
	else
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
void
ss_init_dmnInfo(domainT *dmnP)
{
    bzero( (char *)&dmnP->ssDmnInfo, sizeof( struct ssDmnInfo ) );
    mutex_init3(&dmnP->ssDmnInfo.ssDmnLk, 0, 
                  "ssDmnLk", ADVdomainT_ssDmnLk_lockinfo);
    mutex_init3(&dmnP->ssDmnInfo.ssDmnHotLk, 0, 
                 "ssDmnHotLk", ADVdomainT_ssDmnHotLk_lockinfo);

    dmnP->ssDmnInfo.ssDmnState                  = SS_DEACTIVATED;
    dmnP->ssDmnInfo.ssFirstMountTime            = 0;
    dmnP->ssDmnInfo.ssDmnHotWorking             = FALSE;
    dmnP->ssDmnInfo.ssDmnHotPlaced              = FALSE;
    dmnP->ssDmnInfo.ssDmnDirectIo               = TRUE;
    dmnP->ssDmnInfo.ssDmnDefragment             = FALSE;
    dmnP->ssDmnInfo.ssDmnSmartPlace             = FALSE;
    dmnP->ssDmnInfo.ssDmnBalance                = FALSE;

    /* UI configurable options */
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = SS_INIT_PCT_IOS_WHEN_BUSY;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssPctIOs = 100 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssPctIOs = 0;
    dmnP->ssDmnInfo.ssAccessThreshHits       = SS_INIT_FILE_ACC_THRESH;
    dmnP->ssDmnInfo.ssFilesDefraged          = 0;
    dmnP->ssDmnInfo.ssPagesDefraged          = 0;
    dmnP->ssDmnInfo.ssPagesBalanced          = 0;
    dmnP->ssDmnInfo.ssFilesIOBal             = 0;
    dmnP->ssDmnInfo.ssExtentsConsol          = 0;
    dmnP->ssDmnInfo.ssPagesConsol            = 0;
    dmnP->ssDmnInfo.ssSteadyState            = SS_INIT_STEADY_STATE;
    dmnP->ssDmnInfo.ssMinutesInHotList       = SS_INIT_MINUTES_HOT_LST;
    dmnP->ssDmnInfo.ssReserved0              = 0;
    dmnP->ssDmnInfo.ssReserved1              = 0;
    dmnP->ssDmnInfo.ssReserved2              = 0;

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotFwd =
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr);
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotBwd =
          (struct ssHotEntry *) (&dmnP->ssDmnInfo.ssDmnHotHdr);
    dmnP->ssDmnInfo.ssDmnHotHdr.ssHotListCnt = 0;
    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
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
    mutex_init3(&vdp->ssVolInfo.ssVdMsgLk, 0, 
                 "ssVdMsgLk", ADVvdT_ssVdMsgLk_lockinfo);
    mutex_init3(&vdp->ssVolInfo.ssVdMigLk, 0, 
                 "ssVdMigLk", ADVvdT_ssVdMigLk_lockinfo);
    mutex_init3(&vdp->ssVolInfo.ssFragLk, 0, 
                 "ssFragLk", ADVvdT_ssFragLk_lockinfo);
    mutex_lock( &vdp->ssVolInfo.ssVdMigLk );
    cv_init( &vdp->ssVolInfo.ssContMig_cv );
    vdp->ssVolInfo.ssVdMigState = SS_MIG_IDLE;
    mutex_unlock( &vdp->ssVolInfo.ssVdMigLk );
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

void
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
    dmnP->ssDmnInfo.ssDmnSmartPlace = ssAttr->ssDmnSmartPlace;
    dmnP->ssDmnInfo.ssDmnBalance = ssAttr->ssDmnBalance;
    dmnP->ssDmnInfo.ssDmnVerbosity = ssAttr->ssDmnVerbosity;
    dmnP->ssDmnInfo.ssDmnDirectIo = ssAttr->ssDmnDirectIo;
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = 
                            ssAttr->ssMaxPercentOfIoWhenBusy;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssPctIOs = 100 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssPctIOs = 0;
    dmnP->ssDmnInfo.ssAccessThreshHits = ssAttr->ssAccessThreshHits;
    dmnP->ssDmnInfo.ssFilesDefraged = ssAttr->ssFilesDefraged;
    dmnP->ssDmnInfo.ssPagesDefraged = ssAttr->ssPagesDefraged;
    dmnP->ssDmnInfo.ssPagesBalanced = ssAttr->ssPagesBalanced;
    dmnP->ssDmnInfo.ssFilesIOBal = ssAttr->ssFilesIOBal;
    dmnP->ssDmnInfo.ssExtentsConsol = ssAttr->ssExtentsConsol;
    dmnP->ssDmnInfo.ssPagesConsol = ssAttr->ssPagesConsol;
    dmnP->ssDmnInfo.ssSteadyState = (uint32T)ssAttr->ssSteadyState;
    dmnP->ssDmnInfo.ssMinutesInHotList = ssAttr->ssMinutesInHotList;
    dmnP->ssDmnInfo.ssReserved0 = (uint32T)ssAttr->ssReserved0;
    dmnP->ssDmnInfo.ssReserved1 = (uint32T)ssAttr->ssReserved1;
    dmnP->ssDmnInfo.ssReserved2 = (uint32T)ssAttr->ssReserved2;
    return;
}

/*********************************************************************
 *
 * This routine copies the in memory domain structure to disk.
 *
 ********************************************************************/

void
ss_copy_rec_to_disk(domainT      *dmnP,    /* in */
                    bsSSDmnAttrT *ssAttr)  /* out */
                    
{
    /* user configurable by domain */
    ssAttr->ssDmnState = dmnP->ssDmnInfo.ssDmnState;
    ssAttr->ssDmnDefragment = dmnP->ssDmnInfo.ssDmnDefragment;
    ssAttr->ssDmnSmartPlace = dmnP->ssDmnInfo.ssDmnSmartPlace;
    ssAttr->ssDmnBalance = dmnP->ssDmnInfo.ssDmnBalance;
    ssAttr->ssDmnVerbosity = dmnP->ssDmnInfo.ssDmnVerbosity;
    ssAttr->ssDmnDirectIo = dmnP->ssDmnInfo.ssDmnDirectIo;

    /* user hidden configurable by domain */
    ssAttr->ssMinutesInHotList = dmnP->ssDmnInfo.ssMinutesInHotList;
    ssAttr->ssMaxPercentOfIoWhenBusy = 
                      dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    ssAttr->ssAccessThreshHits = dmnP->ssDmnInfo.ssAccessThreshHits;
    ssAttr->ssSteadyState = (uint16T)dmnP->ssDmnInfo.ssSteadyState;

    /* statistics */
    ssAttr->ssFilesDefraged = dmnP->ssDmnInfo.ssFilesDefraged;
    ssAttr->ssPagesDefraged = dmnP->ssDmnInfo.ssPagesDefraged;
    ssAttr->ssPagesBalanced = dmnP->ssDmnInfo.ssPagesBalanced;
    ssAttr->ssFilesIOBal = dmnP->ssDmnInfo.ssFilesIOBal;
    ssAttr->ssExtentsConsol = dmnP->ssDmnInfo.ssExtentsConsol;
    ssAttr->ssPagesConsol = dmnP->ssDmnInfo.ssPagesConsol;

    /* extras on-disk */
    ssAttr->ssReserved0 = (uint16T)dmnP->ssDmnInfo.ssReserved0;
    ssAttr->ssReserved1 = (uint16T)dmnP->ssDmnInfo.ssReserved1;
    ssAttr->ssReserved2 = (uint16T)dmnP->ssDmnInfo.ssReserved2;
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
    int ftxStarted = FALSE;
    int vd_refed = FALSE;

    MS_SMP_ASSERT(dmnP);

    /*
     * If there has been a domain_panic, do not start another root
     * transaction.
     */
    if ( dmnP->dmn_panic ) {
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    /* Be sure to bump the refCnt until we are done */
    if ( !(logVdp = 
         vd_htop_if_valid(BS_BFTAG_VDI(dmnP->ftxLogTag), 
                                     dmnP, TRUE, FALSE)) ) {
        RAISE_EXCEPTION( EBAD_VDI );
    }
    vd_refed = TRUE;

    /* reset vfast state if we have been relocated by cfs */
    if(dmnP->ssDmnInfo.ssDmnState == SS_CFS_RELOC) {
        /* don't set update on-disk flag or we would recall this routine! */
        dmnP->ssDmnInfo.ssDmnState = SS_ACTIVATED;
    }

    /* write the record to disk structure */

    ss_copy_rec_to_disk(dmnP, &ssAttr);

    sts = FTX_START_N(FTA_NULL, &ftx, FtxNilFtxH, dmnP, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION( EBAD_FTXH );
    }
    ftxStarted = TRUE;

    /*
     * Install OR Update ss record in the BMT (V3) or RBMT (V4).
     */
    sts = bmtr_put_rec(
        (RBMT_THERE(dmnP) ? logVdp->rbmtp : logVdp->bmtp),
        BSR_DMN_SS_ATTR,
        &ssAttr,
        sizeof( bsSSDmnAttrT ),
        ftx );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
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
ss_trace( vdT     *vdp,
          uint16T module,
          uint16T line,
          long    value1,
          long    value2,
          long    value3,
          long    value4
        )
{
    register ssTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    vdp->ss_trace_ptr = (vdp->ss_trace_ptr + 1) % SS_TRACE_HISTORY;
    te = &vdp->ss_trace_buf[vdp->ss_trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val1 = value1; /* tag */
    te->val2 = value2; /* offset */
    te->val3 = value3; /* cnt */
    te->val4 = value4; /* destinationBlk */

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_SS_TRACE */


#ifdef SS_DEBUG
void
ss_sleep(int retry)
{
    printf("ss_xxx_thd_pool: thd 0x%x going to sleep\n",
           (int)(current_thread()->thread_self));
    while(retry)  {
        timeout((void(*)(void *))wakeup, &retry, 1*hz);
        (void)mpsleep(&retry, PZERO, (char *)NULL, 0, (void *) NULL, 0);
        retry--;
    }
    printf("ss_xxx_thd_pool: thd 0x%x waking from sleep\n",
           (int)(current_thread()->thread_self));
}
#endif 

/*********** end of debug routines  ****************/

/********   start of frag list routines  *******/

/*********************************************************************
 *
 * Count the number of extents that are not contiguous to each other.
 * skip holes.  Since we want raw uncontiguous extent count, ignore
 * perm and regular extent terminators in the clones.  For this 
 * routine, a hole is a hole is a hole.
 *
 * lock bfap->xtntMap_lk is held and released by callers!
 *
 ********************************************************************/

int
ss_xtnt_counter(bfAccessT *bfap)
{
    bsInMemXtntMapT *xtntMap=NULL;
    int totXtnts=0;
    uint32T i,j;
    uint64T curr_blk, prev_blk, last_valid_blk;
    uint32T last_pageCnt=0;
    statusT sts=EOK;

    switch ( bfap->xtnts.type ) {
        case BSXMT_APPEND:
                xtntMap = bfap->xtnts.xtntMap;
                break;
        case BSXMT_STRIPE:
                /* stripe files defragment not supported in vfast yet */
                return(0);
        default:
                MS_SMP_ASSERT(0);
    }

    MS_SMP_ASSERT(xtntMap);

    /* determine total number of non-contiguous extents for this file */
    for ( i = 0; i < xtntMap->validCnt; i++ ) {

        if((i > 0) && (xtntMap->subXtntMap[i].vdIndex !=  /* must be on same vd */
                      xtntMap->subXtntMap[i-1].vdIndex))
            last_pageCnt = 0;  

        for ( j = 0; j < xtntMap->subXtntMap[i].cnt; j++) {
            curr_blk = xtntMap->subXtntMap[i].bsXA[j].vdBlk;

            if(j==0) {
                if((curr_blk != XTNT_TERM) && (curr_blk != PERM_HOLE_START)) {
                    /* first extent in mcell is not describing a hole */
                    totXtnts++;
                }
                continue; /* skip first extent */
            }

            prev_blk = xtntMap->subXtntMap[i].bsXA[j-1].vdBlk;

            if((prev_blk != XTNT_TERM) && (prev_blk != PERM_HOLE_START)) {
                /* save the pageCnt & blk from this extent to the previous
                 * extent if the previous extent was not a hole terminator.
                 */
                last_pageCnt = xtntMap->subXtntMap[i].bsXA[j].bsPage -
                               xtntMap->subXtntMap[i].bsXA[j-1].bsPage;
                last_valid_blk = xtntMap->subXtntMap[i].bsXA[j-1].vdBlk;
            }

            if ( curr_blk == XTNT_TERM || curr_blk == PERM_HOLE_START ) {
                /* skip all hole extent terminators */
                continue;
            }

            if(last_pageCnt) {
                /* coming out of a hole */
                /* test for blocks not adjacent to each other */
                if(!((last_valid_blk + (last_pageCnt * bfap->bfPageSz)) == 
                        curr_blk)) {
                    totXtnts ++;
                }
            } else {
                /* not coming out of a hole */
                /* test for current extent's blocks not adjacent to last one */
                if(!((prev_blk +
                   ((xtntMap->subXtntMap[i].bsXA[j].bsPage -
                     xtntMap->subXtntMap[i].bsXA[j-1].bsPage ) * bfap->bfPageSz)) ==
                         curr_blk)) {
                    totXtnts ++;
                }
            }
            last_pageCnt = 0;
        }
    }  /* end for */
    return(totXtnts);
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
    uint32T totXtnts = 0;
    bfSetIdT bfSetId;
    ssListMsgT *listmsg;
    uint32T allocPageCnt = 0;
    vdT *vdp;
    statusT sts=EOK;
    int vd_refed = FALSE;

    MS_SMP_ASSERT(bfap);

    /* ignore files under directio control if user desires */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
         (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO)) {
        /* fset is a directio */
        RAISE_EXCEPTION (EOK);
    }

    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) && 
       (bfap->bfVp) && (bfap->bfVp->v_flag & VDIRECTIO)) {
        /* file is a directio */
        RAISE_EXCEPTION (EOK);
    }

    if(bfap->ssMigrating==SS_STOP) {
        /* vfast closed this file with an error, reset flag
         * to allow next closer to check for placement on 
         * frag list.
         */
        bfap->ssMigrating = SS_MSG_IDLE; /* allows to be placed again */
        RAISE_EXCEPTION (EOK);
    }

    /* check for ss up and activated on this domain */
    if ((SS_is_running == FALSE) ||
        (bfap->ssMigrating != SS_MSG_IDLE) ||
        (bfap->dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (bfap->dmnP->state != BFD_ACTIVATED) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (bfap->dmnP->mountCnt == 0) ||
        (bfap->dmnP->ssDmnInfo.ssDmnDefragment == FALSE) ||
         !(bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_CFS_MOUNT ||
           bfap->dmnP->ssDmnInfo.ssDmnState == SS_CFS_UMOUNT) ) 
    {
        RAISE_EXCEPTION (EOK);
    }
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    allocPageCnt = bfap->xtnts.allocPageCnt;
    /* go get the number of not contiguous extents in the file */
    totXtnts = ss_xtnt_counter(bfap);
    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    if (totXtnts <= 1) {  /* cant do any better than 1, eh? */
        RAISE_EXCEPTION (EOK);
    }

    /* bump the ref count on the vd */
    vdp = vd_htop_if_valid(bfap->primVdIndex, bfap->dmnP, TRUE, FALSE);
    if ( vdp == NULL ) {
        /* only this vd is activated, abort */
        RAISE_EXCEPTION (EOK);
    }
    vd_refed = TRUE;

    bs_bfs_get_set_id( bfap->bfSetp, &bfSetId );

    if ( (EXTENTS_FRAG_RATIO(allocPageCnt,totXtnts) <
           vdp->ssVolInfo.ssFragHdr.ssFragHdrThresh) ||
         ( vdp->ssVolInfo.ssFragHdr.ssFragListCnt < 
           SS_INIT_FRAG_LST_SZ ) )    {

        if((listmsg = 
           (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
            listmsg->msgType = FRAG_UPDATE_ADD;
            listmsg->tag = bfap->tag;
            listmsg->bfSetId = bfSetId;
            listmsg->frag.vdi = vdp->vdIndex;
            listmsg->frag.fragRatio = (ulong)
                  EXTENTS_FRAG_RATIO(allocPageCnt,totXtnts);
            listmsg->frag.xtntCnt = totXtnts;
            msgq_send_msg(ssListQH, listmsg);
        } else {
            mutex_lock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
            bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
            mutex_unlock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
        }
    }

    mutex_lock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
    bfap->dmnP->ssDmnInfo.ssDmnListMsgSendCnt++;
#ifdef DEBUG
    bfap->dmnP->ssDmnInfo.ssDmnMsgSendAttmptCntFrag++;
#endif
    mutex_unlock( &bfap->dmnP->ssDmnInfo.ssDmnLk );

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

void
ss_insert_frag_onto_list( vdIndexT vdi,
                       bfTagT tag,
                       bfSetIdT bfSetId,
                       uint64T fragRatio, /* primary sort in list */
                       uint32T xtntCnt)  /* secondary sort in list */
{
    domainT *dmnP;
    bfSetT *bfSetp;
    vdT* vdp=NULL;
    ssFragLLT *currp, *nextp; /* list pointer */
    ssFragHdrT *fhp;
    int inserted = FALSE;
    int setOpen = FALSE, vd_refed= FALSE;
    statusT sts;
    ssFragLLT *fp = NULL;

    /* Now open the set */
    sts = rbf_bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
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
        goto _cleanup;
    }

    /* bump the ref count on the vd */
    vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE);
    if ( vdp == NULL ) {
        /* must be an rmvol going on - just delete this msg */
        goto _cleanup;
    }
    vd_refed = TRUE;

    /* Allocate a list entry for the fragmented file */
    fp = (ssFragLLT *)(ms_malloc( sizeof(ssFragLLT) ));
    if(fp == NULL) {
        goto _cleanup;
    }

    /* Initialize fields; ms_malloc() inited struct to zeros.  */
    fp->ssFragTag     = tag;
    fp->ssFragBfSetId = bfSetId;
    fp->ssFragRatio   = fragRatio;
    fp->ssXtntCnt     = xtntCnt;

    mutex_lock(&vdp->ssVolInfo.ssFragLk);
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

    mutex_unlock(&vdp->ssVolInfo.ssFragLk);

_cleanup:

    if (vd_refed) {
        vd_dec_refcnt(vdp);
    }
    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
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
ss_delete_from_frag_list(vdT *vdp,   /* in */
                      bfTagT tag,   /* in */
                      bfSetIdT bfSetId)  /* in */
{
    ssFragLLT *currp, *nextp; /* curr, next entries */
    ssFragHdrT *fhp;   /* pointer to frag list header */
    statusT sts;

    MS_SMP_ASSERT(vdp);
    MS_SMP_ASSERT(SLOCK_HOLDER(&vdp->ssVolInfo.ssFragLk.mutex));
    fhp = &vdp->ssVolInfo.ssFragHdr;

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
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
    mutex_lock(&vdp->ssVolInfo.ssFragLk);
    fhp = &vdp->ssVolInfo.ssFragHdr;
    MS_SMP_ASSERT(fhp);

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        mutex_unlock(&vdp->ssVolInfo.ssFragLk);
        return(NULL);
    }

    fp = fhp->ssFragRatFwd;

    if((fp != (ssFragLLT *)fhp ) &&
        (BS_BFTAG_REG(fp->ssFragTag))) {
        /* found the worst one in list */
        mutex_unlock(&vdp->ssVolInfo.ssFragLk);
        return(fp);
    }

    mutex_unlock(&vdp->ssVolInfo.ssFragLk);
    return(NULL);
}


/*********************************************************************
 *
 * This routine deallocates the frag list for the given vd.
 *
 ********************************************************************/

void
ss_dealloc_frag_list(vdT *vdp)           /* in */
{
    ssFragHdrT *fhp;
    ssFragLLT *currp, *nextp;  /* entry pointer */

    MS_SMP_ASSERT(vdp);
    mutex_lock(&vdp->ssVolInfo.ssFragLk);
    fhp = &vdp->ssVolInfo.ssFragHdr;
    MS_SMP_ASSERT(fhp);

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        mutex_unlock(&vdp->ssVolInfo.ssFragLk);
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

    mutex_unlock(&vdp->ssVolInfo.ssFragLk);
    return;
}

#ifdef SS_DEBUG
/* DEBUG 
 * important - hold lock while calling or can end up in infinite loop 
 */
void
print_frag_list(vdT *vdp)           /* in  */
{
    ssFragLLT *fp;  /* entry pointer */
    ssFragHdrT *fhp;

    MS_SMP_ASSERT(vdp);
    fhp = &vdp->ssVolInfo.ssFragHdr;
    MS_SMP_ASSERT(fhp);

    /* check for a null list first! */
    if(fhp->ssFragListCnt == 0) {
        printf("FRAG LIST: dmn(%d) vdIndex(%d) count(%d) fhp(0x%x) EMPTY!\n",
            vdp->dmnP->domainName, vdp->vdIndex, fhp->ssFragListCnt, fhp);
        return;
    }

    /* test forward */
    fp = fhp->ssFragRatFwd;
    printf("FRAG LIST: dmn(%d) vdIndex(%d) count(%d) fhp(0x%x)\n",
           vdp->dmnP->domainName, vdp->vdIndex, fhp->ssFragListCnt, fhp);
    while ( fp != (ssFragLLT *)fhp ) {
        printf("ENTRY: ssFragTag(0x%x)(0x%x) set(0x%x)(0x%x) ssFragRatio(%ld) ssXtntCnt(%d), fp(0x%x) Fwd(0x%x) Bwd(0x%x)\n",
               (int)fp->ssFragTag.num,
               (int)fp->ssFragTag.seq,
                    fp->ssFragBfSetId.dirTag.num,
                    fp->ssFragBfSetId.dirTag.seq,
                    fp->ssFragRatio,
                    fp->ssXtntCnt,
                    fp,
                    fp->ssFragRatFwd,
                    fp->ssFragRatBwd);
        if(fp->ssFragRatFwd == NULL) break;
        fp = fp->ssFragRatFwd;
    }
    /* test backward */
    fp = fhp->ssFragRatBwd;
    while ( fp != (ssFragLLT *)fhp ) {
        if(fp->ssFragRatBwd == NULL)  break;
        fp = fp->ssFragRatBwd;
    }
    printf("FRAG LIST: done\n");
    return;
}
#endif

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

#ifdef SS_DEBUG
void
print_pack_list(ssPackHdrT   *php,  /* list head */
                vdT *vdp)           /* in  */
{
    ssPackLLT *p;  /* entry pointer */

    MS_SMP_ASSERT(vdp);
    MS_SMP_ASSERT(php);

    /* check for a null list first! */
    if(php->ssPackListCnt == 0) {
        printf("PACK LIST: dmn(%d) vdIndex(%d) count(%d) EMPTY!\n",
               vdp->dmnP->domainName, vdp->vdIndex, php->ssPackListCnt);
        return;
    }

    p = php->ssPackFwd;
    printf("PACK LIST: dmn(%d) vdIndex(%d) count(%d)\n",
           vdp->dmnP->domainName, vdp->vdIndex, php->ssPackListCnt);
    while ( p != (ssPackLLT *)php ) {

        printf("ENTRY: Tag(0x%x)(0x%x) Set(0x%x)(0x%x) pageOff(%d) \
pageCnt(%d) BlkStart(%ld) BlkEnd(%ld)\n",
               (int)p->ssPackTag.num,
               (int)p->ssPackTag.seq,
                    p->ssPackBfSetId.dirTag.num,
                    p->ssPackBfSetId.dirTag.seq,
                    p->ssPackPageOffset,
                    p->ssPackPageCnt,
                    p->ssPackStartXtBlock,
                    p->ssPackEndXtBlock);
        if(p->ssPackFwd == NULL)  break;
        p = p->ssPackFwd;
    }
    printf("PACK LIST: done\n");
}
#endif

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
    int wday;
    bfSetIdT bfSetId;
    ssListMsgT *listmsg;

    MS_SMP_ASSERT(bfap);
    /* ignore files under directio control if user desires */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
         (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO)) {
        /* fset is a directio */
        return;
    }
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) && 
       (bfap->bfVp) && (bfap->bfVp->v_flag & VDIRECTIO)) {
        /* file is a directio */
        return;
    }

    /* ignore stripe files - do not io balance these */
    if(bfap->xtnts.type != BSXMT_APPEND)
        return;

    switch ( msgType ) {

        case HOT_REMOVE:

            /* send a message to the vfast hot list */
            bs_bfs_get_set_id( bfap->bfSetp, &bfSetId );
           if((listmsg = (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
               listmsg->msgType = HOT_REMOVE;
               listmsg->tag = bfap->tag;
               listmsg->bfSetId = bfSetId;
               msgq_send_msg(ssListQH, listmsg);
           } else {
               mutex_lock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
               bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
               mutex_unlock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
           }
           break;

        case HOT_ADD_UPDATE_MISS:
        case HOT_ADD_UPDATE_WRITE:
            /* send a message to the vfast hot list */

            bs_bfs_get_set_id( bfap->bfSetp, &bfSetId );
            if((listmsg = 
               (ssListMsgT *)msgq_alloc_msg(ssListQH)) != NULL) {
                listmsg->msgType = HOT_ADD_UPDATE_MISS;
                listmsg->tag = bfap->tag;
                listmsg->bfSetId = bfSetId;
                listmsg->hot.vdi = bfap->primVdIndex;
                for(wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                    listmsg->hot.cnt[wday]  = bfap->ssHotCnt[wday];
                    /* reset counter */
                    bfap->ssHotCnt[wday] = 0;
                }
                listmsg->hot.size  = bfap->file_size;

                msgq_send_msg(ssListQH, listmsg);

            } else {
                mutex_lock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
                bfap->dmnP->ssDmnInfo.ssDmnListMsgDropCnt++;
                mutex_unlock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
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
    mutex_lock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
    bfap->dmnP->ssDmnInfo.ssDmnListMsgSendCnt++;
    mutex_unlock( &bfap->dmnP->ssDmnInfo.ssDmnLk );
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
    long leftTotal, leftBwdTotal;
    int wday;

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

void
ss_insert_hot_list(
         ssListMsgTypeT msgType, /* in */
         ssHotLLT *hp)           /* in  - entry */
{
    domainT *dmnP;
    bfSetT *bfSetp;
    ssHotLLT *currp, *nextp; /* curr entry ptr */
    ssHotHdrT *hhp;   /* pointer to hot list header */
    int updated = FALSE, inserted = FALSE;
    struct timeval new_time;
    int setOpen = FALSE;
    statusT sts;
    int wday,newday;
    uint64T newhpCnt, currhpCnt;

    /* Now open the set */
    sts = rbf_bfs_open( &bfSetp, hp->ssHotBfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
        if(hp) ms_free(hp);
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
        goto _cleanup;
    }

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    currp = hhp->ssHotFwd;


    /* check for a new day of the week */
    TIME_READ(new_time);
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

    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );

_cleanup:

    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
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
    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;

    /* check for a null list first! */
    if(hhp->ssHotListCnt == 0) {
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
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
        delp->ssHotFwd->ssHotBwd = delp->ssHotBwd;
        delp->ssHotBwd->ssHotFwd = delp->ssHotFwd;
        delp->ssHotFwd = delp->ssHotBwd = 0;
        ms_free(delp); hhp->ssHotListCnt--;
    }
    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
    return;
} /* end ss_rmvol_from_hotlst */



/*********************************************************************
 *
 * This routine deletes the entry passed in from the hot list if the
 * entries tag is on the list.
 *
 ********************************************************************/

void
ss_del_from_hot_list(ssHotLLT *hp)
{
    domainT *dmnP;
    bfSetT *bfSetp;
    ssHotLLT *currp, *nextp; /* curr, next entry ptrs */
    ssHotHdrT *hhp;   /* pointer to hot list header */
    int setOpen = FALSE;
    statusT sts;

    /* open the set */
    sts = rbf_bfs_open( &bfSetp, 
                        hp->ssHotBfSetId,  
                        BFS_OP_DEF, 
                        FtxNilFtxH );
    if (sts == EOK) {
        if ( ! BFSET_VALID(bfSetp) ) {
            goto _cleanup;
        }
        setOpen = TRUE;
        dmnP = bfSetp->dmnP;
    } else {
        /* file set is not accessible probably because domain
         * has been deactivated or fset is being removed.
         */
        goto _cleanup;
    } 

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
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

    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );

_cleanup:

    if(hp) ms_free(hp);

    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
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
                  int      srcVdIndex,  /* in */
                  statusT  err,         /* in */
                  int      flag )       /* in */
{
    ssHotHdrT *hhp;
    ssHotLLT  *hp=NULL;
    struct timeval new_time;

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    MS_SMP_ASSERT(hhp);
    MS_SMP_ASSERT(flag != 0);

    if ( hhp->ssHotListCnt == 0) {
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
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
                    TIME_READ(new_time);
                    hp->ssHotErrTime = new_time.tv_sec;
                } else
                    hp->ssHotVdi = hp->ssHotPlaced = srcVdIndex;
                mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
                return(srcVdIndex);
            } 

            if(flag==SS_READ) {
                if(hp->ssHotPlaced > 0) {
                    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
                    return(hp->ssHotPlaced);
                } else {
                    /* not a currently active hot IO placed entry */
                    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
                    return (-1);
                }
            }

        } /* end processing of matched entry */
        hp = hp->ssHotFwd;
    } /* end loop entries in list */

    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
    return(-1);
}

#ifdef SS_DEBUG
/* DEBUG ONLY */
void
print_hot_list(ssHotHdrT   *hhp)
{
    ssHotLLT *hp;  /* entry pointer */

    if ( hhp->ssHotListCnt == 0) {
        printf("HOT LIST: hhp(0x%x) list is EMPTY!\n", hhp);
        return;
    }

    printf("HOT LIST: Cnt(%d)\n",hhp->ssHotListCnt);

    hp = hhp->ssHotFwd;
    while ( hp != (ssHotLLT *)hhp ) {

        printf("ENTRY:(%d) tag(0x%x) set(0x%x):: IOCnts(%d)(%d)(%d)(%d)(%d)(%d)(%d))\n",
                    hp->ssHotVdi,
               (int)hp->ssHotTag.num,
                    hp->ssHotBfSetId.dirTag.num,
                    hp->ssHotIOCnt[0],
                    hp->ssHotIOCnt[1],
                    hp->ssHotIOCnt[2],
                    hp->ssHotIOCnt[3],
                    hp->ssHotIOCnt[4],
                    hp->ssHotIOCnt[5],
                    hp->ssHotIOCnt[6]);

        printf("         updtim(%d) enttim(%d) errtim(%d) hp(0x%x) Fwd(0x%x) Bwd(0x%x)\n",
                    hp->ssHotLastUpdTime,
                    hp->ssHotEntryTime,
                    hp->ssHotErrTime,
                    hp,
                    hp->ssHotFwd,
                    hp->ssHotBwd);

        if(hp->ssHotFwd == NULL)  break;
        hp = hp->ssHotFwd;
    }
    printf("HOT LIST: done\n");
}
#endif


/*********************************************************************
 *
 * This routine deallocates all the entries on the hot list for the
 * given domain.
 *
 ********************************************************************/

void
ss_dealloc_hot(domainT *dmnP)  /* in - pointer to header for list */
{
    ssHotHdrT *hhp;
    ssHotLLT *currp, *nextp;  /* entry pointer */

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    MS_SMP_ASSERT(hhp);
    currp = hhp->ssHotFwd;

    /* check for a null list first! */
    if(hhp->ssHotListCnt == 0) {
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
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

    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
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

    if( (SS_is_running == TRUE) &&
        (vdp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
        (vdp->dmnP->state == BFD_ACTIVATED) &&
        (vdp->dmnP->dmnFlag != BFD_DEACTIVATE_IN_PROGRESS) &&
        (vdp->dmnP->dmnFlag != BFD_DEACTIVATE_PENDING) &&
        (vdp->dmnP->mountCnt > 0) &&
        (vdp->dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE ||
         vdp->dmnP->ssDmnInfo.ssDmnDefragment == TRUE ||
         vdp->dmnP->ssDmnInfo.ssDmnBalance == TRUE) ) {

        mutex_lock( &vdp->ssVolInfo.ssVdMsgLk );
        if((vdp->ssVolInfo.ssVdMsgState == SS_MSG_IDLE) &&
           (vdp->ssVolInfo.ssFragHdr.ssFragListCnt)) {
            if((workmsg = (ssWorkMsgT *)msgq_alloc_msg(ssWorkQH)) != NULL) {
                workmsg->msgType = SS_VD_RUN;
                workmsg->vdIndex = vdp->vdIndex;
                workmsg->domainId = vdp->dmnP->domainId;
                msgq_send_msg(ssWorkQH, workmsg);
                vdp->ssVolInfo.ssVdMsgState = SS_POSTED;
            } /* else just drop it - queue is full */
        }
        mutex_unlock( &vdp->ssVolInfo.ssVdMsgLk );

        if (vdp->ssVolInfo.ssVdMigState == SS_PARKED) {
            cond_broadcast (&vdp->ssVolInfo.ssContMig_cv);
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
    /* allow any other threads of equal or higher priority to run */
    PREEMPT_CHECK(current_thread());

    if((vdp->ssVolInfo.ssVdMigState != SS_ABORT) &&
       (SS_is_running == TRUE) &&
       (vdp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
       !(vdp->consolQ.ioQLen || vdp->blockingQ.ioQLen || 
         vdp->ubcReqQ.ioQLen || vdp->flushQ.ioQLen || 
         vdp->devQ.ioQLen)) {
        /* keep going, no other work is being done */
        vdp->ssVolInfo.ssIOCnt=0;
        return(EOK);
    }

    /* check to see if vd is being deactivated or
     * vfast is being stopped before parking. 
     */
    lock_read(&SSLock);
    mutex_lock( &vdp->ssVolInfo.ssVdMigLk );
    if((vdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (vdp->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        mutex_unlock( &vdp->ssVolInfo.ssVdMigLk );
        SS_UNLOCK(&SSLock);
        return(E_WOULD_BLOCK);
    }

    /* Smartstore work thread blocks and waits for next vd inactivity 
     * broadcast from io thd. 
     */
    vdp->ssVolInfo.ssVdMigState = SS_PARKED;
    cond_wait( &vdp->ssVolInfo.ssContMig_cv, &vdp->ssVolInfo.ssVdMigLk );
    SS_UNLOCK(&SSLock);
    SS_TRACE(vdp,0,0,0,0);

    /* check vd for deactivation before continuing to migrate */
    if((vdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (vdp->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        mutex_unlock( &vdp->ssVolInfo.ssVdMigLk );
        return(E_WOULD_BLOCK);
    }

    vdp->ssVolInfo.ssVdMigState = SS_PROCESSING;
    mutex_unlock( &vdp->ssVolInfo.ssVdMigLk );
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
    int closeBfSetFlag = FALSE, closeBitfileFlag = FALSE;
    bfAccessT *bfap=NULL;
    bfSetT *bfSetp=NULL;
    struct fileSetNode *fsnp=NULL;
    struct mount *mp;
    int do_vrele=FALSE;
    int ssThdCntBumped = FALSE;

    /* open the fileset */
    sts = rbf_bfs_open( &bfSetp,
                        bfSetId,
                        BFS_OP_DEF,
                        FtxNilFtxH );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    closeBfSetFlag = TRUE;

    if ( ! BFSET_VALID(bfSetp) ) {
        RAISE_EXCEPTION (E_NO_SUCH_BF_SET);
    }

    /*
     * Vfast generally opens a file to migrate it.
     * Migrate must coordinate with CFS xtnt map tokens.
     * Owning a token guarentees that the xtnt map will not change.
     * The tokens are associated with a vnode. If the fileset is mounted we
     * need to make sure that we get a vnode for this file so that the vfast
     * initiated migration will coordinate with CFS. If the fileset is not
     * mounted we can open the file without a vnode and migrate the file.
     * Since vfast is asynchronous wrt mount and unmount we need to make
     * it synchronous. During a mount or unmount the BFS_BUSY state is set.
     * Attempted ss_open_file calls during these times fail. Vfast may
     * try to open this file later. It vfast opens the file then mounts
     * and unmounts are held off until vfast closes the file (after it
     * migrates it). msfs_unmount & advfs_mountfs wait if ssDmnSSThdCnt
     * is non zero. ss_close_file decrements ssDmnSSThdCnt and wakes a waiter.
     */
    mutex_lock( &bfSetp->dmnP->mutex );
    if ( bfSetp->state == BFS_BUSY ) {
        mutex_unlock( &bfSetp->dmnP->mutex );
        RAISE_EXCEPTION (E_NO_SUCH_BF_SET);
    }

    INC_SSTHDCNT( bfSetp->dmnP );
    ssThdCntBumped = TRUE;
    mutex_unlock( &bfSetp->dmnP->mutex );

    vp = NULL;
    fsnp = (struct fileSetNode *)bfSetp->fsnp;
    if(fsnp) {
        /* fset mounted - get vnode with mount, do vrele */
        mp = fsnp->mountp;
        sts = bf_get(
                 filetag,
                 0,
                 DONT_UNLOCK,
                 mp,
                 &vp
                 );
        if (sts != EOK) {
            /* file was deleted, lets just stop and cleanup */
            RAISE_EXCEPTION (sts);
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
                     0, /* no vnode when not mounted */
                     NULLMT,
                     &vp);
        if (sts != EOK) {
            /* file was deleted, lets just stop and cleanup */
            RAISE_EXCEPTION (sts);
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
        if(do_vrele == TRUE) 
            vrele(bfap->bfVp);
        else {
            /* DO_VRELE since we have vnode */
            bs_close(bfap, MSFS_SS_NOCALL | MSFS_DO_VRELE);
        }
        closeBitfileFlag = FALSE;
    }

    if ( ssThdCntBumped ) {
        DEC_SSTHDCNT( bfSetp->dmnP );
    }

    if (closeBfSetFlag == TRUE) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        closeBfSetFlag = FALSE;
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
        vrele(bfap->bfVp);
    } else{
        bs_close(bfap, MSFS_SS_NOCALL);
    }

    MS_SMP_ASSERT(bfSetp);

    DEC_SSTHDCNT( bfSetp->dmnP );

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    return(EOK);
}


/*********************************************************************
 *
 * This routine determines the file to move for this volume. It calls
 * ss_vd_migrate to actually move the file.
 *
 ********************************************************************/

void
ss_move_file( vdIndexT vdi,      /* in */
              bfDomainIdT domainId,  /* in */
              ssWorkMsgTypeT msgType
            )
{
    vdT *svdp=NULL;
    domainT *dmnP=NULL;
    int domain_open = FALSE, vdRefed = FALSE;
    statusT sts=EOK;
    ssFragLLT *fp=NULL;  /* frag file ptr */
    bfTagT  filetag=NilBfTag;
    bfSetIdT bfSetId=nilBfSetId;
    int alloc_hint = BS_ALLOC_DEFAULT;
    int vdiPlaced;
    vdIndexT dstVdIndex;

    /* bump the access count on the domain so it can't go away
     * while we are working in it
     */
    if ( sts = bs_domain_access(&dmnP, domainId, FALSE)) {
        /* domain can not be activated, just drop msg */
        RAISE_EXCEPTION (sts);
    }
    domain_open = TRUE;

    if ((SS_is_running == FALSE) ||
        (dmnP->ssDmnInfo.ssFirstMountTime == 0) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING) ||
        (dmnP->mountCnt == 0) ||
        (dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED))
    {
        RAISE_EXCEPTION (E_WOULD_BLOCK);
    }

    /* bump the ref count on this vd so it cant be removed */
    svdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE);
    if ( svdp == NULL ) {
        /* must be an rmvol going on - just drop msg */
        RAISE_EXCEPTION (EBAD_VDI);
    }
    vdRefed = TRUE;

    /* Potentially the thread works on vd for a long time,
     * Increment the volume vfast thread count
     */
    mutex_lock( &svdp->ssVolInfo.ssVdMigLk );
    svdp->ssVolInfo.ssVdSSThdCnt++;
    mutex_unlock( &svdp->ssVolInfo.ssVdMigLk );

    /* First check for a hot file move on svdp here.  Select a 
     * file to move by first checking to see if any hot files 
     * need moved. If no hot file is targeted at this vd default 
     * to the frag list check below.
     */
    mutex_lock( &dmnP->ssDmnInfo.ssDmnLk );
    if((dmnP->ssDmnInfo.ssDmnSmartPlace) &&
       (dmnP->ssDmnInfo.ssDmnHotWorking == FALSE) &&
       (!BS_BFTAG_EQL( svdp->ssVolInfo.ssVdHotTag, NilBfTag ))) {

        /* hot file to be moved to this volume. move it. */
        filetag = svdp->ssVolInfo.ssVdHotTag;
        bfSetId = svdp->ssVolInfo.ssVdHotBfSetId;
        /* block out any other threads */
        dmnP->ssDmnInfo.ssDmnHotWorking = TRUE;  
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );

        sts = ss_vd_migrate( filetag,
                             bfSetId,
                             svdp->vdIndex,
                             msgType,
                             BS_ALLOC_MIG_SINGLEXTNT
                             );
#ifdef SS_DEBUG
        printf("ss_vd_migrate:(hotfile) vd(%d) tag(%d.%d) returns sts=%d\n",
        svdp->vdIndex, bfSetId.dirTag.num, filetag.num, sts);
#endif
        /* ENO_SUCH_TAG indicates file was deleted
         * E_NO_SUCH_BF_SET indicates fileset is being deleted by another thread
         * ENO_MORE_BLKS indicates not enough space to finish migrate command
         * E_WOULD_BLOCK indicates vfast was stopped in middle of a migrate
         * E_RSVD_FILE_INWAY - vfast unable to pack cause rsvd files inway.
         * ENOSPC - vfast unable to find or create enough space
         * E_RANGE_NOT_CLEARED real error, range was not cleared by mig_pack_vd_range
         * E_PAGE_NOT_MAPPED - file was truncated, but still defragmented
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
        mutex_lock( &dmnP->ssDmnInfo.ssDmnLk ); 
        dmnP->ssDmnInfo.ssDmnHotWorking = FALSE;
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );
        dmnP->ssDmnInfo.ssDmnHotPlaced = FALSE;
        RAISE_EXCEPTION(EOK);
    } 
    mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );


    if (dmnP->ssDmnInfo.ssDmnDefragment) {

        /* select a fragmented file to move */
        if((fp = ss_select_frag_file(svdp)) == NULL) {
            /* simply return if there is nothing in list */
            RAISE_EXCEPTION (EOK);
        }
        filetag = fp->ssFragTag;
        bfSetId = fp->ssFragBfSetId;

        /* Delete entry from list.
         * If not done below the bs_access above, the bs_access call 
         * will simply cause the list threads to put the entry back 
         * on the list.
         */
        mutex_lock(&svdp->ssVolInfo.ssFragLk);
        ss_delete_from_frag_list(svdp, filetag, bfSetId);
        mutex_unlock(&svdp->ssVolInfo.ssFragLk);

        /* if hot file is about to be placed as a hot file, skip it */
        if((BS_BFTAG_EQL( svdp->ssVolInfo.ssVdHotTag, filetag ))  &&
           (BS_BFS_EQL( svdp->ssVolInfo.ssVdHotBfSetId, bfSetId))) 
            RAISE_EXCEPTION (EOK);

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
                             alloc_hint 
                             );
#ifdef SS_DEBUG
        printf("ss_vd_migrate: vd(%d) tag(%d.%d) returns sts=%d\n",
        svdp->vdIndex, bfSetId.dirTag.num, filetag.num, sts);
#endif
        /* ENO_SUCH_TAG indicates file was deleted
         * E_NO_SUCH_BF_SET indicates fileset is being deleted by another thread
         * ENO_MORE_BLKS indicates not enough space to finish migrate command
         * E_WOULD_BLOCK indicates vfast was stopped in middle of a migrate
         * E_RSVD_FILE_INWAY - vfast unable to pack cause rsvd files inway.
         * ENOSPC - vfast unable to find or create enough space
         * E_RANGE_NOT_CLEARED real error, range was not cleared by mig_pack_vd_range
         * E_PAGE_NOT_MAPPED - file was truncated, but still defragmented
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
        mutex_lock( &svdp->ssVolInfo.ssVdMsgLk );
        if(svdp->ssVolInfo.ssVdMsgState != SS_STOP)
            svdp->ssVolInfo.ssVdMsgState = SS_MSG_IDLE;
        mutex_unlock( &svdp->ssVolInfo.ssVdMsgLk );

        if((sts != EOK) &&  /*  !already stopped */
           (svdp->ssVolInfo.ssVdMsgState != SS_STOP) &&  /* !rmvol or vfast deactivate */
           (svdp->ssVolInfo.ssVdMigState != SS_ABORT) && /*        or dmn deactivate   */
           !(svdp->blockingQ.ioQLen ||                    /* !normal IO */
             svdp->flushQ.ioQLen || svdp->ubcReqQ.ioQLen ||
             svdp->consolQ.ioQLen || svdp->devQ.ioQLen)) {
            /* no ios on normal queues, run vfast. */
            PREEMPT_CHECK(current_thread());
            ss_startios(svdp);
            svdp->ssVolInfo.ssIOCnt=0;
        }

        /* decrement the volume vfast thread count */
        mutex_lock( &svdp->ssVolInfo.ssVdMigLk );
        svdp->ssVolInfo.ssVdSSThdCnt--;
        if (svdp->ssVolInfo.ssVdSSThdCnt == 0 &&
            svdp->ssVolInfo.ssVdSSWaiters) {
            svdp->ssVolInfo.ssVdSSWaiters = 0;
            thread_wakeup((vm_offset_t)&svdp->ssVolInfo.ssVdSSWaiters);
        }
        mutex_unlock( &svdp->ssVolInfo.ssVdMigLk );

        vd_dec_refcnt(svdp);
        vdRefed = FALSE;
    }

    if(domain_open) {
        bs_domain_close(dmnP);
        domain_open = FALSE;
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
    uint64T requestedBlkCnt, /* in - number of pages requested */
    uint32T *blkOffset,   /* out - ptr to new, clear sbm locked range */
    uint32T *blkCnt,   /* out - count of pages locked */
    int flags)
{
    int sbm_locked = FALSE;
    void *stgDesc=NULL;
    statusT sts=EOK;
    uint32T locblkOffset;
    uint32T locblkCnt;

    STGMAP_LOCK_WRITE(&vdp->stgMap_lk)
    sbm_locked = TRUE;

    stgDesc = sbm_find_space( vdp,
                              requestedBlkCnt,
                              0,
                              BS_ALLOC_MIG_SINGLEXTNT, /* try for all, get most */
                              &locblkOffset,
                              &locblkCnt);

    if(stgDesc == NULL)  {
        /* volume has NO free space */
        RAISE_EXCEPTION (ENOSPC);
    }

    if(flags==TRUE) {
        /* lock the sbm location we may migrate src extents into */
        SS_TRACE(vdp,0,0,locblkOffset,locblkCnt);
        sts = sbm_lock_range (vdp,
                              locblkOffset,
                              locblkCnt );
        if(sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    STGMAP_UNLOCK(&vdp->stgMap_lk)
    sbm_locked = FALSE;

    *blkOffset = locblkOffset;
    *blkCnt = locblkCnt;
    return sts;

HANDLE_EXCEPTION:

    if(sbm_locked) {
        STGMAP_UNLOCK(&vdp->stgMap_lk)
        sbm_locked = FALSE;
    }
    return sts;
}


/*********************************************************************
 *
 * This routine gets the free space required to move a file.
 * 1. Try to find a single extent to hold requested page count. 
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
    uint64T bfPageSize,   /* in - size of one page */
    uint64T reqPageCnt,    /* in - number of pages requested */
    uint32T *allocBlkOffp,/* out - ptr to new, clear sbm locked range */
    uint64T *allocPageCnt, /* out - count of pages locked */
    int alloc_hint)  /* in - hint for type of allocation */

{
    uint64T newBlkCnt,scanBlkCnt = 0;
    uint64T requestedBlkCnt;
    statusT sts = EOK;
    uint64T startClust = 0, numClust = 0, clustCnt;
    int sbm_range_locked = FALSE;
    uint32T scanBlkOffset = 0;
    uint32T blkCnt;
    uint32T blkOffset=0;
    uint32T newBlkOffset=0;
    uint64T totFreeBlks = 0;

    /* (WITHOUT new Storage Reservation)
     * Try to find space for the requested number of pages.
     * sbm_find_space() returns NULL if there is not enough
     * space on the virtual disk.  Otherwise, it returns,
     * in "blkCnt", the largest contiguous block that can be
     * allocated.  This value may or may not be large enough
     * to satisfy the request.
     */

    requestedBlkCnt = reqPageCnt * bfPageSize;
    sts = ss_find_space(vdp,
                        requestedBlkCnt,
                        &blkOffset,
                        &blkCnt,
                        TRUE);
    if(sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    totFreeBlks = vdp->freeClust*vdp->stgCluster;
    if( (blkCnt) &&
        (blkCnt < requestedBlkCnt) &&
        ((alloc_hint == BS_ALLOC_MIG_SINGLEXTNT) ||
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
                RAISE_EXCEPTION (sts);
            } else {
                RAISE_EXCEPTION (EOK);
            }
        }

        if(((alloc_hint == BS_ALLOC_MIG_SINGLEXTNT) ||
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
                /* E_RSVD_FILE_INWAY: There may have been enough space but
                 *          it isn't uninterupted by a non-movable reserved file.
                 * ENO_MORE_MCELLS - File being packed is on DDL, abort
                 *                   since we cant move it.
                 * ENO_MORE_BLKS indicates the reserved
                 *               space was stolen by a needy
                 *               user application file.
                 * E_PAGE_NOT_MAPPED indicates that the file
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
                    RAISE_EXCEPTION (sts);
                } else {
                    RAISE_EXCEPTION (EOK);
                }
            }

            /* Relock the new sbm location at the new clear blk
             * offset.
             */
            newBlkCnt = scanBlkCnt - (newBlkOffset - scanBlkOffset);

            if(newBlkCnt == 0) {
                RAISE_EXCEPTION (ENOSPC);
            }

            STGMAP_LOCK_WRITE(&vdp->stgMap_lk)
            sts = sbm_lock_range (vdp,
                                  newBlkOffset,
                                  newBlkCnt );
            if(sts != EOK) {
                STGMAP_UNLOCK(&vdp->stgMap_lk)
                RAISE_EXCEPTION (sts);
            }
            STGMAP_UNLOCK(&vdp->stgMap_lk)
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
                RAISE_EXCEPTION (sts);
            } else {
                RAISE_EXCEPTION (EOK);
            }
        }
    }

HANDLE_EXCEPTION:

    if(sts != EOK) {
        *allocBlkOffp = 0;
        *allocPageCnt = 0;
    } else {
        *allocBlkOffp = blkOffset;
        *allocPageCnt = blkCnt / ADVFS_PGSZ_IN_BLKS;
    }

    if((sts != EOK) && (sbm_range_locked)) {
        /* if there was an unrecoverable error, clear the lock */
        sbm_lock_unlock_range (vdp, 0, 0);
        SS_TRACE(vdp,0,0,0,sts);
        sbm_range_locked = FALSE;
    }

    SS_TRACE(vdp,0,*allocBlkOffp,*allocPageCnt,sts);
    return sts;

}

#define PROCESS_EXT {                                                           \
    if(whoami==SS_PARENT) {                                                     \
        /* call child to scan ahead and determine how many extents              \
         * before reaching the number of pages we have room for.                \
         */                                                                     \
        *startPg = nextXtntDesc.pageOffset;                                     \
        *extentRunCnt = 0;                                                      \
        *runPageCnt = 0;                                                        \
        /* See how many extents ahead of here */                                \
        ss_get_most_xtnts (bfap,                                                \
                           xtntmap_locked,                                      \
                           reqPageCnt,                                          \
                           startPg,                                             \
                           runPageCnt,                                          \
                           xmRunPageCnt,                                        \
                           extentRunCnt,                                        \
                           SS_CHILD,                                            \
                           nextXtntDesc.pageOffset,                             \
                           prevPageCnt,                                         \
                           prevBlk);                                            \
                                                                                \
         /* We will quit calling child when parent reaches end                  \
          * of extent map.                                                      \
          */                                                                    \
        if(((reqPageCnt) && (*extentRunCnt) &&                                  \
            ((!bestExtentCnt) || (*extentRunCnt > bestExtentCnt))) ||           \
           ((!reqPageCnt) && (*extentRunCnt>1) &&                               \
            ((!bestExtentCnt) || (*runPageCnt < savedPageCnt)))) {              \
            /* This is the most extents weve found within the                   \
             * requested page cnt so far, save it and keep looking              \
             */                                                                 \
            bestExtentCnt = *extentRunCnt;                                      \
            savedStartPage = *startPg;                                          \
            savedPageCnt = *runPageCnt;                                         \
            savedXmPageCnt = *xmRunPageCnt - *startPg;                          \
        }                                                                       \
    } else {                                                                    \
        /* SS_CHILD */                                                          \
                                                                                \
        if((!reqPageCnt) && (consecutiveExtentCnt>0)) {                         \
            consecutivePageCnt += nextXtntDesc.pageCnt;                         \
            consecutiveXmPageCnt = nextXtntDesc.pageOffset+nextXtntDesc.pageCnt;\
            consecutiveExtentCnt++;                                             \
            goto _RUN_DONE;                                                     \
        } else                                                                  \
        if((reqPageCnt) &&                                                      \
           ((nextXtntDesc.pageCnt + consecutivePageCnt) >reqPageCnt)) {         \
            /* childs count of pages is now greater than we have                \
             * space for abort child's run now!                                 \
             */                                                                 \
            consecutiveXmPageCnt = nextXtntDesc.pageOffset;                     \
            goto _RUN_DONE;                                                     \
        } else                                                                  \
        if((reqPageCnt) &&                                                      \
           ((nextXtntDesc.pageCnt + consecutivePageCnt) == reqPageCnt)) {       \
            /* count of pages is the same as we have space for */               \
            /* want the current extent, quit now since can't hold no more */    \
            consecutivePageCnt += nextXtntDesc.pageCnt;                         \
            consecutiveXmPageCnt = nextXtntDesc.pageOffset+nextXtntDesc.pageCnt;\
            consecutiveExtentCnt++;                                             \
            goto _RUN_DONE;                                                     \
        } else {                                                                \
            /* record it and keep going */                                      \
            consecutivePageCnt += nextXtntDesc.pageCnt;                         \
            consecutiveXmPageCnt = nextXtntDesc.pageOffset +                    \
                                   nextXtntDesc.pageCnt;                        \
            if((*startPg == nextXtntDesc.pageOffset) ||                         \
               (nextXtntDesc.blkOffset !=                                       \
               (prevBlk+(prevPageCnt * ADVFS_PGSZ_IN_BLKS)))) {                 \
                /* if this is not the first extent processed or */              \
                /* if extents around a hole are adjacent don't bump xtnt count*/\
                consecutiveExtentCnt++;                                         \
            }                                                                   \
        }                                                                       \
    }                                                                           \
}


/*********************************************************************
 *
 * ss_get_most_xtnts()
 * Routine to retrieve a page range that covers not more than reqPageCnt of pages.
 * This page range contains the most extents in the file for reqPageCnt pages.  
 * Holes are checked to verify the pages on either side of the hole are contiguous.
 * If they are contiguous, the two extents are considered to be one extent since
 * we could not improve upon them.  If they are not contiguous, the two extents 
 * are counted at 2, since we could move them to adjacent position on disk.
 * 
 * Always starts at beginning of file.  SS_PARENT walks each extent, calling 
 * ss_get_most_xtnts as a SS_CHILD.  The child walks ahead of the parent counting
 * extents until reqPageCnt is satisfied.  Child then returns extent count. 
 * Parent examines return extent count and sees if this forward look returns 
 * more extents than the last call.  If so, save off new startPg and extentRunCnt.
 *
 * Parent stops at end of file, returns the offset, pageCnt, and extentRunCnt of
 * most extents that fit into reqPageCnt
 *
 * NOTE: if a 0 is passed in for reqPageCnt, this routine will find
 * the smallest number of pages contained in two consecutive extents.
 ********************************************************************/

static statusT
ss_get_most_xtnts (bfAccessT *bfap,       /* in - file */
                  int       *xtntmap_locked,  /* in/out - xtnts locked? */
                  uint64T   reqPageCnt,   /* in - requested page range size */
                  uint64T   *startPg,     /* out - new start page */
                  uint64T   *runPageCnt,  /* out - new count(alloc only) */
                                          /*       starting from startPg */
                  uint64T   *xmRunPageCnt,/* out - new count(including holes) */
                                          /*       starting from startPg */
                  uint64T   *extentRunCnt,/* out - new page cnt */
                  int       whoami,       /* in - parent=0 or child=1 */
                  uint64T   pageOffset,   /* in - page to start from */
                  uint64T   prevPageCnt,
                  uint64T   prevBlk)
{
    bsInMemXtntT *xtnts=NULL;
    bsInMemXtntMapT *xtntMap=NULL;
    bsInMemSubXtntMapT *subXtntMap;
    uint64T savedStartPage = 0,
            savedPageCnt = 0, 
            savedXmPageCnt = 0, 
            bestExtentCnt = 0,
            consecutivePageCnt = 0,
            consecutiveXmPageCnt = 0,
            consecutiveExtentCnt = 0;
    statusT sts=EOK;
    uint32T pageCnt=0;
    bsXtntDescT startXtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    bsXtntDescT nextXtntDesc;

    MS_SMP_ASSERT(bfap);

    xtnts = &bfap->xtnts;
    xtntMap = xtnts->xtntMap;

    if (xtnts->type != BSXMT_APPEND) {
        goto _RUN_DONE;
    }

    imm_get_xtnt_desc (pageOffset, xtntMap, 0, &xtntDescId, &nextXtntDesc);

    while (nextXtntDesc.pageCnt > 0) {
        if((nextXtntDesc.blkOffset != XTNT_TERM)  &&
           (nextXtntDesc.blkOffset != PERM_HOLE_START)) {
           /* skip holes since we can't eliminate them */
            PROCESS_EXT;
            prevPageCnt = nextXtntDesc.pageCnt;
            prevBlk = nextXtntDesc.blkOffset;
        } 
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) );
        *xtntmap_locked = FALSE;
        PREEMPT_CHECK(current_thread());
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if(sts != EOK) {
            return (sts);
        }
        *xtntmap_locked = TRUE;
        xtnts = &bfap->xtnts;
        xtntMap = xtnts->xtntMap;
        imm_get_next_xtnt_desc (xtntMap, &xtntDescId, &nextXtntDesc);
    }  /* end while */


_RUN_DONE:
    if(whoami == SS_PARENT) {
        if(bestExtentCnt) {
            /* found some extents, return vals */
            *startPg = savedStartPage;
            *runPageCnt = savedPageCnt;
            *xmRunPageCnt = savedXmPageCnt;
            *extentRunCnt = bestExtentCnt;
            return(EOK);
        } else {
            /* no extents small enough for request size, return ENOSPC */
            *startPg = 0;
            *runPageCnt = 0;
            *extentRunCnt = 0;
            return(ENOSPC);
        }
    } else {  /* SS_CHILD */
          if(((reqPageCnt==0) && (consecutiveExtentCnt > 0)) ||
             ((reqPageCnt!=0) && 
              (consecutiveExtentCnt > 0) &&
              (consecutivePageCnt <= reqPageCnt))) {
            /* found a run that will fit */
            *runPageCnt = consecutivePageCnt;
            *xmRunPageCnt = consecutiveXmPageCnt;
            *extentRunCnt = consecutiveExtentCnt;
            return(EOK);
        } else {
            /* no more than one extent was found from startPg to end */
            *runPageCnt = 0;
            *xmRunPageCnt = 0;
            *extentRunCnt = 0;
            return(EOK);
        }
    }
    /* not reached */
    return EOK;
}

static
statusT
ss_get_vd_most_free(bfAccessT *bfap,
                    vdIndexT *newVdIndex)
{
    vdT    *vdp=NULL;
    int vdCnt,vdi;
    statusT sts=EOK;
    uint64T savedtotFreeBlks = 0,
            requestedBlkCnt;
    uint32T blkCnt = 0, 
            blkOffset;

    *newVdIndex = 0;

    for (vdCnt = 0,vdi = 1; 
         vdCnt < bfap->dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if (( !(vdp = vd_htop_if_valid(vdi, bfap->dmnP, TRUE, FALSE)))) { 
            vdCnt++;
            continue;
        }
        sts = sc_valid_vd (
                           bfap->dmnP,
                           bfap->reqServices,
                           bfap->optServices,
                           vdp->vdIndex
                           );
        if (sts != EOK) {
            sts = EOK;
            vdCnt++;
            vd_dec_refcnt( vdp );
            continue;
        }

        /* Now check this vd for enough contig free space, if not enough
         * try to find a better volume with larger free extents.
         */
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if (sts != EOK) {
            vd_dec_refcnt( vdp );
            RAISE_EXCEPTION (sts);
        }

        requestedBlkCnt = bfap->xtnts.allocPageCnt * bfap->bfPageSz;

        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
        if (requestedBlkCnt < 1) {
            vd_dec_refcnt( vdp );
            RAISE_EXCEPTION (E_NOT_ENOUGH_XTNTS);
        }

        blkCnt=blkOffset=0;
        sts = ss_find_space(vdp,
                             requestedBlkCnt,
                             &blkOffset,
                             &blkCnt,
                             FALSE);   /* no lock range */
        if(sts==ENOSPC) sts = EOK;
        if(sts != EOK) {
            vd_dec_refcnt( vdp );
            RAISE_EXCEPTION (sts);
        }
        if((blkCnt) && 
           ((!savedtotFreeBlks) || (blkCnt > savedtotFreeBlks))) {
            *newVdIndex = vdi;
            savedtotFreeBlks = blkCnt;
        }
        vdCnt++;
        vd_dec_refcnt( vdp );
    }
    if(*newVdIndex == 0) {
            sts=ENO_MORE_BLKS;
    }


HANDLE_EXCEPTION:

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
               int alloc_hint
             )
{
    statusT sts=EOK, sts2;
    int closeFileFlag = FALSE;
    bfAccessT *bfap=NULL;
    bfSetT *bfSetp=NULL;
    vdT    *dvdp=NULL;
    uint64T srcPageOffset, 
            migPageCnt,  
            newBlkOffset, 
            xmEndPage,
            xmTotalPageCnt=0,
            allocTotalPageCnt=0,
            allocPagesLeft,
            pgsMigSoFar,
            extentCnt=0;
    uint64T consecutivePageCnt, page;
    uint32T allocBlkOffset=0;    /* sbm is free starting at this location.*/
    uint64T reqPageCnt=0,/* number pages requested to defragment complete file */
            allocPageCnt=0;  /* sbm is free for this many pages.*/
    int pageSize;  /* size in blks of a page */
    int nextpage;  /* next xm page in file */
    int wholeFile = TRUE;
    uint32T totXtnts = 0;
    int vdRefed = FALSE;
    vdIndexT  dstVdIndex=0, newDstVdIndex=0;
    int xtntmap_locked = FALSE;
    int fsetMounted;
    int targVol = -1;

    sts = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    closeFileFlag = TRUE;
    bfap->ssMigrating = SS_POSTED;

    /* shouldn't be needed because check is before its put on lists, 
     * but we'll check anyway. 
     * Ignore files under directio control if user desires.
     */
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) &&
         (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO)) {
        /* fset is a directio */
        RAISE_EXCEPTION (EOK);
    }
    if((bfap->dmnP->ssDmnInfo.ssDmnDirectIo == FALSE) && 
       (bfap->bfVp) && (bfap->bfVp->v_flag & VDIRECTIO)) {
        /* file is a directio */
        RAISE_EXCEPTION (EOK);
    }

    dstVdIndex = srcVdIndex;  /* default is primary mcell vol */
    if((alloc_hint != BS_ALLOC_MIG_SAMEVD) &&
       (alloc_hint != BS_ALLOC_MIG_SINGLEXTNT) &&
       (bfap->dmnP->ssDmnInfo.ssDmnBalance) &&
       (bfap->dmnP->vdCnt > 1)) {
        /* balance is enabled - go find best volume */
        if((targVol = ss_find_target_vd(bfap)) != -1) {
            dstVdIndex = targVol;
        }
    } 
    if((targVol == -1) &&
       (alloc_hint != BS_ALLOC_MIG_SINGLEXTNT) &&
       (alloc_hint != BS_ALLOC_MIG_SAMEVD)) {
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
            RAISE_EXCEPTION (sts);
        }
    }

    /* bump the ref count on this selected vd so it cant be removed */
    dvdp = vd_htop_if_valid(dstVdIndex, bfap->dmnP, TRUE, FALSE);
    if ( dvdp == NULL ) {
        /* must be an rmvol going on - just drop msg */
        RAISE_EXCEPTION (EBAD_VDI);
    }
    vdRefed = TRUE;

    /* Wait for any other threads to complete before continuing */
    mutex_lock( &dvdp->ssVolInfo.ssVdMigLk );
    dvdp->ssVolInfo.ssVdSSThdCnt++;    /* Potentially held for a long time */
    while ((dvdp->ssVolInfo.ssVdMigState == SS_PROCESSING) ||
           (dvdp->ssVolInfo.ssVdMigState == SS_PARKED)) {
        /* must be another thread got here first, simply wait for my turn */
        cond_wait( &dvdp->ssVolInfo.ssContMig_cv, &dvdp->ssVolInfo.ssVdMigLk );
    }

    if((dvdp->ssVolInfo.ssVdMigState == SS_ABORT) ||
       (SS_is_running == FALSE) ||
       (bfap->dmnP->ssDmnInfo.ssDmnState != SS_ACTIVATED) ) {
        mutex_unlock( &dvdp->ssVolInfo.ssVdMigLk );
        /* re-broadcast in case there are more threads waiting */
        cond_broadcast (&dvdp->ssVolInfo.ssContMig_cv);
        RAISE_EXCEPTION(E_WOULD_BLOCK);
    }

    /* Set state to SS_PROCESSING so that no more messages are 
     * sent to work thread while we are working on this destination volume.
     */
    dvdp->ssVolInfo.ssVdMigState = SS_PROCESSING;
    mutex_unlock( &dvdp->ssVolInfo.ssVdMigLk );

    /* determine number of pages of free space needed 
     * Make sure that the in-memory extent maps are valid.
     * Returns with xtntMap_lk read-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    reqPageCnt = bfap->xtnts.allocPageCnt;
    pageSize = bfap->bfPageSz;
    xmTotalPageCnt = bfap->nextPage;
    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    if (!reqPageCnt) {
        /* no pages to migrate in this file */
        RAISE_EXCEPTION (EOK);
    }

    /* Try to get free space for pages to be migrated.
     * lock it with sbm_lock_unlock_range
     */
    sts = ss_get_n_lk_free_space(dvdp, 
                           pageSize,
                           reqPageCnt,
                           &allocBlkOffset,
                           &allocPageCnt,
                           alloc_hint);
#ifdef SS_DEBUG
    printf("ss_get_n_lk_free_space:vd(%d) tag(%d.%d) returns reqPageCnt(%d) allocPageCnt(%d) sts=%d\n",
    dvdp->vdIndex, ((bfSetT *)bfap->bfSetp)->dirTag.num,
    bfap->tag.num, reqPageCnt, allocPageCnt, sts);
#endif
    if(sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if(allocPageCnt == 0) {
        /* no free space found or created! */
        RAISE_EXCEPTION (ENOSPC);
    }

    /* May have less than requested here because ss_get_n_lk_free_space returns 
     * as much as it can even if it was unable to find or create what was 
     * requested.  In this case we will combine the most non-contiguous
     * extents in the file into the free space we did get.
     */
    srcPageOffset = 0;
    if(allocPageCnt < reqPageCnt) {
        xmTotalPageCnt = extentCnt = allocTotalPageCnt = 0; 
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if(sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        xtntmap_locked = TRUE;

        sts = ss_get_most_xtnts(bfap,
                                &xtntmap_locked,
                                allocPageCnt,
                                &srcPageOffset,
                                &allocTotalPageCnt,
                                &xmTotalPageCnt,
                                &extentCnt,
                                SS_PARENT,0,  0,0);
#ifdef SS_DEBUG
        printf("ss_get_most_xtnts: tag(%d.%d) returns extentCnt=%d, sts=%d\n",
                ((bfSetT *)bfap->bfSetp)->dirTag.num,
                bfap->tag.num, extentCnt, sts);
#endif
        if(xtntmap_locked) {
            XTNMAP_UNLOCK( &(bfap->xtntMap_lk) );
            xtntmap_locked = FALSE;
        }

        if((extentCnt < 2) || (sts != EOK)) {
            if(extentCnt < 2) sts = ENOSPC;
            RAISE_EXCEPTION (sts);
        }
        wholeFile = FALSE;
        SS_TRACE(dvdp,bfap->tag.num,  srcPageOffset,
                     xmTotalPageCnt, extentCnt);
    }

    /* determine the number of pages to migrate */
    newBlkOffset = allocBlkOffset;
    xmEndPage = srcPageOffset + xmTotalPageCnt;
    pgsMigSoFar = 0;

    MS_SMP_ASSERT(srcPageOffset <= xmEndPage);
    while((srcPageOffset < xmEndPage) && (pgsMigSoFar < allocPageCnt)) {

        /* get the first page not in a hole */
        if(!page_is_mapped(bfap, srcPageOffset, &nextpage, FALSE) ) {
            srcPageOffset = nextpage;  /* next page */
        }
        /* move whats left or whats allowed, whichever is less */
        migPageCnt = MIN((xmEndPage - srcPageOffset),SSPageCnt);

        /* get the number of pages before the next hole */
        consecutivePageCnt = 1;
        for ( page = srcPageOffset; page < xmEndPage; ) {
            if ( page_is_mapped(bfap, page, &nextpage, FALSE) == TRUE ) {
                /* nextpage is the page beyond the current extent. */
                MS_SMP_ASSERT(nextpage > page);
                page = nextpage;
                continue;
            }

            /* Found a hole in the file. */
            break;
        }
        consecutivePageCnt = page - srcPageOffset;
        migPageCnt = MIN(consecutivePageCnt,migPageCnt);

        /* Make sure we figure for holes now filled in by
         * only migrating pages we have room for.
         */
        allocPagesLeft = allocPageCnt - pgsMigSoFar;
        migPageCnt = MIN(allocPagesLeft,migPageCnt);

        SS_TRACE(dvdp,bfap->tag.num,  srcPageOffset,
                      migPageCnt, newBlkOffset);
        sts = bs_migrate(
                bfap,         /* in */
                -1,           /* -1 indicates from any disk */
                srcPageOffset,/* srcPageOffset - in */
                migPageCnt,   /* srcPageCnt - in */
                dstVdIndex,   /* dstVdIndex - in */
                newBlkOffset, /* dstBlkOffset - in */
                1,            /* forceFlag, ignore pending truncations - in */
                BS_ALLOC_MIG_RSVD
              );
        if(sts != EOK) {
#ifdef SS_DEBUG
        printf("bs_migrate: vd(%d) tag(%d.%d) returns sts=%d\n",
        dstVdIndex, bfap->bfSetp->bfSetId.dirTag.num, bfap->tag.num, sts);
#endif
            /*
             E_PAGE_NOT_MAPPED:
                 if page(s) have been moved or truncd by another thread, an
                 sts == E_PAGE_NOT_MAPPED occurs, go ahead and abort.
                 Subsequent file access will put file back on list anyway.
             E_WOULD_BLOCK:
                 ok error, during vfast operation a stop
                 or all filesets are now dismounted in the domain.
                 Simply abort and return.
             ENO_MORE_BLKS:
                 ok error, Cause is space was stolen by real system write -
                 this is allowed and is not an error.
            */
            RAISE_EXCEPTION (sts);
        }

        srcPageOffset += migPageCnt;
        pgsMigSoFar   += migPageCnt;
        newBlkOffset += (migPageCnt * pageSize);

        /* update the page stats before parking or exiting */
        dvdp->dmnP->ssDmnInfo.ssPagesDefraged += migPageCnt;
        if((dstVdIndex != srcVdIndex) &&
           (targVol != -1))
            dvdp->dmnP->ssDmnInfo.ssPagesBalanced += migPageCnt;

        if(srcPageOffset >= xmEndPage) {
            /* we are done with the file */
            if(wholeFile) {
                mutex_lock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                dvdp->dmnP->ssDmnInfo.ssFilesDefraged++;
                mutex_unlock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );

                if(bfap->primVdIndex != dstVdIndex) {
                    /* since we are done, we need to ensure that the primary 
                     * mcell is moved to the destination volume.
                     */
                    sts = bs_move_metadata (bfap, dvdp);
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                }

                if(alloc_hint == BS_ALLOC_MIG_SINGLEXTNT) {
                    /* record hot file move stat here */
                    mutex_lock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                    dvdp->dmnP->ssDmnInfo.ssFilesIOBal++;
                    mutex_unlock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
                }
            }
            mutex_lock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );
            dvdp->dmnP->ssDmnInfo.ssExtentsConsol += extentCnt;
            mutex_unlock( &dvdp->dmnP->ssDmnInfo.ssDmnLk );

            break;
        }

        /* close the fileset and bfap before parking  so that the fileset can
         * be deleted if needed. We don't want to hold up the delete while
         * waiting for some idle time to run
         */
        sts = ss_close_file( bfSetp, bfap, fsetMounted );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        closeFileFlag = FALSE;

        /* wait for next run approval to continue */
        sts = ss_block_and_wait(dvdp);
        if(sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /* reopen the file */
        sts = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        closeFileFlag = TRUE;
        bfap->ssMigrating = SS_POSTED;

    } /* end while migrating */


HANDLE_EXCEPTION:

    if(xtntmap_locked)
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) );

    if((closeFileFlag == TRUE) &&
       (sts == EOK) &&
       (vdRefed)) {

        SS_TRACE(dvdp,0,0,0,sts);

        /* delete entry from list, if there */
        if(!BS_BFS_EQL(bfSetId, nilBfSetId)) {
            mutex_lock(&dvdp->ssVolInfo.ssFragLk);
            ss_delete_from_frag_list(dvdp, filetag, bfSetId);
            mutex_unlock(&dvdp->ssVolInfo.ssFragLk);
        }
        /* since we may have only combined extents or
         * this file could have been modified by user,
         * recheck for fragmentation.
         */
        /* go get the number of not contiguous extents in the file */
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if(sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        totXtnts = ss_xtnt_counter(bfap);
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) );

        if((totXtnts > 1) &&   /* cant do any better than 1, eh? */
           (!BS_BFS_EQL(bfSetId, nilBfSetId)) &&
           ((EXTENTS_FRAG_RATIO(allocPageCnt,totXtnts) <
            dvdp->ssVolInfo.ssFragHdr.ssFragHdrThresh) ||
           (dvdp->ssVolInfo.ssFragHdr.ssFragListCnt < SS_INIT_FRAG_LST_SZ))) {
            /* insert file at new priority on possibly new vd */
            ss_insert_frag_onto_list(bfap->primVdIndex,
                                     bfap->tag,
                                     bfSetId,
                             (ulong) EXTENTS_FRAG_RATIO(bfap->nextPage,totXtnts),
                                     totXtnts );
        }
    }

    SS_TRACE(dvdp,0,0,0,sts);

    /* Must reset flag away from SS_POSTED */
    if(closeFileFlag == TRUE) {
        if(sts==EOK)
            bfap->ssMigrating = SS_MSG_IDLE; /* allow to be placed again */
        else
            bfap->ssMigrating = SS_STOP; /* will be cleared next ss_chk_fragratio */
        (void) ss_close_file( bfSetp, bfap, fsetMounted );
        closeFileFlag = FALSE;
    } else {
        /* reopen the file - reset msg gate flag */
        sts2 = ss_open_file( bfSetId, filetag, &bfSetp, &bfap, &fsetMounted);
        if (sts2== EOK) {
            bfap->ssMigrating = SS_STOP;
            (void) ss_close_file( bfSetp, bfap, fsetMounted );
        }
    }

    if(vdRefed) {

        SS_TRACE(dvdp,0,0,0,sts);

        /* unlock free space */
        sbm_lock_unlock_range (dvdp, 0, 0);

        mutex_lock( &dvdp->ssVolInfo.ssVdMigLk );
        /* reset State to IDLE and wake up waiting threads */
        if(dvdp->ssVolInfo.ssVdMigState != SS_ABORT)
                dvdp->ssVolInfo.ssVdMigState = SS_MIG_IDLE;
        mutex_unlock( &dvdp->ssVolInfo.ssVdMigLk );

        cond_broadcast (&dvdp->ssVolInfo.ssContMig_cv);

        /* decrement the vd vfast thread count */
        mutex_lock( &dvdp->ssVolInfo.ssVdMigLk );
        dvdp->ssVolInfo.ssVdSSThdCnt--;
        if (dvdp->ssVolInfo.ssVdSSThdCnt == 0 &&
            dvdp->ssVolInfo.ssVdSSWaiters) {
            dvdp->ssVolInfo.ssVdSSWaiters = 0;
            thread_wakeup((vm_offset_t)&dvdp->ssVolInfo.ssVdSSWaiters);
        }
        mutex_unlock( &dvdp->ssVolInfo.ssVdMigLk );

        vd_dec_refcnt(dvdp);
        vdRefed = FALSE;
    }
    return(sts);
} /* end ss_vd_migrate */


/*********************************************************************
 *
 * This routine counts the number of disk blocks on the given volume.
 *
 ********************************************************************/

static
uint64T
ss_blks_on_vd(bfAccessT *bfap,       /* in */ 
              vdIndexT vdi           /* in */
             )
{
    uint64T tot_blks_on_vd=0;
    uint32T i;
    bsInMemXtntMapT *xtntMap=NULL;
    bsInMemSubXtntMapT *subXtntMap=NULL;
    statusT sts=EOK;

    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        return(0);
    }

    switch ( bfap->xtnts.type ) {
        case BSXMT_APPEND:
                xtntMap = bfap->xtnts.xtntMap;
                break;
        case BSXMT_STRIPE:  /* should never be stripe files on any lists */
        default:
                MS_SMP_ASSERT(0);
    }

    /* determine total number of blocks on this volume */
    for ( i = 0; i < xtntMap->validCnt; i++ ) {
        subXtntMap = &(xtntMap->subXtntMap[i]);
        MS_SMP_ASSERT(subXtntMap);
        if (subXtntMap->vdIndex == vdi ) {
            tot_blks_on_vd += subXtntMap->pageCnt * xtntMap->blksPerPage;
        }
    }  /* end for */

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
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
    uint64T fileBlockCount,
            dmnSizeBlks,
            dmnFreeSizeBlks,
            newVolFreeSizeBlks;
    uint64T blksOnTarg,                /* file blocks on target volume */
            blksOnVd;                  /* file blocks on non-target volume */
    uint64T dmnFreeSizePct, targFreePct, srcFreePct;
    long    volDeviation, 
            dmnBalanceDeviation,
            newBalanceDeviation,
            bestNewBalanceDeviation;
    uint64T bestTargetFileBlks;        /* best file blocks on target volume */
    int    i,
           vdi,
           vdCnt,
           validVd,
           targ,
           bestTargetVdi = -1,
           bestTargetIdx = 0,
           targetVdi= -1;
    typedef struct {
        uint64T volSizeBlks;
        uint64T volFreeBlks;
        int vdi;
    } volDataT;
    volDataT *volData = NULL;
    int vdRefed = FALSE;
    uint64T precision=100000000; /* enough for a terabyte of volsize */
    int found_bad_vol = FALSE;

    if ((TEST_DMNP(bfap->dmnP) != EOK) ||
        (bfap->dmnP->state != BFD_ACTIVATED) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (bfap->dmnP->dmnFlag == BFD_DEACTIVATE_PENDING)) {
        RAISE_EXCEPTION(EOK);
    }

    dmnSizeBlks = 0;
    dmnFreeSizeBlks = 0;
    fileBlockCount = bfap->xtnts.allocPageCnt * bfap->bfPageSz;

    /* malloc this structure each time routine is called, 
     * otherwise we would have to reference and dereference the vdp 
     * many times in the following code.
     */
    volData = (volDataT *)ms_malloc(bfap->dmnP->vdCnt * sizeof(volDataT));
    if(volData == NULL) {
        RAISE_EXCEPTION(ENO_MORE_MEMORY);
    }
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
                           bfap->optServices,
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
        volData[validVd].volFreeBlks = vdp->freeClust * ADVFS_PGSZ_IN_BLKS;
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
        RAISE_EXCEPTION(EOK);
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
                   RAISE_EXCEPTION(EOK);
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
ss_sim_file_on_vols( volDataT *volData,   /* in */
                     int validVd,         /* in */
                     long currDmnIOBalance,/* in */
                     ssHotLLT *try_hp,    /* in */
                     long *newDeviation,   /* out */
                     int *targVdi)         /* out */
{
    long    volDeviation,
            newBalanceDeviation,
            bestNewBalanceDeviation;
    uint64T newVolIO, currHotIOCnt;
    int    i, wday,
           targ,
           targetVolumeIndex= -1;


    bestNewBalanceDeviation = 0;
    for(targ=0; targ < validVd; targ++) {

        if((try_hp->ssHotFileSize / BS_BLKSIZE) >=
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
ss_find_hot_target_vd(domainT *dmnP,         /* in */
                      vdIndexT *targVdIndex,  /* out - selected vd */
                      bfTagT *tag,           /* out - selected file */
                      bfSetIdT *setId)       /* out - selected fset */
{
    statusT sts=EOK;
    struct timeval curr_time;
    ssHotHdrT *hhp;
    ssHotLLT *currp, *hp, *new_hp=NULL;  /* entry pointer */
    int vdRefed = FALSE;
    vdT *vdp=NULL,*vdp2=NULL;
    long    volDeviation, 
            newBalanceDeviation,
            bestNewBalanceDeviation,
            currDmnIOBalance ;
    uint64T dmnHotIO=0, newVolIO, currHotIOCnt;
    int    i, wday,
           targ,
           targetVolumeIndex= -1,
           bestVolumeIndex,
           vdi, vdCnt, validVd;
    volDataT *volData = NULL;
    uint16T topHotLstCnt=0;
    int hot_lock_taken = FALSE;

    if ((TEST_DMNP(dmnP) != EOK) ||
        (dmnP->state != BFD_ACTIVATED) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_IN_PROGRESS) ||
        (dmnP->dmnFlag == BFD_DEACTIVATE_PENDING)) {
        RAISE_EXCEPTION(EOK);
    }

    /* malloc this structure each time routine is called, 
     * otherwise we would have to reference and dereference the vdp 
     * many times in the following code to store it on the vdp.
     * malloc the most vds that can be valid even though we may
     * use a subset of them.
     */
    volData = (volDataT *)ms_malloc(dmnP->vdCnt * sizeof(volDataT));
    if(volData == NULL) {
        RAISE_EXCEPTION(ENO_MORE_MEMORY);
    }

    /* I. load the volume information */
    for (vdCnt = 0,vdi = 1, validVd=0; 
         vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        }
        sts = sc_valid_vd (
                           vdp->dmnP,
                           defServiceClass,
                           nilServiceClass,
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

        volData[validVd].volFreeBlks = vdp->freeClust * ADVFS_PGSZ_IN_BLKS;
        volData[validVd].vdi = vdp->vdIndex;

        validVd++;
        vdCnt++;
        vd_dec_refcnt( vdp );
    }

    /* see if there is any work - keep this below sc_valid_vd for lock hier */
    TIME_READ(curr_time);
    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );
    hot_lock_taken = TRUE;
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    if (( hhp->ssHotListCnt == 0)  || 
        ( validVd == 1 ) ||
        (((curr_time.tv_sec - dmnP->ssDmnInfo.ssFirstMountTime)/SECHOUR) <
          dmnP->ssDmnInfo.ssSteadyState)) {
        RAISE_EXCEPTION(EOK);
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
                if((hp->ssHotFileSize / BS_BLKSIZE) < volData[targ].volFreeBlks) {
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
        targetVolumeIndex = hp->ssHotVdi;
        RAISE_EXCEPTION(EOK);
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
        RAISE_EXCEPTION(EOK);
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
        mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );
        hot_lock_taken = FALSE;
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
