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
 *      AdvFS - bs_freeze.c
 *
 * Abstract:
 *
 *      This module contains routines for freezing and thawing domains.
 *
 *      Freeze and thaw are implemented as VFS operations.  The vfs_ops
 *      vector table in the mount stucture contians a pointer to the
 *      freeze/thaw function for the underlying filesystem.
 *
 *      The AdvFs vfs_op packages each request in an advfsFreezeMsgT stucture
 *      and adds it to a global input list.  It then blocks until the 
 *      background worker thread processes the message and sets the status 
 *      code.
 *
 *      The background thread processes the input list of messages and 
 *      maintains another internal list of domains already frozen. Each frozen
 *      domain has a timeout value associated with it and the background 
 *      thread will force thaw a domain if it's timeout value is exceeded.
 *
 *      A domain is frozen by starting an exclusive transaction and then
 *      flushing the log.  All metadata updates are done under transaction
 *      control, and will block while the exclusive transaction is in
 *      effect.  The freeze request can also specify that user buffers are
 *      to be flushed as well.
 *
 *      The timeout period does not start until the exclusive transaction is 
 *      started and all flushing is complete.     
 *
 */

#define ADVFS_MODULE BS_FREEZE

#include <ms_public.h>
#include <bs_public_kern.h>
#include <ftx_public.h>
#include <ms_privates.h>
#include <bs_domain.h>
#include <ms_osf.h>
#include <bs_freeze.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#include <sys/fs/advfs_ioctl.h>
#include <advfs/advfs_conf.h>
#include <advfs/advfs_evm.h>
#include <advfs/fs_dir_routines.h>

/******************************************************************************
 *
 *  Prototypes
 */
static
void
bs_freeze_thread( void );

static
void
check_for_new_msg( void );

static
void
check_for_timeout( void );

static
void
freeze_domain( advfsFreezeMsgT *msg );

static
void
thaw_domain( advfsFreezeInfoT *freezeInfoP, int forced );

/******************************************************************************
 *
 *  Globals
 */

/*
 *  AdvfsFreezeMsgs is used for communication between the AdvFS vfsop and
 *  the freezefs background thread.
 */ 

mutexT            AdvfsFreezeMsgsLock;
cv_t              AdvfsFreezeMsgsCV;          /* Cond Var for freeze msgs */
advfsFreezeMsgT  *AdvfsFreezeMsgs = NULL;     /* List of freeze messages */

/*
 *  advfsFrozenlist is used to hold information about currently frozen
 *  domains. There is no lock protecting AdvfsFrozenList as it is processed
 *  by a single thread.
 */
advfsFreezeInfoT *AdvfsFrozenList = NULL;  /* List of frozen domains */

/* tunable default seconds to freeze */
extern uint64_t ktval_advfs_freezefs_timeout;
int64_t AdvfsFreezefsTimeout = ADVFS_FREEZEFS_TIMEOUT_DEFAULT;

/******************************************************************************
 *
 *  bs_init_freeze_thread()
 *
 *  Create and initialize the AdvFs freeze worker thread.
 *
 *    Called by: bs_init() during AdvFS initialization.
 *
 */
void
bs_init_freeze_thread( void )
{
    extern proc_t *Advfsd;
    tid_t tid;
    
    ADVMTX_FREEZE_MSGS_INIT(&AdvfsFreezeMsgsLock);

    cv_init(&AdvfsFreezeMsgsCV, "AdvfsFreezeMsgs CV", NULL, CV_WAITOK);
    if (kdaemon_thread_create((void (*)(void *))bs_freeze_thread,
                              NULL,
                              Advfsd,
                              &tid,
                              KDAEMON_THREAD_CREATE_DETACHED)) {

        ADVFS_SAD0("The AdvFS freeze thread was not spawned.\n");
    }

    AdvfsFreezefsTimeout = ktval_advfs_freezefs_timeout;

}


/******************************************************************************
 *
 *  bs_freeze_thread()
 *
 *  Process messages to freeze and thaw domains.
 *
 *    Called by: spawned by bs_init_freeze_thread during AdvFS initialization.
 *
 *  This function does the following in an infinite loop:
 *  If there are no messages
 *    Set the timeout period if there is a currently frozen domain
 *        Frozen domains are kept on a list sorted by timeout value
 *    Wait for a new message or timeout expiration
 *  Check for timeouts
 *  Check for new messages
 */

static
void
bs_freeze_thread( void )
{
    advfsFreezeMsgT   *msg;
    advfsFreezeInfoT  *freezeInfoP;
    struct timeval     tv;
    timestruc_t        timo = {0,0};

    while (TRUE) {
        ADVMTX_FREEZE_MSGS_LOCK(&AdvfsFreezeMsgsLock);
        if (!AdvfsFreezeMsgs) {
            /*
             * We would normally use a while loop to recheck the condition
             * on which we are waiting, but in this case, the thread will
             * come around here harmlessly again anyway.
             */
            if (freezeInfoP = AdvfsFrozenList) {
		/*
		 * cv_timedwait wants number of seconds to wait
		 */
		tv = get_system_time();
		timo.tv_sec = freezeInfoP->frziTimeout - tv.tv_sec;
		if (timo.tv_sec < 0) {
		    timo.tv_sec = 0;
		}

                cv_timedwait(&AdvfsFreezeMsgsCV, &AdvfsFreezeMsgsLock,
                             CV_MUTEX, CV_NO_RELOCK, &timo);

            } else {
                cv_wait(&AdvfsFreezeMsgsCV, &AdvfsFreezeMsgsLock,
                                CV_MUTEX, CV_NO_RELOCK);
            }
        }
        check_for_timeout();
        check_for_new_msg();
    }
}


/******************************************************************************
 *  Check for timeouts
 *
 *  Check the list of frozen domains and force thaw any
 *  that have timed out.
 *
 */

static
void
check_for_timeout( void )
{
    advfsFreezeInfoT      *freezeInfoP;
    struct timeval         curTime;

    curTime = get_system_time();
    while (freezeInfoP = AdvfsFrozenList) {
        if ( freezeInfoP->frziTimeout <= curTime.tv_sec ) {
            AdvfsFrozenList = freezeInfoP->frziLink;
            thaw_domain(freezeInfoP, TRUE);
            curTime = get_system_time();
        } else {
            return;
        }
    }
    return;
}


/******************************************************************************
 *
 *  Check for and process a new message.
 *
 *  Take the first message off the input list and process it.
 * 
 */

static
void
check_for_new_msg( void )
{
    advfsFreezeMsgT   *msg;
    advfsFreezeInfoT  *freezeInfoP;
    advfsFreezeInfoT  *trailerP;


    ADVMTX_FREEZE_MSGS_LOCK(&AdvfsFreezeMsgsLock);
    if (AdvfsFreezeMsgs == NULL) {
        ADVMTX_FREEZE_MSGS_UNLOCK(&AdvfsFreezeMsgsLock);
        return;
    }
    msg = AdvfsFreezeMsgs;
    AdvfsFreezeMsgs = msg->frzmLink;
    ADVMTX_FREEZE_MSGS_UNLOCK(&AdvfsFreezeMsgsLock);

    if ( msg->frzmFlags & ADVFS_Q_FREEZE ) {

        freeze_domain( msg );

    } else {

        /*
         *  Find (and thaw) the domain in question
         */
        freezeInfoP = AdvfsFrozenList;
        if (freezeInfoP != NULL ) {
            if ( BS_UID_EQL(msg->frzmDomainId, freezeInfoP->frziDomainId) ) {
                AdvfsFrozenList = freezeInfoP->frziLink;
                thaw_domain( freezeInfoP, FALSE );
            } else {
                trailerP = AdvfsFrozenList;
                freezeInfoP = freezeInfoP->frziLink;
                while (freezeInfoP !=NULL) {
                    if ( BS_UID_EQL(msg->frzmDomainId, freezeInfoP->frziDomainId) ) {
                        trailerP->frziLink = freezeInfoP->frziLink;
                        thaw_domain( freezeInfoP, FALSE );
                        break;
                    }
                    trailerP = freezeInfoP;
                    freezeInfoP = freezeInfoP->frziLink;
                }
            }
        }
        
        if ( freezeInfoP == NULL ) {
            msg->frzmStatus = EALREADY;
        } else {
            msg->frzmStatus = EOK;
        }
        cv_signal( &msg->frzmStatusCV, NULL, CV_NULL_LOCK );
    }
}


/******************************************************************************
 *
 *  Freeze
 *
 *  Handle a freeze message
 *
 */

static
void
freeze_domain( advfsFreezeMsgT *msg )
{

    statusT            sts;
    struct timeval     tv;
    domainT           *dmnP            = NULL;
    struct vnode      *vp;
    struct fsContext  *cp;
    bfAccessT         *bfap;
    advfsFreezeInfoT  *freezeInfoP     = NULL;
    advfsFreezeInfoT  *trailerP        = NULL;
    advfsFreezeInfoT  *leaderP         = NULL;
    bsDmnFreezeAttrT   dmnFreezeAttr; 
    ftxHT              ftxH            = FtxNilFtxH;
    struct vd         *logvdp;         /* pointer to vd of log */
    int64_t            timeout;

    dmnP = msg->frzmFSNP->dmnP;
    vp = msg->frzmFSNP->root_vp;
    cp = VTOC(vp);
    bfap = VTOA(vp);

    /*
     *  Get memory to hold the freeze parameters
     */
    freezeInfoP = (advfsFreezeInfoT *) 
	ms_malloc( sizeof(advfsFreezeInfoT) );
    freezeInfoP->frziDomainId = msg->frzmDomainId;
    freezeInfoP->frziFlags    = msg->frzmFlags;
    freezeInfoP->frziFSNP     = msg->frzmFSNP;

    timeout = msg->frzmTimeout;
    tv = get_system_time(); /* now */
    /* timeout of zero indicates use tunable */
    if ( timeout == 0 ) {
	timeout = AdvfsFreezefsTimeout;
    }
    /* timeout of greater than zero indicates timeout for time specified */
    if ( timeout > 0 ) {
        freezeInfoP->frziTimeout = tv.tv_sec + timeout;
    }
    /* timeout of less than zero indicates do not timeout ... give it a year */
    else {
        freezeInfoP->frziTimeout = tv.tv_sec + (long)(60*60*24*365);
    }

    /*
     *  BFD_FREEZE_IN_PROGRESS has already been set by the vfsop 
     *  Get the lock and proceed if the freeze reference count is 0
     */
    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    if (dmnP->dmnFreezeRefCnt) {
        dmnP->dmnFreezeWaiting++;
        while (TRUE) {
            cv_wait(&dmnP->dmnFreezeWaitingCV, &dmnP->dmnFreezeMutex,
                            CV_MUTEX, CV_NO_RELOCK);
            if (!dmnP->dmnFreezeWaiting) {
                    break;
            }
            ADVMTX_DOMAINFREEZE_LOCK( &dmnP->dmnFreezeMutex);
        }
        dmnP->dmnFreezeWaiting--;
        MS_SMP_ASSERT(!dmnP->dmnFreezeRefCnt);
    } else {
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
    }

    /*
     *  Check to see if we're racing an umount of the
     *  last fileset in the domain.  If so, return 
     *  domain not found.
     */
    ADVRWL_FILESETLIST_READ(&FilesetLock );
    if (dmnP->mountCnt == 0) {
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );
        msg->frzmStatus = ENOENT;
        ms_free (freezeInfoP);
        cv_signal( &msg->frzmStatusCV, NULL, CV_NULL_LOCK );
        return;
    }
    ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

    /*
     * Prior to the upcoming exclusive transaction and call
     * to advfs_lgr_flush(), we write the BSR_DMN_FREEZE_ATTR record into the
     * (r)bmt with the current time in the dmnFreezeAttr.freezeBegin field.
     * This record is for debug purposes only and can not affect
     * the freezefs operation.  Therefore we ignore error handling
     * for this set of routines. 
     */
    logvdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    
    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP );

    if (sts == EOK) {

        bzero((char *)&dmnFreezeAttr, sizeof( bsDmnFreezeAttrT ));

        sts = bmtr_get_rec( logvdp->rbmtp,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ) );

        tv = get_system_time();
        dmnFreezeAttr.freezeBegin = tv.tv_sec;
        dmnFreezeAttr.freezeCount++;

        sts = bmtr_put_rec( logvdp->rbmtp,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ),
                            ftxH );

        ftx_done_n( ftxH, FTA_NULL );
    }

    /*
     * if the stats were changed write them out to minimize
     * possibility that update will happen on close
     */
    if (cp->dirty_stats == TRUE) {

	sts = FTX_START_N( FTA_NULL, &ftxH, FtxNilFtxH, dmnP );
	
	if (sts == EOK) {
            /*
             * log an update to the in-memory file stats.
             */
  
            (void)fs_update_stats(vp, bfap, ftxH, 0);

            ftx_done_n(ftxH, FTA_NULL);
        }
    }

    /*
     *  Starting an exclusive transaction will allow
     *  all current transactions to complete and cause
     *  any new transactions to block. 
     */
    sts = FTX_START_EXC(FTA_NULL, &freezeInfoP->frziFtxH, dmnP);
    if (sts != EOK) {
        ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeFlags &= ~BFD_FREEZE_IN_PROGRESS;
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
        ms_free (freezeInfoP);
        msg->frzmStatus = EIO;
        cv_signal( &msg->frzmStatusCV, NULL, CV_NULL_LOCK );
        return;
    }

    /*
     * Drop the ftx slot lock for the domain if a debug kernel.
     * This is necessary since the freeeze thread is basically the
     * only thread that ever handles transaction on multiple domains
     * at once, in order to avoid lock hierarchy inversions.
     */
#ifdef ADVFS_SMP_ASSERT
     ADVRWL_DOMAIN_FTXSLOT_UNLOCK(&dmnP->ftxSlotLock);
#endif

    /*
     *  Flush the log
     */
    advfs_lgr_flush(dmnP->ftxLogP, nilLSN, FALSE);

    /*
     *  Update the domain flag
     */
    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeFlags &= ~BFD_FREEZE_IN_PROGRESS;
    dmnP->dmnFreezeFlags |=  BFD_FROZEN;
    ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);

    /*
     *  Add the advfsFreezeInfoT to the working list of frozen domains
     *  sorted by timeout value
     */
    trailerP = AdvfsFrozenList;
    if (trailerP == NULL || freezeInfoP->frziTimeout <= trailerP->frziTimeout) {
        freezeInfoP->frziLink = AdvfsFrozenList;
        AdvfsFrozenList = freezeInfoP;
    } else {
        leaderP = trailerP->frziLink;
        while (leaderP != NULL) {
            if ( freezeInfoP->frziTimeout <= leaderP->frziTimeout ) {
                trailerP->frziLink = freezeInfoP;
                freezeInfoP->frziLink = leaderP;                    
                break;
            }
            trailerP = leaderP;
            leaderP = leaderP->frziLink;
        }
        if (leaderP == NULL) {
            trailerP->frziLink = freezeInfoP;
            freezeInfoP->frziLink = NULL;
        }
    }

    /*
     *  Set status and wakeup the originating thread
     */
    msg->frzmStatus = EOK;
    cv_signal( &msg->frzmStatusCV, NULL, CV_NULL_LOCK );

    return;
}


/******************************************************************************
 *
 *  Thaw
 *
 *  Complete the exclusive transaction to thaw the domain
 *
 */
 
static
void
thaw_domain( advfsFreezeInfoT *freezeInfoP, int forced )
{
    statusT            sts;
    int                error;
    struct timeval     tv;
    domainT           *dmnP          = NULL;
    bsDmnFreezeAttrT   dmnFreezeAttr;
    ftxHT              ftxH          = FtxNilFtxH;
    struct vd         *logvdp;       /* pointer to log vd */
    int blockdevice = 0;
    char *tempstring = NULL;
    char *setName=NULL;
    char *dmnName=NULL;
    advfs_ev *advfs_event;

    /*
     *  If forced thawed due to timout 
     *      Post the event
     *
     *  We do it here, before ending the transaction, to prevent problems
     *  with a racing unmount. The ftx_done_n will allow the unmount to
     *  proceed and the m_stat.f* info could disappear.
     */

    if (forced) { 
	tempstring = ms_malloc( (size_t) MAXPATHLEN );
	error = advfs_parsespec(freezeInfoP->frziFSNP->f_mntfromname,
				tempstring, 
				MAXPATHLEN, 
				&dmnName, 
				&setName, 
				&blockdevice);

	if ( error ) {
            ms_free( tempstring );
	    return;
	}

	advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
	advfs_event->domain = dmnName;
	advfs_event->fileset = setName;

	sprintf (advfs_event->formatMsg,
		 (int)sizeof(advfs_event->formatMsg),
		 "Filesystem %s mounted on %s has been thawed due to timeout", 
		 dmnName, freezeInfoP->frziFSNP->f_mntonname);
	(void)advfs_post_kernel_event(EVENT_THAW, advfs_event);

        ms_free( tempstring );
	ms_free( advfs_event );
    }

    dmnP = freezeInfoP->frziFtxH.dmnP;

    /*
     * Re-aquire the ftx slot lock for the domain if a debug kernel.
     * This is necessary since the freeeze thread is basically the
     * only thread that ever handles transaction on multiple domains
     * at once, in order to avoid lock hierarchy inversions.
     */
#ifdef ADVFS_SMP_ASSERT
    ADVRWL_DOMAIN_FTXSLOT_READ(&dmnP->ftxSlotLock);
#endif

    /*
     * Call set_vd_time() to update the UmountThaw records with the thaw
     * time.  Ignore returned status.  Note that set_vd_time() MUST be
     * called within an exclusive transaction to block other transactions
     * from accessing/updating RBMT page0 for all disks.
     */
    sts = set_vd_time(dmnP);
    
    ftx_special_done_mode (freezeInfoP->frziFtxH, FTXDONE_LOGSYNC);
    ftx_done_n (freezeInfoP->frziFtxH, FTA_BS_FREEZE);
    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeFlags &= ~BFD_FROZEN;
    ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
    ms_free (freezeInfoP);

    /*
     * After thaw_domain has completed the exclusive transaction,
     * we write the BSR_DMN_FREEZE_ATTR record into the (r)bmt
     * with the current time in the dmnFreezeAttr.freezeEnd field.
     * This record is for debug purposes only and can not affect
     * the freezefs operation.  Therefore we ignore error handling
     * for this set of routines. 
     */ 
    logvdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    
    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP);
    
    if (sts == EOK) {

        bzero((char *)&dmnFreezeAttr, sizeof( bsDmnFreezeAttrT ));
    
        sts = bmtr_get_rec( logvdp->rbmtp,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ) );

        tv = get_system_time();
        dmnFreezeAttr.freezeEnd = tv.tv_sec;

        sts = bmtr_put_rec( logvdp->rbmtp, 
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ),
                            ftxH );

        ftx_done_n( ftxH, FTA_NULL );
    }

    return;
}
