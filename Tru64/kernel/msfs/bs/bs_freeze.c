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
 *      and adds it to a global input list.  It then blocks until the background
 *      worker thread processes the message and sets the status code.
 *
 *      The background thread processes the input list of messages and maintains
 *      another internal list of domains already frozen.  Each frozen domain as
 *      a timeout value associated with it and the background thread will force
 *      thaw a domain if it's timeout value is exceeded.
 *
 *      A domain is frozen by starting an exclusive transaction and then flushing
 *      the log.  All metadata updates are done under transaction control, and
 *      will block while the exclusive transaction is in effect.  The freeze
 *      request can also specify that user buffers are to be flushed as well.
 *
 *      The timeout period does not start until the exclusive transaction is 
 *      started and all flushing is complete.     
 *
 * Date:
 *      5/22/2001
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_freeze.c,v $ $Revision: 1.1.5.4 $ (DEC) $Date: 2002/01/29 15:11:06 $"

#define ADVFS_MODULE BS_FREEZE

#include <msfs/ms_public.h>
#include <msfs/bs_public.h>
#include <msfs/ftx_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_domain.h>
#include <msfs/ms_osf.h>
#include <msfs/bs_freeze.h>
#include <vfs/vfs_evm.h>

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
    decl_simple_lock_info(, ADVFreezeMsgs_lockinfo)

    mutexT            AdvfsFreezeMsgsLock;
    advfsFreezeMsgT  *AdvfsFreezeMsgs = NULL;     /* List of freeze messages */

    /*
     *  advfsFrozenlist is used to hold information about currently frozen
     *  domains. There is no lock protecting AdvfsFrozenList as it is processed
     *  by a single thread.
     */
    advfsFreezeInfoT *AdvfsFrozenList = NULL;     /* List of frozen domains */


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
    extern task_t first_task;

    mutex_init3(&AdvfsFreezeMsgsLock, 0, "AdvfsFreezeMsgsLock", ADVFreezeMsgs_lockinfo);
    if (!kernel_thread( first_task, bs_freeze_thread )) {
        ADVFS_SAD0("The AdvFS freeze thread was not spawned.\n");
    }
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
    struct timeval     curTime;

    while (TRUE) {
        mutex_lock(&AdvfsFreezeMsgsLock);
        if (!AdvfsFreezeMsgs) {
            assert_wait_mesg( (vm_offset_t) &AdvfsFreezeMsgs, FALSE, "advfs_freeze_thread" );
            if (freezeInfoP = AdvfsFrozenList) {
                TIME_READ (curTime);
                thread_set_timeout(hz * (freezeInfoP->frziTimeout - curTime.tv_sec) );
            }
            mutex_unlock(&AdvfsFreezeMsgsLock);
            thread_block();
        } else {
            mutex_unlock(&AdvfsFreezeMsgsLock);
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

    TIME_READ (curTime);
    while (freezeInfoP = AdvfsFrozenList) {
        if ( freezeInfoP->frziTimeout <= curTime.tv_sec ) {
            AdvfsFrozenList = freezeInfoP->frziLink;
            thaw_domain(freezeInfoP, TRUE);
            TIME_READ (curTime);
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

    mutex_lock(&AdvfsFreezeMsgsLock);
    if (AdvfsFreezeMsgs == NULL) {
        mutex_unlock(&AdvfsFreezeMsgsLock);
        return;
    }
    msg = AdvfsFreezeMsgs;
    AdvfsFreezeMsgs = msg->frzmLink;
    mutex_unlock(&AdvfsFreezeMsgsLock);

    if ( msg->frzmFlags & FS_Q_FREEZE ) {

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
        thread_wakeup( (vm_offset_t) &msg->frzmStatus );
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
    advfsFreezeInfoT  *freezeInfoP     = NULL;
    advfsFreezeInfoT  *trailerP        = NULL;
    advfsFreezeInfoT  *leaderP         = NULL;
    bsDmnFreezeAttrT   dmnFreezeAttr; 
    ftxHT              ftxH            = FtxNilFtxH;
    struct vd         *logvdp;         /* pointer to vd of log */
    bfAccessT         *mdap;           /* bfap for RBMT/BMT */

    dmnP = GETDOMAINP( msg->frzmMP );

    /*
     *  Get memory to hold the freeze parameters
     */
    freezeInfoP = (advfsFreezeInfoT *) ms_malloc( sizeof(advfsFreezeInfoT) );
    freezeInfoP->frziDomainId = msg->frzmDomainId;
    freezeInfoP->frziFlags    = msg->frzmFlags;
    freezeInfoP->frziTimeout  = msg->frzmTimeout;
    freezeInfoP->frziMP       = msg->frzmMP;

    /*
     *  BFD_FREEZE_IN_PROGRESS has already been set by the vfsop 
     *  Get the lock and proceed if the freeze reference count is 0
     */
    mutex_lock(&dmnP->dmnFreezeMutex);
    if (dmnP->dmnFreezeRefCnt) {
        dmnP->dmnFreezeWaiting++;
        assert_wait_mesg( (vm_offset_t)&dmnP->dmnFreezeWaiting,
                           FALSE,
                           "bs_freeze" );
        mutex_unlock(&dmnP->dmnFreezeMutex);
        thread_block();
        dmnP->dmnFreezeWaiting--;
        MS_SMP_ASSERT(!dmnP->dmnFreezeRefCnt);
    } else {
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

    /*
     *  Check to see if we're racing an umount of the
     *  last fileset in the domain.  If so, return 
     *  domain not found.
     */
    FILESET_READ_LOCK(&FilesetLock );
    if (dmnP->mountCnt == 0) {
        FILESET_UNLOCK(&FilesetLock );
        msg->frzmStatus = ENOENT;
        ms_free (freezeInfoP);
        thread_wakeup( (vm_offset_t) &msg->frzmStatus );
        return;
    }
    FILESET_UNLOCK(&FilesetLock );

    /*
     * Prior to the upcoming exclusive transaction and call
     * to lgr_flush(), we write the BSR_DMN_FREEZE_ATTR record into the
     * (r)bmt with the current time in the dmnFreezeAttr.freezeBegin field.
     * This record is for debug purposes only and can not affect
     * the freezefs operation.  Therefore we ignore error handling
     * for this set of routines. 
     */
    logvdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    
    RBMT_THERE(dmnP) ? (mdap = logvdp->rbmtp) : (mdap = logvdp->bmtp);

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP, 0 );

    if (sts == EOK) {

        bzero((char *)&dmnFreezeAttr, sizeof( bsDmnFreezeAttrT ));

        sts = bmtr_get_rec( mdap,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ) );

        TIME_READ (tv);
        dmnFreezeAttr.freezeBegin = tv.tv_sec;
        dmnFreezeAttr.freezeCount++;

        sts = bmtr_put_rec( mdap,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ),
                            ftxH );

        ftx_done_n( ftxH, FTA_NULL );
    }

    /*
     *  Starting an exclusive transaction will allow
     *  all current transactions to complete and cause
     *  any new transactions to block. 
     */
    sts = FTX_START_EXC(FTA_NULL, &freezeInfoP->frziFtxH, dmnP, 0);
    if (sts != EOK) {
        mutex_lock(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeFlags &= ~BFD_FREEZE_IN_PROGRESS;
        mutex_unlock(&dmnP->dmnFreezeMutex);
        ms_free (freezeInfoP);
        msg->frzmStatus = EIO;
        thread_wakeup( (vm_offset_t) &msg->frzmStatus );
        return;
    }

    /*
     *  Flush the log
     */
    lgr_flush( dmnP->ftxLogP );

    /*
     *  Update the domain flag and set the timeout value
     */
    mutex_lock(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeFlags &= ~BFD_FREEZE_IN_PROGRESS;
    dmnP->dmnFreezeFlags |=  BFD_FROZEN;
    mutex_unlock(&dmnP->dmnFreezeMutex);
    TIME_READ (tv);
    if (freezeInfoP->frziTimeout < 0) {
        freezeInfoP->frziTimeout = 0x7FFFFFFF;
    } else if ( (0x7FFFFFFF - freezeInfoP->frziTimeout) <= tv.tv_sec ) {
        freezeInfoP->frziTimeout = 0x7FFFFFFF;
    } else {
        freezeInfoP->frziTimeout  += tv.tv_sec;
    }

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
    thread_wakeup( (vm_offset_t) &msg->frzmStatus );

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
    struct timeval     tv;
    domainT           *dmnP          = NULL;
    bsDmnFreezeAttrT   dmnFreezeAttr;
    ftxHT              ftxH          = FtxNilFtxH;
    struct vd         *logvdp;       /* pointer to log vd */
    bfAccessT         *mdap;         /* bfap to RBMT/BMT */

    /*
     *  If forced thawed due to timout 
     *      Post the VFS event
     *
     *  We do it here, before ending the transaction, to prevent problems
     *  with a racing unmount. The ftx_done_n will allow the unmount to
     *  proceed and the m_stat.f* info could disappear.
     */
    if (forced) {
        vfs_post_kernel_event(EVENT_VFS_THAW_TIMEOUT,
                              freezeInfoP->frziMP->m_stat.f_mntfromname,
                              freezeInfoP->frziMP->m_stat.f_mntonname);
    }

    dmnP = freezeInfoP->frziFtxH.dmnP;
    ftx_special_done_mode (freezeInfoP->frziFtxH, FTXDONE_LOGSYNC);
    ftx_done_n (freezeInfoP->frziFtxH, FTA_BS_FREEZE);
    mutex_lock(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeFlags &= ~BFD_FROZEN;
    mutex_unlock(&dmnP->dmnFreezeMutex);
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
    
    RBMT_THERE(dmnP) ? (mdap = logvdp->rbmtp) : (mdap = logvdp->bmtp);
    
    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP, 0 );
    
    if (sts == EOK) {

        bzero((char *)&dmnFreezeAttr, sizeof( bsDmnFreezeAttrT ));
    
        sts = bmtr_get_rec( mdap,
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ) );

        TIME_READ (tv);
        dmnFreezeAttr.freezeEnd = tv.tv_sec;

        sts = bmtr_put_rec( mdap, 
                            BSR_DMN_FREEZE_ATTR,
                            (void *) &dmnFreezeAttr,
                            sizeof( bsDmnFreezeAttrT ),
                            ftxH );

        ftx_done_n( ftxH, FTA_NULL );
    }

    return;
}
