/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
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
 *
 * Facility:
 *
 *      ADVFS
 *
 * Abstract:
 *
 *      Message queue routines.
 *
 * Date:
 *
 *      Wed Apr 10 14:27:18 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_msg_queue.c,v $ $Revision: 1.1.41.2 $ (DEC) $Date: 2008/02/12 13:07:00 $"

#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_msg_queue.h>

#define ADVFS_MODULE BS_MSG_QUEUE

/* message queue private definitions */

typedef uint32T msgSizeT;
decl_simple_lock_info(, ADVmsgQT_mutex_lockinfo )

/*
 * msgStateT
 *
 * Each message buffer in the pool is in one of the following states. 
 * The states indicate the 'location' of the buffer.
 */

typedef enum {
    MSG_FREE  = 1,      /* msg buffer is on the free list */
    MSG_READY = 2,      /* msg buffer is on the ready list */
    MSG_USER  = 4       /* msg buffer is held by user */
} msgStateT;

/*
 * message queue entry 
 */

typedef struct msgQEntry {
    msgSizeT  msgSize;               /* max bytes in user msg area */
    msgStateT msgState;              /* msg buffer's state */
    struct    msgQEntry *nextMsg;    /* pointer to next msg */
    /* uint32T msg[1];                  dummy debug var - where user         */
                                     /*     message starts.  The actual      */
                                     /*     lenght is in 'msgSize' bytes.    */
} msgQEntryT;

/*
 * message queue descriptor
 */

typedef struct msgQ {
    mutexT mutex;                     /* synchronization mutex              */
    cvT cv;                           /* synchronization condition variable */
    msgQEntryT *qHead;                /* msg queue head pointer             */
    msgQEntryT *qTail;                /* msq queue tail pointer             */
    msgQEntryT *freeMsgLst;           /* list of free message entries       */
    uint16T readyMsgs;                /* number of message on the queue     */
    uint16T freeMsgs;                 /* number of free messages            */
    uint16T userMsgs;                 /* number of messages held by user    */
    uint16T waiters;                  /* num threads waiting for a msg      */
    uint32T totalWaits;               /* num times thread waited for a msg  */
    uint32T totalSends;               /* total number messages sent         */
    uint32T totalRecvs;               /* total number messages received     */
    uint32T maxQLen;                  /* most messages ever on the queue    */
    uint16T maxMsgs;                  /* current max message in pool        */
    uint16T maxMsgSize;               /* max message size in bytes          */
    uint16T allow_q_growth;           /* TRUE: allow q size growth, FALSE no*/
    uint32T radId;                    /* RAD Id that this Queue resides on  */
} msgQT;

static const msgQT nilMsgQ = { 0 };

/*
 * Both of these constants MUST be aligned on the machine architecture's
 * size of a pointer; on ALPHA it is 8 bytes.  This
 * is why they are rounded up to the sizeof( char * ).
 */

#define MSGQ_ENTRY_HDR_SZ \
    (roundup( sizeof( msgQEntryT), sizeof( char * ) ))

#define MSGQ_ENTRY_SZ( maxMsgSz ) \
    (roundup( MSGQ_ENTRY_HDR_SZ + (maxMsgSz), sizeof( char *) ))

static int
ulmq_init_freelist( msgQT *msgQ, uint16T nmsgs );

static void
ulmq_destroy_freelist( msgQT *msgQ );


/*
 * msgq_create - Creates a message queue.  The message queue can be
 * used for inter-thread communication.  Message are added (sent) to
 * the queue's tail and removed (received) from the queue's head
 * (see msq_send_msg() and msgq_recv_msg()).
 * To send a message one must first allocate (msgq_alloc_msg()) a message 
 * buffer and fill it in.  After receiving a message one must free the buffer
 * (msgq_free_msg()).
 *
 * returns EBAD_PARAMS, ENO_MORE_MEMORY or EOK.
 */

statusT
msgq_create(
    msgQHT *msgQH,      /* out - message queue handle */
    int maxMsgs,        /* in -  initial message buffers to allocate for queue */
    int maxMsgSize,      /* in - max number of bytes per message */
    int allow_q_growth,  /* in - flag TRUE, allow q length to grow; FALSE no */
    int radId           /* in - rad Id of desired rad */
    )
{
    msgQT *msgQ = NULL;

    if ((maxMsgs <= 0) || (maxMsgSize < 0) || (radId < 0) || (radId >= nrads)) {
        return EBAD_PARAMS;
    }

    /*
     * Create and initialze queue structure.
     */
    msgQ = (msgQT *) ms_rad_malloc_no_wait( sizeof( msgQT ), M_INSIST, radId );

    if (msgQ == NULL) {
        return ENO_MORE_MEMORY;
    }

    *msgQ = nilMsgQ;
    msgQ->maxMsgSize = maxMsgSize;
    msgQ->radId = radId;

    /*
     * Allocate and initialize the message entries (in the free list).
     */
    msgQ->maxMsgs = ulmq_init_freelist( msgQ, maxMsgs );
    msgQ->freeMsgs  = msgQ->maxMsgs;
    msgQ->allow_q_growth = allow_q_growth;

    if (msgQ->freeMsgLst == NULL) {
        ms_free( msgQ );
        return ENO_MORE_MEMORY;
    }

    mutex_init3( &msgQ->mutex, 0, "msgQMutex", ADVmsgQT_mutex_lockinfo );
    cv_init( &msgQ->cv );

    *msgQH = (msgQHT) msgQ;

    return EOK;
} /* end msgq_create */


/*
 * msgq_alloc_msg
 *
 * Used to get a free message buffer.  Returns a pointer to a free
 * buffer or NULL if none are available.
 */

void *
msgq_alloc_msg(
    msgQHT msgQH  /* in - message queue handle */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    void *msg;

    mutex_lock( &msgQ->mutex );

    msg = ulmq_alloc_msg( msgQH );

    mutex_unlock( &msgQ->mutex );

    return msg;
} /* end msgq_alloc_msg */


/*
 * msgq_send_msg - Used to receive a message.  This routine removes a
 * message from the message queue's head.  If there are no messages in
 * the queue then this routine waits until a message is available.
 */

void
msgq_send_msg(
    msgQHT msgQH,       /* in - message queue handle */
    void *msg           /* in - message ptr */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;

    mutex_lock( &msgQ->mutex );

    ulmq_send_msg( msgQH, msg );

    mutex_unlock( &msgQ->mutex );
} /* end msgq_send_msg */


/*
 * msgq_recv_msg - Used to receive a message.  This routine removes a
 * message from the message queue's head.  If there are no messages in
 * the queue then this routine waits until a message is available.
 */

void *
msgq_recv_msg(
    msgQHT msgQH  /* in - message queue handle */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    void *msg;

    mutex_lock( &msgQ->mutex );

    msg = ulmq_recv_msg( msgQH, &msgQ->mutex );

    mutex_unlock( &msgQ->mutex );

    return msg;
} /* end msgq_recv_msg */


/*
 * msgq_free_msg
 * 
 * Used to put a message buffer that was acquired via msgq_alloc_msg()
 * or msgq_recv_msg() back on the free list.
 */

void
msgq_free_msg(
    msgQHT msgQH, /* in - message queue handle */
    void *msg
    )
{
    msgQT *msgQ = (msgQT *) msgQH;

    mutex_lock( &msgQ->mutex );

    ulmq_free_msg( msgQH, msg );

    mutex_unlock( &msgQ->mutex );
} /* end msgq_free_msg */


/*
 * msgq_purge_msgs
 *
 * Used to purge out the messages in the q.
 * It is callers responsibility to ensure that more messages
 * cannot be sent.
 */

void
msgq_purge_msgs(
    msgQHT msgQH /* in - message queue handle */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    void *msg;

    mutex_lock( &msgQ->mutex );

    while (msgQ->readyMsgs > 0) {
        msg = ulmq_recv_msg( msgQH, &msgQ->mutex );
        ulmq_free_msg( msgQH, msg );
    }
    mutex_unlock( &msgQ->mutex );
} /* end msgq_purge_msgs */


/*
 * msgq_get_length
 *
 * returns number of ready messages
 * currently in the queue.
 */

int
msgq_get_length(
    msgQHT msgQH /* in - message queue handle */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    int cnt = 0;

    cnt = msgQ->readyMsgs;
    return (cnt);
} /* end msgq_get_length */


/*
 * msgq_destroy - Destroys a message queue.
 *
 * returns:
 *    0: A-ok
 *    1: There are user messages outstanding
 *    2: There are ready messages outstanding
 *    3: There are threads waiting on the queue
 *    4: This message queue is corrupted
 */

int
msgq_destroy(
    msgQHT msgQH       /* in - message queue handle */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;

    mutex_lock( &msgQ->mutex );

    if ( (msgQ->userMsgs != 0) ){
        mutex_unlock( &msgQ->mutex );
        return 1;
    }
    if ( (msgQ->readyMsgs != 0) ){
        mutex_unlock( &msgQ->mutex );
        return 2;
    }
    if ( (msgQ->waiters != 0) ){
        mutex_unlock( &msgQ->mutex );
        return 3;
    }
    if ( (msgQ->freeMsgs != msgQ->maxMsgs) ){
        /* the list is corrupt!!! */
        MS_SMP_ASSERT( msgQ->freeMsgs == msgQ->maxMsgs );
        mutex_unlock( &msgQ->mutex );
        return 4;
    }

    ulmq_destroy_freelist( msgQ );

    mutex_unlock( &msgQ->mutex );
    mutex_destroy( &msgQ->mutex );

    ms_free( msgQ );

    return 0;
} /* end msgq_destroy */


/****************************************************************************
 ** The following routines are identical to the ones above except the caller
 ** must hold a mutex locked to protect the queue.  'ulmg_' means
 ** "User-Locked Message Queue" (catchy eh?).  None of the following
 ** routines should ever refer directly to the mutex in the queue
 ** descriptor.  If a ulmq_ routine needs to access a mutex then it
 ** must be passed to via its parameters.
 ****************************************************************************/

/*
 * ulmq_alloc_msg
 *
 * Used to get a free message buffer.  Returns a pointer to a free
 * buffer or NULL if none are available.  
 * 
 * User must lock msgQ->mutex to protect the message queue.
 */

void *
ulmq_alloc_msg(
    msgQHT msgQH  /* in - message queue handle */
    )
{
    msgQT *msgQ = NULL;
    msgQEntryT *newMsg = NULL;
    void *newMsgAddr = NULL;

    /*-----------------------------------------------------------------------*/

    msgQ = (msgQT *) msgQH;

    /* If there are no messages on the free list, allocate half the number
     * that already exist and initialize them.  Be careful since there is a
     * mutex held by the calling routine, so no sleeping!  Due to the design 
     * of the kernel allocator, it is very likely that the ms_malloc_no_wait()
     * will fail if the request is for more than one page so limit the request.
     */
    if (msgQ->freeMsgLst == NULL) {
        uint16T nmsgs = msgQ->maxMsgs / 2;    /* increase queue size by half */
        /*
        nmsgs = MIN(nmsgs, PAGE_SIZE / MSGQ_ENTRY_SZ(msgQ->maxMsgSize));
        */
        /* limit request to one page */
        /*
         * this should no longer be necessary b/c we are no longer allocating in one large
         * chunk, but rather one message at a time.  *And* the allocation is done in
         * ulmq_init_freelist, just prior to the 'actual' initialization.
         */

        if (!msgQ->allow_q_growth) {
            /* growth not allowed return null */
            return(NULL);
        }

        /* Now allocate & initialize the entries */
        msgQ->freeMsgs = ulmq_init_freelist( msgQ, nmsgs );
        msgQ->maxMsgs += msgQ->freeMsgs;

        if (msgQ->freeMsgs == 0) { 
            /* could not allocate more messages */
            return(NULL);
        }
    }

    /*
     * Get the next free buffer 
     */
    newMsg = msgQ->freeMsgLst;
    MS_SMP_ASSERT( newMsg->msgSize == msgQ->maxMsgSize );

    msgQ->freeMsgLst = msgQ->freeMsgLst->nextMsg;
    msgQ->freeMsgs--;
    msgQ->userMsgs++;

    newMsg->nextMsg = NULL;
    newMsg->msgState = MSG_USER;

    /* 
     * We will return the address of the user's message area just
     * past the header info.
     */
    newMsgAddr = ((char *) newMsg + MSGQ_ENTRY_HDR_SZ);

    return newMsgAddr;
}


/* ulmq_init_freelist() - allocates and initializes a series
 *                        of chained msg queue entries.
 */

static int
ulmq_init_freelist( msgQT *msgQ, uint16T nmsgs )
{
    msgQEntryT *msgEntp, *msgLastEntp;
    int i;

    /* Now allocate and initialize the entries */
    msgLastEntp = NULL;
    msgEntp = NULL;
    for (i = 0; i < nmsgs; i++) {
        msgEntp = (msgQEntryT *) 
          ms_rad_malloc_no_wait(MSGQ_ENTRY_SZ( msgQ->maxMsgSize ), M_INSIST, msgQ->radId);
        if (msgEntp == NULL){
            break;
        }
        msgEntp->nextMsg  = msgLastEntp;
        msgEntp->msgSize  = msgQ->maxMsgSize;
        msgEntp->msgState = MSG_FREE;
        msgLastEntp       = msgEntp;
    }

    msgQ->freeMsgLst = msgLastEntp;

    return i;
}


/*
 * ulmq_snd_msg - Used to send a message.  This routine adds the message to
 * the message queue's tail and wakes up any waiters.
 * 
 * User must lock msgQ->mutex to protect the message queue.
 */

void
ulmq_send_msg(
    msgQHT msgQH,       /* in - message queue handle */
    void *msg           /* in - message ptr */
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    msgQEntryT *newMsg = (msgQEntryT *) ((char *) msg - MSGQ_ENTRY_HDR_SZ);

    /*
     * Make sure the we were not given a bogus buffer.
     */

    if (newMsg->msgSize != msgQ->maxMsgSize) {
        ADVFS_SAD0( "ulmq_send_msg: corrupt msg buffer" );
    }
    if (newMsg->msgState != MSG_USER) {
        ADVFS_SAD0( "ulmq_send_msg: sending non-user msg buffer" );
    }
    if (msgQ->userMsgs == 0) {
        ADVFS_SAD0( "ulmq_send_msg: too many buffers sent" );
    }

    /* 
     * Add the new message to the ready queue 
     */

    if (msgQ->readyMsgs == 0) {
        msgQ->qHead = newMsg;
    } else {
        msgQ->qTail->nextMsg = newMsg;
    }

    msgQ->qTail = newMsg;
    newMsg->msgState = MSG_READY;

    /* 
     * Update message queue counters 
     */
    msgQ->readyMsgs++;
    msgQ->userMsgs--;
    msgQ->totalSends++;
    msgQ->maxQLen = MAX( msgQ->maxQLen, msgQ->readyMsgs );

    /*
     * If there are other threads waiting for a message, wake them so that
     * they can receive a message 
     */

    if (msgQ->waiters == 1) {
        if (AdvfsLockStats) {
            AdvfsLockStats->msgQSignal++;
        }

        cond_signal( &msgQ->cv );
    } else if (msgQ->waiters > 1) {
        if (AdvfsLockStats) {
            AdvfsLockStats->msgQBroadcast++;
        }

        cond_broadcast( &msgQ->cv );
    }
}


/*
 * ulmq_recv_msg - Used to receive a message.  This routine removes a
 * message from the message queue's head.  If there are no messages in
 * the queue then this routine waits until a message is available.
 * 
 * User must lock msgQ->mutex to protect the message queue.
 */

void *
ulmq_recv_msg(
    msgQHT msgQH, /* in - message queue handle */
    mutexT *mutex
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    msgQEntryT *curMsg = NULL;
    int wait = 0;

    if (msgQ->readyMsgs == 0) {
        /* 
         * There are no messages on the queue so we wait for one 
         */

        msgQ->waiters++;
        msgQ->totalWaits++;

        while (msgQ->readyMsgs == 0) {
            if (AdvfsLockStats) {
                if (wait) {
                    AdvfsLockStats->msgQReWait++;
                } else {
                    wait = 1;
                }
    
                AdvfsLockStats->msgQWait++;
            }

            cond_wait( &msgQ->cv, mutex );
        }

        msgQ->waiters--;
    }

    curMsg = msgQ->qHead;         /* get a pointer to the current message */

    if (curMsg->msgSize != msgQ->maxMsgSize) {
        ADVFS_SAD0( "ulmq_recv_msg: corrupt msg buffer" );
    }

    curMsg->msgState = MSG_USER;

    /* 
     * Remove current message from the queue 
     */

    msgQ->qHead = msgQ->qHead->nextMsg;

    if (msgQ->readyMsgs == 1) {
        msgQ->qTail = NULL;
    }

    /* 
     * Update message queue counters 
     */

    msgQ->readyMsgs--;
    msgQ->userMsgs++;
    msgQ->totalRecvs++;

    /*
     * We return the address of the user's message area just after the header.
     */
    return ((char *) curMsg + MSGQ_ENTRY_HDR_SZ);
}


/*
 * ulmq_free_msg
 * 
 * Used to put a message buffer that was acquired via msgq_alloc_msg()
 * or msgq_recv_msg() back on the free list.
 * 
 * User must lock msgQ->mutex to protect the message queue.
 */

void
ulmq_free_msg(
    msgQHT msgQH, /* in - message queue handle */
    void *msg
    )
{
    msgQT *msgQ = (msgQT *) msgQH;
    msgQEntryT *freeMsg = (msgQEntryT *) ((char *) msg - MSGQ_ENTRY_HDR_SZ);

    /*
     * Make sure we were not given a bogus message buffer.
     */

    if (freeMsg->msgSize != msgQ->maxMsgSize) {
        ADVFS_SAD0( "ulmq_free_msg: corrupt msg buffer" );
    }
    if (freeMsg->msgState != MSG_USER) {
        ADVFS_SAD0( "ulmq_free_msg: freeing non-user msg buffer" );
    }
    if (msgQ->userMsgs == 0) {
        ADVFS_SAD0( "ulmq_free_msg: too many buffers freed" );
    }

    /* 
     * Put message entry on the head of the free list 
     */

    freeMsg->nextMsg = msgQ->freeMsgLst;
    msgQ->freeMsgLst = freeMsg;
    freeMsg->msgState = MSG_FREE;

    msgQ->freeMsgs++;
    msgQ->userMsgs--;
}


/*
 * ulmq_destroy_freelist
 * 
 * Used as part of the deallocation of a message queue
 * 
 * User must lock msgQ->mutex to protect the message queue.
 */

static void
ulmq_destroy_freelist(
    msgQT *msgQ
    )
{
    msgQEntryT *msgEntp, *msgNextEntp;
    int i;

    msgEntp = msgQ->freeMsgLst;
    for(i = 0; i < msgQ->freeMsgs; i++){
      msgNextEntp = msgEntp->nextMsg;
      ms_free(msgEntp);
      msgEntp = msgNextEntp;
    }

    msgQ->freeMsgLst = NULL;
    msgQ->freeMsgs = 0;
}
