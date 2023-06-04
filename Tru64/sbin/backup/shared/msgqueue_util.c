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
 *      MegaSafe Storage System
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
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: msgqueue_util.c,v $ $Revision: 1.1.23.1 $ (DEC) $Date: 2003/01/06 22:45:37 $";
#endif


#include "backup.h"
#include "util.h"
#include <stdarg.h>
#include <sys/signal.h>
#include "libvdump_msg.h"


/* message queue private definitions */

#define MAX_MSG_SIZE  48

/*
 * message queue entry 
 */
typedef struct msg_q_entry {
    int msg_length;                           /* message size (in bytes)      */
    char msg[MAX_MSG_SIZE];                   /* message                      */
    struct msg_q_entry *next_msg;             /* pointer to next msg in queue */
} msg_q_entry_t;

#define MAX_MSGS  32

/*
 * message queue descriptor
 */
typedef struct msg_q {
    t_mutex mutex;                     /* synchronization mutex              */
    t_cond  cv;                        /* synchronization condition variable */
    msg_q_entry_t *q_head;             /* msg queue head pointer             */
    msg_q_entry_t *q_tail;             /* msq queue tail pointer             */
    msg_q_entry_t *free_lst;           /* list of free message entries       */
    t_cond  free_lst_cv;               /* free list condition variable       */
    int free_lst_waiters;              /* num threads waiting for a free msg */
    int num_msgs;                      /* number of message on the queue     */
    int waiters;                       /* num threads waiting for a msg      */
    int total_waits;                   /* num times thread waited for a msg  */
    int total_sends;                   /* total number messages sent         */
    int total_recvs;                   /* total number messages received     */
    int max_q_length;                  /* most messages ever on the queue    */
    int total_free_lst_waits;          /* num times thread waited for free   */
                                       /*    message entry.                  */
} msg_q_t;

/*
 * msg_q_create - Creates a message queue.  The message queue can be
 * used for inter-thread communication.  Message are added (sent) to
 * the queue's tail and removed (received) from the queue's head.
 *
 * returns ERROR or OKAY.
 */

int
msg_q_create(
             msg_q_handle_t *msg_q_h  /* out - message queue handle */
             )
{
    msg_q_t *msg_q;
    int i;

    /*-----------------------------------------------------------------------*/

    /*
     * Create and initialze queue structure.
     */
    msg_q = (msg_q_t *) malloc( sizeof( msg_q_t ) );

    if (msg_q == NULL) {return ERROR;}

    mutex__create( &msg_q->mutex );
    cond__create( &msg_q->cv );
    cond__create( &msg_q->free_lst_cv );

    msg_q->q_head       = NULL;
    msg_q->q_tail       = NULL;
    msg_q->num_msgs     = 0;
    msg_q->waiters      = 0;
    msg_q->total_waits  = 0;
    msg_q->total_sends  = 0;
    msg_q->total_recvs  = 0;
    msg_q->max_q_length = 0;
    msg_q->free_lst_waiters = 0;
    msg_q->total_free_lst_waits = 0;

    /*
     * Create and initialize the message entries (in the free list).
     */
    msg_q->free_lst = 
        (msg_q_entry_t *) malloc( sizeof( msg_q_entry_t) * MAX_MSGS);

    if (msg_q->free_lst == NULL) {
        free( msg_q );
        return ERROR;
    }

    for (i = 0; i < (MAX_MSGS - 1); i++) {
        msg_q->free_lst[i].next_msg = &msg_q->free_lst[i+1];
    }

    msg_q->free_lst[MAX_MSGS - 1].next_msg = NULL;

    *msg_q_h = (msg_q_handle_t) msg_q;

    return OKAY;
}
/* end msg_q_create */

/*
 * msg_q_delete - Deletes a message queue and deallocates all space
 * that was allocated to the queue and its entries.
 */

void
msg_q_delete(
             msg_q_handle_t msg_q_h,  /* in - message queue handle */
             char *q_name             /* in - */
             )
/*
 * TODO:   1) Need to add code to handle the case where there are still
 *         threads waiting for a message.  For now we just assume 
 *         (require) that there be no threads using the queue when
 *         msg_q_delete is called.
 */

{
    msg_q_t *msg_q;
    msg_q_entry_t *cur_msg;

    /*-----------------------------------------------------------------------*/

    msg_q = (msg_q_t *) msg_q_h;

    mutex__delete( &msg_q->mutex );
    cond__delete( &msg_q->cv );
    cond__delete( &msg_q->free_lst_cv );

    /* deallocate all message entries */
    free( msg_q->free_lst );

    if (Show_resources) {
        fprintf( stderr, 
                 catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL1, "\nMessage Queue (%10s) Usage Statistics\n"), q_name );
        fprintf( stderr,   "------------------------------------------\n" );
        fprintf( stderr, catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL2, "msgs deallocated: %d\n"), msg_q->num_msgs );
        fprintf( stderr, catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL3, "max queue length: %d\n"), msg_q->max_q_length );
        fprintf( stderr, catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL4, "total sends:      %d\n"), msg_q->total_sends );
        fprintf( stderr, catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL5, "free list size:   %d\n"), MAX_MSGS );
        fprintf( stderr, 
                 catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL6, "free list waits:  %d\n"), msg_q->total_free_lst_waits );

        if (msg_q->total_recvs > 0) {
            fprintf( stderr, catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL7, "%d of %d recvs waited; %f %% waited\n"),
                     msg_q->total_waits, msg_q->total_recvs, 
                     ((float) msg_q->total_waits / msg_q->total_recvs) *
                     100.0 );
        }
    }

    free( msg_q );              /* deallocate the message queue descriptor */
}
/* end msg_q_delete */

/*
 * msg_snd - Used to send a message.  This routine adds the message to
 * the message queue's tail.
 */

void
msg_snd(
        msg_q_handle_t msg_q_h,  /* in - message queue handle */
        char *msg,               /* in - message ptr */
        int msg_len              /* in - message byte size */
        )
{
    msg_q_t *msg_q;
    msg_q_entry_t *new_msg;

    /*-----------------------------------------------------------------------*/

    if (msg_len > MAX_MSG_SIZE) {
        abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL8, "%s: msg length <%d> exceeds max msg length <%d>\n"),
                    msg, msg_len, MAX_MSG_SIZE ) ;
    }

    msg_q = (msg_q_t *) msg_q_h;

    mutex__lock( msg_q->mutex );

    /* allocate a message entry */
    while (msg_q->free_lst == NULL) {
        msg_q->free_lst_waiters++;
        cond__wait( msg_q->free_lst_cv, msg_q->mutex );
        msg_q->free_lst_waiters--;

        msg_q->total_free_lst_waits++;
    }

    new_msg = msg_q->free_lst;
    msg_q->free_lst = msg_q->free_lst->next_msg;

    new_msg->next_msg   = NULL;
    new_msg->msg_length = msg_len;

    bcopy( msg, new_msg->msg, msg_len );              /* copy message      */

    /* add the new message to the message queue */
    if (msg_q->num_msgs == 0) {
        msg_q->q_head = new_msg;
    } else {
        msg_q->q_tail->next_msg = new_msg;
    }

    msg_q->q_tail = new_msg;

    /* update message queue counters */
    msg_q->num_msgs++;
    msg_q->total_sends++;
    msg_q->max_q_length = MAX( msg_q->max_q_length, msg_q->num_msgs );

    /*
     * if there are other threads waiting for a message, wake them so that
     * they can receive a message 
     */
    if (msg_q->waiters == 1) {
        cond__signal( msg_q->cv );
    } else if (msg_q->waiters > 1) {
        cond__broadcast( msg_q->cv );
    }

    if (msg_q->num_msgs == 0) {
        if (msg_q->q_head != msg_q->q_tail) {
            abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
        }
    } else if (msg_q->q_head == NULL) {
        abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
    }

    mutex__unlock( msg_q->mutex );
}
/* end msg_snd */

/*
 * msg_recv - Used to receive a message.  This routine removes a
 * message from the message queue's head.  If there are no messages in
 * the queue then this routine waits until a message is available.
 * 'msg_len' must be at least as large as the actual message's length.
 */

void
msg_recv(
         msg_q_handle_t msg_q_h,  /* in - message queue handle */
         char *msg,               /* in - ptr to msg buffer */
         int msg_len              /* in - size of msg buffer */
         )
{
    msg_q_t *msg_q;
    msg_q_entry_t *cur_msg;
    int result;

    /*-----------------------------------------------------------------------*/

    msg_q = (msg_q_t *) msg_q_h;

    mutex__lock( msg_q->mutex );

    while (msg_q->num_msgs == 0) {
        /* there are not messages on the queue so we wait for one */
        msg_q->waiters++;
        cond__wait( msg_q->cv, msg_q->mutex );
        msg_q->waiters--;

        msg_q->total_waits++;
    }

    cur_msg = msg_q->q_head;         /* get a pointer to the current message */

    if (msg_len < cur_msg->msg_length) {
        mutex__unlock( msg_q->mutex );
        abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL10, "msg_recv: buf size <%d> is smaller than msg size <%d>\n"),
                    msg_len, cur_msg->msg_length );
    }

    /* copy the message to the caller's buffer */
    bcopy( cur_msg->msg, msg, cur_msg->msg_length );
    result = cur_msg->msg_length;

    /* remove current message from the queue */
    msg_q->q_head = msg_q->q_head->next_msg;

    if (msg_q->num_msgs == 1) {
        msg_q->q_tail = NULL;
    }

    /* deallocate storage for current message */
    cur_msg->next_msg = msg_q->free_lst;
    msg_q->free_lst = cur_msg;

    if (msg_q->free_lst_waiters == 1) {
        cond__signal( msg_q->free_lst_cv );
   } else if (msg_q->waiters > 1) {
       cond__broadcast( msg_q->free_lst_cv );
   }

    /* update message queue counters */
    msg_q->num_msgs--;
    msg_q->total_recvs++;

    if (msg_q->num_msgs == 0) {
        if (msg_q->q_head != msg_q->q_tail) {
            abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
        }
    } else if (msg_q->q_head == NULL) {
        abort_prog( catgets(comcatd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
    }

    mutex__unlock( msg_q->mutex );
}
/* end msg_recv */

/*
 * msg_available - returns TRUE if there is a message in the specified
 * message queue, else returns FALSE.
 */
int
msg_available(
              msg_q_handle_t msg_q_h  /* in - message queue handle */
              )
{
    msg_q_t *msg_q;
    int result;

    /*-----------------------------------------------------------------------*/

    msg_q = (msg_q_t *) msg_q_h;

    mutex__lock( msg_q->mutex );

    if (msg_q->num_msgs == 0) {
        result = FALSE;
   } else {
       result = TRUE;
   }

    mutex__unlock( msg_q->mutex );

    return result;
}

/* end msg_recv */
/* end msgq_util.c */
