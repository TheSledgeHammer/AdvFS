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
 * Abstract:
 *
 *      Message queue routines.
 *
 */

#include <advfs/backup.h>
#include <advfs/util.h>
#include <stdarg.h>
#include <sys/signal.h>
#include "libvdump_msg.h"

extern nl_catd catd;
extern char *sys_errlist[];

/* message queue private definitions */

#define MAX_MSG_SIZE  48

/*
 * message queue entry 
 */
typedef struct msg_q_entry {
    int32_t msg_length;                       /* message size (in bytes)      */
    char msg[MAX_MSG_SIZE];                   /* message                      */
    struct msg_q_entry *next_msg;             /* pointer to next msg in queue */
} msg_q_entry_t;

#define MAX_MSGS  32

/*
 * message queue descriptor
 */
typedef struct msg_q {
    pthread_mutex_t mutex;             /* synchronization mutex              */
    pthread_cond_t  cv;                /* synchronization condition variable */
    msg_q_entry_t *q_head;             /* msg queue head pointer             */
    msg_q_entry_t *q_tail;             /* msq queue tail pointer             */
    msg_q_entry_t *free_lst;           /* list of free message entries       */
    pthread_cond_t free_lst_cv;        /* free list condition variable       */
    int32_t free_lst_waiters;          /* num threads waiting for a free msg */
    int32_t num_msgs;                  /* number of message on the queue     */
    int32_t waiters;                   /* num threads waiting for a msg      */
    int32_t total_waits;               /* num times thread waited for a msg  */
    int32_t total_sends;               /* total number messages sent         */
    int32_t total_recvs;               /* total number messages received     */
    int32_t max_q_length;              /* most messages ever on the queue    */
    int32_t total_free_lst_waits;      /* num times thread waited for free   */
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

    pthread_mutex_init(&msg_q->mutex, NULL);
    pthread_cond_init(&msg_q->cv, NULL);
    pthread_cond_init(&msg_q->free_lst_cv, NULL);

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
 *
 *         At this time, the thread using the message queue has 
 *         entirely exited before this function is called.  Should this
 *         ever change, a better termination method must be implemented.
 */

{
    msg_q_t *msg_q;
    msg_q_entry_t *cur_msg;

    /*-----------------------------------------------------------------------*/

    msg_q = (msg_q_t *) msg_q_h;

    pthread_mutex_destroy(&msg_q->mutex);
    pthread_cond_destroy(&msg_q->cv);
    pthread_cond_destroy(&msg_q->free_lst_cv);

    /* deallocate all message entries */
    free( msg_q->free_lst );

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
        int32_t msg_len          /* in - message byte size */
        )
{
    msg_q_t *msg_q;
    msg_q_entry_t *new_msg;

    /*-----------------------------------------------------------------------*/

    if (msg_len > MAX_MSG_SIZE) {
        abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL8, 
            "%s: msg length <%d> exceeds max msg length <%d>\n"),
            msg, msg_len, MAX_MSG_SIZE ) ;
    }

    msg_q = (msg_q_t *) msg_q_h;

    pthread_mutex_lock(&msg_q->mutex);

    /* allocate a message entry */
    while (msg_q->free_lst == NULL) {
        msg_q->free_lst_waiters++;
        pthread_cond_wait(&msg_q->free_lst_cv, &msg_q->mutex);
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

    if (msg_q->num_msgs == 0) {
        if (msg_q->q_head != msg_q->q_tail) {
            abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
        }
    } else if (msg_q->q_head == NULL) {
        abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, "msg queue corruption") );
    }

    /*
     * if there are other threads waiting for a message, wake them so that
     * they can receive a message 
     */

    pthread_mutex_unlock(&msg_q->mutex);
    pthread_cond_broadcast(&msg_q->cv);

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
         int32_t msg_len          /* in - size of msg buffer */
         )
{
    msg_q_t *msg_q;
    msg_q_entry_t *cur_msg;
    int32_t result;

    /*-----------------------------------------------------------------------*/

    msg_q = (msg_q_t *) msg_q_h;

    pthread_mutex_lock(&msg_q->mutex);

    while (msg_q->num_msgs == 0) {
        /* there are not messages on the queue so we wait for one */
        msg_q->waiters++;
        pthread_cond_wait(&msg_q->cv, &msg_q->mutex);
        msg_q->waiters--;

        msg_q->total_waits++;
    }

    cur_msg = msg_q->q_head;         /* get a pointer to the current message */

    if (msg_len < cur_msg->msg_length) {
        pthread_mutex_unlock(msg_q->mutex);
        abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL10, 
            "msg_recv: buf size <%d> is smaller than msg size <%d>\n"),
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

    /* update message queue counters */
    msg_q->num_msgs--;
    msg_q->total_recvs++;

    if (msg_q->num_msgs == 0) {
        if (msg_q->q_head != msg_q->q_tail) {
            abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, 
                "msg queue corruption") );
        }
    } else if (msg_q->q_head == NULL) {
        abort_prog( catgets(catd, S_MSGQUEUE_UTIL1, MSGQUEUE_UTIL9, 
            "msg queue corruption") );
    }

    pthread_mutex_unlock(&msg_q->mutex);
    pthread_cond_broadcast(&msg_q->free_lst_cv);
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

    pthread_mutex_lock(&msg_q->mutex);

    if (msg_q->num_msgs == 0) {
        result = FALSE;
    } else {
        result = TRUE;
    }

    pthread_mutex_unlock(&msg_q->mutex);

    return result;
}

/* end msg_recv */

/* end msgq_util.c */
