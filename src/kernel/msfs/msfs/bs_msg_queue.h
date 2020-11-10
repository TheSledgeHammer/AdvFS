/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
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
 * @(#)$RCSfile: bs_msg_queue.h,v $ $Revision: 1.1.22.1 $ (DEC) $Date: 2004/09/02 15:27:21 $
 */

#ifndef _BS_MSG_QUEUE_H_
#define _BS_MSG_QUEUE_H_

typedef char * msgQHT;  /* handle to a message queue */

statusT
msgq_create(
    msgQHT *msgQH,      /* out - message queue handle */
    int maxMsgs,        /* in - initial message buffers to allocate for queue */
    int maxMsgSize,     /* in - max number of bytes per message */
    int allow_q_growth, /* in - flag for allowing q to grow or not */
    int radId           /* in - rad Id of desired rad */
    );

void
msgq_delete(
    msgQHT msgQH        /* in - message queue handle */
    );

void *
msgq_alloc_msg(
    msgQHT msgQH        /* in - message queue handle */
    );

void
msgq_free_msg(
    msgQHT msgQH, /* in - message queue handle */
    void *msg
    );

void
msgq_send_msg(
    msgQHT msgQH,       /* in - message queue handle */
    void *msg           /* in - message ptr */
    );

void *
msgq_recv_msg(
    msgQHT msgQH        /* in - message queue handle */
    );

int
msgq_destroy(
    msgQHT msgQH       /* in - message queue handle */
    );

/*
 ** The following routines identical to the ones above except the caller
 ** must hold a mutex locked to protect the queue.  'ulmg_' means
 ** "User-Locked Message Queue" (catchy eh?).
 */

void *
ulmq_alloc_msg(
    msgQHT msgQH  /* in - message queue handle */
    );

void
ulmq_send_msg(
    msgQHT msgQH,       /* in - message queue handle */
    void *msg           /* in - message ptr */
    );

void *
ulmq_recv_msg(
    msgQHT msgQH,  /* in - message queue handle */
    mutexT *mutex
    );

void
ulmq_free_msg(
    msgQHT msgQH, /* in - message queue handle */
    void *msg
    );

#endif /* _BS_MSG_QUEUE_H_ */
