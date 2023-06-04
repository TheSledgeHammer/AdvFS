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
 *      AdvFS
 *
 * Abstract:
 *
 *      Message Queue definitions and prototypes;
 *
 */

#ifndef _BS_MSG_QUEUE_H_
#define _BS_MSG_QUEUE_H_

typedef char * msgQHT;  /* handle to a message queue */

statusT
msgq_create(
    msgQHT *msgQH,          /* out - message queue handle */
    int32_t maxMsgs,        /* in - initial message buffers to allocate for queue */
    int32_t maxMsgSize,     /* in - max number of bytes per message */
    int32_t allow_q_growth, /* in - flag for allowing q to grow or not */
    int32_t radId           /* in - rad Id of desired rad */
    );

void
msgq_delete(
    msgQHT msgQH        /* in - message queue handle */
    );

void *
msgq_alloc_msg(
    msgQHT msgQH        /* in - message queue handle */
    );

void *
msgq_alloc_msg_nowait(
    msgQHT msgQH        /* in - message queue handle */
    );

void
msgq_free_msg(
    msgQHT msgQH,       /* in - message queue handle */
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

void
msgq_purge_msgs(
        msgQHT msgQH        /* in - message queue handle */
        );

int
msgq_get_length(
        msgQHT msgQH        /* in - message queue handle */
        );

#endif /* _BS_MSG_QUEUE_H_ */
