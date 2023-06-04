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
 * Facility:
 *
 *      AdvFS Storage System
 */

/* message structures for message passing between threads */
typedef enum {
   MSG_NONE,
   MSG_WRITE_BLK,
   MSG_WRITE_BUF,
   MSG_READ_BLKS,
   MSG_READ_BUFS,
   MSG_BLK_READY,
   MSG_BUF_READY,
   MSG_END_OF_FILE,
   MSG_NEW_FD,
   MSG_BLK_READ_ERROR,
   MSG_VOL_SET_DONE,
   MSG_TERMINATE
} msg_type_t;

typedef struct msg {
   msg_type_t type;

   int32_t num; /* general purpose count field */

   /* handles to objects */
   union {
      int32_t fd;
   } h;

   /* pointers to data */
   union {
      struct blk_t *blk;
      char *buf;
   } p;
} mssg_t;

static mssg_t
   terminate_msg = { MSG_TERMINATE };

static mssg_t
   read_blks_msg = { MSG_READ_BLKS };

/* end restore_msg.h */
