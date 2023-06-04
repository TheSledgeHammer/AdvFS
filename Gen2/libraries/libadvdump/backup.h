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
 * Abstract:
 *
 *      General AdvFS Backup header file.
 *
 */
#ifndef _BACKUP_
#define _BACKUP_

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <sys/quota.h>
#include <dirent.h>
#include <advfs/bs_public.h> 

/* error number and message stuff */

#define ERR_MSG sys_errlist[errno]
#define ERRMSG(eno) sys_errlist[(eno)]

/* constants */
#define FILE_NAME_SIZE          255            /* same as in bs_public.h */
#define BUF_SIZE                8192
#define LBUF_SIZE               (BUF_SIZE / sizeof(uint64_t))
#define BLK_CHUNK_SZ            1024
#define MIN_CHUNKS              2
#define MAX_CHUNKS              64
#define MIN_BLK_SZ              (BLK_CHUNK_SZ * MIN_CHUNKS)
#define MAX_BLK_SZ              (BLK_CHUNK_SZ * MAX_CHUNKS)
#define MIN_BUF_SZ              MIN_BLK_SZ
#define MAX_BUF_SZ              MAX_BLK_SZ
#define MIN_XOR_BLOCKS          1
#define MAX_XOR_BLOCKS         32
#define DEFAULT_XOR_BLOCKS      8
#define MIN_BLOCKS              2
#define MAX_BLOCKS             64
#define DEFAULT_BLOCKS          8

#define ERROR       -1
#define OKAY         0

#ifndef TRUE
#define TRUE         1
#define FALSE        0
#endif

/* 
 * AdvFS quota file names and tags.
 */
#define USR_QUOTA_FILE	"quotas"
#define GRP_QUOTA_FILE	".group.usage"
#define USR_QUOTA_INO 	USER_QUOTA_FILE_TAG
#define GRP_QUOTA_INO 	GROUP_QUOTA_FILE_TAG

typedef unsigned char byte;

#ifndef MAX
#define MAX( a, b ) ((a) > (b) ? (a) : (b))
#define MIN( a, b ) ((a) < (b) ? (a) : (b))
#endif
 
/* globals - common to vdump and vrestore */
extern char *Prog;              /* defined in vdump.c and vrestore.c */
extern FILE *Local_tty;         /* defined in vdump.c and vrestore.c */

#endif /* _BACKUP_ */

/* end backup.h */
