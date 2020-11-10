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
 *      General Backup header file.
 *
 * Date:
 *
 *      Thu Apr 19 11:40:17 1990
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: backup.h,v $ $Revision: 1.1.27.2 $ (DEC) $Date: 2003/08/27 10:45:53 $
 */

#ifndef _BACKUP_
#define _BACKUP_

static char*
rcsid_backup = "backup.h - $Date: 2003/08/27 10:45:53 $";

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
#include <sys/mount.h>
#include <sys/dir.h>
#include <sys/quota.h>

#ifndef _OSF_SOURCE

#include <sys/devio.h>

#else /* _OSF_SOURCE */

#include <dirent.h>

#endif /* _OSF_SOURCE */

/* error number and message stuff */

extern int errno;
extern int sys_nerr;
extern int saved_err;
extern int Debug;
extern char *sys_errlist[];

#define ERR_MSG sys_errlist[errno]
#define ERRMSG(eno) sys_errlist[(eno)]

/* constants */

#define MAX_PATH_SZ             MAXPATHLEN     /* defined in sys/param.h */
#ifdef MAXNAMLEN
#define FILE_NAME_SIZE          (MAXNAMLEN+1)  /* defined in dirent.h */
#else
#define FILE_NAME_SIZE          (NAME_MAX+1)  /* defined in syslimits.h */
#endif
#define BUF_SIZE                8192
#define LBUF_SIZE               (BUF_SIZE / sizeof( unsigned long ))
#define BLK_CHUNK_SZ            1024
#define MIN_CHUNKS              2
#define OLD_MAX_CHUNKS		64
#define MAX_CHUNKS              2048
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
#define USR_QUOTA_FILE	"quota.user"
#define GRP_QUOTA_FILE	"quota.group"
#define USR_QUOTA_INO 	4
#define GRP_QUOTA_INO 	5

/*
 * Keywords in /etc/fstab for quota activation.
 */
#define FSTAB_UQUOTAS   "userquota"
#define FSTAB_GQUOTAS   "groupquota"

/*
 * For quota file tag sequence number.
 * Avoid redefining here as it is defined in bs_ods.h
 */
#ifndef BS_TD_IN_USE 
#define BS_TD_IN_USE 0x8000;
#endif

typedef unsigned char byte;

#ifndef MAX
#define MAX( a, b ) ((a) > (b) ? (a) : (b))
#define MIN( a, b ) ((a) < (b) ? (a) : (b))
#endif
 
/* globals - common to vdump and vrestore */
extern char *Prog;              /* defined in vdump.c and vrestore.c */
extern int Show_resources;      /* defined in vdump.c and vrestore.c */
extern FILE *Local_tty;         /* defined in vdump.c and vrestore.c */

#include "thread_interfaces.h"

#ifndef _OSF_SOURCE
extern int sync();
extern int getrusage();
extern int gettimeofday();
extern int lstat();
extern int ioctl();
extern int bcopy();
extern int bzero();
extern int fsync();
extern int fchown();
extern int fchmod();
extern int utimes();
extern int readlink();
extern int symlink();
extern int mknod();
#define S_ISLNK(m)      (((m)&(S_IFMT)) == (S_IFLNK))
#define S_ISSOCK(m)     (((m)&(S_IFMT)) == (S_IFSOCK))
#endif

/* we should only use fprintf() so we make sure a printf() doesn't compile */

#define printf *

#endif /* _BACKUP_ */

/* end backup.h */
