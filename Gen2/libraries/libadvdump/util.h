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
 *      Backup utilities header file.
 *
 */

#ifndef _UTIL_H_
#define _UTIL_H_

#include <sys/types.h>
#include <nl_types.h>
#include <errno.h>
#include <locale.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/advfs_evm.h>
#include <advfs/backup.h>

#ifndef TRUE
#define TRUE         1
#define FALSE        0
#endif

#define YES     1
#define NO      0

#define MINUTE  60
#define OSNAMEBUFSIZE  256
#define MAXUSERNAMELEN LOGIN_NAME_MAX

extern char	os_name[OSNAMEBUFSIZE];		/* name of remote os */

#define MNTTYPE_ADVFS "advfs"

/* system external variables */

extern nl_catd	catd;

extern void
abort_prog(
   char *msg, ...
   );

extern uint16_t
crc16 ( 
   register u_char *buf,         /* in     */
   register int n                /* in     */
   );

extern void 
xor_bufs ( 
   u_char *buf1,                 /* in/out */
   u_char *buf2,                 /* in     */
   int len                       /* in     */
   );

/* 
 * message queue definitions and routines 
 */
typedef char * msg_q_handle_t;

extern int 
msg_q_create ( 
   msg_q_handle_t *msg_q_h       /* out    */
   );

extern void 
msg_q_delete ( 
   msg_q_handle_t msg_q_h,      /* in     */
   char *q_name                 /* in     */
   );

extern void 
msg_snd ( 
   msg_q_handle_t msg_q_h,      /* in     */
   char *msg,                   /* in     */
   int msg_len                  /* in     */
   );

extern void 
msg_recv ( 
   msg_q_handle_t msg_q_h,      /* in     */
   char *msg,                   /* out    */
   int msg_len                  /* in     */
   );

extern int 
msg_available ( 
   msg_q_handle_t msg_q_h       /* in     */
   );

/* 
 * buf pool definitions and routines 
 */
typedef char * buf_pool_handle_t;

extern int
buf_pool_create ( 
   buf_pool_handle_t *buf_pool_h, /* out    */
   int num_bufs,                  /* in     */
   int buf_size                   /* in     */
   );

extern void 
buf_pool_delete ( 
   buf_pool_handle_t buf_pool_h   /* in     */
   );

extern int 
buf_allocate ( 
   buf_pool_handle_t buf_pool_h,  /* in     */ 
   char **buf                     /* out    */
   );

extern int 
buf_release ( 
   buf_pool_handle_t buf_pool_h,  /* in     */
   char *buf                      /* in     */
   );

extern int 
buf_pool_expand ( 
   buf_pool_handle_t buf_pool_h,  /* in     */
   int num_bufs                   /* in     */
   );

extern int
buf_pool_buf_size(
    buf_pool_handle_t buf_pool_h  /* in */
    );

extern void
show_statistics ( 
   uint64_t bytes_read, 
   uint64_t bytes_written, 
   uint64_t file_cnt, 
   struct timeval start_time
   );

extern int
op_retry( void );

extern int
buf_of_zeros(uint64_t *buf, int buf_size);

extern int
posix_style(int argc, char *argp[]);

extern int
post_event_backup_lock(char *Prog);

extern int
post_event_backup_unlock(char *Prog);

extern int
post_event_backup_error(char *Prog);

typedef struct {
    char       fs_type[32];           /* filesystem's type (hfs, advfs, etc.) */
    int        quotas_on;             /* TRUE if quotas are turned on */
    bfSetIdT   advfs_set_id;          /* For AdvFS only: Set ID */
    char       path[MAXPATHLEN+1];    /* name of mount point */
    char       filesys[FILE_NAME_SIZE];  /* mounted filesystem */
    int64_t    fs_id;                 /* Filesystem ID, for statvfs compares */
    char       fset[MAXPATHLEN+1];    /* For AdvFS only; Fileset name */
    char       fsname[MAXPATHLEN+1];  /* For AdvFS only; Filesystem name */
} fs_info_t;


extern fs_info_t Fs_info;

/*  DEV_SIZE used to be defined in kernel/io/common/devio.h --SGF 4/9/03 */

#ifndef DEV_SIZE
#define DEV_SIZE        0x08            /* Eight bytes                  */
#endif 

extern void
print_mtio(
    int fd
);

extern void 
get_fs_info(
    char *path,
    fs_info_t *fs_info,
    int backup_subtree,
    int verbose
    );

extern void
silent_fprintf(char *msg, ...);

typedef enum {
        TAPE,
        DISK,
        DEV_FILE,
        NONE
} Dev_type_t;

extern int
query(
    char *question
     );

extern void
broadcast(
    char *message
         );

/* remote function declarations */

typedef enum {
    DUMP,
    RESTORE
} ProgType_t;

extern void    rmtclose();
extern void    rmthost(char **rmt_peer_name, char *rmt_user_name);
extern int     rmtioctl(int cmd, int count);

extern int
rmtopen(
        char    *tape_device,
        int     flags
       );

extern int
rmtread(
        char    *buf,
        int     count
       );

extern int
rmtseek(
        int     offset,
        int     pos
       );
extern int
rmtwrite(
        char           *buf,
        int             count
        );

extern int
rmtopen_device( ProgType_t prog, char *dev_to_open );


extern int
get_dev_type(
    int  fd,
    char *dev_to_open
    );

extern int
tape_unload( int devFd, int tape_num );

extern int
tape_rewind( int devFd, int tape_num );

extern int
tape_get_new( ProgType_t prog, char *device_name, int tape_num, char *remote_host_name );

extern int
tape_write_eof( int devFd, int tape_num );


#endif

/* end util.h */
