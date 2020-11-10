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
 *      Backup utilities header file.
 *
 * Date:
 *
 *      Thu Apr 19 11:38:46 1990
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: util.h,v $ $Revision: 1.1.27.3 $ (DEC) $Date: 2004/07/19 05:36:49 $
 */

#ifndef _UTIL_H_
#define _UTIL_H_

#include <sys/types.h>
#include <nl_types.h>
#include <errno.h>
#include <locale.h>
#include <msfs/msfs_syscalls.h>
#include <io/common/devio.h>
#include <sys/mount.h>
#include <msfs/advfs_evm.h>

#ifndef TRUE
#define TRUE         1
#define FALSE        0
#endif

#define YES     1
#define NO      0

#define MINUTE  60
#define OSNAMEBUFSIZE  256
#define MAXUSERNAMELEN  65

extern char	os_name[OSNAMEBUFSIZE];		/* name of remote os */

/* system external variables */

extern int      magtape_ioctl_safe;

/*extern nl_catd   catopen(); */
extern nl_catd	catd;
extern nl_catd	comcatd;

extern char *
get_name(
    char *full_name
    );

extern void
abort_prog(
   char *msg, ...
   );

/* #ifndef _OSF_SOURC */
extern void 
dump_devstat ( 
   struct devget *devstat        /* in     */
   );
/* #endif */

extern u_short 
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
   unsigned long bytes_read, 
   unsigned long bytes_written, 
   unsigned long file_cnt, 
   struct timeval start_time
   );

extern int
op_retry( void );

extern int
buf_of_zeros(unsigned long *buf, int buf_size);

extern int
posix_style(int argc, char *argp[]);

extern int
post_event_backup_lock(char *Prog);

extern int
post_event_backup_unlock(char *Prog);

extern int
post_event_backup_error(char *Prog);

typedef struct {
    int fs_type;                /* filesystem's type (ufs, msfs, nfs, etc.) */
    int quotas_on;       	/* TRUE if quotas are turned on */
    mlBfSetIdT advfs_set_id;    /* For AdvFS only: Set ID */
    char path[MAXPATHLEN + 4];  /* name of mount point */
    char filesys[MNAMELEN]; 	/* mounted filesystem */
} fs_info_t;


extern fs_info_t Fs_info;

/* The ULTRIX DEVIOCGET ioctl structure (gets passed directly through the
 * pipe from rmt to rdump)
 * interprets ULTRIX longs as OSF ints
 */
struct ult_devget {
        short   category;               /* Category                     */
        short   bus;                    /* Bus                          */
        char    interface[DEV_SIZE];    /* Interface (string)           */
        char    device[DEV_SIZE];       /* Device (string)              */
        short   adpt_num;               /* Adapter number               */
        short   nexus_num;              /* Nexus or node on adapter no. */
        short   bus_num;                /* Bus number                   */
        short   ctlr_num;               /* Controller number            */
        short   rctlr_num;              /* Remote controller number     */
        short   slave_num;              /* Plug or line number          */
        char    dev_name[DEV_SIZE];     /* Ultrix device pneumonic      */
        short   unit_num;               /* Ultrix device unit number    */
        unsigned soft_count;            /* Driver soft error count      */
        unsigned hard_count;            /* Driver hard error count      */
        int     stat;                   /* Generic status mask          */
        int     category_stat;          /* Category specific mask       */
};

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
        PIPE,
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

extern void    rmtclose();
extern void    rmthost(char **rmt_peer_name, char *rmt_user_name);
extern int     rmtioctl(int cmd, int count);
extern struct devget   *rmtgenioctl();

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
ultrix_rmtdevice_stat( char *dev );

extern int
rmtopen_device( char *dev_to_open );

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
tape_get_new( char *device_name, int tape_num, char *remote_host_name );

extern int
open_new_tape ( char *device_name, char *remote_host_name );

extern int
tape_write_eof( int devFd, int tape_num );

extern int
tape_clear_error( int devFd, int tape_num );

#endif

/* end util.h */
