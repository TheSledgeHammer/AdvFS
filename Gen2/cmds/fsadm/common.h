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
 */
#ifndef COMMON_H
#define COMMON_H

#include <mntent.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/advfs_evm.h>
#include "fsadm_advfs_msg.h"

#define TRUE 1
#define FALSE 0

#define CMSG(n,s) catgets(catd,MS_FSADM_COMMON,n,s)

/* errno extern and macro define */
extern char *sys_errlist[];
#define ERRMSG(x) sys_errlist[x]

/* __blocktochar and __chartoblock haven't got public prototypes */
extern char *__blocktochar(char *blockp, char *charp);
extern char *__chartoblock(char *charp, char *blockp);


typedef struct mntlist mntlist_t;
struct mntlist {
    char *mntl_fromname;
    char *mntl_ondir;
    char *mntl_type;
    mntlist_t *mntl_next_p;
};

/* globally available variables */
extern char    *fstype; /* Type of file system -- storage in fsadm.c */
extern char    *action; /* action to be performed -- storage in fsadm.c */
extern nl_catd catd; /* Natual language catalog. */
extern int     full_usage; /* Print CUSTOM_USAGE message */

/* common utility functions */
void usage(const char *fmt, ...);             /* prints usage message */
int check_device(char *device, char *Prog, int rawflag);  /* checks the command line data */
void check_root(char *Prog);		      /* check for root user */
char *Voption(int argc, char **argv);         /* prints verified command line */
uint64_t size_str_to_fobs(const char *);      /* converts user arg to fobs */

/* EVM routines */
int init_event( advfs_ev *advfs_event );
int advfs_post_user_event( char *evname, advfs_ev advfs_event, char *Prog );

/* gets the domain on which a specified file resides */
int get_fsinfo_from_file(char *file, arg_infoT** fsInfo, int  *err);

/* single file migrate utility used by migrate and defrag */
int file_migrate (char *Prog, int fd, vdIndexT srcVolIndex, uint64_t srcFobOffset, 
        uint64_t srcFobCnt, vdIndexT dstVolIndex, uint64_t dstBlkOffset);

/* superblock functions */
statusT invalidate_superblock( char *devspec );
statusT revalidate_superblock( char *devspec );

/* mount list functions */
mntlist_t *mntl_load(int *err);
void mntl_free(mntlist_t *mnt_head);
char *mntl_findfromname(mntlist_t *mnt_head, char *search_ondir);
char *mntl_findondir(mntlist_t *mnt_head, char *search_fromdir);

/* multi-bitfile set defines, functions and structures */

typedef struct setInfo {
    bfTagT   setTag;
    int      mountedFlag;
    int      unmountFlag;
    char    *mntOnName;
    char    *dotTagsPath;
    int      fd;
} setInfoT;

#define DOT_TAGS "/.tags"

int open_dot_tags(arg_infoT* infop, bfTagT rootBfSetTag,
                  char** metarootpath, char* Prog);
int close_dot_tags(char* Prog);
char* get_tag_path(setInfoT* inSetInfo, bfTagT bfTag, bfTagT bfSetTag, int *unknown);
setInfoT* find_setTag (bfTagT setTag);
setInfoT* insert_setTag (bfTagT setTag, char* Prog);

/* TCR related things */

#define ADVFS_CLU_MEMBER_PATH "/etc/cfs/dev/.cdsl_files/member%d/advfs"

int tcr_fsname_in_use(char* fsname);

#endif /* COMMON_H */
