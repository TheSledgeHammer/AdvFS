/*
 *****************************************************************************
 **                                                                          *
 **  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991                            *
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
 * @(#)$RCSfile: fs_dir_routines.h,v $ $Revision: 1.1.70.3 $ (DEC) $Date: 2006/03/20 15:11:17 $
 */

#ifndef FS_DIR_ROUTINES
#define FS_DIR_ROUTINES

#include <msfs/ms_public.h>
#include <msfs/fs_dir.h>
/* #include <dirent.h> */
#include <sys/uio.h>

void
fs_init_directory (
                   char *buffer_pointer,
                   bfTagT bs_tag, 
                   bfTagT parent_bs_tag,
                   uint32T advfs_page_size
                   );

statusT
get_name(
         struct mount *mp,
         bfTagT bf_tag,
         char *buffer,
         bfTagT *dir_tag
         );

statusT
get_name2(
         char *name,
         bfTagT bf_tag,
         char *buffer,
         bfTagT *parent_tag
         );

statusT
get_undel_dir(
              char *dir_path,
              bfTagT *undelDirTag
              );

statusT
attach_undel_dir(
                 char *dir_path,
                 char *undel_dir_path
                 );

statusT
detach_undel_dir(
                 char *dir_path,
                 long xid
                 );

statusT
insert_seq(
           struct vnode* dvp,     /* in - directory vnode */
           bfTagT new_bs_tag,      /* in - tag of the new entry */
           struct vnode* nvp,     /* in - new file vnode */
           int flag,
           struct nameidata *ndp,
           bfSetT *bfSetp,
           struct mount *mp,
           ftxHT ftx_handle
           );

statusT
remove_dir_ent(
               struct nameidata *ndp,
               ftxHT ftx_handle
               );

statusT
remove_dots(
	    struct vnode *dvp,
	    ftxHT ftx_handle
	    );

statusT
seq_search (
            struct vnode* dir_vp,
            struct nameidata *ndp,
            bfTagT *return_bs_tag,
            fs_dir_entry **found_buffer,
            rbfPgRefHT *found_page_ref,
            int flags,
            ftxHT ftx_handle
            );

void
fs_dir_size_notice(
                   struct bfAccess* dirBfap, /* in - dir's access ptr */
                   int entrySize,            /* in - entry's size    */
                   uint pgNum,               /* in - dir's page number */
                   uint offset               /* in - offset in dir's page */
                   );

statusT
setup_for_glom_dir_entries(
                           char *dir_buffer,
                           fs_dir_entry **start_dir_p,
                           uint32T *entry_size,
                           uint32T *flags,
                           struct bfAccess *dir_access
                          );
void
glom_dir_entries(
                 char *dir_buffer,
                 fs_dir_entry *dir_p,
                 uint32T entry_size,
                 uint32T flags,
                 rbfPgRefHT page_ref
                );

uint32T
dir_trunc_start(
                struct vnode *, 
                unsigned long, 
                ftxHT
               );

void
dir_trunc_finish(
                 struct vnode *vp  
                );

statusT
bf_get(
       bfTagT   bf_tag,     
       struct fsContext *dir_context,
       int flag,
       struct mount *mp,
       struct vnode **vp  
       );

/*
 * bf_get_l
 *
 * Access a bitfile. Analagous to iget operations in other file systems.
 */
statusT
bf_get_l(
         bfTagT   bf_tag,     /* in - tag of bitfile to access */
         struct fsContext *dir_context, /* in - parent dir context pointer */
         int flag,            /* in - flag - META_OPEN, metadata bf being opened */
                            /*           - DONT_UNLOCK, don't unlock the dir */
         struct mount *mp,    /* in - the mount structure pointer */
         struct vnode **vp,   /* out - vnode corresponding to the */
                            /* accessed bitfile  */
         struct nameidata *ndp  /* in - ptr to nameidata data */
       );

void
fs_assemble_dir (
                 char *dir_buffer,
                 bfTagT new_bs_tag,
                 char *new_file_name
                 );

statusT
fs_create_file(
               struct vattr *vap,
               struct nameidata *ndp
               );

statusT
fs_update_stats(
                struct vnode *vp, /* in - vnode of bitfile */
                struct bfAccess *bfap,  /* in - bitfile's access structure */
                ftxHT ftx_handle,       /* in - ftx handle. may be nil. */
                uint32T flags           /* in - flags */
                );

int
fs_trunc_test (
               struct vnode *vp
               );

int
fs_write(
         struct vnode *vp,
         struct uio *uio,
         int ioflag,
         struct ucred *cred
         );

#define ADVFS_IO_NO_ZERO 0x10000000 /* secret ioflag for fs_write() */

/*
 * For POSIX 1003.1b sync io, need to pass ioflag down through the file
 * system.  fs_read is now "parallel" in look to fs_write.
 */

int
fs_read(
        struct vnode *vp,
        struct uio *uio,
        int ioflag,
        struct ucred *cred
        );

opxT fs_insert_undo;

void
fs_insert_undo(
               ftxHT ftxH,
               int opRecSz,
               void* opRec
               );

void
fs_cleanup_thread(void);

void
fs_init_cleanup_thread(void);

statusT
fs_create_file_set(
    bfSetT *bfSetp,
    gid_t quotaId,
    ftxHT ftxH
    );

int
fs_write_add_stg(
    struct bfAccess *bfap,         /* in - file's access structure */
    unsigned long page_to_write,   /* in - pinned pg's number */
    unsigned long bytes_to_write,  /* in - bytes that remain to be written */
    unsigned long file_size,       /* in - bytes in file */
    unsigned starting_byte_in_page,/* in - first byte in page */
    struct ucred *cred,            /* in - credentials of caller */
    int add_stg_flags,             /* in - allocation flags */
    int *pgs_allocated             /* out - # of pages allocated */
    );

/*  add_stg_flags */
#define WASF_SPARSE     0x0001     /* sparse allocation */
#define WASF_LOGGING    0x0002     /* data logging on */
#define WASF_SYNC       0x0004     /* synchronous write */
#define WASF_NO_ZERO    0x0008     /* no-zero-fill allocation */

statusT
fs_delete_frag(
    bfSetT *bfSetp,               /* in - file's bfSet pointer */
    struct bfAccess *bfap,        /* in - file's access structure */
    struct vnode *vnode,          /* in - vnode if external open */
    struct ucred *cred,           /* in - credentials of caller */
    int quotas_done,              /* in - flag, caller did quota update */
    ftxHT parentFtxH              /* in - parent transaction */
    );

statusT
copy_and_del_frag (
                   struct vnode* vp,
                   struct ucred *cred,
                   int flushit, /* synchronous unpin flag */
                   ftxHT ftxH
                   );

#endif /*FS_DIR_ROUTINES*/
