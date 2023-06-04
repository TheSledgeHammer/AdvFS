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
 *   AdvFS 
 *
 * Abstract:
 *
 *   Function prototype definitions of file system internal routines
 *
 */

#ifndef FS_DIR_ROUTINES
#define FS_DIR_ROUTINES

#include <advfs/ms_public.h>
#include <advfs/ms_privates.h>
#include <advfs/fs_dir.h>
#include <sys/uio.h>
#include <fs/vfs_ifaces.h>             /* struct nameidata */

/*
 * Used by dir_trunc_start
 */
extern uint64_t AdvfsMaxAllocFobCnt;

void
fs_init_directory (
    char *buffer_pointer,
    bfTagT bs_tag, 
    bfTagT parent_bs_tag,
    bf_fob_t dir_page_size
    );

statusT
advfs_get_name(
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
advfs_attach_undel_dir(
    char *dir_path,
    char *undel_dir_path
    );

statusT
advfs_detach_undel_dir(
    char *dir_path,
    ftxIdT xid
    );

statusT
insert_seq(
    struct vnode       *dvp,        /* in - directory vnode */
    bfTagT              new_bs_tag, /* in - tag of the new entry */
    struct vnode       *nvp,        /* in - new file vnode */
    char               *name,       /* in - new file name */
    int32_t             flag,
    struct nameidata   *ndp,
    bfSetT             *bfSetp,
    struct fileSetNode *fsnp,
    ftxHT               ftx_handle
    );

statusT
remove_dir_ent(
    struct vnode     *dvp,
    struct nameidata *ndp,
    char             *name,
    ftxHT             ftx_handle,
    int               call_dnlc_remove
    );

statusT
remove_dots(
    struct vnode *dvp,
    ftxHT ftx_handle
    );

statusT
seq_search (
    struct vnode     *dir_vp,
    struct nameidata *ndp,
    char             *name,
    bfTagT           *return_bs_tag,
    fs_dir_entry    **found_buffer,
    rbfPgRefHT       *found_page_ref,
    int32_t           flags,
    ftxHT             ftx_handle
    );

void
fs_dir_size_notice(
    struct bfAccess* dirBfap, /* in - dir's access ptr */
    int32_t entrySize,        /* in - entry's size    */
    bs_meta_page_t pgNum,     /* in - dir's page number */
    uint32_t offset           /* in - offset in dir's page */
    );

statusT
setup_for_glom_dir_entries(
    char *dir_buffer,
    fs_dir_entry **start_dir_p,
    uint32_t *entry_size,
    uint32_t *flags,
    struct bfAccess *dir_access
    );

void
glom_dir_entries(
    char *dir_buffer,
    fs_dir_entry *dir_p,
    uint32_t entry_size,
    uint32_t flags,
    rbfPgRefHT page_ref,
    size_t dir_page_size
    );

uint32_t
dir_trunc_start(
    struct vnode *, 
    bs_meta_page_t, 
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
    int32_t flag,                  
    struct fileSetNode *fsnp,      
    struct vnode **vp,             
    struct vnode *dvp,             
    char * file_name               
    );

void
fs_assemble_dir (
    char *dir_buffer,
    bfTagT new_bs_tag,
    char *new_file_name
    );

statusT
fs_create_file(
    struct vattr *vap,           /* in - vnode attributes pointer */
    struct nameidata *ndp,       /* in - nameidata pointer */
    struct vnode     *dir_vp,    /* in - directory vnode pointer */
           char      *name,      /* in - create this name */
           char      *targetname /* in - target name for symlink */
    );

statusT
fs_update_stats(
    struct vnode *vp,       /* in - vnode of bitfile */
    struct bfAccess *bfap,  /* in - bitfile's access structure */
    ftxHT ftx_handle,       /* in - ftx handle. may be nil. */
    uint32_t flags          /* in - flags */
    );

int
fs_trunc_test (
    struct vnode *vp,
    int64_t allocating_beyond_eof
    );

int
advfs_fs_write(
    struct vnode *vp,
    struct uio *uio,
    enum uio_rw,
    int32_t ioflag,
    struct ucred *cred
    );

int
advfs_fs_read(
    struct vnode *vp,
    struct uio *uio,
    enum uio_rw,
    int32_t ioflag,
    struct ucred *cred
    );


opxT fs_insert_undo;

void
fs_insert_undo(
    ftxHT ftxH,
    int32_t opRecSz,
    void* opRec
    );

statusT
fs_create_file_set(
    bfSetT *bfSetp,
    gid_t quotaId,
    ftxHT ftxH
    );

#endif /*FS_DIR_ROUTINES*/
