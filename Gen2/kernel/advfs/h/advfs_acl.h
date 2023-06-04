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
 * Facilty:
 *
 *  AdvFS
 *
 * Abstract:
 *
 *  advfs_acl.h
 *  This is the header file for AdvFS 1024 ACLs.
 */

#ifndef _ADVFS_ACLS_H_
#define _ADVFS_ACLS_H_

/*
 * In order for this header file to compile properly, aclv.h must be included
 * before including this file.
 */

/* DEFINES */
#ifdef _KERNEL
#define ADVFS_ACL_TYPE          SYSV_ACLS
#else
#define ADVFS_ACL_TYPE		2
#endif /* _KERNEL */

#define ADVFS_MAX_ACL_ENTRIES   1024    /* Max num of ACL entries per file */
#define ADVFS_NUM_BASE_ENTRIES  4       /* user, group, class, and other */

/* The following defines are based on acl.h defines */
#define ADVFS_ACL_USER          6       /* ACL_USER */
#define ADVFS_ACL_GROUP         3       /* ACL_GROUP */
#define ADVFS_ACL_OTHER         0       /* ACL_OTHER */

/* The following defines are based on aclv.h defines */
#define ADVFS_USER_OBJ          0x01    /* owner of the object */
#define ADVFS_USER              0x02    /* optional users */
#define ADVFS_GROUP_OBJ         0x04    /* group of the object */
#define ADVFS_GROUP             0x08    /* optional groups */
#define ADVFS_CLASS_OBJ         0x10    /* file group class entry */
#define ADVFS_OTHER_OBJ         0x20    /* other entry */
#define ADVFS_ACL_DEFAULT       0x10000 /* default entry */
/* default owner */
#define ADVFS_DEF_USER_OBJ      (ADVFS_ACL_DEFAULT | ADVFS_USER_OBJ)
/* default optional */
#define ADVFS_DEF_USER          (ADVFS_ACL_DEFAULT | ADVFS_USER)
/* default group */
#define ADVFS_DEF_GROUP_OBJ     (ADVFS_ACL_DEFAULT | ADVFS_GROUP_OBJ)
/* default optional */
#define ADVFS_DEF_GROUP         (ADVFS_ACL_DEFAULT | ADVFS_GROUP)
/* default class */
#define ADVFS_DEF_CLASS_OBJ     (ADVFS_ACL_DEFAULT | ADVFS_CLASS_OBJ)
/* default other */
#define ADVFS_DEF_OTHER_OBJ     (ADVFS_ACL_DEFAULT | ADVFS_OTHER_OBJ)

/* Used by advfs_verify_acl and advfs_check_default_acls to track base ACLs */
#define         VERIFY_USER     0x0001
#define         VERIFY_GROUP    0x0002
#define         VERIFY_CLASS    0x0004
#define         VERIFY_OTHER    0x0008
#define         VERIFY_ALL      ( VERIFY_USER | VERIFY_GROUP | \
                                  VERIFY_CLASS | VERIFY_OTHER )

#define ANY_EXEC        (S_IEXEC | S_IEXEC >> 3 | S_IEXEC >> 6)

/* Used by advfs_getacl_bmt as indexes for building a base entries only ACL */
#define		ADVFS_USER_BASE_IDX	0
#define		ADVFS_GROUP_BASE_IDX	1
#define		ADVFS_CLASS_BASE_IDX	2
#define		ADVFS_OTHER_BASE_IDX	3

/* MACROS */

/* Based on setbasemode and getbasemode from acl.h */
#define ADVFS_SETBASEMODE( oldmode, mode, ugo ) \
    ( ( oldmode & ~( 7 << ugo ) ) | ( mode << ugo ) )
#define ADVFS_GETBASEMODE( basemode, ugo ) \
    ( ( basemode >> ugo ) & 7 )

/* Log Structures */
typedef struct acl_update_undo_hdr {
    uint64_t auuh_undo_count;   /* Number of undo records */
    bfSetIdT auuh_bf_set_id;     /* bf set id of the file being undone */
    bfTagT auuh_bf_tag;         /* Tag of file being undone */
} acl_update_undo_hdr_t;

typedef struct acl_update_undo {
    bfMCIdT auu_mcell_id;      /* Mcell ID */
    char auu_previous_record[ sizeof( bsr_acl_rec_t ) ];
                                /* Byte array of changed data */
} acl_update_undo_t;


/* PROTOTYPES */

#ifdef _KERNEL

/* Forward declaration */
struct acl;

int32_t
advfs_setacl_int(
    ftxHT parent_ftx,
    struct vnode *vp,
    int num_tuples,
    struct acl *tupleset,
    int acl_type );

static
statusT
advfs_verify_acl(
    struct vnode *vp,
    struct acl *tupleset,
    int num_tuples );

static
statusT
advfs_acl_resize_mcells(
    ftxHT parent_ftx,
    bfAccessT* bfap,
    int mcells_required,
    int *new_mcells,
    bfMCIdT *acl_mcell_start,
    mcellPtrRecT *freeMC );

static
void
advfs_acl_update_undo(
    ftxHT ftx,
    int32_t undo_rec_size,
    void *undo_record_ptr );

static
statusT
advfs_getacl_bmt(
    advfs_acl_t **advfs_acl,
    int *tuple_cnt,
    bfAccessT *bfap );

static
int32_t
advfs_bf_check_access(
    struct vnode *vp,
    struct ucred *ucred );

statusT
advfs_check_default_acls(
    struct vnode *dvp,
    struct vnode *vp,
    struct acl **acl,
    int *num_tuples,
    int is_dir );

int32_t
advfs_update_base_acls(
    ftxHT parent_ftx,
    bfAccessT *bfap,
    uid_t new_owner,
    uid_t new_group,
    mode_t new_mode );

#endif /* _KERNEL */

#endif /* _ADVFS_ACLS_H_ */
