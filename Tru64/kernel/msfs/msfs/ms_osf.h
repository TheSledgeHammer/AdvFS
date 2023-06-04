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
 * @(#)$RCSfile: ms_osf.h,v $ $Revision: 1.1.10.13 $ (DEC) $Date: 1999/02/04 20:51:01 $
 */

#ifndef _MS_OSF_
#define _MS_OSF_

/*
 * All following types in bs_public.h except for fsContext which
 * is in fs_dir.h.
 */

#include <sys/mode.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/fs_quota.h>

/*
 * bfNode is the msfs structure at the end of a vnode
 */

struct bfAccess;

typedef struct bfNode {
        struct bfAccess *accessp;
        struct fsContext *fsContextp;
        bfTagT tag;
        bfSetIdT bfSetId;
} bfNodeT;

/*
 * Quota information that is specific to each quota type.
 */
typedef struct quotaInfo {
    bfAccessT *qiAccessp;         /* quota file access structure pointer */
    struct fsContext *qiContext;  /* quota file context structure pointer */
    bfTagT qiTag;                 /* tag of quota file */
    time_t qiBlkTime;             /* block quota time limit */
    time_t qiFileTime;            /* bitfile quota time limit */
    char qiFlags;                 /* flags, see below */
    uint32T qiPgSz;               /* page size */
    uint32T qiFilePgs;            /* pages allocated to quota file */
    struct ucred *qiCred;         /* credentials */
    /*
     * This lock is protected by the filesetMutex in the fileSetNode.
     * The lock itself serializes adding storage to the quota file.
     */
    lock_data_t qiLock;
} quotaInfoT;

typedef struct fileSetStats {
    unsigned long    msfs_lookup;

    struct {
        unsigned long hit;
        unsigned long hit_not_found;
        unsigned long miss;
    } lookup;

    unsigned long    msfs_create;
    unsigned long    msfs_mknod;
    unsigned long    msfs_open;
    unsigned long    msfs_close;
    unsigned long    msfs_access;
    unsigned long    msfs_getattr;
    unsigned long    msfs_setattr;
    unsigned long    msfs_read;
    unsigned long    msfs_write;
    unsigned long    msfs_mmap;
    unsigned long    msfs_fsync;
    unsigned long    msfs_seek;
    unsigned long    msfs_remove;
    unsigned long    msfs_link;
    unsigned long    msfs_rename;
    unsigned long    msfs_mkdir;
    unsigned long    msfs_rmdir;
    unsigned long    msfs_symlink;
    unsigned long    msfs_readdir;
    unsigned long    msfs_readlink;
    unsigned long    msfs_inactive;
    unsigned long    msfs_reclaim;
    unsigned long    msfs_page_read;
    unsigned long    msfs_page_write;
    unsigned long    msfs_getpage;
    unsigned long    msfs_putpage;
    unsigned long    msfs_bread;
    unsigned long    msfs_brelse;
    unsigned long    msfs_lockctl;
    unsigned long    msfs_setvlocks;
    unsigned long    msfs_syncdata;
} fileSetStatsT;

#define FILESETSTAT( vp, counter ) \
    if ((vp)->v_mount != DEADMOUNT) { \
        GETFILESETNODE( VTOMOUNT( vp ) )->fileSetStats.counter++; \
    }

/* 
 * fileSetNode hangs off the VFS mount dstructure for msfs...
 */
typedef struct fileSetNode {
    struct fileSetNode *fsNext;  
    struct fileSetNode **fsPrev;
    bfTagT rootTag;                 /* tag of root directory */
    bfTagT tagsTag;                 /* tag of ".tags */
    uint_t filesetMagic;            /* magic number: structure validation */
    domainT *dmnP;
    bfAccessT *rootAccessp;         /* Access structure pointer for root */
    bfSetIdT bfSetId;
    bfSetT *bfSetp;                 /* bitfile-set descriptor pointer */
    struct vnode *root_vp;
    int fsFlags;                    /* flags, see below */
    struct mount *mountp;           /* mount table pointer */
    unsigned quotaStatus;           /* see definitions below */
    long blkHLimit;                 /* maximum quota blocks in fileset */
    long blkSLimit;                 /* soft limit for fileset blks */
    long fileHLimit;                /* maximum number of files in fileset */
    long fileSLimit;                /* soft limit for fileset files */
    long blksUsed;                  /* number of quota blocks used */
    long filesUsed;                 /* number of bitfiles used */
    time_t blkTLimit;               /* time limit for excessive disk blk use */
    time_t fileTLimit;              /* time limit for excessive file use */
    mutexT filesetMutex;            /* protect next two fields */
    quotaInfoT qi[2];
    fileSetStatsT fileSetStats;
} fileSetNodeT;
/* 
 * Definitions for fsFlags.
 */
#define FS_CLONEFSET    0x1         /* This is a clone fileset */
#define FS_DQUOT_BLKS   0x2         /* has been warned about blk limit */
#define FS_DQUOT_FILES  0x4         /* has been warned about inode limit */
#define FS_QUOTA_RESET  0x8         /* Usage reset through advfs_set_use() */
#define FS_FORCE_RDONLY 0x10        /* Do not allow mount -u */

/*
 * Definitions for qiFlags (per quota type).
 */
#define QTF_ENFORCED            0x1       /* quota is being enforced */
/*
 * Definitions for quotaStatus (per fileset).  All the fields except the
 * first two are used at mount time to initialize the quota system.  
 *
 * NOTE: These are on-disk values.  Changing them causes an on-disk
 *       structure change.  Also, msfs_syscalls.h has corresponding 
 *       defines.
 */

#define QSTS_QUOTA_ON           0x0001    /* some quota type is active */
#define QSTS_QUOTA_SYNC         0x0002    /* set when quota sync in progress */
#define QSTS_USR_QUOTA_MT       0x0004    /* enable maintaining user quotas */
#define QSTS_GRP_QUOTA_MT       0x0008    /* enable maintaining group quotas */
#define QSTS_USR_QUOTA_EN       0x0010    /* enable enforcing user quotas */
#define QSTS_GRP_QUOTA_EN       0x0020    /* enable enforcing group quotas */
#define QSTS_OBSOLETE_0040      0x0040    /* was QSTS_FS_USR_QUOTA */
#define QSTS_FS_GRP_QUOTA       0x0080    /* fileset quotas from grp quotas */
#define QSTS_FS_QUOTA_MT        0x0100    /* track size of fileset */
#define QSTS_OBSOLETE_0200      0x0200    /* was QSTS_FS_QUOTA_EN */
#define QSTS_LARGE_LIMITS       0x0400    /* set for 8 byte quota fields */

/*
 * Default quota state for a newly created fileset.
 */
#define QSTS_DEFAULT (QSTS_QUOTA_ON | QSTS_USR_QUOTA_MT | \
    QSTS_GRP_QUOTA_MT | QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN | \
    QSTS_FS_GRP_QUOTA | QSTS_FS_QUOTA_MT )

extern struct fileSetNode *FilesetHead;   /* head of the fileset list */

extern lock_data_t FilesetLock;
#define FILESET_WRITE_LOCK( fsl ) \
    lock_write( fsl );
#define FILESET_READ_LOCK( fsl ) \
    lock_read( fsl );
#define FILESET_UNLOCK( fsl ) \
    lock_done( fsl );

/*
 * Define some macros to get pointers and things out of structures
 */

/* given a mount structure pointer, get the domain handle from the
   fileSetNode */

#define GETDOMAINP(mp)     (((struct fileSetNode *)((mp)->m_info))->dmnP)


/* given a mount structure pointer, get the bitfile-set handle from the
   fileSetNode */

#define GETBFSETP(mp)     (((struct fileSetNode *)((mp)->m_info))->bfSetp)

/* given a mount structure pionter, get the bitfile-set id from the
   fileSetNode */

#define GETBFSET(mp)      (((struct fileSetNode *)((mp)->m_info))->bfSetId)

/* given a mount structure pointer, get the root tag from the
   fileSetNode */

#define GETROOT(mp)      (((struct fileSetNode *)((mp)->m_info))->rootTag)

/* given a mount structure pointer, get the pointer to the fileSetNode */

#define GETFILESETNODE(mp)  ((struct fileSetNode *)(mp->m_info))

/* given a mount structure pointer, get the root access handle from the
   fileSetNode */

#define GETROOTACCESS(mp) (((struct fileSetNode *)((mp)->m_info))->rootAccessp)

/* given a mount structure pointer, get the root vnode from the
   fileSetNode */

#define GETROOTVNODE(mp)      (((struct fileSetNode *)((mp)->m_info))->root_vp)

/* given a bitfile access structure pointer, return the vnode pointer */
#define ATOV(bfap)  ( bfap->bfVp )     

/*
 * Given a vnode pointer return the bfAccessT pointer. 
 * If the vnode associates with a shadow accessp of a metadata file, get the
 * real bfAccessT pointer in the root bfSet. 
 */
#define VTOA(vp) \
 ((((struct bfNode *)(&(vp)->v_data[0]))->accessp \
     && (((struct bfNode *)(&(vp)->v_data[0]))->accessp->real_bfap))? \
     (((struct bfNode *)(&(vp)->v_data[0]))->accessp->real_bfap): \
     (((struct bfNode *)(&(vp)->v_data[0]))->accessp))

/* Given a vnode pointer, return the fsContext pointer. */
#define VTOC(vp) (((struct bfNode *)(&(vp)->v_data[0]))->fsContextp)

/* Extract the user id from an fsContext structure. */
#define CPTOUID(cp) (cp)->dir_stats.st_uid

/* Extract the group id from an fsContext structure. */
#define CPTOGID(cp) (cp)->dir_stats.st_gid

/* Extract the size from an fsContext structure. */
#define CPTOSIZ(cp) (cp)->dir_stats.st_size

/* The null fsContext pointer. */
#define NULLCP  ((struct fsContext *)0)

/* given a vnode pointer get the mount structure pointer */

#define VTOMOUNT(vp) ((vp)->v_mount)

/* macros to convert a file create typ */

extern enum vtype typtovt_tab[];
extern int vttotyp_tab[];
#define VTTOTYP(indx) (vttotyp_tab[(int)(indx)])
#define TYPTOVT(mode)   (typtovt_tab[((mode) & S_IFMT) >> 12])
#define MAKEMODE(indx, mode) (int) (VTTOTYP(indx) | (mode))

/* PROTOTYPES */

struct bfAccess *osf_fd_to_bfap(
                               int fileDesc, 
                               struct file **fp
                               );

struct fsContext*
vnode_fscontext_allocate(
                         struct vnode* vp
                         );

void
vnode_fscontext_deallocate(
                           struct vnode* vp
                           );

void
fscontext_lock_init( struct fsContext* fscp );

void
fscontext_lock_destroy( struct fsContext* fscp );

#endif /* _MS_OSF_ */
