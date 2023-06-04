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

#ifndef _MS_OSF_
#define _MS_OSF_

/*
 * All following types in bs_public.h except for fsContext which
 * is in fs_dir.h.
 */

#include <sys/stat.h>
#include <advfs/ms_generic_locks.h>
#include <advfs/fs_quota.h>
#include <sys/fs/advfs_ioctl.h>

#ifdef _KERNEL
#include <fs/vfs_ifaces.h>             /* struct nameidata */
#else
/*
 * Forward declaration for nameidata struct
 * - can't include <fs/vfs_ifaces.h> because it cannot be compiled outside
 *   the kernel
 */ 
struct nameidata;
#endif /* ifdef _KERNEL */

extern struct fileSetNode *FilesetHead;   /* head of the fileset list */
extern        ADVRWL_FILESETLIST_T     FilesetLock;


/*
 * Quota information that is specific to each quota type.
 */
typedef struct quotaInfo {
    struct bfAccess  *qiAccessp;   /* quota file access structure pointer */
    struct fsContext *qiContext;   /* quota file context structure pointer */
    bfTagT            qiTag;       /* tag of quota file */
    uint64_t          qiBlkTime;   /* block quota time limit */
    uint64_t          qiFileTime;  /* bitfile quota time limit */
    char              qiFlags;     /* flags, see below */
    size_t            qiPgSz;      /* page size in bytes. */
    bs_meta_page_t    qiFilePgs;   /* pages allocated to quota file */
    struct ucred     *qiCred;      /* credentials */
    /*
     * This lock is protected by the filesetMutex in the fileSetNode.
     * The lock itself serializes adding storage to the quota file.
     */
    mutex_t           qiLock;

} quotaInfoT;

/*
 * Definitions for qiFlags (per quota type).
 */
#define QTF_ENFORCED            0x0001    /* quota is being enforced */
/*
 * Definitions for quotaStatus (per fileset).  All the fields except the
 * first two are used at mount time to initialize the quota system.  
 *
 * NOTE: These are on-disk values.  Changing them causes an on-disk
 *       structure change.  Also, advfs_syscalls.h has corresponding 
 *       defines.
 */

#define QSTS_QUOTA_ON           0x0001    /* some quota type is active */
#define QSTS_USR_QUOTA_MT       0x0002    /* enable maintaining user quotas */
#define QSTS_GRP_QUOTA_MT       0x0004    /* enable maintaining group quotas */
#define QSTS_USR_QUOTA_EN       0x0008    /* enable enforcing user quotas */
#define QSTS_GRP_QUOTA_EN       0x0010    /* enable enforcing group quotas */
#define QSTS_FS_GRP_QUOTA       0x0020    /* fileset quotas from grp quotas */
#define QSTS_FS_QUOTA_MT        0x0040    /* track size of fileset */
#define QSTS_INACTIVE           0x0080    /* Quota's not initialized yet */

/*
 * Default quota state for a newly created fileset.
 */
#define QSTS_DEFAULT ( /* QSTS_QUOTA_ON   |   * The default is "off" */ \
                       QSTS_USR_QUOTA_MT  |  \
                       QSTS_GRP_QUOTA_MT  |  \
                       QSTS_USR_QUOTA_EN  |  \
                       QSTS_GRP_QUOTA_EN  |  \
                       QSTS_FS_GRP_QUOTA  |  \
                       QSTS_FS_QUOTA_MT )


typedef struct fileSetStats {
    uint64_t    advfs_lookup;

    struct {
        uint64_t hit;
        uint64_t hit_not_found;
        uint64_t miss;
    } lookup;

    uint64_t    advfs_create;
    uint64_t    advfs_spare1;    /* used to be msfs_mknod on Tru64 UNIX */
    uint64_t    advfs_open;
    uint64_t    advfs_close;
    uint64_t    advfs_access;
    uint64_t    advfs_getattr;
    uint64_t    advfs_setattr;
    uint64_t    advfs_read;
    uint64_t    advfs_write;
    uint64_t    advfs_mmap;
    uint64_t    advfs_fsync;
    uint64_t    advfs_spare2;    /* used to be msfs_seek on Tru64 UNIX */
    uint64_t    advfs_remove;
    uint64_t    advfs_link;
    uint64_t    advfs_rename;
    uint64_t    advfs_mkdir;
    uint64_t    advfs_rmdir;
    uint64_t    advfs_symlink;
    uint64_t    advfs_readdir;
    uint64_t    advfs_readlink;
    uint64_t    advfs_inactive;
    uint64_t    advfs_spare3;    /* used to be msfs_reclaim on Tru64 UNIX */
    uint64_t    advfs_spare4;    /* used to be msfs_page_read on Tru64 UNIX */
    uint64_t    advfs_spare5;    /* used to be msfs_page_write on Tru64 UNIX */
    uint64_t    advfs_getpage;
    uint64_t    advfs_putpage;
    uint64_t    advfs_bread;
    uint64_t    advfs_brelse;
    uint64_t    advfs_lockctl;
    uint64_t    advfs_setvlocks;
    uint64_t    advfs_spare6;    /* used to be msfs_syncdata on Tru64 UNIX */
} fileSetStatsT;

#define FILESETSTAT( vp, counter )                                 \
    if ((vp)->v_vfsp != NULL && (vp)->v_vfsp->vfs_data != NULL) {					   \
	ADVGETFILESETNODE( (vp)->v_vfsp )->fileSetStats.counter++; \
    }


#define BSD_MODE(vp) (FALSE)            /* Used for S_ISGID bit processing */

/* 
 * fileSetNode hangs off the VFS vfs structure for msfs...
 */
#ifdef _KERNEL
typedef struct fileSetNode {
    ADVSMP_FILESET_T     filesetMutex ;  /* protect next two fields */
    struct fileSetNode  *fsNext;  
    struct fileSetNode **fsPrev;
    bfTagT               rootTag;        /* tag of root directory */
    bfTagT               tagsTag;        /* tag of ".tags */
    uint32_t             filesetMagic;   /* magic number */
    struct domain       *dmnP;
    struct bfAccess     *rootAccessp;    /* Access structure pointer for root */
    bfSetIdT             bfSetId;
    bfSetT              *bfSetp;         /* bitfile-set descriptor pointer */
    struct vnode        *root_vp;
    int32_t              fsFlags;        /* flags, see below */
    struct vfs          *vfsp;           /* will be vfs pointer */
    uint32_t             quotaStatus;    /* see definitions below */
    int64_t              blkHLimit;      /* maximum quota blocks in fileset */
    int64_t              blkSLimit;      /* soft limit for fileset blks */
    int64_t              fileHLimit;     /* maximum number of files in fileset */
    int64_t              fileSLimit;     /* soft limit for fileset files */
    spin_t               spaceUsed_lock; /* protect next two fields */
    int64_t              blksUsed;       /* number of quota blocks used */
    int64_t              filesUsed;      /* number of bitfiles used */
    uint64_t             blkTLimit;      /* time limit for excessive blk use */
    uint64_t             fileTLimit;     /* time limit for excessive file use */
    quotaInfoT           qi[MAXQUOTAS];
    fileSetStatsT        fileSetStats;
    char                *f_mntonname;    /* pointer to directory mounted on */
    char                *f_mntfromname;  /* pointer to mounted filesystem */
} fileSetNodeT;
#endif /* _KERNEL */

/* 
 * Definitions for fsFlags.
 */
#define FS_DQUOT_BLKS           0x0002    /* has been warned about blk limit */
#define FS_DQUOT_FILES          0x0004    /* has been warned about inode limit */
#define FS_QUOTA_RESET          0x0008    /* Usage reset through advfs_set_use() */
#define FS_FORCE_RDONLY         0x0010    /* Do not allow mount -u */

/*
 * Arguments to AdvFS via mount(2).
 */

#define ADVGETROOTACCESS(vfsp) (ADVGETFILESETNODE(vfsp)->rootAccessp)

/*
 * Define some macros to get pointers and things out of structures
 */

/* given a vfs pointer, get the pointer to the fileSetNode */
#define ADVGETFILESETNODE(vfsp) ((struct fileSetNode *)((vfsp)->vfs_data))

/* given a bfSetT pointer, return the vfs struct pointer. */
#define ADVGETVFS(bfSetp)      \
    ( ((bfSetp)->fsnp)       ? \
      ((bfSetp)->fsnp->vfsp) : \
      (NULL)                   \
    )

/* given a vfs pointer, get the domain handle from the fileSetNode */
#define ADVGETDOMAINP(vfsp) (ADVGETFILESETNODE(vfsp)->dmnP)

/* given a vfs pointer, get the bitfile-set handle from the fileSetNode */
#define ADVGETBFSETP(vfsp) (ADVGETFILESETNODE(vfsp)->bfSetp)

/* given a vfs pointer, get the root vnode from the fileSetNode */
#define ADVGETROOTVNODE(vfsp)   (ADVGETFILESETNODE(vfsp)->root_vp)

/* given a bitfile access structure pointer, return the vnode pointer */
#define ATOV(bfap)  ( &(bfap)->bfVnode )     

/* given a vnode pointer return a bfNode pointer */
#define VTOBNP(vp) ((struct bfNode *)((vp)->v_data))

/*
 * Given a vnode pointer return the bfAccessT pointer. 
 * If the vnode associates with a shadow accessp of a metadata file, get the
 * real bfAccessT pointer in the root bfSet. 
 */
#define VTOA(vp) \
     ( (VTOBNP(vp)->accessp && VTOBNP(vp)->accessp->real_bfap) ?  \
       (VTOBNP(vp)->accessp->real_bfap)                        :  \
       (VTOBNP(vp)->accessp)                                      \
     )

/* Given a vnode pointer, return the fsContext pointer. */
#define VTOC(vp) (VTOBNP(vp)->fsContextp)

/* Extract the user id from an fsContext structure. */
#define CPTOUID(cp) ((cp)->dir_stats.st_uid)

/* Extract the group id from an fsContext structure. */
#define CPTOGID(cp) ((cp)->dir_stats.st_gid)

/* Extract the size from an fsContext structure. */
#define CPTOSIZ(cp) ((cp)->dir_stats.st_size)

#ifdef _KERNEL

/* The null fsContext pointer. */
#define NULLCP  ((struct fsContext *)0)

#endif /* #ifdef _KERNEL */


/* given a vnode pointer, get the fileSetNode structure pointer */
#define VTOFSN(vp) (ADVGETFILESETNODE((vp)->v_vfsp))

/* macros to convert a file create typ */

extern enum vtype advfs_typtovt_tab[];
extern int  advfs_vttotyp_tab[];
#define VTTOTYP(indx)              (advfs_vttotyp_tab[(int)(indx)])
#define TYPTOVT(mode)              (advfs_typtovt_tab[((mode) & S_IFMT) >> 12])
#define MAKEMODE(indx, mode) (int) (VTTOTYP(indx) | (mode))

/*
 * advfs_lookup_t - advfs_lookup_int() optimization and behavioral flags
 */
typedef enum {
    ALI_NO_FLAGS    = 0x0000000000000000LL, /* rest of nameidata is good */
    ALI_NO_DNLC     = 0x1                   /* bypass dnlc - do seq_search() */
} advfs_lookup_t;

/* PROTOTYPES */
#ifdef _KERNEL 

int
advfs_lookup_int( 
    advfs_lookup_t    ali_flags,
    struct nameidata *ndp,
    struct vnode     *nm_dvp,
    char             *nm,
    struct ucred     *cred,
    struct vnode     *access_dvp);

struct bfAccess *
osf_fd_to_bfap(
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
fscontext_lock_init(
    struct fsContext* fscp
     );

void
fscontext_lock_destroy(
    struct fsContext* fscp
    );

int advfs_parsespec(const char *fsnamep, 
                    char *buffer,
                    int   buflen,
                    char **dmnName, 
                    char **setName, 
                    int  *isblockdev
                    );

int64_t
bs_get_avail_mcells_nolock(domainT *dmnP, struct fileSetNode *dn);

int
advfs_getext( struct vnode *vp,  advfs_ext_attr_t *attr );

int
advfs_setext( struct vnode *vp, advfs_ext_attr_t *attr );

int
advfs_getext_prealloc( struct vnode *vp, advfs_ext_prealloc_attr_t *prealloc );

int
advfs_setext_prealloc( struct vnode *vp, advfs_ext_prealloc_attr_t *prealloc );

#endif /* #ifdef _KERNEL */

#ifdef _KERNEL
extern  pool_id_t *vnl_h_sl_pool; /* pool of spinlocks protecting vnode */
                                  /* clean/dirty lists                  */
#endif /* _KERNEL */

/*
 * From Tru64 RAD support:  define  current_rad_id()    as 0 
 *                          and     write_const_data()  as bcopy
 *
 * Keep function names intact to help with potential port for similar
 * concepts in HP-UX futures.
 */
#define	current_rad_id()	0
#define	write_const_data(a,b,c)	bcopy((void *)(a), (void *)(b), (size_t)(c))

#endif /* _MS_OSF_ */
