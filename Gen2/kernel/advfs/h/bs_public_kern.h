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
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 *
 * Facility:
 *
 *  AdvFS
 *
 * Abstract:
 *
 *  Bitfile public types, structure, defines, and function prototypes
 *  limited to kernel usage.
 *
 */

#ifndef BS_PUBLIC_KERN_INCLUDED
#define BS_PUBLIC_KERN_INCLUDED

#include <sys/malloc.h>

#include <sys/user.h>
#include <sys/types.h>
#include <sys/time.h>           /* tmp - using timeval for bs_uid */
#include <sys/dirent.h>
#include <sys/param.h>
#include <sys/syscall.h>        /* To get definition of SYS_NFSSVC */ 
#include <sys/vm_arena_iface.h>

#include <advfs/bs_public.h>
#include <advfs/bs_ods.h>

#ifdef OSDEBUG

void
advfs_crash_recovery_test ( struct domain *dmnP, int crash_event_enum );

#   define ADVFS_CRASH_RECOVERY_TEST(_dmnP, _event_enum) \
        advfs_crash_recovery_test (_dmnP, _event_enum);
#else
#   define ADVFS_CRASH_RECOVERY_TEST(_dmnP, _event_enum)
#endif

/*
 * Magic numbers for validation of in-memory structures.
 * These should all begin with 0xADF and should not use
 * the next-highest bit as that is reserved for MAGIC_DEALLOC,
 * which is set when a structure is deallocated.
 */
#define MAGIC_DEALLOC    0x00080000
#define ACCMAGIC         0xADF00001    /* bfAccessT */
#define SETMAGIC         0xADF00002    /* bfSetT */
#define DMNMAGIC         0xADF00003    /* domainT */
#define VDMAGIC          0xADF00004    /* vdT */
#define FSMAGIC          0xADF00005    /* fileSetNodeT */
#define BSBUFMAGIC       0xADF00006    /* bsBuf */
#define ADVIODESCMAGIC   0xADF00007    /* adviodesc_t */
#define IOANCHORMAGIC    0xADF00008    /* ioanchor_t */
#define MULTIWAITMAGIC   0xADF00009    /* multiWaitT */
#define ACTRANGE_MAGIC   0xADF0000a    /* actRangeT */
#define ACCMAGIC_MARKER  0xADF0000b    /* bfAccessT list marker */
#define ALLOC_MAGIC      0xADF000FF    /* ms_malloc'd memory */


/* prototypes for using VD_HTOP macro and other vdi functions. */
struct domain;
struct vd *vd_htop_if_valid( vdIndexT vdi, 
                             struct domain *dmnp, 
                             int bump_refcnt,
                             int zombie_ok );
struct vd *vd_htop_already_valid( vdIndexT vdi, 
                                  struct domain *dmnp, 
                                  int bump_refcnt );
void vd_dec_refcnt( struct vd *vdp );

/* This definition allows us to use a lot of the existing VD_HTOP()
 * macros.  If the vdi to vdp conversion fails, this routine will
 * panic so long as ADVFS ASSERTs are enabled.
 */
#define VD_HTOP(vdi, dmnp)                       \
    vd_htop_already_valid( (vdi), (dmnp), FALSE )

/*
 * Checks whether the thread is an NFS server thread.
 */
#define NFS_SERVER (u.u_syscall == SYS_NFSSVC)

/*
 * Externs
 */
extern bfPageRefHT NilBfPageRefH;
extern bfTagT NilBfTag;
extern adv_ondisk_version_t BFD_ODS_LAST_VERSION;
extern adv_ondisk_version_t BFD_ODS_NULL_VERSION;
extern bfDomainIdT nilBfDomainId;
extern bfFsCookieT nilFsCookie;
extern bfSetIdT nilBfSetId;
extern logRecAddrT logEndOfRecords;
extern logRecAddrT logNilRecord;
extern serviceClassT nilServiceClass;
extern serviceClassT defServiceClass;
extern bsVdParamsT bsNilVdParams;
extern bfParamsT bsNilBfParams;
extern bfSetParamsT bsNilBfSetParams;
extern vdIndexT bsNilVdIndex;
extern bfMCIdT bsNilMCId;
extern bsDmnAttrT bsNilDmnAttr;
extern bfDmnParamsT bsNilDmnParams;

/*
 * advfs_aligned_arena extern for large variable sized "aligned" memory
 * allocation arena.  The primary consumer is advfs_dio_unaligned_xfer() but
 * advfs_aligned_arena can be used by any AdvFS code that needs large (1K or
 * larger) special case aligned memory buffers.  However, most variable
 * sized allocations should use the ms_malloc()/ms_free() family and not
 * advfs_aligned_arena.
 */
#ifdef _KERNEL
extern kmem_handle_t advfs_aligned_arena;
#endif /* _KERNEL */


/*
 * PROTOTYPES
 */

/*****************************/
/****   Bitfile services  ****/
/*****************************/

#include <advfs/bs_params.h>

struct bfAccess;

statusT
bs_stat_n_inherit(
    struct bfAccess *parentbfap,
    struct bfAccess *childbfap,
    bfInheritT inherit,
    ftxHT ftxH
    );

statusT
bs_domain_access(
    struct domain **dmnPP,
    bfDomainIdT bfDomainId, /* in */
    int deactivated_ok      /* in */
    );

void
bs_domain_close(
    struct domain *dmnP
    );

statusT
set_vd_time(
    struct domain* dmnP    /* IN - domain struct */
    );


/* define values for access_int "options" arg */

enum acc_open_flags {
    BF_OP_NO_FLAGS      = 0x0,    /* No Flags */
    BF_OP_IGNORE_DEL    = 0x1,      
    BF_OP_OVERRIDE_SMAX = 0x2,    /* override acc_ctrl_soft_max to ref bfap */
    BF_OP_BFA_LOCK_HELD = 0x4,    /* bfaLock held on entry to advfs_ref_bfap */
    BF_OP_INMEM_ONLY    = 0x8,    /* Don't init a new bfap, doesn't bump v_count */
    BF_OP_FIND_ON_DDL   = 0x10,   /* Find it on DDL */
    BF_OP_INTERNAL      = 0x20,   /* Doesn't set v_count */
    BF_OP_IGNORE_CLOSED_LIST = 0x40,    /* If the bfap is on the free or closed
                                           list, do not remove it.  This is required 
                                           to prevent sync code from interfering with 
                                           cache aging */
    BF_OP_IGNORE_BFS_DELETING = 0x80, /* If set, bs_access_one will allow a file
                                       * to be opened on a fileset that is
                                       * BFS_DELETING */
    BF_OP_SNAP_REF            = 0x100,/* If set, when refCnt is bumped, 
                                       * bfaRefsFromChildSnaps will also be
                                       * bumped */
    BF_OP_IGNORE_OUT_OF_SYNC  = 0x200 /* If set, ignore the out of sync tag flag
                                       */
};

statusT
bs_access(
    struct bfAccess **outbfap,   /* out - access structure pointer */
    bfTagT tag,                  /* in - tag of bf to access */
    bfSetT *bfSetp,              /* in - BF-set descriptor pointer */
    ftxHT ftxH,                  /* in - ftx handle */
    enum acc_open_flags options, /* in - options flags */
    struct vnode **vp);          /* out - vnode pointer */


/*
 * bs_close options
 */
enum acc_close_flags {
    MSFS_CLOSE_NONE     = 0x0,      /* No Flags */
    MSFS_INACTIVE_CALL  = 0x1,      /* Called from advfs_inactive */ 
    MSFS_BFSET_DEL      = 0x2,      /* */
    MSFS_SS_NOCALL      = 0x4,      /* */
    MSFS_CLOSE_DEALLOC  = 0x8,      /* Dealloc bfap on DEC_REFCNT */
    MSFS_VALID          = 0x10,     /* Set state back to active */
    MSFS_INVALID        = 0x20,     /* set state back to invalid */
    MSFS_SNAP_DEREF     = 0x40,     /* While holding the bfaLock, and 
                                     * before calling DEC_REFCNT, the
                                     * bfaRefsFromChildSnaps must be
                                     * decremented */
    MSFS_SNAP_PARENT_CLOSE = 0x80   /* While holding the bfaLock, and before
                                     * calling DEC_REFCNT, the bfaFlags
                                     * bFA_OPENED_BY_PARENT flag must be
                                     * cleared */
};

statusT
bs_close(
    struct bfAccess *bfAccessp,  /* in */
    enum acc_close_flags options /* in */
    );

statusT
bs_migrate (
    struct bfAccess *bfap,               /* in */
    vdIndexT srcVdIndex,                 /* in */
    bf_fob_t src_fob_offset,             /* in */
    bf_fob_t src_fob_cnt,                /* in */
    vdIndexT dstVdIndex,                 /* in */
    bf_vd_blk_t dstBlkOffset,            /* in */
    uint32_t forceFlag,                  /* in */
    bsAllocHintT alloc_hint              /* in */
    );

struct extentmap;
int
advfs_get_extent_map(
    struct bfAccess *bfap,     /* in */
    struct extentmap *map      /* in/out */
    );

typedef enum {
    BS_NIL,             /* default behavior */
    BS_SEQ_AHEAD,       /* sequential access probable */
    BS_OVERWRITE        /* page will be overwritten */
} bfPageRefHintT;

typedef enum {
    MF_NO_FLAGS         = 0x0,   /* No flags */
    MF_NO_VERIFY        = 0x1,   /* Do not verify data integrity */
    MF_VERIFY_PAGE      = 0x2,   /* MF_NO_VERIFY is sufficient to make
                                  * a VERIFY/NO VERIFY decision, but this
                                  * provides clarity */
    MF_OVERWRITE        = 0x4    /* Metadata page will be fully overwritten
                                  * by bs_pinpg() caller, so skip IO that
                                  * primes new cache pages from disk in
                                  * advfs_getpage().
                                  */
} meta_flags_t;


statusT
bs_pinpg(
    bfPageRefHT *bfPageRefH,       /* out */
    void **bfPageAddr,             /* out */
    struct bfAccess *bfap,         /* in */
    bs_meta_page_t page,           /* in */
    ftxHT ftxH,                    /* in */
    meta_flags_t mflags            /* flags */
    );

typedef enum {
    BS_CACHE_IT,        /* normal cache value to page */
    BS_RECYCLE_IT       /* unlikely to revisit soon */
} bfPageCacheHintT;

typedef enum {
    BS_NOMOD,           /* page not modified */
    BS_LOG_PAGE,        /* one of a synchronous group */
    BS_MOD_LAZY,        /* modified, deferred write is okay */
    BS_MOD_SYNC         /* modified, write synchronously */
} bfPgRlsModeT;

typedef struct {
    bfPageCacheHintT cacheHint;    /* cache hint */
    bfPgRlsModeT rlsMode;          /* page release mode */
} bsUnpinModeT;

extern bsUnpinModeT BS_DIRTY;
extern bsUnpinModeT BS_WRITETHRU;
extern bsUnpinModeT BS_CLEAN;
extern bsUnpinModeT BS_LOG;

statusT
bs_unpinpg(
    bfPageRefHT bfPageRefH,      /* in */
    logRecAddrT wrtAhdLogAddr,   /* in */
    bsUnpinModeT modeNcache      /* in */
    );

statusT
bs_refpg(
    bfPageRefHT *bfPageRefH,       /* out */
    void **bfPageAddr,             /* out */
    struct bfAccess *bfap,         /* in */
    bs_meta_page_t page,           /* in */
    ftxHT ftxH,                    /* in */
    meta_flags_t mflags            /* flags */
    );

statusT
bs_derefpg(
    bfPageRefHT bfPageRefH,      /* in */
    bfPageCacheHintT cacheHint   /* in */
    );

/******************************************************/
/****  Virtual disk and Bitfile Domain management  ****/
/******************************************************/

statusT
bs_disk_init(
    char *diskName,             /* in - disk device name */
    bfDmnParamsT* bfDmnParams,  /* in - domain parameters */
    bsVdParamsT *bsVdParams,    /* in/out - vd parameters */
    serviceClassT tagSvc,       /* in - tag service class */
    serviceClassT logSvc,       /* in - log service class */
    bf_fob_t bmtPreallocFobs,   /* in - num 1k fobs to preallocate for BMT */
    adv_ondisk_version_t dmnVersion   /* in - on-disk version number */
    );

statusT
bs_vd_remove_active (
    struct domain *dmnP,
    vdIndexT vdIndex,    /* in */
    uint32_t forceFlag   /* in */
    );

statusT
bs_vd_add_active(
    char *domain,               /* in - path to domain table */
    char *vdName,               /* in - block special device name */
    serviceClassT *vdSvc,       /* in/out - service class */
    bf_vd_blk_t vdSize,         /* in - size of the virtual disk in DEV_BSIZE units */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number fobs to preallocate for BMT */
    uint32_t flag,               /* in - M_MSFS_MOUNT may be set */
    bfDomainIdT *dmnId,         /* out - domain Id */
    vdIndexT *volIndex           /* out - volume index */
    );

int
vd_extend(
    struct vd   *vdp,           /* in */
    uint64_t     size,          /* in */
    bf_vd_blk_t *oldBlkSize,    /* out */
    bf_vd_blk_t *newBlkSize,    /* out */
    ftxIdT xid                  /* in */
    );

statusT
bs_dmn_init(
    char *domain,               /* in - domain name */
    int maxVds,                 /* in - maximum number of virtual disks */
    bf_fob_t logFobs,           /* in - number of pages in log */
    bf_fob_t userAllocFobs,     /* in - number of fobs per user data page */
    serviceClassT logSvc,       /* in - log service attributes */
    serviceClassT tagSvc,       /* in - tag directory service attributes */
    char *vdName,               /* in - block special device name */
    serviceClassT vdSvc,        /* in - service class */
    bf_vd_blk_t vdSize,         /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of 1k fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs to preallocate for BMT */
    adv_ondisk_version_t domainVersion,/* in - on-disk version for domain */
    bfDomainIdT *domainId       /* out - unique domain id */
    );

statusT
bs_bfdmn_id_activate(
    bfDomainIdT bfDomainId /* in - domain id */
    );

statusT
bs_bfdmn_tbl_activate(
    char* bfDmnName,      /* in - bf domain name */
    uint64_t flag,        /* in - flag */
    bfDomainIdT* domainId /* out - domain id */
    );

statusT
bs_bfdmn_activate(
    bfDomainIdT domainId,
    uint64_t flag
    );

statusT
bs_bfdmn_deactivate(
    bfDomainIdT domainId,
    uint64_t flag
    );

        
/* SC_SUBSET( sc1, sc2 ) - returns true if sc1 is a proper subset of sc2 */
#define SC_SUBSET( sc1, sc2 ) (((sc1) & (sc2)) == (sc1))

/* SC_EQL( sc1, sc2 ) - returns true if sc1 and sc2 are equal */
#define SC_EQL( sc1, sc2 ) ((sc1) == (sc2))

/* SC_LT( sc1, sc2 ) - returns true if sc1 is less than sc2 */
#define SC_LT( sc1, sc2 ) ((sc1) < (sc2))

/* SC_GT( sc1, sc2 ) - returns true if sc1 is greater than sc2 */
#define SC_GT( sc1, sc2 ) ((sc1) > (sc2))


extern char *advfs_sad_format0;
extern char *advfs_sad_format1;
extern char *advfs_sad_format2;
extern char *advfs_sad_format3;

extern char *advfs_sad_buf1;
extern char *advfs_sad_buf2;

#define ADVFS_SAD( fmt, msg )         \
    advfs_sad( __FILE__,              \
               __LINE__,              \
               fmt,                   \
               msg,                   \
               (int64_t)0,            \
               (int64_t)0,            \
               (int64_t)0 );

#define ADVFS_SAD0( msg )             \
    advfs_sad( __FILE__,              \
               __LINE__,              \
               advfs_sad_format0,     \
               msg,                   \
               (int64_t)0,            \
               (int64_t)0,            \
               (int64_t)0 );

#define ADVFS_SAD1( msg, n1 )         \
    advfs_sad( __FILE__,              \
               __LINE__,              \
               advfs_sad_format1,     \
               msg,                   \
               (int64_t)(n1),         \
               (int64_t)0,            \
               (int64_t)0 );

#define ADVFS_SAD2( msg, n1, n2 )     \
    advfs_sad( __FILE__,              \
               __LINE__,              \
               advfs_sad_format2,     \
               msg,                   \
               (int64_t)(n1),         \
               (int64_t)(n2),         \
               (int64_t)0 );

#define ADVFS_SAD3( msg, n1, n2, n3 ) \
    advfs_sad( __FILE__,              \
               __LINE__,              \
               advfs_sad_format3,     \
               msg,                   \
               (int64_t)(n1),         \
               (int64_t)(n2),         \
               (int64_t)(n3) );

void
advfs_sad(char    *module,
          int      line,
          char    *fmt,
          char    *msg,
          int64_t  n1,
          int64_t  n2,
          int64_t  n3);

#define DMN_PANIC_IO_CONNECTIVITY       0x01
#define DMN_PANIC_IO_OTHER              0x02
#define DMN_PANIC_GENERIC               0x04
#define DMN_PANIC_UNMOUNT               0x08

#define DMN_PANIC_MAX_MSG_LEN           128 /* Max length of domain panic message string */
 
void
_domain_panic(
    struct domain *dmnP,
    char *msg,
    int flags
    );
void
domain_panic(
    struct domain *dmnP,
    char *format,
    ...
    );

#define domain_panic_type(dmnP, msg, flags) \
    _domain_panic(dmnP, msg, flags)


#ifdef OSDEBUG
#   define dbg_printf(args) printf((args));
#else
#   define dbg_printf(args)
#endif

void
ms_printf(
#ifndef _KERNEL
    char *msg, ...
#endif
     );

void
ms_uprintf(
#ifndef _KERNEL
    char *msg, ...
#endif
    );

void
ms_uaprintf(
#ifndef _KERNEL
    char *msg, ...
#endif
    );

/*
 * public bitfile set routines
 */

statusT
bs_bfs_get_info(
    uint64_t *nextSetIdx,      /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    struct domain *dmnP,
    uint32_t *userId            /* out - bfset user id */
    );

void
bs_bfs_get_clone_info(
    bfSetT *bfSetp,       /* in - bitfile-set descriptor pointer */
    uint32_t *cloneId,     /* out - bitfile set clone id */
    uint32_t *cloneCnt     /* out - bitfile set clone count */
    );

statusT
bs_bfset_activate(
    char *bfDmnTbl,     /* in - bitfile-set's domain table file name */
    char *bfSetName,    /* in - bitfile-set name */
    uint64_t flag,      /* flag - used for forced mount */
    bfSetIdT *bfSetId   /* out - bitfile-set's ID */
    );

statusT
bs_bfset_deactivate(
    bfSetIdT bfSetId,   /* in - bitfile-set's ID */
    uint64_t flag         /* in - advfs mount flags */
    );

/*
 * misc
 */

char*
toke_it(
    char* str,      /* in - string to scan */
    char  tokchar,  /* in - token end character */
    char* token     /* out - token found */
    );

/*
 * The following are to be used for the 'mode' parameter of bs_accessible().
 */

#define BS_ACC_READ     0400UL
#define BS_ACC_WRITE    0200UL
#define BS_ACC_EXEC     0100UL

int
bs_accessible(
    adv_mode_t mode,        /* in - mode wanted */
    adv_mode_t omode,       /* in - object's permissions mode */
    adv_uid_t ouid,         /* in - object's uid */
    adv_gid_t ogid          /* in - object's gid */
    );

int
bs_owner(
    adv_uid_t ouid  /* in - object's user id */
    );

statusT
bs_get_dmntbl_params(
    char *dmnTbl,             /* in - domain table file name */
    bfDmnParamsT *dmnParams
    );

#endif /* BS_PUBLIC_KERN_INCLUDED */

