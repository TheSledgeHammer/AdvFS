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
 * Facility:
 *
 *    AdvFS 
 *
 * Abstract:
 *
 *    This header file contains structure definitions and function
 *    prototypes for in-memory structures.
 *
 */

#ifndef _BS_IMS_H_
#define _BS_IMS_H_

#include <advfs/bs_ods.h>

#include <advfs/fs_dir_kern.h>             /* for fsContext def - bad layering? */
#include <sys/vnode.h>
#include <sys/buf.h>
#include <advfs/ms_generic_locks.h>

/*
 * Block map structures
 */

typedef struct blkDesc {
    vdIndexT vdIndex;
    bf_vd_blk_t vdBlk;
} blkDescT;

#include <advfs/bs_buf.h>
#include <advfs/bs_service_classes.h>
#include <advfs/bs_domain.h>

/*
 * In-memory extent map data structures.
 */

/* 
 * Used when creating a subextent. Indicates the state of the
 * corresponing Mcell. 
 *
 *  INITIALIZED - Use old mcell as is only update it on-disk xtnt rec.
 *  NEW_MCELL - Brand new Mcell, create on-disk xtnt rec and link mcell
 *  USED_MCELL - Use old mcell, update on-disk xtnt rec and relink mcell
 */

typedef enum
{
    ADV_MCS_INITIALIZED,
    ADV_MCS_NEW_MCELL,
    ADV_MCS_USED_MCELL
} mcellStateT;

/*
 * bsInMemSubXtntMapT - A partial extent map that describes some or all of the
 * storage described by an on-disk extent record.  None or all of the record's
 * extent descriptors are cached in the sub extent map's extent array.  One or
 * more sub extent maps describe a bitfile's storage.
 *
 * The range described by a sub extent map includes mapped and unmapped
 * storage.  The mapped fobs are described by the extent array.  The unmapped
 * fobs are those that are between one sub extent map's last mapped fob and the
 * next sub extent map's first mapped fob.
 */

typedef struct bsInMemSubXtntMap {
    
    bf_fob_t bssxmFobOffset;      /* The first 1k offset described by the map */
    bf_fob_t bssxmFobCnt;  /* The number of 1k file blks described by the map */
    bfMCIdT mcellId;        /* Mcell where extent record is located */
    mcellStateT mcellState; /* sub extent map is new */
    uint32_t type;           /* On-disk record type */
    uint32_t onDiskMaxCnt;   /* max number of entries in on-disk extent record */
    uint32_t updateStart;    /* Index first entry to save in on-disk xtnt record*/
    uint32_t updateEnd;      /* Index last entry to save in on-disk xtnt record */
    uint32_t cnt;            /* The number of valid entries in bsXA */
    uint32_t maxCnt;         /* The number of entries in bsXA */
    bsXtntT *bsXA;          /* Array of extent descriptors */
} bsInMemSubXtntMapT;

/* 
 * bsInMemXtntMapT - Represents cached on-disk extent information.
 */

struct bsInMemXtntMap {
    domainT *domain;         /* pointer to the domain */
    uint32_t hdrType;         /* Type of on-disk header record */
                             /* The header record contains the mcell count */
                             /* and a pointer to the other mcells in the list */
    bfMCIdT hdrMcellId;      /* Pointer to header mcell */

    bf_fob_t bsxmNextFob; /* 1 greater than the last valid 1k unit in */
                                        /* the file.  fob = file_offset_blk         */
                                          
    vdIndexT allocVdIndex;   /* Disk on which the next storage is allocated. */
    uint32_t origStart;       /* Index of the original entry that maps the */
                             /* first fob that is added or removed */
    uint32_t origEnd;         /* Index of the original entry that maps the */
                             /* last fob that is added or removed */
    uint32_t updateStart;     /* Index of first entry that contains new mapping*/
                             /* information. */
    uint32_t updateEnd;       /* Index of last entry that contains new mapping */
                             /* information. */
    uint32_t validCnt;        /* Number of in-use and valid sub extent maps */
    uint32_t cnt;             /* Number of in-use sub extent maps */
    uint32_t maxCnt;          /* Number of sub extent maps */
    bsInMemSubXtntMapT *subXtntMap;   /* Array of in-memory sub extent maps */
};

typedef struct bsInMemXtntMap bsInMemXtntMapT;

/* This enumeration is used to define the state of an extent map.  If
 * XVT_VALID, then the extents are valid.  If XVT_INVALID, then the maps 
 * are invalid. */
    
typedef enum {
    XVT_VALID,
    XVT_INVALID
} xtnt_valid_type_t;

/*
 * bsInMemXtntT - The in-memory extent map header.
 *
 */

typedef struct bsInMemXtnt {
    xtnt_valid_type_t validFlag;   /* XVT_VALID if extents are valid.    *
                                    * XVT_INVALID if extents are invalid */
    bsInMemXtntMapT *xtntMap;      /* Link to primary extent map */
    bsInMemXtntMapT *copyXtntMap;  /* Link to copy extent maps */
    bfMCIdT          copyMcellId;  /* Used by migrate to pass off 
                                    * the switching of storage. */
    ADVRWL_MIGSTG_T migStgLk;      /* Serialize migrate and add/remove of stg */

    bf_fob_t bimxAllocFobCnt;    /* Number of allocated 1k file offset blks */
} bsInMemXtntT;


/*************************advfs_metaflush_thread related ******************/

#define ADVFS_METAFLUSH_QUEUE_SIZE 100

/* 
 * Defines the type of message being sent.  The thread go away will need to
 * be sent n times where n is the number of cpus (one thread per CPU).  Each
 * thread should only pick up one instance of this message before destroying
 * itself 
 */
typedef enum {
    AMFE_META_FLUSH,            /* Cause a meta file to flush in a range */
    AMFE_THREAD_GO_AWAY         /* Kill the first thread to get message */
} advfs_metaflush_msg_type_t;

/*
 * This structure represents a message to the advfs_metaflush_thread 
 * The amf_data is a union to simplify future expansion.
 */
struct advfs_metaflush_thread_msg {
    advfs_metaflush_msg_type_t amf_msg_type;    /* Message type */
    union {
        struct { 
            bfTagT amf_meta_tag;    /* Tag of file to be flushed 
                                     * Cannot use bfAccess because we hold no
                                     * locks and file may be deleted */
            bfSetIdT amf_bf_set_id; /* set id of bfSet for meta file.  Cannot pass
                                     * pointer since bfSetT may be deleted or
                                     * inactive */
            union {
                off_t amf_offset;    /* Byte offset to start flush at. */
                lsnT amf_lsn;        /* Log page LSN to flush */
            }amf_start;
            size_t amf_size;        /* Byte count of flush. */

        } amf_meta_flush;
    } amf_data;
};

typedef struct advfs_metaflush_thread_msg advfs_metaflush_thread_msg_t;

/******************End of advfs_metaflush_thread related*****************/


/*****************advfs_handyman_thread related ************************/

#define ADVFS_HANDYMAN_QUEUE_SIZE 100

/* 
 * This enumeration defines the message type for a message to the 
 * advfs_handyman_thread
 */
typedef enum {
    AHME_FINISH_DIR_TRUNC,      /* Finish a directory trunction */
    AHME_RETRY_IO,              /* Perform an IO Retry */
    AHME_THREAD_GO_AWAY,        /* Kill the thread */
    AHME_CFS_XID_FREE_MEMORY    /* Clean up memory on failover recovery */
} advfs_handyman_msg_type_t;

/* 
 * This structure represents a message to the advfs_handyman_thread.  The
 * union is provides data depending on the message type.
 */
struct advfs_handyman_thread_msg {
    advfs_handyman_msg_type_t ahm_msg_type;     /* Message type */
    union {
        dtinfoT      ahm_dtinfo;          /* Dir truncation info structure */
        struct buf  *ahm_io_retry_bp;     /* buf struct for io retry */
        bfDomainIdT *ahm_xid_free_dmnIdP; /* domainIdP from
                                             cfs_xid_free_memory */
    } ahm_data;
};
typedef struct advfs_handyman_thread_msg advfs_handyman_thread_msg_t;

/**************End of advfs_handyman_thread related**********************/


#include <advfs/bs_access.h>
#include <advfs/bs_vd.h>
#include <advfs/bs_domain.h>
#include <advfs/bs_bitfile_sets.h>
#ifdef _KERNEL
#include <advfs/bs_tagdir_kern.h>
#else
#include <advfs/bs_tagdir.h>
#endif /* _KERNEL */
#include <advfs/bs_bmt.h>
#include <advfs/bs_sbm.h>

/* Data that needs to be passed from IO startup to IO completion when
 * doing a request on behalf of AIO so that the appropriate cleanup
 * can happen during IO completion. Values in this structure are only
 * valid if ioanchor.anchr_aio_bp is set.
 */
typedef enum {
    ANCHR_AIO_NOFLAGS      = 0,
    ANCHR_AIO_BUFPIN       = 0x00000001,        /* unlock w/ bufunpin() */
    ANCHR_AIO_VASLOCKPAGES = 0x00000002,        /* unlock w/ vasunlockpages() */
} anchr_aio_flags_t;
typedef struct advfs_aio_cleanup {
      actRangeT *anchr_actrangep; /* Active range pointer when using
                                   * active range locking Otherwise, set to 0.*/
      rwlock_key_t anchr_lock_key;/* key passed to IO completion routine for
                                   * reclaiming a bfap->cacheModeLock that has
                                   * been disowned. */
      size_t  anchr_aio_iosize;   /* Number of bytes transferred for all bufs
                                   * associated with this ioanchor */
      space_t anchr_aio_sid;      /* space id for buffer address */
      void *anchr_aio_address;    /* user address  to be unwired */
      size_t anchr_aio_size;      /* size of space to be unwired */
      anchr_aio_flags_t anchr_aio_flags;   /* flags as required */
  } anchr_aio_t;

typedef struct  ioanchor  {
#ifdef _KERNEL
    spin_t anchr_lock;          /* Coordinate changes to anchor using lock. */
                                /* advfs_iodone() always gets spin lock. */
#endif /* _KERNEL */
    int64_t anchr_iocounter;    /* Single IO request callers set to 1. Else, */
                                /* set to number of IO's in multi-IO set */
    uint64_t anchr_flags;       /* Anchor flags for advfs_iodone to check */
    struct buf *anchr_origbuf;  /* Set to the original UFC IO buf structure*/
                                /* for advfs_iodone to use. */
    cv_t anchr_cvwait;     	/* Optionally allows caller to sleep on this */
                                /* condition variable until IO completes. */
                                /* Caller can also use with */
                                /* IOANCHORFLG_WAKEUP_ON_ALL_IO flag */
    struct ioanchor *anchr_listfwd; /* Caller can link multiple anchors to */
    struct ioanchor *anchr_listbwd; /* take responsibility for freeing anchors. */
                                /* Caller must set the */
                                /* IOANCHOR_KEEP_ANCHOR flag to use the link.*/
    struct buf *anchr_aio_bp;   /* Asynchronous IO buffer for directIO only.*/
    anchr_aio_t anchr_aio_info; /* info passed to IO completion routine
                                 * when called in AIO context.  Values are
                                 * only valid if anchr_aio_bp is set.
                                 */
    statusT anchr_io_status;    /* Final status of a multi-part IO */
    off_t anchr_min_req_offset; /* Lowest offset of a multi-part IO. Must be
                                 * set if the # of bytes successfully
                                 * transferred is to be returned in next fld */
    off_t anchr_min_err_offset; /* If a multi-part IO is started, set this
                                 * field to a value greater than all the
                                 * file offsets for the IOs started.  This field
                                 * is used internally to track the lowest IO
                                 * that did not succeed.  After the final IO,
                                 * this field contains the # of bytes success-
                                 * fully transferred by this multi-part IO. */
    uint32_t anchr_magicid;     /* Unique structure validation identifier */
    struct buf *anchr_buf_copy; /* A copy of the original buf struct.  Used by snapshots */
    struct adviodesc *anchr_error_ios; /* A chain of adviodec structs that had
                                 * errors occur during IO.  The chain is
                                 * maintained in the advio_fwd pointer of
                                 * the adviodesc_t only if the
                                 * IOANCHORFLG_CHAIN_ERRORS flag is set */
} ioanchor_t;

/* ioanchor_t structure flags. Protected by ioanchor.anchr_lock */
#define IOANCHORFLG_KEEP_ANCHOR         0x1
                                 /* Tell advfs_iodone() not to free ioanchor*/
                                 /* at final IO processing when */
                                 /* anchr_iocounter drops to 0. */
                                 /* By default, advfs_iodone() frees the */
                                 /* anchor at final IO completion procesing. */
#define IOANCHORFLG_KEEP_BUF            0x2 
                                 /* Tell advfs_iodone() not to free every */
                                 /* IO buf structure. The caller will handle*/
                                 /* freeing the buf structures except the */
                                 /* anchr_origbuf buf since UFC iodone */
                                 /* handles freeing the anchr_origbuf. */
                                 /* DirectIO must set the anchr_origbuf= NULL*/
                                 /* but can use this flag if it wants to */
                                 /* handle freeing the buf structures */
#define IOANCHORFLG_WAKEUP_ON_COW_READ    0x4
                                 /* Instruct advfs_iodone() to wakeup */
                                 /* waiter after every IO completion in a */
                                 /* multi-IO anchor set. However, final IO */
                                 /* completion processing is done only when */
                                 /* the anchr_iocounter drops to 0. */

#define IOANCHORFLG_WAKEUP_ON_LAST_IO    0x8
                                 /* Instruct advfs_iodone() to wakeup 
                                  * waiter on the last io assocaited with this
                                  * anchor. If a caller want to wait on the
                                  * anchor then either this flag or the 
                                  * above flag MUST be set
                                  */

#define IOANCHORFLG_DIRECTIO            0x10
                                 /* Instructs advfs_iodone() to do special */
                                 /* special DirectIO processing. */
#define IOANCHORFLG_IODONE              0x20
                                 /* Set in advfs_iodone() when cv_broadcast  */
                                 /* is issued.  A thread can check this flag */
                                 /* to avoid doing a cv_wait() that will     */
                                 /* never wake up. Useful only if also using */
                                 /* IOANCHORFLG_KEEP_ANCHOR flag as well.    */
#define IOANCHORFLG_ADVFS_STRATEGY_IO   0x40
                                 /* IO originates from advfs_strategy() that */
                                 /* tells advfs_iodone() to do traditional   */
                                 /* buffer biodone processing.               */

#define IOANCHORFLG_CHAIN_ERRORS        0x80
                                /* Indicates that any adviodesc's associated
                                 * with this anchor that get an io error 
                                 * should chain the errant adviodesc's */
                  

#ifdef _KERNEL

typedef struct adviodesc {
    blkDescT advio_blkdesc;     /* Virtual disk location and index */
    bfAccessT *advio_bfaccess;  /* File access structure */
    ioanchor_t *advio_ioanchor; /* All callers initiating IO supply an anchor*/
    struct bsBuf *advio_bsbuf;  /* Only metadata/log data write IO set ptr */
    int(*advio_save_iodone)__((struct buf *));
                                /* Save IO caller's buf iodone() */
    uint64_t advio_flags;       /* Flags set by advfs_bs_startio() caller */
                                /* for advfs_iodone() processing. */
    /* Following fields are mainly for AdvFS IO retry. */
    off_t advio_foffset;        /* 1KB aligned file byte block offset. */
    caddr_t advio_targetaddr;   /*starting data virtual address for I/O */
    uint64_t advio_retry_starttime; /* Usec time of initial IO used for retry*/
    uint32_t advio_ioretrycount;/* Count of AdvFS initiated I/O retries */
    uint32_t advio_magicid;     /* Unique structure validation identifier */
    struct adviodesc *advio_fwd;/* Used to link descriptors */
} adviodesc_t;

/* AdvFS advfs_bs_startio()/advfs_iodone() flags. */
#define ADVIOFLG_FAKEIO     0x1 /* Caller wants advfs_bs_startio() to setup */
                                /* for AdvFS IO completion to be called */
                                /* without calling the disk strategy. This */
                                /* allows proper IO completion cleanup. */

#define ADVIOFLG_RETRYIO    0x2
                               /* Caller wants to retry an IO request that */
                               /* previously returned an IO error. The buf */
                               /* structure and other IO structures are */
                               /* already initialized and any fields */
                               /* necessary have been reset. */

#define ADVIOFLG_DMN_PANIC_IO 0x4
                               /* This is not an advfs_bs_startio() API flag.*/
                               /* Only for advfs_bs_startio() to internally  */
                               /* indicate to advfs_iodone() to process a    */
                               /* a filesystem (domain) panic IO error.      */
#define ADVIOFLG_SNAP_READ    0x8
                               /* Used to indicate that advfs_bs_startio
                                * must set an iocounter value of 2 and the
                                * IOANCHORFLG_WAKEUP_ON_ALL_IO */

                               
void
advfs_bs_startio(struct buf * bp,       /*in */
                 ioanchor_t *ioanchorp, /*in */
                 struct bsBuf * bsbufp, /*in */
                 bfAccessT *bfap,       /*in */
                 struct vd *vdp,        /*in */
                 uint64_t flags);       /*in */

void
advfs_iodone(struct buf *bp);

/* Flags definitions for struct advfs_pvt_param */


typedef enum {
    APP_MIGRATE_FLUSH_ALL     = 0x1,    /* Indicates to scan for clean and
                                         * dirty pages 
                                         */
    APP_MIGRATE_FLUSH_WAIT    = 0x2,    /* Just scan for clean and dirty
                                         * don't do i/o 
                                         */
    APP_MIGRATE_FAULT         = 0x4,   /* Migrate is causing the fault.  
                                        * It is not necessary to acquire any
                                        * snapshot locks since they have
                                        * already been acquired.
                                        */
    
    APP_IO_PERFORMED          = 0x8,    /* This flag is set by advfs_getpage to 
                                         * indicate to the caller that IO
                                         * was performed.  This is used to
                                         * determine if a metapage should be
                                         * validated in pin/refpg. Only
                                         * valid for metadata writes and all
                                         * reads.
                                         */
    APP_READ_HOLE             = 0x10,   /* This flag is set if advfs_getpage 
                                         * finds a datafill page that is over 
                                         * a hole. 
                                         */
    APP_ASSERT_NO_DIRTY       = 0x20,   /* If this flag is set on a call to 
                                         * putpage, putpage should ASSERT that 
                                         * no dirty data is found. This is for
                                         * debug mode.
                                         */
    APP_ASSERT_NO_CLEAN       = 0x40,   /* If this flag is set on a call to 
                                         * putpage, putpage should ASSERT 
                                         * that no clean pages are found. 
                                         * This is for debug mode.
                                         */
    APP_ADDSTG_NOCACHE        = 0x80,   /* Request getpage() to add storage w/o
                                         * bringing the pages into the cache or
                                         * zeroing the pages on disk.
                                         */
    APP_ADDEDSTG_NOCACHE      = 0x100,  /* Getpage responds by setting this flag
                                         * to indicate that it did, indeed,
                                         * successfully add storage.
                                         */
    APP_METAPG_OVERWRITE      = 0x200,  /* bs_pinpg caller will ovewrite full
                                         * metadata page. So skip doing IO
                                         * to retrieve data from disk to prime
                                         * new cache pages as an optimization.
                                         */
    APP_KICKOFF_READAHEAD     = 0x400,  /* Indicates that a read-ahead should
                                         * be started.
                                         */
    APP_READAHEAD_FAST_PATH   = 0x800,  /* Indicates to advfs_getpage() that
                                         * it should only start the
                                         * read-ahead described in the
                                         * advfs_pvt_params.
                                         */
    APP_NO_READAHEAD_ENTRY    = 0x1000, /* Indicates to advfs_getpage() that
                                         * caller found no entry for the
                                         * current thread in the file's
                                         * read-ahead history cache.
                                         */
    APP_MARK_READ_ONLY        = 0x2000, /* Tells putpage to just mark the pages
                                         * read only and release them */
    APP_RESTORE_PG_PROTECTION = 0x4000, /* This tells putpage to scan all in 
                                         * cache pages and set protects 
                                         * according to allocated storage.  
                                         * This is necessary to undo the
                                         * APP_MARK_READ_ONLY call that
                                         * protected in cache pages when
                                         * creating a snapshot. */
    APP_FORCE_COW             = 0x8000, /* A hint to getpage to optimize 
                                         * for COW */
    APP_SNAP_LOCK_HELD        = 0x10000,/* An error case from getpage to 
                                         * invalidate needs to tell putpage 
                                         * the snap lock is held */
    APP_DEBUG_DIRTY           = 0x20000 /* Used under debug only. Used for
                                         * verifying that a metadata page
                                         * is dirty.
                                         */
 
} pvt_param_flags_t;
/* 
 * Storage descriptor passed between write and getpage 
 */

struct advfs_pvt_stg_desc {
    bf_fob_t app_stg_start_fob;       /* First FOB with newly-allocated stg */
    bf_fob_t app_stg_end_fob;         /* Last  FOB with newly-allocated stg */
    struct advfs_pvt_stg_desc *next; /* list of descriptors. */
};



/*
 * Private parameter definition for VOP_GETPAGE/VOP_PUTPAGE
 */

struct advfs_pvt_param {
    off_t app_total_bytes; 	      /* Total read() or write() length */
    off_t app_starting_offset;	      /* Starting Offset of original request */
    pvt_param_flags_t app_flags;      /* Flags */
    struct advfs_pvt_stg_desc stg_desc;
    ftxHT    app_parent_ftx;          /* Parent transaction when an explicit
                                       * fault occurs on metadata.
                                       * Also if a Direct I/O call allocates
                                       * storage when BFS_OD_OBJ_SAFETY is
                                       * enabled, the storage allocation
                                       * transaction handle is returned so
                                       * that advfs_fs_write_direct() can
                                       * call ftx_done() after the write.
                                       */
    struct actRange *arp;             /* Null if no active range pointer.
                                       * arp->arDebug == 0 if not set yet.
                                       */
};

/* Use to zero initialize all of the structure's fields */
static const struct advfs_pvt_param Nil_advfs_pvt_param = {0};

/*
 * BsBuf hashtable definitions and macros
 */

#include <advfs/e_dyn_hash.h>

#define BS_BSBUF_INITIAL_SIZE 512 
#define BS_BSBUF_HASH_CHAIN_LENGTH 30
#define BS_BSBUF_ELEMENTS_TO_BUCKETS 5
#define BS_BSBUF_USECS_BETWEEN_SPLITS 1000000

#define BS_BSBUF_GET_KEY(_vnode, _offset) (((uint64_t) _vnode >> 12) + (_offset >> 13))

#define BS_BSBUF_HASH_LOCK(_key,_insert_count) \
( (struct bsBuf *) dyn_hash_obtain_chain(BsBufHashTable,_key,_insert_count))

#define BS_BSBUF_HASH_UNLOCK(_key) \
( dyn_hash_release_chain(BsBufHashTable,_key))

#define BS_BSBUF_HASH_INSERT(_bsBuf,_obtain_lock) \
((void) dyn_hash_insert(BsBufHashTable,_bsBuf,_obtain_lock))

#define BS_BSBUF_HASH_REMOVE(_bsBuf,_obtain_lock) \
((void) dyn_hash_remove(BsBufHashTable,_bsBuf,_obtain_lock))

/*
 * PROTOTYPES
 */

/* This may look odd but it makes lint happy and doesn't
 * require brackets. We tried it with an if statement
 * but that hurts our code coverage numbers. 
 *
 * What is being done is
 * sts=err_code;goto HANDLE_EXCEPTION;
 * but since callers use the form RAISE_EXCEPTION(err);
 * we are getting lint errors saying the extra ; is
 * unreached code. 
 * But having 2 statements in a row will break uses like
 * if (sts != EOK) RAISE_EXCEPTION(sts); Which will cause
 * the goto to always be executed.
 */

#define RAISE_EXCEPTION( err_code ) for(sts = err_code;1;1) goto HANDLE_EXCEPTION

statusT
bfm_open_ms( 
            bfAccessT **outbfap,        /* out */
            domainT* dmnp,              /* in - domain pointer */
            vdIndexT bfDDisk,           /* in - domain disk index */
            bfdBfMetaT bfMIndex         /* in - metadata bitfile index */
            );

#ifdef _KERNEL
int 
advfs_bs_multiWait_ctor(void *mwp, size_t mwpsize, arena_flags_t flags);

void 
advfs_bs_multiWait_dtor(void *mwp, size_t mwpsize, arena_flags_t flags);
#endif /* _KERNEL */

/*
 * Flags for advfs_cfs_flush_and_invalidate().
 */
#define AFI_NO_INVALIDATE       0x1

void
advfs_cfs_flush_and_invalidate(
                          struct vnode *vp, /* in */
                          int flags         /* in */
                          );

/*
 * Flags for advfs domain and bfs flush routines.
 */
typedef enum {
    FLUSH_NOFLAGS       =  0x0,
    FLUSH_ASYNC         =  0x1,  /* asynchronous flush */
    FLUSH_UPDATE_STATS  =  0x2,  /* update file stats */
    FLUSH_NOWAIT        =  0x4,  /* no lock wait UFC flush */
    FLUSH_NO_TBL_LOCK   =  0x8,
    FLUSH_STATS_ONLY    = 0x12  /* flush stats but not user data */
} flushFlagsT;

void
advfs_bs_bfs_flush(bfSetT *bfSetp,      /*in - Fileset to be flushed */
                   int32_t invalidate,  /*in - If TRUE, invalidate pages */
                   flushFlagsT flags    /*in - flags */
                   );

void
advfs_bs_dmn_flush_meta(domainT *dmnp,     /* in - Domain to flush */
                        flushFlagsT flags  /* in - flags */
                        );


void
advfs_bs_dmn_flush(
               domainT* dmnp,     /* in */
               flushFlagsT flags  /* in */
               );


typedef enum {
    MPF_NO_FLAGS        = 0x0,  /* No flags */
    MPF_CFS_CALLER      = 0x1,  /* CFS is the caller.  If the BFA_XTNTS_IN_USE flag
                                 * is set, return E_RETRY since we would
                                 * deadlock trying to get the bfaSnapLock */
    MPF_XTNT_CNT        = 0x2,  /* Just count the extents */
    MPF_FOB_CNT         = 0x4,  /* Just count the fobs */
    MPF_XTNTMAPS        = 0x8   /* Return the extent maps */
       
} map_flags_t;

/*
 * This routine is used by external callers to acquire a copy of the xtnt
 * maps for a file 
 */
statusT
advfs_get_xtnt_map(
        bfAccessT*      bfap,           /* in - file */
        int32_t         start_xtnt,     /* in - start extent */      
        int32_t         array_size,     /* in - size of buffer */
        bsExtentDescT   xtnts_array[],  /* in/out - extent buffer */
        int32_t         *xtnt_cnt,      /* out - number of extents copied */
        bf_fob_t        *fob_cnt,       /* out - number of fobs allocated */
        vdIndexT*       alloc_vd_idx,
        map_flags_t     map_flags);     /* out - disk on which stg is alloced */




void advfs_init_access_area();
void init_crmcell_opx();
void init_bs_delete_opx( void );
void init_bs_bmt_util_opx( void );

int 
advfs_bs_bsbuf_ctor(void *bsbufp, size_t bsBufSize, int flags);

void
advfs_bs_bsbuf_dtor(void *bsbufp, size_t bsBufSize, int flags);

struct bsBuf *
advfs_obtain_bsbuf(struct vnode *vp,off_t offset, void ** cookie, int *found);

struct bsBuf *
advfs_bs_get_bsbuf (int32_t wait);

void
advfs_bs_free_bsbuf(struct bsBuf * bsbufp, int32_t wait);

int
advfs_remove_bsbuf(struct vnode *vp, off_t start_offset, size_t size,  void * cookie);

int
advfs_bs_ioanchor_ctor(void *ioanchorp, size_t ioAnchorSize, int flags);

void
advfs_bs_ioanchor_dtor(void *ioanchorp, size_t ioanchorSize, int flags);

ioanchor_t *
advfs_bs_get_ioanchor(int32_t wait);

void
advfs_bs_free_ioanchor(ioanchor_t * ioanchorp, int32_t wait);

struct buf *
advfs_bs_get_buf (int32_t wait, int32_t zeromem);

void
advfs_bs_free_buf(struct buf * bufp);

adviodesc_t *
advfs_bs_get_adviodesc(int32_t nozero);

void
advfs_bs_free_adviodesc(adviodesc_t * iodescp); 

int
advfs_bs_actRange_ctor( void *actRangep, size_t actRangeSize, int flags );

void
advfs_bs_actRange_dtor( void *actRangep, size_t actRangeSize, int flags );

actRangeT *
advfs_bs_get_actRange(int32_t wait);

void
advfs_bs_free_actRange(actRangeT *actRangep, int32_t wait);

void
advfs_bs_io_complete(struct buf *);

statusT
bs_refpg_direct(void *bfPageAddr,              /* in */
                int number_to_read,            /* in */
                struct bfAccess *bfap,         /* in */
                bf_fob_t bsBlock,              /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_read,              /* out */
                int *aio_flag);                /* out */

statusT
bs_pinpg_direct(void *bfPageAddr,              /* in */
                int number_to_write,           /* in */
                struct bfAccess *bfap,         /* in */
                unsigned long bsBlock,         /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_written,           /* out */
                int *aio_flag,                 /* out */
                struct ucred *cred);           /* in */


/***********************************************************
 *
 * Ftx private function prototypes
 *
 ***********************************************************/

/*
 * ftx_set_dirtybufla - set oldest dirty buffer log address for domain
 */
void
ftx_set_dirtybufla(
                   domainT* dmnp,
                   logRecAddrT dirtyBufLa
                   );

/*
 * ftx_get_dirtybufla - gets the oldest dirty buffer log address
 */

logRecAddrT
ftx_get_dirtybufla(
                   domainT* dmnp
                   );

/*
 * ftx_init_recovery_logaddr - initialize crash restart log address
 * structures for domain.
 */
void
ftx_init_recovery_logaddr(
                          domainT* dmnp
                          );

/*
 * ftx_bfdmn_recovery - recover domain consistency
 */
statusT
ftx_bfdmn_recovery(
                   domainT* dmnp,
		   uint64_t flags
                   );
#endif  /* end _KERNEL */
#endif  /* end bs_ims.h */
