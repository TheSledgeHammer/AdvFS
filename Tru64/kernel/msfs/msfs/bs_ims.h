/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
* @(#)$RCSfile: bs_ims.h,v $ $Revision: 1.1.146.1 $ (DEC) $Date: 2004/07/14 17:41:06 $
 */

#ifndef _BS_IMS_H_
#define _BS_IMS_H_

#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>             /* for fsContext def - bad layering? */
#ifdef KERNEL
#include <sys/vnode.h>
#include <kern/event.h>
#endif /* KERNEL */

/*
 ** LOG_FLUSH_THRESHOLD must be a multiple of 4 **
 */
#define LOG_FLUSH_THRESHOLD  8

/*
 * Block map structures
 */

typedef struct blkDesc {
    uint16T vdIndex;
    uint32T vdBlk;
} blkDescT;

enum ioQueue { NONE=0, WAIT_LAZY, READY_LAZY, BLOCKING, DEVICE, CONSOL, SMSYNC_LAZY0,
               SMSYNC_LAZY1, SMSYNC_LAZY2, SMSYNC_LAZY3, SMSYNC_LAZY4, SMSYNC_LAZY5,
               SMSYNC_LAZY6, SMSYNC_LAZY7, SMSYNC_LAZY8, SMSYNC_LAZY9, SMSYNC_LAZY10,
               SMSYNC_LAZY11, SMSYNC_LAZY12, SMSYNC_LAZY13, SMSYNC_LAZY14,
               SMSYNC_LAZY15, FLUSHQ, TEMPORARYQ, UBCREQQ };

typedef struct ioDesc {
    struct ioDesc *fwd;              /* doubly linked I/O queue */
    struct ioDesc *bwd;
    blkDescT blkDesc;
    enum ioQueue ioQ;           /* which queue desc is on */
    int numBlks;                /* number of 512 byte blocks */
    int ioQtime;                /* lbolt when placed on this queue */
    unsigned long ioQln;        /* who did the deed */
    /* devosfbuf buffer is only valid between I/O start and I/O completion */
    struct buf *devosfbuf;      /* call_disk address for I/O buf struct */
    unsigned char *targetAddr;  /* data address for I/O */
    struct bsBuf *bsBuf;        /* buffer that owns this I/O */
    short ioCount;              /* how many I/O's queued for this descriptor */
    /* Fields used for consolidated I/O */
    short consolidated;         /* bit indicates whether consolidated */
    int totalBlks;              /* total of blocks in big I/O */
    int desCnt;                 /* count of descriptors consolidated */
    short data_never_written;   /* bit indicates this was never written */
    ushort ioRetryCount;        /* Count of AdvFS initiated I/O retries */
} ioDescT;

#define ADVFS_IO_RETRY_MAX  9       /* Maximum number of I/O retries allowed */

#define IODH_TRACE_HISTORY 16

typedef struct {
  uint32T       seq;
  uint16T       mod;
  uint16T       ln;
  struct thread *thd;
  void          *val;
} ioDHTraceElmT;

/* The first two fields of this structure and the ioDesc structure are
 * identical.
 */
typedef struct ioDescHdr {
    ioDescT *fwd;    /* doubly linked I/O queue */
    ioDescT *bwd;
    mutexT ioQLock;  /* simple lock to protect struct fields */
    int ioQLen;
    int lenLimit;
    ulong queue_cnt; /* cumulative count of ioDescs placed onto queue */
    uint ioQ;
    int pad[5];      /* pad to 64-byte boundary to avoid cache line contention */
#ifdef ADVFS_IODH_TRACE
    uint trace_ptr;
    ioDHTraceElmT trace_buf[IODH_TRACE_HISTORY];
#endif
} ioDescHdrT;

typedef struct blkMap {
    int read;            /* index of first read entry in ioDesc[] */
    int readCnt;         /* count of read entries */
    int write;           /* index of first write entry in ioDesc[] */
    int writeCnt;        /* count of write entries */
    int maxCnt;          /* number of entries in ioDesc[] */
    blkDescT *blkDesc;
} blkMapT;

typedef struct ioList {
    int read;            /* index of first read entry in ioDesc[] */
    int readCnt;         /* count of read entries */
    int write;           /* index of first write entry in ioDesc[] */
    int writeCnt;        /* count of write entries */
    int maxCnt;          /* number of entries in ioDesc[] */
    ioDescT *ioDesc;
} ioListT;

#include <msfs/bs_buf.h>
#include <msfs/bs_service_classes.h>
#include <msfs/bs_domain.h>

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
    INITIALIZED,
    NEW_MCELL,
    USED_MCELL
} mcellStateT;

/*
 * bsInMemSubXtntMapT - A partial extent map that describes some or all of the
 * pages described by an on-disk extent record.  None or all of the record's
 * extent descriptors are cached in the sub extent map's extent array.  One or
 * more sub extent maps describe a bitfile's page range.
 *
 * The page range described by a sub extent map includes mapped and unmapped
 * pages.  The mapped pages are described by the extent array.  The unmapped
 * pages are those pages between one sub extent map's last mapped page and the
 * next sub extent map's first mapped page.
 */

typedef struct bsInMemSubXtntMap {
    uint32T pageOffset;     /* The first page described by the map */
    uint32T pageCnt;        /* Number of pages described by the map */
    vdIndexT vdIndex;       /* Disk where extent record is located */
    bfMCIdT mcellId;        /* Mcell where extent record is located */
    mcellStateT mcellState; /* sub extent map is new */
    uint32T type;           /* On-disk record type */
    uint32T onDiskMaxCnt;   /* max number of entries in on-disk extent record */
    uint32T updateStart;    /* Index first entry to save in on-disk xtnt record*/
    uint32T updateEnd;      /* Index last entry to save in on-disk xtnt record */
    uint32T cnt;            /* The number of valid entries in bsXA */
    uint32T maxCnt;         /* The number of entries in bsXA */
    bsXtntT *bsXA;          /* Array of extent descriptors */
} bsInMemSubXtntMapT;

/* 
 * bsInMemXtntMapT - Represents cached on-disk extent information.
 */

struct bsInMemXtntMap {
    struct bsInMemXtntMap *nextXtntMap;  /* Link to next extent map */
    domainT *domain;         /* pointer to the domain */
    uint32T hdrType;         /* Type of on-disk header record */
                             /* The header record contains the mcell count */
                             /* and a pointer to the other mcells in the list */
    vdIndexT hdrVdIndex;     /* Pointer to header mcell */
    bfMCIdT hdrMcellId;      /* Pointer to header mcell */
    uint32T blksPerPage;     /* Number of blocks per page */
    uint32T nextValidPage;   /* 1 beyond highest valid page. Used by page copy*/
    uint32T allocDeallocPageCnt;  /* Number of pages to alloc or dealloc.  */
                                  /* Used for stripe bitfiles. */
    vdIndexT allocVdIndex;   /* Disk on which the next storage is allocated. */
    uint32T origStart;       /* Index of the original entry that maps the */
                             /* first page that is added or removed */
    uint32T origEnd;         /* Index of the original entry that maps the */
                             /* last page that is added or removed */
    uint32T updateStart;     /* Index of first entry that contains new mapping*/
                             /* information. */
    uint32T updateEnd;       /* Index of last entry that contains new mapping */
                             /* information. */
    uint32T validCnt;        /* Number of in-use and valid sub extent maps */
    uint32T cnt;             /* Number of in-use sub extent maps */
    uint32T maxCnt;          /* Number of sub extent maps */
    bsInMemSubXtntMapT *subXtntMap;   /* Array of in-memory sub extent maps */
};

typedef struct bsInMemXtntMap bsInMemXtntMapT;

/*
 * bsStripeHdrT - The control structure for striped bitfiles.
 */

typedef struct bsStripeHdr {
    uint32T segmentSize;       /* minimum number of pages that are allocated */
                               /* to a stripe segment. */
    uint32T cnt;               /* Count of valid elements in xtntMap. This is */
                               /* the number of segments in a stripe */
    bsInMemXtntMapT **xtntMap; /* Array of extent map ptrs.  Stripe segments */
                               /* on one disk are grouped into one extent map.*/
} bsStripeHdrT;

/*
 * bsInMemXtntT - The in-memory extent map header.
 *
 * xtntMap/shadowXtntMap and stripeHdr are mutually exclusive.
 */

typedef struct bsInMemXtnt {
    int validFlag;                 /* Non-zero if extent maps are valid */
    bsInMemXtntMapT *xtntMap;      /* Link to primary extent map */
    bsInMemXtntMapT *shadowXtntMap;  /* Link to shadow extent maps */
    bsStripeHdrT *stripeXtntMap;   /* Link to stripe extent maps */
    bsInMemXtntMapT *copyXtntMap;  /* Link to copy extent maps */
    lock_data_t migTruncLk;        /* Serialize migrate and truncate */
    bsXtntMapTypeT type;           /* The type of extent maps */
    uint32T allocPageCnt;          /* Number of allocated pages */
} bsInMemXtntT;

extern bsInMemXtntT NilXtnts;

/*
 * ioThreadMsgTypeT
 */

typedef enum {
    LF_PB_CONT,
    START_MORE_IO,
    THREAD_GO_AWAY,
    RETRY_IO
} ioThreadMsgTypeT;

/*
 * ioThreadMsgT
 */

typedef struct {
   ioThreadMsgTypeT msgType;
   union {
       bfDomainIdT dmnId;
       domainT *dmnP;
       struct buf *ioRetryBp;
   } u_msg;
   uint32T vdi;   
   struct vd *vdp;
} ioThreadMsgT;

#include <msfs/bs_access.h>
#include <msfs/bs_vd.h>
#include <msfs/bs_domain.h>
#include <msfs/bs_bitfile_sets.h>
#include <msfs/bs_tagdir.h>
#include <msfs/bs_bmt.h>
#include <msfs/bs_sbm.h>

/*
 * PROTOTYPES
 */

#define RAISE_EXCEPTION( err_code ) {sts = err_code; goto HANDLE_EXCEPTION;}

statusT
bfm_open_ms( 
            bfAccessT **outbfap,        /* out */
            domainT* dmnp,              /* in - domain pointer */
            int bfDDisk,                /* in - domain disk index */
            bfdBfMetaT bfMIndex         /* in - metadata bitfile index */
            );

void
bfr_trace( bfTagT tag,                  /* in */
          u_long bsPage,                /* in */
          TraceActionT type,            /* in */
          unsigned unpinType            /* in */
          );

int
bs_find_page( struct bfAccess *bfap,          /* in */
              unsigned long bfPage,           /* in */
              int block                       /* in */
            );

void
bs_invalidate_pages (
                     bfAccessT *bfap,   /* in */
                     uint32T pageOffset,/* in */
                     uint32T pageCnt,   /* in */
                     int invalflag      /* in */
                     );

/*
 * Flags for msfs_flush_and_invalidate().
 */
#define NO_INVALIDATE           0x1
#define INVALIDATE_UNWIRED      0x2
#define INVALIDATE_ALL          0x4
#define FLUSH_PREALLOC          0x8
#define INVALIDATE_QUOTA_FILES  0x10

void
msfs_flush_and_invalidate(
                          bfAccessT *bfap, /* in */
                          int fiflags      /* in */
                          );

void
bs_q_blocking( 
    ioDescT *ioListp,     /* in */
    int count             /* in */
    );

void
bs_q_flushq(
    ioDescT *ioListp,     /* in */
    int count             /* in */
    );

void
bs_q_ubcreq(
    ioDescT *ioListp,     /* in */
    int count             /* in */
    );

void
bs_q_lazy( 
    ioDescT *ioListp,        /* in */
    int len                  /* in */
    );

statusT
bs_raw_page( bfAccessT *bfap,           /* in */
            unsigned vdIndex,           /* in */
            unsigned startBlk,          /* in */
            unsigned blkCnt,            /* in */
            unsigned char *buf,         /* in - out */
            rawModeT rw                 /* in */
            );

void
bs_bflush( struct vd *vdp       /* in */
         );

statusT
bfflush_sync(bfAccessT * bfap, /* in */
             lsnT waitLsn          /* in */
             );

/*
 * Return TRUE if the given page is on the log's
 * dirty list.  Otherwise, return FALSE.
 */
int
bs_logpage_dirty( 
    bfAccessT *bfap,        /* in */
    u_long pageNum          /* in */
    );

void
bs_pinblock(
            domainT *dmnP,              /* in */
            lsnT lsnToWriteTo           /* in */
            );

void 
bs_lsnList_flush(
            domainT *dmnP               /* in */
            );

statusT
bs_set_bufstate( 
                bfPageRefHT bfPageRefH,  /* in */
                int ln,  /* in */
                uint32T stateBit,  /* in */
                int waitFlag  /* in */
                );

statusT
bs_clear_bufstate( 
                  bfPageRefHT bfPageRefH,  /* in */
                  uint32T stateBit  /* in */
                  );

statusT
bs_get_bufstate( 
                bfPageRefHT bfPageRefH,      /* in */
                unsigned whichStateBit,      /* in */
                unsigned *stateBitValue      /* out */
                );

statusT
buf_remap(
   bfPageRefHT bfPageRefH,      /* in */
   blkMapT *blkMap );           /* in */

int
bs_bflush_sync( struct vd *vdp      /* in */
              );

statusT
bs_bf_flush_nowait(
                   bfAccessT* bfap
                   );

statusT
bs_get_bf_xtnt_map(
    bfAccessT *bfap,                    /* in */
    int startXtntMap,                   /* in */
    int startXtnt,                      /* in */
    int xtntArraySize,                  /* in */
    bsExtentDescT xtntsArray[],         /* out */
    int *xtntCnt,                       /* out */
    vdIndexT *allocVdIndex              /* out */
);

/*
 * Flags for backup extent map routines.
 */
#define XTNT_XTNTMAP       1           /* Get the extent map */
#define XTNT_XTNTCNT_ONLY  2           /* Get the extent count */
#define XTNT_PAGECNT_ONLY  4           /* Get the page count */

statusT
bs_get_clone_xtnt_map(
    bfAccessT  *clon_bfap,              /* in */
    int startXtntMap,                   /* in */
    int startXtnt,                      /* in */
    int xtntArraySize,                  /* in */
    int flags,                          /* in  */
    bsExtentDescT xtntsArray[],         /* out */
    int *xtntCnt,                       /* out */
    vdIndexT *allocVdIndex,             /* out */
    int *pageCnt                        /* out */
);

statusT
bs_get_stripe_xtnt_map(
    bfAccessT* in_bfap,                 /* in */
    int startXtntMap,                   /* in */
    int startXtnt,                      /* in */
    int xtntArraySize,                  /* in */
    int flags,                          /* in  */
    bsExtentDescT xtntsArray[],         /* out */
    int *xtntCnt,                       /* out */
    int *pageCnt,                       /* out */
    int cloned                          /* in */
);

statusT
bs_get_bkup_xtnt_map(
    bfAccessT *in_bfp,                  /* in */
    int startXtntMap,                   /* in */
    int startXtnt,                      /* in */
    int xtntArraySize,                  /* in */
    int flags,                          /* in  */
    bsExtentDescT xtntsArray[],         /* out */
    int *xtntCnt,                       /* out */
    vdIndexT *allocVdIndex,             /* out */
    int *pageCnt                        /* out */
);

struct bfAccess*
grab_bsacc(
           bfSetT *bfSetp,      /* in - bitfile-set descriptor pointer */
           bfTagT tag,          /* in - bitfile tag */
           int forceFlag,       /* in - passed to get_free_acc() */
           uint32T options      /* in - options flags */
           );

void *
bs_vd_thread(
             void *
             );

void
bs_init( int doingRoot );

void bs_init_area();
void init_bscreate_opx();
int init_bs_stg_opx();
void init_bs_xtnts_opx();
int init_bs_bitmap_opx();
int init_crmcell_opx();
void init_bs_delete_opx( void );
void init_bs_bmt_util_opx( void );

statusT
bs_logflush_start( 
    bfAccessT *ap,              /* in */
    lsnT lsn                    /* in */
    );

void
bs_startio( 
    struct vd *vdp,      /* in */
    int flushFlag
    );

void
bs_io_complete( 
    struct bsBuf *bp,   /* in */
    int *s              /* in */
    );

void
link_write_req( struct bsBuf *bp );

void
bs_q_list( 
    ioDescT *ioListp,               /* in */
    int len,                        /* in */
    void (*q_for_io)( ioDescT *, int )   /* in */
    );

void
bs_init_io_thread( void );

int 
sendtoiothread(
    struct vd *vdp,                     /* in */
    ioThreadMsgTypeT sendType,          /* in */
    struct buf *bp                      /* in */
    );

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
                   domainT* dmnp
                   );

#endif /* _BS_IMS_H_ */
