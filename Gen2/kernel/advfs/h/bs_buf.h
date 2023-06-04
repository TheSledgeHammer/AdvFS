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
#ifndef _BS_BUF_H_
#define _BS_BUF_H_
#include <advfs/ms_assert.h>
#include <advfs/ms_generic_locks.h>

/* New externs need to be in include files.
 * So anyone including this file will get this extern.
 */

extern void *BsBufHashTable;


/* 
 * Forward declaration for fcache_map_dsc in fcache.h 
 */
struct fcache_map_dsc;

/* This macro sets the upper 28 bits with the line number, and the lower
 * 36 bits with the thread id.
 */
#define SET_LINE_AND_THREAD(ln) \
    ((uint64_t)(ln) << 36) +  \
    ((uint64_t)u.u_kthreadp & 0xfffffffff)

/*
 * multiWait structures are generic wait mechanisms.  They allow a
 * thread to wait for a number of events to occur.
 */

/*
 * multiWaitT - Wait for multiple events to complete.
 */
typedef struct multiWait {
#ifdef _KERNEL
    spin_t   mw_lock;              /* Guards mw_outstandingEvents field */
#endif /* _KERNEL */
    uint32_t mw_magicid;           /* Unique validation identifier */
    cv_t     mw_cv;                /* For synchronization */
    uint64_t mw_outstandingEvents; /* Number of events still to be completed */
} multiWaitT;

/*
 * multiWaitLinkT - links an event structure to a multiWaitT.
 *
 * A single event structure could be associated with multiple
 * multiWaits.  Also, a multiWait could encompass multiple
 * event structures.
 */
typedef struct multiWaitLinkT {
    struct multiWaitLink *mwl_fwd; /* Next multiWaitLinkT for this structure */
    multiWaitT           *mwl_mw;  /* The coordinating multiWait structure */
} multiWaitLinkT;

/* The following flags indicate which pfdats are currently in the cache
 * for this bsBuf. Theses represent pfdats that were brought into the
 * cache via pinpg. A clean pfdat (refpg) could exist in the cache w/o
 * one of these bits being set. A metadata page or bsBuf has 2 pfdats
 * (2 4K VM pages)
 */

typedef enum {
    METADATA_NONE = 0x0,           /* No resident PFDATs (for write) */
    METADATA_FIRST_HALF = 0x1,
    METADATA_SECOND_HALF = 0x2
} bsbuf_pfdatbitmap_t;

/* Bitfile Metadata page information */
struct bsBuf {
    struct bsBuf *bsb_metafwd;     /* Links dirty metadata on domain lsnList */
    struct bsBuf *bsb_metabwd;     /* protected by domainT.lsnLock or */
                                   /*links dirty log data on its dirtyBufList*/
                                   /* protected by bfAccessT.bfIoLock */
    struct bsBuf *bsb_iolistfwd;   /*Link multiple metadata bsBufs as part of*/
                                   /* a single UFC IO buf structure that IO */
                                   /* completion walks. Else set to NULL. */
    uint64_t bsb_padding1;         /* padding for spin lock alignment on PA */
#ifdef ADVFS_SMP_ASSERT
    bfTagT bsb_tag;                /* make a wild guess */
#endif

#ifdef _KERNEL
    spin_t   bsb_lock;             /* Spin lock for updating bsBuf fields */
#endif
#ifdef ADVFS_SMP_ASSERT
    bfDomainIdT bsb_domainId;      /* Make another wild guess */
#endif
    uint64_t bsb_writeref;         /* # of pinners on this buffer for writing*/
    lsnT bsb_flushseq;             /* Log file only: sequence LSN number for */
                                   /* log flushing */
    lsnT bsb_firstlsn;             /* Log file only: first LSN in log pg set */
                                   /* by get_clean_pg() */
    logRecAddrT bsb_origlogrec;    /* lsn of oldest log record for which  */
                                   /* this buffer contains modifications. */
    logRecAddrT bsb_currentlogrec; /* lsn of log record for most recent */
                                   /* modification to this buffer.        */
    uint64_t bsb_flags;            /* Flags to manage various metadata */
                                   /* operation transitions. */
    off_t bsb_foffset;              /* File byte offset of data (1KB aligned)*/
    struct bfAccess* bsb_bfaccess; /* to get to disk mapping info */
    struct fcache_map_dsc *bsb_fcachemapdesc; /*UFC virtual addr. map descriptor*/
                                   /* Volatile, valid only while pinning */
                                   /* a page to pass to bs_unpinpg */
    multiWaitLinkT *bsb_mwllist;   /* List of connectors to multiWaitT's    */
                                   /* that include this bsBuf in their */
                                   /* event list.  bsb_mwllist is protected */
                                   /* by bsBuf.bsb_lock */
    dyn_hashlinks_w_keyT bsb_hashlink; /*Metadata bsBuf hash table list*/
    bsbuf_pfdatbitmap_t bsb_pfdatbitmap;/* Bitmap of pfdat's */
    uint32_t bsb_magicid;          /* Unique structure validation identifier*/
};

/* BsBuf list header template structure.
 * This structure and the bsBuf structure links and names must match.
 */
struct bsBufHdr {
    struct bsBuf *bsb_metafwd;       /* doubly linked lsn list */
    struct bsBuf *bsb_metabwd;       /* These field names must match bsBuf*/
    int64_t      bsbh_length;        /* length of queue */
};

/* bsBuf structure flags. Protected by bsBuf.bsb_lock. */
#define BSBUFFLG_LSNLISTMARKER   0x01    /* Indicates a bsBuf on a domain */
                                         /* lsnList is a temporary */
                                         /* placeholder for flushing */
#define BSBUFFLG_FLUSHNEEDED     0x02    /* Putpage sets flag when it sets */
                                         /* up a Range Flush link on this */
                                         /* bsbuf due to active pinners */
                                         /* keeping the bsb_writeref up. */
                                         /* Instructs unpinner to flush page.*/
/*
 * List manipulation macros
 */
/* ADVFS_ADD_TAIL_METABUF_LIST
 *
 * Inserts a bsBuf onto the end of the metadata bsbuf linked list.
 *
 * Assumptions: 
 * The list header's head and tail are initialized to
 * to the address of the list header.
 *
 * Spin lock must be held for the insertion and list length update.
 * Domain lsnList insertion must hold domain.lsnLock.
 * Log file dirtyBufList insertion must hold bfAccess.bfIoLock.
 *
 * UpdateCntrFlg argument: set TRUE to have macro increment
 * list length counter. Otherwise, FALSE means the caller of this
 * must increment the length counter.
 */
#define ADVFS_ADD_TAIL_METABUF_LIST(bsbufp, listHdrp, updateCntrFlg) \
{ \
    (bsbufp)->bsb_metafwd = (struct bsBuf *)(listHdrp); \
    (bsbufp)->bsb_metabwd = (listHdrp)->bsb_metabwd; \
    (listHdrp)->bsb_metabwd->bsb_metafwd = (bsbufp); \
    (listHdrp)->bsb_metabwd = (bsbufp); \
    if ((updateCntrFlg)) { \
        (listHdrp)->bsbh_length++; \
    } \
}

/* ADVFS_INSERT_METABUF_LIST
 *
 * Inserts a bsBuf after a "previous" bsBuf link of the metadata
 * bsbuf linked list. This must only be used for inserting 
 * placeholder bsBuf markers by the flush routines. As bsBuf's
 * that represent valid metadata must always be inserted at
 * the tail of the list.
 *
 * Assumptions: 
 * The list header's head and tail are initialized to
 * to the address of the list header.
 *
 * Spin lock must be held for the insertion and list length update.
 * Domain lsnList insertion must hold domain.lsnLock.
 * Log file dirtyBufList insertion must hold bfAccess.bfIoLock.
 *
 * UpdateCntrFlg argument: set TRUE to have macro increment
 * list length counter. Otherwise, FALSE means the caller of this
 * must increment the length counter.
 */
#define ADVFS_INSERT_METABUF_LIST(prevbsbufp, \
                                  insbsbufp, listHdrp, updateCntrFlg) \
{ \
    (insbsbufp)->bsb_metafwd = (prevbsbufp)->bsb_metafwd; \
    (insbsbufp)->bsb_metabwd = (prevbsbufp); \
    (insbsbufp)->bsb_metafwd->bsb_metabwd = (insbsbufp); \
    (insbsbufp)->bsb_metabwd->bsb_metafwd = (insbsbufp); \
    if ((updateCntrFlg)) { \
        (listHdrp)->bsbh_length++; \
    } \
}

/* ADVFS_REM_METABUF_LIST
 *
 * Removes a bsBuf from a file's bsBuf list.
 *
 * Assumptions: 
 * The list header's head and tail are initialized to
 * to the address of the list header.
 *
 * Spin lock must be held for the removal and list length update.
 * Domain lsnList insertion must hold domain.lsnLock.
 * Log file dirtyBufList insertion must hold bfAccess.bfIoLock.
 *
 * UpdateCntrFlg argument: set TRUE to have macro increment
 * list length counter. Otherwise, FALSE means the caller of this
 * must increment the length counter.
 */
#define ADVFS_REM_METABUF_LIST(bsbufp, listHdrp, updateCntrFlg)    \
{                                                                       \
    if ((updateCntrFlg)) {                                              \
        (listHdrp)->bsbh_length--;                                      \
        MS_SMP_ASSERT( (listHdrp)->bsbh_length >= 0);                   \
    }                                                                   \
    (bsbufp)->bsb_metafwd->bsb_metabwd = (bsbufp)->bsb_metabwd;         \
    (bsbufp)->bsb_metabwd->bsb_metafwd = (bsbufp)->bsb_metafwd;         \
    (bsbufp)->bsb_metafwd = (bsbufp)->bsb_metabwd = NULL;               \
}


void
advfs_verify_meta_page( struct bfAccess *bfap, off_t offset, void* addr); /*in */

/* The buf struct flags field must be manilpulated thru macros
 * since it is really composed of 2 seperate 32 bit fields. There
 * currently is not macro to initialize the flags since generally
 * the call to fcache_buf_create will do this. But advfs will create
 * a dummy buf struct that we malloc. In this case we first want to
 * initialize the buf struct flags ourselves
 */

#define ADVFS_INIT_B_FLAGS(_bp) BRESETFLAGS(_bp,(bufflags_t) -1)


#endif /* _BS_BUF_H_ */
