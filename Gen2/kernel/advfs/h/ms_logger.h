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
 *  AdvFS 
 *
 * Abstract:
 *
 *  Contains logger definitions and function prototypes.
 *
 */

#ifndef _MS_LOGGERH_
#define _MS_LOGGERH_

#include <advfs/e_dyn_hash.h>      /* Needed for bs_buf.h */
#ifdef _KERNEL
#include <advfs/bs_buf.h>          /* Needed for bsBufHdr */
#endif

/*
 * Forward declarations.
 */
struct logPg;
struct bfAccess;
struct logDesc;

#define BS_DEF_LOG_PGS   512   /* Max default number of pages per log (  4 MB)*/
#define BS_MIN_LOG_PGS   512   /* Minimum number of pages per log     (  4 MB)*/
#define BS_MAX_LOG_PGS  8192   /* Maximum number of pages per log     ( 64 MB)*/

/*
 ** LOG_FLUSH_THRESHOLD_PGS must be a multiple of 4 **
 */
#define LOG_FLUSH_THRESHOLD_PGS  8


/*
 *** Log Management Macros
 */
#define LASTPG  wrtPgD[0]

#define LAST_PAGE( ldP ) \
    ((((ldP)->nextRec.page + 1) % (ldP)->pgCnt) == (ldP)->firstRec.page)

#define LOG_EMPTY( ldP ) \
    (RECADDR_EQ( (ldP)->nextRec, (ldP)->firstRec ))

/*
 *** Lock macros
 */

/* This needs to be a single line !*/

#define DEFINE_LOCK_FLAGS int flushLocked=0, descLocked=0;

#define DESCRIPTOR_LOCK { \
    ADVMTX_LOGDESC_LOCK(&ldP->descLock); \
    descLocked = 1; \
}

#define FLUSH_LOCK  { \
    ADVMTX_LOGDESC_FLUSH_LOCK(&ldP->flushLock); \
    flushLocked = 1; \
}

#define DESCRIPTOR_UNLOCK { \
    ADVMTX_LOGDESC_UNLOCK(&ldP->descLock); \
    descLocked = 0; \
}

#define RELEASE_DESCRIPTOR_LOCK { \
    if (descLocked) { \
        DESCRIPTOR_UNLOCK; \
    } \
}

#define FLUSH_UNLOCK { \
    ADVMTX_LOGDESC_FLUSH_UNLOCK(&ldP->flushLock); \
    flushLocked = 0; \
}

#define RELEASE_LOCKS { \
    if (descLocked) { \
        DESCRIPTOR_UNLOCK; \
    } \
    if (flushLocked) { \
        FLUSH_UNLOCK; \
    } \
}



static const lsnT nilLSN = { 0 };
static const lsnT firstLSN = { LSN_MIN };
static const lsnT maxLSN = { LSN_MAX };

typedef uint32_t logHT;  /* Handles to open logs are of this type */

/*
 * logRdHT - Handle to an open log read stream.  See lgr_read... for 
 * more info.
 */
typedef struct logRdH {
    struct logDesc *ldP;     /* pointer to the open log */
    int64_t rdH;             /* Handle to the read stream descriptor */
} logRdHT;

/*
 * logWriteModeT - When writing a record via lgr_writev() the following
 * write modes can be specified.
 */
typedef enum { 
    LW_SYNC,        /* Synchronous write (wait for completion) */
    LW_ASYNC        /* Asynchronous write (don't wait for completion) */
} logWriteModeT;

/*
 * logReadModeT - When reading log records via lgr_read() the following
 * read modes can be specified.  lgr_read() will read the specified record
 * return the address of the "next" record based on the read mode.
 */
typedef enum { 
    LR_FWD,     /* Return the next log rec's addr */
    LR_BWD,     /* Return the prev log rec's addr */
    LR_BWD_LINK     /* Return the prev client rec's addr */
} logReadModeT;

typedef struct {
    uint32_t *bufPtr;      /* pointer to data buffer */
    int32_t bufWords;      /* number of words in buffer */
} logBufVectorT;

/*****************************************************************************
 * Logger On-Disk Structures.                                                *
 *****************************************************************************/

/*
 * logBlkT -
 *
 * Defines the structure of a log page block.  The LSN of a page's
 * blocks must match in order for the page to be valid.
 */
typedef struct {
    lsnT lsn;
    char data[ADVFS_LOG_BLKSZ - sizeof( lsnT )];
} logBlkT;

/*
 * logPgBlksT -
 *
 * Defines the physical structure of a log page; an array of blocks.
 */
typedef struct {
   logBlkT blks[ADVFS_LOGBLKS_PER_PAGE];
} logPgBlksT;


/*
 * logRecHdrT -
 *
 * Each log record has a header that describes the record
 * and contains record addresses to neighboring records.  Note that
 * the 'nextRec' record address will never point to a record in another
 * page.  The last record's 'nextRec' record address is set to
 * 'logEndOfRecords' which tells you that you must look in the next
 * page for the next record (if one exists).  Both 'prevRec' and
 * 'prevClientRec' do point to records in other pages.
 */
typedef struct logRecHdr {
    logRecAddrT nextRec;        /* log addr of next log record              */
    logRecAddrT prevRec;        /* log addr of previous log record          */
    logRecAddrT prevClientRec;  /* log addr of client's previous log record */
    logRecAddrT firstSeg;       /* log addr of record's first segment       */
    uint32_t wordCnt;            /* log segment size count in words (inc hdr)*/
    uint32_t clientWordCnt;      /* size of client's record (in words)       */
    lsnT lsn;                   /* log sequence number                      */
    uint32_t segment;            /* 31-bit segment number; if bit 31 is set  */
                                /*    then there are more segments in the   */
                                /*    record.                               */
} logRecHdrT;

#define MORE_SEGS 0x80000000

/*
 * Number words per log record header
 */
#define REC_HDR_WORDS howmany( sizeof( logRecHdrT ), sizeof( uint32_t ) )

/*
 * Number words per ftxDoneLRT record header
 */
#define DONE_LRT_WORDS howmany( sizeof( ftxDoneLRT ), sizeof( uint32_t ) )

/* Total header words for a log record */
#define TOT_HDR_WORDS (REC_HDR_WORDS + DONE_LRT_WORDS)

/*
 * Number words per log page header
 */
#define PG_HDR_WORDS howmany( sizeof( logPgHdrT ), sizeof( uint32_t ) )

/*
 * Max number of records per log page (empty records!).
 */
#define LOG_RECS_PG (DATA_WORDS_PG / TOT_HDR_WORDS)


/*****************************************************************************
 * Logger In-Memory Structures.                                              *
 *****************************************************************************/

/*
 * rdPgDescT -
 *
 * For each open read stream there is one read page descriptor
 * which contains information about the current page being accessed.
 */
typedef struct rdPgDesc {
    int32_t         refed;  /* flag is true if we hold the page referrenced */
    bfPageRefHT refH;       /* page's ref handle */
    struct logPg *pgP;      /* pointer to page */
    uint32_t     num;       /* page's number */
} rdPgDescT;

/*
 * wtPgDescT -
 *
 * Describes the current log page being used by lgr_writev().
 */
typedef struct wtPgDesc {
    unsigned    pinned   :  1; /* true if we hold the page pinned */
    unsigned             : 31;
    bfPageRefHT refH;          /* page's ref handle */
    struct logPg  *pgP;        /* pointer to page */
    uint32_t     num;          /* page's number */
    int16_t      prevLastRec;  /* word offset into pg.data */
    int16_t     writers;
    logRecAddrT lastRec;       /* Log record addr of the last record on */
                               /* on this page */
    
} wtPgDescT;
/*
 * logDescT -
 *
 * Describes an open log.  Note that lgr_writev() appends to the
 * end of the log and trimming is done at the beginning of the log.  Also,
 * 'nextRec' points to where next record will be written and 'firstRec' points
 * to the first record in the log.  If they point to the same rec then
 * the log is empty.  If the current page is full and the next page contains
 * the first record in the log then the log is full.  Moving to a new
 * page (ie- incrementing nextRec.page) may only be done in
 * release_dirty_pg().
 */
#ifdef _KERNEL
typedef struct logDesc {
    spin_t      dirtyBufLock;      /* Protects dirtyBufList, hiFlushLsn, */
                                   /* and writeTargetLsn */
    struct bsBufHdr dirtyBufList;  /* List of modified log buffers */
    lsnT        hiFlushLsn;        /* Highest LSN written to disk so far */
    lsnT        writeTargetLsn;    /* Target LSN to flush for ongoing flushes */
    ADVMTX_LOGDESC_T descLock;     /* Protects most fields in the descriptor */
    struct domain *dmnP  ;         /* Domain pointer for log bitfile */
    struct bfAccess  *logAccP;     /* Ptr to log's bf access struct */
    logRecAddrT nextRec;           /* Log record addr of next rec to write */
    logRecAddrT firstRec;          /* Log record addr of first rec in log */
    logRecAddrT lastRec;           /* Log record addr of the last record */
    logRecAddrT lastRecFirstSeg;   /* Addr of the last record's 1st segment */
    lsnT        quadLsn[2];        /* First lsn in last two quadrants */
    uint32_t    oldestPg;          /* oldest log page (meta data not written) */
    uint32_t    pgCnt;             /* Number of pages in the log */
    unsigned    logWrapStateKnown:1; /* 1 = we know log has wrapped or not */
    unsigned    logWrapped : 1;    /* 1 = log has wrapped */
    unsigned    flushing : 1;      /* 1 = log is being flushed */
    unsigned    switching : 1;     /* 1 = switching logs is in progress */
    unsigned    unused: 28;
    ADVMTX_LOGDESC_FLUSH_T flushLock;/* Protects flushing, wrtPgD, and LASTPG  */
    wtPgDescT   *wrtPgD;           /* Desc for current log end page */
} logDescT;

#define get_wrtPgD_index(_log_page,_pgs_in_log) (_log_page%(_pgs_in_log/4)) + 1;


/*             
 *** Function Prototypes ***
 */

int32_t lgr_lsn_lt( lsnT lsn1, lsnT lsn2, logDescT *ldP );
int32_t lgr_lsn_gt( lsnT lsn1, lsnT lsn2, logDescT *ldP );
int32_t lgr_seq_lt( lsnT lsn1, lsnT lsn2 );
int32_t lgr_seq_gt( lsnT lsn1, lsnT lsn2 );

/*
 * lgr_calc_num_pgs
 *
 * Given a 'requested number of log' this routine returns valid
 * interpretation of this request.  It is used to determine the
 * log size (in pages).  There are several rules (see code) that
 * determine the min and max log pages and whether or not the log
 * must have an integral number of log pages.
 */

bs_meta_page_t
lgr_calc_num_pgs(
    int requestedLogPgs,
    bf_vd_blk_t vdSize );

/*
 * lgr_open - Opens a log.  If the log is being opened for the first time
 * then lgr_open() will allocate the necessary storage for the log.  Otherwise,
 * the log's end and beginning are found and the log descriptor is initialized
 * accordingly.
 * if flag is set, then this is a force mount (the disk can't be
 * mounted). so assume the log is hosed and re-init it.
 */
statusT
lgr_open(
    logDescT  **ldP,            /* out - pointer to the open log */
    logRecAddrT *nextLogRec,    /* out - rec addr of next rec in log */
    struct bfAccess **logbfap,  /* out - access structure of the open log */
    bfTagT logtag,              /* in - the log's tag */
    bfSetT *bfSetp,             /* in - the log's bitfile-set desc ptr */
    uint32_t flag               /* in - if set, re-init log */
    );

/*
 * lgr_close - Close a log.  
 *
 * NOTES:
 *
 *    1) This routine works for the domain log only.  It needs to be
 *   revisited when common logging is implemented.
 */
statusT
lgr_close(
     logDescT  *ldP
     );

/*
 * advfs_lgr_flush - Writes the log's cached pages to disk.
 */
void
advfs_lgr_flush(
     logDescT *ldp,
     lsnT lsn,
     int32_t async
     );

/*
 * lgr_switch_vol
 *
 * Creates a new log on the specified disk and switches the current log
 * handle to point to the new log.  This effectively switches a domains
 * log to a new log.  The old log is deleted.  Optionally, the new log's
 * size can be changed to something different from the old log.  The new
 * log's service class can also be changed.
 */

statusT
lgr_switch_vol(
    logDescT *ldP,        /* in - pointer to an open log */
    vdIndexT newVolIdx,   /* in - move log to this vdIndex */
    uint32_t newNumPgs,   /* in - if > 0 then change log's size */
    serviceClassT logSvc, /* in - if != 0 then change log service class */
    uint64_t  xid         /* in - CFS transaction id */
                               /* TODO - This uint64_t should really be
                                * ftxIdT, but that typedef (in ftx_public.h)
                                * has not been defined yet by the time we
                                * include this file.
                                */
    );

/*
 * lgr_read_open - Opens a log "read stream".  In order to read a log one
 * must first open a read stream.  Each read stream has associated with it
 * descriptor which contains information about the current page(s) being
 * accessed.  The reason read streams exist is because lgr_read() does
 * not actually copy log records from the log to the client's buffer; thereby
 * eliminating costly data copying.  So, lgr_read() returns a pointer into
 * the log page that contains the record.  This means that lgr_read()
 * must keep the page referenced until the client is finished with the
 * record (a subsequent call to lgr_read() or lgr_read_close() tells
 * the logger that it can deref any pages associated with the read stream).
 * So, log streams are a way for the logger to keep track of with pages
 * are references by clients reading the log.
 */
statusT
lgr_read_open(
    logDescT *ldP,   /* in  - log descriptor pointer */
    logRdHT *logrdh  /* out - lgr_read handle */
    );

/*
 * lgr_read - Read a record from the log.  lgr_read() does not copy the
 * record into a client's buffer.  Instead it returns a pointer to the
 * record in the log page; thereby eliminating unneccessary data copying.
 * One must first open a "read stream" via lgr_read_open() before reading
 * the log.  lgr_read() returns the log record address of the next record.
 * The meaning of "next record" is dependent on the read mode specified; see
 * the comments for logReadModeT for a description of the various modes
 * and how they affect the meaning of "next record". 
 */
statusT
lgr_read(
    logReadModeT rd_mode,  /* in  - read mode (fwd, bwd, bwd_link, ...) */
    logRdHT logrdh,        /* in  - lgr_read handle */
    logRecAddrT *rec_addr, /* in/out  - log address of record */
    uint32_t **buf,        /* out - ptr to record data */
    int32_t *buf_words,    /* out - words read */
    int32_t *recWords      /* out - size of record (not just this segment) */
    );

/*
 * lgr_read_close - Closes a log "read stream".
 */
statusT
lgr_read_close( 
    logRdHT logrdh   /* in  - lgr_read handle */
    );

/*
 * lgr_get_last_rec - Returns the log record address of the last 
 * record in the log.
 */
statusT
lgr_get_last_rec(
    logDescT *ldP,
    logRecAddrT *rec_addr
    );

/*
 * lgr_dmn_get_pseudo_first_rec - Returns the address of the record
 * that would be the first log record if the log had not been
 * trimmed.
 */
statusT
lgr_dmn_get_pseudo_first_rec(
    struct domain *dmnP,    /* in - domain pointer */
    logRecAddrT *rec_addr   /* out - log record address */
    );
#endif

#endif /* _MS_LOGGERH_ */
