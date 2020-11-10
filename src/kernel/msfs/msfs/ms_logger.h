/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991                            *
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
 * @(#)$RCSfile: ms_logger.h,v $ $Revision: 1.1.15.2 $ (DEC) $Date: 2000/04/05 19:45:02 $
 */

#ifndef _MS_LOGGERH_
#define _MS_LOGGERH_

struct logPg;
struct bfAccess;
struct logDesc;

/* The maximum number of open read streams is limited to the number of
 * transactions that can be writing to this log.
 * This should be (FTX_DEF_RR_SLOTS + 2), but I can't get that value included
 * here without breaking the world.  If FTX_DEF_RR_SLOTS changes, then change
 * MAX_RD_PGS to the new value plus 2.
 */
#define MAX_RD_PGS (32)

#define BS_DEF_LOG_PGS 512   /* Default number of pages per log */
#define BS_MIN_LOG_PGS 512   /* Minimum number of pages per log */

static const lsnT nilLSN = { 0 };
static const lsnT firstLSN = { LSN_MIN };
static const lsnT maxLSN = { LSN_MAX };

typedef uint32T logHT;  /* Handles to open logs are of this type */

/*
 * logRdHT - Handle to an open log read stream.  See lgr_read... for 
 * more info.
 */
typedef struct logRdH {
    struct logDesc *ldP;     /* pointer to the open log */
    uint32T rdH;            /* Handle to the read stream descriptor */
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
    uint32T *bufPtr;      /* pointer to data buffer */
    int bufWords;         /* number of words in buffer */
} logBufVectorT;

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
    int         refed;  /* flag is true if we hold the page referrenced */
    bfPageRefHT refH;   /* page's ref handle */
    struct logPg *pgP;  /* pointer to page */
    uint32T     num;    /* page's number */
} rdPgDescT;

static const rdPgDescT NilRdPgDesc = NULL_STRUCT;

/*
 * wtPgDescT -
 *
 * Describes the current log page being used by lgr_writev().
 */
typedef struct wtPgDesc {
    unsigned    pinned   :  1; /* true if we hold the page pinned */
    unsigned    repinned :  1; /* true if we pinned, unpinned, pinned pg */
    unsigned    dirty    :  1; /* true if we have written to the page */
    unsigned             : 28;
    bfPageRefHT refH;          /* page's ref handle */
    struct logPg  *pgP;        /* pointer to page */
    uint32T     num;           /* page's number */
    int16T      prevLastRec;   /* word offset into pg.data */
    int16T     writers;
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
typedef struct logDesc {
#ifdef KERNEL
    lock_data_t descLock;          /* Protects all fields in the descriptor */
#endif
    struct domain *dmnP  ;         /* Domain pointer for log bitfile */
    struct bfAccess  *logAccP;     /* Ptr to log's bf access struct */
    logRecAddrT nextRec;           /* Log record addr of next rec to write */
    logRecAddrT firstRec;          /* Log record addr of first rec in log */
    logRecAddrT lastRec;           /* Log record addr of the last record */
    logRecAddrT lastRecFirstSeg;   /* Addr of the last record's 1st segment */
    lsnT        quadLsn[2];        /* First lsn in last two quadrants */
    uint32T     oldestPg;          /* oldest log page (meta data not written) */
    uint32T     pgCnt;             /* Number of pages in the log */
    unsigned    logWrapStateKnown:1; /* 1 = we know log has wrapped or not */
    unsigned    logWrapped : 1;    /* 1 = log has wrapped */
    unsigned    flushing : 1;      /* 1 = log is being flushed */
    unsigned    switching : 1;     /* 1 = switching logs is in progress */
    unsigned    reuseLastPg : 1;   /* 1 = repin last page after flushing */
    unsigned    : 27;
#ifdef KERNEL
    lock_data_t flushLock;          /* Protects flushLsn, flushing, wrtPgD */
#endif
    lsnT        flushLsn;          /* lsn we're flushing to */
    wtPgDescT   *wrtPgD;           /* Desc for current log end page */
    rdPgDescT   *rdPgD[MAX_RD_PGS];/* Info about page being held referenced */
} logDescT;

#define get_wrtPgD_index(_log_page,_pgs_in_log) (_log_page%(_pgs_in_log/4)) + 1;


/*             
 *** Function Prototypes ***
 */

int lgr_lsn_lt( lsnT lsn1, lsnT lsn2, logDescT *ldP );
int lgr_lsn_gt( lsnT lsn1, lsnT lsn2, logDescT *ldP );
int lgr_seq_lt( lsnT lsn1, lsnT lsn2 );
int lgr_seq_gt( lsnT lsn1, lsnT lsn2 );

/*
 * lgr_calc_num_pgs
 *
 * Given a 'requested number of log' this routine returns valid
 * interpretation of this request.  It is used to determine the
 * log size (in pages).  There are several rules (see code) that
 * determine the min and max log pages and whether or not the log
 * must have an integral number of log pages.
 */

int lgr_calc_num_pgs( int requestedLogPgs );

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
    uint32T flag                /* in - if set, re-init log */
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
 * lgr_flush - Writes the log's cached pages to disk.
 */
void
lgr_flush(
     logDescT *ldP
     );

statusT
lgr_flush_start(
     logDescT *ldP,
     int noBlock,
     lsnT lsn
     );

statusT
lgr_flush_sync( 
     logDescT *ldP,
     lsnT lsn
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
    uint32T newNumPgs,    /* in - if > 0 then change log's size */
    serviceClassT logSvc, /* in - if != 0 then change log service class */
    long xid              /* in - CFS transaction id */
    );

/*
 * lgr_writev - Writes a record into the log.  The records are doubly
 * linked (using absolute record addressing for now; we should probably
 * switch to relative record addressing in the future) so that the
 * log can be effeciently scanned forward and backward.  There is also
 * a third link to support backward scanning by clients doing transaction
 * undo actions (ie- the client links records associated with a transaction
 * so that if the transaction is aborted the client can scan the log
 * backwards and read only those records that are associated with the
 * aborted transaction).  The figure below shows a section of the log
 * where records 7, 5, 4, and 1 are linked together via client links.
 *
 *       0     1     2     3     4     5     6     7     8
 *        +-----+-----+-----+-----+-----+-----+-----+-----+-----+
 *        |  *->|  *->|  *->|  *->|  *->|  *->|  *->|  *->|     |
 *        |     |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |
 *        |     |     |     |     |  *  |  *  |     |  *  |     |
 *        +-----+-----+-----+-----+--|--+--|--+-----+--|--+-----+
 *                   ^               | ^   | ^         |
 *                   +---------------+ +---+ +---------+
 *
 * Also see the comments for wtPgDescT for a description of how lgr_writev()
 * improves throughput by using a write page "cache".
 *
 */
statusT
lgr_writev(
     logWriteModeT lw_mode, /* in  - write mode (sync, async, ...) */
     logHT logh,            /* in  - log handle */
     logRecAddrT *rec_addr, /* out - log address of record written */
     logRecAddrT bwd_link,  /* in  - rec addr of client's prev rec */
     logBufVectorT bufv[],  /* in  - buffer vector */
     int bufCnt             /* in  - number of buffers in vector */
     );

statusT
lgr_write(
     logWriteModeT lw_mode, /* in  - write mode (sync, async, ...) */
     logHT logh,            /* in  - log handle */
     logRecAddrT *rec_addr, /* out - log address of record written */
     logRecAddrT bwd_link,  /* in  - rec addr of client's prev rec */
     uint32T *buf,          /* in  - record data buffer */
     int buf_words          /* in  - num words in buffer */
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
         logRdHT *logrdh /* out - lgr_read handle */
         );

/*
 * lgr_dmn_read_open - Identical to lgr_read_open above except it
 * takes a domain handle instead of a log handle as input.
 *
 * This is intended for use by utility programs which need to read the
 * log.
 */
statusT
lgr_dmn_read_open(
                  struct domain *dmnP,    /* in  - domain pointer */
                  logRdHT *logrdh   /* out - lgr_read handle */
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
    logReadModeT rd_mode, /* in  - read mode (fwd, bwd, bwd_link, ...) */
    logRdHT logrdh,       /* in  - lgr_read handle */
    logRecAddrT *rec_addr,/* in/out  - log address of record */
    uint32T **buf,        /* out - ptr to record data */
    int *buf_words,       /* out - words read */
    int *recWords         /* out - size of record (not just this segment) */
    );

/*
 * lgr_read_close - Closes a log "read stream".
 */
statusT
lgr_read_close( 
          logRdHT logrdh   /* in  - lgr_read handle */
          );

#define lgr_end     lgr_get_last_rec
#define lgr_dmn_end lgr_dmn_get_last_rec
#define lgr_beg     lgr_get_first_rec
#define lgr_dmn_beg lgr_dmn_get_first_rec

/*
 * lgr_get_last_rec - Returns the log record address of the last 
 * record in the log.
 */
statusT
lgr_get_last_rec( logDescT *ldP, logRecAddrT *rec_addr  );

/*
 * lgr_dmn_get_last_rec - same as lgr_end except for domain's log
 */
statusT
lgr_dmn_get_last_rec(
            struct domain  *dmnP,   /* in - domain pointer */
            logRecAddrT *rec_addr   /* out - log record address */
            );

/*
 * lgr_get_first_rec - Returns the log record address of the first record 
 * in the log.
 */
statusT
lgr_get_first_rec( logDescT *ldP, logRecAddrT *rec_addr  );

/*
 * lgr_dmn_get_first_rec - same as lgr_beg except for domain's log
 */
statusT
lgr_dmn_get_first_rec(
            struct domain *dmnP,    /* in - domain pointer */
            logRecAddrT *rec_addr   /* out - log record address */
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

#endif /* _MS_LOGGERH_ */
