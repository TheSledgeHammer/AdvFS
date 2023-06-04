/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992, 1993                *
 */
/* 
 * =======================================================================
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
 *
 *
 * Facility:
 *
 *  ADVFS
 *
 * Abstract:
 *
 *  Contains the logger code.
 *
 * Date:
 *
 *  Wed Oct 10 15:13:01 1990
 *
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: ms_logger.c,v $ $Revision: 1.1.243.5 $ (DEC) $Date: 2008/02/12 13:07:09 $"

#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_ods.h>
#include <msfs/ftx_privates.h>
#include <kern/lock.h>
#include <msfs/ms_logger.h>
#include <kern/sched_prim.h>

#define ADVFS_MODULE MS_LOGGER

#define SEG_VALID(seg1,seg2)      ((((seg1) & 0x7FFF)+1) == ((seg2) & 0x7FFF))
#define LSN_VALIDATE(lsn1,lsn2)   ( lsn_gt( lsn1, lsn2, 1<<29 )) ? \
        ((lsn1.num - lsn2.num) < ( LOG_FLUSH_THRESHOLD * LOG_RECS_PG)) : \
        ((lsn2.num - lsn1.num) < ( LOG_FLUSH_THRESHOLD * LOG_RECS_PG))

/* Ftx stats collection can be turned on in dbx by setting FtxStats to 1. */
extern int FtxStats;

static int
lsn_gt(
    lsnT lsn1,
    lsnT lsn2,
    unsigned maxLSNs
    );

static int
lsn_lt(
    lsnT lsn1,
    lsnT lsn2,
    unsigned maxLSNs
    );



/*****************************************************************************
 * Logger On-Disk Structures.                                                *
 *****************************************************************************/
/*
** Because the LOG page consists of several disk sectors and the disk
** sector is the amount that can be written atomically, we must insure
** for each sector that the data written there is correct and not garbage
** left over from a previous write. Each sector has an LSN written at
** the beginning. Since this is written to the disk, to change the
** sector size as perceived by this code would be an on disk change.
** So we define the logger's idea of a sector size here and assert that
** it is still the same as the rest of AdvFS's idea of block size in log_init.
*/
#define LOG_BLKSZ 512

/*
 * logBlkT -
 *
 * Defines the structure of a log page block.  The LSN of a page's
 * blocks must match in order for the page to be valid.
 */
typedef struct {
    lsnT lsn;
    char data[LOG_BLKSZ - sizeof( lsnT )];
} logBlkT;

/*
 * logPgBlksT -
 *
 * Defines the physical structure of a log page; an array of blocks.
 */
typedef struct {
   logBlkT blks[ADVFS_PGSZ_IN_BLKS];
} logPgBlksT;

/*
 * logPgU -
 *
 * Defines the two different views of a log page; 1) the logical view, and
 * 2) the array of blocks.
 */
typedef union {
   logPgBlksT logPgBlks;  /* physical log page structure; array of blocks */
   logPgT logPg;          /* logical log page structure */
} logPgU;

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
    uint32T wordCnt;            /* log segment size count in words (inc hdr)*/
    uint32T clientWordCnt;      /* size of client's record (in words)       */
    lsnT lsn;                   /* log sequence number                      */
    uint32T segment;            /* 31-bit segment number; if bit 31 is set  */
                                /*    then there are more segments in the   */
                                /*    record.                               */
} logRecHdrT;

#define MORE_SEGS 0x80000000

/*
 * Number words per log record header
 */
#define REC_HDR_WORDS howmany( sizeof( logRecHdrT ), sizeof( uint32T ) )

/*
 * Number words per ftxDoneLRT record header
 */
#define DONE_LRT_WORDS howmany( sizeof( ftxDoneLRT ), sizeof( uint32T ) )

/* Total header words for a log record */
#define TOT_HDR_WORDS (REC_HDR_WORDS + DONE_LRT_WORDS)

/*
 * Number words per log page header
 */
#define PG_HDR_WORDS howmany( sizeof( logPgHdrT ), sizeof( uint32T ) )

/*
 * Max number of records per log page (empty records!).
 */
#define LOG_RECS_PG (DATA_WORDS_PG / TOT_HDR_WORDS)

static const wtPgDescT NilWtPgDesc = NULL_STRUCT;
static const logDescT  NilLogDesc  = NULL_STRUCT;

/*
 *** Global Variables
 */

struct lockinfo *ADVlogDescT_descLock_info;
struct lockinfo *ADVflushT_flushLock_info;

/*
 *** Log Management Macros
 */
#define lastPg wrtPgD[0]

#define LAST_PAGE( ldP ) \
    ((((ldP)->nextRec.page + 1) % (ldP)->pgCnt) == (ldP)->firstRec.page)

#define LOG_EMPTY( ldP ) \
    (RECADDR_EQ( (ldP)->nextRec, (ldP)->firstRec ))

/*
 *** Lock macros
 */

/* This needs to be a single line !*/

#define DEFINE_LOCK_FLAGS int flushLocked=0, descLocked=0;

#define DESCRIPTOR_WRITE_LOCK { \
    lock_write( &ldP->descLock ); \
    descLocked = 1; \
}

#define FLUSH_WRITE_LOCK  { \
    lock_write( &ldP->flushLock ); \
    flushLocked = 1; \
}

int AdvfsSpinTryCount = 0;

#define FLUSH_TRY_WRITE_LOCK \
    ( lock_try_write( &ldP->flushLock ) ? (flushLocked = 1) : 0)

/* If lock succeeds, set descLocked to 1 and return non-zero value;
 * otherwise lock is not held so return 0.
 */
#define DESCRIPTOR_TRY_WRITE_LOCK  \
    ( lock_try_write(&ldP->descLock) ? (descLocked = 1) : 0 )

#define DESCRIPTOR_UNLOCK { \
    lock_done(  &ldP->descLock ); \
    descLocked = 0; \
}
#define FLUSH_UNLOCK { \
    lock_done(  &ldP->flushLock ); \
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


/*
 * LSN management routines.
 */

int
lgr_lsn_gt(
    lsnT  lsn1,
    lsnT  lsn2,
    logDescT *ldP
    )
{
    if (ldP == NULL) {
        return (lsn1.num > lsn2.num );
    }

    if (LSN_SIGNED( ldP->nextRec.lsn )) {
        return (lsn1.num > lsn2.num );
    } else {
        return ((int) lsn1.num > (int) lsn2.num );
    }
}


int
lgr_lsn_lt(
    lsnT  lsn1,
    lsnT  lsn2,
    logDescT *ldP
    )
{
    if (ldP == NULL) {
        return (lsn1.num < lsn2.num );
    }

    if (LSN_SIGNED( ldP->nextRec.lsn )) {
        return (lsn1.num < lsn2.num );
    } else {
        return ((int) lsn1.num < (int) lsn2.num );
    }
}


int
lgr_seq_lt(
           lsnT lsn1,
           lsnT lsn2
           )
{
    return lsn_lt ( lsn1, lsn2, 1<<29 );
}


int
lgr_seq_gt(
           lsnT lsn1,
           lsnT lsn2
           )
{
    return lsn_gt( lsn1, lsn2, 1<<29 );
}


/*
 * The following are for internal logger use only.
 */

static lsnT
lsn_add(
    lsnT lsn,
    unsigned amt
    )
{
    unsigned adjAmt = amt * LSN_INCR_AMT;

    if ((LSN_MAX - lsn.num) < adjAmt) {
        /*
         * We are wrapping the lsn so we need to skip (move past) lsn 'zero'.
         */
        adjAmt += LSN_INCR_AMT;
    }

    lsn.num += adjAmt;
    return( lsn );
}

/*
 * lsn_gt
 *
 * For use by find_log_end_pg() only.  This version uses the
 * 'magic lsn range' to determine whether signed or unsigned
 * lsn compares are appropriate.
 */

static int
lsn_gt(
    lsnT lsn1,
    lsnT lsn2,
    unsigned maxLSNs
    )
{
    if (LSN_EQ_NIL( lsn1 )) {
        return FALSE;
    } else if (LSN_EQ_NIL( lsn2 )) {
        return TRUE;
    } else if ((lsn1.num < maxLSNs) || (lsn1.num > -maxLSNs)) {
        /* Must use signed compares */
        return( (int) lsn1.num > (int) lsn2.num );
    } else {
        return( lsn1.num > lsn2.num );
    }
}

/*
 * lsn_lt
 *
 * For use by find_log_end_pg() only.  This version uses the
 * 'magic lsn range' to determine whether signed or unsigned
 * lsn compares are appropriate.
 */

static int
lsn_lt(
    lsnT lsn1,
    lsnT lsn2,
    unsigned maxLSNs
    )
{
    if (LSN_EQ_NIL( lsn2 )) {
        return FALSE;
    } else if (LSN_EQ_NIL( lsn1 )) {
        return TRUE;
    } else if ((lsn1.num < maxLSNs) || (lsn1.num > -maxLSNs)) {
        /* Must use signed compares */
        return( (int) lsn1.num < (int) lsn2.num );
    } else {
        return( lsn1.num < lsn2.num );
    }
}


/*
 * Function prototypes for forward references
 */
static statusT release_dirty_pg( logDescT *ldP);
static statusT get_clean_pg( logDescT *ldP);
static void log_flush( logDescT *ldP);
static void log_flush_sync( logDescT *ldP, lsnT lsn);


/*
 * Log Page Consistency -
 *
 * Since log pages (typically) are larger than the atomic write
 * size of the disks that the pages are stored on we have a
 * potential page consistency problem if a page is being written
 * to the disk when a system failure occurs.  Basically, if a page
 * consists of N atomic units (from the disks perspective) then it is
 * possible that less than N atomic units will be written to the
 * disk during a system failure or power outage.  The problem is
 * not necessarily that all N units didn't get written.  The bad
 * part is that the logger must be able to detect that they
 * didn't all get written.  The reason that it is okay to lose the
 * page is because by definition the page can't be representing
 * committed transaction because a transaction cannot be committed
 * until all of its log pages have been written to the disk. Since
 * the log page in question was not written successfully, the logger could
 * not have returned to the client and the client could not assume the
 * transaction is committed.  Therefore, all we care about is detecting
 * that the page is incomplete (and picking a different page for the log end).
 *
 * To solve this problem the logger puts a unique page ID in each
 * block of the corresponding log page.  Then, when it reads a log
 * page it checks to see if the IDs in each block match each other.
 * If they don't then the logger knows the page is bad.
 *
 * This technique has one nasty drawback, it means that the logger
 * has to use fairly complex loops in lgr_writev() to not use the
 * portion of each block that is reserved for the ID.  To get around
 * this, the logger ignores the IDs when writing to a log page until
 * the page needs to be unpinned.  Just before unpinning a log page,
 * the logger writes the ID into each block.  However, before it
 * writes the ID into a block it first copies the value of the
 * area where the ID is to be written to a reserved "save area".  That
 * way the logger isn't overwritting (and losing) log data.  Later,
 * when the log page is read (referenced) the logger overwrites the
 * IDs with the original values (obtained from the page's "save area").
 *
 * The following diagrams illustrate a page with two blocks.  The first
 * diagram shows the page before it is transformed into to a "safe page".
 * Note that the "save area" is kept in the page's trailer (which is in
 * the last block of each page).
 *
 *     |<    blk 0   >|<        blk 1            >|
 *                                  |< save area >|
 *     +--------------+---------------------------+
 *     |  val0 | .... | val1 | .... #### ####     |
 *     +--------------+---------------------------+
 *
 * The second diagram shows the page after it has been transformed.  The
 * values that are overwritten with the ID are in the page's "save area".
 *
 *     |<    blk 0   >|<        blk 1            >|
 *                                  |< save area >|
 *     +--------------+---------------------------+
 *     |  ID   | .... | ID   | .... val0 val1     |
 *     +--------------+---------------------------+
 *
 * The logger uses the page's first record's LSN as the page ID.
 *
 * REWRITING LOG PAGES
 *
 * When the logger is asked to perfrom a synchronous log write it
 * has to flush the log upto the current log page and wait for the
 * flush to complete.  Given that log pages are relatively large (typically
 * 8k) this could result in wasted log space if every sync log write
 * moved to the next log page.  Why move to the next log page?  Because,
 * in general, it is dangerous to rewrite a log page that contains
 * committed transaction records.  In other words, the rewrite could
 * be incomplete if the system crashed and this could invalidate the
 * page (using the scheme discussed above).
 *
 * So, one is left with two choices: 1) never rewrite a log page (until
 * the log is trimmed of course); 2) figure out a reliable way of rewriting
 * log pages so that committed transaction data (the records that have
 * already been written) can't be lost.  The MegaSafe logger takes the
 * latter approach.
 *
 * The basic idea is that each log page block has, in addition to a an ID,
 * a version (or check bit).  The version is set to either a zero or a one.
 * Each block's version must match if all the blocks were written successfully.
 * After each write the version is toggled.  Also, each log page keeps
 * track of the offsets to the current last log record and the previous
 * last log record.
 *
 * Given the page version it is easy to detect which blocks were written
 * successfully.  The last rec offsets are used to determine the real
 * last record in the log.  This is done by checking the page version
 * of the blocks between the the prev and cur last record offset.  If
 * these blocks are okay then the current last record offset is correct.
 * Otherwise, the previous last record offset is correct.
 *
 * The logger uses bit 31 of the LSN for the version (hence LSNs are only
 * 31 bits for now).
 */


/*
 * lgr_make_pg_safe -
 *
 * Puts the page's first record's LSN into each block in the page (except
 * the first block which already has the LSN in it).  The value overwritten
 * by the LSN is saved in the "lsnOverwriteVal" array of the page's
 * trailer.  Also, sets the 'check bit' (version) of each block.
 */

static void
lgr_make_pg_safe(
    logDescT *ldP,     /* in - ptr to log desc */
    logPgT *pgp        /* in - ptr to a log page */
    )
{
    int b;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;
    bfDomainIdT domainId = ldP->logAccP->dmnP->domainId;

    /*
     * Put some page id stuff in the header and trailer so we
     * can identify this page as a log page in the future.
     */

    if (ldP->logAccP->dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
        bfDomainIdT nilDomainId = NULL_STRUCT;

        if (!BS_UID_EQL(ldP->logAccP->dmnP->dualMountId, nilDomainId)) {
            domainId = ldP->logAccP->dmnP->dualMountId;
        }
    }

    pgp->hdr.pgType = BFM_FTXLOG;
    pgp->hdr.dmnId = domainId;
    pgp->hdr.pgSafe = FALSE;

    pgp->trlr.pgType = BFM_FTXLOG;
    pgp->trlr.dmnId = domainId;

    for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
        /*
         * Put page lsn into each block.
         */

        pgp->trlr.lsnOverwriteVal[b - 1] = logPgBlks->blks[b].lsn;
        logPgBlks->blks[b].lsn = pgp->hdr.thisPageLSN;

        /*
         * Set check bit (version) of each block.
         */

        if (pgp->hdr.chkBit) {
            /* Set check bit in LSN; bit 0 */
            logPgBlks->blks[b].lsn.num |= 0x00000001;
        } else {
            /* Clear check bit in LSN; bit 0 */
            logPgBlks->blks[b].lsn.num &= 0xfffffffe;
        }
    }

    pgp->hdr.pgSafe = TRUE;
}


/*
 * lgr_valid_blk
 *
 * Returns TRUE if the block was written at the same time as the
 * first block in the log page; FALSE if not.
 */

static int
lgr_valid_blk(
    logPgT *pgp,       /* in - ptr to a log page */
    int b              /* in - block to validate */
    )
{
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;

    if (pgp->hdr.chkBit) {
        /*
         * hdr indicates all lsn chkbits should be set.
         */

        if ((logPgBlks->blks[b].lsn.num & 0x00000001) != 0x00000001) {
            /** Bummer, found a clear check bit. **/
            return FALSE;
        }

    } else {
        /*
         * hdr indicates all lsn chkbits should be clear.
         */

        if ((logPgBlks->blks[b].lsn.num & 0x00000001) == 0x00000001) {
            /** Bummer, found a set check bit. **/
            return FALSE;
        }
    }
    return TRUE;
}


/*
 * lgr_repair_pg -
 *
 * Given a log page that is valid (all blocks have the same LSN) but has
 * blocks that were not written at the same time (one or more blocks have
 * an incorrect check bit (version)), this routine will determine which
 * record is the real last record in the page.  It will also set this
 * record to look like the last record (ie- it sets its next rec field
 * to nil).
 *
 * This routine should be called only from lgr_restore_pg().
 */

static void
lgr_repair_pg(
    domainT *dmnp,      /* in - domain for domain panic */
    logPgT *pgp         /* in - ptr to a log page */
    )
{
    uint32T validEndRec, startBlk, endBlk;
    logRecHdrT *recHdr;
    int b, badChkBits = FALSE;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;


    MS_SMP_ASSERT(pgp->hdr.pgSafe);

    if (pgp->hdr.prevLastRec == -1) {
        /*
         * The first rewrite of the page failed and the first
         * block in the page was not rewritten.  Therefore, the
         * curLastRec offset is correct.
         */
        validEndRec = pgp->hdr.curLastRec;

    } else if (pgp->hdr.prevLastRec == pgp->hdr.curLastRec) {
        domain_panic(dmnp, "lgr_repair_pg: prev == last");

    } else {
        /*
         * Using the last rec offsets figure out the range of blocks
         * that we need to validate.
         */

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.prevLastRec];

        startBlk = (pgp->hdr.prevLastRec + recHdr->wordCnt + PG_HDR_WORDS)
                                       /
                   (LOG_BLKSZ / sizeof( uint32T ));

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.curLastRec];

        endBlk = (pgp->hdr.curLastRec + recHdr->wordCnt + PG_HDR_WORDS)
                                       /
                   (LOG_BLKSZ / sizeof( uint32T ));

        if ((startBlk == 0) && (startBlk == endBlk)) {
            /*
             * Only the first block has records so the page is okay.
             */
            validEndRec = pgp->hdr.curLastRec;

        } else {
            if (startBlk == 0) {
                startBlk++;
            }

            /*
             * Verify that the end blk is okay.  It has to be since it contains
             * the trailer and the LSN save area.
             */

            badChkBits = !lgr_valid_blk( pgp, ADVFS_PGSZ_IN_BLKS - 1 );

            /*
             * Now verify the blocks between the prev last rec and the
             * cur last rec (including the cur last rec).
             */

            for (b = startBlk; (b <= endBlk) && !badChkBits; b++) {
                badChkBits = !lgr_valid_blk( pgp, b );
            }

            if (badChkBits) {
                validEndRec = pgp->hdr.prevLastRec;
            } else {
                validEndRec = pgp->hdr.curLastRec;
            }
        }
    }

    /* restore the page */

    for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
        logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
    }

    if (validEndRec == pgp->hdr.prevLastRec) {
        /* Set the prev last rec to be the last rec */

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.prevLastRec];
        recHdr->nextRec = logEndOfRecords;
    }
}


/*
 * lgr_complete_pg
 *
 * Returns true if all the blocks in a rewritten log page made it
 * to disk.  The caller must call lgr_valid_pg() before calling this routine.
 */

static int
lgr_complete_pg(
    logPgT *pgp          /* in - ptr to a log page */
    )
{
    int b, badChkBits = FALSE;

    if (pgp->hdr.pgSafe) {
        /*
         * This check makes sense only if the page is
         * still in the 'safe page' format.
         */

        for (b = 1; (b < ADVFS_PGSZ_IN_BLKS) && !badChkBits; b++) {
            badChkBits = !lgr_valid_blk( pgp, b );
        }
    }
    return !badChkBits;
}


/*
 * lgr_restore_pg -
 *
 * Basically the reverse of lgr_make_pg_safe().  It copies back the saved
 * values over the top of each block's LSN.
 *
 * It is assumed that the caller has already validated the page via
 * lgr_valid_pg() before calling this routine!!!
 */

static void
lgr_restore_pg(
    domainT *dmnp,      /* in - domain for domain panic in lgr_repair_pg */
    logPgT *pgp         /* in - ptr to a log page */
    )
{
    int b;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;

    if (pgp->hdr.pgSafe) {
        /*
         * The page restoration makes sense only if the page is
         * still in the 'safe page' format.
         */
        
        /* ???? At this point could we test for !recovery and skip 
         * lgr_complete_pg( pgp ) and lgr_repair_pg( pgp )?
         *
         * Only during recovery should we be concerned with the data
         * in the log file being incomplete. After domain ACTIVATION
         * all data in the log file should be fine?
         */

        if (!lgr_complete_pg( pgp )) {
            /*
             * During the rewrite of the page the system must have crashed so
             * not all of the page's blocks (sectors) where written
             * to the disk (this is detected by the mismatched LSN
             * check bits in each block).  So, we need to repair the page.
             * At worst, this means putting it back to the state it was
             * before the last (failing) write.
             */

            lgr_repair_pg( dmnp, pgp );
        } else {
            /*
             * Page is okay.  Restore it to 'non-safe' (useable) format.
             */

            for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
                logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
            }
        }

        pgp->hdr.pgSafe = FALSE;
    }
}


/*
 * lgr_restore_rec -
 * This will only restore the blocks that the passed in record
 * overlap. It is assumed that the passed in page was at one time
 * brought in via lgr_refpg and therefor was checked for valid and
 * repaired if necessary. Only the record will have values restored
 * to it.
 * 
 */

static void
lgr_restore_rec( 
    logPgT *pgp,         /* in - ptr to a log page */
    uint32T *recp,      /* in - ptr to record. Patch this record */
    uint32T offset,     /* in - offset into log page */
    uint32T size       /* in - size of record in bytes */
    )
{ 
    int idx;
    uint32T *fix_st;   /* Start location in recp to patch */
    uint32T *fix_ed;   /* End of the record */
    uint32T blk_offset = offset & (LOG_BLKSZ - 1);

    /* The log descLock protects the pgSafe field and should be held
     * whenever this routine is called
     */

    if (pgp->hdr.pgSafe) 
    {
        /*
         * The page restoration makes sense only if the page is
         * still in the 'safe page' format.
         */
        MS_SMP_ASSERT(offset > 0);
        
        if ( blk_offset == 0 ) {
            /* offset is right on a block boundary */
            fix_st = recp;
            idx = (offset / LOG_BLKSZ) -1;
        } else {
            /* Round up to nearest block  */
            fix_st = (uint32T*)((char*)recp + LOG_BLKSZ - blk_offset);
            idx = offset / LOG_BLKSZ;
        }

        fix_ed = (uint32T*)((char*)recp + size);

        while(fix_st < fix_ed)
        {
            /* lsnOverwriteVal has ADVFS_PGSZ_IN_BLKS-1 (15) entries. */
            /* Block 0 in a log page does not have its first "int" */
            /* overwritten and therefore it is not saved in lsnOverwriteVal. */
            /* lsnOverwriteVal[0] is for block 1 (second block). */

            *(lsnT *)fix_st = pgp->trlr.lsnOverwriteVal[idx];
            idx++;
            fix_st = (uint32T*)((char*)fix_st + LOG_BLKSZ);
        }
    }
}


/*
 * lgr_valid_pg -
 *
 * Returns true if the page is valid, false if it is not valid.  Page
 * validity is determined by checking the LSN of each block.  They
 * must all match the page's thisPageLSN.
 */

static int
lgr_valid_pg(
    bfAccessT *logAccP,  /* in - log's bitfile access structure */
    logPgT *pgp          /* in - ptr to a log page */
    )
{
    int b;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;
    bfDomainIdT domainId = logAccP->dmnP->domainId;

    if (logAccP->dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
        bfDomainIdT nilDomainId = NULL_STRUCT;

        if (!BS_UID_EQL(logAccP->dmnP->dualMountId, nilDomainId) ) {
            domainId = logAccP->dmnP->dualMountId;
        }
        else {
            goto check_safe;
        }
    }

    if (pgp->hdr.pgSafe &&
        ((pgp->hdr.pgType != BFM_FTXLOG) ||
         (pgp->trlr.pgType != BFM_FTXLOG) ||
         !BS_UID_EQL(pgp->hdr.dmnId, pgp->trlr.dmnId))) {
        /*
         ** This doesn't even look like a log page!!!.
         */
        return FALSE;
    }

check_safe:
    if (pgp->hdr.pgSafe) {
        /*
         * The page validity check makes sense only if the page is
         * still in the 'safe page' format.
         */

        for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
            if (pgp->hdr.thisPageLSN.num !=
                (logPgBlks->blks[b].lsn.num & 0xfffffffe)) {
                return FALSE;
            }
        }
    }

    return TRUE;
}


/*
 * lgr_refpg -
 *
 * Provides an interface to bs_refpg() which also ensures that the page
 * is valid.  It also restores log page from the 'safe' format.
 */

static statusT
lgr_refpg(
    bfPageRefHT *refH,          /* out - log page ref handle */
    logPgT **pgP,               /* out - ptr to log page */
    bfAccessT *bfap,            /* in - log's bitfile access structure */
    unsigned long pg,           /* in - log page number to ref */
    bfPageRefHintT refHint      /* in - cache hint */
    )
{
    statusT sts;

    sts = bs_refpg( refH, (void*) pgP, bfap, pg, refHint );
    if (sts != EOK) {
        return( sts );
    }

    if (!lgr_valid_pg( bfap, *pgP )) {
        /*
         * The page is corrupt.  Deref it and return an error.
         */
        (void) bs_derefpg( *refH, BS_CACHE_IT );
        return( E_CANT_ACCESS_LOG );
    }

    /*
     * The page is valid so restore it to a "usable" form.
     */
    lgr_restore_pg( bfap->dmnP, *pgP );

    return EOK;
}


/*
 * lgr_unpinpg -
 *
 * Provides an interface to bs_unpinpg() which makes sure the
 * page is put into it's safe format before it is unpinned (dirty).
 */

static statusT
lgr_unpinpg(
    logDescT *ldP,
    bfPageRefHT refH,            /* in - log page ref handle */
    logRecAddrT logRec,          /* in - highest LSN (rec addr) in page */
    wtPgDescT *logPg             /* in - ptr to log page write descriptor*/
    )
{
    statusT sts;
    static const bsUnpinModeT bsLogNoMod = { BS_LOG_NOMOD, BS_LOG_PAGE };


    /* Note: Log pages are no longer repinned. This bit and the associated
     * processing can be removed. This may consitute and on-disk change.
     */


    if (logPg->dirty && logPg->repinned) {
        logPg->pgP->hdr.chkBit      = !logPg->pgP->hdr.chkBit;
        logPg->pgP->hdr.prevLastRec = logPg->prevLastRec;
    }

    lgr_make_pg_safe( ldP,logPg->pgP );

    if (logPg->dirty) {
        sts = bs_unpinpg( refH, logRec, BS_LOG );
    } else {
        sts = bs_unpinpg( refH, logRec, bsLogNoMod );
    }

    logPg->pgP    = NULL;
    logPg->dirty  = FALSE;
    logPg->pinned = FALSE;
    
    return sts;
}


/*
 * add_lsn_list
 *
 * Add a buffer to the tail of the domain's lsn list.
 *
 * SMP:  Caller must have seized dmnP->lsnLock.
 */

static void
add_lsn_list(
             struct bsBuf *bp,  /* in */
             domainT* dmnP      /* in */
             )
{
    struct bsBufHdr *hp;

    MS_SMP_ASSERT(SLOCK_HOLDER(&dmnP->lsnLock.mutex));

    hp = &dmnP->lsnList;

    bp->lsnBwd = hp->lsnBwd;
    bp->lsnFwd = (struct bsBuf *)hp;
    hp->lsnBwd->lsnFwd = bp;
    hp->lsnBwd = bp;

    if ( hp->length == 0 ) {
        /*
         * The queue is currently empty, this element makes it
         * non-empty, so set the dirty buffer log record address.
         */
        ftx_set_dirtybufla( dmnP, bp->origLogRec );
    }

    hp->length++;
}


/*
 * lgr_writev -
 *
 * Writes a record into the log.  The records are doubly
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
 *               0     1     2     3     4     5     6     7     8
 *            +-----+-----+-----+-----+-----+-----+-----+-----+-----+
 *            |  *->|  *->|  *->|  *->|  *->|  *->|  *->|  *->|     |
 *            |     |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |<-*  |
 *            |     |     |     |     |  *  |  *  |     |  *  |     |
 *            +-----+-----+-----+-----+--|--+--|--+-----+--|--+-----+
 *                       ^               | ^   | ^         |
 *                       +---------------+ +---+ +---------+
 *
 * The parameters 'bufv' and 'bufCnt' are used to pass the pieces of the
 * record to be written to the log.  Basically, this provices a "gather
 * write" interface.  It allows the caller to gather several buffers
 * into one log record.  lgr_writev concatenates each buffer described
 * by 'bufv' into a single log record.
 */


/*
 * Slightly specialized interface for writing ftx log records, the
 * only user of the logger in version 1.
 */
statusT
lgr_writev_ftx(
    ftxStateT* ftxp,      /* in/out - ftx state */
    domainT* dmnP,        /* in - domain state */
    lrDescT* lrdp,        /* in - ftx log record descriptor */
    logWriteModeT lwMode  /* in  - write mode (sync, async, ...) */
    )
{
    logRecHdrT recHdr, *prevHdrP;
    logRecAddrT prevRec, firstSeg;
    uint32T *dataP;
    uint32T segWdsToWrite = 0, totWdsWritten = 0, segment = 0;
    uint32T bufWdsWritten = 0, wdsToWrite, wdsWritten;
    logDescT *ldP;
    statusT sts;
    int wd, buf = 0, bufWords = 0;
    logBufVectorT* bufv = lrdp->bfrvec;
    int bufCnt = lrdp->count;
    char *log_record_addr;
    logRecAddrT current_rec;
    uint16T current_rec_offset;
    uint32T current_log_page;
    perlvlT* clvlp = &ftxp->cLvl[ftxp->currLvl];
    struct bsBuf *bp;
    int pli;
    logRecAddrT oldestftxla, dirtybufla;

    DEFINE_LOCK_FLAGS;


    if (bufCnt <= 0) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    /*
     * Calculate total number of words that will be in the log record
     */
    for (buf = 0; buf < bufCnt; buf++) {
        bufWords += bufv[buf].bufWords;
    }

    if (bufWords <= 0) {RAISE_EXCEPTION( EBAD_PARAMS );}
    ldP = dmnP->ftxLogP;
    if (ldP == NULL)
        {RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );}

    if ( AdvfsSpinTryCount ) {
        int try_count = AdvfsSpinTryCount;
        while( DESCRIPTOR_TRY_WRITE_LOCK == 0) {
            if( try_count-- == 0 ) {
                DESCRIPTOR_WRITE_LOCK;
                break;
            }
        }
    } else {
        DESCRIPTOR_WRITE_LOCK;
    }

    dmnP->logStat.transactions++;
    if ( bufWords > dmnP->logStat.maxFtxWords ) {
        dmnP->logStat.maxFtxWords = bufWords;
        dmnP->logStat.maxFtxAgent = ftxp->lrh.agentId;
    }

    buf = 0; /* start with buffer zero in buffer vector */
    prevRec = ldP->lastRecFirstSeg;

    /*
     * The following loop is for reserving space in the log. We need
     * to keep log records that comprise multiple segments contiguous
     * so we must pin all the log pages that we will need. Once we have
     * reserved the space we can let go of the locks and let others in.
     */

    while (totWdsWritten < bufWords) 
    {

        /* We must obtain the FLUSH write lock to protect the fields in the
         * descriptor that govern which log pages are being written.
         */
        if( AdvfsSpinTryCount ) {
            int try_count = AdvfsSpinTryCount;
            while( FLUSH_TRY_WRITE_LOCK == 0 ) {
                if( try_count-- == 0 ) {
                    FLUSH_WRITE_LOCK;
                    break;
                }
            }
        } else {
            FLUSH_WRITE_LOCK;
        }
        
        /* Never allow a record to cross a page boundary unless the record */
        /* (plus header) is larger than a page. 
         *  NOTE: The ftxDoneLRT which is present in the segment 0 records
         *        is actually included in bufWords
         * 
         * We will unconditionally move off this page if a flush is currently
         * outstanding. We used to sync up and reuse the page to save log space?
         * but performance numnbers show that moving on is better. I'm
         * unsure of the effect on the log filling up too quickly. We could
         * as an optimization consider waiting based on how much if the current
         * page is unused.
         */

        if ( (bufWords + REC_HDR_WORDS > DATA_WORDS_PG - ldP->nextRec.offset )  ||
             (ldP->flushing))
        {
            /* There is not enough room left on the LOG page for this record. */

            if ( (bufWords + REC_HDR_WORDS <= DATA_WORDS_PG) ||
                 (TOT_HDR_WORDS > DATA_WORDS_PG - ldP->nextRec.offset ) ||
                 (ldP->flushing))
            {
                /* The record would fit on the next page OR */
                /* first segment needs room for logRecHdrT & ftxDoneLRT OR */
                /* subsequent segments only need room for logRecHdrT. 
                 * If only the header would fit then move on to the next
                 * page*/

                /* NOTE other than the first time through the log page is 
                 * actually not unpinned here since the writers count is up
                 */

                sts = release_dirty_pg( ldP );
                if ( sts != EOK ) {
                    RAISE_EXCEPTION( sts );
                }
                
                /* Clear the flusing bit to indicate that the last page of
                 * the log is now available.
                 */
                ldP->flushing = FALSE;
            }
        }

        if (!ldP->lastPg.pinned) {
            sts = get_clean_pg( ldP );
            if (sts != EOK) {RAISE_EXCEPTION( sts );}
        }
        
        ldP->lastPg.dirty = TRUE;
        ldP->lastPg.writers++;

        if (segment == 0)
        {
            /* This is the starting point for the record in the log
             * page. Save this off for actually moving the data.  
             */

            current_rec = ldP->nextRec;
            current_rec_offset = ldP->nextRec.offset;
            current_log_page = ldP->lastPg.num;
            log_record_addr = (char *) &ldP->lastPg.pgP->data[ldP->nextRec.offset];
        }

        /* We have the last log page in place. We may now allow writers to 
         * unpin any old log pages.
         */

        FLUSH_UNLOCK;
        
        segWdsToWrite = MIN( bufWords - totWdsWritten,
                             DATA_WORDS_PG - ldP->nextRec.offset - REC_HDR_WORDS );
        
        
        if ((ldP->lastRec.page == ldP->nextRec.page) &&
            !LSN_EQ_NIL( ldP->lastRec.lsn )) 
        {
            prevHdrP = (logRecHdrT *)
                &ldP->lastPg.pgP->data[ldP->lastRec.offset];
            prevHdrP->nextRec = ldP->nextRec;
        }
        
        ldP->lastRec = ldP->nextRec;
        ldP->lastPg.pgP->hdr.curLastRec = ldP->lastRec.offset;
        
        /* It is important that we initialize our nextRec while holding
         * the log descriptor lock. Since we update the prev recs next rec
         * to point to us and this may be done to our nextrec too.
         */
        
        ((logRecHdrT *) &ldP->lastPg.pgP->data[ldP->nextRec.offset])->nextRec = logEndOfRecords;
        
        if (segment == 0) {firstSeg = ldP->nextRec;}

        ldP->nextRec.offset += (REC_HDR_WORDS + segWdsToWrite);
        ldP->lastRecFirstSeg = firstSeg;
        
        LSN_INCR( ldP->nextRec.lsn );
        
        totWdsWritten       += segWdsToWrite;
        segment++;
        
    }

    /*
     * Set up ftx crash recovery address for this record, as
     * well as potentially set first log record address for
     * this ftx, and the oldest ftx log record address.
     */

    /**********************************************************************
     *   The following section of codes appears to need the logDesc
     *   lock held around it. I attempted to leave this unguarded 
     *   and saw some peculiar panics. Unfortunately I did not have
     *   the time to understand exactly why we must hold the lock.
     *   The performance was not greatly affected either way so It
     *   did not seem fruitful going much farther.
     *
     *   It was necessary to hold the bsBuf lock around the code to
     *   set up bp->currentLogRec and compare that we were not 
     *   putting in a smaller lsn when not holding the log descriptor lock.
     *
     **********************************************************************/

    ftxp->lastLogRec = current_rec;

    /*
     * Because of the locking sequence, it is necessary to read the
     * oldestftxla before the dirtybufla.
     */

    if ( LSN_EQ_NIL( ftxp->firstLogRecAddr.lsn ) ) {
        oldestftxla = ftx_set_firstla( ftxp, dmnP, current_rec );
    } else {
        oldestftxla = ftx_get_oldestftxla( dmnP );
    }

    /*
     * The oldest buffer log record address also needs to be
     * set while we're under the log descriptor lock.  This
     * keeps the maintenance of the list simple, as we know
     * this is the highest possible lsn in existence right
     * now.
     *
     * Scan the list of buffers pinned by this subtransaction,
     * and if they haven't already had the original lsn
     * assigned, add them to the lsn list.  Always reset the
     * highest lsn associated with the buffer page.
     *
     * These values are only set here, and are serialized by
     * the log desc lock.  The i/o system only looks at them
     * or modifies the lsn list when an i/o completes, which
     * cannot be the case here because these are all pinned by
     * this subtransaction, at least.
     */

    for ( pli = clvlp->lastPinS; pli >= 0; pli -= 1 ) {
        lvlPinTblT* plpp = &clvlp->lvlPinTbl[ pli ];
        ftxPinTblT* fpp;
        
        if ( plpp->ftxPinS < 0 ) {
            continue;      /* negative means this slot not in use */
        } else {
            fpp = &ftxp->pinTbl[ plpp->ftxPinS ];
        }

        if ( (plpp->numXtnts == 0) ||
             (fpp->unpinMode.rlsMode == BS_NOMOD) ) {
            continue;    /* don't unpin at this level */
        }

        /* Extract the log record information from the buffer header.
         * Normally only the buffer cache routines
         * manipulate the bsBuf fields.
         */
        bp = (struct bsBuf *)fpp->pgH;
        if ( LSN_EQ_NIL( bp->origLogRec.lsn ) ) {
            
            /* protect the changing of origLogRec with the dmnP->lsnLock
             * to make locking more explicit.  This value will not
             * change while the buffer is on the lsn list, so seizing
             * the lsnLock guarantees the values of origLogRec for all
             * buffers on that list.
             */
            mutex_lock( &dmnP->lsnLock );
            bp->origLogRec = current_rec;
            add_lsn_list( bp, dmnP );
            mutex_unlock( &dmnP->lsnLock );
        }

        bp->currentLogRec = current_rec;
    }
    
    /*
     * Now actually pick up the oldest dirty buffer log record
     * address, which may or may not have changed as a result
     * of the above scan for this subtransaction.
     */
    
    dirtybufla = ftx_get_dirtybufla( dmnP );

    if ( LSN_EQ_NIL( dirtybufla.lsn ) ) {

        ftxp->lrh.crashRedo = oldestftxla;
    } else if ( LSN_LT( dirtybufla.lsn, oldestftxla.lsn )) {

        ftxp->lrh.crashRedo = dirtybufla;
    } else {

        ftxp->lrh.crashRedo = oldestftxla;
    }

    /* We have reserved our spot in the log. Now we are free to let other
     * writers in to claim their space.
     */
    DESCRIPTOR_UNLOCK;

    totWdsWritten = 0;
    segment = 0;

    /* Now we can copy the log records onto the space that
     * we reserved previously.
     */

    while (totWdsWritten < bufWords) 
    {

        /* Calc size of current record segment */
        segWdsToWrite = MIN( bufWords - totWdsWritten,
                        DATA_WORDS_PG - current_rec_offset - REC_HDR_WORDS );

        /* segWdsToWrite counts ftxDoneLRT but does not count logRecHdrT. */
        /* Make sure we are writing at least the ftxDoneLRT header. */

        MS_SMP_ASSERT(segment > 0 || segWdsToWrite >= DONE_LRT_WORDS);

        /* Setup log record header */

        recHdr.prevRec = prevRec;
        recHdr.prevClientRec = ftxp->undoBackLink;
        recHdr.firstSeg = firstSeg;
        recHdr.wordCnt = REC_HDR_WORDS + segWdsToWrite;
        recHdr.clientWordCnt = bufWords;
        recHdr.lsn = current_rec.lsn;
        recHdr.segment = segment;

        if ((totWdsWritten + segWdsToWrite) < bufWords) {
            dmnP->logStat.segmentedRecs++;
            recHdr.segment |= MORE_SEGS;
        }

        /* Copy record header to log page */

        /* The nextRec is the first field in the record header and we
         * needed to initialize it while holding the log descriptor
         * lock (this was done previously). We do not want to over
         * write it now since we are no longer protected by the lock 
         */

        bcopy( (char *) &recHdr + sizeof(recHdr.nextRec),
                  (char *) log_record_addr + sizeof(recHdr.nextRec),
                  REC_HDR_WORDS * sizeof( uint32T ) - sizeof(recHdr.nextRec) );

        /*
         *** Copy data to the log record.  Handle buffers in buffer vector
         */

        log_record_addr += (REC_HDR_WORDS* sizeof( uint32T ));
        wdsWritten = 0;

        /*
         *** Repeat until we've moved 'segWdsToWrite' to the log record.
         */
        while (wdsWritten < segWdsToWrite) {
            if (bufWdsWritten >= bufv[buf].bufWords) {
                /*
                 * Move to next buffer in the buffer vector.
                 */
                bufWdsWritten = 0;
                buf++;

                if (buf >= bufCnt) {
                    /** bummer!! **/
                    domain_panic(ldP->dmnP, "lgr_writev_ftx: buf >= bufCnt");
                    RAISE_EXCEPTION( E_DOMAIN_PANIC );
                }
            }

            wdsToWrite = MIN( bufv[buf].bufWords - bufWdsWritten,
                              segWdsToWrite - wdsWritten );

            /*
             *** Copy data to log page.
             */
            if (wdsToWrite > 8) {
                bcopy(
                   (char*) &bufv[buf].bufPtr[bufWdsWritten],
                   (char*) log_record_addr,
                   wdsToWrite * sizeof( uint32T ) );
            } else {
                dataP = (uint32T *)log_record_addr;
                for (wd = 0; wd < wdsToWrite; wd++ ) {
                    dataP[wd] = bufv[buf].bufPtr[bufWdsWritten + wd];
                }
            }

            log_record_addr     += (wdsToWrite * sizeof( uint32T ));
            totWdsWritten       += wdsToWrite;
            bufWdsWritten       += wdsToWrite;
            wdsWritten          += wdsToWrite;
        }

        segment++;

        /* 
         * We are done with this page. We need to determine if we should unpin.
         */
        if( AdvfsSpinTryCount ) {
            int try_count = AdvfsSpinTryCount;
            while( FLUSH_TRY_WRITE_LOCK == 0 ) {
                if ( try_count-- == 0 ) {
                    FLUSH_WRITE_LOCK;
                    break;
                }
            }
        } else {
            FLUSH_WRITE_LOCK;
        }
        
        if (ldP->lastPg.num != current_log_page)
        {
            /* Another writer moved to the next log page. We
             * need to locate our active rec from the active list
             */
            int index;
            index = get_wrtPgD_index(current_log_page,ldP->pgCnt);
            MS_SMP_ASSERT(ldP->wrtPgD[index].pinned);
            MS_SMP_ASSERT(ldP->wrtPgD[index].num == current_log_page);

            if (--ldP->wrtPgD[index].writers == 0)
            {
                /* We are the last writer of this page we must now
                 * unpin so that it may be flushed. 
                 */

                sts = lgr_unpinpg(ldP,
                            ldP->wrtPgD[index].refH,ldP->wrtPgD[index].lastRec,
                            &ldP->wrtPgD[index]);

                if (sts != EOK) {
                    FLUSH_UNLOCK;
                    domain_panic(ldP->dmnP, "lgr_writev_ftx: unpinpg failed");
                    return sts;
                }

                (ldP->dmnP)->logStat.logWrites++;
                (ldP->dmnP)->logStat.wastedWords += DATA_WORDS_PG - current_rec.offset;
                            
            }
        }
        else
        {
            /* This is still the last log page. We must check to see
             * if a flush was requested while we were writing this
             * page */
            if ( (--ldP->lastPg.writers == 0) && (ldP->flushing))
            {
                /* There is an oustanding flush. The only way the
                 * flushing bit can be set is if we are in the flush range
                 * and no other thread called lgr_writev_ftx. Otherwise
                 * we would have moved off this page.
                 */

                MS_SMP_ASSERT(LSN_GTE( ldP->flushLsn, ldP->lastPg.pgP->hdr.thisPageLSN ));

                /* We know 3 things: 
                 * 1) We are the last writer on the page.
                 * 2) No more writers can get in 
                 * 3) The lastRec.lsn has not changed since the thread that
                 *    started the flush began waiting for it.
                 *
                 * We will unpin this page so that it will be put on the
                 * i/o queues (blocking) to be flushed. Since the flushing bit is
                 * set we also know that any threads about to write the log will
                 * attempt to reuse this page but will be forced off.
                 */
                
                sts = lgr_unpinpg( ldP,
                                   ldP->lastPg.refH,
                                   ldP->lastRec,
                                   &ldP->lastPg );
                
                if (sts != EOK) {
                    FLUSH_UNLOCK;
                    domain_panic(ldP->dmnP, "lgr_writev_ftx: unpinpg failed");
                    return sts;
                }

                /* We do NOT want to clear the flushing bit. The only time it
                 * should be cleared is when we either sync up and actually
                 * know the page made it to disk or if we move off the page.
                 */


                /* Don't update log stats here since they were updated already
                 * at flush time
                 */
            }
            
        }

        if (totWdsWritten < bufWords)
        {
            /* We have a segmented write underway. We already have the log space
             * reserved so set up for the next time through the loop.
             */
            int index;
            
            MS_SMP_ASSERT(current_log_page != ldP->lastPg.num);
            current_log_page = (current_log_page + 1) % ldP->pgCnt;
            
            /* By reserving the space beforehand we know that we have the
             * begining of the next page available for us (offset = 0)
             */

            current_rec.page = current_log_page;
            current_rec.offset = 0;
            LSN_INCR(current_rec.lsn);
            current_rec_offset = 0;

            if(ldP->lastPg.num != current_log_page)
            {

                index = get_wrtPgD_index(current_log_page,ldP->pgCnt);
                MS_SMP_ASSERT(ldP->wrtPgD[index].num == current_log_page);
                log_record_addr = (char *) &ldP->wrtPgD[index].pgP->data[current_rec_offset];
            }
            else
            {
                log_record_addr = (char *) &ldP->lastPg.pgP->data[current_rec_offset];
            }
                
        }
        FLUSH_UNLOCK;
    }

    RELEASE_LOCKS;

    /*
     * Now that we've written the log record we need to determine if the
     * log needs to be flushed.  We should flush the log when
     * lgr_writev's caller requested a synchronous write.
     */

    if (lwMode == LW_SYNC) {

        lgr_flush_start( ldP, FALSE, current_rec.lsn);
        lgr_flush_sync(ldP, current_rec.lsn);
    }

    return EOK;

HANDLE_EXCEPTION:
    
    RELEASE_LOCKS;
    return sts;
}


/*
 * get_clean_pg
 *
 * Used by lgr_writev() to pin a new log page (current end page).
 */

static statusT
get_clean_pg(
    logDescT *ldP
)
{
    statusT sts;
    int b;
    logPgHdrT* pghdrp;


    /*
     * Pin the current log end page.
     */
    sts = bs_pinpg( &ldP->lastPg.refH,
                    (void *) &ldP->lastPg.pgP,
                    ldP->logAccP,
                    ldP->nextRec.page,
                    BS_OVERWRITE );

    if (sts != EOK) {
        return( sts );
    }

    /*
     * Update the log descriptor to indicate that we have pinned the
     * log end page.
     */

    ldP->lastPg.pinned      = TRUE;
    ldP->lastPg.repinned    = FALSE;
    ldP->lastPg.dirty       = FALSE;
    ldP->lastPg.num         = ldP->nextRec.page;
    ldP->lastPg.prevLastRec = -1;
    ldP->lastPg.writers     = 0;
    pghdrp                  = &ldP->lastPg.pgP->hdr;
    pghdrp->thisPageLSN     = ldP->nextRec.lsn;
    pghdrp->pgSafe          = FALSE;
    pghdrp->chkBit          = 0;
    pghdrp->curLastRec      = -1;
    pghdrp->prevLastRec     = -1;
    pghdrp->firstLogRec     = ldP->firstRec;

    for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
        ldP->lastPg.pgP->trlr.lsnOverwriteVal[b - 1] = nilLSN;
    }

    return EOK;
}


/*
 * lgr_retest_firstrec - return the older (or nil) of the current
 * outstanding ftx and oldest dirty buffer log record addresses.
 */
logRecAddrT
lgr_retest_firstrec(
                    domainT* dmnP    /* in - ptr to domain */
                    )
{
    logRecAddrT oldestftxla, dirtybufla, retla;

    /*
     * Because of the locking sequence, it is necessary to read the
     * oldestftxla before the dirtybufla.
     */

    oldestftxla = ftx_get_oldestftxla( dmnP );
    dirtybufla = ftx_get_dirtybufla( dmnP );

    if ( LSN_EQ_NIL( oldestftxla.lsn ) ) {
        retla = dirtybufla;
    } else if ( LSN_EQ_NIL( dirtybufla.lsn ) ) {
        retla = oldestftxla;
    } else if ( LSN_LT( oldestftxla.lsn, dirtybufla.lsn)) {
        retla = oldestftxla;
    } else {
        retla = dirtybufla;
    }

    return retla;
}


/* Find the oldest active (meta data change not flushed to disk) log entry.
 * called from bs_osf_complete
 *
 * SMP: Assumes the dmnP->lsnLock is held by caller to protect:
 *       1. Reading dirtyBufLa
 *       2. Resetting dmnP->logStats.maxLogPgs & minLogPgs
 *
 */
void
resetfirstrec(domainT* dmnP)
{
    statusT sts;
    logDescT *ldP;
    int logpages;
    logRecAddrT firstRec;

    MS_SMP_ASSERT(SLOCK_HOLDER(&dmnP->lsnLock.mutex));

    ldP = dmnP->ftxLogP;

    /* Get log addr of oldest dirty, unflushed buffer in this domain. */
    firstRec = dmnP->dirtyBufLa.lgra[dmnP->dirtyBufLa.read & 1];

    if ( LSN_EQ_NIL(firstRec.lsn) ) {
        /* dmnP->lsnList is empty; get value of oldest outstanding transaction*/
        firstRec = ftx_get_oldestftxla(dmnP);
    }
    if ( LSN_EQ_NIL(firstRec.lsn) ) {
        /* No outstanding ftx, so use value of next rec to write */
        firstRec.page = ldP->nextRec.page;
    }

    /* Save value of page from log rec above into the oldestPg of log desc. */
    ldP->oldestPg = firstRec.page;

    logpages = (int)ldP->nextRec.page - (int)firstRec.page;
    if ( logpages < 0 ) {
        logpages += ldP->pgCnt;
    }
    MS_SMP_ASSERT(logpages >= 0);
    if ( logpages > dmnP->logStat.maxLogPgs ) {
        dmnP->logStat.maxLogPgs = logpages;
    }
    if ( logpages < dmnP->logStat.minLogPgs ) {
        dmnP->logStat.minLogPgs = logpages;
    }
}


/* called from msfs_real_syscall:OP_GET_DMN_PARAMS
 *
 * SMP: dmnP->lsnLock is used to guard resetting of dmnP->logStat.maxLogPgs
 *      and minLogPgs.
 */
statusT
getLogStats(domainT * dmnP, logStatT *logStatp)
{
    statusT sts;
    logDescT *ldP;
    int logpages;
    int slots;
    ftxTblDT *ftxTDp;
    ftxStateT *ftxp;
    extern mutexT FtxMutex;
    DEFINE_LOCK_FLAGS;


    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }
    ldP = dmnP->ftxLogP;
    if (ldP == NULL)
    {
        return E_INVALID_LOG_DESC_POINTER;
    }

    logStatp->logWrites = dmnP->logStat.logWrites;
    logStatp->transactions = dmnP->logStat.transactions;
    logStatp->logTrims = dmnP->logStat.logTrims;
    logStatp->wastedWords = dmnP->logStat.wastedWords;
    logStatp->excSlotWaits = dmnP->logStat.excSlotWaits;
    logStatp->fullSlotWaits = dmnP->logStat.fullSlotWaits;
    logStatp->rsv1 = dmnP->logStat.rsv1;
    logStatp->rsv2 = dmnP->logStat.rsv2;
    logStatp->rsv3 = dmnP->logStat.rsv3;
    logStatp->rsv4 = dmnP->logStat.rsv4;

    mutex_lock( &dmnP->lsnLock );
    logStatp->maxLogPgs = dmnP->logStat.maxLogPgs;
    logStatp->minLogPgs = dmnP->logStat.minLogPgs;
    logpages = (int)ldP->nextRec.page - (int)ldP->oldestPg;
    if ( logpages < 0 ) {
        logpages += ldP->pgCnt;
    }
    MS_SMP_ASSERT(logpages >= 0);
    dmnP->logStat.maxLogPgs = logpages;
    dmnP->logStat.minLogPgs = logpages;
    mutex_unlock( &dmnP->lsnLock );

    ftxTDp = &dmnP->ftxTbld;
    mutex_lock( &FtxMutex );
    logStatp->oldFtxTblAgent = dmnP->logStat.oldFtxTblAgent;
    logStatp->maxFtxTblSlots = dmnP->logStat.maxFtxTblSlots;

    if ( FtxStats ) {
        slots = ftxTDp->rrNextSlot - ftxTDp->oldestSlot;
        if ( slots < 0 ) {
            slots += ftxTDp->rrSlots;
        }
        MS_SMP_ASSERT(slots >= 0);
        dmnP->logStat.maxFtxTblSlots = slots;
        if ( ftxp = ftxTDp->tablep[ftxTDp->oldestSlot].ftxp ) {
            dmnP->logStat.oldFtxTblAgent = ftxp->lrh.agentId;
        } else {
            dmnP->logStat.oldFtxTblAgent = 0;
        }
    } else {
        dmnP->logStat.oldFtxTblAgent = 0;
    }
    mutex_unlock( &FtxMutex );

    lock_write( &ldP->descLock );

    logStatp->maxFtxWords = dmnP->logStat.maxFtxWords;
    logStatp->maxFtxAgent = dmnP->logStat.maxFtxAgent;
    dmnP->logStat.maxFtxWords = 0;
    dmnP->logStat.maxFtxAgent = 0;
    lock_done( &ldP->descLock );

    return EOK;
}


/*
 * release_dirty_pg
 *
 * This routine is used by lgr_writev() to move off of the current
 * log page.  If the page is pinned then we unpin it.  In either
 * case we move to the next log page; however, the new page is not
 * pinned, we only setup the log descriptor to point to the
 * next page.  get_clean_pg() can be used to actually pin the page.
 */

static statusT
release_dirty_pg(
    logDescT *ldP
    )
{
    int quadrant;
    statusT sts;
    domainT *dmnP = ldP->dmnP;


    if (ldP->lastPg.pinned) {

        MS_SMP_ASSERT(ldP->lastPg.writers >= 0);

        if (ldP->lastPg.writers == 0)
        {
            /*
             * The page is full and there are no other
             * writers; write it out asynchronously.
             */

            sts = lgr_unpinpg( ldP,
                               ldP->lastPg.refH,
                               ldP->lastRec,
                               &ldP->lastPg );
            if (sts != EOK) {RAISE_EXCEPTION( sts );}

            /* Update statistics counters */
            dmnP->logStat.logWrites++;
            dmnP->logStat.wastedWords += DATA_WORDS_PG - ldP->nextRec.offset;
        }
        else
        {
            /* The page is full but, it is currently being copied to.
             * Move it onto the active page queue and it will be unpinned
             * when the last writer completes
             */
            int index;

            index = get_wrtPgD_index(ldP->lastPg.num,ldP->pgCnt);

            MS_SMP_ASSERT(!ldP->wrtPgD[index].pinned);

            ldP->wrtPgD[index] = ldP->lastPg;
            ldP->wrtPgD[index].lastRec = ldP->lastRec;
            ldP->lastPg.pgP    = NULL;
            ldP->lastPg.dirty  = FALSE;
            ldP->lastPg.pinned = FALSE;
            
        }
    }

    if ( LAST_PAGE( ldP ) ) {
        /*
         * Make tests necessary to reset log beginning
         * and recheck 'log full' condition.
         */

        ldP->firstRec = lgr_retest_firstrec( ldP->dmnP );

        if ( LSN_EQ_NIL( ldP->firstRec.lsn )) {
            ldP->firstRec = ldP->nextRec;
        }

        if ( LAST_PAGE( ldP ) ) {
            domain_panic(ldP->dmnP, "release_dirty_pg: log full" );
            RAISE_EXCEPTION( E_DOMAIN_PANIC );
        }
    }

    /************************************************************
     ******  Move on to the next log page.                  *****
     ******  This is the ONLY place in the logger where     *****
     ******  nextRec.page should be incremented.            *****
     ************************************************************/
    ldP->nextRec.page = (ldP->nextRec.page + 1) % ldP->pgCnt;
    ldP->nextRec.offset = 0;

    quadrant = ldP->nextRec.page / (ldP->pgCnt / 4);

    if ( !(ldP->nextRec.page - quadrant * (ldP->pgCnt / 4)) ) {
        lsnT firstlsn;

        /*
         * Make tests necessary to reset log beginning.
         */

        ldP->firstRec = lgr_retest_firstrec( dmnP );

        if ( LSN_EQ_NIL( ldP->firstRec.lsn )) {
            ldP->firstRec = ldP->nextRec;
        }

        firstlsn = ldP->firstRec.lsn;

        if ( LSN_LT( firstlsn, ldP->quadLsn[quadrant & 1]) ) {
            /*
             * Hmm.  The log is now half full.  This means there may
             * not be enough left to guarantee recovery would complete.
             */
            domain_panic(ldP->dmnP, "release_dirty_pg: log half full" );
            RAISE_EXCEPTION( E_DOMAIN_PANIC );
        }

        if ( LSN_LT( firstlsn, ldP->quadLsn[(quadrant + 1) & 1]) ) {
            /*
             * The first log record is not already up to the last quadrant,
             * so block new transactions until all current
             * transactions complete, allowing the first log record to
             * advance.
             */
            dmnP->ftxTbld.logTrimLsn  = ldP->quadLsn[(quadrant + 1) & 1];
        }

        ldP->quadLsn[quadrant & 1] = ldP->nextRec.lsn;

    }
    return EOK;

HANDLE_EXCEPTION:
    return sts;
}


/*
 * lgr_read_open -
 *
 * Opens a log "read stream".  In order to read a log one
 * must first open a read stream.  Each read stream has associated with it
 * a descriptor which contains information about the current page(s) being
 * accessed.  The reason read streams exist is because lgr_read() does
 * not actually copy log records from the log to the client's buffer; thereby
 * eliminating costly data copying.  So, lgr_read() returns a pointer into
 * the log page that contains the record.  This means that lgr_read()
 * must keep the page referenced until the client is finished with the
 * record (a subsequent call to lgr_read() or lgr_read_close() tells
 * the logger that it can deref any pages associated with the read stream).
 * So, log streams are a way for the logger to keep track of which pages
 * are references by clients reading the log.
 */

statusT
lgr_read_open(
    logDescT *ldP,        /* in  - logDesc pointer */
    logRdHT *logRdH       /* out - lgr_read handle */
    )
{
    int rddesc, newRdDesc = 0;
    statusT sts;
    rdPgDescT *rdP;
    DEFINE_LOCK_FLAGS;


    if (ldP == NULL)
        RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );

    rdP = (rdPgDescT *) ms_malloc( sizeof( rdPgDescT ) );
    if (rdP == NULL) {
        domain_panic(ldP->dmnP, "lgr_read_open: out of memory");
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    *rdP = NilRdPgDesc;

RETRY:
    if ( AdvfsSpinTryCount ) {
        int try_count = AdvfsSpinTryCount;
        while( DESCRIPTOR_TRY_WRITE_LOCK == 0 ) {
            if( try_count-- == 0 ) {
                DESCRIPTOR_WRITE_LOCK;
                break;
            }
        }
    } else {
        DESCRIPTOR_WRITE_LOCK;
    }

    /* Allocate a read page descriptor */

    for (rddesc = 0; (rddesc < MAX_RD_PGS) && (newRdDesc == 0); rddesc++) {
        if (ldP->rdPgD[rddesc] == NULL) {
            newRdDesc = rddesc + 1;
            ldP->rdPgD[rddesc] = rdP;
        }
    }

    if (newRdDesc == 0) {
        /*
         * Wait for lgr_read_close() to free a read page descriptor.
         */
        assert_wait((vm_offset_t)ldP,FALSE);
        DESCRIPTOR_UNLOCK;
        thread_block();
        goto RETRY;
    }

    DESCRIPTOR_UNLOCK;

    logRdH->ldP = ldP;
    logRdH->rdH = newRdDesc;
    return EOK;

HANDLE_EXCEPTION:

    if (rdP)
        ms_free( rdP );
    return sts;
}


/*
 * lgr_dmn_read_open -
 *
 * Identical to lgr_read_open above except it
 * takes a domain handle instead of a log handle as input.
 *
 * This is intended for use by utility programs which need to read the
 * log.
 */
statusT
lgr_dmn_read_open(
    domainT *dmnP,    /* in  - domain pointer */
    logRdHT *logrdh   /* out - lgr_read handle */
    )
{
    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }

    return lgr_read_open( dmnP->ftxLogP, logrdh );
}


/*
 * lgr_read_close -
 *
 * Closes a log "read stream".
 */
statusT
lgr_read_close(
    logRdHT logRdH        /* in  - lgr_read handle */
    )
{
    logDescT *ldP = logRdH.ldP;
    rdPgDescT *rdP; /* read_page descriptor pointer */
    statusT sts;
    DEFINE_LOCK_FLAGS;


    if (ldP == NULL)
        {RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );
    }

    if ((logRdH.rdH < 1) || (logRdH.rdH > MAX_RD_PGS)) {
        RAISE_EXCEPTION( EINVALID_HANDLE );
    }

    rdP = ldP->rdPgD[logRdH.rdH - 1];
    if (rdP == NULL) {RAISE_EXCEPTION( EINVALID_HANDLE );}

    DESCRIPTOR_WRITE_LOCK;

    if (rdP->refed) {
        /* Release referenced log pages */
        sts = bs_derefpg( rdP->refH, BS_CACHE_IT );
        if (sts != EOK) {RAISE_EXCEPTION( sts );}
    }

    ldP->rdPgD[logRdH.rdH - 1] = NULL;
    DESCRIPTOR_UNLOCK;

    /* Deallocate the descriptor */
    ms_free( rdP );

    /*
     * Wake up any thread waiting for a read page descriptor.
     */
    thread_wakeup((vm_offset_t)ldP);
    return EOK;

HANDLE_EXCEPTION:

    RELEASE_LOCKS;
    return sts;
}


/*
 * lgr_read -
 *
 * Read a record from the log.  lgr_read() does not copy the
 * record into a client's buffer.  Instead it returns a pointer to the
 * record in the log page; thereby eliminating unneccessary data copying.
 * One must first open a "read stream" via lgr_read_open() before reading
 * the log.  lgr_read() returns the log record address of the next record.
 * The meaning of "next record" is dependent on the read mode specified; see
 * the comments for logReadModeT for a description of the various modes
 * and how they affect the meaning of "next record".
 *
 * NOTE: It is now the responsibility of the caller to free the buffer
 *       passed back by this routine.
 *
 */
statusT
lgr_read(
    logReadModeT rdMode,  /* in  - read mode (fwd, bwd, bwd_link, ...) */
    logRdHT logRdH,       /* in  - lgr_read handle */
    logRecAddrT *recAddr, /* in/out  - log address of record */
    uint32T **buf,        /* out - ptr to record data */
    int *bufWords,        /* out - words read */
    int *recWords         /* out - size of record (not just this segment) */
    )
{
    logRecHdrT recHdr;
    logDescT *ldP;
    statusT sts, lsts;
    rdPgDescT *rdP; /* read_page descriptor pointer */

    /* Copy recAddr fields to local variables */

    uint16T rec_pg = recAddr->page;
    uint16T rec_offset = recAddr->offset;
    lsnT rec_lsn = recAddr->lsn;
    uint32T rec_segment;

    DEFINE_LOCK_FLAGS;


    *buf=NULL; /* Initialize to NULL for testing at exception time */
    
    if ((logRdH.rdH < 1) || (logRdH.rdH > MAX_RD_PGS)) {
        RAISE_EXCEPTION( EINVALID_HANDLE );
    }
    ldP = logRdH.ldP;
    if (ldP == NULL){
        RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );
    }

    if ((DATA_WORDS_PG - rec_offset) < TOT_HDR_WORDS) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    rdP = ldP->rdPgD[logRdH.rdH - 1];
    if (rdP == NULL) {RAISE_EXCEPTION( EINVALID_HANDLE );}

    /*
     * We could do better here locking wise but since this routine
     * is not called very often we will just get both locks
     * in write mode. 
     * Concurrent writers should not pose a problem to this routine
     * since the log space is always given out at the end of the log
     * and the log page record headers are setup under the lock. Thus
     * even if the data is currently being written the header can
     * be use to travers the log records in the page. Also the record
     * we are reading must be written since only a single ftx can be
     * accessing a records data at a time.
     */

    DESCRIPTOR_WRITE_LOCK;
    FLUSH_WRITE_LOCK;

    if ((rdP->num != rec_pg) && (rdP->refed))
    {
        /* We have the wrong page refed so deref it */

        sts = bs_derefpg( rdP->refH, BS_CACHE_IT );
        if (sts != EOK) {RAISE_EXCEPTION( sts );}
        rdP->refed = FALSE;
        rdP->pgP = NULL;
    }

    if (!rdP->refed)
    {
        if (bs_logpage_dirty( ldP->logAccP, rec_pg ))
        {
            /* The page that we want to read is dirty (hasn't been
             * flushed to disk). We can not call lgr_refpg because we
             * will write over the dirty page's lsns. We know however
             * that the page is safe to reference, copy locally and
             * then restore. There is no need to wait for the write to
             * finish!  */

            sts = bs_refpg( &rdP->refH,(void*) &rdP->pgP,
                         ldP->logAccP, rec_pg, BS_SEQ_AHEAD );
        }
        else
        {
            /* Read the record's page (and "cache" it ) */
            sts = lgr_refpg( &rdP->refH, &rdP->pgP,
                             ldP->logAccP, rec_pg, BS_SEQ_AHEAD );
        }

        if (sts != EOK) {RAISE_EXCEPTION( sts );}
        
        rdP->refed = TRUE;
        rdP->num = rec_pg;
    }

    /* Copy record header from log page */
    bcopy((logRecHdrT *) &rdP->pgP->data[rec_offset],
          &recHdr,
          sizeof( logRecHdrT));
    /* Restore it */
    lgr_restore_rec(rdP->pgP,
                    (uint32T *)&recHdr,
                    (long)&rdP->pgP->data[rec_offset] - (long)rdP->pgP,
                    sizeof( logRecHdrT));

    if(recHdr.wordCnt > DATA_WORDS_PG - rec_offset ||
       recHdr.wordCnt <= REC_HDR_WORDS)
           RAISE_EXCEPTION( E_INVALID_REC_ADDR);

    rec_segment = recHdr.segment;

    if (LSN_EQ_NIL( rec_lsn )) {
        /* if rec_lsn is zero, ignore bounds check */
        rec_lsn = recHdr.lsn;

    } else if (!LSN_EQL( rec_lsn, recHdr.lsn )) {
        RAISE_EXCEPTION( E_INVALID_REC_ADDR );
    }
    
    /* Return number of words copied to buffer */
    *bufWords = recHdr.wordCnt - REC_HDR_WORDS;
    *recWords = recHdr.clientWordCnt;

    /* Return ptr to data in the log record */
    *buf = (uint32T *)ms_malloc((*bufWords)*4);

    if ( *buf == NULL ) {
        RAISE_EXCEPTION(ENOMEM);
    }

    bcopy(&rdP->pgP->data[rec_offset + REC_HDR_WORDS],
          *buf,
          (*bufWords)*4);

    /* Restore it */
    lgr_restore_rec(rdP->pgP,
                    (uint32T *)*buf,
                    (long)&rdP->pgP->data[rec_offset + REC_HDR_WORDS] -
                     (long)rdP->pgP,
                    (*bufWords)*4);
    sts = EOK;

    if (recHdr.segment & MORE_SEGS) {
        sts = I_LOG_REC_SEGMENT;
        rdMode = LR_FWD;
        if (!RECADDR_EQ( recHdr.nextRec, logEndOfRecords))
        {
            printf("Bad next record in segment !\n");
        }

    } else if (recHdr.segment != 0) {
        sts = I_LAST_LOG_REC_SEGMENT;
    }

    /* Return "next" record's address */

    switch (rdMode) {
    case LR_FWD:
        /* We're doing a forward read so return the next record's address */

        if (RECADDR_EQ( recHdr.nextRec, logEndOfRecords )) {
            /* We've reached the last record in the current page */

            if (recHdr.segment & MORE_SEGS) {
                if ((rec_pg == ldP->nextRec.page ) ||
                    ((((rec_pg + 1) % ldP->pgCnt) == ldP->nextRec.page ) &&
                     (ldP->nextRec.offset == 0))) {
                    /* We've reached the last record in the log */
                    *recAddr = logEndOfRecords;
                    sts = W_LAST_LOG_REC;
                } else {
                    logPgT *pgP;
                    bfPageRefHT refH;
                    logRecHdrT *nextrecHdr;

                    /* Return address of first record in the next page */
                    recAddr->offset = 0;
                    recAddr->page = (rec_pg + 1) % ldP->pgCnt;

                    /* We have to read the next page to get the rec's LSN */

                    lsts = bs_refpg( &refH, (void*)&pgP, ldP->logAccP,
                                    recAddr->page, BS_SEQ_AHEAD );
                    if (lsts != EOK) {RAISE_EXCEPTION( lsts );}

                    /* Get LSN from rec header from first rec in next log pg */

                    recAddr->lsn = ((logRecHdrT *) &pgP->data[0])->lsn;

                    if(!LSN_VALIDATE(recAddr->lsn,recHdr.lsn)){
                        (void )bs_derefpg( refH, BS_CACHE_IT );
                        RAISE_EXCEPTION( E_INVALID_REC_ADDR );
                    }

                    if (rec_segment & MORE_SEGS) {
                        nextrecHdr=(logRecHdrT *)&pgP->data[0];
                        if (!SEG_VALID( rec_segment, nextrecHdr->segment )) {
                            (void )bs_derefpg( refH, BS_CACHE_IT );
                            RAISE_EXCEPTION( E_INVALID_REC_ADDR );
                        }
                    }

                    (void )bs_derefpg( refH, BS_CACHE_IT );
                }

            } else if ((rec_pg == ldP->nextRec.page) ||
                       ((((rec_pg + 1) % ldP->pgCnt) == ldP->nextRec.page ) &&
                        (ldP->nextRec.offset == 0))) {

                /* We've reached the last record in the log */
                *recAddr = logEndOfRecords;
                sts = W_LAST_LOG_REC;
            } else {
                logPgT *pgP;
                bfPageRefHT refH;

                /* Return address of first record in the next page */

                recAddr->offset = 0;
                recAddr->page = (rec_pg + 1) % ldP->pgCnt;

                /* We have to read the next page to get the rec's LSN */

                lsts = bs_refpg( &refH, (void*)&pgP, ldP->logAccP,
                                 recAddr->page, BS_SEQ_AHEAD );
                if (lsts != EOK) {RAISE_EXCEPTION( lsts );}

                /* Get LSN from record header from first rec in next log page */

                recAddr->lsn = ((logRecHdrT *) &pgP->data[0])->lsn;

                (void )bs_derefpg( refH, BS_CACHE_IT );
            }
        } else {
            /* Return the address of the next record (on the current page ) */
            *recAddr = recHdr.nextRec;
        }
        break;

    case LR_BWD:
        /* We're doing a backward read so return the prev record's address */

        if (RECADDR_EQ( recHdr.prevRec, logEndOfRecords )) {
            /* We've reached the first record in the log */
            *recAddr = logEndOfRecords;
            sts = W_LAST_LOG_REC;

        } else if (LSN_EQL( recHdr.lsn, ldP->firstRec.lsn )) {
            /* We've reached the first record in the log */
            *recAddr = logEndOfRecords;
            sts = W_LAST_LOG_REC;

        } else {
            /* Return the address of the prev record */
            *recAddr = recHdr.prevRec;
        }
        break;

    case LR_BWD_LINK:
        /* We're doing a backward read using the client's links */

        if (RECADDR_EQ( recHdr.prevClientRec, logEndOfRecords )) {
            *recAddr = logEndOfRecords;
            sts = W_LAST_LOG_REC;

        } else if (LSN_LT( recHdr.prevClientRec.lsn, 
                           ldP->firstRec.lsn )) {
            /* We've reached the first record in the log */
            *recAddr = logEndOfRecords;

            /* Give back the buffer */
            /* ??? Should we free it here ??? */
            ms_free(*buf);
            *buf=NULL;
            sts = E_BAD_CLIENT_REC_ADDR;

        } else {
            /* Return the address of the prev client record */
            *recAddr = recHdr.prevClientRec;
        }
        break;
    }
    FLUSH_UNLOCK;
    DESCRIPTOR_UNLOCK;
    return sts;

HANDLE_EXCEPTION:

    RELEASE_LOCKS;

    if (*buf != NULL)
    {
        ms_free(*buf);
        *buf=NULL;
    }
    return sts;
}


/*
 * get_pg_lsn
 *
 * Returns the first record LSN of the specified page.  If the page is
 * invalid then no LSN is returned and the flag 'badPg' is set to TRUE.
 */

static void
get_pg_lsn(
    bfAccessT *bfap,    /* in - log's bitfile access structure */
    uint32T pg,         /* in - log page's number */
    lsnT *pgLsn,        /* out - log page's first record's LSN */
    int *invalidPg,     /* out - flag indicates if page is invalid */
    int *incompletePg,  /* out - flag indicates if page is incomplete */
    lsnT *prtdLsn,      /* in/out - thread of prev lsn across pg */
    uint32T *prtdPg     /* in/out - thread of prev pg  across pg */
    )
{
    statusT sts;
    bfPageRefHT pgRef;
    logPgT *pgP;
    logRecHdrT *recHdr;


    *invalidPg = FALSE; /* assume all is okay */
    *incompletePg = FALSE; /* assume all is okay */

    sts = bs_refpg( &pgRef, (void*) &pgP, bfap, (pg), BS_NIL );

    if (sts != EOK) {
        *invalidPg = TRUE;

    } else {
        *pgLsn = pgP->hdr.thisPageLSN;
        *invalidPg = !lgr_valid_pg( bfap, pgP );
        *incompletePg = !lgr_complete_pg( pgP );

        /*
         * check for consistent prevLsn threading
         */
        if ( (!LSN_EQ_NIL(*prtdLsn)) && (*prtdPg == pg) ) {
            lgr_restore_pg(bfap->dmnP, pgP);
            recHdr = (logRecHdrT *) &pgP->data[pgP->hdr.curLastRec];
            if ( !LSN_EQL(recHdr->lsn,*prtdLsn) ) {
                printf("advfs_logger: page %d decipher error; truncating log in domain %s\n", pg, bfap->dmnP->domainName);
                *incompletePg = TRUE;
            }
        }
        recHdr = (logRecHdrT *) &pgP->data[0];
        if ( LSN_GT(recHdr->firstSeg.lsn,recHdr->prevRec.lsn) ) {
            *prtdLsn = recHdr->firstSeg.lsn;
            *prtdPg  = recHdr->firstSeg.page;
        } else {
            *prtdLsn = recHdr->prevRec.lsn;
            *prtdPg  = recHdr->prevRec.page;
        }

        sts = bs_derefpg( pgRef, BS_CACHE_IT );

        if (sts != EOK) {
            *invalidPg = TRUE;
            *incompletePg = TRUE;
        }
    }
}


/*
 * find_log_end_pg -
 *
 * Finds the end of the log.  Since the log is circular the
 * "end of the log" is the last page written to the log.  The page that
 * contains the end of the log can be found by searching the log until we
 * find a page whose first record's LSN is smaller than the previous page's.
 * The previous page is the log end page.
 *
 * This routine uses a modified binary search algorithm to locate
 * the log end.  Basically use binary search to locate two pages
 * where the LSN of the first page is greater than the LSN of the
 * second page.
 *
 * This routine should take no more than approx 2 * log2( N ) reads to find
 * the log end page; N == number of pages in the log.
 *
 * RETURN VALUES:
 *
 * Normally this routine returns the log page number of the page that
 * contains the log's end.  In other words, it returns the log end page's
 * number.
 *
 * Returns -1 if the log end could not be found.
 * Returns -2 if the log is empty.
 */

static int
find_log_end_pg(
    logDescT *ldP /* in - log descriptor ptr */
    )
{
    int i, logWrapped = 0, pgBad = 0, pg0Bad = 0, pgMaxBad = 0, searchPgs;
    uint32T lwrPg = 0, uprPg = ldP->pgCnt - 1, midPg, endPg = 0, pg,
            ptrdPg = -1;
    int ignore, pgIncomplete;
    bfPageRefHT pgRef;
    logRecHdrT *recHdr;
    ftxDoneLRT* dlrp;
    logPgT *logPgP;
    statusT sts;
    uint32T recOffset;
    logRecAddrT crashRedo;
        
    lsnT
        lastPgLSN,
        ptrdLsn,
        prevLSN,
        endLSN,
        pgLSN,
        pg0LSN, /* LSN of log page zero */
        pgMid1LSN,
        pgMid2LSN,
        pgMaxLSN; /* LSN of log page ldP->pgCnt - 1 */

    /* The calculation for maxLSNs used to be:
     *             maxLSNs = LSN_INCR_AMT * LOG_RECS_PG *
     *                       (ldP->pgCnt + (ldP->pgCnt - 1) * LOG_FLUSH_THRESHOLD);
     *
     * which gives the minimum maximum spacing between any two lsns in the log
     * this can be replpaced by 1<<29 which is an eighth of the log. In other words
     * no two consecutive lsns can be more than 1/8 of the max number of lsns apart.
     */

    /*----------------------------------------------------------------------*/

    /*
     * Get the last page's LSN.  We need it to determine if the log
     * has wrapped (the end page number is less than the beginning page
     * number) since this effects the binary search.
     */

    ptrdLsn = nilLSN;
    get_pg_lsn(ldP->logAccP, ldP->pgCnt - 1,
               &pgMaxLSN, &pgMaxBad, &ignore,
               &ptrdLsn, &ptrdPg);

    ptrdLsn = nilLSN;
    get_pg_lsn( ldP->logAccP, 0, &pg0LSN, &pg0Bad, &ignore,
               &ptrdLsn, &ptrdPg);

    if (pg0Bad) {
        if (!pgMaxBad) {
            if (LSN_EQ_NIL( pgMaxLSN )) {
                /* The log is empty! */
                endPg = -2;
                goto EXIT_FIND_LOG_END_PG;
            } else {
                endPg = ldP->pgCnt - 1;
                goto _finish;
            }
        }

    } else if (LSN_EQ_NIL( pg0LSN )) {
        /* The log is empty! */
        endPg = -2;
        goto EXIT_FIND_LOG_END_PG;
    }

    if (pgMaxBad) {
        endPg = ldP->pgCnt - 2;
        goto _finish;
    }

    logWrapped = !LSN_EQ_NIL(pgMaxLSN) && LSN_LT( pgMaxLSN, pg0LSN );

    if ((!logWrapped) && !LSN_EQ_NIL( pgMaxLSN )) {
        /* The last log pg is the log end pg */
        endPg = ldP->pgCnt - 1;
        goto _finish;
    }

    /*
     * Now that we've taken care of the easy and corner cases we use
     * the binary search to locate the log end page.
     */

    while (uprPg >= lwrPg) {
        midPg = (lwrPg + uprPg) / 2;

        if (midPg == (ldP->pgCnt - 1)) {
            midPg--;
        }


        ptrdLsn = nilLSN;
        get_pg_lsn( ldP->logAccP, midPg, &pgMid1LSN, &pgBad, &ignore,
                   &ptrdLsn, &ptrdPg);
        if (pgBad) {
            endPg = midPg - 1;
            goto _finish;
        }

        ptrdLsn = nilLSN;
        get_pg_lsn( ldP->logAccP, midPg + 1, &pgMid2LSN, &pgBad, &ignore,
                   &ptrdLsn, &ptrdPg);
        if (pgBad) {
            endPg = midPg;
            goto _finish;
        }

        if (LSN_GT( pgMid1LSN, pgMid2LSN )) {
            endPg = midPg;
            goto _finish;
        }

        if (logWrapped) {
            if (LSN_GT( pg0LSN, pgMid1LSN )) {
                uprPg = midPg - 1;
            } else {
                lwrPg = midPg + 1;
            }
        } else {
            if (LSN_EQ_NIL( pgMid1LSN )) {
                uprPg = midPg - 1;
            } else {
                lwrPg = midPg + 1;
            }
        }
    }
    endPg = -1; /* log end not found! */

EXIT_FIND_LOG_END_PG:

    return endPg;

_finish:
    /*
     ** Since the I/O scheduler sends log pages out to the disk
     ** in groups of upto LOG_FLUSH_THRESHOLD pages, it is possible that
     ** the pages sent to the disk didn't all get written and
     ** maybe they didn't get written in the proper order.  Therefore,
     ** the current end page may not be correct.  We need to backtrack
     ** upto LOG_FLUSH_THRESHOLD pages to verify that we have the correct
     ** end page.  The backward scan will find the correct page if the
     ** current one is incorrect.  We will backup the end page during the
     ** scan for the following reasons:
     **
     **    - a page is bad
     **    - a page's LSN is greater than the current end page's LSN
     **
     ** Actually the above is no longer true. We still flush every
     ** LOG_FLUSH_THRESHOLD pages but this does not mean the pages
     ** are written to disk. We now allow multiple writers into the
     ** log onto multiple pages. When a flush is underway, any pages
     ** that are currently being written will be flushed at unpin time.
     ** We no longer wait for the flush to complete. Thus at any point
     ** a single log page could lag by 1/2 the log (this is unlikely)
     **/

    /*
     ** Note that the log must have at least one record in it if we
     ** are executing this "finish" code.
     */

    /*
     ** Calculate the number of pages that we need to verify.
     */

    /* We need to locate the last record on the last log page
     * found from the above binary search. From this record, we
     * can find the first active log page. 
     * We then must start searching thru each log page starting 
     * from this first page making sure that lsn are increasing.
     * If we find an lsn that does not increase then the page 
     * prior to it is the new log end.
     * This is necessary because of out of order writes. We know 
     * that all meta data was held up from going out from any pages
     * in the active range. Thus it is safe to truncate the active
     * range back. Note that we may end up pulling the start page
     * back too since recovery will use the start page (crashRedo)
     * contained on this potentially new log end page, this could
     * be optimized by saving off the start page for recovery to use.
     * Regardless it is not incorrect to recover using the new page
     * since we will only be recovering metadata that is already out
     * on the disk. This is wasteful but not incorrect.
     */

    /* Bring in the candidate last page. The call get_pg_lsn already
     * reffed this page above so we should be safe */

    sts = lgr_refpg( &pgRef, &logPgP, ldP->logAccP, endPg, BS_NIL );
    if (sts != EOK)
    {
        endPg = -1; /* log end not found! */
        goto EXIT_FIND_LOG_END_PG;
    }
    
    recOffset = 0;

    /* Locate the last record in this candidate last page. */

    do {
        if (recOffset >= DATA_WORDS_PG) {
            bs_derefpg( pgRef, BS_CACHE_IT );
            printf( "advfs_logger: invalid rec offset: 0x%08x in domain %s\n",
                     recOffset, ldP->dmnP->domainName );
            endPg = -1; /* log end not found! */
            goto EXIT_FIND_LOG_END_PG;
        }

        recHdr = (logRecHdrT *) &logPgP->data[recOffset];
        recOffset += recHdr->wordCnt;

        if (recHdr->wordCnt == 0) {
            bs_derefpg( pgRef, BS_CACHE_IT );
            printf( "advfs_logger: invalid rec offset: 0x%08x in domain %s\n",
                     recOffset, ldP->dmnP->domainName );
            endPg = -1; /* log end not found! */
            goto EXIT_FIND_LOG_END_PG;
        }
    } while (!RECADDR_EQ( recHdr->nextRec, logEndOfRecords ));

    /* Peel out the first active log (starting) log page */

    dlrp = (ftxDoneLRT *)&logPgP->data[recOffset-recHdr->wordCnt+REC_HDR_WORDS];

    crashRedo = dlrp->crashRedo;
    lastPgLSN = logPgP->hdr.thisPageLSN;
    

    /* We no longer need this page */

    sts = bs_derefpg( pgRef, BS_CACHE_IT );

    if (sts != EOK)
    {
        endPg = -1; /* log end not found! */
        goto EXIT_FIND_LOG_END_PG;
    }

    pg = crashRedo.page;
    if (pg > endPg )
        searchPgs = ldP->pgCnt - pg + endPg + 1;
    else
        searchPgs = endPg - pg + 1;
    prevLSN = nilLSN;

    /*
     ** Scan forward thru the range. We have a candidate end page
     ** but we need to be sure there are no missing pages in this
     ** range since the binary search may not have caught them.
     */

    for (i = 1; i <= searchPgs; i++) 
    {

        ptrdLsn = nilLSN;
        get_pg_lsn( ldP->logAccP, pg, &pgLSN, &pgBad, &pgIncomplete,
                    &ptrdLsn, &ptrdPg);

        if (pgBad) {
            /*
             ** The current page was not written in it's entirety.
             ** The previous page in the log must be the log end. 
             */
            endPg = (pg + ldP->pgCnt - 1) % ldP->pgCnt;
            printf( "advfs_logger: bad page %d; truncating log in domain %s\n",
                    endPg, ldP->dmnP->domainName );
            break;
        }
        if (LSN_EQ_NIL( prevLSN))
        {
            /*
             * The following test is for a very special case.
             * if our prevLSN is NIL then this means that we are the first 
             * page in the active range. Furthermore if our lsn is less than
             * the lsn in the crashRedo record of the last candidate page 
             * in the active range then this first active page was not flushed
             * to disk (this is probably due to a hang caused by hardware or maybe
             * software but certainly not advfs software).
             *
             * We need to force the LSN of the next write to the log to be
             * greater than the LSN of the last active candidate page. Otherwise
             * The next log write will take on the vaule of the LSN in the
             * last active record, causing this new log page to appear to be
             * part of the active range, when in fact it is not. 
             * 
             * Lets call this first page X and the last active page
             * Y. Since it was not flushed to disk we will back up one
             * page and call X-1 the last log page. Recovery will run
             * and the next log record will take on the LSN of X-1
             * incremented by some amount. This will exactly match
             * what the last log record's (page Y) creashRedo.lsn.
             * By incrementing beyond page Y's lsn, if we crash and
             * run recovery again, we can treat this page X as the start of
             * the active range, however since its lsn is greater than
             * page Y's we will now find a new log end page somewhere
             * between X and Y.
             */

            if ((LSN_LT(pgLSN,crashRedo.lsn) &&
                 LSN_LT(lsn_add(pgLSN,LOG_RECS_PG),crashRedo.lsn)) ||
                pgIncomplete)
            {

                /* If the range of possible LSNs for this page are 
                 * less than the crashRedo's lsn for this page then
                 * this is a stale/old log page as talked about above
                 */

                ldP->nextRec.lsn = lastPgLSN;
                endPg = (pg + ldP->pgCnt - 1) % ldP->pgCnt;
                break;
            }
        }
        else if ( LSN_LT( pgLSN,prevLSN ) ||
                  pgIncomplete)
        {
            /*
             ** Either we don't have a valid end page or the current
             ** page has a higher LSN than the prev page.  Or, the page
             ** was not overwritten completely.
             ** Make the current page the current end page.
             */

            endPg = (pg + ldP->pgCnt - 1) % ldP->pgCnt;
            break;

        } 
        
        prevLSN = pgLSN;
        endPg = pg;
        pg = (pg + ldP->pgCnt + 1) % ldP->pgCnt;
    }

    return endPg;
}


/*
 * setup_log_desc -
 *
 * This routine is used to setup/initialize the log descriptor when
 * a log is opened for the first time.  The main things it sets up
 * include:  the log's first, last and next record pointers.
 */
static statusT
setup_log_desc(
    logDescT *ldP,              /* in - log descriptor ptr */
    int endPg,                  /* in - log end page */
    logRecAddrT *nextLogRec     /* in - log addr of next rec in the log */
    )
{
    int pgReferenced = 0;
    logRecHdrT *recHdr;
    uint32T recOffset;
    statusT sts;
    bfPageRefHT pgRef;
    logPgT *logPgP;


    if (endPg == -1) {
        RAISE_EXCEPTION( E_CANT_FIND_LOG_END );
    }

    if (endPg == -2) {
        /*
         ** The log is empty!
         */
        ldP->nextRec.lsn = firstLSN;
        ldP->firstRec.lsn = firstLSN;
        ldP->lastRec = logEndOfRecords;
        ldP->lastRecFirstSeg = logEndOfRecords;
        nextLogRec->lsn = firstLSN;
        nextLogRec->page = 0;
        nextLogRec->offset = 0;
        ldP->wrtPgD = (wtPgDescT *)ms_malloc(sizeof(wtPgDescT) * (ldP->pgCnt/4 + 1));
        if (ldP->wrtPgD == NULL)
            RAISE_EXCEPTION( ENO_MORE_MEMORY );
        return EOK;
    }

    /* Now we have to scan the page to find the last record in the page */

    sts = lgr_refpg( &pgRef, &logPgP, ldP->logAccP, endPg, BS_NIL );
    if (sts != EOK) {
        RAISE_EXCEPTION( E_CANT_ACCESS_LOG );
    }
    pgReferenced = 1;

    /*
     * Set the first log record from the header in the last page.
     */

    ldP->firstRec = logPgP->hdr.firstLogRec;

    recOffset = 0;

    do {
        if (recOffset >= DATA_WORDS_PG) {
            printf( "advfs_logger: invalid rec offset: 0x%08x in domain %s\n",
                     recOffset, ldP->dmnP->domainName );
            RAISE_EXCEPTION( E_CANT_ACCESS_LOG );
        }

        recHdr = (logRecHdrT *) &logPgP->data[recOffset];
        recOffset += recHdr->wordCnt;

        if (recOffset == 0) {
            printf( "advfs_logger: invalid rec offset: 0x%08x in domain %s\n", recOffset, ldP->dmnP->domainName );
            RAISE_EXCEPTION( E_CANT_ACCESS_LOG );
        }
    } while (!RECADDR_EQ( recHdr->nextRec, logEndOfRecords ));

    /*
     * We've found the last record so initialize the log descriptor.
     * We setup the 'nextRec' offset to make the page look full to avoid
     * the possibility of corrupting an already written page, i.e., we
     * must not overwrite an existing log page.  We add LOG_FLUSH_THRESHOLD
     * to the next LSN to get past any bad/incomplete pages that may
     * be in the log just beyond the end page (see find_log_end_pg()).
     */

    ldP->nextRec.page = endPg;
    ldP->nextRec.offset = DATA_WORDS_PG;  /* set to "page full" offset */

    if (!LSN_EQ_NIL(ldP->nextRec.lsn))
    {
        ldP->logAccP->hiFlushLsn =
            lsn_add( ldP->nextRec.lsn, (ldP->pgCnt>>1) * LOG_RECS_PG );
    }
    else
    {
        ldP->logAccP->hiFlushLsn =
            lsn_add( recHdr->lsn, (ldP->pgCnt>>1) * LOG_RECS_PG );
    }

    ldP->nextRec.lsn =
        lsn_add( ldP->logAccP->hiFlushLsn, 1 );

    /* lastRec is setup to point to the last record in the log */

    ldP->lastRec.lsn = recHdr->lsn;
    ldP->lastRec.page = endPg;
    ldP->lastRec.offset = recOffset - recHdr->wordCnt;

    ldP->lastRecFirstSeg = recHdr->firstSeg;

    sts = bs_derefpg( pgRef, BS_CACHE_IT );
    if (sts != EOK) {
        RAISE_EXCEPTION( E_CANT_ACCESS_LOG );
    }
    pgReferenced = 0;

    nextLogRec->lsn = ldP->nextRec.lsn;
    nextLogRec->page = (ldP->nextRec.page + 1) % ldP->pgCnt;
    nextLogRec->offset = 0;

    /*
     * This is way overkill. Set up the active page array to be the
     * max number of log pages that could be currently active. This
     * is a qaudrant. This should be more than we need. In testing
     * I've only seen at most 4 active pages.
     */
   
    ldP->wrtPgD = (wtPgDescT *)ms_malloc(sizeof(wtPgDescT) * (ldP->pgCnt/4 + 1));
    if (ldP->wrtPgD == NULL)
        RAISE_EXCEPTION( ENO_MORE_MEMORY );

    return EOK;

HANDLE_EXCEPTION:

    if (pgReferenced) {
        (void) bs_derefpg( pgRef, BS_CACHE_IT );
    }

    return sts;
}


/*
 * log_init
 *
 * Initializes log pages.
 */

static statusT
log_init(
    bfAccessT *bfap,
    uint32T pgCnt,
    bfDomainIdT domainId
    )
{
    statusT sts;
    bfPageRefHT pgRef;
    logPgT *logPgP;
    int pg;

    MS_SMP_ASSERT(LOG_BLKSZ == BS_BLKSIZE);

    for (pg = 0; pg < pgCnt; pg++) {
        int i;
        sts = bs_pinpg( &pgRef,
                        (void*)&logPgP,
                        bfap,
                        pg,
                        BS_OVERWRITE );
        if (sts != EOK) {
            return( E_CANT_CREATE_LOG );
        }

        logPgP->hdr.thisPageLSN = nilLSN;
        logPgP->hdr.pgType      = BFM_FTXLOG;
        logPgP->hdr.dmnId       = domainId;
        logPgP->hdr.pgSafe      = FALSE;
        logPgP->hdr.chkBit      = 0;
        logPgP->hdr.curLastRec  = 0;
        logPgP->hdr.prevLastRec = 0;
        logPgP->hdr.firstLogRec = logNilRecord;

        for (i = 0; i < (ADVFS_PGSZ_IN_BLKS - 1); i++) {
            logPgP->trlr.lsnOverwriteVal[i] = nilLSN;
        }
        logPgP->trlr.pgType = BFM_FTXLOG;
        logPgP->trlr.dmnId = domainId;

        logPgP->data[0] = pg + 1;  /* for debug purposes */

        sts = bs_unpinpg( pgRef, logNilRecord, BS_DIRTY );
        if (sts != EOK) {
            return( E_CANT_CREATE_LOG );
        }
    }

    return EOK;
}


/*
 * lgr_open -
 *
 * Opens a log.  If the log is being opened for the first time
 * then lgr_open() will allocate the necessary storage for the log.  Otherwise,
 * the log's end and beginning are found and the log descriptor is initialized
 * accordingly.
 */

statusT
lgr_open(
    logDescT **ldP,             /* out - pointer to open logDesc Pointer */
    logRecAddrT *nextLogRec,    /* out - rec addr of next rec in log */
    bfAccessT **logbfap,        /* out - access structure of the open log */
    bfTagT logtag,              /* in - the log's tag */
    bfSetT *bfSetp,             /* in - the log's bitfile-set desc pointer */
    uint32T flag                /* in - if set, re-init the log
                                       (forced mount) */
    )
{
    statusT sts;
    bfAccessT *logAccP;
    int desc;
    int newDesc = 0;
    int openedLog = 0;
    domainT* dmnP = bfSetp->dmnP;
    bfPageRefHT pgref;
    struct bsMPg* bmtpgp;
    vdT *vdp;
    bsBfAttrT *attrp;
    struct bfAccess *mdap;
    logDescT *new_ldP = NULL;
    DEFINE_LOCK_FLAGS;


    /* Initialize the log descriptor */
    new_ldP = (logDescT *) ms_malloc( sizeof( logDescT ) );

    if (new_ldP == NULL)
    {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    *new_ldP = NilLogDesc;
    lock_setup( &(new_ldP->descLock) , ADVlogDescT_descLock_info, TRUE);
    lock_setup( &(new_ldP->flushLock) ,ADVflushT_flushLock_info, TRUE);
    /* Open the log's bitfile */
    MS_SMP_ASSERT(BS_BFTAG_RSVD(logtag));
    sts = bfm_open_ms(&logAccP, dmnP, BS_BFTAG_VDI(logtag), BFM_FTXLOG);

    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* Setup the log descriptor */

    openedLog    = 1;
    new_ldP->dmnP    = dmnP;
    new_ldP->logAccP = logAccP;
    new_ldP->pgCnt   = dmnP->ftxLogPgs;

    if ((logAccP->nextPage == 0) ||
        (logAccP->bfState == BSRA_INVALID) ||
        (flag == TRUE)) {
        /*
         * If this is the first open of the log,there are no pages
         * allocated to it.  So, allocate all the pages.
         * The number of pages allocated must be an exact multiple
         * of 4 so that quadrant transitions are easily detected.
         * If the state is BSRA_INVALID, it means that storage
         * was already added to the log but the log initialization
         * did not complete.  Complete it now.
         */

        if (logAccP->nextPage == 0) {
            sts = bs_add_stg(logAccP, 0, new_ldP->pgCnt );
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
        }
        else if (flag == TRUE) {
            printf("Warning - domain is being mounted without recovery.\n");
        }

        new_ldP->nextRec.lsn     = firstLSN;
        new_ldP->firstRec.lsn    = firstLSN;
        new_ldP->lastRec         = logEndOfRecords;
        new_ldP->lastRecFirstSeg = logEndOfRecords;

        sts = log_init(logAccP,
                       new_ldP->pgCnt,
                       new_ldP->logAccP->dmnP->domainId);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        sts = bfflush(logAccP, 0, new_ldP->pgCnt, FLUSH_IMMEDIATE);
        if (sts != EOK) {
            domain_panic(dmnP, "lgr_open: bf flush start failed");
            RAISE_EXCEPTION( sts );
        }

        /*
         * Update the on-disk state to valid.
         */

        vdp = VD_HTOP(logAccP->primVdIndex, dmnP);

        if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(logtag) ) {
            mdap = vdp->rbmtp;
        } else {
            mdap = vdp->bmtp;
        }
        sts = bs_pinpg(&pgref,
                       (void *)&bmtpgp,
                       mdap,
                       logAccP->primMCId.page,
                       BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        attrp = bmtr_find(&bmtpgp->bsMCA[BFM_FTXLOG], BSR_ATTR, dmnP);
        if (attrp == NULL) {
            bs_unpinpg(pgref, logNilRecord, BS_CLEAN);
            RAISE_EXCEPTION(ENO_BS_ATTR);
        }

        attrp->state = BSRA_VALID;
        sts = bs_unpinpg(pgref, logNilRecord, BS_WRITETHRU);
        if (sts != EOK) {
            RAISE_EXCEPTION(E_IO);
        }

        logAccP->bfState = BSRA_VALID;

        logAccP->hiFlushLsn = nilLSN;

        *nextLogRec = new_ldP->nextRec;

        new_ldP->wrtPgD = (wtPgDescT *)ms_malloc(sizeof(wtPgDescT) * (new_ldP->pgCnt/4 + 1));
        if (new_ldP->wrtPgD == NULL)
            RAISE_EXCEPTION( ENO_MORE_MEMORY );


    } else {
        /*
         * The log exists so we have to find the end and initialize
         * the log descriptor accordingly.
         */

        sts = setup_log_desc( new_ldP, find_log_end_pg( new_ldP ), nextLogRec );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    *ldP = new_ldP;
    *logbfap = logAccP;

    return EOK;

HANDLE_EXCEPTION:

    if (openedLog) {
        (void) bs_close(logAccP, MSFS_DO_VRELE);
    }

    if (new_ldP) {
        ms_free( new_ldP );
    }

    *ldP = NULL;
    *logbfap = NULL;

    return sts;
}


/*
 * lgr_close -
 *
 * Close a log.
 *
 * NOTES:
 *
 *    1) This routine works for the domain log only.  It needs to be
 *       revisited when common logging is implemented.  Specifically,
 *       questions related to multiple open/close of same log are not
 *       methodically dealt with.
 */

statusT
lgr_close(
    logDescT  *ldP          /* in - pointer to an open log */
    )
{
    statusT sts;
    int rdDesc;
    DEFINE_LOCK_FLAGS;

    MS_SMP_ASSERT(ldP);

    DESCRIPTOR_WRITE_LOCK;
    log_flush_sync( ldP, ldP->flushLsn);

    /*
     * Deallocate all read_page descriptors
     */
    for (rdDesc = 0; rdDesc < MAX_RD_PGS; rdDesc++) {
        if (ldP->rdPgD[rdDesc] != NULL) {
            if (ldP->rdPgD[rdDesc]->refed) {
                sts = bs_derefpg( ldP->rdPgD[rdDesc]->refH, BS_CACHE_IT );
                if (sts != EOK) {
                    DESCRIPTOR_UNLOCK;
                    domain_panic(ldP->dmnP, "lgr_close: derefpg failed");
                    return sts;
                }
            }

            ms_free( ldP->rdPgD[rdDesc] );
            ldP->rdPgD[rdDesc] = NULL ;
        }
    }

    /*
     * Flush the log to disk.
     */

    log_flush( ldP );

    /*
     * Must unlock the log descriptor (and the log mutex) before
     * calling bs_close(), and lgr_print_stats().
     */
    DESCRIPTOR_UNLOCK;

    /*
     * Close the log bitfile.
     */

    sts = bs_close(ldP->logAccP, MSFS_DO_VRELE);

    /* Free the log descriptor */

    lock_terminate( &ldP->descLock );
    lock_terminate( &ldP->flushLock);

    if ( !(ldP->switching) ) ldP->dmnP->ftxLogP = NULL;

    ms_free(ldP->wrtPgD);

    *ldP = NilLogDesc;
    ms_free( ldP );

    return sts;
}


/*
 * lgr_flush -
 *
 * Writes the log's dirty pages to disk.  Actually, this is just an
 * external interface (jacket routine) for the real log_flush() routine.
 */

void
lgr_flush(
    logDescT *ldP          /* in - pointer to an open log */
    )
{
    DEFINE_LOCK_FLAGS;

    MS_SMP_ASSERT(ldP);

    DESCRIPTOR_WRITE_LOCK;
    log_flush( ldP );

    DESCRIPTOR_UNLOCK;
}


/*
 * lgr_flush_start -
 *
 * Given an LSN this routine starts flushing all pages that contain records
 * with LSN less than or equal to the specified LSN.  In other words, it
 * flushes the log upto and including the specified LSN.  To synchronize
 * with the pages being flushed a thread must call lgr_flush_sync().
 */

statusT
lgr_flush_start(
    logDescT *ldP,      /* in - pointer to an open log */
    int noBlock,        /* in - if true don't block if log is locked */
    lsnT lsn            /* in - flush log upto and including this LSN */
    )
{
    statusT sts;
    lsnT flush_lsn = lsn;
    DEFINE_LOCK_FLAGS;

    MS_SMP_ASSERT(ldP);

    if (noBlock) {
        /* Try to get the lock, but if we can't, just return E_WOULD_BLOCK.
         * This prevents a deadlock if the calling thread already has a
         * logDescT already locked.
         */
        if (!DESCRIPTOR_TRY_WRITE_LOCK){
            return E_WOULD_BLOCK;
        }
    } else {
        DESCRIPTOR_WRITE_LOCK;
     }

    /* If the flushing bit is already set then we know the last page
     * is being flushed. Thus we can skip starting the flush since
     * all outstanding ftxs are being flushed
     */

    if ( !ldP->flushing &&
         (LSN_EQ_NIL( flush_lsn ) ||
          (LSN_GT(flush_lsn,ldP->flushLsn) && 
           LSN_GT( flush_lsn, ldP->logAccP->hiFlushLsn))) )
    {

        int flushlastpage=FALSE;

        /*
         * We need to flush some portion of the log.
         */
        FLUSH_WRITE_LOCK;

        if ((ldP->lastPg.pinned) &&
            ((LSN_EQ_NIL( flush_lsn )) ||
             (LSN_GTE( flush_lsn, ldP->lastPg.pgP->hdr.thisPageLSN ))))
        {
            
            /*
             * We need to flush the entire log, including the currently
             * pinned (in use) log page.
             * If there are no writers on the page currently then go ahead 
             * and unpin it now. If there are writers then fall through
             * and start the flush and set the flushing bit to signal to
             * the active writers that the last writer needs to unpin the
             * page.
             */

            if (ldP->lastPg.writers == 0)
            {

                sts = lgr_unpinpg( ldP,
                                   ldP->lastPg.refH,
                                   ldP->lastRec,
                                   &ldP->lastPg );
                if (sts != EOK) {
                    RELEASE_LOCKS;
                    domain_panic(ldP->dmnP, "lgr_flush_start: unpinpg failed");
                    return sts;
                }
            }
            flush_lsn = ldP->lastRec.lsn;
            flushlastpage=TRUE;
        }
        else if (LSN_EQ_NIL( flush_lsn ))
        {

            flushlastpage=TRUE;
            flush_lsn = ldP->lastRec.lsn;
        }


        bs_logflush_start( ldP->logAccP, flush_lsn );

        /* If the last page is not within the flush range it really isn't
         * necessary to set the flushing bit.
         */

        if(flushlastpage)
        {
            /* Indicate to future writers that they must wait for the
             * flush to complete. Also indicate to active writers that
             * the last page needs to be unpinned.
             */

            ldP->flushing = TRUE;
            ldP->flushLsn = flush_lsn;
        }

        FLUSH_UNLOCK;
    }

    DESCRIPTOR_UNLOCK;
    return EOK;
}


/*
 * lgr_flush_sync
 *
 * Call buffer cache code to block until log page
 * containing the given lsn has been flushed.  This
 * actually an external interface (jacket routine)
 * for log_flush_sync().
 */

statusT
lgr_flush_sync(
     logDescT *ldP,
     lsnT lsn
     )
{
    DEFINE_LOCK_FLAGS;

    MS_SMP_ASSERT(ldP);

    DESCRIPTOR_WRITE_LOCK;

    log_flush_sync( ldP, lsn );

    DESCRIPTOR_UNLOCK;
    return( EOK );
}


/*
 * log_flush
 *
 * Used by lgr_writev() to flush the log.  The current page is
 * reused after the flush (to not waste log space).
 *
 * Assumes that the log flush lock is held exclusively
 *
 * TODO: add 'group commit'.
 */

static void
log_flush(
    logDescT *ldP
    )
{
    statusT sts;
    DEFINE_LOCK_FLAGS;

    FLUSH_WRITE_LOCK;

    if ((ldP->lastPg.pinned) ||
        (LSN_GT(ldP->lastRec.lsn, ldP->logAccP->hiFlushLsn)))
    {
        if (ldP->lastPg.pinned)
        {
            if (ldP->lastPg.writers == 0)
            {
                /*
                 * We have a log page pinned.  Unpin it and flush
                 * the log upto the current page.
                 */

                sts = lgr_unpinpg( ldP,
                                   ldP->lastPg.refH,
                                   ldP->lastRec,
                                   &ldP->lastPg );
                if (sts != EOK) {
                    FLUSH_UNLOCK;
                    domain_panic(ldP->dmnP, "log_flush: unpinpg error");
                    return ;
                }
                
            }
        }

    } else {
        /* Nothing to flush */

        FLUSH_UNLOCK;

        return;
    }

    bs_logflush_start( ldP->logAccP, ldP->lastRec.lsn );
    
    ldP->flushing = TRUE;
    ldP->flushLsn = ldP->lastRec.lsn;

    FLUSH_UNLOCK;

    log_flush_sync( ldP, ldP->lastRec.lsn );

}


/*
 * log_flush_sync
 *
 * Call buffer cache code to block until log page
 * containing the given lsn has been flushed.
 *
 * Generally the caller would have called log_flush_start() to
 * begin flushing the log.  This routine is then used to wait
 * for the flush to finish.
 *
 * Assumes the log desciptor is locked.
 */


static void
log_flush_sync(
    logDescT *ldP,      /* in - log desc */
    lsnT lsn            /* in - lsn to sync to */
    )
{
    statusT sts;
    DEFINE_LOCK_FLAGS;


    DESCRIPTOR_UNLOCK;
    sts = bfflush_sync(ldP->logAccP, lsn );
    DESCRIPTOR_WRITE_LOCK;

    /* We need to be careful here. This sync may have already been serviced
     * by a previous thread and a new flush may have been started. We must
     * not clear the flushing flag or attempt to pin the last page if this
     * is the case. 
     * We will know this by checking the current flushLsn.
     */

    FLUSH_WRITE_LOCK;
    if (LSN_LT(lsn,ldP->flushLsn))
    {

        FLUSH_UNLOCK;
        return;
    }

    if (ldP->flushing) {

        /* If the flushing bit is still set then we know that no other
         * writers came along after the flush was started. We might as
         * well set up the last page to be reused. This poses a problem
         * that must be dealt with in recovery if this code is left here.
         * Basically we are repinning a page that has already been written
         * to disk once. Lets say we are page 10. If page 11 now gets pinned
         * and written out to disk prior to us being written out for the
         * 2nd time (this is possible) then the records that are written
         * during the repin will be lost but pointed to by page 11.
         */

        /*
         * Continue using the current last log page.
         */

        MS_SMP_ASSERT(!ldP->lastPg.pinned);

        /*
         * Make page look full.  This will cause lgr_writev()
         * to go to another page.
         */
            
        (ldP->dmnP)->logStat.logWrites++;
        (ldP->dmnP)->logStat.wastedWords += DATA_WORDS_PG - ldP->nextRec.offset;
        ldP->nextRec.offset = DATA_WORDS_PG;

        MS_SMP_ASSERT(SEQ_LTE(ldP->flushLsn, ldP->logAccP->hiFlushLsn));
        ldP->flushing = FALSE;
    }

    FLUSH_UNLOCK;
}


/*
 * lgr_get_last_rec -
 *
 * Returns the log record address of the
 * last record in the log.
 */

statusT
lgr_get_last_rec(
    logDescT *ldP,              /* in - pointer to an open log */
    logRecAddrT *recAddr        /* out - rec addr of last log rec */
    )
{
    statusT sts;
    DEFINE_LOCK_FLAGS;


    if (ldP == NULL)
        {RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );}

    DESCRIPTOR_WRITE_LOCK;

    if (LOG_EMPTY( ldP )) {
        sts = E_LOG_EMPTY;
    } else {
        /*
         * Return the record address of
         * the first segment of the last record written.
         */
        *recAddr = ldP->lastRecFirstSeg;
        sts = EOK;
    }

    DESCRIPTOR_UNLOCK;
    return( sts );

HANDLE_EXCEPTION:

    RELEASE_LOCKS;
    return( sts );
}


/*
 * lgr_dmn_get_last_rec -
 *
 * same as lgr_last_rec except for domain's log
 */

statusT
lgr_dmn_get_last_rec(
    domainT  *dmnP,         /* in - domain pointer */
    logRecAddrT *recAddr    /* out - rec addr of last log rec */
    )
{
    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }
    return( lgr_get_last_rec( dmnP->ftxLogP, recAddr ) );
}


/*
 * lgr_get_first_rec -
 *
 * Returns the log record address of the first record
 * in the log.
 */
statusT
lgr_get_first_rec(
    logDescT  *ldP,         /* in - pointer to an open log */
    logRecAddrT *recAddr    /* out - rec addr of first log rec */
    )
{
    statusT sts;
    DEFINE_LOCK_FLAGS;


    if (ldP == NULL)
        return E_INVALID_LOG_DESC_POINTER;

    DESCRIPTOR_WRITE_LOCK;

    if (LOG_EMPTY( ldP )) {
        sts = E_LOG_EMPTY;
    } else {
        *recAddr = ldP->firstRec;
        sts = EOK;
    }

    DESCRIPTOR_UNLOCK;
    return( sts );
}


/*
 * lgr_dmn_get_first_rec -
 *
 * same as lgr_first_rec except for domain's log
 */

statusT
lgr_dmn_get_first_rec(
    domainT *dmnP,          /* in - domain pointer */
    logRecAddrT *recAddr    /* out - rec addr of first log rec */
    )
{
    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }

    return( lgr_get_first_rec( dmnP->ftxLogP, recAddr ) );
}


/*
 * lgr_dmn_get_pseudo_first_rec - Returns the address of the record
 * that would be the first log record if the log had not been
 * trimmed.
 */

statusT
lgr_dmn_get_pseudo_first_rec(
    domainT  *dmnP,         /* in - domain pointer */
    logRecAddrT *recAddr    /* out - log record address */
    )
{
    logDescT *ldP;
    statusT sts;
    int nextPg;
    int pgReferenced = 0;
    bfPageRefHT pgRef;
    logPgT *logPgP;
    DEFINE_LOCK_FLAGS;


    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }
    ldP = dmnP->ftxLogP;
    if (ldP == NULL)
        {RAISE_EXCEPTION( E_INVALID_LOG_DESC_POINTER );}

    DESCRIPTOR_WRITE_LOCK;

    if (!ldP->logWrapStateKnown) {
        /*
         ** Figure out if the log has wrapped.  Basically, if the last log
         ** page has not been written (it is all zeros) then the log has
         ** NOT wrapped.
         */
        sts = lgr_refpg( &pgRef,
                         &logPgP,
                         ldP->logAccP,
                         ldP->pgCnt - 1,
                         BS_NIL );
        if (sts != EOK) {RAISE_EXCEPTION( E_CANT_ACCESS_LOG );}
        pgReferenced = 1;

        if (LSN_EQ_NIL( logPgP->hdr.thisPageLSN )) {
            ldP->logWrapped = 0;
        } else {
            ldP->logWrapped = 1;
        }

        ldP->logWrapStateKnown = 1;

        sts = bs_derefpg( pgRef, BS_CACHE_IT );
        if (sts != EOK) {RAISE_EXCEPTION( E_CANT_ACCESS_LOG );}
        pgReferenced = 0;
    }

    /*
     ** Figure out which log record address to return.
     */

    if (LSN_EQL( ldP->nextRec.lsn, firstLSN ) ) {
        RAISE_EXCEPTION( E_LOG_EMPTY );
    } else {
        if (ldP->logWrapped) {
            /*
             ** The log has wrapped (common condition).  Return the address
             ** of the first record on the page that follows the current
             ** end-of-log page (lsP->nextRec.page).
             */
            nextPg = (ldP->nextRec.page + 1) % ldP->pgCnt;

            recAddr->page = nextPg;
            recAddr->offset = 0;

        } else /* if (!ldP->logWrapped) */ {
            /*
             ** The log has not wrapped.  Return the address of the
             ** first record in the first page.
             */

            recAddr->page = 0;
            recAddr->offset = 0;
        }

        sts = bs_refpg( &pgRef, (void*)&logPgP, ldP->logAccP,
                     recAddr->page, BS_NIL );

        if (sts != EOK) {RAISE_EXCEPTION( E_CANT_ACCESS_LOG );}
        pgReferenced = 1;

        recAddr->lsn = ((logRecHdrT *) &logPgP->data[0])->lsn;

        sts = bs_derefpg( pgRef, BS_CACHE_IT );

        if (sts != EOK) {RAISE_EXCEPTION( E_CANT_ACCESS_LOG );}
        pgReferenced = 0;
    }

    DESCRIPTOR_UNLOCK;
    return( EOK );

HANDLE_EXCEPTION:

    if (pgReferenced) {
        (void )bs_derefpg( pgRef, BS_CACHE_IT );
    }

    RELEASE_LOCKS;
    return( sts );
}


/*
 * lgr_checkpoint_log
 *
 * This function starts an exclusive transaction, flushes the domain and
 * ends the transaction.  When this function returns, the only record in
 * the log is the one for this function's transaction.  This has the
 * effect of checkpointing the log.
 */

void
lgr_checkpoint_log (
    domainT *dmnP  /* in */
    )
{
    ftxHT ftxH;
    statusT sts;

    sts = FTX_START_EXC(FTA_NULL, &ftxH, dmnP, 0);
    if (sts != EOK) {
        domain_panic (dmnP, "lgr_checkpoint_log: FTX_START_EXC() failed");
        return;
    }

    /*
     * Flush the log and all the buffers in the domain.
     */
    bs_bfdmn_flush (dmnP);
    (void) bs_bfdmn_flush_sync (dmnP);

    ftx_special_done_mode (ftxH, FTXDONE_LOGSYNC);
    ftx_done_n (ftxH, FTA_FTX_CHECKPOINT_LOG);

    return;
}  /* end lgr_checkpoint_log */


/*
 * lgr_switch_vol
 *
 * Creates a new log on the specified disk and switches the current log
 * pointer to point to the new log.  This effectively switches a domains
 * log to a new log.  The old log is deleted.  Optionally, the new log's
 * size can be changed to something different from the old log.  The new
 * log's service class can also be changed.
 *
 * NOTE: The caller must not hold any locks before calling this function.
 */

statusT
lgr_switch_vol(
    logDescT *ldP,        /* in - pointer to an open log */
    vdIndexT newVolIdx,   /* in - move log to this vdIndex */
    uint32T newNumPgs,    /* in - if > 0 then change log's size */
    serviceClassT logSvc, /* in - if != 0 then change log service class */
    long xid              /* in - CFS transaction id */
    )
{
    statusT sts;
    logDescT *newldP, *newLogP;
    bfAccessT *logAccP, *newAccP;
    domainT *dmnP;
    vdT *vdP, *newVdP;
    uint32T logPgs;
    bfTagT oldTag, newTag;
    bfAccessT *newLogBfap;
    logRecAddrT nextLogAddr;
    int newLogOpen = FALSE, excFtxStarted = FALSE, ftxStarted = FALSE;
    ftxHT ftxH = FtxNilFtxH, excFtxH = FtxNilFtxH;
    bsDmnMAttrT dmnMAttr, *dmnMAttrP;
    bsDmnTAttrT *dmnTAttrP;
    bsDmnFreezeAttrT dmnFreezeAttr;
    struct bsMPg* rbmtPgP;
    bfPageRefHT pgRef;
    logRecAddrT oldestFtxLa, dirtyBufLa;
    int attridx;
    bfAccessT *oldMdap, *newMdap;
    int srcVdpBumped = 0;
    int dstVdpBumped = 0;

    DEFINE_LOCK_FLAGS;

    MS_SMP_ASSERT(ldP);

    DESCRIPTOR_WRITE_LOCK;

    if (ldP->switching) {
        DESCRIPTOR_UNLOCK;
        return( E_ALREADY_SWITCHING_LOGS );
    }

    ldP->switching = TRUE;

    DESCRIPTOR_UNLOCK;

    logAccP = ldP->logAccP;
    dmnP = logAccP->dmnP;

    /* reclaim any old log CFS vnodes that are hanging around on free list */
    sts = bs_reclaim_cfs_rsvd_vn(logAccP);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* Be sure to bump the refCnt for both disks until we are done */
    vdP = vd_htop_already_valid(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP, TRUE );
    srcVdpBumped = 1;

    if ( newVolIdx == vdP->vdIndex ) {
        RAISE_EXCEPTION( EBAD_VDI );
    }
     
    if ( !(newVdP = vd_htop_if_valid(newVolIdx, dmnP, TRUE, FALSE)) ) {
        RAISE_EXCEPTION( EBAD_VDI );
    }
    dstVdpBumped = 1;

    BS_BFTAG_RSVD_INIT( newTag, newVolIdx, BFM_FTXLOG );

    if (newNumPgs != 0) {
        logPgs = lgr_calc_num_pgs( newNumPgs );
    } else {
        logPgs = dmnP->ftxLogPgs;
    }

    if (logSvc != 0) {
        /* TODO - not implemented */
        ;
    }

    /*
     * Open the new log.  Note that all disks have a ftx log
     * bitfile; they are all initialized to be empty.
     */

    sts = bfm_open_ms( &newAccP, dmnP, BS_BFTAG_VDI( newTag ), BFM_FTXLOG );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    newLogOpen = TRUE;

    if (newAccP->nextPage < logPgs ) {
        /*
         * Add disk storage to the new log.  Normally, the log
         * shouldn't have any storage at this point, but it is
         * possible that this is an old log that wasn't deleted (why?)
         * so we add only the number of pages that we need.
         * Storage needs to be obtained now instead of letting lgr_open
         * get it because we will be in an exclusive ftx and the call to
         * bs_add_stg would start a ftx and we would deadlock.
         */

        /* Prevent races with migrate code paths */
        MS_SMP_ASSERT(newAccP->bfSetp->cloneId == BS_BFSET_ORIG);
        MIGTRUNC_LOCK_READ( &(newAccP->xtnts.migTruncLk) );
        sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, newAccP->dmnP, 0 );
        if (sts != EOK) {
            MIGTRUNC_UNLOCK( &(newAccP->xtnts.migTruncLk) );
            RAISE_EXCEPTION( sts );
        }
        ftxStarted = TRUE;

        sts = rbf_add_stg( newAccP,
                           newAccP->nextPage,
                           logPgs - newAccP->nextPage,
                           ftxH, 1 );
        MIGTRUNC_UNLOCK( &(newAccP->xtnts.migTruncLk) );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    sts = log_init( newAccP, logPgs, dmnP->domainId );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bfflush(newAccP, 0, logPgs, FLUSH_IMMEDIATE);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if (ftxH.hndl != 0) {
       ftx_done_n( ftxH, FTA_LGR_SWITCH_VOL );
       ftxStarted = FALSE;
    }

    if ( RBMT_THERE(dmnP) ) {
        oldMdap = vdP->rbmtp;
        newMdap = newVdP->rbmtp;
        attridx = BFM_RBMT_EXT;
    } else {
        oldMdap = vdP->bmtp;
        newMdap = newVdP->bmtp;
        attridx = BFM_BMT_EXT_V3;
    }

    /* 
     * Try to put a BSR_DMN_FREEZE_ATTR on the new Log disk.
     * This record is for freezefs debugging only so we tolerate
     * failure. 
     */

    /* Get old BSR_DMN_FREEZE_ATTR if there, bzero new one if not. */

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP, 0 );
    if (sts == EOK) {

        bzero((char *)&dmnFreezeAttr, sizeof( bsDmnFreezeAttrT ));

        sts = bmtr_get_rec(oldMdap,
                           BSR_DMN_FREEZE_ATTR,
                           (void *) &dmnFreezeAttr,
                           sizeof( bsDmnFreezeAttrT ) );

        sts = bmtr_put_rec(newMdap,
                           BSR_DMN_FREEZE_ATTR,
                           (void *) &dmnFreezeAttr,
                           sizeof( bsDmnFreezeAttrT ),
                           ftxH);

    ftx_done_n( ftxH, FTA_NULL );

    }

    /*
     * We need to stop all ftx activity so that we can flush the log
     * and all ftx/log related buffers.  This effectively makes the
     * old log empty (fully checkpointed) which allows us to switch
     * to the new empty log.
     * NOTE: It is important that no other ftx's get started otherwise a
     * dead lock will occur.
     */

    sts = FTX_START_EXC_XID(FTA_NULL, &excFtxH, newAccP->dmnP, 0, xid );
    if (sts != EOK) {
        domain_panic(ldP->dmnP, "lgr_switch_vol: FTX_START_EXC() failed");
        RAISE_EXCEPTION( sts );
    }

    excFtxStarted = TRUE;

    /*
     * Flush the log and all ftx-related buffers.
     */
    bs_bfdmn_flush( dmnP );
    (void) bs_bfdmn_flush_sync( dmnP );

    oldestFtxLa = ftx_get_oldestftxla( dmnP );
    if (!LSN_EQ_NIL( oldestFtxLa.lsn )) {
        domain_panic(ldP->dmnP, "lgr_switch_vol: oldestFtxLa != NIL " );
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    dirtyBufLa = ftx_get_dirtybufla( dmnP );
    if (!LSN_EQ_NIL( dirtyBufLa.lsn )) {
        domain_panic(ldP->dmnP, "lgr_switch_vol: dirtyBufLa != NIL " );
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    /*
     * Update the log attributes on the log's disk to 'point'
     * to the new log.
     */

    sts = bmtr_get_rec(oldMdap,
                       BSR_DMN_MATTR,
                       (void *) &dmnMAttr,
                       sizeof( bsDmnMAttrT ) );
    if (sts != EOK) {
      RAISE_EXCEPTION( sts );
    }

    sts = bs_pinpg( &pgRef, (void*)&rbmtPgP, newMdap, 0, BS_NIL );
    if ( sts != EOK ) {
        RAISE_EXCEPTION( sts );
    }

    dmnMAttrP = bmtr_find( &rbmtPgP->bsMCA[ attridx ], BSR_DMN_MATTR,
                                                        vdP->dmnP);

    if (dmnMAttrP == NULL) {
        dmnMAttrP = bmtr_assign( BSR_DMN_MATTR,
                                sizeof( bsDmnMAttrT ),
                                &rbmtPgP->bsMCA[ attridx ] );
        if (dmnMAttrP == NULL) {
            bs_unpinpg( pgRef, logNilRecord, BS_CLEAN );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }
    }

    bcopy( (char *) &dmnMAttr, dmnMAttrP, sizeof( bsDmnMAttrT ) );
    dmnMAttrP->ftxLogTag = newTag;
    dmnMAttrP->ftxLogPgs = logPgs;
    dmnMAttrP->seqNum    = dmnMAttr.seqNum + 1;

    /*
     * Make sure there is a transient attribute record on the log's disk.
     */

    dmnTAttrP = bmtr_find( &rbmtPgP->bsMCA[ attridx ], BSR_DMN_TRANS_ATTR,
                                                        vdP->dmnP);

    if (dmnTAttrP == NULL) {
        dmnTAttrP = bmtr_assign( BSR_DMN_TRANS_ATTR,
                                sizeof( bsDmnTAttrT ),
                                &rbmtPgP->bsMCA[ attridx ] );
        if (dmnTAttrP == NULL) {
            bs_unpinpg( pgRef, logNilRecord, BS_DIRTY );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        dmnTAttrP->chainVdIndex = -1;
        dmnTAttrP->chainMCId = bsNilMCId;
        dmnTAttrP->op = BSR_VD_NO_OP;
        dmnTAttrP->dev = 0;
    }

    sts = bs_unpinpg( pgRef, logNilRecord, BS_WRITETHRU );
    
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * From here on we crash if something goes wrong.
     */

    dmnP->ftxLogPgs  = logPgs;  /* update now since lgr_open needs it */

    /*
     * Open the new log.  This sets up a new log descriptor for us.
     */

    sts = lgr_open( &newLogP,
                    &nextLogAddr,
                    &newLogBfap,
                    newTag,
                    dmnP->bfSetDirp,
                    0);
    if (sts != EOK) {
        domain_panic(dmnP, "lgr_switch_vol: log_open failed");
        RAISE_EXCEPTION( sts );
    }

    if (newAccP != newLogBfap) {
        domain_panic(dmnP, "lgr_switch_vol: mismatched access structure pointers");
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    bs_close(newAccP, 0); /* for the bfm_open_ms() earlier */
    newLogOpen = FALSE;

    /*
     * Change current log pointer to use the new log descriptor.
     * This is where we really switch the logs.
     */

    newldP = newLogP;
    newldP->nextRec.lsn  = ldP->nextRec.lsn;

    /*
     * Update domain to point to new log.
     */

    oldTag           = dmnP->ftxLogTag;
    dmnP->logAccessp = newLogBfap;
    dmnP->ftxLogTag  = newTag;
    dmnP->ftxLogPgs  = logPgs;
    dmnP->ftxLogP    = newldP;

    /* assert that the log has valid in-mem structures */
    MS_SMP_ASSERT(dmnP->logAccessp->xtnts.validFlag);
    /* then proceed to figure out which RAD the log's vd struct is on */
    mutex_lock( &dmnP->vdpTblLock );
    dmnP->logVdRadId = PA_TO_MID(vtop(current_processor(), 
                                 dmnP->vdpTbl[dmnP->logAccessp->xtnts.xtntMap->subXtntMap->vdIndex-1]));
    mutex_unlock( &dmnP->vdpTblLock );

    /*
     * Close the old log.
     */

    lgr_close( ldP );

    ftx_done_n( excFtxH, FTA_FTX_SWITCH_LOG );
    excFtxStarted = FALSE;

    /*
     * Access old log. It would be nice to deallocate the disk storage
     * but deallocation of reserved storage is not supported yet.
     */

    sts = bfm_open_ms(&logAccP, dmnP, BS_BFTAG_VDI( oldTag ), BFM_FTXLOG);
    if (sts != EOK) {
        vd_dec_refcnt( vdP );
        vd_dec_refcnt( newVdP );
        return( sts );
    }

    /* close and invalidate old log bitfile */

    bs_invalidate_rsvd_access_struct(dmnP, oldTag, logAccP);

    /* write out the vfast config record to the new log disk */
    ss_put_rec(dmnP);

    /* Decrement vdRefCnt on both volumes */
    vd_dec_refcnt( vdP );
    vd_dec_refcnt( newVdP );

    return EOK;

HANDLE_EXCEPTION:

    if (excFtxStarted) {
        ftx_fail( excFtxH );
    }

    if (ftxStarted) {
        ftx_done_n( ftxH, FTA_LGR_SWITCH_VOL );
    }

    if (newLogOpen) {
        bs_close(newAccP, 0);
    }

    DESCRIPTOR_WRITE_LOCK;
    ldP->switching = FALSE;
    DESCRIPTOR_UNLOCK;

    if ( srcVdpBumped ) {
        vd_dec_refcnt( vdP );
    }
    if ( dstVdpBumped ) {
        vd_dec_refcnt( newVdP );
    }

    return sts;
}


/*
 * lgr_calc_num_pgs
 *
 * Given a 'requested number of log pages' this routine returns a
 * valid interpretation of this request.  It is used to determine the
 * log size (in pages).  There are several rules (see code) that
 * determine the min and max log pages and whether or not the log
 * must have an integral number of log pages.
 */

int
lgr_calc_num_pgs(
    int requestedLogPgs
    )
{
    int logPgs = 0;

    /*
     * Note that the log size must be a multiple of 4 so that the quadrants
     * will work correctly.
     */

    if ((logPgs = (requestedLogPgs/4*4)) == 0) {
        /* use default */
        logPgs = BS_DEF_LOG_PGS;
    }

    logPgs = MAX( logPgs, BS_MIN_LOG_PGS );
    logPgs = MAX( logPgs, LOG_FLUSH_THRESHOLD * 4 );

    /*
     * Note that 65536 is not a valid page number. So the max log size
     * is (((65536 / 4) - 1) * 4) == 65532; the next lowest number
     * divisible by 4.  Log page numbers are unsigned short ints.
     */

    logPgs = MIN( logPgs, 65532 );

    return logPgs;
}


/*
 * lgr_create -
 *
 * NOT IMPLEMENTED BECAUSE THE WE DON'T SUPPORT COMMON LOGGING
 * AND THE DOMAIN LOG IS CREATED BY DISK_INIT().
 */

statusT
lgr_create( void /* ???? */ )
{
    return ENOT_SUPPORTED;
}
