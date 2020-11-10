/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 *      Advanced File System
 *
 * Abstract:
 *
 *      On-disk structure dumper
 *
 * Date:
 *
 *      Thu Nov 17 09:06:33 PST 1994
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: vlogpg.h,v $ $Revision: 1.1.6.2 $ (DEC) $Date: 1993/12/1
6 22:26:53 $
 */

/*
 * This file was created for vlogpg to have access to on-disk log structures.
 * Currently these definitions are embedded within the ms_logger.c module.
 * This code should be broken out for use outside of ms_logger.c.  Until that
 * time this file should be maintained in sync by hand.
 */


/*
 * logBlkT -
 *
 * Defines the structure of a log page block.  The LSN of a page's
 * blocks must match in order for the page to be valid.
 */
typedef struct {
    lsnT lsn;
    char data[BS_BLKSIZE - sizeof( lsnT )];
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
 * Number words per log page header  
 */
#define PG_HDR_WORDS howmany( sizeof( logPgHdrT ), sizeof( uint32T ) )

/*
 * Max number of records per log page (empty records!).
 */
#define LOG_RECS_PG (DATA_WORDS_PG / REC_HDR_WORDS )

