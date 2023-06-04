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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <assert.h>
#include <sys/param.h>

#include <advfs/ms_public.h>
#include <advfs/ms_privates.h>
#include <advfs/ftx_privates.h>
#include <advfs/bs_public.h>

#include "vods.h"

void usage();

/* print_log.c prototypes */

void print_loghdr( FILE*, logPgHdrT*, int, int);
void print_logrec( logRecHdrT*, int, int);

/* private prototypes */

static int find_LOG( bitFileT*);
static void process_summary( bitFileT*);
static void save_log( bitFileT*, char**, int, int, int);
static void process_block( bitFileT*, char**, int, int, int);
static void print_block( bitFileT*, uint64_t, int64_t, int);
static void process_all( bitFileT*, char**, int, int, int);
static void process_ends( bitFileT*, char**, int, int, int);
static void process_range( bitFileT*, char**, int, int, int);
static void process_page( bitFileT*, bs_meta_page_t, int64_t, int64_t, int);
static void print_logpage( logPgT*, uint64_t, int64_t, int64_t, int);
static void restore_logpage( logPgT*);
static void find_first_n_last_rec( bitFileT*, int64_t*, int64_t*, 
				   int64_t*, int64_t*);
static int log_wrapped(bitFileT *bfp);


void
log_main(argc,argv)
int argc;
char *argv[];
{
    bitFileT bf;
    bitFileT  *bfp = &bf;
    int flags = 0;
    int64_t longnum;
    uint64_t blknum = LBN_BLK_TERM;
    bs_meta_page_t pagenum = 0;
    int64_t offset = -1;
    bs_meta_page_t start_pg;
    int64_t start_off;
    bs_meta_page_t end_pg;
    int64_t end_off;
    logRecAddrT crashRedo;
    int ret;
    extern int optind;

    init_bitfile(bfp);
    BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, BFM_FTXLOG);

    ret = resolve_src(argc, argv, &optind, "rvTf:", bfp, &flags);

    /* "-b" must work even on a badly corrupted FS, so service block
       processing without checking error return status */
    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pmcell.volume, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    /* The log file is not associated with an individual disk.  There
       is only one log file in the entire domain, so there's no use in
       entering a volume spec for the log command, except with the
       "-b" option (see above). */

    if (bfp->type & VOL) {
	fprintf(stderr, "The 'log' command does not accept a volume.\n");
	exit(BAD_PARAM);
    }

    if ( find_LOG(bfp) != OK ) {
        exit(3);
    }

    if ( argc == optind ) {
        process_summary(bfp);
        /* NOT REACHED */
    }

    /* dump the log to a file */
    if ( strcmp(argv[optind], "-d") == 0 ) {
        save_log(bfp, argv, argc, optind, flags);
    }

    /* display all the log */
    if ( strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags);
    }

    if ( strcmp(argv[optind], "-R") == 0 ) {
        process_range(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }    

    if ( strcmp(argv[optind], "-s") == 0 || strcmp(argv[optind], "-e") == 0 ) {
        process_ends(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    if ( argc > optind + 4) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    /* get page number */

    if ( strcmp(argv[optind], "-p") == 0 ) {

        if ( argc == ++optind) {
            fprintf(stderr, "missing page number\n");
	    exit(1);
	}

	if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", 
		    argv[optind]);
            exit(1);
        }

        optind++;

        if ( pagenum > bfp->pages || pagenum < 0 ) {
            fprintf(stderr, "Invalid LOG page number (%ld). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %ld.\n",
              bfp->pages - 1);
            exit(2);
        }

	/* get record offset */
	if ( argc > optind ) {
	    if ( getnum(argv[optind], &offset) != OK ) {
	        fprintf(stderr, "Badly formed offset number: \"%s\"\n",
			argv[optind]);
		exit(1);
	    }

	    optind++;

	    if ( offset < 0 || offset > ADVFS_METADATA_PGSZ - 1 ) {
	        fprintf(stderr, "Invalid record offset (%ld). ", offset);
		fprintf(stderr, "Valid offsets range from 0 to %d.\n", ADVFS_METADATA_PGSZ-1);
		exit(2);
	    }
	}

	if ( argc > optind ) {
	    if ( strcmp(argv[optind], "-c") == 0 ) {
	        optind++;
		flags |= CFLG;
	    } else {
	        usage();
	    }
	}
	process_page(bfp, pagenum, offset, offset, flags);
	exit(0);

    } else {
        fprintf(stderr, "Unexpected argument '%s'.\n", argv[optind]);
        usage();
    }


} /* log_main */

/***************************************************************************/
static int
find_LOG(bitFileT *bfp)
{
    vdIndexT vdi;
    int ret;
    bsXtntRT *xtntrp;
    bitFileT *bmtBfp;
    uint16_t mc;
    bsMCT *mcellp;

    assert(bfp);
    bfp->setTag = staticRootTagDirTag;
    bfp->pmcell.page = 0;

    if ( bfp->type & FLE ) {
        BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, BFM_FTXLOG);
        bfp->pmcell.cell = BFM_FTXLOG;
        return OK;
    }

    assert(bfp->dmn);

    mc = BFM_FTXLOG;

    for ( vdi = 0; vdi < BS_MAX_VDI; vdi++ ) {
        if ( FETCH_MC_VOLUME(bfp->pmcell) && vdi != FETCH_MC_VOLUME(bfp->pmcell) ) {
            continue;
        }

        if ( bfp->dmn->vols[vdi] ) {
            bmtBfp = bfp->dmn->vols[vdi]->rbmt;
            assert(bmtBfp);

            if ( read_page(bmtBfp, 0) != OK ) {
                fprintf(stderr,
                 "find_LOG: Warning. read_page 0 failed for RBMT on volume %d.\n",
			vdi);
                continue;
            }

            mcellp = &((bsMPgT *)PAGE(bmtBfp))->bsMCA[mc];
            if ( (int64_t)mcellp->mcTag.tag_num != RSVD_TAGNUM_GEN(vdi, mc) ) {
                fprintf(stderr,
 "find_LOG: Warning. Wrong tag in cell %d page 0 of the RBMT on volume %d. Expected %ld, read %ld\n",
                  mc, vdi, RSVD_TAGNUM_GEN(vdi, mc), (int64_t)mcellp->mcTag.tag_num);
                /* FIX. "Tag %d is not the tag of a LOG file.\n", */
                continue;
            }

            if ( find_bmtr(mcellp, BSR_XTNTS, (char**)&xtntrp) != OK ) {
                fprintf(stderr,
 "find_TAG: Warning. No BSR_XTNTS record at cell %d page 0 of the RBMT on volume %d\n",
                  mc, vdi);
                /* FIX. "Can't find BSR_XTNTS record for LOG file.\n" */
                continue;
            }

            if ( xtntrp->mcellCnt == 1 &&
                   xtntrp->xCnt == 1 )
            {
                /* no log on this volume */
                continue;
            }

            if ( xtntrp->mcellCnt != 1 )
                fprintf(stderr, "Log has more than one extent.\n");

            bfp->pmcell.cell = mc;
            bfp->pmcell.volume = vdi;
            bfp->pmcell.page = 0;
            BS_BFTAG_RSVD_INIT( bfp->fileTag, vdi, mc );
            bfp->setTag = staticRootTagDirTag;

            ret = load_xtnts(bfp);

            return ret;
        }
    }

    fprintf(stderr, "Could not find LOG in filesystem %s\n", 
	    bfp->dmn->dmnName);
    return BAD_DISK_VALUE;
}

/***************************************************************************/
static void
process_summary(bitFileT *bfp)
{
    bs_meta_page_t start_pg;
    int64_t start_off;
    bs_meta_page_t end_pg;
    int64_t end_off;
    bs_meta_page_t pages;

    bfp->pgVol = FETCH_MC_VOLUME(bfp->pmcell);
    bfp->pgLbn = LBN_BLK_TERM;
    bfp->pgNum = BMT_PG_TERM;
    print_header(bfp);

    printf("The log is %ld pages long.\n", bfp->pages);
    if ( bfp->xmap.cnt > 2 )
        printf("The log is not contiguous!\n");

    find_first_n_last_rec(bfp, &start_pg, &start_off, &end_pg, &end_off);

    if ( start_pg == end_pg )
        printf("There is 1 page of active records in the LOG.\n");
    else {
        if ( end_pg > start_pg )
            pages = end_pg - start_pg + 1;
        else
            pages = bfp->pages + end_pg - start_pg + 1;

        printf("There are %ld pages of active records in the LOG.\n", pages);
    }

    printf("The first LOG record is on page %ld at offset %ld.\n",
      start_pg, start_off);
    printf("The last LOG record is on page %ld at offset %ld\n", end_pg, end_off);

    exit(0);
}

/***************************************************************************/
static void
save_log(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile = NULL;

    optind++;
    if ( argc == optind + 1 ) {
        dfile = argv[optind];
    } else if ( argc == optind ) {
        fprintf(stderr, "Missing dump file name.\n");
        usage();
    } else if ( argc > optind + 1 ) {
        fprintf(stderr, "No other arguments allowed with -d.\n");
        usage();
    }

    if ( dumpBitFile(bfp, dfile) ) {
        exit(3);
    }

    exit(0);
}

/***************************************************************************/
static void
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    uint64_t blknum;
    int64_t offset = -1;
    int64_t longnum;

    assert(bfp->fd);
    if ( bfp->type & FLE ) {
        fprintf(stderr, "Cannot use -b flag on a file\n");
        usage();
    }

    if ( strlen(argv[optind]) == 2 ) {
        optind++;
        if ( argc > optind ) {
            if ( getnum(argv[optind], &longnum) != OK ) {
                fprintf(stderr, "Badly formed block number: \"%s\"\n",
                  argv[optind]);
                exit(1);
            }
            optind++;
        } else if ( argc == optind ) {
            fprintf(stderr, "Missing block number.\n");
            usage();
        } 
    } else {
        if ( getnum(argv[optind], &longnum) != OK ) {
            fprintf(stderr, "Badly formed block number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
    }

    blknum = (uint64_t)longnum;
    if ( bfp->dmn->vols[bfp->pmcell.volume]->blocks != 0 ) {
        if ( blknum >= bfp->dmn->vols[bfp->pmcell.volume]->blocks ) {
            fprintf(stderr, "Invalid block number (%ld). ", blknum);
            fprintf(stderr, "Valid block numbers range from 0 to %ld.\n",
              bfp->dmn->vols[bfp->pmcell.volume]->blocks - ADVFS_METADATA_PGSZ_IN_BLKS);
            exit(2);
        }
    }

    if ( blknum / ADVFS_METADATA_PGSZ_IN_BLKS * ADVFS_METADATA_PGSZ_IN_BLKS != blknum ) {
        fprintf(stderr, "LOG pages are on %d block boundaries.\n",
          ADVFS_METADATA_PGSZ_IN_BLKS);
        blknum = blknum / ADVFS_METADATA_PGSZ_IN_BLKS * ADVFS_METADATA_PGSZ_IN_BLKS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%ld).\n",
          ADVFS_METADATA_PGSZ_IN_BLKS, blknum);
    }

    if ( argc > optind ) {
        if ( getnum(argv[optind], &offset) != OK ) {
            fprintf(stderr, "Badly formed offset number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
    }

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-c") == 0 ) {
            optind++;
            flags |= CFLG;
        } else {
            usage();
        }
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    print_block(bfp, blknum, offset, flags);
    exit(0);
}

/***************************************************************************/
static void
print_block(bitFileT *bfp, uint64_t lbn, int64_t offset, int flags)
{
    char page[ADVFS_METADATA_PGSZ];
    int result;

    assert(bfp->fd >= 0);
    if ( lseek(bfp->fd, (lbn) * DEV_BSIZE, SEEK_SET) == -1 ) {
        perror("print_block");
        exit(3);
    }

    result = read(bfp->fd, page, ADVFS_METADATA_PGSZ);
    if ( result == -1 ) {
        perror("print_block");
        exit(3);
    } else if ( result != ADVFS_METADATA_PGSZ ) {
        fprintf(stderr, "Truncated read. Read page %ld returned %d.\n",
          page, result);
        exit(3);
    }

    bfp->pgVol = FETCH_MC_VOLUME(bfp->pmcell);
    bfp->pgLbn = lbn;
    bfp->pgNum = offset;
    print_header(bfp);

    print_logpage((logPgT *)page, lbn, offset, offset, flags);
}

/***************************************************************************/
static void
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bs_meta_page_t pagenum;
    bs_meta_page_t last_page;
    int wrapped;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    /* if the log has not wrapped yet, it contains garbage which can
       cause us to crash, so if the log has not wrapped, print only
       up to the last record. */

    if ((wrapped = log_wrapped(bfp)) < 0) {
	exit(1);
    }

    if (wrapped) {
	 last_page = bfp->pages;
    } else {
	last_page = find_log_end_pg(bfp) + 1;
    }

    for ( pagenum = 0; pagenum < last_page; pagenum++ ) {
        process_page(bfp, pagenum, 0, ADVFS_METADATA_PGSZ, flags);
    }

    exit(0);
}

/***************************************************************************/
static void
process_ends(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bs_meta_page_t pagenum = 0;
    bs_meta_page_t start_pg;
    int64_t start_off;
    bs_meta_page_t end_pg;
    int64_t end_off;
    bs_meta_page_t page;
    int64_t offset;

    if ( argc > optind + 4) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    find_first_n_last_rec(bfp, &start_pg, &start_off, &end_pg, &end_off);

    if ( strcmp(argv[optind], "-s") == 0 ) {
        page = start_pg;
        offset = start_off;
    } else {
        page = end_pg;
        offset = end_off;
    }
    optind++;

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-c") == 0 ) {
            optind++;
            flags |= CFLG;
            if ( argc > optind ) {
                usage();
            }
        }
    }

    /* get the offset fron the start page */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }

        optind++;
        if ( pagenum > bfp->pages || -pagenum > bfp->pages ) {
            fprintf(stderr, "Invalid LOG page number (%ld). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %ld.\n",
              bfp->pages - 1);
            exit(2);
        }

        offset = -1;
        if ( argc > optind ) {
            if ( getnum(argv[optind], &offset) != OK ) {
                fprintf(stderr, "Badly formed offset number: \"%s\"\n",
                  argv[optind]);
                exit(1);
            }
            optind++;

            if ( offset < 0 || offset > ADVFS_METADATA_PGSZ - 1 ) {
                fprintf(stderr, "Invalid record offset (%ld). ", offset);
                fprintf(stderr, "Valid offsets range from 0 to %d.\n",
                  ADVFS_METADATA_PGSZ - 1);
                exit(2);
            }
        }
    }

    pagenum = page + pagenum;
    if ( pagenum < 0 ) {
        pagenum += bfp->pages;
    } else if ( pagenum > bfp->pages ) {
        pagenum -= bfp->pages;
    }

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-c") == 0 ) {
            optind++;
            flags |= CFLG;
        } else {
            usage();
        }
    }

    process_page(bfp, page, offset, offset, flags);
    exit(0);
}

/***************************************************************************/
static void
process_range(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bs_meta_page_t start_pg;
    int64_t start_off;
    bs_meta_page_t end_pg;
    int64_t end_off;
    bs_meta_page_t pagenum;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    find_first_n_last_rec(bfp, &start_pg, &start_off, &end_pg, &end_off);

    process_page(bfp, start_pg, start_off, ADVFS_METADATA_PGSZ, flags);

    if ( end_pg > start_pg ) {
        for ( pagenum = start_pg + 1; pagenum < end_pg; pagenum++ ) {
            process_page(bfp, pagenum, 0, ADVFS_METADATA_PGSZ, flags);
        }
    }

    if ( end_pg < start_pg ) {
        for ( pagenum = start_pg + 1; pagenum < bfp->pages; pagenum++ ) {
            process_page(bfp, pagenum, 0, ADVFS_METADATA_PGSZ, flags);
        }
        for ( pagenum = 0; pagenum < end_pg; pagenum++ ) {
            process_page(bfp, pagenum, 0, ADVFS_METADATA_PGSZ, flags);
        }
    }

    process_page(bfp, end_pg, 0, end_off, flags);

    exit(0);
}

/***************************************************************************/
static void
process_page(bitFileT *bfp, bs_meta_page_t pgnum, int64_t start_off, int64_t end_off, int flags)
{
    read_page(bfp, pgnum);

    bfp->pgNum = pgnum;
    print_header(bfp);

    print_logpage((logPgT *)bfp->pgBuf, pgnum * ADVFS_METADATA_PGSZ_IN_BLKS, 
		  start_off, end_off, flags);
}

/***************************************************************************/
static void
print_logpage( logPgT *pdata, uint64_t lbn, int64_t start_poff, int64_t end_poff, int flags)
{
    int64_t offset = 0, last_offset=-1;
    int found = 0;
    int ftxid, lastftxid = -1;
    ftxDoneLRT *doneLRTp;
    int first = TRUE;

    restore_logpage(pdata);
    if ( start_poff == -1 ) {
        print_loghdr(stdout, &pdata->hdr, lbn, flags);
        printf(SINGLE_LINE);
    }

    while ((offset > last_offset) && (offset < DATA_WORDS_PG)) {

	/* In the HP/UX port records are packed in the log at 32-bit
	   adrs boundaries.  To insure 64-bit aligned access of each
	   record, we malloc a record struct and byte copy the log
	   data into it. */

	doneLRTp = (ftxDoneLRT *) malloc(sizeof(ftxDoneLRT));
	memcpy(doneLRTp, &pdata->data[offset] + REC_HDR_LEN, 
	       sizeof(ftxDoneLRT));

        ftxid = doneLRTp->ftxId;
        if ( start_poff == -1 ||
             (offset >= start_poff && offset <= end_poff) ||
             (flags & CFLG) && (ftxid == lastftxid) ) {

            if ( first ) {
                first = FALSE;
            } else {
                printf(SINGLE_LINE);
            }

            print_logrec((logRecHdrT *)&pdata->data[offset],offset,flags);
            lastftxid = doneLRTp->ftxId;
            found = 1;
        }
        last_offset = offset;
        offset = ((logRecHdrT *)&pdata->data[offset])->nextRec.offset;

	free(doneLRTp);
    }

    if ( start_poff == -1 )
        print_logtrlr(stdout, &pdata->trlr, lbn + 15);

    if ( !found )
        printf("No log entry at offset %ld\n", start_poff);
}

/***************************************************************************/
static void
restore_logpage(logPgT *pgp)
{
    int b;
    logPgBlksT *logPgBlks = (logPgBlksT *)pgp;

    for (b = 1; b < ADVFS_METADATA_PGSZ_IN_BLKS; b++) {
        logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
    }
}

/******************************************************************************/
static void
find_first_n_last_rec(bitFileT *bfp, int64_t *sPg, int64_t *sOff, 
		      int64_t *ePg, int64_t *eOff)
{
    int64_t offset = 0, last_offset=-1;
    int found = 0;
    int ftxid, lastftxid = -1;
    ftxDoneLRT *doneLRTp;
    uint32_t recOffset;
    logRecHdrT record;
    logRecHdrT *recHdr;
    logPgT *pdata;
    size_t size;
    bs_meta_page_t start_pg;
    int64_t start_off;
    bs_meta_page_t end_pg;
    int64_t end_off;
    logRecAddrT crashRedo;

    end_pg = find_log_end_pg(bfp);

    if ( end_pg == -1 ) {
        fprintf(stderr, "E_CANT_FIND_LOG_END\n");
        exit(1);
    } else if ( end_pg == -2 ) {
        fprintf(stderr, "log is empty\n");
        exit(1);
    } else if ( end_pg == -3 ) {
        fprintf(stderr, "get_pg_lsn fail\n");
        exit(1);
    }

    read_page(bfp, end_pg);
    pdata = (logPgT *)bfp->pgBuf;
    restore_logpage(pdata);

    recHdr = &record;
    recOffset = 0;

    /* 
     * Locate the last record.
     * 
     * Log records are packed into a disk page end-to-end at 32 bit
     * addresses, which means that a pointer to a record header lying
     * in the disk page buffer may not be 64-bit aligned.  If it's not
     * and we use it as a structure pointer, we have an unaligned
     * access error which results in a bus error and crash. To avoid
     * this we copy the record into a structure that we know is 64 bit
     * aligned and access that sturcture. 
     */

    do {
        if (recOffset >= DATA_WORDS_PG) {
            printf( "find_first_n_last_rec: Invalid rec offset %d on page %d\n",
		    recOffset, end_pg);
	    exit(2);
        }

	memcpy((void *)recHdr, (void *)&pdata->data[recOffset], 
	       sizeof(logRecHdrT));

        recOffset += recHdr->wordCnt;

        /* FIX. test wordCnt > sizeof header(s) */
        if (recOffset == 0) {
            printf( "advfs_logger: invalid rec offset: %d\n", recOffset );
            exit(2);
        }
    } while ( !RECADDR_EQ(recHdr->nextRec, logEndOfRecords) );

    size = recHdr->wordCnt * sizeof(uint32_t);
    recHdr = (logRecHdrT *) malloc(size);
    memcpy((void *)recHdr, (void *)&pdata->data[recOffset], size);

    doneLRTp = (ftxDoneLRT*)((char *)recHdr + REC_HDR_LEN);

    crashRedo.page = doneLRTp->crashRedo.page;
    crashRedo.offset = doneLRTp->crashRedo.offset;
    crashRedo.lsn = doneLRTp->crashRedo.lsn;

    *ePg = end_pg;
    *eOff = recOffset - recHdr->wordCnt;
    *sPg = crashRedo.page;
    *sOff = crashRedo.offset;

    free(recHdr);
}

/* from ms_public.c */
logRecAddrT logEndOfRecords = { 0xffff, 0xffff, 0 };

/*
 ** LOG_FLUSH_THRESHOLD must be a multiple of 4 **
 */
#define LOG_FLUSH_THRESHOLD  8		/* bs_ims.h */

/* private prototypes */
static int lgr_valid_pg(logPgT *);
static int lsn_lt(lsnT, lsnT);
static int lsn_gt(lsnT, lsnT);
static lsnT lsn_add( lsnT, unsigned);

static int
get_pg_lsn( bitFileT  *bfp,
            bs_meta_page_t   pg,
            lsnT      *pgLsn,
            int       *invalidPg,
            int       *incompletePg,
            lsnT      *prtdLsn,
            uint32_t   *prtdPg );

static void
lgr_restore_pg(logPgT *pgp);

static int
lgr_complete_pg(logPgT *pgp);

static void
lgr_repair_pg(logPgT *pgp);

static int
lgr_valid_blk(logPgT *pgp, int b);

/***************************************************************************/
static int
log_wrapped(bitFileT *bfp)
{
    int logWrapped = 0;
    int pg0Bad = 0;
    int pgMaxBad = 0;
    int ignore;
    bs_meta_page_t finalPg;
    uint32_t ptrdPg = -1;
    lsnT ptrdLsn;
    lsnT pg0LSN; /* LSN of log page zero */
    lsnT pgMaxLSN; /* LSN of last log page */

    finalPg = (bfp->pages - 1);

    /*
     * Get the last page's LSN.  We need it to determine if the log
     * has wrapped (the end page number is less than the beginning page
     * number) since this effects the binary search.
     */

    ptrdLsn = nilLSN;
    if ( get_pg_lsn(bfp, finalPg, &pgMaxLSN, &pgMaxBad, &ignore, &ptrdLsn,
                    &ptrdPg) == -1 ) {
        return -3;
    }

    ptrdLsn = nilLSN;
    if ( get_pg_lsn( bfp, 0, &pg0LSN, &pg0Bad, &ignore, &ptrdLsn,
                     &ptrdPg) == -1 ) {
        return -3;
    }

    if (pg0Bad || pgMaxBad) {
	fprintf(stderr, "log_wrapped: Error reading log file\n");
	return -2;
    }

    logWrapped = !LSN_EQ_NIL(pgMaxLSN) && lsn_lt( pgMaxLSN, pg0LSN);
    return (logWrapped);

} /* end: log_wrapped */

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
 * Returns -3 if a seek or reads fails
 */

int
find_log_end_pg( bitFileT  *bfp   /* log file description */)
{
    int i;
    int logWrapped = 0;
    int pgBad = 0;
    int pg0Bad = 0;
    int pgMaxBad = 0;
    bs_meta_page_t searchPgs;
    int ignore;
    int pgIncomplete;
    bs_meta_page_t finalPg;
    bs_meta_page_t lwrPg = 0;
    bs_meta_page_t uprPg;
    bs_meta_page_t midPg;
    bs_meta_page_t endPg;
    bs_meta_page_t pg;
    uint32_t ptrdPg = -1;

    lsnT ptrdLsn;
    lsnT prevLSN;
    lsnT pgLSN;
    lsnT pg0LSN; /* LSN of log page zero */
    lsnT pgMid1LSN;
    lsnT pgMid2LSN;
    lsnT pgMaxLSN; /* LSN of last log page */

    logPgT *logPgp;
    logRecHdrT record;
    logRecHdrT *recHdr;
    uint32_t recOffset;
    ftxDoneLRT* dlrp;
    logRecAddrT crashRedo;
    size_t size;

    finalPg = (bfp->pages - 1);
    uprPg = finalPg;

    /*
     * Get the last page's LSN.  We need it to determine if the log
     * has wrapped (the end page number is less than the beginning page
     * number) since this effects the binary search.
     */

    ptrdLsn = nilLSN;
    if ( get_pg_lsn(bfp, finalPg, &pgMaxLSN, &pgMaxBad, &ignore, &ptrdLsn,
                    &ptrdPg) == -1 ) {
        return -3;
    }

    ptrdLsn = nilLSN;
    if ( get_pg_lsn( bfp, 0, &pg0LSN, &pg0Bad, &ignore, &ptrdLsn,
                     &ptrdPg) == -1 ) {
        return -3;
    }

    if (pg0Bad) {
        if (!pgMaxBad) {
            if (LSN_EQ_NIL( pgMaxLSN )) {
                /* The log is empty! */
                return -2;
            } else {
                endPg = finalPg;
                goto _finish;
            }
        }
    } else if (LSN_EQ_NIL( pg0LSN )) {
        /* The log is empty! */
        return -2;
    }

    if (pgMaxBad) {
        endPg = finalPg - 1;
        goto _finish;
    }

    logWrapped = !LSN_EQ_NIL(pgMaxLSN) && lsn_lt( pgMaxLSN, pg0LSN);

    if ((!logWrapped) && !LSN_EQ_NIL( pgMaxLSN )) {
        /* The last log pg is the log end pg */
        endPg = finalPg;
        goto _finish;
    }

    /*
     * Now that we've taken care of the easy and corner cases we use
     * the binary search to locate the log end page.
     */

    while (uprPg >= lwrPg) {
        midPg = (lwrPg + uprPg) / 2;

        if (midPg == finalPg) {
            midPg--;
        }

        ptrdLsn = nilLSN;
        assert(midPg >= 0 && midPg <= finalPg);
        if ( get_pg_lsn( bfp, midPg, &pgMid1LSN, &pgBad, &ignore,
                   &ptrdLsn, &ptrdPg) == -1 ) {
            return -3;
        }
        if (pgBad) {
            endPg = midPg - 1;
            goto _finish;
        }

        ptrdLsn = nilLSN;
        assert(midPg >= 0 && midPg <= finalPg);
        if ( get_pg_lsn( bfp, midPg + 1, &pgMid2LSN, &pgBad, &ignore,
                   &ptrdLsn, &ptrdPg) == -1 ) {
            return -3;
        }

        if (pgBad) {
            endPg = midPg;
            goto _finish;
        }

        if ( lsn_gt(pgMid1LSN, pgMid2LSN) ) {
            endPg = midPg;
            goto _finish;
        }

        if (logWrapped) {
            if ( lsn_gt(pg0LSN, pgMid1LSN) ) {
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

    /* log end not found! */
    return -1;

_finish:
    /*
     ** Note that the log must have at least one record in it if we
     ** are executing this "finish" code.
     */

    /*
     ** Calculate the number of pages that we need to verify.
     */
    /* Read the page we think may be the end of the log. */
    if ( read_page(bfp, endPg) != OK ) {
        return -3;
    }

    logPgp = (logPgT*)bfp->pgBuf;
    if ( lgr_valid_pg(logPgp) == FALSE ) {
        return -1;
    }

    lgr_restore_pg(logPgp);

    recHdr = &record;
    recOffset = 0;

    /* 
     * Locate the last record in this candidate last page.
     * 
     * Log records are packed into a disk page end-to-end at 32 bit
     * addresses, which means that a pointer to a record header lying
     * in the disk page buffer may not be 64-bit aligned.  If it's not
     * and we use it as a structure pointer, we have an unaligned
     * access error which results in a bus error and crash. To avoid
     * this we copy the record into a structure that we know is 64 bit
     * aligned and access that sturcture. 
     */

    do {
        if (recOffset >= DATA_WORDS_PG) {
            fprintf(stderr, "invalid rec offset: %d in log\n", recOffset);
            return -1;
        }

	memcpy((void *)recHdr, (void *)&logPgp->data[recOffset], 
	       sizeof(logRecHdrT));

        recOffset += recHdr->wordCnt;

        if ( recHdr->wordCnt == 0 ) {
            printf("invalid rec offset: %d in log\n", recOffset);
            return -1;
        }
    } while (!RECADDR_EQ( recHdr->nextRec, logEndOfRecords ));

    /* Peel out the first active log (starting) log page */

    size = recHdr->wordCnt * sizeof(uint32_t);
    recHdr = (logRecHdrT *) malloc(size);
    memcpy((void *)recHdr, (void *)&logPgp->data[recOffset], size);

    dlrp = (ftxDoneLRT*)((char *)recHdr + REC_HDR_LEN);

    crashRedo = dlrp->crashRedo;

    pg = crashRedo.page;
    if ( pg > endPg ) {
        searchPgs = bfp->pages - pg + endPg + 1;
    } else {
        searchPgs = endPg - pg + 1;
    }
    prevLSN = nilLSN;

    /*
     ** Scan forward thru the range. We have a candidate end page
     ** but we need to be sure there are no missing pages in this
     ** range since the binary search may not have caught them.
     */

    for ( i = 1; i <= searchPgs; i++ ) {

        ptrdLsn = nilLSN;
        if ( get_pg_lsn(bfp, pg, &pgLSN, &pgBad, &pgIncomplete, &ptrdLsn,
                    &ptrdPg) == -1 ) {
	    free(recHdr);
            return -3;
        }

        if ( pgBad ) {
            /*
             * The current page was not written in it's entirety.
             * The previous page in the log must be the log end.
             */
            endPg = (pg + bfp->pages - 1) % bfp->pages;
            printf("bad page %ld; truncating log\n", endPg);
            break;
        }

        if ( LSN_EQ_NIL(prevLSN) ) {
            /*
             * The following test is for a very special case.
             * if our prevLSN is NIL then this means that we are the first
             * page in the active range.
             */
            if ( (lsn_lt(pgLSN, crashRedo.lsn) &&
                  lsn_lt(lsn_add(pgLSN, LOG_RECS_PG), crashRedo.lsn)) ||
                 pgIncomplete )
            {
                endPg = (pg + bfp->pages - 1) % bfp->pages;
                break;
            }
        } else if ( lsn_lt(pgLSN, prevLSN) || pgIncomplete ) {
            /*
             * Either we don't have a valid end page or the current
             * page has a higher LSN than the prev page.  Or, the page
             * was not overwritten completely.
             * Make the current page the current end page.
             */

            endPg = (pg + bfp->pages - 1) % bfp->pages;
            break;
        }

        prevLSN = pgLSN;
        endPg = pg;
        pg = (pg + bfp->pages + 1) % bfp->pages;
    }

    free(recHdr);
    return endPg;
}

/*
 * get_pg_lsn
 *
 * Returns the first record LSN of the specified page.  If the page is
 * invalid then no LSN is returned and the flag 'badPg' is set to TRUE.
 */

static int
get_pg_lsn(
    bitFileT  *bfp,
    bs_meta_page_t pg,         /* in - log page's number */
    lsnT *pgLsn,        /* out - log page's first record's LSN */
    int *invalidPg,     /* out - flag indicates if page is invalid */
    int *incompletePg,  /* out - flag indicates if page is incomplete */
    lsnT *prtdLsn,      /* in/out - thread of prev lsn across pg */
    uint32_t *prtdPg     /* in/out - thread of prev pg  across pg */
    )
{
    logPgT pgP;
    logRecHdrT *recHdr;
    int results;
    logPgT *logPgp;

    if ( read_page(bfp, pg) != OK ) {
        return 1;
    }
    logPgp = (logPgT*)bfp->pgBuf;

    *invalidPg = FALSE; /* assume all is okay */
    *incompletePg = FALSE; /* assume all is okay */

    *pgLsn = logPgp->hdr.thisPageLSN;
    *invalidPg = !lgr_valid_pg( logPgp );
    *incompletePg = !lgr_complete_pg( logPgp );

    /*
     * check for consistent prevLsn threading
     */
    if ((!LSN_EQ_NIL(*prtdLsn)) && (*prtdPg == pg)) {
        lgr_restore_pg(logPgp);
        recHdr = (logRecHdrT *) &logPgp->data[logPgp->hdr.curLastRec];
        if (!LSN_EQL(recHdr->lsn,*prtdLsn)) {
            printf("ADVFS: caught deceipher bug, trunc log at pg %ld\n",pg);
            *incompletePg = TRUE;
        }
    }

    recHdr = (logRecHdrT *) &logPgp->data[0];
    if ( lsn_gt(recHdr->firstSeg.lsn, recHdr->prevRec.lsn) ) {
        *prtdLsn = recHdr->firstSeg.lsn;
        *prtdPg  = recHdr->firstSeg.page;
    } else {
        *prtdLsn = recHdr->prevRec.lsn;
        *prtdPg  = recHdr->prevRec.page;
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
    logPgT *pgp          /* in - ptr to a log page */
    )
{
    int b;

    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;

    if (pgp->hdr.pgSafe &&
        ((pgp->hdr.pgType != BFM_FTXLOG) ||
         (pgp->trlr.pgType != BFM_FTXLOG) ||
         !BS_UID_EQL(pgp->hdr.fsCookie, pgp->trlr.fsCookie))) {
        /*
         ** This doesn't even look like a log page!!!.
         */
        return FALSE;
    }

    if (pgp->hdr.pgSafe) {
        /*
         * The page validity check makes sense only if the page is
         * still in the 'safe page' format.
         */

        for (b = 1; b < ADVFS_METADATA_PGSZ_IN_BLKS; b++) {
            if (pgp->hdr.thisPageLSN.num !=
                (logPgBlks->blks[b].lsn.num & 0xfffffffe)) {
                return FALSE;
            }
        }
    }
    return TRUE;
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
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;

    if (pgp->hdr.pgSafe) {
        /*
         * This check makes sense only if the page is
         * still in the 'safe page' format.
         */

        for (b = 1; (b < ADVFS_METADATA_PGSZ_IN_BLKS) && !badChkBits; b++) {
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
    logPgT *pgp         /* in - ptr to a log page */
    )
{
    int b, badChkBits = FALSE;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;
    logRecHdrT *recHdr;

    if (pgp->hdr.pgSafe) {
        /*
         * The page restoration makes sense only if the page is
         * still in the 'safe page' format.
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

            lgr_repair_pg( pgp );
        } else {
            /*
             * Page is okay.  Restore it to 'non-safe' (useable) format.
             */

            for (b = 1; b < ADVFS_METADATA_PGSZ_IN_BLKS; b++) {
                logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
            }
        }

        pgp->hdr.pgSafe = FALSE;
    }
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
    logPgT *pgp         /* in - ptr to a log page */
    )
{
    uint64_t validEndRec, startBlk, endBlk;
    logRecHdrT *recHdr;
    int b, badChkBits = FALSE;
    logPgBlksT *logPgBlks = (logPgBlksT *) pgp;

    if (!pgp->hdr.pgSafe) {
        printf( "lgr_repair_pg: log page not in 'safe' format" );
        exit(1);
    }

    if (pgp->hdr.prevLastRec == -1) {
        /*
         * The first rewrite of the page failed and the first
         * block in the page was not rewritten.  Therefore, the
         * curLastRec offset is correct.
         */
        validEndRec = pgp->hdr.curLastRec;

    } else if (pgp->hdr.prevLastRec == pgp->hdr.curLastRec) {
        printf( "prev == last", pgp->hdr.curLastRec );
        exit(1);
    } else {
        /*
         * Using the last rec offsets figure out the range of blocks
         * that we need to validate.
         */

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.prevLastRec];

        startBlk = (pgp->hdr.prevLastRec + recHdr->wordCnt + PG_HDR_WORDS)
                                       /
                   (ADVFS_FOB_SZ / sizeof( uint32_t ));

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.curLastRec];

        endBlk = (pgp->hdr.curLastRec + recHdr->wordCnt + PG_HDR_WORDS)
                                       /
                   (ADVFS_FOB_SZ / sizeof( uint32_t ));

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

            badChkBits = !lgr_valid_blk( pgp, ADVFS_METADATA_PGSZ_IN_BLKS - 1 );

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

    for (b = 1; b < ADVFS_METADATA_PGSZ_IN_BLKS; b++) {
        logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
    }

    if (validEndRec == pgp->hdr.prevLastRec) {
        /* Set the prev last rec to be the last rec */

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.prevLastRec];
        recHdr->nextRec = logEndOfRecords;
    }
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
 * lsn_lt
 *
 * For use by find_log_end_pg() only.  This version uses the
 * 'magic lsn range' to determine whether signed or unsigned
 * lsn compares are appropriate.
 */

static int
lsn_lt(
    lsnT lsn1,
    lsnT lsn2
    )
{
    unsigned maxLSNs = 1 << 29;

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
 * lsn_gt
 *
 * For use by find_log_end_pg() only.  This version uses the
 * 'magic lsn range' to determine whether signed or unsigned
 * lsn compares are appropriate.
 */

static int
lsn_gt(
    lsnT lsn1,
    lsnT lsn2
    )
{
    unsigned maxLSNs = 1 << 29;

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

static lsnT
lsn_add(
    lsnT lsn,
    unsigned amt
    )
{
    unsigned adjAmt = amt * LSN_INCR_AMT;

    if ( (LSN_MAX - lsn.num) < adjAmt ) {
        /*
         * We are wrapping the lsn so we need to skip (move past) lsn 'zero'.
         */
        adjAmt += LSN_INCR_AMT;
    }

    lsn.num += adjAmt;

    return( lsn );
}
