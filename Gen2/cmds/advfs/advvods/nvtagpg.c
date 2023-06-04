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

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <assert.h>
#include <advfs/bs_public.h>
#include <advfs/ms_public.h>
#include <advfs/ms_logger.h>
#include <advfs/ftx_public.h>
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>

#include "vods.h"

/* public prototypes */

void get_fileset(bitFileT *bfp, bitFileT *setBfp, char 
		 *argv[], int argc, int *optind, int flags);

/* private prototypes */

static void save_tag(bitFileT *bfp, char *argv[], int argc, 
		     int optind, int flags);
static void process_block(bitFileT *bfp, char *argv[], int argc, 
			  int optind, int flags);
static void process_all(bitFileT *bfp, char *argv[], int argc, 
			int optind, int flags);
static void process_tag(bitFileT *bfp, char *argv[], int argc, 
			int optind, int flags);
static void process_file(bitFileT *bfp, char *argv[], int argc, 
			 int optind, int flags);

static void test_page_num(bitFileT *bfp, bs_meta_page_t pagenum, int flg);
static void test_tag_num(bitFileT *bfp, uint64_t tagNum, int flg);

static int print_setname(bitFileT *bfp, bfMCIdT mcid, uint64_t tag);
static void print_block(bitFileT *bfp, int fd, uint64_t lbn, int vflg);
static void print_page(bitFileT *bfp, bs_meta_page_t pgnum, int flg);
static void print_tag(bitFileT *bfp, uint64_t tagNum, int flags);
static void print_tagpage(bitFileT *bfp, bsTDirPgT *pdata, uint64_t lbn, int flags);
static void print_taghdr(bitFileT *bfp, bsTDirPgHdrT *pdata, int flags);
static void print_tag_entry(bitFileT *bfp, bsTDirPgT *pdata, 
			    int slot, int flg);



/******************************************************************************/
void
tag_main(int argc,
	 char **argv)
{
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    bitFileT rootBf;
    bitFileT *rootBfp = &rootBf;
    bitFileT  setBf;
    bitFileT  *setBfp = &setBf;
    int flags = 0;
    uint64_t blknum = LBN_BLK_TERM;
    bs_meta_page_t pagenum = 0;
    int64_t longnum;
    int ret;
    extern int optind;

    init_bitfile(bfp);
    BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, BFM_BFSDIR);

    ret = resolve_src(argc, argv, &optind, "rvf:", bfp, &flags);

    /* "-b" must work even on a badly corrupted FS, so we service
       block requests without checking error return status */

    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pmcell.volume, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags & VFLG);
        /* NOT REACHED */
    }

    /* Now check ret from resolve_src. At this point if the volume
       serious AdvFS format errors, bail. */

    if ( ret != OK ) {
        exit(ret);
    }

    /* root tag file and the fileset tag files are not associated with
       an individual disk.  There's no use in the user specifying a
       volume spec for the tag/rtag command, except for use with the
       "-b" option (see above). */

    if (bfp->type & VOL) {
	fprintf(stderr, "The 'rtag/tag' command does not accept a volume.\n");
	exit(BAD_PARAM);
    }

    /* fetch the root tag file's extent info */

    if ( find_TAG(bfp) != OK ) {
        exit(3);
    }
    *rootBfp = *bfp;

    /* if the command is "rtag", we're dealing with the root tag
       directory.  If the command is "tag", we're dealing with a
       fileset.  For a fileset, fetch the fileset tag file. */

    if (!strcmp(argv[0], "tag") && !(bfp->type & FLE)) {
	/* fetch the needed fileset tag file (also handles snapshots
	   via the -S or -t options) */

	get_fileset(bfp, setBfp, argv, argc, &optind, flags & VFLG);
	*bfp = *setBfp;
    }

    if (strcmp(argv[0], "rtag") == 0  &&
	(strcmp(argv[optind], "-S") == 0  ||
	 strcmp(argv[optind], "-t") == 0)) {
	
        fprintf(stderr, "'rtag' command cannot be used with snapshots.");
	usage();
    }

    /* if there are no more args then print the 1st page of data */

    if ( argc == optind ) {
        /* if only 1 page long (few filesets) and type == DMN or */
        /* type == VOL and bmt says vol cnt = 1 then print fileset names */
        if ( bfp->pages == 1 ) {
            if ( bfp->type & DMN /*|| bfp->type == VOL*/ ) {
                flags |= PSET;
            }
        }
        print_page(bfp, 0, flags);
        exit(0);
    }


    /* dump the tagdir to a file */
    if ( strcmp(argv[optind], "-d") == 0 ) {
        save_tag(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* print the whole tagfile */
    if ( strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* print a specific tagfile page */
    if ( strcmp(argv[optind], "-p") == 0 ) {
        optind++;
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }
        optind++;

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            usage();
        }

        test_page_num(bfp, pagenum, flags); /* exits if bad page */
	print_page(bfp, pagenum, flags);
    }

    /* print a file's tag */
    if ( strcmp(argv[optind], "-f") == 0 ) {
	optind++;
        process_file(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* if we got here and there are no more args, this one is a file
       name */
    if ( argc > optind ) {
	if(strcmp(argv[0], "rtag") == 0) {
	    fprintf(stderr, "'rtag' command can not look up a file's tag. Use 'tag'\n");
	    exit(1);
	}
        process_file(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    exit(0);
}

/****************************************************************************/
static void
test_page_num(bitFileT *bfp, bs_meta_page_t pagenum, int flg)
{
    bs_meta_page_t initedPages;

    if ( (int64_t)pagenum < 0 || pagenum >= bfp->pages ) {
        fprintf(stderr, "Invalid tag page number (%ld). ", pagenum);
        fprintf(stderr, "Valid page numbers are 0 through %ld.\n",
          bfp->pages - 1);
        exit(1);
    }

    get_max_tag_pages(bfp, &initedPages);

    if ( pagenum >= initedPages && !(flg & VFLG) ) {
        fprintf(stderr,
          "Page %ld is beyond the last initialized tag page (%ld).\n",
          pagenum, initedPages-1);
        exit(1);
    }
}


/*****************************************************************************/
static void
test_tag_num(bitFileT *bfp, uint64_t tagNum, int flg)
{
    bs_meta_page_t initPages;
    bs_meta_page_t pagenum;
    bfTagT tag;

    tag.tag_num = tagNum;
    tag.tag_seq = 0;
    pagenum = TAGTOPG(&tag);

    if ( pagenum >= bfp->pages ) {
        fprintf(stderr, "Invalid tag number (%ld) would be on tag page %ld.\n",
          tagNum, pagenum);
        fprintf(stderr, "Valid page numbers are 0 - %ld (tag 0 - %ld).\n",
          bfp->pages - 1, bfp->pages * BS_TD_TAGS_PG - 1);
        exit(1);
    }

    get_max_tag_pages(bfp, &initPages);

    if ( pagenum >= initPages && !(flg & VFLG) ) {
        fprintf(stderr,
          "Tag %ld on page %ld is beyond the last initialized tag page (%ld).\n",
          tagNum, pagenum, initPages);
        exit(1);  }
}


/******************************************************************************/
static void
save_tag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile = NULL;

    /* FIX. test for DMN or VOL only */
    optind++;
    if ( flags ) {
        fprintf(stderr, "No -v flag with -d flag\n");
        usage();
    }

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

/******************************************************************************/
static void
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    uint64_t blknum;
    int64_t longnum;

    assert(bfp->fd);
    if ( bfp->type & FLE ) {
        fprintf(stderr, "Cannot use -b flag on a file\n");
        usage();
    }

    if ( strlen(argv[optind]) == 2 ) {		/* -b # */
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
    } else {					/* -b# */
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
        fprintf(stderr, "Tag pages are on %d block boundaries.\n",
          ADVFS_METADATA_PGSZ_IN_BLKS);
        blknum = blknum / ADVFS_METADATA_PGSZ_IN_BLKS * ADVFS_METADATA_PGSZ_IN_BLKS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%ld).\n",
          ADVFS_METADATA_PGSZ_IN_BLKS, blknum);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    print_block(bfp, bfp->fd, blknum, flags);

    exit(0);
}

/******************************************************************************/
static void
print_block(bitFileT *bfp, int fd, uint64_t lbn, int vflg)
{
    bsTDirPgT xpdata;
    bsTDirPgT *pdata = &xpdata;
    int result;

    if ( lseek(fd, ((uint64_t)lbn) * DEV_BSIZE, L_SET) == -1 ) {
        fprintf(stderr, "bad seek\n");
        exit(3);
    }

    result = read(fd, &xpdata, sizeof(bsTDirPgT));
    if ( result == -1 ) {
        fprintf(stderr, "bad read\n");
        exit(3);
    } else if ( result != sizeof(bsTDirPgT) ) {
        fprintf(stderr, "truncated read\n");
        exit(3);
    }

    bfp->pgVol = bfp->pmcell.volume;
    bfp->pgLbn = lbn;
    print_tagpage(bfp, pdata, lbn, vflg);
}

/***************************************************************************/
static void
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bs_meta_page_t pagenum;
    bs_meta_page_t pages;
    int ret;
    bsTDirPgT *pdata;

    optind++;

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }
    /* the number of valid pages may be less than the number
     * of pages in the extent map. tag directories are allocated
     * in units of 8 pages, but not all of them are necessarily
     * valid.
     */
    if (bfp->pages == 0) 
        exit(0);

    get_max_tag_pages(bfp, &pages);

    for ( pagenum = 0; pagenum < pages; pagenum++ ) {
        print_page(bfp, pagenum, flags);
    }

    exit(0);
}

/******************************************************************************/
#ifdef FUTURE
process_summary(bitFileT *bfp)
{
    bs_meta_page_t pagenum;
    bs_meta_page_t pages;
    int totalAlloc = 0;

    if (bfp->pages == 0) 
        exit(0);

    /* the number of valid pages may be less than the number
     * of pages in the extent map. tag directories are allocated
     * in units of 8 pages, but not all of them are necessarily
     * valid.
     */
    get_max_tag_pages(bfp, &pages);

    for ( pagenum = 0; pagenum < pages; pagenum++ ) {
        bsTDirPgT *bsTDirPgp;
        bsTDirPgHdrT *bsTDirPgHdrp;
        int ret;

        ret = read_page(bfp, pagenum);
        if ( ret != OK ) {
            fprintf(stderr, "Failed to read page %ld of tag file.\n", pagenum);
            continue;
        }

        bsTDirPgp = (bsTDirPgT*)bfp->pgBuf;
        bsTDirPgHdrp = &bsTDirPgp->tpPgHdr;
        totalAlloc += bsTDirPgHdrp->numAllocTMaps;
    }
    printf("Pages %ld\n", pages);
    printf("Total tags %ld\n", pages * BS_TD_TAGS_PG);
    printf("Total allocated tags %ld\n", totalAlloc);

    exit(0);
}

/******************************************************************************/
process_free(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bs_meta_page_t pagenum;
    bs_meta_page_t pages;
    int ret;
    bsTDirPgT *bsTDirPgp;
    bsTDirPgHdrT *bsTDirPgHdrp;
    bs_meta_page_t nextPage;

    optind++;

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    if (bfp->pages == 0) 
        exit(0);

    /* the number of valid pages may be less than the number
     * of pages in the extent map. tag directories are allocated
     * in units of 8 pages, but not all of them are necessarily
     * valid.
     */
    get_max_tag_pages(bfp, &pages);

    ret = read_page(bfp, 0);
    if ( ret != OK ) {
        fprintf(stderr, "Failed to read page 0 of tag file.\n");
        exit(1);
    }

    bsTDirPgp = (bsTDirPgT*)bfp->pgBuf;
    bsTDirPgHdrp = &bsTDirPgp->tpPgHdr;
    nextPage = bsTDirPgHdrp->nextFreePage;
    nextPage = bsTDirPgp->tMapA[0].tmFreeListHead;

    print_taghdr(bfp, bsTDirPgHdrp, flags);
    printf("tMapA[0] ");
    printf("   ");
    printf("tag %-8ld ", 0);
    printf("seqNo %-5d  ", bsTDirPgp->tMapA[0].tmSeqNo);
    printf("tmFreeListHead %lu ", bsTDirPgp->tMapA[0].tmFreeListHead);
    printf("tmUninitPg %lu\n", bsTDirPgp->tMapA[0].tmUninitPg);
    printf("\n");

    while ( nextPage != 0 ) {
        if ( nextPage > pages ) {
            fprintf(stderr, "Bad free chain.");
            fprintf(stderr, "Page %ld is beyond the last page (%ld)\n",
              nextPage, pages - 1);
            exit(2);
        }

        ret = read_page(bfp, nextPage - 1);
        if ( ret != OK ) {
            fprintf(stderr, "Failed to read page %ld of tag file.\n",
              nextPage - 1);
            exit(1);
        }

        bsTDirPgp = (bsTDirPgT*)bfp->pgBuf;
        bsTDirPgHdrp = &bsTDirPgp->tpPgHdr;
        nextPage = bsTDirPgHdrp->nextFreePage;

        print_taghdr(bfp, bsTDirPgHdrp, flags);
    }

    exit(0);
}
#endif  /* FUTURE */

/******************************************************************************/
static void
print_page(bitFileT *bfp, bs_meta_page_t pgnum, int flg)
{
    bsTDirPgT *pdata;
    int ret;

    ret = read_page(bfp, pgnum);
    if ( ret != OK ) {
        fprintf(stderr, "Failed to read page %ld of tag file.\n", pgnum);
        exit(1);
    }

    pdata = (bsTDirPgT*)bfp->pgBuf;

    print_tagpage(bfp, pdata, bfp->pgLbn, flg);

    return;
}

/******************************************************************************/
static void
process_file(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    bitFileT tagBf, *tagBfp = &tagBf;
    int ret;

    tagBf = *bfp;
    /* tagBfp & bfp share xtnts */
    tagBfp->pgBuf = NULL;

    if ( argc == optind ) {
        fprintf(stderr, "must have file name\n");
        usage();
    }

    ret = get_file(tagBfp, argv, argc, &optind, bfp);
    if ( ret != OK ) {
        if ( ret == BAD_SYNTAX ) {
            usage();
        } else {
            exit(ret);
        }
    }

    if ( argc > optind ) {
        fprintf(stderr, "too many args\n");
        usage();
    }

    print_tag(tagBfp, bfp->fileTag.tag_num, flags);

    exit(0);
}

/******************************************************************************/
static void
process_tag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    uint64_t tagNum;
    int64_t longnum;
    bfTagT tag;

    optind++;
    if ( argc == optind ) {
        fprintf(stderr, "must have tag\n");
        usage();
    }

    if ( argc > optind + 1 ) {
        fprintf(stderr, "too many args\n");
        usage();
    }

    if ( getnum(argv[optind], &longnum) != OK ) {
        fprintf(stderr, "Badly formed tag number: \"%s\"\n", argv[optind]);
        exit(1);
    }

    tagNum = (uint64_t)longnum;
    test_tag_num(bfp, tagNum, flags);

    print_tag(bfp, tagNum, flags);
    /* NEVER REACHED */
}

/******************************************************************************/
static void
print_tag(bitFileT *bfp, uint64_t tagNum, int flags)
{
    bfTagT tag;
    bs_meta_page_t pagenum;
    bsTDirPgT *pdata;
    uint64_t lbn = bfp->pgLbn;
    int flg = flags;
    int slot;
    bs_meta_page_t pages;

    tag.tag_num = tagNum;
    tag.tag_seq = 0;
    pagenum = TAGTOPG(&tag);

    get_max_tag_pages(bfp, &pages);
    if ( pagenum >= pages && !(flg & VFLG) ) {
        fprintf(stderr, "Page %ld is beyond the last good tag page.\n", pagenum);
        return;
    }

    if ( read_page(bfp, pagenum) != OK )
        exit(3);

    pdata = (bsTDirPgT*)bfp->pgBuf;

    print_taghdr(bfp, &pdata->tpPgHdr, flags);

    slot = TAGTOSLOT(&tag);

    print_tag_entry(bfp, pdata, slot, 1);

    exit(0);
}

/***************************************************************************/
static void
print_tagpage(bitFileT *bfp, bsTDirPgT *pdata, uint64_t lbn, int flags)
{
    bfMCIdT bfMCId;
    int slot;

    print_taghdr(bfp, &pdata->tpPgHdr, flags);

    /* FIX. if currPage != bfp->pgNum then don't look at tags unless verbose */
    for (slot = 0; slot < BS_TD_TAGS_PG; slot++) {
        print_tag_entry(bfp, pdata, slot, flags);
    }
}

/***************************************************************************/
static void
print_taghdr(bitFileT *bfp, bsTDirPgHdrT *pdata, int flags)
{
    char time_buf[40];

    print_header(bfp);

    if ( flags & VFLG ) {
	printf("magicNumber 0x%x  ", pdata->magicNumber);

	ctime_r(&pdata->fsCookie, time_buf, 40);
	time_buf[strlen(time_buf) - 1] = '\0';

	printf("fsCookie 0x%lx.%lx (%s)\n",
		pdata->fsCookie.id_sec, pdata->fsCookie.id_usec, time_buf);

        printf("currPage %ld  ", pdata->currPage);
        /* Reading at a given block (-b) leaves pgNum = -1 */
        if ( bfp->pgNum != BMT_PG_TERM && bfp->pgNum != pdata->currPage ) {
            printf("ERROR. currPage != file page.\n");
        }
    } else {
        if ( bfp->pgNum != BMT_PG_TERM && bfp->pgNum != pdata->currPage ) {
            printf("ERROR. currPage != file page. currPage = %ld\n",
              pdata->currPage);
        }
    }

    printf("numAllocTMaps %d  ", pdata->numAllocTMaps);
    printf("nextFreePage %ld  ", pdata->nextFreePage);
    printf("nextFreeMap %d  ", pdata->nextFreeMap);
    printf("\n\n");
    
}

/******************************************************************************/
/*
** Tag entries are a union of 3 uses. The first tag on the first page
** has a head of the free list and the first un initilaized tag page.
** The remainder of the tags are either on the free list or in use or dead.
*/

static void
print_tag_flags(bfTagFlagsT tagFlags)
{
    bfDataSafetyT dataSafety = tagFlags & BS_DATA_SAFETY_MASK;

    printf("\n\t\ttmflags: 0x%x:", tagFlags);

    if (tagFlags & BS_TD_IN_USE) {
        printf(" IN_USE");
    }
    if (tagFlags & BS_TD_OUT_OF_SYNC_SNAP) {
        printf(" OUT_OF_SYNC_SNAP");
    }
    if (tagFlags & BS_TD_VIRGIN_SNAP) {
        printf(" VIRGIN_SNAP");
    }
    if (tagFlags & BS_TD_FRAGMENTED) {
        printf(" FRAGMENTED");
    }

    if (dataSafety) {

	printf("  Data Safety:");

	switch ( dataSafety ) {
	case BFD_USERDATA:
	    printf(" BFD_USERDATA"); 
	    break; 
	case BFD_METADATA:
	    printf(" BFD_METADATA");
	    break;
	case BFD_LOG:
	    printf(" BFD_LOG");
	    break; 
	default: 
	    printf(" *** unknown dataSafety (%d) ***", dataSafety); 
	    break;
	}
    } else {
	printf("\n\t\tNo Data Safety Bits");
    }

    printf("\n");

} /* print_tag_flags */


static void
print_tag_entry(bitFileT *bfp, bsTDirPgT *pdata, int slot, int flg)
{
    bfTagT tag;
    bs_meta_page_t pgNum;
    bfMCIdT mcid;

    if ( bfp->pgNum != BMT_PG_TERM ) {
        /* We are reading a page of a tag file. */
        /* We trust the file page number more than the data at the page. */
        pgNum = bfp->pgNum;
        tag.tag_num = MKTAGNUM(pgNum, slot);
    } else {
        /* We are reading a block from disk (-b argument). */
        /* We have only the data on the page (currPage) to tell the page num. */
        pgNum = pdata->tpPgHdr.currPage;
        tag.tag_num = MKTAGNUM(pgNum, slot);
        if ( BS_BFTAG_RSVD(tag) ) {
            /* Use this as a flag that we can't find a reasonable tag number. */
            tag.tag_num = (uint64_t)-1;
        }
    }

    if ( slot == 0 && pgNum == 0 ) {
        /* This is the first tag on the first page of the tag file. */

        printf("tMapA[0] ");

        printf("tmFreeListHead %lu ", pdata->tMapA[0].tmFreeListHead);
        printf("tmUninitPg %lu\n", pdata->tMapA[0].tmUninitPg);

    } else if (pdata->tMapA[slot].tmFlags & BS_TD_IN_USE) {
        mcid = pdata->tMapA[slot].tmBfMCId;

        printf("tMapA[%d] ", slot);
        if ( slot < 10 ) {
            printf("   ");
        } else if ( slot < 100 ) {
            printf("  ");
        } else if ( slot < 1000 ) {
            printf(" ");
        }

        if ( tag.tag_num != (uint64_t)-1 ) {
            if ( flg & PSET ) {
                printf("tag %-2d ", tag.tag_num);
            } else {
                printf("tag %-8ld ", tag.tag_num);
            }
        } else {
            printf("tag ??? ");
        }

        printf("seqNo %-5d  ", pdata->tMapA[slot].tmSeqNo);
        print_tag_flags(pdata->tMapA[slot].tmFlags);
        printf("\t\tprimary mcell (vol,page,cell)  %d,%ld,%d   ",
                mcid.volume, mcid.page, mcid.cell);

        if ( flg & PSET ) {
            print_setname(bfp, mcid, tag.tag_num);
        } else {
            printf("\n");
        }

    } else if ( flg & VFLG ) {
        /* This tag is on the free list. */

        printf("tMapA[%d] ", slot);
        if ( slot < 10 ) {
            printf("   ");
        } else if ( slot < 100 ) {
            printf("  ");
        } else if ( slot < 1000 ) {
            printf(" ");
        }

        if ( flg & PSET ) {
            printf("tag %-2d ", tag.tag_num);
        } else {
            printf("tag %-8ld ", tag.tag_num);
        }

        printf("seqNo %-5d  ", pdata->tMapA[slot].tmSeqNo);
        printf("tmNextMap %d\n", pdata->tMapA[slot].tmNextMap);
    }
}

/******************************************************************************/
/*    bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};	ms_privates.c */
static int
print_setname(bitFileT *bfp, bfMCIdT mcid, uint64_t tag)
{
    bitFileT *bmtbfp;
    bsMCT *mcellp;
    bsMRT *recordp;
    char *data;
    int ret;

    assert(bfp);
    assert(bfp->dmn);
    if ( bfp->dmn->vols[mcid.volume] == NULL ) {
        fflush(stdout);
        fprintf(stderr, "\n  Tag %ld primary mcell volume (%d) not in domain.\n",
          tag, mcid.volume);
        return BAD_DISK_VALUE;
    }

    bmtbfp = bfp->dmn->vols[mcid.volume]->bmt;
    assert(bmtbfp);
    ret = read_page(bmtbfp, (bs_meta_page_t)FETCH_MC_PAGE(mcid));
    if ( ret != OK ) {
        fflush(stdout);
        fprintf(stderr, "\n  Read BMT page %ld on volume %d failed.\n", mcid.page, mcid.volume);
        return ret;
    }

    mcellp = &((bsMPgT*)bmtbfp->pgBuf)->bsMCA[mcid.cell];
    if ((int64_t) mcellp->mcBfSetTag.tag_num != -BFM_BFSDIR || mcellp->mcTag.tag_num != tag ) {
        fflush(stdout);
        fprintf(stderr,
      "\n  Wrong primary mcell (%d %ld %d). Set,tag is %ld,%ld, should be -2,%ld\n",
          mcid.volume, mcid.page, mcid.cell, mcellp->mcBfSetTag.tag_num,  mcellp->mcTag.tag_num, tag);
        return BAD_DISK_VALUE;
    }

    /* FIX. use find_rec */
    recordp = (bsMRT *)mcellp->bsMR0;
    if ( recordp->type != BSR_ATTR ) {
        fflush(stdout);
        fprintf(stderr,
          "\n  Primary mcell first record type is %d, should be BSR_ATTR\n",
          recordp->type);
        return BAD_DISK_VALUE;
    }

    data = ((char *)recordp) + sizeof(bsMRT);
    if ( ((bsBfAttrT *)data)->state != BSRA_VALID ) {
        fflush(stdout);
        fprintf(stderr, "\n  set not BSRA_VALID\n");
        return BAD_DISK_VALUE;
    }

    /* FIX. test vdI */
    mcid = mcellp->mcNextMCId;
    ret = read_page(bmtbfp, (bs_meta_page_t)FETCH_MC_PAGE(mcid));
    if ( ret != OK ) {
        fflush(stdout);
        fprintf(stderr, "\n  Read BMT page %ld on volume %d failed.\n", mcid.page, mcid.volume);
        return ret;
    }

    mcellp = &((bsMPgT*)bmtbfp->pgBuf)->bsMCA[mcid.cell];
    if ( (int64_t)mcellp->mcBfSetTag.tag_num != -BFM_BFSDIR || mcellp->mcTag.tag_num != tag ) {
        fflush(stdout);
        fprintf(stderr,
    "\n  Wrong secondary mcell (%d %ld %d). Set,tag is %ld,%ld, should be -2,%ld\n",
          mcid.volume, mcid.page, mcid.cell, mcellp->mcBfSetTag.tag_num,  mcellp->mcTag.tag_num, tag);
        return BAD_DISK_VALUE;
    }

    /* FIX. use find_rec */
    recordp = (bsMRT *)mcellp->bsMR0;
    if ( recordp->type != BSR_BFS_ATTR ) {
        fflush(stdout);
        fprintf(stderr,
        "\n  Secondary mcell first record type is %d, should be BSR_BFS_ATTR\n",
          recordp->type);
        return BAD_DISK_VALUE;
    }
    data = ((char *)recordp) + sizeof(bsMRT);
    printf("%s\n", ((bsBfSetAttrT *)data)->setName);
}
