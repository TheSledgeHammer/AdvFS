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
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <advfs/bs_public.h>
#include <advfs/ms_public.h>
#include <advfs/ms_logger.h>
#include <advfs/ftx_public.h>
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>
#include "vods.h"

static void save_sbm(bitFileT *bfp, char *argv[], int argc, int optind);
static void print_page(bsStgBmT *bsStgBmp, bs_meta_page_t pn, int len);
static void process_summary(bitFileT *bfp, int flag);
static void process_all(bitFileT *bfp, char *argv[], int argc, int optind);
static int process_page(bitFileT *bfp, bs_meta_page_t page);
static void process_index(bitFileT *bfp, bs_meta_page_t pagenum, int64_t index);
static void process_find_index(bitFileT *bfp, char *argv[], int argc, 
			       int optind);
static void process_block(bitFileT *bfp, char *argv[], int argc, int optind);
static void process_find_block(bitFileT *bfp, char *argv[], int argc, 
			       int optind);

/* Make new defines. SBM_LONGS_PG & SBM_BITS_LONG are too misleading. */

/* SBM_BITS_INT is the number of bits in a mapInt entry (32) */
#define SBM_BITS_INT SBM_BITS_LONG
/* SBM_INTS_PG is the number of SBM mapInt entries (uint32_t) per SBM page. */
#define SBM_INTS_PG SBM_LONGS_PG

/* blks per SBM bit */
#define BLKS_MAPPED_BY_INT (SBM_BITS_INT * ADVFS_BS_CLUSTSIZE)
#define BLKS_MAPPED_BY_PG (SBM_BITS_PG * ADVFS_BS_CLUSTSIZE)


/*****************************************************************************/
void
sbm_main(argc,argv)
int argc;
char *argv[];
{
    int fd;
    bitFileT bf;
    bitFileT *bfp = &bf;
    int typeFlg = 0;
    int flags = 0;
    bs_meta_page_t pagenum = 0;
    int64_t longnum;
    int ret;
    extern int optind;
    int index;

    bfp->pgLbn = LBN_BLK_TERM;

    init_bitfile(bfp);
    BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, BFM_SBM);

    ret = resolve_src(argc, argv, &optind, "rCf:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pmcell.volume, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind);
        /* NOT REACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( optind == argc ) {
        process_summary(bfp, flags);
        /* NOT REACHED */
    }

    if ( bfp->type & DMN && bfp->pmcell.volume == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    if ( bfp->type & VOL || bfp->type & DMN )
    {
        ret = find_SBM(bfp);
        assert(ret == OK);
    }

#ifdef FUTURE
    /* Test the SBM file */
    if ( strcmp(argv[optind], "-X") == 0 ) {
        optind++;
        if ( argc > optind && strcmp(argv[optind], "BMT") == 0 ) {
            if ( !(bfp->type & DMN) &&
                 !(bfp->type & VOL))
            {
                fprintf(stderr, "Cannot test SBM on saved file.\n");
                exit(1);
            }
            test_sbm(bfp, flags);
            /* NOT REACHED */
        } else {
            fprintf(stderr, "Bad syntax. Need BMT\n");
            exit(1);
        }
    }
#endif

    /* dump the SBM to a file */
    if ( argc > optind && strcmp(argv[optind], "-d") == 0 ) {
        save_sbm(bfp, argv, argc, optind);
        /* NOT REACHED */
    }

    /* show all the entries */
    if ( argc > optind && strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind);
        /* NOT REACHED */
    }

    /* show word specified by index # */
    if ( argc > optind && strcmp(argv[optind], "-i") == 0 ) {
        process_find_index(bfp, argv, argc, optind);
        /* NOT REACHED */
    }

    /* show the entry that maps a specific block */
    if ( argc > optind && strcmp(argv[optind], "-B") == 0 ) {
        process_find_block(bfp, argv, argc, optind);
        /* NOT REACHED */
    }

    /* get page number */
    if ( argc > optind && strcmp(argv[optind], "-p") == 0 ) {
        optind++;

        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }
        optind++;
        if ( pagenum < 0 || pagenum >= bfp->pages ) {
            fprintf(stderr, "Invalid SBM page number (%ld). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %ld.\n",
              bfp->pages - 1);
            exit(1);
        }

	/* get SBM page relative SBM entry index */
	if ( argc > optind ) {
	    if ( getnum(argv[optind], &longnum) != OK ) {
		fprintf(stderr, "Badly formed index: \"%s\"\n", argv[optind]);
		exit(1);
	    }
	    optind++;
	    if ( longnum < 0 || longnum >= SBM_INTS_PG ) {
		fprintf(stderr, "Invalid page relative index (%ld). ", longnum);
		fprintf(stderr, "Valid index is from 0 to %ld.\n", SBM_INTS_PG - 1);
		exit(2);
	    }

	    if ( argc > optind ) {
		fprintf(stderr, "Too many arguments.\n");
		usage();
	    }
	    process_index(bfp, pagenum, longnum);
	    /* NOT REACHED */
	}

	process_page(bfp, pagenum);
	exit(0);
    }

    usage();

} /* main */

/*****************************************************************************/
/* Input: bfp->pmcell.volume - volume on which to find the SBM
** Input: bfp->dmn->vols[] filled in
*/
int
find_SBM(bitFileT *bfp)
{
    int i;
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    uint16_t mc;

    assert(bfp);

    mc = BFM_SBM;

    bfp->pmcell.page = 0;
    bfp->pmcell.cell = mc;
    BS_BFTAG_RSVD_INIT( bfp->fileTag, bfp->pmcell.volume, mc);
    bfp->setTag = staticRootTagDirTag;

    if ( bfp->pmcell.volume == 0 ) {
        return OK;
    }

    if ( bfp->type & FLE ) {
        return OK;
    }

    assert(bfp->dmn);

    if ( bfp->dmn->vols[bfp->pmcell.volume] ) {
        return load_xtnts(bfp);
    }

    return 1;

} /* find_SBM */

/*****************************************************************************/

static void
process_summary(bitFileT *bfp, int flag)
{
    int ret;
    vdIndexT vdi;
    vdIndexT savpvol = bfp->pmcell.volume;
    bitFileT *bmtp;
    bsVdAttrT *bsVdAttrp;
    bsMCT *bsMCp;
    bsStgBmT *bsStgBmp;
    int index;
    uint32_t xor;
    int fobsUsed;
    int volBlks;
    int stop = FALSE;
    bs_meta_page_t pg;
    uint16_t mc;
    uint32_t sbm_entry;
    uint32_t mask;
    mc = BFM_RBMT; 

    for ( vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
            volBlks = 0;
            print_header(bfp);
        } else {
            if ( savpvol != 0 ) {
                /* if pvol set then only display summary for one volume */
                stop = TRUE;
                vdi = savpvol;
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pgVol = vdi;
            bfp->pgLbn = LBN_BLK_TERM;
            bfp->pgNum = BMT_PG_TERM;
            print_header(bfp);

	    bmtp = bfp->dmn->vols[vdi]->rbmt;

            ret = read_page(bmtp, 0);
            if ( ret != OK ) {
                fprintf(stderr, "read_page rbmt page 0 on vdi %d failed.\n",
                  vdi);
                continue;
            }

            /* find the volume block count in appropriate mcell */
            bsMCp = &((bsMPgT*)bmtp->pgBuf)->bsMCA[mc];
            ret = find_bmtr(bsMCp, BSR_VD_ATTR, (char**)&bsVdAttrp);
            if ( ret != OK ) {
                fprintf(stderr, "No BSR_VD_ATTR in mcell %d on vdi %d.\n", 
                        mc, vdi );
                continue;
            }
            volBlks = bsVdAttrp->vdBlkCnt;

            bfp->pmcell.volume = vdi;
            if ( find_SBM(bfp) ) {
                fprintf(stderr, "load_xtnts failed on vdi %d\n", vdi);
                continue;
            }
        }

        if ( bfp->pages == 1 ) {
            printf("There is 1 page in the SBM on this volume.\n");
        } else {
            printf("There are %ld pages in the SBM on this volume.\n",
              bfp->pages);
        }

        fobsUsed = 0;
        for ( pg = 0; pg < bfp->pages; pg++ ) {

            ret = read_page(bfp, pg);
            if ( ret != OK ) {
                fprintf(stderr,
                  "read_page failed for page %ld on SBM on vdi %d.\n", pg, vdi);
            }

            bsStgBmp = (bsStgBmT *)bfp->pgBuf;
            xor = 0;
            for ( index = 0; index < SBM_INTS_PG; index++ ) {
                sbm_entry = bsStgBmp->mapInt[index];

                xor ^= sbm_entry;

                if ( volBlks &&
		     (pg * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT) > 
		     volBlks &&
                     sbm_entry != 0 )
                {
                    fprintf(stderr,
            "SBM entries mapping beyond the end of the volume are not zero.\n");
                }

		/* count the number of FOBs allocated in this SBM entry */

                for (mask = 0x01; mask; mask <<= 1) {
                    if ( sbm_entry & mask ) {
                        fobsUsed += ADVFS_BS_CLUSTSIZE;
		    }
                }
            }

            if ( flag & CCFLG && xor != bsStgBmp->xor ) {
                printf("checksum on page %ld is wrong: ", pg);
                printf("stored xor %08x, calculated xor %08x.\n",
                       bsStgBmp->xor, xor);
            }
        }

        if ( volBlks ) {
            printf("The volume has %ld blocks (%ld FOBs).\n",
              volBlks, volBlks * ADVFS_FOBS_PER_DEV_BSIZE);
            printf("This domain SBM shows %ld FOBs (%ld%%) are used.\n\n", 
		   fobsUsed, 
		   (100 * fobsUsed) / (volBlks * ADVFS_FOBS_PER_DEV_BSIZE));
        } else {
            printf("The domain SBM shows %ld FOBs are used.\n\n", fobsUsed);
        }
    }

    exit(0);

} /* process_summary */

/*****************************************************************************/
static void
save_sbm(bitFileT *bfp, char *argv[], int argc, int optind)
{
    char *dfile = NULL;
    int ret;

    optind++;
    if ( !(bfp->type & DMN) && !(bfp->type & VOL) ) {
        fprintf(stderr, "-d only valid on a volume or domain.\n");
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

    bfp->pmcell.page = 0;
    bfp->pmcell.cell = BFM_SBM;
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pmcell.volume]);
    bfp->fd = bfp->dmn->vols[bfp->pmcell.volume]->fd;

    ret = load_xtnts(bfp);
    if ( ret != OK )
        exit(ret);

    if ( dumpBitFile(bfp, dfile) ) {
        exit(3);
    }

    exit(0);

} /* save_sbm */

/*****************************************************************************/
static void
process_block(bitFileT *bfp, char *argv[], int argc, int optind)
{
    uint64_t blknum = LBN_BLK_TERM;
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
              bfp->dmn->vols[bfp->pmcell.volume]->blocks - ADVFS_METADATA_PGSZ_IN_FOBS);
            exit(2);
        }
    }

    if ( rounddown(blknum, ADVFS_METADATA_PGSZ_IN_FOBS) != blknum ) {
        fprintf(stderr, "SBM pages are on %d block boundaries.\n",
          ADVFS_METADATA_PGSZ_IN_FOBS);
        blknum = rounddown(blknum, ADVFS_METADATA_PGSZ_IN_FOBS);
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%ld).\n",
          ADVFS_METADATA_PGSZ_IN_FOBS, blknum);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    bfp->pmcell.page = blknum / ADVFS_METADATA_PGSZ;
    bfp->pgLbn = blknum;
    print_pg_at_blk(bfp, blknum);
    exit(0);

} /* process_block */

/******************************************************************************/
int
print_pg_at_blk(bitFileT *bfp, uint64_t lbn)
{
    int results;
    bsStgBmT *bsStgBmp;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    /* malloc so that buffer is long alligned */
    bsStgBmp = (bsStgBmT*)malloc(ADVFS_METADATA_PGSZ);
    if ( bsStgBmp == NULL ) {
        perror("print_pg_at_blk");
        return ENOMEM;
    }

    if ( lseek(bfp->fd, (uint64_t)lbn * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "lseek to block %ld failed", lbn);
        perror(errstr);
	free(bsStgBmp);
        return 1;
    }

    results = read(bfp->fd, bsStgBmp, ADVFS_METADATA_PGSZ);
    if ( results != ADVFS_METADATA_PGSZ ) {
        if ( results == -1 ) {
            perror("print_pg_at_blk");
        } else {
            fprintf(stderr, "read returned %d expected %d\n",
              results, DEV_BSIZE);
        }
	free(bsStgBmp);
        return 2;
    }

    bfp->pgVol = bfp->pmcell.volume;
    bfp->pgLbn = lbn;
    print_header(bfp);

    print_page(bsStgBmp, 0, ADVFS_METADATA_PGSZ);

    free(bsStgBmp);
    return 0;

} /* print_pg_at_blk */

/*****************************************************************************/
static void
process_all(bitFileT *bfp, char *argv[], int argc, int optind)
{
    bs_meta_page_t pagenum;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
        process_page(bfp, pagenum);
    }

    exit(0);

} /* process_all */

/*****************************************************************************/
static int
process_page(bitFileT *bfp, bs_meta_page_t page)
{
    if ( read_page(bfp, page) != OK ) {
        fprintf(stderr, "read_page %ld failed.\n", page);
        exit(3);
    }

    print_header(bfp);

    print_page((bsStgBmT*)PAGE(bfp), page, ADVFS_METADATA_PGSZ);

    return 0;

} /* process_page */

/******************************************************************************/
#define INTS_PER_LINE 4
/* len in bytes */
static void
print_page(bsStgBmT *bsStgBmp, bs_meta_page_t pn, int len)
{
    int index;
    uint32_t xor;

    printf("pageNumber %ld  ", bsStgBmp->pageNumber);
    printf("xor %08x\n", bsStgBmp->xor);
    printf(" index      block   mapInt[]\n");

    xor = 0;
    index = 0;
    while ( index < SBM_INTS_PG ) {
        printf("%6d   ", pn * SBM_INTS_PG + index);
        printf("0x%06x  ", pn * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT);

        while ( index < SBM_INTS_PG ) {
            printf(" %08x", bsStgBmp->mapInt[index]);

	    xor ^= bsStgBmp->mapInt[index];
            index++;

            if ( index % INTS_PER_LINE == 0 ) {
                break;
            }
        }
        printf("\n");
    }

    printf("  On-disk xor: %08x, Calculated xor: %08x\n", bsStgBmp->xor, xor);
    return;

} /* print_page */

/*****************************************************************************/
static void
process_index(bitFileT *bfp, bs_meta_page_t pagenum, int64_t index)
{
    if (read_page(bfp, pagenum)) {
        fprintf(stderr, "Bad read\n");
        exit(2);
    }

    print_header(bfp);

    printf("index %d  block %d-%d (0x%x-0x%x)   mapInt 0x%08x\n",
      pagenum * SBM_INTS_PG + index,
      pagenum * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT,
      pagenum * BLKS_MAPPED_BY_PG + (index + 1) * BLKS_MAPPED_BY_INT - 1,
      pagenum * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT,
      pagenum * BLKS_MAPPED_BY_PG + (index + 1) * BLKS_MAPPED_BY_INT - 1,
      ((bsStgBmT *)PAGE(bfp))->mapInt[index]);

    exit(0);

} /* process_index */

/*****************************************************************************/
static void
process_find_index(bitFileT *bfp, char *argv[], int argc, int optind)
{
    int64_t index;
    bs_meta_page_t pagenum;

    optind++;
    if ( argc == optind ) {
        fprintf(stderr, "-i requires index argument\n");
        usage();
    }

    if ( getnum(argv[optind], &index) != OK ) {
        fprintf(stderr, "Badly formed index number: \"%s\"\n", argv[optind]);
        exit(1);
    }
    optind++;

    if ( argc != optind ) {
        fprintf(stderr, "too many arguments\n");
        usage();
    }

    if ( index < 0 ) {
        fprintf(stderr,
          "Invalid index number (%d), index number must be positive\n", index);
        exit(1);
    }

    pagenum = index / SBM_INTS_PG;
    if ( pagenum >= bfp->pages ) {
        fprintf(stderr,
          "Invalid index number (%d), the last valid index is %d\n",
          pagenum, SBM_INTS_PG * (bfp->pages - 1));
        exit(1);
    }

    if (read_page(bfp, pagenum)) {
        fprintf(stderr, "Bad read\n");
        exit(2);
    }

    print_header(bfp);

    printf("index %d  block %d-%d (0x%x-0x%x)   mapInt 0x%08x\n", index,
      index * BLKS_MAPPED_BY_INT,
      (index + 1) * BLKS_MAPPED_BY_INT - 1,
      index * BLKS_MAPPED_BY_INT,
      (index + 1) * BLKS_MAPPED_BY_INT - 1,
      ((bsStgBmT *)PAGE(bfp))->mapInt[index % SBM_INTS_PG]);

    exit(0);

} /* process_find_index */

/*****************************************************************************/
#define FORMAT_BITS_PER_BYTE 8

static void
process_find_block(bitFileT *bfp, char *argv[], int argc, int optind)
{
    int64_t block;
    int64_t index;
    bs_meta_page_t pagenum;
    int bitIndex;
    uint32_t sbmMapInt;
    char number[10];
    int numsz;
    int blkBitIndex;

    optind++;
    if ( argc == optind ) {
        fprintf(stderr, "-B requires block argument\n");
        usage();
    }

    if ( getnum(argv[optind], &block) != OK ) {
        fprintf(stderr, "Badly formed block number: \"%s\"\n", argv[optind]);
        exit(1);
    }
    optind++;

    if ( argc != optind ) {
        fprintf(stderr, "too many arguments\n");
        usage();
    }

    if ( block < 0 ) {
        fprintf(stderr,
          "Invalid block number (%ld), block number must be positive\n", block);
        exit(1);
    }

    pagenum = block / BLKS_MAPPED_BY_PG;
    if ( pagenum >= bfp->pages ) {
        fprintf(stderr,
          "Invalid block number (%ld), the last block mapped by the SBM is %ld\n",
                pagenum, SBM_INTS_PG * (bfp->pages - 1));
        exit(1);
    }

    if (read_page(bfp, pagenum)) {
        fprintf(stderr, "Bad read\n");
        exit(2);
    }

    print_header(bfp);

    block = rounddown(block, ADVFS_BS_CLUSTSIZE);

    index = block / BLKS_MAPPED_BY_INT;
    blkBitIndex = (block / ADVFS_BS_CLUSTSIZE) % SBM_BITS_INT;

    printf("blocks %d - %d (0x%x - 0x%x) are mapped by SBM map entry %d, bit %d\n",
	   block, block + ADVFS_BS_CLUSTSIZE - 1, block, 
	   block + ADVFS_BS_CLUSTSIZE - 1, index, blkBitIndex);

    sbmMapInt = ((bsStgBmT *)PAGE(bfp))->mapInt[index % SBM_INTS_PG];

    sprintf(number, "%d", block);
    numsz = strlen(number);
    sprintf(number, "%d", index);
    numsz -= strlen(number);
    for ( bitIndex = 0; bitIndex < numsz; bitIndex++ ) {
        printf(" ");
    }

    printf("mapInt[%d]  ", index);
    for ( bitIndex = SBM_BITS_INT - 1; bitIndex >= 0; bitIndex-- ) {
        uint32_t mask = 0x1;

        if ( mask << bitIndex & sbmMapInt ) {
            printf("1");
        } else {
            printf("0");
        }

        /* break bit string every 8 bits */
        if ( bitIndex % FORMAT_BITS_PER_BYTE == 0 ) {
            printf(" ");
        }
    }
    printf("\n");

    printf("  block %d  ", block);
    for ( bitIndex = SBM_BITS_INT - 1; bitIndex >= 0; bitIndex-- ) {
        if ( bitIndex == blkBitIndex ) {
            printf("^");
            break;
        } else {
            printf(" ");
        }

        /* break bit string every 8 bits */
        if ( bitIndex % FORMAT_BITS_PER_BYTE == 0 ) {
            printf(" ");
        }
    }
    printf("\n");

    exit(0);

} /* process_find_block */

#ifdef FUTURE
test_sbm(bitFileT *bfp, int flags)
{
    struct dmn *dmnp;
    volT *volp;
    bitFileT *bmtBfp;
    bsStgBmT *bsStgBmp;
    bs_meta_page_t sbmPgs;
    bf_vd_blk_t sbmBlks;
    bs_meta_page_t pg;
    uint32_t skipXor = 0;

    dmnp = bfp->dmn;
    assert(dmnp != NULL);
    assert(bfp->pmcell.volume != 0);
    volp = dmnp->vols[bfp->pmcell.volume];
    assert(volp != NULL);
    bmtBfp = volp->bmt;
    assert(bmtBfp != NULL);

    sbmPgs = howmany( volp->blocks / BS_CLUSTSIZE, SBM_BITS_PG);
//        sbmBlks = roundup( ADVFS_METADATA_PGSZ_IN_FOBS * sbmPgs, BS_CLUSTSIZE );

    if ( bfp->pages > sbmPgs ) {
        fprintf(stderr, "test_sbm: bfp->pages > sbmPgs\n");
        sbmPgs = bfp->pages;
    } else if ( sbmPgs > bfp->pages ) {
        fprintf(stderr, "test_sbm: sbmPgs > bfp->pages\n");
    }

    bsStgBmp = (bsStgBmT*)malloc(sbmPgs * ADVFS_PGSZ);

// need to do this for RBMT and BMT
    for ( pg = 0; pg < bmtBfp->pages; pg++ ) {
        bsMPgT *bsMPgp;
        uint32_t cn;

        if ( read_page(bmtBfp, pg) != OK ) {
            fprintf(stderr, "test_sbm: read BMT page %ld failed\n", pg);
            return 1;
        }

        bsMPgp = (bsMPgT*)bmtBfp->pgBuf;

        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            bsMCT *bsMCp = &bsMPgp->bsMCA[cn];
            bsXtntRT *bsXtntRp;
            bsXtraXtntRT *bsXtraXtntRp;
            bsXtntT *bsXA;
            uint32_t xcnt;
            int ret;
            uint32_t xc;
            uint32_t startBlk;
            uint32_t startPg;

            if ( find_bmtr(bsMCp, BSR_XTNTS, (char**)&bsXtntRp) == OK ) {
                xcnt = bsXtntRp->xCnt;

                if ( xcnt > BMT_XTNTS ) {
                    fprintf(stderr, "test_sbm: xcnt > BMT_XTNTS\n");
                    xcnt = 3;
                }

                bsXA = bsXtntRp->bsXA;
            } else if ( find_bmtr(bsMCp, BSR_XTRA_XTNTS,
                                  (char**)&bsXtraXtntRp) == OK )
            {
                xcnt = bsXtraXtntRp->xCnt;

                if ( xcnt > BMT_XTRA_XTNTS ) {
                    fprintf(stderr, "test_sbm: xcnt > BSR_XTRA_XTNTS\n");
                    xcnt = BMT_XTRA_XTNTS;
                }

                bsXA = bsXtraXtntRp->bsXA;
            } else {
                continue;
            }

            startPg = bsXA[0].bsPage;
            startBlk = bsXA[0].vdBlk;
            for ( xc = 1; xc < xcnt; xc++ ) {
                if ( startBlk != XTNT_TERM && startBlk != COWED_HOLE) {
                    uint32_t blks;

                    if ( bsXA[xc].bsPage <= startPg ) {
//                        fprintf(stderr, "Bad xtnt: last pg %d, current pg %d\n", startPg, bsXA[xc].bsPage);
                    } else {
                        blks = (bsXA[xc].bsPage - startPg) * ADVFS_METADATA_PGSZ_IN_FOBS;

                        if ( fill_inmemSBM(bfp, bsStgBmp, startBlk, blks) != OK ) {
                            fprintf(stderr, "BMT pg %ld, cell %d, index %d\n", pg, cn, xc);
                        }
                    }
                }

                if ( bsXA[xc].bsPage <= startPg ) {
                    uint32_t startClust;
                    uint32_t startPg;
                    uint32_t firstInt;
                    uint32_t cur_bit;

                    fprintf(stderr, "Bad BMT entry: pg %ld, cell %d, entry %d:\n",  pg, cn, xc);
                    if ( (signed)bsXA[xc - 1].vdBlk > 0 ) {
                        startClust = bsXA[xc - 1].vdBlk / BS_CLUSTSIZE;
                        startPg = startClust / SBM_BITS_PG;
                        cur_bit = startClust % SBM_BITS_PG;
                        firstInt = cur_bit/32;
                        cur_bit %= 32;
                        skipXor ^= 1 << cur_bit;
//fprintf(stderr, "skipXor %x\n", skipXor);

                        fprintf(stderr, "  skip entry %d: pg %d blk %d ",
                          xc - 1, bsXA[xc - 1].bsPage, bsXA[xc - 1].vdBlk);
                        fprintf(stderr, "(SBM pg %d, int %d, bit %d [%4x])\n",
                          startPg, firstInt, cur_bit, 1<<cur_bit);
                    } else {
                        fprintf(stderr, "       entry %d: pg %d blk %d\n",
                          xc - 1, bsXA[xc - 1].bsPage, bsXA[xc - 1].vdBlk);
                    }
                    if ( (signed)bsXA[xc].vdBlk > 0 ) {
                        startClust = bsXA[xc].vdBlk / BS_CLUSTSIZE;
                        startPg = startClust / SBM_BITS_PG;
                        cur_bit = startClust % SBM_BITS_PG;
                        firstInt = cur_bit/32;
                        cur_bit %= 32;
                        skipXor ^= 1 << cur_bit;
//fprintf(stderr, "skipXor %x\n", skipXor);

                        fprintf(stderr, "  skip entry %d: pg %d blk %d ",
                          xc, bsXA[xc].bsPage, bsXA[xc].vdBlk);
                        fprintf(stderr, "(SBM pg %d, int %d, bit %d [%4x])\n",
                          startPg, firstInt, cur_bit, 1<<cur_bit);
                    }

                    if ( xc < xcnt - 1 ) {
                        /* ignore bad area. move on */
                        xc++;

                        fprintf(stderr, "       entry %d: pg %d blk %d\n",
                          xc, bsXA[xc].bsPage, bsXA[xc].vdBlk);
                    }
                }
                startPg = bsXA[xc].bsPage;
                startBlk = bsXA[xc].vdBlk;
            }  /* for each extent */
        }  /* for each mcell */
    }  /* for each BMT page */

//compare in-mem-sbm with SBM
// separate func. pass in -v flag: if -v then find BMT entries.
    for ( pg = 0; pg < bfp->pages; pg++ ) {
        bsStgBmT *SBMpgp;
        uint32_t index;
        uint32_t xor = 0;

        if ( read_page(bfp, pg) != OK ) {
            fprintf(stderr, "test_sbm: read SBM page %d failed\n", pg);
            return 1;
        }

        SBMpgp = (bsStgBmT*)bfp->pgBuf;

        for ( index = 0; index < SBM_LONGS_PG; index++ ) {
            if ( SBMpgp->mapInt[index] != bsStgBmp[pg].mapInt[index] ) {
                fprintf(stderr, "SBM pg %d, int %d is %x. inmem SBM is %x\n", pg, index, SBMpgp->mapInt[index], bsStgBmp[pg].mapInt[index]);
//fprintf(stderr, "%x ^ %x => ", xor, SBMpgp->mapInt[index] ^ bsStgBmp[pg].mapInt[index]);
                xor ^= SBMpgp->mapInt[index] ^ bsStgBmp[pg].mapInt[index];
//fprintf(stderr, "%x\n", xor);
            }
        }

        if ( SBMpgp->xor != bsStgBmp[pg].xor ) {
            fprintf(stderr, "SBM pg %d xor checksum is %x, inmem SBM xor checksum is %x\n", pg, SBMpgp->xor, bsStgBmp[pg].xor);
            if ( SBMpgp->xor != (bsStgBmp[pg].xor ^ xor) ) {
fprintf(stderr, "  The xor delta between these two checksums is %x\n", SBMpgp->xor ^ bsStgBmp[pg].xor);
fprintf(stderr, "  The xor delta from the SBM vs BMT map entry differences is %x\n", xor);
fprintf(stderr, "  Applying the SBM/BMT delta leaves an xor delta of %x\n", (bsStgBmp[pg].xor ^ xor) ^ SBMpgp->xor);
if ( skipXor != 0 )
fprintf(stderr, "  Some BMT extents were skipped. The skip xor is %x\n", skipXor);
//fprintf(stderr, "xor %x, SBMpgp->xor ^ xor %x\n", xor, SBMpgp->xor ^ xor);
//fprintf(stderr, "delta %x, skipXor %x\n", (bsStgBmp[pg].xor ^ xor) ^ SBMpgp->xor, skipXor);
            } else {
                fprintf(stderr, "  The difference between SBM entries and BMT data fully accounts for this.\n");
            }
        } else {
            fprintf(stderr, "The pg %d SBM checksum and the inmem SBM checksum are the same.\n", pg);
        }
    }

    exit(0);

} /* test_sbm */

//sbm_remove_space(,uint32_t startBlk,uint32_t blks,)
fill_inmemSBM(bitFileT *bfp,
              bsStgBmT *bsStgBmp,
              uint32_t startBlk,
              uint32_t blks)
{
    uint32_t startClust;
    uint32_t clusts;
    uint32_t startPg;
    uint32_t endPg;
    uint32_t pg;
    uint32_t cur_bit;
    uint32_t endBit;
    uint32_t set_bits;
    uint32_t bits_to_set;
    int ret = 0;

    startClust = startBlk / BS_CLUSTSIZE;
    clusts = blks / BS_CLUSTSIZE;
    if ( clusts * BS_CLUSTSIZE != blks ) {
        fprintf(stderr, "fill_inmemSBM: clusts * BS_CLUSTSIZE != blks\n");
    }

    startPg = startClust / SBM_BITS_PG;
    endPg = (startClust + (clusts - 1)) / SBM_BITS_PG;

    cur_bit = startClust % SBM_BITS_PG;
    bits_to_set = clusts;

//fprintf(stderr, "startBlk %d, blks %d, startPg %d, endPg %d\n", startBlk, blks, startPg, endPg);
    for ( pg = startPg; pg <= endPg; pg++ ) {
        int firstInt;
        int lastInt;
        int wordBit;
        uint32_t mask;
        const uint32_t allSet = 0xffffffff;
        uint32_t i;

        set_bits = MIN( bits_to_set, SBM_BITS_PG - cur_bit );
        endBit   = cur_bit + set_bits - 1;

//      alloc_bits_page (, sbm_pg, cur_bit, endBit,);
        firstInt = cur_bit/32;
        lastInt  = endBit/32;
        wordBit  = firstInt*32;
        mask     = allSet << (cur_bit - wordBit);

        if (firstInt == lastInt) {
            mask &= allSet>>((wordBit + 31) - endBit);
            if (bsStgBmp[pg].mapInt[firstInt] & mask) {
                fprintf(stderr, "CSBT: SBM pg %d int %d: existing %x, to be set %x\n",
                  pg, firstInt, bsStgBmp[pg].mapInt[firstInt], mask);
//fprintf(stderr, "startBlk %d, blks %d\n", startBlk, blks);
                ret = 1;
            }
            bsStgBmp[pg].mapInt[firstInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
        } else {
            if (bsStgBmp[pg].mapInt[firstInt] & mask) {
                fprintf(stderr, "CSBT: firstint: existing %x, to be set %x\n",
                  bsStgBmp[pg].mapInt[firstInt], mask);
//fprintf(stderr, "pg %d, int %d\n", pg, firstInt);
//fprintf(stderr, "startBlk %d, blks %d\n", startBlk, blks);
                ret = 1;
            }
            bsStgBmp[pg].mapInt[firstInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
    
            for (i = firstInt + 1; i < lastInt; i++) {
                if (bsStgBmp[pg].mapInt[i] != 0) {
                    fprintf(stderr, "CSBT: existing %x, to be set %x\n",
                      bsStgBmp[pg].mapInt[i], allSet);
//fprintf(stderr, "pg %d, int %d\n", pg, i);
//fprintf(stderr, "startBlk %d, blks %d\n", startBlk, blks);
                    ret = 1;
                }
                bsStgBmp[pg].mapInt[i] = allSet;
                bsStgBmp[pg].xor ^= allSet;
            }
            wordBit = lastInt * 32;  /* first bit of last word */
            mask = allSet>>((wordBit + 31) - endBit);
            if (bsStgBmp[pg].mapInt[lastInt] & mask) {
                fprintf(stderr, "CSBT: lastInt: existing %x, to be set %x\n",
                  bsStgBmp[pg].mapInt[lastInt], mask);
//fprintf(stderr, "pg %d, int %d\n", pg, lastInt);
//fprintf(stderr, "startBlk %d, blks %d\n", startBlk, blks);
                ret = 1;
            }
            bsStgBmp[pg].mapInt[lastInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
        }

        cur_bit = (cur_bit + set_bits) % SBM_BITS_PG;
        bits_to_set -= set_bits;
    }

    return ret;

} /* fill_inmemSBM */
#endif  /* FUTURE */
