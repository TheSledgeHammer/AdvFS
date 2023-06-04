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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      View On-Disk Structure
 *
 * Date:
 *
 *	Tue Mar  5 16:31:54 PST 1996
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vsbmpg.c,v $ $Revision: 1.1.8.2 $ (DEC) $Date: 2006/08/30 15:25:53 $"

#include <stdio.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>
#include <msfs/bs_public.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>
#include "vods.h"

static char *Prog;
static int SBMversion = 0;

extern int dmnVersion;    /* declared in vods.c */

/* Make new defines. SBM_LONGS_PG & SBM_BITS_LONG are too misleading. */
/* SBM_INTS_PG is the number of SBM mapInt entries (uint32T) per SBM page. */
#define SBM_INTS_PG SBM_LONGS_PG
/* SBM_BITS_INT is the number of bits in a mapInt entry (32) */
#define SBM_BITS_INT SBM_BITS_LONG

/* blks per SBM bit. 16 in version 4, 2 in version 3 */
#define CLUSTER_SZ (SBMversion == 3 ? BS_CLUSTSIZE_V3 : BS_CLUSTSIZE)
#define BLKS_MAPPED_BY_INT (SBM_BITS_INT * CLUSTER_SZ)
#define BLKS_MAPPED_BY_PG (SBM_BITS_PG * CLUSTER_SZ)

/* external prototypes */
void modify_sbm( int, char *[], int, bitFileT *, int, int, int );


/*****************************************************************************/
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s [-v] <dmn>		print summary, all volumes\n", Prog);
    fprintf(stderr, "%s [-v] <vol>		print summary\n", Prog);
    fprintf(stderr, "%s <src> page [index]	print SBM page or entry\n", Prog);
    fprintf(stderr, "%s <src> -i index		print SBM entry\n", Prog);
    fprintf(stderr, "%s <src> -B block		print SBM entry\n", Prog);
    fprintf(stderr, "%s <vol> -b block		print SBM page\n", Prog);
    fprintf(stderr, "%s <vol> -d sav_file	save SBM\n", Prog);
    fprintf(stderr, "%s <vol> -X		test SBM\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<src> = <vol> | [-F] file\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = [-V] volume_name | <dmn> vdi\n");

    exit(1);
}

/*
** vsbmpg [-v] <dmn>		summary
** vsbmpg [-v] <src>		summary
** vsbmpg <src> page		one page
** vsbmpg <src> page [entry]	one int, page relative
** vsbmpg <src> -i index	one int, global index
** vsbmpg <src> -B block	one int, maps block
** vsbmpg <vol> -b block	one page
** vsbmpg <vol> -d file		dump SBM

** <src> = <vol> | [-F] file
** <dmn> = [-r] [-D] domain_name
** <vol> = [-V] volume_name | <dmn> vdi

** to do
** vsbmpg [-v] <dmn> | <src>  print histogram of free extent sizes
** vsbmpg <vol> -b block entry	one entry
** vsbmpg [-3|-4] <src> ...  1K cluster sz for version 3 SBM files
*/

/*****************************************************************************/
main(argc,argv)
int argc;
char *argv[];
{
    int fd;
    bitFileT bf;
    bitFileT *bfp = &bf;
    int typeFlg = 0;
    int flags = 0;
    long pagenum = 0;
    long longnum;
    int ret;
    extern int optind;

    bfp->pgLbn = XTNT_TERM;
    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    if ( argc < 2 ) {
        usage();
    }

    init_bitfile(bfp);
    bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_SBM);

    ret = resolve_src(argc, argv, &optind, "rvD:V:F:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        find_version(bfp);
        /* process_block needs bfp->dmn, bfp->pvol, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( optind == argc ) {
        find_version(bfp);
        process_summary(bfp, flags);
        /* NOTREACHED */
    }

    if ( bfp->type & VOL ||
         (bfp->type & DMN || bfp->type & SAV) && bfp->pvol != 0 )
    {
        ret = find_SBM(bfp);
        assert(ret == OK);
    }

    if ( (bfp->type & DMN || bfp->type & SAV) && bfp->pvol == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    /* Test the SBM file */
    if ( strcmp(argv[optind], "-X") == 0 ) {
        optind++;
        if ( argc > optind && strcmp(argv[optind], "BMT") == 0 ) {
            if ( !(bfp->type & DMN) &&
                 !(bfp->type & VOL) &&
                 !(bfp->type & SAV) )
            {
                fprintf(stderr, "Can't test SBM on saved file.\n");
                exit(1);
            }
            test_sbm(bfp, flags);
            /* NOTREACHED */
        } else {
            fprintf(stderr, "Bad syntax. Need BMT\n");
            exit(1);
        }
    }

    /* dump the SBM to a file */
    if ( argc > optind && strcmp(argv[optind], "-d") == 0 ) {
        save_sbm(bfp, argv, argc, optind/*, flags & VFLG*/);
    }

    find_version(bfp);

    if ( argc > optind && strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind);
    }

    if ( argc > optind && strcmp(argv[optind], "-i") == 0 ) {
        process_find_index(bfp, argv, argc, optind);
    }

    if ( argc > optind && strcmp(argv[optind], "-B") == 0 ) {
        process_find_block(bfp, argv, argc, optind);
    }

    /* get page number */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }
        optind++;
        if ( pagenum < 0 || pagenum >= bfp->pages ) {
            fprintf(stderr, "Invalid SBM page number (%d). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %d.\n",
              bfp->pages - 1);
            exit(1);
        }
    }

    /* get SBM page relative SBM entry index */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &longnum) != OK ) {
            fprintf(stderr, "Badly formed index: \"%s\"\n", argv[optind]);
            exit(1);
        }
        optind++;
        if ( longnum < 0 || longnum >= SBM_INTS_PG ) {
            fprintf(stderr, "Invalid page relative index (%d). ", longnum);
            fprintf(stderr, "Valid index is from 0 to %d.\n", SBM_INTS_PG - 1);
            exit(2);
        }

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            usage();
        }
        process_index(bfp, pagenum, longnum);
    }

    process_page(bfp, pagenum);

    exit(0);
}

/*****************************************************************************/
/* Try to guess the version of the domain for SBM saved files and
** corrupted volumes.
** If we have a version number from the AdvFS on disk structures use that.
** Otherwise, read the SBM pages and look for any non FF and non 00 bytes.
** The existance of such a byte is taken to mean that the domain
** is version 4 with 1 bit per page allocation. If all the bytes
** are 00 or FF then we assume that the domain is version 3.
*/
find_version(bitFileT *bfp)
{
    vdIndexT vdi;
    vdIndexT savpvol = bfp->pvol;
    bsStgBmT *bsStgBmp;
    int pg;
    int stop;
    int index;
    int ret;

    if ( bfp->type & DMN || bfp->type & VOL ) {
        assert(bfp->dmn);

        if ( bfp->dmn->dmnVers != 0 ) {
            SBMversion = bfp->dmn->dmnVers;
            if ( SBMversion < 2 ) {
                /* failsafe. This should be done in open_vol. */
                SBMversion = 3;
            }

            return;
        }
    }

    SBMversion = 3;

    for ( stop = FALSE, vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
        } else {
            if ( savpvol != 0 ) {
                /* if pvol set then we only want to know about one volume */
                stop = TRUE;
                vdi = savpvol;
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pvol = vdi;
            if ( find_SBM(bfp) ) {
                fprintf(stderr, "load_xtnts failed on vdi %d\n", vdi);
                continue;
            }
        }

        for ( pg = 0; pg < bfp->pages; pg++ ) {
            ret = read_page(bfp, pg);
            if ( ret != OK ) {
                fprintf(stderr,
                  "read_page failed for page %d on SBM on vdi %d.\n", pg, vdi);
            }

            bsStgBmp = (bsStgBmT *)bfp->pgBuf;
            for ( index = 0; index < SBM_INTS_PG; index++ ) {
                if ( bsStgBmp->mapInt[index] & 0xff &&
                      (bsStgBmp->mapInt[index] & 0xff) != 0xff ||
                     bsStgBmp->mapInt[index] & 0xff00 &&
                      (bsStgBmp->mapInt[index] & 0xff00) != 0xff00 ||
                     bsStgBmp->mapInt[index] & 0xff0000 &&
                      (bsStgBmp->mapInt[index] & 0xff0000) != 0xff0000 ||
                     bsStgBmp->mapInt[index] & 0xff000000 &&
                      (bsStgBmp->mapInt[index] & 0xff000000) != 0xff000000 )
                {
                    SBMversion = 4;
                    bfp->pvol = savpvol;
                    return;
                }
            }
        }
    }

    bfp->pvol = savpvol;
}

/*****************************************************************************/
/* Input: bfp->pvol - volume on which to find the SBM
** Input: bfp->dmn->vols[] filled in
*/
int
find_SBM(bitFileT *bfp)
{
    int i;
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    int mc;

    assert(bfp);

    mc = RBMT_PRESENT ? BFM_SBM : BFM_SBM_V3;

    bfp->ppage = 0;
    bfp->pcell = mc;
    bfp->fileTag = RSVD_TAGNUM_GEN(bfp->pvol, mc);
    bfp->setTag = -2;

    if ( bfp->pvol == 0 ) {
        return OK;
    }

    if ( bfp->type & FLE ) {
        return OK;
    }

    assert(bfp->dmn);
    /* FIX. for RBMT_PRESENT above!! vers for FLE?? */
    assert(bfp->dmn->dmnVers);

    if ( bfp->dmn->vols[bfp->pvol] ) {
        return load_xtnts(bfp);
    }

    return 1;
}

/*****************************************************************************/
process_summary(bitFileT *bfp, int flag)
{
    int ret;
    vdIndexT vdi;
    vdIndexT savpvol = bfp->pvol;
    bitFileT *bmtp;
    bsVdAttrT *bsVdAttrp;
    bsMCT *bsMCp;
    bsStgBmT *bsStgBmp;
    int index;
    uint32T xor;
    int pagesUsed;
    lbnT volBlks;
    int stop = FALSE;
    int pg;
    int mc;
    int pg_shf;
    int pg_mask;
    int shft;
    uint32T sbm_entry;
    uint32T mask;
    int badXOR;

    assert(SBMversion != 0);

    if ( SBMversion == 3 ) {
        mc = BFM_BMT_EXT_V3;
        pg_shf = 8;                    /* SBM bits per AdvFS page (8K) */
        pg_mask = 0xff;                /* mask for SBM bits per page */
    } else  {
        mc = BFM_RBMT_EXT;
        pg_shf = 1;                    /* SBM bits per AdvFS page (8K) */
        pg_mask = 0x1;                 /* mask for SBM bits per page */
    }

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
            bfp->pgLbn = XTNT_TERM;
            bfp->pgNum = -1;
            print_header(bfp);

            if ( RBMT_PRESENT ) {
                bmtp = bfp->dmn->vols[vdi]->rbmt;
            } else {
                bmtp = bfp->dmn->vols[vdi]->bmt;
            }

            ret = read_page(bmtp, 0);
            if ( ret != OK ) {
                fprintf(stderr, "read_page bmt page 0 on vdi %d failed.\n",
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

            bfp->pvol = vdi;
            if ( find_SBM(bfp) ) {
                fprintf(stderr, "load_xtnts failed on vdi %d\n", vdi);
                continue;
            }
        }

        if ( bfp->pages == 1 ) {
            printf("There is 1 page in the SBM on this volume.\n");
        } else {
            printf("There are %d pages in the SBM on this volume.\n",
              bfp->pages);
        }

        pagesUsed = 0;
        for ( pg = 0; pg < bfp->pages; pg++ ) {
            badXOR = FALSE;
            ret = read_page(bfp, pg);
            if ( ret != OK ) {
                fprintf(stderr,
                  "read_page failed for page %d on SBM on vdi %d.\n", pg, vdi);
            }

            bsStgBmp = (bsStgBmT *)bfp->pgBuf;
            xor = 0;
            for ( index = 0; index < SBM_INTS_PG; index++ ) {
                sbm_entry = bsStgBmp->mapInt[index];

                xor ^= sbm_entry;

                if ( volBlks &&
              (pg * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT) > volBlks &&
                     sbm_entry != 0 )
                {
                    fprintf(stderr,
            "SBM entries mapping beyond the end of the volume are not zero.\n");
                }

                mask = pg_mask;
                for ( shft = SBM_BITS_INT; shft; shft -= pg_shf ) {
                    if ( sbm_entry & mask ) {
                        pagesUsed++;
                        if ( (sbm_entry & mask) != mask ) {
                            fprintf(stderr,
   "SBM corruption. Partial page allocated: vdi %d, page %d, index %d is %x.\n",
                              vdi, pg, index, bsStgBmp->mapInt[index]);
                        }
                    }

                    mask <<= pg_shf;
                }
            }

            if ( flag & VFLG && xor != bsStgBmp->xor ) {
                badXOR = TRUE;
                printf("checksum on page %d is wrong: ", pg);
                printf("stored xor %08x, calculated xor %08x.\n",
                       bsStgBmp->xor, xor);
            }
        }

        if (  flag & VFLG && !badXOR ) {
            printf("All page XOR checksums are good\n");
        }

        if ( volBlks ) {
            printf("The volume has %u blocks (%d pages).\n",
              volBlks, volBlks / ADVFS_PGSZ_IN_BLKS);
            printf("This domain version %d SBM shows %d pages (%d%%) are used.\n\n",
              SBMversion, pagesUsed,
              (ulong)100 * pagesUsed * ADVFS_PGSZ_IN_BLKS / volBlks);
        } else {
            printf("Domain version %d SBM. %d pages are used.\n\n",
              SBMversion, pagesUsed);
        }
    }

    exit(0);
}

/*****************************************************************************/
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

    bfp->ppage = 0;
    bfp->pcell = BFM_SBM;
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);
    bfp->fd = bfp->dmn->vols[bfp->pvol]->fd;

    ret = load_xtnts(bfp);
    if ( ret != OK )
        exit(ret);

    if ( dumpBitFile(bfp, dfile) ) {
        exit(3);
    }

    exit(0);
}

/*****************************************************************************/
process_block(bitFileT *bfp, char *argv[], int argc, int optind)
{
    long blknum = -1;

    assert(bfp->fd);
    if ( bfp->type & FLE || bfp->type & SAV ) {
        fprintf(stderr, "Can't use -b flag on a file\n");
        usage();
    }

    optind++;

    if ( argc > optind ) {
        if ( getnum(argv[optind], &blknum) != OK ) {
            fprintf(stderr, "Badly formed block number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
    } else if ( argc == optind ) {
        fprintf(stderr, "Missing block number.\n");
        usage();
    }

    if ( bfp->dmn->vols[bfp->pvol]->blocks != 0 ) {
        if ( (unsigned long)blknum >= bfp->dmn->vols[bfp->pvol]->blocks ) {
            fprintf(stderr, "Invalid block number (%u). ", blknum);
            fprintf(stderr, "Valid block numbers range from 0 to %u.\n",
              bfp->dmn->vols[bfp->pvol]->blocks - ADVFS_PGSZ_IN_BLKS);
            exit(2);
        }
    }

    if ( rounddown(blknum, ADVFS_PGSZ_IN_BLKS) != blknum ) {
        fprintf(stderr, "SBM pages are on %d block boundaries.\n",
          ADVFS_PGSZ_IN_BLKS);
        blknum = rounddown(blknum, ADVFS_PGSZ_IN_BLKS);
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%d).\n",
          ADVFS_PGSZ_IN_BLKS, blknum);
    }

    bfp->pgLbn = blknum;

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    bfp->ppage = blknum / PAGE_SIZE;
    print_pg_at_blk(bfp, blknum);
    exit(0);
}

/******************************************************************************/
int
print_pg_at_blk(bitFileT *bfp, lbnT lbn)
{
    int results;
    bsStgBmT *bsStgBmp;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    /* malloc so that buffer is long alligned */
    bsStgBmp = (bsStgBmT*)malloc(PAGE_SIZE);
    if ( bsStgBmp == NULL ) {
        perror("print_pg_at_blk");
        return ENOMEM;
    }

    if ( lseek(bfp->fd, (ulong_t)lbn * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "lseek to block %u failed", lbn);
        perror(errstr);
        return 1;
    }

    results = read(bfp->fd, bsStgBmp, PAGE_SIZE);
    if ( results != PAGE_SIZE ) {
        if ( results == -1 ) {
            perror("print_pg_at_blk");
        } else {
            fprintf(stderr, "read returned %d expected %d\n",
              results, DEV_BSIZE);
        }
        return 2;
    }

    bfp->pgVol = bfp->pvol;
    bfp->pgLbn = lbn;
    print_header(bfp);

    print_page(bsStgBmp, 0, PAGE_SIZE);

    return 0;
}

/*****************************************************************************/
process_all(bitFileT *bfp, char *argv[], int argc, int optind)
{
    int pagenum;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
        process_page(bfp, pagenum);
    }

    exit(0);
}

/*****************************************************************************/
int
process_page(bitFileT *bfp, int page)
{
    if ( read_page(bfp, page) != OK ) {
        fprintf(stderr, "read_page %d failed.\n", page);
        exit(3);
    }

    print_header(bfp);

    print_page((bsStgBmT*)PAGE(bfp), page, PAGE_SIZE);

    return 0;
}

/******************************************************************************/
#define INTS_PER_LINE 4
/* len in bytes */
print_page(bsStgBmT *bsStgBmp, int pn, int len)
{
    int index;

    assert(SBMversion != 0);

    printf("lgSqNm %d  ", bsStgBmp->lgSqNm);
    printf("xor %08x\n", bsStgBmp->xor);
    printf(" index      block   mapInt[]\n");

    index = 0;
    while ( index < SBM_INTS_PG ) {
        printf("%6d   ", pn * SBM_INTS_PG + index);
        printf("0x%06x  ", pn * BLKS_MAPPED_BY_PG + index * BLKS_MAPPED_BY_INT);

        while ( index < SBM_INTS_PG ) {
            printf(" %08x", bsStgBmp->mapInt[index]);
            index++;
            if ( index % INTS_PER_LINE == 0 ) {
                break;
            }
        }
        printf("\n");
    }
}

/*****************************************************************************/
process_index(bitFileT *bfp, long pagenum, long index)
{
    assert(SBMversion != 0);

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
}

/*****************************************************************************/
process_find_index(bitFileT *bfp, char *argv[], int argc, int optind)
{
    long index;
    uint32T pagenum;
    uint pgIdx;

    assert(SBMversion != 0);

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

    if ( argc != optind ) {
        fprintf(stderr, "too many arguments\n");
        usage();
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
}

/*****************************************************************************/
#define FORMAT_BITS_PER_BYTE 8
process_find_block(bitFileT *bfp, char *argv[], int argc, int optind)
{
    long block;
    long index;
    long pagenum;
    int bitIndex;
    uint sbmMapInt;
    char number[10];
    int numsz;
    int blkBitIndex;
    int pgIdx;

    assert(SBMversion != 0);

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

    if ( block < 0 ) {
        fprintf(stderr,
          "Invalid block number (%d), block number must be positive\n", block);
        exit(1);
    }

    pagenum = block / BLKS_MAPPED_BY_PG;
    if ( pagenum >= bfp->pages ) {
        fprintf(stderr,
          "Invalid block number (%d), the last block mapped by the SBM is %d\n",
                pagenum, SBM_INTS_PG * (bfp->pages - 1));
        exit(1);
    }

    block = rounddown(block, CLUSTER_SZ);

    index = block / BLKS_MAPPED_BY_INT;
    blkBitIndex = (block / CLUSTER_SZ) % SBM_BITS_INT;

    if ( argc != optind ) {
        fprintf(stderr, "too many arguments\n");
        usage();
    }

    if (read_page(bfp, pagenum)) {
        fprintf(stderr, "Bad read\n");
        exit(2);
    }

    print_header(bfp);

    printf(
      "blocks %d - %d (0x%x - 0x%x) are mapped by SBM map entry %d, bit %d\n",
      block, block + CLUSTER_SZ - 1, block, block + CLUSTER_SZ - 1,
      index, blkBitIndex);

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
        uint mask = 0x1;

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
}

test_sbm(bitFileT *bfp, int flags)
{
    struct dmn *dmnp;
    volT *volp;
    bitFileT *bmtBfp;
    bsStgBmT *bsStgBmp;
    uint32T sbmPgs;
    uint32T sbmBlks;
    uint32T pg;
    uint32T skipXor = 0;

    dmnp = bfp->dmn;
    assert(dmnp != NULL);
    assert(bfp->pvol != 0);
    volp = dmnp->vols[bfp->pvol];
    assert(volp != NULL);
    bmtBfp = volp->bmt;
    assert(bmtBfp != NULL);

    if ( dmnp->dmnVers >= FIRST_VALID_SBM_XOR_VERSION ) {
        sbmPgs = howmany( volp->blocks / BS_CLUSTSIZE, SBM_BITS_PG);
    } else {
        sbmPgs = howmany( volp->blocks / BS_CLUSTSIZE_V3, SBM_BITS_PG);
    }

    if ( bfp->pages > sbmPgs ) {
        fprintf(stderr, "test_sbm: bfp->pages > sbmPgs\n");
        sbmPgs = bfp->pages;
    } else if ( sbmPgs > bfp->pages ) {
        fprintf(stderr, "test_sbm: sbmPgs > bfp->pages\n");
    }

    bsStgBmp = (bsStgBmT*)malloc(sbmPgs * ADVFS_PGSZ);

    /* FIX. need to do this for RBMT and BMT */
    for ( pg = 0; pg < bmtBfp->pages; pg++ ) {
        bsMPgT *bsMPgp;
        uint cn;

        if ( read_page(bmtBfp, pg) != OK ) {
            fprintf(stderr, "test_sbm: read BMT page %d failed\n", pg);
            return 1;
        }

        bsMPgp = (bsMPgT*)bmtBfp->pgBuf;

        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            bsMCT *bsMCp = &bsMPgp->bsMCA[cn];
            bsXtntRT *bsXtntRp;
            bsShadowXtntT *bsShadowXtntp;
            bsXtraXtntRT *bsXtraXtntRp;
            bsXtntT *bsXA;
            uint xcnt;
            int ret;
            uint xc;
            uint32T startBlk;
            uint32T startPg;

            if ( find_bmtr(bsMCp, BSR_XTNTS, (char**)&bsXtntRp) == OK ) {
                xcnt = bsXtntRp->firstXtnt.xCnt;

                if ( xcnt > BMT_XTNTS ) {
                    fprintf(stderr, "test_sbm: xcnt > BMT_XTNTS\n");
                    xcnt = 3;
                }

                bsXA = bsXtntRp->firstXtnt.bsXA;
            } else if ( find_bmtr(bsMCp, BSR_SHADOW_XTNTS,
                                  (char**)&bsShadowXtntp) == OK )
            {
                xcnt = bsShadowXtntp->xCnt;

                if ( xcnt > BMT_SHADOW_XTNTS ) {
                    fprintf(stderr, "test_sbm: xcnt > BMT_SHADOW_XTNTS\n");
                    xcnt = BMT_SHADOW_XTNTS;
                }

                bsXA = bsShadowXtntp->bsXA;
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
                if ( startBlk != XTNT_TERM && startBlk != PERM_HOLE_START ) {
                    uint32T blks;

                    if ( bsXA[xc].bsPage > startPg ) {
                        blks = (bsXA[xc].bsPage - startPg) * ADVFS_PGSZ_IN_BLKS;

                        if ( fill_inmemSBM(bfp, bsStgBmp, startBlk, blks) != OK ) {
                            fprintf(stderr, "BMT pg %d, cell %d, index %d\n", pg, cn, xc);
                        }
                    }
                }

                if ( bsXA[xc].bsPage <= startPg ) {
                    uint32T startClust;
                    uint32T startPg;
                    uint firstInt;
                    uint cur_bit;

                    fprintf(stderr, "Bad BMT entry: pg %d, cell %d, entry %d:\n",  pg, cn, xc);
                    if ( (signed)bsXA[xc - 1].vdBlk > 0 ) {
                        if ( bfp->dmn->dmnVers >= FIRST_VALID_SBM_XOR_VERSION ) {
                            startClust = bsXA[xc - 1].vdBlk / BS_CLUSTSIZE;
                        } else {
                            startClust = bsXA[xc - 1].vdBlk / BS_CLUSTSIZE_V3;
                        }
                        startPg = startClust / SBM_BITS_PG;
                        cur_bit = startClust % SBM_BITS_PG;
                        firstInt = cur_bit/32;
                        cur_bit %= 32;
                        skipXor ^= 1 << cur_bit;

                        fprintf(stderr, "  skip entry %d: pg %d blk %u ",
                          xc - 1, bsXA[xc - 1].bsPage, bsXA[xc - 1].vdBlk);
                        fprintf(stderr, "(SBM pg %d, int %d, bit %d [%4x])\n",
                          startPg, firstInt, cur_bit, 1<<cur_bit);
                    } else {
                        fprintf(stderr, "       entry %d: pg %d blk %u\n",
                          xc - 1, bsXA[xc - 1].bsPage, bsXA[xc - 1].vdBlk);
                    }
                    if ( (signed)bsXA[xc].vdBlk > 0 ) {
                        if ( bfp->dmn->dmnVers >= FIRST_VALID_SBM_XOR_VERSION ) {
                            startClust = bsXA[xc].vdBlk / BS_CLUSTSIZE;
                        } else {
                            startClust = bsXA[xc].vdBlk / BS_CLUSTSIZE_V3;
                        }
                        startPg = startClust / SBM_BITS_PG;
                        cur_bit = startClust % SBM_BITS_PG;
                        firstInt = cur_bit/32;
                        cur_bit %= 32;
                        skipXor ^= 1 << cur_bit;

                        fprintf(stderr, "  skip entry %d: pg %d blk %u ",
                          xc, bsXA[xc].bsPage, bsXA[xc].vdBlk);
                        fprintf(stderr, "(SBM pg %d, int %d, bit %d [%4x])\n",
                          startPg, firstInt, cur_bit, 1<<cur_bit);
                    }

                    if ( xc < xcnt - 1 ) {
                        /* ignore bad area. move on */
                        xc++;

                        fprintf(stderr, "       entry %d: pg %d blk %u\n",
                          xc, bsXA[xc].bsPage, bsXA[xc].vdBlk);
                    }
                }
                startPg = bsXA[xc].bsPage;
                startBlk = bsXA[xc].vdBlk;
            }  /* for each extent */
        }  /* for each mcell */
    }  /* for each BMT page */

    /* compare in-mem-sbm with SBM */
    /* FIX. separate func. pass in -v flag: if -v then find BMT entries. */
    for ( pg = 0; pg < bfp->pages; pg++ ) {
        bsStgBmT *SBMpgp;
        uint index;
        uint xor = 0;

        if ( read_page(bfp, pg) != OK ) {
            fprintf(stderr, "test_sbm: read SBM page %d failed\n", pg);
            return 1;
        }

        SBMpgp = (bsStgBmT*)bfp->pgBuf;

        for ( index = 0; index < SBM_INTS_PG; index++ ) {
            if ( SBMpgp->mapInt[index] != bsStgBmp[pg].mapInt[index] ) {
                fprintf(stderr, "SBM pg %d, int %d is %x. inmem SBM is %x\n", pg, index, SBMpgp->mapInt[index], bsStgBmp[pg].mapInt[index]);
                xor ^= SBMpgp->mapInt[index] ^ bsStgBmp[pg].mapInt[index];
            }
        }

        if ( SBMpgp->xor != bsStgBmp[pg].xor ) {
            fprintf(stderr, "SBM pg %d xor checksum is %x, inmem SBM xor checksum is %x\n", pg, SBMpgp->xor, bsStgBmp[pg].xor);
            if ( SBMpgp->xor != (bsStgBmp[pg].xor ^ xor) ) {
                fprintf(stderr, "  The xor delta between these two checksums is %x\n", SBMpgp->xor ^ bsStgBmp[pg].xor);
                fprintf(stderr, "  The xor delta from the SBM vs BMT map entry differences is %x\n", xor);
                fprintf(stderr, "  Applying the SBM/BMT delta leaves an xor delta of %x\n", (bsStgBmp[pg].xor ^ xor) ^ SBMpgp->xor);
                if ( skipXor != 0 ) {
                    fprintf(stderr, "  Some BMT extents were skipped. The skip xor is %x\n", skipXor);
                }
            } else {
                fprintf(stderr, "  The difference between SBM entries and BMT data fully accounts for this.\n");
            }
        } else {
            fprintf(stderr, "The pg %d SBM checksum and the inmem SBM checksum are the same.\n", pg);
        }
    }

    exit(0);
}

/* FIX. sb sbm_remove_space(,uint32T startBlk,uint32T blks,) */
fill_inmemSBM(bitFileT *bfp,
              bsStgBmT *bsStgBmp,
              uint32T startBlk,
              uint32T blks)
{
    uint32T startClust;
    uint32T clusts;
    uint32T startPg;
    uint32T endPg;
    uint32T pg;
    uint cur_bit;
    uint endBit;
    uint set_bits;
    uint bits_to_set;
    int ret = 0;

    if ( bfp->dmn->dmnVers >= FIRST_VALID_SBM_XOR_VERSION ) {
        startClust = startBlk / BS_CLUSTSIZE;
        clusts = blks / BS_CLUSTSIZE;
        if ( clusts * BS_CLUSTSIZE != blks ) {
            fprintf(stderr, "fill_inmemSBM: clusts * BS_CLUSTSIZE != blks\n");
        }
    } else {
        startClust = startBlk / BS_CLUSTSIZE_V3;
        clusts = blks / BS_CLUSTSIZE_V3;
        if ( clusts * BS_CLUSTSIZE_V3 != blks ) {
            fprintf(stderr, "fill_inmemSBM: clusts*BS_CLUSTSIZE_V3 != blks\n");
        }
    }

    startPg = startClust / SBM_BITS_PG;
    endPg = (startClust + (clusts - 1)) / SBM_BITS_PG;

    cur_bit = startClust % SBM_BITS_PG;
    bits_to_set = clusts;

    for ( pg = startPg; pg <= endPg; pg++ ) {
        int firstInt;
        int lastInt;
        int wordBit;
        uint32T mask;
        const uint32T allSet = 0xffffffff;
        uint i;

        set_bits = MIN( bits_to_set, SBM_BITS_PG - cur_bit );
        endBit   = cur_bit + set_bits - 1;

        firstInt = cur_bit/32;
        lastInt  = endBit/32;
        wordBit  = firstInt*32;
        mask     = allSet << (cur_bit - wordBit);

        if (firstInt == lastInt) {
            mask &= allSet>>((wordBit + 31) - endBit);
            if (bsStgBmp[pg].mapInt[firstInt] & mask) {
                fprintf(stderr, "CSBT: SBM pg %d int %d: existing %x, to be set %x\n",
                  pg, firstInt, bsStgBmp[pg].mapInt[firstInt], mask);
                ret = 1;
            }
            bsStgBmp[pg].mapInt[firstInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
        } else {
            if (bsStgBmp[pg].mapInt[firstInt] & mask) {
                fprintf(stderr, "CSBT: firstint: existing %x, to be set %x\n",
                  bsStgBmp[pg].mapInt[firstInt], mask);
                ret = 1;
            }
            bsStgBmp[pg].mapInt[firstInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
    
            for (i = firstInt + 1; i < lastInt; i++) {
                if (bsStgBmp[pg].mapInt[i] != 0) {
                    fprintf(stderr, "CSBT: existing %x, to be set %x\n",
                      bsStgBmp[pg].mapInt[i], allSet);
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
                ret = 1;
            }
            bsStgBmp[pg].mapInt[lastInt] |= mask;
            bsStgBmp[pg].xor ^= mask;
        }

        cur_bit = (cur_bit + set_bits) % SBM_BITS_PG;
        bits_to_set -= set_bits;
    }

    return ret;
}
