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
 *      Thu Nov 17 09:21:23 PST 1994
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: nvtagpg.c,v $ $Revision: 1.1.13.2 $ (DEC) $Date: 2006/08/30 15:25:43 $"

#include <stdio.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>
#include <assert.h>

#include <msfs/bs_public.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>
#include "vods.h"

#define T2P(_t) ( (_t) / BS_TD_TAGS_PG )
#define T2S(_t) ( (_t) % BS_TD_TAGS_PG )

/* private prototypes */
char * get_setname( bitFileT *, vdIndexT, uint32T, uint32T, uint32T, int );
void test_tag( bitFileT * );

/* extern prototypes */
void modify_tag( int, char *[], int, bitFileT *, int, int, int );

static char *Prog;

/******************************************************************************/
void
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s [-v] <dmn>			print root TAG summary\n", Prog);
    fprintf(stderr, "%s [-v] <src> {page | -a}		print root TAG page or all\n", Prog);
    fprintf(stderr, "%s [-v] <dmn> <set>		print set TAG summary\n", Prog);
    fprintf(stderr, "%s [-v] <dmn> ROOT <set>		print root TAG entry\n", Prog);
    fprintf(stderr, "%s [-v] <dmn> <set> {page | -a}	print fileset TAG page or all\n", Prog);
    fprintf(stderr, "%s [-v] <dmn> <set> <file>		print fileset TAG entry\n", Prog);
    fprintf(stderr, "%s [-v] <dmn> <set> -f             print free list\n", Prog);
    fprintf(stderr, "%s <dmn> -X                        test ROOT tag file\n", Prog);
    fprintf(stderr, "%s <dmn> <set> -X          test fileset tag file\n", Prog);
    fprintf(stderr, "%s [-v] <vol> -b block		print TAG page\n", Prog);
    fprintf(stderr, "%s <dmn> [<set>] -d sav_file	save root or fileset TAG file\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<src> = <dmn> | [-F] file\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = [-V] volume_name | <dmn> vdi\n");
    fprintf(stderr, "<set> = [-S] set_name | -T set_tag\n");
    fprintf(stderr, "<file> = [-F] file_name | [-t] file_tag\n");

    exit(1);
}

/*
** vtagpg [-v] <dmn>			root TAG page 0 w/ fileset name
** vtagpg [-v] <src> {[page] | -a}	root TAG, default page 0
** vtagpg [-v] <dmn> <set>		one entry in root TAG

** vtagpg [-v] <dmn> <set> page		one page of fileset tag
** vtagpg [-v] <dmn> <set> <file>	one entry in fileset tag

** vtagpg [-v] {<vol> | <dmn> vdi} -b block

** vtagpg <dmn> -d file
** vtagpg <dmn> <set> -d file

** <src> = <dmn> | [-F] file
** <dmn> = [-r] [-D] domain_name
** <vol> = [-V] volume_name
** <set> = [-S] set_name | -t set_tag
** <file> = file_name | [-t] file_tag

** vtagpg [-v] <vol> [page | -a]
** vtagpg [-v] <vol> <set> [page]
** vtagpg [-v] <vol> <set> <file>
** vtagpg <vol> [<set>] -d file

** to do
** vtagpg [-v] [-F] file [-t] tag
** nvtagpg dmn fset -s vn pn cn
*/


/******************************************************************************/
void
main(argc,argv)
int argc;
char *argv[];
{
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    bitFileT tagBf;
    bitFileT *tagBfp = &tagBf;
    int flags = 0;
    long blknum = -1;
    long pagenum = 0;
    long longnum;
    int ret;
    extern int optind;
    int rootTag = FALSE;

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
    bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_BFSDIR);

    ret = resolve_src(argc, argv, &optind, "rvD:V:F:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( argc > optind &&
         bfp->type & VOL && strcmp(argv[optind], "-b" ) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pvol, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( find_TAG(bfp) != OK ) {
        exit(3);
    }
    *tagBfp = *bfp;

    if ( argc == optind ) {
        /* If only one page long and type == DMN then print filesets. */
        if ( bfp->pages == 1 ) {
            if ( bfp->type & DMN /*|| bfp->type == VOL*/ ) {
                flags |= PSET;
            }
        }
        process_summary(bfp);
        exit(0);
    }

    /* display all the root TAG file */
    if ( strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags);
    }

    /* Test the tag file */
    if ( strcmp(argv[optind], "-X") == 0 ) {
        test_tag( tagBfp );
        /* NOTREACHED */
    }

    /* report on the free tag slots */
    if ( strcmp(argv[optind], "-f") == 0 ) {
        process_free(bfp, argv, argc, optind, flags);
    }

    /* dump the root tagdir to a file */
    if ( strcmp(argv[optind], "-d") == 0 ) {
        save_tag(bfp, argv, argc, optind, flags);
    }

    /* If the next argument is a number, it is a root TAG page number. */
    if ( getnum(argv[optind], &pagenum) == OK ) {
        optind++;
        if ( argc == optind ) {
            test_page_num(bfp, pagenum, flags);
            /* Display root TAG page. */
            print_page(bfp, pagenum, flags);
            exit(0);
        }

        usage();
    }

    /* The arg wasn't a page number. It must be "ROOT" or a set. */
    if ( strcmp(argv[optind], "ROOT") == 0 ) {
        optind++;
        rootTag = TRUE;
        if ( argc == optind ) {
            usage();
        }
    }

    ret = get_set(tagBfp, argv, argc, &optind, bfp);
    if ( ret != OK ) {
        /* If no such set, get_set has already printed a message. */
        if ( ret == BAD_SYNTAX ) {
            usage();
        } else {
            exit(ret);
        }
        /* NOTREACHED */
    }

    /* bfp is now the set. */

    if ( argc == optind ) {
        /* No page specified. Display entry for set tag. */
        if ( !rootTag ) {
            /* Print a summary of the set TAG file. */
            process_summary(bfp);
        } else {
            /* Display root TAG file entry for set. */
            print_tag(tagBfp, bfp->fileTag, flags);
            exit(1);
        }
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-d") == 0 ) {
        /* dump the file_set tagdir to a file */
        save_tag(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-a") == 0 ) {
        /* display all the file_set tagdir */
        process_all(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* Test the BMT file */
    if ( strcmp(argv[optind], "-X") == 0 ) {
        test_tag( bfp );
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-f") == 0 ) {
        /* report on the free tag slots */
        process_free(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* If the next argument is a number, it is a set TAG page number. */
    if ( getnum(argv[optind], &pagenum) == OK ) {
        optind++;
        if ( argc == optind ) {
            test_page_num(bfp, pagenum, flags);
            /* Display the set TAG page. */
            print_page(bfp, pagenum, flags);
            exit(0);
        }

        usage();
        /* NOTREACHED */
    }

    *tagBfp = *bfp;
    /* tagBfp now represents the set TAG file, bfp will soon be the file. */
    ret = get_file(tagBfp, argv, argc, &optind, bfp);
    if ( ret != OK ) {
        if ( ret == BAD_SYNTAX ) {
            usage();
        } else {
            exit(ret);
        }
        /* NOTREACHED */
    }

    if ( argc == optind ) {
        /* Display set TAG entry. */
        print_tag(tagBfp, bfp->fileTag, flags);
        exit(1);
    }

    usage();
    /* NOTREACHED */
}

/******************************************************************************/
test_page_num(bitFileT *bfp, long pagenum, int flg)
{
    int initPages;

    if ( pagenum < 0 || pagenum >= bfp->pages ) {
        fprintf(stderr, "Invalid tag page number (%ld). ", pagenum);
        fprintf(stderr, "Valid page numbers are 0 through %d.\n",
          bfp->pages - 1);
        exit(1);
    }

    get_max_tag_pages(bfp, &initPages);

    if ( pagenum >= initPages && !(flg & VFLG) ) {
        fprintf(stderr,
          "Page %d is beyond the last initialized tag page (%d).\n",
          pagenum, initPages);
        exit(1);
    }
}

/******************************************************************************/
test_tag_num(bitFileT *bfp, int tagNum, int flg)
{
    int initPages;
    int pagenum;
    bfTagT tag;

    tag.num = tagNum;
    tag.seq = 0;
    pagenum = TAGTOPG(&tag);

    if ( pagenum >= bfp->pages ) {
        fprintf(stderr, "Invalid tag number (%d) would be on tag page %d.\n",
          tagNum, pagenum);
        fprintf(stderr, "Valid page numbers are 0 - %d (tag 0 - %d).\n",
          bfp->pages - 1, bfp->pages * BS_TD_TAGS_PG - 1);
        exit(1);
    }

    get_max_tag_pages(bfp, &initPages);

    if ( pagenum >= initPages && !(flg & VFLG) ) {
        fprintf(stderr,
          "Tag %d on page %d is beyond the last initialized tag page (%d).\n",
          tagNum, pagenum, initPages);
        exit(1);
    }
}

/******************************************************************************/
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
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long blknum;

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

    if ( blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS != blknum ) {
        fprintf(stderr, "Tag pages are on %d block boundaries.\n",
          ADVFS_PGSZ_IN_BLKS);
        blknum = blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%u).\n",
          ADVFS_PGSZ_IN_BLKS, blknum);
    }

    bfp->pgLbn = blknum;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    print_block(bfp, bfp->fd, blknum, flags);

    exit(0);
}

/******************************************************************************/
print_block(bitFileT *bfp, int fd, lbnT lbn, int vflg)
{
    bsTDirPgT xpdata;
    bsTDirPgT *pdata = &xpdata;
    int result;

    if ( lseek(fd, ((unsigned long)lbn) * DEV_BSIZE, L_SET) == -1 ) {
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

    bfp->pgVol = bfp->pvol;
    bfp->pgLbn = lbn;
    print_tagpage(bfp, pdata, lbn, vflg);
}

/******************************************************************************/
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int pagenum;
    int pages;
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
process_summary(bitFileT *bfp)
{
    int pagenum;
    int pages;
    int totalAlloc = 0;
    int totalDead = 0;
    int root = bfp->fileTag < 0;

    if ( bfp->pages == 0 )  {
        fprintf(stderr, "No pages in this TAG file.\n");
        exit(1);
    }

    printf(DOUBLE_LINE);
    assert(bfp->dmn);
    printf("DOMAIN \"%s\"  ", bfp->dmn->dmnName);
    if ( root ) {
        printf("root TAG file\n");
    } else {
        if ( bfp->setName ) {
            printf("FILESET \"%s\" TAG file\n", bfp->setName);
        } else {
            printf("FILESET tag %d TAG file\n", bfp->fileTag);
        }
    }
    printf(SINGLE_LINE);

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
            fprintf(stderr, "Failed to read page %d of tag file.\n", pagenum);
            continue;
        }

        bsTDirPgp = (bsTDirPgT*)bfp->pgBuf;
        bsTDirPgHdrp = &bsTDirPgp->tpPgHdr;
        totalAlloc += bsTDirPgHdrp->numAllocTMaps;
        totalDead += bsTDirPgHdrp->numDeadTMaps;
    }
    printf("Total pages %d,  Initialized pages %d\n", bfp->pages, pages);
    printf("Total tags %d,  ", pages * BS_TD_TAGS_PG);
    printf("Total allocated tags %d,  ", totalAlloc);
    printf("Total dead tags %d\n", totalDead);

    if ( root ) {
        print_fileset_names(bfp);
    }

    exit(0);
}

/******************************************************************************/
print_fileset_names(bitFileT *bfp)
{
    bsTDirPgT *bsTDirPgP;
    int slot;
    int ret;
    int cnt = 0;
    int i;

    ret = read_page(bfp, 0);
    if ( ret != OK ) {
        fprintf(stderr, "Failed to read page 0 of root TAG file.\n");
        exit(1);
    }

    bsTDirPgP = (bsTDirPgT*)bfp->pgBuf;

    for (slot = 0; slot < BS_TD_TAGS_PG; slot++) {
        if (bsTDirPgP->tMapA[slot].tmSeqNo & BS_TD_IN_USE) {
            vdIndexT vdI = bsTDirPgP->tMapA[slot].tmVdIndex;
            uint32T pn = bsTDirPgP->tMapA[slot].tmBfMCId.page;
            uint32T cn = bsTDirPgP->tMapA[slot].tmBfMCId.cell;
            char *setName = get_setname(bfp, vdI, pn, cn, slot, FALSE);

            if ( cnt >= 20 ) {
                printf("...\n");
                /* Print only the first 20 filesets. */
                break;
            }

            if ( setName == NULL ) {
                continue;
            }

            printf("%s%*s %d  ",
              setName, BS_SET_NAME_SZ + 8 - strlen(setName), "tag", slot);

            printf("primary mcell %d %d %d\n", vdI, pn, cn);

            cnt++;
        }
    }
}

/******************************************************************************/
process_free(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int pagenum;
    int pages;
    int ret;
    bsTDirPgT *bsTDirPgp;
    bsTDirPgHdrT *bsTDirPgHdrp;
    int nextPage;

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
    printf("tag %-8d ", 0);
    printf("seqNo %-5d  ", bsTDirPgp->tMapA[0].tmSeqNo);
    printf("tmFreeListHead %d  ", bsTDirPgp->tMapA[0].tmFreeListHead);
    printf("tmUninitPg %d\n", bsTDirPgp->tMapA[0].tmUninitPg);
    printf("\n");

    while ( nextPage != 0 ) {
        if ( nextPage > pages ) {
            fprintf(stderr, "Bad free chain.");
            fprintf(stderr, "Page %d is beyond the last page (%d)\n",
              nextPage, pages - 1);
            exit(2);
        }

        ret = read_page(bfp, nextPage - 1);
        if ( ret != OK ) {
            fprintf(stderr, "Failed to read page %d of tag file.\n",
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

/******************************************************************************/
print_page(bitFileT *bfp, int pgnum, int flg)
{
    bsTDirPgT *pdata;
    int ret;

    ret = read_page(bfp, pgnum);
    if ( ret != OK ) {
        fprintf(stderr, "Failed to read page %d of tag file.\n", pgnum);
        exit(1);
    }

    pdata = (bsTDirPgT*)bfp->pgBuf;

    print_tagpage(bfp, pdata, bfp->pgLbn, flg);

    return;
}

/******************************************************************************/
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

    print_tag(tagBfp, bfp->fileTag, flags);

    exit(0);
}

/******************************************************************************/
process_tag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long tagNum;
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

    if ( getnum(argv[optind], &tagNum) != OK ) {
        fprintf(stderr, "Badly formed tag number: \"%s\"\n", argv[optind]);
        exit(1);
    }

    test_tag_num(bfp, tagNum, flags);

    print_tag(bfp, (int)tagNum, flags);
    /* NOTREACHED */
}

/******************************************************************************/
print_tag(bitFileT *bfp, int tagNum, int flags)
{
    bfTagT tag;
    int pagenum;
    bsTDirPgT *pdata;
    lbnT lbn = bfp->pgLbn;
    int flg = flags;
    int slot;
    int pages;

    tag.num = tagNum;
    tag.seq = 0;
    pagenum = TAGTOPG(&tag);

    get_max_tag_pages(bfp, &pages);
    if ( pagenum >= pages && !(flg & VFLG) ) {
        fprintf(stderr, "Page %d is beyond the last good tag page.\n", pagenum);
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
print_tagpage(bitFileT *bfp, bsTDirPgT *pdata, lbnT lbn, int flags)
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
print_taghdr(bitFileT *bfp, bsTDirPgHdrT *pdata, int flags)
{
    print_header(bfp);

    if ( flags & VFLG ) {
        printf("currPage %d\n", pdata->currPage);
        /* Reading at a given block (-b) leaves pgNum = -1 */
        if ( bfp->pgNum != (unsigned)-1 && bfp->pgNum != pdata->currPage ) {
            printf("ERROR. currPage != file page.\n");
        }
    } else {
        if ( bfp->pgNum != (unsigned)-1 && bfp->pgNum != pdata->currPage ) {
            printf("ERROR. currPage != file page. currPage = %d\n",
              pdata->currPage);
        }
    }

    printf("numAllocTMaps %d  ", pdata->numAllocTMaps);
    printf("numDeadTMaps %d  ", pdata->numDeadTMaps);
    printf("nextFreePage %d  ", pdata->nextFreePage);
    printf("nextFreeMap %d  ", pdata->nextFreeMap);
    if ( flags & VFLG ) {
        printf("padding %d\n\n", pdata->padding);
    } else {
        printf("\n\n");
    }
}

/******************************************************************************/
/*
** Tag entries are a union of 3 uses. The first tag on the first page
** has a head of the free list and the first un initilaized tag page.
** The remainder of the tags are either on the free list or in use or dead.
*/
print_tag_entry(bitFileT *bfp, bsTDirPgT *pdata, int slot, int flg)
{
    int tagNum;
    uint pgNum;

    if ( bfp->pgNum != (unsigned)-1 ) {
        /* We are reading a page of a tag file. */
        /* We trust the file page number more than the data at the page. */
        pgNum = bfp->pgNum;
        tagNum = MKTAGNUM(pgNum, slot);
    } else {
        /* We are reading a block from disk (-b argument). */
        /* We have only the data on the page (currPage) to tell the page num. */
        pgNum = pdata->tpPgHdr.currPage;
        tagNum = MKTAGNUM(pgNum, slot);
        if ( tagNum < 0 ) {
            /* Use this as a flag that we can't find a reasonable tag number. */
            tagNum = -1;
        }
    }

    if ( slot == 0 && pgNum == 0 && !(flg & PSET) ) {
        /* This is the first tag on the first page of the tag file. */

        printf("tMapA[0] ");

        printf("tmFreeListHead %d  ", pdata->tMapA[0].tmFreeListHead);
        printf("tmUninitPg %d\n", pdata->tMapA[0].tmUninitPg);

    } else if (pdata->tMapA[slot].tmSeqNo & BS_TD_IN_USE) {
        vdIndexT vdI = pdata->tMapA[slot].tmVdIndex;
        uint32T pn = pdata->tMapA[slot].tmBfMCId.page;
        uint32T cn = pdata->tMapA[slot].tmBfMCId.cell;

        printf("tMapA[%d] ", slot);
        if ( slot < 10 ) {
            printf("   ");
        } else if ( slot < 100 ) {
            printf("  ");
        } else if ( slot < 1000 ) {
            printf(" ");
        }

        if ( tagNum != -1 ) {
            if ( flg & PSET ) {
                printf("tag %-2d ", tagNum);
            } else {
                printf("tag %-8d ", tagNum);
            }
        } else {
            printf("tag ??? ");
        }

        printf("seqNo %-5d  ", pdata->tMapA[slot].tmSeqNo & ~BS_TD_IN_USE);
        printf("primary mcell (vol,page,cell)  %d %d %d   ", vdI, pn, cn);

        if ( flg & PSET ) {
            char *setName = get_setname(bfp, vdI, pn, cn, tagNum, TRUE);
            if ( setName ) {
                printf("%s", setName);
            }
        }
        printf("\n");

    } else if (pdata->tMapA[slot].tmSeqNo == BS_TD_DEAD_SLOT) {
        printf("tMapA[%d]", slot );
        if ( !(flg & PSET) ) {
            printf("  ");
        }

        if ( slot < 10 ) {
            printf("  ");
        } else if ( slot < 100 ) {
            printf(" ");
        } else if ( slot < 1000 ) {
            printf(" ");
        }

        printf("EXPIRED     nextMap %d\n", pdata->tMapA[slot].tmNextMap);

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
            printf("tag %-2d ", tagNum);
        } else {
            printf("tag %-8d ", tagNum);
        }

        printf("seqNo %-5d  ", pdata->tMapA[slot].tmSeqNo);
        printf("tmNextMap %d\n", pdata->tMapA[slot].tmNextMap);
    }
}

/******************************************************************************/
/*    bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};	ms_privates.c */
char *
get_setname(bitFileT *bfp, vdIndexT vdI, uint32T pn, uint32T cn, uint32T tag,
              int prtFlg)
{
    bitFileT *bmtbfp;
    bsMCT *mcellp;
    bsMRT *recordp;
    char *data;
    int ret;

    assert(bfp);
    assert(bfp->dmn);
    if ( bfp->dmn->vols[vdI] == NULL ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
             "\n  Tag %d primary mcell volume (%d) not in domain.\n", tag, vdI);
        }

        return NULL;
    }

    bmtbfp = bfp->dmn->vols[vdI]->bmt;
    assert(bmtbfp);
    ret = read_page(bmtbfp, pn);
    if ( ret != OK ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
              "\n  Read BMT page %d on volume %d failed.\n", pn, vdI);
        }

        return NULL;
    }

    mcellp = &((bsMPgT*)bmtbfp->pgBuf)->bsMCA[cn];
    if ( mcellp->bfSetTag.num != -BFM_BFSDIR || mcellp->tag.num != tag ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
      "\n  Wrong primary mcell (%d %d %d). Set,tag is %d,%d, should be -2,%d\n",
              vdI, pn, cn, mcellp->bfSetTag.num,  mcellp->tag.num, tag);
        }

        return NULL;
    }

    /* FIX. use find_rec */
    recordp = (bsMRT *)mcellp->bsMR0;
    if ( recordp->type != BSR_ATTR ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
              "\n  Primary mcell first record type is %d, should be BSR_ATTR\n",
              recordp->type);
        }
        return NULL;
    }

    data = ((char *)recordp) + sizeof(bsMRT);
    if ( ((bsBfAttrT *)data)->state != BSRA_VALID ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr, "\n  set not BSRA_VALID\n");
        }

        return NULL;
    }

    /* FIX. test vdI */
    vdI = mcellp->nextVdIndex;
    pn = mcellp->nextMCId.page;
    cn = mcellp->nextMCId.cell;
    ret = read_page(bmtbfp, pn);
    if ( ret != OK ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
              "\n  Read BMT page %d on volume %d failed.\n", pn, vdI);
        }

        return NULL;
    }

    mcellp = &((bsMPgT*)bmtbfp->pgBuf)->bsMCA[cn];
    if ( mcellp->bfSetTag.num != -BFM_BFSDIR || mcellp->tag.num != tag ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
    "\n  Wrong secondary mcell (%d %d %d). Set,tag is %d,%d, should be -2,%d\n",
              vdI, pn, cn, mcellp->bfSetTag.num,  mcellp->tag.num, tag);
        }

        return NULL;
    }

    /* FIX. use find_rec */
    recordp = (bsMRT *)mcellp->bsMR0;
    if ( recordp->type != BSR_BFS_ATTR ) {
        if ( prtFlg ) {
            fflush(stdout);
            fprintf(stderr,
        "\n  Secondary mcell first record type is %d, should be BSR_BFS_ATTR\n",
              recordp->type);
        }

        return NULL;
    }

    data = ((char *)recordp) + sizeof(bsMRT);

    return ((bsBfSetAttrT *)data)->setName;
}

void test_all_tags( bitFileT *, uint ** );
void test_tag_file( bitFileT *, int, uint ** );

void
test_tag( bitFileT *bfp )
{
    ushort vdi;
    uint **dmnMeta;

    if ( !(bfp->type & DMN) ) {
        fprintf(stderr, "Test saved files not supported.\n");
        exit(1);
    }

    dmnMeta = malloc( BS_MAX_VDI * sizeof(uint *) );
    if ( dmnMeta == NULL ) {
        perror("malloc");
        exit(1);
    }
    bzero( dmnMeta, BS_MAX_VDI * sizeof(uint *) );

    for ( vdi = 1; vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->dmn->vols[vdi] == NULL ) {
            continue;
        }

        dmnMeta[vdi] = malloc( bfp->dmn->vols[vdi]->bmt->pages * sizeof(uint) );
        if ( dmnMeta[vdi] == NULL ) {
            perror("malloc");
            exit(1);
        }
        bzero( dmnMeta[vdi], bfp->dmn->vols[vdi]->bmt->pages * sizeof(uint) );
    }

    if ( bfp->fileTag > 0 ) {
        uint fileSeq = 0;
        test_tag_file( bfp, fileSeq, dmnMeta );
        exit(0);
    }

    test_all_tags( bfp, dmnMeta );

    exit(0);
}

int set_dmnMeta( ushort, uint, uint, uint ** );
int test_mcell( bitFileT *, int, int, int, int, int, ushort, uint, uint );

void
test_all_tags( bitFileT *rootTagBfP, uint **dmnMeta )
{
    bsTDirPgT *rootPg;
    bitFileT tagBf;
    bitFileT *tagBfP = &tagBf;
    uint pg;
    uint rootPgs;
    int ret;

    if ( rootTagBfP->pages == 0 ) {
        printf("no pages in root tag!!!\n");
        exit(0);
    }

    ret = get_max_tag_pages( rootTagBfP, &rootPgs );
    if ( ret != OK ) {
        fprintf(stderr, "test_all_tags: get_max_tag_pages failed\n");
        exit(2);
    }

    for ( pg = 0; pg < rootPgs; pg++ ) {
        uint entry;

        ret = read_page( rootTagBfP, pg );
        if ( ret != OK ) {
            fprintf(stderr, "test_all_tags: read_page failed\n");
            exit(1);
        }

        rootPg = (bsTDirPgT *)rootTagBfP->pgBuf;

        for ( entry = 0; entry < BS_TD_TAGS_PG; entry++ ) {
            ushort vol;
            uint page, cell;
            uint fileTag;
            int fileSeq;

            if ( !(rootPg->tMapA[entry].tm_u.tm_s2.seqNo & BS_TD_IN_USE) ) {
                continue;
            }

            vol = rootPg->tMapA[entry].tm_u.tm_s3.vdIndex;
            page = rootPg->tMapA[entry].tm_u.tm_s3.bfMCId.page;
            cell = rootPg->tMapA[entry].tm_u.tm_s3.bfMCId.cell;
            fileSeq = rootPg->tMapA[entry].tm_u.tm_s3.seqNo;

            fileTag = pg * BS_TD_TAGS_PG + entry;

            newbf( rootTagBfP, tagBfP );
            tagBfP->fileTag = -2;

            /* test_mcell nneds the set tag from tagBfP to test the mcells. */
            ret = test_mcell( tagBfP, 0, 0, 0, fileTag, fileSeq,
                              vol, page, cell );
            if ( ret == OK ) {
                ret = set_dmnMeta( vol, page, cell, dmnMeta );
                if ( ret != OK ) {
                    fprintf(stderr,
                 "BMT mcell %d %d %d is pointed to by another tagfile entry.\n",
                      vol, page, cell );
                }
            }

            ret = get_file_by_tag( rootTagBfP, fileTag, tagBfP );
            if ( ret != OK ) {
                fprintf(stderr, "test_all_tags: get_file_by_tag failed\n");
                return;
            }

            /* get_file_by_tag clobbers setTag */
            tagBfP->setTag = -BFM_BFSDIR;  /* -2 */

            printf("Check fileset tag %d\n", fileTag);
            test_tag_file( tagBfP, fileSeq, dmnMeta );

            free(tagBfP->xmap.xtnt);
        }
    }
}

void
test_tag_file( bitFileT *tagBfP, int setSeq, uint **dmnMeta )
{
    bsTDirPgT *tagPg;
    uint pg;
    uint tagPgs;
    int fileTag;
    int ret;
    bsBfSetAttrT *bsBfSetAttrp;
    int origTag = 0;
    int origSeq = 0;

    if ( tagBfP->pages == 0 ) {
        printf("no pages in tagfile!!!\n");
        exit(0);
    }

    ret = get_max_tag_pages( tagBfP, &tagPgs );
    if ( ret != OK ) {
        fprintf(stderr, "test_all_tags: get_max_tag_pages failed\n");
        exit(2);
    }

    ret = find_rec( tagBfP, BSR_BFS_ATTR, (char**)&bsBfSetAttrp );
    if ( ret != OK ) {
        fprintf(stderr, "test_all_tags: find_rec failed\n");
    } else {
        origTag = bsBfSetAttrp->origSetTag.num;
        origSeq = bsBfSetAttrp->origSetTag.seq;
    }

    for ( pg = 0; pg < tagPgs; pg++ ) {
        uint entry;

        ret = read_page( tagBfP, pg );
        if ( ret != OK ) {
            fprintf(stderr, "test_tag_files: read_page failed\n");
            exit(1);
        }

        tagPg = (bsTDirPgT *)tagBfP->pgBuf;

        for ( entry = 0; entry < BS_TD_TAGS_PG; entry++ ) {
            ushort vol;
            uint page, cell;
            uint fileTag;
            int fileSeq;

            if ( !(tagPg->tMapA[entry].tm_u.tm_s2.seqNo & BS_TD_IN_USE) ) {
                continue;
            }

            vol = tagPg->tMapA[entry].tm_u.tm_s3.vdIndex;
            page = tagPg->tMapA[entry].tm_u.tm_s3.bfMCId.page;
            cell = tagPg->tMapA[entry].tm_u.tm_s3.bfMCId.cell;
            fileSeq = tagPg->tMapA[entry].tm_u.tm_s3.seqNo;

            fileTag = pg * BS_TD_TAGS_PG + entry;

            ret = test_mcell( tagBfP, setSeq, origTag, origSeq,
                              fileTag, fileSeq, vol, page, cell );
            if ( ret == OK ) {
                ret = set_dmnMeta( vol, page, cell, dmnMeta );
                if ( ret != OK ) {
                    fprintf(stderr,
                 "BMT mcell %d %d %d is pointed to by another tagfile entry.\n",
                      vol, page, cell );
                }
            }
        }

        fprintf(stderr, CLEAR_LINE);
        fprintf(stderr, " Testing tags on tagfile page %d of %d\r", pg, tagPgs);
    }

    fprintf(stderr, CLEAR_LINE);
}

int
test_mcell( bitFileT *setBfp, int setSeq,
            int origSetTag, int origSetSeq, int fileTag, int fileSeq,
            ushort vol, uint page, uint cell )
{
    bitFileT *bmtBfp;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    bsMRT *bsMRp;
    int ret;
    int retval = 0;

    if ( setBfp->dmn->vols[vol] == NULL ) {
        fprintf(stderr,
          "Bad set %d tagfile entry (tag) %d: Bad vdIndex %d %d %d\n",
          setBfp->fileTag, fileTag, vol, page, cell );
        return 1;
    }

    bmtBfp = setBfp->dmn->vols[vol]->bmt;

    if ( page >= bmtBfp->pages ) {
        fprintf(stderr,
          "Bad set %d tagfile entry (tag) %d: Bad page %d %d %d\n",
          setBfp->fileTag, fileTag, vol, page, cell );
        return 1;
    }

    if ( cell >= BSPG_CELLS ) {
        fprintf(stderr,
          "Bad set %d tagfile entry (tag) %d: Bad cell %d %d %d\n",
          setBfp->fileTag, fileTag, vol, page, cell );
        return 1;
    }

    ret = read_page( bmtBfp, page );
    if ( ret != OK ) {
        fprintf(stderr, "test_mcell: read_page failed\n");
        exit(1);
    }

    bsMPgp = (bsMPgT *)bmtBfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[cell];

    if ( bsMCp->bfSetTag.num != setBfp->fileTag ||
         bsMCp->bfSetTag.seq != setSeq )
    {
        if ( bsMCp->bfSetTag.num != origSetTag ||
             bsMCp->bfSetTag.seq != origSetSeq )
        {
            fprintf(stderr,
            "Bad set %d.%x tagfile entry %d => cell %d %d %d: bfSetTag %d.%x\n",
              setBfp->fileTag, setSeq, fileTag, vol, page, cell,
              bsMCp->bfSetTag.num, bsMCp->bfSetTag.seq );
        }
        /*
        ** even if this is a valid orig file return error to prevent
        ** call to set_dmnMeta. This file will be counted by the
        ** orig set.
        */
        retval = 2;
    }

    if ( bsMCp->tag.num != fileTag || bsMCp->tag.seq != fileSeq ) {
        fprintf(stderr,
          "Bad set %d tagfile entry %d.%x => cell %d %d %d: tag %d.%x\n",
          setBfp->fileTag, fileTag, fileSeq, vol, page, cell,
          bsMCp->tag.num, bsMCp->tag.seq );
        retval = 1;
    }

    bsMRp = (bsMRT *)bsMCp->bsMR0;

    if ( bsMRp->type != BSR_ATTR ) {
        fprintf(stderr,
     "Bad set %d tagfile entry %d => cell %d %d %d: not a primary mcell\n",
          setBfp->fileTag, fileTag, vol, page, cell );
        retval = 1;
    }

    return retval;
}

int
set_dmnMeta( ushort vol, uint page, uint cell, uint **dmnMeta )
{
    if ( dmnMeta[vol][page] & (1 << cell) ) {
        return 1;
    }

    dmnMeta[vol][page] |= 1 << cell;

    return 0;
}
