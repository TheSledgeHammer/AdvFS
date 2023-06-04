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
 *      Thu Nov 17 09:06:33 PST 1994
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vfilepg.c,v $ $Revision: 1.1.11.1 $ (DEC) $Date: 2006/03/17 04:18:12 $"

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

/* private prototypes */
void get_format(char *argv[], int argc, int *optind, int *flags);

static char *Prog;

/*****************************************************************************/
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s <dmn> <set> <file> [page [-f d]]	print a page\n", Prog);
    fprintf(stderr, "%s <dmn> <set> <file> -a [-f d]		print all pages\n", Prog);
    fprintf(stderr, "%s <vol> -b block [-f d]			print a block\n", Prog);
    fprintf(stderr, "%s <dmn> <set> <file> -d sav_file		save the file\n", Prog);
    fprintf(stderr, "%s [-F] sav_file [page [-f d] | -a]	print saved file\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = [-V] volume_name | <dmn> vdi\n");
    fprintf(stderr, "<set> = [-S] set_name | -t set_tag\n");
    fprintf(stderr, "<file> = file_path | [-t] file_tag\n");

    exit(1);
}


/*
** vfilepg <dmn> <set> <file> [page [-f d]]
** vfilepg <dmn> <set> <file> -a [-f d]
** vfilepg sav_file [{page | -a} [-f d]]
** vfilepg <vol> -b blk
** vfilepg <dmn> <set> <file> -d sav_file

** <vol> = [-r] [-D] domain vdi | [-V] volume
** <dmn> = [-r] [-D] domain | [-V] volume
** <set> = fileset | -t set_tag
** <file> = file_path | [-t] file_tag

** To do:
** vfilepg <vol> -b blk [#blks]
** vfilepg ... [-f {c|s|i|l|a|d}]] 		char|short|int|long|ascci|dir
*/

/*****************************************************************************/
main(argc,argv)
int argc;
char *argv[];
{
    int ret;
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    int flags = 0;
    long pagenum = 0;
    long longnum;
    char *fset = NULL;
    char *fname = NULL;

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

    ret = resolve_src(argc, argv, &optind, "rvD:V:F:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pvol, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( bfp->type != FLE ) {
        if ( argc < optind + 2) {
            usage();
        }

        ret = get_set_n_file(bfp, argv, argc, &optind);
        if ( ret != OK ) {
            if ( ret == BAD_SYNTAX ) {
                usage();
            } else {
                exit(1);
            }
        }

        assert(bfp->pvol != 0);
        assert(bfp->ppage != -1);
        assert(bfp->pcell != -1);
    }

    /* dump the file to a file */
    if ( argc > optind && strcmp(argv[optind], "-d") == 0 ) {
        save_file(bfp, argv, argc, optind, flags);
    }

    /* display all the file */
    if ( argc > optind && strcmp(argv[optind], "-a") == 0 ) {
        optind++;
        if ( argc > optind && strcmp(argv[optind], "-f") == 0 ) {
            get_format(argv, argc, &optind, &flags);
        }

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            usage();
        }

        for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
            process_page(bfp, pagenum, fset, fname, flags & (DFLG | VFLG));
        }
        if ( test_for_frag(bfp) == TRUE )
            process_page(bfp, pagenum, fset, fname, flags & (DFLG | VFLG));

        exit(0);
    }

    /* get page number */
    if ( argc > optind && isdigit(*argv[optind]) ) {
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }

        if ( pagenum < 0 ||
             !test_for_frag(bfp) && pagenum >= bfp->pages ||
             pagenum > bfp->pages )
        {
            fprintf(stderr, "Invalid page number (%d). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %d.\n",
              test_for_frag(bfp) ? bfp->pages : bfp->pages - 1);
            exit(1);
        }
        optind++;
    }

    if ( argc > optind && strcmp(argv[optind], "-f") == 0 ) {
        get_format(argv, argc, &optind, &flags);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    process_page(bfp, pagenum, fset, fname, flags & (DFLG | VFLG));

    exit(0);
}

/*****************************************************************************/
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long blknum;

    assert(bfp->fd);
    if ( bfp->type & FLE || bfp->type & SAV ) {
        fprintf(stderr, "Can't use -b flag on a file\n");
        usage();
    }

    assert(bfp->fd);
    if ( strlen(argv[optind]) == 2 ) {		/* -b # */
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
    } else {					/* -b# */
        if ( getnum(argv[optind], &blknum) != OK ) {
            fprintf(stderr, "Badly formed block number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
    }

    if ( bfp->dmn->vols[bfp->pvol]->blocks != 0 ) {
        if ( (unsigned long)blknum >= bfp->dmn->vols[bfp->pvol]->blocks ) {
            fprintf(stderr, "Invalid block number (%u). ", blknum);
            fprintf(stderr, "Valid block numbers range from 0 to %u.\n",
              bfp->dmn->vols[bfp->pvol]->blocks - 16);
            exit(2);
        }
    }

    if ( argc > optind && strcmp(argv[optind], "-f") == 0 ) {
        get_format(argv, argc, &optind, &flags);
        if ( (blknum / ADVFS_PGSZ_IN_BLKS) * ADVFS_PGSZ_IN_BLKS != blknum ) {
            fprintf(stderr, "The directory format requires the block number to be on a page boundary.\n");
            blknum = blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS;
            fprintf(stderr, "Block number changed to next lower multiple of 16 (%d).\n", blknum);
        }
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    bfp->pgLbn = blknum;
    print_block(bfp, flags);
    exit(0);
}

/*****************************************************************************/
int
print_block(bitFileT *bfp, int flags)
{
    ssize_t read_ret;
    char *block;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    /* malloc so that buffer is long alligned */
    /* FIX. use bfp->pg_buf */
    block = malloc(PAGE_SIZE);
    if ( block == NULL ) {
        perror("print_block");
        return ENOMEM;
    }

    if ( lseek(bfp->fd, (off_t)bfp->pgLbn * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "lseek to block %u failed", bfp->pgLbn);
        perror(errstr);
        return 1;
    }

    if ( flags & DFLG ) {
        read_ret = read(bfp->fd, block, (size_t)PAGE_SIZE);
    } else {
        read_ret = read(bfp->fd, block, (size_t)DEV_BSIZE);
    }

    if ( flags & DFLG && read_ret != PAGE_SIZE ||
         !(flags & DFLG) && read_ret != DEV_BSIZE )
    {
        if ( read_ret == (ssize_t)-1 ) {
            perror("print_block: read failed");
        } else {
            fprintf(stderr, "read returned %d expected %d\n",
              read_ret, DEV_BSIZE);
        }
        return 2;
    }

    bfp->pgVol = bfp->pvol;
    print_header(bfp);

    if ( flags & DFLG ) {
        print_dirpage(block, flags & VFLG);
    } else {
        print_page(block, bfp->pgLbn * DEV_BSIZE, DEV_BSIZE);
    }

    return 0;
}

/*****************************************************************************/
/*from bs_bitfile_sets.h */
/* number of pages per group */
#define BF_FRAG_GRP_PGS 16

/* bytes per frag slot */
#define BF_FRAG_SLOT_BYTES 1024

/* bytes per pg */
#define BF_FRAG_PG_BYTES 8192

/* slots per pg */
#define BF_FRAG_PG_SLOTS (BF_FRAG_PG_BYTES / BF_FRAG_SLOT_BYTES)

/* slots per group */
#define BF_FRAG_GRP_SLOTS \
    (BF_FRAG_GRP_PGS * BF_FRAG_PG_BYTES / BF_FRAG_SLOT_BYTES)

/* max slots (2^31-1 to allow 0xffffffff as the end of slot number) */
#define BF_FRAG_MAX_SLOTS 0xfffffffe

/* max groups */
#define BF_FRAG_MAX_GRPS (BF_FRAG_MAX_SLOTS / BF_FRAG_GRP_SLOTS)

/* max pages per frag bf */
#define BF_FRAG_MAX_PGS (BF_FRAG_MAX_GRPS * BF_FRAG_GRP_PGS)

#define FRAG2PG( frag ) ((frag) / BF_FRAG_PG_SLOTS)
#define FRAG2SLOT( frag ) ((frag) % BF_FRAG_PG_SLOTS)
#define FRAG2GRP( frag ) (( frag ) / BF_FRAG_GRP_SLOTS * BF_FRAG_GRP_PGS)

#define FRAGS_PER_GRP( fragType ) ((fragType == BF_FRAG_ANY) ? \
            (BF_FRAG_GRP_SLOTS - 1) : (BF_FRAG_GRP_SLOTS - 1) / (fragType))


save_file(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile;
    statT *statp;
    int dumpFd;
    statT fs_stat;
    int ret;
    char *bufp;

    optind++;
    if ( flags & VFLG ) {
        /* FIX. should we just ignore the flag? */
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

    if ( find_rec(bfp, BMTR_FS_STAT, (char**)&statp) != OK ) {
        fprintf(stderr, "can't find stat, frag not written.\n");
        exit(5);
    }

    /* statp is unaligned! */
    bcopy(statp, &fs_stat, sizeof(fs_stat));
    statp = &fs_stat;
    printf("frag type %d, frag slot %d\n",
      statp->fragId.type, statp->fragId.frag);
    if ( statp->fragId.type != 0 ) {

        if ( statp->st_size % 8192 > statp->fragId.frag * 1024 )
            fprintf(stderr, "File size modulo 1 page (%d) won't fit in %dK frag.\n", statp->st_size % 8192, statp->fragId.frag);

        if ( statp->fragPageOffset != bfp->pages )
            fprintf(stderr, "Frag fragPageOffset (%d) is not the last page (%d).\n", statp->fragPageOffset, bfp->pages);
        
        dumpFd = open(dfile, O_RDWR | O_APPEND);
        if ( dumpFd == -1 ) {
            fprintf(stderr, "open failed, frag not written.\n");
            perror("open");
            exit(5);
        }

        bufp = malloc(statp->fragId.frag * 1024);
        if ( bufp == NULL ) {
            perror("save_file");
            exit(4);
        }

        ret = read_frag(bfp, statp->fragId, bufp);
        if ( ret != OK )
            exit(ret);

        write(dumpFd, bufp, statp->st_size % 8192);
    }

    exit(0);
}

/*****************************************************************************/
int
read_frag(bitFileT *bfp, bfFragIdT fragId, char *bufp)
{
    int frag_bytes, resid;
    bfTagT tag;
    char *src;
    int ret;
    bitFileT fragBf;

    ret = open_frag(bfp, &fragBf);
    if ( ret != OK ) {
        return ret;
    }

    ret = read_page(&fragBf, FRAG2PG(fragId.frag));
    if ( ret != OK ) {
        fprintf(stderr, "Error reading frag id %d on frag page %d.\n",
          fragId.frag, FRAG2PG(fragId.frag));
        return ret;
    }

    frag_bytes = fragId.type * 1024;
    resid = frag_bytes - (8192 - (FRAG2SLOT(fragId.frag) * 1024));
    if (resid <= 0) {
        src = fragBf.pgBuf + (FRAG2SLOT(fragId.frag) * 1024);
        bcopy(src, bufp, frag_bytes);
    } else {
        src = fragBf.pgBuf + (FRAG2SLOT(fragId.frag) * 1024);
        bcopy(src, bufp, frag_bytes - resid);
        ret = read_page(&fragBf, FRAG2PG(fragId.frag) +1);
        if ( ret != OK ) {
            fprintf(stderr, "Error reading frag page %d, frag %d.\n",
              FRAG2PG(fragId.frag) +1, fragId.frag);
            exit(1);
        }
        bcopy(fragBf.pgBuf, bufp + frag_bytes - resid, resid);
    }
    return OK;
}

/*****************************************************************************/
int
open_frag(bitFileT *bfp, bitFileT *fragBfp)
{
    int frag_bytes, resid;
    bfTagT tag;
    char *src;
    int ret;
    bitFileT setBf;
    bitFileT *setBfp = &setBf;
    bitFileT rootBf;

    newbf(bfp, &rootBf);
    newbf(bfp, &setBf);
    newbf(bfp, fragBfp);
        
    ret = find_TAG(&rootBf);
    if ( ret != OK )
    {
        fprintf(stderr, "Couldn't find the root TAG file\n");
        return ret;
    }

    ret = get_file_by_tag(&rootBf, bfp->setTag, setBfp);
    if ( ret != OK ) {
        fprintf(stderr,
          "read_frag: Could not find TAG file for fileset tag %d\n", tag.num);
        return ret;
    }

    /* tag 1 is the frag file */
    ret = get_file_by_tag(setBfp, 1, fragBfp);
    if ( ret != OK ) {
        fprintf(stderr,
          "read_frag: Could not find frag file in fileset tag %d.\n",
          setBfp->setTag);
        return ret;
    }

    return OK;
}

/*****************************************************************************/
/* if one past the last page, look at the frag */

int
process_page(bitFileT *bfp, int page, char *fset, char *fname, int flg)
{
    int i;

    if ( page == bfp->pages ) {
        bfp->pgNum = page;
        process_frag(bfp, flg);
        /* NOT REACHED */
    }

    if (read_page(bfp, page)) {
        printf("no page %d in this file\n", page);
        exit(0);
    }

    print_header(bfp);

    if ( flg & DFLG ) {
        print_dirpage(PAGE(bfp), flg & VFLG);
    } else {
        print_page(PAGE(bfp), page * PAGE_SIZE, PAGE_SIZE);
    }

    return 0;
}

/*****************************************************************************/
process_frag(bitFileT *bfp, int flg)
{
    bitFileT fragBf;
    statT *statp;
    statT fs_stat;
    int frag_bytes, resid;
    int ret;
    ssize_t read_ret;
    char *src;

    if ( bfp->type & FLE ) {
        char *block;

        if ( lseek(bfp->fd, (off_t)bfp->pgNum * PAGE_SIZE, SEEK_SET) == -1 ) {
            sprintf(stderr, "lseek to block %u failed", bfp->pgLbn);
            perror("process_frag");
            return 1;
        }

        block = malloc(PAGE_SIZE);
        if ( block == NULL ) {
            perror("process_frag");
            exit(4);
        }

        read_ret = read(bfp->fd, block, (size_t)PAGE_SIZE);
        if ( read_ret == (ssize_t)-1 ) {
            perror("process_frag: read failed");
            exit(2);
        }

        print_header(bfp);

        print_page(block, (long)bfp->pgNum * PAGE_SIZE, (int)read_ret);

        exit(0);
    }

    if ( find_rec(bfp, BMTR_FS_STAT, (char**)&statp) != OK ) {
        fprintf(stderr, "can't find stat, frag not written.\n");
        exit(5);
    }

    /* statp is unaligned! */
    bcopy(statp, &fs_stat, sizeof(fs_stat));
    statp = &fs_stat;
    printf("frag type %d, frag slot %d\n",
      statp->fragId.type, statp->fragId.frag);

    if ( statp->fragId.type == 0 ) {
        fprintf(stderr, "No frag page. Valid page range from 0 to %d.\n",
          bfp->pages - 1);
        exit(1);
    }

    if ( statp->fragId.type != 0 ) {

        if ( statp->st_size % 8192 > statp->fragId.frag * 1024 )
            fprintf(stderr, "File size modulo 1 page (%d) won't fit in %dK frag.\n", statp->st_size % 8192, statp->fragId.frag);

        if ( statp->fragPageOffset != bfp->pages )
            fprintf(stderr, "Frag fragPageOffset (%d) is not the last page (%d).\n", statp->fragPageOffset, bfp->pages);

        if ( statp->fragId.type != 0 ) {
            ret = open_frag(bfp, &fragBf);
            if ( ret != OK ) {
                return ret;
            }
        }

        ret = read_page(&fragBf, FRAG2PG(statp->fragId.frag));
        if ( ret != OK ) {
            fprintf(stderr, "Error reading frag id %d on frag page %d.\n",
              statp->fragId.frag, FRAG2PG(statp->fragId.frag));
            return ret;
        }

        frag_bytes = statp->fragId.type * 1024;
        resid = frag_bytes - (8192 - (FRAG2SLOT(statp->fragId.frag) * 1024));
        /* FIX. only print st_size bytes. if VFLG print all */
        if (resid <= 0) {
            src = fragBf.pgBuf + (FRAG2SLOT(statp->fragId.frag) * 1024);

            print_header(&fragBf);

            print_page(src, 0, frag_bytes);
        } else {
            src = fragBf.pgBuf + (FRAG2SLOT(statp->fragId.frag) * 1024);

            print_header(&fragBf);

            print_page(src, 0, frag_bytes - resid);

            ret = read_page(&fragBf, FRAG2PG(statp->fragId.frag) +1);
            if ( ret != OK ) {
                fprintf(stderr, "Error reading frag page %d, frag %d.\n",
                  FRAG2PG(statp->fragId.frag) +1, statp->fragId.frag);
                exit(1);
            }

            print_header(&fragBf);

            print_page(fragBf.pgBuf, frag_bytes - resid, resid);
        }

    }
    exit(0);
}

/*****************************************************************************/
int
test_for_frag(bitFileT *bfp)
{
    int ret;
    statT fs_stat;
    statT *statp;

    if ( bfp->type & FLE ) {
        struct stat  statbuf;

        ret = fstat(bfp->fd, &statbuf);
        if ( ret == -1 ) {
            perror("test_for_frag");
            return FALSE;
        }

        if ( statbuf.st_size != bfp->pages * PAGE_SIZE )
            return TRUE;
        else
            return FALSE;
    }

    if ( find_rec(bfp, BMTR_FS_STAT, (char**)&statp) != OK ) {
        fprintf(stderr, "can't find stat.\n");
        return FALSE;
    }

    /* statp is unaligned! */
    bcopy(statp, &fs_stat, sizeof(fs_stat));
    statp = &fs_stat;

    if ( statp->fragId.type != 0 )
        return TRUE;
    else
        return FALSE;
}

/*****************************************************************************/
void
get_format(char *argv[], int argc, int *optind, int *flags)
{
    (*optind)++;
    if ( *optind == argc ) {
        fprintf(stderr, "Missing format after -f flag\n");
        usage();
    }

    if ( strlen(argv[*optind]) != 1 ) {
        fprintf(stderr, "Bad format after -f flag\n");
        usage();
    }

    switch ( argv[*optind][0] ) {
      case 'a':   /* print page as ascii */
        *flags |= AFLG;
        break;
      case 'c':   /* print page as bytes (default) */
        *flags |= CFLG;
        break;
      case 's':   /* print page as shorts */
        *flags |= SFLG;
        break;
      case 'i':   /* print page as integers */
        *flags |= IFLG;
        break;
      case 'l':   /* print page as longs */
        *flags |= LFLG;
        break;
      case 'd':   /* format the page as a directory page */
        *flags |= DFLG;
        break;
      default:
        fprintf(stderr, "Bad format after -f flag\n");
        usage();
    }

    (*optind)++;
}

/*****************************************************************************/
#define BPL 16		/* bytes of data per line */

print_page(uchar_t *page, int offset, int len)
{
    int lines = (len + BPL - 1) / BPL;
    int ln;
    uchar_t *line;
    uchar_t *cp;

    for ( ln = 0; ln < lines; ln++) {
        line = &page[ln * BPL];
        printf("%06x   ", offset + (int)line - (int)page);
        for ( cp = line; cp < &page[ln * BPL + BPL]; cp++ ) {
            printf("%02x ", *cp);
        }

        printf("   ");
        for ( cp = line; cp < &page[ln * BPL + BPL]; cp++ ) {
            if (isprint(*cp)) {
                printf("%c", *cp);
            } else {
                printf(".");
            }
        }

        printf("\n");
    }
}

/*****************************************************************************/
/* insert_seq */
/* lastEntry_offset */
/* p = (char *)begin_page + pageSz * BS_BLKSIZE - sizeof(dirRec) */
print_dirpage(uchar_t *page, int vflg)
{
    fs_dir_entry  *dir_ent_p;
    dirRec *dirRecp;
    int i;
    char fname[FS_DIR_MAX_NAME + 1];

    dir_ent_p = (fs_dir_entry*)(page);

    if ( vflg )
    {
          printf("      fs_dir_bs_tag_num\n");
          printf("      .    fs_dir_size\n");
          printf("      .    .   fs_dir_namecount\n");
          printf("      .    .   .  fs_dir_name_string                           tag\n");
    }
    else
    {
        printf("    tag  name\n");
    }

    while ( (uchar_t*)dir_ent_p < &page[PAGE_SIZE] ) {
        if ( dir_ent_p->fs_dir_header.fs_dir_namecount > FS_DIR_MAX_NAME ) {
            printf("dir err\n");
            exit(1);
        }
        strncpy(fname, dir_ent_p->fs_dir_name_string,
          dir_ent_p->fs_dir_header.fs_dir_namecount);
        fname[dir_ent_p->fs_dir_header.fs_dir_namecount] = '\0';

        if ( vflg ) {
            printf("%7d  ", dir_ent_p->fs_dir_header.fs_dir_bs_tag_num);
            printf("%3d  ", dir_ent_p->fs_dir_header.fs_dir_size);
            printf("%2d  ", dir_ent_p->fs_dir_header.fs_dir_namecount);
            printf("\"%s\"  ", fname);
            for ( i = 41 - strlen(dir_ent_p->fs_dir_name_string); i > 0; i-- )
            {
                printf(" ");
            }
            printf("(%04x.%x)\n",
              GETTAGP(dir_ent_p)->num, GETTAGP(dir_ent_p)->seq);
            if ( dir_ent_p->fs_dir_header.fs_dir_bs_tag_num != 0 &&
                 dir_ent_p->fs_dir_header.fs_dir_bs_tag_num != 
                 GETTAGP(dir_ent_p)->num ) {
                printf("fs_dir_bs_tag_num (%d) doesn't match GETTAGP(dir_ent_p)->num (%d)\n",
                  dir_ent_p->fs_dir_header.fs_dir_bs_tag_num,
                  GETTAGP(dir_ent_p)->num);
            }
        } else {
            if ( dir_ent_p->fs_dir_header.fs_dir_bs_tag_num != 0 ) {
                printf("%7d  %s\n",
                  dir_ent_p->fs_dir_header.fs_dir_bs_tag_num,
                  dir_ent_p->fs_dir_name_string);
                if ( dir_ent_p->fs_dir_header.fs_dir_bs_tag_num != 
                     GETTAGP(dir_ent_p)->num ) {
                    printf("fs_dir_bs_tag_num (%d) doesn't match GETTAGP(dir_ent_p)->num (%d)\n",
                      dir_ent_p->fs_dir_header.fs_dir_bs_tag_num,
                      GETTAGP(dir_ent_p)->num);
                }
            }
        }

        if ( dir_ent_p->fs_dir_header.fs_dir_size == 0 )
            break;

        dir_ent_p = (fs_dir_entry*)((char*)dir_ent_p +
                                    dir_ent_p->fs_dir_header.fs_dir_size);
    }
    if ( vflg ) {
        dirRecp = (dirRec *)((char *)page + ADVFS_PGSZ - sizeof(dirRec));
        printf("pageType %d, largestFreeSpace %d, lastEntry_offset %d\n",
          dirRecp->pageType, dirRecp->largestFreeSpace,
          dirRecp->lastEntry_offset);
    }
}
