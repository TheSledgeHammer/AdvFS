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
#pragma ident "@(#)$RCSfile: nvlogpg.c,v $ $Revision: 1.1.11.2 $ (DEC) $Date: 2006/08/30 15:25:42 $"

#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/mode.h>
#include <ufs/fs.h>
#include <assert.h>
#include <sys/param.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ftx_privates.h>

#include "vods.h"
#include "vlogpg.h"

/* private prototypes */
void usage();
static int find_LOG( bitFileT*);
static void process_summary( bitFileT*);
static void save_log( bitFileT*, char**, int, int, int);
static void process_block( bitFileT*, char**, int, int, int);
static void print_block( bitFileT*, lbnT, int, int);
static void process_all( bitFileT*, char**, int, int, int);
static void process_ends( bitFileT*, char**, int, int, int);
static void process_range( bitFileT*, char**, int, int, int);
static void process_ftxid( bitFileT*, int, int);
static void process_page( bitFileT*, int, int, int, int);
static int process_page_range( bitFileT *, int, int, int, int, int, int);
static int print_logpage( bitFileT*, int, int, int, int);
static void restore_logpage( logPgT*);
static void find_first_n_last_rec( bitFileT*, int*, int*, int*, int*);

/* print_log.c prototypes */
void print_loghdr( FILE*, logPgHdrT*);
void print_logrec( logRecHdrT*, int, int);

static char *Prog;

/***************************************************************************/
void
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s [-v | -B] <src>					print summary\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> page [rec_off [-f]]		print page or record[s]\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> {-s | -e} [-f]			print log record[s]\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> {-s | -e} pg_off [rec_off [-f]]	print page or record[s]\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> -a				print all the log\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> -R				print active log\n", Prog);
    fprintf(stderr, "%s [-v | -B] <src> -i ftxid [-f]			print active log\n", Prog);
    fprintf(stderr, "%s [-v | -B] <vol> -b block				print page\n", Prog);
    fprintf(stderr, "%s {<dmn> | <vol>} -d file				save log\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<src> = <dmn> | -V volume_name | [-F] save_file\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = [-V] volume_name | <dmn> vdi\n");

    exit(1);
}

/*
** vlogpg <opt> <src>						summary
** vlogpg <opt> <src> page					page of log
** vlogpg <opt> <src> page record_offset			one log record
** vlogpg <opt> <src> page record_offset -f			chain of records
** vlogpg <opt> <src> {-s | -e}					log record
** vlogpg <opt> <src> {-s | -e} -f				chain of records
** vlogpg <opt> <src> {-s | -e} page_offset			page of log
** vlogpg <opt> <src> {-s | -e} page_offset record_offset	one record
** vlogpg <opt> <src> {-s | -e} page_offset record_offset -f	chain of records
** vlogpg {<dmn> vdi | <vol>} -b block
** vlogpg {<dmn> | <vol>} -d save_file

** <opt> = -v | -B
** <src> = <dmn> | <vol> | [-F] file_name
** <dmn> = [-r] [-D] domain_name
** <vol> = [-V] volume_name

** to do
** vlogpg {<dmn> vdi | <vol>} -b block record_offset -f
** vlogpg -r when -e & -s are on same page, repeats the page
**short logs from new domains cause core dump
*/


int
main(argc,argv)
int argc;
char *argv[];
{
    bitFileT bf;
    bitFileT  *bfp = &bf;
    int flags = 0;
    long longnum;
    long blknum = -1;
    long pagenum = 0;
    long offset = -1;
    long start_pg;
    long start_off;
    long end_pg;
    long end_off;
    logRecAddrT crashRedo;
    int ret;
    extern int optind;

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
    bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_FTXLOG);

    ret = resolve_src(argc, argv, &optind, "rvBD:V:F:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pvol, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* Now check ret from resolve_src */
    /* At this point if the volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( find_LOG(bfp) != OK ) {
        exit(3);
    }

    if ( argc == optind ) {
        process_summary(bfp);
        /* NOTREACHED */
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
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-s") == 0 || strcmp(argv[optind], "-e") == 0 ) {
        process_ends(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-i") == 0 ) {
        long ftxid;

        optind++;

        if ( argc > optind ) {
            if ( getnum(argv[optind], &ftxid) != OK ) {
                fprintf(stderr, "Badly formed ftxid: \"%s\"\n", argv[optind]);
                exit(1);
            }
            if ( ftxid < 1 || ftxid > 0xffffffffL ) {
                fprintf(stderr, "ftxid out of range: \"%s\"\n", argv[optind]);
                exit(1);
            }
            optind++;
        }

        if ( argc > optind ) {
            if ( strcmp(argv[optind], "-f") == 0 ) {
                optind++;
                flags |= FFLG;
            } else {
                usage();
            }
        }

        if ( argc > optind ) {
            usage();
        }

        process_ftxid(bfp, (int)ftxid, flags);
        /* NOTREACHED */
    }

    if ( argc > optind + 3) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    /* get page number */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }
        optind++;

        if ( pagenum > bfp->pages || pagenum < 0 ) {
            fprintf(stderr, "Invalid LOG page number (%d). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %d.\n",
              bfp->pages - 1);
            exit(2);
        }
    }

    /* get record offset */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &offset) != OK ) {
            fprintf(stderr, "Badly formed offset number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
        if ( offset < 0 || offset > PAGE_SIZE - 1 ) {
            fprintf(stderr, "Invalid record offset (%d). ", offset);
            fprintf(stderr, "Valid offsets range from 0 to %d.\n", PAGE_SIZE-1);
            exit(2);
        }
    }

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-f") == 0 ) {
            optind++;
            flags |= FFLG;
        } else {
            usage();
        }
    }

    process_page(bfp, pagenum, offset, offset, flags);

    exit(0);
}

/***************************************************************************/
static int
find_LOG(bitFileT *bfp)
{
    int vdi;
    int ret;
    bsXtntRT *xtntrp;
    bitFileT *bmtBfp;
    int mc;
    bsMCT *mcellp;
    extern int dmnVersion;        /* declared in vods.c; set in read_rbmt() */

    assert(bfp);
    bfp->setTag = -2;
    bfp->ppage = 0;

    if ( bfp->type & FLE ) {
        bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_FTXLOG);
        bfp->pcell = BFM_FTXLOG;
        return OK;
    }

    assert(bfp->dmn);
    assert(bfp->dmn->dmnVers);

    /* The LOG primary mcell is at cell 3 for both versions. */
    mc = RBMT_PRESENT ? BFM_FTXLOG : BFM_FTXLOG_V3;

    for ( vdi = 0; vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->pvol && vdi != bfp->pvol ) {
            continue;
        }

        if ( bfp->dmn->vols[vdi] ) {
            bmtBfp = RBMT_PRESENT ? bfp->dmn->vols[vdi]->rbmt :
                                    bfp->dmn->vols[vdi]->bmt;
            assert(bmtBfp);

            if ( read_page(bmtBfp, 0) != OK ) {
                fprintf(stderr,
                 "find_LOG: Warning. read_page 0 failed for %s on volume %d.\n",
                  RBMT_PRESENT ? "RBMT" : "BMT", vdi);
                continue;
            }

            mcellp = &((bsMPgT *)PAGE(bmtBfp))->bsMCA[mc];
            if ( (signed)mcellp->tag.num != RSVD_TAGNUM_GEN(vdi, mc) ) {
                fprintf(stderr,
 "find_LOG: Warning. Wrong tag in cell %d page 0 of the %s on volume %d. Expected %d, read %d\n",
                  mc, RBMT_PRESENT ? "RBMT" : "BMT", vdi,
                  RSVD_TAGNUM_GEN(vdi, mc), (signed)mcellp->tag.num);
                /* FIX. "Tag %d is not the tag of a LOG file.\n", */
                continue;
            }

            if ( find_bmtr(mcellp, BSR_XTNTS, (char**)&xtntrp) != OK ) {
                fprintf(stderr,
 "find_TAG: Warning. No BSR_XTNTS record at cell %d page 0 of the %s on volume %d\n",
                  mc, RBMT_PRESENT ? "RBMT" : "BMT", vdi);
                /* FIX. "Can't find BSR_XTNTS record for LOG file.\n" */
                continue;
            }

            if ( xtntrp->firstXtnt.mcellCnt == 1 &&
                   xtntrp->firstXtnt.xCnt == 1 )
            {
                /* no log on this volume */
                continue;
            }

            if ( xtntrp->firstXtnt.mcellCnt != 1 )
                fprintf(stderr, "Log has more than one extent.\n");

            bfp->pcell = mc;
            bfp->pvol = vdi;
            bfp->ppage = 0;
            bfp->fileTag = RSVD_TAGNUM_GEN(vdi, mc);
            bfp->setTag = -BFM_BFSDIR;   /* -2 */

            ret = load_xtnts(bfp);

            return ret;
        }
    }

    fprintf(stderr, "Couldn't find LOG in domain %s\n", bfp->dmn->dmnName);
    return BAD_DISK_VALUE;
}

/***************************************************************************/
static void
process_summary(bitFileT *bfp)
{
    int start_pg;
    int start_off;
    int end_pg;
    int end_off;
    int pages;

    bfp->pgVol = bfp->pvol;
    bfp->pgLbn = XTNT_TERM;
    bfp->pgNum = -1;
    print_header(bfp);

    printf("The log is %d pages long.\n", bfp->pages);
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

        printf("There are %d pages of active records in the LOG.\n", pages);
    }

    printf("The first LOG record is on page %d at offset %d.\n",
      start_pg, start_off);
    printf("The last LOG record is on page %d at offset %d\n", end_pg, end_off);

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
    long blknum;
    long offset = -1;

    assert(bfp->fd);
    if ( bfp->type & FLE || bfp->type & SAV ) {
        fprintf(stderr, "Can't use -b flag on a file\n");
        usage();
    }

    if ( strlen(argv[optind]) == 2 ) {
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
    } else {
        if ( getnum(argv[optind], &blknum) != OK ) {
            fprintf(stderr, "Badly formed block number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
    }

    if ( bfp->dmn->vols[bfp->pvol]->blocks != 0 ) {
        if ( (unsigned long)blknum >= bfp->dmn->vols[bfp->pvol]->blocks ) {
            fprintf(stderr, "Invalid block number (%d). ", blknum);
            fprintf(stderr, "Valid block numbers range from 0 to %d.\n",
              bfp->dmn->vols[bfp->pvol]->blocks - ADVFS_PGSZ_IN_BLKS);
            exit(2);
        }
    }

    if ( blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS != blknum ) {
        fprintf(stderr, "LOG pages are on %d block boundaries.\n",
          ADVFS_PGSZ_IN_BLKS);
        blknum = blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%d).\n",
          ADVFS_PGSZ_IN_BLKS, blknum);
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
        if ( strcmp(argv[optind], "-f") == 0 ) {
            optind++;
            flags |= FFLG;
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
print_block(bitFileT *bfp, lbnT lbn, int offset, int flags)
{
    char page[PAGE_SIZE];
    int result;

    assert(bfp->fd >= 0);
    if ( lseek(bfp->fd, ((unsigned long)lbn) * DEV_BSIZE, SEEK_SET) == -1 ) {
        perror("print_block");
        exit(3);
    }

    result = read(bfp->fd, page, PAGE_SIZE);
    if ( result == -1 ) {
        perror("print_block");
        exit(3);
    } else if ( result != PAGE_SIZE ) {
        fprintf(stderr, "Truncated read. Read page %d returned %d.\n",
          page, result);
        exit(3);
    }

    bfp->pgVol = bfp->pvol;
    bfp->pgLbn = lbn;

    (void)print_logpage(bfp, offset, offset, 0, flags);
}

/***************************************************************************/
static void
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int pagenum;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
        process_page(bfp, pagenum, 0, PAGE_SIZE, flags);
    }

    exit(0);
}

/***************************************************************************/
static void
process_ends(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int start_pg;
    int start_off;
    int end_pg;
    int end_off;
    int page;
    int offset;
    long value;

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
        if ( strcmp(argv[optind], "-f") == 0 ) {
            optind++;
            flags |= FFLG;
            if ( argc > optind ) {
                usage();
            }
        }
    }

    /* get the offset from the start page */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &value) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }

        optind++;
        if ( value >= bfp->pages - 1 || -value >= bfp->pages - 1 ) {
            fprintf(stderr, "Invalid LOG page offset (%ld). ", value);
            fprintf(stderr, "Valid page offset is %d to %d.\n",
              -(bfp->pages - 1), bfp->pages - 1);
            exit(2);
        }

        page += value;
        if ( page < 0 ) {
            page += bfp->pages;
        } else if ( page >= bfp->pages ) {
            page -= bfp->pages;
        }

        offset = -1;
        if ( argc > optind ) {
            if ( getnum(argv[optind], &value) != OK ) {
                fprintf(stderr, "Badly formed offset number: \"%s\"\n",
                  argv[optind]);
                exit(1);
            }
            optind++;

            if ( value < 0 || value > PAGE_SIZE - 1 ) {
                fprintf(stderr, "Invalid record offset (%ld). ", value);
                fprintf(stderr, "Valid offsets range from 0 to %d.\n",
                  PAGE_SIZE - 1);
                exit(2);
            }

            offset = value;
        }
    }


    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-f") == 0 ) {
            optind++;
            flags |= FFLG;
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
    int start_pg;
    int start_off;
    int end_pg;
    int end_off;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    find_first_n_last_rec(bfp, &start_pg, &start_off, &end_pg, &end_off);

    (void)process_page_range(bfp, start_pg, start_off, end_pg, end_off,
                             0, flags);

    exit(0);
}

/***************************************************************************/
static void
process_ftxid(bitFileT *bfp, int ftxid, int flags)
{
    int start_pg;
    int start_off;
    int end_pg;
    int end_off;
    int pagenum;
    int pageoff;
    int found = 0;

    find_first_n_last_rec(bfp, &start_pg, &start_off, &end_pg, &end_off);

    found = process_page_range(bfp, start_pg, start_off, end_pg, end_off,
                               ftxid, flags);

    if ( found && !(flags & FFLG) ) {
        exit(0);
    }

    if ( !found ) {
        printf("ftxid not found in log active range. ");
    }
    printf("Begin searching inactive range.\n");

    pagenum = end_pg;
    pageoff = end_off;
    if ( start_off > 0 ) {
        end_pg = start_pg;
        end_off = start_off - 1;
    } else {
        if ( start_pg != 0 ) {
            end_pg = start_pg - 1;
        } else {
            end_pg = bfp->pages - 1;
        }
        end_off = PAGE_SIZE;
    }
    start_pg = pagenum + 1;
    if ( start_pg == bfp->pages ) {
        start_pg = 0;
    }
    start_off = 0;

    found |= process_page_range(bfp, start_pg, start_off, end_pg, end_off,
                                ftxid, flags);

    if ( found == 0 ) {
        printf("ftxid not found\n");
    }

    exit(0);
}

/***************************************************************************/
static void
process_page(bitFileT *bfp, int pgnum, int start_off, int end_off, int flags)
{
    int ftxId;
    int startPg;
    int startOff;
    int endPg;
    int endOff;

    read_page(bfp, pgnum);

    ftxId = print_logpage(bfp, start_off, end_off, 0, flags);

    if ( flags & FFLG ) {
        find_first_n_last_rec(bfp, &startPg, &startOff, &endPg, &endOff);
        if ( pgnum < bfp->pages - 1 ) {
            pgnum++;
        } else {
            pgnum = 0;
        }
        (void)process_page_range(bfp, pgnum, 0, endPg, endOff, ftxId, flags);
    }
}

/***************************************************************************/
static int
process_page_range(bitFileT *bfp, int start_pg, int start_off,
                   int end_pg, int end_off, int reqFtxId, int flags)
{
    int ftxid = 0;
    int pagenum;

    if ( start_pg == end_pg ) {
        read_page(bfp, start_pg);
        ftxid |= print_logpage(bfp, start_off, end_off, reqFtxId, flags);
    }

    if ( end_pg > start_pg ) {
        read_page(bfp, start_pg);
        ftxid |= print_logpage(bfp, start_off, PAGE_SIZE, reqFtxId, flags);

        for ( pagenum = start_pg + 1; pagenum < end_pg; pagenum++ ) {
            read_page(bfp, pagenum);
            ftxid |= print_logpage(bfp, 0, PAGE_SIZE, reqFtxId, flags);
        }

        read_page(bfp, pagenum);
        ftxid |= print_logpage(bfp, 0, end_off, reqFtxId, flags);
    }

    if ( end_pg < start_pg ) {
        read_page(bfp, start_pg);
        ftxid |= print_logpage(bfp, start_off, PAGE_SIZE, reqFtxId, flags);

        for ( pagenum = start_pg + 1; pagenum < bfp->pages; pagenum++ ) {
            read_page(bfp, pagenum);
            ftxid |= print_logpage(bfp, 0, PAGE_SIZE, reqFtxId, flags);
        }
        for ( pagenum = 0; pagenum < end_pg; pagenum++ ) {
            read_page(bfp, pagenum);
            ftxid |= print_logpage(bfp, 0, PAGE_SIZE, reqFtxId, flags);
        }

        read_page(bfp, pagenum);
        ftxid |= print_logpage(bfp, 0, end_off, reqFtxId, flags);
    }

    return ftxid;
}

/***************************************************************************/
/* returns ftxid of last record printed or 0 if no record printed
*/
static int
print_logpage( bitFileT *bfp, int start_poff, int end_poff, int reqId, int flags)
{
    logPgT *pdata = (logPgT *)bfp->pgBuf;
    int offset = 0;
    int last_offset = -1;
    int found = 0;
    int ftxid, lastftxid = -1;
    ftxDoneLRT *doneLRTp;
    int first = TRUE;

    if ( reqId == 0 ) {
        print_header(bfp);
    }

    restore_logpage(pdata);
    if ( start_poff == -1 ) {
        print_loghdr(stdout, &pdata->hdr);
        printf(SINGLE_LINE);
    }

    while ((offset > last_offset) && (offset < DATA_WORDS_PG)) {
        doneLRTp = (ftxDoneLRT*)((char *)&pdata->data[offset] +
                                 (REC_HDR_WORDS * sizeof(uint32T)));
        ftxid = doneLRTp->ftxId;
        if ( reqId == 0 ||
             reqId == ftxid && offset >= start_poff && offset <= end_poff &&
              (flags & FFLG || doneLRTp->level == 0 && doneLRTp->member == 0) )
        {
            if ( start_poff == -1 ||
                 (offset >= start_poff && offset <= end_poff) ||
                 (flags & FFLG) && (ftxid == lastftxid) )
            {
                if ( first ) {
                    if ( reqId != 0 ) {
                        print_header(bfp);
                    }
                    first = FALSE;
                } else {
                    printf(SINGLE_LINE);
                }

                print_logrec((logRecHdrT *)&pdata->data[offset],offset,flags);
                lastftxid = doneLRTp->ftxId;
                found = lastftxid;
            }
        }
        last_offset = offset;
        offset = ((logRecHdrT *)&pdata->data[offset])->nextRec.offset;
    }

    if ( start_poff == -1 ) {
        print_logtrlr(stdout, &pdata->trlr);
    }

    if ( found == 0 && reqId == 0 ) {
        printf("No log entry at offset %d\n", start_poff);
    }

    return found;
}

/***************************************************************************/
static void
restore_logpage(logPgT *pgp)
{
    int b;
    logPgBlksT *logPgBlks = (logPgBlksT *)pgp;

    for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
        logPgBlks->blks[b].lsn = pgp->trlr.lsnOverwriteVal[b - 1];
    }
}

/******************************************************************************/
static void
find_first_n_last_rec(bitFileT *bfp, int *sPg, int *sOff, int *ePg, int *eOff)
{
    int offset = 0, last_offset=-1;
    int found = 0;
    int ftxid, lastftxid = -1;
    ftxDoneLRT *doneLRTp;
    uint32T recOffset;
    logRecHdrT *recHdr;
    logPgT *pdata;
    int start_pg;
    int start_off;
    int end_pg;
    int end_off;
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

    recOffset = 0;

    do {
        if (recOffset >= DATA_WORDS_PG) {
            printf( "find_first_n_last_rec: Invalid rec offset %d on page %d\n",
                recOffset, end_pg);
            exit(2);
        }

        recHdr = (logRecHdrT *) &pdata->data[recOffset];
        recOffset += recHdr->wordCnt;

        /* FIX. test wordCnt > sizeof header(s) */
        if (recOffset == 0) {
            printf( "advfs_logger: invalid rec offset: 0x%08x\n", recOffset );
            exit(2);
        }
    } while ( !RECADDR_EQ(recHdr->nextRec, logEndOfRecords) );

    doneLRTp = (ftxDoneLRT*)((char *)recHdr + REC_HDR_WORDS * sizeof(uint32T));

    crashRedo.page = doneLRTp->crashRedo.page;
    crashRedo.offset = doneLRTp->crashRedo.offset;
    crashRedo.lsn = doneLRTp->crashRedo.lsn;

    *ePg = end_pg;
    *eOff = recOffset - recHdr->wordCnt;
    *sPg = crashRedo.page;
    *sOff = crashRedo.offset;
}

/* from ms_public.c */
logRecAddrT logEndOfRecords = { 0xffffffff, 0xffffffff, 0 };

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
            uint32T   pg,
            lsnT      *pgLsn,
            int       *invalidPg,
            int       *incompletePg,
            lsnT      *prtdLsn,
            uint32T   *prtdPg );

static void
lgr_restore_pg(logPgT *pgp);

static int
lgr_complete_pg(logPgT *pgp);

static void
lgr_repair_pg(logPgT *pgp);

static int
lgr_valid_blk(logPgT *pgp, int b);

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
    int i,
        logWrapped = 0,
        pgBad = 0,
        pg0Bad = 0,
        pgMaxBad = 0,
        searchPgs;
    uint32T lastPg = bfp->pages - 1;
    uint32T lwrPg = 0,
            uprPg = lastPg,
            midPg,
            endPg,
            pg,
            ptrdPg = -1;
    int ignore,
        pgIncomplete;
    lsnT
        ptrdLsn,
        prevLSN,
        pgLSN,
        pg0LSN, /* LSN of log page zero */
        pgMid1LSN,
        pgMid2LSN,
        pgMaxLSN; /* LSN of last log page */
    logPgT *logPgp;
    logRecHdrT *recHdr;
    uint32T recOffset;
    ftxDoneLRT* dlrp;
    logRecAddrT crashRedo;

    /*
     * Get the last page's LSN.  We need it to determine if the log
     * has wrapped (the end page number is less than the beginning page
     * number) since this effects the binary search.
     */

    ptrdLsn = nilLSN;
    if ( get_pg_lsn(bfp, lastPg, &pgMaxLSN, &pgMaxBad, &ignore, &ptrdLsn,
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
                endPg = lastPg;
                goto _finish;
            }
        }
    } else if (LSN_EQ_NIL( pg0LSN )) {
        /* The log is empty! */
        return -2;
    }

    if (pgMaxBad) {
        endPg = lastPg - 1;
        goto _finish;
    }

    logWrapped = !LSN_EQ_NIL(pgMaxLSN) && lsn_lt( pgMaxLSN, pg0LSN);

    if ((!logWrapped) && !LSN_EQ_NIL( pgMaxLSN )) {
        /* The last log pg is the log end pg */
        endPg = lastPg;
        goto _finish;
    }

    /*
     * Now that we've taken care of the easy and corner cases we use
     * the binary search to locate the log end page.
     */

    while (uprPg >= lwrPg) {
        midPg = (lwrPg + uprPg) / 2;

        if (midPg == lastPg) {
            midPg--;
        }

        ptrdLsn = nilLSN;
        assert(midPg >= 0 && midPg <= lastPg);
        if ( get_pg_lsn( bfp, midPg, &pgMid1LSN, &pgBad, &ignore,
                   &ptrdLsn, &ptrdPg) == -1 ) {
            return -3;
        }
        if (pgBad) {
            endPg = midPg - 1;
            goto _finish;
        }

        ptrdLsn = nilLSN;
        assert(midPg >= 0 && midPg <= lastPg);
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
        return -1;
    }

    logPgp = (logPgT*)bfp->pgBuf;
    if ( lgr_valid_pg(logPgp) == FALSE ) {
        return -1;
    }

    lgr_restore_pg(logPgp);

    recOffset = 0;

    /* Locate the last record in this candidate last page. */

    do {
        if (recOffset >= DATA_WORDS_PG) {
            fprintf(stderr, "invalid rec offset: 0x%08x in log\n", recOffset);
            return -1;
        }

        recHdr = (logRecHdrT *)&logPgp->data[recOffset];
        recOffset += recHdr->wordCnt;

        if ( recHdr->wordCnt == 0 ) {
            printf("invalid rec offset: 0x%08x in log\n", recOffset);
            return -1;
        }
    } while (!RECADDR_EQ( recHdr->nextRec, logEndOfRecords ));

    /* Peel out the first active log (starting) log page */

    dlrp = (ftxDoneLRT *)&logPgp->data[recOffset-recHdr->wordCnt+REC_HDR_WORDS];

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
            return -3;
        }

        if ( pgBad ) {
            /*
             * The current page was not written in it's entirety.
             * The previous page in the log must be the log end.
             */
            endPg = (pg + bfp->pages - 1) % bfp->pages;
            printf("bad page %d; truncating log\n", endPg);
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
    uint32T pg,         /* in - log page's number */
    lsnT *pgLsn,        /* out - log page's first record's LSN */
    int *invalidPg,     /* out - flag indicates if page is invalid */
    int *incompletePg,  /* out - flag indicates if page is incomplete */
    lsnT *prtdLsn,      /* in/out - thread of prev lsn across pg */
    uint32T *prtdPg     /* in/out - thread of prev pg  across pg */
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
            printf("ADVFS: caught deceipher bug, trunc log at pg %d\n",pg);
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
         !BS_UID_EQL(pgp->hdr.dmnId, pgp->trlr.dmnId))) {
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

            for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
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
    uint32T validEndRec, startBlk, endBlk;
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
                   (BS_BLKSIZE / sizeof( uint32T ));

        recHdr = (logRecHdrT *) &pgp->data[pgp->hdr.curLastRec];

        endBlk = (pgp->hdr.curLastRec + recHdr->wordCnt + PG_HDR_WORDS)
                                       /
                   (BS_BLKSIZE / sizeof( uint32T ));

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
