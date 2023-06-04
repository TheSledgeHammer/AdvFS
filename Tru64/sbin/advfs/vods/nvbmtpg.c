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
#pragma ident "@(#)$RCSfile: nvbmtpg.c,v $ $Revision: 1.1.13.4 $ (DEC) $Date: 2006/08/30 15:25:41 $"

#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
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

/* private prototypes */
int find_BMT(bitFileT*, int);
int print_chain(bitFileT *, vdIndexT, uint32T, uint32T, int *, int);
static int scan_cell_for_block(bitFileT*, int, int, int, long, int, int);
static int scan_cell_for_frag(bitFileT*, int, int, int, long, int, int);
static int scan_cell_for_tag(bitFileT*, int, int, int, long, int, int);

/* from fs_dir.h */
/* largest page number that is supported (0x7fffffff) */
#define max_page 0x7fffffffL

static char *Prog;
extern int dmnVersion;        /* declared in vods.c; set in read_rbmt() */

/*****************************************************************************/
void
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s [-R] [-v] <dmn> [-f]			print summary, all volumes\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <src> [-f]			print summary\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <src> page [-f]		print page\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <src> page mcell [-c]		print mcell\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <src> -a			print all\n", Prog);
    fprintf(stderr, "%s [-R] [-v] {<dmn> | <src>} -l		print ddl\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <dmn> <set> [<file>] [-c]	find primary mcell\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <src> -s b block [-c]		search for extent block\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <dmn> <set> -s f frag [-c]	search for frag\n", Prog);
    fprintf(stderr, "%s [-R] [-v] {<vol> | <dmn> [<set>]} -s t tag [-c]	search for tag\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <vol> -b blk [-f]		print page\n", Prog);
    fprintf(stderr, "%s [-R] [-v] <vol> -b blk mcell [-c]	print mcell\n", Prog);
    fprintf(stderr, "%s [-R] <vol> -d sav_file			save BMT\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<src> = <vol> | [-F] sav_file\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = [-V] volume_name | <dmn> vdi\n");
    fprintf(stderr, "<set> = [-S] fileset | -T set_tag\n");
    fprintf(stderr, "<file> = [-F] file_path | [-t] file_tag\n");

    exit(1);
}

/*
** vbmtpg [-v] {<dmn> | <vol>} [-f]
** vbmtpg [-v] <vol> page [{-f | mcell [-c]}]
** vbmtpg [-v] <vol> -a
** vbmtpg [-v] <dmn> <set> [<file>] [-c]
** vbmtpg [-v] <vol> -s b blk
** vbmtpg [-v] <dmn> <set> -s f frag
** vbmtpg [-v] <vol> -b blk [mcell]
** vbmtpg <vol> -d sav_file
** vbmtpg [-v] [-F] sav_file [-f]
** vbmtpg [-v] [-F] sav_file page [{-f | mcell}]
** vbmtpg [-v] [-F] sav_file -a

** vbmtpg [-v] <vol> <set> [<file>] [-c]
** vbmtpg [-v] <vol> <set> -s f frag

** <dmn> = [-r] [-D] domain
** <vol> = [-V] volume | <dmn> vdi
** <set> = [-S] fileset | -t set_tag
** <file> = file_path | [-t] file_tag

** to do
** use -v instead of -f for free cells in summary
** vbmtpg [-v] <vol> -b blk -f
** <dmn> = -V volume [-V volume ...]
** <vol> = [-F] sav_dir vdi
** vbmtpg [-D] domain [-S] file_set .tags/<tag>			including M-6
** vbmtpg [-D] domain [-t]{-6 | M-6}
** vbmtpg <src> -X						test bmt
** vbmtpg [-v] <vol> -l						deferred delete list
*/

#define RBMT_VERSION(version) ( version >= FIRST_RBMT_VERSION )

main(argc, argv)
int argc;
char *argv[];
{
    int ret;
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    int flags = 0;
    uint32T pagenum = 0;
    uint32T mcellnum = -1;
    long longnum;
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

    /* Set tag to reserved so that if it's a saved file we can check the */
    /* lenght in resolve_fle. Default to -6. */
    bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_RBMT);

    ret = resolve_src(argc, argv, &optind, "RrvD:V:F:", bfp, &flags);

    /* "-b" must work even on non AdvFS volumes. */
    if ( argc > optind &&
         bfp->type & VOL && strcmp(argv[optind], "-b" ) == 0 ) 
    {
        /* process_block needs bfp->dmn, bfp->pvol, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }


    /* Again check ret from resolve_name() */
    /* At this point if the volume serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( bfp->type != FLE ) {
        assert(bfp->dmn);
        if ( !RBMT_VERSION(bfp->dmn->dmnVers) && (flags & RBMT) ) {
            fprintf(stderr, "This domain does not have a RBMT.  ");
            fprintf(stderr, "Don't use -R to view the BMT file.\n");
            exit(1);
        }

        if ( find_BMT(bfp, flags) != OK ) {
            fprintf(stderr, "find_BMT failed\n");
            exit(1);
        }
    } else {
        find_n_set_vdi(bfp);
    }

    /*
    ** At this point if bfp->type & FLE then fd set, dmn is not malloced.
    ** If bfp->type & (DMN | VOL) then dmn, vols[] are set.
    ** If bfp->type & VOL then bfp->pvol, ppage, pcell are set.
    */

    /* if no other arguments, print a summary for BMT on one volume */
    if ( argc == optind || strcmp(argv[optind], "-f") == 0 ) {
        process_summary(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* dump the RBMT or BMT to a file */
    if ( strcmp(argv[optind], "-d") == 0 ) {
        save_bmt(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }

    /* display all the bmt */
    if ( strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "-l") == 0 ) {
        process_ddl(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    /* search for block or frag */
    if ( strcmp(argv[optind], "-s") == 0 ) {
        bfp->setTag = 0;
        search(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }

    /* At this point we have a domain and expect a set or */
    /* we have a volume and expect a page */
    if ( strcmp(argv[optind], "-S") == 0 ) {
        optind++;
        process_file(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }
    
    /* get page number, if it's not a number it must be a set name */
    if ( getnum(argv[optind], &longnum) != OK ) {
        process_file(bfp, argv, argc, optind, flags & VFLG);
        /* NOTREACHED */
    }

    process_page(bfp, longnum, argv, argc, optind, flags);
    /* NOTREACHED */
}

/******************************************************************************/
/* Given bfp->pvol, this routine loads the extents for the selected (R)BMT
**
** Input: bfp->pvol - volume on which to find the BMT
** Input: bfp->dmn->vols[] filled in
** Output: fileTag, setTag (vol 1 defaults for FLE & vol == 0)
** OutPut: ppage, pcell
** Return: 0 if all went OK.
** Return: 1 if volume does not exist in domain.
*/
int
find_BMT(bitFileT *bfp, int flags)
{
    bsVdAttrT *bsVdAttrp;
    int ret;
    vdIndexT vol = bfp->pvol;

    bfp->setTag = -2;
    bfp->ppage = 0;

    if ( bfp->pvol == 0 ) {
        /* we haven't selected a volume. default to vdi 1 */
        bfp->fileTag = RSVD_TAGNUM_GEN(1, bfp->pcell);
        bfp->pcell = 0;
        return OK;
    }

    assert(!(bfp->type & FLE));
    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->dmnVers);

    if ( RBMT_VERSION(bfp->dmn->dmnVers) ) {
        bfp->pcell = flags & RBMT ? BFM_RBMT : BFM_BMT;
    } else {
        if ( flags & RBMT ) {
            fprintf(stderr,
              "This is not a V4 domain. It does not have a RBMT.\n");
            exit(2);
        }
        bfp->pcell = BFM_BMT_V3;
    }

    bfp->fileTag = RSVD_TAGNUM_GEN(bfp->pvol, bfp->pcell);

    if ( bfp->dmn->vols[bfp->pvol] ) {
        ret = load_xtnts(bfp);
        /* The extents for the (R)BMT are already loaded in */
        /* bfp->dmn->vols[]->(r)bmt. load_xtnts() will just copy them. */
        /* This can't fail. */
        assert(ret == OK);
        return OK;
    }

    return 1;
}

/******************************************************************************/
/* Print a summary of the RBMT or the BMT on one volume or all the volumes
** in the domain. The summary of the RBMT prints the number of pages and
** the number of free mcells. The summary of the BMT prints the number of
** pages and the number of extents used to describe the pages. The summary
** of a BMT file in a V3 domain also prints the number of free mcells on
** page 0 of the BMT. The -f flag will display the total free mcells in
** the BMT (for a suammry of the BMT).
**
** Input: bfp->dmn
**        bfp->pvol - specifies volume to summarize. 0 means all volumes
** Output: none
** Return: does not return
*/
process_summary(bitFileT *bfp, char *argv[], int argc, int optind, int flgs)
{
    if ( argc > optind +1 ) {
        fprintf(stderr, "Too many arguments\n");
        usage();
    }

    if ( argc > optind && strcmp(argv[optind], "-f") == 0 ) {
        flgs |= FFLG;
    }

    if ( flgs & RBMT ) {
        if ( flgs & FFLG ) {
            fprintf(stderr, "-f flag ignored for RBMT summary\n");
        }
        print_RBMT_summary(bfp, flgs);
    } else {
        print_BMT_summary(bfp, flgs);
    }

    exit(0);
}

/******************************************************************************/
/* Prints a summary of the RBMT on a volume (or saved file) or all the volumes.
** The summary gives the number of pages in the RBMT and the number of free
** mcells.
**
** Input: bfp->pvol - volume to summarize, 0 means all volumes
** Input: bfp->dmn->vols[] filled in if DMN
** Output: none
** Return: does not return
*/
/* TODO: get_next_mcell checks cell pointer. don't do it here */
print_RBMT_summary(bitFileT *bfp, int flgs)
{
    int ret;
    vdIndexT vdi;
    vdIndexT vn;
    int extents;
    bsXtntRT *bsXtntRecp;
    int cellCnt;
    int freeCells;
    int stop = FALSE;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    vdIndexT prevVn;
    uint32T prevPn;
    uint32T prevCn;
    bfMCIdT mcid;

    if ( bfp->type & FLE ) {

        ret = read_page(bfp, 0);
        if ( ret != OK ) {
            fprintf(stderr, "print_RBMT_summary: read_page failed\n");
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;
        dmnVersion = bsMPgp->megaVersion;
        bsMCp = &bsMPgp->bsMCA[BFM_BMT];
        if ( (signed)bsMCp->tag.num == 0 ) {
            fprintf(stderr, "This is a BMT file. Don't use -R to view the BMT.\n");
            exit(2);
        }
    } else {
        assert(bfp->dmn);
        assert(RBMT_VERSION(bfp->dmn->dmnVers));
    }

    for ( vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
            bfp->ppage = 0;
            bfp->pcell = BFM_RBMT;
            vdi = bfp->pvol;
        } else {
            bfp->fileTag = RSVD_TAGNUM_GEN(vdi, BFM_RBMT);
            if ( bfp->type & VOL ) {
                /* if pvol set the first time thru (bfp->fd == -1),
                 * then only display summary for one volume
                 */
                stop = TRUE;
                vdi = bfp->pvol;
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pvol = vdi;

            /* call find_BMT() to load BMT's xtnt maps */
            ret = find_BMT(bfp, flgs);
            /* just point to dmn RBMT extents. no possibility of error. */
            assert(ret == OK );

            /* The DMN & VOL case haven't read the first page yet. */
            if ( read_page(bfp, 0) != OK ) {
                if ( bfp->type & FLE )
                    fprintf(stderr, "Read error on page 0 of BMT\n");
                continue;
            }
        }

        print_header(bfp);

        if ( bfp->pages == 1 ) {
            printf("There is 1 page in the RBMT ");
        } else {
            printf("There are %d pages in the RBMT ",
                bfp->pages);
        }
        if ( bfp->type & FLE ) {
            printf("in this file.\n");
        } else {
            printf("on this volume.\n");
        }

        if ( read_page(bfp, bfp->pages - 1) != OK ) {
            fprintf(stderr, "read_page failed. RBMT on vol %d, pg %d\n",
              vdi, bfp->pages - 1);
            exit(2);
        }
        freeCells = ((bsMPgT *)bfp->pgBuf)->freeMcellCnt;

        if ( freeCells == 1 ) {
            printf("There is 1 free mcell in the RBMT on this volume.\n");
        } else {
            printf("There are %d free mcells in the RBMT on this volume.\n",
                   freeCells);
        }
    }

    return;
}

/******************************************************************************/
/* Prints a summary of the BMT on a volume (or saved file) or all the volumes.
** The summary gives the number of pages in the BMT and the number of extents
** and mcell used to describe the BMT. The number of extents is not given if
** bfp is a saved file from a V4 domain. In that case the extents describing
** the BMT is in another file. Fo a V3 domain, the number of free mcells
in page 0 is reported. If the -f option is set, report the total number
** of free mcells in the BMT.
**
** Input: bfp->pvol - volume to summarize, 0 means all volumes
** Input: flgs:FFLG - print total number of BMT free mcells
** Input: fflg:V4DMN - bfp is from a V4 domain
** Input: bfp->dmn->vols[] filled in if DMN
** Output: none
** Return: does not return
*/
/* TODO: get_next_mcell checks cell pointer. don't do it here */
print_BMT_summary(bitFileT *bfp, int flgs)
{
    int ret;
    vdIndexT vdi;
    vdIndexT vn;
    int extents;
    bsXtraXtntRT *bsXtraXtntRp;
    int cellCnt;
    int freeCells;
    bitFileT *bmtp;                     /* can point to BMT or RBMT */
    int stop = FALSE;
    int bmtidx;
    int rbmt_present;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    vdIndexT prevVn;
    uint32T prevPn;
    uint32T prevCn;
    bfMCIdT mcid;

    if ( bfp->type & FLE ) {

        ret = read_page(bfp, 0);
        if ( ret != OK ) {
            fprintf(stderr, "print_BMT_summary: read_page failed\n");
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;
        dmnVersion = bsMPgp->megaVersion;
        /*FIX. test valid range. Is this already done in open_vol? */
        if ( bsMPgp->megaVersion >= FIRST_RBMT_VERSION ) {
            rbmt_present = 1;
            bsMCp = &bsMPgp->bsMCA[0];
            if ( (signed)bsMCp->tag.num < 0 &&
                 (signed)bsMCp->tag.num % BFM_RSVD_TAGS == BFM_RBMT ) {
                fprintf(stderr, "This is a RBMT file. Use -R to view the RBMT.\n");
                exit(2);
            }
        } else {
            rbmt_present = 0;
            /* TODO. test valid BMT page?? */
        }
    } else {
        assert(bfp->dmn);
        rbmt_present = RBMT_VERSION(bfp->dmn->dmnVers);
    }

    if ( rbmt_present ) {
         flgs |= V4DMN;
         bmtidx = BFM_BMT;
    } else {
         bmtidx = BFM_BMT_V3;
    }

    for ( vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
            bfp->ppage = 0;
            bfp->pcell = bmtidx;
            vdi = bfp->pvol;
        } else {
            bfp->fileTag = RSVD_TAGNUM_GEN(vdi, bmtidx);
            if ( bfp->type & VOL ) {
                /* if pvol set the first time thru (bfp->fd == -1),
                 * then only display summary for one volume
                 */
                stop = TRUE;
                vdi = bfp->pvol;
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pvol = vdi;

            /* call find_BMT() to load BMT's xtnt maps */
            ret = find_BMT(bfp, flgs);
            /* just point to dmn RBMT extents. no possibility of error. */
            assert(ret == OK );

            /* The DMN & VOL case haven't read the first page yet. */
            if ( read_page(bfp, 0) != OK ) {
                if ( bfp->type & FLE )
                    fprintf(stderr, "Read error on page 0 of BMT\n");
                continue;
            }
        }

        print_header(bfp);

        if ( bfp->pages == 1 ) {
            printf("There is 1 page in the BMT ");
        } else {
            printf("There are %d pages in the BMT ",
                bfp->pages);
        }
        if ( bfp->type & FLE ) {
            printf("in this file.\n");
        } else {
            printf("on this volume.\n");
        }

        extents = 1;
        cellCnt = 1;

        freeCells = ((bsMPgT *)bfp->pgBuf)->freeMcellCnt;

        /* If we have access to the xtnts (V3 BMT or rbmt_present) count them */
        if ( !(bfp->type & FLE && rbmt_present) ) {
            if ( rbmt_present ) {
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
                bmtp = bfp->dmn->vols[vdi]->rbmt;
                assert(bmtp);
            } else {
                bmtp = bfp;
            }

            /* Read primary mcell for this file from the RBMT. Get the
             * chain mcell from the BSR_XTNTS record in the primary mcell
             */
            ret = get_chain_mcell(bfp, &vn, &mcid);
            if ( ret != OK )
                continue;

            prevVn = bfp->pvol;
            prevPn = bfp->ppage;
            prevCn = bfp->pcell;

            while ( vn != 0 ) {
                char *filename = rbmt_present ? "RBMT" : "BMT";

                if ( vn != vdi ) {
                    fprintf(stderr, "Bad %s mcell vol %d page %d cell %d: ",
                      filename, prevVn, prevPn, prevCn);
                    fprintf(stderr, "bad nextVdIndex. expect %d read %d\n",
                      vdi, vn);
                    break;
                }

                if ( rbmt_present ) {
                    if ( mcid.page > bmtp->pages ) {
                        fprintf(stderr, "Bad BMT mcell vol %d page %d cell %d: ",
                          prevVn, prevPn, prevCn);
                        fprintf(stderr, "Bad nextMCId.page (%d). ", mcid.page);
                        fprintf(stderr, "Valid range 0 - %d\n", bmtp->pages - 1);
                        break;
                    }
                } else {
                    if ( mcid.page != 0 ) {
                        fprintf(stderr, "Bad BMT mcell vol %d page %d cell %d: ",
                          prevVn, prevPn, prevCn);
                        fprintf(stderr, "Bad nextMCId.page (%d). ", mcid.page);
                        fprintf(stderr, "Expected 0\n");
                        break;
                    }
                }

                if ( mcid.cell > 27 ) {
                    fprintf(stderr, "Bad RBMT mcell vol %d page %d cell %d: ",
                      prevVn, prevPn, prevCn);
                    fprintf(stderr, "Bad nextMCId.cell (%d). ", mcid.cell);
                    fprintf(stderr, "Valid range is 0 - 27.\n");
                    break;
                }

                if ( read_page(bmtp, mcid.page) != OK ) {
                    fprintf(stderr, "read_page failed\n");
                    exit(2);
                }
                bsMPgp = (bsMPgT*)bmtp->pgBuf;
                bsMCp = &bsMPgp->bsMCA[mcid.cell];

                ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)&bsXtraXtntRp);
                if ( ret != OK ) {
                    fprintf(stderr, "no BSR_XTRA_XTNTS\n");
                    break;
                }

                cellCnt++;
                extents += bsXtraXtntRp->xCnt - 1;

                ret = get_next_mcell(bfp, &vn, &mcid);
                if ( ret != OK )
                    break;
            }
        }

        printf("The BMT uses %d extents (out of %d) in %d %s.\n",
                 extents,  1 + (cellCnt - 1) * BMT_XTRA_XTNTS, cellCnt,
                 cellCnt == 1 ? "mcell" : "mcells");

        if ( !rbmt_present ) {
            if ( freeCells == 1 ) {
                printf("There is 1 free mcell in BMT page 0 ");
            } else {
                printf("There are %d free mcells in BMT page 0 ", freeCells);
            }
            if ( bfp->type & FLE ) {
                printf("in this file.\n");
            } else {
                printf("on this volume.\n");
            }
        }

        if ( !(flgs & FFLG) ) {
             continue;
        }

        print_BMT_freecells(bfp, flgs);
    }

    return;
}

/******************************************************************************/
/* Read the entire BMT and display the total number of free mcells and the 
** number of BMT pages with free mcells. As it reads the pages of the BMT
** it prints a progress report every 100 pages. A large BMT can take several
** minutes to read.
**
** Input: bfp->xmap or bfp->fd is set up
** Input: flgs:V4DMN - BMT is from a V4 domain
** Output: none
** Return: never returns
*/
/*
 * Define the location of the mcell that contains the head of the
 * deferred delete list. from bs_delete.h
 */
/* FIX. including bs_delete.h caused problems, so use local defines */
#define MCELL_LIST_PAGE 0
#define MCELL_LIST_PAGE_V3 1
#define MCELL_LIST_CELL 0

print_BMT_freecells(bitFileT* bfp, int flgs)
{
    int ret;
    bsMCT *cellp;
    int cellCnt;
    bsMcellFreeListT *mcellFreeListp;
    uint32T pg;
    int cnt;
    int free_list_head_page;
    bsMPgT *bsMPgp;

    fflush(stdout);
    cellCnt = 0;

    /* Print some info about the MCELL_FREE_LIST (-f option).
     * This info is kept in BMT Page 1 for V3, and BMT Page 0 for
     * Version 4.  (Both at lbn 48).
     */
    free_list_head_page = flgs & V4DMN ? MCELL_LIST_PAGE : MCELL_LIST_PAGE_V3;

    if ( read_page(bfp, free_list_head_page) != OK ) {
        fprintf(stderr, "read page %d failed\n", 1);
        exit(1);
    }

    bsMPgp = (bsMPgT *)bfp->pgBuf;
    /* The head of the free list is in cell 0. */
    cellp = &bsMPgp->bsMCA[MCELL_LIST_CELL];
    ret = find_bmtr( cellp, BSR_MCELL_FREE_LIST, &(char*)mcellFreeListp);
    if ( ret != OK ) {
        fprintf(stderr, "can't find BSR_MCELL_FREE_LIST\n");
        exit(2);
    }

    pg = mcellFreeListp->headPg;
    if ( flgs & VFLG ) {
        printf("first free pg %d\n", pg);
    }

    cnt = 0;
    while ( 1 ) {
        /* V4 domains can have BMT pg 0 on the free list and use pg -1 as end */
        if ( flgs & V4DMN && pg == (uint32T)(-1) ) {
            break;
        }
        /* V3 domains can't have BMT pg 0 on free list. pg == 0 is the end */
        if ( !(flgs & V4DMN) && (pg == 0 || pg == (uint32T)(-1)) ) {
            break;
        }

        cnt++;
        if ( !(flgs & VFLG) ) {
            if ( cnt % 100 == 0 ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr, " scanning the free list, %d pages read\r",
                  cnt);
            }
        }

        if ( read_page(bfp, pg) != OK ) {
            fprintf(stderr,"read page BMT page %d failed\n", pg);
            break;
        }

        bsMPgp = (bsMPgT *)bfp->pgBuf;
        if ( flgs & VFLG ) {
            printf("BMT pg %d has %d free mcells. Next free pg %d\n",
              pg, bsMPgp->freeMcellCnt, bsMPgp->nextFreePg);
        }
        /* TODO. if VFLG print page num and num of free cells */

        cellCnt += bsMPgp->freeMcellCnt;
        pg = bsMPgp->nextFreePg;
    }

    if ( !(flgs & VFLG) ) {
        fprintf(stderr, CLEAR_LINE);
    }

    printf("There are %d pages on the free list with a total of %d free mcells.\n", cnt, cellCnt);
}

/******************************************************************************/
/* Reads the (R)BMT and writes it to a file.
**
** Input: bfp->xmap or bfp->fd is set up
** Input: argv[optind] is the file name
** Output: none
** Return: never returns
*/
save_bmt(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile = NULL;

    optind++;
    if ( (bfp->type & DMN || bfp->type & SAV) && bfp->pvol == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    if ( flags & VFLG ) {
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
/* Displays ALL the pages of the (R)BMT. For a large BMT this could be
** megabytes of data.
**
** Input: bfp->xmap or bfp->fd is set up
** Input: argv, argc, optind just used to check for more (invalid) arguments
** Output: none
** Return: never returns
*/
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;
    uint32T pagenum;

    optind++;
    if ( (bfp->type & DMN || bfp->type & SAV) && bfp->pvol == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    ret = 0;
    for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
        ret |= print_page(bfp, pagenum, -1, flags & VFLG);
    }

    exit(ret);
}

/******************************************************************************/
/* Displays a page of BMT at a given disk block. NO valid AvdFS structures
** are assumed. The block is rounded down to the next multiple of 16 and
** is displayed as a page (16 blocks) of BMT. Within the page a particular
** mcell can be selected. The size of the volume has been obtained fron
** the disklabel not the AdvFS volume size.
**
** Input: bfp->dmn->vols[bfp->pvol]->blocks - if non zero, used for arg check
** Input: argv[optind] = -b <block_number> [cell_number]
** Output: none
** Return: never returns
*/
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long blknum;
    long mcellnum = -1;                          /* default: print all mcells */
    int ret;

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
            exit(BAD_PARAM);
        }
        optind++;
    } else if ( argc == optind ) {
        fprintf(stderr, "Missing block number.\n");
        usage();
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
        fprintf(stderr, "BMT pages are on %d block boundaries.\n",
          ADVFS_PGSZ_IN_BLKS);
        blknum = blknum / ADVFS_PGSZ_IN_BLKS * ADVFS_PGSZ_IN_BLKS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%d).\n",
          ADVFS_PGSZ_IN_BLKS, blknum);
    }

    bfp->pgLbn = blknum;
    if ( argc > optind ) {

        if ( getnum(argv[optind], &mcellnum) != OK ) {
            fprintf(stderr, "Badly formed mcell number: \"%s\"\n",
              argv[optind]);
            exit(BAD_PARAM);
        }

        if ( mcellnum < 0 || mcellnum >= BSPG_CELLS ) {
            fprintf(stderr, "Invalid value (%d) for mcell. ", mcellnum);
            fprintf(stderr, "Valid mcell numbers range from 0 to 27.\n");
            exit(BAD_PARAM);
        }
        optind++;
    }

    if ( argc > optind ) {

        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    (void)print_block(bfp, (int)mcellnum, flags);

    exit(0);
}

/*****************************************************************************/
/* Prints one or all the mcells in a BMT page read from the disk. NO AdvFS
** data structure is assumed.
**
** Input: bfp->fd - fd of volume
** Input: mcellnum - selected mcell or -1 for all mcells
** Input: vflg - verbose display of mcell contents
** Output: writes to stdout
** Return: OK all went OK
**         non zero if lseek or read failed
*/
int
print_block(bitFileT *bfp, int mcellnum, int vflg)
{
    bsMPgT page;
    int cn;
    int results;
    lbnT lbn = bfp->pgLbn;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    if ( lseek(bfp->fd, ((unsigned long)lbn) * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "print_block: lseek to block %d failed", lbn);
        perror(errstr);
        return 1;
    }

    results = read(bfp->fd, &page, sizeof(bsMPgT));
    if ( results != sizeof(bsMPgT) ) {
        if ( results == -1 ) {
            perror("print_block: read failed");
        } else {
            fprintf(stderr, "read returned %d expected %d\n",
              results, sizeof(bsMPgT));
        }
        return 2;
    }

    bfp->pgVol = bfp->pvol;
    bfp->pgLbn = lbn;
    bfp->pgNum = -1;
    print_BMT_page_header(bfp, &page, vflg);

    if ( mcellnum == -1 ) {
        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            print_cell(&page.bsMCA[cn], cn, vflg, page.megaVersion);
        }
    } else {
        print_cell(&page.bsMCA[mcellnum], mcellnum, vflg | ONECELL,
                   page.megaVersion);
    }

    return OK;
}

/******************************************************************************/
/* Using AdvFS metadata files, find the primary mcell for the specified
** fileset or file in a fileset. Display the content of the mcell. If the
** -c option is given, display the contents of all the mcells in the chain
** and next mcell.
**
** Input: bfp->dmn is set, bfp->type is set
** Input: argv is a fileset (or tag) and possibly a file path (or tag)
** Input: flags:VFLG - display verbose contents of mcell(s)
** Output: writes to stdout
** Return: never returns
*/
process_file(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;
    bfTagT tag;
    long longnum;
    bitFileT setBf;
    bitFileT *setBfp = &setBf;
    bitFileT rootBf;
    bitFileT *rootBfp = &rootBf;
    bitFileT *bmt;

    if ( bfp->type & FLE ) {
        fprintf(stderr, "Can't specify set and file for a saved BMT.\n");
        usage();
    }

    assert(bfp->dmn);
    newbf(bfp, setBfp);
    newbf(bfp, rootBfp);

    if ( argc == optind ) {
        fprintf(stderr, "Not enough arguments\n");
        usage();
    }

    /* load xtnts of root TAG file */
    if ( find_TAG(rootBfp) != OK ) {
        fprintf(stderr, "Couldn't find the root TAG file\n");
        exit(2);
    }

    ret = get_set(rootBfp, argv, argc, &optind, setBfp);
    if ( ret != OK ) {
        if ( ret == BAD_SYNTAX ) {
            usage();
        } else {
            exit(1);
        }
    }

    if ( argc == optind )
        bfp = setBfp;

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-s") == 0 ) {
            bfp = setBfp;
            bfp->setTag = setBfp->fileTag;
            search(bfp, argv, argc, optind, flags & VFLG);
            /*NOTREACHED*/
        }

        if ( strcmp(argv[optind], "-c") == 0 ) {
            bfp = setBfp;
            goto chain;
        }

        ret = get_file(setBfp, argv, argc, &optind, bfp);
        /* If the disk had bad data, try to print what mcells we got. */
        if ( ret != OK ) {
            if ( ret == BAD_SYNTAX ) {
                usage();
            } else if ( ret != BAD_DISK_VALUE ) {
                exit(1);
            } else if ( bfp->pvol == 0 || bfp->dmn->vols[bfp->pvol] == NULL ) {
                exit(1);
            }
        } else if ( bfp->setp->origSetp != NULL && bfp->pvol == 0 ) {
            printf("This clone file (orig set %d) has no metadata.\n",
              bfp->setp->origSetp->fileTag);
            exit(0);
        }
    }

    if ( argc > optind ) {
        if ( strcmp(argv[optind], "-c") == 0 ) {
chain:
            optind++;
            if ( argc > optind ) {
                fprintf(stderr, "Too many arguments.\n");
                usage();
            }

            ret = process_chain(bfp, bfp->ppage, bfp->pcell, flags);
            exit(ret);
        }

        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);
    /* TODO. use find_BMT */
    if ( bfp->fileTag < 0 ) {
        bmt = bfp->dmn->vols[bfp->pvol]->rbmt;
    } else {
        bmt = bfp->dmn->vols[bfp->pvol]->bmt;
    }
    bfp->xmap = bmt->xmap;
    bfp->pages = bmt->pages;
    bfp->fileTag = bmt->fileTag;
    /* FIX. need to set setTag?? */

    ret = print_page(bfp, bfp->ppage, bfp->pcell, flags & VFLG);

    exit(ret);
}

/*****************************************************************************/
/* Possibly select a mcell from the passed in (R)BMT file/ page number and
** display them. If a mcell is specified, the -c flag will display all
** conected mcells.
**
** Input: bfp defines a (R)BMT file. xmap or fd set up, pvol set up.
** Input: longnum - page number
** Input: argv, argc, optind = [cell_number [-c]]
** Output: writes to stdout
** Return: never returns
*/
process_page(bitFileT *bfp, long longnum, char *argv[], int argc, int optind,
             int flags)
{
    uint32T pagenum;
    uint32T mcellnum = -1;
    int ret;

    optind++;
    if ( (unsigned long)longnum >= bfp->pages ) {
        fprintf(stderr, "Invalid BMT page number (%ld). ", longnum);
        fprintf(stderr, "Valid page numbers are 0 through %d.\n",
          bfp->pages - 1);
        exit(1);
    }
    pagenum = longnum;

    /* At this point we have identified a page on a particular disk. */
    if ( argc > optind && strcmp(argv[optind], "-f") == 0 ) {
        process_free(bfp, pagenum, flags & VFLG);
        exit(0);
    }

    /* get mcell number */
    if ( argc > optind ) {
        if ( getnum(argv[optind], &longnum) != OK ) {
            fprintf(stderr, "Badly formed mcell number: \"%s\"\n",
              argv[optind]);
            exit(1);
        }
        optind++;
        if ( (unsigned long)longnum >= BSPG_CELLS ) {
            fprintf(stderr, "Mcell number must be from 0 to 27\n");
            exit(7);
        }
        mcellnum = longnum;
    }

    bfp->ppage = pagenum;
    bfp->pcell = mcellnum;

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        if ( bfp->type == FLE ) {
            fprintf(stderr, "-c not allowed on files\n");
            exit(1);
        }

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            exit(1);
        }

        ret = process_chain(bfp, pagenum, mcellnum, flags);
        exit(ret);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        exit(1);
    }

    ret = print_page(bfp, (int)pagenum, (int)mcellnum, flags & VFLG);
    exit(ret);
}

/*****************************************************************************/
/* Prints the page header then prints one or all the mcells.
** The -v flag causes all the contents of the header and the mcell(s) to
** be printed.
**
** Input: bfp->type used to print the header, bfp->pgBuf contains the page
** Input: vflg:VFLG - print entire header and mcell contents
** Output: writes to stdout
** Return: OK if all went well
**         non zero if read_page() failed
*/
int
print_page(bitFileT *bfp, int pgnum, int mcellnum, int vflg)
{
    int cn;
    bsMPgT *bsMPgp;

    if ( read_page(bfp, pgnum) != OK ) {
        fprintf(stderr, "read page %d failed\n", pgnum);
        return 1;
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;

    print_BMT_page_header(bfp, bsMPgp, vflg);

    if ( mcellnum == -1 ) {
        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            print_cell(&bsMPgp->bsMCA[cn], cn, vflg, bsMPgp->megaVersion);
        }
    } else {
        print_cell(&bsMPgp->bsMCA[mcellnum], mcellnum, 
                   vflg | ONECELL, bsMPgp->megaVersion);
    }

    return OK;
}

/*****************************************************************************/
/* Prints the contents of the given mcell and all the chain and next mcells.
** The -v flag prints the entire contents of each mcell.
**
** Input: bfp defines the (R)BMT. xmap or fd set. pvol set.
** Input: page, cell defines the mcell in the (R)BMT.
** Output: writes to stdout
** return: OK if all went weel
**         non zero if get_chain_mcell or print_chain fails
*/
process_chain(bitFileT *bfp, uint32T page, uint32T cell, int flgs)
{
    vdIndexT vn;
    int ret;
    bfMCIdT mcid;

    bfp->ppage = page;
    bfp->pcell = cell;
    bfp->fileTag = 0;   /* So print_chain won't miscompare on the tag */

    vn = bfp->pvol;

    /*
     * Print the mcell chain linked by the mcell headers.
     */
    ret = print_chain(bfp, vn, page, cell, &bfp->fileTag, flgs);

    if ( bfp->fileTag == 0 ) {
        /* The ddl header cell has a tag of 0. */
        /* There is no chain, so don't follow. */
        return ret;
    }

    if ( get_chain_mcell(bfp, &vn, &mcid) ) {
        fprintf(stderr, "get_chain_mcell failed\n");
        return 1;
    }

    if ( vn ) {
        printf("\nExtent mcells from BSR_XTNTS record chain pointer.\n");
        ret |= print_chain(bfp, vn, mcid.page, mcid.cell, &bfp->fileTag, flgs);
    }

    return ret;
}

/*****************************************************************************/
/* Print the contents of the entire next list of mcells, starting at the
** given mcell. Mcell Id validity is checks at each mcell including the first.
**
** Input: bfp->dmn set up. bfp->fileTag. dmn->vols[] used to check vdi validity
** Input: vn, pn, cn - first in a list of mcells
** Output: writes to stdout
** Return: OK if all went well
**         non zero if list points to a bad vn, pn, cn. Or if read_page fails.
*/
int
print_chain(bitFileT *bfp, vdIndexT vn, uint32T pn, uint32T cn, int *tag,
            int flgs)
{
    bsMCT *mcell;
    bitFileT *bmt;
    int lastv = -1;
    int lastp = -1;
    int ret;
    int savtag;
    bsMPgT *bsMPgp;

    while (vn > 0) {
        if ( bfp->type & FLE ) {
            if ( vn != bfp->pvol ) {
                printf("Can't follow chain\n");
            }
        } else if ( bfp->dmn->vols[vn] == NULL ) {
            if ( bfp->type & DMN ) {
                printf("No volume %d in domain \"%s\".\n",
                  vn, bfp->dmn->dmnName);
            } else {
                printf("No information about volume %d\n", vn);
            }
            return 1;
        }

        if ( bfp->type & FLE ) {
            bmt = bfp;
        } else if ( flgs & RBMT ) {
            bmt = bfp->dmn->vols[vn]->rbmt;
        } else {
            bmt = bfp->dmn->vols[vn]->bmt;
        }

        if ( pn >= bmt->pages ) {
            fprintf(stderr, "Chain BMT page %d is invalid. ", pn);
            fprintf(stderr, "Valid pages range from 0 to %d.\n", bmt->pages-1);
            return BAD_DISK_VALUE;
        }
        if ( cn >= BSPG_CELLS ) {
            fprintf(stderr, "Chain BMT mcell number %d is invalid. ", cn);
            fprintf(stderr, "Valid mcells range from 0 to 27.\n");
            return BAD_DISK_VALUE;
        }
        if ( vn != lastv || pn != lastp ) {
            ret = read_page(bmt, pn);
            if ( ret != OK ) {
                fprintf(stderr, "read page BMT page %d on volume %d failed\n",
                  pn, vn);
                return ret;
            }

            bsMPgp = (bsMPgT*)bmt->pgBuf;
            bfp->pgVol = bmt->pgVol;
            bfp->pgLbn = bmt->pgLbn;
            bfp->pgNum = bmt->pgNum;
            savtag = bfp->fileTag;
            bfp->fileTag = bmt->fileTag;
            print_BMT_page_header(bfp, bsMPgp, flgs & VFLG);
            bfp->fileTag = savtag;
        } else
            printf(SINGLE_LINE);

        mcell = &bsMPgp->bsMCA[cn];

        if ( *tag && (int)mcell->tag.num != *tag ) {
            fprintf(stderr, "Mcell (%d %d %d) tag is %d, expected %d.\n",
              vn, pn, cn, mcell->tag.num, bfp->fileTag);
        } else
            *tag = (int)mcell->tag.num;

        print_cell(mcell, cn, flgs | ONECELL, bsMPgp->megaVersion);

        lastv = vn;
        lastp = pn;

        vn = mcell->nextVdIndex;
        pn = mcell->nextMCId.page;
        cn = mcell->nextMCId.cell;
    }

    return OK;
}

/*****************************************************************************/
/* Prints the header of each mcell on the free list in a page. The -v option
** prints the entire mcell header for each mcell on the free list.
**
** Input: bfp selects the (R)BMT
** Input: pn selects the page.
** Input: vflg:VFLG - print the entire header contents
** Output: writes to stdout
** Return: OK if all went well
**         non zero if free list has bad data or read_page() fails
*/
int
process_free(bitFileT *bfp, uint32T pn, int vflg)
{
    vdIndexT vn;
    int i;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint32T cn;
    int ret;

    if ( !(bfp->type & FLE) ) {
        vn = bfp->pvol;
    } else {
        /* FIX. use vdi from attr */
        vn = 0;
    }

    ret = read_page(bfp, pn);
    if ( ret != OK ) {
        fprintf(stderr,"read page BMT page %d on volume %d failed\n", pn, vn);
        return ret;
    }
    bsMPgp = (bsMPgT*)bfp->pgBuf;

    print_BMT_page_header(bfp, bsMPgp, vflg);

    if ( bsMPgp->freeMcellCnt == 0 ) {
        printf("No free mcells.\n");
        return OK;
    }

    cn = bsMPgp->nextfreeMCId.cell;
    if ( cn >= BSPG_CELLS ) {
        fprintf(stderr,
          "nextfreeMCId.cell (%d) in BMT page %d on vol %d is > %d\n",
          cn, pn, vn, BSPG_CELLS);
        return 1;
    }

    if ( bsMPgp->freeMcellCnt > BSPG_CELLS ) {
        fprintf(stderr,
          "freeMcellCnt (%d) in BMT page %d on vol %d is > %d\n",
          bsMPgp->freeMcellCnt, pn, vn, BSPG_CELLS);
        return 1;
    }

    for ( i = 0; i < bsMPgp->freeMcellCnt; i++ ) {
        bsMCp = &bsMPgp->bsMCA[cn];
        print_cell_hdr(bsMCp, cn, vflg);

        cn = bsMCp->nextMCId.cell;
        if ( cn >= BSPG_CELLS ) {
            fprintf(stderr,
             "nextMCId.cell (%d) at cell %d in BMT page %d on vol %d is > %d\n",
              cn, cn, pn, vn, BSPG_CELLS);
            return 1;
        }

#ifdef NOTYET
        /*FIX. errs on the last (valid) mcell in the chain */
        if ( bsMCp->nextMCId.page != pn ) {
            fprintf(stderr, "Mcell (%d %d %d) has bad nextMCId.page (%d).\n",
              vn, pn, cn, bsMCp->nextMCId.page);
            return BAD_DISK_VALUE;
        }
#endif

        /* nextVdIndex is 0 in mcells on the free list.  */
        /* FIX. check set tag & file tag == 0 */
        /* FIX. check last cell on list has nextMCId.page,cell == 0 */

        bsMCp = &bsMPgp->bsMCA[cn];
    }

    return OK;
}

/******************************************************************************/
/* Search for a given block in the (R)BMT extents on a given volume or
** search for a given fragment in the mcells for a given fileset.
** Prints the mcell that conatins the searched for information.
** vbmtpg {domain vdi | vol} -s b block [-c]
** vbmtpg domain set -s {f <fragid> | F <page> <frag>}          search
**
** Input: bfp specifies the (R)BMT on a volume for block searchs
**        bfp specifies a domain and fileset for frag searches
** Input: argv, argc, optind - search type and block or frag
** Input: flags:VFLG - print entire contents of found mcell
** Output: writes to stdout
** Return: never returns
*/
search(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;

            optind++;
    if ( argc == optind ) {
        fprintf(stderr, "missing search argument");
        usage();
    }

    if ( strcmp(argv[optind], "b") == 0 ) {
        search_block(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "f") == 0 ) {
        search_frag(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    if ( strcmp(argv[optind], "t") == 0 ) {
        search_tag(bfp, argv, argc, optind, flags);
        /* NOTREACHED */
    }

    fprintf(stderr, "Bad search arguments\n");
    usage();
}

/******************************************************************************/
/* Search thru the (R)BMT file for extents that contain the given block.
** When an extent is fount, print it out.
**
** Input: bfp defines the (R)BMT to search. xmap set up
** Input: block - find an extnet with this block
** Input: flags:VFLG - print entire contents of found cell
**        flags:CFLG - keep on searching after first match found
** Output: writes to stdout
** Return: never returns
*/
search_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long block;
    int ret;

    optind++;
    if ( bfp->fd == -1 ) {
        fprintf(stderr, "A volume must be specified to search for a block.\n");
        usage();
    }

    if ( argc == optind ) {
        fprintf(stderr, "missing block search argument");
        usage();
    }

    if ( getnum(argv[optind], &block) != OK ) {
        fprintf(stderr, "badly formed search block number: \"%s\".\n",
          argv[optind]);
        exit(1);
    }
    optind++;

    if ( bfp->type != FLE && bfp->dmn->vols[bfp->pvol]->blocks != 0 ) {
        if ( (unsigned long)block >= bfp->dmn->vols[bfp->pvol]->blocks ) {
            fprintf(stderr, "Invalid block number (%d). ", block);
            fprintf(stderr, "Valid block numbers range from 0 to %d.\n",
              bfp->dmn->vols[bfp->pvol]->blocks - 1);
            exit(2);
        }
    }

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        flags |= FFLG;
    }

    if ( argc != optind ) {
        fprintf(stderr, "too many search arguments");
        usage();
    }

    ret = scan_mcells(bfp, block, flags, scan_cell_for_block);
    if ( ret == ENOENT && bfp->dmn && RBMT_VERSION(bfp->dmn->dmnVers) ) {
        /* Didn't find block in BMT (RBMT). Try RBMT (BMT). */
        /* Check 'other' BMT only if this is a volume (not a file) and */
        /* it has the 'other' BMT. */
        flags ^= RBMT;
        ret = scan_mcells(bfp, block, flags, scan_cell_for_block);
    }

    if ( ret == ENOENT ) {
        printf("Block %d is not in any extent.\n", block);
    }

    exit(0);
}

/******************************************************************************/
/* Search for fragid being used by a file in the specified fileset.
** bfp->setTag is used to pass in the fileset tag.
**
** Input: bfp specifies the domain. bfp->dmn filled in
** Input: bfp->setTag specifies the set tag to search
** Input: fragid specifies the frag to look for.
** Input: flags:VFLG - print entire contents of found mcell
** Output: prints to stdout
** Return: never returns
*/
search_frag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long frag;
    int ret;

    optind++;

    if ( bfp->setTag <= 0 ) {
        fprintf(stderr, "A set must be specified to search for a fragId\n");
        usage();
    }

    if ( argc == optind ) {
        fprintf(stderr, "missing frag search argument");
        usage();
    }

    if ( getnum(argv[optind], &frag) != OK ) {
        fprintf(stderr, "badly formed frag number: \"%s\".\n", argv[optind]);
        exit(1);
    }
    optind++;

    if ( frag < 0 ) {
        fprintf(stderr, "Invalid frag number (%d). ", frag);
        fprintf(stderr, "Frag number must be positve.\n");
        exit(2);
    }
    /* it's hard to test the allowable frag range. */
    /* we would have to open the frag file */

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        flags |= FFLG;
    }

    if ( argc != optind ) {
        fprintf(stderr, "too many search arguments");
        usage();
    }

    flags &= ~RBMT;
    bfp->pvol = 0;
    ret = scan_mcells(bfp, frag, flags, scan_cell_for_frag);
    if ( ret == ENOENT ) {
        if ( bfp->setName[0] != '\0' ) {
            printf("fragid %d is not used by any file in fileset \"%s\"\n",
              frag, bfp->setName);
        } else {
            printf("fragid %d is not used by any file in fileset tag %d\n",
              frag, bfp->setTag);
        }
    }

    exit(0);
}

/******************************************************************************/
/* Search for tag being used by a file in the specified fileset.
** bfp->setTag is used to pass in the fileset tag.
**
** Input: bfp specifies the domain. bfp->dmn filled in
** Input: bfp->setTag specifies the set tag to search
** Input: flags:VFLG - print entire contents of found mcell
** Output: prints to stdout
** Return: never returns
*/
search_tag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long tag;
    int ret;

    optind++;

    if ( argc == optind ) {
        fprintf(stderr, "missing tag search argument");
        usage();
    }

    if ( getnum(argv[optind], &tag) != OK ) {
        fprintf(stderr, "badly formed tag number: \"%s\".\n", argv[optind]);
        exit(1);
    }
    optind++;

    /*FIX. see MAXTAG in vods.h */
    if ( tag > 0x7fffffffL || tag < -0x80000000L ) {
        fprintf(stderr, "Invalid tag number (%d).\n", tag);
        exit(2);
    }

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        flags |= FFLG;
    }

    if ( argc != optind ) {
        fprintf(stderr, "too many search arguments");
        usage();
    }

    /* FIX? if tag neg, scan RBMT */
    if ( bfp->setTag > 0 ) {
        flags &= ~RBMT;
        bfp->pvol = 0;
    }

    if ( bfp->type & FLE ) {
        bfp->pvol = 0;
    }

    ret = scan_mcells(bfp, tag, flags, scan_cell_for_tag);
    if ( ret == ENOENT ) {
        if ( bfp->setTag > 0 ) {
            if ( bfp->setName[0] != '\0' ) {
                printf("No mcell belonging to tag %d was found in fileset \"%s\"\n",
                  tag, bfp->setName);
            } else {
                printf("No mcell belonging to tag %d was found in fileset tag %d\n",
                  tag, bfp->setTag);
            }
        } else if ( bfp->pvol != 0 ) {
            printf("No mcell belonging to tag %d was found on volume index %d\n",
                  tag, bfp->pvol);
        } else {
            printf("No mcell belonging to tag %d was found\n", tag);
        }
    }

    exit(0);
}

/******************************************************************************/
/* Scan all the mcells and call a function for each mcell.
**
** Input: bfp defines the domain and its RBMTs and BMTs
**        bfp->pvol - 0 means search all volumes, non zero means search one
**        flags:VFLG - verbose print mcell contents
**        flags:CFLG - continue, keep searching after match found
**        flags:RBMT - search RBMT
** Output: writes to stdout
** Return:
*/
int
scan_mcells(bitFileT *bfp, long value, int flags, int (*funcp)())
{
    int startVdi;
    int endVdi;
    bitFileT *bmtbfp;
    int vdi;
    uint32T pn;
    int cn;
    int ret;
    bsMCT *cellp;
    int found = ENOENT;
    bsMPgT *bsMPgp;
    int version;

    if ( bfp->pvol == 0 ) {
        /* No volume specified, scan all the volumes. */
        startVdi = 0;
        if ( bfp->type & FLE ) {
            /* Unless its a file, then just scan the file. */
            endVdi = 0;
        } else {
            endVdi = BS_MAX_VDI - 1;
        }
    } else {
        /* A volume is specified, just scan that one volume. */
        startVdi = bfp->pvol;
        endVdi = bfp->pvol;
    }

    for ( vdi = startVdi; vdi <= endVdi; vdi++ ) {
        if ( bfp->type & FLE ) {
            /* bfp is a saved file, just look in the saved file. */
            bmtbfp = bfp;
        } else {
            assert(bfp->dmn);
            if ( bfp->dmn->vols[vdi] == NULL ) {
                /* No such volume in this domain. */
                continue;
            }

            if ( flags & RBMT ) {
                assert(bfp->dmn->vols[vdi]->rbmt);
                bmtbfp = bfp->dmn->vols[vdi]->rbmt;
            } else {
                bmtbfp = bfp->dmn->vols[vdi]->bmt;
            }
        }

        for ( pn = 0; pn < bmtbfp->pages; pn++ ) {
            ret = read_page(bmtbfp, pn);
            if ( ret != OK ) {
                fprintf(stderr,
                  "scan_mcells: read_page (tag %d, page %d) failed.\n",
                  bmtbfp->fileTag, bmtbfp->ppage);
                exit(2);
            }

            bsMPgp = (bsMPgT*)bmtbfp->pgBuf;

            if ( pn == 0 ) {
                version = bsMPgp->megaVersion;
            }

            if ( pn % 100 == 0 ) {
                fprintf(stderr, CLEAR_LINE);
                if ( flags & RBMT ) {
                    fprintf(stderr, " scanning volume %d, RBMT page %d of %d\r",
                      vdi, pn, bmtbfp->pages);
                } else {
                    fprintf(stderr, " scanning volume %d, BMT page %d of %d\r",
                      vdi, pn, bmtbfp->pages);
                }
            }

            for ( cn = 0; cn < BSPG_CELLS; cn++) {

                cellp = &bsMPgp->bsMCA[cn];

                if ( cellp->bfSetTag.num == 0 && cellp->tag.num == 0 ) {
                    /* this cell is not in use */
                    continue;
                }

                ret = (*funcp)(bmtbfp, vdi, pn, cn, value, version, flags);
                /* OK means found, ENOENT means not found */
                if ( ret == OK ) {
                    if ( flags & FFLG ) {
                        found = OK;
                        continue;
                    } else {
                        return OK;
                    }
                } else {
                    continue;
                }
            }
        }
    }
    fprintf(stderr, CLEAR_LINE);

    return found;
}


/* return ENOENT if not found, OK if found */
static int
scan_cell_for_block(bitFileT *bfp, int vdi, int pn, int cn, long value,
                    int version, int flags)
{
    bsXtntRT *bsXtntRecp;
    bsShadowXtntT *bsShadowXtntRecp;
    bsXtraXtntRT *bsXtraXtntRecp;
    long block = value;
    int ret;
    uint16T xCnt;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCT *cellp = &bsMPgp->bsMCA[cn];

    ret = find_bmtr(cellp, BSR_XTNTS, &(char*)bsXtntRecp);
    if ( ret == OK ) {
        if ( !BS_BFTAG_RSVD(cellp->tag) &&
             !FIRST_XTNT_IN_PRIM_MCELL(version, bsXtntRecp->type) )
        {
            /*
             * This is a regular (non-striped) file that does
             * not support extent information in the BSR_XTNTS
             * record.  Skip this mcell.
             */
            return ENOENT;
        }

        xCnt = bsXtntRecp->firstXtnt.xCnt;
        if ( xCnt > BMT_XTNTS ) {
            if ( (signed)cellp->bfSetTag.num == -BFM_BFSDIR &&
                 BS_BFTAG_RSVD(cellp->tag) &&
                 BS_IS_TAG_OF_TYPE(cellp->tag, BFM_MISC) &&
                 cn == 5 )
            {
                if ( xCnt != 3 ) {
                    fprintf(stderr,
 "Bad xCnt (%d) in primary extent record for MISC bitfile on page %d, cell %d\n", xCnt, pn, cn);
                    xCnt = 3;
                 }
            } else {
                fprintf(stderr,
                 "Bad xCnt (%d) in primary extent record on page %d, cell %d\n",
                  xCnt, pn, cn);

                xCnt = BMT_XTNTS;
            }
        }
        return search_xa(bfp, cellp, cn, bsXtntRecp->firstXtnt.bsXA, xCnt,
                         block, flags, version);
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Bad mcell number %d on page %d\n", cn, pn);
        return BAD_DISK_VALUE;
    }

    ret = find_bmtr(cellp, BSR_SHADOW_XTNTS, &(char*)bsShadowXtntRecp);
    if ( ret == OK ) {
        xCnt = bsShadowXtntRecp->xCnt;
        if ( xCnt > BMT_SHADOW_XTNTS ) {
            fprintf(stderr,
          "Bad xCnt (%d) in shadow extent record on page %d, cell %d\n",
              xCnt, pn, cn);
            xCnt = BMT_SHADOW_XTNTS;
        }
        return search_xa(bfp, cellp, cn, bsShadowXtntRecp->bsXA, xCnt,
                       block, flags, version);
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Bad mcell number %d on page %d\n", cn, pn);
        return BAD_DISK_VALUE;
    }

    ret = find_bmtr(cellp, BSR_XTRA_XTNTS, &(char*)bsXtraXtntRecp);
    if ( ret == OK ) {
        xCnt = bsXtraXtntRecp->xCnt;
        if ( xCnt > BMT_XTRA_XTNTS ) {
            fprintf(stderr,
            "Bad xCnt (%d) in xtra extent record on page %d, cell %d\n",
              xCnt, pn, cn);
            xCnt = BMT_XTRA_XTNTS;
        }
        return search_xa(bfp, cellp, cn, bsXtraXtntRecp->bsXA, xCnt,
                       block, flags, version);
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Bad mcell number %d on page %d\n", cn, pn);
        return BAD_DISK_VALUE;
    }

    return ENOENT;
}

/******************************************************************************/
/* Search an extent array for the given block. If found print the contents
** of the mcell. Indentify the file tag and set tag. Print the page number
** of the file page that contains the block.
**
** Input: bfp->type used to print cell header
** Input: cellp - contenst of cell to search
** Input: cn - cell number of cell to search. used to display the contents
** Input: bsXA, ndesc - extent array to search
** Input: block - look for this block in the extent array
** Input: flags:VFLG - print entire cell contents
** Input: dmnVersion - passed to print_cell
** Output: prints to stdout
** Return: OK if bsXA covers block
**         ENOENT if block is not in bsXA
*/
search_xa(bitFileT *bfp, bsMCT *cellp, int cn, bsXtntT *bsXA, int ndesc,
          long block, int flags, int dmnVersion)
{
    int j;
    uint32T first_blk;
    uint32T last_blk;
    uint32T pgs;
    uint pn;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;

    assert(cellp >= &bsMPgp->bsMCA[0]);
    assert(cellp <= &bsMPgp->bsMCA[BSPG_CELLS]);
    for (j = 0; j < ndesc - 1; j++) {
        if ( bsXA[j].vdBlk == XTNT_TERM || bsXA[j].vdBlk == PERM_HOLE_START ) {
            continue;
        }

        first_blk = bsXA[j].vdBlk;
        pgs  = bsXA[j + 1].bsPage - bsXA[j].bsPage;
        last_blk = first_blk + pgs * ADVFS_PGSZ_IN_BLKS;
        if ( block >= first_blk && block < last_blk ) {
            fprintf(stderr, CLEAR_LINE);
            pn = bsXA[j].bsPage + (block - first_blk) / ADVFS_PGSZ_IN_BLKS;
            printf("block %d is part of page %d in set tag %d, file tag %d\n ",
              block, pn, cellp->bfSetTag.num, cellp->tag.num);

            print_BMT_page_header(bfp, bsMPgp, flags & VFLG);

            print_cell(cellp, cn, (flags & VFLG) | ONECELL, dmnVersion);

            return OK;
        }
    }
    return ENOENT;
}


scan_cell_for_frag(bitFileT *bfp, int vdi, int pn, int cn, long value,
                   int version, int flags)
{
    long fragid = value;
    statT *statRecp;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCT *cellp = &bsMPgp->bsMCA[cn];

    if ( cellp->bfSetTag.num != bfp->setTag ) {
        /* this cell belongs to a different fileset */
        return ENOENT;
    }

    if ( find_bmtr( cellp, BMTR_FS_STAT, &(char*)statRecp) != OK ) {
        return ENOENT;
    }

    if ( statRecp->fragId.frag != fragid ) {
        return ENOENT;
    }

    fprintf(stderr, CLEAR_LINE);
    if ( bfp->setName[0] != '\0' ) {
        printf( "Fragid %d in fileset \"%s\" is used by file tag %d\n",
                  fragid, bfp->setName, cellp->tag.num);
    } else {
        printf( "Fragid %d in fileset tag %d is used by file tag %d\n",
              fragid, cellp->bfSetTag.num, cellp->tag.num);
    }

    print_BMT_page_header(bfp, bsMPgp, flags & VFLG);

    print_cell(cellp, cn, (flags & VFLG) | ONECELL, version);
    fflush(stdout);

    return OK;
}

/******************************************************************************/
/* Search for tagnum being used by a file in the specified fileset.
** bfp->setTag is used to pass in the fileset tag.
**
** Input: bfp specifies the domain. bfp->dmn filled in
** Input: bfp->setTag specifies the set tag to search
** Input: fragid specifies the frag to look for.
** Input: flags:VFLG - print entire contents of found mcell
** Output: prints to stdout
** Return: OK if tag is found
**         ENOENT if tag is not found
*/
scan_cell_for_tag(bitFileT *bfp, int vdi, int pn, int cn, long value,
                  int version, int flags)
{
    int results;
    int found = 0;
    int tagnum = (int)value;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCT *cellp = &bsMPgp->bsMCA[cn];

    if ( bfp->setTag && cellp->bfSetTag.num != bfp->setTag ) {
        /* this cell belongs to a different fileset */
        return ENOENT;
    }

    if ( cellp->tag.num != tagnum ) {
        return ENOENT;
    }

    fprintf(stderr, CLEAR_LINE);

    print_BMT_page_header(bfp, bsMPgp, flags & VFLG);

    print_cell(cellp, cn, (flags & VFLG) | ONECELL, version);
    fflush(stdout);

    return OK;
}

/******************************************************************************/
process_ddl(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;
    int ddlPg;
    bsMCT *bsMCp;
    delLinkT *delLinkp;
    int version;
    bfMCIdT nextMCId;
    bfMCIdT prevMCId;
    bfMCIdT curMCId;
    bsXtntRT *bsXtntp;
    int vdi;
    int startVdi;
    int endVdi;
    bitFileT *bmtbfp;
    bsMPgT *bsMPgp;

    flags |= DDL;

    if ( flags & RBMT ) {
        fprintf(stderr, "No defered list in the RBMT.\n");
        exit(1);
    }

    if ( bfp->pvol == 0 ) {
        /* No volume specified, scan all the volumes. */
        startVdi = 0;
        if ( bfp->type & FLE ) {
            /* Unless its a file, then just scan the file. */
            endVdi = 0;
        } else {
            endVdi = BS_MAX_VDI - 1;
        }
    } else {
        /* A volume is specified, just scan that one volume. */
        startVdi = bfp->pvol;
        endVdi = bfp->pvol;
    }

    for ( vdi = startVdi; vdi <= endVdi; vdi++ ) {
        if ( bfp->type & FLE ) {
            /* bfp is a saved file, just look in the saved file. */
            bmtbfp = bfp;
        } else {
            if ( bfp->dmn->vols[vdi] == NULL ) {
                /* No such volume in this domain. */
                continue;
            }

            bmtbfp = bfp->dmn->vols[vdi]->bmt;
        }


        ret = read_page(bmtbfp, 0);
        if ( ret != OK ) {
            fprintf(stderr, "process_ddl: read_page page 0 failed.\n");
            exit(2);
        }

        bsMPgp = (bsMPgT*)bmtbfp->pgBuf;
        version = bsMPgp->megaVersion;
        if ( version >= FIRST_RBMT_VERSION ) {
            ddlPg = MCELL_LIST_PAGE;      /* ddl is on BMT page 0 */
        } else {
            ddlPg = MCELL_LIST_PAGE_V3;   /* ddl is on BMT page 1 */
        }

        ret = read_page(bmtbfp, ddlPg);
        if ( ret != OK ) {
            fprintf(stderr, "process_ddl: read_page page %d failed.\n", ddlPg);
            exit(2);
        }

        bsMPgp = (bsMPgT*)bmtbfp->pgBuf;
        bsMCp = &bsMPgp->bsMCA[MCELL_LIST_CELL];  /* cell 0 */

        ret = find_bmtr(bsMCp, BSR_DEF_DEL_MCELL_LIST, (char**)&delLinkp);
        if ( ret != OK ) {
            fprintf(stderr,
              "Can't find BSR_DEF_DEL_MCELL_LIST record in cell 0\n");
            exit(3);
        }

        print_BMT_page_header(bmtbfp, bsMPgp, flags & VFLG);
        print_cell(bsMCp, MCELL_LIST_CELL, flags & VFLG, version);

        prevMCId.page = 0;
        prevMCId.cell = 0;
        curMCId = delLinkp->nextMCId;
        while ( curMCId.page != 0 || curMCId.cell != 0 ) {
            ret = read_page(bmtbfp, curMCId.page);
            if ( ret != OK ) {
                fprintf(stderr, "process_ddl: read_page page %d failed.\n",
                  curMCId.page);
                exit(2);
            }

            bsMPgp = (bsMPgT*)bmtbfp->pgBuf;
            bsMCp = &bsMPgp->bsMCA[curMCId.cell];
            if ( find_bmtr(bsMCp, BSR_XTNTS, (char**)&bsXtntp) != OK ) {
                fprintf(stderr, "Can't find BSR_XTNTS record in cell\n");
                exit(3);
            }

            if ( bsXtntp->delLink.prevMCId.page != prevMCId.page ||
                 bsXtntp->delLink.prevMCId.cell != prevMCId.cell )
            {
                fprintf(stderr,
                  "process_ddl: Bad deffered delete list back pointer.\n");
                fprintf(stderr,
                  "Back pointer (%d %d) at page %d cell %d, should be %d %d\n",
                  bsXtntp->delLink.prevMCId.page,
                  bsXtntp->delLink.prevMCId.cell,
                  curMCId.page, curMCId.cell, prevMCId.page, prevMCId.cell);
            }

            prevMCId = curMCId;
            nextMCId = bsXtntp->delLink.nextMCId;

            bmtbfp->fileTag = bsMCp->tag.num;
            bmtbfp->setTag = bsMCp->bfSetTag.num;
            process_chain(bmtbfp, curMCId.page, curMCId.cell, flags);

            curMCId = nextMCId;
        }
    }

    exit(0);
}

/******************************************************************************/
/* Find the vdi from a file.
** If the file is a RBMT just find the vdi in the BSR_VD_ATTR record.
** If the file is a BMT file, look for mcell chains whose vdi will indicate
** the vdi of this file.
*/
find_n_set_vdi(bitFileT *bfp)
{
    int ret;
    int version;
    bsMCT *bsMCp;
    bsVdAttrT *bsVdAttrp;
    bsXtntRT *bsXtntRp;
    bsShadowXtntT *bsShadowXtntp;
    bsXtraXtntRT *bsXtraXtntRp;
    vdIndexT vdi;
    bfTagT fTag;
    bfTagT sTag;
    int pn;
    int cn;
    int i;
    char notHere[BS_MAX_VDI];
    int nextPg;
    bfMCIdT nextMCId;
    int type;
    bsMPgT *bsMPgp;

    assert(bfp->type & FLE);

    ret = read_page(bfp, 0);
    if ( ret != OK ) {
        fprintf(stderr, "find_n_set_vdi: read_page page 0 failed.\n");
        exit(2);
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    version = bsMPgp->megaVersion;
    if ( version < FIRST_RBMT_VERSION ) {
        /* This is a BMT file (there is no RBMT in V3). */
        /* The vdi is in the BSR_VD_ATTR record in cell 4. */

        bsMCp = &bsMPgp->bsMCA[BFM_BMT_EXT_V3];   /* cell 4 */

        if ( find_bmtr(bsMCp, BSR_VD_ATTR, (char**)&bsVdAttrp) != OK ) {
            fprintf(stderr, "Can't find BSR_VD_ATTR record in cell 4.\n");
            return;   /* leave vdi = 0 */
        }

        vdi = bsVdAttrp->vdIndex;
        goto set_vdi;

    } else {
        bsMCp = &bsMPgp->bsMCA[BFM_RBMT_EXT];   /* cell 6 */

        /* This may be a BMT file or a RBMT file. Only a RBMT file will */
        /* have a BSR_VD_ATTR record in mcell 6. */
        if ( find_bmtr(bsMCp, BSR_VD_ATTR, (char**)&bsVdAttrp) == OK ) {
            vdi = bsVdAttrp->vdIndex;
            goto set_vdi;
        }
    }

    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        notHere[i] = FALSE;
    }

    /* look for next extent records (not next general mcells) */
    for ( pn = 0; pn < bfp->pages; pn++ ) {
        ret = read_page(bfp, pn);
        if ( ret != OK ) {
            fprintf(stderr, "find_n_set_vdi: read_page page %d failed.\n", pn);
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;

        for ( cn = 0; cn < BSPG_CELLS; cn++ ) {
            vdi = 0;
            bsMCp = &bsMPgp->bsMCA[cn];
            if ( find_bmtr(bsMCp, BSR_XTNTS, (char**)&bsXtntRp) == OK ) {
                if ( bsXtntRp->chainVdIndex ) {
                    nextMCId = bsXtntRp->chainMCId;
                    vdi = bsXtntRp->chainVdIndex;
                    fTag = bsMCp->tag;
                    sTag = bsMCp->bfSetTag;
                    nextPg =
                    bsXtntRp->firstXtnt.bsXA[bsXtntRp->firstXtnt.xCnt-1].bsPage;
                    if ( FIRST_XTNT_IN_PRIM_MCELL(version, bsXtntRp->type) ) {
                        type = BSR_XTRA_XTNTS;
                    } else {
                        type = BSR_SHADOW_XTNTS;
                    }
                }
            }

            ret = find_bmtr(bsMCp, BSR_SHADOW_XTNTS, &(char*)bsShadowXtntp);
            if ( ret == OK ) {
                if ( bsMCp->nextVdIndex ) {
                    vdi = bsMCp->nextVdIndex;
                    nextMCId = bsMCp->nextMCId;
                    fTag = bsMCp->tag;
                    sTag = bsMCp->bfSetTag;
                    nextPg = bsShadowXtntp->bsXA[bsShadowXtntp->xCnt-1].bsPage;
                    type = BSR_XTRA_XTNTS;
                }
            }

            ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, &(char*)bsXtraXtntRp);
            if ( ret == OK ) {
                if ( bsMCp->nextVdIndex ) {
                    vdi = bsMCp->nextVdIndex;
                    nextMCId = bsMCp->nextMCId;
                    fTag = bsMCp->tag;
                    sTag = bsMCp->bfSetTag;
                    nextPg = bsXtraXtntRp->bsXA[bsXtraXtntRp->xCnt-1].bsPage;
                    type = BSR_XTRA_XTNTS;
                }
            }

            if ( vdi && notHere[vdi] == FALSE &&
                 nextMCId.page < bfp->pages )
            {
                ret = read_page(bfp, nextMCId.page);
                if ( ret != OK ) {
                    continue;
                }

                bsMPgp = (bsMPgT*)bfp->pgBuf;
                bsMCp = &bsMPgp->bsMCA[nextMCId.cell];
                if ( bsMCp->tag.num == fTag.num &&
                     bsMCp->tag.seq == fTag.seq &&
                     bsMCp->bfSetTag.num == sTag.num &&
                     bsMCp->bfSetTag.seq == sTag.seq )
                {
                    if ( type == BSR_SHADOW_XTNTS ) {
                        ret =
                      find_bmtr(bsMCp, BSR_SHADOW_XTNTS, &(char*)bsShadowXtntp);
                        if ( ret == OK ) {
                            if ( nextPg == bsShadowXtntp->bsXA[0].bsPage ) {
                                goto set_vdi;
                            }
                        }
                    } else if ( type == BSR_XTRA_XTNTS ) {
                        ret =
                         find_bmtr(bsMCp, BSR_XTRA_XTNTS, &(char*)bsXtraXtntRp);
                        if ( ret == OK ) {
                            if ( nextPg == bsXtraXtntRp->bsXA[0].bsPage ) {
                                goto set_vdi;
                            }
                        }
                    }
                }
            }
        }
    }

set_vdi:
    bfp->pvol = vdi;
}
