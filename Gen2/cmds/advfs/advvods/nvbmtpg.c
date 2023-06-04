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
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <assert.h>
#include <advfs/bs_public.h>         /* needed for ms_logger.h */
#include <advfs/ms_public.h>         /* needed for ms_logger.h */
#include <advfs/ms_logger.h>         /* needed for bs_ods.h */
#include <advfs/ftx_public.h>        /* needed for bs_ods.h */
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>
#include "vods.h"

/* public prototypes */

void get_fileset(bitFileT *bfp, bitFileT *setBfp, char 
		 *argv[], int argc, int *optind, int flags);

/* private prototypes */

static int scan_cell_for_block(bitFileT*, bfMCIdT, int64_t, int);
static int scan_cell_for_tag(bitFileT*, bfMCIdT, int64_t, int);
static void print_RBMT_summary(bitFileT *bfp, int flgs);
static void print_BMT_summary(bitFileT *bfp, int flgs);
static void print_BMT_freecells(bitFileT* bfp, int flgs);
static int print_chain(bitFileT *bfp, bfMCIdT mcell, bfTagT *tag, int flgs);
static int process_chain(bitFileT *bfp, bfMCIdT mcell, int flgs);
static void save_bmt(bitFileT *bfp, char *argv[], int argc, 
		     int optind, int flags);
static int find_n_set_vdi(bitFileT *bfp);

/* Command line processing protos... */

static void process_summary(bitFileT *bfp, char *argv[], int argc, 
			    int optind, int flgs);
static void process_all(bitFileT *bfp, char *argv[], int argc, 
			int optind, int flags);
static void process_block(bitFileT *bfp, char *argv[], int argc, 
			  int optind, int flags);
static void process_file(bitFileT *bfp, bitFileT *setBfp, 
			 char *argv[], int argc, int optind, int flags);
static void process_page(bitFileT *bfp, int64_t longnum, char *argv[], 
			 int argc, int optind, int flags);
static void process_ddl(bitFileT *bfp, char *argv[], int argc, 
			int optind, int flags);
static void search(bitFileT *bfp, char *argv[], int argc, 
		   int optind, int flags);
static void search_tag(bitFileT *bfp, char *argv[], int argc, 
		       int optind, int flags);
static void search_block(bitFileT *bfp, char *argv[], int argc, 
			      int optind, int flags);


void
bmt_main(argc, argv)
int argc;
char *argv[];
{
    int ret;
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    bitFileT  setBf;
    bitFileT  *setBfp = &setBf;
    int flags = 0;
    int64_t longnum;
    extern int optind;

    init_bitfile(bfp);

    /* if the subcommand is for rbmt, set that here needed by
       resolve_src() */

    if (!strcmp(argv[0], "rbmt"))
      flags |= RBMT;

    /* Set tag to reserved so that if it's a saved file we can check the */
    /* length in resolve_fle. */

    BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, BFM_RBMT );

    ret = resolve_src(argc, argv, &optind, "rvf:", bfp, &flags);

    /* "-b" must work even on a badly corrupted FS, so service block
       processing without checking resolve_src() error return status */

    if ( bfp->type & VOL &&
         argc > optind && strncmp(argv[optind], "-b", 2) == 0 )
    {
        /* process_block needs bfp->dmn, bfp->pmcell.volume, bfp->dmn->vols[] */
        process_block(bfp, argv, argc, optind, flags & VFLG);
        /* NOT REACHED */
    }

    /* Again check ret from resolve_src().  At this point if there
       are serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    if ( bfp->type != FLE ) {
	/* go load the BMT extents into the bfp struct*/
        if ( find_BMT(bfp, flags) != OK ) {
            fprintf(stderr, "find_BMT failed\n");
            exit(1);
        }
    } else {
        if (find_n_set_vdi(bfp) != OK) {
            fprintf(stderr, "Cannot find volume index, file may not be a dump file.\n");
            exit(1);
	}
    }

    /*
    ** At this point if bfp->type & FLE then fd set, dmn is not malloced.
    ** If bfp->type & (DMN | VOL) then dmn, vols[] are set.
    ** If bfp->type & VOL then bfp->pmcell.volume, ppage, pcell are set.
    */

    /* if no other arguments, or a -n is next, print a summary for BMT
       on one volume */

    if ( argc == optind || strcmp(argv[optind], "-n") == 0 ) {
        process_summary(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }


    /* if the user specified a storage domain (not a volume or
       dumpfile) and there's only one other argument and it's not an
       option, the argument must be a file name. */

    if ( bfp->type & DMN && (argc == optind+1) && *(argv[optind]) != '-') {
	get_fileset(bfp, setBfp, argv, argc, &optind, flags & VFLG);
	process_file(bfp, setBfp, argv, argc, optind, flags & VFLG);
    }

    /* all the rest of the options require a volume specifier.  If the
       user entered a domain name without a volume index, complain. */

    if ( (bfp->type & DMN) && bfp->pmcell.volume == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    /* display all the bmt */
    if ( strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags & VFLG);
        /* NOT REACHED */
    }

    if ( strcmp(argv[optind], "-l") == 0 ) {

	if ( flags & RBMT ) {
	    fprintf(stderr, "No deferred delete list in the RBMT.\n");
	    exit(1);
	}

        process_ddl(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* fetch the snapshot's tag file (fileset tag file) */
    if ( strcmp(argv[optind], "-S") == 0 || strcmp(argv[optind], "-t") == 0) {

	if (flags & RBMT) {
            fprintf(stderr, "The RBMT has no snapshot specific data\n");
	    exit(1);
	}

	get_fileset(bfp, setBfp, argv, argc, &optind, flags & VFLG);
	if ((strcmp(argv[optind], "-L") != 0) &&
	    (strcmp(argv[optind], "-d") != 0) ) {
	    process_file(bfp, setBfp, argv, argc, optind, flags & VFLG);
	    /* NOT REACHED */
	}
    }

    /* dump the RBMT or BMT to a file */
    if ( strcmp(argv[optind], "-d") == 0 ) {
        save_bmt(bfp, argv, argc, optind, flags & VFLG);
        /* NOT REACHED */
    }

    /* search for block or tag */
    if ( strcmp(argv[optind], "-L") == 0 ) {
        bfp->setTag.tag_num = 0;
        bfp->setTag.tag_seq = 0;
        search(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    /* get page number, if it's not a number it must be a set name */
    if ( strcmp(argv[optind], "-p") == 0 ) {
        optind++;

	if (optind == argc) {
            fprintf(stderr, "The '-p' option requires a page number arg\n");
	    exit(1);
	}

	if ( getnum(argv[optind], &longnum) == OK ) {
	    process_page(bfp, longnum, argv, argc, optind, flags);
	    /* NOT REACHED */
	}
    }

    fprintf(stderr, "unknown option '%s'\n", argv[optind]);
    usage();
    /* NOT REACHED */

} /* bmt_main */

/******************************************************************************/
/* Given bfp->pmcell.volume, this routine loads the extents for the selected (R)BMT
**
** Input: bfp->pmcell.volume - volume on which to find the BMT
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

    bfp->setTag = staticRootTagDirTag;
    bfp->pmcell.page = 0;

    if ( bfp->pmcell.volume == 0 ) {
        /* we haven't selected a volume. default to vdi 1 */
        BS_BFTAG_RSVD_INIT( bfp->fileTag, 1, FETCH_MC_CELL(bfp->pmcell));
        bfp->pmcell.cell = 0;
        return OK;
    }

    assert(!(bfp->type & FLE));
    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->dmnVers.odv_major);

    bfp->pmcell.cell = flags & RBMT ? BFM_RBMT : BFM_BMT;
    BS_BFTAG_RSVD_INIT( bfp->fileTag, FETCH_MC_VOLUME(bfp->pmcell), 
            FETCH_MC_CELL(bfp->pmcell));

    if ( bfp->dmn->vols[bfp->pmcell.volume] ) {
        ret = load_xtnts(bfp);
        /* The extents for the (R)BMT are already loaded in */
        /* bfp->dmn->vols[]->(r)bmt. load_xtnts() will just copy them. */
        /* This can't fail. */
        assert(ret == OK);
        return OK;
    }

    return 1;

} /* find_BMT */

/***************************************************************************/
/* fill in the "setp" below with the fileset tag file for the fileset
** (or snapshot) specified on the command line.
**
** Input: bfp->dmn is set, bfp->type is set
** Input: setp is an allocated bitfile struct (setp)
** Input: argv is a fileset (or tag) and possibly a file path (or tag)
** Input: flags:VFLG - display verbose contents of mcell(s)
** Output: a bitfile to the appropriate set (or snapshot) tag file (step)
** Return: never returns
*/

void
get_fileset(bitFileT *bfp,
	    bitFileT *setp,  /* filled in below */
	    char *argv[], 
	    int argc, 
	    int *optind, 
	    int flags)
{
    int ret;
    bitFileT setBf;
    bitFileT *setBfp = &setBf;
    bitFileT rootBf;
    bitFileT *rootBfp = &rootBf;
    bitFileT *bmt;

    assert(bfp->dmn);
    newbf(bfp, setBfp);
    newbf(bfp, rootBfp);

    /* load xtnts of root TAG file */
    if ( find_TAG(rootBfp) != OK ) {
        fprintf(stderr, "Could not find the root TAG file\n");
        exit(2);
    }

    ret = get_set(rootBfp, argv, argc, optind, setBfp);
    if ( ret != OK ) {
        if ( ret == BAD_SYNTAX ) {
            usage();
        } else {
            exit(1);
        }
    }

    *setp = *setBfp;

} /* get_fileset */


/******************************************************************************/
/* Print a summary of the RBMT or the BMT on one volume or all the
** volumes in the domain. The summary of the RBMT prints the number of
** pages and the number of free mcells. The summary of the BMT prints
** the number of pages and the number of extents used to describe the
** pages. The -n flag will display the total free mcells in the BMT
** (for a suammry of the BMT).
**
** Input: bfp->dmn
**        bfp->pmcell.volume - specifies volume to summarize. 0 means all volumes
** Output: none
** Return: does not return
*/
static void
process_summary(bitFileT *bfp, char *argv[], int argc, int optind, int flgs)
{
    if ( argc > optind +1 ) {
        fprintf(stderr, "Too many arguments\n");
        usage();
    }

    if ( argc > optind && strcmp(argv[optind], "-n") == 0 ) {
        flgs |= NFLG;
    }

    if ( flgs & RBMT ) {
        if ( flgs & NFLG ) {
            fprintf(stderr, "-n flag ignored for RBMT summary\n");
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
** Input: bfp->pmcell.volume - volume to summarize, 0 means all volumes
** Input: bfp->dmn->vols[] filled in if DMN
** Output: none
** Return: does not return
*/
/* TODO: get_next_mcell checks cell pointer. don't do it here */
static void
print_RBMT_summary(bitFileT *bfp, int flgs)
{
    int ret;
    vdIndexT vdi;
    int extents;
    bsXtntRT *bsXtntRecp;
    int cellCnt;
    int freeCells;
    int stop = FALSE;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    bfMCIdT prevMcell;
    bfMCIdT mcid;

    if ( bfp->type & FLE ) {

        ret = read_page(bfp, 0);
        if ( ret != OK ) {
            fprintf(stderr, "print_RBMT_summary: read_page failed\n");
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;
        bsMCp = &bsMPgp->bsMCA[BFM_BMT];
        if ( BS_BFTAG_NULL(bsMCp->mcTag) ) {
            fprintf(stderr, "This is a BMT file. Do not use -R to view the BMT.\n");
            exit(2);
        }
    }

    for ( vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
            bfp->pmcell.page = 0;
            bfp->pmcell.cell = BFM_RBMT;
            vdi = FETCH_MC_VOLUME(bfp->pmcell);
        } else {
            BS_BFTAG_RSVD_INIT( bfp->fileTag, vdi, BFM_RBMT);
            if ( bfp->type & VOL ) {
                /* if pvol set the first time thru (bfp->fd == -1),
                 * then only display summary for one volume
                 */
                stop = TRUE;
                vdi = FETCH_MC_VOLUME(bfp->pmcell);
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pmcell.volume = vdi;

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
            printf("There are %ld pages in the RBMT ",
                bfp->pages);
        }
        if ( bfp->type & FLE ) {
            printf("in this file.\n");
        } else {
            printf("on this volume.\n");
        }

        freeCells = ((bsMPgT *)bfp->pgBuf)->bmtFreeMcellCnt;

        /* Read primary mcell for this file from the RBMT.
         * Get the chain mcell from the BSR_XTNTS record in the primary mcell
         */
        ret = get_chain_mcell(bfp, &mcid);
        if ( ret != OK )
            continue;

        if ( mcid.page != 0 ) {
            fprintf(stderr, "Bad mcell RBMT vol %d page %ld cell %d: ",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            fprintf(stderr, "First chain mcell is not on page 0\n");
        }

        extents = 1;
        cellCnt = 1;

        prevMcell = bfp->pmcell;

        while ( mcid.volume != 0 )
        {
            if (FETCH_MC_VOLUME(mcid)  != vdi ) {
                fprintf(stderr, "Bad RBMT mcell vol %d page %ld cell %d: ",
                  prevMcell.volume, prevMcell.page, prevMcell.cell);
                fprintf(stderr, "bad nextVdIndex. expect %d read %d\n",
                  vdi, mcid.volume);
                break;
            }
            if ( mcid.page >= bfp->pages ) {
                fprintf(stderr, "Bad RBMT mcell vol %d page %ld cell %d: ",
                  prevMcell.volume, prevMcell.page, prevMcell.cell);
                fprintf(stderr, "Bad nextMCId.page (%ld). ", mcid.page);
                fprintf(stderr, "Valid range is 0 - %ld.\n", bfp->pages - 1);
                break;
            }
            if ( mcid.cell != BSPG_CELLS ) {
                fprintf(stderr, "Bad RBMT mcell vol %d page %ld cell %d: ",
                  prevMcell.volume, prevMcell.page, prevMcell.cell);
                fprintf(stderr, "Bad nextMCId.cell (%d). ", mcid.cell);
                if ( mcid.cell > BSPG_CELLS ) {
                    fprintf(stderr, "Valid range is 0 - %d.\n", BSPG_CELLS );
                } else {
                    fprintf(stderr, "Expected %d.\n", BSPG_CELLS );
                }
            }

            if ( read_page(bfp, (bs_meta_page_t)FETCH_MC_PAGE(mcid)) != OK ) {
                fprintf(stderr, "read_page failed\n");
                exit(2);
            }
            bsMPgp = (bsMPgT*)bfp->pgBuf;
            bsMCp = &bsMPgp->bsMCA[mcid.cell];

            /* For RBMT, walk a chain of BSR_XTRA_XTNTS records;  there will
             * not be a BSR_XTRA_XTNTS rec in the last page!
             */
            ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)(&bsXtntRecp));
            if ( ret == ENOENT ) {
                break;
            }
            if ( ret != OK ) {
                fprintf(stderr,
                  "Bad RBMT mcell at volume, page, cell: %d %ld %d\n",
                  mcid.volume, mcid.page, mcid.cell);
                fprintf(stderr, "No BSR_XTRA_XTNTS record\n");
                break;
            }
            cellCnt++;
            extents++;

            ret = get_next_mcell(bfp, &mcid);
            if ( ret != OK ) {
                fprintf(stderr, "get_next_mcell failed\n");
                break;
            }
        }

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
/* Prints a summary of the BMT on a volume (or saved file) or all the
** volumes.  The summary gives the number of pages in the BMT and the
** number of extents and mcell used to describe the BMT. The number of
** extents is not given if bfp is a saved file. In that case the
** extents describing the BMT is in another file. If the -n option is
** set, report the total number of free mcells in the BMT.
**
** Input: bfp->pmcell.volume - volume to summarize, 0 means all volumes
** Input: flgs:NFLG - print total number of BMT free mcells
** Input: bfp->dmn->vols[] filled in if DMN
** Output: none
** Return: does not return
*/
/* TODO: get_next_mcell checks cell pointer. don't do it here */

static void
print_BMT_summary(bitFileT *bfp, int flgs)
{
    int ret;
    vdIndexT vdi;
    int extents;
    bsXtraXtntRT *bsXtraXtntRp;
    int cellCnt;
    int freeCells;
    bitFileT *bmtp;                     /* can point to BMT or RBMT */
    int stop = FALSE;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    bfMCIdT prevMcell;
    bfMCIdT mcid;

    if ( bfp->type & FLE ) {

        ret = read_page(bfp, 0);
        if ( ret != OK ) {
            fprintf(stderr, "print_BMT_summary: read_page failed\n");
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;

	bsMCp = &bsMPgp->bsMCA[0];
	if ( BS_BFTAG_RSVD(bsMCp->mcTag) && BS_BFTAG_RBMT(bsMCp->mcTag) ) {
	    fprintf(stderr, "This is an RBMT file. Use -R to view the RBMT.\n");
	    exit(2);
	}
    }

    for ( vdi = 1; !stop && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->type & FLE ) {
            stop = TRUE;
            bfp->pmcell.page = 0;
            bfp->pmcell.cell = BFM_BMT;
            vdi = FETCH_MC_VOLUME(bfp->pmcell);
        } else {
            BS_BFTAG_RSVD_INIT( bfp->fileTag, vdi, BFM_BMT);
            if ( bfp->type & VOL ) {
                /* if pvol set the first time thru (bfp->fd == -1),
                 * then only display summary for one volume
                 */
                stop = TRUE;
                vdi = FETCH_MC_VOLUME(bfp->pmcell);
                assert(bfp->dmn);
                assert(bfp->dmn->vols[vdi]);
            } else if ( bfp->dmn->vols[vdi] == NULL ) {
                continue;
            }

            bfp->pmcell.volume = vdi;

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
            printf("There are %ld pages in the BMT ",
                bfp->pages);
        }
        if ( bfp->type & FLE ) {
            printf("in this file.\n");
        } else {
            printf("on this volume.\n");
        }

        extents = 1;
        cellCnt = 1;

        freeCells = ((bsMPgT *)bfp->pgBuf)->bmtFreeMcellCnt;

        /* If we have access to the xtnts count them */
        if ( !(bfp->type & FLE) ) {
	    assert(bfp->dmn);
	    assert(bfp->dmn->vols[vdi]);
	    bmtp = bfp->dmn->vols[vdi]->rbmt;
	    assert(bmtp);

            /* Read primary mcell for this file from the RBMT. Get the
             * chain mcell from the BSR_XTNTS record in the primary mcell
             */
            ret = get_chain_mcell(bfp, &mcid);
            if ( ret != OK )
                continue;

            prevMcell = bfp->pmcell;

            while ( mcid.volume != 0 ) {
                char *filename = "RBMT";

                if ( FETCH_MC_VOLUME(mcid) != vdi ) {
                    fprintf(stderr, "Bad %s mcell vol %d page %ld cell %d: ",
                      filename, prevMcell.volume, prevMcell.page, prevMcell.cell);
                    fprintf(stderr, "bad nextVdIndex. expect %d read %d\n",
                      vdi, mcid.volume);
                    break;
                }

		if ( mcid.page > bmtp->pages ) {
		    fprintf(stderr, "Bad BMT mcell vol %d page %ld cell %d: ",
			    prevMcell.volume, prevMcell.page, prevMcell.cell);
		    fprintf(stderr, "Bad nextMCId.page (%ld). ", mcid.page);
		    fprintf(stderr, "Valid range 0 - %ld\n", bmtp->pages - 1);
		    break;
                }

                if ( mcid.cell > BSPG_CELLS ) {
                    fprintf(stderr, "Bad RBMT mcell vol %d page %ld cell %d: ",
                      prevMcell.volume, prevMcell.page, prevMcell.cell);
                    fprintf(stderr, "Bad nextMCId.cell (%d). ", mcid.cell);
                    fprintf(stderr, "Valid range is 0 - %d.\n", BSPG_CELLS );
                    break;
                }

                if ( read_page(bmtp, (bs_meta_page_t)FETCH_MC_PAGE(mcid)) != OK ) {
                    fprintf(stderr, "read_page failed\n");
                    exit(2);
                }
                bsMPgp = (bsMPgT*)bmtp->pgBuf;
                bsMCp = &bsMPgp->bsMCA[mcid.cell];

                ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)(&bsXtraXtntRp));
                if ( ret != OK ) {
                    fprintf(stderr, "no BSR_XTRA_XTNTS\n");
                    break;
                }

                cellCnt++;
                extents += bsXtraXtntRp->xCnt - 1;

                ret = get_next_mcell(bfp, &mcid);
                if ( ret != OK )
                    break;
            }
        }

        printf("The BMT uses %d extents (out of %d) in %d %s.\n",
                 extents,  1 + (cellCnt - 1) * BMT_XTRA_XTNTS, cellCnt,
                 cellCnt == 1 ? "mcell" : "mcells");

        if ( !(flgs & NFLG) ) {
             continue;
        }

        print_BMT_freecells(bfp, flgs);
    }

    return;
}

/******************************************************************************/
/* Read the entire BMT and display the total number of free mcells and
** the number of BMT pages with free mcells. While scanning the BMT
** pages print a progress report every 100 pages. A large BMT can take
** several minutes to read.
**
** Input: bfp->xmap or bfp->fd is set up
** Output: none
** Return: never returns
*/
/*
 * Define the location of the mcell that contains the head of the
 * deferred delete list. from bs_delete.h
 */
/* FIX. including bs_delete.h caused problems, so use local defines */
#define MCELL_LIST_PAGE 0
#define MCELL_LIST_CELL 0

static void
print_BMT_freecells(bitFileT* bfp, int flgs)
{
    int ret;
    bsMCT *cellp;
    int cellCnt;
    bsMcellFreeListT *mcellFreeListp;
    bs_meta_page_t pg;
    int64_t cnt;
    bs_meta_page_t free_list_head_page;
    bsMPgT *bsMPgp;

    fflush(stdout);
    cellCnt = 0;

    /* Print some info about the MCELL_FREE_LIST (-n option) */
    free_list_head_page = MCELL_LIST_PAGE;

    if ( read_page(bfp, free_list_head_page) != OK ) {
        fprintf(stderr, "read page %d failed\n", 1);
        exit(1);
    }

    bsMPgp = (bsMPgT *)bfp->pgBuf;
    /* The head of the free list is in cell 0. */
    cellp = &bsMPgp->bsMCA[MCELL_LIST_CELL];
    ret = find_bmtr( cellp, BSR_MCELL_FREE_LIST, (char**)(&mcellFreeListp));
    if ( ret != OK ) {
        fprintf(stderr, "Cannot find BSR_MCELL_FREE_LIST\n");
        exit(0);
    }

    pg = mcellFreeListp->headPg;
    if ( flgs & VFLG ) {
        printf("first free pg %ld\n", pg);
    }

    cnt = 0;
    while ( 1 ) {
        /* free list and uses pg -1 to mark the end */
        if ( pg == BMT_PG_TERM ) {
            break;
        }

        cnt++;
        if ( !(flgs & VFLG) ) {
            if ( cnt % 100 == 0 ) {
                printf(CLEAR_LINE);
                printf(" ...scanning the free list, %ld pages read\r",
		       cnt);
            }
        }

        if ( read_page(bfp, pg) != OK ) {
            fprintf(stderr,"read page BMT page %ld failed\n", pg);
            break;
        }

        bsMPgp = (bsMPgT *)bfp->pgBuf;
        if ( flgs & VFLG ) {
            printf("BMT pg %ld has %d free mcells. Next free pg %ld\n",
              pg, bsMPgp->bmtFreeMcellCnt, bsMPgp->bmtNextFreePg);
        }
        /* TODO. if VFLG print page num and num of free cells */

        cellCnt += bsMPgp->bmtFreeMcellCnt;
        pg = bsMPgp->bmtNextFreePg;
    }

    if ( !(flgs & VFLG) ) {
        fprintf(stderr, CLEAR_LINE);
    }

    printf("There are %ld pages on the free list with a total of %d free mcells.\n", cnt, cellCnt);
}

/******************************************************************************/
/* Reads the (R)BMT and writes it to a file.
**
** Input: bfp->xmap or bfp->fd is set up
** Input: argv[optind] is the file name
** Output: none
** Return: never returns
*/

static void
save_bmt(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile = NULL;

    optind++;
    if ( bfp->type & DMN && bfp->pmcell.volume == 0 ) {
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

static void
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;
    bs_meta_page_t pagenum;

    optind++;
    if ( bfp->type & DMN && bfp->pmcell.volume == 0 ) {
        fprintf(stderr, "Missing volume index number\n");
        exit(1);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    ret = 0;
    for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
        ret |= print_page(bfp, pagenum, CELL_TERM, flags & VFLG);
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
** Input: bfp->dmn->vols[bfp->pmcell.volume]->blocks - if non zero, used for arg check
** Input: argv[optind] = -b <block_number> [cell_number]
** Output: none
** Return: never returns
*/

static void
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    uint64_t blknum;
    uint16_t mcellnum = CELL_TERM;         /* default: print all mcells */
    int64_t longnum;
    int ret;

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
                exit(BAD_PARAM);
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
            exit(BAD_PARAM);
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

    if ( blknum / ADVFS_METADATA_PGSZ_IN_FOBS * ADVFS_METADATA_PGSZ_IN_FOBS != blknum ) {
        fprintf(stderr, "BMT pages are on %d block boundaries.\n",
          ADVFS_METADATA_PGSZ_IN_FOBS);
        blknum = blknum / ADVFS_METADATA_PGSZ_IN_FOBS * ADVFS_METADATA_PGSZ_IN_FOBS;
        fprintf(stderr,
          "Block number changed to next lower multiple of %d (%ld).\n",
          ADVFS_METADATA_PGSZ_IN_FOBS, blknum);
    }

    if ( argc > optind ) {
        if ( getnum(argv[optind], &longnum) != OK ) {
            fprintf(stderr, "Badly formed mcell number: \"%s\"\n",
              argv[optind]);
            exit(BAD_PARAM);
        }
        if ( longnum < 0 || longnum >= BSPG_CELLS ) {
            fprintf(stderr, "Invalid value (%ld) for mcell. ", longnum);
            fprintf(stderr, "Valid mcell numbers range from 0 to %d.\n", BSPG_CELLS);
            exit(BAD_PARAM);
        }
        optind++;
        mcellnum = (uint16_t)longnum;
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    bfp->pgLbn = blknum;
    (void)print_block(bfp, mcellnum, flags);

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
print_block(bitFileT *bfp, uint16_t mcellnum, int vflg)
{
    bsMPgT page;
    uint16_t cn;
    int results;
    uint64_t lbn = bfp->pgLbn;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    if ( lseek(bfp->fd, (lbn) * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "print_block: lseek to block %ld failed", lbn);
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

    bfp->pgVol = FETCH_MC_VOLUME(bfp->pmcell);
    bfp->pgLbn = lbn;
    bfp->pgNum = BMT_PG_TERM;
    print_mcell_header(bfp, &page, vflg);

    if ( mcellnum == CELL_TERM ) {
        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            print_cell(&page.bsMCA[cn], cn, vflg);
        }
    } else {
        print_cell(&page.bsMCA[mcellnum], mcellnum, vflg | ONECELL);
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
** Input: setBfp is the set (or snap) tag file 
** Input: argv is a fileset (or tag) and possibly a file path (or tag)
** Input: flags:VFLG - display verbose contents of mcell(s)
** Output: writes to stdout
** Return: never returns
*/

static void
process_file(bitFileT *bfp,
	     bitFileT *setBfp, 
	     char *argv[], 
	     int argc, 
	     int optind, 
	     int flags)
{
    int ret;
    bfTagT tag;
    int64_t longnum;
    bitFileT *bmt;

    if ( bfp->type & FLE ) {
        fprintf(stderr, "Cannot specify set and file for a saved BMT.\n");
        usage();
    }

    if ( argc == optind )
        bfp = setBfp;

    if ( argc > optind ) {
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
            } else if ( bfp->pmcell.volume == 0 || bfp->dmn->vols[bfp->pmcell.volume] == NULL ) {
                exit(1);
            }
        } else if ( bfp->setp->origSetp != NULL && bfp->pmcell.volume == 0 ) {
            printf("This clone file has no metadata.\n");
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

            ret = process_chain(bfp, bfp->pmcell, flags);
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
    assert(bfp->dmn->vols[bfp->pmcell.volume]);
    /* TODO. use find_BMT */
    if (flags & RBMT) {
        bmt = bfp->dmn->vols[bfp->pmcell.volume]->rbmt;
    } else {
        bmt = bfp->dmn->vols[bfp->pmcell.volume]->bmt;
    }
    bfp->xmap = bmt->xmap;
    bfp->pages = bmt->pages;
    bfp->fileTag = bmt->fileTag;
    /* FIX. need to set setTag?? */

    ret = print_page(bfp, (bs_meta_page_t)FETCH_MC_PAGE(bfp->pmcell),
                          (int)FETCH_MC_CELL(bfp->pmcell), flags & VFLG);

    exit(ret);

} /* process_file */

/*****************************************************************************/
/* Possibly select a mcell from the passed in (R)BMT file/ page number and
** display them. If a mcell is specified, the -c flag will display all
** the mcell chain.
**
** Input: bfp defines a (R)BMT file. xmap or fd set up, pvol set up.
** Input: longnum - page number
** Input: argv, argc, optind = [cell_number [-c]]
** Output: writes to stdout
** Return: never returns
*/

static void
process_page(bitFileT *bfp, bs_meta_page_t longnum, char *argv[], int argc, 
	     int optind, int flags)
{
    bs_meta_page_t pagenum;
    uint32_t mcellnum = CELL_TERM;
    int ret;

    optind++;
    if ( (uint64_t)longnum >= bfp->pages ) {
        fprintf(stderr, "Invalid %s page number (%ld). ", 
		flags & RBMT ? "RBMT" : "BMT", longnum);
        fprintf(stderr, "Valid page numbers are 0 through %ld.\n",
          bfp->pages - 1);
        exit(1);
    }
    pagenum = longnum;

    /* At this point we have identified a page on a particular disk. */
    if ( argc > optind && strcmp(argv[optind], "-n") == 0 ) {
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
        if ( (uint64_t)longnum >= BSPG_CELLS ) {
            fprintf(stderr, "Mcell number must be from 0 to %d\n", BSPG_CELLS );
            exit(7);
        }
        mcellnum = longnum;
    }

    bfp->pmcell.page = pagenum;
    bfp->pmcell.cell = mcellnum;

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        if ( bfp->type == FLE ) {
            fprintf(stderr, "-c not allowed on dump files\n");
            exit(1);
        }

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            exit(1);
        }

        ret = process_chain(bfp, bfp->pmcell, flags);
        exit(ret);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        exit(1);
    }

    ret = print_page(bfp, pagenum, mcellnum, flags & VFLG);
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
print_page(bitFileT *bfp, bs_meta_page_t pgnum, uint16_t mcellnum, int vflg)
{
    uint16_t cn;
    bsMPgT *bsMPgp;

    if ( read_page(bfp, pgnum) != OK ) {
        fprintf(stderr, "read page %ld failed\n", pgnum);
        return 1;
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;

    print_mcell_header(bfp, bsMPgp, vflg);

    if ( mcellnum == CELL_TERM ) {
        for ( cn = 0; cn < BSPG_CELLS; cn++) {
            print_cell(&bsMPgp->bsMCA[cn], cn, vflg);
        }
    } else {
        print_cell(&bsMPgp->bsMCA[mcellnum], mcellnum, 
                   vflg | ONECELL);
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
static int
process_chain(bitFileT *bfp, bfMCIdT mcell, int flgs)
{
    int ret;
    bfMCIdT mcid;

    bfp->pmcell = mcell;
    bfp->fileTag.tag_num = 0;   /* So print_chain won't miscompare on the tag */
    bfp->fileTag.tag_seq = 0;

    /*
     * Print the mcell chain linked by the mcell headers.
     */
    ret = print_chain(bfp, bfp->pmcell, &bfp->fileTag, flgs);

    ret = get_chain_mcell(bfp, &mcid);

    if (ret != OK) {
	if (ret == NOT_FOUND) {
	    return OK;
	}

        fprintf(stderr, "get_chain_mcell failed\n");
        return 1;
    }

    if ( mcid.volume ) {
        printf("\nExtent mcells from BSR_XTNTS record chain pointer...\n");
        ret |= print_chain(bfp, mcid, &bfp->fileTag, flgs);
    }

    return ret;
}

/*****************************************************************************/
/* Print the contents of the entire next list of mcells, starting at the
** given mcell. Mcell Id validity is checked at each mcell including the first.
**
** Input: bfp->dmn set up. bfp->fileTag. dmn->vols[] used to check vdi validity
** Input: vn, pn, cn - first in a list of mcells
** Output: writes to stdout
** Return: OK if all went well
**         non zero if list points to a bad vn, pn, cn. Or if read_page fails.
*/
static int
print_chain(bitFileT *bfp, bfMCIdT mcid, bfTagT *tag,
            int flgs)
{
    bsMCT *mcell;
    bitFileT *bmt;
    bfMCIdT lastMcid;
    int ret;
    bfTagT savtag;
    bsMPgT *bsMPgp;

    lastMcid.volume = VOL_TERM;
    lastMcid.page   = PAGE_TERM;

    while ((mcid.volume > 0) && (mcid.volume != VOL_TERM)) {
        if ( bfp->type & FLE ) {
            if ( mcid.volume != bfp->pmcell.volume ) {
                printf("Cannot follow chain\n");
            }
        } else if ( bfp->dmn->vols[mcid.volume] == NULL ) {
            if ( bfp->type & DMN ) {
                printf("No volume %d in storage domain \"%s\".\n",
                  mcid.volume, bfp->dmn->dmnName);
            } else {
                printf("No information about volume %d\n", mcid.volume);
            }
            return 1;
        }

        if ( bfp->type & FLE ) {
            bmt = bfp;
        } else if ( flgs & RBMT ) {
            bmt = bfp->dmn->vols[mcid.volume]->rbmt;
        } else {
            bmt = bfp->dmn->vols[mcid.volume]->bmt;
        }

        if ( FETCH_MC_PAGE(mcid) >= bmt->pages ) {
            fprintf(stderr, "Chain BMT page %ld is invalid. ", mcid.page);
            fprintf(stderr, "Valid pages range from 0 to %ld.\n", bmt->pages-1);
            return BAD_DISK_VALUE;
        }
        if ( FETCH_MC_CELL(mcid) >= BSPG_CELLS ) {
            fprintf(stderr, "Chain BMT mcell number %d is invalid. ", mcid.cell);
            fprintf(stderr, "Valid mcells range from 0 to %d.\n", BSPG_CELLS );
            return BAD_DISK_VALUE;
        }
        if ( mcid.volume != lastMcid.volume || mcid.page != lastMcid.page) {
            ret = read_page(bmt, (bs_meta_page_t)FETCH_MC_PAGE(mcid));
            if ( ret != OK ) {
                fprintf(stderr, "read page BMT page %ld on volume %d failed\n",
                  mcid.page, mcid.volume);
                return ret;
            }

            bsMPgp = (bsMPgT*)bmt->pgBuf;
            bfp->pgVol = bmt->pgVol;
            bfp->pgLbn = bmt->pgLbn;
            bfp->pgNum = bmt->pgNum;
            savtag = bfp->fileTag;
            bfp->fileTag = bmt->fileTag;
            print_mcell_header(bfp, bsMPgp, flgs & VFLG);
            bfp->fileTag = savtag;
        } else
            printf(SINGLE_LINE);

	mcell = &bsMPgp->bsMCA[mcid.cell];

        if ( tag->tag_num && mcell->mcTag.tag_num != tag->tag_num ) {
            fprintf(stderr, "Mcell (%d %ld %d) tag is %ld, expected %ld.\n",
             mcid.volume , mcid.page, mcid.cell, mcell->mcTag.tag_num, bfp->fileTag.tag_num);
        } else
            *tag = mcell->mcTag;

        print_cell(mcell, mcid.cell, flgs | ONECELL);

        lastMcid = mcid;

        mcid = mcell->mcNextMCId;
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
process_free(bitFileT *bfp, bs_meta_page_t pn, int vflg)
{
    vdIndexT vn;
    int i;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint16_t cn;
    int ret;

    if ( !(bfp->type & FLE) ) {
        vn = FETCH_MC_VOLUME(bfp->pmcell);
    } else {
        /* FIX. use vdi from attr */
        vn = 0;
    }

    ret = read_page(bfp, pn);
    if ( ret != OK ) {
        fprintf(stderr,"read page BMT page %ld on volume %d failed\n", pn, vn);
        return ret;
    }
    bsMPgp = (bsMPgT*)bfp->pgBuf;

    print_mcell_header(bfp, bsMPgp, vflg);

    if ( bsMPgp->bmtFreeMcellCnt == 0 ) {
        printf("No free mcells.\n");
        return OK;
    }

    cn = bsMPgp->bmtNextFreeMcell;
    if ( cn >= BSPG_CELLS ) {
        fprintf(stderr,
          "nextfreeMCId.cell (%d) in BMT page %ld on vol %d is > %d\n",
          cn, pn, vn, BSPG_CELLS);
        return 1;
    }

    if ( bsMPgp->bmtFreeMcellCnt > BSPG_CELLS ) {
        fprintf(stderr,
          "freeMcellCnt (%d) in BMT page %ld on vol %d is > %d\n",
          bsMPgp->bmtFreeMcellCnt, pn, vn, BSPG_CELLS);
        return 1;
    }

    for ( i = 0; i < bsMPgp->bmtFreeMcellCnt; i++ ) {
        bsMCp = &bsMPgp->bsMCA[cn];
        print_cell_hdr(bsMCp, cn, vflg);

        cn = bsMCp->mcNextMCId.cell;
        if ( cn >= BSPG_CELLS ) {
            fprintf(stderr,
             "nextMCId.cell (%d) at cell %d in BMT page %ld on vol %d is > %d\n",
              cn, cn, pn, vn, BSPG_CELLS);
            return 1;
        }

#ifdef NOTYET
        /*FIX. errs on the last (valid) mcell in the chain */
        if ( FETCH_MC_PAGE(bsMCp->mcNextMCId) != pn ) {
            fprintf(stderr, "Mcell (%d %ld %d) has bad nextMCId.page (%ld).\n",
              vn, pn, cn, bsMCp->mcNextMCId.page);
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

/****************************************************************************/
/* Search for a given block in the (R)BMT extents on a given volume or
** for a specified tag.  Prints the mcell that conatins the searched
** for information.  
** advvods (r)bmt {fs vdi | vol} -L b=block [-c] 
** advvods (r)bmt {fs vdi | vol} -L t=tag [-c] 
**
** Input: bfp specifies the (R)BMT on a volume for block searchs
** Input: argv, argc, optind - search type and block
** Input: flags:VFLG - print entire contents of found mcell
** Output: writes to stdout
** Return: never returns
*/
static void
search(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;

    optind++;
    if ( argc == optind ) {
        fprintf(stderr, "missing Lookup argument");
        usage();
    }

    if ( strncmp(argv[optind], "b=", 2) == 0 ) {
        search_block(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    if ( strncmp(argv[optind], "t=", 2) == 0 ) {
        search_tag(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    fprintf(stderr, "Bad Lookup arguments\n");
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
static void
search_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int64_t block;
    char *pC;
    int ret;

    if ( bfp->fd == -1 ) {
        fprintf(stderr, "A volume must be specified to lookup a block.\n");
        usage();
    }

    pC = argv[optind];
    pC += strlen("b=");

    if ( *pC == '\0' || *pC == ' ') {
        fprintf(stderr, "missing block number argument");
        usage();
    }

    if ( getnum(pC, &block) != OK ) {
        fprintf(stderr, "badly formed Lookup block number: \"%s\".\n", pC);
        exit(1);
    }
    optind++;

    if ( bfp->type != FLE && bfp->dmn->vols[bfp->pmcell.volume]->blocks != 0 ) {
        if ( (uint64_t)block >= bfp->dmn->vols[bfp->pmcell.volume]->blocks ) {
            fprintf(stderr, "Invalid block number (%ld). ", block);
            fprintf(stderr, "Valid block numbers range from 0 to %ld.\n",
              bfp->dmn->vols[bfp->pmcell.volume]->blocks - 1);
            exit(2);
        }
    }

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        flags |= CFLG;
    }

    if ( argc != optind ) {
        fprintf(stderr, "too many Lookup arguments");
        usage();
    }

    ret = scan_mcells(bfp, block, flags, scan_cell_for_block);
    if ( ret == ENOENT && bfp->dmn && bfp->type != FLE ) {
        /* Didn't find block in BMT (RBMT). Try RBMT (BMT). */
        /* Check 'other' BMT only if this is a volume (not a file) and */
        /* it has the 'other' BMT. */
        printf("Block %ld is not in the %s, checking the %s...\n", block,
	       flags & RBMT ? "RBMT" : "BMT", flags & RBMT ? "RBMT" : "BMT");

        flags ^= RBMT;
        ret = scan_mcells(bfp, block, flags, scan_cell_for_block);
    }

    if ( ret == ENOENT ) {
        printf("Block %ld is not in any extent.\n", block);
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
static void
search_tag(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    uint64_t tag;
    int64_t  longnum;
    int ret;
    char *pC;

    pC = argv[optind];
    pC += strlen("t=");

    if ( *pC == '\0' || *pC == ' ') {
        fprintf(stderr, "missing tag Lookup argument");
        usage();
    }

    if ( getnum(pC, &longnum) != OK ) {
        fprintf(stderr, "badly formed tag number: \"%s\".\n", pC);
        exit(1);
    }
    optind++;
    tag = (uint64_t)longnum;

    /*FIX. see MAXTAG in vods.h */
    if ( tag > MAXTAG) {
        fprintf(stderr, "Invalid tag number (%ld).\n", tag);
        exit(2);
    }

    if ( argc > optind && strcmp(argv[optind], "-c") == 0 ) {
        optind++;
        flags |= CFLG;
    }

    if ( argc != optind ) {
        fprintf(stderr, "too many Lookup arguments");
        usage();
    }

    /* FIX? if tag neg, scan RBMT */
    if ( BS_BFTAG_REG(bfp->setTag) ) {
        flags &= ~RBMT;
        bfp->pmcell.volume = 0;
    }

    if ( bfp->type & FLE ) {
        bfp->pmcell.volume = 0;
    }

    ret = scan_mcells(bfp, tag, flags, scan_cell_for_tag);
    if ( ret == ENOENT ) {
        if ( BS_BFTAG_REG(bfp->setTag) ) {
            if ( bfp->setName[0] != '\0' ) {
                printf("No mcell belonging to tag %ld was found in file system \"%s\"\n",
                  tag, bfp->setName);
            } else {
                printf("No mcell belonging to tag %ld was found in file system tag %d\n",
                  tag, bfp->setTag);
            }
        } else if ( bfp->pmcell.volume != 0 ) {
            printf("No mcell belonging to tag %ld was found on volume index %d\n",
                  tag, bfp->pmcell.volume);
        } else {
            printf("No mcell belonging to tag %ld was found\n", tag);
        }
    }

    exit(0);
}

/******************************************************************************/
/* Scan all the mcells and call a function for each mcell.
**
** Input: bfp defines the domain and its RBMTs and BMTs
**        bfp->pmcell.volume - 0 means search all volumes, non zero means search one
**        flags:VFLG - verbose print mcell contents
**        flags:CFLG - continue, keep searching after match found
**        flags:RBMT - search RBMT
** Output: writes to stdout
** Return:
*/
int
scan_mcells(bitFileT *bfp, int64_t value, int flags, int (*funcp)())
{
    vdIndexT startVdi;
    vdIndexT endVdi;
    bitFileT *bmtbfp;
    vdIndexT vdi;
    bs_meta_page_t pn;
    uint16_t cn;
    int ret;
    bsMCT *cellp;
    int found = ENOENT;
    bsMPgT *bsMPgp;
    bfMCIdT mcid;

    if ( bfp->pmcell.volume == 0 ) {
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
        startVdi = FETCH_MC_VOLUME(bfp->pmcell);
        endVdi =   FETCH_MC_VOLUME(bfp->pmcell);
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
                  "scan_mcells: read_page (tag %ld, page %ld) failed.\n",
                  bmtbfp->fileTag.tag_num, bmtbfp->pmcell.page);
                exit(2);
            }

            bsMPgp = (bsMPgT*)bmtbfp->pgBuf;

            if ( pn % 100 == 0 && flags & VFLG) {
                fprintf(stderr, CLEAR_LINE);
                if ( flags & RBMT ) {
                    printf(" ...scanning volume %d, RBMT page %ld of %ld\n",
			   vdi, pn, bmtbfp->pages);
                } else {
                    printf(" ...scanning volume %d, BMT page %ld of %ld\n",
			   vdi, pn, bmtbfp->pages);
                }
            }

            for ( cn = 0; cn < BSPG_CELLS; cn++) {

                cellp = &bsMPgp->bsMCA[cn];

                if ( cellp->mcBfSetTag.tag_num == 0 && cellp->mcTag.tag_num == 0 ) {
                    /* this cell is not in use */
                    continue;
                }
                mcid.volume = vdi;
                mcid.page = pn;
                mcid.cell = cn;
                ret = (*funcp)(bmtbfp, mcid, value, flags);
                /* OK means found, ENOENT means not found */
                if ( ret == OK ) {
                    if ( flags & CFLG ) {
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

} /* scan_mcells */


/* return ENOENT if not found, OK if found */
static int
scan_cell_for_block(bitFileT *bfp, bfMCIdT mcid, int64_t value,
                    int flags)
{
    bsXtntRT *bsXtntRecp;
    bsXtraXtntRT *bsXtraXtntRecp;
    int64_t block = value;
    int ret;
    uint16_t xCnt;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCT *cellp = &bsMPgp->bsMCA[mcid.cell];

    ret = find_bmtr(cellp, BSR_XTNTS, (char **)(&bsXtntRecp));
    if ( ret == OK ) {
        xCnt = bsXtntRecp->xCnt;
        if ( xCnt > BMT_XTNTS ) {
            if ( BS_BFTAG_EQL(cellp->mcBfSetTag,staticRootTagDirTag) &&
                 BS_BFTAG_RSVD(cellp->mcTag) &&
                 BS_IS_TAG_OF_TYPE(cellp->mcTag, BFM_MISC) &&
                 mcid.cell == BFM_MISC )
            {
                if ( xCnt != 3 ) {
                    fprintf(stderr,
 "Bad xCnt (%d) in primary extent record for MISC bitfile on page %ld, cell %d\n", xCnt,
   mcid.page, mcid.cell);
                    xCnt = 3;
                 }
            } else {
                fprintf(stderr,
                 "Bad xCnt (%d) in primary extent record on page %ld, cell %d\n",
                  xCnt, mcid.page, mcid.cell);

                xCnt = BMT_XTNTS;
            }
        }
        return search_xa(bfp, cellp, (int)FETCH_MC_CELL(mcid), bsXtntRecp->bsXA, xCnt,
                         block, flags);
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Bad mcell number %d on page %ld\n", mcid.cell, mcid.page);
        return BAD_DISK_VALUE;
    }

    ret = find_bmtr(cellp, BSR_XTRA_XTNTS, (char**)(&bsXtraXtntRecp));
    if ( ret == OK ) {
        xCnt = bsXtraXtntRecp->xCnt;
        if ( xCnt > BMT_XTRA_XTNTS ) {
            fprintf(stderr,
            "Bad xCnt (%d) in xtra extent record on page %ld, cell %d\n",
              xCnt, mcid.page, mcid.cell);
            xCnt = BMT_XTRA_XTNTS;
        }
        return search_xa(bfp, cellp, mcid.cell, bsXtraXtntRecp->bsXA, xCnt,
                       block, flags);
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Bad mcell number %d on page %ld\n", mcid.cell, mcid.page);
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
** Output: prints to stdout
** Return: OK if bsXA covers block
**         ENOENT if block is not in bsXA
*/
search_xa(bitFileT *bfp, bsMCT *cellp, uint16_t cn, bsXtntT *bsXA, int ndesc,
          int64_t block, int flags)
{
    int j;
    bf_vd_blk_t first_blk;
    bf_vd_blk_t last_blk;
    bf_fob_t fob;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;

    assert(cellp >= &bsMPgp->bsMCA[0]);
    assert(cellp <= &bsMPgp->bsMCA[BSPG_CELLS]);
    for (j = 0; j < ndesc - 1; j++) {
        if ( bsXA[j].bsx_vd_blk == XTNT_TERM || 
	     bsXA[j].bsx_vd_blk == COWED_HOLE) {
            continue;
        }

        first_blk = bsXA[j].bsx_vd_blk;
        last_blk = first_blk + 
	           (bsXA[j+1].bsx_fob_offset - bsXA[j].bsx_fob_offset);

        if ( block >= first_blk && block < last_blk ) {
            fprintf(stderr, CLEAR_LINE);
            fob = bsXA[j].bsx_fob_offset + 
		  (block - first_blk) * ADVFS_FOBS_PER_DEV_BSIZE;
            printf("block %ld is in fob %ld in file tag %ld, set tag %ld\n ",
		   block, fob, 
		   cellp->mcTag.tag_num, 
		   cellp->mcBfSetTag.tag_num);
            print_mcell_header(bfp, bsMPgp, flags & VFLG);
            print_cell(cellp, cn, (flags & VFLG) | ONECELL);

            return OK;
        }
    }
    return ENOENT;
}

/******************************************************************************/
/* Search for tagnum being used by a file in the specified fileset.
** bfp->setTag is used to pass in the fileset tag.
**
** Input: bfp specifies the domain. bfp->dmn filled in
** Input: bfp->setTag specifies the set tag to search
** Input: flags:VFLG - print entire contents of found mcell
** Output: prints to stdout
** Return: OK if tag is found
**         ENOENT if tag is not found
*/
static int
scan_cell_for_tag(bitFileT *bfp, bfMCIdT mcid, int64_t value,
                  int flags)
{
    int results;
    int found = 0;
    uint64_t tagnum = value;
    bsMPgT *bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCT *cellp = &bsMPgp->bsMCA[mcid.cell];

    if ( cellp->mcTag.tag_num != tagnum ) {
        return ENOENT;
    }

    fprintf(stderr, CLEAR_LINE);

    print_mcell_header(bfp, bsMPgp, flags & VFLG);

    
    print_cell(cellp, mcid.cell, (flags & VFLG) | ONECELL);
    fflush(stdout);

    return OK;
}

/******************************************************************************/
static void
process_ddl(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int ret;
    bs_meta_page_t ddlPg;
    bsMCT *bsMCp;
    delLinkT *delLinkp;
    bfMCIdT nextMCId;
    bfMCIdT prevMCId;
    bsXtntRT *bsXtntp;
    vdIndexT vdi;
    vdIndexT startVdi;
    vdIndexT endVdi;
    bitFileT *bmtbfp;
    bsMPgT *bsMPgp;

    flags |= DDL;

    if ( bfp->pmcell.volume == 0 ) {
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
        startVdi = FETCH_MC_VOLUME(bfp->pmcell);
        endVdi =   FETCH_MC_VOLUME(bfp->pmcell);
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

	ddlPg = MCELL_LIST_PAGE;      /* ddl is on BMT page 0 */

        ret = read_page(bmtbfp, ddlPg);
        if ( ret != OK ) {
            fprintf(stderr, "process_ddl: read_page page %ld failed.\n", ddlPg);
            exit(2);
        }

        bsMPgp = (bsMPgT*)bmtbfp->pgBuf;
        bsMCp = &bsMPgp->bsMCA[MCELL_LIST_CELL];  /* cell 0 */

        ret = find_bmtr(bsMCp, BSR_DEF_DEL_MCELL_LIST, (char**)(&delLinkp));
        if ( ret != OK ) {
            fprintf(stderr,
              "Cannot find BSR_DEF_DEL_MCELL_LIST record in cell 0\n");
            exit(0);
        }

        print_mcell_header(bmtbfp, bsMPgp, flags & VFLG);
        print_cell(bsMCp, MCELL_LIST_CELL, flags & VFLG);

        prevMCId.page = 0;
        prevMCId.cell = 0;
        nextMCId = delLinkp->nextMCId;

	/* loop to print all chained DDL entries */

        while ( nextMCId.page != 0 || nextMCId.cell != 0 ) {
            ret = read_page(bmtbfp, (bs_meta_page_t)FETCH_MC_PAGE(nextMCId));
            if ( ret != OK ) {
                fprintf(stderr, "process_ddl: read_page page %ld failed.\n",
                  nextMCId.page);
                exit(2);
            }

            bsMPgp = (bsMPgT*)bmtbfp->pgBuf;
            bsMCp = &bsMPgp->bsMCA[nextMCId.cell];
            if ( find_bmtr(bsMCp, BSR_XTNTS, (char**)(&bsXtntp)) != OK ) {
                fprintf(stderr, "Cannot find BSR_XTNTS record in cell\n");
                exit(0);
            }

            if ( bsXtntp->delLink.prevMCId.page != prevMCId.page ||
                 bsXtntp->delLink.prevMCId.cell != prevMCId.cell )
            {
                fprintf(stderr,
                  "process_ddl: Bad deffered delete list back pointer.\n");
                fprintf(stderr,
                  "Back pointer (%ld %d) at page %ld cell %d, should be %ld %d\n",
                  bsXtntp->delLink.prevMCId.page,
                  bsXtntp->delLink.prevMCId.cell,
                  nextMCId.page, nextMCId.cell, 
                  prevMCId.page, prevMCId.cell);
                exit(3);
            }

            bmtbfp->fileTag = bsMCp->mcTag;
            bmtbfp->setTag = bsMCp->mcBfSetTag;
            process_chain(bmtbfp, nextMCId, flags);

	    prevMCId = nextMCId;
            nextMCId = bsXtntp->delLink.nextMCId;
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
static int
find_n_set_vdi(bitFileT *bfp)
{
    int ret;
    bsMCT *bsMCp;
    bsVdAttrT *bsVdAttrp;
    bsXtntRT *bsXtntRp;
    bsXtraXtntRT *bsXtraXtntRp;
    vdIndexT vdi = 0;
    bfTagT fTag;
    bfTagT sTag;
    bs_meta_page_t pn;
    uint16_t cn;
    int i;
    char notHere[BS_MAX_VDI];
    bs_meta_page_t nextPg;
    bfMCIdT nextMCId;
    int type;
    bsMPgT *bsMPgp;
    int giveupCount = 0;  /* error tolerance */

    assert(bfp->type & FLE);

    ret = read_page(bfp, 0);
    if ( ret != OK ) {
        fprintf(stderr, "find_n_set_vdi: read_page page 0 failed.\n");
        exit(2);
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[BFM_RBMT];

    /* This may be a BMT file or a RBMT file. Only a RBMT file has a
       BSR_VD_ATTR record. */
    if ((ret = find_bmtr(bsMCp, BSR_VD_ATTR, (char**)(&bsVdAttrp))) == OK ) {
      vdi = bsVdAttrp->vdIndex;
      goto set_vdi;
    } else if (ret != ENOENT) {
	giveupCount++;
    }

    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        notHere[i] = FALSE;
    }

    /* look for next extent records (not next general mcells) */
    for ( pn = 0; pn < bfp->pages; pn++ ) {
        ret = read_page(bfp, pn);
        if ( ret != OK ) {
            fprintf(stderr, "find_n_set_vdi: read_page page %ld failed.\n", pn);
            exit(2);
        }

        bsMPgp = (bsMPgT*)bfp->pgBuf;

        for ( cn = 0; cn < BSPG_CELLS; cn++ ) {
            vdi = 0;
            bsMCp = &bsMPgp->bsMCA[cn];
            if ((ret = find_bmtr(bsMCp, BSR_XTNTS, (char**)(&bsXtntRp))) 
		== OK ) {
                if ( bsXtntRp->chainMCId.volume ) {
                    nextMCId = bsXtntRp->chainMCId;
                    vdi = FETCH_MC_VOLUME(bsXtntRp->chainMCId);
                    fTag = bsMCp->mcTag;
                    sTag = bsMCp->mcBfSetTag;
                    nextPg =
                    bsXtntRp->bsXA[bsXtntRp->xCnt-1].bsx_fob_offset;
                    type = BSR_XTRA_XTNTS;
                }
            } else if (ret != ENOENT) {
		if (++giveupCount > 5) {
		    fprintf(stderr, "find_BMT failed\n");
		    return (-1);
		}
	    }

            ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)(&bsXtraXtntRp));
            if ( ret == OK ) {
                if ( bsMCp->mcNextMCId.volume ) {
                    vdi = FETCH_MC_VOLUME(bsMCp->mcNextMCId);
                    nextMCId = bsMCp->mcNextMCId;
                    fTag = bsMCp->mcTag;
                    sTag = bsMCp->mcBfSetTag;
                    nextPg = bsXtraXtntRp->bsXA[bsXtraXtntRp->xCnt-1].bsx_fob_offset;
                    type = BSR_XTRA_XTNTS;
		}
	    } else if (ret != ENOENT) {
		if (++giveupCount > 5) {
		    fprintf(stderr, "find_BMT failed\n");
		    return -1;
		}
	    }

            if ( vdi && notHere[vdi] == FALSE &&
                 nextMCId.page < bfp->pages )
            {
                ret = read_page(bfp, (bs_meta_page_t)FETCH_MC_PAGE(nextMCId));
                if ( ret != OK ) {
                    continue;
                }

                bsMPgp = (bsMPgT*)bfp->pgBuf;
                bsMCp = &bsMPgp->bsMCA[nextMCId.cell];
                if ( bsMCp->mcTag.tag_num == fTag.tag_num &&
                     bsMCp->mcTag.tag_seq == fTag.tag_seq &&
                     bsMCp->mcBfSetTag.tag_num == sTag.tag_num &&
                     bsMCp->mcBfSetTag.tag_seq == sTag.tag_seq )
                {
                    if ( type == BSR_XTRA_XTNTS ) {
                        ret =
                         find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)(&bsXtraXtntRp));
                        if ( ret == OK ) {
                            if ( nextPg == bsXtraXtntRp->bsXA[0].bsx_fob_offset ) {
                                goto set_vdi;
                            }
                        } else if (ret != ENOENT) {
			    if (++giveupCount > 5) {
				fprintf(stderr, "find_BMT failed\n");
				return -1;
			    }
			}
                    }
                }
            }
        }
    }

set_vdi:
    bfp->pmcell.volume = vdi;
    return (OK);
}
