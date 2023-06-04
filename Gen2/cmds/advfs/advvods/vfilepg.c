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
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <dirent.h>
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

/* prevailing output format value */

#define FMT_D 1
static int format = NULL;

/* prototypes */

static void print_data(char *data, int offset, int len);
static void print_block(bitFileT *bfp, int flags);
static void print_dirblock(char *block, int vflg);
static void save_file(bitFileT *bfp, char *argv[], int argc, 
		      int optind, int flags);
static void process_block(bitFileT *bfp, char *argv[], int argc, 
			  int optind, int flags);
static void process_fob(bitFileT *bfp, bf_fob_t fobnum, char *fset, 
			char *fname, int flg);
static int read_fob(bitFileT *bfp, bf_fob_t fobnum);


/*****************************************************************************/
void 
file_main(int argc, 
	  char **argv)
{
    int ret;
    int fd;
    bitFileT  bf;
    bitFileT  *bfp = &bf;
    int flags = 0;
    bf_fob_t fobnum = 0;
    bf_fob_t fobcnt;
    int64_t longnum;
    char *fset = NULL;
    char *fname = NULL;
    struct stat statBuf;

    init_bitfile(bfp);

    ret = resolve_src(argc, argv, &optind, "rvf:", bfp, &flags);

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

    if ( bfp->type != FLE ) {
        if ( argc < optind + 1) {
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

        assert(bfp->pmcell.volume != 0);
        assert(bfp->pmcell.page != PAGE_TERM);
        assert(bfp->pmcell.cell != CELL_TERM);
    }

    fobcnt = PAGE2FOB(bfp->pages);

    /* If this file contains no fobs, it might be a symbolic link.  We
       test that here and print the link if it is. */

    if (fobcnt == 0) {

	bsMRT *pRecHdr;
	statT *pStatRec;
	char *pLinkStr;

        fname = strdup(argv[optind-1]);

	if (find_rec(bfp, BMTR_FS_STAT, (char **)&pStatRec) != OK) {
	    fprintf(stderr, "Cannot find BMTR_FS_STAT rec in '%s' primary mcell.\n",
		    fname);
	    exit(1);
	}

	if (S_ISLNK(pStatRec->st_mode)) {

	    if (find_rec(bfp, BMTR_FS_DATA, &pLinkStr) != OK) {
		fprintf(stderr, "Cannot find BMTR_FS_DATA rec in '%s' primary mcell.\n",
			fname);
		exit(1);
	    }

	    pRecHdr = (bsMRT *)(pLinkStr - sizeof(bsMRT));
	    pLinkStr[pRecHdr->bCnt - sizeof(bsMRT)] = '\0';
	    printf("File '%s' is a symbolic link to '%s'\n", fname, pLinkStr);
	    exit(0);
	}
    }
	
    /* dump the file to a file */
    if ( argc > optind && strcmp(argv[optind], "-d") == 0 ) {
        save_file(bfp, argv, argc, optind, flags);
    }

    /* display all the file */
    if ( argc > optind && strcmp(argv[optind], "-a") == 0 ) {

        fname = strdup(argv[optind-1]);

        optind++;

        if ( argc > optind ) {
            fprintf(stderr, "Too many arguments.\n");
            usage();
        }

        for ( fobnum = 0; fobnum < fobcnt; fobnum++ ) {
            process_fob(bfp, fobnum, fset, fname, flags & VFLG);
        }

        exit(0);
    }

    /* get FOB number */
    if ( strcmp(argv[optind], "-o") == 0 ) {
        optind++;

	  if ( argc > optind && isdigit(*argv[optind]) ) {
	      if ( getnum(argv[optind], (int64_t *)&fobnum) != OK ) {
		fprintf(stderr, "Badly formed FOB number: \"%s\"\n", 
			argv[optind]);
		exit(1);
	      }

	      fobcnt = ADVFS_FOBS_PER_DEV_BSIZE 
		       * bfp->dmn->vols[bfp->pmcell.volume]->blocks;

	      if ( fobnum < 0 || fobnum >= fobcnt - 1) {
		  fprintf(stderr, "Invalid FOB number (%ld). ", fobnum);
		  fprintf(stderr, "Valid FOB numbers are 0 through %ld.\n",
			  fobcnt - 1);
		  exit(1);
	      }
	      optind++;
	  }
    }

    if ( argc > optind && strcmp(argv[optind], "-D") == 0 ) {
        format = FMT_D;
	optind++;
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    process_fob(bfp, fobnum, fset, fname, flags & VFLG);
    exit(0);

} /* file_main */

/*****************************************************************************/
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

    assert(bfp->fd);
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
              bfp->dmn->vols[bfp->pmcell.volume]->blocks - 16);
            exit(2);
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

static void
print_block(bitFileT *bfp, int flags)
{
    ssize_t read_ret;
    char *block;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    /* malloc so that buffer is long alligned */
    /* FIX. use bfp->pg_buf */
    block = malloc(DIRBLKSIZ > DEV_BSIZE ? DIRBLKSIZ : DEV_BSIZE);
    if ( block == NULL ) {
        perror("print_block");
        return;
    }

    if ( lseek(bfp->fd, (off_t)bfp->pgLbn * DEV_BSIZE, SEEK_SET) == -1 ) {
        sprintf(errstr, "lseek to block %ld failed", bfp->pgLbn);
        perror(errstr);
	free(block);
        return;
    }

    if ( format == FMT_D ) {
        read_ret = read(bfp->fd, block, (size_t)DIRBLKSIZ);
    } else {
        read_ret = read(bfp->fd, block, (size_t)DEV_BSIZE);
    }

    if ( format == FMT_D && read_ret != DIRBLKSIZ ||
         format != FMT_D && read_ret != DEV_BSIZE )
    {
        if ( read_ret == (ssize_t)-1 ) {
            perror("print_block: read failed");
        } else {
            fprintf(stderr, 
		    "print_block: read returned %d bytes, expected %d\n",
		    read_ret, flags & FMT_D ? DIRBLKSIZ : DEV_BSIZE);
        }
	free(block);
        return;
    }

    bfp->pgVol = bfp->pmcell.volume;
    print_header(bfp);

    if ( format == FMT_D ) {
        print_dirblock(block, flags & VFLG);
    } else {
        print_data(block, bfp->pgLbn * DEV_BSIZE, DEV_BSIZE);
    }

    free(block);
    return;

} /* print_block */

/*****************************************************************************/

static void
save_file(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    char *dfile;

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

    exit(0);

} /* save_file */

/*****************************************************************************/

static void
process_fob(bitFileT *bfp, bf_fob_t fobnum, char *fset, char *fname, int flg)
{
    int i, ret;

    if ((ret = read_fob(bfp, fobnum)) == OK) {

        print_fob_header(bfp, fobnum);

	if ( format == FMT_D ) {
	    print_dirblock(bfp->pgBuf, flg & VFLG);
	} else {
	    print_data(bfp->pgBuf, fobnum * ADVFS_FOB_SZ, ADVFS_FOB_SZ);
	}

	return;
    }

    printf("error reading FOB #%ld\n", fobnum);
    exit(0);

} /* process_fob */

#ifdef FUTURE
/************************************************************************/
static void
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
        format = FMT_A;
        break;
      case 'c':   /* print page as bytes (default) */
        format = FMT_C;
        break;
      case 's':   /* print page as shorts */
        format = FMT_S;
        break;
      case 'i':   /* print page as integers */
        format = FMT_I;
        break;
      case 'l':   /* print page as longs */
        format = FMT_L;
        break;
      case 'd':   /* format the page as a directory page */
        format = FMT_D;
        break;
      default:
        fprintf(stderr, "Bad format after -f flag\n");
        usage();
    }

    (*optind)++;

} /* get_format */
#endif /* FUTURE */

/*****************************************************************************/
#define BPL 16		/* bytes of data per line */

static void
print_data(char *data, int offset, int len)
{
    int lines = (len + BPL - 1) / BPL;
    int ln;
    u_char *line;
    u_char *cp;

    for ( ln = 0; ln < lines; ln++) {
        line = (u_char *)(&data[ln * BPL]);
        printf("%06x   ", offset + (int)line - (int)data);
        for ( cp = line; cp < (u_char*)(&data[ln * BPL + BPL]); cp++ ) {
            printf("%02x ", *cp);
        }

        printf("   ");
        for ( cp = line; cp < (u_char*)(&data[ln * BPL + BPL]); cp++ ) {
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
static void
print_dirblock(char *block, int vflg)
{
    fs_dir_entry  *dir_ent_p;
    int i;
    char fname[FS_DIR_MAX_NAME + 1];
    bfTagT tag;

    dir_ent_p = (fs_dir_entry*)(block);

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

    while ( (u_char *)dir_ent_p < (u_char *)(&block[DIRBLKSIZ]) ) {
        if ( dir_ent_p->fs_dir_namecount > FS_DIR_MAX_NAME ) {
            printf("dir err\n");
            exit(1);
        }
        strncpy(fname, dir_ent_p->fs_dir_name_string,
          dir_ent_p->fs_dir_namecount);
        fname[dir_ent_p->fs_dir_namecount] = '\0';

        if ( vflg ) {
            printf("%7ld  ", dir_ent_p->fs_dir_bs_tag_num);
            printf("%3d  ", dir_ent_p->fs_dir_size);
            printf("%2d  ", dir_ent_p->fs_dir_namecount);
            printf("\"%s\"  ", fname);
            for ( i = 41 - strlen(dir_ent_p->fs_dir_name_string); i > 0; i-- )
            {
                printf(" ");
            }
            GETTAG(tag,dir_ent_p);
            printf("(%04lx.%x)\n",
              tag.tag_num, tag.tag_seq);
        } else {
            if ( dir_ent_p->fs_dir_bs_tag_num != 0 ) {
                printf("%7ld  %s\n",
                  dir_ent_p->fs_dir_bs_tag_num,
                  dir_ent_p->fs_dir_name_string);
            }
        }

        if ( dir_ent_p->fs_dir_size == 0 )
            break;

        dir_ent_p = (fs_dir_entry*)((char*)dir_ent_p +
                                    dir_ent_p->fs_dir_size);
    }
}

/*****************************************************************************/
/* Read a FOB from the bitFile into the bitFile page buffer.
** Maintain a simple one FOB cache. If pgVol & pgLbn as calculated from page
** and xmap are aready set in bfp then pgBuf already contains the page.
**
** input: bfp->xmap hold extents for file
**        bfp->type determines whether to use xtnts or fd
**        bfp->fd - read from this fd if type is FLE or VOL
**        bfp->pgVol, pgNum, pgLbn - if same as requested, don't read again
**        fobnum is the FOB # to read (FOBs start at 0)
** output: bfp->pgBuf - a FOB of data is loaded here
**         bfp->pgBuf - if NULL, we malloc a FOB buffer here
**         bfp->pgVol, pgNum, pgLbn - set to reflect where pgBuf read from
** Errors: diagnostic output to stderr for all errors
** return OK - if read succeeds
**        BAD_DISK_VALUE - block is not mapped or volume does not exist
**        ENOMEM - malloc failed
*/
/* TODO: use read_vol_at_blk. */
static int
read_fob(bitFileT *bfp,  
	 bf_fob_t fobnum)
{
    uint64_t lbn;
    bf_fob_t lastfob;
    int vol = 0;
    int fd, low = 0, high = bfp->xmap.cnt, idx = high / 2;
    int lastidx;
    ssize_t read_ret;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    if ( bfp->type & FLE ) {
        /* working with dump file;  fob # is real offset into the file */
        if (bfp->pgBuf && (fobnum * ADVFS_FOBS_PER_DEV_BSIZE) == bfp->pgLbn) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }

        fd = bfp->fd;
        if ( lseek(fd, (off_t)ADVFS_FOB_SZ * fobnum, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_fob: lseek to FOB %ld failed", fobnum);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    } else {
        assert ( bfp->xmap.cnt != 0 );

	/* find the extent whose tange includes the target page */

        while ( (idx > 0) &&
                ((fobnum < bfp->xmap.xtnt[idx].fob) ||
                 ((idx < bfp->xmap.cnt - 1) &&
                  (fobnum >= bfp->xmap.xtnt[idx + 1].fob))) ) {

            lastidx = idx;
            if (fobnum < bfp->xmap.xtnt[idx].fob) {
                high = idx;
                idx = low + (idx - low) / 2;
            } else {
                low = idx;
                idx = idx + (high - idx) / 2;
            }

            /* is this a test that the page is off the end? return err */
            assert(idx != lastidx);
        }
        assert(idx >= 0);

        if (bfp->xmap.xtnt[idx].blk < 0) {
            /* page not mapped */
            return BAD_DISK_VALUE;
        }

        if ( !(bfp->type & FLE) ) {
            vol = bfp->xmap.xtnt[idx].vol;
            if ( bfp->dmn->vols[vol] == NULL ) {
                fprintf(stderr,
                  "Bad volume (%d) for extent %d of set tag %ld, file tag %ld\n",
                  vol, idx, bfp->setTag, bfp->fileTag);
                fprintf(stderr,
                  "Fob %ld defined in extent %d is on volume index %d.\n",
                   fobnum, idx, bfp->xmap.xtnt[idx].vol);
                fprintf(stderr, "Storage Domain \"%s\" does not have this volume.\n",
                   bfp->dmn->dmnName);
                return BAD_DISK_VALUE;
            }
            fd = bfp->dmn->vols[vol]->fd;
        } else {
            /* this is a BMT. It is self contained on this volume */
            fd = bfp->fd;
        }

	/* compute the block number where the target fob starts */

        lbn = (uint64_t)bfp->xmap.xtnt[idx].blk +
	       ((fobnum - bfp->xmap.xtnt[idx].fob) / ADVFS_FOBS_PER_DEV_BSIZE);
        if ( bfp->pgBuf && lbn == bfp->pgLbn && bfp->pgVol == vol ) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }

        bfp->pgVol = vol;
        bfp->pgLbn = lbn;
        if ( lseek(fd, lbn * DEV_BSIZE, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_fob: lseek to block %ld failed", lbn);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    }

    if ( bfp->pgBuf == NULL ) {
        bfp->pgBuf = malloc(ADVFS_FOB_SZ);
        if ( bfp->pgBuf == NULL ) {
            perror("read_fob: malloc failed");
            return ENOMEM;
        }
    }

    read_ret = read(fd, bfp->pgBuf, (size_t)ADVFS_FOB_SZ);
    if ( read_ret != (ssize_t)ADVFS_FOB_SZ ) {
        if ( read_ret == (ssize_t)-1 ) {
            sprintf(errstr, "read_fob: Read FOB %ld failed.", fobnum);
            perror(errstr);
        } else {
            fprintf(stderr,
              "read_fob: Truncated read on FOB %ld, expected %d, got %ld\n",
              fobnum, ADVFS_FOB_SZ, read_ret);
        }
        return BAD_DISK_VALUE;
    }

    bfp->pgNum = FOB2PAGE(fobnum);

    return OK;

} /* read_fob */
