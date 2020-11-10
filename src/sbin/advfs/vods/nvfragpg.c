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
 *      View On-Disk Structures
 *
 * Date:
 *
 *      Tue Feb 8 1995
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: nvfragpg.c,v $ $Revision: 1.1.19.1 $ (DEC) $Date: 2007/09/27 05:48:47 $"

#include <stdio.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>
#include <sys/types.h>
#include <unistd.h>

#include <msfs/bs_public.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>
#include "vods.h"

#define FRAG_PGSZ   (BF_FRAG_PG_BYTES / BS_BLKSIZE)

#define UNKNOWN_DVN 0
#define BLK 0x10

char *Prog;

typedef struct frag {
    long lbn;
    uint32T nextFreeGrp;
    uint_t fragType;
    int freeFrags;
    unsigned int version;
    int flags;
} fragT;

/* flags values */
#define EXISTS 1
#define SEEN   2

/* function return values */
#define BAD 1
#define SPARSE 2

typedef struct {
    grpHdrT fragHdr;
    int exists;
    bsBfSetAttrT setAttr;
} savGrpT;

/* private prototypes */
void usage();
void process_block( bitFileT*, char**, int, int, int );
void print_block( bitFileT*, int, int );
void process_all( bitFileT*, char**, int, int, int );
int print_page( bitFileT*, int, int );
int print_fragpage( bitFileT*, fragSlotT*, int );
void print_freelist_heads( bitFileT*, bsBfSetAttrT*, fragT* );
void process_summary( bitFileT*, bsBfSetAttrT*, char*, int );
int get_frag_grps( bitFileT*, fragT**, int**, int*, int*, int, bsBfSetAttrT* );
void print_frag_summary( int*, int* );
void print_freelist_heads( bitFileT*, bsBfSetAttrT*, fragT* );
void print_verbose_page_order_frags( bitFileT*, fragT* );
void print_verbose_free_order_frags( bitFileT*, bsBfSetAttrT*, fragT*, int**,
                                     int*, int );
void print_free_frags( bitFileT*, bsBfSetAttrT*, fragT*, int**, int*, int );
void fill_free_array( bitFileT*, bsBfSetAttrT*, fragT*, int**, int*, int*, int);
void dump_meta( bitFileT*, char *[], int, int, bsBfSetAttrT*, int );
void seek_read( bitFileT*, void*, size_t, off_t, char* );
int test_saved_slot( bitFileT*, fragSlotT* );
int test_grpHdr( bitFileT*, grpHdrT*, bsBfSetAttrT*, uint );
int test_self( uint, uint32T );
int test_fragType( uint, uint32T );
int test_version( uint, uint32T );
int test_freeFrags( grpHdrT*, uint, uint32T );
int test_firstFrag( bitFileT*, grpHdrT*, bsBfSetAttrT*, uint32T );
int test_freeGrp( bitFileT*, uint32T, int, char* );
int test_xxxFreeFrag( grpHdrT*, uint32T );
int test_setId( bfSetIdT, bfSetIdT, uint32T );
int test_free_frag_slots( grpHdrT*, uint, uint, int );
int test_frag_slot( ushort, grpHdrT*, uint, uint, int );

void
usage()
{
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "%s [-v] [-f] <dmn> <set>		print frag summary\n", Prog);
    fprintf(stderr, "%s [-v] [-f] <dmn> <set> page	print frag page\n", Prog);
    fprintf(stderr, "%s <vol> -b block			print frag page\n", Prog);
    fprintf(stderr, "%s <dmn> <set> -d sav_file		save frag metadata\n", Prog);
    fprintf(stderr, "%s [-v] [-f] [-F] sav_file [page]	print saved info\n", Prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "<dmn> = [-r] [-D] domain_name\n");
    fprintf(stderr, "<vol> = volume_name | <dmn> vdi\n");
    fprintf(stderr, "<set> = [-S] set_name | -t set_tag\n");

    exit(1);
}

/*
** vfragpg [-v] [-f] <dmn> <set>
** vfragpg [-v] [-f] <dmn> <set> page
** vfragpg [-v] [-f] [-F] file [page]
** vfragpg <vol> -b block
** vfragpg <dmn> <set> -d file

** vfragpg [-v] [-f] <vol> <set> page

** <dmn> = [-r] [-D] domain_name
** <vol> = volume_name | <dmn> vdi
** <set> = [-S] set_name | -t set_tag

** to do
** vfragpg <src> -i frag_id					print data
** vfragpg [-S] saved_dmn_dir file_set				print free info
*/

main(argc,argv)
int argc;
char *argv[];
{
    int fd;
    char *fileset = NULL;
    bitFileT bf;
    bitFileT *bfp = &bf;
    int flags = 0;
    long pagenum = -1;
    long longnum;
    bitFileT tagBf;
    bitFileT setBf;
    bsBfSetAttrT *bsBfSetAttrp = NULL;
    bsBfSetAttrT setAttr;
    int i;
    int ret;
    struct stat statbuf;
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
    bfp->fileTag = 1;

    ret = resolve_src(argc, argv, &optind, "rvfD:V:F:", bfp, &flags);

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

    if ( bfp->type & FLE ) {
        ret = fstat(bfp->fd, &statbuf);
        if ( ret == -1 ) {
            perror("main");
            exit(2);
        }
        bfp->pages = statbuf.st_size * BF_FRAG_GRP_PGS / BF_FRAG_SLOT_BYTES;
    }

    /* All -b usages have been caught. */
    /* No "vfragpg volume ..." or "vfragpg domain index ..." from here on. */
    if ( bfp->pvol )
        usage();

    /* If a domain was specified we need a file set (or set tag). */
    /* If a file was specified the next argument (if any) is a page number. */
    if ( bfp->type & DMN || bfp->type & SAV ) {
        bfTagT tag;

        if ( argc == optind ) {
            fprintf(stderr, "Fileset specification needed.\n");
            usage();
        }

        /* vfragpg <domain> <fileset> */
        newbf(bfp, &tagBf);
        newbf(bfp, &setBf);

        if ( find_TAG(&tagBf) ) {
            exit(1);
        }

        ret = get_set(&tagBf, argv, argc, &optind, &setBf);
        if ( ret != OK ) {
            fprintf(stderr, "set doesn't exist\n");
            if ( ret == BAD_SYNTAX ) {
                usage();
            } else {
                exit(2);
            }
        }

        ret = find_rec(&setBf, BSR_BFS_ATTR, (char**)&bsBfSetAttrp);
        if ( ret == OK ) {
            setAttr = *bsBfSetAttrp;
            bsBfSetAttrp = &setAttr;
        } else {
            fprintf(stderr, "couldn't find BSR_BFS_ATTR record\n");
        }

        if ( bsBfSetAttrp->cloneId != 0 ) {
            /* Modify inmem setid to that of original set's setid. */
            mod_attr_dmnid(&setBf, bsBfSetAttrp);
        }

        /* tag 1 is the frag file */
        if ( get_file_by_tag(&setBf, (uint32T)1, bfp) ) {
            fprintf(stderr, "nvfragpg: Can't find file tag 1.\n");
            exit(1);
        }

        if ( bfp->pages == 0 || bfp->pages == -1 ) {
            if ( setBf.setName[0] != '\0' ) {
                printf("Set \"%s\" has no frag groups allocated.\n",
                  setBf.setName);
            } else {
                printf("Set tag %d has no frag groups allocated.\n",
                  setBf.setTag);
            }
            exit(0);
        }

        fileset = argv[optind];
    }

    if ( argc == optind ) {
        process_summary(bfp, bsBfSetAttrp, fileset, flags);
        /* NOT REACHED */
    }

    /* dump the frag to a file */
    if ( argc > optind && strcmp(argv[optind], "-d") == 0 ) {
        dump_meta(bfp, argv, argc, optind, bsBfSetAttrp, flags);
        /* NOT REACHED */
    }

    /* display all the frag file */
    if ( argc > optind && strcmp(argv[optind], "-a") == 0 ) {
        process_all(bfp, argv, argc, optind, flags);
        /* NOT REACHED */
    }

    assert(argc > optind);
    /* FIX. could be separate routine called process_page()  */
    if ( argc > optind ) {
        /* If a page number is entered, get it */
        if ( getnum(argv[optind], &pagenum) != OK ) {
            fprintf(stderr, "Badly formed page number: \"%s\"\n", argv[optind]);
            exit(1);
        }
        if ( pagenum < 0 || pagenum >= bfp->pages ) {
            fprintf(stderr, "Invalid frag page number (%d). ", pagenum);
            fprintf(stderr, "Valid page numbers are 0 through %d.\n",
              bfp->pages - 1);
            exit(1);
        }
        if ( pagenum / BF_FRAG_GRP_PGS * BF_FRAG_GRP_PGS != pagenum ) {
            fprintf(stderr,
        "Page number must be multiple of %d since frag groups are %d pages.\n",
             BF_FRAG_GRP_PGS, BF_FRAG_GRP_PGS);
            pagenum = pagenum / BF_FRAG_GRP_PGS * BF_FRAG_GRP_PGS;
            fprintf(stderr,
            "Page number changed to next lower group page number (%d)\n",
             pagenum);
        }
        optind++;
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    print_page(bfp, pagenum, flags);

    exit(0);
}

/*****************************************************************************/
void
process_block(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    long blknum = -1;

    assert(bfp->fd);
    if ( bfp->type & FLE || bfp->type & SAV ) {
        fprintf(stderr, "Can't use -b flag on a file\n");
        usage();
    }

    if ( strlen(argv[optind]) == 2 ) {              /* -b # */
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
    } else {                                        /* -b# */
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
              bfp->dmn->vols[bfp->pvol]->blocks - FRAG_PGSZ);
            exit(2);
        }
    }

    if ( (blknum / FRAG_PGSZ) * FRAG_PGSZ != blknum ) {
        fprintf(stderr, "Frag pages are on 16 block boundaries.\n");
        blknum = blknum / FRAG_PGSZ * FRAG_PGSZ;
        fprintf(stderr,
         "Block number changed to next lower multiple of 16 (%d).\n", blknum);
    }

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    bfp->ppage = -1;
    bfp->pgLbn = blknum;
    print_block(bfp, blknum, flags);
    /* NOT REACHED */
}

/*****************************************************************************/
void
print_block(bitFileT *bfp, int lbn, int flgs)
{
    ssize_t readRet;
    char *pagep;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    /* malloc so that buffer is long alligned */
    pagep = malloc(BF_FRAG_SLOT_BYTES);
    if ( pagep == NULL ) {
        perror("print_block");
        exit(3);
    }

    seek_read(bfp, pagep, BF_FRAG_SLOT_BYTES,
              (ulong_t)lbn * DEV_BSIZE, "print_block");

    bfp->pgNum = (uint32T)-1;  /* for routines that expect pgNum to be set up */
    bfp->pgVol = bfp->pvol;
    bfp->type |= BLK;
    print_header(bfp);
    print_fragpage(bfp, (fragSlotT*)pagep, flgs);

    exit(0);
}

/*****************************************************************************/
void
process_all(bitFileT *bfp, char *argv[], int argc, int optind, int flags)
{
    int pagenum;

    optind++;

    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    for ( pagenum = 0; pagenum < bfp->pages; pagenum += BF_FRAG_GRP_PGS ) {
        print_page(bfp, pagenum, flags);
    }

    exit(0);
}

/*****************************************************************************/
int
print_page(bitFileT *bfp, int page, int flg)
{
    int i;
    fragSlotT fragSlot;
    int ret;
    off_t lseekOff;

    assert(page % BF_FRAG_GRP_PGS == 0);

    if ( bfp->type & FLE ) {
        bfp->pgNum = page;  /* for print_header() */
        lseekOff = page / BF_FRAG_GRP_PGS * BF_FRAG_SLOT_BYTES;
        seek_read(bfp, &fragSlot, BF_FRAG_SLOT_BYTES, lseekOff, "print_page");

        bfp->pgNum = page;  /* for routines that expect pgNum to be set up */
        print_header(bfp);

        if ( test_saved_slot(bfp, &fragSlot) == SPARSE ) {
            printf("Page %d does not exist. It is a sparse hole.\n", page);
            return OK;
        }
    } else {
        ret = read_page(bfp, page);
        print_header(bfp);
        if ( ret != OK ) {
            printf("Page %d does not exist. It is a sparse hole.\n", page);
            return OK;
        }

        /* local copy so later read_page doesn't clobber this. */
        fragSlot = ((slotsPgT*)PAGE(bfp))->slots[0];
    }

    print_fragpage(bfp, &fragSlot, flg);

    return 0;
}

/*****************************************************************************/
#define FRAG_TYPE(type)                      \
    type == BF_FRAG_ANY ? "BF_FRAG_ANY" :    \
    type == BF_FRAG_1K ? "BF_FRAG_1K" :      \
    type == BF_FRAG_2K ? "BF_FRAG_2K" :      \
    type == BF_FRAG_3K ? "BF_FRAG_3K" :      \
    type == BF_FRAG_4K ? "BF_FRAG_4K" :      \
    type == BF_FRAG_5K ? "BF_FRAG_5K" :      \
    type == BF_FRAG_6K ? "BF_FRAG_6K" :      \
    type == BF_FRAG_7K ? "BF_FRAG_7K" :      \
                         "???"

#define PRINT_FRAG_TYPE(type)                   \
    switch ( type ) {                           \
      case BF_FRAG_ANY:                         \
        printf(FRAG_TYPE(type));                \
        break;                                  \
      case BF_FRAG_1K:                          \
      case BF_FRAG_2K:                          \
      case BF_FRAG_3K:                          \
      case BF_FRAG_4K:                          \
      case BF_FRAG_5K:                          \
      case BF_FRAG_6K:                          \
      case BF_FRAG_7K:                          \
        printf(FRAG_TYPE(type)" ");             \
        break;                                  \
      default:                                  \
        printf("*** unknown (%d) ***", type);   \
        break;                                  \
    }

/*****************************************************************************/
int
print_fragpage(bitFileT *bfp, fragSlotT *grpPgp, int flgs)
{
    grpHdrT *grpHdrp;
    unsigned short fragSlot;
    uint32T nextFreeFrag;
    int freeFrags = 0;
    int grpPg = 0;
    int ret;
    int dvn;
    char time_buf[40];

    grpHdrp = (grpHdrT *)grpPgp;

    ctime_r(&grpHdrp->setId.domainId.tv_sec, time_buf, 40);
    time_buf[strlen(time_buf) - 1] = '\0';

    printf("self %d  ", grpHdrp->self);
    printf("fragType ");
    PRINT_FRAG_TYPE(grpHdrp->fragType);
    printf("  version %d\n", grpHdrp->version);
    printf("freeFrags %d  ", grpHdrp->freeFrags);
    printf("firstFrag %d  ", grpHdrp->firstFrag);
    printf("nextFreeGrp %d\n", grpHdrp->nextFreeGrp);
    printf("nextFreeFrag %d  ", grpHdrp->nextFreeFrag);
    printf("lastFreeFrag %d\n", grpHdrp->lastFreeFrag);
    if ((grpHdrp->version == BF_FRAG_VERSION)  &&
        (grpHdrp->lastFreeFrag == BF_FRAG_BAD_GROUP)) {
        printf("WARNING: This group has been marked as BAD by the kernel:  BF_FRAG_BAD_GROUP\n");
    }
    printf("setId %x.%x.%x.%x  (%s)\n",
      grpHdrp->setId.domainId.tv_sec, grpHdrp->setId.domainId.tv_usec,
      grpHdrp->setId.dirTag.num, grpHdrp->setId.dirTag.seq, time_buf);

    if ( bfp->type & FLE ) {
        dvn = UNKNOWN_DVN;
    } else {
        dvn = bfp->dmn->dmnVers;
    }

    ret = test_grpHdr( bfp, grpHdrp, NULL, dvn );

    if ( flgs & VFLG ) {
        uint i;

        printf("freeList:\n");
        if ( grpHdrp->version == 0 ) {
            fprintf(stderr, "*** Warning. "
                   "freeList is not defined for version 0 frag group.\n");
        } else if ( grpHdrp->version != BF_FRAG_VERSION ) {
            fprintf(stderr, "*** Warning. Bad version. "
                   "freeList is only valid for version 1 frag groups.\n");
        }
        for ( i = 0; i < BF_FRAG_GRP_SLOTS; i++ ) {
            if ( grpHdrp->freeList[i] == BF_FRAG_NULL ) {
                printf(" -1  ");
            } else {
                printf("%3u  ", grpHdrp->freeList[i]);
            }
            if ( (i + 1) % 10 == 0 ) {
                printf("\n");
            }
        }

        if ( i % 10 != 0 ) {
            printf("\n");
        }

        if ( grpHdrp->version == BF_FRAG_VERSION ) {
            for ( i = 0; i < BF_FRAG_GRP_SLOTS; i++ ) {
                /* Slots not on free list can be anything. Just test range. */
                test_frag_slot_range(grpHdrp, i, dvn, TRUE);
            }
        }
    }

    if ( flgs & FFLG ) {
        uint i;
        char freeList[BF_FRAG_GRP_SLOTS];

        for ( i = 0; i < BF_FRAG_GRP_SLOTS; i++ ) {
            freeList[i] = FALSE;
        }

        if ( test_free_frag_slots(grpHdrp, BF_FRAG_NEXT_FREE, dvn, TRUE) ) {
            return BAD_DISK_VALUE;
        }

        printf("free slots:\n");
        fragSlot = grpHdrp->freeList[BF_FRAG_NEXT_FREE];
        i = 0;
        while ( fragSlot != BF_FRAG_NULL ) {

            if ( test_free_frag_slots(grpHdrp, fragSlot, dvn, FALSE) ) {
                break;
            }

            if ( freeList[fragSlot] == TRUE ) {
                fprintf(stderr, "*** Circular free frag slot list.\n");
                return BAD_DISK_VALUE;
            }

            freeList[fragSlot] = TRUE;

            printf("%3u  ", fragSlot);

            if ( (i + 1) % 10 == 0 ) {
                printf("\n");
            }

            i++;
            fragSlot = grpHdrp->freeList[fragSlot];
        }

        if ( i % 10 != 0 ) {
            printf("\n");
        }

        if ( fragSlot != BF_FRAG_NULL ) {
            test_free_frag_slots(grpHdrp, fragSlot, dvn, TRUE);
            return BAD_DISK_VALUE;
        }
    }

    return ret;
}

/******************************************************************************/
/*
** Read every group header and print a summary of all the groups.
** 
** Read group header information into in-memory arrays. 'frags'
** contains information about each frag group. 'groupPage' contains
** an array of the page numbers of the groups for each frag type.
** 'freeFrags' contains the total free frags for each frag type.
*/
void
process_summary(bitFileT *bfp, bsBfSetAttrT *bsBfSetAttrp, char *fset, int flgs)
{
    int i, j;
    fragT *frags;                  /* Summary info for each frag group */
    int *groupPage[BF_FRAG_MAX];   /* group page numbers for each group type */
    int totalGroups[BF_FRAG_MAX];  /* total groups of each group type */
    int freeFrags[BF_FRAG_MAX];    /* number of free frags in each group type */
    int groups;
    bsBfSetAttrT setAttr;
    int fileOK;

    print_header(bfp);

    if ( bfp->type & FLE ) {
        /* Send in setAttr for get_frag_grps to fill up. */
        bsBfSetAttrp = &setAttr;
    }

    fileOK = get_frag_grps(bfp, &frags, groupPage, totalGroups,
                           freeFrags, flgs, bsBfSetAttrp);

    if ( !fileOK ) {
        assert(bfp->type & FLE);  /* fileOK only bad for FLE */
        bsBfSetAttrp = NULL;
    }

    print_frag_summary(totalGroups, freeFrags);

    if ( flgs & FFLG && bsBfSetAttrp != NULL && fileOK ) {
        print_freelist_heads(bfp, bsBfSetAttrp, frags);
    }

    if ( flgs & FFLG ) {
        if ( fileOK ) {
            groups = bfp->pages / BF_FRAG_GRP_PGS;
            if ( groups * BF_FRAG_GRP_PGS != bfp->pages ) {
                fprintf(stderr,
                  "Pages in Frag file (%d) should be a multiple of %d\n",
                  bfp->pages, BF_FRAG_GRP_PGS);
            }

            if ( flgs & VFLG ) {
                print_verbose_free_order_frags(bfp, bsBfSetAttrp, frags,
                                               groupPage, totalGroups,
                                               groups);
            } else {
                print_free_frags(bfp, bsBfSetAttrp, frags, groupPage,
                                 totalGroups, groups);
            }
        } else {
            fprintf(stderr,
                   "Set attribute record not saved. Can't print free frags.\n");
        }
    } else if ( flgs & VFLG ) {
        print_verbose_page_order_frags(bfp, frags);
    }

    exit(0);
}

/*****************************************************************************/
/*
** This routine fills in the "frags" array, the "groupPage" array, the
** "totalGroups" array and the "freeFrags" array.
** The "frags" array contains information on every frag group. It is indexed
** by group number, ie, information about the group that starts on frag file
** page 0 (the first group or group 0) is in frags[0]. Information about the
** group that starts at page 16 (the second group) is in frags[1]. The frag
** file can be sparse. Missing groups have an entry in the frags array.
** The "groupPage" array is a doubley indexed array. The first index is
** frag type (0-7). The second index is group number in the frag type. 
** If group 3,5,9 & 15 are 4K frags then group 9 (at page 9*16) is the third
** group of the 4K type. It's index in the "groupPage" array is 2.
** The contents of the "groupPage" array is the starting
** frag file page number for the first page in the specified group.
** The "totalGroups" array is indexed by frag type and contains the number
** of groups of each frag type.
** The "freeFrags" array is indexed by frag type and contains the total number
** of free frags of each frag type.
*/
int
get_frag_grps(bitFileT *bfp, fragT **frags, int **groupPage, int *totalGroups,
              int *freeFrags, int flgs, bsBfSetAttrT *bsBfSetAttrp)
{
    int groups = bfp->pages / BF_FRAG_GRP_PGS;
    int grp;
    int pg;
    int j;
    uint_t type;
    slotsPgT *grpPgp;
    grpHdrT *grpHdrp;
    char grpHdr[BF_FRAG_SLOT_BYTES];
    ssize_t readRet;
    int goodFile = TRUE;
    int ret;
    uint dvn;

    totalGroups[0] = totalGroups[1] = totalGroups[2] = totalGroups[3] = 0;
    totalGroups[4] = totalGroups[5] = totalGroups[6] = totalGroups[7] = 0;
    freeFrags[0] = freeFrags[1] = freeFrags[2] = freeFrags[3] = 0;
    freeFrags[4] = freeFrags[5] = freeFrags[6] = freeFrags[7] = 0;

    *frags = (fragT *)malloc(groups * sizeof(fragT));
    if ( *frags == NULL ) {
        perror("get_frag_grps");
        exit(4);
    }

    for ( type = 0; type < BF_FRAG_MAX; type++ ) {
        groupPage[type] = (int *)malloc(groups * sizeof(int));
        if ( groupPage[type] == NULL ) {
            perror("get_frag_grps");
            exit(4);
        }
    }

    if ( bfp->type & FLE ) {
        dvn = UNKNOWN_DVN;
    } else {
        dvn = bfp->dmn->dmnVers;
    }

    for ( grp = 0; grp < groups; grp++ ) {
        pg = grp * BF_FRAG_GRP_PGS;

        (*frags)[grp].flags = 0;

        if ( bfp->type & FLE ) {
            bfp->pgNum = pg;
            seek_read(bfp, grpHdr, BF_FRAG_SLOT_BYTES, grp * BF_FRAG_SLOT_BYTES,
              "get_frag_grps");

            grpHdrp = (grpHdrT*)grpHdr;
            ret = test_saved_slot(bfp, (fragSlotT*)grpHdr);
            if ( ret == BAD ) {
                /* File not saved by nvfragpg. Therefore no BfSetAttrT. */
                goodFile = FALSE;
                bsBfSetAttrp = NULL;
            } else if ( grp == 0 ) {
                /* Retrieve saved bfSetAttrT record. */
                *bsBfSetAttrp = ((savGrpT*)grpHdr)->setAttr;
            }
            if ( ret == SPARSE ) {
                    (*frags)[grp].lbn = 0;
                    (*frags)[grp].nextFreeGrp = -1;
                    (*frags)[grp].fragType = BF_FRAG_MAX;
                    (*frags)[grp].freeFrags = -1;
                    (*frags)[grp].version = 0;
                    continue;
            }
        } else {  /* bfp->type != FLE */
            /* skip holes */
            if ( !page_mapped(bfp, pg) ) {
                (*frags)[grp].lbn = 0;
                (*frags)[grp].nextFreeGrp = -1;
                (*frags)[grp].fragType = BF_FRAG_MAX;
                (*frags)[grp].freeFrags = -1;
                (*frags)[grp].version = 0;
                continue;
            }

            if (read_page(bfp, pg)) {
                fprintf(stderr, "read frag page %d failed\n", pg);
                exit(1);
            }

            /* test that this frag group has 16 pages mapped */
            for ( j = 1; j < BF_FRAG_GRP_PGS; j++ )
            {
                if ( !page_mapped(bfp, pg + j) ) {
                    fprintf(stderr, "In frag group starting at page %d ", pg);
                    fprintf(stderr, "page %d is not mapped\n", pg + j);
                }
            }

            if ( (grp + 1) % 100 == 0 ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr,
                  " reading %d frag group headers, %d headers read\r",
                  groups, grp + 1);
            }

            grpPgp = (slotsPgT*)PAGE(bfp);       /* not needed. see next line */
            grpHdrp = (grpHdrT *)grpPgp;
        }

        (*frags)[grp].flags |= EXISTS;

        test_grpHdr( bfp, grpHdrp, bsBfSetAttrp, dvn );

        if ( !(bfp->type & FLE) ) {
            (*frags)[grp].lbn = bfp->pgLbn;
        } else {
            (*frags)[grp].lbn = 0;
        }
        (*frags)[grp].nextFreeGrp = grpHdrp->nextFreeGrp;
        (*frags)[grp].fragType = grpHdrp->fragType;
        (*frags)[grp].freeFrags = grpHdrp->freeFrags;
        (*frags)[grp].version = grpHdrp->version;

        type = grpHdrp->fragType;
        if ( type < BF_FRAG_MAX ) {
            groupPage[type][totalGroups[type]] = pg;
            freeFrags[type] += grpHdrp->freeFrags;
            totalGroups[type]++;
        }
    }

    return goodFile;
}

/*****************************************************************************/
/* print disk space in K (1024 bytes) or M (1048576 bytes) */
#define ONE_K 1024

#define PRINT_SPACE(n) {                                                \
    if ( (n) < 100000 ) {                                               \
        printf(" %5dK", (n));                                           \
    } else if ( (n) < 100000 * ONE_K ) {                                \
        printf(" %5dM", ((n) + ONE_K / 2) / ONE_K);                     \
    } else {                                                            \
        printf(" %5dG", ((n) + ONE_K / 2 * ONE_K) / (ONE_K * ONE_K));   \
    }                                                                   \
}

/* Print number in modified scientific notation. Use only 10^3, 10^6 & 10^9. */
#define PRINT_NUM(n) {                                                         \
    if ( (n) < 1000000 ) {   /* 1, 12 & 123 */                                 \
        printf(" %6d", (n));                                                   \
    } else if ( (n) < 10000 ) {   /* 1.23K */                                  \
        printf(" %d.%02dE3", (n) / 1000, ((n) - (n) / 1000 * 1000 + 5) / 10);  \
    } else if ( (n) < 100000 ) {   /* 12.3K */                                 \
        printf(" %d.%01dE3", (n) / 1000, ((n) - (n) / 1000 * 1000 + 50) / 100);\
    } else if ( (n) < 1000000 ) {   /* 123K */                                 \
        printf("  %dE3", ((n) + 500) / 1000);                                  \
    } else if ( (n) < 10000000 ) {   /* 1.23M */                               \
        printf(" %d.%02dE6", (n) / 1000000,                                    \
               ((n) - (n) / 1000000 * 1000000 + 5000) / 10000);                \
    } else if ( (n) < 100000000 ) {   /* 12.3M */                              \
        printf(" %d.%01dE6", (n) / 1000000,                                    \
               ((n) - (n) / 1000000 * 1000000 + 50000) / 100000);              \
    } else if ( (n) < 1000000000 ) {   /* 123M */                              \
        printf("  %dE6", ((n) + 500000) / 1000000);                            \
    } else {   /* 1.23G */                                                     \
        printf(" %d.%02dE9", (n) / 1000000000,                                 \
               ((n) - (n) / 1000000000 * 1000000000 + 5000000) / 10000000);    \
    }                                                                          \
}

#define PRINT_PERCENT(numerator, demoninator) {                                \
    int percent;                                                               \
    if ( (numerator) == 0 ) {                                                  \
        printf("  0%%   ");                                                    \
    } else  if ( (numerator) != (demoninator) ) {                              \
        percent = 100L * ((numerator) + (demoninator) / 200) / (demoninator);  \
        if ( percent == 0 ) {                                                  \
            printf(" <1%%   ");                                                \
        } else if ( percent != 100 ) {                                         \
            printf(" %2d%%   ", percent);                                      \
        } else {                                                               \
            printf(">99%%   ");                                                \
        }                                                                      \
    } else {                                                                   \
        printf("100%%   ");                                                    \
    }                                                                          \
}

/*****************************************************************************/
#define SLOTS_IN_GROUP(n) ((BF_FRAG_GRP_SLOTS - 1) / (n))
#define TOTAL_SLOTS_IN_TYPE(n) ((long)totalGroups[n] * SLOTS_IN_GROUP(n))
#define K_PER_GROUP (BF_FRAG_GRP_PGS * BF_FRAG_PG_BYTES / ONE_K)

void
print_frag_summary(int *totalGroups, int *freeFrags)
{
    int type, percent;
    long used, tot;

    printf("frag type     free     1K     2K     3K     4K     5K     6K     7K   totals\n");

    printf("groups     ");
    tot = 0;
    for ( type = 0; type < BFS_FRAG_MAX; type++ ) {
        PRINT_NUM(totalGroups[type]);
        tot += totalGroups[type];
    }
    printf("  ");
    PRINT_NUM(tot);
    printf("\n");

    printf("frags            -");
    tot = 0;
    for ( type = 1; type < BFS_FRAG_MAX; type++ ) {
        PRINT_NUM(TOTAL_SLOTS_IN_TYPE(type));
        tot += TOTAL_SLOTS_IN_TYPE(type);
    }
    printf("  ");
    PRINT_NUM(tot);
    printf("\n");

    printf("frags used       -");
    tot = 0;
    for ( type = 1; type < BFS_FRAG_MAX; type++ ) {
        PRINT_NUM(TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type]);
        tot += TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type];
    }
    printf("  ");
    PRINT_NUM(tot);
    printf("\n");

    printf("disk space ");
    tot = 0;
    for ( type = 0; type < BFS_FRAG_MAX; type++ ) {
        PRINT_SPACE((long)totalGroups[type] * K_PER_GROUP);
        tot += (long)totalGroups[type] * K_PER_GROUP;
    }
    printf("  ");
    PRINT_SPACE(tot);
    printf("\n");

    printf("space used       -");
    tot = 0;
    for ( type = 1; type < BFS_FRAG_MAX; type++ ) {
        PRINT_SPACE((TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type]) * type);
        tot += (TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type]) * type;
    }
    printf("  ");
    PRINT_SPACE(tot);
    printf("\n");

    printf("space free ");
    PRINT_SPACE(totalGroups[0] * (BF_FRAG_GRP_SLOTS - 1));
    tot = totalGroups[0] * (BF_FRAG_GRP_SLOTS - 1);
    for ( type = 1; type < BFS_FRAG_MAX; type++ ) {
        PRINT_SPACE(freeFrags[type] * type);
        tot += freeFrags[type] * type;
    }
    printf("  ");
    PRINT_SPACE(tot);
    printf("\n");

    printf("overhead   ");
    tot = 0;
    for ( type = 0; type < BFS_FRAG_MAX; type++ ) {
        PRINT_SPACE(totalGroups[type]);
        tot += totalGroups[type];
    }
    printf("  ");
    PRINT_SPACE(tot);
    printf("\n");

    printf("wasted           -     0K");
    for ( type = 2; type < BFS_FRAG_MAX; type++ ) {
        PRINT_SPACE(totalGroups[type]);
    }
    printf("  ");
    PRINT_SPACE(totalGroups[2] + totalGroups[3] + totalGroups[4] * 3 +
                totalGroups[5] * 2 + totalGroups[6] + totalGroups[7]);
    printf("\n");

    printf("%% used           -   ");
    used = 0;
    tot = totalGroups[0] * K_PER_GROUP;
    for ( type = 1; type < BFS_FRAG_MAX; type++ ) {
        PRINT_PERCENT((TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type]) * type,
                      totalGroups[type] * K_PER_GROUP);
        used += (TOTAL_SLOTS_IN_TYPE(type) - freeFrags[type]) * type;
        tot += totalGroups[type] * K_PER_GROUP;
    }
    printf("  ");
    PRINT_PERCENT(used, tot);
    printf("\n");
}

/*****************************************************************************/
void
print_freelist_heads(bitFileT *bfp, bsBfSetAttrT *bsBfSetAttrp, fragT *frags)
{
    int type;

    printf("Head of free lists of frag groups from bsBfSetAttrT record:\n");

    /* Print "BF_FRAG_ANY" line seperately since "ANY" is longer than "1K". */
    printf("frag type BF_FRAG_ANY  firstFreeGrp %6d  lastFreeGrp %6d\n",
          bsBfSetAttrp->fragGrps[BF_FRAG_ANY].firstFreeGrp,
          bsBfSetAttrp->fragGrps[BF_FRAG_ANY].lastFreeGrp);

    for ( type = BF_FRAG_1K; type < BFS_FRAG_MAX; type++ ) {
        uint32T firstFreeGrp = bsBfSetAttrp->fragGrps[type].firstFreeGrp;
        uint32T lastFreeGrp = bsBfSetAttrp->fragGrps[type].lastFreeGrp;

        printf("frag type %s   firstFreeGrp %6d  lastFreeGrp %6d\n",
          FRAG_TYPE(type), firstFreeGrp, lastFreeGrp);
    }

    for ( type = 0; type < BFS_FRAG_MAX; type++ ) {
        uint32T firstFreeGrp = bsBfSetAttrp->fragGrps[type].firstFreeGrp;
        uint32T lastFreeGrp = bsBfSetAttrp->fragGrps[type].lastFreeGrp;
        uint grpFragType;

        if ( firstFreeGrp != BF_FRAG_EOG &&
             !test_freeGrp(bfp, firstFreeGrp, type, "firstFreeGrp") )
        {
            grpFragType = frags[firstFreeGrp / BF_FRAG_GRP_PGS].fragType;
            if ( grpFragType != type ) {
                fprintf(stderr,
               "*** Corrupt bsBfSetAttrT. Bad %s firstFreeGrp. Page %d is %s\n",
               FRAG_TYPE(type), firstFreeGrp, FRAG_TYPE(grpFragType));
            }
        }

        if ( lastFreeGrp != BF_FRAG_EOG &&
             !test_freeGrp(bfp, lastFreeGrp, type, "lastFreeGrp") )
        {
            grpFragType = frags[lastFreeGrp / BF_FRAG_GRP_PGS].fragType;
            if ( grpFragType != type ) {
                fprintf(stderr,
                "*** Corrupt bsBfSetAttrT. Bad %s lastFreeGrp. Page %d is %s\n",
                FRAG_TYPE(type), lastFreeGrp, FRAG_TYPE(grpFragType));
            }
        }
    }

    printf("\n");
}

/*****************************************************************************/
/*
** The frag file is never at lbn 0 so frags[i].lbn == 0 is invalid.
** Use this as a flag that the frags array was filled in by a saved file
** which does not have lbns.
*/
void
print_verbose_page_order_frags(bitFileT *bfp, fragT *frags)
{
    int i;

    for ( i = 0; i < bfp->pages / BF_FRAG_GRP_PGS; i++ ) {
        if ( !(frags[i].flags & EXISTS) ) {
            continue;
        }

        printf("PAGE %4d ", i * BF_FRAG_GRP_PGS);
        if ( frags[i].lbn ) {
            printf("lbn %6d  ", frags[i].lbn);
        }

        PRINT_FRAG_TYPE(frags[i].fragType);
        printf("  version %d  ", frags[i].version);
        printf("freeFrags %3d  ", frags[i].freeFrags);
        printf("nextFreeGrp %d\n", frags[i].nextFreeGrp);
    }
}

/*****************************************************************************/
/*
** This routine prints the output for the -fv flags.
** It prints infomation about each frag group on the free list, in free list
** order, then each frag not on the free list. It does this for all 8 frag
** types.
** 'freeGroups' contains the number of groups with free frags for
** each frag type.
*/
void
print_verbose_free_order_frags(bitFileT *bfp, bsBfSetAttrT *bsBfSetAttrp,
                               fragT *frags, int **groupPage,
                               int *totalGroups, int groups)
{
    int freeGroups[BF_FRAG_MAX];   /* number of groups with frag frags */
    int type;
    int i = 0;

    fill_free_array(bfp, bsBfSetAttrp, frags, groupPage,
                    totalGroups, freeGroups, groups);

    for ( type = 0; type < BF_FRAG_MAX; type++ )
    {
        if ( freeGroups[type] > 0 )
        {
            PRINT_FRAG_TYPE(type);
            printf(" groups on the free list\n");
            for ( i = 0; i < freeGroups[type]; i++ )
            {
                int grp = groupPage[type][i] / BF_FRAG_GRP_PGS;
                printf("PAGE %4d lbn %6d  ",
                  groupPage[type][i], frags[grp].lbn);
                PRINT_FRAG_TYPE(frags[grp].fragType);
                printf("  version %d  ", frags[grp].version);
                printf("freeFrags %3d  ", frags[grp].freeFrags);
                printf("nextFreeGrp %d\n", frags[grp].nextFreeGrp);
            }
        }

        if ( totalGroups[type] > freeGroups[type] )
        {
            PRINT_FRAG_TYPE(type);
            printf(" full groups\n");
            for ( ; i < totalGroups[type]; i++ )
            {
                int grp = groupPage[type][i] / BF_FRAG_GRP_PGS;
                printf("PAGE %4d lbn %6d  ",
                  groupPage[type][i], frags[grp].lbn);
                PRINT_FRAG_TYPE(frags[grp].fragType);
                printf("  version %d  ", frags[grp].version);
                printf("freeFrags %3d  ", frags[grp].freeFrags);
                printf("nextFreeGrp %d\n", frags[grp].nextFreeGrp);
            }
        }
    }
}

/*****************************************************************************/
/*
** For each frag type, this routine prints a list of the free frags, in free
** list order, then prints a list of the full frags.
** 'freeGroups' contains the number of groups with free frags for
** each frag type.
*/
void
print_free_frags(bitFileT *bfp, bsBfSetAttrT *bsBfSetAttrp, fragT *frags,
                 int **groupPage, int *totalGroups, int groups)
{
    int freeGroups[BF_FRAG_MAX];   /* number of groups with frag frags */
    int type;
    int i = 0;

    fill_free_array(bfp, bsBfSetAttrp, frags, groupPage,
                    totalGroups, freeGroups, groups);

    for ( type = 0; type < BF_FRAG_MAX; type++ ) {
        if ( freeGroups[type] > 0 )
        {
            printf("%s groups on the free list\n", FRAG_TYPE(type));

            for ( i = 0; i < freeGroups[type]; ) {
                if ( i % 10 == 0 )
                    printf("       ");

                printf(" %6d", groupPage[type][i]);
                i++;
                if ( i % 10 == 0 )
                    printf("\n");
            }

            if ( i == 0 || i % 10 )
                printf("\n");
        }

        if ( totalGroups[type] > freeGroups[type] ) {
            int j;
            printf("%s full groups\n", FRAG_TYPE(type));

            for ( j = 0; i < totalGroups[type]; i++ ) {
                if ( j % 10 == 0 )
                    printf("       ");
                printf(" %6d", groupPage[type][i]);
                j++;
                if ( j % 10 == 0 )
                    printf("\n");
            }

            if ( j == 0 || j % 10 )
                printf("\n");
        }
    }
    printf("\n");
}

/*****************************************************************************/
/*
** This routine redefines "groupPage" & "totalGroups" arrays.
** This routine is called when the program is displaying groups with free
** frags.
** The groupPage array is still a doubley indexed array
** (see get_frag_grps), however the second index is now indexed by the
** position in the free list of a frag group. The contents is the frag file
** page number. eg if the frag group at page 256 is a 5K group with free
** frags, and it is 4th (index 3) on the 5K free list, then
** groupPage[5][3] = 256. The entries in groupPage beyond the length of the
** free list are the frag groups that have not free space.
** groupPage starts with groups in free list order then continues with
** groups not on the free list in page order.
** The freeGroups array is indexed by the frag type. Its
** contents is the number of free frag groups. eg freeGroups[4] == 18
** means there are 18 free 4K frag groups. freeGroups defines the
** bounary in groupPage between the free groups and the not free groups.
** The totalGroups array is recalculated but still contains the number
** of groups of each type of frag. totalGroups defines the extent of the
** arrays in groupPage.
*/
void
fill_free_array(bitFileT *bfp, bsBfSetAttrT *bsBfSetAttrp, fragT *frags,
                int **groupPage, int *totalGroups, int *freeGroups, int groups)
{
    uint type;
    int i;

    /* First find all the frag groups on the 8 free lists. */
    for ( type = 0; type < BF_FRAG_MAX; type++ ) {
        uint free;
        uint lastfree;
        uint grp;

        totalGroups[type] = 0;

        /* Get the head of the free list from the bsBfSetAttrT record. */
        free = bsBfSetAttrp->fragGrps[type].firstFreeGrp;
        if ( test_freeGrp(bfp, free, type, NULL) ) {
            free = -1;
        }

        while ( free != (uint)-1 ) {
            grp = free / BF_FRAG_GRP_PGS;
            if ( grp >= groups ) {
                /* No need to printf here. Seen in get_frag_grp. */
                break;
            }

            if ( free % BF_FRAG_GRP_PGS ) {
                /* No need to printf here. Seen in get_frag_grp. */
                break;
            }

            if ( frags[grp].flags & SEEN && frags[grp].fragType == type ) {
                fprintf(stderr, "*** Bad %s free list. ", FRAG_TYPE(type));
                fprintf(stderr, "List loop. ");
                fprintf(stderr, "Page %d seen before on this list.\n", free);

                break;
            } else if ( frags[grp].flags & SEEN ) {
                fprintf(stderr, "*** Bad %s free list. ", FRAG_TYPE(type));
                fprintf(stderr, "Cross link. ");
                fprintf(stderr, "Page %d (%s) on another list.\n",
                  free, FRAG_TYPE(frags[grp].fragType));
                break;
            }

            if ( frags[grp].fragType != type ) {
                printf("*** Bad %s free list. Page %d is %s.\n",
                  FRAG_TYPE(type), free, FRAG_TYPE(frags[grp].fragType));
            }

            groupPage[type][totalGroups[type]] = free;
            frags[grp].flags |= SEEN;
            lastfree = free;
            free = frags[grp].nextFreeGrp;
            totalGroups[type]++;
        }

        freeGroups[type] = totalGroups[type];
    }

    /* Find all the frag groups not on the 8 free lists. */
    /* In a non corrupted frag file, these are the full frag groups. */
    for ( i = 0; i < groups; i++ ) {
        if ( frags[i].flags & SEEN )
            continue;

        type = frags[i].fragType;
        if ( type < BF_FRAG_MAX ) {
            groupPage[type][totalGroups[type]] = i * BF_FRAG_GRP_PGS;
            totalGroups[type]++;
        }
    }
}

/*****************************************************************************/
/*
** Write the group header (first 1K) from each frag group to a file.
** Save the bsBfSetAttrT in the first 1K after the group header.
*/
void
dump_meta(bitFileT *bfp, char *argv[], int argc, int optind,
          bsBfSetAttrT *bsBfSetAttrp, int flags)
{
    fragT *frags;
    int groups;
    int dumpFd;
    int ret;
    int pg;
    char *dfile;

    optind++;

    if ( bfp->setTag == 0 ) {
        fprintf(stderr, "Fileset specification needed.\n");
        usage();
    }

    if ( flags & VFLG ) {
        fprintf(stderr, "-v flag ignored with -d flag\n");
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

    dumpFd = creat(dfile, 0644);
    if ( dumpFd == -1 ) {
        fprintf(stderr, "Can't open output file %s\n", dfile);
        perror("dump");
        exit(1);
    }

    for ( pg = 0; pg < bfp->pages; pg += BF_FRAG_GRP_PGS ) {
        if ( page_mapped(bfp, pg) ) {
            ret = read_page(bfp, pg);
            if ( ret != OK ) {
                fprintf(stderr, "dump_meta: read_page failed\n");
                exit(1);
            }
            ((savGrpT*)PAGE(bfp))->exists = TRUE;
        } else {
            bzero(PAGE(bfp), BF_FRAG_SLOT_BYTES);
            ((savGrpT*)PAGE(bfp))->exists = FALSE;
        }

        if ( pg == 0 && bsBfSetAttrp ) {
            if ( sizeof(savGrpT) < BF_FRAG_SLOT_BYTES )
                ((savGrpT*)PAGE(bfp))->setAttr = *bsBfSetAttrp;
            else
                fprintf(stderr, "Not enough room to store frag free heads.\n");
        }

        ret = write(dumpFd, bfp->pgBuf, BF_FRAG_SLOT_BYTES);
        if ( ret != BF_FRAG_SLOT_BYTES ) {
            perror("write");
            exit(1);
        }

        if ( (pg + BF_FRAG_GRP_PGS) / BF_FRAG_GRP_PGS % 100 == 0 ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr, " dumping %d frag groups, %d groups dumped\r",
              bfp->pages / BF_FRAG_GRP_PGS,
              (pg + BF_FRAG_GRP_PGS) / BF_FRAG_GRP_PGS);
        }
    }
    fprintf(stderr, CLEAR_LINE);
    printf("%d frag groups dumped.\n", bfp->pages / 16);

    exit(0);
}

/*****************************************************************************/
void
seek_read(bitFileT *bfp, void *buffer, size_t size, off_t offset, char *routine)
{
    ssize_t readRet;
    char errstr[80];
    errstr[0] = '\0';       /* just in case sprintf fails */

    readRet = pread(bfp->fd, buffer, size, offset);
    if ( readRet != size ) {
        if ( readRet == (ssize_t)-1 ) {
            sprintf(errstr, "%s: pread %d bytes at offset %d failed",
              routine, size, offset);
            perror(errstr);
        } else {
            fprintf(stderr, "%s: read returned %d expected %d\n",
              routine, readRet, size);
        }

        exit(3);
    }
}

/*****************************************************************************/
/*
** Tests saved frag slot (1k bytes).
** The first slot in a frag group is zeroed then filled with a grpHdrT.
** This program saves the grpHdrT for each group in the frag file.
** Frag groups that exist have an 'int' TRUE appended after the grpHdrT.
** Frag groups that don't exist (sparse holes) have an 'int' FALSE appended
** after the grpHdrT. The first group has bsBfSetAttrT appended after the int.
** A good saved slot will have a grpHdrT followed by an int and possibly
** a bsBfSetAttrT followed by zeros.  A manually saved slot (dd skip= count=2)
** will have a grpHdrT followed by zeros.
**
** A slot of all 0's is likely a good saved sparse hole.
** A file with the 'int' flag set to TRUE and all zeros after the bsBfSetAttrT
** is likely to be a good saved slot.
** A file with non zeros after the bsBfSetAttrT is likely not a good frag slot.
** It may have been saved from a corrupted disk or not be a saved frag file.
**
** Input: if bfp->pgNum is 0 then the slot has a saved bfSetAttrT.
**   If the page is not known (-b <block>) then set bfp->pgNum to 0.
** Return OK, SPARSE & BAD
*/
int
test_saved_slot(bitFileT *bfp, fragSlotT *fragSlotp)
{
    uint offset;
    uint i;
    int ret = OK;
    static int once = TRUE;
    int blank = TRUE;

    if ( ((savGrpT*)fragSlotp)->exists != TRUE &&
         ((savGrpT*)fragSlotp)->exists != FALSE )
    {
        ret = BAD;
        goto bad;
    }

    if ( ((savGrpT*)fragSlotp)->exists == FALSE ) {
        /* A saved filed with a sparse group would be all zeros. */
        /* If 'exists' is FALSE and the slot is not all zeros (except saved */
        /* bfSetAttrT) then the file is not a saved frag file. */
        if ( bfp->pgNum != 0 ) {
            /* No saved bfSetAttrT here. Check for all zeros. */
            for ( i = 0; i < BF_FRAG_SLOT_BYTES; i++ ) {
                if ( ((char*)fragSlotp)[i] != 0 ) {
                    ret = BAD;
                    goto bad;
                }
            }
        } else {
            for ( i = 0; i < sizeof(grpHdrT) + sizeof(int); i++ ) {
                if ( ((char*)fragSlotp)[i] != 0 ) {
                    ret = BAD;
                    goto bad;
                }
            }
            /* Skip around saved bfSetAttrT. */
            for ( i = sizeof(savGrpT); i < BF_FRAG_SLOT_BYTES; i++ ) {
                if ( ((char*)fragSlotp)[i] != 0 ) {
                    ret = BAD;
                    goto bad;
                }
            }
        }

        /* The 'exists' flag says it was a saved sparse group and it was */
        /* indeed all zeros. Probably a good saved sparse group. */
        return SPARSE;
    }

    if ( bfp->pgNum == 0 ) {
        offset = sizeof(savGrpT);
    } else {
        offset = sizeof(grpHdrT) + sizeof(int);
    }

    for ( i = offset; i < BF_FRAG_SLOT_BYTES; i++ ) {
        if ( ((char*)fragSlotp)[i] != '\0' ) {
            /* Most likely a bad saved file, but I can't tell for sure. */
            /* It could be a properly saved corrupted file. Print the warning */
            /* but return OK. */
            ret = OK;
            blank = FALSE;
            goto bad;
        }
    }

    /* There's data where there should be data and zeros where there should */
    /* be zeros. Most likely a good saved group slot. */
    return OK;

bad:
    if ( once ) {
        fprintf(stderr, "**** Corrupted saved file. ");
        if ( ret == BAD ) {
            fprintf(stderr, "File was not produced by %s. ****\n", Prog);
        } else {
            fprintf(stderr, "File likely not produced by %s. ****\n", Prog);
        }

        if ( !blank ) {
            fprintf(stderr, "**** Data after grpHdrT is not zero.\n");
        }

        once = FALSE;
    }

    return ret;
}

/*****************************************************************************/
/*
** This routine tests all the components of the group header.
*/
int
test_grpHdr( bitFileT *bfp, grpHdrT *grpHdrp, bsBfSetAttrT *bsBfSetAttrp,
             uint dvn)
{
    static bfSetIdT savSetId = NULL_STRUCT;
    int ret = OK;
    uint32T pgNum = bfp->pgNum;

    if ( test_self( (uint)grpHdrp->self, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_fragType((uint)grpHdrp->fragType, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_version((uint)grpHdrp->version, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_freeFrags( grpHdrp, dvn, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_firstFrag(bfp, grpHdrp, bsBfSetAttrp, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_freeGrp(bfp, grpHdrp->nextFreeGrp, -1, "nextFreeGrp") ) {
        ret = BAD_DISK_VALUE;
    }

    if ( test_xxxFreeFrag(grpHdrp, pgNum) ) {
        ret = BAD_DISK_VALUE;
    }

    if ( pgNum == 0 && bsBfSetAttrp != NULL  ) {
        test_setId( grpHdrp->setId, bsBfSetAttrp->bfSetId, pgNum );
    } else {
        test_setId( grpHdrp->setId, savSetId, pgNum );
    }
    savSetId = grpHdrp->setId;

    return ret;
}

/*****************************************************************************/
/*
** Test the page number (self).
** Each group header has the page number of the first page in the group.
*/
int
test_self( uint self, uint32T pgNum )
{
    if ( pgNum == (uint32T)-1 ) {
        /* We're looking at a block on disk. We don't know the page number. */
        return 0;
    }

    if ( self != pgNum ) {
        fprintf(stderr, "*** Page %u. ", pgNum);
        fprintf(stderr, "Bad self (%d). Expected %u.\n", self, pgNum);
        return 1;
    }

    return 0;
}

/*****************************************************************************/
/*
** Test the frag type.
** There are only 8 vaild frag types, 0 - 7.
*/
int
test_fragType( uint fragType, uint32T pgNum )
{
    if ( fragType >= BF_FRAG_MAX ) {
        if ( pgNum != (uint32T)-1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad fragType (%d). ", fragType);
        } else {
            fprintf(stderr, "*** Bad fragType (%d). ", fragType);
        }
        fprintf(stderr, "Valid range is 0 - 7.\n");

        return 1;
    }

    return 0;
}

/*****************************************************************************/
/*
** Test the frag file version.
** There are only 2 valid versions, 0 & BF_FRAG_VERSION (1).
*/
int
test_version( uint version, uint32T pgNum )
{
    if ( version > 1 ) {
        if ( pgNum != (uint32T)-1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad version (%d). ", version);
        } else {
            fprintf(stderr, "*** Bad version (%d). ", version);
        }
        fprintf(stderr, "Valid versions are 0 & 1.\n");

        return 1;
    }

    return 0;
}

/*****************************************************************************/
/*
** freeFrags is the number of free frags in the frag group. There are
** BF_FRAG_GRP_SLOTS 1K 'slots' in a group. There can't be more than
** that many free frags. For larger frags (2K, 3K, ... 7K) there can't
** be more than BF_FRAG_GRP_SLOTS/type frags in the group.
*/
int
test_freeFrags( grpHdrT *grpHdrp, uint dvn, uint32T pgNum )
{
    uint freeFrags = grpHdrp->freeFrags;
    uint fragType = grpHdrp->fragType;
    uint i;
    char freeList[BF_FRAG_GRP_SLOTS];
    uint32T fragSlot;

    if ( fragType == BF_FRAG_ANY || fragType >= BFS_FRAG_MAX) {
        /* BF_FRAG_ANY groups don't change the free list or freeFrags from */
        /* what it was before. So freeFrags could be previously valid number. */
        if ( freeFrags > BF_FRAG_GRP_SLOTS - 1 ) {
            if ( pgNum != (uint32T)-1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad freeFrags (%d). ", freeFrags);
            } else {
                fprintf(stderr, "*** Bad freeFrags (%d). ", freeFrags);
            }
            fprintf(stderr, "Valid range is 0 - %d.\n", BF_FRAG_GRP_SLOTS - 1);

            return 1;
        }

        return 0;
    } else {
        if ( freeFrags > (BF_FRAG_GRP_SLOTS - 1) / fragType ) {
            if ( pgNum != (uint32T)-1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad freeFrags (%d). ", freeFrags);
            } else {
                fprintf(stderr, "*** Bad freeFrags (%d). ", freeFrags);
            }
            fprintf(stderr, "Valid range is 0 - %d.\n", BF_FRAG_GRP_SLOTS - 1);

            return 1;
        }
    }

    for ( i = 0; i < BF_FRAG_GRP_SLOTS; i++ ) {
        freeList[i] = FALSE;
    }

    fragSlot = grpHdrp->freeList[BF_FRAG_NEXT_FREE];
    i = 0;
    while ( fragSlot != BF_FRAG_NULL ) {
        if ( test_free_frag_slots(grpHdrp, fragSlot, dvn, FALSE) ) {
            break;
        }

        if ( freeList[fragSlot] == TRUE ) {
            break;
        }

        freeList[fragSlot] = TRUE;
        i++;
        fragSlot = grpHdrp->freeList[fragSlot];
    }

    if ( freeFrags != i ) {
        if ( pgNum != (uint32T)-1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad freeFrags (%d). ", freeFrags);
        } else {
            fprintf(stderr, "*** Bad freeFrags (%d). ", freeFrags);
        }
        fprintf(stderr, "Expected %d.\n", i);

        return 1;
    }

    return 0;
}

/*****************************************************************************/
/*
** "firstFrag" is the first usable frag slot in the group. Each frag group is
** 16 pages long and each slot is 1K long. Each frag group uses the the
** first frag slot in the group for frag metadata. The second frag slot
** in each group is the first useful slot.
** When the frag file is extended, it grows by INIT_GRPS(3) groups or less.
** If there is a hole in the frag file it will fill in the hole with up to
** INIT_GRPS groups. Of the three groups, one is used immediately. The other
** 2 are not quite initialized. "firstFrag" is not initialized until the
** frag group is used (it is set to zero). So test_firstFrag may not work
** for 2 groups in the frag file. The 2 groups will be at the end of the
** free list of BF_FRAG_ANY frags. Therefore if the group is BF_FRAG_ANY
** and nextFreeGrp == BF_FRAG_EOG (-1), or the group is BF_FRAG_ANY and
** nextFreeGrp == bsBfSetAttrT.fragGrps[BF_FRAG_FREE_GRPS].lastFreeGrp then 0 is
** a OK value.
*/
int
test_firstFrag( bitFileT *bfp, grpHdrT *grpHdrp, bsBfSetAttrT *bsBfSetAttrp,
                uint32T pgNum)
{
    uint32T goodFirstFrag;

    if ( pgNum != (uint32T)-1 ) {
        assert(pgNum % BF_FRAG_GRP_PGS == 0);
        goodFirstFrag = pgNum * BF_FRAG_PG_SLOTS + 1;
    } else {
        goodFirstFrag = 0;
    }

    if ( grpHdrp->firstFrag == 0 && grpHdrp->fragType == BF_FRAG_ANY ) {
        /* The last 2 allocated BF_FRAG_ANY groups may have firstFrag == 0 */
        if ( grpHdrp->nextFreeGrp == BF_FRAG_EOG ) {
            /* This is the last BF_FRAG_ANY group on the free chain. */
            /* firstFrag == 0 is OK here. */
            return 0;
        }

        if ( bsBfSetAttrp  != NULL ) {
            uint32T lastgrp;

            lastgrp = bsBfSetAttrp->fragGrps[BF_FRAG_FREE_GRPS].lastFreeGrp;
            if ( grpHdrp->nextFreeGrp == lastgrp ) {
                /* Second to the last group. firstFrag == 0 is OK. */
                return 0;
            }
        }

        if ( bfp->type & FLE && grpHdrp->nextFreeGrp < bfp->pages ) {
            fragSlotT fragSlot;
            off_t lseekOff;

            lseekOff = grpHdrp->nextFreeGrp /
                           BF_FRAG_GRP_PGS * BF_FRAG_SLOT_BYTES;
            seek_read(bfp, &fragSlot, BF_FRAG_SLOT_BYTES,
                          lseekOff, "test_firstFrag");
            if ( test_saved_slot(bfp, &fragSlot) == OK &&
                 ((savGrpT*)&fragSlot)->fragHdr.nextFreeGrp == BF_FRAG_EOG )
            {
                return 0;
            }
        } else if ( !(bfp->type & BLK) && grpHdrp->nextFreeGrp < bfp->pages &&
                    page_mapped(bfp, grpHdrp->nextFreeGrp) &&
                    read_page(bfp, grpHdrp->nextFreeGrp) == OK &&
                    ((grpHdrT*)PAGE(bfp))->nextFreeGrp == BF_FRAG_EOG )
        {
            return 0;
        }
    }

    if ( goodFirstFrag != 0 ) {
        if ( grpHdrp->firstFrag != goodFirstFrag ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad firstFrag (%d). ", grpHdrp->firstFrag);
            fprintf(stderr, "Should be %d for this page.\n", goodFirstFrag);

            return 1;
        }
    } else {
        if ( grpHdrp->firstFrag % BF_FRAG_PG_SLOTS != 1 ) {
            fprintf(stderr, "*** Bad firstFrag (%d).  ", grpHdrp->firstFrag);
            fprintf(stderr, "Must be %d * n + 1.\n",
              BF_FRAG_GRP_PGS * BF_FRAG_PG_SLOTS);

            return 1;
        }
    }

    return 0;
}

/*****************************************************************************/
/*
** test firstFreeGrp, lastFreeGrp & nextFreeGrp.
** Test that frag page number is reasonable.
** It must be the page number of a page within the bounds of the frag file
** and it must be a mapped page, ie not a sparse hole.
** It must be divisible by BF_FRAG_GRP_PGS (16).
** Frag page numbers are used to link the free list of frag groups for
** each group type. The free list is headed by an entry in the bfSetAttrT
** record. Each frag group header has a nextFreeGrp entry.
** This routine checks page numbers from both of these sources.
** If the 'type' argument is == -1, 'freeGrp' came from a nextFreeGrp entry.
** bfp->pgNum is used to identify the page contained the bad
** nextFreeGrp entry.
** If 'type' is >= 0 the bfSetAttrT head of free list is being checked.
** 'type' idendifies the frag group (1K, 2K, ... )
*/
int
test_freeGrp( bitFileT *bfp, uint32T freeGrp, int type, char *head )
{
    char grpHdr[BF_FRAG_SLOT_BYTES];
    ssize_t readRet;
    off_t lseekOff;
    uint fragType;
    int bfSetAttrFlg = FALSE;
    uint32T pgNum = bfp->pgNum;

    if ( freeGrp == (uint)-1 ) {
        /* -1 is an OK page in a free list since it is used as the terminator */
        return 0;
    }

    if ( type >= 0 ) {
        /* Test firstFreeGrp/lastFreeGrp from the bfSetAttrT record. */
        fragType = type;
        bfSetAttrFlg = TRUE;
    }

    /* Test that the page is within range. */
    if ( freeGrp >= bfp->pages ) {
        if ( head == NULL ) {
            return 1;
        }

        if ( bfSetAttrFlg ) {
            fprintf(stderr, "*** Bad %s %s (%d). ",
              FRAG_TYPE(fragType), head, freeGrp);
            fprintf(stderr, "Valid range 0 - %d\n",
              bfp->pages - BF_FRAG_GRP_PGS);
        } else {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad nextFreeGrp (%d). ", freeGrp);
            } else {
                fprintf(stderr, "*** Bad nextFreeGrp (%d). ", freeGrp);
            }
            fprintf(stderr, "Valid range is 0 - %d.\n",
              bfp->pages - BF_FRAG_GRP_PGS);
        }
        return 1;
    }

    /* Test that the page is divisible by 16. */
    if ( freeGrp % BF_FRAG_GRP_PGS ) {
        if ( head == NULL ) {
            return 1;
        }

        if ( bfSetAttrFlg ) {
            fprintf(stderr, "*** Bad %s %s (%d). ",
              FRAG_TYPE(fragType), head, freeGrp);
        } else {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad nextFreeGrp (%d). ", freeGrp);
            } else {
                fprintf(stderr, "*** Bad nextFreeGrp (%d). ", freeGrp);
            }
        }
        fprintf(stderr, "Not divisible by %d.\n", BF_FRAG_GRP_PGS);

        return 1;
    }

    /* Test that the page exists. */
    if ( bfp->type & FLE ) {
        lseekOff = freeGrp / BF_FRAG_GRP_PGS * BF_FRAG_SLOT_BYTES;
        seek_read(bfp, grpHdr, BF_FRAG_SLOT_BYTES, lseekOff, "test_freeGrp");

        bfp->pgNum = freeGrp;
        /* Test page 'freeGrp' in the save file. */
        if ( test_saved_slot( bfp, (fragSlotT*)grpHdr ) == SPARSE ) {
            if ( head == NULL ) {
                return 1;
            }

            if ( bfSetAttrFlg ) {
                fprintf(stderr, "*** Bad %s %s (%d). "
                                "Page is in a sparse hole.\n",
                  FRAG_TYPE(fragType), head, freeGrp);
            } else {
                if ( pgNum != -1 ) {
                    fprintf(stderr, "*** Page %u. ", pgNum);
                    fprintf(stderr, "Bad nextFreeGrp (%d). ", freeGrp);
                } else {
                    fprintf(stderr, "*** Bad nextFreeGrp (%d). ", freeGrp);
                }
                fprintf(stderr, "Page is in a sparse hole.\n");
            }
            return 1;
        }
    } else if ( !(bfp->type & BLK) ) {
        /* The other 15 pages in the group will be tested in get_frag_grps. */
        if ( !page_mapped(bfp, freeGrp) ) {
            if ( head == NULL ) {
                return 1;
            }

            if ( bfSetAttrFlg ) {
                fprintf(stderr, "*** Bad %s %s (%d). ",
                  FRAG_TYPE(fragType), head, freeGrp);
            } else {
                if ( pgNum != -1 ) {
                    fprintf(stderr, "*** Page %u. ", pgNum);
                    fprintf(stderr, "Bad nextFreeGrp (%d). ", freeGrp);
                } else {
                    fprintf(stderr, "*** Bad nextFreeGrp (%d). ", freeGrp);
                }
            }
            fprintf(stderr, "Page is in a sparse hole.\n");
            return 1;
        }
    }

    return 0;
}

/*****************************************************************************/
/*
** Test nextFreeFrag and lastFreeFrag.
** In frag file version 0 these two fields are indexes into 1K slots
** in the frag group. As such, they both must be >= 1 and <= 127. Also
** for all frag groups except BF_FRAG_ANY and BF_FRAG_1K only
** certain values are valid since, eg, a 4K frag can't start at
** the third K in the group.
** Frag file version 0 code has not been checked on a real domain since
** frag file version 0 domains are rare (nonexistant?).
** In frag file version 1 these fields were both unused and set to -1.
** Later (without changing the frag file version) lastFreeFrag was used
** as a flag to indicated that the frag group had a free list problem
** and was not going to be used anymore.
*/
int
test_xxxFreeFrag( grpHdrT *grpHdrp, uint32T pgNum )
{
    int ret = OK;

    if ( grpHdrp->version == 0 ) {
        /* Check that nextFreeFrag is within proper range. */
        if ( grpHdrp->nextFreeFrag == 0 ||
             grpHdrp->nextFreeFrag >= BF_FRAG_GRP_SLOTS )
        {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad %s (%d). ", "nextFreeFrag",
                  grpHdrp->nextFreeFrag);
            } else {
                fprintf(stderr, "*** Bad %s (%d). ", "nextFreeFrag",
                  grpHdrp->nextFreeFrag);
            }
            fprintf(stderr, "Frag slot must be 1 - %d.\n",
              BF_FRAG_GRP_SLOTS - 1);

            ret = BAD;
        }

        /* Check that nextFreeFrag points to slot with the proper alignment. */
        /* Frag type BF_FRAG_ANY and BF_FRAG_1K can start on any slot */
        if ( (uint)grpHdrp->fragType < BF_FRAG_MAX && grpHdrp->fragType > 1 ) {
            /* If the frag type is OK and not 0 or 1 then only some are OK */
            if ( grpHdrp->nextFreeFrag % grpHdrp->fragType != 1 ) {
                fprintf(stderr, "Bad %s (%d). ", "nextFreeFrag",
                  grpHdrp->nextFreeFrag);
                fprintf(stderr, "A type %s frag can not start in this slot.\n",
                  FRAG_TYPE(grpHdrp->fragType));

                ret = BAD;
            }
        }

        /* Check that lastFreeFrag is within proper range. */
        if ( grpHdrp->lastFreeFrag == 0 ||
             grpHdrp->lastFreeFrag >= BF_FRAG_GRP_SLOTS )
        {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad %s (%d). ", "lastFreeFrag",
                  grpHdrp->lastFreeFrag);
            } else {
                fprintf(stderr, "Bad %s (%d). ", "lastFreeFrag",
                  grpHdrp->lastFreeFrag);
            }
            fprintf(stderr, "Frag slot must be 1 - %d.\n",
              BF_FRAG_GRP_SLOTS - 1);

            ret = BAD;
        }

        /* Check alignment. */
        if ( (uint)grpHdrp->fragType < BF_FRAG_MAX && grpHdrp->fragType > 1 ) {
            if ( grpHdrp->lastFreeFrag % grpHdrp->fragType != 1 ) {
                fprintf(stderr,"Bad %s (%d). ",
                  "lastFreeFrag", grpHdrp->lastFreeFrag);
                fprintf(stderr, "A type %s frag can not start in this slot.\n",
                  FRAG_TYPE(grpHdrp->fragType));

                ret = BAD;
            }
        }

        return ret;
    }

    if ( grpHdrp->version == BF_FRAG_VERSION ) {
        /* Only valid value is BF_FRAG_EOF (-1). */
        if ( grpHdrp->nextFreeFrag != BF_FRAG_EOF ) {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad %s (%d). ",
                  "nextFreeFrag", grpHdrp->nextFreeFrag);
            } else {
                fprintf(stderr, "*** Bad %s (%d). ",
                  "nextFreeFrag", grpHdrp->nextFreeFrag);
            }
            fprintf(stderr, "Should be -1 for this frag version.\n");
            ret = BAD;
        }

        /* lastFreeFrag can be -1 or the new flag BF_FRAG_BAD_GROUP (1). */
        if ( grpHdrp->lastFreeFrag != BF_FRAG_EOF &&
             grpHdrp->lastFreeFrag != BF_FRAG_BAD_GROUP )
        {
            if ( pgNum != -1 ) {
                fprintf(stderr, "*** Page %u. ", pgNum);
                fprintf(stderr, "Bad %s (%d). ",
                  "lastFreeFrag", grpHdrp->lastFreeFrag);
            } else {
                fprintf(stderr, "*** Bad %s (%d). ",
                  "lastFreeFrag", grpHdrp->lastFreeFrag);
            }
            fprintf(stderr, "Should be -1 or 1 for this frag version.\n");
            ret = BAD;
        }

        return ret;
    }

    /* The frag version is corrupt. Try both version 0 & 1 forms. */
    if ( grpHdrp->nextFreeFrag != BF_FRAG_EOF &&
         (grpHdrp->nextFreeFrag == 0 ||
          grpHdrp->nextFreeFrag >= BF_FRAG_GRP_SLOTS) )
    {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ",  pgNum);
            fprintf(stderr, "Bad %s (%d). ", "nextFreeFrag",
              grpHdrp->nextFreeFrag);
        } else {
            fprintf(stderr, "*** Bad %s (%d). ", "nextFreeFrag",
              grpHdrp->nextFreeFrag);
        }
        fprintf(stderr, "Valid values are -1 & 1 - %d.\n",
          BF_FRAG_GRP_SLOTS - 1);
        ret = BAD;
    }

    if ( grpHdrp->lastFreeFrag != BF_FRAG_EOF &&
         (grpHdrp->lastFreeFrag == 0 ||
          grpHdrp->lastFreeFrag >= BF_FRAG_GRP_SLOTS) )
    {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad %s (%d). ", "lastFreeFrag",
              grpHdrp->lastFreeFrag);
        } else {
            fprintf(stderr, "*** Bad %s (%d). ", "lastFreeFrag",
              grpHdrp->lastFreeFrag);
        }
        fprintf(stderr, "Valid values are -1 & 1 - %d.\n",
          BF_FRAG_GRP_SLOTS - 1);
        ret = BAD;
    }

    return ret;
}

/*****************************************************************************/
/*
** Test the setId.
** The setId is composed of 4 ints: the domainId tv_sec & tv_usec and
** the dirTag num & seq. The domainId tv_sec is the time when the domain
** was created. It can be any number, but the only way it can be a time
** in the distant past or future is if the system time is wrong. Due to
** different time zones even a correct system time can be a few hours
** in the future. tv_usec should be < 1,000,000. The dirTag is the set
** tag. dirTag.num is the tag number and must be >= 1 and < 2B (ie,
** although it is an unsigned number, it must be signed positive).
** seq must have BS_TD_IN_USE (0x8000) set and be < BS_TD_DEAD_SLOT (0x7fff).
** Furthermore, each group has the setId, and the bfSetAttr has the setId.
** All these setIds must be the same.
*/

#define ONE_DAY (24 * 60 * 60)

int
test_setId( bfSetIdT testSetid, bfSetIdT refSetId, uint32T pgNum )
{
    bfSetIdT nilBfSetId = NULL_STRUCT;
    time_t tsec;
    struct tm date = NULL_STRUCT;

    (void)time(&tsec);

    if ( testSetid.domainId.tv_sec > tsec + ONE_DAY ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Suspect setId.domainId.tv_sec (0x%x). ",
              testSetid.domainId.tv_sec);
        } else {
            fprintf(stderr, "*** Suspect setId.domainId.tv_sec (0x%x). ",
              testSetid.domainId.tv_sec);
        }
        fprintf(stderr, "Impossible future date.\n");
    }

    date.tm_year = 90;  /* 1990 */
    date.tm_mday = 1;
    tsec = mktime(&date);
    if ( testSetid.domainId.tv_sec < tsec ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Suspect setId.domainId.tv_sec (0x%x). ",
              testSetid.domainId.tv_sec);
        } else {
            fprintf(stderr, "*** Suspect setId.domainId.tv_sec (0x%x). ",
              testSetid.domainId.tv_sec);
        }
        fprintf(stderr, "Impossibly early date.\n");
    }

    if ( (unsigned)testSetid.domainId.tv_usec > 1000000 ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad setId.domainId.tv_usec (0x%x). ",
              testSetid.domainId.tv_usec);
        } else {
            fprintf(stderr, "*** Bad setId.domainId.tv_usec (0x%x). ",
              testSetid.domainId.tv_usec);
        }
        fprintf(stderr, "Should be < 1000000\n");
    }

    if ( (signed)testSetid.dirTag.num < 1 ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad setId.dirTag.num (0x%x). ",
              testSetid.dirTag.num);
        } else {
            fprintf(stderr, "*** Bad setId.dirTag.num (0x%x). ",
              testSetid.dirTag.num);
        }
        fprintf(stderr, "Valid range: 1 - 0x7fffffff\n");
    }

    if ( !(testSetid.dirTag.seq & BS_TD_IN_USE) ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad setId.dirTag.seq (0x%x). ",
              testSetid.dirTag.seq);
        } else {
            fprintf(stderr, "*** Bad setId.dirTag.seq (0x%x). ",
              testSetid.dirTag.seq);
        }
        fprintf(stderr, "BS_TD_IN_USE not set.\n");
    }

    if ( testSetid.dirTag.seq > (BS_TD_IN_USE | BS_TD_DEAD_SLOT) ) {
        if ( pgNum != -1 ) {
            fprintf(stderr, "*** Page %u. ", pgNum);
            fprintf(stderr, "Bad setId.dirTag.seq (0x%x). ",
              testSetid.dirTag.seq);
        } else {
            fprintf(stderr, "*** Bad setId.dirTag.seq (0x%x). ",
              testSetid.dirTag.seq);
        }
        fprintf(stderr, "Valid range is 1 - 0x%x.\n",
          (BS_TD_IN_USE | BS_TD_DEAD_SLOT) - 1);
    }

    if ( BS_BFS_EQL(testSetid, refSetId) ) {
        return 0;
    } else if ( BS_BFS_EQL(refSetId, nilBfSetId) ) {
        return 0;
    } else {
        /* Only way for pgNum to be not set is for this to be a -b request. */
        /* In that case, there will be no refSetId (refSetId==nilBfSetId). */
        assert(pgNum != -1);
        fprintf(stderr, "*** Page %u. ", pgNum);
        fprintf(stderr, "Different setId than last seen (%x.%x.%x.%x).\n",
          refSetId.domainId.tv_sec, refSetId.domainId.tv_usec,
          refSetId.dirTag.num, refSetId.dirTag.seq);
        return 1;
    }
}

/*****************************************************************************/
/*
** All the freeList slots should conatain values from 0 to 127 (or -1).
*/
int
test_frag_slot_range( grpHdrT *grpHdrp, uint index, uint dvn, int prtFlg )
{
    unsigned short contents;
    bfFragT fragType = grpHdrp->fragType;

    assert(index < BF_FRAG_GRP_SLOTS);
    contents = grpHdrp->freeList[index];

    /* Check the range. All slots should be 0, 1-127 or  -1. */
    if ( contents >= BF_FRAG_GRP_SLOTS  && contents != BF_FRAG_NULL ) {
        if ( !prtFlg ) {
            return 1;
        }

        fprintf(stderr, "*** Bad frag slot (%d) in freeList[%d]. ",
          contents, index);
        fprintf(stderr, "Frag slot must be 1 - %d.\n", BF_FRAG_GRP_SLOTS - 1);

        return 1;
    }

    return 0;
}

/*****************************************************************************/
/*
** Free slots in the freeList follow a pattern based on the frag type.
** For domain version number (DVN) 4 and higher, 2K & 6K frags start
** at slot 2. 4K frags start at slot 4. For older domains, all frags
** start at slot 1. Saved frag files do not save the DVN so 2K, 4K & 6K
** frags have 2 OK sets of slots.
** This is only called when printing a frag page (vs frag summary) so
** the frag page number is not printed.
*/
int
test_free_frag_slots( grpHdrT *grpHdrp, uint index, uint dvn, int prtFlg )
{
    unsigned short contents;
    bfFragT fragType = grpHdrp->fragType;

    assert(index < BF_FRAG_GRP_SLOTS);
    contents = grpHdrp->freeList[index];

    /* Check the range. All slots should be 0, 1-127 or  -1. */
    if ( test_frag_slot_range(grpHdrp, index, dvn, prtFlg) == 1 ) {
        return 1;
    }

    if ( fragType >= BFS_FRAG_MAX ) {
        /* If the fragType is corrupt we can't say anything about the slot. */
        return 0;
    }

    /* Slot 0 must contain a valid slot or -1. */
    if ( index == BF_FRAG_NEXT_FREE ) {
        if ( contents == BF_FRAG_NULL ) {
            return 0;
        }

        return test_frag_slot( contents, grpHdrp, index, dvn, prtFlg );
    }

    if ( fragType == BF_FRAG_ANY ) {
        /* Old slots are not cleared so they can be anything. */
        return 0;
    }

    /* Some slots contain the list while other slots contain a 0. */
    if ( test_frag_slot(index, grpHdrp, index, dvn, FALSE) == 0 ) {
        /* This slot conatains the list. Test the contents. */
        if ( contents == BF_FRAG_NULL ) {
            /* End of the free list (-1), an OK value. */
            return 0;
        }

        if ( dvn == UNKNOWN_DVN && contents == 0 ) {
            /* Even in the list, unused slots can contain 0. */
            return 0;
        }

        /* The contents in this slot must point to another slot in the list. */
        return test_frag_slot( contents, grpHdrp, index, dvn, prtFlg );
    }

    /* For those slots not on the free list, the contents can be a value */
    /* left over from previous use as another frag type. */
    /* So that's all the checking we can do. */
    return 0;
}

/*****************************************************************************/
/*
** Test that the frag slot is valid for a given frag type and dvn.
** Never called for BF_FRAG_ANY frag groups.
** Valid slots for 1K frag groups are 1-127.
** Valid slots for 2K frag groups in a V3 domain are 1,3,2n+1,125 (!127)
** Valid slots for 2K frag groups in a V4 domain are 2,4,2n,126
** Valid slots for 3K frag groups are 1,4,3n+1,123 (!127)
** Valid slots for 4K frag groups in a V3 domain are 1,5,4n+1,121 (!125)
** Valid slots for 4K frag groups in a V4 domain are 2,6,4n+2,122 (!126)
** Valid slots for 5K frag groups are 1,6,5n+1,121 (!126)
** Valid slots for 6K frag groups in a V3 domain are 1,7,6n+1,121 (!127)
** Valid slots for 6K frag groups in a V4 domain are 2,8,6n+2,122
** Valid slots for 7K frag groups are 1,8,7n+1,120 (!127)
*/
int
test_frag_slot( ushort slot, grpHdrT *grpHdrp, uint index, uint dvn, int prtFlg)
{
    bfFragT fragType = grpHdrp->fragType;
    int ret = 0;

    assert(fragType != BF_FRAG_ANY);

    /* Check the range. All slots should be 1-127. */
    if ( slot >= BF_FRAG_GRP_SLOTS ) {
        if ( prtFlg ) {
            fprintf(stderr, "*** Bad frag slot (%d) in freeList[%d]. ",
              slot, index);
            fprintf(stderr, "Frag slot must be 1 - %d.\n",
              BF_FRAG_GRP_SLOTS - 1);
        }

        return 1;
    }

    if ( fragType >= BFS_FRAG_MAX ) {
        /* If the fragType is corrupt we can't say anything about the slot. */
        return 0;
    }

    switch ( fragType ) {
      case BF_FRAG_1K:
        break;

      case BF_FRAG_2K:
        if ( dvn == UNKNOWN_DVN ) {
            if ( slot == 127 ) {
                ret = 1;
            }
        } else if ( dvn < FIRST_MIN_FRAG_CROSSINGS_VERSION ) {
            if ( slot == 127 || slot % BF_FRAG_2K != 1 ) {
                ret = 1;
            }
        } else {
            if ( slot % BF_FRAG_2K != 0 ) {
                ret = 1;
            }
        }

        break;

      case BF_FRAG_3K:
        if ( slot == 127 || slot % BF_FRAG_3K != 1 ) {
            ret = 1;
        }

        break;

      case BF_FRAG_4K:
        if ( dvn == UNKNOWN_DVN ) {
            if ( slot == 125 || slot == 126 ||
                 slot % BF_FRAG_4K != 1 &&
                 slot % BF_FRAG_4K != 2 )
            {
                ret = 1;
            }
        } else if ( dvn < FIRST_MIN_FRAG_CROSSINGS_VERSION ) {
            if ( slot == 125 || slot % BF_FRAG_4K != 1 ) {
                ret = 1;
            }
        } else {
            if ( slot == 126 || slot % BF_FRAG_4K != 2 ) {
                ret = 1;
            }
        }

        break;

      case BF_FRAG_5K:
        if ( slot == 126 || slot % BF_FRAG_5K != 1 ) {
            ret = 1;
        }

        break;

      case BF_FRAG_6K:
        if ( dvn == UNKNOWN_DVN ) {
            if ( slot == 127 ||
                 slot % BF_FRAG_6K != 1 &&
                 slot % BF_FRAG_6K != 2 )
            {
                ret = 1;
            }
        } else if ( dvn < FIRST_MIN_FRAG_CROSSINGS_VERSION ) {
            if ( slot == 127 || slot % BF_FRAG_6K != 1 ) {
                ret = 1;
            }
        } else {
            if ( slot % BF_FRAG_6K != 2 ) {
                ret = 1;
            }
        }

        break;

      case BF_FRAG_7K:
        if ( slot == 127 || slot % BF_FRAG_7K != 1 ) {
            ret = 1;
        }

        break;
    }

    if ( ret == 1 ) {
        if ( prtFlg ) {
            fprintf(stderr, "*** Bad frag slot (%d) in freeList[%d]. ",
              slot, index);
            fprintf(stderr, "Invalid slot for frag type %s.\n",
              FRAG_TYPE(fragType));
        }

        return 1;
    }

    return 0;
}

mod_attr_dmnid(bitFileT *cloneBfp, bsBfSetAttrT *cloneSetAttrp)
{
    bsBfSetAttrT *origSetAttrp = NULL;
    int ret;
    bfDomainIdT origbfDomainId;
    bfDomainIdT clonebfDomainId;
    bfTagT origTag;
    bfTagT cloneTag;

    if ( cloneSetAttrp->origSetTag.num == 0 ) {
        fprintf(stderr, "Set is a clone, yet origSetTag.num == 0\n");
        return;
    }

    ret = find_rec(cloneBfp->origSetp, BSR_BFS_ATTR, (char**)&origSetAttrp);
    if ( ret != OK ) {
        fprintf(stderr, "Could not find bsBfSetAttr for orig set.\n");
        return;
    }

    origbfDomainId = origSetAttrp->bfSetId.domainId;
    clonebfDomainId = cloneSetAttrp->bfSetId.domainId;
    if ( !BS_UID_EQL(origbfDomainId, clonebfDomainId) ) {
        fprintf(stderr, "Original set domain ID does not match clone set domain ID\n");
        return;
    }

    origTag = cloneSetAttrp->origSetTag;
    if ( !BS_BFTAG_EQL(origTag, origSetAttrp->bfSetId.dirTag) ) {
        fprintf(stderr, "Clone's origSetTag doesn't match original set's tag.\n");
        return;
    }

    cloneTag = origSetAttrp->nextCloneSetTag;
    if ( !BS_BFTAG_EQL(cloneTag, cloneSetAttrp->bfSetId.dirTag) ) {
        fprintf(stderr, "Original set's nextCloneSetTag doesn't match clone set's tag.\n");
        return;
    }

    /* Since the clone's pages are copies of (or are) the original set's */
    /* pages, the set ID info in the first page is the original set's ID. */
    /* In test_setId, we compare the expected set ID (from bsBfSetAttr) */
    /* to the set ID info in the first page of the frag file. Since this */
    /* is a clone, we'll put the orig set ID in the in-mem copy of the */
    /* bsBfSetAttr so that the comparison will succeed. */
    cloneSetAttrp->bfSetId.dirTag = origSetAttrp->bfSetId.dirTag;
}
