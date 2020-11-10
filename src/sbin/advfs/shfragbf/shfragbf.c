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
 */
/*
 * HISTORY
 */
#ifndef lint
static char *rcsid = "@(#)$RCSfile: shfragbf.c,v $ $Revision: 1.1.27.1 $ (DEC) $Date: 2001/12/14 19:15:55 $";
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <stdio.h>
#include <strings.h>
#include <fcntl.h>
#include <dirent.h>
#include <msfs/bs_error.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>

#include <locale.h>
#include "shfragbf_msg.h"
nl_catd catd;

extern int errno;

void usage( void )
{
fprintf( stderr, 
catgets(catd, 1, USAGE, "usage: shfragbf [-t 0|1|2|3|4|5|6|7] [-v] fragBf\n") );
}

int
read_pg(
    int fd,
    int pg,
    slotsPgT *pgp
    )
{
    off_t pos;

    pos = lseek( fd, pg * 8192L, SEEK_SET );
    if (pos < 0) {
        perror( catgets(catd, 1, 2, "lseek") );
        return pos;
    }

    return read( fd, pgp, 8192 );
}

main( int argc, char *argv[] )
{
    int fd, c, s = 0, n = 0, e = 0, t = 0, v = 0;
    char *pp, *fragBf;
    extern int optind;
    extern char *optarg;
    int pg, endGrp = 0, startGrp = 0, numGrps = 0, fType = -1;
    struct stat stats;
    slotsPgT grppg;
    int waste, grps, maxFree, h, freeBlks, sts, tt, freeFrags, grpFType;
    struct summary {
        int cnt;
        int badCnt;
        int freeFrags;
        int histogram[12];
    };
    struct summary grpSummary[8] = { 0 };
    void *xtntmap;
    long nextbyte, curbyte;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_SHFRAGBF, NL_CAT_LOCALE);

    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd, 1, 34,
                "\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

    while ((c = getopt( argc, argv, "e:n:s:t:v" )) != EOF) {
        switch (c) {
            case 'e':
                e++;
                endGrp = strtoul( optarg, &pp, 0 );
                break;

            case 'n':
                n++;
                numGrps = strtoul( optarg, &pp, 0 );
                break;

            case 's':
                s++;
                startGrp = strtoul( optarg, &pp, 0 );
                break;

            case 't':
                t++;
                fType = strtoul( optarg, &pp, 0 );
                if (fType > 7) {
                    usage();
                    exit( 1 );
                }
                break;

            case 'v':
                v++;
                break;

            case '?':

            default:
                usage();
                exit( 1 );
        }
    }

    if (e > 1 || n > 1 || s > 1 || t > 1 || v > 1) {
        usage();
        exit( 1 );
    }

    if (optind != (argc - 1)) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    fragBf = argv[optind];

    fd = open( fragBf, O_RDONLY, 0 );
    if (fd < 0) {
        perror( catgets(catd, 1, 3, "open()") );
        exit( 1 );
    }

    if (fstat( fd, &stats )) {
        perror( catgets(catd, 1, 4, "fstat()") );
        exit( 1 );
    }

    xtntmap = advfs_xtntmap_open( fd, &sts, 0 );
    if (xtntmap == NULL) {
        printf( catgets(catd, 1, 5, "advfs_xtntmap_open() error; %s\n"), BSERRMSG( sts ) );
        return( 1 );
    }

    curbyte = -1;
    nextbyte = 0;

    advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte );

    pg = nextbyte / 8192;

    while ((read_pg( fd, pg, &grppg ) ) > 0) {
        sts = check_group( fd, pg, &grppg, fType, v, &freeFrags, &grpFType );
        if (sts > 0) {
            maxFree = FRAGS_PER_GRP( grpFType );
            grpSummary[ grpFType ].cnt++;
            grpSummary[ grpFType ].freeFrags += freeFrags;
	    
	    if (0 == freeFrags) {
		/*
		 * Full ones get their own entry in the histrogram.
		 */
		h = 0;
	    } else {
		h = (int) ((double) freeFrags / (double) maxFree * 10.0);
		/*
		 * Need to increment h by 1 so it goes into the 
		 * correct slot, full ones now go in slot 0.
		 */
		h++;
	    }
	    grpSummary[ grpFType ].histogram[ h ]++;

            if (sts == 2) {
                grpSummary[ grpFType ].badCnt++;
            }
        }

        curbyte = nextbyte + 16*8192 - 1;

        if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
            break;
        }
        
        pg = nextbyte / 8192;
    }

    freeBlks = 0;
    waste = 0;
    grps = 0;

    printf( catgets(catd, 1, 6, "\nType    Grps     bad   Frags    free  in-use   Bytes    free  in-use\n") );
    printf( catgets(catd, 1, 7, "----  ------  ------  ------  ------  ------  ------  ------  ------\n") );
  
    for (tt = 0; tt < 8; tt++) {
        int fragsPerGroup = FRAGS_PER_GRP( tt );

        if ((fType == -1) || (fType == tt)) {

            printf( catgets(catd, 1, 8, "  %dk %7d %7d %7d %7d %7d %6dk %6dk %6dk\n"),
                    tt,
                    grpSummary[ tt ].cnt, 
                    grpSummary[ tt ].badCnt, 
                    grpSummary[ tt ].cnt * fragsPerGroup, 
                    grpSummary[ tt ].freeFrags,
                    grpSummary[ tt ].cnt * fragsPerGroup -
                        grpSummary[ tt ].freeFrags,
                    (grpSummary[ tt ].cnt * fragsPerGroup) * tt, 
                    grpSummary[ tt ].freeFrags * tt,
                    (grpSummary[ tt ].cnt * fragsPerGroup -
                        grpSummary[ tt ].freeFrags) * tt
                  );
        }

        grps += grpSummary[ tt ].cnt; 

        if (tt == 0) {
            freeBlks += grpSummary[ tt ].freeFrags;
        } else {
            waste += (127 - (fragsPerGroup) * (tt)) * grpSummary[ tt ].cnt;
            freeBlks += grpSummary[ tt ].freeFrags * (tt);
        }
    }

    printf( catgets(catd, 1, 9, "\nPercent Free Frags per Group Histogram\n") );
    printf( catgets(catd, 1, 10, "Type    Grps   0%%   9%%  19%%  29%%  39%%  49%%  59%%  69%%  79%%  89%%  99%%  100%%\n") );
    printf( catgets(catd, 1, 11, "----  ------  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---\n") );

    for (tt = 0; tt < 8; tt++) {

        if ((fType == -1) || (fType == tt)) {

            printf( catgets(catd, 1, 12, "  %dk %7d"), tt, grpSummary[ tt ].cnt );

            for (h = 0; h < 12; h++) {
                printf( " %4d", grpSummary[ tt ].histogram[ h ] ); 
            }

            printf( "\n" );
        }
    }

    printf( catgets(catd, 1, 13, "\nfragbf occupies %dk bytes\n"), stats.st_blocks / 2 );
    printf( catgets(catd, 1, 14, "                %dk in-use, %dk free, %dk overhead, %dk wasted\n"), 
            (stats.st_blocks / 2) - freeBlks - grps - waste, 
            freeBlks,
            grps,
            waste );

    advfs_xtntmap_close( xtntmap );
    close( fd );
}

int
check_group(
    int fd,
    int grpPg,
    slotsPgT *grpPgp,
    int fType,
    int v,
    int *grpFreeFrags,
    int *grpFragType
    )
{
    int sz, f, frag;
    grpHdrT *grpHdrp;
    slotsPgT fragPgBuf;
    bfFragT fragType;
    fragHdrT *fragHdrp;
    uint32T fragPg, fragSlot;
    uint32T nextFreeFrag;
    int freeFrags = 0;

    *grpFreeFrags = 0;

    grpHdrp = (grpHdrT *)grpPgp;

    if (grpHdrp->self != grpPg) {
        printf( catgets(catd, 1, 15, "invalid frag group, pg %d"), grpPg ); 
        return 0;
    }

    fragType = grpHdrp->fragType;

    *grpFragType = fragType;

    if ((fType != -1) && (fType != fragType)) {
        return 0;
    }

    if (v) {
        printf( catgets(catd, 1, 16, "\n--\ngroup pg = %6d, next pg = %6d, type = %d\n"), 
                grpPg, (signed) grpHdrp->nextFreeGrp, fragType );

        if (grpHdrp->version == 0) {
            printf( catgets(catd, 1, 17, "      nextFree = %5d, numFree = %5d, lastFree = %5d\n\n"),
                grpHdrp->nextFreeFrag, 
                grpHdrp->freeFrags,
                grpHdrp->lastFreeFrag );
        } else {
            printf( catgets(catd, 1, 18, "      nextFree = %5d, numFree = %5d\n\n"),
                grpHdrp->freeList[ BF_FRAG_NEXT_FREE ],
                grpHdrp->freeFrags);
        }
    }

    if (fragType == 0) {
        *grpFreeFrags = FRAGS_PER_GRP( fragType );
        return 1;
    }

    freeFrags = 0;
    *grpFreeFrags = grpHdrp->freeFrags;

    if (grpHdrp->version == 0) {
        nextFreeFrag = grpHdrp->nextFreeFrag;

        while (nextFreeFrag != BF_FRAG_EOF) {
    
            freeFrags++;
            if (v) {
                printf( catgets(catd, 1, 19, "frag on free list : %5d"), nextFreeFrag );
            }
    
            fragPg   = FRAG2PG( nextFreeFrag );
            fragSlot = FRAG2SLOT( nextFreeFrag );
        
            if (read_pg( fd, fragPg, &fragPgBuf ) <= 0) {
                printf( catgets(catd, 1, 20, "read error: fragPg %d, errno %d\n"), fragPg, errno );
                break;
            }
    
            fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];
            nextFreeFrag = fragHdrp->nextFreeFrag;
    
            if (v) {
                printf( catgets(catd, 1, 21, ", nextFree = %5d\n"), nextFreeFrag );
            }
        }
    } else {

        fragSlot = grpHdrp->freeList[ BF_FRAG_NEXT_FREE ];

        while (fragSlot != BF_FRAG_NULL) {

            freeFrags++;

            if (v) {
                nextFreeFrag = grpPg * BF_FRAG_PG_SLOTS + fragSlot;
                printf( catgets(catd, 1, 19, "frag on free list : %5d"), nextFreeFrag );
            }

            fragSlot = grpHdrp->freeList[ fragSlot ];

            if (v) {
                if (fragSlot == BF_FRAG_NULL) {
                    printf( catgets(catd, 1, 21, ", nextFree = %5d\n"), -1 );
                } else {
                    nextFreeFrag = grpPg * BF_FRAG_PG_SLOTS + fragSlot;
                    printf( catgets(catd, 1, 21, ", nextFree = %5d\n"), nextFreeFrag );
                }
            }
        }
    }

    if (freeFrags == grpHdrp->freeFrags) {
        return 1;
    }

    if (!v) {
        printf( catgets(catd, 1, 22, "\n--\ngroup pg = %6d, type = %d\n"), grpPg, fragType );
        printf( catgets(catd, 1, 23, "      nextFree = %5d, lastFree = %5d, numFree = %5d\n\n"),
                grpHdrp->nextFreeFrag, grpHdrp->lastFreeFrag, 
                grpHdrp->freeFrags);
    }

    printf( catgets(catd, 1, 24, "\n** inconsistency found **\n") );
    printf( catgets(catd, 1, 25, "** group header indicates %d free frags"), grpHdrp->freeFrags );
    printf( catgets(catd, 1, 26, ", free list has %d free frag(s)\n"), freeFrags );

    if (grpHdrp->version == 1) {
        if (grpHdrp->lastFreeFrag == BF_FRAG_BAD_GROUP) {
            printf( catgets(catd, 1, 35, "** group has been marked as BAD by the kernel: BF_FRAG_BAD_GROUP\n"));
        }
        return 2;
    }

    printf( catgets(catd, 1, 27, "** searching for additional free frags...\n") );

    freeFrags = 0;

    for (f = 0; f < FRAGS_PER_GRP( fragType ); f++) {
        frag     = grpPg * 8 + f * fragType + 1;
        fragPg   = FRAG2PG( frag );
        fragSlot = FRAG2SLOT(  frag );

        if (read_pg( fd, fragPg, &fragPgBuf ) <= 0) {
            printf( catgets(catd, 1, 20, "read error: fragPg %d, errno %d\n"), fragPg, errno );
            exit( 1 );
        }

        fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];

        if (v) {
            printf( catgets(catd, 1, 28, "frag = %5d, fragPg = %d, fragSlot = %d\n"), 
                    frag, fragPg, fragSlot );
            printf( catgets(catd, 1, 29, "\tself = %d, type = %d, nextFree = %d\n"),
                    fragHdrp->self, 
                    fragHdrp->fragType,
                    fragHdrp->nextFreeFrag );
            printf( catgets(catd, 1, 30, "\tself = 0x%08x, type = 0x%08x, nextFree = 0x%08x\n"),
                    fragHdrp->self, 
                    fragHdrp->fragType,
                    fragHdrp->nextFreeFrag );
        }

        if ((fragHdrp->self == frag) && (fragHdrp->fragType == fragType)) {
            freeFrags++;
            printf( catgets(catd, 1, 31, "\t** possible free frag: %5d, nextFree = %5d\n"),
                frag, fragHdrp->nextFreeFrag );
            if (!v) {
                printf( catgets(catd, 1, 32, "\t\tfrag = %5d, fragPg = %d, fragSlot = %d\n"), 
                        frag, fragPg, fragSlot );
            }
        }
    }

    printf( catgets(catd, 1, 33, "** search found %d possible free frag(s)\n"), freeFrags );

    return 2;
}
