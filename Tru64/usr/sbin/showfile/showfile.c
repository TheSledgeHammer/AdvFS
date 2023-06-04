/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992, 1993                            *
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
 *      ADVFS
 *
 * Date:
 *
 *      Wed Dec  9 11:07:33 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: showfile.c,v $ $Revision: 1.1.59.1 $ (DEC) $Date: 2003/06/11 20:40:10 $";
#endif

#include <msfs/msfs_syscalls.h>
#include <msfs/bs_error.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <pwd.h>
#include <grp.h>
#include <ctype.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/fs_types.h>
#include <sys/mount.h>
#include <locale.h>
#include "showfile_msg.h"

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define TRUE 1
#define FALSE 0
#define ERRMSG( e ) sys_errlist[e]

/* from bs_ods.h */
/* An extent with vdBlk set to XTNT_TERM terminates the previous descriptor. */
#define XTNT_TERM ((uint32T)-1)
/* An extent with vdBlk set to PERM_HOLE_START starts a permanent hole */
/* desriptor in a clone file. */
#define PERM_HOLE_START ((uint32T)-2)

/* from bs_stripe.h */
/*
 * This macro converts an extent map relative page to a bitfile relative
 * page.
 */
#define XMPAGE_TO_BFPAGE(mapIndex, pageOffset, segmentCnt, segmentSize) \
  ( ((mapIndex) * (segmentSize)) + \
   ( ((pageOffset) / (segmentSize)) * ((segmentCnt) * (segmentSize)) ) + \
   ((pageOffset) % (segmentSize)) )

char *Prog;

typedef struct volInfo {
    int volCnt;
    mlVolIoQParamsT params[1024];
} volInfoT;

static
int
show_file (
               char *path, /* in */
               int xflg,
               int hflg,
               int iflg
               );

static
mlStatusT
display_xtnt_map (
                  int fd,  /* in */
                  u32T bfPageSz,  /* in */
                  int bfXtntMapCnt,  /* in */
                  int bfXtntCnt,  /* in */
                  int hflg  /* in */
                  );

static
mlStatusT
display_stripe_xtnt_map (
                         int fd,  /* in */
                         u32T bfPageSz,  /* in */
                         u32T segmentCnt,  /* in */
                         u32T segmentSize,  /* in */
                         int bfXtntMapCnt,  /* in */
                         int bfXtntCnt,  /* in */
                         int hflg,  /* in */
                         int *outXtntCnt,  /* out */
                         int *outMcellCnt  /* out */
                         );

void usage( void )
{
    fprintf( stderr, catgets(catd, 1, USAGE1, "usage: %s [-i] [-x | -h] file ...\n"), Prog );
}

/*
 * The switches to show file are as follows:
 *
 *       <none> Display statistics about the named file or directory
 *       <-x>   Display extent map information for the named file or directory.
 *       <-h>   Display extent map including holes.
 *       <-i>   Display statistics about an index file associated with 
 *              the named directory
 *       <-ix>  Display extent map information for the index of the named
 *              directory
 *
 * If a directory has an index file and the <-i> switch is not present
 * The directory name will be followed by (index)
 *
 * If the <-i> switch is used and then the word index will be printed
 * followed by (<directory name>).
 */

main( int argc, char *argv[] )
{
    char *fileName = NULL;
    int c = 0, iflg = 0, ii = 0, xflg = 0, hflg = 0, fd, fileOpen = FALSE;
    char *p;
    mlStatusT sts;
    extern int optind;
    extern char *optarg;
    int errorFlag;
    uid_t ruid, euid;

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_SHOWFILE, NL_CAT_LOCALE);

    ruid = getuid();
    euid = geteuid();
    if (ruid != euid) {
        if (seteuid(ruid) == -1) {
            fprintf(stderr, catgets(catd, 1, 51, "%s: seteuid error\n"), Prog);
            exit(1);
        }
    }

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "ixh" )) != EOF) {
        switch (c) {
            case 'x':
                xflg++;
		break;

            case 'i':
                iflg++;
                break;

            case 'h':
                hflg++;
                break;

            case '?':

            default:
                usage();
                exit( 1 );
        }
    }

    if (xflg > 1 || hflg > 1 || iflg > 1 || (xflg && hflg)) {
        usage();
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    errorFlag = FALSE;
    for (ii = optind; ii < argc; ii++) {
        fileName = argv[ii];

        if (show_file( fileName, xflg, hflg, iflg ))
            errorFlag = TRUE;
    }

    if (errorFlag)
        exit( 1 );

    return 0;

}

#define DONE {finished = 1;}
#define XTNTARRAYSIZE 500

int 
get_vols( 
    mlDmnParamsT dmnParams,
    volInfoT *volInfo
    );

static
int
show_file (
    char *path, /* in */
    int xflg, /* in */
    int hflg, /* in */
    int iflg  /* in */
    )
{
    int fd = -1;
    char *p;
    char *IDX;
    char idx_tag_path[PATH_MAX];
    char out_txt[PATH_MAX];
    mlStatusT sts;
    char *type;
    int xtntCnt;                            /* number of extents to print */
    int xtntMapCnt;
    static int banner = 0;                  /* Has a banner been displayed? */
    volInfoT volInfo;
    mlDmnParamsT dmnParams;
    mlBfDomainIdT dmnId;
    struct statfs fsStats;
    mlBfAttributesT bfAttrs = { 0 };
    mlBfInfoT bfInfo;
    struct stat fStats;
    mlExtentDescT xtntsArray[XTNTARRAYSIZE];     /* Array of in memory
                                                       extent descriptors */
    u32T volIndex;
    int errorFlag = FALSE;
    int sXtntCnt = 0;
    int sMcellCnt = 0;
    uid_t ruid;

    IDX = (char *) malloc(strlen(catgets(catd, 1, 41, "index")));

    if (IDX == NULL) 
    {
        fprintf( stderr, catgets(catd, 1, 30, "%s: out of memory\n"), Prog );
        errorFlag=TRUE;
        goto _finish;
    }
    
    strcpy(IDX,catgets(catd, 1, 41, "index"));

    if ((p = strrchr( path, '/' )) != NULL) {
        p++;
    } else {
        p = path;
    }

    strcpy(out_txt,p);

    sts = lstat( path, &fStats );
    if (sts < 0) {
        fprintf( stderr, catgets(catd, 1, 4, "\n%s: lstat failed for file '%s'; %s\n"),
                 Prog, path, BSERRMSG( errno ) ); 
        return 1;
    }

    if (S_ISLNK( fStats.st_mode)) {
        type = catgets(catd, 1, 5, "symlink"); 
        if (!banner) {                        /* Has a banner been printed? */
            printf( catgets(catd, 1, 3, "\n         Id  Vol  PgSz  Pages  XtntType  Segs  SegSz  I/O   Perf  File\n") );
            banner = 1;              /* A banner has been printed. */
        }
        printf( "         **   **    **     **  %8s    **     **   **     **  %s\n", 
                type, out_txt);
        return 0;

    } else {
        sts = statfs( path, &fsStats, sizeof( fsStats) );
        if (sts < 0) {
	    char path_dot[4] = "..";
            if (path[0] == 'M') {
                sts = statfs( path_dot, &fsStats, sizeof( fsStats) );
            } else {
                type = "??";
	    }
        } else {
            type = mnt_names[fsStats.f_type];
        }
    }

    fd = open( path, O_RDONLY, 0 );
    
    if (fd >= 0) {
        if (iflg)
        {
            if (S_ISDIR( fStats.st_mode ))
            {
                sts = advfs_get_idx_bf_params(fd, &bfAttrs, &bfInfo );
                if (sts == EINVALID_HANDLE)
                {
                    printf( catgets(catd, 1, 40, "%s: does not have an index file\n"),
                            path );
                    errorFlag=FALSE;
                    goto _finish;
                }
                if (sts != EOK)
                {
                    errorFlag=TRUE;
                    goto _finish;
                }
                sprintf(idx_tag_path,"%s/.tags/0x%08x",fsStats.f_mntonname,bfInfo.tag.num);
                if (close( fd )) {
                    fprintf( stderr, catgets(catd, 1, 29, "%s: close of %s failed. [%d] %s\n"),
                             Prog, path, errno, sys_errlist[errno] );
                    errorFlag = TRUE;
                    goto _finish;
                }

                ruid = getuid();

                if (geteuid()) {
                    if (seteuid((uid_t)0) == -1) {
                        fprintf(stderr, catgets(catd, 1, 51, "%s: seteuid error\n"), Prog);
                        exit(1);
                    }
                }

                fd = open( idx_tag_path, O_RDONLY, 0 );

                if (getuid()) {
                    if (seteuid(ruid) == -1) {
                        fprintf(stderr, catgets(catd, 1, 51, "%s: seteuid error\n"), Prog);
                        exit(1);
                    }
                }

                if (fd < 0) {
                    errorFlag=TRUE;
                    goto _finish;
                }
                sprintf(out_txt,"%s (%s)",IDX,p);
                free(IDX);
            }
            else
            {
                fprintf( stderr, catgets(catd, 1, 39, "%s: is not a directory\n"),
                         path );
                errorFlag=TRUE;
                goto _finish;
            }
        }
        else
        {

            if (S_ISDIR( fStats.st_mode ))
            {
                sts = advfs_get_idx_bf_params(fd, &bfAttrs, &bfInfo );
                if (sts == EOK)
                {
                    sprintf(out_txt,"%s (%s)",p,IDX);
                    free(IDX);
                }
                else if (sts != EINVALID_HANDLE)
                {
                    errorFlag=TRUE;
                    goto _finish;
                }
            }

            sts = advfs_get_bf_params (fd, &bfAttrs, &bfInfo );  
        }
    }

    if (!banner) {      /* Has a banner been printed? Only want one. */
        printf( catgets(catd, 1, 3, "\n         Id  Vol  PgSz  Pages  XtntType  Segs  SegSz  I/O   Perf  File\n") );
        banner = 1;     /* A banner has been printed. */
    }

    if ((fd < 0) || (sts != EOK)) {
        printf( "         **   **    **     **  %8s    **     **   **     **  %s\n", 
                type, out_txt );
        
        goto _finish;
    } 

    switch (bfAttrs.mapType)  {

      case XMT_SIMPLE:

        type = catgets(catd, 1, 6, "simple");
        break;

      case XMT_STRIPE:

        type = catgets(catd, 1, 8, "stripe");
        break;

      default:

        type = "??";
        break;

    }  /* end switch */

    printf( "%6x.%04x ", bfInfo.tag.num, bfInfo.tag.seq );
    printf( "%4d ", bfInfo.volIndex );
    printf( "%5d ", bfInfo.pageSize );
    printf( "%6d  ", bfInfo.numPages );
    printf( "%8s  ", type );

    if (bfAttrs.mapType == XMT_STRIPE) {
        printf( "%4d  %5d  ", bfAttrs.attr.stripe.segmentCnt,
               bfAttrs.attr.stripe.segmentSize );
    } else {
        printf( "  **     **  " );
    }

    switch (bfAttrs.dataSafety) {

        case BF_SYNC_WRITE:
            printf( catgets(catd, 1, 9, "sync  ") );
            break;

        case BF_SAFETY_AGENT:
            printf( catgets(catd, 1, 10, "ftx   ") );
            break;

        default:
            printf( catgets(catd, 1, 11, "async ") );
            break;
    }

    dmnId.tv_sec = fsStats.mount_info.msfs_args.id.id1;
    dmnId.tv_usec = fsStats.mount_info.msfs_args.id.id2;

    sts = msfs_get_dmn_params( dmnId, &dmnParams );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 16, "%s: msfs_get_dmn_params failed; %s\n"), 
                 Prog, BSERRMSG( sts ) );
        exit( 1 );
    }

    if (get_vols( dmnParams, &volInfo ))
        errorFlag = TRUE;

    xtntMapCnt = 1;
    sts = msfs_get_bf_xtnt_map (fd, xtntMapCnt, 0, 0, xtntsArray, &xtntCnt,
                                    &volIndex);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 17, "%s: msfs_get_bf_xtnt_map failed. %s\n"),
                    Prog, BSERRMSG( sts ) ); 
        errorFlag = TRUE;
        goto _finish;
    }
    if(xtntCnt==1) xtntCnt=2;  /* if only a frag or no stg, force 100% */
    printf( "%3.0f%%  ",((double)(1/((double)xtntCnt-1))*100) );
    printf( "%s\n", out_txt);

    if (!xflg && !hflg) {
        goto _finish;
    }

    printf( "\n" );

    switch (bfAttrs.mapType)  {

      case XMT_SIMPLE:

        /*
         * Get the first extent map's total number of extents.
         */

        xtntMapCnt = 1;
        sts = msfs_get_bf_xtnt_map (fd, xtntMapCnt, 0, 0, xtntsArray, &xtntCnt,
                                    &volIndex);
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 17, "%s: msfs_get_bf_xtnt_map failed. %s\n"),
                    Prog, BSERRMSG( sts ) ); 
            errorFlag = TRUE;
            goto _finish;
        }

        sts = display_xtnt_map (fd, bfInfo.pageSize, xtntMapCnt, xtntCnt, hflg);
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 26, "%s: display_xtnt_map failed.\n"), Prog );
            errorFlag = TRUE;
            goto _finish;
        }

        break;

      case XMT_STRIPE:

        /*
         * Get the first extent map's total number of extents.
         */

        xtntMapCnt = 1;
        sts = msfs_get_bf_xtnt_map (fd, xtntMapCnt, 0, 0, xtntsArray, &xtntCnt,
                                    &volIndex);
        if (sts != EOK) {
            printf( catgets(catd, 1, 27, "*get bf extent map error %s*\n"), BSERRMSG( sts ) );  
            errorFlag = TRUE;
            goto _finish;
        }

        while (1) {

            sts = display_stripe_xtnt_map (
                                           fd,
                                           bfInfo.pageSize,
                                           bfAttrs.attr.stripe.segmentCnt,
                                           bfAttrs.attr.stripe.segmentSize,
                                           xtntMapCnt,
                                           xtntCnt,
                                           hflg,
                                           &sXtntCnt,
                                           &sMcellCnt);
            if (sts != EOK) {
                printf( catgets(catd, 1, 28, "*display extent map error %s*\n"), BSERRMSG( sts ) );  
                errorFlag = TRUE;
                goto _finish;
            }

            xtntMapCnt++;
            sts = msfs_get_bf_xtnt_map (fd, xtntMapCnt, 0, 0, xtntsArray, &xtntCnt,
                                        &volIndex);
            if (sts != EOK) {
                if (sts == EBAD_PARAMS) {
                    /* no more extent maps */
                    if ( hflg ) {
                        printf("\n");
                        printf(catgets(catd, 1, 42,
                          "        Total storage extents: %d\n"), sXtntCnt);
                        printf(catgets(catd, 1, 43,
                          "        Total extent mcells: %d\n"), sMcellCnt);
                    }
                    goto _finish;
                } else {
                    printf( catgets(catd, 1, 27, "*get bf extent map error %s*\n"), BSERRMSG( sts ) );  
                    errorFlag = TRUE;
                    goto _finish;
                }
            }
            printf( "\n" );
        }  /* end while */

        break;

      default:

        break;

    }  /* end switch */

_finish:

    if (fd >= 0) {
        if (close( fd )) {
            fprintf( stderr, catgets(catd, 1, 29, "%s: close of %s failed. [%d] %s\n"),
                    Prog, path, errno, sys_errlist[errno] );
            errorFlag = TRUE;
        }
    }

    if (errorFlag)
        return 1;

    return 0;

} /* end show_file */

int
get_vols( 
    mlDmnParamsT dmnParams,
    volInfoT *volInfo
    )
{
    mlStatusT sts;
    char *tok;
    int vol, vols, volIndex;
    mlVolInfoT volInfop;
    mlVolPropertiesT volPropertiesp;
    mlVolCountersT volCountersp;
    u32T *volIndexArray;
    long totalSpace = 0L, totalFree = 0L;
    int errorFlag;

    volIndexArray = (u32T*) malloc( dmnParams.curNumVols * sizeof(u32T) );
    if (volIndexArray == NULL) {
        fprintf( stderr, catgets(catd, 1, 30, "%s: out of memory\n"), Prog );
        return 1;
    }

    sts = msfs_get_dmn_vol_list( dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 volIndexArray, 
                                 &vols );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, 1, 31, "%s: unable to display volume info; domain not active\n"), Prog );
        return 1;

    } else if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 32, "%s: get dmn vol list error %s\n"), 
                 Prog, BSERRMSG( sts ) );
        free( volIndexArray );
        return 1;
    }

    errorFlag = FALSE;

    for (vol = 0; vol < vols; vol++) {


        sts = advfs_get_vol_params( dmnParams.bfDomainId, 
                                    volIndexArray[vol], 
                                    &volInfop, 
                                    &volPropertiesp,
                                    &volInfo->params[ volIndexArray[ vol ] ],
                                    &volCountersp );
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 33, "%s: get vol params error %s\n"), 
                     Prog, BSERRMSG( sts ) );
            errorFlag = TRUE;
        }
    }

    if (errorFlag)
        return 1;

    return 0;
}

static
mlStatusT
display_xtnt_map (
                  int fd,  /* in */
                  u32T bfPageSz,  /* in */
                  int bfXtntMapCnt,  /* in */
                  int bfXtntCnt,  /* in */
                  int hflg  /* in */
                  )

{

    mlStatusT sts;
    int x, xtntCnt;                            /* number of extents to print */
    int i;
    int lastPg = -1;
    int mcellCnt = 1;
    int startXtnt = 0;
    int actualXtntCnt = 0;
    mlExtentDescT xtntsArray[XTNTARRAYSIZE];     /* Array of in memory
                                                       extent descriptors */
    u32T volIndex;

    printf (catgets(catd, 1, 34, "    extentMap: %d\n"), bfXtntMapCnt);
    printf (catgets(catd, 1, 35, "        pageOff    pageCnt     vol    volBlock    blockCnt\n"));

    for (i = 0; i < bfXtntCnt; i = i + xtntCnt) {
        sts = msfs_get_bf_xtnt_map (fd, bfXtntMapCnt, startXtnt, XTNTARRAYSIZE,
                                    xtntsArray, &xtntCnt, &volIndex);

        if (sts == EOK) {
            for (x = 0; x < xtntCnt; x++) {
                if (hflg ||
                     xtntsArray[x].volBlk != XTNT_TERM &&
                     xtntsArray[x].volBlk != PERM_HOLE_START)
                {

                    if ( hflg && lastPg == xtntsArray[x].bfPage ) {
                        mcellCnt++;
                        printf ("\n");
                    }
                    lastPg = xtntsArray[x].bfPage;
                    printf ("     %10u", xtntsArray[x].bfPage );  
                    printf (" %10u", xtntsArray[x].bfPageCnt);
                    printf ("   %5d", xtntsArray[x].volIndex);
                    if (xtntsArray[x].volBlk != XTNT_TERM &&
                        xtntsArray[x].volBlk != PERM_HOLE_START)
                    {
                        printf ("  %10u", xtntsArray[x].volBlk);
                        printf ("  %10u", xtntsArray[x].bfPageCnt * bfPageSz);
                        actualXtntCnt++;
                    } else if ( xtntsArray[x].volBlk == PERM_HOLE_START ) {
                        printf ("    PERMHOLE");
                    } else if ( xtntsArray[x].volBlk == XTNT_TERM &&
                                xtntsArray[x].bfPageCnt != 0 )
                    {
                        printf ("        HOLE");
                    } else {
                        printf ("        TERM");
                    }
                    printf ("\n");
                }

            }  /* end for */
            startXtnt = startXtnt + xtntCnt;
        } else {
            fprintf( stderr, catgets(catd, 1, 36, "%s: msfs_get_bf_xtnt_map failed. %s\n"),
                    Prog, BSERRMSG( sts ) ); 
            return sts;  
        } /* end if */
    } /* end while */

    if ( hflg ) {
        printf (catgets(catd, 1, 44, "        Number of storage extents: %d\n"),
          actualXtntCnt);
        printf (catgets(catd, 1, 45, "        Number of extent mcells: %d\n"),
          mcellCnt);
    } else {
        printf (catgets(catd, 1, 37, "        extentCnt: %d\n"), actualXtntCnt);
    }

    return EOK;

}  /* end display_xtnt_map */

static
mlStatusT
display_stripe_xtnt_map (
                         int fd,  /* in */
                         u32T bfPageSz,  /* in */
                         u32T segmentCnt,  /* in */
                         u32T segmentSize,  /* in */
                         int bfXtntMapCnt,  /* in */
                         int bfXtntCnt,  /* in */
                         int hflg,  /* in */
                         int *outXtntCnt,  /* out */
                         int *outMcellCnt  /* out */
                         )

{

    mlStatusT sts;
    int x, xtntCnt;                            /* number of extents to print */
    int i;
    int lastPg = -1;
    int mcellCnt = 1;
    int startXtnt = 0;
    int actualXtntCnt = 0;
    u32T numPgs, numBlks;
    u32T pageOffset;
    mlExtentDescT xtntsArray[XTNTARRAYSIZE];     /* Array of in memory
                                                       extent descriptors */
    u32T volIndex;

    if ( hflg ) {
        printf (catgets(catd, 1, 46, "    stripe: %d\n"), bfXtntMapCnt);
    } else {
        printf (catgets(catd, 1, 34, "    extentMap: %d\n"), bfXtntMapCnt);
    }
    printf (catgets(catd, 1, 38, "        pageOff    pageCnt    volIndex    volBlock    blockCnt\n"));

    for (i = 0; i < bfXtntCnt; i = i + xtntCnt) {
        sts = msfs_get_bf_xtnt_map (fd, bfXtntMapCnt, startXtnt, XTNTARRAYSIZE,
                                    xtntsArray, &xtntCnt, &volIndex);

        if (sts == EOK) {
            for (x = 0; x < xtntCnt; x++) {
                if (hflg ||
                     xtntsArray[x].volBlk != XTNT_TERM &&
                     xtntsArray[x].volBlk != PERM_HOLE_START)
                {
                    if ( hflg ) {
                        pageOffset = xtntsArray[x].bfPage;
                    } else {
                        pageOffset = XMPAGE_TO_BFPAGE (
                                                      bfXtntMapCnt - 1,
                                                      xtntsArray[x].bfPage,
                                                      segmentCnt,
                                                      segmentSize);

                        numPgs = segmentSize - (pageOffset % segmentSize);
                        if (numPgs > xtntsArray[x].bfPageCnt) {
                            numPgs = xtntsArray[x].bfPageCnt;
                        }
                    }

                    if ( hflg && lastPg == xtntsArray[x].bfPage ) {
                        mcellCnt++;
                        printf ("\n");
                    }
                    lastPg = xtntsArray[x].bfPage;
                    printf ("      %9u", pageOffset );  
                    if ( hflg ) {
                        printf ("  %9u", xtntsArray[x].bfPageCnt);
                    } else {
                        printf ("  %9u", numPgs);
                    }
                    printf ("        %4d", xtntsArray[x].volIndex);
                    if (xtntsArray[x].volBlk != XTNT_TERM &&
                        xtntsArray[x].volBlk != PERM_HOLE_START)
                    {
                        printf ("   %9u", xtntsArray[x].volBlk);
                        printf ("   %9u", xtntsArray[x].bfPageCnt * bfPageSz);
                        actualXtntCnt++;
                    } else if ( xtntsArray[x].volBlk == PERM_HOLE_START ) {
                        printf (catgets(catd, 1, 47, "    PERMHOLE"));
                    } else if ( xtntsArray[x].volBlk == XTNT_TERM &&
                                xtntsArray[x].bfPageCnt != 0 )
                    {
                        printf (catgets(catd, 1, 48, "        HOLE"));
                    } else {
                        printf (catgets(catd, 1, 49, "        TERM"));
                    }
                    printf ("\n");

                    if ( !hflg ) {
                        pageOffset += numPgs + ((segmentCnt - 1) * segmentSize);
                        numPgs = xtntsArray[x].bfPageCnt - numPgs;
                        while (numPgs > 0) {
                            printf ("      %9u", pageOffset );
                            if (numPgs > segmentSize) {
                                printf ("  %9u", segmentSize);
                                numPgs = numPgs - segmentSize;
                            } else {
                                printf ("  %9u", numPgs);
                                numPgs = 0;
                            }
                            printf ("\n");
                            pageOffset += segmentCnt * segmentSize;
                        }
                    }
                }

            }  /* end for */
            startXtnt = startXtnt + xtntCnt;
        } else {
            fprintf( stderr, catgets(catd, 1, 36, "%s: msfs_get_bf_xtnt_map failed. %s\n"),
                    Prog, BSERRMSG( sts ) ); 
            return sts;  
        } /* end if */
    } /* end while */

    if ( hflg ) {
        printf (catgets(catd, 1, 44, "        Number of storage extents: %d\n"),
          actualXtntCnt);
        printf (catgets(catd, 1, 45, "        Number of extent mcells: %d\n"),
          mcellCnt);
    } else {
        printf (catgets(catd, 1, 37, "        extentCnt: %d\n"), actualXtntCnt);
    }

    *outXtntCnt += actualXtntCnt;
    *outMcellCnt += mcellCnt;

    return EOK;

}  /* end display_stripe_xtnt_map */

/* end showfile.c */
