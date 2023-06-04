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
static char rcsid[] = "@(#)$RCSfile: advfsstat.c,v $ $Revision: 1.1.47.1 $ (DEC) $Date: 2002/12/18 14:49:46 $";
#endif

#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#ifndef _OSF_SOURCE
extern char * index( char *, char );
#endif
#include <ctype.h>
#include <signal.h>
#include <unistd.h>
#include <pwd.h>
#include <sys/file.h>
#include <sys/param.h> 
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#ifndef _OSF_SOURCE
#define CLK_TCK 60 
extern clock_t times( struct tms * );
#endif
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/namei.h>
#include <sys/table.h>
#define ADVFS_LK_STRINGS
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <assert.h>
#include <locale.h>
#include "advfsstat_msg.h"

nl_catd catd;

char *Prog;

void usage( void )
{
printf( catgets(catd, 1, USAGE1, "\nusage: %s [options] [stats-type] domain\n"), Prog );
printf( catgets(catd, 1, USAGE2, "\nusage: %s [options] -f 0 | 1 | 2 domain fileset\n"), Prog );
printf( catgets(catd, 1, 3, "\noptions:\n") );
printf( catgets(catd, 1, 4, "\t-i sec  \trepeat display every 'sec' seconds\n") );
printf( catgets(catd, 1, 5, "\t-c count\trepeat display 'count' times\n") );
printf( catgets(catd, 1, 90,"\t-s      \tdisplay raw statistics\n") );
printf( catgets(catd, 1, 6, "\t-R      \tdisplay ratios (use with -b, -p, -r)\n") );
printf( catgets(catd, 1, 7, "\nstats-type:\n") );
printf( catgets(catd, 1, 8, "\t-b      \tbuffer cache statistics\n") );
printf( catgets(catd, 1, 9, "\t-f 0    \tall fileset vnop statistics\n") );
printf( catgets(catd, 1, 10, "\t-f 1    \tall fileset lookup statistics\n") );
printf( catgets(catd, 1, 11, "\t-f 2    \tcommon fileset vnop statistics\n") );
printf( catgets(catd, 1, 12, "\t-l 0    \tbasic lock statistics\n") );
printf( catgets(catd, 1, 13, "\t-l 1    \tlock statistics\n") );
printf( catgets(catd, 1, 14, "\t-l 2    \tdetailed lock statistics\n") );
printf( catgets(catd, 1, 15, "\t-n      \tnamei cache statistics\n") );
printf( catgets(catd, 1, 16, "\t-p      \tbuffer cache pin statistics\n") );
printf( catgets(catd, 1, 17, "\t-r      \tbuffer cache ref statistics\n") );
printf( catgets(catd, 1, 91, "\t-S      \tsmoothsync queue statistics\n") );
printf( catgets(catd, 1, 18, "\t-v 0    \tvolume read/write statistics\n") );
printf( catgets(catd, 1, 19, "\t-v 1    \tdetailed volume statistics\n") );
printf( catgets(catd, 1, 20, "\t-v 2    \tvolume I/O queue statistics (snapshot)\n") );
printf( catgets(catd, 1, 92, "\t-v 3    \tvolume I/O queue statistics (interval)\n") );
printf( catgets(catd, 1, 21, "\t-B r    \tBMT Record read statistics\n") );
printf( catgets(catd, 1, 22, "\t-B w    \tBMT Record write/update statistics\n") );
}

/* When computing K and M use 1024 and 1048576 respectively */
/*
 * Add an additional space to prevent fields from appearing contiguous.
 *
 * If num lies between 1000 and 1024, round up to display 1K rather
 * than 0K.  Same for M.
 */
void print_num3( unsigned num )
{
    if (num < 1000) {
        printf( "%4u ", num );
    } else if (num < 1000000) {
        printf( "%3uK ", num / 1024 ? num / 1024 : 1 );
    } else if (num < 1000000000) {
        printf( "%3uM ", num / 1048576 ? num / 1048576 : 1 );
    } else {
        printf( " *** " );
    }
}
void print_num4( unsigned num )
{
    if (num < 10000) {
        printf( "%5u", num );
    } else if (num < 1000000) {
        printf( "%4uK", num / 1024 ? num / 1024 : 1 );
    } else if (num < 1000000000) {
        printf( "%4uM", num / 1048576 ? num / 1048576 : 1 );
    } else {
        printf( " ****" );
    }
}

void print_num5( unsigned num )
{
    if (num < 10000) {
        printf( "%6u", num );
    } else if (num < 1000000) {
        printf( "%5uK", num / 1024 ? num / 1024 : 1 );
    } else if (num < 1000000000) {
        printf( "%5uM", num / 1048576 ? num / 1048576 : 1 );
    } else {
        printf( " *****" );
    }
}

/* We want to print out only 10 digits, so we may need to scale.
 * This routine will both scale and create the format string we
 * will need to print the string out.
 */
unsigned long
find_arg_format(unsigned long num, char *string)
{
    if ( num < 10000000000 ) {
        strcat(string,"%10lu ");
        return( num );
    } else if (num < 10995116277760 ) {    /* Use Gigabyte - 2^30. */
        strcat(string,"%9luG ");
        return( num / 1073741824L ? num / 1073741824L : 1 );
    } else {                               /* Use Terabyte - 2^40. */
        strcat(string,"%9luT ");
        return( num / 1099511627776 ? num / 1099511627776 : 1 );
    }
}



/*
 * print_generic_dmnstat() 
 *
 * print the currrent domain struct.
 */

void
print_generic_dmnstat( 
    mlDmnStatT *dmnStat,       /* in */
    mlBfDomainIdT dmnid         /* in */
    )
{
    int i;
    unsigned unpin;

    if( ( dmnid.tv_sec == 0 ) && ( dmnid.tv_usec == 0 ) ){
        return;
    }
    if( dmnStat == NULL ) {
        return;
    }

    /* 
     * Print stats
     */
    printf("\n");
    printf("Domain -%d.%d- Stats -\n", dmnid.tv_sec, dmnid.tv_usec);
    printf("\n");

    printf("%-6s%-10d%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "Deref",
        dmnStat->dmnParams->mlBcStat.derefCnt,
        " Refhit",
        dmnStat->dmnParams->mlBcStat.refHit,
        " Refhitwait",
        dmnStat->dmnParams->mlBcStat.refHitWait,
        " R_ahead",
        dmnStat->dmnParams->mlBcStat.raBuf );

    unpin = dmnStat->dmnParams->mlBcStat.unpinCnt.blocking +
        dmnStat->dmnParams->mlBcStat.unpinCnt.lazy +
        dmnStat->dmnParams->mlBcStat.unpinCnt.log +
        dmnStat->dmnParams->mlBcStat.unpinCnt.clean;

    printf("%-6s%-10d%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "Unpin",
        unpin,
        " Pinhit",
        dmnStat->dmnParams->mlBcStat.pinHit,
        " Pinhitwait",
        dmnStat->dmnParams->mlBcStat.pinHitWait,
        " Pinreads", 
        dmnStat->dmnParams->mlBcStat.pinRead );

    printf("%-6s%-10d%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "Lazy",
        dmnStat->dmnParams->mlBcStat.unpinCnt.lazy,
        " Log",
        dmnStat->dmnParams->mlBcStat.unpinCnt.log,
        " Blocking",
        dmnStat->dmnParams->mlBcStat.unpinCnt.blocking,
        " Clean",
        dmnStat->dmnParams->mlBcStat.unpinCnt.clean );

    printf("%16s%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "",
        " Ubchit",
        dmnStat->dmnParams->mlBcStat.ubcHit,
        " Unconsol",
        dmnStat->dmnParams->mlBcStat.unconsolidate,
        " ConsolAbort",
        dmnStat->dmnParams->mlBcStat.consolAbort );

    printf("%16s%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "",
        " UnpinMeta",
        dmnStat->dmnParams->mlBcStat.unpinFileType.meta,
        " UnpinFtx",
        dmnStat->dmnParams->mlBcStat.unpinFileType.ftx,
        " UnpinData",
        unpin - dmnStat->dmnParams->mlBcStat.unpinFileType.ftx -
                dmnStat->dmnParams->mlBcStat.unpinCnt.log );

    printf("%16s%-11s%-10d%-12s%-10d%-13s%-8d\n",
        "",
        " DerefMeta",
        dmnStat->dmnParams->mlBcStat.derefFileType.meta,
        " DerefFtx",
        dmnStat->dmnParams->mlBcStat.derefFileType.ftx,
        " DerefData",
        dmnStat->dmnParams->mlBcStat.derefCnt -
        dmnStat->dmnParams->mlBcStat.derefFileType.ftx );
    
    printf("\n%4s %10s %10s %10s %10s %10s %10s\n", 
           "Disk",
           "Reads",
           "Writes",
           "Rglobs",
           "AveRglob",
           "Wglobs",
           "AveWglob" );
    printf("%4s %10s %10s %10s %10s %10s %10s\n", 
           "----",
           "-----",
           "------",
           "------",
           "--------",
           "------",
           "--------" );

    for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
        unsigned rave, wave;
        unsigned long temp1, temp2, temp3, temp4, temp5, temp6;
        char string[80];

        rave = ( dmnStat->volCounters[i].rglob ? 
            dmnStat->volCounters[i].rglobBlk /  
            dmnStat->volCounters[i].rglob : 0 );
            
        wave = ( dmnStat->volCounters[i].wglob ? 
            dmnStat->volCounters[i].wglobBlk /  
            dmnStat->volCounters[i].wglob : 0 );
            
        /* If the values require more than 10 digits, use terabyte notation.
         * Build the string: "%4u %10u %10u %10u %10u %10u %10u\n"
         */
        strcpy(string,"%4u ");
        temp1 = find_arg_format(dmnStat->volCounters[i].nread, string);
        temp2 = find_arg_format(dmnStat->volCounters[i].nwrite, string);
        temp3 = find_arg_format(dmnStat->volCounters[i].rglob, string);
        temp4 = find_arg_format(rave, string);
        temp5 = find_arg_format(dmnStat->volCounters[i].wglob, string);
        temp6 = find_arg_format(wave, string);
        printf(string, dmnStat->volIndex[i], temp1, temp2, temp3, temp4,
                       temp5, temp6);
    }
    printf("\n");
}


void print_buf_cache_stats( mlDmnStatT *dmnStat, int R )
{
    static int cnt = 0;
    double pins, refs;

    pins = (double) (dmnStat->dmnParams->mlBcStat.unpinCnt.blocking +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.lazy +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.log +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.clean );

    refs = (double) dmnStat->dmnParams->mlBcStat.derefCnt;

    if (!R) {
        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, 23, " pin                 ref            unpin-type          misc      cons\n") );
            printf(catgets(catd, 1, 24, "  cnt  hit hitw read  cnt  hit hitw lazy  blk  cln  log   ra  ubc   un abrt\n") );
        }
    } else {
        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, 25, " pin                     ref               unpin-type\n") );
            printf(catgets(catd, 1, 26, "   cnt   hit  hitw  read   cnt   hit  hitw  lazy   blk   cln   log\n") );
        }
    }
    
    
    if (!R) {
        print_num4( (unsigned) pins );
        print_num4( dmnStat->dmnParams->mlBcStat.pinHit );
        print_num4( dmnStat->dmnParams->mlBcStat.pinHitWait );
        print_num4( dmnStat->dmnParams->mlBcStat.pinRead );
        print_num4( refs );
        print_num4( dmnStat->dmnParams->mlBcStat.refHit );
        print_num4( dmnStat->dmnParams->mlBcStat.refHitWait ); 
        print_num4( dmnStat->dmnParams->mlBcStat.unpinCnt.lazy );
        print_num4( dmnStat->dmnParams->mlBcStat.unpinCnt.blocking );
        print_num4( dmnStat->dmnParams->mlBcStat.unpinCnt.clean );
        print_num4( dmnStat->dmnParams->mlBcStat.unpinCnt.log );
        print_num4( dmnStat->dmnParams->mlBcStat.raBuf );
        print_num4( dmnStat->dmnParams->mlBcStat.ubcHit );
        print_num4( dmnStat->dmnParams->mlBcStat.unconsolidate );
        print_num4( dmnStat->dmnParams->mlBcStat.consolAbort );

    } else {
        print_num5( (unsigned) pins );

        if (pins > 0.0) {
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.pinHit / pins * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.pinHitWait / pins * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.pinRead / pins * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
        }

        print_num5( refs );

        if (refs > 0.0) {
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.refHit / refs * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.refHitWait / refs * 100.0 ); 
        } else {
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
        }

        if (pins > 0.0) {
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.unpinCnt.lazy / pins * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.unpinCnt.blocking / pins * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.unpinCnt.clean / pins * 100.0 );
            printf( "%6.1f", (double) dmnStat->dmnParams->mlBcStat.unpinCnt.log / pins * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
            printf( "%6.1f",  0.0 );
        }
    }

    printf( "\n" );
    fflush( stdout );

    cnt++;
}

/*
 * Numerical definitions for the typed records in the BMT
 * are defined in bs_ods.h.  By design, only the first 21
 * record types will be reported on.
 */
char* fStatType[] = {"",
                     "BSR_XTNTS=",              /* BSR_XTNTS              = 1  */
                     "BSR_ATTR=",               /* BSR_ATTR               = 2  */
                     "BSR_VD_ATTR=",            /* BSR_VD_ATTR            = 3  */
                     "BSR_DMN_ATTR=",           /* BSR_DMN_ATTR           = 4  */
                     "BSR_XTRA_XTNTS=",         /* BSR_XTRA_XTNTS         = 5  */
                     "BSR_SHADOW_XTNTS=",       /* BSR_SHADOW_XTNTS       = 6  */
                     "BSR_MCELL_FREE_LIST=",    /* BSR_MCELL_FREE_LIST    = 7  */
                     "BSR_BFS_ATTR=",           /* BSR_BFS_ATTR           = 8  */
                     "BSR_VD_IO_PARAMS=",       /* BSR_VD_IO_PARAMS       = 9  */
                     "BSR_FREE_2=",             /* BSR_FREE_2             = 10 */
                     "BSR_RSVD11=",             /* BSR_RSVD11             = 11 */
                     "BSR_RSVD12=",             /* BSR_RSVD12             = 12 */
                     "BSR_RSVD13=",             /* BSR_RSVD13             = 13 */
                     "BSR_DEF_DEL_MCELL_LIST=", /* BSR_DEF_DEL_MCELL_LIST = 14 */
                     "BSR_DMN_MATTR=",          /* BSR_DMN_MATTR          = 15 */
                     "BSR_BF_INHERIT_ATTR=",    /* BSR_BF_INHERIT_ATTR    = 16 */
                     "BSR_RSVD17=",             /* BSR_RSVD17             = 17 */
                     "BSR_BFS_QUOTA_ATTR=",     /* BSR_BFS_QUOTA_ATTR     = 18 */
                     "BSR_PROPLIST_HEAD=",      /* BSR_PROPLIST_HEAD      = 19 */
                     "BSR_PROPLIST_DATA=",      /* BSR_PROPLIST_DATA      = 20 */
                     "BSR_DMN_TRANS_ATTR="      /* BSR_DMN_TRANS_ATTR     = 21 */
};

void print_bmt_stats( mlDmnStatT *dmnStat, int R, int bmtRead )
{
    static int cnt = 0;
    static int displayed[ML_BSR_MAX+1] = { 0 };
    mlBmtStatT *bmtStat = &dmnStat->dmnParams->mlBmtStat;
    int i;

    if ((cnt % 20) == 0) {
        for (i = 0; i <= ML_BSR_MAX; i++ ) {
            displayed[i] = 0;
        }
        printf( "\n" );
    }

    printf( "BMTR_FS_STAT=" );
    assert(sizeof(fStatType)/sizeof(fStatType[0]) == ML_BSR_MAX+1);

    if (bmtRead) {
        print_num3( bmtStat->fStatRead );

        for (i = 0; i <= ML_BSR_MAX; i++ ) {
            if ((bmtStat->bmtRecRead[i] > 0) || displayed[i]) {
                printf( fStatType[i] );
                print_num3( bmtStat->bmtRecRead[i] );
                displayed[i] = 1;
            }
        }

    } else {
        print_num3( bmtStat->fStatWrite );

        for (i = 0; i <= ML_BSR_MAX; i++ ) {
            if ((bmtStat->bmtRecWrite[i] > 1) || displayed[i]) {
                printf( fStatType[i] );
                print_num3( bmtStat->bmtRecWrite[i] );
                displayed[i] = 1;
            }
        }
    }

    printf( "\n" );
    fflush( stdout );
    cnt++;
}

void print_buf_cache_pin_stats( mlDmnStatT *dmnStat, int R )
{
    static int cnt = 0;
    double pins, bsFtx, fsFtx, other;

    if ((cnt % 20) == 0) {
        printf(catgets(catd, 1, 27, " pin                    pin-type          data-type   \n") );
        printf(catgets(catd, 1, 28, "  cnt   hit  hitw  read  lazy   blk   cln   log bsFtx fsFtx other\n") );
    }

    pins = (double) (dmnStat->dmnParams->mlBcStat.unpinCnt.blocking +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.lazy +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.log +
                     dmnStat->dmnParams->mlBcStat.unpinCnt.clean );

    bsFtx = (double) (dmnStat->dmnParams->mlBcStat.unpinFileType.meta -
                      dmnStat->dmnParams->mlBcStat.unpinCnt.log );

    fsFtx = (double) dmnStat->dmnParams->mlBcStat.unpinFileType.ftx - bsFtx;
    other = pins - 
            (double) dmnStat->dmnParams->mlBcStat.unpinFileType.ftx -
            (double) dmnStat->dmnParams->mlBcStat.unpinCnt.log;

    print_num4( (unsigned) pins );

    if (!R) {
        print_num5( dmnStat->dmnParams->mlBcStat.pinHit );
        print_num5( dmnStat->dmnParams->mlBcStat.pinHitWait );
        print_num5( dmnStat->dmnParams->mlBcStat.pinRead );
        print_num5( dmnStat->dmnParams->mlBcStat.unpinCnt.lazy );
        print_num5( dmnStat->dmnParams->mlBcStat.unpinCnt.blocking );
        print_num5( dmnStat->dmnParams->mlBcStat.unpinCnt.clean );
        print_num5( dmnStat->dmnParams->mlBcStat.unpinCnt.log );
        print_num5( (unsigned) bsFtx );
        print_num5( (unsigned) fsFtx );
        print_num5( (unsigned) other );

    } else if (pins > 0.0) {
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.pinHit / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.pinHitWait / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.pinRead / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.unpinCnt.lazy / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.unpinCnt.blocking / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.unpinCnt.clean / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.unpinCnt.log / pins * 100.0 );
        printf( "%6.1f", bsFtx / pins * 100.0 );
        printf( "%6.1f", fsFtx / pins * 100.0 );
        printf( "%6.1f", other / pins * 100.0 );
    } else {
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
    }

    printf( "\n" );
    fflush( stdout );

    cnt++;
}

void print_buf_cache_ref_stats( mlDmnStatT *dmnStat, int R )
{
    static int cnt = 0;
    double refs, bsFtx, fsFtx, other;

    if ((cnt % 20) == 0) {
        printf(catgets(catd, 1, 29, " ref              data-type   \n") );
        printf(catgets(catd, 1, 30, "  cnt   hit  hitw bsFtx fsFtx other\n") );
    }

    refs  = (double) dmnStat->dmnParams->mlBcStat.derefCnt;
    bsFtx = (double) dmnStat->dmnParams->mlBcStat.derefFileType.meta;
    fsFtx = (double) dmnStat->dmnParams->mlBcStat.derefFileType.ftx - bsFtx;
    other = refs - bsFtx - fsFtx;

    print_num4( (unsigned) refs );

    if (!R) {
        print_num5( dmnStat->dmnParams->mlBcStat.refHit );
        print_num5( dmnStat->dmnParams->mlBcStat.refHitWait );
        print_num5( (unsigned) bsFtx );
        print_num5( (unsigned) fsFtx );
        print_num5( (unsigned) other );

    } else if (refs > 0.0) {
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.refHit / refs * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->mlBcStat.refHitWait / refs * 100.0 );
        printf( "%6.1f", bsFtx / refs * 100.0 );
        printf( "%6.1f", fsFtx / refs * 100.0 );
        printf( "%6.1f", other / refs * 100.0 );
    } else {
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
        printf( "%6.1f",  0.0 );
    }

    printf( "\n" );
    fflush( stdout );

    cnt++;
}

void print_vol_stats( mlDmnStatT *dmnStat, int vLevel, int sflag )
{
    int i;
    static int cnt = 0;
    static int numVols = 0;
    unsigned rave, wave;

    if (((cnt %20) == 0) || (numVols != dmnStat->dmnParams->curNumVols)) {

        cnt = 0;

        if (vLevel == 2 || vLevel == 3) {
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 31, "vol%1d                                        "), dmnStat->volIndex[i] );
            }
            printf( "\n" );
        
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 32, "  rd   wr   rg  arg   wg  awg  blk ubcr flsh  wlz  sms  rlz  con  dev ") );
            }

        } else if (vLevel == 1) {
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 33, "vol%1d                    "), dmnStat->volIndex[i] );
            }
            printf( "\n" );
        
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 34, "  rd  wr  rg arg  wg awg") );
            }

        } else {
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 35, "vol%1d    "), dmnStat->volIndex[i] );
            }
            printf( "\n" );
        
            for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
                printf( catgets(catd, 1, 36, "  rd  wr") );
            }
        }

        printf( "\n" );
    }

    for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
        if (vLevel >= 1) {
            rave = ( dmnStat->volCounters[i].rglob ? 
                dmnStat->volCounters[i].rglobBlk /  
                dmnStat->volCounters[i].rglob : 0 );
                
            wave = ( dmnStat->volCounters[i].wglob ? 
                dmnStat->volCounters[i].wglobBlk /  
                dmnStat->volCounters[i].wglob : 0 );
        }
            
        print_num3( dmnStat->volCounters[i].nread );
        print_num3( dmnStat->volCounters[i].nwrite );
        
        if (vLevel >= 1) {
            print_num3( dmnStat->volCounters[i].rglob );
            print_num3( rave );
            print_num3( dmnStat->volCounters[i].wglob );
            print_num3( wave );
        }

        if (vLevel == 2 || vLevel == 3) {
            print_num3( dmnStat->volCounters[i].blockingQ );
            print_num3( dmnStat->volCounters[i].ubcReqQ );
            print_num3( dmnStat->volCounters[i].flushQ );
            print_num3( dmnStat->volCounters[i].waitLazyQ );
            print_num3( get_vol_smsync_cnt(vLevel, dmnStat, 
                dmnStat->volIndex[i], sflag) );
            print_num3( dmnStat->volCounters[i].readyLazyQ );
            print_num3( dmnStat->volCounters[i].consolQ );
            print_num3( dmnStat->volCounters[i].devQ );
        }
    }

    printf("\n");
    fflush( stdout );
    cnt++;
    numVols = dmnStat->dmnParams->curNumVols;
}


/*
 * vLevel == 2 --> return the # bufs currently on the smoothsync queues.
 * vLevel == 3 --> return the # bufs entered onto the smoothsync queues
 *                 during the last interval (or system startup for no 
 *                 interval) -- similar to iostat(8)
 */
int get_vol_smsync_cnt(
    int vLevel, 
    mlDmnStatT *dmnstat,
    int vol,
    int sflag)
{
    libParamsT buf;
    int cnt = 0;
    static int prevcnt[BS_MAX_VDI] = {0};
    int i;

    /* 
     * get smsyncq stats for diskvol
     */
    buf.getVolSmsync.bfDomainId = dmnstat->dmnParams->bfDomainId;
    buf.getVolSmsync.volIndex = vol;


    if (msfs_syscall(ADVFS_GET_SMSYNC_STATS, &buf, sizeof(buf.getVolSmsync)))
        return(-1);


    /*
     * return # bufs currently on the set of smoothsync queues
     */
    if (vLevel == 2) {
        for ( i = 0; i < SMSYNC_NQS; i++ )
            cnt += buf.getVolSmsync.smsync[i];
        return ( cnt );
    }


    /*
     * return # bufs entered onto the smoothsync queues during the last
     * interval (or boot time)
     */
    cnt = buf.getVolSmsync.smsyncQ_cnt - prevcnt[vol];
    prevcnt[vol] = buf.getVolSmsync.smsyncQ_cnt;
    if (vLevel == 3 && sflag)
        return ( buf.getVolSmsync.smsyncQ_cnt );
    else if (vLevel == 3)
        return ( cnt );

    return(0);
}


/*
 * display the current buf count on each smoothsync queue
 */
void print_vol_smsyncstats(mlDmnParamsT *dmnParams, mlDmnStatT *dmnStats)
{
    libParamsT buf;
    u_int volindx;
    int i, j;

    buf.getVolSmsync.bfDomainId = dmnParams->bfDomainId;

    for( volindx = i = 1; i <= dmnStats->dmnParams->curNumVols; volindx++ ) {
        buf.getVolSmsync.volIndex = volindx;

        if (msfs_syscall(ADVFS_GET_SMSYNC_STATS, &buf, sizeof(buf.getVolSmsync)))
            continue;
        else {
            i++;
            printf ( catgets(catd, 1, 93, "volume %d:\n"), volindx );
            printf ( catgets(catd, 1, 94, "  smsyncQ counter:  %d\n"), buf.getVolSmsync.smsyncQ_cnt );
            printf ( catgets(catd, 1, 95, "  smsync queues:") );
            for ( j = 0; j < SMSYNC_NQS; j++ ) {
                if ( !(j%8) ) 
                    printf ( "\n    %2d-%2d:\t", j, j+7 < SMSYNC_NQS ? j+7 : SMSYNC_NQS );
                printf ( " %6d", buf.getVolSmsync.smsync[j] );
            }
            printf("\n\n");
        }
    }

    fflush( stdout );
}


void print_namei_stats( 
    struct nchstats *nameiStats1, 
    struct nchstats *nameiStats2
    )
{
    static int cnt = 0;
    int i;

    if ((cnt % 20) == 0) {
        printf(catgets(catd, 1, 37, "\n goodh  negh  badh falsh  miss \n") );
    }

    print_num5( nameiStats2->ncs_goodhits - nameiStats1->ncs_goodhits );
    print_num5( nameiStats2->ncs_neghits - nameiStats1->ncs_neghits );
    print_num5( nameiStats2->ncs_badhits - nameiStats1->ncs_badhits );
    print_num5( nameiStats2->ncs_falsehits - nameiStats1->ncs_falsehits );
    print_num5( nameiStats2->ncs_miss - nameiStats1->ncs_miss );
    printf("\n");
    fflush( stdout );
    cnt++;
}

void print_fset_stats( 
    mlFileSetStatsT *fsetStats1, 
    mlFileSetStatsT *fsetStats2,
    int fLevel
    )
{
    static int cnt = 0;
    int i;

    if (fLevel == 0) {
        printf( "%10s", catgets(catd, 1, 38, "lookup") );
        print_num4( fsetStats2->msfs_lookup - fsetStats1->msfs_lookup );
        printf( "%10s", catgets(catd, 1, 39, "create") );
        print_num4( fsetStats2->msfs_create - fsetStats1->msfs_create );
        printf( "%10s", catgets(catd, 1, 40, "close") );
        print_num4( fsetStats2->msfs_close - fsetStats1->msfs_close );
        printf( "%10s", catgets(catd, 1, 41, "getattr") );
        print_num4( fsetStats2->msfs_getattr - fsetStats1->msfs_getattr );
        printf( "%10s", catgets(catd, 1, 42, "setattr") );
        print_num4( fsetStats2->msfs_setattr - fsetStats1->msfs_setattr );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, 43, "read") );
        print_num4( fsetStats2->msfs_read - fsetStats1->msfs_read );
        printf( "%10s", catgets(catd, 1, 44, "write") );
        print_num4( fsetStats2->msfs_write - fsetStats1->msfs_write );
        printf( "%10s", catgets(catd, 1, 45, "mmap") );
        print_num4( fsetStats2->msfs_mmap - fsetStats1->msfs_mmap );
        printf( "%10s", catgets(catd, 1, 46, "fsync") );
        print_num4( fsetStats2->msfs_fsync - fsetStats1->msfs_fsync );
        printf( "%10s", catgets(catd, 1, 47, "syncdata") );
        print_num4( fsetStats2->msfs_syncdata - fsetStats1->msfs_syncdata );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, 48, "remove") );
        print_num4( fsetStats2->msfs_remove - fsetStats1->msfs_remove );
        printf( "%10s", catgets(catd, 1, 49, "rename") );
        print_num4( fsetStats2->msfs_rename - fsetStats1->msfs_rename );
        printf( "%10s", catgets(catd, 1, 50, "readdir") );
        print_num4( fsetStats2->msfs_readdir - fsetStats1->msfs_readdir );
        printf( "%10s", catgets(catd, 1, 51, "mkdir") );
        print_num4( fsetStats2->msfs_mkdir - fsetStats1->msfs_mkdir );
        printf( "%10s", catgets(catd, 1, 52, "rmdir") );
        print_num4( fsetStats2->msfs_rmdir - fsetStats1->msfs_rmdir );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, 53, "symlink") );
        print_num4( fsetStats2->msfs_symlink - fsetStats1->msfs_symlink );
        printf( "%10s", catgets(catd, 1, 54, "readlink") );
        print_num4( fsetStats2->msfs_readlink - fsetStats1->msfs_readlink );
        printf( "%10s", catgets(catd, 1, 55, "link") );
        print_num4( fsetStats2->msfs_link - fsetStats1->msfs_link );
        printf( "%10s", catgets(catd, 1, 56, "bread") );
        print_num4( fsetStats2->msfs_bread - fsetStats1->msfs_bread );
        printf( "%10s", catgets(catd, 1, 57, "brelse") );
        print_num4( fsetStats2->msfs_brelse - fsetStats1->msfs_brelse );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, 58, "page_write") );
        print_num4( fsetStats2->msfs_page_write - fsetStats1->msfs_page_write );
        printf( "%10s", catgets(catd, 1, 59, "page_read") );
        print_num4( fsetStats2->msfs_page_read - fsetStats1->msfs_page_read );
        printf( "%10s", catgets(catd, 1, 60, "getpage") );
        print_num4( fsetStats2->msfs_getpage - fsetStats1->msfs_getpage );
        printf( "%10s", catgets(catd, 1, 61, "putpage") );
        print_num4( fsetStats2->msfs_putpage - fsetStats1->msfs_putpage );
        printf("\n");

    } else if (fLevel == 1) {
        double lookups = (double) (fsetStats2->msfs_lookup - fsetStats1->msfs_lookup);

        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, 62, "\nlookup   hit     %% noent     %%  miss     %%\n") );
        }

        print_num5( (unsigned) lookups );
        print_num5( fsetStats2->lookup.hit - fsetStats1->lookup.hit );
        if (lookups > 0.0) {
            printf( "%6.1f", (double) (fsetStats2->lookup.hit - fsetStats1->lookup.hit) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }
        print_num5( fsetStats2->lookup.hit_not_found - fsetStats1->lookup.hit_not_found );
        if (lookups > 0.0) {
            printf( "%6.1f", (double) (fsetStats2->lookup.hit_not_found - fsetStats1->lookup.hit_not_found) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }

        print_num5( fsetStats2->lookup.miss - fsetStats1->lookup.miss );
        if (lookups > 0.0) {
            printf( "%6.1f", (double) (fsetStats2->lookup.miss - fsetStats1->lookup.miss) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }

    } else {
        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, 63, "\n lkup  crt geta read writ fsnc dsnc   rm   mv rdir  mkd  rmd link\n") );
        }

        print_num4( fsetStats2->msfs_lookup - fsetStats1->msfs_lookup );
        print_num4( fsetStats2->msfs_create - fsetStats1->msfs_create );
        print_num4( fsetStats2->msfs_getattr - fsetStats1->msfs_getattr );
        print_num4( fsetStats2->msfs_read - fsetStats1->msfs_read );
        print_num4( fsetStats2->msfs_write - fsetStats1->msfs_write );
        print_num4( fsetStats2->msfs_fsync - fsetStats1->msfs_fsync );
        print_num4( fsetStats2->msfs_syncdata - fsetStats1->msfs_syncdata );
        print_num4( fsetStats2->msfs_remove - fsetStats1->msfs_remove );
        print_num4( fsetStats2->msfs_rename - fsetStats1->msfs_rename );
        print_num4( fsetStats2->msfs_readdir - fsetStats1->msfs_readdir );
        print_num4( fsetStats2->msfs_mkdir - fsetStats1->msfs_mkdir );
        print_num4( fsetStats2->msfs_rmdir - fsetStats1->msfs_rmdir );
        print_num4( fsetStats2->msfs_link - fsetStats1->msfs_link );
    }

    printf("\n");
    fflush( stdout );
    cnt++;
}

void print_lock_stats( 
    mlAdvfsLockStatsT *lockStats1, 
    mlAdvfsLockStatsT *lockStats2,
    int l
    )
{
    static int cnt = 0;
    int i;

    switch (l) {

      case 1:
      case 2:
        printf( catgets(catd, 1, 64, "%20s  wait rwait signl bcast  lock"), catgets(catd, 1, 65, "type") );
        printf( "\n%20s                        ", catgets(catd, 1, 66, "mutex") );
        print_num5( lockStats2->mutexLock - lockStats1->mutexLock );
        printf( "\n%20s", catgets(catd, 1, 67, "genLk") );
        print_num5( lockStats2->genWait - lockStats1->genWait );
        print_num5( lockStats2->genReWait - lockStats1->genReWait );
        print_num5( lockStats2->genSignal - lockStats1->genSignal );
        print_num5( lockStats2->genBroadcast - lockStats1->genBroadcast );
        print_num5( lockStats2->genLock - lockStats1->genLock );
        printf( "\n%20s", catgets(catd, 1, 68, "stateLk") );
        print_num5( lockStats2->stateWait - lockStats1->stateWait );
        print_num5( lockStats2->stateReWait - lockStats1->stateReWait );
        print_num5( lockStats2->stateSignal - lockStats1->stateSignal );
        print_num5( lockStats2->stateBroadcast - lockStats1->stateBroadcast );
        print_num5( lockStats2->stateLock - lockStats1->stateLock );
        printf( "\n%20s", catgets(catd, 1, 69, "shrLk") );
        print_num5( lockStats2->shrWait - lockStats1->shrWait );
        print_num5( lockStats2->shrReWait - lockStats1->shrReWait );
        print_num5( lockStats2->shrSignal - lockStats1->shrSignal );
        printf( "     0" );
        print_num5( lockStats2->shrLock - lockStats1->shrLock );
        printf( "\n%20s", catgets(catd, 1, 70, "excLk") );
        print_num5( (lockStats2->excWaitShr - lockStats1->excWaitShr) + 
                    (lockStats2->excWaitExc - lockStats1->excWaitExc) );
        print_num5( (lockStats2->excReWaitShr - lockStats1->excReWaitShr) + 
                    (lockStats2->excReWaitExc - lockStats1->excReWaitExc) );
        print_num5( lockStats2->excSignal - lockStats1->excSignal );
        print_num5( lockStats2->excBroadcast - lockStats1->excBroadcast );
        print_num5( lockStats2->excLock - lockStats1->excLock );
        printf( "\n%20s", catgets(catd, 1, 71, "bufStateLk") );
        print_num5( lockStats2->bufWait - lockStats1->bufWait );
        print_num5( lockStats2->bufReWait - lockStats1->bufReWait );
        print_num5( lockStats2->bufSignal - lockStats1->bufSignal );
        print_num5( lockStats2->bufBroadcast - lockStats1->bufBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, 72, "pinBlkCv") );
        print_num5( lockStats2->pinBlockWait - lockStats1->pinBlockWait );
        print_num5( lockStats2->pinBlockReWait - lockStats1->pinBlockReWait );
        print_num5( lockStats2->pinBlockSignal - lockStats1->pinBlockSignal );
        print_num5( lockStats2->pinBlockBroadcast - lockStats1->pinBlockBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, 73, "bfFlushCv") );
        print_num5( lockStats2->bfFlushWait - lockStats1->bfFlushWait );
        print_num5( lockStats2->bfFlushReWait - lockStats1->bfFlushReWait );
        print_num5( lockStats2->bfFlushSignal - lockStats1->bfFlushSignal );
        print_num5( lockStats2->bfFlushBroadcast - lockStats1->bfFlushBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, 74, "ftxCv") );
        print_num5( lockStats2->ftxWait - lockStats1->ftxWait );
        print_num5( lockStats2->ftxReWait - lockStats1->ftxReWait );
        print_num5( lockStats2->ftxSignal - lockStats1->ftxSignal );
        print_num5( lockStats2->ftxBroadcast - lockStats1->ftxBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, 75, "msgQCv") );
        print_num5( lockStats2->msgQWait - lockStats1->msgQWait );
        print_num5( lockStats2->msgQReWait - lockStats1->msgQReWait );
        print_num5( lockStats2->msgQSignal - lockStats1->msgQSignal );
        print_num5( lockStats2->msgQBroadcast - lockStats1->msgQBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, 76, "total") );
        print_num5( lockStats2->wait - lockStats1->wait );
        printf( "     0" );
        print_num5( lockStats2->signal - lockStats1->signal );
        print_num5( lockStats2->broadcast - lockStats1->broadcast );
        printf( "\n" );

        if (l == 1) {
            break;
        }

        printf( catgets(catd, 1, 77, "\n%20s  wait rwait signl bcast  lock\n"), catgets(catd, 1, 65, "type") );

        for (i = 1; i < 43; i++) {
            printf( "%20s", lkUsageNames[i] );
            print_num5( lockStats2->usageStats[i].wait - lockStats1->usageStats[i].wait );
            print_num5( lockStats2->usageStats[i].reWait - lockStats1->usageStats[i].reWait );
            print_num5( lockStats2->usageStats[i].signal - lockStats1->usageStats[i].signal );
            print_num5( lockStats2->usageStats[i].broadcast - lockStats1->usageStats[i].broadcast );
            print_num5( lockStats2->usageStats[i].lock - lockStats1->usageStats[i].lock );
            printf( "\n" );
        }

        printf( "\n" );

        break;

      case 0:
      default:
        if ((cnt %20) == 0) {
            printf( catgets(catd, 1, 78, "\n mutex  wait   sig bcast\n") );
        }

        print_num5( lockStats2->mutexLock - lockStats1->mutexLock );
        print_num5( lockStats2->wait - lockStats1->wait );
        print_num5( lockStats2->signal - lockStats1->signal );
        print_num5( lockStats2->broadcast - lockStats1->broadcast );
        printf( "\n" );
    }

    fflush( stdout );
    cnt++;
}

main( int argc, char *argv[] )
{
    char *domain, *fset;
    mlStatusT sts;
    mlDmnParamsT dmnParams;
    int r = 0, l = 0, B = 0, p = 0, c = 0, cc, i = 0, 
        n = 0, f = 0, v = 0, b = 0, R = 0, s = 0, S = 0;
    int repeat, interval = 1, count = 0, lLevel = 0, vLevel = 0;
    int fLevel = 0;
    int bmtRead = 1;
    char *pp;
    mlDmnStatT *dmnStats = NULL; 
    mlDmnStatT *dmnStats2 = NULL;
    mlAdvfsLockStatsT lockStats1, lockStats2;
    struct nchstats nameiStats1, nameiStats2;
    mlFileSetStatsT fsetStats1, fsetStats2;
    int z, report_wanted = 1;
    char * zero[6] = {"0", "-0", "+0", "0.0", "-0.0", "+0.0"};

    extern int optind;
    extern char *optarg;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_ADVFSSTAT, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((cc = getopt( argc, argv, "f:nl:bv:rpi:c:B:RsS" )) != EOF) {
        switch (cc) {
            case 'r':
                r++;
                break;

            case 'f':
                f++;
                fLevel = strtoul( optarg, &pp, 0 );
                if (fLevel > 2) {
                   fprintf( stderr, catgets(catd, 1, 79, "%s: invalid arg to -f\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'n':
                n++;
                break;

            case 'b':
                b++;
                break;

            case 'l':
                l++;
                lLevel = strtoul( optarg, &pp, 0 );
                if (lLevel > 2) {
                   fprintf( stderr, catgets(catd, 1, 80, "%s: invalid arg to -l\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'B':
                B++;
                if (optarg[0] == 'r') {
                   bmtRead = 1;
                } else if (optarg[0] == 'w') {
                   bmtRead = 0;
                } else {
                   fprintf( stderr, catgets(catd, 1, 81, "%s: invalid arg to -B\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'v':
                v++;
                vLevel = strtoul( optarg, &pp, 0 );
                if (vLevel > 3) {
                   fprintf( stderr, catgets(catd, 1, 82, "%s: invalid arg to -v\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'R':
                R++;
                break;

            case 'p':
                p++;
                break;

            case 'i':
                i++;
                interval = strtoul( optarg, &pp, 0 );
                if (interval < 0) {
                    printf("\nA negative time interval has been entered.\n");
                    usage();
                    exit( 1 );
                }
                /*
                 * Check for the case where the i option does not include
                 * the "sec" field, that is, an interval is not specified.
                 * This could be a problem  if no other options are specified.
                 * In this case, you now have one less command line argument
                 * than expected.  This would cause a usage error to occur.
                 * Adjusting optind will prevent this.  This change will be
                 * ignored if there are more command line options.
                 */
                if (interval == 0) {
                    /*
                     * If the "sec" field was omitted, interval will be 0.
                     * You can also have interval 0 if the interval was
                     * +/-0, +/-0.0, 0.0, or 0.
                     *
                     * First test for the 6 different zeroes.  If these
                     * are not found, we have an omitted "sec" field.
                     */
                    for (z = 0; z < 6; z++) {
                        if (!strcmp(optarg, zero[z])) {
                            printf("\nA zero time interval has been entered.\n");
                            usage();
                            exit( 1 );
                        }
                    }
                    optind--;
                    interval = 1;
                }
                break;

            case 'c':
                c++;
                count = strtoul( optarg, &pp, 0 );
                if (count < 0) {
                    report_wanted = 0;
                }
                /*
                 * Check for the case where the c option does not include
                 * the "count" field, that is, an interval is not specified.
                 * This is a problem because you now have one less command
                 * line argument than expected.  This will cause an error
                 * below.  We will leave the count as 0.  We do need to
                 * modify optind however, for a possible usage check (if this
                 * is the last option specified.
                 *
                 * If the count argument was omitted, set count to 1.
                 */
                if (count == 0) {
                    /*
                     * If the "count" field was omitted, the "count" should
                     * be 1.
                     */
                    if (count == (*(int *)optarg - 0x30)) {  /* Real 0. */
                        report_wanted = 0;
                    } else {               /* omitted count field. */
                        optind--;
                        count = 1;
                    }
                }
                if (report_wanted == 0) {
                    printf("\nNo report has been printed.  A non-positive count of %d was specified\n", count);
                    printf("with the -c option.\n");
                    usage();
                    exit( 1 );
                }
                break;

            case 's':
                s++;
                break;

            case 'S':
                S++;
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (r > 1 || l > 1 || B > 1 || R > 1 || f > 1 || s > 1 ||
        p > 1 || b > 1 || i > 1 || c > 1 || v > 1 || n > 1) {
        usage();
        exit( 1 );
    }

    if ((l + B + b + v + p + r + f + n + S) > 1) {
        usage();
        exit( 1 );
    }

    if (R && ((l + B + f + v + n + S) > 0)) {
        usage();
        exit( 1 );
    }

    if (!f && (optind != (argc - 1))) {
        /* missing required domain */
        usage();
        exit( 1 );
    }

    if (f && (optind != (argc - 2))) {
        /* missing required domain and fset */
        usage();
        exit( 1 );
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, ONDISK,
            "%s: This utility can not process the on-disk structures on this system.\n"), Prog);
        exit(1);
    }

    domain = argv[optind];
    fset = argv[optind+1];

    sts = msfs_get_dmnname_params( domain, &dmnParams );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 83, "%s: unable to get info for domain '%s'\n"), 
                 Prog, domain );
        fprintf( stderr, catgets(catd, 1, 84, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        exit( 1 );
    } 

    if (l) {
        sts = advfs_get_lock_stats( &lockStats1 );
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 85, "%s: unable to get lock stats; %s\n"),
                     Prog, BSERRMSG( sts ) );
            exit( 1 );
        }

    } else if (n) {
        if ( table(TBL_NCSTATS, 1, &nameiStats1, 1, sizeof(struct nchstats)) != 1 ) {
            fprintf( stderr, catgets(catd, 1, 86, "%s: unable to get namei stats\n"), Prog );
            exit( 1 );
        }

    } else if (f) {
        sts = advfs_fset_get_stats( domain, fset, &fsetStats1 );
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 87, "%s: unable to get fileset stats; %s\n"),
                     Prog, BSERRMSG( sts ) );
            exit( 1 );
        }

    } else {
        dmnStats = msfs_init_dmnstat( dmnParams.bfDomainId );
        if (dmnStats == NULL) {
            fprintf( stderr, catgets(catd, 1, 88, "%s: unable to get stats for domain '%s'\n"),
                     Prog, domain );
            exit( 1 );
        }
    }

    if (count == 0) {
        repeat = 1;
    } else {
        repeat = count;
    }

    while (repeat) {

        sleep( interval );

        if (l) {
            sts = advfs_get_lock_stats( &lockStats2 );
            if (sts != EOK) {
                fprintf( stderr, catgets(catd, 1, 85, "%s: unable to get lock stats; %s\n"),
                         Prog, BSERRMSG( sts ) );
                exit( 1 );
            }
	    
	    if (s) {
		/* We want the raw statistics.  Zero out lockStat1 */
		/* and that is what you will get                   */
		memset(&lockStats1, 0, sizeof(lockStats1));
	    }
            print_lock_stats( &lockStats1, &lockStats2, lLevel );
            lockStats1 = lockStats2;

        } else if (n) {
            if ( table(TBL_NCSTATS, 1, &nameiStats2, 1, sizeof(struct nchstats)) != 1 ) {
                fprintf( stderr, catgets(catd, 1, 86, "%s: unable to get namei stats\n"), Prog );
                exit( 1 );
            }

	    if (s) {
		/* We want the raw statistics.  Zero out nameiStat1 */
		/* and that is what you will get                   */
		memset(&nameiStats1, 0, sizeof(nameiStats1));
	    }
	    print_namei_stats( &nameiStats1, &nameiStats2 );
            nameiStats1 = nameiStats2;

        } else if (f) {
            sts = advfs_fset_get_stats( domain, fset, &fsetStats2 );
            if (sts != EOK) {
                fprintf( stderr, catgets(catd, 1, 87, "%s: unable to get fileset stats; %s\n"),
                         Prog, BSERRMSG( sts ) );
                exit( 1 );
            }
	    if (s) {
		/* We want the raw statistics.  Zero out fsetStat1 */
		/* and that is what you will get                   */
		memset(&fsetStats1, 0, sizeof(fsetStats1));
	    }

            print_fset_stats( &fsetStats1, &fsetStats2, fLevel );
            fsetStats1 = fsetStats2;

        } else if (B || b || v || p || r || S) {
	    
	    if (s) {
		dmnStats2 = msfs_init_dmnstat( dmnParams.bfDomainId );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, 88, "%s: unable to get stats for domain '%s'\n"),
			    Prog, domain );
		    exit( 1 );
		}
	    }
	    else
	    {
		if (vLevel == 3)
			dmnStats2 = msfs_get_dmnstat2( dmnStats, 
						      dmnParams.bfDomainId, 
						      1 );
		else 
			dmnStats2 = msfs_get_dmnstat( dmnStats, 
						     dmnParams.bfDomainId, 
						     1 );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, 89, "%s: unable to update stats for domain '%s'\n"),
			    Prog, domain );
		    exit( 1 );
		}
	    }

            if (b) {
                print_buf_cache_stats( dmnStats2, R );
            } else if (p) {
                print_buf_cache_pin_stats( dmnStats2, R );
            } else if (r) {
                print_buf_cache_ref_stats( dmnStats2, R );
            } else if (B) {
                print_bmt_stats( dmnStats2, R, bmtRead );
            } else if (S) {
                print_vol_smsyncstats( &dmnParams, dmnStats2 );
            } else {
                print_vol_stats( dmnStats2, vLevel, s );
            }

            msfs_free_dmnstat( dmnStats2 );

        } else {

	    if (s) {
		dmnStats2 = msfs_init_dmnstat( dmnParams.bfDomainId );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, 88, "%s: unable to get stats for domain '%s'\n"),
			    Prog, domain );
		    exit( 1 );
		}
	    }
	    else
	    {
		dmnStats2 = msfs_get_dmnstat( dmnStats, 
					     dmnParams.bfDomainId, 
					     1 );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, 89, "%s: unable to update stats for domain '%s'\n"),
			    Prog, domain );
		    exit( 1 );
		}
	    }
	    /* No longer use the print statements inside libmsfs_stat */
	    /* instead use our newly created one                      */
            print_generic_dmnstat( dmnStats2, dmnParams.bfDomainId );
            msfs_free_dmnstat( dmnStats2 );
        }

        if (!i || (count > 0)) {
            repeat--;
        }
    }

    msfs_free_dmnstat( dmnStats );
}
