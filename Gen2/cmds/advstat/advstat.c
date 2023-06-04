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

/*
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 */

#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <ctype.h>
#include <signal.h>
#include <unistd.h>
#include <pwd.h>
#include <sys/file.h>
#include <sys/param.h> 
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <advfs/advfs_syscalls.h>
#include <assert.h>
#include <locale.h>
#include "advstat_advfs_msg.h"

nl_catd catd;

char *Prog;
char fsPrintName[MAXPATHLEN+1];

void usage( void )
{
printf( catgets(catd, 1, USAGE1, "\nusage: %s [options] [stats-type] {special | fsname}\n"), Prog );
printf( catgets(catd, 1, USAGE2, "\noptions:\n") );
printf( catgets(catd, 1, USAGEI, "\t-i sec  \trepeat display every 'sec' seconds\n") );
printf( catgets(catd, 1, USAGEC, "\t-c count\trepeat display 'count' times\n") );
printf( catgets(catd, 1, USAGES, "\t-s      \tdisplay raw statistics\n") );
printf( catgets(catd, 1, USAGEB, "\t-b      \tbuffer cache statistics\n") );
printf( catgets(catd, 1, USAGEF0, "\t-f 0    \tall filesystem vnop statistics\n") );
printf( catgets(catd, 1, USAGEF1, "\t-f 1    \tall filesystem lookup statistics\n") );
printf( catgets(catd, 1, USAGEF2, "\t-f 2    \tcommon filesystem vnop statistics\n") );
printf( catgets(catd, 1, USAGEG, "\t-g      \tlog statistics\n") );
printf( catgets(catd, 1, USAGEL0, "\t-l 0    \tbasic lock statistics\n") );
printf( catgets(catd, 1, USAGEL1, "\t-l 1    \tlock statistics\n") );
printf( catgets(catd, 1, USAGEL2, "\t-l 2    \tdetailed lock statistics\n") );
printf( catgets(catd, 1, USAGEV, "\t-v      \tvolume read/write statistics\n") );
printf( catgets(catd, 1, USAGER, "\t-B r    \tBMT Record read statistics\n") );
printf( catgets(catd, 1, USAGEW, "\t-B w    \tBMT Record write/update statistics\n") );
}

/* When computing K and M use 1024 and 1048576 respectively */
#define KB 1024
#define MB 1048576
#define GB 1073741824

/*
 * Add an additional space to prevent fields from appearing contiguous.
 *
 * If num lies between 1000 and 1024, round up to display 1K rather
 * than 0K.  Same for M.
 *
 */
void print_num3( uint32_t num)
{
    if (num < 1000) {
        printf( "%4u ", num );
    } else if (num < 1000000) {
        printf( "%3uK ", num / KB ? num / KB : 1 );
    } else if (num < 1000000000) {
        printf( "%3uM ", num / MB ? num / MB : 1 );
    } else if (num < 1000000000000) {
        printf( "%3uG ", num / GB ? num / GB : 1 ); 
    } else {
        printf( " *** " );
    }
}
void print_num4( uint32_t num )
{
    if (num < 10000) {
        printf( "%5u", num );
    } else if (num < 1000000) {
        printf( "%4uK", num / KB ? num / KB : 1 );
    } else if (num < 1000000000) {
        printf( "%4uM", num / MB ? num / MB : 1 );
    } else if (num < 1000000000000) {
        printf( "%3uG ", num / GB ? num / GB : 1 ); 
    } else {
        printf( " ****" );
    }
}

void print_num5( uint32_t num )
{
    if (num < 10000) {
        printf( "%6u", num );
    } else if (num < 1000000) {
        printf( "%5uK", num / KB ? num / KB : 1 );
    } else if (num < 1000000000) {
        printf( "%5uM", num / MB ? num / MB : 1 );
    } else if (num < 1000000000000) {
        printf( "%3uG ", num / GB ? num / GB : 1 ); 
    } else {
        printf( " *****" );
    }
}



/*
 * print_generic_dmnstat() 
 *
 * print the current filesystem struct.
 */

void
print_generic_dmnstat( 
    adv_dmn_stats_t  *dmnStat,      /* in */
    bfDomainIdT  dmnid         /* in */
    )
{
    int i;
    uint32_t unpin;

    if( ( dmnid.id_sec == 0 ) && ( dmnid.id_usec == 0 ) ){
        return;
    }
    if( dmnStat == NULL ) {
        return;
    }

    /* 
     * Print stats
     */
    printf("\n");
    printf("Filesystem -%d.%d- Stats -\n", dmnid.id_sec, dmnid.id_usec);
    printf("\n");

    printf("%-8s%-10d%\n",
        "RefCnt",
        dmnStat->dmnParams->bcStat.refCnt);

    unpin = dmnStat->dmnParams->bcStat.unpinCnt.blocking +
        dmnStat->dmnParams->bcStat.unpinCnt.lazy +
        dmnStat->dmnParams->bcStat.unpinCnt.log +
        dmnStat->dmnParams->bcStat.unpinCnt.clean;

    printf("%-8s%-10d%",
        "Unpin",
        unpin);

    printf("--> ");
    printf("%-8s%-10d%-8s%-10d%-12s%-10d%-10s%-8d\n",
        "Lazy",
        dmnStat->dmnParams->bcStat.unpinCnt.lazy,
        " Log",
        dmnStat->dmnParams->bcStat.unpinCnt.log,
        " Blocking",
        dmnStat->dmnParams->bcStat.unpinCnt.blocking,
        " Clean",
        dmnStat->dmnParams->bcStat.unpinCnt.clean );

    printf("\n%10s %10s %10s %10s %10s\n", 
           "Disk",
           "Reads",
           "Writes",
           "Rblks",
           "Wblks" );
    printf("%10s %10s %10s %10s %10s\n", 
           "----",
           "-----",
           "------",
           "-----",
           "-----" );

    for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
        printf("%10d %10d %10d %10d %10d\n",
            dmnStat->volIndex[i],
            dmnStat->volCounters[i].nread,
            dmnStat->volCounters[i].nwrite,
            dmnStat->volCounters[i].readblk,
            dmnStat->volCounters[i].writeblk
            );
    }
    printf("\n");
}


/*
 * Numerical definitions for the typed records in the BMT
 * are defined in bs_ods.h.  By design, only the first 23
 * record types will be reported on.  
 */
char* fStatType[] = {"",
                    "BSR_XTNTS=",              /* BSR_XTNTS              = 1  */
                    "BSR_ATTR=",               /* BSR_ATTR               = 2  */
                    "BSR_VD_ATTR=",            /* BSR_VD_ATTR            = 3  */
                    "BSR_DMN_ATTR=",           /* BSR_DMN_ATTR           = 4  */
                    "BSR_XTRA_XTNTS=",         /* BSR_XTRA_XTNTS         = 5  */
                    "BSR_MCELL_FREE_LIST=",    /* BSR_MCELL_FREE_LIST    = 6  */
                    "BSR_BFS_ATTR=",           /* BSR_BFS_ATTR           = 7  */
                    "BSR_VD_IO_PARAMS=",       /* BSR_VD_IO_PARAMS       = 8  */
                    "BSR_DEF_DEL_MCELL_LIST=", /* BSR_DEF_DEL_MCELL_LIST = 9  */
                    "BSR_DMN_MATTR=",          /* BSR_DMN_MATTR          = 10 */
                    "BSR_BF_INHERIT_ATTR=",    /* BSR_BF_INHERIT_ATTR    = 11 */
                    "BSR_BFS_QUOTA_ATTR=",     /* BSR_BFS_QUOTA_ATTR     = 12 */
                    "BSR_ACL_REC=",            /* BSR_ACL_REC            = 13 */
                    "BSR_DMN_TRANS_ATTR=",     /* BSR_DMN_TRANS_ATTR     = 14 */
                    "BSR_DMN_SS_ATTR=",        /* BSR_DMN_SS_ATTR        = 15 */
                    "BSR_DMN_FREEZE_ATTR=",    /* BSR_DMN_FREEZE_ATTR    = 16 */
                    "BSR_RUN_TIMES=",          /* BSR_RUN_TIMES          = 17 */
                    "BSR_FS_NAME="             /* BSR_FS_NAME            = 18 */
};

void print_bmt_stats( adv_dmn_stats_t *dmnStat, int bmtRead )
{
    static int cnt = 0;
    static int displayed[BSR_MAX+1] = { 0 };
    bmtStatT *bmtStat = &dmnStat->dmnParams->bmtStat;
    int i;

    if ((cnt % 20) == 0) {
        for (i = 0; i <= BSR_MAX; i++ ) {
            displayed[i] = 0;
        }
        printf( "\n" );
    }

    printf( "BMTR_FS_STAT=" );
    assert(sizeof(fStatType)/sizeof(fStatType[0]) == BSR_MAX+1);

    if (bmtRead) {
        print_num3( bmtStat->fStatRead );

        for (i = 0; i <= BSR_MAX; i++ ) {
            if ((bmtStat->bmtRecRead[i] > 0) || displayed[i]) {
                printf( fStatType[i] );
                print_num3( bmtStat->bmtRecRead[i] );
                displayed[i] = 1;
            }
        }

    } else {
        print_num3( bmtStat->fStatWrite );

        for (i = 0; i <= BSR_MAX; i++ ) {
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

void print_buf_cache_pin_stats( adv_dmn_stats_t *dmnStat, int R )
{
    static int cnt = 0;
    int64_t pins, bsFtx, fsFtx, other;

    if ((cnt % 20) == 0) {
        printf(catgets(catd, 1, HDR3, " pin                    pin-type          data-type   \n") );
        printf(catgets(catd, 1, HDR4, "  cnt   lazy   blk   cln   log other\n") );
    }

    pins = (int64_t) (dmnStat->dmnParams->bcStat.unpinCnt.blocking +
                     dmnStat->dmnParams->bcStat.unpinCnt.lazy +
                     dmnStat->dmnParams->bcStat.unpinCnt.log +
                     dmnStat->dmnParams->bcStat.unpinCnt.clean );

    other = pins - 
            (int64_t) dmnStat->dmnParams->bcStat.unpinCnt.log;

    print_num4( (uint32_t) pins );

    if (!R) {
        print_num5( dmnStat->dmnParams->bcStat.unpinCnt.lazy );
        print_num5( dmnStat->dmnParams->bcStat.unpinCnt.blocking );
        print_num5( dmnStat->dmnParams->bcStat.unpinCnt.clean );
        print_num5( dmnStat->dmnParams->bcStat.unpinCnt.log );
        print_num5( (uint32_t) other );

    } else if (pins > 0.0) {
        printf( "%6.1f", 
                dmnStat->dmnParams->bcStat.unpinCnt.lazy / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->bcStat.unpinCnt.blocking / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->bcStat.unpinCnt.clean / pins * 100.0 );
        printf( "%6.1f", 
                dmnStat->dmnParams->bcStat.unpinCnt.log / pins * 100.0 );
        printf( "%6.1f", other / pins * 100.0 );
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

void print_vol_stats( adv_dmn_stats_t *dmnStat)
{
    int i;
    static int cnt = 0;
    static int numVols = 0;
    uint32_t rave, wave;

    if (((cnt %20) == 0) || (numVols != dmnStat->dmnParams->curNumVols)) {

        cnt = 0;

        for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
            printf( catgets(catd, 1, HDR5, "vol%1d    "), dmnStat->volIndex[i] );
        }
        printf( "\n" );
        
        for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
            printf( catgets(catd, 1, HDR6, "  rd  wr") );
        }

        printf( "\n" );
    }

    for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {
        print_num3( dmnStat->volCounters[i].nread );
        print_num3( dmnStat->volCounters[i].nwrite );
    }

    printf("\n");
    fflush( stdout );
    cnt++;
    numVols = dmnStat->dmnParams->curNumVols;
}


void print_log_stats( adv_dmn_stats_t *dmnStat )
{
    static int cnt = 0;

    if ((cnt % 20) == 0) {
        printf(catgets(catd, 1, HDR37, " writes trans segRecs trims wstWds maxPgs minPgs maxFWds maxAg maxSlots oldAg excWaits fullWaits\n") );
    }
    
    printf(" ");
    print_num5( dmnStat->dmnParams->logStat.logWrites );
    print_num5( dmnStat->dmnParams->logStat.transactions );
    printf("  ");
    print_num5( dmnStat->dmnParams->logStat.segmentedRecs );
    print_num5( dmnStat->dmnParams->logStat.logTrims );
    printf(" ");
    print_num5( dmnStat->dmnParams->logStat.wastedWords );
    printf(" ");
    print_num5( dmnStat->dmnParams->logStat.maxLogPgs );
    printf(" ");
    print_num5( dmnStat->dmnParams->logStat.minLogPgs );
    printf("  ");
    print_num5( dmnStat->dmnParams->logStat.maxFtxWords );
    print_num5( dmnStat->dmnParams->logStat.maxFtxAgent );
    printf("   ");
    print_num5( dmnStat->dmnParams->logStat.maxFtxTblSlots );
    print_num5( dmnStat->dmnParams->logStat.oldFtxTblAgent );
    printf("   ");
    print_num5( dmnStat->dmnParams->logStat.excSlotWaits );
    printf("    ");
    print_num5( dmnStat->dmnParams->logStat.fullSlotWaits );

    printf( "\n" );
    fflush( stdout );

    cnt++;
}

void print_fset_stats( 
    adv_file_set_stats_t *fsetStats1, 
    adv_file_set_stats_t *fsetStats2,
    int fLevel
    )
{
    static int cnt = 0;
    int i;

    if (fLevel == 0) {
        printf( "%10s", catgets(catd, 1, HDR7, "lookup") );
        print_num4( fsetStats2->advfs_lookup - fsetStats1->advfs_lookup );
        printf( "%10s", catgets(catd, 1, HDR8, "create") );
        print_num4( fsetStats2->advfs_create - fsetStats1->advfs_create );
        printf( "%10s", catgets(catd, 1, HDR9, "close") );
        print_num4( fsetStats2->advfs_close - fsetStats1->advfs_close );
        printf( "%10s", catgets(catd, 1, HDR10, "getattr") );
        print_num4( fsetStats2->advfs_getattr - fsetStats1->advfs_getattr );
        printf( "%10s", catgets(catd, 1, HDR11, "setattr") );
        print_num4( fsetStats2->advfs_setattr - fsetStats1->advfs_setattr );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, HDR12, "read") );
        print_num4( fsetStats2->advfs_read - fsetStats1->advfs_read );
        printf( "%10s", catgets(catd, 1, HDR13, "write") );
        print_num4( fsetStats2->advfs_write - fsetStats1->advfs_write );
        printf( "%10s", catgets(catd, 1, HDR14, "mmap") );
        print_num4( fsetStats2->advfs_mmap - fsetStats1->advfs_mmap );
        printf( "%10s", catgets(catd, 1, HDR15, "fsync") );
        print_num4( fsetStats2->advfs_fsync - fsetStats1->advfs_fsync );
        printf( "%10s", catgets(catd, 1, HDR16, "remove") );
        print_num4( fsetStats2->advfs_remove - fsetStats1->advfs_remove );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, HDR17, "rename") );
        print_num4( fsetStats2->advfs_rename - fsetStats1->advfs_rename );
        printf( "%10s", catgets(catd, 1, HDR18, "readdir") );
        print_num4( fsetStats2->advfs_readdir - fsetStats1->advfs_readdir );
        printf( "%10s", catgets(catd, 1, HDR19, "mkdir") );
        print_num4( fsetStats2->advfs_mkdir - fsetStats1->advfs_mkdir );
        printf( "%10s", catgets(catd, 1, HDR20, "rmdir") );
        print_num4( fsetStats2->advfs_rmdir - fsetStats1->advfs_rmdir );
        printf( "%10s", catgets(catd, 1, HDR21, "symlink") );
        print_num4( fsetStats2->advfs_symlink - fsetStats1->advfs_symlink );
        printf( "\n" );
        printf( "%10s", catgets(catd, 1, HDR22, "readlink") );
        print_num4( fsetStats2->advfs_readlink - fsetStats1->advfs_readlink );
        printf( "%10s", catgets(catd, 1, HDR23, "link") );
        print_num4( fsetStats2->advfs_link - fsetStats1->advfs_link );
        printf( "%10s", catgets(catd, 1, HDR26, "getpage") );
        print_num4( fsetStats2->advfs_getpage - fsetStats1->advfs_getpage );
        printf( "%10s", catgets(catd, 1, HDR27, "putpage") );
        print_num4( fsetStats2->advfs_putpage - fsetStats1->advfs_putpage );
        printf("\n");

    } else if (fLevel == 1) {
        int64_t lookups = (int64_t) (fsetStats2->advfs_lookup - fsetStats1->advfs_lookup);

        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, HDR28, "\nlookup   hit     %% noent     %%  miss     %%\n") );
        }

        print_num5( (uint32_t) lookups );
        print_num5( fsetStats2->lookup.hit - fsetStats1->lookup.hit );
        if (lookups > 0.0) {
            printf( "%6.1f", (int64_t) (fsetStats2->lookup.hit - fsetStats1->lookup.hit) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }
        print_num5( fsetStats2->lookup.hit_not_found - fsetStats1->lookup.hit_not_found );
        if (lookups > 0.0) {
            printf( "%6.1f", (int64_t) (fsetStats2->lookup.hit_not_found - fsetStats1->lookup.hit_not_found) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }

        print_num5( fsetStats2->lookup.miss - fsetStats1->lookup.miss );
        if (lookups > 0.0) {
            printf( "%6.1f", (int64_t) (fsetStats2->lookup.miss - fsetStats1->lookup.miss) / lookups * 100.0 );
        } else {
            printf( "%6.1f",  0.0 );
        }

    } else {
        if ((cnt % 20) == 0) {
            printf(catgets(catd, 1, HDR29, "\n lkup  crt geta read writ fsnc   rm   mv rdir  mkd  rmd link\n") );
        }

        print_num4( fsetStats2->advfs_lookup - fsetStats1->advfs_lookup );
        print_num4( fsetStats2->advfs_create - fsetStats1->advfs_create );
        print_num4( fsetStats2->advfs_getattr - fsetStats1->advfs_getattr );
        print_num4( fsetStats2->advfs_read - fsetStats1->advfs_read );
        print_num4( fsetStats2->advfs_write - fsetStats1->advfs_write );
        print_num4( fsetStats2->advfs_fsync - fsetStats1->advfs_fsync );
        print_num4( fsetStats2->advfs_remove - fsetStats1->advfs_remove );
        print_num4( fsetStats2->advfs_rename - fsetStats1->advfs_rename );
        print_num4( fsetStats2->advfs_readdir - fsetStats1->advfs_readdir );
        print_num4( fsetStats2->advfs_mkdir - fsetStats1->advfs_mkdir );
        print_num4( fsetStats2->advfs_rmdir - fsetStats1->advfs_rmdir );
        print_num4( fsetStats2->advfs_link - fsetStats1->advfs_link );
    }

    printf("\n");
    fflush( stdout );
    cnt++;
}

/*  In Tru64, we had an array of strings to match the locks so when we
 *  printed the stats there was some context.  This is no longer true in 
 *  HPUX, so we'll need to create our own.  If lkUsageT in 
 *  ms_generic_locks.h changes, this array will need to change as well.
 */

char*  lockNames[LKU_num_usage_types] = {
    "LKU_UNKNOWN",
    "LKU_BF_STATE"
};

void print_lock_stats( 
    adv_lock_stats_t *lockStats1, 
    adv_lock_stats_t *lockStats2,
    int l
    )
{
    static int cnt = 0;
    int i;

    switch (l) {

      case 1:
      case 2:
        printf( "\n%20s", catgets(catd, 1, HDR31, "type") );
        printf( catgets(catd, 1, HDR30, "  wait rwait signl bcast  lock") );
        printf( "\n%20s", catgets(catd, 1, HDR32, "stateLk") );
        print_num5( lockStats2->stateWait - lockStats1->stateWait );
        print_num5( lockStats2->stateReWait - lockStats1->stateReWait );
        print_num5( lockStats2->stateSignal - lockStats1->stateSignal );
        print_num5( lockStats2->stateBroadcast - lockStats1->stateBroadcast );
        print_num5( lockStats2->stateLock - lockStats1->stateLock );
        printf( "\n%20s", catgets(catd, 1, HDR33, "msgQCv") );
        print_num5( lockStats2->msgQWait - lockStats1->msgQWait );
        print_num5( lockStats2->msgQReWait - lockStats1->msgQReWait );
        print_num5( lockStats2->msgQSignal - lockStats1->msgQSignal );
        print_num5( lockStats2->msgQBroadcast - lockStats1->msgQBroadcast );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, HDR38, "ftxExc") );
        print_num5( lockStats2->ftxExcWait - lockStats1->ftxExcWait );
        print_num5( lockStats2->ftxExcReWait - lockStats1->ftxExcReWait );
        print_num5( lockStats2->ftxExcSignal - lockStats1->ftxExcSignal );
        printf( "     0" );
        printf( "     0" );
        printf( "\n%20s", catgets(catd, 1, HDR39, "ftxTrim") );
        print_num5( lockStats2->ftxTrimWait - lockStats1->ftxTrimWait );
        print_num5( lockStats2->ftxTrimReWait - lockStats1->ftxTrimReWait );
        print_num5( lockStats2->ftxTrimSignal - lockStats1->ftxTrimSignal );
        print_num5( lockStats2->ftxTrimBroadcast - lockStats1->ftxTrimBroadcast );
        printf( "     0" );
        printf( "\n" );
        printf( "\n%20s", catgets(catd, 1, HDR31, "type") );
        printf( catgets(catd, 1, HDR41, "  wait sigFree sigNext") );
        printf( "\n%20s", catgets(catd, 1, HDR40, "ftxSlot") );
        print_num5( lockStats2->ftxSlotWait - lockStats1->ftxSlotWait );
        print_num5( lockStats2->ftxSlotSignalFree - lockStats1->ftxSlotSignalFree );
        printf( "  " );
        print_num5( lockStats2->ftxSlotSignalNext - lockStats1->ftxSlotSignalNext );
        printf( "\n" );

        if (l == 1) {
            break;
        }

        printf( "\n%20s", catgets(catd, 1, HDR31, "type") );
        printf( catgets(catd, 1, HDR30, "  wait rwait signl bcast  lock") );
	printf("\n");
        for (i = 0; i < LKU_num_usage_types ; i++) {
            printf( "%20s", lockNames[i] );

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
            printf( catgets(catd, 1, HDR36, "\n wait   sig bcast\n") );
        }

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
    struct stat   fStat;
    adv_status_t sts;
    adv_bf_dmn_params_t dmnParams;
    int l = 0, B = 0, c = 0, cc, i = 0, f = 0, v = 0, s = 0, g = 0;
    int repeat;
    long count = 0, interval = 1;
    unsigned long fLevel = 0, lLevel = 0;
    int bmtRead = 1;
    adv_dmn_stats_t *dmnStats = NULL; 
    adv_dmn_stats_t *dmnStats2 = NULL;
    adv_lock_stats_t lockStats1, lockStats2;
    adv_file_set_stats_t fsetStats1, fsetStats2;
    int z, report_wanted = 1;
    char * zero[6] = {"0", "-0", "+0", "0.0", "-0.0", "+0.0"};
    arg_infoT*  infop;
    int raw = 0;
    char* badchars;

    extern int optind;
    extern char *optarg;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_ADVSTAT_ADVFS, NL_CAT_LOCALE);


    /*  Block SIGSYS - otherwise, if Advfs isn't built into the kernel, 
     *  Advfs syscalls will cause a core dump
     */
    signal(SIGSYS, SIG_IGN);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((cc = getopt( argc, argv, "f:l:gvi:c:B:s" )) != EOF) {
        switch (cc) {
            case 'f':
                f++;
                fLevel = strtoul( optarg, &badchars, 0 );
                if (*optarg == '-' || *badchars != '\0' || fLevel > 1) {
                   fprintf( stderr, catgets(catd, 1, BADARGF, 
                       "%s: invalid arg to -f\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'l':
                l++;
                lLevel = strtoul( optarg, &badchars, 0 );
                if (*badchars != '\0' || lLevel > 2) {
                   fprintf( stderr, catgets(catd, 1, BADARGL, 
                       "%s: invalid arg to -l\n"), Prog );
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
                   fprintf( stderr, catgets(catd, 1, BADARGB, 
                       "%s: invalid arg to -B\n"), Prog );
                   usage();
                   exit( 1 );
                }
                break;

            case 'v':
                v++;
                break;

            case 'g':
                g++;
                break;

            case 'i':
                i++;

                interval = strtol( optarg, NULL, 0 );

                if (interval == 0) {
                    fprintf(stderr, catgets(catd, 1, ERRINT, 
                        "%s: A zero time interval has been entered.\n"), Prog);
                    usage();
                    exit( 1 );
                } else if (interval < 0) {
                    fprintf(stderr, catgets(catd, 1, ERRINT2, 
                        "%s: A negative time interval has been entered.\n"), 
                        Prog);
                    usage();
                    exit( 1 );
                }
                break;

            case 'c':
                c++;

                count = strtol( optarg, NULL, 0 );

                /*  check to see if optarg was really specified.  If not,
                 *  set it to 1
                 */
                if (count == 0 && !isdigit(optarg[0])) {
                    count = 1;
                    optind--;
                }

                if (count <= 0) {
                    report_wanted = 0;
                }

                if (report_wanted == 0) {
                    fprintf(stderr, catgets(catd, 1, NORPT, "%s: No report has been printed.  A non-positive count of %d was specified with the -c option.\n"), Prog, count);
                    usage();
                    exit( 1 );
                }
                break;

            case 's':
                s++;
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (l > 1 || B > 1 || f > 1 || s > 1 ||
        i > 1 || c > 1 || v > 1  || g > 1) {
        usage();
        exit( 1 );
    }

    if ((l + B + v + f + g) > 1) {
        usage();
        exit( 1 );
    }

    if (optind != (argc - 1)) {
        /* missing required fsname */
        usage();
        exit( 1 );
    }

    if (s || (!i && !c)) {
        /*
         * Print raw statistics if requested or only 1 instance.
         */
        raw = 1;
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR, 
              BFD_ODS_LAST_VERSION_MINOR);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, ONDISK,
            "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        exit(1);
    }

    /*  normalize the fs|special argument to the advfs filesystem name */

    infop = process_advfs_arg(argv[optind]);

    if (infop == NULL) {
        fprintf(stderr, catgets(catd, 1, BADFSN, 
            "%s: error processing specified name %s\n"), Prog, argv[optind]);
        exit(1);
    }
   
    if (infop->is_multivol) {
        strcpy(fsPrintName, infop->fsname);
        if (strcmp(infop->fset, "default")) {
            strcat(fsPrintName, "/");
            strcat(fsPrintName, infop->fset);
        }
    } else {
        strcpy(fsPrintName, infop->blkfile);
    }

    sts = advfs_get_dmnname_params( infop->fsname, &dmnParams );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, ERRFS, "%s: unable to get info for filesystem '%s'\n"), 
                 Prog, fsPrintName);
        fprintf( stderr, catgets(catd, 1, ERRGEN, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        exit( 1 );
    } 

    if (l) {
        sts = advfs_get_lock_stats( &lockStats1 );
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, ERRLKST, "%s: unable to get lock stats; %s\n"),
                     Prog, BSERRMSG( sts ) );
            exit( 1 );
        }

    } else if (f) {
        sts = advfs_fset_get_stats( infop->fsname, infop->fset, &fsetStats1 );
        if (sts != EOK) {
            if (sts == E_NO_SUCH_BF_SET) {
                fprintf(stderr, catgets(catd, 1, ERRMNT, "%s: Filesystem %s must be mounted for filesystem stats\n"), Prog, fsPrintName);
            } else {
                fprintf( stderr, catgets(catd, 1, ERRSTAT, "%s: unable to get stats for filesystem %s: %s\n"), 
                     Prog, fsPrintName, BSERRMSG( sts ) );
            }
            exit( 1 );
        }

    } else {
        dmnStats = advfs_init_dmnstat( dmnParams.bfDomainId );
        if (dmnStats == NULL) {
            fprintf( stderr, catgets(catd, 1, ERRSTAT, "%s: unable to get stats for filesystem %s: %s\n"),
                     Prog, fsPrintName, "<unknown>");
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
                fprintf( stderr, catgets(catd, 1, ERRLKST, "%s: unable to get lock stats; %s\n"),
                         Prog, BSERRMSG( sts ) );
                exit( 1 );
            }
	    
	    if (raw) {
		/* We want the raw statistics.  Zero out lockStat1 */
		/* and that is what you will get                   */
		memset(&lockStats1, 0, sizeof(lockStats1));
	    }
            print_lock_stats( &lockStats1, &lockStats2, lLevel );
            lockStats1 = lockStats2;

        } else if (f) {
            sts = advfs_fset_get_stats( infop->fsname, infop->fset, &fsetStats2 );
            if (sts != EOK) {
                fprintf( stderr, catgets(catd, 1, ERRSTAT, "%s: unable to get stats for filesystem %s: %s\n"),
                         Prog, fsPrintName, BSERRMSG( sts ) );
                exit( 1 );
            }
	    if (raw) {
		/* We want the raw statistics.  Zero out fsetStat1 */
		/* and that is what you will get                   */
		memset(&fsetStats1, 0, sizeof(fsetStats1));
	    }

            print_fset_stats( &fsetStats1, &fsetStats2, fLevel );
            fsetStats1 = fsetStats2;

        } else if (B || v || g) {
	    
	    if (raw) {
		dmnStats2 = advfs_init_dmnstat( dmnParams.bfDomainId );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, ERRSTAT, "%s: unable to get stats for filesystem %s: %s\n"),
			    Prog, fsPrintName, "<unknown>");
		    exit( 1 );
		}
	    }
	    else
	    {
		dmnStats2 = advfs_get_dmnstat( dmnStats, 
					     dmnParams.bfDomainId, 
					     1 );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, ERRUPD, "%s: unable to update stats for filesystem '%s'\n"),
			    Prog, fsPrintName);
		    exit( 1 );
		}
	    }

            if (B) {
                print_bmt_stats( dmnStats2, bmtRead );
            } else if (v) {
                print_vol_stats( dmnStats2);
            } else {
                print_log_stats( dmnStats2);
            }

            advfs_free_dmnstat( dmnStats2 );

        } else {

	    if (raw) {
		dmnStats2 = advfs_init_dmnstat( dmnParams.bfDomainId );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, ERRSTAT, "%s: unable to get stats for filesystem %s: %s\n"),
			    Prog, fsPrintName, "<unknown>");
		    exit( 1 );
		}
	    }
	    else
	    {
		dmnStats2 = advfs_get_dmnstat( dmnStats, 
					     dmnParams.bfDomainId, 
					     1 );
		if (dmnStats2 == NULL) {
		    fprintf( stderr, catgets(catd, 1, ERRUPD, "%s: unable to update stats for filesystem '%s'\n"),
			    Prog, fsPrintName);
		    exit( 1 );
		}
	    }
	    /* No longer use the print statements inside libmsfs_stat */
	    /* instead use our newly created one                      */
            print_generic_dmnstat( dmnStats2, dmnParams.bfDomainId );
            advfs_free_dmnstat( dmnStats2 );
        }

        if (!i || (count > 0)) {
            repeat--;
        }
    }

    advfs_free_dmnstat( dmnStats );
    exit( 0 );
}
