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
#include <sys/types.h>
#include <mntent.h>
#include <advfs/advfs_syscalls.h>
#include <errno.h>

#define MNTTYPE_ADVFS "advfs"

/*
 * advfs_init_dmnstat
 *
 * Return a pointer to a struct containing pointers to all
 * domain statistics.  Space is allocated in this routine.
 * Return NULL if an error.
 * Caller should eventually free the struct (by calling
 * bs_free_dmnstat)
 */
static adv_dmn_stats_t *advfs_init_dmnstat_int(bfDomainIdT  ,int);

adv_dmn_stats_t *
advfs_init_dmnstat( bfDomainIdT   dmnid )
{
    return(advfs_init_dmnstat_int( dmnid, 0 ));
}

static adv_dmn_stats_t *
advfs_init_dmnstat_int(
    bfDomainIdT   dmnid,
    int flag                /* if set, use advfs_get_vol_params2 */
    )
{
    adv_vol_info_t volInfo;
    adv_vol_prop_t volProp;
    adv_vol_ioq_params_t volIoQP;
    int vols;
    uint32_t vol;
    adv_status_t sts;
    int i, volError;
    adv_dmn_stats_t *dmnStat;

    if( ( dmnid.id_sec == 0 ) && ( dmnid.id_usec == 0 ) ){
        return( NULL );
    }

    dmnStat = (adv_dmn_stats_t *)malloc( sizeof( adv_dmn_stats_t ) );

    if( dmnStat == NULL ) {
        errno = ENOMEM;
        return( NULL );
    }
        
    dmnStat->dmnParams = (adv_bf_dmn_params_t *)malloc( sizeof( adv_bf_dmn_params_t ) );

    if( dmnStat->dmnParams == NULL ) {
        errno = ENOMEM;
        free( dmnStat );
        return( NULL );
    }
        
    sts = advfs_get_dmn_params( dmnid, dmnStat->dmnParams );

    if (sts != EOK) {
#ifdef ADVFS_DEBUG
        printf( "*get dmn params error %s*\n", BSERRMSG( sts ) );
#endif
	errno = sts;
        free( dmnStat->dmnParams );
        free( dmnStat );
        return( NULL );
    } 

    dmnStat->volIndex = (uint32_t*)malloc( dmnStat->dmnParams->curNumVols * 
                                    sizeof(uint32_t) );
    if( dmnStat->volIndex == NULL ) {
        errno = ENOMEM;
        free( dmnStat->dmnParams );
        free( dmnStat );
        return( NULL );
    }

    sts = advfs_get_dmn_vol_list( dmnid, 
                                 dmnStat->dmnParams->curNumVols, 
                                 dmnStat->volIndex, 
                                 &vols );
    if (sts != EOK) {
#ifdef ADVFS_DEBUG      
        printf( "*get dmn vol list error %s*\n", BSERRMSG( sts ) );
#endif
	errno = sts;
        free( dmnStat->dmnParams );
        free( dmnStat->volIndex );
        free( dmnStat );
        return( NULL );
    }

    dmnStat->volCounters = 
             (adv_vol_counters_t *)malloc( vols * sizeof( adv_vol_counters_t ) );

    if( dmnStat->volCounters == NULL ) {
        errno = ENOMEM;
        free( dmnStat->dmnParams );
        free( dmnStat->volIndex );
        free( dmnStat );
        return( NULL );
    }

    for( i = 0, volError = FALSE; i < vols; i++ ) {

        vol = dmnStat->volIndex[i];
        if (flag)
            sts = advfs_get_vol_params2( dmnid, vol, &volInfo, &volProp, &volIoQP,
                                       &dmnStat->volCounters[i], 0 );
        else
            sts = advfs_get_vol_params( dmnid, vol, &volInfo, &volProp, &volIoQP,
                                      &dmnStat->volCounters[i] );

        if (sts != EOK) {
            if (sts != EBAD_VDI) {
                errno = sts;
                volError = TRUE;
            } else {
                /* Try again to get info which doesn't require reading
                   the bmt.  If the vol is being removed (so the bmt is
                   unavailable), the service class will be nil.
                */
                if (flag)
                    sts = advfs_get_vol_params2( dmnid, vol, &volInfo, &volProp, &volIoQP,
                                               &dmnStat->volCounters[i], 0 );
                else
                    sts = advfs_get_vol_params( dmnid, vol, &volInfo, &volProp, &volIoQP,
                                              &dmnStat->volCounters[i] );
                if (!(sts == EOK && volProp.serviceClass == 0) && (sts != EBAD_VDI)) {
#ifdef ADVFS_DEBUG
                    printf( "*get vol params error %s;*\n", BSERRMSG( sts ) );
#endif
                    errno = sts;
                    volError = TRUE;
                }
            }
        } /* end not eok */
    } /* end for */

    if (volError) {
        free( dmnStat->dmnParams );
        free( dmnStat->volIndex );
        free( dmnStat );
        return( NULL );
    }

    return( dmnStat );
}

/*
 * advfs_init_dmnstat_list
 *
 * Init domain stat structures for all mounted
 * domains indicated in the domain stat handle struct.
 * Return -1 if an error occurs, zero otherwise.
 */

int
advfs_init_dmnstat_list( dmnStatHT *dmnSt )
{
    int i;

    if( dmnSt == NULL ) {
        return( 0 );
    }

    for( i = 0; i < dmnSt->len; i++ ) {
        dmnSt->dmnInitStp[i] = advfs_init_dmnstat( dmnSt->dmnIdp[i] );
        if( dmnSt->dmnInitStp[i] == NULL ) {
            return( -1 );
        }
    }
    return( 0 );
}


/*
 * advfs_get_dmnstat()
 *
 * Return a pointer to a domain stat struct given the 
 * domain stat struct with the initial values.  Allocate
 * a struct, get the current stats, subtract the values
 * from the initial domain stat struct to return the 
 * differences.  Return NULL upon error.
 *
 * If refresh == 1, the initial values get updated to
 * the current values, otherwise the initial values remain
 * the same.  The code would be more reasonable if we can
 * eventually remove the refresh flag and do things one way
 * or the other.
 */
static adv_dmn_stats_t *advfs_get_dmnstat_int();

adv_dmn_stats_t *
advfs_get_dmnstat(
    adv_dmn_stats_t *initStat,       /* in (out too, depending on refresh) */
    bfDomainIdT   dmnid,        /* in */
    int refresh                 /* in */
    )
{
    return (advfs_get_dmnstat_int ( initStat, dmnid, refresh, 0 ));
}

adv_dmn_stats_t *
advfs_get_dmnstat2(
    adv_dmn_stats_t *initStat,       /* in (out too, depending on refresh) */
    bfDomainIdT   dmnid,        /* in */
    int refresh                 /* in */
    )
{
    return (advfs_get_dmnstat_int ( initStat, dmnid, refresh, 1 ));
}

static adv_dmn_stats_t *
advfs_get_dmnstat_int(
    adv_dmn_stats_t *initStat,       /* in (out too, depending on refresh) */
    bfDomainIdT   dmnid,        /* in */
    int refresh,                /* in */
    int flag                    /* in (0: snapshot, 1: cumulativel) */
    )
{
    adv_dmn_stats_t *dmnStat;
    adv_dmn_stats_t *tmpStat;
    bcStatT *initBcStatp;
    bcStatT *newBcStatp;
    bmtStatT *initBmtStatp;
    bmtStatT *newBmtStatp;
    int i;
    int vols;

    if( ( dmnid.id_sec == 0 ) && ( dmnid.id_usec == 0 ) ){
        return( NULL );
    }
    if( initStat == NULL ) {
        return( NULL );
    }

    /*
     * Get current domain stats
     */
    dmnStat = advfs_init_dmnstat_int( dmnid, flag );

    if( dmnStat == NULL ) {
        return( NULL );
    }

    if( refresh ) {
        /*
         * Allocate a new struct to save the
         * current values.
         */
        tmpStat = calloc(1, sizeof( adv_dmn_stats_t ) );

        if( tmpStat == NULL ) {
	    errno = ENOMEM;
            free(dmnStat);
            return( NULL );
        }

        tmpStat->dmnParams = (adv_bf_dmn_params_t *)malloc( sizeof( adv_bf_dmn_params_t ) );

        vols = dmnStat->dmnParams->curNumVols;

        tmpStat->volIndex = (uint32_t*)malloc( vols * sizeof(uint32_t) );
        tmpStat->volCounters = (adv_vol_counters_t *)malloc( vols * 
            sizeof( adv_vol_counters_t ) );

        if( tmpStat->dmnParams == NULL || 
            tmpStat->volIndex == NULL ||
            tmpStat->volCounters == NULL ) {
                
	    errno = ENOMEM;
            if (tmpStat->dmnParams != NULL) {
                free(tmpStat->dmnParams);
            }

            if (tmpStat->volIndex != NULL) {
                free(tmpStat->volIndex);
            }

            if (tmpStat->volCounters != NULL) {
                free(tmpStat->volCounters);
            }

            free(dmnStat);
            return( NULL );
        }

        /*
         * Copy the current values to this struct.
         */
        *tmpStat->dmnParams = *dmnStat->dmnParams;  /* struct copy */

        for( i = 0; i < vols; i++ ) {
            tmpStat->volIndex[i] = dmnStat->volIndex[i];
            tmpStat->volCounters[i] = dmnStat->volCounters[i]; /* struct copy */
        }
    }

    /* 
     * Compute the diffs between the initial
     * stats and the current stats
     */
    newBcStatp = &dmnStat->dmnParams->bcStat;
    initBcStatp = &initStat->dmnParams->bcStat;
    newBmtStatp = &dmnStat->dmnParams->bmtStat;
    initBmtStatp = &initStat->dmnParams->bmtStat;

    newBcStatp->unpinCnt.lazy -= initBcStatp->unpinCnt.lazy;
    newBcStatp->unpinCnt.blocking -= initBcStatp->unpinCnt.blocking;
    newBcStatp->unpinCnt.clean -= initBcStatp->unpinCnt.clean;
    newBcStatp->unpinCnt.log -= initBcStatp->unpinCnt.log;

    newBmtStatp->fStatRead -= initBmtStatp->fStatRead;
    newBmtStatp->fStatWrite -= initBmtStatp->fStatWrite;

    for( i = 0; i < ML_BSR_MAX; i++ ) {
        newBmtStatp->bmtRecRead[i] -= initBmtStatp->bmtRecRead[i];
        newBmtStatp->bmtRecWrite[i] -= initBmtStatp->bmtRecWrite[i];
    }
    
    for( i = 0; i < dmnStat->dmnParams->curNumVols; i++ ) {

        adv_vol_counters_t *newVolCountersp;
        adv_vol_counters_t *initVolCountersp;

        newVolCountersp = &dmnStat->volCounters[i];
        initVolCountersp = &initStat->volCounters[i];

        newVolCountersp->nread -= initVolCountersp->nread;
        newVolCountersp->nwrite -= initVolCountersp->nwrite;
        newVolCountersp->readblk -= initVolCountersp->readblk;
        newVolCountersp->writeblk -= initVolCountersp->writeblk;
    }

    /*
     * Update the fields of initStat if necessary.
     */
    if( refresh ) {
        free( initStat->dmnParams );
        free( initStat->volIndex );
        free( initStat->volCounters );
        initStat->dmnParams = tmpStat->dmnParams;
        initStat->volIndex = tmpStat->volIndex;
        initStat->volCounters = tmpStat->volCounters;
        free( tmpStat );
    }
    return( dmnStat );
}

/*
 * advfs_get_dmnstat_list
 *
 * Given a domain stat handle, update all
 * current domain stat structures.
 * Return -1 if an error occurs, zero otherwise.
 */

int
advfs_get_dmnstat_list( 
    dmnStatHT *dmnSt,           /* in/out */
    int refresh )               /* in */
{
    int i;

    if( dmnSt == NULL ) {
        return( -1 );
    }

    for( i = 0; i < dmnSt->len; i++ ) {

        dmnSt->dmnStp[i] = advfs_get_dmnstat( dmnSt->dmnInitStp[i], 
                                        dmnSt->dmnIdp[i],
                                        refresh );

        if( dmnSt->dmnStp[i] == NULL ) {
            return( -1 );
        }
    }
    return( 0 );
}

/*
 * advfs_free_dmnstat()
 *
 * Free the data areas contained in the
 * adv_dmn_stats_t struct.
 */

void
advfs_free_dmnstat(
    adv_dmn_stats_t *dmnStat     /* in */
    )
{
    if( dmnStat != NULL ) {
        free( dmnStat->dmnParams );
        free( dmnStat->volIndex );
        free( dmnStat->volCounters );
        free( dmnStat );
    }
}


/*
 * print_dmnstat() 
 *
 * Get the current domain stats, print them out,
 * free the stat struct.
 * Set the refresh flag when calling bs_get_dmnstat
 * so that the initStat struct is updated to the 
 * current stats.
 */

void
print_dmnstat( 
    adv_dmn_stats_t *initStat,       /* in */
    bfDomainIdT   dmnid         /* in */
    )
{
    adv_dmn_stats_t *dmnStat;
    int i;
    unsigned unpin;

    if( ( dmnid.id_sec == 0 ) && ( dmnid.id_usec == 0 ) ){
        return;
    }
    if( initStat == NULL ) {
        return;
    }

    dmnStat = advfs_get_dmnstat( initStat, dmnid, 1 );

    if( dmnStat == NULL ) {
        printf("dmnStat not available\n");
        return;
    }

    /* 
     * Print stats
     */
    printf("\n");
    printf("Filesystem -%d.%d- Stats -\n", dmnid.id_sec, dmnid.id_usec);
    printf("\n");

    printf("%-10s%-9d%\n",
        "Deref",
        dmnStat->dmnParams->bcStat.refCnt);

    unpin = dmnStat->dmnParams->bcStat.unpinCnt.blocking +
        dmnStat->dmnParams->bcStat.unpinCnt.lazy +
        dmnStat->dmnParams->bcStat.unpinCnt.log +
        dmnStat->dmnParams->bcStat.unpinCnt.clean;

    printf("%-10s%-9d%\n",
        "Unpin",
        unpin);

    printf("%-10s%-9d%-10s%-9d%-14s%-7d%-14s%-7d\n",
        "Lazy",
        dmnStat->dmnParams->bcStat.unpinCnt.lazy,
        "Log",
        dmnStat->dmnParams->bcStat.unpinCnt.log,
        "Blocking",
        dmnStat->dmnParams->bcStat.unpinCnt.blocking,
        "Clean",
        dmnStat->dmnParams->bcStat.unpinCnt.clean );

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
            
        printf("%4d %10d %10d %10d %10d %10d %10d\n",
            dmnStat->volIndex[i],
            dmnStat->volCounters[i].nread,
            dmnStat->volCounters[i].nwrite
            );
    }
    printf("\n");
    advfs_free_dmnstat( dmnStat );
}

void
advfs_print_dmnstat( 
    dmnStatHT *dmnSt,           /* in */
    bfDomainIdT   dmnid )       /* in */
{
    int i;

    if( dmnSt == NULL ) {
        return;
    }

    /*
     * If we were given a specific handle, print
     * stats for only that domain.
     */

    if( !(dmnid.id_sec == 0 && dmnid.id_usec == 0) ) {
        
        /*
         * We need to find the initialized domain
         * data struct for the given handle, first
         * locate the handle.
         */

        for( i = 0; i < dmnSt->len; i++ ) {
            if( dmnSt->dmnIdp[i].id_sec == dmnid.id_sec  &&
                dmnSt->dmnIdp[i].id_usec == dmnid.id_usec ){
                break;
            }
        }

        /*
         * If we found the handle, print the domain
         * info.
         */

        if( dmnSt->dmnIdp[i].id_sec == dmnid.id_sec  &&
            dmnSt->dmnIdp[i].id_usec == dmnid.id_usec ){
            print_dmnstat( dmnSt->dmnInitStp[i], dmnid );
        }
        return;
    }

    /*
     * Print stats for all domains we know about.
     */

    for( i = 0; i < dmnSt->len; i++ ) {
        
        print_dmnstat(dmnSt->dmnInitStp[i], dmnSt->dmnIdp[i] );
    }
}

/*
 * advfs_end_dmnstat()
 *
 * Deallocate the domain stat handle arrays when
 * finished with domain stats.
 */

dmnStatHT *
advfs_end_dmnstat( dmnStatHT *dmnSp )
{
    int i;

    if( dmnSp == NULL ) {
        return( NULL );
    }

    for( i = 0; i < dmnSp->len; i++ ) {
        if( dmnSp->dmnInitStp[i] != NULL ) {
            advfs_free_dmnstat( dmnSp->dmnInitStp[i] );
        }
        if( dmnSp->dmnStp[i] != NULL ) {
            advfs_free_dmnstat( dmnSp->dmnStp[i] );
        }
    }
    free( dmnSp->dmnIdp );
    free( dmnSp->dmnInitStp );
    free( dmnSp->dmnStp );
    free( dmnSp );

    return( NULL );
}
