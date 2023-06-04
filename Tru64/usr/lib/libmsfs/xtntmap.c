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
static char rcsid[] = "@(#)$RCSfile: xtntmap.c,v $ $Revision: 1.1.9.3 $ (DEC) $Date: 1999/03/12 21:56:22 $";
#endif

#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/mode.h>
#include <sys/stat.h>
#include <msfs/bs_error.h>
#include <msfs/bs_public.h>         /* for bs_ods.h */
#include <msfs/ms_generic_locks.h>  /* for ftx_public.h */
#include <msfs/ftx_public.h>        /* for bs_ods.h */
#include <msfs/bs_ods.h>            /* for XTNT_TERM  & PERM_HOLE_START */
#include <msfs/msfs_syscalls.h>

typedef struct xtntMap {
    struct xtntMap *nextMap;
    int xtntMapNum;
    int curXtnt;
    int xtntCnt;
    mlExtentDescT *xtnts;
} xtntMapT;

typedef struct xtntMapDesc {
    int fd;
    struct stat stats;
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    struct xtntMap *nextMap;
} xtntMapDescT;


int
advfs_xtntmap_next_page( 
    void *xtntMapD,
    int curPg,
    int *nextPg
    )
{
    xtntMapDescT *xmd = xtntMapD;
    xtntMapT *xm = xmd->nextMap;
    long bytes;

    *nextPg = curPg + 1; /* let's be optimistic! */

    if (*nextPg == xmd->bfInfo.nextPage) {
        goto __frag;
    }

    if (*nextPg > xmd->bfInfo.nextPage) {
        return -1;
    }

    if (*nextPg < xm->xtnts[ xm->curXtnt ].bfPage) {
        xm->curXtnt = 0;
    }

    if (*nextPg < xm->xtnts[ 0 ].bfPage) {
        *nextPg = xm->xtnts[ 1 ].bfPage;
        return 0;
    }

    while (1) {

        if ((*nextPg >= xm->xtnts[ xm->curXtnt ].bfPage) &&
            (*nextPg < (xm->xtnts[ xm->curXtnt ].bfPage + 
                        xm->xtnts[ xm->curXtnt ].bfPageCnt))) {

            if ( xm->xtnts[xm->curXtnt].volBlk == XTNT_TERM ||
                 xm->xtnts[xm->curXtnt].volBlk == PERM_HOLE_START )
            {

                /* Version 3 domains can have adjacent holes. */
                while ( xm->curXtnt < xm->xtntCnt -1 &&
                        (xm->xtnts[xm->curXtnt + 1].volBlk == XTNT_TERM ||
                         xm->xtnts[xm->curXtnt + 1].volBlk == PERM_HOLE_START) )
                {
                    xm->curXtnt++;
                }

                *nextPg = xm->xtnts[ xm->curXtnt ].bfPage + 
                          xm->xtnts[ xm->curXtnt ].bfPageCnt;
            }

            return 0;
        }

        xm->curXtnt++;

        if (xm->curXtnt >= xm->xtntCnt) {
            xm->curXtnt = 0;
            goto __frag;
        }
    }

    return 0;

__frag:

    bytes = (long) xmd->bfInfo.nextPage * (long) xmd->bfInfo.pageSize * 512L;

    if (xmd->stats.st_size > bytes) {
        *nextPg = xmd->bfInfo.nextPage;
        return 0;
    }

    return -1;
}

int
advfs_xtntmap_next_byte( 
    void *xtntMapD,
    long curByte,
    long *nextByte
    )
{
    xtntMapDescT *xmd = xtntMapD;
    int nextPg;
    int result;

    if (curByte < 0) {
        result = advfs_xtntmap_next_page( 
                                      xtntMapD, 
                                      -1, 
                                      &nextPg );
        if (result < 0) {
            return result;
        }

        *nextByte = (long) nextPg * (long) xmd->bfInfo.pageSize * 512L;

    } else {

        int curPg = curByte / ((long) xmd->bfInfo.pageSize * 512L);

        if ((curByte + 1L) >= xmd->stats.st_size) {
            return -1;
        }

        if (((curByte + 1L) % ((long) xmd->bfInfo.pageSize * 512L)) == 0) {

            result = advfs_xtntmap_next_page( xtntMapD, curPg, &nextPg );
            if (result < 0) {
                return result;
            }

            *nextByte = (long) nextPg * (long) xmd->bfInfo.pageSize * 512L;

        } else {

            result = advfs_xtntmap_next_page( xtntMapD, curPg - 1, &nextPg );
            if (result < 0) {
                return result;
            }
    
            if (curPg == nextPg) {
                *nextByte = curByte + 1L;
            } else {
                *nextByte = (long) nextPg * (long) xmd->bfInfo.pageSize * 512L;
            }
        }
    }

    return 0;
}

void
advfs_xtntmap_close( 
    void *xtntMapD
    )
{
    xtntMapDescT *xmd = xtntMapD;
    xtntMapT *xm = NULL, *nextXm = NULL;

    if (xmd != NULL) {

        xm = xmd->nextMap;
    
        while (xm != NULL) {
            if (xm->xtnts != NULL) {
                free( xm->xtnts );
            }
    
            nextXm = xm->nextMap;
            free( xm );
            xm = nextXm;
        }

        free( xmd );
    }
}

void *
advfs_xtntmap_open( 
    int fd,
    mlStatusT *status,
    int xtnt_use
    )
{
    xtntMapDescT *xmd = NULL;
    xtntMapT *xm = NULL, *nextXm = NULL;
    u32T volIndex;    
    int xtntCnt;
    int pageCnt;
    mlStatusT sts;
    mlExtentDescT fakeXtnts;
    int error;

    *status = EOK;

    xmd = (xtntMapDescT *) malloc( sizeof( xtntMapDescT ) );
    if (xmd == NULL) {
        *status = ENO_MORE_MEMORY;
        goto __error;
    }

    bzero( xmd, sizeof( xtntMapDescT ) );

    xmd->fd = fd;

    error = fstat( xmd->fd, &xmd->stats );
    if (error< 0) {
        *status = error;
        goto __error;
    }

    sts = advfs_get_bf_params( fd, &xmd->bfAttr, &xmd->bfInfo );  
    if (sts != EOK) {
        *status = sts;
        goto __error;
    }

    /*
     * If xtnt_use is not set to one, then just get the extent map for the
     * file. This is the default behavior.
     * If other options for xtnt_use are added in the future, this should
     * be changed to xtnt_use == 0, and values greater than 1 used to
     * represent the future options.
     */

    if (xtnt_use != 1) {

        /* get the total number of extents */

        sts = msfs_get_bf_xtnt_map (fd, 1, 0, 0, &fakeXtnts, &xtntCnt, &volIndex);

    } else {

        /* first get the xtntCnt so we can allocate
        *  the total number of extents for the merged extent map
        *  correct during the next call to advfs_get_bkup_xtnt_map
        */

        sts = advfs_get_bkup_xtnt_map (fd, 1, 0, 0, &fakeXtnts, 
				      &xtntCnt, &volIndex, &pageCnt);
    }

    if (sts != EOK) {
        *status = sts;
        goto __error;
    }

    xm = (xtntMapT *) malloc( sizeof( xtntMapT ) );
    if (xm == NULL) {
        *status = ENO_MORE_MEMORY;
        goto __error;
    }

    bzero( xm, sizeof( xtntMapT ) );

    xmd->nextMap = xm;

    xm->xtnts = (mlExtentDescT *) malloc( xtntCnt * sizeof( mlExtentDescT ) );
    if (xm->xtnts == NULL) {
        /* todo - xm->xtnts should be a cache, not the whole map */
        *status = ENO_MORE_MEMORY;
        goto __error;
    }

    if (xtnt_use != 1) {

    /* now that we have the memory for it, get the extent map */

        sts = msfs_get_bf_xtnt_map( fd, 1, 0, xtntCnt, 
                                xm->xtnts, &xtntCnt, &volIndex);

    } else {

        /* check to make sure the maps have not grown since called above */

        do {

            sts = advfs_get_bkup_xtnt_map( fd, 1, 0, xtntCnt, 
                                xm->xtnts, &xtntCnt, &volIndex, &pageCnt);
            if(sts == E_NOT_ENOUGH_XTNTS) {

                xm->xtnts = (mlExtentDescT *) realloc(xm->xtnts, 
                                    xtntCnt * sizeof( mlExtentDescT ) );
        	if (xm->xtnts == NULL) {
        	    *status = ENO_MORE_MEMORY;
        	    goto __error;
                }
            }
        } while(sts == E_NOT_ENOUGH_XTNTS); 

        /* reset the pageCnt for clones */
        if (pageCnt > 0) xmd->bfInfo.nextPage = pageCnt;

    } /* end else if */

    /* reset the xtntCnt to the actual xtntCnt, overwrite the estimate */
    xm->xtntCnt = xtntCnt;

    if (sts != EOK) {
        *status = sts;
        goto __error;
    }

    return xmd;

__error:

    advfs_xtntmap_close( (void *) xmd );

    return NULL;
}

