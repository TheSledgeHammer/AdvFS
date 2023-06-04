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
/*
 * HISTORY
 */
#ifndef lint
#endif

#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <advfs/bs_error.h>
#include <advfs/bs_public.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/ms_generic_locks.h>  /* for ftx_public.h */
#include <advfs/ftx_public.h>        /* for bs_ods.h */
#include <advfs/bs_ods.h>            /* for XTNT_TERM  & PERM_HOLE_START */


typedef struct xtntMap {
    struct xtntMap *nextMap;        /* currently not used */
    int xtntMapNum;                 /* currently not used */
    int curXtnt;                    /* current xtnt in xtnts array */
    int xtntCnt;                    /* number of xtnts in xtnts array */
    bsExtentDescT *xtnts;           /* xtnts array */
} xtntMapT;

typedef struct xtntMapDesc {
    int fd;
    struct stat stats;
    adv_bf_attr_t bfAttr;
    adv_bf_info_t bfInfo;
    struct xtntMap *nextMap;
} xtntMapDescT;


int
advfs_xtntmap_next_fob( 
    void *xtntMapD,
    bf_fob_t curFob,
    bf_fob_t *nextFob
    )
{
    xtntMapDescT *xmd = xtntMapD;
    xtntMapT *xm = xmd->nextMap;

    *nextFob = curFob + 1; /* let's be optimistic! */

    if (*nextFob >= xmd->bfInfo.mbfNextFob) {
        return -1;
    }

    if (*nextFob < xm->xtnts[ xm->curXtnt ].bsed_fob_offset) {
        xm->curXtnt = 0;
    }

    if (*nextFob < xm->xtnts[ 0 ].bsed_fob_offset) {
        *nextFob = xm->xtnts[ 1 ].bsed_fob_offset;
        return 0;
    }

    while (1) {

        if ((*nextFob >= xm->xtnts[ xm->curXtnt ].bsed_fob_offset) &&
            (*nextFob < (xm->xtnts[ xm->curXtnt ].bsed_fob_offset + 
                        xm->xtnts[ xm->curXtnt ].bsed_fob_cnt))) {

            if ( xm->xtnts[xm->curXtnt].bsed_vd_blk == XTNT_TERM ||
                 xm->xtnts[xm->curXtnt].bsed_vd_blk == COWED_HOLE)
            {

                /* Version 3 domains can have adjacent holes. */
                while ( xm->curXtnt < xm->xtntCnt -1 &&
                        (xm->xtnts[xm->curXtnt + 1].bsed_vd_blk == XTNT_TERM ||
                         xm->xtnts[xm->curXtnt + 1].bsed_vd_blk == COWED_HOLE) )
                {
                    xm->curXtnt++;
                }

                *nextFob = xm->xtnts[ xm->curXtnt ].bsed_fob_offset + 
                          xm->xtnts[ xm->curXtnt ].bsed_fob_cnt;
            }

            return 0;
        }

        xm->curXtnt++;

        if (xm->curXtnt >= xm->xtntCnt) {
            xm->curXtnt = 0;
            return -1;
        }
    }

    return 0;
}

int
advfs_xtntmap_next_byte( 
    void *xtntMapD,
    off_t curByte,
    off_t *nextByte
    )
{
    xtntMapDescT *xmd = xtntMapD;
    bf_fob_t next_fob;
    int result;

    if (curByte < 0) {
        result = advfs_xtntmap_next_fob( 
                                      xtntMapD, 
                                      -1, 
                                      &next_fob );
        if (result < 0) {
            return result;
        }

        *nextByte = (off_t) next_fob * (off_t) ADVFS_FOB_SZ;

    } else {

        bf_fob_t cur_fob = curByte / ((off_t) ADVFS_FOB_SZ);

        if ((curByte + 1) >= xmd->stats.st_size) {
            return -1;
        }

        if (((curByte + 1) % ((off_t) ADVFS_FOB_SZ)) == 0) {

            result = advfs_xtntmap_next_fob( xtntMapD, cur_fob, &next_fob );
            if (result < 0) {
                return result;
            }

            *nextByte = (off_t) next_fob * (off_t) ADVFS_FOB_SZ;

        } else {

            result = advfs_xtntmap_next_fob( xtntMapD, cur_fob - 1, &next_fob );
            if (result < 0) {
                return result;
            }
    
            if (cur_fob == next_fob) {
                *nextByte = curByte + 1;
            } else {
                *nextByte = (off_t) next_fob * (off_t) ADVFS_FOB_SZ;
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
    adv_status_t *status,
    int xtnt_use
    )
{
    xtntMapDescT *xmd = NULL;
    xtntMapT *xm = NULL, *nextXm = NULL;
    vdIndexT volIndex;    
    int xtntCnt;
    bf_fob_t fobCnt;
    adv_status_t sts;
    bsExtentDescT fakeXtnts;
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

        sts = advfs_get_bf_xtnt_map (fd, 1, 0, 0, &fakeXtnts, &xtntCnt, &volIndex);

    } else {

        /* first get the xtntCnt so we can allocate
        *  the total number of extents for the merged extent map
        *  correct during the next call to advfs_get_bkup_xtnt_map
        */

        sts = advfs_get_bkup_xtnt_map (fd, 1, 0, 0, &fakeXtnts, 
				      &xtntCnt, &volIndex, &fobCnt);
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

    xm->xtnts = (bsExtentDescT *) malloc( xtntCnt * sizeof( bsExtentDescT ) );
    if (xm->xtnts == NULL) {
        /* todo - xm->xtnts should be a cache, not the whole map */
        *status = ENO_MORE_MEMORY;
        goto __error;
    }

    if (xtnt_use != 1) {

    /* now that we have the memory for it, get the extent map */

        sts = advfs_get_bf_xtnt_map( fd, 1, 0, xtntCnt, 
                                xm->xtnts, &xtntCnt, &volIndex);

    } else {

        /* check to make sure the maps have not grown since called above */

        do {

            sts = advfs_get_bkup_xtnt_map( fd, 1, 0, xtntCnt, 
                                xm->xtnts, &xtntCnt, &volIndex, &fobCnt);
            if(sts == E_NOT_ENOUGH_XTNTS) {

                xm->xtnts = (bsExtentDescT *) realloc(xm->xtnts, 
                                    xtntCnt * sizeof( bsExtentDescT ) );
        	if (xm->xtnts == NULL) {
        	    *status = ENO_MORE_MEMORY;
        	    goto __error;
                }
            }
        } while(sts == E_NOT_ENOUGH_XTNTS); 

        /* reset the fobCnt for clones */
        if (fobCnt > 0) xmd->bfInfo.mbfNextFob = fobCnt;

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
