/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
 */
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
 * @(#)$RCSfile: bs_copy.h,v $ $Revision: 1.1.27.1 $ (DEC) $Date: 2003/04/23 19:41:56 $
 */

#ifndef _BS_COPY_
#define _BS_COPY_

#define STG 0
#define MAX_COPY_XFER_SIZE 65535    /* max size is 512 MB - 512 bytes */ 

typedef struct pageRange {
    uint32T  pageOffset;
    uint32T  pageCnt;
    int32T  pageType;  /* STG, XTNT_TERM, or PERM_HOLE_START */
} pageRangeT;

statusT
cp_copy_page_range (
                    bfAccessT *bfAccess,      /* in */
                    pageRangeT *bfPageRange,  /* in */
                    uint32T bfPageRangeCnt,   /* in */
                    bsInMemXtntT *copyXtnts,  /* in */
                    uint32T copyXferSize,     /* in */
                    uint32T forceFlag         /* in */
                    );

void
cp_insert_onto_xtnt_map_list (
                              bsInMemXtntMapT **xtntMapListhead,  /* in */
                              bsInMemXtntMapT *targetXtntMap  /* in */
                              );

statusT
cp_remove_from_xtnt_map_list (
                              bsInMemXtntMapT **xtntMapListhead,  /* in */
                              bsInMemXtntMapT *targetXtntMap  /* in */
                              );

#endif  /* _BS_COPY_ */
