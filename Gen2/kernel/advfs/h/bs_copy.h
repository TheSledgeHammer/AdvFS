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
/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 *
 * Facility:
 *
 *   AdvFS 
 *
 * Abstract:
 *
 *   This header file contains structure definitions and function
 *   prototypes related to bitfile page copy.
 *
 */

#ifndef _BS_COPY_
#define _BS_COPY_

#include <advfs/bs_extents.h>


statusT
cp_copy_page_range (
                    bfAccessT *bfAccess,  /* in */
                     extent_blk_desc_t *bfPageRange,  /* in */
                    uint32_t bfPageRangeCnt,   /* in */
                    bsInMemXtntT *copyXtnts,  /* in */
                    uint32_t forceFlag  /* in */
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

