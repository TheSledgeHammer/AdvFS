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
 * @(#)$RCSfile: bs_migrate.h,v $ $Revision: 1.1.30.1 $ (DEC) $Date: 2001/12/14 16:50:52 $
 */

#ifndef _BS_MIGRATE_
#define _BS_MIGRATE_

#define BLKDESC_CNT 2	/* Currently a page is never mapped to more than
                           two disk locations.  If we ever support 
                           migration to multiple locations, this would have
                           to be changed. */

statusT
bs_move_metadata (
                  bfAccessT *bfap,/* in */
                  vdT       *vdp  /* in or NULL */
                  );

void
mig_register_migrate_agent ();

statusT
mig_migrate (
             bfAccessT *bfAccess,  /* in */
             vdIndexT  srcVdIndex,  /* in */
             uint32T  srcPageOffset,  /* in */
             uint32T  srcPageCnt,  /* in */
             vdIndexT  dstVdIndex,  /* in */
             uint64T  dstBlkOffset,  /* in */
             uint32T  forceFlag,     /* in */
             bsAllocHintT alloc_hint /* in */
             );

#endif  /* _BS_MIGRATE_ */
