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
 * @(#)$RCSfile: bs_extents.h,v $ $Revision: 1.1.58.1 $ (DEC) $Date: 2004/09/15 15:48:50 $
 */

#ifndef _BS_EXTENTS_H_
#define _BS_EXTENTS_H_

statusT
update_mcell_cnt (
                  domainT *domain,  /* in */
                  bfTagT bfTag, /* in */
                  vdIndexT bfVdIndex,  /* in */
                  bfMCIdT bfMcellId,  /* in */
                  uint32T type,  /* in */
                  int32T mcellCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  );

statusT
odm_remove_mcells_from_xtnt_map (
                                 domainT *domain,  /* in */
                                 bfTagT bfTag, /* in */
                                 bsInMemXtntMapT *xtntMap,  /* in */
                                 uint32T start_index,
                                 ftxHT parentFtx  /* in */
                                 );

statusT
odm_create_xtnt_map (
                     bfAccessT *bfAccess,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bfTagT bfTag,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     ftxHT parentFtx,  /* in */
                     vdIndexT *bfVdIndex,  /* out */
                     bfMCIdT *bfMcellId  /* out */
                     );

statusT x_create_inmem_xtnt_map( bfAccessT*, bsMCT*);

statusT
x_load_inmem_xtnt_map (
                       bfAccessT *bfap,     /* in, modified */
                       uint32T lock_request /* in */
                       );

/* Possible values to pass for lock_request to x_load_inmem_xtnt_map(). */
#define X_LOAD_REFERENCE   1
#define X_LOAD_UPDATE      2
#define X_LOAD_LOCKSOWNED  3

statusT
x_create_shadow_rec (
                     bfAccessT *bfAccess,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     vdIndexT prevVdIndex,  /* in */
                     bfMCIdT prevMcellId,  /* in */
                     ftxHT parentFtxH, /* in */
                     int striping_file /* in */
                     );

statusT
x_detach_extent_chain (
                     bfAccessT *bfAccess,       /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     ftxHT parentFtxH,          /* in */
                     vdIndexT *retPrevVdIndex,  /* out */
                     bfMCIdT *retPrevMcellId,   /* out */
                     vdIndexT *retfreedVdIndex, /* out */
                     bfMCIdT *retfreedMcellId   /* out */
                     );

statusT
odm_rewrite_xtnt_map (
                      bfAccessT* bfap,   /* in */
                      int xtntMapIndex,  /* in */
                      ftxHT parentFtxH,  /* in */
                      long xid           /* in */
                      );

statusT
x_update_ondisk_xtnt_map (
                          domainT *domain,  /* in */
                          bfAccessT *bfAccess,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          ftxHT parentFtx  /* in */
                          );

statusT
odm_create_xtnt_rec (
                     bfAccessT *bfap,  /* in */
                     vdIndexT allocVdIndex,  /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     int xferFlg,  /* in */
                     ftxHT parentFtx  /* in */
                     );

statusT
x_page_to_iolist (
                  bfAccessT *bfAccessp,
                  uint32T pageOffset,
                  ioListT *ioList
                  );

statusT
x_page_to_blkmap (
                  bfAccessT *bfAccess,
                  uint32T pageOffset,  /* in */
                  blkMapT *blkMap     /* in */
                  );

statusT
x_copypage_to_blkmap (
                      bfAccessT *bfap,      /* in */
                      bsInMemXtntT *xtnts,  /* in */
                      uint32T pageOffset,   /* in */
                      blkMapT *blkMap       /* in */
                      );

statusT
x_page_to_blk (
               bfAccessT *bfap,            /* in */
               uint32T pageOffset,         /* in */
               bsInMemXtntMapT *xtntMap,   /* in */
               blkDescT *blkDesc           /* in */
               );

statusT
x_update_nwr(
             bfAccessT* bfap,   /* in - bitfile access struct */
             ftxHT ftx          /* in - ftx handle */
             );

statusT
create_xtnt_map_hdr (
                     bfAccessT *bfAccess,  /* in */
                     vdIndexT firstVdIndex,  /* in */
                     bfMCIdT firstMcellId,  /* in */
                     vdIndexT lastVdIndex,  /* in */
                     bfMCIdT lastMcellId,  /* in */
                     int xferFlg,  /* in */
                     ftxHT parentFtx,  /* in */
                     vdIndexT *bfVdIndex,  /* out */
                     bfMCIdT *bfMcellId  /* out */
                     );

#endif  /* _BS_EXTENTS_H_ */
