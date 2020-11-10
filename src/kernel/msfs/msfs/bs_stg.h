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
 * @(#)$RCSfile: bs_stg.h,v $ $Revision: 1.1.35.2 $ (DEC) $Date: 2006/08/28 17:41:07 $
 */

#ifndef _BS_STG_
#define _BS_STG_

#include <msfs/bs_public.h>

/*****************************************
 * 
 * The following #define will cause stress on the RBMT
 * by disabling the storage reservtion code and forcing
 * the BMT to be extended by a single page at a time.
 *
 * Beware that existing domains may end up with a
 * less than optimal BMT that can not be removed if much
 * storage is added.
 */

/* #define ADVFS_RBMT_STRESS */

statusT
stg_add_stg (
             ftxHT ftxH,                /* in */
             struct bfAccess* bfap,     /* in */
             unsigned long bfPageOffset,/* in */
             unsigned long bfPageCnt,    /* in */
             int allowOverlapFlag,  /* in */
             uint32T *allocPageCnt  /* out */
             );

statusT
stg_add_stg_no_cow (
             ftxHT ftxH,                /* in */
             struct bfAccess* bfap,     /* in */
             unsigned long bfPageOffset,/* in */
             unsigned long bfPageCnt,    /* in */
             int allowOverlapFlag,  /* in */
             uint32T *allocPageCnt  /* out */
             );

statusT
stg_set_alloc_disk (
                    struct bfAccess *bfap,  /* in */
                    vdIndexT curVdIndex,  /* in */
                    uint32T pageCntNeeded,  /* in */
                    int forceFlag  /* in */
                    );

statusT
stg_alloc_from_svc_class (
                          bfAccessT *bfAccess,  /* in */
                          serviceClassT reqServices,  /* in */
                          serviceClassT optServices,  /* in */
                          bfTagT bfSetTag,  /* in */
                          bfTagT bfTag,  /* in */
                          uint32T bfPageOffset,  /* in */
                          uint32T bfPageCnt,  /* in */
                          int bfPageSize,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          bsAllocHintT *alloc_hint, /* in/out */
                          ftxHT parentFtx,  /* in */
                          uint32T *allocPageCnt  /* out */
                          );

statusT
cp_stg_alloc_from_svc_class (
                          bfAccessT *bfap,            /* in */
                          uint32T bfPageOffset,       /* in */
                          uint32T bfPageCnt,          /* in */
                          int32T stg_type,
                          bsInMemXtntMapT *xtntMap,   /* in */
                          uint32T startPage,          /* in */
                          bsAllocHintT alloc_hint,    /* in */
                          uint32T *copyXferSize       /* out */
                          );

statusT
stg_alloc_from_one_disk (
                         bfAccessT *bfAccess,  /* in */
                         vdIndexT bfVdIndex,  /* in */
                         bfTagT bfSetTag,  /* in */
                         bfTagT bfTag,  /* in */
                         uint32T bfPageOffset,  /* in */
                         uint32T bfPageCnt,  /* in */
                         int bfPageSize,  /* in */
                         bsInMemXtntMapT *xtntMap,  /* in */
                         bsAllocHintT *alloc_hint, /* in/out */
                         ftxHT parentFtx,  /* in */
                         uint32T *allocPageCnt  /* out */
                         );

statusT
cp_stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in */
                         vdIndexT bfVdIndex,        /* in */
                         uint32T bfPageOffset,      /* in */
                         uint32T bfPageCnt,         /* in */
                         int32T stg_type,
                         bsInMemXtntMapT *xtntMap,  /* in */
                         uint32T startPage,         /* in */
                         uint32T *allocPageCnt,     /* out */
                         uint64T dstBlkOffset,      /* in */
                         bsAllocHintT alloc_hint     /* in */
                         );

statusT
xfer_stg (
          bfAccessT  *bfap,         /* file to add storage to */
          ulong_t    bfPageOffset,  /* offset to add storage */
          ulong_t    bfPageCnt,     /* number of pages to add */
          uint32T    startblk,      /* storage in hand to add */
          vdIndexT   vdIndex,       /* storage is on this disk */
          ftxHT      parentFtx
         );

statusT
stg_alloc_new_mcell (
                     bfAccessT *bfAccess,  /* in */
                     bfTagT bfSetTag,  /* in */
                     bfTagT bfTag,  /* in */
                     vdIndexT newVdIndex,  /* in */
                     ftxHT parentFtx,      /* in */
                     bfMCIdT *newMcellId,  /* out */
                     int forceFlag         /* in */
                     );

statusT
stg_remove_stg_start (
                bfAccessT *bfAccess,    /* in */
                uint32T pageOffset,     /* in */
                uint32T pageCnt,        /* in */
                int relQuota,           /* in */
                ftxHT parentFtx,        /* in */
                uint32T *delCnt,        /* out */
                void **delList,         /* out */
                int32T doCow            /* in */
                );

void
stg_remove_stg_finish (
    domainT *dmnp,      /* in */
    uint32T delCnt,     /* in */
    void *delList       /* in */
    );

int
page_is_mapped_local(
    bfAccessT *bfap,
    uint32T pg,
    uint32T *nextPage, /* out */
    int permHoleFlg,  /* in */
    vdIndexT *vdIndex, /* out */
    uint32T *vdBlk, /* out */
    uint32T *start_pg, /* out */
    int load_map
    );

#endif  /* BS_STG */
