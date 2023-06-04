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
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 *
 *
 * Facility:
 *
 *     AdvFS 
 *
 * Abstract:
 *
 *     This header file contains structure definitions and function
 *     prototypes related to on-disk storage.
 *
 */

#ifndef _BS_STG_
#define _BS_STG_

#include <advfs/bs_public.h>

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

/*
 * An enum for flags passed into storage routines.
 */
typedef enum {
    STG_NO_FLAGS        = 0x0,  /* No flags */
    STG_MIG_STG_HELD    = 0x1,  /* migStgLk held on entrance */
    STG_NO_COW          = 0x2,  /* Calling to add storage to a child,
                                 * don't do any COW processing. */
} stg_flags_t;

/* #define ADVFS_RBMT_STRESS */

/* 
 * MAX_ALLOC_FOB_CNT restricts the maximum number of fobs to be added in a
 * single storage request.   This value is used by avdfs_bs_add_stg to
 * prevent log half full problems.  It is also used to bound the size of
 * potential COWs for snapshots. 
 */
#define MAX_ALLOC_FOB_CNT 2048

#ifdef _KERNEL
struct advfs_pvt_param;

statusT
advfs_bs_add_stg(
    struct bfAccess *bfap,           /* in - file's access structure */
    off_t first_byte_to_add,         /* in - offset of first byte to add */
    size_t *bytes_to_add,            /* in/out - bytes to add/bytes added */
    size_t file_size,                /* in - bytes in file */
    struct ucred *cred,              /* in - credentials of caller */
    struct advfs_pvt_param *priv_paramp,/* in - AdvFS-specific information */
    ftxHT  parent_ftx,               /* in- parent ftx to operate under */
    ftxHT *ftx_handlep,              /* out - parent transaction handle */
    uint32_t *delCnt,
    void     **delList               /* out - returns storage to be cleaned up */
    );

/*
 * rbf_add_stg - recoverable bitfile storage allocation
 */

statusT
rbf_add_stg( 
    struct bfAccess *bfap,      /* in */
    bf_fob_t fob_offset,        /* in */
    bf_fob_t fob_cnt,           /* in */
    ftxHT parentFtx,            /* in */
    stg_flags_t stg_flags  /* in */
    );


statusT
stg_add_stg (
             ftxHT ftxH,                /* in */
             struct bfAccess* bfap,     /* in */
             bf_fob_t fob_offset,  /* in */
             bf_fob_t fob_cnt,     /* in */
             bf_fob_t minimum_contiguous_fobs, /* in */
             int allowOverlapFlag,        /* in */
             bf_fob_t *alloc_fob_cnt /* out */
             );

statusT
stg_alloc_from_svc_class (
                          bfAccessT *bfAccess,  /* in */
                          serviceClassT reqServices,  /* in */
                          bfTagT bfSetTag,  /* in */
                          bfTagT bfTag,  /* in */
                          bf_fob_t fob_offset,       /* in */
                          bf_fob_t fob_cnt,          /* in */
                          bf_fob_t minimum_contiguous_fobs, /* in */
                          bsInMemXtntMapT *xtntMap,   /* in */
                          bsAllocHintT *alloc_hint,   /* in */
                          ftxHT parentFtx,            /* in */
                          bf_fob_t *alloc_fob_cnt   /* out */
                          );

statusT
cp_stg_alloc_from_svc_class (
                          bfAccessT *bfap,            /* in */
                          bf_fob_t fob_offset,   /* in  start here */
                          bf_fob_t fob_cnt,      /* in  for this long */
                          bf_fob_t minimum_contiguous_fobs, /* in */
                          int32_t stg_type,
                          bsInMemXtntMapT *xtntMap,             /* in  put in this map */
                          bf_fob_t xtnt_start_fob,  /* in  first xtnt fob */
                          bsAllocHintT alloc_hint,     /* in */
                          ftxHT parent_ftx      /* in */
                          );

statusT
cp_stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in */
                         vdIndexT bfVdIndex,        /* in */
                         bf_fob_t fob_offset,  /* in   stg starts here */
                         bf_fob_t fob_cnt,     /* in   ask for this much */
                         bf_fob_t minimum_contiguous_fobs, /* in */
                         int32_t stg_type,
                         bsInMemXtntMapT *xtntMap,              /* in   put in this map */
                         bf_fob_t xtnt_start_fob,   /* in  first xtnt fob */
                         bf_fob_t *alloc_fob_cnt,   /* out - got this many 1k fobs */
                         bf_vd_blk_t dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                         bsAllocHintT alloc_hint,   /* in */
                         ftxHT  parent_ftx
                         );

statusT
xfer_stg (
          bfAccessT  *bfap,             /* file to add storage to */
          bf_fob_t fob_offset,          /* 1k fob offset to add storage */
          bf_fob_t fob_cnt,             /* number of fobs to add */
          bf_vd_blk_t   startblk,       /* storage in hand to add */
          vdIndexT   vdIndex,           /* storage is on this disk */
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
                bfAccessT *bfAccess,    /* in  */
                bf_fob_t fob_offset,    /* in  */
                bf_fob_t fob_cnt,       /* in  */
                int relQuota,           /* in  */
                ftxHT parentFtx,        /* in  */
                uint32_t *delCnt,       /* out */
                void **delList,         /* out */
                int32_t doCow,          /* in  */
                int32_t force_alloc,    /* in  */
                int64_t flags           /* in */
                );

void
stg_remove_stg_finish (
    domainT *dmnp,      /* in */
    uint32_t delCnt,     /* in */
    void *delList       /* in */
    );

typedef enum {
    DOMAIN_UPPER = 0,
    DOMAIN_LOWER = 1,
    FSET_UPPER   = 2,
    FSET_LOWER   = 3
}thresholdType_t;

statusT
advfs_bs_check_threshold(
                         bfAccessT *bfap,       /* in */
                         bf_fob_t fob_cnt,      /* in */
                         thresholdType_t type,  /* in */
                         ftxHT parentFtx        /* in */
                         );

void
merge_xtnt_maps (
                 bsInMemXtntT *xtnts  /* in */
                 );

void
unload_sub_xtnt_maps (
                      bsInMemXtntMapT *xtntMap  /* in */
                      );

statusT
extend_skip (
             int *skipCnt,           /* in/out */
             vdIndexT **skipVdIndex  /* in/out */
             );


#endif /* #ifdef _KERNEL */

#endif  /* BS_STG */

