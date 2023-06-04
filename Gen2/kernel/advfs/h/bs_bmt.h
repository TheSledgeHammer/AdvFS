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
 * Facility:
 *
 *      AdvFS 
 *
 * Abstract:
 *
 *      Bitfile Metadata Table prototypes.
 *
 */

#ifndef _BMT_H_
#define _BMT_H_

/*
 * Default number of pages to expand the BMT by when a new extent is needed
 */
#define BS_BMT_XPND_PGS 128

#ifdef _KERNEL

statusT
bmt_set_mcell_free_list(
    vdT* vdp                 /* in - pointer to current disk */
     );

typedef enum { 
    BMT_NORMAL_MCELL,
    RBMT_MCELL,
    BMT_NORMAL_MCELL_PAGE
} mcellPoolT;

statusT
bmt_alloc_prim_mcell(
    ftxHT        ftxH,       /* in - callers ftx handle */
    mcellUIdT   *mcellUIdp,  /* in/out - mcelluid ptr */
    vdT         *vdp,        /* in - pointer to virtual disk */
    rbfPgRefHT  *pgRefp,     /* out - free mcell page ref */
    struct bsMC **p_mcp      /* out - New mcell      */
    );

statusT
bmt_alloc_mcell(
    bfAccessT *bfAccess,     /* in */
    vdIndexT  poolVdIndex,  /* in */
    mcellPoolT poolType,     /* in */
    bfTagT     bfSetTag,     /* in */
    bfTagT     bfTag,        /* in */
    uint32_t   linkSeg,      /* in */
    ftxHT      parentFtx,    /* in */
    bfMCIdT   *retMcellId,   /* out */
    int32_t    forceFlag     /* in */
    );
                 

statusT
deferred_free_mcell(
    vdT *vd,
    bfMCIdT mcellId,
    ftxHT pftxH
    );

statusT
bmt_alloc_link_mcell(
    domainT   *dmnP,         /* in */
    vdIndexT   poolVdIndex,  /* in */
    mcellPoolT poolType,     /* in */
    bfMCIdT    oldMcellId,   /* in */
    ftxHT      parentFtx,    /* in */
    bfMCIdT   *retMcellId,    /* out */
    uint32_t  *alt_linkSegment /* out */
    );

statusT
bmt_link_mcells(
    domainT *domain,              /* in */
    bfTagT   bfTag,               /* in */
    bfMCIdT  prevMcellId,         /* in */
    bfMCIdT  firstMcellId,        /* in */
    bfMCIdT  lastMcellId,         /* in */
    ftxHT    parentFtx            /* in */
    );

/*
 * These flag will indicate if we want to follow the chainMCId or the nextMCId
 * when unlinking mcells.
 */
typedef enum { 
    CHAINMCID,
    NEXTMCID
} unlinkFlagT;


statusT
bmt_unlink_mcells(
    domainT *domain,            /* in */
    bfTagT   bfTag,             /* in */
    bfMCIdT  prevMcellId,       /* in */
    bfMCIdT  firstMcellId,      /* in */
    bfMCIdT  lastMcellId,       /* in */
    ftxHT    parentFtx,         /* in */
    unlinkFlagT unlinkFlag      /* in */
    );

void
bmt_free_bf_mcells(
    vdT     *vdp,               /* in - ptr to vd struct */
    bfMCIdT  primMCellId,       /* in - prim mcell id */
    ftxHT    parentFtxH,        /* in - parent ftx */
    int32_t  freeXchain         /* in - flag */
    );

void
bmt_free_mcell(
    ftxHT       ftxH,           /* in */
    vdT        *vdp,            /* in */
    bsMCT*      mcp,            /* in */
    bfMCIdT     mcid,           /* in */
    bsMPgT     *bmtpgp,         /* in - page ptr */
    rbfPgRefHT  pgRef           /* in */
    );

void
bmt_init_page(
    bsMPgT        *dpg,         /* pointer to page to init */ 
    bs_meta_page_t pgnum,       /* page number of page to init */
    vdIndexT       vdIndex,     /* in - volume index for this page */
    int32_t        mdtype,      /* metadata type - RBMT or BMT */
    adv_ondisk_version_t dmnVersion,  /* on-disk version number */
    bfFsCookieT    dmnCookie,   /* in - domain cookie */
    vdCookieT      vdCookie     /* in - unique ID for this volume */
    );


statusT
bmtr_update_rec(
    bfAccessT *bfap,            /* in - bf access structure */
    uint16_t rType,             /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,             /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    uint32_t flags              /* in - flags */
    );

statusT
bmtr_update_rec_n_unlk(
    bfAccessT *bfap,            /* in - bf access structure */
    uint16_t rType,             /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,             /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk                  /* in - unlock mcell? */
    );

statusT
allocate_link_new_mcell(
    bfAccessT *bfAccess,  /* in */
    bfMCIdT oldMcellId,   /* in */
    ftxHT parentFtx,      /* in */
    bfMCIdT *newMcellId,  /* out */
    mcellPoolT poolType,  /* in */
    uint32_t *alt_linkSegment /* in */
    );

/*
 *  BMT mcell record management routines
 */

typedef struct {
    bfMCIdT mcid;
} mcellPtrRecT;

void*
bmtr_assign(
    uint16_t type,        /* the record type to assign */
    uint64_t bcnt,        /* the size of record to assign */
    struct bsMC* bmtcp    /* cell to assign record from */
    );

void *
bmtr_assign_rec(
    uint16_t type,        /* in - type of record */
    uint16_t bcnt,        /* in - size of record */
    bsMCT* bmtcp,         /* in - BMT cell pointer */
    rbfPgRefHT pgref      /* in - mcell's page handle */
    );

void*
bmtr_find(
    struct bsMC* bsMCp,   /* pointer to cell */
    int32_t rtype,        /* record type to find */
    domainT *dmnP         /* in - domain handle  */
    );

/*
 * bmtr_scan - like find_bmtr except it will search all chained cells
 *
 * NOTE:  The current BMT page will be referenced
 * when get_bmtr returns with status EOK.  It is the
 * caller's responsibility to deref the page in that case.
 */

statusT
bmtr_scan(
    void **bsrp,          /* out - pointer to BMT record */
    bfPageRefHT *pgref,   /* out - pgref of BMT cell's page */
    int32_t rtype,        /* in - record type to find */
    struct vd *vdp,       /* in - vd (of mcell) pointer */
    bfMCIdT* mcid,        /* in/out - mcell id (primary/found) */
    bfTagT tag            /* in - bitfile's tag */
    );

opxT free_mcell_chains_opx;

void
free_mcell_chains_opx(
    ftxHT ftxH,           /* in - transaction handle */
    int32_t opRecSz,      /* in - undo record size */
    void *opRec           /* in - ptr to undo record */
    );

/***************************************************************************
 ***  BMT access routines                                                ***
 ***************************************************************************/

statusT
bmt_get_vd_bf_descs (
   domainT *dmnP,          /* in */
   vdIndexT vdIndex,       /* in */
   int32_t bfDescSize,     /* in */
   bsBfDescT *bfDesc,      /* in, initialized */
   bfMCIdT *nextBfDescId,  /* in/out */
   int32_t *bfDescCnt      /* out */
   );

/*
 * bmtHT
 */
typedef struct bmtH {
   vdT *vdp;
   uint32_t curMcell;
   bs_meta_page_t curPg;
   bfPageRefHT pgRef;
   bsMPgT *pgPtr;
} bmtHT;

statusT
bmt_open( 
    bmtHT *bmth,         /* out */
    domainT *dmnP,       /* in */
    vdIndexT vdIndex     /* in */
    );

statusT
bmt_read( 
    bmtHT *bmth,                 /* in - BMT handle */
    bfTagT *bfSetTag,            /* out */
    bfTagT *bfTag,               /* out */
    metadataTypeT *metadataType  /* out */
    );

statusT
bmt_close( 
    bmtHT *bmth  /* in */
    );

/***************************************************************************
 ***  BMT mcell integrety checking routines                              ***
 ***************************************************************************/

statusT
check_mcell_ptr(
    bfMCIdT mcid,
    bfTagT tag,
    domainT *dmnp
    );

void
check_BMT_pg(
    struct bsBuf *bp
    );

statusT
check_mcell_hdr(
    bsMCT *mcp,
    bfAccessT *bfap
    );

statusT
check_BSR_XTNTS_rec(
    bsXtntRT *bsXtntRp,
    bfAccessT *bfap,
    vdT *vdp
    );

statusT
check_BSR_XTRA_XTNTS_rec(
    bsXtraXtntRT *bsXtraXtntRp,
    bfAccessT *bfap,
    vdT *vdp,
    bf_fob_t last_fob
    );

#endif /* _KERNEL */
#endif /* _BMT_H_ */
