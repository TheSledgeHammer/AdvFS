/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 * @(#)$RCSfile: bs_bmt.h,v $ $Revision: 1.1.50.2 $ (DEC) $Date: 2006/03/14 19:39:49 $
 */

#ifndef _BMT_H_
#define _BMT_H_

/*
 * Default number of pages to expand the BMT by when a new extent is needed
 */
#define BS_BMT_XPND_PGS 128

#ifdef KERNEL

struct bsMPg* get_bmt_pgptr();

statusT
bmt_set_mcell_free_list(
    vdT* vdp            /* in - pointer to current disk */
    );

typedef enum { 
    BMT_NORMAL_MCELL,
    RBMT_MCELL,
    BMT_NORMAL_MCELL_PAGE
} mcellPoolT;

#ifdef ADVFS_MCELL_TRACE

void mcell_trace( uint16T, uint32T, uint16T, uint16T, uint16T, void* );

#define MCELL_TRACE(_v, _p, _c, _value) \
    mcell_trace((uint16T)(_v), (uint32T)(_p), (uint16T)(_c), \
                (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void*)(_value))

#else /* ADVFS_MCELL_TRACE */

#define MCELL_TRACE(_v, _p, _c, _value)

#endif /* ADVFS_MCELL_TRACE */

statusT
bmt_alloc_prim_mcell(
               ftxHT ftxH,              /* in - callers ftx handle */
               mcellUIdT* mcellUIdp,    /* in/out - mcelluid ptr */
               vdT* vdp,                /* in - pointer to virtual disk */
               rbfPgRefHT* pgRefp,      /* out - free mcell page ref */
               struct bsMC   **p_mcp    /* out - New mcell      */
               );

statusT
bmt_alloc_mcell (
                 bfAccessT * bfAccess,  /* in */
                 uint16T poolVdIndex,   /* in */
                 mcellPoolT poolType,   /* in */
                 bfTagT bfSetTag,       /* in */
                 bfTagT bfTag,          /* in */
                 unsigned int linkSeg,  /* in */
                 ftxHT parentFtx,       /* in */
                 bfMCIdT *retMcellId,   /* out */
                 int forceFlag          /* in */
                 );

statusT
deferred_free_mcell ( vdT *vd,
                      bfMCIdT mcellId,
                      ftxHT pftxH);

statusT
bmt_alloc_link_mcell (
                      domainT *dmnP,        /* in */
                      uint16T poolVdIndex,  /* in */
                      mcellPoolT poolType,  /* in */
                      uint16T oldVdIndex,  /* in */
                      bfMCIdT oldMcellId,  /* in */
                      ftxHT parentFtx,  /* in */
                      bfMCIdT *retMcellId  /* out */
                      );

statusT
bmt_find_mcell (
                domainT *dmnP,  /* in */
                uint16T bfSearchVdIndex,  /* in */
                bfMCIdT bfSearchMCId,  /* in */
                uint16T bfStartVdIndex,  /* in */
                bfMCIdT bfStartMCId,  /* in */
                uint16T *bfPrevVdIndex,  /* out */
                bfMCIdT *bfPrevMCId  /* out */
                );

statusT
bmt_link_mcells (
                 domainT *domain,  /* in */
                 bfTagT bfTag, /* in */
                 vdIndexT prevVdIndex,  /* in */
                 bfMCIdT prevMcellId,  /* in */
                 vdIndexT firstVdIndex,  /* in */
                 bfMCIdT firstMcellId,  /* in */
                 vdIndexT lastVdIndex,  /* in */
                 bfMCIdT lastMcellId,  /* in */
                 ftxHT parentFtx  /* in */
                 );

statusT
bmt_unlink_mcells (
                   domainT *domain,  /* in */
                   bfTagT bfTag, /* in */
                   vdIndexT prevVdIndex,  /* in */
                   bfMCIdT prevMcellId,  /* in */
                   vdIndexT firstVdIndex,  /* in */
                   bfMCIdT firstMcellId,  /* in */
                   vdIndexT lastVdIndex,  /* in */
                   bfMCIdT lastMcellId,  /* in */
                   ftxHT parentFtx  /* in */
                   );

void
bmt_free_bf_mcells(
                   vdT* vdp,            /* in - ptr to vd struct */
                   bfMCIdT primMCellId, /* in - prim mcell id */
                   ftxHT parentFtxH,    /* in - parent ftx */
                   int freeXchain       /* in - flag */
                   );

void
bmt_free_mcell(
               ftxHT ftxH,     /* in */
               vdT* vdp,       /* in */
               bsMCT* mcp,     /* in */
               bfMCIdT mcid,   /* in */
               bsMPgT* bmtpgp,  /* in - page ptr */
               rbfPgRefHT pgRef /* in */
               );

void
bmt_init_page(
              struct bsMPg *dpg,        /* pointer to page to init */
              bsPageT pgnum,            /* page number of page to init */
              int mdtype,               /* metadata type - RBMT or BMT */
              int dmnVersion            /* on-disk version number */
              );

statusT
bmtr_clone_recs(
    bfMCIdT sMCId,
    uint16T sVdIndex,
    bfMCIdT dMCId,
    uint16T dVdIndex,
    uint16T npVdIndex,
    domainT *dmnp,
    ftxHT parentFtxH
    );

statusT
bmtr_put_rec_n_unlk_int(
    bfAccessT *bfap,            /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    long xid                    /* in - CFS transaction id */
    );

statusT
bmtr_update_rec(
    bfAccessT *bfap,            /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    uint32T flags               /* in - flags */
    );

statusT
bmtr_update_rec_n_unlk(
    bfAccessT *bfap,            /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk                  /* in - unlock mcell? */
    );

statusT
allocate_link_new_mcell (
                         bfAccessT *bfAccess,  /* in */
                         vdIndexT oldVdIndex,  /* in */
                         bfMCIdT oldMcellId,   /* in */
                         ftxHT parentFtx,      /* in */
                         vdIndexT *newVdIndex, /* out */
                         bfMCIdT *newMcellId,  /* out */
                         mcellPoolT poolType   /* in */
                         );

/***************************************************************************
 ***  BMT mcell record management routines                               ***
 ***************************************************************************/

typedef struct {
    bfMCIdT mcid;
    vdIndexT vdIndex;
} mcellPtrRecT;

void*
bmtr_assign(
              unsigned short type,      /* the record type to assign */
              unsigned short bcnt,      /* the size of record to assign */
              struct bsMC* bmtcp        /* cell to assign record from */
              );

void *
bmtr_assign_rec(
    u_short type,        /* in - type of record */
    u_short bcnt,        /* in - size of record */
    bsMCT* bmtcp,        /* in - BMT cell pointer */
    rbfPgRefHT pgref     /* in - mcell's page handle */
    );

void*
bmtr_find(
          struct bsMC* bsMCp,           /* pointer to cell */
          int rtype,                    /* record type to find */
          domainT *dmnP               /* in - domain handle  */
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
          int rtype,            /* in - record type to find */
          struct vd *vdp,       /* in - vd (of mcell) pointer */
          bfMCIdT* mcid,        /* in/out - mcell id (primary/found) */
          bfTagT tag            /* in - bitfile's tag */
          );

opxT free_mcell_chains_opx;

void
free_mcell_chains_opx(
                      ftxHT ftxH,       /* in - transaction handle */
                      int opRecSz,  /* in - undo record size */
                      void *opRec   /* in - ptr to undo record */
                      );

/***************************************************************************
 ***  BMT access routines                                                ***
 ***************************************************************************/

typedef enum { 
    BMT_NORMAL_METADATA,
    BMT_XTNT_METADATA,
    BMT_PRIME_MCELL_XTNT_METADATA
} metadataTypeT;

typedef struct bsBfDesc {
    bfTagT bfSetTag;
    bfTagT bfTag;
    metadataTypeT metadataType;
}  bsBfDescT;

statusT
bmt_get_vd_bf_descs (
                     domainT *dmnP,  /* in */
                     vdIndexT vdIndex,  /* in */
                     int bfDescSize,  /* in */
                     bsBfDescT *bfDesc,  /* in, initialized */
                     bfMCIdT *nextBfDescId,  /* in/out */
                     int *bfDescCnt  /* out */
                     );

/*
 * bmtHT
 */
typedef struct bmtH {
   vdT *vdp;
   uint32T curMcell;
   uint32T curPg;
   bfPageRefHT pgRef;
   bsMPgT *pgPtr;
} bmtHT;

statusT
bmt_open( 
           bmtHT *bmth,         /* out */
           domainT *dmnP,     /* in */
           vdIndexT vdIndex      /* in */
           );

statusT
bmt_read( 
           bmtHT *bmth,  /* in - BMT handle */
           bfTagT *bfSetTag,   /* out */
           bfTagT *bfTag,   /* out */
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
check_mcell_ptr( vdIndexT vdi, bfMCIdT mcid, bfTagT tag, domainT *dmnp);

void check_BMT_pg(struct bsBuf *bp);

statusT
check_mcell_hdr( bsMCT *mcp, bfAccessT *bfap);

statusT
check_BSR_XTNTS_rec( bsXtntRT *bsXtntRp, bfAccessT *bfap, vdT *vdp);

statusT
check_BSR_SHADOW_XTNTS_rec( bsShadowXtntT *bsShadowXtntp,
                            bfAccessT *bfap,
                            vdT *vdp);

statusT
check_BSR_XTRA_XTNTS_rec( bsXtraXtntRT *bsXtraXtntRp,
                          bfAccessT *bfap,
                          vdT *vdp,
                          uint32T lastPg);

#endif /* KERNEL */
#endif /* _BMT_H_ */
