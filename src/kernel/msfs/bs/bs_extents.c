/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
 */
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
 *
 *
 * Facility:
 *
 *  MFS/bs
 *
 * Abstract:
 *
 *  bs_extents.c
 *  This module contains routines related to on-disk and in-memory extents.
 *
 *
 * Date:
 *
 *  Wed Oct 30 15:51:12 1991
 *
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: bs_extents.c,v $ $Revision: 1.1.145.7 $ (DEC) $Date: 2006/01/03 18:54:59 $"

#include <sys/lock_probe.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_bmt.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_stripe.h>

#define ADVFS_MODULE BS_EXTENTS

/*
 * protos
 */

opxT undo_mcell_cnt;

void
undo_mcell_cnt (
                ftxHT ftxH,
                int size,
                void* address
                );

opxT undo_upd_xtnt_rec;

void
undo_upd_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void* address
                   );

opxT undo_cre_xtnt_rec;

void
undo_cre_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void* address
                   );

void
x_dealloc_extent_maps(bsInMemXtntT *xtnts);

statusT
update_xtnt_rec ( 
                 domainT *domain,  /* in */
                 bfTagT bfTag, /* in */
                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                 ftxHT parentFtx  /* in */
                 );

static
void
update_xtnt_array (
                   rbfPgRefHT pgPin,  /* in */
                   bsInMemSubXtntMapT *subXtntMap,  /* in */
                   uint16T *bsXACnt,  /* in */
                   bsXtntT *bsXA  /* in */
                   );

static
statusT
load_inmem_xtnt_map (
                     bfAccessT *bfap,           /* in, modified */
                     bsXtntRT *xtntRec,         /* in */
                     uint32T *retTotalPageCnt   /* out */
                     );

static
statusT
load_from_shadow_rec (
                      bfAccessT *bfap,               /* in, modified */
                      vdIndexT bfVdIndex,            /* in */
                      bfMCIdT bfMcellId,             /* in */
                      bsInMemXtntMapT **bfXtntMap,   /* out */
                      uint32T *allocPageCnt,         /* out */
                      vdIndexT *bfNextVdIndex,       /* out */
                      bfMCIdT *bfNextMcellId         /* out */
                      );

static
statusT
load_from_xtnt_rec (
                    bfAccessT *bfap,           /* in, modified */
                    bsXtntRT *xtntRec,         /* in */
                    uint32T *allocPageCnt,     /* out */
                    vdIndexT *bfNextVdIndex,   /* out */
                    bfMCIdT *bfNextMcellId     /* out */
                    );

static
statusT
load_from_rbmt_xtnt_rec (
                         bfAccessT *bfap,        /* in, modified */
                         bsXtntRT *xtntRec,      /* in */
                         uint32T *allocPageCnt   /* out */
                         );

static
statusT
load_from_xtra_xtnt_rec (
                         bfAccessT *bfap,                  /* in, modified */
                         vdIndexT vdIndex,                 /* in */
                         bfMCIdT mcellId,                  /* in */
                         bsInMemSubXtntMapT *subXtntMap,   /* in */
                         uint32T lastPg,                   /* in */
                         vdIndexT *nextVdIndex,            /* out */
                         bfMCIdT *nextMcellId              /* out */
                         );

static
statusT
load_from_bmt_xtra_xtnt_rec (
                             domainT *dmnP,  /* in */
                             vdIndexT vdIndex,  /* in */
                             bfMCIdT mcellId,  /* in */
                             lbnT vdBlk,
                             bsInMemSubXtntMapT *subXtntMap,  /* in */
                             vdIndexT *nextVdIndex,  /* out */
                             bfMCIdT *nextMcellId,  /* out */
                             lbnT *nextVdBlk
                             );

static
statusT
load_from_xtnt_array (
                      int cnt,  /* in */
                      bsXtntT *bsXA,  /* in */
                      bsInMemSubXtntMapT *subXtntMap  /* in */
                      );

int CheckXtnts = FALSE;

typedef struct mcellCntUndoRec
{
    uint32T type;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bfTagT bfTag;
    uint32T mcellCnt;
} mcellCntUndoRecT;

/*
 * undo_mcell_cnt
 *
 * This function is called when an on-disk extent transaction must be backed
 * out.  The subtransactions that were completed before this subtransaction
 * have not been backed out while the subtransactions after this subtransaction
 * have been backed out.
 *
 * This function assumes that the transaction was a subtransaction and that
 * the appropriate locks are still held.  During run-time, the parent
 * transaction took out the locks.  During recovery, the parent transaction's
 * undo routine took out the locks.
 *
 * Transaction:  The primary or shadow extent record's "mcellCnt" field is
 * modified.
 */

void
undo_mcell_cnt (
                ftxHT ftxH,
                int size,
                void *address
                )
{
    bsMPgT *bmtp;
    domainT *dmnP;
    bsMCT *mcellp;
    rbfPgRefHT pgRef;
    bsShadowXtntT *shadowRec;
    statusT sts;
    mcellCntUndoRecT *undoRec;
    bsXtntRT *xtntRec;
    vdT *vdp;
    struct bfAccess *mdap;

    undoRec = (mcellCntUndoRecT *)address;

    dmnP = ftxH.dmnP;

    vdp = VD_HTOP(undoRec->vdIndex, dmnP);

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(undoRec->bfTag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }
    sts = rbf_pinpg ( &pgRef,
                      (void *)&bmtp,
                      mdap,
                      undoRec->mCId.page,
                      BS_NIL,
                      ftxH);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_mcell_cnt: rbf_pinpg failed ", (long) BSERRMSG(sts));
    }

    mcellp = &(bmtp->bsMCA[undoRec->mCId.cell]);

    switch (undoRec->type) {

      case BSR_XTNTS:

        /*
         * Find the primary extent record and restore the mcell count.
         */

        xtntRec = bmtr_find (mcellp, BSR_XTNTS, vdp->dmnP);
        if (xtntRec == NULL) {
            ADVFS_SAD0("undo_mcell_cnt: can't find extent record");
        }

        rbf_pin_record( pgRef,
                        &xtntRec->firstXtnt.mcellCnt,
                        sizeof(xtntRec->firstXtnt.mcellCnt));
        xtntRec->firstXtnt.mcellCnt = undoRec->mcellCnt;

        break;

      case BSR_SHADOW_XTNTS:

        /*
         * Find the shadow record and restore the mcell count.
         */

        shadowRec = bmtr_find (mcellp, BSR_SHADOW_XTNTS, vdp->dmnP);
        if (shadowRec == NULL) {
            ADVFS_SAD0("undo_mcell_cnt: can't find shadow record");
        }

        rbf_pin_record( pgRef,
                        &(shadowRec->mcellCnt),
                        sizeof(shadowRec->mcellCnt));
        shadowRec->mcellCnt = undoRec->mcellCnt;

        break;

      default:
        ADVFS_SAD1("undo_mcell_cnt: bad record type ", undoRec->type);
    }

    return;

}  /* end undo_mcell_cnt */

typedef struct updXtntRecUndoRec
{
    uint16T type;
    uint16T xCnt;
    uint16T index;
    uint16T cnt;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bfTagT bfTag;
    union {
        bsXtntT xtnt[BMT_XTNTS];
        bsXtntT shadow[BMT_SHADOW_XTNTS];
        bsXtntT xtraXtnt[BMT_XTRA_XTNTS];
    } bsXA;
} updXtntRecUndoRecT;

/*
 * undo_upd_xtnt_rec
 *
 * This function when an on-disk extent transaction must be backed out.  The
 * subtransactions that were completed before this subtransaction have not been
 * backed out while the subtransactions after this subtransaction have been
 * backed out.
 *
 * This function assumes that the transaction was a subtransaction and that
 * the appropriate locks are still held.  During run-time, the parent
 * transaction took out the locks.  During recovery, the parent transaction's
 * undo routine took out the locks.
 *
 * Transaction:  The primary, shadow or extra extent record's descriptors were
 * updated.
 */

void
undo_upd_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void *address
                   )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16T *bsXACnt;
    domainT *domain;
    uint16T i;
    bsMCT *mcell;
    rbfPgRefHT pgPin;
    bsShadowXtntT *shadowRec;
    statusT sts;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    updXtntRecUndoRecT undoRec;
    vdT *vdp;
    struct bfAccess *mdap,*bfap;
    bsInMemXtntT *xtnts;
    int v4bmt_or_rbmt;
    int this_is_v3bmt;

    /*
     * Copy address locally to avoid potential unaligned access
     * to the integer aligned log page.
     */

    bcopy (address, &undoRec, size);

    domain = ftxH.dmnP;

    vdp = VD_HTOP(undoRec.vdIndex, domain);

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(undoRec.bfTag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }
    sts = rbf_pinpg (&pgPin,
                     (void *)&bmt,
                     mdap,
                     undoRec.mCId.page,
                     BS_NIL,
                     ftxH);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_upd_xtnt_rec: rbf_pinpg failed ", (long)BSERRMSG(sts));
    }

    mcell = &(bmt->bsMCA[undoRec.mCId.cell]);

    switch (undoRec.type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vdp->dmnP);
        if (xtntRec == NULL) {
            ADVFS_SAD0("undo_upd_xtnt_rec: can't find extent record");
        }
        bsXACnt = &(xtntRec->firstXtnt.xCnt);
        bsXA = xtntRec->firstXtnt.bsXA;

        break;

      case BSR_SHADOW_XTNTS:

        shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, vdp->dmnP);
        if (shadowRec == NULL) {
            ADVFS_SAD0("undo_upd_xtnt_rec: can't find shadow extent record");
        }
        bsXACnt = &(shadowRec->xCnt);
        bsXA = shadowRec->bsXA;

        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vdp->dmnP);
        if (xtraXtntRec == NULL) {
            ADVFS_SAD0("undo_upd_xtnt_rec: can't find extra extent record");
        }
        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = xtraXtntRec->bsXA;

        break;

      default:
        ADVFS_SAD1("undo_upd_xtnt_rec: bad extent record type", undoRec.type);
    }  /* end switch */

    rbf_pin_record (
                    pgPin,
                    &(bsXA[undoRec.index]),
                    sizeof (bsXtntT) * undoRec.cnt
                    );

    for (i = 0; i < undoRec.cnt; i++) {
        bsXA[undoRec.index + i] = undoRec.bsXA.xtnt[i];
    }

    rbf_pin_record (pgPin, bsXACnt, sizeof (*bsXACnt));
    *bsXACnt = undoRec.xCnt;

    v4bmt_or_rbmt = RBMT_THERE(domain) &&
                    BS_BFTAG_RSVD(undoRec.bfTag) &&
                    (BS_BFTAG_RBMT(undoRec.bfTag) || BS_IS_TAG_OF_TYPE( undoRec.bfTag, BFM_BMT));
    this_is_v3bmt = !RBMT_THERE(domain) &&
                    BS_BFTAG_RSVD(undoRec.bfTag) &&
                    BS_IS_TAG_OF_TYPE(undoRec.bfTag,BFM_BMT_V3);

    /* 
     * We need to treat the RBMT/BMT differently here. Regular files will
     * have had their xtnt maps deleted via a prior call to add_stg_undo. This
     * routine would then update their on-disk xtnt maps. The next open of 
     * the file would trigger the new xtnt maps to be reread from disk. 
     * The RBMT/BMT however is never closed. We can not just remove the in
     * memory xtnt maps in add_stg_undo since other routines (other undo routines)
     * expect the maps to be present. So only after we have actually updated the
     * on-disk xtnt maps should we update the in-memory xtnt maps.
     * NOTE:
     *  The following asumptions about the RBMT/BMT are being made here:
     *
     *  1) The bfap is available via the vd strucuture.
     *  2) There RBMT/BMT is NOT striped.
     *  3) The RBMT/BMT is never sparse (The call to imm_get_alloc_page_cnt
     *                                    is probably unnecessary (just use nextPage).
     */

    if(v4bmt_or_rbmt || this_is_v3bmt) 
    {
        if ((v4bmt_or_rbmt) && (BS_BFTAG_RBMT(undoRec.bfTag)))
            bfap = vdp->rbmtp;
        else 
            bfap = vdp->bmtp;

        xtnts = &(bfap->xtnts);

        XTNMAP_LOCK_WRITE( &bfap->xtntMap_lk );

        imm_delete_xtnt_map (xtnts->xtntMap);
        xtnts->xtntMap = NULL;
        xtnts->validFlag = 0;
        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
        x_load_inmem_xtnt_map(bfap,X_LOAD_REFERENCE);

        bfap->nextPage = imm_get_next_page(xtnts);
        imm_get_alloc_page_cnt(xtnts->xtntMap,0,bfap->nextPage,&xtnts->allocPageCnt);

        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
    }
        
    return;

}  /* end undo_upd_xtnt_rec */

typedef struct creXtntRecUndoRec
{
    vdIndexT vdIndex;
    bfMCIdT mcellId;
} creXtntRecUndoRecT;

/*
 * undo_cre_xtnt_rec
 *
 * This function backs out an on-disk extent transaction for 
 * the rbmt mcell 27.  (All other cases are handled by the 
 * mcell allocate routine.)  The subtransactions that were 
 * completed before this subtransaction have not been backed 
 * out while the subtransactions after this subtransaction 
 * have been backed out.
 *
 * Transaction:  The mcell's tag, set tag, rec type and rec 
 * count need to be cleared for rbmt mcell 27 case.
 */

void
undo_cre_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void *address
                   )
{
    bsMPgT *rbmt;
    domainT *domain;
    bsMCT *mcp;
    rbfPgRefHT pgPin;
    statusT sts;
    creXtntRecUndoRecT undoRec;
    vdT *vdp;
    struct bfAccess *mdap,*bfap;
    bsMRT *rec;
 
    /*
     * Copy address locally to avoid potential unaligned access
     * to the integer aligned log page.
     */

    bcopy (address, &undoRec, size);

    MS_SMP_ASSERT( undoRec.mcellId.cell == RBMT_RSVD_CELL );

    domain = ftxH.dmnP;

    MS_SMP_ASSERT( RBMT_THERE(domain) );

    vdp = VD_HTOP(undoRec.vdIndex, domain);
    mdap = vdp->rbmtp;

    sts = rbf_pinpg (&pgPin,
                     (void *)&rbmt,
                     mdap,
                     undoRec.mcellId.page,
                     BS_NIL,
                     ftxH);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_cre_xtnt_rec: rbf_pinpg failed ", (long)BSERRMSG(sts));
    }

    mcp = &(rbmt->bsMCA[undoRec.mcellId.cell]);
    rec = (bsMRT *)&(mcp->bsMR0[0]);

    rbf_pin_record (
                    pgPin,
                    &mcp->tag,
                    (char*)mcp->bsMR0 - (char*)&mcp->tag
                    );

    mcp->tag         = NilBfTag;
    mcp->bfSetTag    = NilBfTag;

    rec->type = BSR_NIL;
    rec->bCnt = sizeof (bsMRT);

    return;

}  /* end undo_cre_xtnt_rec */

/*
 * init_bs_xtnts_opx
 *
 * This function registers the on-disk extent code as a transaction agent.
 */

void
init_bs_xtnts_opx ()
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_XTNT_UPD_MCELL_CNT_V1,
                             &undo_mcell_cnt,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(0):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_UPD_REC_V1,
                             &undo_upd_xtnt_rec,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(1):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_CRE_REC,
                             &undo_cre_xtnt_rec,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(2):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_CRE_XTRA_REC_V1,
                             NULL,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(3):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_CRE_SHADOW_REC_V1,
                             NULL,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(4):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_REWRITE_MAP_V1,
                             NULL,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(5):register failure", sts);
    }

    return;

} /* end init_bs_xtnts_opx */

/*
 * odm_remove_mcells_from_xtnt_map
 *
 * This function removes the specified mcells from the bitfile's extent
 * chain and adjusts the mcell count.  It is left to the caller to either
 * re-use the mcells or to deallocate them.
 *
 * The mcells to remove are the "origStart" + 1 mcell thru the "origEnd"
 * mcell.  The "origStart" mcell is needed to remove the first mcell.
 *
 * This function assumes that the caller has exclusive access to both
 * the in-mem and on-disk extent maps.
 * 
 * This function assumes that the file is not reserved.
 */

statusT
odm_remove_mcells_from_xtnt_map (
                                 domainT *domain,  /* in */
                                 bfTagT bfTag, /* in */
                                 bsInMemXtntMapT *xtntMap,  /* in */
                                 uint32T start_index,
                                 ftxHT parentFtx  /* in */
                                 )
{
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    
    if (xtntMap->origEnd == xtntMap->origStart) {
        return EOK;
    }
    
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    MS_SMP_ASSERT( (subXtntMap->type == BSR_XTRA_XTNTS)||
                   (subXtntMap->type == BSR_XTNTS) ||
                   (subXtntMap->type == BSR_SHADOW_XTNTS) );
    
    MS_SMP_ASSERT( (subXtntMap->type != BSR_XTNTS ) || 
                   ( (start_index == 1) &&
                     (domain->dmnVersion >= FIRST_XTNT_IN_PRIM_MCELL_VERSION))  );
    
    if (start_index == 0)
    {
        MS_SMP_ASSERT(xtntMap->origStart > 0);
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart-1]);
    }
    
    sts = bmt_unlink_mcells ( domain,
                              bfTag,
                              subXtntMap->vdIndex,
                              subXtntMap->mcellId,
                              xtntMap->subXtntMap[xtntMap->origStart + start_index].vdIndex,
                              xtntMap->subXtntMap[xtntMap->origStart + start_index].mcellId,
                              xtntMap->subXtntMap[xtntMap->origEnd].vdIndex,
                              xtntMap->subXtntMap[xtntMap->origEnd].mcellId,
                              parentFtx );
    if (sts != EOK) {
        return sts;
    }
        
    sts = update_mcell_cnt( domain,
                            bfTag,
                            xtntMap->hdrVdIndex,
                            xtntMap->hdrMcellId,
                            xtntMap->hdrType,
                            xtntMap->origStart - xtntMap->origEnd + (start_index -1) ,
                            parentFtx );

    return sts;

}  /* end odm_remove_mcells_from_xtnt_map */

/*
 * odm_create_xtnt_map
 *
 * This function creates an on-disk extent map using the in-memory extent map
 * specified by the caller.
 *
 * This function assumes that the caller has shared or exclusive access to
 * the in-mem extent map.  In other words, this function assumes that the
 * in-mem extent map cannot change.
 */

statusT
odm_create_xtnt_map (
                     bfAccessT *bfap,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bfTagT bfTag,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     ftxHT parentFtx,  /* in */
                     vdIndexT *bfVdIndex,  /* out */
                     bfMCIdT *bfMcellId  /* out */
                     )
{
    domainT *domain;
    uint32T i;
    bfMCIdT mcellId;
    bsInMemSubXtntMapT *nextSubXtntMap;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    vdIndexT vdIndex;

    if (BS_BFTAG_RSVD (bfTag)) {
        return ENOT_SUPPORTED;
    }
    domain = bfap->dmnP;

    /*
     * Link all the mcells together.
     */

    subXtntMap = &(xtntMap->subXtntMap[0]);
    for (i = 1; i < xtntMap->cnt; i++) {
        nextSubXtntMap = &(xtntMap->subXtntMap[i]);
        sts = bmt_link_mcells (
                               domain,
                               bfap->tag,
                               subXtntMap->vdIndex,
                               subXtntMap->mcellId,
                               nextSubXtntMap->vdIndex,
                               nextSubXtntMap->mcellId,
                               nextSubXtntMap->vdIndex,
                               nextSubXtntMap->mcellId,
                               parentFtx
                               );
        if (sts != EOK) {
            return sts;
        }
        subXtntMap = nextSubXtntMap;
    }  /* end while */

    /*
     * If there is extent information in the primary mcell, then there is
     * no need to create a new extent header record and mcell.  Just use 
     * the one we have.
     */
    if (xtntMap->subXtntMap[0].type == BSR_XTNTS) {
        *bfVdIndex = xtntMap->subXtntMap[0].vdIndex;
        *bfMcellId = xtntMap->subXtntMap[0].mcellId;
    }
    else {
        sts = create_xtnt_map_hdr(
                                  bfap,
                                  xtntMap->subXtntMap[0].vdIndex,
                                  xtntMap->subXtntMap[0].mcellId,
                                  xtntMap->subXtntMap[xtntMap->cnt - 1].vdIndex,
                                  xtntMap->subXtntMap[xtntMap->cnt - 1].mcellId,
                                  0,  /* no clone xfer xtnts here */
                                  parentFtx,
                                  &vdIndex,
                                  &mcellId
                                  );
        if (sts != EOK) {
            return sts;
        }

        *bfVdIndex = vdIndex;
        *bfMcellId = mcellId;
    }

    return EOK;

}  /* end odm_create_xtnt_map */

/*
 * create_xtnt_map_hdr
 *
 * This function creates the extent map header and links the mcells onto it.
 */

statusT
create_xtnt_map_hdr (
                     bfAccessT *bfap,  /* in */
                     vdIndexT firstVdIndex,  /* in */
                     bfMCIdT firstMcellId,  /* in */
                     vdIndexT lastVdIndex,  /* in */
                     bfMCIdT lastMcellId,  /* in */
                     int xferFlg,
                     ftxHT parentFtx,  /* in */
                     vdIndexT *bfVdIndex,  /* out */
                     bfMCIdT *bfMcellId  /* out */
                     )
{
    domainT *domain;
    bfSetT *bfSetp;
    bfTagT bfTag;
    bsInMemSubXtntMapT emptySubXtntMap;
    bfMCIdT primMcellId;
    vdIndexT primVdIndex;
    statusT sts;
    vdT *vd;

    MS_SMP_ASSERT(!BS_BFTAG_RSVD(bfap->tag));  /* not designed for rsvd files */

    bfTag = bfap->tag;
    bfSetp = bfap->bfSetp;

    if ( xferFlg ) {
        MS_SMP_ASSERT(BFSET_VALID(bfSetp->cloneSetp));
        MS_SMP_ASSERT(bfap->noClone == FALSE);
        bfSetp = bfSetp->cloneSetp;  /* set up to xfer xtnts to clone */
    }

    /*
     * Allocate an mcell and initialize a primary extent record.
     */

    domain = bfap->dmnP;
    primVdIndex = firstVdIndex;
    vd = VD_HTOP(primVdIndex, domain);
    sts = bmt_alloc_mcell( bfap,
                           primVdIndex,
                           BMT_NORMAL_MCELL,
                           bfSetp->dirTag,
                           bfTag,
                           0,  /* linkSeg */
                           parentFtx,
                           &primMcellId,
                           TRUE);
    if ( sts != EOK ) {
        return sts;
    }

    sts = imm_init_sub_xtnt_map( &emptySubXtntMap,
                                 0, /* pageOffset */
                                 0, /* pageCnt */
                                 primVdIndex,
                                 primMcellId,
                                 BSR_XTNTS,
                                 BMT_XTNTS,
                                 1);
    if ( sts != EOK ) {
        return sts;
    }

    sts = odm_create_xtnt_rec( bfap,
                               -1,  /* allocVdIndex */
                               &emptySubXtntMap,
                               xferFlg,
                               parentFtx);

    imm_unload_sub_xtnt_map (&emptySubXtntMap);
    if (sts != EOK) {
        return sts;
    }

    /*
     * Link the first mcell the new primary extent record.
     */
    sts = bmt_link_mcells (
                           domain,
                           bfap->tag,
                           primVdIndex,
                           primMcellId,
                           firstVdIndex,
                           firstMcellId,
                           lastVdIndex,
                           lastMcellId,
                           parentFtx
                          );
    if (sts != EOK) {
        return sts;
    }

    *bfVdIndex = primVdIndex;
    *bfMcellId = primMcellId;

    return EOK;

}  /* end create_xtnt_map_hdr */

/*
 * odm_rewrite_xtnt_map
 *
 * This function rewrites the on-disk extent map from the in-mem extent map.
 * It deletes the old on-disk extent map, copies the extent descriptors from
 * the old in-mem extent map to the new in-mem extent map, and creates the
 * new on-disk extent map from the new in-mem extent map.
 */

statusT
odm_rewrite_xtnt_map (
                      bfAccessT *bfap,   /* in */
                      int xtntMapIndex,  /* in */
                      ftxHT parentFtxH,  /* in */
                      long xid           /* in - CFS transaction id */
                      )
{
    bfTagT bfSetTag;
    domainT *domain;
    ftxHT ftxH;
    int failFtxFlag = 0;
    uint32T i;
    bfMCIdT mcellId;
    bsInMemXtntMapT *newXtntMap = NULL;
    bfMCIdT prevMcellId;
    vdIndexT prevVdIndex;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    vdIndexT vdIndex;
    bsInMemXtntMapT *xtntMap;
    vdT *vdp = NULL;

    if ((bfap->xtnts.type != BSXMT_APPEND) &&
        (bfap->xtnts.type != BSXMT_STRIPE)) {
        return ENOT_SUPPORTED;
    }

    bfSetTag = bfap->bfSetp->dirTag;

    domain = bfap->dmnP;

    if(FIRST_XTNT_IN_PRIM_MCELL(bfap->dmnP->dmnVersion, bfap->xtnts.type)) {
        /* for version 4 domains the whole problem 
         * of mcell not describing ANY storage has been fixed - therefore, 
         * we don't need to rewrite the extent map to clean up these maps.
         * Just return here.
         */

        return EOK;
    }

    /* Version 3 and earlier domains only from this point on */

    /*
     * FIX - if this is a striped bitfile and the extent map doesn't describe
     * any storage, we can't delete the mcell because it serves as a ref count
     * on that disk.
     */

    /*
     * Find a sub extent map that has only 1 entry, the termination extent
     * descriptor.
     */

    sts = FTX_START_META_XID (
                              FTA_BS_XTNT_REWRITE_MAP_V1,
                              &ftxH,
                              parentFtxH,
                              bfap->dmnP,
                              0,
                              xid
                             );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    /*
     * Make sure that the in-memory extent maps are valid.
     * Return with the mcellList_lk write-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Place the lock returned on our list of locks to release when the
     * transaction ends.
     */
    FTX_ADD_LOCK(&(bfap->mcellList_lk), ftxH)

    switch (bfap->xtnts.type) {

      case BSXMT_APPEND:

        if (xtntMapIndex != 1) {
            RAISE_EXCEPTION (EBAD_PARAMS);
        }
        xtntMap = bfap->xtnts.xtntMap;

        break;

      case BSXMT_STRIPE:

        if ( (xtntMapIndex < 1) ||
             (xtntMapIndex > bfap->xtnts.stripeXtntMap->cnt) ) {
            RAISE_EXCEPTION (EBAD_PARAMS);
        }
        xtntMap = bfap->xtnts.stripeXtntMap->xtntMap[xtntMapIndex - 1];

        break;

      default:
        RAISE_EXCEPTION (ENOT_SUPPORTED);

    }  /* end switch */

    /*
     * Delete the on-disk extent map and re-write it with those mcells that
     * describe storage.  Exclude empty extent mcells.
     */

    sts = x_detach_extent_chain (
                                 bfap,
                                 xtntMap,
                                 ftxH,
                                 &prevVdIndex,
                                 &prevMcellId,
                                 &vdIndex,
                                 &mcellId
                                 );
    if (sts == EOK) {
        bmt_free_bf_mcells (
                            VD_HTOP(vdIndex, domain),
                            mcellId,
                            ftxH,
                            FALSE  /* freeXchain */
                            );
    } else {
        RAISE_EXCEPTION (sts);
    }


    sts = imm_create_xtnt_map (
                               xtntMap->blksPerPage,
                               xtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &newXtntMap
                               );
    if (sts == EOK) {

        for (i = 0; i < xtntMap->validCnt; i++) {

            subXtntMap = &(xtntMap->subXtntMap[i]);
            sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                       subXtntMap->bsXA,
                                       subXtntMap->cnt,
                                       FALSE,  /* copyHoleFlag */
                                       newXtntMap,
                                       bfap->xtnts.type);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }  /* end for */

    } else {
        RAISE_EXCEPTION (sts);
    }

    if (bfap->xtnts.type == BSXMT_STRIPE) {
        if (newXtntMap->cnt == 0) {
            /*
             * The striped bitfile's extent map is empty.  We must maintain
             * a placeholder mcell on the disk that is the extent map's next
             * allocation disk.  The placeholder is needed so that if a disk
             * is going to be removed, the remove code knows that the striped
             * bitfile is interested in the disk.
             */
            if (xtntMap->allocVdIndex != (vdIndexT)(-1)) {
                vdIndex = xtntMap->allocVdIndex;
            } else {
                /*
                 * The striped bitfile is suboptimal.  That is, storage for
                 * multiple segments of the stripe are allocated from the same
                 * disk.  Just select an available disk.
                 */
                sts = sc_select_vd_for_stg( &vdp,
                                            bfap->dmnP,
                                            bfap->reqServices,
                                            bfap->optServices,
                                            0,  /* skipCnt */
                                            NULL,  /* skipVdIndex */
                                            bfap->bfPageSz, /*requestedBlkCnt*/
                                            bfap->bfPageSz,
                                            FALSE); /* ret stgMap_lk unlocked */
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }

                vdIndex = vdp->vdIndex;
            }

            sts = imm_init_sub_xtnt_map (
                                         &(newXtntMap->subXtntMap[0]),
                                         0,  /* pageOffset */
                                         0,  /* pageCnt */
                                         vdIndex,
                                         bsNilMCId,
                                         BSR_SHADOW_XTNTS,
                                         BMT_SHADOW_XTNTS,
                                         0  /* force default value for maxCnt */
                                         );
            if (sts == EOK) {
                newXtntMap->cnt++;
            } else {
                RAISE_EXCEPTION (sts);
            }
        }
    }

    if (newXtntMap->cnt >= 1) {

        /*
         * The extent map has at least one extent descriptor that describes
         * storage or is an empty striped extent map.  Save the in-mem map
         * on disk.
         */

        for (i = 0; i < newXtntMap->cnt; i++) {

            subXtntMap = &(newXtntMap->subXtntMap[i]);
            sts = stg_alloc_new_mcell (
                                       bfap,
                                       bfSetTag,
                                       bfap->tag,
                                       subXtntMap->vdIndex,
                                       ftxH,
                                       &(subXtntMap->mcellId),
                                       TRUE
                                       );
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            subXtntMap->mcellState = NEW_MCELL;
            subXtntMap->updateStart = 0;
            subXtntMap->updateEnd = subXtntMap->cnt - 1;

        }  /* end for */

        newXtntMap->updateStart = 0;
        newXtntMap->updateEnd = newXtntMap->cnt - 1;

        newXtntMap->hdrType =
                        newXtntMap->subXtntMap[newXtntMap->updateStart].type;
        newXtntMap->hdrVdIndex =
                        newXtntMap->subXtntMap[newXtntMap->updateStart].vdIndex;
        newXtntMap->hdrMcellId =
                        newXtntMap->subXtntMap[newXtntMap->updateStart].mcellId;
        newXtntMap->allocVdIndex = xtntMap->allocVdIndex;

        sts = x_create_shadow_rec (
                                   bfap,
                                   newXtntMap,
                                   prevVdIndex,
                                   prevMcellId,
                                   ftxH,
                                   FALSE
                                   );
        if (sts == EOK) {
            for (i = 0; i < newXtntMap->cnt; i++) {
                subXtntMap = &(newXtntMap->subXtntMap[i]);
                subXtntMap->updateStart = subXtntMap->cnt;
            }  /* end for */
        } else {
            RAISE_EXCEPTION (sts);
        }
    } else {
        /*
         * The extent map has no extent descriptors that describe storage.
         * Don't save the in-mem map on disk.  No need to burn an mcell.
         */
        sts = imm_init_sub_xtnt_map (
                                     &(newXtntMap->subXtntMap[0]),
                                     0,  /* pageOffset */
                                     0,  /* pageCnt */
                                     bsNilVdIndex,
                                     bsNilMCId,
                                     BSR_SHADOW_XTNTS,
                                     BMT_SHADOW_XTNTS,
                                     0  /* maxCnt - take default */
                                     );
        if (sts == EOK) {
            newXtntMap->cnt = 1;
            newXtntMap->validCnt = newXtntMap->cnt;
            newXtntMap->updateStart = newXtntMap->validCnt;
            newXtntMap->hdrType = newXtntMap->subXtntMap[0].type;
            newXtntMap->hdrVdIndex = newXtntMap->subXtntMap[0].vdIndex;
            newXtntMap->hdrMcellId = newXtntMap->subXtntMap[0].mcellId;
        } else {
            RAISE_EXCEPTION (sts);
        }
    }

    if ( vdp != NULL ) {
        vd_dec_refcnt(vdp);
    }

    FTX_LOCKWRITE(&(bfap->xtntMap_lk), ftxH)

    /*
     * Switch the new and the old sub extent maps.
     */

    imm_delete_sub_xtnt_maps (xtntMap);
    xtntMap->hdrType = newXtntMap->hdrType;
    xtntMap->hdrVdIndex = newXtntMap->hdrVdIndex;
    xtntMap->hdrMcellId = newXtntMap->hdrMcellId;
    xtntMap->cnt = newXtntMap->cnt;
    xtntMap->maxCnt = newXtntMap->maxCnt;
    xtntMap->updateStart = xtntMap->cnt;
    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->subXtntMap = newXtntMap->subXtntMap;

    newXtntMap->subXtntMap = NULL;
    imm_delete_xtnt_map (newXtntMap);

    failFtxFlag = 0;
    ftx_done_n (ftxH, FTA_BS_XTNT_REWRITE_MAP_V1);

    return EOK;

HANDLE_EXCEPTION:

    if ( vdp != NULL ) {
        vd_dec_refcnt(vdp);
    }
    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }
    if (newXtntMap != NULL) {
        imm_delete_xtnt_map (newXtntMap);
    }

    return sts;

}  /* end odm_rewrite_xtnt_map */

/*
 * x_update_ondisk_xtnt_map
 *
 * This function saves the modified contents of an in-memory extent map
 * in the corresponding on-disk extent map mcells.
 *
 * Before calling this function, the caller must set the in-memory extent
 * map's "updateStart" and "updateEnd" fields.  These fields specify which
 * in-memory sub extent map entries to save.
 *
 * Also, the caller must set each modified sub extent map's "updateStart"
 * and "updateEnd" fields.  These fields specify which extent descriptor
 * array entries to save.
 *
 * If updateStart's value is equal to the number of array entries, the sub
 * extent map was not modified and nothing is saved.  Typically, this type
 * of entry is followed by a new sub extent map.  The unmodified entry's
 * mcell is needed because it will point to the new entry's mcell.
 *
 * If the first sub extent map entry is new, this function tests the
 * entry's type.  If it is BSR_SHADOW_XTNTS, this function assumes the
 * bitfile is not a reserved bitfile and the bitfile does not have any
 * storage allocated to it.  This function creates a shadow extent record
 * and inserts the mcell onto the head of the chain mcell list.
 *
 * If a sub extent map entry is new and is not the first sub extent map, the
 * previous sub extent map entry must exist.  This is necessary so that the
 * previous entry's mcell can point to the new entry's mcell.
 *
 * The thread must synchronize access to the on-disk extent map.
 *
 * NOTE:
 * 
 * The subXtntMap->mcellState inidicates which actions to perform.
 *
 * INITIAILIZED - Mcell already linked properly, and the xtnt rec exists
 *                just update the on-disk xtnt rec.
 * NEW_MCELL - Create on-disk xtnt recorc and link together mcells.
 * 
 * USED_MCELL - The caller previously called bmt_unlink_mcells on this
 *              mcell!!!!! Update the existing xtnt record and link the
 *              mcell on-disk.
 */

statusT
x_update_ondisk_xtnt_map (
                          domainT *domain,  /* in */
                          bfAccessT *bfap,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          ftxHT parentFtx  /* in */
                          )
{
    uint32T i;
    uint32T mcellCnt = 0;
    statusT sts = EOK;
    bsInMemSubXtntMapT *subXtntMap;
    vdIndexT prevVdIndex;
    bfMCIdT prevMcellId;

    /*
     * Update the extent records.
     */

    for (i = xtntMap->updateStart; i <= xtntMap->updateEnd; i++) 
    {
        
        subXtntMap = &(xtntMap->subXtntMap[i]);

        if (subXtntMap->updateStart < subXtntMap->cnt) {

            if ( subXtntMap->mcellState == NEW_MCELL ) {

                /* This subxtnt has a brand new Mcell, we must create its
                 * on-disk xtnt rec.
                 */
                
                sts = odm_create_xtnt_rec( bfap,
                                           xtntMap->allocVdIndex,
                                           subXtntMap,
                                           0,  /* no clone xfer xtnts */
                                           parentFtx );
                if ( sts != EOK ) {
                    return sts;
                }

                if ( subXtntMap->type == BSR_XTRA_XTNTS ) {
                    mcellCnt++;
                }
            }
            else
            {
                /* This is not a new MCELL so just update the xtnt rec on-disk */

                sts = update_xtnt_rec (domain, bfap->tag, subXtntMap, parentFtx);
                if ( sts != EOK ) {
                    return sts;
                }
            }
            if ((subXtntMap->mcellState == NEW_MCELL) ||
                (subXtntMap->mcellState == USED_MCELL))
            {
                subXtntMap->mcellState = INITIALIZED;
                    
                /* Test to see if the subxtnt we are updating is 
                 * a shadow in which case our previous mcell is
                 * the primary.
                 */

                if (subXtntMap->type != BSR_XTRA_XTNTS) 
                {
                    
                    /* Currently we can not tolerate being called with a
                     * BSR_XTNTS descriptor that needs to be created
                     */
                    
                    MS_SMP_ASSERT(subXtntMap->type != BSR_XTNTS);

                    prevVdIndex = bfap->primVdIndex;
                    prevMcellId = bfap->primMCId;
                }
                else
                {
                    /* Test to see if the subxtnt we are updating is the
                     * first on the update list. If it is then we are creating
                     * the MCELL for it and must link it in at the origStart.
                     * otherwise just link the list to the previous one on the
                     * update list.
                     */

                    if (i == xtntMap->updateStart) 
                    {
                        prevVdIndex = xtntMap->subXtntMap[xtntMap->origStart - 1].vdIndex;
                        prevMcellId = xtntMap->subXtntMap[xtntMap->origStart - 1].mcellId;
                    }
                    else
                    {
                        prevVdIndex = xtntMap->subXtntMap[i - 1].vdIndex;
                        prevMcellId = xtntMap->subXtntMap[i - 1].mcellId;
                    }
                }

                sts = bmt_link_mcells (
                                       domain,
                                       bfap->tag,
                                       prevVdIndex,
                                       prevMcellId,
                                       subXtntMap->vdIndex,
                                       subXtntMap->mcellId,
                                       subXtntMap->vdIndex,
                                       subXtntMap->mcellId,
                                       parentFtx
                                      );

                if (sts != EOK) {
                    return sts;
                }
            }
        }
    }  /* end for */

    if (mcellCnt > 0) {
        sts = update_mcell_cnt (
                                domain,
                                bfap->tag,
                                xtntMap->hdrVdIndex,
                                xtntMap->hdrMcellId,
                                xtntMap->hdrType,
                                mcellCnt,
                                parentFtx
                                );
    }

    return sts;

}  /* end x_update_ondisk_xtnt_map */


/* 
 * update_xtnt_rec
 *
 * This function updates the on-disk extent record by copying the
 * modified portion of the corresponding in-memory sub extent map
 * to the record.
 */

statusT
update_xtnt_rec ( 
                 domainT *domain,  /* in */
                 bfTagT bfTag, /* in */
                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                 ftxHT parentFtx  /* in */
                 )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16T *bsXACnt;
    int failFtxFlag = 0;
    ftxHT ftx;
    uint16T i;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    bsShadowXtntT *shadowRec;
    statusT sts;
    updXtntRecUndoRecT undoRec;
    int undoRecSize;
    vdT *vd;
    vdIndexT vdIndex;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    struct bfAccess *mdap;

    vdIndex = subXtntMap->vdIndex;
    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(vdIndex, domain);

    sts = FTX_START_META(FTA_BS_XTNT_UPD_REC_V1, &ftx, parentFtx, vd->dmnP, 1);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    if ( (RBMT_THERE(domain)) && (BS_BFTAG_RSVD(bfTag)) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, mcellId.page, BS_NIL, ftx );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    mcell = &(bmt->bsMCA[mcellId.cell]);

    switch (subXtntMap->type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = &(xtntRec->firstXtnt.xCnt);
        bsXA = xtntRec->firstXtnt.bsXA;

        break;

      case BSR_SHADOW_XTNTS:

        shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, vd->dmnP);
        if (shadowRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = &(shadowRec->xCnt);
        bsXA = shadowRec->bsXA;

        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = xtraXtntRec->bsXA;

        break;

      default:

        ADVFS_SAD0("update_xtnt_rec: bad extent record type");
        
    }  /* end switch */

    undoRec.type = subXtntMap->type;
    undoRec.vdIndex = vdIndex;
    undoRec.mCId = mcellId;
    undoRec.bfTag = bfTag;
    undoRec.xCnt = *bsXACnt;
    undoRec.index = subXtntMap->updateStart;
    undoRec.cnt = (subXtntMap->updateEnd - subXtntMap->updateStart) + 1;

    MS_SMP_ASSERT(subXtntMap->updateEnd >= subXtntMap->updateStart);

    for (i = 0; i < undoRec.cnt; i++) {
        undoRec.bsXA.xtnt[i] = bsXA[undoRec.index + i];
    }  /* end for */

    undoRecSize = (char*)(&undoRec.bsXA.xtnt[undoRec.cnt]) - (char*)(&undoRec);

    update_xtnt_array (pgPin, subXtntMap, bsXACnt, bsXA);

    ftx_done_u (ftx, FTA_BS_XTNT_UPD_REC_V1, undoRecSize, &undoRec);

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ftx_fail (ftx);
    }

    return sts;

}  /* end update_xtnt_rec */

/*
 * update_xtnt_array
 *
 * This function updates an on-disk extent descriptor array by copying the
 * modified portion of the corresponding in-memory extent descriptor array
 * to the on-disk array.
 */

static
void
update_xtnt_array (
                   rbfPgRefHT pgPin,  /* in */
                   bsInMemSubXtntMapT *subXtntMap,  /* in */
                   uint16T *bsXACnt,  /* in */
                   bsXtntT *bsXA  /* in */
                   )
{
    uint16T i;
    uint16T xtntCnt;

    /*
     * Save the modified portion of the in-memory extent descriptor array
     * in the on-disk extent descriptor array.
     */

    xtntCnt = (subXtntMap->updateEnd - subXtntMap->updateStart) + 1;
    rbf_pin_record( pgPin,
                    &(bsXA[subXtntMap->updateStart]),
                    sizeof (bsXtntT) * xtntCnt);

    for (i = subXtntMap->updateStart; i <= subXtntMap->updateEnd; i++) {

        bsXA[i] = subXtntMap->bsXA[i];

    }  /* end for */

    rbf_pin_record (pgPin, bsXACnt, sizeof (*bsXACnt));
    *bsXACnt = subXtntMap->cnt;

    return;

}  /* end update_xtnt_array */

/*
 * odm_create_xtnt_rec
 *
 * This function initializes an extent record in the sub extent map's
 * mcell and copies the extent descriptors from the in-memory map to
 * the record.
 *
 * xferFlg is used to construct a pseudo prime mcell for insertion
 * onto the free list. xferFlg can have one of 3 values:
 *  0  - There is no cloneset. Delete the storage
 *  -1 - This storage is going to a non-striped clone
 *  n  - This storage is going to stripe (n-1) in a striped clone
 */

statusT
odm_create_xtnt_rec (
                     bfAccessT *bfap,  /* in */
                     vdIndexT allocVdIndex,  /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     int xferFlg,  /* in */
                     ftxHT parentFtx  /* in */
                     )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16T *bsXACnt;
    ftxHT ftx;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    bsShadowXtntT *shadowRec;
    bsBfAttrT *odattrp;
    struct bfAccess *mdap;
    creXtntRecUndoRecT undoRec;

    vdIndex = subXtntMap->vdIndex;
    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(vdIndex, bfap->dmnP);

    sts = FTX_START_META(FTA_BS_XTNT_CRE_REC, &ftx, parentFtx, vd->dmnP, 1);
    if (sts != EOK) {
        return sts;
    }

    if ( (RBMT_THERE(bfap->dmnP)) && (BS_BFTAG_RSVD(bfap->tag)) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, mcellId.page, BS_NIL, ftx );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }
    mcell = &(bmt->bsMCA[mcellId.cell]);

    switch (subXtntMap->type) {
      case BSR_XTNTS:
        odattrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), mcell);
        if (odattrp == 0) {
            ftx_fail (ftx);
            return ENO_XTNTS;
        }

        /* pin mcell record */
        rbf_pin_record(pgPin, mcell->bsMR0, sizeof(mcell->bsMR0));

        /* set on-disk attributes */
        odattrp->state = BSRA_VALID;
        /* This is the case where we give truncated storage to the clone. */
        if ( xferFlg ) {
            odattrp->cloneId = 1;
        } else {
            odattrp->cloneId = 0;
        }

        xtntRec = bmtr_assign(BSR_XTNTS, sizeof(struct bsXtntR), mcell);
        if (xtntRec == 0) {
            ftx_fail (ftx);
            return ENO_XTNTS;
        }

        xtntRec->type = bfap->xtnts.type;
        xtntRec->chainVdIndex = bsNilVdIndex;
        xtntRec->chainMCId = bsNilMCId;
        /* If positive, this is the stripe index+1 this stg belongs to. */
        /* If negaitive, this is not a striped file. rsvd1 will not be used. */
        xtntRec->rsvd1 = xferFlg;
        xtntRec->rsvd2 = bsNilMCId;
        xtntRec->blksPerPage = ADVFS_PGSZ_IN_BLKS;
        bzero((char *)&xtntRec->delRst, sizeof(delRstT));

        if ( xtntRec->type == BSXMT_STRIPE ) {
            xtntRec->segmentSize = bfap->xtnts.stripeXtntMap->segmentSize;
        } else {
            xtntRec->segmentSize = 0;
        }

        /*
         * If there is extent information in the primary mcell,
         * we need to copy in that extent information.
         */
        if (FIRST_XTNT_IN_PRIM_MCELL(bfap->dmnP->dmnVersion,
                                     bfap->xtnts.type)) {
            /* 
             * No need to pin records; the above call to
             * rbf_pin_record() has pinned the entire mcell.
             */
            xtntRec->firstXtnt.mcellCnt = 1;
            bsXACnt = &(xtntRec->firstXtnt.xCnt);
            bsXA = &(xtntRec->firstXtnt.bsXA[0]);
            break;
        }

        ftx_done_u (ftx, FTA_BS_XTNT_CRE_REC, 0, NULL);
        return EOK;

      case BSR_XTRA_XTNTS:
        xtraXtntRec = bmtr_assign_rec (
                                       BSR_XTRA_XTNTS,
                                       sizeof (bsXtraXtntRT),
                                       mcell,
                                       pgPin
                                       );
        if (xtraXtntRec == NULL) {
            ftx_fail (ftx);
            return ENO_XTNTS;
        }

        rbf_pin_record( pgPin,
                        &(xtraXtntRec->blksPerPage),
                        sizeof (xtraXtntRec->blksPerPage));
        xtraXtntRec->blksPerPage = ADVFS_PGSZ_IN_BLKS;

        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = &(xtraXtntRec->bsXA[0]);

        break;

      case BSR_SHADOW_XTNTS:
        shadowRec = bmtr_assign_rec (
                                     BSR_SHADOW_XTNTS,
                                     sizeof (bsShadowXtntT),
                                     mcell,
                                     pgPin
                                     );
        if (shadowRec == NULL) {
            ftx_fail (ftx);
            return ENO_XTNTS;
        }

        rbf_pin_record( pgPin,
                        &(shadowRec->allocVdIndex),
                        sizeof (shadowRec->allocVdIndex));
        shadowRec->allocVdIndex = allocVdIndex;

        rbf_pin_record( pgPin,
                        &(shadowRec->mcellCnt),
                        sizeof(shadowRec->mcellCnt));
        shadowRec->mcellCnt = 1;

        rbf_pin_record( pgPin,
                        &(shadowRec->blksPerPage),
                        sizeof (shadowRec->blksPerPage));
        shadowRec->blksPerPage = ADVFS_PGSZ_IN_BLKS;

        bsXACnt = &(shadowRec->xCnt);
        bsXA = &(shadowRec->bsXA[0]);

        break;

      default:
        ADVFS_SAD0("odm_create_xtnt_rec: bad extent record type");
        
    }  /* end switch */

    subXtntMap->updateStart = 0;
    subXtntMap->updateEnd = subXtntMap->cnt - 1;
    update_xtnt_array (pgPin, subXtntMap, bsXACnt, bsXA);

    /*
     * Undo is not necessary for most mcells because we are 
     * initializing the first record in a new mcell.  When we 
     * undo the mcell allocation the record will be deleted.  
     * Undo is necessary for  RBMT cell 27 only.  The RBMT 
     * mcell 27 is pre-allocated and will not be freed.  
     * Therefore its record will not be cleared.  The undo
     * will clear the record.
     */

    if ( RBMT_THERE(bfap->dmnP) && 
         BS_BFTAG_RSVD(bfap->tag) &&
         subXtntMap->mcellId.cell == RBMT_RSVD_CELL &&
         subXtntMap->type == BSR_XTRA_XTNTS ) {                         
        undoRec.vdIndex = subXtntMap->vdIndex;
        undoRec.mcellId = subXtntMap->mcellId;
        ftx_done_u (ftx, FTA_BS_XTNT_CRE_REC, sizeof(creXtntRecUndoRecT), &undoRec );
    } else {
        ftx_done_u (ftx, FTA_BS_XTNT_CRE_REC, 0, NULL );
    }

    return EOK;

}  /* end odm_create_xtnt_rec */

/*
 * x_create_shadow_rec
 *
 * This function creates an on-disk shadow record for the specified
 * in-memory extent map.  If the previous mcell pointer is nil, it
 * inserts the shadow record mcell at the head of the bitfile's extent
 * mcell list.  Otherwise, it inserts the new shadow record mcell after
 * the specified previous mcell.
 *
 * The caller of this function must own the bitfile's mcell list lock.
 */

statusT
x_create_shadow_rec (
                     bfAccessT *bfap,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     vdIndexT prevVdIndex,  /* in */
                     bfMCIdT prevMcellId,  /* in */
                     ftxHT parentFtxH, /* in */
                     int striping_file /* in - TRUE if activating striping */
                     )
{
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;

    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    /*
     * Files with extent information in the primary mcell should not
     * have shadow records.  Files that are in the process of being
     * striped temporarily look like that and so are excluded from
     * the check.
     */
    MS_SMP_ASSERT(striping_file || 
                  !FIRST_XTNT_IN_PRIM_MCELL(bfap->dmnP->dmnVersion,
                                            bfap->xtnts.type));

    /*
     * This statement assumes that the extent map's first update sub
     * extent map entry describes a shadow extent record.
     */
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);

    if ((prevVdIndex == bsNilVdIndex) &&
        (prevMcellId.page == bsNilMCId.page) &&
        (prevMcellId.cell == bsNilMCId.cell)) {

        /*
         * The previous mcell pointer is nil.  Insert the shadow record mcell
         * at the head of the extent mcell chain.
         */

        prevVdIndex = bfap->primVdIndex;
        prevMcellId = bfap->primMCId;
    } 

    sts = bmt_link_mcells (
                           bfap->dmnP,
                           bfap->tag,
                           prevVdIndex,
                           prevMcellId,
                           subXtntMap->vdIndex,
                           subXtntMap->mcellId,
                           subXtntMap->vdIndex,
                           subXtntMap->mcellId,
                           parentFtxH
                          );
    if (sts != EOK) {
        return sts;
    }

    sts = odm_create_xtnt_rec (
                               bfap,
                               xtntMap->allocVdIndex,
                               subXtntMap,
                               0,  /* no clone xfer xtnts */
                               parentFtxH
                               );
    if (sts == EOK) {

        subXtntMap->mcellState = INITIALIZED;
        subXtntMap->updateStart = subXtntMap->cnt;

        if ((xtntMap->updateStart + 1) <= xtntMap->updateEnd) {
            sts = x_update_ondisk_xtnt_map (
                                            bfap->dmnP,
                                            bfap,
                                            xtntMap,
                                            parentFtxH
                                            );
        }
    }

    return sts;

}  /* end x_create_shadow_rec */

/*
 * x_detach_extent_chain
 *
 * This function unlinks the on-disk extent records from a file's primary mcell.
 * 
 * This function returns the mcell pointer of the previous mcell that
 * points to the removed shadow record mcell.  If the first extent record
 * mcell is at the head of the extent chain, the previous mcell is
 * nil.
 *
 * The caller of this function must own the bitfile's mcell list lock
 * and free the mcell chain pointed to by retfreedVdIndex and
 * retfreedMcellId.
 */

statusT
x_detach_extent_chain (
                       bfAccessT *bfap,       /* in */
                       bsInMemXtntMapT *xtntMap,  /* in */
                       ftxHT parentFtxH,          /* in */
                       vdIndexT *retPrevVdIndex,  /* out */
                       bfMCIdT *retPrevMcellId,   /* out */
                       vdIndexT *retfreedVdIndex, /* out */
                       bfMCIdT *retfreedMcellId   /* out */
                       )
{
    bsMPgT *bmt;
    bfMCIdT chainMcellId;
    vdIndexT chainVdIndex;
    bsMCT *mcell;
    bfPageRefHT pgRef;
    bfMCIdT prevMcellId;
    vdIndexT prevVdIndex;
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtntT primXtntSave[2], *tempXtntp;
    bsInMemSubXtntMapT *tempSubXtntp;
    uint32T cntSave, updateStartSave, updateEndSave;
    uint32T i;
    uint32T starting_index;

    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    /*
     * Get the primary extent record's extent mcell chain pointer.
     */
    vd = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     vd->bmtp,
                     bfap->primMCId.page,
                     BS_NIL);
    if (sts != EOK) {
        return sts;
    }

    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        bs_derefpg (pgRef, BS_CACHE_IT);
        return (ENO_XTNTS);
    }

    chainVdIndex = xtntRec->chainVdIndex;
    chainMcellId = xtntRec->chainMCId;

    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        return sts;
    }

    /*
     * For files which have extent information stored in the primary mcell, 
     * invalidate the extent information in the primary mcell.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(bfap->dmnP->dmnVersion,
                                 bfap->xtnts.type)) {
        
        /*
         * The semantics of this routine are to modify the on-disk mcell
         * chain but to leave the extent maps unchanged.  When the file
         * has a BSR_XTNTS record, we temporarily zap the in-memory
         * fields just so that update_xtnt_rec() does the right thing.
         * After that call, we put the extent map back to the way it
         * was in memory.
         */
        XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk);
        tempSubXtntp = &xtntMap->subXtntMap[0];
        tempXtntp = &tempSubXtntp->bsXA[0];
        cntSave = tempSubXtntp->cnt;

        for ( i = 0; i < cntSave; i++ ) {
            primXtntSave[i] = *(tempXtntp + i);
        }

        updateStartSave = tempSubXtntp->updateStart;
        updateEndSave = tempSubXtntp->updateEnd;

        tempSubXtntp->cnt = 1;
        tempSubXtntp->updateStart = 0;
        tempSubXtntp->updateEnd = 0;
        tempXtntp->bsPage = 0;
        tempXtntp->vdBlk = XTNT_TERM;
        sts = update_xtnt_rec( bfap->dmnP,
                               bfap->tag,
                               tempSubXtntp,
                               parentFtxH);
        for ( i = 0; i < cntSave; i++ ) {
            *(tempXtntp + i) = primXtntSave[i];
        }
        tempSubXtntp->cnt = cntSave;
        tempSubXtntp->updateStart = updateStartSave;
        tempSubXtntp->updateEnd = updateEndSave;
        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
        
        /*
         * If there is no chain off the primary mcell, we're done.
         */
        if ((chainVdIndex == bsNilVdIndex) || 
            (MCID_EQL(chainMcellId, bsNilMCId))) {
            if (!FIRST_XTNT_IN_PRIM_MCELL(bfap->dmnP->dmnVersion,
                                          bfap->xtnts.type)) {
                domain_panic(bfap->dmnP, "Corrupted extent chain");
                sts = E_DOMAIN_PANIC;
                return sts;
            }

            *retPrevVdIndex = bsNilVdIndex;
            *retPrevMcellId = bsNilMCId;
            *retfreedVdIndex = bsNilVdIndex;
            *retfreedMcellId = bsNilMCId;
            return (sts);
        }

        prevVdIndex = bfap->primVdIndex;
        prevMcellId = bfap->primMCId;
        *retPrevVdIndex = bsNilVdIndex;
        *retPrevMcellId = bsNilMCId;
        starting_index=1;
    }
    else if ((xtntMap->hdrVdIndex == chainVdIndex) &&
             (MCID_EQL(xtntMap->hdrMcellId, chainMcellId)))
    {
        prevVdIndex = bfap->primVdIndex;
        prevMcellId = bfap->primMCId;
        *retPrevVdIndex = bsNilVdIndex;
        *retPrevMcellId = bsNilMCId;
        starting_index=0;

    } 
    else 
    {
        sts = bmt_find_mcell (
                              bfap->dmnP,
                              xtntMap->hdrVdIndex,
                              xtntMap->hdrMcellId,
                              chainVdIndex,
                              chainMcellId,
                              &prevVdIndex,
                              &prevMcellId
                              );
        if (sts != EOK) {
            return sts;
        }

        starting_index=0;
        *retPrevVdIndex = prevVdIndex;
        *retPrevMcellId = prevMcellId;
    }

    sts = bmt_unlink_mcells ( bfap->dmnP,
                              bfap->tag,
                              prevVdIndex,
                              prevMcellId,
                              xtntMap->subXtntMap[starting_index].vdIndex,
                              xtntMap->subXtntMap[starting_index].mcellId,
                              xtntMap->subXtntMap[xtntMap->cnt - 1].vdIndex,
                              xtntMap->subXtntMap[xtntMap->cnt - 1].mcellId,
                              parentFtxH
                             );
    if (sts != EOK) {
        return sts;
    }

    *retfreedVdIndex = xtntMap->subXtntMap[0].vdIndex;
    *retfreedMcellId = xtntMap->subXtntMap[0].mcellId;

    return EOK;

}  /* end x_detach_extent_chain */

/*
 * update_mcell_cnt
 *
 * This function updates the "mcellCnt" field in an extent record.
 */

statusT
update_mcell_cnt (
                  domainT *domain,  /* in */
                  bfTagT bfTag, /* in */
                  vdIndexT bfVdIndex,  /* in */
                  bfMCIdT bfMcellId,  /* in */
                  uint32T type,  /* in */
                  int32T mcellCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  )
{
    bsMPgT *bmt;
    ftxHT ftx;
    bsMCT *mcell;
    uint16T *mcellCntAddr;
    rbfPgRefHT pgPin;
    bsShadowXtntT *shadowRec;
    statusT sts;
    mcellCntUndoRecT undoRec;
    vdT *vd;
    bsXtntRT *xtntRec;
    struct bfAccess *mdap;

    vd = VD_HTOP(bfVdIndex, domain);

    sts = FTX_START_META(FTA_BS_XTNT_UPD_MCELL_CNT_V1, &ftx, parentFtx, vd->dmnP, 1);
    if (sts != EOK) {
        return sts;
    }

    if ( (RBMT_THERE(domain)) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, bfMcellId.page, BS_NIL, ftx );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }

    mcell = &(bmt->bsMCA[bfMcellId.cell]);

    switch (type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            ftx_fail (ftx);
            return (ENO_XTNTS);
        }
        mcellCntAddr = &(xtntRec->firstXtnt.mcellCnt);

        break;

      case BSR_SHADOW_XTNTS:

        shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, vd->dmnP);
        if (shadowRec == NULL) {
            ftx_fail (ftx);
            return (ENO_XTNTS);
        }
        mcellCntAddr = &(shadowRec->mcellCnt);

        break;

      default:

        ADVFS_SAD0("update_mcell_cnt: bad extent record type");

    }  /* end switch */

    undoRec.type = type;
    undoRec.vdIndex = bfVdIndex;
    undoRec.mCId = bfMcellId;
    undoRec.bfTag = bfTag;
    undoRec.mcellCnt = *mcellCntAddr;

    rbf_pin_record (pgPin, mcellCntAddr, sizeof (*mcellCntAddr));
    *mcellCntAddr +=  mcellCnt;

    ftx_done_u (ftx, FTA_BS_XTNT_UPD_MCELL_CNT_V1, sizeof (undoRec), &undoRec);

    return EOK;

}  /* end update_mcell_cnt */


/*
 * x_create_inmem_xtnt_map
 *
 * This function creates the specified bitfile's in-memory extent map from
 * the bitfile's on-disk extent map.
 *
 * The caller must own the mcellList_lk lock and must exclusively own the
 * xtntMap_lk lock.  Or, have exclusive access to both the on-disk and
 * in-memory extent maps.
 */

statusT
x_create_inmem_xtnt_map (
                         bfAccessT *bfap,  /* in, modified */
                         bsMCT *bfMcellp   /* in */
                         )
{
    statusT sts;
    bsXtntRT *xtntRec;
    vdT *vdp;
    bsInMemXtntT *xtnts = &bfap->xtnts;

    XTNMAP_LOCK_WRITE(&(bfap->xtntMap_lk));

    /*
     * Clear out any old extent maps.
     */
    x_dealloc_extent_maps(xtnts);

    vdp = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
    xtntRec = bmtr_find (bfMcellp, BSR_XTNTS, vdp->dmnP);
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    /* Check the structure, especially chainVdIndex and MCell, to avoid
       KMF if there is disk corruption.
    */

    if ( check_BSR_XTNTS_rec(xtntRec, bfap, vdp) != EOK ) {
        RAISE_EXCEPTION (E_BAD_BMT);
    }

    xtnts->type = xtntRec->type;

    sts = load_inmem_xtnt_map( bfap, xtntRec, &xtnts->allocPageCnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtnts->validFlag = 1;

    bfap->nextPage = imm_get_next_page (xtnts);
        
HANDLE_EXCEPTION:

    XTNMAP_UNLOCK(&(bfap->xtntMap_lk));
    return sts;

}  /* end x_create_inmem_xtnt_map */

/*
 * x_load_inmem_xtnt_map
 *
 * This function loads a bitfile's in-memory extent maps from its
 * on-disk extent maps.
 *
 * This function can modify the extent map pointers and the valid flag
 * in the extent map header.
 *
 * When called, this routine assumes that no locks are held if the
 * X_LOAD_LOCKSOWNED flag is not used.
 * When this routine returns successfully, one of two locks will be
 * held depending on the value of lock_request supplied:
 *  1. X_LOAD_REFERENCE  - xntnMap_lk is returned READ locked.
 *  2. X_LOAD_UPDATE     - mcellListLk is returned WRITE locked.
 *  3. X_LOAD_LOCKSOWNED - all necessary locks are already held.
 * If this function returns unsuccessfully, then there are no locks held
 * when it returns.
 *
 * The current use of the xtntMap_lk and mcellList_lk locks guarantee that
 * once this routine updates the extent maps in memory, they cannot be 
 * modified by another thread until the calling routine drops the lock that
 * is returned locked.
 *
 * Callers should specify X_LOAD_REFERENCE if they need to reference, but not
 * modify the in-memory extent maps, and specify X_LOAD_UPDATE if they
 * intend to modify the extent maps.
 */

statusT
x_load_inmem_xtnt_map (
                       bfAccessT *bfap,      /* in, modified */
                       uint32T lock_request  /* in */
                       )
{
    bsMPgT *bmt;
    int derefFlag = 0;
    bsMCT *mcell;
    bfPageRefHT pgRef;
    statusT sts;
    uint32T totalPageCnt;
    unLkActionT unlock_action;
    vdT *vd;
    bsXtntRT *xtntRec;
    struct bfAccess *mdap;
    domainT *domain = bfap->dmnP;
    bsInMemXtntT *xtnts = &bfap->xtnts;

    /* If lock_request is NOT X_LOAD_LOCKSOWNED, then nothing is locked,
     * but depending on how the caller wants to treat the returned xtnt maps,
     * we will lock the extent maps and return with the requested locks held.
     *
     * If lock_request is X_LOAD_LOCKSOWNED, then we are coming from the
     * ftx_fail() in rbmt_extend(), where we have already taken the necessary
     * locks in the correct order.  This will avoid a lock hierarchy problem.
     */
    MS_SMP_ASSERT(lock_request == X_LOAD_REFERENCE || 
                  lock_request == X_LOAD_UPDATE    ||
                  lock_request == X_LOAD_LOCKSOWNED);
    if (lock_request == X_LOAD_REFERENCE) {
        XTNMAP_LOCK_READ( &(bfap->xtntMap_lk) )
        if (xtnts->validFlag) {
            /* If extents are valid, just return with xtntMap READ locked */
            return EOK;
        } else {

            /* release the READ lock and then seize both locks in 
             * anticipation of reloading the extent maps; the mcellList lock
             * get read-locked to prevent racing modifieres, while the 
             * xtntMap lock gets write-locked.  Check to see if we got to 
             * valid in the meantime, however.
             */
            XTNMAP_UNLOCK( &(bfap->xtntMap_lk))  
            lock_read( &(bfap->mcellList_lk.lock) );
            XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )
            if (xtnts->validFlag) {
                /* If extents are now valid, release  mcellList lock, and 
                 * downgrade the xtntMap to a READ lock.
                 */
                XTNMAP_LOCK_DOWNGRADE(&(bfap->xtntMap_lk))  
                MCELLIST_UNLOCK(&(bfap->mcellList_lk))
                return EOK;
            }
            /* otherwise fall thru and reload the extent maps */
        }
    } else if (lock_request == X_LOAD_UPDATE ) {
        /* UPDATE locking was requested, so WRITE lock both locks.  */
        MCELLIST_LOCK_WRITE( &(bfap->mcellList_lk) )
        XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )
        if (xtnts->validFlag) {
            /* If extents are valid, just return with mcellList locked */
            XTNMAP_UNLOCK( &(bfap->xtntMap_lk))  
            return EOK;
        }
        /* otherwise fall thru and reload the extent maps */
    } else {
        /* This is the rare case: lock_request=X_LOAD_LOCKSOWNED.
         * If the extents are valid, return.  Otherwise fall through
         * and reload them.
         */
        if (xtnts->validFlag) {
            /* If extents are valid, just return */
            return EOK;
        }
        /* otherwise fall thru and reload the extent maps */
    }

    /* If we get here, we have to reload the extent maps from disk; we have
     * WRITE lock held on the xtntMap lock and either a read or write lock
     * on the mcellList_lk depending on the value of lock_request.
     */

    vd = VD_HTOP(bfap->primVdIndex, domain);

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfap->tag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bmt_refpg ( &pgRef, (void*)&bmt, mdap, bfap->primMCId.page, BS_NIL );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefFlag = 1;

    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);
    if ( check_mcell_hdr(mcell, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }
    if ( CheckXtnts && check_BSR_XTNTS_rec(xtntRec, bfap, vd) != EOK ) {
        RAISE_EXCEPTION (E_BAD_BMT);
    }

    sts = load_inmem_xtnt_map( bfap, xtntRec, &totalPageCnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    derefFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtnts->validFlag = 1;

    /* If caller only wants to reference the extent maps, release the mcellList
     * lock and downgrade the xtntMap lock to a read lock.  Otherwise,
     * return with just the mcellList lock held.
     */
    if (lock_request == X_LOAD_REFERENCE) {
        XTNMAP_LOCK_DOWNGRADE(&(bfap->xtntMap_lk))  
        MCELLIST_UNLOCK(&(bfap->mcellList_lk))   /* was read-locked */
    } else if (lock_request == X_LOAD_UPDATE) {
        /* McellList_lk is already write-locked in this case */
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk))
    }
    /* else if (lock_request == X_LOAD_LOCKSOWNED)
     *    we don't release either lock.
     */

    return sts;

HANDLE_EXCEPTION:

    /*
     * On failure, return with no locks held, unless X_LOAD_LOCKSOWNED is set.
     * When X_LOAD_LOCKSOWNED is set, rbmt_extend will release mcellList_lk
     * after a return from link_unlink_mcells_undo.  link_unlink_mcells_undo is
     * the only routine that calls here with X_LOAD_LOCKSOWNED set.
     */
    if (lock_request != X_LOAD_LOCKSOWNED) {
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk))
        MCELLIST_UNLOCK(&(bfap->mcellList_lk));
    }

    if (derefFlag != 0) {
        (void) bs_derefpg (pgRef, BS_CACHE_IT);  /* Ignore status */
    }

    return sts;

}  /* end x_load_inmem_xtnt_map */

/*
 * load_inmem_xtnt_map
 *
 * This function loads a bitfile's in-memory extent maps from its on-disk
 * extent maps.
 *
 * This function does not modify any fields in the extent map header other than
 * the extent map pointers.
 *
 * The caller must own the mcellList_lk lock and must exclusively own the
 * xtntMap_lk lock.  Or, have exclusive access to both the on-disk and
 * in-memory extent maps. 
 */

static
statusT
load_inmem_xtnt_map (
                     bfAccessT *bfap,          /* in, modified */
                     bsXtntRT *xtntRec,        /* in */
                     uint32T *retTotalPageCnt  /* out */
                     )
{
    int i;
    bfMCIdT mcellId;
    bfMCIdT nextMcellId;
    vdIndexT nextVdIndex;
    bsInMemXtntMapT *nextXtntMap;
    uint32T pageCnt;
    bsInMemXtntMapT *prevXtntMap;
    int segmentCnt;
    statusT sts;
    uint32T totalPageCnt;
    vdIndexT vdIndex;
    bsInMemXtntMapT *xtntMap;
    domainT *domain = bfap->dmnP;
    bfTagT bfTag = bfap->tag;
    vdIndexT bfVdIndex = bfap->primVdIndex;
    bsInMemXtntT *xtnts = &bfap->xtnts;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        if ((!FIRST_XTNT_IN_PRIM_MCELL(domain->dmnVersion, xtnts->type)) &&
            (!BS_BFTAG_RSVD(bfTag))) {
            /*
             * Normal bitfile with no extent information in primary mcell.
             */
            sts = load_from_shadow_rec( bfap,
                                        xtntRec->chainVdIndex,
                                        xtntRec->chainMCId,
                                        &xtnts->xtntMap,
                                        &totalPageCnt,
                                        &nextVdIndex,
                                        &nextMcellId);
            if (sts != EOK) {
                return sts;
            }
        } else {

            sts = load_from_xtnt_rec( bfap,
                                      xtntRec,
                                      &totalPageCnt,
                                      &nextVdIndex,
                                      &nextMcellId);

            if (sts != EOK) {
                return sts;
            }
        }
        MS_SMP_ASSERT(imm_check_xtnt_map(xtnts->xtntMap) == EOK);

        break;

      case BSXMT_STRIPE:

        if ( RBMT_THERE(domain) ) {
            MS_SMP_ASSERT(VD_HTOP(bfVdIndex, domain)->rbmtp != NULL);
        }

        MS_SMP_ASSERT(VD_HTOP(bfVdIndex, domain)->bmtp != NULL);

        sts = load_from_shadow_rec( bfap,
                                    xtntRec->chainVdIndex,
                                    xtntRec->chainMCId,
                                    &(xtnts->xtntMap),
                                    &totalPageCnt,
                                    &nextVdIndex,
                                    &nextMcellId);
        if (sts != EOK) {
            return sts;
        }
        MS_SMP_ASSERT(imm_check_xtnt_map(xtnts->xtntMap) == EOK);

        xtnts->shadowXtntMap = xtnts->xtntMap;
        xtnts->xtntMap = NULL;
        segmentCnt = 1;

        prevXtntMap = xtnts->shadowXtntMap;
        while ((nextVdIndex != bsNilVdIndex) &&
               ((nextMcellId.page != bsNilMCId.page) ||
                (nextMcellId.cell != bsNilMCId.cell))) {

            vdIndex = nextVdIndex;
            mcellId = nextMcellId;
            sts = load_from_shadow_rec( bfap,
                                        vdIndex,
                                        mcellId,
                                        &xtntMap,
                                        &pageCnt,
                                        &nextVdIndex,
                                        &nextMcellId);
            if (sts != EOK) {
                return sts;
            }
            MS_SMP_ASSERT(imm_check_xtnt_map(xtntMap) == EOK);

            totalPageCnt = totalPageCnt + pageCnt;

            prevXtntMap->nextXtntMap = xtntMap;
            prevXtntMap = xtntMap;
            segmentCnt++;

        }  /* end while */

        sts = str_create_stripe_hdr (
                                     segmentCnt,
                                     xtntRec->segmentSize,
                                     &(xtnts->stripeXtntMap)
                                     );
        if (sts != EOK) {
            return sts;
        }

        i = 0;
        xtntMap = xtnts->shadowXtntMap;
        while (xtntMap != NULL) {
            xtnts->stripeXtntMap->xtntMap[i] = xtntMap;
            xtntMap = xtntMap->nextXtntMap;
            xtnts->stripeXtntMap->xtntMap[i]->nextXtntMap = NULL;
            i++;
        }  /* end while */
        xtnts->shadowXtntMap = NULL;

        break;

      default:
        ADVFS_SAD0 ("load_inmem_xtnt_map: bad extent map type");

    }  /* end switch */

    *retTotalPageCnt = totalPageCnt;

    return EOK;

}  /* end load_inmem_xtnt_map */

/*
 * load_from_shadow_rec
 *
 * This function initializes the sub extent map for the specified
 * shadow record and optionally loads the extent descriptors into the
 * sub extent map.  If there are extra extent records, this
 * function calls load_from_xtra_xtnt_rec() for each record.
 */

static
statusT
load_from_shadow_rec (
                      bfAccessT *bfap,              /* in, modified */
                      vdIndexT bfVdIndex,           /* in */
                      bfMCIdT bfMcellId,            /* in */
                      bsInMemXtntMapT **bfXtntMap,  /* out */
                      uint32T *allocPageCnt,        /* out */
                      vdIndexT *bfNextVdIndex,      /* out */
                      bfMCIdT *bfNextMcellId        /* out */
                      )
{
    bsMPgT *bmtp;
    int derefPgFlag = 0;
    bfMCIdT mcellId;
    bsMCT *mcellp;
    bfMCIdT nextMcellId;
    vdIndexT nextVdIndex;
    uint32T pageCnt;
    uint32T pageOffset;
    bfPageRefHT pgRef;
    bsShadowXtntT *shadowRec;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap = NULL;
    uint32T totalPageCnt;
    vdIndexT vdIndex;
    bsInMemXtntMapT *xtntMap = NULL;
    vdT *vdp;
    domainT *dmnP = bfap->dmnP;
    uint32T blksPerPage = bfap->bfPageSz;
    uint32T lastPg;
    uint32T mcellCnt;

    if ((bfVdIndex == bsNilVdIndex) &&
        (bfMcellId.page == bsNilMCId.page) &&
        (bfMcellId.cell == bsNilMCId.cell)) {
        /*
         * The bitfile has no storage allocated to it.
         */
        sts = imm_create_xtnt_map (
                                   blksPerPage,
                                   dmnP,
                                   1,
                                   0,  /* termPage */
                                   bsNilVdIndex,  /* termVdIndex */
                                   &xtntMap
                                   );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        xtntMap->hdrType = BSR_SHADOW_XTNTS;
        xtntMap->hdrVdIndex = bsNilVdIndex;
        xtntMap->hdrMcellId = bsNilMCId;
        xtntMap->allocVdIndex = -1;

        subXtntMap = &(xtntMap->subXtntMap[0]);
        sts = imm_init_sub_xtnt_map (
                                     subXtntMap,
                                     0,  /* pageOffset */
                                     0,  /* pageCnt */
                                     bsNilVdIndex,
                                     bsNilMCId,
                                     BSR_SHADOW_XTNTS,
                                     BMT_SHADOW_XTNTS,
                                     1
                                     );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        xtntMap->cnt = 1;

        xtntMap->validCnt = xtntMap->cnt;
        xtntMap->updateStart = xtntMap->cnt;

        *bfXtntMap = xtntMap;
        *allocPageCnt = 0;
        *bfNextVdIndex = bsNilVdIndex;
        *bfNextMcellId = bsNilMCId;

        return EOK;
    }

    vdp = VD_HTOP(bfVdIndex, dmnP);

    MS_SMP_ASSERT(TEST_PAGE(bfMcellId.page, vdp->bmtp) == EOK);
    sts = bmt_refpg( &pgRef,
                     (void*)&bmtp,
                     vdp->bmtp,
                     bfMcellId.page,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPgFlag = 1;

    MS_SMP_ASSERT(TEST_CELL(bfMcellId.cell) == EOK);
    mcellp = &(bmtp->bsMCA[bfMcellId.cell]);
    if ( check_mcell_hdr(mcellp, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    shadowRec = bmtr_find (mcellp, BSR_SHADOW_XTNTS, vdp->dmnP);
    if (shadowRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }
    if ( CheckXtnts &&
         check_BSR_SHADOW_XTNTS_rec(shadowRec, bfap, vdp) != EOK )
    {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    mcellCnt = shadowRec->mcellCnt;
    sts = imm_create_xtnt_map (
                               shadowRec->blksPerPage,
                               dmnP,
                               mcellCnt,
                               0,
                               bfVdIndex,
                               &xtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->hdrType = BSR_SHADOW_XTNTS;
    xtntMap->hdrVdIndex = bfVdIndex;
    xtntMap->hdrMcellId = bfMcellId;
    xtntMap->allocVdIndex = shadowRec->allocVdIndex;

    subXtntMap = &(xtntMap->subXtntMap[0]);
    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0,  /* pageOffset */
                                 0,  /* pageCnt */
                                 bfVdIndex,
                                 bfMcellId,
                                 BSR_SHADOW_XTNTS,
                                 BMT_SHADOW_XTNTS,
                                 shadowRec->xCnt
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = load_from_xtnt_array (
                                shadowRec->xCnt,
                                shadowRec->bsXA,
                                subXtntMap
                                );
    if (sts != EOK) {
        if (sts == ENO_XTNTS) {
            domain_panic (dmnP, "load_from_xtnt_array called from load_from_shadow_rec: cnt must be non-zero");
            sts = E_DOMAIN_PANIC;
        }
        RAISE_EXCEPTION (sts);
    }

    xtntMap->cnt = 1;

    nextVdIndex = mcellp->nextVdIndex;
    nextMcellId = mcellp->nextMCId;

    /* We're done with the shadow mcell */
    derefPgFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Extra extent records.
     */
    subXtntMap = xtntMap->subXtntMap;

    while ( nextVdIndex != bsNilVdIndex ) {
        if ( xtntMap->cnt == mcellCnt ) {
            /* We must test for shadowRec->mcellCnt wrap. Look at next mcell. */
            /*
            ** Striped files have extent mcell chains for each stripe linked
            ** one after the other. The BSR_XTNTS record in the primary mcell
            ** does not contain any extent records. It does contain the chain
            ** pointer to the first BSR_SHADOW_XTNTS mcell. This record and
            ** any following BSR_XTRA_XTNTS records consitute the first stripe.
            ** The mcellCnt in the first BSR_SHADOW_XTNTS record counts the
            ** mcells in the first stripe (one BSR_SHADOW_XTNTS mcell plus
            ** zero or more BSR_XTRA_XTNTS mcells). The next pointer in the
            ** last mcell in the first stripe points to the BSR_SHADOW_XTNTS
            ** record that starts the next stripe. The last stripe ends when
            ** the next pointer is zero.  The number of stripes is  determined
            ** just by counting the number of BSR_SHADOW_XTNTS records in the
            ** mcell extent chain.
            ** The mcellCnt in the BSR_SHADOW_XTNTS record that starts each
            ** stripe counts the extent mcells in the that stripe.  Since
            ** the mcellCnt is a short, it will wrap if there are more than
            ** 64K mcells in a stripe.
            ** If the mcellCnt has not wrapped, then when we have read mcellCnt
            ** mcells, and if there is a next mcell, we expect it to contain a
            ** BSR_SHADOW_XTNTS record, indicating the start of a new stripe.
            ** But if the mcellCnt has wrapped, then the next mcell will
            ** contain a BSR_XTRA_XTNTS record.
            ** At this point the we have read mcellCnt extent records in this
            ** stripe. We don't know yet if the mcellCnt has wrapped. We do
            ** know there are more mcells in the chain, but we don't know it
            ** the next mcell is a continuation of the current stripe (in
            ** which case it will contain a BSR_XTRA_XTNTS record) or the start
            ** of a new stripe (it will caontain a BSR_SHADOW_XTNTS record).
            ** We need to peek at the record in the next mcell to see what kind
            ** of record it contains. If it contains a BSR_SHADOW_XTNTS record
            ** we are done with this stripe. We need to break out of this loop
            ** and return to the caller. If it contains a BSR_XTRA_XTNTS
            ** record, the stripe continues for (a multiple of) 64K more mcells.
            */
            vdp = VD_HTOP(nextVdIndex, dmnP);
            MS_SMP_ASSERT(TEST_PAGE(nextMcellId.page, vdp->bmtp) == EOK);
            sts = bmt_refpg( &pgRef,
                             (void*)&bmtp,
                             vdp->bmtp,
                             nextMcellId.page,
                             BS_NIL);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            derefPgFlag = 1;

            MS_SMP_ASSERT(TEST_CELL(bfMcellId.cell) == EOK);
            mcellp = &(bmtp->bsMCA[nextMcellId.cell]);
            if ( check_mcell_hdr(mcellp, bfap) != EOK ) {
                RAISE_EXCEPTION(E_BAD_BMT);
            }

            shadowRec = bmtr_find (mcellp, BSR_SHADOW_XTNTS, vdp->dmnP);
            derefPgFlag = 0;
            sts = bs_derefpg (pgRef, BS_CACHE_IT);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            if (shadowRec != NULL) {
                /* If the next mcell is a BSR_SHADOW_XTNTS then the mcellCnt */
                /* was correct (it did not wrap). */
                break;
            }

            /* The next mcell is not a BSR_SHADOW_XTNTS so this is still part */
            /* of a very fragmented stripe. The mcellCnt is at least 64K more */
            /* than indicated on disk (the short on disk mcellCnt wrapped). */
            mcellCnt += 0x10000;
        }

        if ( xtntMap->cnt >= xtntMap->maxCnt ) {
            aprintf("WARNING: file tag %d ", bfap->tag.num);
            aprintf("in fileset \"%s\" ", bfap->bfSetp->bfSetName);
            aprintf("has > 64K extent mcells.\n");
            sts = imm_big_extend_xtnt_map(xtntMap);
            if ( sts != EOK ) {
                RAISE_EXCEPTION (sts);
            }
            subXtntMap = xtntMap->subXtntMap;
        }

        lastPg = subXtntMap[xtntMap->cnt - 1].pageOffset +
                 subXtntMap[xtntMap->cnt - 1].pageCnt;
        vdIndex = nextVdIndex;
        mcellId = nextMcellId;
        sts = load_from_xtra_xtnt_rec( bfap,
                                       vdIndex,
                                       mcellId,
                                       &subXtntMap[xtntMap->cnt],
                                       lastPg,
                                       &nextVdIndex,
                                       &nextMcellId);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        xtntMap->cnt++;
    }

    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->updateStart = xtntMap->cnt;

    pageOffset = xtntMap->subXtntMap[0].pageOffset;
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
    pageCnt = (subXtntMap->pageOffset + subXtntMap->pageCnt) - pageOffset;

    sts = imm_get_alloc_page_cnt (xtntMap, pageOffset, pageCnt, &totalPageCnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->nextValidPage = pageOffset + pageCnt;

    *bfXtntMap = xtntMap;
    *allocPageCnt = totalPageCnt;
    *bfNextVdIndex = nextVdIndex;
    *bfNextMcellId = nextMcellId;

    return sts;

HANDLE_EXCEPTION:
    if (derefPgFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }

    return sts;

}  /* end load_from_shadow_rec */

/*
 * load_from_xtnt_rec
 *
 * This function initializes the sub extent map for the specified
 * extent record and optionally loads the extent descriptors into the
 * sub extent map.  If there are extra extent records, this function
 * calls load_from_xtra_xtnt_rec() for each record.
 */

static
statusT
load_from_xtnt_rec (
                    bfAccessT *bfap,               /* in, modified */
                    bsXtntRT *xtntRec,             /* in */
                    uint32T *allocPageCnt,         /* out */
                    vdIndexT *bfNextVdIndex,       /* out */
                    bfMCIdT *bfNextMcellId         /* out */
                    )
{
    uint32T i;
    bfMCIdT mcellId;
    bfMCIdT nextMcellId;
    vdIndexT nextVdIndex;
    uint32T pageCnt;
    uint32T pageOffset;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt;
    vdIndexT vdIndex;
    bsInMemXtntMapT *xtntMap = NULL;
    bfAccessT *mdap;
    domainT *dmnP = bfap->dmnP;
    vdIndexT bfVdIndex = bfap->primVdIndex;
    bfMCIdT bfMcellId = bfap->primMCId;
    uint32T blksPerPage = bfap->bfPageSz;
    vdT *vdp;
    lbnT nextVdBlk;
    lbnT vdBlk;
    uint32T lastPg;

    if (xtntRec->blksPerPage != blksPerPage) {
        return E_BAD_PAGESIZE;
    }

    /*
    ** firstXtnt.mcellCnt is a ushort and will wrap for very fragmented files.
    ** We will believe it for now and get that many subextent maps, but
    ** later we will follow the mcell chain. If there are more mcells
    ** than mcellCnt we will allocate 64K more.
    ** Remember that if mcellCnt is 0 we still get 1 subextent map.
    */
    sts = imm_create_xtnt_map (
                               xtntRec->blksPerPage,
                               dmnP,
                               xtntRec->firstXtnt.mcellCnt,
                               0,
                               bfVdIndex,
                               &xtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->hdrType = BSR_XTNTS;
    xtntMap->hdrVdIndex = bfVdIndex;
    xtntMap->hdrMcellId = bfMcellId;
    xtntMap->allocVdIndex = -1;

    MS_SMP_ASSERT(xtntMap->subXtntMap != NULL);
    subXtntMap = &(xtntMap->subXtntMap[0]);

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0,  /* pageOffset */
                                 0,  /* pageCnt */
                                 bfVdIndex,
                                 bfMcellId,
                                 BSR_XTNTS,
                                 BMT_XTNTS,
                                 xtntRec->firstXtnt.xCnt
                                 );
    if (sts == EOK) {
        sts = load_from_xtnt_array (
                                    xtntRec->firstXtnt.xCnt,
                                    xtntRec->firstXtnt.bsXA,
                                    subXtntMap
                                    );
        if (sts == EOK) {
            xtntMap->cnt = 1;
        } else {
            if (sts == ENO_XTNTS) {
                domain_panic (dmnP, "load_from_xtnt_array called from load_from_xtnt_rec: cnt must be non-zero");
                sts = E_DOMAIN_PANIC;
            }
            RAISE_EXCEPTION (sts);
        }
    } else {
        RAISE_EXCEPTION (sts);
    }

    nextVdIndex = xtntRec->chainVdIndex;
    nextMcellId = xtntRec->chainMCId;
    nextVdBlk = xtntRec->firstXtnt.bsXA[0].vdBlk;

    /*
     * Extra extent records.
     */
    subXtntMap = xtntMap->subXtntMap;
    vdp = VD_HTOP( bfVdIndex, dmnP );
    mdap = RBMT_THERE(dmnP) ? vdp->rbmtp : vdp->bmtp;

    if (mdap != NULL) {
        while ( nextVdIndex != bsNilVdIndex ) {
            if ( xtntMap->cnt >= xtntMap->maxCnt ) {
                aprintf("WARNING: file tag %d ", bfap->tag.num);
                aprintf("in fileset \"%s\" ", bfap->bfSetp->bfSetName);
                aprintf("has > 64K extent mcells.\n");
                sts = imm_big_extend_xtnt_map(xtntMap);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION (sts);
                }
                subXtntMap = xtntMap->subXtntMap;
            }

            lastPg = subXtntMap[xtntMap->cnt - 1].pageOffset +
                     subXtntMap[xtntMap->cnt - 1].pageCnt;
            vdIndex = nextVdIndex;
            mcellId = nextMcellId;
            sts = load_from_xtra_xtnt_rec( bfap,
                                           vdIndex,
                                           mcellId,
                                           &subXtntMap[xtntMap->cnt],
                                           lastPg,
                                           &nextVdIndex,
                                           &nextMcellId);
            /* Check if mounting vd and opening BMT prior to doing
             * recovery.  This can happen if the next BMT page extents
             * have not been flushed to disk, but we crashed, and will
             * update the BMT extents during recovery (has not happened
             * yet since vdp->bmtp is still NULL or state is
             * BFD_RECOVER_REDO).
             */
            if ( sts == EOK          &&
                 nextVdIndex == 0    &&
                 RBMT_THERE(dmnP)    &&
                 (vdp->bmtp == NULL  ||
                  dmnP->state == BFD_RECOVER_REDO) ) {
                 xtntMap->cnt++;
                 break;
            } else if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            xtntMap->cnt++;
        }  /* end for */
    } else {  /* mdap == NULL */
        /*
         * If there is no BMT yet, this must be a reserved file.
         * Note that load_from_bmt_xtra_xtnt_rec() makes the 
         * assumption that the BMT's extent records are in memory.
         */
        MS_SMP_ASSERT(BS_BFTAG_RSVD(bfap->tag));
        /*
         * This is only for the RBMT or V3 BMT.
         * stg_add_rbmt_stg ensures that mcellCnt will not wrap for RBMT.
         * The BMT in a V3 domain can only have ~20 mcells.
         * So we don't need imm_big_extend_xtnt_map here.
         */
        for (i = 1; i < xtntRec->firstXtnt.mcellCnt; i++) 
        {
            vdIndex = nextVdIndex;
            mcellId = nextMcellId;
            vdBlk = nextVdBlk;

            sts = load_from_bmt_xtra_xtnt_rec (
                                               dmnP,
                                               vdIndex,
                                               mcellId,
                                               vdBlk,
                                               &(subXtntMap[xtntMap->cnt]),
                                               &nextVdIndex,
                                               &nextMcellId,
                                               &nextVdBlk
                                              );

            /* If we are rebooting after a crash, but we have not
             * yet done recovery, then the last RBMT subextent
             * record may not have been flushed to disk, but will
             * be fixed by the recovery pass.  (This only happens
             * if the crash occurs very soon after the RBMT
             * extension has been committed.) In this case, we
             * need to continue to mount the domain [via
             * setup_vd()] with the RBMT extents that are
             * currently OK, and remap the RBMT extents in the
             * operational redo phase of recovery. This should
             * never happen on the first RBMT extension since that
             * update is limited to page 0.  */

            if ((sts == ENO_XTNTS) && 
                (i == (xtntRec->firstXtnt.mcellCnt-1)) &&
                (mcellId.page != 0))
            {
                break;
            }
            else if (sts != EOK)
            {
                RAISE_EXCEPTION (sts);
            }
            xtntMap->cnt++;
        }  /* end for */
    }

    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->updateStart = xtntMap->cnt;

    pageOffset = xtntMap->subXtntMap[0].pageOffset;
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
    pageCnt = (subXtntMap->pageOffset + subXtntMap->pageCnt) - pageOffset;

    sts = imm_get_alloc_page_cnt (xtntMap, pageOffset, pageCnt, &totalPageCnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->nextValidPage = pageOffset + pageCnt;

    bfap->xtnts.xtntMap = xtntMap;
    *allocPageCnt = totalPageCnt;
    *bfNextVdIndex = nextVdIndex;
    *bfNextMcellId = nextMcellId;

    return sts;

HANDLE_EXCEPTION:
    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }

    return sts;

}  /* end load_from_xtnt_rec */


/*
 * load_from_xtra_xtnt_rec
 *
 * This function initializes the sub extent map for the specified
 * extra extent record and optionally loads the extent descriptors into
 * the in-memory extent mcell.
 */

static
statusT
load_from_xtra_xtnt_rec (
                         bfAccessT *bfap,                  /* in, modified */
                         vdIndexT vdIndex,                 /* in */
                         bfMCIdT mcellId,                  /* in */
                         bsInMemSubXtntMapT *subXtntMap,   /* in */
                         uint32T lastPg,                   /* in */
                         vdIndexT *nextVdIndex,            /* out */
                         bfMCIdT *nextMcellId              /* out */
                         )
{
    bsMPgT *bmtp;
    int derefPgFlag = 0;
    bsMCT *mcellp;
    bsXtraXtntRT *xtraXtntRec;
    bfPageRefHT pgRef;
    statusT sts;
    vdT *vdp;
    struct bfAccess *mdap;
    domainT *dmnP = bfap->dmnP;

    vdp = VD_HTOP(vdIndex, dmnP);
    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(bfap->tag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }

    MS_SMP_ASSERT(TEST_PAGE(mcellId.page, mdap) == EOK);
    sts = bmt_refpg( &pgRef, (void*)&bmtp, mdap, mcellId.page, BS_NIL );
    if (sts != EOK) {
        return sts;
    }
    derefPgFlag = 1;

    MS_SMP_ASSERT(TEST_CELL(mcellId.cell) == EOK);
    mcellp = &(bmtp->bsMCA[mcellId.cell]);
    if ( check_mcell_hdr(mcellp, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    xtraXtntRec = bmtr_find (mcellp, BSR_XTRA_XTNTS, vdp->dmnP);
    if (xtraXtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }
    if ( CheckXtnts &&
         check_BSR_XTRA_XTNTS_rec(xtraXtntRec, bfap, vdp, lastPg) != EOK )
    {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0,  /* pageOffset */
                                 0,  /* pageCnt */
                                 vdIndex,
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 xtraXtntRec->xCnt
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = load_from_xtnt_array (
                                xtraXtntRec->xCnt,
                                xtraXtntRec->bsXA,
                                subXtntMap
                                );
    if (sts != EOK) {
        if (sts == ENO_XTNTS) {
            domain_panic (dmnP, "load_from_xtnt_array called from load_from_xtra_xtnt_rec: cnt must be non-zero");
            sts = E_DOMAIN_PANIC;
        }
        RAISE_EXCEPTION (sts);
    }

    *nextVdIndex = mcellp->nextVdIndex;
    *nextMcellId = mcellp->nextMCId;

    sts = bs_derefpg (pgRef, BS_CACHE_IT);

    return sts;

HANDLE_EXCEPTION:
    if (derefPgFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end load_from_xtra_xtnt_rec */

/*
 * load_from_bmt_xtra_xtnt_rec
 *
 * This function initializes the sub extent map for the specified
 * extra extent record and optionally loads the extent descriptors into
 * the in-memory extent mcell.
 *
 * NOTE:  The bmt's on-disk extra extent records are assumed to be in memory.
 */

static
statusT
load_from_bmt_xtra_xtnt_rec (
                             domainT *dmnP,  /* in */
                             vdIndexT vdIndex,  /* in */
                             bfMCIdT mcellId,  /* in */
                             lbnT vdBlk,
                             bsInMemSubXtntMapT *subXtntMap,  /* in */
                             vdIndexT *nextVdIndex,  /* out */
                             bfMCIdT *nextMcellId,  /* out */
                             lbnT *nextVdBlk
                             )
{
    bsMPgT *bmtp;
    bsMPgT *rbmtPg=NULL;
    bsMCT *mcellp;
    statusT sts;
    int xtntCnt;
    bsXtraXtntRT *xtraXtntRec;
    vdT *vdp;

    /* Page zero for the rbmt or the V3 bmt is already in 
     * memory so just grab it from there
     */

    vdp = VD_HTOP(vdIndex, dmnP);

    if (mcellId.page == 0) 
    {
        bmtp = get_bmt_pgptr(dmnP);
        mcellp = &(bmtp->bsMCA[mcellId.cell]);
    }
    else
    {
        rbmtPg = (struct bsMPg *)ms_malloc(sizeof(struct bsMPg));

        if (sts = (statusT) read_raw_bmt_page(vdp->devVp, vdBlk, rbmtPg)) 
        {
            RAISE_EXCEPTION (sts);
        }
        mcellp = &(rbmtPg->bsMCA[mcellId.cell]);
    }

    xtraXtntRec = bmtr_find (mcellp, BSR_XTRA_XTNTS, vdp->dmnP);
    if (xtraXtntRec == NULL) 
    {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0,  /* pageOffset */
                                 0,  /* pageCnt */
                                 vdIndex,
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 xtraXtntRec->xCnt
                                 );
    if (sts != EOK) 
    {
        RAISE_EXCEPTION (sts);
    }

    sts = load_from_xtnt_array (
                                xtraXtntRec->xCnt,
                                xtraXtntRec->bsXA,
                                subXtntMap
                                );
    if (sts != EOK) 
    {
        if (sts == ENO_XTNTS)
        {
            domain_panic (dmnP, "load_from_xtnt_array called from load_from_bmt_xtra_xtnt_rec: cnt must be non-zero");
            sts = E_DOMAIN_PANIC;
        }
        RAISE_EXCEPTION (sts);
    }

    if (mcellp->nextVdIndex != bsNilVdIndex) {
        if (mcellp->nextVdIndex != vdIndex) {
            ADVFS_SAD0("load_from_bmt_xtra_xtnt_rec: bad mcellp->nextVdIndex");
        }
    }

    *nextVdIndex = mcellp->nextVdIndex;
    *nextMcellId = mcellp->nextMCId;
    *nextVdBlk = xtraXtntRec->bsXA[0].vdBlk; /* Next Page of the RBMT */

HANDLE_EXCEPTION:

    if (rbmtPg != NULL) 
    {
        ms_free(rbmtPg);
    }

    return sts;
}  /* end load_from_bmt_xtra_xtnt_rec */

/*
 * load_from_xtnt_array
 *
 * This function copies entries from the on-disk extent descriptor
 * array to the in-memory extent mcell.  The on-disk descriptors
 * are ordered in ascending page order.
 */

static
statusT
load_from_xtnt_array (
                      int cnt,  /* in */
                      bsXtntT *bsXA,  /* in */
                      bsInMemSubXtntMapT *subXtntMap  /* in */
                      )
{
    uint32T inMem;
    uint32T onDisk;
    statusT sts;

    /*
     * Check to see if cnt > 0.  If it is not, we return ENO_XTNTS so that
     * the callers of load_from_xtnt_array() can domain_panic.
     *
     * When file panics are supported, this domain_panic should be converted
     * into a file panic.
     */
    if (!cnt) {
        return ENO_XTNTS;
    }

    inMem = 0;
    for (onDisk = 0; onDisk < cnt; onDisk++) {
        if (inMem >= subXtntMap->maxCnt) {
            sts = imm_extend_sub_xtnt_map (subXtntMap);
            if (sts != EOK) {
                return sts;
            }
        }
        subXtntMap->bsXA[inMem] = bsXA[onDisk];
        inMem++;
    }  /* end for */

    subXtntMap->cnt = inMem;
    subXtntMap->updateStart = subXtntMap->cnt;

    subXtntMap->pageOffset = subXtntMap->bsXA[0].bsPage;
    subXtntMap->pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
                          subXtntMap->pageOffset;

    return EOK;

}  /* end load_from_xtnt_array */


#ifdef ADVFS_DEBUG
/*
 * odm_check_xtnt_map
 *
 * This functions checks the specified on-disk extent map.  If something is
 * weird, SADs all around.
 *
 * NOTE:  This function assumes a non-striped bitfile.
 * bitfile.
 */

statusT
odm_check_xtnt_map (
                    domainT *domain,  /* in */
                    bfTagT bfTag, /* in */
                    vdIndexT mcellVdIndex,  /* in */
                    uint32T mcellPage,  /* in */
                    uint32T mcellCell  /* in */
                    )
{
    bsMPgT *bmt;
    int derefPageFlag = 0;
    uint32T i;
    uint32T j;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    uint32T prevBsPage;
    bfMCIdT prevMcellId;
    vdIndexT prevVdIndex;
    bsShadowXtntT *shadowRec;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    int xtnts_in_prim = FALSE;
    bfAccessT *mdap;

    vd = VD_HTOP(mcellVdIndex, domain);

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     mdap,
                     mcellPage,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellCell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        ADVFS_SAD0 ("odm_check_xtnt_map: no xtntRec");
    }

    /* 
     * If the file has extent information in the primary mcell,
     * start there.  Otherwise, start in the mcell pointed to by
     * the primary mcell.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(domain->dmnVersion, xtntRec->type)) {
        xtnts_in_prim = TRUE;
        vdIndex = mcellVdIndex;
        mcellId.page = mcellPage;
        mcellId.cell = mcellCell;
    } else {
        vdIndex = xtntRec->chainVdIndex;
        mcellId = xtntRec->chainMCId;
    }

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ((vdIndex == bsNilVdIndex) &&
        (mcellId.page == bsNilMCId.page) &&
        (mcellId.cell == bsNilMCId.cell)) {
        RAISE_EXCEPTION (EOK);
    }

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(vdIndex, domain)->rbmtp;
    } else {
        mdap = VD_HTOP(vdIndex, domain)->bmtp;
    }

    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     mdap,
                     mcellId.page,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellId.cell]);
    shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, vd->dmnP);
    if (shadowRec != NULL) {
        prevBsPage = shadowRec->bsXA[0].bsPage;
        for (j = 1; j < shadowRec->xCnt; j++) {
            if (shadowRec->bsXA[j].bsPage <= prevBsPage) {
                ADVFS_SAD0 ("odm_check_xtnt_map: bad shadowRec bsPage");
            }
            prevBsPage = shadowRec->bsXA[j].bsPage;
        }  /* end for */
    } else {
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec != NULL) {
            prevBsPage = xtraXtntRec->bsXA[0].bsPage;
            for (j = 1; j < xtraXtntRec->xCnt; j++) {
                if (xtraXtntRec->bsXA[j].bsPage <= prevBsPage) {
                    ADVFS_SAD0 ("odm_check_xtnt_map: bad xtraXtntRec bsPage");
                }
                prevBsPage = xtraXtntRec->bsXA[j].bsPage;
            }  /* end for */
        } else {
            xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
            if (xtntRec != NULL) {
               if (xtntRec->firstXtnt.bsXA[1].bsPage <=
                   xtntRec->firstXtnt.bsXA[0].bsPage) {
                   ADVFS_SAD0 ("odm_check_xtnt_map: bad xtntRec bsPage");
               }
            }
            else {
                ADVFS_SAD0 ("odm_check_xtnt_map: no xtnt, shadow, or xtra rec");
            }
        }
    }

    prevVdIndex = vdIndex;
    prevMcellId = mcellId;
    if (xtnts_in_prim) {
        vdIndex = xtntRec->chainVdIndex;
        mcellId = xtntRec->chainMCId;
    }else {
        vdIndex = mcell->nextVdIndex;
        mcellId = mcell->nextMCId;
    }

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    while ((vdIndex != bsNilVdIndex) &&
           ((mcellId.page != bsNilMCId.page) ||
            (mcellId.cell != bsNilMCId.cell))) {

        if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
            mdap = VD_HTOP(vdIndex, domain)->rbmtp;
        } else {
            mdap = VD_HTOP(vdIndex, domain)->bmtp;
        }
        sts = bmt_refpg( &pgRef,
                         (void*)&bmt,
                         mdap,
                         mcellId.page,
                         BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        derefPageFlag = 1;

        mcell = &(bmt->bsMCA[mcellId.cell]);
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            ADVFS_SAD0 ("odm_check_xtnt_map: no xtraXtntRec");
        }

        prevBsPage = xtraXtntRec->bsXA[0].bsPage;
        for (j = 1; j < xtraXtntRec->xCnt; j++) {
            if (xtraXtntRec->bsXA[j].bsPage <= prevBsPage) {
                ADVFS_SAD0 ("odm_check_xtnt_map: bad xtraXtntRec bsPage");
            }
            prevBsPage = xtraXtntRec->bsXA[j].bsPage;
        }  /* end for */

        prevVdIndex = vdIndex;
        vdIndex = mcell->nextVdIndex;
        prevMcellId = mcellId;
        mcellId = mcell->nextMCId;

        derefPageFlag = 0;
        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

    }  /* end while */

    return EOK;

HANDLE_EXCEPTION:
    if (derefPageFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end odm_check_xtnt_map */

/*
 * odm_print_xtnt_map
 *
 * This function prints the specified on-disk extent map.
 *
 * NOTE:  This function assumes a non-striped bitfile.
 */

statusT
odm_print_xtnt_map (
                    domainT *domain,  /* in */
                    bfTagT bfTag, /* in */
                    vdIndexT mcellVdIndex,  /* in */
                    uint32T mcellPage,  /* in */
                    uint32T mcellCell  /* in */
                    )
{
    bsMPgT *bmt;
    int derefPageFlag = 0;
    uint32T i;
    uint32T j;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    uint32T prevBsPage;
    bfMCIdT prevMcellId;
    vdIndexT prevVdIndex;
    bsShadowXtntT *shadowRec;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    int xtnts_in_prim = FALSE;
    bfAccessT *mdap;

    vd = VD_HTOP(mcellVdIndex, domain);

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     mdap,
                     mcellPage,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellCell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    /* 
     * If the file has extent information in the primary mcell,
     * start there.  Otherwise, start in the mcell pointed to by
     * the primary mcell.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(domain->dmnVersion, xtntRec->type)) {
        xtnts_in_prim = TRUE;
        vdIndex = mcellVdIndex;
        mcellId.page = mcellPage;
        mcellId.cell = mcellCell;
    }
    else {
        vdIndex = xtntRec->chainVdIndex;
        mcellId = xtntRec->chainMCId;
    }

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ((vdIndex == bsNilVdIndex) &&
        (mcellId.page == bsNilMCId.page) &&
        (mcellId.cell == bsNilMCId.cell)) {
        RAISE_EXCEPTION (EOK);
    }

    if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(vdIndex, domain)->rbmtp;
    } else {
        mdap = VD_HTOP(vdIndex, domain)->bmtp;
    }
    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     mdap,
                     mcellId.page,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellId.cell]);
    shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, vd->dmnP);
    if (shadowRec != NULL) {
        printf ("shadowRec - vdIndex: %d, mcellId (page.cell): %d.%d\n",
                vdIndex, mcellId.page, mcellId.cell);

        for (j = 0; j < shadowRec->xCnt; j++) {

            printf ("    bsPage: %d (0x%08x), vdBlk: %u (0x%08x)\n",
                    shadowRec->bsXA[j].bsPage, shadowRec->bsXA[j].bsPage,
                    shadowRec->bsXA[j].vdBlk, shadowRec->bsXA[j].vdBlk);

        }  /* end for */
    } else {
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec != NULL) {

            printf ("xtraXtntRec - vdIndex: %d, mcellId (page.cell): %d.%d\n",
                    vdIndex, mcellId.page, mcellId.cell);

            for (j = 0; j < xtraXtntRec->xCnt; j++) {

                printf ("    bsPage: %d (0x%08x), vdBlk: %u (0x%08x)\n",
                        xtraXtntRec->bsXA[j].bsPage, xtraXtntRec->bsXA[j].bsPage,
                        xtraXtntRec->bsXA[j].vdBlk, xtraXtntRec->bsXA[j].vdBlk);

            }  /* end for */
        } else {
            xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
            if (xtntRec != NULL) {
                printf ("xtntRec - vdIndex: %d, mcellId (page.cell): %d.%d\n",
                    vdIndex, mcellId.page, mcellId.cell);
                printf ("    bsPage: %d (0x%08x), vdBlk: %u (0x%08x)\n",
                        xtntRec->firstXtnt.bsXA[0].bsPage, 
                        xtntRec->firstXtnt.bsXA[0].bsPage,
                        xtntRec->firstXtnt.bsXA[0].vdBlk,
                        xtntRec->firstXtnt.bsXA[0].vdBlk);
                printf ("    bsPage: %d (0x%08x), vdBlk: %u (0x%08x)\n",
                        xtntRec->firstXtnt.bsXA[1].bsPage, 
                        xtntRec->firstXtnt.bsXA[1].bsPage,
                        xtntRec->firstXtnt.bsXA[1].vdBlk,
                        xtntRec->firstXtnt.bsXA[1].vdBlk);
            }
            else {
                RAISE_EXCEPTION (ENO_XTNTS);
            }
        }
    }

    prevVdIndex = vdIndex;
    prevMcellId = mcellId;
    if (xtnts_in_prim) {
        vdIndex = xtntRec->chainVdIndex;
        mcellId = xtntRec->chainMCId;
    }
    else {
        vdIndex = mcell->nextVdIndex;
        mcellId = mcell->nextMCId;
    }

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    while ((vdIndex != bsNilVdIndex) &&
           ((mcellId.page != bsNilMCId.page) ||
            (mcellId.cell != bsNilMCId.cell))) {

        if ( RBMT_THERE(domain) && BS_BFTAG_RSVD(bfTag) ) {
            mdap = VD_HTOP(vdIndex, domain)->rbmtp;
        } else {
            mdap = VD_HTOP(vdIndex, domain)->bmtp;
        }
        sts = bmt_refpg( &pgRef,
                         (void*)&bmt,
                         mdap,
                         mcellId.page,
                         BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        derefPageFlag = 1;

        mcell = &(bmt->bsMCA[mcellId.cell]);
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }

        printf ("xtraXtntRec - vdIndex: %d, mcellId (page.cell): %d.%d\n",
                vdIndex, mcellId.page, mcellId.cell);

        for (j = 0; j < xtraXtntRec->xCnt; j++) {

            printf ("    bsPage: %d (0x%08x), vdBlk: %u (0x%08x)\n",
                    xtraXtntRec->bsXA[j].bsPage, xtraXtntRec->bsXA[j].bsPage,
                    xtraXtntRec->bsXA[j].vdBlk, xtraXtntRec->bsXA[j].vdBlk);

        }  /* end for */

        prevVdIndex = vdIndex;
        vdIndex = mcell->nextVdIndex;
        prevMcellId = mcellId;
        mcellId = mcell->nextMCId;

        derefPageFlag = 0;
        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

    }  /* end while */

    return EOK;

HANDLE_EXCEPTION:
    if (derefPageFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end odm_print_xtnt_map */

#endif /* ADVFS_DEBUG */

/*
 * Deallocate all extent map structures for a
 * given in-memory extent map.
 */
void
x_dealloc_extent_maps(bsInMemXtntT *xtnts) 
{
    bsInMemXtntMapT       *xtntMap,
                          *nextXtntMap;

    if (xtnts->validFlag != 0) {
        if (xtnts->xtntMap != NULL) {
            imm_delete_xtnt_map (xtnts->xtntMap);
            xtnts->xtntMap = NULL;
        }

        if (xtnts->shadowXtntMap != NULL) {
            xtntMap = xtnts->shadowXtntMap;
            do {
                nextXtntMap = xtntMap->nextXtntMap;
                imm_delete_xtnt_map (xtntMap);
                xtntMap = nextXtntMap;
            } while (xtntMap != NULL);
            xtnts->shadowXtntMap = NULL;
        }

        if (xtnts->stripeXtntMap != NULL) {
            str_delete_stripe_hdr (xtnts->stripeXtntMap);
            xtnts->stripeXtntMap = NULL;
        }

        if (xtnts->copyXtntMap != NULL) {
            xtntMap = xtnts->copyXtntMap;
            do {
                nextXtntMap = xtntMap->nextXtntMap;
                imm_delete_xtnt_map (xtntMap);
                xtntMap = nextXtntMap;
            } while (xtntMap != NULL);
            xtnts->copyXtntMap = NULL;
        }

        xtnts->validFlag = 0;
    }
}
