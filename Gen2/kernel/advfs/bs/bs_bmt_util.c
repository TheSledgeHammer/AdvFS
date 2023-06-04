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
 *  AdvFS
 *
 * Abstract:
 *
 *  Metadata bitfile structure utility routines
 *
 */

/**************************************************************************
 *****************  IMPORANT NOTE  **  IMPORTANT NOTE *********************
 **************************************************************************
 *
 *  The bitfile metadata cell record (bmtr) access routines require that
 *  the caller hold the corresponding bitfile's mcellList_lk locked.
 *  There are exceptions.
 *
 *  bs_create() - Since no other thread can be accessing the new bitfile's
 *      mcell list this routine does not need to lock the mcellList_lk.
 *
 *  bs_access()/map_diskbf() - When a bitfile is accessed for the first
 *  time and we map its storage into memory the bitfile is not
 *  accessible by other threads.  Therefore, we don't need to lock
 *  the mcell list during the mapping.
 *
 *  Since the bitfile's attributes and primary extent map records always
 *  exist (and always in the primary mcell) it is not necessary to
 *  to lock the mcellList_lk when calling bmtr_find() or bmtr_scan().
 *  However, one will most likely need a lock when accessing/modifying
 *  the contents of these records.
 *
 **************************************************************************/

#include <sys/param.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_delete.h>
#include <bs_extents.h>
#include <bs_migrate.h>
#include <bs_stg.h>
#include <ms_osf.h>
#include <ftx_public.h>
#include <ftx_privates.h>
#include <bs_msg_queue.h>
#include <advfs_evm.h>
#include <bs_snapshot.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_BMT_UTIL

/*
 * By default, if the mcell free list gets corrupted, just print a warning 
 * and fix it up.  If, for some reason, we want to crash if we discover 
 * such corruption, set AdvfsFixUpBMT to 0 via dbx.
 */
int AdvfsFixUpBMT = 1;

/* next structs used for RBMT extension */
#define RBMT_EXTEND_Q_ENTRIES 12
static msgQHT RbmtExtQH;       /* Msg queue for extending RBMTs */

typedef enum {
    EXTEND_RBMT = 1            /* extend the RBMT */
} rbmtExtMsgTypeT;

typedef struct {
    rbmtExtMsgTypeT msgType;   /* msg type (verification; future
expansion) */
    bfDomainIdT domainId;      /* domain with rbmt to expand */
    vdIndexT vdIndex;          /* vd with rbmt to expand */
} rbmtExtMsgT;

/******************************************/
/****** private functions prototypes ******/
/******************************************/

opxT alloc_mcell_undo;

void
alloc_mcell_undo (
                  ftxHT ftx,
                  int undoRecSize,
                  void *undoRecAddr
                  );

opxT alloc_rbmt_mcell_undo;

void
alloc_rbmt_mcell_undo (
                  ftxHT ftx,
                  int undoRecSize,
                  void *undoRecAddr
                  );

opxT alloc_link_mcell_undo;

void
alloc_link_mcell_undo (
                       ftxHT ftx,
                       int undoRecSize,
                       void *undoRecAddr
                       );

opxT undo_insert_remove_xtnt_chain;

void
undo_insert_remove_xtnt_chain (
                               ftxHT ftx,
                               int size,
                               void* address
                               );

opxT link_unlink_mcells_undo;

void
link_unlink_mcells_undo (
                         ftxHT ftx,
                         int undoRecSize,
                         void *undoRecAddr
                         );

opxT extend_bmt_redo_opx;
opxT extend_rbmt_redo_opx;

static
statusT
alloc_mcell (
             vdT* vd,  /* in */
             ftxHT parentFtx,  /* in */
             bfMCIdT *retMcellId,  /* out */
             rbfPgRefHT *retPgPin,  /* out */
             bsMCT **retMcell,  /* out */
             mcellPoolT poolType  /* in */
             );

static
statusT
alloc_rbmt_mcell (
                  vdT* vd,  /* in */
                  ftxHT parentFtx,  /* in */
                  bfMCIdT *retMcellId,  /* out */
                  rbfPgRefHT *retPgPin,  /* out */
                  bsMCT **retMcell  /* out */
                  );

static
void
init_mcell (
            rbfPgRefHT pgPin,  /* in */
            bsMCT *mcell,  /* in/initialized */
            bfTagT bfSetTag,  /* in */
            bfTagT bfTag,  /* in */
            unsigned int linkSeg  /* in */
            );

static
void
link_mcells (
             rbfPgRefHT oldPgPin,  /* in */
             bsMCT *oldMcell,  /* in */
             rbfPgRefHT newPgPin,  /* in */
             bsMCT *newMcell,  /* in */
             bfMCIdT newMcellId  /* in */
            );

statusT
bmtr_scan_mcells(
    bfMCIdT *mcid,              /* in/out - mcell id (primary/found) */
    vdT **vdp,                  /* in/out - vd (of mcell) pointer */
    void **bsrp,                /* out - pointer to BMT record */
    bfPageRefHT *pgRef,         /* out - pgref of BMT cell's page */
    uint32_t *recOffset,        /* out - record's mcell offset */
    uint16_t *rSize,            /* out - record's size in bytes */
    const int rtype,            /* in - record type to find */
    const bfTagT tag            /* in - bitfile's tag */
    );

statusT
advfs_bmtr_mcell_refpg(
    bfAccessT *bfap,		/* in - current file */
    bfMCIdT mcell_id,		/* in - ID of mcell to be read */
    bsMCT **mcell_ptr,		/* out - Pointer to the mcell to be read */
    bfPageRefHT *page_ref	/* out - Page reference id */
    );


static statusT
bmtr_put_rec_n_unlk_int(
    struct bfAccess *bfap,     /* in - bf access structure */
    uint16_t rType,            /* in - type of record */
    void *bPtr,                /* in - ptr to buffer */
    uint16_t bSize,            /* in - size of buffer */
    ftxHT parentFtxH,          /* in - transaction handle */
    bmtrLkT lk,                /* in - unlock mcell? */
    ftxIdT  xid                /* in - CFS transaction id */
    );


/*
 * bmt_upd_mcell_free_list - update page that starts list of free mcells
 */

static bs_meta_page_t 
bmt_upd_mcell_free_list(
                    ftxHT ftxH,        /* in - callers ftx handle */
                    vdT* vdp,          /* in - current disk pointer */
                    bs_meta_page_t freeMcellPage  /* in - free mcell page */
                    );
statusT
bmtr_update_rec_int(
    bfAccessT* bfap,            /* in - ptr to bf access struct */
    uint16_t rType,             /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,             /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,
    uint32_t flags              /* in - flags */
    );

static statusT check_xtnts( bsXtntT*, uint16_t, bfAccessT*, vdT*);

static statusT test_mcell_ptr(bfMCIdT, bfTagT, domainT*);

/*
 * init metadata bitfile page
 *
 * for the given page, initialize all cells to be forward linked
 * to the next cell, except the last cell, which terminates the list.
 *
 * within each cell, initialize a NIL record.
 */

void
bmt_init_page(
    bsMPgT *dpg,                     /* in - BMT page pointer */
    bs_meta_page_t pgnum,            /* in - this page number */
    vdIndexT vdIndex,                /* in - volume index for this page */
    int mdtype,                      /* in - metadata type - RBMT or BMT */
    adv_ondisk_version_t dmnVersion, /* in - on-disk version number */
    bfFsCookieT dmnCookie,           /* in - domian cookie */
    vdCookieT vdCookie               /* in - unique ID for this volume */
    )

{
    int i, numMcells;

    if ( mdtype == BFM_BMT ) {
        numMcells = BSPG_CELLS;
        dpg->bmtMagicNumber = BMT_MAGIC;
        dpg->bmtRsvd1 = 0; 
    } else {
        numMcells = BSPG_CELLS - 1;
        dpg->bmtMagicNumber = RBMT_MAGIC;
        dpg->bmtODSVersion = dmnVersion;  /* for RBMT page only */
    }
    dpg->bmtFsCookie = dmnCookie;
    dpg->bmtNextFreeMcell = 0;
    dpg->bmtFreeMcellCnt = numMcells;
    dpg->bmtNextFreePg = 0;
    dpg->bmtPageId = pgnum;
    dpg->bmtVdIndex = vdIndex;
    dpg->bmtVdCookie = vdCookie;
    dpg->bmtRsvd2 = 0;
    bzero(&dpg->filler,BSPG_FILLER);

    for (i = 0; i < numMcells; i++) {
        struct bsMR *rp;

        dpg->bsMCA[i].mcNextMCId.cell = i + 1;
        dpg->bsMCA[i].mcNextMCId.page = pgnum;
        dpg->bsMCA[i].mcNextMCId.volume = vdIndex;
        dpg->bsMCA[i].mcLinkSegment = 0;
        dpg->bsMCA[i].mcTag = NilBfTag;
        dpg->bsMCA[i].mcBfSetTag = NilBfTag;
        dpg->bsMCA[i].mcNumRecords = 0;
        dpg->bsMCA[i].mcRsvd1 = 0;
        dpg->bsMCA[i].mcRsvd2 = 0;
        rp = (struct bsMR *) dpg->bsMCA[i].bsMR0;
        rp->bCnt = sizeof(struct bsMR);
        rp->type = BSR_NIL;
        rp->padding = 0;
    }

    dpg->bsMCA[numMcells-1].mcNextMCId = bsNilMCId; 
}

/*
 * bmtr_assign
 *
 * This routine attempts to allocate space for the requested size
 * record within the specified cell.  If there is not enough space
 * within the cell, NULL is returned.
 *
 * If successful, the record header is initialized with the specified
 * type, and a pointer to the data area of the record is returned.
 *
 * Caller must hold the bitfile's mcellList_lk locked.  See not above.
 */

void*
bmtr_assign(
    uint16_t type,       /* in - type of record */
    uint64_t bcnt,       /* in - size of record */
    bsMCT* bmtcp        /* in - BMT cell pointer */
    )
{
    bsMRT* rp;
    char* rtn;
    int freebytes;

    /* walk through existing records in cell to find free space */

    rp = (struct bsMR*)bmtcp->bsMR0;

    while (rp->type != BSR_NIL) {
        rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( uint64_t) ));
    }

    freebytes = BSC_R_SZ -
                ((char*)rp - (char*)bmtcp->bsMR0 + 2 * sizeof(struct bsMR));

    if (freebytes < (int) bcnt) {
        /*
         * There is not enough free space in this mcell for the record.
         */
        return NULL;
    }

    rp->bCnt = (uint16_t)(bcnt + sizeof(struct bsMR));
    rp->type = type;
    rtn = (char*)rp + sizeof(struct bsMR);

    /* init terminating NIL record */
    rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( uint64_t ) ));
    rp->bCnt = (uint16_t)sizeof(struct bsMR);
    rp->type = BSR_NIL;
    rp->padding = 0;

    return (void*)rtn;
}

/*
 * bmtr_find - find an mcell record
 *
 * If found,
 *   return a pointer to the desired record in the cell.
 *
 * If not found 
 *   returns NULL
 *
 * If corruption detected
 *   panic the domain
 *   return NULL
 *
 * Caller must hold the bitfile's mcellList_lk locked.  See note above.
 */
 
void*
bmtr_find(
          struct bsMC* bsMCp,       /* in - pointer to cell */
          int rtype,                /* in - record type to find */
          domainT *dmnP             /* in - domain pointer (advfs_size() may
                                     *      pass a NULL when we do not want
                                     *      to panic just because we could
                                     *      not file the record) */
          )
{
    struct bsMR* rp;


    /*
     * scan cell for desired record type.  all cells contain a 
     * NIL type record as a "stopper".
     */

    rp = (struct bsMR *) bsMCp->bsMR0;

    while ((rp->type != rtype) && (rp->type != BSR_NIL)) {
        /*
         * This check prevents infinite loop on a bogus record
         */
        if (rp->bCnt == 0 || rp->bCnt > BSC_R_SZ) {
            if ( dmnP != NULL ) {
                /* advfs_size() passes NULL to prevent panic */
                domain_panic(dmnP,"bmtr_find: corrupt bmt record header");
            }
            return NULL;
        }
        rp = (struct bsMR *) ((char *)rp + roundup( rp->bCnt, sizeof( uint64_t )));
    }

    if (rp->type == BSR_NIL) {
        return NULL;
    } else {
        return (char*)rp + sizeof(struct bsMR);
    }
}

/*
 * bmtr_assign_rec
 *
 * Same as bmtr_assign() except that this version logs the
 * changes to the mcell that it makes (for ftx recover/failure
 * purposes).  The changes are not undoable (only redoable).
 *
 * Caller must hold the bitfile's mcellList_lk locked.
 */

void *
bmtr_assign_rec(
    uint16_t type,        /* in - type of record */
    uint16_t bcnt,        /* in - size of record */
    bsMCT* bmtcp,        /* in - BMT cell pointer */
    rbfPgRefHT pgref     /* in - mcell's page handle */
    )
{
    bsMRT* rp;
    char* rtn;
    int freebytes;

    /* walk through existing records in cell to find free space */

    rp = (struct bsMR*)bmtcp->bsMR0;

    while (rp->type != BSR_NIL) {
        rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( uint64_t ) ));
    }

    freebytes = BSC_R_SZ -
                ((char*)rp - (char*)bmtcp->bsMR0 + 2 * sizeof(struct bsMR));

    if (freebytes < (int) bcnt) {
        /*
         ** There is not enough room in the mcell for the record.
         */
        return NULL;
    }

    rbf_pin_record( pgref, rp, sizeof( bsMRT ) );
    rp->bCnt = bcnt + sizeof(struct bsMR);
    rp->type = type;
    rtn = (char*)rp + sizeof(struct bsMR);
    ADVFS_CRASH_RECOVERY_TEST(pgref.dmnP, AdvfsCrash_79);

    rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( uint64_t )));

    /* init terminating NIL record */
    rbf_pin_record( pgref, rp, sizeof( bsMRT ) );
    rp->bCnt = sizeof(struct bsMR);
    rp->bCnt = 0;
    rp->type = BSR_NIL;
    rp->padding = 0;
    ADVFS_CRASH_RECOVERY_TEST(pgref.dmnP, AdvfsCrash_80);

    return (void*)rtn;
}

/*
 * bmtr_create_rec
 *
 * Creates a new mcell record;  it may as a side effect also
 * create a new mcell and link it into the bitfile's mcell list.
 * The effects of this routine are not undoable (only redoable).
 *
 * Caller must hold the bitfile's mcellList_lk locked.
 *
 * Caller must have started a transaction.
 *
 * If the mcell list is a bitfile's metadata list, the caller must hold
 * hold the bitfile's moveMetadata_lk lock (shared).
 */

static statusT
bmtr_create_rec(
    bfAccessT *bfAccp,    /* in - bitfiles's access structure */
    bfMCIdT *mcid,        /* in - mcell id (primary/found) */
    vdT *vdp,             /* in - vd (of mcell) pointer */
    void *bPtr,           /* in - contents of new record */
    uint16_t rSize,       /* in - size of new record */
    int rType,            /* in - record type */
    ftxHT ftxH            /* in - parent ftx handle */
    )
{
    statusT sts = EOK;
    bs_meta_page_t newpage = -1;
    int newVd = FALSE;
    struct bsMPg *bmtp = NULL;
    struct bsMC *mcp = NULL;
    void* rPtr = NULL;
    rbfPgRefHT pgref;
    int ftxStarted = FALSE;
    domainT *dmnP;
    bfAccessT *mdap;
    int file_is_rbmt;
    bfMCIdT currMcellId;
    uint32_t linkSeg;

    dmnP = vdp->dmnP;

    /*
     * If the file that needs a new record is the RBMT, we will
     * pin pages in that file.  Otherwise, we will pin pages in
     * the BMT.
     */
    if ((BS_BFTAG_RBMT(bfAccp->tag)) &&
        (BS_BFTAG_RSVD(bfAccp->tag))) {
        file_is_rbmt = TRUE;
        mdap = vdp->rbmtp;
    }
    else {
        file_is_rbmt = FALSE;
        mdap = vdp->bmtp;
    }

    /* access the BMT or RBMT page */

    sts = rbf_pinpg( &pgref, (void*)&bmtp,
                     mdap, mcid->page, BS_NIL, ftxH, MF_VERIFY_PAGE );
    if (sts != EOK) { RAISE_EXCEPTION( sts ); }

    mcp = &bmtp->bsMCA[ mcid->cell ];

    if (!BS_BFTAG_EQL(mcp->mcTag, bfAccp->tag)) {RAISE_EXCEPTION( EBAD_TAG );}


    /*
     ** Search for an mcell that can hold the new record.  The while
     ** loop will terminate it successfully inserts the record or it
     ** reaches the end of the bitfile's mcell list.
     */

    while ((rPtr = bmtr_assign_rec( rType, rSize, mcp, pgref )) == NULL) {

        /* Go to next chained cell */

        if (FETCH_MC_VOLUME(mcp->mcNextMCId) != vdp->vdIndex) {
            if (mcp->mcNextMCId.volume == 0) {
                /* end of mcell chain */
                break;
            }

            /*
             * Next mcell is on a different disk
             * NOTE: RBMT mcells MUST be on the same volume!
             */
            if (file_is_rbmt) {
                domain_panic(dmnP,
                        "bmtr_create_rec: corrupt RBMT on volume = %d, "
                        "page = %ld;  Invalid nextMCId ( %d, %ld, %d) "
                        "found in cell %d",
                        bmtp->bmtVdIndex, bmtp->bmtPageId,
                        mcp->mcNextMCId.volume, mcp->mcNextMCId.page,
                        mcp->mcNextMCId.cell, mcid->cell);
                RAISE_EXCEPTION(E_DOMAIN_PANIC);        
            }

            vdp = VD_HTOP(mcp->mcNextMCId.volume, dmnP);
            newVd = TRUE;
            mdap = vdp->bmtp;
        }

        newpage = FETCH_MC_PAGE(mcp->mcNextMCId);
        *mcid = mcp->mcNextMCId;

        if ((newpage != bmtp->bmtPageId) || newVd) {
            /* Need to get next BMT or RBMT page */

            rbf_deref_page( pgref, BS_CACHE_IT );

            sts = rbf_pinpg( &pgref, (void *)&bmtp,
                             mdap, newpage, BS_NIL, ftxH, MF_VERIFY_PAGE );

            if (sts != EOK) {RAISE_EXCEPTION( sts ); }
        }

        mcp = &bmtp->bsMCA[mcid->cell]; /* get pointer to new cell */

        if (!BS_BFTAG_EQL(mcp->mcTag, bfAccp->tag)) {
            /* something's not right */
            RAISE_EXCEPTION( EBAD_TAG );
        }

        /* Assert that cell RBMT_RSVD_CELL (20) for the rbmt never contains
         * anything but BSR_XTRA_XTNTS.  If this occurs it is probably due
         * to a bug where the cell 6(bmt extension cell)
         * mistakenly pointed to cell RBMT_RSVD_CELL when it should have had
         * the mcell chain terminated within cell 6.
         *
         * If new code is adding enough records to fill cell 6 and it
         * needs to allocate another cell it cannot fill cell BMT_RSVD_CELL
         * because the extent chain for the rbmt MUST go there.
         */
        if((file_is_rbmt) && (mcid->cell == RBMT_RSVD_CELL)) {
            domain_panic(dmnP,"bmtr_create_rec: attempt to use RBMT_RSVD_CELL");
            RAISE_EXCEPTION(E_DOMAIN_PANIC);
        }
    }

    if (rPtr == NULL) {

        /*
         * Save end-of-chain mcell to link new mcell at the end and to ensure
         * link segment numbers are corrrect.
         */
        currMcellId = *mcid;
        linkSeg = mcp->mcLinkSegment;

        /*
         * Above loop didn't find space for
         * the new record.  So we'll allocate a new mcell
         * and link it into the bitfile's mcell list.
         */

        rbf_deref_page( pgref, BS_CACHE_IT );

        
        if (file_is_rbmt) {

            if (currMcellId.volume != bfAccp->primMCId.volume) {
                domain_panic(dmnP,
                        "bmtr_create_rec: corrupt RBMT on volume = %d, "
                        "page = %ld; primary mcell (%d, %ld, %d) chain "
                        "includes invalid mcellId (%d, %ld, %d) \n",
                        bmtp->bmtVdIndex, bmtp->bmtPageId,
                        bfAccp->primMCId.volume, bfAccp->primMCId.page,
                        bfAccp->primMCId.cell, currMcellId.volume,
                        currMcellId.page, currMcellId.cell);
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }
            
            sts = bmt_alloc_mcell (bfAccp,
                                   (vdIndexT) bfAccp->primMCId.volume,
                                   RBMT_MCELL,
                                   bfAccp->bfSetp->dirTag,
                                   bfAccp->tag,
                                   linkSeg + 1,
                                   ftxH,
                                   mcid,
                                   FALSE
                                   );
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            MS_SMP_ASSERT(mcid->volume == bfAccp->primMCId.volume);
            
            /* Assert that cell RBMT_RSVD_CELL (20) for the rbmt never
             * contains anything but BSR_XTRA_XTNTS.  If this occurs it is
             * probably due to a bug where the cell 6 (bmt
             * extension cell) mistakenly pointed to cell RBMT_RSVD_CELL
             * when it should have had the mcell chain terminated within
             * cell 6.
             *
             * If new code is adding enough records to fill cell 6 and 
             * it needs to allocate another cell it cannot fill cell
             * RBMT_RSVD_CELL because the extent chain for the rbmt MUST
             * go there.
             * If this ever occurs, fix up this code path to not chain
             * to cell RBMT_RSVD_CELL but instead allocate a non-cell
             * RBMT_RSVD_CELL but instead allocate a non-cell RBMT_RSVD_CELL
             * mcell.
             */
            if (mcid->cell == RBMT_RSVD_CELL) {
                domain_panic(dmnP,"bmtr_create_rec: attempt to use "
                                  "RBMT_RSVD_CELL");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);     
            }

            /*
             * Link the mcells together
             */
            sts = bmt_link_mcells(dmnP, bfAccp->tag, currMcellId, *mcid,
                                  *mcid, ftxH); 
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
        } else {

            /*
             * NOTE: bmt_alloc_link_mcell() creates a sub-transaction in the
             * current transaction tree.  This ftx sets a special done
             * mode to skip it's undo so that the new mcell will not be
             * returned.
             */

            sts = bmt_alloc_link_mcell( bfAccp->dmnP,
                                        (vdIndexT)currMcellId.volume,
                                        BMT_NORMAL_MCELL,
                                        currMcellId,
                                        ftxH,
                                        mcid,
			    	        NULL );
            if (sts != EOK) {

                /*
                 * Most likely the vd that contains the bitfile's last 
                 * mcell is out of mcells.  So, next we try a different vd.
                 */

                sts = sc_select_vd_for_mcell( &vdp,
                                              bfAccp->dmnP,
                                              dmnP->scTbl,
                                              bfAccp->reqServices );
                if (sts != EOK) {
                    RAISE_EXCEPTION( ENO_MORE_MCELLS );
                }

                sts = bmt_alloc_link_mcell( bfAccp->dmnP,
                                            vdp->vdIndex,
                                            BMT_NORMAL_MCELL,
                                            currMcellId,
                                            ftxH,
                                            mcid,
				    	    NULL );
                vd_dec_refcnt(vdp);
                if (sts != EOK) {
                    RAISE_EXCEPTION( sts );
                }
            } else {
                vdp = VD_HTOP(currMcellId.volume, dmnP);
            }

            /*
             * Set done mode to skip undo for sub ftxs, in this case
             * bmt_alloc_link_mcell.
             */
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_81);
            ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_82);
        }

        mdap = (file_is_rbmt ? vdp->rbmtp : vdp->bmtp);

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid->page, BS_NIL, ftxH, MF_VERIFY_PAGE );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        mcp = &bmtp->bsMCA[ mcid->cell ];

        rPtr = bmtr_assign_rec( rType, rSize, mcp, pgref );
        if (rPtr == NULL) {
            ADVFS_SAD0( "bmtr_create_rec: can't assign attr record" );
        }
    }

    rbf_pin_record( pgref, rPtr, rSize );
    flmemcpy( (char *)bPtr, rPtr, rSize );

    if ( !file_is_rbmt) {
        if (rType <= BSR_API_MAX) {
            dmnP->bmtStat.bmtRecWrite[rType]++;
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_83);
        } else if (rType == BMTR_FS_STAT) {
            dmnP->bmtStat.fStatWrite++;
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_84);
        }
    }

    return EOK;

HANDLE_EXCEPTION:

    return sts;
}

/*
 * bmtr_scan_mcells
 *
 * Like bmtr_find except it will search all chained cells and search across
 * vds.
 *
 * NOTE:  The current BMT page will be referenced
 * when the routine returns with status EOK.  It is the
 * caller's responsibility to deref the page in that case.
 *
 * Other than EOK possibly returns status from bs_refpg, EBAD_TAG,
 * E_BAD_MCELL_LINK_SEGMENT, EBMTR_NOT_FOUND, or EBAD_TAG
 *
 * Caller must hold the bitfile's mcellList_lk locked.
 */

statusT
bmtr_scan_mcells(
    bfMCIdT *mcid,              /* in/out - mcell id (primary/found) */
    vdT **vdp,                  /* in/out - vd (of mcell) pointer */
    void **bsrp,                /* out - pointer to BMT record */
    bfPageRefHT *pgRef,         /* out - pgref of BMT cell's page */
    uint32_t *recOffset,        /* out - record's mcell offset */
    uint16_t *rSize,            /* out - record's size in bytes */
    const int rtype,            /* in - record type to find */
    const bfTagT tag            /* in - bitfile's tag */
    )
{
    statusT sts = EOK;
    bs_meta_page_t newpage = -1;
    int newVd = FALSE;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    int pgRefed = FALSE;
    bsMRT* rp;
    domainT *dmnP;
    bfAccessT *mdap;

    *bsrp = NULL;

    dmnP = (*vdp)->dmnP;

    /* access the metadata bitfile page */

    if ( BS_BFTAG_RSVD(tag) ) {
        mdap = (*vdp)->rbmtp;
    } else {
        mdap = (*vdp)->bmtp;
    }

    if (mdap == NULL) {
        if (( mcid->cell != 0 ) || ( mcid->page != 0 )) {
            domain_panic(dmnP, "bmtr_scan_mcells(1): BMT not mapped");
            RAISE_EXCEPTION(E_DOMAIN_PANIC);
        }
        /* special case - metadata bitfile not mapped yet */

        bmtp = dmnP->metaPagep;
        *pgRef = NilBfPageRefH;
    } else {
        /* access the metadata bitfile page */
        sts = bs_refpg(pgRef,
                       (void*)&bmtp,
                       mdap,
                       mcid->page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            return( sts );
        }
        pgRefed = TRUE;
    }

    mcp = &bmtp->bsMCA[ mcid->cell ];

    if (!BS_BFTAG_EQL( mcp->mcTag, tag )) {
        RAISE_EXCEPTION( EBAD_TAG );
    }

    /*
     ** Search for an mcell that contains new record.  The while
     ** loop will terminate if it successfully finds the record or
     ** it reaches the end of the bitfile's mcell list.
     */

    while ((*bsrp = bmtr_find( mcp, rtype, (*vdp)->dmnP)) == NULL) {

        /* Go to next chained cell */

        if (FETCH_MC_VOLUME(mcp->mcNextMCId) != (*vdp)->vdIndex) {
            if (mcp->mcNextMCId.volume == 0) {
                /* end of mcell chain */
                RAISE_EXCEPTION( EBMTR_NOT_FOUND );
            }

            /* Next mcell is on a different disk */

            *vdp = VD_HTOP(mcp->mcNextMCId.volume, dmnP);
            newVd = TRUE;
            /*
             * Reset access structure to new volume's BMT.
             * No possibility of needing to reset this to
             * the new volumes's RBMT since the RBMT only
             * maps the BMT and the BMT doesn't span volumes.
             */
            MS_SMP_ASSERT(!BS_BFTAG_RSVD(tag));
            mdap = (*vdp)->bmtp;
        }

        newpage = FETCH_MC_PAGE(mcp->mcNextMCId);
        *mcid = mcp->mcNextMCId;

        if ((newpage != bmtp->bmtPageId) || newVd ) {
            /* Need to get next BMT page */

            if (!pgRefed) {
                /* BMT is not mapped yet.  Can't access beyond pg 0 */
                domain_panic(dmnP, "bmtr_scan_mcells(2): BMT not mapped");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }

            (void) bs_derefpg( *pgRef, BS_CACHE_IT );
            pgRefed = FALSE;

            sts = bs_refpg(pgRef,
                           (void*)&bmtp,
                           mdap,
                           newpage,
                           FtxNilFtxH,
                           MF_VERIFY_PAGE);
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
            pgRefed = TRUE;

            newVd = FALSE;
        }

        mcp = &bmtp->bsMCA[mcid->cell]; /* get pointer to new cell */

        if (!BS_BFTAG_EQL( mcp->mcTag, tag )) {
            /* something's not right */
            RAISE_EXCEPTION( EBAD_TAG );
        }
    }

    if (*bsrp == NULL) {
        /*
         ** Above loop didn't find the record.
         */
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    rp = (bsMRT *) ((char *)*bsrp - sizeof( struct bsMR ));
    *rSize = rp->bCnt - sizeof(struct bsMR);
    *recOffset = (uint32_t) ((char *)*bsrp - (char *) mcp);

    return EOK;

HANDLE_EXCEPTION:

    if (pgRefed) {
        (void) bs_derefpg( *pgRef, BS_CACHE_IT );
    }

    return sts;
}

/*
 * advfs_bmtr_mcell_refpg
 *
 * Wrapper for reading mcells more easily.
 *
 * Returns EOK on success.  Returns error from bs_refpg.
 */
statusT
advfs_bmtr_mcell_refpg(
    bfAccessT *bfap,
    bfMCIdT mcell_id,
    bsMCT **mcell_ptr,
    bfPageRefHT *page_ref )
{
    statusT sts;
    bsMPgT *page_ptr=NULL;

    /* Subtract one from the volume because volume indexes start at one */
    sts = bs_refpg( page_ref,
                    (void **)&page_ptr,
                    bfap->dmnP->vdpTbl[ mcell_id.volume - 1 ]->bmtp,
                    mcell_id.page,
                    FtxNilFtxH,
                    MF_VERIFY_PAGE );
    if (sts != EOK)
        return sts;

    *mcell_ptr = &page_ptr->bsMCA[ mcell_id.cell ];

    return EOK;
}

/*
 * bmtr_scan
 *
 * Like bmtr_find except it will search all chained cells.  The
 * search is terminated if the mcell chain continues on another
 * disk.  Therefore, this routine is suitable only for locating
 * bitfile attributes, not extra extent maps.
 *
 * OLD INTERFACE: This is an older version of bmtr_scan_mcells() that
 * did not find mcells on other disks.  This interface preserves that
 * behavior.
 *
 * NOTE:  The current BMT page will be referenced
 * when get_bmtr returns with status EOK.  It is the
 * caller's responsibility to deref the page in that case.
 *
 * Other than EOK possibly returns status from bs_refpg, EBAD_TAG,
 * E_BAD_MCELL_LINK_SEGMENT, EBMTR_NOT_FOUND, or EBAD_TAG
 *
 * Caller must hold the bitfile's mcellList_lk locked.  See not above.
 */

statusT
bmtr_scan(
          void **bsrp,          /* out - pointer to BMT record */
          bfPageRefHT *pgref,   /* out - pgref of BMT cell's page */
          int rtype,            /* in - record type to find */
          struct vd *vdp,       /* in - vd (of mcell) pointer */
          bfMCIdT *mcid,        /* in/out - mcell id (primary/found) */
          bfTagT tag            /* in - bitfile's tag */
          )
{
    statusT sts;
    uint32_t recOffset;
    uint16_t recSize;
    vdT *mcellVdp = vdp;

    sts = bmtr_scan_mcells( mcid,
                            &mcellVdp,
                            bsrp,
                            pgref,
                            &recOffset,
                            &recSize,
                            rtype,
                            tag );

    if (sts != EOK) {
        return sts;
    }

    if (vdp != mcellVdp) {
        /*
         * bmtr_scan() is not supposed to find mcells on other disks; which
         * is what happened here.  So, deref the page and return error.
         */
        (void) bs_derefpg( *pgref, BS_CACHE_IT );
        return EBMTR_NOT_FOUND;
    }

    return EOK;
}

/*
 * bmtr_get_rec_ptr
 */

statusT
bmtr_get_rec_ptr(
    bfAccessT *bfap,            /* in - bf access struct ptr */
    ftxHT parentFtxH,           /* in - transaction handle */
    uint16_t rType,              /* in - type of record */
    uint16_t bSize,              /* in - size of buffer */
    int pinPg,                  /* in - 1 == pin pg, 0 == ref pg */
    void **bPtr,                /* out - ptr to buffer */
    rbfPgRefHT *pinPgH,         /* out - pg handle (if pinPg == 1) */
    bfPageRefHT *refPgH         /* out - pg handle (if pinPg == 0) */
    )
{
    statusT sts;
    vdT *vdp;
    rbfPgRefHT pgref;
    bfPageRefHT refpgref;
    bfMCIdT mcid;
    char *recp = NULL;
    uint32_t recOffset;
    int lkLocked = FALSE;
    uint16_t recSize;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    bfAccessT *mdap;

    if (bSize > BS_USABLE_MCELL_SPACE) {
        ADVFS_SAD1( "bmtr_get_rec_ptr: invalid request", bSize );
    }

    if ( parentFtxH.hndl == FtxNilFtxH.hndl ) {
        ADVFS_SAD0( "bmtr_get_rec_ptr: missing ftxH" );
    }

    ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
    lkLocked = TRUE;

    /*
     * find record
     */

    vdp = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    mcid = bfap->primMCId;

    sts = bmtr_scan_mcells( &mcid,
                            &vdp,
                            (void *) &recp,
                            &refpgref,
                            &recOffset,
                            &recSize,
                            rType,
                            bfap->tag );
    if ( sts != EOK ) {
        /* rec not found */
        RAISE_EXCEPTION( sts );
    }

    if (recSize < bSize) {
        /* rec is not big enough */
        (void) bs_derefpg( refpgref, BS_CACHE_IT );
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    lkLocked = FALSE;

    if (pinPg) {
        (void) bs_derefpg( refpgref, BS_CACHE_IT );

        if (refPgH != NULL) {
            ADVFS_SAD0( "bmtr_get_rec_ptr: refPgH must be NULL" );
        }

        /*
         * If the file whose record is being updated is the RBMT,
         * we will pin a page in that file.  Otherwise, we will pin
         * a page in the BMT.
         */

        if ((BS_BFTAG_RBMT(bfap->tag)) &&
            (BS_BFTAG_RSVD(bfap->tag))) {
            mdap = vdp->rbmtp;
        }
        else {
            mdap = vdp->bmtp;
        }

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid.page, BS_NIL, parentFtxH, MF_VERIFY_PAGE );
        if (sts != EOK) { RAISE_EXCEPTION( sts ); }

        mcp = &bmtp->bsMCA[ mcid.cell ];
        recp = (char *)mcp + recOffset;


        if (rType <= BSR_API_MAX) {
            bfap->dmnP->bmtStat.bmtRecWrite[rType]++;

        } else if (rType == BMTR_FS_STAT) {
            bfap->dmnP->bmtStat.fStatWrite++;
        }

        *pinPgH = pgref;

    } else {
        if (pinPgH != NULL) {
            ADVFS_SAD0( "bmtr_get_rec_ptr: pinPgH must be NULL" );
        }

        if (rType <= BSR_API_MAX) {
            bfap->dmnP->bmtStat.bmtRecRead[rType]++;

        } else if (rType == BMTR_FS_STAT) {
            bfap->dmnP->bmtStat.fStatRead++;
        }

        *refPgH = refpgref;
    }

    *bPtr = (void *)recp;

    return EOK;

HANDLE_EXCEPTION:

    if (lkLocked) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    }

    return sts;
}

/*
 * bmtr_get_rec_n_lk
 *
 * 'reads' an mcell record into the caller's buffer.
 * If sucessfull, return EOK
 * If error , return error code
 *
 * If lk == BMTR_LOCK then this routine will also leave the
 * mcellList_lk locked for write.   
 *
 * Note that if an error occurs then 'lk' is ignored and the mcellList_lk
 * is unlocked.
 */

statusT
bmtr_get_rec_n_lk(
    bfAccessT *bfap,            /* in - bf access structure */
    uint16_t rType,              /* in - type of record */
    void *bPtr,                 /* in/out - ptr to buffer */
    uint16_t bSize,              /* in - size of buffer */
    bmtrLkT lk                  /* in - keep mcell locked? */
    )
{
    vdT *vdp = NULL;
    bfPageRefHT pgref;
    bfMCIdT mcid;
    char *recp = NULL;
    statusT sts;
    uint32_t recOffset;
    uint16_t recSize;
    int pgRefed = FALSE, mcellListLocked = FALSE;

    /*
     * Check validity of parameters.
     */
    if ((bfap == NULL) || (bSize > BS_USABLE_MCELL_SPACE)) {
        RAISE_EXCEPTION(EBAD_PARAMS);
    }

    /*
     * If the caller does not want to have the mcell list locked
     * upon return from this function, just lock the list for
     * shared access while we read the chain.  This will allow
     * multiple readers concurrently.
     */
    if (lk == BMTR_NO_LOCK) {
        ADVRWL_MCELL_LIST_READ( &bfap->mcellList_lk );
    }
    else {
        ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
    }
    mcellListLocked = TRUE;

    /*
     * find record
     */

    vdp  = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    mcid = bfap->primMCId;

    sts = bmtr_scan_mcells( &mcid,
                            &vdp,
                            (void *)&recp,
                            &pgref,
                            &recOffset,
                            &recSize,
                            rType,
                            bfap->tag );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    pgRefed = TRUE;

    if (recSize > bSize) {
        RAISE_EXCEPTION(EBMTR_NOT_FOUND);
    }

    /*
     ** Copy the mcell record to the callers buffer.
     */

    bcopy( recp, (char *)bPtr, recSize );

    (void) bs_derefpg( pgref, BS_CACHE_IT );

    if (lk == BMTR_NO_LOCK) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
        mcellListLocked = FALSE;
    }

    if (rType <= BSR_API_MAX) {
        bfap->dmnP->bmtStat.bmtRecWrite[rType]++;

    } else if (rType == BMTR_FS_STAT) {
        bfap->dmnP->bmtStat.fStatWrite++;
    }

    return EOK;

HANDLE_EXCEPTION:

    if (pgRefed) {
        (void) bs_derefpg( pgref, BS_CACHE_IT );
    }

    if (mcellListLocked) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    }

    return sts;
}

/*
 * bmtrPutRecUndoT
 *
 * Struct used for undoing the effects of bmtr_put_rec().
 */

typedef struct {
    bfTagT  bfTag;              /* bitfile's tag */
    bfSetIdT bfSetId;           /* bitfile's set id */
    bfMCIdT mcid;               /* mcell id of record's mcell */
    uint32_t recOffset;         /* record's byte offset into mcell */
    uint16_t beforeImageLen;    /* bytes in 'beforeImage' */
    char beforeImage[BS_USABLE_MCELL_SPACE];  /* before image of mcell rec */
} bmtrPutRecUndoT;

/*
 * bmtr_put_rec_undo_opx
 *
 * Restore's the previous contents of the mcell record described
 * by the undo record (opRec).
 */

opxT bmtr_put_rec_undo_opx;

void
bmtr_put_rec_undo_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    bmtrPutRecUndoT undoRec;
    statusT sts;
    struct bfAccess *bfap=NULL;
    bfSetT *bfSetp=NULL;
    domainT *dmnP;
    vdT *vdp;
    char *rDatap = NULL;
    rbfPgRefHT pgref;
    struct bsMPg* bmtp;
    struct bsMC* mcp;
    struct vnode *nullvp = NULL;

    /*
     * The undo record passed in may not completely fill up the
     * structure (it is sized for the max). So just copy in what
     * we need.
     */

    bcopy(opRec,&undoRec,opRecSz);

    dmnP = ftxH.dmnP;
    vdp = VD_HTOP(undoRec.mcid.volume, dmnP);

    if ( !BS_UID_EQL(undoRec.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRec.bfSetId.domainId, dmnP->dualMountId)) )
        {
            undoRec.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bmtr_put_rec_undo_opx: domainId mismatch");
        }
    }

    sts = bfs_open( &bfSetp, undoRec.bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) {
        domain_panic( dmnP,"bmtr_put_rec_undo_opx: bfs_open failed");
        goto HANDLE_EXCEPTION;
    }

    sts = bs_access(&bfap, undoRec.bfTag, bfSetp, ftxH, BF_OP_INTERNAL, &nullvp);
    if (sts != EOK) {
        domain_panic( dmnP,"bmtr_put_rec_undo_opx: bs_access() failed");
        goto HANDLE_EXCEPTION;
    }

    /*
     * In order to prevent a hierarchy violation we grab the
     * BfSetTblLock prior to locking the mcellList_lk.
     * Later, when we call bs_bfs_close() we will already
     * have the lock.
     */
    ADVRWL_BFSETTBL_WRITE( dmnP );

    FTX_LOCKWRITE( &bfap->mcellList_lk, ftxH );

    sts = rbf_pinpg( &pgref, (void*)&bmtp,
                     vdp->bmtp, undoRec.mcid.page, BS_NIL, ftxH, MF_NO_VERIFY);
    if (sts != EOK) {
        domain_panic(dmnP, "bmtr_put_rec_undo_opx: rbf_pinpg() failed, return code = %d", sts );
        goto HANDLE_EXCEPTION;
    }

    /*
     ** Restore the record's data.
     */

    mcp = &bmtp->bsMCA[ undoRec.mcid.cell ];
    rDatap = (char *)mcp + undoRec.recOffset;

    rbf_pin_record( pgref, rDatap, undoRec.beforeImageLen );
    flmemcpy( undoRec.beforeImage, rDatap, undoRec.beforeImageLen );
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_85);

HANDLE_EXCEPTION:
    if (bfap) bs_close(bfap, MSFS_CLOSE_NONE);
    if (bfSetp) bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

    return;
}



int
bmtr_max_rec_size( void )
{
    return BS_USABLE_MCELL_SPACE;
}

/*
 * bmtr_put_rec_n_unlk
 *
 * Copies the caller's buffer to the specified mcell record.  If
 * the record doesn't exist then it is created.  If the existing
 * record is too small then an error is returned.
 */

statusT
bmtr_put_rec_n_unlk(
    bfAccessT *bfap,            /* in - bf access structure */
    uint16_t rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    ftxIdT xid                  /* in - CFS transaction id */
    )
{
    bfSetT *bfSetp;
    statusT sts;
    snap_flags_t snap_flags = 0;
    int close_snap_children = FALSE;

    /*
     * Check validity of bfap.
     */
    if( bfap == NULL ) {
        return( EINVALID_HANDLE );
    }

    if (bSize > BS_USABLE_MCELL_SPACE) {
        return( EBAD_PARAMS );
    }

    bfSetp = bfap->bfSetp;

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        /* Probably a read-only snapshot */
        return( E_READ_ONLY );
    }

    /*
     * We are about to modify a record in the metadata for a file.  This
     * will change the data, so we need to make sure any child snapshots (if
     * they exist) have their own copy of the un-modified data.  Calling
     * advfs_access_snap_children will make sure that any children have
     * their own metadata.
     */
    if (HAS_SNAPSHOT_CHILD( bfap ) && 
            (bfap->bfaFirstChildSnap == NULL || (bfap->bfaFlags & BFA_SNAP_CHANGE))) {
        advfs_access_snap_children( bfap, parentFtxH, &snap_flags );
        if (snap_flags & SF_ACCESSED_CHILDREN) {
            close_snap_children = TRUE;
        }
    }

    sts =  bmtr_put_rec_n_unlk_int(
               bfap, rType, bPtr, bSize, parentFtxH, lk, xid );

    if (close_snap_children == TRUE) {
        ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
        advfs_close_snap_children(bfap);
        ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    }
    return(sts);
}

/*
 * bmtr_put_rec_n_unlk_int
 *
 * Copies the caller's buffer to the specified mcell record.  If
 * the record doesn't exist then it is created.  If the existing
 * record is too small then an error is returned.
 *
 * May return error if new mcell is needed and either no more blocks
 * or no more mcells are available.  Will return ENO_MORE_MCELLS in
 * either case.
 */

static statusT
bmtr_put_rec_n_unlk_int(
    bfAccessT* bfap,            /* in - bf access structure */
    uint16_t rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    ftxIdT xid                  /* in - CFS transaction id */
    )
{
    vdT *vdp = NULL;
    rbfPgRefHT pgref;
    bfPageRefHT refpgref;
    bfMCIdT mcid;
    char *recp = NULL;
    statusT sts;
    uint32_t recOffset;
    ftxHT ftxH;
    int lkLocked = FALSE, ftxStarted = FALSE;
    uint16_t recSize;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    bmtrPutRecUndoT *undoRecp = NULL;
    uint32_t undoRecSize;
    bfAccessT *mdap;

    undoRecp = (bmtrPutRecUndoT *) ms_malloc( sizeof( bmtrPutRecUndoT ));

    if ((lk != BMTR_NO_LOCK) && (parentFtxH.hndl == 0)) {
        ADVFS_SAD0( "bmtr_put_rec_n_unlk_int: caller holds mcellList_lk but has no ftx" );
    }

    sts = FTX_START_XID( FTA_BS_BMT_PUT_REC_V1, &ftxH, parentFtxH,
                       bfap->dmnP, xid );
    if (sts != EOK) {
        ADVFS_SAD1( "bmtr_put_rec_n_unlk_int: ftx start failed", sts );
    }
    ftxStarted = TRUE;

    if (lk == BMTR_NO_LOCK) {
        ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
        lkLocked = TRUE;
    }

    /*
     * find record
     */

    vdp  = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    mcid = bfap->primMCId;

    sts = bmtr_scan_mcells( &mcid,
                            &vdp,
                            (void *)&recp,
                            &refpgref,
                            &recOffset,
                            &recSize,
                            rType,
                            bfap->tag );
    if ( sts == EOK ) {
        (void) bs_derefpg( refpgref, BS_CACHE_IT );

        if (recSize < bSize) {
            RAISE_EXCEPTION( EBAD_PARAMS );
        }

        /*
         * The record already exists; update it.
         * Copy the caller's buffer to the new mcell record.
         * If the file whose record is being updated is the RBMT,
         * we will pin a page in that file.  Otherwise, we will pin
         * a page in the BMT.
         */
        if ((BS_BFTAG_RBMT(bfap->tag)) &&
            (BS_BFTAG_RSVD(bfap->tag))) {
            mdap = vdp->rbmtp;
        }
        else {
            mdap = vdp->bmtp;
        }

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid.page, BS_NIL, ftxH, MF_VERIFY_PAGE );
        if (sts != EOK) { RAISE_EXCEPTION( sts ); }

        mcp = &bmtp->bsMCA[ mcid.cell ];
        recp = (char *)mcp + recOffset;

        undoRecp->bfSetId = bfap->bfSetp->bfSetId;
        undoRecp->bfTag = bfap->tag;
        undoRecp->mcid = mcid;
        MS_SMP_ASSERT( FETCH_MC_VOLUME(undoRecp->mcid) == vdp->vdIndex); 
        undoRecp->recOffset = recOffset;
        undoRecp->beforeImageLen = bSize;

        flmemcpy( recp, undoRecp->beforeImage, bSize ); /* save before image */

        rbf_pin_record( pgref, recp, bSize );
        flmemcpy( (char *) bPtr, recp, bSize );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_86);

        undoRecSize = sizeof (bmtrPutRecUndoT) - (BS_USABLE_MCELL_SPACE - bSize);

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_87);
        ftx_done_u ( ftxH, FTA_BS_BMT_PUT_REC_V1,
                    undoRecSize, undoRecp);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_88);
    } else {
        /*
         ** The record doesn't already exist; create it.
         */

        mcid = bfap->primMCId;
        
        sts = bmtr_create_rec( bfap,
                               &mcid,
                               vdp,
                               bPtr,
                               bSize,
                               rType,
                               ftxH );

        if (sts == EOK) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_89);
            ftx_done_n( ftxH, FTA_BS_BMT_CREATE_REC );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_90);
        }
        if (sts != EOK) {
            if ( sts == ENO_MORE_BLKS ) {
                sts = ENO_MORE_MCELLS;
            }
            RAISE_EXCEPTION( sts );
        }
    }

    if ( lk != BMTR_IGNORE_LOCK) { 
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    }
    lkLocked = FALSE;

    ms_free( undoRecp );
    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_326);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_327);
    }

    if (lkLocked) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    }

    if (undoRecp != NULL) {
        ms_free( undoRecp );
    }
    return sts;
}

/*
 * bmtr_update_rec
 *
 * Copies the caller's buffer to the specified mcell record.
 * The record must already exist.  If the existing
 * record is too small then an error is returned.
 *
 * This routine is intended for cloneable bitfiles, and works like
 * bmtr_put_rec_n_unlk.
 *
 * This routine is not undoable, only redoable.  Reason:  Some callers
 * are manipulating linked lists maintained via BMT records.  These
 * callers don't want the simple Before-Image Undo that this routine
 * can implement.  These callers have their own Operational Undo.
 *
 * The parent ftx is used unless a nil handle is passed, in which case
 * a new root is started.
 */

statusT
bmtr_update_rec(
    bfAccessT* bfap,            /* in - bf access structure */
    uint16_t rType,             /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,             /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    uint32_t flags)             /* in - flags  */
{
    bfSetT *bfSetp;
    statusT sts;
    snap_flags_t snap_flags = 0;
    int close_snap_children = FALSE;

    /*
     * Check validity of bfap.
     */
    if( bfap == NULL ) {
        return( EINVALID_HANDLE );
    }

    bfSetp = bfap->bfSetp;

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        /* Probably a read-only snapshot */
        return( E_READ_ONLY );
    }

    /*
     * We are about to modify a record in the metadata for a file.  This
     * will change the data, so we need to make sure any child snapshots (if
     * they exist) have their own copy of the un-modified data.  Calling
     * advfs_access_snap_children will make sure that any children have
     * their own metadata.
     */
    if (HAS_SNAPSHOT_CHILD( bfap )  && !(flags & ADVFS_UPDATE_NO_COW) && 
       (bfap->bfaFirstChildSnap == NULL || (bfap->bfaFlags & BFA_SNAP_CHANGE))) {
        advfs_access_snap_children( bfap, parentFtxH, &snap_flags );
        if (snap_flags & SF_ACCESSED_CHILDREN) {
            close_snap_children = TRUE;
        }
    }

    sts = bmtr_update_rec_int( bfap, rType, bPtr, bSize,
                               parentFtxH, BMTR_NO_LOCK, flags );

    if (close_snap_children == TRUE) {
        ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
        advfs_close_snap_children(bfap);
        ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    }
    return(sts);
}

/*
 * bmtr_update_rec_n_unlk
 *
 * Copies the caller's buffer to the specified mcell record.
 * The record must already exist.  If the existing
 * record is too small then an error is returned.
 *
 * This routine may be used only on meta-data that is never
 * cloned (BMT, all TAGDIRS, LOG, BITMAP).  Actually, it can
 * be called for any bitfile as long as it doesn't have a clone
 * such that a call to this routine would cause a copy on write
 * to occur.  For, now this routine will abort if called for a clone
 * or a bitfile that has been cloned.
 *
 * This routine is not undoable, only redoable.  Reason:  Some callers
 * are manipulating linked lists maintained via BMT records.  These
 * callers don't want the simple Before-Image Undo that this routine
 * can implement.  These callers have their own Operational Undo.
 *
 * The parent ftx is used unless a nil handle is passed, in which case
 * a new root is started.
 */

statusT
bmtr_update_rec_n_unlk(
    bfAccessT *bfap,            /* in - bf access structure */
    uint16_t rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk
    )
{
    bfSetT *bfSetp;
    statusT sts;

    /*
     * Check validity of bfap.
     */
    if( bfap == NULL ) {
        return( EINVALID_HANDLE );
    }

    bfSetp = bfap->bfSetp;

    sts = bmtr_update_rec_int( bfap, rType, bPtr, bSize, parentFtxH, lk, 0 );
    return(sts);
}

/*
 * bmtr_update_rec_int
 *
 * Copies the caller's buffer to the specified mcell record.
 * The record must already exist.  If the existing
 * record is too small then an error is returned.
 *
 * This is the internal routine called from either
 * bmtr_update_rec_n_unlk or bmtr_update_rec jacket routines.
 *
 * This routine is not undoable, only redoable.  Reason:  Some callers
 * are manipulating linked lists maintained via BMT records.  These
 * callers don't want the simple Before-Image Undo that this routine
 * can implement.  These callers have their own Operational Undo.
 *
 * The parent ftx is used unless a nil handle is passed, in which case
 * a new root is started.
 */

statusT
bmtr_update_rec_int(
    bfAccessT* bfap,            /* in - ptr to bf access struct */
    uint16_t rType,             /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    uint16_t bSize,             /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,
    uint32_t flags)             /* in - flags */
{
    statusT sts;
    vdT *vdp;
    rbfPgRefHT pgref;
    bfPageRefHT refpgref;
    bfMCIdT mcid;
    char *recp = NULL;
    uint32_t recOffset;
    ftxHT ftxH;
    int ftxStarted = FALSE, lkLocked = FALSE;
    uint16_t recSize;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    bfAccessT *mdap;

    if (bSize > BS_USABLE_MCELL_SPACE) {
        return( EBAD_PARAMS );
    }

    if ( parentFtxH.hndl != FtxNilFtxH.hndl ) {
        ftxH = parentFtxH;
    } else {
        if (flags & FTX_NOWAIT) {
            sts = FTX_START_NOWAIT( FTA_BS_BMT_UPDATE_REC_V1, &ftxH,
                      parentFtxH, bfap->dmnP );
            if (sts == EWOULDBLOCK) {
                return( sts );
            }
            ftxStarted = TRUE;
        } else {
            sts = FTX_START_N( FTA_BS_BMT_UPDATE_REC_V1, &ftxH, parentFtxH,
                               bfap->dmnP);
            if (sts != EOK) {
                ADVFS_SAD1( "bmtr_update_rec_int: FTA_BS_BMT_UPDATE_REC_V1 ftx start failed", sts );
            }
            ftxStarted = TRUE;
        }
    }

    if (lk == BMTR_NO_LOCK) {
        ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
        lkLocked = TRUE;
    }

    /*
     * find record
     */

    vdp = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    mcid = bfap->primMCId;

    sts = bmtr_scan_mcells( &mcid,
                            &vdp,
                            (void *) &recp,
                            &refpgref,
                            &recOffset,
                            &recSize,
                            rType,
                            bfap->tag );
    if ( sts == EOK ) {
        (void) bs_derefpg( refpgref, BS_CACHE_IT );

        if (recSize < bSize) {
            RAISE_EXCEPTION( EBAD_PARAMS );
        }
    } else {
        /* rec not found */
        RAISE_EXCEPTION( sts );
    }

    /*
     * Copy the caller's buffer to the mcell record.
     * If the file whose record is being updated is the RBMT,
     * we will pin a page in that file.  Otherwise, we will pin
     * a page in the BMT.
     */

    if ((BS_BFTAG_RBMT(bfap->tag)) &&
        (BS_BFTAG_RSVD(bfap->tag))) {
        mdap = vdp->rbmtp;
    }
    else {
        mdap = vdp->bmtp;
    }

    sts = rbf_pinpg( &pgref, (void*)&bmtp,
                     mdap, mcid.page, BS_NIL, ftxH, MF_VERIFY_PAGE );
    if (sts != EOK) { RAISE_EXCEPTION( sts ); }

    mcp = &bmtp->bsMCA[ mcid.cell ];
    recp = (char *)mcp + recOffset;

    rbf_pin_record( pgref, recp, bSize );
    flmemcpy( (char *) bPtr, recp, bSize );
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_91);

    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_92);
        ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_93);
    }

    ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    lkLocked = FALSE;

    if (rType <= BSR_API_MAX) {
        bfap->dmnP->bmtStat.bmtRecWrite[rType]++;

    } else if (rType == BMTR_FS_STAT) {
        bfap->dmnP->bmtStat.fStatWrite++;
    }

    return EOK;

HANDLE_EXCEPTION:

    if (lkLocked) {
        ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
    }

    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_328);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_329);
    }

    return sts;
}

/*
 * bmt_alloc_prim_mcell
 *
 * This routine inits a primary mcell.  It will update
 * the global free mcell free list head (in the vd and on-disk) if
 * necessary.  It will also extend the bmt if necessary.
 *
 * Returns mcell pointer.
 * A NULL pointer will be returned if no mcells are available.
 *
 * NOTE: It is assumed that the caller of this routine holds the
 * mcell_lk, which serializes access to the per page and global
 * free mcell page links.
 */

statusT
bmt_alloc_prim_mcell(
               ftxHT ftxH,              /* in - callers ftx handle */
               mcellUIdT* mcellUIdp,    /* in/out - mcelluid ptr */
               vdT* vdp,                /* in - pointer to virtual disk */
               rbfPgRefHT* pgRefp,      /* out - free mcell page ref */
               struct bsMC **p_mcp      /* out - New mcell */
               )
{
    statusT sts;
    bfMCIdT mcid;
    struct bsMC* mcp;
    rbfPgRefHT pgref;

    MS_SMP_ASSERT( ADVFS_MUTEX_OWNED(&vdp->mcell_lk.lock.mtx));
    sts = alloc_mcell (
                       vdp,
                       ftxH,
                       &mcellUIdp->mcell,
                       &pgref,
                       &mcp,
                       BMT_NORMAL_MCELL
                       );
    if (sts != EOK) {
        return sts;
    }

    init_mcell (pgref, mcp, mcellUIdp->ut.bfsid.dirTag,
                mcellUIdp->ut.tag, 0);

    *pgRefp = pgref;
    *p_mcp = mcp;

    return EOK;
}


typedef struct allocMcellUndoRec {
    bfMCIdT newMcellId;
} allocMcellUndoRecT;

/*
 * alloc_mcell_undo
 *
 * This function "undoes" the actions performed by the bmt_alloc_mcell()
 * function.  This function is only be called for 'normal' mcells in V3
 * domains NOT BMT Page 0 and V4 domains BMT.
 *
 * This function assumes that the transaction was a sub-transaction and that
 * the parent transaction's do or undo routine has taken the appropriate action to
 * to synchronize access to this mcell.  During run-time, the parent transaction's
 * do routine syncs access.  During recovery, the parent transaction's undo
 * routine syncs access.
 *
 * This function assumes that the mcell is not on any list and that all of the
 * mcell's records are inactive.
 */

void
alloc_mcell_undo (
                  ftxHT ftx,
                  int undoRecSize,
                  void *undoRecAddr
                  )
{

    bsMPgT *bmt;
    domainT *dmnP;
    struct bsMC *mcell;
    rbfPgRefHT pgPin;
    statusT sts;
    allocMcellUndoRecT undoRec;
    vdT *vd;

    if (undoRecSize != sizeof (allocMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_mcell_undo: bad undo record size" );
    }
    bcopy(undoRecAddr, &undoRec, sizeof(allocMcellUndoRecT));

    dmnP = ftx.dmnP;
    vd = VD_HTOP(undoRec.newMcellId.volume, dmnP);

    FTX_LOCKMUTEX(&(vd->mcell_lk), ftx);

    /*
     * Pin the mcell's page and free the mcell.
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&bmt,
                     vd->bmtp,
                     undoRec.newMcellId.page,
                     BS_NIL,
                     ftx,
                     MF_NO_VERIFY 
                     );
    if (sts != EOK) {
        /* FIX - correct action? */
        domain_panic(dmnP,  "alloc_mcell_undo: rbf_pinpg failed, return code = %d", sts );
        return;
    }
    if (bmt->bmtPageId != FETCH_MC_PAGE(undoRec.newMcellId)) {
        domain_panic(dmnP, "alloc_mcell_undo: got bmt page %ld instead of %ld",
                    bmt->bmtPageId, undoRec.newMcellId.page );
        return;
    }

    mcell = &(bmt->bsMCA[undoRec.newMcellId.cell]);

    bmt_free_mcell (
                    ftx,
                    vd,
                    mcell,
                    undoRec.newMcellId,
                    bmt,
                    pgPin
                    );

    return;

}  /* end alloc_mcell_undo */

/*
 * alloc_rbmt_mcell_undo
 *
 * This function "undoes" the actions performed by the bmt_alloc_mcell()
 * function when an mcell was allocated from the RBMT.  This routine is
 * called when we are modifying either the RBMT or BMT Page0 in a V3 domain.
 *
 * This function assumes that the transaction was a sub-transaction and that
 * the parent transaction's do or undo routine has taken the appropriate action to
 * to synchronize access to this mcell.  During run-time, the parent transaction's
 * do routine syncs access.  During recovery, the parent transaction's undo
 * routine syncs access.
 *
 * This function assumes that the mcell is not on any list and that all of the
 * mcell's records are inactive.
 */

void
alloc_rbmt_mcell_undo (
                  ftxHT ftx,
                  int undoRecSize,
                  void *undoRecAddr
                  )
{

    bsMPgT *bmt;
    domainT *dmnP;
    struct bsMC *mcell;
    rbfPgRefHT pgPin;
    statusT sts;
    allocMcellUndoRecT undoRec;
    vdT *vd;

    if (undoRecSize != sizeof (allocMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_rbmt_mcell_undo --- bad undo record size" );
    }
    bcopy(undoRecAddr, &undoRec, sizeof(allocMcellUndoRecT));

    dmnP = ftx.dmnP;
    vd = VD_HTOP(undoRec.newMcellId.volume, dmnP);

    FTX_LOCKWRITE(&(vd->rbmt_mcell_lk), ftx);

    /*
     * Pin the mcell's page and free the mcell.
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&bmt,
                     vd->rbmtp,
                     undoRec.newMcellId.page,
                     BS_NIL,
                     ftx, 
                     MF_NO_VERIFY
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_rbmt_mcell_undo: rbf_pinpg failed, return code = %d", sts );
        return;
    }
    if (bmt->bmtPageId != FETCH_MC_PAGE(undoRec.newMcellId)) {
        domain_panic(dmnP, "alloc_rbmt_mcell_undo: got rbmt page %ld instead of %ld",
                    bmt->bmtPageId, undoRec.newMcellId.page );
        return;
    }

    mcell = &(bmt->bsMCA[undoRec.newMcellId.cell]);

    rbf_pin_record (
                    pgPin,
                    &mcell->mcNextMCId,
                    sizeof(bs_mcell_hdr_t)
                    );

    if (bmt->bmtFreeMcellCnt == 0) {
        MS_SMP_ASSERT(bmt->bmtNextFreeMcell == 0);
        mcell->mcNextMCId = bsNilMCId;
    } else {
       mcell->mcNextMCId.cell = bmt->bmtNextFreeMcell;
       mcell->mcNextMCId.page = bmt->bmtPageId;
       mcell->mcNextMCId.volume = bmt->bmtVdIndex;
    }
    mcell->mcLinkSegment = 0;
    mcell->mcNumRecords = 0;
    mcell->mcRsvd1 = 0;
    mcell->mcRsvd2 = 0;
    mcell->mcTag = NilBfTag;
    mcell->mcBfSetTag = NilBfTag;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_94);

    rbf_pin_record (
                    pgPin,
                    &(bmt->bmtTrackMcells),
                    sizeof (bs_bmt_track_mcells_t)
                    );

    bmt->bmtNextFreeMcell = FETCH_MC_CELL(undoRec.newMcellId);
    bmt->bmtFreeMcellCnt++;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_95);

    return;

}  /* end alloc_rbmt_mcell_undo */

/*
 * bmt_alloc_mcell
 *
 * This routine allocates an mcell from an mcell pool, either the general
 * pool or page 0 pool.
 *
 * This function starts/ends a new transaction.  If the transaction is a
 * sub-transaction (non-nil parentFtx handle), the caller must isolate the
 * new mcell from other threads until the mcell is full or the root transaction
 * completes.  This prevents another thread from assigning a record in the
 * mcell.  If the caller later fails the transaction, the undo routine unlinks
 * and deallocates the mcell.  The other thread's record evaporates along with
 * the mcell.
 *
 * FIX - One solution is to add a record count field to the mcell's header.
 * In the undo routine, if the record count is non-zero, don't unlink and
 * deallocate the mcell.  In conjunction, the record assign code would have
 * an undo routine that undoes a record assignment (set rec type to BSR_NIL).
 *
 * NOTE: If this routine allocates a mcell from the page 0 pool, the transaction
 * is not logged.  This is because updates to page 0 are specially handled.
 */

statusT
bmt_alloc_mcell (
                 bfAccessT *bfAccess,  /* in */
                 vdIndexT poolVdIndex,  /* in */
                 mcellPoolT poolType,  /* in */
                 bfTagT bfSetTag,  /* in */
                 bfTagT bfTag,  /* in */
                 unsigned int linkSeg,  /* in */
                 ftxHT parentFtx,  /* in */
                 bfMCIdT *retMcellId, /* out */
                 int force_alloc /* in - force an allocation */
                 )

{

    domainT *dmnP;
    ftxHT ftx;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;
    vdT *vd;
    allocMcellUndoRecT undoRec;

    dmnP = bfAccess->dmnP;

    sts = sc_valid_vd( dmnP,
                       bfAccess->reqServices,
                       poolVdIndex);
    if (sts != EOK) {
        if (sts != E_VOL_NOT_IN_SVC_CLASS) {
            return sts;
        }
        else {
            if (((bfAccess->xtnts).copyXtntMap != 0) ||
                (force_alloc == TRUE) ||
                (poolType==RBMT_MCELL) )
            {
                /*
                 * Special case - there is a copy in progress.
                 * We need this mcell so that we can migrate data off
                 * this volume onto another one. It's ok if this
                 * volume is no longer in the service class.
                 *
                 * Also, sometimes we need to force an allocation of an mcell
                 * if we have already allocated the file storage and just
                 * need an mcell to describe it.  An example would be during
                 * a splitting of an extent descriptor when a hole is filled in.
                 *
                 * Also, we need to force an allocation of an mcell if we are
                 * extending the bmt, even though that vd has been taken offline.
                 *
                 */
                 sts = EOK;
            }
            else {
                return ENO_MORE_BLKS;
            }
        }
    }

    vd = vd_htop_if_valid(poolVdIndex, dmnP, TRUE, FALSE);

    if (poolType == BMT_NORMAL_MCELL) {
        sts = FTX_START_META (FTA_BS_BMT_ALLOC_MCELL_V1, &ftx, parentFtx,
                              dmnP);
        if (sts != EOK) {
            vd_dec_refcnt(vd);
            return sts;
        }
        FTX_LOCKMUTEX(&(vd->mcell_lk), ftx);
        sts = alloc_mcell (
                           vd,
                           ftx,
                           &mcellId,
                           &pgPin,
                           &mcell,
                           BMT_NORMAL_MCELL
                           );
        if (sts != EOK) {
            vd_dec_refcnt(vd);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_330);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_331);
            return sts;
        }
        init_mcell (pgPin, mcell, bfSetTag, bfTag, linkSeg);
        MS_SMP_ASSERT( FETCH_MC_VOLUME(mcellId) == vd->vdIndex);
        undoRec.newMcellId = mcellId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_96);
        ftx_done_u (ftx, FTA_BS_BMT_ALLOC_MCELL_V1, sizeof (undoRec), &undoRec);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_97);
    } else {
        if (poolType == RBMT_MCELL) {
            sts = FTX_START_META (FTA_BS_RBMT_ALLOC_MCELL_V1, &ftx, parentFtx,
                                  dmnP);
            if (sts != EOK) {
                vd_dec_refcnt(vd);
                return sts;
            }

            /* We have a lock heirarchy condition here. When we pin a page
             * of the RBMT we will obtain the xtntmap lock for the RBMT that
             * is out of order with the rbmt_mcell_lk. We need to either
             * take the xtntmap lock for write here and test in getpage not
             * to take it or make a seperate instantiation of the RBMT 
             * xtntmap_lk like we do for the BMT and make it lower than
             * the rbmt_mcell_lk. For now these are both in the heirarchy
             * at the same level. 
             * NOTE obtaining the RBMT's extent map lock of write here will
             * violate the rule that no one can hold the xtntmap_lk for 
             * write and then block for memory or UFC pages. We can make
             * an exception for the RBMT since it can not consume all of 
             * the UFC and therefor stop VHAND from making progress.
             */



            FTX_LOCKWRITE(&(vd->rbmt_mcell_lk), ftx);

            sts = alloc_rbmt_mcell(
                                   vd,
                                   ftx,
                                   &mcellId,
                                   &pgPin,
                                   &mcell
                                   );
            if (sts != EOK) {
                vd_dec_refcnt(vd);
                ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_332);
                ftx_fail (ftx);
                ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_333);
                return sts;
            }
            init_mcell (pgPin, mcell, bfSetTag, bfTag, linkSeg);
            MS_SMP_ASSERT( FETCH_MC_VOLUME(mcellId) == vd->vdIndex);
            undoRec.newMcellId = mcellId;
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_98);
            ftx_done_u( ftx, FTA_BS_RBMT_ALLOC_MCELL_V1, sizeof(undoRec),
                        &undoRec);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_99);
        } else {
            vd_dec_refcnt(vd);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_334);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_335);
            return EBAD_PARAMS;
        }
    }
    vd_dec_refcnt(vd);

    *retMcellId = mcellId;

    return EOK;

}  /* end bmt_alloc_mcell */

statusT
deferred_free_mcell ( vdT *vd,
                      bfMCIdT mcellId,
                      ftxHT pftxH)
{
    statusT sts;
    ftxHT ftxH;
    allocMcellUndoRecT rdRec;
    MS_SMP_ASSERT(vd);
    MS_SMP_ASSERT(vd->vdIndex ==  FETCH_MC_VOLUME(mcellId));
    
    sts = FTX_START_META (FTA_BS_BMT_DEFERRED_MCELL_FREE, &ftxH, pftxH,
                          vd->dmnP);
    if (sts != EOK) {
        return sts;
    }
    rdRec.newMcellId = mcellId;

    /* this ftx has a root done routine: alloc_mcell_undo */
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_100);
    ftx_done_urd (ftxH, FTA_BS_BMT_DEFERRED_MCELL_FREE, 0, NULL,
                  sizeof(rdRec), &rdRec);
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_101);
    return EOK;
}


typedef struct allocLinkMcellUndoRec {
    bfMCIdT mcellId;
    bfMCIdT newMcellId;
} allocLinkMcellUndoRecT;

/*
 * alloc_link_mcell_undo
 *
 * This function "undoes" the actions performed by the bmt_alloc_link_mcell()
 * function.
 *
 * This function assumes that the transaction was a sub-transaction and that
 * the parent transaction's do or undo routine has taken the appropriate action to
 * to synchronize access to this mcell and the list that the mcell is on.  During
 * run-time, the parent transaction's do routine syncs access.  During recovery,
 * the parent transaction's undo routine syncs access.
 *
 * This function assumes that all of the mcell's records are inactive.
 */

void
alloc_link_mcell_undo (
                       ftxHT ftx,
                       int undoRecSize,
                       void *undoRecAddr
                       )
{

    bsMPgT *bmt;
    domainT *dmnP;
    bsMCT *mcell;
    bsMCT *newMcell;
    rbfPgRefHT newPgPin;
    rbfPgRefHT pgPin;
    statusT sts;
    allocLinkMcellUndoRecT undoRec;
    vdT *vd;

    if (undoRecSize != sizeof (allocLinkMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_link_mcell_undo: bad undo record size" );
    }
    bcopy(undoRecAddr, &undoRec, sizeof(allocLinkMcellUndoRecT));

    dmnP = ftx.dmnP;

    /*
     * Pin the old and new mcell's pages, unlink the two mcells and free
     * the new mcell.
     */

    vd = VD_HTOP(undoRec.mcellId.volume, dmnP);
    sts = rbf_pinpg (
                     &pgPin,
                     (void *)&bmt,
                     vd->bmtp,
                     undoRec.mcellId.page,
                     BS_NIL,
                     ftx,
                     MF_NO_VERIFY
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(1) failed, return code = %d", sts );
        return;
    }
    if (bmt->bmtPageId != FETCH_MC_PAGE(undoRec.mcellId) ) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(1) got bmt page %ld instead of %ld",
                    bmt->bmtPageId, undoRec.mcellId.page );
        return;
    }
    mcell = &(bmt->bsMCA[undoRec.mcellId.cell]);

    vd = VD_HTOP(undoRec.newMcellId.volume, dmnP);

    FTX_LOCKMUTEX(&(vd->mcell_lk), ftx);

    sts = rbf_pinpg (
                     &newPgPin,
                     (void*)&bmt,
                     vd->bmtp,
                     undoRec.newMcellId.page,
                     BS_NIL,
                     ftx,
                     MF_NO_VERIFY
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(2) failed, return code = %d", sts );
        return;
    }
    if (bmt->bmtPageId != FETCH_MC_PAGE(undoRec.newMcellId)) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(2) got bmt page %ld instead of %ld",
                    bmt->bmtPageId, undoRec.newMcellId.page );
        return;
    }
    newMcell = &(bmt->bsMCA[undoRec.newMcellId.cell]);

    rbf_pin_record (pgPin, &(mcell->mcNextMCId), sizeof (mcell->mcNextMCId));
    mcell->mcNextMCId = newMcell->mcNextMCId;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_102);

    bmt_free_mcell(
                   ftx,
                   vd,
                   newMcell,
                   undoRec.newMcellId,
                   bmt,
                   newPgPin
                   );

    return;

}  /* end alloc_link_mcell_undo */

/*
 * bmt_alloc_link_mcell
 *
 * This routine allocates an mcell from the general mcell pool and links the
 * mcell to another mcell.  To alloc a page 0 mcell, use bmt_alloc_mcell.
 *
 * The caller is responsible for protecting the list that the mcell is linked
 * onto.  If the new mcell is linked onto a bitfile's mcell chain, the caller
 * must hold the bitfile's mcellList_lk lock.
 *
 * See bmt_alloc_mcell() header comments.
 */

statusT
bmt_alloc_link_mcell (
                      domainT *dmnP,        /* in */
                      vdIndexT poolVdIndex,  /* in */
                      mcellPoolT poolType,  /* in */
                      bfMCIdT oldMcellId,  /* in */
                      ftxHT parentFtx,     /* in */
                      bfMCIdT *retMcellId, /* out */
		      uint32_t *alt_linkSegment /* in */
                      )

{

    bsMPgT *bmt;
    ftxHT ftx;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bsMCT *oldMcell;
    rbfPgRefHT oldPgPin;
    rbfPgRefHT pgPin;
    statusT sts;
    allocLinkMcellUndoRecT undoRec;
    vdT *oldVd, *poolVd;

    sts = FTX_START_META (FTA_BS_BMT_ALLOC_LINK_MCELL_V1, &ftx, parentFtx,
                          dmnP);
    if (sts != EOK) {
        return sts;
    }

    oldVd = VD_HTOP(oldMcellId.volume, dmnP);
    poolVd = VD_HTOP(poolVdIndex, dmnP);

    /*
     * Acquire the vd mcell list lock prior to pinning any pages to
     * avoid pinblock deadlock.
     */

    if (poolType == BMT_NORMAL_MCELL) {
        FTX_LOCKMUTEX(&(poolVd->mcell_lk), ftx);
    } else {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_336);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_337);
        return EBAD_PARAMS;
    }

    sts = alloc_mcell (
                       poolVd,
                       ftx,
                       &mcellId,
                       &pgPin,
                       &mcell,
                       poolType
                       );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_338);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_339);
        return sts;
    }

    MS_SMP_ASSERT(FETCH_MC_VOLUME(mcellId) == poolVdIndex);
    sts = rbf_pinpg (
                     &oldPgPin,
                     (void*)&bmt,
                     oldVd->bmtp,
                     oldMcellId.page,
                     BS_NIL,
                     ftx, 
                     MF_VERIFY_PAGE 
                     );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_340);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_341);
        return sts;
        }
    oldMcell = &(bmt->bsMCA[oldMcellId.cell]);

    if( alt_linkSegment != NULL ) {
	init_mcell (
		    pgPin,
		    mcell,
		    oldMcell->mcBfSetTag,
		    oldMcell->mcTag,
		    *alt_linkSegment
		    );
    } else {
	init_mcell (
		    pgPin,
		    mcell,
		    oldMcell->mcBfSetTag,
		    oldMcell->mcTag,
		    oldMcell->mcLinkSegment + 1
		    );
    }
    link_mcells (oldPgPin, oldMcell, pgPin, mcell, mcellId);
    undoRec.mcellId = oldMcellId;
    undoRec.newMcellId = mcellId;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_103);
    ftx_done_u (ftx, FTA_BS_BMT_ALLOC_LINK_MCELL_V1,
                sizeof (undoRec), &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_104);

    *retMcellId = mcellId;

    return EOK;

}  /* end bmt_alloc_link_mcell */

/*
 * bmt_extend
 *
 * Add pages to the Bitfile Metadata Table.
 *
 * Possible status returned from bs_pinpg, bs_unpinpg,
 * ENO_MORE_BLOCKS or EOK.
 */


static statusT
bmt_extend(
           struct vd* vdp,               /* in - ptr to vd */
           ftxHT parFtxH
           )
{
    struct bfAccess *bmtap;
    bf_fob_t fob_cnt;
    bf_fob_t first_new_fob, fob, num_new_fobs;
    struct bsMPg *bmtpgp;
    bfPageRefHT pgRef;
    ftxHT ftxH;
    statusT sts;                  /* status                                  */
    domainT* dmnP = vdp->dmnP;

    /*-----------------------------------------------------------------------*/

    bmtap = vdp->bmtp;
    first_new_fob = bmtap->bfaNextFob;

    num_new_fobs = vdp->bmtXtntPgs * bmtap->bfPageSz;
    if (!(BMT_EXT_REQUEST_OK(num_new_fobs, dmnP->ftxLogPgs, vdp->stgCluster))) /* log large enough? */
    {
    /* 
     * Here: the requested size of bmtXtntFobs is too large for the current 
     * log size and could potentially lead to a 'log half full' panic.      
     * Use an ext size that is less than 25% of  (log size SBM_FOBS_PER_PG(x)) 
     * and send a message to the console 
     */

        num_new_fobs = BMT_EXT_REQUEST_DEFAULT(dmnP->ftxLogPgs, vdp->stgCluster);
        printf("\nBMT ext size (%d) of %s in domain %s is too large\n",
                vdp->bmtXtntPgs, vdp->vdName, dmnP->domainName);
        printf("using new size: %d (25%% of (log size * %d))\n",
                num_new_fobs, SBM_FOBS_PER_PG(vdp->stgCluster));
    }


#ifdef ADVFS_RBMT_STRESS
    /* Just add a single page */
    num_new_fobs=bmtap->bfPageSz; 
#endif

    sts = FTX_START_META( FTA_BS_BMT_EXTEND_V1, &ftxH, parFtxH,
                          vdp->dmnP);
    if ( sts != EOK ) {
        ADVFS_SAD1( "bmt_extend: ftx start failed", sts );
    }

    do {

        /*
         * Add the desired amount of storage to the
         * bitfile metadata table.  If we fail because
         * of no more disk blocks, try allocating half
         * as much storage.
         */

        sts = stg_add_stg( ftxH, 
                           bmtap, 
                           first_new_fob, 
                           num_new_fobs, 
                           bmtap->bfPageSz,
                           FALSE, 
                           &fob_cnt);

        if (sts == ENO_MORE_BLKS) {
            /*
             * Make sure our fobs align on bmt page boundaries.
             */
            num_new_fobs = rounddown(num_new_fobs/2, bmtap->bfPageSz);
        }

        /*
         * If we fail because we are running out of page 0 mcells
         * while trying to add multiple pages to the BMT, just try
         * to add one page for now.
         */

        if ((sts == ENO_MORE_MCELLS) && 
            (num_new_fobs > bmtap->bfPageSz)) {
            sts = ENO_MORE_BLKS;
            num_new_fobs = bmtap->bfPageSz;
        }
    }
    while ((sts == ENO_MORE_BLKS) && (num_new_fobs> 0));

    /*
     * If we came out of the loop because "num_new_fobs " is less than or
     * equal to 0, "sts" will not be equal to EOK.
     */

    if (sts != EOK) {
        if(sts == ENO_MORE_MCELLS) {
            vdp->nextMcellPg = NO_MORE_MCELLS;
        }
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_342);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_343);
        return sts;
    }

    /* Optimize performance by telling pinpg/getpage to skip IO to prime
     * new cache pages from disk since this routine is initializing the
     * whole metadata page.
     */
    for (fob = first_new_fob; 
         fob < (first_new_fob + num_new_fobs); 
         fob += bmtap->bfPageSz) {

        sts = bs_pinpg(&pgRef,
                       (void *) &bmtpgp,
                       vdp->bmtp,
                       fob / bmtap->bfPageSz,
                       FtxNilFtxH,
                       MF_OVERWRITE | MF_NO_VERIFY);
        if (sts != EOK) {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_344);
            ftx_fail(ftxH);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_345);
            return sts;
        }

        bmt_init_page( bmtpgp, fob / bmtap->bfPageSz, vdp->vdIndex,  
                       BFM_BMT, BFD_ODS_NULL_VERSION, dmnP->dmnCookie,
                       vdp->vdCookie );

        if (fob < (first_new_fob + num_new_fobs - bmtap->bfPageSz)){
            /*
             * Continue free Mcell list on to next page (if it's not the
             * last page we're adding to the BMT).
             */
            bmtpgp->bmtNextFreePg = (fob / bmtap->bfPageSz) + 1;
        } else  {
            bmtpgp->bmtNextFreePg = BMT_PG_TERM; 
        }

        sts = bs_unpinpg( pgRef, logNilRecord, BS_DIRTY);
        if (sts != EOK) {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_346);
            ftx_fail(ftxH);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_347);
            return sts;
        }
    }

    /* Sync the initialized BMT pages to disk. */
    fcache_vn_flush(&vdp->bmtp->bfVnode,
                    ADVFS_FOB_TO_OFFSET(first_new_fob),
                    num_new_fobs * ADVFS_FOB_SZ,
                    NULL,
                    FVF_WRITE | FVF_SYNC);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_360);

    (void)bmt_upd_mcell_free_list( ftxH,
                                   vdp,
                                   first_new_fob / bmtap->bfPageSz);

    /*
     * Set done mode to skip undo for sub ftxs, in this case,
     * the rbf_add_stg, so it will not be undone after the
     * current ftx is done.  This is because the new page will
     * be initialized and the global free list updated, so the
     * new page of tags will become visible after either this
     * parent ftx completes, or the system restarts.
     */

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_14);

    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_105);

    ftx_done_urdr( ftxH, FTA_BS_BMT_EXTEND_V1, 0,0,0,0,
                  sizeof(vdIndexT), &vdp->vdIndex );

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_15);

    return EOK;
}

/*
 * extend_bmt_redo_opx
 *
 * This redo opx forces a remap of the bmt during recovery after it
 * has been extended.  This keeps the cached in-memory extent map in
 * sync with the updated on-disk extent structures.
 */

void
extend_bmt_redo_opx(
                    ftxHT ftxH,         /* in - ftx */
                    int opRecSz,        /* in - redo rec size */
                    void *opRec         /* in - ptr to redo rec */
                    )
{
    vdIndexT vdIndex;
    domainT *dmnP = ftxH.dmnP;
    vdT* vdp;
    statusT sts;
    bfTagFlagsT tagFlags = BFD_METADATA;
    bfAccessT  *bfap;
    struct bsMPg* bmtp_page;
    struct bsMC* mcp;
    bfPageRefHT pgref;


    


    bcopy(opRec, &vdIndex, sizeof(vdIndexT));

    vdp = VD_HTOP(vdIndex, dmnP);

    bfap = vdp->bmtp;

    /*
     * Make sure our extents are invalid before we reload them.
     */
    x_dealloc_extent_maps( &bfap->xtnts );

    sts = bs_refpg(&pgref,
                      (void*)&bmtp_page,
                      vdp->rbmtp,
                      bfap->primMCId.page,
                      FtxNilFtxH,
                      MF_VERIFY_PAGE);
    
    if (sts != EOK) {
        domain_panic( bfap->dmnP, "extend_bmt_redo_opx: failed to ref BMT page", sts );
        return;
    }


    mcp = &bmtp_page->bsMCA[bfap->primMCId.cell];


    sts = x_create_inmem_xtnt_map( bfap, mcp );
    if (sts != EOK) {
        domain_panic( bfap->dmnP, "extend_bmt_redo_opx: x_create_inmem_xtnt_map failed", sts );
    }

    (void) bs_derefpg(pgref, BS_CACHE_IT);

}

/*
 * alloc_mcell
 *
 * This routine gets a free mcell from the normal mcell pool.  It
 * will update the global free mcell free list head (in the vd and
 * on-disk) if necessary.  It will also extend the bmt if necessary.
 *
 * NOTE: It is assumed that the caller of this routine holds the
 * mcell_lk, which serializes access to the per page and global
 * free mcell page links.
 */

static statusT
alloc_mcell(
            vdT* vd,              /* in */
            ftxHT parentFtx,      /* in */
            bfMCIdT *retMcellId,  /* out */
            rbfPgRefHT *retPgPin, /* out */
            bsMCT **retMcell,     /* out */
            mcellPoolT poolType   /* in */
            )
{
    statusT sts;
    bfMCIdT mcid;
    struct bsMPg* bmtpgp;
    struct bsMC* mcp;
    rbfPgRefHT pgref;
    bs_meta_page_t page;
    domainT* dmnP = vd->dmnP;
    uint64_t bad_pages_found = 0;

    MS_SMP_ASSERT( ADVFS_MUTEX_OWNED(&vd->mcell_lk.lock.mtx) );

start:

    /* extend BMT if there are no more free mcells */

    page = vd->nextMcellPg;

    if(page == NO_MORE_MCELLS) {

        return ENO_MORE_MCELLS;

    }

    if ( page == EXTEND_BMT ) {
        if ((sts = bmt_extend( vd, parentFtx )) != EOK) {
            /*
             * Cannot extend the bmt, hence cannot allocate a free
             * mcell.  Return a null pointer.
             */
            return sts;
        }

        page = vd->nextMcellPg;

        if ( page == 0 ) {
            domain_panic(dmnP, "alloc_mcell: no free mcells after bmt_extend" );
            return (E_DOMAIN_PANIC);
        }
    }

    /* access the page that contains free mcells */

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vd->bmtp, page, BS_NIL, parentFtx,
                     MF_VERIFY_PAGE);
    if ( sts != EOK ) {
        return sts;
    }

    if ( bmtpgp->bmtPageId != page ) {
        domain_panic(dmnP, "alloc_mcell: read page %d instead of %d, ",
                    bmtpgp->bmtPageId, page );
        return(E_DOMAIN_PANIC);
    }

    rbf_pin_record(pgref,
                   &(bmtpgp->bmtTrackMcells),
                   sizeof(bs_bmt_track_mcells_t));

    /* check to make sure there is a free mcell on this page */
    if ( bmtpgp->bmtFreeMcellCnt == 0) {
#ifdef ADVFS_SMP_ASSERT
        printf("ADVFS: alloc_mcell: next free page has no free mcells\n");
        printf("ADVFS: alloc_mcell: domain = %s, vol = %s, page = %d\n",
                dmnP->domainName, vd->vdName, page);
        ADVFS_SAD0( "alloc_mcell: bad mcell free list");
#endif
        goto cleanup_corruption;
    }

    mcid.volume = bmtpgp->bmtVdIndex;
    mcid.page   = bmtpgp->bmtPageId;
    mcid.cell   = bmtpgp->bmtNextFreeMcell;
    
    mcp = &bmtpgp->bsMCA[mcid.cell];
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_106);

    if (!BS_BFTAG_EQL( mcp->mcTag, NilBfTag ) && 
        !BS_BFTAG_EQL( mcp->mcBfSetTag, NilBfTag )) { 
#ifdef ADVFS_DEBUG
        ADVFS_SAD2( "alloc_mcell: mcell not really free",
                mcid.volume, mcid.page, mcid.cell);
#endif
        printf("ADVFS error: alloc_mcell: mcell (%d.%ld.%d) not really free\n", 
                mcid.volume,
                mcid.page,
                mcid.cell );
        printf( "ADVFS cont : alloc_mcell: domain = %s, vol = %s, page = %d\n",
                dmnP->domainName,
                vd->vdName,
                page );
        printf( "ADVFS cont : alloc_mcell: tag = 0x%016lx.%04x, setTag = 0x%016lx.%04x\n",
                mcp->mcTag.tag_num, mcp->mcTag.tag_seq, mcp->mcBfSetTag.tag_num, mcp->mcBfSetTag.tag_seq );
        goto cleanup_corruption;
    }

    /*
     * mark cell as in use
     */
    rbf_pin_record(pgref, &mcp->mcTag, sizeof(bfTagT));
    mcp->mcTag.tag_num = -1;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_107);

    bmtpgp->bmtNextFreeMcell = mcp->mcNextMCId.cell;
    bmtpgp->bmtFreeMcellCnt--;

    if ( bmtpgp->bmtFreeMcellCnt == 0 ) {

        /*
         * next free mcell is on another page.  note no more here.
         * update the global next free mcell list head.
         */

        if ( bmtpgp->bmtNextFreeMcell != 0) {
#ifdef ADVFS_DEBUG
            ADVFS_SAD2( "alloc_mcell: freeMCellCnt == 0, nextFreeMcell == (N1)",
                        bmtpgp->bmtNextFreeMcell );
#endif
            printf( "ADVFS error: alloc_mcell: invalid free list\n" );
            printf( "ADVFS cont : alloc_mcell: domain = %s, vol = %s, page = %d\n",
                    dmnP->domainName,
                    vd->vdName,
                    page );
            printf( "ADVFS cont : alloc_mcell: freeMCellCnt = %d, nextFreeMcell == %d",
                    bmtpgp->bmtFreeMcellCnt,
                    bmtpgp->bmtNextFreeMcell );
            goto cleanup_corruption;
        }

        (void)bmt_upd_mcell_free_list( parentFtx, vd, bmtpgp->bmtNextFreePg );

        rbf_pin_record( pgref,
                        &(bmtpgp->bmtNextFreePg),
                        (int32_t)sizeof(bmtpgp->bmtNextFreePg));

        bmtpgp->bmtNextFreePg = BMT_PG_TERM;

    }

    *retMcellId = mcid;
    *retPgPin = pgref;
    *retMcell = mcp;

    /* extend BMT when disk < 3.125 % free space and no more free mcells */
    if (vd->nextMcellPg == EXTEND_BMT && vd->freeClust < vd->vdClusters/32) {
	(void)bmt_extend(vd, parentFtx);
    }

    return EOK;

cleanup_corruption:
    {
    advfs_ev advfs_event;
    bs_meta_page_t new_head_of_free_list;

        if (!AdvfsFixUpBMT) {
            ADVFS_SAD0( "alloc_mcell: bad mcell free list");
        }
    
        /* 
         * If this is the first bad page we've found in this call and 
         * it points to a page other than itself, we'll try to patch
         * the mcell free list to start with that other page.  If this
         * is the first bad page we've found in this call and it points
         * to itself as the next page with free mcells OR if this is
         * the second bad page that we've found in this call, cauterize
         * the list.  In any case, if we're here it's because some kind
         * of corruption has occurred.  We try to work around it since
         * it's just a free list but advise the admin to run fixfdmn,
         * which can rebuild the mcell free list.  If we find two bad
         * pages with no free mcells, only print out a warning and post
         * an event for the first one.
         */
        if (++bad_pages_found == 1) {
            ms_uaprintf("Warning: AdvFS has detected an inconsistency in a BMT metadata file's free list\nof mcells:\n");
            ms_uaprintf("Domain = '%s', volume = '%s', BMT page = %d\n",
                    dmnP->domainName,
                    vd->vdName,
                    page );
            ms_uaprintf("AdvFS is attempting to work around this problem but this message may indicate\n");
            ms_uaprintf("metadata inconsistency in the '%s' domain.  Unmount all filesets in the\n", dmnP->domainName);
            ms_uaprintf("'%s' domain at the earliest convenient time and run the\n", dmnP->domainName);
            ms_uaprintf("/sbin/advfs/fixfdmn command on the domain to correct the inconsistency.\n");
            ms_uaprintf("See the fixfdmn(8) man page for details.  If you do not run fixfdmn,\n");
            ms_uaprintf("AdvFS may use more space than is necessary on the %s volume\n", vd->vdName);
            ms_uaprintf("in the %s domain for its BMT metadata.\n", dmnP->domainName);
    
            bzero(&advfs_event, sizeof(advfs_ev));
            advfs_event.special = vd->vdName;
            advfs_event.domain = dmnP->domainName;
            advfs_post_kernel_event(EVENT_FDMN_BAD_MCELL_LIST,&advfs_event);
        }
    
        new_head_of_free_list = ((bad_pages_found == 1) && 
                                 (bmtpgp->bmtNextFreePg != page)) ? 
                                     bmtpgp->bmtNextFreePg : 
                                     EXTEND_BMT;
    
        (void)bmt_upd_mcell_free_list( parentFtx, vd, new_head_of_free_list );
    
        bmtpgp->bmtFreeMcellCnt = 0;
        bmtpgp->bmtNextFreeMcell = 0;
        bmtpgp->bmtNextFreePg = BMT_PG_TERM;
        goto start;
    }

    /* NOT REACHED */

}  /* end alloc_mcell */

/*
 * rbmt_extend
 *
 * Add one page to the Reserved Metadata Table. This must be called in
 * such a manner as to allow a root transaction to be started.  Currently,
 * this is called from bs_extend_rbmt_thread() only.
 *
 * This function waits for message sender transaction to finish, as
 * well as all other transactions, then it gets a slot because of the
 * exclusive transaction request made.  The exclusive transaction
 * cannot start until the sending transaction finishes.  The exclusive
 * transaction also avoids potential deadlocks caused by timing/load
 * variations.  This approach also avoids the problem of more than
 * FTX_MX_NEST levels in a  transaction.  It is possible to add two levels to
 * the transaction table structures and then to code this as a sub transaction
 * of of the message sending function. (It  appears that the exclusive nature
 * was a precaution not a necessity.
 */

static statusT
rbmt_extend (
             domainT *dmnP,
             vdIndexT vdIndex
            )
{
    statusT sts;
    bsMPgT *curRbmtPg;
    bsMPgT *newRbmtPg;
    struct bsMR *rp;
    ftxHT ftxH;
    bf_fob_t fob_cnt, new_fob;
    bfPageRefHT newPgPin;
    rbfPgRefHT  curPgPin;
    bfMCIdT mcid;
    bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };
    struct vd* vdp = VD_HTOP(vdIndex, dmnP);
    bfAccessT *rbmtap = vdp->rbmtp;

    /* This starts an exclusive, root transaction.  The transaction
     * sending the message to the bs_extend_rbmt_thread must complete
     * before this transaction will start.
     */
    sts = FTX_START_RSVD_META( FTA_BS_RBMT_EXTEND_V1,     /* agent */
                               &ftxH,                     /* new handle */
                               FtxNilFtxH,                /* root tx */
                               vdp->dmnP                 /* domain pointer */
                               );       
    if ( sts != EOK ) {
        ADVFS_SAD1( "rbmt_extend: ftx start failed", sts );
    }

    /*
     * Seize the mcellList_lk and the rbmt_mcell_lk to synchronize
     * access to rbmt mcell pool. Do them here in the right order to
     * keep lock hierarchy happy.
     *
     * If we wind up taking the error case, we will hit a lock hierarchy
     * violation in link_unlink_mcells_undo() when we try to take out
     * the xtntMap_lk.  To avoid this we take out the lock now.  We will
     * release the lock before we exit from the normal case.
     */

    /* We have a lock heirarchy condition here from the above comment
     * and when we pin a page of the RBMT we will obtain the xtntmap
     * lock for the RBMT that is out of order with the
     * rbmt_mcell_lk. We need to either take the xtntmap lock for
     * write here and test in getpage not to take it or make a
     * seperate instantiation of the RBMT xtntmap_lk like we do for
     * the BMT and make it lower than the rbmt_mcell_lk. For now these
     * are both in the hierarchy at the same level.
     *
     * NOTE obtaining the RBMT's extent map lock of write here will
     * violate the rule that no one can hold the xtntmap_lk for write
     * and then block for memory or UFC pages. We can make an
     * exception for the RBMT since it can not consume all of the UFC
     * and therefor stop VHAND from making progress.
     */
    

    FTX_LOCKWRITE(&(rbmtap->mcellList_lk), ftxH);
    FTX_LOCKWRITE(&(vdp->rbmt_mcell_lk), ftxH);

    /*
     * Pin current RBMT page
     */
    sts = rbf_pinpg (&curPgPin,
                     (void*)&curRbmtPg,
                     rbmtap,
                     vdp->lastRbmtPg,
                     BS_NIL,
                     ftxH,
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    new_fob = rbmtap->bfaNextFob;
    mcid.volume = curRbmtPg->bmtVdIndex;
    mcid.page = vdp->lastRbmtPg;
    mcid.cell = RBMT_RSVD_CELL;

    sts = stg_add_rbmt_stg( ftxH,
                            rbmtap,
                            new_fob,
                            rbmtap->bfPageSz,
                            mcid,
                            &fob_cnt);

    if ((sts == EOK) && (fob_cnt!= rbmtap->bfPageSz)) {
        domain_panic(vdp->dmnP,
                     "rbmt_extend: invalid fob_cnt from stg_add_rbmt_stg" );
        RAISE_EXCEPTION( sts );
    }
    else if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* pin the new page and initialize it;  not using rbf_pinpg and the
     * overhead of pinning the records since no other threads can see this
     * page, and we won't undo this ftx anyway.  If we crash before this
     * gets written to disk, this work will be redone on recovery.
     * Optimize performance by telling pinpg/getpage to skip IO to prime
     * new cache pages from disk since this routine is initializing the
     * whole metadata page.
     */
    sts = bs_pinpg(&newPgPin,
                   (void *)&newRbmtPg,
                   rbmtap,
                   new_fob / rbmtap->bfPageSz,
                   FtxNilFtxH,
                   MF_OVERWRITE | MF_NO_VERIFY);

    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    bmt_init_page( newRbmtPg,
                   new_fob / rbmtap->bfPageSz, vdp->vdIndex,
                   BFM_RBMT, dmnP->dmnVersion,
                   dmnP->dmnCookie, vdp->vdCookie );

    newRbmtPg->bsMCA[RBMT_RSVD_CELL].mcLinkSegment =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcLinkSegment + 1;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].mcTag =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcTag;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].mcBfSetTag =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcBfSetTag;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].mcNextMCId = bsNilMCId;

    /* We need to initialize the records too. */

    rp = (struct bsMR *) newRbmtPg->bsMCA[RBMT_RSVD_CELL].bsMR0;
    rp->bCnt = sizeof(struct bsMR);
    rp->type = BSR_NIL;
    rp->padding = 0;

    /* This page gets flushed to disk at this point */
    sts = bs_unpinpg( newPgPin, logNilRecord, writethru );
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_361);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* Pin the records in the rbmt reserved cell for chaining to the
     * next logical page.  These will get recovered if we crash.
     */
    rbf_pin_record( curPgPin,
                    &curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcNextMCId,
                    sizeof(bfMCIdT));

    curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcNextMCId.page = 
        new_fob / rbmtap->bfPageSz;
    curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcNextMCId.cell = RBMT_RSVD_CELL;
    curRbmtPg->bsMCA[RBMT_RSVD_CELL].mcNextMCId.volume = vdp->vdIndex;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_108);

    if (vdp->nextMcellPg == NO_MORE_MCELLS)
        vdp->nextMcellPg = EXTEND_BMT;

    /* Set new page and clear flag; these flds protected by rbmt_mcell_lk */
    vdp->lastRbmtPg = new_fob / rbmtap->bfPageSz;
    vdp->rbmtFlags &= ~RBMT_EXTENSION_IN_PROGRESS;

    /* This tx has only a redo operation; force log record to disk when
     * we commit the transaction.
     */

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_16);

    ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );
    
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_109);
    
    ftx_done_urdr( ftxH, FTA_BS_RBMT_EXTEND_V1, 0, 0, 0, 0, sizeof(vdIndexT),
                   &vdp->vdIndex );

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_17);

    return EOK;

HANDLE_EXCEPTION:

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_348);
    ftx_fail( ftxH );
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_349);
    vdp->rbmtFlags &= ~RBMT_EXTENSION_IN_PROGRESS;
    return sts;
}


/*
 * extend_rbmt_redo_opx
 *
 * This redo opx forces a remap of the rbmt during recovery after it
 * has been extended.  This keeps the cached in-memory extent map in
 * sync with the updated on-disk extent structures.
 */

void
extend_rbmt_redo_opx(
                    ftxHT ftxH,         /* in - ftx */
                    int opRecSz,        /* in - redo rec size */
                    void *opRec         /* in - ptr to redo rec */
                    )
{
    vdIndexT vdIndex;
    domainT *dmnP = ftxH.dmnP;
    vdT* vdp;
    statusT sts;
    bfAccessT *bfap;
    struct bsMPg* rbmtp_page;
    struct bsMC* mcp;
    bfPageRefHT pgref;


    bcopy(opRec, &vdIndex, sizeof(vdIndexT));
    
    vdp = VD_HTOP(vdIndex, dmnP);


    bfap = vdp->rbmtp;

    /*
     * Make sure our extents are invalid before we reload them.
     */
    x_dealloc_extent_maps( &bfap->xtnts );

    sts = bs_refpg(&pgref,
                      (void*)&rbmtp_page,
                      vdp->rbmtp,
                      bfap->primMCId.page,
                      FtxNilFtxH,
                      MF_VERIFY_PAGE);
    
    if (sts != EOK) {
        domain_panic( bfap->dmnP, "extend_rbmt_redo_opx: failed to ref BMT page", sts );
        return;
    }


    mcp = &rbmtp_page->bsMCA[bfap->primMCId.cell];


    /*
     * re-setup extent map.
     */
    sts = x_create_inmem_xtnt_map( bfap, mcp );
    if (sts != EOK) {
        domain_panic( bfap->dmnP, "extend_rbmt_redo_opx: x_create_inmem_xtnt_map failed", sts );
    }

    (void) bs_derefpg(pgref, BS_CACHE_IT);


}

/*
 * alloc_rbmt_mcell
 *
 * This routine allocates an mcell from the reserved mcell pool.
 * The mcells of all the reserved files (bmt, log, sbm), free
 * and busy, are located in this pool.
 *
 * NOTE: It is assumed that the caller of this routine holds the
 * rbmt_mcell_lk, which serializes access to the rbmt mcell pool.
 */
static
statusT
alloc_rbmt_mcell (
                  vdT* vd,  /* in */
                  ftxHT parentFtx,  /* in */
                  bfMCIdT *retMcellId,  /* out */
                  rbfPgRefHT *retPgPin,  /* out */
                  bsMCT **retMcell  /* out */
                 )
{
    bsMPgT *rbmt;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;

    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&vd->rbmt_mcell_lk.lock.rw,
		   RWL_UNLOCKED));

    /*
     * Pin current RBMT page
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&rbmt,
                     vd->rbmtp,
                     vd->lastRbmtPg,
                     BS_NIL,
                     parentFtx,
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        return sts;
    }

    /*
     * Check the # mcells left on this page;  if running low, send a
     * message to the extend_rbmt_thread to add a new page.  The first
     * leg of this check is the 'normal' code path.
     */
    if ( (rbmt->bmtNextFreeMcell == (RBMT_RSVD_CELL - 1)) &&
         !(vd->rbmtFlags & RBMT_EXTENSION_IN_PROGRESS) ) {
        rbmtExtMsgT *msg;
        /*
         * 1. Send a message to background thread.
         * 2. Set a flag indicating that we are extending
         *    so no other thread also notices and sends same msg.
         */
        msg = (rbmtExtMsgT *)msgq_alloc_msg( RbmtExtQH );
        if (msg) {
            msg->msgType  = EXTEND_RBMT;
            msg->domainId = vd->rbmtp->dmnP->domainId;
            msg->vdIndex  = vd->vdIndex;
            msgq_send_msg( RbmtExtQH, msg );

            /* Setting this flags is protected by the rbmt_mcell_lk */
            vd->rbmtFlags |= RBMT_EXTENSION_IN_PROGRESS;
        }
    } else if ( rbmt->bmtFreeMcellCnt == 0 ) {
        MS_SMP_ASSERT( rbmt->bmtNextFreeMcell == 0 );
        /* No room in this page, come back later; in case we crashed
         * before the rbmt got extended, and now we get to this point
         * after reboot, restart the rbmt extension.  Either way, we
         * must return ENO_MORE_MCELLS.  This will not be returned
         * after the rbmt extension transaction completes.  Note that
         * on any other page, cell 0 may be a valid free cell.
         */
        if ( !(vd->rbmtFlags & RBMT_EXTENSION_IN_PROGRESS) ) {
            rbmtExtMsgT *msg;

            msg = (rbmtExtMsgT *)msgq_alloc_msg( RbmtExtQH );
            if (msg) {
                msg->msgType  = EXTEND_RBMT;
                msg->domainId = vd->rbmtp->dmnP->domainId;
                msg->vdIndex  = vd->vdIndex;
                msgq_send_msg( RbmtExtQH, msg );

                /* Setting this flags is protected by the rbmt_mcell_lk */
                vd->rbmtFlags |= RBMT_EXTENSION_IN_PROGRESS;
            }
        }
        return ENO_MORE_MCELLS;
    }

    mcellId.volume = rbmt->bmtVdIndex;
    mcellId.page   = rbmt->bmtPageId;
    mcellId.cell   = rbmt->bmtNextFreeMcell;
    mcell = &(rbmt->bsMCA[mcellId.cell]);
    rbf_pin_record (
                    pgPin,
                    &(rbmt->bmtTrackMcells),
                    sizeof (bs_bmt_track_mcells_t)
                    );

    rbmt->bmtNextFreeMcell = mcell->mcNextMCId.cell;
    rbmt->bmtFreeMcellCnt--;
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_110);

    *retMcellId    = mcellId;
    *retPgPin      = pgPin;
    *retMcell      = mcell;

    return EOK;

} /* alloc_rbmt_mcell */

/*
 * init_mcell
 *
 * This function initializes an mcell header and initializes the
 * record area so it is empty.
 *
 * This function assumes that it is called after an mcell is allocated.  This
 * function must have an undo routine if the caller initializes a pre-existing
 * mcell.  The undo routine must restore the mcell's previous values.
 *
 * This function is re-doable because of the pin_record() and is un-doable
 * because of the above assumption.
 */

static
void
init_mcell (
            rbfPgRefHT pgPin,  /* in */
            bsMCT *mcell,  /* in/initialized */
            bfTagT bfSetTag,  /* in */
            bfTagT bfTag,  /* in */
            unsigned int linkSeg  /* in */
            )

{

    bsMRT *rec;

    rbf_pin_record (
                    pgPin,
                    &(mcell->bs_mcell_hdr),
                    sizeof(bs_mcell_hdr_t) + sizeof (bsMRT)
                    );
    mcell->mcNextMCId    = bsNilMCId;
    mcell->mcLinkSegment = linkSeg;
    mcell->mcNumRecords  = 0;
    mcell->mcRsvd1       = 0;
    mcell->mcRsvd2       = 0;
    mcell->mcTag         = bfTag;
    mcell->mcBfSetTag    = bfSetTag;
    ADVFS_CRASH_RECOVERY_TEST(pgPin.dmnP, AdvfsCrash_112);

    rec = (bsMRT *)&(mcell->bsMR0[0]);
    rec->bCnt = sizeof (bsMRT);
    rec->type = BSR_NIL;
    rec->padding = 0;

    return;

}  /* end init_mcell */

/*
 * link_mcells
 *
 * This function links two mcells together.
 */

static
void
link_mcells (
             rbfPgRefHT oldPgPin,  /* in */
             bsMCT *oldMcell,  /* in */
             rbfPgRefHT newPgPin,  /* in */
             bsMCT *newMcell,  /* in */
             bfMCIdT newMcellId  /* in */
            )

{

    rbf_pin_record (
                    newPgPin,
                    &(newMcell->mcNextMCId),
                    sizeof (newMcell->mcNextMCId)
                    );
    newMcell->mcNextMCId = oldMcell->mcNextMCId;
    ADVFS_CRASH_RECOVERY_TEST(newPgPin.dmnP, AdvfsCrash_113);

    rbf_pin_record (
                    oldPgPin,
                    &(oldMcell->mcNextMCId),
                    sizeof (oldMcell->mcNextMCId)
                    );
    oldMcell->mcNextMCId = newMcellId;
    ADVFS_CRASH_RECOVERY_TEST(oldPgPin.dmnP, AdvfsCrash_114);

    return;

}  /* end link_mcells */

/*
 * bmt_upd_mcell_free_list
 *
 * update page that starts list of free mcells
 *
 * NOTE: This routine directly uses the ftx of the caller instead of
 * starting a subtransaction.  It modifies a single page.  Therefore
 * no undo is logged, and if the ftx is later aborted, the next free
 * mcell link will *not* be undone.
 *
 * The subtransaction that this routine is performed under cannot fail since
 * this will pin records.
 */

static bs_meta_page_t 
bmt_upd_mcell_free_list(
    ftxHT ftxH,         /* in - ftx handle */
    vdT* vdp,           /* in - pointer to current disk */
    bs_meta_page_t freeMcellPage   /* in - free mcell page */
    )
{
    struct bsMPg* bmtpgp;
    rbfPgRefHT pgRef;
    bsMcellFreeListT *mcellFreeListp;
    bsMCT* mcp;
    statusT sts;
    bs_meta_page_t oldHeadPg;
    bs_meta_page_t freeListPg;
    domainT *dmnP = vdp->dmnP;

    /*
     * The first page containing free mcells is pointed to from the
     * free mcell pointer in bmt page 1.  This is to avoid increasing
     * the frequency of updates to page 0 (which contains the extent
     * maps for the reserved bitfiles), thereby minimizing the risk of
     * corruption of page 0.
     * For AdvFS domain versions 4 onwards, the free list is maintained
     * in BMT page 0 mcell 0.
     */

    freeListPg = 0;

    /* update the free page pointer on bmt page 1/0 */

    sts = rbf_pinpg( &pgRef, (void*)&bmtpgp, vdp->bmtp, freeListPg,
                     BS_NIL, ftxH, MF_VERIFY_PAGE );
    if ( sts != EOK ) {
        domain_panic( dmnP, "bmt_upd_mcell_free_list: rbf_pinpg failed, return code = %d", sts );
        return(E_DOMAIN_PANIC);
    }

    if ( bmtpgp->bmtPageId != freeListPg ) {
        domain_panic(dmnP,  "bmt_upd_mcell_free_list: got page %d instead of free list head (page %d)",
                    bmtpgp->bmtPageId, freeListPg );
        return(E_DOMAIN_PANIC);
    }

    mcp = &bmtpgp->bsMCA[0];

    mcellFreeListp = (bsMcellFreeListT*) bmtr_find( mcp, BSR_MCELL_FREE_LIST,
                                                                vdp->dmnP);
    if (mcellFreeListp == NULL) {
        domain_panic(dmnP,  "bmt_upd_mcell_free_list: cannot find free list record" );
        return(E_DOMAIN_PANIC);
    }

    oldHeadPg = mcellFreeListp->headPg;

    rbf_pin_record( pgRef, mcellFreeListp, sizeof( bsMcellFreeListT ) );
    mcellFreeListp->headPg = freeMcellPage;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_115);
    vdp->nextMcellPg = freeMcellPage;

    return oldHeadPg;
}

/*
 * bmt_set_mcell_free_list
 *
 * This routine initializes the in-memory mcell free list head using
 * the on-disk list head obtained from:
 * For AdvFS on-disk version < 4, page 1, cell 0 of the disk's BMT.
 * For AdvFS on-disk version == 4, page 0, cell 0 of the disk's BMT.
 */

statusT
bmt_set_mcell_free_list(
    vdT* vdp            /* in - pointer to current disk */
    )
{
    domainT* dmnP = vdp->dmnP;
    struct bsMPg* bmtpgp;
    bs_meta_page_t freeListPg;
    bfPageRefHT  pgRef;
    bsMcellFreeListT *mcellFreeListp;
    bsMCT* mcp;
    statusT sts;

    freeListPg = 0;

    if (vdp->bmtp->bfaNextFob <= freeListPg) {
        /*
         * Since the free list head is maintained on page 1 of the BMT
         * we must assume that the list is empty if page 1 doesn't exist.
         */
        vdp->nextMcellPg = 0;

    } else {

        /*
         * Get the mcell free list head from page 1, cell 0 of the BMT.
         */

        sts = bs_refpg(&pgRef,
                       (void*)&bmtpgp,
                       vdp->bmtp,
                       freeListPg,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if ( sts != EOK ) {
            domain_panic(vdp->dmnP, 
                       "bmt_set_mcell_free_list: refpg failure.");
            return E_DOMAIN_PANIC;
        }

        if ( bmtpgp->bmtPageId != freeListPg ) {
             (void) bs_derefpg( pgRef, BS_CACHE_IT );
             domain_panic(vdp->dmnP, 
                         "bmt_set_mcell_free_list: Got the wrong BMT page.");
             return E_DOMAIN_PANIC;
        }

        mcp = &bmtpgp->bsMCA[0];

        mcellFreeListp = bmtr_find( mcp, BSR_MCELL_FREE_LIST, dmnP);
        if (mcellFreeListp == NULL) {
            (void) bs_derefpg( pgRef, BS_CACHE_IT );
            return E_INVALID_MSFS_STRUCT;
        }

        vdp->nextMcellPg = mcellFreeListp->headPg;

        sts = bs_derefpg( pgRef, BS_CACHE_IT );

        if ( sts != EOK ) {
            return sts;
        }

    }
    return EOK;
}

typedef struct linkUnlinkMcellsUndoRec
{
    bfTagT bfTag;
    bfMCIdT prevMCId;
    bfMCIdT prevNextMCId;
    bfMCIdT lastMCId;
    bfMCIdT lastNextMCId;
} linkUnlinkMcellsUndoRecT;

typedef struct insertRemoveXtntChainUndoRec
{
    bfMCIdT primMCId;
    bfMCIdT primChainMCId;
    bfMCIdT lastMCId;
    bfMCIdT lastNextMCId;
    bfTagT bfTag;
} insertRemoveXtntChainUndoRecT;

/*
 * undo_insert_xtnt_chain
 *
 * This function used to "undo" the actions performed by the insert_xtnt_chain() 
 * or the remove_xtnt_chain() function.
 *
 * THIS FUNCTION SHOULD NOT BE REMOVED !!!!!!!!!!!!!!!!!!!!!
 * 
 * This needs to exist in case this kernel encounters an older domain that
 * needs recovery run on it. We must support the undoing of the now
 * obsoleted routines.
 *
 */

void
undo_insert_remove_xtnt_chain (
                               ftxHT ftx,
                               int undoRecSize,
                               void *undoRecAddr
                               )
{
    insertRemoveXtntChainUndoRecT oldundoRec;
    linkUnlinkMcellsUndoRecT newundoRec;

    bcopy(undoRecAddr, &oldundoRec, sizeof(insertRemoveXtntChainUndoRecT));

    newundoRec.bfTag            = oldundoRec.bfTag;
    newundoRec.prevMCId         = oldundoRec.primMCId;
    newundoRec.prevNextMCId     = oldundoRec.primChainMCId;
    newundoRec.lastMCId         = oldundoRec.lastMCId;
    newundoRec.lastNextMCId     = oldundoRec.lastNextMCId;

    link_unlink_mcells_undo( ftx, undoRecSize,(void *)&newundoRec );


    return;

}  /* end undo_insert_remove_xtnt_chain */


/*
 * link_unlink_mcells_undo
 *
 * This function "undoes" the actions performed by the bmt_link_mcells()
 * or the bmt_unlink_mcells() function.
 *
 * This function assumes that the transaction was a sub-transaction and that
 * the parent transaction's do or undo routine has taken the appropriate action
 * to synchronize access to the mcells and the list that the mcells are on,
 * if any.
 *
 * During run-time, the parent transaction's do routine syncs access.  During
 * recovery, the parent transaction's undo routine syncs access.
 *
 * This function assumes that a higher level undo routine has acquired the
 * appropriate locks for unlinking the mcells.
 *
 * WARNING:  This function does image undo.  It assumes that the mcell list
 * is the same as when the mcells where linked/unlinked onto the list.  If
 * this undo routine is called after the mcell list synchronization is released,
 * this routine has unpredictable results.  FIX - Yikes!
 */

void
link_unlink_mcells_undo (
                         ftxHT ftx,
                         int undoRecSize,
                         void *undoRecAddr
                         )

{

    bsMPgT *bmt;
    domainT *dmnP;
    bsMCT *mcell;
    bsMCT *lastMcell;
    rbfPgRefHT pgPin;
    rbfPgRefHT lastPgPin;
    statusT sts;
    linkUnlinkMcellsUndoRecT undoRec;
    vdT *vd;
    bsXtntRT *xp = NULL;
    bsXtraXtntRT *xtrp = NULL;
    bfAccessT *mdap, *bfap;
    bsInMemXtntT *xtnts;

    bcopy(undoRecAddr, &undoRec, sizeof(linkUnlinkMcellsUndoRecT));

    dmnP = ftx.dmnP;

    /*
     * Restore the previous mcell's next mcell pointer.
     */

    vd = VD_HTOP(undoRec.prevMCId.volume, dmnP);

    if ( BS_BFTAG_RSVD(undoRec.bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }
    sts = rbf_pinpg (
                     &pgPin,
                     (void *)&bmt,
                     mdap,
                     undoRec.prevMCId.page,
                     BS_NIL,
                     ftx,
                     MF_NO_VERIFY
                     );
    if (sts != EOK) {
        domain_panic(ftx.dmnP, "link_unlink_mcells_undo: rbf_pinpg(1) failed, return code = %d", sts );
        return;
    }
    mcell = &(bmt->bsMCA[undoRec.prevMCId.cell]);

    /*
     * Restore the last mcell's next mcell pointer.
     */

    vd = VD_HTOP(undoRec.lastMCId.volume, dmnP);

    if ( BS_BFTAG_RSVD(undoRec.bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg (
            &lastPgPin,
            (void *)&bmt,
            mdap,
            undoRec.lastMCId.page,
            BS_NIL,
            ftx,
            MF_NO_VERIFY
            );
    if (sts != EOK) {
        
        domain_panic(ftx.dmnP, "link_unlink_mcells_undo: rbf_pinpg(2) "
                               "failed, return code = %d", sts );
        return;
    }
    lastMcell = &(bmt->bsMCA[undoRec.lastMCId.cell]);

    /*
     * Search for a BSR_XTRA_XTNTS record in the last mcell. If found
     * then search for a BSR_XTNTS record in the previous mcell. If
     * also found then unlink the mcells via the bsXtntRT.chainMCId field.
     * Otherwise, use the standard mcell chain via the bsMCT.nextMCId
     * field.
     */
    xtrp = bmtr_find(lastMcell, BSR_XTRA_XTNTS, dmnP);
    if (xtrp) {
        xp = bmtr_find(mcell, BSR_XTNTS, dmnP);
    }

    if (xp) {
        rbf_pin_record (
                        pgPin,
                        &(xp->chainMCId),
                        sizeof(xp->chainMCId)
                        );
        xp->chainMCId = undoRec.prevNextMCId;
        ADVFS_CRASH_RECOVERY_TEST(ftx.dmnP, AdvfsCrash_116);
    }
    else {
        rbf_pin_record (
                        pgPin,
                        &(mcell->mcNextMCId),
                        sizeof(mcell->mcNextMCId)
                        );
        mcell->mcNextMCId = undoRec.prevNextMCId;
        ADVFS_CRASH_RECOVERY_TEST(ftx.dmnP, AdvfsCrash_117);
    }

    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->mcNextMCId),
                    sizeof(lastMcell->mcNextMCId)
            );
    lastMcell->mcNextMCId = undoRec.lastNextMCId;
    ADVFS_CRASH_RECOVERY_TEST(ftx.dmnP, AdvfsCrash_118);

    /* We no longer need to force the extent maps to be reloaded here
     * for the rbmt/bmt. This will now be done for all files in a
     * seperate undo that runs after this undo runs.
     */

    return;

}  /* end link_unlink_mcells_undo */

/*
 * bmt_link_mcells
 *
 * This function links the mcell chain after the mcell specified by the
 * "prevMcellId" parameter.  The first mcell in the chain is specified by
 * the "firstMcellId" parameter.  The last mcell in the chain is specified
 * by the "lastMcellId" parameter.
 *
 * This function assumes that the caller has exclusive access to the previous
 * mcell and the mcell chain.
 *
 * This function's undo routine does image undo.  See the
 * link_unlink_mcells_undo() function header comment for further information.
 *
 *
 * This function will link a list of mcells containing BSR_XTRA_XTNTS
 * (bsXtraXtntRT) records into the BSR_XTNTS (bsXtntRT) extents chain
 * (chainMCId):  
 *                IF  the prevMcell contains a BSR_XTNTS record
 *                AND the lastMcell contains a BSR_XTRA_XTNTS record
 *                
 * NOTE1: The lastMcell is searched for a BSR_XTRA_XTNTS record instead
 *        of the firstMcell to avoid an additional rbf_pinpg() call. 
 *
 * NOTE2: If the lastMcell contains a BSR_XTRA_XTNTS record then the entire
 *        list of mcells identified by the firstMcellId and lastMcellId MUST
 *        be BSR_XTRA_XTNTS records otherwise non-BSR_XTRA_XTNTS records will
 *        be placed on the extents chain.
 *
 * NOTE3: A BSR_XTRA_XTNTS record fully consumes an mcell.
 * 
 *
 */

statusT
bmt_link_mcells (
                 domainT *dmnP,  /* in */
                 bfTagT bfTag, /* in */
                 bfMCIdT prevMcellId,  /* in */
                 bfMCIdT firstMcellId,  /* in */
                 bfMCIdT lastMcellId,  /* in */
                 ftxHT parentFtx  /* in */
                 )

{

    bsMPgT *bmt;
    ftxHT ftx;
    bsMCT *lastMcell;
    rbfPgRefHT lastPgPin;
    bsMCT *prevMcell;
    rbfPgRefHT prevPgPin;
    statusT sts;
    linkUnlinkMcellsUndoRecT undoRec;
    vdT *vd;
    bsXtntRT *xp = NULL;
    bsXtraXtntRT *xtrp = NULL;
    bfAccessT *mdap;

    vd = VD_HTOP(prevMcellId.volume, dmnP);

    sts = FTX_START_META (FTA_BS_BMT_LINK_MCELLS_V1, &ftx, parentFtx,
                          vd->dmnP);
    if (sts != EOK) {
        return sts;
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }
    sts = rbf_pinpg (
                     &prevPgPin,
                     (void*)&bmt,
                     mdap,
                     prevMcellId.page,
                     BS_NIL,
                     ftx, 
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_350);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_351);
        return sts;
    }
    prevMcell = &(bmt->bsMCA[prevMcellId.cell]);


    vd = VD_HTOP(lastMcellId.volume, dmnP);

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg (
                     &lastPgPin,
                     (void*)&bmt,
                     mdap,
                     lastMcellId.page,
                     BS_NIL,
                     ftx,
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_352);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_353);
        return sts;
    }
    lastMcell = &(bmt->bsMCA[lastMcellId.cell]);

    undoRec.bfTag = bfTag;
    undoRec.prevMCId = prevMcellId;


    /*
     * Search for a BSR_XTRA_XTNTS record in the last mcell. If found
     * then search for a BSR_XTNTS record in the previous mcell. If
     * also found then link the mcells via the bsXtntRT.chainMCId field.
     * Otherwise, use the standard mcell chain via the bsMCT.nextMCId
     * field.
     */ 
    xtrp = bmtr_find(lastMcell, BSR_XTRA_XTNTS, dmnP); 
    if (xtrp) {
        xp = bmtr_find(prevMcell, BSR_XTNTS, dmnP);
    }
    
    if (xp) {
        undoRec.prevNextMCId = xp->chainMCId;
    } else {
        undoRec.prevNextMCId = prevMcell->mcNextMCId;
    }
    undoRec.lastMCId = lastMcellId;
    undoRec.lastNextMCId = lastMcell->mcNextMCId;

    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->mcNextMCId),
                    sizeof(lastMcell->mcNextMCId)
                    );
    if (xp) {
        lastMcell->mcNextMCId = xp->chainMCId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_119);
    }
    else {
        lastMcell->mcNextMCId = prevMcell->mcNextMCId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_120);
    }

    if (xp) {
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainMCId),
                        sizeof(xp->chainMCId)
                        );
        xp->chainMCId = firstMcellId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_121);
    } else {
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->mcNextMCId),
                        sizeof(prevMcell->mcNextMCId)
                        );
        prevMcell->mcNextMCId = firstMcellId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_122);
    }

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_123);
    ftx_done_u (ftx, FTA_BS_BMT_LINK_MCELLS_V1, sizeof (undoRec), &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_124);

    return sts;

}  /* end bmt_link_mcells */

/*
 * bmt_unlink_mcells
 *
 * This function unlinks the mcells chained to the mcell specified by the
 * "prevVdIndex" and "prevMcellId" parameters.  The last mcell in the
 * chain is specified by the "lastVdIndex" and "lastMcellId" parameters.
 *
 * If the previous mcell does not point to the first mcell, this is
 * a fatal error;  a domain panic will occur.
 *
 * This function assumes that the caller has exclusive access to the mcell
 * list.
 *
 * NOTE: This function is not equiped to unlink a BSR_XTNTS from the
 *       primary MCELL. In other words firstMcellId CAN NOT be of
 *       type BSR_XTNTS !.
 *
 * This function's undo routine does image undo.  See the link_unlink_mcells_undo()
 * function header comment for further information.
 */

statusT
bmt_unlink_mcells (
                   domainT *dmnP,  /* in */
                   bfTagT bfTag, /* in */
                   bfMCIdT prevMcellId,  /* in */
                   bfMCIdT firstMcellId,  /* in */
                   bfMCIdT lastMcellId,  /* in */
                   ftxHT parentFtx,  /* in */
		   unlinkFlagT unlinkFlag /* in */
                   )

{

    bsMPgT *bmt;
    ftxHT ftx;
    bsMCT *lastMcell;
    rbfPgRefHT lastPgPin;
    bsMCT *prevMcell;
    rbfPgRefHT prevPgPin;
    statusT sts;
    linkUnlinkMcellsUndoRecT undoRec;
    vdT *vd;
    bsXtntRT *xp = NULL;
    bfAccessT *mdap;

    vd = VD_HTOP(prevMcellId.volume, dmnP);

    sts = FTX_START_META (FTA_BS_BMT_UNLINK_MCELLS_V1, &ftx, parentFtx,
                          vd->dmnP);
    if (sts != EOK) {
        return sts;
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }
    sts = rbf_pinpg (
                     &prevPgPin,
                     (void*)&bmt,
                     mdap,
                     prevMcellId.page,
                     BS_NIL,
                     ftx,
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_354);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_355);
        return sts;
    }
    prevMcell = &(bmt->bsMCA[prevMcellId.cell]);

    /*
     * See if there is a BSR_XTNTS record in the previous mcell.
     * If so, we're going to disconnect the chain via the
     * bsXtntRT.chain* fields.  This is indicated by the CHAINMCID unlink
     * flag.  Otherwise, we're going to use the bsMCT.next* fields in the
     * mcell header.
     *
     * However, we could have the case where we want to delete an mcell linked
     * to a previous mcell containing a record of BSR_XTNTS, but we do not want
     * to delete the extra extent records.  In this case, we will pass the
     * NEXTMCID unlink flag to indicate this is a normal mcell we're deleting,
     */
    if( unlinkFlag == CHAINMCID ) {
	xp = bmtr_find(prevMcell, BSR_XTNTS, dmnP);
    } else if( unlinkFlag == NEXTMCID ) {
	xp = NULL;
    }

    if (xp) {
        if ( !MCID_EQL(firstMcellId, xp->chainMCId)) {
            /*
             * This is a condition that only affects this domain.
             * Domain panic - don't panic.  Also, clean up before we leave.
             */
            printf("firstMcellId.volume is %lu - xp->chainMCId.volume is %lu\n",
                   firstMcellId.volume , xp->chainMCId.volume );
            printf("firstMcellId.page is %lu - xp->chainMCId.page is %lu\n",
                   firstMcellId.page , xp->chainMCId.page);
            printf("firstMcellId.cell is %lu - xp->chainMCId.cell is %lu\n",
                   firstMcellId.cell, xp->chainMCId.cell);
            domain_panic( dmnP,
               "previous mcell doesn't point to first mcell" );
            ftx_fail (ftx);
            return E_DOMAIN_PANIC;
        }
    }
    else {
        if ( !MCID_EQL(firstMcellId, prevMcell->mcNextMCId)) {
            /*
             * This is a condition that only affects this domain.
             * Domain panic - don't panic.  Also, clean up before we leave.
             */
            printf("firstMcellId.volume is %lu - prevMcell->mcNextMCId.volume is %lu\n",
                   firstMcellId.volume, prevMcell->mcNextMCId.volume);
            printf("firstMcellId.page is %lu - prevMcell->mcNextMCId.page is %lu\n",
                   firstMcellId.page , prevMcell->mcNextMCId.page);
            printf("firstMcellId.cell is %lu - prevMcell->mcNextMCId.cell is %lu\n",
                   firstMcellId.cell, prevMcell->mcNextMCId.cell);
            domain_panic( dmnP,
               "previous mcell doesn't point to first mcell" );
            ftx_fail (ftx);
            return E_DOMAIN_PANIC;
        }
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(lastMcellId.volume, dmnP)->rbmtp;
    } else {
        mdap = VD_HTOP(lastMcellId.volume, dmnP)->bmtp;
    }
    sts = rbf_pinpg (
                     &lastPgPin,
                     (void*)&bmt,
                     mdap,
                     lastMcellId.page,
                     BS_NIL,
                     ftx,
                     MF_VERIFY_PAGE
                     );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_362);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_363);
        return sts;
    }
    lastMcell = &(bmt->bsMCA[lastMcellId.cell]);

    undoRec.bfTag = bfTag;
    undoRec.prevMCId = prevMcellId;
    if (xp) {
        undoRec.prevNextMCId = xp->chainMCId;
    }
    else {
        undoRec.prevNextMCId = prevMcell->mcNextMCId;
    }
    undoRec.lastMCId = lastMcellId;
    undoRec.lastNextMCId = lastMcell->mcNextMCId;

    if (xp) {
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainMCId),
                        sizeof (xp->chainMCId)
                        );
        xp->chainMCId = lastMcell->mcNextMCId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_125);
    }
    else {
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->mcNextMCId),
                        sizeof (prevMcell->mcNextMCId)
                        );
        prevMcell->mcNextMCId = lastMcell->mcNextMCId;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_126);
    }

    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->mcNextMCId),
                    sizeof (lastMcell->mcNextMCId)
                    );
    lastMcell->mcNextMCId = bsNilMCId;

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_127);
    ftx_done_u (ftx, FTA_BS_BMT_UNLINK_MCELLS_V1, sizeof (undoRec), &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_128);

    return sts;

}  /* end bmt_unlink_mcells */

/*
 * bmt_free_mcell
 *
 * Put the specified mcell on the free mcell list.
 * Caller has locked the vdT.mcell_lk.  This function does not undo itself,
 * and modifies pages in the given ftx, which cannot be aborted after
 * this.
 */

void
bmt_free_mcell(
               ftxHT ftxH,      /* in - ftx handle */
               vdT* vdp,        /* in - pointer to virtual disk */
               bsMCT* mcp,      /* in - mcell pointer */
               bfMCIdT mcid,    /* in - mcell id */
               bsMPgT* bmtpgp,  /* in - page ptr */
               rbfPgRefHT pgref /* in - page ref */
               )
{
    statusT sts = EOK;
    bsMRT *rec;
    domainT* dmnP = vdp->dmnP;
#ifdef ADVFS_SMP_ASSERT
    bfMCIdT ddlMcellId;
    int delFlag;
    extern int AdvfsCheckDDL;
#endif


    MS_SMP_ASSERT(ADVFS_MUTEX_OWNED(&vdp->mcell_lk.lock.mtx ));

    if (FETCH_MC_PAGE(mcid) != bmtpgp->bmtPageId) {
        ADVFS_SAD0( "bmt_free_mcell: page id does not agree with mcid" );
    }

    if (BS_BFTAG_EQL(mcp->mcTag,NilBfTag) &&
        BS_BFTAG_EQL(mcp->mcBfSetTag,NilBfTag)) {
        ADVFS_SAD2("bmt_free_mcell: mcell already free",mcid.cell,mcid.page);
    }

#ifdef ADVFS_SMP_ASSERT
    /*
     * If we're freeing this mcell, it had better not be on the DDL.
     */
    if (AdvfsEnableAsserts && AdvfsCheckDDL) {
        sts = del_find_del_entry(dmnP, vdp->vdIndex, mcp->mcBfSetTag,
                                 mcp->mcTag, &ddlMcellId, &delFlag);
        if ((sts == EOK) &&
            ((mcid.page == ddlMcellId.page) &&
             (mcid.cell == ddlMcellId.cell))) {
            /*
             * This should never happen.  It means we're about to free an
             * mcell that is on the DDL.
             */
            ADVFS_SAD0("trap code:  Freeing mcell on DDL!");
        }
    }
#endif

    rec = (bsMRT *)&(mcp->bsMR0[0]);

    /* 
     * There used to be code to support full page mcells.  We no longer
     * support that and we never used it.  This just checks for that case.
     */
    MS_SMP_ASSERT ( (rec->bCnt - sizeof(bsMRT)) <= BS_USABLE_MCELL_SPACE );

    /*
    ** Link the mcell into it's page's free mcell list.
    */
    rbf_pin_record( pgref,
                    &mcp->mcNextMCId,
                    sizeof(bs_mcell_hdr_t) + sizeof (bsMRT) );
    
    if ( bmtpgp->bmtFreeMcellCnt == 0 ) {
        MS_SMP_ASSERT( bmtpgp->bmtNextFreeMcell == 0 );
        mcp->mcNextMCId = bsNilMCId;
    } else {
       mcp->mcNextMCId.volume = bmtpgp->bmtVdIndex;
       mcp->mcNextMCId.page   = bmtpgp->bmtPageId;
       mcp->mcNextMCId.cell   = bmtpgp->bmtNextFreeMcell;
    }
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_129);
    mcp->mcLinkSegment = 0;
    mcp->mcNumRecords  = 0;
    mcp->mcRsvd1       = 0;
    mcp->mcRsvd2       = 0;
    mcp->mcTag         = NilBfTag;
    mcp->mcBfSetTag    = NilBfTag;
    
    rec->bCnt = sizeof (bsMRT);
    rec->type = BSR_NIL;
    rec->padding = 0;
    
    rbf_pin_record( pgref,
                    &(bmtpgp->bmtTrackMcells),
                    sizeof(bs_bmt_track_mcells_t));
    
    bmtpgp->bmtNextFreeMcell = mcid.cell;
    bmtpgp->bmtFreeMcellCnt++;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_130);
    
    /*
    ** If the page's mcell free list had been empty (until we added
    ** the current mcell to it) then we need to put this page at the
    ** head of the global mcell free list (the list of pages that
    ** contain free mcells).
    */
    
    if ( bmtpgp->bmtFreeMcellCnt == 1 ) {
        
        rbf_pin_record( pgref,
                        &(bmtpgp->bmtNextFreePg),
                        (int32_t)sizeof(bmtpgp->bmtNextFreePg));

        bmtpgp->bmtNextFreePg =
            bmt_upd_mcell_free_list( ftxH, vdp, bmtpgp->bmtPageId );
    }

    return;
}

/*
 * Check to see if the extents in the mcell lie in the given range.
 * If one does, add it to the pack linked list hanging from this
 * volume vd structure.
 */

static statusT
check_xtnt_in_range(
                    vdT *vdp,  /* in */
                    struct bsMC *mcp,  /* in */
                    bf_vd_blk_t cRangeBeginBlk,  /* in */
                    bf_vd_blk_t cRangeEndBlk,    /* in */
                    int *list_too_big,          /* out - call back required? */
                    int *bmt_cnt                /* out - Count of fobs in range */
                   )
{
    statusT sts=EOK;
    bsXtntT *fxp;
    int xCnt;
    bsXtraXtntRT *xrp;
    bsXtntRT *xp;
    int j;
    bf_vd_blk_t extentEndBlk, extentBeginBlk;
    bf_vd_blk_t migStartBlk, migEndBlk;
    bf_fob_t xm_fob_offset;
    bf_fob_t xm_fob_cnt;

    ssPackLLT *currp=NULL, *xtntp=NULL;
    ssPackHdrT *plp=NULL;
    int inserted=FALSE;

    /* scan the in-use mcell for xtnt records */
    xCnt = 0;
    fxp = NULL;

    if ((xrp = (bsXtraXtntRT *)
            bmtr_find(mcp, BSR_XTRA_XTNTS, vdp->dmnP)) != NULL) {
        fxp = xrp->bsXA;
        xCnt = xrp->xCnt;
    }
    else if ( (xp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) != NULL) {
        /* ODS changes - only 1 type of extents exists now  */
        fxp = xp->bsXA;
        xCnt = xp->xCnt;
    }

    /* look at the extents record and search the extents in it for range input */
    for(j=0; j<(xCnt-1); j++) {

        /* try to find block in extent */
        if(fxp[j].bsx_vd_blk== XTNT_TERM ||
            fxp[j].bsx_vd_blk== COWED_HOLE) {
            continue;    /* on to next extent */
        }

        extentBeginBlk = fxp[j].bsx_vd_blk;
        extentEndBlk = fxp[j].bsx_vd_blk +
                       ((fxp[j+1].bsx_fob_offset - 
                         fxp[j].bsx_fob_offset) / ADVFS_FOBS_PER_DEV_BSIZE); 
        /*
         * test for this extent overlapping clear range -
         * first check if begins in range, then check if ends in range,
         * finally check if it extends on both sides of range.
         * If its in range add it to list.
         *
         * We cannot go ahead and move the extents at this time because
         * the extents must all be gathered so that they can be placed in
         * order.  They must be moved in order from left to right in sequence
         * as they lie in the clear range.  If we did not do this then an
         * extent to the right of another extent in the range would end up
         * overlapping the extent to the left of it when it was migrated.
         * Moving them from left to right ensures that moves will not overlap
         * each other.  This is why we create a list of all tags and later
         * do the migrating!
         */
        if( ((cRangeBeginBlk >= extentBeginBlk) &&
             (cRangeBeginBlk <  extentEndBlk)) ||
            ((cRangeEndBlk   >  extentBeginBlk) &&
             (cRangeEndBlk   <= extentEndBlk)) ||
            ((cRangeBeginBlk <= extentBeginBlk) &&
             (cRangeEndBlk   >  extentEndBlk)) )     {

            if (BS_BFTAG_RSVD(mcp->mcTag)) {
               /* ran into some reserved file allocated bits that are in the
                * range to be packed.  Simply abort since we can't migrate
                * reserved files with the standard migrate.
                */
                RAISE_EXCEPTION(E_RSVD_FILE_INWAY); /* exit routine */
            }

            /* adjust entry for only that part of extent thats in range */
            migStartBlk = MAX(cRangeBeginBlk,extentBeginBlk);
            migEndBlk = MIN(cRangeEndBlk,extentEndBlk);

            xm_fob_offset = fxp[j].bsx_fob_offset + 
                ((migStartBlk-extentBeginBlk) * ADVFS_FOBS_PER_DEV_BSIZE);
            xm_fob_cnt = ((MIN(cRangeEndBlk, extentEndBlk) - migStartBlk) *
                                ADVFS_FOBS_PER_DEV_BSIZE);
                      

            /* Allocate a list entry for the extent found in the clear range */
            xtntp = (ssPackLLT *)(ms_malloc( sizeof(ssPackLLT) ));

            /* add this entry to the linked list */
            xtntp->ssPackBfSetId.domainId   = vdp->dmnP->domainId;
            xtntp->ssPackBfSetId.dirTag.tag_num = mcp->mcBfSetTag.tag_num;
            xtntp->ssPackBfSetId.dirTag.tag_seq = mcp->mcBfSetTag.tag_seq;
            xtntp->ssPackTag.tag_num    = mcp->mcTag.tag_num;
            xtntp->ssPackTag.tag_seq    = mcp->mcTag.tag_seq;
            xtntp->ssPackFobOffset  = xm_fob_offset;
            xtntp->ssPackFobCount   = xm_fob_cnt;
            xtntp->ssPackStartXtBlock = migStartBlk;
            xtntp->ssPackEndXtBlock   = migEndBlk;
            *bmt_cnt += xtntp->ssPackFobCount;

            /* Put this file's extent range onto the list so that it can be
             * moved later by caller.
             *
             * add to list - sorted by xtntp->ssPackStartXtBlock asc
             * Note: Since this list is sorted by the blk number, any
             * subsequent calls to this routine will get the next
             * highest block set of files.   Another call to this routine
             * is required whenever the list has been maxed out in size.
             */

            inserted=FALSE;
            plp = &vdp->ssVolInfo.ssPackHdr;
            currp = plp->ssPackFwd;

            while( currp != (ssPackLLT *)plp ) {
                if (xtntp->ssPackStartXtBlock <
                    currp->ssPackStartXtBlock ) {
                    /* insert xtntp at head or middle of list, before currp */
                    xtntp->ssPackFwd = currp;
                    xtntp->ssPackBwd = currp->ssPackBwd;
                    currp->ssPackBwd->ssPackFwd = xtntp;
                    currp->ssPackBwd = xtntp;
                    plp->ssPackListCnt++;
                    inserted = TRUE;
                    break;
                }
                currp = currp->ssPackFwd;  /* Keep going! */
            } /* end while */

            /* if not inserted above, put it on end of list */
            if(!inserted) {
                /* list not full yet - goes on end of list */
                xtntp->ssPackFwd = (ssPackLLT *) plp;
                xtntp->ssPackBwd = plp->ssPackBwd;
                plp->ssPackBwd->ssPackFwd = xtntp;
                plp->ssPackBwd = xtntp;
                plp->ssPackListCnt++;
            } /* end if not inserted */

            /* check for list too big */
            if ( plp->ssPackListCnt > SS_INIT_PACK_LST_SZ ) {
                /* too many entries in list, delete the last one in list */
                currp = plp->ssPackBwd;
                *bmt_cnt -= currp->ssPackFobCount;
                currp->ssPackFwd->ssPackBwd = currp->ssPackBwd;
                currp->ssPackBwd->ssPackFwd = currp->ssPackFwd;
                currp->ssPackFwd = currp->ssPackBwd = 0;
                ms_free(currp); plp->ssPackListCnt--;
                *list_too_big = TRUE;
            }
        } /* end if found xtnt in the way */
    } /* end for all extents in in-use mcell */

HANDLE_EXCEPTION:
    return sts;
}

/*
 * bmt_get_vd_bf_inway -
 * This routine reads the specified vd's rbmt & bmt and returns a linked list
 * of files and their pages that are located in the disk block range provided.
 */

statusT
bmt_get_vd_bf_inway (
                vdT *vdp,  /* in */
                bf_vd_blk_t cRangeBeginBlk,  /* in */
                bf_vd_blk_t cRangeEndBlk,    /* in */
                int *another_scan_required,/* out */
                int *bmt_cnt            /* out - cnt of allocated fobs in range */
                )
{
    bmtHT bmtH;
    bmtHT *bmth=NULL;
    int closeBmtFlag = 0, closeRbmtFlag=0;
    statusT sts;
    struct bsMC *mcp = NULL;
    bfAccessT *mdap;
    struct bsMPg* rbmtp = NULL;
    bfPageRefHT pgref;
    int list_too_big=FALSE;
    int doing_bmt = TRUE;

    *another_scan_required = FALSE;
    /*
     *  Do rbmt first if there. Optimizes because if rbmt has an extent in the
     *  way then we can abort this range since we cant move reseerved files.
     */
    doing_bmt = FALSE;

chk_bmt:

    bmtH.curPg = bsNilMCId.page;
    bmtH.curMcell = bsNilMCId.cell;

    bmth = &bmtH;

    bmth->vdp = vdp;
    sts = bs_refpg(&bmth->pgRef,
                   (void *)&bmth->pgPtr,
                   (doing_bmt==FALSE)?vdp->rbmtp:vdp->bmtp,
                   bmth->curPg,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if(doing_bmt==FALSE)
        closeRbmtFlag = 1;
    else
        closeBmtFlag = 1;

    /*
     ** Scan the bmt and rbmt pages until an mcell is found.
     */
    while (1) {

        if (bmth->curMcell >= BSPG_CELLS) {
            /*
             ** We've scanned past the end of the current pages.  Release it
             ** and go on to the next page.
             */
            sts = bs_derefpg( bmth->pgRef, BS_CACHE_IT );
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }
            bmth->pgPtr = NULL;
            bmth->curPg++;
            bmth->curMcell = 0;

            if ((bmth->curPg % SS_INIT_BMT_PAGES_PER_PASS) == 0) {
                /* wait for next run approval to continue */
                sts = ss_block_and_wait(vdp);
                if(sts != EOK) {
                    RAISE_EXCEPTION(sts);
                }
            }

            if((doing_bmt==TRUE) &&   /* check for end of bmt */
               (bmth->curPg >= 
                (vdp->bmtp->bfaNextFob / vdp->bmtp->bfPageSz ))) {
                /* no more pages in bmt to process */
                RAISE_EXCEPTION(EOK);
            }
            if((doing_bmt==FALSE) &&   /* check for end of rbmt */
               (bmth->curPg >= 
                (vdp->rbmtp->bfaNextFob / vdp->bmtp->bfPageSz ))) {
                /* no more pages in rbmt to process */
                RAISE_EXCEPTION(EOK);
            }

            /*
             ** Try and get the next (R)BMT page.
             */
            sts = bs_refpg(&bmth->pgRef,
                           (void *)&bmth->pgPtr,
                           (doing_bmt==FALSE) ? bmth->vdp->rbmtp 
                                              : bmth->vdp->bmtp,
                           bmth->curPg,
                           FtxNilFtxH,
                           MF_VERIFY_PAGE);
            if (sts != EOK) {
                if(sts == E_RANGE_NOT_MAPPED) {
                    /* no more pages in bmt to process */
                    RAISE_EXCEPTION(EOK);
                } else
                    RAISE_EXCEPTION(sts);
            }

        }
        mcp = &bmth->pgPtr->bsMCA[bmth->curMcell];
        bmth->curMcell++;   /* bump to the next mcell for next loop */

        if ((!BS_BFTAG_EQL (mcp->mcBfSetTag, NilBfTag)) &&
            (!BS_BFTAG_EQL (mcp->mcTag, NilBfTag))) {

            /*
             * Found an in-use mcell.
             * now scan the in-use mcell for xtnt records in pack range
             */
            sts = check_xtnt_in_range(
                                vdp,
                                mcp,
                                cRangeBeginBlk,
                                cRangeEndBlk,
                                &list_too_big,
                                bmt_cnt
                                );
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }
            if(list_too_big==TRUE) {
                *another_scan_required = TRUE;
            }
        }

    }  /* end while looking for an in-use mcell */

HANDLE_EXCEPTION:

    if (closeRbmtFlag != 0) {
        /* dereferences the last page */
        bmt_close (bmth);
        closeRbmtFlag = 0;
    }

    /* if rbmt processed ok, go back and look in the bmt */
    if((sts==EOK) &&
       (doing_bmt == FALSE)) {
        doing_bmt = TRUE;
        goto chk_bmt;
    }

    if (closeBmtFlag != 0) {
        /* dereferences the last page */
        bmt_close (bmth);
        closeBmtFlag = 0;
    }

    return sts;
}   /* end bmt_get_vd_bf_inway */

/*
 * bmt_get_vd_bf_descs
 *
 * This function reads the specified bmt and returns an array of bitfile
 * descriptors (bfSetTag/bfTag pairs).
 */

statusT
bmt_get_vd_bf_descs (
                     domainT *dmnP,  /* in */
                     vdIndexT vdIndex,  /* in */
                     int bfDescSize,  /* in */
                     bsBfDescT *bfDesc,  /* in, initialized */
                     bfMCIdT *nextBfDescId,  /* in/out */
                     int *bfDescCnt  /* out */
                     )
{
    bmtHT bmtH;
    int i;
    statusT sts;

    bmtH.curPg = FETCH_MC_PAGE(*nextBfDescId);
    bmtH.curMcell = FETCH_MC_CELL(*nextBfDescId);

    sts = bmt_open (&bmtH, dmnP, vdIndex);
    if (sts != EOK) {
        if (sts == E_RANGE_NOT_MAPPED) {
            *bfDescCnt = 0;
            sts = EOK;
        }
        return sts;
    }

    for (i = 0; i < bfDescSize; i++) {
        sts = bmt_read (
                        &bmtH,
                        &(bfDesc[i].bfSetTag),
                        &(bfDesc[i].bfTag),
                        &(bfDesc[i].metadataType)
                        );
        if (sts != EOK) {
            if (sts == E_RANGE_NOT_MAPPED) {
                break;  /* out of for */
            } else {
                bmt_close (&bmtH);
                return sts;
            }
        }
    }  /* end for */

    sts = bmt_close (&bmtH);
    if (sts != EOK) {
        return sts;
    }

    *bfDescCnt = i;
    nextBfDescId->page = bmtH.curPg;
    nextBfDescId->cell = bmtH.curMcell;

    return sts;

}   /* end bmt_get_vd_bf_descs */

/*
 * bmt_open
 *
 * "Opens" the BMT for the specified domain and virtual disk
 * (via dmnP and vdIndex).  Since the BMT is always open this
 * routine doesn't bother to really open it.
 *
 * This routine also sets up the "state buffer" maintained in bmth
 * and reads the first page specified in bmth.  The caller must
 * initalize the "curMcell" and "curPg" bmth fields.
 *
 * Returns EBAD_VDI or EOK
 */

statusT
bmt_open(
           bmtHT *bmth,      /* out - BMT handle */
           domainT *dmnP,    /* in - domain pointer */
           vdIndexT vdIndex   /* in - vd index */
           )
{
    statusT sts = EOK;

    if ( bmth->vdp = vd_htop_if_valid(vdIndex, dmnP, TRUE, FALSE) ) {
        /*              
         * bs_refpg does not return E_RANGE_NOT_MAPPED and therefore we  
         * we must be sure not to ref a page past the end of the BMT (and
         * we aren't concerned with holes -- the BMT can't have any).
         */
        if(!advfs_bs_is_offset_mapped(bmth->vdp->bmtp,
                                      (off_t)((bmth->curPg) *
                                              (bmth->vdp->bmtp->bfPageSz *
                                               ADVFS_FOB_SZ))) ) {
            /* be sure to dec the vdRefCnt */
            vd_dec_refcnt( bmth->vdp );
            return E_RANGE_NOT_MAPPED;
        }
        /*
         * Setup state buffer and read the first page.
         */
        sts = bs_refpg(&bmth->pgRef,
                       (void *)&bmth->pgPtr,
                       bmth->vdp->bmtp,
                       bmth->curPg,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);

        vd_dec_refcnt( bmth->vdp );
        return sts;

    } else {
        /*
         ** Major bummer!  Caller passed a crummy vd index!.
         */
        return EBAD_VDI;
    }
}

/*
 * bmt_read
 *
 * Reads and scans BMT pages looking for in-use bitfile mcells.
 * When such an mcell is found, the corresponding bitfile set tag,
 * bitfile tag and mcell metadata type are returned.  The current
 * position within the BMT page is saved in 'bmth' so that on the
 * next call to this routine it knows where it left off.  Also, the
 * BMT page is kept referenced between calls in order to elliminate
 * the need to call ref/deref each time this routine is called; therefore,
 * users of these BMT routines must call bmt_close() when they
 * are finished scanning the BMT.
 *
 * Returns possible status from bs_derefpg, bs_refpg, or EOK.
 */

statusT
bmt_read(
           bmtHT *bmth,  /* in - BMT handle */
           bfTagT *bfSetTag,   /* out */
           bfTagT *bfTag,   /* out */
           metadataTypeT *metadataType  /* out */
           )
{
    statusT sts = EOK;
    int found = FALSE;
    struct bsMC* mcp;

    /*
     ** Scan the pages until a primary mcell is found.
     */
    while (!found) {

        if (bmth->curMcell >= BSPG_CELLS) {
            /*              
             * bs_refpg does not return E_RANGE_NOT_MAPPED and therefore we
             * must be sure not to ref a page past the end of the BMT (and
             * we aren't concerned with holes -- the BMT can't have any).
             */
            if(!advfs_bs_is_offset_mapped(bmth->vdp->bmtp,
                                          (off_t)((bmth->curPg+1) *
                                                  (bmth->vdp->bmtp->bfPageSz *
                                                   ADVFS_FOB_SZ))) ) {
                return E_RANGE_NOT_MAPPED;
            }
            
            /*
             ** We've scanned past the end of the current pages.  Release it
             ** and go on to the next page.
             */
            sts = bs_derefpg( bmth->pgRef, BS_CACHE_IT );
            if (sts != EOK) {
                return sts;
            }
            bmth->pgPtr = NULL;
            bmth->curPg++;
            bmth->curMcell = 0;

            /*
             ** Get the next BMT page.
             */
            sts = bs_refpg(&bmth->pgRef,
                           (void *)&bmth->pgPtr,
                           bmth->vdp->bmtp,
                           bmth->curPg,
                           FtxNilFtxH,
                           MF_VERIFY_PAGE);
            if (sts != EOK) {
                return sts;
            }
        }

        mcp = &bmth->pgPtr->bsMCA[bmth->curMcell];
        bmth->curMcell++;

        if ((BS_BFTAG_REG(mcp->mcTag)) &&
            (!BS_BFTAG_EQL (mcp->mcBfSetTag, NilBfTag)) &&
            (!BS_BFTAG_EQL (mcp->mcTag, NilBfTag))) {
            /*
             ** Found an in-use mcell.
             */
            found = TRUE;
        }
    }  /* end while */

    *bfSetTag = mcp->mcBfSetTag;
    *bfTag = mcp->mcTag;

    if (bmtr_find (mcp, BSR_XTRA_XTNTS, bmth->vdp->dmnP) != NULL) {
        *metadataType = BMT_XTNT_METADATA;
    } else if (bmtr_find (mcp, BSR_XTNTS, bmth->vdp->dmnP) != NULL) {
         *metadataType = BMT_PRIME_MCELL_XTNT_METADATA;
    } else {
        *metadataType = BMT_NORMAL_METADATA;
    }

    return EOK;
}

/*
 * bmt_close
 *
 * Resets the state buffer (bmth) to indicate that no BMT
 * is being access and ensures that no BMT pages are left
 * referenced.
 *
 * Returns status from bs_derefpg.
 */

statusT
bmt_close(
            bmtHT *bmth   /* in - BMT handle */
            )
{
    statusT sts = EOK;

    if (bmth->pgPtr != NULL) {
        /*
         ** Release the BMT page.
         */
        sts = bs_derefpg( bmth->pgRef, BS_CACHE_IT );
    }

    bmth->vdp = NULL;
    bmth->pgPtr = NULL;

    return sts;
}

/*
 * init_bs_bmt_util_opx
 *
 * Initializes the bmt utility agent's ftx operations.
 */

void
init_bs_bmt_util_opx( void )
{
    statusT sts;

    sts = ftx_register_agent( FTA_BS_BMT_PUT_REC_V1,
                              &bmtr_put_rec_undo_opx, /* undo */
                              NULL  /* root done */
                              );
    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent_n2( FTA_BS_BMT_FREE_BF_MCELLS_V1,
                                0, /* undo */
                                &free_mcell_chains_opx,  /* root done */
                                0,  /* redo */
                                &free_mcell_chains_opx  /* continuation */
                                );

    if (sts != EOK) {
        goto abort;
    }
    sts = ftx_register_agent( FTA_BS_BMT_ALLOC_MCELL_V1,
                              &alloc_mcell_undo, /* undo */
                              NULL /* root done */
                              );

    if (sts != EOK) {
        goto abort;
    }
    sts = ftx_register_agent( FTA_BS_BMT_ALLOC_LINK_MCELL_V1,
                              &alloc_link_mcell_undo, /* undo */
                              NULL /* root done */
                              );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent( FTA_BS_RBMT_ALLOC_MCELL_V1,
                              &alloc_rbmt_mcell_undo, /* undo */
                              NULL /* root done */
                              );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent(
                             FTA_BS_BMT_LINK_MCELLS_V1,
                             &link_unlink_mcells_undo, /* undo */
                             NULL /* root done */
                             );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent(
                             FTA_BS_BMT_UNLINK_MCELLS_V1,
                             &link_unlink_mcells_undo, /* undo */
                             NULL /* root done */
                             );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent(
                             FTA_BS_BMT_DEFERRED_MCELL_FREE,
                             NULL, /* undo */
                             &alloc_mcell_undo /* root done */
                             );
    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent_n( FTA_BS_BMT_EXTEND_V1,
                               0, 0,   /* no undo, root-done */
                               &extend_bmt_redo_opx
                               );
    if (sts != EOK) {
        goto abort;
    }

    /* These two agents are now obsolete but must be kept around
     * incase we encounter an old domain that needs to have
     * recovery run on it.
     */

    sts = ftx_register_agent(
                             FTA_BS_XTNT_INSERT_CHAIN_V1,
                             &undo_insert_remove_xtnt_chain,
                             NULL
                             );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_REMOVE_CHAIN_V1,
                             &undo_insert_remove_xtnt_chain,
                             NULL
                             );

    if (sts != EOK) {
        goto abort;
    }

    sts = ftx_register_agent_n( FTA_BS_RBMT_EXTEND_V1,
                               0, 0,   /* no undo, root-done */
                               &extend_rbmt_redo_opx
                               );
    if ( sts == EOK ) {
        return;
    }
abort:
    ADVFS_SAD1( "init_bs_bmt_util_opx: ftx register failure 6", sts );
}  /* end init_bs_bmt_util_opx */


/*
 * This is a continuation and a root done agent.  I.e. it may be invoked
 * either via a root done record or a continuation record.
 *
 * Note that each call to bmt_free_mcell could result in up to two
 * pages being pinned.
 *
 */
void
free_mcell_chains_opx(
                      ftxHT ftxH,       /* in - transaction handle */
                      int opRecSz,      /* in - undo record size */
                      void *opRec       /* in - ptr to undo record */
                      )
{
    int chainCnt = opRecSz/sizeof(mcellPtrRecT);
    mcellPtrRecT fbfm[3];
    int i;
    statusT sts;
    vdIndexT vdIndex;
    bfMCIdT mcellId;
    domainT *dmnP;
    bsMPgT *bmtp;
    bsMCT *mcp;
    rbfPgRefHT pgref;

    vdT *vdp;
    int pinPages, pinRecs;


    if ( chainCnt > 3 ) {
        ADVFS_SAD1("free_mcell_chains_opx: chain cnt(N1) > 3", chainCnt);
    }

    bcopy(opRec, &fbfm[0], opRecSz);
    vdIndex = FETCH_MC_VOLUME(fbfm[0].mcid);
    mcellId = fbfm[0].mcid;

    dmnP =  ftxH.dmnP;
    pinPages = 0;
    pinRecs = 0;

    vdp = VD_HTOP(vdIndex, dmnP);
    FTX_LOCKMUTEX(&vdp->mcell_lk, ftxH);

delchain:
    if ( ++pinPages > FTX_MX_PINP ) {
        goto set_continuation;
    }

    if ((sts = rbf_pinpg(&pgref, (void*)&bmtp, vdp->bmtp, mcellId.page,
        BS_NIL, ftxH, MF_NO_VERIFY)) != EOK) {
        domain_panic(dmnP, "free_mcell_chains_opx: rbf_pinpg failed, return code = %d", sts);
        return;
    }

    if ( bmtp->bmtFreeMcellCnt == 0 ) {
        if ( ++pinPages > FTX_MX_PINP ) {
            goto set_continuation;
        }
        ++pinRecs;
    }

    do {
        /*
         * This frees up mcells on the same page until the pin record
         * limit is exceeded.
         */
        pinRecs += 2;
        if ( pinRecs > FTX_MX_PINR ) {
            goto set_continuation;
        }
        mcp = &bmtp->bsMCA[mcellId.cell];
        fbfm[0].mcid = mcp->mcNextMCId;

        bmt_free_mcell(ftxH, vdp, mcp, mcellId, bmtp, pgref);
nextmcell:
        mcellId = fbfm[0].mcid;

    } while ( (FETCH_MC_PAGE(mcellId) == bmtp->bmtPageId) &&
             (FETCH_MC_VOLUME(fbfm[0].mcid) == vdIndex) );

    if ( FETCH_MC_VOLUME(fbfm[0].mcid) == vdIndex ) {
        /*
         * Still on the same vd, but a different page, so go test the
         * pin page limit and continue if okay.
         */
        goto delchain;
    } else if ( fbfm[0].mcid.volume == bsNilVdIndex ) {
        if ( --chainCnt > 0 ) {
            /*
             * There are more chains left to do.  Shift them down,
             * go back and make the page/vd tests.
             */
            for ( i = 0; i < chainCnt; ++i ) {
                fbfm[i] = fbfm[i+1];
            }
            goto nextmcell;
        } else {
            /*
             * All chains exhausted.  All done.
             */
            return;
        }
    }
    /*
     * There are more chained mcells, but we can't free them
     * in this transaction tree because we may exceed
     * the ftx pin page limit, or we need to change vd,
     * and we cannot lock the vd mcell_lk in arbitrary
     * order.   Submit a continuation record so that we
     * may be called again in a different transaction.
     * This also allows the log to be trimmed.
     */
set_continuation:
    ftx_set_continuation( ftxH, sizeof(mcellPtrRecT)*chainCnt, (void *)fbfm );
}

/*
 * Free a primary mcell and the mcell chain linked from the "nextMCId"
 * field.  If the caller specified "freeXchain" to be TRUE, do two more things:
 *     1. Free the extent chain of mcells, if any.  
 *     2. Remove the primary mcell from the DDL.
 */
void
bmt_free_bf_mcells(
                   vdT* vdp,            /* in - Pointer to vdT */
                   bfMCIdT primMCellId, /* in - Primary mcell ID */
                   ftxHT parentFtxH,    /* in - Parent ftx */
                   int freeXchain       /* in - If TRUE, free extent chain */
                   )
{
    statusT sts;          /* Return status from calls made here */
    ftxHT ftxH;           /* Transaction started here */
    bsMPgT *bmtpg;        /* BMT page ref'ed */
    bsMCT *mcellp;        /* Mcell pointer */
    bfPageRefHT pgRef;    /* Page reference structure */
    bsXtntRT *xtntRecp;   /* Pointer to BSR_XTNTS record in primary mcell */
    mcellPtrRecT fbfm[2]; /* An array of mcell chain heads to free */
    int chainCnt = 1;     /* The number of chains to free */

    /* 
     * We're always going to free the non-extent chain of mcells.
     */
    fbfm[0].mcid = primMCellId;
    MS_SMP_ASSERT(FETCH_MC_VOLUME(primMCellId) == vdp->vdIndex);

    /*
     * Optionally, we will also free the extent chain of mcells.
     * Get the first BSR_XTRA_XTNT mcell in the chain, if any.
     */
    if (freeXchain) {

        sts = bs_refpg(&pgRef,
                       (void*)&bmtpg,
                       vdp->bmtp,
                       primMCellId.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            domain_panic (vdp->dmnP, 
                          "bmt_free_bf_mcells: bs_refpg returned: %d", sts);
            return;
        }

        mcellp = &(bmtpg->bsMCA[primMCellId.cell]);
        if(xtntRecp = bmtr_find (mcellp, BSR_XTNTS, vdp->dmnP)) {
            fbfm[1].mcid = xtntRecp->chainMCId;
            chainCnt = 2;
        }

        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            domain_panic (vdp->dmnP, 
                          "bmt_free_bf_mcells: bs_derefpg returned: %d", sts);
            return;
        }
    }

    /* 
     * Start a transaction.
     */
    sts = FTX_START_N(FTA_BS_BMT_FREE_BF_MCELLS_V1, &ftxH,
                      parentFtxH, vdp->dmnP);
    if ( sts != EOK ) {
            domain_panic(vdp->dmnP, 
                         "bmt_free_bf_mcells: FTX_START_N returned: %d", sts);
        return;
    }

    /*
     * If we're freeing the extent chain of mcells, remove the primary
     * mcell from the deferred delete list.
     */
    if (freeXchain) {
        sts = del_remove_from_del_list( primMCellId, vdp, TRUE, ftxH );
        if ( sts != EOK ) {
            domain_panic(vdp->dmnP, 
              "bmt_free_bf_mcells: del_remove_from_del_list returned: %d", sts);
	    ftx_fail( ftxH );
            return;
        }
    }

    /*
     * Commit the transaction.  The mcell chains will be freed as a
     * root-done. 
     */
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_131);
    ftx_done_urd(ftxH, FTA_BS_BMT_FREE_BF_MCELLS_V1,
                 0, 0, /* undo */
                 sizeof(mcellPtrRecT)*chainCnt, (void *)&fbfm[0]); /*rt dn*/
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_132);
}


/*
 * allocate_link_new_mcell
 *
 * This function allocates and initializes a new mcell and links it
 * to another mcell.
 */

statusT
allocate_link_new_mcell (
                         bfAccessT *bfAccess,  /* in */
                         bfMCIdT oldMcellId,   /* in */
                         ftxHT parentFtx,      /* in */
                         bfMCIdT *newMcellId,  /* out */
                         mcellPoolT poolType,  /* in */
			 uint32_t *alt_linkSegment /* in */
                         )
{

    bfMCIdT mcellId;
    statusT sts;
    vdT *vdp;

    /* sc_select_vd_for_mcell will bump the volume's vdRefCnt if EOK. */
    sts = sc_select_vd_for_mcell( &vdp,
                                  bfAccess->dmnP,
                                  bfAccess->dmnP->scTbl,
                                  bfAccess->reqServices);

    if ( sts != EOK ) {
        return ENO_MORE_MCELLS;
    }

    sts = bmt_alloc_link_mcell( bfAccess->dmnP,
                                vdp->vdIndex,
                                poolType,
                                oldMcellId,
                                parentFtx,
                                &mcellId,
				alt_linkSegment);
    if ( sts == EOK ) {
        MS_SMP_ASSERT(FETCH_MC_VOLUME(mcellId) == vdp->vdIndex);
        *newMcellId = mcellId;
    }

    vd_dec_refcnt(vdp);

    return sts;

} /* end allocate_link_new_mcell */


/* Routines for supporting a separate kernel thread to extend the RBMT.  */

/* bs_init_rbmt_thread
 *
 * Initialize a message queue for communication between alloc_rbmt_mcell()
 * and bs_extend_rbmt_thread().  Then create and start the thread.  This thread
 * acts to extend the RBMT before all the mcells are depleted; this is
 * detected and signalled in alloc_rbmt_mcell().
 */
void
bs_init_rbmt_thread( void )
{
    extern proc_t *Advfsd;
    tid_t tid;
    statusT sts;
    void bs_extend_rbmt_thread();


    /* Create a message queue to send messages to the cleanup thread.  */
    sts = msgq_create( &RbmtExtQH,               /* returned handle to q */
                        RBMT_EXTEND_Q_ENTRIES,   /* initial messages in queue */
                        sizeof( rbmtExtMsgT ),   /* max size of each msg */
                        TRUE,                    /* ok to increase msg q size */
                        current_rad_id()         /* RAD to create on */
                      );
    if (sts != EOK) {
        ADVFS_SAD0("AdvFS rbmt extension thread was not spawned.\n");
    }

    /* Create and start the RBMT extension thread.  */
    if (kdaemon_thread_create((void (*)(void *))bs_extend_rbmt_thread,
                              NULL,
                              Advfsd,
                              &tid,
                              KDAEMON_THREAD_CREATE_DETACHED)) {
        ADVFS_SAD0("AdvFS rbmt extension thread was not spawned.\n");
    }

    return;
}

void
bs_extend_rbmt_thread( void )
{
    rbmtExtMsgT *msg;
    domainT *dmnP = NULL;
    statusT sts;

    while (TRUE) {
        /* Wait for something to do */
        msg = (rbmtExtMsgT *)msgq_recv_msg( RbmtExtQH );

        switch ( msg->msgType ) {
            case EXTEND_RBMT:
                /* only do if the domain is still active */
                sts = bs_domain_access(&dmnP, msg->domainId, FALSE);
                if ( sts == EOK ) {
                    rbmt_extend(dmnP, msg->vdIndex);
                    bs_domain_close(dmnP);
                }
                break;
            default:
                break;
        }
        msgq_free_msg( RbmtExtQH, msg );
    }
    /* NOT REACHED */
}

/*
** The test_xxx routines and TEST_xxx macros return an error if they
** discover invalid data.
** The check_xxx routines are meant to be called after reading
** AdvFS metadata. If invalid data is found, these routines panic
** the domain.
*/

/*
** Test validity of vdi and mcid.
** A vd index of 0 is OK. It means the end of a chain.
** If vdi is 0 don't bother checking the mcid.
** No asserts or domain_panic.
*/
static statusT
test_mcell_ptr(bfMCIdT mcid, bfTagT tag, domainT *dmnP)
{
    statusT sts = EOK;
    vdT *vdp;
    bfAccessT *mdap;

    if (mcid.volume  == 0 ) {
        return( EOK );
    }

    if ( !(vdp = vd_htop_if_valid(mcid.volume, dmnP, TRUE, FALSE)) ) {
        return E_BAD_BMT;
    } 

    if ( BS_BFTAG_RSVD(tag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }

    if ( TEST_META_PAGE(mcid.page, mdap) != EOK ) {
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( TEST_CELL(mcid.cell) != EOK ) {
        RAISE_EXCEPTION( E_BAD_BMT );
    }

HANDLE_EXCEPTION:
    vd_dec_refcnt( vdp );
    return sts;
}

/*
** Test a mcell pointer (ie vdi, page, cell).
** Panic the domain if not OK.
*/
statusT
check_mcell_ptr(bfMCIdT mcid, bfTagT tag, domainT *dmnP)
{
    statusT sts = EOK;


    if ( mcid.volume == 0 ) {
        domain_panic(dmnP, "Invalid (0) volume");
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( test_mcell_ptr(mcid, tag, dmnP) != EOK ) {
        domain_panic(dmnP,
            "Bad mcell pointer: volume %d, mcellId page,cell %ld,%d ",
            mcid.volume, mcid.page, mcid.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

HANDLE_EXCEPTION:
    return sts;
}



/*
** Test the validity of the mcell header.
** Test the set tag and the file tag.
** Test the next mcell pointer.
** Panic the domain and return E_BAD_BMT if an error is found.
** Else return EOK.
** Input: mcp - pointer to mcell to check
** Input: bfap is (R)BMT (metadata file for file of mcp)
*/
statusT
check_mcell_hdr( bsMCT *mcp, bfAccessT *bfap)
{
    statusT sts = EOK;

    if ( test_mcell_ptr(mcp->mcNextMCId, bfap->tag, bfap->dmnP) ) {
        domain_panic(bfap->dmnP,
            "Bad mcell next pointer: volume %d, mcellId page,cell %ld,%d ",
            mcp->mcNextMCId.volume, mcp->mcNextMCId.page, mcp->mcNextMCId.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( !BS_BFTAG_EQL(mcp->mcTag, bfap->tag) ) {
        domain_panic(bfap->dmnP,
            "Incorrect tag (%llx.%x) in mcell. Expected %llx.%x.",
            mcp->mcTag.tag_num, mcp->mcTag.tag_seq, bfap->tag.tag_num, bfap->tag.tag_seq);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

#ifdef NOTYET
    /* This doesn't work for some reason.
     * In a one fileset domain, bfSetTag == 1 but dirTag == 2.
     */
    if ( !BS_BFTAG_EQL(mcp->mcBfSetTag, bfap->bfSetp->dirTag) ) {
        domain_panic(bfap->dmnP, "Incorrect set tag in mcell");
        RAISE_EXCEPTION( E_BAD_BMT );
    }
#endif

HANDLE_EXCEPTION:
    return sts;
}

/*
** Test the validity of a BSR_XTNTS record.
*/
statusT
check_BSR_XTNTS_rec( bsXtntRT *bsXtntRp, bfAccessT *bfap, vdT *vdp)
{
    statusT sts = EOK;
    int i;
    bf_fob_t bfPageSz = bfap->bfPageSz;


    if ( test_mcell_ptr( bsXtntRp->chainMCId,
                         bfap->tag,
                         bfap->dmnP) )
    {
        domain_panic(bfap->dmnP,
            "Invalid chain mcell pointer: volume %d, mcellId page,cell %ld,%d.",
            bsXtntRp->chainMCId.volume,
            bsXtntRp->chainMCId.page, bsXtntRp->chainMCId.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsXtntRp->xCnt > BMT_XTNTS ) {
        domain_panic(bfap->dmnP,
            "Bad first xCnt (%d) in BSR_XTNTS record.",
            bsXtntRp->xCnt);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    return check_xtnts( bsXtntRp->bsXA,
                        bsXtntRp->xCnt,
                        bfap,
                        vdp);

HANDLE_EXCEPTION:
    return sts;
}



/*
** Test the validity of a BSR_XTRA_XTNTS record.
** Test allocVdIndex for validity.
** Test that the this map starts where the last one left off.
** Failure panics the domain and returns E_BAD_BMT, else return EOK.
*/
statusT
check_BSR_XTRA_XTNTS_rec( bsXtraXtntRT *bsXtraXtntRp,
                          bfAccessT *bfap,
                          vdT *vdp,
                          bf_fob_t last_fob)
{
    statusT sts;



    if ( bsXtraXtntRp->xCnt > BMT_XTRA_XTNTS ) {
        domain_panic(bfap->dmnP,
            "Bad xCnt (%d) in BSR_XTRA_XTNTS record.", bsXtraXtntRp->xCnt);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsXtraXtntRp->bsXA[0].bsx_fob_offset!= last_fob) {
        domain_panic(bfap->dmnP,
            "Bad first page (%d) in BSR_XTRA_XTNTS record, expected %d.",
            bsXtraXtntRp->bsXA[0].bsx_fob_offset, last_fob);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    return check_xtnts( bsXtraXtntRp->bsXA, bsXtraXtntRp->xCnt, bfap, vdp);

HANDLE_EXCEPTION:
    return sts;
}

/*
** Check the pages and blocks in the extent array.
** The pages must monotonically increase.
** The disk blocks must be multiples of the page size (16 blocks).
** The disk blocks must be within the volume (bounds checking).
** Panic the domain if errors are found.
*/
static statusT
check_xtnts( bsXtntT bsXA[], uint16_t cnt, bfAccessT *bfap, vdT *vdp)
{
    statusT sts = EOK;
    int i;
    bf_fob_t last_fob;


    for ( i = 0; i < cnt; i++ ) {
        /* Pages are monotonically increasing */
        if ( i == 0 ) {
            last_fob = bsXA[0].bsx_fob_offset;
        } else {
            if ( bsXA[i].bsx_fob_offset<= last_fob) {
                domain_panic(bfap->dmnP,
                 "Bad bsx_fob_offset (%d) in xtnt record at extent %d. Expected file offset block > %d",
                 bsXA[i].bsx_fob_offset, i, last_fob );
                RAISE_EXCEPTION( E_BAD_BMT );
            }

            last_fob = bsXA[i].bsx_fob_offset;
        }

        if ( bsXA[i].bsx_vd_blk == XTNT_TERM || bsXA[i].bsx_vd_blk == COWED_HOLE ) {
            continue;
        }

        if ( bsXA[i].bsx_vd_blk % bfap->bfPageSz ) {
            domain_panic(bfap->dmnP,
             "Bad bsx_vd_blk (%d) in xtnt record %d. bsx_vd_blk must be a multiple of %d",
             bsXA[i].bsx_vd_blk, i, bfap->bfPageSz / ADVFS_FOBS_PER_DEV_BSIZE);
            RAISE_EXCEPTION( E_BAD_BMT );
        }

        if ( bsXA[i].bsx_vd_blk >= vdp->vdSize ) {
            domain_panic(bfap->dmnP,
                "Bad bsx_vd_blk (%d) in xtnt record %d. Max block in volume is %u",
                bsXA[i].bsx_vd_blk, i, vdp->vdSize);
            RAISE_EXCEPTION( E_BAD_BMT );
        }
    }

HANDLE_EXCEPTION:
    return sts;
}

