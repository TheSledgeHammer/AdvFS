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
 *  MegaSafe Storage System
 *
 * Abstract:
 *
 *  Metadata bitfile structure utility routines
 *
 * Date:
 *
 *  Fri May 18 15:06:23 1990
 *
 */
/*
 * HISTORY
 * 
 * 
 * 
 * */

#pragma ident "@(#)$RCSfile: bs_bmt_util.c,v $ $Revision: 1.1.245.14 $ (DEC) $Date: 2006/09/26 12:43:35 $"

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
#include <sys/lock_probe.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_stg.h>
#include <msfs/ms_osf.h>
#include <msfs/ftx_public.h>
#include <msfs/ftx_privates.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/advfs_evm.h>

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
statusT
alloc_page0_mcell (
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
             uint16T newVdIndex,  /* in */
             bfMCIdT newMcellId  /* in */
            );

static
statusT
bmtr_scan_mcells(
    bfMCIdT *mcid,              /* in/out - mcell id (primary/found) */
    vdT **vdp,                  /* in/out - vd (of mcell) pointer */
    void **bsrp,                /* out - pointer to BMT record */
    bfPageRefHT *pgRef,         /* out - pgref of BMT cell's page */
    uint32T *recOffset,         /* out - record's mcell offset */
    u_short *rSize,             /* out - record's size in bytes */
    const int rtype,            /* in - record type to find */
    const bfTagT tag            /* in - bitfile's tag */
    );

static
statusT
bmtr_clone_mcell(
                 bsMRT   *rp,
                 vdT     *srcVdp,  /* Source mcell's disk index */
                 bfMCIdT *srcMCId, /* Source mcell's id */
		 vdT     *destVdp, /* Destination mcell's disk index */
                 ftxHT   parentFtxH,
                 int     first
                 );

/*
 * bmt_upd_mcell_free_list - update page that starts list of free mcells
 */

static uint32T
bmt_upd_mcell_free_list(
                    ftxHT ftxH,        /* in - callers ftx handle */
                    vdT* vdp,          /* in - current disk pointer */
                    int freeMcellPage  /* in - free mcell page */
                    );
statusT
bmtr_update_rec_int(
    bfAccessT* bfap,            /* in - ptr to bf access struct */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,
    uint32T flags               /* in - flags */
    );

static statusT check_xtnts( bsXtntT*, uint16T, bfAccessT*, vdT*);

static statusT test_mcell_ptr( vdIndexT, bfMCIdT, bfTagT, domainT*);

#ifdef ADVFS_MCELL_TRACE
#define MCELL_TRACE_HISTORY 128
/* MCELL_TRACE_NUM_BUCKETS must be a power of 2 */
#define MCELL_TRACE_NUM_BUCKETS 8
typedef struct {
    uint32T       seq;
    uint16T       mod;
    uint16T       ln;
    struct thread *thd;
    uint16T       vdi;
    uint32T       page;
    uint16T       mcidx;
    void          *val;
} mcellTraceElmtT;

typedef struct {
    int             mcTracePtr;
    mcellTraceElmtT mcTraceBuf[MCELL_TRACE_HISTORY];
} mcellTraceT;

mcellTraceT McellTrace[MCELL_TRACE_NUM_BUCKETS];

void
mcell_trace( uint16T vdi, uint32T pg, uint16T mcidx,
             uint16T module, uint16T line, void *value )
{
    register mcellTraceElmtT *mtep;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;
    mcellTraceT *mtp;

    simple_lock(&TraceLock);

    mtp = &McellTrace[pg & (MCELL_TRACE_NUM_BUCKETS - 1)];
    mtp->mcTracePtr = (mtp->mcTracePtr + 1) % MCELL_TRACE_HISTORY;
    mtep = &mtp->mcTraceBuf[mtp->mcTracePtr];
    mtep->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    mtep->seq = TraceSequence++;
    mtep->mod = module;
    mtep->ln = line;
    mtep->page = pg;
    mtep->vdi = vdi;
    mtep->mcidx = mcidx;
    mtep->val = value;

    simple_unlock(&TraceLock);
}
#endif

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
    bsMPgT *dpg,      /* in - BMT page pointer */
    bsPageT pgnum,    /* in - this page number */
    int mdtype,       /* in - metadata type - RBMT or BMT */
    int dmnVersion    /* in - on-disk version number */
    )

{
    int i, numMcells;

    if ( mdtype == BFM_BMT ) {
        numMcells = BSPG_CELLS;
    } else {
        numMcells = BSPG_CELLS - 1;
    }
    dpg->nextfreeMCId.cell = 0;
    dpg->nextfreeMCId.page = pgnum;
    dpg->freeMcellCnt = numMcells;
    dpg->nextFreePg = 0;
    dpg->pageId = pgnum;
    dpg->megaVersion = dmnVersion;

    for (i = 0; i < numMcells; i++) {
        struct bsMR *rp;

        dpg->bsMCA[i].nextMCId.cell = i + 1;
        dpg->bsMCA[i].nextMCId.page = pgnum;
        dpg->bsMCA[i].nextVdIndex = 0;
        dpg->bsMCA[i].linkSegment = 0;
        dpg->bsMCA[i].tag = NilBfTag;
        dpg->bsMCA[i].bfSetTag = NilBfTag;
        rp = (struct bsMR *) dpg->bsMCA[i].bsMR0;
        rp->bCnt = sizeof(struct bsMR);
        rp->type = BSR_NIL;
    }

    dpg->bsMCA[numMcells-1].nextMCId.cell = 0;
    dpg->bsMCA[numMcells-1].nextMCId.page = 0;
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
    u_short type,       /* in - type of record */
    u_short bcnt,       /* in - size of record */
    bsMCT* bmtcp        /* in - BMT cell pointer */
    )
{
    bsMRT* rp;
    char* rtn;
    int freebytes, page_cell;

    /* walk through existing records in cell to find free space */

    rp = (struct bsMR*)bmtcp->bsMR0;

    while (rp->type != BSR_NIL) {
        rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( int ) ));
    }

    /*
     * If over the end of single cell, assume whole page allocation,
     * This code will require first record in whole page to be bigger
     * than BS_USABLE_MCELL_SPACE.
     */
    page_cell = ((bcnt > BS_USABLE_MCELL_SPACE) ||
                 (((char*)rp - (char*)bmtcp->bsMR0) > BS_USABLE_MCELL_SPACE));
    freebytes = (page_cell ?
                 (BSPG_CELLS * sizeof(bsMCT) -
                  ((char*)bmtcp->bsMR0 - (char*)bmtcp))
                 : BSC_R_SZ) -
                ((char*)rp - (char*)bmtcp->bsMR0 + 2 * sizeof(struct bsMR));

    if (freebytes < (int) bcnt) {
        /*
         * There is not enough free space in this mcell for the record.
         */
        return NULL;
    }

    rp->bCnt = bcnt + sizeof(struct bsMR);
    rp->type = type;
    rp->version = 0;
    rtn = (char*)rp + sizeof(struct bsMR);

    /* init terminating NIL record */
    rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( int ) ));
    rp->bCnt = sizeof(struct bsMR);
    rp->type = BSR_NIL;

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
          domainT *dmnP             /* in - domain pointer       */
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
            if ( dmnP == NULL ) {
                /* We may be early in the domain activation process,
                 * so that a domain does not yet exist.  Let the caller
                 * handle this case.
                 */
                return NULL;
            }
            domain_panic(dmnP,"bmtr_find: corrupt bmt record header");
            return NULL;
        }
        rp = (struct bsMR *) ((char *)rp + roundup( rp->bCnt, sizeof( int )));
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
    u_short type,        /* in - type of record */
    u_short bcnt,        /* in - size of record */
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
        rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( int ) ));
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
    rp->version = 0;
    rtn = (char*)rp + sizeof(struct bsMR);

    rp = (struct bsMR*)((char*)rp + roundup( rp->bCnt, sizeof( int )));

    /* init terminating NIL record */
    rbf_pin_record( pgref, rp, sizeof( bsMRT ) );
    rp->bCnt = sizeof(struct bsMR);
    rp->type = BSR_NIL;

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
    u_short rSize,        /* in - size of new record */
    int rType,            /* in - record type */
    ftxHT ftxH            /* in - parent ftx handle */
    )
{
    statusT sts = EOK;
    int newpage = -1;
    int newcell = -1;
    int newVd = FALSE;
    struct bsMPg *bmtp = NULL;
    struct bsMC *mcp = NULL;
    void* rPtr = NULL;
    rbfPgRefHT pgref;
    int ftxStarted = FALSE;
    domainT *dmnP;
    bfAccessT *mdap;
    int file_is_rbmt;

    dmnP = vdp->dmnP;

    /*
     * If the file that needs a new record is the RBMT, we will
     * pin pages in that file.  Otherwise, we will pin pages in
     * the BMT.
     */
    if ((RBMT_THERE(dmnP)) &&
        (BS_BFTAG_RBMT(bfAccp->tag)) &&
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
                     mdap, mcid->page, BS_NIL, ftxH );
    if (sts != EOK) { RAISE_EXCEPTION( sts ); }

    mcp = &bmtp->bsMCA[ mcid->cell ];

    if (!BS_BFTAG_EQL(mcp->tag, bfAccp->tag)) {RAISE_EXCEPTION( EBAD_TAG );}


    /*
     ** Search for an mcell that can hold the new record.  The while
     ** loop will terminate it successfully inserts the record or it
     ** reaches the end of the bitfile's mcell list.
     */

    while ((rPtr = bmtr_assign_rec( rType, rSize, mcp, pgref )) == NULL) {

        /* Go to next chained cell */

        if (mcp->nextVdIndex != vdp->vdIndex) {
            if (mcp->nextVdIndex == 0) {
                /* end of mcell chain */
                break;
            }

            /* Next mcell is on a different disk */

            vdp = VD_HTOP(mcp->nextVdIndex, dmnP);
            newVd = TRUE;
            mdap = (file_is_rbmt ? vdp->rbmtp : vdp->bmtp);
        }

        newcell = mcp->nextMCId.cell;
        newpage = mcp->nextMCId.page;
        *mcid = mcp->nextMCId;

        if ((newpage != bmtp->pageId) || newVd) {
            /* Need to get next BMT or RBMT page */

            rbf_deref_page( pgref, BS_CACHE_IT );

            sts = rbf_pinpg( &pgref, (void *)&bmtp,
                             mdap, newpage, BS_NIL, ftxH );

            if (sts != EOK) {RAISE_EXCEPTION( sts ); }
        }

        mcp = &bmtp->bsMCA[newcell]; /* get pointer to new cell */

        if (!BS_BFTAG_EQL(mcp->tag, bfAccp->tag)) {
            /* something's not right */
            RAISE_EXCEPTION( EBAD_TAG );
        }

        /* Assert that cell 27 for the rbmt never contains anything
         * but BSR_XTRA_XTNTS.  If this occurs it is probably due
         * to a bug where the cell 6(bmt extension cell)
         * mistakenly pointed to cell 27 when it should have had
         * the mcell chain terminated within cell 6.
         *
         * If new code is adding enough records to fill cell 6 and it
         * needs to allocate another cell it cannot fill cell 27 
         * because the extent chain for the rbmt MUST go there.
         */
        if((file_is_rbmt) && (newcell == RBMT_RSVD_CELL)) {
            MS_SMP_ASSERT (rType == BSR_XTRA_XTNTS)
        }
    }

    if (rPtr == NULL) {

        /*
         * Above loop didn't find space for
         * the new record.  So we'll allocate a new mcell
         * and link it into the bitfile's mcell list.
         */

        rbf_deref_page( pgref, BS_CACHE_IT );

        /*
         * NOTE: bmt_alloc_link_mcell() creates a sub-transaction in the
         * current transaction tree.  This ftx sets a special done
         * mode to skip it's undo so that the new mcell will not be
         * returned.
         */

        sts = bmt_alloc_link_mcell( bfAccp->dmnP,
                                    bfAccp->primVdIndex,
                                    BMT_NORMAL_MCELL,
                                    bfAccp->primVdIndex,
                                    bfAccp->primMCId,
                                    ftxH,
                                    mcid );
        if (sts != EOK) {

            /*
             * Most likely the vd that contains the bitfile's primary
             * mcell is out of mcells.  So, next we try a different vd.
             */

            sts = sc_select_vd_for_mcell( &vdp,
                                          bfAccp->dmnP,
                                          dmnP->scTbl,
                                          bfAccp->reqServices,
                                          bfAccp->optServices );
            if (sts != EOK) {
                RAISE_EXCEPTION( ENO_MORE_MCELLS );
            }

            sts = bmt_alloc_link_mcell( bfAccp->dmnP,
                                        vdp->vdIndex,
                                        BMT_NORMAL_MCELL,
                                        bfAccp->primVdIndex,
                                        bfAccp->primMCId,
                                        ftxH,
                                        mcid );
            vd_dec_refcnt(vdp);
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
        } else {
            vdp = VD_HTOP(bfAccp->primVdIndex, dmnP);
        }
        /*
         * Set done mode to skip undo for sub ftxs, in this case,
         * bmt_alloc_link_mcell.
         */
        ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

        mdap = (file_is_rbmt ? vdp->rbmtp : vdp->bmtp);

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid->page, BS_NIL, ftxH );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        mcp = &bmtp->bsMCA[ mcid->cell ];

        /* Assert that cell 27 for the rbmt never contains anything
         * but BSR_XTRA_XTNTS.  If this occurs it is probably due
         * to a bug where the cell 6(bmt extension cell)
         * mistakenly pointed to cell 27 when it should have had
         * the mcell chain terminated within cell 6.
         *
         * If new code is adding enough records to fill cell 6 and it
         * needs to allocate another cell it cannot fill cell 27 
         * because the extent chain for the rbmt MUST go there.
         * If this ever occurs, fix up this code path to not chain
         * to cell 27 but instead allocate a non-cell 27 mcell.
         */
        if((file_is_rbmt) && (mcid->cell == RBMT_RSVD_CELL)) {
            MS_SMP_ASSERT (rType == BSR_XTRA_XTNTS)
        }

        rPtr = bmtr_assign_rec( rType, rSize, mcp, pgref );
        if (rPtr == NULL) {
            ADVFS_SAD0( "bmtr_create_rec: can't assign attr record" );
        }
    }

    rbf_pin_record( pgref, rPtr, rSize );
    bcopy( (char *)bPtr, rPtr, rSize );

    if (rType <= BSR_API_MAX) {
        dmnP->bmtStat.bmtRecWrite[rType]++;

    } else if (rType == BMTR_FS_STAT) {
        dmnP->bmtStat.fStatWrite++;
    }

    return EOK;

HANDLE_EXCEPTION:
    return sts;
}

/*
 * bmtr_scan_mcells
 *
 * This routine will search all chained cells. The search 
 * continues across volumes and the volume used in the
 * last search is returned to the caller
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

static
statusT
bmtr_scan_mcells(
    bfMCIdT *mcid,              /* in/out - mcell id (primary/found) */
    vdT **vdp,                  /* in/out - vd (of mcell) pointer */
    void **bsrp,                /* out - pointer to BMT record */
    bfPageRefHT *pgRef,         /* out - pgref of BMT cell's page */
    uint32T *recOffset,         /* out - record's mcell offset */
    u_short *rSize,             /* out - record's size in bytes */
    const int rtype,            /* in - record type to find */
    const bfTagT tag            /* in - bitfile's tag */
    )
{
    statusT sts = EOK;
    int newpage = -1;
    int newcell = -1;
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

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(tag) ) {
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

        bmtp = get_bmt_pgptr(dmnP);
        *pgRef = NilBfPageRefH;
    } else {
        /* access the metadata bitfile page */

        sts = bmt_refpg( pgRef, (void*)&bmtp, mdap, mcid->page, BS_NIL );
        if (sts != EOK) {
            return( sts );
        }
        pgRefed = TRUE;
    }

    mcp = &bmtp->bsMCA[ mcid->cell ];

    if (!BS_BFTAG_EQL( mcp->tag, tag )) {
        RAISE_EXCEPTION( EBAD_TAG );
    }

    /*
     ** Search for an mcell that contains new record.  The while
     ** loop will terminate if it successfully finds the record or
     ** it reaches the end of the bitfile's mcell list.
     */

    while ((*bsrp = bmtr_find( mcp, rtype, (*vdp)->dmnP)) == NULL) {

        /* Go to next chained cell */

        if (mcp->nextVdIndex != (*vdp)->vdIndex) {
            if (mcp->nextVdIndex == 0) {
                /* end of mcell chain */
                RAISE_EXCEPTION( EBMTR_NOT_FOUND );
            }

            /* Next mcell is on a different disk */

            *vdp = VD_HTOP(mcp->nextVdIndex, dmnP);
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

        newcell = mcp->nextMCId.cell;
        newpage = mcp->nextMCId.page;
        *mcid = mcp->nextMCId;

        if ((newpage != bmtp->pageId) || newVd ) {
            /* Need to get next BMT page */

            if (!pgRefed) {
                /* BMT is not mapped yet.  Can't access beyond pg 0 */
                domain_panic(dmnP, "bmtr_scan_mcells(2): BMT not mapped");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }

            (void) bs_derefpg( *pgRef, BS_CACHE_IT );
            pgRefed = FALSE;

            sts = bmt_refpg(pgRef, (void*)&bmtp, mdap, newpage, BS_NIL);
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
            pgRefed = TRUE;

            newVd = FALSE;
        }

        mcp = &bmtp->bsMCA[newcell]; /* get pointer to new cell */

        if (!BS_BFTAG_EQL( mcp->tag, tag )) {
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
    *recOffset = (uint32T) ((char *)*bsrp - (char *) mcp);

    return EOK;

HANDLE_EXCEPTION:

    if (pgRefed) {
        (void) bs_derefpg( *pgRef, BS_CACHE_IT );
    }

    return sts;
}

/*
 * bmtr_scan
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
    uint32T recOffset;
    u_short recSize;
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
    u_short rType,              /* in - type of record */
    u_short bSize,              /* in - size of buffer */
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
    uint32T recOffset;
    int lkLocked = FALSE;
    u_short recSize;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    bfAccessT *mdap;

    if (bSize > BS_USABLE_MCELL_SPACE) {
        ADVFS_SAD1( "bmtr_get_rec_ptr: invalid request", bSize );
    }

    if ( parentFtxH.hndl == FtxNilFtxH.hndl ) {
        ADVFS_SAD0( "bmtr_get_rec_ptr: missing ftxH" );
    }

    MCELLIST_LOCK_WRITE( &bfap->mcellList_lk )
    lkLocked = TRUE;

    /*
     * find record
     */

    vdp = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
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

    MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
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

        if ((RBMT_THERE(bfap->dmnP)) &&
            (BS_BFTAG_RBMT(bfap->tag)) &&
            (BS_BFTAG_RSVD(bfap->tag))) {
            mdap = vdp->rbmtp;
        }
        else {
            mdap = vdp->bmtp;
        }

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid.page, BS_NIL, parentFtxH );
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
        MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
    }

    return sts;
}

/*
 * bmtr_get_rec_n_lk
 *
 * 'reads' an mcell record into the caller's buffer.
 *
 * If it successfully finds and transfers the record it will
 * return the actual size (in bytes) of the record; this size
 * is <= the caller's buffer size (bSize).  If the routine is
 * unsuccessful it will return zero.
 *
 * If lk == BMTR_LOCK then this routine will also leave the
 * mcell_lk locked.  bmtr_put_rec_n_unlk() or bmtr_update_rec_n_unlk()
 * must be called with lk == BMTR_UNLOCK to unlock the mcell_lk.
 *
 * Note that if an error occurs then 'lk' is ignored and the mcell_lk
 * is unlocked.
 */

statusT
bmtr_get_rec_n_lk(
    bfAccessT *bfap,            /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in/out - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    bmtrLkT lk                  /* in - keep mcell locked? */
    )
{
    vdT *vdp = NULL;
    bfPageRefHT pgref;
    bfMCIdT mcid;
    char *recp = NULL;
    statusT sts;
    uint32T recOffset;
    u_short recSize;
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
        MCELLIST_LOCK_READ( &bfap->mcellList_lk )
    }
    else {
        MCELLIST_LOCK_WRITE( &bfap->mcellList_lk )
    }
    mcellListLocked = TRUE;

    /*
     * find record
     */

    vdp  = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
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
        MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
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
        MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
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
    uint32T recOffset;          /* record's byte offset into mcell */
    u_short vdIndex;            /* disk index of record's mcell */
    u_short beforeImageLen;     /* bytes in 'beforeImage' */
    char beforeImage[BS_USABLE_MCELL_SPACE];  /* before image of mcell rec */
} bmtrPutRecUndoT;

/*
 * bmtr_put_rec_undo_opx
 *
 * Restore's the previous contents of the mcell record described
 * by the undo record (opRec).
 */

opxT bmtr_put_rec_undo_opx;

void bmtr_put_rec_undo_opx(
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
    vdp = VD_HTOP(undoRec.vdIndex, dmnP);

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

    sts = rbf_bfs_open( &bfSetp, undoRec.bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) {
        domain_panic( dmnP,"bmtr_put_rec_undo_opx: rbf_bfs_open failed");
        goto HANDLE_EXCEPTION;
    }

    sts = bs_access(&bfap, undoRec.bfTag, bfSetp, ftxH, 0, NULLMT, &nullvp);
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
    BFSETTBL_LOCK_WRITE( dmnP )

    FTX_LOCKWRITE( &bfap->mcellList_lk, ftxH )

    sts = rbf_pinpg( &pgref, (void*)&bmtp,
                     vdp->bmtp, undoRec.mcid.page, BS_NIL, ftxH );
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
    bcopy( undoRec.beforeImage, rDatap, undoRec.beforeImageLen );

HANDLE_EXCEPTION:
    if (bfap) bs_close(bfap, 0);
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
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    long xid                    /* in - CFS transaction id */
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

    if (bSize > BS_USABLE_MCELL_SPACE) {
        return( EBAD_PARAMS );
    }

    bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return( E_READ_ONLY );
    }

    if ( bfSetp->cloneSetp != NULL ) {
        /*
         * The set has been cloned at least once.  Check to
         * see if we need to do any 'copy on write' processing.
         */
        bs_cow( bfap, COW_NONE, 0, 0, parentFtxH );
    }

    sts =  bmtr_put_rec_n_unlk_int(
               bfap, rType, bPtr, bSize, parentFtxH, lk, xid );
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

statusT
bmtr_put_rec_n_unlk_int(
    bfAccessT* bfap,            /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    long xid                    /* in - CFS transaction id */
    )
{
    vdT *vdp = NULL;
    rbfPgRefHT pgref;
    bfPageRefHT refpgref;
    bfMCIdT mcid;
    char *recp = NULL;
    statusT sts;
    uint32T recOffset;
    ftxHT ftxH;
    int lkLocked = FALSE, ftxStarted = FALSE;
    u_short recSize;
    struct bsMPg* bmtp = NULL;
    struct bsMC* mcp = NULL;
    bmtrPutRecUndoT *undoRecp = NULL;
    uint32T undoRecSize;
    bfAccessT *mdap;

    undoRecp = (bmtrPutRecUndoT *) ms_malloc( sizeof( bmtrPutRecUndoT ));

    if ((lk != BMTR_NO_LOCK) && (parentFtxH.hndl == 0)) {
        ADVFS_SAD0( "bmtr_put_rec_n_unlk_int: caller holds mcellList_lk but has no ftx" );
    }

    sts = FTX_START_XID( FTA_BS_BMT_PUT_REC_V1, &ftxH, parentFtxH,
                       bfap->dmnP, 1, xid );
    if (sts != EOK) {
        ADVFS_SAD1( "bmtr_put_rec_n_unlk_int: ftx start failed", sts );
    }
    ftxStarted = TRUE;

    if (lk == BMTR_NO_LOCK) {
        MCELLIST_LOCK_WRITE( &bfap->mcellList_lk )
        lkLocked = TRUE;
    }

    /*
     * find record
     */

    vdp  = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
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
        if ((RBMT_THERE(bfap->dmnP)) &&
            (BS_BFTAG_RBMT(bfap->tag)) &&
            (BS_BFTAG_RSVD(bfap->tag))) {
            mdap = vdp->rbmtp;
        }
        else {
            mdap = vdp->bmtp;
        }

        sts = rbf_pinpg( &pgref, (void*)&bmtp,
                         mdap, mcid.page, BS_NIL, ftxH );
        if (sts != EOK) { RAISE_EXCEPTION( sts ); }

        mcp = &bmtp->bsMCA[ mcid.cell ];
        recp = (char *)mcp + recOffset;

        (void) bs_bfs_get_set_id( bfap->bfSetp, &undoRecp->bfSetId );
        undoRecp->bfTag = bfap->tag;
        undoRecp->mcid = mcid;
        undoRecp->vdIndex = vdp->vdIndex;
        undoRecp->recOffset = recOffset;
        undoRecp->beforeImageLen = bSize;

        bcopy( recp, undoRecp->beforeImage, bSize ); /* save before image */

        rbf_pin_record( pgref, recp, bSize );
        bcopy( (char *) bPtr, recp, bSize );

        undoRecSize = sizeof (bmtrPutRecUndoT) - (BS_USABLE_MCELL_SPACE - bSize);

        ftx_done_u ( ftxH, FTA_BS_BMT_PUT_REC_V1,
                    undoRecSize, undoRecp);
    } else {
        /*
         * The record doesn't already exist; create it.
	 * Reset mcell context to start of chain
         */

	vdp  = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
        mcid = bfap->primMCId;

        sts = bmtr_create_rec( bfap,
                               &mcid,
                               vdp,
                               bPtr,
                               bSize,
                               rType,
                               ftxH );

        if (sts == EOK) {
            ftx_done_n( ftxH, FTA_BS_BMT_CREATE_REC );
        }
        if (sts != EOK) {
            if ( sts == ENO_MORE_BLKS ) {
                sts = ENO_MORE_MCELLS;
            }
            RAISE_EXCEPTION( sts );
        }
    }

    MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
    lkLocked = FALSE;

    ms_free( undoRecp );
    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (lkLocked) {
        MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
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
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    uint32T flags)              /* in - flags  */
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

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return( E_READ_ONLY );
    }

    if ( bfSetp->cloneSetp != NULL ) {
        /*
         * The set has been cloned at least once.  Check to
         * see if we need to do any 'copy on write' processing.
         */
        bs_cow( bfap, COW_NONE, 0, 0, parentFtxH );
    }

    sts = bmtr_update_rec_int( bfap, rType, bPtr, bSize,
                               parentFtxH, BMTR_NO_LOCK, flags );
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
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
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

    if ((bfSetp->cloneCnt > 0) ||
        (bfSetp->cloneId != BS_BFSET_ORIG)) {
        ADVFS_SAD0( "bmtr_update_rec_n_unlk: must be meta-data bitfile" );
    }

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
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,
    uint32T flags)              /* in - flags */
{
    statusT sts;
    vdT *vdp;
    rbfPgRefHT pgref;
    bfPageRefHT refpgref;
    bfMCIdT mcid;
    char *recp = NULL;
    uint32T recOffset;
    ftxHT ftxH;
    int ftxStarted = FALSE, lkLocked = FALSE;
    u_short recSize;
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
                      parentFtxH, bfap->dmnP, 1 );
            if (sts == EWOULDBLOCK) {
                return( sts );
            }
            ftxStarted = TRUE;
        } else {
            sts = FTX_START_N( FTA_BS_BMT_UPDATE_REC_V1, &ftxH, parentFtxH,
                               bfap->dmnP, 1 );
            if (sts != EOK) {
                ADVFS_SAD1( "bmtr_update_rec_int: FTA_BS_BMT_UPDATE_REC_V1 ftx start failed", sts );
            }
            ftxStarted = TRUE;
        }
    }

    if (lk == BMTR_NO_LOCK) {
        MCELLIST_LOCK_WRITE( &bfap->mcellList_lk )
        lkLocked = TRUE;
    }

    /*
     * find record
     */

    vdp = VD_HTOP(bfap->primVdIndex, bfap->dmnP);
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

    if ((RBMT_THERE(bfap->dmnP)) &&
        (BS_BFTAG_RBMT(bfap->tag)) &&
        (BS_BFTAG_RSVD(bfap->tag))) {
        mdap = vdp->rbmtp;
    }
    else {
        mdap = vdp->bmtp;
    }

    sts = rbf_pinpg( &pgref, (void*)&bmtp,
                     mdap, mcid.page, BS_NIL, ftxH );
    if (sts != EOK) { RAISE_EXCEPTION( sts ); }

    mcp = &bmtp->bsMCA[ mcid.cell ];
    recp = (char *)mcp + recOffset;

    rbf_pin_record( pgref, recp, bSize );
    bcopy( (char *) bPtr, recp, bSize );

    if (ftxStarted) {
        ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
    }

    MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
    lkLocked = FALSE;

    if (rType <= BSR_API_MAX) {
        bfap->dmnP->bmtStat.bmtRecWrite[rType]++;

    } else if (rType == BMTR_FS_STAT) {
        bfap->dmnP->bmtStat.fStatWrite++;
    }

    return EOK;

HANDLE_EXCEPTION:

    if (lkLocked) {
        MCELLIST_UNLOCK( &(bfap->mcellList_lk) )
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
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

    MS_SMP_ASSERT(lock_holder(&vdp->mcell_lk.lock));
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
    uint16T newVdIndex;
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
    allocMcellUndoRecT *undoRec;
    vdT *vd;

    if (undoRecSize != sizeof (allocMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_mcell_undo: bad undo record size" );
    }
    undoRec = (allocMcellUndoRecT *)undoRecAddr;

    dmnP = ftx.dmnP;
    vd = VD_HTOP(undoRec->newVdIndex, dmnP);

    FTX_LOCKWRITE(&(vd->mcell_lk), ftx)

    /*
     * Pin the mcell's page and free the mcell.
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&bmt,
                     vd->bmtp,
                     undoRec->newMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        /* FIX - correct action? */
        domain_panic(dmnP,  "alloc_mcell_undo: rbf_pinpg failed, return code = %d", sts );
        return;
    }
    if (bmt->pageId != undoRec->newMcellId.page ) {
        domain_panic(dmnP, "alloc_mcell_undo: got bmt page %d instead of %d",
                    bmt->pageId, undoRec->newMcellId.page );
        return;
    }

    mcell = &(bmt->bsMCA[undoRec->newMcellId.cell]);

    bmt_free_mcell (
                    ftx,
                    vd,
                    mcell,
                    undoRec->newMcellId,
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
    allocMcellUndoRecT *undoRec;
    vdT *vd;
    bfAccessT *mdap;

    if (undoRecSize != sizeof (allocMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_rbmt_mcell_undo --- bad undo record size" );
    }
    undoRec = (allocMcellUndoRecT *)undoRecAddr;

    dmnP = ftx.dmnP;
    vd = VD_HTOP(undoRec->newVdIndex, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    FTX_LOCKWRITE(&(vd->rbmt_mcell_lk), ftx)

    /*
     * Pin the mcell's page and free the mcell.
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&bmt,
                     mdap,
                     undoRec->newMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_rbmt_mcell_undo: rbf_pinpg failed, return code = %d", sts );
        return;
    }
    if (bmt->pageId != undoRec->newMcellId.page ) {
        domain_panic(dmnP, "alloc_rbmt_mcell_undo: got rbmt page %d instead of %d",
                    bmt->pageId, undoRec->newMcellId.page );
        return;
    }

    mcell = &(bmt->bsMCA[undoRec->newMcellId.cell]);

    rbf_pin_record (
                    pgPin,
                    &mcell->nextMCId,
                    (char*)mcell->bsMR0 - (char*)&mcell->nextMCId
                    );

    mcell->nextMCId = bmt->nextfreeMCId;
    mcell->nextVdIndex = 0;
    mcell->linkSegment = 0;
    mcell->tag = NilBfTag;
    mcell->bfSetTag = NilBfTag;

    rbf_pin_record (
                    pgPin,
                    &(bmt->nextfreeMCId),
                    sizeof (bmt->nextfreeMCId)
                    );
    bmt->nextfreeMCId = undoRec->newMcellId;

    rbf_pin_record (
                    pgPin,
                    &(bmt->freeMcellCnt),
                    sizeof (bmt->freeMcellCnt)
                    );
    bmt->freeMcellCnt++;

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
                 uint16T poolVdIndex,  /* in */
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
                       bfAccess->optServices,
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

    MS_SMP_ASSERT(poolType == BMT_NORMAL_MCELL || poolType == RBMT_MCELL);
    if (poolType == BMT_NORMAL_MCELL) {
        sts = FTX_START_META (FTA_BS_BMT_ALLOC_MCELL_V1, &ftx, parentFtx,
                              dmnP, 1);
        if (sts != EOK) {
            vd_dec_refcnt(vd);
            return sts;
        }
        FTX_LOCKWRITE(&(vd->mcell_lk), ftx)
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
            ftx_fail (ftx);
            return sts;
        }
        init_mcell (pgPin, mcell, bfSetTag, bfTag, linkSeg);
        undoRec.newVdIndex = vd->vdIndex;
        undoRec.newMcellId = mcellId;
        ftx_done_u (ftx, FTA_BS_BMT_ALLOC_MCELL_V1, sizeof (undoRec), &undoRec);
    } else if ( poolType == RBMT_MCELL ) {
        sts = FTX_START_META( FTA_BS_RBMT_ALLOC_MCELL_V1, &ftx, parentFtx,
                              dmnP, 1);
        if ( sts != EOK ) {
            vd_dec_refcnt( vd );
            return sts;
        }

        /*
         * Take the BMT's extent map lock, if we don't already
         * have it, to avoid a possible lock hierarchy violation.  
         */
        if ( !lock_holder(&vd->bmtp->xtntMap_lk.lock) ) {
            FTX_LOCKWRITE(&vd->bmtp->xtntMap_lk, ftx)
        }
        if ( !lock_holder(&vd->rbmt_mcell_lk.lock) ) {
            FTX_LOCKWRITE(&vd->rbmt_mcell_lk, ftx)
        }

        if ( RBMT_THERE(dmnP) ) {
            sts = alloc_rbmt_mcell( vd, ftx, &mcellId, &pgPin, &mcell );
        } else {
            sts = alloc_page0_mcell( vd, ftx, &mcellId, &pgPin, &mcell );
        }

        if ( sts != EOK ) {
            vd_dec_refcnt( vd );
            ftx_fail( ftx );
            return sts;
        }
        init_mcell( pgPin, mcell, bfSetTag, bfTag, linkSeg );
        undoRec.newVdIndex = vd->vdIndex;
        undoRec.newMcellId = mcellId;
        ftx_done_u( ftx, FTA_BS_RBMT_ALLOC_MCELL_V1, sizeof(undoRec), &undoRec);
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

    sts = FTX_START_META (FTA_BS_BMT_DEFERRED_MCELL_FREE, &ftxH, pftxH,
                          vd->dmnP, 1);
    if (sts != EOK) {
        return sts;
    }
    rdRec.newVdIndex = vd->vdIndex;
    rdRec.newMcellId = mcellId;

    /* this ftx has a root done routine: alloc_mcell_undo */
    ftx_done_urd (ftxH, FTA_BS_BMT_DEFERRED_MCELL_FREE, 0, NULL,
                  sizeof(rdRec), &rdRec);
    return EOK;
}


typedef struct allocLinkMcellUndoRec {
    uint16T vdIndex;
    bfMCIdT mcellId;
    uint16T newVdIndex;
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
    allocLinkMcellUndoRecT *undoRec;
    vdT *vd;

    if (undoRecSize != sizeof (allocLinkMcellUndoRecT)) {
        ADVFS_SAD0( "alloc_link_mcell_undo: bad undo record size" );
    }
    undoRec = (allocLinkMcellUndoRecT *)undoRecAddr;

    dmnP = ftx.dmnP;

    /*
     * Pin the old and new mcell's pages, unlink the two mcells and free
     * the new mcell.
     */

    vd = VD_HTOP(undoRec->vdIndex, dmnP);
    sts = rbf_pinpg (
                     &pgPin,
                     (void *)&bmt,
                     vd->bmtp,
                     undoRec->mcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(1) failed, return code = %d", sts );
        return;
    }
    if (bmt->pageId != undoRec->mcellId.page ) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(1) got bmt page %d instead of %d",
                    bmt->pageId, undoRec->mcellId.page );
        return;
    }
    mcell = &(bmt->bsMCA[undoRec->mcellId.cell]);

    vd = VD_HTOP(undoRec->newVdIndex, dmnP);

    FTX_LOCKWRITE(&(vd->mcell_lk), ftx)

    sts = rbf_pinpg (
                     &newPgPin,
                     (void*)&bmt,
                     vd->bmtp,
                     undoRec->newMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(2) failed, return code = %d", sts );
        return;
    }
    if (bmt->pageId != undoRec->newMcellId.page ) {
        domain_panic(dmnP, "alloc_link_mcell_undo: rbf_pinpg(2) got bmt page %d instead of %d",
                    bmt->pageId, undoRec->newMcellId.page );
        return;
    }
    newMcell = &(bmt->bsMCA[undoRec->newMcellId.cell]);

    rbf_pin_record (pgPin, &(mcell->nextVdIndex), sizeof (mcell->nextVdIndex));
    mcell->nextVdIndex = newMcell->nextVdIndex;
    rbf_pin_record (pgPin, &(mcell->nextMCId), sizeof (mcell->nextMCId));
    mcell->nextMCId = newMcell->nextMCId;

    bmt_free_mcell(
                   ftx,
                   vd,
                   newMcell,
                   undoRec->newMcellId,
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
                      uint16T poolVdIndex,  /* in */
                      mcellPoolT poolType,  /* in */
                      uint16T oldVdIndex,  /* in */
                      bfMCIdT oldMcellId,  /* in */
                      ftxHT parentFtx,     /* in */
                      bfMCIdT *retMcellId  /* out */
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
                          dmnP, 2);
    if (sts != EOK) {
        return sts;
    }

    oldVd = VD_HTOP(oldVdIndex, dmnP);
    poolVd = VD_HTOP(poolVdIndex, dmnP);

    /*
     * Acquire the vd mcell list lock prior to pinning any pages to
     * avoid pinblock deadlock.
     */

    if ((poolType == BMT_NORMAL_MCELL) ||
        (poolType == BMT_NORMAL_MCELL_PAGE)) {
        FTX_LOCKWRITE(&(poolVd->mcell_lk), ftx)
    } else {
        ftx_fail (ftx);
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
        ftx_fail (ftx);
        return sts;
    }

    sts = rbf_pinpg (
                     &oldPgPin,
                     (void*)&bmt,
                     oldVd->bmtp,
                     oldMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
        }
    oldMcell = &(bmt->bsMCA[oldMcellId.cell]);

    init_mcell (
                pgPin,
                mcell,
                oldMcell->bfSetTag,
                oldMcell->tag,
                oldMcell->linkSegment + 1
                );
    link_mcells (oldPgPin, oldMcell, pgPin, mcell, poolVdIndex, mcellId);
    undoRec.vdIndex = oldVdIndex;
    undoRec.mcellId = oldMcellId;
    undoRec.newVdIndex = poolVdIndex;
    undoRec.newMcellId = mcellId;
    ftx_done_u (ftx, FTA_BS_BMT_ALLOC_LINK_MCELL_V1,
                sizeof (undoRec), &undoRec);

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

statusT
bmt_extend(
           struct vd* vdp,               /* in - ptr to vd */
           ftxHT parFtxH
           )
{
    struct bfAccess *bmtap;
    uint32T pageCnt = 0;
    int firstNewPg, pg, numNewPgs;
    struct bsMPg *bmtpgp;
    bfPageRefHT pgRef;
    ftxHT ftxH;
    statusT sts;                  /* status                                  */
    bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };
    domainT* dmnP = vdp->dmnP;

    /*-----------------------------------------------------------------------*/

    bmtap = vdp->bmtp;
    firstNewPg = bmtap->nextPage;

    numNewPgs = vdp->bmtXtntPgs;
    if (!(BMT_EXT_REQUEST_OK(numNewPgs, dmnP->ftxLogPgs, vdp->stgCluster))) /* log large enough? */
    {
    /* Here: the requested size of bmtXtntPgs is too large for the current */
    /* log size and could potentially lead to a 'log half full' panic.      */
    /* Use an ext size that is less than 25% of  (log size * SBM_ADVFS_PGS(x)) */
    /* and send a message to the console */

        numNewPgs = BMT_EXT_REQUEST_DEFAULT(dmnP->ftxLogPgs, vdp->stgCluster);
        aprintf("\nBMT ext size (%d) of %s in domain %s is too large\n",
                vdp->bmtXtntPgs, vdp->vdName, dmnP->domainName);
        aprintf("using new size: %d (25%% of (log size * %d))\n",
                numNewPgs, SBM_ADVFS_PGS(vdp->stgCluster));
    }


#ifdef ADVFS_RBMT_STRESS
    numNewPgs=1; 
#endif

    sts = FTX_START_META( FTA_BS_BMT_EXTEND_V1, &ftxH, parFtxH, vdp->dmnP, 1 );
    if ( sts != EOK ) {
        ADVFS_SAD1( "bmt_extend: ftx start failed", sts );
    }

    sts = stg_add_stg( ftxH, bmtap, firstNewPg, numNewPgs, FALSE, &pageCnt);
    if (sts != EOK) {
        if(sts == ENO_MORE_MCELLS) {
            vdp->nextMcellPg = NO_MORE_MCELLS;
        }
        ftx_fail( ftxH );
        return sts;
    }

    /*
     * TODO: this currently depends on pre-initialization of bmt
     * pages, and it's possible the new extents could be written to
     * the extent map before these pages are formatted.  Actually,
     * that shouldn't cause a problem because hiwrite won't be updated
     * yet, but this may be better done by formatting as hiwrite is
     * exceeded and writing through one page at a time then.  Of
     * course, that would mean hiwrite gets forced for each page.
     * Figure out the best thing to do here later...
     */

    for (pg = firstNewPg; pg < (firstNewPg + pageCnt); pg++) {
        sts = bs_pinpg( &pgRef, (void *) &bmtpgp, vdp->bmtp, pg, BS_OVERWRITE );
        if (sts != EOK) {
            ftx_fail(ftxH);
            return sts;
        }

        bmt_init_page( bmtpgp, pg, BFM_BMT, dmnP->dmnVersion );

        if (pg < (firstNewPg + pageCnt - 1)) {
            /*
             * Continue free Mcell list on to next page (if it's not the
             * last page we're adding to the BMT).
             */
            bmtpgp->nextFreePg = pg + 1;
        } else if ( RBMT_THERE(dmnP) ) {
            bmtpgp->nextFreePg = -1;
        } else {
            bmtpgp->nextFreePg = 0;
        }

        sts = bs_unpinpg( pgRef, logNilRecord, writethru );
        if (sts != EOK) {
            ftx_fail(ftxH);
            return sts;
        }
    }

    (void)bmt_upd_mcell_free_list( ftxH, vdp, firstNewPg );

    /*
     * Set done mode to skip undo for sub ftxs, in this case,
     * the rbf_add_stg, so it will not be undone after the
     * current ftx is done.  This is because the new page will
     * be initialized and the global free list updated, so the
     * new page of tags will become visible after either this
     * parent ftx completes, or the system restarts.
     */

    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

    ftx_done_urdr( ftxH, FTA_BS_BMT_EXTEND_V1, 0,0,0,0,
                  sizeof(vdIndexT), &vdp->vdIndex );

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
    vdIndexT vdIndex = *(vdIndexT*)opRec;
    domainT *dmnP = ftxH.dmnP;
    vdT* vdp;
    statusT sts;

    vdp = VD_HTOP(vdIndex, dmnP);

    sts = bs_map_bf(vdp->bmtp, BS_REMAP, NULL);
    if ( sts != EOK ) {
        ADVFS_SAD1( "extend_bmt_redo_opx: bs_map_bf failed", sts );
    }
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
    int page;
    domainT* dmnP = vd->dmnP;
    int bad_pages_found = 0;

    MS_SMP_ASSERT(lock_holder(&vd->mcell_lk.lock));

start:

    /* extend BMT if there are no more free mcells */

    page = vd->nextMcellPg;

start2:

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

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vd->bmtp, page, BS_NIL, parentFtx);
    if ( sts != EOK ) {
        return sts;
    }

    if ( bmtpgp->pageId != page ) {
        domain_panic(dmnP, "alloc_mcell: read page %d instead of %d, ",
                    bmtpgp->pageId, page );
        return(E_DOMAIN_PANIC);
    }

    /*
     * search for full page of cells if requested
     */
    if ((poolType == BMT_NORMAL_MCELL_PAGE) &&
        (bmtpgp->freeMcellCnt != BSPG_CELLS)) {

      page = bmtpgp->nextFreePg;
      rbf_deref_page(pgref, BS_CACHE_IT);
      goto start2;
    }

    rbf_pin_record(pgref,
                   &bmtpgp->nextfreeMCId,
                   (char*)&bmtpgp->bsMCA[0] - (char*)&bmtpgp->nextfreeMCId);

    /* check to make sure there is a free mcell on this page */
    if ( bmtpgp->freeMcellCnt == 0) {
#ifdef ADVFS_SMP_ASSERT
        aprintf("ADVFS: alloc_mcell: next free page has no free mcells\n");
        aprintf("ADVFS: alloc_mcell: domain = %s, vol = %s, page = %d\n",
                dmnP->domainName, vd->vdName, page);
        ADVFS_SAD0( "alloc_mcell: bad mcell free list");
#endif
        goto cleanup_corruption;
    }

    mcid = bmtpgp->nextfreeMCId;

    /*
     * if allocating page of cells, return top cell
     */
    if (poolType == BMT_NORMAL_MCELL_PAGE) {
      mcid.cell = 0;
    }

    mcp = &bmtpgp->bsMCA[mcid.cell];

    if (!BS_BFTAG_EQL( mcp->tag, NilBfTag ) &&
        !BS_BFTAG_EQL( mcp->bfSetTag, NilBfTag )) {
#ifdef ADVFS_DEBUG
        ADVFS_SAD2( "alloc_mcell: mcell not really free", mcid.cell, mcid.page );
#endif
        aprintf( "ADVFS error: alloc_mcell: mcell (%d.%d) not really free\n", mcid.cell, mcid.page );
        aprintf( "ADVFS cont : alloc_mcell: domain = %s, vol = %s, page = %d\n", dmnP->domainName, vd->vdName, page );
        aprintf( "ADVFS cont : alloc_mcell: tag = 0x%08x.%04x, setTag = 0x%08x.%04x\n",
                mcp->tag.num, mcp->tag.seq, mcp->bfSetTag.num, mcp->bfSetTag.seq );
        goto cleanup_corruption;
    }

    /*
     * mark cell as in use
     */
    rbf_pin_record(pgref, &mcp->tag, sizeof(bfTagT));
    mcp->tag.num = -1;

    bmtpgp->nextfreeMCId = mcp->nextMCId;
    bmtpgp->freeMcellCnt--;

    if ((poolType == BMT_NORMAL_MCELL_PAGE) ||
        ( bmtpgp->freeMcellCnt == 0 )) {

        /*
         * next free mcell is on another page.  note no more here.
         * update the global next free mcell list head.
         */

        if (( bmtpgp->freeMcellCnt == 0 ) &&
            ((bmtpgp->nextfreeMCId.cell != 0) ||
             (bmtpgp->nextfreeMCId.page != 0))) {
#ifdef ADVFS_DEBUG
            ADVFS_SAD2( "alloc_mcell: freeMCellCnt == 0, nextFreeMCId == (N1.N2)",
                        bmtpgp->nextfreeMCId.cell, bmtpgp->nextfreeMCId.page );
#endif
            aprintf( "ADVFS error: alloc_mcell: invalid free list\n" );
            aprintf( "ADVFS cont : alloc_mcell: domain = %s, vol = %s, page = %d\n", dmnP->domainName, vd->vdName, page );
            aprintf( "ADVFS cont : alloc_mcell: freeMCellCnt = %d, nextFreeMCId == (%d.%d)",
                    bmtpgp->freeMcellCnt,
                    bmtpgp->nextfreeMCId.cell, bmtpgp->nextfreeMCId.page );
            goto cleanup_corruption;
        }

        (void)bmt_upd_mcell_free_list( parentFtx, vd, bmtpgp->nextFreePg );

        if ( RBMT_THERE(dmnP) ) {
            bmtpgp->nextFreePg = -1;
        } else {
            bmtpgp->nextFreePg = 0;
        }

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
    uint32T new_head_of_free_list;

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
            ms_uaprintf("Domain = '%s', volume = '%s', BMT page = %d\n", dmnP->domainName, vd->vdName, page );
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
                                 (bmtpgp->nextFreePg != page)) ? 
                                     bmtpgp->nextFreePg : 
                                     (RBMT_THERE(dmnP) ? EXTEND_BMT : 0);
    
        (void)bmt_upd_mcell_free_list( parentFtx, vd, new_head_of_free_list );
    
        bmtpgp->freeMcellCnt = 0;
        bmtpgp->nextfreeMCId = bsNilMCId;
        if ( RBMT_THERE(dmnP) ) {
            bmtpgp->nextFreePg = -1;
        } else {
            bmtpgp->nextFreePg = 0;
        }
        goto start;
    }

    /* NOT REACHED */

}  /* end alloc_mcell */

/*
 * rbmt_extend
 *
 * Add one page to the Reserved Metadata Table. This must be called 
 * in such a manner as to allow a root transaction to be started.  
 * Currently, this is called from bs_extend_rbmt_thread() only.
 *
 * This function waits for message sender transaction to finish, as 
 * well as all other transactions, then it gets a slot because of the 
 * exclusive transaction request made.  The exclusive transaction 
 * cannot start until the sending transaction finishes.  The exclusive
 * transaction also avoids potential deadlocks caused by timing/load 
 * variations.  This approach also avoids the problem of more than 
 * FTX_MX_NEST (curretly 11 - 8/1/03) levels in a  transaction.  It is 
 * possible to add two levels to the transaction table structures and 
 * then to code this as a sub transaction of of the message sending 
 * function. (It  appears that the exclusive nature was a precaution 
 * not a necessity. -- wl - 8/1/03) 
 */

statusT
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
    uint32T pageCnt, newPg;
    bfPageRefHT newPgPin;
    rbfPgRefHT  curPgPin;
    bfMCIdT mcid;
    bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };
    struct vd* vdp = VD_HTOP(vdIndex, dmnP);
    bfAccessT *rbmtap = vdp->rbmtp;

    /* 
     * This starts an exclusive, root transaction.  The transaction
     * sending the message to the bs_extend_rbmt_thread must complete
     * before this transaction will start.
     */

    sts = FTX_START_RSVD_META( FTA_BS_RBMT_EXTEND_V1,     /* agent */
                               &ftxH,                     /* new handle */
                               FtxNilFtxH,                /* root tx */
                               vdp->dmnP,                 /* domain pointer */
                               0 );                       /* pages rsvd */

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

    FTX_LOCKWRITE(&(rbmtap->mcellList_lk), ftxH);
    FTX_LOCKWRITE(&(rbmtap->xtntMap_lk), ftxH);
    FTX_LOCKWRITE(&(vdp->rbmt_mcell_lk), ftxH);

    /*
     * Pin current RBMT page  (Would be clearer if done with bs_refpg, followed 
     * by bs_derefpg. But then I'd need to retest this.)
     */   
    sts = rbf_pinpg (&curPgPin,
                     (void*)&curRbmtPg,
                     rbmtap,
                     vdp->lastRbmtPg,
                     BS_NIL,
                     ftxH
                     );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    newPg = rbmtap->nextPage;
    mcid.page = vdp->lastRbmtPg;
    mcid.cell = RBMT_RSVD_CELL;

    sts = stg_add_rbmt_stg( ftxH, rbmtap, newPg, 1, mcid, &pageCnt);

    if ((sts == EOK) && (pageCnt != 1)) {
        domain_panic(vdp->dmnP,
                     "rbmt_extend: invalid pageCnt from stg_add_rbmt_stg" );
        RAISE_EXCEPTION( sts );
    }
    else if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* pin the new page and initialize it. */ 
    
    /* The new page is written with careful ordering.  The bs_unpinpg 
     * synchronously writes the page through to the disk before the 
     * ftx_done is issued.  If the transaction is recovered before the
     * metadata is written to the disk, this will be redone on recovery 
     * by the image redo.
     */
    sts = bs_pinpg( &newPgPin, (void *)&newRbmtPg, rbmtap,
                                         newPg, BS_OVERWRITE);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    bmt_init_page( newRbmtPg, newPg, BFM_RBMT, dmnP->dmnVersion );
     
    /* Now initialize 27th mcell which is not initialized by
     * bmt_init_page for rbmt.
     */

    newRbmtPg->bsMCA[RBMT_RSVD_CELL].linkSegment =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].linkSegment + 1;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].tag =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].tag;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].bfSetTag =
        curRbmtPg->bsMCA[RBMT_RSVD_CELL].bfSetTag;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].nextVdIndex = bsNilVdIndex;
    newRbmtPg->bsMCA[RBMT_RSVD_CELL].nextMCId = bsNilMCId;

    /* We need to initialize 27th mcell records too, which are 
     * not initialized by bmt_init_page for rbmt.
     */

    rp = (struct bsMR *) newRbmtPg->bsMCA[RBMT_RSVD_CELL].bsMR0;
    rp->bCnt = sizeof(struct bsMR);
    rp->type = BSR_NIL;

    /* This page gets flushed to disk at this point */
    sts = bs_unpinpg( newPgPin, logNilRecord, writethru );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if (vdp->nextMcellPg == NO_MORE_MCELLS)
        vdp->nextMcellPg = EXTEND_BMT;

    /* Set new page and clear flag; these flds protected by rbmt_mcell_lk */
    vdp->lastRbmtPg = newPg;
    vdp->rbmtFlags &= ~RBMT_EXTENSION_IN_PROGRESS;

    /* This tx has only a redo operation; force log record to disk when
     * we commit the transaction.
     */

    ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );
    ftx_done_urdr( ftxH, FTA_BS_RBMT_EXTEND_V1, 0, 0, 0, 0, sizeof(vdIndexT),
                   &vdp->vdIndex );

    return EOK;

HANDLE_EXCEPTION:

    ftx_fail( ftxH );
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
    vdIndexT vdIndex = *(vdIndexT*)opRec;
    domainT *dmnP = ftxH.dmnP;
    vdT* vdp;
    statusT sts;

    vdp = VD_HTOP(vdIndex, dmnP);

    sts = bs_map_bf( vdp->rbmtp, BS_REMAP, NULL);
    if ( sts != EOK ) {
        domain_panic(vdp->dmnP, "extend_rbmt_redo_opx: bs_map_bf failed");
    }

    return;
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

    MS_SMP_ASSERT(lock_holder(&vd->rbmt_mcell_lk.lock));

    /*
     * Pin current RBMT page
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&rbmt,
                     vd->rbmtp,
                     vd->lastRbmtPg,
                     BS_NIL,
                     parentFtx
                     );
    if (sts != EOK) {
        return sts;
    }

    /*
     * Check the # mcells left on this page;  if running low, send a
     * message to the extend_rbmt_thread to add a new page.  The first
     * leg of this check is the 'normal' code path.
     */
    if ( (rbmt->nextfreeMCId.cell == (RBMT_RSVD_CELL - 1)) &&
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
    } else if ( ( rbmt->nextfreeMCId.cell == 0 ) &&
                ( rbmt->nextfreeMCId.page == 0 ) ) {
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

    mcellId = rbmt->nextfreeMCId;
    mcell = &(rbmt->bsMCA[mcellId.cell]);
    rbf_pin_record (
                    pgPin,
                    &(rbmt->nextfreeMCId),
                    sizeof (rbmt->nextfreeMCId)
                    );
    rbmt->nextfreeMCId = mcell->nextMCId;

    rbf_pin_record (
                    pgPin,
                    &(rbmt->freeMcellCnt),
                    sizeof (rbmt->freeMcellCnt)
                    );
    rbmt->freeMcellCnt--;

    *retMcellId = mcellId;
    *retPgPin = pgPin;
    *retMcell = mcell;

    return EOK;

} /* alloc_rbmt_mcell */

/*
 * alloc_page0_mcell
 *
 * This routine allocates an mcell from the page 0 mcell pool.
 * The mcells of all the reserved files (bmt, log, sbm), free
 * and busy, are located in this pool.
 *
 * NOTE: It is assumed that the caller of this routine holds the
 * rbmt_mcell_lk, which serializes access to the page 0 mcell pool.
 */

static
statusT
alloc_page0_mcell (
                   vdT* vd,  /* in */
                   ftxHT parentFtx,  /* in */
                   bfMCIdT *retMcellId,  /* out */
                   rbfPgRefHT *retPgPin,  /* out */
                   bsMCT **retMcell  /* out */
                   )

{

    bsMPgT *bmt;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;

    MS_SMP_ASSERT(lock_holder(&vd->rbmt_mcell_lk.lock));
    /*
     * Pinning page 0 serves a couple of purposes.  It gets us the bmt
     * address and it gets us a reference to the new mcell's page.
     */

    sts = rbf_pinpg (
                     &pgPin,
                     (void*)&bmt,
                     vd->bmtp,
                     0,
                     BS_NIL,
                     parentFtx
                     );
    if (sts != EOK) {
        return sts;
    }

    if (bmt->nextfreeMCId.cell == 0) {
        /*
         * Out of reserved mcells.
         */
        return ENO_MORE_MCELLS;
    }

    mcellId = bmt->nextfreeMCId;
    mcell = &(bmt->bsMCA[mcellId.cell]);
    rbf_pin_record (
                    pgPin,
                    &(bmt->nextfreeMCId),
                    sizeof (bmt->nextfreeMCId)
                    );
    bmt->nextfreeMCId = mcell->nextMCId;

    rbf_pin_record (
                    pgPin,
                    &(bmt->freeMcellCnt),
                    sizeof (bmt->freeMcellCnt)
                    );
    bmt->freeMcellCnt--;

    *retMcellId = mcellId;
    *retPgPin = pgPin;
    *retMcell = mcell;

    return EOK;

}  /* end alloc_page0_mcell */

/*
 * bmt_find_mcell
 *
 * This function searches the mcell list and returns the mcell that points
 * to the mcell specified by the caller.  The search does not include the
 * starting mcell.
 *
 * This function assumes that the caller has shared access to the mcell
 * list.
 */

statusT
bmt_find_mcell (
                domainT *dmnP,  /* in */
                uint16T bfSearchVdIndex,  /* in */
                bfMCIdT bfSearchMCId,  /* in */
                uint16T bfStartVdIndex,  /* in */
                bfMCIdT bfStartMCId,  /* in */
                uint16T *bfPrevVdIndex,  /* out */
                bfMCIdT *bfPrevMCId  /* out */
                )

{

    bsMPgT *bmtp;
    bfMCIdT mcellId;
    bsMCT *mcellp;
    bfMCIdT nextMcellId;
    uint16T nextVdIndex;
    bfPageRefHT pgRef;
    statusT sts;
    uint16T vdIndex;
    vdT *vdp;

    vdIndex = bfStartVdIndex;
    mcellId = bfStartMCId;

    while ( vdIndex ) {

        vdp = VD_HTOP(vdIndex, dmnP);

        sts = bmt_refpg( &pgRef, (void*)&bmtp, vdp->bmtp, mcellId.page, BS_NIL);
        if (sts != EOK) {
            return sts;
        }
        mcellp = &(bmtp->bsMCA[mcellId.cell]);

        nextVdIndex = mcellp->nextVdIndex;
        nextMcellId = mcellp->nextMCId;

        sts = bs_derefpg (pgRef, BS_NIL);
        if (sts != EOK) {
            return sts;
        }

        if ((nextVdIndex == bfSearchVdIndex) &&
            (nextMcellId.page == bfSearchMCId.page) &&
            (nextMcellId.cell == bfSearchMCId.cell)) {
            *bfPrevVdIndex = vdIndex;
            *bfPrevMCId = mcellId;
            return EOK;
        }

        vdIndex = nextVdIndex;
        mcellId = nextMcellId;
    }  /* end while */

    return ENO_MORE_MCELLS;

}  /* end bmt_find_mcell */

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
                    &(mcell->nextMCId),
                    (char *)&(mcell->bsMR0[0]) - (char *)&(mcell->nextMCId) +
                        sizeof (bsMRT)
                    );
    mcell->nextMCId    = bsNilMCId;
    mcell->nextVdIndex = bsNilVdIndex;
    mcell->linkSegment = linkSeg;
    mcell->tag         = bfTag;
    mcell->bfSetTag    = bfSetTag;

    rec = (bsMRT *)&(mcell->bsMR0[0]);
    rec->bCnt = sizeof (bsMRT);
    rec->type = BSR_NIL;

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
             uint16T newVdIndex,  /* in */
             bfMCIdT newMcellId  /* in */
            )

{

    rbf_pin_record (
                    newPgPin,
                    &(newMcell->nextVdIndex),
                    sizeof (newMcell->nextVdIndex)
                    );
    newMcell->nextVdIndex = oldMcell->nextVdIndex;
    rbf_pin_record (
                    newPgPin,
                    &(newMcell->nextMCId),
                    sizeof (newMcell->nextMCId)
                    );
    newMcell->nextMCId = oldMcell->nextMCId;

    rbf_pin_record (
                    oldPgPin,
                    &(oldMcell->nextVdIndex),
                    sizeof (oldMcell->nextVdIndex)
                    );
    oldMcell->nextVdIndex = newVdIndex;
    rbf_pin_record (
                    oldPgPin,
                    &(oldMcell->nextMCId),
                    sizeof (oldMcell->nextMCId)
                    );
    oldMcell->nextMCId = newMcellId;

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
 */

static uint32T
bmt_upd_mcell_free_list(
    ftxHT ftxH,         /* in - ftx handle */
    vdT* vdp,           /* in - pointer to current disk */
    int freeMcellPage   /* in - free mcell page */
    )
{
    struct bsMPg* bmtpgp;
    rbfPgRefHT pgRef;
    bsMcellFreeListT *mcellFreeListp;
    bsMCT* mcp;
    statusT sts;
    uint32T oldHeadPg;
    uint32T freeListPg;
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

    if ( RBMT_THERE(dmnP) ) {
        freeListPg = 0;
    } else {
        freeListPg = 1;
    }

    /* update the free page pointer on bmt page 1/0 */

    sts = rbf_pinpg( &pgRef, (void*)&bmtpgp, vdp->bmtp, freeListPg, BS_NIL, ftxH );
    if ( sts != EOK ) {
        domain_panic( dmnP, "bmt_upd_mcell_free_list: rbf_pinpg failed, return code = %d", sts );
        return(E_DOMAIN_PANIC);
    }

    if ( bmtpgp->pageId != freeListPg ) {
        domain_panic(dmnP,  "bmt_upd_mcell_free_list: got page %d instead of free list head (page %d)",
                    bmtpgp->pageId, freeListPg );
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

    if ( ( !RBMT_THERE(dmnP) ) && (freeMcellPage == 0) ) {
        vdp->nextMcellPg = -1;
    } else {
        vdp->nextMcellPg = freeMcellPage;
    }

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
    int freeListPg;
    bfPageRefHT  pgRef;
    bsMcellFreeListT *mcellFreeListp;
    bsMCT* mcp;
    statusT sts;

    if ( RBMT_THERE(dmnP) ) {
        freeListPg = 0;
    } else {
        freeListPg = 1;
    }

    if (vdp->bmtp->nextPage <= freeListPg) {
        /*
         * Since the free list head is maintained on page 1 of the BMT
         * we must assume that the list is empty if page 1 doesn't exist.
         */
        vdp->nextMcellPg = 0;

    } else {

        /*
         * Get the mcell free list head from page 1, cell 0 of the BMT.
         */

        sts = bmt_refpg( &pgRef, (void*)&bmtpgp, vdp->bmtp, freeListPg, BS_NIL);
        if ( sts != EOK ) {
            domain_panic(vdp->dmnP, 
                       "bmt_set_mcell_free_list: refpg failure.");
            return E_DOMAIN_PANIC;
        }

        if ( bmtpgp->pageId != freeListPg ) {
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

        if ((bmtpgp->megaVersion < FIRST_RBMT_VERSION) &&
            (mcellFreeListp->headPg == 0)) {
            vdp->nextMcellPg = -1;
        } else {
            vdp->nextMcellPg = mcellFreeListp->headPg;
        }

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
    vdIndexT prevVdIndex;
    bfMCIdT prevMCId;
    vdIndexT prevNextVdIndex;
    bfMCIdT prevNextMCId;
    vdIndexT lastVdIndex;
    bfMCIdT lastMCId;
    vdIndexT lastNextVdIndex;
    bfMCIdT lastNextMCId;
} linkUnlinkMcellsUndoRecT;

typedef struct insertRemoveXtntChainUndoRec
{
    vdIndexT primVdIndex;
    bfMCIdT primMCId;
    vdIndexT primChainVdIndex;
    bfMCIdT primChainMCId;
    vdIndexT lastVdIndex;
    bfMCIdT lastMCId;
    vdIndexT lastNextVdIndex;
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
 * It is currently 12/18/98. At some future date (say 12/18/03 ?)
 * this routine can be removed.
 *
 */

void
undo_insert_remove_xtnt_chain (
                               ftxHT ftx,
                               int undoRecSize,
                               void *undoRecAddr
                               )
{
    insertRemoveXtntChainUndoRecT *oldundoRec;
    linkUnlinkMcellsUndoRecT newundoRec;

    oldundoRec = (insertRemoveXtntChainUndoRecT *)undoRecAddr;

    newundoRec.bfTag            = oldundoRec->bfTag;
    newundoRec.prevVdIndex      = oldundoRec->primVdIndex;
    newundoRec.prevMCId         = oldundoRec->primMCId;
    newundoRec.prevNextVdIndex  = oldundoRec->primChainVdIndex;
    newundoRec.prevNextMCId     = oldundoRec->primChainMCId;
    newundoRec.lastVdIndex      = oldundoRec->lastVdIndex;
    newundoRec.lastMCId         = oldundoRec->lastMCId;
    newundoRec.lastNextVdIndex  = oldundoRec->lastNextVdIndex;
    newundoRec.lastNextMCId     = oldundoRec->lastNextMCId;

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
    rbfPgRefHT pgPin;
    statusT sts;
    linkUnlinkMcellsUndoRecT *undoRec;
    vdT *vd;
    bsXtntRT *xp = NULL;
    bfAccessT *mdap, *bfap;
    bsInMemXtntT *xtnts;
    int v4bmt_or_rbmt;
    int this_is_v3bmt;
    int keep_lock = 0;

    undoRec = (linkUnlinkMcellsUndoRecT *)undoRecAddr;

    dmnP = ftx.dmnP;

    /*
     * Restore the previous mcell's next mcell pointer.
     */

    vd = VD_HTOP(undoRec->prevVdIndex, dmnP);

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(undoRec->bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }
    sts = rbf_pinpg (
                     &pgPin,
                     (void *)&bmt,
                     mdap,
                     undoRec->prevMCId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        domain_panic(ftx.dmnP, "link_unlink_mcells_undo: rbf_pinpg(1) failed, return code = %d", sts );
        return;
    }
    mcell = &(bmt->bsMCA[undoRec->prevMCId.cell]);

    /*
     * See if there is a BSR_XTNTS record in the previous mcell.
     * If so, we're going to connect the chain via the
     * bsXtntRT.chain* fields.  Otherwise, we're going to use
     * the bsMCT.next* fields in the mcell header.
     */
    xp = bmtr_find(mcell, BSR_XTNTS, dmnP);

    if (xp) {
        rbf_pin_record (
                        pgPin,
                        &(xp->chainVdIndex),
                        sizeof(xp->chainVdIndex)
                        );
        rbf_pin_record (
                        pgPin,
                        &(xp->chainMCId),
                        sizeof(xp->chainMCId)
                        );
        xp->chainVdIndex = undoRec->prevNextVdIndex;
        xp->chainMCId = undoRec->prevNextMCId;
    }
    else {
        rbf_pin_record (
                        pgPin,
                        &(mcell->nextVdIndex),
                        sizeof(mcell->nextVdIndex)
                        );
        rbf_pin_record (
                        pgPin,
                        &(mcell->nextMCId),
                        sizeof(mcell->nextMCId)
                        );
        mcell->nextVdIndex = undoRec->prevNextVdIndex;
        mcell->nextMCId = undoRec->prevNextMCId;
    }

    /*
     * Restore the last mcell's next mcell pointer.
     */

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(undoRec->bfTag) ) {
        mdap = VD_HTOP(undoRec->lastVdIndex, dmnP)->rbmtp;
    } else {
        mdap = VD_HTOP(undoRec->lastVdIndex, dmnP)->bmtp;
    }

    sts = rbf_pinpg (
                     &pgPin,
                     (void *)&bmt,
                     mdap,
                     undoRec->lastMCId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        domain_panic(ftx.dmnP,  "link_unlink_mcells_undo: rbf_pinpg(2) failed, return code = %d", sts );
        return;
    }
    mcell = &(bmt->bsMCA[undoRec->lastMCId.cell]);

    rbf_pin_record (
                    pgPin,
                    &(mcell->nextVdIndex),
                    sizeof(mcell->nextVdIndex)
                    );
    rbf_pin_record (
                    pgPin,
                    &(mcell->nextMCId),
                    sizeof(mcell->nextMCId)
                    );
    mcell->nextVdIndex = undoRec->lastNextVdIndex;
    mcell->nextMCId = undoRec->lastNextMCId;

    v4bmt_or_rbmt = RBMT_THERE(dmnP) &&
                    BS_BFTAG_RSVD(undoRec->bfTag) &&
                    (BS_BFTAG_RBMT(undoRec->bfTag) || 
                     BS_IS_TAG_OF_TYPE( undoRec->bfTag, BFM_BMT));
    this_is_v3bmt = !RBMT_THERE(dmnP) &&
                    BS_BFTAG_RSVD(undoRec->bfTag) &&
                    BS_IS_TAG_OF_TYPE(undoRec->bfTag,BFM_BMT_V3);

    /* This is similar to the undo routine undo_upd_xtnt_rec. This is
     * the case where we were extending the BMT and we could not fit
     * the new extents on the exisiting subxtnt map. We needed to
     * create a new mcell tfor the new subxtnt map. This mcell is
     * now being unlinked from our mcell chain on-disk so at this
     * point we need to ditch the BMT's extent maps and reload them to
     * reflect in memory the fact that the on-disk extent maps have
     * changed.  */

    if(v4bmt_or_rbmt || this_is_v3bmt) 
    {
        if ((v4bmt_or_rbmt) && (BS_BFTAG_RBMT(undoRec->bfTag)))
            bfap = vd->rbmtp;
        else 
            bfap = vd->bmtp;

        xtnts = &(bfap->xtnts);

    /*
     * Originally, we were assuming that the xtntMap_lk lock was not taken, so
     * we took it exclusively here in order to invalidate the extent map.
     * Then we could release it before calling x_load_inmem_xtnt_map(...,
     * X_LOAD_REFERENCE), in order to satisfy that routine's preconditions.
     * But we may come through here from the ftx_fail() case of rbmt_extend()
     * and in that case, we are already holding all the locks we need to avoid
     * a lock hierarchy violation in x_load_inmem_xtnt_map().
     *
     * So we now have to check whether we hold the lock before taking it and
     * remember whether we need to unlock it on return, or let the caller
     * unlock it.
     */
        if ( !lock_holder(&bfap->xtntMap_lk.lock) ) {
            XTNMAP_LOCK_WRITE( &bfap->xtntMap_lk );
        } else {
            keep_lock = 1;
        }

        imm_delete_xtnt_map (xtnts->xtntMap);
        xtnts->xtntMap = NULL;
        xtnts->validFlag = 0;
       /* Keep the lock if keep_lock is 1. */
        if ( keep_lock == 0) {
            XTNMAP_UNLOCK(&bfap->xtntMap_lk);
            x_load_inmem_xtnt_map(bfap,X_LOAD_REFERENCE);
        } else {
            x_load_inmem_xtnt_map(bfap,X_LOAD_LOCKSOWNED);
        }

        bfap->nextPage = imm_get_next_page(xtnts);
        imm_get_alloc_page_cnt(xtnts->xtntMap,0,bfap->nextPage,&xtnts->allocPageCnt);

        /* Release the lock only if keep_lock is zero. */
        if ( keep_lock == 0) {
            XTNMAP_UNLOCK(&bfap->xtntMap_lk);
        }
    }

    return;

}  /* end link_unlink_mcells_undo */

/*
 * bmt_link_mcells
 *
 * This function links the mcell chain after the mcell specified by the
 * "prevVdIndex" and "prevMcellId" parameters.  The first mcell in the
 * chain is specified by the "firstVdIndex" and "firstMcellId" parameters.
 * The last mcell in the chain is specified by the "lastVdIndex" and
 * "lastMcellId" parameters.
 *
 * This function assumes that the caller has exclusive access to the previous
 * mcell and the mcell chain.
 *
 * This function's undo routine does image undo.  See the
 * link_unlink_mcells_undo() function header comment for further information.
 *
 * NOTE: This function is not equiped to link a BSR_XTNTS into the
 *       primary MCELL. In other words firstMcellId CAN NOT be of
 *       type BSR_XTNTS !.
 *
 */

statusT
bmt_link_mcells (
                 domainT *dmnP,  /* in */
                 bfTagT bfTag, /* in */
                 vdIndexT prevVdIndex,  /* in */
                 bfMCIdT prevMcellId,  /* in */
                 vdIndexT firstVdIndex,  /* in */
                 bfMCIdT firstMcellId,  /* in */
                 vdIndexT lastVdIndex,  /* in */
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
    bfAccessT *mdap;

    vd = VD_HTOP(prevVdIndex, dmnP);

    sts = FTX_START_META (FTA_BS_BMT_LINK_MCELLS_V1, &ftx, parentFtx,
                          vd->dmnP, 2);
    if (sts != EOK) {
        return sts;
    }

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(bfTag) ) {
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
                     ftx
                     );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }
    prevMcell = &(bmt->bsMCA[prevMcellId.cell]);

    /*
     * See if there is a BSR_XTNTS record in the previous mcell.
     * If so, we're going to connect the chain via the
     * bsXtntRT.chain* fields.  Otherwise, we're going to use
     * the bsMCT.next* fields in the mcell header.
     */
    xp = bmtr_find(prevMcell, BSR_XTNTS, dmnP);

    vd = VD_HTOP(lastVdIndex, dmnP);

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(lastVdIndex, dmnP)->rbmtp;
    } else {
        mdap = VD_HTOP(lastVdIndex, dmnP)->bmtp;
    }

    sts = rbf_pinpg (
                     &lastPgPin,
                     (void*)&bmt,
                     mdap,
                     lastMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }
    lastMcell = &(bmt->bsMCA[lastMcellId.cell]);

    undoRec.bfTag = bfTag;
    undoRec.prevVdIndex = prevVdIndex;
    undoRec.prevMCId = prevMcellId;
    if (xp) {
        undoRec.prevNextVdIndex = xp->chainVdIndex;
        undoRec.prevNextMCId = xp->chainMCId;
    }
    else {
        undoRec.prevNextVdIndex = prevMcell->nextVdIndex;
        undoRec.prevNextMCId = prevMcell->nextMCId;
    }
    undoRec.lastVdIndex = lastVdIndex;
    undoRec.lastMCId = lastMcellId;
    undoRec.lastNextVdIndex = lastMcell->nextVdIndex;
    undoRec.lastNextMCId = lastMcell->nextMCId;

    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->nextVdIndex),
                    sizeof(lastMcell->nextVdIndex)
                    );
    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->nextMCId),
                    sizeof(lastMcell->nextMCId)
                    );
    if (xp) {
        lastMcell->nextVdIndex = xp->chainVdIndex;
        lastMcell->nextMCId = xp->chainMCId;
    }
    else {
        lastMcell->nextVdIndex = prevMcell->nextVdIndex;
        lastMcell->nextMCId = prevMcell->nextMCId;
    }

    if (xp) {
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainVdIndex),
                        sizeof(xp->chainVdIndex)
                        );
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainMCId),
                        sizeof(xp->chainMCId)
                        );
        xp->chainVdIndex = firstVdIndex;
        xp->chainMCId = firstMcellId;
    }
    else {
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->nextVdIndex),
                        sizeof(prevMcell->nextVdIndex)
                        );
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->nextMCId),
                        sizeof(prevMcell->nextMCId)
                        );
        prevMcell->nextVdIndex = firstVdIndex;
        prevMcell->nextMCId = firstMcellId;
    }

    ftx_done_u (ftx, FTA_BS_BMT_LINK_MCELLS_V1, sizeof (undoRec), &undoRec);

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
                   vdIndexT prevVdIndex,  /* in */
                   bfMCIdT prevMcellId,  /* in */
                   vdIndexT firstVdIndex,  /* in */
                   bfMCIdT firstMcellId,  /* in */
                   vdIndexT lastVdIndex,  /* in */
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
    bfAccessT *mdap;

    vd = VD_HTOP(prevVdIndex, dmnP);

    sts = FTX_START_META (FTA_BS_BMT_UNLINK_MCELLS_V1, &ftx, parentFtx,
                          vd->dmnP, 2);
    if (sts != EOK) {
        return sts;
    }

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(bfTag) ) {
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
                     ftx
                     );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }
    prevMcell = &(bmt->bsMCA[prevMcellId.cell]);

    /*
     * See if there is a BSR_XTNTS record in the previous mcell.
     * If so, we're going to disconnect the chain via the
     * bsXtntRT.chain* fields.  Otherwise, we're going to use
     * the bsMCT.next* fields in the mcell header.
     */
    xp = bmtr_find(prevMcell, BSR_XTNTS, dmnP);

    if (xp) {
        if ((firstVdIndex != xp->chainVdIndex) ||
            (firstMcellId.page != xp->chainMCId.page) ||
            (firstMcellId.cell != xp->chainMCId.cell)) {
            /*
             * This is a condition that only affects this domain.
             * Domain panic - don't panic.  Also, clean up before we leave.
             */
            printf("firstVdIndex is %lu - xp->chainVdIndex is %lu\n",
                   firstVdIndex, xp->chainVdIndex);
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
        if ((firstVdIndex != prevMcell->nextVdIndex) ||
            (firstMcellId.page != prevMcell->nextMCId.page) ||
            (firstMcellId.cell != prevMcell->nextMCId.cell)) {
            /*
             * This is a condition that only affects this domain.
             * Domain panic - don't panic.  Also, clean up before we leave.
             */
            printf("firstVdIndex is %lu - prevMcell->nextVdIndex is %lu\n",
                   firstVdIndex, prevMcell->nextVdIndex);
            printf("firstMcellId.page is %lu - prevMcell->nextMCId.page is %lu\n",
                   firstMcellId.page , prevMcell->nextMCId.page);
            printf("firstMcellId.cell is %lu - prevMcell->nextMCId.cell is %lu\n",
                   firstMcellId.cell, prevMcell->nextMCId.cell);
            domain_panic( dmnP,
               "previous mcell doesn't point to first mcell" );
            ftx_fail (ftx);
            return E_DOMAIN_PANIC;
        }
    }

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(lastVdIndex, dmnP)->rbmtp;
    } else {
        mdap = VD_HTOP(lastVdIndex, dmnP)->bmtp;
    }
    sts = rbf_pinpg (
                     &lastPgPin,
                     (void*)&bmt,
                     mdap,
                     lastMcellId.page,
                     BS_NIL,
                     ftx
                     );
    if (sts != EOK) {
        ftx_fail (ftx);
        return sts;
    }
    lastMcell = &(bmt->bsMCA[lastMcellId.cell]);

    undoRec.bfTag = bfTag;
    undoRec.prevVdIndex = prevVdIndex;
    undoRec.prevMCId = prevMcellId;
    if (xp) {
        undoRec.prevNextVdIndex = xp->chainVdIndex;
        undoRec.prevNextMCId = xp->chainMCId;
    }
    else {
        undoRec.prevNextVdIndex = prevMcell->nextVdIndex;
        undoRec.prevNextMCId = prevMcell->nextMCId;
    }
    undoRec.lastVdIndex = lastVdIndex;
    undoRec.lastMCId = lastMcellId;
    undoRec.lastNextVdIndex = lastMcell->nextVdIndex;
    undoRec.lastNextMCId = lastMcell->nextMCId;

    if (xp) {
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainVdIndex),
                        sizeof (xp->chainVdIndex)
                        );
        rbf_pin_record (
                        prevPgPin,
                        &(xp->chainMCId),
                        sizeof (xp->chainMCId)
                        );
        xp->chainVdIndex = lastMcell->nextVdIndex;
        xp->chainMCId = lastMcell->nextMCId;
    }
    else {
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->nextVdIndex),
                        sizeof (prevMcell->nextVdIndex)
                        );
        rbf_pin_record (
                        prevPgPin,
                        &(prevMcell->nextMCId),
                        sizeof (prevMcell->nextMCId)
                        );
        prevMcell->nextVdIndex = lastMcell->nextVdIndex;
        prevMcell->nextMCId = lastMcell->nextMCId;
    }

    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->nextVdIndex),
                    sizeof (lastMcell->nextVdIndex)
                    );
    lastMcell->nextVdIndex = bsNilVdIndex;
    rbf_pin_record (
                    lastPgPin,
                    &(lastMcell->nextMCId),
                    sizeof (lastMcell->nextMCId)
                    );
    lastMcell->nextMCId = bsNilMCId;

    ftx_done_u (ftx, FTA_BS_BMT_UNLINK_MCELLS_V1, sizeof (undoRec), &undoRec);

    return sts;

}  /* end bmt_unlink_mcells */

/*
 * bmt_free_mcell
 *
 * Put the specified mcell on the dead list.
 * Caller has the mcell_lk.  This function does not undo itself,
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
    bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };
    statusT sts = EOK;
    bsMRT *rec;
    domainT* dmnP = vdp->dmnP;

    MS_SMP_ASSERT(lock_holder(&vdp->mcell_lk.lock));

    if (mcid.page != bmtpgp->pageId) {
        ADVFS_SAD0( "bmt_free_mcell: page id does not agree with mcid" );
    }

    if (BS_BFTAG_EQL(mcp->tag,NilBfTag) &&
        BS_BFTAG_EQL(mcp->bfSetTag,NilBfTag)) {
        ADVFS_SAD2("bmt_free_mcell: mcell already free",mcid.cell,mcid.page);
    }

    rec = (bsMRT *)&(mcp->bsMR0[0]);
    if ((rec->bCnt - sizeof(bsMRT)) > BS_USABLE_MCELL_SPACE) {
        if (mcid.cell != 0) {
            ADVFS_SAD0("bmt_free_mcell: page type must begin in cell zero");
        }

        rbf_pin_record(pgref, bmtpgp, ADVFS_PGSZ);

        bmt_init_page( bmtpgp, mcid.page, BFM_BMT, dmnP->dmnVersion );

        bmtpgp->nextFreePg =
          bmt_upd_mcell_free_list( ftxH, vdp, bmtpgp->pageId );

    } else {

        /*
         ** Link the mcell into it's page's free mcell list.
         */

        rbf_pin_record( pgref,
                        &mcp->nextMCId,
                        (char*)mcp->bsMR0 - (char*)&mcp->nextMCId
                          + sizeof (bsMRT) );

        mcp->nextMCId    = bmtpgp->nextfreeMCId;
        mcp->nextVdIndex = 0;
        mcp->linkSegment = 0;
        mcp->tag         = NilBfTag;
        mcp->bfSetTag    = NilBfTag;

        rec->bCnt = sizeof (bsMRT);
        rec->type = BSR_NIL;

        rbf_pin_record( pgref,
                       &bmtpgp->nextfreeMCId,
                       (char*)&bmtpgp->bsMCA[0] - (char*)&bmtpgp->nextfreeMCId);

        bmtpgp->nextfreeMCId = mcid;
        bmtpgp->freeMcellCnt++;

        /*
         ** If the page's mcell free list had been empty (until we added
         ** the current mcell to it) then we need to put this page at the
         ** head of the global mcell free list (the list of pages that
         ** contain free mcells).
         */

        if ( bmtpgp->freeMcellCnt == 1 ) {

            bmtpgp->nextFreePg =
              bmt_upd_mcell_free_list( ftxH, vdp, bmtpgp->pageId );
        }
    }

    return;
}

/*
 * bmtr_clone_recs
 *
 * NOTE: This is a copy of the mcell recs
 *
 * Copies the mcell records from the original bitfile's mcells to
 * the clone's mcells.  Only the non-extra extent records are
 * are copied.
 *
 * The caller must prevent the original from changing.  This is done
 * by either holding the original's mcell list and the moveMetadata_lk
 * lock (shared) or holding the original's moveMetadata_lk lock (exclusive).
 * Also, the caller must have exclusive access to the clone's primary
 * mcell.
 *
 * Changed the names of the parameters to better reflect how they are used.
 * original became source, and clone was changed to destination.
 *
 * As the primary mcell chain can cross volumes, added the parameter
 * npVdIndex.  This is used by move_metadata if it has to move the
 * location of the primary mcell.
 *
 */


statusT
bmtr_clone_recs(
    bfMCIdT srcMCId,     /* in - Source bf primary mcell's id */
    uint16T srcVdIndex,  /* in - Source bf primary mcell's disk index */
    bfMCIdT destMCId,    /* in - Destination bf primary mcell's id */
    uint16T destVdIndex, /* in - Destination bf primary mcell's disk index */
    uint16T npVdIndex,   /* in - new primary disk index */
    domainT *dmnP,       /* in - ptr to domain struct */
    ftxHT parentFtxH     /* in - parent ftx handle */
    )
{
    vdT *srcVdp, *parentVdp, *childVdp;
    bfPageRefHT pgRef;
    bsMPgT *srcBmtp;
    bsMCT *srcMcp;
    bfMCIdT mcid;
    bsMRT *rp;
    ftxHT ftxH;
    int pgRefed = FALSE, ftxStarted = FALSE, first = TRUE;
    uint16T vdIndex;
    statusT sts;

    mcid = srcMCId;
    vdIndex = srcVdIndex;

    parentVdp = VD_HTOP(npVdIndex, dmnP); 
    childVdp  = VD_HTOP(destVdIndex, dmnP);

    sts = FTX_START_N( FTA_BS_BMT_CLONE_RECS_V1, &ftxH, parentFtxH,
		       parentVdp->dmnP, 4 );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    ftxStarted = TRUE;

    /*
     * For each mcell in the original bitfile...
     */

    do {
	srcVdp = VD_HTOP(vdIndex, dmnP);

        sts = bmt_refpg(&pgRef, (void*)&srcBmtp, srcVdp->bmtp, mcid.page, BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        pgRefed = TRUE;

        /* get a pointer to the next mcell */
        srcMcp = &srcBmtp->bsMCA[ mcid.cell ];

        /* get a pointer to the first mcell record in the current mcell */
        rp = (struct bsMR *) srcMcp->bsMR0;

        /* copy contence of mcell */
        sts = bmtr_clone_mcell(rp, parentVdp, &destMCId, 
			       childVdp, ftxH, first);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

	if (TRUE == first) {
	    first = FALSE;
	} else if (FALSE == first) {
	    /*
	     * first call to bmtr_clone_mcell: destMCId returns itself
	     *	   NOTE: childVdp ignored, no mcell alloced
	     *
	     * second call to bmtr_clone_mcell: parentVdp & destMCId 
	     * are first mcell in chain
	     *     NOTE: first mcell is on parentVdi
	     *           alloc second mcell from childVdp
	     *
	     * after second call, before 3rd: 
	     *   2nd mcell alloced on childVdp, so set parentVdp to childVdp
	     */
	    parentVdp = childVdp;
	}


        /*
         * Move on to the next mcell in the orig bitfile's mcell list.
         */

        mcid = srcMcp->nextMCId;
        vdIndex = srcMcp->nextVdIndex;

        (void) bs_derefpg( pgRef, BS_CACHE_IT );
        pgRefed = FALSE;

    } while ((vdIndex != 0) &&
             !((mcid.page == 0) && (mcid.cell == 0)));

    ftx_done_n( ftxH, FTA_BS_BMT_CLONE_RECS_V1 );

    return EOK;

HANDLE_EXCEPTION:

    if (pgRefed) {
        (void) bs_derefpg( pgRef, BS_CACHE_IT );
    }

    if (ftxStarted) {
        ftx_done_n( ftxH, FTA_BS_BMT_CLONE_RECS_V1 );
    }

    return sts;
}


/*
 * bmtr_clone_mcell
 *
 * The function copies the source mcell into a new mcell.  The newly
 * allocated mcell will be located on the destination disk index.
 */
static statusT
bmtr_clone_mcell(
                 bsMRT   *rp,
                 vdT     *srcVdp,  /* Source mcell's disk index */
                 bfMCIdT *srcMCId, /* Source mcell's id */
		 vdT     *destVdp, /* Destination mcell's disk index */
                 ftxHT   parentFtxH,
                 int     first
                 )
{
    statusT sts;
    ftxHT ftxH;
    int ftxStarted = FALSE, i, page_cell;
    bfMCIdT currentMcid;
    char *origRp, *currentRp;
    bsMPgT *currentBmtp;
    bsMCT *currentMcp;
    rbfPgRefHT currentPgRef;
    vdT *currentVdp;

    sts = FTX_START_N(FTA_BS_BMT_CLONE_MCELL_V1, &ftxH, parentFtxH,
                      srcVdp->dmnP, 4);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    ftxStarted = TRUE;

    /*
     * determine if this cell occupies a whole page
     */
    page_cell = (rp->bCnt - sizeof(bsMRT)) > BS_USABLE_MCELL_SPACE;

    /*
     * select MC, on first call use primary mcell already alloced
     */
    if (first) {
        currentMcid = *srcMCId;
	currentVdp  = srcVdp; /* we are going to pin on this vd */
    } else {
        sts = bmt_alloc_link_mcell (
                                    srcVdp->dmnP,
				    destVdp->vdIndex, /* destination index */
                                    (page_cell ?
                                     BMT_NORMAL_MCELL_PAGE : BMT_NORMAL_MCELL),
                                    srcVdp->vdIndex,  /* source index */
                                    *srcMCId,
                                    ftxH,
                                    &currentMcid
                                    );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
	currentVdp = destVdp; /* we are going to pin on this vd */
    }

    /*
     * advance mc pointer
     */
    *srcMCId = currentMcid;

    /*
     * pin down whole mcell record area
     */
    sts = rbf_pinpg( &currentPgRef, (void*)&currentBmtp,
                    currentVdp->bmtp, currentMcid.page, BS_NIL, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    currentMcp = &currentBmtp->bsMCA[ currentMcid.cell ];
    rbf_pin_record(currentPgRef,
                   currentMcp->bsMR0,
                   (page_cell ?
                    (((char *)currentBmtp) + ADVFS_PGSZ - ((char *)currentMcp->bsMR0)) :
                    BSC_R_SZ)
                   );

    /*
     * For each record in the mcell...
     */
    while (rp->type != BSR_NIL) {

        if ((rp->type != BSR_ATTR) && (rp->type != BSR_XTNTS)) {

            /*
             * Reached a record that we want to copy to the new mcell.
             * Allocate the record in the current's mcell.
             */
            currentRp = bmtr_assign( rp->type, rp->bCnt - sizeof(bsMRT), currentMcp );
            if (currentRp == NULL) {
                ADVFS_SAD0( "bmtr_clone_mcells: assign failed" );
            }

            /*
             * Copy the record from the orig bitfile to the allocated one.
             */
            origRp = (char *)rp + sizeof( bsMRT );

            for (i = 0; i < rp->bCnt - sizeof( bsMRT ); i++) {
                currentRp[i] = origRp[i];
            }
        }

        rp = (bsMRT *) ((char *)rp + roundup( rp->bCnt, sizeof( int )));
    }  /* end while */

    ftx_done_n(ftxH, FTA_BS_BMT_CLONE_MCELL_V1);

    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ftx_done_n(ftxH, FTA_BS_BMT_CLONE_MCELL_V1);
    }

    return sts;
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
                    uint64T cRangeBeginBlk,  /* in */
                    uint64T cRangeEndBlk,    /* in */
                    int *list_too_big, /* out - call back required? */
                    int *bmt_cnt  /* out */
                   )
{
    statusT sts=EOK;
    bsXtntT *fxp;
    uint xCnt;
    bsXtraXtntRT *xrp;
    bsShadowXtntT *srp;
    bsXtntRT *xp;
    int j;
    uint64T extentEndBlk, extentBeginBlk;
    uint64T migStartBlk, migEndBlk, xmPageOffset, xmPageCnt;
    ssPackLLT *currp=NULL, *xtntp=NULL;
    ssPackHdrT *plp=NULL;
    int inserted=FALSE;
    int retryFlg = 0;
    uint32T pg1, pg2;

    MS_SMP_ASSERT(cRangeBeginBlk < vdp->vdSize); 
    MS_SMP_ASSERT(cRangeEndBlk <= vdp->vdSize); 
    MS_SMP_ASSERT(cRangeEndBlk > cRangeBeginBlk);

    /* mcell contents might have changed while reading them. 
     * check the mcell again.
     */
    try_again:
    /* If there is a corruption in the mcell don't loop forever 
     * on a bad value. One try is enough and  go out .
     */
    if ( retryFlg > 1) { 
        return E_BAD_BMT; 
    }

    /* scan the in-use mcell for xtnt records */
    xCnt = 0;
    fxp = NULL;

    if ((xrp = (bsXtraXtntRT *)
            bmtr_find(mcp, BSR_XTRA_XTNTS, vdp->dmnP)) != NULL) {
        fxp = xrp->bsXA;
        xCnt = xrp->xCnt;
        /* Difficult to trust that this is still a BSR_XTRA_XTNTS mcell. 
         * so xCnt, as read now, could be bogus. If the value is not 
         * reasonable, try rereading the mcell again.
         */
	if ( xCnt > BMT_XTRA_XTNTS ) { 
	    retryFlg++; 
	    goto try_again; 
	}
    }
    else if ( ((xp = (bsXtntRT *)
            bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) != NULL)  &&
            ((BS_BFTAG_RSVD(mcp->tag)) ||
             (FIRST_XTNT_IN_PRIM_MCELL(vdp->dmnP->dmnVersion, xp->type))) ) {
        /* Only v4 non-stripe and v3/v4 reserved files have 
         * extents in BSR_XTNTS. 
         */
        fxp = xp->firstXtnt.bsXA;
        xCnt = xp->firstXtnt.xCnt;
	/* If the value is not reasonable for BMT_XTNTS mcell
	 * try rereading the mcell again.
	 */
	if ( xCnt > BMT_XTNTS && 
                !BS_IS_TAG_OF_TYPE(mcp->tag, BFM_MISC) ) {
	    retryFlg++; 
	    goto try_again; 
	}
    }
    else if ( (srp = (bsShadowXtntT *)
            bmtr_find(mcp, BSR_SHADOW_XTNTS, vdp->dmnP)) != NULL) {
        fxp = srp->bsXA;
        xCnt = srp->xCnt;
	/* If the value is not reasonable for BSR_SHADOW_XTNTS mcell
	 * try rereading the mcell again.
	 */
	if ( xCnt > BMT_SHADOW_XTNTS ) { 
	     retryFlg++; 
	     goto try_again; 
	}
    }
    if ( xCnt == 0 ) {
        retryFlg++;
        goto try_again;
    }

    /* look at the extents record and search the extents for range input */
    for(j=0; j<(xCnt-1); j++) {

        /* try to find block in extent */
        if(fxp[j].vdBlk == XTNT_TERM ||
            fxp[j].vdBlk == PERM_HOLE_START) {
            continue;    /* on to next extent */
        }

        extentBeginBlk = fxp[j].vdBlk;
	if ( extentBeginBlk >= vdp->vdSize ) { 
	    retryFlg++; 
	    goto try_again; 
	}
	/* capture the 2 page values in local variables to test them.
         * The variables are in the referenced mcell chain, so would 
         * not be read from disk again.
	 */
	pg1 = fxp[j].bsPage;
	pg2 = fxp[j+1].bsPage;
	if ( pg2 <= pg1 ) { 
	    retryFlg++; 
	    goto try_again; 
	} 
	if ( pg2 > max_page ) { 
	    retryFlg++; 
	    goto try_again; 
	}

	/* Use local copy to find out extent end block. */
	extentEndBlk = extentBeginBlk + ( pg2 - pg1 ) * ADVFS_PGSZ_IN_BLKS;
	if ( extentEndBlk > vdp->vdSize ) { 
            retryFlg++; 
            goto try_again; 
        }
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

            if (BS_BFTAG_RSVD(mcp->tag)) {
               /* ran into some reserved file allocated bits that are in the
                * range to be packed.  Simply abort since we can't migrate
                * reserved files with the standard migrate.
                */
                RAISE_EXCEPTION(E_RSVD_FILE_INWAY); /* exit routine */
            }

            /* adjust entry for only that part of extent thats in range */
            migStartBlk = MAX(cRangeBeginBlk,extentBeginBlk);
            migEndBlk = MIN(cRangeEndBlk,extentEndBlk);

	    /* Use local copy rather than reading disk */
	    MS_SMP_ASSERT(migStartBlk >= extentBeginBlk); 
	    xmPageOffset = pg1 + ( migStartBlk - extentBeginBlk ) 
	                                    / ADVFS_PGSZ_IN_BLKS; 
	    MS_SMP_ASSERT(migEndBlk > migStartBlk); 
	    xmPageCnt = ( migEndBlk - migStartBlk ) / ADVFS_PGSZ_IN_BLKS;


            /* Allocate a list entry for the extent found in the clear range */
            xtntp = (ssPackLLT *)(ms_malloc( sizeof(ssPackLLT) ));

            /* add this entry to the linked list */
            xtntp->ssPackBfSetId.domainId   = vdp->dmnP->domainId;
            xtntp->ssPackBfSetId.dirTag.num = mcp->bfSetTag.num;
            xtntp->ssPackBfSetId.dirTag.seq = mcp->bfSetTag.seq;
            xtntp->ssPackTag.num    = mcp->tag.num;
            xtntp->ssPackTag.seq    = mcp->tag.seq;
            xtntp->ssPackPageOffset = xmPageOffset;
            xtntp->ssPackPageCnt    = xmPageCnt;
            xtntp->ssPackStartXtBlock = migStartBlk;
            xtntp->ssPackEndXtBlock   = migEndBlk;
            *bmt_cnt += xtntp->ssPackPageCnt * (ADVFS_PGSZ_IN_BLKS / vdp->stgCluster);

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
                *bmt_cnt -= currp->ssPackPageCnt;
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
                uint64T cRangeBeginBlk,  /* in */
                uint64T cRangeEndBlk,    /* in */
                int *another_scan_required,/* out */
                int *bmt_cnt  /* out - cnt of allocated pages in range */
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
    if(RBMT_THERE(vdp->dmnP)) {
        /* do rbmt first if there. Optimizes because if rbmt has an extent in the
         * way then we can abort this range since we cant move reseerved files.
         */
        doing_bmt = FALSE;
    }

chk_bmt:

    bmtH.curPg = bsNilMCId.page;
    bmtH.curMcell = bsNilMCId.cell;

    bmth = &bmtH;

    bmth->vdp = vdp;
    MCELL_LOCK_WRITE( &vdp->mcell_lk )
    sts = bmt_refpg( &bmth->pgRef,
                         (void *)&bmth->pgPtr,
                         (doing_bmt==FALSE)?vdp->rbmtp:vdp->bmtp,
                         bmth->curPg,
                         BS_SEQ_AHEAD);
    MCELL_UNLOCK( &vdp->mcell_lk )
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
               (bmth->curPg >= vdp->bmtp->nextPage)) {
                /* no more pages in bmt to process */
                RAISE_EXCEPTION(EOK);
            }
            if((doing_bmt==FALSE) &&   /* check for end of rbmt */
               (bmth->curPg >= vdp->rbmtp->nextPage)) {
                /* no more pages in rbmt to process */
                RAISE_EXCEPTION(EOK);
            }

            /*
             ** Try and get the next (R)BMT page.
             */
            MCELL_LOCK_WRITE( &vdp->mcell_lk)
            sts = bmt_refpg(&bmth->pgRef,
                           (void *)&bmth->pgPtr,
                           (doing_bmt==FALSE) ? bmth->vdp->rbmtp : bmth->vdp->bmtp,
                            bmth->curPg,
                            BS_SEQ_AHEAD);
            MCELL_UNLOCK( &vdp->mcell_lk)

            if (sts != EOK) {
                if(sts == E_PAGE_NOT_MAPPED) {
                    /* no more pages in bmt to process */
                    RAISE_EXCEPTION(EOK);
                } else
                    RAISE_EXCEPTION(sts);
            }

        }
        mcp = &bmth->pgPtr->bsMCA[bmth->curMcell];
        bmth->curMcell++;   /* bump to the next mcell for next loop */

        if ((!BS_BFTAG_EQL (mcp->bfSetTag, NilBfTag)) &&
            (!BS_BFTAG_EQL (mcp->tag, NilBfTag))) {

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

    bmtH.curPg = nextBfDescId->page;
    bmtH.curMcell = nextBfDescId->cell;
    sts = bmt_open (&bmtH, dmnP, vdIndex);
    if (sts != EOK) {
        if (sts == E_PAGE_NOT_MAPPED) {
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
            if (sts == E_PAGE_NOT_MAPPED || sts == E_LAST_PAGE) {
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
         ** Setup state buffer and read the first page.
         */

        MCELL_LOCK_WRITE( &bmth->vdp->mcell_lk )
        sts = bmt_refpg( &bmth->pgRef,
                         (void *)&bmth->pgPtr,
                         bmth->vdp->bmtp,
                         bmth->curPg,
                         BS_SEQ_AHEAD);
        MCELL_UNLOCK( &bmth->vdp->mcell_lk )

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
    uint32T lastPage;	
    /*
     ** Scan the pages until a primary mcell is found.
     */
    while (!found) {

        if (bmth->curMcell >= BSPG_CELLS) {
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
	    /* 1 past the highest page allocated in the bmt */
	    lastPage =  bmth->vdp->bmtp->nextPage;		
	    /* check if we are past the last page of the bmt */		
	    if(bmth->curPg >= lastPage)
		return E_LAST_PAGE;
		
            /*
             ** Get the next BMT page.
             */
            MCELL_LOCK_WRITE( &bmth->vdp->mcell_lk)
            sts = bmt_refpg( &bmth->pgRef,
                             (void *)&bmth->pgPtr,
                             bmth->vdp->bmtp,
                             bmth->curPg,
                             BS_SEQ_AHEAD);
            MCELL_UNLOCK( &bmth->vdp->mcell_lk)

            if (sts != EOK) {
                return sts;
            }
        }

        mcp = &bmth->pgPtr->bsMCA[bmth->curMcell];
        bmth->curMcell++;

        if ((BS_BFTAG_REG(mcp->tag)) &&
            (!BS_BFTAG_EQL (mcp->bfSetTag, NilBfTag)) &&
            (!BS_BFTAG_EQL (mcp->tag, NilBfTag))) {
            /*
             ** Found an in-use mcell.
             */
            found = TRUE;
        }
    }  /* end while */

    *bfSetTag = mcp->bfSetTag;
    *bfTag = mcp->tag;

    if ((bmtr_find (mcp, BSR_SHADOW_XTNTS, bmth->vdp->dmnP) != NULL) ||
        (bmtr_find (mcp, BSR_XTRA_XTNTS, bmth->vdp->dmnP) != NULL)) {
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

#define DELDEL 1
#define XCHAIN 2
/*
 * Remove a primary mcell from the del chain, freeing appropriate
 * chained mcells.  None of the chained mcells contain extent descriptors
 * which represent allocated storage.
 *
 * There are two chains of mcells which may be attached to
 * the primary:
 *
 *   1. The mcell chain linked from the "next" pointers in
 *      the mcell header.  This chain will not contain any extent
 *      descriptors.
 *
 *   2. The mcell chain linked from the "chain" pointers in the
 *      the primary extent record in the mcell header.  This chain
 *      contains only extent descriptors.
 *
 * The primary mcell is removed from the delete pending list.
 */

void
bmt_free_bf_mcells_i(
                     vdT* vdp,          /* in - ptr to vd struct */
                     bfMCIdT primMCellId, /* in - prim mcell id */
                     ftxHT parentFtxH,  /* in - parent ftx */
                     vdIndexT xvdIndex, /* in - extra chain vdindex */
                     bfMCIdT xmcid,     /* in - extra chain mcellid */
                     int mode           /* in - mode */
                     )
{
    ftxHT ftxH;
    statusT sts;
    bsMPgT *bmt;
    bsMCT *mcell;
    bfPageRefHT pgRef;
    bsXtntRT *xtntRec;
    mcellPtrRecT fbfm[3];
    int chainCnt = 1;

    fbfm[0].mcid = primMCellId;
    fbfm[0].vdIndex = vdp->vdIndex;

    if ( mode & DELDEL ) {

        sts = bmt_refpg( &pgRef,
                         (void*)&bmt,
                         vdp->bmtp,
                         primMCellId.page,
                         BS_NIL);
        if (sts != EOK) {
            return;
        }

        mcell = &(bmt->bsMCA[primMCellId.cell]);
        xtntRec = bmtr_find (mcell, BSR_XTNTS, vdp->dmnP);
        if (xtntRec != NULL) {
            fbfm[1].mcid = xtntRec->chainMCId;
            fbfm[1].vdIndex = xtntRec->chainVdIndex;
            chainCnt = 2;
        }


        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            return;
        }
    }

    if ( mode & XCHAIN ) {
        fbfm[chainCnt].mcid = xmcid;
        fbfm[chainCnt].vdIndex = xvdIndex;
        ++chainCnt;
    }
    sts = FTX_START_N(FTA_BS_BMT_FREE_BF_MCELLS_V1, &ftxH,
                      parentFtxH, vdp->dmnP, 1);
    if ( sts != EOK ) {
        return;
    }

    if ( mode & DELDEL ) {
        sts = del_remove_from_del_list( primMCellId, vdp, TRUE, ftxH );
        if ( sts != EOK ) {
            domain_panic (vdp->dmnP,
                "bmt_free_bf_mcells_i: del_remove_from_del_list returned: %d",
                sts);
            ftx_fail( ftxH );
            return;
        }
    }
    ftx_done_urd(ftxH, FTA_BS_BMT_FREE_BF_MCELLS_V1,
                 0, 0, /* undo */
                 sizeof(mcellPtrRecT)*chainCnt, (void *)&fbfm[0]); /*rt dn*/
}

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
    mcellPtrRecT* mcprp = (mcellPtrRecT*)opRec;
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

    vdIndex = mcprp->vdIndex;
    mcellId = mcprp->mcid;

    if ( chainCnt > 3 ) {
        ADVFS_SAD1("free_mcell_chains_opx: chain cnt(N1) > 3", chainCnt);
    }
    for ( i = 0; i < chainCnt; ++i ) {
        fbfm[i] = *mcprp;
        ++mcprp;
    }

    dmnP =  ftxH.dmnP;
    pinPages = 0;
    pinRecs = 0;

    vdp = VD_HTOP(vdIndex, dmnP);
    FTX_LOCKWRITE(&vdp->mcell_lk, ftxH)

delchain:
    if ( ++pinPages > FTX_MX_PINP ) {
        goto set_continuation;
    }

    if ((sts = rbf_pinpg(&pgref, (void*)&bmtp, vdp->bmtp, mcellId.page,
        BS_NIL, ftxH)) != EOK) {
        domain_panic(dmnP, "free_mcell_chains_opx: rbf_pinpg failed, return code = %d", sts);
        return;
    }

    if ( bmtp->freeMcellCnt == 0 ) {
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
        fbfm[0].mcid = mcp->nextMCId;
        fbfm[0].vdIndex = mcp->nextVdIndex;

        bmt_free_mcell(ftxH, vdp, mcp, mcellId, bmtp, pgref);
nextmcell:
        mcellId = fbfm[0].mcid;

    } while ( (mcellId.page == bmtp->pageId) &&
             (fbfm[0].vdIndex == vdIndex) );

    if ( fbfm[0].vdIndex == vdIndex ) {
        /*
         * Still on the same vd, but a different page, so go test the
         * pin page limit and continue if okay.
         */
        goto delchain;
    } else if ( fbfm[0].vdIndex == bsNilVdIndex ) {
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
 * Remove a primary mcell from the del chain, freeing appropriate
 * chained mcells.  None of the chained mcells contain extent descriptors
 * which represent allocated storage.
 *
 * There are two chains of mcells which may be attached to
 * the primary:
 *
 *   1. The mcell chain linked from the "next" pointers in
 *      the mcell header.  This chain will not contain any extent
 *      descriptors.
 *
 *   2. The mcell chain linked from the "chain" pointers in the
 *      the primary extent record in the mcell header.  This chain
 *      contains only extent descriptors.
 *
 * The primary mcell is removed from the delete pending list.
 */

void
bmt_free_bf_mcells(
                   vdT* vdp,            /* in - ptr to vd struct */
                   bfMCIdT primMCellId, /* in - prim mcell id */
                   ftxHT parentFtxH,    /* in - parent ftx */
                   int freeXchain       /* in - free xtent chain */
                   )
{
    int mode;

    if ( freeXchain ) {
        mode = DELDEL;
    } else {
        mode = 0;
    }

    bmt_free_bf_mcells_i( vdp, primMCellId, parentFtxH,
                         bsNilVdIndex, bsNilMCId, mode );
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
                         vdIndexT oldVdIndex,  /* in */
                         bfMCIdT oldMcellId,   /* in */
                         ftxHT parentFtx,      /* in */
                         vdIndexT *newVdIndex, /* out */
                         bfMCIdT *newMcellId,  /* out */
                         mcellPoolT poolType   /* in */
                         )
{

    bfMCIdT mcellId;
    statusT sts;
    vdT *vdp;

    /* sc_select_vd_for_mcell will bump the volume's vdRefCnt if EOK. */
    sts = sc_select_vd_for_mcell( &vdp,
                                  bfAccess->dmnP,
                                  bfAccess->dmnP->scTbl,
                                  bfAccess->reqServices,
                                  bfAccess->optServices);

    if ( sts != EOK ) {
        return ENO_MORE_MCELLS;
    }

    sts = bmt_alloc_link_mcell( bfAccess->dmnP,
                                vdp->vdIndex,
                                poolType,
                                oldVdIndex,
                                oldMcellId,
                                parentFtx,
                                &mcellId);
    if ( sts == EOK ) {
        *newVdIndex = vdp->vdIndex;
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
    extern task_t first_task;
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
        return;
    }

    /* Create and start the RBMT extension thread.  */
    if (!kernel_thread( first_task, bs_extend_rbmt_thread )) {
        ADVFS_SAD0("AdvFS rbmt extension thread was not spawned.\n");
        return;
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
test_mcell_ptr( vdIndexT vdi, bfMCIdT mcid, bfTagT tag, domainT *dmnP)
{
    statusT sts = EOK;
    vdT *vdp;
    bfAccessT *mdap;

    if ( vdi == 0 ) {
        return( EOK );
    }

    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
        return E_BAD_BMT;
    } 

    if ( RBMT_THERE(dmnP) && BS_BFTAG_RSVD(tag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }

    /* During domain activation rbmtp or bmtp in vdp are not populated.
       Hence, mdap above will be NULL. Igonre the check below for this
       special case.
    */

    if (mdap && (TEST_PAGE(mcid.page, mdap) != EOK)) {
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
check_mcell_ptr( vdIndexT vdi, bfMCIdT mcid, bfTagT tag, domainT *dmnP)
{
    statusT sts = EOK;

    if ( vdi == 0 ) {
        domain_panic(dmnP, "Invalid (0) vdi");
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( test_mcell_ptr(vdi, mcid, tag, dmnP) != EOK ) {
        domain_panic(dmnP,
            "Bad mcell pointer: vdIndex %d, mcellId page,cell %d,%d ",
            vdi, mcid.page, mcid.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

HANDLE_EXCEPTION:
    return sts;
}

/*
 * Test a (R)BMT page by checking the page number and version number.
 *
 * Assumes bfap, bfap->dmnP, and dmnVersion is set up.
 * If an error is found, panic the domain and set bsBuf result E_BAD_BMT.
 *
 * Notes:
 *      1) This is called inside an LWC and must not sleep.
 *      2) The bsBuf vmpage must be busy and/or held.
 */

void check_BMT_pg( struct bsBuf *bp)
{
    bsMPgT *bsMPgp = (bsMPgT *)ubc_load(bp->vmpage, 0, 0);

    /* This redundant checking gives a fast path out on success. */
    if (!bp->result && bsMPgp->pageId == bp->bfPgNum &&
        (bsMPgp->megaVersion == bp->bfAccess->dmnP->dmnVersion ||
         bsMPgp->megaVersion == 2) ) {

        return;

    } else {

        char buf[64];        /* get message buffer stack now */

        if (bp->result) {
            sprintf(buf, "BMT page %d I/O status %d.",
                    bp->bfPgNum, bp->result);
        } else if (bsMPgp->megaVersion != bp->bfAccess->dmnP->dmnVersion &&
                 bsMPgp->megaVersion != 2) {
            sprintf(buf, "BMT page %d version %d invalid.",
                    bp->bfPgNum, bsMPgp->megaVersion);
            bp->result = E_BAD_BMT;
        } else if (bsMPgp->pageId != bp->bfPgNum) {
            sprintf(buf, "BMT page %d page id %d invalid.",
                    bp->bfPgNum, bsMPgp->pageId);
            bp->result = E_BAD_BMT;
        }

        domain_panic(bp->bfAccess->dmnP, buf);
    }
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
    vdIndexT vdi = (vdIndexT)mcp->nextVdIndex;

    if ( test_mcell_ptr(vdi, mcp->nextMCId, bfap->tag, bfap->dmnP) ) {
        domain_panic(bfap->dmnP,
            "Bad mcell next pointer: vdIndex %d, mcellId page,cell %d,%d ",
            vdi, mcp->nextMCId.page, mcp->nextMCId.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( !BS_BFTAG_EQL(mcp->tag, bfap->tag) ) {
        domain_panic(bfap->dmnP,
            "Incorrect tag (%x.%x) in mcell. Expected %x.%x.",
            mcp->tag.num, mcp->tag.seq, bfap->tag.num, bfap->tag.seq);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

#ifdef NOTYET
    This doesn't work for some reason.
    In a one fileset domain, bfSetTag == 1 but dirTag == 2.
    if ( !BS_BFTAG_EQL(mcp->bfSetTag, bfap->bfSetp->dirTag) ) {
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
    int bfPageSz = bfap->bfPageSz;

    if ( test_mcell_ptr( bsXtntRp->chainVdIndex,
                         bsXtntRp->chainMCId,
                         bfap->tag,
                         bfap->dmnP) )
    {
        domain_panic(bfap->dmnP,
            "Invalid chain mcell pointer: vdIndex %d, mcellId page,cell %d,%d.",
            bsXtntRp->chainVdIndex,
            bsXtntRp->chainMCId.page, bsXtntRp->chainMCId.cell);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    /* Do not try to validate firstXtnt values for striped files.  For
     * both V3 and V4 domains, we cannot predict the values that will
     * be in these fields.  This is stated in a comment inside the
     * correct_bsr_xtnts_record() in fixfdmn, and I found this to be
     * true during testing as well.
     * Also, do not try to validate these fields for non-reserved files 
     * in a V3 domain.  V3 records built under the 4.0 streams will have 
     * random values, although the records built under a 5.x stream will 
     * have predetermined, invalid values.  Since we can't detect how the
     * domain was built, just skip the validation for V3 normal files.
     */
    if (((bfap->dmnP->dmnVersion < FIRST_XTNT_IN_PRIM_MCELL_VERSION) &&
         BS_BFTAG_REG(bfap->tag)) ||
         (bsXtntRp->type == BSXMT_STRIPE) ) {
         return sts;
    } 

    /* Verify V3 metadata and all V4 non-striped files here.
     * The firstXtnt.xCnt is <= BMT_XTNTS (2) for all reserved files 
     * except the MISC file, where it is 3.
     */
    if ( (BS_IS_TAG_OF_TYPE(bfap->tag, BFM_MISC) && 
                ( bsXtntRp->firstXtnt.xCnt != 3 )) ||
         (!BS_IS_TAG_OF_TYPE(bfap->tag, BFM_MISC) && 
                ( bsXtntRp->firstXtnt.xCnt > BMT_XTNTS )) ) {
        domain_panic(bfap->dmnP,
            "Bad firstXtnt.xCnt (%d) in BSR_XTNTS record.",
            bsXtntRp->firstXtnt.xCnt);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    return check_xtnts( bsXtntRp->firstXtnt.bsXA,
                        bsXtntRp->firstXtnt.xCnt,
                        bfap,
                        vdp);

HANDLE_EXCEPTION:
    return sts;
}

/*
** Test the validity of a BSR_SHADOW_XTNTS record.
** Test blksPerPage in the record against bfPageSz from bfap.
** Test xCnt against BMT_SHADOW_XTNTS.
** Test allocVdIndex for validity.
** Failure panics the domain and returns E_BAD_BMT, else return EOK.
*/
statusT
check_BSR_SHADOW_XTNTS_rec( bsShadowXtntT *bsShadowXtntp,
                            bfAccessT *bfap,
                            vdT *vdp)
{
    statusT sts = EOK;
    int i;
    domainT *dmnP = bfap->dmnP;
    uint32T lastPage;

    if ( bsShadowXtntp->blksPerPage != bfap->bfPageSz ) {
        domain_panic(dmnP,
            "Bad blksPerPage (%d) in BSR_SHADOW_XTNTS record. Expected %d.",
            bsShadowXtntp->blksPerPage, bfap->bfPageSz);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsShadowXtntp->allocVdIndex != (vdIndexT)(-1) &&
         !vd_htop_if_valid(bsShadowXtntp->allocVdIndex, dmnP, FALSE, FALSE) )
    {
        domain_panic(dmnP,
            "Bad allocVdIndex (%d) in BSR_SHADOW_XTNTS record.",
            (int)bsShadowXtntp->allocVdIndex);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsShadowXtntp->xCnt > BMT_SHADOW_XTNTS ) {
        domain_panic(dmnP,
            "Bad xCnt (%d) in BSR_SHADOW_XTNTS record.", bsShadowXtntp->xCnt);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsShadowXtntp->bsXA[0].bsPage != 0 ) {
        domain_panic(dmnP,
            "Bad first page (%d) in BSR_SHADOW_XTNTS record.",
            bsShadowXtntp->bsXA[0].bsPage);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    return check_xtnts( bsShadowXtntp->bsXA, bsShadowXtntp->xCnt, bfap, vdp);

HANDLE_EXCEPTION:
    return sts;
}

/*
** Test the validity of a BSR_XTRA_XTNTS record.
** Test blksPerPage in the record against bfPageSz from bfap.
** Test xCnt against BMT_SHADOW_XTNTS.
** Test allocVdIndex for validity.
** Test that the this map starts where the last one left off.
** Failure panics the domain and returns E_BAD_BMT, else return EOK.
*/
statusT
check_BSR_XTRA_XTNTS_rec( bsXtraXtntRT *bsXtraXtntRp,
                          bfAccessT *bfap,
                          vdT *vdp,
                          uint32T lastPg)
{
    statusT sts;

    if ( bsXtraXtntRp->blksPerPage != bfap->bfPageSz ) {
        domain_panic(bfap->dmnP,
            "Bad blksPerPage (%d) in BSR_XTRA_XTNTS record. Expected %d.",
            bsXtraXtntRp->blksPerPage, bfap->bfPageSz);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsXtraXtntRp->xCnt > BMT_XTRA_XTNTS ) {
        domain_panic(bfap->dmnP,
            "Bad xCnt (%d) in BSR_XTRA_XTNTS record.", bsXtraXtntRp->xCnt);
        RAISE_EXCEPTION( E_BAD_BMT );
    }

    if ( bsXtraXtntRp->bsXA[0].bsPage != lastPg ) {
        domain_panic(bfap->dmnP,
            "Bad first page (%d) in BSR_XTRA_XTNTS record, expected %d.",
            bsXtraXtntRp->bsXA[0].bsPage, lastPg);
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
check_xtnts( bsXtntT bsXA[], uint16T cnt, bfAccessT *bfap, vdT *vdp)
{
    statusT sts = EOK;
    int i;
    uint32T lastPage;

    for ( i = 0; i < cnt; i++ ) {
        /* Pages are monotonically increasing */
        if ( i == 0 ) {
            lastPage = bsXA[0].bsPage;
        } else {
            if ( bsXA[i].bsPage <= lastPage ) {
                domain_panic(bfap->dmnP,
                 "Bad bsPage (%d) in xtnt record at extent %d. Expected page > %d",
                 bsXA[i].bsPage, i, lastPage);
                RAISE_EXCEPTION( E_BAD_BMT );
            }

            lastPage = bsXA[i].bsPage;
        }

        if ( bsXA[i].vdBlk == XTNT_TERM || bsXA[i].vdBlk == PERM_HOLE_START ) {
            continue;
        }

        /* All extents start on a multiple of 16 blocks */
        if ( bsXA[i].vdBlk % bfap->bfPageSz ) {
            domain_panic(bfap->dmnP,
             "Bad vdBlk (%d) in xtnt record %d. vdBlk must be a multiple of %d",
             bsXA[i].vdBlk, i, bfap->bfPageSz);
            RAISE_EXCEPTION( E_BAD_BMT );
        }

        if ( bsXA[i].vdBlk >= vdp->vdSize ) {
            domain_panic(bfap->dmnP,
                "Bad vdBlk (%d) in xtnt record %d. Max block in volume is %u",
                bsXA[i].vdBlk, i, vdp->vdSize);
            RAISE_EXCEPTION( E_BAD_BMT );
        }
    }

HANDLE_EXCEPTION:
    return sts;
}

/* end bs_bmt_util.c */
