/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992, 1993                *
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
 *  MSFS/bs
 *
 * Abstract:
 *
 *  bs_stg.c
 *  This module contains routines related to bitfile storage allocation and
 *  to in-memory and on-disk extent map maintenance.
 *
 * Date:
 *
 *  Thu Jul 12 18:29:30 1990
 *
 */
/*
 * HISTORY
 * 
 */

#pragma ident "@(#)$RCSfile: bs_stg.c,v $ $Revision: 1.1.357.20 $ (DEC) $Date: 2008/01/16 13:32:15 $"

#include <sys/param.h>
#include <sys/lock_probe.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_ims.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_stripe.h>
#include <msfs/ftx_privates.h>    /* for FTX_MX_PINP */
#include <msfs/bs_ods.h>        /* needed for BS_IS_TAG_OF_TYPE() */

extern mutexT IoQueueMutex;

#define ADVFS_MODULE BS_STG

/****************/
/* Private type */
/****************/

typedef struct mcellPtr {
    vdIndexT vdIndex;
    bfMCIdT mcellId;
} mcellPtrT;

/*******************************/
/* Private function prototypes */
/*******************************/

statusT
stg_add_stg_no_cow (
             ftxHT ftxH,                  /* in */
             bfAccessT *bfap,             /* in */
             unsigned long bfPageOffset,  /* in */
             unsigned long bfPageCnt,     /* in */
             int allowOverlapFlag,        /* in */
             uint32T *allocPageCnt        /* out */
             );

static
statusT
add_stg (
         bfAccessT *bfap,       /* in */
         uint32T bfPageOffset,  /* in */
         uint32T bfPageCnt,     /* in */
         int allowOverlapFlag,  /* in */
         bsInMemXtntT *xtnts,   /* in, modified */
         ftxHT parentFtx,       /* in */
         int updateDisk,        /* in */
         uint32T *allocPageCnt  /* out */
         );

static
statusT
add_rsvd_stg (
              bfAccessT *bfap,       /* in */
              uint32T bfPageOffset,  /* in */
              uint32T bfPageCnt,     /* in */
              bsInMemXtntT *xtnts,   /* in, modified */
              ftxHT parentFtx,       /* in */
              uint32T *allocPageCnt  /* out */
              );

static
statusT
alloc_stg (
           bfAccessT *bfap,           /* in */
           uint32T bfPageOffset,      /* in */
           uint32T bfPageCnt,         /* in */
           vdIndexT bfVdIndex,        /* in */
           bsInMemXtntMapT *xtntMap,  /* in */
           bsAllocHintT alloc_hint,   /* in */
           ftxHT parentFtx,           /* in */
           int updateDisk,            /* in */
           uint32T *allocPageCnt      /* out */
           );

static
statusT
alloc_append_stg (
                  bfAccessT *bfap,           /* in */
                  uint32T bfPageOffset,      /* in */
                  uint32T bfPageCnt,         /* in */
                  vdIndexT bfVdIndex,        /* in */
                  bsInMemXtntMapT *xtntMap,  /* in */
                  uint32T mapIndex,          /* in */
                  bsAllocHintT alloc_hint,   /* in */
                  ftxHT parentFtx,           /* in */
                  int updateDisk,            /* in */
                  uint32T *allocPageCnt      /* out */
                  );

static
statusT
get_first_mcell (
                 bfTagT bfSetTag,       /* in */
                 bfAccessT *bfap,       /* in */
                 uint32T bfPageCnt,     /* in */
                 ftxHT parentFtx,       /* in */
                 vdIndexT *retVdIndex,  /* out */
                 bfMCIdT *retMcellId    /* out */
                 );

static
statusT
alloc_hole_stg (
                bfAccessT *bfap,           /* in */
                uint32T bfPageOffset,      /* in */
                uint32T bfPageCnt,         /* in */
                vdIndexT bfVdIndex,        /* in */
                bsInMemXtntMapT *xtntMap,  /* in */
                uint32T mapIndex,          /* in */
                uint32T descIndex,         /* in */
                bsAllocHintT alloc_hint,   /* in */
                ftxHT parentFtx,           /* in */
                int updateDisk,            /* in */
                uint32T *allocPageCnt      /* out */
                );

static
statusT
extend_skip (
             int *skipCnt,           /* in/out */
             vdIndexT **skipVdIndex  /* in/out */
             );

statusT
alloc_from_one_disk (
                     bfAccessT *bfap,                 /* in */
                     uint32T bfPageOffset,            /* in */
                     uint32T bfPageCnt,               /* in */
                     int bfPageSize,                  /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     bsAllocHintT *alloc_hint,        /* in/out */
                     ftxHT parentFtx,                 /* in */
                     uint32T *reservedPagesCount,     /* in/out */
                     uint32T *reservedPagesLocation,  /* in/out */
                     uint32T *allocPageCnt            /* out */
                     );

static statusT
xfer_stg_to_bf (
                bfAccessT *bfap,
                ulong_t   bfPageOffset,
                ulong_t   bfPageCnt,
                uint32T   startblk,
                vdIndexT  vdIndex,
                ftxHT     parentFtx
               );

static statusT
xfer_stg_to_xtnts ( bfAccessT        *bfap,
                    bsInMemXtntMapT  *xtntMap,
                    ulong_t          bfPageOffset,
                    ulong_t          bfPageCnt,
                    uint32T          startblk,
                    vdIndexT         vdIndex,
                    ftxHT            parentFtx);

static statusT
xfer_append_stg (
                 bfAccessT       *bfap,
                 bsInMemXtntMapT *xtntMap,
                 ulong_t         bfPageOffset,
                 ulong_t         bfPageCnt,
                 uint32T         startblk,
                 vdIndexT        vdIndex,
                 ftxHT           parentFtx
                );

static statusT
xfer_hole_stg (
               bfAccessT       *bfap,
               bsInMemXtntMapT *xtntMap,
               uint32T         mapIndex,
               uint32T         descIndex,
               ulong_t         bfPageOffset,
               ulong_t         bfPageCnt,
               uint32T         startblk,
               vdIndexT        vdIndex,
               ftxHT           parentFtx
              );

static statusT
xfer_stg_on_one_disk (
                      bfAccessT       *bfap,
                      bfTagT          bfSetTag,
                      bsInMemXtntMapT *xtntMap,
                      ulong_t         bfPageOffset,
                      ulong_t         bfPageCnt,
                      ulong_t         startBlk,
                      vdIndexT        vdIndex,
                      ftxHT           parentFtx
                     );


static statusT
xfer_stg_in_one_subxtnt (
                         bsInMemSubXtntMapT *subXtntMap,
                         int                bfPageSize,
                         ulong_t            bfPageOffset,
                         ulong_t            bfPageCnt,
                         uint32T            blkOff
                        );

static
statusT
make_perm_hole( bfAccessT *cloneap,
                uint32T page,
                ftxHT ftxH);

static
statusT
append_perm_hole( bfAccessT *bfap,
                  bsInMemXtntMapT *xtntMap,
                  uint32T holeStart,
                  uint32T holeEnd,
                  int updateFlg,
                  ftxHT ftxH);

static
statusT
insert_perm_hole( bfAccessT *bfap,
                  bsInMemXtntMapT *xtntMap,
                  bsInMemXtntDescIdT xtntDescId,
                  uint32T holeStart,
                  uint32T holeEnd,
                  int updateFlg,
                  ftxHT ftxH);

static
statusT
add_hole_extent( bfAccessT *bfap,
                 bsInMemXtntMapT *xtntMap,
                 uint32T holeStart,
                 uint32T holeEnd,
                 int updateFlg,
                 ftxHT ftxH);

static
statusT
new_sub( bfAccessT *bfap,
         bsInMemXtntMapT *xtntMap,
         ftxHT ftxH);

static statusT
cp_stg_alloc_in_one_mcell(
                           bfAccessT *bfap,                  /* in */
                           uint32T bfPageOffset,             /* in */
                           uint32T bfPageCnt,                /* in */
                           bsInMemSubXtntMapT *subXtntMap,   /* in */
                           ftxHT *ftxHp,                     /* in */
                           uint32T *allocPageCnt,           /* out */
                           uint64T dstBlkOffset,            /* in */
                           bsAllocHintT alloc_hint);        /* in */

static statusT
cp_hole_alloc_in_one_mcell(
                      uint32T bfPageOffset,           /* in  start here */
                      uint32T bfPageCnt,              /* in  requested length */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      uint32T *allocPageCnt           /* out. this much added */
                      );

static statusT
add_extent(
            bfAccessT *bfap,                 /* in */
            bsInMemSubXtntMapT *subXtntMap,  /* in/contents modified */
            uint32T *xIp,                    /* in/out */
            uint32T pgOff,                   /* in */
            uint32T blkOff,                  /* in */
            uint32T pgCnt,                   /* in */
            ftxHT *ftxHp,                    /* in/out */
            bsXtntT *termDescp,              /* out */
            int *pinCntp);                   /* in/out */

static statusT
stg_alloc_one_xtnt(
                    bfAccessT *bfap,           /* in */
                    vdT *vd,                   /* in */
                    uint32T bfPageCnt,         /* in */
                    uint64T dstBlkOffset,      /* in */
                    bsAllocHintT alloc_hint,   /* in */
                    ftxHT parentFtx,           /* in */
                    uint32T *allocPageCntp,    /* out */
                    uint32T *allocBlkOffp,     /* in/out */
                    int *pinCntp);             /* in/out */

static
statusT
scan_xtnt_map_list (
                    bfAccessT *bfap,               /* in */
                    uint32T pageOffset,            /* in */
                    bsInMemXtntMapT *xtntMapList,  /* in */
                    uint32T blkDescCnt,            /* in */
                    blkDescT *blkDescList,         /* in */
                    uint32T *mappedCnt             /* out */
                    );

static
statusT
remove_stg (
            bfAccessT *bfap,       /* in */
            uint32T bfPageOffset,  /* in */
            uint32T bfPageCnt,     /* in */
            bsInMemXtntT *xtnts,   /* in, modified */
            ftxHT parentFtx,       /* in */
            uint32T *delPageCnt,   /* out */
            uint32T *delCnt,       /* out */
            mcellPtrT **delList    /* out */
            );

static
statusT
dealloc_stg (
             bfAccessT *bfap,           /* in */
             uint32T bfPageOffset,      /* in */
             uint32T bfPageCnt,         /* in */
             bsInMemXtntMapT *xtntMap,  /* in */
             int xferFlg,               /* in */
             ftxHT parentFtx,           /* in */
             uint32T *delPageCnt,       /* out */
             vdIndexT *delVdIndex,      /* out */
             bfMCIdT *delMcellId        /* out */
             );

static
statusT
clip_del_xtnt_map (
                   bfAccessT *bfap,           /* in */
                   uint32T bfPageOffset,      /* in */
                   uint32T bfPageCnt,         /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   ftxHT parentFtx            /* in */
                   );

static
void
merge_xtnt_maps (
                 bsInMemXtntT *xtnts  /* in */
                 );

static
void
merge_sub_xtnt_maps (
                     bsInMemXtntMapT *xtntMap  /* in */
                     );

static
void
unload_sub_xtnt_maps (
                      bsInMemXtntMapT *xtntMap  /* in */
                      );

#ifdef ADVFS_SMP_ASSERT
static
int
lock_rwholder(
             lock_t lock                       /* in */
             );
#endif

typedef struct addStgUndoRec
{
    bfSetIdT setId;
    bfTagT tag;
    uint32T nextPage;
    uint32T pageOffset;
    uint32T pageCnt;
} addStgUndoRecT;

/*
 * add_stg_undo
 *
 * This function acquires the locks needed to synchronize the undo of
 * the on-disk and in-memory extent maps when an add storage operation
 * is undone.  This routine also invalidates the in-memory extent maps
 * so that they are re-loaded from the on-disk map when they are next
 * referenced.
 *
 * See the stg_add_stg() function header NOTE concerning sub-transactions.
 *
 * FIX - sync with migrate.
 */

void
add_stg_undo (
              ftxHT ftxH,
              int size,
              void *address
              )
{
    bfAccessT *bfap;
    bfSetT *bfSetp;
    domainT *domain;
    statusT sts;
    addStgUndoRecT *undoRec;
    bsInMemXtntMapT *xtntMap;
    bsInMemXtntT *xtnts;
    int bfs_open = 0;              /* true if bitfile set was opened here */
    struct vnode *nullvp = NULL;
    int v4bmt_or_rbmt;
    int this_is_v3bmt;

    /* This code used to just return when the domain was not marked
     * active. This is a problem since a directory could have run a
     * prior undo routine (i.e. fs_insert_undo) which causes the
     * in-mem extent maps to be read in. If this is the case then it
     * is imperative that we fix them since shortly after this the on-disk
     * MCELLS will be changed and we will have a mismatch.
     */

    /*
     * NOTE:  This assumes single threaded recovery.  If this assumption
     * does not hold, this code needs to acquire the on-disk and in-memory
     * extent map locks so that add storage sub-transactions are correctly
     * synchronized.  */
    domain = ftxH.dmnP;

    undoRec = (addStgUndoRecT *)address;

    if ( !BS_UID_EQL(undoRec->setId.domainId, domain->domainId) ) {
        if ( (domain->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRec->setId.domainId, domain->dualMountId)) ) {
            undoRec->setId.domainId = domain->domainId;
        }
        else {
            ADVFS_SAD0("add_stg_undo: domainId mismatch");
        }
    }

    v4bmt_or_rbmt = RBMT_THERE(domain) &&
                    BS_BFTAG_RSVD(undoRec->tag) &&
                    (BS_BFTAG_RBMT(undoRec->tag) || BS_IS_TAG_OF_TYPE( undoRec->tag, BFM_BMT));
    this_is_v3bmt = !RBMT_THERE(domain) &&
                    BS_BFTAG_RSVD(undoRec->tag) &&
                    BS_IS_TAG_OF_TYPE(undoRec->tag,BFM_BMT_V3);

    if(v4bmt_or_rbmt || this_is_v3bmt) {
        /* If this is undoing of storage of either the BMT or the RBMT then
         * we want to skip this routine. We have already opened the RBMT/BMT
         * and read in the extent maps. These may not be totally correct since
         * we are in the middle of recovery but we should only be accessing those
         * parts of the RBMT/BMT that have valid extents (since this undo is because
         * of an extension). At this point we have not actually undone the storage
         * or fixed up the on-disk extent maps. We can not just pitch the
         * extent maps as we would for a regular file, since the subsequent undo
         * routines will need to access the RBMT/BMT. So we must keep going with
         * the extens we have until the on-disk xtnts are corrected then we can
         * force them to be re-read in.
         */

        goto HANDLE_EXCEPTION;
    }

    if (domain->state != BFD_ACTIVATED) {
        /* We are in recovery, must open the bitfile set.  Remember that we
         * must close this on the way out.
         */
        sts = rbf_bfs_open (&bfSetp, undoRec->setId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"add_stg_undo rbf_bfs_access error");
            goto HANDLE_EXCEPTION;
        }
        bfs_open = 1;
    } else {
        /* Just lookup the bitfile set handle.  */
        bfSetp = bs_bfs_lookup_desc( undoRec->setId );
        if ( bfSetp == NULL ) {
            /* If a set create fails due to ENO_MORE_BLKS from */
            /* fs_create_file_set => create_root_file => rbf_add_stg */
            /* then there will be no bfSetp. This undo routine is */
            /* to fix the in-memory bfap nextPage, allocPageCnt and to */
            /* invalidate cachec UBC pages. Since the set creation failed, */
            /* the set does not exist and the files don't exist. There is */
            /* no in-memory cleanup to do. */

            goto HANDLE_EXCEPTION;
        }
        MS_SMP_ASSERT(bfSetp->bfSetMagic == SETMAGIC);
    }

    sts = bs_access (&bfap, undoRec->tag, bfSetp, ftxH, 0, NULLMT, &nullvp);
    if (sts != EOK) {
        if (sts != ENO_SUCH_TAG) {
            domain_panic(ftxH.dmnP,"add_stg_undo bs_access error %d",sts);
        }
            
        /* else
         * We must allow for a failed access on a file that was removed.
         * A file could have been deleted but held open and storage added 
         * to it. At recovery we will remove the file and then attempt to
         * run this undo routine. The storage has all been cleaned up the
         * purpose of this undo routine is to make sure the in memory extent
         * maps are correct. Since the file is gone this undo can be skipped.
         */
        goto HANDLE_EXCEPTION;
    }

    XTNMAP_LOCK_WRITE( &bfap->xtntMap_lk );

    xtnts = &(bfap->xtnts);

    if (xtnts->validFlag) {

        /*
         * The bitfile has valid in-memory extent maps so we must invalidate
         * them and re-load from the on-disk map at the next reference.
         */

        switch (xtnts->type)  {

          case BSXMT_APPEND:

            imm_delete_xtnt_map (xtnts->xtntMap);
            xtnts->xtntMap = NULL;

            break;

          case BSXMT_STRIPE:

            str_delete_stripe_hdr (xtnts->stripeXtntMap);
            xtnts->stripeXtntMap = NULL;

            break;

          default:
            ADVFS_SAD0("add_stg_undo: bad extent map type");

        }  /* end switch */

        xtnts->validFlag = 0;
    }

    xtnts->allocPageCnt -= undoRec->pageCnt;
    bfap->nextPage = undoRec->nextPage;
    XTNMAP_UNLOCK(&bfap->xtntMap_lk);

    /*
     * Migrate and this function must synchronize so that migrate does not
     * have any pages ref'd or pin'd when this function is called.
     */
    /* block migrate */
    if ( undoRec->pageCnt ) {
        bs_invalidate_pages(bfap, undoRec->pageOffset, undoRec->pageCnt, 0);
    }
    /* unblock migrate */

    sts = bs_close(bfap, 0);
    if (sts != EOK) {
      domain_panic(ftxH.dmnP,"add_stg_undo --- bs_close failed ---");
    }

HANDLE_EXCEPTION:
    if ( bfs_open ) {
        bs_bfs_close (bfSetp, ftxH, BFS_OP_DEF);
    }

    return;

}  /* end add_stg_undo */

typedef struct rmStgUndoRec
{
    bfSetIdT setId;
    bfTagT tag;
    uint32T nextPage;
    uint32T pageCnt;
} rmStgUndoRecT;

/*
 * remove_stg_undo
 *
 * This function is necessary to update the in-memory xtnt maps
 * after failing the removal of storage. The on-disk xtnt maps
 * have changed and this needs to be reflected in-memory.
 * This is true for both recovery and runtime. 
 *
 * NOTE: There is a missing piece to this routine. If this routine
 * is called any pin records from prior root transactions may have
 * been ignored, resulting in data corruption. This routines needs 
 * a counter part in the recovery code, to save the redo records that
 * were skipped and then reapply them at this point. This is only
 * necessary if the file is marked FTX_DATA_SAFTEY
 */

void
remove_stg_undo (
              ftxHT ftxH,
              int size,
              void *address
              )
{
    bfAccessT *bfap;
    bfSetT *bfSetp;
    domainT *domain;
    statusT sts;
    rmStgUndoRecT undoRec;
    bsInMemXtntMapT *xtntMap;
    bsInMemXtntT *xtnts;
    extern ftxLkT BfSetTblLock;
    int bfs_open = 0;            /* true if bitfile set was opened here */
    struct vnode *nullvp = NULL;

    domain = ftxH.dmnP;

    bcopy(address,&undoRec,size);

    if ( !BS_UID_EQL(undoRec.setId.domainId, domain->domainId) ) {
        if ( (domain->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRec.setId.domainId, domain->dualMountId)) ) {
            undoRec.setId.domainId = domain->domainId;
        }
        else {
            domain_panic(ftxH.dmnP,"remove_stg_undo: domainId mismatch");
            goto HANDLE_EXCEPTION;
        }
    }

    if (domain->state != BFD_ACTIVATED) {
        /* We are in recovery, must open the bitfile set.  Remember that we
         * must close this on the way out.
         */
        sts = rbf_bfs_open (&bfSetp, undoRec.setId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"remove_stg_undo rbf_bfs_access error");
            goto HANDLE_EXCEPTION;
        }
        bfs_open = 1;
    } else {
        /* Just lookup the bitfile set handle.  */
        bfSetp = bs_bfs_lookup_desc( undoRec.setId );
        MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    }

    sts = bs_access (&bfap, undoRec.tag, bfSetp, ftxH, 0, NULLMT, &nullvp);
    if (sts != EOK) {
        domain_panic(ftxH.dmnP,"remove_stg_undo --- bs_access failed ---");
        goto HANDLE_EXCEPTION;
    }

    XTNMAP_LOCK_WRITE( &bfap->xtntMap_lk );

    xtnts = &(bfap->xtnts);

    if (xtnts->validFlag) {

        /*
         * The bitfile has valid in-memory extent maps so we must invalidate
         * them.  They are re-loaded from the on-disk map at the next reference.
         */

        switch (xtnts->type)  {

          case BSXMT_APPEND:

            imm_delete_xtnt_map (xtnts->xtntMap);
            xtnts->xtntMap = NULL;

            break;

          case BSXMT_STRIPE:

            str_delete_stripe_hdr (xtnts->stripeXtntMap);
            xtnts->stripeXtntMap = NULL;

            break;

          default:
            domain_panic(ftxH.dmnP,"remove_stg_undo: Bad xtnts->type");
            goto HANDLE_EXCEPTION;

        }  /* end switch */

        xtnts->validFlag = 0;
    }

    xtnts->allocPageCnt += undoRec.pageCnt;
    bfap->nextPage = undoRec.nextPage;

    if (domain->state != BFD_ACTIVATED) 
    {
        /* I'm not sure why we need to do
         * this in recovery. I know that it fixed a problem
         * but it seems like its the wrong thing to do !
         * Something to be investigated later
         */

        XTNMAP_UNLOCK(&bfap->xtntMap_lk);

        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if (sts != EOK) {

            domain_panic(ftxH.dmnP,
                         "remove_stg_undo x_load_inmem_xtnt_map failed ---");
            goto HANDLE_EXCEPTION;
        }
    }
        
    XTNMAP_UNLOCK(&bfap->xtntMap_lk);

    /*
     * Note:  It is ok to close the bitfile with locks held.  The
     * code that determines if an access structure can be recycled
     * checks if any locks are locked.  If one is, the access structure
     * is not recycled.
     */

    sts = bs_close(bfap, 0);
    if (sts != EOK) {

      domain_panic(ftxH.dmnP,"remove_stg_undo --- bs_close failed ---");
      goto HANDLE_EXCEPTION;
    }

HANDLE_EXCEPTION:
    if ( bfs_open ) {
        bs_bfs_close (bfSetp, ftxH, BFS_OP_DEF);    /* releases BfSetTblLock */
        bfs_open = 0;
    }

    return;

}  /* end add_stg_undo */


/*
 * init_bs_stg_opx
 *
 * This function registers the storage code as transaction agents.
 */

init_bs_stg_opx ()
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_STG_ADD_V1,
                             &add_stg_undo,
                             NULL
                             );
    if (sts != EOK) {
        ADVFS_SAD1("init_bs_stg_opx(0):register failure", (long)BSERRMSG(sts));
    }

    sts = ftx_register_agent(
                             FTA_BS_STG_REMOVE_V1,
                             &remove_stg_undo,  /* undo */
                             NULL   /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1("init_bs_stg_opx(1):register failure", (long)BSERRMSG(sts));
    }

} /* end init_bs_stg_opx */


/*
 * bs_add_overlapping_stg
 *
 * This function allocates storage to a bitfile.  If the requested page range
 * overlaps other allocations, only the pages from the page offset to the first
 * overlap are allocated.
 */

statusT
bs_add_overlapping_stg(
                       bfAccessT *bfap,           /* in */
                       unsigned long pageOffset,  /* in */
                       unsigned long pageCnt,     /* in */
                       uint32T *allocPageCnt      /* out */
                       )
{
    statusT sts;

    MIGTRUNC_LOCK_READ( &(bfap->xtnts.migTruncLk) );
    sts = rbf_add_overlapping_stg (
                                    bfap,
                                    pageOffset,
                                    pageCnt,
                                    FtxNilFtxH,
                                    allocPageCnt
                                    );
    MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );
    return sts;
} /* end bs_add_overlapping_stg */


/*
 * rbf_add_overlapping_stg
 *
 * This function allocates storage starting at the specified page offset
 * for the specified number of pages.    If the requested page range
 * overlaps other allocations, only the pages from the page offset to the first
 * overlap are allocated.  The bitfile is specified by the access handle.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * FIX - Do we need this function?  Can add storage be a part of a larger
 * transaction?  What happens when the transaction doesn't complete for
 * a long time?  Do we block other add storage requests that are not part
 * of the root transaction?
 */

statusT
rbf_add_overlapping_stg(
                        bfAccessT *bfap,            /* in */
                        unsigned long pageOffset,   /* in */
                        unsigned long pageCnt,      /* in */
                        ftxHT parentFtx,            /* in */
                        uint32T *allocPageCnt       /* out */
                        )
{
    statusT sts;

    /* Make sure that we catch all the rogue add/remove stg places that
     * don't take the migtrunc lock.
     */
    MS_SMP_ASSERT(lock_rwholder(&(bfap->xtnts.migTruncLk)));

    if (VD_HTOP(bfap->primVdIndex, bfap->dmnP)->bmtp == bfap) {
        /*
         * Can't add storage to the bmt via this interface.
         */
        return ENOT_SUPPORTED;
    }

    return stg_add_stg (
                        parentFtx,
                        bfap,
                        pageOffset,
                        pageCnt,
                        TRUE,
                        allocPageCnt
                        );
} /* end rbf_add_overlapping_stg */


/*
 * rbf_add_overlapping_clone_stg
 *
 * This function allocates storage starting at the specified page offset
 * for the specified number of pages.    If the requested page range
 * overlaps other allocations, only the pages from the page offset to the first
 * overlap are allocated.  The bitfile is specified by the access handle.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * This function is used to add storage to clone bitfiles (it will not
 * do any copy-on-write processing.  It is meant to be called only from
 * bs_cow_pg().
 *
 * FIX -What happens when the transaction doesn't complete for
 * a long time?  Do we block other add storage requests that are not part
 * of the root transaction?
 */
/*
 * "holepg" is the first page of a hole in the original file.
 * The clone will get a permanent hole to match the original's hole.
 * To not add a hole set holePg to zero & pageCnt != 0.
 *
 * rbf_add_overlapping_clone_stg makes up to two modifications to the
 * in memory extent map before it modifies the disk.
 * The two changes are:
 * 1) Add cloned pages (add_stg)
 * 2) Add a permanent hole in the clone (extents but no storage)
 *
 * After add_stg has modified the extents, a set of the subextents
 * (origStart through origEnd) need to be replaced by updateStart
 * through updateEnd. If make_perm_hole must modfiy an already modified
 * subextent, it must make copies of updateStart through updateEnd and
 * change updateStart and updateEnd to point to the new copies.
 * This leaves an island of unused subextents from validCnt through
 * updateStart-1.
 */

statusT
rbf_add_overlapping_clone_stg(
                                bfAccessT *bfap,       /* in */
                                uint32T pageOffset,    /* in */
                                uint32T reqPageCnt,       /* in */
                                uint32T holePg,        /* in */
                                ftxHT parentFtx,       /* in */
                                uint32T *allocPageCnt  /* out */
                                )
{
    int failFtxFlag = 0;
    ftxHT ftxH;
    uint32T grantedPageCnt = 0;
    statusT sts;
    addStgUndoRecT undoRec;
    bsInMemXtntT *xtnts;
    int xtntMap_locked = 0;

    MS_SMP_ASSERT(!BS_BFTAG_RSVD(bfap->tag));


    /* Make sure that we catch all the rogue add/remove stg places that
     * don't take the migtrunc lock.
     */
    MS_SMP_ASSERT(lock_rwholder(&(bfap->xtnts.migTruncLk)));

    /*
     * first extent change: add storage
     */
    xtnts = &bfap->xtnts;

    /* If this is a v3 domain allocate a page for the hole.
     * It will be zero-filled
     */
    if (!(CREATE_PERM_HOLE(bfap->dmnP))&&(holePg == pageOffset + reqPageCnt)) {
	reqPageCnt++;
    }
 	
    if ( reqPageCnt ) {

retry:
        /*
         * This transaction ties all of the "add storage" sub transactions
         * together so that if there is an error, this function calls
         * ftx_fail() to back out the on-disk modifications.  Easy as 1-2-3.
         */

        sts = FTX_START_N(FTA_NULL, &ftxH, parentFtx, bfap->dmnP, 0);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        failFtxFlag = 1;

        /* bs_cow_pg opened the clone and loaded the extent map. */
        MS_SMP_ASSERT(xtnts->validFlag != 0);

        FTX_LOCKWRITE(&bfap->mcellList_lk, ftxH)

        /* Need to lock this here; add_stg() can extend the subXtntMap
         * array which involves reallocating the subXtntMap array, copying
         * the old data to the new array,  and then swapping pointers.  
         * Racing threads that have seized the xtntMap lock expect this 
         * pointer to stay sane while walking through the subXtntMap array.
         */
        XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk)
        xtntMap_locked = 1;

        sts = add_stg( bfap,
                       pageOffset,
                       reqPageCnt,
                       TRUE,       /* check for overlap stg */
                       xtnts,
                       ftxH,
                       FALSE,      /* don't write changes to disk */
                       &grantedPageCnt);
        if ( sts == E_VOL_NOT_IN_SVC_CLASS ) {
            /*
             * We get this deep in the bowels of storage allocation.
             * It can happen when we a disk is removed from service
             * while we are allocating storage.
             */
            XTNMAP_UNLOCK(&bfap->xtntMap_lk)
            xtntMap_locked = 0;
            ftx_fail(ftxH);
            goto retry;
        }
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    } else {  /* !reqPageCnt */
        /* No stg to add. Start a ftx for make_perm_hole */
        sts = FTX_START_N(FTA_NULL, &ftxH, parentFtx, bfap->dmnP, 0);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        failFtxFlag = 1;
        FTX_LOCKWRITE(&bfap->mcellList_lk, ftxH)
        XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk)   
        xtntMap_locked = 1;
    }

    *allocPageCnt = grantedPageCnt;

    /*
     * second extent change: add a permanent hole to the clone's extents
     */
    if ( CREATE_PERM_HOLE(bfap->dmnP) && 
         holePg == pageOffset + grantedPageCnt ) {
        /* Add a permanent hole to the clone's extents */
        sts = make_perm_hole(bfap, holePg, ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    } else if ( grantedPageCnt == 0 ) {
        /* No pages added, no permanent hole. Just return. */
        RAISE_EXCEPTION(EOK);
    }

    /*
     * Now write both extent changes to the disk
     */
    if ( xtnts->type == BSXMT_APPEND ) {
        sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                        bfap,
                                        xtnts->xtntMap,
                                        ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    } else {
        /* striped file */
        uint32T stripeIndex;
        uint32T segCnt;
        uint32T segSize;
        bsInMemXtntMapT *xtntMap;

        MS_SMP_ASSERT(xtnts->type == BSXMT_STRIPE);
        segCnt = xtnts->stripeXtntMap->cnt;
        segSize = xtnts->stripeXtntMap->segmentSize;

        for ( stripeIndex = 0; stripeIndex < segCnt; stripeIndex++) {
            xtntMap = xtnts->stripeXtntMap->xtntMap[stripeIndex];
            if ( xtntMap->allocDeallocPageCnt > 0 ) {
                sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                                bfap,
                                                xtntMap,
                                                ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
            }
        }
    }

    /*
     * This function can't fail from this point on.  This is because we
     * modify the in-memory extent maps and the changes cannot be backed out.
     */

    merge_xtnt_maps(xtnts);

    xtnts->allocPageCnt = xtnts->allocPageCnt + grantedPageCnt;

    XTNMAP_UNLOCK(&bfap->xtntMap_lk)
    xtntMap_locked = 0;

    undoRec.nextPage = bfap->nextPage;

    if ( (pageOffset + grantedPageCnt) > bfap->nextPage ) {
        bfap->nextPage = pageOffset + grantedPageCnt;
    }

    bs_bfs_get_set_id( bfap->bfSetp, &undoRec.setId );
    undoRec.tag = bfap->tag;
    undoRec.pageOffset = pageOffset;
    undoRec.pageCnt = grantedPageCnt;
    ftx_done_u(ftxH, FTA_BS_STG_ADD_V1, sizeof (undoRec), &undoRec);
    return sts;

HANDLE_EXCEPTION:
    if ( xtntMap_locked ) {
        XTNMAP_UNLOCK(&bfap->xtntMap_lk)
        xtntMap_locked = 0;
    }

    if ( failFtxFlag != 0 ) {
        ftx_fail(ftxH);
    }

    return sts;
}


/*
 * bs_add_stg
 *
 * This function allocates storage to a bitfile.  If the requested page range
 * overlaps other allocations, no pages are allocated and an error status is
 * returned.
 */

statusT
bs_add_stg(
           bfAccessT *bfap,           /* in */
           unsigned long pageOffset,  /* in */
           unsigned long pageCnt      /* in */
           )
{
    statusT sts;
    
    /* Prevent races with migrate code paths */
    MIGTRUNC_LOCK_READ( &(bfap->xtnts.migTruncLk) );
    sts = rbf_add_stg (bfap, pageOffset, pageCnt, FtxNilFtxH, 1);
    MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );
    return sts;
} /* end bs_add_stg */


/*
 * rbf_add_stg
 *
 * This function allocates storage starting at the specified page offset
 * for the specified number of pages.    If the requested page range
 * overlaps other allocations, no pages are allocated and an error status
 * is returned.  The bitfile is specified by the access handle.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * FIX - Do we need this function?  Can add storage be a part of a larger
 * transaction?  What happens when the transaction doesn't complete for
 * a long time?  Do we block other add storage requests that are not part
 * of the root transaction?
 */

statusT
rbf_add_stg(
            bfAccessT *bfap,            /* in */
            unsigned long pageOffset,   /* in */
            unsigned long pageCnt,      /* in */
            ftxHT parentFtx,            /* in */
            int checkmigtrunclock       /* in */
            )
{
    uint32T allocPageCnt;
    statusT sts;

    /* Make sure that we catch all the rogue add/remove stg places that
     * don't take the migtrunc lock.
     */
    if (checkmigtrunclock) {
      MS_SMP_ASSERT(lock_rwholder(&(bfap->xtnts.migTruncLk)));
    }

    if (VD_HTOP(bfap->primVdIndex, bfap->dmnP)->bmtp == bfap) {
        /*
         * Can't add storage to the bmt via this interface.
         */
        return ENOT_SUPPORTED;
    }

    return stg_add_stg (
                        parentFtx,
                        bfap,
                        pageOffset,
                        pageCnt,
                        FALSE,
                        &allocPageCnt
                        );
} /* end rbf_add_stg */



/*
 * stg_add_stg
 *
 * This function allocates storage starting at the specified page offset
 * for the specified number of pages.  The bitfile is specified by an
 * access pointer.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * Code outside of the bitfile access system must call bs_add_stg(),
 * rbf_add_stg(), bs_add_overlapping_stg() or rbf_add_overlapping_stg() to add
 * storage to a bitfile.
 * Code within the bitfile access system should call stg_add_stg().
 *
 * NOTE: if the add storage operation is a sub-transaction::  If the operation
 * completes successfully, the new storage is exposed before the caller's
 * root transaction completes.  This is because the locks guarding the on-disk
 * and in-memory extent maps are released before returning to the caller.
 * Threads that add storage and threads that read/write the bitfile must
 * synchronize with each other so that the root transaction completes
 * successfully or fails without other threads seeing the new storage.
 *
 * If the bitfile is an original, this routine will use copy-on-write
 * to zero-fill and/or copy pages to the clone if this is a sparse allocation
 * (starting page number is less than highest allocated page).
 *
 * FIX - how does this function interact with shadow and migrate??
 */

#ifdef DEBUG
static int stg_verbose = 0;
#endif

statusT
stg_add_stg (
             ftxHT parentFtx,             /* in */
             bfAccessT *bfap,             /* in */
             unsigned long bfPageOffset,  /* in */
             unsigned long bfPageCnt,     /* in */
             int allowOverlapFlag,        /* in */
             uint32T *allocPageCnt        /* out */
             )
{
    bfSetT *bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */

        return E_READ_ONLY;
    }

    if ( bfSetp->cloneSetp != NULL ) {

        /* The set has been cloned at least once.  Check to
         * see if we need to do any 'copy on write' processing.
         * bs_cow will do something only if this is a sparse allocation.
         */
        bs_cow(bfap, COW_PINPG, bfPageOffset, bfPageCnt, parentFtx);
    }

    return stg_add_stg_no_cow( parentFtx,         /* in */
                               bfap,              /* in */
                               bfPageOffset,      /* in */
                               bfPageCnt,         /* in */
                               allowOverlapFlag,  /* in */
                               allocPageCnt);       /* out */
}


/*
 * stg_add_stg_no_cow
 *
 * This function allocates storage starting at the specified page offset
 * for the specified number of pages.  The bitfile is specified by an
 * access pointer.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * Code outside of the bitfile access system must call bs_add_stg(),
 * rbf_add_stg(), bs_add_overlapping_stg() or rbf_add_overlapping_stg() to add
 * storage to a bitfile.
 * Code within the bitfile access system should call stg_add_stg().
 *
 * NOTE: if the add storage operation is a sub-transaction::  If the operation
 * completes successfully, the new storage is exposed before the caller's
 * root transaction completes.  This is because the locks guarding the on-disk
 * and in-memory extent maps are released before returning to the caller.
 * Threads that add storage and threads that read/write the bitfile must
 * synchronize with each other so that the root transaction completes
 * successfully or fails without other threads seeing the new storage.
 *
 * No copy-on-write processing is done.
 *
 * FIX - how does this function interact with shadow and migrate??
 */

statusT
stg_add_stg_no_cow (
                    ftxHT parentFtx,             /* in */
                    bfAccessT *bfap,             /* in */
                    unsigned long bfPageOffset,  /* in */
                    unsigned long bfPageCnt,     /* in */
                    int allowOverlapFlag,        /* in */
                    uint32T *allocPageCnt        /* out */
                    )
{
    int failFtxFlag = 0;
    ftxHT ftx;
    uint32T pageCnt = 0;
    int retryFlag;
    statusT sts;
    addStgUndoRecT undoRec;
    bsInMemXtntT *xtnts;
    int xtntMap_locked = 0;

#ifdef DEBUG
    if (stg_verbose) {
        printf("stg_add_stg: bfPageOffset=0x%x bfPageCnt=0x%x\n",
               bfPageOffset, bfPageCnt);
    }
#endif

    if (bfPageCnt == 0) {
        /*
         * No storage was requested.
         */
        *allocPageCnt = 0;

        return EOK;
    }

    xtnts = &(bfap->xtnts);

    /*
     * Start a new transaction.  This transaction allocates disk storage and
     * updates the on-disk extent map.  This transaction is necessary because
     * our caller could pass a nil ftx handle.
     */

    /*
     * Discussion with chris on 20-nov-1992 about add storage undo.
     *
     * - add storage must be a sub-transaction because we can't have
     * nested roots.
     * - if add storage is a sub-transaction, the caller does not have
     * to have a sub-transaction that specifies the storage added in
     * case the system crashes.
     * - set a state bit in the access structure that acts as a lock.
     * If the state bit is set, no storage can be added, removed or
     * migrated.  The bit is cleared at root done.  This follows the
     * rule that the storage is not seen until the transaction completes.
     * - The state bit allows add storage to use image undo.  Otherwise,
     * storage could be added, removed or migrated.
     * - What about pins/refs after add storage but before the root completes?
     * If we allow them, somehow have to handle them during undo.
     * - What about pins/refs by threads other than the one that adds storage?
     */

    retryFlag = 1;
    while (retryFlag != 0) {

        /*
         * This transaction ties all of the "add storage" sub transactions
         * together so that if there is an error, this function calls ftx_fail()
         * to back out the on-disk modifications.  Easy as 1-2-3.
         */

        sts = FTX_START_N(FTA_NULL, &ftx, parentFtx, bfap->dmnP, 0);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        failFtxFlag = 1;

        /*
         * Make sure that the in-memory extent maps are valid.
         * Returns with mcellList_lk write-locked.
         */
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /* Add this lock to our transaction list so it can be released
         * when the transaction completes.
         */
        FTX_ADD_LOCK(&(bfap->mcellList_lk), ftx)

        /* Need to lock this before we modify any of the extents or 
         * subextents for this file in add_stg().
         */
        XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )
        xtntMap_locked = 1;

        if (BS_BFTAG_RSVD (bfap->tag) == 0) {

            sts = add_stg( bfap,
                           bfPageOffset,
                           bfPageCnt,
                           allowOverlapFlag,
                           xtnts,
                           ftx,
                           TRUE,
                           &pageCnt);
            if (sts == EOK) {
                retryFlag = 0;
            } else {
                if (sts == E_VOL_NOT_IN_SVC_CLASS) {
                    /*
                     * We get this deep in the bowels of storage allocation.
                     * It can happen when we a disk is removed from service
                     * while we are allocating storage.
                     */
                    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
                    xtntMap_locked = 0;
                    ftx_fail (ftx);
                } else {
                    RAISE_EXCEPTION (sts);
                }
            }
        } else {

            sts = add_rsvd_stg( bfap,
                                bfPageOffset,
                                bfPageCnt,
                                xtnts,
                                ftx,
                                &pageCnt);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            retryFlag = 0;
        }
    }  /* end while */

    if (pageCnt == 0) {
        *allocPageCnt = 0;
        RAISE_EXCEPTION (EOK);
    }

    /*
     * This function can't fail from this point on.  This is because we
     * modify the in-memory extent maps and the changes cannot be backed out.
     */
    merge_xtnt_maps (xtnts);

    xtnts->allocPageCnt = xtnts->allocPageCnt + pageCnt;

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    xtntMap_locked = 0;

    undoRec.nextPage = bfap->nextPage;

    if ((bfPageOffset + pageCnt) > bfap->nextPage) {
        bfap->nextPage = bfPageOffset + pageCnt;
    }

    bs_bfs_get_set_id( bfap->bfSetp, &undoRec.setId);
    undoRec.tag = bfap->tag;
    undoRec.pageOffset = bfPageOffset;
    undoRec.pageCnt = pageCnt;
    ftx_done_u (ftx, FTA_BS_STG_ADD_V1, sizeof (undoRec), &undoRec);

    *allocPageCnt = pageCnt;

    return EOK;

HANDLE_EXCEPTION:
    if ( xtntMap_locked ) {
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
        xtntMap_locked = 0;
    }

    if (failFtxFlag != 0) {
        ftx_fail (ftx);
    }

    return sts;

} /* end stg_add_stg_no_cow */


statusT
stg_add_rbmt_stg (
                  ftxHT parentFtx,             /* in */
                  bfAccessT *rbmtap,           /* in */
                  unsigned long bfPageOffset,  /* in */
                  unsigned long bfPageCnt,     /* in */
                  bfMCIdT mcellId,             /* in */
                  uint32T *allocPageCnt        /* out */
                  )
{
    bsInMemXtntT *xtnts;
    bsInMemXtntMapT *xtntMap;
    uint32T mapIndex, i, pageCnt, temp = 0;
    bsInMemSubXtntMapT *subXtntMap;
    statusT sts;
    bsAllocHintT alloc_hint = BS_ALLOC_FIT;


    /* Note that the extent maps at this point are guaranteed to be
     * up-to-date since they get set up at disk activation, and are
     * modified only thru rbmt extension which is done under an
     * exclusive transaction. The caller must already hold the
     * mcellList_lk
     */

    xtnts = &(rbmtap->xtnts);

    xtntMap = xtnts->xtntMap;

    if ( xtntMap->cnt == 0xffff ) {
        /* This means we have 64K xtra extent records. */
        /* For load_from_xtnt_rec's sake, we can't allow more. */
        return EXTND_FAILURE;
    }

    /*
     * Since we MUST use a new mcell for this allocation (as specified)
     * we need a new subxtntmap to work with.
     */

    i = xtntMap->cnt;

    if (i >= xtntMap->maxCnt) {
        sts = imm_extend_xtnt_map (xtntMap);
        if (sts != EOK) {
            return sts;
        }
    }

    subXtntMap = &(xtntMap->subXtntMap[i]);

    sts = imm_init_sub_xtnt_map( subXtntMap,
                                 bfPageOffset,
                                 0,  /* pageCnt */
                                 rbmtap->primVdIndex,
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS);

    if (sts != EOK) {
        return sts;
    }

    subXtntMap->mcellState = NEW_MCELL;

    /* It would make every path consistent to add the tracking of
     * vd->allocClust here.  But I hate adding code that has no
     * other purpose than cosmetic consistency.  We can't race here
     * with any other threads since we are in an exclusive domain
     * transaction, so everyone else is blocked out of the storage
     * allocation paths.
     */
    sts = alloc_from_one_disk( rbmtap,
                               bfPageOffset,
                               bfPageCnt,
                               rbmtap->bfPageSz,
                               subXtntMap,
                               &alloc_hint,
                               parentFtx,
                               &temp,  /* Not used by this path */
                               &temp,  /* Not used by this path */
                               &pageCnt);
    if (sts != EOK) {
        /*
        ftx_fail (ftxH);
        imm_unload_sub_xtnt_map (subXtntMap);
        */
        return sts;
    }

    /* Since we are adding a subXtntmap to the end of the
     * array we don't need to call merge_xtnt_maps. Also
     * We are not replacing any mcells so set origStart to
     * updateStart.
     */

    xtntMap->origStart = xtntMap->updateStart = xtntMap->cnt;
    xtntMap->origEnd = xtntMap->updateEnd = xtntMap->cnt;

    xtntMap->cnt++;
    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->nextValidPage = xtntMap->subXtntMap[xtntMap->validCnt - 1].pageOffset+1;

    sts = x_update_ondisk_xtnt_map( rbmtap->dmnP,
                                         rbmtap,
                                         xtntMap,
                                         parentFtx);
    if (sts != EOK) {
        return sts;
    }
    
    xtnts->allocPageCnt += pageCnt;

    if ((bfPageOffset + bfPageCnt) > rbmtap->nextPage) {
        rbmtap->nextPage = bfPageOffset + bfPageCnt;
    }

    *allocPageCnt = pageCnt;

    return sts;
}


/*
 * stg_set_alloc_disk
 *
 * This function sets a bitfile extent map's "allocVdIndex" field.  This
 * field is used by the storage code when allocating storage for a striped
 * or a mirrored bitfile.
 */

statusT
stg_set_alloc_disk (
                    bfAccessT *bfap,        /* in */
                    vdIndexT curVdIndex,    /* in */
                    uint32T pageCntNeeded,  /* in */
                    int forceFlag           /* in */
                    )
{
    bsMPgT *bmt;
    int failFtxFlag = 0;
    int foundIndex;
    ftxHT ftxH;
    int i;
    bsMCT *mcell;
    uint32T pageCnt;
    rbfPgRefHT pgPin;
    bsShadowXtntT *shadowRec;
    bsStripeHdrT *stripeHdr;
    statusT sts;
    vdT *vdp = NULL;
    bsInMemXtntMapT *xtntMap;
    NEW_VD_SKIP(vd_skip_list);


    if ( !vd_htop_if_valid(curVdIndex, bfap->dmnP, FALSE, FALSE) ) {
        RAISE_EXCEPTION(EBAD_VDI);
    }

    if (bfap->xtnts.type != BSXMT_STRIPE) {
        RAISE_EXCEPTION (ENOT_SUPPORTED);
    }

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 1);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with mcellList_lk write-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    FTX_ADD_LOCK(&(bfap->mcellList_lk), ftxH)

    /*
     * Find the extent map that matches the caller's specified vdIndex.
     * While we are add it, build the vdIndex skip list.
     */

    stripeHdr = bfap->xtnts.stripeXtntMap;

    foundIndex = stripeHdr->cnt;
    for (i = 0; i < stripeHdr->cnt; i++) {

        if ( curVdIndex == stripeHdr->xtntMap[i]->allocVdIndex &&
             foundIndex == stripeHdr->cnt )
        {
            foundIndex = i;
        }
        if (stripeHdr->xtntMap[i]->allocVdIndex != (vdIndexT) -1) {
            SET_VD_SKIP(vd_skip_list, stripeHdr->xtntMap[i]->allocVdIndex);
        }
    }

    if (foundIndex >= stripeHdr->cnt) {
        RAISE_EXCEPTION (EBAD_VDI);
    }

    pageCnt = 1;
    if (pageCntNeeded != -1) {
        /*
         * The caller has specified the number of pages that it will need.
         * Select the best disk based on this request.
         */
        pageCnt = pageCntNeeded;
    }

    sts = sc_select_vd_for_stg( &vdp,
                                bfap->dmnP,
                                bfap->reqServices,
                                bfap->optServices,
                                0,
                                vd_skip_list,
                                pageCnt * bfap->bfPageSz, /* requestedBlkCnt */
                                bfap->bfPageSz,
                                FALSE); /* don't add to allocClust */
    if (sts != EOK) {
        if (forceFlag != 0) {
            /*
             * The caller wants to change this even though it will make the
             * striped bitfile sub-optimal.  We will move the stripe to
             * one of the remaining volumes.
             */

            EMPTY_VD_SKIP(vd_skip_list);
            if (stripeHdr->xtntMap[foundIndex]->allocVdIndex != (vdIndexT) -1) {
                SET_VD_SKIP(vd_skip_list,
                            stripeHdr->xtntMap[foundIndex]->allocVdIndex);
            }
            sts = sc_select_vd_for_stg( &vdp,
                                bfap->dmnP,
                                bfap->reqServices,
                                bfap->optServices,
                                0,
                                vd_skip_list,
                                pageCnt * bfap->bfPageSz, /* requestedBlkCnt */
                                bfap->bfPageSz,
                                FALSE); /* don't add to allocClust */

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

        } else {
            if (sts == ENO_MORE_BLKS) {
                /*
                 * No more available disks in the service class.
                 */
                sts = E_NOT_ENOUGH_DISKS;
            }
            RAISE_EXCEPTION (sts);
        }
    }

    xtntMap = stripeHdr->xtntMap[foundIndex];

    sts = rbf_pinpg( &pgPin,
                     (void *)&bmt,
                     VD_HTOP(xtntMap->hdrVdIndex, bfap->dmnP)->bmtp,
                     xtntMap->hdrMcellId.page,
                     BS_NIL,
                     ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    mcell = &(bmt->bsMCA[xtntMap->hdrMcellId.cell]);

    shadowRec = bmtr_find (mcell, BSR_SHADOW_XTNTS, bfap->dmnP);
    if (shadowRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    rbf_pin_record( pgPin,
                    &(shadowRec->allocVdIndex),
                    sizeof (shadowRec->allocVdIndex));

    shadowRec->allocVdIndex = vdp->vdIndex;
    xtntMap->allocVdIndex = vdp->vdIndex;

    vd_dec_refcnt(vdp);

    ftx_done_n (ftxH, FTA_BS_STG_SET_ALLOC_DISK_V1);

    return EOK;

HANDLE_EXCEPTION:
    if ( vdp != NULL ) {
        vd_dec_refcnt(vdp);
    }
    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }

    return sts;

}  /* end stg_set_alloc_disk */


/*
 * add_stg
 *
 * This function adds storage to a bitfile.
 */

static
statusT
add_stg (
         bfAccessT *bfap,       /* in */
         uint32T bfPageOffset,  /* in */
         uint32T bfPageCnt,     /* in */
         int allowOverlapFlag,  /* in */
         bsInMemXtntT *xtnts,   /* in, modified */
         ftxHT parentFtx,       /* in */
         int updateDisk,        /* in */
         uint32T *allocPageCnt  /* out */
         )
{
    uint32T holePageCnt;
    uint32T i;
    int mapIndex;
    uint32T pageCnt;
    uint32T pageOffset;
    uint32T segmentCnt;
    uint32T segmentSize;
    statusT sts;
    uint32T totalPageCnt;
    bsInMemXtntMapT *xtntMap;
    vdT *vdp;
    int vdpCnt_bumped = FALSE;

    if (bfPageOffset >= bfap->nextPage) {
        holePageCnt = bfPageCnt;
    } else {
        /*
         * Add storage in the middle of the bitfile.  Determine if
         * we can add some or all of the storage.
         */
        holePageCnt = imm_get_hole_size (bfPageOffset, xtnts);
        if (holePageCnt >= bfPageCnt) {
            holePageCnt = bfPageCnt;
        } else {
            if (allowOverlapFlag == TRUE) {
                if (holePageCnt == 0) {
                    *allocPageCnt = 0;
                    return EOK;
                }
            } else {
                return EALREADY_ALLOCATED;
            }
        }
    }

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        xtntMap = xtnts->xtntMap;
        xtntMap->updateStart = xtntMap->cnt;
        sts = alloc_stg( bfap,
                         bfPageOffset,
                         holePageCnt,
                         -1,  /* any disk */
                         xtntMap,
                         BS_ALLOC_DEFAULT,
                         parentFtx,
                         updateDisk,
                         &totalPageCnt);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        break;

      case BSXMT_STRIPE:

        for (i = 0; i < xtnts->stripeXtntMap->cnt; i++) {
            xtnts->stripeXtntMap->xtntMap[i]->updateStart =
              xtnts->stripeXtntMap->xtntMap[i]->cnt;
        }  /* end for */

        str_calc_page_alloc (bfPageOffset, holePageCnt, xtnts->stripeXtntMap);

        segmentCnt = xtnts->stripeXtntMap->cnt;
        segmentSize = xtnts->stripeXtntMap->segmentSize;

        totalPageCnt = 0;

        i = 0;
        pageOffset = bfPageOffset;
        mapIndex = BFPAGE_TO_MAP (pageOffset, segmentCnt, segmentSize);
        while ((i < segmentCnt) &&
               (xtnts->stripeXtntMap->xtntMap[mapIndex]->allocDeallocPageCnt > 0)) {

            xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];
            /* There could be a race with migrate and rmvol.
             * If there is a vdp, bump its vdRefCnt by 1.
             */
            vdp = vd_htop_if_valid( xtntMap->allocVdIndex,
                                    bfap->dmnP,
                                    TRUE,
                                    FALSE );
            /* If vdp is now NULL, what use to be the preferred
             * allocation disk no longer exists (it may have
             * have been rmvol-ed).  If this is the case, reset
             * xtntMap->allocVdIndex to -1.  The next allocation
             * will reset it to a proper value.  If not, flag that
             * we have raised the vdRefCnt by 1.  We will decrement
             * it after we call alloc_stg().
             */
            if ( vdp != NULL ) {
                vdpCnt_bumped = TRUE;
            } else {
                xtntMap->allocVdIndex = -1;
            }
            sts = alloc_stg (
                             bfap,
                         BFPAGE_TO_XMPAGE (pageOffset, segmentCnt, segmentSize),
                             xtntMap->allocDeallocPageCnt,
                             xtntMap->allocVdIndex,
                             xtntMap,
                             BS_ALLOC_DEFAULT,
                             parentFtx,
                             updateDisk,
                             &pageCnt
                             );

            if ( vdpCnt_bumped ) {
                vd_dec_refcnt( vdp );
                /* reset vdpCnt_bumped. */
                vdpCnt_bumped = FALSE;
            }

            if (sts != EOK) {
                /* FIX - if run out of blocks, go to new disk if possible */
                RAISE_EXCEPTION (sts);
            }
            totalPageCnt = totalPageCnt + pageCnt;
            i++;
            pageOffset = NEXT_SEGMENT_PAGE (pageOffset, segmentSize);
            mapIndex = NEXT_MAP (mapIndex, segmentCnt);

        }  /* end while */

        break;

      default:
        ADVFS_SAD0("add_stg(1): bad extent map type");

    }  /* end switch */

    *allocPageCnt = totalPageCnt;

    return sts;

HANDLE_EXCEPTION:
    /*
     * Run through the updated extent maps and unload the sub extent maps.
     */

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        /* unloaded by alloc_stg() */
        break;

      case BSXMT_STRIPE:

        for (i = 0; i < xtnts->stripeXtntMap->cnt; i++) {
            unload_sub_xtnt_maps (xtnts->stripeXtntMap->xtntMap[i]);
        }  /* end for */

        break;

      default:
        ADVFS_SAD0("add_stg(2): bad extent map type");

    }  /* end switch */

    return sts;

}  /* end add_stg */


/*
 * add_rsvd_stg
 *
 * This function adds storage to a reserved bitfile (bmt, root tag directory,
 * log and sbm).,
 */

static
statusT
add_rsvd_stg (
              bfAccessT *bfap,       /* in */
              uint32T bfPageOffset,  /* in */
              uint32T bfPageCnt,     /* in */
              bsInMemXtntT *xtnts,   /* in, modified */
              ftxHT parentFtx,       /* in */
              uint32T *allocPageCnt  /* out */
              )
{
    uint32T pageCnt;
    statusT sts;
    bsInMemXtntMapT *xtntMap;

    /*
     * All rsvd bitfile extents must be on the same vd.
     */

    if (bfap->xtnts.type != BSXMT_APPEND) {
        ADVFS_SAD0("add_rsvd_stg: bad reserved bitfile extent type");
    }
    if (bfPageOffset != bfap->nextPage) {
        ADVFS_SAD0("add_rsvd_stg: sparse reserved bitfile not supported");
    }

    xtntMap = xtnts->xtntMap;
    xtntMap->updateStart = xtntMap->cnt;
    sts = alloc_stg( bfap,
                     bfPageOffset,
                     bfPageCnt,
                     bfap->primVdIndex,
                     xtntMap,
                     BS_ALLOC_RSVD,
                     parentFtx,
                     TRUE,
                     &pageCnt);
    if (sts == EOK) {
        *allocPageCnt = pageCnt;
    }

    return sts;

}  /* end add_rsvd_stg */


/*
 * alloc_stg
 *
 * This function allocates storage and saves the information in the extent map.
 */

static
statusT
alloc_stg (
           bfAccessT *bfap,           /* in */
           uint32T bfPageOffset,      /* in */
           uint32T bfPageCnt,         /* in */
           vdIndexT bfVdIndex,        /* in */
           bsInMemXtntMapT *xtntMap,  /* in */
           bsAllocHintT alloc_hint,   /* in */
           ftxHT parentFtx,           /* in */
           int updateDisk,            /* in */
           uint32T *allocPageCnt      /* out */
           )
{
    uint32T descIndex;
    uint32T holePageCnt;
    uint32T mapIndex;
    uint32T pageCnt;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt = 0;

    sts = imm_page_to_sub_xtnt_map (bfPageOffset, xtntMap, 0, &mapIndex);
    switch (sts) {

      case E_PAGE_NOT_MAPPED:
        /*
         * The page is not mapped by any sub extent maps and its page offset
         * is after the last page mapped by the last valid sub extent map.
         *
         * Append the page after the last page mapped by the last subextent map.
         */
        sts = alloc_append_stg( bfap,
                                bfPageOffset,
                                bfPageCnt,
                                bfVdIndex,
                                xtntMap,
                                xtntMap->validCnt - 1,  /* mapIndex */
                                alloc_hint,
                                parentFtx,
                                updateDisk,
                                &totalPageCnt);
        break;

      case EOK:
        /*
         * The page is mapped by a sub extent map.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapIndex]);
        sts = imm_page_to_xtnt( bfPageOffset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_PAGE_HOLE */
                                &descIndex);
        switch (sts) {

          case E_PAGE_HOLE:
            /*
             * The page is mapped by the sub extent map and the extent
             * descriptor describes a hole.
             */
            sts = alloc_hole_stg( bfap,
                                  bfPageOffset,
                                  bfPageCnt,
                                  bfVdIndex,
                                  xtntMap,
                                  mapIndex,
                                  descIndex,
                                  alloc_hint,
                                  parentFtx,
                                  updateDisk,
                                  &totalPageCnt);
            break;

          case EOK:
            /*
             * The page is mapped by the sub extent map and the extent
             * descriptor describes an existing extent.
             */
            ADVFS_SAD0("alloc_stg: page already allocated");
            break;

          default:
            return sts;

        }  /* end switch */

        break;

      default:
        return sts;
    }  /* end switch */

    if (sts == EOK) {
        *allocPageCnt = totalPageCnt;
    }

    return sts;

}  /* end alloc_stg */


/*
 * alloc_append_stg
 *
 * This function allocates storage and appends it after the last extent.
 */

static
statusT
alloc_append_stg (
                  bfAccessT *bfap,           /* in */
                  uint32T bfPageOffset,      /* in */
                  uint32T bfPageCnt,         /* in */
                  vdIndexT bfVdIndex,        /* in */
                  bsInMemXtntMapT *xtntMap,  /* in */
                  uint32T mapIndex,          /* in */
                  bsAllocHintT alloc_hint,   /* in */
                  ftxHT parentFtx,           /* in */
                  int updateDisk,            /* in */
                  uint32T *allocPageCnt      /* out */
                  )
{
    bfSetT *bfSetDesc;
    uint32T i;
    uint32T pageCnt;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;


    MS_SMP_ASSERT( lock_holder(&bfap->xtntMap_lk.lock) );

    xtntMap->origStart = mapIndex;
    xtntMap->origEnd = mapIndex;

    bfSetDesc = bfap->bfSetp;

    /*
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */

    i = xtntMap->cnt;
    if (i >= xtntMap->maxCnt) {
        sts = imm_extend_xtnt_map (xtntMap);
        if (sts != EOK) {
            return sts;
        }
    }

    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtntMap,
                                 &xtntMap->subXtntMap[mapIndex],
                                 &xtntMap->subXtntMap[i]);
    if (sts != EOK) {
        return sts;
    }
    xtntMap->cnt++;

    subXtntMap = &(xtntMap->subXtntMap[i]);
    if ((subXtntMap->cnt == 1) &&
        (i == 1) &&
        (subXtntMap->type == BSR_SHADOW_XTNTS)) {
        /*
         * This is a normal bitfile and it does not have any storage
         * allocated to it.  If needed, allocate an mcell for the sub
         * extent map before allocating storage.
         *
         * If the bitfile has never had any storage allocated to it,
         * the sub extent map's mcell id is nil.  Otherwise, if all
         * the storage has been removed from the bitfile, the sub extent
         * map's mcell id is valid.
         */
        if ((subXtntMap->mcellId.page == bsNilMCId.page) &&
            (subXtntMap->mcellId.cell == bsNilMCId.cell)) {

            if (bfVdIndex == (vdIndexT)(-1)) {
                sts = get_first_mcell( bfSetDesc->dirTag,
                                       bfap,
                                       bfPageCnt,
                                       parentFtx,
                                       &(subXtntMap->vdIndex),
                                       &(subXtntMap->mcellId));
            } else {
                subXtntMap->vdIndex = bfVdIndex;
                sts = stg_alloc_new_mcell( bfap,
                                           bfSetDesc->dirTag,
                                           bfap->tag,
                                           bfVdIndex,
                                           parentFtx,
                                           &(subXtntMap->mcellId),
                                           FALSE);
            }
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            subXtntMap->mcellState = NEW_MCELL;
            xtntMap->hdrVdIndex = subXtntMap->vdIndex;
            xtntMap->hdrMcellId = subXtntMap->mcellId;
        }
    }

    if (bfVdIndex == (vdIndexT)(-1)) {
        sts = stg_alloc_from_svc_class( bfap,
                                        bfap->reqServices,
                                        bfap->optServices,
                                        bfSetDesc->dirTag,
                                        bfap->tag,
                                        bfPageOffset,
                                        bfPageCnt,
                                        bfap->bfPageSz,
                                        xtntMap,
                                        &alloc_hint,
                                        parentFtx,
                                        &pageCnt);
    } else {
        uint32T fileClust;
        vdT *vdp;

        vdp = VD_HTOP(bfVdIndex, bfap->dmnP);
        if (vdp->stgCluster == BS_CLUSTSIZE) {
             fileClust = (bfPageCnt * bfap->bfPageSz) / BS_CLUSTSIZE;
        } else {
             MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
             fileClust = (bfPageCnt * bfap->bfPageSz) / BS_CLUSTSIZE_V3;
        }

        SC_TBL_LOCK(bfap->dmnP);
        vdp->allocClust += fileClust;
        SC_TBL_UNLOCK(bfap->dmnP);

        sts = stg_alloc_from_one_disk( bfap,
                                       bfVdIndex,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       bfPageOffset,
                                       bfPageCnt,
                                       bfap->bfPageSz,
                                       xtntMap,
                                       &alloc_hint,
                                       parentFtx,
                                       &pageCnt);

        SC_TBL_LOCK(bfap->dmnP);
        vdp->allocClust -= fileClust;
        SC_TBL_UNLOCK(bfap->dmnP);
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if ( pageCnt != bfPageCnt && !BF_IS_BMT(bfap) ) {
        RAISE_EXCEPTION (ENO_MORE_BLKS);
    }

    /*
     * try to merge contiguous extents
     */
    if (alloc_hint == BS_ALLOC_NFS) {
        MS_SMP_ASSERT( lock_holder(&bfap->xtntMap_lk.lock) );
        (void) imm_compress_subxtnt_map(&bfap->xtnts, bfap->bfPageSz);
    }

    if ( updateDisk ) {
        sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                        bfap,
                                        xtntMap,
                                        parentFtx);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    *allocPageCnt = pageCnt;

    return sts;

HANDLE_EXCEPTION:
    unload_sub_xtnt_maps (xtntMap);

    return sts;

}  /* end alloc_append_stg */


/*
 * get_first_mcell
 *
 * This function gets the first extent record mcell for a normal bitfile.
 * It tries to allocate an mcell on the same disk as the primary mcell.
 * If this fails, it tries other disks in the service class.
 */

static
statusT
get_first_mcell (
                 bfTagT bfSetTag,       /* in */
                 bfAccessT *bfap,       /* in */
                 uint32T bfPageCnt,     /* in */
                 ftxHT parentFtx,       /* in */
                 vdIndexT *retVdIndex,  /* out */
                 bfMCIdT *retMcellId    /* out */
                 )
{
    bfMCIdT mcellId;
    statusT sts;
    vdIndexT vdIndex;
    vdT *vdp;
    NEW_VD_SKIP(vd_skip_list);


    vdIndex = bfap->primVdIndex;

    while (1) {

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    bfap->reqServices,
                                    bfap->optServices,
                                    vdIndex, /* start with preferred disk */
                                    vd_skip_list,
                                    bfPageCnt * bfap->bfPageSz, /* req blk cnt*/
                                    bfap->bfPageSz,
                                    FALSE); /* don't add to allocClust */

        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        vdIndex = vdp->vdIndex;
        sts = stg_alloc_new_mcell( bfap,
                                   bfSetTag,
                                   bfap->tag,
                                   vdIndex,
                                   parentFtx,
                                   &mcellId,
                                   FALSE);

        vd_dec_refcnt(vdp);
        if (sts == EOK) {
            *retVdIndex = vdIndex;
            *retMcellId = mcellId;
            return sts;
        }

        /* add to skip list so we try to allocate from another disk */
        MS_SMP_ASSERT(vdIndex != (vdIndexT) -1)
        SET_VD_SKIP(vd_skip_list, vdIndex);
        vdIndex = 0; /* do normal selection */

    }  /* end while */

HANDLE_EXCEPTION:

    return sts;

}  /* end get_first_mcell */


/*  
 * If there is room in the single, new storage sub extent map,
 * combine extents from both the previous(old) and new(stg) subextent maps
 * into the new storage sub extent map.
 * Old storage is placed at the head of the new extent list in the
 * new(stg) sub extent map.  The old sub extent map's mcell is deleted.
 */ 
statusT 
imm_try_combine_submaps(
                bsInMemSubXtntMapT *prevOldSubXtntMap,  /* in */
                bsInMemSubXtntMapT *newSubXtntMap,  /* in/modified */
                ftxHT parentFtx)           /* in */
{
    int curr_avail_slots, tot_avail_slots, tot_free_slots;
    bsXtntT bsXA[BMT_XTRA_XTNTS];/* tmp array of disk extent descriptors */
    statusT sts;
    int i,j;

    /* see if enough free slots to hold extents in prevOldSubXtntMap */
    if( prevOldSubXtntMap->vdIndex == newSubXtntMap->vdIndex ) {

        MS_SMP_ASSERT(newSubXtntMap->type == BSR_XTRA_XTNTS);
        MS_SMP_ASSERT(newSubXtntMap->onDiskMaxCnt == BMT_XTRA_XTNTS);

        curr_avail_slots = newSubXtntMap->maxCnt - newSubXtntMap->cnt;
        if(curr_avail_slots < prevOldSubXtntMap->cnt) {
            /* see if we can make some room */
            tot_avail_slots=newSubXtntMap->onDiskMaxCnt- newSubXtntMap->cnt;

            if(tot_avail_slots >= prevOldSubXtntMap->cnt) {
                /* grow the extents array to make room */
                while(newSubXtntMap->maxCnt < newSubXtntMap->onDiskMaxCnt) {
                    sts = imm_extend_sub_xtnt_map(newSubXtntMap);
                    if (sts != EOK) {
                        return(sts);
                    }
                }
                MS_SMP_ASSERT(newSubXtntMap->maxCnt == 
                                  newSubXtntMap->onDiskMaxCnt);
            } else {
                    return(ENO_SPACE_IN_MCELL);
            }
        }
    } else {
        /* vds don't match */
        return(ENO_SPACE_IN_MCELL);
    }

    /* should be enough slots now to combine extents into 1 subxtntmap */
    MS_SMP_ASSERT(newSubXtntMap->maxCnt >= 
                      (newSubXtntMap->cnt + prevOldSubXtntMap->cnt));
        
    /*save off the extents to tmp that are currently in the new subxtntmap*/
    for(i=0; i<newSubXtntMap->cnt; i++) {
        bsXA[i].bsPage = newSubXtntMap->bsXA[i].bsPage;
        bsXA[i].vdBlk  = newSubXtntMap->bsXA[i].vdBlk;
        /* re-init newSub as contents copied */
        newSubXtntMap->bsXA[i].bsPage = 0;
        newSubXtntMap->bsXA[i].vdBlk = XTNT_TERM;
    }

    /* Write in the extents currently in the prevOldSubXtntMap. */
    newSubXtntMap->pageOffset = prevOldSubXtntMap->pageOffset;
    for(i=0; i<prevOldSubXtntMap->cnt; i++) {
        if((i == (prevOldSubXtntMap->cnt-1)) &&  /* on last extent */
           (bsXA[0].bsPage == prevOldSubXtntMap->bsXA[i].bsPage) &&
           (prevOldSubXtntMap->bsXA[i].vdBlk == XTNT_TERM)) {
            /* skip the terminator cause next extent in next map has pages */
            break; 
        }
        newSubXtntMap->bsXA[i].bsPage = prevOldSubXtntMap->bsXA[i].bsPage;
        newSubXtntMap->bsXA[i].vdBlk  = prevOldSubXtntMap->bsXA[i].vdBlk;
    }

    /* finally write back in the extents saved off */
    for(j=0; j<newSubXtntMap->cnt && i<=newSubXtntMap->maxCnt; i++,j++) {
        newSubXtntMap->bsXA[i].bsPage = bsXA[j].bsPage;
        newSubXtntMap->bsXA[i].vdBlk  = bsXA[j].vdBlk;
    }

    newSubXtntMap->pageCnt = prevOldSubXtntMap->pageCnt + newSubXtntMap->pageCnt;
    newSubXtntMap->cnt = i;
    newSubXtntMap->updateStart = 0;
    newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;

    return(EOK);
}


/*
** alloc_hole_stg
**
** This function allocates storage and inserts it between two extents.
** The first extent is the last storage extent before the hole and the
** second extent is the extents describing the hole itself.
*
*
***  one "box" == subextent (in memory) = mcell (on the disk)
*
*** Case I   - storage added before hole on the SAME vd as the vd of 
***            the first extent following the hole.
*
*          before merge sub                    new stg at  5036
*                    origStart                 updateStart
*                    origEnd                   updateEnd
*         vd3          vd3                       vd3
***  ------------- ------------             ------------
***  |5032  66960| |5036    -1|             |5036  5678|
*    |5036    -1 | |  (hole)  |....  .mapend|5040    -1|
*    |           | |42666  160| copy hole-->|42666  160|
*    |           | |42670   -1|          -->|42670   -1|
***  ------------- ------------             ------------
*                       ^        sub/cell         |
*                       |  replaced during merge  |
*                       ---------------------------
*
*step 1) make sure map has an extra submap on end
*step 2) copy any extents before hole to extra submap on end
*step 3) stg_alloc_from_one_disk appends storage to extra submap on end
*step 4) copy hole to extra submap on end  (42666   -1)
*step 5) copy remaining extents to extra submap on end (42666 160)(42670  -1)
*step 6) try to merge contiguous extents
*step 7) update original mcell on disk (same mcell as origStart)
*(caller performs submap merge)
*
*
*** Case II  - storage added before hole on DIFFERENT vd as the vd of 
***            the first extent following the hole. (due to full vd)
*              Previously added storage on a new volume(vd4).
*
*step 1) make sure map has an extra submap on end
*step 2) copy any extents before hole to extra submap on end
*
*                    origStart                 updateStart
*                    origEnd                   updateEnd
*         vd4          vd3                       vd3
***  ------------- ------------             ------------
***  |5032  66960| |5036    -1|             |5036    -1|
*    |5036    -1 | |  (hole)  |....  .mapend|          |
*    |           | |42666  160|             |          |
*    |           | |42670   -1|             |          |
***  ------------- ------------             ------------
*
*
*step 3) stg_alloc_from_one_disk appends storage submap to extra submap on end
*
*                    origStart               updateStart
*                    origEnd                            NEW STG/updateEnd
*         vd4          vd3                       vd3         vd4
***  ------------- ------------             ------------ ------------
***  |5032  66960| |5036    -1|             |5036    -1| |5036  6714|
*    |5036    -1 | |  (hole)  |....  .mapend|          | |5040    -1|
*    |           | |42666  160|             |          | |          |
*    |           | |42670   -1|             |          | |          |
***  ------------- ------------             ------------ ------------ 
*
*step 4a) set updateStart to NEW STG, old updateStart is orphan
*step 4b) unlink origStart submap because a new submap will be inserted
*step 4c) free (deferred) the orphan submap
*step 4d) imm_try_combine_submaps() - if new stg submap has room for orig-1
*         map's storage and vds match, combine the extents from orig-1 with
*         NEW STG.
*step 4e) unlink vd4 previous stg mcell, set origStart so it will be overwriten
*                                                 updateStart
*       (unlinked)                                updateEnd
*       origStart     origEnd          freed!      NEW STG
*         vd4          vd3               vd3         vd4           vd3
***  ------------- ------------      ------------ ------------ -----------
***  |5032  66960| |5036    -1|      |5036    -1| |5032 66960| |5040   -1|
*    |5036    -1 | |  (hole)  |....  |          | |5036  6714| |42666  -1|
*    |           | |42666  160|      |          | |5040    -1| |         |
*    |           | |42670   -1|      |          | |          | |         |
***  ------------- ------------      ------------ ------------ -----------
*          |         combine extents from same vds      ^
*          ---------------------------------------------| 
*                    inserted BEFORE NEW STG
*
*step 5a) split/copy hole to extra submap on end  (42666   -1)
*                    origStart                     updateStart
*                    origEnd                       updateEnd
*         vd4          vd3               vd3         vd4
***  ------------- ------------      ------------ ------------ -----------
***  |5032  66960| |5036    -1|      |5036    -1| |5032 66960| |5040   -1|
*    |5036    -1 | |  (hole)  |....  |          | |5036  6714| |42666  -1|
*    |           | |42666  160|      |          | |5040    -1| |         |
*    |           | |42670   -1|      |          | |          | |         |
***  ------------- ------------      ------------ ------------ -----------
*
*step 5b) copy remaining extents to extra submap on end (42666 160)(42670  -1)
*                    origStart                     updateStart
*                    origEnd                       NEW STG       updateEnd
*         vd4          vd3               vd3         vd4           vd3
***  ------------- ------------      ------------ ------------ -----------
***  |5032  66960| |5036    -1|      |5036    -1| |5032 66960| |5040   -1|
*    |5036    -1 | |  (hole)  |....  |          | |5036  6714| |42666 160|
*    |           | |42666  160|      |          | |5040    -1| |42670  -1|
*    |           | |42670   -1|      |          | |          | |         |
***  ------------- ------------      ------------ ------------ -----------
*
*step 6) try to merge contiguous, physical extents
*step 7) update original mcell on disk (same mcell as origStart)
*(caller performs submap merge)
*
*
*/

static
statusT
alloc_hole_stg (
                bfAccessT *bfap,           /* in */
                uint32T bfPageOffset,      /* in */
                uint32T bfPageCnt,         /* in */
                vdIndexT bfVdIndex,        /* in */
                bsInMemXtntMapT *xtntMap,  /* in */
                uint32T mapIndex,          /* in */
                uint32T descIndex,         /* in */
                bsAllocHintT alloc_hint,   /* in */
                ftxHT parentFtx,           /* in */
                int updateDisk,            /* in */
                uint32T *allocPageCnt      /* out */
                )
{
    bfSetT *bfSetDesc;
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32T i;
    bsInMemSubXtntMapT *newSubXtntMap;
    bsInMemSubXtntMapT *oldSubXtntMap;
    bsInMemSubXtntMapT *prevOldSubXtntMap;
    uint32T pageCnt;
    statusT sts;
    bfMCIdT reuseMcellId = bsNilMCId;


    MS_SMP_ASSERT( lock_holder(&bfap->xtntMap_lk.lock) );

    xtntMap->origStart = mapIndex;
    xtntMap->origEnd = mapIndex;

    /*
     * step one: extend the XTNTMAP to hold an extra subXtntmap
     *
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */
    if (xtntMap->cnt >= xtntMap->maxCnt) {
        sts = imm_extend_xtnt_map (xtntMap);
        if (sts != EOK) {
            return sts;
        }
    }

    /* step 2: copy submap where stg to be added TO extra map (updateStart)*/
    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtntMap,
                                 &xtntMap->subXtntMap[mapIndex],
                                 &xtntMap->subXtntMap[xtntMap->cnt]);
    if (sts != EOK) {
        return sts;
    }

    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);
    xtntMap->cnt++;

    /*
     * Change the updateStart map contents so it only shows hole start
     *
     * Make the hole descriptor into the termination descriptor.  This is
     * needed so that the storage information is added correctly in the sub
     * extent map.
     */
    newSubXtntMap->cnt = descIndex + 1;
    MS_SMP_ASSERT(newSubXtntMap->cnt <= newSubXtntMap->maxCnt);


    /* set the sub-xtnt updateStart */
    newSubXtntMap->updateStart = newSubXtntMap->cnt;
    newSubXtntMap->pageCnt = newSubXtntMap->bsXA[newSubXtntMap->cnt-1].bsPage -
                                                      newSubXtntMap->pageOffset;

    /* step 3: alloc the storage on any volume (tries last vd alloc first) */
    bfSetDesc = bfap->bfSetp;
    if (bfap->xtnts.type != BSXMT_STRIPE) {
       MS_SMP_ASSERT(bfVdIndex == (vdIndexT)-1);
    }  

    if (bfVdIndex == (vdIndexT)(-1)) {
        sts = stg_alloc_from_svc_class( bfap,
                                        bfap->reqServices,
                                        bfap->optServices,
                                        bfSetDesc->dirTag,
                                        bfap->tag,
                                        bfPageOffset,
                                        bfPageCnt,
                                        bfap->bfPageSz,
                                        xtntMap,
                                        &alloc_hint,
                                        parentFtx,
                                        &pageCnt);
    } else {
        uint32T fileClust;
        vdT *vdp;

        vdp = VD_HTOP(bfVdIndex, bfap->dmnP);
        if (vdp->stgCluster == BS_CLUSTSIZE) {
             fileClust = (bfPageCnt * bfap->bfPageSz) / BS_CLUSTSIZE;
        } else {
             MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
             fileClust = (bfPageCnt * bfap->bfPageSz) / BS_CLUSTSIZE_V3;
        }

        SC_TBL_LOCK(bfap->dmnP);
        vdp->allocClust += fileClust;
        SC_TBL_UNLOCK(bfap->dmnP);

        sts = stg_alloc_from_one_disk( bfap,
                                       bfVdIndex,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       bfPageOffset,
                                       bfPageCnt,
                                       bfap->bfPageSz,
                                       xtntMap,
                                       &alloc_hint,
                                       parentFtx,
                                       &pageCnt);

        SC_TBL_LOCK(bfap->dmnP);
        vdp->allocClust -= fileClust;
        SC_TBL_UNLOCK(bfap->dmnP);
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if (pageCnt != bfPageCnt) {
        RAISE_EXCEPTION (ENO_MORE_BLKS);
    }


    /* step 4: 4a.unlink orphan mcell from orig, 
     *         4b.setup reuse of mcell Id, 
     *         4c.free from memory the copy
     *         4d.try to merge sub extent maps
     *         4e.free from memory the previous map.
     */
    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
    if (newSubXtntMap->updateStart == newSubXtntMap->cnt) {
        /*
         * No new storage was allocated to the first update sub extent map.
         * Most likely is that storage was allocated on a different disk.
         * Set the update range so that the termination extent descriptor
         * from above is copied to the on-disk record and the entry count
         * is updated.
         */

        if((newSubXtntMap->cnt == 1) &&
           (newSubXtntMap->type != BSR_SHADOW_XTNTS))
        {

            /* We have an orphan subextentmap. This happens if we are 
             * adding storage into a hole that lives in the first
             * descriptor of a subextent map and there was no more
             * storage available. We will remove this from the update
             * range, unlink it from the on-disk mcell chain and mark 
             * it to be reused after the new storage descriptors.
             * 
             * We will orphan a shadow extent since it is the first
             * extent in the map. This is legal. It would be too much
             * headache to try to do otherwise.
             */
            reuseMcellId= newSubXtntMap->mcellId;

            /* Since this only occurs with an extent that starts
             * with a hole we know we have a previous sub xtnt.
             */
            MS_SMP_ASSERT(xtntMap->origStart > 0);

            sts= bmt_unlink_mcells(bfap->dmnP,
                                   bfap->tag,
                                   xtntMap->subXtntMap[xtntMap->origStart-1].vdIndex,
                                   xtntMap->subXtntMap[xtntMap->origStart-1].mcellId,
                                   newSubXtntMap->vdIndex,
                                   newSubXtntMap->mcellId,
                                   newSubXtntMap->vdIndex,
                                   newSubXtntMap->mcellId,
                                   parentFtx);
            if (sts != EOK)
            {
                RAISE_EXCEPTION (sts);
            }
                              
            /* free the orphan */
            imm_unload_sub_xtnt_map(newSubXtntMap);
            xtntMap->updateStart++;

            /* Try to combine previous map contents with new stg map contents to 
             * eliminate another mcell creation.
             */
            if(mapIndex > 1) {    /* make sure PRIME extent mcell is not involved */
                prevOldSubXtntMap = &(xtntMap->subXtntMap[mapIndex-1]);
                newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
                sts = imm_try_combine_submaps( prevOldSubXtntMap, 
                                               newSubXtntMap, 
                                               parentFtx);
  
                /* error just means that combining submaps did not succeed - 
                 * we'll just use existing two maps to describe the stg added
                 */
                if(sts == EOK) {
                    /*
                    ** imm_try_combine_submaps has succeeded in combining the
                    ** xtnts in the previous mcell (before the hole) with the
                    ** new stg xtnts in a brand new mcell. One of these two
                    ** mcells is no longer needed. Delete the new one.
                    ** Since we are now changing the contents of the previous
                    ** subxtntmap we must decrement origStart to include it.
                    */
                    xtntMap->origStart--;

                    sts = deferred_free_mcell (
                               VD_HTOP(newSubXtntMap->vdIndex, bfap->dmnP),
                               newSubXtntMap->mcellId,
                               parentFtx);
                    if ( sts != EOK ) {
                        RAISE_EXCEPTION(sts);
                    }

                    /*
                    ** UpdateStart (newSubXtntMap) is now a modified copy
                    ** of the mcell before the insertion point.
                    ** Set the in-memory subxtntmap mcell pointer to point
                    ** at previous mcell.
                    */
                    newSubXtntMap->vdIndex = prevOldSubXtntMap->vdIndex;
                    newSubXtntMap->mcellId = prevOldSubXtntMap->mcellId;
                    newSubXtntMap->mcellState = INITIALIZED;
                 } /* end if combined ok */
            }  /* end if not primary mcell */
        }
        else /* no orphan */
        {
            /*
             * Set the update range so that the termination extent descriptor
             * from above is copied to the on-disk record and the entry count
             * is updated.
             */

            newSubXtntMap->updateStart = newSubXtntMap->cnt - 1;
            newSubXtntMap->updateEnd = newSubXtntMap->updateStart;
        }
    }

    /*
     * We added storage in a hole mapped by the extent array.  Now, copy
     * the extent descriptors positioned after the hole descriptor.
     */

    oldSubXtntMap = &(xtntMap->subXtntMap[mapIndex]);
    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->updateEnd]);

    if (newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage <
        oldSubXtntMap->bsXA[descIndex + 1].bsPage) {
        /*
         * The last new page allocated and the next page after the hole are
         * not contiguous, we need to create a hole descriptor and copy it to
         * the end of the extent map.
         */

        /* step 5a: create hole desc in temp variable bsXA */
        imm_split_desc( newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage,
                        xtntMap->blksPerPage,
                        &(oldSubXtntMap->bsXA[descIndex]),  /* orig */
                        &desc,  /* first part */
                        &(bsXA[0]));  /* last part */
        bsXA[1].bsPage = oldSubXtntMap->bsXA[descIndex + 1].bsPage;
        bsXA[1].vdBlk = XTNT_TERM;
        /* copy new tmp hole desc into a subXtntMap after any new stg maps */
        sts = imm_copy_xtnt_descs( oldSubXtntMap->vdIndex,
                                   &(bsXA[0]),
                                   1,
                                   TRUE,
                                   xtntMap,
                                   bfap->xtnts.type);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        oldSubXtntMap = &(xtntMap->subXtntMap[mapIndex]);
    }
    descIndex++;

    /* Step 5b:
     * Copy the remaining entries from the split sub extent map to after hole
     */
    sts = imm_copy_xtnt_descs( oldSubXtntMap->vdIndex,
                               &(oldSubXtntMap->bsXA[descIndex]),
                               (oldSubXtntMap->cnt - 1) - descIndex,
                               TRUE,
                               xtntMap,
                               bfap->xtnts.type);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Set the last update sub extent map's "updateEnd" field as
     * imm_copy_xtnt_descs() may have modified the map.
     */

    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->updateEnd]);
    newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;

    if ( !MCID_EQL(reuseMcellId, bsNilMCId) &&
          xtntMap->cnt == xtntMap->updateEnd + 1 )
    {
        /*
        ** A non nil reuseMcellId means that we added stg in a leading hole.
        ** It also means that the new stg was not on the same vd as mapIndex.
        ** Therefore the mapIndex subXtntMap has not been used yet.
        ** The cnt vs updateEnd test means that the updateEnd subXtntMap is
        ** on the same vd as the mapIndex subXtntMap and imm_copy_xtnt_descs
        ** was able to copy the remaining extents in mapIndex into updateEnd.
        ** If this was not true imm_copy_xtnt_descs would have added new
        ** subXtntMaps but not bumped updateEnd. This would be handled in the
        ** for loop below. The mcell allocated in stg_alloc_from_one_disk for
        ** updateEnd now contains the remaining xtnts (minus all or part of
        ** the leading hole) from the mapIndex subxtntMap.
        */
        oldSubXtntMap = &xtntMap->subXtntMap[mapIndex];
        MS_SMP_ASSERT(newSubXtntMap->mcellState == NEW_MCELL);
        MS_SMP_ASSERT(!MCID_EQL(oldSubXtntMap->mcellId, newSubXtntMap->mcellId));
        MS_SMP_ASSERT(oldSubXtntMap->vdIndex == newSubXtntMap->vdIndex);
        /* Delete newly alloced mcell, we'll keep the old (mapIndex) mcell. */
        sts = deferred_free_mcell( VD_HTOP(newSubXtntMap->vdIndex, bfap->dmnP),
                                   newSubXtntMap->mcellId,
                                   parentFtx);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        newSubXtntMap->mcellId = reuseMcellId;  /* this is mapIndex mcell */
        newSubXtntMap->mcellState = USED_MCELL;
    }

    /*
     * Fix up on-disk mcells.
     * Allocate new mcells for any new sub extent maps that were added
     * by imm_copy_xtnt_descs().
     */

    for (i = xtntMap->updateEnd + 1; i < xtntMap->cnt; i++) {
        newSubXtntMap = &(xtntMap->subXtntMap[i]);
        if ((!MCID_EQL(reuseMcellId, bsNilMCId)) &&
            (i==xtntMap->cnt-1))
        {
            /* Reuse the mcell from the orphan. We know
             * this must go on the last MCELL on the update
             * list.
             */
            
            newSubXtntMap->mcellId = reuseMcellId;
            newSubXtntMap->mcellState = USED_MCELL;
        }
        else
        {
            sts = stg_alloc_new_mcell( bfap,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       newSubXtntMap->vdIndex,
                                       parentFtx,
                                       &(newSubXtntMap->mcellId),
                                       TRUE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            newSubXtntMap->mcellState = NEW_MCELL;
        }

        newSubXtntMap->updateStart = 0;
        newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
    }  /* end for */

    xtntMap->updateEnd = xtntMap->cnt - 1;

    /* Step 6:
     * try to merge contiguous extents
     */
    if (alloc_hint == BS_ALLOC_NFS) {
        MS_SMP_ASSERT( lock_holder(&bfap->xtntMap_lk.lock) );
        (void) imm_compress_subxtnt_map(&bfap->xtnts, bfap->bfPageSz);
    }

    if ( updateDisk ) {
        /* Step 7 */
        sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                        bfap,
                                        xtntMap,
                                        parentFtx);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    *allocPageCnt = pageCnt;
    return sts;

HANDLE_EXCEPTION:
    xtntMap->updateEnd = xtntMap->cnt - 1;
    unload_sub_xtnt_maps (xtntMap);

    return sts;

}  /* end alloc_hole_stg */


/*
 * stg_alloc_from_svc_class
 *
 * This function allocates storage from the disks within the bitfile's
 * service class and saves the allocation information in the in-memory
 * extent map's sub extent maps.  The number of pages allocated are
 * returned via the "allocPageCnt" parameter.
 *
 * The page offset of the start of the allocation is assumed to be
 * equal to or greater than the page offset of the last page mapped
 * by the extent map.
 *
 * This function sets the extent map's "updateStart" and "updateEnd"
 * fields to indicate which sub extent maps may have been modified.
 * If no sub extent maps were modified, "updateStart" is set to
 * "cnt".
 *
 * This function always sets the extent map's "upateStart" and "updateEnd"
 * fields.  The sub extent maps in this range are valid.  If this function
 * returns an error, the caller is responsible for cleaning up the extent
 * map.  That is, cleaning up the sub extent maps in the update range.
 *
 * This function sets each modified sub extent map's "updateStart" and
 * "updateEnd" fields to indicate which extent descriptors were modified.
 * If no descriptors were modified, "updateStart" is set equal to the
 * number of descriptors.
 *
 * If a new sub extent map is initialized, this function allocates
 * the mcell before allocating storage so that the caller is guaranteed
 * an mcell.  The pointer to the mcell is saved in the sub extent map's
 * "vdIndex" and "mcellId" fields.
 *
 * This function assumes that the extent map and the "xtntMap->cnt" - 1
 * sub extent map is initialized.  The function begins appending storage
 * information to this sub extent map.
 *
 * This function does not start a sub transaction.  If it has an error,
 * it does not deallocate the storage or the mcells it has already allocated.
 * The caller must do this by calling ftx_fail().
 *
 * The caller must specify a non-nil parent ftx.
 */

statusT
stg_alloc_from_svc_class (
                          bfAccessT *bfap,            /* in */
                          serviceClassT reqServices,  /* in */
                          serviceClassT optServices,  /* in */
                          bfTagT bfSetTag,            /* in */
                          bfTagT bfTag,               /* in */
                          uint32T bfPageOffset,       /* in */
                          uint32T bfPageCnt,          /* in */
                          int bfPageSize,             /* in */
                          bsInMemXtntMapT *xtntMap,   /* in */
                          bsAllocHintT *alloc_hint,   /* in */
                          ftxHT parentFtx,            /* in */
                          uint32T *allocPageCnt       /* out */
                          )
{
    uint32T pageCnt;
    uint32T requestedBlkCnt;
    statusT sts;
    uint32T totalPageCnt = 0;
    uint32T updateStart;
    vdIndexT vdIndex;
    vdT *vdp;
    NEW_VD_SKIP(vd_skip_list);


    updateStart = xtntMap->cnt - 1;

    if (FTX_EQ(parentFtx, FtxNilFtxH)) {
        RAISE_EXCEPTION (EBAD_PAR_FTXH);
    }

    /* start looking on last vd stg was allocated from 
     * otherwise, if this is first open from disk, try the previous
     * subXtntMap's vd 
     */
    if (xtntMap->subXtntMap[updateStart].mcellState == NEW_MCELL){
        vdIndex = xtntMap->subXtntMap[updateStart].vdIndex;
    } else {
        if((int16T)xtntMap->allocVdIndex > 0)
            vdIndex = xtntMap->allocVdIndex;
        else
            vdIndex = xtntMap->subXtntMap[updateStart].vdIndex;
    }


    /*******************************************************************
     *                   WARNING WARNING WARNING                       *
     *                                                                 *
     * It might seem like a good idea to try to get storage more than  *
     * one time from each VD...  DON'T DO IT! ... There is a problem   *
     * with the way the needs-to-be-updated extent maps are handled    *
     * such that preceeding changes are lost (and we get corruption)   *
     * if we call into stg_alloc_from_one_disk(), get some storage and *
     * then call stg_alloc_from_one_disk() again to get more storage   *
     * on that same VD.  The second allocation destroys the info for   *
     * the first allocation in the temporary extent map update area.   *
     *                                                                 *
     ******************************************************************/
    while (totalPageCnt < bfPageCnt) {

        /*
         * This function assumes that sc_select_vd_for_stg() returns
         * a vd index of the virtual disk that has "requestedBlkCnt"
         * blocks free.  Or, if no disk has "requestedBlkCnt" blocks
         * free, it returns the index of the virtual disk that has the
         * most free space greater than the minimum requested ("bfPageSz").
         *
         * Note:  If a disk has free space, there is no guarantee that
         * it is contiguous.  So, even though a disk has free space
         * greater than the minimum we requested, it may not have
         * enough contiguous space to satisfy our request.
         *
         * It would be nice if sc_select_vd_for_stg() checked the
         * contiguous space on a disk.
         */

        requestedBlkCnt = (bfPageCnt - totalPageCnt) * bfPageSize;

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    reqServices,
                                    optServices,
                                    vdIndex, /* start with preferred disk */
                                    vd_skip_list,
                                    requestedBlkCnt,
                                    bfPageSize,
                                    TRUE); /* add to allocClust flag */
        if (sts) {

            if ((sts == ENO_MORE_BLKS) && (totalPageCnt > 0)) {
                /*
                 * No more storage available in the service class.
                 */
                break;  /* out of while */
            }

            RAISE_EXCEPTION (sts);
        }

        vdIndex = vdp->vdIndex;
        pageCnt = 0;

        sts = stg_alloc_from_one_disk( bfap,
                                       vdIndex,
                                       bfSetTag,
                                       bfTag,
                                       bfPageOffset + totalPageCnt,
                                       bfPageCnt - totalPageCnt,
                                       bfPageSize,
                                       xtntMap,
                                       alloc_hint,
                                       parentFtx,
                                       &pageCnt);

        SC_TBL_LOCK(bfap->dmnP);
        if (vdp->stgCluster == BS_CLUSTSIZE) {
            vdp->allocClust -= requestedBlkCnt / BS_CLUSTSIZE;
        } else {
            MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
            vdp->allocClust -= requestedBlkCnt / BS_CLUSTSIZE_V3;
        }
        SC_TBL_UNLOCK(bfap->dmnP);

        vd_dec_refcnt(vdp);

        if (sts)
            if (sts != ENO_MORE_BLKS) {
                RAISE_EXCEPTION (sts);
            }

        totalPageCnt = totalPageCnt + pageCnt;
        /* add to skip list so we try to allocate from another disk */
        MS_SMP_ASSERT(vdIndex != (vdIndexT) -1)
        SET_VD_SKIP(vd_skip_list, vdIndex);
        vdIndex = 0; /* do normal selection */

    }  /* end while */

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;
    *allocPageCnt = totalPageCnt;

    return EOK;

HANDLE_EXCEPTION:

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;
    *allocPageCnt = totalPageCnt;

    return sts;

}  /* end stg_alloc_from_svc_class */


/*
** Allocate "bfPageCnt" pages of storage for "bfap". The pages will be used
** for file page "bfPageOffset". Allocate storage from as many volumes as
** necessary to fill the request. The storage will be represented on disk
** by a chain of mcells on the defered delete list. The storage will be
** represented in memory by "xtntMap". On entry xtntMap is empty.
*/
statusT
cp_stg_alloc_from_svc_class (
                          bfAccessT *bfap,            /* in  stg for this file*/
                          uint32T bfPageOffset,       /* in  start here */
                          uint32T bfPageCnt,          /* in  for this long */
                          int32T stg_type,
                          bsInMemXtntMapT *xtntMap,   /* in  put in this map */
                          uint32T startPage,          /* in  first xtnt page */
                          bsAllocHintT alloc_hint,    /* in */
                          uint32T *copyXferSize       /* out */
                          )
{
    int maxSkipCnt = 0;
    uint32T pageCnt=0;
    uint32T requestedBlkCnt;
    statusT sts;
    uint32T totalPageCnt = 0;
    vdIndexT vdIndex;
    vdT *vdp;
    NEW_VD_SKIP(vd_skip_list);


    MS_SMP_ASSERT(bfap->bfPageSz == xtntMap->blksPerPage);
    MS_SMP_ASSERT(bfap->dmnP == xtntMap->domain);

    while ( totalPageCnt < bfPageCnt ) {

        /*
         * This function assumes that sc_select_vd_for_stg() returns
         * a vd index of the virtual disk that has "requestedBlkCnt"
         * blocks free.  Or, if no disk has "requestedBlkCnt" blocks
         * free, it returns the index of the virtual disk that has the
         * most free space greater than the minimum requested ("bfPageSz").
         *
         * Note:  If a disk has free space, there is no guarantee that
         * it is contiguous.  So, even though a disk has free space
         * greater than the minimum we requested, it may not have
         * enough contiguous space to satisfy our request.
         */

        if (stg_type == PERM_HOLE_START)
        {
            /* We really don't need any storage but we have to pick a disk
             * so chose one that has at least a page
             */

            requestedBlkCnt = bfap->bfPageSz;
        }
        else
        {
            requestedBlkCnt = (bfPageCnt - totalPageCnt) * bfap->bfPageSz;
        }

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    bfap->reqServices,
                                    bfap->optServices,
                                    0,     /* no preferred disk */
                                    vd_skip_list,
                                    requestedBlkCnt,
                                    bfap->bfPageSz,
                                    TRUE); /* add to allocClust flag */

        if (sts) {
            return(sts);
        }

        vdIndex = vdp->vdIndex;
        pageCnt = 0;

        /* Select the copy transfer size as the max wrmaxio for all 
         *  the volumes.
         */

        if ( vdp->wrmaxio > *copyXferSize ) *copyXferSize = vdp->wrmaxio;

        sts = cp_stg_alloc_from_one_disk( bfap,
                                          vdp->vdIndex,
                                          bfPageOffset + totalPageCnt,
                                          bfPageCnt - totalPageCnt,
                                          stg_type,
                                          xtntMap,
                                          startPage,
                                          &pageCnt,
                                          0,
                                          alloc_hint);

        SC_TBL_LOCK(bfap->dmnP);
        if (vdp->stgCluster == BS_CLUSTSIZE) {
            vdp->allocClust -= requestedBlkCnt / BS_CLUSTSIZE;
        } else {
            MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
            vdp->allocClust -= requestedBlkCnt / BS_CLUSTSIZE_V3;
        }
        SC_TBL_UNLOCK(bfap->dmnP);

        vd_dec_refcnt(vdp);

        if ( sts != EOK && sts != ENO_MORE_BLKS ) {
            return(sts);
        }

        totalPageCnt = totalPageCnt + pageCnt;

        /* add to skip list so we try to allocate from another disk */
        MS_SMP_ASSERT(vdIndex != (vdIndexT) -1);
        SET_VD_SKIP(vd_skip_list, vdIndex);

    }  /* end while */

    return EOK;
}


/*
 * stg_alloc_from_one_disk
 *
 * This function allocates storage from the specified disk and saves
 * the allocation information in the in-memory extent map's sub extent
 * maps.  The number of pages allocated are returned via the
 * "allocPageCnt" parameter.
 *
 * The page offset of the start of the allocation is assumed to be
 * equal to or greater than the page offset of the last page mapped
 * by the extent map.
 *
 * This function sets the extent map's "updateStart" and "updateEnd"
 * fields to indicate which sub extent maps may have been modified.
 * If no sub extent maps were modified, "updateStart" is set to
 * "cnt".
 *
 * This function always sets the extent map's "upateStart" and "updateEnd"
 * fields.  The sub extent maps in this range are valid.  If this function
 * returns an error, the caller is responsible for cleaning up the extent
 * map.  That is, cleaning up the sub extent maps in the update range.
 *
 * This function sets each modified sub extent map's "updateStart" and
 * "updateEnd" fields to indicate which extent descriptors were modified.
 * If no descriptors were modified, "updateStart" is set equal to the
 * number of descriptors.
 *
 * If a new sub extent map is initialized, this function allocates
 * the mcell before allocating storage so that the caller is guaranteed
 * an mcell.  The pointer to the mcell is saved in the sub extent map's
 * "vdIndex" and "mcellId" fields.
 *
 * This function assumes that the extent map and the "xtntMap->cnt" - 1
 * sub extent map is initialized.  The function begins appending storage
 * information to this sub extent map.
 *
 * This function does not start a sub transaction.  If it has an error,
 * it does not deallocate the storage or the mcells it has already allocated.
 * The caller must do this by calling ftx_fail().
 *
 * The caller must specify a non-nil parent ftx.
 */

statusT
stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in */
                         vdIndexT bfVdIndex,        /* in */
                         bfTagT bfSetTag,           /* in */
                         bfTagT bfTag,              /* in */
                         uint32T bfPageOffset,      /* in */
                         uint32T bfPageCnt,         /* in */
                         int bfPageSize,            /* in */
                         bsInMemXtntMapT *xtntMap,  /* in */
                         bsAllocHintT *alloc_hint,  /* in */
                         ftxHT parentFtx,           /* in */
                         uint32T *allocPageCnt      /* out */
                         )
{
    ftxHT ftxH;
    bfMCIdT mcellId;
    uint32T pageCnt;
    uint32T pageOffset;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt = 0;
    uint32T updateStart;
    int ftxStarted = FALSE;
    uint32T reservedPagesCount = 0,
            reservedPagesLocation = 0;

    updateStart = xtntMap->cnt - 1;

    if (FTX_EQ(parentFtx, FtxNilFtxH)) {
        RAISE_EXCEPTION (EBAD_PAR_FTXH);
    }

    subXtntMap = &(xtntMap->subXtntMap[updateStart]);

    if (subXtntMap->vdIndex == bfVdIndex) {

        /*
         * Start a transaction since the call to alloc_from_one_disk()
         * might allocate some storage but not place it into a
         * subextent map because a new mcell is needed.  If the
         * mcell cannot be acquired, it will be necessary to release
         * the storage allocated.  This will be done by the ftx_fail().
         */
        sts = FTX_START_N( FTA_BS_STG_ALLOC_MCELL_V1,
                           &ftxH,
                           parentFtx,
                           bfap->dmnP,
                           0);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        ftxStarted = TRUE;

        /*
         * The last sub extent map's disk matches the specified disk.
         * Allocate storage and save the information in the last map.
         */

        sts = alloc_from_one_disk( bfap,
                                   bfPageOffset,
                                   bfPageCnt,
                                   bfPageSize,
                                   subXtntMap,
                                   alloc_hint,
                                   ftxH,
                                   &reservedPagesCount,
                                   &reservedPagesLocation,
                                   &totalPageCnt);
        /* We must return the pages we successfully allocated even if we */
        /* fail subsequent operations. */
        *allocPageCnt = totalPageCnt;
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
 
        /*
         * If the above call succeeded, updated its extent maps,
         * and didn't reserve any storage, commit this subtransaction now.
         * If the call reserved storage but didn't update its extent
         * maps because it's waiting for a new mcell, leave the transaction
         * open so that the storage allocation and new mcell allocation
         * are done in the same subtransaction.
         */
        if (reservedPagesCount == 0) {
            ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
            ftxStarted = FALSE;
        }

    }

    while (totalPageCnt < bfPageCnt) {

        if ( BF_IS_BMT(bfap) && reservedPagesCount == 0 ) {
            /* Accept less than the request for reserved files.
             * If reservedPagesCount > 0 then alloc_from_one_disk found the
             * mcell was full. We get a neww mcell in this loop and use the
             * pages found from the first call to alloc_from_one_disk.
             */
            break;
        }

        if (xtntMap->cnt >= xtntMap->maxCnt) {
            sts = imm_extend_xtnt_map (xtntMap);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
        prevSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);

        if (!ftxStarted) {
            sts = FTX_START_N( FTA_BS_STG_ALLOC_MCELL_V1,
                               &ftxH,
                               parentFtx,
                               bfap->dmnP,
                               0);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);

            }
            ftxStarted = TRUE;
        }

        /*
         * Allocate an mcell early so that the bmt can expand if
         * necessary.  If we did this after allocating storage, we
         * could suck up all available free space and fail to allocate
         * a new mcell because there isn't space for bmt expansion.
         */
        sts = stg_alloc_new_mcell( bfap,
                                   bfSetTag,
                                   bfTag,
                                   bfVdIndex,
                                   ftxH,
                                   &mcellId,
                                   FALSE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /*
         * NOTE:  The first page of the new sub extent map must be the
         * next page after the last page mapped by the previous sub extent
         * map.
         */
        MS_SMP_ASSERT(prevSubXtntMap->pageOffset + prevSubXtntMap->pageCnt <= max_page + 1);
        sts = imm_init_sub_xtnt_map( subXtntMap,
                                     (prevSubXtntMap->pageOffset +
                                      prevSubXtntMap->pageCnt),
                                     0,  /* pageCnt */
                                     bfVdIndex,
                                     mcellId,
                                     BSR_XTRA_XTNTS,
                                     BMT_XTRA_XTNTS,
                                     0);  /* force default value for maxCnt */
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        subXtntMap->mcellState = NEW_MCELL;

        sts = alloc_from_one_disk( bfap,
                                   bfPageOffset + totalPageCnt,
                                   bfPageCnt - totalPageCnt,
                                   bfPageSize,
                                   subXtntMap,
                                   alloc_hint,
                                   ftxH,
                                   &reservedPagesCount,
                                   &reservedPagesLocation,
                                   &pageCnt);
        if (sts != EOK) {
            /*
             * Fail the sub-transaction.  This frees the mcell that we
             * allocated above.  If we don't allocate the mcell within a
             * sub-transaction, we can't safely return the mcell because
             * this function's caller may either "done" or "fail" the parent
             * transaction.
             *
             * If we didn't allocate the mcell within a sub-transaction and
             * the caller failed the transaction, the mcell is freed as part
             * of the allocate mcell undo.  If the caller doesn't fail the
             * transaction, we lose the mcell.  We could start a sub-transaction
             * in this error path so that it can schedule a root done to free
             * the mcell.  This doesn't work because there is a limit on the
             * number of root dones a transaction tree can have and the number
             * of times thru this code is non-deterministic.
             */
            imm_unload_sub_xtnt_map (subXtntMap);
            if ((sts == ENO_MORE_BLKS) && (totalPageCnt > 0)) {
                ftx_fail (ftxH);
                ftxStarted = FALSE;
                break;  /* out of while loop */
            } else {
                RAISE_EXCEPTION (sts);
            }
        }

        ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
        ftxStarted = FALSE;

        totalPageCnt = totalPageCnt + pageCnt;
        /* We need to return this now in case we fail on subsequent passes */
        /* thru the loop. */
        *allocPageCnt = totalPageCnt;

        xtntMap->cnt++;

    }  /* end while */

    if (ftxStarted) {
        ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
    }

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return EOK;

HANDLE_EXCEPTION:
    if (ftxStarted) {
        ftx_fail (ftxH);
    }
    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;

}  /* end stg_alloc_from_one_disk */


/*
** Allocate as much of the request ("bfPageCnt" pages) from the volume
** identified by "bfVdIndex" as possible. The allocated storage is for
** "bfap" file. The storage will be used for file page "bfPageOffset".
** The storage will be represented in memory by "xtntMap". The allocated
** storage is represented on disk by a chain of mcells on the defered
** delete list. "allocPageCnt" is the number of pages actually allocated.
**
** This routine gets a new mcell as needed, innitializes it, then calls
** cp_stg_alloc_in_one_mcell to allocate storage to fill it up.
** The first mcell is allocated is put on the defered delete list.
** Subsequent mcells are attached to the first mcell.
**
** We start a new transaction for each mcell. Furthermore,
** cp_stg_alloc_in_one_mcell may end the transaction and start a new one if
** the amount of storage allocated is large (several pages of SBM logged).
** cp_stg_alloc_in_one_mcell may return with a transaction in progress or
** the transaction may have been ended by cp_stg_alloc_in_one_mcell.
** ftxH.hndl is used a a flag to tell if a transaction is in progress
** and therefore needs to finish the transaction.
*/
statusT
cp_stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in   stg for this file */
                         vdIndexT bfVdIndex,        /* in   stg in this vol */
                         uint32T bfPageOffset,      /* in   stg starts here */
                         uint32T bfPageCnt,         /* in   ask for this much */
                         int32T stg_type,
                         bsInMemXtntMapT *xtntMap,  /* in   put in this map */
                         uint32T startPage,         /* in  first xtnt page */
                         uint32T *allocPageCnt,     /* out  got this many pgs */
                         uint64T dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                         bsAllocHintT alloc_hint    /* in */
                         )
{
    bfMCIdT mcellId;
    uint32T pageCnt = 0;
    uint32T pageOffset;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;
    statusT sts2 = EOK;
    bsInMemSubXtntMapT *subXtntMap = NULL;
    uint32T totalPageCnt = 0;
    uint32T updateStart;
    uint32T firstPg;
    ftxHT ftxH;
    uint32T newtype;
    uint32T newmax;
    bfMCIdT delMcellId;
    vdIndexT delVdIndex;
    vdIndexT allocVdIndex;

    MS_SMP_ASSERT(bfap->bfPageSz == xtntMap->blksPerPage);
    MS_SMP_ASSERT(bfap->dmnP == xtntMap->domain);

    ftxH.hndl = 0;  /* flag that a ftx has or has not been started */

    while ( totalPageCnt < bfPageCnt ) {

	PREEMPT_CHECK(current_thread());

        if ( xtntMap->cnt >= xtntMap->maxCnt ) {
            sts = imm_extend_xtnt_map(xtntMap);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

        if ( xtntMap->cnt == 0 ) {
            /* The first extent may start with a hole. */
            /* In this case startPage will be smaller than bfPageOffset. */
            firstPg = startPage;
        } else {
            prevSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
            firstPg = prevSubXtntMap->pageOffset + prevSubXtntMap->pageCnt;
        }
        subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

        sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 0);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        sts = stg_alloc_new_mcell( bfap,
                                   bfap->bfSetp->dirTag,
                                   bfap->tag,
                                   bfVdIndex,
                                   ftxH,
                                   &mcellId,
                                   FALSE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * NOTE:  The first page of the new sub extent map must be the
         * next page after the last page mapped by the previous sub extent
         * map.
         */
        /*
         * If this file can have extent information in the primary mcell,
         * make the first subextent map type BSR_XTRA_XTNTS.  Otherwise, make
         * it BSR_SHADOW_XTNTS.
         */
        if ( firstPg == 0 &&
             !FIRST_XTNT_IN_PRIM_MCELL(xtntMap->domain->dmnVersion,
                                      bfap->xtnts.type) )
        {
            newtype = BSR_SHADOW_XTNTS;
            newmax = BMT_SHADOW_XTNTS;
        } else {
            newtype = BSR_XTRA_XTNTS;
            newmax = BMT_XTRA_XTNTS;
        }
        sts = imm_init_sub_xtnt_map( subXtntMap,
                                     firstPg,
                                     0,  /* pageCnt */
                                     bfVdIndex,
                                     mcellId,
                                     newtype,
                                     newmax,
                                     0);  /* force default value for maxCnt */
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        subXtntMap->mcellState = NEW_MCELL;

        if ( bfap->xtnts.type == BSXMT_APPEND ) {
            allocVdIndex = -1;
        } else {                       /* BSXMT_STRIPE */
            allocVdIndex = bfVdIndex;
        }
        sts = odm_create_xtnt_rec( bfap,
                                   allocVdIndex,
                                   subXtntMap,
                                   0,  /* no clone xfer xtnts here */
                                   ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /* odm_create_xtnt_map needs to see the last subextent map */
        xtntMap->cnt++;

        if ( xtntMap->cnt == 1 ) {
            /* This is the first mcell of stg. */
            /* Make a header and add this stg list to the ddl. */
            sts = odm_create_xtnt_map( bfap,
                                       bfap->bfSetp,
                                       bfap->tag,
                                       xtntMap,
                                       ftxH,
                                       &delVdIndex,
                                       &delMcellId);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            xtntMap->hdrVdIndex = delVdIndex;
            xtntMap->hdrMcellId = delMcellId;

            sts = del_add_to_del_list( delMcellId,
                                       bfap->dmnP->vdpTbl[delVdIndex - 1],
                                       TRUE,  /* start sub ftx */
                                       ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        } else {
            /* We already have some mcells on the ddl. */
            /* Add this mcell to the list. */
            sts = bmt_link_mcells( bfap->dmnP,
                                   bfap->tag,
                                   xtntMap->subXtntMap[xtntMap->cnt-2].vdIndex,
                                   xtntMap->subXtntMap[xtntMap->cnt-2].mcellId,
                                   subXtntMap->vdIndex,
                                   subXtntMap->mcellId,
                                   subXtntMap->vdIndex,
                                   subXtntMap->mcellId,
                                   ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            if ( xtntMap->subXtntMap[0].type == BSR_SHADOW_XTNTS ) {
                sts = update_mcell_cnt( bfap->dmnP,
                                        bfap->tag,
                                        xtntMap->subXtntMap[0].vdIndex,
                                        xtntMap->subXtntMap[0].mcellId,
                                        BSR_SHADOW_XTNTS,
                                        1,     /* add one more to the list */
                                        ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
            }

        }

        /* cp_stg_alloc_in_one_mcell may stop this transaction and it may */
        /* start another. We pass a pointer to the ftxH so we can know */
        /* if it returned with a transaction in progress or not. */

        if (stg_type == PERM_HOLE_START)
        {
            /* When migrating a clone special care must be taken to also
             * migrate any permenant holes. Backing storage is not required
             * but, the extent record must be there.
             */ 

            sts2 = cp_hole_alloc_in_one_mcell( bfPageOffset + totalPageCnt,
                                               bfPageCnt - totalPageCnt,
                                               subXtntMap,
                                               &pageCnt);
            if ( sts2 != EOK )
            {
                RAISE_EXCEPTION(sts2);
            }
        }
        else
        {

            sts2 = cp_stg_alloc_in_one_mcell( bfap,
                                              bfPageOffset + totalPageCnt,
                                              bfPageCnt - totalPageCnt,
                                              subXtntMap,
                                              &ftxH,
                                              &pageCnt,
                                              dstBlkOffset,
                                              alloc_hint);
            if ( ( sts2 != EOK) && (sts2 != ENO_MORE_BLKS) )
            {
                RAISE_EXCEPTION(sts2);
            }
        }

        if (pageCnt > 0)
        {
            if ( ftxH.hndl ) {
                /* We still have a transaction going. Update the on disk xtnt */
                /* chain and finish the transaction here. */
                sts = update_xtnt_rec(bfap->dmnP, bfap->tag, subXtntMap, ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
                ftx_done_n( ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
                ftxH.hndl=0;
                subXtntMap->updateStart = subXtntMap->cnt;
                subXtntMap->updateEnd = subXtntMap->cnt;
            }

            totalPageCnt = totalPageCnt + pageCnt;
            /* We need to return this now in case we fail on subsequent passes */
            /* thru the loop. */
            *allocPageCnt = totalPageCnt;

            xtntMap->validCnt = xtntMap->cnt;

        }
        /*
         * If we have an ENO_MORE_BLKS return status:
         *
         * If the pageCnt > 0 this pass through the loop was able to get some
         * of the requested space. We want to return the space we got but also return 
         * the error so the caller can take appropriate action (ie try another disk).
         * 
         * If the pageCnt == 0 we were unable to get any storage for this pass thru
         * the loop. We want to undo any resources we obtained for this storage
         * request and return the ENO_MORE_BLKS status to our caller. Note a 
         * previous pass thru the loop may have obtained some storage so we only
         * want to free the resources for this pass thru.
         */
        
        MS_SMP_ASSERT((pageCnt > 0) || (sts2 == ENO_MORE_BLKS));

        if ( sts2 == ENO_MORE_BLKS )
        {
            RAISE_EXCEPTION(sts2);
        }

    }  /* end while */

    return sts2;

HANDLE_EXCEPTION:
    if ( ftxH.hndl != 0 ) {
        ftx_fail(ftxH);
    }

    if ( pageCnt == 0 ) {
        if (xtntMap->cnt > 0) xtntMap->cnt--;
        if ( xtntMap->cnt == 0 ) {
            xtntMap->hdrVdIndex = 0;
            xtntMap->hdrMcellId = bsNilMCId;
        }
        if ( subXtntMap ) {
            imm_unload_sub_xtnt_map(subXtntMap);
        }
    }

    return sts;
}


/*
 * alloc_from_one_disk
 *
 * This function allocates storage from the specified disk and saves
 * the allocation information in the specified sub extent map's extent
 * descriptor array.  It assumes that the specified page offset is
 * greater than or equal to the bitfile's last page offset.
 *
 * This function sets the sub extent map's "updateStart" and "updateEnd"
 * fields to indicate which extent descriptors were modified.  If no
 * descriptors are modified, "updateStart" is set equal to the number
 * of descriptors.
 *
 * This function does not start a sub transaction.  If it has an error,
 * it does not deallocate the storage it has already allocated.  The caller
 * must deallocate the storage calling ftx_fail().
 *
 * The caller must specify a non-nil parent ftx.
 *
 * Successful return status values are EOK and ENO_MORE_BLKS.  If EOK is
 * returned, the "allocPageCnt" parameter has a valid value.
 */
int AdvfsAllocNFS = 1;

statusT
alloc_from_one_disk (
                     bfAccessT *bfap,                 /* in */
                     uint32T bfPageOffset,            /* in */
                     uint32T bfPageCnt,               /* in */
                     int bfPageSize,                  /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     bsAllocHintT *alloc_hint,        /* in/out */
                     ftxHT parentFtx,                 /* in */
                     uint32T *reservedPagesCount,     /* in/out */
                     uint32T *reservedPagesLocation,  /* in/out */
                     uint32T *allocPageCnt            /* out */
                     )
{
    domainT *domain;
    uint32T blkOff;
    uint32T i, j;
    uint32T nextBlkOff;
    uint32T pageCnt;
    statusT sts;
    bsXtntT termDesc;
    uint32T totalPageCnt = 0;
    vdT *vd;
    int pinCnt = 6;    /* no more than 6 pages of SBM pinned */
    int tryToExpandLastExtent = FALSE;
    extern int nfs_max_daemon;

/*
 * From nfs_server.c:
 *
 * A field in the "u" structure is reserved for filesystem usage.  As far
 * as NFS is concerned this field serves 2 purposes:
 * 1) For the nfsd processes this field is set to be a pointer to the daemons
 *    state.  This allows write gathering to query the state of other daemons.
 * 2) Other subordinate filesystems can look at this field to see if the
 *    request is in behalf of a "remote" requestor.  So as one example,
 *    if this field is nonzero the local filesystem knows the request is from
 *    NFS and performs the operation synchronously.
 */
    domain = bfap->dmnP;

    vd = VD_HTOP(subXtntMap->vdIndex, domain);

    /*
     * Save the current termination descriptor and index in case we fail and
     * must restore the sub extent map's previous state.
     */
    subXtntMap->updateStart = subXtntMap->cnt - 1;
    termDesc = subXtntMap->bsXA[subXtntMap->updateStart];

    i = subXtntMap->updateStart;

    if (bfPageOffset == subXtntMap->bsXA[i].bsPage) {
        /*
         * Allocating storage that is virtually contiguous with the
         * bitfile's last page.  Determine if the new extent and the
         * previous extent are physically contiguous on disk.
         */
        if (i > 0) {
            if ( subXtntMap->bsXA[i - 1].vdBlk != XTNT_TERM &&
                 subXtntMap->bsXA[i - 1].vdBlk != PERM_HOLE_START )
            {
                /*
                 * Is there enough room in the extent descriptor array for two
                 * entries, the real and the termination extent descriptors?
                 * If not, then either extend the array and then allocate
                 * some storage or, if the array is already at its on-disk
                 * maximum size, allocate the storage first.  If we get lucky
                 * and the new storage is physically contiguous with the last
                 * piece of storage, we can just extend the last extent.
                 * If we didn't get contiguous storage, remember where it is
                 * because, as Arnold would say, "I'll be back".  We'll
                 * come back in here with a new subexent in just a jiffy and
                 * we can use the reserved storage for that subextent.
                 *
                 * Another solution is to check the storage bitmap for the block
                 * adjacent to the extent's last block.  If the block is free,
                 * then extend the extent.  Otherwise, check the array size and
                 * call stg_alloc_one_xtnt().
                 *
                 * On the surface, this solution seems like the way to
                 * go.  But, if the next block is not in the bitmap cache,
                 * you must read the bitmap page to determine if the block is
                 * allocated.  Unless the bitmap code is improved, for example,
                 * cache all unallocated block information, the current
                 * solution's restriction is a reasonable tradeoff.
                 */

                if (i == (subXtntMap->maxCnt - 1)) {
                    if (subXtntMap->maxCnt == subXtntMap->onDiskMaxCnt) {
                        /*
                         * We know that we can't fit another extent descriptor
                         * into this record.  But if we can get back some 
                         * storage that's physically contiguous to that in 
                         * the last extent descriptor in this record, we can 
                         * just expand that last descriptor.
                         */
                        tryToExpandLastExtent = TRUE;
                    }
                    else {
                        /* One entry left. */
                        sts = imm_extend_sub_xtnt_map (subXtntMap);
                        if (sts != EOK) {
                            RAISE_EXCEPTION (sts);
                        }
                    }
                }

                nextBlkOff = subXtntMap->bsXA[i - 1].vdBlk +
                  ((subXtntMap->bsXA[i].bsPage -
                    subXtntMap->bsXA[i - 1].bsPage) * bfPageSize);


                /*
                 * When dealing with files being written by NFS,
                 * set the alloc_hint and the blkOff such that the
                 * change for contiguous allocation is improved.
                 */
                if (NFS_SERVER_TSD && AdvfsAllocNFS) {
                    blkOff = nextBlkOff;
                    *alloc_hint = BS_ALLOC_NFS;
                }

                /*
                 * Allocate a new extent and determine if it and the previous
                 * extent are physically contiguous on disk.
                 */
                sts = stg_alloc_one_xtnt( bfap,
                                          vd,
                                          bfPageCnt,
                                          0,
                                          *alloc_hint,
                                          parentFtx,
                                          &totalPageCnt,
                                          &blkOff,
                                          &pinCnt);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }

                if (blkOff == nextBlkOff) {
                    /*
                     * The extents are contiguous.  Increase the size of
                     * the previous extent.
                     */
                    subXtntMap->bsXA[i].bsPage = subXtntMap->bsXA[i].bsPage +
                      totalPageCnt;
                } else {
                    /*
                     * The extents are not contiguous.
                     */
                    if (tryToExpandLastExtent) {
                        *reservedPagesCount = totalPageCnt;
                        *reservedPagesLocation = blkOff;
                        RAISE_EXCEPTION(EXTND_FAILURE);
                    }
                    else {
                        subXtntMap->bsXA[i].bsPage = bfPageOffset;
                        subXtntMap->bsXA[i].vdBlk = blkOff;
                        i++;
                    }
                }

                if ( BF_IS_BMT(bfap) && *reservedPagesCount == 0 ) {
                    /* We only need one extent for reserved files.
                     * If reservedPagesCount was passed in > 0,
                     * we will use it here.
                     */
                    goto done;
                }
            }
        }
    } else {
        /*
         * Allocating storage at least one page beyond the bitfile's last page,
         * or within a hole.
         */
        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the hole and the termination extent descriptors?
         * If not, extend the array.
         */
        if (i == (subXtntMap->maxCnt - 1)) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map (subXtntMap);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
        subXtntMap->bsXA[i + 1] = subXtntMap->bsXA[i];
        subXtntMap->bsXA[i + 1].bsPage = bfPageOffset;
        i++;
    }

    while (totalPageCnt < bfPageCnt) {

        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the real and the termination extent descriptors?
         * If not, extend the array.
         */
        if (i == (subXtntMap->maxCnt - 1)) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map (subXtntMap);
            if (sts != EOK) {
                if ((sts == EXTND_FAILURE) && (totalPageCnt > 0)) {
                    /*
                     * No more room in the extent array but there is
                     * more space on the disk.
                     */
                    break;  /* out of while */
                } else {
                    RAISE_EXCEPTION (sts);
                }
            }
        }

        /*
         * When dealing with files being written by NFS,
         * set the alloc_hint and the blkOff such that the
         * chance for contiguous allocation is improved.
         */
        if (NFS_SERVER_TSD && i > 1 && AdvfsAllocNFS) {

            /*
             * calculate the new block offset based on the delta between
             * the requested page and the descriptor info for the final
             * page range (the "i-2" descriptor).
             */
            if (bfPageOffset+totalPageCnt > subXtntMap->bsXA[i-2].bsPage) {
                j = (bfPageOffset+totalPageCnt) - subXtntMap->bsXA[i-2].bsPage;
                blkOff = subXtntMap->bsXA[i-2].vdBlk + j*bfPageSize;
                j = (bfPageOffset+totalPageCnt) - subXtntMap->bsXA[i-1].bsPage;
            }
            else {
                j = subXtntMap->bsXA[i-2].bsPage - (bfPageOffset+totalPageCnt);
                blkOff = subXtntMap->bsXA[i-2].vdBlk - j*bfPageSize;
            }
            if (j && j < nfs_max_daemon) {
                *alloc_hint = BS_ALLOC_NFS;
            }
        }

        /*
         * If we already reserved some storage from the last call in
         * to this routine, use that.  Otherwise, get an extent of
         * storage.
         */
        if (*reservedPagesCount) {
            subXtntMap->bsXA[i].bsPage = bfPageOffset + totalPageCnt;
            subXtntMap->bsXA[i].vdBlk = *reservedPagesLocation;
            totalPageCnt = totalPageCnt + *reservedPagesCount;
            *reservedPagesCount = 0;
            *reservedPagesLocation = 0;
        }
        else {
            sts = stg_alloc_one_xtnt( bfap,
                                      vd,
                                      bfPageCnt - totalPageCnt,
                                      0,
                                      *alloc_hint,
                                      parentFtx,
                                      &pageCnt,
                                      &blkOff,
                                      &pinCnt);
            if (sts != EOK) {
                if ((sts == ENO_MORE_BLKS) && (totalPageCnt > 0)) {
                    break;  /* out of while */
                } else {
                    RAISE_EXCEPTION (sts);
                }
            }
            subXtntMap->bsXA[i].bsPage = bfPageOffset + totalPageCnt;
            subXtntMap->bsXA[i].vdBlk = blkOff;
            totalPageCnt = totalPageCnt + pageCnt;
        }
        i++;

        if ( BF_IS_BMT(bfap) && *reservedPagesCount == 0 ) {
            /* We only need one extent for resevred files. */
            /* If reservedPagesCount was passed in > 0, we will use it here. */
            break;
        }
 
    }  /* end while */

done:
    subXtntMap->bsXA[i].bsPage = bfPageOffset + totalPageCnt;
    subXtntMap->bsXA[i].vdBlk = XTNT_TERM;

    subXtntMap->updateEnd = i;

    subXtntMap->cnt = i + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
                                                    subXtntMap->bsXA[0].bsPage;
    if (pageCnt > subXtntMap->pageCnt) {
        subXtntMap->pageCnt = pageCnt;
    }

    *allocPageCnt = totalPageCnt;

    return EOK;

HANDLE_EXCEPTION:
    /*
     * Restore the sub extent map to its previous state.
     */

    subXtntMap->bsXA[subXtntMap->updateStart] = termDesc;
    subXtntMap->cnt = subXtntMap->updateStart + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
    subXtntMap->updateStart = subXtntMap->cnt;

    if (sts == EXTND_FAILURE) {
        /*
         * An EXTND_FAILURE status value indicates the extent array is full
         * but there may be space on the disk.
         */
        *allocPageCnt = 0;
        sts = EOK;
    }

    return sts;

}  /* end alloc_from_one_disk */


/*
** Allocate as much storage as we can ( up to "bfPageCnt" pages) in one mcell.
** The storage is for the "bfap" file at page "bfPageOffset". The mcell
** has already been allocated. It is represented in memory by "subXtntMap".
** On entry "subXtntMap" is empty.
**
** Within the subextent map (mcell), cp_stg_alloc_in_one_mcell extents the
** subextent (gets another extent) and calls stg_alloc_one_xtnt to fill
** it in. stg_alloc_one_xtnt will only allocate an extent that requires
** pinning "FTX_MX_PINP" pages of SBM, so it is called with a transaction
** handle. cp_stg_alloc_in_one_mcell may stop the current transaction and
** start another one to prevent overflowing the log if we are allocating
** a huge amount of storage. Even though we get limited sized extents from
** stg_alloc_one_xtnt, cp_stg_alloc_in_one_mcell will merge extents together
** to minimize the number of extents used in an mcell. However between
** each root transaction, the extent map (in memory) and the mcell chain
** (on disk) must have a terminating extent map entry.
*/
static
statusT
cp_stg_alloc_in_one_mcell(
                      bfAccessT *bfap,                /* in  add stg this file*/
                      uint32T bfPageOffset,           /* in  start here */
                      uint32T bfPageCnt,              /* in  requested length */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      ftxHT *ftxHp,                   /* in */
                      uint32T *allocPageCnt,          /* out. this much added */
                      uint64T dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                      bsAllocHintT alloc_hint
                      )
{
    domainT *dmnP;
    uint32T blkOff;
    uint32T xI;
    uint32T nextBlkOff;
    uint32T pageCnt=0;
    statusT sts = EOK;
    bsXtntT termDesc;
    uint32T totalPageCnt = 0;
    vdT *vdp;
    int pinCnt = FTX_MX_PINP - 1;

    MS_SMP_ASSERT(bfPageCnt);   /* don't call this routine w/o a request */
    MS_SMP_ASSERT(subXtntMap->cnt == 1);
    MS_SMP_ASSERT(subXtntMap->bsXA[0].bsPage == subXtntMap->pageOffset);
    MS_SMP_ASSERT(bfPageOffset >= subXtntMap->pageOffset);

    dmnP = bfap->dmnP;

    vdp = VD_HTOP(subXtntMap->vdIndex, dmnP);

    xI = 0;
    subXtntMap->updateStart = 0;
    termDesc = subXtntMap->bsXA[0];
    *allocPageCnt = 0;

    while ( totalPageCnt < bfPageCnt ) {

        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the real and the termination extent descriptors?
         * If not, extend the array.
         */
        if ( xI >= subXtntMap->maxCnt - 2 ) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map(subXtntMap);
            if ( sts == EXTND_FAILURE && totalPageCnt > 0 ) {
                /*
                 * No more room in the extent array but there is
                 * more space on the disk.
                 */
                break;  /* out of while */
            } else if ( xI >= (subXtntMap->maxCnt - 2) && totalPageCnt > 0 ) {
                sts = EXTND_FAILURE;
                break;  /* out of while */
            } else if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

        /* Get as much as we can of the request in one extent. */
        /* However we are also limited to pining "pinCnt" pages of SBM */
        /* so very large request may not be filled even if there is */
        /* that much free storage. */
        sts = stg_alloc_one_xtnt( bfap,
                                  vdp,
                                  bfPageCnt - totalPageCnt,
                                  dstBlkOffset,
                                  alloc_hint,
                                  *ftxHp,
                                  &pageCnt,
                                  &blkOff,
                                  &pinCnt);
        if ( sts == ENO_MORE_BLKS && totalPageCnt > 0 ) {
            break;  /* out of while */
        } else if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        sts = add_extent( bfap,
                          subXtntMap,
                          &xI,
                          bfPageOffset + totalPageCnt,
                          blkOff,
                          pageCnt,
                          ftxHp,
                          &termDesc,
                          &pinCnt);
        totalPageCnt += pageCnt;
        if ( sts != EOK ) {
            /* The only failure here is a failure to start a new ftx. */
            /* We need to tell the caller we got some storage. */
            *allocPageCnt = totalPageCnt;
            RAISE_EXCEPTION(sts);
        }
    }  /* end while */

    /* We got all the storage we wanted or */
    /* ran out of extents in the subextent map (mcell) or */
    /* ran out of storage. In any case we are done adding storage to this */
    /* mcell. Wrap it up. */
    xI++;
    subXtntMap->bsXA[xI].bsPage = bfPageOffset + totalPageCnt;
    MS_SMP_ASSERT(subXtntMap->bsXA[xI].bsPage <= max_page + 1);
    subXtntMap->bsXA[xI].vdBlk = XTNT_TERM;

    subXtntMap->updateEnd = xI;

    subXtntMap->cnt = xI + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
                                                    subXtntMap->bsXA[0].bsPage;
    if (pageCnt > subXtntMap->pageCnt) {
        subXtntMap->pageCnt = pageCnt;
    }

    *allocPageCnt = totalPageCnt;

    if (sts == ENO_MORE_BLKS)
        return sts;
    else
        return EOK;

HANDLE_EXCEPTION:
    /*
     * Restore the sub extent map to its previous state.
     */

    subXtntMap->bsXA[subXtntMap->updateStart] = termDesc;
    subXtntMap->cnt = subXtntMap->updateStart + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
    subXtntMap->updateStart = subXtntMap->cnt;

    if ( sts == EXTND_FAILURE ) {
        /*
         * An EXTND_FAILURE status value indicates the extent array is full
         * but there may be space on the disk.
         */
        *allocPageCnt = 0;
        sts = EOK;
    }

    return sts;
}


/*
 * This routine appends a permanent hole onto the end of the
 * passed in extent map. If the extent can not be extened we
 * let our caller get a new mcell and then call back in.
 */
static
statusT
cp_hole_alloc_in_one_mcell(
                      uint32T bfPageOffset,           /* in  start here */
                      uint32T bfPageCnt,              /* in  requested length */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      uint32T *allocPageCnt           /* out. this much added */
                      )
{
    uint32T blkOff;
    uint32T xI;
    uint32T nextBlkOff;
    uint32T pageCnt;
    statusT sts = EOK;
    uint32T totalPageCnt = 0;

    MS_SMP_ASSERT(bfPageCnt);   /* don't call this routine w/o a request */
    MS_SMP_ASSERT(subXtntMap->cnt == 1);
    MS_SMP_ASSERT(subXtntMap->bsXA[0].bsPage == subXtntMap->pageOffset);
    MS_SMP_ASSERT(bfPageOffset >= subXtntMap->pageOffset);

    /* Currently we are only ever coming in here with a new mcell.
     * but this is coded to handle a partially full subextent map.
     */

    /*
     * Is there enough room in the extent descriptor array for two
     * entries, the real and the termination extent descriptors?
     * If not, extend the array.
     */

    xI = subXtntMap->cnt-1;
    
    if (subXtntMap->bsXA[xI].bsPage < bfPageOffset)
    {
        /* We may come in here with a trailing hole, which
         * we must preserve. So use the next extent entry
         */

        xI++;
    }

    if ( xI >= subXtntMap->maxCnt - 2 ) 
    {
        /* One entry left. */
        sts = imm_extend_sub_xtnt_map(subXtntMap);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }

    subXtntMap->updateStart = xI;

    subXtntMap->bsXA[xI].bsPage = bfPageOffset;
    subXtntMap->bsXA[xI].vdBlk = PERM_HOLE_START;

    xI++;

    subXtntMap->bsXA[xI].bsPage = bfPageOffset + bfPageCnt;
    MS_SMP_ASSERT(subXtntMap->bsXA[xI].bsPage <= max_page + 1);
    subXtntMap->bsXA[xI].vdBlk = XTNT_TERM;

    subXtntMap->updateEnd = xI;

    subXtntMap->cnt = xI + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
                                                    subXtntMap->bsXA[0].bsPage;
    if (pageCnt > subXtntMap->pageCnt) {
        subXtntMap->pageCnt = pageCnt;
    }

    *allocPageCnt = bfPageCnt;

    return sts;

HANDLE_EXCEPTION:
    /*
     * Restore the sub extent map to its previous state.
     */

    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    if ( sts == EXTND_FAILURE ) {
        /*
         * An EXTND_FAILURE status value indicates the extent array is full
         * but there may be space on the disk.
         */
        *allocPageCnt = 0;
        sts = EOK;
    }

    return sts;
}


/*
** Add an extent to the subextent map "subXtntMap" at index "xIp".
** The subextent map belongs to file "bfap".
** The extent starts at file page "pgOff" and at disk block "blkOff".
** The extent is "pgCnt" page long.
** If we have pinned the maximum pages then we need to tidy up the extents,
** write them to disk and start a new transaction.
*/
static
statusT
add_extent(
            bfAccessT *bfap,
            bsInMemSubXtntMapT *subXtntMap,  /* add extent to this map */
            uint32T *xIp,                    /* add extent at this index */
            uint32T pgOff,                   /* first page of extent */
            uint32T blkOff,                  /* first block of extent */
            uint32T pgCnt,                   /* extent to be this long */
            ftxHT *ftxHp,
            bsXtntT *termDescp,
            int *pinCntp)
{
    uint32T nextBlkOff;
    uint32T subPgCnt;
    statusT sts;

    MS_SMP_ASSERT(pgOff <= max_page);
    MS_SMP_ASSERT(pgCnt <= max_page + 1);
    MS_SMP_ASSERT(pgOff + pgCnt <= max_page + 1);
    MS_SMP_ASSERT(*xIp < subXtntMap->maxCnt - 2);

    /* Add the extent just obtained, possibly merging with the previous. */
    if ( *xIp == 0 ) {
        if ( subXtntMap->bsXA[0].bsPage == pgOff ) {
            subXtntMap->bsXA[0].vdBlk = blkOff;
        } else {
            *xIp = 1;
            subXtntMap->bsXA[1].bsPage = pgOff;
            subXtntMap->bsXA[1].vdBlk = blkOff;
        }
    } else {
        nextBlkOff = subXtntMap->bsXA[*xIp].vdBlk +
          (pgOff - subXtntMap->bsXA[*xIp].bsPage) * bfap->bfPageSz;
        if ( blkOff != nextBlkOff ) {
            *xIp += 1;
            subXtntMap->bsXA[*xIp].bsPage = pgOff;
            subXtntMap->bsXA[*xIp].vdBlk = blkOff;
        }
    }

    if ( *pinCntp == 0 ) {
        /* Can't pin any more pages in this transaction. */
        /* Have to wrap up this subextent and finish the transaction. */
        subXtntMap->bsXA[*xIp + 1].bsPage = pgOff + pgCnt;
        subXtntMap->bsXA[*xIp + 1].vdBlk = XTNT_TERM;
        subXtntMap->updateEnd = *xIp + 1;
        subXtntMap->cnt = *xIp + 2;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
        sts = update_xtnt_rec(bfap->dmnP, bfap->tag, subXtntMap, *ftxHp);
        ftx_done_n (*ftxHp, FTA_BS_STG_ALLOC_MCELL_V1);

        /* All of the previous storage is committed to memory on the */
        /* deferred list. Now lets start a new transaction and add */
        /* more storage. */
        subXtntMap->cnt = *xIp + 1;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
        subXtntMap->updateStart = *xIp + 1;
        *termDescp = subXtntMap->bsXA[*xIp + 1];
        subPgCnt = subXtntMap->bsXA[*xIp + 1].bsPage -
                    subXtntMap->bsXA[0].bsPage;
        if ( subPgCnt > subXtntMap->pageCnt ) {
            subXtntMap->pageCnt = subPgCnt;
        }
        *pinCntp = FTX_MX_PINP - 1;;
        ftxHp->hndl = 0;
        sts = FTX_START_N(FTA_NULL, ftxHp, FtxNilFtxH, bfap->dmnP, 0);
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }
    }

    return EOK;

HANDLE_EXCEPTION:
    return sts;
}


/*
** Alloc one extent of "bfPageCnt" pages max. The extent is located
** and removed from the SBM. It is not put in a subextent or mcell.
** "allocPageCntp" pages allocated from the "vdp" volume starting
** at block "allocBlkOffp". Only "*pinCntp" pages of SBM may be pined.
** This is a running number that is decremented and returned.
*/
static
statusT
stg_alloc_one_xtnt(
                   bfAccessT *bfap,          /* in  find stg for this file */
                   vdT *vdp,                 /* in  in this volume */
                   uint32T bfPageCnt,        /* in  Requested length */
                   uint64T dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                   bsAllocHintT alloc_hint,  /* in */
                   ftxHT ftxH,               /* in */
                   uint32T *allocPageCntp,   /* out  Got this much stg. */
                   uint32T *allocBlkOffp,    /* in/out  At this location.*/
                   int *pinCntp              /* in/out  this many pins left */
                   )
{
    uint32T blkCnt;
    domainT *dmnP;
    uint32T pageCnt;
    uint32T requestedBlkCnt;
    void *stgDesc;
    statusT sts;
    int bfPageSize = bfap->bfPageSz;

    MS_SMP_ASSERT(ftxH.hndl <= FTX_MAX_FTXH && ftxH.hndl > 0);
    MS_SMP_ASSERT(ftxH.dmnP);

    dmnP = bfap->dmnP;
    MS_SMP_ASSERT(dmnP == ftxH.dmnP);

    /*
     * Try to find space for the requested number of pages.
     * sbm_find_space() returns NULL if there is not enough
     * space on the virtual disk.  Otherwise, it returns,
     * in "blkCnt", the largest contiguous block that can be
     * allocated.  This value may or may not be large enough
     * to satisfy the request.
     */

    STGMAP_LOCK_WRITE(&vdp->stgMap_lk)

    pageCnt = bfPageCnt;
    requestedBlkCnt = pageCnt * bfPageSize;

retry:
    stgDesc = sbm_find_space( vdp,
                              requestedBlkCnt,
                              dstBlkOffset,
                              alloc_hint,
                              allocBlkOffp,
                              &blkCnt);
    if(( stgDesc == NULL ) &&
       (alloc_hint != BS_ALLOC_MIG_RSVD) &&
       (vdp->freeMigRsvdStg.num_clust != 0) ) {
        /* if this is a non-smartstore request and it didn't find stg,
         * and smartstore currently has some stg locked;  take away
         * the storage reserved for smartstore and then retry the find.
         *
         * The next time smartstore goes to allocate storage in this
         * region that it expects to have reserved,  the blkCnt <
         * requestedBlkCnt, and we exit this routine below with a
         * ENO_MORE_BLKS error.  Smartstore ignores the error by
         * aborting the whole migrate cycle and picks a new file.
         * The file will go back on the frag list when it is next
         * accessed.
         */
        vdp->freeMigRsvdStg.start_clust = 0;
        vdp->freeMigRsvdStg.num_clust   = 0;
        SS_TRACE(vdp,0,0,0,0);
        goto retry;
    }

    if(( stgDesc == NULL ) ||
       ((alloc_hint == BS_ALLOC_MIG_RSVD) && (blkCnt < requestedBlkCnt)) ) {
        /*
         * The disk does not have 'requestedBlkCnt' free blks.
         */
        STGMAP_UNLOCK(&vdp->stgMap_lk)
        SS_TRACE(vdp,stgDesc,alloc_hint,blkCnt,requestedBlkCnt);
        sts = ENO_MORE_BLKS;
        goto EXIT_ALLOC_FROM_BITMAP;
    }

    /* We can only change so many pages of the SBM at once. If a really big */
    /* request comes in, reduce the number of blocks in one transaction. */
    sbm_howmany_blks( *allocBlkOffp, &blkCnt, pinCntp, vdp, bfPageSize);

    if ( blkCnt != requestedBlkCnt ) {
        MS_SMP_ASSERT(blkCnt < requestedBlkCnt);

        /*
         * Round the page and block counts down so that they are
         * a multiple of pages.
         */
        pageCnt = blkCnt / bfPageSize;
        blkCnt = pageCnt * bfPageSize;
        if ( blkCnt < 1 ) {
            /*
             * The largest contiguous set of free blks is less than
             * the number of blks per bitfile page.  Disk VERY fragmented.
             */
            STGMAP_UNLOCK(&vdp->stgMap_lk)
            sts = ENO_MORE_BLKS;
            goto EXIT_ALLOC_FROM_BITMAP;
        }
    }

    sts = sbm_remove_space( vdp,
                            *allocBlkOffp,
                            blkCnt,
                            stgDesc,
                            ftxH,
                            alloc_hint==BS_ALLOC_NFS);
    if ( sts != EOK) {
        if ( sts != E_DOMAIN_PANIC ) {
            domain_panic(dmnP, "stg_alloc_one_xtnt: sbm_remove_space failed");
        } 
        sts = EIO;
    } 

    STGMAP_UNLOCK(&vdp->stgMap_lk)

    if (bfap->xtnts.type == BSXMT_APPEND) 
        bfap->xtnts.xtntMap->allocVdIndex = vdp->vdIndex;

    *allocPageCntp = pageCnt;

EXIT_ALLOC_FROM_BITMAP:

    return sts;
}


/* This is a wrapper for xfer_stg_to_bf. It accepts one extent of disk
/* storage and adds it to the extent map for bfap.
*/
statusT
xfer_stg (
          bfAccessT  *bfap,         /* file to add storage to */
          ulong_t    bfPageOffset,  /* offset to add storage */
          ulong_t    bfPageCnt,     /* number of pages to add */
          uint32T    startblk,      /* storage in hand to add */
          vdIndexT   vdIndex,       /* storage is on this disk */
          ftxHT      parentFtx
         )
{
    int failFtxFlag = 0;
    ftxHT ftxH;
    statusT sts;
    addStgUndoRecT undoRec;
    bsInMemXtntT *xtnts;
    int xtntMap_locked = 0;

    xtnts = &(bfap->xtnts);

    /*
     * This transaction ties all of the "xfer storage" sub transactions
     * together so that if there is an error, this function calls ftx_fail()
     * to back out the on-disk modifications.  Easy as 1-2-3.
     */
    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtx, bfap->dmnP, 0);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with mcellList_lk write-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Add this lock to our transaction list so it can be released
     * when the transaction completes.
     */
    FTX_ADD_LOCK(&(bfap->mcellList_lk), ftxH)

    /* Need to lock this before we modify any of the extents or
     * subextents for this file in merge_xtnt_maps().
     */
    XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )
    xtntMap_locked = 1;

    MS_SMP_ASSERT(BS_BFTAG_RSVD(bfap->tag) == 0);
    sts = xfer_stg_to_bf( bfap,
                          bfPageOffset,
                          bfPageCnt,
                          startblk,
                          vdIndex,
                          ftxH);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * This function can't fail from this point on.  This is because we
     * modify the in-memory extent maps and the changes cannot be backed out.
     */

    merge_xtnt_maps(xtnts);

    xtnts->allocPageCnt += bfPageCnt;

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    xtntMap_locked = 0;

    undoRec.nextPage = bfap->nextPage;

    if ( (bfPageOffset + bfPageCnt) > bfap->nextPage ) {
        bfap->nextPage = bfPageOffset + bfPageCnt;
    }

    bs_bfs_get_set_id(bfap->bfSetp, &undoRec.setId);
    undoRec.tag = bfap->tag;
    undoRec.pageOffset = bfPageOffset;
    undoRec.pageCnt = bfPageCnt;
    ftx_done_u(ftxH, FTA_BS_STG_ADD_V1, sizeof(undoRec), &undoRec);

    return EOK;

HANDLE_EXCEPTION:

    if ( xtntMap_locked ) {
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
        xtntMap_locked = 0;
    }

    if ( failFtxFlag != 0 ) {
        ftx_fail(ftxH);
    }

    return sts;
}


/* Deal with striped vs append extent maps
 */
static statusT
xfer_stg_to_bf (
                bfAccessT *bfap,         /* add storage to this file */
                ulong_t   bfPageOffset,  /* at this page offset */
                ulong_t   bfPageCnt,     /* for this many pages */
                uint32T   startblk,      /* storage exists at this block */
                vdIndexT  vdIndex,       /* on this disk */
                ftxHT     parentFtx
                )
{
    uint32T i;
    int mapIndex;
    statusT sts;
    bsInMemXtntMapT *xtntMap;
    bsInMemXtntT *xtnts = &bfap->xtnts;

    switch ( xtnts->type ) {
      case BSXMT_APPEND:
        xtntMap = xtnts->xtntMap;
        xtntMap->updateStart = xtntMap->cnt;
        sts = xfer_stg_to_xtnts( bfap,
                                 xtntMap,
                                 bfPageOffset,
                                 bfPageCnt,
                                 startblk,
                                 vdIndex,
                                 parentFtx);
        break;

      case BSXMT_STRIPE:
        mapIndex = -1;
        for ( i = 0; i < xtnts->stripeXtntMap->cnt; i++ ) {
            if ( xtnts->stripeXtntMap->xtntMap[i]->allocVdIndex == vdIndex ) {
                mapIndex = i;
                break;
            }
        }
        if ( mapIndex == -1 ) {
            return EBAD_VDI;
        }
        xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];
        xtntMap->updateStart = xtntMap->cnt;
        sts = xfer_stg_to_xtnts( bfap,
                                 xtntMap,
                                 bfPageOffset,
                                 bfPageCnt,
                                 startblk,
                                 vdIndex,
                                 parentFtx);
        break;

      default:
        sts = ENOT_SUPPORTED;
    }  /* end switch */

    return sts;
}


/* Find the subextent map to modify. If bfPageOffset is in the range mapped by
 * xtntMap then also find the descriptor that maps the page or is before the
 * page.
 */
static statusT
xfer_stg_to_xtnts (
                   bfAccessT        *bfap,        /* Add stg to this file */
                   bsInMemXtntMapT  *xtntMap,     /* in this extent map */
                   ulong_t          bfPageOffset, /* Add storage at this page */
                   ulong_t          bfPageCnt,    /* add this many pages */
                   uint32T          startblk,     /* Storage is at this block */
                   vdIndexT         vdIndex,      /* on this disk */
                   ftxHT            parentFtx
                   )
{
    uint32T descIndex;
    uint32T holePageCnt;
    uint32T mapIndex;
    uint32T pageCnt;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt = 0;

    sts = imm_page_to_sub_xtnt_map( bfPageOffset, xtntMap, 0, &mapIndex);
    /* returns EOK or E_PAGE_NOT_MAPPED */
    if ( sts == E_PAGE_NOT_MAPPED ) {
        /* imm_page_to_sub_xtnt_map returns E_PAGE_NOT_MAPPED if */
        /* bfPageOffset is before the first page of the first subextent or */
        /* after the last page of the last (valid) subextent. However, */
        /* the extent map always starts at 0, if only with a termination */
        /* extent (vdBlk == -1), so bfPageOffset is after the last extent. */
        /* Append page after the last page mapped by the last sub extent map. */
        sts = xfer_append_stg( bfap,
                               xtntMap,
                               bfPageOffset,
                               bfPageCnt,
                               startblk,
                               vdIndex,
                               parentFtx);
    } else if ( sts == EOK ) {
        /* The page is mapped by a sub extent map. */
        subXtntMap = &(xtntMap->subXtntMap[mapIndex]);
        sts = imm_page_to_xtnt( bfPageOffset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_PAGE_HOLE */
                                &descIndex);
        /* we know the page is not mapped but is within this subextent */
        MS_SMP_ASSERT( sts != EOK );
        MS_SMP_ASSERT( sts != E_PAGE_NOT_MAPPED );
        if ( sts == E_PAGE_HOLE ) {
            /*
             * The page is mapped by the sub extent map and the extent
             * descriptor describes a hole.
             */
            sts = xfer_hole_stg( bfap,
                                 xtntMap,
                                 mapIndex,
                                 descIndex,
                                 bfPageOffset,
                                 bfPageCnt,
                                 startblk,
                                 vdIndex,
                                 parentFtx);
        }
    }

    return sts;
}


/* Copies last valid subextent map (xtntMap->validCnt - 1) to the last
 * subextent map (xtntMap->cnt - 1) and modifies the copied map.
 * Sets xtntMap->origStart, xtntMap->origEnd
 */
static statusT
xfer_append_stg (
                 bfAccessT       *bfap,        /* add storage to this file */
                 bsInMemXtntMapT *xtntMap,     /* in this xtntMap */
                 ulong_t         bfPageOffset, /* add at this page offset */
                 ulong_t         bfPageCnt,    /* add this many pages */
                 uint32T         startblk,     /* The storage is at this blk */
                 vdIndexT        vdIndex,      /* on this disk */
                 ftxHT           parentFtx
                )
{
    bfSetT *bfSetDesc;
    uint32T i;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T mapIndex = xtntMap->validCnt - 1;

    xtntMap->origStart = mapIndex;
    xtntMap->origEnd = mapIndex;

    bfSetDesc = bfap->bfSetp;

    /*
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */

    i = xtntMap->cnt;
    if ( i >= xtntMap->maxCnt ) {
        sts = imm_extend_xtnt_map(xtntMap);
        if ( sts != EOK ) {
            return sts;
        }
    }

    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtntMap,
                                 &xtntMap->subXtntMap[mapIndex],
                                 &xtntMap->subXtntMap[i]);
    if ( sts != EOK ) {
        return sts;
    }
    xtntMap->cnt++;

    subXtntMap = &(xtntMap->subXtntMap[i]);
    if ( (subXtntMap->cnt == 1) && (i == 1) &&
         (subXtntMap->type == BSR_SHADOW_XTNTS) ) {
        /*
         * This is a normal bitfile and it does not have any storage
         * allocated to it.  If needed, allocate an mcell for the sub
         * extent map before allocating storage.
         *
         * If the bitfile has never had any storage allocated to it,
         * the sub extent map's mcell id is nil.  Otherwise, if all
         * the storage has been removed from the bitfile, the sub extent
         * map's mcell id is valid.
         */
        if ( (subXtntMap->mcellId.page == bsNilMCId.page) &&
             (subXtntMap->mcellId.cell == bsNilMCId.cell) ) {

            subXtntMap->vdIndex = vdIndex;
            sts = stg_alloc_new_mcell( bfap,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       vdIndex,
                                       parentFtx,
                                       &subXtntMap->mcellId,
                                       FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION (sts);
            }
            subXtntMap->mcellState = NEW_MCELL;
            xtntMap->hdrVdIndex = subXtntMap->vdIndex;
            xtntMap->hdrMcellId = subXtntMap->mcellId;
        }
    }

    sts = xfer_stg_on_one_disk( bfap,
                                bfSetDesc->dirTag,
                                xtntMap,
                                bfPageOffset,
                                bfPageCnt,
                                startblk,
                                vdIndex,
                                parentFtx);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                    bfap,
                                    xtntMap,
                                    parentFtx);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    return sts;

HANDLE_EXCEPTION:
    unload_sub_xtnt_maps (xtntMap);

    return sts;
}


static statusT
xfer_hole_stg (
              bfAccessT       *bfap,         /* add storge to this file */
              bsInMemXtntMapT *xtntMap,      /* in this xtnt map */
              uint32T         mapIndex,      /* at this subextent */
              uint32T         descIndex,     /* before this descriptor */
              ulong_t         bfPageOffset,  /* add storge at this page */
              ulong_t         bfPageCnt,     /* add this many pages */
              uint32T         startblk,      /* storage is at this blk */
              vdIndexT        vdIndex,       /* on this disk */
              ftxHT           parentFtx
             )
{
    bfSetT *bfSetDesc;
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32T i;
    bsInMemSubXtntMapT *newSubXtntMap;
    bsInMemSubXtntMapT *oldSubXtntMap;
    uint32T pageCnt;
    statusT sts;
    bfMCIdT reuseMcellId = bsNilMCId;
    int added_perm_hole=0;
    int savedMapCnt;

    xtntMap->origStart = mapIndex;
    xtntMap->origEnd = mapIndex;

    /*
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */
    if ( xtntMap->cnt >= xtntMap->maxCnt ) {
        sts = imm_extend_xtnt_map (xtntMap);
        if ( sts != EOK ) {
            return sts;
        }
    }

    /* copy sub extent to be modified to sub after last valid sub */
    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtntMap,
                                 &xtntMap->subXtntMap[mapIndex],
                                 &xtntMap->subXtntMap[xtntMap->cnt]);
    if ( sts != EOK ) {
        return sts;
    }
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];
    xtntMap->cnt++;
    savedMapCnt = xtntMap->cnt;

    /*
     * Make the hole descriptor into the termination descriptor.  This is
     * needed so that the storage information is added correctly in the sub
     * extent map.
     */
    /* truncate extent map, only those subs thru the modified sub are valid */
    newSubXtntMap->cnt = descIndex + 1;
    MS_SMP_ASSERT(newSubXtntMap->cnt <= newSubXtntMap->maxCnt);

    /* update will start after the modified sub extent */
    newSubXtntMap->updateStart = newSubXtntMap->cnt;
    newSubXtntMap->pageCnt = newSubXtntMap->bsXA[newSubXtntMap->cnt-1].bsPage -
                             newSubXtntMap->pageOffset;

    bfSetDesc = bfap->bfSetp;

    /* this will modify the last subextent and change updateStart & updateCnt */
    sts = xfer_stg_on_one_disk( bfap,
                                bfSetDesc->dirTag,
                                xtntMap,
                                bfPageOffset,
                                bfPageCnt,
                                startblk,
                                vdIndex,
                                parentFtx);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    /* xfer_stg_on_one_disk can remap sub maps into new mem addr if
     * another submap was added.  This is the case when storage being
     * added to the clone is on a different vd than the previous submap
     * that is already in the clone. Reassign ptr just in case.
     */
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->updateStart];

    /* If there is new sub maps, no need for added_perm_hole code & test
     * since two perm holes cannot be adjacently spanning two volumes
     * because a hole cannot be the last thing in a sub map.
     * Test for new maps added by comparing saved mapcnt above with new mapcnt.
     */
    if(savedMapCnt == xtntMap->cnt) {
        MS_SMP_ASSERT(newSubXtntMap->cnt >= 2);
        if (newSubXtntMap->bsXA[newSubXtntMap->cnt-2].vdBlk == PERM_HOLE_START) {
            added_perm_hole=1;
        }
    }

    if ( newSubXtntMap->updateStart == newSubXtntMap->cnt ) {
        /*
         * No new storage was allocated to the first update sub extent map.
         * Most likely is that storage was allocated on a different disk.
         * Set the update range so that the termination extent descriptor
         * from above is copied to the on-disk record and the entry count
         * is updated.
         */
        if((newSubXtntMap->cnt == 1) &&
           (newSubXtntMap->type != BSR_SHADOW_XTNTS))
        {
            /* We have an orphan subextentmap. This happens if we are 
             * adding storage into a hole that lives in the first
             * descriptor of a subextent map and there was no more
             * storage available. We will remove this from the update
             * range, unlink it from the on-disk mcell chain and mark 
             * it to be reused after the new storage descriptors.
             *
             * We will orphan a shadow extent since it is the first
             * extent in the map. This is legal. It would be too much
             * headache to try to do otherwise.
             */
            reuseMcellId= newSubXtntMap->mcellId;

            /* Since this only occurs with an extent that starts
             * with a hole we know we have a previous sub xtnt.
             */
            MS_SMP_ASSERT(xtntMap->origStart > 0);

            sts= bmt_unlink_mcells(bfap->dmnP,
                                   bfap->tag,
                                   xtntMap->subXtntMap[xtntMap->origStart-1].vdIndex,
                                   xtntMap->subXtntMap[xtntMap->origStart-1].mcellId,
                                   newSubXtntMap->vdIndex,
                                   newSubXtntMap->mcellId,
                                   newSubXtntMap->vdIndex,
                                   newSubXtntMap->mcellId,
                                   parentFtx);
            if (sts != EOK)
            {
                RAISE_EXCEPTION (sts);
            }
                              
            imm_unload_sub_xtnt_map(newSubXtntMap);
            newSubXtntMap = NULL;
            xtntMap->updateStart++;
        }
        else
        {
            /*
             * Set the update range so that the termination extent descriptor
             * from above is copied to the on-disk record and the entry count
             * is updated.
             */

            newSubXtntMap->updateStart = newSubXtntMap->cnt - 1;
            newSubXtntMap->updateEnd = newSubXtntMap->updateStart;
        }
    }

    /*
     * We added storage in a hole mapped by the extent array.  Now, copy
     * the extent descriptors positioned after the hole descriptor.
     */

    oldSubXtntMap = &xtntMap->subXtntMap[mapIndex];
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->updateEnd];

    if ( newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage <
         oldSubXtntMap->bsXA[descIndex + 1].bsPage ) {
        /*
         * The last new page allocated and the next page after the hole are
         * not contiguous, we need to create a hole descriptor and copy it to
         * the end of the extent map.
         */
        imm_split_desc ( newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage,
                         xtntMap->blksPerPage,
                         &oldSubXtntMap->bsXA[descIndex],  /* orig */
                         &desc,                            /* first part */
                         &bsXA[0] );                       /* last part */
        bsXA[1].bsPage = oldSubXtntMap->bsXA[descIndex + 1].bsPage;
        bsXA[1].vdBlk = XTNT_TERM;
        sts = imm_copy_xtnt_descs ( oldSubXtntMap->vdIndex,
                                    &bsXA[0],
                                    1,
                                    TRUE,
                                    xtntMap,
                                    bfap->xtnts.type);
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }
        oldSubXtntMap = &(xtntMap->subXtntMap[mapIndex]);

    } else if (added_perm_hole &&
               oldSubXtntMap->bsXA[descIndex+1].vdBlk == PERM_HOLE_START) {

        /*
         * If we just added a permanent hole (i.e., a PERM_HOLE_START entry
         * followed by an XTNT_TERM entry), and the extent following it
         * is a permanent hole, do not copy the second permanent hole
         * entry.
         *
         * Instead, advance the descIndex pointer so that it points
         * to the entry following the second permanent hole.  Update
         * the bsPage field of the XTNT_TERM entry following the new
         * permanent hole with the bsPage value of the entry following
         * the second permanent hole.
         *
         * imm_copy_xtnt_descs() will make sure the XTNT_TERM entry
         * following the new permanent hole will contain the vdBlock
         * value of the entry following the second permanent hole,
         * if this latter value is not XTNT_TERM.
         */

        MS_SMP_ASSERT
            (newSubXtntMap->bsXA[newSubXtntMap->cnt-1].vdBlk == XTNT_TERM);

        descIndex++;

        newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage =
            oldSubXtntMap->bsXA[descIndex+1].bsPage;

    }
    descIndex++;

    /*
     * Copy the remaining entries from the split sub extent map.
     */
    sts = imm_copy_xtnt_descs ( oldSubXtntMap->vdIndex,
                                &oldSubXtntMap->bsXA[descIndex],
                                oldSubXtntMap->cnt - 1 - descIndex,
                                TRUE,
                                xtntMap,
                                bfap->xtnts.type);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    /*
     * Set the last update sub extent map's "updateEnd" field as
     * imm_copy_xtnt_descs() may have modified the map.
     */

    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->updateEnd];
    newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
    newSubXtntMap->pageCnt =
        newSubXtntMap->bsXA[newSubXtntMap->updateEnd].bsPage -
        newSubXtntMap->pageOffset;

    /*
     * Allocate new mcells for any new sub extent maps that were added
     * by imm_copy_xtnt_descs().
     */

    for ( i = xtntMap->updateEnd + 1; i < xtntMap->cnt; i++ ) {
        newSubXtntMap = &xtntMap->subXtntMap[i];
        if ((!MCID_EQL(reuseMcellId, bsNilMCId)) &&
            (i==xtntMap->cnt-1))
        {
            /* Reuse the mcell from the orphan. We know
             * this must go on the last MCELL on the update
             * list.
             */
            
            newSubXtntMap->mcellId = reuseMcellId;
            newSubXtntMap->mcellState = USED_MCELL;
        }
        else
        {
        /* for new in memory sub extent */
            sts = stg_alloc_new_mcell ( bfap,
                                        bfSetDesc->dirTag,
                                        bfap->tag,
                                        newSubXtntMap->vdIndex,
                                        parentFtx,
                                        &newSubXtntMap->mcellId,
                                        TRUE );
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            newSubXtntMap->mcellState = NEW_MCELL;
        }

        newSubXtntMap->updateStart = 0;
        newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
    }  /* end for */

    xtntMap->updateEnd = xtntMap->cnt - 1;

    sts = x_update_ondisk_xtnt_map ( bfap->dmnP,
                                     bfap,
                                     xtntMap,
                                     parentFtx );
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    return sts;

HANDLE_EXCEPTION:
    xtntMap->updateEnd = xtntMap->cnt - 1;
    unload_sub_xtnt_maps (xtntMap);

    if ( newSubXtntMap ) {
        imm_unload_sub_xtnt_map(newSubXtntMap);
    }

    return sts;
}


/* Updates last (xtntMap->cnt - 1) subextent.
 * Changes xtntMap->updateStart, xtntMap->updateEnd, xtntMap->end
 */
static statusT
xfer_stg_on_one_disk (
                      bfAccessT       *bfap,        /* Add stg to this file */
                      bfTagT          bfSetTag,     /* in this file set */
                      bsInMemXtntMapT *xtntMap,     /* using this map */
                      ulong_t         bfPageOffset, /* at this file page */
                      ulong_t         bfPageCnt,    /* for this many pages */
                      ulong_t         startBlk,     /* stg starts at this blk */
                      vdIndexT        vdIndex,      /* on this disk */
                      ftxHT           parentFtx
                     )
{
    ftxHT ftxH;
    bfMCIdT mcellId;
    uint32T pageCnt;
    uint32T pageOffset;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt = 0;
    uint32T updateStart;

    MS_SMP_ASSERT(parentFtx.hndl != 0);

    updateStart = xtntMap->cnt - 1;

    subXtntMap = &(xtntMap->subXtntMap[updateStart]);

    if ( subXtntMap->vdIndex == vdIndex ) {
        /*
         * The last sub extent map's disk matches the specified disk.
         * Allocate storage and save the information in the last map.
         */
        /* may change cnt, updateCnt & pageCnt */
        sts = xfer_stg_in_one_subxtnt ( subXtntMap,
                                        bfap->bfPageSz,
                                        bfPageOffset,
                                        bfPageCnt,
                                        startBlk );
        if ( sts == EXTND_FAILURE ) {
            goto tryagain;
        }
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }
    } else {
tryagain:
        if ( xtntMap->cnt >= xtntMap->maxCnt ) {
            sts = imm_extend_xtnt_map (xtntMap);
            if ( sts != EOK ) {
                RAISE_EXCEPTION (sts);
            }
        }
        prevSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
        subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

        sts = FTX_START_N ( FTA_BS_STG_ALLOC_MCELL_V1,
                            &ftxH,
                            parentFtx,
                            bfap->dmnP,
                            0);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Allocate an mcell early so that the bmt can expand if
         * necessary.  If we did this after allocating storage, we
         * could suck up all available free space and fail to allocate
         * a new mcell because there isn't space for bmt expansion.
         */
        sts = stg_alloc_new_mcell ( bfap,
                                    bfSetTag,
                                    bfap->tag,
                                    vdIndex,
                                    ftxH,
                                    &mcellId,
                                    TRUE);
        if ( sts != EOK ) {
            ftx_fail(ftxH);
            RAISE_EXCEPTION(sts);
        }

        /*
         * NOTE:  The first page of the new sub extent map must be the
         * next page after the last page mapped by the previous sub extent
         * map.
         */
        sts = imm_init_sub_xtnt_map ( subXtntMap,
                                     (prevSubXtntMap->pageOffset +
                                      prevSubXtntMap->pageCnt),
                                      0,  /* pageCnt */
                                      vdIndex,
                                      mcellId,
                                      BSR_XTRA_XTNTS,
                                      BMT_XTRA_XTNTS,
                                      0);  /* force default value for maxCnt */
        if ( sts != EOK ) {
            ftx_fail(ftxH);
            RAISE_EXCEPTION(sts);
        }
        subXtntMap->mcellState = NEW_MCELL;

        sts = xfer_stg_in_one_subxtnt  (subXtntMap,
                                        bfap->bfPageSz,
                                        bfPageOffset,
                                        bfPageCnt,
                                        startBlk );

        /* Since this is a new subextent we don't expect EXTND_FAILURE */
        if ( sts != EOK ) {
            /*
             * Fail the sub-transaction.  This frees the mcell that we
             * allocated above.  If we don't allocate the mcell within a
             * sub-transaction, we can't safely return the mcell because
             * this function's caller may either "done" or "fail" the parent
             * transaction.
             *
             * If we didn't allocate the mcell within a sub-transaction and
             * the caller failed the transaction, the mcell is freed as part
             * of the allocate mcell undo.  If the caller doesn't fail the
             * transaction, we lose the mcell.  We could start a sub-transaction
             * in this error path so that it can schedule a root done to free
             * the mcell.  This doesn't work because there is a limit on the
             * number of root dones a transaction tree can have and the number
             * of times thru this code is non-deterministic.
             */
            ftx_fail(ftxH);
            imm_unload_sub_xtnt_map(subXtntMap);
            RAISE_EXCEPTION(sts);
        }

        ftx_done_n(ftxH, FTA_BS_STG_ALLOC_MCELL_V1);

        xtntMap->cnt++;
    }

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return EOK;

HANDLE_EXCEPTION:
    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;
}


/* Add storage at the last descriptor, overwriting bsXA[subXtntMap->cnt - 1]. */
/* Set subXtntMap->updateStart, subXtntMap->updateEnd, subXtntMap->cnt */
/* Return EOK, ENO_MORE_MEMORY, EXTND_FAILURE */
static statusT
xfer_stg_in_one_subxtnt (
                         bsInMemSubXtntMapT *subXtntMap,   /* in this subxtnt */
                         int                bfPageSize,
                         ulong_t            bfPageOffset,  /* at this offset */
                         ulong_t            bfPageCnt,     /* this many pages */
                         uint32T            blkOff        /* from this place */
                        )
{
    uint32T i;
    uint32T nextBlkOff;
    uint32T pageCnt;
    statusT sts;
    bsXtntT termDesc;

    /*
     * Save the current termination descriptor and index in case we fail and
     * must restore the sub extent map's previous state.
     */
    i = subXtntMap->cnt - 1;
    subXtntMap->updateStart = i;
    termDesc = subXtntMap->bsXA[i];

    if (bfPageOffset == subXtntMap->bsXA[i].bsPage) {
        /*
         * Allocating storage that is virtually contiguous with the
         * bitfile's last page.  Determine if the new extent and the
         * previous extent are physically contiguous on disk.
         */
        if ( i > 0 ) {         /* there exists some pages already */
            if ( subXtntMap->bsXA[i - 1].vdBlk != XTNT_TERM &&
                 subXtntMap->bsXA[i - 1].vdBlk != PERM_HOLE_START )
            {
                /* prev page exists*/

                nextBlkOff = subXtntMap->bsXA[i - 1].vdBlk +
                  ((subXtntMap->bsXA[i].bsPage -
                    subXtntMap->bsXA[i - 1].bsPage) * bfPageSize);
                /*
                 * Determine if the new storage and the previous
                 * extent are physically contiguous on disk.
                 */
                if ( blkOff == nextBlkOff ) {
                    /*
                     * The extents are contiguous.  Increase the size of
                     * the previous extent.
                     */
                    /* E.g., add page 2 at block 64 to the following map, i = 2
                     * page   0   1   2   ->  0   1   3
                     * block  16  48  x   ->  16  48  -1
                     */
                    /* bsPage & vdBlk at bsXA[i] will be modified below. */
                } else {
                    /*
                     * The extents are not contigous.
                     */
                    /* E.g., add page 2 at block 80 to the following map, i = 2
                     * page   0   1   2   ->  0   1   2   3
                     * block  16  48  x   ->  16  48  80  -1
                     */
                    if ( i == (subXtntMap->maxCnt - 1) ) {
                        /* One entry left. */
                        sts = imm_extend_sub_xtnt_map(subXtntMap);
                        if ( sts != EOK ) {
                            RAISE_EXCEPTION(sts);
                        }
                    }
                    /* bsPage at bsXA[i] will be modified below. */
                    subXtntMap->bsXA[i].vdBlk = blkOff;
                    i++;
                }
            } else { /* the previous page doesn't exist */
                /* E.g., add page 2 at block 80 to the following map, i = 2
                 * page   0   1   2   ->  0   1   2   3
                 * block  16  -1  x   ->  16  -1  80  -1
                 */
                if ( i == (subXtntMap->maxCnt - 1) ) {
                    sts = imm_extend_sub_xtnt_map(subXtntMap);
                    if ( sts != EOK ) {
                        RAISE_EXCEPTION(sts);
                    }
                }
                /* bsPage at bsXA[i] will be modified below. */
                MS_SMP_ASSERT(!((subXtntMap->bsXA[i-1].vdBlk == PERM_HOLE_START) &&
                              (blkOff == PERM_HOLE_START)));
                subXtntMap->bsXA[i].vdBlk = blkOff;
                i++;
            }
        } else { /* no pages in this subextent */
            /* E.g., add page 2 at block 80 to the following map, i = 0
             * page   x   ->  2   3
             * block  -1  ->  80  -1
             */
            if ( i == (subXtntMap->maxCnt - 1) ) {
                sts = imm_extend_sub_xtnt_map(subXtntMap);
                /* curently ms_malloc waits, so this can't fail */
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
            }
            subXtntMap->bsXA[i].bsPage = bfPageOffset;
            subXtntMap->bsXA[i].vdBlk = blkOff;
            i++;
        }
    } else {
        /*
         * Allocating storage at least one page beyond the bitfile's last page.
         */
        /* E.g., add page 2 at block 80 to the following map, i = 1
         * page   0   1   ->  0   1   2   3
         * block  16  -1  ->  16  -1  80  -1
         */
        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the hole and the termination extent descriptors?
         * If not, extend the array.
         */
        if ( (int)i >= ((int)subXtntMap->maxCnt - 2) ) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map(subXtntMap);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            /* if we only got one more descriptor, fail */
            if ( i >= (subXtntMap->maxCnt - 2) ) {
                RAISE_EXCEPTION(EXTND_FAILURE);
            }
        }
        MS_SMP_ASSERT(subXtntMap->bsXA[i].vdBlk == XTNT_TERM);
        i++;
        subXtntMap->bsXA[i].bsPage = bfPageOffset;
        subXtntMap->bsXA[i].vdBlk = blkOff;
        i++;
    }

    subXtntMap->bsXA[i].bsPage = bfPageOffset + bfPageCnt;
    subXtntMap->bsXA[i].vdBlk = XTNT_TERM;

    subXtntMap->updateEnd = i;

    subXtntMap->cnt = i + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
              subXtntMap->bsXA[0].bsPage;
    if (pageCnt > subXtntMap->pageCnt) {
        subXtntMap->pageCnt = pageCnt;
    }

    return EOK;

HANDLE_EXCEPTION:
    /*
     * Restore the sub extent map to its previous state.
     */

    subXtntMap->bsXA[subXtntMap->updateStart] = termDesc;
    subXtntMap->cnt = subXtntMap->updateStart + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
    subXtntMap->updateStart = subXtntMap->cnt;

    return sts;
}


/*
 * stg_alloc_new_mcell
 *
 * This function allocates and initializes a new mcell.
 */

statusT
stg_alloc_new_mcell (
                     bfAccessT *bfap,      /* in */
                     bfTagT bfSetTag,      /* in */
                     bfTagT bfTag,         /* in */
                     vdIndexT newVdIndex,  /* in */
                     ftxHT parentFtx,      /* in */
                     bfMCIdT *newMcellId,  /* out */
                     int force_alloc       /* in - force allocate flag */
                     )
{
    bfMCIdT mcellId;
    mcellPoolT mcellType;
    statusT sts;

    /*
     * Determine if we are trying to add storage to a reserved
     * bitfile, currently (May 1991) only the bmt and tagdir are ever
     * extended after vd initialization.
     * Reserved bitfiles must stay on their original virtual
     * disk.  Revisit this assumption when they are shadowed, also if
     * we "virtually" restore a bmt to another vd during recovery -
     * would it ever be extended in that case?
     */

    if (BS_BFTAG_REG(bfTag)) {

        /*
         * This is not a reserved bitfile.
         * Allocate a new mcell from the virtual disk free mcell list.
         */

        mcellType = BMT_NORMAL_MCELL;

    } else {

        /*
         * We are expanding a reserved bitfile, which can only have
         * extents on page 0 of the bmt to avoid truly horrible
         * inconsistency issues when the reserved files are accessed
         * prior to running recovery, as they must be accessed in
         * order to run recovery at all!
         */

        mcellType = RBMT_MCELL;
    }

    sts = bmt_alloc_mcell( bfap,
                           newVdIndex,
                           mcellType,
                           bfSetTag,
                           bfTag,
                           0,  /* linkSeg */
                           parentFtx,
                           &mcellId,
                           force_alloc);
    *newMcellId = mcellId;

    return sts;

} /* end stg_alloc_new_mcell */


/*
 * This function inserts a permanent hole in a clone's extent map.
 * A permanent hole is an extent that starts with a -2 vdBlk.
 * It marks an extent that was a hole in the original when the clone
 * was made. When the hole in the original is filled in the clone
 * extent gets a permanent hole instead of a block of zeros.
 * The hole is the intersection of the extent in the original that
 * contains "page" and the extent in the clone that conatins "page".
 * If the hole page range is not mapped by the clone extent map (off the
 * end of the extent map), call append_perm_hole. If the hole page range
 * is mapped by the clone extent map (extents before and after the hole
 * range), call insert_perm_hole.
 *
 * Adapted from stg_add_stg_no_cow.
 */
static
statusT
make_perm_hole( bfAccessT *cloneap,
                uint32T page,
                ftxHT ftxH)
{
    bfAccessT *origap;
    bsInMemXtntT *origXtnts;
    bsInMemXtntT *cloneXtnts;
    bsInMemXtntMapT *origXtntMap;
    bsInMemXtntMapT *cloneXtntMap;
    bsInMemXtntDescIdT xtntDescId;
    bsXtntDescT xtntDesc;
    uint32T holeStart;
    uint32T holeEnd;
    statusT sts;
    uint32T stripeIndex;
    uint32T segCnt;
    uint32T segSize;
    uint32T bfPage;
    bsInMemSubXtntMapT *subXtntMap;
    int failFtxFlag = 0;
    addStgUndoRecT undoRec;
    int updateFlg;

    MS_SMP_ASSERT(cloneap->origAccp);
    MS_SMP_ASSERT(page < cloneap->maxClonePgs); 
    origap = cloneap->origAccp;
    MS_SMP_ASSERT(origap->nextCloneAccp == cloneap);
    origXtnts = &origap->xtnts;
    cloneXtnts = &cloneap->xtnts;
    MS_SMP_ASSERT(origXtnts->validFlag != 0);
    MS_SMP_ASSERT(cloneXtnts->validFlag != 0);

    /* Find the start and end of the hole in the original file. */
    if ( origXtnts->type == BSXMT_APPEND ) {
        MS_SMP_ASSERT(cloneXtnts->type == BSXMT_APPEND);

        origXtntMap = origXtnts->xtntMap;
        cloneXtntMap = cloneXtnts->xtntMap;

        imm_get_xtnt_desc(page, origXtntMap, FALSE, &xtntDescId, &xtntDesc);

        if ( xtntDesc.volIndex ) {
            /* If the page is mapped in the orig, it must be a hole. */
            MS_SMP_ASSERT(xtntDesc.pageCnt != 0);
            MS_SMP_ASSERT(xtntDesc.blkOffset == XTNT_TERM);
            holeStart = xtntDesc.pageOffset;
            holeEnd = holeStart + xtntDesc.pageCnt;
        } else {
            holeStart = page;
            /* The page is beyond the end of the orig file, set holeEnd to */
            /* the max. However, the page is within maxClonePgs to get here. */
            holeEnd = cloneap->maxClonePgs;
        }

        if ( cloneXtntMap->updateStart < cloneXtntMap->cnt ) {
            updateFlg = TRUE;
        } else {
            updateFlg = FALSE;
        }
    } else {  /* striped file */
        MS_SMP_ASSERT(origXtnts->type == BSXMT_STRIPE);
        segCnt = origXtnts->stripeXtntMap->cnt;
        segSize = origXtnts->stripeXtntMap->segmentSize;

        MS_SMP_ASSERT(cloneap->xtnts.type == BSXMT_STRIPE);
        MS_SMP_ASSERT(cloneXtnts->stripeXtntMap->cnt == segCnt);
        MS_SMP_ASSERT(cloneXtnts->stripeXtntMap->segmentSize == segSize);

        stripeIndex = BFPAGE_TO_MAP( page, segCnt, segSize);
        bfPage = page;
        page = BFPAGE_TO_XMPAGE( page, segCnt, segSize);
        origXtntMap = origXtnts->stripeXtntMap->xtntMap[stripeIndex];
        cloneXtntMap = cloneXtnts->stripeXtntMap->xtntMap[stripeIndex];

        imm_get_xtnt_desc(page, origXtntMap, FALSE, &xtntDescId, &xtntDesc);

        /* The page is mapped, but it can be off the end of a stripe extent. */

        if ( xtntDesc.volIndex == bsNilVdIndex ) {
            /* There'll be a subextent for this stripe even if it has no stg. */
            MS_SMP_ASSERT(origXtntMap->validCnt);
            MS_SMP_ASSERT(origXtntMap->cnt);

            /* The hole starts after the last block in this stripe. */
            subXtntMap = &origXtntMap->subXtntMap[origXtntMap->cnt - 1];
            holeStart = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage;
            /* Since this page is off the end of the extent map for this
            /* stripe, the hole ends at the end of the stripe. */
            holeEnd = page + (segSize - page % segSize);
        } else {
            MS_SMP_ASSERT(xtntDesc.pageCnt != 0);
            MS_SMP_ASSERT(xtntDesc.blkOffset == XTNT_TERM);
            holeStart = xtntDesc.pageOffset;
            holeEnd = holeStart + xtntDesc.pageCnt;
        }
        cloneXtntMap->allocDeallocPageCnt += holeEnd - holeStart;
        if ( cloneXtntMap->updateStart < cloneXtntMap->cnt ) {
            updateFlg = TRUE;
        } else {
            updateFlg = FALSE;
        }
    }

    holeEnd = MIN(holeEnd, cloneap->maxClonePgs);
    MS_SMP_ASSERT(holeStart < holeEnd);

    imm_get_xtnt_desc( holeStart,
                       cloneXtntMap,
                       updateFlg,
                       &xtntDescId,
                       &xtntDesc);
    if ( xtntDesc.volIndex == bsNilVdIndex ) {
        /* Page is not mapped. It is beyond the last extent. */

        /* Add a permanent hole descriptor after the last descriptor. */
        sts = append_perm_hole( cloneap,
                                cloneXtntMap,
                                holeStart,
                                holeEnd,
                                updateFlg,
                                ftxH);
    } else {
        /* There are descriptors before and after the hole. */
        MS_SMP_ASSERT(xtntDesc.pageCnt != 0);
        if ( xtntDesc.blkOffset == PERM_HOLE_START ) {
              /* The clone already has a perm hole here. Nothing to do. */
              RAISE_EXCEPTION(EOK);
        }

        if ( xtntDesc.blkOffset != XTNT_TERM ) {
              /* The clone already has extents here. Nothing to do. */
              RAISE_EXCEPTION(EOK);
        }

        /* Find the intersection of the hole in the original and */
        /* the hole in the clone. */
        if ( holeStart < xtntDesc.pageOffset ) {
            holeStart = xtntDesc.pageOffset;
        }
        if ( holeEnd > xtntDesc.pageOffset + xtntDesc.pageCnt ) {
            holeEnd = xtntDesc.pageOffset + xtntDesc.pageCnt;
        }
        sts = insert_perm_hole( cloneap,
                                cloneXtntMap,
                                xtntDescId,
                                holeStart,
                                holeEnd,
                                updateFlg,
                                ftxH);
    }
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    if ( origXtnts->type == BSXMT_STRIPE ) {
        holeEnd = XMPAGE_TO_BFPAGE(stripeIndex, holeEnd, segCnt, segSize);
    }
    if ( holeEnd > cloneap->nextPage ) {
        cloneap->nextPage = holeEnd;
    }

    return sts;

HANDLE_EXCEPTION:
    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }

    return sts;
}


/*
 * Add a permanent hole (starts with a -2 vdBlk) to the clone extent map
 * after the last extent.
 * Sets xtntMap-> origStart, origEnd, updateStart, updateEnd.
 *
 * Adapted from alloc_append_stg
 */

static
statusT
append_perm_hole( bfAccessT *bfap,
                  bsInMemXtntMapT *xtntMap,
                  uint32T holeStart,
                  uint32T holeEnd,
                  int updateFlg,
                  ftxHT ftxH)
{
    bfSetT *bfSetp;
    uint32T lSubI;          /* last subextent index */
    uint32T uSubI;          /* update subextent index */
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;

    MS_SMP_ASSERT(bfap->origAccp);   /* this file is a clone */

    bfSetp = bfap->bfSetp;
    if ( updateFlg ) {
        /* We already have an update subextent from add_stg */
        MS_SMP_ASSERT(xtntMap->cnt > xtntMap->validCnt);
        MS_SMP_ASSERT(xtntMap->updateStart >= xtntMap->validCnt);
        MS_SMP_ASSERT(xtntMap->updateStart < xtntMap->cnt);
        /* Just checking. */
        MS_SMP_ASSERT(xtntMap->updateEnd >= xtntMap->updateStart);
        MS_SMP_ASSERT(xtntMap->origStart <= xtntMap->origEnd);
        MS_SMP_ASSERT(xtntMap->origEnd < xtntMap->validCnt);

        uSubI = xtntMap->updateEnd;
        subXtntMap = &xtntMap->subXtntMap[uSubI];
        MS_SMP_ASSERT(holeStart = subXtntMap->bsXA[subXtntMap->cnt -1].bsPage);
    } else {
        /* No stg was added. We have to setup update subextents here. */
        /* Just checking. */
        /* This should be true except when actively changing the xtntMap. */
        MS_SMP_ASSERT(xtntMap->cnt == xtntMap->validCnt);

        lSubI = xtntMap->validCnt - 1;
        /* holeStart is after this subextent. */
        MS_SMP_ASSERT(holeStart >= xtntMap->subXtntMap[lSubI].pageOffset +
                               xtntMap->subXtntMap[lSubI].pageCnt);

        xtntMap->origStart = lSubI;
        xtntMap->origEnd = lSubI;

        uSubI = xtntMap->cnt;
        xtntMap->updateStart = uSubI;
        if ( uSubI >= xtntMap->maxCnt ) {
            /* One more decriptor needed to hold the permanent hole start. */
            sts = imm_extend_xtnt_map(xtntMap);
            if ( sts != EOK ) {
                return sts;
            }
        }

        sts = imm_copy_sub_xtnt_map( bfap,
                                     xtntMap,
                                     &xtntMap->subXtntMap[lSubI],
                                     &xtntMap->subXtntMap[uSubI]);
        if ( sts != EOK ) {
            return sts;
        }
        xtntMap->cnt++;

        subXtntMap = &xtntMap->subXtntMap[uSubI];

        if ( (subXtntMap->cnt == 1) && (uSubI == 1) ) {
            MS_SMP_ASSERT(subXtntMap->bsXA[0].vdBlk == XTNT_TERM);
            /*
             * This file has one subextent with one descriptor.
             * It is a normal bitfile and it does not have any storage
             * allocated to it.  If needed, allocate an mcell for the sub
             * extent map before allocating storage.
             *
             * If the bitfile has never had any storage allocated to it,
             * the sub extent map's mcell id is nil.
             */
            if ( (subXtntMap->mcellId.page == bsNilMCId.page) &&
                 (subXtntMap->mcellId.cell == bsNilMCId.cell) )
            {
                if ( bfap->xtnts.type == BSXMT_STRIPE ) {
                    /* Get a new mcell on the correct volume for this stripe. */
                    MS_SMP_ASSERT(xtntMap->allocVdIndex != -1);
                    sts = stg_alloc_new_mcell( bfap,
                                               bfSetp->dirTag,
                                               bfap->tag,
                                               xtntMap->allocVdIndex,
                                               ftxH,
                                               &subXtntMap->mcellId,
                                               FALSE);
                } else {
                    /* Try to get an mcell on the volume of the prim mcell. */
                    /* Accept an mcell on any volume. */
                    sts = get_first_mcell( bfSetp->dirTag,
                                           bfap,
                                           0,               /* 0 bytes of stg */
                                           ftxH,
                                           &subXtntMap->vdIndex,
                                           &subXtntMap->mcellId);
                }
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }

                subXtntMap->mcellState = NEW_MCELL;
                xtntMap->hdrVdIndex = subXtntMap->vdIndex;
                xtntMap->hdrMcellId = subXtntMap->mcellId;
            }
        }
    }

    sts = add_hole_extent(bfap, xtntMap, holeStart, holeEnd, updateFlg, ftxH);

    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    xtntMap->updateEnd = xtntMap->cnt - 1;

    return EOK;

HANDLE_EXCEPTION:
    xtntMap->updateEnd = xtntMap->cnt - 1;
    unload_sub_xtnt_maps(xtntMap);

    return sts;
}


/*
 * This function inserts a permanent hole in the clone file's extent map.
 * The hole is added between existing descriptors or within an existing
 * descriptor.
 * This function starts a new update subextent. It may be updating an
 * existing update subextent or it may update the origStart, origEnd subs.
 * Sets xtntMap-> origStart, origEnd, updateStart, updateEnd.
 *
 * Adapted from alloc_hole_stg.
 */

static
statusT
insert_perm_hole( bfAccessT *bfap,
                  bsInMemXtntMapT *xtntMap,
                  bsInMemXtntDescIdT xtntDescId,
                  uint32T holeStart,
                  uint32T holeEnd,
                  int updateFlg,
                  ftxHT ftxH)
{
    bfSetT *bfSetp;
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32T i;
    bsInMemSubXtntMapT *newSubXtntMap;
    bsInMemSubXtntMapT *oldSubXtntMap;
    uint32T pageCnt;
    uint32T updateStart;
    statusT sts;
    bsXtntT *oldbsXA;

    MS_SMP_ASSERT(bfap->origAccp);   /* This is a clone file. */
    bfSetp = bfap->bfSetp;

    MS_SMP_ASSERT(xtntDescId.subXtntMapIndex < xtntMap->cnt);
    if ( !updateFlg ) {
        xtntMap->updateStart = xtntMap->cnt;
        xtntMap->origStart = xtntDescId.subXtntMapIndex;
        xtntMap->origEnd = xtntDescId.subXtntMapIndex;
    } else {
        MS_SMP_ASSERT(xtntMap->updateStart <= xtntDescId.subXtntMapIndex);
        MS_SMP_ASSERT(xtntDescId.subXtntMapIndex <= xtntMap->updateEnd);
        MS_SMP_ASSERT(xtntMap->origEnd < xtntMap->cnt);
        MS_SMP_ASSERT(xtntMap->cnt <= xtntMap->maxCnt);
        updateStart = xtntMap->updateStart;
        xtntMap->updateStart = xtntMap->cnt;
        for ( i = updateStart; i < xtntDescId.subXtntMapIndex; i++ ) {
            if ( xtntMap->cnt == xtntMap->maxCnt ) {
                sts = imm_extend_xtnt_map(xtntMap);
                if ( sts != EOK ) {
                    return sts;
                }
            }
            oldSubXtntMap = &xtntMap->subXtntMap[i];
            oldbsXA = oldSubXtntMap->bsXA;
            newSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

            sts = imm_copy_sub_xtnt_map( bfap,
                                         xtntMap,
                                         oldSubXtntMap,
                                         newSubXtntMap);
            if ( sts != EOK ) {
                return sts;
            }
            ms_free(oldbsXA);

            newSubXtntMap->mcellState = oldSubXtntMap->mcellState;
            newSubXtntMap->updateStart = oldSubXtntMap->updateStart;
            newSubXtntMap->updateEnd = oldSubXtntMap->updateEnd;

            xtntMap->cnt++;
        }
    }
    /*
     * We use the free sub extent maps after the last valid sub extent map
     * to save the storage allocation information.  If all goes well, the
     * maps are later merged with the valid maps.
     */
    MS_SMP_ASSERT(xtntMap->cnt <= xtntMap->maxCnt);
    if ( xtntMap->cnt == xtntMap->maxCnt ) {
        sts = imm_extend_xtnt_map(xtntMap);
        if ( sts != EOK ) {
            return sts;
        }
    }

    /* This may be an update sub or an orig sub */
    oldSubXtntMap = &xtntMap->subXtntMap[xtntDescId.subXtntMapIndex];
    oldbsXA = oldSubXtntMap->bsXA;
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

    sts = imm_copy_sub_xtnt_map( bfap,
                                 xtntMap,
                                 oldSubXtntMap,
                                 newSubXtntMap);
    if ( sts != EOK ) {
        return sts;
    }

    xtntMap->cnt++;

    MS_SMP_ASSERT(xtntDescId.xtntDescIndex + 1 < newSubXtntMap->cnt);
    newSubXtntMap->cnt = xtntDescId.xtntDescIndex + 1;
    MS_SMP_ASSERT(newSubXtntMap->cnt <= newSubXtntMap->maxCnt);
    if ( updateFlg ) {
        newSubXtntMap->updateStart = oldSubXtntMap->updateStart;
        newSubXtntMap->mcellState = oldSubXtntMap->mcellState;
    }

    /* The hole to be inserted overlaps a hole extent. */
    MS_SMP_ASSERT(holeStart >=
                  newSubXtntMap->bsXA[xtntDescId.xtntDescIndex].bsPage);
    MS_SMP_ASSERT(newSubXtntMap->bsXA[xtntDescId.xtntDescIndex].vdBlk ==
                  XTNT_TERM);
    MS_SMP_ASSERT(holeEnd <=
                  newSubXtntMap->bsXA[xtntDescId.xtntDescIndex + 1].bsPage);

    sts = add_hole_extent(bfap, xtntMap, holeStart, holeEnd, updateFlg, ftxH);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    xtntMap->updateEnd = xtntMap->cnt - 1;
    /* subXtntMap array may have changed due to adding a new subextent. */
    /* Recompute oldSubXtntMap & newSubXtntMap. */
    oldSubXtntMap = &xtntMap->subXtntMap[xtntDescId.subXtntMapIndex];
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->updateEnd];
    /*
     * We added the permanent hole in a hole mapped by the extent array.
     * Now, copy the extent descriptors positioned after the hole descriptor.
     */
    if (newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage <
        oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex + 1].bsPage) {
        /*
         * The last new page allocated and the next page after the hole are
         * not contiguous, we need to create a (non permanent) hole descriptor
         * and copy it to the end of the extent map.
         */
        imm_split_desc( newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage,
                        xtntMap->blksPerPage,
                        &oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex],/* orig*/
                        &desc,                            /* first part */
                        &bsXA[0]);                        /* last part */

        bsXA[1].bsPage = oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex+1].bsPage;
        bsXA[1].vdBlk = XTNT_TERM;

        /* copy bsXA to xtntMap[cnt-1].bsXA[cnt-1] */
        sts = imm_copy_xtnt_descs( newSubXtntMap->vdIndex,
                                   &bsXA[0],
                                   1,
                                   TRUE,
                                   xtntMap,
                                   bfap->xtnts.type);
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }
        oldSubXtntMap = &xtntMap->subXtntMap[xtntDescId.subXtntMapIndex];

    } else if (oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex+1].vdBlk ==
              PERM_HOLE_START) {

        /*
         * We just added a permanent hole (i.e., a PERM_HOLE_START entry
         * followed by an XTNT_TERM entry), and the extent following it
         * is a permanent hole.  Do not copy the second permanent hole
         * entry.
         *
         * Instead, advance the xtntDescIndex pointer so that it points
         * to the entry following the second permanent hole.  Update
         * the bsPage field of the XTNT_TERM entry following the new
         * permanent hole with the bsPage value of the entry following
         * the second permanent hole.
         *
         * imm_copy_xtnt_descs() will make sure the XTNT_TERM entry
         * following the new permanent hole will contain the vdBlock
         * value of the entry following the second permanent hole,
         * if this latter value is not XTNT_TERM.
         */

        MS_SMP_ASSERT
            (newSubXtntMap->bsXA[newSubXtntMap->cnt-1].vdBlk == XTNT_TERM);

        xtntDescId.xtntDescIndex++;

        newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsPage =
            oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex+1].bsPage;
    }
    xtntDescId.xtntDescIndex++;

    /*
     * Copy the remaining entries from the split sub extent map.
     */
    sts = imm_copy_xtnt_descs( oldSubXtntMap->vdIndex,
                               &oldSubXtntMap->bsXA[xtntDescId.xtntDescIndex],
                               oldSubXtntMap->cnt-1 - xtntDescId.xtntDescIndex,
                               TRUE,
                               xtntMap,
                               bfap->xtnts.type);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    /* If the copy above was made from an update sub, and since we
     * have now duplicated that subMap, cleanup the stg map from add_stg.
     * Otherwise the copy was made from an orig sub, and we don't want
     * to delete the orig here.
     */
    if ( updateFlg )
        ms_free(oldbsXA);

    /*
     * Set the last update sub extent map's "updateEnd" field as
     * imm_copy_xtnt_descs() may have modified the map.
     */
    newSubXtntMap = &xtntMap->subXtntMap[xtntMap->updateEnd];
    newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
    newSubXtntMap->pageCnt =
        newSubXtntMap->bsXA[newSubXtntMap->updateEnd].bsPage -
        newSubXtntMap->pageOffset;

    /*
     * Allocate new mcells for any new sub extent maps that were added
     * by imm_copy_xtnt_descs().
     */

    for ( i = xtntMap->updateEnd + 1; i < xtntMap->cnt; i++ ) {
        newSubXtntMap = &xtntMap->subXtntMap[i];
        sts = stg_alloc_new_mcell( bfap,
                                   bfSetp->dirTag,
                                   bfap->tag,
                                   newSubXtntMap->vdIndex,
                                   ftxH,
                                   &newSubXtntMap->mcellId,
                                   TRUE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }

        newSubXtntMap->updateStart = 0;
        newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
        newSubXtntMap->mcellState = NEW_MCELL;
    }

    if ( updateFlg ) {
        for ( i = xtntDescId.subXtntMapIndex + 1;
              i < xtntMap->updateStart; i++ )
        {
            if ( xtntMap->cnt == xtntMap->maxCnt ) {
                sts = imm_extend_xtnt_map(xtntMap);
                if ( sts != EOK ) {
                    return sts;
                }
            }
            oldSubXtntMap = &xtntMap->subXtntMap[i];
            oldbsXA = oldSubXtntMap->bsXA;
            newSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

            sts = imm_copy_sub_xtnt_map( bfap,
                                         xtntMap,
                                         oldSubXtntMap,
                                         newSubXtntMap);
            if ( sts != EOK ) {
                return sts;
            }
            ms_free(oldbsXA);
            newSubXtntMap->mcellState = oldSubXtntMap->mcellState;
            newSubXtntMap->updateStart = oldSubXtntMap->updateStart;
            newSubXtntMap->updateEnd = oldSubXtntMap->updateEnd;

            xtntMap->cnt++;
        }
    }

    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;

HANDLE_EXCEPTION:
    xtntMap->updateEnd = xtntMap->cnt - 1;
    unload_sub_xtnt_maps (xtntMap);

    return sts;
}


/*
 * Add a permanent hole with a starting extent of -2 to the extent map.
 * The hole will be added after the last page in the last subextent map.
 * updateStart, updateEnd & cnt in the subextent map are set.
 * If there is not enough descriptors in the last subextent map (mcell)
 * then get a new subextent map (mcell).
 * The only failure possible is the failure to get a new mcell
 * or to initialize the new mcell. If this happens, the sub extent is
 * not modified.
 * Sets subXtntMap-> updateStart, updateEnd & pageCnt.
 *
 * adapted from alloc_from_one_disk
 */

static
statusT
add_hole_extent( bfAccessT *bfap,
                 bsInMemXtntMapT *xtntMap,
                 uint32T holeStart,
                 uint32T holeEnd,
                 int updateFlg,
                 ftxHT ftxH)
{
    bsInMemSubXtntMapT *subXtntMap;
    uint32T bfPageOffset;
    uint32T xI;           /* extent index of last descriptor */
    statusT sts = EOK;
    uint32T lastPage;

    subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
    MS_SMP_ASSERT(subXtntMap->cnt > 0);
    xI = subXtntMap->cnt - 1;
    if ( !updateFlg )
        subXtntMap->updateStart = xI;
    if ( holeStart == subXtntMap->bsXA[xI].bsPage ) {
        /* Add hole adjacent to the last extent in this subextent. */
        /* The start of the hole will replace the original extent terminator. */
        /* We only need one more descriptor to terminate the hole. */
        if ( xI == subXtntMap->maxCnt - 1 ) {
            /* We need one more to terminate the permanent hole. */
            sts = imm_extend_sub_xtnt_map(subXtntMap);
            if ( sts == EXTND_FAILURE ) {
                /* subXtntMap is full. Get a new one. */
                MS_SMP_ASSERT(subXtntMap->bsXA[subXtntMap->cnt - 1].vdBlk ==
                              XTNT_TERM);

                if ( !updateFlg ) {
                    subXtntMap->updateEnd = subXtntMap->updateStart;
                }

                /* The PERMHOLE we are about to create should not be
                 * filling a hole that lives at the end of an extent
                 * map (this is the RULE THAT HOLES MUST STICK TO THE
                 * STORAGE FOLLOWING IT). We could leave the extent map
                 * that is full alone and use a new one without
                 * initializing pageCnt.  
                 *
                 * However since this on-disk anomaly can exist in
                 * older domains we must deal with it here. Set the
                 * pageCnt in the passed in subxtntmap to reflect that
                 * we may be moving the hole and therefor this
                 * descriptor's terminator onto the new subxtnt map we
                 * are about to create.  This will have the effect of
                 * cleaning up this persistent on-disk bug.  
                 */

                subXtntMap->pageCnt = subXtntMap->bsXA[xI].bsPage -
                                      subXtntMap->bsXA[0].bsPage;

                sts = new_sub(bfap, xtntMap, ftxH);

                /* The subXtntMap array has changed. Recalculate subXtntMap. */
                subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
                xI = 0;
            }
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }
    } else {
        MS_SMP_ASSERT(holeStart > subXtntMap->bsXA[xI].bsPage);
        /* The hole starts beyond the last extent. We need 2 more descriptors */
        /* to describe the hole, a hole start and a terminator. */
        xI++;
        if ( subXtntMap->maxCnt < 2 || xI > subXtntMap->maxCnt - 2 ) {
            /* There are not enough free descriptors. We must get more. */
            if ( xI + 1 < subXtntMap->onDiskMaxCnt ) {
                /* There are at least 2 descs left in this subextent (mcell). */
                /* Extend it knowing we will get at least 2 more. */
                sts = imm_extend_sub_xtnt_map(subXtntMap);
                MS_SMP_ASSERT(xI + 1 < subXtntMap->maxCnt);
            } else {
                /* There is not enough room in the subextent (mcell), get */
                /* a new one. */
                MS_SMP_ASSERT(subXtntMap->bsXA[subXtntMap->cnt - 1].vdBlk ==
                              XTNT_TERM);
                lastPage = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage;

                if ( !updateFlg ) {
                    subXtntMap->updateEnd = subXtntMap->updateStart;
                }

                /* See on-disk anomaly comment above */

                subXtntMap->pageCnt = lastPage -
                                      subXtntMap->bsXA[0].bsPage;


                sts = new_sub(bfap, xtntMap, ftxH);

                subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];

                /* Start the new extent where the last subextent left off. */
                subXtntMap->bsXA[0].bsPage = lastPage;
                subXtntMap->bsXA[0].vdBlk = XTNT_TERM;
                /* Since the permanent hole is not adjacent to the last */
                /* extent, this subextent starts with a normal hole. */
                xI = 1;
            }
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }
    }

    /* Start the hole. */
    subXtntMap->bsXA[xI].bsPage = holeStart;
    subXtntMap->bsXA[xI].vdBlk = PERM_HOLE_START;
    xI++;

    /* Terminate the permanent hole. */
    subXtntMap->bsXA[xI].bsPage = holeEnd;
    subXtntMap->bsXA[xI].vdBlk = XTNT_TERM;
    subXtntMap->updateEnd = xI;
    subXtntMap->cnt = xI + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
    subXtntMap->pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
                          subXtntMap->bsXA[0].bsPage;

HANDLE_EXCEPTION:
    return sts;
}


/*
 * The caller ran out of descriptors in a subextent
 * (imm_extent_sub_xtnt_map failed). Get a new subextent and mcell.
 * Sets mcellState in new subXtntMap. Increments count of subextents.
 */
static
statusT
new_sub(bfAccessT *bfap, bsInMemXtntMapT *xtntMap, ftxHT ftxH)
{
    bfSetT *bfSetp;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;

    bfSetp = bfap->bfSetp;

    if ( xtntMap->cnt >= xtntMap->maxCnt ) {
        /* Get a new subXtntMap */
        sts = imm_extend_xtnt_map(xtntMap);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    prevSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
    subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];
    if ( bfap->xtnts.type == BSXMT_STRIPE ) {
        MS_SMP_ASSERT(xtntMap->allocVdIndex != -1);
        /* get the mcell on the specified volume */
        sts = stg_alloc_new_mcell( bfap,
                                   bfSetp->dirTag,
                                   bfap->tag,
                                   xtntMap->allocVdIndex,
                                   ftxH,
                                   &subXtntMap->mcellId,
                                   FALSE);
        subXtntMap->vdIndex = xtntMap->allocVdIndex;
    } else {
        /* Try to get an mcell on the volume of the prim mcell, */
        /* but accept any mcell. */
        sts = get_first_mcell( bfSetp->dirTag,
                               bfap,
                               0,                   /* 0 bytes of stg */
                               ftxH,
                               &subXtntMap->vdIndex,
                               &subXtntMap->mcellId);
    }
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    sts = imm_init_sub_xtnt_map( subXtntMap,
                                 (prevSubXtntMap->pageOffset +
                                  prevSubXtntMap->pageCnt),
                                 0,  /* pageCnt */
                                 subXtntMap->vdIndex,
                                 subXtntMap->mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 0);  /* force default value for maxCnt */
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    subXtntMap->mcellState = NEW_MCELL;
    xtntMap->cnt++;

HANDLE_EXCEPTION:
    return sts;
}


/*
 * x_page_to_iolist
 *
 * This function searches a bitfile's in-memory extent maps for entries
 * that match the specified page.
 *
 * This function is the ioList parameter interface.
 *
 * If mapped, returns W_NOT_WRIT if never written, else EOK.
 *
 * FIX - Is there a better way of doing this??
 */

#define BLKDESCMAX 10

statusT
x_page_to_iolist (
                  bfAccessT *bfap,     /* in */
                  uint32T pageOffset,  /* in */
                  ioListT *ioList      /* in */
                  )
{
    blkDescT blkDesc[BLKDESCMAX];
    blkMapT blkMap;
    int deallocFlag;
    uint32T i;
    statusT sts;

    blkMap.maxCnt = ioList->maxCnt;
    if (BLKDESCMAX >= ioList->maxCnt) {
        blkMap.blkDesc = &(blkDesc[0]);
        deallocFlag = 0;
    } else {
        blkMap.blkDesc =
                       (blkDescT *)ms_malloc(ioList->maxCnt * sizeof(blkDescT));
        if (blkMap.blkDesc == NULL) {
            return ENO_MORE_MEMORY;
        }
        deallocFlag = 1;
    }

    sts = x_page_to_blkmap (bfap, pageOffset, &blkMap);
    if ( sts == EOK || sts == W_NOT_WRIT ) {
        ioList->read = 0;
        ioList->readCnt = blkMap.readCnt;
        ioList->write = 0;
        ioList->writeCnt = blkMap.writeCnt;
        for ( i = 0; i < blkMap.writeCnt; i++ ) {
            ioList->ioDesc[i].blkDesc = blkMap.blkDesc[i];
        }
    }

    if ( deallocFlag != 0 ) {
        ms_free(blkMap.blkDesc);
    }

    return sts;
}  /* end x_page_to_iolist */


/*
 * x_page_to_blkmap
 *
 * This function searches a bitfile's in-memory extent maps for entries
 * that match the specified page.
 *
 * If mapped, returns W_NOT_WRIT if never written, else EOK.
 */

statusT
x_page_to_blkmap (
                  bfAccessT *bfap,     /* in */
                  uint32T pageOffset,  /* in */
                  blkMapT *blkMap      /* in */
                  )
{
    uint32T cnt = 0;
    int mapIndex;
    uint32T rdWrtCnt;
    bsStripeHdrT *stripeHdr;
    statusT sts;
    bsInMemXtntT *xtnts;
    uint32T stripePg;
    int this_is_log_file;
    int this_is_sbm_file;
    int this_is_tagdir;
    int this_is_rbmt;
    int this_is_v3_bmt;
    int unlock_xtnt_map = 0;

    if (blkMap->maxCnt < 1) {
        return E_BLKDESC_ARRAY_TOO_SMALL;
    }

    xtnts = &(bfap->xtnts);
    this_is_log_file = BS_IS_TAG_OF_TYPE( bfap->tag, BFM_FTXLOG);
    this_is_sbm_file = BS_IS_TAG_OF_TYPE( bfap->tag, BFM_SBM);
    this_is_tagdir  =  BS_BFTAG_REG(bfap->tag) &&
                       ((((bfSetT *)bfap->bfSetp)->dirTag.num) == -2 );
    this_is_rbmt     = RBMT_THERE(bfap->dmnP) &&
                       BS_BFTAG_RSVD(bfap->tag) &&
                       BS_BFTAG_RBMT(bfap->tag);

    /* Avoid needless locking and checking of in-memory extent maps for the
     * following cases:
     *  a. this is the log metadata file; its extents are only modified when 
     *     a single thread has access to it.  
     *  b. this is the SBM; the vdT.stgMap_lk should synchronize access to it.  
     *  c. this is a bitfile set tag directory. The bfSetp->dirLock will
     *     synchronize access to it during file creation/deletion, and it is
     *     migrated under an exclusive transaction.
     *  d. This is the RBMT; the RBMT extents are only modified under an
     *     exclusive transaction. (Ver. 4 domains only)
     *  e. This is the BMT on a Ver. 3 or earlier domain and this thread
     *     already has the BMT's xtntMap lock write-locked.
     *  f. This is not the RBMT, BMT, SBM, LOG, or TAG, but its xtntMap_lk is
     *     already write-locked by this thread and its xtnt.validFlag is true.
     *     (If write-locked and its xtnt.validFlag is false, return an error).
     *  These optimizations also avoid lock hierarchy violations.
     */
    if ( !(this_is_log_file || 
           this_is_sbm_file || 
           this_is_tagdir   || 
           this_is_rbmt )) {

        if ( lock_holder(&bfap->xtntMap_lk.lock) ) {

            this_is_v3_bmt = !RBMT_THERE(bfap->dmnP) &&
                             BS_IS_TAG_OF_TYPE(bfap->tag, BFM_BMT_V3);
            if ( this_is_v3_bmt ) {
                    MS_SMP_ASSERT( bfap->xtnts.validFlag );
            } else {
                if ( !( bfap->xtnts.validFlag )) {
                    return (E_XTNT_MAP_NOT_FOUND);
                }
            }
        } else {

            /* Be sure in-memory maps are valid; on successful return, the
             * xtntMap_lk is READ locked.
             */
            sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
            if (sts != EOK) {
                return sts;
            }

            unlock_xtnt_map = 1;
        }
    }

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        sts = x_page_to_blk( bfap,
                             pageOffset,
                             xtnts->xtntMap,
                             &(blkMap->blkDesc[0]));
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        rdWrtCnt = 1;
        break;

      case BSXMT_STRIPE:

        stripeHdr = xtnts->stripeXtntMap;
        mapIndex = BFPAGE_TO_MAP( pageOffset,
                                  stripeHdr->cnt,
                                  stripeHdr->segmentSize);
        stripePg = BFPAGE_TO_XMPAGE( pageOffset,
                                     stripeHdr->cnt,
                                     stripeHdr->segmentSize);
        sts = x_page_to_blk( bfap,
                             stripePg,
                             stripeHdr->xtntMap[mapIndex],
                             &blkMap->blkDesc[0]);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        rdWrtCnt = 1;
        break;

      default:
        ADVFS_SAD1 ("x_page_to_blkmap: bad extent map type", xtnts->type);

    }  /* end switch */

    blkMap->read = 0;
    blkMap->readCnt = rdWrtCnt;
    blkMap->write = 0;
    if (xtnts->copyXtntMap == NULL) {
        blkMap->writeCnt = rdWrtCnt;
    } else {
        /*
         * A page mapped by a copy extent map is write only.
         */
        sts = scan_xtnt_map_list( bfap,
                                  pageOffset,
                                  xtnts->copyXtntMap,
                                  blkMap->maxCnt - rdWrtCnt,
                                  &blkMap->blkDesc[rdWrtCnt],
                                  &cnt);
        if (sts == EOK) {
            blkMap->writeCnt = rdWrtCnt + cnt;
        } else {
            if (sts == E_PAGE_NOT_MAPPED) {
                sts = EOK;
                blkMap->writeCnt = rdWrtCnt;
            } else {
                RAISE_EXCEPTION (sts);
            }
        }
    }

    /* Fall thru to exception handler. */

HANDLE_EXCEPTION:

    if ( unlock_xtnt_map ) {
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    }
    return sts;

}  /* end x_page_to_blkmap */


/*
 * x_copypage_to_blkmap
 *
 * This function searches the bitfile's copy in-memory extent map list
 * for entries that match the specified page.  The caller must share 
 * the in-memory extent map lock.
 */

statusT
x_copypage_to_blkmap (
                      bfAccessT *bfap,      /* in */
                      bsInMemXtntT *xtnts,  /* in */
                      uint32T pageOffset,   /* in */
                      blkMapT *blkMap       /* in */
                      )
{
    statusT sts;
    uint32T writeCnt;

    if ((xtnts->type != BSXMT_APPEND) && (xtnts->type != BSXMT_STRIPE)) {
        ADVFS_SAD0("x_copypage_to_blkmap: bad xtnt type");
    }

    sts = scan_xtnt_map_list( bfap,
                              pageOffset,
                              xtnts->copyXtntMap,
                              blkMap->maxCnt,
                              &blkMap->blkDesc[0],
                              &writeCnt);
    if (sts == EOK) {
        blkMap->read = 0;
        blkMap->readCnt = 0;
        blkMap->write = 0;
        blkMap->writeCnt = writeCnt;
    }

    return sts;

}  /* end x_copypage_to_blkmap */


/*
 * scan_xtnt_map_list
 *
 * This function scans each extent map.  It returns
 * EOK if the page is mapped by at least one extent map.
 * If the page is not mapped, it returns E_PAGE_NOT_MAPPED.
 * If the block descriptor array is too small, it returns
 * E_BLKDESC_ARRAY_TOO_SMALL.
 *
 * This function sets "mappedCnt" when it returns a status
 * of EOK.
 */

static
statusT
scan_xtnt_map_list (
                    bfAccessT *bfap,               /* in */
                    uint32T pageOffset,            /* in */
                    bsInMemXtntMapT *xtntMapList,  /* in */
                    uint32T blkDescCnt,            /* in */
                    blkDescT *blkDescList,         /* in */
                    uint32T *mappedCnt             /* out */
                    )
{
    blkDescT blkDesc;
    uint32T i;
    statusT sts;
    bsInMemXtntMapT *xtntMap;

    sts = E_PAGE_NOT_MAPPED;

    i = 0;
    xtntMap = xtntMapList;
    while (xtntMap != NULL) {

        sts = x_page_to_blk( bfap,
                             pageOffset,
                             xtntMap,
                             &blkDesc);
        if (sts == EOK) {
            if (i < blkDescCnt) {
                blkDescList[i] = blkDesc;
                i++;
            } else {
                sts = E_BLKDESC_ARRAY_TOO_SMALL;
                goto EXIT_SCAN_XTNT_MAP_LIST;
            }
        } else {
            if (sts != E_PAGE_NOT_MAPPED) {
                goto EXIT_SCAN_XTNT_MAP_LIST;
            }
        }

        xtntMap = xtntMap->nextXtntMap;

    }  /* end while */

    if (i > 0) {
        *mappedCnt = i;
        sts = EOK;
    } else {
        sts = E_PAGE_NOT_MAPPED;
    }

EXIT_SCAN_XTNT_MAP_LIST:
    return sts;

}  /* end scan_xtnt_map_list */


/*
 * x_page_to_blk
 *
 * This function searches an in-memory extent map's sub extent maps for the
 * extent descriptor that describes the specified page.  Searching a sub extent
 * map causes that sub extent map to be cached.
 *
 * The caller must acquire the in-memory extent map lock prior to calling
 * this function.
 */

statusT
x_page_to_blk (
               bfAccessT *bfap,           /* in */
               uint32T pageOffset,        /* in */
               bsInMemXtntMapT *xtntMap,  /* in */
               blkDescT *blkDesc          /* in */
               )
{
    uint32T i;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;

    if (pageOffset >= xtntMap->nextValidPage) {
        sts = E_PAGE_NOT_MAPPED;
        goto EXIT_X_PAGE_TO_BLK;
    }

    sts = imm_page_to_sub_xtnt_map (pageOffset, xtntMap, 0, &i);
    if (sts == EOK) {
        subXtntMap = &(xtntMap->subXtntMap[i]);
        if (subXtntMap->cnt == 0) {
            sts = imm_load_sub_xtnt_map( bfap, xtntMap, subXtntMap);
            if (sts != EOK) {
                goto EXIT_X_PAGE_TO_BLK;
            }
        }

        sts = imm_page_to_xtnt( pageOffset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_PAGE_HOLE */
                                &i);
        if (sts == EOK) {
            blkDesc->vdIndex = subXtntMap->vdIndex;
            blkDesc->vdBlk = subXtntMap->bsXA[i].vdBlk +
              ((pageOffset - subXtntMap->bsXA[i].bsPage) * bfap->bfPageSz);
        } else {
            if (sts == E_PAGE_HOLE) {
                sts = E_PAGE_NOT_MAPPED;
            }
        }
    }

EXIT_X_PAGE_TO_BLK:
    return sts;

} /* end x_page_to_blk */


/*
 * Returns TRUE or FALSE depending on whether 'pg' is mapped in bfap's extents
 * A page is mapped if it has storage.
 * It is not mapped if it is beyond the last extent or in a "normal" hole.
 * It is also mapped if it is in a permanent hole extent and permHoleFlg is TRUE.
 * It is not mapped if it is in a permanent hole extent and permHoleFlg is FALSE.
 *
 * Returns the first page in the next extent in nextPage.
 */

int
page_is_mapped(
               bfAccessT *bfap,
               uint32T pg,
               uint32T *nextPage,
               int permHoleFlg  /* in */
              )
{
    bfSetT *bfSetp;
    statusT sts;
    bsInMemXtntT *xtnts;
    bsStripeHdrT *stripeHdr;
    uint32T i;
    int strIndex;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T stripePg;
    int results;
    int this_is_sbm_file;
    int this_is_log_file;
    int this_is_tagdir;
    int this_is_rbmt;
    int this_is_v3_bmt;
    int unlock_xtnt_map = 0;

    if ( nextPage ) {
        *nextPage = -1;
    }

    bfSetp = bfap->bfSetp;

    if ( (bfSetp->cloneId != BS_BFSET_ORIG) &&
         (bfap->cloneId == BS_BFSET_ORIG) ) {

        /*
         * This is a clone bitfile that has not had anything copied to
         * it so it still uses the orig bitfile's extent map.  Therefore,
         * say that it does not map this block.
         */
        return FALSE;
    }

    xtnts = &(bfap->xtnts);
    this_is_sbm_file = BS_IS_TAG_OF_TYPE( bfap->tag, BFM_SBM);
    this_is_log_file = BS_IS_TAG_OF_TYPE( bfap->tag, BFM_FTXLOG);
    this_is_rbmt     = RBMT_THERE(bfap->dmnP) &&
                       BS_BFTAG_RSVD(bfap->tag) &&
                       BS_BFTAG_RBMT(bfap->tag);
    this_is_tagdir  =  BS_BFTAG_REG(bfap->tag) &&
                       ((((bfSetT *)bfap->bfSetp)->dirTag.num) == -2 );

    /* Avoid needless locking and checking of in-memory extent maps for the
     * following cases:
     *  a. this is the log metadata file; its extents are only modified when 
     *     a single thread has access to it.  
     *  b. this is the SBM; the vdT.stgMap_lk will synchronize access to it.  
     *  c. This is the RBMT; the RBMT extents are only modified under an
     *     exclusive transaction. (Ver. 4 domains only)
     *  d. this is a bitfile set tag directory. The bfSetp->dirLock will
     *     synchronize access to it during file creation/deletion, and it is
     *     migrated under an exclusive transaction.
     *  e. This is the BMT on a Ver. 3 or earlier domain and this thread
     *     already has the BMT's xtntMap lock write-locked.
     *  These optimizations also avoid lock hierarchy violations.
     */
    if ( !( this_is_log_file || 
            this_is_sbm_file || 
            this_is_rbmt     || 
            this_is_tagdir ) ) {

        this_is_v3_bmt = !RBMT_THERE(bfap->dmnP) && 
                          BS_IS_TAG_OF_TYPE(bfap->tag, BFM_BMT_V3);
        if ( this_is_v3_bmt &&
             lock_holder(&bfap->xtntMap_lk.lock) ) {
             MS_SMP_ASSERT( bfap->xtnts.validFlag );
        } else {
            sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
            if ( sts != EOK ) {
                return FALSE;
            }
    
            unlock_xtnt_map = 1;
        }
    }

    switch ( xtnts->type )  {
      case BSXMT_APPEND:
        sts = imm_page_to_sub_xtnt_map (pg, xtnts->xtntMap, 0, &i);
        if (sts != EOK) {
            results = FALSE;
            break;
        }

        MS_SMP_ASSERT(xtnts->xtntMap->validCnt > i);
        subXtntMap = &(xtnts->xtntMap->subXtntMap[i]);
        sts = imm_page_to_xtnt( pg,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                permHoleFlg, /* if TRUE,perm hole returns EOK */
                                   /* if FALSE, perm hole returns E_PAGE_HOLE */
                                &i);
        if ( sts == E_PAGE_NOT_MAPPED ) {
            results = FALSE;
            break;
        }

        if ( nextPage ) {
            MS_SMP_ASSERT(subXtntMap->cnt > i + 1);
            *nextPage = subXtntMap->bsXA[i + 1].bsPage;
        }

        if ( sts == EOK ) {
            results = TRUE;
        } else if ( sts == E_PAGE_HOLE  &&
                    subXtntMap->bsXA[i].vdBlk == PERM_HOLE_START &&
                    permHoleFlg == TRUE ) {
            results = TRUE;
        } else {
            results = FALSE;
        }

        break;

      case BSXMT_STRIPE:
        results = page_is_mapped_local(bfap, pg, nextPage, permHoleFlg,
                                       NULL, NULL, NULL, FALSE);
        unlock_xtnt_map = 0;
        break;

      default:
        if ( unlock_xtnt_map ) {
            XTNMAP_UNLOCK(&bfap->xtntMap_lk)
        }
        ADVFS_SAD1 ("page_is_mapped --- bad extent map type", xtnts->type);
    }  /* end switch */

    if ( unlock_xtnt_map ) {
        XTNMAP_UNLOCK(&bfap->xtntMap_lk)
    }
    return results;
}

/* The function "page_is_mapped" is modified to return the volume index
 * and block number for the input page in addition to checking whether
 * 'pg' is mapped and returning  the first page in the next extent
 * in nextPage.
 */

int
page_is_mapped_local(
               bfAccessT *bfap,
               uint32T pg,
               uint32T *nextPage,
               int permHoleFlg,  /* in */
               vdIndexT *vdIndex,
               uint32T *vdBlk,
               uint32T *start_pg,
               int load_map
              )
{
    statusT sts;
    bsStripeHdrT *stripeHdr;
    uint32T i;
    int strIndex;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T stripePg;
    int results;
    int unlock_xtnt_map = 0;

    if ( load_map ) {
        if ( nextPage ) {
            *nextPage = -1;
        }

        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if ( sts != EOK ) {
            return FALSE;
        }
    }

    stripeHdr = bfap->xtnts.stripeXtntMap;
    strIndex = BFPAGE_TO_MAP( pg,
                              stripeHdr->cnt,
                              stripeHdr->segmentSize);
    stripePg = BFPAGE_TO_XMPAGE( pg,
                                 stripeHdr->cnt,
                                 stripeHdr->segmentSize);
    sts = imm_page_to_sub_xtnt_map( stripePg,
                                    stripeHdr->xtntMap[strIndex],
                                    0,
                                    &i);
    if ( sts != EOK ) {
        results = FALSE;
        goto end;
    }

    MS_SMP_ASSERT(stripeHdr->xtntMap[strIndex]->validCnt > i);
    subXtntMap = &(stripeHdr->xtntMap[strIndex]->subXtntMap[i]);
    sts = imm_page_to_xtnt( stripePg,
                            subXtntMap,
                            FALSE,   /* Don't use the update maps */
                            permHoleFlg, /* if TRUE,perm hole returns EOK */
                               /* if FALSE, perm hole returns E_PAGE_HOLE */
                            &i);
    if ( sts == E_PAGE_NOT_MAPPED ) {
        results = FALSE;
        goto end;
    }

    if ( nextPage ) {
        MS_SMP_ASSERT(subXtntMap->cnt > i + 1);
        *nextPage = XMPAGE_TO_BFPAGE( strIndex,
                                      subXtntMap->bsXA[i + 1].bsPage,
                                      stripeHdr->cnt,
                                      stripeHdr->segmentSize);
        if ( *nextPage > NEXT_SEGMENT_PAGE(pg, stripeHdr->segmentSize) ) {
            *nextPage = NEXT_SEGMENT_PAGE(pg, stripeHdr->segmentSize);
        }
    }

    if ( sts == EOK ) {
        results = TRUE;
    } else if ( sts == E_PAGE_HOLE &&
                subXtntMap->bsXA[i].vdBlk == PERM_HOLE_START &&
                permHoleFlg == TRUE ) {
        results = TRUE;
    } else {
        results = FALSE;
    }

    if ( vdIndex != NULL ) {
        *vdIndex = subXtntMap->vdIndex;
        *vdBlk = subXtntMap->bsXA[i].vdBlk;
        *start_pg = subXtntMap->bsXA[i].bsPage;
    }

end:
    XTNMAP_UNLOCK(&bfap->xtntMap_lk)
    return results;
}


/*
 * stg_remove_stg_start
 *
 * This function removes storage starting at the specified page offset
 * for the specified number of pages.  The bitfile is specified by an
 * access pointer.
 *
 * Storage is removed from a file in two steps. In this, the first step,
 * the storage is taken away from the file and put on the deferred delete list.
 * This step is preformed under a transaction and when completed, the
 * file extents and size reflect the truncation having ocurred. The
 * storage has not been returned to the SBM yet. In the second step
 * stg_remove_stg_start() returns the storage to the SBM in a series of
 * transactions. This two step method is used to limit the size of
 * any single transaction. If a file has a clone, then the storage is
 * given to the clone in part two instead of returning it to the SBM.
 *
 * WARNING:  This function calls bs_invalidate_pages().  It blows
 * the system if the pages in the page range are pin'ed or ref'ed when
 * it is called.  Callers of this function must synchronize with each
 * other to prevent pin'ed or ref'ed pages when calling this function.
 *
 * LOCKS:  The caller must hold the migTruncLk (this lock must
 *         be locked before starting a root transaction).
 */

statusT
stg_remove_stg_start (
                bfAccessT *bfap,        /* in */
                uint32T pageOffset,     /* in */
                uint32T pageCnt,        /* in */
                int relQuota,           /* in */
                ftxHT parentFtx,        /* in */
                uint32T *delCnt,        /* out */
                void **delList,         /* out */
                int32T doCow           /* in */
                )
{
    bfSetT *bfSetp;
    uint32T delPageCnt;
    ftxHT ftx;
    uint32T i;
    statusT sts;
    bsInMemXtntT *xtnts;
    rmStgUndoRecT undoRec;


    /* Make sure that we catch all the rogue add/remove storage places that
     * don't take the migtrunc lock.
     */
    MS_SMP_ASSERT(lock_rwholder(&(bfap->xtnts.migTruncLk)));

    *delCnt = 0;
    *delList = NULL;

    bfSetp = bfap->bfSetp;
    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    if (pageCnt == 0) {
        return EOK;
    }

    if (BS_BFTAG_RSVD (bfap->tag) != 0) {
        return ENOT_SUPPORTED;
    }

    /* A transaction has been started. We do not want to cow every page */
    /* we are deleting since bs_cow starts a subtransaction. A large number */
    /* of pages to cow would fill the log. Instead we set up the clone here */
    /* but defer cowing the pages until later. The pages will be put on the */
    /* deferred delete list. In stg_remove_stg_finish() the pages will be */
    /* given to the clone. */
    if ( bfSetp->cloneSetp != NULL && doCow) {
        bs_cow( bfap, COW_NONE, 0, 0, parentFtx );
    }

    xtnts = &(bfap->xtnts);

    /* We are setting up and UNDO so we better be part of a root ftx */

    MS_SMP_ASSERT (parentFtx.hndl);

    sts = FTX_START_N(FTA_BS_STG_REMOVE_V1, &ftx, parentFtx, bfap->dmnP, 0);

    if (sts != EOK) {
        return sts;
    }

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with mcellList_lk write-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Add to list of locks to be released when transaction completes. */
    FTX_ADD_LOCK(&(bfap->mcellList_lk), ftx);

    /* We need to exclusively lock the xtntmaps across this call 
     * since we modify the update ranges of the subxtnt maps and this
     * could cause a call to extend the subxtntmaps. Extending the subxtnt
     * maps involves replacing the subXtntMap pointer in the xtntMap.
     *
     * Since there can be other readers we must get this exclusively
     */
    
    XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk);

    sts = remove_stg( bfap,
                      pageOffset,
                      pageCnt,
                      xtnts,
                      ftx,
                      &delPageCnt,
                      delCnt,
                      (mcellPtrT **)delList);
 
    XTNMAP_UNLOCK(&bfap->xtntMap_lk);

    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if (delPageCnt == 0) {
        RAISE_EXCEPTION (EOK);
    }

    bs_invalidate_pages(bfap, pageOffset, pageCnt, 0);

    if (relQuota && bfap->bfVp) {
        long quota_change = -(long)(delPageCnt * bfap->bfPageSz);
        /* special case for when a frag was made and we are releasing
         * the original page, the quota for the frag is applied here by
         * not removing those blocks.  note we may also have additional
         * preallocated pages being removed too.
         */
        if (bfap->fragState == FS_FRAG_VALID &&
                bfap->fragPageOffset >= pageOffset &&
                bfap->fragPageOffset < pageOffset + delPageCnt) {
            quota_change += bfap->fragId.type * 2;
        }
        (void)change_quotas(bfap->bfVp, 0, quota_change, NULL, NOCRED, 0, ftx);
    }

    FTX_LOCKWRITE(&(bfap->xtntMap_lk), ftx);

    merge_xtnt_maps (xtnts);

    xtnts->allocPageCnt = xtnts->allocPageCnt - delPageCnt;

    bs_bfs_get_set_id( bfap->bfSetp, &undoRec.setId );
    undoRec.nextPage = bfap->nextPage;

    if ((pageOffset + pageCnt) == bfap->nextPage) {
        bfap->nextPage = imm_get_next_page (xtnts);
    }

    undoRec.tag = bfap->tag;
    undoRec.pageCnt = delPageCnt;

    ftx_done_u (ftx, FTA_BS_STG_REMOVE_V1, sizeof (undoRec), &undoRec);
    
    return EOK;

HANDLE_EXCEPTION:

    ftx_fail (ftx);

    if (*delList != NULL) {
        ms_free (*delList);
        *delList = NULL;
    }

    return sts;

}  /* end stg_remove_stg */


/*
 * stg_remove_stg_finish
 *
 * Second part of stg_remove_stg_start().
 */

void
stg_remove_stg_finish (
    domainT *dmnP,      /* in */
    uint32T delCnt,     /* in */
    void *delList       /* in */
    )
{
    int i;
    statusT sts;
    mcellPtrT *dList = (mcellPtrT *)delList;

    for (i = 0; i < delCnt; i++) {
        sts = del_dealloc_stg( dList[i].mcellId,
                               VD_HTOP(dList[i].vdIndex, dmnP));
        /*
         * Ignore return sts.  The storage will get cleaned up
         * (hopefull!) during the next system reboot.
         */
    }

    ms_free (dList);

}


/*
 * remove_stg
 *
 * This function removes storage starting at the page offset for the page count.
 * The page range can be anywhere in the bitfile.
 *
 * This function assumes the caller owns the mcell list lock.
 */

static
statusT
remove_stg (
            bfAccessT *bfap,       /* in */
            uint32T bfPageOffset,  /* in */
            uint32T bfPageCnt,     /* in */
            bsInMemXtntT *xtnts,   /* in, modified */
            ftxHT parentFtx,       /* in */
            uint32T *delPageCnt,   /* out */
            uint32T *delCnt,       /* out */
            mcellPtrT **delList    /* out */
            )
{
    uint32T endPageOffset;
    uint32T i;
    int mapIndex;
    uint32T mcellCnt;
    mcellPtrT *mcellList = NULL;
    uint32T pageCnt;
    uint32T pageOffset;
    uint32T segmentCnt;
    uint32T segmentSize;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalPageCnt;
    uint32T xmPageOffset;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        xtntMap = xtnts->xtntMap;
        xtntMap->updateStart = xtntMap->cnt;

        mcellCnt = 1;
        mcellList = (mcellPtrT *) ms_malloc (mcellCnt * sizeof (mcellPtrT));
        if (mcellList == NULL) {
            RAISE_EXCEPTION (ENO_MORE_MEMORY);
        }

        sts = dealloc_stg( bfap,
                           bfPageOffset,
                           bfPageCnt,
                           xtntMap,
                           -1,  /* xfer clone stg, not striped */
                           parentFtx,
                           &totalPageCnt,
                           &(mcellList[0].vdIndex),
                           &(mcellList[0].mcellId));
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        break;

      case BSXMT_STRIPE:

        segmentCnt = xtnts->stripeXtntMap->cnt;
        segmentSize = xtnts->stripeXtntMap->segmentSize;

        for (i = 0; i < xtnts->stripeXtntMap->cnt; i++) {
            xtnts->stripeXtntMap->xtntMap[i]->updateStart =
              xtnts->stripeXtntMap->xtntMap[i]->cnt;
        }  /* end for */

        mcellList = (mcellPtrT *) ms_malloc (segmentCnt * sizeof (mcellPtrT));
        if (mcellList == NULL) {
            RAISE_EXCEPTION (ENO_MORE_MEMORY);
        }

        /*
         * Verify that the last page is valid for this bitfile.  If it
         * is, all pages in the range are valid.
         */
        endPageOffset = (bfPageOffset + bfPageCnt) - 1;
        xmPageOffset = BFPAGE_TO_XMPAGE(endPageOffset, segmentCnt, segmentSize);
        mapIndex = BFPAGE_TO_MAP (endPageOffset, segmentCnt, segmentSize);
        sts = imm_page_to_sub_xtnt_map( xmPageOffset,
                                        xtnts->stripeXtntMap->xtntMap[mapIndex],
                                        0,
                                        &i);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        str_calc_page_alloc (bfPageOffset, bfPageCnt, xtnts->stripeXtntMap);

        i = 0;
        mcellCnt = 0;
        totalPageCnt = 0;

        pageOffset = bfPageOffset;
        mapIndex = BFPAGE_TO_MAP (pageOffset, segmentCnt, segmentSize);
        while ((i < segmentCnt) &&
               (xtnts->stripeXtntMap->xtntMap[mapIndex]->allocDeallocPageCnt > 0)) {

            xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];

            /*
             * Remove space from this extent map only if the extent map
             * relative page offset is within the mapped page range.
             */

            xmPageOffset = BFPAGE_TO_XMPAGE( pageOffset,
                                             segmentCnt,
                                             segmentSize);
            subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
            if (xmPageOffset < (subXtntMap->pageOffset + subXtntMap->pageCnt)) {
                pageCnt = (subXtntMap->pageOffset + subXtntMap->pageCnt) -
                  xmPageOffset;
                if (pageCnt > xtntMap->allocDeallocPageCnt) {
                    pageCnt = xtntMap->allocDeallocPageCnt;
                }

                sts = dealloc_stg( bfap,
                                   xmPageOffset,
                                   pageCnt,
                                   xtntMap,
                                   mapIndex + 1,  /* xfer to clone stripe */
                                   parentFtx,
                                   &(xtntMap->allocDeallocPageCnt),
                                   &(mcellList[mcellCnt].vdIndex),
                                   &(mcellList[mcellCnt].mcellId));
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }

                if (xtntMap->allocDeallocPageCnt > 0) {
                    mcellCnt++;
                    totalPageCnt = totalPageCnt + xtntMap->allocDeallocPageCnt;
                }
            }
            i++;
            pageOffset = NEXT_SEGMENT_PAGE (pageOffset, segmentSize);
            mapIndex = NEXT_MAP (mapIndex, segmentCnt);

        }  /* end while */

        break;

      default:
        ADVFS_SAD0("remove_stg(1): bad extent map type");

    }  /* end switch */

    *delPageCnt = totalPageCnt;
    if (totalPageCnt == 0) {
        RAISE_EXCEPTION (EOK);
    }
    *delCnt = mcellCnt;
    *delList = mcellList;

    return EOK;

HANDLE_EXCEPTION:

    /*
     * Run through the updated extent maps and unload the sub extent maps.
     */

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        unload_sub_xtnt_maps (xtnts->xtntMap);

        break;

      case BSXMT_STRIPE:

        for (i = 0; i < xtnts->stripeXtntMap->cnt; i++) {
            unload_sub_xtnt_maps (xtnts->stripeXtntMap->xtntMap[i]);
        }  /* end for */

        break;

      default:
        ADVFS_SAD0("remove_stg(2): bad extent map type");

    }  /* end switch */

    if (mcellList != NULL) {
        ms_free (mcellList);
    }

    return sts;

}  /* end remove_stg */


/*
 * dealloc_stg
 *
 * This function removes storage starting at the page offset for the page count.
 * The page range can be anywhere in the bitfile.
 *
 * This function assumes the caller owns the mcell list lock.
 */

static
statusT
dealloc_stg (
             bfAccessT *bfap,           /* in */
             uint32T bfPageOffset,      /* in */
             uint32T bfPageCnt,         /* in */
             bsInMemXtntMapT *xtntMap,  /* in */
             int xferFlg,               /* in */
             ftxHT parentFtx,           /* in */
             uint32T *delPageCnt,       /* out */
             vdIndexT *delVdIndex,      /* out */
             bfMCIdT *delMcellId        /* out */
             )
{
    bfSetT *bfSetDesc;
    bfSetT *cloneSetp = NULL;
    uint32T cnt;
    bsInMemXtntMapT *delXtntMap =  NULL;
    int emptyFlag = 0;
    uint32T i;
    bfMCIdT mcellId;
    uint32T remPageCnt;
    uint32T remPageOffset;
    uint32T reuseIndex;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemSubXtntMapT *sxm0, *sxm1, *sxmN;
    uint32T totalPageCnt;
    vdIndexT vdIndex;

    /*
     * Determine if there are any pages to deallocate.
     */
    sts = imm_get_alloc_page_cnt( xtntMap,
                                  bfPageOffset,
                                  bfPageCnt,
                                  &totalPageCnt);
    if (sts != EOK) {
        return sts;
    }

    if (totalPageCnt == 0) {
        *delPageCnt = totalPageCnt;
        return EOK;
    }

    bfSetDesc = bfap->bfSetp;

    if ( BFSET_VALID(bfSetDesc->cloneSetp) && bfap->noClone == FALSE) {
        cloneSetp = bfSetDesc->cloneSetp;
    }

    /*
     * Remove the mappings for the page range from the in-memory extent
     * map.
     */

    sts = imm_remove_page_map( bfPageOffset,
                               bfPageCnt,
                               xtntMap,
                               bfap->xtnts.type,
                               &remPageOffset,
                               &remPageCnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if (xtntMap->updateStart == 0) 
    {
        /*
         * If we return from the above routine with no updates
         * then we must be truncating the file. This does not include 
         * truncating all pages.
         */

        emptyFlag = 1;
    }

    /*
     * Create the delete extent map and copy the original sub extent maps
     * that describe the to-be-modified range.  These are exact copies,
     * including the mcell pointers.
     */

    cnt = (xtntMap->origEnd - xtntMap->origStart) + 1;
    sts = imm_create_xtnt_map( xtntMap->blksPerPage,
                               xtntMap->domain,
                               cnt,
                               0,  /* termPage */
                               bsNilVdIndex,  /* termVdIndex */
                               &delXtntMap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    for (i = 0; i < cnt; i++) {
        sts = imm_copy_sub_xtnt_map( bfap,
                                     xtntMap,
                                   &xtntMap->subXtntMap[xtntMap->origStart + i],
                                     &delXtntMap->subXtntMap[i]);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }  /* end for */

    delXtntMap->cnt = cnt;
    delXtntMap->validCnt = cnt;
    delXtntMap->updateStart = delXtntMap->cnt;

    /*
     * Decide which of the to-be-modified original mcells go to the update
     * sub extent maps and which go to the delete sub extent maps.
     */

    if (emptyFlag == 0) {


        /*
         * The "origStart" mcell goes to the "updateStart" sub extent map.
         */
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
        if(subXtntMap->vdIndex != xtntMap->subXtntMap[xtntMap->origStart].vdIndex)
        {
            /* We can not reuse the first MCELL. We know that the origStart
             * is not the PRIMARY otherwise the indexes would have matched
             */
            uint32T rType = xtntMap->subXtntMap[xtntMap->origStart].type;
            MS_SMP_ASSERT((rType != BSR_XTNTS) && (rType != BSR_SHADOW_XTNTS));
            reuseIndex = 0;
        }
        else
        {
            /* We can reuse the first sub extent */
            reuseIndex=1;

            subXtntMap->vdIndex = xtntMap->subXtntMap[xtntMap->origStart].vdIndex;
            subXtntMap->mcellId = xtntMap->subXtntMap[xtntMap->origStart].mcellId;
        }

        /*
         * Allocate mcells for the "updateStart" + 1 thru "updateEnd" sub extent
         * maps.
         */
        MS_SMP_ASSERT(xtntMap->updateEnd - xtntMap->updateStart < 4);
        for (i = xtntMap->updateStart + reuseIndex; i <= xtntMap->updateEnd; i++) {
            subXtntMap = &(xtntMap->subXtntMap[i]);
            sts = stg_alloc_new_mcell( bfap,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       subXtntMap->vdIndex,
                                       parentFtx,
                                       &(subXtntMap->mcellId),
                                       FALSE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            subXtntMap->mcellState = NEW_MCELL;
        }  /* end for */

        /*
         * The "origStart" + 1 thru "origEnd" mcells go to the 2nd thru last
         * delete sub extent maps.  The mcell pointers for these sub extent
         * maps are already set.
         *
         * Allocate an mcell for the first delete sub extent map and save
         * the contents of the sub extent map.
         */

        if (reuseIndex)
        {
            subXtntMap = &(delXtntMap->subXtntMap[0]);
            sts = stg_alloc_new_mcell( bfap,
                                       (cloneSetp ? cloneSetp->dirTag : 
                                        bfSetDesc->dirTag),
                                       bfap->tag,
                                       subXtntMap->vdIndex,
                                       parentFtx,
                                       &(subXtntMap->mcellId),
                                       FALSE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            
            sts = odm_create_xtnt_rec( bfap,
                                       -1,  /* allocVdIndex */
                                       subXtntMap,
                                       cloneSetp ? xferFlg : 0,
                                       parentFtx);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
    } 
    else 
    {

        /*
         * The "origStart" thru "origEnd" mcells go to the 1st thru the last
         * delete sub extent maps.  The mcell pointers are already set.
         */
        
        if (xtntMap->cnt >= xtntMap->maxCnt) {
            sts = imm_extend_xtnt_map (xtntMap);
            if (sts != EOK) {
                return sts;
            }
        }

        MS_SMP_ASSERT(xtntMap->origStart > 0);

        /*
         * Include the previous sub extent map in the to-be-modified range by
         * copying the previous sub extent map to the update sub extent
         * map.  The copy operation sets the new "origStart" mcell in
         * the "updateStart" sub extent map.
         *
         * Since we are going to reuse the new origStart MCELL , set the reuse flag
         * so we don't remove it.
         */
        xtntMap->updateStart=xtntMap->updateEnd=xtntMap->cnt;
        xtntMap->origStart--;
        reuseIndex=1;

        sts = imm_copy_sub_xtnt_map( bfap,
                                     xtntMap,
                                     &xtntMap->subXtntMap[xtntMap->origStart],
                                     &xtntMap->subXtntMap[xtntMap->updateStart]);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        xtntMap->cnt++;
    }

    /*
     * Remove the mcells associated with the "origStart" + 1 thru
     * "origEnd" sub extent maps from the bitfile's extent mcell list.
     * These belong to the on-disk delete extent map.
     */

    sts = odm_remove_mcells_from_xtnt_map( bfap->dmnP,
                                           bfap->tag,
                                           xtntMap,
                                           reuseIndex,
                                           parentFtx);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Update the original map minus the deleted page range.
     */

    sts = x_update_ondisk_xtnt_map (xtntMap->domain, bfap, xtntMap, parentFtx);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Create the on-disk delete extent map and insert it onto the delete list
     * for later deletion.
     *
     * First, make sure that the in-memory delete extent map and its on-disk
     * mcells describe only the storage that is going to be deallocated.
     */

    sts = clip_del_xtnt_map( bfap,
                             remPageOffset,
                             remPageCnt,
                             delXtntMap,
                             parentFtx);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Relink the first mcell on the delete extent map with the list
     * of mcells starting at the second subextent map.  The list
     * starting at the second mcell is already linked; it is the list
     * for the file from which storage is being deallocated.  The first
     * mcell from the target file does not get moved to the delete
     * extent map; that mcell gets allocated.
     *
     * If the entire file is to be deallocated (emptyFlag == 1), no 
     * mcell gets allocated, so re-linking is not necessary.
     *
     * If the delXtntMap->cnt == 1, then the only entry on the list
     * is for the mcell which gets allocated, so re-linking is not
     * necessary.
     */
    sxm0 = &delXtntMap->subXtntMap[0];
    if (emptyFlag == 0 && delXtntMap->cnt > 1) {
        sxm1 = &delXtntMap->subXtntMap[1];
        sxmN = &delXtntMap->subXtntMap[delXtntMap->cnt-1];
        sts = bmt_link_mcells ( bfap->dmnP,
                                bfap->tag,
                                sxm0->vdIndex,
                                sxm0->mcellId,
                                sxm1->vdIndex,
                                sxm1->mcellId,
                                sxmN->vdIndex,
                                sxmN->mcellId,
                                parentFtx);
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * If there is extent information in the primary mcell, then there is
     * no need to create a new extent header record and mcell.  Just use 
     * the one we have.
     */
    if (sxm0->type == BSR_XTNTS) {
        vdIndex = sxm0->vdIndex;
        mcellId = sxm0->mcellId;
    }
    else sts = create_xtnt_map_hdr ( bfap,
                                     delXtntMap->subXtntMap[0].vdIndex,
                                     delXtntMap->subXtntMap[0].mcellId,
                                     delXtntMap->subXtntMap[delXtntMap->cnt-1].vdIndex,
                                     delXtntMap->subXtntMap[delXtntMap->cnt-1].mcellId,
                                     cloneSetp ? xferFlg : 0,
                                     parentFtx,
                                     &vdIndex,
                                     &mcellId);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = del_add_to_del_list( mcellId,
                               VD_HTOP(vdIndex, bfap->dmnP),
                               TRUE,  /* start sub ftx */
                               parentFtx);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_delete_xtnt_map (delXtntMap);

    *delPageCnt = totalPageCnt;
    *delVdIndex = vdIndex;
    *delMcellId = mcellId;

    return sts;

HANDLE_EXCEPTION:
    unload_sub_xtnt_maps (xtntMap);

    if (delXtntMap != NULL) {
        imm_delete_xtnt_map (delXtntMap);
    }

    return sts;

}  /* end dealloc_stg */


/*
 * clip_del_xtnt_map
 *
 * This function determines if the starting page is the first page
 * mapped by the first sub extent map and the last page is the last
 * page mapped by the last sub extent map.  If not, the on-disk and
 * in-memory extent maps are modified and the changes saved to disk.
 *
 * This function is needed because we don't want to put storage information
 * not in the page range on the delete pending list.
 */

static
statusT
clip_del_xtnt_map (
                   bfAccessT *bfap,           /* in */
                   uint32T bfPageOffset,      /* in */
                   uint32T bfPageCnt,         /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   ftxHT parentFtx            /* in */
                   )
{
    uint32T descIndex;
    uint32T endPageOffset;
    bsXtntT firstDesc;
    uint32T i;
    bsXtntT lastDesc;
    uint32T moveCnt;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    int updateFlag = 0;

    endPageOffset = (bfPageOffset + bfPageCnt) - 1;

    /*
     * Check for start page alignment.
     */

    subXtntMap = &(xtntMap->subXtntMap[0]);
    if (bfPageOffset != subXtntMap->bsXA[0].bsPage) {

        sts = imm_page_to_xtnt( bfPageOffset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_PAGE_HOLE */
                                &descIndex);
        if ((sts != EOK) && (sts != E_PAGE_HOLE)) {
            return sts;
        }

        if (bfPageOffset != subXtntMap->bsXA[descIndex].bsPage) {
            imm_split_desc( bfPageOffset,
                            xtntMap->blksPerPage,
                            &(subXtntMap->bsXA[descIndex]),  /* orig */
                            &firstDesc,
                            &lastDesc);
            subXtntMap->bsXA[descIndex] = lastDesc;

            subXtntMap->updateStart = descIndex;
            subXtntMap->updateEnd = subXtntMap->updateStart;
        }

        if (descIndex != 0) {
            /*
             * Shift the entries in the first sub extent map up so that
             * the extent descriptor that maps the first page is the first
             * descriptor.
             */
            moveCnt = subXtntMap->cnt - descIndex;
            for (i = 0; i < moveCnt; i++) {
                subXtntMap->bsXA[i] = subXtntMap->bsXA[descIndex + i];
            }  /* end for */
            subXtntMap->cnt = moveCnt;

            subXtntMap->updateStart = 0;
            subXtntMap->updateEnd = subXtntMap->cnt - 1;
        }

        updateFlag = 1;

        subXtntMap->pageOffset = subXtntMap->bsXA[0].bsPage;
        subXtntMap->pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
          subXtntMap->pageOffset;

        xtntMap->updateStart = 0;
        xtntMap->updateEnd = xtntMap->updateStart;
    }

    /*
     * Check for end page alignment.
     */

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
    if (endPageOffset < (subXtntMap->bsXA[0].bsPage)) 
    {
        /* This is a case where the originalEnd range has no pages
         * in the removal range. This is because the hole being created
         * needs to stick to the storage in this subextent. This made it
         * onto the delete list since the MCELL must go but not any of
         * the pages.
         * Mark the range as a one entry termination descriptor with
         * no corresponding storage.
         * NOTE:
         * This is one of the few places that a subextent can contain
         * only a termination descriptor
         */

        subXtntMap->bsXA[0].vdBlk = -1;
        subXtntMap->cnt = 1;

        subXtntMap->pageOffset = subXtntMap->bsXA[0].bsPage;
        subXtntMap->pageCnt = 0;

        subXtntMap->updateStart = 0;
        subXtntMap->updateEnd = 0;

        if (updateFlag == 0) {
            xtntMap->updateStart = xtntMap->cnt - 1;
            xtntMap->updateEnd = xtntMap->updateStart;
        } else {
            xtntMap->updateStart = 0;
            xtntMap->updateEnd = xtntMap->cnt - 1;
        }

        updateFlag = 1;

        /* Set it up so that the next to last is used for the end range */

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 2]);
    }

    if (endPageOffset != (subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage - 1)) 
    {
        sts = imm_page_to_xtnt( endPageOffset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_PAGE_HOLE */
                                &descIndex);
        if ((sts != EOK) && (sts != E_PAGE_HOLE)) {
            return sts;
        }

        if (endPageOffset != (subXtntMap->bsXA[descIndex + 1].bsPage - 1)) {
            imm_split_desc( endPageOffset + 1,
                            xtntMap->blksPerPage,
                            &(subXtntMap->bsXA[descIndex]),  /* orig */
                            &firstDesc,
                            &lastDesc);
            subXtntMap->bsXA[descIndex + 1].bsPage = lastDesc.bsPage;
        }
        subXtntMap->bsXA[descIndex + 1].vdBlk = -1;
        subXtntMap->cnt = descIndex + 2;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

        subXtntMap->pageOffset = subXtntMap->bsXA[0].bsPage;
        subXtntMap->pageCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage -
          subXtntMap->pageOffset;

        if ((xtntMap->cnt == 1) && (updateFlag != 0)) {
            subXtntMap->updateStart = 0;
            subXtntMap->updateEnd = subXtntMap->cnt - 1;
        } else {
            subXtntMap->updateStart = subXtntMap->cnt - 1;
            subXtntMap->updateEnd = subXtntMap->updateStart;
        }

        if (updateFlag == 0) {
            xtntMap->updateStart = xtntMap->cnt - 1;
            xtntMap->updateEnd = xtntMap->updateStart;
        } else {
            xtntMap->updateStart = 0;
            xtntMap->updateEnd = xtntMap->cnt - 1;
        }

        updateFlag = 1;
    }

    if (updateFlag != 0) {
        sts = x_update_ondisk_xtnt_map(bfap->dmnP, bfap, xtntMap, parentFtx);
        if (sts != EOK) {
            return sts;
        }
    }

    return EOK;

}  /* end clip_del_xtnt_map */


/*
 * merge_xtnt_maps
 *
 * This function, for each extent map, merges the modified portion of the
 * map with the unmodified protion of the map.
 */

static
void
merge_xtnt_maps (
                 bsInMemXtntT *xtnts  /* in */
                 )
{
    uint32T i;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        merge_sub_xtnt_maps (xtnts->xtntMap);
        break;

      case BSXMT_STRIPE:

        for (i = 0; i < xtnts->stripeXtntMap->cnt; i++) {
            merge_sub_xtnt_maps (xtnts->stripeXtntMap->xtntMap[i]);
        }  /* end for */

        break;

      default:
        ADVFS_SAD0("merge_xtnt_maps: bad extent map type");

    }  /* end switch */

    return;

}  /* end merge_xtnt_maps */


/*
 * merge_sub_xtnt_maps
 *
 * This function merges the modified subextent maps (from updateStart to
 * updateEnd) with the unmodified subextent maps (from origStart to origEnd).
 * When two maps intersect, the modified map overrides the unmodified map.
 *
 * There are 3 cases:
 * 1) The to-be-replaced subextnts are at the end of the array of unmodified
 *    subextents and the modified subextents follow them.
 *    e.g. change                           to       this
 *         cnt = 4, validCnt = 3                     cnt = validCnt = 2
 *         subXtntMap[0]                             subXtntMap[0] (no change)
 *         subXtntMap[1] origStart                   subXtntMap[1] (old 3)
 *         subXtntMap[2] origEnd
 *         subXtntMap[3] updateStart, updateEnd
 * 2) The update subextents (updateStart through updateEnd) are not
 *    adjacent to the to-be-replaced subextents (origStart through origEnd)
 *    and the number of update subextents is less than or equal to the
 *    number of to-be-replaced subextent maps.  So the update subextent
 *    maps fit in the space vacated by the to-be-replaced subextent maps.
 *    The subextents between origEnd and updateStart are unmodified or
 *    unused subextents.
 *    e.g. change                           to       this
 *         cnt = 6, validCnt = 4                     cnt = validCnt = 3
 *         subXtntMap[0]                             subXtntMap[0] (no change)
 *         subXtntMap[1] origStart                   subXtntMap[1] (old 5)
 *         subXtntMap[2] origEnd                     subXtntMap[2] (old 3)
 *         subXtntMap[3]
 *         subXtntMap[4]
 *         subXtntMap[5] updateStart, updateEnd
 *    Subextent 4 is unused.
 *
 * 3) The update subextents (updateStart through updateEnd) are not
 *    adjacent to the to-be-replaced subextents (origStart through origEnd)
 *    and the number of update subextents is greater than the
 *    number of to-be-replaced subextent maps. The update subextent maps
 *    will not fit in the space vacated by the to-be-replaced subextent maps.
 *    The subextent maps will have to be moved around like musical chairs.
 *    e.g. change                           to       this
 *         cnt = 5, validCnt = 3                     cnt = validCnt = 3
 *         subXtntMap[0]                             subXtntMap[0] (no change)
 *         subXtntMap[1] origStart,origEnd           subXtntMap[1] (old 3)
 *         subXtntMap[2]                             subXtntMap[2] (old 4)
 *         subXtntMap[3] updateStart                 subXtntMap[3] (old 2)
 *         subXtntMap[4] updateEnd
 */

static
void
merge_sub_xtnt_maps (
                     bsInMemXtntMapT *xtntMap  /* in */
                     )
{
    uint32T i;
    uint32T moveCnt;
    uint32T newOrigStart;
    uint32T newSaveEnd;
    uint32T origCnt;
    uint32T origStart;
    uint32T saveCnt;
    uint32T saveEnd;
    uint32T updateCnt;
    uint32T updateStart;
    uint32T growth;

    if (xtntMap->updateStart >= xtntMap->cnt) {
        /*
         * The extent map didn't change.
         */
        return;
    }

    /*
     * Reset the "updateStart" field for each update entry.
     */
    for (i = xtntMap->updateStart; i <= xtntMap->updateEnd; i++) {
        xtntMap->subXtntMap[i].updateStart = xtntMap->subXtntMap[i].cnt;
    }  /* end for */

    /*
     * We are going to replace some of the original sub extent maps so
     * unload their previous extent arrays.
     */
    for (i = xtntMap->origStart; i <= xtntMap->origEnd; i++) {
        imm_unload_sub_xtnt_map (&(xtntMap->subXtntMap[i]));
    }  /* end for */

    origCnt = (xtntMap->origEnd + 1) - xtntMap->origStart;
    updateCnt = (xtntMap->updateEnd + 1) - xtntMap->updateStart;
    growth = updateCnt - origCnt;

    if (xtntMap->origEnd == (xtntMap->updateStart - 1)) {
        /*
         * The last to-be-modified original entry and the first update
         * entry are adjacent.  Just shift the update entries into position.
         */
        for (i = 0; i < updateCnt; i++) {
            xtntMap->subXtntMap[xtntMap->origStart + i] =
              xtntMap->subXtntMap[xtntMap->updateStart + i];
        }  /* end for */
    } else {

        if (updateCnt <= origCnt) {

            /*
             * The number of original entries that will be replaced is greater
             * than the number of update entries.  Move the update entries into
             * position and shift the remaining trailing originals, if any, up.
             */

            /*
             * Move the update entries onto the original entries that are being
             * replaced.
             */
            for (i = 0; i < updateCnt; i++) {
                xtntMap->subXtntMap[xtntMap->origStart + i] =
                  xtntMap->subXtntMap[xtntMap->updateStart + i];
            }  /* end for */

            if (updateCnt < origCnt) {
                /*
                 * Move the original entries, if any, that are positioned
                 * after the replaced entries up next to the last new
                 * entry.
                 */
                origStart = xtntMap->origEnd + 1;
                newOrigStart = xtntMap->origStart + updateCnt;
                moveCnt = xtntMap->updateStart - origStart;
                for (i = 0; i < moveCnt; i++) {
                    xtntMap->subXtntMap[newOrigStart + i] =
                      xtntMap->subXtntMap[origStart + i];
                }  /* end for */
            }
        } else {

            /*
             * The number of update entries is greater than the number of
             * original entries that will be replaced.  The update entries
             * must be moved into place in stages.
             */

            /*
             * The idea is to make space for some update entries and move the
             * update entries into the vacated space.  When we first start,
             * the "xtntMap->origStart" thru "xtntMap->origEnd" entries are
             * treated as vacant because they are being replaces.  So, we
             * move as many of the update entries as possible into the space.
             *
             * Next, we must make vacant space for some more update entries.
             * We do this by shifting the original entries positioned after
             * the just-moved update entries down by the number of update
             * entries we want to save.  Then, we move some update entries
             * into the vacant space.  We do this until all the update entries
             * are in their correct positions.
             *
             * NOTE:  I could have shuffled entries around more efficiently by
             * allocating some memory.  But, since this routine can't fail, I
             * can't allocate memory for fear of not getting any.
             */

            origStart = xtntMap->origStart;
            updateStart = xtntMap->updateStart;
            moveCnt = origCnt;

            saveEnd = xtntMap->updateStart - 1;
            saveCnt = saveEnd - xtntMap->origEnd;

            /*
             * Move some update entries onto the original entries that are being
             * replaced.
             */
            for (i = 0; i < moveCnt; i++) {
                xtntMap->subXtntMap[origStart + i] =
                  xtntMap->subXtntMap[updateStart + i];
            }  /* end for */
            updateCnt = updateCnt - moveCnt;

            while (updateCnt > 0) {

                origStart = origStart + moveCnt;
                updateStart = updateStart + moveCnt;

                /*
                 * Calculate the number of update entries to move.
                 */
                moveCnt = (xtntMap->updateEnd + 1) - updateStart;
                if (moveCnt > origCnt) {
                    moveCnt = origCnt;
                }

                /*
                 * Save some original entries in the space vacated by the update
                 * entries.
                 */
                newSaveEnd = saveEnd + moveCnt;
                for (i = 0; i < saveCnt; i++) {
                    xtntMap->subXtntMap[newSaveEnd - i] =
                      xtntMap->subXtntMap[saveEnd - i];
                }  /* end for */
                saveEnd = newSaveEnd;

                /*
                 * Move some update entries into the space vacated by the
                 * just-moved original entries.
                 */
                for (i = 0; i < moveCnt; i++) {
                    xtntMap->subXtntMap[origStart + i] =
                      xtntMap->subXtntMap[updateStart + i];
                }  /* end for */
                updateCnt = updateCnt - moveCnt;
            }  /* end while */
        }  /* end else updateCnt > origCnt */
    }  /* end else update subextents in middle of subextent array */

    xtntMap->cnt = xtntMap->validCnt + growth;
    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->updateStart = xtntMap->cnt;
    xtntMap->nextValidPage =
                        (xtntMap->subXtntMap[xtntMap->validCnt - 1].pageOffset +
                           xtntMap->subXtntMap[xtntMap->validCnt - 1].pageCnt) -
                                xtntMap->subXtntMap[0].pageOffset;

    MS_SMP_ASSERT(imm_check_xtnt_map(xtntMap) == EOK);

    return;

}  /* end merge_sub_xtnt_maps */


/*
 * unload_sub_xtnt_maps
 *
 * This function unloads one or more sub extent maps in the specified map.
 *
 * The sub extent maps to unload are specified by the extent map's "updateStart"
 * and "updateEnd" fields.  If the extent map's "updateStart" value is equal to
 * the number of sub extent maps, no sub extent maps are unloaded.
 *
 */

static
void
unload_sub_xtnt_maps (
                      bsInMemXtntMapT *xtntMap  /* in */
                      )
{
    uint32T i;

    if (xtntMap->updateStart >= xtntMap->cnt) {
        return;
    }

    for (i = xtntMap->updateStart; i <= xtntMap->updateEnd; i++) {
        imm_unload_sub_xtnt_map (&(xtntMap->subXtntMap[i]));
    }  /* end for */

    xtntMap->cnt = xtntMap->validCnt;
    xtntMap->updateStart = xtntMap->cnt;

    return;

}  /* end unload_sub_xtnt_maps */

#ifdef ADVFS_SMP_ASSERT
/*
 * Check whether the current thread holds the given lock.
 * The lockstats part only works in lockmode = 4, so
 * don't use it if lockmode < 4.
 */
static
int
lock_rwholder(
              lock_t lock
             )
{
    struct thread *th;
    int i;

    if (!lock_islocked(lock))
        return FALSE;
    
    if (lockmode < 4)
        return TRUE;

    th = current_thread();
    for (i = 0; i < th->lock_count; i++)
        if (lock == th->lock_addr[i])
            return TRUE;

    return FALSE;
} /* end lock_rwholder */
#endif
