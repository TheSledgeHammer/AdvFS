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
 *  MSFS/bs
 *
 * Abstract:
 *
 *  bs_stg.c
 *  This module contains routines related to bitfile storage allocation and
 *  to in-memory and on-disk extent map maintenance.
 *
 */

#include <sys/param.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_delete.h>
#include <bs_extents.h>
#include <bs_ims.h>
#include <bs_inmem_map.h>
#include <bs_stg.h>
#include <ftx_privates.h>    /* for FTX_MX_PINP */
#include <bs_ods.h>        /* needed for BS_IS_TAG_OF_TYPE() */
#include <advfs_evm.h>     
#include <bs_public_kern.h>
#include <bs_snapshot.h>
#include <bs_sbm.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_STG

/****************/
/* Private type */
/****************/

typedef struct mcellPtr {
    bfMCIdT mcellId;
} mcellPtrT;

/*******************************/
/* Private function prototypes */
/*******************************/

static
statusT
add_stg (
         bfAccessT *bfap,       /* in */
         bf_fob_t fob_offset,  /* in */
         bf_fob_t fob_cnt,     /* in */
         bf_fob_t minimum_contiguous_fobs, /* in */
         int allowOverlapFlag,  /* in */
         bsInMemXtntT *xtnts,   /* in, modified */
         ftxHT parentFtx,       /* in */
         int updateDisk,        /* in */
         bf_fob_t *alloc_fob_cnt /* out */
         );

static
statusT
add_rsvd_stg (
              bfAccessT *bfap,       /* in */
              bf_fob_t fob_offset,          /* in */
              bf_fob_t fob_cnt,             /* in */
              bsInMemXtntT *xtnts,   /* in, modified */
              ftxHT parentFtx,       /* in */
              bf_fob_t *alloc_fob_cnt       /* out */
              );

static
statusT
alloc_stg (
           bfAccessT *bfap,           /* in */
           bf_fob_t fob_offset,       /* in */
           bf_fob_t fob_cnt,          /* in */
           bf_fob_t minimum_contiguous_fobs, /* in */
           vdIndexT bfVdIndex,        /* in */
           bsInMemXtntMapT *xtntMap,  /* in */
           bsAllocHintT alloc_hint,   /* in */
           ftxHT parentFtx,           /* in */
           int updateDisk,            /* in */
           bf_fob_t *alloc_fob_cnt    /* out */
           );

static
statusT
alloc_append_stg (
                  bfAccessT *bfap,          /* in */
                  bf_fob_t fob_offset,      /* in */
                  bf_fob_t fob_cnt,         /* in */
                  bf_fob_t minimum_contiguous_fobs, /* in */
                  vdIndexT bfVdIndex,       /* in */
                  bsInMemXtntMapT *xtntMap, /* in */
                  uint32_t mapIndex,        /* in */
                  bsAllocHintT alloc_hint,  /* in */
                  ftxHT parentFtx,          /* in */
                  int updateDisk,           /* in */
                  bf_fob_t *alloc_fob_cnt   /* out */
                  );

static
statusT
alloc_hole_stg (
                bfAccessT *bfap,           /* in */
                bf_fob_t fob_offset,       /* in */
                bf_fob_t fob_cnt,          /* in */
                bf_fob_t minimum_contiguous_fobs, /* in */
                vdIndexT bfVdIndex,        /* in */
                bsInMemXtntMapT *xtntMap,  /* in */
                uint32_t mapIndex,         /* in */
                uint32_t descIndex,        /* in */
                bsAllocHintT alloc_hint,   /* in */
                ftxHT parentFtx,           /* in */
                int updateDisk,            /* in */
                bf_fob_t *alloc_fob_cnt    /* out */
                );


static
statusT
alloc_from_one_disk (
                     bfAccessT *bfap,                 /* in */
                     bf_fob_t fob_offset,             /* in */
                     bf_fob_t fob_cnt,                /* in */
                     bf_fob_t minimum_contiguous_fobs, /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     bsAllocHintT *alloc_hint,        /* in/out */
                     ftxHT parentFtx,                 /* in */
                     bf_fob_t *rsvd_fob_cnt,          /* in/out */
                     bf_vd_blk_t *rsvd_fob_vd_blk,    /* in/out */
                     bf_fob_t *alloc_fob_cnt          /* out */
                     );


static
statusT
cp_stg_alloc_in_one_mcell(
                      bfAccessT *bfap,                /* in  add stg this file*/
                      bf_fob_t fob_offset,/* in  start here */
                      bf_fob_t fob_cnt,   /* in  requested length */
                      bf_fob_t minimum_contiguous_fobs, /* in */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      ftxHT *ftxHp,                   /* in */
                      bf_fob_t *alloc_fob_cnt, /* out. this much added */
                      bf_vd_blk_t dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                      bsAllocHintT alloc_hint
                      );

static
statusT
cp_hole_alloc_in_one_mcell(
                      bf_fob_t fob_offset,/* in  start here */
                      bf_fob_t fob_cnt,   /* in  requested length */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      bf_fob_t *alloc_fob_cnt /* out. this much added */
                      );


static
statusT
add_extent(
            bfAccessT *bfap,
            bsInMemSubXtntMapT *subXtntMap,  /* add extent to this map */
            uint32_t *xIp,                   /* add extent at this index */
            bf_fob_t fob_offset,             /* first fob of extent */
            bf_vd_blk_t blkOff,              /* first block of extent */
            bf_fob_t fob_cnt,                /* extent to be this long */
            ftxHT *ftxHp,
            bsXtntT *termDescp,
            int *pinCntp,
            bsAllocHintT alloc_hint
            );


static
statusT
stg_alloc_one_xtnt(
                   bfAccessT *bfap,          /* in  find stg for this file */
                   vdT *vdp,                 /* in  in this volume */
                   bf_fob_t fob_cnt,        /* in  Requested length */
                   bf_vd_blk_t dstBlkOffset, /* in, checked if hint == 
                                                BS_ALLOC_MIG_RSVD */
                   bf_fob_t minimum_contiguous_fobs, /* in */
                   bsAllocHintT alloc_hint,  /* in */
                   ftxHT ftxH,               /* in */
                   bf_fob_t *alloc_fob_cnt_p,   /* out  Got this much stg. */
                   bf_vd_blk_t *alloc_vd_blk_p,         /* in/out  At this location.*/
                   int *pinCntp              /* in/out  this many pins left */
                   );


static
statusT
stg_alloc_from_one_disk (
                         bfAccessT *bfAccess,  /* in */
                         vdIndexT bfVdIndex,  /* in */
                         bfTagT bfSetTag,  /* in */
                         bfTagT bfTag,  /* in */
                         bf_fob_t fob_offset,      /* in */
                         bf_fob_t fob_cnt,         /* in */
                         bf_fob_t minimum_contiguous_fobs, /* in */
                         bsInMemXtntMapT *xtntMap,  /* in */
                         bsAllocHintT *alloc_hint,  /* in */
                         ftxHT parentFtx,           /* in */
                         bf_fob_t *alloc_fob_cnt /* out */
        );


static
statusT
remove_stg (
            bfAccessT *bfap,       /* in  */
            bf_fob_t fob_offset,   /* in  */
            bf_fob_t fob_cnt,      /* in  */
            bsInMemXtntT *xtnts,   /* in, modified */
            ftxHT parentFtx,       /* in  */
            bf_fob_t *del_fob_cnt, /* out */
            uint32_t *delCnt,      /* out */
            mcellPtrT **delList,   /* out */
            int32_t force_alloc   /* in  */
            );

static
statusT
dealloc_stg (
             bfAccessT *bfap,          /* in  */
             bf_fob_t fob_offset,      /* in  */
             bf_fob_t fob_cnt,         /* in  */
             bsInMemXtntMapT *xtntMap, /* in  */
             ftxHT parentFtx,          /* in  */
             bf_fob_t *del_fob_cnt,    /* out */
             bfMCIdT *delMcellId,      /* out */
             int32_t force_alloc      /* in  */
             );


static
statusT
clip_del_xtnt_map (
                   bfAccessT *bfap,           /* in */
                   bf_fob_t fob_offset,      /* in */
                   bf_fob_t fob_cnt,         /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   ftxHT parentFtx            /* in */
                   );


typedef struct thresholdAlertRtDnRec
{
    thresholdType_t type; 
    bfSetIdT        bfSetID; /* only used for FSET_UPPER, FSET_LOWER types */
} thresholdAlertRtDnRec_t;

typedef struct addStgUndoRec
{
    bfSetIdT setId;             /* Set id */
    bfTagT tag;                 /* File that had storage added */
    bf_fob_t asurNextFob; 
                                /* Previous value of the
                                 * bfAcess->next_fob before the 
                                 * storage was added */
    bf_fob_t asurFobOffset;
                                /* File offset in 1k units were storage was
                                 * added */
    bf_fob_t asurFobCnt;
                                /* Count of 1k units that were added to the
                                 * file */
} addStgUndoRecT;

/*
 * add_stg_undo
 *
 * The purpose of this undo is to invalidate any pages that storage
 * was added for. This still needs to run during recovery since
 * recovery itself may have brought pages into the cache. For example
 * the insertion of a filename into a directory that caused the
 * directory to grow.  If the system crashed before this transaction
 * was committed: First we would apply all the changes doing image
 * redo. This would make the page that we added visible in the extent
 * map, then we would run the undo routines in the opposite order that
 * they were done. So the filename would first be removed from the
 * directory page. This would cause the directory page to be brought
 * into the cache. This undo would run and next we would undo the
 * storage that was added to this directory by running the mcell and
 * extent map undo routines. Finally we would reload the extent maps
 * from disk. 
 * If we did not run this undo in the above example we would leave
 * a dirty page in the cache that does not have any storage to back
 * it. At some point a flush would be attempted and we would 
 * panic because there was no backing store.
 * 
 * It should also be noted that at this point and time none of the
 * storage has been undone. In order for this routine to even be
 * called we have successfully added storage and are about to undo
 * it. So we know that if there are pages in the cache that are backed
 * by this storage, it is safe for a thread to flush them out since
 * the storage and extent maps currently in place are complete.
 * The migStg_lk is keeping any thread from looking these pages up
 * in the cache and actually seeing the data but a flush can come
 * in. By invalidating these pages now before we actually undo the
 * underlying storage, we are insuring that no flush can happen
 * once the storage is removed.
 *
 * See the stg_add_stg() function header NOTE concerning sub-transactions.
 *
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
    addStgUndoRecT undoRec;
    bsInMemXtntMapT *xtntMap;
    bsInMemXtntT *xtnts;
    int bfset_open = 0;              /* true if bitfile set was opened here */
    struct vnode *nullvp = NULL;

    /* This code used to just return when the domain was not marked
     * active. This is a problem since a directory could have run a
     * prior undo routine (i.e. fs_insert_undo) which causes the
     * in-mem extent maps to be read in. If this is the case then it
     * is imperative that we fix them since shortly after this the on-disk
     * MCELLS will be changed and we will have a mismatch. So ditch
     * the pages.
     */

    domain = ftxH.dmnP;

    bcopy(address, &undoRec, sizeof(addStgUndoRecT));

    if ( !BS_UID_EQL(undoRec.setId.domainId, domain->domainId) ) {
        if ( (domain->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRec.setId.domainId, domain->dualMountId)) ) {
            undoRec.setId.domainId = domain->domainId;
        }
        else {
            ADVFS_SAD0("add_stg_undo: domainId mismatch");
        }
    }

    if (domain->state != BFD_ACTIVATED) {
        /* We are in recovery, must open the bitfile set.  Remember that we
         * must close this on the way out.
         */
        sts = bfs_open(&bfSetp, undoRec.setId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"add_stg_undo rbf_bfs_access error");
            goto HANDLE_EXCEPTION;
        }
        bfset_open = 1;
    } else {
        /* Just lookup the bitfile set handle.  */
        bfSetp = bfs_lookup( undoRec.setId );
        MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    }

    sts = bs_access (&bfap, 
                undoRec.tag, 
                bfSetp, 
                ftxH, 
                BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX, 
                &nullvp);
    if (sts != EOK) {
        if (sts != ENO_SUCH_TAG) {
            domain_panic(ftxH.dmnP,"add_stg_undo bs_access error %d",sts);
        }
        /* else We must allow for a failed access on a file that was
         * removed.  A file could have been deleted but held open and
         * storage added to it. At recovery we will remove the file
         * and then attempt to run this undo routine. The storage has
         * all been cleaned up the purpose of this undo routine is
         * make sure there are no pages for the added storage still in
         * the cache. Since the file is gone this undo can be skipped.
         */
        goto HANDLE_EXCEPTION;
    }


    ADVFS_ACCESS_LOGIT_2( bfap, "add_stg_undo: opened access struct",
                undoRec.asurFobOffset * ADVFS_FOB_SZ,
                undoRec.asurFobCnt * ADVFS_FOB_SZ );

    if ( undoRec.asurFobCnt) {
        off_t  offset;
        size_t size;

        offset = undoRec.asurFobOffset * ADVFS_FOB_SZ; 
        size   = undoRec.asurFobCnt    * ADVFS_FOB_SZ;
        MS_SMP_ASSERT( offset % NBPG == 0 &&
                       size   % NBPG == 0 &&
                       size          != 0 );

        (void)fcache_vn_invalidate(&bfap->bfVnode, 
                                   offset,
                                   size,
                                   NULL, 
                                   (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL);
    }

    sts = bs_close(bfap, MSFS_CLOSE_NONE);
    if (sts != EOK) {
      domain_panic(ftxH.dmnP,"add_stg_undo --- bs_close failed ---");
    }

HANDLE_EXCEPTION:
    if ( bfset_open ) {
        sts = bs_bfs_close (bfSetp, ftxH, BFS_OP_DEF);
        if (sts != EOK) {
          domain_panic(ftxH.dmnP,"add_stg_undo --- bs_bfs_close failed ---");
        }
    }

    return;

}  /* end add_stg_undo */


/*
 * Root done function for FTA_ADVFS_THRESHOLD_EVENT agent
 */
void
advfs_threshold_rtdn_opx(ftxHT ftxH,
                        int size,
                        void* address)
{
    advfs_ev advfs_event;

    thresholdAlertRtDnRec_t rootDnRec;

    struct timeval tv;
    time_t currentTime;

    domainT *domain;
    bfSetT  *bfSetp;

    statusT sts;
    int recoveryMode_bfs_open = FALSE;

    domain = ftxH.dmnP;

    bcopy (address, &rootDnRec, size);

    tv = get_system_time();

    /* make sure tv_sec size does not change without us knowing */
    MS_SMP_ASSERT(sizeof(tv.tv_sec) == sizeof(currentTime));
    currentTime = tv.tv_sec;

    bzero(&advfs_event, sizeof(advfs_event));

    advfs_event.domain = domain->domainName;

    if (rootDnRec.type == DOMAIN_UPPER) {
        advfs_event.threshold_domain_upper_limit =
            domain->dmnThreshold.threshold_upper_limit;

        sprintf (advfs_event.formatMsg,
                 sizeof(advfs_event.formatMsg),
                 "Upper Threshold crossed in AdvFS filesystem %s",
                 domain->domainName);

        advfs_post_kernel_event(EVENT_FILESYSTEM_THRESHOLD_UPPER_CROSSED,
                                &advfs_event);
        domain->dmnThreshold.time_of_last_upper_event = currentTime;
        return;
    }

    if (rootDnRec.type == DOMAIN_LOWER) {
        advfs_event.threshold_domain_lower_limit =
            domain->dmnThreshold.threshold_lower_limit;

        sprintf (advfs_event.formatMsg,
                 sizeof(advfs_event.formatMsg),
                 "Lower Threshold crossed in AdvFS filesystem %s",
                 domain->domainName);

        advfs_post_kernel_event(EVENT_FILESYSTEM_THRESHOLD_LOWER_CROSSED,
                                &advfs_event);
        domain->dmnThreshold.time_of_last_lower_event = currentTime;
        return;
    }

    if ((rootDnRec.type == FSET_UPPER) || (rootDnRec.type == FSET_LOWER)) {
        if (domain->state != BFD_ACTIVATED) {
            /* We are in recovery, must open the bitfile set.  Remember that we
             * must close this on the way out.
             */
            sts = bfs_open(&bfSetp,
                           rootDnRec.bfSetID,
                           BFS_OP_IGNORE_DEL,
                           ftxH
                          );
            if (sts != EOK) {
                domain_panic(domain,
                             "advfs_threshold_rtdn_opx rbf_bfs_open error"
                            );
                goto exit;
            }
            recoveryMode_bfs_open = TRUE;
        }
        else {
           /* Just lookup the bitfile set handle. */
           bfSetp = bs_bfs_lookup_desc( rootDnRec.bfSetID );
           MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
        }

        advfs_event.fileset  = bfSetp->bfSetName;

        if (rootDnRec.type == FSET_UPPER) {
            advfs_event.threshold_fset_upper_limit =
                bfSetp->fsetThreshold.threshold_upper_limit;

            sprintf (advfs_event.formatMsg,
                     sizeof(advfs_event.formatMsg),
                     "Upper Threshold crossed in AdvFS fileset %s",
                     bfSetp->bfSetName);

            advfs_post_kernel_event(EVENT_FSET_THRESHOLD_UPPER_CROSSED,
                                    &advfs_event);
            bfSetp->fsetThreshold.time_of_last_upper_event = currentTime;
        } else {
            advfs_event.threshold_fset_lower_limit =
                bfSetp->fsetThreshold.threshold_lower_limit;

            sprintf (advfs_event.formatMsg,
                     sizeof(advfs_event.formatMsg),
                     "Lower Threshold crossed in AdvFS fileset %s",
                     bfSetp->bfSetName);

            advfs_post_kernel_event(EVENT_FSET_THRESHOLD_LOWER_CROSSED,
                                    &advfs_event);
            bfSetp->fsetThreshold.time_of_last_lower_event = currentTime;
        }
    }

exit:
    if (recoveryMode_bfs_open) {
        sts = bs_bfs_close (bfSetp, FtxNilFtxH, BFS_OP_DEF);
        if (sts != EOK) {
          domain_panic(ftxH.dmnP,
                       "advfs_threshold_rtdn_opx bs_bfs_close failed");
        }
    }
    return;
}


/*
 * init_bs_stg_opx
 *
 * This function registers the storage code as transaction agents.
 */

void
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
                             NULL,  /* undo */ 
                             NULL   /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1("init_bs_stg_opx(1):register failure", (long)BSERRMSG(sts));
    }

    sts = ftx_register_agent(
                             FTA_ADVFS_THRESHOLD_EVENT,
                             NULL,  /* undo */
                             &advfs_threshold_rtdn_opx   /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1("init_bs_stg_opx(2):register failure", (long)BSERRMSG(sts));
    }

} /* end init_bs_stg_opx */


/* 
 * Display messages and post EVM events when storage is gone in this domain.
 */
static void
advfs_bs_post_no_storage_msg(struct bfAccess *bfap)
{
    int set = 0;
    struct vnode *vp = &bfap->bfVnode;
    advfs_ev *advfs_event;
    int32_t fs_full_threshold = 3600; /* seconds */

#ifdef Tru64_to_HPUX_Port_Comments
/*
 *                         ---> Ifdefed out for second pass compile work.
 *                              will need to be addressed later.
 */
    /* Error to controlling terminal. */
    if (!AT_INTR_LVL() && !SLOCK_COUNT) {
        uprintf("\n%s: write failed, file system is full\n",
                VTOFSN(vp)->f_mntonname);
    }
#endif /* Tru64_to_HPUX_Port_Comments */

    /*
     * Post EVM event and write to syslog/console only
     * once every fs_full_threshold.
     */
    ADVMTX_DOMAIN_LOCK(&(bfap->dmnP->dmnMutex));
    if ((bfap->dmnP->fs_full_time == 0) ||
        (time.tv_sec - bfap->dmnP->fs_full_time >= fs_full_threshold)) {
        bfap->dmnP->fs_full_time = time.tv_sec;
        set=1;
    }
    ADVMTX_DOMAIN_UNLOCK(&(bfap->dmnP->dmnMutex));

    if (set) {
        printf("%s: file system full\n", VTOFSN(vp)->f_mntonname);

        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = bfap->dmnP->domainName;
#ifdef Tru64_to_HPUX_Port_Comments
/*
 *                         ---> Ifdefed out for second pass compile work.
 *                              Will need to be addressed later.
 */
        advfs_post_kernel_event(EVENT_FDMN_FULL, advfs_event);
#endif /* Tru64_to_HPUX_Port_Comments */
        ms_free(advfs_event);
    }
    return;
}


/*
 * advfs_bs_add_stg
 *
 * Add storage to a file.  This is the highest-level entry point
 * for adding storage to non-reserved files.
 *
 * Note: the file_lock must be exclusively locked upon entry, and the
 * function returns with this lock held.  
 * The migStgLk must be held for read on entry.
 */

/* The following started out at 2048 and was reduced to 128. We would
 * like to make it bigger but that may cause problems with CFS.  The
 * smaller value interacts badly with AdvfsNotPickyBlkCnt in bs_sbm.c:
 * if it is smaller than AdvfsNotPickyBlkCnt, the result is spurious
 * fragmentation of large files. It should be greater than
 * AdvfsNotPickyBlkCnt. The downside is that there is increased CPU
 * usage in the allocator, but it amounted to no more than noise in
 * the regression tests. Since CFS won't let us make the
 * preallocation bigger for now, the value of AdvfsNotPickyBlkCnt was
 * reduced to 64 blocks in bs_sbm.c.
 */
#define MAX_PREALLOC_FOBS 128

/*
 * Define these so that they can be adjusted, if necessary, 
 * through the kernel debugger on a live system.
 */
uint64_t AdvfsMaxAllocFobCnt = MAX_ALLOC_FOB_CNT;
uint64_t AdvfsMaxPreallocFobs = MAX_PREALLOC_FOBS;

#ifdef ADVFS_SMP_ASSERT
    int ForceStgAddFailure = 0;
#endif /* ADVFS_SMP_ASSERT */

statusT
advfs_bs_add_stg(
    struct bfAccess *bfap,           /* in - file's access structure */
    off_t first_byte_to_add,         /* in - offset of first byte to add */
    size_t *bytes_to_add,            /* in/out - bytes to add/bytes added */
    size_t file_size,                /* in - bytes in file */
    struct ucred *cred,              /* in - credentials of caller */
    struct advfs_pvt_param *priv_paramp,/* in - AdvFS-specific information */
    ftxHT parent_ftx,                /* in - parent transaction */
    ftxHT *ftx_handlep,              /* ou - parent transaction handle */
    uint32_t *delCnt,             /* out - delCnt of storage to free */
    void     **delList            /* out - delList of storage to free */
    )
{
    statusT error = EOK;             /* Internal error */
    ftxHT ftxH;                      /* Transaction handle */
    int ftx_started = FALSE,         /* TRUE if we started a transaction */
        locked_migstg_here = FALSE;  /* TRUE if migStgLk is locked */
    uint64_t file_alloc_unit_size,   /* Size in bytes of FAU */
             alloc_unit_overrun,     /* File size modulo FAU size */
             bytes_to_end_of_next_alloc_unit, /* from end of file */
             fobs_needed,            /* FOBs needed to fulfill request */
             fobs_to_add,            /* May include preallocation */
             fobs_added,             /* FOBs actually added to file */
             fobs_prealloc,          /* FOBS to add from preallocation calc. */
             fobs_charged,           /* FOBs charged to user's quotas */
             first_fob_to_add;       /* First FOB to allocate */
    off_t max_offset_in_fobs;        /* Largest possible FOB in AdvFS */


    /*
     * The migStgLk should always be held coming into this routine.  The
     * storage needs to be protected until it is zeroed and accessible by
     * migrate.
     */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->xtnts.migStgLk, RWL_READLOCKED));

    file_alloc_unit_size = (ADVFS_FILE_HAS_SMALL_AU(bfap) && 
                            first_byte_to_add >= VM_PAGE_SZ) ?
                            VM_PAGE_SZ :
                            bfap->bfPageSz * ADVFS_FOB_SZ;

    /*
     * The following assertion is here only to confirm our understanding
     * of what this function will be seeing as inputs.  It should not
     * be interpreted to mean that the code below would fail somehow if
     * this assertions failed.  It may be that the code below would work 
     * just fine with non-aligned storage offsets and sizes.
     */
    MS_SMP_ASSERT(!(first_byte_to_add % file_alloc_unit_size));

    *bytes_to_add = roundup(*bytes_to_add, file_alloc_unit_size);
    /* 
     * Check that the preallocation contraints are not back in 
     * the bad interaction range with the SBM.
     */
    MS_SMP_ASSERT(AdvfsNotPickyBlkCnt < 
                  AdvfsMaxPreallocFobs * ADVFS_FOBS_PER_DEV_BSIZE);

    /*
     * If we have already preallocated storage for the file and we have
     * now skipped past that storage to write to a higher offset, deallocate
     * that preallocated storage.  We'll only check this if we have private
     * params.  That is because this indicates a write().  A memory-mapped
     * update will never extend the file and only a file extension could
     * cause the scenario where we'd want to deallocate preallocated
     * storage.  We will only make this check on the first call to 
     * advfs_bs_add_stg() from a write() request.
     */
                    
    if ((priv_paramp) && 
        (priv_paramp->app_starting_offset > bfap->file_size) &&
        (first_byte_to_add <= priv_paramp->app_starting_offset) &&
        (!ADVFS_SMALL_FILE(bfap))) {


        alloc_unit_overrun = bfap->file_size % file_alloc_unit_size;
        bytes_to_end_of_next_alloc_unit = (alloc_unit_overrun) ?  
            (file_alloc_unit_size - alloc_unit_overrun) : 0;

        if ((priv_paramp->app_starting_offset > 
            bfap->file_size + bytes_to_end_of_next_alloc_unit) &&
            (fs_trunc_test(&bfap->bfVnode, (int64_t)TRUE))) {

            *delCnt = 0;
            *delList = NULL;
    
#ifdef ADVFS_SMP_ASSERT
            if (bfap->dioCnt) {
                /* A Direct I/O should have acquired an active range before
                 * advfs_bs_add_stg() was called, and that range should
                 * include that potential pre-allocated storage we are about
                 * to release.  The following asserts make sure we are
                 * properly protecting that storage from nasty things like
                 * migrate.
                 */
                MS_SMP_ASSERT(priv_paramp->arp);
                MS_SMP_ASSERT(priv_paramp->arp->arDebug);
                MS_SMP_ASSERT(priv_paramp->arp->arStartFob <= 
                    ADVFS_OFFSET_TO_FOB_DOWN(
                        bfap->file_size + bytes_to_end_of_next_alloc_unit )
                             );
                /* If desired, this assert could be scalled back to make
                 * sure that arEndFob covers at least the last FOB to be
                 * released, but since the advfs_getpage() code is currently
                 * using ~0L for any file extension, that is what is being
                 * asserted here.  These asserts could also be moved to
                 * after the truncation if it would make it easier to figure
                 * out where the last released FOB was.
		 */
                MS_SMP_ASSERT(priv_paramp->arp->arEndFob == ~0UL);
            }
#endif /* ADVFS_SMP_ASSERT */
    
            /* No need to take the quota locks here because
             * the quota entry already exists. It's only when
             * the file is created or the quota is explicitly
             * set, that storage may be added to the quota file
             *
             * The potential danger path is
             * bf_setup_truncation --> stg_remove_stg_start -->
             * fs_quota_blks --> dqsync --> rbf_add_stg
             * but it is tame in this case.
             */
    
            if (error = FTX_START_N(FTA_FS_WRITE_TRUNC, &ftxH,
                                    parent_ftx, bfap->dmnP)) {
    
                goto _error_exit;
            }
            ftx_started = TRUE;

    
            error = bf_setup_truncation(bfap, ftxH, 
                    (int64_t)priv_paramp->app_flags, delList, delCnt);

            /* This shouldn't happen but catch it in debug if it does */
            MS_SMP_ASSERT(error == EOK);

            bfap->trunc = FALSE;
    
            /*
             * This was previously an ftx_done_fs which forces a
             * synchronous log flush.  It is believed that this is
             * unnecessary and inefficient.  
             *
             * This transaction does not need to be undone since it is just
             * preallocated storage.  
             */
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_260);
            ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_261);
            ftx_done_n (ftxH, FTA_FS_WRITE_TRUNC);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_262);
            ftx_started = FALSE;
    
    
     
            if( *delCnt )  {
                if (!FTX_EQ( parent_ftx, FtxNilFtxH ) ) {
                    /*
                     * We are in a transaction context so we can't call
                     * stg_remove_stg_finish.  Return the delCnt and delList
                     * to be processed when the transaction is completed.
                     */
                    SNAP_STATS( bs_add_stg_returning_delList ); 
                } else {
                    stg_remove_stg_finish (bfap->dmnP, *delCnt, *delList);
                }
            }
        } 
    } /* Truncate preallocated storage, if necessary */

    /*
     * Figure out how many FOBs to allocate to the file.  This amount
     * is one of the following:
     *          - the number of FOBs needed
     *          - an increasing number up to AdvfsMaxPreallocFobs
     *            based on FOBs being written and sequential access.
     */

    first_fob_to_add = ADVFS_OFFSET_TO_FOB_DOWN(first_byte_to_add);

    fobs_needed = howmany(*bytes_to_add, ADVFS_FOB_SZ);

    /*
     * Limit the amount of storage added here to something reasonably
     * small.  This is done to prevent log half full panics, which are
     * panics caused by unbounded transactions that fill up half the
     * log.  The value of AdvfsMaxAllocFobCnt could scale with the size
     * of the log file and the number of transaction slots in the future
     * to reduce fragmentation of large files.
     */
    if (fobs_needed > AdvfsMaxAllocFobCnt) {
        fobs_needed = AdvfsMaxAllocFobCnt;
    }

    fobs_to_add = fobs_needed;

    /* Try to preallocate storage if it makes sense.  DirectI/O does not permit 
     * preallocation since those pages will not be properly initialized.  
     * NFS server threads do not preallocate for performance.  And mmapping
     * threads currently do not preallocate storage since they cannot
     * extend a file and the preallocation today is limited to storage
     * added at the end of the file.  In the future, if storage
     * preallocation is made more general, mmappers writing into holes in a
     * file might preallocate storage.  We test whether a thread is an
     * mmapper by looking at whether priv_paramp is NULL (mmapper) or not.
     */
    if (priv_paramp &&                           /* Not an mmapper */
        !(bfap->dioCnt) &&                       /* Not using direct I/O */
        (!NFS_SERVER) &&                         /* Not NFS server thread */
        (first_byte_to_add >= file_size) &&      /* Extending the file */
        (first_byte_to_add >=
         4 * file_alloc_unit_size) &&            /* Already wrote 4 AU's */
        (first_fob_to_add == 
         (bfap->bfaLastWrittenFob + 1))) {        /* Writing sequentially */

        /*
         * If seeing sequential writes, ramp slowly 
         * up to maximum preallocation.  In keeping with the Tru64 policy,
         * we start trying to preallocate when the file reaches eight
         * allocation units in size.  We top out at allocations of 128k.
         * Calculate how many FOBs we would allocate if we were to follow
         * the preallocation policy.
         */
        fobs_prealloc = MIN((first_fob_to_add / 4), AdvfsMaxPreallocFobs);

        /*
         * If this is a file with a small allocation unit and we are 
         * considering preallocation, we must be beyond the first VM_PAGE_SZ
         * bytes of the file.  In that case, treat the file as if it had
         * an allocation unit of VM_PAGE_SZ bytes and only consider 
         * preallocating storage in multiples of VM_PAGE_SZ bytes.
         */
        fobs_prealloc = ADVFS_FILE_HAS_SMALL_AU(bfap) ?
                        rounddown(fobs_prealloc, (VM_PAGE_SZ / ADVFS_FOB_SZ)) :
                        ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, fobs_prealloc);

#if 0
        /* CFS objected to any large preallocation
           increase, because it might tickle a problem in CFS
           that would be a standards violation, so the max preallocation
           value was left at 16 and the following code is currently
           commented out. We may revisit it later so I am leaving
           it in here. */
        /*
         * CFS objected that the increased preallocation (from a
         * max of 16 pages to a max of 256 pages) might lead to
         * spurious ENOSPC conditions much more often. So we take
         * some pains to reduce that possibility: if the domain is
         * more than 90% full, reduce the preallocation to a max
         * of 256 / 16 = 16 pages; if more than 80% full, reduce it
         * to 256 / 8 = 32 pages, etc. In no case should the max preallocation
         * fall below the AdvfsNotPickyBlkCnt (= 32 pages = 512 blocks).
         */

        if (dmnP->freeBlks < dmnP->totalBlks / 10) /* < 10% free */
            fobs_prealloc = MIN(fobs_prealloc, AdvfsMaxPreallocFobs / 16);
        else if (dmnP->freeBlks < dmnP->totalBlks * 2 / 10) /* < 20% free */
            fobs_prealloc = MIN(fobs_prealloc, AdvfsMaxPreallocFobs / 8);
        else if (dmnP->freeBlks < dmnP->totalBlks * 3 / 10) /* < 30% free */
            fobs_prealloc = MIN(fobs_prealloc, AdvfsMaxPreallocFobs / 4);
        else if (dmnP->freeBlks < dmnP->totalBlks * 4 / 10) /* < 40% free */
            fobs_prealloc = MIN(fobs_prealloc, AdvfsMaxPreallocFobs / 2);
#endif

        /*
         * If the preallocation policy would have us allocate more storage
         * than was requested, adjust the storage request size.
         */
        if (fobs_prealloc > fobs_to_add ) {
            fobs_to_add = fobs_prealloc;
        }
    }

    /* 
     * Make sure that we don't overextend the file beyond AdvFS limits.
     */
    max_offset_in_fobs = ADVFS_OFFSET_TO_FOB_DOWN(ADVFS_MAX_OFFSET);
    fobs_to_add = MIN(fobs_to_add, max_offset_in_fobs - first_fob_to_add + 1);


    /*
     * Start a root transaction which will bind together the allocation
     * of storage and the update of quotas.
     */
    if (error = FTX_START_N(FTA_FS_WRITE_ADD_STG_V1, &ftxH, parent_ftx,
                            bfap->dmnP)) {
        goto _error_exit;
    }
    ftx_started = TRUE;


    /* No need to lock the quota locks. Storage is only
     * added to them at file creation, chown and explicit
     * quota setting.
     *
     * The potential danger path is 
     * chk_blk_quota --> dqsync --> rbf_add_stg
     * but it is tame in this case.
     */


    /*
     * Make sure this storage allocation won't put user over his quotas.
     */
    fobs_charged = fobs_to_add;

    if (fobs_needed >= fobs_to_add) {
	    /*
	     * We are not asking for any more blocks that we need, i.e.
	     * we are not preallocating extra storage.
	     * Therefore, a single call to check quotas is sufficient.
	     * If we cannot get this amount, return a quota error.
	     */
            if (error = chk_blk_quota(&bfap->bfVnode,
                               (int64_t) QUOTABLKS(fobs_to_add * ADVFS_FOB_SZ),
                               cred, 0, ftxH )) {
                goto _error_exit;
	    }
    } else {
	    /*
	     * We are asking for more blocks that we really need, i.e.
	     * we are trying to preallocate extra storage.
	     * First, check to see if quotas will allow the preallocation.
	     * Must pass PREALLOC flag on this first call to get special
	     * behavior needed for CFS.
	     * If the preallocation would exceed quotas, back off to amount
	     * we actually need and see if quotas will allow that.
	     * If not, return quota error.
	     * Note that quota code assumes that if a PREALLOC call exceeds
	     * quotas, there will ALWAYS be a followup call for the
	     * amount of storage actually needed.
	     */
	    if (error = chk_blk_quota(&bfap->bfVnode, 
				      (int64_t) QUOTABLKS(fobs_to_add * ADVFS_FOB_SZ),
				      cred, PREALLOC, ftxH )) {
		    /*
		     * Couldn't deduct quotas for this user by fobs_to_add.  If we 
		     * were trying to preallocate storage, scale back to see if 
		     * he can handle the minimum we really need.
		     */
		    fobs_charged = fobs_to_add = fobs_needed;
		    if (error = chk_blk_quota(&bfap->bfVnode,
					      (int64_t) QUOTABLKS(fobs_to_add * ADVFS_FOB_SZ),
					      cred, 0, ftxH )) {
			    goto _error_exit;
		    }
	    }
    }

    error = stg_add_stg(ftxH, bfap, first_fob_to_add, fobs_to_add, 
                        file_alloc_unit_size / ADVFS_FOB_SZ, TRUE, &fobs_added);

    if (error == ENO_MORE_BLKS) {

        /*
         * If we asked for more storage than we need right now,
         * scale back the request to what we absolutely need
         * right now and try again.
         */
        if (fobs_needed < fobs_to_add) {
            fobs_to_add = fobs_needed;
            error = stg_add_stg(ftxH, bfap, first_fob_to_add, fobs_to_add, 
                                file_alloc_unit_size / ADVFS_FOB_SZ, 
                                TRUE, &fobs_added);
        }
    }

    if (fobs_added < fobs_charged) {
        /*
         * We didn't allocate as much storage as we thought we would.
         * Back out the storage not allocated from the user's quota.
         */
        (void) chk_blk_quota(&bfap->bfVnode,
                             - (int64_t)QUOTABLKS((fobs_charged - fobs_added) *
                                                  ADVFS_FOB_SZ),
                             cred,
                             0,
                             ftxH );
    }

    if (error) {
        goto _error_exit;
    }

#ifdef ADVFS_SMP_ASSERT
    /* This is a special failure point for forcing code thru the
     * add_stg_undo() routine.  Used in the SMP coverage tests for
     * bfAccess struct manipulation.
     */
    if (ForceStgAddFailure) {
        error = ENOSPC;
        goto _error_exit;
    }
#endif /* ADVFS_SMP_ASSERT */

    /*
     * If the fileset supports object safety, return transaction handle 
     * to the caller, who will commit or rollback the transaction after
     * zero-filling the storage to disk.  Otherwise, commit the transaction
     * here.
     */
    if (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY) {
        *ftx_handlep = ftxH;
    }
    else {
        /*
         * If a parent transaction was passed in, it was a COW transaction.
         * We don't want to fail adding storage to the parent because of a
         * COW failure, so make sure to not undo this FTX.
         */
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_263);
        ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_264);
        ftx_done_n( ftxH, FTA_FS_WRITE_ADD_STG_V1 );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_265);
        ftx_started = FALSE;
    }

    *bytes_to_add = ADVFS_FOB_TO_OFFSET(fobs_added);
    MS_SMP_ASSERT( (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY) ||
                (ftx_started == FALSE) );

    return EOK;

_error_exit:

    if (ftx_started) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_456);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_457);
        ftx_started = FALSE;
    }
    *ftx_handlep = FtxNilFtxH;

    if (BSERRMAP(error) == ENOSPC) {
        advfs_bs_post_no_storage_msg(bfap);
    }

    MS_SMP_ASSERT( (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY) ||
                (ftx_started == FALSE) );
    return BSERRMAP(error);
}


/*
 * rbf_add_stg
 *
 * This function allocates storage starting at the specified fob offset
 * for the specified number of fobs .    If the requested range
 * overlaps other allocations, no storage is allocated and an error status
 * is returned.  The bitfile is specified by the access handle.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 */

statusT
rbf_add_stg(
            bfAccessT *bfap,            /* in */
            bf_fob_t fob_offset,        /* in */
            bf_fob_t fob_cnt,           /* in */
            ftxHT parent_ftx,           /* in */
            stg_flags_t stg_flags       /* in */
            )
{
    bf_fob_t alloc_fob_cnt,
             minimum_contiguous_fobs;
    statusT sts;
    int dummy;

    /* 
     * The migStgLk MUST be held when calling into this routine if there is
     * any potential for snapshots existing.  Additionally, the lock must be
     * held to synchronize with migrate.  Migrate is the
     * only caller that would ever hold the migStgLk for write and migrate
     * shouldn't use this interface.   As a result, if the lock is held (the
     * STG_MIG_STG_HELD flag is set) it should be held for read.  If
     * snapshots exist, the lock may be dropped while processing snapshots
     * but will be reacquired for read before calling stg_add_stg.  
     */
    if (stg_flags & STG_MIG_STG_HELD) {
      MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL(&bfap->xtnts.migStgLk, RWL_READLOCKED));
    }

    if (VD_HTOP(bfap->primMCId.volume, bfap->dmnP)->bmtp == bfap) {
        /*
         * Can't add storage to the bmt via this interface.
         */
        return ENOT_SUPPORTED;
    }


    /*
     * See if any holes need to be COWed.
     */
    if ( IS_COWABLE_METADATA( bfap ) && HAS_SNAPSHOT_CHILD( bfap ) &&
            !(stg_flags & STG_NO_COW) ) {
        extent_blk_desc_t*   parent_hole= NULL;
        uint64_t             xtnt_cnt = 0;

        ACCESS_STATS( rbf_add_stg_metadata_stg_add_during_snap );

        /* 
         * A nil transaction handle opens a race with snapset creation. 
         */
        MS_SMP_ASSERT( parent_ftx.hndl != FtxNilFtxH.hndl ); 

        if (bfap->bfaFlags & BFA_SNAP_CHANGE) {
            sts = advfs_access_snap_children( bfap, parent_ftx, &dummy );
            /* If sts != EOK, the domain should be paniced. Generally, 
             * a failure to open the child should just mark that child 
             * out of sync and return EOK. */
            if (sts != EOK) {
                MS_SMP_ASSERT( bfap->dmnP->dmn_panic ); 
                return sts;
            }
        }

        /* 
         * Get the sparseness maps for the given file.  External locking
         * should be protecting multiple threads from adding storage to the
         * same page of a metadata file at the same time so the extent maps
         * shouldn't change over the range we are concerned with. The
         * round_type RND_ENTIRE_HOLE is used to COW the entire hole rather
         * than portions of it.  Additionally, we should only get back one
         * hole, we shouldn't have a case where we are adding overlapping
         * storage.
         */
        {
            off_t       start_off = ADVFS_FOB_TO_OFFSET(fob_offset);
            sts = advfs_get_blkmap_in_range( bfap,
                                        bfap->xtnts.xtntMap,
                                        &start_off,
                                        ADVFS_FOB_TO_OFFSET(fob_cnt),
                                        &parent_hole,
                                        &xtnt_cnt,
                                        RND_ENTIRE_HOLE,
                                        EXB_ONLY_HOLES,
                                        XTNT_WAIT_OK);
        }
        if (sts != EOK) {
            return sts;
        }

        /* Make sure we actually have storage to add and that it was only
         * one extent. */
        MS_SMP_ASSERT( xtnt_cnt == 1);

        ADVRWL_BFAP_SNAP_READ_LOCK( bfap );
        /* Walk each child and make the necessary holes */
        {
            bfAccessT*          child_bfap = bfap->bfaFirstChildSnap;
            extent_blk_desc_t*  cur_hole = NULL; 
            extent_blk_desc_t*  sparseness_maps = NULL;
            off_t               end_offset = 0;
            while (child_bfap) {
                ADVRWL_BFAP_SNAP_WRITE_LOCK( child_bfap );

                /*
                 * Truncate the extent descriptor to not extend beyond
                 * the COWable range of child_bfap.  This means chopping off
                 * the parent's hole at the child's orig_file_size.
                 */
                if (parent_hole->ebd_offset + parent_hole->ebd_byte_cnt >=
                        child_bfap->bfa_orig_file_size) {
                    end_offset = roundup(child_bfap->bfa_orig_file_size,
                            child_bfap->bfPageSz * ADVFS_FOB_SZ);
                } else {
                    end_offset = parent_hole->ebd_offset + parent_hole->ebd_byte_cnt;
                }

                /*
                 * Get the extent maps for the child that overlap the entire
                 * hole in the parent.  This may be multiple extents in the
                 * child for a sparse metadata file like an index.
                 */


                {
                    off_t       start_off = ADVFS_FOB_TO_OFFSET(fob_offset);
                    sts = advfs_get_blkmap_in_range( child_bfap,
                                        child_bfap->xtnts.xtntMap,
                                        &start_off,
                                        end_offset - start_off,
                                        &sparseness_maps,
                                        &xtnt_cnt,
                                        RND_NONE,
                                        EXB_ONLY_HOLES|EXB_DO_NOT_INHERIT,
                                        XTNT_WAIT_OK);

                    if (sts != EOK) {
                        /* Mark the child as out of sync and continue. */
                        sts = advfs_snap_out_of_sync( child_bfap->tag,
                                                child_bfap, 
                                                child_bfap->bfSetp,
                                                parent_ftx );
                        if ( sts != EOK ) {
                            MS_SMP_ASSERT( bfap->dmnP->dmn_panic );
                            advfs_free_blkmaps( &parent_hole );
                            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
                            ADVRWL_BFAP_SNAP_UNLOCK( child_bfap );
                            return sts;
                        }
                    }
                }


                /*
                 * Now loop for each child hole (in the range of
                 * parent_hole) and add a COWed hole.
                 */
                for (cur_hole = sparseness_maps; 
                     cur_hole != NULL;
                     cur_hole = cur_hole->ebd_next_desc) {

                    /*
                     * Make sure we need to do work on this particular
                     * snapshot and don't add a COWed hole beyond the
                     * original end of file.
                     */
                    MS_SMP_ASSERT( child_bfap->bfa_orig_file_size != ADVFS_ROOT_SNAPSHOT );
                    MS_SMP_ASSERT( child_bfap->bfa_orig_file_size >= cur_hole->ebd_offset );

                    /* Insert the COWED_HOLE into the child.   If the insert
                     * fails, then the child_bfap should be out of sync. */
                    sts = advfs_make_cow_hole( child_bfap,
                            ADVFS_OFFSET_TO_FOB_UP(cur_hole->ebd_offset),
                            ADVFS_OFFSET_TO_FOB_UP(cur_hole->ebd_byte_cnt),
                            parent_ftx);
                    if (sts != EOK) {
                        MS_SMP_ASSERT( child_bfap->bfaFlags & BFA_OUT_OF_SYNC );
                        cur_hole = NULL;
                    }
                }


                ADVRWL_BFAP_SNAP_UNLOCK( child_bfap );
                advfs_free_blkmaps( &sparseness_maps );
                child_bfap = child_bfap->bfaNextSiblingSnap;
            }
        }

        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        advfs_free_blkmaps( &parent_hole);
        
        /* 
         * Now the migStg_lk is held for read again so any migrates will be
         * blocked while storage is added.
         */
    }


    /*
     * Generally, this path adds storage to metadata files.  But it also
     * adds storage to snapshots of user files.  If the latter is occurring,
     * make sure the special small-files allocation unit rules are followed.
     */
    minimum_contiguous_fobs = (ADVFS_FILE_HAS_SMALL_AU(bfap) && 
                               ((fob_offset + fob_cnt) >= 
                                (VM_PAGE_SZ / ADVFS_FOB_SZ)) ?
                               VM_PAGE_SZ / ADVFS_FOB_SZ :
                               bfap->bfPageSz);


    return stg_add_stg (parent_ftx,
                        bfap,
                        fob_offset,
                        fob_cnt,
                        minimum_contiguous_fobs,
                        FALSE,
                        &alloc_fob_cnt
                        );
} /* end rbf_add_stg */


#ifdef DEBUG
static int stg_verbose = 0;
#endif

/*
 * stg_add_stg
 *
 * This function allocates storage starting at the specified fob offset
 * for the specified number of fobs.  The bitfile is specified by an
 * access pointer.
 *
 * The storage is allocated if the entire request is satisfied.  Otherwise,
 * no storage is allocated.
 *
 * This function does not guarantee how many extents will be needed to
 * add the specified storage to the bitfile.
 *
 * NOTE: if the add storage operation is a sub-transaction::  If the operation
 * completes successfully, the new storage is exposed before the caller's
 * root transaction completes.  This is because the locks guarding the on-disk
 * and in-memory extent maps are released before returning to the caller.
 * Threads that add storage and threads that read/write the bitfile must
 * synchronize with each other so that the root transaction completes
 * successfully or fails without other threads seeing the new storage.
 */

statusT
stg_add_stg (
             ftxHT parentFtx,             /* in */
             bfAccessT *bfap,             /* in */
             bf_fob_t fob_offset,    /* in */
             bf_fob_t fob_cnt,       /* in */
             bf_fob_t minimum_contiguous_fobs, /* in */
             int allowOverlapFlag,        /* in */
             bf_fob_t *alloc_fob_cnt /* out */
             )
{
    int failFtxFlag = 0;
    ftxHT ftx;
    bf_fob_t tmp_fob_cnt= 0;
    int retryFlag;
    statusT sts;
    addStgUndoRecT undoRec;
    bsInMemXtntT *xtnts;

#ifdef DEBUG
    if (stg_verbose) {
        printf("stg_add_stg: fob_offset=0x%x fob_cnt=0x%x\n",
               fob_offset, fob_cnt);
    }
#endif

    if (fob_cnt == 0) {
        /*
         * No storage was requested.
         */
        *alloc_fob_cnt= 0;

        return EOK;
    }

    xtnts = &(bfap->xtnts);

    /*
     * Start a new transaction.  This transaction allocates disk storage and
     * updates the on-disk extent map.  This transaction is necessary because
     * our caller could pass a nil ftx handle.
     */
    retryFlag = 1;
    while (retryFlag != 0) {

        /*
         * This transaction ties all of the "add storage" sub transactions
         * together so that if there is an error, this function calls ftx_fail()
         * to back out the on-disk modifications.  Easy as 1-2-3.
         */

        sts = FTX_START_N(FTA_NULL, &ftx, parentFtx, bfap->dmnP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        failFtxFlag = 1;

        /* This subtransaction is needed to restore the original
         * xtntmaps in case of a failure. This will do nothing if
         * the addition of storage succeeds. This is setup here so
         * that it is the last undo to run if storage is backed
         * out. It will reload the extent maps into memory from
         * the disk. This assumes that ALL on-disk changes have
         * already been backed out.
         */
        {
            ftxHT subftx;
            restoreOrigXtntmapUndoRecT subundoRec;
            
            sts = FTX_START_N(FTA_NULL, &subftx, ftx, bfap->dmnP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            subundoRec.setId = bfap->bfSetp->bfSetId;
            subundoRec.bfTag = bfap->tag;
            /*
             * If it is a bmt or rbmt, fill in the vd field, otherwise fill in the bfap field.
             */
            if (BS_BFTAG_RSVD(bfap->tag) && 
                (BS_BFTAG_RBMT(bfap->tag) || BS_IS_TAG_OF_TYPE(bfap->tag, BFM_BMT))) {
                subundoRec.bfap_or_vd.vdIndex = FETCH_MC_VOLUME(bfap->primMCId);
            } else {
                subundoRec.bfap_or_vd.bfap    = bfap;
            }
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_266);
            ftx_done_u (subftx, 
                        FTA_BS_XTNT_RELOAD_ORIG_XTNTMAP, 
                        sizeof (subundoRec), 
                        &subundoRec);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_267);
        }


        /*
         * Make sure that the in-memory extent maps are valid.
         * Returns with mcellList_lk write-locked.
         */
        sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_UPDATE|XTNT_WAIT_OK);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /* Add this lock to our transaction list so it can be released
         * when the transaction completes.
         */
        FTX_ADD_LOCK(&(bfap->mcellList_lk), ftx);

        /* We no longer need to lock the xtntmaps across this call since
         * we will be reading the extent maps. We only need to hold
         * the lock exclusively when we grow the extent maps. We will
         * only be modifying the update ranges of the xtntmap and we
         * have exclusive access to this range due to the mcellList_lk
         * which is always held during manipulation of the updates.
         * We keep migrate out by holding the MigStgLock and the only
         * other candidates are the log and the root tag dir which
         * have protection via and exclusive ftx.
         * 
         */
    
        if (BS_BFTAG_RSVD (bfap->tag) == 0) {

            sts = add_stg( bfap,
                           fob_offset,
                           fob_cnt,
                           minimum_contiguous_fobs,
                           allowOverlapFlag,
                           xtnts,
                           ftx,
                           TRUE,
                           &tmp_fob_cnt);
            if (sts == EOK) {
                retryFlag = 0;
            } else {
                if (sts == E_VOL_NOT_IN_SVC_CLASS) {
                    /*
                     * We get this deep in the bowels of storage allocation.
                     * It can happen when we a disk is removed from service
                     * while we are allocating storage.
                     */
                    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_458);
                    ftx_fail (ftx);
                    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_459);
                } else {
                    RAISE_EXCEPTION (sts);
                }
            }
        } else {

            sts = add_rsvd_stg( bfap,
                                fob_offset,
                                fob_cnt,
                                xtnts,
                                ftx,
                                &tmp_fob_cnt);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            retryFlag = 0;
        }
    }  /* end while */

    if (tmp_fob_cnt== 0) {
        *alloc_fob_cnt = 0;
        RAISE_EXCEPTION (EOK);
    }

    /*
     * This function can't fail from this point on.  This is because we
     * modify the in-memory extent maps and the changes cannot be backed out.
     */
    ADVRWL_XTNTMAP_WRITE( &(bfap->xtntMap_lk) );
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);
    
    merge_xtnt_maps (xtnts);

    if (BS_BFTAG_REG(bfap->tag) && !(bfap->bfaFlags & BFA_FRAGMENTED) &&
        ss_bfap_fragmented(bfap)) {
        sts = advfs_bs_tagdir_update_tagmap(bfap->tag,
                                            bfap->bfSetp,
                                            BS_TD_FRAGMENTED,
                                            bsNilMCId,
                                            ftx,
                                            TDU_SET_FLAGS);
        if (sts != EOK) {
            ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
            ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
            RAISE_EXCEPTION(sts);
        }
        ss_set_fragmented_tag_page(bfap->tag, bfap->bfSetp);
#ifdef SS_DEBUG
        if (ss_debug) {
            printf("vfast-%s SET fragmented -> tag(%ld)\n",
                       bfap->dmnP->domainName, bfap->tag.tag_num);
        }
#endif /* SS_DEBUG */
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        bfap->bfaFlags |= BFA_FRAGMENTED;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

    xtnts->bimxAllocFobCnt = xtnts->bimxAllocFobCnt + tmp_fob_cnt;

    undoRec.asurNextFob = bfap->bfaNextFob;

    if ((fob_offset + tmp_fob_cnt ) > bfap->bfaNextFob) {
        bfap->bfaNextFob = fob_offset + tmp_fob_cnt;
    }
    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    undoRec.setId = bfap->bfSetp->bfSetId;
    undoRec.tag = bfap->tag;
    undoRec.asurFobOffset = fob_offset;
    undoRec.asurFobCnt = tmp_fob_cnt;

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_268);
    ftx_done_u (ftx, FTA_BS_STG_ADD_V1, sizeof (undoRec), &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_269);

    *alloc_fob_cnt = tmp_fob_cnt;

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_461);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_462);
    }

    return sts;

} /* end stg_add_stg */


statusT
stg_add_rbmt_stg (
                  ftxHT parentFtx,             /* in */
                  bfAccessT *rbmtap,           /* in */
                  bf_fob_t fob_offset,  /* in */
                  bf_fob_t fob_cnt,     /* in */
                  bfMCIdT mcellId,             /* in */
                  bf_fob_t *alloc_fob_cnt /* out */
                  )
{
    bsInMemXtntT *xtnts;
    bsInMemXtntMapT *xtntMap;
    uint32_t mapIndex, i = 0;
    bf_fob_t temp_fob   = 0;
    bf_vd_blk_t temp_vd = 0;
 
    bf_fob_t tmp_fob_cnt = 0;
    bsInMemSubXtntMapT *subXtntMap;
    statusT sts;
    bsAllocHintT alloc_hint = BS_ALLOC_FIT;

    /* Note that the extent maps at this point are guaranteed to be
     * up-to-date since they get set up at disk activation, and are
     * modified only thru rbmt extension which is done under an
     * exclusive transaction. The caller must already hold the
     * mcellList_lk.
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
        /* We are operating on the original, we need to tell
         * imm_extend_xtnt_map to lock the extents when replacing
         * the memory.
         */
        sts = imm_extend_xtnt_map (rbmtap,xtntMap,XTNT_ORIG_XTNTMAP);
        if (sts != EOK) {
            return sts;
        }
    }

    subXtntMap = &(xtntMap->subXtntMap[i]);

    mcellId.volume = rbmtap->primMCId.volume;
    sts = imm_init_sub_xtnt_map( subXtntMap,
                                 fob_offset,
                                 0,  /* fob cnt */
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS);

    if (sts != EOK) {
        return sts;
    }

    subXtntMap->mcellState = ADV_MCS_NEW_MCELL;

    sts = alloc_from_one_disk( rbmtap,
                               fob_offset,
                               fob_cnt,
                               rbmtap->bfPageSz,
                               subXtntMap,
                               &alloc_hint,
                               parentFtx,
                               &temp_fob, /* Not used by this path */
                               &temp_vd,  /* Not used by this path */
                               &tmp_fob_cnt);
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

    /* Not too sure any threads can be reading these fields
     * but just to be safe
     */

    ADVRWL_XTNTMAP_WRITE( &rbmtap->xtntMap_lk );
    ADVRWL_VHAND_XTNTMAP_WRITE(&rbmtap->vhand_xtntMap_lk);
    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->bsxmNextFob = xtntMap->subXtntMap[xtntMap->validCnt - 1].bssxmFobOffset+1;

    xtnts->bimxAllocFobCnt += tmp_fob_cnt;
    temp_fob = rbmtap->bfaNextFob;
    if ((fob_offset + fob_cnt) > rbmtap->bfaNextFob) {
        rbmtap->bfaNextFob = fob_offset+ fob_cnt;
    }
    ADVRWL_VHAND_XTNTMAP_UNLOCK(&rbmtap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK( &rbmtap->xtntMap_lk );

    sts = x_update_ondisk_xtnt_map( rbmtap->dmnP,
                                         rbmtap,
                                         xtntMap,
                                         parentFtx);
    if (sts != EOK) {
        ADVRWL_XTNTMAP_WRITE( &rbmtap->xtntMap_lk );
        ADVRWL_VHAND_XTNTMAP_WRITE(&rbmtap->vhand_xtntMap_lk);
        xtnts->bimxAllocFobCnt -= tmp_fob_cnt;
        rbmtap->bfaNextFob = temp_fob;
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&rbmtap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK( &rbmtap->xtntMap_lk );
        return sts;
    }
    
    *alloc_fob_cnt = tmp_fob_cnt;

    return sts;
}

/*
 * add_stg
 *
 * This function adds storage to a bitfile.
 */

static
statusT
add_stg (
         bfAccessT *bfap,       /* in */
         bf_fob_t fob_offset,  /* in */
         bf_fob_t fob_cnt,     /* in */
         bf_fob_t minimum_contiguous_fobs, /* in */
         int allowOverlapFlag,  /* in */
         bsInMemXtntT *xtnts,   /* in, modified */
         ftxHT parentFtx,       /* in */
         int updateDisk,        /* in */
         bf_fob_t *alloc_fob_cnt /* out */
         )
{
    bf_fob_t hole_fob_cnt;
    uint32_t i;
    int mapIndex;
    statusT sts;
    bf_fob_t total_fob_cnt;
    bsInMemXtntMapT *xtntMap;

    /*  If domain has an upper threshold set */
    if (bfap->dmnP->dmnThreshold.threshold_upper_limit > 0) {

        sts = advfs_bs_check_threshold(bfap, fob_cnt, DOMAIN_UPPER, parentFtx);

       /*
        * Ignore failure in non-debug kernel.  We cannot fail the
        * storage addition because of failing threshold checking.
        */
        MS_SMP_ASSERT(sts==EOK);
    } 

    /*  If fileset has an upper threshold set and it's mounted */
    if ((bfap->bfSetp->fsetThreshold.threshold_upper_limit > 0) &&
        (bfap->bfSetp->fsnp)) {

        sts = advfs_bs_check_threshold(bfap, fob_cnt, FSET_UPPER, parentFtx);

       /*
        * Ignore failure in non-debug kernel.  We cannot fail the
        * storage addition because of failing threshold checking.
        */
        MS_SMP_ASSERT(sts==EOK);
    } 

    if (fob_offset >= bfap->bfaNextFob) {
        hole_fob_cnt = fob_cnt;
    } else {
        /*
         * Add storage in the middle of the bitfile.  Determine if
         * we can add some or all of the storage.
         */
        hole_fob_cnt = imm_get_hole_size (fob_offset, xtnts);
        if (hole_fob_cnt >= fob_cnt) {
            hole_fob_cnt = fob_cnt;
        } else {
            if (allowOverlapFlag == TRUE) {
                if (hole_fob_cnt== 0) {
                    *alloc_fob_cnt= 0;
                    return EOK;
                }
            } else {
                return EALREADY_ALLOCATED;
            }
        }
    }


        xtntMap = xtnts->xtntMap;
        xtntMap->updateStart = xtntMap->cnt;
        sts = alloc_stg( bfap,
                         fob_offset,
                         hole_fob_cnt,
                         minimum_contiguous_fobs,
                         -1,  /* any disk */
                         xtntMap,
                         BS_ALLOC_DEFAULT,
                         parentFtx,
                         updateDisk,
                         &total_fob_cnt);
        if (sts != EOK) {
            return(sts);
        }

    *alloc_fob_cnt = total_fob_cnt;

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
              bf_fob_t fob_offset,          /* in */
              bf_fob_t fob_cnt,             /* in */
              bsInMemXtntT *xtnts,   /* in, modified */
              ftxHT parentFtx,       /* in */
              bf_fob_t *alloc_fob_cnt       /* out */
              )
{
    bf_fob_t tmp_fob_cnt;
    statusT sts;
    bsInMemXtntMapT *xtntMap;

    /*
     * All rsvd bitfile extents must be on the same vd.
     */

    if (fob_offset != bfap->bfaNextFob) {
        ADVFS_SAD0("add_rsvd_stg: sparse reserved bitfile not supported");
    }

    xtntMap = xtnts->xtntMap;
    xtntMap->updateStart = xtntMap->cnt;
    sts = alloc_stg( bfap,
                     fob_offset,
                     fob_cnt,
                     bfap->bfPageSz,
                     (vdIndexT) bfap->primMCId.volume,
                     xtntMap,
                     BS_ALLOC_RSVD,
                     parentFtx,
                     TRUE,
                     &tmp_fob_cnt);
    if (sts == EOK) {
        *alloc_fob_cnt= tmp_fob_cnt;
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
           bf_fob_t fob_offset,     /* in */
           bf_fob_t fob_cnt,        /* in */
           bf_fob_t minimum_contiguous_fobs, /* in */
           vdIndexT bfVdIndex,        /* in */
           bsInMemXtntMapT *xtntMap,  /* in */
           bsAllocHintT alloc_hint,   /* in */
           ftxHT parentFtx,           /* in */
           int updateDisk,            /* in */
           bf_fob_t *alloc_fob_cnt  /* out */
           )
{
    uint32_t descIndex;
    uint32_t mapIndex;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t total_fob_cnt = 0;

    sts = imm_fob_to_sub_xtnt_map (fob_offset, xtntMap, 0, &mapIndex);
    switch (sts) {

      case E_RANGE_NOT_MAPPED:
        /*
         * The page is not mapped by any sub extent maps and its fob offset
         * is after the last fob mapped by the last valid sub extent map.
         *
         * Append the fob after the last fob mapped by the last subextent map.
         */
        sts = alloc_append_stg( bfap,
                                fob_offset,
                                fob_cnt,
                                minimum_contiguous_fobs,
                                bfVdIndex,
                                xtntMap,
                                xtntMap->validCnt - 1,  /* mapIndex */
                                alloc_hint,
                                parentFtx,
                                updateDisk,
                                &total_fob_cnt );
        break;

      case EOK:
        /*
         * The fob is mapped by a sub extent map.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapIndex]);
        sts = imm_fob_to_xtnt( fob_offset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_STG_HOLE */
                                &descIndex);
        switch (sts) {

          case E_STG_HOLE:
            /*
             * The fob is mapped by the sub extent map and the extent
             * descriptor describes a hole.
             */
            sts = alloc_hole_stg( bfap,
                                  fob_offset,
                                  fob_cnt,
                                  minimum_contiguous_fobs,
                                  bfVdIndex,
                                  xtntMap,
                                  mapIndex,
                                  descIndex,
                                  alloc_hint,
                                  parentFtx,
                                  updateDisk,
                                  &total_fob_cnt );
            break;

          case EOK:
            /*
             * The fob is mapped by the sub extent map and the extent
             * descriptor describes an existing extent.
             */
            ADVFS_SAD0("alloc_stg: storage already allocated");
            break;

          default:
            return sts;

        }  /* end switch */

        break;

      default:
        return sts;
    }  /* end switch */

    if (sts == EOK) {
        *alloc_fob_cnt = total_fob_cnt;
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
                  bfAccessT *bfap,          /* in */
                  bf_fob_t fob_offset,      /* in */
                  bf_fob_t fob_cnt,         /* in */
                  bf_fob_t minimum_contiguous_fobs, /* in */
                  vdIndexT bfVdIndex,       /* in */
                  bsInMemXtntMapT *xtntMap, /* in */
                  uint32_t mapIndex,        /* in */
                  bsAllocHintT alloc_hint,  /* in */
                  ftxHT parentFtx,          /* in */
                  int updateDisk,           /* in */
                  bf_fob_t *alloc_fob_cnt   /* out */
                  )
{
    bfSetT *bfSetDesc;
    uint32_t i;
    bf_fob_t tmp_fob_cnt;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;

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
        /* Tell imm_extend_xtnt_map we are working on the in place
         * xtntmap so lock the xtntMap_lk before swapping memory.
         */
        sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);
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

    if (bfVdIndex == (vdIndexT)(-1)) {
        sts = stg_alloc_from_svc_class( bfap,
                                        bfap->reqServices,
                                        bfSetDesc->dirTag,
                                        bfap->tag,
                                        fob_offset,
                                        fob_cnt,
                                        minimum_contiguous_fobs,
                                        xtntMap,
                                        &alloc_hint,
                                        parentFtx,
                                        &tmp_fob_cnt);
    } else {
        sts = stg_alloc_from_one_disk( bfap,
                                       bfVdIndex,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       fob_offset,
                                       fob_cnt,
                                       minimum_contiguous_fobs,
                                       xtntMap,
                                       &alloc_hint,
                                       parentFtx,
                                       &tmp_fob_cnt);
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if (tmp_fob_cnt != fob_cnt) {
        RAISE_EXCEPTION (ENO_MORE_BLKS);
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

    *alloc_fob_cnt= tmp_fob_cnt;

    return sts;

HANDLE_EXCEPTION:
    unload_sub_xtnt_maps (xtntMap);

    return sts;

}  /* end alloc_append_stg */



/*  
 * If there is room in the single, new storage sub extent map,
 * combine extents from both the previous(old) and new(stg) subextent maps
 * into the new storage sub extent map.
 * Old storage is placed at the head of the new extent list in the
 * new(stg) sub extent map.  The old sub extent map's mcell is deleted.
 */ 
static
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
    if( prevOldSubXtntMap->mcellId.volume == newSubXtntMap->mcellId.volume ) {

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
        bsXA[i].bsx_fob_offset = newSubXtntMap->bsXA[i].bsx_fob_offset;
        bsXA[i].bsx_vd_blk  = newSubXtntMap->bsXA[i].bsx_vd_blk;
        /* re-init newSub as contents copied */
        newSubXtntMap->bsXA[i].bsx_fob_offset = 0;
        newSubXtntMap->bsXA[i].bsx_vd_blk = XTNT_TERM;
    }

    /* Write in the extents currently in the prevOldSubXtntMap. */
    newSubXtntMap->bssxmFobOffset = prevOldSubXtntMap->bssxmFobOffset;
    for(i=0; i<prevOldSubXtntMap->cnt; i++) {
        if((i == (prevOldSubXtntMap->cnt-1)) &&  /* on last extent */
           (bsXA[0].bsx_fob_offset == prevOldSubXtntMap->bsXA[i].bsx_fob_offset) &&
           (prevOldSubXtntMap->bsXA[i].bsx_vd_blk == XTNT_TERM)) {
            /* skip the terminator cause next extent in next map has fobs */
            break; 
        }
        newSubXtntMap->bsXA[i].bsx_fob_offset = prevOldSubXtntMap->bsXA[i].bsx_fob_offset;
        newSubXtntMap->bsXA[i].bsx_vd_blk  = prevOldSubXtntMap->bsXA[i].bsx_vd_blk;
    }

    /* finally write back in the extents saved off */
    for(j=0 ; j<newSubXtntMap->cnt && i<=newSubXtntMap->maxCnt; i++,j++) {
        newSubXtntMap->bsXA[i].bsx_fob_offset = bsXA[j].bsx_fob_offset;
        newSubXtntMap->bsXA[i].bsx_vd_blk  = bsXA[j].bsx_vd_blk;
    }

    newSubXtntMap->bssxmFobCnt = prevOldSubXtntMap->bssxmFobCnt + 
                                               newSubXtntMap->bssxmFobCnt;
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
*step 6) update original mcell on disk (same mcell as origStart)
*(caller performs submap merge)
*
*
*/

static
statusT
alloc_hole_stg (
                bfAccessT *bfap,           /* in */
                bf_fob_t fob_offset,       /* in */
                bf_fob_t fob_cnt,          /* in */
                bf_fob_t minimum_contiguous_fobs, /* in */
                vdIndexT bfVdIndex,        /* in */
                bsInMemXtntMapT *xtntMap,  /* in */
                uint32_t mapIndex,         /* in */
                uint32_t descIndex,        /* in */
                bsAllocHintT alloc_hint,   /* in */
                ftxHT parentFtx,           /* in */
                int updateDisk,            /* in */
                bf_fob_t *alloc_fob_cnt    /* out */
                )
{
    bfSetT *bfSetDesc;
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32_t i;
    bsInMemSubXtntMapT *newSubXtntMap;
    bsInMemSubXtntMapT *oldSubXtntMap;
    bsInMemSubXtntMapT *prevOldSubXtntMap;
    bf_fob_t tmp_fob_cnt;
    statusT sts;
    bfMCIdT reuseMcellId = bsNilMCId;
    bfMCIdT newMcellId;

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
        /* Tell imm_extend_xtnt_map we are working on the in place
         * xtntmap so lock the xtntMap_lk before swapping memory.
         */
        sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);
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
    newSubXtntMap->bssxmFobCnt = newSubXtntMap->bsXA[newSubXtntMap->cnt-1].bsx_fob_offset -
                                                      newSubXtntMap->bssxmFobOffset;

    /* step 3: alloc the storage on any volume (tries last vd alloc first) */
    bfSetDesc = bfap->bfSetp;

    if (bfVdIndex == (vdIndexT)(-1)) {
        sts = stg_alloc_from_svc_class( bfap,
                                        bfap->reqServices,
                                        bfSetDesc->dirTag,
                                        bfap->tag,
                                        fob_offset,
                                        fob_cnt,
                                        minimum_contiguous_fobs,
                                        xtntMap,
                                        &alloc_hint,
                                        parentFtx,
                                        &tmp_fob_cnt);
    } else {
        sts = stg_alloc_from_one_disk( bfap,
                                       bfVdIndex,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       fob_offset,
                                       fob_cnt,
                                       minimum_contiguous_fobs,
                                       xtntMap,
                                       &alloc_hint,
                                       parentFtx,
                                       &tmp_fob_cnt);
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if (tmp_fob_cnt!= fob_cnt) {
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

        if(newSubXtntMap->cnt == 1)
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
                                   xtntMap->subXtntMap[xtntMap->origStart-1].mcellId,
                                   newSubXtntMap->mcellId,
                                   newSubXtntMap->mcellId,
                                   parentFtx,
				   CHAINMCID); 
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
                               VD_HTOP(newSubXtntMap->mcellId.volume, bfap->dmnP),
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
                    newSubXtntMap->mcellId.volume = prevOldSubXtntMap->mcellId.volume;
                    newSubXtntMap->mcellId = prevOldSubXtntMap->mcellId;
                    newSubXtntMap->mcellState = ADV_MCS_INITIALIZED;
                 } /* end if combined ok */
            }  /* end if not primary mcell */
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

    oldSubXtntMap = &(xtntMap->subXtntMap[mapIndex]);
    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->updateEnd]);

    if (newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsx_fob_offset <
        oldSubXtntMap->bsXA[descIndex + 1].bsx_fob_offset) {
        /*
         * The last new fob allocated and the next fob after the hole are
         * not contiguous, we need to create a hole descriptor and copy it to
         * the end of the extent map.
         */

        /* step 5a: create hole desc in temp variable bsXA */
        imm_split_desc( newSubXtntMap->bsXA[newSubXtntMap->cnt - 1].bsx_fob_offset,
                        &(oldSubXtntMap->bsXA[descIndex]),  /* orig */
                        &desc,  /* first part */
                        &(bsXA[0]));  /* last part */
        bsXA[1].bsx_fob_offset = oldSubXtntMap->bsXA[descIndex + 1].bsx_fob_offset;
        bsXA[1].bsx_vd_blk = XTNT_TERM;
        /* copy new tmp hole desc into a subXtntMap after any new stg maps */
        sts = imm_copy_xtnt_descs( bfap,
                                   (vdIndexT) oldSubXtntMap->mcellId.volume,
                                   &(bsXA[0]),
                                   1,
                                   TRUE,
                                   xtntMap,
                                   XTNT_ORIG_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        oldSubXtntMap = &(xtntMap->subXtntMap[mapIndex]);
    }
    descIndex++;

    /* Step 5b:
     * Copy the remaining entries from the split sub extent map to after hole
     */
    sts = imm_copy_xtnt_descs( bfap,
                               (vdIndexT) oldSubXtntMap->mcellId.volume,
                               &(oldSubXtntMap->bsXA[descIndex]),
                               (oldSubXtntMap->cnt - 1) - descIndex,
                               TRUE,
                               xtntMap,
                               XTNT_ORIG_XTNTMAP);
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
        MS_SMP_ASSERT(newSubXtntMap->mcellState == ADV_MCS_NEW_MCELL);
        MS_SMP_ASSERT(!MCID_EQL(oldSubXtntMap->mcellId, newSubXtntMap->mcellId));
        MS_SMP_ASSERT(oldSubXtntMap->mcellId.volume == newSubXtntMap->mcellId.volume);
        /* Delete newly alloced mcell, we'll keep the old (mapIndex) mcell. */
        sts = deferred_free_mcell( VD_HTOP(newSubXtntMap->mcellId.volume, bfap->dmnP),
                                   newSubXtntMap->mcellId,
                                   parentFtx);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        newSubXtntMap->mcellId = reuseMcellId;  /* this is mapIndex mcell */
        newSubXtntMap->mcellState = ADV_MCS_USED_MCELL;
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
            newSubXtntMap->mcellState = ADV_MCS_USED_MCELL;
        }
        else
        {
            sts = stg_alloc_new_mcell( bfap,
                                       bfSetDesc->dirTag,
                                       bfap->tag,
                                       (vdIndexT) newSubXtntMap->mcellId.volume,
                                       parentFtx,
                                       &newMcellId,
                                       TRUE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            MS_SMP_ASSERT(newSubXtntMap->mcellId.volume == newMcellId.volume);
            newSubXtntMap->mcellId = newMcellId;
            newSubXtntMap->mcellState = ADV_MCS_NEW_MCELL;
        }

        newSubXtntMap->updateStart = 0;
        newSubXtntMap->updateEnd = newSubXtntMap->cnt - 1;
    }  /* end for */

    xtntMap->updateEnd = xtntMap->cnt - 1;

    if ( updateDisk ) {
        /* Step 6 */
        sts = x_update_ondisk_xtnt_map( bfap->dmnP,
                                        bfap,
                                        xtntMap,
                                        parentFtx);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    *alloc_fob_cnt= tmp_fob_cnt;
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
 * extent map's sub extent maps.  The number of fobs allocated are
 * returned via the "alloc_fob_cnt" parameter.
 *
 * The fob offset of the start of the allocation is assumed to be
 * equal to or greater than the fob offset of the last fob mapped
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
                          bfTagT bfSetTag,            /* in */
                          bfTagT bfTag,               /* in */
                          bf_fob_t fob_offset,       /* in */
                          bf_fob_t fob_cnt,          /* in */
                          bf_fob_t minimum_contiguous_fobs, /* in */
                          bsInMemXtntMapT *xtntMap,   /* in */
                          bsAllocHintT *alloc_hint,   /* in */
                          ftxHT parentFtx,            /* in */
                          bf_fob_t *alloc_fob_cnt   /* out */
                          )
{
    int maxSkipCnt = 0;
    bf_fob_t tmp_fob_cnt;
    bf_vd_blk_t requestedBlkCnt;
    int skipCnt = 0;
    vdIndexT *skipVdIndex = NULL;
    statusT sts;
    bf_fob_t total_fob_cnt = 0;
    uint32_t updateStart;
    vdIndexT vdIndex;
    vdT *vdp;

    updateStart = xtntMap->cnt - 1;

    if (FTX_EQ(parentFtx, FtxNilFtxH)) {
        RAISE_EXCEPTION (EBAD_PAR_FTXH);
    }

    /* start looking on last vd stg was allocated from 
     * otherwise, if this is first open from disk, try the previous
     * subXtntMap's vd 
     */
    if (xtntMap->subXtntMap[updateStart].mcellState == ADV_MCS_NEW_MCELL){
        vdIndex = FETCH_MC_VOLUME(xtntMap->subXtntMap[updateStart].mcellId);
    } else {
        if(xtntMap->allocVdIndex != -1)
            vdIndex = xtntMap->allocVdIndex;
        else
            vdIndex = FETCH_MC_VOLUME(xtntMap->subXtntMap[updateStart].mcellId);
    }

    /*
     * Verify that the disk is still in the bitfile's service class.  If
     * we kicked out earlier, this disk can't be selected again.
     */
    sts = sc_valid_vd( bfap->dmnP,
                       bfap->reqServices,
                       vdIndex);
    if ( sts == E_VOL_NOT_IN_SVC_CLASS ) {
        goto findvalidvd;
    }
    if ( sts != EOK ) {
        return sts;
    }

    /* bump the ref count on the vd */
    vdp = vd_htop_if_valid(vdIndex, bfap->dmnP, TRUE, FALSE);
    if ( vdp == NULL ) {
        goto findvalidvd;
    }

    /*
     * The volume is in the bitfile's service class.
     */
    sts = stg_alloc_from_one_disk( bfap,
                                   vdIndex,
                                   bfSetTag,
                                   bfTag,
                                   fob_offset,
                                   fob_cnt,
                                   minimum_contiguous_fobs,
                                   xtntMap,
                                   alloc_hint,
                                   parentFtx,
                                   &total_fob_cnt);
    vd_dec_refcnt(vdp);
    if ((sts != EOK) && (sts != ENO_MORE_BLKS)) {
        RAISE_EXCEPTION (sts);
    }

    while (total_fob_cnt < fob_cnt) {

        /*
         * Add the previous disk to the skip list.
         */

        if (skipCnt == maxSkipCnt) {
            sts = extend_skip (&maxSkipCnt, &skipVdIndex);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
        skipVdIndex[skipCnt] = vdIndex;
        skipCnt++;
        MS_SMP_ASSERT(skipCnt <= bfap->dmnP->vdCnt);

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
    findvalidvd:
        requestedBlkCnt = (fob_cnt - total_fob_cnt) / ADVFS_FOBS_PER_DEV_BSIZE;

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    reqServices,
                                    skipCnt,
                                    skipVdIndex,
                                    requestedBlkCnt,
                                    bfap->bfPageSz,
                                    FALSE); /** return stgMap_lk unlocked **/
        if (sts != EOK) {
            if ((sts == ENO_MORE_BLKS) && (total_fob_cnt > 0)) {
                /*
                 * No more storage available in the service class.
                 */
                break;  /* out of while */
            } else {
                RAISE_EXCEPTION (sts);
            }
        }

        vdIndex = vdp->vdIndex; 
        tmp_fob_cnt = 0;

        /*
         * Assert that we're about to add storage in allocation units.
         */
        MS_SMP_ASSERT(((fob_cnt - total_fob_cnt) % bfap->bfPageSz)  == 0);

        sts = stg_alloc_from_one_disk( bfap,
                                       vdIndex,
                                       bfSetTag,
                                       bfTag,
                                       fob_offset + total_fob_cnt,
                                       fob_cnt - total_fob_cnt,
                                       minimum_contiguous_fobs,
                                       xtntMap,
                                       alloc_hint,
                                       parentFtx,
                                       &tmp_fob_cnt);
        vd_dec_refcnt(vdp);

        if ((sts != EOK) && (sts != ENO_MORE_BLKS)) {
                RAISE_EXCEPTION (sts);
        }
        total_fob_cnt = total_fob_cnt + tmp_fob_cnt;

    }  /* end while */

    if (skipVdIndex != NULL) {
        ms_free (skipVdIndex);
    }

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;
    *alloc_fob_cnt = total_fob_cnt;

    return EOK;

HANDLE_EXCEPTION:
    if (skipVdIndex != NULL) {
        ms_free (skipVdIndex);
    }

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;

}  /* end stg_alloc_from_svc_class */

/*
** Allocate "fob_cnt" fobs of storage for "bfap". The fobs will be added at file offset
** "fob_offset". Allocate storage from as many volumes as
** necessary to fill the request. The storage will be represented on disk
** by a chain of mcells on the defered delete list. The storage will be
** represented in memory by "xtntMap". On entry xtntMap is empty.
*/
statusT
cp_stg_alloc_from_svc_class (
                          bfAccessT *bfap,      /* in  stg for thsi file*/
                          bf_fob_t fob_offset,  /* in  start here */
                          bf_fob_t fob_cnt,     /* in  for this long */
                          bf_fob_t minimum_contiguous_fobs, /* in */
                          int32_t stg_type,
                          bsInMemXtntMapT *xtntMap, /* in  put in this map */
                          bf_fob_t xtnt_start_fob,  /* in  first xtnt fob */
                          bsAllocHintT alloc_hint,  /* in */
                          ftxHT parent_ftx          /* in used for snapshot metadata */
                          )
{
    int maxSkipCnt = 0;
    bf_fob_t tmp_fob_cnt =0;
    bf_vd_blk_t requestedBlkCnt;
    int skipCnt = 0;
    vdIndexT *skipVdIndex = NULL;
    statusT sts;
    bf_fob_t total_fob_cnt = 0;
    vdT *vdp;

    MS_SMP_ASSERT(bfap->dmnP == xtntMap->domain);

    while ( total_fob_cnt < fob_cnt ) {

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

        if (stg_type == COWED_HOLE)
        {
            /* We really don't need any storage but we have to pick a disk
             * so chose one that has at least an allocation unit. 
             */
            requestedBlkCnt = bfap->bfPageSz / ADVFS_FOBS_PER_DEV_BSIZE;
        }
        else
        {
            requestedBlkCnt = (fob_cnt - total_fob_cnt) / ADVFS_FOBS_PER_DEV_BSIZE;
        }

        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    bfap->reqServices,
                                    skipCnt,
                                    skipVdIndex,
                                    requestedBlkCnt,
                                    bfap->bfPageSz,
                                    FALSE); /* return stgMap_lk unlocked */
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }

        sts = cp_stg_alloc_from_one_disk( bfap,
                                          vdp->vdIndex,
                                          fob_offset + total_fob_cnt,
                                          fob_cnt - total_fob_cnt,
                                          minimum_contiguous_fobs, 
                                          stg_type,
                                          xtntMap,
                                          xtnt_start_fob,
                                          &tmp_fob_cnt,
                                          0,
                                          alloc_hint,
                                          parent_ftx);
        vd_dec_refcnt(vdp);
        if ( sts != EOK && sts != ENO_MORE_BLKS ) {
            RAISE_EXCEPTION (sts);
        }

        total_fob_cnt = total_fob_cnt + tmp_fob_cnt;

        /* Add the previous disk to the skip list. */
        if ( skipCnt == maxSkipCnt ) {
            sts = extend_skip(&maxSkipCnt, &skipVdIndex);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }
        skipVdIndex[skipCnt] = vdp->vdIndex;
        skipCnt++;
        MS_SMP_ASSERT(skipCnt <= bfap->dmnP->vdCnt);
    }  /* end while */

    if ( skipVdIndex != NULL ) {
        ms_free(skipVdIndex);
    }

    return EOK;

HANDLE_EXCEPTION:
    if ( skipVdIndex != NULL ) {
        ms_free(skipVdIndex);
    }

    return sts;
}

/*
 * extend_skip
 *
 * This function extends the skip vd index array.  If it is empty, it creates
 * a new one.
 */

#define SKIP_SIZE 4

statusT
extend_skip (
             int *skipCnt,           /* in/out */
             vdIndexT **skipVdIndex  /* in/out */
             )
{
    int i;
    int newCnt;
    vdIndexT *newVdIndex;
    int oldCnt;
    vdIndexT *oldVdIndex;

    oldCnt = *skipCnt;
    oldVdIndex = *skipVdIndex;

    if (oldCnt != 0) {
        newCnt = oldCnt + SKIP_SIZE;
        newVdIndex = (vdIndexT *) ms_malloc (newCnt * sizeof(vdIndexT));

        for (i = 0; i < oldCnt; i++) {
            newVdIndex[i] = oldVdIndex[i];
        }

        ms_free (oldVdIndex);
    } else {
        newCnt = SKIP_SIZE;
        newVdIndex = (vdIndexT *) ms_malloc (newCnt * sizeof(vdIndexT));
    }

    *skipCnt = newCnt;
    *skipVdIndex = newVdIndex;

    return EOK;

}  /* end extend_skip */

/*
 * stg_alloc_from_one_disk
 *
 * This function allocates storage from the specified disk and saves
 * the allocation information in the in-memory extent map's sub extent
 * maps.  The number of fobs allocated are returned via the
 * "alloc_fob_cnt" parameter.
 *
 * The fob offset of the start of the allocation is assumed to be
 * equal to or greater than the fob offset of the last fob mapped
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

static
statusT
stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in */
                         vdIndexT bfVdIndex,        /* in */
                         bfTagT bfSetTag,           /* in */
                         bfTagT bfTag,              /* in */
                         bf_fob_t fob_offset,      /* in */
                         bf_fob_t fob_cnt,         /* in */
                         bf_fob_t minimum_contiguous_fobs, /* in */
                         bsInMemXtntMapT *xtntMap,  /* in */
                         bsAllocHintT *alloc_hint,  /* in */
                         ftxHT parentFtx,           /* in */
                         bf_fob_t *alloc_fob_cnt /* out */
                         )
{
    ftxHT ftxH;
    bfMCIdT mcellId;
    bf_fob_t tmp_fob_cnt;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t total_fob_cnt = 0;

    uint32_t updateStart;
    int ftxStarted = FALSE;
    bf_fob_t rsvd_fob_cnt = 0;
    bf_vd_blk_t rsvd_fob_vd_blk = 0;

    updateStart = xtntMap->cnt - 1;

    if (FTX_EQ(parentFtx, FtxNilFtxH)) {
        RAISE_EXCEPTION (EBAD_PAR_FTXH);
    }

    subXtntMap = &(xtntMap->subXtntMap[updateStart]);

    if (FETCH_MC_VOLUME(subXtntMap->mcellId) == bfVdIndex) {

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
                           bfap->dmnP
                           );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        ftxStarted = TRUE;

        /*
         * The last sub extent map's disk matches the specified disk.
         * Allocate storage and save the information in the last map.
         */

        sts = alloc_from_one_disk( bfap,
                                   fob_offset,
                                   fob_cnt,
                                   minimum_contiguous_fobs,
                                   subXtntMap,
                                   alloc_hint,
                                   ftxH,
                                   &rsvd_fob_cnt,
                                   &rsvd_fob_vd_blk,
                                   &total_fob_cnt);
        /* We must return the fobs we successfully allocated even if we */
        /* fail subsequent operations. */
        *alloc_fob_cnt = total_fob_cnt;
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
        if (rsvd_fob_cnt == 0) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_270);
            ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_271);
            ftxStarted = FALSE;
        }

    }

    while (total_fob_cnt< fob_cnt) {

        if (xtntMap->cnt >= xtntMap->maxCnt) {
            /* Tell imm_extend_xtnt_map we are working on the in place
             * xtntmap so lock the xtntMap_lk before swapping memory.
             */
            sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);
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
                               bfap->dmnP
                               );
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
                                   TRUE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        MS_SMP_ASSERT(bfVdIndex == FETCH_MC_VOLUME(mcellId));

        /*
         * NOTE:  The first fob of the new sub extent map must be the
         * next fob after the last fob mapped by the previous sub extent
         * map.
         */
        sts = imm_init_sub_xtnt_map( subXtntMap,
                                     (prevSubXtntMap->bssxmFobOffset +
                                      prevSubXtntMap->bssxmFobCnt),
                                     0,  /* fob cnt*/
                                     mcellId,
                                     BSR_XTRA_XTNTS,
                                     BMT_XTRA_XTNTS,
                                     0);  /* force default value for maxCnt */
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subXtntMap->mcellState = ADV_MCS_NEW_MCELL;

        sts = alloc_from_one_disk( bfap,
                                   fob_offset + total_fob_cnt,
                                   fob_cnt - total_fob_cnt,
                                   minimum_contiguous_fobs,
                                   subXtntMap,
                                   alloc_hint,
                                   ftxH,
                                   &rsvd_fob_cnt,
                                   &rsvd_fob_vd_blk,
                                   &tmp_fob_cnt);
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
            if ((sts == ENO_MORE_BLKS) && (total_fob_cnt > 0)) {
                ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_463);
                ftx_fail (ftxH);
                ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_464);
                ftxStarted = FALSE;
                break;  /* out of while loop */
            } else {
                RAISE_EXCEPTION (sts);
            }
        }

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_272);
        ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_273);
        ftxStarted = FALSE;

        total_fob_cnt = total_fob_cnt + tmp_fob_cnt;
        /* We need to return this now in case we fail on subsequent passes */
        /* thru the loop. */
        *alloc_fob_cnt = total_fob_cnt;

        xtntMap->cnt++;

    }  /* end while */

    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_274);
        ftx_done_n (ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_275);
    }

    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return EOK;

HANDLE_EXCEPTION:
    if (ftxStarted) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_465);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_466);
    }
    xtntMap->updateStart = updateStart;
    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;

}  /* end stg_alloc_from_one_disk */

/*
** Allocate as much of the request ("fob_cnt" fobs) from the volume
** identified by "bfVdIndex" as possible. The allocated storage is for
** "bfap" file. The storage will be added at offset "fob_offset".
** The storage will be represented in memory by "xtntMap". The allocated
** storage is represented on disk by a chain of mcells on the defered
** delete list. "alloc_fob_cnt " is the number of fobs actually allocated.
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
* 
*  If alloc_hint & BS_ALLOC_ONE_FTX is true, then we cannot stop the
*  transaction since one was passed in.  This is used for migrating child
*  snapshot metadata files.  We will return as much storage as could be
*  allocated in the given transaction and return EAGAIN to the caller to let
*  them come back in with a fresh transaction.
*/
statusT
cp_stg_alloc_from_one_disk (
                         bfAccessT *bfap,           /* in   stg for this file */
                         vdIndexT bfVdIndex,        /* in   stg in this vol */
                         bf_fob_t fob_offset,       /* in   stg starts here */
                         bf_fob_t fob_cnt,          /* in   ask for this much */
                         bf_fob_t minimum_contiguous_fobs, /* in */
                         int32_t stg_type,
                         bsInMemXtntMapT *xtntMap,  /* in   put in this map */
                         bf_fob_t xtnt_start_fob,   /* in  first xtnt fob */
                         bf_fob_t *alloc_fob_cnt,   /* out - got this many 1k fobs */
                         bf_vd_blk_t dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                         bsAllocHintT alloc_hint,  /* in */
                         ftxHT  parent_ftx      /* in used for snapshot metadata */
                         )
{
    bfMCIdT mcellId;
    bf_fob_t tmp_fob_cnt = 0;
    bsInMemSubXtntMapT *prevSubXtntMap;
    statusT sts;
    statusT sts2 = EOK;

    uint32_t updateStart;
    bsInMemSubXtntMapT *subXtntMap;

    bf_fob_t total_fob_cnt = 0;
    bf_fob_t first_fob;
    ftxHT ftxH;
    
    bfMCIdT delMcellId;

    MS_SMP_ASSERT(bfap->dmnP == xtntMap->domain);

    ftxH.hndl = 0;  /* flag that a ftx has or has not been started */

    MS_SMP_ASSERT(fob_cnt > 0);

    while ( total_fob_cnt < fob_cnt ) {

        if ( xtntMap->cnt >= xtntMap->maxCnt ) {
            /* This routine is working on a copy extent map.
             * This is no need to lock the extentmap for the
             * file since this copy is not seen by any other 
             * threads
             */
            sts = imm_extend_xtnt_map(bfap,xtntMap,XTNT_TEMP_XTNTMAP);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

        if ( xtntMap->cnt == 0 ) {
            /* The first extent may start with a hole. */
            /* In this case xtnt_start_fob will be smaller than fob_offset. */
            first_fob = xtnt_start_fob;
        } else {
            prevSubXtntMap = &xtntMap->subXtntMap[xtntMap->cnt - 1];
            first_fob = prevSubXtntMap->bssxmFobOffset + prevSubXtntMap->bssxmFobCnt;
        }
        subXtntMap = &xtntMap->subXtntMap[xtntMap->cnt];

        sts = FTX_START_N(FTA_NULL, &ftxH, parent_ftx, bfap->dmnP);
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
        MS_SMP_ASSERT(bfVdIndex == FETCH_MC_VOLUME(mcellId));

        /*
         * NOTE:  The first fob of the new sub extent map must be the
         * next fob after the last fob mapped by the previous sub extent
         * map.
         */
        sts = imm_init_sub_xtnt_map( subXtntMap,
                                     first_fob,
                                     0,  /* fob cnt*/
                                     mcellId,
                                     BSR_XTRA_XTNTS,
                                     BMT_XTRA_XTNTS,
                                     0);  /* force default value for maxCnt */
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        subXtntMap->mcellState = ADV_MCS_NEW_MCELL;

        sts = odm_create_xtnt_rec( bfap,
                                   bfap->bfSetp,
                                   subXtntMap,
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
                                       &delMcellId);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            xtntMap->hdrMcellId = delMcellId;

            sts = del_add_to_del_list( delMcellId,
                                       bfap->dmnP->vdpTbl[delMcellId.volume - 1],
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
                                   xtntMap->subXtntMap[xtntMap->cnt-2].mcellId,
                                   subXtntMap->mcellId,
                                   subXtntMap->mcellId,
                                   ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

                }

        /* cp_stg_alloc_in_one_mcell may stop this transaction and it may */
        /* start another. We pass a pointer to the ftxH so we can know */
        /* if it returned with a transaction in progress or not. */

        if (stg_type == COWED_HOLE)
        {
            /* When migrating a clone special care must be taken to also
             * migrate any permenant holes. Backing storage is not required
             * but, the extent record must be there.
             */ 

            sts2 = cp_hole_alloc_in_one_mcell( fob_offset + total_fob_cnt,
                                               fob_cnt - total_fob_cnt,
                                               subXtntMap,
                                               &tmp_fob_cnt);
            if ( sts2 != EOK )
            {
                RAISE_EXCEPTION(sts2);
            }
        }
        else
        {

            sts2 = cp_stg_alloc_in_one_mcell( bfap,
                                              fob_offset + total_fob_cnt,
                                              fob_cnt - total_fob_cnt,
                                              minimum_contiguous_fobs, 
                                              subXtntMap,
                                              &ftxH,
                                              &tmp_fob_cnt,
                                              dstBlkOffset,
                                              alloc_hint);
            if ( ( sts2 != EOK) && (sts2 != ENO_MORE_BLKS) && (sts2 != EAGAIN) )
            {
                RAISE_EXCEPTION(sts2);
            }
        }

        if (tmp_fob_cnt> 0)
        {
            if ( ftxH.hndl ) {
                /* We still have a transaction going. Update the on disk xtnt */
                /* chain and finish the transaction here. 
                 * If status is EAGAIN it means we need to finish this
                 * transaction and return to migrate to let the caller start
                 * a new root transaction and call back in.  */
                sts = update_xtnt_rec(bfap->dmnP, bfap->tag, subXtntMap, ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
                ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_276);
                ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
                ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_277);
                ftx_done_n( ftxH, FTA_BS_STG_ALLOC_MCELL_V1);
                ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_278);
                ftxH.hndl=0;
                subXtntMap->updateStart = subXtntMap->cnt;
                subXtntMap->updateEnd = subXtntMap->cnt;
            }

            total_fob_cnt = total_fob_cnt + tmp_fob_cnt;
            /* We need to return this now in case we fail on subsequent passes */
            /* thru the loop. */
            *alloc_fob_cnt= total_fob_cnt;

            xtntMap->validCnt = xtntMap->cnt;

        }
        /*
         * If we have an ENO_MORE_BLKS return status:
         *
         * If the tmp_fob_cnt> 0 this pass through the loop was able to get some
         * of the requested space. We want to return the space we got but also return 
         * the error so the caller can take appropriate action (ie try another disk).
         * 
         * If the tmp_fob_cnt == 0 we were unable to get any storage for this pass thru
         * the loop. We want to undo any resources we obtained for this storage
         * request and return the ENO_MORE_BLKS status to our caller. Note a 
         * previous pass thru the loop may have obtained some storage so we only
         * want to free the resources for this pass thru.
         */
        
        MS_SMP_ASSERT((tmp_fob_cnt > 0) || (sts2 == ENO_MORE_BLKS));

        if ( sts2 == ENO_MORE_BLKS )
        {
            RAISE_EXCEPTION(sts2);
        }

    }  /* end while */

    return sts2;

HANDLE_EXCEPTION:
    if ( ftxH.hndl != 0 ) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_467);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_468);
    }

    if ( tmp_fob_cnt == 0 ) {
        if (xtntMap->cnt > 0) xtntMap->cnt--;
        if ( xtntMap->cnt == 0 ) {
            xtntMap->hdrMcellId = bsNilMCId;
        }
        imm_unload_sub_xtnt_map(subXtntMap);
    }

    return sts;
}

/*
 * alloc_from_one_disk
 *
 * This function allocates storage from the specified disk and saves
 * the allocation information in the specified sub extent map's extent
 * descriptor array.  It assumes that the specified fob offset is
 * greater than or equal to the bitfile's last fob offset.
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
 * returned, the "alloc_fob_cnt" parameter has a valid value.
 */

static
statusT
alloc_from_one_disk (
                     bfAccessT *bfap,                 /* in */
                     bf_fob_t fob_offset, /* in */
                     bf_fob_t fob_cnt,    /* in */
                     bf_fob_t minimum_contiguous_fobs, /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     bsAllocHintT *alloc_hint,        /* in/out */
                     ftxHT parentFtx,                 /* in */
                     bf_fob_t *rsvd_fob_cnt,        /* in/out */
                     bf_vd_blk_t          *rsvd_fob_vd_blk,     /* in/out */
                     bf_fob_t *alloc_fob_cnt        /* out */
                     )
{
    domainT *domain;
    bf_vd_blk_t blkOff;
    uint32_t i, j;
    bf_vd_blk_t nextBlkOff;
    bf_fob_t tmp_fob_cnt;
    statusT sts;
    bsXtntT termDesc;
    bf_fob_t total_fob_cnt = 0;
    vdT *vd;
    int pinCnt = FTX_MX_PINP - 1; /*no more than 6 pages of SBM can be pinned*/
    int tryToExpandLastExtent = FALSE;

    MS_SMP_ASSERT(ADVFS_SMALL_FILE(bfap) || 
                  minimum_contiguous_fobs >= (VM_PAGE_SZ/ADVFS_FOB_SZ) ||
                  fob_offset + fob_cnt <= (VM_PAGE_SZ/ADVFS_FOB_SZ));
    
    domain = bfap->dmnP;

    vd = VD_HTOP(subXtntMap->mcellId.volume, domain);

    /*
     * Save the current termination descriptor and index in case we fail and
     * must restore the sub extent map's previous state.
     */
    subXtntMap->updateStart = subXtntMap->cnt - 1;
    termDesc = subXtntMap->bsXA[subXtntMap->updateStart];

    i = subXtntMap->updateStart;

    if (fob_offset == subXtntMap->bsXA[i].bsx_fob_offset) {
        /*
         * Allocating storage that is virtually contiguous with the
         * bitfile's last fob.  Determine if the new extent and the
         * previous extent are physically contiguous on disk.
         */
        if (i > 0) {
            if ( subXtntMap->bsXA[i - 1].bsx_vd_blk != XTNT_TERM &&
                 subXtntMap->bsXA[i - 1].bsx_vd_blk != COWED_HOLE )
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

                nextBlkOff = subXtntMap->bsXA[i - 1].bsx_vd_blk +
                  ((subXtntMap->bsXA[i].bsx_fob_offset -
                    subXtntMap->bsXA[i - 1].bsx_fob_offset) / ADVFS_FOBS_PER_DEV_BSIZE);

                /*
                 * When dealing with files being written by NFS,
                 * set the alloc_hint and the blkOff such that the
                 * change for contiguous allocation is improved.
                 */
                if (NFS_SERVER) {
                    blkOff = nextBlkOff;
                    *alloc_hint = BS_ALLOC_NFS;
                }

                /*
                 * Allocate a new extent and determine if it and the previous
                 * extent are physically contiguous on disk.
                 */
                sts = stg_alloc_one_xtnt( bfap,
                                          vd,
                                          fob_cnt,
                                          0,
                                          minimum_contiguous_fobs,
                                          *alloc_hint,
                                          parentFtx,
                                          &total_fob_cnt,
                                          &blkOff,
                                          &pinCnt);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }

                MS_SMP_ASSERT(ADVFS_SMALL_FILE(bfap) || 
                             total_fob_cnt  >= (VM_PAGE_SZ/ADVFS_FOB_SZ) ||
                             fob_offset + fob_cnt <= (VM_PAGE_SZ/ADVFS_FOB_SZ));
    
                if (blkOff == nextBlkOff) {
                    /*
                     * The extents are contiguous.  Increase the size of
                     * the previous extent.
                     */
                    subXtntMap->bsXA[i].bsx_fob_offset = subXtntMap->bsXA[i].bsx_fob_offset +
                      total_fob_cnt;
                } else {
                    /*
                     * The extents are not contiguous.
                     */
                    if (tryToExpandLastExtent) {
                        *rsvd_fob_cnt = total_fob_cnt;
                        *rsvd_fob_vd_blk = blkOff;
                        RAISE_EXCEPTION(EXTND_FAILURE); 
                    }
                    else {
                        subXtntMap->bsXA[i].bsx_fob_offset = fob_offset;
                        subXtntMap->bsXA[i].bsx_vd_blk = blkOff;
                        i++;
                    }
                }
            }
        }
    } else {
        /*
         * Allocating storage at least one fob beyond the bitfile's last fob,
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
        subXtntMap->bsXA[i + 1].bsx_fob_offset = fob_offset;
        i++;
    }

    while (total_fob_cnt < fob_cnt) {

        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the real and the termination extent descriptors?
         * If not, extend the array.
         */
        if (i == (subXtntMap->maxCnt - 1)) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map (subXtntMap);
            if (sts != EOK) {
                if ((sts == EXTND_FAILURE) && (total_fob_cnt > 0)) {
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
        if (NFS_SERVER && i > 1) {

            /*
             * calculate the new block offset based on the delta between
             * the requested fob and the descriptor info for the final
             * fob range (the "i-2" descriptor).
             */
            if (fob_offset + total_fob_cnt > 
                subXtntMap->bsXA[i-2].bsx_fob_offset) {

                j = (fob_offset + total_fob_cnt ) - 
                     subXtntMap->bsXA[i-2].bsx_fob_offset;
                blkOff = subXtntMap->bsXA[i-2].bsx_vd_blk + 
                                ((j * bfap->bfPageSz) / 
                                 ADVFS_FOBS_PER_DEV_BSIZE);
                j = (fob_offset + total_fob_cnt ) - 
                    subXtntMap->bsXA[i-1].bsx_fob_offset;
            }
            else {
                j = subXtntMap->bsXA[i-2].bsx_fob_offset - 
                    (fob_offset + total_fob_cnt );
                blkOff = subXtntMap->bsXA[i-2].bsx_vd_blk - 
                                ((j * bfap->bfPageSz) / 
                                 ADVFS_FOBS_PER_DEV_BSIZE);
            }

        }

        /*
         * If we already reserved some storage from the last call in
         * to this routine, use that.  Otherwise, get an extent of
         * storage.
         */
        if (*rsvd_fob_cnt ) {
            subXtntMap->bsXA[i].bsx_fob_offset = fob_offset + total_fob_cnt;
            subXtntMap->bsXA[i].bsx_vd_blk = *rsvd_fob_vd_blk;
            total_fob_cnt = total_fob_cnt + *rsvd_fob_cnt;
            *rsvd_fob_cnt = 0;
            *rsvd_fob_vd_blk = 0;
        }
        else {
            sts = stg_alloc_one_xtnt( bfap,
                                      vd,
                                      fob_cnt - total_fob_cnt,
                                      0UL,
                                      minimum_contiguous_fobs,
                                      *alloc_hint,
                                      parentFtx,
                                      &tmp_fob_cnt,
                                      &blkOff,
                                      &pinCnt);
            if (sts != EOK) {
                if ((sts == ENO_MORE_BLKS) && (total_fob_cnt > 0)) {
                    break;  /* out of while */
                } else {
                    RAISE_EXCEPTION (sts);
                }
            }
            subXtntMap->bsXA[i].bsx_fob_offset = fob_offset + total_fob_cnt;
            subXtntMap->bsXA[i].bsx_vd_blk = blkOff;
            total_fob_cnt = total_fob_cnt + tmp_fob_cnt;
        }
        i++;


    }  /* end while */

    subXtntMap->bsXA[i].bsx_fob_offset = fob_offset + total_fob_cnt ;
    subXtntMap->bsXA[i].bsx_vd_blk = XTNT_TERM;

    subXtntMap->updateEnd = i;

    subXtntMap->cnt = i + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    tmp_fob_cnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
                                                    subXtntMap->bsXA[0].bsx_fob_offset;
    if (tmp_fob_cnt > subXtntMap->bssxmFobCnt) {
        subXtntMap->bssxmFobCnt = tmp_fob_cnt;
    }

    *alloc_fob_cnt = total_fob_cnt;

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
        *alloc_fob_cnt = 0;
        sts = EOK;
    }

    return sts;

}  /* end alloc_from_one_disk */

/*
** Allocate as much storage as we can ( up to "fob_cnt" fobs) in one mcell.
** The storage is for the "bfap" file at offset "fob_offset". The mcell
** has already been allocated. It is represented in memory by "subXtntMap".
** On entry "subXtntMap" is empty.
**
** Within the subextent map (mcell), cp_stg_alloc_in_one_mcell extends the
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
                      bf_fob_t fob_offset,/* in  start here */
                      bf_fob_t fob_cnt,   /* in  requested length */
                      bf_fob_t minimum_contiguous_fobs, /* in */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      ftxHT *ftxHp,                   /* in */
                      bf_fob_t *alloc_fob_cnt, /* out. this much added */
                      bf_vd_blk_t dstBlkOffset,  /* in, chkd if hint == BS_ALLOC_MIG_RSVD */
                      bsAllocHintT alloc_hint
                      )
{
    domainT *dmnP;
    bf_vd_blk_t blkOff;
    uint32_t xI;
    bf_fob_t tmp_fob_cnt=0;
    statusT sts = EOK;
    bsXtntT termDesc;
    bf_fob_t total_fob_cnt = 0;
    vdT *vdp;
    int pinCnt = FTX_MX_PINP - 1;

    MS_SMP_ASSERT(fob_cnt);   /* don't call this routine w/o a request */
    MS_SMP_ASSERT(subXtntMap->cnt == 1);
    MS_SMP_ASSERT(subXtntMap->bsXA[0].bsx_fob_offset == subXtntMap->bssxmFobOffset);
    MS_SMP_ASSERT(fob_offset >= subXtntMap->bssxmFobOffset);

    dmnP = bfap->dmnP;

    vdp = VD_HTOP(subXtntMap->mcellId.volume, dmnP);

    xI = 0;
    subXtntMap->updateStart = 0;
    termDesc = subXtntMap->bsXA[0];
    *alloc_fob_cnt = 0;

    while ( (total_fob_cnt < fob_cnt) && (sts == EOK) ) {

        /*
         * Is there enough room in the extent descriptor array for two
         * entries, the real and the termination extent descriptors?
         * If not, extend the array.
         */
        if ( xI >= subXtntMap->maxCnt - 2 ) {
            /* One entry left. */
            sts = imm_extend_sub_xtnt_map(subXtntMap);
            if ( sts == EXTND_FAILURE && total_fob_cnt > 0 ) {
                /*
                 * No more room in the extent array but there is
                 * more space on the disk.
                 */
                break;  /* out of while */
            } else if ( xI >= (subXtntMap->maxCnt - 2) && total_fob_cnt > 0 ) {
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
                                  fob_cnt - total_fob_cnt,
                                  dstBlkOffset,
                                  minimum_contiguous_fobs,
                                  alloc_hint,
                                  *ftxHp,
                                  &tmp_fob_cnt,
                                  &blkOff,
                                  &pinCnt);
        if ( sts == ENO_MORE_BLKS && total_fob_cnt > 0 ) {
            break;  /* out of while */
        } else if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        sts = add_extent( bfap,
                          subXtntMap,
                          &xI,
                          fob_offset + total_fob_cnt,
                          blkOff,
                          tmp_fob_cnt,
                          ftxHp,
                          &termDesc,
                          &pinCnt,
                          alloc_hint);
        total_fob_cnt += tmp_fob_cnt;
        if ( (sts != EOK) && (sts != EAGAIN) ) {
            /* The only failure here is a failure to start a new ftx. */
            /* We need to tell the caller we got some storage. */
            *alloc_fob_cnt= total_fob_cnt;
            RAISE_EXCEPTION(sts);
        }
    }  /* end while */

    /* We got all the storage we wanted or */
    /* ran out of extents in the subextent map (mcell) or */
    /* ran out of storage. In any case we are done adding storage to this */
    /* mcell. Wrap it up. */
    xI++;
    subXtntMap->bsXA[xI].bsx_fob_offset = fob_offset + total_fob_cnt;
    MS_SMP_ASSERT( subXtntMap->bsXA[xI].bsx_fob_offset <= 
                        (ADVFS_OFFSET_TO_FOB_UP(ADVFS_MAX_OFFSET) ) );
    subXtntMap->bsXA[xI].bsx_vd_blk = XTNT_TERM;

    subXtntMap->updateEnd = xI;

    subXtntMap->cnt = xI + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    tmp_fob_cnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
                                                    subXtntMap->bsXA[0].bsx_fob_offset;
    if (tmp_fob_cnt > subXtntMap->bssxmFobCnt) {
        subXtntMap->bssxmFobCnt = tmp_fob_cnt;
    }

    *alloc_fob_cnt = total_fob_cnt;

    if ( (sts == ENO_MORE_BLKS) || (sts == EAGAIN) ) {
        return sts;
    } else {
        return EOK;
    }

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
        *alloc_fob_cnt = 0;
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
                      bf_fob_t fob_offset,/* in  start here */
                      bf_fob_t fob_cnt,   /* in  requested length */
                      bsInMemSubXtntMapT *subXtntMap, /* in  use this map */
                      bf_fob_t *alloc_fob_cnt /* out. this much added */
                      )
{
    bf_vd_blk_t blkOff;
    uint32_t xI;
    bf_fob_t tmp_fob_cnt;
    statusT sts = EOK;

    MS_SMP_ASSERT(fob_cnt);   /* don't call this routine w/o a request */
    MS_SMP_ASSERT(subXtntMap->cnt == 1);
    MS_SMP_ASSERT(subXtntMap->bsXA[0].bsx_fob_offset == subXtntMap->bssxmFobOffset);
    MS_SMP_ASSERT(fob_offset >= subXtntMap->bssxmFobOffset);

    /* Currently we are only ever coming in here with a new mcell.
     * but this is coded to handle a partially full subextent map.
     */

    /*
     * Is there enough room in the extent descriptor array for two
     * entries, the real and the termination extent descriptors?
     * If not, extend the array.
     */

    xI = subXtntMap->cnt-1;
    
    if (subXtntMap->bsXA[xI].bsx_fob_offset < fob_offset)
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

    subXtntMap->bsXA[xI].bsx_fob_offset = fob_offset;
    subXtntMap->bsXA[xI].bsx_vd_blk = COWED_HOLE;

    xI++;

    subXtntMap->bsXA[xI].bsx_fob_offset = fob_offset + fob_cnt;
    MS_SMP_ASSERT(subXtntMap->bsXA[xI].bsx_fob_offset <= 
                        ADVFS_OFFSET_TO_FOB_UP(ADVFS_MAX_OFFSET) );
    subXtntMap->bsXA[xI].bsx_vd_blk = XTNT_TERM;

    subXtntMap->updateEnd = xI;

    subXtntMap->cnt = xI + 1;
    MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

    tmp_fob_cnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
                                                    subXtntMap->bsXA[0].bsx_fob_offset;
    if (tmp_fob_cnt > subXtntMap->bssxmFobCnt) {
        subXtntMap->bssxmFobCnt = tmp_fob_cnt;
    }

    *alloc_fob_cnt = fob_cnt;

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
        *alloc_fob_cnt= 0;
        sts = EOK;
    }

    return sts;
}

/*
** Add an extent to the subextent map "subXtntMap" at index "xIp".
** The subextent map belongs to file "bfap".
** The extent starts at file offset "fob_offset" and at disk block "blkOff".
** The extent is "fob_cnt" fobs long.
** If we have pinned the maximum pages in the SBM then we need to tidy up the extents,
** write them to disk and start a new transaction.
*
* If alloc_hint & BS_ALLOC_ONE_FTX then the passed in transaction will not
* be done'd.  But we will return EAGAIN to indicate continuing requires
* another call with a new transaction.
*/
static
statusT
add_extent(
            bfAccessT *bfap,
            bsInMemSubXtntMapT *subXtntMap,  /* add extent to this map */
            uint32_t *xIp,                   /* add extent at this index */
            bf_fob_t fob_offset,             /* first fob of extent */
            bf_vd_blk_t blkOff,              /* first block of extent */
            bf_fob_t fob_cnt,                /* extent to be this long */
            ftxHT *ftxHp,
            bsXtntT *termDescp,
            int *pinCntp,
            bsAllocHintT alloc_hint
            )
{
    bf_vd_blk_t nextBlkOff;
    bf_fob_t sub_fob_cnt;
    statusT sts;

    MS_SMP_ASSERT(fob_offset <= ADVFS_OFFSET_TO_FOB_UP( ADVFS_MAX_OFFSET) );
    MS_SMP_ASSERT(fob_cnt <=  ADVFS_OFFSET_TO_FOB_UP( ADVFS_MAX_OFFSET) );
    MS_SMP_ASSERT(fob_offset + fob_cnt <= 
                        ADVFS_OFFSET_TO_FOB_UP( ADVFS_MAX_OFFSET) );
    MS_SMP_ASSERT(*xIp < subXtntMap->maxCnt - 2);

    /* Add the extent just obtained, possibly merging with the previous. */
    if ( *xIp == 0 ) {
        if ( subXtntMap->bsXA[0].bsx_fob_offset == fob_offset ) {
            subXtntMap->bsXA[0].bsx_vd_blk = blkOff;
        } else {
            *xIp = 1;
            subXtntMap->bsXA[1].bsx_fob_offset = fob_offset;
            subXtntMap->bsXA[1].bsx_vd_blk = blkOff;
        }
    } else {
        nextBlkOff = subXtntMap->bsXA[*xIp].bsx_vd_blk +
          ((fob_offset - subXtntMap->bsXA[*xIp].bsx_fob_offset) /
            ADVFS_FOBS_PER_DEV_BSIZE);
        if ( blkOff != nextBlkOff ) {
            *xIp += 1;
            subXtntMap->bsXA[*xIp].bsx_fob_offset = fob_offset;
            subXtntMap->bsXA[*xIp].bsx_vd_blk = blkOff;
        }
    }

    if ( ( *pinCntp == 0 ) && !(alloc_hint & BS_ALLOC_ONE_FTX) ) {
        /* Can't pin any more SBM pages in this transaction. */
        /* Have to wrap up this subextent and finish the transaction. */
        subXtntMap->bsXA[*xIp + 1].bsx_fob_offset = fob_offset + fob_cnt;
        subXtntMap->bsXA[*xIp + 1].bsx_vd_blk = XTNT_TERM;
        subXtntMap->updateEnd = *xIp + 1;
        subXtntMap->cnt = *xIp + 2;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
        sts = update_xtnt_rec(bfap->dmnP, bfap->tag, subXtntMap, *ftxHp);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_279);
        ftx_done_n (*ftxHp, FTA_BS_STG_ALLOC_MCELL_V1);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_280);

        /* All of the previous storage is committed to memory on the */
        /* deferred list. Now lets start a new transaction and add */
        /* more storage. */
        subXtntMap->cnt = *xIp + 1;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
        subXtntMap->updateStart = *xIp + 1;
        *termDescp = subXtntMap->bsXA[*xIp + 1];
        sub_fob_cnt = subXtntMap->bsXA[*xIp + 1].bsx_fob_offset -
                    subXtntMap->bsXA[0].bsx_fob_offset;
        if ( sub_fob_cnt > subXtntMap->bssxmFobCnt ) {
            subXtntMap->bssxmFobCnt = sub_fob_cnt;
        }
        *pinCntp = FTX_MX_PINP - 1;
        ftxHp->hndl = 0;
        sts = FTX_START_N(FTA_NULL, ftxHp, FtxNilFtxH, bfap->dmnP);
        if ( sts != EOK ) {
            RAISE_EXCEPTION (sts);
        }
    } else if ( ( *pinCntp == 0) && (alloc_hint & BS_ALLOC_ONE_FTX) ) {
        ACCESS_STATS( add_extents_returning_eagain );
        return EAGAIN;
    }

    return EOK;

HANDLE_EXCEPTION:
    return sts;
}

/*
** Alloc one extent of "fob_cnt" fobs max. The extent is located
** and removed from the SBM. It is not put in a subextent or mcell.
** "alloc_fob_cnt_p" fobs allocated from the "vdp" volume starting
** at block "allocBlkOffp". Only "*pinCntp" pages of SBM may be pined.
** This is a running number that is decremented and returned.
*/
static
statusT
stg_alloc_one_xtnt(
                   bfAccessT *bfap,          /* in  find stg for this file */
                   vdT *vdp,                 /* in  in this volume */
                   bf_fob_t fob_cnt,         /* in  Requested length */
                   bf_vd_blk_t dstBlkOffset, /* in, checked if hint == 
                                                BS_ALLOC_MIG_RSVD */
                   bf_fob_t minimum_contiguous_fobs, /* in */
                   bsAllocHintT alloc_hint,  /* in */
                   ftxHT ftxH,               /* in */
                   bf_fob_t *alloc_fob_cnt_p,/* out  Got this much stg. */
                   bf_vd_blk_t *alloc_vd_blk_p,/* in/out  At this location.*/
                   int *pinCntp              /* in/out  this many pins left */
                   )
{
    bf_vd_blk_t blkCnt;
    domainT *dmnP;
    bf_vd_blk_t requestedBlkCnt;
    void *stgDesc;
    statusT sts;

    MS_SMP_ASSERT(ftxH.dmnP);

    dmnP = bfap->dmnP;
    MS_SMP_ASSERT(dmnP == ftxH.dmnP);
    MS_SMP_ASSERT(ftxH.hndl <= dmnP->ftxTbld.rrSlots && ftxH.hndl > 0);

    MS_SMP_ASSERT(!(fob_cnt % minimum_contiguous_fobs));

    /*
     * Try to find space for the requested number of fobs.
     * sbm_find_space() returns NULL if there is not enough
     * space on the virtual disk.  Otherwise, it returns,
     * in "blkCnt", the largest contiguous block that can be
     * allocated.  This value may or may not be large enough
     * to satisfy the request.
     */

    ADVFTM_VDT_STGMAP_LOCK(&vdp->stgMap_lk);

    requestedBlkCnt = fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE; 

retry:
    stgDesc = sbm_find_space( vdp,
                              requestedBlkCnt,
                              minimum_contiguous_fobs,
                              dstBlkOffset,
                              alloc_hint,
                              alloc_vd_blk_p,
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
        ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk)
        SS_TRACE(vdp,stgDesc,alloc_hint,blkCnt,requestedBlkCnt);
        sts = ENO_MORE_BLKS;
        goto EXIT_ALLOC_FROM_BITMAP;
    }

    MS_SMP_ASSERT(!(blkCnt % minimum_contiguous_fobs));

    /* We can only change so many pages of the SBM at once. If a really big */
    /* request comes in, reduce the number of blocks in one transaction. */
    sbm_howmany_blks( *alloc_vd_blk_p, &blkCnt, pinCntp, vdp, 
                     minimum_contiguous_fobs);

    MS_SMP_ASSERT(!(blkCnt % minimum_contiguous_fobs));

    if ( blkCnt != requestedBlkCnt ) {
        MS_SMP_ASSERT(blkCnt < requestedBlkCnt);

        fob_cnt = blkCnt * ADVFS_FOBS_PER_DEV_BSIZE; 

        if ( blkCnt < 1 ) {
            /*
             * The largest contiguous set of free blks is less than
             * the number of blks per bitfile page.  Disk VERY fragmented.
             */
            ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
            sts = ENO_MORE_BLKS;
            goto EXIT_ALLOC_FROM_BITMAP;
        }
    }

    sts = sbm_remove_space( vdp,
                            *alloc_vd_blk_p,
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

    ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);

        bfap->xtnts.xtntMap->allocVdIndex = vdp->vdIndex;

    *alloc_fob_cnt_p= fob_cnt;

EXIT_ALLOC_FROM_BITMAP:

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
     * bitfile, currently only the bmt and tagdir are ever
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
 * stg_remove_stg_start
 *
 * This function removes storage starting at the specified fob offset
 * for the specified number of fobs.  The bitfile is specified by an
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
 * LOCKS:  The caller must hold the migStgLk (this lock must
 *         be locked before starting a root transaction).
 */

statusT
stg_remove_stg_start (
                bfAccessT *bfap,     /* in */
                bf_fob_t fob_offset, /* in */
                bf_fob_t fob_cnt,    /* in */
                int relQuota,        /* in */
                ftxHT parentFtx,     /* in */
                uint32_t *delCnt,    /* out */
                void **delList,      /* out */
                int32_t doCow,       /* in */
                int32_t force_alloc, /* in - force alloc in bmt_alloc_mcell */
                int64_t flags        /* flags from getpage */
                )
{
    bf_fob_t del_fob_cnt;
    ftxHT ftx;
    uint32_t i;
    statusT sts;
    bsInMemXtntT *xtnts;
    struct advfs_pvt_param *fs_priv_param = NULL;
    int64_t locked_snap_lock = FALSE;
    off_t inval_offset;
    size_t inval_size;

    /* Make sure that we catch all the rogue add/remove storage places that
     * don't take the migStgLk.
     */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL( &bfap->xtnts.migStgLk, RWL_UNLOCKED));

    *delCnt = 0;
    *delList = NULL;

    if (fob_cnt == 0) {
        return EOK;
    }

    if (BS_BFTAG_RSVD (bfap->tag) != 0) {
        return ENOT_SUPPORTED;
    }

    xtnts = &(bfap->xtnts);

    /* We are setting up and UNDO so we better be part of a root ftx */

    MS_SMP_ASSERT (parentFtx.hndl);

    sts = FTX_START_N(FTA_BS_STG_REMOVE_V1, &ftx, parentFtx, bfap->dmnP);

    if (sts != EOK) {
        return sts;
    }

    /* This subtransaction is needed to restore the original
     * xtntmaps in case of a failure. This will do nothing if
     * the removal of storage succeeds. This is setup here so
     * that it is the last undo to run if the removal is backed
     * out. It will reload the extent maps into memory from
     * the disk. This assumes that ALL on-disk changes have
     * already been backed out.
     */
    {
        ftxHT subftx;
        restoreOrigXtntmapUndoRecT subundoRec;
        
        sts = FTX_START_N(FTA_NULL, &subftx, ftx, bfap->dmnP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subundoRec.setId = bfap->bfSetp->bfSetId;
        subundoRec.bfTag = bfap->tag;
        /*
         * If it is a bmt or rbmt, fill in the vd field, otherwise fill in the bfap field.
         */
        if (BS_BFTAG_RSVD(bfap->tag) && 
            (BS_BFTAG_RBMT(bfap->tag) || BS_IS_TAG_OF_TYPE(bfap->tag, BFM_BMT))) {
            subundoRec.bfap_or_vd.vdIndex = FETCH_MC_VOLUME(bfap->primMCId);
        } else {
            subundoRec.bfap_or_vd.bfap    = bfap;
        }
        
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_281);
        ftx_done_u (subftx, 
                    FTA_BS_XTNT_RELOAD_ORIG_XTNTMAP, 
                    sizeof (subundoRec), 
                    &subundoRec);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_282);
    }


    /* Lock the snap lock here in order to avoid a deadlock w/ another thread.
     * We want to avoid the following
     * this thread: mcell_list_lk -> snap_lock (putpage)
     * another thread: snap_lock(access_snap_children) -> mcell_list_lk 
     * (fs_update_stats)
     */
    if (!(flags & APP_SNAP_LOCK_HELD) && HAS_SNAPSHOT_CHILD(bfap)) {
        ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
        locked_snap_lock = TRUE;
    }

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with mcellList_lk write-locked.
     */
    sts = x_lock_inmem_xtnt_map( bfap, X_LOAD_UPDATE|XTNT_WAIT_OK);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Add to list of locks to be released when transaction completes. */
    FTX_ADD_LOCK(&(bfap->mcellList_lk), ftx);

    /* We no longer will lock the xtntmaps across this call since
     * We will be reading the extent maps but the can not be changing
     * We only need to hold
     * the lock exclusively when we grow the extent maps. We will
     * only be modifying the update ranges of the xtntmap and we
     * have exclusive access to this range due to the mcellList_lk
     * which is always held during manipulation of the updates.
     * We keep migrate out by holding the MigStgLock and the only
     * other candidates are the log and the root tag dir which
     * have protection via and exclusive ftx.
     */

    sts = remove_stg( bfap,
                      fob_offset,
                      fob_cnt,
                      xtnts,
                      ftx,
                      &del_fob_cnt,
                      delCnt,
                      (mcellPtrT **)delList,
                      force_alloc);
 
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    if (del_fob_cnt == 0) {
        RAISE_EXCEPTION (EOK);
    }

    if (flags & APP_SNAP_LOCK_HELD || (locked_snap_lock == TRUE)) {
        fs_priv_param = 
            (struct advfs_pvt_param *)ms_malloc(sizeof(struct advfs_pvt_param));
        fs_priv_param->app_flags = (pvt_param_flags_t)flags;
        if (locked_snap_lock == TRUE) {
            fs_priv_param->app_flags |= APP_SNAP_LOCK_HELD;
        }
    }

    /*
     * Invalidate the cache pages for the storage that was removed.
     * If fob_offset is not zero, round fob_offset up to the next VM page.  
     * Usually, this rounding has no effect.  But in the case of a file 
     * with a small allocation unit being truncated from, say, 12k to 2k, 
     * we do not want to invalidate the first page of the file.  
     * fs_setattr() will make sure that the now-unbacked part of the 
     * VM page is zero-filled in this case.  If fob_offset is zero,
     * we round up fob_cnt to be a multiple of VM_PAGE_SZ.  Again,
     * this is usually a no-op but in the case of small files, if
     * we're truncating from 3k to zero, for example, fob_cnt comes in
     * as 3k so we round up to VM_PAGE_SZ.
     */
    if (fob_offset) {
        inval_offset = roundup(fob_offset * ADVFS_FOB_SZ, VM_PAGE_SZ);
        inval_size   = rounddown(fob_cnt * ADVFS_FOB_SZ, VM_PAGE_SZ);
    }
    else {
        inval_offset = fob_offset;
        inval_size   = roundup(fob_cnt * ADVFS_FOB_SZ, VM_PAGE_SZ);
    }

    if (inval_size) {
        MS_SMP_ASSERT(inval_offset % NBPG == 0 && inval_size   % NBPG == 0);
        (void)fcache_vn_invalidate(&bfap->bfVnode, 
                                   inval_offset,
                                   inval_size,
                                   (uintptr_t)fs_priv_param, 
                                   (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL);
    }

    if (flags & APP_SNAP_LOCK_HELD || (locked_snap_lock == TRUE)) {
        ms_free(fs_priv_param);
    }

    /* It's okay to unlock the snap lock after calling into VM */
    if (locked_snap_lock == TRUE) {
        ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    }

    if (relQuota) {
        (void) fs_quota_blks( &bfap->bfVnode,
                              -(QUOTABLKS(del_fob_cnt * ADVFS_FOB_SZ)),
                              ftx);
    }

    ADVRWL_XTNTMAP_WRITE(&(bfap->xtntMap_lk));
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);

    merge_xtnt_maps (xtnts);

    if (BS_BFTAG_REG(bfap->tag) && (bfap->bfaFlags & BFA_FRAGMENTED) &&
        !(ss_bfap_fragmented(bfap))) {
        sts = advfs_bs_tagdir_update_tagmap(bfap->tag,
                                            bfap->bfSetp,
                                            BS_TD_FRAGMENTED,
                                            bsNilMCId,
                                            ftx,
                                            TDU_UNSET_FLAGS);
        if (sts != EOK) {
            ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
            ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
            RAISE_EXCEPTION(sts);
        }
#ifdef SS_DEBUG
        if (ss_debug) {
            printf("vfast-%s CLR fragmented -> tag(%ld)\n",
                      bfap->dmnP->domainName, bfap->tag.tag_num);
        }
#endif
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        bfap->bfaFlags &= ~BFA_FRAGMENTED;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

    xtnts->bimxAllocFobCnt = xtnts->bimxAllocFobCnt - del_fob_cnt;

    if ((fob_offset + fob_cnt) == bfap->bfaNextFob) {
        bfap->bfaNextFob = imm_get_next_fob (xtnts);
    }
    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_283);
    ftx_done_u (ftx, FTA_BS_STG_REMOVE_V1, 0, 0);
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_284);
    
    return EOK;

HANDLE_EXCEPTION:

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_469);
    ftx_fail (ftx);
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_470);

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
    uint32_t delCnt,    /* in */
    void *delList       /* in */
    )
{
    uint32_t i;
    statusT sts;
    mcellPtrT *dList = (mcellPtrT *)delList;

    for (i = 0; i < delCnt; i++) {
        sts = del_dealloc_stg( dList[i].mcellId,
                               VD_HTOP(dList[i].mcellId.volume, dmnP));
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
 * This function removes storage starting at the fob offset for the fob count.
 * The fob range can be anywhere in the bitfile.
 *
 * This function assumes the caller owns the mcell list lock.
 */

static
statusT
remove_stg (
            bfAccessT *bfap,       /* in */
            bf_fob_t fob_offset,  /* in */
            bf_fob_t fob_cnt,     /* in */
            bsInMemXtntT *xtnts,   /* in, modified */
            ftxHT parentFtx,       /* in */
            bf_fob_t *del_fob_cnt,   /* out */
            uint32_t *delCnt,       /* out */
            mcellPtrT **delList,    /* out */
            int32_t force_alloc        /* in - force alloc in bmt_alloc_mcell */
            )
{
    uint32_t i;
    int mapIndex;
    uint32_t mcellCnt;
    mcellPtrT *mcellList = NULL;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t total_fob_cnt;
    bsInMemXtntMapT *xtntMap;

        xtntMap = xtnts->xtntMap;
        xtntMap->updateStart = xtntMap->cnt;

        mcellCnt = 1;
        mcellList = (mcellPtrT *) ms_malloc (mcellCnt * sizeof (mcellPtrT));

        sts = dealloc_stg( bfap,
                           fob_offset,
                           fob_cnt,
                           xtntMap,
                           parentFtx,
                           &total_fob_cnt,
                           &(mcellList[0].mcellId),
                           force_alloc);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

    *del_fob_cnt = total_fob_cnt;
    if (total_fob_cnt == 0) {
        RAISE_EXCEPTION (EOK);
    }
    *delCnt = mcellCnt;
    *delList = mcellList;

    return EOK;

HANDLE_EXCEPTION:

    /*
     * Run through the updated extent maps and unload the sub extent maps.
     */

        unload_sub_xtnt_maps (xtnts->xtntMap);

    if (mcellList != NULL) {
        ms_free (mcellList);
    }

    return sts;

}  /* end remove_stg */

/*
 * dealloc_stg
 *
 * This function removes storage starting at the fob offset for the fob count.
 * The fob range can be anywhere in the bitfile.
 *
 * This function assumes the caller owns the mcell list lock.
 */

static
statusT
dealloc_stg (
             bfAccessT *bfap,          /* in  */
             bf_fob_t fob_offset,      /* in  */
             bf_fob_t fob_cnt,         /* in  */
             bsInMemXtntMapT *xtntMap, /* in  */
             ftxHT parentFtx,          /* in  */
             bf_fob_t *del_fob_cnt,    /* out */
             bfMCIdT *delMcellId,      /* out */
             int32_t force_alloc      /* in - force alloc in bmt_alloc_mcell */
             )
{
    bfSetT *bfSetp;
    uint32_t cnt;
    bsInMemXtntMapT *delXtntMap =  NULL;
    int emptyFlag = 0;
    uint32_t i;
    bfMCIdT mcellId, newMcellId;
    bf_fob_t rem_fob_cnt;
    bf_fob_t rem_fob_offset;
    uint32_t reuseIndex;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemSubXtntMapT *sxm0, *sxm1, *sxmN;
    bf_fob_t total_fob_cnt;

    /*
     * Determine if there are any fobs to deallocate.
     */
    sts = imm_get_alloc_fob_cnt( xtntMap,
                                  fob_offset,
                                  fob_cnt,
                                  &total_fob_cnt);

    if (sts != EOK) {
        return sts;
    }

    if (total_fob_cnt == 0) {
        *del_fob_cnt = total_fob_cnt;
        return EOK;
    }

    bfSetp = bfap->bfSetp;

    

    /*  If domain has a lower threshold set */
    if (bfap->dmnP->dmnThreshold.threshold_lower_limit > 0) {

        sts = advfs_bs_check_threshold( bfap,
                                        total_fob_cnt,
                                        DOMAIN_LOWER,
                                        parentFtx);
        /*
         * Ignore failure in non-debug kernel.  We cannot fail the
         * truncation because of failing threshold checking.
         */
        MS_SMP_ASSERT(sts==EOK);
    } 

    /*  If fileset has a lower threshold set and it's mounted */
    if ((bfSetp->fsetThreshold.threshold_lower_limit > 0) &&
        (bfap->bfSetp->fsnp)) {

        sts = advfs_bs_check_threshold( bfap,
                                        total_fob_cnt,
                                        FSET_LOWER,
                                        parentFtx);
        /*
         * Ignore failure in non-debug kernel.  We cannot fail the
         * truncation because of failing threshold checking.
         */
        MS_SMP_ASSERT(sts==EOK);
    } 

    /*
     * Remove the mappings for the fob range from the in-memory extent
     * map.
     */

    sts = imm_remove_range_map( bfap,
                                fob_offset,
                                fob_cnt,
                                xtntMap,
                                &rem_fob_offset,
                                &rem_fob_cnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if (xtntMap->updateStart == 0) 
    {
        /*
         * If we return from the above routine with no updates
         * then we must be truncating the file. This does not include 
         * truncating all storage.
         */

        emptyFlag = 1;
    }

    /*
     * Create the delete extent map and copy the original sub extent maps
     * that describe the to-be-modified range.  These are exact copies,
     * including the mcell pointers.
     */

    cnt = (xtntMap->origEnd - xtntMap->origStart) + 1;
    sts = imm_create_xtnt_map( 
                               xtntMap->domain,
                               cnt,
                               &delXtntMap
                             );
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
        if(subXtntMap->mcellId.volume != xtntMap->subXtntMap[xtntMap->origStart].mcellId.volume)
        {
            /* We can not reuse the first MCELL. We know that the origStart
             * is not the PRIMARY otherwise the indexes would have matched
             */

            reuseIndex=0;
            MS_SMP_ASSERT((xtntMap->subXtntMap[xtntMap->origStart].type != BSR_XTNTS))
        }
        else
        {
            /* We can reuse the first sub extent */
            reuseIndex=1;

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
                                       bfSetp->dirTag,
                                       bfap->tag,
                                       (vdIndexT) subXtntMap->mcellId.volume,
                                       parentFtx,
                                       &newMcellId,
                                       force_alloc);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            MS_SMP_ASSERT(subXtntMap->mcellId.volume == newMcellId.volume);
            subXtntMap->mcellId = newMcellId;
            subXtntMap->mcellState = ADV_MCS_NEW_MCELL;
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
                                       bfSetp->dirTag,
                                       bfap->tag,
                                       (vdIndexT) subXtntMap->mcellId.volume,
                                       parentFtx,
                                       &newMcellId,
                                       force_alloc);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            MS_SMP_ASSERT(subXtntMap->mcellId.volume == newMcellId.volume);
            subXtntMap->mcellId = newMcellId;

            sts = odm_create_xtnt_rec( bfap,
                                       bfap->bfSetp,
                                       subXtntMap,
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
            /* Tell imm_extend_xtnt_map we are working on the in place
             * xtntmap so lock the xtntMap_lk before swapping memory.
             */
            sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);
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

        /* Manipulating this without the xtntmap lock for write should be
         * ok since no one reading the extent maps will use this.
         */

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
     * Update the original map minus the deleted fob range.
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
                             rem_fob_offset,
                             rem_fob_cnt,
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
                                sxm0->mcellId,
                                sxm1->mcellId,
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
        mcellId = sxm0->mcellId;
    }
    else sts = create_xtnt_map_hdr ( bfap,
                                     bfSetp,
                                     bfap->tag,
                                     delXtntMap->subXtntMap[0].mcellId,
                                     delXtntMap->subXtntMap[delXtntMap->cnt-1].mcellId,
                                     parentFtx,
                                     &mcellId);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = del_add_to_del_list( mcellId,
                               VD_HTOP(mcellId.volume, bfap->dmnP),
                               TRUE,  /* start sub ftx */
                               parentFtx);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_delete_xtnt_map (delXtntMap);

    *del_fob_cnt = total_fob_cnt;
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
 * This function determines if the starting fob is the first fob 
 * mapped by the first sub extent map and the last fob is the last
 * fob mapped by the last sub extent map.  If not, the on-disk and
 * in-memory extent maps are modified and the changes saved to disk.
 *
 * This function is needed because we don't want to put storage information
 * not in the fob range on the delete pending list.
 */

static
statusT
clip_del_xtnt_map (
                   bfAccessT *bfap,           /* in */
                   bf_fob_t fob_offset,      /* in */
                   bf_fob_t fob_cnt,         /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   ftxHT parentFtx            /* in */
                   )
{
    uint32_t            descIndex;
    bf_fob_t            end_fob_offset;
    bsXtntT             firstDesc;
    uint32_t            i;
    bsXtntT             lastDesc;
    uint32_t            moveCnt;
    statusT             sts;
    bsInMemSubXtntMapT *subXtntMap;
    int                 updateFlag = 0;

    end_fob_offset = (fob_offset + fob_cnt) - 1;

    /*
     * Check for start fob alignment.
     */

    subXtntMap = &(xtntMap->subXtntMap[0]);
    if (fob_offset != subXtntMap->bsXA[0].bsx_fob_offset) {

        sts = imm_fob_to_xtnt(  fob_offset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_STG_HOLE */
                                &descIndex);
        if ((sts != EOK) && (sts != E_STG_HOLE)) {
            return sts;
        }

        if (fob_offset != subXtntMap->bsXA[descIndex].bsx_fob_offset) {
            imm_split_desc( fob_offset,
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
             * the extent descriptor that maps the first fob is the first
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

        subXtntMap->bssxmFobOffset = subXtntMap->bsXA[0].bsx_fob_offset;
        subXtntMap->bssxmFobCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
          subXtntMap->bssxmFobOffset;

        xtntMap->updateStart = 0;
        xtntMap->updateEnd = xtntMap->updateStart;
    }

    /*
     * Check for end fob alignment.
     */

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
    if ( end_fob_offset < (subXtntMap->bsXA[0].bsx_fob_offset)) 
    {
        /* This is a case where the originalEnd range has no fobs 
         * in the removal range. This is because the hole being created
         * needs to stick to the storage in this subextent. This made it
         * onto the delete list since the MCELL must go but not any of
         * the fobs.
         * Mark the range as a one entry termination descriptor with
         * no corresponding storage.
         * NOTE:
         * This is one of the few places that a subextent can contain
         * only a termination descriptor
         */

        subXtntMap->bsXA[0].bsx_vd_blk = -1;
        subXtntMap->cnt = 1;

        subXtntMap->bssxmFobOffset = subXtntMap->bsXA[0].bsx_fob_offset;
        subXtntMap->bssxmFobCnt = 0;

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

    if ( end_fob_offset != (subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset - 1)) 
    {
        sts = imm_fob_to_xtnt( end_fob_offset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_STG_HOLE */
                                &descIndex);
        if ((sts != EOK) && (sts != E_STG_HOLE)) {
            return sts;
        }

        if ( end_fob_offset != (subXtntMap->bsXA[descIndex + 1].bsx_fob_offset - 1)) {
            imm_split_desc( end_fob_offset + 1,
                            &(subXtntMap->bsXA[descIndex]),  /* orig */
                            &firstDesc,
                            &lastDesc);
            subXtntMap->bsXA[descIndex + 1].bsx_fob_offset = lastDesc.bsx_fob_offset;
        }
        subXtntMap->bsXA[descIndex + 1].bsx_vd_blk = -1;
        subXtntMap->cnt = descIndex + 2;
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);

        subXtntMap->bssxmFobOffset = subXtntMap->bsXA[0].bsx_fob_offset;
        subXtntMap->bssxmFobCnt = subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
          subXtntMap->bssxmFobOffset;

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

void
merge_xtnt_maps (
                     bsInMemXtntT *xtnts  /* in */
                     )
{
    uint32_t i;
    uint32_t moveCnt;
    uint32_t newOrigStart;
    uint32_t newSaveEnd;
    uint32_t origCnt;
    uint32_t origStart;
    uint32_t saveCnt;
    uint32_t saveEnd;
    uint32_t updateCnt;
    uint32_t updateStart;
    uint32_t growth;
    bsInMemXtntMapT *xtntMap;

    xtntMap = xtnts->xtntMap;

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
    xtntMap->bsxmNextFob =
                        (xtntMap->subXtntMap[xtntMap->validCnt - 1].bssxmFobOffset +
                           xtntMap->subXtntMap[xtntMap->validCnt - 1].bssxmFobCnt) -
                                xtntMap->subXtntMap[0].bssxmFobOffset;

    MS_SMP_ASSERT(imm_check_xtnt_map(xtntMap) == EOK);

    return;

}  /* end merge_xtnt_maps */

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
void
unload_sub_xtnt_maps (
                      bsInMemXtntMapT *xtntMap  /* in */
                      )
{
    uint32_t i;

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

/*
 * advfs_bs_check_threshold
 *
 * This function checks for upper and lower threshold crossing on both
 * domains and filesets.
 *
 */

statusT
advfs_bs_check_threshold(
                         bfAccessT *bfap,       /* in */
                         bf_fob_t fob_cnt,      /* in */
                         thresholdType_t type,  /* in */
                         ftxHT parentFtx        /* in */
                )
{
    statusT sts = EOK;
    bfSetT  *bfSetp;
    struct  timeval tv;
    time_t  currentTime;

    ftxHT   subFtxH;
    thresholdAlertRtDnRec_t rootdoneRec;

    uint32_t dmnThresholdLimit,
             dmnEventInterval,
             dmnTimeOfLastEvent;

    uint32_t fsThresholdLimit,
             fsEventInterval,
             fsTimeOfLastEvent;

    uint64_t totalDmnBlks;

    uint64_t dmnBlksUsed,
             totalFsBlksUsed;

    uint64_t potDmnBlksUsed,
             potFsBlksUsed;

    uint32_t percentDmnUsed,
             percentFsBlksUsed;

    uint32_t percentPotDmnUsed,
             percentPotFsBlksUsed;

    bfSetp = bfap->bfSetp;

    tv = get_system_time();

    currentTime = tv.tv_sec; 

    totalDmnBlks   = bfap->dmnP->totalBlks;
    dmnBlksUsed    = totalDmnBlks - bfap->dmnP->freeBlks;
    percentDmnUsed = ((dmnBlksUsed * 100) / totalDmnBlks);

    switch (type) {

     case DOMAIN_UPPER:

       /* Potential values if the caller (add_stg) was to succede */
       potDmnBlksUsed     = dmnBlksUsed + (fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE);
       percentPotDmnUsed  = ((potDmnBlksUsed * 100) / totalDmnBlks);

       dmnThresholdLimit  = bfap->dmnP->dmnThreshold.threshold_upper_limit;
       dmnEventInterval   = bfap->dmnP->dmnThreshold.upper_event_interval;
       dmnTimeOfLastEvent = bfap->dmnP->dmnThreshold.time_of_last_upper_event;

       /* If we are crossing the threshold and the interval has expired */
       if ((percentDmnUsed     < dmnThresholdLimit) &&
           (percentPotDmnUsed >= dmnThresholdLimit) &&
           ((currentTime - dmnTimeOfLastEvent) > dmnEventInterval) ) {

           sts = FTX_START_N(FTA_ADVFS_THRESHOLD_EVENT,
                             &subFtxH,
                             parentFtx,
                             bfap->dmnP
                            );

           rootdoneRec.type = type;

           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_285);
           ftx_done_urd(subFtxH,
                        FTA_ADVFS_THRESHOLD_EVENT,
                        0,
                        NULL,
                        sizeof (rootdoneRec),
                        &rootdoneRec
                       );
           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_286);
       }

       break;

     case DOMAIN_LOWER:

       /* Potential values if the storage dealloc was to succede */
       potDmnBlksUsed     = dmnBlksUsed - (fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE);
       percentPotDmnUsed  = ((potDmnBlksUsed * 100) / totalDmnBlks);

       dmnThresholdLimit  = bfap->dmnP->dmnThreshold.threshold_lower_limit;
       dmnEventInterval   = bfap->dmnP->dmnThreshold.lower_event_interval;
       dmnTimeOfLastEvent = bfap->dmnP->dmnThreshold.time_of_last_lower_event;

       /* If we are crossing the threshold and the interval has expired */
       if ( (percentDmnUsed     > dmnThresholdLimit) &&
            (percentPotDmnUsed <= dmnThresholdLimit) &&
            ((currentTime - dmnTimeOfLastEvent) > dmnEventInterval) ) {

           sts = FTX_START_N(FTA_ADVFS_THRESHOLD_EVENT,
                             &subFtxH,

                            /* Trunc path via dealloc_stg gives us
                             * a parentFtx.  Rm's via bs_close_one
                             * do not happen in the context of a 
                             * parentFtx, so parentFtx passed in
                             * will be FtxNilFtxH.
                             */
                             parentFtx,
                             bfap->dmnP
                            );

           rootdoneRec.type = DOMAIN_LOWER;

           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_287);
           ftx_done_urd(subFtxH,
                        FTA_ADVFS_THRESHOLD_EVENT,
                        0,
                        NULL,
                        sizeof (rootdoneRec),
                        &rootdoneRec
                       );
           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_288);
       }       

       break;

     case FSET_UPPER:

       totalFsBlksUsed = ((fileSetNodeT*)bfSetp->fsnp)->blksUsed;
       potFsBlksUsed   = totalFsBlksUsed + (fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE);

       percentFsBlksUsed    = (totalFsBlksUsed * 100) / totalDmnBlks;
       percentPotFsBlksUsed = (potFsBlksUsed   * 100) / totalDmnBlks;

       fsThresholdLimit  = bfSetp->fsetThreshold.threshold_upper_limit;
       fsEventInterval   = bfSetp->fsetThreshold.upper_event_interval;
       fsTimeOfLastEvent = bfSetp->fsetThreshold.time_of_last_upper_event;

       if ((percentFsBlksUsed     < fsThresholdLimit) &&
           (percentPotFsBlksUsed >= fsThresholdLimit) &&
           ((currentTime - fsTimeOfLastEvent) > fsEventInterval) ) {

           sts = FTX_START_N(FTA_ADVFS_THRESHOLD_EVENT,
                             &subFtxH,
                             parentFtx,
                             bfap->dmnP
                            );
           rootdoneRec.type    = type;
           rootdoneRec.bfSetID = bfSetp->bfSetId;

           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_289);
           ftx_done_urd(subFtxH,
                        FTA_ADVFS_THRESHOLD_EVENT,
                        0,
                        NULL,
                        sizeof (rootdoneRec),
                        &rootdoneRec
                       );
           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_290);
       }

       break;

     case FSET_LOWER:

       totalFsBlksUsed = ((fileSetNodeT*)bfSetp->fsnp)->blksUsed;
       potFsBlksUsed   = totalFsBlksUsed - (fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE);

       percentFsBlksUsed    = (totalFsBlksUsed * 100) / totalDmnBlks;
       percentPotFsBlksUsed = (potFsBlksUsed   * 100) / totalDmnBlks;

       fsThresholdLimit  = bfSetp->fsetThreshold.threshold_lower_limit;
       fsEventInterval   = bfSetp->fsetThreshold.lower_event_interval;
       fsTimeOfLastEvent = bfSetp->fsetThreshold.time_of_last_lower_event;

       if ((percentFsBlksUsed     > fsThresholdLimit) &&
           (percentPotFsBlksUsed <= fsThresholdLimit) &&
           ((currentTime - fsTimeOfLastEvent) > fsEventInterval) ) {

           sts = FTX_START_N(FTA_ADVFS_THRESHOLD_EVENT,
                             &subFtxH,

                            /* Trunc path via dealloc_stg gives us
                             * a parentFtx.  Rm's via bs_close_one
                             * do not happen in the context of a 
                             * parentFtx, so parentFtx passed in
                             * will be FtxNilFtxH.
                             */
                             parentFtx,
                             bfap->dmnP
                            );
           rootdoneRec.type    = type;
           rootdoneRec.bfSetID = bfSetp->bfSetId;

           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_291);
           ftx_done_urd(subFtxH,
                        FTA_ADVFS_THRESHOLD_EVENT,
                        0,
                        NULL,
                        sizeof (rootdoneRec),
                        &rootdoneRec
                       );
           ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_292);
       }

       break;

     default:
       return EBAD_PARAMS;
    }


    return sts;
}
