
/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991, 1992, 1993    *
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
 *  ADVFS Storage System
 *
 * Abstract:
 *
 *  ftx_recovery
 *  This module contains the recovery routines.
 *
 * Date:
 *
 *  Broken out from ftx_routines.c November 7, 1991.
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: ftx_recovery.c,v $ $Revision: 1.1.159.2 $ (DEC) $Date: 2004/09/16 20:38:54 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ftx_privates.h>
#include <msfs/bs_domain.h>
#include <msfs/bs_public.h>
#include <sys/lock_probe.h>
#include <msfs/ms_osf.h>
#include <sys/clu.h>

#define ADVFS_MODULE FTX_RECOVERY

int cfs_list(domainT *dmnP, ftxIdT ftxId, int done_flag);

/**********************************************
 *  module private typedefs ******************
**********************************************/

typedef struct {
    ftxRecRedoT pgdesc;         /* page descriptor */
    ftxRecXT recX[FTX_MX_PINR]; /* record extent list */
} pageredoT;

extern unsigned TrFlags;
extern mutexT FtxMutex;
extern ftxAgentT FtxAgents [FTX_MAX_AGENTS];
extern struct ftx_dyn_alloc FtxDynAlloc;


/********************************************
 ******** Record image recovery *************
 *******************************************/

/*
 * TODO - function/arg comments
 */
static statusT
ftx_metameta_rec_redo(
                    domainT* dmnP,
                    pageredoT* pgredop
                    )
{
    char* srcRecp;
    char* dstRecp;
    bfAccessT *bfap;
    statusT sts;
    bfPageRefHT pgH;
    unsigned ptag = -(signed)pgredop->pgdesc.tag.num;
    int rbmtidx, recxi, vdi;
    vdT* vdp;
    bsMPgT* rbmtpgp;

    if ( !RBMT_THERE(dmnP) ) {
        domain_panic(dmnP,
                     "ftx_metameta_rec_redo: Domain \"%s\". Bad domain version",
                     dmnP->domainName);
        return E_DOMAIN_PANIC;
    }

    rbmtidx = ptag % BFM_RSVD_TAGS;
    MS_SMP_ASSERT(rbmtidx == BFM_RBMT );

    vdi = ptag / BFM_RSVD_TAGS;
    /* During recovery we don't need to worry about the vdp being removed,
     * so don't bump the vdRefCnt on this call.
     */
    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) ) {
        domain_panic(dmnP,
            "ftx_metameta_rec_redo: Domain \"%s\". Bad volume index (%d).",
            dmnP->domainName, vdi);
        return E_DOMAIN_PANIC;
    }
    bfap = vdp->rbmtp;

    sts = bs_pinpg_int( &pgH, (void *)&rbmtpgp, bfap,
                        pgredop->pgdesc.page, BS_NIL, PINPG_NOFLAG);
    if ( sts != EOK ) {
        domain_panic(dmnP,
            "ftx_metatmeta_rec_redo: Domain \"%s\", volume %d, RBMT page %d. bs_pinpg_int error (%d).",
            dmnP->domainName, vdi, pgredop->pgdesc.page, sts);
        return E_DOMAIN_PANIC;
    }

    if ( rbmtpgp->pageId != pgredop->pgdesc.page ) {
        domain_panic(dmnP,
            "ftx_metameta_rec_redo: Domain \"%s\", volume %d, RBMT page %d. Bad pageId (%d).",
            dmnP->domainName, vdi, pgredop->pgdesc.page, rbmtpgp->pageId);
        (void)bs_unpinpg( pgH, logNilRecord, BS_CLEAN );
        return E_DOMAIN_PANIC;
    }

    srcRecp = (char*)pgredop + sizeof(ftxRecRedoT) +
              pgredop->pgdesc.numXtnts*sizeof(ftxRecXT);

    for ( recxi = 0; recxi < pgredop->pgdesc.numXtnts; recxi++) {

        /* adjust src pointer for odd byte offset */

        srcRecp += pgredop->recX[recxi].pgBoff & 3;
        
        dstRecp = (char*)rbmtpgp + pgredop->recX[recxi].pgBoff;

        bcopy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

        srcRecp = (char*)(((long)( srcRecp + pgredop->recX[recxi].bcnt + 3) >> 2) << 2);
    }

    sts = bs_unpinpg( pgH, logNilRecord, BS_DIRTY );
    if ( sts != EOK ) {
        domain_panic(dmnP,
          "ftx_metameta_rec_redo: Domain \"%s\", volume %d, RBMT page %d. bs_unpinpg error (%d).",
          dmnP->domainName, vdi, pgredop->pgdesc.page, sts);
        return E_DOMAIN_PANIC;
    }

    return EOK;
}

/*
 * TODO - function/arg comments
 */
static statusT
ftx_bfmeta_rec_redo(
                    domainT* dmnP,
                    pageredoT* pgredop
                    )
{
    char* srcRecp;
    char* dstRecp;
    bfAccessT *bfap;
    statusT sts;
    bfPageRefHT pgH;
    void* bpp;
    unsigned ptag = -(signed)pgredop->pgdesc.tag.num;
    int bmtidx;
    int recxi;

    bmtidx = ptag % BFM_RSVD_TAGS;

    if ( bmtidx == BFM_BFSDIR ) {
        bfap = dmnP->bfSetDirAccp;
    } else {
        int vdi;
        vdT* vdp;

        vdi = ptag / BFM_RSVD_TAGS;
        vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE);
        /* We got this vdi from the disk (a log record). */
        /* We need to check (and don't trust) values read from disk. */
        if ( vdp == NULL ) {
            domain_panic(dmnP,
               "ftx_bfmeta_rec_redo: Domain \"%s\". Bad volume index (%d).",
               dmnP->domainName, vdi);
            return E_DOMAIN_PANIC;
        }

        if ( RBMT_THERE(dmnP) && (bmtidx == BFM_RBMT) ) {
            bfap = vdp->rbmtp;
        } else if ( RBMT_THERE(dmnP) && (bmtidx == BFM_BMT) ) {
            bfap = vdp->bmtp;
        } else if ( (!RBMT_THERE(dmnP)) && (bmtidx == BFM_BMT_V3) ) {
            bfap = vdp->bmtp;
        } else if ( bmtidx == BFM_SBM ) {
            bfap = vdp->sbmp;
        } else {
            domain_panic(dmnP,
              "ftx_bfmeta_rec_redo: Domain \"%s\". Bad mcell ID (%d).",
              dmnP->domainName, bmtidx);
            return E_DOMAIN_PANIC;
        }
    }

    sts = bs_pinpg_int( &pgH, &bpp, bfap, pgredop->pgdesc.page, BS_NIL, PINPG_NOFLAG );

    if ( sts != EOK ) {
        if ( bmtidx == BFM_BFSDIR ) {
            domain_panic(dmnP,
              "ftx_bfmeta_rec_redo: Domain \"%s\", root TAG page %d. bs_pinpg_int error (%d).",
              dmnP->domainName, pgredop->pgdesc.page, sts);
        } else {
            domain_panic(dmnP,
              "ftx_bfmeta_rec_redo: Domain \"%s\", volume %d, %s page %d. bs_pinpg_int error (%d).",
              dmnP->domainName, ptag / BFM_RSVD_TAGS,
              RBMT_THERE(dmnP) && (bmtidx == BFM_RBMT) ? "RBMT" : "BMT",
              pgredop->pgdesc.page, sts);
        }
        return E_DOMAIN_PANIC;
    }

    if ( (RBMT_THERE(dmnP) && (bmtidx == BFM_BMT)) || 
         (!RBMT_THERE(dmnP) && (bmtidx == BFM_BMT_V3)) ||
         (RBMT_THERE(dmnP) && (bmtidx == BFM_RBMT)) )
    {
        bsMPgT* bmtp = (bsMPgT*)bpp;

        if ( bmtp->pageId != pgredop->pgdesc.page ) {
            domain_panic(dmnP,
               "ftx_bfmeta_rec_redo: Domain \"%s\", volume %d, %s page %d. Bad pageId (%d).",
               dmnP->domainName, ptag / BFM_RSVD_TAGS,
               RBMT_THERE(dmnP) && (bmtidx == BFM_RBMT) ? "RBMT" : "BMT",
               pgredop->pgdesc.page, bmtp->pageId);
            (void)bs_unpinpg( pgH, logNilRecord, BS_CLEAN );
            return E_DOMAIN_PANIC;
        }
    }

    srcRecp = (char*)pgredop + sizeof(ftxRecRedoT) +
              pgredop->pgdesc.numXtnts*sizeof(ftxRecXT);

    for ( recxi = 0; recxi < pgredop->pgdesc.numXtnts; recxi++) {

        /* adjust src pointer for odd byte offset */

        srcRecp += pgredop->recX[recxi].pgBoff & 3;
        
        dstRecp = (char*)bpp + pgredop->recX[recxi].pgBoff;

        bcopy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

        srcRecp = (char*)(((long)( srcRecp + pgredop->recX[recxi].bcnt + 3) >> 2) << 2);
    }

    sts = bs_unpinpg( pgH, logNilRecord, BS_DIRTY );
    if ( sts != EOK ) {
        domain_panic(dmnP, "ftx_bfmeta_rec_redo: bs_unpinpg error (%d).", sts);
        return E_DOMAIN_PANIC;
    }
    return EOK;
}

/*
 * TODO - function/arg comments
 */
static statusT
ftx_bfdata_rec_redo(
                    ftxHT ftxH,
                    bfSetIdT bfSetId,
                    pageredoT* pgredop
                    )
{
    char* srcRecp;
    char* dstRecp;
    bfAccessT *bfap;
    statusT sts;
    statusT retsts = EOK;
    bfPageRefHT pgH;
    void* bpp;
    bfSetT *bfSetp;
    int recxi;
    struct vnode *nullvp = NULL;

    /*
     * Because the bitfile metadata is completely recovered in the
     * first pass, it is quite possible that a bitfile set or bitfile
     * was deleted after being written to originally.  It is also
     * possible that storage had been deallocated later.  So if any of
     * these errors occur, just dismiss them.
     */

    sts = rbf_bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if ( sts != EOK ) {
        return EOK;
    }

    sts = bs_access(&bfap, pgredop->pgdesc.tag, bfSetp, ftxH, 0, NULLMT, 
                    &nullvp );
    if ( sts != EOK ) {
        goto close_bfs;
    }

    /*
     * Data logging may have been turned off for this bitfile.  Only
     * do redo if still enabled.
     */
    if ((bfap->dataSafety != BFD_FTX_AGENT) && 
        (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY)) {
        goto close_bf;
    }

    sts = bs_pinpg_int( &pgH, &bpp, bfap, pgredop->pgdesc.page, BS_NIL, PINPG_NOFLAG);
    if ( sts != EOK ) {
        if ( sts != E_PAGE_NOT_MAPPED ) {
            domain_panic(ftxH.dmnP,
                "ftx_bfdata_rec_redo: bs_pinpg_int error = %d.", sts);
            retsts = E_DOMAIN_PANIC;
        }
        goto close_bf;
    }

    srcRecp = (char*)pgredop + sizeof(ftxRecRedoT) +
              pgredop->pgdesc.numXtnts*sizeof(ftxRecXT);

    for ( recxi = 0; recxi < pgredop->pgdesc.numXtnts; recxi++) {

        /* adjust src pointer for odd byte offset */

        srcRecp += pgredop->recX[recxi].pgBoff & 3;
        
        dstRecp = (char*)bpp + pgredop->recX[recxi].pgBoff;

        bcopy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

        srcRecp = (char*)(((long)( srcRecp + pgredop->recX[recxi].bcnt + 3) >> 2) << 2);
    }

    sts = bs_unpinpg( pgH, logNilRecord, BS_DIRTY );
    if ( sts != EOK ) {
        domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_unpinpg error = %d.",
                     sts);
        retsts = E_DOMAIN_PANIC;
    }

close_bf:
    sts = bs_close(bfap, 0);
    if ( sts != EOK ) {
        domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_close error = %d.",
                     sts);
        retsts = E_DOMAIN_PANIC;
    }

close_bfs:
    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    return retsts;
}

/****************************************************
 ********  Bitfile domain crash recovery  ***********
 ***************************************************/

static statusT
ftx_recovery_pass(
                  domainT* dmnP,
                  int recoveryPass,
                  logRdHT logrh,
                  logRecAddrT logaddr
                  );

/*
 * ftx_bfdmn_recovery - recover domain consistency
 */
statusT
ftx_bfdmn_recovery(
                   domainT* dmnP
                   )
{
    statusT sts, lrsts=0;
    int wordcnt, totwordcnt, ftxSlot, incomplete;
    logRdHT logrh;
    logRecAddrT logaddr, crashRedo;
    ftxDoneLRT* dlrp;
    struct ftx* ftxp;
    extern int MountWriteDelay;
    uint32T *segptr;
    int segwordcnt;
    ftxIdT ftxId=0;
    struct timeval ltime;
    bfSetT* bfSetp;
    bfsQueueT *entry;
    
    /* open a log read stream */
    sts = lgr_read_open( dmnP->ftxLogP, &logrh );
    if (sts != EOK) {
        domain_panic(dmnP, "ftx_bfdmn_recovery: lgr_read_open error = %d.",
                     sts);
        goto freexid;
    }

    /* position to end of log */

    sts = lgr_end( dmnP->ftxLogP, &logaddr );
    if ( sts == E_LOG_EMPTY ) {  /* only return besides EOK */
        sts = EOK; /* nothing to do so allow domain to mount */
        goto logclose;
    }

    /* read the last record.  should scan back if other than ftx */
    /* records are possible in the log. */

    sts = lgr_read( LR_BWD, logrh, &logaddr, (uint32T**)&dlrp,
                   &wordcnt, &totwordcnt );
    if ( sts != EOK && (sts < MSFS_FIRST_ERR) ) {
        if ( sts == E_LOG_EMPTY ) {
            goto logclose;
        } else {
            domain_panic(dmnP, "ftx_bfdmn_recovery: lgr_read error = %d.", sts);
            goto logclose;
        }
    } else if ( wordcnt < sizeof(ftxDoneLRT)/4 ) {
        domain_panic(dmnP, "ftx_bfdmn_recovery: bad record size %d.", wordcnt);
        sts = E_DOMAIN_PANIC;
        goto logclose;
    }

    if ( !BS_UID_EQL( dmnP->domainId, dlrp->bfDmnId) ) {
        if ( !(dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) ||
             !BS_UID_EQL( dmnP->dualMountId, dlrp->bfDmnId) )
        {
            domain_panic(dmnP,
                         "ftx_bfdmn_recovery: Bad domain ID in log (%x.%x).",
                         dlrp->bfDmnId.tv_sec, dlrp->bfDmnId.tv_usec);
            sts = E_DOMAIN_PANIC;
            goto logclose;
        }
    }

    crashRedo = dlrp->crashRedo;

    /* We are done with the buffer from lgr_read */
    
    ms_free(dlrp);

    if (clu_is_ready()) {

        MS_SMP_ASSERT(dmnP->xidRecovery.head == NULL);

        /* 
         * Read the inactive log for CFS xid status
         * If we have an error, we will skip the inactive log read.
         * One reason we don't continue: issues with potentially
         * reading past crashRedo (start of active log) if on-disk
         * lsn is corrupted.
         */

        sts = lgr_dmn_get_pseudo_first_rec(dmnP, &logaddr);

        if ( sts == E_LOG_EMPTY ) {
            goto logclose;
        } else if (sts != EOK) {
            goto after_inactive;
        } 

        while (logaddr.lsn.num < crashRedo.lsn.num) {

            lrsts = lgr_read( LR_FWD, logrh, &logaddr, (uint32T**)&dlrp,
                       &wordcnt, &totwordcnt );

            if ((lrsts != EOK) && (lrsts < MSFS_FIRST_ERR)) {
                /* Skip further inactive log processing for cfs */

                if (dlrp) {
                    ms_free(dlrp);
                }
                break;
            }

            /*
             * Check dlrp just to be safe; avoid any unexpected kmf's.
             * If not good, skip further inactive log processing.
             */

            if (!dlrp) {
                aprintf("ftx_bfdmn_recovery: Domain %s: unexpected NULL buf; lgr_read sts: %d\n",
                    dmnP->domainName, lrsts);
                break;
            }

            if (ftxId != dlrp->ftxId) {
                ftxId = dlrp->ftxId;

                if (CFS_XID(ftxId)) {
                    cfs_list(dmnP, dlrp->ftxId, TRUE);
                }
            }
 
            ms_free(dlrp);

            /*
             * Here, if the current record is a segment,
             * we simply advance the pointer past the record's
             * segments to the next record.
             */

            while ( lrsts == I_LOG_REC_SEGMENT ) {
                    lrsts = lgr_read( LR_FWD,
                                      logrh,
                                      &logaddr,
                                      &segptr,
                                      &segwordcnt,
                                      &totwordcnt );

                if (segptr) {
                    ms_free(segptr);
                }

                if ((lrsts != EOK) && (lrsts < MSFS_FIRST_ERR)) {
                    /* Skip further inactive log processing for cfs */
                    break;
                } 
            } 
            /*
             * break out of main while loop 
             */
            if ((lrsts != EOK) && (lrsts < MSFS_FIRST_ERR)) {
                break;
            }
        }
    }

after_inactive:

    /*
     * Artificially set the oldest ftx log record address to the crash
     * redo log record address in the log record just read.  This will
     * have the effect of "freezing" the crash recovery log address at
     * this value for any log records written during recovery as the
     * result of undo or rootdone actions.
     *
     * It will be reset after recovery is complete.
     */

    ftx_set_oldestftxla( dmnP, crashRedo );

#ifdef ADVFS_DEBUG
    printf("\nADVFS file domain recovery starting.\n");
#endif

    if ( RBMT_THERE(dmnP) ) {
        /*********************************************/
        /* scan log to recover reserved file metadata*/
        /*********************************************/
        dmnP->state = BFD_RECOVER_REDO;
 
        sts = ftx_recovery_pass( dmnP, META_META_PASS, logrh, crashRedo );
        if ( sts != EOK ) {
            goto flushlog;
        }
    }

    /*********************************************/
    /* scan log to recover bitfile metadata only */
    /*********************************************/
    dmnP->state = BFD_RECOVER_REDO;

    sts = ftx_recovery_pass( dmnP, META_PASS, logrh, crashRedo );
    if ( sts != EOK ) {
        goto flushlog;
    }

    /******************************************/
    /* scan log again to recover bitfile data */
    /******************************************/

    dmnP->state = BFD_RECOVER_REDO;

    sts = ftx_recovery_pass( dmnP, DATA_PASS, logrh, crashRedo );
    if ( sts != EOK ) {
        goto flushlog;
    }

    /*
     * Do continuations and sanity check to make sure there are no
     * dangling trees after recovery is run.
     *
     * The noTrimCnt is set to one, so that if a log trim is called
     * for, the log will be flushed when reset_oldest_lsn is called
     * from do_ftx_continuations.
     */
    dmnP->state = BFD_RECOVER_CONTINUATIONS;
    dmnP->ftxTbld.noTrimCnt = 1;

    do {
        incomplete = 0;
        for ( ftxSlot = 0; ftxSlot < dmnP->ftxTbld.nextNewSlot; ftxSlot++ ) {
            struct ftx* ftxp = dmnP->ftxTbld.tablep[ftxSlot].ftxp;

            if ( dmnP->ftxTbld.tablep[ftxSlot].state == FTX_SLOT_BUSY ||
                 dmnP->ftxTbld.tablep[ftxSlot].state == FTX_SLOT_EXC ) {
                if ( ftxp->contDesc.agentId ) {
                    ftxHT ftxH;
                    lrDescT *lrvecp;

                    ftxH.hndl = ftxSlot + 1;
                    ftxH.level = 0;
                    ftxH.dmnP = dmnP;

                    ftxp->lrh.type = ftxDoneLR;
                    ftxp->lrh.level = 0;
                    if ( (ftxp->lrh.member = ftxp->cLvl[0].member) > 0 ) {
                        ftxp->lrh.member = 0;
                    }

                    lrvecp = &ftxp->lrdesc;
                    /* we zero out lrdesc, V3.2 paranoia only */
                    bzero(lrvecp, sizeof(lrDescT));
                    lrvecp->bfrvec[0].bufPtr = (uint32T *)&ftxp->lrh;
                    lrvecp->bfrvec[0].bufWords = (sizeof(ftxDoneLRT)+3)/4;

                    do_ftx_continuations( dmnP, ftxp, ftxH, lrvecp );

                    if ( ftxp->contDesc.agentId ) {
                        incomplete = 1;
                    } else {
                        /*
                         * Compensate for artificially stuffing
                         * noTrimCnt to 1, as ftx_free will decrement it.
                         */
                        mutex_lock( &FtxMutex );
                        ++dmnP->ftxTbld.noTrimCnt;

                        if (CFS_XID(ftxp->lrh.ftxId)) {
                            cfs_list(dmnP, ftxp->lrh.ftxId, TRUE);
                        }

                        /* mark the slot available. */
                        ftx_free( ftxSlot, &dmnP->ftxTbld );
                        reset_oldest_lsn( ftxp, dmnP, FALSE );

                        /* Now free the ftx struct allocated to the 
                         * newly-available slot.
                         */
                        ftx_free_2( ftxp );
                        mutex_unlock( &FtxMutex );
                    }
                } else {
                    domain_panic(dmnP,
                      "ftx_bfdmn_recovery: ftx table not empty");
                    sts = E_DOMAIN_PANIC;
                    goto flushlog;
                }
            }
        }        
    } while ( incomplete );

    if ( --dmnP->ftxTbld.noTrimCnt || dmnP->ftxTbld.slotUseCnt ) {
        domain_panic(dmnP,
          "ftx_bfdmn_recovery: slot counts non-zero after recovery");
        sts = E_DOMAIN_PANIC;
        goto flushlog;
    }

    /*
     * Reset oldest ftx log record address to nil.  This will be used
     * when the next log record is written.
     */
    ftx_set_oldestftxla( dmnP, logNilRecord );
    dmnP->ftxTbld.nextNewSlot = FTX_DEF_RR_SLOTS;

flushlog:
    /* 
     * If not doing write delay then flush all dirty buffers and wait for 
     * that i/o to complete 
     */
    if ( !MountWriteDelay || dmnP->dmn_panic ) {
	bs_bfdmn_flush( dmnP );
	if ( bs_bfdmn_flush_sync( dmnP ) ) {
            domain_panic(dmnP,
              "ftx_bfdmn_recovery: pinned buffers after flush");
            sts = E_DOMAIN_PANIC;
	}
    } 
   
#ifdef ADVFS_DEBUG
    printf("ADVFS file domain recovery now complete.\n\n");
#endif
logclose:
    /* close the log read stream */

    if ( lgr_read_close(logrh) != EOK ) {
        /* This probably should be an assert. The error paths in */
        /* lgr_read_close should probably all be asserts. */
        domain_panic(dmnP, "ftx_bfdmn_recovery: lgr_read_close error");
        sts = E_DOMAIN_PANIC;
    }
      
  
    /*
     * Recovery is single threaded so no locks need to be held for
     * the following, unlike bs_bfs_remove_root.  Also there will
     * be at least two sets on the set queue -- the root fileset and 
     * the activating fileset.
     */
    BFSETTBL_LOCK_WRITE( dmnP );
    mutex_lock( &dmnP->mutex );
    while( dmnP->bfSetHead.bfsQfwd != &dmnP->bfSetHead ) {
        entry = dmnP->bfSetHead.bfsQfwd;
        BFSET_DMN_REMQ( dmnP, entry );
        mutex_unlock( &dmnP->mutex );
        bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
        if (bfSetp->fsRefCnt == 0) {
            bfs_dealloc( bfSetp, TRUE );
        }    
        mutex_lock( &dmnP->mutex );
    }
    mutex_unlock( &dmnP->mutex );
    BFSETTBL_UNLOCK( dmnP );    

freexid:
    /*
     * Call cfs_xid_free_memory() after 15 minutes.
     * It should no longer be needed after that point.
     */

    if (clu_is_ready()) 
    {
        ulong domainId_cast;

        /* We are passing the actual domainId instead of a pointer
         * to it. Rather than try to cast a non-scalar just copy
         * it into a scalar container. We can not pass a pointer into
         * the domain since the domain struct could be freed by the
         * time we get to the time out routine. We can not malloc
         * a chunk either since the untimeout wouldn't have access to
         * this pointer to free it.
         */

        bcopy(&dmnP->domainId,&domainId_cast,sizeof(ulong));
        XID_RECOVERY_LOCK_WRITE(dmnP);
        TIME_READ(ltime);
        dmnP->xidRecovery.timestamp = ltime;
        timeout((void(*))cfs_xid_free_memory, (char *) domainId_cast,
            XID_TIMEOUT*hz);
        XID_RECOVERY_UNLOCK(dmnP);
    }
    return sts;
}

/*
 * ftx_bfdmn_recovery - recover domain consistency
 */
static statusT
ftx_recovery_pass(
                  domainT* dmnP,
                  int recoveryPass,         /* TODO - arg comments */
                  logRdHT logrh,
                  logRecAddrT logaddr
                  )
{
    statusT sts, lrsts;
    int segwordcnt=0, totwordcnt=0, cntRead=0;
    uint32T* lgrrecp = 0;
    struct ftx* ftxp;
    perlvlT* clvlp;
    int ftxSlot, redoloff, lrrtdnloff, rdwords;
    ftxHT ftxH;
    ftxDoneLRT* dlrp;
    domainT *newdmnP;
    logRecAddrT oldftxrec;
    ftxTblDT* ftxTDp = &dmnP->ftxTbld;

    /* access the domain to get a handle to it */

    sts = bs_domain_access( &newdmnP, dmnP->domainId, FALSE );
    if ( sts != EOK ) {
        printf("ftx_recovery_pass: cannot access domain!!\n");
        goto _error;
    }

    /**********************************************************/
    /******** Forward scan log looking for "redo" to do *******/
    /**********************************************************/

    do {
        logRecAddrT thislogaddr = logaddr;
        int lvl = 0;
        int availSlot = -1;

        lrsts = lgr_read( LR_FWD, logrh, &logaddr, (uint32T**)&dlrp,
                          &segwordcnt, &totwordcnt );

        cntRead = segwordcnt;

        if (lrsts ==  E_INVALID_REC_ADDR)  {
            logaddr.lsn.num = 0;
            continue ;
        }

        if ( ( lrsts != EOK ) && ( lrsts < MSFS_FIRST_ERR ) ) {
            domain_panic(dmnP, "ftx_recovery_pass: log read error %d", lrsts);
            sts = E_DOMAIN_PANIC;
            goto _exit;
        } else if ( lrsts == I_LOG_REC_SEGMENT  ) {
            uint32T* segptr;

            /*
             * segmented record - get the whole thing into a buffer (lgrrecp)
             * freed as dlrp at end of loop
             */
            lgrrecp = (uint32T*)ms_malloc( totwordcnt * 4 );
            /* TODO - if (lgrrecp == NULL) ... */
            bcopy( (char*)dlrp, (char*)lgrrecp, segwordcnt * 4 );

            /* Give back the memory given to us from lgr_read */
            ms_free(dlrp);

            dlrp = (ftxDoneLRT*)lgrrecp;

            do {
                lrsts = lgr_read( LR_FWD, 
                                  logrh, 
                                  &logaddr, 
                                  &segptr,
                                  &segwordcnt, 
                                  &totwordcnt );

                if (lrsts ==  E_INVALID_REC_ADDR)  {
                    logaddr.lsn.num = 0;
                } else {

                    if ( ( lrsts != EOK ) && ( lrsts < MSFS_FIRST_ERR ) ) {
                        domain_panic(dmnP,
                                     "ftx_recovery_pass: log read error %d",
                                     lrsts);
                        sts = E_DOMAIN_PANIC;
                        ms_free(dlrp);
                        goto _exit;
                    }

                    bcopy( (char*)segptr, (char*)&lgrrecp[cntRead],
                        segwordcnt*4 );

                    /* Give back the memory given to us from lgr_read */
                    ms_free(segptr);

                    cntRead += segwordcnt;
                }
            } while ( lrsts == I_LOG_REC_SEGMENT );

            if (lrsts ==  E_INVALID_REC_ADDR)  {
                   continue;
            }
        }

        if ( cntRead < totwordcnt ) {
            /* did not get entire record */
            ms_free(dlrp);
            if ( lrsts != W_LAST_LOG_REC ) {
                domain_panic(dmnP, "ftx_recovery_pass: bad log sts %d", lrsts);
                sts = E_DOMAIN_PANIC;
                goto _exit;
            } else {
                break;  /* stop processing log records */
            }
        }

        /* only "done" records are supported now (forever?) */

        if ( dlrp->type != ftxDoneLR ) {
            domain_panic(dmnP, "ftx_recovery_pass:non *done* ftx record");
            sts = E_DOMAIN_PANIC;
            ms_free(dlrp);
            goto _exit;
        }

        if ( !BS_UID_EQL( dmnP->domainId, dlrp->bfDmnId)) {
            if ( !(dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) ||
                 !BS_UID_EQL(dmnP->dualMountId, dlrp->bfDmnId) )
            {
                domain_panic(dmnP,
                             "ftx_recovery_pass: Bad domain ID in log (%x.%x).",
                             dlrp->bfDmnId.tv_sec, dlrp->bfDmnId.tv_usec);
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }
        }

        for ( ftxSlot = 0; ftxSlot < ftxTDp->nextNewSlot; ftxSlot++) {
            ftxp = ftxTDp->tablep[ftxSlot].ftxp;

            if (( ftxTDp->tablep[ftxSlot].state == FTX_SLOT_BUSY ||
                  ftxTDp->tablep[ftxSlot].state == FTX_SLOT_EXC ) &&
                ( ftxp && ftxp->lrh.ftxId == dlrp->ftxId ))
            {
                /* found entry */
                break;

            } else if (ftxTDp->tablep[ftxSlot].state == FTX_SLOT_AVAIL) {
                /* in case we don't find ent, save location of avail slot */
                availSlot = ftxSlot;
            }
        }

        if ( ftxSlot == ftxTDp->nextNewSlot ) {
            /*
             * We didn't find an existing ftx for the record just
             * read, so get one and init it for the entire ftx tree.
             */

            if ((ftxSlot = availSlot) < 0) {
                /* all slots are busy; allocate a new one */

                if (ftxTDp->nextNewSlot == FTX_MAX_FTXH) {
                    domain_panic(dmnP,
                      "ftx_recovery_pass: no more slots in ftx table");
                    sts = E_DOMAIN_PANIC;
                    ms_free(dlrp);
                    goto _exit;
                }

                ftxSlot = ftxTDp->nextNewSlot++;
            }

            /* Allocate an ftx struct; no limit check in recovery.
             * FtxMutex synchronizes updates to FtxDynAlloc counters. */
            ftxp = newftx();
            MS_SMP_ASSERT( ftxp != NULL ); /* ms_malloc waits, it never fails */

            ftxTDp->tablep[ftxSlot].ftxp = ftxp;
            mutex_lock(&FtxMutex);
            FtxDynAlloc.currAllocated++;
            if ( FtxDynAlloc.currAllocated > FtxDynAlloc.maxAllocated )
                FtxDynAlloc.maxAllocated = FtxDynAlloc.currAllocated;
            mutex_unlock(&FtxMutex);

            ftxTDp->tablep[ftxSlot].state = FTX_SLOT_BUSY;
            ++ftxTDp->slotUseCnt;
            ++ftxTDp->noTrimCnt;

            /*
             * Initialize this ftx for the entire tree.
             */
            ftxp->firstLogRecAddr = thislogaddr;
            ftxp->type = REC_REDO;
            ftxp->lrh.ftxId = dlrp->ftxId;
            ftxp->lrh.bfDmnId = dmnP->domainId;
            ftxp->lastFtxPinS = -1;
            ftxp->rootDnCnt = 0;
            ftxp->nextRDoff = 0;
            ftxp->contDesc.agentId = 0;
        }

        /*
         * Init ftx for this specific done record.
         */
        ftxp->lastLogRec = thislogaddr;
        ftxp->currLvl = lvl = abs(dlrp->level);
        clvlp = &ftxp->cLvl[lvl];
        clvlp->atomicRPass = dlrp->atomicRPass;
        clvlp->member = dlrp->member;
        clvlp->lastPinS = -1;
        clvlp->lkList = NULL;

        /* set up ftx handle for opx routines */

        ftxH.hndl = ftxSlot + 1;
        ftxH.level = lvl;
        ftxH.dmnP = dmnP;

        /*
         * Buffer continuation record.  Only the last one is used.
         */
        if ( (lvl == 0) && dlrp->contOrUndoRBcnt &&
           ((recoveryPass == META_META_PASS) || (recoveryPass == META_PASS)) )
        {
            ftxp->lrh.agentId = dlrp->agentId;
            ftxp->contDesc.agentId = 0;
            ftxp->type = ROOTDONE;

            /* ???? The manipulation of dlrp here is scary ???*/

            MS_SMP_ASSERT(sizeof(ftxDoneLRT) <= (totwordcnt<<2));

            ftx_set_continuation( ftxH, dlrp->contOrUndoRBcnt,
                                 (char*)dlrp +
                                 sizeof(ftxDoneLRT));
            ftxp->type = REC_REDO;
        }

        lrrtdnloff = (sizeof(ftxDoneLRT)>>2) + ((dlrp->contOrUndoRBcnt +
                                                 3)>>2);
        rdwords = (dlrp->rootDRBcnt + 3)>>2;

        /* ???? The manipulation of dlrp here is scary ???*/

        MS_SMP_ASSERT((lrrtdnloff) <= totwordcnt);

        /* 
         * Buffer all rootdone records on the first pass
         * through ftx_recovery_pass().
         *
         * For v4 domains, this is META_META_PASS.
         * For pre-v4 domains, this is META_PASS.
         */

        if (rdwords &&
            ((RBMT_THERE(dmnP) && (recoveryPass == META_META_PASS)) ||
             (!RBMT_THERE(dmnP) && (recoveryPass == META_PASS))))
        {
            char* recptr = (char*)dlrp + (lrrtdnloff << 2);
            int nextrdoff, i;
            int32T * dp;

            /*
             * buffer the root done record in the ftx state struct
             */

            if ( (nextrdoff = ftxp->nextRDoff + rdwords
                   + (sizeof(ftxRDHdrT)/4)) > FTX_RD_BFRSZ )
            {
                domain_panic(dmnP,
                  "ftx_recovery_pass: out of buffer room for root done");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            dp = &ftxp->rootDoneRecs[ftxp->nextRDoff];
            ((ftxRDHdrT*)dp)->atomicRPass = dlrp->atomicRPass;
            ((ftxRDHdrT*)dp)->agentId = dlrp->agentId;
            ((ftxRDHdrT*)dp)->bCnt = dlrp->rootDRBcnt;
            dp += sizeof(ftxRDHdrT)/4;
            for (i = 0; i < rdwords; i++) {
                dp[i] = ((uint32T *)recptr)[i];
            }
            ftxp->nextRDoff = nextrdoff;
            ftxp->rootDnCnt += 1;
        }

        redoloff =  lrrtdnloff + rdwords + ((dlrp->opRedoRBcnt + 3)>>2);

        /* execute all record image redo records in this record */

        while ( redoloff < totwordcnt ) {
            int i;
            pageredoT* pgredop = (pageredoT*)((char*)dlrp + (redoloff<<2));

            /* ???? The manipulation of dlrp here is scary ???*/

            MS_SMP_ASSERT((redoloff) <= totwordcnt);

            redoloff += sizeof(ftxRecRedoT)/4;
            for ( i = 0; i < pgredop->pgdesc.numXtnts; i++ ) {
                redoloff += ((pgredop->recX[i].pgBoff & 3) +
                           (pgredop->recX[i].bcnt + 3) + sizeof(ftxRecXT)) >> 2;
            }

            if ( redoloff > totwordcnt ) {
                domain_panic(dmnP,
                  "ftx_recovery_pass: log record redo size bad");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            /* call the record image redo routine */

            if ( recoveryPass == META_META_PASS ) {
                /* Records to be applied in this pass are ALWAYS marked
                 * with the correct pass #
                 */
                if ( dlrp->atomicRPass == META_META_PASS ) {
                    MS_SMP_ASSERT( BS_BFTAG_RSVD(pgredop->pgdesc.tag)  &&
                                   BS_BFTAG_RBMT(pgredop->pgdesc.tag) );
                    sts = ftx_metameta_rec_redo( dmnP, pgredop );
                    if ( sts != EOK ) {
                        ms_free(dlrp);
                        goto _exit;
                    }
                }
            } else if ( recoveryPass == META_PASS ) {
                /* Any record to be applied to metadata files that is
                 * not explicitly done in the metameta pass is done now.
                 */
                if ( dlrp->atomicRPass != META_META_PASS &&
                     BS_BFTAG_RSVD(pgredop->pgdesc.tag)  ) {
                    sts = ftx_bfmeta_rec_redo( dmnP, pgredop );
                    if ( sts != EOK ) {
                        ms_free(dlrp);
                        goto _exit;
                    }
                }
            } else if ( ((signed)pgredop->pgdesc.tag.num > 0) &&
                       ( recoveryPass == dlrp->atomicRPass ) )
            {
                bfSetIdT bfSetId;

                bfSetId.domainId = dmnP->domainId;
                bfSetId.dirTag = pgredop->pgdesc.bfsTag;

                sts = ftx_bfdata_rec_redo( ftxH, bfSetId, pgredop );
                if ( sts != EOK ) {
                    ms_free(dlrp);
                    goto _exit;
                }
            }
        }       /* done scanning for record image redo subrecords */

        /*
         * Execute the op redo routine on the appropriate pass.
         */

        if ( dlrp->opRedoRBcnt && ( recoveryPass == dlrp->atomicRPass ) ) {
            char* recptr = (char*)dlrp + ((lrrtdnloff + rdwords) << 2);
            opxT* opxp;

            /* ???? The manipulation of dlrp here is scary ???*/

            MS_SMP_ASSERT((lrrtdnloff + rdwords) <= totwordcnt);

            if ( (dlrp->agentId > FTX_MAX_AGENTS) ||
                !(opxp = FtxAgents[dlrp->agentId].redoOpX) )
            {
                domain_panic(dmnP, "ftx_recovery_pass: no redo opx agent");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            ftxp->type = OP_REDO;
            opxp( ftxH, dlrp->opRedoRBcnt, recptr );

            if ( clvlp->lastPinS >= 0 ) {
                domain_panic(dmnP, "ftx_recovery_pass:pin left by redo opx");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            ftxp->type = REC_REDO;
        }

        if ( (ftxp->currLvl == 0) && (clvlp->member == 0) ) {

            if ((recoveryPass == FTX_MAX_RECOVERY_PASS) &&
                (CFS_XID(dlrp->ftxId)))
            {
                    cfs_list(dmnP, dlrp->ftxId, TRUE);
            }


            /* Mark this slot available.  FtxMutex synchronizes updates
             * to FtxDynAlloc counters in ftx_free_2.
             */
            mutex_lock(&FtxMutex);
            ftx_free( ftxSlot, ftxTDp );

            /* Now free the ftx struct allocated to the newly-available slot. */

            ftx_free_2( ftxp );
            mutex_unlock(&FtxMutex);
        }

        /* Give back the memory given to us from lgr_read */
        ms_free(dlrp);

    } while ( lrsts != W_LAST_LOG_REC );

    dmnP->state = BFD_RECOVER_FTX;

    /* loop through ftx and fail for this pass */

    for ( ftxSlot = 0; ftxSlot < ftxTDp->nextNewSlot; ftxSlot++) {
        lrDescT *lrvecp;

        ftxp = ftxTDp->tablep[ftxSlot].ftxp;
        if ( ftxTDp->tablep[ftxSlot].state != FTX_SLOT_BUSY &&
             ftxTDp->tablep[ftxSlot].state != FTX_SLOT_EXC )
        {/* not in use */
            continue;
        }

        clvlp = &ftxp->cLvl[ftxp->currLvl];

        ftxH.hndl = ftxSlot + 1;
        ftxH.level = ftxp->currLvl;
        ftxH.dmnP = dmnP;

        if ( ftxp->currLvl != 0 ) {

            /*
             * Only fail an ftx if appropriate for this recovery pass.
             * Note that this test is will also be made in ftx_fail_2
             * so that only ftx nodes for this and lower recovery
             * levels will be undone now.
             */

            if ( RECOV_PASS_LT(dmnP, recoveryPass, clvlp->atomicRPass) ) {
                continue;
            }

#ifdef ADVFS_DEBUG
            printf("ADVFS Pass %d recovery - ftx rollback\n",
                   recoveryPass );
#endif
            if ((recoveryPass == FTX_MAX_RECOVERY_PASS) &&
                (CFS_XID(ftxp->lrh.ftxId)))
            {
                cfs_list(dmnP, ftxp->lrh.ftxId, FALSE);
            }

            /*
             * ftx_fail will put it on the free list if failed to
             *  level 0.
             */

            ftx_fail_2( ftxH, recoveryPass );
        } else {
            int rdoff = 0;
            int rdcnt =0;
            int rtdni;

            if ( (unsigned)clvlp->member > (unsigned)ftxp->rootDnCnt) {
                /*
                 * okay - a hack, because continuatinon records have a
                 * negative (signed) member number, hence will always
                 * be larger than a valid root done count when making
                 * an unsigned test.
                 *
                 * If there is a continuation, the final root done
                 * record will have a member number 1 greater than the
                 * number of buffered root done records.
                 */
                continue;
            }

            /*
             * There are root done records to re-execute, because we
             * are at level zero and the member number was non-zero.
             * In fact, the member number is the number of root done
             * records already done plus one.
             */

            /* first scan to the first buffered root done to redo */

            for ( rtdni = clvlp->member - 1; rtdni > 0; rtdni-- ) {
                ftxRDHdrT* rdrec = (ftxRDHdrT*)&ftxp->rootDoneRecs[rdoff];

                rdoff += ((rdrec->bCnt + 3) >> 2) + (sizeof(ftxRDHdrT)/4);
            }

            /*
             * Disallow independent subtransactions.
             */
            ftxp->type = ROOTDONE;

            ftxp->lrh.type = ftxDoneLR;
            ftxp->lrh.level = 0;
            ftxp->lrh.member = clvlp->member;
            ftxp->lrh.contOrUndoRBcnt = 0;
            ftxp->lrh.rootDRBcnt = 0;
            ftxp->lrh.opRedoRBcnt = 0;

            ftxH.level = 0;

            do {
                ftxRDHdrT* rdrec = (ftxRDHdrT*)&ftxp->rootDoneRecs[rdoff];
                int contAgent = ftxp->contDesc.agentId;
                opxT* opxp;

                if ( (rdrec->agentId > FTX_MAX_AGENTS) ||
                    !(opxp = FtxAgents[rdrec->agentId].rootDnOpX) )
                {
                    domain_panic(dmnP,
                      "ftx_recovery_pass: no root done opx agent");
                    sts = E_DOMAIN_PANIC;
                    goto _exit;
                }

                if ( RECOV_PASS_LT(dmnP, recoveryPass, rdrec->atomicRPass) ) {
                    /*
                     * Stop now if this root done record is for a
                     * higher recovery pass.  Only finish incomplete
                     * root done sub trees for this pass.
                     */
                    break;
                }

                ftxp->lrh.atomicRPass = rdrec->atomicRPass;
                ftxp->lrh.agentId = rdrec->agentId;
                ftxp->lrh.contOrUndoRBcnt = 0;

                /* execute the root-done procedure */

                opxp( ftxH, rdrec->bCnt, ((char*)rdrec +
                                          sizeof(ftxRDHdrT)) );

                /*
                 * Reset log record descriptor for record header.
                 */
                lrvecp = &ftxp->lrdesc;
                /* we zero out lrdesc, V3.2 paranoia only */
                bzero(lrvecp, sizeof(lrDescT));
                lrvecp->count = 1;
                lrvecp->dataLcnt = 0;
                lrvecp->bfrvec[0].bufPtr = (uint32T *)&ftxp->lrh;
                lrvecp->bfrvec[0].bufWords = (sizeof(ftxDoneLRT)+3)/4;

                if ( contAgent != ftxp->contDesc.agentId ) {

                    /*
                     * A continuation record was buffered by the
                     * rootdone executed above.
                     */
                    ftxp->lrh.contOrUndoRBcnt = ftxp->contDesc.bCnt;
                    addone_cont( (void*)ftxp->contDesc.rec, ftxp, lrvecp );
                }

                /* buffer record image redo */

                addone_recredo( ftxp, lrvecp );

                ftxp->undoBackLink = logEndOfRecords;

                /*
                 * Root done record member is how many have been done
                 * plus one.  Except for the last one, which is zero,
                 * so we can tell we're done during recovery.
                 */
                if ( (ftxp->lrh.member++ == ftxp->rootDnCnt) &&
                    ftxp->contDesc.agentId == 0 )
                {
                    ftxp->lrh.member = 0;
                }

                /*
                 * Log the root done redo record.
                 */
                if((sts=log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK)
                {
                    goto _exit; /* E_DOMAIN_PANIC */
                }

                /*
                 * Release ftx locks.
                 */
                release_ftx_locks( clvlp );

                rdoff += ((rdrec->bCnt + 3)/4) + (sizeof(ftxRDHdrT)/4);
                rdcnt += 1;
            } while ( rdoff < ftxp->nextRDoff );

            /*
             * If all root done is complete for this ftx, make this
             * slot available.
             */
            if ( ftxp->lrh.member == 0 ) {

                if ((recoveryPass == FTX_MAX_RECOVERY_PASS) &&
                    (CFS_XID(ftxp->lrh.ftxId))) {
                    cfs_list(dmnP, ftxp->lrh.ftxId, TRUE);
                }

                /* FtxMutex synchronizes updates to FtxDynAlloc counters */
                /* in ftx_free_2. */
                mutex_lock(&FtxMutex);
                ftx_free( ftxSlot, ftxTDp );
                ftx_free_2( ftxp );
                mutex_unlock(&FtxMutex);
            }
#ifdef ADVFS_DEBUG
            if ( rdcnt ) {
                printf("ADVFS Pass %d recovery - %d rootdone completion\n",
                       recoveryPass, rdcnt );
            }
#endif
        }
    }

_exit:
    bs_domain_close( newdmnP );

_error:
    return sts;
}

/* 
 * This routine returns the number of records for recovery in the log, up to
 * a number specified by 'limit'.   If 'limit' is 0, then it will count
 * the records until the end of the log.
 */
int 
num_of_log_recs(
        struct domain *dmnP,
        int limit)
{
    statusT sts;
    logRdHT logrh;
    logRecAddrT lastlogaddr, crashRedo;
    ftxDoneLRT *dlrp = NULL;
    int wordcnt, totwordcnt;
    int nrecs = 9999;
    int endoflog = 0;
    
    MS_SMP_ASSERT(dmnP->ftxLogP != NULL);
    sts = lgr_read_open( dmnP->ftxLogP, &logrh );
    MS_SMP_ASSERT(sts == EOK);

    sts = lgr_get_last_rec( dmnP->ftxLogP, &lastlogaddr );
    if ( sts == E_LOG_EMPTY ) {
        goto logclose;
    }

    sts = lgr_read( LR_BWD, logrh, &lastlogaddr, (uint32T**)&dlrp,
                   &wordcnt, &totwordcnt );
    if ( sts != EOK && (sts < MSFS_FIRST_ERR) ) {
	if ( sts == E_LOG_EMPTY ) {
	    goto logclose;
	} else {
            domain_panic(dmnP, "num_of_log_recs: lgr_read error = %d.", sts);
            if ( dlrp ) ms_free(dlrp);
            goto logclose;
	}
    } else if ( wordcnt < sizeof(ftxDoneLRT)/4 ) {
        domain_panic(dmnP, "num_of_log_recs: bad record size (%d).", wordcnt);
        ms_free(dlrp);
        goto logclose;
    }

    if ( !BS_UID_EQL( dmnP->domainId, dlrp->bfDmnId)) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(dmnP->dualMountId, dlrp->bfDmnId)) )
        {
            dlrp->bfDmnId = dmnP->domainId;
        } else {
            domain_panic(dmnP, "num_of_log_recs: Bad domain ID in log (%x.%x).",
                         dlrp->bfDmnId.tv_sec, dlrp->bfDmnId.tv_usec);
            ms_free(dlrp);
            goto logclose;
        }
    }
    
    crashRedo = dlrp->crashRedo;
    nrecs = 0;

    /* Give back the memory given to us from lgr_read */

    ms_free(dlrp);
    
    do {
        sts = lgr_read( LR_FWD, logrh, &crashRedo, (uint32T**)&dlrp,
                       &wordcnt, &totwordcnt );

        if (sts ==  E_INVALID_REC_ADDR)  {
            crashRedo.lsn.num = 0;
            continue ;
        }
        if ( sts != EOK && (sts < MSFS_FIRST_ERR) ) {
            if ( sts == E_LOG_EMPTY ) {
                goto logclose;
            } else {
                domain_panic(dmnP, "num_of_log_recs: lgr_read error = %d.",
                             sts);
                ms_free(dlrp);
                goto logclose;
            }
        }

        nrecs++;

        /* Give back the memory given to us from lgr_read */

        ms_free(dlrp);

        if ( (sts == W_LAST_LOG_REC) || ((limit != 0) && (nrecs >= limit)) )
            break;

    } while (1);
    
logclose:
    sts = lgr_read_close(logrh);
    if ( sts != EOK ) {
        /* This probably should be an assert. The error paths in */
        /* lgr_read_close should probably all be asserts. */
        domain_panic(dmnP, "num_of_log_recs: lgr_read_close error");
    }
    
    return nrecs;
}

/*
 * cfs_list
 *
 * This routine writes to memory the status of a particular xid
 * (CFS transaction).  Status is either "committed" or "rolled back."
 *
 * cfs_list() is called once and only once for each CFS transaction
 * in the inactive or active log.
 *
 * inactive log: read in ftx_bfdmn_recovery().
 *
 * Active log: cfs_list() is called on the last pass (DATA_PASS)
 * of ftx_recovery_pass() each time an ftx transaction slot is
 * freed.  It is also called on transactions with continuation records
 * when they get freed in ftx_bfdmn_recovery(), after the last pass
 * of ftx_recovery_pass().
 *
 * The reason cfs_list() is called only in the last pass (DATA_PASS)
 * of ftx_recovery_pass() is to avoid redundant listings.  Currently,
 * (3/2/99) each pass of ftx_recovery_pass() reads the same records--
 * all the active log records starting from crashRedo.
 *
 *
 * IMPLEMENTATION
 *
 * xid information is stored in chained xidInfoT's, each with
 * a next pointer and a number of xid slots.  In each slot is
 * an xid (4 bytes) with its high bit toggled to indicate whether
 * the xid was committed or rolled back.
 *
 * (The high bit of the xid originally is set to indicate it is
 * a CFS xid transaction.)
 *
 * The chain of xidInfoT's hangs off of dmnP->xidRecovery.head.
 * The last struct in the chain is pointed to by dmnP->xidRecovery.tail.
 * The next free slot in the tail is contained in
 * dmnP->xidRecovery.current_free_slot.
 *
 * Return values: EOK, ENO_MORE_MEMORY.
 *
 * If this routine does not return EOK, a subsequent call to
 * cfs_check_for_pfs_commit() for this xid will simply return
 * CFS_XID_NOT_FOUND.
 */
cfs_list(domainT *dmnP, /* in */
         ftxIdT ftxId,  /* in */
         int done_flag  /* in -- TRUE/FALSE [committed/rolled back] */ )
{
    xidInfoT *xidRecoveryp;
    xidInfoT **xidRecoverypp;
    int *current_free_slot;

    XID_RECOVERY_LOCK_WRITE(dmnP);

    /*
     * Allocate a new xidInfoT if head is NULL or the current
     * free slot in the tail is the last slot.
     * The last slot of each xidInfo is always kept NULL.
     */

    current_free_slot = &(dmnP->xidRecovery.current_free_slot);
    if ((dmnP->xidRecovery.head == NULL) 
		|| (*current_free_slot == (NUM_XIDS-1))) {

        if (dmnP->xidRecovery.head == NULL) {
            xidRecoverypp = &(dmnP->xidRecovery.head);
        } else {
            xidRecoverypp = &(dmnP->xidRecovery.tail->next); 
        }

        *xidRecoverypp = (xidInfoT *) ms_malloc(sizeof(xidInfoT));
        if (*xidRecoverypp == NULL) {
            XID_RECOVERY_UNLOCK(dmnP);
            return ENO_MORE_MEMORY;
        }
        dmnP->xidRecovery.tail = *xidRecoverypp;
        *current_free_slot = 0; 
    } 

    /* 
     *  The xid status is kept in the high-order bit,
     *  originally used to indicate whether this ftxId
     *  was an xid.
     */

    xidRecoveryp = dmnP->xidRecovery.tail;
    xidRecoveryp->data[*current_free_slot] = ftxId; 
    if (done_flag) {
        xidRecoveryp->data[*current_free_slot] |= (1<<31);
    } else {
        xidRecoveryp->data[*current_free_slot] &= ~(1<<31);
    }
    (*current_free_slot)++;

    XID_RECOVERY_UNLOCK(dmnP);
 
    return EOK;
} 


/* 
 * cfs_check_for_pfs_commit
 *
 * This routine is called by CFS to check whether a particular xid
 * committed its transaction. 
 *
 * Return values are TRUE, FALSE, or CFS_XID_NOT_FOUND.
 *
 * An xid might not be found due to previous error conditions:
 * the inactive or active log read was incomplete, or memory
 * could not be allocated in cfs_list() for xid status.
 * It may also be returned upon errors encountered in this routine.
 *
 * UNMOUNT_LOCK is held if getvfs() successful.  Must release at exit points.
 *
 */
cfs_check_for_pfs_commit(fsid_t fsid,  /* in */
                         ftxIdT xid    /* in */)
{
    struct mount *mp, *pmp;
    domainT *dmnP=NULL;
    xidInfoT *xidInfop;
    ftxIdT xid_lo, loop_xid;
    int slot, ret;

start:

    if ((mp = getvfs(&fsid)) != NULL) { /* UNMOUNT_LOCK(mp) held if success */
        if (CC_GETPFSMP_BLOCK(mp, &pmp) != ESUCCESS) {

            ADVFS_SAD0("cfs_check_for_pfs_commit:could not get pfs mount point");
        }

        if (!UNMOUNT_TRY_READ(pmp)) {   /* get pmp lock */
            UNMOUNT_READ_UNLOCK(mp);
            /*
             * The following use of thread_block is fine.
             * A non-interruptible assert_wait was setup by UMOUNT_TRY_READ.
             */
            thread_block();
            goto start;
        }

        dmnP = GETDOMAINP (pmp);
        UNMOUNT_READ_UNLOCK(mp);   /* note pmp lock still held. */
    } else {
        return CFS_XID_NOT_FOUND;
    }

    XID_RECOVERY_LOCK_READ(dmnP);

    xid_lo = xid; 
    xid_lo &= ~(1<<31);		/* clear the high bit */

    xidInfop = dmnP->xidRecovery.head;
    while (xidInfop != NULL) {
        slot=0;

        while (xidInfop->data[slot] != NULL) {
            loop_xid = xidInfop->data[slot];
            loop_xid &= ~(1<<31);		/* clear the high bit */

            /*  
             * Compare the lower bits of the incoming xid with
             * the lower bits of the stored xid. 
             */

            if (xid_lo == loop_xid) {

                /* 
                 * If the high bit is set, this xid committed.
                 * Otherwise, it was rolled back.
                 */

		if (xidInfop->data[slot] & (1<<31)) {
                    ret = TRUE;
		} else {
                    ret = FALSE;
                }
                XID_RECOVERY_UNLOCK(dmnP);
                UNMOUNT_READ_UNLOCK(pmp);

                return ret;
            }
            slot++;
        }  
        xidInfop = xidInfop->next;
    } 

    XID_RECOVERY_UNLOCK(dmnP);
    UNMOUNT_READ_UNLOCK(pmp);

    return CFS_XID_NOT_FOUND;
}

/*
 * cfs_xid_free_memory
 *
 * This routine is called via timeout() from bfdmn_ftx_recovery(),
 * executed 15 minutes after that routine has completed.
 * Note it is executing in interrupt context.
 *
 * Checks are made to protect against races with dmn_dealloc().
 */
void
cfs_xid_free_memory(char *arg)
{
    bfDomainIdT domainId;
    domainT *dmnP;
    struct timeval ltime;
    statusT sts;

    /* arg is really not a pointer. It is the actual value */

    bcopy(&arg,&domainId,sizeof(ulong));

    sts = bs_domain_access( &dmnP, domainId, FALSE );
    if (sts != EOK) 
    {
        return;
    }

    if (dmnP->state != BFD_ACTIVATED) 
    {
        bs_domain_close(dmnP);
        return;
    }

    /*
     * At this point, we have the domain access count up so it
     * cannot change out from under us (dmn_dealloc()).
     * There is then no way to race dmn_dealloc() for the
     * XID_RECOVERY_WRITE_LOCK.  We should only be racing other reads.
     *
     * The following check is still necessary to guarantee we are
     * working on the right domain.
     */

    if ((dmnP->xidRecovery.timestamp.tv_sec == 0) &&
        (dmnP->xidRecovery.timestamp.tv_usec == 0)) 
    {
        bs_domain_close(dmnP);
        return;
    }

    TIME_READ(ltime);
    if (ltime.tv_sec - dmnP->xidRecovery.timestamp.tv_sec < XID_TIMEOUT) 
    {
        bs_domain_close(dmnP);
        return;
    }

    if (XID_RECOVERY_LOCK_TRY_WRITE(dmnP) == FALSE) {
        /*
         * At this point, not racing with dmn_dealloc().
         * Legit to do a timeout.
         */
        timeout((void(*))cfs_xid_free_memory, arg, (1*60*hz));
        bs_domain_close(dmnP);
        return;
    }
    cfs_xid_free_memory_int(dmnP);
    XID_RECOVERY_UNLOCK(dmnP);
    bs_domain_close(dmnP);

}


/* 
 * cfs_xid_free_memory_int
 *
 * Called from cfs_xid_free_memory() and dmn_dealloc().
 */
void
cfs_xid_free_memory_int(domainT* dmnP)
{
    xidInfoT *xidInfop, *next;

    xidInfop = dmnP->xidRecovery.head;
    while (xidInfop != NULL) {
        next = xidInfop->next;
        ms_free(xidInfop);
        xidInfop = next;
    } 
    dmnP->xidRecovery.head = NULL;
    dmnP->xidRecovery.tail = NULL;
    dmnP->xidRecovery.current_free_slot = 0;
    dmnP->xidRecovery.timestamp.tv_sec = 0;
    dmnP->xidRecovery.timestamp.tv_usec = 0;

}
