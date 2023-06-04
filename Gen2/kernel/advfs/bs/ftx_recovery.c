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
 *  ADVFS Storage System
 *
 * Abstract:
 *
 *  ftx_recovery
 *  This module contains the recovery routines.
 */

#include <ms_public.h>
#include <ms_privates.h>
#include <ftx_privates.h>
#include <ftx_public.h>
#include <bs_domain.h>
#include <bs_public.h>
#include <ms_osf.h>
#include <tcr/clu.h>
#include <bs_access.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE FTX_RECOVERY



#ifdef ADVFS_FTX_TRACE_LOG
#include <sys/spinlock.h>

#define ADVFS_FTX_TRACE_LOG_SIZE 10000

ftx_trace_t advfs_ftx_log[ADVFS_FTX_TRACE_LOG_SIZE];
int32_t advfs_ftx_trace_cur_index = 0;
#pragma align SPINLOCK_ALIGN
spin_t ftx_trace_lock = {0};

void
advfs_ftx_logit( ftx_trace_op_t         ftx_op,
                 ftx_trace_caller_t     caller,
                 bfTagT                 tag,
                 off_t                  off,
                 size_t                 size,
                 domainT*               dmnp,
                 logRecAddrT            log_addr,
                 ftxDoneLRT*            ftx_done_lr,
                 ftxAgentIdT            ftx_agent,
                 char*                  desc /* description of event */
        )
{

    if (dmnp->state == BFD_ACTIVATED) {
        /* Don't log transactions on an activated domain by default, just
         * during recovery. */
        return;
    }
    /* Need to hold the spin lock accross the stores since
     * it is global and another thread could bump it while
     * we are using it. Same is true for bfa_trace_index.
     */
    spin_lock(&ftx_trace_lock);
    if (++advfs_ftx_trace_cur_index >= ADVFS_FTX_TRACE_LOG_SIZE) {
        advfs_ftx_trace_cur_index = 0;
    }
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].ftx_op = ftx_op;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].caller = caller;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].tag = tag;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].offset = off;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].size = size;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].dmnP = dmnp;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].ftx_dmn_state = dmnp->state;
    advfs_ftx_log[ advfs_ftx_trace_cur_index ].log_addr = log_addr; 
    if (ftx_done_lr) {
        advfs_ftx_log[ advfs_ftx_trace_cur_index ].ftx_done_lr = *ftx_done_lr; 
    } else {
        bzero( &advfs_ftx_log[ advfs_ftx_trace_cur_index ].ftx_done_lr,
                sizeof( ftxDoneLRT ) );
    }
    bcopy( desc, &advfs_ftx_log[ advfs_ftx_trace_cur_index ].description,
            min( strlen( desc ), FTX_LOGIT_MSG_SZ ) );
    spin_unlock(&ftx_trace_lock);

}
#endif


static int cfs_list(domainT *dmnP, ftxIdT ftxId, int done_flag);



/**********************************************
 *  module private typedefs ******************
**********************************************/

typedef struct {
    ftxRecRedoT pgdesc;         /* page descriptor */
    ftxRecXT recX[FTX_MX_PINR]; /* record extent list */
} pageredoT;

extern ftxAgentT FtxAgents [FTX_MAX_AGENTS];


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
    uint64_t ptag = -(int64_t)pgredop->pgdesc.tag.tag_num;
    int rbmtidx, recxi, vdi;
    vdT* vdp;
    bsMPgT* rbmtpgp;

    rbmtidx = ptag % BFM_RSVD_TAGS;
    MS_SMP_ASSERT(rbmtidx == BFM_RBMT );

    vdi = ptag / BFM_RSVD_TAGS;
    /* During recovery we don't need to worry about the vdp being removed,
     * so don't bump the vdRefCnt on this call.
     */
    if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) ) {
        domain_panic(dmnP,
            "ftx_metameta_rec_redo: Domain \"%s\". Bad volume index (%d).", dmnP->domainName,
            vdi);
        return E_DOMAIN_PANIC;
    }
    bfap = vdp->rbmtp;

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_META_META_REDO,
                        bfap->tag,
                        pgredop->pgdesc.page, 0,
                        dmnP,
                        logNilRecord,
                        NULL,
                        FTA_NULL,
                        "ftx_metameta_rec_redo" );


    sts = bs_pinpg(&pgH,
                   (void *)&rbmtpgp,
                   bfap,
                   pgredop->pgdesc.page,
                   FtxNilFtxH,
                   MF_NO_VERIFY);
    if ( sts != EOK ) {
        domain_panic(dmnP,
            "ftx_metatmeta_rec_redo: Domain \"%s\", volume %d, RBMT page %d. bs_pinpg error (%d).",
            dmnP->domainName, vdi, pgredop->pgdesc.page, sts);
        return E_DOMAIN_PANIC;
    }

    if ( rbmtpgp->bmtPageId != pgredop->pgdesc.page ) {
        domain_panic(dmnP,
            "ftx_metameta_rec_redo: Domain \"%s\", volume %d, RBMT page %d. Bad pageId (%d).",
            dmnP->domainName, vdi, pgredop->pgdesc.page, rbmtpgp->bmtPageId);
        (void)bs_unpinpg( pgH, logNilRecord, BS_CLEAN );
        return E_DOMAIN_PANIC;
    }

    srcRecp = (char*)pgredop + sizeof(ftxRecRedoT) +
              pgredop->pgdesc.numXtnts*sizeof(ftxRecXT);

    for ( recxi = 0; recxi < pgredop->pgdesc.numXtnts; recxi++) {

        /* adjust src pointer for odd byte offset */

        srcRecp += pgredop->recX[recxi].pgBoff & 3;
        
        dstRecp = (char*)rbmtpgp + pgredop->recX[recxi].pgBoff;

        flmemcpy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

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
    uint64_t ptag = -(int64_t)pgredop->pgdesc.tag.tag_num;
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

        if ( bmtidx == BFM_RBMT ) {
            bfap = vdp->rbmtp;
        } else if ( bmtidx == BFM_BMT ) {
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

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_BFMETA_REDO,
                        bfap->tag,
                        pgredop->pgdesc.page, 0,
                        dmnP,
                        logNilRecord,
                        NULL,
                        FTA_NULL,
                        "ftx_bfmeta_rec_redo" );


    sts = bs_pinpg(&pgH,
                   &bpp,
                   bfap,
                   pgredop->pgdesc.page,
                   FtxNilFtxH,
                   MF_NO_VERIFY);

    if ( sts != EOK ) {
        if ( bmtidx == BFM_BFSDIR ) {
            domain_panic(dmnP,
              "ftx_bfmeta_rec_redo: Domain \"%s\", root TAG page %d. bs_pinpg error (%d).",
              dmnP->domainName, pgredop->pgdesc.page, sts);
        } else {
            domain_panic(dmnP,
              "ftx_bfmeta_rec_redo: Domain \"%s\", volume %d, %s page %d. bs_pinpg error (%d).",
              dmnP->domainName, ptag / BFM_RSVD_TAGS,
              (bmtidx == BFM_RBMT) ? "RBMT" : "BMT",
              pgredop->pgdesc.page, sts);
        }
        return E_DOMAIN_PANIC;
    }

    if ( bmtidx == BFM_BMT || 
         bmtidx == BFM_RBMT )
    {
        bsMPgT* bmtp = (bsMPgT*)bpp;

        if ( bmtp->bmtPageId != pgredop->pgdesc.page ) {
            domain_panic(dmnP,
               "ftx_bfmeta_rec_redo: Domain \"%s\", volume %d, %s page %d. Bad pageId (%d).",
               dmnP->domainName, ptag / BFM_RSVD_TAGS,
               (bmtidx == BFM_RBMT) ? "RBMT" : "BMT",
               pgredop->pgdesc.page, bmtp->bmtPageId);
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

        flmemcpy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

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

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if ( sts != EOK ) {
        return EOK;
    }

    sts = bs_access(&bfap, pgredop->pgdesc.tag, bfSetp, ftxH, BF_OP_INTERNAL, 
                    &nullvp );
    if ( sts != EOK ) {
        goto close_bfs;
    }

    MS_SMP_ASSERT(bfap->dataSafety == BFD_METADATA);

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_DATA_REDO,
                        bfap->tag,
                        pgredop->pgdesc.page, 0,
                        bfap->dmnP,
                        logNilRecord,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdata_rec_redo" );

    /*
     * It is possible that the page we want to modify was removed in a
     * previous transaction recovery pass.  If that is the case, we will
     * just skip redoing this page.
     */
    if (advfs_bs_is_offset_mapped( bfap,
                                   (pgredop->pgdesc.page 
                                    * ADVFS_FOB_SZ * bfap->bfPageSz) )) {

        sts = bs_pinpg(&pgH,
                   &bpp,
                   bfap,
                   pgredop->pgdesc.page,
                   ftxH,
                   MF_NO_VERIFY);
        if ( sts != EOK ) {
         domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_pinpg error = %d.",
                     sts);
        }
    } else {
        goto close_bf;
    }

    srcRecp = (char*)pgredop + sizeof(ftxRecRedoT) +
              pgredop->pgdesc.numXtnts*sizeof(ftxRecXT);

    for ( recxi = 0; recxi < pgredop->pgdesc.numXtnts; recxi++) {

        /* adjust src pointer for odd byte offset */

        srcRecp += pgredop->recX[recxi].pgBoff & 3;
        
        dstRecp = (char*)bpp + pgredop->recX[recxi].pgBoff;

        flmemcpy( srcRecp, dstRecp, pgredop->recX[recxi].bcnt );

        srcRecp = (char*)(((long)( srcRecp + pgredop->recX[recxi].bcnt + 3) >> 2) << 2);
    }

    sts = bs_unpinpg( pgH, logNilRecord, BS_DIRTY );
    if ( sts != EOK ) {
        domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_unpinpg error = %d.",
                     sts);
        retsts = E_DOMAIN_PANIC;
    }

close_bf:
    sts = bs_close(bfap, MSFS_CLOSE_NONE);
    if ( sts != EOK ) {
        domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_close error = %d.",
                     sts);
        retsts = E_DOMAIN_PANIC;
    }

close_bfs:
    sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    if ( sts != EOK ) {
        domain_panic(ftxH.dmnP, "ftx_bfdata_rec_redo: bs_bfs_close error = %d.",
                     sts);
        retsts = E_DOMAIN_PANIC;
    }
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
 * These macros are used to clarify inline code used in ftx_recovery_pass.
 * The code added three then shifted two to get the effect of adding three
 * then dividing by four.  This converted from bytes to words.
 */
#define ADVFS_BYTES_TO_WORDS( _bytes ) ((_bytes+3)>>2)
/*
 * This macro converts from words to bytes by shifting by 2, or multiplying
 * by 4...
 */
#define ADVFS_WORDS_TO_BYTES( _words ) (_words<<2)

/*
 * ftx_bfdmn_recovery - recover domain consistency
 */
statusT
ftx_bfdmn_recovery(
                   domainT* dmnP,
		   uint64_t flags              /* in - flag */
                   )
{
    statusT sts, lrsts=0;
    int wordcnt, totwordcnt, ftxSlot, incomplete;
    logRdHT logrh;
    logRecAddrT logaddr, crashRedo;
    ftxDoneLRT* dlrp;
    struct ftx* ftxp;
    extern int MountWriteDelay;
    uint32_t *segptr;
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

    sts = lgr_get_last_rec( dmnP->ftxLogP, &logaddr );
    if ( sts == E_LOG_EMPTY ) {  /* only return besides EOK */
        sts = EOK; /* nothing to do so allow domain to mount */
        goto logclose;
    }

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_BFDMN_RECOVERY_START,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logaddr,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery just got last rec of log. log_addr is last rec" );


    /* read the last record.  should scan back if other than ftx */
    /* records are possible in the log. */

    sts = lgr_read( LR_BWD, logrh, &logaddr, (uint32_t**)&dlrp,
                   &wordcnt, &totwordcnt );
    if ( sts != EOK && (sts < MSFS_FIRST_ERR) ) {
        if ( sts == E_LOG_EMPTY ) {
            goto logclose;
        } else {
            domain_panic(dmnP, "ftx_bfdmn_recovery: lgr_read error = %d.", sts);
            goto logclose;
        }
    } else if ( wordcnt < ADVFS_BYTES_TO_WORDS(sizeof(ftxDoneLRT)) ) {
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
                         dlrp->bfDmnId.id_sec, dlrp->bfDmnId.id_usec);
            sts = E_DOMAIN_PANIC;
            goto logclose;
        }
    }

    crashRedo = dlrp->crashRedo;

    /* We are done with the buffer from lgr_read */
    
    ms_free(dlrp);

    if (flags & DMNA_FAILOVER) {

        MS_SMP_ASSERT(dmnP->xidRecovery.xid_head == NULL);

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

        ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_BFDMN_RECOVERY_CLU_START,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logaddr,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery just pseudo first rec" );


        while (logaddr.lsn.num < crashRedo.lsn.num) {

            lrsts = lgr_read( LR_FWD, logrh, &logaddr, (uint32_t**)&dlrp,
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
                printf("ftx_bfdmn_recovery: Domain %s: unexpected NULL buf; lgr_read sts: %d\n",
                    dmnP->domainName, lrsts);
                break;
            }

            if (ftxId != dlrp->ftxId) {
                ftxId = dlrp->ftxId;

                if (CFS_XID(ftxId)) {
                    cfs_list(dmnP, dlrp->ftxId, TRUE);
                }
            }
 
            ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_BFDMN_RECOVERY_CLU_FWD_READ,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logaddr,
                        dlrp,
                        FTA_NULL,
                        "ftx_bfdmn_recovery just read foward in cluster " );

            ms_free(dlrp);
            dlrp = NULL;

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

            ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_BFDMN_RECOVERY_CLU_FWD_READ,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logaddr,
                        dlrp,
                        FTA_NULL,
                        "ftx_bfdmn_recovery just read foward in cluster past all segments " );



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

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_31);

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_GOT_OLDEST_REDO,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        crashRedo,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery got oldest redo record" );



#ifdef ADVFS_DEBUG
    printf("\nADVFS file domain recovery starting.\n");
#endif

    /*********************************************/
    /* scan log to recover reserved file metadata*/
    /*********************************************/
    dmnP->state = BFD_RECOVER_REDO;
 
    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_STARINTG_META_META_PASS,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        crashRedo,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery starting META META pass" );

    sts = ftx_recovery_pass( dmnP, META_META_PASS, logrh, crashRedo );
    if ( sts != EOK ) {
        goto flushlog;
    }

    /*********************************************/
    /* scan log to recover bitfile metadata only */
    /*********************************************/
    dmnP->state = BFD_RECOVER_REDO;

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_STARINTG_META_PASS,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        crashRedo,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery starting META pass" );

    sts = ftx_recovery_pass( dmnP, META_PASS, logrh, crashRedo );
    if ( sts != EOK ) {
        goto flushlog;
    }

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_32);

    /******************************************/
    /* scan log again to recover bitfile data */
    /******************************************/

    dmnP->state = BFD_RECOVER_REDO;

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_STARINTG_DATA_PASS,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        crashRedo,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery starting DATA pass" );

    sts = ftx_recovery_pass( dmnP, DATA_PASS, logrh, crashRedo );
    if ( sts != EOK ) {
        goto flushlog;
    }

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_33);


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

    ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                        FTX_STARTING_CONTINUATIONS,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logNilRecord,
                        NULL,
                        FTA_NULL,
                        "ftx_bfdmn_recovery staring continuations" );


    do {
        incomplete = 0;
        for ( ftxSlot = 0; ftxSlot < dmnP->ftxTbld.rrSlots; ftxSlot++ ) {
            struct ftx* ftxp = &dmnP->ftxTbld.tablep[ftxSlot];

            if ( dmnP->ftxTbld.tableSltStatep[ftxSlot] == FTX_SLOT_BUSY ||
                 dmnP->ftxTbld.tableSltStatep[ftxSlot] == FTX_SLOT_EXC ) {
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
                    lrvecp->bfrvec[0].bufPtr = (uint32_t *)&ftxp->lrh;
                    lrvecp->bfrvec[0].bufWords = ADVFS_BYTES_TO_WORDS(sizeof(ftxDoneLRT));


                    ADVFS_FTX_LOGIT( FTX_CONT_TRACE,
                        FTX_DO_CONTINUATION,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logNilRecord,
                        NULL,
                        ftxp->contDesc.agentId,
                        "ftx_bfdmn_recovery staring continuations" );


                    do_ftx_continuations( dmnP, ftxp, ftxH, lrvecp );

                    if ( ftxp->contDesc.agentId ) {
                        incomplete = 1;
                    } else {
                        /*
                         * Compensate for artificially stuffing
                         * noTrimCnt to 1, as ftx_release_slot()
                         * will decrement it.
                         */
                        spin_lock(&dmnP->ftxTblLock);
                        ++dmnP->ftxTbld.noTrimCnt;

                        if (CFS_XID(ftxp->lrh.ftxId)) {
                            cfs_list(dmnP, ftxp->lrh.ftxId, TRUE);
                        }

                        /* mark the slot available. */
                        ftx_release_slot(ftxSlot, &dmnP->ftxTbld);
                        /* reset_oldest_lsn unlocks dmnP->ftxTblLock */
                        reset_oldest_lsn( ftxp, dmnP, FALSE );
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

flushlog:
    /* 
     * If not doing write delay then flush all dirty buffers and wait for 
     * that i/o to complete 
     */
    if ( !MountWriteDelay || dmnP->dmn_panic ) {
        advfs_bs_dmn_flush(dmnP, FLUSH_UPDATE_STATS);
    } 
   
#ifdef ADVFS_DEBUG
    printf("ADVFS file domain recovery now complete.\n\n");
#endif
logclose:
    /* close the log read stream */

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_34);

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
    ADVRWL_BFSETTBL_WRITE( dmnP );
    ADVMTX_DOMAIN_LOCK( &dmnP->dmnMutex );
    while( dmnP->bfSetHead.bfsQfwd != &dmnP->bfSetHead ) {
        entry = dmnP->bfSetHead.bfsQfwd;
        BFSET_DMN_REMQ( dmnP, entry );
        ADVMTX_DOMAIN_UNLOCK( &dmnP->dmnMutex );
        bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
        if (bfSetp->fsRefCnt == 0) {
            bfs_dealloc( bfSetp, TRUE );
        }    
        ADVMTX_DOMAIN_LOCK( &dmnP->dmnMutex );
    }
    ADVMTX_DOMAIN_UNLOCK( &dmnP->dmnMutex );
    ADVRWL_BFSETTBL_UNLOCK( dmnP );    

freexid:
    /*
     * Call cfs_xid_free_memory() after 15 minutes.
     * It should no longer be needed after that point.
     */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_35);

    if (flags & DMNA_FAILOVER) {
        /*
         * This domainIdP points to malloced memory that will need to be
         * freed by the cfs_xid_free_memory code path. It contains a copy of
         * the domainId for cfs_xid_free_memory to be able to make a
         * bs_access_one() call safely, even under a race with
         * dmn_dealloc().  Passing the dmnP is not safe due to dmn_dealloc()
         * possibly freeing that memory and the cfs_xid_free_memory() code
         * not knowing about it and the trying to deref it.
         */
        bfDomainIdT *domainIdP;
        domainIdP = (bfDomainIdT *)ms_malloc(sizeof(bfDomainIdT));
        bcopy(&(dmnP->domainId), domainIdP, sizeof(bfDomainIdT));

        ADVRWL_XID_RECOVERY_WRITE(dmnP);
        ltime = get_system_time();
        dmnP->xidRecovery.xid_timestamp = ltime;

        timeout( (void(*))cfs_xid_free_memory,
                 (char *) domainIdP,
                 XID_TIMEOUT*HZ );
        ADVRWL_XID_RECOVERY_UNLOCK(dmnP);

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
    uint32_t* lgrrecp = 0;
    struct ftx* ftxp;
    perlvlT* clvlp;
    int ftxSlot, redoloff, lrrtdnloff, rdwords;
    int redoloffstart; 
    size_t redosize;
    ftxHT ftxH;
    ftxDoneLRT* dlrp;
    domainT *newdmnP;
    logRecAddrT oldftxrec;
    ftxTblDT* ftxTDp = &dmnP->ftxTbld;
    char *rootDoneRecPtr = NULL;
    pageredoT *pageRedoRecPtr = NULL;
    pageredoT *pageRedoRecPtrTmp = NULL;

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

        lrsts = lgr_read( LR_FWD, logrh, &logaddr, (uint32_t**)&dlrp,
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
            uint32_t* segptr;

            /*
             * segmented record - get the whole thing into a buffer (lgrrecp)
             * freed as dlrp at end of loop
             */
            lgrrecp = (uint32_t*)ms_malloc( (size_t) ADVFS_WORDS_TO_BYTES(
                         (size_t) (uint32_t)totwordcnt) );
            flmemcpy( (char*)dlrp, (char*)lgrrecp, ADVFS_WORDS_TO_BYTES( 
                        (size_t) (uint32_t) segwordcnt ) );

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

                    flmemcpy( (char*)segptr, (char*)&lgrrecp[cntRead],
                        ADVFS_WORDS_TO_BYTES((size_t) (uint32_t) segwordcnt) );

                    /* Give back the memory given to us from lgr_read */
                    ms_free(segptr);

                    cntRead += segwordcnt;
                }
            } while ( lrsts == I_LOG_REC_SEGMENT );

            if (lrsts ==  E_INVALID_REC_ADDR)  {
                   continue;
            }
        }

        ADVFS_FTX_LOGIT( FTX_REC_REDO_TRACE,
                        FTX_PASS_SCAN_REDO,
                        NilBfTag,
                        0, 0,
                        dmnP,
                        logNilRecord,
                        dlrp,
                        FTA_NULL,
                        "ftx_recovery_pass scanned for redo record" );


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
                             dlrp->bfDmnId.id_sec, dlrp->bfDmnId.id_usec);
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }
        }

        for ( ftxSlot = 0; ftxSlot < ftxTDp->rrSlots; ftxSlot++) {
            ftxp = &ftxTDp->tablep[ftxSlot];

            if (( ftxTDp->tableSltStatep[ftxSlot] == FTX_SLOT_BUSY ||
                  ftxTDp->tableSltStatep[ftxSlot] == FTX_SLOT_EXC ) &&
                ( ftxp && ftxp->lrh.ftxId == dlrp->ftxId ))
            {
                /* found entry */
                break;
            } else if (ftxTDp->tableSltStatep[ftxSlot] == FTX_SLOT_AVAIL) {
                /* in case we don't find ent, save location of avail slot */
                availSlot = ftxSlot;
            }
        }

        if ( ftxSlot == ftxTDp->rrSlots ) {
            /*
             * We didn't find an existing ftx for the record just
             * read, so get one and init it for the entire ftx tree.
             */
            if ((ftxSlot = availSlot) < 0) {
                /* All slots are busy. Issue a domain panic. */
                domain_panic(dmnP,
                      "ftx_recovery_pass: no more slots in ftx table");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }
            else {
                ftxp = &ftxTDp->tablep[ftxSlot];
            }

            ftxTDp->tableSltStatep[ftxSlot] = FTX_SLOT_BUSY;
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
            ftxp->lrh.fdl_agentId = dlrp->fdl_agentId;
            ftxp->contDesc.agentId = 0;
            ftxp->type = ROOTDONE;

            MS_SMP_ASSERT(sizeof(ftxDoneLRT) <= (size_t)ADVFS_WORDS_TO_BYTES(
                        (size_t) (uint32_t) totwordcnt));

            /*
             * Pass to ftx_set_continuation the log record point offset to
             * the start of the continuation record.  Specify the size of
             * the continuation record.  ftx_set_continutation will do a
             * bcopy out of the dlrp offset, this will guarentee alignment.
             */
            ftx_set_continuation( ftxH, dlrp->contOrUndoRBcnt,
                                 (char*)dlrp +
                                 sizeof(ftxDoneLRT));
            ftxp->type = REC_REDO;
        }

        /*
         * Get the offset of the root done record in the log record pointer.  
         * We will use this to get the acture root done record.
         */
        lrrtdnloff = ADVFS_BYTES_TO_WORDS(sizeof(ftxDoneLRT)) + ADVFS_BYTES_TO_WORDS(dlrp->contOrUndoRBcnt);
        rdwords = ADVFS_BYTES_TO_WORDS(dlrp->rootDRBcnt);


        MS_SMP_ASSERT((lrrtdnloff) <= totwordcnt);

        /* 
         * Buffer all rootdone records on the first pass
         * through ftx_recovery_pass().
         *
         * For v4 domains, this is META_META_PASS.
         * For pre-v4 domains, this is META_PASS.
         */

        if (rdwords &&
           (recoveryPass == META_META_PASS))
        {
            int nextrdoff, i;
            int32_t * dp;

            /*
             * Get enough memory to hold the root done structure.  Then
             * bcopy the rootdone record out.
             */
            rootDoneRecPtr = ms_malloc( ADVFS_WORDS_TO_BYTES( 
                        (size_t) (uint32_t) rdwords ) );
            bcopy( (char*)dlrp + (uint64_t)ADVFS_WORDS_TO_BYTES(lrrtdnloff),
                    rootDoneRecPtr, ADVFS_WORDS_TO_BYTES( rdwords ) );
            
            /*
             * buffer the root done record in the ftx state struct
             */

            if ( (nextrdoff = ftxp->nextRDoff + rdwords
                   + ADVFS_BYTES_TO_WORDS(sizeof(ftxRDHdrT)) ) > FTX_RD_BFRSZ )
            {
                domain_panic(dmnP,
                  "ftx_recovery_pass: out of buffer room for root done");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            dp = &ftxp->rootDoneRecs[ftxp->nextRDoff];
            ((ftxRDHdrT*)dp)->atomicRPass = dlrp->atomicRPass;
            ((ftxRDHdrT*)dp)->agentId = dlrp->fdl_agentId;
            ((ftxRDHdrT*)dp)->bCnt = dlrp->rootDRBcnt;
            dp += ADVFS_BYTES_TO_WORDS(sizeof(ftxRDHdrT));
            for (i = 0; i < rdwords; i++) {
                dp[i] = ((uint32_t *)rootDoneRecPtr)[i];
            }
            ftxp->nextRDoff = nextrdoff;
            ftxp->rootDnCnt += 1;
            ms_free (rootDoneRecPtr);
            rootDoneRecPtr = NULL;
        }


        /* 
         * Calculate word offset of page redo record.
         */
        redoloff =  lrrtdnloff + rdwords + ADVFS_BYTES_TO_WORDS(dlrp->opRedoRBcnt);

        /* execute all record image redo records in this record */

        while ( redoloff < totwordcnt ) {
            int i;

            MS_SMP_ASSERT( pageRedoRecPtr == NULL );
       /*
        * Here we bcopy the record to avoid potential alignment issues
        * with the data from the log. However, there are constraints on
        * the way the record was written which must be observed. These
        * are:
        *     1) We don't know how long the record is until we see the
        *        extent pointers, and
        *     2) The data starts at the end of the last use extent
        *        pointer. Hence, the record may not even be as long
        *        as sizeof(pageredoT) in the log.
        *
        * So, in the interests of clarity, we do this in three steps:
        *     1) bcopy the ftxRecRedoT portion of the record.
        *     2) re-bcopy with the size of the used extents.
        *     3) Count the real size of the record, malloc a new buffer,
        *        copy the whole record and proceed.
        * We could copy the extent pointers, but this seems more
        * straightforward.
        */

            pageRedoRecPtrTmp = (pageredoT *)ms_malloc( sizeof( pageredoT ) );
            redosize = sizeof( ftxRecRedoT );
            bcopy( ((char*)dlrp + (uint64_t)ADVFS_WORDS_TO_BYTES(redoloff) ),
                    (char*)pageRedoRecPtrTmp, redosize );

            MS_SMP_ASSERT( pageRedoRecPtrTmp->pgdesc.numXtnts <= 7 );

            redosize += ((size_t) pageRedoRecPtrTmp->pgdesc.numXtnts) *
                        sizeof( ftxRecXT );
            bcopy( ((char*)dlrp + (uint64_t)ADVFS_WORDS_TO_BYTES(redoloff) ),
                    (char*)pageRedoRecPtrTmp, redosize );

            /* Remember where we started so we can do the real copy */

	    redoloffstart = redoloff;

            MS_SMP_ASSERT((redoloff) <= totwordcnt);

            redoloff += ADVFS_BYTES_TO_WORDS(sizeof(ftxRecRedoT));
            for ( i = 0; i < pageRedoRecPtrTmp->pgdesc.numXtnts; i++ ) {
                /*
                 * Calculate the redo record offset but try to compensate
                 * for double rounding in ADVFS_BYTES_TO_WORDS.  The & 3 is
                 * to add in a few bytes if the offset in the page is not
                 * word aligned.
                 */
                redoloff += ADVFS_BYTES_TO_WORDS((pageRedoRecPtrTmp->recX[i].pgBoff & 3) +
                                 (pageRedoRecPtrTmp->recX[i].bcnt) + sizeof(ftxRecXT));
            }

	    /* Now allocate the right size buffer, copy, and free the temporary */
	    
	    redosize = (size_t) ADVFS_WORDS_TO_BYTES(redoloff-redoloffstart);
            pageRedoRecPtr = (pageredoT *)ms_malloc( redosize );
            bcopy( ((char*)dlrp + (uint64_t)ADVFS_WORDS_TO_BYTES(redoloffstart) ),
                    (char*)pageRedoRecPtr, redosize );
            ms_free(pageRedoRecPtrTmp);

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
                    MS_SMP_ASSERT( BS_BFTAG_RSVD(pageRedoRecPtr->pgdesc.tag)  &&
                                   BS_BFTAG_RBMT(pageRedoRecPtr->pgdesc.tag) );

                    ADVFS_FTX_LOGIT( FTX_REC_REDO_TRACE,
                        FTX_PASS_SCAN_REC_REDO_CALL,
                        pageRedoRecPtr->pgdesc.tag,
                        pageRedoRecPtr->pgdesc.page, 
                        pageRedoRecPtr->pgdesc.numXtnts,
                        dmnP,
                        logNilRecord,
                        dlrp,
                        dlrp->fdl_agentId,
                        "ftx_recovery_pass calling ftx_metameat_rec_redo" );

                    sts = ftx_metameta_rec_redo( dmnP, pageRedoRecPtr);
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
                     BS_BFTAG_RSVD(pageRedoRecPtr->pgdesc.tag)  ) {

                     ADVFS_FTX_LOGIT( FTX_REC_REDO_TRACE,
                             FTX_PASS_SCAN_REC_REDO_CALL,
                             pageRedoRecPtr->pgdesc.tag,
                             pageRedoRecPtr->pgdesc.page, 
                             pageRedoRecPtr->pgdesc.numXtnts,
                             dmnP,
                             logNilRecord,
                             dlrp,
                             dlrp->fdl_agentId,
                             "ftx_recovery_pass calling ftx_bfmeta_rec_redo" );
                   
                    sts = ftx_bfmeta_rec_redo( dmnP, pageRedoRecPtr);
                    if ( sts != EOK ) {
                        ms_free(dlrp);
                        goto _exit;
                    }
                }
            } else if ( BS_BFTAG_REG(pageRedoRecPtr->pgdesc.tag)  &&
                       ( recoveryPass == dlrp->atomicRPass ) )
            {
                bfSetIdT bfSetId;

                bfSetId.domainId = dmnP->domainId;
                bfSetId.dirTag = pageRedoRecPtr->pgdesc.bfsTag;

                ADVFS_FTX_LOGIT( FTX_REC_REDO_TRACE,
                        FTX_PASS_SCAN_REC_REDO_CALL,
                        pageRedoRecPtr->pgdesc.tag,
                        pageRedoRecPtr->pgdesc.page, 
                        pageRedoRecPtr->pgdesc.numXtnts,
                        dmnP,
                        logNilRecord,
                        dlrp,
                        dlrp->fdl_agentId,
                        "ftx_recovery_pass calling ftx_bfdate_rec_redo" );


                sts = ftx_bfdata_rec_redo( ftxH, bfSetId, pageRedoRecPtr);
                if ( sts != EOK ) {
                    ms_free(dlrp);
                    goto _exit;
                }
            }
            ms_free( pageRedoRecPtr );
            pageRedoRecPtr = NULL;
        }       /* done scanning for record image redo subrecords */

        /*
         * Execute the op redo routine on the appropriate pass. In this
         * case, we will pass a pointer directly into the page (the dlrp).
         * This pointer may not be aligned.   It is assumed that the opx
         * routine that is called will bcopy out of this array and get the
         * required alignment.
         */

        if ( dlrp->opRedoRBcnt && ( recoveryPass == dlrp->atomicRPass ) ) {
            char* recptr = (char*)dlrp + (uint64_t)ADVFS_WORDS_TO_BYTES((lrrtdnloff + rdwords));
            opxT* opxp;


            MS_SMP_ASSERT((lrrtdnloff + rdwords) <= totwordcnt);

            if ( (dlrp->fdl_agentId > FTX_MAX_AGENTS) ||
                !(opxp = FtxAgents[dlrp->fdl_agentId].redoOpX) )
            {
                domain_panic(dmnP, "ftx_recovery_pass: no redo opx agent");
                sts = E_DOMAIN_PANIC;
                ms_free(dlrp);
                goto _exit;
            }

            ftxp->type = OP_REDO;
            ADVFS_FTX_LOGIT( FTX_OP_REDO_TRACE,
                    FTX_PASS_SCAN_OP_REDO_CALL,
                    NilBfTag,
                    0,0,
                    dmnP,
                    logNilRecord,
                    dlrp,
                    dlrp->fdl_agentId,
                    "ftx_recovery_pass calling op redo opx" );

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


            /* 
             * Mark this slot available.  
             */
            spin_lock(&dmnP->ftxTblLock);
            ftx_release_slot(ftxSlot, ftxTDp);
            spin_unlock(&dmnP->ftxTblLock);
        }

        /* Give back the memory given to us from lgr_read */
        ms_free(dlrp);

    } while ( lrsts != W_LAST_LOG_REC );

    dmnP->state = BFD_RECOVER_FTX;

    /* loop through ftx and fail for this pass */

    for ( ftxSlot = 0; ftxSlot < ftxTDp->rrSlots; ftxSlot++) {
        lrDescT *lrvecp;

        ftxp = &ftxTDp->tablep[ftxSlot];
        if ( ftxTDp->tableSltStatep[ftxSlot] != FTX_SLOT_BUSY &&
             ftxTDp->tableSltStatep[ftxSlot] != FTX_SLOT_EXC )
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
            ADVFS_FTX_LOGIT( FTX_OP_NOTE,
                    FTX_PASS_FAILING_SUB,
                    NilBfTag,
                    0,0,
                    dmnP,
                    logNilRecord,
                    NULL,
                    FTA_NULL,
                    "ftx_recovery_pass calling ftx_fail on sub" );


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

                rdoff += ADVFS_BYTES_TO_WORDS(rdrec->bCnt) + ADVFS_BYTES_TO_WORDS(sizeof(ftxRDHdrT));
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
                ftxp->lrh.fdl_agentId = rdrec->agentId;
                ftxp->lrh.contOrUndoRBcnt = 0;

                /* execute the root-done procedure */

                ADVFS_FTX_LOGIT( FTX_RT_DONE_TRACE,
                        FTX_PASS_CALLING_RT_DONE,
                        NilBfTag,
                        0,0,
                        dmnP,
                        logNilRecord,
                        NULL,
                        rdrec->agentId,
                        "ftx_recovery_pass calling rootdone opx" );

                opxp( ftxH, rdrec->bCnt, ((char*)rdrec +
                                          sizeof(ftxRDHdrT)) );

                /*
                 * Reset log record descriptor for record header.
                 */
                lrvecp = &ftxp->lrdesc;
                lrvecp->count = 1;
                lrvecp->dataLcnt = lrvecp->bfrvec[0].bufWords =
                                   ADVFS_BYTES_TO_WORDS(sizeof(ftxDoneLRT));
                lrvecp->bfrvec[0].bufPtr = (uint32_t *)&ftxp->lrh;

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

                rdoff += ADVFS_BYTES_TO_WORDS(rdrec->bCnt) + ADVFS_BYTES_TO_WORDS(sizeof(ftxRDHdrT));
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

                spin_lock(&dmnP->ftxTblLock);
                ftx_release_slot(ftxSlot, ftxTDp);
                spin_unlock(&dmnP->ftxTblLock);
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
    if (rootDoneRecPtr != NULL) {
        ms_free(rootDoneRecPtr);
    }
    if (pageRedoRecPtr != NULL) {
        ms_free(pageRedoRecPtr);
    }
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

    sts = lgr_read( LR_BWD, logrh, &lastlogaddr, (uint32_t**)&dlrp,
                   &wordcnt, &totwordcnt );
    if ( sts != EOK && (sts < MSFS_FIRST_ERR) ) {
	if ( sts == E_LOG_EMPTY ) {
	    goto logclose;
	} else {
            domain_panic(dmnP, "num_of_log_recs: lgr_read error = %d.", sts);
            if ( dlrp ) ms_free(dlrp);
            goto logclose;
	}
    } else if ( wordcnt < ADVFS_BYTES_TO_WORDS(sizeof(ftxDoneLRT)/4) ) {
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
                         dlrp->bfDmnId.id_sec, dlrp->bfDmnId.id_usec);
            ms_free(dlrp);
            goto logclose;
        }
    }
    
    crashRedo = dlrp->crashRedo;
    nrecs = 0;

    /* Give back the memory given to us from lgr_read */

    ms_free(dlrp);
    
    do {
        sts = lgr_read( LR_FWD, logrh, &crashRedo, (uint32_t**)&dlrp,
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

    ADVFS_FTX_LOGIT( FTX_RT_DONE_TRACE,
            FTX_PASS_CALLING_RT_DONE,
            NilBfTag,
            nrecs,0,
            dmnP,
            logNilRecord,
            NULL,
            FTA_NULL,
            "num_of_log_recs returning, off is count" );

    
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
 * each pass of ftx_recovery_pass() reads the same records--
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
 * The chain of xidInfoT's hangs off of dmnP->xidRecovery.xid_head.
 * The last struct in the chain is pointed to by dmnP->xidRecovery.xid_tail.
 * The next free slot in the tail is contained in
 * dmnP->xidRecovery.xid_current_free_slot.
 *
 * Return values: EOK, ENO_MORE_MEMORY.
 *
 * If this routine does not return EOK, a subsequent call to
 * cfs_check_for_pfs_commit() for this xid will simply return
 * CFS_XID_NOT_FOUND.
 */
static
int
cfs_list(domainT *dmnP, /* in */
         ftxIdT ftxId,  /* in */
         int done_flag  /* in -- TRUE/FALSE [committed/rolled back] */ )
{
    xidInfoT *xidRecoveryp;
    xidInfoT **xidRecoverypp;
    int *current_free_slot;

    ADVRWL_XID_RECOVERY_WRITE(dmnP);

    /*
     * Allocate a new xidInfoT if head is NULL or the current
     * free slot in the tail is the last slot.
     * The last slot of each xidInfo is always kept NULL.
     */

    current_free_slot = &(dmnP->xidRecovery.xid_current_free_slot);
    if ((dmnP->xidRecovery.xid_head == NULL) 
		|| (*current_free_slot == (NUM_XIDS-1))) {

        if (dmnP->xidRecovery.xid_head == NULL) {
            xidRecoverypp = &(dmnP->xidRecovery.xid_head);
        } else {
            xidRecoverypp = &(dmnP->xidRecovery.xid_tail->xid_next); 
        }

        *xidRecoverypp = (xidInfoT *) ms_malloc(sizeof(xidInfoT));
        dmnP->xidRecovery.xid_tail = *xidRecoverypp;
        *current_free_slot = 0; 
    } 

    /* 
     *  The xid status is kept in the high-order bit,
     *  originally used to indicate whether this ftxId
     *  was an xid.
     */

    xidRecoveryp = dmnP->xidRecovery.xid_tail;
    xidRecoveryp->xid_data[*current_free_slot] = ftxId; 
    if (done_flag) {
        xidRecoveryp->xid_data[*current_free_slot] |= (1L<<63);
    } else {
        xidRecoveryp->xid_data[*current_free_slot] &= ~(1L<<63);
    }
    (*current_free_slot)++;

    ADVRWL_XID_RECOVERY_UNLOCK(dmnP);
 
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
 */
cfs_check_for_pfs_commit(fsid_t fsid,  /* in */
                         ftxIdT xid    /* in */)
{
    struct vfs *vfsp = NULL; 
    struct vfs *pvfsp = NULL;
    domainT    *dmnP=NULL;
    xidInfoT   *xidInfop;
    ftxIdT     xid_lo;
    ftxIdT     loop_xid;
    int        slot;
    int        ret;

    if ((vfsp = getvfs(fsid, GETVFS_NONINTERRUPT)) != NULL) {
        CLU_GETPFSVFSP_BLOCK(vfsp, &pvfsp, ret); 
        if (ret != ESUCCESS) {
            ADVFS_SAD0("cfs_check_for_pfs_commit:could not get pfs mount point");
        }
        dmnP = ADVGETDOMAINP (pvfsp);
    } else {
        return CFS_XID_NOT_FOUND;
    }

    ADVRWL_XID_RECOVERY_READ(dmnP);

    xid_lo = xid; 
    xid_lo &= ~(1L<<63);		/* clear the high bit */

    xidInfop = dmnP->xidRecovery.xid_head;
    while (xidInfop != NULL) {
        slot=0;

        while (xidInfop->xid_data[slot] != NULL) {
            loop_xid = xidInfop->xid_data[slot];
            loop_xid &= ~(1L<<63);		/* clear the high bit */

            /*  
             * Compare the lower bits of the incoming xid with
             * the lower bits of the stored xid. 
             */

            if (xid_lo == loop_xid) {

                /* 
                 * If the high bit is set, this xid committed.
                 * Otherwise, it was rolled back.
                 */

		if (xidInfop->xid_data[slot] & (1L<<63)) {
                    ret = TRUE;
		} else {
                    ret = FALSE;
                }
                ADVRWL_XID_RECOVERY_UNLOCK(dmnP);
                vfs_rele(vfsp);            /* ref was taken by getvfs() */
                return ret;
            }
            slot++;
        }  
        xidInfop = xidInfop->xid_next;
    } 

    ADVRWL_XID_RECOVERY_UNLOCK(dmnP);
    vfs_rele(vfsp);                        /* ref was taken by getvfs() */  
    return CFS_XID_NOT_FOUND;
}

/*
 * cfs_xid_free_memory
 *
 * This routine is called via timeout() from ftx_bfdmn_recovery(),
 * executed 15 minutes after that routine has completed.
 * Note it is executing in interrupt context.
 *
 * Checks are made to protect against races with dmn_dealloc().
 */
void
cfs_xid_free_memory(char *domainIdP)
{
    advfs_handyman_thread_msg_t *msg;
    extern msgQHT advfs_handyman_msg_queue;

    msg = (advfs_handyman_thread_msg_t *)
           msgq_alloc_msg_nowait(advfs_handyman_msg_queue);

    if (msg) {
        msg->ahm_msg_type = AHME_CFS_XID_FREE_MEMORY;
        msg->ahm_data.ahm_xid_free_dmnIdP = (bfDomainIdT *)domainIdP;
        msgq_send_msg( advfs_handyman_msg_queue, msg );
    }
    else{
        /*
         * Could not get a message buffer so reissue timeout.
         */
        timeout((void(*))cfs_xid_free_memory, domainIdP, (1*10*HZ));
    }
    return;
}


/* 
 * cfs_xid_free_memory_int
 *
 * Called from advfs_handyman_cfs_xid_free_memory() and dmn_dealloc().
 */
void
cfs_xid_free_memory_int(domainT* dmnP)
{
    xidInfoT *xidInfop, *next;

    xidInfop = dmnP->xidRecovery.xid_head;
    while (xidInfop != NULL) {
        next = xidInfop->xid_next;
        ms_free(xidInfop);
        xidInfop = next;
    } 
    dmnP->xidRecovery.xid_head = NULL;
    dmnP->xidRecovery.xid_tail = NULL;
    dmnP->xidRecovery.xid_current_free_slot = 0;
    dmnP->xidRecovery.xid_timestamp.tv_sec = 0;
    dmnP->xidRecovery.xid_timestamp.tv_usec = 0;

}


/*
 * advfs_handyman_cfs_xid_free_memory
 *
 * This routine is called via the advfs_handyman_thread 
 * in response to receiving a mesage from cfs_xid_free_memory(). 
 * This routine is responsible for freeing the malloced memory that
 * domainIdP points to.
 */
void
advfs_handyman_cfs_xid_free_memory(bfDomainIdT *domainIdP)
{
    domainT *dmnP;
    struct timeval ltime;
    statusT sts;

    sts = bs_domain_access( &dmnP, *domainIdP, FALSE );
    if (sts != EOK)
    {
        goto EXIT2;
    }

    if (dmnP->state != BFD_ACTIVATED)
    {
        goto EXIT1;
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

    if ((dmnP->xidRecovery.xid_timestamp.tv_sec == 0) &&
        (dmnP->xidRecovery.xid_timestamp.tv_usec == 0))
    {
        goto EXIT1;
    }

    ltime = get_system_time();
    if (ltime.tv_sec - dmnP->xidRecovery.xid_timestamp.tv_sec < XID_TIMEOUT)
    {
        goto EXIT1;
    }

    if (ADVRWL_XID_RECOVERY_TRY_WRITE(dmnP) == FALSE) {
        /*
         * At this point, not racing with dmn_dealloc().
         * Legit to do a timeout.
         */
        timeout((void(*))cfs_xid_free_memory, (char *)domainIdP, (1*60*HZ));

        bs_domain_close(dmnP);
        return;
    }
    cfs_xid_free_memory_int(dmnP);
    ADVRWL_XID_RECOVERY_UNLOCK(dmnP);
EXIT1:
    bs_domain_close(dmnP);
EXIT2:
    ms_free(domainIdP);
}
