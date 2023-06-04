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
 *  AdvFS Storage System
 *
 * Abstract:
 *
 *  ftx_routines.c
 *  This is the main implementation module for the Flyweight 
 *  Transactions (ftx) functions.
 *
 */

#define ADVFS_MODULE FTX_ROUTINES

#include <limits.h>
#include <sys/time.h>
#include <sys/user.h>

#include <ms_public.h>
#include <ms_privates.h>
#include <ftx_privates.h>
#include <ftx_agents.h>
#include <ms_assert.h>
#include <bs_access.h>
#include <tcr/clu.h>

ftxHT FtxNilFtxH = {0,0}; /* Nil ftx for use as root parent */

/*
 * Crude transaction profiling:  Count the number of ftx_done's for
 * each agent.  It might also be interesting to record cumulative time
 * spent within each agent, but for this ftx_start would have to be
 * modified to also pass the agent Id.
 */

int FtxStats = 0;

/* global type to set default ftx behavior sync or async */

logWriteModeT FtxDoneMode = LW_ASYNC;

/**************************************
 * module specific function prototypes
 *************************************/

/*
 * addone_undo - add undo record to log vector
 *
 * recptr must be int aligned.
 */

static
void
addone_undo (
             void* ptr,         /* in - ptr to undo record */
             ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
             lrDescT* lrdp      /* in/out - ptr to log rec desc */
             );

/*
 * addone_redo - add redo record to log vector
 *
 * recptr must be int aligned.
 */

static
void
addone_redo (
             void* ptr,         /* in - ptr to redo record */
             ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
             lrDescT* lrdp      /* in/out - ptr to log rec desc */
             );

/*
 * addone_rtdn - add rtdn record to log vector
 *
 * recptr must be int aligned.
 */

static
void
addone_rtdn (
             void* ptr,         /* in - ptr to rtdn record */
             ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
             lrDescT* lrdp      /* in/out - ptr to log rec desc */
             );

/*
 * addone_cont - add cont record to log vector.  This uses the
 * rootdone part of the record when the ftx level is zero.
 *
 * recptr must be int aligned.
 */

void
addone_cont(
            void* recptr,      /* in - ptr to redo record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            );

#ifdef ADVFS_FTX_TRACE
void
ftx_trace( ftxStateT *ftxp,
           uint16_t  module,
           uint16_t  line,
           void      *value)
{
     ftxTraceElmtT *te;
    extern mutexT TraceLock;
    extern int TraceSequence;

    ADVMTX_TRACE_LOCK(&TraceLock);

    ftxp->trace_ptr = (ftxp->trace_ptr + 1) % FTX_TRACE_HISTORY;
    te = &ftxp->trace_buf[ftxp->trace_ptr];
    te->thd = (struct kthread *)(((long)current_cpu() << 36) |
                             (long)u.u_kthreadp & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    ADVMTX_TRACE_UNLOCK(&TraceLock);
}
#endif /* ADVFS_FTX_TRACE */

/*
 * ftx_register_agent, ftx_register_agent_n, ftx_register_agent_n2
 *
 * This routine must be called prior to using any other ftx functions.
 */

ftxAgentT FtxAgents [FTX_MAX_AGENTS];

/* TODO: temp ******/


statusT
ftx_register_agent(ftxAgentIdT agentId, /* in - agent id */
                   opxT *undoOpX,   /* in - undo opx routine ptr */
                   opxT *rootDnOpX  /* in - root done opx routine ptr */
                                /* Note - opX routines must fail !! */
                   )
{
    return ftx_register_agent_n2( agentId, undoOpX, rootDnOpX, 0, 0 );
}


statusT
ftx_register_agent_n(ftxAgentIdT agentId, /* in - agent id */
                     opxT *undoOpX,       /* in - undo opx proc ptr */
                     opxT *rootDnOpX,     /* in - root done opx proc ptr */
                     opxT *redoOpX        /* in - redo opx proc ptr */
                                /* Note - opX routines must NOT fail !! */
                   )
{
    return ftx_register_agent_n2( agentId, undoOpX, rootDnOpX,
                                 redoOpX, 0 );
}


statusT
ftx_register_agent_n2(ftxAgentIdT agentId, /* in - agent id */
                      opxT *undoOpX,       /* in - undo opx proc ptr */
                      opxT *rootDnOpX,     /* in - root done opx proc ptr */
                      opxT *redoOpX,       /* in - redo opx proc ptr */
                      opxT *contOpX        /* in - continuation opx */
                                /* Note - opX routines must NOT fail !! */
                   )
{
    unsigned int i;

    /* The agent id is a direct 1-based index into the agent table. */

    if ((i = agentId ) > FTX_MAX_AGENTS-1) {
        return EBAD_FTX_AGENTH;
    }

    /* Agent "0" is the "nil" agent and cannot be registered. */

    if (i == 0) {
        return EBAD_FTX_AGENTH;
    }

    FtxAgents[i].id = agentId;
    FtxAgents[i].undoOpX = undoOpX;
    FtxAgents[i].rootDnOpX = rootDnOpX;
    FtxAgents[i].redoOpX = redoOpX;
    FtxAgents[i].contOpX = contOpX;
    return EOK;
}

/*
 * The ftx handle table and the last ftx id assigned are in the
 * domain structure.  These are protected by the domainT.ftxTblLock.
 * Note that these structures are only manipulated when a root ftx 
 * starts and finishes, and not for subtransactions.
 *
 * *******COSMIC ASSUMPTION********.
 * An ftx is used by only one thread at a time.  Eventually, the ftx
 * handle could/should be part of the thread context and not passed to
 * any recoverable services directly.  This avoids the need to
 * synchronize on anything but the global ftx id.
 * *******EOCOSMIC ASSUMPTION*******.
 */

statusT
_ftx_start_i(
      ftxHT *ftxH,              /* out - ftx handle */
      ftxHT parentFtxH,         /* in - parent ftx handle */
      domainT *dmnP,            /* in - domain pointer */
      unsigned int atomicRPass, /* in - atomic recovery pass */
      int flag,                 /* in - 1 == start exclusive ftx */
      ftxIdT xid                /* in - CFS-generated transaction id */
      )
{
    uint32_t ftxSlot;
    ftxStateT* ftxp;
    unsigned int lvl;
    perlvlT* clvlp;
    statusT sts;
    ftxHT retFtxH;
    ftxTblDT* ftxTDp;
    int trimwait = 0;

    if (dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }

    /*
     * Set pointer to ftx table descriptor.
     */

    ftxTDp = &dmnP->ftxTbld;

    if (parentFtxH.hndl == 0) {

        int wait = 0;

        /***************************/
        /* This is a new root ftx. */
        /***************************/

        /*
         * Catch a thread that has locked a lock before it has started
         * a transaction.  The thread should lock it after starting the
         * transaction.
         */
#ifdef ADVFS_SMP_ASSERT
        ADVRWL_DOMAIN_FTXSLOT_READ(&dmnP->ftxSlotLock);
#endif

        spin_lock(&dmnP->ftxTblLock);

#ifdef ADVFS_SMP_ASSERT
        if ( AdvfsEnableAsserts ) {
            /*
             * check for more than one root transaction for this thread
             */
            struct kthread *thread = u.u_kthreadp; 

            for ( ftxSlot = 0; ftxSlot < ftxTDp->rrSlots; ftxSlot++ ) {
                if ((ftxTDp->tableSltStatep[ftxSlot] == FTX_SLOT_EXC || 
                     ftxTDp->tableSltStatep[ftxSlot] == FTX_SLOT_BUSY)) {
                    MS_SMP_ASSERT(ftxTDp->tablep[ftxSlot].thd != thread);
                }
            }
            MS_SMP_ASSERT(ftxTDp->rrNextSlot < ftxTDp->rrSlots);
        }
#endif /* ADVFS_SMP_ASSERT */

        if ( FtxStats ) {
            int slots = ftxTDp->rrNextSlot - ftxTDp->oldestSlot;

            if ( slots < 0 ) {
                slots += ftxTDp->rrSlots;
            }
            /* These slots can't be used even if they are FTX_SLOT_AVAIL */
            if ( slots > dmnP->logStat.maxFtxTblSlots ) {
                dmnP->logStat.maxFtxTblSlots = slots;
                dmnP->logStat.oldFtxTblAgent =
                      ftxTDp->tablep[ftxTDp->oldestSlot].lrh.fdl_agentId;
            }
        }

        /* Put the caller to sleep until a slot becomes available. */
        while (ftxTDp->tableSltStatep[ftxTDp->rrNextSlot] != FTX_SLOT_AVAIL){
            /* The caller doesn't want to wait */
            if (flag & FTX_NOWAIT) {
                spin_unlock(&dmnP->ftxTblLock);
#ifdef ADVFS_SMP_ASSERT
                ADVRWL_DOMAIN_FTXSLOT_UNLOCK(&dmnP->ftxSlotLock);
#endif
                return EWOULDBLOCK;
            }

            if (AdvfsLockStats) {
                AdvfsLockStats->ftxSlotWait++;
                if (ftxTDp->tableSltStatep[ftxTDp->rrNextSlot] == 
                    FTX_SLOT_EXC )
                    dmnP->logStat.excSlotWaits++;
                else
                    dmnP->logStat.fullSlotWaits++;
            }

            /* Waiters get woken up when the next slot becomes available
             * when the next slot rrNextSlot gets incremented or 
             * a call to ftx_release_slot() changes a slot to AVAIL status.
             */
            ftxTDp->ftxWaiters++;
            cv_wait(&ftxTDp->slotCv,&dmnP->ftxTblLock,CV_SPIN,CV_DFLT_FLG);
            ftxTDp->ftxWaiters--;
	}

        ftxSlot = ftxTDp->rrNextSlot;
        ftxp = &ftxTDp->tablep[ftxSlot];
        MS_SMP_ASSERT(ftxp != 0);

        ftxp->thd = u.u_kthreadp;

        /* Initialize before potentially losing CPU in excl wait loop */
        ftxp->firstLogRecAddr.lsn = nilLSN;

        /*
         * Get a new ftx id.  The only constraint on this id is that
         * it must be unique for all outstanding ftx trees, that is,
         * ones that have something in the log.
         *
         * CFS: If a non-zero (CFS-client-generated) xid was passed in to
         * this function, use it for the ftxId.
         */
        if (xid == 0) {
            ftxTDp->lastFtxId += 1;
            /* if high-order bit set after incr, roll over to FtxId == 1 */
            if ((ftxTDp->lastFtxId >> 63) == 1)
                ftxTDp->lastFtxId = 1;
            ftxp->lrh.ftxId = ftxTDp->lastFtxId;
        } else {
            /*
             * set ftxId to passed-in xid. 
             * High-order must be set to indicate this is an externally-
             * generated ftxId.
             */
            MS_SMP_ASSERT(xid & 0x8000000000000000);
            ftxp->lrh.ftxId = xid;
        }

        if ( flag & FTX_EXC ) {
            ftxTDp->tableSltStatep[ftxSlot] = FTX_SLOT_EXC;
        } else {
            ftxTDp->tableSltStatep[ftxSlot] = FTX_SLOT_BUSY;
        }

        if (ftxTDp->slotUseCnt < 0) {
            ADVFS_SAD1( "_ftx_start_i: bad count", ftxTDp->slotUseCnt );
        }
        ++ftxTDp->slotUseCnt;

        /*
         * If this is an exclusive transaction we must wait until all
         * slots are free excluding this exclusive transaction's slot
         * (slotUseCnt - 1 == 0).
         * Note that since we've marked the ftx slot busy all new root ftxs
         * will block until we finish this exclusive transaction. This is also
         * because we don't advance the round robin pointer to the
         * next slot. This holds the pointer at our busy slot.
         *
         * We must also wait if the log is being trimmed.  The actual
         * log flushing occurs either when the last active transaction
         * tree completes, or in between ftx continuations.
         */

        wait = 0;
        while ( (flag & FTX_EXC) && ((ftxTDp->slotUseCnt - 1) > 0) )  {
            if ( AdvfsLockStats ) {
                if (wait) {
                    AdvfsLockStats->ftxExcReWait++;
                } else {
                    wait = 1;
                }
                AdvfsLockStats->ftxExcWait++;
            }

            ftxTDp->excWaiters++;
            cv_wait(&ftxTDp->excCv, &dmnP->ftxTblLock, 
                    CV_SPIN, CV_DFLT_FLG);
            ftxTDp->excWaiters--;
        }

        /* Now wait on the trimCv.  We will be awakened whenever 
         * the oldest lsn is reset.
         */
        wait = 0;
        if ( !LSN_EQ_NIL( dmnP->ftxTbld.logTrimLsn ) ) {
            if ( AdvfsLockStats ) {
                if (wait) {
                    AdvfsLockStats->ftxTrimReWait++;
                } else {
                    wait = 1;
                }
                AdvfsLockStats->ftxTrimWait++;
            }

            trimwait = 1;
            ftxTDp->trimWaiters++;
            cv_wait(&ftxTDp->trimCv, &dmnP->ftxTblLock, 
                    CV_SPIN, CV_DFLT_FLG);
            ftxTDp->trimWaiters--;
        }

        if (!(flag & FTX_EXC)) {
            /* Advance round robin pointer to the next slot for the next ftx */
            ftxTDp->rrNextSlot = (ftxTDp->rrNextSlot + 1) % ftxTDp->rrSlots;
        }

        ++ftxTDp->noTrimCnt;

        /* Since we've bumped the next slot, wake a waiter only if it
         * is AVAILABLE.
         */
        if (!(flag & FTX_EXC) &&
            (ftxTDp->ftxWaiters > 0) &&
            (ftxTDp->tableSltStatep[ftxTDp->rrNextSlot] == FTX_SLOT_AVAIL)){

            spin_unlock(&dmnP->ftxTblLock);

            if ( AdvfsLockStats ) {
                uint64_t dummy;
                ADVFS_ATOMIC_FETCH_INCR(&AdvfsLockStats->ftxSlotSignalNext,
                                        &dummy);
            }

            /* If we just trimmed the log making lots of slots available,
             * then broadcast to the waiters in hopes of many getting to run.
             */
            if ( trimwait ) {
                cv_broadcast( &ftxTDp->slotCv, NULL, CV_NULL_LOCK );
            } else {
                cv_signal( &ftxTDp->slotCv, NULL, CV_NULL_LOCK );
                }
        }
        else 
            spin_unlock(&dmnP->ftxTblLock);

        /* Initialize the new root ftx. */

        ftxp->type = NORMAL;
        ftxp->currLvl = 0;
        ftxp->lastLogRec = logEndOfRecords;

        ftxp->lastFtxPinS = -1;
        if ( atomicRPass ) {
            ftxp->cLvl[0].atomicRPass = atomicRPass;
        } else {
            ftxp->cLvl[0].atomicRPass = FTX_MAX_RECOVERY_PASS;
        }

        /* Initialize done record */

        ftxp->lrh.type = ftxDoneLR;
        ftxp->lrh.bfDmnId = dmnP->domainId;

        lvl = FTX_MX_NEST;
        do {
            lvl -= 1;
            clvlp = &ftxp->cLvl[lvl];
            clvlp->member = 0;
        } while (lvl > 0);

        ftxp->rootDnCnt = 0;
        ftxp->nextRDoff = 0;
        ftxp->contDesc.agentId = 0;

        /* end of the "new root" ftx block of code */
    }

    /************************************************************
     * The following sequence of "if" tests are all for the
     * case where this is a new child ftx.
     ************************************************************/

    else {
        if ( parentFtxH.dmnP != dmnP ) {
            MS_SMP_ASSERT( FALSE );
            return EBAD_PAR_FTXH;
        }

        /* Pick up parent ftx slot and range check. */
        if ((ftxSlot = parentFtxH.hndl - 1) >= ftxTDp->rrSlots) {
            MS_SMP_ASSERT( FALSE );
            return EBAD_PAR_FTXH;
        }

        /* Save a pointer to the slot's ftx structure */
        ftxp = &ftxTDp->tablep[ftxSlot];
     
        /* check if level in parent handle matches current level */
        if (ftxp->currLvl != parentFtxH.level) {
            MS_SMP_ASSERT( FALSE );
            return EBAD_PAR_FTXH;
        }

        /* increment current transaction level */
        if ((lvl = ftxp->currLvl +=1) == FTX_MX_NEST) {
            ftxp->currLvl -= 1;
            return EFTX_TOO_DEEP;
        }

        /* increment child member, check for overflow */
        if ((ftxp->cLvl[lvl].member += 1) == 0) {
            ftxp->currLvl -= 1;
            return EFTX_TOO_DEEP;
        }
        else {
            clvlp = &ftxp->cLvl[lvl];
            if ( atomicRPass != 0 ) {
                if ( ftxp->cLvl[lvl - 1].atomicRPass != 0 ) {
                    /* An atomic recovery pass is currently active. */
                    if ( RECOV_PASS_LTE(dmnP,
                                        atomicRPass,
                                        ftxp->cLvl[lvl-1].atomicRPass) ) {
                        clvlp->atomicRPass = atomicRPass;
                    }
                    else {
                        ADVFS_SAD0("_ftx_start_i: bad rpass nesting");
                    }
                }
                else {
                    clvlp->atomicRPass = atomicRPass;
                }
            }
            else {
                /*
                 * No particular recovery pass is specified, so propagate the
                 * current recovery pass to the current level.
                 */
                clvlp->atomicRPass = ftxp->cLvl[lvl-1].atomicRPass;
            }
        }
    }
    /*
     * Common (root or child) ftx initialization.
     */

    clvlp->donemode = FTXDONE_NORMAL;
    clvlp->lkList = NULL;
    clvlp->skipSubsLink = ftxp->lastLogRec;
    clvlp->lastPinS = -1;

    /* return ftx handle */

    retFtxH.hndl = ftxSlot + 1;
    retFtxH.level = lvl;
    retFtxH.dmnP = dmnP;
    *ftxH = retFtxH;

    return EOK;
}

/*
 * ftx_done
 *
 * Ftx_done commits the current ft action.  This guarantees the
 * results of this ft action will be recovered regardless of
 * subsequent system or media failures.
 *
 * Ftx_done cannot fail.
 *
 * The specified operational action records
 * are captured at this time, and they may be supplied to the
 * operational recovery routine at any time after ftx_done is
 * called.
 *
 * undoOp - ftx_done_u
 *
 * An undo operation must be supplied when this ftx is a child and the
 * action performed in the ftx must be undone if the parent fails.
 * As noted above, consistency
 * locks are released at this point, so we cannot use record update
 * before images to roll back - this must be a "logical" or
 * "operational" action.
 *
 * The undoOp action may also be executed if a parent ftx subsequently
 * calls ftx_fail.
 *
 * rootDnOp - ftx_done_urdr
 *
 * This action specifies an action to be taken only if the root ftx
 * calls ftx_done.  In this case, the deferred action must not depend
 * on the to acquire any limited (quota-controlled or physically
 * limited) resources, such as storage.  In other words, whatever
 * resources a deferred action requires must have been reserved by
 * the current ftx.
 *
 * redoOp - ftx_done_urdr
 *
 * If supplied, this operational redo action will be performed during
 * crash recovery.  If this operation is not virtual disk specific
 * (most ops should not be) then this redo_op could be executed during
 * media recovery also.  This is currently only intended to keep
 * in-memory data structures consistent with record image updates made
 * to on-disk data structures.
 *
 */

/******** TODO: temp ********/

statusT
ftx_done(
         ftxHT ftxH,            /* in - leaf ftx handle */
         ftxAgentIdT agentId,   /* in - opx agent id */
         int undoOpSz,          /* in - size of op undo struct */
         void* undoOp,          /* in - ptr to op undo struct */
         int redoOpSz,          /* in - size of op redo struct */
         void* redoOp,          /* in - ptr to op redo struct */
         int rootDnOpSz,        /* in - size of root done struct */
         void* rootDnOp         /* in - ptr to root done struct */
         )
{
    ftx_done_urdr( ftxH, agentId, undoOpSz, undoOp, rootDnOpSz,
                  rootDnOp, redoOpSz, redoOp );
    return EOK;
}

void
ftx_done_fs(
            ftxHT ftxH,            /* in - leaf ftx handle */
            ftxAgentIdT agentId    /* in - opx agent id */
            )
{
    domainT *dmnP;
    struct bfsQueue *entry;
    bfSetT *bfSetp;
    struct vfs *vfsp;
    int32_t do_sync = FALSE;

    /*
     * Check whether cfs is stacked on top.
     */
    if(clu_is_ready())  {
        int32_t vfs_flags;
	dmnP = ftxH.dmnP;
	ADVMTX_DOMAIN_LOCK( &dmnP->dmnMutex );
	entry = dmnP->bfSetHead.bfsQfwd;
        bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
        vfsp = ADVGETVFS(bfSetp);
	if(vfsp)  {
            vfs_flags = vfsp->vfs_flag;
	}
        ADVMTX_DOMAIN_UNLOCK( &dmnP->dmnMutex );
        if ( (vfs_flags & VFS_CFSONTOP) &&
             ( ((agentId == FTA_FS_CREATE_1) &&
                (!(vfs_flags & VFS_SERVERONLY))) ||
               CLU_IS_ICSDAEMON()) ) {
            do_sync = TRUE;
        }
    }
        
    /*
     * Synchronous commit of the transaction is required if
     *     - in the context of an NFS server process
     * OR
     *     - running in a cluster and a client transaction is being
     *       commited
     * OR
     *     - running in a cluster without the server_only option and
     *       within a create transaction (client or server)
     *       
     * Note that the log will synch on a CFS server for the create ftx
     * unless server_only is enabled.  The log sync is required to
     * fix a failover-related bug.  However, no failover is possible
     * if the mount -o server_only is used.  Therefore, the log sync is
     * not needed for server_only creates and saves a log sync I/O.
     */

    if (NFS_SERVER || do_sync) { 
        ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );
    }

    ftx_done_urdr( ftxH, agentId, 0,0,0,0,0,0 );
}

void
ftx_done_n(
         ftxHT ftxH,            /* in - leaf ftx handle */
         ftxAgentIdT agentId    /* in - opx agent id */
         )
{
    ftx_done_urdr( ftxH, agentId, 0,0,0,0,0,0 );
}

void
ftx_done_u(
           ftxHT ftxH,          /* in - leaf ftx handle */
           ftxAgentIdT agentId, /* in - opx agent id */
           int undoOpSz,        /* in - size of op undo struct */
           void* undoOp         /* in - ptr to op undo struct */
           )
{
    ftx_done_urdr( ftxH, agentId, undoOpSz, undoOp, 0,0,0,0 );
}

void
ftx_done_urd(
             ftxHT ftxH,        /* in - leaf ftx handle */
             ftxAgentIdT agentId, /* in - opx agent id */
             int undoOpSz,      /* in - size of op undo struct */
             void* undoOp,      /* in - ptr to op undo struct */
             int rootDnOpSz,    /* in - size of root done struct */
             void* rootDnOp     /* in - ptr to root done struct */
             )
{
    ftx_done_urdr( ftxH, agentId, undoOpSz, undoOp, rootDnOpSz, rootDnOp,0,0 );
}

void
ftx_done_urdr(
              ftxHT ftxH,       /* in - leaf ftx handle */
              ftxAgentIdT agentId, /* in - opx agent id */
              int undoOpSz,     /* in - size of op undo struct */
              void* undoOp,     /* in - ptr to op undo struct */
              int rootDnOpSz,   /* in - size of root done struct */
              void* rootDnOp,   /* in - ptr to root done struct */
              int redoOpSz,     /* in - size of op redo struct */
              void* redoOp      /* in - ptr to op redo struct */
              )
{
    unsigned int ftxSlot;
    ftxStateT* ftxp;
    statusT sts;
    domainT* dmnP = ftxH.dmnP;
    ftxTblDT* ftxTDp;
    unsigned int lvl;
    lrDescT *lrvecp = NULL;


    if ( dmnP == NULL){
        ADVFS_SAD1("ftx_done_urdr: bad domain pointer", EBAD_DOMAIN_POINTER);
    }

    ftxTDp = &dmnP->ftxTbld;

    /* Range check handle. */
    if ((ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD1("ftx_done_urdr: bad ftx handle slot ",ftxSlot);
    }

    /* Save a pointer to the slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    /* check if level in handle matches current level. */

    if ((lvl = ftxp->currLvl) != ftxH.level) {
        ADVFS_SAD2("ftx_done_urdr: handle level N1 doesn't match ftx lvl N2",
                 ftxH.level, lvl);
    }

    /*
     * range check agent id - 0 means no opx agent.  However,
     * a 0 agent ID causes crash testing to be incomplete.
     */
    if ( agentId > FTX_MAX_AGENTS) {
        ADVFS_SAD1("ftx_done_urdr: agent id N1 out of range", agentId);
    }

    if ( ftxp->type != NORMAL ) {
        /*
         * This is part of undo/rootdone and not a "real" done.
         */
        ADVFS_SAD1("ftx_done_urdr: N1 not NORMAL ftx", ftxp->type );
    }

    /* common log record header initialization */

    ftxp->lrh.level = lvl;
    ftxp->lrh.atomicRPass = ftxp->cLvl[lvl].atomicRPass;
    ftxp->lrh.member = ftxp->cLvl[lvl].member;
    ftxp->lrh.fdl_agentId = agentId;

    /* Init log record descriptor for record header. */
    lrvecp = &ftxp->lrdesc;
    lrvecp->count = 1;
    lrvecp->dataLcnt = lrvecp->bfrvec[0].bufWords = (sizeof(ftxDoneLRT)+3)/4;
    lrvecp->bfrvec[0].bufPtr = (uint32_t *)&ftxp->lrh;

    if (lvl != 0) {

        /****************************************************/
        /******** This block is the child ftx case **********/
        /****************************************************/

        /* check for op undo record */

        if ( (ftxp->lrh.contOrUndoRBcnt = undoOpSz) ) {
            addone_undo( undoOp, ftxp, lrvecp );
        }

        /* Check for root-done-op record and buffer it if present */

        if ( (ftxp->lrh.rootDRBcnt = rootDnOpSz) ) {
            addone_rtdn( rootDnOp, ftxp, lrvecp );
        }

        /* check for op redo record */

        if ( (ftxp->lrh.opRedoRBcnt = redoOpSz) ) {
            addone_redo( redoOp, ftxp, lrvecp );
        }

        /* buffer record image redo record */

        addone_recredo( ftxp, lrvecp );

        ftxp->undoBackLink = ftxp->lastLogRec;

        /* Log the "Done" record. */
        /*
         * Log the ftx done record with the redo, undo, and root done
         * information buffered above.  Release any locks assoicated
         * with this ftx and unpin pages pinned during the ftx.
         */

        /* log_donerec_nunpin() now returns statusT instead of void.
         * This if statement is a place holder for the handling of that
         * return value.
         */
        if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

        release_ftx_locks( &ftxp->cLvl[lvl] );

        /* reduce current ftx level */

        ftxp->currLvl = lvl - 1;

        return;
    }

    /***************************************************/
    /***** This is the root ftx completion case ********/
    /***************************************************/

#ifdef ADVFS_SMP_ASSERT
    ADVRWL_DOMAIN_FTXSLOT_UNLOCK(&dmnP->ftxSlotLock);
#endif

    /* never any undo for root ftx */

    ftxp->lrh.contOrUndoRBcnt = 0;

    /* Check for root-done-op record and buffer it if present */

    if ( (ftxp->lrh.rootDRBcnt = rootDnOpSz) ) {
        addone_rtdn( rootDnOp, ftxp, lrvecp );
    }

    /* Check for op redo record and buffer if present */

    if ( (ftxp->lrh.opRedoRBcnt = redoOpSz) ) {
        addone_redo( redoOp, ftxp, lrvecp );
    }

    /* buffer record image redo */

    addone_recredo( ftxp, lrvecp );

    ftxp->undoBackLink = logEndOfRecords;

    /*
     * set ftx member to one if there are any buffered root done
     * records (else it will be zero).
     */

    if ( ftxp->lrh.member = ftxp->rootDnCnt ) {  /* = is right! */
        ftxp->lrh.member = 1;
    }

    /*
     * Log the ftx done record with the redo, undo, and root done
     * information buffered above.  Release any locks assoicated
     * with this ftx and unpin pages pinned during the ftx.
     */

    /* log_donerec_nunpin() now returns statusT instead of void.
     * This if statement is a place holder for the handling of that
     * return value.
     */
    if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

    release_ftx_locks( &ftxp->cLvl[0] );

    /*
     * Check if any root done procedures are buffered.  If so,
     * execute them.  Note that no subtransactions are allowed.
     */

    if ( ftxp->nextRDoff ) {
        int rdoff = 0;

        ftxp->type = ROOTDONE;

        /*
         * Reset the log record header in the ftx for any
         * root-done procedure redo that gets logged.
         * Level is already zero from above because
         * this is the root ftx.
         */

        ftxp->lrh.rootDRBcnt = 0;
        ftxp->lrh.opRedoRBcnt = 0;

        do {
            ftxRDHdrT* rdrec = (ftxRDHdrT*)&ftxp->rootDoneRecs[rdoff];
            int contAgent = ftxp->contDesc.agentId;
            opxT* opxp;

            if ( (rdrec->agentId > FTX_MAX_AGENTS) ||
                !(opxp = FtxAgents[rdrec->agentId].rootDnOpX) ) {
                ADVFS_SAD0("ftx_done_urdr: no root done opx agent");
            }

            ftxp->lrh.atomicRPass = rdrec->atomicRPass;
            ftxp->lrh.fdl_agentId = rdrec->agentId;
            ftxp->lrh.contOrUndoRBcnt = 0;

            /* execute the root-done procedure */

            opxp( ftxH, rdrec->bCnt, ((char*)rdrec +
                                      sizeof(ftxRDHdrT)) );

            /*
             * Reset log record descriptor for record header.
             */

            lrvecp->count = 1;
            lrvecp->dataLcnt = (sizeof(ftxDoneLRT)+3)/4;

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
                ftxp->contDesc.agentId == 0 ) {
                ftxp->lrh.member = 0;
            }

            /*
             * Log the root done redo record.
             */

            /* log_donerec_nunpin() now returns statusT instead of void.
             * This if statement is a place holder for the handling of that
             * return value.
             */
            if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

            release_ftx_locks( &ftxp->cLvl[0] );

            rdoff += ((rdrec->bCnt + 3)/4) + (sizeof(ftxRDHdrT)/4);

        } while ( rdoff < ftxp->nextRDoff );
    }

    if ( ftxp->contDesc.agentId ) {
        ftxp->lrh.member = 0;
        do_ftx_continuations( dmnP, ftxp, ftxH, lrvecp );
    }

    spin_lock(&dmnP->ftxTblLock);

    /* Mark this ftx as available. It won't get used until 
     * we drop the domainT.ftxTblLock.
     */
    ftx_release_slot(ftxSlot, ftxTDp);

    /*
     * Call routine to reset oldest lsn AFTER the slot is marked
     * as AVAILABLE and noTrimCnt is set to zero.
     * This routine releases the ftxTblLock before returning.
     */
    reset_oldest_lsn( ftxp, dmnP, FALSE );
}

/*
 * ftx_quit
 *
 * Used by caller if nothing (no pages) were modified by the
 * transaction.  It is a noop transaction.
 */

void
ftx_quit(
        ftxHT ftxH        /* in - root ftx handle */
        )
{
    unsigned int ftxSlot;
    ftxStateT* ftxp;
    statusT sts;
    domainT* dmnP = ftxH.dmnP;
    ftxTblDT* ftxTDp;
    unsigned int lvl;


    if ( dmnP == NULL){
        ADVFS_SAD1("ftx_quit: bad domain pointer", (long)dmnP);
    }

    ftxTDp = &dmnP->ftxTbld;

    /* Range check handle. */
    if ((ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD1("ftx_quit: bad ftx handle slot ",ftxSlot);
    }

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    /* check if level in handle matches current level. */

    if ((lvl = ftxp->currLvl) != ftxH.level) {
        ADVFS_SAD2("ftx_quit: handle level N1 doesn't match ftx lvl N2",
                 ftxH.level, lvl);
    }

    if (lvl != 0) {
        ADVFS_SAD1("ftx_quit: not root ftx", lvl);
    }

    if (!LSN_EQ_NIL( ftxp->lastLogRec.lsn )) {
        ADVFS_SAD0("ftx_quit: lastLogRec not nil");
    }

#ifdef ADVFS_SMP_ASSERT
    ADVRWL_DOMAIN_FTXSLOT_UNLOCK(&dmnP->ftxSlotLock);
#endif

    ftxp->undoBackLink = logEndOfRecords;

    release_ftx_locks( &ftxp->cLvl[0] );

    /* free the ftx */
    spin_lock(&dmnP->ftxTblLock);
    ftx_release_slot(ftxSlot, ftxTDp);

    /*
     * Even though the oldest lsn hasn't changed, this will trim the
     * log if this was the last ftx in use and a trim request is active.
     * This routine releases the ftxTblLock before returning.
     */
    reset_oldest_lsn( ftxp, dmnP, FALSE );
    return;
}

/*
 *
 * ftx_fail
 *
 * Fail the current ftx, undoing any ft actions taken during this ftx.
 *
 * Any operational action records specified by ftx_start are discarded
 * when ftx_fail is called, and will never be passed to the
 * operational recovery routine.
 */

void
ftx_fail(
         ftxHT ftxH     /* in - leaf (current) ftx handle */
         )
{
    ftx_fail_2( ftxH, 0 );
}

/*
 * ftx_fail_2 - this version of fail takes the recovery pass as an
 * additional argument for use by recovery.
 */

void
ftx_fail_2(
           ftxHT ftxH,  /* in - leaf (current) ftx handle */
           unsigned int atomicRPass /* recovery pass */
           )
{
    int lgrreclcnt = 0;
    uint32_t* lgrrecp = 0;
    unsigned int ftxSlot;
    ftxStateT* ftxp;
    statusT sts = EOK;
    statusT lrsts = EOK;
    domainT* dmnP = NULL;
    ftxTblDT* ftxTDp;
    int lvl,pli;
    logRdHT logrh;
    perlvlT* clvlp;
    lrDescT *lrvecp = NULL;
    ftxDoneLRT* ftxDLRp;
    int segwordcnt=0, recwordcnt=0, cntRead=0;

    MS_SMP_ASSERT(ftxH.dmnP != NULL);
    dmnP = ftxH.dmnP;

    /* set ftx table pointer and slot */

    ftxTDp = &dmnP->ftxTbld;

    /* check slot */
    ftxSlot = ftxH.hndl - 1;
    MS_SMP_ASSERT(ftxSlot < ftxTDp->rrSlots);

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    /* check if level in handle matches current level. */
    MS_SMP_ASSERT(ftxp->currLvl == ftxH.level);
    lvl = ftxp->currLvl;

#ifdef ADVFS_SMP_ASSERT
    if (!lvl)
        ADVRWL_DOMAIN_FTXSLOT_UNLOCK(&dmnP->ftxSlotLock);
#endif

    clvlp = &ftxp->cLvl[lvl];

    /* unpin pinned pages - make sure they're clean */

    sts = EOK;

    for ( pli = clvlp->lastPinS; pli >= 0; pli -= 1 ) {
        lvlPinTblT* plpp = &clvlp->lvlPinTbl[ pli ];
        ftxPinTblT* fpp;

        if ( plpp->ftxPinS < 0 ) {
            continue;      /* negative means this slot not in use */
        } else {
            fpp = &ftxp->pinTbl[ plpp->ftxPinS ];
        }

        if ( plpp->numXtnts == 0 ) {
            if ( fpp->ftxLvl != lvl ) {
                continue;    /* don't unpin at this level */
            }

            if (bs_unpinpg( fpp->pgH, logNilRecord,
                            fpp->unpinMode ) != EOK) {
                domain_panic(dmnP, "ftx_fail_2: CLEAN unpin page failed!" );
                release_ftx_locks( clvlp );   /* release any locks held by this ftx */
                sts = E_DOMAIN_PANIC;
            }
            fpp->ftxLvl = -1;  /* mark as not in use */
        } else {
            domain_panic(dmnP,
                         "ftx_fail_2: dirty page not allowed; numXtnts == %d",
                         plpp->numXtnts);
            (void)bs_unpinpg( fpp->pgH, logNilRecord, fpp->unpinMode );
            release_ftx_locks( clvlp );   /* release any locks held by this ftx */
            sts = E_DOMAIN_PANIC;
        }
    }
    
    if (sts != EOK) goto free_ftx;

    clvlp->donemode = FTXDONE_NORMAL;

    /* open a log read stream */

    sts = lgr_read_open( dmnP->ftxLogP, &logrh );
    if (sts != EOK) {
        goto free_ftx;   /* only happens when log allocate fails */
    }

    /*
     * The big loop here is to read a log record, execute the undo,
     * then read the next record (following the backlinks), until the
     * end of record backlink (first done record written) is hit.
     */

    /* set the initial record to read */

    ftxp->undoBackLink = ftxp->lastLogRec;

    lrvecp = &ftxp->lrdesc;

    while ( !LSN_EQ_NIL(ftxp->undoBackLink.lsn) ) {

        lrsts = lgr_read(LR_BWD_LINK,
                       logrh,
                       &ftxp->undoBackLink,
                       (uint32_t**)&ftxDLRp,
                       &segwordcnt,
                       &recwordcnt);

        cntRead = segwordcnt;

        if ( ( lrsts != EOK ) && ( lrsts < MSFS_FIRST_ERR ) ) {
              domain_panic(dmnP, "ftx_fail_2 (1): lgr_read failure" );
              ms_free(ftxDLRp);
              release_ftx_locks( clvlp );   /* release any locks held by this ftx */
              (void)lgr_read_close(logrh);  /* close the log read stream */
              goto free_ftx;
        } else if ( ( lrsts == I_LOG_REC_SEGMENT ) ) {
            uint32_t* segptr;

            /* segmented record - get the whole thing into a buffer (lgrrecp)*/

            if ( recwordcnt > lgrreclcnt ) {
                if ( lgrreclcnt ) {
                    ms_free(lgrrecp);
                }
                lgrreclcnt = recwordcnt;
                lgrrecp = (uint32_t*)ms_malloc( lgrreclcnt*4 );
            }
            flmemcpy( (char*)ftxDLRp, (char*)lgrrecp, segwordcnt*4 );

            /* Give back the buffer handed to us by lgr_read */

            ms_free(ftxDLRp);

            ftxDLRp = (ftxDoneLRT*)lgrrecp;

            do {
                lrsts = lgr_read( LR_BWD_LINK,
                                  logrh,
                                  &ftxp->undoBackLink,
                                  &segptr,
                                  &segwordcnt,
                                  &recwordcnt );
                if ( ( lrsts != EOK ) && ( lrsts < MSFS_FIRST_ERR ) ) {
                    domain_panic(dmnP, "ftx_fail_2 (2): lgr_read error" );
                    /* Give back the buffer handed to us by lgr_read */
                    ms_free(segptr);
                    release_ftx_locks( clvlp ); /* release any locks held by this ftx */
                    (void)lgr_read_close(logrh);  /* close the log read stream */
                    ms_free(ftxDLRp);
                    goto free_ftx;
                }

                flmemcpy( (char*)segptr, (char*)&lgrrecp[cntRead],
                         segwordcnt*4 );
                cntRead += segwordcnt;

                /* Give back the buffer handed to us by a successful lgr_read */

                ms_free(segptr);
                
            } while ( lrsts == I_LOG_REC_SEGMENT );

            /*
             * lgrrecp will be freed near the end of the loop, reset
             * lgrreclcnt here so that if there are another group of
             * segment records, we will reallocate new copy buffer 
             * lgrrecp.
             */
            lgrreclcnt = 0;
        }

        if ( cntRead < recwordcnt ) {
            /* did not get entire record */
            if ( lrsts != W_LAST_LOG_REC ) {
                domain_panic(dmnP, "ftx_fail_2: bad log sts (%d)", lrsts);
                release_ftx_locks( clvlp ); /* release any locks held by this ftx */
                (void)lgr_read_close(logrh);      /* close the log read stream */
                ms_free(ftxDLRp);
                goto free_ftx;
            } else {
                /* Give back the buffer handed to us by lgr_read */
                ms_free(ftxDLRp);
                break;  /* stop processing log records */
            }
        }

        if ( ftxDLRp->type != ftxDoneLR ) {
            domain_panic(dmnP, "ftx_fail_2: unexpected log record type (%d)",
                         ftxDLRp->type);
            release_ftx_locks( clvlp ); /* release any locks held by this ftx */
            (void)lgr_read_close(logrh);      /* close the log read stream */
            ms_free(ftxDLRp);
            goto free_ftx;
        }

        if ( ftxDLRp->level < 0 ) {

            /* Give back the buffer handed to us by lgr_read */
            ms_free(ftxDLRp);

            /*
             * redo records logged by undo (below) get the inverse of
             * their original level, hence will always be skipped by
             * this test.
             */
            continue;  /* skip this record */

        }
        /*
         * Ftx_fail_2 is called in one of two modes.  The first is
         * during normal operation to fail all completed
         * subtransactions of an incomplete ftx.  The second mode is
         * during recovery, which needs to fail all subtransactions
         * associated with a given recovery pass, to ensure atomicity
         * and consistency of recovery pass "n" data structures prior
         * to entering pass "n+1".
         */
         else if ( atomicRPass ) {
             if ( RECOV_PASS_LT(dmnP, atomicRPass, ftxDLRp->atomicRPass) ) {
                 /*
                  * We need to reset lrsts to EOK here so that the
                  * final exit test does not think this record was
                  * processed in the case where this was the last log
                  * record in the backlink chain (W_LAST_LOG_REC).
                  */
                 lrsts = EOK;
                 /* Give back the buffer handed to us by lgr_read */
                 ms_free(ftxDLRp);
                 break; /* terminate undo processing */
             }
         } else if ( ftxDLRp->level <= lvl ) {
            /*
             * only execute undo for deeper nested levels.  See
             * comment just above about reseting lrsts.
             */
             lrsts = EOK;
             /* Give back the buffer handed to us by lgr_read */
             ms_free(ftxDLRp);
             break;     /* terminate the backlink scan */
        }

        /*
         * If an undo record is present, call the undo opx routine.
         */

        if ( ftxDLRp->contOrUndoRBcnt ) {
            opxT* opxp;

            if ( (ftxDLRp->fdl_agentId > FTX_MAX_AGENTS) ||
                !(opxp = FtxAgents[ftxDLRp->fdl_agentId].undoOpX) ) {
                domain_panic(dmnP, "ftx_fail_2: no undo opx agent");
                release_ftx_locks( clvlp ); /* release any locks held by this ftx */
                (void)lgr_read_close(logrh); /* close the log read stream */
                goto free_ftx;
            }

            /*
             * Disallow independent subtransactions.
             */

            ftxp->type = OP_UNDO;

            /*
             * Reset current level in ftx - do not change "lvl"
             * because in normal (not recovery) use that is the level
             * to fail back to.
             */

            ftxp->currLvl = ftxDLRp->level;
            ftxH.level = ftxDLRp->level;
            ftxp->cLvl[ftxDLRp->level].donemode = FTXDONE_NORMAL;

            /* execute the undo opx routine */

            opxp( ftxH, ftxDLRp->contOrUndoRBcnt, ((char*)ftxDLRp +
                                             sizeof(ftxDoneLRT)));

            /*
             * Init ftx log record header in the ftx.  Note that the
             * level has been set to the negative value of the original
             * (positive) level number.  This has the effect of
             * causing those records to be skipped entirely on any
             * subsequent execution of this fail routine due to the
             * level test above.  This may happen if the system
             * crashes during the original execution of ftx_fail.
             * Also, no undo or rootdone procedures are possible for
             * these undo routines.
             */

            ftxp->lrh.type = ftxDoneLR;
            ftxp->lrh.level = - ftxp->currLvl;
            ftxp->lrh.atomicRPass = ftxDLRp->atomicRPass;
            ftxp->lrh.member = ftxDLRp->member;
            /* ftxId and bfDmnId will already be correct in the ftx */
            ftxp->lrh.fdl_agentId = ftxDLRp->fdl_agentId;
            ftxp->lrh.contOrUndoRBcnt = 0;
            ftxp->lrh.rootDRBcnt = 0;
            ftxp->lrh.opRedoRBcnt = 0;


            /* Init log record descriptor for record header. */
            lrvecp->bfrvec[0].bufPtr = (uint32_t *)&ftxp->lrh;
            lrvecp->bfrvec[0].bufWords =(sizeof(ftxDoneLRT)+3)/4;
            lrvecp->dataLcnt = lrvecp->bfrvec[0].bufWords;
            lrvecp->count = 1;

            /* buffer record image redo */

            addone_recredo( ftxp, lrvecp );

            /*
             * The ftxp->undoBackLink field is the same field used to
             * read this record and lgr_read has set it to the next undo
             * log record, which is what log_donerec_nunpin will pass
             * to the logger as the backlink of the new log record.
             * This means this undo will be skipped on any subsequent
             * failures once the logging of this undo occurs.
             */

            /* log_donerec_nunpin() now returns statusT instead of void.
             * This if statement is a place holder for the handling of that
             * return value.
             */
            if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

            /*
             * TODO: need to hold this until after lower level
             * sub ftxs are done.
             */

            release_ftx_locks( &ftxp->cLvl[ ftxp->currLvl ] );
        }

        /* Give back the memory given to us by lgr_read */
        ms_free( ftxDLRp );
    }

    /* release any locks held by this ftx */

    release_ftx_locks( clvlp );

    /* close the log read stream */

    sts = lgr_read_close(logrh);
    if ( sts != EOK ) {
        domain_panic(dmnP, "ftx_fail_2: log stream close failed");
        goto free_ftx;
    }

    /*
     * We need to write a final done record if there is no more undo
     * to do, and any records were written.
     */
    if ( ( lrsts == W_LAST_LOG_REC ) &&
        !LSN_EQ_NIL(ftxp->lastLogRec.lsn) ) {

        /*
         * Write a final (0,0) done record.
         */

        ftxp->lrh.type = ftxDoneLR;
        ftxp->lrh.level = 0;
        ftxp->lrh.atomicRPass = 0;
        ftxp->lrh.member = 0;

        ftxp->lrh.fdl_agentId = 0;
        ftxp->lrh.contOrUndoRBcnt = 0;
        ftxp->lrh.rootDRBcnt = 0;
        ftxp->lrh.opRedoRBcnt = 0;


        /*
         * Init log record descriptor for record header.
         */

        lrvecp->count = 1;
        lrvecp->dataLcnt =lrvecp->bfrvec[0].bufWords =(sizeof(ftxDoneLRT)+3)/4;
        lrvecp->bfrvec[0].bufPtr = (uint32_t *)&ftxp->lrh;

        ftxp->undoBackLink = logEndOfRecords;

        /* log_donerec_nunpin() now returns statusT instead of void.
         * This if statement is a place holder for the handling of that
         * return value.
         */
        if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

    }

free_ftx:
    if ( atomicRPass || !lvl ) {
        /*
         * If this is either part of recovery, or the level is
         * zero, reset the usage of the ftx now that we're at the end of the
         * undo backlink chain.
         */
        spin_lock(&dmnP->ftxTblLock);

        /* Mark this slot as available */
        ftx_release_slot(ftxSlot, ftxTDp);

        /* This routine releases the ftxTblLock before returning. */
        reset_oldest_lsn( ftxp, dmnP, FALSE );
    } else if ( lvl ) {

        /* If the level was non-zero reduce the ftx level */

        ftxp->currLvl = lvl - 1;
        ftxp->type = NORMAL;
    }
}

/*
 * ftx_set_continuation - store a continuation record.  This can only
 * be called from a root-done or continuation ftx.
 *
 * The outstanding agent is used.
 */


void
ftx_set_continuation(
                     ftxHT ftxH,        /* in - ftx handle */
                     int contRecSz,     /* in - cont rec size */
                     void* contRec      /* in - continuation record */
                     )
{
    struct ftx* ftxp;
    domainT* dmnP = ftxH.dmnP;
    ftxTblDT* ftxTDp;
    ftxAgentIdT agentId;
    ftxContRecT* fcrp;

    ftxTDp = &dmnP->ftxTbld;

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxH.hndl - 1];
    fcrp = &ftxp->contDesc;

    /*
     * Same agent as outstanding rootdone/continuation agent.
     */
    agentId = ftxp->lrh.fdl_agentId;

    if ( ftxp->type == ROOTDONE ) {
        if ( !FtxAgents[agentId].contOpX ) {
            ADVFS_SAD0("ftx_set_continuation: no cont agent");
        }

    } else if ( ftxp->type != CONTINUATION ) {
        ADVFS_SAD1("ftx_set_continuation: N1 ftx type wrong",
                   ftxp->type );
        /*
         * Note - this routine will overwrite the arguments to the current
         * continuation call!!  The caller must be done referencing them or
         * have copied them to local variables.
         */
    }
    if ( fcrp->agentId ) {
        ADVFS_SAD0("ftx_set_continuation: already set");
    }

    fcrp->agentId = agentId;

    if ( contRecSz > FTX_MAX_CONT_REC_BSZ ) {
        ADVFS_SAD0("ftx_set_continuation: record size too large");
    }
    fcrp->bCnt = contRecSz;
    bcopy( contRec, fcrp->rec, contRecSz );
}

/*
 * ftx_set_special - set special done mode
 *
 * This routine is called prior to calling ftx_done when the done
 * record must be logged synchronously, or the dirty pages must be
 * written through.  The page writethru mode will also force the log
 * to be written synchronously.
 */


void
ftx_special_done_mode(
                      ftxHT ftxH,       /* in - ftx handle */
                      ftxDoneModeT mode /* in - special done mode */
                      )
{
    struct ftx* ftxp;
    unsigned int ftxSlot;
    domainT* dmnP  = ftxH.dmnP;
    ftxTblDT* ftxTDp;
    unsigned int lvl;
    perlvlT* clvlp;

    /* check domain handle and pick up domain pointer */
    if ( dmnP == NULL){
        ADVFS_SAD0("ftx_special_done_mode: bad domain pointer in ftxH"); 
    }
    ftxTDp = &dmnP->ftxTbld;

    /* check slot, set ftx table pointer */
    if ( (ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD1("ftx_special_done_mode: bad ftxH.hndl", ftxSlot);
    }

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    /* check if level in handle matches current level. */

    if ( (lvl = ftxp->currLvl) != ftxH.level) {
        ADVFS_SAD0("ftx_special_done_mode: bad level");
    }

    clvlp = &ftxp->cLvl[lvl];

    clvlp->donemode = mode;
}


/*
 * This routine is used to determine whether or not additional records can
 * be pinned in the current page.  This routine returns true if fewer than
 * FTX_MAX_PINR have been pinned so far.  If FTX_MAX_PINR records have
 * already been pinned, this routine returns false.
 */
int32_t 
rbf_can_pin_record(
        rbfPgRefHT      page_hdl,
        ftxHT           ftx_handle)
{
    int32_t     page_pin_id;
    uint32_t    cur_lvl;
    struct ftx* ftxp;
    lvlPinTblT* per_lvl_pin_ptr;
    domainT*    dmnP = ftx_handle.dmnP;

    /*
     * See rbf_pin_record for this calculations explanation 
     */
    ftxp = &dmnP->ftxTbld.tablep[ page_hdl.hndl - 1 ];

    page_pin_id = page_hdl.pgHndl - 1;

    cur_lvl = ftxp->currLvl;

    per_lvl_pin_ptr = &ftxp->cLvl[ cur_lvl ].lvlPinTbl[ page_pin_id ];

    MS_SMP_ASSERT( per_lvl_pin_ptr->ftxPinS >= 0 );


    /*
     * The assert in rbf_pin_record blows up if numXtnts is >= to
     * FTX_MX_PINR, so we want to check for less than one less than
     * FTX_MX_PINR
     */

    if ( per_lvl_pin_ptr->numXtnts < FTX_MX_PINR-1 ) {
        return TRUE;
    } else {
        return FALSE;
    }
}




/*
 * rbf_pinpg - recoverable bitfile page pin
 *
 * Note that rbf_pinpg returns a different type of page reference
 * handle than bs_pinpg.
 */


statusT
rbf_pinpg(
          rbfPgRefHT *pinPgH,     /* out */
          void **bfPageAddr,      /* out */
          struct bfAccess *bfap,  /* in */
          bs_meta_page_t bsPage,  /* in */
          bfPageRefHintT refHint, /* in - future page ref hint */
          ftxHT ftxH,             /* in */
          meta_flags_t mflags     /* Flags for pinning */
          )
{
    struct ftx* ftxp;
    unsigned int ftxSlot;
    domainT* dmnP = ftxH.dmnP;
    ftxTblDT* ftxTDp;
    statusT sts;
    int ftxPinS,plpinS;
    void* pgAddr;
    rbfPgRefHT retppH;
    ftxPinTblT* fpp;
    lvlPinTblT* plpp;
    perlvlT* clvlp;

    /* check domain handle and pick up domain pointer */

    if ( dmnP == NULL){
        return EBAD_DOMAIN_POINTER;
    }
    ftxTDp = &dmnP->ftxTbld;

    /* check slot, set ftx table pointer */
    if ( (ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots ) {
        return EBAD_FTXH;
    }

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    /* check if level in handle matches current level. */

    if ( ftxp->currLvl != ftxH.level ) {
        return EBAD_FTXH;
    }

    clvlp = &ftxp->cLvl[ftxp->currLvl];

    /*
     * Scan the global ftx pinned page table for the requested page.
     */

    for ( ftxPinS = ftxp->lastFtxPinS; ftxPinS >= 0; ftxPinS -= 1 ) {
        fpp = &ftxp->pinTbl[ftxPinS];

        if ( fpp->ftxLvl >= 0 &&     /* in use */
             fpp->pinAccessp == bfap &&  /* same bitfile */
             fpp->pgdesc.page == bsPage ) {  /* same page */

            /* got the page in the ftx, get/check per-level handle */

            for ( plpinS = clvlp->lastPinS; plpinS >= 0; plpinS -= 1 ) {
                lvlPinTblT* plpp = &clvlp->lvlPinTbl[ plpinS ];

                if ( plpp->ftxPinS == ftxPinS ) {
                    break;
                }
            }

            if ( plpinS < 0 ) {
                plpinS = ++clvlp->lastPinS;
                if ( plpinS == FTX_MX_PINP ) {
                    clvlp->lastPinS = FTX_MX_PINP - 1;
                    for ( plpinS = FTX_MX_PINP - 1; plpinS >= 0;
                         plpinS -= 1 ) {

                        if ( clvlp->lvlPinTbl[plpinS].ftxPinS < 0 ) {
                            break;
                        }
                    }
                    if ( plpinS < 0 ) {
			return E_MAX_PINS_EXCEEDED;
                    }
                }
                plpp = &clvlp->lvlPinTbl[ plpinS ];
                plpp->ftxPinS = ftxPinS;
                plpp->numXtnts = 0;
            }

            *bfPageAddr = fpp->pgAddr;
            retppH.hndl = ftxH.hndl;
            retppH.dmnP = ftxH.dmnP;
            retppH.pgHndl = plpinS + 1;

            *pinPgH = retppH;
            return EOK;
        }
    }

    if ( (ftxPinS = ++ftxp->lastFtxPinS) == FTX_TOTAL_PINS ) {
        ftxp->lastFtxPinS = FTX_TOTAL_PINS - 1;
        for ( ftxPinS = FTX_TOTAL_PINS - 1; ftxPinS >= 0; ftxPinS-- ) {
            fpp = &ftxp->pinTbl[ftxPinS];
            if ( fpp->ftxLvl < 0 ) {  /* negative if not in use */
                break;
            }
        }
        if ( ftxPinS < 0 ) {
            ADVFS_SAD0("rbf_pinpg: exceeded max total pinned pages");
        }
    } else {
        fpp = &ftxp->pinTbl[ftxPinS];
    }

    plpinS = ++clvlp->lastPinS;
    if ( plpinS == FTX_MX_PINP ) {
        clvlp->lastPinS = FTX_MX_PINP - 1;
        for ( plpinS = FTX_MX_PINP - 1; plpinS >= 0;
             plpinS -= 1 ) {

            if ( clvlp->lvlPinTbl[plpinS].ftxPinS < 0 ) {
                break;
            }
        }
        if ( plpinS < 0 ) {
            ADVFS_SAD0("rbf_pinpg (2):exceeded max pins");
        }
    }

    plpp = &clvlp->lvlPinTbl[ plpinS ];
    plpp->ftxPinS = ftxPinS;
    plpp->numXtnts = 0;

    sts = bs_pinpg(&fpp->pgH,
                   &pgAddr,
                   bfap,
                   bsPage,
                   ftxH,
                   mflags);
    if (sts != EOK) {
        plpp->ftxPinS = -1; /* mark as not in use */
        fpp->ftxLvl = -1;   /* mark as not in use */
        return sts;
    }

    fpp->pgdesc.bfsTag = bfap->bfSetp->bfSetId.dirTag;
    fpp->pgdesc.tag = bfap->tag;
    fpp->pgdesc.page = bsPage;
    fpp->ftxLvl = ftxp->currLvl;
    fpp->pgAddr = pgAddr;
    fpp->pinAccessp = bfap;
    fpp->pgSz = bfap->bfPageSz;
    fpp->unpinMode = BS_CLEAN;
    *bfPageAddr = pgAddr;

    retppH.hndl = ftxH.hndl;
    retppH.dmnP = ftxH.dmnP;
    retppH.pgHndl = plpinS + 1;

    *pinPgH = retppH;
    return EOK;
}

/*
 * rbf_deref_page - unpin/deref an unmodified pinned/ref'd page
 */


void
rbf_deref_page(
               rbfPgRefHT rbfPgRefH,            /* in */
               bfPageCacheHintT cacheHint       /* in */
               )
{
    struct ftx* ftxp;
    unsigned int ftxSlot;
    int ftxPinS,plpinS;
    ftxPinTblT* fpp;
    lvlPinTblT* plpp;
    domainT* dmnP = rbfPgRefH.dmnP;
    ftxTblDT* ftxTDp;

    /* check domain handle and pick up domain pointer */

    if (dmnP == NULL){
        ADVFS_SAD0("rbf_deref_page: bad domain pointer");
    }
    ftxTDp = &dmnP->ftxTbld;

    /* check slot, set ftx table pointer */
    if ((ftxSlot = rbfPgRefH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD0("rbf_deref_page: ftx handle out of range");
    }

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    if ((plpinS = rbfPgRefH.pgHndl - 1) >= FTX_MX_PINP ) {
        ADVFS_SAD0("rbf_deref_page: bad pin page handle page slot");
    }

    plpp = &ftxp->cLvl[ftxp->currLvl].lvlPinTbl[plpinS];

    if ( plpp->ftxPinS < 0 ) {
        ADVFS_SAD0("rbf_deref_page: bad ftxPinS"); 
    }

    fpp = &ftxp->pinTbl[plpp->ftxPinS];

    if ( fpp->ftxLvl < 0 ) {
        ADVFS_SAD0("rbf_deref_page: bad ftxLvl");
    }

    /*
     * If the page has been modified, simply store the cache hint
     */

    if ( fpp->unpinMode.rlsMode != BS_NOMOD ) {
        fpp->unpinMode.cacheHint = cacheHint;
        return;

        /* if this is a deeper level, return */

    } else if ( fpp->ftxLvl != ftxp->currLvl ) {
        return;
    }

    /* unpin the page "clean" */

    if ( bs_unpinpg(fpp->pgH, logNilRecord, fpp->unpinMode ) != EOK ) {
        ADVFS_SAD0("rbf_deref_page:bs_unpinpg CLEAN failed");
    }

    plpp->ftxPinS = -1;
    fpp->ftxLvl = -1;  /* negative is "not in use" */

}

/*
 * rbf_pin_record
 *
 * Record image undo/redo.
 *
 * Record image based recovery saves before/after record images when
 * changes are made to data in a pinned page.
 *
 * The current implementation does not support capturing before
 * images.  This is a considerable overhead savings when it is not
 * necessary, and so far has not proven necessary.
 *
 * All users of this facility should continue to follow the convention
 * of calling rbf_pin_record BEFORE any modifications are made.
 *
 * The after image for each pinned record is logged when ftx_done is
 * called.
 *
 * If no changes are made to a pinned page, that is, rbf_pin_record
 * was not called, ftx_done will obviously log no changes and release
 * the page as unmodified.
 *
 * There is some overhead to this routine, so it is probably best to
 * not call it separately for each individual field in a structure.
 * However, it will collapse overlapping or adjacent record extents
 * into a single extent to avoid the per extent overhead in the
 * logging procedure.
 *
 * Only FTX_MX_PINR unique extents per page are allowed.
 *
 * This routine will abort if the specified record extent is not
 * contained within the page.
 */


void
rbf_pin_record(
               rbfPgRefHT pinPgH,       /* in - pinned page handle */
               void* recAddr,           /* in - address of record */
               int recSz                /* in - byte count of record */
               )
{
    struct ftx* ftxp;
    unsigned int ftxSlot;
    unsigned int lvl;
    int ftxPinS,plpinS;
    ftxPinTblT* fpp;
    lvlPinTblT* plpp;
    int recxi,recboff,recendoff;
    domainT* dmnP = pinPgH.dmnP;
    ftxTblDT* ftxTDp;
    int zapi = 0;

    /* check domain handle and pick up domain pointer */

    if (dmnP == NULL){
        ADVFS_SAD0("rbf_pin_record: bad domain pointer");
    }
    ftxTDp = &dmnP->ftxTbld;

    /* check slot, set ftx table pointer */
    if ((ftxSlot = pinPgH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD0("rbf_pin_record: ftx handle out of range");
    }

    /* Save pointer to slot's ftx structure. */
    ftxp = &ftxTDp->tablep[ftxSlot];

    if ((plpinS = pinPgH.pgHndl - 1) >= FTX_MX_PINP) {
        ADVFS_SAD0("rbf_pin_record: bad pin page handle page slot");
    }

    lvl = ftxp->currLvl;

    plpp = &ftxp->cLvl[lvl].lvlPinTbl[plpinS];

    if ( plpp->ftxPinS < 0 ) {
        ADVFS_SAD0("rbf_pin_record: bad ftxPinS");
    }

    fpp = &ftxp->pinTbl[plpp->ftxPinS];

    if ( fpp->ftxLvl < 0 ) {
        ADVFS_SAD0("rbf_pin_record: bad ftxLvl");
    }

    /*
     * Set the unpin mode to "dirty".
     */

    if ( fpp->unpinMode.rlsMode == BS_NOMOD ) {
        fpp->unpinMode.rlsMode = BS_MOD_LAZY;
    }

    /*
     * Calculate page relative beginning and ending offsets and make
     * range checks to ensure they're within the page.
     */

    if ( (recboff = (char*)recAddr - (char*)fpp->pgAddr) < 0) {
        ADVFS_SAD2("rbf_pin_record: negative offset: pg addr N1, rec addr N2",
                 (uint64_t) fpp->pgAddr, (uint64_t) recAddr);
    } else {
        recendoff = recboff + recSz;
        if ( recendoff >  fpp->pgSz * ADVFS_FOB_SZ ) {
            ADVFS_SAD1("rbf_pin_record: end offset beyond page", recendoff);
        }
    }

    /*
     * Scan to collapse overlaps.  The specified record is "grown" to
     * include any overlaps with existing record extents.  The
     * existing record extent is zapped, as the new extent will
     * include it.  If the new extent is completely contained within
     * an existing extent, we're done.
     */

    for ( recxi = 0; recxi < plpp->numXtnts; recxi += 1) {
        int boff = plpp->recX[recxi].pgBoff;
        int endoff = boff + plpp->recX[recxi].bcnt;

        if ( (recboff >= boff) && (recboff <= endoff) ) {
            /* start is within existing record */
            if ( recendoff <= endoff ) {
                /* it's completely contained, all done */
                return;
            }
            /* start is inside existing, end is beyond */
            recboff = boff;
            plpp->recX[recxi].bcnt = 0;  /* will replace this one */
            zapi = 1;
        }

        if ( (recendoff <= endoff) && (recendoff >= boff) ) {
            /* end within existing, already know it's not contained */
            recendoff = endoff;
            plpp->recX[recxi].bcnt = 0;  /* to be zapped */
            zapi = 1;
        }
    }

    /*
     * If any existing record extents are now part of the new extent,
     * scan the record extent list and make the list dense again so
     * there are no unused holes in the list.
     */

    if ( zapi ) {
        for ( recxi = 0; recxi < plpp->numXtnts; recxi += 1) {
            if ( !plpp->recX[recxi].bcnt ) {
                plpp->recX[recxi] = plpp->recX[--plpp->numXtnts];
            }
        }
    }

    if ((recxi = plpp->numXtnts++) >= FTX_MX_PINR) {
        ADVFS_SAD1("rbf_pin_record: exceeded maximum pins per page",
                   FTX_MX_PINR);
    }

    /*
     * Record the new extent, which has possibly "consumed" all or
     * part of a pre-existing extent.
     */
    plpp->recX[recxi].pgBoff = recboff;
    plpp->recX[recxi].bcnt = recendoff - recboff;
    return;
}

/***********************************
 * various "done" utility routines *
 **********************************/

/*
 * addone_undo - add undo record to log vector
 *
 * recptr must be int aligned.
 */


static
void
addone_undo(
            void* recptr,      /* in - ptr to undo record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            )
{
    int i = lrdp->count++;
    int words;

    if ( !recptr ) {
        ADVFS_SAD0("addone_undo: no undo record");
    } else if ( !FtxAgents[ftxp->lrh.fdl_agentId].undoOpX ) {
        ADVFS_SAD0("addone_undo: no undo agent");
    }

    lrdp->bfrvec[i].bufPtr = recptr;
    words = (ftxp->lrh.contOrUndoRBcnt + 3)/4;
    lrdp->bfrvec[i].bufWords = words;
    lrdp->dataLcnt += words;
}

/*
 * addone_redo - add redo record to log vector
 *
 * recptr must be int aligned.
 */


static
void
addone_redo(
            void* recptr,      /* in - ptr to redo record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            )
{
    int i = lrdp->count++;
    int words;

    if ( !recptr ) {
        ADVFS_SAD0("addone_redo: no redo record");
    } else if ( !FtxAgents[ftxp->lrh.fdl_agentId].redoOpX ) {
        ADVFS_SAD0("addone_redo: no redo agent");
    }

    lrdp->bfrvec[i].bufPtr = recptr;
    words = (ftxp->lrh.opRedoRBcnt + 3)/4;
    lrdp->bfrvec[i].bufWords = words;
    lrdp->dataLcnt += words;
}

/*
 * addone_rtdn - add rtdn record to log vector
 *
 * recptr must be int aligned.
 */


static
void
addone_rtdn(
            void* recptr,      /* in - ptr to rtdn record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            )
{
    int i = lrdp->count++;
    int words,nextrdoff;
    int32_t * dp;

    if ( !recptr ) {
        ADVFS_SAD0("addone_rtdn: no rootdone proc record");
    } else if ( !FtxAgents[ftxp->lrh.fdl_agentId].rootDnOpX ) {
        ADVFS_SAD0("addone_rtdn: no rootdone agent");
    }

    lrdp->bfrvec[i].bufPtr = recptr;
    words = (ftxp->lrh.rootDRBcnt + 3)/4;
    lrdp->bfrvec[i].bufWords = words;
    lrdp->dataLcnt += words;

    /*
     * buffer the root done record in the ftx state struct also
     */

    if ( (nextrdoff = ftxp->nextRDoff + words + (sizeof(ftxRDHdrT)/4))
        > FTX_RD_BFRSZ ) {
        ADVFS_SAD0("addone_rtdn: out of buffer room for root done");
    }

    dp = &ftxp->rootDoneRecs[ftxp->nextRDoff];
    ((ftxRDHdrT*)dp)->atomicRPass = ftxp->lrh.atomicRPass;
    ((ftxRDHdrT*)dp)->agentId = ftxp->lrh.fdl_agentId;
    ((ftxRDHdrT*)dp)->bCnt = ftxp->lrh.rootDRBcnt;
    dp += sizeof(ftxRDHdrT)/4;
    for (i = 0; i < words; i++) {
        dp[i] = ((uint32_t *)recptr)[i];
    }
    ftxp->rootDnCnt += 1;
    ftxp->nextRDoff = nextrdoff;
}

/*
 * addone_cont - add cont record to log vector.  This uses the
 * undo part of the record when the ftx level is zero.
 *
 * recptr must be int aligned.
 */


void
addone_cont(
            void* recptr,      /* in - ptr to redo record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            )
{
    int i = lrdp->count++;
    int words;

    lrdp->bfrvec[i].bufPtr = recptr;
    words = (ftxp->lrh.contOrUndoRBcnt + 3)/4;
    lrdp->bfrvec[i].bufWords = words;
    lrdp->dataLcnt += words;
}


/*
 * addone_recredo - add record image redo elements to log record
 * vector.
 */


void
addone_recredo(
               ftxStateT* ftxp, /* in/out - ftx state */
               lrDescT* lrdp    /* in/out - log rec desc */
               )
{
    perlvlT* clvlp = &ftxp->cLvl[ftxp->currLvl];
    int pli,reci,veci;

    /*
     * for each pinned page, add a vector element for the page descriptor,
     * another for its extents, and a vector element for the data
     * in each extent.
     */

    for ( pli = 0; pli <= clvlp->lastPinS; pli++ ) {
        lvlPinTblT* plpp = &clvlp->lvlPinTbl[ pli ];
        ftxPinTblT* fpp;

        if ( !plpp->numXtnts || plpp->ftxPinS < 0 ) {
            continue;  /* nothing to log or not in use */
        } else {
            fpp = &ftxp->pinTbl[plpp->ftxPinS];
            fpp->pgdesc.numXtnts = plpp->numXtnts;
        }

        if ( (veci = lrdp->count++) >= sizeof(lrdp->bfrvec)/4 ) {
            ADVFS_SAD0("addone_recredo: too many log vector elements(1)");
        }

        lrdp->bfrvec[veci].bufPtr = (uint32_t *)&fpp->pgdesc;
        lrdp->dataLcnt += (lrdp->bfrvec[veci].bufWords = sizeof(ftxRecRedoT)/4);

        if ( (veci = lrdp->count++) >= sizeof(lrdp->bfrvec)/4 ) {
            ADVFS_SAD0("addone_recredo: too many log vector elements(2)");
        }

        lrdp->bfrvec[veci].bufPtr = (uint32_t *)plpp->recX;
        lrdp->dataLcnt += (lrdp->bfrvec[veci].bufWords =
                         plpp->numXtnts * sizeof(ftxRecXT) / 4);

        for ( reci = 0; reci < plpp->numXtnts; reci++ ) {
            ftxRecXT* mrecxp = &plpp->recX[ reci ];

            if ( (veci = lrdp->count++) >= sizeof(lrdp->bfrvec)/4 ) {
                ADVFS_SAD0("addone_recredo: too many log vector elements(3)");
            }

            lrdp->bfrvec[veci].bufPtr = (uint32_t *)fpp->pgAddr +
                                                 (mrecxp->pgBoff >> 2);
            lrdp->dataLcnt += (lrdp->bfrvec[veci].bufWords =
                           ((mrecxp->pgBoff & 3) + mrecxp->bcnt + 3) >> 2);
        }
    }
}

/*
 * log_donerec - call the logger with the ftx done record, and unpin
 * dirty pages associated with it.
 */


statusT
log_donerec_nunpin(
                   ftxStateT* ftxp,     /* in/out - ftx state */
                   domainT* dmnP,       /* in - domain state */
                   lrDescT* lrdp        /* in/out - done record desc */
                   )
{
    perlvlT* clvlp = &ftxp->cLvl[ftxp->currLvl];
    ftxDoneModeT donemode;
    logWriteModeT lwmode;
    statusT sts = EOK;
    int pli,reci;

    donemode = clvlp->donemode;

    /* Log the "Done" record. */

    if ( donemode == FTXDONE_NORMAL ) {
        lwmode = FtxDoneMode;
    } else if ( donemode == FTXDONE_LOGSYNC ){
        lwmode = LW_SYNC;
    } else {
        ftxp->undoBackLink = clvlp->skipSubsLink;
        lwmode = LW_ASYNC;
    }

    /*
     * Process the log for domain states >= BFD_RECOVER_FTX. Also, if we
     * are in domain deactivate path so that the log can be trimmed.
     */

    if ( (dmnP->state >= BFD_RECOVER_FTX) || 
         (dmnP->dmnFlag & (BFD_DEACTIVATE_IN_PROGRESS | BFD_DEACTIVATE_PENDING)))
    {
        sts = lgr_writev_ftx(
                             ftxp,
                             dmnP,
                             lrdp,
                             lwmode
                             );

        if( (sts != EOK) && (sts != E_DOMAIN_PANIC) ) {
            domain_panic(dmnP,  "log_donerec_nunpin: lgr_write failed, return code = %d", sts);
            sts =  E_DOMAIN_PANIC;
            goto _unpin;
        }

    } else if ( dmnP->state == BFD_VIRGIN ) {
        ftxp->undoBackLink = logNilRecord;
    } else {
        domain_panic(dmnP, "log_donerec_nunpin: not VIRGIN state (%d)", dmnP->state );
        sts =  E_DOMAIN_PANIC;
        goto _unpin;
    }

    /* The transaction has been persistently stored or the domain paniced.
     * It is completely recoverable from this point on.
     *
     * If ASYNC logging was used we can't be sure
     * that the transaction is persistent at this point.  The
     * only thing we can guarantee is that the log will be written
     * before the real bitfile pages are written, that is, the write
     * ahead log protocol is always enforced.
     */

    /* unpin dirty/clean pages */

_unpin:

    for ( pli = clvlp->lastPinS; pli >= 0; pli -= 1 ) {
        lvlPinTblT* plpp = &clvlp->lvlPinTbl[ pli ];
        ftxPinTblT* fpp;

        if ( plpp->ftxPinS < 0 ) {
            continue;
        } else {
            fpp = &ftxp->pinTbl[plpp->ftxPinS];
            plpp->ftxPinS = -1;
            if ( fpp->ftxLvl != ftxp->currLvl ) {
                continue;
            } else {
                fpp->ftxLvl = -1;
            }
        }

        if ( fpp->unpinMode.rlsMode == BS_NOMOD ) {
            if (bs_unpinpg( fpp->pgH, logNilRecord,
                           fpp->unpinMode ) != EOK) {
                domain_panic(dmnP, "log_donerec_nunpin: CLEAN unpin page failed!");
                sts =  E_DOMAIN_PANIC;
            }
        } else {
            /*
             * A Nil log record is passed in here, as the unpin lsn
             * was set by lgr_writev_ftx above.
             */
            if ( (dmnP->state >= BFD_RECOVER_FTX) ) {
                if (bs_unpinpg( fpp->pgH,
                            logNilRecord,
                            fpp->unpinMode ) != EOK) {
                    domain_panic(dmnP, "log_donerec_nunpin: DIRTY unpin page failed!");
                    sts = E_DOMAIN_PANIC;
                }
            } else {
                /* 
                 * This is a case when we are not able to log, will will do
                 * a synchronous log flush.  This could be because we are
                 * still initializing the log (getting storage for the log
                 * for example
                 */
                if (bs_unpinpg( fpp->pgH,
                                logNilRecord,
                                BS_WRITETHRU) != EOK) {
                    domain_panic(dmnP, "log_donerec_nunpin: DIRTY unpin page failed!");
                    sts = E_DOMAIN_PANIC;
                }
            }
        }
    }

    return sts;
}
/*
 * do_ftx_continuations - execute the ftx continuation chain.
 *
 * This presumes that there is an initial continuation, or this
 * routine would not have been called.
 */


void
do_ftx_continuations(
                     domainT* dmnP,     /* in/out - ptr to domain */
                     ftxStateT* ftxp,   /* in/out - ftx state */
                     ftxHT ftxH,        /* in - ftx handle */
                     lrDescT* lrvecp    /* in/out - done record desc */
                     )
{
    ftxTblDT* ftxTDp = &dmnP->ftxTbld;
    opxT* opxp;
    int wait = 0;
    statusT sts=0;

    if ( !(opxp = FtxAgents[ftxp->contDesc.agentId].contOpX) ) {
        ADVFS_SAD0("do_ftx_continuations: no cont opx");
    }
    ftxp->type = CONTINUATION;
    /* ftxp->lrh.level = 0;  already */
    ftxp->lrh.atomicRPass = FTX_MAX_RECOVERY_PASS;
    ftxp->lrh.fdl_agentId = ftxp->contDesc.agentId;
    ftxp->lrh.rootDRBcnt = 0;
    ftxp->lrh.opRedoRBcnt = 0;

    do {
        /*
         * Okay, let's call the continuation agent.  Clear the agentId
         * first, as this will be set again if this continuation sets
         * another continuation.
         */

        ftxp->contDesc.agentId = 0;
        opxp( ftxH, ftxp->contDesc.bCnt, ftxp->contDesc.rec );

        /*
         * Reset log record descriptor for record header.  The zeroth
         * element is assumed to already be pointing to the ftx record header.
         */

        lrvecp->count = 1;
        lrvecp->dataLcnt = (sizeof(ftxDoneLRT)+3)/4;

        if ( ftxp->contDesc.agentId ) {

            /*
             * Another continuation record was buffered by the
             * continuation executed above.
             */
            ftxp->lrh.contOrUndoRBcnt = ftxp->contDesc.bCnt;
            addone_cont( (void*)ftxp->contDesc.rec, ftxp, lrvecp );
        } else {
            ftxp->lrh.contOrUndoRBcnt = 0;
        }

        /* buffer record image redo */

        addone_recredo( ftxp, lrvecp );

        ftxp->undoBackLink = logEndOfRecords;

        /*
         * Continuation record member is how many have been done
         * as a negative value.  Except for the last one, which is zero,
         * so we can tell we're done during recovery.
         */

        if ( ftxp->contDesc.agentId == 0 ) {
            ftxp->lrh.member = 0;
        } else {
            --ftxp->lrh.member;
        }

        /*
         * Log the root done redo record.
         */

        /* log_donerec_nunpin() now returns statusT instead of void.
         * This if statement is a place holder for the handling of that
         * return value.
         */
        if ((sts = log_donerec_nunpin( ftxp, dmnP, lrvecp )) != EOK) ;

        release_ftx_locks( &ftxp->cLvl[0] );

        /*
         * Before executing any more continuations, test whether a
         * checkpoint is being requested.  The main purpose of
         * continuations is to allow the oldest ftx log address to be
         * advanced, as they do not require the entire ftx tree to be
         * retained in the log, thereby allowing it to be trimmed.
         *
         * We do at least one continuation, because that will advance
         * the oldest lsn for this continuation chain to a current
         * value in the log.
         */
        if ( !LSN_EQ_NIL( ftxTDp->logTrimLsn ) ) {
            spin_lock(&dmnP->ftxTblLock);
            --ftxTDp->noTrimCnt;

            /* This routine releases the ftxTblLock before returning so 
             * relock it after the call.
             */
            reset_oldest_lsn( ftxp, dmnP, TRUE );
            spin_lock(&dmnP->ftxTblLock);

            /*
             * We must wait until the log is trimmed.  The actual
             * log flushing is done by reset_oldest_lsn when no
             * transactions are active.
             */

            wait = 0;

            while ( !LSN_EQ_NIL( ftxTDp->logTrimLsn ) &&
                    ((dmnP->state == BFD_ACTIVATED) || 
                     (dmnP->dmnFlag & BFD_DEACTIVATE_IN_PROGRESS)) ) {
                if (AdvfsLockStats) {
                    if (wait) {
                        AdvfsLockStats->ftxTrimReWait++;
                    } else {
                        wait = 1;
                    }

                    AdvfsLockStats->ftxTrimWait++;
                }

                ftxTDp->trimWaiters++;
                cv_wait(&ftxTDp->trimCv, &dmnP->ftxTblLock, 
                        CV_SPIN, CV_DFLT_FLG);
                ftxTDp->trimWaiters--;
            }

            ++ftxTDp->noTrimCnt;
            spin_unlock(&dmnP->ftxTblLock);

        }
        if ( dmnP->state == BFD_RECOVER_CONTINUATIONS ) {
            /*
             * If this is being called during recovery, simply
             * return, as recovery is currently single threaded
             * and this will force rotation through all
             * outstanding continuations, allowing the log to be
             * trimmed.
             */
            return;
        }
    } while ( ftxp->contDesc.agentId );
}

/******************************************************************
**********************  ftx locking routines **********************
******************************************************************/


perlvlT *
get_perlvl_p(
             ftxHT ftxH
             )
{
    unsigned int ftxSlot;
    unsigned int lvl;
    domainT *dmnP = ftxH.dmnP;
    ftxTblDT *ftxTDp;

    if (dmnP == NULL){
        return NULL;
    }

    ftxTDp = &dmnP->ftxTbld;

    if ((ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots) {
        return NULL;
    }

    if ((lvl = ftxTDp->tablep[ ftxSlot ].currLvl) != ftxH.level) {
        return NULL;
    }

    return &ftxTDp->tablep[ ftxSlot ].cLvl[lvl];
}

/* ======================================================================= */


/*
 * ftx_rwlock_init
 *
 * Initializes a ftx lock and adds it to its mutex's linked list of locks.
 *
 */

void
ftx_rwlock_init(
    ftxLkT *lk,            /* in - pointer to the lock */
    spin_t *spin,         /* in - pointer to the lock's spin lock */
    char   *name,          /* in - pointer to the lock name */
    int     order          /* in - lock hierarchy order */
)

{
    lkHdrT *lkHdr = (lkHdrT *) lk;
    lkHdr->lkType = LKT_FTXRW;
    lkHdr->spin   = spin;
    ADVFTX_FTXRW_INIT( &(lk->lock.rw), name, order );

} /* end ftx_rwlock_init */

void
ftx_mutex_init(
    ftxLkT *lk,            /* in - pointer to the lock */
    spin_t *spin,          /* in - pointer to the lock's mutex */
    char   *name,          /* in - pointer to the lock name */
    int     order          /* in - lock hierarchy order */
)

{
    lkHdrT *lkHdr = (lkHdrT *) lk;
    lkHdr->lkType = LKT_FTXMTX;
    lkHdr->spin   = spin;
    ADVFTM_FTXMTX_INIT( &(lk->lock.mtx), name, order );

} /* end ftx_rwlock_init */


void
ftx_mutex_lock(
    ftxLkT *lk,      /* in */
    ftxHT ftxH,      /* in */
    int ln,          /* in */
    char *fn         /* in */
    )
{
    perlvlT *clvlp;

    if ((clvlp = get_perlvl_p( ftxH )) == NULL) {
       /*
         * get_perlvl_p could be NULL for various reasons, such as there
         * being no domain, or the slot value is too large.  If a domain
         * does not exist, you can't issue a domain_panic.  An ASSERT does
         * not seem appropriate either.  The original panic is being left in.
         */
        ADVFS_SAD0( "ftx_mutex_write: bad ftx handle" );
    }

    ADVFTM_FTXMTX_LOCK( &(lk->lock.mtx) );

    /* 
     *  Mutex locking unnecessary because of single threaded ftx structure
     */
     
    lk->hdr.nxtFtxLk = clvlp->lkList;
    clvlp->lkList = lk;
}

void
ftx_rwlock_write(
    ftxLkT *lk,      /* in */
    ftxHT ftxH,      /* in */
    int ln,          /* in */
    char *fn         /* in */
    )
{
    perlvlT *clvlp;

    if ((clvlp = get_perlvl_p( ftxH )) == NULL) {
       /*
         * get_perlvl_p could be NULL for various reasons, such as there
         * being no domain, or the slot value is too large.  If a domain
         * does not exist, you can't issue a domain_panic.  An ASSERT does
         * not seem appropriate either.  The original panic is being left in.
         */
        ADVFS_SAD0( "ftx_rwlock_write: bad ftx handle" );
    }

    ADVFTX_FTXRW_WRITE( &(lk->lock.rw) );

    /* 
     *  Mutex locking unnecessary because of single threaded ftx structure
     */
     
    lk->hdr.nxtFtxLk = clvlp->lkList;
    clvlp->lkList = lk;
}

void
ftx_rwlock_read(
    ftxLkT *lk,      /* in */
    ftxHT ftxH,      /* in */
    int ln,          /* in */
    char *fn         /* in */
    )
{
    perlvlT *clvlp;

    if ((clvlp = get_perlvl_p( ftxH )) == NULL) {
        ADVFS_SAD0( "ftx_rwlock_read: bad ftx handle" );
    }

    ADVFTX_FTXRW_READ( &(lk->lock.rw) );
    
    /* 
     *  Mutex locking unnecessary because of single threaded ftx structure
     */
     
    lk->hdr.nxtFtxLk = clvlp->lkList;  
    clvlp->lkList = lk;
}



static void
ftx_unlock(
    void *lk               /* in */
    )
{
    lkHdrT *lkHdr = (lkHdrT *) lk;
    ftxLkT *fLk = (ftxLkT *) lk;
    stateLkT *sLk = (stateLkT *) lk;

    if ((lkHdr == NULL) || (lkHdr->spin == NULL)) {
        ADVFS_SAD0( "ftx_unlock: lkHdr or lkHdr->mutex is NULL" );
    }

    switch( lkHdr->lkType ) {

        case LKT_STATE:
            spin_lock( lkHdr->spin );
            lk_signal( lk_set_state( sLk, sLk->pendingState ), sLk );
            sLk->pendingState = LKW_NONE;
            spin_unlock( lkHdr->spin );
            break;

        case LKT_FTXRW:
            ADVFS_RWLOCK_UNLOCK( &(fLk->lock.rw) );
            break;

        case LKT_FTXMTX:
            ADVFS_MUTEX_UNLOCK( &(fLk->lock.mtx) );
            break;

        default:
            ADVFS_SAD1("ftx_unlock: unknown lock type", lkHdr->lkType);
    }
} /* end ftx_unlock */



void
_ftx_add_lock(
              ftxLkT *lk,      /* in */
              ftxHT ftxH,      /* in */
              int ln,          /* in */
              char *fn         /* in */
              )
{
    perlvlT *clvlp;

    if ((clvlp = get_perlvl_p( ftxH )) == NULL) {
        ADVFS_SAD0( "_ftx_add_lock: bad ftx handle" );
    }
    
    /* 
     *  Spinlock locking unnecessary because of single threaded ftx structure
     */
     
    lk->hdr.nxtFtxLk = clvlp->lkList;
    clvlp->lkList = lk;
}

/*********************************************************************
*********************************************************************/


void
_ftx_set_state(
    stateLkT *lk,
    spin_t *lk_mutex,
    lkStatesT newState,
    ftxHT ftxH,
    int ln,
    char *fn
    )
{
    perlvlT *clvlp;

    if ((clvlp = get_perlvl_p( ftxH )) == NULL) {
        ADVFS_SAD0( "_ftx_set_state: bad ftx handle" );
    }

    if (lk->pendingState != LKW_NONE) {
        ADVFS_SAD0( "_ftx_set_state: pending state already set" );
    }

    lk->pendingState = newState;

    lk->hdr.nxtFtxLk = clvlp->lkList;
    clvlp->lkList = lk;
}


void
release_ftx_locks(
                  perlvlT *clvlp
                  )
{
    lkHdrT *lkp;

    while ((lkp = clvlp->lkList) != NULL) {
        clvlp->lkList = lkp->nxtFtxLk;
        lkp->nxtFtxLk = NULL;

        ftx_unlock( lkp );
    }
}

/************************************
 * various other utility routines
 ***********************************/

/* ftx_init_table - initializes a domain's transaction table descriptor.
 * Allocate and initialize the transaction table and a variable number of
 * transactions slots based upon the log size and a typical large
 * transaction record size.
 * 
 * Assumptions:
 * dmn_alloc() was previously called and zeroed the domain's table descriptor
 * structure when it ms_malloc'd the domain structure.
 * dmn_dealloc() will free memory allocated here.
 * The domain's ftxLogPgs field is initialized already by the
 * the domain activate routines who call this routine.
 */

/* The large log record byte value size represents a typical large log
 * record size that the AdvFS generates and is not an absolute maximum size.
 * The purpose of this size is to calculate the number of ftx slots for
 * a particular domain log size.
 * The number of concurrent transactions that can start are dependent
 * upon the number of ftx slots.
 */
static uint64_t ftx_large_logrec =
                 (FTX_MX_PINP * ADVFS_METADATA_PGSZ) +
                 (FTX_MX_PINR * (sizeof(logRecHdrT) + sizeof(logPgTrlrT)));
void
ftx_init_table(domainT *dmnP) 
{
    uint32_t slot;

    /* This routine must only be called once per initial domain activation. */
    MS_SMP_ASSERT(dmnP->ftxTbld.tablep == NULL ||
                  dmnP->ftxTbld.tableSltStatep == NULL);

    /* Allocate and initialize the ftx table and associated variables.
     *
     * Calculate the number of ftx slots based upon the log quartile size 
     * divided by the large transaction record size.
     */
    dmnP->ftxTbld.rrSlots = 
      (dmnP->ftxLogPgs * ADVFS_METADATA_PGSZ) / (4 * ftx_large_logrec);
    dmnP->ftxTbld.rrNextSlot  = 0;
    (void)cv_init( &dmnP->ftxTbld.slotCv, "ftx slot CV", NULL, CV_WAITOK );
    (void)cv_init( &dmnP->ftxTbld.trimCv, "ftx trim CV", NULL, CV_WAITOK );
    (void)cv_init( &dmnP->ftxTbld.excCv,  "ftx exc CV",  NULL, CV_WAITOK );

    dmnP->ftxTbld.tablep =
        (struct ftx *)ms_malloc(sizeof(struct ftx) * dmnP->ftxTbld.rrSlots);

    dmnP->ftxTbld.tableSltStatep = 
      (ftxSlotStateT*)ms_malloc(sizeof(ftxSlotStateT) * dmnP->ftxTbld.rrSlots);
         
    /* Initialize the state of the ftx slots. */
    for (slot = 0; slot < dmnP->ftxTbld.rrSlots; slot++)
	dmnP->ftxTbld.tableSltStatep[slot] = FTX_SLOT_AVAIL;
}

/*
 * get_ftx_id - returns ftx id given an ftx handle
 */


ftxIdT
get_ftx_id(ftxHT ftxH       /* ftx handle */
           )
{
    unsigned int ftxSlot;
    domainT* dmnP = ftxH.dmnP;
    ftxTblDT* ftxTDp;

    if (dmnP == NULL){
        ADVFS_SAD0("get_ftx_id: bad domain pointer in ftxH");
    }
    ftxTDp = &dmnP->ftxTbld;

    /* check slot, set ftx table pointer */
    if ((ftxSlot = ftxH.hndl - 1) >= ftxTDp->rrSlots) {
        ADVFS_SAD0("get_ftx_id: ftx handle out of range");
    }

    return ftxTDp->tablep[ftxSlot].lrh.ftxId;
}

/*
 * ftx_release_slot
 *
 * Release an ftx slot in the ftx table by making it available again.
 * After calling this function, the calling thread often calls
 * reset_oldest_lsn(), which references the fields in the ftx structure.
 * The slot's ftx structure memory is freed in dmn_dealloc() during
 * domain deactivation.
 * Assumes the domainT.ftxTblLock is locked except in recovery.  The calling
 * routines are responsible for locking this lock when necessary.
 */

void
ftx_release_slot(
    int ftxSlot,
    ftxTblDT *ftxTDp
    )
{
    /* mark ftx slot AVAIL; The fields in that struct may be used in
     * reset_oldest_lsn() after this routine returns.
     */
    ftxTDp->tableSltStatep[ftxSlot] = FTX_SLOT_AVAIL;
    --ftxTDp->slotUseCnt;
    --ftxTDp->noTrimCnt;

    if ( FtxStats ) {
        if ( ftxTDp->oldestSlot == ftxSlot ) {
            int slot = ftxSlot;

            /* The oldest slot is being freed. Find the new oldest slot. */
            while ( slot != ftxTDp->rrNextSlot ) {
                if ( ftxTDp->tableSltStatep[slot] == FTX_SLOT_BUSY ||
                     ftxTDp->tableSltStatep[slot] == FTX_SLOT_EXC ) {
                    break;
                }
                slot = (slot + 1) % ftxTDp->rrSlots;
            }
            ftxTDp->oldestSlot = slot;
        }
    }

    /*
     * Wake up any waiters.  They could be waiting for an
     * ftx slot to become AVAIL, or it could be an exclusive ftx
     * waiting for all other ftx's to finish.  However, only wake
     * them up if we are at the rrNextSlot.
     */
    if ((ftxTDp->rrNextSlot == ftxSlot) &&
        (ftxTDp->ftxWaiters > 0) ) {
        cv_signal( &ftxTDp->slotCv, NULL, CV_NULL_LOCK );
        if (AdvfsLockStats)
            AdvfsLockStats->ftxSlotSignalFree++;
    }

    /* Now wake up anyone waiting for an exclusive transaction */
    if ( ftxTDp->excWaiters > 0 ) {
        cv_broadcast(&ftxTDp->excCv, NULL, CV_NULL_LOCK);
        if (AdvfsLockStats)
            AdvfsLockStats->ftxExcSignal++;
    }
}


/*************************************************
 ***** "crash restart" log address maintenance ***
 ************************************************/

/*
 * The crash restart log address is constrained by two different log
 * record addresses.  One is the oldest log address associated with
 * an outstanding ftx tree.  The other is the original log record
 * address associated with the oldest dirty buffer in the buffer
 * cache.
 *
 * These are kept in separate structures because the oldest dirty
 * buffer address is updated at i/o completion and access is
 * serialized by the ioqueuemutex, which must mask interrupts.  The
 * oldest outstanding ftx log address is serialized by the ftx mutex
 * and the log descriptor.
 *
 * The data structures used for both of these are designed to avoid
 * the need to acquire the mutex when in fact the value has not been
 * modified since last read.
 *
 * The crash restart log address for a particular ftx log record is
 * determined by the lgr_writev_ftx routine while holding the log
 * descriptor lock, so that it is correctly serialized with the
 * buffering of the log record.
 *
 */

/*
 * ftx_init_recovery_logaddr - initialize crash restart log address
 * structures for domain.
 */


void
ftx_init_recovery_logaddr(
                          domainT* dmnP
                          )
{
    /* Initialize the crash recovery log address structures.
     * See comments with ftx_set_recovery_logaddr to explain the
     * following.
     */

    (void)ftx_set_dirtybufla( dmnP, logNilRecord );
    (void)ftx_get_dirtybufla( dmnP );
    (void)ftx_set_dirtybufla( dmnP, logNilRecord );

    (void)ftx_set_oldestftxla( dmnP, logNilRecord );
    (void)ftx_get_oldestftxla( dmnP );
    (void)ftx_set_oldestftxla( dmnP, logNilRecord );
}

/*
 * ftx_set_dirtybufla
 *
 * This routine sets the oldest dirty buffer log record address.
 * This value is originally set when the first entry is placed onto the
 * domain's lsnList, which contains an ordered list of the dirty buffers
 * outstanding for this domain.  When an IO completes for the first buffer
 * on the lsnList, then it is removed from the chain, and this routine is
 * called to set the value to the log rec address of the next buffer.  If
 * the chain is now empty, then the value in dirtyBufLa is set to a NIL
 * value.
 *
 * Use the "minimal sync" data structure.
 *
 * SMP: 1. dmnP->lsnLock must be held by the caller except during
 *         recovery, when there is only one thread active.
 *
 * Note that in order to initialize dirtyBufLa correctly, the following
 * sequence must be executed:
 *
 *    ftx_set_dirtybufla
 *    get_dirtybufla - gets a null result, throw it away
 *    ftx_set_dirtybufla
 *
 */

void
ftx_set_dirtybufla(
                   domainT* dmnP,
                   logRecAddrT dirtyBufLa
                   )
{
    /* The dmnP->lsnLock must be held by the caller. No ASSERT
     * since this can also be called during recovery when this
     * will not be necessary.
     */
    int updindex = dmnP->dirtyBufLa.update;

    if ( dmnP->dirtyBufLa.read == updindex ) {
        dmnP->dirtyBufLa.update = ++updindex;
    }

    dmnP->dirtyBufLa.lgra[(updindex & 1)] = dirtyBufLa;
}


/*
 * ftx_get_dirtybufla - gets the oldest dirty buffer log address
 *
 * SMP: Locks dmnP->lsnLock while modifying dmnP->dirtyBufLa.
 */


logRecAddrT
ftx_get_dirtybufla(
                   domainT* dmnP
                   )
{
    int thisread = dmnP->dirtyBufLa.read;
    logRecAddrT dirtyBufLa;

    /* Optimistically pick up the log address */

    dirtyBufLa = dmnP->dirtyBufLa.lgra[(thisread & 1)];

    if ( thisread == dmnP->dirtyBufLa.update ) {

        /* We win! It was valid, so simply return it */

        return dirtyBufLa;
    } else {

        /* It's been updated, get the lock, re-read and reset */

        spin_lock( &dmnP->lsnLock );

        thisread = dmnP->dirtyBufLa.update;
        dmnP->dirtyBufLa.read = thisread;
        dirtyBufLa = dmnP->dirtyBufLa.lgra[(thisread & 1)];

        spin_unlock( &dmnP->lsnLock );
    }

    return dirtyBufLa;
}

/*
 * ftx_set_oldestftxla
 *
 * This routine sets the log record address for the oldest active transaction
 * in this domain.  This is originally set (in ftx_set_firstla) when the first
 * transaction in a domain is started.  It is subsequently updated when the
 * oldest transaction completes, and then dmnP->ftxTbld->oldestFtxLa will
 * be set to the new value for the oldest outstanding transaction (in
 * reset_oldest_lsn).  If there are no other transactions, it is set to a
 * NIL value.
 *
 * Use the "minimal sync" data structure.
 *
 * Note that in order to initialize oldftxLa correctly, the following
 * sequence must be executed:
 *
 *    ftx_set_oldestftxla
 *    get_oldestftxla - gets a null result, throw it away
 *    ftx_set_oldestftxla
 *
 */


void
ftx_set_oldestftxla(
                    domainT* dmnP,
                    logRecAddrT oldftxLa
                    )
{
    int updindex;
    ftxCRLAT* oldftxlap = &dmnP->ftxTbld.oldestFtxLa;

    spin_lock(&dmnP->ftxTblLock);

    updindex = oldftxlap->update;

    if ( oldftxlap->read == updindex ) {
        oldftxlap->update = ++updindex;
    }

    oldftxlap->lgra[(updindex & 1)] = oldftxLa;

    spin_unlock(&dmnP->ftxTblLock);
}

/*
 * ftx_get_oldestftxla - gets the log record address for the oldest
 *                       outstanding transaction in this domain.
 */


logRecAddrT
ftx_get_oldestftxla(
                    domainT* dmnP
                    )
{
    int thisread;
    logRecAddrT oldftxLa;
    ftxCRLAT* oldftxlap = &dmnP->ftxTbld.oldestFtxLa;

    thisread = oldftxlap->read;

    /* Optimistically pick up the log address */

    oldftxLa = oldftxlap->lgra[(thisread & 1)];

    if ( thisread == oldftxlap->update ) {

        /* We win! It was valid, so simply return it */

        return oldftxLa;
    }

    /* It's been updated, get the lock, re-read and reset */
    spin_lock(&dmnP->ftxTblLock);

    thisread = oldftxlap->update;
    oldftxlap->read = thisread;
    oldftxLa = oldftxlap->lgra[(thisread & 1)];

    spin_unlock(&dmnP->ftxTblLock);

    return oldftxLa;
}

/*
 * ftx_set_firstla - set the first log record address for a particular
 * ftx, as well as the overall oldest ftx log record address, if
 * appropriate.
 *
 * Return the oldest ftx address, as we've got to acquire the 
 * domainT.ftxTblLock to set the first log address for this ftx anyway.
 */


logRecAddrT
ftx_set_firstla(
                ftxStateT* ftxp,   /* in/out - ptr to ftx state */
                domainT* dmnP,     /* in/out - ptr to domain */
                logRecAddrT fla    /* in - first log addr */
                )
{
    int updindex;
    logRecAddrT oldftxLa;
    ftxCRLAT* oldftxlap = &dmnP->ftxTbld.oldestFtxLa;

    spin_lock(&dmnP->ftxTblLock);

    updindex = oldftxlap->update;

    ftxp->firstLogRecAddr = fla;

    if ( LSN_EQ_NIL( oldftxlap->lgra[(updindex & 1)].lsn )) {
        /*
         * This will now be the oldest ftx log record address, so set
         * it.
         */
        if ( oldftxlap->read == updindex ) {
            oldftxlap->update = ++updindex;
        }
        oldftxlap->lgra[(updindex & 1)] = fla;
        oldftxLa = fla;
    } else {
        if ( oldftxlap->read != updindex ) {
            oldftxlap->read = updindex;
        }
        oldftxLa = oldftxlap->lgra[(updindex & 1)];
    }

    spin_unlock(&dmnP->ftxTblLock);

    return oldftxLa;
}

/*
 * reset_oldest_lsn - check to see if this was the oldest lsn and force
 * update if so.
 *
 * Lock Assumptions:
 * The caller must hold the domainT.ftxTblLock upon entry.
 * But this routine will release the lock upon exit.
 */


void
reset_oldest_lsn(
                 ftxStateT* ftxp,       /* in - ptr to ftx state */
                 domainT* dmnP,         /* in/out - ptr to domain */
                 int continuation       /* in - TRUE if continuation */
                )
{
    int updindex;
    lsnT thisLsn = ftxp->firstLogRecAddr.lsn;
    ftxTblDT* ftxTDp = &dmnP->ftxTbld;
    ftxCRLAT* oldftxlap = &ftxTDp->oldestFtxLa;
    int32_t wakeup_exc_waiters;
    int32_t wakeup_trim_waiters;

    MS_SMP_ASSERT(owns_spinlock(&dmnP->ftxTblLock));
    if ( (dmnP->state < BFD_RECOVER_CONTINUATIONS) && 
         (!(dmnP->dmnFlag & BFD_DEACTIVATE_IN_PROGRESS)) ) {
        spin_unlock(&dmnP->ftxTblLock);
        return;
    }

    updindex = oldftxlap->update;

    if ( continuation ) {
        ftxp->firstLogRecAddr = ftxp->lastLogRec;
    }

    if ( !LSN_EQL(oldftxlap->lgra[updindex & 1].lsn, thisLsn) ) {
        goto checklogtrim;
    }

    /*
     * This ftx had the oldest lsn, so scan the remaining ftx structs
     * for the currently oldest lsn, unless there are no longer
     * any transactions outstanding, in which case the oldest ftx
     * is set to nil.
     */

    if ( oldftxlap->read == updindex ) {
        oldftxlap->update = ++updindex;
    }

    if ( ftxTDp->slotUseCnt == 0 ) {
        if ( continuation ) {
            oldftxlap->lgra[(updindex & 1)].lsn = ftxp->lastLogRec.lsn;
        } else {
            oldftxlap->lgra[(updindex & 1)].lsn = nilLSN;
        }
    } else {
        /*
         * Other slots are in use (FTX_SLOT_BUSY or FTX_SLOT_EXC),
         * so scan them for the oldest. Only need to scan until
	 * all in use slots have been examined.
         */
        lsnT oldlsn;
        logRecAddrT oldftxrec;
        ftxStateT* oldftxp;
        ftxStateT* sftxp;
        int32_t slotsUsed;
        int i;

        oldlsn = nilLSN;
        slotsUsed = ftxTDp->slotUseCnt;

        for ( i = 0; slotsUsed > 0 ; i++ ) {
            if ( ftxTDp->tableSltStatep[i] != FTX_SLOT_AVAIL) {
                sftxp = &ftxTDp->tablep[i];
                slotsUsed--;

                if ( !LSN_EQ_NIL( sftxp->firstLogRecAddr.lsn ) ) {
                    if ( LSN_EQ_NIL( oldlsn ) ||
                        LSN_LT( sftxp->firstLogRecAddr.lsn,
                               oldlsn ) ) {
                        oldlsn = sftxp->firstLogRecAddr.lsn;
                        oldftxp = sftxp;
                    }
                }
            }
        }
        if ( LSN_EQ_NIL( oldlsn )) {
            oldftxlap->lgra[(updindex & 1)] = logNilRecord;
        } else {
            oldftxlap->lgra[(updindex & 1)] = oldftxp->firstLogRecAddr;
        }
    }

checklogtrim:
    if ( !LSN_EQ_NIL(ftxTDp->logTrimLsn) &&
                    (ftxTDp->noTrimCnt == 0) ) {
        /*
         * A log trim checkpoint is active, and there are no
         * outstanding ftxs (this was the last one).
         * Synchronously flush all dirty metadata.  This will
         * also cause the log to be flushed.  Wait for completion so that
         * no more than a quarter of the log is unbacked before we
         * continue to write in the next quarter.
         */
        if (AdvfsLockStats)
            dmnP->logStat.logTrims++;
        spin_unlock(&dmnP->ftxTblLock);

        advfs_bs_dmn_flush_meta(dmnP, FLUSH_NOFLAGS);

        spin_lock(&dmnP->ftxTblLock);
        ftxTDp->logTrimLsn = nilLSN;
        wakeup_exc_waiters = (ftxTDp->excWaiters > 0) ? TRUE : FALSE;
        wakeup_trim_waiters = (ftxTDp->trimWaiters > 0) ? TRUE : FALSE;
        spin_unlock(&dmnP->ftxTblLock);

        if (wakeup_exc_waiters || wakeup_trim_waiters) {        
            /* Wake up any thread waiting for an exclusive transaction. */
            if (wakeup_exc_waiters) { 
                cv_signal( &ftxTDp->excCv, NULL, CV_NULL_LOCK );
                if (AdvfsLockStats)
                    AdvfsLockStats->ftxExcSignal++;
            }

            /*
             * Wake up any threads waiting for the log to be trimmed.
             * Hopefully there are a bunch so we can wake them all up
             * at once.
             */
            if (wakeup_trim_waiters) {
                cv_broadcast( &ftxTDp->trimCv, NULL, CV_NULL_LOCK );
                if (AdvfsLockStats)
                    AdvfsLockStats->ftxTrimBroadcast++;
            }
        }
    }
    else
        spin_unlock(&dmnP->ftxTblLock);
}
/* end ftx_routines.c */
