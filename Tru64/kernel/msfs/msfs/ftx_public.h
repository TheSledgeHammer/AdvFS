/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
 * @(#)$RCSfile: ftx_public.h,v $ $Revision: 1.1.48.1 $ (DEC) $Date: 2003/04/30 20:47:35 $
 */

#ifndef FTX_PUBLIC
#define FTX_PUBLIC

#include <msfs/ftx_agents.h>

typedef uint32T ftxIdT;

struct domain;

/* 
 * An xid has its high bit set to differentiate it from
 * other ftxId's.
 */

#define CFS_XID(x) (x & 0x80000000)
#define CFS_XID_NOT_FOUND -1

#define NUM_XIDS 2046		/* This makes xidInfoT an 8K page */

#define XID_TIMEOUT 15*60	/* Release xid memory after 15 minutes */

typedef struct xidInfo {
    struct xidInfo *next;
    ftxIdT data[NUM_XIDS];	 
} xidInfoT;			

typedef struct xidRecovery {
    xidInfoT *head;              /* head of xidInfoT chain */
    xidInfoT *tail;              /* last in xidInfoT chain */
    int current_free_slot;       /* Index of next free xidInfoT.data */	
    struct timeval timestamp;    /* Time of recovery */
} xidRecoveryT;			

/*
 * cfs_check_for_pfs_commit() is called by CFS to 
 * check for xid status.
 */

#ifdef _KERNEL
int cfs_check_for_pfs_commit(fsid_t fsid, ftxIdT xid);
#endif

extern ftxHT FtxNilFtxH; /* Nil ftx for use as root parent */

#define FTX_EQ(ftxA, ftxB) ((ftxA).dmnP == (ftxB).dmnP && \
                            (ftxA).hndl == (ftxB).hndl && \
                            (ftxA).level == (ftxB).level)

/* crash redo log address structure */

typedef struct ftxCRLA {
    int read;           /* read "index" */
    int update;         /* update "index" */
    logRecAddrT lgra[2]; /* two log address records */
} ftxCRLAT;

/*
 * agent operational redo/undo/rootDn execution
 *
 * The appropriate operation execution procedure is called to process all
 * operation redo, undo actions, and deferred root ftx done actions.
 *
 * The agent op execution routine is responsible for dispatching to the
 * relevant action based on the operation type code.  How these codes
 * are interpreted is left to the agent's discretion.
 *
 * The opX routine is only allowed to use the ftbs_pinpg function.  An
 * ftx handle for ftbs_pinpg is supplied as a parameter to the opX routine.
 *
 * The bitfile and domain handles used by the ftx are not valid in
 * the recovery routine.  A valid domain handle is explicitly passed
 * to the recovery routine.  Bitfile tags, though, are invariant and
 * may be used to access a bitfile within the recovery routine.
 *
 */

typedef void opxT (
                   ftxHT ftxH,      /* in - ftx handle */
                   int opRecSz,     /* in - size of opx record */
                   void* opRec      /* in - ptr to opx record */
                   );

/*
 * ftx_register_agent
 *
 * In order to use the ftx services, you must first register as an
 * ftx agent.  This is expected to be done as part of (bs/fs/...)init
 * with the returned handle stored in a global variable accessible to
 * any code using the ftx services.  For example, the bitfile agent
 * could store its agent handle in the global BsFtxAgentH variable.
 * By doing this, facility specific ftx service macros can be written
 * which generate calls to the ftx services and automatically include
 * the agent handle.
 *
 * If your agent uses operational redo or undo, or defers root
 * done/fail actions, it must specify the relevant operational action
 * routines.
 *
 * The agent identifier is intended to encode both the functional
 * agent (bitfile create, delete, ...) *and* the protocol version of
 * that agent.  Therefore a separate identifier is used for different
 * versions of the same agent.
 *
 */
statusT
ftx_register_agent_n2(ftxAgentIdT agentId, /* in - agent id */
                      opxT *undoOpX,       /* in - undo opx proc ptr */
                      opxT *rootDnOpX,     /* in - root done opx proc ptr */
                      opxT *redoOpX,       /* in - redo opx proc ptr */
                      opxT *contOpX        /* in - continuation opx */
                                /* Note - opX routines must NOT fail !! */
                   );

statusT
ftx_register_agent_n(ftxAgentIdT agentId, /* in - agent id */
                   opxT *undoOpX,       /* in - undo opx proc ptr */
                   opxT *rootDnOpX,     /* in - root done opx proc ptr */
                   opxT *redoOpX        /* in - redo opx proc ptr */
                                /* Note - opX routines must NOT fail !! */
                   );


statusT
ftx_register_agent(ftxAgentIdT agentId, /* in - agent id */
                   opxT *undoOpX,   /* in - undo opx routine ptr */
                   opxT *rootDnOpX  /* in - root done opx routine ptr */
                                /* Note - opX routines must fail !! */
                   );


/*
 * Ftx demarcation:
 *  ftx_start
 *  ftx_done
 *  ftx_fail
 *
 * Ft actions are nested.  Each ft action, aka ftx, may specify a parent.
 * If there is no parent, a new top level ft action, known as a
 * root ft action, is created.
 *
 * Each new ftx is a leaf ftx.  The entire family of
 * ftxs with the same root ancestor must execute in a single
 * serial thread.  Therefore a new leaf always creates a new
 * level of ftx, that is, only one ftx exists at each
 * level from root to current leaf at any given time.
 *
 */

/*
 * ftx_start
 *
 * Ftx_start must be called prior to calling any recoverable
 * services.  It creates a new ftx, optionally a child of another ftx,
 * and returns a handle to the newly created ftx.
 *
 * Saving the record before image is unnecessary if a given ftx cannot
 * possibly fail after any page is modified and the ftx specifies
 * operational redo information.  This is currently
 * the only mode supported.
 *
 * The page_reservation parameter guarantees that the specified
 * number of pages will be available to ref or pin during the course
 * of the ftx.  This is done so that buffer resource deadlocks between
 * different ftxs can be avoided.
 *
 * The atomicRPass parameter specifies on which recovery pass this ftx
 * must be made atomic.  It is used to specify which
 * (sub) trees must be undone at the end of a given pass, so that
 * whatever structures they modify are consistent prior to starting
 * the next recovery pass.  It is only meaningful if there will be an
 * ftx subtree associated with the ftx.  A value of zero (FINAL_PASS) is 
 * taken to mean "final pass", that is, no special action on earlier passes.
 *
 * All new code should use the FTX_START* macros.  
 */
#define FTX_EXC    0x01		/* exclusive ftx */
#define FTX_NOWAIT 0x04		/* don't wait if slot not available */

/* Because we can't #include ftx_privates.h here, but we want to use 
 * the #defined values for the recovery passes, they are duplicated here.
 * If you change the values here, be sure to change them there as well.
 */

#ifndef FTX_MAX_RECOVERY_PASS
#define FINAL_PASS            0
#define META_META_PASS        3
#define META_PASS             1         /* do not change this value */
#define DATA_PASS             2         /* do not change this value */
#define FTX_MAX_RECOVERY_PASS DATA_PASS /* last recovery pass */
#endif /* FTX_MAX_RECOVERY_PASS */


statusT
_ftx_start_i(
    ftxHT *ftxH,                /* out - ftx handle */
    ftxHT parentFtxH,           /* in - parent ftx handle */
#ifdef FTX_PROFILING
    ftxAgentIdT agentId,        /* in - agent ID; only if profiling */
#endif /* FTX_PROFILING */
    struct domain *dmnP,        /* in - domain pointer */
    int page_reservation,       /* in - ref/pin pages reserved */
    unsigned int atomicRPass,   /* in - atomic recovery pass */
    int flag,                   /* in - 1 == start exclusive ftx, 2 == force */
    long xid                    /* in - transaction id (gen'd by CFS client) */
#ifdef ADVFS_DEBUG
    , int ln,                     /* in - caller's line number */
    char *fn                    /* in - caller's file name */
#endif /* ADVFS_DEBUG */
    );

#if defined(FTX_PROFILING) && defined(ADVFS_DEBUG)

#define FTX_START_N(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, 0, __LINE__, __FILE__)
#define FTX_START_EXC(ag, fh, dh, pr) \
    _ftx_start_i((fh), FtxNilFtxH, (ag), (dh), (pr), 0, FTX_EXC, 0, __LINE__, __FILE__)
#define FTX_START_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), META_PASS, 0, 0, __LINE__, __FILE__)
#define FTX_START_RSVD_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), META_META_PASS, FTX_EXC, 0, __LINE__, __FILE__)
#define FTX_START(ag, fh, pfh, sh, dh, fl, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, 0, __LINE__, __FILE__)
#define FTX_START_NOWAIT(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, FTX_NOWAIT, 0, __LINE__, __FILE__)
#define FTX_START_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, xid, __LINE__, __FILE__)
#define FTX_START_EXC_XID(ag, fh, dh, pr, xid) \
    _ftx_start_i((fh), FtxNilFtxH, (ag), (dh), (pr), 0, FTX_EXC, xid, __LINE__, __FILE__)
#define FTX_START_META_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), META_PASS, 0, xid, __LINE__, __FILE__)
#endif /* FTX_PROFILING && ADVFS_DEBUG */

#if defined(FTX_PROFILING) && !defined(ADVFS_DEBUG)

#define FTX_START_N(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, 0)
#define FTX_START_EXC(ag, fh, dh, pr) \
    _ftx_start_i((fh), FtxNilFtxH, (ag), (dh), (pr), 0, FTX_EXC, 0)
#define FTX_START_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 1, 0, 0)
#define FTX_START_RSVD_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), META_META_PASS, FTX_EXC, 0)
#define FTX_START(ag, fh, pfh, sh, dh, fl, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, 0)
#define FTX_START_NOWAIT(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, FTX_NOWAIT, 0)
#define FTX_START_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 0, 0, xid)
#define FTX_START_EXC_XID(ag, fh, dh, pr, xid) \
    _ftx_start_i((fh), FtxNilFtxH, (ag), (dh), (pr), 0, FTX_EXC, xid)
#define FTX_START_META_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (ag), (dh), (pr), 1, 0, xid)
#endif /* FTX_PROFILING && !ADVFS_DEBUG */

#if !defined(FTX_PROFILING) && !defined(ADVFS_DEBUG)

#define FTX_START_N(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, 0)
#define FTX_START_EXC(ag, fh, dh, pr) \
    _ftx_start_i((fh), FtxNilFtxH, (dh), (pr), 0, FTX_EXC, 0)
#define FTX_START_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_PASS, 0, 0)
#define FTX_START_RSVD_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_META_PASS, FTX_EXC, 0)
#define FTX_START(ag, fh, pfh, sh, dh, fl, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, 0)
#define FTX_START_NOWAIT(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, FTX_NOWAIT, 0)
#define FTX_START_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, xid)
#define FTX_START_EXC_XID(ag, fh, dh, pr, xid) \
    _ftx_start_i((fh), FtxNilFtxH, (dh), (pr), 0, FTX_EXC, xid)
#define FTX_START_META_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_PASS, 0, xid)
#endif /* !FTX_PROFILING && !ADVFS_DEBUG */

#if !defined(FTX_PROFILING) && defined(ADVFS_DEBUG)

#define FTX_START_N(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, 0, __LINE__, __FILE__)
#define FTX_START_EXC(ag, fh, dh, pr) \
    _ftx_start_i((fh), FtxNilFtxH, (dh), (pr), 0, FTX_EXC, 0, __LINE__, __FILE__)
#define FTX_START_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_PASS, 0, 0, __LINE__, __FILE__)
#define FTX_START_RSVD_META(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_META_PASS, FTX_EXC, 0, __LINE__, __FILE__)
#define FTX_START(ag, fh, pfh, sh, dh, fl, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, 0, __LINE__, __FILE__)
#define FTX_START_NOWAIT(ag, fh, pfh, dh, pr) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, FTX_NOWAIT, 0, __LINE__, __FILE__)
#define FTX_START_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (dh), (pr), 0, 0, xid, __LINE__, __FILE__)
#define FTX_START_EXC_XID(ag, fh, dh, pr, xid) \
    _ftx_start_i((fh), FtxNilFtxH, (dh), (pr), 0, FTX_EXC, xid, __LINE__, __FILE__)
#define FTX_START_META_XID(ag, fh, pfh, dh, pr, xid) \
    _ftx_start_i((fh), (pfh), (dh), (pr), META_PASS, 0, xid, __LINE__, __FILE__)
#endif /* !FTX_PROFILING && ADVFS_DEBUG */


#define FTX_NOOP_REDO 0
#define FTX_OP_REDO 1
#define FTX_NO_FAIL 2

void
ftx_quit(
        ftxHT ftxH        /* in - root ftx handle */
        );
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
         );

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

void
ftx_done_n(
         ftxHT ftxH,
         ftxAgentIdT agentId
         );

void
ftx_done_fs(
            ftxHT ftxH,
            ftxAgentIdT agentId,
            long server_only
            );

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
         );

void
ftx_done_u(
           ftxHT ftxH,          /* in - leaf ftx handle */
           ftxAgentIdT agentId, /* in - opx agent id */
           int undoOpSz,        /* in - size of op undo struct */
           void* undoOp         /* in - ptr to op undo struct */
           );

void
ftx_done_urd(
             ftxHT ftxH,        /* in - leaf ftx handle */
             ftxAgentIdT agentId, /* in - opx agent id */
             int undoOpSz,      /* in - size of op undo struct */
             void* undoOp,      /* in - ptr to op undo struct */
             int rootDnOpSz,    /* in - size of root done struct */
             void* rootDnOp     /* in - ptr to root done struct */
             );

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
              );

/*
 * ftx_set_special - set special done mode
 *
 * This routine is called prior to calling ftx_done when the done
 * record must be logged synchronously, or the dirty pages must be
 * written through.  The page writethru mode will also force the log
 * to be written synchronously.
 */

typedef enum {
    FTXDONE_NORMAL,
    FTXDONE_LOGSYNC,
    FTXDONE_SKIP_SUBFTX_UNDO
} ftxDoneModeT;

void
ftx_set_continuation(
                     ftxHT ftxH,        /* in - ftx handle */
                     int contRecSz,     /* in - cont rec size */
                     void* contRec      /* in - continuation record */
                     );

void
ftx_special_done_mode(
                      ftxHT ftxH,       /* in - ftx handle */
                      ftxDoneModeT mode /* in - special done mode */
                      );
/*
 * Recoverable bitfile functions.
 *
 * A variant form of the bs* functions that modify storage take an
 * additional parent ftxH parameter.  Specifically:
 *
 *  bs_access
 *  rbf_create
 *  rbf_delete
 *  rbf_add_stg
 *  rbf_free_stg
 *  rbf_pinpg
 *
 * The page ref and pin functions will be modified to implicitly
 * acquire shared-read and exclusive write consistency locks, respectively,
 * on the specified page.
 *
 * The pin page consistency locks are always released when a leaf
 * ftx completes.  A bs_refpg consistency lock is released when the
 * ref'd page is dereferenced by bs_derefpg.
 *
 * An unmodified pinned page may be unpinned using rbf_unpin_nomod before
 * ftx_done/fail.  This may be necessary to avoid deadlocks.  A
 * modified pinned page, however, can only be dereferenced implicitly
 * by the ftx_done/fail procedure.
 *
 * Consolidated bitfile access routines - no more rbf_access flavors.
 */

struct bfAccess;

/*
 * rbf_create - recoverable bitfile create
 */

statusT
rbf_create(
           bfTagT *tag,                 /* out */
           bfSetT *bfSetp,              /* in */
           bfParamsT *bfParams,         /* in */
           ftxHT parentFtx,             /* in */
           int checkmigtrunclock        /* in */
           );

void ftbs_create(void);  /* obsolete */

/*
 * rbf_delete - recoverable bitfile deletion
 */

statusT
rbf_delete(
           struct bfAccess *bfap,       /* in */
           ftxHT parentFtx              /* in */
           );

/*
 * rbf_add_stg - recoverable bitfile storage allocation
 */

statusT
rbf_add_stg( 
            struct bfAccess *bfap,          /* in */
            unsigned long pageOFfset,       /* in */
            unsigned long pageCnt,          /* in */
            ftxHT parentFtx,                /* in */
            int checkmigtrunclock           /* in */
            );

statusT
rbf_add_overlapping_stg( 
                        struct bfAccess *bfap,          /* in */
                        unsigned long pageOFfset,       /* in */
                        unsigned long pageCnt,          /* in */
                        ftxHT parentFtx,                /* in */
                        uint32T *allocPageCnt           /* out */
                        );

/*
 * rbf_refpg, rbf_pinpg - recoverable, resource assured
 * bitfile page ref/pin
 *
 * Note that these functions a different type of page reference
 * handle than bs_refpg or bs_pinpg.
 */

typedef struct rbfPgRefH {
    struct domain *dmnP;        /* domain pointer */
    signed hndl : 16;           /* handle into ftx table */
    unsigned pgHndl : 8;        /* page ref handle */
} rbfPgRefHT;

statusT
rbf_pinpg( 
          rbfPgRefHT *rbfPgRefH,        /* out */
          void **bfPageAddr,            /* out */
          struct bfAccess *bfap,        /* in */
          unsigned long bsPage,         /* in */
          bfPageRefHintT refHint,       /* in */
          ftxHT ftxH                    /* in */
          );

/*
 * rbf_deref_page - deref an unmodified pinned page or ref'd page
 *
 * If this is called for an already modified pinned page, it is not
 * actually unpinned, but the cacheHint is retained and used when it
 * is eventually released during ftx_done.
 */

void
rbf_deref_page(
               rbfPgRefHT rbfPgRefH,            /* in */
               bfPageCacheHintT cacheHint       /* in */
               );

/*
 * Record image undo/redo.
 *
 * Record image based recovery saves before/after record images when
 * changes are made to data in a pinned page.
 *
 * Record image recovery is used in two circumstances:
 *
 *  1. Record image redo may be used instead of operational redo.
 *     Note that operational undo is always required, however.
 *
 *  2. Record image undo must be used if ftx_fail can be executed
 *     after a pinned page is modified.
 *
 * rbf_pin_record
 *
 * Pin_record is called prior to modifying data in a pinned page.  It
 * will save a before record image in memory at that time, if record
 * image undo is being used for the current ftx.  See ftx_nofail_start
 * below.
 *
 * The after image for each pinned record is logged when ftx_done is
 * called if operational redo was not specified for the ftx.
 *
 * Pin_record must be called when any change is made to data in a
 * pinned page regardless of whether record image recovery is being
 * used at all.  If no records have been pinned for a given page when
 * ftx_done is called, the page is considered unmodified and will not
 * be marked dirty, which has the same effect as calling
 * rbf_unpin_nomod.
 */

void
rbf_pin_record(
               rbfPgRefHT pgRefH,       /* in - pinned page handle */
               void* recAddr,           /* in - address of record */
               int recSz                /* in - byte count of record */
               );

void pin_record(void);  /* obsolete */

/*
 * get_ftx_id
 * This routine returns the ftx id given an ftx handle.
 */

ftxIdT
get_ftx_id(ftxHT ftxH           /* in - ftx handle */
           );

void
ftx_init();

statusT
rbf_set_bfset_params(
    bfSetT *bfSetp,             /* in - bitfile-set's descriptor pointer */
    bfSetParamsT *bfSetParams,  /* in - bitfile-set's params */
    ftxHT ftxH,
    long xid                    /* in - CFS transaction id */
    );

#define FTX_LOCKS

#define ftx_set_state( lkp, mp, s, ftxh ) \
       _ftx_set_state( lkp, mp, s, ftxh, __LINE__, __FILE__ )

/*
 * Lock shortcut macros
 */

#define FTX_LOCKREAD( lkp, ftxh ) { \
    ftx_lock_read( (lkp), (ftxh), __LINE__, __FILE__ ); \
}
#define FTX_LOCKWRITE( lkp, ftxh ) { \
    ftx_lock_write( (lkp), (ftxh), __LINE__, __FILE__ ); \
}

#define FTX_ADD_LOCK( lkp, ftxh ) {\
       _ftx_add_lock( lkp, ftxh, __LINE__, __FILE__ ); \
}

void
ftx_lock_write(
    ftxLkT *lk,      /* in */
    ftxHT ftxH,      /* in */
    int ln,          /* in */
    char *fn         /* in */
    );

void
ftx_lock_read(
    ftxLkT *lk,      /* in */
    ftxHT ftxH,      /* in */
    int ln,          /* in */
    char *fn         /* in */
    );

void
_ftx_add_lock(
              ftxLkT *lk,      /* in */
              ftxHT ftxH,      /* in */
              int ln,          /* in */
              char *fn         /* in */
              );

#endif  /* FTX_PUBLIC */
