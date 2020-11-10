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
 *      AdvFS
 *
 * Abstract:
 *
 *      Bitfile Set management routines.
 *
 * Date:
 *
 *      Tue Sep 10 12:52:11 1991
 *
 */
/*
 * HISTORY
 * 
 * 
 */
#pragma ident "@(#)$RCSfile: bs_bitfile_sets.c,v $ $Revision: 1.1.473.33 $ (DEC) $Date: 2008/01/18 14:30:02 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/ms_osf.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/bs_stripe.h>
#include <msfs/bs_extents.h>
#include <sys/syslog.h>
#include <sys/lock_probe.h>
#include <sys/clu.h>
#include <msfs/bs_stripe.h>
#include <msfs/bs_extents.h>
#include <kern/sched_prim.h>
#include <kern/event.h>
#include <msfs/advfs_evm.h>
#include <msfs/bs_msg_queue.h>

#ifdef _OSF_SOURCE
#include <sys/mode.h>
#else
#include <sys/stat.h>
#endif

#define ADVFS_MODULE BS_BITFILE_SETS

#define RBF_PIN_FIELD( h, f ) rbf_pin_record( h, &(f), sizeof( f ) )

decl_simple_lock_info(, ADVbfSetT_setMutex_lockinfo )
decl_simple_lock_info(, ADVbfSetT_accessChainLock_lockinfo )
decl_simple_lock_info(, ADVLookupMutex_lockinfo )
decl_simple_lock_info(, ADVbfSetT_cloneDelStateMutex_lockinfo)
decl_simple_lock_info(, ADVbfSetT_bfSetMutex_lockinfo )
struct lockinfo * ADVbfSetT_dirLock_info ;
struct lockinfo * ADVbfSetT_fragLock_info;

/*
 * These can be patched via DBX on-the-fly.  Can go in sysconfigtab
 * as a dynamic variable.
 */

#ifdef ADVFS_SMP_ASSERT
int AdvfsFragDbg = 0;  /* 1 == bs_frag_validate calls dmn_panic*/
                       /* 2 == bs_frag_validate calls SAD */
                       /* 3 == display frag group msgs */
                       /* 4 == also display frag msgs  */
#endif
/*
 * Private prototypes
 */
static statusT bfs_access(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set desc */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    uint32T options,       /* in - options flags */
    ftxHT ftxH             /* in - transaction handle */
    );

#ifdef ADVFS_SMP_ASSERT
static void bs_frag_validate(
    bfSetT *setp);        /* in */
#endif

statusT rbf_add_overlapping_clone_stg(
    bfAccessT *bfap,         /* in */
    uint32T pageOffset,      /* in */
    uint32T pageCnt,         /* in */
    uint32T holePg,          /* in */
    ftxHT parentFtx,         /* in */
    uint32T *allocPageCnt);  /* out */

#ifdef ADVFS_BFSET_TRACE
void
bfset_trace( bfSetT  *bfsetp,
             uint16T module,
             uint16T line,
             void    *value)
{
    register bfsetTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    bfsetp->trace_ptr = (bfsetp->trace_ptr + 1) % BFSET_TRACE_HISTORY;
    te = &bfsetp->trace_buf[bfsetp->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_BFSET_TRACE */

/****************************************************************************
 * frag mgt
 ****************************************************************************/

/* implementation notes:
 *
 * - the frag bf is divided into 8 lists of frag groups
 * - group lists 1 thru 7 represent the frag groups for 1k thru 8k frags
 * - group list 0: when a group has become completely free
 *   we can place it on group list 0 where it is available for use by another
 * - when the number of free groups on list 0 exceeds a "max" threshold, a
 *   thread is notified to trim list 0 back to a "min" threshold.
 *   group list if another group list runs out of groups.
 * - if a group is on a list then it has at least one free frag
 * - each group consists of 16 8k pages
 * - each group consists of 128 1k slots
 * - the frag bf consists of some number of 1k slots
 * - the frag bf is addressed either by group page number or slot number
 *   in other words, the frag bf can be viewed as:
 *       o an array of 8k pages (like any other btifile)
 *       o an array of 1k slots
 *       o an array of 128K groups
 * - a group is identified by its starting page number
 * - slots are 1k each (slot 0 is the first slot on page 0)
 * - a frag is identified by its starting slot number
 * - frags do not span groups (thus some groups have wasted slots at the end)
 * - groups are linked together via pointers in the group header
 * - empty (all frags are in-use) groups are removed from the head of the list
 * - groups transitioning from empty to non-empty are placed at the end of
 *   the list
 * - each group has its own free list of frags; the free list is maintained
 *   in the group header
 * - frags are allocated from the head of the list
 * - frags are put back (deallocated) at the head of the list
 * - frag headers are valid only when the frag is on a free list otherwise
 *   that space is available for use by the client (the client will clobber
 *   the header!!).  NOTE: frag headers are not used for FRAGBF Version 1 and
 *   beyond.
 *
 *
 * The figure below shows the frag group descriptors for frag types 0 to 7.
 * Each descriptor contains a 'head' pointer and a 'tail' pointer.  Allocation
 * is done from the head and deallocation is done at the tail.
 *
 *      +-+
 *      |0|
 *      |H|
 *      |T|
 *      +-+
 *      |1|
 *      |H|
 *      |T|
 *      +-+
 *      |2|
 *      |H|
 *      |T|  Group1                       Group3
 *      +-+  +---+-----+-----+---+-----+  +---+-----+-----+---+-----+
 *      |3|  |hdr|frag1|frag2|...|fragN|->|hdr|frag1|frag2|...|fragN|
 *      |H|->+---+-----+-----+---+-----+  +---+-----+-----+---+-----+
 *      |T|-------------------------------^
 *      +-+
 *      |4|
 *      |H|
 *      |T|
 *      +-+
 *      |5|
 *      |H|
 *      |T|  Group2
 *      +-+  +---+-----+-----+---+-----+
 *      |6|  |hdr|frag1|frag2|...|fragN|
 *      |H|->+---+-----+-----+---+-----+
 *      |T|--^
 *      +-+
 *      |7|
 *      |H|
 *      |T|
 *      +-+
 *
 * The above three groups might be layed out in the frag bf as follows:
 *
 *      Group1            Group2             Group3
 *      +----------------+------------------+-----------------+
 *      |pg0|pg1|...|pg15|pg16|pg17|...|pg31|pg32|pg33|..|pg47|
 *      +----------------+------------------+-----------------+
 *      ^                ^                  ^
 *      slot0            slot128            slot256
 *
 * The above descriptions are all of the on-disk format of the frag bf.
 *
 */

void
print_set_id(
    bfSetT *setp
    )
{
    printf( "0x%08x.%05x.%x.%04x",
            setp->bfSetId.domainId.tv_sec,
            setp->bfSetId.domainId.tv_usec,
            setp->bfSetId.dirTag.num,
            setp->bfSetId.dirTag.seq );
}

/*
 * When the number of groups on list 0 (zero) exceeds
 * BF_FRAG_FREE_GRPS_MAX the bs_fragbf_thread() is sent a message
 * to inform it that a fileset's list 0 needs to be trimmed
 * back to BF_FRAG_FREE_GRPS_MIN.
 *
 * FragBfMsgQH is the handle to the message queue used to
 * communicate with the bs_fragbf_thread().
 *
 * The message format is defined by fragBfMsgT.
 */

static msgQHT FragBfMsgQH;

typedef struct {
    bfSetIdT bfSetId;
} fragBfMsgT;


/*
 * bs_fragbf_thread()
 *
 * This thread's purpose is to wait for a message telling
 * it to deallocated frag groups from the specified fileset's
 * list 0 (zero) which is the fileset's list of available
 * frag groups.  The thread will deallocate frag groups until
 * there are no more than BF_FRAG_FREE_GRPS_MIN on the list.
 *
 * There is only one of these threads per system.  It handles
 * requests for all filesets on the system.
 *
 * The thread that sent a message to the bs_fragbf_thread()
 * must set the "truncating" flag in the corresponding fileset
 * descriptor.  The prevents others from flooding the message
 * queue with requests for the same fileset.  There should never
 * be more than one message per fileset on the message queue.
 * When the bs_fragbf_thread() is finished with a fileset it
 * clears the "truncating" flag.
 */

void
bs_fragbf_thread( void )
{
    fragBfMsgT *msg;
    bfSetIdT bfSetId;
    bfSetT *bfSetp;
    bsBfSetAttrT *setAttrp;
    grpHdrT *grpHdrp;
    bfPageRefHT grpPgRef;
    uint32T grpPg, nextFreeGrp;
    statusT sts;
    int done;
    uint32T delCnt;
    void *delListp;
    ftxHT ftxH;
    bfAccessT *bfap;
    rbfPgRefHT pinPgH;
    boolean_t migtrunc_locked = FALSE;
    struct vnode *nullvp;

    while (TRUE) {

        /*
         * Wait for something to do
         */

        msg = msgq_recv_msg( FragBfMsgQH );

        bfSetId = msg->bfSetId;

#ifdef ADVFS_SMP_ASSERT
        if (AdvfsFragDbg > 2) {
            aprintf( "ADVFS-FRAG: trunc frag bf for fileset 0x%08x.%04x.%04x\n",
                    bfSetId.domainId.tv_sec,
                    bfSetId.domainId.tv_usec,
                    bfSetId.dirTag.num );
        }
#endif

        msgq_free_msg( FragBfMsgQH, msg );

        /*
         * Open the fileset and the fileset's frag bitfile.  This is
         * necessary in case the fileset is unmounted while we're working
         * on it; we can't have it disappear on us in the middle.
         */

        sts = rbf_bfs_access( &bfSetp, bfSetId, FtxNilFtxH );
        if (sts != EOK) {
            continue;
        }

        if ( ! BFSET_VALID(bfSetp) ) {
            goto close_bfs;
        }

        nullvp = NULL;
        sts = bs_access( &bfap, bfSetp->fragBfTag, bfSetp, FtxNilFtxH, 
                         0, NULLMT, &nullvp );
        if (sts != EOK) {
            goto close_bfs;
        }

        /*
         * Deallocate frag groups until the number of frags on the list
         * is at or below BF_FRAG_FREE_GRPS_MIN.
         */

        done = FALSE;

        while (!done) {
            /*
             * Coordinate with migrate code.  Note that we must take this
             * lock before starting the transaction since that is the same
             * order that code in the migrate path does it.
             */
            MIGTRUNC_LOCK_READ(&(bfap->xtnts.migTruncLk));

            /* No need to take the quota mt locks here. Storage is only
             * added to the quota files at file creation, chown, and
             * explicit quota setting - it is never removed (at least
             * currently).
             * The potential danger path would be
             * stg_remove_stg_start --> fs_quota_blks --> dqsync -->
             * rbf_add_stg
             * But it is tame in this case.
             */
            migtrunc_locked = TRUE;

            sts = FTX_START_N( FTA_FRAG_GRP_DEALLOC,
                               &ftxH, FtxNilFtxH, bfSetp->dmnP, 0 );
            if (sts != EOK) {
                goto close_fragbf;
            }


            FTX_LOCKWRITE( &bfSetp->fragLock, ftxH )

            grpPg = bfSetp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp;

#ifdef ADVFS_SMP_ASSERT
            if (AdvfsFragDbg > 2) {
                aprintf( "ADVFS-FRAG: trunc group %d for fileset 0x%08x.%04x.%04x\n",
                        grpPg,
                        bfSetId.domainId.tv_sec,
                        bfSetId.domainId.tv_usec,
                        bfSetId.dirTag.num );
            }
#endif

            /*
             * Get a pointer to the next free group
             */

            sts = bs_refpg( &grpPgRef,
                            (void*)&grpHdrp,
                            bfap,
                            grpPg,
                            BS_NIL );
            if (sts != EOK) {
                ftx_fail( ftxH );
                goto close_fragbf;
            }

            nextFreeGrp = grpHdrp->nextFreeGrp;

            bs_derefpg( grpPgRef, BS_RECYCLE_IT );

            /*
             * Update the header for list 0 (zero) to point to the
             * next free frag group.
             */

            sts = bmtr_get_rec_ptr( bfSetp->dirBfAp,
                                    ftxH,
                                    BSR_BFS_ATTR,
                                    sizeof( *setAttrp ),
                                    1 /* pin pg */,
                                    (void *) &setAttrp,
                                    &pinPgH,
                                    NULL );
            if (sts != EOK) {
                ftx_fail( ftxH );
                goto close_fragbf;
            }


            /*
             * Deallocate the current frag group.
             */

           sts = stg_remove_stg_start( bfap,
                                       grpPg,
                                       BF_FRAG_GRP_PGS,
                                       FALSE,          /* no quotas */
                                       ftxH,
                                       &delCnt,
                                       &delListp,
                                       TRUE           /* do COW */
                                      );
            if ((sts != EOK) || (delCnt == 0)) {
                ftx_fail( ftxH );
                goto close_fragbf;
            }

            RBF_PIN_FIELD( pinPgH,
                setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp );
            setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = nextFreeGrp;
            bfSetp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = nextFreeGrp;

            RBF_PIN_FIELD( pinPgH, setAttrp->freeFragGrps );
            setAttrp->freeFragGrps--;
            bfSetp->freeFragGrps--;
            done = (bfSetp->freeFragGrps <= BF_FRAG_FREE_GRPS_MIN);

#ifdef ADVFS_SMP_ASSERT
            if (AdvfsFragDbg > 0) {
                bs_frag_validate( bfSetp );
            }
#endif

            ftx_done_n( ftxH, FTA_FRAG_GRP_DEALLOC );

            MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
            migtrunc_locked = FALSE;

            stg_remove_stg_finish( bfap->dmnP, delCnt, delListp );
        }

close_fragbf:
        if (migtrunc_locked) {
            MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
            migtrunc_locked = FALSE;
        }
        bs_close(bfap, 0);

close_bfs:
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

        /*
         * We're done with this set.  Clear the "truncating" flag
         * so that frag_group_dealloc() knows that it can send messages
         * to this thread.
         */
        if (BFSET_VALID(bfSetp) ) {
            bfSetp->truncating = FALSE;
        }
    }

    /* NOT REACHED */
}

/*
 * frag_group_dealloc
 *
 * Deallocates a frag group from a specific frag-type group (like
 * a 6k frag group) and places the free frag group on the fileset's
 * free frag group list (frag type 0 (zero)).  If the free frag group
 * list reaches the threshold for the maximum desired free frags then
 * this routine will send a message to the bs_fragbf_thread() to
 * truncate the free list.
 *
 * Assumes that the fragLock in the set descriptor is locked.
 */

statusT
frag_group_dealloc(
    bfSetT *setp,               /* in - fields modified */
    ftxHT ftxH,                 /* in */
    bfFragT fragType,           /* in */
    uint32T grpPg,              /* in */
    grpHdrT *grpHdrp,           /* in */
    rbfPgRefHT grpPgRef,        /* in */
    uint32T firstGrpPg,         /* in */
    uint32T lastGrpPg           /* in */
    )
{
    statusT sts;
    uint32T pg = 0;
    int found = FALSE;
    uint32T nextFreeGrp = grpHdrp->nextFreeGrp;
    rbfPgRefHT pinPgH;
    bsBfSetAttrT *setAttrp;

#ifdef ADVFS_SMP_ASSERT
    if (AdvfsFragDbg > 2) {
        aprintf( "ADVFS-FRAG: dealloc group %6d, type %d, for fileset 0x%08x.%04x.%04x\n",
                grpPg,
                fragType,
                setp->bfSetId.domainId.tv_sec,
                setp->bfSetId.domainId.tv_usec,
                setp->bfSetId.dirTag.num );
    }
#endif

    if (grpPg != firstGrpPg) {

        /*
         * Scan for the group that is ahead of the group
         * to be deallocated.  This is a singly-linked list
         * so to remove the group we need a reference to the
         * previous group.
         */

        pg = firstGrpPg;

        while (!found && (pg != BF_FRAG_EOG)) {
            bfPageRefHT pgRef;
            grpHdrT *pgp;

            sts = bs_refpg( &pgRef,
                            (void *) &pgp,
                            setp->fragBfAp,
                            pg,
                            BS_NIL );

            if (sts != EOK) {
                printf("frag_group_dealloc: bs_refpg failed, return code = %d", sts );
                return( sts );
            }

            if (pgp->nextFreeGrp == grpPg) {
                found = TRUE;
            } else {
                pg = pgp->nextFreeGrp;
            }

            bs_derefpg( pgRef, BS_CACHE_IT );
        }


        if (pg != BF_FRAG_EOG) {
            rbfPgRefHT rbfPgRef;
            grpHdrT *pgp;

            /*
             * Update the previous frag group to point to the group
             * after the group that we are deallocating.
             */

            sts = rbf_pinpg( &rbfPgRef,
                             (void *) &pgp,
                             setp->fragBfAp,
                             pg,
                             BS_SEQ_AHEAD,
                             ftxH );
            if (sts != EOK) {
                printf("frag_group_dealloc: rbf_pinpg failed, return code = %d", sts );
                return( sts );
            }

            RBF_PIN_FIELD( rbfPgRef, pgp->nextFreeGrp );
            pgp->nextFreeGrp = nextFreeGrp;
        }
    }

    /*
     * Update the frag group headers, moving the
     * group being deallocated from it's current frag
     * group to the free frag group list (frag type 0).
     */

    sts = bmtr_get_rec_ptr( setp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &pinPgH,
                            NULL );
    if (sts != EOK) {
        printf("frag_group_dealloc: bmtr_get_rec_ptr failed, return code = %d", sts );
        return( sts );
    }

    if (grpPg == lastGrpPg) {
        /* update free list descriptor tail pointer to point to the group */

        if (!found) {
            printf("frag_group_dealloc: tried to use pg that was not found" );
            return(E_FRAG_PAGE);
        }

        RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[ fragType ].lastFreeGrp );
        setAttrp->fragGrps[ fragType ].lastFreeGrp = pg;
        setp->fragGrps[ fragType ].lastFreeGrp = pg;
    }

    if (grpPg == firstGrpPg) {
        /* also update the list head pointer if the list was empty */

        RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[ fragType ].firstFreeGrp );
        setAttrp->fragGrps[ fragType ].firstFreeGrp = nextFreeGrp;
        setp->fragGrps[ fragType ].firstFreeGrp = nextFreeGrp;
    }

    RBF_PIN_FIELD( grpPgRef, grpHdrp->nextFreeGrp );
    grpHdrp->nextFreeGrp = setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp;

    RBF_PIN_FIELD( grpPgRef, grpHdrp->fragType );
    grpHdrp->fragType = BF_FRAG_FREE_GRPS;

    RBF_PIN_FIELD( grpPgRef, grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] );
    grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] = BF_FRAG_NULL;

    RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[BF_FRAG_FREE_GRPS].firstFreeGrp);
    setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpPg;
    setp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpPg;

    RBF_PIN_FIELD( pinPgH, setAttrp->freeFragGrps );
    setAttrp->freeFragGrps++;
    setp->freeFragGrps++;

    if (!setp->truncating && (setp->freeFragGrps >= BF_FRAG_FREE_GRPS_MAX)) {

        /*
         * The free frag group list (frag type 0) has too many free
         * frag groups in it (wasting free disk space) so we send a
         * message to the bs_fragbf_thread() to truncate the list.
         * Note that we don't execute this code if the 'truncating'
         * flag is set; it indicates that we've already sent a message
         * to the thread and the thread has not finished proccessing
         * previous request.
         */

        fragBfMsgT *msg = msgq_alloc_msg( FragBfMsgQH );

        if (msg != NULL) {
            setp->truncating = TRUE;

            msg->bfSetId = setp->bfSetId;

#ifdef ADVFS_SMP_ASSERT
            if (AdvfsFragDbg > 2) {
                aprintf( "ADVFS-FRAG: request trunc for fileset 0x%08x.%04x.%04x\n",
                        setp->bfSetId.domainId.tv_sec,
                        setp->bfSetId.domainId.tv_usec,
                        setp->bfSetId.dirTag.num );
            }
#endif

            msgq_send_msg( FragBfMsgQH, msg );
        }
    }

#ifdef ADVFS_SMP_ASSERT
    if (AdvfsFragDbg > 0) {
        bs_frag_validate( setp );
    }
#endif
    return(EOK);
}

/*
 * frag_group_init
 *
 * Initializes the frag group header and the frag free list for
 * for a frag group in the frag bf.
 *
 * Assumes that the fragLock in the set descriptor is locked.
 */


static void
frag_group_init(
    bfSetT *setp,       /* in */
    bfFragT fragType,   /* in */
    uint32T grpPg,      /* in */
    grpHdrT *grpHdrp    /* in */
   )
{
    int f, slot;

    /*
     * initialize the group header.
     */

    grpHdrp->self         = grpPg;
    grpHdrp->setId        = setp->bfSetId;
    grpHdrp->nextFreeFrag = BF_FRAG_EOF;     /* used only when 'version == 0' */
    grpHdrp->lastFreeFrag = BF_FRAG_EOF;     /* used only when 'version == 0' */
    grpHdrp->nextFreeGrp  = BF_FRAG_EOG;
    grpHdrp->fragType     = fragType;
    grpHdrp->freeFrags    = FRAGS_PER_GRP( fragType );
    grpHdrp->version      = BF_FRAG_VERSION;
    grpHdrp->firstFrag    = grpPg * BF_FRAG_PG_SLOTS + 1;

    /*
     * The group's header contains a free frag list.  It is a
     * simple singly-linked free list implemented using an
     * array where each element corresponds to a frag slot
     * in the group and each element contains the frag slot
     * index of the next free element in the list.  Slot 0 (zero)
     * is used as the free list header becuase frag slot 0
     * can never be assigned to a frag because that is where
     * group header is maintained!  Note that the list contains
     * an element for each possible frag slot but for frag types
     * greater than 1k only the first frag slot of a multi-slot
     * frag needs to be represented in the free list array.  Therefore,
     * the free list array has many unused entries for mutlti-slot
     * frag types (> 1K).  For example, the first few elements of
     * a free list for frag type 2 would look like this:
     *
     *    element #  0  1  2  3  4  5  6  7  8 ...
     *    contents   1  3  0  5  0  7  0  9  0 ...
     *
     * For frag type 3:
     *
     *    element #  0  1  2  3  4  5  6  7  8 ...
     *    contents   1  4  0  0  7  0  0 10  0 ...
     * 
     * In order to minimize the number of frags within a group that
     * cross a page boundary, all 2K, 4K and 6K frags start on even 
     * boundaries in version 4 domains.  This moves the wasted space 
     * within a group from the end to the beginning of that group. 
     */
    
    slot=1;

    if (setp->dmnP->dmnVersion >= FIRST_MIN_FRAG_CROSSINGS_VERSION)
    {
        switch (fragType)
        {
        case BF_FRAG_2K:
        case BF_FRAG_6K: slot=2; break;
        case BF_FRAG_4K: slot=4; break;
        }
    }

    grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] = slot;

    for (f = 1; f < grpHdrp->freeFrags; f++) {
        grpHdrp->freeList[ slot ] = slot + fragType;
        slot += fragType;
    }

    grpHdrp->freeList[ slot ] = BF_FRAG_NULL;
}

/*
 * frag_list_extend
 *
 * Allocates a new frag group of pages to the specified frag list.
 *
 * Assumes that the fragLock in the set descriptor is locked.
 */

#define INIT_GRPS 3

static statusT
frag_list_extend(
    bfSetT *setp,       /* in */
    bfFragT fragType,   /* in */
    ftxHT parentFtxH    /* in */
    )
{
    rbfPgRefHT grpPgRef;
    statusT sts;
    grpHdrT *grpHdrp;
    int g;
    ftxHT ftxH;
    uint32T grpPg, newPg, newPgCnt, initGrps, holePg, holePgCnt;
    bfAccessT *fragBfAp;
    bsBfSetAttrT *setAttrp;
    rbfPgRefHT pinPgH;

    struct {
        rbfPgRefHT grpPgRef;
        grpHdrT *grpHdrp;
    } grps[INIT_GRPS];

    MS_SMP_ASSERT(lock_islocked(&(setp->fragBfAp->xtnts.migTruncLk)));

    if ((setp->fragGrps[ fragType ].firstFreeGrp != BF_FRAG_EOG) ||
        (setp->fragGrps[ fragType ].lastFreeGrp != BF_FRAG_EOG)) {
        ADVFS_SAD3( "frag list is not empty",
                    fragType,
                    setp->fragGrps[ fragType ].firstFreeGrp,
                    setp->fragGrps[ fragType ].lastFreeGrp );
    }

    if (setp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp == BF_FRAG_EOG ) {
        MS_SMP_ASSERT(setp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp ==
                      BF_FRAG_EOG);

        /* Make sure the extents are valid before calling imm_get_first_hole.
         * Returns with fragBfAp->xtntMap_lk read-locked.
         */
        fragBfAp = setp->fragBfAp;

        sts = x_load_inmem_xtnt_map( fragBfAp, X_LOAD_REFERENCE);
        if (sts != EOK) {
            goto EXIT_FRAG_LIST_EXTEND;
        }

        /*
         * The free groups list (frag type 0) is empty then allocate
         * a few groups to it.
         */

        sts = FTX_START_N( FTA_FRAG_ALLOC, &ftxH, parentFtxH,
                           setp->dmnP, 3 );
        if (sts != EOK) {
            XTNMAP_UNLOCK( &(fragBfAp->xtntMap_lk) )
            goto EXIT_FRAG_LIST_EXTEND;
        }

        /*
         * Since we may have deallocated some groups from the frag bf, it
         * may be sparse.  We should try to fill in the sparse holes before
         * extending the frag bf.
         */

        imm_get_first_hole( &fragBfAp->xtnts, &holePg, &holePgCnt );

        if (holePgCnt == 0) {
            /*
             * The frag bf is not sparse so extend it.
             */
            newPg = fragBfAp->nextPage;
            initGrps = INIT_GRPS;

        } else {
            /*
             * The frag bf is sparse so fill in a hole.
             */
            newPg = holePg;
            initGrps = MIN( INIT_GRPS, holePgCnt / BF_FRAG_GRP_PGS );
        }

        newPgCnt = BF_FRAG_GRP_PGS * initGrps;

#ifdef ADVFS_SMP_ASSERT
        if (AdvfsFragDbg > 2) {
            aprintf( "ADVFS-FRAG: %s at pg %6d, pgcnt %2d, for fileset 0x%08x.%04x.%04x\n",
                    (holePgCnt == 0) ? "extend " : "fill   ", newPg, newPgCnt,
                    setp->bfSetId.domainId.tv_sec,
                    setp->bfSetId.domainId.tv_usec,
                    setp->bfSetId.dirTag.num );
        }
#endif

        /* Unlock the extent maps here; the add storage will relock them
         * for modification.
         */
        XTNMAP_UNLOCK( &(fragBfAp->xtntMap_lk) )

        sts = rbf_add_stg( setp->fragBfAp, newPg, newPgCnt, ftxH, 1 );
        if (sts != EOK) {
            ftx_fail( ftxH );
            goto EXIT_FRAG_LIST_EXTEND;
        }

        /*
         * First pin all the group header pages, this way if a pin fails
         * we can abort gracefully.  If we had done pin_rec in between
         * each pinpg then we would not be able to backout of a failed pinpg.
         */

        for (g = 0; g < initGrps; g++) {

            grpPg = newPg + (g * BF_FRAG_GRP_PGS);

            sts = rbf_pinpg( &grps[g].grpPgRef,
                             (void*)&grps[g].grpHdrp,
                             setp->fragBfAp,
                             grpPg,
                             BS_SEQ_AHEAD,
                             ftxH );
            if (sts != EOK) {
                ftx_fail( ftxH );
                goto EXIT_FRAG_LIST_EXTEND;
            }
        }

        for (g = initGrps - 1; g >= 0; g--) {

            /* update free list; new group goes to front of free list */

            grpPg = newPg + (g * BF_FRAG_GRP_PGS);
            grpHdrp = grps[g].grpHdrp;
            grpPgRef = grps[g].grpPgRef;

            sts = bmtr_get_rec_ptr( setp->dirBfAp,
                                    ftxH,
                                    BSR_BFS_ATTR,
                                    sizeof( *setAttrp ),
                                    1 /* pin pg */,
                                    (void *) &setAttrp,
                                    &pinPgH,
                                    NULL );
            if (sts != EOK) {
                ADVFS_SAD1( "get bf set attr failed", sts );
            }

            rbf_pin_record( grpPgRef, grpHdrp, BF_FRAG_SLOT_BYTES );
            bzero( (char *) grpHdrp, BF_FRAG_SLOT_BYTES );
            grpHdrp->nextFreeGrp  =
                           setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp;
            grpHdrp->self         = grpPg;
            grpHdrp->setId        = setp->bfSetId;
            grpHdrp->nextFreeFrag = BF_FRAG_EOF;
            grpHdrp->lastFreeFrag = BF_FRAG_EOF;
            grpHdrp->fragType     = BF_FRAG_FREE_GRPS;
            grpHdrp->version      = BF_FRAG_VERSION;
            grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] = BF_FRAG_NULL;

            RBF_PIN_FIELD( pinPgH,
                           setAttrp->fragGrps[BF_FRAG_FREE_GRPS].firstFreeGrp);
            setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpPg;
            setp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpPg;

            if ( setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp ==
                 BF_FRAG_EOG ) {
                /*
                 * if the free list was empty then set last free grp ptr too
                 */
                RBF_PIN_FIELD( pinPgH,
                             setAttrp->fragGrps[BF_FRAG_FREE_GRPS].lastFreeGrp);
                setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp = grpPg;
                setp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp = grpPg;
            }

            RBF_PIN_FIELD( pinPgH, setAttrp->freeFragGrps );
            setAttrp->freeFragGrps++;
            setp->freeFragGrps++;
        }

        /*
         * Set done mode to skip undo for sub ftxs, in this case,
         * the rbf_add_stg, so it will not be undone after the
         * current ftx is done.  This is because the new pages will
         * be initialized and the global free list updated, so the
         * new pages will become visible after either this
         * parent ftx completes, or the system restarts.  Basically,
         * we don't want the pages to disappear due to an undo.
         */

        ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

        ftx_done_n( ftxH, FTA_FRAG_ALLOC );
    }

    /*
     * Move a free group from the free groups list to the
     * group list that corresponds to the specified frag type.
     */

    sts = FTX_START_N( FTA_FRAG_ALLOC2, &ftxH, parentFtxH,
                           setp->dmnP, 3 );
    if (sts != EOK) {
        goto EXIT_FRAG_LIST_EXTEND;
    }

    grpPg = setp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp;

#ifdef ADVFS_SMP_ASSERT
    if (AdvfsFragDbg > 2) {
        aprintf( "ADVFS-FRAG: alloc group %8d, type %d, for fileset 0x%08x.%04x.%04x\n",
                grpPg, fragType,
                setp->bfSetId.domainId.tv_sec,
                setp->bfSetId.domainId.tv_usec,
                setp->bfSetId.dirTag.num );
    }
#endif

    sts = rbf_pinpg( &grpPgRef,
                     (void*)&grpHdrp,
                     setp->fragBfAp,
                     grpPg,
                     BS_SEQ_AHEAD,
                     ftxH );
    if (sts != EOK) {
        ftx_fail( ftxH );
        goto EXIT_FRAG_LIST_EXTEND;
    }

    sts = bmtr_get_rec_ptr( setp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &pinPgH,
                            NULL );
    if (sts != EOK) {
        ADVFS_SAD1( "get bf set attr failed", sts );
    }

    RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[BF_FRAG_FREE_GRPS].firstFreeGrp );
    setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpHdrp->nextFreeGrp;
    setp->fragGrps[ BF_FRAG_FREE_GRPS ].firstFreeGrp = grpHdrp->nextFreeGrp;
    if ( grpHdrp->nextFreeGrp == BF_FRAG_EOG ) {
        RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[BF_FRAG_FREE_GRPS].lastFreeGrp );
        setAttrp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp = BF_FRAG_EOG;
        setp->fragGrps[ BF_FRAG_FREE_GRPS ].lastFreeGrp = BF_FRAG_EOG;
    }

    rbf_pin_record( grpPgRef, grpHdrp, sizeof( grpHdrT ) );
    frag_group_init( setp, fragType, grpPg, grpHdrp );

    RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[ fragType ] );
    setAttrp->fragGrps[ fragType ].firstFreeGrp = grpPg;
    setAttrp->fragGrps[ fragType ].lastFreeGrp = grpPg;
    setp->fragGrps[ fragType ].firstFreeGrp = grpPg;
    setp->fragGrps[ fragType ].lastFreeGrp = grpPg;

    RBF_PIN_FIELD( pinPgH, setAttrp->freeFragGrps );
    setAttrp->freeFragGrps--;
    setp->freeFragGrps--;

#ifdef ADVFS_SMP_ASSERT
    if (AdvfsFragDbg > 0) {
        bs_frag_validate( setp );
    }
#endif
    ftx_done_n( ftxH, FTA_FRAG_ALLOC2 );

    sts = EOK;

EXIT_FRAG_LIST_EXTEND:
    return(sts);
}

/*
 * bs_frag_alloc
 *
 * Allocate a frag and return the fragId to the caller
 *
 * Note: this routine supports frag bf format version 0 and 1.
 *
 * Note: this routine returns with the fragLocked locked (always!).
 */

statusT
bs_frag_alloc(
    bfSetT *setp,       /* in */
    bfFragT fragType,   /* in */
    ftxHT ftxH,         /* in */
    bfFragIdT *fragId   /* out */
    )
{
    statusT sts;
    uint32T frag, grpPg, fragPg, fragSlot;
    fragHdrT *fragHdrp;
    grpHdrT *grpHdrp;
    slotsPgT *grpPgp, *fragPgp;
    bsBfSetAttrT *setAttrp;
    rbfPgRefHT pinPgH, grpPgRef, fragPgRef;
    boolean_t have_not_found_frag = TRUE;
    uint32T freeFrags = 0;

    if (setp->fragBfAp == NULL) {
        ADVFS_SAD0( "no frag bf" );
    }

    if (ftxH.hndl == 0) {
        ADVFS_SAD0( "no parent ftx" );
    }

    if ((fragType < BF_FRAG_1K) || (fragType >= BF_FRAG_MAX)) {
        ADVFS_SAD0( "invalid frag type" );
    }

    FTX_LOCKWRITE( &setp->fragLock, ftxH )

    while (have_not_found_frag) {

        if (setp->fragGrps[ fragType ].firstFreeGrp == BF_FRAG_EOG) {
            /*
             * no more free groups with free frags, allocate another
             * to this frag list
             */
    
            sts = frag_list_extend( setp, fragType, ftxH );
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
        }
    
        grpPg = setp->fragGrps[ fragType ].firstFreeGrp;
    
        sts = rbf_pinpg( &grpPgRef,
                         (void*)&grpPgp,
                         setp->fragBfAp,
                         grpPg,
                         BS_SEQ_AHEAD,
                         ftxH );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    
        grpHdrp = (grpHdrT *)grpPgp;
    
        if ((grpHdrp->self != grpPg) ||
            (grpHdrp->fragType != fragType) ||
            (grpHdrp->freeFrags == 0)) {
            /*
             * The domain's group header is not valid.  Issue a domain_panic.
             */
            printf("grpHdrp->self is %u and grpPg is %u\n",
                   grpHdrp->self, grpPg);
            printf("grpHdrp->fragType is %u and fragType is %u\n",
                   grpHdrp->fragType, fragType);
            printf("grpHdrp->freeFrags is %d - it should be non-zero\n",
                   grpHdrp->freeFrags);
            domain_panic( setp->dmnP, "bs_frag_alloc: invalid frag group" );
            RAISE_EXCEPTION( E_DOMAIN_PANIC );
        }
    
        if (grpHdrp->version == 0) {
    
            /*
             * Allocate from a frag group with version 0 (old) format.
             */
            frag     = grpHdrp->nextFreeFrag;
            fragPg   = FRAG2PG( frag );
            fragSlot = FRAG2SLOT( frag );
    
            if (fragPg != grpPg) {
                sts = rbf_pinpg( &fragPgRef,
                                 (void*)&fragPgp,
                                 setp->fragBfAp,
                                 fragPg,
                                 BS_SEQ_AHEAD,
                                 ftxH );
                if (sts != EOK) {
                    RAISE_EXCEPTION( sts );
                }
            } else {
                fragPgRef = grpPgRef;
                fragPgp   = grpPgp;
            }
    
            fragHdrp = (fragHdrT *)&fragPgp->slots[ fragSlot ];
    
            if ((fragHdrp->self != frag) ||
                (fragHdrp->fragType != fragType)) {
                /*
                 * An invalid frag should not panic the system.
                 */
                printf("fragHdrp->self is %u and frag is %u\n",
                       fragHdrp->self, frag);
                printf("fragHdrp->fragType is %u and fragType is %u\n",
                       fragHdrp->fragType, fragType);
                domain_panic( setp->dmnP, "bs_frag_alloc: invalid frag" );
                RAISE_EXCEPTION( E_DOMAIN_PANIC );
            }
    
            rbf_pin_record( fragPgRef, fragHdrp, sizeof( fragHdrT ) );
            rbf_pin_record( grpPgRef, grpHdrp, sizeof( grpHdrT ) );
    
            /* unlink the frag from the group's free list */
    
            grpHdrp->nextFreeFrag  = fragHdrp->nextFreeFrag;
            grpHdrp->freeFrags--;
            fragHdrp->nextFreeFrag = BF_FRAG_EOF;
    
            have_not_found_frag = FALSE;
    
        } else {
    
            /*
             * Allocate from a frag group with version 1 format.
             */
    
            /*
             * Check to see if group has already been marked as bad.
             */
           if (grpHdrp->lastFreeFrag == BF_FRAG_BAD_GROUP) {
    
                /*
                 * Set this in memory only so that the code below will
                 * pull this group off the free list of groups for this
                 * frag type.  We don't reset this on disk because we
                 * want to leave the on-disk state as-is for examination
                 * by utilities.
                 */
                grpHdrp->freeFrags = 0;
           }
           else {

                /*
                 * Check for corrupted intra-group free list.
                 * If free list is corrupted, mark group as bad and
                 * remove it from the free list of groups for this
                 * type.  The group will never again be allowed to
                 * have frags allocated from it.
                 */
                for (fragSlot = grpHdrp->freeList[BF_FRAG_NEXT_FREE], 
                     freeFrags = 0;
                     (fragSlot != BF_FRAG_NULL) && 
                     (freeFrags <= BF_FRAG_GRP_SLOTS);
                     fragSlot = grpHdrp->freeList[fragSlot], freeFrags++);
    
                if (freeFrags != grpHdrp->freeFrags) {
        
                    advfs_ev advfs_event;
                    bfPageRefHT badPgRef;
                    grpHdrT *badGroupHdrp;
                    clupThreadMsgT *msg;
                    extern msgQHT CleanupMsgQH;
        
                    ms_uaprintf("Warning: AdvFS has detected an inconsistency in a frag metadata file free list:\n");
                    ms_uaprintf("  Domain '%s'\n  Fileset '%s'\n  Frag group header page %d\n",
                                setp->dmnP->domainName, setp->bfSetName, grpHdrp->self);
                    ms_uaprintf("AdvFS is attempting to work around this problem but this message may indicate\n");
                    ms_uaprintf("metadata inconsistency in the '%s' fileset.  Unmount all filesets in the\n", setp->bfSetName);
                    ms_uaprintf("'%s' domain at the earliest convenient time and run the command:\n", setp->dmnP->domainName); 
                    ms_uaprintf("  /sbin/advfs/verify %s\n", setp->dmnP->domainName);
                    ms_uaprintf("to check the integrity of the domain's metadata.\n");
        
                    if (AT_INTR_LVL()) {
                        EvmEventPostImmedVa(NULL,
                                       EvmITEM_NAME,
                                       EVENT_FSET_BAD_FRAG,
                                       EvmITEM_CLUSTER_EVENT, EvmTRUE,
                                       EvmITEM_VAR_STRING,"domain", 
                                       setp->dmnP->domainName,
                                       EvmITEM_VAR_STRING,"fileset", 
                                       setp->bfSetName,
                                       EvmITEM_NONE);
                    } else {
                        bzero(&advfs_event, sizeof(advfs_ev));
                        advfs_event.domain = setp->dmnP->domainName;
                        advfs_event.fileset = setp->bfSetName;
                        advfs_post_kernel_event(EVENT_FSET_BAD_FRAG,&advfs_event);
                    }
        
                    /*
                     * Set the on-disk flag indicating that this frag group
                     * is bad.  Since we are deep inside of a transaction
                     * tree and since we may find an unbounded number of
                     * bad frag groups and since we're limited by how many
                     * subtransactions and pinned records AdvFS can handle,
                     * we cannot start a new subtransaction here and update
                     * the group header page.  Also, since another thread 
                     * may have modified this group header page under 
                     * transaction control, we cannot synchronously write
                     * out the page outside of a transaction.  So instead,
                     * we send a message to the cleanup thread, asking it
                     * to update this group header page under transaction
                     * control.  Until this happens, the in-memory flag
                     * BF_FRAG_BAD_GROUP will ward off any further attempts
                     * to allocate frags from this group.  It is not critical
                     * that this message be sent or that it be processed.
                     * If it is not, we will simply come through there again
                     * at frag allocation time during a subsequent reboot.
                     */
                    grpHdrp->lastFreeFrag = BF_FRAG_BAD_GROUP;
                    msg = (clupThreadMsgT *)msgq_alloc_msg( CleanupMsgQH );
                    if (msg) {
                        msg->msgType = UPDATE_BAD_FRAG_GRP_HDR;
                        msg->body.badFragGrp.bfSetId = setp->bfSetId;
                        msg->body.badFragGrp.badGrpHdrPg = grpPg;
                        msgq_send_msg( CleanupMsgQH, msg );
                    }

                    /*
                     * Set this in memory only so that the code below will
                     * pull this group off the free list of groups for this
                     * frag type.  We don't reset this on disk because we
                     * want to leave the on-disk state as-is for examination
                     * by utilities.
                     */
                    grpHdrp->freeFrags = 0;
                }
                else {
                
                    /*
                     * No corruption obvious in the intra-group free list.
                     * Grab the next free frag.
                     */
        
                    fragSlot = grpHdrp->freeList[ BF_FRAG_NEXT_FREE ];
        
                    if ((fragSlot < 1) ||
                        (fragSlot >= BF_FRAG_GRP_SLOTS))
                    {
                        /*
                         * Do not system panic for an invalid frag slot.
                         * This is domain specific.
                         */
                        domain_panic( setp->dmnP,
                            "bs_frag_alloc: invalid fragSlot %d", fragSlot );
                        RAISE_EXCEPTION( E_DOMAIN_PANIC );
                    }
        
                    RBF_PIN_FIELD(grpPgRef, grpHdrp->freeList[ BF_FRAG_NEXT_FREE ]);
                    grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] = 
                                                      grpHdrp->freeList[ fragSlot ];
        
                    RBF_PIN_FIELD( grpPgRef, grpHdrp->freeFrags );
                    grpHdrp->freeFrags--;
        
                    frag = grpPg * BF_FRAG_PG_SLOTS + fragSlot;
        
#ifdef ADVFS_SMP_ASSERT
                    if (AdvfsFragDbg > 3) {
                        aprintf( "ADVFS-FRAG: alloc frag %8d, slot %6d, grp %6d, type %d, for fileset 0x%08x.%04x.%04x\n",
                                frag, fragSlot, grpPg, fragType,
                                setp->bfSetId.domainId.tv_sec,
                                setp->bfSetId.domainId.tv_usec,
                                setp->bfSetId.dirTag.num );
                    }
#endif
        
                    have_not_found_frag = FALSE;
                }
            }
        }
    
        if (grpHdrp->freeFrags == 0) {
            /*
             * this was the last free frag in the group. remove the group
             * from the 'group free list'.
             */
    
            if (grpHdrp->version == 0) {
                if (grpHdrp->nextFreeFrag != BF_FRAG_EOF) {
                    /*
                     * Do not system panic for a bad v0 frag free list.
                     * There is a problem with the domain however.
                     */
                    domain_panic( setp->dmnP,
                        "bs_frag_alloc: bad v0 frag free list. grpHdrp->nextFreeFrag is %u", grpHdrp->nextFreeFrag);
                    RAISE_EXCEPTION( E_DOMAIN_PANIC );
                 }
            }
    
            sts = bmtr_get_rec_ptr( setp->dirBfAp,
                                    ftxH,
                                    BSR_BFS_ATTR,
                                    sizeof( *setAttrp ),
                                    1 /* pin pg */,
                                    (void *) &setAttrp,
                                    &pinPgH,
                                    NULL );
            if (sts != EOK) {
                /*
                 * Domain panic if we do not get a bf record pointer.
                 */
                domain_panic( setp->dmnP,
                    "bs_frag_alloc: get bf set attr failed with status %d",
                    sts );
                RAISE_EXCEPTION( E_DOMAIN_PANIC );
            }
    
            /* 
             * Update free list head for this frag list to point to next group 
             */
            RBF_PIN_FIELD(pinPgH, setAttrp->fragGrps[ fragType ].firstFreeGrp);
            setAttrp->fragGrps[ fragType ].firstFreeGrp = grpHdrp->nextFreeGrp;
            setp->fragGrps[ fragType ].firstFreeGrp   = grpHdrp->nextFreeGrp;
    
            if (grpHdrp->nextFreeGrp == BF_FRAG_EOG) {
                RBF_PIN_FIELD(pinPgH, setAttrp->fragGrps[fragType].lastFreeGrp);
                setAttrp->fragGrps[ fragType ].lastFreeGrp = BF_FRAG_EOG;
                setp->fragGrps[ fragType ].lastFreeGrp   = BF_FRAG_EOG;
            }
    
            /*
             * If we hit a corrupted frag group header page, just
             * dereference it.  
             */
            if (have_not_found_frag) {
                rbf_deref_page(grpPgRef, BS_NIL);
            }
            else {
                RBF_PIN_FIELD( grpPgRef, grpHdrp->nextFreeGrp );
                grpHdrp->nextFreeGrp = BF_FRAG_EOG;
    
                RBF_PIN_FIELD( grpPgRef, grpHdrp->lastFreeFrag );
                grpHdrp->lastFreeFrag = BF_FRAG_EOF;
            }
        }
    
#ifdef ADVFS_SMP_ASSERT
        if (AdvfsFragDbg > 0) {
            bs_frag_validate( setp );
        }
#endif
    
    } /* while (have_not_found_frag)

    /*
     * return allocated frag's fragId
     */
    fragId->frag = frag;
    fragId->type = fragType;
    return EOK;

HANDLE_EXCEPTION:
    return sts;
}

unsigned int AdvfsFragGroupDealloc = 1;
                 /*  sysconfig tunable to set frag group deallocation policy:
                  *  0: disables deallocation of frag groups
                  *  1: enables deallocation of frag groups (default)
                  */
/*
 * bs_frag_dealloc
 *
 * Note: this routine supports frag bf format version 0 and 1.
 *
 * Note: this routine returns with the fragLocked locked (always!).
 */

void
bs_frag_dealloc(
    bfSetT *setp,       /* in */
    ftxHT ftxH,         /* in */
    bfFragIdT fragId    /* in */
    )
{
    statusT sts;
    uint32T freeFrag;
    int fragBfOpen = FALSE;
    rbfPgRefHT grpPgRef, pinPgH;
    bsBfSetAttrT *setAttrp;
    bfFragT fragType;
    bsPageT firstGrpPg, lastGrpPg, grpPg;
    grpHdrT *grpHdrp;
    slotsPgT *grpPgp;
    struct vnode *nullvp = NULL;
    advfs_ev advfsEvent;

    if (setp->state == BFS_DELETING) {
        /* nothing to do the entire frag bf will be deleted */
        return;
    }

    if (ftxH.hndl == 0) {
        ADVFS_SAD0( "no parent ftx" );
    }

    if ( !lock_holder(&setp->fragLock.lock) ) {
        FTX_LOCKWRITE(&setp->fragLock, ftxH)
    }


    if (setp->fragBfAp == NULL) {
        /* fileset is not mounted so we need to open the fragbf on-the-fly */
        sts = bs_access( &setp->fragBfAp, setp->fragBfTag, setp, 
                         FtxNilFtxH, 0, NULLMT, &nullvp );

        if (sts != EOK) {
            printf("bs_frag_dealloc: bs_access failed, return code = %d", sts);
            goto abandoned_frag;
        }
        fragBfOpen = TRUE;
    }

    freeFrag = fragId.frag;
    fragType = fragId.type;

    if ((fragType < BF_FRAG_1K) || (fragType >= BF_FRAG_MAX)) {
        printf("bs_frag_dealloc: Invalid fragType = %d, frag = %d\n", fragType, freeFrag);
        goto abandoned_frag;
    }

    firstGrpPg = setp->fragGrps[ fragType ].firstFreeGrp;
    lastGrpPg = setp->fragGrps[ fragType ].lastFreeGrp;

    grpPg = FRAG2GRP( freeFrag );

    sts = rbf_pinpg( &grpPgRef,
                     (void*)&grpPgp,
                     setp->fragBfAp,
                     grpPg,
                     BS_NIL,
                     ftxH );
    if (sts != EOK) {
        printf("bs_frag_dealloc: rbf_pinpg failed(1), return code = %d\n", sts);
        goto abandoned_frag;
    }

    grpHdrp = (grpHdrT *)grpPgp;
    if ((grpHdrp->self != grpPg) || (grpHdrp->fragType != fragType)) {
        printf("bs_frag_dealloc: invalid frag" );
        goto abandoned_frag;
    }

    /*
     * If this frag group is marked as being bad, don't attempt
     * to manipulate the intra-group free list and don't put
     * the group onto any free list of groups.
     */
    if ((grpHdrp->version == 1) && 
        (grpHdrp->lastFreeFrag == BF_FRAG_BAD_GROUP)) {
        goto done;
    }

    if (AdvfsFragGroupDealloc &&
        !((grpPg == firstGrpPg) && (grpPg == lastGrpPg))) {

        if ((grpHdrp->freeFrags + 1) == FRAGS_PER_GRP( fragType )) {
            /*
             * This group now contains all free frags and it isn't the
             * only group so give to the global free list.
             */
            sts = frag_group_dealloc( setp, ftxH,
                                fragType, grpPg,
                                grpHdrp, grpPgRef,
                                firstGrpPg, lastGrpPg );
            if (sts != EOK) {
                goto abandoned_frag;
            }
            goto done;
        }
    }

    /* add the frag to end of its group's free list */

    if (grpHdrp->version == 0) {
        /*
         * Frag group has version 0 (old) format.
         */

        if (grpHdrp->lastFreeFrag != BF_FRAG_EOF) {
            /*
             * the group's free list is not empty so update the last frag's
             * header to point to the new last frag.
             */

            slotsPgT *fragPgp;
            fragHdrT *fragHdrp;
            uint32T fragPg, fragSlot;
            rbfPgRefHT fragPgRef;

            fragPg   = FRAG2PG( grpHdrp->lastFreeFrag );
            fragSlot = FRAG2SLOT(  grpHdrp->lastFreeFrag );

            if (fragPg != grpPg) {
                sts = rbf_pinpg( &fragPgRef,
                                 (void*)&fragPgp,
                                 setp->fragBfAp,
                                 fragPg,
                                 BS_SEQ_AHEAD,
                                 ftxH );
                if (sts != EOK) {
                    printf("bs_frag_dealloc: rbf_pinpg failed(2), return code = %d", sts );
                    goto abandoned_frag;
                }
            } else {
                fragPgRef = grpPgRef;
                fragPgp   = grpPgp;
            }

            fragHdrp = (fragHdrT *)&fragPgp->slots[ fragSlot ];
            rbf_pin_record( fragPgRef, fragHdrp, sizeof( fragHdrT ) );
            fragHdrp->nextFreeFrag = freeFrag;
        }

        /* initialize the free frag's header */

        {
            slotsPgT *ffragPgp;
            fragHdrT *ffragHdrp;
            uint32T ffragPg, ffragSlot;
            rbfPgRefHT ffragPgRef;

            ffragPg   = FRAG2PG( freeFrag );
            ffragSlot = FRAG2SLOT(  freeFrag );

            if (ffragPg != grpPg) {
                sts = rbf_pinpg( &ffragPgRef,
                                 (void*)&ffragPgp,
                                 setp->fragBfAp,
                                 ffragPg,
                                 BS_SEQ_AHEAD,
                                 ftxH );
                if (sts != EOK) {
                    printf("bs_frag_dealloc: rbf_pinpg (3) failed, return code = %d", sts );
                    goto abandoned_frag;
                }
            } else {
                ffragPgRef = grpPgRef;
                ffragPgp   = grpPgp;
            }

            ffragHdrp = (fragHdrT *)&ffragPgp->slots[ ffragSlot ];
            rbf_pin_record( ffragPgRef, ffragHdrp, sizeof( fragHdrT ) );
            ffragHdrp->self         = freeFrag;
            ffragHdrp->fragType     = fragType;
            ffragHdrp->nextFreeFrag = BF_FRAG_EOF;
        }

        rbf_pin_record( grpPgRef, grpHdrp, sizeof( grpHdrT ) );

        if (grpHdrp->lastFreeFrag == BF_FRAG_EOF) {
            /*
             * since this is the first free entry we need to set the group's
             * free list header to point to the deallocated frag.
             */
            grpHdrp->nextFreeFrag = freeFrag;
        }

        grpHdrp->lastFreeFrag = freeFrag;
        grpHdrp->freeFrags++;

    } else {
        /*
         * Frag group has version 1 format.
         */

        uint32T fragSlot;

        fragSlot = freeFrag - grpPg * BF_FRAG_PG_SLOTS;

#ifdef ADVFS_SMP_ASSERT
        if (AdvfsFragDbg > 3) {
            aprintf( "ADVFS-FRAG: dealloc frag %6d, slot %6d, grp %6d, type %d, for fileset 0x%08x.%04x.%04x\n",
                    freeFrag, fragSlot, grpPg, fragType,
                    setp->bfSetId.domainId.tv_sec,
                    setp->bfSetId.domainId.tv_usec,
                    setp->bfSetId.dirTag.num );
        }
#endif

        if ((fragSlot < 1) ||
            (fragSlot >= BF_FRAG_GRP_SLOTS)){
            ADVFS_SAD1( "invalid fragSlot", fragSlot );
        }

        RBF_PIN_FIELD( grpPgRef, grpHdrp->freeList[ fragSlot ] );
        grpHdrp->freeList[ fragSlot ] = grpHdrp->freeList[ BF_FRAG_NEXT_FREE ];

        RBF_PIN_FIELD( grpPgRef, grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] );
        grpHdrp->freeList[ BF_FRAG_NEXT_FREE ] = fragSlot;

        RBF_PIN_FIELD( grpPgRef, grpHdrp->freeFrags );
        grpHdrp->freeFrags++;
    }

    if (grpHdrp->freeFrags == 1) {

        /*
         * This group was previously empty (no free frags).  So we need
         * to put it back in its "free groups list".
         */

        if (lastGrpPg != BF_FRAG_EOG) {
            grpHdrT *lastGrpHdrp;
            rbfPgRefHT lastGrpPgRef;

            /*
             * if the list is not empty then we need to update the
             * entry at the tail of the list to point to the group
             * that is being added to the list (at the tail).
             */

            sts = rbf_pinpg( &lastGrpPgRef,
                             (void *) &lastGrpHdrp,
                             setp->fragBfAp,
                             lastGrpPg,
                             BS_NIL,
                             ftxH );
            if (sts != EOK) {
                printf("bs_frag_dealloc: rbf_pinpg (4) failed, return code = %d", sts );
                goto abandoned_frag;
            }

            RBF_PIN_FIELD( lastGrpPgRef, lastGrpHdrp->nextFreeGrp );
            lastGrpHdrp->nextFreeGrp = grpPg;
        }

        sts = bmtr_get_rec_ptr( setp->dirBfAp,
                                ftxH,
                                BSR_BFS_ATTR,
                                sizeof( *setAttrp ),
                                1 /* pin pg */,
                                (void *) &setAttrp,
                                &pinPgH,
                                NULL );
        if (sts != EOK) {
            printf("bs_frag_dealloc: bmtr_get_rec_ptr failed, return code = %d", sts);
            goto abandoned_frag;
        }

        /* update free list descriptor tail pointer to point to the group */

        RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[ fragType ].lastFreeGrp );
        setAttrp->fragGrps[ fragType ].lastFreeGrp = grpPg;
        setp->fragGrps[ fragType ].lastFreeGrp = grpPg;

        if (lastGrpPg == BF_FRAG_EOG) {
            /* also update the list head pointer if the list was empty */

            RBF_PIN_FIELD( pinPgH, setAttrp->fragGrps[fragType].firstFreeGrp );
            setAttrp->fragGrps[ fragType ].firstFreeGrp = grpPg;
            setp->fragGrps[ fragType ].firstFreeGrp = grpPg;
        }
    }
#ifdef ADVFS_SMP_ASSERT
    if (AdvfsFragDbg > 0) {
        bs_frag_validate( setp );
    }
#endif

done:
    if (fragBfOpen) {
        sts = bs_close(setp->fragBfAp, 0);
        setp->fragBfAp = NULL;
    }

    return;

abandoned_frag:
    
    printf("bs_frag_dealloc failed on fileset = %s, domain = %s\n",
              setp->bfSetName, setp->dmnP->domainName);
    printf("  fragId.type = %d, fragId.frag = %d\n", fragId.type, fragId.frag);
    printf("  frag remains in use, but will never be accessed again.\n");
    printf("  To restore usage of frag you may run fixfdmn\n");

    bzero(&advfsEvent, sizeof(advfs_ev));
    advfsEvent.domain = setp->dmnP->domainName;
    advfsEvent.fileset = setp->bfSetName;
    advfsEvent.frag = fragId.frag;
    advfsEvent.fragtype = fragId.type;
 
    advfs_post_kernel_event(EVENT_FSET_FRAG_ABANDONED, &advfsEvent); 

    goto done;
}

/*
 * bs_frag_validate
 * Note: this is mousetrap code to help uncover frag corruption bugs.
 */
#ifdef ADVFS_SMP_ASSERT

static void
bs_frag_validate(
    bfSetT *setp        /* in */
    )
{
    statusT sts;
    uint32T grpPg, lastFreeGrpPg;
    bfFragT fragType;
    grpHdrT *grpHdrp;
    slotsPgT *grpPgp;
    bfPageRefHT grpPgRef;

    for (fragType = BF_FRAG_1K; fragType < BF_FRAG_MAX; fragType++) {

        grpPg = setp->fragGrps[ fragType ].firstFreeGrp;
        lastFreeGrpPg = BF_FRAG_EOG;
        while (grpPg != BF_FRAG_EOG) {

        sts = bs_refpg( &grpPgRef,
                        (void*)&grpPgp,
                        setp->fragBfAp,
                        grpPg,
                        BS_NIL );

            if (sts != EOK) {
                if (AdvfsFragDbg == 1)
                    domain_panic(setp->dmnP,
                                 "bs_frag_validate: bs_refpg failed" );
                else ADVFS_SAD0( "bs_frag_validate: bs_refpg failed" );

            }

            grpHdrp = (grpHdrT *)grpPgp;
            if ((grpHdrp->version != 0) && (grpHdrp->version != 1)) {
                if (AdvfsFragDbg == 1)
                    domain_panic(setp->dmnP,
                                 "bs_frag_validate: invalid grpHdr version");
                else ADVFS_SAD0( "bs_frag_validate: invalid grpHdr version");
            }

            if (grpHdrp->self != grpPg) {
                if (AdvfsFragDbg == 1)
                    domain_panic(setp->dmnP,
                                 "bs_frag_validate: self != grpPg");
                else ADVFS_SAD2( "bs_frag_validate: self != grpPg",
                                  grpHdrp->self, grpPg );
            }

            if (grpHdrp->fragType != fragType) {
                if (AdvfsFragDbg == 1)
                    domain_panic(setp->dmnP,
                                 "bs_frag_validate: grpHdrp->fragType != fragType");
                else ADVFS_SAD2( "bs_frag_validate: grpHdrp->fragType != fragType",
                                  grpHdrp->fragType, fragType );
            }

            if (grpHdrp->freeFrags == 0) {
                if (AdvfsFragDbg == 1)
                    domain_panic(setp->dmnP,
                                 "bs_frag_validate: freeFrags = 0");
                else ADVFS_SAD0( "bs_frag_validate: freeFrags = 0" );
            }

            lastFreeGrpPg = grpPg;
            grpPg = grpHdrp->nextFreeGrp;
            (void) bs_derefpg( grpPgRef, BS_CACHE_IT );
        }

        if (setp->fragGrps[ fragType ].lastFreeGrp != lastFreeGrpPg) {
            if (AdvfsFragDbg == 1)
                domain_panic(setp->dmnP,
                             "bs_frag_validate: lastFreeGrp mismatch");
            else ADVFS_SAD0( "bs_frag_validate: lastFreeGrp mismatch");
        }
    }
}

#endif


/****************************************************************************
 * bf set mgt
 ****************************************************************************/

static void
bfs_close(
    bfSetT *bfSetp,     /* in - pointer to open bitfile-set desc */
    ftxHT   ftxH	/* in - transaction handle */
    );

#define NULL_ENT -1

/*
 * There is no more freelist, but maintain the stats anyway.  Negative
 * values on the free list are normal.  If the freeCnt and the
 * allocCnt are matched (eg. freeCnt = -4 and allocCnt = 4), then
 * there are no free descriptors in the hash table.  If they are not
 * matched (eg. freeCnt = -2 and allocCnt = 4), then this would
 * indicate that there are free descriptors hanging around in the
 * hash table (eg. 2 in this example).
 */
struct bfsFreeListStats {
    int freeCnt;        /* allocation decrements freeCnt - negative is OK */
    int allocCnt;       /* descriptors currently allocated. */
    int descAllocated;
    int descFreed;
} BfsFreeListStats = {0, 0, 0, 0};

static mutexT LookupMutex;        /* protects the insertion/deletion of desc */

/* This is the dynamic hashtable for BfSet access structures */
void * BfSetHashTbl;
struct lockinfo *ADVBfSetHashChainLock_lockinfo;


opxT bs_bfs_create_undo;
void
bs_bfs_create_undo(
    ftxHT ftxH,      /* in - ftx handle */
    int opRecSz,     /* in - size of opx record */
    void* opRec      /* in - ptr to opx record */
    )
{
   /*
    * There doesn't appear to be anything to do.  The bitfile will
    * be removed by the bf_create undo opx and it is not necessary to
    * erase the fs context record in the mcell.
    */
}

/*
 * bs_fragbf_open
 *
 * Opens the frag bitfile.
 */

statusT
bs_fragbf_open(
    bfSetT *bfSetp      /* in */
    )
{
    statusT sts;
    struct vnode *nullvp = NULL;

    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    FRAG_LOCK_WRITE( &bfSetp->fragLock )

    sts = bs_access(&bfSetp->fragBfAp, bfSetp->fragBfTag, bfSetp,
                    FtxNilFtxH, 0, NULLMT, &nullvp );
    if (sts != EOK) {
        FRAG_UNLOCK( &bfSetp->fragLock );
        return sts;
    }

    FRAG_UNLOCK( &bfSetp->fragLock );

    return EOK;
}

/*
 * bs_fragbf_close
 *
 * Closes the frag bitfile.
 */

statusT
bs_fragbf_close(
    bfSetT *bfSetp      /* in */
    )
{
    statusT sts;

    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    FRAG_LOCK_WRITE( &bfSetp->fragLock )

    sts = bs_close(bfSetp->fragBfAp, 0);
    bfSetp->fragBfAp = NULL;

    FRAG_UNLOCK( &bfSetp->fragLock );

    return EOK;
}


/*
 * bs_frag_mark_group_header_as_bad
 *
 * This function updates a specified frag file group header on disk.
 * It is assumed that this update is  not critical so if the fileset is 
 * not active or if any error occurs, the function exits silently.
 */

void
bs_frag_mark_group_header_as_bad(bfSetIdT bfSetId, uint32T groupHdrPage)
{
    bfSetT *bfSetp;
    grpHdrT *grpHdrp;
    rbfPgRefHT grpPgRef;
    ftxHT ftxH;
    bfAccessT *fragBfap;
    struct vnode *nullvp = NULL;

    /*
     * Open the fileset and the fileset's frag bitfile.  This is
     * necessary in case the fileset is unmounted while we're working
     * on it; we can't have it disappear on us in the middle.
     */

    if (rbf_bfs_access(&bfSetp, bfSetId, FtxNilFtxH)) {
        goto done;
    }

    if (!BFSET_VALID(bfSetp)) {
        goto close_bfs;
    }

    if (bs_access(&fragBfap, bfSetp->fragBfTag, bfSetp, FtxNilFtxH, 
                     0, NULLMT, &nullvp)) {
        goto close_bfs;
    }

    if (FTX_START_N(FTA_FRAG_UPDATE_GROUP_HEADER,
                    &ftxH, FtxNilFtxH, bfSetp->dmnP, 0)) {
        goto close_fragbf;
    }

    /*
     * Pin the desired group header page.
     */

    if (rbf_pinpg(&grpPgRef,
                  (void*)&grpHdrp,
                  fragBfap,
                  groupHdrPage,
                  BS_NIL,
                  ftxH)) {
        ftx_fail(ftxH);
        goto close_fragbf;
    }

    /*
     * Mark the group as bad.
     */
    RBF_PIN_FIELD(grpPgRef, grpHdrp->lastFreeFrag);
    grpHdrp->lastFreeFrag = BF_FRAG_BAD_GROUP;

    ftx_done_n(ftxH, FTA_FRAG_UPDATE_GROUP_HEADER);

close_fragbf:
    bs_close(fragBfap, 0);

close_bfs:
    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

done:
}

/*
 * bs_bfs_init
 *
 * Initializes bitfile-set global structures.
 */

void
bs_bfs_init(
    void
    )
{
    extern task_t first_task;
    thread_t fragBfThreadH;
    statusT sts;
    int e;
    void unlink_clone_undo( ftxHT ftxH, int opRecSz, void* opRec );
    void del_list_remove_undo( ftxHT ftxH, int opRecSz, void* opRec );

    mutex_init3( &LookupMutex, 0, "BFS LookupMutex", ADVLookupMutex_lockinfo );

    sts = ftx_register_agent( FTA_BS_BFS_CREATE_V1, &bs_bfs_create_undo, NIL );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: ftx register agent failed 1", sts );
    }

    sts = ftx_register_agent( FTA_BS_BFS_UNLINK_CLONE_V1,
                              &unlink_clone_undo, NIL );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: ftx register agent failed 2", sts );
    }

    sts = ftx_register_agent( FTA_BS_BFS_DEL_LIST_REMOVE_V1,
                              &del_list_remove_undo,
                              NIL );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: ftx register agent failed 3", sts );
    }

    /*
     * Initialize the BfSetHashTbl
     */
    BfSetHashTbl = dyn_hashtable_init(  BFSET_HASH_INITIAL_SIZE,
                                        BFSET_HASH_CHAIN_LENGTH,
                                        BFSET_HASH_ELEMENTS_TO_BUCKETS,
                                        BFSET_HASH_USECS_BETWEEN_SPLITS,
                                        offsetof(bfSetT,hashlinks),
                                        ADVBfSetHashChainLock_lockinfo,
                                        NULL);
    if (BfSetHashTbl == NULL) {
        ADVFS_SAD0("bs_bfs_init: can't get space for hash table");
    }

    sts = msgq_create( &FragBfMsgQH,              /* returned handle to queue */
                       MSFS_INITIAL_FRAG_MSGQ_ENTRIES,/* starting msgs in que */
                       sizeof( fragBfMsgT ),      /* max size of each message */
                       TRUE,                  /* ok to increase msg q size */
                       current_rad_id()           /* RAD to create on */
                     );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_init: can't create msq queue", sts );
    }

    fragBfThreadH = kernel_thread( first_task, bs_fragbf_thread );
    if (fragBfThreadH == THREAD_NULL) {
        ADVFS_SAD0( "bs_bfs_init: can't create fragbf thread" );
    }
}

/*
 * bfs_lookup
 *
 * Return a pointer to the requested bitfile-set's descriptor.
 * Returns NULL if the bitfile-set was not found.
 *
 * SYNCHRONIZATION NOTES
 *
 * Assumes the bitfile-set table mutex is locked by the caller.
 *
 * The bitfile-set table lookup mutex (LookupMutex) is used to synch
 * with other threads that might be inserting (bfs_alloc()) a
 * bitfile set descriptor into the table or removing (bfs_invalidate())
 * a descriptor from the table.
 *
 * Most bitfile-set routines use the bitfile-set table
 * lock (BfSetTblLock) to synchronize there actions.  However,
 * bs_bfs_lookup_desc() can't use the lock because it is called
 * by external modules to do lookups in the table and it can
 * cause deadlocks if this routine uses the lock instead of the lookup mutex
 * during the lookup.  So, the lookup mutex is used simply so that
 * bs_bfs_lookup_desc() can be called to do a lookup and not cause
 * a deadlock.  Note that this deadlock condition is a result
 * of porting MegaSafe to DEC OSF which now has unifies
 * file system buffer caches with VM and as a result it ties vnodes
 * with memory objects and bitfile access structures.  It is the
 * need to reclaim a vnode that gets us into trouble.
 *
 * The deadlock can happen if a bfs_access() tries to open the
 * tag dir and the system is out of vnodes so msfs_reclaim() is
 * called, it calls bs_bfs_lookup_desc() to translate a set id to a
 * set descriptor pointer.  When bfs_access() is called the bf-set table
 * lock is held.  If bs_bfs_lookup_desc() also grabbed the lock we have
 * a deadlock.  So the quick solution is to use only the lookup mutex
 * for lookups.  The mutex never needs to be held during any
 * subroutine calls except bfs_lookup() so there is no danger
 * of a deadlock.
 *
 * RACE CONDITIONS
 *
 * Any routine that relies only on the lookup mutex must be able
 * to handle the situation where as soon as the caller releases
 * the lookup mutex that some other thread can add/remove the
 * bitfile set to/from the table.  All routines in this module
 * use the bf-set table lock for synchronization.  Only callers
 * of bs_bfs_lookup_desc() must be able to handle this case.
 */

static bfSetT *
bfs_lookup(
    bfSetIdT bfSetId    /* in - bitfile set's ID */
    )
{
    bfSetT *bfSetp_return = NULL;
    bfSetT *bfSetp;
    bfSetT *bfSetp_start;
    ulong key;

    MS_SMP_ASSERT(SLOCK_HOLDER(&LookupMutex.mutex));

    /*
     * calculate the hash key using the bitfile-set ID as input to the hash
     * algorithm.
     */
    key = BFSET_GET_HASH_KEY( bfSetId );

    /*
     * Lookup up the bitfile-set ID in the dynamic hash table
     */
    bfSetp_start = BFSET_HASH_LOCK( key, NULL );

    if ( BFSET_VALID(bfSetp_start) ) {
        /*
         * We found a bucket with something in it.  Check each entry
         * chained to this bucket.
         */
        bfSetp = bfSetp_start;
        do {
            if ( BS_BFS_EQL(bfSetp->bfSetId, bfSetId ) ) {
                /*
                 * Bitfile-set was found; return a pointer to it's descriptor.
                 */
                bfSetp_return = bfSetp;
                break;
            }
            bfSetp = bfSetp->hashlinks.dh_links.dh_next;
        } while ( bfSetp != bfSetp_start );
    }

    /*
     * Unlock the bucket.
     */
    BFSET_HASH_UNLOCK( key );

    /*
     * If the fileset ID was not found, then NULL will be returned
     */
    return bfSetp_return;
}

/*
 * bs_bfs_flush
 *
 * Flush and invalidate all the dirty buffers associated with a fileset.
 */

static void
bs_bfs_flush (bfSetT *bfSetp  /* in - fileset to be flushed */)
{
    bfAccessT  *currbfap,                /* File currently being flushed */
               *nextbfap;                /* Next file to flush */
    lkStatesT  accState;                 /* State of current bfap */
    int        dirty;

    /*
     * Walk down the chain of access structures for the fileset,
     * flushing each file synchronously.
     */

    start:

    mutex_lock(&bfSetp->accessChainLock);

    currbfap = bfSetp->accessFwd;
    while (currbfap != (bfAccessT *)(&bfSetp->accessFwd)) {

        MS_SMP_ASSERT(currbfap->bfSetp == bfSetp);

        mutex_lock(&currbfap->bfaLock);

        /*
         * If the access structure state is invalid or
         * if it is on the access structure free list,
         * there will be no buffers to flush.  Skip it.
         * If the access structure is being recycled,
         * it is going to be removed from this fileset
         * list, so skip it.  Ditto for access structures
         * that are being deallocated.
         * 
         * The exception is for invalid access structures
         * that are also BSRA_DELETING.  Such structures exist
         * because an image redo was done at recovery time, 
         * but the delete transaction was redone successfully.
         */
        accState = lk_get_state(currbfap->stateLk);
        if (((accState == ACC_INVALID) &&
             (currbfap->bfState != BSRA_DELETING)) ||
            (accState == ACC_RECYCLE)              ||
            (accState == ACC_DEALLOC)              ||
            (currbfap->onFreeList == 1)) {
            mutex_unlock(&currbfap->bfaLock);
            currbfap = currbfap->setFwd;
            continue;
        }

        /*
         * If the access structure is on the access structure
         * free list, remove it from that list.  That is because
         * we are about to bump its refCnt field and later call
         * DEC_REFCNT() to compensate.  But the call to
         * DEC_REFCNT() will attempt to put the access structure
         * onto the free list if its refCnt is zero.  Therefore,
         * it better not be on the free list already when that happens.
         */
        RM_ACC_LIST_COND(currbfap);

        /*
         * Bump the reference count in the access structure.
         * This will prevent another thread from deallocating
         * or recycling this access structure while we are
         * in the call to bfflush() below and we have unlocked
         * the access structure.
         */
        currbfap->refCnt++;

        /*
         * We are ready to flush this access structure.  First,
         * though, we need to release any simple locks held.
         */
        mutex_unlock(&currbfap->bfaLock);
        mutex_unlock(&bfSetp->accessChainLock);

        /*
         * Flush and invalidate the pages of the file.
         */
        msfs_flush_and_invalidate(currbfap, 
                                  INVALIDATE_ALL | INVALIDATE_QUOTA_FILES);

        /*
         * Reseize the access structure and chain locks.
         */
        mutex_lock(&bfSetp->accessChainLock);
        mutex_lock(&currbfap->bfaLock);

        /*
         * Get a pointer to the next access structure in the fileset.
         * This may have changed during the time that we had no
         * lock on the chain.  However, since new access structures
         * are always added to the head of the chain, we are sure
         * that the next access structure we look at will get us
         * closer to the end of the chain.  Also, the fact that
         * we had the refCnt bumped on the current access structure
         * during the flush guarantees us that the current access
         * structure is still associated with the fileset that is
         * being flushed and hasn't been recycled to another fileset
         * or deallocated.
         */
        nextbfap = currbfap->setFwd;

        /*
         * Decrement the refCnt on the current access structure.  This
         * may place it onto the access structure free list.  Then
         * unlock it.  Our place in the chain is saved in nextbfap.
         */
        DEC_REFCNT(currbfap);
        mutex_unlock(&currbfap->bfaLock);
        currbfap = nextbfap;
    }
    mutex_unlock(&bfSetp->accessChainLock);
}

/*
 * bfs_alloc
 *
 * Creates a new bitfile-set descriptor and adds it to the global
 * dynamic hash table (BfSetHashTbl).
 *
 * Assumes the bitfile-set table is locked by the caller.
 *
 * Returns EOK, ENO_MORE_MEMORY or E_TOO_MANY_BF_SETS;
 */

static statusT
bfs_alloc(
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    domainT *dmnP,         /* in - BF-set's domain's struct pointer */
    bfSetT **retBfSetp     /* out - ptr to BF-set's descriptor */
    )
{
    bfSetT *bfSetp;
    statusT sts;

    mutex_lock( &LookupMutex ); /* bfs_lookup synchronization */
    bfSetp = bfs_lookup( bfSetId );

    if ( BFSET_VALID(bfSetp) ) {

        /*
         * Set the dmnP to the current domain
         */
        MS_SMP_ASSERT( bfSetp->dmnP == dmnP );
        
        BFSET_TRACE( bfSetp, 0 );

        /*
         * This set's descriptor is already cached in the table so
         * reuse the desc.
         */
        *retBfSetp = bfSetp;

        /*
         * Update the stats.
         */
        BfsFreeListStats.freeCnt--;

        sts = EOK;
        mutex_unlock( &LookupMutex );
        goto EXIT_BFS_ALLOC;
    }

    /*
     * Update the stats.
     */

    BfsFreeListStats.freeCnt--;
    BfsFreeListStats.allocCnt++;
    BfsFreeListStats.descAllocated++;

    mutex_unlock( &LookupMutex );

    /*
     * Allocate bitfile set descriptor
     */

    bfSetp = (bfSetT *) ms_malloc( sizeof( bfSetT ) );
    if (bfSetp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto EXIT_BFS_ALLOC;
    }

    /*
     * "nilBfSet" is all zeros so only non-zero fields need
     * to be initialized.
     */
    *bfSetp            = nilBfSet;
    bfSetp->bfSetId    = bfSetId;
    bfSetp->dirTag     = bfSetId.dirTag;
    bfSetp->dmnP       = dmnP;
    bfSetp->state      = BFS_INVALID;
    bfSetp->bfSetMagic = SETMAGIC;
    bfSetp->accessFwd  =
    bfSetp->accessBwd  = (bfAccessT *)(&bfSetp->accessFwd);

    mutex_init3(&bfSetp->setMutex, 0, "bfSetT.setMutex",
                ADVbfSetT_setMutex_lockinfo );
    mutex_init3(&bfSetp->accessChainLock, 0, "bfSetT.accessChainLock",
                ADVbfSetT_accessChainLock_lockinfo );
    ftx_lock_init(&bfSetp->dirLock, &bfSetp->setMutex, ADVbfSetT_dirLock_info );
    ftx_lock_init(&bfSetp->fragLock, &bfSetp->setMutex, ADVbfSetT_fragLock_info );
    mutex_init3(&bfSetp->cloneDelStateMutex, 0, "bfSetT.cloneDelStateMutex", 
                ADVbfSetT_cloneDelStateMutex_lockinfo);
    mutex_lock(&bfSetp->cloneDelStateMutex);
    lk_init(&bfSetp->cloneDelState, &bfSetp->cloneDelStateMutex, 
            LKT_STATE, 0, LKU_CLONE_DEL);
    (void)lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_NORMAL);
    bfSetp->xferThreads = 0;
    mutex_unlock(&bfSetp->cloneDelStateMutex);

    mutex_init3(&bfSetp->bfSetMutex, 0, "bfSetT.bfSetMutex",
                ADVbfSetT_bfSetMutex_lockinfo);
               

    /*
     * return the bitfile-set structure pointer
     */
    *retBfSetp = bfSetp;

    /*
     * Link the bitfile-set descriptor into the domain's list of
     * bitfile-sets.
     */
    mutex_lock( &bfSetp->dmnP->mutex );
    BFSET_DMN_INSQ(bfSetp->dmnP, &bfSetp->dmnP->bfSetHead, &bfSetp->bfSetList);
    mutex_unlock( &bfSetp->dmnP->mutex );

    /*
     * Place the new bitfile-set descriptor in the hash table.
     */
    bfSetp->hashlinks.dh_key = BFSET_GET_HASH_KEY( bfSetId );
    BFSET_HASH_INSERT( bfSetp, TRUE );

    sts = EOK;

EXIT_BFS_ALLOC:
    return sts;
}

/*
 * bfs_dealloc
 *
 * Puts the bitfile set's desc on the LRU free list.
 *
 * Assumes the bitfile-set table is locked by the caller.
 */

void
bfs_dealloc(
    bfSetT *bfSetp, /* in - bitfile set descriptor pointer */
    int deallocate  /* in - flag indicates to remove the set from existence */
    )
{

    statusT sts;

    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt == 0 );
    MS_SMP_ASSERT(lock_holder(&bfSetp->dmnP->BfSetTblLock.lock));

    /*
     * There is no more freelist, but we are keeping the statistics for now.
     */
    mutex_lock( &LookupMutex );
    BfsFreeListStats.freeCnt++;
    mutex_unlock( &LookupMutex );

    /*
     * Invalidate all buffers and access structs associated with
     * this set and deallocate the set's desc.
     */

    if (deallocate) {

        /*
         * Flush and invalidate all pages for all files in the fileset.
         */
        bs_bfs_flush(bfSetp);

        /*
         * Invalidate all access structs associated with this set descriptor.
         */
        access_invalidate( bfSetp );

        /*
         * Let's make sure it did what it's supposed to do.
         */
        MS_SMP_ASSERT(bfSetp->accessFwd ==  (bfAccessT *)(&bfSetp->accessFwd));

        /*
         * Remove this bitfile-set from the list in the domain structure.
         */
        mutex_lock( &bfSetp->dmnP->mutex );
        BFSET_DMN_REMQ( bfSetp->dmnP, &bfSetp->bfSetList );
        mutex_unlock( &bfSetp->dmnP->mutex );

        /*
         * Remove the bfSetT from the dyn_hashtable.
         */
        BFSET_HASH_REMOVE( bfSetp, TRUE );

        mutex_lock( &LookupMutex ); /* bfs_lookup synchronization */
        BfsFreeListStats.descFreed++;
        BfsFreeListStats.allocCnt--;
        mutex_unlock( &LookupMutex );

        mutex_destroy( &bfSetp->setMutex );

        mutex_destroy(&bfSetp->cloneDelStateMutex);
        lk_destroy(&bfSetp->cloneDelState);

        mutex_destroy( &bfSetp->accessChainLock );

        mutex_destroy( &bfSetp->bfSetMutex );

        bfSetp->bfSetMagic |= MAGIC_DEALLOC;
        ms_free( bfSetp );
    }
    return;
}

/*
 * bs_bfs_lookup_desc
 *
 * Return a pointer to the requested bitfile-set descriptor.
 * Returns 0 if the set is not found.
 *
 * RACE CONDITION
 *
 * See bfs_lookup().
 */

bfSetT *
bs_bfs_lookup_desc(
    bfSetIdT bfSetId    /* in - bitfile set's ID */
    )
{
    bfSetT *bfSetp;

    mutex_lock( &LookupMutex ); /* bfs_lookup synchronization */
    bfSetp = bfs_lookup( bfSetId );
    mutex_unlock( &LookupMutex );

    return bfSetp;
}

/*
 * bfs_create
 *
 * Creates a bitfile-set in the specified domain.  This entails creating
 * the bitfile-set's tag directory and initializing the bitfile-set's
 * client "FS context" area (kept in the tag directories primary mcell).
 *
 * Assumes the bitfile-set table is locked by the caller.
 *
 * Returns EOK, EBAD_DOMAIN_POINTER, and errors from bs_create().
 */

static statusT
bfs_create(
    domainT *dmnP,          /* in - domain pointer */
    serviceClassT reqServ,  /* in - required service class */
    serviceClassT optServ,  /* in - optional service class */
    char *setName,          /* in - the new set's name */
    uint32T fsetOptions,    /* in - fileset options */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    bfParamsT *tagDirParamsp = NULL;
    bfTagT dirTag;
    statusT sts;
    bfMCIdT tagDirMCId;
    int ftxStarted = FALSE, tagDirOpen = FALSE;
    bsBfSetAttrT *bfsAttrp = NULL;
    bsQuotaAttrT *quotaAttrp = NULL;
    ftxHT ftxH;
    vdIndexT tagDirDiskIdx;
    bfAccessT *dirbfap;
    bfSetParamsT *setParamsp = NULL;
    int i;
    bsrRsvd17T bsrRsvd17;
    struct vnode *nullvp = NULL;

    /*
     * Check validity of domain pointer.
     */

    if (dmnP == NULL) {
        RAISE_EXCEPTION( EBAD_DOMAIN_POINTER );
    }

    /*
     * Minimize stack usage.
     * TODO:  We could do one ms_malloc and manage it ourselves.
     */
    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    if (setParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    bfsAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));
    if (bfsAttrp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    tagDirParamsp = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));
    if (tagDirParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    quotaAttrp = (bsQuotaAttrT *) ms_malloc( sizeof( bsQuotaAttrT ));
    if (quotaAttrp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    sts = bs_bfs_find_set( setName, dmnP, 0, setParamsp );
    if (sts == EOK) {
        RAISE_EXCEPTION( E_DUPLICATE_SET );
    }

    sts = FTX_START_N( FTA_BFS_CREATE, &ftxH, parentFtxH, dmnP, 1 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    ftxStarted = TRUE;

    tagDirParamsp->pageSize = ADVFS_PGSZ_IN_BLKS;
    tagDirParamsp->cl.dataSafety = BFD_FTX_AGENT;
    tagDirParamsp->cl.reqServices = reqServ;
    tagDirParamsp->cl.optServices = optServ;

    sts = rbf_create( &dirTag, dmnP->bfSetDirp, tagDirParamsp, ftxH, 0 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    bfSetId->dirTag = dirTag;
    bfSetId->domainId = dmnP->domainId;

    sts = tagdir_lookup( dmnP->bfSetDirp,
                         &dirTag,
                         &tagDirMCId,
                         &tagDirDiskIdx
                         );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_create: can't find Tag Dir tag", sts );
    }

    /*
     * Initialize the BF-set attributes in the BF-set's primary mcell.
     */

    sts = bs_access( &dirbfap, dirTag, dmnP->bfSetDirp, ftxH, 0, 
                     NULLMT, &nullvp );
    if (sts != EOK) {
        if (dmnP->dmn_panic) {
            RAISE_EXCEPTION( E_DOMAIN_PANIC );
        } else {
            ADVFS_SAD1( "bfs_create: can't open tag dir", sts );
        }

    }

    tagDirOpen = TRUE;

    bfsAttrp->bfSetId = *bfSetId;
    bfsAttrp->cloneId = 0;
    bfsAttrp->cloneCnt = 0;
    bfsAttrp->state = BFS_ODS_VALID;
    bfsAttrp->mode = S_IRWXU | S_IRGRP| S_IROTH;
    if (fsetOptions & BFS_OD_NOFRAG) {
        bfsAttrp->flags |= BFS_OD_NOFRAG;
    }
    bfsAttrp->uid = u.u_nd.ni_cred->cr_uid;
    bfsAttrp->gid = u.u_nd.ni_cred->cr_gid;
    bcopy( setName, bfsAttrp->setName, BS_SET_NAME_SZ );
    for (i = 0; i < BF_FRAG_MAX; i++) {
        bfsAttrp->fragGrps[i].firstFreeGrp = BF_FRAG_EOG;
        bfsAttrp->fragGrps[i].lastFreeGrp = BF_FRAG_EOG;
    }

    sts = bmtr_put_rec( dirbfap,
                        BSR_BFS_ATTR,
                        bfsAttrp,
                        sizeof( bsBfSetAttrT ),
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    /* put a zero-filled quota attributes record in mcell */

    sts = bmtr_put_rec( dirbfap,
                        BSR_BFS_QUOTA_ATTR,
                        quotaAttrp,
                        sizeof( bsQuotaAttrT ),
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    /*
     *  Prior to V4.0, AdvFS supported a hierarchical storage manager.
     *  If we now find that the HSM product is enabled for a fileset
     *  then we disallow the mount because shelving is no longer supported.
     *  So we continue to write the following default record indicating
     *  that HSM is not enabled for this new fileset.
     */

    bsrRsvd17 = DefbsrRsvd17;

    sts = bmtr_put_rec( dirbfap,
                        BSR_RSVD17,
                        &bsrRsvd17,
                        sizeof( bsrRsvd17 ),
                        ftxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }
    
    bfflush(dirbfap, 0, 0, FLUSH_IMMEDIATE);
    bs_close(dirbfap, 0);

    ftx_done_n( ftxH, FTA_BFS_CREATE );

    ms_free( quotaAttrp );
    ms_free( tagDirParamsp );
    ms_free( bfsAttrp );
    ms_free( setParamsp );

    return EOK;

HANDLE_EXCEPTION:

    if (tagDirOpen) {
        bs_close(dirbfap, 0);
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (quotaAttrp != NULL) {
        ms_free( quotaAttrp );
    }
    if (tagDirParamsp != NULL) {
        ms_free( tagDirParamsp );
    }
    if (bfsAttrp != NULL) {
        ms_free( bfsAttrp );
    }
    if (setParamsp != NULL) {
        ms_free( setParamsp );
    }

    return sts;
}

/*
 * rbf_bfs_create
 *
 * Higher level interface to bfs_create().  This routine calls
 * bfs_create() to create the bitfile set and then it creates
 * the bitfile set's frag bitfile.
 */

statusT
rbf_bfs_create(
    domainT *dmnP,          /* in - domain pointer */
    serviceClassT reqServ,  /* in - required service class */
    serviceClassT optServ,  /* in - optional service class */
    char *setName,          /* in - the new set's name */
    uint32T fsetOptions,    /* in - fileset options */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    bfParamsT fragBfParams = bsNilBfParams;
    bfSetT *bfSetp;
    bfTagT fragBfTag;
    bsBfSetAttrT *setAttrp;
    statusT sts;
    int ftxStarted = FALSE, lkLocked = FALSE;
    ftxHT ftxH;
    rbfPgRefHT pinPgH;

    sts = FTX_START_N( FTA_BFS_CREATE_2, &ftxH, parentFtxH, dmnP, 1 );
    if ( sts != EOK ) {
        RAISE_EXCEPTION( sts );
    }

    ftxStarted = TRUE;

    BFSETTBL_LOCK_WRITE( dmnP )
    lkLocked = TRUE;

    sts = bfs_create( dmnP,
                      reqServ,
                      optServ,
                      setName,
                      fsetOptions,
                      ftxH,
                      bfSetId );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bfs_access( &bfSetp, *bfSetId, BFS_OP_DEF, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    fragBfParams.pageSize = ADVFS_PGSZ_IN_BLKS;
    fragBfParams.cl.dataSafety = BFD_FTX_AGENT;

    sts = rbf_create( &fragBfTag,
                      bfSetp,
                      &fragBfParams,
                      ftxH, 0);
    if (sts != EOK) {
        bfs_close( bfSetp, ftxH );
        RAISE_EXCEPTION( sts );
    }

    sts = bmtr_get_rec_ptr( bfSetp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &pinPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(dmnP, "rbf_bfs_create: bmtr_get_rec_ptr failed, return code = %d.", sts );
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    RBF_PIN_FIELD( pinPgH, setAttrp->fragBfTag );
    setAttrp->fragBfTag = fragBfTag;
    bfSetp->fragBfTag = fragBfTag;

    bfs_close( bfSetp, ftxH );
    BFSETTBL_UNLOCK( dmnP )

    ftx_done_n( ftxH, FTA_BFS_CREATE_2 );

    return EOK;

HANDLE_EXCEPTION:

    /*
     * a transaction failure may release BfSetTblLock
     */
    if (lkLocked && lock_holder(&dmnP->BfSetTblLock.lock)) {
        BFSETTBL_UNLOCK( dmnP )
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    return sts;
}

/*
 * bfs_close
 *
 * This routine is called to close a bitfile-set.
 *
 * Assumes the bitfile-set table is locked by the caller.
 */

static void
bfs_close(
    bfSetT *bfSetp,     /* in - bitfile-set desc pointer */
    ftxHT   ftxH	/* in - transaction handle */
    )
{
    domainT *dmnP = bfSetp->dmnP;
        
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    bfSetp->fsRefCnt--;

    if (bfSetp->fsRefCnt == 0) { 
        /*
         * Flush all remaining dirty stats.
         */
        if (dmnP->state == BFD_ACTIVATED) 
            bfs_flush_dirty_stats(bfSetp, ftxH);
        (void) bs_close(bfSetp->dirBfAp, 0);
        bfSetp->dirBfAp = NULL;
        
        /*
         * for active domains, remove all in-memory traces of set structure, 
         * flushes any remaining dirty buffers.  For recovering domains, 
         * this is delayed until bs_bfdmn_flush.
         */
        if((dmnP->state == BFD_RECOVER_REDO) ||
           (dmnP->state == BFD_RECOVER_FTX) ||
           (dmnP->state == BFD_RECOVER_CONTINUATIONS))
            bfs_dealloc( bfSetp, FALSE );
        else
            bfs_dealloc( bfSetp, TRUE );
             
    } else if ( bfSetp->fsRefCnt < 0 ) {
        ADVFS_SAD0("bfs_close: bitfile set ref cnt went negative");
    }

    /* Close the bitfile domain */

    bs_domain_close( dmnP );
}

/*
 * bs_bfs_close
 *
 * This routine is called to close the specified bitfile-set.
 *
 * All sets in the clone linked list are also closed.  If the
 * specified bitfile set is a clone then it will be closed twice.
 */

void
bs_bfs_close(
    bfSetT *bfSetp,	/* in - pointer to open bitfile-set */
    ftxHT   ftxH,	/* in - transaction handle */
    uint32T options	/* in - options flags */
    )
{
    bfSetT *setp;
    lkStatesT currentCloneDelState;
    domainT *dmnP = bfSetp->dmnP;
    unLkActionT unLkAction = UNLK_NEITHER;

    /*
     * Here is where we grab BfSetTblLock.  Now we have to call 
     * lock_holder() first to see if we already have this locked 
     * since some undo and rtdn routines now acquire this lock 
     * early to prevent a hierarchy violation.
     */

    if (!lock_holder(&dmnP->BfSetTblLock.lock)) {
        BFSETTBL_LOCK_WRITE( dmnP );
    }
    
    MS_SMP_ASSERT( BFSET_VALID(bfSetp));
    MS_SMP_ASSERT( bfSetp->fsRefCnt != 0 );

    /*
     ** Get a pointer to the original set's descriptor.
     */

    if ( !BFSET_VALID(bfSetp->origSetp) ) {
        /*
         ** The specified set is the original parent set.
         */
        setp = bfSetp->cloneSetp;

    } else {
        /*
         ** The specified set is a clone.
         */
        setp = bfSetp;
    }

    /*
     ** If the specified set is not the original parent set then close
     ** the orig set.
     */

    if ( BFSET_VALID(bfSetp->origSetp) ) {
        bfs_close( bfSetp->origSetp, ftxH );
    }


    /*
     * If xfer_xtnts_to_clone() is calling in, having just tranferred
     * some storage to a clone file, we decrement bfSetp->xferThreads.
     * What we do next depends on the count in bfSetp->xferThreads and the
     * state of bfSetp->cloneDelState:
     *
     * 1.  If xferThreads is greater than zero, there are other
     *     xfer_xtnts_to_clone() threads still working on the clone fileset.
     *     Leave without doing any more work.
     * 2.  If xferThreads is zero and the state is CLONE_DEL_XFER_STG,
     *     we are the last xfer_xtnts_to_clone() thread currently working
     *     on the clone fileset and there is no clone fileset deletion
     *     pending.  Set the state to CLONE_DEL_NORMAL.
     * 3.  If xferThreads is zero and the state is CLONE_DEL_PENDING,
     *     we are the last xfer_xtnts_to_clone() thread currently working
     *     on the clone fileset and there is an rmfset thread waiting
     *     for all xfer_xtnts_to_clone() threads to complete so it can
     *     delete the clone fileset.  Send a signal to that thread since
     *     it can now wake up and continue the fileset deletion.
     */
    if (options & BFS_OP_XFER_XTNTS_TO_CLONE) {
        mutex_lock(&bfSetp->cloneDelStateMutex);

        currentCloneDelState = lk_get_state(bfSetp->cloneDelState); 
        MS_SMP_ASSERT(currentCloneDelState == CLONE_DEL_XFER_STG ||
                      currentCloneDelState == CLONE_DEL_PENDING);
        MS_SMP_ASSERT(bfSetp->xferThreads > 0);

        if (--bfSetp->xferThreads == 0) {
            if (currentCloneDelState == CLONE_DEL_XFER_STG) {
                (void)lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_NORMAL);
            }
            else {
                unLkAction = lk_set_state(&bfSetp->cloneDelState,
                                          CLONE_DEL_DELETING);
                MS_SMP_ASSERT(unLkAction == UNLK_SIGNAL);
                lk_signal(unLkAction, &bfSetp->cloneDelState);
            }
        }

        mutex_unlock(&bfSetp->cloneDelStateMutex);
    }

    bfs_close( bfSetp, ftxH );

    /* Close the clone only after the original, this way we have covered
     * all that needs to be cowed 
     */
    if( BFSET_VALID(setp) ) {
        MS_SMP_ASSERT(setp->cloneId != BS_BFSET_ORIG);
        bfs_close( setp, ftxH );
    }

    /*
     * Unlock the table.
     */

    BFSETTBL_UNLOCK( dmnP )

    return;
}

/*
 * bfs_access
 *
 * Opens a bitfile set.
 *
 * Returns EOK, E_NO_SUCH_BF_SET, EHANDLE_OVF, E_TOO_MANY_BF_SETS or errors
 * from bs_domain_access();
 */

static statusT
bfs_access(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set desc */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    uint32T options,       /* in - options flags */
    ftxHT ftxH             /* in - transaction handle */
    )
{
    domainT *dmnP;
    statusT sts;
    bfSetT *bfSetp;
    bfAccessT *tagDirBfap;
    int i;
    int dmnOpen = FALSE, tagDirOpen = FALSE, newSet = FALSE;
    int tblLocked = FALSE;
    bsBfSetAttrT setAttr;
    struct vnode *nullvp;
    lkStatesT currentCloneDelState;

    /*
     * Open the bitfile-set's domain.
     */
    sts = bs_domain_access( &dmnP, bfSetId.domainId, FALSE );
    if (sts != EOK) { RAISE_EXCEPTION( sts ); }
    dmnOpen = TRUE;

    /*
     * Lock the domain's bitfile-set lock if not already locked.
     */

    if (!lock_holder(&dmnP->BfSetTblLock.lock)) {
        BFSETTBL_LOCK_WRITE( dmnP )
        tblLocked = TRUE;
    }

    if (BS_BFTAG_SEQ( bfSetId.dirTag ) == 0) {
        /* wild card bitfile set id (tag) */
        if (bs_get_current_tag(dmnP->bfSetDirp, &bfSetId.dirTag) != EOK) {
            if (!BS_BFTAG_EQL(bfSetId.dirTag, staticRootTagDirTag)) {
                RAISE_EXCEPTION(ENO_SUCH_TAG);
            }
        }
    }

    /*
     * Get a pointer to the bitfile-set's descriptor.
     */
    mutex_lock( &LookupMutex ); /* bfs_lookup synchronization */
    bfSetp = bfs_lookup( bfSetId );
    mutex_unlock( &LookupMutex );

    if ( !BFSET_VALID(bfSetp) ) {
        /*
         * This bitfile-set is not currently open
         */

        /*
         * Open the bitfile-set's tag directory
         */

        nullvp = NULL;
        sts = bs_access( &tagDirBfap, bfSetId.dirTag, dmnP->bfSetDirp, 
                         ftxH, 0, NULLMT, &nullvp );
        if (sts == ENO_SUCH_TAG) {RAISE_EXCEPTION( E_NO_SUCH_BF_SET );}
        if (sts != EOK) {
           if (sts == EHANDLE_OVF) {
                RAISE_EXCEPTION( sts );
            }
            ADVFS_SAD1( "bfs_access: can't access bitfile-set's tag dir", sts );
        }
        tagDirOpen = TRUE;

        /*
         * Call bfs_alloc() create a descriptor for the bitfile-set.
         */

        sts = bfs_alloc( bfSetId, dmnP, &bfSetp );
        if (sts != EOK) { RAISE_EXCEPTION( sts ); };
        newSet = TRUE;

        bfSetp->dirBfAp = tagDirBfap;

        sts = bmtr_get_rec(tagDirBfap,
                           BSR_BFS_ATTR,
                           (void *) &setAttr,
                           sizeof( setAttr ) );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_access: get orig bf set attr failed", sts );
        }

        strncpy( bfSetp->bfSetName, setAttr.setName, BS_SET_NAME_SZ );
        bfSetp->cloneSetp = NULL;
        bfSetp->origSetp = NULL;
        bfSetp->cloneId = setAttr.cloneId;
        bfSetp->cloneCnt = setAttr.cloneCnt;
        bfSetp->numClones = setAttr.numClones;
        bfSetp->fragBfTag = setAttr.fragBfTag;
        /*
         *  setAttr.flags maps to the low 16 bits of BfSetFlags
         */
        bfSetp->bfSetFlags = (uint32T) setAttr.flags;
        
        bfSetp->freeFragGrps = setAttr.freeFragGrps;
        for (i = 0; i < BF_FRAG_MAX; i++) {
            bfSetp->fragGrps[i].firstFreeGrp = setAttr.fragGrps[i].firstFreeGrp;
            bfSetp->fragGrps[i].lastFreeGrp = setAttr.fragGrps[i].lastFreeGrp;
        }

        if (setAttr.state == BFS_ODS_VALID) {
            bfSetp->state = BFS_READY;

        } else if (setAttr.state == BFS_ODS_DELETED) {
            bfSetp->state = BFS_DELETING;
        }

        if (setAttr.flags & BFS_OD_OUT_OF_SYNC) {
            aprintf( "\n\n" );
            aprintf( "WARNING: advfs clone fileset is out of sync with the original fileset\n" );
            aprintf( "WARNING: clone fileset name = %s\n", setAttr.setName );
            aprintf( "WARNING: do not continue to use this clone fileset\n\n" );
        }

        sts = tagdir_get_info(bfSetp, &bfSetp->tagUnInPg,
                              &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                              &bfSetp->bfCnt );
        if (sts != EOK) {
            domain_panic(bfSetp->dmnP,
                         "bfs_access: can't get tag info");
            RAISE_EXCEPTION( sts );
        }

        if ( dmnP->state != BFD_ACTIVATED ) {
            /*
             * crash recovery may alter the on-disk version of the
             * info we just loaded so we need to reload it next time
             * the set is accessed (it will be accessed again during mount)
             */
            bfSetp->infoLoaded = FALSE;
        } else {
            bfSetp->infoLoaded = TRUE;
        }

        if (options & BFS_OP_XFER_XTNTS_TO_CLONE) 
        {
            mutex_lock(&bfSetp->cloneDelStateMutex);

            MS_SMP_ASSERT(lk_get_state(bfSetp->cloneDelState) == CLONE_DEL_NORMAL);
            MS_SMP_ASSERT(bfSetp->xferThreads == 0);

            (void)lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_XFER_STG);
            bfSetp->xferThreads = 1;

            mutex_unlock(&bfSetp->cloneDelStateMutex);
        }
        
    } else if (bfSetp->fsRefCnt == 0) {
        /*
         * This bitfile-set is not currently open but was open at some time.
         */

        if (options & BFS_OP_XFER_XTNTS_TO_CLONE) {
            mutex_lock(&bfSetp->cloneDelStateMutex);
            currentCloneDelState = lk_get_state(bfSetp->cloneDelState); 

            MS_SMP_ASSERT(currentCloneDelState == CLONE_DEL_NORMAL   ||
                          currentCloneDelState == CLONE_DEL_DELETING);
            
            if (currentCloneDelState == CLONE_DEL_NORMAL) {
                MS_SMP_ASSERT(bfSetp->xferThreads == 0);
                (void)lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_XFER_STG);
                bfSetp->xferThreads = 1;
            }
            else 
            {
                mutex_unlock(&bfSetp->cloneDelStateMutex);
                RAISE_EXCEPTION(E_NO_SUCH_BF_SET);
            }

            mutex_unlock(&bfSetp->cloneDelStateMutex);
        }

        /*
         * Open the bitfile-set's tag directory
         */

        nullvp = NULL;
        sts = bs_access(&tagDirBfap, bfSetId.dirTag, dmnP->bfSetDirp, ftxH,
                        0, NULLMT, &nullvp);
        if (sts == ENO_SUCH_TAG) {RAISE_EXCEPTION( E_NO_SUCH_BF_SET );}
        if (sts != EOK) {
            if (sts == EHANDLE_OVF) {
                RAISE_EXCEPTION( sts );
            }
            ADVFS_SAD1( "bfs_access: can't access bitfile-set's tag dir", sts );
        }

        bfSetp->dirBfAp = tagDirBfap;

        tagDirOpen = TRUE;

        sts = bfs_alloc( bfSetId, dmnP, &bfSetp );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_access: bfs_alloc failed", sts );
        }
    }

    /*
     * If xfer_xtnts_to_clone() is calling in preparation for tranferring
     * some storage to a clone file, what we do next depends on the state 
     * of bfSetp->cloneDelState:
     *
     * 1. If state is CLONE_DEL_NORMAL, set the number of xfer threads to 1
     *    and set state to CLONE_DEL_XFER_STG.  We are currently the only
     *    xfer_xtnts_to_clone() thread working on this fileset.
     * 2. If state is CLONE_DEL_XFER_STG, at least one other thread is also
     *    doing an xfer_xtnts_to_clone() call within this fileset.  Bump
     *    the number of xfer threads and leave the state unchanged.
     * 3. If state is CLONE_DEL_PENDING or CLONE_DEL_DELETING, another thread 
     *    is either removing the clone fileset or is waiting to remove the
     *    clone fileset.  Return to caller as if the fileset were already
     *    gone.
     */
    else if (options & BFS_OP_XFER_XTNTS_TO_CLONE) {
        mutex_lock(&bfSetp->cloneDelStateMutex);
        currentCloneDelState = lk_get_state(bfSetp->cloneDelState); 

        MS_SMP_ASSERT(currentCloneDelState == CLONE_DEL_NORMAL   ||
                      currentCloneDelState == CLONE_DEL_XFER_STG ||
                      currentCloneDelState == CLONE_DEL_PENDING ||
                      currentCloneDelState == CLONE_DEL_DELETING);

        if (currentCloneDelState == CLONE_DEL_NORMAL) {
            MS_SMP_ASSERT(bfSetp->xferThreads == 0);
            (void)lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_XFER_STG);
            bfSetp->xferThreads = 1;
        }
        else if (currentCloneDelState == CLONE_DEL_XFER_STG) {
            MS_SMP_ASSERT(bfSetp->xferThreads > 0);
            bfSetp->xferThreads++;
        }
        else if ((currentCloneDelState == CLONE_DEL_PENDING) ||
                 (currentCloneDelState == CLONE_DEL_DELETING)) {

            MS_SMP_ASSERT((currentCloneDelState != CLONE_DEL_PENDING) ||
                          (lk_waiters(bfSetp->cloneDelState) > 0));

            mutex_unlock(&bfSetp->cloneDelStateMutex);
            RAISE_EXCEPTION(E_NO_SUCH_BF_SET);
        }
        mutex_unlock(&bfSetp->cloneDelStateMutex);
    }
    
    if ((options & BFS_OP_IGNORE_DEL) == 0) {
        /*
         * When the set is deleting, treat as if it is deleted.
         */
        if (bfSetp->state == BFS_DELETING) {
            RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
        }
    }

    if (!bfSetp->infoLoaded && (dmnP->state == BFD_ACTIVATED)) {
        /*
         * When bitfile set is activated for the first time during
         * recovery, the bitfile counts can be wrong.  So reload now.
         */

        sts = tagdir_get_info( bfSetp, &bfSetp->tagUnInPg,
                               &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                               &bfSetp->bfCnt );
        if (sts != EOK) {
            domain_panic(bfSetp->dmnP,
                         "bfs_access: can't get tag info");
            RAISE_EXCEPTION( sts );
        }

        bfSetp->infoLoaded = TRUE;
    }

    bfSetp->fsRefCnt++;

    if (tblLocked) {
        BFSETTBL_UNLOCK( dmnP )
    }
    *retBfSetp = bfSetp;

    return EOK;

HANDLE_EXCEPTION:

    if (newSet) {
        bfs_dealloc( bfSetp, TRUE );
    }

    if (tblLocked) {
        BFSETTBL_UNLOCK( dmnP )
    }

    if (tagDirOpen) {
        (void) bs_close(tagDirBfap, 0);
    }

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    return sts;
}

/*
 * bfs_open
 *
 * This routine must be called to acquire a bitfile-set
 * access handle prior to accessing or creating bitfiles in
 * a bitfile set.
 *
 * This routine opens the specified bitfile set and all other
 * sets that it depends upon and all sets that depend on it.
 * In other words, whenever a set is opened, all sets in the
 * linked list of clones and the original parent set are also
 * opened.  If the specified set is a clone it will be opened
 * twice by this routine.
 *
 * Returns EOK, E_NO_SUCH_BF_SET, E_TOO_MANY_BF_SETS or errors
 * from bs_domain_access();
 */

static statusT
bfs_open(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    uint32T options,       /* in - options flags */
    ftxHT ftxH             /* in - transacton handle */
    )
{
    int tblLocked = FALSE;
    statusT sts;
    bfSetT *bfSetp, *setp, *prevSetp;
    bfSetIdT setId;
    bfSetT *origSetp;
    bsBfSetAttrT bfSetAttr;
    bfTagT setDirTag;

    /*
     *  Open the specified bitfile set.
     */

    sts = bfs_access( &bfSetp, bfSetId, options, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if (!lock_holder(&bfSetp->dmnP->BfSetTblLock.lock)) {
        BFSETTBL_LOCK_WRITE( bfSetp->dmnP )
        tblLocked = TRUE;
    }

    /*
     ** The following section is valid only for 'normal' bitfile sets.
     ** The 'root' bitfile set of each domain can't be cloned so this
     ** code does not apply; in fact, it will not work!!
     */

    if (BS_BFTAG_REG(bfSetp->dirTag)) {
        /*
         ** Starting with the root, open all bitfile sets in the linked
         ** list of clones.
         */

        sts = bmtr_get_rec(bfSetp->dirBfAp,
                           BSR_BFS_ATTR,
                           (void *) &bfSetAttr,
                           sizeof( bfSetAttr ) );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_open: get bf set attr failed", sts );
        }

        if (BS_BFTAG_EQL( bfSetAttr.origSetTag, NilBfTag )) {
            /*
             ** The specified set is the original set.  We've already
             ** opened it above so don't open it again.
             */

            setp = bfSetp;

        } else {
            /*
             ** The specified set is a clone.  Open the original set.
             */

            setId = bfSetId;
            setId.dirTag = bfSetAttr.origSetTag;

            sts = bfs_access( &setp, setId, options & BFS_OP_IGNORE_DEL, ftxH );
            if (sts != EOK) {
                /* close the clone opened above */
                (void) bfs_close( bfSetp, ftxH );
                RAISE_EXCEPTION( sts );
            }

            sts = bmtr_get_rec(setp->dirBfAp,
                               BSR_BFS_ATTR,
                               (void *) &bfSetAttr,
                               sizeof( bfSetAttr ) );
            if (sts != EOK) {
                ADVFS_SAD1( "bfs_open: get orig bf set attr failed", sts);
            }
        }

        origSetp = setp;
        prevSetp = setp;

        setDirTag = bfSetAttr.nextCloneSetTag;

        /*
         ** The following loop opens all the clone sets.  As it opens
         ** them it also sets up the bitfile set descriptors so that
         ** they are linked together.
         */
        /* Actually we only have one clone fileset (max) for each original */
        /* fileset. The following "while loop" will only execute once. */

        while (!BS_BFTAG_EQL( setDirTag, NilBfTag )) {
            setId = bfSetId;
            setId.dirTag = setDirTag;

            sts = bfs_access( &setp, setId, options & BFS_OP_IGNORE_DEL, ftxH );
            if (sts != EOK) {
                /* close the requested set opened above */
                (void) bfs_close( bfSetp, ftxH );
                /* close the original opened above if clone requested */
                if ( bfSetp != prevSetp )
                    (void) bfs_close( prevSetp, ftxH );
                RAISE_EXCEPTION( sts );
            }

            /* Link the set's descriptors together. */

            setp->origSetp = origSetp;
            prevSetp->cloneSetp = setp;

            prevSetp = setp;

            sts = bmtr_get_rec(setp->dirBfAp,
                               BSR_BFS_ATTR,
                               (void *) &bfSetAttr,
                               sizeof( bfSetAttr ) );
            if (sts != EOK) {
                ADVFS_SAD1("bfs_open: get clone bf set attr failed", sts);
            }

            setDirTag = bfSetAttr.nextCloneSetTag;
        }
    }

    /*
     * Unlock the table (if locked here).
     */

    if (tblLocked) {
      BFSETTBL_UNLOCK( bfSetp->dmnP )
    }

    *retBfSetp = bfSetp;

    return EOK;

HANDLE_EXCEPTION:

    if (tblLocked) {
        BFSETTBL_UNLOCK( bfSetp->dmnP )
    }

    /* Tell caller that no bitfile set was opened.*/
    *retBfSetp = NULL;
    return sts;
}

statusT
rbf_bfs_open(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    uint32T options,       /* in - options flags */
    ftxHT ftxH             /* in - transaction handle */
    )
{
    return bfs_open( retBfSetp, bfSetId, options, ftxH );
}

statusT
rbf_bfs_access(
    bfSetT **retBfSetp,    /* out - pointer to open bitfile-set */
    bfSetIdT bfSetId,      /* in - bitfile-set id */
    ftxHT ftxH             /* in - transaction handle */
    )
{
    return bfs_open( retBfSetp, bfSetId, BFS_OP_DEF, ftxH );
}


/*
 * delete_orig_set_tags
 *
 * Deletes all bitfiles in an 'orig' bitfile set.
 */

void
delete_orig_set_tags(
    bfSetT *bfSetp      /* in - ptr to bitfile set's desc */
    )
{
    bfTagT tag;
    statusT sts;
    bfAccessT *bfap;
    bfMCIdT mcid;
    vdIndexT vdi;
    int bfCnt = 1;
    struct vnode *nullvp;
    thread_t th = current_thread();

    /* TODO: locks */

    /*
     ** Scan the tagdir starting with tag 1 and delete all
     ** bitfiles that exist.
     */

    tag = NilBfTag;

    while (1) {
        sts = tagdir_lookup_next( bfSetp, &tag, &mcid, &vdi );
        if (sts == ENO_SUCH_TAG) {
            break;                   /* normal loop exit */
        }
        if (sts != EOK) {
            ADVFS_SAD1( "delete_orig_set_tags: can't find next tag: ", sts );
        }
        /*
         ** Open and delete the bitfile.
         */
        nullvp = NULL;
        sts = bs_access( &bfap, tag, bfSetp, FtxNilFtxH, BF_OP_GET_VNODE,
                         NULLMT, &nullvp );
        if (sts != EOK) {
            /* not worth crashing */
            ms_uprintf("delete_tags: bs_access( tag = %d ) err = %d", tag, sts);
            continue;
        }

        sts = bs_delete(bfap);
        if (sts != EOK) {
            domain_panic(bfap->dmnP, "delete_orig_set_tags: bs_delete returned: %d", sts);
            return;
        }

        bs_close(bfap, MSFS_BFSET_DEL | MSFS_DO_VRELE );

        /*
         * Since deleting a file set can take a long time and since the
         * kernel is not premptible,  we give up the processor periodically
         * so we don't "lock up" the system.
         */

        if ((bfCnt & 0x7f ) == 0) {        /* Remainder mod 128. */
            thread_preempt(th, FALSE);
        }
        bfCnt++;
    }
}

/*
 * delete_clone_set_tags
 *
 * Deletes all bitfiles in a 'clone' bitfile set.
 */

void
delete_clone_set_tags(
    bfSetT *bfSetp      /* in - ptr to bitfile set's desc */
    )
{
    bfTagT tag, delTag;
    statusT sts;
    bfAccessT *bfap, *origap;
    bfMCIdT mcid;
    vdIndexT vdi;
    int bfCnt = 1;
    int origExists, cloneExists;
    ftxHT ftxH;
    struct vnode *nullvp;
    thread_t th = current_thread();

    statusT
    rbf_delete_int(
        bfAccessT* bfap,          /* in - bitfile's access structure */
        ftxHT parentFtxH          /* in - handle to parent transaction */
        );

    tag = NilBfTag;

    while (1) {
        sts = tagdir_lookup_next( bfSetp, &tag, &mcid, &vdi );
        if (sts == ENO_SUCH_TAG) {
            break;                   /* normal loop exit */
        } else if (sts != EOK) {
            ADVFS_SAD1("delete_clone_set_tags: can't find next tag", sts );
        }

        /* Instead of calling bs_access (which opens the clone then fails
         * if it can't open the orig) I have "inlined" bs_access here so that
         * if the clone exists and the orig does not (due to a racing delete
         * the orig) I will still remove the clone.
         */
        if ( BS_BFTAG_RSVD(tag) ) {
            continue;
        }

        /* Open the original first. We need to do this since we are not
         * holding any locks. If the clone file we are about to clean up
         * has never had its metadata cloned, its primary mcell will be
         * the same as the original file. If the original is being 
         * deleted then we need to make sure its primary mcell can not
         * be freed while we are trying to map in the clone. 
         * By opening the original first we will either find that it 
         * is already deleted (we raced) or we will keep it from 
         * being removed by opening it.
         * if it already is deleted then the call to access_one on the
         * clone will fail and will just clean up the tag.
         *
         * The race that is being avoided here is when we are mapping 
         * in the extentmaps and the primary mcell goes away
         */
        nullvp = NULL;
        sts = bs_access_one( &origap, tag, bfSetp->origSetp, FtxNilFtxH, 
                             0, NULL, &nullvp, NULL );
        if (sts == EOK) {
            origExists = TRUE;
        } else {
            origExists = FALSE;
        }

        /* open the clone */
        nullvp = NULL;
        sts = bs_access_one( &bfap, tag, bfSetp, FtxNilFtxH,
                             BF_OP_GET_VNODE, NULL, &nullvp, origap );
        if (sts == EOK) {
            cloneExists = TRUE;
        } else {
            /*
             * It is possibile that during recovery the tag is still in
             * tag dir but we are using the original's mcells.
             */
            cloneExists = FALSE;
        }
        
        if (cloneExists && origExists)
        {
            bfap->origAccp = origap;
            origap->nextCloneAccp = bfap;
        }
        
        /*
         ** Delete the clone.
         */
        /* The transaction starts here vs before bs_access since any transaction
         * can behave like an exclusive transaction if the transaction list
         * wraps. An exclusive transaction (really any lock in the bs_access
         * path) can deadlock if held thru bs_access for a clone since that
         * will bs_access the orig which may block on VINACTIVATING (in vgetm).
         * The thread that set VINACTIVATING can block on a tranaction.
         * This is a lock ordering problem. See bs_cow.
         */
        sts = FTX_START_N( FTA_BFS_DELETE_CLONE, &ftxH, FtxNilFtxH,
                             bfSetp->dmnP, 0 );
        if (sts != EOK) {
            ADVFS_SAD1( "delete_clone_set_tags: ftx start exc failed", sts );
        }

        if (cloneExists && (bfap->cloneId != BS_BFSET_ORIG)) {
            /*
             ** The clone has its own metadata and possibly some pages from
             ** a copy-on-write operation.  So, delete the clone.
             */

            sts = rbf_delete_int( bfap, ftxH );
            if (sts != EOK) {
                domain_panic(bfap->dmnP, "delete_clone_set_tags: rbf_delete_int returned: %d", sts );
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }

        } else {
            /*
             ** The clone has no metadata of its own so just delete the tag.
             */

            delTag = tag;
            tagdir_remove_tag( bfSetp, &delTag, ftxH );

            /*
             ** Mark bitfile invalid (as bs_close() does when it deletes
             ** a bitfile) so that the bitfile can't be accessed once we
             ** release the exclusive ftx.
             */

            if (cloneExists) bfap->bfState = BSRA_INVALID;
        }

        /*
         ** Delete or close the original.
         */
        if (origExists) {
            COW_LOCK_READ( &origap->cow_lk )
            if (origap->deleteWithClone) {
                /*
                 ** The orignal was 'deleted' while this clone existed.  The
                 ** 'delete' was postponed until now because the clone depends
                 ** on the orignal (for some or all pages).  So, we delete
                 ** the original now.
                 */
                COW_UNLOCK( &origap->cow_lk )
                sts = rbf_delete_int( origap, ftxH );
                if (sts != EOK) {
                    domain_panic(origap->dmnP, "delete_clone_set_tags: rbf_delete_int returned: %d", sts );
                    RAISE_EXCEPTION(E_DOMAIN_PANIC);
                }
            }
            else
                COW_UNLOCK( &origap->cow_lk )
        }

        ftx_done_n( ftxH, FTA_BFS_DELETE_CLONE );

        /*
         ** At this point the clone's tag no longer exists.  This means
         ** that bs_cow(), bs_cow_pg() and rbf_delete() can't 'see' the
         ** clone anymore so we don't need to synchronize with them.
         **
         ** Close the clone and the original.
         */

        if (cloneExists)
        {
            sts = bs_close_one(bfap, MSFS_BFSET_DEL | MSFS_DO_VRELE, FtxNilFtxH );
            if (sts != EOK) {
                ADVFS_SAD1( "delete_clone_set_tags: bs_close_one err", sts );
            }
        }

        if (origExists) {
            sts = bs_close_one(origap, MSFS_BFSET_DEL, FtxNilFtxH);
            if (sts != EOK) {
                ADVFS_SAD1( "delete_clone_set_tags: bs_close_one err", sts );
            }
        }

        /*
         ** Since deleting a file set can take a long time and since the
         ** kernel is not premptible,  we give up the processor periodically
         ** so we don't "lock up" the system.
         */

        if ((bfCnt & 0x7f) == 0) {        /* Remainder mod 128. */
            thread_preempt(th, FALSE);
        }
        bfCnt++;
    }
    return;

HANDLE_EXCEPTION:

    ftx_fail(ftxH);

    bs_close_one(bfap, MSFS_BFSET_DEL | MSFS_DO_VRELE, FtxNilFtxH );
    if (origExists)
        bs_close_one(origap, MSFS_BFSET_DEL, FtxNilFtxH);

    return;
}

/*
 * bfs_delete_pending_list_add
 *
 * Adds a bitfile set to the "bf set delete pending" list.
 *
 * Caller must hold the BfSetTblLock locked via an FTX lock.
 *
 * Returns 1 if the bf set was successfully added to the delete pending list.
 * Returns 0 if the bf set was already on the delete pending list.
 *
 * Note that this routine cannot start its own sub transaction because
 * it is called from a ftx undo routine in addition to being a general
 * "add to the list" routine.  Therefore, the caller is responsible for
 * starting the transaction.
 */

static int
bfs_delete_pending_list_add(
    bfAccessT *dirBfAp,         /* in - set's tagdir bf struct */
    domainT *dmnP,              /* in - set's domain pointer */
    ftxHT ftxH                  /* in - transaction handle */
    )
{
    bsBfSetAttrT *setAttrp;
    bsDmnMAttrT *dmnMAttrp;
    statusT sts;
    vdT *logVdp = NULL;
    rbfPgRefHT fsetPgH, dmnPgH;
    bfAccessT *mdap;

    sts = bmtr_get_rec_ptr( dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_add: bmtr_get_rec_ptr failed return code = %d.", sts );
        return 0;    /* E_DOMAIN_PANIC */
    }

    if (setAttrp->state == BFS_ODS_DELETED) {
        /*
         * The set is already on the delete pending list.
         * deref the page returned by bmtr_get_rec_ptr()
         */

        rbf_deref_page( fsetPgH, BS_CACHE_IT );

        return 0;
    }

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = logVdp->rbmtp;
    } else {
        mdap = logVdp->bmtp;
    }

    sts = bmtr_get_rec_ptr( mdap,
                            ftxH,
                            BSR_DMN_MATTR,
                            sizeof( *dmnMAttrp ),
                            1 /* pin pg */,
                            (void *) &dmnMAttrp,
                            &dmnPgH,
                            NULL );
    if (sts != EOK){
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_add: bmtr_get_rec_ptr failed return code = %d.", sts );
        return 0;    /* E_DOMAIN_PANIC */
    }

    /* mark bitfile set 'deleted' */
    RBF_PIN_FIELD( fsetPgH, setAttrp->state );
    setAttrp->state = BFS_ODS_DELETED;

    /* add the bitfile set to the 'delete pending list' */
    RBF_PIN_FIELD( fsetPgH, setAttrp->nxtDelPendingBfSet );
    setAttrp->nxtDelPendingBfSet = dmnMAttrp->delPendingBfSet;

    RBF_PIN_FIELD( dmnPgH, dmnMAttrp->delPendingBfSet );
    dmnMAttrp->delPendingBfSet = setAttrp->bfSetId.dirTag;

    return 1;
}

/*
 * del_list_remove_undo
 *
 * Puts the bitfile set back on the 'bf set delete pending' list.
 */

typedef struct {
    bfSetIdT setId;
} delPendUndoRecT ;

opxT del_list_remove_undo;

void
del_list_remove_undo(
    ftxHT ftxH,      /* in - ftx handle */
    int opRecSz,     /* in - size of opx record */
    void* opRec      /* in - ptr to opx record */
    )
{
    delPendUndoRecT undoRec = *(delPendUndoRecT *)opRec;
    bfAccessT *tagDirBfap;
    domainT *dmnP;
    statusT sts;
    struct vnode *nullvp = NULL;

    dmnP =  ftxH.dmnP;

    /*
     * Open the orig set's tag dir.
     */

    sts = bs_access(&tagDirBfap, undoRec.setId.dirTag, dmnP->bfSetDirp,
                    FtxNilFtxH, 0, NULLMT, &nullvp);
    if (sts != EOK) {
        ADVFS_SAD1( "del_list_remove_undo: bs_access failed", sts );
    }

    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH )

    bfs_delete_pending_list_add(tagDirBfap, dmnP, ftxH );

    bs_close(tagDirBfap, 0);
}

/*
 * bfs_delete_pending_list_remove
 *
 * Removes a bitfile set from the 'bf set delete pending' list.
 *
 * This routine uses a subtransaction to do its work.  It also
 * has an operational undo which simply puts the set back on the
 * delete pending list.
 */

static void
bfs_delete_pending_list_remove(
    bfAccessT *dirBfAp,         /* in - set's tagdir bf struct */
    ftxHT parentFtxH            /* in - parent transaction handle */
    )
{
    ftxHT ftxH;
    bsBfSetAttrT *setAttrp, *nextSetAttrp;
    bsDmnMAttrT *dmnMAttrp;
    statusT sts;
    domainT *dmnP;
    vdT *logVdp = NULL;
    delPendUndoRecT undoRec;
    bfAccessT *nextTagDirBfap;
    bfTagT nextTag;
    rbfPgRefHT fsetPgH, nextFsetPgH, dmnPgH;
    bfAccessT *mdap;
    struct vnode *nullvp;

    dmnP = dirBfAp->dmnP;

    sts = FTX_START_N( FTA_BS_BFS_DEL_LIST_REMOVE_V1, &ftxH,
                       parentFtxH, dmnP, 2 );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_delete_pending_list_remove: ftx_start failed", sts );
    }

    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH )

    sts = bmtr_get_rec_ptr( dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (1)  failed, return code = %d", sts );
        ftx_fail(ftxH);
        return;
    }

    /*
     * Get the 'head' of the 'bf set delete pending list' from
     * the domain attributes record.
     */

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = logVdp->rbmtp;
    } else {
        mdap = logVdp->bmtp;
    }

    sts = bmtr_get_rec_ptr( mdap,
                            ftxH,
                            BSR_DMN_MATTR,
                            sizeof( *dmnMAttrp ),
                            1 /* pin pg */,
                            (void *) &dmnMAttrp,
                            &dmnPgH,
                            NULL );
    if (sts != EOK){
        domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (2)  failed, return code = %d", sts );
        ftx_fail(ftxH);
        return;
    }

    /*
     * Now scan the list until we find the set that preceeds the set
     * we are deleting.  This is a typical remove an element from
     * a singly-linked list where you need the element that preceeds
     * the to-be-deleted element so that you can fix up the pointers
     * properly.
     */

    nextTag = dmnMAttrp->delPendingBfSet;

    while (!BS_BFTAG_EQL( nextTag, setAttrp->bfSetId.dirTag )) {

        nullvp = NULL;
        sts = bs_access( &nextTagDirBfap, nextTag, dmnP->bfSetDirp,
                              FtxNilFtxH, 0, NULLMT, &nullvp );
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bs_access failed, return code = %d", sts ); 
            ftx_fail(ftxH);
            return;
        }

        sts = bmtr_get_rec_ptr( nextTagDirBfap,
                                ftxH,
                                BSR_BFS_ATTR,
                                sizeof( *nextSetAttrp ),
                                1 /* pin pg */,
                                (void *) &nextSetAttrp,
                                &nextFsetPgH,
                                NULL );
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, "bfs_delete_pending_list_remove: bmtr_get_rec_ptr (3) failed, return code = %d", sts );
            ftx_fail(ftxH);
            return;
        }

        nextTag = nextSetAttrp->nxtDelPendingBfSet;

        if (!BS_BFTAG_EQL( nextTag, setAttrp->bfSetId.dirTag )) {
            rbf_deref_page( nextFsetPgH, BS_CACHE_IT );
            bs_close(nextTagDirBfap, 0);
        }
    }

    /*
     * Mark bitfile set 'invalid'
     */

    RBF_PIN_FIELD( fsetPgH, setAttrp->state );
    setAttrp->state = BFS_ODS_INVALID;

    /*
     * remove the bitfile set from the 'delete pending list'
     */

    if (BS_BFTAG_EQL( dmnMAttrp->delPendingBfSet, setAttrp->bfSetId.dirTag )) {

        RBF_PIN_FIELD( dmnPgH, dmnMAttrp->delPendingBfSet );
        dmnMAttrp->delPendingBfSet = setAttrp->nxtDelPendingBfSet;

    } else {
        RBF_PIN_FIELD( nextFsetPgH, nextSetAttrp->nxtDelPendingBfSet );
        nextSetAttrp->nxtDelPendingBfSet = setAttrp->nxtDelPendingBfSet;

        bs_close(nextTagDirBfap, 0);
    }

    RBF_PIN_FIELD( fsetPgH, setAttrp->nxtDelPendingBfSet );
    setAttrp->nxtDelPendingBfSet = NilBfTag;

    undoRec.setId = setAttrp->bfSetId;

    ftx_done_u(ftxH, FTA_BS_BFS_DEL_LIST_REMOVE_V1, sizeof(undoRec), &undoRec);
}

/*
 * bfs_delete_pending_list_finish_all
 *
 * This routine scans the 'bf set delete pending' list and finishes
 * deleting all the bitfile sets which are on the list.
 *
 * Note:  Since this routine is only called during domain activation it
 *        assumes that no other thread is mucking with the 'bf set
 *        delete pending' list.  Therefore, no locking is done.
 */

void
bfs_delete_pending_list_finish_all(
    domainT *dmnP,                       /* in */
    u_long flag				 /* in */ 
    )
{
    statusT sts;
    bfSetIdT setId;
    bsDmnMAttrT dmnMAttr;
    bfTagT nextTag;
    bfAccessT *tagDirBfap;
    bsBfSetAttrT setAttr;
    vdT *logVdp = NULL;
    bfAccessT *mdap;
    struct vnode *nullvp;

    /*
     * Get the 'head' of the 'bf set delete pending list' from
     * the domain attributes record.
     */

    logVdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = logVdp->rbmtp;
    } else {
        mdap = logVdp->bmtp;
    }

    sts = bmtr_get_rec(mdap,
                       BSR_DMN_MATTR,
                       &dmnMAttr,
                       sizeof( dmnMAttr ) );
    if (sts != EOK) {
        ADVFS_SAD1( "bfs_delete_pending_list_finish_all: get dmn attr failed", sts );
    }

    /*
     * Now scan the list and call bs_bfs_delete() for each set in the list.
     * Note that bs_bfs_delete() will remove the deleted set from the
     * list so this routine does not have to deal with that issue.
     */

    setId.domainId = dmnP->domainId;
    nextTag = dmnMAttr.delPendingBfSet;

    while (!BS_BFTAG_EQL( nextTag, NilBfTag )) {
        /*
         * Get and save the pointer (tag) to the next set in the list
         * before deleting the set.  This is because the pointer to the
         * the next set is kept in the set's tag dir's mcells; these
         * deleted when the set is deleted.
         */

        nullvp = NULL;
        sts = bs_access( &tagDirBfap,
                         nextTag,
                         dmnP->bfSetDirp,
                         FtxNilFtxH,
                         0,
                         NULLMT,
                         &nullvp );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_delete_pending_list_finish_all: bs_access failed", sts );
        }

        sts = bmtr_get_rec( tagDirBfap,
                           BSR_BFS_ATTR,
                           (void *) &setAttr,
                           sizeof( setAttr ) );
        if (sts != EOK) {
            ADVFS_SAD1( "bfs_delete_pending_list_finish_all: get orig set attr failed", sts );
        }

        bs_close(tagDirBfap, 0);

        setId.dirTag = nextTag;
        nextTag = setAttr.nxtDelPendingBfSet;

        bs_bfs_delete( setId, dmnP, 0 , flag );
    }
}

typedef struct {
    bfSetIdT origSetId;
    bfTagT   nextCloneSetTag;
} unlinkCloneUndoRecT;

/*
 * unlink_clone_undo
 *
 * Relinks a orig set with its clone.
 *
 * Note:  It is assume that this routine will be called only
 *        during crash recovery.  Runtime ftx_fails() are not
 *        handled; and don't need to be given the way bs_bfs_delete()
 *        written.
 */

opxT unlink_clone_undo;

void
unlink_clone_undo(
    ftxHT ftxH,      /* in - ftx handle */
    int opRecSz,     /* in - size of opx record */
    void* opRec      /* in - ptr to opx record */
    )
{
    unlinkCloneUndoRecT undoRec = *(unlinkCloneUndoRecT *)opRec;
    domainT *dmnP;
    bfAccessT *tagDirBfap;
    statusT sts;
    bsBfSetAttrT *origSetAttrp;
    rbfPgRefHT fsetPgH;
    struct vnode *nullvp = NULL;

    dmnP =  ftxH.dmnP;

    /*
     * Open the orig set's tag dir.
     */

    sts = bs_access( &tagDirBfap,
                     undoRec.origSetId.dirTag,
                     dmnP->bfSetDirp,
                     FtxNilFtxH,
                     0,
                     NULLMT,
                     &nullvp );
    if (sts != EOK) {
        ADVFS_SAD1( "unlink_clone_undo: bs_access failed", sts );
    }

    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH )

    sts = bmtr_get_rec_ptr( tagDirBfap,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *origSetAttrp ),
                            1 /* pin pg */,
                            (void *) &origSetAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(dmnP, "unlink_clone_undo: bmtr_get_rec_ptr failed, return code = %d", sts );
        return;
    }

    /*
     * Relink the clone to the orig set.
     */

    RBF_PIN_FIELD( fsetPgH, origSetAttrp->numClones );
    origSetAttrp->numClones++;
    RBF_PIN_FIELD( fsetPgH, origSetAttrp->nextCloneSetTag );
    origSetAttrp->nextCloneSetTag = undoRec.nextCloneSetTag;

    bs_close(tagDirBfap, 0);
}

/*
 * unlink_clone
 *
 * Changes the on-disk set attributes for a clone's parent original
 * set so that the sets are no longer tied to each other.
 * The BfSetTblLock should be held while calling this function.
 */

static void
unlink_clone(
    bfSetT *origSetp, /* in - orig set's desc */
    ftxHT parentFtxH  /* in - parent ftx handle */
    )
{
    bsBfSetAttrT *origSetAttrp;
    ftxHT ftxH;
    statusT sts;
    unlinkCloneUndoRecT undoRec;
    rbfPgRefHT fsetPgH;

    /*
     * Unlink clone from orig set.
     * NOTE: this only works when only one clone is allowed.
     */

    sts = FTX_START_N( FTA_BS_BFS_UNLINK_CLONE_V1, &ftxH, parentFtxH,
                       origSetp->dmnP, 2 );
    if (sts != EOK) {
        ADVFS_SAD1( "unlink_clone: start ftx failed", sts );
    }

    sts = bmtr_get_rec_ptr( origSetp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *origSetAttrp ),
                            1 /* pin pg */,
                            (void *) &origSetAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "unlink_clone: bmtr_get_rec_ptr failed, return code = %d", sts );
        ftx_fail(ftxH);
        return;
    }

    undoRec.origSetId = origSetp->bfSetId;
    undoRec.nextCloneSetTag = origSetAttrp->nextCloneSetTag;

    RBF_PIN_FIELD( fsetPgH, origSetAttrp->numClones );
    origSetAttrp->numClones--;
    origSetp->numClones--;

    RBF_PIN_FIELD( fsetPgH, origSetAttrp->nextCloneSetTag );
    origSetAttrp->nextCloneSetTag = NilBfTag;

    /*
    ** Use domain mutex to help serialize hold_cloneset vs rmfset.
    ** hold_cloneset looks at origSetp->cloneSetp then looks at
    ** bfsDelPending (set by unlink_clone's caller, bs_bfs_delete)
    ** all under the domain mutex. Those 2 tests will both occur
    ** before or after cloneSetp is set to NULL. There is no possibility
    ** that this rmfset will continue to deallocating the bfSet structure
    ** while hold_cloneset is holding the domain mutex.
    */
    mutex_lock( &origSetp->dmnP->mutex );
    origSetp->cloneSetp = NULL;
    mutex_unlock( &origSetp->dmnP->mutex );


    ftx_done_u( ftxH, FTA_BS_BFS_UNLINK_CLONE_V1, sizeof(undoRec), &undoRec );
}


/*
 * bs_bfs_delete
 *
 * Deletes a bitfile set.   This basically means:
 *      - delete all bitfiles in the set
 *      - delete the set's tagdir
 *
 * Note:  This routine must be idempotent.  In other words, if the system
 *        crashes during a bs_bfs_delete() it must be possible for
 *        domain activation to call this routine again to complete
 *        the deletetion.  Therefore, this routine can't make any
 *        assumptions about being called only once.
 */


statusT
bs_bfs_delete(
    bfSetIdT bfSetId,   /* in - bitfile set id */
    domainT *dmnP,      /* in - set's domain pointer */
    long xid,		/* in - CFS transaction id */
    u_long flag		/* in - flag to indicate global root failover */
    )
{
    bfSetT *bfSetp;
    int setOpen = FALSE, abortOnError = FALSE;
    int deletingClone = FALSE, ftxStarted = FALSE;
    int restoreCloneDelState = FALSE;
    bfSetT *setp, *origSetp = NULL;
    statusT sts;
    bfAccessT *dirBfAp;
    ftxHT ftxH = FtxNilFtxH;
    lkStatesT currentCloneDelState;
    int ss_was_activated = FALSE;
    u_long dmnState;
    uint refCnt;

    /*
     * Note that the following 'access' opens the orig and the clone (if
     * a clone exists).
     */
    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    setOpen = TRUE;

    /*
    ** Check here for operator errors (rmfset orig while clone exists)
    ** early. If we get into bs_bfs_delete and fail then the clone set
    ** is marked out of sync.
    */
    BFSETTBL_LOCK_READ( dmnP );

    refCnt = bfSetp->cloneId != BS_BFSET_ORIG ?
               bfSetp->origSetp->fsRefCnt + 1 : 1;

    /* Add in threads currently xfering xtnts in xfer_xtnts_to_clone.
     * We wait for these xfering threads below before beginning the
     * actual clone delete.
     */
    refCnt += bfSetp->xferThreads;

    MS_SMP_ASSERT(refCnt > 0);
    if ( bfSetp->fsRefCnt > refCnt ) {
        /* fileset is accessed by another thread */
        RAISE_EXCEPTION( E_TOO_MANY_ACCESSORS );
    }

    if ( bfSetp->numClones > 0 ) {
        /* orig fileset has clones dependent on it  */
        RAISE_EXCEPTION( E_HAS_CLONE );
    }
    refCnt = 0;  /* refCnt is also a BfSetTablLock-needs-unlock flag */
    BFSETTBL_UNLOCK( dmnP );

    if( (flag & M_FAILOVER) && (flag & M_GLOBAL_ROOT) ) {
		dmnState =  M_GLOBAL_ROOT;
    }
    else {
		dmnState = 0;
    }
    		 
    if(dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        /* deactivate/drain vfast operations */
        ss_change_state(dmnP->domainName, SS_SUSPEND, &dmnState, 0);
        ss_was_activated = TRUE;
    }

    /* 
     * If we're deleting a clone fileset, wait until any threads currently
     * in xfer_xtnts_to_clone() finish their work.  This also blocks new
     * xfer_xtnts_to_clone() threads from accessing the clone fileset.
     */
    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        mutex_lock(&bfSetp->cloneDelStateMutex);

        currentCloneDelState = lk_get_state(bfSetp->cloneDelState); 

        MS_SMP_ASSERT(currentCloneDelState == CLONE_DEL_NORMAL   ||
                      currentCloneDelState == CLONE_DEL_XFER_STG);

        if (currentCloneDelState == CLONE_DEL_XFER_STG) {
            MS_SMP_ASSERT(bfSetp->xferThreads > 0);
            lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_PENDING);
            lk_wait_for(&bfSetp->cloneDelState, &bfSetp->cloneDelStateMutex,
                        CLONE_DEL_DELETING);
        }
        else {
            MS_SMP_ASSERT(bfSetp->xferThreads == 0);
            lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_DELETING);
        }

        mutex_unlock(&bfSetp->cloneDelStateMutex);
        restoreCloneDelState = TRUE;

        mutex_lock(&dmnP->mutex);
        bfSetp->bfsDelPend = TRUE;
        /* Wait for current cows. All future cows will silently do nothing. */
        while ( bfSetp->bfsHoldCnt > 0 ) {
            MS_SMP_ASSERT(bfSetp->bfsOpWait == 0);
            bfSetp->bfsOpWait = 1;
            thread_sleep( (vm_offset_t)&bfSetp->bfsHoldCnt,
                          &dmnP->mutex.mutex, FALSE );
            mutex_lock( &dmnP->mutex );
        }
        mutex_unlock(&dmnP->mutex);
    }
   
    sts = FTX_START_EXC_XID( FTA_BFS_DEL_PENDING_ADD, &ftxH, dmnP, 2, xid );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_delete: ftx_start failed", sts );
    }
    ftxStarted = TRUE;


    FTX_LOCKWRITE( &dmnP->BfSetTblLock, ftxH )

    if (bfSetp->fsRefCnt > ((bfSetp->cloneId != BS_BFSET_ORIG) ? bfSetp->origSetp->fsRefCnt + 1: 1)) {
        /*
         * Can't delete set if someone else is accessing it.
         */
        RAISE_EXCEPTION( E_TOO_MANY_ACCESSORS );
    }

    if (bfSetp->numClones > 0) {
        /*
         * Can't delete set if it has clones dependent on it.
         */
        RAISE_EXCEPTION( E_HAS_CLONE );
    }

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /*
         * Open the orig bf set again to keep its set desc valid
         * until we're done.
         */
        sts = bfs_access( &origSetp,
                          bfSetp->origSetp->bfSetId,
                          BFS_OP_DEF,
                          ftxH );
        if ( sts == E_DOMAIN_PANIC ) {
            RAISE_EXCEPTION( sts );
        }
        if ( sts != EOK ) {
            if ( sts == EHANDLE_OVF ) {
                RAISE_EXCEPTION( EHANDLE_OVF );
            }
            else {
            ADVFS_SAD1( "bs_bfs_delete: bfs_access err", sts );
            }
        }

        deletingClone = TRUE;
    }

    bfSetp->state = BFS_DELETING;

    /*
     * We need to add the set to the delete pending list.
     * This way if the system crashes during the delete we'll know
     * at domain activation time that we need to finish deleting this set.
     */

    if (bfs_delete_pending_list_add(bfSetp->dirBfAp, dmnP, ftxH)){
        ftx_done_n( ftxH, FTA_BFS_DEL_PENDING_ADD );

    } else {
        /* bitfile set was already on the delete pending list */
        ftx_fail( ftxH );
    }

    ftxStarted = FALSE;

    /*
     * Delete the bitfiles in the set. This can take a long time so
     * we don't hold any locks or have any open transactions.
     */

    if (bfSetp->cloneId == BS_BFSET_ORIG) {
        delete_orig_set_tags( bfSetp );
    } else {
        delete_clone_set_tags( bfSetp );
    }

    abortOnError = TRUE;
    restoreCloneDelState = FALSE;

    /*
     * Now, really delete the set.
     */

    sts = FTX_START_EXC( FTA_BFS_DELETE, &ftxH, dmnP, 2 );
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_delete: start ftx failed", sts );
    }

    ftxStarted = TRUE;
    BFSETTBL_LOCK_WRITE( dmnP )

    /*
     * Open only the set we are deleting again to keep its set desc
     * valid until we're done.
     */

    sts = bfs_access( &setp, bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if ( sts == E_DOMAIN_PANIC ) {
        if (deletingClone)
            bfs_close( origSetp, ftxH );
        BFSETTBL_UNLOCK( dmnP );
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
        ftx_fail( ftxH );
        return sts;
    }
    if (sts != EOK) {
        if ( sts == EHANDLE_OVF ) {
            RAISE_EXCEPTION( EHANDLE_OVF );
        }
        else {
        ADVFS_SAD1( "bs_bfs_delete: bfs_access err", sts );
        }
    }

    BFSETTBL_UNLOCK( dmnP )

    /* Close the sets. */

    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

    /*
     * Unlinking of the clone and decrementing the reference count
     * down to zero must be done atomically, i.e. under tha
     * bfsettbl lock.
     */

    BFSETTBL_LOCK_WRITE( dmnP )
    
    while (setp->fsRefCnt > 0) {
        setp->fsRefCnt--;
        bs_domain_close( dmnP );
    }
    
    dirBfAp = setp->dirBfAp;

    /*
     * This transaction ensures that the unlinking of a clone
     * from it orig set, the unlinking of the deleted set from the
     * delete pending list, and the deleting of the deleted set's
     * tag dir are done atomically.
     */

    if (deletingClone) {
        unlink_clone( origSetp, ftxH );
    }

    BFSETTBL_UNLOCK( dmnP )

    bfs_delete_pending_list_remove(dirBfAp, ftxH);

    /*
     * Delete the set's tag dir.  This has to be done after the above
     * unlinking since the links are maintained in the tag dir's
     * mcells.  Hence, we can't delete the tag dir before we deal with
     * links.
     */

    sts = rbf_delete( dirBfAp, ftxH );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_delete: rbf_delete returned: %d", sts);
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    /*
     * Now that the on-disk representation of the fileset has been
     * invalidated, invalidate the in-memory representation.  It is
     * important to do this after the on-disk representation has been
     * invalidated because if the in-memory representation were
     * invalidated (and removed, via ms_free), the fileset could still
     * be accessed afresh, since there would be no in-memory warning
     * that the fileset is going away.
     */

    BFSETTBL_LOCK_WRITE( dmnP )
    bfs_dealloc( setp, TRUE );
    BFSETTBL_UNLOCK( dmnP )

    /* in a cluster ensure removing a clone fset cannot be undone if 
     * the server fails because we are about to notify all cluster
     * nodes that they can write directly to storage if they hold the
     * appropriate tokens.  Because this bypasses AdvFS, no COWs will
     * be performed.
     */
    if (clu_is_ready() && deletingClone)
	ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC );

    ftx_done_n( ftxH, FTA_BFS_DELETE );
    ftxStarted = FALSE;
   

    bs_close(dirBfAp, MSFS_BFSET_DEL);

    if (deletingClone) {
        /*
         * Mark orig bf set as READY and close it.
         */
	if (origSetp->fsnp) {

            fsid_t fsid;
            BS_GET_FSID(origSetp,fsid);
	    /*
	     * Tell CFS that the clone has been deleted
	     */

	    CC_CFS_CLONE_NOTIFY(fsid, CLONE_DELETE);
	}
        BFSETTBL_LOCK_WRITE( dmnP )
        bfs_close( origSetp, ftxH );
        BFSETTBL_UNLOCK( dmnP )
    }

    if( (flag & M_FAILOVER) && (flag & M_GLOBAL_ROOT) ) {
		dmnState =  M_GLOBAL_ROOT;
    }
    else {
		 dmnState = 0;
    }
		
    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
        ss_was_activated = FALSE;
    }

    return EOK;

HANDLE_EXCEPTION:

    if (abortOnError) {
        ADVFS_SAD1( "bs_bfs_delete: can't recover from error", sts );
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    } else if ( refCnt ) {
        BFSETTBL_UNLOCK( dmnP );
    }

    if (deletingClone) {
        BFSETTBL_LOCK_WRITE( dmnP )
        bfs_close( origSetp, ftxH );
        BFSETTBL_UNLOCK( dmnP )
    }

    if ( restoreCloneDelState ) {
        mutex_lock(&bfSetp->cloneDelStateMutex);
        lk_set_state(&bfSetp->cloneDelState, CLONE_DEL_NORMAL);
        mutex_unlock(&bfSetp->cloneDelStateMutex);
    }

    mutex_lock(&dmnP->mutex);  /* bind bfsDelPend & BFS_OD_OUT_OF_SYNC */
    bfSetp->bfsDelPend = FALSE;

    if ( bfSetp->bfSetFlags & BFS_OD_OUT_OF_SYNC ) {
        mutex_unlock(&dmnP->mutex);
        /* A cow was ignored because bfsDelPend was set. */
        /* But bs_bfs_delete failed so now the clone is out of sync. */
        sts = FTX_START_N(FTA_BS_COW_PG, &ftxH, FtxNilFtxH, dmnP, 0);
        if ( sts == EOK ) {
            bs_bfs_out_of_sync(bfSetp, ftxH);
            ftx_done_n(ftxH, FTA_BS_COW_PG);
        } else {
            domain_panic(dmnP, "bs_bfs_delete: can't start ftx");
        }
    } else {
        mutex_unlock(&dmnP->mutex);
    }

    if (setOpen) {
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    }

    if( (flag & M_FAILOVER) && (flag & M_GLOBAL_ROOT) ) {
		dmnState =  M_GLOBAL_ROOT;
    }
    else {
		 dmnState = 0;
    }

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
    }

    return sts;
}

/*
 * clone_tagdir
 *
 * This routine copies the tag directory from one set (orig) to
 * another set (clone).
 *
 * This routine does not use transactions to perform the copy.  It
 * simply flushes the clone's tag dir after copying all of the orig's
 * tag dir to it.  Therefore, upon successfully completion of this
 * routine, it is guaranteed that the the clone's tag dir is on
 * stable storage.
 *
 * Returns status of the routines it calls.
 */

static statusT
clone_tagdir(
    bfSetT *origSetp,           /* in - orig set desc */
    bfSetT *cloneSetp,          /* in - clone set desc */
    uint32T numDirPages,        /* in - num pages in orig set's tag dir */
    ftxHT ftxH                  /* in - ftx handle */
    )
{
    statusT sts;
    bfPageRefHT origPgRef, clonePgRef;
    char *origPgp, *clonePgp;
    uint32T pg;
    bfParamsT cloneDirParams;
    extern struct lockinfo *ADVClonebsInMemXtntT_migTruncLk_info;

    sts = bs_get_bf_params( cloneSetp->dirBfAp, &cloneDirParams, 0 );
    if (sts != EOK) {
        ADVFS_SAD1( "clone_tagdir: get clone set dir attr failed", sts );
    }

    /*
     ** Allocate enough pages to the clone's tag dir to do the copy.
     */

    if (cloneDirParams.numPages < numDirPages ) {
        /* we can't require the migTruncLk to be held here because,
         * even if the fileset is a clone, the fileset tag directory
         * is never a clone (in the sense that we never do COW on it -
         * it is created once and for all here in this routine),
         * and we are already inside a transaction by the time we
         * get an access structure for it (in bs_bfs_clone()).
         * But since the transaction is exclusive, migrate will not
         * race with the rbf_add_stg() here.
         */
        sts = rbf_add_stg( cloneSetp->dirBfAp,
                           cloneDirParams.numPages,
                           numDirPages - cloneDirParams.numPages,
                           ftxH, 0 );
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    /*
     ** Copy the orig set's tag dir to the clone's tag dir one page at a time.  */

    for (pg = 0; pg < numDirPages; pg++) {
        sts = bs_refpg( &origPgRef,
                        (void *) &origPgp,
                        origSetp->dirBfAp,
                        pg,
                        BS_NIL );

        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
        sts = bs_pinpg( &clonePgRef,
                        (void *) &clonePgp,
                        cloneSetp->dirBfAp,
                        pg,
                        BS_NIL );

        if (sts == EOK) {
            bcopy( origPgp, clonePgp, ADVFS_PGSZ );
            (void) bs_unpinpg( clonePgRef, logNilRecord, BS_DIRTY );
            (void) bs_derefpg( origPgRef, BS_CACHE_IT );
        }

        if (sts != EOK) {
            (void) bs_derefpg( origPgRef, BS_CACHE_IT );
            RAISE_EXCEPTION( sts );
        }
    }

    sts = bfflush(cloneSetp->dirBfAp, 0, 0, FLUSH_IMMEDIATE);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    cloneSetp->tagFrLst = origSetp->tagFrLst;
    cloneSetp->tagUnInPg = origSetp->tagUnInPg;
    cloneSetp->tagUnMpPg = origSetp->tagUnMpPg;

    return EOK;

HANDLE_EXCEPTION:
    return sts;
}

/*
 * bs_bfs_clone
 *
 * Given the bitfile set id of an existing and activated bitfile set,
 * this routine will create a bitfile which is a clone of the original.
 * A clone is created by creating a new set, copying the orignal sets
 * tag directory to the new set, and by copying the orignal sets
 * attributes to the new set (with a few modifications).
 *
 * Upon successful completion of this routine, the clone bitfile set
 * will be linked into the list of clones and it will have been open
 * the same number of times as the original bitfile set.  The latter
 * item is required in order for bs_bfs_close() to work properly.
 *
 * Returns status returned by bfs_access(), rbf_bfs_create(),
 * bs_get_bf_params(), and clone_tagdir().
 */

statusT
bs_bfs_clone(
    bfSetIdT origSetId,         /* in - activated orig set's id */
    char *cloneSetName,         /* in - clone set's name */
    bfSetIdT *cloneSetId,       /* out - new clone's id */
    domainT *dmnP,              /* in - set's domain pointer */
    long xid                    /* in - CFS transaction id */
    )
{
    statusT sts;
    int ftxStarted = FALSE, lkLocked = FALSE;
    bfSetT *origSetp = NULL, *cloneSetp = NULL;
    bsBfSetAttrT *origSetAttrp = NULL, *cloneSetAttrp = NULL;
    bfParamsT *origDirParamsp = NULL;
    ftxHT ftxH = FtxNilFtxH;

    /*
     * Minimize stack usage.
     */

    origSetAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));
    if (origSetAttrp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    cloneSetAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));
    if (cloneSetAttrp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    origDirParamsp = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));
    if (origDirParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    sts = bfs_access( &origSetp, origSetId, BFS_OP_DEF, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if (origSetp->cloneId != BS_BFSET_ORIG) {
        RAISE_EXCEPTION( E_CANT_CLONE_A_CLONE );
    }

    /*
    ** This code serializes with hold_cloneset.
    ** bfsOpPend is set from here to when numClones is incremented.
    ** So between the two, we know a clone operation is started or completed.
    */
    mutex_lock( &dmnP->mutex );

    /* Racing bs_bfs_clone will see bfsOpPend or numClones and bail. */
    if ( origSetp->bfsOpPend || origSetp->numClones >= BS_MAX_CLONES) {
        mutex_unlock( &dmnP->mutex );
        RAISE_EXCEPTION( E_TOO_MANY_CLONES );
    }

    origSetp->bfsOpPend = 1;

    while ( origSetp->bfsHoldCnt > 0 ) {
        /*
        ** Wait for threads that don't want the clone fileset to come into
        ** existence just yet.
        ** No other thread can have set bfsOpWait. We can't be waiting
        ** in bs_bfs_delete or advfs_mountfs.
        */
        MS_SMP_ASSERT(origSetp->bfsOpWait == 0);
        origSetp->bfsOpWait = 1;
        thread_sleep( (vm_offset_t)&origSetp->bfsHoldCnt,
                      &dmnP->mutex.mutex, FALSE );
        mutex_lock( &dmnP->mutex );
    }

    mutex_unlock( &dmnP->mutex );

    sts = FTX_START_EXC_XID( FTA_BFS_CLONE, &ftxH, dmnP, 2, xid );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    ftxStarted = TRUE;

    BFSETTBL_LOCK_WRITE( dmnP )
    lkLocked = TRUE;

#if 0
    if ((dmnP->freeBlks / (dmnP->totalBlks / 100)) < 5) {
        RAISE_EXCEPTION( E_AVAIL_BLKS_TOO_LOW );
    }
#endif

    sts = bs_get_bf_params( origSetp->dirBfAp, origDirParamsp, 0 );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bfs_create( dmnP,
                      origDirParamsp->cl.reqServices,
                      origDirParamsp->cl.optServices,
                      cloneSetName,
                      0,
                      ftxH,
                      cloneSetId );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    sts = bfs_access( &cloneSetp, *cloneSetId, BFS_OP_DEF, ftxH );
    if (sts != EOK) {
        /*
         * Can fail with E_TOO_MANY_BF_SETS or EHANDLE_OVF
         */
        RAISE_EXCEPTION( sts );
    }

    /* The domain flush routine here will wait for all I/O on this
     * domain to finish, so we must prevent new reads and non-allocating 
     * writes from racing with us or we can wait here for an indeterminate
     * amount of time while holding an exclusive transaction.  To do this,
     * we synchronize with read, write, and other clonefsets using the
     * domain's BFD_DOMAIN_FLUSH_IN_PROGRESS flag.
     */
    mutex_lock( &dmnP->mutex );
    if ( dmnP->dmnFlag & BFD_DOMAIN_FLUSH_IN_PROGRESS ) {
        /* The domain flush is being completed for us by a racing clonefset. */
        assert_wait((vm_offset_t)(&dmnP->dmnFlag), FALSE);
        mutex_unlock( &dmnP->mutex );
        thread_block();

    } else {
        dmnP->dmnFlag |= BFD_DOMAIN_FLUSH_IN_PROGRESS;
        mutex_unlock( &dmnP->mutex );

        bs_bfdmn_flush( dmnP );
        (void) bs_bfdmn_flush_sync( dmnP );

        mutex_lock( &dmnP->mutex );
        dmnP->dmnFlag &= ~BFD_DOMAIN_FLUSH_IN_PROGRESS;
        thread_wakeup((vm_offset_t)(&dmnP->dmnFlag));
        mutex_unlock( &dmnP->mutex );
    }

    /*
     * Copy the orig set's tagdir to the clone.  Do this before linking
     * the sets together since this is the most likely thing to fail (out
     * of disk space!).
     */

    sts = clone_tagdir( origSetp, cloneSetp, origDirParamsp->numPages, ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * We need to reload the tagdir info now that it clone has a tagdir
     */

    sts = tagdir_get_info( cloneSetp, &cloneSetp->tagUnInPg,
                           &cloneSetp->tagUnMpPg, &cloneSetp->tagFrLst,
                           &cloneSetp->bfCnt );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_clone: tagdir_get_info failed, return code = %d", sts );
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    /*
     ** Clone the set attributes and link the new set into the clone list.
     */

    sts = bmtr_get_rec_n_lk(origSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            (void *) origSetAttrp,
                            sizeof( bsBfSetAttrT ),
                            BMTR_LOCK );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_clone: bmtr_get_rec_n_lk (1) failed, return code = %d", sts );
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    sts = bmtr_get_rec_n_lk(cloneSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            (void *) cloneSetAttrp,
                            sizeof( bsBfSetAttrT ),
                            BMTR_LOCK );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_clone: bmtr_get_rec_n_lk (2) failed, return code = %d", sts );
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    mutex_lock( &dmnP->mutex );

    origSetAttrp->cloneCnt++;
    origSetp->cloneCnt++;
    origSetAttrp->numClones++;
    origSetp->numClones++;

    cloneSetAttrp->cloneId = origSetAttrp->cloneCnt;
    cloneSetp->cloneId     = origSetAttrp->cloneCnt;

    cloneSetAttrp->origSetTag = origSetp->dirTag;
    cloneSetp->origSetp       = origSetp;

    /*
     ** Link new clone into linked list of clones.
     */
    MS_SMP_ASSERT(BS_BFTAG_EQL(origSetAttrp->nextCloneSetTag, NilBfTag));
    cloneSetAttrp->nextCloneSetTag = origSetAttrp->nextCloneSetTag;
    MS_SMP_ASSERT(cloneSetp->cloneSetp == NULL);
    origSetAttrp->nextCloneSetTag  = cloneSetp->dirTag;

    origSetp->cloneSetp = cloneSetp;

    origSetp->bfsOpPend = 0;
    if ( origSetp->bfsHoldWait > 0 ) {
        thread_wakeup( (vm_offset_t)&origSetp->bfsHoldWait );
    }

    mutex_unlock( &dmnP->mutex );

    cloneSetAttrp->fragBfTag = origSetAttrp->fragBfTag;
    cloneSetp->fragBfTag     = origSetp->fragBfTag;
    bcopy(origSetAttrp->fsContext, cloneSetAttrp->fsContext,
          BS_FS_CONTEXT_SZ*4);
    bcopy(origSetAttrp->fragGrps, cloneSetAttrp->fragGrps, BF_FRAG_MAX*8);
    bcopy(origSetp->fragGrps, cloneSetp->fragGrps, BF_FRAG_MAX*8);

    cloneSetAttrp->state = BFS_ODS_VALID;

    sts = bmtr_update_rec_n_unlk( cloneSetp->dirBfAp,
                                  BSR_BFS_ATTR,
                                  cloneSetAttrp,
                                  sizeof( bsBfSetAttrT ),
                                  ftxH,
                                  BMTR_UNLOCK );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_clone: bmtr_update_rec_n_unlk (1) failed, return code = %d", sts );
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    sts = bmtr_update_rec_n_unlk( origSetp->dirBfAp,
                                  BSR_BFS_ATTR,
                                  origSetAttrp,
                                  sizeof( bsBfSetAttrT ),
                                  ftxH,
                                  BMTR_UNLOCK );
    if (sts != EOK) {
        domain_panic(dmnP, "bs_bfs_clone: bmtr_update_rec_n_unlk (2) failed, return code = %d", sts );
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    /*
     ** The clone set must be open the same number of times as
     ** the orig set in order for bs_bfs_close() to work properly.
     */

    while( cloneSetp->fsRefCnt < origSetp->fsRefCnt ) {

        sts = bfs_access( &cloneSetp, *cloneSetId, BFS_OP_DEF, ftxH );
        /*
         ** These calls to bfs_access should not fail since they will only
         ** bump reference counts.  If for some reason one did, by calling
         ** RAISE_EXCEPTION, ftx_fail() will cause a domain_panic.  ftx_fail
         ** will domain_panic because there are pinned records.
         */
        MS_SMP_ASSERT(sts == EOK);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    BFSETTBL_UNLOCK( dmnP )

    ftx_done_n( ftxH, FTA_BFS_CLONE );

    /*
     * Finally, close both sets.  This compensates for opening the
     * original set at the beginning of this routine.  Since they
     * must be open the same number times we close both here.
     */

    BFSETTBL_LOCK_WRITE( dmnP )

    bfs_close( cloneSetp, ftxH );
    bfs_close( origSetp, ftxH );

    BFSETTBL_UNLOCK( dmnP )

    ms_free( origDirParamsp );
    ms_free( cloneSetAttrp );
    ms_free( origSetAttrp );

    return EOK;

HANDLE_EXCEPTION:

    /*
     * transaction failure may release BfSetTblLock
     */
    if (lkLocked && lock_holder(&dmnP->BfSetTblLock.lock)) {
        BFSETTBL_UNLOCK( dmnP )
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if ( BFSET_VALID(origSetp) ) {
        origSetp->bfsOpPend = 0;
        BFSETTBL_LOCK_WRITE( dmnP )
        bfs_close( origSetp, ftxH );
        BFSETTBL_UNLOCK( dmnP )
    }

    if ( BFSET_VALID(cloneSetp) ) {
        BFSETTBL_LOCK_WRITE( dmnP )
        bfs_close( cloneSetp, ftxH );
        BFSETTBL_UNLOCK( dmnP )
    }

    if (origDirParamsp != NULL) {
        ms_free( origDirParamsp );
    }
    if (cloneSetAttrp != NULL) {
        ms_free( cloneSetAttrp );
    }
    if (origSetAttrp != NULL) {
        ms_free( origSetAttrp );
    }

    return sts;
}

void
bs_bf_out_of_sync(

    bfAccessT *cloneap,
    ftxHT parentFtxH
    )
{
    bsBfAttrT *bfAttrp;
    rbfPgRefHT attrPgH;
    ftxHT ftxH;
    statusT sts;

    MS_SMP_ASSERT(cloneap->bfSetp->cloneId != BS_BFSET_ORIG);

    if (!cloneap->outOfSyncClone)
    {

        /*
         * Mark the clone as out of sync with the original.  This
         * will prevent anyone from accessing this bitfile
         * (refpg() will return an error).
         */

        /*
         * We need to start an ftx here incase a caller 
         * needs to fail. We are pinning a record here
         * so this could cause an ftx_fail_2: dirty page not allowed.
         * By putting this in a sub ftx we avoid this. Also
         * we are not supplying an undo routine so that means
         * that if we are failed out record may or may not
         * get out to disk. We don't care either way. If it
         * gets out, great we are officially out-of-sync.
         * if not we will catch it next time we attempt to
         * COW.
         */


        sts = FTX_START_N( FTA_BS_COW, &ftxH, parentFtxH,
                           cloneap->dmnP, 4 );
        if (sts != EOK){
            domain_panic(parentFtxH.dmnP, 
              "bs_bf_out_of_sync: FTX_START_N failed, return code = %d", 
               sts );
            return;
        }
        cloneap->outOfSyncClone = TRUE;

        sts = bmtr_get_rec_ptr( cloneap,
                                ftxH,
                                BSR_ATTR,
                                sizeof( *bfAttrp ),
                                1 /* pin pg */,
                                (void *) &bfAttrp,
                                &attrPgH,
                                NULL );
        if (sts != EOK) {
            domain_panic(ftxH.dmnP, 
               "bs_bf_out_of_sync: bmtr_get_rec_ptr failed, return code = %d", 
               sts );
            ftx_fail(ftxH);
            return;
        }

        RBF_PIN_FIELD( attrPgH, bfAttrp->outOfSyncClone );
        bfAttrp->outOfSyncClone = TRUE;

        ftx_done_n( ftxH, FTA_BS_COW );
    }

}


void
bs_bfs_out_of_sync(

    bfSetT *bfSetp,
    ftxHT ftxH
    )
{
    bsBfSetAttrT *setAttrp;
    rbfPgRefHT fsetPgH;
    statusT sts;

    if ( ! BFSET_VALID(bfSetp) ) {
        ADVFS_SAD1("bs_bfs_out_of_sync: E_BAD_BF_SET_POINTER", (long)bfSetp);
    }

    sts = bmtr_get_rec_ptr( bfSetp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( *setAttrp ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP, "bs_bfs_out_of_sync: bmtr_get_rec_ptr failed, return code = %d", sts );
        return;
    }

    RBF_PIN_FIELD( fsetPgH, setAttrp->flags );
    setAttrp->flags |= BFS_OD_OUT_OF_SYNC;

    mutex_lock(&bfSetp->bfSetMutex);
    bfSetp->bfSetFlags |= BFS_OD_OUT_OF_SYNC;
    mutex_unlock(&bfSetp->bfSetMutex);
    return;
}

/*
 * bs_bfs_get_set_id
 *
 * Given a bitfile-set desc pointer this routine will return the set's
 * ID.  If the BF-set desc pointer is invalid then we die.
 *
 * NOTE:  No locking is done.
 */

void
bs_bfs_get_set_id(
    bfSetT *bfSetp,     /* in - bitfile-set descriptor pointer */
    bfSetIdT *bfSetId   /* out - bitfile set id */
    )
{
    if ( ! BFSET_VALID(bfSetp) ) {
        ADVFS_SAD1("bs_bfs_get_set_id: E_BAD_BF_SET_POINTER", (long)bfSetp);
    }

    *bfSetId = bfSetp->bfSetId;
}

/*
 * bs_bfs_get_clone_info
 *
 * Given a bitfile-set desc pointer this routine will return the set's
 * clone ID and number times it has been cloned.
 * If the BF-set desc pointer is invalid then we die!
 *
 * NOTE:  No locking is done; doesn't seem necessary until we support
 * deletion of bitfile-sets.
 */

void
bs_bfs_get_clone_info(
    bfSetT *bfSetp,     /* in - bitfile-set desc pointer */
    uint32T *cloneId,   /* out - bitfile set clone id */
    uint32T *cloneCnt   /* out - bitfile set clone count */
    )
{
    if ( ! BFSET_VALID(bfSetp) ) {
        ADVFS_SAD1("bs_bfs_get_clone_info: E_BAD_BF_SET_POINTER", (long)bfSetp);
    }

    *cloneId = bfSetp->cloneId;
    *cloneCnt = bfSetp->cloneCnt;
    return;
}

/*
 * bs_bfs_add_root
 *
 * Special interface to add the root bitfile-set to the bitfile-set
 * table.
 *
 * Returns status of bfs_alloc().
 */

statusT
bs_bfs_add_root(
    bfSetIdT bfSetDirId,   /* in - bitfile-set id */
    domainT *dmnP,         /* in - BF-set's domain's pointer */
    bfSetT **retBfSetDirp  /* out - pointer to BF-set's descriptor */
    )
{
    statusT sts;
    bfSetT *bfSetp;

    BFSETTBL_LOCK_WRITE( dmnP )

    sts = bfs_alloc( bfSetDirId, dmnP, &bfSetp );

    if (sts == EOK) {
        bfSetp->state = BFS_READY;
        *retBfSetDirp = bfSetp;
    }

    BFSETTBL_UNLOCK( dmnP )
    return sts;
}

/*
 * bs_bfs_switch_root_tagdir
 *
 * Special interface to switch the root bitfile-set's tag directory.  This
 * function closes the old tag directory and opens the new tag directory.
 *
 * This function assumes that the caller has exclusive access to the bitfile
 * set descriptor.
 */

void
bs_bfs_switch_root_tagdir (
                           domainT *domain,  /* in */
                           bfTagT newTag  /* in */
                           )
{
    bfSetT *bfSetp;
    statusT sts;
    bfAccessT *dirbfap;

    bfSetp = domain->bfSetDirp;

    bs_invalidate_rsvd_access_struct(domain,
                                     domain->bfSetDirTag,
                                     bfSetp->dirBfAp);

    sts = bfm_open_ms(&dirbfap,
                      domain,
                      BS_BFTAG_VDI(newTag),
                      BFM_BFSDIR);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfs_switch_root_tagdir: bfm_open_ms() failed", sts );
    }
    bfSetp->dirBfAp = dirbfap;

    return;
}

/*
 * bs_bfset_activate
 *
 * Activates the bitfile set specified by bfSetName.
 *
 * Returns EOK, E_BAD_BF_SET_TBL, or the status returned by the
 * called subroutines.
 */

statusT
bs_bfset_activate(
    char *bfDmnName,    /* in - bitfile-set's domain file name */
    char *bfSetName,    /* in - bitfile-set name */
    u_long flag,        /* in - advfs related mount flags - defs in mount.h */
    bfSetIdT *bfSetId   /* out - bitfile-set's ID */
    )
{
    bfDomainIdT dmnId;
    domainT *dmnP;
    bfSetParamsT setParams;
    int dmnActive = FALSE, dmnOpen = FALSE;
    statusT sts;
    bfsQueueT *entry;
    bfSetT *bfSetP, *temp;

    sts = bs_bfdmn_tbl_activate( bfDmnName, flag, &dmnId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, dmnId, FALSE );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    /* 
     * Try the fast path to the in memory bfSetIdT structure if the bfSet is
     * mounted. We need to look through the associated filesets in the active
     * domain structure.  If we find the matching fileset, then we can grab 
     * the bfSetId from there. Otherwise find the bfSetIdT on the disk.
     */
    
    BFSETTBL_LOCK_READ(dmnP);
    entry = dmnP->bfSetHead.bfsQfwd;
    bfSetP = NULL;
    while (entry != &dmnP->bfSetHead) {
        temp = BFSET_QUEUE_TO_BFSETP(entry);
        if (!strcmp(temp->bfSetName, bfSetName) 
                && (temp->fsRefCnt > 0) && (temp->fsnp != NULL)) {
            bfSetP = temp;
            break;
        }
        entry = entry->bfsQfwd;
    }
    BFSETTBL_UNLOCK(dmnP);

    if (bfSetP != NULL) {
        /* Found the bfSetId in memory already.  Let's make a copy
           of it for ourselves. */

        *bfSetId = bfSetP->bfSetId;
    } else { 
        /*
         * Get the bfSetId from the disk.
         */
        
        sts = bs_bfs_find_set( bfSetName, dmnP, 
                           flag & (M_LOCAL_ROOT | M_GLOBAL_ROOT), &setParams);

        if (sts != EOK) {
            /* The set doesn't exist! */
            goto _error;
        }

        *bfSetId = setParams.bfSetId;
    }

    bs_domain_close( dmnP );

    return EOK;

_error:

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, flag );
    }

    return( sts );
}

/*
 * bs_bfs_get_info
 *
 * This routine may be used to scan thru the bitfile sets in a
 * domain and get each set's attributes.
 *
 * The routine uses the parameter 'nextSetIdx' to determin which
 * set's info is desired.  If it is 0 (zero) then the routine starts
 * with the set whose tag is 1.  Otherwise, it uses the nextSetIdx
 * as the set's tag (which means that the caller can ask for the
 * info for a specific set by passing the correct tag; note that if
 * an invalid tag is passed this routine will most likely return
 * the attributes of the next valid set).
 *
 * Whenever the routine is successful it will update 'nextSetIdx' to
 * the next tag.  This way the caller can initialize 'nextSetIdx' to
 * zero and keep calling this routine to get the attributes of all
 * sets in the domain.
 */

statusT
bs_bfs_get_info(
    uint32T *nextSetIdx,       /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    domainT *dmnP,             /* in - domain pointer */
    uint32T *userId            /* out - bfset user id */
    )
{
    bfTagT setTag;
    statusT sts;
    bfMCIdT mcid;
    vdIndexT vdi;
    bfSetT *setp;
    bfSetT *bfSetp;
    bfAccessT *bfap;
    bsBfSetAttrT *setAttrp = NULL;
    bsQuotaAttrT quotaAttr;
    struct vnode *nullvp = NULL;
    int bitfile_opened = FALSE;

    if (dmnP == NULL){
        RAISE_EXCEPTION(EBAD_DOMAIN_POINTER);
    }

    setAttrp = (bsBfSetAttrT *) ms_malloc( sizeof( bsBfSetAttrT ));
    if (setAttrp == NULL) {
        RAISE_EXCEPTION(ENO_MORE_MEMORY);
    }

    bfSetp = dmnP->bfSetDirp;

    setTag.seq = 0;
    setTag.num = (*nextSetIdx == 0) ? 1 : *nextSetIdx;

    sts = tagdir_lookup2( dmnP->bfSetDirp, &setTag, &mcid, &vdi );
    *nextSetIdx = setTag.num + 1;

    if (sts == ENO_SUCH_TAG) {
        RAISE_EXCEPTION(E_NO_MORE_SETS);

    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /*
     * The set exists. Open it and get its attributes.
     */

    sts = bs_access( &bfap, setTag, dmnP->bfSetDirp, FtxNilFtxH, 0,
                     NULLMT, &nullvp );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    bitfile_opened = TRUE;

    sts = bmtr_get_rec(bfap,
                       BSR_BFS_ATTR,
                       (void *) setAttrp,
                       sizeof( bsBfSetAttrT ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if ( !BS_UID_EQL(setAttrp->bfSetId.domainId, dmnP->domainId) ) {
        if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
            if ( BS_UID_EQL(setAttrp->bfSetId.domainId, dmnP->dualMountId) ) {
                setAttrp->bfSetId.domainId = dmnP->domainId;
            }
            else {
                RAISE_EXCEPTION(ENO_SUCH_DOMAIN);
            }
        }
        else {
            RAISE_EXCEPTION(ENO_SUCH_DOMAIN);
        }
    }

    sts = bmtr_get_rec(bfap,
                       BSR_BFS_QUOTA_ATTR,
                       (void *) &quotaAttr,
                       sizeof( quotaAttr ) );
    if (sts != EOK) {
        bzero( &quotaAttr, sizeof( quotaAttr ) );
        quotaAttr.quotaStatus = setAttrp->oldQuotaStatus;
    }

    *bfSetParams                 = bsNilBfSetParams;
    bfSetParams->bfSetId         = setAttrp->bfSetId;
    bfSetParams->nextCloneSetTag = setAttrp->nextCloneSetTag;
    bfSetParams->origSetTag      = setAttrp->origSetTag;
    bfSetParams->cloneId         = setAttrp->cloneId;
    bfSetParams->cloneCnt        = setAttrp->cloneCnt;
    bfSetParams->numClones       = setAttrp->numClones;
    bfSetParams->mode            = setAttrp->mode;
    bfSetParams->uid             = setAttrp->uid;
    bfSetParams->gid             = setAttrp->gid;
    bfSetParams->bfSetFlags      = setAttrp->flags;
    bfSetParams->quotaStatus     = quotaAttr.quotaStatus;
    bfSetParams->blkHLimit       = quotaAttr.blkHLimitLo;
    bfSetParams->blkSLimit       = quotaAttr.blkSLimitLo;
    bfSetParams->blkHLimit      |= (long) quotaAttr.blkHLimitHi << 32L;
    bfSetParams->blkSLimit      |= (long) quotaAttr.blkSLimitHi << 32L;
    bfSetParams->fileHLimit      = quotaAttr.fileHLimitLo;
    bfSetParams->fileSLimit      = quotaAttr.fileSLimitLo;
    bfSetParams->blkTLimit       = quotaAttr.blkTLimit;
    bfSetParams->fileTLimit      = quotaAttr.fileTLimit;
    bfSetParams->filesUsed       = 0;
    bfSetParams->blksUsed        = 0;
    bfSetParams->dmnAvailFiles   = bs_get_avail_mcells( dmnP );
/*
 * advfs fix
 *
 * The fields on the left are declared long while the fields on the
 * right a supposed to be uint64 which is a quad.  SOmeone please fix
 * this.  For now we just use the low 32 bits.
 */
#ifdef __arch64__
    bfSetParams->dmnTotalBlks    = dmnP->totalBlks;
    bfSetParams->dmnAvailBlks    = dmnP->freeBlks;
#else
    bfSetParams->dmnTotalBlks    = dmnP->totalBlks.low;
    bfSetParams->dmnAvailBlks    = dmnP->freeBlks.low;
#endif
    setp = bs_bfs_lookup_desc( setAttrp->bfSetId );

    if ( BFSET_VALID(setp) ) {
        if (setp->fsnp != NULL) {
            struct fileSetNode *fsnp = setp->fsnp;

           /*
            * If bfSetT for a fileset is valid,
            * override the first value from disk with
            * in-memory value because the changed value
            * in-memory should be applied immediately
            * for a proper operation(s).
            */
            bfSetParams->bfSetFlags = setp->bfSetFlags;

            bfSetParams->quotaStatus = fsnp->quotaStatus;


            if ((fsnp->quotaStatus & QSTS_QUOTA_ON) &&
                (fsnp->quotaStatus & QSTS_FS_QUOTA_MT)) {
                bfSetParams->filesUsed = fsnp->filesUsed;
                bfSetParams->blksUsed  = fsnp->blksUsed;

            } else {
                bfSetParams->filesUsed = setp->bfCnt;
            }

        } else {
            bfSetParams->filesUsed = setp->bfCnt;
        }
    }

    bcopy( setAttrp->setName, bfSetParams->setName, BS_SET_NAME_SZ );
    bcopy( setAttrp->fsContext, bfSetParams->fsContext, BS_FS_CONTEXT_SZ * 4 );

    *userId = 0;

    bs_close(bfap, 0);

    ms_free( setAttrp );
    return EOK;

HANDLE_EXCEPTION:
    if ( bitfile_opened ) {
        bs_close(bfap, 0);
    }

    if (setAttrp != NULL) {
        ms_free( setAttrp );
    }
    return sts;
}

/*
 * bs_bfs_find_set
 *
 * Given a bitfile set name this routine will determine if a
 * set by that name exists in the domain.
 */

statusT
bs_bfs_find_set(
    char *setName,             /* in - name of set to find */
    domainT *dmnP,             /* in - domain pointer */
    u_long doingRoot,          /* in - flag */
    bfSetParamsT *setParams    /* out - the bitfile-set's parameters */
    )
{
    statusT sts;
    uint32T setIdx;
    int done = FALSE;
    uint32T t1;                /* unused output param */

    setIdx = 0;

    do {
        sts = bs_bfs_get_info( &setIdx, setParams, dmnP, &t1 );
        if (sts == EOK) {
            if (strcmp( setName, setParams->setName ) == 0 ||
                doingRoot) {
                done = TRUE;
            }
        }
    } while (!done && (sts == EOK));

    if (sts != EOK) {
        return E_NO_SUCH_BF_SET;
    } else {
        return EOK;
    }
}

/****************************************************************************
 **  Bitfile cloning and copy on write routines
 ***************************************************************************/
/*
 * new_clone_mcell - for use by clone code.  This is undoable.
 */
statusT
new_clone_mcell(
               bfMCIdT* bfMCIdp,        /* out - ptr to mcell id */
               domainT* dmnP,           /* in - domain ptr */
               ftxHT parFtx,            /* in - parent ftx */
               vdIndexT* vdIndex,       /* out - new vd index */
               bsBfAttrT* bfAttrp,      /* in - bitfile attributes ptr */
               bfSetT *bfSetp,          /* in - bitfile's bf set desc ptr */
               bfTagT newtag,           /* in - tag of new bitfile */
               bsInMemXtntT *oxtntp     /* in - ptr to orig extent map */
               )
{
    statusT sts;
    ftxHT ftx;
    mcellUIdT mcelluid;

    dmnP = parFtx.dmnP;

    sts = FTX_START_META( FTA_NULL, &ftx, parFtx, dmnP, 1 );
    if (sts != EOK) {
        ADVFS_SAD1("new_clone_mcell: ftx_start failed", sts );
    }

    bs_bfs_get_set_id( bfSetp, &mcelluid.ut.bfsid );

    mcelluid.ut.tag = newtag;

    sts = new_mcell( ftx, &mcelluid, bfAttrp, dmnP, oxtntp );
    if ( sts != EOK ) {
        ftx_fail( ftx );
        return sts;
    }

    *bfMCIdp = mcelluid.mcell;
    *vdIndex = mcelluid.vdIndex;

    ftx_done_u( ftx, FTA_BS_CRE_MCELL_V1, sizeof(mcelluid), &mcelluid );

    return EOK;
}

/*
 *
 */
opxT new_clone_mcell_undo_opx;

void
new_clone_mcell_undo_opx(
                    ftxHT ftxH,
                    int opRecSz,
                    void* opRecp
                    )
{
    mcellUIdT* undoRp = (mcellUIdT*)opRecp;
    domainT *dmnP;
    vdT* vdp;
    rbfPgRefHT pgref;
    bsMPgT* bmtpgp;
    statusT sts;
    bsMCT* mcp;
    bsBfAttrT* odattrp;

    if (opRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("new_clone_mcell_undo_opx: bad root done record size");
    }

    dmnP = ftxH.dmnP;

    if (((unsigned)undoRp->vdIndex > BS_MAX_VDI) ||
        ((vdp = VD_HTOP(undoRp->vdIndex, dmnP)) == 0 )) {
        ADVFS_SAD1( "new_clone_mcell_undo_opx: vd index no good", undoRp->vdIndex );
    }

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vdp->bmtp,
                    undoRp->mcell.page, BS_NIL, ftxH );
    if (sts != EOK) {
        domain_panic(dmnP,
            "new_clone_mcell_undo_opx: failed to access bmt pg %d, err %d",
            undoRp->mcell.page, sts );
        return;
    }

    if ( bmtpgp->pageId != undoRp->mcell.page ) {
        domain_panic(dmnP, "new_clone_mcell_undo_opx: got bmt page %d instead of %d",
                   bmtpgp->pageId, undoRp->mcell.page );
        return;
    }

    mcp = &bmtpgp->bsMCA[undoRp->mcell.cell];

    /* find attributes, set state invalid */

    odattrp = bmtr_find( mcp, BSR_ATTR, dmnP);
    if ( !odattrp ) {
        ADVFS_SAD0("new_clone_mcell_undo_opx: can't find bf attributes");
    }

    RBF_PIN_FIELD( pgref, odattrp->state );
    odattrp->state = BSRA_INVALID;

    sts = dealloc_mcells (dmnP, vdp->vdIndex, undoRp->mcell, ftxH);
    if (sts != EOK) {
        domain_panic(dmnP, "new_clone_mcell_undo_opx: dealloc_mcells() failed - sts %d", sts);
        return;
    }
}

/*
 * init_crmcell_opx
 *
 * initialize mcell create agent.
 *
 * No error return.
 */

init_crmcell_opx()
{
    statusT sts;

    sts = ftx_register_agent_n(FTA_BS_CRE_MCELL_V1,
                             &new_clone_mcell_undo_opx,
                             0,
                             0
                             );
    if (sts != EOK) {
        ADVFS_SAD1( "init_crmcell_opx:register failure", sts );
    }
}

/*
 * clone
 *
 * Recoverable bitfile clone function
 *
 * It is assumed that the bitfile's are already accessed.
 *
 * Returns possible error from select_vd, init_new_mcell
 * and tagdir_insert_tag. Or returns EOK.
 *
 * NOTE:  Currently this routine is meant to be a subroutine of bs_cow()
 * and not a general function.
 */

static statusT
clone(
    bfAccessT *origBfAp,     /* in - ptr to orig bitfile's access struct */
    bfAccessT *cloneBfAp,    /* in - ptr to clone bitfile's access struct */
    ftxHT parentFtx          /* in - transaction handle */
    )
{
    domainT *dmnP;
    vdIndexT vdIndex;
    bfMCIdT newMCId;
    statusT sts;
    ftxHT ftxH;
    int initState = FALSE;
    bfSetT *origSetp, *cloneSetp;
    bsBfAttrT bfAttr, *origBfAttrp;
    tagInfoT tagInfo;
    bfMCIdT origMCId;
    vdIndexT origVdIndex;
    unLkActionT unlkAction = UNLK_NEITHER;
    rbfPgRefHT attrPgH;
    int ftxStarted = FALSE;
    int cleanMcell=0,cleanXtnts=0;

    /*----------------------------------------------------------------------*/

    dmnP = cloneBfAp->dmnP;
    origSetp = origBfAp->bfSetp;
    cloneSetp = cloneBfAp->bfSetp;

    /*
     * Set the bfAccess object for the to-be-created clone into
     * the INIT_TRANS state.  This serializes us with
     * other threads that could be attempting to access this tag.
     *
     * NOTE: that there may already be threads that have
     * accessed this bitfile.  However, they will block on the cow_lk
     * if they try to pin or ref pgs.
     *
     * NOTE: we don't need to synchronize with rbf_delete() (which
     * also sets the state to INIT_TRANS) because clones are only
     * deleted in bs_bfs_delete() which can't be happening since
     * we're in a transaction (bs_bfs_delete() uses exclusive
     * transactions for synchronization).
     *
     * The whole point is to not allow any other thread to 'see'
     * the clone bitfile while we're creating new on-disk metadata
     * for it.
     */

    mutex_lock( &cloneBfAp->bfaLock );
    lk_set_state( &cloneBfAp->stateLk, ACC_INIT_TRANS );
    mutex_unlock( &cloneBfAp->bfaLock );
    initState = TRUE;

    sts = FTX_START_N( FTA_BS_BFS_CLONE_V1, &ftxH, parentFtx, dmnP, 4 );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    ftxStarted = TRUE;

    /* set ftxid into transition field of ds attributes */

    sts = bmtr_get_rec(origBfAp,
                       BSR_ATTR,
                       &bfAttr,
                       sizeof(bfAttr));
    if (sts != EOK) {
      RAISE_EXCEPTION(sts);
    }
    if ( bfAttr.bfPgSz == 0 ) {
        /* In the future this could be expanded to check_BSR_ATTR_rec */
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    bfAttr.state = BSRA_VALID;
    bfAttr.transitionId = get_ftx_id( ftxH );
    bfAttr.cloneId = cloneSetp->cloneId;
    bfAttr.cloneCnt = cloneSetp->cloneCnt;

    /* A side effect exists in cloning a file that has 
     * preallocated storage. The preallocated storage gets
     * cloned but the file_size prohibits the clone from 
     * accessing it. This is correct behavior but inefficient
     * use of storage. Unfortunately if we used the file_size
     * then truncate would misbehave (as well as some meta data
     * files do not keep file_size up to date). It would be a nice
     * optimization to not clone preallocated storage
     */

    bfAttr.maxClonePgs = origBfAp->nextPage;

    FTX_LOCKWRITE( &cloneBfAp->mcellList_lk, ftxH )

    /* Get a new primary mcell for the clone and init it */

    sts = new_clone_mcell( &newMCId,
                           dmnP,
                           ftxH,
                           &vdIndex,
                           &bfAttr,
                           cloneBfAp->bfSetp,
                           cloneBfAp->tag,
                           &origBfAp->xtnts );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * Get previous clone's tagdir entry for the undo opx.
     */

    sts = tagdir_lookup( cloneBfAp->bfSetp,
                         &cloneBfAp->tag,
                         &origMCId,
                         &origVdIndex );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * Copy the BMT records from the orig to the clone.
     */

    sts = bmtr_clone_recs( origMCId,
                           origVdIndex,
                           newMCId,
                           vdIndex,
                           vdIndex, /* other caller need this parameter */
                           cloneBfAp->dmnP,
                           ftxH );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* update tagdir entry for the cloned tag */

    tagInfo.bfSetp = cloneBfAp->bfSetp;
    tagInfo.dmnP = dmnP;
    tagInfo.tag = cloneBfAp->tag;
    tagInfo.vdIndex = vdIndex;
    tagInfo.bfMCId = newMCId;
    tagInfo.ftxH = ftxH;

    sts = tagdir_stuff_tagmap( &tagInfo );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * The clone has new mcell(s) and an empty extent map.  Remap it.
     * Note that we don't need to lock anything here (except the cow_lk
     * which we already have) since any other pinpg or refpg will be
     * be blocked on the cow_lk.
     */

    cloneBfAp->primMCId = newMCId;
    cloneBfAp->primVdIndex = vdIndex;
    cloneBfAp->origAccp = origBfAp;

    cleanMcell = 1;

    sts = bs_map_bf(cloneBfAp, BS_REMAP, NULL);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }
    
    cleanXtnts = 1;
    
    if ( origBfAp->xtnts.type == BSXMT_STRIPE ) {
        sts = str_stripe_clone(cloneBfAp, &origBfAp->xtnts, ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    /*
     * Update the orig bitfile to indicate that we've copied the
     * the mcell records to the clone.
     */

    sts = bmtr_get_rec_ptr( origBfAp,
                            ftxH,
                            BSR_ATTR,
                            sizeof( *origBfAttrp ),
                            1 /* pin pg */,
                            (void *) &origBfAttrp,
                            &attrPgH,
                            NULL );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    RBF_PIN_FIELD( attrPgH, origBfAttrp->cloneCnt );
    origBfAttrp->cloneCnt = origSetp->cloneCnt;
    origBfAp->cloneCnt = origSetp->cloneCnt;

    /*
     * don't undo this sub ftx if parent fails.  don't want some
     * undo routine to require copy on write (not allowed in undo).
     */
    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
    ftx_done_n( ftxH, FTA_BS_BFS_CLONE_V1 );

    mutex_lock( &cloneBfAp->bfaLock );
    unlkAction = lk_set_state( &cloneBfAp->stateLk, ACC_VALID );
    lk_signal( unlkAction, &cloneBfAp->stateLk );
    mutex_unlock( &cloneBfAp->bfaLock );

    return( EOK );

HANDLE_EXCEPTION:

    if ( ftxStarted ) {
        ftx_fail( ftxH );
    }

    if (initState) {
        mutex_lock( &cloneBfAp->bfaLock );
        unlkAction = lk_set_state( &cloneBfAp->stateLk, ACC_VALID );
        lk_signal( unlkAction, &cloneBfAp->stateLk );
        mutex_unlock( &cloneBfAp->bfaLock );
    }
    if (cleanMcell) {
        cloneBfAp->primMCId = origMCId;
        cloneBfAp->primVdIndex = origVdIndex;
    }
    if (cleanXtnts) {
        
        /* This was set during the call to bs_map_bf.
         * We must reset this otherwise we will think
         * that our primary mcell is not the originals
         */

        cloneBfAp->cloneId=0;
        x_dealloc_extent_maps(&cloneBfAp->xtnts);
    }

    return sts;
}

/*
 * bs_have_clone
 *
 * Returns true if the tag has a clone in the supplied bitfile set desc.
 * This means that the set desc pointer must be non-nil and the tag can
 * be found in the set's tagdir.
 *
 * The caller must have an outstanding transaction (called ftx_start())
 * before calling this routine.
 */

int
bs_have_clone(
    bfTagT tag,         /* in - bitfile tag */
    bfSetT *cloneSetp,  /* in - clone bitfile set's desc pointer */
    ftxHT ftxH          /* in - ftx handle */
    )
{
    bfMCIdT mcid;
    vdIndexT  vdi;

    if (ftxH.hndl == 0) {
        ADVFS_SAD0( "bs_have_clone: must have a transaction" );
    }

    if ( !BFSET_VALID(cloneSetp) ) {
        return FALSE;
    }
    if (tagdir_lookup( cloneSetp, &tag, &mcid, &vdi ) != EOK) {
        return FALSE;
    }
    return TRUE;
}

void
print_out_of_sync_msg(
    bfAccessT *bfap,      /* in - bitfile access struct */
    bfSetT *bfSetp,       /* in - bitfile set descriptor */
    statusT insts
    )
{
    bsBfSetAttrT setAttr;
    bfSetT *cloneSetp;
    statusT sts;

    ms_uaprintf( "\n\nWARNING: AdvFS cannot copy-on-write data to a clone file.\n" );
    ms_uaprintf( "WARNING: encountered the following error: %s\n",
                BSERRMSG( insts ) );
    ms_uaprintf( "WARNING: do not continue using the clone fileset.\n" );

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &setAttr,
                       sizeof( setAttr ) );

    ms_uprintf( "WARNING: original file set: name = %s, id = %08x.%08x.%08x.%04x\n",
               (sts != EOK) ? "??" : setAttr.setName,
             bfSetp->bfSetId.domainId.tv_sec,
             bfSetp->bfSetId.domainId.tv_usec,
             bfSetp->bfSetId.dirTag.num,
             bfSetp->bfSetId.dirTag.seq );

    cloneSetp = bfSetp->cloneSetp;

    sts = bmtr_get_rec(cloneSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &setAttr,
                       sizeof( setAttr ) );

    ms_uprintf( "WARNING: clone file set: name = %s, id = %08x.%08x.%08x.%04x\n",
               (sts != EOK) ? "??" : setAttr.setName,
               cloneSetp->bfSetId.domainId.tv_sec,
               cloneSetp->bfSetId.domainId.tv_usec,
               cloneSetp->bfSetId.dirTag.num,
               cloneSetp->bfSetId.dirTag.seq );

    ms_uprintf( "WARNING: file id = %08x.%04x\n\n",
               bfap->tag.num, bfap->tag.seq );

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &setAttr,
                       sizeof( setAttr ) );

    aprintf("WARNING: original file set: name = %s, id = %08x.%08x.%08x.%04x\n",
            (sts != EOK) ? "??" : setAttr.setName,
            bfSetp->bfSetId.domainId.tv_sec,
            bfSetp->bfSetId.domainId.tv_usec,
            bfSetp->bfSetId.dirTag.num,
            bfSetp->bfSetId.dirTag.seq );

    cloneSetp = bfSetp->cloneSetp;

    sts = bmtr_get_rec(cloneSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &setAttr,
                       sizeof( setAttr ) );

    aprintf( "WARNING: clone file set: name = %s, id = %08x.%08x.%08x.%04x\n",
             (sts != EOK) ? "??" : setAttr.setName,
             cloneSetp->bfSetId.domainId.tv_sec,
             cloneSetp->bfSetId.domainId.tv_usec,
             cloneSetp->bfSetId.dirTag.num,
             cloneSetp->bfSetId.dirTag.seq );

    aprintf( "WARNING: file id = %08x.%04x\n\n",
             bfap->tag.num, bfap->tag.seq );
}

/*
** This routine "holds" a clone fileset stable against 3 different changes.
** It works with bs_bfs_clone, advfs_mountfs & bs_bfs_delete to prevent
** races with clone fileset creation, mounting and deletion.
**
** If the clone fileset does not exist, the bfsHoldCnt is bumped on the
** original fileset. bs_bfs_clone will not proceed while this count is non-zero.
** If bs_bfs_clone is in the process of making a clone fileset of the
** original fileset it will have set bfsOpPend on the original fileset.
** If waitFlg is set, hold_cloneset will wait for bs_bfs_clone to complete.
**
** If the clone set exists and is not mounted, hold_cloneset will bump
** bfsHoldCnt on the clone fileset. advfs_mountfs will wait while bfsHoldCnt
** is non-zero. If advfs_mountfs has started it will have set bfsOpPend
** on the clone fileset.
** If waitFlg is set, hold_cloneset will wait for advfs_mountfs to complete.
** waitFlg is only set from routines that can't tolerate the clone set
** coming into existence or can't tolerate a race with the clone set being
** mounted. The waitFlg can only be set in routines that have not started
** a root transaction, else this can deadlock.
**
** When an original fileset is opened its clone fileset is also opened.
** Some routines, like bs_cow, rely on the clone set being opened.
** However, the clone fileset can be deleted at any time. This routine
** cooperates with bs_bfs_delete to resolve the race. If bs_bfs_delete
** starts first, then this routine returns NULL. This tells callers
** that there is no clone fileset. If this routine starts first, it
** sets (increments) bfsHoldCnt and bs_bfs_delete waits for this flag
** to go to zero. If this routine starts first it returns the clone
** fileset of the original fileset that the passed in bfap belongs to.
** The caller must call release_cloneset to release the clone fileset
**
** This routine always bumps the bfsHoldCnt on either the original set
** or the cloneset.
** Every call here must be matched by a call to release_cloneset.
*/
bfSetT*
hold_cloneset( bfAccessT *bfap, int waitFlg )
{
    bfSetT *bfSetp = bfap->bfSetp;
    domainT *dmnP = bfSetp->dmnP;
    bfSetT *cloneSetp;

    MS_SMP_ASSERT(bfSetp->cloneId == BS_BFSET_ORIG);

    mutex_lock( &dmnP->mutex );  /* protects bfSetp->cloneSetp */

    if ( waitFlg ) {
        /* Check to see if the clone set is being created. */
        while ( bfSetp->bfsOpPend > 0 ) {
            /* Wait for bs_bfs_clone in progress. */
            MS_SMP_ASSERT(bfSetp->bfsOpPend == 1);
            bfSetp->bfsHoldWait++;
            thread_sleep( (vm_offset_t)&bfSetp->bfsHoldWait,
                          &dmnP->mutex.mutex, FALSE );
            bfSetp->bfsHoldWait--;
            mutex_lock( &dmnP->mutex );
        }
    }

    /* Clone fileset either exists or not. Hold it in that state. */
    cloneSetp = bfSetp->cloneSetp;
    if ( cloneSetp == NULL ) {
        /* No clone fileset. Keep it that way. */
        bfSetp->bfsHoldCnt++;
        mutex_unlock( &dmnP->mutex );
        return NULL;
    }

    if ( cloneSetp->bfsDelPend ) {
        /* The clone set is being deleted. Tell the caller it doesn't exist. */
        bfSetp->bfsHoldCnt++;
        /* Here BFS_OD_OUT_OF_SYNC is a flag to bs_bfs_delete. */
        /* If bs_bfs_delete fails then this cow was missed and */
        /* the clone fileset is out of sync. */
        cloneSetp->bfSetFlags |= BFS_OD_OUT_OF_SYNC;
        bfap->noClone = TRUE;
        mutex_unlock( &dmnP->mutex );
        return NULL;
    }

    if ( waitFlg ) {
        /* Check to see if the clone set is being mounted. */
        while ( cloneSetp->bfsOpPend > 0 ) {
            /* Wait for advfs_mountfs in progress. */
            MS_SMP_ASSERT(cloneSetp->bfsOpPend == 1);
            cloneSetp->bfsHoldWait++;
            thread_sleep( (vm_offset_t)&cloneSetp->bfsHoldWait,
                          &dmnP->mutex.mutex, FALSE );
            cloneSetp->bfsHoldWait--;
            mutex_lock( &dmnP->mutex );
        }
    }

    /*
    ** The clone fileset exists.
    ** It will be held against deletion, and mounting.
    ** It can become unmounted but we don't care about that.
    */
    cloneSetp->bfsHoldCnt++;
    mutex_unlock( &dmnP->mutex );
    MS_SMP_ASSERT(cloneSetp->bfSetMagic == SETMAGIC);

    return cloneSetp;
}

/*
** This is the partner routine for hold_cloneset.
** This routine decrements bfsHoldCnt.
** The caller decides whether the original or the clone set has had
** its bfsHoldCnt bumped by hold_cloneset. It passes in the set with
** the bumped bfsHoldCnt.
** If bfsHoldCnt is zero and a thread is waiting for bfsHoldCnt
** to go to zero, this routine will wakeup the sleeping thread.
*/
void
release_cloneset( bfSetT *bfSetp )
{
    mutex_lock( &bfSetp->dmnP->mutex );

    MS_SMP_ASSERT(bfSetp->bfsHoldCnt > 0);
    bfSetp->bfsHoldCnt--;

    if ( bfSetp->bfsHoldCnt == 0 && bfSetp->bfsOpWait ) {
        /* Only one thread can be waiting on bfsOpWait. */
        MS_SMP_ASSERT(bfSetp->bfsOpWait == 1);
        bfSetp->bfsOpWait = 0;
        thread_wakeup_one( (vm_offset_t)&bfSetp->bfsHoldCnt );
    }

    mutex_unlock( &bfSetp->dmnP->mutex );
}


/*
** Locks the clone's migTrunkLk shared (read).
** Locks the orig's cow_lk exclusive (write).
** Locks the orig's file_lock shared (read).
** May take CFS file xtnt map token.
** Seizes an active range covering the page range if the file is 
**   open for directIO.
** Returns 2 bit values: 
**   CLU_TOKEN_TAKEN is set if CFS file xtnt map token taken, clear if not.
**   FILE_LOCK_TAKEN is set if file lock was seized, clear if not.
*/
#define CLU_TOKEN_TAKEN 0x01
#define FILE_LOCK_TAKEN 0x10

static int
cow_get_locks( bfAccessT *cloneap, 
               ftxHT *parentFtxHA,
               bsPageT pg,
               bsPageT pgCnt,
               actRangeT **arp)
{
    bfAccessT *bfap = cloneap->origAccp;
    int token_taken = 0;
    int ftxStarted = FALSE;
    statusT sts;

    MS_SMP_ASSERT(*arp == NULL);

try_again:
    /* No need to do special locking for metadata files */
    if ( cloneap->dataSafety != BFD_FTX_AGENT  &&
         cloneap->dataSafety != BFD_FTX_AGENT_TEMPORARY) {
        /* If in a cluster, then provide mutual exclusion with cluster
         * directio reads on the clone.  These reads may have caused
         * extent maps for this file to be cached on other cluster
         * nodes.  This callout will cause all cluster nodes to
         * delete their cached versions and re-read the map after bs_cow_pg
         * has finished modifying the xtnt map.
         * If clu_clonextnt_lk is already locked then a caller higher in the
         * stack (eg fs_setattr) has already purged other node's cached
         * xtnt maps and acquired the tokens.
         */
        if ( clu_is_ready() && !lock_holder(&cloneap->clu_clonextnt_lk) ) {
            fsid_t fsid;
            BS_GET_FSID( cloneap->bfSetp, fsid );

            /* The clu_clonextnt_lk is not necessary here since the 
             * COW lock is obtained when requesting the clone xtnt maps.
             */

            /* Ok if this fails.  It just means no cluster nodes have the
             * extent map cached.  No one can get the extent map while we
             * hold the COW lock.
             */
            if ( cloneap->bfSetp->fsnp ) {
                /* The CFS DIO token should be in the lock (resource) */
                /* hierarchy and is higher than ftx or file_lock. */
                MS_SMP_ASSERT(parentFtxHA->hndl == 0);
                MS_SMP_ASSERT(!lock_holder(&VTOC(bfap->bfVp)->file_lock));
                if ( !CC_CFS_COW_MODE_ENTER(fsid, cloneap->tag) ) {
                    token_taken |= CLU_TOKEN_TAKEN; 
                    cloneap->cloneXtntsRetrieved = 0;
                }
            }
        }

        /* The file lock must be taken after the cluster token.  This lock
         * prevents the file open mode from changing from cached mode to
         * directIO mode while the cow is in progress.  This lock may
         * already be held if we are being called from bs_pinpg().
         */
        if (!lock_holder( &VTOC(bfap->bfVp)->file_lock) ) {
            FS_FILE_READ_LOCK_RECURSIVE( VTOC(bfap->bfVp) );
            token_taken |= FILE_LOCK_TAKEN; 
        }

        /* For files opened for directIO, we need to seize an active range
         * to guard the page range being cowed from racing readers/writers.
         * This range must be seized before the root transaction is started.
         */
        if ( bfap->bfVp->v_flag & VDIRECTIO ) {
            *arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));

            (*arp)->arStartBlock = pg * bfap->bfPageSz;
            (*arp)->arEndBlock   = ((pg + pgCnt) * bfap->bfPageSz) - 1;
            (*arp)->arState      = AR_COW;
            (*arp)->arDebug      = SET_LINE_AND_THREAD(__LINE__);

            insert_actRange_onto_list( bfap, *arp, VTOC(bfap->bfVp) );
        }
    }

    if ( parentFtxHA->hndl == 0 ) {
        sts = FTX_START_N( FTA_BS_COW_PG, parentFtxHA, FtxNilFtxH,
                           bfap->dmnP, 0 );
        if ( sts != EOK ) {
            ADVFS_SAD1( "bs_cow_pg: ftx_start (1) failed", sts );
        }

        ftxStarted = TRUE;
    }

    /*
     * Take the clone's migTruncLk to prevent races with
     * migration code which might be in the process of
     * migrating the clone file.  Note that for clone files,
     * the migTruncLk is taken AFTER a root transaction is
     * started.  This differs from the non-clone rule of 
     * taking the migTruncLk before starting a root transaction.
     * Also, relock the cow_lk of the original file to lock out
     * routines that update the original file or examine the clone
     * file's extent map.  We need to take the cow_lk after taking
     * the migTruncLk because the migrate paths use this order.
     */
    MIGTRUNC_LOCK_READ( &cloneap->xtnts.migTruncLk );
    COW_LOCK_WRITE( &bfap->cow_lk )

    /* Lets do the cluster clone shuffle:
     *
     * If we didn't get the token then a client could sneak
     * in here and get the token before we lock the cow_lk
     * This would be bad since we would not know to revoke their
     * token/xtntmaps after the COW is complete.
     * 
     * Each time a cluster client request a clone extent map
     * they will (while holding the cow_lk) set the
     * cloneXtntsRetrieved flag in the access structure. This
     * will tell us if they snuck in during the above window.
     *
     * This loop should generally only happen once. If the client
     * did sneak in then they will be holding the token
     * and when we loop we will wait for them to drop the token
     * breaking the loop.
     */

    /* NOTE if not in a cluster cloneap->cloneXtntsRetrieved is
     * always zero
     */

    if ( cloneap->cloneXtntsRetrieved &&
         !(token_taken & CLU_TOKEN_TAKEN) &&
         !lock_holder(&cloneap->clu_clonextnt_lk) )
    {
        cloneap->cloneXtntsRetrieved = 0;

        /* This is a Cathy Page special test. */
        if ( cloneap->bfSetp->fsnp == NULL ) {
            /*
             * If the clone fileset is not mounted then no on else could
             * have obtained the token and set cloneXtntsRetrieved.
             * cloneXtntsRetrieved was set long ago and not cleared.
             * Since we have the cow lock no one can get the extent map
             * now even if they have the token (and they don't since
             * the fileset is not mounted).
             */
            return token_taken;
        }

        COW_UNLOCK( &bfap->cow_lk );
        MIGTRUNC_UNLOCK( &cloneap->xtnts.migTruncLk );

        if ( token_taken & FILE_LOCK_TAKEN ) {
            FS_FILE_UNLOCK_RECURSIVE( VTOC(bfap->bfVp) );
            token_taken &= ~FILE_LOCK_TAKEN;
        }

        if ( *arp != NULL ) {
            mutex_lock(&bfap->actRangeLock);
            remove_actRange_from_list(bfap, *arp);
            mutex_unlock(&bfap->actRangeLock);
            ms_free(*arp);
            *arp = NULL;
        }

        if ( ftxStarted ) {
            ftx_quit(*parentFtxHA);
            parentFtxHA->hndl = 0;
            ftxStarted = FALSE;
        }

        goto try_again;
    }

    return token_taken;
}

/*
** Given the page to cow, try to cow the next 31 pages.
** Limit this based on the end of the orig file and maxClonePgs.
** If the original file is mark VDIRECTIO then try to cow the
** active range that covers pg. Limit this count based on
** previously cowed pages and maxClonePgs.
** Return 1 for an error, 0 for OK.
** Return the first page to cow, the number of pages to cow, and the
** start of a hole in the orig.
*/
static int
cow_get_page_range( bfAccessT *cloneap, bsPageT pg,
                    bsPageT *pgOffA, bsPageT *pgCntA , bsPageT *holePgA )
{
    bsPageT cnt;
    bsPageT arPg;
    bsPageT arCnt;
    bsPageT holePg;
    bsPageT nextPg;
    bsPageT lastPg;
    bfAccessT *bfap = cloneap->origAccp;
    statusT sts;

    /*
     * We need to copy the page to the clone.  Instead of copying
     * just one page we assume that one or more of the next few
     * pages will also need to be copied so we go ahead copy
     * them now.  
     */

    if ( bfap->nextPage > pg ) {
        cnt = MIN( bfap->nextPage - pg, 32 );
        cnt = MIN( cnt, cloneap->maxClonePgs - pg );
    } else {
        cnt = 0;
    }
        
    if ( cnt && bfap->bfVp && bfap->bfVp->v_flag & VDIRECTIO ) {
        /* We don't want to bring pages into the UBC that are
         * either: 1) outside of our active range (base), or 2)
         * won't be explicitly invalidated (base and CFS).
         *
         * COW as many pages as the active range covers. Note that
         * the active range may cover pages partially. It is OK to
         * COW these pages: the pinpg path worries explicitly
         * about two active ranges covering the same page and does
         * the right thing. The first thread that tries to pin the
         * page marks the bsBuf DIO_REMOVING. Any subsequent
         * would-be pinpg'ers wait. When the page has been flushed
         * and invalidated, the waiters are woken up and go look
         * up the page again.
         *
         * Note that bs_pinpg_direct() explicitly invalidates all
         * the pages in the active range.
         *
         * Note also that the COW code will stop COWing when it
         * hits a hole in the original, and we will not even get
         * to this point if the page has been COWed already.  
         *
         * The hope is that we will COW the whole active range in
         * one gulp (and improve our chances of getting a single
         * extent for the whole gulp), so all subsequent attempts
         * to COW will find the pages COWed already.
         *
         * There might be additional opportunities for
         * optimization lurking here.
         */
        sts = limits_of_active_range( bfap, pg, &arPg, &arCnt );
        if ( sts != EOK ) {
            return 1;
        }

        /*
         * The active range may start before pg.
         * It may extend beyond pg + cnt. So arPg may be before pg and
         * arCnt may take us beyond maxClonePgs. If arPg is before pg
         * pages before pg may already be cowed. We need to find the first
         * un-cowed page in the active range if arPg is before pg.
         * rbf_add_overlapping_clone_stg will only add stg until an existing
         * page is encountered so it does not matter if cnt pages
         * overlaps pages that are already cowed, but the first page
         * must not already exist.
         */
        MS_SMP_ASSERT(arPg + arCnt > pg);  /* active range covers pg */
        MS_SMP_ASSERT(arPg < pg + cnt);  /* active range covers pg */
        while ( arPg < pg ) {
            if ( !page_is_mapped(cloneap, arPg, &nextPg, TRUE) ) {
                break;
            }

            /* If the page is already mapped we need to find the first
             * unmapped page.
             */
            arCnt -= nextPg - arPg;
            arPg = nextPg;
        }

        pg = arPg;
        cnt = MIN( arCnt, cloneap->maxClonePgs - arPg );
    }

    lastPg = pg + cnt;
    /* Look through pages to be cow'ed for holes in the orig file. */
    for ( holePg = pg; holePg < lastPg; ) {

        if ( page_is_mapped(bfap, holePg, &nextPg, TRUE) == TRUE ) {
            /* nextPg is the page beyond the current extent. */
            MS_SMP_ASSERT(nextPg > holePg);
            holePg = nextPg;
            continue;
        }

        /* Found a hole in the original file. */
        cnt = holePg - pg;
        break;
    }

    /* After the above for loop if holePg > lastPg then the next */
    /* extent (hole or not) in the orig starts after the cow range. */
    /* rbf_add_overlapping_clone_stg will not make a permanent hole */
    /* since holePg is not adjacent to the cow range. */
    /* If holePg < lastPg then the above loop found a hole within the */
    /* cow range and rbf_add_overlapping_clone_stg will add a permanent */
    /* hole after the cow pages. */
    /* If holePg == lastPg then the original has an extent (hole or not) */
    /* That starts right at the end of the cow range. */
    /* This could be because we limited the cow range to the end of the */
    /* original file (nextPage) or the end or the clone file (maxClonePgs). */
    /* In those cases we don't want to make a permanent hole after the */
    /* cow page range. It could be that the next extent in the original */
    /* just happens to be at the end of the cow range. In this case also */
    /* we choose not to make a permanent hole after the cow range. */
    /* To force rbf_add_overlapping_clone_stg to not make a permanent hole */
    /* we set holePg to be non adjacent to the cow range. */
    /* If p is adjacent to the pages to be added (p == pg + cnt), */
    /* then rbf_add_overlapping_clone_stg will add a hole. */
    /* So, to prevent adding a permanent hole, increment p. */

    if ( holePg == lastPg ) {
        if ( holePg == bfap->nextPage || holePg >= cloneap->maxClonePgs ) {
            /* Don't add a hole beyond the end of the file. */
            holePg++;
        } else if ( page_is_mapped(bfap, holePg, &nextPg, TRUE) ) {
            /* If the 32 pages ends right on an extent boundary, */
            /* don't add a hole. */
            holePg++;
        }
    }

    *pgOffA = pg;
    *pgCntA = cnt;
    *holePgA = holePg;

    return 0;
}

/*
 * bs_cow_pg
 *
 * Determines if the specified page needs to be copied from the
 * original bitfile to the clone bitfile.  Implements the 'copy on write'
 * function for bitfile pages.
 *
 * In general, a page needs to be copied if it is not in the clone (the clone
 * has a 'hole' in its extent map for this page).
 *
 * This routine returns 0 if an error was encountered and no pages were
 * cowed.  Otherwise, it returns the number of pages that were cowed, or the
 * number of pages that do not need to be cowed because they are already
 * cowed or are in a permanent hole.  Either way, the calling routine can
 * skip over this number of pages in its quest to cow a page range.
 */

static bsPageT
bs_cow_pg(
    bfAccessT *bfap,      /* in - bitfile access struct */
    bsPageT pg,           /* in - page being modified (if applicable) */
    bsPageT pgCnt,        /* in - # pages being modified (if app) */
    ftxHT parentFtxH      /* in - parent ftx handle */
    )
{
    bfSetT *bfSetp = bfap->bfSetp;
    bfAccessT *cloneap = NULL;
    statusT sts;
    int cowLkLocked = FALSE, ftxStarted = FALSE, cloneOp = FALSE;
    uint32T p;
    uint32T cnt;
    ftxHT ftxH;
    bfPageRefHT origPgRef, clonePgRef;
    char *origPgp, *clonePgp;
    uint32T pgsAdded = 0;
    bfSetT *cloneSetp = bfSetp->cloneSetp;   /* stabilzed in bs_cow */
    statusT orig_sts;
    int lastpg;
    uint32T nextpage;
    struct vnode *nullvp = NULL;
    int migTruncLocked = FALSE;
    int fileLkLocked = 0;
    int cloneTokenHeld=0;
    bfPageRefHintT ref_hint;
    int ret;
    actRangeT *arp = NULL;
    uint32T nextPage = 0;

    MS_SMP_ASSERT(cloneSetp->bfsHoldCnt > 0);

    if (bfap->noClone) {
        /*
         * The original bitfile has no clone so just return since there
         * is nothing to do.  This is a racy test so if it fails (we
         * think that there is a clone) we will find out under the
         * protection of the ftx and COW lock below if the clone has
         * disappear.
         */
        goto error_exit;
    }

    sts = bs_access_one( &cloneap,
                         bfap->tag,
                         cloneSetp,
                         parentFtxH,
                         0,
                         NULL,
                         &nullvp,
                         NULL );
    if (sts != EOK) {
        /*
         * There doesn't appear to be a clone , why didn't the above check
         * catch this already?
         */
        bfap->noClone = TRUE;
        goto error_exit;
    }
    bfap->nextCloneAccp  = cloneap;

    cloneOp = TRUE;
    cloneap->origAccp = bfap;
    
    MS_SMP_ASSERT(cloneap->onFreeList == 0);

    if (cloneap->outOfSyncClone){
        /* clone bfset exists but file is out of sync with the orig */
        bfap->noClone = TRUE;
        goto error_exit;
    }

    /* This and all succeeding pages are 'uncowable' because they are
     * after the EOF of the original when it was cloned; maxClonePgs 
     * never changes, so no need to seize locks for this check.
     */
    if ( pg >= cloneap->maxClonePgs ) {
        pgsAdded = 0;
        goto cow_done;
    }

    if ( parentFtxH.hndl == 0 ) {
        /* ftx will be started in cow_get_locks */
        ftxStarted = TRUE;
    }

    /* Lock the clone's migTrunckLk & the orig's file_lock and cow_lk. 
     * May take clu token. An active range will be seized if the file
     * is open for directIO.
     */
    ret =  cow_get_locks( cloneap, &parentFtxH, pg, pgCnt, &arp );
    cloneTokenHeld = ret & CLU_TOKEN_TAKEN;
    fileLkLocked   = ret & FILE_LOCK_TAKEN;
    migTruncLocked = TRUE;
    cowLkLocked = TRUE;

    if ( cloneSetp->state == BFS_DELETING ) {  /* protected by starting ftx */
        bfap->noClone = TRUE;
        goto error_exit;
    }

    /*
     * Determine if the page needs to be copied to the clone.  The
     * page must be in the clone's page range and it must not be
     * known to the clone (the clone doesn't have this page, it gets
     * the page from the original bitfile).
     */
    if ( pg >= cloneap->maxClonePgs ) {
        pgsAdded = 0;
        goto cow_done;
    } else if ( page_is_mapped(cloneap, pg, &nextPage, TRUE) ) {
        pgsAdded = nextPage - pg;
        goto cow_done;
    }

    /*
     * We only want to mark the bitfile out of sync if we
     * are indeed going to attempt to COW a page otherwise
     * the file really is not out-of-sync yet
     */
    if (cloneSetp->bfSetFlags & BFS_OD_OUT_OF_SYNC) {
        /* clone bfset exists but is out of sync with the orig */
        MIGTRUNC_UNLOCK(&(cloneap->xtnts.migTruncLk));
        migTruncLocked = FALSE;
        bfap->noClone = TRUE;
        bs_bf_out_of_sync(cloneap,parentFtxH);

        goto error_exit;
    }

    sts = FTX_START_N(FTA_BS_COW_PG, &ftxH, parentFtxH, bfap->dmnP, 0);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_cow_pg: ftx_start (2) failed", sts );
    }

    /* Determine how many pages will be cowed; this number will depend
     * on the open mode (cached vs directIO) and the number of pages
     * being modified in this path. 
     */
    ret = cow_get_page_range( cloneap, pg, &pg, &cnt, &p );
    if ( ret ) {
        MIGTRUNC_UNLOCK( &cloneap->xtnts.migTruncLk );
        migTruncLocked = FALSE;
        bfap->noClone = TRUE;
        bs_bf_out_of_sync( cloneap, ftxH );
        ftx_fail( ftxH );
        goto error_exit;
    }

    MS_SMP_ASSERT(cloneap->xtnts.type == bfap->xtnts.type);
    sts = rbf_add_overlapping_clone_stg( cloneap,
                                         pg,
                                         cnt,
                                         p,
                                         ftxH,
                                         &pgsAdded);
    if (sts == EOK) {
        cloneap->cowPgCount++;
    }

    MIGTRUNC_UNLOCK(&(cloneap->xtnts.migTruncLk));
    migTruncLocked = FALSE;

    if (sts != EOK) {
        goto clone_out_of_sync;
    }

    if ( bfap->bfVp && bfap->bfVp->v_flag & VDIRECTIO ) {
        ref_hint = BS_NIL;
    } else {
        ref_hint = BS_SEQ_AHEAD;
    }

    /*
     * Copy the pages from the orig to the clone.
     */
    for (p = pg; p < (pg + pgsAdded); p++) {
        sts = bs_refpg( &origPgRef,
                        (void *) &origPgp,
                        bfap,
                        p,
                        ref_hint );

        if ( sts == EOK ) {

            int inCache;
            blkMapT blkMap;
#define BLKDESCMAX 10
            blkDescT blkDesc[BLKDESCMAX];
            /*
             * Determine whether the page is already in
             * the cache.
             */
            inCache = bs_find_page( cloneap, p, 1 );

            /*
             * Copy the orig page to the clone page.
             */
            sts = bs_pinpg_int( &clonePgRef,
                                (void *) &clonePgp,
                                cloneap,
                                p,
                                BS_OVERWRITE,
                                PINPG_NOFLAG );

            if ( sts != EOK ) {
                ADVFS_SAD1( "bs_cow_pg: pin clone err", sts );
            }

            /*
             * If page was in the cache, get the new mapping
             * and change the mapping for the buffer.
             * The fact that we have the page pinned and are
             * the only ones that can have the clone's page
             * pinned allows us to remap the buffer without
             * further sychronization.
             */
            if ( inCache ) {
                blkMap.blkDesc = &blkDesc[0];
                blkMap.maxCnt = BLKDESCMAX;

                sts = x_page_to_blkmap( cloneap, p, &blkMap );
                if ((sts != EOK) && (sts != W_NOT_WRIT)) {
                    ADVFS_SAD0("bs_cow_pg: cannot get blkMap");
                }
                blkMap.maxCnt = blkMap.readCnt + blkMap.writeCnt;
                buf_remap( clonePgRef, &blkMap );
            } else {
                bcopy(origPgp, clonePgp, bfap->bfPageSz * BS_BLKSIZE);
            }

            (void) bs_unpinpg( clonePgRef, logNilRecord, BS_DIRTY );
            (void) bs_derefpg( origPgRef, BS_CACHE_IT );

        } else {
            /*
             * Zero-fill the clone page.
             * TODO: could create a fake 'hole'.
             *       Do this when it is supported.
             */
            int *pgp;

            sts = bs_pinpg_int( &clonePgRef,
                                (void *) &pgp,
                                cloneap,
                                p,
                                BS_OVERWRITE,
                                PINPG_NOFLAG );
            if (sts != EOK) {
                ADVFS_SAD1( "bs_cow_pg: pin clone err", sts );
            }

            bzero((char *)pgp, cloneap->bfPageSz * BS_BLKSIZE);

            (void) bs_unpinpg( clonePgRef, logNilRecord, BS_DIRTY );
        }
    }

    if ( pgsAdded ) {
        sts = bfflush(cloneap, pg, pgsAdded, FLUSH_IMMEDIATE);
    }
    /* else sts is EOK from above */

    /*
     * don't undo this sub ftx if parent fails.  don't want some
     * undo routine to require copy on write (not allowed in undo).
     */
    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

    if (sts != EOK) {
        goto clone_out_of_sync;
    }

    ftx_done_n( ftxH, FTA_BS_COW_PG );

cow_done:

    if (migTruncLocked) {
        MIGTRUNC_UNLOCK(&(cloneap->xtnts.migTruncLk));
    }

    if ( arp ) {
        mutex_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }
    if ( fileLkLocked ) {
        FS_FILE_UNLOCK_RECURSIVE( VTOC(bfap->bfVp) );
    }

    if (cloneTokenHeld){
        fsid_t fsid;
        BS_GET_FSID( cloneap->bfSetp, fsid );
        CC_CFS_COW_MODE_LEAVE( fsid, cloneap->tag );
    }

    bs_close_one(cloneap, 0, parentFtxH );


    if (ftxStarted) {
        ftx_done_n( parentFtxH, FTA_BS_COW_PG );
    }

    if (cowLkLocked) {
        COW_UNLOCK( &(bfap->cow_lk) )
    }

    return( pgsAdded );

clone_out_of_sync:
    /*
     * Mark the clone as out of sync with the original.  This
     * will prevent anyone from accessing this bitfile
     * (refpg() will return an error).
     */
    orig_sts = sts;
    bfap->noClone = TRUE;

    print_out_of_sync_msg( bfap, bfSetp, orig_sts );

    bs_bf_out_of_sync(cloneap,ftxH);

    bs_bfs_out_of_sync( cloneap->bfSetp, ftxH );

    ftx_done_n( ftxH, FTA_BS_COW_PG );

error_exit:

    if (cloneTokenHeld) {
        fsid_t fsid;
        BS_GET_FSID( cloneap->bfSetp, fsid );
        CC_CFS_COW_MODE_LEAVE( fsid, cloneap->tag );
    }

    /* it should not be locked by bs_cow_pg if we get here */
    MS_SMP_ASSERT(!migTruncLocked);    

    if ( arp ) {
        mutex_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }
    if ( fileLkLocked ) {
        FS_FILE_UNLOCK_RECURSIVE( VTOC(bfap->bfVp) );
    }

    if (cloneOp) {
        bs_close_one(cloneap, 0, parentFtxH );
    }

    if (ftxStarted) {
        ftx_done_n( parentFtxH, FTA_BS_COW_PG );
    }

    if (cowLkLocked) {
        COW_UNLOCK( &(bfap->cow_lk) )
    }
    return( 0 ); /* error occurred; caller should not try to cow more pgs */
}

/*
 * bs_cow
 *
 * Bitfile 'copy on write' routine.  This routine determines if we
 * need to perform a copy-on write operation between the original
 * bitfile being modified and its clone bitfile.  The routine
 * also does the copy-on write operation.
 *
 * What does copy on write mean?  Basically, if an original bitfile is
 * modified we need to copy the original mcell records and or pages
 * to the clone; so it's more of a 'copy when modified' operation.
 *
 * The first time an original is modified (first time after a clone
 * was created) we need to create a new primary mcell for the clone
 * and copy the original's bitfile attributes record to the clone's
 * primary mcell.  At this time we also copy all none extent map
 * mcell records to the clone (possibly adding more mcells to it).
 *
 * Each time a page is modified we call bs_cow_pg() to determine if
 * the page needs to be copied to the clone.
 */

void
bs_cow(
    bfAccessT *bfap,      /* in - bitfile access struct */
    unsigned cowOpt,      /* in - COW options */
    unsigned long pg,     /* in - page being modified (if applicable) */
    unsigned long pgCnt,  /* in - the # of pages being modified (if app); */
                          /*      only needed if COW_PINPG is specified  */
    ftxHT parentFtxH      /* in - parent transaction handle */
    )
{
    bfSetT *bfSetp = bfap->bfSetp;
    bfAccessT *cloneap;
    statusT sts;
    int cloneOp = FALSE, ftxStarted = FALSE, lkLocked = FALSE;
    ftxHT ftxH = { 0 };
    bfSetT *cloneSetp;
    struct vnode *nullvp = NULL;

    cloneSetp = hold_cloneset( bfap, FALSE );
    if ( cloneSetp == NULL ) {
        release_cloneset( bfap->bfSetp );
        return;
    }

    if ( bfap->cloneCnt != bfSetp->cloneCnt ) {
        /*
         * Clone does not appear to have it's own metadata.  This
         * is a racy check since two threads could see this before
         * grabbing the lock below.  We recheck the test to make sure.
         */

        /*
         * The only reason that we're starting an ftx is to synchronize with
         * rmfset/clonefset which use an exclusive ftx to block bs_cow() and
         * each other.  So, if the caller has already started an ftx we don't
         * need to start one here.
         */

        if (parentFtxH.hndl == 0) {
            sts = FTX_START_N( FTA_BS_COW, &ftxH, parentFtxH,
                               bfap->dmnP, 4 );
            if (sts != EOK) {
                ADVFS_SAD1( "bs_cow: ftx_start failed", sts );
            }
            ftxStarted = TRUE;

        } else {
            ftxH = parentFtxH;
        }

        /*
         * This can't be an ftx lock because some callers call bs_cow() more
         * than once under the same ftx; deadlock city!
         */

        COW_LOCK_WRITE( &(bfap->cow_lk) )
        lkLocked = TRUE;

        if (bfap->cloneCnt == bfSetp->cloneCnt) {
            /*
             * Two threads thought the clone doesn't have metadata.  One
             * got the lock and created the clone's metadata, the other
             * is now here and has just determined that the clone really does
             * have it's own metadata so quit.
             */

            if (ftxStarted) {
                ftx_fail( ftxH );
            }

            if (lkLocked) {
                COW_UNLOCK( &(bfap->cow_lk) )
            }
            goto _finish;
        }

        if ((!bs_have_clone( bfap->tag, bfSetp->cloneSetp, ftxH )) || 
            (bfSetp->cloneSetp->state == BFS_DELETING)) {
             /*
              * There doesn't appear to be a clone.
              */
             bfap->noClone = TRUE;
             goto error_exit;
        }

        MS_SMP_ASSERT(cloneSetp == bfSetp->cloneSetp);

        /*
         * Open the clone.
         */
        sts = bs_access_one( &cloneap,
                             bfap->tag,
                             cloneSetp,
                             ftxH,
                             0,
                             NULL,
                             &nullvp,
                             NULL );
        if (sts != EOK) {
            /*
             * There doesn't appear to be a clone any more so do nothing?
             */
            bfap->noClone = TRUE;
            goto error_exit;
            
        }

        bfap->nextCloneAccp = cloneap;
        bfap->noClone = FALSE;
        cloneOp = TRUE;

        if (cloneSetp->bfSetFlags & BFS_OD_OUT_OF_SYNC) {
            
            /* clone bfset exists but is out of sync with the orig 
             * We do not have our own cloned metadata so we
             * can not store outofSyncClone out to disk.
             */
            bfap->noClone = TRUE;
            cloneap->outOfSyncClone = TRUE;
            goto error_exit;
        }

        /*
         * Create a primary mcell for the clone and copy all non-extent
         * map BMT records to the clone.
         */

        sts = clone( bfap, cloneap, ftxH );
        if (sts != EOK) {
            /*
             * Couldn't create the clone.  So we mark it out of sync
             * with the orig.  Unfortunately, once the access struct
             * is recycled we have no way of knowing that the clone
             * is out of sync.
             *
             * TODO:  Could mark the original instead
             * of the clone in this case.  The original would have to
             * be reset when a new clone is made.
             */

            bfap->noClone = TRUE;

            /* We weren't able to get a primary MCELL so we
             * can't mark the clone as out-of-sync on disk
             * we'll mark it in memory and in the fileset.
             * The clone is truely out of sync
             */
            
            cloneap->outOfSyncClone = TRUE;
            print_out_of_sync_msg( bfap, bfSetp, sts );
            bs_bfs_out_of_sync( cloneSetp, ftxH );

            if (ftxStarted) {
                ftx_done_n( ftxH, FTA_BS_COW );
                ftxStarted = FALSE;
            }

            goto error_exit;
        }

       /*
        * Must close file in clone fileset before committing the
        * transaction.  This prevents a race between this thread's
        * closing the clone file and an rmfset of the clone fileset.
        */
        bs_close_one( cloneap, 0, ftxStarted ? ftxH : parentFtxH );

        if (ftxStarted) {
            ftx_done_n( ftxH, FTA_BS_COW );
        }

        if (lkLocked) {
            COW_UNLOCK( &(bfap->cow_lk) )
        }
    }

_finish:

    if (cowOpt & COW_PINPG) {
        bsPageT i = pg;
        bsPageT npages = pgCnt;
        bsPageT pgsCowed = 0;

        /* The set has been cloned at least once.  Check to
         * see if we need to do any 'copy on write' processing.
         * This loop is needed because the cow algorithm, although it
         * will try to cow a range of pages, may not be able to cow
         * the entire range in one fell swoop, so we go back in again
         * if that happens.
         */
        while ( i < (pg + pgCnt) ) {
            pgsCowed = bs_cow_pg( bfap, i, npages, parentFtxH );
            if ( pgsCowed == 0 ) 
                break;                /* clone now out of sync */
            i += pgsCowed;
            npages -= pgsCowed;
        }
    }

    release_cloneset( cloneSetp );
    return;

error_exit:

    if (cloneOp) {
        bs_close_one( cloneap, 0, ftxStarted ? ftxH : parentFtxH );
    }

    release_cloneset( cloneSetp );

    if (ftxStarted) {
        ftx_done_n( ftxH, FTA_BS_COW );
    }

    if (lkLocked) {
        COW_UNLOCK( &(bfap->cow_lk) )
    }
    return;
}


/*
 * bs_get_bfset_params
 *
 * Get the parameters for a bitfile-set
 *
 * Returns E_BAD_BF_SET_POINTER, possible status
 * from bmtr_scan or bs_derefpg
 */

statusT
bs_get_bfset_params(
    bfSetT *bfSetp,            /* in - the bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    int lock                   /* in - not used */
    )
{
    statusT sts = EOK;
    bsBfSetAttrT bfsAttr;
    bsQuotaAttrT quotaAttr;

    /*
     * Check validity of descriptor pointer.
     */
    if ( ! BFSET_VALID(bfSetp) ) {
        RAISE_EXCEPTION(E_BAD_BF_SET_POINTER);
    }

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_ATTR,
                       (void *) &bfsAttr,
                       sizeof( bfsAttr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    sts = bmtr_get_rec(bfSetp->dirBfAp,
                       BSR_BFS_QUOTA_ATTR,
                       &quotaAttr,
                       sizeof( quotaAttr ) );
    if (sts == EBMTR_NOT_FOUND) {
        bzero( &quotaAttr, sizeof( quotaAttr ) );
        quotaAttr.quotaStatus = bfsAttr.oldQuotaStatus;
    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    *bfSetParams                 = bsNilBfSetParams;
    bfSetParams->bfSetId         = bfsAttr.bfSetId;
    bfSetParams->nextCloneSetTag = bfsAttr.nextCloneSetTag;
    bfSetParams->origSetTag      = bfsAttr.origSetTag;
    bfSetParams->cloneId         = bfsAttr.cloneId;
    bfSetParams->cloneCnt        = bfsAttr.cloneCnt;
    bfSetParams->numClones       = bfsAttr.numClones;
    bfSetParams->mode            = bfsAttr.mode;
    bfSetParams->uid             = bfsAttr.uid;
    bfSetParams->gid             = bfsAttr.gid;
    bfSetParams->bfSetFlags      = bfsAttr.flags;
    bfSetParams->quotaStatus     = quotaAttr.quotaStatus;
    bfSetParams->blkHLimit       = quotaAttr.blkHLimitLo;
    bfSetParams->blkSLimit       = quotaAttr.blkSLimitLo;
    bfSetParams->blkHLimit      |= (long) quotaAttr.blkHLimitHi << 32L;
    bfSetParams->blkSLimit      |= (long) quotaAttr.blkSLimitHi << 32L;
    bfSetParams->fileHLimit      = quotaAttr.fileHLimitLo;
    bfSetParams->fileSLimit      = quotaAttr.fileSLimitLo;
    bfSetParams->blkTLimit       = quotaAttr.blkTLimit;
    bfSetParams->fileTLimit      = quotaAttr.fileTLimit;
    bfSetParams->filesUsed       = bfSetp->bfCnt;
    bfSetParams->blksUsed        = 0;
    bfSetParams->dmnAvailFiles   = bs_get_avail_mcells( bfSetp->dmnP );
/*
 * advfs fix
 *
 * The fields on the left are declared long while the fields on the
 * right a supposed to be uint64 which is a quad.  SOmeone please fix
 * this.  For now we just use the low 32 bits.
 */
#ifdef __arch64__
    bfSetParams->dmnTotalBlks    = bfSetp->dmnP->totalBlks;
    bfSetParams->dmnAvailBlks    = bfSetp->dmnP->freeBlks;
#else
    bfSetParams->dmnTotalBlks    = bfSetp->dmnP->totalBlks.low;
    bfSetParams->dmnAvailBlks    = bfSetp->dmnP->freeBlks.low;
#endif
    if (bfSetp->fsnp != NULL) {
        struct fileSetNode *fsnp = bfSetp->fsnp;

        /*
         * If bfSetT for a fileset is valid,
         * override the first value from disk with 
         * in-memory value because the changed value
         * in-memory should be applied immediately
         * for a proper operation(s).
         */
        bfSetParams->bfSetFlags = bfSetp->bfSetFlags;

        bfSetParams->quotaStatus = fsnp->quotaStatus;

        if ((fsnp->quotaStatus & QSTS_QUOTA_ON) &&
            (fsnp->quotaStatus & QSTS_FS_QUOTA_MT)) {
            bfSetParams->filesUsed = fsnp->filesUsed;
            bfSetParams->blksUsed  = fsnp->blksUsed;
        }
    }

    bcopy( bfsAttr.setName, bfSetParams->setName, BS_SET_NAME_SZ );
    bcopy( bfsAttr.fsContext, bfSetParams->fsContext, BS_FS_CONTEXT_SZ * 4 );
    bcopy( bfsAttr.fragGrps, bfSetParams->fragGrps, BF_FRAG_MAX * 8);

HANDLE_EXCEPTION:
    return sts;
}

statusT
bs_set_bfset_params(
    bfSetT *bfSetp,            /* in - bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams, /* in - bitfile-set's params */
    long xid                   /* in - CFS transaction id */
    )
{
    return rbf_set_bfset_params( bfSetp, bfSetParams, FtxNilFtxH, xid );
}

/*
 * rbf_set_bfset_params
 *
 * This routine sets the writable bitfile-set parameters in the bitfile-set
 * metadata.
 *
 * NOTE: The file system context struct is written, and it is
 * only written to the stable metadata - any "in-memory" copy of it
 * will not be affected.
 *
 * NOTE: The frag groups headers are not allowed to be modified by
 * this routine.  Only the frag code is allowed to modify that data.
 *
 * Returns E_BAD_BF_SET_POINTER, possible error return from
 * bmtr_scan, bs_deref_pg, bs_pinpg, or bs_unpinpg.
 */

statusT
rbf_set_bfset_params(
    bfSetT *bfSetp,             /* in - bitfile-set's desc pointer */
    bfSetParamsT *bfSetParams,  /* in - bitfile-set's params */
    ftxHT parentFtxH,
    long xid                    /* in - CFS transaction id */
    )
{
    statusT sts = EOK;
    bsBfSetAttrT bfsAttr;
    bsQuotaAttrT quotaAttr;
    int ftxStarted = FALSE,
        bfSetTblLocked = FALSE,
        mcellListLocked = FALSE,
        exclusiveFtx = FALSE;
    ftxHT ftxH;

    /*
     * Check validity of descriptor pointer.
     */
    if ( ! BFSET_VALID(bfSetp) ) {
        RAISE_EXCEPTION(E_BAD_BF_SET_POINTER);
    }

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    if ((bfSetParams->fileSLimit & 0xffffffff00000000) ||
        (bfSetParams->fileHLimit & 0xffffffff00000000)) {
        /* only support 32-bit file limits for now */
        RAISE_EXCEPTION(EINVAL);
    }

    if ((bfSetParams->blkSLimit < 0) ||
        (bfSetParams->blkHLimit < 0)) {
        /* only support 63-bit block limits */
        RAISE_EXCEPTION(EINVAL);
    }

    if (parentFtxH.hndl == 0) {
        /* valid xids (xid>0) only come from bs_set_bfset_params(), *
         * which always starts a new transaction.                   */
        sts = FTX_START_EXC_XID( FTA_BS_SET_BFSET_PARAMS,
                                 &ftxH, bfSetp->dmnP, 1, xid );
        exclusiveFtx = TRUE;
    } else {
        sts = FTX_START_N( FTA_BS_SET_BFSET_PARAMS,
                           &ftxH, parentFtxH, bfSetp->dmnP, 1 );
    }

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    ftxStarted = TRUE;

    if (!lock_holder(&bfSetp->dmnP->BfSetTblLock.lock)) {
        /*
         * fs_fset_name_change() calls this routine with the
         * BfSetTblLock locked so we don't need to lock it here.
         */

        BFSETTBL_LOCK_WRITE( bfSetp->dmnP )
        bfSetTblLocked = TRUE;
    }

    /* basic fileset attributes */

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            (void *) &bfsAttr,
                            sizeof( bfsAttr ),
                            BMTR_LOCK );
    if (sts != EOK) {
        sts = EBMTR_NOT_FOUND;
        goto the_end;
    }
    mcellListLocked = TRUE;

    bfsAttr.uid = bfSetParams->uid;
    bfsAttr.gid = bfSetParams->gid;
    bfsAttr.mode = bfSetParams->mode;
    bfsAttr.flags = bfSetParams->bfSetFlags;

    bcopy( bfSetParams->setName, bfsAttr.setName, BS_SET_NAME_SZ );
    bcopy( bfSetParams->fsContext, bfsAttr.fsContext, BS_FS_CONTEXT_SZ * 4 );

    sts = bmtr_put_rec_n_unlk( bfSetp->dirBfAp,
                               BSR_BFS_ATTR,
                               &bfsAttr,
                               sizeof( bfsAttr ),
                               ftxH,
                               BMTR_UNLOCK,
                               0 );
    if (sts != EOK) {
        goto the_end;
    }

    /*
     * Take a simple lock to modify bfSetFlags.
     * Locate statement to change bfSetp->bfSetFlags here so
     * if bmtr_put_rec_n_unlk() above fails, bfSetp->bfSetFlags
     * can still hold old information. If not, the value will
     * be changed.
     */
    mutex_lock(&bfSetp->bfSetMutex);
    bfSetp->bfSetFlags = bfSetParams->bfSetFlags;
    mutex_unlock(&bfSetp->bfSetMutex);

    mcellListLocked = FALSE;

    /* now deal with the quota attributes */

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_QUOTA_ATTR,
                            (void *) &quotaAttr,
                            sizeof( quotaAttr ),
                            BMTR_LOCK );
    if (sts != EOK) {
        sts = EBMTR_NOT_FOUND;
        goto the_end;
    }
    else {
        mcellListLocked = TRUE;
    }

    /* set the time limits now; they may be overridden below */
    quotaAttr.blkTLimit = bfSetParams->blkTLimit;
    quotaAttr.fileTLimit = bfSetParams->fileTLimit;

    if (bfSetp->fsnp != NULL) {
        time_t sec;
        struct timeval tv;
        struct fileSetNode *fsnp = bfSetp->fsnp;

        TIME_READ(tv);
        sec = tv.tv_sec;

        /*
         * rules for updating the grace period - update if new
         * soft limit is exceeded and there either wasn't a softlimit
         * before or the soft limit was not exceeded before.  basically,
         * the grace period should not be changed if it is already in affect.
         */

        if (bfSetParams->blkSLimit &&
           (fsnp->blksUsed >= bfSetParams->blkSLimit) &&
           (!(fsnp->blkSLimit) ||
            fsnp->blksUsed < fsnp->blkSLimit)) {

            fsnp->blkTLimit     = sec + fsnp->qi[GRPQUOTA].qiBlkTime;
            quotaAttr.blkTLimit = fsnp->blkTLimit;
        }

        if (bfSetParams->fileSLimit &&
           (fsnp->filesUsed >= bfSetParams->fileSLimit) &&
           (!(fsnp->fileSLimit) ||
            fsnp->filesUsed < fsnp->fileSLimit)) {

            fsnp->fileTLimit     = sec + fsnp->qi[GRPQUOTA].qiFileTime;
            quotaAttr.fileTLimit = fsnp->fileTLimit;
        }

        /*
         * the BMT quota attributes
         * no longer have the QSTS_QUOTA_ON, QSTS_USR_QUOTA_EN, or
         * QSTS_GRP_QUOTA_EN flags set.  So when we compare the
         * in-memory and on-disk quota stati here, we ignore
         * discrepancies due only to these flags.
         * We must have an EXCL Ftx to change quotaStatus.
         */
        if (quotaAttr.quotaStatus &&
            ((quotaAttr.quotaStatus & 
              ~(QSTS_QUOTA_ON | QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN)) != 
             (bfSetParams->quotaStatus & 
              ~(QSTS_QUOTA_ON | QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN)))) 
          {
            if (!exclusiveFtx) {
              sts = EBAD_FTXH;
              goto the_end;
            }
            fsnp->quotaStatus = bfSetParams->quotaStatus;
          }
        
        fsnp->fileHLimit  = bfSetParams->fileHLimit;
        fsnp->blkHLimit   = bfSetParams->blkHLimit;
        fsnp->fileSLimit  = bfSetParams->fileSLimit;
        fsnp->blkSLimit   = bfSetParams->blkSLimit;

        if (fsnp->blksUsed < fsnp->blkSLimit) {
            fsnp->fsFlags &= ~FS_DQUOT_BLKS;
        }

        if (fsnp->filesUsed < fsnp->fileSLimit) {
            fsnp->fsFlags &= ~FS_DQUOT_FILES;
        }
    }

    quotaAttr.blkHLimitLo = bfSetParams->blkHLimit & 0x00000000ffffffff;
    quotaAttr.blkHLimitHi = (bfSetParams->blkHLimit >> 32L) &
                            0x00000000ffffffff;
    quotaAttr.blkSLimitLo = bfSetParams->blkSLimit & 0x00000000ffffffff;
    quotaAttr.blkSLimitHi = (bfSetParams->blkSLimit >> 32L) &
                            0x00000000ffffffff;

    quotaAttr.fileHLimitLo = bfSetParams->fileHLimit;
    quotaAttr.fileSLimitLo = bfSetParams->fileSLimit;
    /*
     * we do not write
     * quota enforcement flags or "quotas are on" flags to the BMT.
     */
    quotaAttr.quotaStatus  = bfSetParams->quotaStatus &
                            ~(QSTS_QUOTA_ON |
                              QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);

    sts = bmtr_put_rec_n_unlk( bfSetp->dirBfAp,
                               BSR_BFS_QUOTA_ATTR,
                               &quotaAttr,
                               sizeof( quotaAttr ),
                               ftxH,
                               mcellListLocked ? BMTR_UNLOCK : BMTR_NO_LOCK,
                               0 );
    if (sts != EOK) {
        goto the_end;
    }
    mcellListLocked = FALSE;

    if (bfSetTblLocked) {
        BFSETTBL_UNLOCK( bfSetp->dmnP )
        bfSetTblLocked = FALSE;
    }

    ftx_done_n( ftxH, FTA_BS_SET_BFSET_PARAMS );
    return EOK;

the_end:

    if (bfSetTblLocked) {
        BFSETTBL_UNLOCK( bfSetp->dmnP )
    }

    if (mcellListLocked) {
        MCELLIST_UNLOCK( &(bfSetp->dirBfAp->mcellList_lk) )
    }

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

HANDLE_EXCEPTION:
    return sts;
}

set_bfset_flag(bfAccessT *bfap)
{
    bsBfSetAttrT *setAttrp;
    statusT sts=0;
    ftxHT ftxH;
    rbfPgRefHT fsetPgH;
    bfSetT *bfSetp;

    bfSetp = bfap->bfSetp;

    sts = FTX_START_N (FTA_SET_BFSET_FLAG, &ftxH,
                        FtxNilFtxH, bfSetp->dmnP, 0);

    if (sts != EOK) {
        return sts;
    }

    BFSETTBL_LOCK_WRITE( bfSetp->dmnP );

    sts = bmtr_get_rec_ptr( bfSetp->dirBfAp,
                            ftxH,
                            BSR_BFS_ATTR,
                            sizeof( bsBfSetAttrT ),
                            1 /* pin pg */,
                            (void *) &setAttrp,
                            &fsetPgH,
                            NULL );

    if (sts != EOK) {
        BFSETTBL_UNLOCK( bfSetp->dmnP )
        return sts;
    }

    RBF_PIN_FIELD( fsetPgH, setAttrp->flags );

    setAttrp->flags = bfSetp->bfSetFlags;

    ftx_done_n( ftxH, FTA_SET_BFSET_FLAG );

    BFSETTBL_UNLOCK( bfSetp->dmnP )
    return EOK;
}


/*
** Given original file 'bfap', see if clone file exists and open it.
** Returns opened clone file at '*cloneapA".
** If *cloneapA is not NULL, the clone fileset is 'held' against
** rmfset by hold_cloneset. The caller must release it.
** The cloneset will also not be created or mounted until
** release_clu_clone_locks is called. Calls to this routine must be
** matched with calls to release_clu_clone_locks.
** If *cloneSetpA is set then the cloneset has bfsHoldCnt bumped,
** else the original set has its bfsHoldCnt bumped.
** If *cloneapA is not NULL then the clu_clonextnt_lk is held and must be
** released by the caller. If *cloneapA is not NULL the caller must close
** the clone file.
** Routine returns 1 if clu tokens held, 0 otherwise.
** A TRUE return means the cloneap is open and cloneset is mounted.
** The caller is responsible for releasing the clu token.
** Note that *cloneapA may be non-NULL yet the token not taken.
** However, the clone file must be open for the token to be taken.
**
** This routine needs to synchronize with bs_bfs_clone & advfs_mountfs since
** we cannot allow a client node to access the clone's extent map while a
** migrate of the original file is in progress. If a clone fileset was
** created and mounted after this point but before the migrate was completed
** the client could get the tokens and look at the clone extents.
** This routine also sychronizes with bs_bfs_delete to hold the clone
** fileset in place against asynchronous rmfset.
*/
int
get_clu_clone_locks( bfAccessT *bfap, struct fsContext *cp,
                     bfSetT **cloneSetpA, bfAccessT **cloneapA)
{
    bfSetT *clonesetp;
    vnode_t *nullvp = NULL;
    bfMCIdT mcid;
    vdIndexT vdi;
    int do_clxtnt_unlock = 0;
    int release_clone_token = FALSE;
    fsid_t fsid;
    bfAccessT *cloneap;
    statusT sts;

    MS_SMP_ASSERT(bfap->bfSetp->cloneId == BS_BFSET_ORIG);

    *cloneapA = NULL;
    clonesetp = hold_cloneset( bfap, TRUE );
    *cloneSetpA = clonesetp;

    if ( clonesetp == NULL ) {
        goto lock;
    }

    if ( clonesetp->bfSetFlags & BFS_OD_OUT_OF_SYNC ) {
        goto lock;
    }

    if ( bfap->noClone ) {
        goto lock;
    }

    COW_LOCK_WRITE( &bfap->cow_lk )

    if ( tagdir_lookup( clonesetp, &bfap->tag, &mcid, &vdi ) != EOK ) {
        bfap->noClone = TRUE;
        COW_UNLOCK( &(bfap->cow_lk) )
        goto lock;
    }

    COW_UNLOCK( &(bfap->cow_lk) )

    sts = bs_access_one( &cloneap,
                         bfap->tag,
                         clonesetp,
                         FtxNilFtxH,
                         0,
                         NULL,
                         &nullvp,
                         NULL );
    if ( sts != EOK ) {
        bfap->noClone = TRUE;
        goto lock;
    }

    if ( cloneap->outOfSyncClone ) {
        bfap->noClone = TRUE;
        bs_close_one( cloneap, 0, FtxNilFtxH );
        goto lock;
    }

    MS_SMP_ASSERT(cloneap->onFreeList == 0);

    if ( cloneap->dataSafety == BFD_FTX_AGENT ||
         cloneap->dataSafety == BFD_FTX_AGENT_TEMPORARY )
    {
        bs_close_one( cloneap, 0, FtxNilFtxH );
        goto lock;
    }

    *cloneapA = cloneap; /* so the caller can remember to deref the clone */

    bfap->nextCloneAccp  = cloneap;
    cloneap->origAccp = bfap;

    BS_GET_FSID(clonesetp, fsid);

    while ( !do_clxtnt_unlock ) {
        /* Try to get the token only if the clone fileset is mounted. */
        /* hold_cloneset prevents a race here. The clone fileset cannot */
        /* be transitioning to mounted at this time. It is either mounted */
        /* or not and will remain so until release_clu_clone_locks is called. */
        if ( clonesetp->fsnp ) {
            /* Getting the token retrieves the extent map that was */
            /* checked-out to the client. Once no client has the extent map */
            /* we can lock clu_clonextnt_lk and know they can't get a map. */
            /* CC_CFS_COW_MODE_ENTER will fail if there is no cnode. */
            if ( CC_CFS_COW_MODE_ENTER(fsid, cloneap->tag) == 0 ) {
                /* Since we got the token bs_get_clone_xtnt_map can't be */
                /* running. So no race to set cloneXtntsRetrieved. */
                release_clone_token = 1;
                cloneap->cloneXtntsRetrieved = 0;
            }
        }

        if ( cp != NULL ) {
            FS_FILE_WRITE_LOCK( cp );
        }

        CLU_CLXTNT_WRITE( &cloneap->clu_clonextnt_lk );
        do_clxtnt_unlock = 1;

        if ( cloneap->cloneXtntsRetrieved && !release_clone_token ) {
            /*
            ** When we called CC_CFS_COW_MODE_ENTER there was no cnode and
            ** so we didn't get the token. In the meantime a racer created
            ** a cnode and got the xtnt map. The racer also has the token.
            ** We will reset cloneXtntsRetrieved under the protection of
            ** clu_clonextnt_lk then call CC_CFS_COW_MODE_ENTER and wait for
            ** the token. This time CC_CFS_COW_MODE_ENTER will not fail.
            */
            cloneap->cloneXtntsRetrieved = 0;
            CLU_CLXTNT_UNLOCK( &cloneap->clu_clonextnt_lk );
            do_clxtnt_unlock = 0;
            if ( cp != NULL ) {
                FS_FILE_UNLOCK( cp );
            }
        }
    }

    /*
    ** Here, if the cnode existed, we have the token.
    ** Cluster clients will not call bs_get_clone_xtnt_map without the token.
    ** If the cnode didn't exist and we don't have the token at least we
    ** have clu_clonextnt_lk locked. If a client later creates the cnode
    ** and gets the token, bs_get_clone_xtnt_map will block on clu_clonextnt_lk.
    */

    return release_clone_token;

lock:
    if ( cp != NULL ) {
        FS_FILE_WRITE_LOCK( cp );
    }

    return 0;
}

/*
** This is the partner routine to get_clu_clone_locks.
*/
void
release_clu_clone_locks( bfAccessT *bfap,
                         bfSetT *cloneSetp,
                         bfAccessT *cloneap,
                         int token_flg )
{
    fsid_t fsid;

    if ( cloneap != NULL ) {
        CLU_CLXTNT_UNLOCK( &cloneap->clu_clonextnt_lk );
        if ( token_flg ) {
            BS_GET_FSID( cloneSetp, fsid );
            CC_CFS_COW_MODE_LEAVE( fsid, cloneap->tag );
        }

        bs_close_one( cloneap, 0, FtxNilFtxH );
    }

    if ( cloneSetp != NULL ) {
        release_cloneset( cloneSetp );
    } else {
        release_cloneset( bfap->bfSetp );
    }
}
/* end bs_bitfile_sets.c */
