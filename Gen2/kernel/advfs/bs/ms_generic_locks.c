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
 *      MSFS
 *
 * Abstract:
 *
 *      Defines generic locks and routines to manipulate the locks.
 */

#define ADVFS_MODULE MS_GENERIC_LOCKS

#include <ms_public.h>
#include <ms_privates.h>
#include <sys/kernel.h>
#include <sys/time.h>

static
lkHdrT *
find_locked_lock(
    lkHdrT *lk
    );
/*
 * Lock names for multiply used names. This macro is
 * defined in ms_generic_locks.h to include all of the
 * name strings for locks which have more than one init
 * macro, so that the lock package will think they are
 * all the same lock type.
 */

 ADVFS_LOCK_NAME_STRINGS

/*
 * Lock statistics
 */

advfsLockStatsT *AdvfsLockStats = NULL;


/*
 * trace_hdr
 *
 * Print the header out for tracing
 */
void 
trace_hdr(void)
{
    struct timeval hdrTime;
    hdrTime = get_system_time();
    printf( "%2d.%3d %d ", hdrTime.tv_sec & 0x7f,
            hdrTime.tv_usec>>10, u.u_kthreadp );
}


void
bs_lock_mgr_init( void )
{
    AdvfsLockStats = (advfsLockStatsT *)ms_malloc( sizeof( advfsLockStatsT ) );
}

/*
 * lk_init
 *
 * Initializes a lock and adds it to its mutex's linked list of locks.
 *
 * NOTE: The caller must hold the mutex locked.
 */

void
lk_init(
    void *lk,           /* in - pointer to the lock */
    spin_t *spin,      /* in - pointer to the lock's mutex */
    lkTypeT lkType,     /* in - lock's type (generic, state, etc.) */
    int resources,      /* in - number of resources for a generic lock */
    lkUsageT usage      /* in - lock's usage */
    )
{
    lkHdrT *lkHdr = lk;


    switch (lkType) {
        case LKT_STATE:
            {
            stateLkT *lk = (void *)lkHdr;
            stateLkT nilStateLk = { LKT_STATE, 0 }; 
            *lk = nilStateLk;
            cv_init(&lk->cv, "State Lock Condition Variable", NULL, CV_WAITOK);
            }
            break;

        default:
            ADVFS_SAD1( "lk_init: unknown lock type", lkType );
    }

    lkHdr->lkUsage = usage;
    lkHdr->spin = spin;
}

/*
 * lk_destroy
 *
 * Removes a lock from its mutex's linked list of locks.
 *
 * NOTE: The caller must hold the lock's mutex locked.
 */

void
lk_destroy(
    void *lk    /* in - pointer to the lock */
    )
{
    stateLkT *lock = (void *)lk;
    cv_destroy(&lock->cv);
}

/*
 * lk_signal
 *
 * Wakes up threads waiting on the semaphore.  The routine
 * either does nothing, wakes up a single thread, or wakes up all waiting
 * threads depending on the value of the 'action' parameter.
 *
 * ASSUMPTIONS: The mutex protecting the semaphore must be locked by the
 * caller.
 */
void
_lk_signal( 
           unLkActionT action,       /* in */ 
           void *lk,                 /* in */
           int ln,                   /* in */
           char *fn                  /* in */
           )
{
    lkHdrT *lkHdr = (lkHdrT *) lk;
    stateLkT *slk = (stateLkT *) lk;

    if (lkHdr->lkType == LKT_STATE) {
        if (action == UNLK_SIGNAL) {
            if (AdvfsLockStats) {
                AdvfsLockStats->usageStats[ lkHdr->lkUsage ].signal++;
                AdvfsLockStats->stateSignal++;
            }
            cv_signal( &slk->cv, NULL, CV_NULL_LOCK );
        } else if (action == UNLK_BROADCAST) {
            if (AdvfsLockStats) {
                AdvfsLockStats->usageStats[ lkHdr->lkUsage ].broadcast++;
                AdvfsLockStats->stateBroadcast++;
            }
            cv_broadcast( &slk->cv, NULL, CV_NULL_LOCK );
        }
    }
}

/* 
 * lk_set_state
 *
 * Changes the state of the state lock to the desired state.  Returns
 * the apporiate unlock action (signal or broadcast).  The caller should
 * call lk_signal() after releasing the associated mutex (see lk_unlock()
 * and lk_signal()).
 *
 * ASSUMPTIONS: The mutex protecting the state lock must be locked by the
 * caller.
 */

unLkActionT
_lk_set_state(
    stateLkT *lk,
    lkStatesT newState,
    int ln,
    char *fn
    )
{
    unLkActionT unlock_action = UNLK_NEITHER;

    lk->state = newState;
    return UNLK_BROADCAST;  
}

/* 
 * lk_wait_for
 *
 * This routine will wait until the state of a state variable changes
 * to the desired state.
 *
 * ASSUMPTIONS: The mutex protecting the state lock must be locked by the
 * caller.
 */

void
_lk_wait_for(
    stateLkT *lk,
    spin_t *lk_spin,
    lkStatesT waitState,
    int ln,
    char *fn
    )
{
    int wait = 0;

    if (AdvfsLockStats) {
        AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].lock++;
        AdvfsLockStats->stateLock++;
    }

    if (lk->state != waitState) {
        if (AdvfsLockStats) {
            if (wait) {
                AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].reWait++;
                AdvfsLockStats->stateReWait++;
            } else {
                wait = 1;
            }

            AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].wait++;
            AdvfsLockStats->stateWait++;
        }

        lk->waiters++;

        while (lk->state != waitState) {
            cv_wait( &lk->cv, lk_spin, CV_SPIN, CV_DFLT_FLG );
        }

        lk->waiters--;
    }

}

/* 
 * lk_wait
 *
 * This routine is designed to wait on a lock until any broadcast.  Unlike
 * lk_wait_while, this routine will not wait on a particular state or a
 * particular change.  This routine also will not try to retake the bfaLock
 * when it wakes up.  Therefore, this routine is safe to use when needing to
 * wait on something that may be deallocated (like access structures in
 * state ACC_RECYCLE, ACC_DEALLOC, ACC_INIT_TRANS or pretty much any other
 * state...)
 *
 * The routine can wakeup spuriously, so one cannot count on the thing being
 * waited on to still exist in any particular state.  This is generally
 * better than doing a delay and restarting. 
 *
 * It is assumed that the lk_mutex is locked on entry. It will be unlocked
 * on exit.
 */

void
_lk_wait(
    stateLkT *lk,
    spin_t *lk_spin,
    int ln,
    char *fn
    )
{
    cv_wait( &lk->cv, lk_spin, CV_SPIN, CV_NO_RELOCK);
}

/* 
 * lk_wait_while
 *
 * This routine will wait while the state of a state variable remains
 * set to the specified state.
 *
 * ASSUMPTIONS: The mutex protecting the state lock must be locked by the
 * caller.
 */

void
_lk_wait_while(
    stateLkT *lk,
    spin_t *lk_spin,
    lkStatesT waitState,
    int ln,
    char *fn
    )
{
    int wait = 0;

    if (AdvfsLockStats) {
        AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].lock++;
        AdvfsLockStats->stateLock++;
    }

    if (lk->state == waitState) {
        if (AdvfsLockStats) {
            if (wait) {
                AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].reWait++;
                AdvfsLockStats->stateReWait++;
            } else {
                wait = 1;
            }

            AdvfsLockStats->usageStats[ lk->hdr.lkUsage ].wait++;
            AdvfsLockStats->stateWait++;
        }

        lk->waiters++;

        while (lk->state == waitState) {
            cv_wait( &lk->cv, lk_spin, CV_SPIN, CV_DFLT_FLG );
        }

        lk->waiters--;
    }
}
