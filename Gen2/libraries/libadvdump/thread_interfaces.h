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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 *
 *
 * Facility:
 *
 *      advfs
 *
 * Abstract:
 *
 *      Contains macros that are used by the prototype
 *      backup and restore programs to call the different types of
 *      thread packages.  
 *
 * Date:
 *
 *      Fri Mar  1 11:01:00 1991
 *
 */

#ifndef _THREAD_INTERFACES_
#define _THREAD_INTERFACES_

#include <pthread.h>

typedef void * any_t;
typedef void * t_start_routine;
typedef void * t_address;

typedef pthread_t t_thread;
typedef uint64_t t_exit_status;
typedef pthread_cond_t t_cond;
typedef pthread_mutex_t t_mutex;

#ifndef MACRO_BEGIN
#define MACRO_BEGIN	do {
#endif

#ifndef MACRO_END
#define MACRO_END	} while (FALSE)
#endif

#define CheckStatus(_s_,_n_)	MACRO_BEGIN		\
    int __s = (_s_);					\
    if (__s) {						\
	abort_prog (					\
		"%s failed; %d %s",			\
		(_n_),					\
		(__s),					\
		sys_errlist[__s]);			\
	} MACRO_END

#define cond__create( _cp_ )							\
    CheckStatus (pthread_cond_init( (_cp_), NULL ), "pthread_cond_init")

#define cond__delete( _cp_ )							\
    CheckStatus (pthread_cond_destroy( _cp_ ), "pthread_cond_destroy")

#define cond__wait( _c_, _m_ )							\
    CheckStatus (pthread_cond_wait( &(_c_), &(_m_) ), "pthread_cond_wait")

#define cond__signal( _c_ )							\
    CheckStatus (pthread_cond_signal( &(_c_) ), "pthread_cond_signal")

#define cond__broadcast( _c_ )							\
    CheckStatus (pthread_cond_broadcast( &(_c_) ), "pthread_cond_broadcast")

#define mutex__create( _mp_ )							\
    CheckStatus (pthread_mutex_init( (_mp_), NULL ), "pthread_mutex_init")

#define mutex__delete( _mp_ )							\
    CheckStatus (pthread_mutex_destroy( _mp_ ), "pthread_mutex_destroy")

#define mutex__lock( _m_ )							\
    CheckStatus (pthread_mutex_lock( &(_m_) ), "pthread_mutex_lock")

#define mutex__unlock( _m_ )							\
    CheckStatus (pthread_mutex_unlock( &(_m_) ), "pthread_mutex_unlock")

#define thread__attr_create( _ap_ )						\
    CheckStatus (pthread_attr_init( _ap_ ), "pthread_attr_init")

#define thread__attr_setstacksize( _a_, _ss_ )					\
    CheckStatus (pthread_attr_setstacksize( &(_a_), (_ss_) ), "pthread_attr_setstacksize")

#define thread__create( _hp_, _a_, _r_, _p_ )					\
    CheckStatus (pthread_create( (_hp_), &(_a_), (_r_), (_p_) ), "pthread_create")

#define thread__join( _h_, _rp_ )						\
    CheckStatus (pthread_join( (_h_), (_rp_) ), "pthread_join")

#define thread__yield() sched_yield()

#endif /* _THREAD_INTERFACES_ */

/* end thread_interfaces.h */
