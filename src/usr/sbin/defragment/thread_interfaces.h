/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Contains macros that are used by the 
 *      defragment program to call the different types of
 *      thread packages.  
 *
 * Date:
 *
 *      Apr 15 1997
 *
 */
/*
 * HISTORY
 */

#ifndef _THREAD_INTERFACES_
#define _THREAD_INTERFACES_

static char*
rcsid_thread_interfaces = "thread_interfaces.h - $Date: 1998/06/24 17:06:44 $";

#include <pthread.h>

typedef void * t_start_routine;
typedef void * t_address;

typedef pthread_t t_thread;
typedef u_long t_exit_status;
typedef pthread_cond_t t_cond;
typedef pthread_mutex_t t_mutex;


#define cond__create( cvp ) {                                           \
    pthread_condattr_t condattr;                                        \
    if (pthread_condattr_init( &condattr )) {                           \
        abort_prog( "pthread_condattr_init failed; %d %s",              \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
    if (pthread_cond_init( cvp, &condattr )) {                          \
        abort_prog( "pthread_cond_init failed; %d %s",                  \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define mutex__create( mp ) {                                           \
    pthread_mutexattr_t mutexattr;                                      \
    if (pthread_mutexattr_init( &mutexattr )) {                         \
        abort_prog( "pthread_mutexattr_create failed; %d %s",           \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
    if (pthread_mutex_init( mp, &mutexattr )) {                         \
        abort_prog( "pthread_mutex_init failed; %d %s",                 \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define mutex__delete( mp ) {                                           \
    if (pthread_mutex_destroy( mp )) {                                  \
        abort_prog( "pthread_mutex_destroy failed; %d %s",              \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define mutex__lock( m ) {                                              \
    if (pthread_mutex_lock( &(m) )) {                                   \
        abort_prog( "pthread_mutex_lock failed; %d %s",                 \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define mutex__unlock( m ) {                                            \
    if (pthread_mutex_unlock( &(m) )) {                                 \
        abort_prog( "pthread_mutex_unlock failed; %d %s",               \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define cond__delete( cvp ) {                                           \
    if (pthread_cond_destroy( cvp )) {                                  \
        abort_prog( "pthread_cond_destroy failed; %d %s",               \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define cond__wait( cv, m ) {                                           \
    if (pthread_cond_wait( &(cv), &(m) )) {                             \
        abort_prog( "pthread_cond_wait failed; %d %s",                  \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define cond__signal( cv ) {                                            \
    if (pthread_cond_signal( &(cv) )) {                                 \
        abort_prog( "pthread_cond_signal failed; %d %s",                \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define cond__broadcast( cv ) {                                         \
    if (pthread_cond_broadcast( &(cv) )) {                              \
        abort_prog( "pthread_cond_broadcast failed; %d %s",             \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define thread__attr_create( attrp ) {                                  \
    if (pthread_attr_create( attrp )) {                                 \
        abort_prog( "pthread_attr_create failed; %d %s",                \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define thread__attr_setstacksize( attrp, stksize ) {                   \
    if (pthread_attr_setstacksize( attrp, stksize )) {                  \
        abort_prog( "pthread_attr_setstacksize failed; %d %s",          \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define thread__create( handlep, attr, thread, parmsp ) {               \
    if (pthread_create( handlep, attr, thread, parmsp )) {              \
        abort_prog( "pthread_create failed; %d %s",                     \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#define thread__yield() pthread_yield()

#define thread__join( handle, retvalp ) {                               \
    if (pthread_join( handle, retvalp )) {                              \
        abort_prog( "pthread_join failed; %d %s",                       \
                    errno, sys_errlist[errno]);                         \
    }                                                                   \
}

#endif /* _THREAD_INTERFACES_ */

/* end thread_interfaces.h */
