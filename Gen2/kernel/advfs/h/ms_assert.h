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
 *      AdvFS Assertion
 *
 */

#ifndef MS_ASSERT_H
#define MS_ASSERT_H

/*
 *  Debug kernels have the symbol OSDEBUG defined and we key off this
 *  to enable our assertions.  AdvFS assertions are OFF in all performance
 *  kernels.
 *
 *  To disable ASSERTs for a kernel that has them compiled in, use
 *  kctune or kwdb to set AdvfsEnableAsserts to 0.
 *
 *  The Bullseye code coverage tools needs to build debug kernels
 *  but we don't want our asserts counted.  Also, we set
 *  ADVFS_RBMT_STRESS for code coverage kernels to force more
 *  code paths to be executed.
 */
#ifndef _BullseyeCoverage 
#   ifdef OSDEBUG
#      define ADVFS_SMP_ASSERT 
#   endif
#else
#   define ADVFS_RBMT_STRESS
#endif

/*
 * Next section determines the declaration for MS_SMP_ASSERT()
 */
#ifdef ADVFS_SMP_ASSERT 
    extern uint32_t AdvfsEnableAsserts;
#   define MS_SMP_ASSERT( ex )                   \
        if (AdvfsEnableAsserts && !(ex)) {       \
            ADVFS_SAD0("AdvFS SMP Assertion failed");  \
        }
#else 
#   define MS_SMP_ASSERT( ex )
#endif
 
/*
 * ADVFS_SMP_RW_LOCK_EQL and ADVFS_SMP_RW_LOCK_NEQL are macros that only
 * work when ADVFS_SMP_ASSERT is defined and SEMAPHORE_DEBUG is defined.
 * The macros will return TRUE if the lock passed in is either equal or not
 * equal to the passed in state.
 */

#ifdef ADVFS_SMP_ASSERT 
#   ifdef SEMAPHORE_DEBUG                                                          
#       define ADVFS_SMP_RW_LOCK_EQL( _lock_addr, _state)                              \
            (rwlock_owned( _lock_addr ) == _state) ? TRUE : FALSE
#       define ADVFS_SMP_RW_LOCK_NEQL( _lock_addr, _state)                             \
            (rwlock_owned( _lock_addr ) != _state) ? TRUE : FALSE
#   else 
#       define ADVFS_SMP_RW_LOCK_EQL( _lock_addr, _state) TRUE
#       define ADVFS_SMP_RW_LOCK_NEQL( _lock_addr, _state) TRUE
#   endif
#else 
#   define ADVFS_SMP_RW_LOCK_EQL( _lock_addr, _state) TRUE
#   define ADVFS_SMP_RW_LOCK_NEQL( _lock_addr, _state) TRUE
#endif

#endif /* MS_ASSERT_H */






