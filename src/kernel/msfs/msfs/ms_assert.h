/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 * @(#)$RCSfile: ms_assert.h,v $ $Revision: 1.1.44.9 $ (DEC) $Date: 2007/06/18 18:37:51 $
 */

#ifndef MS_ASSERT_H
#define MS_ASSERT_H

/*
 * There are 2 MS_XXX_ASSERT() macros:
 *   1. MS_SMP_ASSERT()  - the normal assert; can be turned on in any version
 *                         of the kernel by #defining ADVFS_SMP_ASSERT.
 *   2. MS_DBG_ASSERT()  - a version that exists whenever ADVFS_DEBUG is
 *                         defined.  Note that you can force this assertion
 *                         to skip the coredump and just print an error
 *                         message by setting -DMS_DEBUG_NOCOREDUMP in the
 *                         makefile before compiling.
 *
 *  To remove ASSERTs for product ship, just comment out the #define
 *  of ADVFS_SMP_ASSERT in the next section, and be sure no one mistakenly
 *  #defines ADVFS_DEBUG in the makefile.
 *
 *  To disable ASSERTs for a kernel that has them compiled in, use
 *  sysconfig or sysconfigdb or dbx to set AdvfsEnableAsserts to 0.
 */

#define ADVFS_SMP_ASSERT

/*
 * Next section determines the declaration for MS_SMP_ASSERT()
 */
#ifdef ADVFS_SMP_ASSERT 
 
    extern unsigned int AdvfsEnableAsserts;

#   define MS_SMP_ASSERT( ex )                   \
        if (AdvfsEnableAsserts && !(ex)) {       \
            ADVFS_SAD0("SMP Assertion failed");  \
        }

    int
    macro_sad( char *module,
               int   line,
               char *fmt,
               char *msg,
               long  n1,
               long  n2,
               long  n3);

    /*
     * MS_MACRO_ASSERT - Returns TRUE if AdvfsEnableAsserts is disabled, Returns
     * TRUE if AdvfsEnableAsserts is enabled _AND_ (ex) is TRUE, else
     * MS_MACRO_ASSERT will panic the system.  This MACRO is intended to be used
     * within other MACROs (such as BFSET_VALID, DMN_HTOP, and VD_HTOP).
     *
     * If ADVFS_SMP_ASSERT is not defined (it is only defined during development
     * cycles), then MS_MACRO_ASSERT always returns TRUE.
     */
         
#   define MS_MACRO_ASSERT(ex)                                                        \
        ( AdvfsEnableAsserts && !(ex) ?                                               \
          macro_sad( __FILE__, __LINE__, SadFmt0, "SMP Assertion failed", 0, 0, 0 ) : \
          TRUE )

#else   /* ADVFS_SMP_ASSERT */
 
#   define MS_MACRO_ASSERT(ex)  TRUE
#   define MS_SMP_ASSERT( ex )

#endif  /* ADVFS_SMP_ASSERT */
 

/*
 * Next section determines the declaration for MS_DBG_ASSERT()
 */
#ifdef ADVFS_DEBUG
#   ifdef MS_DEBUG_NOCOREDUMP
#       define MS_DBG_ASSERT( ex )                                             \
            if (!(ex)) {                                                       \
                printf( "Assertion failed: file: \"%s\", line: %d test: %s\n", \
                __FILE__, __LINE__, "ex");                                     \
                enter_kdebug();                                                \
            }
                
#   else  /* MS_DEBUG_NOCOREDUMP */
#       define MS_DBG_ASSERT( ex )                 \
            if (!(ex)) {                           \
                ADVFS_SAD0("Assertion failed");    \
            }
#   endif /* MS_DEBUG_NOCOREDUMP */

#else  /* ADVFS_DEBUG */

#   define MS_DBG_ASSERT( ex )

#endif /* ADVFS_DEBUG */


/* DO NOT SUBMIT - uncomment ADVFS_NO_ZERO_ALLOCATE
 * define only in developer builds for test kernels.
 * #define ADVFS_NO_ZERO_ALLOCATE
 */

#endif /* MS_ASSERT_H */
