/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1996                                  *
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
 *      Advanced File System
 *
 * Abstract:
 *
 *      On-disk structure Salvager
 *
 * Date:
 *
 *      Mon Oct 28 12:00:00 PST 1996
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: salvage.h,v $ $Revision: 1.1.34.2 $ (DEC) $Date: 2006/04/06 03:21:23 $
 */

#ifndef SALVAGE_H
#define SALVAGE_H

/*
 * For BUILDING salvage on V4.0x of the OS, we need to ONLY
 * use the old On Disk Structures.  As the h files only know
 * about the old format.
 */
/*#define OLD_ODS*/


#include <nl_types.h>
#include <locale.h>
#include "salvage_msg.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sys/mode.h>
#include <sys/resource.h>
#include <strings.h>
#include <dirent.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/mtio.h>

#ifdef OLD_ODS
/* The ordering of the next three is important */
#include <msfs/bs_public.h>
#include <msfs/ms_shelve.h>
#ifdef ADVFS_SHELVE
#include <msfs/mss_entry.h>
#endif
#include <msfs/ms_generic_locks.h> 
#include <msfs/ftx_public.h>
#endif

#include <msfs/ms_public.h>
#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>
#include <assert.h>

/*
 * This is not currently defined in pax.h, but should be.
 * If it is in the future, the #include will override this definition.
 */
#define DEF_BLOCKING 20

#include "pax.h"

/*
 * Private types.
 */
typedef enum { 
               OW_YES, 
	       OW_NO, 
	       OW_ASK 
} overwrite_t;

typedef enum { 
               FT_DEFAULT,
               FT_REGULAR, 
	       FT_DIRECTORY, 
	       FT_SPECIAL, 
	       FT_FIFO,
	       FT_SYMLINK,
	       FT_UNKNOWN
} fileTypeT;

typedef enum { 
               F_DISK,
               F_TAR, 
	       F_VDUMP
} formatTypeT;



#ifdef DEBUG
#define PROFILE
#endif /* DEBUG */

/*
 * Global defines.
 */
#define CLOSED -1
#define MAX_VOLUMES BS_MAX_VDI

#define ONE_K 1024L
#define PAGESIZE 8192L
#define BLOCKS_PER_PAGE 16
#define TAGNUM_BUFSIZ 24

#define MAX_TAGS 2147483648
#define TAGS_SOFTLIMIT 10000000

#define FAILURE 0
#define SUCCESS 1
#define INVALID 2
#define PARTIAL 3
#define NO_MORE_ENTRIES 4
#define NO_SPACE 5
#define NO_MEMORY 6
#define NO_ENTRIES 7

#define IGNORE (void *) -1
#define DEAD   (void *) -2

#define MISSING_PARENT -1

#ifdef OLD_ODS
#define XTNT_TERM ((uint32T)-1)
#define PERM_HOLE_START ((uint32T)-2)
#endif

#define XTNT_TERM ((uint32T)-1)
#define PERM_HOLE_START ((uint32T)-2)
#define CORRUPT_RECORD ((uint32T)-2)

/*
 * Symbolic names for node processing order in walk_tree().
 */
#define PARENT_FIRST 0
#define CHILD_FIRST 1

/*
 * Symbolic names for types of sort actions, in sort_linked_list().
 */
#define COUNT_UNKNOWN          0
#define LISTSORT_NODE_NAME     0
#define LISTSORT_NODE_TAGNUM   1
#define LISTSORT_EXTENT_OFFSET 2

/*
 * Global Exit Values.
 */
#define EXIT_SUCCESS 0
#define EXIT_PARTIAL 1
#define EXIT_FAILED  2

/*
 * Symbolic names for verbosity levels, in writemsg().
 */
#define SV_ALL     0
#define SV_NORMAL  1
#define SV_VERBOSE 2
#define SV_DEBUG   3
#define SV_DATE    8    /* Single bit, to be or'ed with severity code */
#define SV_ERR     16
#define SV_CONT    32

/*
 * Define a "maximum error message length", used in writemsg().
 */
#define MAXERRMSG  256

/*
 * Symbolic names for "requested path found" actions, in insert_filenames().
 */
#define REQUESTED_PATH_NOCHECK   -2
#define REQUESTED_PATH_INITVAL   -1
#define REQUESTED_PATH_NOT_FOUND  0
#define REQUESTED_PATH_FOUND      1
#define READ_ERROR_OCCURRED       2

/*
 * Symbolic names for directory entry states, in get_dirrec_state().
 */
#define DIRENTRY_VALID              0
#define DIRENTRY_NEVERUSED          1
#define DIRENTRY_DELETED            2
#define DIRENTRY_CORRUPT_NAMLEN    -1
#define DIRENTRY_CORRUPT           -2
#define DIRENTRY_CORRUPT_MISMATCH  -3
#define DIRENTRY_BADCHAR           -4
#define DIRENTRY_CORRUPT_PAGE      -5

/*
 * Domain Bit Status
 */
#define D_RESTORE            0x00000001
#define D_NOT_RESTORE        0x00000002
#define D_RESTORE_MASK       0x0000000F

#define D_PASS1_NEEDED       0x00000010
#define D_PASS1_COMPLETED    0x00000020
#define D_PASS1_MASK         0x000000F0

#define D_PASS2_NEEDED       0x00000100
#define D_PASS2_COMPLETED    0x00000200
#define D_PASS2_DEFAULT      0x00000400
#define D_PASS2_MASK         0x00000F00

#define D_PASS3_NEEDED       0x00001000
#define D_PASS3_COMPLETED    0x00002000
#define D_PASS3_DEFAULT      0x00004000
#define D_PASS3_MASK         0x0000F000

#define D_VOL_SETUP_NEEDED    0x00010000
#define D_VOL_SETUP_COMPLETED 0x00020000
#define D_VOL_SETUP_PASS3     0x00040000
#define D_VOL_SETUP_MASK      0x000F0000

/*
 * Set Domain Bit Status
 */
#define D_SET_RESTORE(s)         (s) = (((s)&(~D_RESTORE_MASK))|(D_RESTORE))
#define D_SET_NOT_RESTORE(s)     (s) = (((s)&(~D_RESTORE_MASK))|(D_NOT_RESTORE))

#define D_SET_PASS1_NEEDED(s)    (s) = (((s)&(~D_PASS1_MASK))|(D_PASS1_NEEDED))
#define D_SET_PASS1_COMPLETED(s) (s) = (((s)&(~D_PASS1_MASK))|(D_PASS1_COMPLETED))

#define D_SET_PASS2_NEEDED(s)    (s) = (((s)&(~D_PASS2_MASK))|(D_PASS2_NEEDED))
#define D_SET_PASS2_COMPLETED(s) (s) = (((s)&(~D_PASS2_MASK))|(D_PASS2_COMPLETED))
#define D_SET_PASS2_DEFAULT(s)   (s) = (((s)&(~D_PASS2_MASK))|(D_PASS2_DEFAULT))

#define D_SET_PASS3_NEEDED(s)    (s) = (((s)&(~D_PASS3_MASK))|(D_PASS3_NEEDED))
#define D_SET_PASS3_COMPLETED(s) (s) = (((s)&(~D_PASS3_MASK))|(D_PASS3_COMPLETED))
#define D_SET_PASS3_DEFAULT(s)   (s) = (((s)&(~D_PASS3_MASK))|(D_PASS3_DEFAULT))

#define D_SET_VOL_SETUP_NEEDED(s)    (s) = (((s)&(~D_VOL_SETUP_MASK))|(D_VOL_SETUP_NEEDED))
#define D_SET_VOL_SETUP_COMPLETED(s) (s) = (((s)&(~D_VOL_SETUP_MASK))|(D_VOL_SETUP_COMPLETED))
#define D_SET_VOL_SETUP_PASS3(s)     (s) = (((s)&(~D_VOL_SETUP_MASK))|(D_VOL_SETUP_PASS3))

/*
 * Check Domain Bit Status
 */
#define D_IS_RESTORE(s)          (((s)&(D_RESTORE_MASK)) == (D_RESTORE))
#define D_IS_NOT_RESTORE(s)      (((s)&(D_RESTORE_MASK)) == (D_NOT_RESTORE))

#define D_IS_PASS1_NEEDED(s)     (((s)&(D_PASS1_MASK)) == (D_PASS1_NEEDED))
#define D_IS_PASS1_COMPLETED(s)  (((s)&(D_PASS1_MASK)) == (D_PASS1_COMPLETED))

#define D_IS_PASS2_NEEDED(s)     (((s)&(D_PASS2_MASK)) == (D_PASS2_NEEDED))
#define D_IS_PASS2_COMPLETED(s)  (((s)&(D_PASS2_MASK)) == (D_PASS2_COMPLETED))
#define D_IS_PASS2_DEFAULT(s)    (((s)&(D_PASS2_MASK)) == (D_PASS2_DEFAULT))

#define D_IS_PASS3_NEEDED(s)     (((s)&(D_PASS3_MASK)) == (D_PASS3_NEEDED))
#define D_IS_PASS3_COMPLETED(s)  (((s)&(D_PASS3_MASK)) == (D_PASS3_COMPLETED))
#define D_IS_PASS3_DEFAULT(s)    (((s)&(D_PASS3_MASK)) == (D_PASS3_DEFAULT))

#define D_IS_VOL_SETUP_NEEDED(s)    (((s)&(D_VOL_SETUP_MASK)) == (D_VOL_SETUP_NEEDED))   
#define D_IS_VOL_SETUP_COMPLETED(s) (((s)&(D_VOL_SETUP_MASK)) == (D_VOL_SETUP_COMPLETED))
#define D_IS_VOL_SETUP_PASS3(s)     (((s)&(D_VOL_SETUP_MASK)) == (D_VOL_SETUP_PASS3))

/*
 * Fileset Bit Status
 */
#define FS_RESTORE            0x00000001
#define FS_NOT_RESTORE        0x00000002
#define FS_RESTORE_MASK       0x0000000F

#define FS_NO_CLONE           0x00000010
#define FS_HAS_CLONE          0x00000020
#define FS_CLONE              0x00000040
#define FS_CLONE_MASK         0x000000F0

#define FS_PASS1_NEEDED       0x00000100
#define FS_PASS1_COMPLETED    0x00000200
#define FS_PASS1_MASK         0x00000F00

#define FS_PASS2_NEEDED       0x00001000
#define FS_PASS2_COMPLETED    0x00002000
#define FS_PASS2_DEFAULT      0x00004000
#define FS_PASS2_MASK         0x0000F000

/*
 * Set Fileset Bit Status
 */
#define FS_SET_RESTORE(s)         (s) = (((s)&(~FS_RESTORE_MASK))|(FS_RESTORE))
#define FS_SET_NOT_RESTORE(s)     (s) = (((s)&(~FS_RESTORE_MASK))|(FS_NOT_RESTORE))

#define FS_SET_NO_CLONE(s)        (s) = (((s)&(~FS_CLONE_MASK))|(FS_NO_CLONE))
#define FS_SET_HAS_CLONE(s)       (s) = (((s)&(~FS_CLONE_MASK))|(FS_HAS_CLONE))
#define FS_SET_CLONE(s)           (s) = (((s)&(~FS_CLONE_MASK))|(FS_CLONE))

#define FS_SET_PASS1_NEEDED(s)    (s) = (((s)&(~FS_PASS1_MASK))|(FS_PASS1_NEEDED))
#define FS_SET_PASS1_COMPLETED(s) (s) = (((s)&(~FS_PASS1_MASK))|(FS_PASS1_COMPLETED))

#define FS_SET_PASS2_NEEDED(s)    (s) = (((s)&(~FS_PASS2_MASK))|(FS_PASS2_NEEDED))
#define FS_SET_PASS2_COMPLETED(s) (s) = (((s)&(~FS_PASS2_MASK))|(FS_PASS2_COMPLETED))
#define FS_SET_PASS2_DEFAULT(s)   (s) = (((s)&(~FS_PASS2_MASK))|(FS_PASS2_DEFAULT))

/*
 * Check Fileset Bit Status
 */
#define FS_IS_RESTORE(s)          (((s)&(FS_RESTORE_MASK)) == (FS_RESTORE))
#define FS_IS_NOT_RESTORE(s)      (((s)&(FS_RESTORE_MASK)) == (FS_NOT_RESTORE))

#define FS_IS_NO_CLONE(s)         (((s)&(FS_CLONE_MASK)) == (FS_NO_CLONE))
#define FS_IS_HAS_CLONE(s)        (((s)&(FS_CLONE_MASK)) == (FS_HAS_CLONE))
#define FS_IS_CLONE(s)            (((s)&(FS_CLONE_MASK)) == (FS_CLONE))

#define FS_IS_PASS1_NEEDED(s)     (((s)&(FS_PASS1_MASK)) == (FS_PASS1_NEEDED))
#define FS_IS_PASS1_COMPLETED(s)  (((s)&(FS_PASS1_MASK)) == (FS_PASS1_COMPLETED))

#define FS_IS_PASS2_NEEDED(s)     (((s)&(FS_PASS2_MASK)) == (FS_PASS2_NEEDED))
#define FS_IS_PASS2_COMPLETED(s)  (((s)&(FS_PASS2_MASK)) == (FS_PASS2_COMPLETED))
#define FS_IS_PASS2_DEFAULT(s)    (((s)&(FS_PASS2_MASK)) == (FS_PASS2_DEFAULT))

/*
 * File Tag Bit status
 */
#define S_EMPTY          0x00000000
#define S_PARTIAL        0x00000001
#define S_MORE           0x00000002
#define S_COMPLETE       0x00000003
#define S_FDAT_MASK      0x00000003

#define S_NOT_STRIPED    0x00000000
#define S_STRIPED        0x00000010
#define S_STRIPE_MASK    0x00000010

#define S_NO_TRASHCAN    0x00000000
#define S_TRASHCAN       0x00000020
#define S_TRASHCAN_MASK  0x00000020

#define S_REAL           0x00000000
#define S_CREATED        0x00000100
#define S_REALNAME_MASK  0x00000100

#define S_ORIGINAL       0x00000000
#define S_ALTERED        0x00000200
#define S_ORIGNAME_MASK  0x00000200

#define S_NO_GEN_NAMES   0x00000000
#define S_GEN_NAMES      0x00000400
#define S_GENNAME_MASK   0x00000400

#define S_NO_ALT_NAMES   0x00000000
#define S_ALT_NAMES      0x00000800
#define S_ALTNAME_MASK   0x00000800

#define S_NOT_EXT_SORTED 0x00000000
#define S_EXT_SORTED     0x00001000
#define S_EXTSORT_MASK   0x00001000

#define S_NOT_RECOVERED  0x00000000
#define S_RECOVERED      0x00010000
#define S_RECOV_MASK     0x00010000

#define S_NLINKS_OK      0x00000000
#define S_TOO_MANY_LINKS 0x00100000
#define S_TOO_FEW_LINKS  0x00200000
#define S_LINKS_MASK     0x00300000

#define S_NOT_CORRUPT    0x00000000
#define S_CORRUPT        0x00800000
#define S_CORRUPT_MASK   0x00800000

#define S_PL_NONE        0x00000000
#define S_PL_COMPLETE    0x01000000
#define S_PL_PARTIAL     0x02000000
#define S_PL_MASK        0x03000000


/*
 * Set File Tag Bit status
 */
#define S_SET_EMPTY(s)          (s) = (((s)&(~S_FDAT_MASK))|(S_EMPTY))
#define S_SET_PARTIAL(s)        (s) = (((s)&(~S_FDAT_MASK))|(S_PARTIAL))
#define S_SET_MORE(s)           (s) = (((s)&(~S_FDAT_MASK))|(S_MORE))
#define S_SET_COMPLETE(s)       (s) = (((s)&(~S_FDAT_MASK))|(S_COMPLETE))

#define S_SET_NOT_STRIPED(s)    (s) = (((s)&(~S_STRIPE_MASK))|(S_NOT_STRIPED))
#define S_SET_STRIPED(s)        (s) = (((s)&(~S_STRIPE_MASK))|(S_STRIPED))

#define S_SET_NO_TRASHCAN(s)    (s) = (((s)&(~S_TRASHCAN_MASK))|(S_NO_TRASHCAN))
#define S_SET_TRASHCAN(s)       (s) = (((s)&(~S_TRASHCAN_MASK))|(S_TRASHCAN))

#define S_SET_REAL(s)           (s) = (((s)&(~S_REALNAME_MASK))|(S_REAL))
#define S_SET_CREATED(s)        (s) = (((s)&(~S_REALNAME_MASK))|(S_CREATED))

#define S_SET_ORIGINAL(s)       (s) = (((s)&(~S_ORIGNAME_MASK))|(S_ORIGINAL))
#define S_SET_ALTERED(s)        (s) = (((s)&(~S_ORIGNAME_MASK))|(S_ALTERED))

#define S_SET_NO_GEN_NAMES(s)   (s) = (((s)&(~S_GENNAME_MASK))|(S_NO_GEN_NAMES))
#define S_SET_HAS_GEN_NAMES(s)  (s) = (((s)&(~S_GENNAME_MASK))|(S_GEN_NAMES))

#define S_SET_NO_ALT_NAMES(s)   (s) = (((s)&(~S_ALTNAME_MASK))|(S_NO_ALT_NAMES))
#define S_SET_HAS_ALT_NAMES(s)  (s) = (((s)&(~S_ALTNAME_MASK))|(S_ALT_NAMES))

#define S_SET_NOT_EXT_SORTED(s) (s) = (((s)&(~S_EXTSORT_MASK))|(S_NOT_EXT_SORTED))
#define S_SET_EXT_SORTED(s)     (s) = (((s)&(~S_EXTSORT_MASK))|(S_EXT_SORTED))

#define S_SET_NOT_RECOVERED(s)  (s) = (((s)&(~S_RECOV_MASK))|(S_NOT_RECOVERED))
#define S_SET_RECOVERED(s)      (s) = (((s)&(~S_RECOV_MASK))|(S_RECOVERED))

#define S_SET_NLINKS_OK(s)      (s) = (((s)&(~S_LINKS_MASK))|(S_NLINKS_OK))
#define S_SET_TOO_MANY_LINKS(s) (s) = (((s)&(~S_LINKS_MASK))|(S_TOO_MANY_LINKS))
#define S_SET_TOO_FEW_LINKS(s)  (s) = (((s)&(~S_LINKS_MASK))|(S_TOO_FEW_LINKS))

#define S_SET_NOT_CORRUPT(s)    (s) = (((s)&(~S_CORRUPT_MASK))|(S_NOT_CORRUPT))
#define S_SET_CORRUPT(s)        (s) = (((s)&(~S_CORRUPT_MASK))|(S_CORRUPT))

#define S_SET_PL_NONE(s)        (s) = (((s)&(~S_PL_MASK))|(S_PL_NONE))
#define S_SET_PL_COMPLETE(s)    (s) = (((s)&(~S_PL_MASK))|(S_PL_COMPLETE))
#define S_SET_PL_PARTIAL(s)     (s) = (((s)&(~S_PL_MASK))|(S_PL_PARTIAL))

/*
 * Check File Tag Bit status
 */
#define S_IS_EMPTY(s)          (((s)&(S_FDAT_MASK)) == (S_EMPTY))
#define S_IS_PARTIAL(s)        (((s)&(S_FDAT_MASK)) == (S_PARTIAL))
#define S_IS_MORE(s)           (((s)&(S_FDAT_MASK)) == (S_MORE))
#define S_IS_COMPLETE(s)       (((s)&(S_FDAT_MASK)) == (S_COMPLETE))

#define S_IS_NOT_STRIPED(s)    (((s)&(S_STRIPE_MASK)) == (S_NOT_STRIPED))
#define S_IS_STRIPED(s)        (((s)&(S_STRIPE_MASK)) == (S_STRIPED))

#define S_IS_NO_TRASHCAN(s)    (((s)&(S_TRASHCAN_MASK)) == (S_NO_TRASHCAN))
#define S_IS_TRASHCAN(s)       (((s)&(S_TRASHCAN_MASK)) == (S_TRASHCAN))

#define S_IS_REAL(s)           (((s)&(S_REALNAME_MASK)) == (S_REAL))
#define S_IS_CREATED(s)        (((s)&(S_REALNAME_MASK)) == (S_CREATED)) 

#define S_IS_ORIGINAL(s)       (((s)&(S_ORIGNAME_MASK)) == (S_ORIGINAL))
#define S_IS_ALTERED(s)        (((s)&(S_ORIGNAME_MASK)) == (S_ALTERED))

#define S_HAS_NO_GEN_NAMES(s) (((s)&(S_GENNAME_MASK)) == (S_NO_GEN_NAMES))
#define S_HAS_GEN_NAMES(s)    (((s)&(S_GENNAME_MASK)) == (S_GEN_NAMES))

#define S_HAS_NO_ALT_NAMES(s) (((s)&(S_ALTNAME_MASK)) == (S_NO_ALT_NAMES))
#define S_HAS_ALT_NAMES(s)    (((s)&(S_ALTNAME_MASK)) == (S_ALT_NAMES))

#define S_IS_NOT_EXT_SORTED(s) (((s)&(S_EXTSORT_MASK)) == (S_NOT_EXT_SORTED))
#define S_IS_EXT_SORTED(s)     (((s)&(S_EXTSORT_MASK)) == (S_EXT_SORTED))

#define S_IS_NOT_RECOVERED(s)  (((s)&(S_RECOV_MASK)) == (S_NOT_RECOVERED))
#define S_IS_RECOVERED(s)      (((s)&(S_RECOV_MASK)) == (S_RECOVERED))

#define S_IS_NLINKS_OK(s)      (((s)&(S_LINKS_MASK)) == (S_NLINKS_OK))
#define S_IS_TOO_MANY_LINKS(s) (((s)&(S_LINKS_MASK)) == (S_TOO_MANY_LINKS))
#define S_IS_TOO_FEW_LINKS(s)  (((s)&(S_LINKS_MASK)) == (S_TOO_FEW_LINKS))

#define S_IS_NOT_CORRUPT(s)    (((s)&(S_CORRUPT_MASK)) == (S_NOT_CORRUPT))
#define S_IS_CORRUPT(s)        (((s)&(S_CORRUPT_MASK)) == (S_CORRUPT))

#define S_IS_PL_NONE(s)        (((s)&(S_PL_MASK)) == (S_PL_NONE))
#define S_IS_PL_COMPLETE(s)    (((s)&(S_PL_MASK)) == (S_PL_COMPLETE))
#define S_IS_PL_PARTIAL(s)     (((s)&(S_PL_MASK)) == (S_PL_PARTIAL))

#define S_SET_DEFAULT(s)       (s) = (0x00000000)

/*
 * Global data
 */
static char *Prog        = "salvage";
struct options    Options;
static FILE *Localtty = stdin;

/*
 * Macros
 */
#define ERRMSG( eno ) sys_errlist[( eno )]

#define CHECK_MALLOC(ptr,funcName,size) \
    if( NULL == ptr ) { \
        fprintf( stderr, "%s: malloc failed, %d bytes\n", Prog, size ); \
        perror( funcName ); \
        return FAILURE; }

#define IS_VALID_TAG_POINTER(pTag) \
    ( pTag != NULL && pTag != DEAD && pTag != IGNORE )

/*
 * Property list Macros.
 */
#define ALIGN(x1) (((x1) + 7) &~ 7)
#define PL_ENTRY_SIZE(x1, x2) (ALIGN(x1)+ALIGN(x2))


/*
 * Salvage structures
 */

/* optionT - Section 2.1.1 */
typedef struct options {
    FILE                     *logFD;       /* Stream Descriptor of Log file */
    int                      recPartial;   /* Recover Partial Files       */
    int                      addSuffix;    /* Add suffix to Partial Files */
    int                      verboseLog;   /* Verbose Log entries         */
    int                      verboseOutput;/* Verbose I/O                 */
    long                     tagSoftLimit; /* Soft limit on tag array     */
    long                     tagHardLimit; /* Hard limit on tag array     */
    struct timeval           recoverDate;  /* Recover files M/C after     */
    char                     *recoverDir;  /* Place recover files here    */
    overwrite_t              overwrite;    /* Overwrite files if needed   */
    char                     *setName;     /* Fileset Name : NULL == ALL  */
    char                     *pathName;    /* Dir Path to restore         */
    formatTypeT              outputFormat; /* Save to disk, tar, or other */
    char                     *archiveName; /* Output file for tar format  */
    int                      stdoutArchive; /* Boolean if archiving to stdout*/
    int                      progressReport; /* progress reports */
} optionsT;

/* domainInfoT - Section 2.1.2 */
typedef struct domainInfo {
    bfDomainIdT              dmnId;	   /* Domain Id                   */
    int                      recoverType;  /* 0 = All, 1 = Single FS      */
    int                      status;       /* A bit mask status field     */
    struct filesetLL         *filesets;    /* Head of fileset LL          */
    struct volMap            *volumes;     /* Ptr to volume Array         */
    int                      numberVolumes;/* # volumes this domain has   */
    int                      version;      /* Version number of domain    */
} domainInfoT;

/* volMapT - Section 2.1.3 */
typedef struct volMap {
    int                      volFd;          /* File descriptor of Volume */
    int                      volBlockFd;     /* File desc of block device */
    int                      volRawFd;       /* File desc of raw device   */
    int                      lsm;            /* Indicator of lsm usage    */	
    int			     badVolNum;      /* Flag if can't find volnum */
    char                     *volName;       /* Volume Name               */
    struct extent            *extentArray;   /* Extent to LBN mapping     */
    long                     extentArraySize;/* Size of extentArray       */
    struct extent            *rbmtArray;     /* Extent to LBN mapping     */
    long                     rbmtArraySize;  /* Size of rbmtArray         */
} volMapT;

/* filesetLLT - Section 2.1.4 */
typedef struct filesetLL {
    bfSetIdT                 filesetId;    /* from bs_public.h            */
    domainInfoT              *domain;      /* Ptr back to domain struct   */
    int                      tagArraySize; /* Size of array : Pass 2 info */
    int                      status;       /* ie Pass 2 needed            */
    char                     *fsName;      /* Fileset Name                */
    struct filesetTag        **tagArray;   /* Ptr to array of pointers    */
    struct filesetTreeNode   *dirTreeHead; /* Ptr to head of tree         */
    struct extent            *fragLbnArray;/* Ptr to Array of LBN mapping */
    long                     fragLbnSize;  /* Size of fragLbnArray        */
    struct mcell             *tagFileMcell;/* Ptr to mcell of Tag File    */
    struct filesetLinks      *hardLinks;   /* Ptr to head of hardlink LL  */
    struct filesetStats      *statistics;  /* Ptr to statistics data      */
    struct filesetQuota      *quota;       /* Ptr to Quota data           */
    struct filesetLL         *next;        /* Ptr to Next fileset         */
    long                     maxSize;      /* Max size of array: Alloc    */
    long                     activeNodes;  /* Number of active nodes      */
} filesetLLT;

/* mcellT - Section 2.1.5 */
typedef struct mcell {
    int                      vol;          /* volume index                */
    int                      page;         /* BMT page number             */ 
    int                      cell;         /* Cell number in BMT Page     */
} mcellT;

/* filesetTagT - Section 2.1.6 */
typedef struct filesetTag {
    int                      status;         /* Recovery Status of tag    */
    fileTypeT                fileType;       /* (reg,dir,special,...)     */
    int                      linksFound;     /* Increment per tree entry  */
    unsigned int             seqNum;         /* The tags sequence number  */
    long                     bytesFound;     /* Increment per extent      */
    struct extentLL          *extentHead;    /* Ptr to head of Extent LL  */
    struct extentLL          *extentTail;    /* Ptr to tail of Extent LL  */
    struct filesetTreeNode   *firstInstance; /* Used for create hardlinks */
    struct tagAttribute      *attrs;         /* Ptr to file Attributes    */
    struct tagPropList       *props;	     /* Ptr to Property List LL   */
    void                     *addAttrs;      /* Ptr to Additional Attrs   */

} filesetTagT;

typedef uint32T                 LBNT;

/* extentLLT - Section 2.1.7 */
typedef struct extentLL {
    int                      volume;     /* Used as index into volMap array */
    LBNT                     diskBlock;  /* Logical Block on Disk           */
    long                     extentSize; /* Size of Extent in Bytes         */
    long                     byteInFile; /* Logical Byte in File            */
    struct extentLL          *next;
} extentLLT;

/* tagAttributeT - Section 2.1.8 */
typedef struct tagAttribute {
    int                      mode;       /* File Permission                 */
    int                      numLinks;   /* Number of hard links            */
    long                     size;       /* Size of file                    */
    uid_t                    uid;        /* User id                         */
    gid_t                    gid;        /* Group id                        */
    struct timeval           mtime;      /* Modification time               */
    struct timeval           atime;      /* Access time                     */
    struct timeval           ctime;      /* Creation time                   */
} tagAttributeT;

/* tagPropListT - Section 2.1.9 */

#define PL_COMPLETE   1
#define PL_HEAD       2
#define PL_DATA       3
#define PL_INCOMPLETE 4

typedef struct tagPropList {
    int                     plType;       /* Which union type to use */
    unsigned int            pl_num;

    union {
        struct { 
	    long            flag;
	    unsigned int    nameLen;     /* Length of Name in Buffer       */
	    unsigned int    valueLen;    /* Length of Value in Buffer      */
	    unsigned int    tail;        /* Last char written in valBuffer */
	    char            *nameBuffer; /* Buffer for Name                */
	    char            *valBuffer;  /* Buffer for Value               */
	} pl_complete;

	struct { 
	    long            flag;
	    unsigned int    nameLen;     /* Length of Name in Buffer       */
	    unsigned int    valueLen;    /* Length of Value in Buffer      */
	    unsigned int    valSize;     /* Curent val buffer size         */
	    char            *nameBuffer; /* Buffer for Name                */
	    char            *valBuffer;  /* Buffer for Value               */
	} pl_head;

	struct { 
	    unsigned int    pl_seg;
	    unsigned int    bufferSize;
	    char            *buffer;     
	} pl_data;
    } pl_union;

    struct tagPropList      *next;  
} tagPropListT;

/* filesetTreeNodeT - Section 2.1.10 */
typedef struct filesetTreeNode {
    char                     *name;        /* File Name, without Path   */
    fileTypeT                fileType;     /* Regular, Dir, Symlink     */
    int                      parentTagNum; /* -2 for tree root          */
    struct filesetTreeNode   *parent;      /* NULL for tree root        */
    struct filesetTreeNode   *children;
    struct filesetTreeNode   *nextSibling; /* NOTE Special case for L&F */
    struct filesetTreeNode   *prevSibling;
    int                      tagNum;  
} filesetTreeNodeT;

/*extentT - Section 2.1.11 */
typedef struct extent {
    int                      volume;       /* Volume Page is on   */
    int                      page;         /* Page to map from    */
    LBNT                     lbn;          /* LBN to map to       */
} extentT;

/* filesetLinksT - Section 2.1.12 */
typedef struct filesetLinks {
    int                      tag;
    struct filesetLinks      *nextTag;
    struct tagLinks          *links;
} filesetLinksT;

/* tagLinksT - Section 2.1.13 */
typedef struct tagLinks {     
    struct filesetTreeNode   *treeNode;    /* Ptr to node in tree for link */
    struct tagLinks          *next;
} tagLinksT;

/* fsDirDataT - Section 2.1.14 */
typedef struct fsDirData {
    struct filesetLL  *fileset;       /* Ptr to current fileset data       */ 
    struct volMap     *volumes;       /* Ptr to domain's volume map data   */
    int               dirTagNum;      /* Tag number for current directory  */
    char              fsDirBuf[PAGESIZE];/* Buffer of Tag/Name data        */
    int               nPages;         /* Total size, of all extents        */
    int               nPagesInExtent; /* Total size, of the current extent */
    struct extentLL   *currExtentRec; /* Ptr to current extent record      */
    int               currExtentPage; /* Page within the current extent    */
    int               currentOffset;  /* Byte offset of current entry      */
    fs_dir_entry      *currDirEntry;  /* Ptr for stepping thru dir data    */
} fsDirDataT;

/* filesetStatsT - Section 2.1.n */
typedef struct filesetStats {
    unsigned long  nodesInTree;
    unsigned long  tagsInUse;
    unsigned long  tagsSetIgnore;
    unsigned long  totalDirFiles;
    unsigned long  filesLostFound;
    unsigned long  bytesInAllFiles;
    unsigned long  filesRecoveredFull;
    unsigned long  filesRecoveredPartial;
    unsigned long  filesNotRecovered;
    unsigned long  filesLargerThanExpected;
    unsigned long  filesMissingAttrs;
} filesetStatsT;

/* filesetQuotaT - Section 2.1.n */
typedef struct filesetQuota {
    unsigned long  hardBlockLimit;
    unsigned long  softBlockLimit;
    unsigned long  hardFileLimit;
    unsigned long  softFileLimit;

    time_t         blockTimeLimit;
    time_t         fileTimeLimit;
} filesetQuotaT;

/* procNodeArgsT - Section 3.54.3 */
typedef struct procNodeArgs {
    filesetLLT *fileset;        /* Pointer to fileset */
    int        *reqPathFound;   /* Pointer to "requested path found" flag. */
} procNodeArgsT;

/* recoverDataT - Section 3.65.3 */
typedef struct recoverData {
     struct filesetTag **tagArray; /* Ptr to tag array for fileset          */
     struct volMap     *volumes;  /* Ptr to the volume information         */
     char              *pathName; /* Ptr to path name for the current node */
     dev_t             fsId; /* Fileset tagnum -- needed for tar hardlinks */
     unsigned long     totalFilesFound; /* total files found               */
     unsigned long     dirFiles;  /* total directories found */
     unsigned long     successFiles; /* total successfully recovered files */ 
     unsigned long     partialFiles; /* total partially recovered files    */
     unsigned long     largerFiles; /* total files larger than expected */
     unsigned long     unrecoveredFiles; /* total unrecovered files        */
     unsigned long     noAttributeFiles; /* total files w/no attributes */
} recoverDataT;


/* fragInfoT */
typedef struct fragInfo {
  long      fragOffset;
  long      fragType;
  long      fileSize;
  long      byteInFile;
  long      fragSize;
  long      fragPage;
  long      pageOffset;
  long      pageSpaceAvail;
  long      firstFrag;
  long      secondFrag;
  int       volNumber;
} fragInfoT;

/*====================================
 * profiling stuff
 *===================================*/

#ifdef PROFILE

typedef enum { 
    FUNC_USAGE,
    FUNC_MAIN,
    FUNC_BUILD_TREE,
    FUNC_P2_BUILD_TREE,
    FUNC_P3_BUILD_TREE,
    FUNC_LOAD_TAG_FROM_MCELL,
    FUNC_P2_LOAD_TAG_FROM_MCELL,
    FUNC_LOAD_FILESET_TAG_EXTENTS,
    FUNC_LOAD_FILESETS,
    FUNC_PROCESS_ROOT_TAG_FILE_PAGE,
    FUNC_LOAD_BMT_LBN_ARRAY,
    FUNC_LOAD_FRAG_ARRAY,
    FUNC_P2_LOAD_FRAG_ARRAY,
    FUNC_FIND_NUMBER_EXTENTS,
    FUNC_LOAD_DOMAIN_ATTRIBUTES,
    FUNC_ADD_EXTENTS_TO_TAG,
    FUNC_P2_ADD_EXTENTS_TO_TAG,
    FUNC_ADD_FRAG_EXTENT_TO_TAG,
    FUNC_P2_ADD_FRAG_RECORD_TO_TAG,
    FUNC_P2_ADD_FRAG_EXTENT_TO_TAG,
    FUNC_SORT_AND_FILL_EXTENTS,
    FUNC_FILL_EXTENT_HOLE,
    FUNC_TRIM_REQUESTED_PATHNAME,
    FUNC_TRIM_REQUESTED_TAGNUM,
    FUNC_LOAD_TAG_FROM_TAGNUM,
    FUNC_FIND_TAG_PRIMARY_MCELL_LOCATION,
    FUNC_PRUNE_SIBLINGS,
    FUNC_DELETE_TAG_ARRAY_ENTRY,
    FUNC_BUILD_CLEANUP,
    FUNC_P3_PROCESS_VALID_PAGE,
    FUNC_FIND_FILESET,
    FUNC_ENLARGE_TAG_ARRAY,
    FUNC_ADD_FIRST_INSTANCE,
    FUNC_VALIDATE_FILESET,
    FUNC_ADD_STRIPED_EXTENTS_TO_TAG,
    FUNC_CREATE_CACHE,
    FUNC_COMPARE_PAGES,
    FUNC_READ_PAGE_FROM_CACHE,
    FUNC_INSERT_FILENAMES,
    FUNC_PROCESS_DIR_DATA,
    FUNC_GET_NEXT_DIRENTRY,
    FUNC_CHECK_NODE_IN_PATH,
    FUNC_CHECK_NAME_IN_PATH,
    FUNC_SETUP_VOLUME,
    FUNC_P3_SETUP_VOLUME,
    FUNC_SETUP_BMT,
    FUNC_SETUP_RBMT,
    FUNC_SETUP_FILESET,
    FUNC_SETUP_TAG_2,
    FUNC_SETUP_FS_TREE,
    FUNC_CREATE_VOLUMES,
    FUNC_CREATE_TAG_ARRAY_ELEMENT,
    FUNC_CREATE_UNNAMED_TREE_NODE,
    FUNC_CREATE_EXTENT_ELEMENT,
    FUNC_CREATE_TAG_ATTR,
    FUNC_CREATE_TAG_PROP_LIST,
    FUNC_LOAD_TAG_PROP_DATA,
    FUNC_P2_CREATE_TAG_PROP_LIST,
    FUNC_CREATE_FILESET_ELEMENT,
    FUNC_FIND_NUMBER_TAGS,
    FUNC_FIND_LAST_BMT_EXTENT,
    FUNC_LOAD_ROOT_TAG_FILE,
    FUNC_FIND_BMT_CHAIN_PTR,
    FUNC_BMT_PAGE_TO_LBN,
    FUNC_FRAG_PAGE_TO_LBN,
    FUNC_CONVERT_PAGE_TO_LBN,
    FUNC_FIND_NEXT_MCELL,
    FUNC_GET_DOMAIN_VERSION,
    FUNC_VALIDATE_PAGE,
    FUNC_P2_CREATE_TAG_PROP_HEAD,
    FUNC_P2_CREATE_TAG_PROP_DATA,
    FUNC_P2_CREATE_TAG_PROP_FULL,
    FUNC_FIND_LAST_RBMT_EXTENT,
    FUNC_LOAD_RBMT_LBN_ARRAY,
    FUNC_SETUP_TAR,
    FUNC_CREATE_LIST,
    FUNC_RANDOM_LEVEL,
    FUNC_INSERT_S_NODE,
    FUNC_DELETE_S_NODE,
    FUNC_FIND_S_NODE,
    FUNC_FIND_S_NODE_OR_NEXT,
    FUNC_FIND_FIRST_S_NODE,
    FUNC_FIND_NEXT_S_NODE,
    FUNC_RECOVER_FILESET,
    FUNC_RECOVER_FILE,
    FUNC_RECOVER_FILE_TO_TAPE,
    FUNC_RESTORE_DIR,
    FUNC_RESTORE_REG_FILE,
    FUNC_RESTORE_FILE_DATA,
    FUNC_RESTORE_ATTRS,
    FUNC_RESTORE_PROPLIST,
    FUNC_RESTORE_SYMLINK,
    FUNC_RESTORE_SPECIAL,
    FUNC_RESTORE_HARDLINK,
    FUNC_RESET_DIR_DATES,
    FUNC_OUTPUT_TAR_DATA,
    FUNC_INSERT_NODE_IN_TREE,
    FUNC_ADD_NEW_TREE_NODE,
    FUNC_MOVE_NODE_IN_TREE,
    FUNC_DELETE_NODE,
    FUNC_DELETE_SUBTREE,
    FUNC_WALK_TREE,
    FUNC_RELINK_LOST_FOUND,
    FUNC_GET_PATHNAME,
    FUNC_VALIDATE_TREE,
    FUNC_TRIM_TREE_IGNORE_DIRS,
    FUNC_SORT_LINKED_LIST,
    FUNC_GET_DOMAIN_VOLUMES,
    FUNC_OPEN_DOMAIN_VOLUMES,
    FUNC_CHECK_ADVFS_MAGIC_NUMBER,
    FUNC_READ_PAGE_BY_LBN,
    FUNC_READ_BYTES_BY_LBN,
    FUNC_CONVERT_DATE,
    FUNC_GET_TIMESTAMP,
    FUNC_WRITEMSG,
    FUNC_CHECK_OVERWRITE,
    FUNC_PREPARE_SIGNAL_HANDLERS,
    FUNC_SIGNAL_HANDLER,
    FUNC_WANT_ABORT,
    FUNC_CHECK_TAG_ARRAY_SIZE,
    FUNC_SALVAGE_MALLOC,
    FUNC_SALVAGE_CALLOC,
    FUNC_SALVAGE_REALLOC,
    FUNC_PROCESS_NODE,
    FUNC_READ_DIR_DATA,
    FUNC_GET_DIRREC_STATE,
    FUNC_CHECK_NAME_AND_LINKS,
    FUNC_SET_FILENAME,
    FUNC_ADD_HARD_LINK,
    FUNC_TRIM_RELATIVE_PATHNAME,
    FUNC_CHECK_REQUESTED_FILE_FOUND,
    FUNC_REMOVE_NODE_FROM_TREE,
    FUNC_SAVE_HARD_LINKS,
    FUNC_VALIDATE_NODE_NAME,
    FUNC_VALIDATE_NODE_LINKS,
    FUNC_GENERATE_NODE_NAME,
    FUNC_CHECK_TAG_IGNORE_DIR,
    FUNC_FIND_HARDLINK_IN_TREE,
    FUNC_CHECK_DUP_NODE_NAMES,
    FUNC_REPLACE_DUP_NODE_NAME,
    FUNC_SIMPLE_SORT,
    FUNC_SUBSORT,
    FUNC_SWAP,
    FUNC_CREATE_TREENODE_ARRAY,
    FUNC_REORDER_TREENODE_LIST,
    FUNC_CREATE_EXTENT_ARRAY,
    FUNC_REORDER_EXTENT_LIST,
    FUNC_ADD_DELIMITERS,
    FUNC_WRITE,
    FUNC_NUMBER_FUNCTIONS
} funcNameT;

static char *prof_funcnames[FUNC_NUMBER_FUNCTIONS] = {
    "usage",
    "main",
    "build_tree",
    "p2_build_tree",
    "p3_build_tree",
    "load_tag_from_mcell",
    "p2_load_tag_from_mcell",
    "load_fileset_tag_extents",
    "load_filesets",
    "process_root_tag_file_page",
    "load_bmt_lbn_array",
    "load_frag_array",
    "p2_load_frag_array",
    "find_number_extents",
    "load_domain_attributes",
    "add_extents_to_tag",
    "p2_add_extents_to_tag",
    "add_frag_extent_to_tag",
    "p2_add_frag_record_to_tag",
    "p2_add_frag_extent_to_tag",
    "sort_and_fill_extents",
    "fill_extent_hole",
    "trim_requested_pathname",
    "trim_requested_tagnum",
    "load_tag_from_tagnum",
    "find_tag_primary_mcell_location",
    "prune_siblings",
    "delete_tag_array_entry",
    "build_cleanup",
    "p3_process_valid_page",
    "find_fileset",
    "enlarge_tag_array",
    "add_first_instance",
    "validate_fileset",
    "add_striped_extents_to_tag",
    "create_cache",
    "compare_pages",
    "read_page_from_cache",
    "insert_filenames",
    "process_dir_data",
    "get_next_direntry",
    "check_node_in_path",
    "check_name_in_path",
    "setup_volume",
    "p3_setup_volume",
    "setup_bmt",
    "setup_rbmt",
    "setup_fileset",
    "setup_tag_2",
    "setup_fs_tree",
    "create_volumes",
    "create_tag_array_element",
    "create_unnamed_tree_node",
    "create_extent_element",
    "create_tag_attr",
    "create_tag_prop_list",
    "load_tag_prop_data",
    "p2_create_tag_prop_list",
    "create_fileset_element",
    "find_number_tags",
    "find_last_bmt_extent",
    "load_root_tag_file",
    "find_bmt_chain_ptr",
    "bmt_page_to_lbn",
    "frag_page_to_lbn",
    "convert_page_to_lbn",
    "find_next_mcell",
    "get_domain_version",
    "validate_page",
    "p2_create_tag_prop_head",
    "p2_create_tag_prop_data",
    "p2_create_tag_prop_full",
    "find_last_rbmt_extent",
    "load_rbmt_lbn_array",
    "setup_tar",
    "create_list",
    "random_level",
    "insert_s_node",
    "delete_s_node",
    "find_s_node",
    "find_s_node_or_next",
    "find_first_s_node",
    "find_next_s_node",
    "recover_fileset",
    "recover_file",
    "recover_file_to_tape",
    "restore_dir",
    "restore_reg_file",
    "restore_file_data",
    "restore_attrs",
    "restore_proplist",
    "restore_symlink",
    "restore_special",
    "restore_hardlink",
    "reset_dir_dates",
    "output_tar_data",
    "insert_node_in_tree",
    "add_new_tree_node",
    "move_node_in_tree",
    "delete_node",
    "delete_subtree",
    "walk_tree",
    "relink_lost_found",
    "get_pathname",
    "validate_tree",
    "trim_tree_ignore_dirs",
    "sort_linked_list",
    "get_domain_volumes",
    "open_domain_volumes",
    "check_advfs_magic_number",
    "read_page_by_lbn",
    "read_bytes_by_lbn",
    "convert_date",
    "get_timestamp",
    "writemsg",
    "check_overwrite",
    "prepare_signal_handlers",
    "signal_handler",
    "want_abort",
    "check_tag_array_size",
    "salvage_malloc",
    "salvage_calloc",
    "salvage_realloc",
    "process_node",
    "read_dir_data",
    "get_dirrec_state",
    "check_name_and_links",
    "set_filename",
    "add_hard_link",
    "trim_relative_pathname",
    "check_requested_file_found",
    "remove_node_from_tree",
    "save_hard_links",
    "validate_node_name",
    "validate_node_links",
    "generate_node_name",
    "check_tag_ignore_dir",
    "find_hardlink_in_tree",
    "check_dup_node_names",
    "replace_dup_node_name",
    "simple_sort",
    "subsort",
    "swap",
    "create_treenode_array",
    "reorder_treenode_list",
    "create_extent_array",
    "reorder_extent_list",
    "add_delimiters",
    "write"
};

struct timeval prof_starttime[FUNC_NUMBER_FUNCTIONS];
struct timeval prof_endtime[FUNC_NUMBER_FUNCTIONS];
long prof_timeused[FUNC_NUMBER_FUNCTIONS];
long prof_currcalls[FUNC_NUMBER_FUNCTIONS];
long prof_numcalls[FUNC_NUMBER_FUNCTIONS];

#define PROF_START(func) \
    prof_currcalls[func]++; \
    prof_numcalls[func]++; \
    if (1 == prof_currcalls[func]) { \
        gettimeofday(&(prof_starttime[func]), NULL); \
    }

#define PROF_RETURN(func) \
    prof_currcalls[func]--; \
    if (0 == prof_currcalls[func]) { \
	long start, end; \
        gettimeofday(&(prof_endtime[func]), NULL); \
	start = prof_starttime[func].tv_sec * 1000000L + \
		prof_starttime[func].tv_usec; \
	end = prof_endtime[func].tv_sec * 1000000L + \
		prof_endtime[func].tv_usec; \
        prof_timeused[func] += end - start; \
    }

#endif /* PROFILE */

/*====================================
 * function prototypes
 *===================================*/

/*
 * salvage.c
 */

void usage( void );

/*
 * salvage_build.c
 */

int build_tree (domainInfoT *domain,
	        filesetLLT  *fileset);

int p2_build_tree (domainInfoT *domain,
                   int         volId);

int p3_build_tree (domainInfoT *domain,
		   int         volId);
  
int load_tag_from_mcell (domainInfoT *domain,
			 filesetLLT  *fileset,
                         int         tagNumber,
                         mcellT      tagMcell,
			 int         *parentTag);
                                                
int p2_load_tag_from_mcell (domainInfoT *domain,
			    filesetLLT  *fileset,
                            int         tagNumber,
			    bsMCT       *pMcell,
			    int         *parentTag,
			    int         volume);
                                                        
int load_fileset_tag_extents(domainInfoT *domain,
                             mcellT      *fsTagDirMcell,
                             extentT     *tagExtentBuffer);

int load_filesets (bsMPgT      *mcellBuffer,
                   void        *pData,
                   domainInfoT *domain,
		   int         volume,
		   int         bufferType);

int process_root_tag_file_page(domainInfoT *domain,
			       int         fd,
			       LBNT         lbn);

int load_bmt_lbn_array (domainInfoT *domain,
			volMapT     *volume,
                        bsMPgT      *BMT0buffer);

int load_frag_array (domainInfoT *domain,
		     filesetLLT  *fileset,
                     extentT     *fsTagFileBuffer);

int p2_load_frag_array (domainInfoT *domain,
			filesetLLT  *fileset,
			filesetTagT *tag);

int find_number_extents(domainInfoT *domain,
			mcellT      current,
			int         *extents);

int load_domain_attributes (bsMPgT      *BMT0buffer,
                            domainInfoT *domain,
                            int         *volNumber);

int add_extents_to_tag (domainInfoT *domain,
			filesetTagT *tag,
                        bsXtntRT    *pdata,
			int         volume);

int p2_add_extents_to_tag (domainInfoT *domain,
			   filesetTagT *tag,
                           void        *pdata,
			   int         type,
			   int         volume);

int add_frag_extent_to_tag (filesetLLT  *fileset,
			    int         volNumer,
			    filesetTagT *tag,
                            statT       *pdata);

int p2_add_frag_record_to_tag(filesetLLT  *fileset,
			      int         volNumber,
			      filesetTagT *tag,
			      statT       *pdata);

int p2_add_frag_extent_to_tag(filesetLLT  *fileset,
			      filesetTagT *tag);

int sort_and_fill_extents (filesetTagT *tag);

int fill_extent_hole (filesetTagT *pTag,
                      extentLLT   *pCurr,
                      long        holeSize,
		      long        firstFlag);

int trim_requested_pathname (filesetLLT *fileset,
			     extentT    *extentBuf );

int trim_requested_tagnum (filesetLLT *fileset,
                           extentT    *extentBuf,
                           int        arraySize );

int load_tag_from_tagnum( filesetLLT *fileset,
                          int        tagNum,
                          extentT    *tagfileExtArray,
                          int        arraySize );

int find_tag_primary_mcell_location( filesetLLT *fileset,
                                     int        tagToFind, 
                                     extentT    *tagfileExtArray,
                                     int        arraySize,
                                     mcellT     *primMcell );

int prune_siblings( filesetLLT       *fileset,
                    filesetTreeNodeT *pDirNode,
                    filesetTreeNodeT *pSaveNode,
                    extentT          *tagfileExtArray,
                    int              arraySize );

int delete_tag_array_entry (filesetLLT *fileset,
                            int        tagNum);

int build_cleanup (domainInfoT *domain);

int p3_process_valid_page (domainInfoT *domain, 
			   bsMPgT      *BMTbuffer,
			   int         volId);

int find_fileset (domainInfoT *domain,
		  int         tag,
		  int         setTag,
		  filesetLLT  **foundFileset);

int enlarge_tag_array (filesetLLT  *fileset,
		       int         tag);

int add_first_instance(filesetLLT  *fileset,
		       int         tag,
		       int         parentTagNum);

int validate_fileset(filesetLLT  *fileset);


int add_striped_extents_to_tag(domainInfoT *domain,
			       filesetTagT *tag,
			       bsXtntRT    *pData);

/*
 * salvage_fnames.c
 */

int insert_filenames (filesetLLT *fileset,
                      int        *reqPathFound);

int process_dir_data (filesetLLT       *fileset, 
                      filesetTreeNodeT *pDirNode,
                      int              checkPathFlag);

int get_next_direntry (fsDirDataT    *currDirData,
                       fs_dir_entry  *pReturn);

int check_node_in_path (filesetLLT       *fileset,
                        filesetTreeNodeT *pNode, 
                        char             *testPath, 
                        int              *isInPath);

int check_name_in_path (char  *fileName,
                        char  *testPath,
                        int   *isInPath);

/*
 * salvage_init.c
 */

int setup_volume (domainInfoT *domain);

int p3_setup_volume (domainInfoT *domain);

int setup_bmt (domainInfoT *domain,
	       volMapT     *volume,
               bsMPgT      *BMT0buffer);

int setup_rbmt(domainInfoT *domain,
	       volMapT     *volume,
	       bsMPgT      *buffer);

int setup_fileset (domainInfoT *domain,
                   filesetLLT  *fileset,
                   extentT     **fsTagFileBuffer,
		   int         *fsTagFileExtents);

int setup_tag_2 (filesetLLT *fileset,
		 extentT    *extentBuf);

int setup_fs_tree (filesetLLT *fileset);

int create_volumes (volMapT  **volumes);

int create_tag_array_element (filesetTagT  **tag);

int create_unnamed_tree_node (filesetTreeNodeT **node);

int create_extent_element (extentLLT  **extent,
                           int        volume,
                           LBNT        diskBlock,
                           long       size,
                           long       fileLocation);
 
int create_tag_attr (tagAttributeT  **tagAttr,
                     statT          *pdata);

int create_tag_prop_list (domainInfoT  *domain,
			  tagPropListT **tagPropList,
                          void         *pdata,
			  int          bCnt);

int load_tag_prop_data (tagPropListT    *tagPropList,
			bsPropListPageT *pdata,
			int             bCnt);

int p2_create_tag_prop_list (domainInfoT  *domain,
			     tagPropListT **tagPropList,
                             void         *pdata,
			     int          bCnt);

int create_fileset_element (filesetLLT  **fileset,
                            domainInfoT *domain );

int find_number_tags (domainInfoT *domain,
		      filesetLLT  *fileset,
                      long        *tagsFound,
                      long        *extentsFound);

int find_last_bmt_extent (domainInfoT  *domain,
			  volMapT      *volume,
			  unsigned int *lastExtent,
			  bsMPgT       *BMT0buffer);

int load_root_tag_file (bsMPgT      *BMT0buffer,
                        domainInfoT *domain,
			int         volume);

int find_bmt_chain_ptr(domainInfoT  *domain,
		       bsMPgT       *BMT0buffer,
                       int          *cellNumber);

int bmt_page_to_lbn(domainInfoT *domain,
		    int         volume,
		    int         page,
		    LBNT        *lbn,
		    int         *fd);

int frag_page_to_lbn(filesetLLT  *fileset,
		     int         page,
		     LBNT        *lbn,
		     int         *vol);

int convert_page_to_lbn( extentT     *extentArray,
                         int         extentArraySize,
                         int         page,
                         LBNT        *lbn,
                         int         *vol );

int find_next_mcell(domainInfoT *domain,
		    mcellT      *current,
		    bsMCT       **pMcell,
		    bsMPgT      *pPage,
		    int         followChain);

int get_domain_version(domainInfoT *domain);

int validate_page(bsMPgT  page);

int p2_create_tag_prop_head (tagPropListT    **tagPropList,
			     bsPropListHeadT *pdata,
			     int             bCnt);

int p2_create_tag_prop_data (tagPropListT    **tagPropList,
			     bsPropListPageT *pdata,
			     int             bCnt);

int p2_create_tag_prop_full (filesetLLT  *fileset);

int find_last_rbmt_extent(domainInfoT  *domain,
			  volMapT      *volume,
			  unsigned int *lastExtent,
			  bsMPgT       *BMTbuffer);

int load_rbmt_lbn_array(domainInfoT *domain,
			volMapT     *volume,
			bsMPgT      *BMTbuffer);

void setup_tar(void);


/*
 * salvage_recover.c
 */
int recover_fileset (domainInfoT *domain,
                     filesetLLT  *filesetp);

int recover_file (filesetTreeNodeT *nodep,
                  recoverDataT     *recoverData);

int recover_file_to_tape (filesetTreeNodeT *nodep,
                          recoverDataT     *recoverData);

int restore_dir (filesetTagT *tagEntryp,
                 char        *pathName,
                 char        *localPath);

int restore_reg_file (filesetTagT *tagEntryp,
                      char        *pathName,
                      char        **localPathp,
                      volMapT     *volumes);

int restore_file_data (int         fd,
                       filesetTagT *tagEntryp,
                       volMapT     *volumes);

int restore_attrs (char          *pathName,
                   char          *localPath,
                   tagAttributeT *attrp);

int restore_proplist (filesetTagT *tagEntryp,
		      Stat	  *pStatbuf,
                      char        *pathName,
                      char        *localPath);

int restore_symlink (filesetTagT *tagEntryp,
                     char        *pathName,
                     char        *localPath);

int restore_special (filesetTreeNodeT *nodep,
                     char             *pathName,
                     char             *localPath,
                     filesetTagT      *tagEntryp);

int restore_hardlink (filesetTreeNodeT *nodep,
                      char             *pathName,
                      char             *localPath,
                      filesetTagT      *tagEntryp);

int reset_dir_dates (filesetTreeNodeT *nodep,
                     recoverDataT     *recoverData);

int output_tar_data (long fileSize,
		     long bufSize,
		     int zeroFill,
		     char *buffer,
		     long *bytesWritten);

/*
 * salvage_tree.c
 */
int insert_node_in_tree (filesetTreeNodeT *newNode,
			 filesetTreeNodeT *parentNode);

int add_new_tree_node (filesetLLT       *fileset,
                       int              tagNum,
                       int              parentTag,
                       char             *name,
                       int              len,
                       filesetTreeNodeT *pDirNode,
                       filesetTreeNodeT **pNewNode);

int move_node_in_tree (filesetTreeNodeT *pNodeToMove,
                       filesetTreeNodeT *pNewParent);

int delete_node (filesetTreeNodeT **nodeToDelete,
                 filesetLLT       *fileset);

int delete_subtree (filesetTreeNodeT **topNodeToDelete,
                    filesetLLT       *fileset);

int walk_tree (filesetTreeNodeT *pStartNode,
               int              childFirst,
               int              (*funcPtr)(),
               void             *optArg);

int relink_lost_found (filesetLLT *fileset);

int get_pathname (char             **pathName,
                  filesetTreeNodeT *tag);

int validate_tree (filesetLLT *fileset);

int trim_tree_ignore_dirs (filesetLLT *fileset);

int sort_linked_list (void **listHead,
                      int type,
                      int nRecs );

/*
 * utility.c
 */
int get_domain_volumes (char    *domain,
                        int     *numVolumes,
                        volMapT volumes[]);

int open_domain_volumes (domainInfoT *domain);

int check_advfs_magic_number (int volFd,
                              int *isAdvfs);

int read_page_by_lbn (int    fd,
                      void   *ppage,
                      LBNT   lbn,
		      long   *bytesRead);

int read_bytes_by_lbn (int  fd,
                       unsigned long numBytes,
                       void *pbuffer,
                       LBNT  lbn,
		       long *bytesRead);

int convert_date (char       *timeString,
		  const char *dateFormat[]);

char *get_timestamp( char *dateStr, 
                     size_t maxLen );

void writemsg( int severity, char *fmt, ... );

int check_overwrite (char *fileName);

void prepare_signal_handlers (void);

void signal_handler (int sigNum);

int want_abort (void);

int check_tag_array_size (int old,
			  int new);

void *salvage_malloc (size_t size);

void *salvage_calloc (size_t numElts,
		      size_t eltSize);

void *salvage_realloc (void   *pointer,
		       size_t size);

#endif /* not SALVAGE_H */

/* end salvage.h */
