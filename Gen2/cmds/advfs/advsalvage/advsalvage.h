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
 * Advanced File System On-disk structure Salvager
 *
 */

#ifndef SALVAGE_H
#define SALVAGE_H


#include <nl_types.h>
#include <locale.h>
#include "advsalvage_advfs_msg.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <sys/resource.h>
#include <strings.h>
#include <dirent.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/mtio.h>
#include <advfs/ms_public.h>
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>
#include <aclv.h>
#include <assert.h>

/*
 * This is not currently defined in pax.h, but should be.
 * If it is in the future, the #include will override this definition.
 */
#define DEF_BLOCKING 20

#include <langinfo.h>
#include "pax/pax.h"

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

/* TODO: this should perhaps be dynamic for the future ? */
#define BLOCKS_PER_PAGE 8
#define TAGNUM_BUFSIZ 24

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

#define FS_NO_SNAP           0x00000010
#define FS_HAS_SNAP          0x00000020
#define FS_SNAP              0x00000040
#define FS_SNAP_MASK         0x000000F0

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

#define FS_SET_NO_SNAP(s)        (s) = (((s)&(~FS_SNAP_MASK))|(FS_NO_SNAP))
#define FS_SET_HAS_SNAP(s)       (s) = (((s)&(~FS_SNAP_MASK))|(FS_HAS_SNAP))
#define FS_SET_SNAP(s)           (s) = (((s)&(~FS_SNAP_MASK))|(FS_SNAP))

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

#define FS_IS_NO_SNAP(s)         (((s)&(FS_SNAP_MASK)) == (FS_NO_SNAP))
#define FS_IS_HAS_SNAP(s)        (((s)&(FS_SNAP_MASK)) == (FS_HAS_SNAP))
#define FS_IS_SNAP(s)            (((s)&(FS_SNAP_MASK)) == (FS_SNAP))

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

#define S_ACL_NONE        0x00000000
#define S_ACL_COMPLETE    0x01000000
#define S_ACL_PARTIAL     0x02000000
#define S_ACL_MASK        0x03000000


/*
 * Set File Tag Bit status
 */
#define S_SET_EMPTY(s)          (s) = (((s)&(~S_FDAT_MASK))|(S_EMPTY))
#define S_SET_PARTIAL(s)        (s) = (((s)&(~S_FDAT_MASK))|(S_PARTIAL))
#define S_SET_MORE(s)           (s) = (((s)&(~S_FDAT_MASK))|(S_MORE))
#define S_SET_COMPLETE(s)       (s) = (((s)&(~S_FDAT_MASK))|(S_COMPLETE))

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

#define S_SET_ACL_NONE(s)        (s) = (((s)&(~S_ACL_MASK))|(S_ACL_NONE))
#define S_SET_ACL_COMPLETE(s)    (s) = (((s)&(~S_ACL_MASK))|(S_ACL_COMPLETE))
#define S_SET_ACL_PARTIAL(s)     (s) = (((s)&(~S_ACL_MASK))|(S_ACL_PARTIAL))

/*
 * Check File Tag Bit status
 */
#define S_IS_EMPTY(s)          (((s)&(S_FDAT_MASK)) == (S_EMPTY))
#define S_IS_PARTIAL(s)        (((s)&(S_FDAT_MASK)) == (S_PARTIAL))
#define S_IS_MORE(s)           (((s)&(S_FDAT_MASK)) == (S_MORE))
#define S_IS_COMPLETE(s)       (((s)&(S_FDAT_MASK)) == (S_COMPLETE))

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

#define S_IS_ACL_NONE(s)        (((s)&(S_ACL_MASK)) == (S_ACL_NONE))
#define S_IS_ACL_COMPLETE(s)    (((s)&(S_ACL_MASK)) == (S_ACL_COMPLETE))
#define S_IS_ACL_PARTIAL(s)     (((s)&(S_ACL_MASK)) == (S_ACL_PARTIAL))

#define S_SET_DEFAULT(s)       (s) = (0x00000000)

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

/* Macros to deal with ODS versions.  The VERSION_UNSUPPORTED macro
   will need to be adjusted as the ODS version marches forward. */

#define MAJ_VERSION_EQ(v1, v2)  ((v1).odv_major == (v2).odv_major)
#define MIN_VERSION_EQ(v1, v2)  ((v1).odv_minor == (v2).odv_minor)
#define VERSION_EQ(v1, v2) (MAJ_VERSION_EQ(v1, v2) && MIN_VERSION_EQ(v1, v2))

#define VERSION_UNSUPPORTED(v) \
    ((v).odv_major > BFD_ODS_LAST_VERSION_MAJOR || \
     ((v).odv_major == BFD_ODS_LAST_VERSION_MAJOR && \
      (v).odv_minor > BFD_ODS_LAST_VERSION_MINOR))

/*
 * Some structures to compare against
 */
static bfDomainIdT nilMlBfDomainIdT = { 0 };

static bfSetIdT nilMlBfSetIdT = { 0 };



/*
 * Salvage structures
 */

/* optionT */
typedef struct options {
    FILE                     *logFD;       /* Stream Descriptor of Log file */
    int                      recPartial;   /* Recover Partial Files       */
    int                      addSuffix;    /* Add suffix to Partial Files */
    int                      verboseLog;   /* Verbose Log entries         */
    int                      verboseOutput;/* Verbose I/O                 */
    int64_t                  tagSoftLimit; /* Soft limit on tag array     */
    int64_t                  tagHardLimit; /* Hard limit on tag array     */
    struct timeval           recoverDate;  /* Recover files M/C after     */
    char                     *recoverDir;  /* Place recover files here    */
    overwrite_t              overwrite;    /* Overwrite files if needed   */
    char                     *setName;     /* Fileset Name : NULL == ALL  */
    char                     *pathName;    /* Dir Path to restore         */
    formatTypeT              outputFormat; /* Save to disk, tar, or other */
    char                     *archiveName; /* Output file for tar format  */
    int                      stdoutArchive; /* Boolean if archiving to stdout*/
    int                      progressReport; /* progress reports */
    int                      qtest;        /* Use files instead of devices */
    char                     *qtestPath;   /* qtest replacement'/etc/fdmns' */
} optionsT;

/* domainInfoT */
typedef struct domainInfo {
    bfDomainIdT              dmnId;	   /* Domain Id                   */
    int                      recoverType;  /* 0 = All, 1 = Single FS      */
    int                      status;       /* A bit mask status field     */
    struct filesetLL         *filesets;    /* Head of fileset LL          */
    struct volMap            *volumes;     /* Ptr to volume Array         */
    int                      numberVolumes;/* # volumes this domain has   */
    bfFsCookieT              fsCookie;     /* The domain cookie           */
    adv_ondisk_version_t     ODSversion;   /* ODS version of domain       */
} domainInfoT;

/* volMapT */
typedef struct volMap {
    int                      volFd;          /* File descriptor of Volume */
    int                      volBlockFd;     /* File desc of block device */
    int                      volRawFd;       /* File desc of raw device   */
    int			     badVolNum;      /* Flag if can't find volnum */
    char                     *volName;       /* Volume Name               */
    struct extent            *extentArray;   /* Extent to LBN mapping     */
    int64_t                  extentArraySize;/* Size of extentArray       */
    struct extent            *rbmtArray;     /* Extent to LBN mapping     */
    int64_t                  rbmtArraySize;  /* Size of rbmtArray         */
} volMapT;

/* filesetLLT */
typedef struct filesetLL {
    bfSetIdT                 filesetId;    /* from bs_public.h            */
    domainInfoT              *domain;      /* Ptr back to domain struct   */
    tagNumT                  tagArraySize; /* Size of array : Pass 2 info */
    int                      status;       /* ie Pass 2 needed            */
    char                     *fsName;      /* Fileset Name                */
    struct filesetTag        **tagArray;   /* Ptr to array of pointers    */
    struct filesetTreeNode   *dirTreeHead; /* Ptr to head of tree         */
    struct bfMCId            *tagFileMcell;/* Ptr to mcell of Tag File    */
    struct filesetLinks      *hardLinks;   /* Ptr to head of hardlink LL  */
    struct filesetStats      *statistics;  /* Ptr to statistics data      */
    struct filesetQuota      *quota;       /* Ptr to Quota data           */
    struct filesetLL         *next;        /* Ptr to Next fileset         */
    int64_t                  maxSize;      /* Max size of array: Alloc    */
    int64_t                  activeNodes;  /* Number of active nodes      */
} filesetLLT;

/* filesetTagT */
typedef struct filesetTag {
    int                      status;         /* Recovery Status of tag    */
    fileTypeT                fileType;       /* (reg,dir,special,...)     */
    int                      linksFound;     /* Increment per tree entry  */
    unsigned int             seqNum;         /* The tags sequence number  */
    int64_t                  bytesFound;     /* Increment per extent      */
    struct extentLL          *extentHead;    /* Ptr to head of Extent LL  */
    struct extentLL          *extentTail;    /* Ptr to tail of Extent LL  */
    struct filesetTreeNode   *firstInstance; /* Used for create hardlinks */
    struct tagAttribute      *attrs;         /* Ptr to file Attributes    */
    struct advfs_acl         *acls;          /* Ptr to ACL buffer         */
    int                      num_acls;       /* Size of ACL buffer        */
    int                      next_acl;       /* Start of empty in ACL buf */
    void                     *addAttrs;      /* Ptr to Additional Attrs   */

} filesetTagT;

/* extentLLT */
typedef struct extentLL {
    int                      volume;     /* Used as index into volMap array */
    bf_vd_blk_t              diskBlock;  /* Logical Block on Disk           */
    int64_t                  extentSize; /* Size of Extent in Bytes         */
    int64_t                  byteInFile; /* Logical Byte in File            */
    struct extentLL          *next;
} extentLLT;

/* tagAttributeT */
typedef struct tagAttribute {
    int                      mode;       /* File Permission                 */
    int                      numLinks;   /* Number of hard links            */
    int64_t                  size;       /* Size of file                    */
    uid_t                    uid;        /* User id                         */
    gid_t                    gid;        /* Group id                        */
    struct timeval           mtime;      /* Modification time               */
    struct timeval           atime;      /* Access time                     */
    struct timeval           ctime;      /* Creation time                   */
} tagAttributeT;

/* filesetTreeNodeT */
typedef struct filesetTreeNode {
    char                     *name;        /* File Name, without Path   */
    fileTypeT                fileType;     /* Regular, Dir, Symlink     */
    tagNumT                  parentTagNum; /* -2 for tree root          */
    struct filesetTreeNode   *parent;      /* NULL for tree root        */
    struct filesetTreeNode   *children;
    struct filesetTreeNode   *nextSibling; /* NOTE Special case for L&F */
    struct filesetTreeNode   *prevSibling;
    tagNumT                  tagNum;  
} filesetTreeNodeT;

/*extentT */
typedef struct extent {
    int                      volume;       /* Volume Fob is on   */
    bf_fob_t                 fob;          /* Fob to map from    */
    bf_vd_blk_t              lbn;          /* LBN to map to       */
} extentT;

/* filesetLinksT */
typedef struct filesetLinks {
    tagNumT                  tag;
    struct filesetLinks      *nextTag;
    struct tagLinks          *links;
} filesetLinksT;

/* tagLinksT */
typedef struct tagLinks {     
    struct filesetTreeNode   *treeNode;    /* Ptr to node in tree for link */
    struct tagLinks          *next;
} tagLinksT;

/* fsDirDataT */
typedef struct fsDirData {
    struct filesetLL  *fileset;       /* Ptr to current fileset data       */ 
    struct volMap     *volumes;       /* Ptr to domain's volume map data   */
    tagNumT           dirTagNum;      /* Tag number for current directory  */
    char              fsDirBuf[ADVFS_METADATA_PGSZ];/* Buffer of Tag/Name data*/
    bs_meta_page_t    nPages;         /* Total size, of all extents        */
    bs_meta_page_t    nPagesInExtent; /* Total size, of the current extent */
    struct extentLL   *currExtentRec; /* Ptr to current extent record      */
    bs_meta_page_t    currExtentPage; /* Page within the current extent    */
    int               currentOffset;  /* Byte offset of current entry      */
    fs_dir_entry      *currDirEntry;  /* Ptr for stepping thru dir data    */
} fsDirDataT;

/* filesetStatsT */
typedef struct filesetStats {
    uint64_t       nodesInTree;
    uint64_t       tagsInUse;
    uint64_t       tagsSetIgnore;
    uint64_t       totalDirFiles;
    uint64_t       filesLostFound;
    uint64_t       bytesInAllFiles;
    uint64_t       filesRecoveredFull;
    uint64_t       filesRecoveredPartial;
    uint64_t       filesNotRecovered;
    uint64_t       filesLargerThanExpected;
    uint64_t       filesMissingAttrs;
} filesetStatsT;

/* filesetQuotaT */
typedef struct filesetQuota {
    uint64_t       hardBlockLimit;
    uint64_t       softBlockLimit;
    uint64_t       hardFileLimit;
    uint64_t       softFileLimit;

    int64_t        blockTimeLimit;
    int64_t        fileTimeLimit;
} filesetQuotaT;

/* procNodeArgsT */
typedef struct procNodeArgs {
    filesetLLT *fileset;        /* Pointer to fileset */
    int        *reqPathFound;   /* Pointer to "requested path found" flag. */
} procNodeArgsT;

/* recoverDataT */
typedef struct recoverData {
     struct filesetTag **tagArray; /* Ptr to tag array for fileset          */
     struct volMap     *volumes;  /* Ptr to the volume information         */
     char              *pathName; /* Ptr to path name for the current node */
     dev_t             fsId; /* Fileset tagnum -- needed for tar hardlinks */
     uint64_t          totalFilesFound; /* total files found               */
     uint64_t          dirFiles;  /* total directories found */
     uint64_t          successFiles; /* total successfully recovered files */ 
     uint64_t          partialFiles; /* total partially recovered files    */
     uint64_t          largerFiles; /* total files larger than expected */
     uint64_t          unrecoveredFiles; /* total unrecovered files        */
     uint64_t          noAttributeFiles; /* total files w/no attributes */
     uint64_t          lost_acls; /* total files with lost ACLs due to pax */
} recoverDataT;


/*
 * Global data
 */
static char *Prog        = "advsalvage";
static char *salvage_version = "0.04";
struct options    Options;
static FILE *Localtty = stdin;
extern nl_catd mcat;

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
    FUNC_FIND_NUMBER_EXTENTS,
    FUNC_LOAD_DOMAIN_ATTRIBUTES,
    FUNC_ADD_EXTENTS_TO_TAG,
    FUNC_P2_ADD_EXTENTS_TO_TAG,
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
    FUNC_ADD_TAG_ACL_DATA,
    FUNC_CREATE_FILESET_ELEMENT,
    FUNC_FIND_NUMBER_TAGS,
    FUNC_FIND_LAST_BMT_EXTENT,
    FUNC_LOAD_ROOT_TAG_FILE,
    FUNC_FIND_BMT_CHAIN_PTR,
    FUNC_BMT_PAGE_TO_LBN,
    FUNC_CONVERT_PAGE_TO_LBN,
    FUNC_FIND_NEXT_MCELL,
    FUNC_GET_DOMAIN_COOKIE,
    FUNC_VALIDATE_PAGE,
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
    FUNC_RESTORE_ACLS,
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
    "find_number_extents",
    "load_domain_attributes",
    "add_extents_to_tag",
    "p2_add_extents_to_tag",
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
    "add_tag_acl_data",
    "create_fileset_element",
    "find_number_tags",
    "find_last_bmt_extent",
    "load_root_tag_file",
    "find_bmt_chain_ptr",
    "bmt_page_to_lbn",
    "convert_page_to_lbn",
    "find_next_mcell",
    "get_domain_cookie",
    "validate_page",
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
    "restore_acls",
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
int64_t        prof_timeused[FUNC_NUMBER_FUNCTIONS];
int64_t        prof_currcalls[FUNC_NUMBER_FUNCTIONS];
int64_t        prof_numcalls[FUNC_NUMBER_FUNCTIONS];

#define PROF_START(func) \
    prof_currcalls[func]++; \
    prof_numcalls[func]++; \
    if (1 == prof_currcalls[func]) { \
        gettimeofday(&(prof_starttime[func]), NULL); \
    }

#define PROF_RETURN(func) \
    prof_currcalls[func]--; \
    if (0 == prof_currcalls[func]) { \
	int64_t start, end; \
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
                         tagNumT     tagNumber,
                         bfMCIdT     tagMcell,
			 tagNumT     *parentTag);
                                                
int p2_load_tag_from_mcell (domainInfoT *domain,
			    filesetLLT  *fileset,
                            tagNumT     tagNumber,
			    bsMCT       *pMcell,
			    tagNumT     *parentTag,
			    int         volume);
                                                        
int load_fileset_tag_extents(domainInfoT *domain,
                             bfMCIdT     *fsTagDirMcell,
                             extentT     *tagExtentBuffer);

int load_filesets (bsMPgT      *mcellBuffer,
                   void        *pData,
                   domainInfoT *domain,
		   int         volume,
		   int         bufferType);

int process_root_tag_file_page(domainInfoT *domain,
			       int         fd,
			       bf_vd_blk_t lbn);

int load_bmt_lbn_array (domainInfoT *domain,
			volMapT     *volume,
                        bsMPgT      *BMT0buffer);

int find_number_extents(domainInfoT *domain,
			bfMCIdT     current,
			int64_t     *extents);

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

int sort_and_fill_extents (filesetTagT *tag);

int fill_extent_hole (filesetTagT *pTag,
                      extentLLT   *pCurr,
                      int64_t     holeSize,
		      int64_t     firstFlag);

int trim_requested_pathname (filesetLLT *fileset,
			     extentT    *extentBuf );

int trim_requested_tagnum (filesetLLT *fileset,
                           extentT    *extentBuf,
                           int64_t    arraySize );

int load_tag_from_tagnum( filesetLLT *fileset,
                          tagNumT    tagNum,
                          extentT    *tagfileExtArray,
                          int64_t    arraySize );

int find_tag_primary_mcell_location( filesetLLT *fileset,
                                     tagNumT    tagToFind, 
                                     extentT    *tagfileExtArray,
                                     int64_t    arraySize,
                                     bfMCIdT    *primMcell );

int prune_siblings( filesetLLT       *fileset,
                    filesetTreeNodeT *pDirNode,
                    filesetTreeNodeT *pSaveNode,
                    extentT          *tagfileExtArray,
                    int64_t          arraySize );

int delete_tag_array_entry (filesetLLT *fileset,
                            tagNumT    tagNum);

int build_cleanup (domainInfoT *domain);

int p3_process_valid_page (domainInfoT *domain, 
			   bsMPgT      *BMTbuffer,
			   int         volId);

int find_fileset (domainInfoT *domain,
		  tagNumT     tag,
		  tagNumT     setTag,
		  filesetLLT  **foundFileset);

int enlarge_tag_array (filesetLLT  *fileset,
		       tagNumT     tag);

int add_first_instance(filesetLLT  *fileset,
		       tagNumT     tag,
		       tagNumT     parentTagNum);

int validate_fileset(filesetLLT  *fileset);


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
		   int64_t     *fsTagFileExtents);

int setup_tag_2 (filesetLLT *fileset,
		 extentT    *extentBuf);

int setup_fs_tree (filesetLLT *fileset);

int create_volumes (volMapT  **volumes);

int create_tag_array_element (filesetTagT  **tag);

int create_unnamed_tree_node (filesetTreeNodeT **node);

int create_extent_element (extentLLT  **extent,
                           int        volume,
                           bf_vd_blk_t diskBlock,
                           int64_t    size,
                           int64_t    fileLocation);
 
int create_tag_attr (tagAttributeT  **tagAttr,
                     statT          *pdata);

int add_tag_acl_data (filesetTagT  *currentTag,
                      void         *pdata);

int create_fileset_element (filesetLLT  **fileset,
                            domainInfoT *domain );

int find_number_tags (domainInfoT *domain,
		      filesetLLT  *fileset,
                      tagNumT     *tagsFound,
                      int64_t     *extentsFound);

int find_last_bmt_extent (domainInfoT  *domain,
			  volMapT      *volume,
			  uint64_t     *lastExtent,
			  bsMPgT       *BMT0buffer);

int load_root_tag_file (bsMPgT      *BMT0buffer,
                        domainInfoT *domain,
			int         volume);

int find_bmt_chain_ptr(domainInfoT  *domain,
		       bsMPgT       *BMT0buffer,
                       int          *cellNumber);

int bmt_page_to_lbn(domainInfoT *domain,
		    int         volume,
		    bs_meta_page_t page,
		    bf_vd_blk_t *lbn,
		    int         *fd);

int convert_page_to_lbn( extentT     *extentArray,
                         int64_t     extentArraySize,
                         bs_meta_page_t page,
                         bf_vd_blk_t *lbn,
                         int         *vol );

int find_next_mcell(domainInfoT *domain,
		    bfMCIdT     *current,
		    bsMCT       **pMcell,
		    bsMPgT      *pPage,
		    int         followChain);

int get_domain_cookie(domainInfoT *domain);

int validate_page(bsMPgT  page);

int find_last_rbmt_extent(domainInfoT  *domain,
			  volMapT      *volume,
			  uint64_t     *lastExtent,
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

int restore_acls (filesetTagT *tagEntryp,
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

int output_tar_data (int64_t fileSize,
		     int64_t bufSize,
		     int     zeroFill,
		     char    *buffer,
		     int64_t *bytesWritten);

/*
 * salvage_tree.c
 */
int insert_node_in_tree (filesetTreeNodeT *newNode,
			 filesetTreeNodeT *parentNode);

int add_new_tree_node (filesetLLT       *fileset,
                       tagNumT          tagNum,
                       tagNumT          parentTag,
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
                      int64_t nRecs );

/*
 * utility.c
 */
int get_domain_volumes (char    *domain,
                        int     *numVolumes,
                        volMapT volumes[]);

int open_domain_volumes (domainInfoT *domain);

int check_advfs_magic_number (int volFd,
                              int *isAdvfs);

int read_page_by_lbn (int     fd,
                      void    *ppage,
                      bf_vd_blk_t lbn,
		      int64_t *bytesRead);

int read_bytes_by_lbn (int      fd,
                       uint64_t numBytes,
                       void     *pbuffer,
                       bf_vd_blk_t lbn,
		       int64_t  *bytesRead);

int convert_date (char       *timeString);

char *get_timestamp( char *dateStr, 
                     size_t maxLen );

void writemsg( int severity, char *fmt, ... );

int check_overwrite (char *fileName);

void prepare_signal_handlers (void);

void signal_handler (int sigNum);

int want_abort (void);

int check_tag_array_size (int64_t old,
			  int64_t new);

void *salvage_malloc (size_t size);

void *salvage_calloc (size_t numElts,
		      size_t eltSize);

void *salvage_realloc (void   *pointer,
		       size_t size);

#endif /* not SALVAGE_H */

/* end advsalvage.h */
