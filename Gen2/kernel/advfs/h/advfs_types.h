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
 *  AdvFS
 *
 * Abstract:
 *
 *  AdvFS type definitions for basic structures that are used to define
 *  other AdvFS structures. Also used in most interfaces to AdvFS.
 *
 */

#ifndef _ADVFS_TYPES_INCLUDED 
#define _ADVFS_TYPES_INCLUDED

#include <sys/fs/advfs_public.h>

typedef int64_t  adv_time_t;
typedef uint64_t adv_mode_t;
typedef int32_t  adv_uid_t;
typedef int32_t  adv_gid_t;
typedef int64_t  adv_dev_t;
typedef int64_t  adv_off_t;
typedef struct   adv_timeval {
    uint64_t tv_sec;
    int64_t  tv_usec;
} adv_timeval_t;


/*
 *  Define AdvFS value to be used as a unique identifier. Previous versions
 *  of AdvFS used a timeval but there was no guarantee that the size would
 *  remain constant. Prevent this potential problem by defining one that
 *  can safely be stored on disk.
 */
typedef struct bsId {
   int64_t id_sec;
   int64_t id_usec;
} bsIdT;

/* The following types represent file and disk blocks            
 * The bf_fob_t should be used for offsets in a     
 * file that are in units of 1k as when dealing with extent maps 
 * The  bf_vd_blk_t should be used when tracking disk blocks 
 */
typedef uint64_t  bf_fob_t;          /* File offsets in 1k units */
typedef uint64_t  bf_vd_blk_t;       /* Disk blocks (DEV_BSIZE blocks) */
typedef int64_t   bs_meta_page_t;    /* signed 64 bits */

typedef uint32_t  vdIndexT;          /* VolumeId */

typedef int32_t   statusT;           /* tmp */


typedef struct bfMCId {
    uint64_t volume  :  8;    /* volume number (1 based)  */
    uint64_t page    : 48;    /* Page number */
    uint64_t cell    :  8;    /* Cell number within page 
                               * NOTE: 8-bit cell size limits BMT
                               *   pages to max of 255 mcell which
                               *   allows for a 64K BMT page with
                               *   current record structures.
                               */
} bfMCIdT;


/*
 * Service Class stuff
 */
typedef uint32_t serviceClassT;

typedef uint64_t ftxIdT;

/*
 * ADVFS_LOGBLKS_PER_PAGE defines the number of 512byte blocks make up the
 * logical view of a log page.  
 */
/*
 * TEMP - remove later
 */
#define ADVFS_LOG_BLKSZ         512
#define ADVFS_LOGBLKS_PER_PAGE  ( ADVFS_METADATA_PGSZ / ADVFS_LOG_BLKSZ )


#endif /* _ADVFS_TYPES_INCLUDED */
