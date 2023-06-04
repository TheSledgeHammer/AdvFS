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
 * *  Copyright (c) 2003 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 */

/* @(#) */
 
#ifndef _ADVFS_IOCTL_
#define _ADVFS_IOCTL_

#include <sys/types.h>          /* uint32_t,uint64_t */

#define ADVFS_IOCTL 'y'

#define ADVFS_GETCACHEPOLICY    _IOR(ADVFS_IOCTL, 0, int )
#define ADVFS_DIRECTIO 1        /* returned by ADVFS_GETCACHEPOLICY */
#define ADVFS_CACHED 0          /* returned by ADVFS_GETCACHEPOLICY */

/*
 * The ADVFS_GETMAP request gets the sparseness map of the file referred to
 * by the open file descriptor (fildes) parameter. The argument parameter,
 * taken as a pointer to type struct extentmap, is filled in with data that
 * describes the extent map of the file.
 *
 *     ioctl( (int)fildes, ADVFS_GETMAP, (struct extentmap *)&map );
 *
 * The map returned by the ADVFS_GETMAP function may be different from the
 * actual number of extents and their size if the file is activily being
 * written.  It is recommended that you use this function only on files that
 * are not being written.
 *
 * The map return by ADVFS_GETMAP only describes the virtually contiguous
 * parts of the file.  The map does not say anything about how the file is
 * stored on-disk, nor how fragmented the storage may be.  It only shows if
 * the file has sparse holes of unallocated storage in it.
 *
 * If extentmap.arraysize is zero, ADVFS_GETMAP will return the total number
 * of extents in extentmap.numextents, and not attempt to return any extent
 * information.
 *
 * If extentmap.arraysize is smaller than the number of extents to be
 * returned, ADVFS_GETMAP will fill in the extent array with
 * extentmap.arraysize number of entries and extentmap.offset will point to
 * the offset of the next extent to be returned.  ADVFS_GETMAP may be called
 * again using the same extentmap structure to get the next
 * extentmap.arraysize number of extents.  This may be repeated until
 * extentmap.numextents have been returned.
 *
 * If ADVFS_GETMAP is called in a loop, extentmap.offset should be
 * initialized to zero, and then the value returned from the previous call
 * should be passed in on the next call.  That is to say, applications
 * should treat extentmap.offset as a opaque value and programs should not
 * put their own values into extentmap.offset.
 */
#define ADVFS_GETMAP            _IOWR(ADVFS_IOCTL, 1, struct extentmap )

/* structures used by ADVFS_GETMAP ioctl call */
struct extentmapentry {
    uint64_t offset;               /* out - extent starts at this offset */
    uint64_t size;                 /* out - extent is this long */
};

struct extentmap {
    uint64_t arraysize;            /* in  - number extent array entries */
    uint64_t numextents;           /* out - total number of extents in file */
    uint64_t offset;               /* in/out (should be treated as opaque) */
                                   /* in  - offset to start returning extents */
                                   /* out - offset of next extent to return */
    struct extentmapentry *extent; /* in  - extent array */
};

/*
 * ADVFS_FREEZEFS causes the specified fileset's domain to enter into a
 * metadata stable state.  All the filesets in the AdvFS domain are
 * affected.  This places the domain metadata in a consistent state and
 * guarantees that it stays that way until thawed.  All metadata, which
 * could be spread across multiple volumes or logical units (LUNS), is
 * flushed to disk and does not change for the duration of the freeze.
 *
 *     ioctl( (int)fildes, ADVFS_FREEZEFS, (int)timeout );
 *
 * The open file descriptor (fildes) must be for the mount point. The
 * passed argument is a timeout.  If timeout is greater than zero specified
 * the maximum time allowed for the fileset's domain to remain frozen.  If
 * timeout is zero then use the default timeout as specified by
 * advfreezefs_default_timeout. Or, if advfsfreezefs_default_timeout is not
 * specified, then default to 60 seconds.  If timeout is less than zero, this 
 * specifies no timeout and the fileset's domain remains frozen until 
 * explicitly thawed by ADVFS_THAWFS.
 */
#define ADVFS_FREEZEFS          _IOW(ADVFS_IOCTL, 2, int )

/*
 * ADVFS_FREEZEQUERY returns the current freeze state of the specified
 * fileset's domain.
 *
 *     status = ioctl( (int)fildes, ADVFS_FREEZEQUERY, (int *)&frozen );
 *
 * The open file descriptor (fildes) may be for any file in the domain.
 * The passed argument is a pointer to an int in which the result will
 * be returned (0 -> Not Frozen, 1-> Frozen).
 */
#define ADVFS_FREEZEQUERY       _IOR(ADVFS_IOCTL, 3, int )

/*
 * ADVFS_THAWFS causes the previously frozen fileset's domain to unfreeze
 * and allow normal I/O activity.
 *
 *     ioctl( (int)fildes, ADVFS_THAWFS, (int)0 );
 *
 * The open file descriptor (fildes) may be for any file in the domain.
 */
#define ADVFS_THAWFS            _IO(ADVFS_IOCTL, 4 )

#define ADVFS_Q_NOFLAG  INT_MIN
#define ADVFS_Q_THAW    0
#define ADVFS_Q_FREEZE  1
#define ADVFS_Q_QUERY   2

/*
 * ADVFS_SETEXT alters certain aspects of file extent attributes
 *
 *      ioctl( (int)fildes, ADVFS_SETEXT, (struct advfs_ext_attr *)&attr );
 */
#define ADVFS_SETEXT            _IOW(ADVFS_IOCTL, 5, struct advfs_ext_attr )

/*
 * ADVFS_GETEXT queries certain aspects of file extent attributes
 *
 *      ioctl( (int)fildes, ADVFS_GETEXT, (struct advfs_ext_attr *)&attr );
 */
#define ADVFS_GETEXT            _IOWR(ADVFS_IOCTL, 6, struct advfs_ext_attr )

typedef struct advfs_ext_prealloc_attr {
#define ADVFS_EXT_PREALLOC_NOZERO 0x1
#define ADVFS_EXT_PREALLOC_RESERVE_ONLY 0x2
    uint64_t flags;
    uint64_t bytes;
} advfs_ext_prealloc_attr_t;

typedef enum advfs_ext_attr_type {
    ADVFS_EXT_NOOP = 0,
    ADVFS_EXT_PREALLOC = 1
} advfs_ext_attr_type_t;

typedef struct advfs_ext_attr {
  advfs_ext_attr_type_t type;
  union {
        advfs_ext_prealloc_attr_t prealloc;
    } value;
} advfs_ext_attr_t;

#endif /* _ADVFS_IOCTL_ */
