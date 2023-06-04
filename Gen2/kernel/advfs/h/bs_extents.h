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
 *    AdvFS 
 *
 * Abstract:
 *
 *    This header file contains structure definitions and function
 *    prototypes related to extent maps.
 *
 */

#ifndef _BS_EXTENTS_H_
#define _BS_EXTENTS_H_

extern int CheckXtnts;


/* Forward Declarations */
struct domain;
struct bsInMemXtntMap;
struct bfAccess;


void
x_dealloc_extent_maps (
                       bsInMemXtntT *xtnts
                       );

statusT
update_xtnt_rec (
                 domainT *domain,                 /* in */
                 bfTagT bfTag,                    /* in */
                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                 ftxHT parentFtx                  /* in */
                 );

statusT
update_mcell_cnt (
                  struct domain* domain,  /* in */
                  bfTagT bfTag, /* in */
                  bfMCIdT bfMcellId,  /* in */
                  uint32_t type,  /* in */
                  int32_t mcellCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  );

statusT
odm_remove_mcells_from_xtnt_map (
                                 struct domain *domain,  /* in */
                                 bfTagT bfTag, /* in */
                                 struct bsInMemXtntMap *xtntMap,  /* in */
                                 uint32_t start_index,
                                 ftxHT parentFtx  /* in */
                                 );

statusT
odm_create_xtnt_map (
                     struct bfAccess *bfAccess,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bfTagT bfTag,  /* in */
                     struct bsInMemXtntMap *xtntMap,  /* in */
                     ftxHT parentFtx,  /* in */
                     bfMCIdT *bfMcellId  /* out */
                     );

statusT x_create_inmem_xtnt_map( struct bfAccess*, bsMCT*);



/* The two sets of flags used below are shared by advfs_get_blkmaps_in_range
 * and x_load_inmem_xtnt_map.  They must remain unique relative to eachother
 * */

/* 
 * All of the following are related. They are passed from
 * advfs_get_blkmap_in_range to x_load_inmem_xtnt_map and must be unique
 */
/* Possible values to pass for lock_request to x_load_inmem_xtnt_map(). */

typedef enum {
    X_LOAD_REFERENCE    = 0x1,  /* Take xtntMap_lk for read only */
    X_LOAD_UPDATE       = 0x2,  /* Need to modify xtnt maps, take
                                * mcellList_lk */
/* Flags for advfs_get_blkmap_in_range */
    XTNT_LOCKS_HELD     = 0x4,  /* For advfs_get_blkmap_in_range, locks
                                * owned, extent maps are somehow
                                * proected. */
    XTNT_WAIT_OK        = 0x8, /* We can wait on building the block
                                * maps */
    XTNT_NO_WAIT        = 0x10, /* No blocking allowed while building
                                * block maps */
    XTNT_NO_MAPS        = 0x20, /* Do not build block maps, just count
                                * how many there would be and do the
                                * rounding. */
    XTNT_TEMP_XTNTMAP   = 0x40, /* The extent map being modified is
                                * a copy and no locking needed*/
    XTNT_ORIG_XTNTMAP   = 0x80 /* Modify xtntmap in place */

} xtntMapFlagsT;



statusT
x_lock_inmem_xtnt_map (
                       struct bfAccess *bfap,      /* in, modified */
                       xtntMapFlagsT xtnt_flags   /* in */
                       );
statusT
x_create_shadow_rec (
                     struct bfAccess *bfAccess,  /* in */
                     struct bsInMemXtntMap *xtntMap,  /* in */
                     bfMCIdT prevMcellId,  /* in */
                     ftxHT parentFtxH, /* in */
                     int striping_file /* in */
                     );

statusT
x_detach_extent_chain (
                     struct bfAccess *bfAccess,       /* in */
                     struct bsInMemXtntMap *xtntMap,  /* in */
                     ftxHT parentFtxH,          /* in */
                     bfMCIdT *retPrevMcellId,   /* out */
                     bfMCIdT *retfreedMcellId   /* out */
                     );

statusT
x_update_ondisk_xtnt_map (
                          struct domain *domain,  /* in */
                          struct bfAccess *bfAccess,  /* in */
                          struct bsInMemXtntMap *xtntMap,  /* in */
                          ftxHT parentFtx  /* in */
                          );

statusT
odm_create_xtnt_rec (
                     struct bfAccess *bfap,     /* in */
                     bfSetT *bfSetp,            /* in */
                     struct bsInMemSubXtntMap *subXtntMap,  /* in */
                     ftxHT parentFtx            /* in */
                     );



typedef struct restoreOrigXtntmapUndoRec
{
    bfSetIdT setId;             /* Set id */
    bfTagT bfTag;               /* File that had storage added/removed */
    union {
        vdIndexT vdIndex;
        bfAccessT *bfap;
    }bfap_or_vd;
    
} restoreOrigXtntmapUndoRecT;

/* The extent_blk_desc structure is used to provide a chain of extent
 * descriptors.  The structure was introduced to provide advfs_getpage and
 * advfs_putpage with a list of contiguous disk regions without the need to 
 * walk extent maps repeatedly.  Migrate will also use this structure.   The
 * structure will be returned from advfs_get_blkmaps_in_range.  The
 * enumerations provided are used to specify how the block maps returned
 * should present the information and what information should be presented. 
 */

struct extent_blk_desc {
    struct extent_blk_desc *ebd_next_desc;  /* Next desc. in a list, null term */
    struct extent_blk_desc *ebd_snap_fwd;   /* Next desc. for use by snapshots */
    struct bfAccess        *ebd_bfap;       /* Bfap in which extent is mapped */
    off_t                  ebd_offset;      /* Starting offset in bytes in file*/
    size_t                 ebd_byte_cnt;    /* length of the range described */
    bf_vd_blk_t            ebd_vd_blk;      /* Disk block the mapping begins at 
                                             * -1 = hole, -2 = start perm hole */
    vdIndexT               ebd_vd_index;    /* volume index this mapping is on */
};

typedef struct extent_blk_desc extent_blk_desc_t;

/* round_type_t describes the type of rounding that
 * advfs_get_blkmaps_in_range should perform. 
 */

typedef enum {
   RND_ALLOC_UNIT,      /* Round to complete allocation units if the offset is 
                         * in the middle of a hole.  This is primarily
                         * intended for writes that start in a hole.  If the
                         * end of a range falls in the middle of a 4k
                         * boundary, the end will be rounded up to 4k. (not
                         * the allocation unit). */
   RND_VM_PAGE,         /* Round to a vm page boundary (4k).  This is intended 
                         * for use when a read is occuring.   This round
                         * type prevents processing of entire allocation
                         * units when only part of the unit is required.
                         * The start of the range will be rounded down to a
                         * 4k boundary and the end will be rounded up to a
                         * 4k boundary */
   RND_MIGRATE,         /* Round to adjacent leading (left) holes and truncate
                         * trailing (right) holes in order to guarantee
                         * that a hole accompanies its trailing storage.
                         * If the range is completely contained within
                         * a hole, return the entire hole.  Logical adjacent
                         * blocks are coalesced into one extent block descriptor.
                         */
   RND_NONE,            /* No rounding.  The passed in offset and length must be
                         * a multiple of DEV_BSIZE. */
   RND_ENTIRE_HOLE      /* This rounding type will be used to indicate that rounding 
                         * should encompass entire holes.  If the range is
                         * 1byte in the middle of a 1GB hole, the entire 1GB
                         * hole will be returned.  This is for COW
                         * operations to COW entire holes at once. */
} round_type_t;

/* Macros for rounding */
/* 4 = VM_PAGE size in fobs (1024 * 4 = 4096) */
#define ADVFS_ROUND_FOB_DOWN_TO_VM_PAGE(fob)      ( (fob / 4) * 4 )
#define ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, fob)     \
                ( (fob / bfap->bfPageSz) * bfap->bfPageSz )

/* Macros for extent block descriptor holes */
#define ADVFS_NORMAL_HOLE(ebd)    ((ebd)->ebd_vd_blk == XTNT_TERM)
#define ADVFS_COWED_HOLE(ebd)     ((ebd)->ebd_vd_blk == COWED_HOLE)
#define ADVFS_HOLE(ebd)           (ADVFS_NORMAL_HOLE(ebd) || ADVFS_COWED_HOLE(ebd))

/* extent_blk_map_type_t defines what information should be presented in a
 * block map.  The options are storage only, holes only, or complete (both
 * storage and holes).
 */

typedef enum {
    EXB_COMPLETE        = 0x1, /* Return a map of holes and storage. */
    EXB_ONLY_HOLES      = 0x2, /* Return only a map of holes.  Do not include storage */
    EXB_ONLY_STG        = 0x4, /* Return only a map of storage. Do not include holes */
    EXB_DO_NOT_INHERIT  = 0x8  /* Return only local xtnts maps, none from parent snaps */
} extent_blk_map_type_t;


/* End of types for advfs_get_blkmap_in_range */


/* advfs_bs_is_offset_mapped() provides a way to determine if the specified
 * offset is a sparse hole. */
int32_t 
advfs_bs_is_offset_mapped(
    struct bfAccess *bfap,      /* in - file's access structure */
    off_t            offset     /* in - file offset */
    );

/* advfs_get_blkmap_in_range provides a means of getting continuity and
 * block map information over a range of bytes in a specific file.  See the
 * definition for detailed explanation of operations. */

statusT
advfs_get_blkmap_in_range (
        struct bfAccess *bfap,  /* IN - Access struct for file                */
        struct bsInMemXtntMap *xtnt_map,      /* IN - Extent map to use to          *
                                         * generate range maps                */
        off_t *offset,          /* IN - offset in file to start range map     */
                                /* OUT - offset adjusted to correct           * 
                                 * alignment                                  */
        size_t length,          /* IN - length of range to map                */
        extent_blk_desc_t **extent_blk_desc,    
                                /* IN - pointer to an extent_blk_desc         *
                                 * OUT - pointer to head of list that maps    *
                                 * the given range                            */
        uint64_t *xtnt_count,   /* IN - a pointer or NULL                     *
                                 * OUT - Overloaded meaning, see function
                                 * description for usage.                     */
        round_type_t round_type,/* IN - type of rounding to be performed      */
        extent_blk_map_type_t extent_blk_map_type,
                                /* IN - determines the map type. (sparse,     *
                                 * stg, both                                  */ 
        int blkmap_flags        /* In - flags for the function                */           
        );         
                     


/* This function frees the memory in a list of extent_blk_desc_t structures
 * */
statusT
advfs_free_blkmaps(
    extent_blk_desc_t** extent_blk_desc /* IN - pointer to head of list to be freed */
    );



statusT
create_xtnt_map_hdr (
                     struct bfAccess *bfAccess, /* in */
                     bfSetT *bfSetp,            /* in */
                     bfTagT bfTag,              /* in */
                     bfMCIdT firstMcellId,      /* in */
                     bfMCIdT lastMcellId,       /* in */
                     ftxHT parentFtx,           /* in */
                     bfMCIdT *bfMcellId         /* out */
                     );

#endif  /* _BS_EXTENTS_H_ */

