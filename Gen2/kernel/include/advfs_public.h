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
 
#ifndef _ADVFS_PUBLIC_
#define _ADVFS_PUBLIC_

#define ADVFS_FAKE_SB_BLK       8  /* fake super block start (1024 byte sect */
#define ADVFS_MAGIC_OFFSET   1372  /* byte offset into fake super block
                                    * MUST line up with fs_magic as defined
                                    * in the fs struct.
                                    */
#define ADVFS_MAGIC    0xADF02004  
#define ADVFS_RBMT_SB_OFFSET 2048  /* byte offset into sb for RBMT loc/offset */
/* TODO:  change ADVFS_FSB_SIZE to use ADVFS_..FOB.. value with hdr file
 *        restructure
 */
#define ADVFS_FSB_SIZE       8192 


typedef struct adv_ondisk_version {
    uint16_t odv_major;
    uint16_t odv_minor;
} adv_ondisk_version_t;


/* AdvFS specific data stored in the AdvFS FAKE super block 
 * - rbmt_offset identifies location of RBMT for the bootloader   
 * - on-disk version number used by bootloader which must match master copy
 *   stored in BSR_DMN_MATTR record
 * - Cluster Private Data Area (CPDA) location and size for use by clusters
 *
 * NOTE: ALWAYS ensure that existing fields remain in the same location/
 *       offset within this structure and within the AdvFS fake super block
 *       when making changes. Add new fields to the end of this structure.
 *       Failure to preserve field locations/offsets and field sizes could 
 *       result in incompatible AdvFS filesystems. 
 */
typedef struct advfs_fsb_data {
    int64_t              fsb_rbmt_offset_bytes; /* RBMT offset on adv device */
    adv_ondisk_version_t fsb_ods_version;
    uint32_t             fsb_multi_vol_flag;
    uint64_t             fsb_cpda_offset_bytes; /* CPDA offset on adv device */
    uint64_t             fsb_cpda_size_bytes;   
} advfs_fsb_data_t;


/* AdvFS FAKE SuperBlock
 *
 * - includes ADVFS_MAGIC number as in traditional super block location
 *   which is accessed with ADVFS macros (see below) 
 *   NOTE: first 2048 bytes MUST NOT be used for AdvFS data except for 
 *         fs_magic which corresponds to standard fs_magic offset/size
 *         
 * - advfs_fsb_data starts at location RBMT_SB_OFFSET (2048) and MUST NOT
 *   be relocated within the AdvFS fake super block. Relocating the AdvFS data
 *   will break the bootloader and cause compatibility issues with existing
 *   AdvFS filesystems.
 *
 * - defined as a union of an advfs_fsb_data struct (with offset fill) and
 *   a char[ADVFS_FSB_SIZE(8192)] to force/ensure that the AdvFS fake super
 *   block is 8K (assert for 8K size included in bs_disk_init() )
 *
 */
typedef struct advfs_fake_superblock {
    union { 
        /* 
         * Used to force an 8192 byte AdvFS fake superblock.
         */
        char             fsb_size[ADVFS_FSB_SIZE]; /*force 8K size */

        /*
         * Map AdvFS fs_magic to corresponding (struct fs) field
         */
        struct {
            char          fill_to_fs_magic[ADVFS_MAGIC_OFFSET];
            uint32_t      fs_magic;  /* MUST overlay (struct fs.fs_magic) */
        } fsb_magic;
        /*
         * AdvFS specific data
         */
        struct {
            char             fill_to_rbmt_offset[ADVFS_RBMT_SB_OFFSET];
            advfs_fsb_data_t adv_data;
        } fsb_data; 

        /*
         * Insert any overlapping structures here.
         */
    } fsb_u;
} advfs_fake_superblock_t;

#define adv_fs_magic          fsb_u.fsb_magic.fs_magic
#define adv_rbmt_offset_bytes fsb_u.fsb_data.adv_data.fsb_rbmt_offset_bytes
#define adv_ods_version       fsb_u.fsb_data.adv_data.fsb_ods_version
#define adv_multi_vol_flag    fsb_u.fsb_data.adv_data.fsb_multi_vol_flag
#define adv_cpda_offset_bytes fsb_u.fsb_data.adv_data.fsb_cpda_offset_bytes
#define adv_cpda_size_bytes   fsb_u.fsb_data.adv_data.fsb_cpda_size_bytes


/* Access AdvFS fs_magic via following macros which map it to the standard
 * fs_magic field found in the fs struct. 
 */
#define ADVFS_VALID_FS_MAGIC(fsb)  \
    (((struct advfs_fake_superblock *)(fsb))->adv_fs_magic == ADVFS_MAGIC)
#define ADVFS_GET_FS_MAGIC(fsb, magic_num) \
    ((magic_num) = ((struct advfs_fake_superblock *)(fsb))->adv_fs_magic)
#define ADVFS_SET_FS_MAGIC(fsb) \
    (((struct advfs_fake_superblock *)(fsb))->adv_fs_magic = ADVFS_MAGIC)

    
#define ADVFS_GET_CPDA_OFF(fsb, offset,size)                                  \
{  (offset) = ((struct advfs_fake_superblock *)(fsb))->adv_cpda_offset_bytes; \
   (size)   = ((struct advfs_fake_superblock *)(fsb))->adv_cpda_size_bytes;   \
}


/*
 * Arguments to AdvFS via mount(2).
 */
#ifndef _ADVFS_ARGS
#define _ADVFS_ARGS
typedef struct advfs_args {
    char        *fspec;            /* filesytem to mount */
    int32_t      adv_flags;        /* AdvFS flags */
    uint32_t     fsid;             /* Potential fileset id/dev */
#   ifdef tru64_to_hpux_port_comments
    /*                           -> currently the kernel doesn't look for
     *                              these fields. They may be used in the
     *                              future.
     */
    char         *fileset;
#   endif
} advfs_args_t;
#endif

/*
 * AdvFS specific mount flags
 */
#define MS_ADVFS_NOATIMES      0x00000001
#define MS_ADVFS_DIRTY         0x00000002
#define MS_ADVFS_DIRECTIO      0x00000004
#define MS_ADVFS_SINGLE        0x00000008
#define MS_ADVFS_NOREADAHEAD   0x00000010

#ifndef MNTTYPE_ADVFS
#   define MNTTYPE_ADVFS     "advfs"    /* should be defined in mntent.h */
#endif


typedef uint64_t tagNumT;
typedef uint32_t tagSeqT;

typedef struct {
    tagNumT tag_num;         /* tag number, 1 based */
    tagSeqT tag_seq;         /* sequence number */
    uint32_t tag_flags;      /* Flags for the tag */
} bfTagT;


#endif /* _ADVFS_PUBLIC_ */
