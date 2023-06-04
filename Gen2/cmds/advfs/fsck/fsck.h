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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#ifndef FSCK_H
#define FSCK_H


#include <nl_types.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <values.h>
#include <assert.h>
#include <errno.h>
#include <mntent.h>
#include <pthread.h>
#include <time.h>
#include <ctype.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/resource.h>

#include <advfs/ms_public.h>
#include <advfs/bs_public.h>
#include <advfs/ftx_public.h>
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>
#include <advfs/bs_index.h>
#include <advfs/bs_error.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/ms_logger.h>
#include <advfs/advfs_acl.h>

#include "fsck_msg.h"


/*
 * Private types.
 */
typedef enum {
	      RBMT, 
	      BMT, 
	      SBM, 
	      log,
	      tagFile,
	      dir,
	      tag,
	      misc
} locationT;

typedef enum {
	      FREE,
	      USED,
	      ONLIST
} mcellUsage;

/* typedefs for various ODS elements */

typedef vdIndexT       volNumT;
typedef bs_meta_page_t pageNumT;
typedef uint32_t       cellNumT;
typedef bf_vd_blk_t    lbnNumT;
typedef bf_fob_t       fobNumT;
typedef uint64_t       clustNumT;
typedef uint64_t       xtntNumT;

/* zero an mcell address */

#define ZERO_MCID(mc)  ((mc).volume = 0, (mc).page = 0, (mc).cell = 0)

/* how many blocks in a metadata page */

#define ADVFS_METADATA_PGSZ_IN_BLKS (ADVFS_METADATA_PGSZ / DEV_BSIZE)

/* Convenience macros for converting values */

#define PAGE2FOB(pages) ((pages) == XTNT_TERM ? XTNT_TERM : \
                         (pages) * ADVFS_METADATA_PGSZ_IN_FOBS)

#define FOB2PAGE(fobs) ((fobs) == XTNT_TERM ? XTNT_TERM : \
                         (fobs) % ADVFS_METADATA_PGSZ_IN_FOBS ? \
                         ((fobs) / ADVFS_METADATA_PGSZ_IN_FOBS) + 1 : \
                         (fobs) / ADVFS_METADATA_PGSZ_IN_FOBS)

#define FOB2CLUST(fob) \
          (((fob) / ADVFS_FOBS_PER_DEV_BSIZE) / ADVFS_BS_CLUSTSIZE)
#define CLUST2FOB(clust) \
          (((clust) * ADVFS_BS_CLUSTSIZE) * ADVFS_FOBS_PER_DEV_BSIZE)

#define LBN2CLUST(lbn) ((lbn) / ADVFS_BS_CLUSTSIZE)
#define CLUST2LBN(clust) ((clust) * ADVFS_BS_CLUSTSIZE)

/* Macro to determine if two mcidT's match */

#define MCIDS_EQ(a, b) \
    ((a).page != (b).page || (a).cell != (b).cell || (a).volume != (b).volume \
     ? 0 : 1)

/* Macro to determine a tag number is for the root tag file */

#define IS_ROOT_TAG(tag) ((int64_t)((tag).tag_num) == -2)

/* Macros to deal with ODS versions.  The VERSION_UNSUPPORTED macro
   will need to be adjusted as the ODS version marches forward. */

#define MAJ_VERSION_EQ(v1, v2)  ((v1).odv_major == (v2).odv_major)
#define MIN_VERSION_EQ(v1, v2)  ((v1).odv_minor == (v2).odv_minor)
#define VERSION_EQ(v1, v2) (MAJ_VERSION_EQ(v1, v2) && MIN_VERSION_EQ(v1, v2))

#define VERSION_UNSUPPORTED(v) \
    ((v).odv_major > BFD_ODS_LAST_VERSION_MAJOR || \
     ((v).odv_major == BFD_ODS_LAST_VERSION_MAJOR && \
      (v).odv_minor > BFD_ODS_LAST_VERSION_MINOR))

/* Macros to deal with cookies. */

#define COOKIE_SEC_EQ(c1, c2)  ((c1).id_sec == (c2).id_sec)
#define COOKIE_USEC_EQ(c1, c2)  ((c1).id_usec == (c2).id_usec)
#define COOKIE_EQ(f1, f2) (COOKIE_SEC_EQ(f1, f2) && COOKIE_USEC_EQ(f1, f2))
#define DOMAIN_EQ(d1, d2) (COOKIE_EQ(d1, d2))

#define COOKIE_SEC_GT(c1, c2)  ((c1).id_sec > (c2).id_sec)
#define COOKIE_USEC_GT(c1, c2)  ((c1).id_usec > (c2).id_usec)
#define COOKIE_GT(f1, f2) (COOKIE_SEC_GT(f1, f2) || \
                           (COOKIE_SEC_EQ(f1, f2) && COOKIE_USEC_GT(f1, f2)))

/* These are assignment macros corresponding to the GETTAG and GETSEQP
   macros found in fs_dir.h */

#define PUTTAG_NUM(n, dir_entry_p) ((dir_entry_p)->fs_dir_bs_tag_num = (n))
#define PUTTAG_SEQ(s, dir) (*(GETSEQP(dir)) = (s))

#define BS_TD_MAX_SEQ 0x7fffffff  /* duplicated from bs_tagdir_kern.h */

/* 
 * These definitions to show which RBMT mcells are for which purpose.
 * This is used in rbmt.c for RBMT content correction.  These should
 * all be defined in bs_ods.h, but for now the RBMT last few are not.
 */

#define RBMT_RBMT       BFM_RBMT       /* cell 0 : RBMT extent & attr info */
#define RBMT_SBM        BFM_SBM        /* cell 1 : SBM info */
#define RBMT_RTAG       BFM_BFSDIR     /* cell 2 : root tag dir info */
#define RBMT_LOG        BFM_FTXLOG     /* cell 3 : LOG info */
#define RBMT_BMT        BFM_BMT        /* cell 4 : BMT info */
#define RBMT_MISC       BFM_MISC       /* cell 5 : MISC info */
#define RBMT_RBMT_EXT   BFM_RBMT_EXT   /* cell 6 : RBMT dmn attr info */
#define RBMT_RBMT_EXT1  BFM_DMN_NAME   /* cell 7 : RBMT  dmn_name rec here */
#define RBMT_RBMT_EXT2  BFM_RUN_TIMES  /* cell 8 : RBMT: run_times rec here */
#define RBMT_BMT_EXT    BFM_RSVD_CELLS /* cell 9 : BMT extra xtnts */
#define RBMT_MISC_EXT   10             /* cell 10 : MISC extra xtnts */
#define RBMT_RBMT_EXT3  RBMT_RSVD_CELL /* last cell : RBMT extra xtnts */

/* 
 * This macro is similar to BS_BFTAG_RSVD_INIT() but evaluates to only the
 * tag.tag_num part of a normal tag.
 */
#define RSVD_TAGNUM_GEN(vdi, cellnum)    \
       ((uint64_t) -((((int64_t)vdi) * BFM_RSVD_TAGS) + (cellnum)))

/*
 * These two defines should be in bs_index.h, but instead they're in
 * bs_index.c.  So, we need to also define them.
 */
#define FNAME 0
#define FFREE 1


/* XXX - this should be defined in bs_ods.h (not bs_bmt.h, where it
   now is).  For now, we duplicate it here */

#define BS_BMT_XPND_PGS 128

/* XXX - the below value defines at what percentage across the disk
   the SBM should start.  This is currently defined in the kernel in
   bs_init.c, but that should be moved to BS-ods.h so we can us it
   here.  For now we duplicate it here. */

#define SBM_START_PCT 40

/*
 * Global defines.
 */
#define TRUE  1
#define FALSE 0

#define ADVFS_METADATA_PGSZ_IN_BLKS (ADVFS_METADATA_PGSZ / DEV_BSIZE)
#define MAX_VOLUMES (BS_MAX_VDI + 1)

/* mcellId.page is 48-bit a value with PAGE_TERM (-1) used as
 * end-of-list indicator.
 */
#define MAX_BMT_PAGES PAGE_TERM 

#define ALL_BITS_SET	UINT_MAX
#define ALL_BITS_SHORT  USHRT_MAX

#define INITIAL_CACHE_SIZE 1024

#define LAST_MCELL (BSPG_CELLS - 1)

typedef enum {
    TAGPAGE_FREE,
    TAGPAGE_FULL,
    TAGPAGE_USED,
} tagPageStateT;

typedef enum {
    TAG_FREE,
    TAG_USED,
    TAG_FREE_LIST
} tagStateT;

typedef enum {
    BMTPAGE_FREE,
    BMTPAGE_FULL,
    BMTPAGE_ON_LIST,
} bmtPageStateT;

#define RBMT_START_BLK RBMT_BLK_LOC 	/* LBN */
#define BMT_START_BLK (RBMT_START_BLK + ADVFS_METADATA_PGSZ_IN_BLKS) 

/*
 * The following 4 defines (MAXLEVELS through BITS_IN_RANDOM)
 * are for skiplists.
 */
#define MAXLEVELS 16
#define MAXLEVEL (MAXLEVELS - 1)
#define NEW_NODE_OF_LEVEL(level) (nodeT *)ffd_malloc(sizeof(struct node) + \
						     (level) * sizeof(nodeT *))
#define BITS_IN_RANDOM 31

#define MAXERRMSG 256	/* Max size of error message string for corrupt_exit */

/*
 * the following defines do not exist on this version of the OS.  To
 * work around build problems we are adding them here.
 */
#define DEF_SVC_CLASS 1
#define LOG_SVC_CLASS 2


/*
 * Function Return Codes
 */

typedef enum {
    SUCCESS,  /* 0 */
    FAILURE,
    NO_MEMORY,
    CORRUPT,
    NON_ADVFS,
    FIXED,
    RESTART,
    NODE_EXISTS,
    NOT_FOUND,
    DELETED,
    FIXED_PRIOR,
    DONE,
    UNSUPPORTED_VERSION
} returnCodeT;

/*
 * Global exit() Codes
 */

#define EXIT_SUCCESS 0
#define EXIT_CORRUPT 1
#define EXIT_FAILED  2

/*
 * Symbolic names for verbosity levels, in writemsg().
 */

#define SV_ALL	   0x0000
#define SV_NORMAL  0x0001
#define SV_VERBOSE 0x0002
#define SV_DEBUG   0x0003

#define SV_MASK    0x0007  /* Masks the basic severity levels */

#define SV_DATE	   0x0008  /* Single bit, to be or'ed with severity code */
#define SV_ERR	   0x0010  /* Single bit, to be or'ed with severity code */
#define SV_CONT	   0x0020  /* Single bit, to be or'ed with severity code */

/*
 * Bit masks to let us know they should go to the log as well.
 */
#define SV_LOG_INFO   0x0100 
#define SV_LOG_SUM    0x0200
#define SV_LOG_FOUND  0x0300
#define SV_LOG_FIXED  0x0400
#define SV_LOG_WARN   0x0500
#define SV_LOG_ERR    0x0600
#define SV_LOG_DEBUG  0x0700

#define SV_LOG_MASK   0x0F00


/*
 * Domain Bit Status
 */
#define DMN_SBM_LOADED		0x00000001
#define DMN_SBM_LOADED_MASK	0x0000000F
#define DMN_META_WRITTEN        0x00000010

/*
 * Set Domain Bit Status
 */
#define DMN_SET_SBM_LOADED(s)   (s) = (((s)&(~DMN_SBM_LOADED_MASK))|(DMN_SBM_LOADED))
#define DMN_CLEAR_SBM_LOADED(s) (s) = ((s)&(~DMN_SBM_LOADED_MASK))

/*
 * Check Domain Bit Status
 */
#define DMN_IS_SBM_LOADED(s)	(((s)&(DMN_SBM_LOADED_MASK)) == (DMN_SBM_LOADED))




/*
 * Fileset Bit Status
 */
#define FS_SNAP		        0x00000001
#define FS_SNAP_MASK		0x0000000F
#define FS_ORIGFS		0x00000010
#define FS_ORIGFS_MASK  	0x000000F0
#define FS_SNAP_OUT_SYNC	0x00000100
#define FS_SNAP_OUT_SYNC_MASK	0x00000F00

/*
 * Set Fileset Bit Status
 */
#define FS_SET_SNAP(s)   (s) = (((s)&(~FS_SNAP_MASK))|(FS_SNAP))
#define FS_SET_ORIGFS(s)  (s) = (((s)&(~FS_ORIGFS_MASK))|(FS_ORIGFS))
#define FS_SET_SNAP_OUT_SYNC(s)  (s) = (((s)&(~FS_SNAP_OUT_SYNC_MASK))|(FS_SNAP_OUT_SYNC))

/*
 * Check Fileset Bit Status
 */
#define FS_IS_SNAP(s)	(((s)&(FS_SNAP_MASK)) == (FS_SNAP))
#define FS_IS_ORIGFS(s)	(((s)&(FS_ORIGFS_MASK)) == (FS_ORIGFS))
#define FS_IS_SNAP_OUT_SYNC(s)	(((s)&(FS_SNAP_OUT_SYNC_MASK)) == (FS_SNAP_OUT_SYNC))


/*
 * Mcell Bit Status
 */
#define MC_DEF_DEL		0x00000001
#define MC_DEF_DEL_MASK		0x0000000F
#define MC_FOUND_PTR		0x00000010
#define MC_FOUND_PTR_MASK	0x000000F0
#define MC_MISSING_TAG          0x00000100
#define MC_MISSING_TAG_MASK     0x00000F00
#define MC_PRIMARY		0x00001000
#define MC_PRIMARY_MASK		0x0000F000
#define MC_FSSTAT		0x00010000
#define MC_FSSTAT_MASK		0x000F0000

/*
 * Set Mcell Bit Status
 */
#define MC_SET_DEF_DEL(s)	(s) = (((s)&(~MC_DEF_DEL_MASK))|(MC_DEF_DEL))
#define MC_SET_FOUND_PTR(s)	(s) = (((s)&(~MC_FOUND_PTR_MASK))|(MC_FOUND_PTR))
#define MC_SET_MISSING_TAG(s)	(s) = (((s)&(~MC_MISSING_TAG_MASK))|(MC_MISSING_TAG))
#define MC_SET_PRIMARY(s)	(s) = (((s)&(~MC_PRIMARY_MASK))|(MC_PRIMARY))
#define MC_SET_FSSTAT(s)	(s) = (((s)&(~MC_FSSTAT_MASK))|(MC_FSSTAT))

#define MC_CLEAR_MISSING_TAG(s) (s) = ((s)&(~MC_MISSING_TAG_MASK))
#define MC_CLEAR_FOUND_PTR(s)   (s) = ((s)&(~MC_FOUND_PTR_MASK))
#define MC_CLEAR_FSSTAT(s)       (s) = ((s)&(~MC_FSSTAT_MASK))

/*
 * Check Mcell Bit Status
 */
#define MC_IS_DEF_DEL(s)	(((s)&(MC_DEF_DEL_MASK)) == (MC_DEF_DEL))
#define MC_IS_FOUND_PTR(s)	(((s)&(MC_FOUND_PTR_MASK)) == (MC_FOUND_PTR))
#define MC_IS_MISSING_TAG(s)	(((s)&(MC_MISSING_TAG_MASK)) == (MC_MISSING_TAG))
#define MC_IS_PRIMARY(s)	(((s)&(MC_PRIMARY_MASK)) == (MC_PRIMARY))
#define MC_IS_FSSTAT(s)		(((s)&(MC_FSSTAT_MASK)) == (MC_FSSTAT))

/*
 * Tag Bit Status
 */
#define T_FOUND_PMCELL			0x00000001
#define T_FOUND_PMCELL_MASK		0x0000000F
#define T_FOUND_DIR			0x00000010
#define T_FOUND_DIR_MASK		0x000000F0
#define T_HAS_ERRORS			0x00000100
#define T_HAS_ERRORS_MASK		0x00000F00
#define T_DIR_INDEX			0x00001000
#define T_DIR_INDEX_MASK		0x0000F000
#define T_DELETE_WITH_SNAP		0x00010000
#define T_DELETE_WITH_SNAP_MASK 	0x000F0000
#define T_SNAP_ORIG_PMCELL		0x00100000
#define T_SNAP_ORIG_PMCELL_MASK 	0x00F00000


/*
 * Set Tag Bit Status
 */
#define T_SET_FOUND_PMCELL(s)		(s) = (((s)&(~T_FOUND_PMCELL_MASK))|(T_FOUND_PMCELL))
#define T_SET_FOUND_DIR(s)		(s) = (((s)&(~T_FOUND_DIR_MASK))|(T_FOUND_DIR))
#define T_SET_HAS_ERRORS(s)		(s) = (((s)&(~T_HAS_ERRORS_MASK))|(T_HAS_ERRORS))
#define T_SET_DIR_INDEX(s)		(s) = (((s)&(~T_DIR_INDEX_MASK))|(T_DIR_INDEX))
#define T_SET_DELETE_WITH_SNAP(s)	(s) = (((s)&(~T_DELETE_WITH_SNAP_MASK))|(T_DELETE_WITH_SNAP))
#define T_SET_SNAP_ORIG_PMCELL(s)	(s) = (((s)&(~T_SNAP_ORIG_PMCELL_MASK))|(T_SNAP_ORIG_PMCELL))


/*
 * Check Tag Bit Status
 */
#define T_IS_FOUND_PMCELL(s)		(((s)&(T_FOUND_PMCELL_MASK)) == (T_FOUND_PMCELL))
#define T_IS_FOUND_DIR(s)		(((s)&(T_FOUND_DIR_MASK)) == (T_FOUND_DIR))
#define T_IS_HAS_ERRORS(s)		(((s)&(T_HAS_ERRORS_MASK)) == (T_HAS_ERRORS))
#define T_IS_DIR_INDEX(s)		(((s)&(T_DIR_INDEX_MASK)) == (T_DIR_INDEX))
#define T_IS_DELETE_WITH_SNAP(s)	(((s)&(T_DELETE_WITH_SNAP_MASK)) == (T_DELETE_WITH_SNAP))
#define T_IS_SNAP_ORIG_PMCELL(s)	(((s)&(T_SNAP_ORIG_PMCELL_MASK)) == (T_SNAP_ORIG_PMCELL))


/*
 * Page Bit Status
 */
#define P_MODIFIED	0x00000001
#define P_MODIFIED_MASK	0x0000000F

/*
 * Set Page Bit Status
 */
#define P_SET_MODIFIED(s)	(s) = (((s)&(~P_MODIFIED_MASK))|(P_MODIFIED))

/*
 * Check Page Bit Status
 */
#define P_IS_MODIFIED(s)	(((s)&(P_MODIFIED_MASK)) == (P_MODIFIED))

/*
 * byte count macros to deal with property lists
 */
#define PL_ALIGN(x1)		(((x1) + 7) &~ 7)
#define PL_ENTRY_SIZE(x1, x2)	(PL_ALIGN(x1)+PL_ALIGN(x2))

/* load x1 from x2; x1 must be long-word aligned; return x1 */
#define FLAGS_READ(x1,x2)                               \
  (                                                     \
   ((uint32_t *) &(x1))[0] = ((u_int *)&(x2))[0],        \
   ((uint32_t *) &(x1))[1] = ((u_int *)&(x2))[1],        \
   (x1)                                                 \
   )

/* macro to check an mcell record type and return TRUE if it's a
   floating record.  A "floating" record is an RBMT record that does
   not live in a predetermined mcell in the RBMT. */

#define IS_RBMT_FLOATER(type) \
 ((type) == BSR_VD_IO_PARAMS || (type) == BSR_DMN_SS_ATTR)

/*
 * Global data
 */
char *Prog;
FILE *Localtty;

/*
 * Macros
 */

extern char *sys_errlist[];
#define ERRMSG( eno ) sys_errlist[( eno )]


/*
 * fsck structures
 */

/* The following typedefs are declared here so that the following
   structs reference these typedefs. */

typedef struct domain domainT;
typedef struct volume volumeT;
typedef struct fileset filesetT;
typedef struct fobtoLBN fobtoLBNT;
typedef struct page pageT;
typedef struct fileNode fileNodeT;
typedef struct rbmtInfo rbmtInfoT;
typedef struct tagNode tagNodeT;
typedef struct mcellNode mcellNodeT;
typedef struct message messageT;
typedef struct sbmInfo sbmInfoT;
typedef struct extentId extentIdT;
typedef struct overlapNode overlapNodeT;
typedef struct list listT;
typedef struct fsQuota fsQuotaT;
typedef struct ddlSnapFile ddlSnapFileT;

/* optionsT */
typedef struct options {
    int         echoCmdline;    /* -V option */
    int         advfs;          /* for -V option: was -F advfs present */
    int		verboseOutput;  /* level of output verbosity */
    int		activate;       /* activate FS after updates done */
    int         yes;            /* -y option */
    int         no;             /* -n option */
    int         sanityCheck;    /* -m option - do sanity check only */
    int         full;           /* do full FS check */
    int         nolog;          /* skip the normal preActivation */
    int		nochange;       /* if checks change will not go to disk */
    int		preen;          /* if user interaction is needed, exit */
    int         sync;           /* sync certain metadata across the FS */
    int		undo;           /* if user spec'd an undo file path */
    int         msglog;         /* if user spec'd a log file path */
    FILE	*msglogHndl;	/* FILE handle of the fsck log file */
    char	*msglogFile;	/* Full pathname of the fsck log file */
    char	*dirPath;       /* log/undo directory path */
} optionsT;

optionsT Options;


/* mcidT */

typedef bfMCIdT mcidT;

/* domainT */
struct domain {
    char		*dmnName;	/* domain name */
    bfDomainIdT		dmnId;		/* domain Id */
    bfFsCookieT         fsCookie;       /* definitive FS cookie */
    adv_ondisk_version_t ODSVersion;	/* definitive ODS version */
    listT		*mcellList;	/* list of mcells */
    volumeT     	*volumes;	/* array of volume information */
    volNumT		numVols;	/* number of active volumes */
    volNumT		highVolNum;	/* highest volume number */
    volNumT		logVol;		/* volume containing the log */
    volNumT		rootTagVol;	/* volume containing the root tag */
    fobtoLBNT   	*rootTagLBN;	/* root tag file page to LBN array */
    xtntNumT    	rootTagLBNSize;	/* number of entries in rootTagLBN */
    int			errorCount;	/* number of errors found in domain */
    int			numFilesets;	/* number of FS's in domain */
    filesetT	        *filesets;	/* linked list of FS structs */
    fsQuotaT            *filesetQuota;  /* linked list of FS quotas */
    int			status;		/* status */
    ddlSnapFileT        *ddlSnapList;   /* list of snap files from 
					   Deferred Delete List (DDL) */
};


/* filesetT */
struct fileset {
    domainT		*domain;	  /* Pointer to the parent domain */
    bfSetIdT		filesetId;	  /* fileset Id */
    char		*fsName;	  /* fileset name */
    mcidT       	primMCId;	  /* fileset tag file's primary MCID */
    fobtoLBNT	        *tagLBN;	  /* tag file  extents */
    xtntNumT		tagLBNSize;	  /* number of extent entries */
    listT		*tagList;	  /* list of tags */

    bfTagT              parentSetTag;     /* tag for snap's parent FS */
    bfTagT              childSetTag;      /* tag for snap's 1st child FS */
    bfTagT              sibSetTag;        /* tag for snap's next sibling FS */
    uint16_t            snapLevel;        /* 0=root. >0 is # levels to root */
    int                 linkFixed;        /* bool: a snap link was fixed */
    filesetT            *origSnapPtr;     /* pointer to original set struct */

    int			status;		  /* status */
    int			errorCount;	  /* error count for fileset */
    filesetT    	*next;		  /* next fileset */
};


/* volumeT */
typedef struct volume {
    int		       volFD;		/* volume file descriptor */
    char	       *volName;	/* volume device name */
    fsblkcnt_t         volSize;		/* size of volume in DEV_BSIZE units */
    bf_fob_t           userAllocSz;     /* user page size in FOBs */
    bf_fob_t           metaAllocSz;     /* metadata page size in FOBs */
    advfs_fake_superblock_t  super;     /* fake superblk for this volume */
    rbmtInfoT          *volRbmt;	/* volume RBMT information */
    bsIdT              mountId;		/* last mount time for this volume */
    int		       mntStatus;	/* mount status for this volume */
    int		       rootTag;		/* Is the root tag on this volume. */
    sbmInfoT           *volSbmInfo;	/* pointer to Volume SBM structures. */
    vdCookieT          vdCookie;        /* volume cookie - unique ID */
};


/* rbmtInfoT */
struct rbmtInfo {
    fobtoLBNT   	*sbmLBN;	/* SBM fob to LBN array */
    xtntNumT		sbmLBNSize;	/* number of entries in sbmLBN */
    fobtoLBNT   	*bmtLBN;	/* BMT fob to LBN array */
    xtntNumT		bmtLBNSize;	/* number of entries in bmtLBN */
    fobtoLBNT   	*logLBN;	/* log file fob to LBN array */
    xtntNumT		logLBNSize;	/* number of entries in logLBN */
    fobtoLBNT   	*rbmtLBN;	/* RBMT fob to LBN array */
    xtntNumT	       	rbmtLBNSize;	/* number of entries in rbmtLBN */
    fobtoLBNT           *miscLBN;       /* misc file fob to LBN array */
    xtntNumT            miscLBNSize;    /* number of entries in miscLBN */
};

/* mcellNodeT - Section 2.1.5 */
struct mcellNode {
    mcidT	        mcid;	        /* volume, BMT page, and mcell num */
					/* mcellId used for key into skiplist*/
    int			status;		/* status bits */
    bfTagT		tagSeq;		/* tag and sequence number */
    bfTagT		setTag;		/* fileset tag and sequence number */
};


/* tagNodeT */
struct tagNode {
    bfTagT		tagSeq;		/* used for key into skiplist */
    int			status;		/* status bits */
    int			fileType;	/* st_mode from FS_STAT record */
    adv_off_t		size;		/* size in bytes from FS_STAT record */
    mcidT      	        tagMCId;	/* primary mcell for tag */
    bf_fob_t		fobsFound;	/* number of FOBS found in extents */
    uint64_t		numLinks;	/* number of links from FS_STAT rec */
    uint64_t		numLinksFound;	/* number of links found in metadata */
    bfTagT		parentTagNum;	/* tag number of this tag's parent */
    bfTagT		dirIndexTagNum;	/* tag number of this tag's dir index*/
};


/* messageT */
struct message {
    locationT		diskArea;	/* Which ODS has the message */
    char		*message;	/* error message text */
    messageT     	*next;		/* next message in list */
};


/* overlapNodeT - Section 2.1.8 */
/* 
 * The extentId structure holds the "address" of an extent
 * record.  This structure is used by the SBM routines.  It
 * is also embedded within the overlapNode structure.
 */
struct extentId {
    mcidT 	cellAddr;
    xtntNumT   	extentNumber;	/* index into extent array */
};

/*
 * The overlapNode structure was created because we cannot use the
 * skip lists as they are currently implemented, so the overlapping
 * extent information will be stored in a simple linked list.
 *
 * There are two pointers to these structures embedded with this
 * structure.  One for ordering the nodes by the file/extent
 * information.  The second is for ordering the nodes by LBNs.
 */
struct overlapNode {
    extentIdT          xtntId;		 /* The addr of the defining extent. */
    tagNumT	       tagnum;		 /* Tag num of the overlapping file. */
    tagNumT	       setnum;		 /* Fileset tag number. */
    lbnNumT	       extentStartLbn;	 /* LBN of the start of the extent. */
    clustNumT	       extentClustCount;  /* # of clusters in this extent. */
    clustNumT	       overlapStartClust; /* cluster # where overlap begins. */
    clustNumT	       overlapClustCount;  /* Size of the overlapped area. */
    overlapNodeT       *nextByLbn;
    overlapNodeT       *nextByMcell;
};


/* 
 * The fileNode structure is to be used to maintain a list
 * of files that will be deleted from the domain.  This is
 * internal to the SBM routines.
 */
struct fileNode {
    tagNumT	tagnum;	/* tag number of the file to be deleted */
    tagNumT	setnum;	/* fileset number */
    fileNodeT	*next;
};


/* Storage Arrays - Section 2.1.10 */

/* sbmInfoT */
struct sbmInfo {
    uint32_t		*sbm;
    uint32_t		*overlapBitMap;
    overlapNodeT	*overlapListByFile;
    overlapNodeT	*overlapListByLbn;
};


/* Cache_Structure - Section 2.1.11 */

typedef char pageBufT[ADVFS_METADATA_PGSZ];

/* pageT - Section 2.1.11.1 */
struct page {
    pageBufT		*pageBuf;	/* pointer to page data */
    pageT		*nextMRU;	/* next most recently used page */
    pageT		*nextLRU;	/* next least recently used page */
    struct message	*messages;	/* list of messages */
    pthread_mutex_t	lock;		/* page lock */
    lbnNumT		pageLBN;	/* start LBN of page (used for key) */
    int			status;		/* status bits */
    volNumT		vol;		/* volume number (used for key) */
};


/* cacheT - Section 2.1.11.2 */
typedef struct cache {
    listT	*readCache;	  /* list of read-only cache entries */
    listT	*writeCache;	  /* list of modified pages */
    pageT       *pageArray;       /* start adrs of cache page descriptors */
    pageBufT    *pageArrayData;   /* start adrs of cache pages */
    pageT	*MRU;		  /* most recently used page in read cache. */
    pageT	*LRU;		  /* least recent used page in read cache. */
    pageNumT	pagesLocked;	  /* number of locked pages in read cache. */
    int64_t	writeHits;	  /* number of hits on the write cache */
    int64_t	readHits;	  /* number of hits on the read cache */
    int64_t	misses;		  /* number of cache misses */
    int64_t	maxSize;	  /* maximum size of the readCache */
    domainT	*domain;	  /* pointer to the domain */
    pthread_mutex_t lock;	  /* cache lock */
} cacheT;


/* General_List_Structures - Section 2.1.12 */

/* nodeT - Section 2.1.12.1 */
typedef struct node {
    void	*data;		/* data stored for public use. */
    struct node	*forward[1];	/* dynamically malloc'ed array of pointers */
				/* to the next nodes at each level of the  */
				/* skiplist */
} nodeT;


/* listT - Section 2.1.12.2 */
typedef struct list {
    struct node	*header;	/* header node for the list */
    struct node	*tail;		/* pseudo-node with the maximum key possible */
    int		level;		/* current highest level of the list */
    int		size;		/* number of nodes in the list */
    int (*compare)(void *, void *); /* The comparison function for the list */
    int		randomsLeft;	/* How many randoms are left in randomBits */
    int		randomBits;	/* Stores a random number to get bits out of */
    pthread_rwlock_t lock;	/* read-write lock for exclusive operations */
};

typedef struct fobtoLBN {
    bf_fob_t fob;	/* FOB to map from    */
    lbnNumT  lbn;	/* LBN to map to       */
    volNumT  volume;    /* Volume FOB is on   */
    int      isOrig;	/* TRUE means we're in a snap fileset yet
			   these extents belong to a file that's
			   untouched since the snap was made (still
			   owned by the original fileset). */
};

/* logVol */
typedef struct logVol {
    volNumT	  volume;
    struct logVol *next;
} logVolT;

/*
 * structure to store quota info
 */
struct fsQuota {
    bfTagT     filesetTag;  /* fileset tag and sequence number */    
    struct fsQuota *next;	
    uint64_t    hardBlockLimit;
    uint64_t    softBlockLimit;
    uint64_t    hardFileLimit;
    uint64_t    softFileLimit;
    int64_t    blockTimeLimit;
    int64_t    fileTimeLimit; 
    uint32_t   quotaStatus;
};

/*
 * Need to save deferred delete list records for snapshot files.
 */

struct ddlSnapFile {
    bfTagT   setTag;  /* fileset tag and sequence number */
    bfTagT   fileTag;
    ddlSnapFileT *next;
};

/*====================================
 * function prototypes
 *===================================*/

/*
 * utilities.c
 */

void writemsg(int  severity, 
	      char *fmt, 
	      ...);

void writelog(int  severity, 
	      char *fmt, 
	      ...);

char * get_timestamp(char   *dateStr, 
		     size_t maxLen);

char * get_time(char   *dateStr, 
		size_t maxLen);

int prompt_user(char *answer,
		int  size);

int prompt_user_yes_no(char *answer,
		       char *defaultAns);

int prompt_user_to_continue(domainT *domain,
			    char    *message);

int create_volumes(volumeT **volumes);

int get_domain_volumes(arg_infoT *fsInfo,
		       domainT *domain);

int open_domain_volumes(domainT *domain);

int load_subsystem_extents(domainT *domain);

int collect_extents(domainT   *domain,
		    filesetT  *fileset,
		    mcidT    *pPrimMcell, 
		    int	      addToSbm, 
		    fobtoLBNT **extents, 
		    xtntNumT  *arraySize,
		    int	      isRBMT);

int save_fileset_info(domainT *domain,
		      pageT   *pPage);

void corrupt_exit(char *message);
void preen_exit(void);
void failed_exit(void);
void finalize_fs(domainT *domain);

int create_undo_files(domainT *domain);

int write_cache_to_disk(void);

int close_domain_volumes(domainT *domain);

int activate_domain(domainT *domain);

int pre_activate_domain(domainT *domain);

int restore_undo_files(domainT *domain);

int convert_page_to_lbn(fobtoLBNT *extentArray,
			xtntNumT  extentArraySize,
			pageNumT  page,
			lbnNumT   *lbn,
			volNumT   *vol);

int predict_memory_usage(domainT *domain);

void *ffd_malloc(size_t size);

void *ffd_calloc(size_t numElts,
		 size_t eltSize);

void *ffd_realloc(void   *pointer,
		  size_t size);

int add_fixed_message(messageT  **pMessage,
		      locationT diskArea, 
		      char      *fmt,
		      ...);

int collect_rbmt_extents(domainT *domain, 
			 volNumT volNum, 
			 int     collectSbm);

int log_lsn_gt(lsnT lsn1, 
	       lsnT lsn2);

/*
 * metadata.c
 */

int is_advfs_volume(volumeT *volume);

int correct_magic_number(domainT *domain,
			 volNumT volNum);

int validate_tag_file_page(domainT *domain,
			   pageT *tagFilePage,
			   pageNumT pageNum,
			   int isRbmt);

int validate_log_file_page(domainT *domain,
			   pageT *logFilePage);

int clear_log(domainT *domain);

int zero_log(domainT *domain);

int validate_bmt_page(domainT *domain,
		      pageT *pPage,
		      pageNumT pageId,
		      int is_rbmt);

int reset_free_mcell_links(pageT      *pPage, 
			   mcellUsage *freeMcells);

int correct_free_mcell_list(volumeT *volume, 
			    int	    *bmtStatusArray);

int clear_deferred_delete_list(domainT *domain);


int correct_snap_files_on_ddl(domainT *domain);

int correct_tag_file(domainT *domain, 
		     tagNumT filesetNum);

int correct_tag_page_records(pageT    *pPage,
			     domainT  *domain,
			     filesetT *fileset,
			     pageNumT pageNumber,
			     pageNumT tagUninitPg);

int correct_tag_to_mcell(filesetT *fileset,
			 tagNumT  tagNum,
			 uint32_t seqNum,
                         mcidT    primMCId);

int correct_nodes(domainT *domain);

int correct_orphans(domainT *domain);

int correct_bmt_chains(filesetT *fileset);

int correct_chain_mcell_count(filesetT *fileset, 
			      mcidT    chainMcell, 
			      mcidT    xtntMcell, 
			      int      whichRecord);

int correct_empty_chain(filesetT *fileset, 
			tagNodeT *pTagNode);

int correct_quota_files(filesetT *fileset);

int correct_fsstat(filesetT *fileset, 
		   tagNodeT *tagNode);

int validate_fileset_name(char *fsName);

int delete_tag(filesetT *fileset,
	       tagNumT	tagNum);

int correct_chain_fsstat(filesetT *fileset, 
			 mcidT	  primaryMcell, 
			 tagNumT  tagNum);

int correct_chain_extents(filesetT   *fileset, 
			  mcidT      primaryMcell, 
			  fobtoLBNT  *xtntArray);

int correct_extent_array(xtntNumT   numExtents, 
			 int        *recordArray, 
			 fobtoLBNT  *xtntArray);

int correct_bsr_xtnt_tag(filesetT *fileset, 
			 mcidT    bmtXtntMcell, 
			 mcidT    chainMcell,
			 pageT    *pPage);

int append_page_to_file(filesetT *fileset,
			tagNodeT *tag,
			int      *foundPage);

int free_orphan(domainT  *domain, 
		volNumT  volNum, 
		pageNumT pageNum, 
		cellNumT mcellNum);

/*
 * rbmt.c
 */

int correct_domain_rbmt(domainT *domain);

/*
 * bmt.c
 */
int free_mcell(domainT  *domain,
	       volNumT  volNum,
	       pageT    *pPage, 
	       cellNumT mcellNum);

int delete_mcell_record(domainT  *domain,
			pageT    *pPage,
			cellNumT mcellNum,
			bsMRT    *pRecord);

int clear_mcell_record(domainT *domain,
		       pageT   *pPage, 
		       cellNumT mcellNum,
		       int      recNum);

int correct_bmt_mcell_data (domainT *domain);

int correct_bmt_page_header(pageNumT pageNumber,
			    volNumT  volNum,
			    int	     inExtent,
			    lbnNumT  nextLBN,
			    pageNumT nextPageNumber,
			    pageT    *pPage,
			    domainT  *domain,
			    int      rbmtFlag);

/*
 * dir.c
 */

int correct_all_dirs(filesetT *fileset);

/*
 * sbm.c
 */

int add_tag_to_delete_list(tagNumT   tag,
			   tagNumT   settag,
			   fileNodeT **deleteFileListHead);

int collect_sbm_info(domainT    *domain,
		     fobtoLBNT  *extentArray,
		     int        numRecords,
		     tagNumT    tag, 
		     tagNumT    settag,
		     mcidT      *xtntMcell,
		     xtntNumT	xtntNumOffset);

int correct_sbm(domainT *domain);

int correct_sbm_overlaps(domainT *domain);

int is_page_free(domainT *domain,
		 volNumT volNum,
		 lbnNumT pageLBN);

int remove_extent_from_sbm(domainT   *domain,
			   volNumT   volNum,
			   lbnNumT   startLbn, 
			   clustNumT extentClustCount,
			   tagNumT   tag, 
			   tagNumT   settag,
			   mcidT     *xtntMcell,
			   xtntNumT  extentNumber);

int update_sbm_mcell_tag_and_set_info(domainT *domain,
                                      mcidT   *currMcell,
                                      tagNumT oldTag,
                                      tagNumT oldSetTag,
                                      tagNumT newTag,
                                      tagNumT newSetTag);

int validate_sbm_page(pageT *sbmPage);

/*
 * cache.c
 */

int create_cache(domainT *domain);

int free_cache();

int compare_pages(void *a, 
		  void *b);

int read_page_from_cache(volNumT volume,
			 lbnNumT lbn,
			 pageT **page);

int release_page_to_cache(volNumT volume,
			  lbnNumT lbn,
			  pageT *page,
			  int   modified,
			  int   keepPageForever);

/*
 * list.c
 */
int compare_tags(void *a, 
		 void *b);

int compare_mcell_nodes(void *a, 
			void *b);

int create_list(int (*compare)(void *a, void *b),
		listT **list,
		void  *max);

int random_level(listT *list);

int insert_node(listT *list, 
		void  *data);

int delete_node(listT *list, 
		void  **data);

int find_node(listT *list, 
	      void  **data, 
	      nodeT **node);

int find_node_or_next(listT *list, 
		      void  **data, 
		      nodeT **node);

int find_first_node(listT *list, 
		    void  **data, 
		    nodeT **node);

int find_next_node(listT *list, 
		   void	 **data, 
		   nodeT **node);


#endif /* not FSCK_H */
/* end fsck.h */



