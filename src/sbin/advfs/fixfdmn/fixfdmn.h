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
 *      On-disk structure fixer
 *
 * Date:
 *
 *      Mon Jan 10 13:20:44 PST 2000
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: fixfdmn.h,v $ $Revision: 1.1.57.6 $ (DEC) $Date: 2006/03/17 03:03:18 $
 */

#ifndef FIXFDMN_H
#define FIXFDMN_H


#include <nl_types.h>
#include <locale.h>
#include "fixfdmn_msg.h"
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/syslimits.h>
#include <sys/types.h>
#include <sys/fs_types.h>
#include <sys/proplist.h>
#include <msfs/ms_public.h>
#include <msfs/bs_ods.h>
#include <msfs/bs_public.h>
#include <msfs/bs_bitfile_sets.h>
#include <msfs/bs_index.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/time.h>
#include <sys/mode.h>
#include <sys/resource.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <values.h>
#include <tis.h>
#include <assert.h>
#include <safe_open.h>
#include <sys/dir.h>
#include <msfs/fs_dir.h>
#include <msfs/msfs_syscalls.h>


/*
 * The next section was created for fixfdmn to have access to on-disk
 * log structures.  Currently these definitions are embedded within the
 * ms_logger.c module.  This code should be broken out for use outside
 * of ms_logger.c.  Until that time this file should be maintained in
 * sync by hand.  
 */

/*
 * logBlkT -
 *
 * Defines the structure of a log page block.  The LSN of a page's
 * blocks must match in order for the page to be valid.
 */
typedef struct {
    lsnT lsn;
    char data[BS_BLKSIZE - sizeof( lsnT )];
} logBlkT;

/*
 * logPgBlksT -
 *
 * Defines the physical structure of a log page; an array of blocks.
 */
typedef struct {
   logBlkT blks[ADVFS_PGSZ_IN_BLKS];
} logPgBlksT;

/*
 * logPgU -
 *
 * Defines the two different views of a log page; 1) the logical view, and
 * 2) the array of blocks.
 */
typedef union {
   logPgBlksT logPgBlks;  /* physical log page structure; array of blocks */
   logPgT logPg;          /* logical log page structure */
} logPgU;

/*
 * logRecHdrT - 
 *
 * Each log record has a header that describes the record
 * and contains record addresses to neighboring records.  Note that
 * the 'nextRec' record address will never point to a record in another
 * page.  The last record's 'nextRec' record address is set to 
 * 'logEndOfRecords' which tells you that you must look in the next
 * page for the next record (if one exists).  Both 'prevRec' and
 * 'prevClientRec' do point to records in other pages.
 */
typedef struct logRecHdr {
    logRecAddrT nextRec;        /* log addr of next log record              */
    logRecAddrT prevRec;        /* log addr of previous log record          */
    logRecAddrT prevClientRec;  /* log addr of client's previous log record */
    logRecAddrT firstSeg;       /* log addr of record's first segment       */
    uint32T wordCnt;            /* log segment size count in words (inc hdr)*/
    uint32T clientWordCnt;      /* size of client's record (in words)       */
    lsnT lsn;                   /* log sequence number                      */
    uint32T segment;            /* 31-bit segment number; if bit 31 is set  */
                                /*    then there are more segments in the   */
                                /*    record.                               */
} logRecHdrT;

/* 
 * Number words per log record header 
 */
#define REC_HDR_WORDS howmany( sizeof( logRecHdrT ), sizeof( uint32T ) )

/*
 * Number words per ftxDoneLRT record header
 */
#define DONE_LRT_WORDS howmany( sizeof( ftxDoneLRT ), sizeof( uint32T ) )


/*
 * The next section was created for fixfdmn to have access to on-disk
 * log structures.  Currently these definitions are embedded within the
 * ftx_privates.h which contains a whole bunch of stuff we don't need 
 * and causes conflicts on build.
 */
typedef enum { ftxNilLR, ftxDoneLR } ftxLRTypeT;

typedef struct {
    ftxLRTypeT type;            /* ftx Log Record Type */
    signed level : 8;           /* ftx node level */
    unsigned atomicRPass : 8;   /* atomic recovery pass number */
    signed member : 16;         /* ftx node member */
    ftxIdT ftxId;               /* ftx tree globally unique id */
    bfDomainIdT bfDmnId;        /* bitfile domain global unique id */
    logRecAddrT crashRedo;      /* crash redo start log address */
    ftxAgentIdT agentId;        /* ftx opx agent for this ftx node */
    uint32T contOrUndoRBcnt;    /* continuation/undo opx record byte count */
    uint32T rootDRBcnt;         /* root done opx record byte count */
    uint32T opRedoRBcnt;        /* redo opx record byte count */
} ftxDoneLRT;





/*
 * Private types.
 */
typedef enum {
	      RBMT, 
	      BMT, 
	      SBM, 
	      log,
	      frag,
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


/*
 * LBNT is the Logical Block Number Type.  Every variable that
 * holds LBNs should be of this type.  Currently this is defined
 * as a 32-bit address.  When volumes exceed 1TB in size, however,
 * this will need to be enlarged.
 *
 * bs_public.h also defines a bsPageT.
 * that this type is used exclusively as a logical Page Number.
 * Therefore, all variables holding page numbers should be
 * defined as bsPageT variables.
 */
typedef uint32T			LBNT;
typedef bsPageT			pageNumberT;
#define MAXUINT                 4294967293
typedef unsigned long int	uint64;

/*
 * These two defines should be in bs_index.h, but instead they're in
 * bs_index.c.  So, we need to also define them.
 */
#define FNAME 0
#define FFREE 1


/*
 * Global defines.
 */
#define TRUE  1
#define FALSE 0

#define ONE_K 1024L
#define PAGESIZE 8192L
#define BITS_PER_WORD 64
#define BITS_PER_LONG 64
#define BLOCKS_PER_PAGE 16
#define MAX_VOLUMES (BS_MAX_VDI + 1)

#define ALL_BITS_SET	(unsigned long)0xFFFFFFFFFFFFFFFFL
#define ALL_BITS_SHORT  (unsigned short)0xFFFF

#define INITIAL_CACHE_SIZE 1024

#define LAST_MCELL (BSPG_CELLS - 1)

#define TAGPAGE_FREE 0
#define TAGPAGE_FULL 1
#define TAGPAGE_USED 2

#define TAG_FREE 0
#define TAG_USED 1
#define TAG_FREE_LIST 2

#define BMTPAGE_FREE 0
#define BMTPAGE_FULL 1
#define BMTPAGE_ON_LIST 2

#define RBMT_PAGE0 32	/* LBN */

/*
 * The following 4 defines (MAXLEVELS through BITS_IN_RANDOM)
 * are for skiplists.
 */
#define MAXLEVELS 16
#define MAXLEVEL (MAXLEVELS - 1)
#define NEW_NODE_OF_LEVEL(level) (nodeT *)ffd_malloc(sizeof(struct node) + \
						     (level) * sizeof(nodeT *))
#define BITS_IN_RANDOM 31

/*
 * The parameter SBM_LONGS_PG is defined in bs_ods.h as the
 * number of longwords (4 byte words, not long ints which are
 * 8 bytes) that are map words in this SBM page.  Since the
 * ODS V3 code in fixfdmn_sbm references each byte individually,
 * the parameter is multiplied by 4.  The ODS V4 code references
 * each 64 bit word, so the parameter is divided by 2.
 */
#define SBM_MAP_BYTES_PER_PAGE (SBM_LONGS_PG * 4)
#define SBM_MAP_LONGS_PER_PAGE (SBM_LONGS_PG / 2)

#define SM_FREE_MASK		0x80000000
#define SM_MARK_PAGE_FREE(s)	(s) = ((s)|(SM_FREE_MASK))
#define SM_PAGE_IS_FREE(s)	((SM_FREE_MASK) == ((s)&(SM_FREE_MASK)))
#define SM_NEVER_USED(s)	((SM_FREE_MASK) == (s))
#define SM_COUNTER_SET_NUM(s)	((s) == (0xDEADBEEF))


#define MAXERRMSG 256	/* Max size of error message string for corrupt_exit */

/*
 * defines do not exist on this version of the OS.  To work around build
 * problems we are adding them here.
 */
#define DEF_SVC_CLASS 1
#define LOG_SVC_CLASS 2
#define BSR_DMN_TRANS_ATTR 21
#define RBMT_RSVD_CELL (BSPG_CELLS - 1)


/*
 * Return Codes - Section 2.1.17
 */
#define FAILURE		0
#define SUCCESS		1
#define NO_MEMORY	2
#define CORRUPT		3
#define NON_ADVFS	4
#define FIXED		5
#define RESTART		6
#define NODE_EXISTS	7
#define NOT_FOUND	8
#define DELETED		9
#define FIXED_PRIOR	10


/*
 * Global Exit Values - Section 2.1.18
 */
#define EXIT_SUCCESS	0
#define EXIT_CORRUPT	1
#define EXIT_FAILED	2


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
 * Option Type bit status
 */
#define OT_LOG		0x00000001
#define OT_LOG_MASK	0x0000000F
#define OT_SBM		0x00000010
#define OT_SBM_MASK	0x000000F0
#define OT_BMT		0x00000100
#define OT_BMT_MASK	0x00000F00
#define OT_FRAG		0x00001000
#define OT_FRAG_MASK	0x0000F000
#define OT_QUOTA	0x00010000
#define OT_QUOTA_MASK	0x000F0000
#define OT_FILES	0x00100000
#define OT_FILES_MASK	0x00F00000
#define OT_SYNC		0x01000000
#define OT_SYNC_MASK	0x0F000000

/*
 * Set Option Type Bit Status
 */
#define OT_SET_LOG(s)	(s) = (((s)&(~OT_LOG_MASK))|(OT_LOG))
#define OT_SET_SBM(s)	(s) = (((s)&(~OT_SBM_MASK))|(OT_SBM))
#define OT_SET_BMT(s)	(s) = (((s)&(~OT_BMT_MASK))|(OT_BMT))
#define OT_SET_FRAG(s)	(s) = (((s)&(~OT_FRAG_MASK))|(OT_FRAG))
#define OT_SET_QUOTA(s)	(s) = (((s)&(~OT_QUOTA_MASK))|(OT_QUOTA))
#define OT_SET_FILES(s)	(s) = (((s)&(~OT_FILES_MASK))|(OT_FILES))
#define OT_SET_SYNC(s)	(s) = (((s)&(~OT_SYNC_MASK))|(OT_SYNC))

/*
 * Check Option Type Bit Status
 */
#define OT_IS_LOG(s)	(((s)&(OT_LOG_MASK)) == (OT_LOG))
#define OT_IS_SBM(s)	(((s)&(OT_SBM_MASK)) == (OT_SBM))
#define OT_IS_BMT(s)	(((s)&(OT_BMT_MASK)) == (OT_BMT))
#define OT_IS_FRAG(s)	(((s)&(OT_FRAG_MASK)) == (OT_FRAG))
#define OT_IS_QUOTA(s)	(((s)&(OT_QUOTA_MASK)) == (OT_QUOTA))
#define OT_IS_FILES(s)	(((s)&(OT_FILES_MASK)) == (OT_FILES))
#define OT_IS_SYNC(s)	(((s)&(OT_SYNC_MASK)) == (OT_SYNC))


/*
 * Domain Bit Status
 */
#define DMN_SBM_LOADED		0x00000001
#define DMN_SBM_LOADED_MASK	0x0000000F
#define DMN_SM_LOADED		0x00000010
#define DMN_SM_LOADED_MASK	0x000000F0

/*
 * Set Domain Bit Status
 */
#define DMN_SET_SBM_LOADED(s)   (s) = (((s)&(~DMN_SBM_LOADED_MASK))|(DMN_SBM_LOADED))
#define DMN_SET_SM_LOADED(s)    (s) = (((s)&(~DMN_SM_LOADED_MASK)) |(DMN_SM_LOADED))

#define DMN_CLEAR_SBM_LOADED(s) (s) = ((s)&(~DMN_SBM_LOADED_MASK))
#define DMN_CLEAR_SM_LOADED(s)  (s) = ((s)&(~DMN_SM_LOADED_MASK))


/*
 * Check Domain Bit Status
 */
#define DMN_IS_SBM_LOADED(s)	(((s)&(DMN_SBM_LOADED_MASK)) == (DMN_SBM_LOADED))
#define DMN_IS_SM_LOADED(s)	(((s)&(DMN_SM_LOADED_MASK))  == (DMN_SM_LOADED))


/*
 * Fileset Bit Status
 */
#define FS_CLONE		0x00000001
#define FS_CLONE_MASK		0x0000000F
#define FS_ORIGFS		0x00000010
#define FS_ORIGFS_MASK  	0x000000F0
#define FS_CLONE_OUT_SYNC	0x00000100
#define FS_CLONE_OUT_SYNC_MASK	0x00000F00

/*
 * Set Fileset Bit Status
 */
#define FS_SET_CLONE(s)   (s) = (((s)&(~FS_CLONE_MASK))|(FS_CLONE))
#define FS_SET_ORIGFS(s)  (s) = (((s)&(~FS_ORIGFS_MASK))|(FS_ORIGFS))
#define FS_SET_CLONE_OUT_SYNC(s)  (s) = (((s)&(~FS_CLONE_OUT_SYNC_MASK))|(FS_CLONE_OUT_SYNC))

/*
 * Check Fileset Bit Status
 */
#define FS_IS_CLONE(s)	(((s)&(FS_CLONE_MASK)) == (FS_CLONE))
#define FS_IS_ORIGFS(s)	(((s)&(FS_ORIGFS_MASK)) == (FS_ORIGFS))
#define FS_IS_CLONE_OUT_SYNC(s)	(((s)&(FS_CLONE_OUT_SYNC_MASK)) == (FS_CLONE_OUT_SYNC))


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
#define T_DELETE_WITH_CLONE		0x00010000
#define T_DELETE_WITH_CLONE_MASK	0x000F0000
#define T_CLONE_ORIG_PMCELL		0x00100000
#define T_CLONE_ORIG_PMCELL_MASK	0x00F00000


/*
 * Set Tag Bit Status
 */
#define T_SET_FOUND_PMCELL(s)		(s) = (((s)&(~T_FOUND_PMCELL_MASK))|(T_FOUND_PMCELL))
#define T_SET_FOUND_DIR(s)		(s) = (((s)&(~T_FOUND_DIR_MASK))|(T_FOUND_DIR))
#define T_SET_HAS_ERRORS(s)		(s) = (((s)&(~T_HAS_ERRORS_MASK))|(T_HAS_ERRORS))
#define T_SET_DIR_INDEX(s)		(s) = (((s)&(~T_DIR_INDEX_MASK))|(T_DIR_INDEX))
#define T_SET_DELETE_WITH_CLONE(s)	(s) = (((s)&(~T_DELETE_WITH_CLONE_MASK))|(T_DELETE_WITH_CLONE))
#define T_SET_CLONE_ORIG_PMCELL(s)	(s) = (((s)&(~T_CLONE_ORIG_PMCELL_MASK))|(T_CLONE_ORIG_PMCELL))


/*
 * Check Tag Bit Status
 */
#define T_IS_FOUND_PMCELL(s)		(((s)&(T_FOUND_PMCELL_MASK)) == (T_FOUND_PMCELL))
#define T_IS_FOUND_DIR(s)		(((s)&(T_FOUND_DIR_MASK)) == (T_FOUND_DIR))
#define T_IS_HAS_ERRORS(s)		(((s)&(T_HAS_ERRORS_MASK)) == (T_HAS_ERRORS))
#define T_IS_DIR_INDEX(s)		(((s)&(T_DIR_INDEX_MASK)) == (T_DIR_INDEX))
#define T_IS_DELETE_WITH_CLONE(s)	(((s)&(T_DELETE_WITH_CLONE_MASK)) == (T_DELETE_WITH_CLONE))
#define T_IS_CLONE_ORIG_PMCELL(s)	(((s)&(T_CLONE_ORIG_PMCELL_MASK)) == (T_CLONE_ORIG_PMCELL))


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
 * Frag Slot Free List Status
 */
#define FG_USED      0x0001
#define FG_FREE      0x0002
#define FG_ONFRLIST  0x0004
#define FG_FREE_MASK 0x000F

/*
 * Set Frag Slot Status
 */
#define FG_SET_USED(s)        (s) = (((s)&(~FG_FREE_MASK))|(FG_USED))
#define FG_SET_FREE(s)        (s) = (((s)&(~FG_FREE_MASK))|(FG_FREE))
#define FG_SET_ONFRLIST(s)    (s) = (((s)&(~FG_FREE_MASK))|(FG_ONFRLIST))

/*
 * Check Frag Slot Status
 */
#define FG_IS_USED(s)         (((s)&(FG_FREE_MASK)) == (FG_USED))
#define FG_IS_FREE(s)         (((s)&(FG_FREE_MASK)) == (FG_FREE))
#define FG_IS_ONFRLIST(s)     (((s)&(FG_FREE_MASK)) == (FG_ONFRLIST)) 

/*
 * Indices into the frag slot array
 */
#define FRAG_USED 0
#define FRAG_UNUSABLE 1
#define FRAG_OVERLAP 2

/*
 * byte count macros to deal with property lists
 */
#define PL_ALIGN(x1)		(((x1) + 7) &~ 7)
#define PL_ENTRY_SIZE(x1, x2)	(PL_ALIGN(x1)+PL_ALIGN(x2))

/* load x1 from x2; x1 must be long-word aligned; return x1 */
#define FLAGS_READ(x1,x2)                               \
  (                                                     \
   ((uint32T *) &(x1))[0] = ((u_int *)&(x2))[0],        \
   ((uint32T *) &(x1))[1] = ((u_int *)&(x2))[1],        \
   (x1)                                                 \
   )


/*
 * Global data
 */
char *Prog;
FILE *Localtty;
struct options	  Options;

/*
 * Macros
 */
#define ERRMSG( eno ) sys_errlist[( eno )]


/*
 * fixfdmn structures
 */

/* optionT */
typedef struct options {
    int		clearClones;	/* Delete clones? */
    char	*type;		/* Which subsections should be fixed. */
    int         typeStatus;
    int		verboseOutput;
    int		activate;
    int		nochange;
    int		answer;
    int		undo;
    char	*undoDir;
    FILE	*logHndl;	/* FILE handle of the fixfdmn log file */
    char	*logFile;	/* Full pathname of the fixfdmn log file */
    char        *filesetName;
} optionsT;


/* mcellT - Section 2.1.14 */
typedef struct mcell {
    int	  vol;		/* volume index		       */
    int	  page;		/* BMT page number	       */ 
    int	  cell;		/* Cell number in BMT Page     */
} mcellT;


/* domainT - Section 2.1.1 */
typedef struct domain {
    char		*dmnName;	/* domain name */
    bfDomainIdT		dmnId;		/* domain Id */
    int			dmnVersion;	/* on disk structure version number */
    struct fileset	*filesets;	/* information on filesets in domain */
    int			numFilesets;	/* number of filesets in domain */
    struct volume	*volumes;	/* array of volume information */
    int			numVols;	/* number of active volumes */
    int			highVolNum;	/* highest volume number */
    struct list		*mcellList;	/* list of mcells */
    int			logVol;		/* volume containing the log */
    int			rootTagVol;	/* volume containing the root tag */
    struct pagetoLBN	*rootTagLBN;	/* root tag file page to LBN array */
    int			rootTagLBNSize;	/* number of entries in rootTagLBN */
    int			errorCount;	/* number of errors found in domain */
    struct fsQuota      *filesetQuota;  /* linked list of fileset quotas */
    struct ddlCloneFile *ddlClone;	/* linked list of ddl clone file */
    int			status;		/* status */
    int			countOverlaps;	/* TRUE if overlaps are being counted. */
} domainT;


/* filesetT - Section 2.1.2 */
typedef struct fileset {
    domainT		*domain;	  /* Pointer to the parent domain */
    bfSetIdT		filesetId;	  /* fileset Id */
    char		*fsName;	  /* fileset name */
    struct mcell	fsPmcell;	  /* fileset tag file primary mcell */
    struct pagetoLBN	*fragLBN;	  /* frag file page to LBN array */
    int			fragLBNSize;	  /* number of entries in fragLBN */
    struct pagetoLBN	*tagLBN;	  /* tag file page to LBN array */
    int			tagLBNSize;	  /* number of entries in tagLBN */
    struct list		*tagList;	  /* list of tags */
    struct fragOverlap	*fragOverlapHead; /* list of overlaping frag slots */
    unsigned long	(*fragSlotArray)[3];   /* frag slot array for this fileset */
    bfTagT              origCloneTag;     /* tag for original or clone fileset */
    struct fileset      *origClonePtr;    /* pointer to original or clone fileset */
    char                *groupUse;        /* pointer to frag group use array */
    int			status;		  /* status */
    int			errorCount;	  /* error count for fileset */
    struct fileset	*next;		  /* next fileset */
} filesetT;


/* volumeT - Section 2.1.3 */
typedef struct volume {
    int		       volFD;		/* volume file descriptor */
    char	       *volName;	/* volume device name */
    unsigned int       volSize;		/* size of volume in disk blocks */
    struct rbmtInfo    *volRbmt;	/* volume RBMT/BMT0 information */
    bfDomainIdT	       domainID;	/* domain ID on this volume */
    int		       dmnVersion;	/* domain version on this volume */
    struct timeval     mountId;		/* last mount time for this volume */
    int		       mntStatus;	/* mount status for this volume */
    int		       rootTag;		/* Is the root tag on this volume. */
    struct sbmInfo     *volSbmInfo;	/* pointer to Volume SBM structures. */
} volumeT;


/* rbmtInfoT - Section 2.1.4 */
typedef struct rbmtInfo {
    struct pagetoLBN	*sbmLBN;	/* SBM page to LBN array */
    int			sbmLBNSize;	/* number of entries in sbmLBN */
    struct pagetoLBN	*bmtLBN;	/* BMT page to LBN array */
    int			bmtLBNSize;	/* number of entries in bmtLBN */
    int			maxBmtPage;	/* Maximum number of pages in BMT */
    struct pagetoLBN	*logLBN;	/* log file page to LBN array */
    int			logLBNSize;	/* number of entries in logLBN */
    struct pagetoLBN	*rbmtLBN;	/* RBMT page to LBN array */
    int			rbmtLBNSize;	/* number of entries in rbmtLBN */
    int			maxRbmtPage;	/* Maximum number of pages in RBMT */
    struct pagetoLBN    *miscLBN;       /* misc file page to LBN array */
    int                 miscLBNSize;    /* number of entries in miscLBN */
} rbmtInfoT;


/* mcellNodeT - Section 2.1.5 */
typedef struct mcellNode {
    struct mcell	mcellId;	/* volume, BMT page, and mcell num */
					/* mcellId used for key into skiplist*/
    int			status;		/* status bits */
    bfTagT		tagSeq;		/* tag and sequence number */
    bfTagT		setTag;		/* fileset tag and sequence number */
} mcellNodeT;


/* tagNodeT - Section 2.1.6 */
typedef struct tagNode {
    bfTagT		tagSeq;		/* tag and sequence number */ 
					/* tagSeq used for key into skiplist */
    int			status;		/* status bits */
    long		size;		/* size in bytes from FS_STAT record */
    int			fileType;	/* type of file from FS_STAT record */
    struct mcell	tagPmcell;	/* primary mcell for tag */
    int			pagesFound;	/* number of pages found in extents */
    int			fragSize;	/* size of frag from FS_STAT record */
    int			fragPage;	/* frag page offset from FS_STAT rec */
    int			fragSlot;	/* frag slot in frag file from FS_STAT rec*/
    int			numLinks;	/* number of links from FS_STAT rec */
    int			numLinksFound;	/* number of links found in metadata */
    bfTagT		parentTagNum;	/* tag number of this tag's parent */
    bfTagT		dirIndexTagNum;	/* tag number of this tag's dir index*/
} tagNodeT;


/* messageT - Section 2.1.7 */
typedef struct message {
    locationT		diskArea;	/* Which ODS has the message */
    char		*message;	/* error message text */
    struct message	*next;		/* next message in list */
} messageT;


/* overlapNodeT - Section 2.1.8 */
/* 
 * The extentId structure holds the "address" of an extent
 * record.  This structure is used by the SBM routines.  It
 * is also embedded within the overlapNode structure.
 */
typedef struct extentId {
    struct mcell	cellAddr;
    int			extentNumber;	/* index into extent array */
} extentIdT;

/*
 * The overlapNode structure is a modification of the previously
 * defined extOverlapT (see fixfdmn spec section 2.1.8)
 * This structure was created because we cannot use the skip
 * lists as they are currently implemented, so the overlapping
 * extent information will be stored in a simple linked list.
 *
 * There are two pointers to these structures embedded with
 * this structure.  One of for ordering the nodes by the
 * file/extent information.  The second is for ordering the
 * nodes by LBNs.
 */
typedef struct overlapNode {
    struct overlapNode *nextByLbn;
    struct overlapNode *nextByMcell;
    struct extentId    xtntId;		 /* The addr of the defining extent. */
    int		       tagnum;		 /* Tag num of the overlapping file. */
    int		       setnum;		 /* Fileset tag number. */
    pageNumberT	       extentPageCount;  /* The num of pages in this extent. */
    pageNumberT	       overlapPageCount; /* Size of the overlapped area. */
    LBNT	       extentStartLbn;	 /* LBN of the start of the extent. */
    pageNumberT	       overlapStartPage; /* Page num where overlap begins. */
} overlapNodeT;


/* 
 * The fileNode structure is to be used to maintain a list
 * of files that will be deleted from the domain.  This is
 * internal to the SBM routines.
 */
typedef struct fileNode {
    int			tagnum;	/* tag number of the file to be deleted */
    int			setnum;	/* fileset number */
    struct fileNode	*next;
} fileNodeT;


/* fragOverlapT - Section 2.1.9 */
typedef struct fragOverlap {
    LBNT        startSlot;	/* LBN for start of duplicated storage area */
    int         fragSize;	/* size in bytes of the overlapped area */
    bfTagT	tagSeq;		/* tag and seq of the file with the overlap */
    bfTagT	setTag;		/* fileset tag and sequence number */
    struct fragOverlap *nextFrag;  /* next frag in the list */
} fragOverlapT;

/* Storage Arrays - Section 2.1.10 */

/* storageMapT */
typedef struct storageMap {
    int		setTag;
    int		fileTag;
} storageMapT;

/* sbmInfoT */
typedef struct sbmInfo {
    uint64		*sbm;
    uint64		*overlapBitMap;
    storageMapT		*storageMap;		/* V5 "SBM" support */
    struct overlapNode	*overlapListByFile;
    struct overlapNode	*overlapListByLbn;
    pageNumberT		storageMapSize;
} sbmInfoT;


/* Cache_Structure - Section 2.1.11 */

typedef char pageBufT[PAGESIZE];

/* pageT - Section 2.1.11.1 */
typedef struct page {
    pageBufT		*pageBuf;	/* pointer to page data */
    struct page		*nextMRU;	/* next most recently used page */
    struct page		*nextLRU;	/* next least recently used page */
    struct message	*messages;	/* list of messages */
    pthread_mutex_t	*lock;		/* page lock */
    int			status;		/* status bits */
    LBNT		pageLBN;	/* start LBN of page (used for key) */
    vdIndexT		vol;		/* volume number (used for key) */
} pageT;


/* cacheT - Section 2.1.11.2 */
typedef struct cache {
    struct list	*readCache;	  /* list of read-only cache entries */
    struct list	*writeCache;	  /* list of modified pages */
    struct page	*MRU;		  /* most recently used page in read cache. */
    struct page	*LRU;		  /* least recent used page in read cache. */
    long	pagesLocked;	  /* number of locked pages in read cache. */
    long	writeHits;	  /* number of hits on the write cache */
    long	readHits;	  /* number of hits on the read cache */
    long	misses;		  /* number of cache misses */
    long	maxSize;	  /* maximum size of the readCache */
    domainT	*domain;	  /* pointer to the domain */
    pthread_mutex_t *lock;	  /* cache lock */
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
    tis_rwlock_t	*lock;	/* read-write lock for exclusive operations */
} listT;


/* pagetoLBNT - Section 2.1.13 */
typedef struct pagetoLBN {
    int	 	volume;		/* Volume Page is on   */
    int	 	page;		/* Page to map from    */
    LBNT 	lbn;		/* LBN to map to       */
    int  	isOrig;		/* Flag - set to true if fileset is clone and extent */
			/* is from the original fileset. */
} pagetoLBNT;


/* logVol - Section 2.1.15 */
typedef struct logVol {
    int		  volume;
    struct logVol *next;
} logVolT;


/*
 * structure to store quota info
 */
typedef struct fsQuota {
    bfTagT  filesetTag;  /* fileset tag and sequence number */    
    long    hardBlockLimit;
    long    softBlockLimit;
    long    hardFileLimit;
    long    softFileLimit;
    time_t  blockTimeLimit;
    time_t  fileTimeLimit; 
    uint32T quotaStatus;
    struct fsQuota *next;	
} fsQuotaT;

/*
 * Need to save deferred delete list records for clone files.
 */
typedef struct ddlCloneFile {
    bfTagT filesetId;  /* fileset tag and sequence number */
    bfTagT   cloneTag;
    struct   ddlCloneFile *next;
} ddlCloneFileT;

/*
 * This structure is used to help cut down on the number of
 * variables passed in to correct_bmt_primary_chain.
 */
typedef struct chainFound {
    int      foundFsData;
    int      foundFsStat;
    int      foundBsrXtnts;
    int      foundPropList;
    int      foundInherit;
    int      isStriped;
    int      mcellCnt;
    int      pageCnt;
    int      stripeCnt;
} chainFoundT;

/*====================================
 * function prototypes
 *===================================*/

/*
 * fixfdmn.c
 */

void usage(void);

void prepare_signal_handlers(void);

void signal_handler(int sigNum);

int want_abort(void);

/*
 * fixfdmn_utilities.c
 */

void writemsg(int  severity, 
	      char *fmt, 
	      ...);

void writelog(int  severity, 
	      char *fmt, 
	      ...);

void writelog_real(int     severity, 
		   char    *fmt, 
		   va_list args);

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

int get_domain_volumes(char    *domainName, 
		       domainT *domain);

int open_domain_volumes(domainT *domain);

int get_volume_number(domainT *domain, 
		      int     index,
		      int     *volNumber, 
		      int     *volSize);

int load_subsystem_extents(domainT *domain);

int collect_extents(domainT    *domain,
		    filesetT   *fileset,
		    mcellT     *pPrimMcell, 
		    int	       addToSbm, 
		    pagetoLBNT **extents, 
		    int	       *arraySize,
		    int	       isRBMT);

int merge_clone_extents(pagetoLBNT *origXtntArray,
			int        origXtntArraySize,
			pagetoLBNT *cloneXtntArray,
			int        cloneXtntArraySize,
			pagetoLBNT **newXtntArray,
			int        *newXtntArraySize);

int save_fileset_info(domainT *domain,
		      pageT   *pPage);

void corrupt_exit(char *message);

void clean_exit(domainT *domain);

void failed_exit(void);

int create_undo_files(domainT *domain);

int write_cache_to_disk(void);

int close_domain_volumes(domainT *domain);

int activate_domain(domainT *domain);

int remove_clone_from_domain(char *domainName, 
			     char *cloneName);

int restore_undo_files(domainT *domain);

int convert_page_to_lbn(pagetoLBNT *extentArray,
			int	   extentArraySize,
			int	   page,
			LBNT	   *lbn,
			int	   *vol);

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
			 int     volNum, 
			 int     collectSbm);

/*
 * fixfdmn_metadata.c
 */

int correct_magic_number(domainT *domain,
			 int	 volNum);

int validate_tag_file_page(pageT *tagFilePage,
			   int   pageNum);

int validate_log_file_page(domainT *domain,
			   pageT   *logFilePage);

int clear_log(domainT *domain);

int zero_log(domainT *domain);

int validate_bmt_page(domainT *domain,
		      pageT   *pPage,
		      int     pageId,
		      int     isRbmt);

int validate_bmt_page_header(domainT *domain,
			     pageT   *pPage, 
			     int     pageId, 
			     int     *numErrors, 
			     int     rbmtFlag);

int validate_mcell_header(domainT *domain,
			  pageT	  *pPage,
			  int     pageId,
			  int	  mcellId,
			  int	  rbmtFlag,
			  int	  *numErrors);

int reset_free_mcell_links(pageT      *pPage, 
			   mcellUsage *freeMcells);

int correct_free_mcell_list(volumeT *volume, 
			    int	    *bmtStatusArray);

int clear_deferred_delete_list(domainT *domain);

int correct_clone_files_on_ddl(domainT *domain);

int correct_tag_file(domainT *domain, 
		     int     filesetNum);

int correct_tag_page_header(filesetT *fileset,
			    pageT    *pPage,
			    int	     pageNumber,
			    int	     tagUninitPg);

int correct_tag_page_records(pageT    *pPage,
			     domainT  *domain,
			     filesetT *fileset,
			     int      pageNumber,
			     int      tagUninitPg);

int correct_tag_to_mcell(filesetT *fileset,
			 int      tagNum,
			 int      seqNum,
                         int      volNum,
			 int      pageNum,
			 int      mcellNum);

int correct_nodes(domainT *domain);

int correct_orphans(domainT *domain);

int correct_bmt_chains(filesetT *fileset);

int correct_chain_mcell_count(filesetT *fileset, 
			      mcellT   chainMcell, 
			      mcellT   xtntMcell, 
			      int      numMcells, 
			      int      isStriped);

int correct_chain_frag(filesetT *fileset, 
		       tagNodeT *pTagNode, 
		       mcellT   chainMcell, 
		       int      lastExtentPage,
		       int      foundFsData,
		       int      noXtnts);

int correct_empty_chain(filesetT *fileset, 
			tagNodeT *pTagNode);

int correct_quota_files(filesetT *fileset);

int correct_fsstat(filesetT *fileset, 
		   tagNodeT *tagNode);

int validate_fileset_name(char *fsName);

int delete_tag(filesetT *fileset,
	       int	tagNum);

int correct_chain_fsstat(filesetT *fileset, 
			 mcellT	  primaryMcell, 
			 int	  tagNum);

int correct_chain_proplist(filesetT *fileset, 
			   mcellT   primaryMcell, 
			   int	    tagNum);

int correct_chain_extents(filesetT   *fileset, 
			  mcellT     primaryMcell, 
			  pagetoLBNT *xtntArray);

int correct_chain_tag_and_set(filesetT *fileset, 
			      mcellT   currMcell,
			      mcellT   prevMcell, 
			      int      primTagNum, 
			      int      primSeqNum, 
			      int      nextTagNum,
			      int      firstXtnt,
			      pageT    *pPage);

int append_page_to_file(filesetT *fileset,
			tagNodeT *tag,
			int      *foundPage);

int free_orphan(domainT *domain, 
		int     volNum, 
		int     pageNum, 
		int     mcellNum);

int correct_chain_inherit_attr(filesetT *fileset,
			       mcellT   primaryMcell, 
			       int      tagNum);

int correct_bmt_chain_by_tag(filesetT *fileset, 
			     tagNodeT *pTagNode);

int copy_clone_tag_data(filesetT *fileset, 
			tagNodeT *pTagNode);

int correct_bmt_primary_chain(filesetT *fileset, 
			      tagNodeT *pTagNode,
			      mcellT   *chain,
			      mcellT   *xtnt,
			      chainFoundT *found);

int correct_bmt_extent_chain(filesetT *fileset, 
			     tagNodeT *pTagNode,
			     mcellT   xtntMcell,
			     mcellT   *chainMcell,
			     chainFoundT *found);

int correct_mcell_extent_boundary(domainT *domain,
				  pageT   *pPage,
				  mcellT  currMcell,	
				  mcellT  *nextMcell);

int correct_tag_and_set(filesetT *fileset,
			tagNodeT *pTagNode);

int correct_tag_chains(filesetT *fileset,
		       tagNodeT *pTagNode);

int correct_dir_extents(filesetT *fileset, 
			tagNodeT *tag,
			int numXtnts,
			pagetoLBNT *xtntArray);
/*
 * fixfdmn_frag.c
 */
int set_frag_file_arrays(filesetT *fileset, 
                         domainT  *domain);

int collect_frag_info(filesetT *fileset, 
                      int size,
                      int slot, 
                      bfTagT tag, 
                      bfTagT settag);
                       
int correct_frag_overlaps(filesetT *fileset, 
		          domainT  *domain);

int correct_frag_file(filesetT *fileset, 
		      domainT  *domain);

void set_unusable_frag_slots(filesetT *fileset, 
                             int slot,
                             int size,
                             int setUsed);
                             
void set_frag_unused(filesetT *fileset,
                      int slot,
                      int size);

/*
 * fixfdmn_rbmt.c
 */
int correct_domain_rbmt(domainT *domain);

int correct_rbmt_pages(domainT *domain);

int correct_rbmt_bmt_mcell(pageT *pPage, 
			   int   isRBMT, 
			   int   volNum);

int correct_rbmt_sbm_mcell(pageT   *pPage, 
			   domainT *domain,
			   int	   volNum);

int correct_rbmt_root_tag_mcell(pageT   *pPage,
				domainT *domain,
				int     volNum, 
				int     *rootTagVolume);

int correct_rbmt_log_mcell(pageT   *pPage, 
			   int	   volNum, 
			   domainT *domain,
			   logVolT **logVolume);

int correct_rbmt_primary_bmt(pageT *pPage, 
			     int   volNum);

int correct_rbmt_vol_dom_mcell(pageT   *pPage, 
			       int     isRBMT, 
			       int     volNum, 
			       domainT *domain,
			       int     *mattrVol, 
			       int     *mattrSeq,
			       int     *bfSetDirVol);

int correct_rbmt_misc_mcell(pageT   *pPage, 
			    int	    volNum,
			    int     pageNumber,
			    domainT *domain);

int correct_rbmt_other_mcell(pageT   *pPage, 
			     int     mcellNum,
			     int     volNum, 
			     domainT *domain);

int correct_smart_store_record(pageT *pPage,
			       bsMRT *pRecord,
			       int   mcellNum);

int correct_freeze_record(pageT *pPage,
			  bsMRT *pRecord,
			  int   mcellNum);

int correct_rbmt_bsr_attr_record (pageT *pPage, 
				  bsMRT *pRecord, 
				  int   mcellNum, 
				  int   dataSafety,
				  int   reqServices);

int correct_bsr_xtnts_default_values (pageT *pPage,
				      bsMRT *pRecord, 
				      int   mcellNum, 
				      int   byteCount);
int find_sbm_first_page ( bsXtntRT pXtnt, LBNT sbmFirstBlk, int volNum,int odsVersion); 

/*
 * fixfdmn_bmt.c
 */
int correct_bmt_mcell_data (domainT *domain);

int correct_bmt_page_header(int	    pageNumber,
			    int	    volNum,
			    int	    inExtent,
			    LBNT    nextLBN,
			    pageT   *pPage,
			    domainT *domain,
			    int     rbmtFlag);

int correct_mcell_header(pageT	 *pPage,
			 int	 volNum,
			 int	 pageNum,
			 int	 mcellId,
			 domainT *domain);

int correct_mcell_records(domainT *domain,
			  pageT	  *pPage, 
			  int	  volNum,
			  int	  pageNum,
			  int	  mcellNum,
			  int     *isPrimary,
			  int     *isFsstat);

int match_record_byte_count(bsMRT *pRecord, 
			    int	  pageNum, 
			    int	  mcellNum,
			    int	  rbmtFlag,
			    int	  *recordType);

int correct_bsr_xtnts_record(pageT *pPage,
			     int   mcellNum, 
			     bsMRT *pRecord);

int correct_bsr_attr_record(pageT *pPage, 
			    int	  mcellNum, 
			    bsMRT *pRecord,
			    int	  RBMTflag);

int correct_bsr_xtra_xtnts_record(domainT *domain,
				  int	  volNum,
				  pageT	  *pPage, 
				  int	  mcellNum, 
				  bsMRT	  *pRecord);

int correct_bsr_shadow_xtnts_record(domainT *domain,
				    int	    volNum,
				    pageT   *pPage, 
				    int	    mcellNum, 
				    bsMRT   *pRecord);

int correct_bsr_mcell_free_list_record(domainT *domain,
				       pageT   *pPage, 
				       int     mcellNum, 
				       bsMRT   *pRecord);

int correct_bsr_bfs_attr_record(domainT *domain,
				pageT	*pPage, 
				int	mcellNum, 
				bsMRT	*pRecord);

int correct_bsr_def_del_mcell_list_record(domainT *domain,
					  pageT	  *pPage, 
					  int	  mcellNum, 
					  bsMRT	  *pRecord);

int correct_bsr_bf_inherit_attr_record(pageT *pPage, 
				       int   mcellNum, 
				       bsMRT *pRecord,
				       int   RBMTflag);

int correct_bsr_rsvd17_record(pageT *pPage, 
			      int   mcellNum, 
			      bsMRT *pRecord);

int correct_bsr_bfs_quota_attr_record(domainT *domain,
				      pageT   *pPage, 
				      int     mcellNum, 
				      bsMRT   *pRecord);

int correct_bsr_proplist_head_record(domainT *domain,
				     pageT   *pPage, 
				     int     mcellNum, 
				     bsMRT   *pRecord);

int correct_bsr_proplist_data_record(pageT *pPage, 
				     int   mcellNum, 
				     bsMRT *pRecord);

int correct_bmtr_fs_dir_index_file(pageT *pPage, 
				   int	 mcellNum, 
				   bsMRT *pRecord);

int correct_bmtr_fs_index_file_record(pageT *pPage, 
				      int   mcellNum, 
				      bsMRT *pRecord);

int correct_bmtr_fs_time_record(domainT *domain,
				pageT   *pPage, 
				int     mcellNum, 
				bsMRT   *pRecord,
				int     tag);

int correct_bmtr_fs_undel_dir_record(pageT *pPage, 
				     int   mcellNum, 
				     bsMRT *pRecord);

int correct_bmtr_fs_data_record(pageT *pPage, 
				int   mcellNum, 
				bsMRT *pRecord);

int correct_bmtr_fs_stat_record(pageT *pPage, 
				int   mcellNum, 
				bsMRT *pRecord);

int free_mcell(domainT *domain,
	       int     volNum,
	       pageT   *pPage, 
	       int     mcellNum);

int delete_mcell_record(domainT *domain,
			pageT   *pPage,
			int     mcellNum,
			bsMRT   *pRecord);

int clear_mcell_record(domainT *domain,
		       pageT   *pPage, 
		       int     mcellNum,
		       int     recNum);

int correct_mcell_record_bcnt(domainT *domain, 
			      pageT   *pPage, 
			      int     mcellNum,
			      bsMRT   *pRecord, 
			      int     currentSize);

/*
 * fixfdmn_dir.c
 */

int correct_all_dirs(filesetT *fileset);

int correct_dirs_recurse(filesetT *fileset,
			 tagNodeT *tag,
			 char     *path);

int correct_dir_index(filesetT *fileset,
		      tagNodeT *dirTag);

int validate_dir_index_page(idxNodeT   *page,
			    int        tree,
			    int        pageNum,
			    pagetoLBNT *idxXtnt,
			    int        idxXtntSize);

int validate_dir_index_leaf(idxNodeT   *page,
			    int        pageNum,
			    pagetoLBNT *dirXtnt,
			    int        dirXtntSize);

int validate_dir_index_free_space(idxNodeT   *page,
				  long       *currFreeOffset,
				  int        pageNum,
				  pagetoLBNT *dirXtnt,
				  int        dirXtntSize);

int clear_tag_index(filesetT *fileset,
		    tagNodeT *tagNode);

int move_lost_file(filesetT *fileset,
		   tagNodeT *tag);


/*
 * fixfdmn_sbm.c
 */

int add_tag_to_delete_list(int	     tag,
			   int	     settag,
			   fileNodeT **deleteFileListHead);

int collect_sbm_info(domainT    *domain,
		     pagetoLBNT *extentArray,
		     int        numRecords,
		     int        tag, 
		     int        settag,
		     mcellT     *xtntMcell,
		     int	xtntNumOffset);

int correct_sbm(domainT *domain);

int correct_sbm_overlaps(domainT *domain);

int find_overlap_node_by_lbn(overlapNodeT *listHead, 
			     overlapNodeT *extentNode,
			     overlapNodeT **foundNode);

int find_overlap_node_by_mcell(overlapNodeT *listHead, 
			       overlapNodeT *extentNode,
			       overlapNodeT **foundNode);

int init_sbm_structures (volumeT	*volume);

int insert_overlap_data_by_lbn(overlapNodeT **listHead,
			       overlapNodeT *node);

int insert_overlap_data_by_mcell(overlapNodeT **listHead, 
				 overlapNodeT *node);

int is_page_free(domainT *domain,
		 int     volNum,
		 LBNT     pageLBN);

int load_sbm(domainT *domain);

int remove_extent_from_sbm(domainT *domain,
			   int	   volNum,
			   LBNT	   startLbn, 
			   int	   extentPageCount,
			   int	   tag, 
			   int	   settag,
			   mcellT  *xtntMcell,
			   int	   extentNumber);

int remove_overlap_node_from_lists(overlapNodeT *overlapNode,
				   overlapNodeT **fileListHead,
				   overlapNodeT **lbnListHead);

int update_sbm_mcell_tag_and_set_info(domainT *domain,
                                      mcellT  *currMcell,
                                      int     oldTag,
                                      int     oldSetTag,
                                      int     newTag,
                                      int     newSetTag,
				      pageT   *pPage);

int validate_sbm_page(pageT *sbmPage,
		      int   odsVersion,
		      int   pgNum);

/*
 * fixfdmn_cache.c
 */

int create_cache(domainT *domain);

int compare_pages(void *a, 
		  void *b);

int read_page_from_cache(int   volume,
			 LBNT  lbn,
			 pageT **page);

int release_page_to_cache(int   volume,
			  LBNT  lbn,
			  pageT *page,
			  int   modified,
			  int   keepPageForever);

/*
 * fixfdmn_list.c
 */
int compare_tags(void *a, 
		 void *b);

int compare_mcells(void *a, 
		   void *b);

int create_list (int   (*compare)(void *a, void *b),
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

/*
 * fixfdmn_lock.c
 */
int ffd_rwlock_create(tis_rwlock_t **lock);

int ffd_rwlock_destroy(tis_rwlock_t *lock);

int ffd_mutex_create(pthread_mutex_t **lock);

int ffd_mutex_destroy(pthread_mutex_t *lock);

#endif /* not FIXFDMN_H */

/* end fixfdmn.h */
