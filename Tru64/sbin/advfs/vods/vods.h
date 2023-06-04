/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 *      View On-Disk Structures
 *
 * Author:
 *
 *      Alan Jones
 *
 * Date:
 *
 *      Thu Nov 17 09:06:33 PST 1994
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: vods.h,v $ $Revision: 1.1.6.2 $ (DEC) $Date: 2006/08/30 15:25:51 $
 */

#include <msfs/bs_public.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <sys/param.h>
#include <errno.h>

/* Generate TRUE if, according to the domain version, an RBMT is present */
#define RBMT_PRESENT ( dmnVersion >= FIRST_RBMT_VERSION )
#define RBMT_VERS(_dmnP) ( (_dmnP)->dmnVers >= FIRST_RBMT_VERSION )
#define USE_RBMT ( RBMT_PRESENT && bfp->fileTag < 0 )

/* This macro is similar to BS_BFTAG_RSVD_INIT() but evaluates to only the
 * tag.num part of a normal tag.
 */
#define RSVD_TAGNUM_GEN(vdi, bmtidx) -(((vdi) * BFM_RSVD_TAGS) + (bmtidx))

/* from bs_tagdir.h */
#define TAGTOPG(tag) ((tag)->num / BS_TD_TAGS_PG)
#define TAGTOSLOT(tag) ((tag)->num  % BS_TD_TAGS_PG)
#define MKTAGNUM(pg, slot) (((pg) * BS_TD_TAGS_PG) + (slot))

#define DOUBLE_LINE \
"==========================================================================\n"
#define SINGLE_LINE \
"--------------------------------------------------------------------------\n"
#define CLEAR_LINE \
"\r                                                                          \r"

#define PAGE_SIZE 8192
#define BLKS_PER_PG 16
/* BS_BLKSIZE = 512  from msfs/msfs/bs_public.h */

/* input flags */
#define VFLG  0x01	/* verbose */
#define RFLG  0x02	/* open raw domain volumes */
#define FFLG  0x04	/* follow ftxid, follow chain (-c), free frag */
#define BFLG  0x08	/* brief */

#define CFLG  0x10	/* interpret as uchar (and ascii) */
#define SFLG  0x20	/* interpret as shorts (16 bits) */
#define IFLG  0x40	/* interpret as ints (32 bits) */
#define LFLG  0x80	/* interpret as longs (64 bits) */
#define AFLG  0x100	/* interpret as ascii data */
#define DFLG  0x200	/* interpret as directory data */

/* internal flags */
#define PSET     0x400     /* for vtagpg */
#define RSVDBF   0x800     /* for print_mcells */
#define ONECELL  0x1000    /* for nvbmtpg */
#define MISCF    0x2000    /* for Misc file with 3 primary extents entries */
#define V4DMN    0x4000    /* for nvbmtpg */
#define RBMT     0x8000    /* for nvbmtpg */
#define V3DMN    0x10000   /* for print_mcells */
#define DDL      0x20000   /* nvbmtpg to print_mcells */
#define EDIT     0x40000   /* edit the data */
#define WRITE    0x80000   /* write data */
#define RECORD   0x100000  /* looking at a record vs a sub-structure */

/* routine return values */
#define OK         0		/* good return */
#define BAD_SYNTAX 1		/* garbled command syntax */
#define BAD_PARAM  2		/* out of bounds input parameter value */
#define BAD_DISK_VALUE 3	/* incoherent data read from disk */
#define OUT_OF_RANGE 6

typedef uint32T lbnT;

typedef struct {
    int vol;
    int page;
    lbnT blk;
} extentT;

typedef struct {
    int cnt;
    extentT *xtnt;
} extentMapT;

/* Structures for striped files. Used in vods.c */
typedef struct {
    int segXtntIdx;       /* index into segXMap[] */
    extentMapT segXMap;   /* array[number of extents in stripe] */
} segXtntT;

typedef struct {
    int segCnt;           /* number of stripes */
    int segSz;            /* width of stripe. Currently always 8 */
    segXtntT *segXtnt;    /* array[number of stripes] */
} segMapT;

typedef struct dmn DmnT;

/* BS_SET_NAME_SZ = 32         from msfs/msfs/bs_public.h */

#define MAXTAG 0x7fffffff
/* TODO. move pages to xmap. */
/* TODO. make xmap into pointer to facilitate sharing */
typedef struct bitFile {
    struct bitFile *next;	/* for tag2name */
    int        type;
    int        fd;		/* if type FLE */
    char       fileName[MAXPATHLEN];  /* if type FLE */
    char       setName[BS_SET_NAME_SZ];
    uint32T    pages;		/* pages in the file */
    DmnT       *dmn;		/* has the BMT xtnts for each vol */
    vdIndexT   pvol;		/* prim mcell vdi */
    uint32T    ppage;		/* prim mcell pg */
    uint32T    pcell;		/* prim mcell number */
    int        fileTag;
    int        setTag;
    struct bitFile *setp;       /* non zero means a set was specified */
    struct bitFile *origSetp;   /* non zero means this is a clone */
    vdIndexT   pgVol;		/* vol of current page */
    lbnT       pgLbn;		/* block number of current page */
    uint32T    pgNum;		/* page number of current page */
    char       *pgBuf;		/* current page */
    extentMapT xmap;
} bitFileT;

/* bitFileT type */
#define FLE  1
#define DMN  2
#define VOL  4
#define SAV  8

#define PAGE(f) ((f)->pgBuf)

/* BS_VD_NAME_SZ = 32            from msfs/msfs/bs_public.h */
typedef struct {
    int      fd;
    lbnT     blocks;
    char     volName[MAXPATHLEN];
    bitFileT *rbmt;
    bitFileT *bmt;
} volT;

/* BS_DOMAIN_NAME_SZ = 32         from msfs/msfs/bs_public.h */
struct dmn {
    uint32T dmnVers;
    char dmnName[BS_DOMAIN_NAME_SZ];
    volT *vols[BS_MAX_VDI];
};
  
/* prototypes */

void init_bitfile(bitFileT*);
void newbf(bitFileT*, bitFileT*);
int resolve_name(bitFileT*, char*, int);
int load_xtnts(bitFileT*);
int dumpBitFile(bitFileT*, char*);
int find_TAG(bitFileT*);
int get_set_n_file(bitFileT*, char**, int, int*);
int get_set(bitFileT*, char**, int, int*, bitFileT*);
int get_file(bitFileT*, char**, int, int*, bitFileT*);
int get_chain_mcell(bitFileT*, vdIndexT*, bfMCIdT*);
int get_next_mcell(bitFileT*, vdIndexT*, bfMCIdT*);
int find_rec(bitFileT*, int, char**);
int find_bmtr(bsMCT*, int, char**);
int getnum(char*, long*);
void print_header(bitFileT*);
void print_unknown(char*, int);
int page_mapped(bitFileT*, int);
int read_page(bitFileT*, int);


/* For printing out version 2 & 3 primary mcells */
/* used by print_mcells.c */
#define BSX_NW_BITS 64
typedef struct {
    int16T nwPgCnt;             /* page count of not writ range */
    uint16T fill1;              /* alignment fill */
    uint32T nwrPage;            /* start page of not writ range */
    uint32T notWrit[BSX_NW_BITS/32]; /* not writ bits */
} bfNWnFragT;

/* from mss_entry.h, used here (bsTerXtntT) */
typedef uint32T mssShelfIdT;

/* from mss_entry.h, used here (bsTerXtntT) */
typedef struct {
    uint32T low;
    int32T high;
} mssOnDiskShelfRefNumT;

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
#define BSR_TERTIARY_XTNTS 12        /* bsTerXtntRT structure type */

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
typedef struct bsTerXtnt {
    uint32T bsPage;        /* Bitfile page number */
    mssShelfIdT mediaId;   /* tertiary storage media Id */
    int32T flags;          /* extent is SS_DIRTY and/or SS_RESIDENT */
    uint32T accessTime;    /* last time extent was read or written */
    mssOnDiskShelfRefNumT mediaRefNum;  /* tertiary stg media reference number*/
} bsTerXtntT;

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
#define BMT_TERTIARY_XTNTS \
    ((BSC_R_SZ - 2 * sizeof(bsMRT) - 2 * sizeof(uint32T)) / sizeof(bsTerXtntT))

/* from bs_ods.h, used by print_mcells.c  & here (BMT_TERTIARY_XTRA_XTNTS) */
typedef struct bsTerXtntR {
    uint32T totalXtnts;         /* total number of bitfile extents */
    uint32T xCnt;               /* Count of elements used in bsXA */
    bsTerXtntT bsXA[BMT_TERTIARY_XTNTS];/* Array of disk extent descriptors */
} bsTerXtntRT;

/* from bs_ods.h, used by print_mcells.c & print_log.c */
#define BSR_TERTIARY_XTRA_XTNTS 13   /* bsTerXtraXtntRT structure type */

/* from bs_ods.h, used by print_mcells.c & print_log.c */
#define BMT_TERTIARY_XTRA_XTNTS \
    ((BSC_R_SZ - 2*sizeof( bsMRT ) - sizeof( uint32T )) / sizeof( bsTerXtntT ))

