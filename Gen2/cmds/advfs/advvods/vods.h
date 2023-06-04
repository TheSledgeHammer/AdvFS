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

#include <advfs/bs_service_classes.h>
#include <advfs/bs_public.h>
#include <advfs/bs_ims.h>

#include <advfs/bs_tagdir.h>

extern bfTagT staticRootTagDirTag; 

/* this macro calculates the bytes in a log record header using the
 * system defined number of words in that header */

#define REC_HDR_LEN (REC_HDR_WORDS * sizeof(uint32_t))

/* This macro is similar to BS_BFTAG_RSVD_INIT() but evaluates to only the
 * tag.tag_num part of a normal tag.
 */
#define RSVD_TAGNUM_GEN(vdi, bmtidx)    \
       ((uint64_t) -((((int64_t)vdi) * BFM_RSVD_TAGS) + (bmtidx)))

#define DOUBLE_LINE \
"==========================================================================\n"
#define SINGLE_LINE \
"--------------------------------------------------------------------------\n"
#define CLEAR_LINE \
"\r                                                                          \r"
/* useful FOB definitions */

#define FOB2PAGE(fobs) \
(((fobs) % ADVFS_METADATA_PGSZ_IN_FOBS) ? \
 ((fobs) / ADVFS_METADATA_PGSZ_IN_FOBS) + 1 : \
 ((fobs) / ADVFS_METADATA_PGSZ_IN_FOBS))

#define PAGE2FOB(pages) ((pages) * ADVFS_METADATA_PGSZ_IN_FOBS)
#define ADVFS_METADATA_PGSZ_IN_BLKS (ADVFS_METADATA_PGSZ / DEV_BSIZE)

/* command line flag definitions */

#define VFLG  0x01	/* "-v" print verbose */
#define RFLG  0x02	/* "-r" open raw volumes */
#define TTFLG  0x04     /* "-T" print terse log info */
#define NFLG  0x010     /* "-n" print free mcells */
#define CFLG  0x020     /* "-c" print a chain of records */
#define CCFLG 0x040     /* "-C" check SBM page xor values */

/* internal flags */

#define PSET     0x400     /* for vtagpg */
#define RSVDBF   0x800     /* for print_mcells */
#define ONECELL  0x1000    /* for nvbmtpg */
#define RBMT     0x8000    /* for nvbmtpg */
#define DDL      0x10000   /* nvbmtpg to print_mcells */

/* routine return values */
#define OK             0   	/* good return */
#define BAD_SYNTAX     1	/* garbled command syntax */
#define BAD_PARAM      2	/* out of bounds input parameter value */
#define BAD_DISK_VALUE 3	/* incoherent data read from disk */
#define NOT_FOUND      4
#define OUT_OF_RANGE   6

typedef struct {
    int vol;
    bf_fob_t fob;        /* Starting FOB */
    bf_vd_blk_t blk;     /* Starting DEV_BSIZE disk block */
} extentT;

typedef struct {
    int cnt;
    extentT *xtnt;
} extentMapT;

typedef struct dmn DmnT;

#define MAXTAG 0x7fffffffffffffff 

#define LBN_BLK_TERM        ((uint64_t)-1)

/* TODO. make xmap into pointer to facilitate sharing */
typedef struct bitFile {
    struct bitFile *next;	/* for tag2name */
    int             type;
    int             fd;		/* if type FLE */
    char            fileName[MAXPATHLEN];  /* if type FLE */
    char            setName[BS_SET_NAME_SZ];
    bs_meta_page_t  pages;	/* pages in the file */
    DmnT           *dmn;        /* has the BMT xtnts for each vol */
    bfMCIdT         pmcell;     /* prim mcell */
    bfTagT          fileTag;
    bfTagT          setTag;
    struct bitFile *setp;       /* non zero means a set was specified */
    struct bitFile *origSetp;   /* non zero means this is a clone */
    vdIndexT        pgVol;	/* vol of current page */
    uint64_t        pgLbn;	/* block number of current page */
    bf_fob_t        bfPgSz;     /* from BSR_ATTR - actually alloc size */ 
    bs_meta_page_t  pgNum;	/* page number of current page */
    char           *pgBuf;	/* current page */
    extentMapT      xmap;
} bitFileT;

/* bitFileT type */
#define FLE  0x01
#define DMN  0x02
#define VOL  0x04

#define PAGE(f) ((f)->pgBuf)

typedef struct {
    int      fd;
    uint64_t blocks;
    char     volName[MAXPATHLEN];
    bitFileT *rbmt;
    bitFileT *bmt;
} volT;

struct dmn {
    adv_ondisk_version_t dmnVers;
    char dmnName[BS_DOMAIN_NAME_SZ];
    volT *vols[BS_MAX_VDI];
};
  
/* from mss_entry.h, used here (bsTerXtntT) */
typedef uint32_t mssShelfIdT;

/* from mss_entry.h, used here (bsTerXtntT) */
typedef struct {
    uint32_t low;
    int32_t high;
} mssOnDiskShelfRefNumT;

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
#define BSR_TERTIARY_XTNTS   BSR_MAX+1     /* bsTerXtntRT structure type */

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
typedef struct bsTerXtnt {
    bs_meta_page_t bsPage; /* Bitfile page number */
    mssShelfIdT mediaId;   /* tertiary storage media Id */
    int32_t flags;         /* extent is SS_DIRTY and/or SS_RESIDENT */
    uint32_t accessTime;   /* last time extent was read or written */
    mssOnDiskShelfRefNumT mediaRefNum;  /* tertiary stg media reference number*/
} bsTerXtntT;

/* from bs_ods.h, used by print_mcells.c & here (bsTerXtntRT) */
#define BMT_TERTIARY_XTNTS \
    ((BSC_R_SZ - 2 * sizeof(bsMRT) - 2 * sizeof(uint32_t)) / sizeof(bsTerXtntT))

/* from bs_ods.h, used by print_mcells.c  & here (BMT_TERTIARY_XTRA_XTNTS) */
typedef struct bsTerXtntR {
    uint32_t totalXtnts;         /* total number of bitfile extents */
    uint32_t xCnt;               /* Count of elements used in bsXA */
    bsTerXtntT bsXA[BMT_TERTIARY_XTNTS];/* Array of disk extent descriptors */
} bsTerXtntRT;

/* from bs_ods.h, used by print_mcells.c & print_log.c */
#define BSR_TERTIARY_XTRA_XTNTS BSR_MAX+2   /* bsTerXtraXtntRT structure type */

/* from bs_ods.h, used by print_mcells.c & print_log.c */
#define BMT_TERTIARY_XTRA_XTNTS \
    ((BSC_R_SZ - 2*sizeof( bsMRT ) - sizeof( uint32_t )) / sizeof( bsTerXtntT ))

/* public protoypes */

void bmt_main(int argc, char **argv);
void log_main(int argc, char **argv);
void sbm_main(int argc, char **argv);
void tag_main(int argc, char **argv);
void file_main(int argc, char **argv);

void init_bitfile(bitFileT*);
void newbf(bitFileT*, bitFileT*);
int resolve_src(int, char**, int*, char*, bitFileT*, int*);
int resolve_name(bitFileT*, char*, int);
void print_mcell_header (bitFileT *bfp, bsMPgT *pdata, int vflg);
void print_cell(bsMCT *bsMCp, uint16_t cn, int flags);
int dumpBitFile(bitFileT*, char*);
int find_BMT(bitFileT*, int);
int find_TAG(bitFileT*);
int find_rec(bitFileT*, int, char**);
int find_bmtr(bsMCT*, int, char**);
int get_max_tag_pages(bitFileT*, bs_meta_page_t*);
int get_set_n_file(bitFileT*, char**, int, int*);
int get_set(bitFileT*, char**, int, int*, bitFileT*);
int get_file(bitFileT*, char**, int, int*, bitFileT*);
int get_file_by_tag(bitFileT*, uint64_t, bitFileT*);
int get_chain_mcell(bitFileT*, bfMCIdT*);
int get_next_mcell(bitFileT*, bfMCIdT*);
int getnum(char*, int64_t*);
int load_xtnts(bitFileT*);
int read_page(bitFileT*, bs_meta_page_t);
int read_tag_page(bitFileT*, bs_meta_page_t, bsTDirPgT**);
int page_mapped(bitFileT*, int);
void print_header(bitFileT*);
void print_fob_header(bitFileT *bfp, bf_fob_t fobnum);
void print_cell_hdr(bsMCT *pdata, uint16_t cn, int vflg);
void print_unknown(char*, int);
void print_logtrlr( FILE *fp, logPgTrlrT *pdata, uint64_t lbn);
