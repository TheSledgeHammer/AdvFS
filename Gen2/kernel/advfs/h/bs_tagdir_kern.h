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
 *     AdvFS 
 *
 * Abstract:
 *
 *     Tag Directory function prototypes.
 *
 */

#ifndef _BS_TAGDIR_KERN_INCLUDED
#define _BS_TAGDIR_KERN_INCLUDED


#include <advfs/bs_tagdir.h>

#define   BS_TD_MAX_SEQ              0x7fffffff

extern bfTagT staticRootTagDirTag;

/* Common argument structure. */
typedef struct tagInfo {
    bfSetT *bfSetp;
    domainT *dmnP;
    bfTagT tag;
    bfMCIdT bfMCId;
    ftxHT ftxH;
} tagInfoT;

typedef enum {
    TDU_NO_FLAGS        = 0x0,  /* No flags */
    TDU_SET_FLAGS       = 0x1,  /* Set the tmFlags field by ORing in flags */
    TDU_UNSET_FLAGS     = 0x2,  /* Unset the passed in flags in tmFlags */
    TDU_SET_MCELL       = 0x4,  /* Set the mcellid */
    TDU_SKIP_FTX_UNDO   = 0x8   /* Skip ftx undo */
} tdu_flags_t;


/* Macros for tags. */
#define TAGTOPG(tag) ((tag)->tag_num / BS_TD_TAGS_PG)
#define TAGTOSLOT(tag) ((tag)->tag_num  % BS_TD_TAGS_PG)
#define MKTAGNUM(pg, slot) (((pg) * BS_TD_TAGS_PG) + (slot))

void
tagdir_remove_tag(
    bfSetT *bfSetp,      /* in - bitfile set from which to free tag */
    bfTagT *tp,          /* in - pointer to tag */
    ftxHT ftxH           /* in - transaction handle */
    );

void
tagdir_tag_to_freelist(
    bfSetT *bfSetp,      /* in - bitfile set from which to free tag */
    bfTagT *tp,          /* in - pointer to tag */
    ftxHT ftxH,          /* in - transaction handle */
    int set_tag_to_unused/* in - if TRUE, turn off in-use flag */
    );

statusT
tagdir_lookup(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in - pointer to tag to look up */
    bfMCIdT *bfMCId             /* out - primary mcell id */
    );

statusT
tagdir_lookup_full(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in - pointer to tag to look up */
    bfMCIdT *bfMCId,            /* out - primary mcell id */
    bfTagFlagsT *tagFlags       /* out - tmFlags from tagmap entry */ 
    );

statusT
tagdir_get_info(
    bfSetT *bfSetp,                         /* in - bitfile set desc ptr */
    bs_meta_page_t *firstUninitializedPage, /* out - next page to use */
    bs_meta_page_t *firstUnmappedPage,      /* out - first unmapped page */
    bs_meta_page_t *freeListHead,           /* out - start of free list */
    int *bfCnt                              /* out - num bitfiles in set */
    );

statusT
tagdir_lookup_next(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId             /* out - bitfile metadata cell */
    );

statusT
tagdir_alloc_tag(
    ftxHT parentFtxH,   /* in - transaction handle */
    mcellUIdT* mcellUIdp, /* in/out - ptr mcell uid (tag rtnd) */
    bfSetT* bfSetp,     /* in - ptr to bitfile set */
    bsTDirPgT **tdpgpp, /* out - pointer to tagdir page hdr */
    bsTMapT **tdmapp,   /* out - ptr to tagdir map entry */
    rbfPgRefHT *tdpgRefp, /* out - tagdir page ref */
    int checkmigtrunclock /*in */
    );

statusT
tagdir_insert_tag(
                  ftxHT parentFtxH,
                  mcellUIdT* mcellUIdp, /* in - mcelluid ptr */
                  bfSetT* bfSetp,       /* in - ptr to bitfile set */
                  bsTDirPgT *tdpgp,
                  bsTMapT *tdmap,
                  bfTagFlagsT tagFlags,
                  rbfPgRefHT tdpgRef
                  );


statusT
advfs_bs_tagdir_update_tagmap(
        bfTagT tag,             /* in - tag to update. Used to find tag entry */
        bfSetT *bf_set_p,       /* in - fileset for tag to update */
        bfTagFlagsT tag_flags,  /* in - tag flags to set/unset */
        bfMCIdT mcell_id,       
                    /* in - mcell_id to update in tag. This may not be valid. */
        ftxHT parent_ftx,       /* in - parent ftx */
        tdu_flags_t flags       /* in - type of op for update */
        );


statusT
tagdir_lookup2(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId             /* out - bitfile metadata cell */
    );

void
init_tagdir_opx(
    void
    );

statusT
bs_switch_root_tagdir (
                       domainT *dmnP,  /* in */
                       vdIndexT newVdIndex  /* in */
                       );




#endif /* _BS_TAGDIR_KERN_INCLUDED */
