/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
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
 * @(#)$RCSfile: bs_tagdir.h,v $ $Revision: 1.1.18.1 $ (DEC) $Date: 1999/10/01 14:13:07 $
 */

#ifndef _BS_TAGDIR_
#define _BS_TAGDIR_

extern bfTagT staticRootTagDirTag;

#define TAG_DIR_LOCK(bfSetp, ftxH) \
    ftx_lock_write(&bfSetp->dirLock, ftxH, __LINE__, __FILE__); 

/* Common argument structure. */
typedef struct tagInfo {
    bfSetT *bfSetp;
    domainT *dmnP;
    bfTagT tag;
    vdIndexT vdIndex;
    bfMCIdT bfMCId;
    ftxHT ftxH;
} tagInfoT;

/* Macros for tags. */
#define TAGTOPG(tag) ((tag)->num / BS_TD_TAGS_PG)
#define TAGTOSLOT(tag) ((tag)->num  % BS_TD_TAGS_PG)
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
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    vdIndexT *vdIndex           /* out - virtual disk index */
    );

statusT
tagdir_get_info(
    bfSetT *bfSetp,                         /* in - bitfile set desc ptr */
    unsigned long *firstUninitializedPage,  /* out - next page to use */
    unsigned long *firstUnmappedPage,       /* out - first unmapped page */
    unsigned long *freeListHead,            /* out - start of free list */
    int *bfCnt                              /* out - num bitfiles in set */
    );

statusT
tagdir_lookup_next(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    vdIndexT *vdIndex           /* out - virtual disk index */
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
                  rbfPgRefHT tdpgRef
                  );

statusT
tagdir_set_next_tag(
                    bfSetT *bfSetp,	/* in - bitfile set desc pointer */
                    bfTagT *tag         /* in - ptr to next tag value to use */
                    );

statusT
tagdir_reset_tagmap(
    tagInfoT *tip
    );

statusT
tagdir_stuff_tagmap(
    tagInfoT *tip
    );

statusT
tagdir_lookup2(
    bfSetT *bfSetp,             /* in - bitfile set descriptor pointer */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    vdIndexT *vdIndex           /* out - virtual disk index */
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

#endif /* _BS_TAGDIR_ */
