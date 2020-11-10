/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 * @(#)$RCSfile: fs_file_sets.h,v $ $Revision: 1.1.12.1 $ (DEC) $Date: 2000/09/19 18:08:13 $
 */

statusT
fs_fset_create(
    char *dmnTbl,               /* in */
    char *setName,              /* in */
    serviceClassT reqServices,  /* in */
    serviceClassT optServices,  /* in */
    uint32T fsetOptions,        /* in */
    gid_t quotaId,              /* in */
    bfSetIdT *bfSetId,          /* out */
    long xid1,                  /* in - CFS transaction id */
    long xid2                   /* in - CFS transaction id */
    );

statusT
fs_fset_delete(
    char *dmnTbl,       /* in - name of set's domain table */
    char *setName,      /* in - name of set to delete */
    long xid            /* in - CFS transaction id */
    );

statusT
fs_fset_clone(
    char *dmnTbl,                 /* in */
    char *origSetName,            /* in */
    char *cloneSetName,           /* in */
    bfSetIdT *cloneBfSetId,       /* out */
    long xid                      /* in - CFS transaction id */
    );

statusT
fs_fset_get_info(
    char *dmnTbl,              /* in - domain table */
    uint32T *nextSetIdx,       /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    uint32T *userId,           /* out - bfset user id */
    int flag                   /* in - flag for bs_bfdmn_tbl_activate() */
    );

statusT
fs_fset_get_id(
    char *dmnTbl,       /* in - name of set's domain table */
    char *setName,      /* in - name of set to delete */
    bfSetIdT *bfSetId   /* out */
    );

statusT
fs_create_frag (
                bfSetT *bfSetp,  /* in */
                bfAccessT* bfap,  /* in */
                struct ucred *cred,  /* in */
                ftxHT parentFtxH  /* in */
                );

int
fs_quick_frag_test (
                    bfAccessT *bfap  /* in */
                    );
