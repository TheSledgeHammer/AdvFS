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
 *      AdvFS
 *
 * Abstract:
 *
 *      File Set routines
 */

statusT
fs_fset_create(
    char *dmnTbl,               /* in */
    char *setName,              /* in */
    serviceClassT reqServices,  /* in */
    uint32_t fsetOptions,       /* in */
    gid_t quotaId,              /* in */
    bfSetIdT *bfSetId,          /* out */
    ftxIdT  xid1,               /* in - CFS transaction id */
    ftxIdT  xid2                /* in - CFS transaction id */
    );

statusT
fs_fset_delete(
    char *dmnTbl,               /* in - name of set's domain table */
    char *setName,              /* in - name of set to delete */
    ftxIdT  xid                 /* in - CFS transaction id */
    );


statusT
fs_fset_get_info(
    char *dmnTbl,               /* in - domain table */
    uint64_t *nextSetIdx,       /* in/out - index of set */
    bfSetParamsT *bfSetParams,  /* out - the bitfile-set's parameters */
    uint32_t *userId,           /* out - bfset user id */
    int32_t flag                /* in - flag for bs_bfdmn_tbl_activate() */
    );

statusT
fs_fset_get_id(
    char *dmnTbl,               /* in - name of set's domain table */
    char *setName,              /* in - name of set to delete */
    bfSetIdT *bfSetId           /* out */
    );

/* end fs_file_sets.h */
