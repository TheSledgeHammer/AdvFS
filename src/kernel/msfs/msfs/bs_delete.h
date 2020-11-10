/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
 * @(#)$RCSfile: bs_delete.h,v $ $Revision: 1.1.10.1 $ (DEC) $Date: 2000/01/18 13:20:40 $
 */

#ifndef _BS_DELETE_H_
#define _BS_DELETE_H_

/*
 * Define the location of the mcell that contains the head of the
 * deferred delete list.
 */
#define MCELL_LIST_PAGE 0
#define MCELL_LIST_PAGE_V3 1
#define MCELL_LIST_CELL 0

statusT
del_clean_mcell_list(
    vdT *vdp,                   /* virtual disk */
    u_long flag
    );

statusT
del_dealloc_stg(
    bfMCIdT pmcid,              /* in - primary mcell ID */
    vdT *pvdp                   /* in - virtual disk of primary mcell */
    ); 

statusT
del_add_to_del_list(
    bfMCIdT mcid,               /* in - mcell ID */
    vdT *vdp,                   /* in - virtual disk ptr */
    int ftxFlag,                /* in - don't start subtransaction if zero */
    ftxHT parentFtxH            /* in - transaction handle */
    );

statusT
del_remove_from_del_list(
    bfMCIdT mcid,               /* in - mcell ID */
    vdT *vdp,                   /* in - virtual disk */
    int ftxFlag,                /* in - don't start subtransaction if zero */
    ftxHT ftxH                  /* in - transaction handle */
    );

statusT
del_find_del_entry (
                    domainT *domain,  /* in */
                    vdIndexT vdIndex,  /* in */
                    bfTagT bfSetTag,  /* in */
                    bfTagT bfTag,  /* in */
                    bfMCIdT *delMcellId,  /* out */
                    int *delFlag  /* out */
                    );

#endif  /* _BS_DELETE_H_ */
