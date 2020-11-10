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
 * @(#)$RCSfile: bs_params.h,v $ $Revision: 1.1.9.5 $ (DEC) $Date: 1998/12/21 23:09:55 $
 */

#ifndef _BS_PARAMS_
#define _BS_PARAMS_

struct bfAccess;
struct domain;

statusT
bs_get_bf_params(
                 struct bfAccess *bfap, /* in */
                 bfParamsT *bfParams,   /* out */
                 int lock               /* in */
                 );

statusT
bs_set_bf_params(
                 struct bfAccess *bfap, /* in */
                 bfParamsT *bfParams    /* in */
                 );

statusT
bs_get_bf_iparams(
                 struct bfAccess *bfap, /* in */
                 bfIParamsT *bfIParams, /* out */
                 int lock /* in */
                 );

statusT
bs_set_bf_iparams(
                 struct bfAccess *bfap, /* in */
                 bfIParamsT *bfIParams  /* in */
                );

statusT
bs_get_bf_page_cnt (
                    struct bfAccess *bfAccess,  /* in */
                    uint32T *pageCnt  /* out */
                    );

statusT
bs_get_dmn_params(
                  struct domain *dmnP,         /* in */
                  bfDmnParamsT *dmnParams,      /* out */
                  int lock                      /* in */
                  );

statusT
bs_set_dmn_params(
                  struct domain *dmnP,         /* in */
                  bfDmnParamsT *dmnParams       /* in */
                  );

statusT
bs_get_vd_params(
                 struct domain *dmnP,  /* in */
                 uint32T vdIndex,       /* in */
                 bsVdParamsT *vdParams, /* out */
                 int lock               /* in */
                 );

statusT
bs_set_vd_params(
                 struct domain *dmnP,  /* in */
                 uint32T vdIndex,       /* in */
                 bsVdParamsT *vdParams  /* in */
                 );
statusT
bs_get_dmn_vd_list(
                  struct domain *dmnP,   /* in - the domain handle */
                  int vdIndexArrayLen,    /* in - number of ints in array */
                  uint32T vdIndexArray[], /* out - list of vd indices */
                  int *numVds             /* out - num vds put in array */
                 );

statusT
bs_set_bfset_params(
    bfSetT *bfSetp,            /* in - bitfile-set's descriptor pointer */
    bfSetParamsT *bfSetParams, /* in - bitfile-set's params */
    long xid                   /* in - CFS transaction id */
    );

statusT
bs_get_bfset_params(
    bfSetT *bfSetp,            /* in - the bitfile-set's descriptor pointer */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    int lock                   /* in - not used */
    );

#endif /* _BS_PARAMS_ */
