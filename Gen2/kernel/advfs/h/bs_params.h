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
 * Facility:
 *
 *     AdvFS 
 *
 * Abstract:
 *
 *     Get/Set params prototypes.
 *
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
bs_get_dmn_params(
                  struct domain *dmnP,          /* in */
                  bfDmnParamsT *dmnParams,      /* out */
                  int lock                      /* in */
                  );

statusT
bs_set_dmn_params(
                  struct domain *dmnP,          /* in */
                  bfDmnParamsT *dmnParams       /* in */
                  );

statusT
bs_get_vd_params(
                 struct domain *dmnP,  /* in */
                 vdIndexT vdIndex,       /* in */
                 bsVdParamsT *vdParams, /* out */
                 int lock               /* in */
                 );

statusT
bs_set_vd_params(
                 struct domain *dmnP,  /* in */
                 vdIndexT vdIndex,       /* in */
                 bsVdParamsT *vdParams  /* in */
                 );
statusT
bs_get_dmn_vd_list(
                  struct domain *dmnP,   /* in - the domain handle */
                  int vdIndexArrayLen,    /* in - number of ints in array */
                  vdIndexT vdIndexArray[], /* out - list of vd indices */
                  int *numVds             /* out - num vds put in array */
                 );

statusT
bs_set_bfset_params(
    bfSetT *bfSetp,            /* in - bitfile-set's descriptor pointer */
    bfSetParamsT *bfSetParams, /* in - bitfile-set's params */
    uint64_t xid               /* in - CFS transaction id */
                               /* TODO - This uint64_t should really be
                                * ftxIdT, but that typedef (in ftx_public.h)
                                * has not been defined yet by the time we
                                * include this file.
                                */
    );

statusT
bs_get_bfset_params(
    bfSetT *bfSetp,            /* in - the bitfile-set's descriptor pointer */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    int lock                   /* in - not used */
    );

#endif /* _BS_PARAMS_ */

