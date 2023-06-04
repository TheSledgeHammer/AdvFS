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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Public structure constants.
 *
 */

#include <ms_public.h>

/****************************************************************************
 *    bs_public.h constants
 ****************************************************************************/

bfTagT         NilBfTag        = { 0, 0, 0 };

bfDomainIdT    nilBfDomainId   = NULL_STRUCT;

bfFsCookieT    nilFsCookie     = NULL_STRUCT;

bfSetIdT       nilBfSetId      = NULL_STRUCT;

bfPageRefHT    NilBfPageRefH   = { 0 };

logRecAddrT    logEndOfRecords = { 0xffff, 0xffff, 0 };
logRecAddrT    logNilRecord    = NULL_STRUCT;

serviceClassT  nilServiceClass = 0;
serviceClassT  defServiceClass = 1;

bsVdParamsT    bsNilVdParams   = { 0 };

bfParamsT      bsNilBfParams   = { 0 };

bfDmnParamsT   bsNilDmnParams  = { 0 };

bfSetParamsT   bsNilBfSetParams = { 0 };

bsUnpinModeT   BS_DIRTY        = { BS_CACHE_IT, BS_MOD_LAZY };
bsUnpinModeT   BS_WRITETHRU    = { BS_CACHE_IT, BS_MOD_SYNC };
bsUnpinModeT   BS_CLEAN        = { BS_CACHE_IT, BS_NOMOD };
bsUnpinModeT   BS_LOG          = { BS_RECYCLE_IT, BS_LOG_PAGE };

adv_ondisk_version_t BFD_ODS_LAST_VERSION = { BFD_ODS_LAST_VERSION_MAJOR,
                                        BFD_ODS_LAST_VERSION_MINOR }; 
adv_ondisk_version_t BFD_ODS_NULL_VERSION = { 0, 0 };

/* end bs_public.c */

