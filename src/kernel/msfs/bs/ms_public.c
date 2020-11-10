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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Public structure constants.
 *
 * Date:
 *
 *      Fri Apr  3 17:34:58 1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: ms_public.c,v $ $Revision: 1.1.10.2 $ (DEC) $Date: 1998/06/16 20:12:52 $"

#include <msfs/ms_public.h>

/****************************************************************************
 *    bs_public.h constants
 ****************************************************************************/

bfTagT NilBfTag = { 0 };

bfDomainIdT nilBfDomainId = NULL_STRUCT;

bfSetIdT nilBfSetId = NULL_STRUCT;

bfPageRefHT NilBfPageRefH = { 0 };

logRecAddrT logEndOfRecords = { 0xffffffff, 0xffffffff, 0 };
logRecAddrT logNilRecord = NULL_STRUCT;

bfFragIdT bsNilFragId = {0, BF_FRAG_ANY};

serviceClassT nilServiceClass = 0;
serviceClassT defServiceClass = 1;

bsVdParamsT bsNilVdParams = { 0 };

bfParamsT bsNilBsParams = { 0 }; /* old name */
bfParamsT bsNilBfParams = { 0 };

bfDmnParamsT bsNilDmnParams = { 0 };

bfSetParamsT bsNilBfSetParams;

bsUnpinModeT BS_DIRTY = { BS_CACHE_IT, BS_MOD_LAZY };
bsUnpinModeT BS_WRITETHRU = { BS_CACHE_IT, BS_MOD_SYNC };
bsUnpinModeT BS_CLEAN = { BS_CACHE_IT, BS_NOMOD };
bsUnpinModeT BS_LOG = { BS_RECYCLE_IT, BS_LOG_PAGE };


char *SadFmt0 = "%s";
char *SadFmt1 = "%s\n N1 = %d";
char *SadFmt2 = "%s\n N1 = %d, N2 = %d";
char *SadFmt3 = "%s\n N1 = %d, N2 = %d, N3 = %d";
