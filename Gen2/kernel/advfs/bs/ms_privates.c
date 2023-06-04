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
 *      Private structure constants.
 */
/*
 * HISTORY
 */

#include <ms_public.h>
#include <ms_privates.h>

/****************************************************************************
 *    bs_ods.h constants
 ****************************************************************************/

vdIndexT bsNilVdIndex = 0;
bfMCIdT bsNilMCId = { 0, 0, 0 };
bsDmnAttrT bsNilDmnAttr = { 0 };

/****************************************************************************
 *    bs_service_classes.h constants
 ****************************************************************************/

vdLstSgmntT nilVdLstSgmnt = NULL_STRUCT;
scEntryT nilScEntry = { 0, 0, 0, -1, 0, 0 };
serviceClassTblT nilServiceClassTbl = NULL_STRUCT;

/****************************************************************************
 *    bs_tagdir.h constants
 ****************************************************************************/

bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};


/* end ms_privates.c */
