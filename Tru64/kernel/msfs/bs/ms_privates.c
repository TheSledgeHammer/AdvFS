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
 *      Private structure constants.
 *
 * Date:
 *
 *      Fri Apr  3 18:07:19 1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: ms_privates.c,v $ $Revision: 1.1.9.5 $ (DEC) $Date: 1998/03/13 20:20:34 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>

/****************************************************************************
 *    bs_ods.h constants
 ****************************************************************************/

uint16T bsNilVdIndex = 0;
bfMCIdT bsNilMCId = { 0, 0 };
bsInMemXtntT NilXtnts = { 0 };
bsDmnAttrT bsNilDmnAttr = { 0 };
bsBfSetAttrT NilBfSetAttr = { 0 };

/****************************************************************************
 *    bs_service_classes.h constants
 ****************************************************************************/

vdLstSgmntT nilVdLstSgmnt = NULL_STRUCT;
scEntryT nilScEntry = { 0, 0, 0, -1, 0, 0 };
serviceClassTblT nilServiceClassTbl = NULL_STRUCT;

/****************************************************************************
 *    bs_domain.h constants
 ****************************************************************************/

domainT nilDomain = NULL_STRUCT;

/****************************************************************************
 *    bs_bitfile_sets.h constants
 ****************************************************************************/

bfSetT nilBfSet = NULL_STRUCT;
bsrRsvd17T DefbsrRsvd17 = { 0x4, 5, 0, 5, 50, 100, 0, 0 };


/****************************************************************************
 *    bs_tagdir.h constants
 ****************************************************************************/

bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};

/****************************************************************************
 *    file list for sad.  
 *
 *    NOTE: This list must match, in order, the list in msfs/advfs_modules.h
 *
 ****************************************************************************/
char *msfs_filenames[] = {
    "no_module",
    "bs_access.c",
    "bs_bitfile_sets.c",
    "bs_bmt_util.c",
    "bs_buffer2.c",
    "bs_copy.c",
    "bs_create.c",
    "bs_delete.c",
    "bs_domain.c",
    "bs_errlst.c",
    "bs_extents.c",
    "bs_init.c",
    "bs_inmem_map.c",
    "bs_migrate.c",
    "bs_misc.c",
    "bs_msg_queue.c",
    "bs_params.c",
    "bs_qio.c",
    "bs_sbm.c",
    "bs_service_class.c",
    "bs_stg.c",
    "bs_stripe.c",
    "bs_tagdir.c",
    "ftx_recovery.c",
    "ftx_routines.c",
    "ms_generic_locks.c",
    "ms_logger.c",
    "ms_mode.c",
    "ms_privatesc.c",
    "ms_publicc.c",
    "fs_create.c",
    "fs_dir_init.c",
    "fs_dir_lookup.c",
    "fs_file_sets.c",
    "fs_quota.c",
    "fs_read_write.c",
    "msfs_config.c",
    "msfs_io.c",
    "msfs_lookup.c",
    "msfs_misc.c",
    "msfs_syscalls.c",
    "msfs_vfsops.c",
    "msfs_vnops.c",
    "msfs_proplist.c",
    "bs_index.c"
};
