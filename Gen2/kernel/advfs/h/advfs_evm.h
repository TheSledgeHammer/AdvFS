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
 */
/*
 * Facility:
 *	Advanced File System
 *
 * Abstract:
 * 	Header file for AdvFS EVM support (Event Management)
 *
 */

#ifndef _ADVFS_EVM_H_
#define _ADVFS_EVM_H_

#include <evm/evm.h>

#define EVENT_FDMN_MK                _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.mk")
#define EVENT_FDMN_RM                _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rm")
#define EVENT_FDMN_BAL_LOCK          _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.lock")
#define EVENT_FDMN_BAL_UNLOCK        _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.unlock")
#define EVENT_FDMN_BAL_ERROR         _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.error")
#define EVENT_FDMN_DEFRAG_LOCK       _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.defrag.lock")
#define EVENT_FDMN_DEFRAG_UNLOCK     _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.defrag.unlock")
#define EVENT_FDMN_DEFRAG_ERROR      _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.defrag.error")
#define EVENT_FDMN_ADDVOL            _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.addvol")
#define EVENT_FDMN_RMVOL_LOCK        _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.lock")
#define EVENT_FDMN_RMVOL_UNLOCK      _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.unlock")
#define EVENT_FDMN_RMVOL_ERROR       _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.error")
#define EVENT_FDMN_FULL              _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.full")
#define EVENT_FDMN_PANIC             _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.panic")
#define EVENT_FDMN_BAD_MCELL_LIST    _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bad.mcell.list")
#define EVENT_FDMN_VD_EXTEND         _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.vd.extend")

#define EVENT_FSET_MK		     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.mk")
#define EVENT_SNAPSET_CREATE         _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.snap")
#define EVENT_FSET_RENAME	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.rename")
#define EVENT_FSET_RM_LOCK	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.rm.lock")
#define EVENT_FSET_RM_UNLOCK	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.rm.unlock")
#define EVENT_FSET_RM_ERROR	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.rm.error")
#define EVENT_FSET_BACKUP_LOCK	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.backup.lock")
#define EVENT_FSET_BACKUP_UNLOCK     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.backup.unlock")
#define EVENT_FSET_BACKUP_ERROR      _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.backup.error")
#define EVENT_FSET_MOUNT	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.mount")
#define EVENT_FSET_UMOUNT	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.umount")
#define EVENT_FSET_QUOTA_HFILE_LIMIT _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.quota.hfile.limit")
#define EVENT_FSET_QUOTA_HBLK_LIMIT  _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.quota.hblk.limit")
#define EVENT_FSET_QUOTA_SFILE_LIMIT _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.quota.sfile.limit")
#define EVENT_FSET_QUOTA_SBLK_LIMIT  _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.quota.sblk.limit")
#define EVENT_FSET_OPTIONS           _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.options")

#define EVENT_FSET_INCONSISTENT_DIR_ENTRY  _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.inconsistent_dir_entry")
#define EVENT_FILESYSTEM_THRESHOLD_UPPER_CROSSED      _EvmSYSTEM_EVENT_NAME("fs.advfs.filesystem.threshold.upper.crossed")

#define EVENT_FILESYSTEM_THRESHOLD_LOWER_CROSSED      _EvmSYSTEM_EVENT_NAME("fs.advfs.filesystem.threshold.lower.crossed")

#define EVENT_FSET_THRESHOLD_UPPER_CROSSED      _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.threshold.upper.crossed")

#define EVENT_FSET_THRESHOLD_LOWER_CROSSED      _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.threshold.lower.crossed")
#define EVENT_QUOTA_ON               _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.on")
#define EVENT_QUOTA_OFF              _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.off")
#define EVENT_QUOTA_SETGRP           _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.setgrp")
#define EVENT_QUOTA_SETUSR           _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.setusr")
#define EVENT_ADVSCAN_RECREATE       _EvmSYSTEM_EVENT_NAME("fs.advfs.advscan.recreate")
#define EVENT_MAX_ACCESS_STRUCT      _EvmSYSTEM_EVENT_NAME("fs.advfs.special.maxacc")
#define EVENT_SS_ACTIVATED           _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.activated")
#define EVENT_SS_DEACTIVATED         _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.deactivated")
#define EVENT_SS_SUSPENDED           _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.suspended")
#define EVENT_SS_DEFRAG_ENABLED      _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.defrag.enabled")
#define EVENT_SS_DEFRAG_DISABLED     _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.defrag.disabled")
#define EVENT_SS_BAL_ENABLED         _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.bal.enabled")
#define EVENT_SS_BAL_DISABLED        _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.bal.disabled")
#define EVENT_SS_TOPIOBAL_ENABLED    _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.topiobal.enabled")
#define EVENT_SS_TOPIOBAL_DISABLED   _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.topiobal.disabled")
#define EVENT_FDMN_CCBT_IGNORED      _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.ccbt.ignored")
#define EVENT_SNAP_BAD_PARENT_ACCESS _EvmSYSTEM_EVENT_NAME("fs.advfs.snap.open.parentfailed")
#define EVENT_FREEZE                 _EvmSYSTEM_EVENT_NAME("fs.advfs.freeze.frozen")
#define EVENT_THAW                   _EvmSYSTEM_EVENT_NAME("fs.advfs.freeze.thawed")


typedef struct _advfs_event {
    char     *special;
    char     *domain;
    char     *fileset;
    char     *directory;
    char     *snapfset;
    char     *renamefset;
    char     *user;
    char     *group;
    char     *options;
    uint64_t  fileHLimit;
    uint64_t  blkHLimit;
    uint64_t  fileSLimit;
    uint64_t  blkSLimit;
    uint64_t  dirTag;
    uint64_t  dirPg;
    uint64_t  pgOffset;
    uint32_t  threshold_domain_upper_limit;
    uint32_t  threshold_domain_lower_limit;
    uint32_t  threshold_fset_upper_limit;
    uint32_t  threshold_fset_lower_limit;
    /* 120 is the maximum advfs_ev.formatMsg size we want to use to preserve
     * readability in the EVM output.  The Event Format string is commonly
     * displayed with a timestamp header and restricting the formatMsg to 
     * 120 + timestamp keeps linewraping to two lines on an 80 character term.
     */
    char      formatMsg[120];
} advfs_ev;

#ifdef _KERNEL
    int advfs_post_kernel_event(char *evname, advfs_ev *advfs_event);
#else
    int init_event(advfs_ev *advfs_event);
    int advfs_post_user_event(char *evname, advfs_ev advfs_event, char *Prog);
#endif

#endif /* _ADVFS_EVM_H_ */

