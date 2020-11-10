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
 * @(#)$RCSfile: advfs_evm.h,v $ $Revision: 1.1.27.2 $ (DEC) $Date: 2006/03/01 22:24:27 $
 */

#include <evm/evm.h>

#define EVENT_FDMN_MK 		_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.mk")
#define EVENT_FDMN_RM		_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rm")
#define EVENT_FDMN_BAL_LOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.lock")
#define EVENT_FDMN_BAL_UNLOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.unlock")
#define EVENT_FDMN_BAL_ERROR	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bal.error")
#define EVENT_FDMN_FRAG_LOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.frag.lock")
#define EVENT_FDMN_FRAG_UNLOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.frag.unlock")
#define EVENT_FDMN_FRAG_ERROR	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.frag.error")
#define EVENT_FDMN_ADDVOL	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.addvol")
#define EVENT_FDMN_RMVOL_LOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.lock")
#define EVENT_FDMN_RMVOL_UNLOCK	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.unlock")
#define EVENT_FDMN_RMVOL_ERROR	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.rmvol.error")
#define EVENT_FDMN_MULTI_RMVOL_START	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.multi.rmvol.start")
#define EVENT_FDMN_MULTI_RMVOL_STOP	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.multi.rmvol.stop")
#define EVENT_FDMN_MULTI_RMVOL_ERROR	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.multi.rmvol.error")
#define EVENT_FDMN_FULL		_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.full")
#define EVENT_FDMN_PANIC	_EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.panic")
#define EVENT_FDMN_BAD_MCELL_LIST _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.bad.mcell.list")
#define EVENT_FDMN_VD_EXTEND    _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.vd.extend")

#define EVENT_FSET_MK		     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.mk")
#define EVENT_FSET_CLONE	     _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.clone")
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
#define EVENT_FSET_BAD_FRAG          _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.bad.frag")
#define EVENT_FSET_OPTIONS           _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.options")
#define EVENT_FSET_INCONSISTENT_DIR_ENTRY  _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.inconsistent_dir_entry")
#define EVENT_FSET_FRAG_ABANDONED    _EvmSYSTEM_EVENT_NAME("fs.advfs.fset.frag.abandoned") 

#define EVENT_QUOTA_ON		 _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.on")
#define EVENT_QUOTA_OFF		 _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.off")
#define EVENT_QUOTA_SETGRP	 _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.setgrp")
#define EVENT_QUOTA_SETUSR	 _EvmSYSTEM_EVENT_NAME("fs.advfs.quota.setusr")

#define EVENT_ADVSCAN_RECREATE _EvmSYSTEM_EVENT_NAME("fs.advfs.advscan.recreate")

#define EVENT_MAX_ACCESS_STRUCT	 _EvmSYSTEM_EVENT_NAME("fs.advfs.special.maxacc")
#define EVENT_SS_ACTIVATED         _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.activated")
#define EVENT_SS_DEACTIVATED       _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.deactivated")
#define EVENT_SS_SUSPENDED         _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.suspended")
#define EVENT_SS_DEFRAG_ENABLED    _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.defrag.enabled")
#define EVENT_SS_DEFRAG_DISABLED   _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.defrag.disabled")
#define EVENT_SS_BAL_ENABLED       _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.bal.enabled")
#define EVENT_SS_BAL_DISABLED      _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.bal.disabled")
#define EVENT_SS_TOPIOBAL_ENABLED  _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.topiobal.enabled")
#define EVENT_SS_TOPIOBAL_DISABLED _EvmSYSTEM_EVENT_NAME("fs.advfs.ss.topiobal.disabled")
#define EVENT_FDMN_CCBT_IGNORED         _EvmSYSTEM_EVENT_NAME("fs.advfs.fdmn.ccbt.ignored")

typedef struct _advfs_event {
    char *special;
    char *domain;
    char *fileset;
    char *directory;
    char *clonefset;
    char *renamefset;
    char *user;
    char *group;
    char *options;
    long fileHLimit;
    long blkHLimit;
    long fileSLimit;
    long blkSLimit;
    long dirTag;
    long dirPg;
    long pgOffset;
    long frag;
    long fragtype;
    char *ev_info_str;
} advfs_ev;


#ifdef KERNEL
    int advfs_post_kernel_event(char *evname, advfs_ev *advfs_event);
#else
    int init_event(advfs_ev *advfs_event);
    int advfs_post_user_event
                    (char *evname, advfs_ev advfs_event, char *Prog);
#endif
