/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 *      TODO
 *
 * Date:
 *
 *      Mon Aug 19 14:44:52 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: msfs_config.c,v $ $Revision: 1.1.10.3 $ (DEC) $Date: 1997/06/18 15:35:22 $"

#include <sys/sysconfig.h>
#include <sys/mount.h>
#include <sys/errno.h>

/*
 * msfs routines to load and unload MFS from the kernel
 */

extern struct msfsops msfs_vfsops;

/*
 * msfs_fs_configure
 *
 * TODO
 */

int 
msfs_fs_configure(
	cfg_op_t op,
	caddr_t indata,
	size_t indatalen,
	caddr_t outdata,
	size_t outdatalen)
{
    int ret;

    switch (op) {
        case CFG_OP_CONFIGURE:
                 if ((ret = vfssw_add_fsname(MOUNT_MSFS, &msfs_vfsops, "advfs")) != 0) 
                     return (ret);
                 break;
                            
        case CFG_OP_UNCONFIGURE:
                 if ((ret = vfssw_del(MOUNT_MSFS)) != 0) 
                     return (ret);
                 break;
        default:         
                 return ENOTSUP;
                 break;
             }
    return 0;
}
