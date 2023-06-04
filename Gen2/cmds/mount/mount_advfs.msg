$ =======================================================================
$   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
$
$   This program is free software: you can redistribute it and/or modify
$   it under the terms of version 2 the GNU General Public License as
$   published by the Free Software Foundation.
$   
$   This program is distributed in the hope that it will be useful,
$   but WITHOUT ANY WARRANTY; without even the implied warranty of
$   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
$   GNU General Public License for more details.
$   
$   You should have received a copy of the GNU General Public License
$   along with this program.  If not, see <http://www.gnu.org/licenses/>.
$
$ =======================================================================
 
$ *****************************************************************
$ *                                                               *
$ *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
$ *                                                               *
$ *****************************************************************
$
$ HISTORY
$ 
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
ERR_LOCK	"mount: error locking '%s'; [%d] %s\n"
ERR_NOT_ADVFS	"mount: file system type not 'advfs'\n"
REPORT		"%s on %s: \n"
ERR_PERM	"mount: Permission denied - must be privileged user to mount an AdvFS file system.\n"
ERR_TBL_FULL	"mount: ERROR - file system or mount table full\n"
ERR_NO_MATCH	"mount: ERROR - Specified device does not match\nmounted device\n"
ERR_INVALID_FS	"mount: ERROR - Device does not contain a valid\nAdvFS file system\n"
ERR_NO_FS	"mount: ERROR - No such file system or mount directory\n"
ERR_NOT_SUPP	"mount: ERROR - Operation not supported\n"
USAGE		"usage:\n  mount %s "
OUTOFMEM		"mount: ERROR - could not allocate memory.\n"
ERR_INVALID_DEV		"mount: ERROR - %s is an invalid device or cannot be opened.\n"
ERR_NOT_BLOCKDEV	"mount: ERROR - %s is not a block device.\n"
ERR_EQUALS_EITHER	"mount: ERROR = %s or %s\n"
ERR_SERVER_ONLY		"mount: the -o server_only option can only be used in a cluster.\n"
ERR_INVALID_CONFIG_1	"mount: Error opening %s for %s. This\ndirectory stores configuration information needed by the AdvFS file system.\n"
ERR_INVALID_CONFIG_2	"mount: Either the specified file system does not exist or\nthe directory has become corrupt.  Please see fsadm for a possible fix.\n"
ERR_RW_RO		"mount: Conflicting ro/rw options.\n"
ERR_INVALID_MNTDIR	"mount: ERROR - %s must be a directory\n"
ERR_MAGIC_1		"mount: ERROR - %s does not contain the valid AdvFS identifier.\n"
ERR_MAGIC_2		"mount: Either the device does not contain a valid AdvFS file system\nor the AdvFS identifier has become corrupt.\n"
ERR_FSADM_SCAN		"mount: Please use fsadm to scan the file system config for errors\n"
ERR_FSCK		"mount: Please use fsck to check the file system\n"
ERR_SYSCALL		"mount: ERROR - %s"
ERR_ABSPATH		"mount: ERROR - absolute path name is required for %s\n"
