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

$ 
$ Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s %s"
EROOT	"Permission denied - user must be privileged to run %s.\n"
ELIMIT	"%s: file system name '%s' exceeds limit of %d characters\n"
BADCHAR	"%s: invalid character '%c' in file system name '%s'\n"
EMKDIR	"%s: cannot create dir '%s'; [%d] %s\n"
EGLOB	"%s: error locking AdvFS global lock; [%d] %s\n"
EXISTS	"%s: file system '%s' already exists\n"
ESIZE	"%s: cannot get device size; '%s'\n"
ELINK	"%s: cannot create symlink '%s @--> %s'; [%d] %s\n"
ESIG	"%s: Error modifying signal handler.\n"
EPATH	"%s: special must be a complete path\n"
NOACC	"%s: Cannot access specified device %s\n"
EBLK	"%s: Specified device %s is not a block special device\n"
EDEV	"%s: Error processing specified device name %s\n"
EVOLM	"%s: %s belongs to a volume manager. Please use the appropriate volume name.\n"
OVERWR	"%s: There appears to be a valid file system on %s.  If you continue, the current file system will be overwritten.\n"
SMALL   "%s: invalid file system size.  Specified size %ld is greater than device capacity %ld (blocks)\n"
ESPACE  "%s: Insufficient space for boot/swap reservation\n"
EINIT	"%s: file system init error %d : '%s'\n"
ERRCR	"%s: file system creation error %d : '%s'\n"
CONT    "CONTINUE? [y/n] "
EMNT    "%s: device %s already mounted\n"
NOTOURS "%s: %s is not an AdvFS file system.\n"
EFSPAR  "%s: error reading file system parameters for '%s'\n"
BADDEV  "%s: cannot create device '%s/%s'\n"
USAGE2	"       %s %s"
BADFSNAME  "%s:  file system name must not begin with a \".\"\n"
NOMMULTI   "%s:  The -m option is not supported for multi-volume filesystems.\n"

