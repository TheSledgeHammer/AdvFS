$ 
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

$ HISTORY
$
$ @(#)$RCSfile: migrate.msg,v $ $Revision: 1.1.2.5 $ (DEC) $Date: 1999/03/29 14:47:52 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-p pageoffset] [-n pagecount] "
2	"[-s volumeindex] [-d volumeindex] filename\n"
3	"%s: open of %s failed. [%d] %s\n"
4	"%s: close of %s failed. [%d] %s\n"
5	"%s: msfs_get_bf_params failed --- %s\n"
6	"%s: advfs_migrate failed --- %s\n"
7	"%s: error locking '%s'; [%d] %s\n"
8	"%s: error unlocking '%s'; [%d] %s\n"
9	"%s: statfs of %s failed. [%d] %s\n"
10      "%s: domain '%s' doesn't exist\n"
11	"%s: strtok of %s failed. [%d] %s\n"
12      "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
13	"%s: %s not located on an AdvFS domain\n"
14	"\nPermission denied - user must be root to run %s.\n\n"
15	"%s: pagecount must be a positive integer.\n"
