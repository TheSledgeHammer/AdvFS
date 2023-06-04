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
$
$ HISTORY
$ 
$ @(#)$RCSfile: rmfdmn.msg,v $ $Revision: 1.1.4.4 $ (DEC) $Date: 1999/03/29 14:47:43 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-f] domain\n"
2	"%s: error access dir '%s'; [%d] %s\n"
3	"%s: error locking '%s'; [%d] %s\n"
4	"%s: domain '%s' does not exist\n"
5	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
6	"%s: error locking '%s'.\n"
7	"%s: Error = [%d] %s\n"
8	"%s: getfsstat failed. \n"
9	"%s: Can't allocate file system mount table.\n"
10	"%s: Error = [%d] %s \n"
11	"%s: Fileset %s mounted on %s in domain %s. \n"
12	"All bitfile sets must be dismounted prior to removal. \n"
13	"%s: remove domain %s? [y/n] "
14	"%s: can't open %s, domain partitions will not be set unused\n"
15	"%s: domain %s removed.\n"
16	"%s: error unlocking '%s'; [%d] %s\n"
17	"%s: can't remove domain '%s' \n"
18	"%s: set_usage error = [%d] while setting disklabel of %s\n"
19      "%s: '%s' is not a symbolic link to a device.\n"
20	"\nPermission denied - user must be root to run %s.\n\n"
21	"%s: Could not rename domain %s\nto temporary name %s.\n"
