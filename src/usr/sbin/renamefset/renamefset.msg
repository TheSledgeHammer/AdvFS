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
$ @(#)$RCSfile: renamefset.msg,v $ $Revision: 1.1.4.3 $ (DEC) $Date: 1999/03/29 14:47:54 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s domain origSetName NewSetName\n"
2	"%s: bad arg <%s>\n"
3	"%s: error locking '%s'; [%d] %s\n"
4	"%s: domain '%s' doesn't exist\n"
5	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
6	"%s: error locking '%s'.\n"
7	"%s: Error = [%d] %s\n"
8	"%s: can't rename file set '%s' to set '%s' in domain '%s'\n"
9	"%s: error = %s\n"
10	"%s: error unlocking '%s'; [%d] %s\n"
11	"\nPermission denied - user must be root to run %s.\n\n"
