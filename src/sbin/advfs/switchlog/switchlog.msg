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
$ @(#)$RCSfile: switchlog.msg,v $ $Revision: 1.1.4.3 $ (DEC) $Date: 1999/03/29 14:47:36 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s domain vol-idx\n"
2	"%s: cannot switch log to a new volume; %s\n"
3	"%s: error locking '%s'; [%d] %s\n"
4	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
5	"%s: error unlocking '%s'; [%d] %s\n"
6	"%s: domain '%s' doesn't exist\n"
7	"\nPermission denied - user must be root to run %s.\n\n"
