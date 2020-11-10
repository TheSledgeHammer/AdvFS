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
$ @(#)$RCSfile: stripe.msg,v $ $Revision: 1.1.6.1 $ (DEC) $Date: 2003/11/10 16:44:08 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s -n volume_count filename\n"
2	"%s: Illegal volume count.\n"
3	"%s: open of %s failed. [%d] %s\n"
4	"%s: advfs_get_bf_params failed --- %s\n"
5	"%s: advfs_set_bf_attributes failed --- %s\n"
6	"%s: close of %s failed. [%d] %s\n"
7       "\n%s: stat failed for file '%s'; %s\n"
