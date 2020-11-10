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
$ @(#)$RCSfile: mkfset.msg,v $ $Revision: 1.1.14.1 $ (DEC) $Date: 2002/01/31 19:25:48 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"operator"
USAGE	"usage: %s [-o arg[,arg] domain fileset\n"
USAGE2  "\t[-o arg[,arg]    - specify fileset options, valid arguments are:\n"
USAGE3  "\t\t\t   frag and nofrag.\n"
8	"%s: fileset name '%s' exceeds limit of %d characters\n"
9	"%s: invalid character '%c' in fileset name '%s'\n"
10	"%s: error locking '%s'; [%d] %s\n"
11	"%s: domain directory '%s' does not exist\n"
12	"%s: error getting '%s' stats; [%d] %s\n"
13	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
14	"%s: error locking '%s'.\n"
15	"%s: error = [%d] %s\n"
16	"%s: can't create fileset '%s' in domain '%s'\n"
17	"%s: error = %s\n"
18	"%s: error unlocking '%s'; [%d] %s\n"
19	"No group \"operator\", using gid 0 for quota files\n"
20	"\nPermission denied - user must be root to run %s.\n\n"
21	"%s: no fileset name provided.\n"
22      "%s: insufficient memory.\n"
23      "%s: error, unknown -o argument '%s'.\n"

