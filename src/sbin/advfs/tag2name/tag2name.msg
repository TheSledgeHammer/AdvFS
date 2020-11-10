$ cat tag2name.msg 
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
$ @(#)$RCSfile: tag2name.msg,v $ $Revision: 1.1.14.1 $ (DEC) $Date: 2003/11/11 21:22:21 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-r] <domain> {[-S] <fileset> | [-T] <fileset_tag>} <file_tag>\n"
2	"       %s <mount_point>/.tags/<file_tag>\n"

$ ----Message 3 is now unused----
3       "%s mount_point/.tags/file_tag\n"

4	"%s: statfs(%s) error; [%d] %s\n"
5	"%s: stat(%s) error; %s\n"
6	"%s: open(%s) error; [%d] %s\n"
7	"%s: can't get file attributes; [%d] %s\n"
8	"%s: msfs_get_name() error.\nTry this:  %s [-r] <domain> {[-S] <fileset> | [-T] <fileset_tag>} <file_tag>.\n"
9	"\nPermission denied - user must be root to run %s.\n\n"
10	"File specified is not in an AdvFS fileset\n"
11	"%s is not the special .tags directory.\n"
12	"%s: Tags must be numeric.\n"
13	"%s: Tags cannot be less than 2, or greater than %d.\n"
14	"%s: illegal option -- %s\n"
15	"%d is a directory index file.\n"
16	"Tag %d is a directory index file.\n"
