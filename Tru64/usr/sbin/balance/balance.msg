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
$ @(#)$RCSfile: balance.msg,v $ $Revision: 1.1.8.1 $ (DEC) $Date: 2001/12/14 16:51:54 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"%s: Stopping balance operation...\n"
2	"%s: Unexpected signal: %d\n"
3	"%s: Domain '%s' not balanced\n"
USAGE	"usage: %s [-v] domain\n"
5	"%s: Balancing domain '%s'\n"
6	"%s: Balanced domain '%s'\n"
7	"%s: Can't balance domain '%s'\n"
8	"%s: error locking '%s'.\n"
9	"%s: Error = [%d] %s\n"
10	"%s: Domain directory '%s' does not exist\n"
11	"%s: Error getting '%s' stats.\n"
12	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
13	"%s: Can't get domain parameters\n"
14	"%s: Error = %s\n"
15	"%s: Can't get domain's volume list\n"
16	"%s: Domain must have at least two volumes\n"
17	"%s: Error unlocking '%s'.\n"
18	"%s: Can't get volume file descriptors\n"
19	"%s: Can't allocate memory\n"
20	"%s: Can't find file set hdr - tag %d.%d\n"
21	"%s: Open of %s failed.\n"
22	"%s: Can't get file %s parameters\n"
23	"%s: Close of %s failed.\n"
24	"%s: Moving file %s to volume %d\n"
25	"%s: Can't move file %s\n"
26	"%s: Can't get extent descriptors\n"
27	"%s: getfsstat failed.\n"
28	"%s: Mount count = %d\n"
29	"%s: Can't allocate file system mount table.\n"
30	"%s: Can't allocate mount on name memory\n"
31	"%s: Can't allocate path name memory\n"
32	"%s: Can't allocate memory for file set header\n"
33	"%s: Can't get file set id.\n"
34	"%s: Domain name: %s, file set name: %s\n"
35	"%s: File set not mounted\n"
36	"%s: Can't get file set info for domain '%s'\n"
37	"%s: Close of .tags file failed. [%d] %s\n"
38	"%s: Can't allocate memory for expanded set table\n"
39	"%s: Can't allocate memory for new set table\n"
40	"%s: get vol params error %s\n"
41	"<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))"
42	"\nPermission denied - user must be root to run %s.\n\n"
43      "%s: error: All filesets must be mounted\n"
44  "%s: vfast is currently running in the background, cannot run %s on domain '%s'\n"
ONDISK  "%s: This utility can not process the on-disk structures on this system\n"
