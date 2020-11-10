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
$ @(#)$RCSfile: chvol.msg,v $ $Revision: 1.1.16.1 $ (DEC) $Date: 2001/10/10 21:21:42 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-l] [-r blks] [-w blks] [-t blks] [-c on | off] [-A] special domain\n"
2	"%s: consol arg must be \"on\" or \"off\"\n"
4	"%s: threshold not a multiple of 16\n"
5	"%s: threshold not within range %d\-%d\n"
6	"%s: read blocks not within range %d\-%d\n"
7	"%s: write blocks not within range %d\-%d\n"
8	"%s: block fact not within range %d\-%d\n"
9	"%s: queue device not within range %d\-%d\n"
10	"%s: -r or -w has no immediate effect without consolidate\n"
15	"rblks = %d  wblks = %d  cmode = %s, thresh = %d\n"
16	"%s: can't set I/O parameters\n"
17	"%s: error = %s\n"
18	"%s: error locking '%s'.\n"
19	"%s: Error = [%d] %s\n"
20	"%s: Domain directory '%s' does not exist\n"
21	"%s: error getting '%s' stats; [%d] %s\n"
22	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
23	"%s: can't get domain parameters\n"
24	"%s: error looking for '/' in %s\n"
25	"%s: Volume directory '%s' does not exist\n"
26	"%s: error getting '%s' symbolic link; [%d] %s\n"
27	"%s: can't allocate memory for volume index array\n"
28	"%s: can't get domain's volume list\n"
29	"%s: can't get volume parameters\n"
30	"%s: volume '%s' is not a member of domain '%s'\n"
31	"\nPermission denied - user must be root to run %s.\n\n"
32      "%s: error: All filesets must be mounted\n"
ONDISK  "%s: This utility can not process the on-disk structures on this system.\n"
34      "rblks: min = %d  max = %d  pref = %d\nwblks: min = %d  max = %d  pref = %d\n"
35	"%s: A potentially ambiguous domain volume name was found.\n"
36	"    Changing from: %s\n"
37	"               to: %s\n"
38	"   Error renaming as: %s\n"
