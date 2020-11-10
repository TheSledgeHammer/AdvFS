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
$ @(#)$RCSfile: showfdmn.msg,v $ $Revision: 1.1.6.1 $ (DEC) $Date: 2000/03/10 18:43:52 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-k] domain ...\n"
2	"%s: out of memory\n"
3	"%s: unable to display volume info; domain not active\n"
4	"%s: get dmn vol list error %s\n"
5	"Vol"
6	"1K-Blks"
7	"512-Blks"
8	"Free"
9	"% Used"
10	"Cmode"
11	"Rblks"
12	"Wblks"
13	"Vol Name"
14	"%s: get vol params error %s\n"
15	"on"
16	"off"
17	"%s: error locking '%s'; [%d] %s\n"
18	"%s: no such domain '%s'\n"
19	"%s: unable to get info for domain '%s'\n"
20	"%s: error = %s\n"
22	"\n               Id              Date Created  LogPgs  Version  Domain Name\n"
23	"%s: error unlocking '%s'; [%d] %s\n"
24      "-data unavailable-"
ONDISK  "%s: This utility can not process the on-disk structures on this system\n"
26      "%s: getfsstat failed.\n"
27	"%s: can't malloc file system mount table.\n"
29	"BAD get_dmnname_params call, status = %d\n"
31	"%s: can't show set info for domain '%s'\n"
32	"Domain: %s, Metadata Size = %d Blocks\n"
33	"Domain: %s, Metadata Size = %d Blocks (1K)\n"
34	"open()"
35	"fstat()"
36	"advfs_xtntmap_open() error; %s\n"
37	"lseek"
38	"invalid frag group, pg %d"
39	"read error: fragPg %d, errno %d\n"
40	"\n--\ngroup pg = %6d, type = %d\n"
41	"      nextFree = %5d, lastFree = %5d, numFree = %5d\n\n"
42	"\n** inconsistency found **\n"
43	"** group header indicates %d free frags"
44	", free list has %d free frag(s)\n"
45	"** searching for additional free frags...\n"
46	"read error: fragPg %d, errno %d\n"
47	"\t** possible free frag: %5d, nextFree = %5d\n"
48	"\t\tfrag = %5d, fragPg = %d, fragSlot = %d\n"
49	"** search found %d possible free frag(s)\n"
50	"All filesets in domain must be mounted\n"
