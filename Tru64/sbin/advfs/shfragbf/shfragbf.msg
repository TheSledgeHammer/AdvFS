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
$ @(#)$RCSfile: shfragbf.msg,v $ $Revision: 1.1.24.1 $ (DEC) $Date: 2001/12/14 19:15:57 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: shfragbf [-t 0|1|2|3|4|5|6|7] [-v] fragBf\n"
2	"lseek"
3	"open()"
4	"fstat()"
5	"advfs_xtntmap_open() error; %s\n"
6	"\nType    Grps     bad   Frags    free  in-use   Bytes    free  in-use\n"
7	"----  ------  ------  ------  ------  ------  ------  ------  ------\n"
8	"  %dk %7d %7d %7d %7d %7d %6dk %6dk %6dk\n"
9	"\nPercent Free Frags per Group Histogram\n"
10	"Type    Grps   0%%   9%%  19%%  29%%  39%%  49%%  59%%  69%%  79%%  89%%  99%%  100%%\n"
11	"----  ------  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---  ---\n"
12	"  %dk %7d"
13	"\nfragbf occupies %dk bytes\n"
14	"                %dk in-use, %dk free, %dk overhead, %dk wasted\n"
15	"invalid frag group, pg %d"
16	"\n--\ngroup pg = %6d, next pg = %6d, type = %d\n"
17	"      nextFree = %5d, numFree = %5d, lastFree = %5d\n\n"
18	"      nextFree = %5d, numFree = %5d\n\n"
19	"frag on free list : %5d"
20	"read error: fragPg %d, errno %d\n"
21	", nextFree = %5d\n"
22	"\n--\ngroup pg = %6d, type = %d\n"
23	"      nextFree = %5d, lastFree = %5d, numFree = %5d\n\n"
24	"\n** inconsistency found **\n"
25	"** group header indicates %d free frags"
26	", free list has %d free frag(s)\n"
27	"** searching for additional free frags...\n"
28	"frag = %5d, fragPg = %d, fragSlot = %d\n"
29	"\tself = %d, type = %d, nextFree = %d\n"
30	"\tself = 0x%08x, type = 0x%08x, nextFree = 0x%08x\n"
31	"\t** possible free frag: %5d, nextFree = %5d\n"
32	"\t\tfrag = %5d, fragPg = %d, fragSlot = %d\n"
33	"** search found %d possible free frag(s)\n"
34	"\nPermission denied - user must be root to run %s.\n\n"
35	"** group has been marked as BAD by the kernel: BF_FRAG_BAD_GROUP\n";
