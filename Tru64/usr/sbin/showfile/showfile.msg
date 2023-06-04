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
$ @(#)$RCSfile: showfile.msg,v $ $Revision: 1.1.25.2 $ (DEC) $Date: 2001/12/20 22:33:00 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE1	"usage: %s [-i] [-x | -h] file ...\n"
3	"\n         Id  Vol  PgSz  Pages  XtntType  Segs  SegSz  I/O   Perf  File\n"
4	"\n%s: lstat failed for file '%s'; %s\n"
5	"symlink"
6	"simple"
7	"mirror"
8	"stripe"
9	"sync  "
10	"ftx   "
11	"async "
12	"    cloneId    = %d\n"
13	"    cloneCnt   = %d\n"
14	"    reqServices= 0x%08x\n"
15	"    optServices= 0x%08x\n"
16	"%s: msfs_get_dmn_params failed; %s\n"
17	"%s: msfs_get_bf_xtnt_map failed. %s\n"
18	"%s: get_xtnt_frag failed.\n"
19	"Inheritable Attributes:  "
20	"dataSafety = %d; "
21	"extendSize = %d\n"
22	"reqService = %d\n"
23	"optService = %d\n"
24	"s_shelfPri = %d\n"
25	"s_readAhead = %d\n"
26	"%s: display_xtnt_map failed.\n"
27	"*get bf extent map error %s*\n"
28	"*display extent map error %s*\n"
29	"%s: close of %s failed. [%d] %s\n"
30	"%s: out of memory\n"
31	"%s: unable to display volume info; domain not active\n"
32	"%s: get dmn vol list error %s\n"
33	"%s: get vol params error %s\n"
34	"    extentMap: %d\n"
35	"        pageOff    pageCnt     vol    volBlock    blockCnt\n"
36	"%s: msfs_get_bf_xtnt_map failed. %s\n"
37	"        extentCnt: %d\n"
38	"        pageOff    pageCnt    volIndex    volBlock    blockCnt\n"
39      "%s: is not a directory\n"
40      "%s: does not have an index file\n"
41	"index"
42	"        Total storage extents: %d\n"
43	"        Total extent mcells: %d\n"
44	"        Number of storage extents: %d\n"
45	"        Number of extent mcells: %d\n"
46	"    stripe: %d\n"
47	"    PERMHOLE"
48	"        HOLE"
49	"        TERM"
51      "%s: seteuid error\n"
