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
$ @(#)$RCSfile: advfsstat.msg,v $ $Revision: 1.1.10.1 $ (DEC) $Date: 2000/12/12 20:19:29 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE1	"\nusage: %s [options] [stats-type] domain\n"
USAGE2	"\nusage: %s [options] -f 0 | 1 | 2 domain fileset\n"
3	"\noptions:\n"
4	"\t-i sec  \trepeat display every 'sec' seconds\n"
5	"\t-c count\trepeat display 'count' times\n"
6	"\t-R      \tdisplay ratios (use with -b, -p, -r)\n"
7	"\nstats-type:\n"
8	"\t-b      \tbuffer cache statistics\n"
9	"\t-f 0    \tall fileset vnop statistics\n"
10	"\t-f 1    \tall fileset lookup statistics\n"
11	"\t-f 2    \tcommon fileset vnop statistics\n"
12	"\t-l 0    \tbasic lock statistics\n"
13	"\t-l 1    \tlock statistics\n"
14	"\t-l 2    \tdetailed lock statistics\n"
15	"\t-n      \tnamei cache statistics\n"
16	"\t-p      \tbuffer cache pin statistics\n"
17	"\t-r      \tbuffer cache ref statistics\n"
18	"\t-v 0    \tvolume read/write statistics\n"
19	"\t-v 1    \tdetailed volume statistics\n"
20	"\t-v 2    \tvolume I/O queue statistics (snapshot)\n"
21	"\t-B r    \tBMT Record read statistics\n"
22	"\t-B w    \tBMT Record write/update statistics\n"
23	" pin                 ref            unpin-type          misc      cons\n"
24	"  cnt  hit hitw read  cnt  hit hitw lazy  blk  cln  log   ra  ubc   un abrt\n"
25	" pin                     ref               unpin-type\n"
26	"   cnt   hit  hitw  read   cnt   hit  hitw  lazy   blk   cln   log\n"
27	" pin                    pin-type          data-type   \n"
28	"  cnt   hit  hitw  read  lazy   blk   cln   log bsFtx fsFtx other\n"
29	" ref              data-type   \n"
30	"  cnt   hit  hitw bsFtx fsFtx other\n"
31	"vol%1d                                        "
32	"  rd   wr   rg  arg   wg  awg  blk ubcr flsh  wlz  rlz  con  dev"
33	"vol%1d                    "
34	"  rd  wr  rg arg  wg awg"
35	"vol%1d    "
36	"  rd  wr"
37	"\n goodh  negh  badh falsh  miss \n"
38	"lookup"
39	"create"
40	"close"
41	"getattr"
42	"setattr"
43	"read"
44	"write"
45	"mmap"
46	"fsync"
47	"syncdata"
48	"remove"
49	"rename"
50	"readdir"
51	"mkdir"
52	"rmdir"
53	"symlink"
54	"readlink"
55	"link"
56	"bread"
57	"brelse"
58	"page_write"
59	"page_read"
60	"getpage"
61	"putpage"
62	"\nlookup   hit     %% noent     %%  miss     %%\n"
63	"\n lkup  crt geta read writ fsnc dsnc   rm   mv rdir  mkd  rmd link\n"
64	"%20s  wait rwait signl bcast  lock"
65	"type"
66	"mutex"
67	"genLk"
68	"stateLk"
69	"shrLk"
70	"excLk"
71	"bufStateLk"
72	"pinBlkCv"
73	"bfFlushCv"
74	"ftxCv"
75	"msgQCv"
76	"total"
77	"\n%20s  wait rwait signl bcast  lock\n"
78	"\n mutex  wait   sig bcast\n"
79	"%s: invalid arg to -f\n"
80	"%s: invalid arg to -l\n"
81	"%s: invalid arg to -B\n"
82	"%s: invalid arg to -v\n"
83	"%s: unable to get info for domain '%s'\n"
84	"%s: error = %s\n"
85	"%s: unable to get lock stats; %s\n"
86	"%s: unable to get namei stats\n"
87	"%s: unable to get fileset stats; %s\n"
88	"%s: unable to get stats for domain '%s'\n"
89	"%s: unable to update stats for domain '%s'\n"
90	"\t-s      \tdisplay raw statistics\n"
91	"\t-S      \tsmoothsync queue statistics\n"
92	"\t-v 3    \tvolume I/O queue statistics (interval)\n"
93	"volume %d:\n"
94	"  smsyncQ counter:  %d\n"
95	"  smsync queues:"
ONDISK  "%s: This utility can not process the on-disk structures on this system.\n"
