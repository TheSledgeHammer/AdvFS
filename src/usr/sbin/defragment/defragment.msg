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
$ @(#)$RCSfile: defragment.msg,v $ $Revision: 1.1.14.1 $ (DEC) $Date: 2003/01/23 16:54:31 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"%s: Stopping defragment operation...\n"
2	"%s: Unexpected signal: %d\n"
3	"%s: Domain '%s' not defragmented\n"
4	"%s: Data not gathered for domain '%s'\n"
USAGE	"usage: %s [-e] [-n] [-t time] [-T time] [-N threads] "
6	"[-v] [-V] domain\n"
7	"%s: -n may not be specified with -t or -T\n"
8	"%s: -t and -T may not be specified together\n"
10	"%s: Gathering data for domain '%s'\n"
11	"%s: Defragmented domain '%s'\n"
12	"%s: Can't defragment domain '%s'\n"
13	"%s: Can't gather data for domain '%s'\n"
14	"%s: error locking '%s'.\n"
15	"%s: Error = [%d] %s\n"
16	"%s: Domain directory '%s' does not exist\n"
17	"%s: Error getting '%s' stats.\n"
18	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
19	"%s: Can't get domain parameters\n"
20	"%s: Error = %s\n"
21	"%s: Can't get domain's volume list\n"
22	"%s: Error unlocking '%s'.\n"
23	"\nPass %d; Clearing\n"
24      "%s: Insufficient free space for defragmenting volume %d.\n"
25	"  There was insufficient time to gather data.\n"
26	"  Current domain data:\n"
27	"  Domain data as of the start of this pass:\n"
28	"      files modified since %s\n"
29	"    Extents:               %8d\n"
30	"    Files w/extents:       %8d\n"
31	"    Avg exts per file w/exts: %5.2f\n"
32	"    Aggregate I/O perf:        %3.0f%%\n"
33	"    Free space fragments:    %6d\n"
34	"      Free space:"
35	"\n      Fragments: "
36	"Filling\n"
37	"%s: Can't get volume file descriptors\n"
38	"%s: Can't allocate memory\n"
39	"%s: chk_move_file()::Can't find file set hdr - set tag %d.%d, file tag %d.%d - volume %d\n"
40	"%s: Open of %s failed.\n"
41	"%s: Can't get file %s parameters\n"
42	"%s: Defragmentation of striped files "
43	"is not implemented.\n"
44	"%s: Can't move file %s\n"
45	"%s: Unknown file type\n"
46	"%s: Close of %s failed.\n"
47	"  fill sub-pass %d\n"
48	"%s: Can't get file %s data\n"
49	"%s: Defragmentation of striped files"
50	"%s: Can't create extent desc list\n"
51	"%s: Moving file %s\n"
52	"    page offset: %d, page count: %d\n"
53	"%s: Rewriting extent map for %s\n"
54	"%s: Can't rewrite file %s extent map\n"
55	"%s: Migrate already active\n"
56	"%s: Clone does not have its own storage\n"
57	"%s: Can't get extent descriptors\n"
58	"%s: Can't allocate memory for expanded xtnt desc table\n"
59	"%s: Can't allocate memory for new xtnt desc table\n"
60	"%s: Can't allocate memory for run descriptors.\n"
61	"%s: getfsstat failed.\n"
62	"%s: Mount count = %d\n"
63	"%s: Can't allocate file system mount table.\n"
65	"%s: Can't allocate mount on name memory\n"
66	"%s: Can't allocate path name memory\n"
67	"%s: Can't allocate memory for file set header\n"
68	"%s: Can't get file set id.\n"
69	"%s: Domain name: %s, file set name: %s\n"
70	"%s: File set not mounted\n"
71	"%s: Can't get file set info for domain '%s'\n"
72	"%s: Close of .tags file failed. [%d] %s\n"
73	"%s: Can't allocate memory for expanded set table\n"
74	"%s: Can't allocate memory for new set table\n"
75	"%s: Can't allocate memory for map file name\n"
76	"%s: advfs_get_bf_params failed --- %s\n"
77	"%s: Can't allocate memory for bmt file name\n"
78	"%s: Can't get extents of bmt %s --- %s\n"
79	"%s: Can't allocate memory for map file buffer\n"
80	"%s: Can't allocate memory for anaylzing storage bit map\n"
81	"%s: Error opening bitmap file %s\n"
82	"  Volume %d: area at block %8d (%8d blocks): %2.0f%% full\n"
83	"%s: Error reading from bitmap file: %s\n"
84	"%s: Can't get system time\n"
85	"%s: get vol params error %s\n"
86	"<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))"
87      "%s: error: All filesets must be mounted\n"
88  "%s: cannot get domain params on domain %s; %s\n"
89  "%s: vfast is currently running in the background, cannot run %s on domain '%s'\n"
90  "%s: status not available on domain %s; %s\n"
NOROOT           "\nPermission denied - user must be root to run %s.\n\n"
UTIL1   "\n\n************* PROGRAM ABORT **************\n\n"
NOUMOUNT        "%s: Can't unmount bitfile sets\n"
NOMOUNT         "%s: Can't mount bitfile sets\n"
PASSERR         "%s: Error occurred during pass %d on volume %d.   Continuing...\n"
BADTIME         "%s: unable to set a time limit for defragment\n"
BADSYSTIME      "%s: unable to get system time\n"
GETRUN          "%s: Error occurred getting extent descriptors\n"
NOCOPY	        "%s: Can't allocate memory for linked list of files.\n"
INVALBIT	"%s: Invalid bitfile entry\n"
NOSETHDR	"%s: move_files_fill()::Can't find file set hdr - tag %d.%d volume %d\n"
BADLIST1      "%s: internal defragment error: insert_bf_entry_criteria1() list corruption\n (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))\n"
BADLIST2      "%s: internal defragment error: insert_bf_entry_criteria2() list corruption\n (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))\n"
MOVECLEAR       "%s: Moving file %s clear\n"
MOVEFILL        "%s: Moving file %s fill\n"
ONDISK  "%s: This utility can not process the on-disk structures on this system.\n"
OUTOFTIME	"%s: unable to complete pass %d on volume %d in remaining time.\n"
INVALID_NUM_THREADS	"%s: The number of threads specified is out of bounds, setting to %d\n"
NO_VOLUMES_FOUND	"%s: Unable to open domain.\n"
RLIMIT	"setrlimit() failed - Unable to change memory usage limits\n"
