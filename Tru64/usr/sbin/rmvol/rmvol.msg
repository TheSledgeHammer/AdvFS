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
$ @(#)$RCSfile: rmvol.msg,v $ $Revision: 1.1.29.3 $ (DEC) $Date: 2006/08/29 12:56:15 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"%s: Stopping rmvol operation...\n"
2	"%s: Unexpected signal: %d\n"
3	"%s: Can't add volume '%s' back into svc class\n"
4	"%s: Volume '%s' not removed from domain '%s'\n"
USAGE	"usage: %s [-f][-v] special domain\n"
6	"%s: Removing volume '%s' from domain '%s'\n"
7	"%s: Removed volume '%s' from domain '%s'\n"
8	"%s: Can't remove volume '%s' from domain '%s'\n"
9	"%s: error locking '%s'.\n"
10	"%s: Error = [%d] %s\n"
11	"%s: '%s' domain does not exist\n"
12	"%s: Error getting '%s' stats.\n"
13	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
14	"%s: Can't get domain parameters\n"
15	"%s: Error = %s\n"
16	"%s: %s is not the fully qualified device pathname\n"
17	"%s: Volume symlink '%s' does not exist\n"
18	"%s: Error getting '%s' symbolic link.\n"
19	"%s: Can't allocate memory for volume index array\n"
20	"%s: Can't get domain's volume list\n"
21	"%s: At least one volume must remain in the domain\n"
22	"%s: Can't get volume parameters\n"
23	"%s: Volume '%s' is not a member of domain '%s'\n"
24	"%s: An earlier rmvol did not completely remove volume '%s'.\n"
25	"Use the 'chvol -A' command to activate the volume.\n"
26	"%s: Can't remove volume from service\n"
27	"%s: Error unlocking '%s'.\n"
28	"%s: Switching root tag directory\n"
29	"%s: Can't switch root tag directory\n"
30	"%s: No one disk has enough available storage\n"
31	"%s: No one disk has enough available metadata\n"
32	"%s: Switching log\n"
33	"%s: Can't switch log\n"
34	"%s: Can't remove the volume\n"
35	"%s: Error locking '%s'.\n"
36	"%s: Error deleting '%s' symbolic link.\n"
37	"%s: Can't get volume file descriptors\n"
38	"%s: Can't find file set hdr - tag %d.%d\n"
39	"%s: Stat of %s failed.\n"
40	"%s: Open of %s failed.\n"
41	"%s: Moving file metadata\n"
42	"    file name: %s\n"
43	"%s: Can't move file %s metadata\n"
44	"%s: Can't get file %s parameters\n"
45	"%s: Unknown file type\n"
46	"%s: Close of %s failed.\n"
47	"%s: Moving file\n"
48	"%s: Can't create extent desc list\n"
49	"    moving pages - "
50	"page offset: %d, page count: %d\n"
51	"    rewriting extent map\n"
52	"%s: Can't rewrite file %s extent map\n"
53	"    no pages moved\n"
54	"%s: Can't malloc volume index array.\n"
55	"%s: Can't get extent map volume index\n"
56	"%s: Setting stripe segment's next allocation volume\n"
57	"This volume contains one stripe segment of %s, which will\n"
58	"be moved to another volume in the file domain that already\n"
59	"contains a stripe segment of %s.\n"
60	"Do you want to continue? (y/n):"
61	"%s: Stripe segment will be moved.\n"
62	"%s: Striped file will be suboptimal.\n"
63	"%s: Can't set stripe segment's next storage "
64	"allocation volume\n"
65	"%s: Stripe segment will not be moved.\n"
66	"%19spage offset: %d, "
67	"page count: %d\n"
68	"%s: Can't move file %s pages\n"
69	"%s: Migrate already active\n"
70	"%s: Clone does not have its own storage\n"
71	"%s: Can't get extent descriptors\n"
72	"%s: Can't allocate memory for expanded xtnt desc table\n"
73	"%s: Can't allocate memory for new xtnt desc table\n"
74	"%s: Open of reserved file %s failed.\n"
75	"%s: Can't get extent descriptors for reserved file %s\n"
76	"    set tag: %d.%d (0x%x.0x%x)\n"
77	"    tag: %d.%d (0x%x.0x%x)\n"
78	"%s: getfsstat failed.\n"
79	"%s: Mount count = %d\n"
80	"%s: Can't malloc file system mount table.\n"
81	"%s: Can't malloc mount on name memory\n"
82	"%s: Can't malloc path name memory\n"
83	"%s: Can't malloc memory for file set header\n"
84	"%s: Can't get file set id.\n"
85	"%s: Domain name: %s, file set name: %s\n"
86	"%s: File set not mounted\n"
87	"%s: Can't get file set info for domain '%s'\n"
88	"%s: Close of .tags file failed. [%d] %s\n"
89	"%s: Can't allocate memory for expanded set table\n"
90	"%s: Can't allocate memory for new set table\n"
91	"<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))"
92      "Error from set_usage %d setting disklabel on %s\n"
93	"\nPermission denied - user must be root to run %s.\n\n"
94      "%s: error: All filesets must be mounted\n"
ONDISK  "%s: This utility can not process the on-disk structures on this system\n"
96      "%s: Moving file prime mcell\n"
97	"%s: clu_adm_lock failed to get lock for cluster root\n"
98      "%s: cannot get hostname \n"
99      "%s: rsh failure, check that the /.rhosts file allows cluster alias access.\n"
100     "%s: cluster filesystem error \n"
101     "%s: forking error \n"
102     "use 'chvol -A %s %s' to reactivate the volume\n"
103     "%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"
104	"%s: Ran out of free space to migrate data off volume\n"
105     "%s: '%s' does not exist in domain\n"
106     "    extents have changed - reloading extent map for file %s\n"
107     "%s: Can't remove %d volume(s) from domain '%s'\n"
108     "    Free space available: %dK\n    Free space needed:    %dK\n"
109     "%s: Not enough free space for complete migration of all volumes requested for removal.\n"
110     "%s: Use 'showfdmn %s' to see which volumes are unavailable.\n"
111     "%s: Then use 'chvol -A' to activate each unavailable volume.\n"
112     "%s: One or more volumes in the domain are unavailable.\n"
113     "%s: Removing %d volume(s) from domain '%s'\n"
114     "%s: Removed %d volume(s) from domain '%s'\n"
115	"%s: Can't remove volume '%s' from service\n"
116	"%s: Insufficient space on volume being removed. Can not create metadata. Remove unwanted files and try again\n"
