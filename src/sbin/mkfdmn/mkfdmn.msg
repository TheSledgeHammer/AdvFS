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
$ @(#)$RCSfile: mkfdmn.msg,v $ $Revision: 1.1.31.1 $ (DEC) $Date: 2001/11/20 20:38:31 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE2  "usage: %s [-o] [-r] [-F] [-l num_log_pages] [-V version]\n              [-x meta_extent_size] [-p meta_prealloc_size]\n              special domain\n"), Prog );
3	"%s: domain name '%s' exceeds limit of %d characters\n"
4	"%s: invalid character '%c' in domain name '%s'\n"
5	"%s: can't create dir '%s'; [%d] %s\n"
6	"%s: error access dir '%s'; [%d] %s\n"
7	"%s: error locking '%s'; [%d] %s\n"
8	"%s: domain '%s' already exists\n"
9	"%s: can't get device size; %s"
10	"%s: domain init error %s\n"
11	"%s: can't create symlink '%s @--> %s'; [%d] %s\n"
12	"%s: error unlocking '%s'; [%d] %s\n"
13	"%s: can't create new domain '%s' \n"
14	"Partition(s) which overlap %s are marked in use.\n"
15	"The disklabel for %s does not exist or is corrupted.\n"
16	"%s or an overlapping partition is open.\n"
17	"%s is an invalid device or cannot be opened.\n"
18	"The disklabel for %s cannot be updated.\n"
19	"Warning: %s is marked in use for %s.\n"
20    "unknown error code of %d\n"
21	"If you continue with the operation you can \n"
22	"possibly destroy existing data.\n"
23	"Quitting .... \n"
24	"Errors in checking %s with active AdvFS domains.\n"
25 	"/etc/fdmns directory seems to be missing or wrong.\n"
26	"Errors in checking %s with active swap devices.\n"
27	"Special device files associated with swap device(s) are missing.\n"	
28	"Specified partition %s is marked in use.\n"
29	"Also partition(s) which overlap %s are marked in use.\n"
30  "\nPermission denied - user must be root to run %s.\n\n"
31  "\nError:\n\t-x (extend size) parameter and -l (log size) relationship is unacceptable.\n"
32  "\t-x value cannot exceed 25%% of (size of log * %d)\n"
33  "\tCurrent size of log (-l) = %d\n"
34  "\tCurrent bmt extend size (-x) = %d\n"
35  "\nPlease modify bmt extend size (-x) or size of log (-l) parameter and try again.\n"
36  "Maximum -x value for log size (%d) is %d\n"
37  "Minimum -l value for bmt extend size (%d) is %d\n\n"
38  "\nWarning: domain '%s' created.  However extending the BMT\n"
39  ""with this -x value (%d) will take approximately %d minutes.\n"
40  "\nIf this is unacceptable, run 'rmfdmn %s' and then\n"
41  "re-create the domain with more reasonable -x or -l values.\n"
42  "\nIt will take approximately one minute of elapsed\n"
43  "time for every 5000 pages represented in the -x value.\n\n"
44  "%s: Invalid value for -x\n"
45  "%s: Setting to minimum value of %d\n"
46  "%s: Invalid value for -p\n"
47  "CONTINUE? [y/n] "
48  "%s: Invalid value for -V.  On-disk version must be between %d and %d.\n"
49  "%s: cluster filesystem error \n"
50  "%s: Can't use any part of the quorum disk for domain storage\n"
51  "At least one overlapping partition belongs to an existing AdvFS domain.\n"
