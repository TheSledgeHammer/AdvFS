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
$ @(#)$RCSfile: addvol.msg,v $ $Revision: 1.1.23.3 $ (DEC) $Date: 2001/11/20 20:38:36 $
$

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE1	"usage: %s [-s size] special domain\n"
USAGE2	"usage: %s [-F] [-x meta_extent_size] [-p meta_prealloc_size] special domain\n"
3	"%s: error locking '%s'; [%d] %s\n"
4	"%s: domain '%s' doesn't exist\n"
5	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
6	"%s: error locking '%s'.\n"
7	"%s: Error = [%d] %s\n"
8	"%s: error looking for '/' in %s\n"
9	"%s: link '%s' already exists\n"
10	"%s: can't get device size; %s"
11	"%s: error = %s\n"
12	"%s: can't create symlink '%s @--> %s'; [%d] %s\n"
13	"%s: error unlocking '%s'; [%d] %s\n"
14	"%s: Can't add volume '%s' to domain '%s'\n"
15    "The disklabel for %s does not exist or is corrupted.\n"
16    "%s or an overlapping partition is open.\n"
17    "%s is an invalid device or cannot be opened\n"
18    "The disklabel for %s cannot be updated.\n"
19      "Warning: %s is marked in use for %s.\n"
20      "unknown error code of %d\n"

22	"To edit an incorrect disklabel, use the -e flag with disklabel;\n"
23      "  for more information see the disklabel(8) reference page.\n"
24      "Errors in checking %s with active AdvFS domains.\n"
25      "/etc/fdmns directory seems to be missing or wrong.\n"
26      "Errors in checking %s with active swap devices.\n"
27      "Special device files associated with swap device(s) are missing .\n"
28      "Specified partition %s is marked in use.\n"
29      "Also partition(s) which overlap %s are marked in use.\n"
30      "Partition(s) which overlap %s are marked in use.\n"
31	"\nPermission denied - user must be root to run %s.\n\n"
32	"%s: Error, the %s argument must be greater than or equal to %d\n"
33	""
34	""
35	"%s: clu_adm_lock failed to get lock for cluster root\n"
36      "%s: cannot get hostname \n"
37      "%s: rsh failure, check that the /.rhosts file allows cluster alias access.\n"
38      "%s: cluster filesystem error \n"
39      "%s: forking error \n"
40      "%s: Can't add volume '%s' into svc class\n"
41      "use '/sbin/chvol -A %s %s' to activate the volume\n"
43	"%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"
44      "%s: Can't use any part of the quorum disk for domain storage\n"
45	"Error: The fstype for %s cannot be updated.\n"
46	"       Make sure node serving domain %s is running /sbin/vold.\n"
47	"       Make sure /sbin/vold is running.\n"
48	"lsm volume %s must have usage type of cluroot for cluster root domain\n"
49      "At least one overlapping partition belongs to an existing AdvFS domain.\n"
50      "If you continue with the operation you can \n"
51      "possibly destroy existing data.\n"
52      "Quitting .... \n"

