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
$ @(#)$RCSfile: advscan.msg,v $ $Revision: 1.1.22.1 $ (DEC) $Date: 2002/09/26 17:14:56 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"no mem\n"
2	"\t\t\t\tV%d, %s"
3	", bootable"
4	"\t\t\t\t\t%d volume in domain\n"
5	"\t\t\t\t\t%d volumes in domain\n"
6	"\t\t\t\tLast mounted %08x.%08x\n"
7	"\tV%d, %s"
8	"\n\t\tCreated\t\t\t%s "
9	"\t\tLast mount\t\t%s\n"
USAGE	"usage: %s [-argq] [-f domain] <disk list> <lsm diskgroup list>\n"
11	"Ex: %s rz10 rz11 rz12 rootdg acctdg\n"
13	"Ex: %s -a              (Scans all disks found in /etc/fdmns)\n"
14	"Ex: %s -a -r           (Recreates anything missing from /etc/fdmns)\n"
15	"Ex: %s -a -f my_domain (Fixes links for my_domain)\n"
16	"Ex: %s -a -g           (Prints advfs partitions by disk)\n"
17	"\n Disk               Domain Id   	Date Created, State\n"
18	"Can't open %s\n"
19	"Can't open device %s\n"
20	"Can't read /etc/fdmns. You should rerun \n"
21	"advscan with -r option (recreate fdmns)\n"
22	"No devices to scan, rerun advscan with\n"
23	"devices specified\n"
24	"\nScanning devices\t"
25	"\n Partition		Domain Id\n"
26	"Error reading disklabel for %s\n"
27	"\nFound domains:\n\n"
28	"*unknown*\n"
29	"\t\tDomain Id\t%08x.%08x\n"
30	"\t\tCreated\t\t%s\n"
31	"\t\tDomain volumes\t\t%d\n"
32	"\t\t/etc/fdmns links\t%d\n"
33	"\n\t\tActual partitions found:\n"
34	"  Rmvoled"
35	"Unable to create /etc/fdmns; [%d] %s\n"
36	"\nCreating %s\n"
37	"Unable to create %s; [%d] %s\n"
38	"\tlinking %s\n"
39	"Could not link %s in %s; [%d] %s\n"
40	"\nAttempting to fix link/dev_count for domain \n\n\t%s\n\n"
41	"Partition containing volume count is missing.\n"
42	"Can't fix.\n"
43	"type 1\n"
44	"Domain OK; will mount with %d partitions\n"
45	"Nothing to fix\n"
46	"type 4\n"
47	"It appears that a partition is missing from the\n"
48	"domain. Can't fix.\n"
49	"type 3\n"
50	"type 2\n"
51	"Adding link %s for domain %s\n"
52	"type 5\n"
53	"The domain thinks it has more partitions than\n"
54	"were found. Can't fix.\n"
55	"type 6\n"
56	"The domain thinks it has less partitions than\n"
57	"\nNo domain by name %s found, can't fix\n"
58	"Can't open /etc/fdmns\n"
59	"Can't open domain dir %s\n"
60	"Can't readlink %s\n"
61	"Could not get raw device name for %s\n"
62	"\nPermission denied - user must be root to run %s.\n\n"
63      "\t\t\t\t\t%s"
64      "\t\t\t\t\t%1$s   *** Incorrectly located in ***\n\t\t\t\t\t        /etc/fdmns/%2$s"
