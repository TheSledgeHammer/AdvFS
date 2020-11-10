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
$ @(#)$RCSfile: chfile.msg,v $ $Revision: 1.1.36.1 $ (DEC) $Date: 2002/12/18 21:24:23 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s {[-l on | off] | [-L on | off]} filename ...\n"
2	"%s: error getting file stats for '%s'; %s\n"
3	"%s: '%s' must be a regular file\n"
4	"%s: open of %s failed; %s\n"
10	"atomic write data logging\n"
11	"forced synchronous writes\n"
12	"unknown\n"
13	"Are you trying to activate atomic write data logging on a non-AdvFS file or\non an AdvFS file which has been memory-mapped via mmap() or which ha a frag?\n"
14	"Are you trying to activate forced synchronous writes on a non-AdvFS file?\n"
15	"Are you trying to get information about a non-AdvFS file?\n"
16	"I/O mode = "
17	"%s: error setting file attributes for '%s'; %s\n"
18	"Are you trying to turn off a file attribute that is not turned on?\n"
19	"%s: error getting file attributes for '%s'; %s\n"
20	"normal asynchronous writes\n"
21	"Forced synchronous writes has already been activated.\n"
22      "Atomic write data logging has already been activated.\n"
23      "normal asynchronous writes (permanent)\n"
24      "           atomic write data logging (temporarily active)\n"
25      "unexpected in-memory I/O mode: %d\n"
26      "%s: error getting in-memory file attributes for '%s'; %s\n"
27      "Atomic write data logging cannot be enabled with direct I/O\n"
28      "Are you trying to activate atomic write data logging on a non-AdvFS file, or\non an AdvFS file which has direct I/O enabled, or on an AdvFS file which has\nbeen memory-mapped via mmap() or which has a frag?\n"
