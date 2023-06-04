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
$ @(#)$RCSfile: showfsets.msg,v $ $Revision: 1.1.9.4 $ (DEC) $Date: 2001/01/26 14:59:04 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"   none"
2	"%3ddays"
3	"%s: error locking '%s'; [%d] %s\n"
4	"                         Block (%3s) limits               File limits\n"
5	"1k"
6	"512"
7	"Fileset         BF    used    soft    hard  grace    used  soft  hard  grace\n"
8	"\tId           : %08x.%08x.%x.%04x\n"
14	"\tClone is     : %s\n"
15	"\tClone of     : %s\n"
16	"\tRevision     : %d\n"
17	"\tUser ID      : 0x%08x\n"
18	"\tFiles        : %8ld,"
19	"  SLim= %8ld,"
20	"  HLim= %8ld"
21	"  grc= "
22	"\tBlocks %5s :"
23	"(1k)"
24	"(512)"
34	"\tQuota Status : "
35	"user=on  "
36	"user=off "
37	"group=on  "
38	"group=off "
39	"%s: can't show fileset info for domain '%s'\n"
40	"%s: error = %s\n"
41	"%s: fileset '%s' does not exist in domain '%s'\n"
42	"%s: domain '%s' has no filesets\n"
43	"%s: error unlocking '%s'; [%d] %s\n"
USAGE	"usage: %s [-b | -q] [-k] domain [fileset ...]\n"
USAGE2  "\t[-b]\t - Lists the names of the filesets, without additional detail\n"
USAGE3  "\t[-q]\t - Displays the quota limits for filesets in a domain\n"
USAGE4  "\t[-k]\t - Displays block information in 1k units instead of 512\n"
OBJ1    "\tObject Safety: "
FRG1    "\tFragging     : "
OBJ2    "on "
OBJ3    "off "
