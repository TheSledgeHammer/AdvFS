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
$ @(#)$RCSfile: chfsets.msg,v $ $Revision: 1.1.23.2 $ (DEC) $Date: 2002/01/31 19:25:47 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
1	"%s: error locking '%s'; [%d] %s\n"
2	"%s: domain directory '%s' does not exist\n"
3	"%s: error getting '%s' stats; [%d] %s\n"
4	"%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"
5	"%s: error locking '%s'.\n"
6	"%s: error = [%d] %s\n"
7	"%s: clone fileset '%s' not changed\n"
8	"\tId           : %08x.%08x.%x.%04x\n"
9	"\tFile H Limit : %ld --> %ld\n"
10	"\tBlock H Limit: %ld --> %ld\n"
11	"\tFile S Limit : %ld --> %ld\n"
12	"\tBlock S Limit: %ld --> %ld\n"
13	"\tFragging     : %s --> %s\n"
14	"\tObject Safety: %s --> %s\n"
24	"%s: can't change fileset info\n"
25	"%s: error = %s\n"
26	"%s: fileset '%s' does not exist in domain '%s'\n"
27	"%s: domain '%s' has no filesets\n"
28	"%s: error unlocking '%s'; [%d] %s\n"
USAGE	"usage: %s options domain [fileset ...]\n"
30	"\t[-f limit]\t - specify fileset's file usage limit (quota)\n"
31	"\t[-F limit]\t - specify fileset's file usage soft limit (quota)\n"
32	"\t[-b limit]\t - specify fileset's block (1k) usage limit (quota)\n"
33	"\t[-B limit]\t - specify fileset's block (1k) usage soft limit (quota)\n"
OPTION1 "\t[-o arg[,arg]    - specify fileset options, valid arguments are:\n"
OPTION2 "\t\t\t    frag, nofrag,\n\t\t\t   objectsafety and noobjectsafety.\n"
37	"\nPermission denied - user must be root to run %s.\n\n"
38	"%s: no options specified, nothing to do\n"
39      "%s: at least one fileset in this domain must be mounted.\n"
40      "%s: getfsstat failed.\n"
41      "%s: can't allocate file system mount table.\n"
42      "%s: mount count = %d\n"
43      "%s: getfsstat failed.\n"
INVALOP "%s: error, unknown -o argument '%s'.\n"
NMALLOC "%s: insufficient memory.\n"
ROLL     "%s: The specified -o option cannot be set until rolling upgrade\nis complete.\n"
CONTINUE "%s: Continuing...\n"
