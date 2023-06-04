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
$ @(#)$RCSfile: libmsfs.msg,v $ $Revision: 1.1.9.1 $ (DEC) $Date: 2000/06/30 13:49:01 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set S_LIBRARY1
LIBRARY1	"**** ADVFS warning ****\n"
LIBRARY2	"The command failed.\n"
LIBRARY3	"A valid error message cannot be reported.\n\n"
LIBRARY4	"This system appears to be running with an\n"
LIBRARY5	"old version of /sys/BINARY/msfs_syscalls.o.\n"
LIBRARY6	"Reinstall the AFABASIC subset and rebuild\n"
LIBRARY7	"the system's kernel (vmunix).\n"
LIBRARY9	"Can't read bootblock for %s\n"
LIBRARY10	"Can't open ascii disklabel file\n"
LIBRARY11	"can't find partition for %s, disklabel not modified\n"
LIBRARY12	"can't find disk type for %s, disklabel not modified\n"
LIBRARY13	"Can't lseek ascii disklabel file %d, label not modified\n"
LIBRARY14	"ioctl (WDINFO)"
LIBRARY15	"%s: can't access device %s; [%d] %s\n"
LIBRARY16	"%s: unable to open directory <%s>; [%d] %s\n"
LIBRARY17	"%s: device in use by domain %s; device %s\n"


$set S_MU_TASK_GETRUSAGE1
MU_TASK_GETRUSAGE1	"task_threads:task_getrusage task_info call error\n"
MU_TASK_GETRUSAGE2	"\n debug info begins..\n"
MU_TASK_GETRUSAGE3	"  CPU Seconds : %10.6f  (Usr: %10.6f,  Sys: %10.6f) TASK\n"
MU_TASK_GETRUSAGE4	"task_threads:task_getrusage\n"
MU_TASK_GETRUSAGE5	"skipping %d: task_getrusage\n"
MU_TASK_GETRUSAGE6	"  CPU Seconds : %10.6f  (Usr: %10.6f,  Sys: %10.6f) THREAD\n"
MU_TASK_GETRUSAGE7	"\n debug info ends..\n"

$set S_LIBRARY2
LIBRARY_EVM		"%s: error [%d] posting event: %s\n"
LIBRARY_EVM2            "%s: informational: [%d] posting event: %s\n\tIf running in single user mode, EVM is not running.\n\tPlease ignore this posting.\n"


