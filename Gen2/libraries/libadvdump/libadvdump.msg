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

$ /*
$  * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
$  */
$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set S_BUFPOOL_UTIL1
BUFPOOL_UTIL1	"%s: invalid buffer size %d\n"
BUFPOOL_UTIL6	"%s: cannot find free buffer."
BUFPOOL_UTIL7	"%s: tried to deallocate invalid buffer <0x%08x>."


$set S_LZW1
LZW1	"\n**** compression stack overflow ****\n"
LZW2	"\n**** compression stack underflow ****\n"
LZW3	"Data is not in compressed format.\n"


$set S_MSGQUEUE_UTIL1
MSGQUEUE_UTIL8	"%s: msg length <%d> exceeds max msg length <%d>\n"
MSGQUEUE_UTIL9	"msg queue corruption"
MSGQUEUE_UTIL10	"msg_recv: buf size <%d> is smaller than msg size <%d>\n"


$set S_UTIL1
UTIL1	"\n\n************* PROGRAM ABORT **************\n\n"
UTIL2	"%s: *internal error* missing path name in get_fs_info()\n"
UTIL3	"%s: error accessing file system <%s>; [%d] %s\n"
UTIL4	"path     : %s\n"
UTIL5	"dev/fset : %s\n"
UTIL6	"type     : %s\n"
UTIL7	"type     : unknown\n"
UTIL8   "advfs id : 0x%08x.%08x.%x\n"
UTIL19	"\n     CPU Time :  %02i:%02i:%09.6f  (Usr: %02i:%02i:%09.6f,  Sys: %02i:%02i:%09.6f)"
UTIL20	"\n Elapsed Time :  %02i:%02i:%09.6f\n"
UTIL21	"\n Files Processed :  %9li"
UTIL22	"\n      Bytes Read :  %9li,        Bytes Written : %9li"
UTIL23	"\n  Bytes/Sec Read :  %9li,    Bytes/Sec Written : %9li\n"
UTIL24	"%s: Write EOF on tape %04d failed; [%d] %s\n"
UTIL26	"%s: Unload of tape %04d failed; [%d] %s\n"
UTIL27	"%s: Rewind of tape %04d failed; [%d] %s\n"
UTIL28	"%s: do you want to retry? "
UTIL29	"\n%s: Change Tapes: Mount tape# %04d\n"
UTIL30	"%s: Press RETURN when tape is mounted... "
UTIL31	"%s: cannot open device file <%s>; not ready\n"
UTIL32	"%s: cannot open device file <%s>; [%d] %s\n"
UTIL33	"%s: Device '%s' is not ready.\n"
UTIL34	"%s: cannot get dev info for writing; [%d] %s\n"
UTIL35	"\nmtiocget ioctl failed!\n"
UTIL36	"\nMTIOCGET ELEMENT	CONTENTS"
UTIL37	"mt_type			"
UTIL38	"MT_ISTS\n"
UTIL39	"MT_ISHT\n"
UTIL40	"MT_ISTM\n"
UTIL46	"Unknown mt_type = 0x%x\n"
UTIL47  "mt_dsreg1\t\t%X\n"
UTIL47B "mt_dsreg2\t\t%X\n"
UTIL48	"mt_erreg\t\t%X\n"
UTIL49	"mt_resid\t\t%X\n"
UTIL50	"\nmtget ioctl failed!\n"
UTIL51  "MT_ISHPIB_REEL\n"
UTIL52  "MT_ISDDS1\n"
UTIL53  "MT_ISDDS2\n"
UTIL54  "MT_ISSCSI_REEL\n"
UTIL55  "MT_ISQIC\n"
UTIL56  "MT_IS8MM\n"
UTIL57  "MT_IS3480\n"
UTIL58  "MT_ISDLT\n"
UTIL59  "MT_ISAIT\n"
UTIL60  "MT_IS3590\n"
UTIL61  "MT_ISLTO\n"
UTIL126	"DEV_BOM "
UTIL127	"DEV_EOM "
UTIL128	"DEV_OFFLINE "
UTIL129	"DEV_WRTLCK "
UTIL164 "%s: exiting; user interaction required and no tty present\n"
UTIL165 "%s: cannot open device file <%s> on node <%s>; [%d] %s\n"
UTIL166 "%s: cannot open device file <%s> on node <%s>; not ready\n"
UTIL167	"%s: cannot get dev info for reading; [%d] %s\n"
UTIL168 "%s: could not open %s\n"
UTIL169 "%s: error [%d] posting event: %s\n"
UTIL170 "%s: informational: [%d] posting event: %s\n\tIf running in single user mode, EVM is not running.\n\tPlease ignore this posting.\n"

$set RVDUMP
CNTFK           "Cannot fork to broadcast message\n"
COUTMP          "Cannot open %s for reading\n"
FOPENTTY        "Cannot fopen /dev/tty for reading\n"
LOSTCON         "Lost connection to remote peer\n"
MFD1            "Message from the dump program to all operators "
MFD2            "at %d:%02d ... \r\n\n"
NEEDATTB        "DUMP NEEDS ATTENTION!\7\7\n"
NEEDATT1        "\nNEEDS ATTENTION: %s?: (\"%s\" or \"%s\") "
NOCON           "Cannot establish connection to remote rmt command\nPossibly allow user connection via .rhosts on remote host. (See rmt(8) manpage)"
NOCON1          "Cannot establish connection to remote %s command, trying %s\n"
OPENRTF         "Cannot open remote device file %s\n"
OPENRTFR        "Cannot open remote device file %s, retrying...\n"
PROTO           "rmt_reply(): Protocol to remote tape server botched (code %s)\n"
PROTO1          "rmt_gets(): Protocol to remote tape server botched\n"
RERR            "Communication error in connection to remote peer\n"
RRSCN           "Cannot read single character from socket\n"
RTRYOP          "Do you want to retry the open"
RRBN            "Cannot read buffer from socket\n"
RWBN            "Cannot write buffer to socket\n"
RWWLN           "Cannot write command line to socket\n"
UNKSERV         "rvdump: shell/tcp: unknown service\n"
YESNO1          "\"%s\" or \"%s\"?\n\n"
INFO1           "Trying to establish connection to host %s using username %s...\n"

$set S_COMMON

DEVOFFLINE      "%s: device <%s> is OFFLINE\n"
BADDEV          "%s: cannot get dev info for <%s>; [%d] %s\n"


