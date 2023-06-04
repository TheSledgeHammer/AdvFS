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

$ "\n wait   sig bcast\n"
$ *****************************************************************
$ *                                                               *
$ *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
$ *                                                               *
$ *****************************************************************
$
$ HISTORY
$ 
$ 

$quote "

$set 1
USAGE1	"\nusage: %s [options] [stats-type] {special | fsname}\n"
USAGE2	"\noptions:\n"
USAGEI	"\t-i sec  \trepeat display every 'sec' seconds\n"
USAGEC	"\t-c count\trepeat display 'count' times\n"
USAGES	"\t-s      \tdisplay raw statistics\n"
USAGEB	"\t-b      \tbuffer cache statistics\n"
USAGEF0	"\t-f 0    \tall filesystem vnop statistics\n"
USAGEF1	"\t-f 1    \tall filesystem lookup statistics\n"
USAGEF2	"\t-f 2    \tcommon filesystem vnop statistics\n"
USAGEG	"\t-g      \tlog statistics\n"
USAGEL0	"\t-l 0    \tbasic lock statistics\n"
USAGEL1	"\t-l 1    \tlock statistics\n"
USAGEL2	"\t-l 2    \tdetailed lock statistics\n"
USAGEV	"\t-v      \tvolume read/write statistics\n"
USAGER	"\t-B r    \tBMT Record read statistics\n"
USAGEW	"\t-B w    \tBMT Record write/update statistics\n"
HDR1	" pin                 ref            unpin-type          misc      cons\n"
HDR2	"  cnt  cnt  lazy  blk  cln  log\n"
HDR3	" pin                    pin-type          data-type   \n"
HDR4	"  cnt   lazy   blk   cln   log other\n"
HDR5	"vol%1d    "
HDR6	"  rd  wr"
HDR7	"lookup"
HDR8	"create"
HDR9	"close"
HDR10	"getattr"
HDR11	"setattr"
HDR12	"read"
HDR13	"write"
HDR14	"mmap"
HDR15	"fsync"
HDR16	"remove"
HDR17	"rename"
HDR18	"readdir"
HDR19	"mkdir"
HDR20	"rmdir"
HDR21	"symlink"
HDR22	"readlink"
HDR23	"link"
HDR26	"getpage"
HDR27	"putpage"
HDR28	"\nlookup   hit     %% noent     %%  miss     %%\n"
HDR29	"\n lkup  crt geta read writ fsnc   rm   mv rdir  mkd  rmd link\n"
HDR30	"  wait rwait signl bcast  lock"
HDR31	"type"
HDR32	"stateLk"
HDR33	"msgQCv"
HDR36   "\n wait   sig bcast\n"
HDR37   " writes trans segRecs trims wstWds maxPgs minPgs maxFWds maxAg maxSlots oldAg excWaits fullWaits\n"
HDR38	"ftxExc"
HDR39	"ftxTrim"
HDR40	"ftxSlot"
HDR41	"  wait sigFree sigNext"
BADARGF	"%s: invalid arg to -f\n"
BADARGL	"%s: invalid arg to -l\n"
BADARGB	"%s: invalid arg to -B\n"
ERRFS	"%s: unable to get info for filesystem '%s'\n"
ERRGEN	"%s: error = %s\n"
ERRLKST	"%s: unable to get lock stats; %s\n"
ERRSTAT	"%s: unable to get stats for filesystem %s: %s\n"
ERRMNT  "%s: Filesystem %s must be mounted for filesystem stats\n"
ERRUPD	"%s: unable to update stats for filesystem '%s'\n"
ONDISK  "%s: This utility cannot process the on-disk structures on this system.\n"
ERRINT	"%s: A zero time interval has been entered.\n"
ERRINT2 "%s: A negative time interval has been entered.\n"
NORPT	"%s: No report has been printed.  A non-positive count of %d was specified with the -c option.\n"
BADFSN  "%s: error processing specified name %s\n"

