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

$ cat tag2name.msg 
$ 
$ *****************************************************************
$ *                                                               *
$ *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
$ *                                                               *
$ *****************************************************************
$
$ HISTORY
$ 
$ 
$ Messages for the ff_advfs command
$

$quote "
$set MS_FF

USAGE	"usage: %s [-F advfs] [-a num] [-c num] [-VIlsu] [-i tag-list]"
USAGE2  "          [-m num] [-n file] [-p prefix] {special | fsname} ...\n"
NOTNUM  "%s: Argument to option %s must be a number\n"
NOTMS   "%s: Filesystem %s is not AdvFS\n"
BADID   "%s: Could not get filesystem id for %s\n"
ETAGDIR "%s: %s is not the special .tags directory.\n"
ERRSTAT	"%s: stat(%s) error; %s\n"
BADPATH "%s: Pathname %s is too long\n"
NOPERM	"%s: Permission denied - user must be privileged to run %s.\n"
FSNERR  "%s: error processing specified device or filesystem name %s\n"
NOTAG   "%s: Requested tag %ld not found\n"
