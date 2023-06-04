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
$ @(#)$RCSfile: mktrashcan.msg,v $ $Revision: 1.1.2.3 $ (DEC) $Date: 1997/04/10 13:27:50 $
$

$quote "
$set MS_TRASHCAN

MKUSAGE         "usage: %s trashcan directory ... \n"
RMUSAGE         "usage: %s directory ... \n"
SHUSAGE         "usage: %s directory ... \n"
E_ATTACH        "%s: can't attach trashcan '%s' to '%s'; %s\n"
E_DETACH        "%s: can't detach trashcan from '%s'; %s\n"
E_STATFS        "%s: statfs() error; [%d] %s\n"
E_STAT          "%s: can't access '%s'; %s\n"
E_ADVFS         "%s: %s is not in a ADVFS filesystem\n"
E_NOTRASHCAN    "%s: '%s' has no trashcan\n"
E_PROG          "%s: program name must be one of 'mktrashcan', 'rmtrashcan' or 'shtrashcan'\n" ), 
E_ATTACHED1     "    '%s' attached to '%s'\n"
E_ATTACHED2     "' attached to '%s'\n"
E_DETACHED      "    '%s' detached\n"
ILLEGAL		"%s:  illegal option -- %s\n"
