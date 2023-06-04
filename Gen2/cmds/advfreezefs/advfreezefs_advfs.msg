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
$set MS_ADVFREEZEFS

USAGE1          "usage: %s mount-point\n"
USAGE3          "    or %s -t timeout mount-point\n"
USAGE4          "    or %s -q mount-point\n"
NAME		"%s: error, command name must be advfreezefs or advthawfs\n"
SUSER		"%s: error, permission denied - must be privileged user\n"
ROOTUSRVAR      "%s: error, cannot freeze /, /usr or /var\n"
FROZEN          "%s is frozen\n"
NOTFROZEN       "%s is not frozen\n"
MPOPEN          "%s: Cannot open %s\n"
FREEZEFAIL	"%s: error performing freeze: [%d]\n"
THAWFAIL        "%s: error performing thaw: [%d]\n"
QUERYFAIL       "%s: error performing query: [%d]\n"
FREEZESUCCESS   "%s %s: Successful\n"
NOTMNT          "%s: file system is not mounted or not AdvFS\n"
FSNAME_ERR      "%s: error processing specified name %s\n"

