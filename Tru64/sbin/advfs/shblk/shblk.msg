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
$ @(#)$RCSfile: shblk.msg,v $ $Revision: 1.1.10.1 $ (DEC) $Date: 2001/10/10 21:25:47 $
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set 1
USAGE	"usage: %s [-startblk N] [-blkcnt N] filename\n"
2	"usage: %s [-sb N] [-bc N] filename\n"
3	"\n%s: missing parameter for option <-sb>\n"
4	"\n%s: missing parameter for option <-bc>\n"
5	"%s: illegal option <%s>\n"
6	"%s: bad arg <%s>\n"
7	"%s: missing file name\n"
8	"open"
9	"fstat"
10	"lseek"
11	"%s: startblk <%d> exceeds file's max blk <%d>\n"
12	"read"
13	"\nPermission denied - user must be root to run %s.\n\n"
14      "End-of-file reached\n"
