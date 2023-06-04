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

$quote "

$set MS_NCHECK
NCHECK_BAD_IFLG "ncheck (advfs): -i must be followed by inode numbers\n"
NCHECK_INVALID_FSTYPE "ncheck (advfs): invalid fstype %s\n"
NCHECK_BAD_FLAG "ncheck (advfs): bad flag %c\n"
NCHECK_BAD_MFLG "ncheck (advfs): '-m' can only be used with '-i' or '-s'\n"
NCHECK_USAGE "Usage:\tncheck (advfs)\n\tncheck [-F advfs] [-i numbers] [-V|-a|-s|-m|-r] {special...|fsname...}\n"
NCHECK_BAD_STAT "ncheck (advfs): could not stat device %s\n"
NCHECK_BAD_FILE_SET "ncheck (advfs): could not get file set ID: %s\n"
NCHECK_NO_FILE "ncheck (advfs): no %s file\n"
NCHECK_NO_FSPEC "ncheck (advfs): %s not in %s\n"
NCHECK_BAD_FS "ncheck (advfs): %s not an advfs filesystem\n"
NCHECK_BAD_BFSET_PARAMS "ncheck (advfs): get bitfile set parameters: %s\n"
NCHECK_NO_MEM "ncheck (advfs): out of memory\n"
NCHECK_HASH_TOO_SMALL "ncheck (advfs): tag hash size of %ld is too small\n"
NCHECK_BAD_TAGDIR_PAGE "ncheck (advfs): cannot read tag directory page %ld\n"
