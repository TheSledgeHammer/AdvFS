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
$ @(#)$RCSfile: vdf.msg,v $ $Revision: 1.1.10.1 $ (DEC) $Date: 2000/03/10 18:43:48 $
$ 
$
$ ==========================================================================
$ To translator:
$
$ ==========================================================================
$	Messages [6-10] : English display:
$ ==========================================================================
$ Domain            1024-blocks    Metadata        Used   Available Capacity
$ bos_rootdmn            150000        7056      113096       29848      80%
$ 
$ Fileset            QuotaLimit                    Used   Available Capacity
$ root                        -                  113096       29848      80%
$
$ ==========================================================================
$	Messages [6-10]: The spacing must be properly preserved.
$
$ 	Msgs 6 & 9: Both "Domain" and "Fileset" are 17+1 spaces wide (default)
$	- Note whitespaces at end of Msgs 6 & 9.
$	- The translated version of "Domain" and "Fileset" headings
$	  must fit into 17 spaces.
$ 	- The script will pad spaces if the actual domain/fileset name
$	  is longer.
$



1	usage: vdf [-kl] domain | domain#fileset\n
3	vdf: All filesets in the domain must be mounted.\n
4	vdf: No such domain \"%s\"\n
5	vdf: No such fileset \"%s\"\n
6	Domain            
7	 512-blocks    Metadata        Used   Available Capacity\n
8	1024-blocks    Metadata        Used   Available Capacity\n
9	Fileset           
10	 QuotaLimit                    Used   Available Capacity\n
15	\nClone fileset(s) in this domain; totals may not be accurate\n
16	vdf: Domain \"%s\" has no filesets\n
	
