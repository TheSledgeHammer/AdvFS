/* =======================================================================
 *   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of version 2 the GNU General Public License as
 *   published by the Free Software Foundation.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * =======================================================================
 */
/* 
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 *
 * Facility:
 *
 *     AdvFS 
 *
 * Abstract:
 *
 *     Tag Directory function prototypes.
 *
 */

#ifndef _BS_TAGDIR_
#define _BS_TAGDIR_



/* Macros for tags. */
#define TAGTOPG(tag) ((tag)->tag_num / BS_TD_TAGS_PG)
#define TAGTOSLOT(tag) ((tag)->tag_num  % BS_TD_TAGS_PG)
#define MKTAGNUM(pg, slot) (((pg) * BS_TD_TAGS_PG) + (slot))

#endif /* _BS_TAGDIR_ */
