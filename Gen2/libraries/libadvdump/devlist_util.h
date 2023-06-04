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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 *
 *
 * Facility:
 *
 *      File System
 *
 * Abstract:
 *
 *      Device list function prototypes and type definitions.
 *
 * Date:
 *
 *      Thu Jun  6 12:33:35 1991
 *
 */
#ifndef _DEVLIST_UTIL_
#define _DEVLIST_UTIL_

typedef char * dev_list_handle_t;

#define DEV_NAME_SZ 64

int dev_list_create( dev_list_handle_t *dev_list_h );
void dev_list_delete( dev_list_handle_t dev_list_h );
int dev_add( dev_list_handle_t dev_list_h, int dev_fd, char *dev_name );
void dev_alloc( dev_list_handle_t dev_list_h, int *dev_fd, char *dev_name );
int dev_dealloc( dev_list_handle_t dev_list_h, int dev_fd );


#endif /* _DEVLIST_UTIL_ */

/* end devlist_util.h */

