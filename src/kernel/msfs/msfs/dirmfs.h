/*
  ****************************************************************************
 **                                                                          *
 **  (C) DIGITAL EQUIPMENT CORPORATION 1990                                  *
 */
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
 * @(#)$RCSfile: dirmfs.h,v $ $Revision: 1.1.8.3 $ (DEC) $Date: 1995/12/06 13:38:09 $
 */

#ifndef DIRMFS
#define DIRMFS

#include <msfs/ms_public.h>
#ifdef KERNEL
#include <sys/user.h> /* eventually includes UFS dirent */
#endif
#define NAME_MAX 255

#ifndef KERNEL /* take out our dirent definition */
struct dirent
{
    unsigned long  d_ino;
    unsigned short d_reclen;
    unsigned short d_namlen;
    char d_name[NAME_MAX+1];
}; 
#endif
struct dirContext
{
    int        total_pages;    /* total number of pages in dir */
    int        cur_page;       /* page currently reading from */
    int        dir_offset;     /* offset of dir entry to be read next */
    char      *dir_buffer;     /* pointer to buffer containing
                                  directory */
};

typedef struct dirContext DIRT;

extern DIRT opendir_s[100];

#ifndef KERNEL
extern DIRT* opendir( char * );
extern struct dirent* readdir( DIRT * );
extern int closedir( DIRT * );
#endif

#endif /*DIRMFS*/
