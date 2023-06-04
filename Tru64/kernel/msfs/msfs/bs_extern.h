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
 * @(#)$RCSfile: bs_extern.h,v $ $Revision: 1.1.3.2 $ (DEC) $Date: 1993/09/08 15:53:51 $
 */
#include <unistd.h>
extern int open();
extern int close();
extern int creat();
extern int read();
extern int write();
extern int lseek();
extern int lstat();
extern int stat();
extern int getrusage();
extern int getpid();
extern int gettimeofday();
extern int sync();
extern int bcopy();
extern int bzero();
