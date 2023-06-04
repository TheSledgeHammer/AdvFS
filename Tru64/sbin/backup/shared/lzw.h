/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
/* 
 * =======================================================================
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
 *
 *
 * Facility:
 *
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Contains function definitions for lzw.c.
 *
 * Date:
 *
 *      Wed May 16 16:49:06 1990
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: lzw.h,v $ $Revision: 1.1.2.2 $ (DEC) $Date: 1993/09/08 16:03:01 $
 */


#define MIN_OUT_BUF_SIZE 4

typedef char * lzw_desc_handle_t;

/* needs IN, OUT notation on func protos */

int
init_compress (
   lzw_desc_handle_t *lzw_desc_h
   );

int
end_compress (
   lzw_desc_handle_t lzw_desc_h
   );

int
compress ( 
   lzw_desc_handle_t lzw_desc_h,
   char *in_buf, 
   int   bytes, 
   char *out_buf, 
   int  out_buf_size ,
   int  *bytes_compressed,
   int  *bytes_xferred
   );

void
start_compress (
   lzw_desc_handle_t lzw_desc_h
   );

int
finish_compress ( 
   lzw_desc_handle_t lzw_desc_h,
   char *out_buf 
   );

/* 
 * uncompress routines 
 */

int
init_uncompress (
   lzw_desc_handle_t *lzw_desc_h
   );

int
end_uncompress (
   lzw_desc_handle_t lzw_desc_h
   );

int
uncompress ( 
   lzw_desc_handle_t lzw_desc_h,
   char *in_buf, 
   int  bytes, 
   char *out_buf, 
   int  out_buf_size ,
   int  *bytes_decoded,
   int  *bytes_xferred
   );

int
start_uncompress (
   lzw_desc_handle_t lzw_desc_h 
   );

int
finish_uncompress ( 
   lzw_desc_handle_t lzw_desc_h,
   char *out_buf 
   );

/* end lzw.h */
