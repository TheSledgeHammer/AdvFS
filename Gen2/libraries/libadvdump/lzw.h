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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Contains function definitions for lzw.c.
 *
 */

#ifndef _ADV_LZW_H_
#define _ADV_LZW_H_

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
   char     *in_buf, 
   int32_t   bytes, 
   char     *out_buf, 
   int32_t  out_buf_size ,
   int32_t  *bytes_compressed,
   int32_t  *bytes_xferred
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
   char    *in_buf, 
   int32_t  bytes, 
   char    *out_buf, 
   int32_t  out_buf_size ,
   int32_t  *bytes_decoded,
   int32_t  *bytes_xferred
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

#endif /* _ADV_LZW_H_ */
/* end lzw.h */
