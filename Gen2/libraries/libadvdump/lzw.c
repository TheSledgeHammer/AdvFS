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
 *      Contains routines that implement LZW compression and decompression
 *      as described in: "A Technique for High-Performance Data
 *      Compression", by Terry A. Welch in the June 1984 issue of
 *      IEEE Computer.  The variable names are taken directly from the
 *      algorithm in the article so that it's easy to read the article 
 *      and the code together.
 *
 *      The main difference between this implementation and others that
 *      are available is that this one is buffer oriented rather than
 *      being file oriented (as most others are).  This allows an application
 *      to compress/uncompress arbitrary data (whereas most implementations
 *      are written to compress/uncompress only files).  Also, this 
 *      implementation uses 16-bit codes rather than 12-bit codes (for no 
 *      particular reason other than it's easier to implement).
 *
 *
 */
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include "lzw.h"
#include "libvdump_msg.h"

extern nl_catd catd;

#define TRUE                    1
#define FALSE                   0
#define OKAY                    0
#define ERROR                  -1

typedef struct {
    int32_t  wK;     /* w is in upper 16 bits and K is in lower 16 bits */
    int32_t  wKcode; /* code for wK                                      */
} table_entry_t;

typedef struct {
    char      *chr;  /* stack elements; characters                           */
    int32_t   top;   /* stack top index; the stack is empty when top == 0    */
} stk_t;

typedef struct {
    /* 
     * Used only by compress 
     */
    int32_t new_compress; 
    int32_t wcode;
    int32_t wKcode; 
    int32_t old_ratio; 
    uint64_t bytes_in; 
    uint64_t bytes_out;

    /* 
     * Used only by uncompress 
     */
    int32_t new_uncompress; /* flag indicates whether a new set of buffers  */
                    /*    are being uncompressed                            */
    int32_t reset;      /* is the new_uncompress due to a RESET?            */
    int32_t old_code;   /* previous code read from input                    */
    int32_t next_code;  /* next available code                              */
    int32_t fin_char;   /* the last character decoded from a code (however  */
                    /* it's actually the first character of the final string) */
    stk_t stk;      /* see comments for uncompress                          */

    /* 
     * Used by both compress and uncompress 
     */
    table_entry_t *tbl;
} lzw_desc_t;

const lzw_desc_t new_lzw_desc = { 0 };

#define MAX_BYTE_CODE         255
#define BYTE_BITS               8
#define MAX_STK_SIZE       262144

#define CODE_BITS              16
#define CODE_BYTES              2
#define FIRST_CODE            259
#define COMPRESSED_TAG        258
#define RESET                 256
#define FREE_ENTRY            257

#define MAX_CODE              ((1 << CODE_BITS) - 1)

/*
 * The table size must be prime, and bigger than (CODE_BITS**2 - 1).  And
 * (CODE_BITS**2 - 1) should be 90% of the table size, since research has 
 * shown that hash table performance degrades rapidly once the table 
 * is 90% full.
 */
#if CODE_BITS == 14
#   define TABLE_SIZE          18013 /* (91%) */
#elif CODE_BITS == 15
#   define TABLE_SIZE          35023 /* (94%) */
#elif CODE_BITS == 16
#   define TABLE_SIZE          72817 /*69001 also works but too small (95%)*/
#endif

#define HASH( w, K ) ((w) ^ ((K) << (CODE_BITS - BYTE_BITS)))

#define REHASH( w, o ) ((((w) - (o)) < 0) ? \
                        (w) + (TABLE_SIZE - (o)) :  (w) - (o))

#define WRITE_CODE( w, ob, ox, b ) ob[ox] = w & 0xff; \
                                   ob[ox+1] = (w >> BYTE_BITS) & 0xff; \
                                   b += 2; ox += 2;

#define READ_CODE( ib, ix ) (u_char)ib[ix] | ((u_char)ib[ix+1] << BYTE_BITS); \
                            ix += 2; 

#define MAKE_wK( w, K ) (((w) << 16) | (K))
#define GET_K( wK )     ((wK) & 0xffff)
#define GET_w( wK )     ((uint32_t) (wK) >> 16)

/*
 * COMPRESS
 *
 *    The basic idea behind LZW compression is to scan the input one byte at
 *    a time and contruct a strings/code table based on the strings 
 *    encountered in the input.  Each string is assigned a unique code. This 
 *    way, when a string that is already in the table is encountered in the
 *    input only that strings code needs to be written to the output.  The
 *    longer the string the more compression is achieved.
 *
 * Algorithm:
 *
 *    In the algorithm the symbol "wk" represents the current string, "K"
 *    represents the current character (ie- the one just read from the
 *    input), and "w" represents the prefix string, and "wcode" is the
 *    code of "w".
 *
 *    Initialize the table to contain all free entries
 *    Initialize the next code (wKcode)  to the constant FIRST_CODE
 *    Read the first character and assign it to "w"
 *
 *    While there is input to process do
 *       input -> K
 *       compute hash index for wK
 *       compute rehash offset (for secondary Probes)
 *       
 *       While the current wK is not in the table (either found or added) do
 *          if the table entry for the hash index is free then
 *             if wKcode < the maximum code then
 *                add wK and its code (wKcode) to the table
 *                increment the next code (wKcode)
 *             endif
 *             write wcode (code for prefix string) to the output
 *             K -> wcode
 *             if wKcode >= maximum code then
 *                if the compression ratio has degraded then
 *                   write the RESET code to the output
 *                   reinitialize the table to contine all free entries
 *                   reinitialze wKcode to FIRST_CODE
 *                endif
 *             endif
 *          else if the table entry for hash index is for wK then
 *             take wKcode from the table and assign it to wcode
 *          else (need to rehash because hash index collides with an exiting
 *                entry)
 *             compute new hash index
 *          endif
 *       endwhile
 *    endwhile
 *
 *   Some of the details have been left out to keep the algorithm as simple
 *   as possible.  Some of the details left out include:
 *      - The algorithm also keeps track of the compression ratio once the
 *        "table is full" (actually the table is 90% full when the highest
 *        code is generated).  The table is not reset until the the
 *        compression ratio degrades.
 *      - The implementation processes one buffer full at a time and writes
 *        its output to an output buffer.  When either the input buffer is
 *        exhausted or the output buffer is filled the compression routine
 *        returns to its caller indicating how many bytes are left in the
 *        input buffer and how many bytes are in the output buffer.
 *
 * Usage:
 *
 *   The following code example implements a very simple file compression
 *   program.  It illustrates how the compression routines are used.  Note
 *   that if a program compresses several files it must call start_compress
 *   and finish_compress for each file.
 *
 *       #include <stdio.h>
 *       #include <sys/file.h>
 *       #include "lzw.h"
 *       
 *       main( int agrc, char *argv[] )
 *          {
 *          char uncomp_buf[1024], char comp_buf[8192];
 *          int total_bytes_compressed, bytes_compressed, bytes_xferred, 
 *              uncomp_fd, comp_fd, rcnt;
 *       
 *          uncomp_fd = open( argv[1], O_RDONLY );
 *          comp_fd = creat( strcat( argv[1], ".ZZ" ) , 0660);
 *       
 *          init_compress();
 *          start_compress();
 *       
 *          while ((rcnt = read(uncomp_fd, uncomp_buf, 1024)) > 0)
 *             {
 *             total_bytes_compressed = 0;
 *       
 *             while (total_bytes_compressed < rcnt)
 *                {
 *               compress( &uncomp_buf[total_bytes_compressed], 
 *                          rcnt - total_bytes_compressed,
 *                         &comp_buf[0], 8192,
 *                         &bytes_compressed, &bytes_xferred );
 *                total_bytes_compressed += bytes_compressed;
 *                write( comp_fd, comp_buf, bytes_xferred );
 *               }
 *             }
 *       
 *          finish_compress( comp_buf );
 *          write( comp_fd, comp_buf, 2 );
 *          end_compress();
 *       
 *          close( uncomp_fd );
 *          close( comp_fd );
 *          }
 *       
 */

/*
 * Function:
 *
 *      init_compress
 *
 * Function description:
 *
 *      Initializes compression.  Specifically, the string/code table
 *      is allocated.  This routine must be called before any other
 *      compression routine is called.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
int
init_compress(
              lzw_desc_handle_t *lzw_desc_h  /* out - lzw table handle */
              )


{
    int e;
    lzw_desc_t *lzw;

    lzw = (lzw_desc_t *) malloc( sizeof( lzw_desc_t ) );

    if (lzw == NULL) {return ERROR;}

    *lzw = new_lzw_desc;
    lzw->tbl = (table_entry_t *) malloc( TABLE_SIZE * sizeof( table_entry_t ) );

    lzw->new_compress = TRUE; 

    *lzw_desc_h = (lzw_desc_handle_t) lzw;

    return OKAY;
}

/* end init_compress */

/*
 * Function:
 *
 *      end_compress
 *
 * Function description:
 *
 *      This routine deallocates the string/code table and effectively
 *      shuts down compression.  This routine should be called when
 *      the compression routines are no longer needed.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
int
end_compress(
             lzw_desc_handle_t lzw_desc_h  /* in - lzw table handle */
             )

{
    lzw_desc_t *lzw;

    lzw = (lzw_desc_t *) lzw_desc_h;

    free( lzw->tbl );
    lzw->tbl = NULL;
    free( lzw );

    return OKAY;
}

/* end end_compress */

/*
 * Function:
 *
 *      start_compress
 *
 * Function description:
 *
 *      This routine sets up compression.  It initializes the string/code
 *      table and any other global variables used by compression.  This
 *      routine must be called before compressing a file or some other
 *      logical unit of data; finish_compression should be called when
 *      the file or unit of data has been compressed.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
void
start_compress(
               lzw_desc_handle_t lzw_desc_h  /* in - lzw table handle */
               )

{
    int e;
    lzw_desc_t *lzw;

    lzw = (lzw_desc_t *) lzw_desc_h;

    lzw->new_compress = TRUE;

    for (e = 0; e < TABLE_SIZE; e++) {
        lzw->tbl[e].wK = FREE_ENTRY;
    }
}

/* end start_compress */

/*
 * Function:
 *
 *      finish_compress
 *
 * Function description:
 *
 *      This routines is used to get the last string code after a file
 *      or logical unit of data has been compressed.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
int
finish_compress(
                lzw_desc_handle_t lzw_desc_h,  /* in - lzw tbl handle */
                char *out_buf                  /* in - ptr to output bfr */
                )
{
    int ox = 0, bytes_out = 0;
    lzw_desc_t *lzw;

    lzw = (lzw_desc_t *) lzw_desc_h;

    WRITE_CODE( lzw->wcode, out_buf, ox, bytes_out );

    return OKAY;
}

/* end finish_compress */

/*
 * Function:
 *
 *      compress
 *
 * Function description:
 *
 *      This routine compresses data in 'in_buf' and writes string codes
 *      to 'out_buf'.  When either all bytes in 'in_buf' are compressed
 *      or when 'out_buf' is full this routine returns to the caller and
 *      'bytes_compressed' will contain the number of bytes in 'in_buf'
 *      that were compressed and 'bytes_xferred' will contain the number
 *      of bytes written to 'out_buf'.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.  The out buf is not big enough
 *      OKAY  - If the function completed successfully
 */
int
compress(
         lzw_desc_handle_t lzw_desc_h,  /* in - lzw tbl handle */
         char *in_buf,        /* in - ptr to buf to compress */
         int  bytes,          /* in - byte count to compress */
         char *out_buf,       /* in - ptr to output buffer */
         int  out_buf_size ,  /* in - byte size of output buffer */
         int  *bytes_compressed,  /* out - num bytes compressed */
         int  *bytes_xferred  /* out - num bytes in out buf */
         )
{
    int 
      wK,
      K,
      hash_ix,
      new_ratio,
      rehash_offset,
      wK_found_or_added, 
      e,                      /* code table index                           */
      ix = 0,                 /* input buf index                            */
      ox = 0;                 /* output buf index                           */

    lzw_desc_t *lzw;

    /*----------------------------------------------------------------------*/

    lzw = (lzw_desc_t *) lzw_desc_h;

    out_buf_size -= out_buf_size % 2; /* make even since codes are 2 bytes */

    if (lzw->new_compress) {
        if (out_buf_size < (MIN_OUT_BUF_SIZE + CODE_BYTES)) {
            /* 
             * not enough room; need enough for 3 codes (6 bytes).
             */
            *bytes_compressed = 0; 
            *bytes_xferred    = 0; 
            return ERROR;
        }

        lzw->new_compress = FALSE;
        WRITE_CODE( COMPRESSED_TAG, out_buf, ox, lzw->bytes_out );
        lzw->wKcode = FIRST_CODE;
        lzw->old_ratio = 100;
        lzw->bytes_in = 1; 
        lzw->bytes_out = 0;
        lzw->wcode = (u_char) in_buf[ix++];
    }

    /* while there are bytes to compress and the output buf is not full */

    while ((ix < bytes) && ((ox + MIN_OUT_BUF_SIZE) <= out_buf_size)) {
        lzw->bytes_in++;
        K = (u_char) in_buf[ix];         /* input -> K                         */
        hash_ix = HASH( lzw->wcode , K );/* compute hash index of wK           */
        wK = MAKE_wK( lzw->wcode, K );   /* combine w and K into a 32-bit int  */

        if (hash_ix == 0) {
            rehash_offset = 1;
        } else {
            rehash_offset = TABLE_SIZE - hash_ix;
        }

        wK_found_or_added = FALSE;

        while (!wK_found_or_added) {
            if (lzw->tbl[hash_ix].wK == FREE_ENTRY) {
                /* wK is not in the table */

                if (lzw->wKcode <= MAX_CODE) {
                    /* wK -> string table */
                    lzw->tbl[hash_ix].wK     = wK;
                    lzw->tbl[hash_ix].wKcode = lzw->wKcode++;
                }
                /* code(w) -> output */
                WRITE_CODE( lzw->wcode, out_buf, ox, lzw->bytes_out ); 

                /* K -> w */
                lzw->wcode = K;

                if (lzw->wKcode > MAX_CODE) {
                    /* 
                     * table is full; reset only when compression
                     * ratio degrades
                     */
                    new_ratio = lzw->bytes_out * 100 / lzw->bytes_in;

                    if (new_ratio > lzw->old_ratio) {
                        /* the table is full; reset */
                        WRITE_CODE( RESET, out_buf, ox, lzw->bytes_out );

                        for (e = 0; e < TABLE_SIZE; e++) {
                            lzw->tbl[e].wK = FREE_ENTRY;
                        }
                        lzw->wKcode    = FIRST_CODE;
                        lzw->bytes_in  = lzw->bytes_out = 0;
                        lzw->old_ratio = 100;
                    } else {
                        lzw->old_ratio = new_ratio;
                    }
                }
                wK_found_or_added = TRUE;
            } else if (wK == lzw->tbl[hash_ix].wK) {
                /* wk is in the table already */
                lzw->wcode        = lzw->tbl[hash_ix].wKcode; /* wk -> w */
                wK_found_or_added = TRUE;
            } else {
                /* wk's code collides with an existing entry */
                hash_ix = REHASH( hash_ix, rehash_offset );
            }
        } 
        ix++;
    }

    *bytes_compressed = ix; /* return number of 'in_buf' bytes compressed     */
    *bytes_xferred    = ox; /* return number of bytes written to 'out_buf'    */

    return OKAY;
}

/* end compress */

/*
 * UNCOMPRESS
 *
 *    Uncompression works similarly to compression except in reverse.  It
 *    reads the string codes from the input and builds a string/code table.
 *    The table is build such that the string/code entry for a particular code
 *    is always present before the code is encountered in the input (there is
 *    an exception and Terry Welch's article explains this; it has to do with
 *    strings of the form KwKwK).  So, for each code there is a string in the 
 *    table that is represented by the code; this string is written to the 
 *    output as each code is processed from the input.  Also, given the way the
 *    table is constructed, the strings are produced in reverse order.  
 *    Therefore, the string characters are first placed in a stack and when 
 *    the string is complete it is transferred from the stack to the output 
 *    buffer.
 *
 * algorithm:
 *
 *    Initialize the table to contain all free entries
 *    Initialize the string stack to be empty
 *    Initialize the next code to FIRST_CODE
 *    Initialize new code, old code, final character to the first code in input
 *
 *    While there are codes to process and the output buffer is not full
 *       get new code from input
 *
 *       if new code is the RESET code then
 *          Reinitialize the table, stack and next
 *          go to the outer while loop (C continue)
 *       endif
 *
 *       new code -> temp code
 *
 *       if new code >= next code then
 *          (special case where the string is not yet in the table)
 *          push final character onto the stack
 *          old code -> temp code
 *       endif
 *
 *       while temp code > 255 do
 *          (construct the string from the table entries)
 *          push table[temp code].K onto the stack
 *          table[temp code].wcode -> temp code
 *       endwhile
 *
 *       temp code -> final character
 *       push final character onto the stack
 * 
 *       While the stack is not empty do
 *          pop the stack to output
 *       endwhile
 *
 *       add the current string to the table and increment the next code
 *
 *       new code -> old code
 *    endwhile
 *
 *    As with compression, several details with respect to input and output
 *    buffer handling have been left out for the sake of clarity (see the 
 *    code).
 *
 * Usage:
 *
 *   The following code example implements a very simple file uncompression
 *   program.  It illustrates how the uncompression routines are used.  Note
 *   that if a program uncompresses several files it must call start_uncompress
 *   and finish_uncompress for each file.
 *
 *      #include <stdio.h>
 *      #include <sys/file.h>
 *      #include "lzw.h"
 *      
 *      main( int agrc, char *argv[] )
 *         {
 *         char uncomp_buf[8192], char comp_buf[8192];
 *         int total_bytes_decoded, bytes_decoded, bytes_xferred, 
 *             uncomp_fd, comp_fd, rcnt;
 *      
 *      
 *         comp_fd = open( strcat( argv[1], ".ZZ" ), O_RDONLY );
 *         argv[1][strlen( argv[1] )-3] = '\0';
 *         uncomp_fd = creat( strcat( argv[1], ".UU" ) , 0660);
 *      
 *         init_uncompress();
 *         start_uncompress();
 *      
 *         while ((rcnt = read(comp_fd, comp_buf, 8192)) > 0)
 *            {
 *            total_bytes_decoded = 0;
 *      
 *            while (total_bytes_decoded < rcnt)
 *               {
 *               uncompress( &comp_buf[total_bytes_decoded], 
 *                           rcnt -total_bytes_decoded,
 *                           &uncomp_buf[0], 8192,
 *                           &bytes_decoded, &bytes_xferred );
 *               total_bytes_decoded += bytes_decoded;
 *               write( uncomp_fd, uncomp_buf, bytes_xferred );
 *               }
 *            }
 *      
 *         bytes_xferred = finish_uncompress( uncomp_buf );
 *         write( uncomp_fd, uncomp_buf, bytes_xferred );
 *         end_uncompress();
 *      
 *         close( comp_fd );
 *         close( uncomp_fd );
 *         }
 *      
 */

/* 
 * uncompress globals 
 */

/*
 * String Stack - As discussed in the algorthim above, when decompress 
 *    decodes a string code it reconstructs the corresponding string
 *    in reverse order.  To be able to output the string in correct order
 *    the string is decoded into a stack and when the string is fully
 *    decoded the stack is emptied to the output buffer.  The following
 *    definitions are for this stack and the corresponding stack 
 *    manipulation functions (defined as macros for speed efficiency).
 *    Also, see lzw_desc_t definition.
 *
 *    struct {
 *       char *chr;   * stack elements; characters                        *
 *       int   top;   * stack top index; the stack is empty when top == 0 *
 *    } Stk;
 */
static int my_exit( int ecode ) { exit( ecode ); return 0; }

#define STK_PUSH( c ) if (lzw->stk.top < MAX_STK_SIZE) \
                         lzw->stk.chr[lzw->stk.top++] = (c); \
                      else \
                         { \
                         fprintf( stderr, \
                             catgets(catd, S_LZW1, LZW1, "\n**** compression stack overflow ****\n") ); \
                         kill( getpid(), SIGQUIT ); \
                         }

#define STK_POP()\
   ((lzw->stk.top > 0) ? \
    lzw->stk.chr[--lzw->stk.top] : \
    (fprintf( stderr, catgets(catd, S_LZW1, LZW2, "\n**** compression stack underflow ****\n") ), \
     kill( getpid(), SIGQUIT )))

#define STK_EMPTY()   (lzw->stk.top <= 0)


/*
 * Function:
 *
 *      init_uncompress
 *
 * Function description:
 *
 *      This routine initializes the uncompress facility.  Specifically, 
 *      it allocates the string/code table and the string stack.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 *
 * Side effects:
 *
 *      Modifies global variables 'Stk' and 'Tbl'.
 */
int
init_uncompress(
                lzw_desc_handle_t *lzw_desc_h  /* in - lzw desc handle */
                )


{
    int e;
    lzw_desc_t *lzw;

    lzw = (lzw_desc_t *) malloc( sizeof( lzw_desc_t ) );
    if (lzw == NULL) { return ERROR; }

    lzw->new_uncompress = TRUE;
    lzw->reset = FALSE;

    lzw->tbl = (table_entry_t *) malloc( TABLE_SIZE * sizeof( table_entry_t ) );
    if (lzw->tbl == NULL) {
        free( lzw );
        return ERROR;
    }

    lzw->stk.chr = (char *) malloc( MAX_STK_SIZE );
    if (lzw->stk.chr == NULL) {
        free( lzw->tbl );
        free( lzw );
        return ERROR;
    }

    *lzw_desc_h = (lzw_desc_handle_t) lzw;

    return OKAY;
}

/* end init_uncompress */

/*
 * Function:
 *
 *      end_uncompress
 *
 * Function description:
 *
 *      This routine shuts down the uncompress facility.  Specifically
 *      it deallocates the string/code table and the string stack.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 *
 * Side effects:
 *
 *      Modifies global variables 'Stk' and 'Tbl'.
 */
int
end_uncompress(
               lzw_desc_handle_t lzw_desc_h  /* in - lzw desc handle */
               )


{
    lzw_desc_t *lzw;
    
    /*------------------------------------------------------------------------*/

    lzw = (lzw_desc_t *) lzw_desc_h;

    free( lzw->tbl ); 
    lzw->tbl = NULL;

    free( lzw->stk.chr ); 
    lzw->stk.chr = NULL;
    lzw->stk.top = 0;

    free( lzw );
    
    return OKAY;
}

/* end end_uncompress */

/*
 * Function:
 *
 *      start_uncompress
 *
 * Function description:
 *
 *      This routine sets up uncompression.  It initializes the string/code
 *      table and any other global variables used by uncompression.  This
 *      routine must be called before uncompressing a file or some other
 *      logical unit of data; finish_uncompression should be called when
 *      the file or unit of data has been uncompressed.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
int
start_uncompress(
                 lzw_desc_handle_t lzw_desc_h  /* in - lzw desc handle */
                 )
{
    int e;
    lzw_desc_t *lzw;

    /*------------------------------------------------------------------------*/

    lzw = (lzw_desc_t *) lzw_desc_h;

    lzw->new_uncompress = TRUE;
    lzw->reset = FALSE;

    for (e = 0; e < TABLE_SIZE; e++) {
        lzw->tbl[e].wK = FREE_ENTRY;
    }

    lzw->stk.top = 0;

    return OKAY;
}

/* end start_uncompress */

/*
 * Function:
 *
 *      finish_uncompress
 *
 * Function description:
 *
 *      This routines is used to get the last string (or portion of the
 *      last string) after a file or logical unit of data has been 
 *      uncompressed.
 *
 * Return value:
 *
 *      Number of bytes written to 'out_buf'.
 *
 * Side effects:
 *
 *      Modifies global variable 'Stk'.
 */
int
finish_uncompress(
                  lzw_desc_handle_t lzw_desc_h,  /* in - lzw desc handle */
                  char *out_buf           /* in - ptr to output buffer */
                  )
{
    int ox = 0;
    lzw_desc_t *lzw;

    /*------------------------------------------------------------------------*/

    lzw = (lzw_desc_t *) lzw_desc_h;

    while (!STK_EMPTY()) {
        out_buf[ox++] = STK_POP();
    }

    return ox;
}

/* end finish_uncompress */

/*
 * Function:
 *
 *      uncompress
 *
 * Function description:
 *
 *      This routine uncompresses data in 'in_buf' and writes strings
 *      to 'out_buf'.  When either all bytes in 'in_buf' are uncompressed
 *      or when 'out_buf' is full this routine returns to the caller and
 *      'bytes_decoded' will contain the number of bytes in 'in_buf'
 *      that were uncompressed and 'bytes_xferred' will contain the number
 *      of bytes written to 'out_buf'.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */
int
uncompress( 
           lzw_desc_handle_t lzw_desc_h,  /* in - lzw desc handle */
           char *in_buf,         /* in - ptr to input buffer */
           int   bytes,          /* in - byte size of input buffer */
           char *out_buf,        /* in - ptr to output buffer */
           int   out_buf_size,   /* in - byte size of output buffer */
           int  *bytes_decoded,  /* out - count of input bytes decoded */
           int  *bytes_xferred   /* out - count of output bytes written */
           )
{
    int 
      ix = 0,        /* 'in_buf' index                                     */
      ox = 0,        /* 'out_buf' index                                    */
      code,          /* temporary storage for 'new_code'                   */
      new_code = 0;  /* the current code read from input                   */

    lzw_desc_t *lzw;

    /*----------------------------------------------------------------------*/

    lzw = (lzw_desc_t *) lzw_desc_h;

    /* while the input buf is not empty and the output buf is not full */

    while ((ix < bytes) && (ox < out_buf_size)) {
        if (lzw->new_uncompress) {

            if (!lzw->reset) {
                /* this is the 1st buf so first code must be COMPRESSED_TAG */
                new_code = READ_CODE( in_buf, ix );
                if (new_code != COMPRESSED_TAG) {
                    fprintf( stderr, catgets(catd, S_LZW1, LZW3, "Data is not in compressed format.\n") );
                    return ERROR;
                }
            }

            lzw->new_uncompress = FALSE;
            lzw->reset          = FALSE;

            lzw->next_code = FIRST_CODE;
            new_code       = READ_CODE( in_buf, ix );
            out_buf[ox++]  = lzw->old_code = lzw->fin_char = new_code;
        } else {
            /* 
             * starting on a new input or output buf;
             * empty the stack if needed
             */
            while (!STK_EMPTY() && (ox < out_buf_size)) {
                out_buf[ox++] = STK_POP();
            }
        }

        /* while the input buf is not empty and the output buffer is not full */

        while ((ix < bytes) && (ox < out_buf_size)) {
            new_code = READ_CODE( in_buf, ix );

            if (new_code == RESET) {
                /* encountered a RESET code so re-initialize everything    */
                start_uncompress( lzw_desc_h ); /* reset the table         */
                lzw->reset = TRUE;
                break;                          /* exit to outer while loop*/
            }

            code = new_code; /* copy to temp code */

            if (code >= lzw->next_code) {
            /*
             * encountered the anomolous case of KwKwK; so the previous final 
             * character is last character for the current string and the code
             * for the current string is the same as the code of the previous 
             * string (ie- Kw was the previous string and we are now doing KwK
             * and KwK is not yet in the table)
             */
                STK_PUSH( lzw->fin_char );
                code = lzw->old_code;
            }

            while (code > MAX_BYTE_CODE) {
            /* decode to the final char. (characters are in reverse order */
                STK_PUSH( GET_K( lzw->tbl[code].wK ));
                code = GET_w( lzw->tbl[code].wK );
            }

            lzw->fin_char = code;
            STK_PUSH( lzw->fin_char );

            while (!STK_EMPTY() && (ox < out_buf_size)) {
            /* now output the string in correct order */
                out_buf[ox++] = STK_POP();
            }
            if (lzw->next_code <= MAX_CODE) {
            /* add wK to table if it isn't full */
                lzw->tbl[lzw->next_code++].wK = 
                                MAKE_wK( lzw->old_code, lzw->fin_char ); 
            }
            lzw->old_code = new_code;
        }
    }

    *bytes_decoded = ix;
    *bytes_xferred = ox;

    return OKAY;
}

/* end uncompress */

/* end lzw.c */
