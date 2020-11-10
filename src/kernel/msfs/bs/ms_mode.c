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
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: ms_mode.c,v $ $Revision: 1.1.27.4 $ (DEC) $Date: 2000/04/26 12:56:07 $"

/*
 * ms_mode.c - provides function hooks so that AdvFS may be compiled
 * in user mode or in kernel mode.
 */

#include <msfs/ms_public.h>
#include <msfs/ms_assert.h>

#ifdef KERNEL
#include <sys/syslog.h>
#include <sys/limits.h>
#else
#include <stdlib.h>
#ifndef _OSF_SOURCE
#include <msfs/bs_extern.h>
#endif /* _OSF_SOURCE */
#endif /* KERNEL */

extern unsigned TrFlags;

#ifdef KERNEL

char *
kalloc();

struct msHdr {
    unsigned int magic;
    unsigned int size;
};

#endif /* KERNEL */

#define ADVFS_MODULE MS_MODE

#define ALLOC_MAGIC 0xa578

void
mem_trace(
          char *ftype,   /* in */
          int size,      /* in */
          int ln,        /* in */
          char *fn       /* in */
          )
{
    char *ptr;
    trace_hdr();

    if (!(ptr = (char *)strrchr( fn, '/' ))) return;
#ifdef KERNEL
    log( LOG_MEGASAFE | LOG_INFO,
#else
    ms_printf( 
#endif
        "%6s( %8d )  %4d:%s\n", ftype, size, ln, &ptr[1] );
}

char *
_ms_malloc(
           unsigned size,       /* in */
           int ln,              /* in */
           char *fn,            /* in */
           int flag,            /* in */
           int rad_id           /* in */
           )
{
#ifdef KERNEL
    struct msHdr *hdr;
    long amt = size + sizeof( struct msHdr );
    if (flag & M_WAITOK) {
        MS_SMP_ASSERT(!SLOCK_COUNT);
    }
    MS_SMP_ASSERT(size != 0);

    if (flag & (M_INSIST | M_PREFER)) {
        RAD_MALLOC(hdr,struct msHdr *,amt,M_ADVFS,flag, rad_id);
    }
    else {
        MALLOC_VAR(hdr,struct msHdr *,amt,M_ADVFS,flag);
    }

    if( hdr == (struct msHdr *)NULL ) {
        return( (char *)NULL );
    }

    bzero( (char *)hdr, amt );
    hdr->magic = ALLOC_MAGIC;
    hdr->size = amt;

    if ( TrFlags & trMem )
         mem_trace( "malloc", amt, ln, fn );

    return( (char *)hdr + sizeof( struct msHdr ) );
#else
    return( malloc( size ) );
#endif /* KERNEL */
}

void
_ms_free(
         void *ptr,
         int ln,              /* in */
         char *fn             /* in */

         )
{
#ifdef KERNEL
    struct msHdr *hdr;
    int size;

    if( ptr == 0 )
        return;
    
    hdr = (struct msHdr *)((char *)ptr - sizeof( struct msHdr ));

    MS_SMP_ASSERT( hdr->magic == ALLOC_MAGIC );
	MS_SMP_ASSERT( hdr->size != 0xdeadbeef );  /* already freed? */

    if ( TrFlags & trMem )
         mem_trace( "free  ", hdr->size, ln, fn );

    size = hdr->size;
    hdr->size = 0xdeadbeef;

    FREE(hdr,M_ADVFS);

#else
    free( ptr );


#endif /* KERNEL */
}


#ifdef KERNEL

long int
atol( char *nptr )
{
    register long n;
    register int sign;

    n = 0;
    sign = 1;
    for(;;nptr++) {
        switch(*nptr) {
        case ' ':       /* check for whitespace */
        case '\t':
        case '\f':
        case '\n':
        case '\v':
        case '\r':
            continue;
        case '-':
            sign = -1;
        case '+':
            nptr++;
        }
        break;
    }
    while(*nptr >= '0' && *nptr <= '9') {
          /*
        Check for overflow.  If the current value (before
        adding current digit) is already greater than
            INT_MAX / 10, we know that another digit will
        not fit.  Also if after the current digit is added,
            if the new value is less than the old value, we 
        know that overflow will occur.
              */
        if (((n * 10 + *nptr - '0') < n) || (n > (INT_MAX /10))) {
            if (sign == 1)
                n = INT_MAX;
            else
                n = INT_MIN;
            panic( "atol: integer overflow" );
            return(n);
        }
        n = n * 10 + *nptr++ - '0';
    }
    n *= sign;
    return (n);
}
#endif /* KERNEL */

int
ms_copyin( void *src, void *dest, int len )
{
#ifdef KERNEL
    return copyin( src, dest, len );
#else
    bcopy( src, dest, len );
    return 0;
#endif
}


int
ms_copyout( void *src, void *dest, int len )
{
#ifdef KERNEL
    return copyout( src, dest, len );
#else
    bcopy( src, dest, len );
    return 0;
#endif
}
/*
 * 
 * What to fix in mounting (bs_vd_mount.c)
 *
 * The routine bs_vd_mount opens a virtual disk that used to be the
 * file system. This has been ifndef'd out. Also, bs_vd_dismount
 * closed the same virtual file. Ifndef's out also.
 *
 * The routine open_fstab opened the pfstab file, and close_fstab
 * closed the same file. The open and close are commented out. We will
 * have to decide how to read this file. The routine get_fstab_ent
 * preformed an fgets to read each entry. and used strtok and strtol
 * to format the entries. This entire routine has been commented out.
 *
 * Startup of the I/O thread was also removed from bs_vd_mount.c
 * as well as the associated cma def's.
 *
 *
 * In the bs_ods_init.c code, the disk is no longer "opened."
 *
 */

/*
 *
 * Routines that use read and write need to change
 *
 * bs_od_io.c       write_raw_bmt_page()
 *                  read_raw_bmt_page()
 *                  write_raw_sbm_page()
 *
 * bs_qio.c         bs_qtodev()
 *
 */

/* 
 * All calls to do tracing have been ifdef'd out from the code.
 * A more general messaging interface will need to be created.  Locations
 * of the trace calls are as follows:
 *
 * bs_access.c      bs_close
 *                  ftbs_access (2)
 *
 * bs_create.c      ftbs_create (2)
 *
 * bs_bmt_util.c
 *
 * ftx_routines.c   ftx_start
 *                  ftx_done (7)
 *                  ftx_fail
 *                  ftbx_pinpg
 *
 * bs_buffer.c      bfr_trace
 *
 * bs_qio.c         dev_trace
 *
 * ms_generic_locks_code.h  trace_hdr
 */

/*
 * All code for locks is nicely isolated in ms_generic_locks.h
 * and ms_generic_locks_code.h (thanks Pete!).  Currently,
 * all calls to cma or pthreads code is simply ifdef'd out
 * until we find what kernel facilities are available.
 * 
 */

/*
 * The disk statistics routines will need to be changed.
 * They are located in bs_dstat.c and have been ifdef'd
 * out.
 */

/*
 * task_getrusage was ifdef'd out of the code in the 
 * bs_misc.c file.
 */

/*
 * a gettimeofday was ifdef'd out of the ftx_routines.c file.
 * It should be replaced with the equivalent kernel code.
 */

/*
 * bs_vd_mount.c - all printing to stderr has been ifdef'd out,
 * this stuff should go to some error log if its an error but
 * not serious enough to cause a panic.
 */
