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
 *      Backup utility routines.
 *
 * Date:
 *
 *      Thu Apr 19 14:09:09 1990
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: util.c,v $ $Revision: 1.1.53.1 $ (DEC) $Date: 2003/01/06 22:45:39 $";
#endif


#include "backup.h"
#include "util.h"
#include <stdarg.h>
#include <sys/signal.h>
#include <stdio.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/mtio.h>
#include <sys/ioctl.h>
#include <io/common/devio.h>
#include <io/common/devgetinfo.h>
#include <sys/mtio.h>
#include "libvdump_msg.h"

extern t_mutex tty_mutex;

static advfs_ev advfs_event;
static char *advfs_setName=NULL; 
static char advfs_dmnName[MNAMELEN];
static int posted_lock=0;

char    os_name[OSNAMEBUFSIZE];         /* name of remote os */

/*
 * abort_prog - prints message, signals SIGQUIT
 */
void
abort_prog(
          char *msg, ...
          )
{
    va_list ap;
    
    va_start( ap, msg );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL1, "\n\n************* PROGRAM ABORT **************\n\n") );
    vfprintf( stderr, msg, ap );
    fprintf( stderr, "\n\n" );
    va_end( ap );
    fflush( stderr );

    post_event_backup_error(Prog);	
    if(Debug)
	kill( getpid(), SIGQUIT ); 
    signal(SIGQUIT,SIG_DFL);
    exit(3);
}

char *
get_name(
    char *full_name
    )
{
    char *name;

    if ((name = strrchr( full_name, '/' )) == NULL) {
         name = full_name;
    } else {
         name++;
    }

    return name;
}

void 
get_fs_info(
    char *path,
    fs_info_t *fs_info,
    int backup_subtree,
    int verbose
    )
{
    struct statfs fs_stats;
    extern char *mnt_names[];
    int ret;

#define verbosefprintf  if (verbose) fprintf

    if (path == NULL) {
        fprintf( stderr, 
                 catgets(comcatd, S_UTIL1, UTIL2, "%s: *internal error* missing path name in get_fs_info()\n"),
                 Prog );
        exit( 1 );
    }

    ret = statfs( path, &fs_stats );

    if (ret < 0) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL3, "%s: error accessing file system <%s>; [%d] %s\n"),
                 Prog, path, errno, ERR_MSG );
        exit( 1 );
    }

    if (backup_subtree) {
        verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL4, "path     : %s\n"), path );
    } else {
        verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL4, "path     : %s\n"), fs_stats.f_mntonname );
    }
    verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL5, "dev/fset : %s\n"), fs_stats.f_mntfromname );

    if (mnt_names[ fs_stats.f_type ] != NULL) {
        verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL6, "type     : %s\n"), mnt_names[ fs_stats.f_type ] );
    } else {
        verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL7, "type     : unknown\n") );
    }

    if (fs_stats.f_type == MOUNT_MSFS) {
        verbosefprintf( stderr, catgets(comcatd, S_UTIL1, UTIL8, "advfs id : 0x%08x.%08x.%x\n"), 
                 fs_stats.mount_info.msfs_args.id.id1, 
                 fs_stats.mount_info.msfs_args.id.id2,
                 fs_stats.mount_info.msfs_args.id.tag );
        fs_info->advfs_set_id.domainId.tv_sec = fs_stats.mount_info.msfs_args.id.id1;
        fs_info->advfs_set_id.domainId.tv_usec = fs_stats.mount_info.msfs_args.id.id2;
        fs_info->advfs_set_id.dirTag.num = fs_stats.mount_info.msfs_args.id.tag;
        fs_info->advfs_set_id.dirTag.seq = 0;  
    }
    fs_info->fs_type = fs_stats.f_type;
    fs_info->quotas_on = (fs_stats.f_flags & M_QUOTA) ? TRUE : FALSE;
    strcpy( fs_info->path, fs_stats.f_mntonname );
    strcpy( fs_info->filesys, fs_stats.f_mntfromname);
}

/*
 * xor_bufs - XORs the contents of two buffers of length 'len' bytes.
 * the result is 'returned' in 'buf1'.
 */
void
xor_bufs(
         u_char *buf1,  /* in/out - ptr to buffer */
         u_char *buf2,  /* in - ptr to another buffer */
         int len        /* in - number of bytes to xor */
         )
{
    int i;
    
    for (i = 0; i < len; i++) {
        buf1[i] ^= buf2[i];
    }
}

/* end xor_bufs */

/* #ifdef ULTRIX  */
void
dump_devstat( struct devget *devstat )

   /*
    *
    * Function:
    *
    *      dump_devstat
    *
    * Function description:
    *
    *      Writes to stdout several device stat buffer fields. Used
    *      for debugging only.
    *
    * Arguments:
    *
    *      devstat (in)
    *          - device stat buffer
    *
    * Return value:
    *
    *      None
    *
    * Side effects:
    *
    *      None
    *
    */

{
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL13, "devstat\n-------\n") );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL14, "   category : %d\n"), devstat->category );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL15, "   interface: %s\n"), devstat->interface );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL16, "   device   : %s\n"), devstat->device );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL17, "   unit     : %d\n"), devstat->unit_num );
    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL18, "   stat     : 0x%x\n"), devstat->stat );
}

/* end dump_devstat */
/* #endif */

u_int 
crc16_by_nibble( u_char *data, u_int data_len )

   /*
    *
    * Function:
    *
    *   crc16_by_nibble
    *
    * Function description:
    *
    *   This function calculates the CRC-16 CRC code for a block of data 
    *   referenced by "*data".  The length of the data is in "data_len".
    *
    *   The algorithm was derived from the description of the VAX CRC
    *   instruction in the VAX-11 Architecture Reference Manual, Revision
    *   6.1, ppg 4-171 to 4-174.  This function calculates the CRC a
    *   nibble at a time; see the function 'crc16' for a faster implementation
    *   which does the calculation a byte at a time.
    *
    * Arguments:
    *
    *      None
    *
    * Return value:
    *
    *      The CRC16 code for the data.
    *
    * Side effects:
    *
    *      None
    *
    */

{
    u_int crc, loop_cnt;

    /*
       "crc_table" contents were obtained from the lib$crc_table VMS
       runtime library routine.  The polynomial is 0xa001.
    */

    static u_short crc_table[16] = 
        {
        0x0000, 0xcc01, 0xd801, 0x1400, 0xf001, 0x3c00, 0x2800, 0xe401,
        0xa001, 0x6c00, 0x7800, 0xb401, 0x5000, 0x9c01, 0x8801, 0x4400
        };

    /*......................................................................*/

    crc = 0;                   /* initialize CRC to zero */

    while (data_len > 0) {      /* process each byte one at a time */

        crc = crc ^ *data;     /* XOR a byte into bits <7:0> of CRC */

        /* 
           for each 4 bits (ie- loop twice), right shift the CRC 4 bits
           and XOR this with the CRC table entry indexed by the 4 bits
           shifted off the CRC 
        */

        for (loop_cnt = 1; loop_cnt <= 2; loop_cnt++) {
            crc = (crc >> 4) ^ crc_table[crc & 0x0000000f];
        }

        data_len--;            /* decrement number bytes to process */
        data++;                /* go to next byte */
    }

    return (crc & 0x0000ffff); /* return 16 bit CRC */
}
    
/* end crc16_by_nibble */

/* #define CRC 0xa001 */

u_short 
crc16( register u_char *s1, register int n )

   /*
    *
    * Function:
    *
    *   crc16
    *
    * Function description:
    *
    *   This function calculates the CRC-16 CRC code for a block of data 
    *   referenced by "*s1".  The length of the data is in "int".  The
    *   CRC code is calculated a byte at a time.  The source code for
    *   this routine was generated using a program that creates routines
    *   for calculating CRC codes.
    *
    * Arguments:
    *
    *      None
    *
    * Return value:
    *
    *      The CRC16 code for the data.
    *
    * Side effects:
    *
    *      None
    *
    */

{
    register unsigned short accum;

    static unsigned short crc_tbl[256] = 
      {
        0x0000, 0xc0c1, 0xc181, 0x0140, 0xc301, 0x03c0, 0x0280, 0xc241,
        0xc601, 0x06c0, 0x0780, 0xc741, 0x0500, 0xc5c1, 0xc481, 0x0440,
        0xcc01, 0x0cc0, 0x0d80, 0xcd41, 0x0f00, 0xcfc1, 0xce81, 0x0e40,
        0x0a00, 0xcac1, 0xcb81, 0x0b40, 0xc901, 0x09c0, 0x0880, 0xc841,
        0xd801, 0x18c0, 0x1980, 0xd941, 0x1b00, 0xdbc1, 0xda81, 0x1a40,
        0x1e00, 0xdec1, 0xdf81, 0x1f40, 0xdd01, 0x1dc0, 0x1c80, 0xdc41,
        0x1400, 0xd4c1, 0xd581, 0x1540, 0xd701, 0x17c0, 0x1680, 0xd641,
        0xd201, 0x12c0, 0x1380, 0xd341, 0x1100, 0xd1c1, 0xd081, 0x1040,
        0xf001, 0x30c0, 0x3180, 0xf141, 0x3300, 0xf3c1, 0xf281, 0x3240,
        0x3600, 0xf6c1, 0xf781, 0x3740, 0xf501, 0x35c0, 0x3480, 0xf441,
        0x3c00, 0xfcc1, 0xfd81, 0x3d40, 0xff01, 0x3fc0, 0x3e80, 0xfe41,
        0xfa01, 0x3ac0, 0x3b80, 0xfb41, 0x3900, 0xf9c1, 0xf881, 0x3840,
        0x2800, 0xe8c1, 0xe981, 0x2940, 0xeb01, 0x2bc0, 0x2a80, 0xea41,
        0xee01, 0x2ec0, 0x2f80, 0xef41, 0x2d00, 0xedc1, 0xec81, 0x2c40,
        0xe401, 0x24c0, 0x2580, 0xe541, 0x2700, 0xe7c1, 0xe681, 0x2640,
        0x2200, 0xe2c1, 0xe381, 0x2340, 0xe101, 0x21c0, 0x2080, 0xe041,
        0xa001, 0x60c0, 0x6180, 0xa141, 0x6300, 0xa3c1, 0xa281, 0x6240,
        0x6600, 0xa6c1, 0xa781, 0x6740, 0xa501, 0x65c0, 0x6480, 0xa441,
        0x6c00, 0xacc1, 0xad81, 0x6d40, 0xaf01, 0x6fc0, 0x6e80, 0xae41,
        0xaa01, 0x6ac0, 0x6b80, 0xab41, 0x6900, 0xa9c1, 0xa881, 0x6840,
        0x7800, 0xb8c1, 0xb981, 0x7940, 0xbb01, 0x7bc0, 0x7a80, 0xba41,
        0xbe01, 0x7ec0, 0x7f80, 0xbf41, 0x7d00, 0xbdc1, 0xbc81, 0x7c40,
        0xb401, 0x74c0, 0x7580, 0xb541, 0x7700, 0xb7c1, 0xb681, 0x7640,
        0x7200, 0xb2c1, 0xb381, 0x7340, 0xb101, 0x71c0, 0x7080, 0xb041,
        0x5000, 0x90c1, 0x9181, 0x5140, 0x9301, 0x53c0, 0x5280, 0x9241,
        0x9601, 0x56c0, 0x5780, 0x9741, 0x5500, 0x95c1, 0x9481, 0x5440,
        0x9c01, 0x5cc0, 0x5d80, 0x9d41, 0x5f00, 0x9fc1, 0x9e81, 0x5e40,
        0x5a00, 0x9ac1, 0x9b81, 0x5b40, 0x9901, 0x59c0, 0x5880, 0x9841,
        0x8801, 0x48c0, 0x4980, 0x8941, 0x4b00, 0x8bc1, 0x8a81, 0x4a40,
        0x4e00, 0x8ec1, 0x8f81, 0x4f40, 0x8d01, 0x4dc0, 0x4c80, 0x8c41,
        0x4400, 0x84c1, 0x8581, 0x4540, 0x8701, 0x47c0, 0x4680, 0x8641,
        0x8201, 0x42c0, 0x4380, 0x8341, 0x4100, 0x81c1, 0x8081, 0x4040
      };

    /*-----------------------------------------------------------------------*/

    for( accum=0; n; --n ) {
        /*
         * This is the heart of the CRC calculation.  The CRC
         * accumulator is shifted 8 bits and the next byte from
         * the s2 string is inserted into the accumulator.  The
         * byte which was shifted out of the accumulator is used
         * as an index into the 256 entry CRC table above.  The
         * value from the CRC table is exclusived OR'ed with the
         * shifted accumulator and s2 byte.
         *  This was incorported into one line of 'C' code so that
         * the compiler could optimize it to its hearts content.
         * If your compiler can do better by breaking it down, then
         * have fun, but I would suggest testing the modified code
         * against an unmodified version.
         */
        accum = (((unsigned short)*s1++ << 8) | 
                 ((accum >> 8) & 0xff)) ^ crc_tbl[accum & 0xff];
    }
    /*
     * Flush last byte of data through the entire
     * 16 bit CRC accumulator by forcing the equivalent
     * of 2 bytes of zero through the accumulator.
     */
    accum = ((accum >> 8) & 0xff) ^ crc_tbl[accum & 0xff];
    accum = ((accum >> 8) & 0xff) ^ crc_tbl[accum & 0xff];

    return( accum );                                /* Return the 16 bit CRC */
}

/* end crc16 */


#define HOURS( sec ) ((int) (sec) / 3600) 
#define MINUTES( sec ) ((int) (sec) / 60 - HOURS( sec ) * 60)
#define HMS( sec ) \
              HOURS( sec ), \
              MINUTES( sec ), \
              (sec) - MINUTES( sec ) * 60 - HOURS( sec ) * 3600 

/* 
 * show_statistics - Displays backup/restore statistics (CPU usage,
 * bytes read, bytes written, etc).
 */
void
show_statistics ( 
   unsigned long bytes_read, 
   unsigned long bytes_written, 
   unsigned long file_cnt, 
   struct timeval start_time
   )
{
   int unit;
   struct rusage resource_usage, child_usage;
   struct timezone tz;
   struct timeval end_time;
   double total_time;

#if defined( _OSF_SOURCE) && defined (_THREADS_)
   task_getrusage( &resource_usage );
#else
   getrusage( RUSAGE_SELF, &resource_usage );
#endif
   gettimeofday( &end_time, &tz );

   total_time = resource_usage.ru_utime.tv_sec +
                resource_usage.ru_stime.tv_sec +
                (double) resource_usage.ru_utime.tv_usec / 1000000.0 +
                (double) resource_usage.ru_stime.tv_usec / 1000000.0;

#if !defined( _OSF_SOURCE) || !defined(_THREADS_)
   getrusage( RUSAGE_CHILDREN, &child_usage );

   total_time = total_time + child_usage.ru_utime.tv_sec +
                child_usage.ru_stime.tv_sec +
                (double) child_usage.ru_utime.tv_usec / 1000000.0 +
                (double) child_usage.ru_stime.tv_usec / 1000000.0;
#endif

   fprintf( stderr, 
            catgets(comcatd, S_UTIL1, UTIL19, "\n     CPU Time :  %02i:%02i:%09.6f  (Usr: %02i:%02i:%09.6f,  Sys: %02i:%02i:%09.6f)"), 
            HMS( total_time ), 
            HMS( (double) resource_usage.ru_utime.tv_sec + 
                 (double) resource_usage.ru_utime.tv_usec / 1000000.0 ),
            HMS( (double) resource_usage.ru_stime.tv_sec +
                 (double) resource_usage.ru_stime.tv_usec / 1000000.0 ) );

   total_time = (end_time.tv_sec + (double) end_time.tv_usec / 1000000.0) -
              (start_time.tv_sec + (double) start_time.tv_usec / 1000000.0);

   fprintf( stderr, 
            catgets(comcatd, S_UTIL1, UTIL20, "\n Elapsed Time :  %02i:%02i:%09.6f\n"), HMS( total_time ) );

   fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL21, "\n Files Processed :  %9li"), file_cnt );
   fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL22, "\n      Bytes Read :  %9li,        Bytes Written : %9li"), 
           bytes_read, bytes_written );

   fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL23, "\n  Bytes/Sec Read :  %9li,    Bytes/Sec Written : %9li\n"),
           (long) (bytes_read / total_time), 
           (long) (bytes_written / total_time) );
}

/*
 * op_retry()
 *
 * Prompts the user to see if they want to retry an operation.
 * Returns 1 if the user responed with a Y or y.  Otherwise
 * 0 is returned.
 */

int
op_retry( void )
{
    char request[10];
    char *resp = NULL;

    /* If the user can't respond, skip the query; assume no retry */
    if (!Local_tty) 
        return 0;

    do {
        mutex__lock( tty_mutex );
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL28, "%s: do you want to retry? "), Prog );
        /*
         * We need an answer from the user.  If there is no way to
         * get one (Local_tty is NULL), then abort the job.  Otherwise,
         * it may be that Local_tty is still initialized to stdin but
         * a read on stdin may not be possible.  This would be the
         * case if we were running through cron.  Therefore, it is
         * crucial to check the return value from fgets().
         */
        if ((resp = fgets (request, 6, Local_tty)) == NULL) {
            request[0] = 'n';
        }
        mutex__unlock( tty_mutex );
    } while ((resp = strchr( "yYnN", request[0] )) == NULL);

    if (strchr( "yY", request[0] )) {
        return (1);
    }

    return (0);
}

/*
 * Given the arguments for the utility, this function determines whether
 * the command arguments are in POSIX or BSD style.
 */

int
posix_style(int argc, char *argp[])
{
    int i;

    for( i = 1; i < argc; i++ ) {
        /* look for -option and avoid "-" as default output file */
        if ( argp[i][0] == '-' && argp[i][1] != '\0' )
            return(1);               /* POSIX style */
        }
        return(0);                   /* bsd style */
}

extern int Silent_mode;

void
silent_fprintf(char *msg, ...)
{
    va_list ap;

    va_start( ap, msg );

    if (!Silent_mode) {
	if (!strcmp( Prog,"vrestore")) {
		vfprintf(stdout, msg, ap);
	} else {
	 	vfprintf(stderr, msg, ap);
	}
    }

    va_end(ap);
    fflush( stderr );
}

/*
 *  The following routine display data fromth the devgetinfo data structure.
 */
 void
PrintStatus( v1_device_info_t *p_info )
{

    /*
     *  This function assumes a tape device.
     *
     *  NOTE:  the text really should be TPDEV_* not DEV_*
     */
    fprintf( stderr, "                        " );
    if( p_info->devinfo.tape.media_status & TPDEV_BOM ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL126, "DEV_BOM ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_EOM ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL127, "DEV_EOM ") );
        }
    if( p_info->devinfo.tape.unit_status & TPDEV_OFFLINE ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL128, "DEV_OFFLINE ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_WRTPROT ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL129, "DEV_WRTLCK ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_BLANK ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL130, "DEV_BLANK ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_WRITTEN ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL131, "DEV_WRITTEN ") );
        }
#if 0 /* !!!! NOTE: there is no equivalent for CSE in the new info data */
    if( p_info->devinfo.tape.media_status & TPDEV_CSE ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL132, "DEV_CSE ") );
        }
#endif /* 0 */
    if( p_info->devinfo.tape.media_status & TPDEV_SOFTERR ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL133, "DEV_SOFTERR ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_HARDERR ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL134, "DEV_HARDERR ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_DONE ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL135, "DEV_DONE ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_RETRY ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL136, "DEV_RETRY ") );
        }
    if( p_info->devinfo.tape.media_status & TPDEV_ERASED ) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL137, "DEV_ERASED ") );
        }


    return;
    }

/*
 * Display the contents of the mtget struct.
 * Args: fd a file descriptor of the already opened tape device.
 */
print_mtio(fd)
	int fd;
{
	struct mtget mt;
	if (ioctl(fd, MTIOCGET, (char *)&mt) < 0) {
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL35, "\nmtiocget ioctl failed!\n"));
		exit(1);
	}
	fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL36, "\nMTIOCGET ELEMENT	CONTENTS"));
	fprintf( stderr,"\n----------------	--------\n");
	fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL37, "mt_type			"));
	switch(mt.mt_type) {
	case MT_ISTS:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL38, "MT_ISTS\n"));
		break;
	case MT_ISHT:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL39, "MT_ISHT\n"));
		break;
	case MT_ISTM:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL40, "MT_ISTM\n"));
		break;
	case MT_ISMT:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL41, "MT_ISMT\n"));
		break;
	case MT_ISUT:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL42, "MT_ISUT\n"));
		break;
	case MT_ISTMSCP:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL43, "MT_ISTMSCP\n"));
		break;
	case MT_ISST:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL44, "MT_ISST\n"));
		break;
	case MT_ISSCSI:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL45, "MT_ISSCSI\n"));
		break;
	default:
		fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL46, "Unknown mt_type = 0x%x\n"),mt.mt_type);
	}
	fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL47, "mt_dsreg		%X\n"), mt.mt_dsreg);
	fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL48, "mt_erreg		%X\n"), mt.mt_erreg);
	fprintf( stderr,catgets(comcatd, S_UTIL1, UTIL49, "mt_resid		%X\n"), mt.mt_resid);
	fprintf( stderr,"\n");
}


/*
 * buf_of_zeros
 *
 * Returns TRUE if the buffer contains all zeros, FALSE otherwise.
 */

int
buf_of_zeros(
    unsigned long *buf,
    int buf_size        /* number longs in buffer */
    )
{
    int i;

    for (i = 0; i < buf_size; i++ ) {

        if (buf[i] != 0L) {
            return FALSE;
        }
    }

    return TRUE;
}

/*
 * toke_it	< Taken from bs/bs_misc.c >
 *
 * Copies the portion of "str" up to the matching "tokchar" into
 * "token", terminating "token" with a NULL and returning a pointer to
 * the character beyond the matching tokchar in str.  If "tokchar" is
 * not found in "str", NULL is returned.
 *
 * "token" must be large enough to contain the token.
 * either "tokchar" must be in "str" or "str" must have a NULL to
 * terminate the loop.
 */

char*
toke_it(
        char* str,      /* in - string to scan */
        char  tokchar,  /* in - token end character */
        char* token)    /* out - token found */
{
    char c;

    if ( !str ) {
        return NULL;
    }

    while ( c = (*token++ = *str++) ) {
        if ( c == tokchar ) {
            *(token - 1) = NULL;
            return str;
        }
    }

    return NULL;
}


int
post_event_backup_lock(char *Prog)
{
    if (Fs_info.fs_type == MOUNT_MSFS) {
    	advfs_setName = toke_it( Fs_info.filesys, '#', advfs_dmnName);

        init_event(&advfs_event);
        advfs_event.domain = advfs_dmnName;
        advfs_event.fileset = advfs_setName;
        advfs_post_user_event(EVENT_FSET_BACKUP_LOCK, advfs_event, Prog);

        posted_lock = 1;
    }
    return 0;
}

int
post_event_backup_unlock(char *Prog)	
{
    if (Fs_info.fs_type == MOUNT_MSFS) {
        if (posted_lock) {
            advfs_post_user_event(EVENT_FSET_BACKUP_UNLOCK, advfs_event, Prog);
        }	
        posted_lock = 0;
    }
    return 0;
}

int
post_event_backup_error(char *Prog)	
{
    if (Fs_info.fs_type == MOUNT_MSFS) {
        if (posted_lock) {
            advfs_post_user_event(EVENT_FSET_BACKUP_ERROR, advfs_event, Prog);
        }	
        posted_lock = 0;
    }
    return 0;
}


/* end util.c */
