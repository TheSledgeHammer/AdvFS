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
 * Abstract:
 *
 *      AdvFS Backup utility routines.
 *
 *
 */

#include <advfs/backup.h>
#include <advfs/util.h>
#include <stdarg.h>
#include <sys/signal.h>
#include <stdio.h>
#include <ctype.h>
#include <sys/types.h>
#include <mntent.h>
#include <sys/mtio.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <advfs/advfs_evm.h>
#include <dlfcn.h>
#include "libvdump_msg.h"

extern pthread_mutex_t tty_mutex;
extern char *sys_errlist[];

/*
 * Used for EVM routine, advfs_post_user_event
 */
static EvmStatus_t( *dl_EvmEventCreateVa )( EvmEvent_t*, ...);
static EvmStatus_t( *dl_EvmVarSet )( EvmEvent_t, EvmVarName_t, EvmVarType_t,
                    const EvmVarValue_t, EvmI18NMsgId_t, EvmSize_t );
static EvmStatus_t( *dl_EvmConnCreate )( EvmConnectionType_t, EvmResponseMode_t,
                    const EvmTransport_t*, EvmCallback_t, EvmCallbackArg_t,
                    EvmConnection_t* );
static EvmStatus_t( *dl_EvmEventPost ) ( EvmConnection_t, EvmEvent_t );
static EvmStatus_t( *dl_EvmConnDestroy ) ( EvmConnection_t );
static EvmStatus_t( *dl_EvmEventDestroy) ( EvmEvent_t );

static advfs_ev advfs_event;
static int posted_lock=0;

char*  defaultSetName = "default";
extern int Debug;


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
    fprintf( stderr, catgets(catd, S_UTIL1, UTIL1, 
        "\n\n************* PROGRAM ABORT **************\n\n") );
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

void 
get_fs_info(
    char *path,
    fs_info_t *fs_info,
    int backup_subtree,
    int verbose
    )
{
    struct statvfs fs_stats, mnt_stats;
    extern char *mnt_names[];
    int ret;
    int found = 0;
    struct mntent*   mnt;
    FILE*            mnttab = NULL;
    bfSetIdT         advfsid;

#define verbosefprintf  if (verbose) fprintf

    if (path == NULL) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL2, 
            "%s: *internal error* missing path name in get_fs_info()\n"),
            Prog );
        exit( 1 );
    }

    ret = statvfs( path, &fs_stats );

    if (ret < 0) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL3, 
            "%s: error accessing file system <%s>; [%d] %s\n"),
            Prog, path, errno, ERR_MSG );
        exit( 1 );
    }

    /*  loop through /etc/mnttab and get the filesystem information. */

    mnttab = setmntent(MNT_MNTTAB, "r");
    if (mnttab == NULL) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL168, 
            "%s: could not open %s\n"), Prog, MNT_MNTTAB);
        exit(1);
    }

    while (mnt = getmntent(mnttab)) {
        ret = statvfs(mnt->mnt_dir, &mnt_stats);
        if (ret != 0) {
            continue;   /*  should never happen, but could be a race 
                         *  condition with a filesystem being unmounted. 
                         */
        }
        if (mnt_stats.f_fsid == fs_stats.f_fsid) {
            found++;
            break;
        }
    }
    
    endmntent(mnttab);

    if (!found) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL3, 
            "%s: error accessing file system <%s>; [%d] %s\n"),
            Prog, path, errno, ERR_MSG );
        exit( 1 );
    }
        
    if (backup_subtree) {
        verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL4, 
            "path     : %s\n"), path );
    } else {
        verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL4, 
            "path     : %s\n"), mnt->mnt_dir);
    }
    verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL5, 
            "dev/fset : %s\n"), mnt->mnt_fsname);

    if (mnt->mnt_type != NULL) {
        verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL6, 
            "type     : %s\n"), mnt->mnt_type);
    } else {
        verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL7, 
            "type     : unknown\n") );
    }

    if (strcmp(mnt->mnt_type, MNTTYPE_ADVFS) == 0) {
        arg_infoT*   infop; 
        int          ret = -1;

        infop = process_advfs_arg(mnt->mnt_fsname);
        if (infop != NULL) {
            ret = advfs_fset_get_id(infop->fsname, infop->fset, &advfsid);
        }

        if (ret != 0) {
            fprintf( stderr, catgets(catd, S_UTIL1, UTIL3, 
                "%s: error accessing file system <%s>; [%d] %s\n"),
                Prog, path, ret, "");
            exit( 1 );
        }

        fs_info->advfs_set_id.domainId.id_sec = advfsid.domainId.id_sec;
        fs_info->advfs_set_id.domainId.id_usec = advfsid.domainId.id_usec;
        fs_info->advfs_set_id.dirTag.tag_num = advfsid.dirTag.tag_num;
        fs_info->advfs_set_id.dirTag.tag_seq = 0;  
        strcpy(fs_info->fset, infop->fset);
        strcpy(fs_info->fsname, infop->fsname);

        free(infop);

        verbosefprintf( stderr, catgets(catd, S_UTIL1, UTIL8, 
            "advfs id : 0x%08x.%08x.%x\n"), 
            advfsid.domainId.id_sec, advfsid.domainId.id_usec, 
            advfsid.dirTag.tag_num);
    }

    strcpy(fs_info->fs_type, mnt->mnt_type);
    if (hasmntopt(mnt, MNTOPT_QUOTA)) {
        fs_info->quotas_on = TRUE;
    } else {
        fs_info->quotas_on = FALSE;
    }
    strcpy( fs_info->path, mnt->mnt_dir);
    strcpy( fs_info->filesys, mnt->mnt_fsname);
    fs_info->fs_id = mnt_stats.f_fsid;
}

/*
 * xor_bufs - XORs the contents of two buffers of length 'len' bytes.
 * the result is 'returned' in 'buf1'.
 */
void
xor_bufs(
         u_char *buf1,  /* in/out - ptr to buffer */
         u_char *buf2,  /* in - ptr to another buffer */
         int32_t len    /* in - number of bytes to xor */
         )
{
    int i;
    
    for (i = 0; i < len; i++) {
        buf1[i] ^= buf2[i];
    }
}

/* end xor_bufs */

uint32_t
crc16_by_nibble( u_char *data, uint32_t data_len )

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
    uint32_t crc, loop_cnt;

    /*
     * "crc_table" contents were obtained from the lib$crc_table VMS
     * runtime library routine.  The polynomial is 0xa001.
     */

    static uint16_t crc_table[16] = 
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

uint16_t
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
    register uint16_t accum;

    static uint16_t crc_tbl[256] = 
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
        accum = (((uint16_t)*s1++ << 8) | 
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
   uint64_t bytes_read, 
   uint64_t bytes_written, 
   uint64_t file_cnt, 
   struct timeval start_time
   )
{
   struct rusage resource_usage, child_usage;
   struct timezone tz;
   struct timeval end_time;
   double total_time;

   getrusage( RUSAGE_SELF, &resource_usage );

   gettimeofday( &end_time, &tz );

   total_time = resource_usage.ru_utime.tv_sec +
                resource_usage.ru_stime.tv_sec +
                (double) resource_usage.ru_utime.tv_usec / 1000000.0 +
                (double) resource_usage.ru_stime.tv_usec / 1000000.0;

   getrusage( RUSAGE_CHILDREN, &child_usage );

   total_time = total_time + child_usage.ru_utime.tv_sec +
                child_usage.ru_stime.tv_sec +
                (double) child_usage.ru_utime.tv_usec / 1000000.0 +
                (double) child_usage.ru_stime.tv_usec / 1000000.0;

   fprintf( stderr, 
            catgets(catd, S_UTIL1, UTIL19, "\n     CPU Time :  %02i:%02i:%09.6f  (Usr: %02i:%02i:%09.6f,  Sys: %02i:%02i:%09.6f)"), 
            HMS( total_time ), 
            HMS( (double) resource_usage.ru_utime.tv_sec + 
                 (double) resource_usage.ru_utime.tv_usec / 1000000.0 ),
            HMS( (double) resource_usage.ru_stime.tv_sec +
                 (double) resource_usage.ru_stime.tv_usec / 1000000.0 ) );

   total_time = (end_time.tv_sec + (double) end_time.tv_usec / 1000000.0) -
              (start_time.tv_sec + (double) start_time.tv_usec / 1000000.0);

   fprintf( stderr, catgets(catd, S_UTIL1, UTIL20, 
       "\n Elapsed Time :  %02i:%02i:%09.6f\n"), HMS( total_time ) );

   fprintf( stderr, catgets(catd, S_UTIL1, UTIL21, 
       "\n Files Processed :  %9li"), file_cnt );
   fprintf( stderr, catgets(catd, S_UTIL1, UTIL22, 
       "\n      Bytes Read :  %9li,        Bytes Written : %9li"), 
       bytes_read, bytes_written );

   fprintf( stderr, catgets(catd, S_UTIL1, UTIL23, 
       "\n  Bytes/Sec Read :  %9li,    Bytes/Sec Written : %9li\n"),
       (int64_t) (bytes_read / total_time), 
       (int64_t) (bytes_written / total_time) );
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
        pthread_mutex_lock(&tty_mutex);
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL28, 
            "%s: do you want to retry? "), Prog );
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
        pthread_mutex_unlock(&tty_mutex);
    } while ((resp = strchr( "yYnN", request[0] )) == NULL);

    if (strchr( "yY", request[0] )) {
        return (1);
    }

    return (0);
}

extern int Silent_mode;

void
silent_fprintf(char *msg, ...)
{
    va_list ap;

    va_start( ap, msg );

    if (!Silent_mode) {
	if (!strcmp( Prog,"advrestore")) {
		vfprintf(stdout, msg, ap);
	} else {
	 	vfprintf(stderr, msg, ap);
	}
    }

    va_end(ap);
    fflush( stderr );
}

/*
 *  The following routine display data from the devgetinfo data structure.
 */
void
PrintStatus( struct mtget *p_info )
{

    /*
     *  This function assumes a tape device.
     */
    fprintf( stderr, "                        " );
    if (GMT_BOT(p_info->mt_gstat)) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL126, "DEV_BOM ") );
    }
    if (GMT_EOT(p_info->mt_gstat)) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL127, "DEV_EOM ") );
    }
    if (!GMT_ONLINE(p_info->mt_gstat)) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL128, "DEV_OFFLINE ") );
    }
    if (GMT_WR_PROT(p_info->mt_gstat)) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL129, "DEV_WRTLCK ") );
    }

    return;
    }


 
/*
 * Display the contents of the mtget struct.
 * Args: fd a file descriptor of the already opened tape device.
 */
void
print_mtio(
	int fd
)
{
    struct mtget mt;

    if (ioctl(fd, MTIOCGET, (char *)&mt) < 0) {
	fprintf( stderr,catgets(catd, S_UTIL1, UTIL35, "\nmtiocget ioctl failed!\n"));
	return;
    }

    fprintf( stderr,catgets(catd, S_UTIL1, UTIL36, "\nMTIOCGET ELEMENT	CONTENTS"));
    fprintf( stderr,"\n----------------	--------\n");
    fprintf( stderr,catgets(catd, S_UTIL1, UTIL37, "mt_type			"));
    switch(mt.mt_type) {
	case MT_ISTS:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL38, "MT_ISTS\n"));
		break;
	case MT_ISHT:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL39, "MT_ISHT\n"));
		break;
	case MT_ISTM:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL40, "MT_ISTM\n"));
		break;
        case MT_ISHPIB_REEL:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL51, "MT_ISHPIB_REEL\n"));
                break;
        case MT_ISDDS1:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL52, "MT_ISDDS1\n"));
             break;
        case MT_ISDDS2:  /*  same ID for DDS3 & DDS4 */
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL53, "MT_ISDDS2\n"));
             break;
        case MT_ISSCSI_REEL:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL54, "MT_ISSCSI_REEL\n"));
             break;
        case MT_ISQIC:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL55, "MT_ISQIC\n"));
             break;
        case MT_IS8MM:  
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL56, "MT_IS8MM\n"));
             break;
        case MT_IS3480:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL57, "MT_IS3480\n"));
             break;
        case MT_ISDLT:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL58, "MT_ISDLT\n"));
             break;
        case MT_ISAIT:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL59, "MT_ISAIT\n"));
             break;
        case MT_IS3590:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL60, "MT_IS3590\n"));
             break;
        case MT_ISLTO:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL61, "MT_ISLTO\n"));
             break;
	default:
		fprintf( stderr,catgets(catd, S_UTIL1, UTIL46, "Unknown mt_type = 0x%x\n"),mt.mt_type);
    }

    fprintf( stderr,catgets(catd, S_UTIL1, UTIL47, 
        "mt_dsreg1\t\t%X\n"), mt.mt_dsreg1);
    fprintf( stderr,catgets(catd, S_UTIL1, UTIL47B, 
        "mt_dsreg2\t\t%X\n"), mt.mt_dsreg2);
    fprintf( stderr,catgets(catd, S_UTIL1, UTIL48, 
        "mt_erreg\t\t%X\n"), mt.mt_erreg);
    fprintf( stderr,catgets(catd, S_UTIL1, UTIL49, 
        "mt_resid\t\t%X\n"), mt.mt_resid);
    fprintf( stderr,"\n");

    return;
}


/*
 * buf_of_zeros
 *
 * Returns TRUE if the buffer contains all zeros, FALSE otherwise.
 */

int
buf_of_zeros(
    uint64_t *buf,
    int buf_size        /* number of uint64s in buffer */
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
init_event(advfs_ev *advfs_event)
{
    advfs_event->special = NULL;
    advfs_event->domain = NULL;
    advfs_event->fileset = NULL;
    advfs_event->directory = NULL;
    advfs_event->snapfset = NULL;
    advfs_event->renamefset = NULL;
    advfs_event->user = NULL;
    advfs_event->group = NULL;
    advfs_event->fileHLimit =  NULL;
    advfs_event->blkHLimit =  NULL;
    advfs_event->fileSLimit =  NULL;
    advfs_event->blkSLimit =  NULL;
    advfs_event->options = NULL;
    return 0;
}

static int
init_lib_evm( void )
{
    int      ret;
    void*    evmHandle = NULL;

    evmHandle = dlopen("libevm.sl", RTLD_LAZY);

    if (evmHandle == NULL) {
        /*  Couldn't find the library */
        return(1);
    }

    dl_EvmEventCreateVa = dlsym(evmHandle, "EvmEventCreateVa");
    dl_EvmVarSet = dlsym(evmHandle, "EvmVarSet");
    dl_EvmConnCreate = dlsym(evmHandle, "EvmConnCreate");
    dl_EvmEventPost = dlsym(evmHandle, "EvmEventPost");
    dl_EvmConnDestroy = dlsym(evmHandle, "EvmConnDestroy");
    dl_EvmEventDestroy = dlsym(evmHandle, "EvmEventDestroy");

    if (!dl_EvmEventCreateVa || !dl_EvmVarSet      ||
        !dl_EvmConnCreate    || !dl_EvmEventPost   ||
        !dl_EvmConnDestroy   || !dl_EvmEventDestroy) {
        return(1);
    }

    return(0);
}

int
advfs_post_user_event(char *evname, advfs_ev advfs_event, char *Prog)
{
    int i, ret=0;
    static evm_lib_init_done = 0;

    EvmEvent_t      ev;
    EvmVarValue_t   value;
    EvmConnection_t evconn;

    if( !evm_lib_init_done ) {
        evm_lib_init_done = init_lib_evm();
    }

    /*
     * If init_lib_evm fails, EVM is not available on this system, but this
     * doesn't mean we still can't run fsadm and vdump/vrestore, so we return 0.
     */
    if( evm_lib_init_done != 0 ) {
        return 0;
    }

    ret = dl_EvmEventCreateVa(&ev, EvmITEM_NAME,evname,EvmITEM_NONE);
    if (ret != EvmERROR_NONE)  {
        goto _error2;
    }

    if (advfs_event.special != NULL) {
        value.STRING = advfs_event.special;
        dl_EvmVarSet(ev, "special", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.domain != NULL) {
        value.STRING = advfs_event.domain;
        dl_EvmVarSet(ev, "domain", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileset != NULL) {
        value.STRING = advfs_event.fileset;
        dl_EvmVarSet(ev, "fileset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileHLimit != NULL) {
        value.INT64 = advfs_event.fileHLimit;
        dl_EvmVarSet(ev, "fileHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkHLimit != NULL) {
        value.INT64 = advfs_event.blkHLimit;
        dl_EvmVarSet(ev, "blkHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.fileSLimit != NULL) {
        value.INT64 = advfs_event.fileSLimit;
        dl_EvmVarSet(ev, "fileSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkSLimit != NULL) {
        value.INT64 = advfs_event.blkSLimit;
        dl_EvmVarSet(ev, "blkSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.options != NULL) {
        value.STRING = advfs_event.options;
        dl_EvmVarSet(ev, "options", EvmTYPE_STRING, value, 0, 0);
    }

    ret = dl_EvmConnCreate(EvmCONNECTION_POST, EvmRESPONSE_WAIT,
            NULL, NULL, NULL, &evconn);
    if (ret != EvmERROR_NONE)  {
        goto _error1;
    }

    /* EvmEventPost waits for daemon response before returning,
       due to EvmRESPONSE_WAIT in EvmConnCreate */

    ret = dl_EvmEventPost(evconn,ev);

    dl_EvmConnDestroy(evconn);

_error1:
    dl_EvmEventDestroy(ev);

_error2:

    if (ret) {
        if ( ret == EvmERROR_CONNECT ) {
            fprintf(stderr, catgets(catd, S_UTIL1, UTIL170,
                    "%s: informational: [%d] posting event: %s\n"
                    "\tIf running in single user mode, EVM is not running.\n"
                    "\tPlease ignore this posting.\n"),
                    Prog, ret, evname);
        } else {
            fprintf(stderr, catgets(catd, S_UTIL1, UTIL169,
                    "%s: error [%d] posting event: %s\n"),
                    Prog, ret, evname);
        }
    }
    return ret;
}



int
post_event_backup_lock(char *Prog)
{
    if (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) == 0) {
        init_event(&advfs_event);
        advfs_event.domain = Fs_info.fsname;
        advfs_event.fileset = Fs_info.fset;
        advfs_post_user_event(EVENT_FSET_BACKUP_LOCK, advfs_event, Prog);

        posted_lock = 1;
    }
    return 0;
}

int
post_event_backup_unlock(char *Prog)	
{
    if (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) == 0) {
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
    if (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) == 0) {
        if (posted_lock) {
            advfs_post_user_event(EVENT_FSET_BACKUP_ERROR, advfs_event, Prog);
        }	
        posted_lock = 0;
    }
    return 0;
}

/* end util.c */
