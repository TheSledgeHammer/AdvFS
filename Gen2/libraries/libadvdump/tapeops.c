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
 *      Advfs Storage System
 *
 * Abstract:
 *
 *      Tape handling routines for advdump/advrdump and advrestore/advrrestore.
 *
 *
 */

#include <advfs/util.h>
#include <advfs/backup.h>
#include <advfs/tapefmt.h>
#include <stdarg.h>
#include <sys/mtio.h>
#include <sys/diskio.h>
#include <sys/ioctl.h>
#include "libvdump_msg.h"

extern nl_catd catd;
extern char *sys_errlist[];
extern pthread_mutex_t tty_mutex;
extern uint32_t Blk_size;
extern int Use_stdout;
extern Dev_type_t Dev_type;
extern char os_name[];
extern char* remote_host_name;     /*  From vdump.c */


/*
 * tape_write_eof()
 *
 * Writes an EOF mark on a tape.
 */

int
tape_write_eof( int devFd, int tape_num )
{
    struct mtop  mt;
    int          st;

    mt.mt_op = MTWEOF;
    mt.mt_count = 1;

    if (remote_host_name) {
        st = rmtioctl(MTWEOF,1);
    } else {
        st = ioctl( devFd, MTIOCTOP, &mt);
    }

    if (st < 0) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL24, 
                 "%s: Write EOF on tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }

    return 0;
} /* end tape_write_eof */

/*
 * tape_unload()
 *
 * Rewinds the tape and ejects it from the tape drive.
 */

int
tape_unload( int devFd, int tape_num )
{
    struct mtop  mt;
    int          st;

    mt.mt_op = MTOFFL;
    mt.mt_count = 1;

    if (remote_host_name) {
        st = rmtioctl(MTOFFL,1);
    } else {
        st = ioctl( devFd, MTIOCTOP, &mt );
    }

    if (st < 0) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL26, 
                 "%s: Unload of tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }

    return 0;
}

/*
 * tape_rewind()
 *
 * Rewinds the tape.
 */

int
tape_rewind( int devFd, int tape_num )
{
    struct mtop  mt;
    int          st;

    /* call ioctl to rewind the tape */
    mt.mt_op = MTREW;
    mt.mt_count = 1;

    if (remote_host_name) {
        st = rmtioctl(MTREW,1);
    } else {
        st = ioctl( devFd, MTIOCTOP, &mt );
    }

    if (st < 0) {
        fprintf( stderr, catgets(catd, S_UTIL1, UTIL27,
                 "%s: Rewind of tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }
    
    return 0;
}

/*
 * tape_get_new()
 *
 * Prompts the user to unload the tape we have been writing to and to
 * mount another tape in the tape device.  Returns the devFd back to
 * the calling process else ERROR (-1).
 */
int
tape_get_new( 
    ProgType_t prog, 
    char       *device_name, 
    int        tape_num, 
    char       *remote_host_name 
)
{
    char                request[10];
    char                s[10];
    char *              resp;
    int                 retrys          = 0;
    int                 status;
    struct mtget        devget;
    int                 fd;
    int                 rval            = 0;
    pthread_mutex_t     tt_mutex;
    mode_t              mode;

    if (prog == DUMP) {
        mode = O_RDWR;
    } else {
        mode = O_RDONLY;
    }

    /*
     * Need to prompt user for the next media device
     */
    fprintf( stderr, catgets(catd, S_UTIL1, UTIL29, 
        "\n%s: Change Tapes: Mount tape# %04d\n"),
        Prog, tape_num+1 );

    for( ; ; ) {
        /*
         * Based on the number of requests for this functionality, lets assume 
         * that most users now have autoloaders.  So before prompting the user
         * lets attempt to load the next tape.  But to be on the safe side lets
         * give the autoloader time to load the next tape, at this point I'm 
         * assuming 10 seconds is more than enough, and most people will not 
         * notice an additional wait.
         */
        sleep( 10 );

        retrys++;

        if( retrys > TAPE_LOADER_RETRYS ) {
            pthread_mutex_lock(&tty_mutex);  
            fprintf( stderr, catgets(catd, S_UTIL1, UTIL30,
                    "%s: Press RETURN when tape is mounted... "),
                    Prog );
            /*
             * We need an answer from the user.  If there is no way to
             * get one (Local_tty is NULL), then abort the job.  Otherwise,
             * it may be that Local_tty is still initialized to stdin but
             * a read on stdin may not be possible.  This would be the
             * case if we were running through cron.  Therefore, it is
             * crucial to check the return value from fgets().
             */
            if(   ( !Local_tty               )
               || ( !fgets (s, 9, Local_tty) ) ) {
                pthread_mutex_unlock(&tty_mutex);
                abort_prog( catgets(catd, S_UTIL1, UTIL164,
                    "%s: exiting; user interaction required and no tty present\n"),
                    Prog );
                }
            pthread_mutex_unlock(&tty_mutex);
        }

        /*
         *  Open the new tape
         */
        if (remote_host_name) {
            fd = rmtopen( device_name, mode);
            if( fd == ERROR ) {
                if( errno == EIO ) {
                    fprintf( stderr , catgets(catd, S_UTIL1, UTIL166, 
                        "%s: cannot open device file <%s> on node <%s>; not ready\n")
                        , Prog, device_name, remote_host_name );
                }
                else {
                    fprintf( stderr
                        , catgets(catd, S_UTIL1, UTIL165
                        , "%s: cannot open device file <%s> on node <%s>; [%d] %s\n")
                        , Prog, device_name, remote_host_name, errno, ERR_MSG);
                }
            }
        } else {  /* !remote */

            fd = open( device_name, mode, 0 );
            if( fd == ERROR ) {
                if( errno == EIO ) {
                    fprintf( stderr, catgets(catd, S_UTIL1, UTIL31, 
                        "%s: cannot open device file <%s>; not ready\n"), 
                        Prog, device_name );
                } else {
                    fprintf( stderr , catgets(catd, S_UTIL1, UTIL32, 
                        "%s: cannot open device file <%s>; [%d] %s\n"), 
                        Prog, device_name, errno, ERR_MSG );
                }
            } else {
                /*
                 *  Get device status of dev
                 *
                 */
    
                status = ioctl( fd, MTIOCGET, (void *)&devget );
                if( status  ) {
                    char*  errstr;
                    /*
                     * It failed too: nothing more to try - return error
                     */
                    if (prog == DUMP) {
                        errstr = catgets(catd, S_UTIL1, UTIL34, 
                            "%s: cannot get dev info for writing; [%d] %s\n");
                    } else {
                        errstr = catgets(catd, S_UTIL1, UTIL167, 
                            "%s: cannot get dev info for reading; [%d] %s\n");
                    }

                    fprintf( stderr, errstr, Prog, errno, ERR_MSG );
                    fprintf( stderr, catgets(catd, S_UTIL1, UTIL50, 
                        "\nmtget ioctl failed!\n") );

                    print_mtio( fd );
                    close( fd );
                    return( ERROR );
                }
            }

            if (GMT_BOT(devget.mt_gstat) && GMT_ONLINE(devget.mt_gstat) ) {
                break;   /* we are done */
            }

            fprintf( stderr, catgets(catd, S_UTIL1, UTIL33, 
                "%s: Device '%s' is not ready.\n"), Prog, device_name );
            PrintStatus( &devget);
            close( fd );
        }  /* endif !remote */

        if(   ( retrys > TAPE_LOADER_RETRYS )
           && ( !op_retry()                 ) ) {
            return( ERROR );
        }
    } /* end for() */

    return( fd );
}


int
rmtopen_device(
    ProgType_t  prog,
    char        *dev_to_open
    )
{
    int remote_open_count = 0;
    int open_done = 0;
    int   remote_reopen_count = 0;
    int fd = 0;
    int oflag;

    while (!open_done)
    {
        if (prog == DUMP) {
            oflag = O_WRONLY | O_CREAT | O_TRUNC;
        } else {
            oflag = O_RDONLY;
        }

        while (rmtopen(dev_to_open, oflag) < 0) 
        {
            fprintf(stderr,catgets(catd, RVDUMP,OPENRTFR, 
                "Cannot open remote device file %s, retrying...\n"), 
                dev_to_open);

            /* give extra time for a loader to eject and reload */
            if (remote_open_count < 4)
            {
                remote_open_count++;
                sleep(1);
            }
            else
            {
                remote_open_count = 0;
                fprintf(stderr,catgets(catd, RVDUMP,OPENRTF, 
                    "Cannot open remote device file %s\n"), dev_to_open);
                if (query(catgets(catd, RVDUMP, RTRYOP, "Do you want to retry the open")) == 0)
                {
                    abort_prog("Cannot open remote device file.\n");

                    /* NOT REACHED */
                }
            }
        } /* end while */


        /*
         * Use the magtape ioctl function call 'MTNOP' to detect tape.
         */

        if (rmtioctl(MTNOP,1) < 0) {
            /* 
             * dev is a disk file 
             */
            Dev_type = DEV_FILE;

            /* bind file-specific I/O routines */
            tape_bind();
        } else {
            /*
	     * We know its a tape
             */
             Dev_type = TAPE;
             tape_bind();
        }

        /* We're done with the open since rmtioctl doesn't */
        /* support a status call to a remote device */
        /* assume open and writeable or readable */
        open_done = 1;

    } /* end while */
  return (fd);
} /* end rmtopen_device */


int
get_dev_type( int fd, char *dev_to_open )
{
    int                     status;
    int                     check_norewind=FALSE;
    disk_describe_type      diskinfo;
    struct mtget            tapeinfo;


    /* get device status of dev */

    /*
     * We don't seem to have a generic way to determine the device type,
     * so try tape a tape ioctl first.  If that fails, try the disk ioctl.
     */

    status = ioctl(fd, MTIOCGET, &tapeinfo);

    if (status == 0) {      /* success, it's a tape */
        if (!GMT_ONLINE(tapeinfo.mt_gstat)) {
            fprintf( stderr, catgets(catd, S_COMMON, DEVOFFLINE, 
                "%s: device <%s> is OFFLINE\n"), Prog, dev_to_open );
            return( ERROR );
        }

    /*  I don't see a way to check the rewind status.  This we'll need
     *  to try to set it to norewind, and just mention it if it fails.
     */
        /* 
         * bind tape dev routines 
         */
        Dev_type = TAPE;
        tape_bind();

        return(check_norewind);
    }


    status = ioctl(fd, DIOC_DESCRIBE, &diskinfo);
   
    if (status == 0) {
        /* 
         * bind raw disk dev routines 
         */
        Dev_type = DISK;
        file_bind(); /* raw disk partition */

        return(check_norewind);
    }

    /* If we get here, dev is not a disk or a tape! */
    fprintf( stderr, catgets(catd, S_COMMON, BADDEV, 
            "%s: cannot get dev info for <%s>; [%d] %s\n")
            , Prog, dev_to_open, errno, ERR_MSG );
    close( fd );
    return(ERROR);
}
