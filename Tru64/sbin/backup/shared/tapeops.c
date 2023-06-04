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
 *      Advfs Storage System
 *
 * Abstract:
 *
 *      Tape handling routines for vdump/rvdump and vrestore/rvrestore.
 *
 * Date:
 *
 *      June 12, 1996
 *
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: tapeops.c,v $ $Revision: 1.1.29.5 $ (DEC) $Date: 2005/08/24 06:12:41 $"

#include "util.h"
#include "backup.h"
#include "tapefmt.h"
#include <stdarg.h>
#include <sys/mtio.h>
#include <sys/ioctl.h>
#include <io/common/devgetinfo.h>
#include <syslog.h>
#include "libvdump_msg.h"
#include "../vdump/vdump_msg.h"

extern t_mutex tty_mutex;
extern int Blk_size;
extern int Use_stdout;
extern Dev_type_t Dev_type;
extern char os_name[];

/* Added flag to chk. connected autoloader. */
int check_autoloader = FALSE;
int magtape_ioctl_safe = 0;  /* Uses known magtape ioctl values */


/*
 * tape_write_eof()
 *
 * Writes an EOF mark on a tape.
 */

int
tape_write_eof( int devFd, int tape_num )
{
    struct mtop  mt;

    mt.mt_op = MTWEOF;
    mt.mt_count = 1;
#ifdef REMOTE
    if (rmtioctl(MTWEOF,1) < 0) {
#else
    if (ioctl( devFd, MTIOCTOP, &mt ) < 0) {
#endif
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL24, 
                 "%s: Write EOF on tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }

    return 0;
} /* end tape_write_eof */

/*
 * tape_clear_error()
 *
 *
 */

int
tape_clear_error( int devFd, int tape_num )
{
    struct mtop  mt;

    mt.mt_op = MTCSE;
    mt.mt_count = 1;

    if (ioctl( devFd, MTIOCTOP, &mt ) < 0) {
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL25, 
                 "%s: Clear error on tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }

    return 0;
}

/*
 * tape_unload()
 *
 * Rewinds the tape and ejects it from the tape drive.
 */

int
tape_unload( int devFd, int tape_num )
{
    struct mtop  mt;

    mt.mt_op = MTOFFL;
    mt.mt_count = 1;

#ifdef REMOTE
    if (rmtioctl(MTOFFL,1) < 0) {
#else
    if (ioctl( devFd, MTIOCTOP, &mt ) < 0) {
#endif
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL26, 
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

    /* call ioctl to rewind the tape */
    mt.mt_op = MTREW;
    mt.mt_count = 1;

#ifdef REMOTE
    if (rmtioctl(MTREW,1) < 0) {
#else
    if (ioctl( devFd, MTIOCTOP, &mt ) < 0) {
#endif
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL27,
                 "%s: Rewind of tape %04d failed; [%d] %s\n"), 
                 Prog, tape_num, errno, ERR_MSG );
        return errno;
    }
    
    return 0;
}

/*
 * ultrix_rmtdevice_stat()
 *
 * Only called if the remote system is ultrix.
 * Does its best to determine if tape is mounted and ready to write.
 * Returns 1 if caller should retry rmtopen, Otherwise 0 is returned.
 */

int
ultrix_rmtdevice_stat( dev )
    char *dev;
{
    int foo = 0,error=0;
    struct devget *devgetp; /* use converted ULTRIX devget struct */

    if ((devgetp = rmtgenioctl()) == NULL)
    {
        mutex__lock( tty_mutex );  

        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL30, 
                 "%s: Press RETURN when tape is mounted... "), Prog );

        fprintf(stderr,catgets(comcatd, RVDUMP,IOCE,
                "Cannot get device information for %s\n"), dev);
        if (query(catgets(comcatd, RVDUMP, RTRYOP,
                          "Do you want to retry the open")) == 0)
        {
            mutex__unlock( tty_mutex );  
            abort_prog("Cannot get device information.\n");
            /* never returns */
        }
        else
        {
            mutex__unlock( tty_mutex );  
            rmtclose();
            return(1);
        }
    }

    if((devgetp->stat & DEV_OFFLINE) == DEV_OFFLINE) {
        mutex__lock(tty_mutex);
        fprintf(stderr,catgets(comcatd, RVDUMP,PLONLR,
                "Place Remote %s device unit #%u ONLINE\n"),
                devgetp->device,devgetp->unit_num);
        mutex__unlock(tty_mutex);
        error++;
    }

    if((devgetp->stat & DEV_WRTLCK) == DEV_WRTLCK) {
        mutex__lock(tty_mutex);
        fprintf(stderr,catgets(comcatd, RVDUMP, WRTENR,
                "WRITE ENABLE Remote %s device unit #%u\n"),
                devgetp->device,devgetp->unit_num);
        mutex__unlock(tty_mutex);
        error++;
    }

    if(error)
    {
        mutex__lock(tty_mutex);
        fprintf(stderr,catgets(comcatd, RVDUMP,OPENRTF,
                "Cannot open remote device file %s\n"),dev);
        if (query(catgets(comcatd, RVDUMP, RTRYOP,
                "Do you want to retry the open")) == 0)
        {
                mutex__unlock(tty_mutex);
                abort_prog("Cannot open remote device file.\n");
                /* never gets here */
        }
        else
        {
            mutex__unlock(tty_mutex);
            rmtclose();
            return(1);
        }
    }


    /* everything is ok, we think! */
    return(0);

} /* end  ultrix_rmtdevice_stat() */


/*
 * tape_get_new()
 *
 * Prompts the user to unload the tape we have been writing to and to
 * mount another tape in the tape device.  Returns the devFd back to
 * the calling process else ERROR (-1).
 */
 int
tape_get_new( char *device_name, int tape_num, char *remote_host_name )
{
    char                request[10];
    char                s[10];
    int                 retrys          = 0;
    int                 status;
    device_info_t       devinfo;
    int                 fd;
    int                 rval            = 0;
    t_mutex             tt_mutex;
    
    
    char		*in_put		= NULL;
#ifdef DUMP
#define OPEN_MODE       O_RDWR
#else /* !DUMP */
#define OPEN_MODE       O_RDONLY
#endif /* !DUMP */


    /* Added conditional to define behaviour for manual laoders.
     */
    if( check_autoloader == FALSE ) { 

        /*
         * Need to prompt user for the next media device
         */
        fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL29, 
                    "\n%s: Change Tapes: Mount tape# %04d\n"), Prog, tape_num+1 );

        /* Check if vdump is being run as a cron job.*/

        if( !isatty(STDIN_FILENO) ) {

            /* wait for the user to load the next tape or exit on serious
             * errors.
             */
            while (1) {

                /* Sleep first. Someone may load the next tape 
                 * when the tape ejects.*/
                sleep( 60 ); 

                fd = open_new_tape (device_name, remote_host_name);
                if ( fd != ERROR ) { 
                    fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL169, 
                                "%s: Tape #%04d mounted\n") , Prog, tape_num+1 ); 
                    return ( fd );
                }
                else if ((fd == ERROR) && ((errno == ENODEV) || 
                            (errno == ENOMEM) || (errno == EINVAL) || 
                            (errno == ENXIO) || (errno == EINTR))) {
                    /* ctape_open may return with some errors for which we need
                     * to exit.
                     */
                    abort_prog( catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL168 , 
                                "%s: exiting; unable to open new tape.\n")
                            , Prog ); 
                }
                else if (fd == ERROR) {
                    /* Log message to EVM. */
                    syslog (LOG_NOTICE, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL171, "%s: Change Tapes:Mount tape# %04d"), Prog, tape_num+1);
                }
            }
        }
        else  {    /* tty present */

            do {
                mutex__lock( tty_mutex );  
                fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL30 , 
                            "%s: Press RETURN when tape is mounted... ") , Prog );

		in_put=fgets (s, 9, Local_tty);
		mutex__unlock( tty_mutex );

                if (in_put) { 

                    fd = open_new_tape (device_name, remote_host_name);
                    if (fd != ERROR ) {
                        fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL169, 
                                    "%s: Tape #%04d mounted\n") , Prog, tape_num+1 ); 
                        return ( fd );
                    }
                }
            } while ( op_retry());

            abort_prog( catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL168 , 
                        "%s: exiting; unable to open new tape.\n")
                    , Prog ); 
        }
    } /* end of check_autoloader FALSE */

    else {  /* check_autoloader == TRUE */   
        /* vdump behaviour for autoloader. */

        /*
         * Need to inform user that autoloader is loading next media.
         */
        fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL170, 
                    "\n%s: Changing Tapes: Mounting tape# %04d\n") , Prog, tape_num+1 );

        if( !isatty(STDIN_FILENO) ) { 

            /*
             * There may be some recoverable errors with the autoloader.
             * Keep retrying until open() succeds. exit on un-recoverable 
             * error.
             */
            do {
                /* 
                 * Average cartridge swap time for a typical supported 
                 * autoloader is 60 Secs.
                 */
                sleep( 60 );
                fd = open_new_tape (device_name, remote_host_name);

                if ( fd != ERROR ) {
                    fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL169, "%s: Tape #%04d mounted\n") , Prog, tape_num+1 ); 

                    return ( fd );
                }
                else if ((fd == ERROR) && ((errno == ENODEV) || 
                            (errno == ENOMEM) || (errno == EINVAL) || 
                            (errno == ENXIO) || (errno == EINTR))) {
                    /* ctape_open may return with some errors for which we need
                     * to exit.
                     */
                    abort_prog( catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL168 , 
                                "%s: exiting; unable to open new tape.\n")
                            , Prog ); 
                }
                else if (fd == ERROR) {
                    /* Log message to EVM. */
                    syslog (LOG_NOTICE, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL171, "%s: Change Tapes:Mount tape# %04d"), Prog, tape_num+1);
                }
            } while ( fd == ERROR ); 
        }

        else {     /*tty present */

            /* 
             * In case of command line execution, op_retry() will allow
             * user to confirm the new tape being mounted. Let autoloader 
             * give it a try first.
             */

            do {
                /* 
                 * Average cartridge swap time for a typical supported 
                 * autoloader is 60 Secs.
                 */
                sleep( 60 );
                fd = open_new_tape (device_name, remote_host_name);

                if ( fd != ERROR ) { 
                    fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL169, 
                                "%s: Tape #%04d mounted\n") , Prog, tape_num+1 ); 
                    return ( fd );
                }
                else 
                    retrys++;

            } while ( retrys < TAPE_LOADER_RETRYS );

            /* Now keep retrying until user wants to exit. */
            if ( retrys >= TAPE_LOADER_RETRYS ) {    
                do {
                    mutex__lock( tty_mutex );  
                    fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL30 , 
                                "%s: Press RETURN when tape is mounted... ") , Prog );
	
		    in_put=fgets (s, 9, Local_tty);		
		    mutex__unlock( tty_mutex );

                    if (in_put) { 

                        fd = open_new_tape (device_name, remote_host_name);
                        if (fd != ERROR ) {
                            fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL169, 
                                        "%s: Tape #%04d mounted\n") , Prog, tape_num+1 ); 
                            return ( fd );
                        }
                    }
                } while ( op_retry());

                abort_prog( catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL168 , 
                            "%s: exiting; unable to open new tape.\n")
                        , Prog ); 
            }
        } /* end tty present */

    } /* end of check_autoloader TRUE */
} /* end of tape_get_new. */ 

/*
 * open_new_tape()
 *
 * Open new tape and mount it on the tape drive. 
 * Returns the devFd back to the calling process else ERROR (-1).
 */ 

int open_new_tape ( char *device_name, char *remote_host_name )
{
    int                 status;
    device_info_t       devinfo;
    int                 fd;

#ifdef REMOTE
    fd = rmtopen( device_name, OPEN_MODE );
    if( fd == ERROR ) {
        if( errno == EIO ) {
            fprintf( stderr
                    , catgets(comcatd, S_UTIL1, UTIL166
                        , "%s: can't open device file <%s> on node <%s>; not ready\n")
                    , Prog, device_name, remote_host_name );
        }
        else {
            fprintf( stderr
                    , catgets(comcatd, S_UTIL1, UTIL165
                        , "%s: can't open device file <%s> on node <%s>; [%d] %s\n")
                    , Prog, device_name, remote_host_name, errno, ERR_MSG );
        }
        return( ERROR );
    }
    else {
        /*
         *  Get device status of dev
         */
        if(   ( strncmp(os_name,"ULTRIX",6)         )        /* NOT ultrix          */
                || ( Dev_type != TAPE                    )        /* NOT tape            */
                || ( !ultrix_rmtdevice_stat(device_name) ) ) {    /* remote check passed */
            return ( fd );                                       /* ok: we are done */
        }
        /*
         *    !!!! this is a change from to old code that did not check
         *         retries when ultrix returned a bad stat.
         */
        /* continue;   restore this for the old behaviour: no retry check */
    }

#else /* !REMOTE */

    fd = open( device_name, OPEN_MODE, 0 );
    if( fd == ERROR ) {
        if( errno == EIO ) {
            fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL31
                        , "%s: can't open device file <%s>; not ready\n")
                    , Prog, device_name );
        }
        else {
            fprintf( stderr
                    , catgets(comcatd, S_UTIL1, UTIL32
                        , "%s: can't open device file <%s>; [%d] %s\n")
                    , Prog, device_name, errno, ERR_MSG );
        }
        return( ERROR );
    }
    else {
        /*
         *  Get device status of dev
         *
         *  First, attempt to use the DEVGETINFO ioctl (preferred)
         */
        status = ioctl( fd, DEVGETINFO, &devinfo );
        if(   ( status                       )
                || ( devinfo.version != VERSION_1 ) ) {
            struct devget           devget;

            /*
             *  Failed, try the backward-compatibility DEVIOCGET:
             */
            status = ioctl( fd, DEVIOCGET, (void *)&devget );
            if( status  ) {
                /*
                 * It failed too: nothing more to try - return error
                 */
                fprintf( stderr
#ifdef DUMP
                        , catgets(comcatd, S_UTIL1, UTIL34, "%s: can't get dev info for writing; [%d] %s\n")
#else /* !DUMP */
                        , catgets(comcatd, S_UTIL1, UTIL167, "%s: can't get dev info for reading; [%d] %s\n")
#endif /* !DUMP */
                        , Prog, errno, ERR_MSG );
                fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL50, "\ndevget ioctl failed!\n") );

                print_mtio( fd );
                close( fd );
                return( ERROR );
            }

            devio_2_devgetinfo( &devget, &devinfo );
        }

        if(   ( devinfo.version == VERSION_1                           )
                && ( devinfo.v1.category == DEV_TAPE                        )
                && ( devinfo.v1.devinfo.tape.media_status & TPDEV_BOM       )
                && ( !(devinfo.v1.devinfo.tape.unit_status & TPDEV_OFFLINE) ) ) {
            return ( fd );   /* we are done */
        }

        /* Chk. if tape is positioned on begining of media. */ 
        if( !(devinfo.v1.devinfo.tape.media_status & TPDEV_BOM)) {

            fprintf( stderr, catgets(comcatd, S_UTIL_PATCH, PATCH_UTIL172, 
                        "%s: Tape not positioned on beginning-of-medium.\n"),
                    Prog, device_name );
        }
        else {

            fprintf( stderr, catgets(comcatd, S_UTIL1, UTIL33, "%s: Device '%s' is not ready.\n")
                    , Prog, device_name );
            PrintStatus( &devinfo.v1 );
        }

        close( fd );
        return( ERROR );
    }

#endif /* !REMOTE */

} /* End of open_new_tape */


int
rmtopen_device(
    char *dev_to_open
    )
{
    int remote_open_count = 0;
    int open_done = 0;
    int   remote_reopen_count = 0;
    int fd = 0;

    while (!open_done)
    {
#ifdef DUMP
        while (rmtopen(dev_to_open, O_WRONLY | O_CREAT | O_TRUNC) < 0)
#else /* RESTORE */
        while (rmtopen(dev_to_open, O_RDONLY) < 0) 
#endif
        {
            fprintf(stderr,catgets(comcatd, RVDUMP,OPENRTFR, 
                        "Cannot open remote device file %s, retrying...\n"), dev_to_open);
            /* give extra time for a loader to eject and reload */
            if (remote_open_count < 4)
            {
                remote_open_count++;
                sleep(1);
            }
            else
            {
                remote_open_count = 0;
                fprintf(stderr,catgets(comcatd, RVDUMP,OPENRTF, 
                            "Cannot open remote device file %s\n"), dev_to_open);
                if (query(catgets(comcatd, RVDUMP, RTRYOP, "Do you want to retry the open")) == 0)
                {
                    abort_prog("Cannot open remote device file.\n");

                    /* NOT REACHED */
                }
            }
        } /* end while */


        /*
         * The OSF rmt program doesn't have the rmtgenioctl capability
         * (remote DEVIOCGET - example is present in ULTRIX version)
         * If this were added to OSF rmt it might be necessary to hide
         * it to avoid having other implementations be surprised by it's
         * presence - it's not standard code, but the Stevens book doesn't
         * include that function). This can be accomplished by requiring
         * test of an environment variable before returning data
         * The rdump prog would have to pass the environment variable when
         * invoking rmt in order to get the "smarter" rmt.
         * Until then, for ULTRIX and OSF systems we can use the magtape
         * ioctl function call 'MTNOP' to detect tape.
         * Note: if rmtgenioctl is implemented in OSF rmt, we will still
         * need this workaround for dealing with older systems (we
         * would need to have the uname command return version number
         * If we do the rmtgenioctl we will know the exact tape drive
         * type, allowing all of the other calculations to be done just
         * as in local dumps. But we would probably also want rmtstat
         * also to distinguish between regular files and devices.
         * And if we add rmtgenioctl, we wouldn't use this MTNOP to detect
         * tape and we could merge the following remote with the local code
         */

        if (magtape_ioctl_safe) {
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
        } else {
             fprintf(stderr,catgets(comcatd, RVDUMP,MAGNC,
                      "Remote system is not known to be compatible for magtape functions, output being treated as regular file\n"));
            /* 
             * dev is a disk file 
             */
             Dev_type = DEV_FILE;

             /* bind tape-specific I/O routines */
             tape_bind();
        }


        /* Avoid ULTRIX bug (offline tape drive returns success on open) */
        /* recheck to see if online */

        if ((strncmp(os_name,"ULTRIX",6) == 0) && (Dev_type == TAPE)) 
        {
                int rval = 0;

                if (remote_reopen_count == 0)
                {
                    /* re-open and recheck to avoid timing problems */
                    remote_reopen_count = 1;
                    rmtclose();
                    continue;
                }

                rval = ultrix_rmtdevice_stat(dev_to_open); /* we can check if ultrix on other end */

                /* if it's ULTRIX and we finally have a good open - leave */
                if(!rval) 
                    open_done = 1;

        } else {
                /* if it's not ULTRIX we're done with the open since rmtioctl doesn't */
                /* support a status call(DEVIOCGET nor DEVGETINFO) to a remote device */
                /* assume open and writeable or readable */
                open_done = 1;
        }
    } /* end while */
  return (fd);
} /* end rmtopen_device */



 int
get_dev_type( int fd, char *dev_to_open )
{
    int                     status;
    int                     check_norewind=FALSE;
    device_info_t           devinfo;
    v1_device_info_t *      p_info;


    /* get device status of dev */

	/*
	 * First, attempt to use the DEVGETINFO ioctl (preferred),
	 * if that fails, try the backward-compatibility ioctl
	 * DEVIOCGET.
	 */
    status = ioctl( fd, DEVGETINFO, &devinfo );
    if(   ( status                       )
       || ( devinfo.version != VERSION_1 ) ) {
        struct devget       devget;

        /*
         *  Failed, try the backward-compatibility DEVIOCGET:
         */
        status = ioctl( fd, DEVIOCGET, (void *)&devget );
        if( status  ) {
            /*
             * It failed too: nothing more to try - return error
             */
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP72, "%s: can't get dev info for <%s>; [%d] %s\n")
                    , Prog, dev_to_open, errno, ERR_MSG );
            close( fd );
            return ERROR;
            }
        devio_2_devgetinfo( &devget, &devinfo );
        }
    p_info = &devinfo.v1;
    /* 
     * bind I/O routines based on dev device category 
     */
    switch( p_info->category ) {

      case  DEV_TAPE :
        if( p_info->devinfo.tape.unit_status & TPDEV_OFFLINE ) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP70, "%s: device <%s> is OFFLINE\n")
                    , Prog, dev_to_open );
            return( ERROR );
            }


            if (!(p_info->devinfo.tape.unit_status & TPDEV_REW_ONCLOSE))
                   check_norewind=TRUE;
        /* 
         * bind tape dev routines 
         */
        Dev_type = TAPE;
        tape_bind();
        break;
  
      case  DEV_DISK :
      case  DEV_SPECIAL :
        /* 
         * bind raw disk dev routines 
         */
        Dev_type = DISK;
        file_bind(); /* raw disk partition */
        break;
  
      default :
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP71, "%s: Unsupported device category %d\n")
                , Prog, p_info->category );
        close( fd );
        return( ERROR );
        }

    return( check_norewind );
    }
