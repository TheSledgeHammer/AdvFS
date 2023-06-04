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
 * @(#)$RCSfile: osf_file.h,v $ $Revision: 1.1.23.1 $ (DEC) $Date: 2001/11/19 16:50:40 $
 */

/*
 * flags- also for fcntl call.
 */
#define FOPEN       (-1)
#define FREAD       00001       /* descriptor read/receive'able */
#define FWRITE      00002       /* descriptor write/send'able */
#define FNDELAY     00004       /* no delay */
#define FAPPEND     00010       /* append on each write */
#define FMARK       00020       /* mark during gc() */
#define FDEFER      00040       /* defer for next gc pass */
#define FASYNC      00100       /* signal pgrp when data ready */
#define FSHLOCK     00200       /* shared lock present */
#define FEXLOCK     00400       /* exclusive lock present */
#define FSYNCRON    0100000     /* Write file syncronously *0004*/
#define FNBUF       0200000     /* file used for n-buffering */
#define FNBLOCK     0400000     /* POSIX no delay */


/* open only modes */
#define FCREAT      01000       /* create if nonexistant */
#define FTRUNC      02000       /* truncate to zero length */
#define FEXCL       04000       /* error if already created */
#define FBLKINUSE       010000      /* block if "in use"    *0002*/
#define FNOCTTY     02000000    /* termio no controlling tty */

/* fcntl(2) requests
 */
#define F_DUPFD 0   /* Duplicate fildes */
#define F_GETFD 1   /* Get fildes flags */
#define F_SETFD 2   /* Set fildes flags */
#define F_GETFL 3   /* Get file flags */
#define F_SETFL 4   /* Set file flags */
#define F_GETOWN 5  /* Get owner */
#define F_SETOWN 6  /* Set owner */
#define F_GETLK 7   /* Get file lock */
#define F_SETLK 8   /* Set file lock */
#define F_SETLKW 9  /* Set file lock and wait */
#define F_SETSYN 10 /* Set syncronous write *0004*/
#define F_CLRSYN 11 /* Clear syncronous write *0004*/

/*
 * Open call.
 */
#define O_RDONLY    000     /* open for reading */
#define O_WRONLY    001     /* open for writing */
#define O_RDWR      002     /* open for read & write */
#define O_NDELAY    FNDELAY     /* non-blocking open */
#define O_APPEND    FAPPEND     /* append on each write */
#define O_CREAT     FCREAT      /* open with file create */
#define O_TRUNC     FTRUNC      /* open with truncation */
#define O_EXCL      FEXCL       /* error on create if file exists */
#define O_BLKANBSET FBLKANBSET  /* block, test and set "in use" */
#define O_FSYNC     FSYNCRON    /* syncronous write *0004*/
#define O_SYNC      O_FSYNC     /* system V synchronous write */
#define O_TERMIO    FTERMIO     /* termio style program */
#define O_NONBLOCK  FNBLOCK     /* POSIX non-blocking I/O */
#define O_NOCTTY    FNOCTTY     /* POSIX don't give controlling tty */
#define O_ACCMODE   O_RDONLY|O_WRONLY|O_RDWR
/*
 * Flock call.
 */
#define LOCK_SH     1   /* shared lock */
#define LOCK_EX     2   /* exclusive lock */
#define LOCK_NB     4   /* don't block when locking */
#define LOCK_UN     8   /* unlock */

/* 
 *  file segment locking types
 */
#define F_RDLCK 01  /* Read lock */
#define F_WRLCK 02  /* Write lock */
#define F_UNLCK 03  /* Remove lock(s) */

/*
 * Lseek call.
 */
#define L_SET       0   /* absolute offset */
#define L_INCR      1   /* relative to current offset */
#define L_XTND      2   /* relative to end of file */

/*
 * Access call.
 * These are defined here for use by access() system call.  User level
 * programs should get these definitions from <unistd.h>. Included here
 * for historical reasons.
 */
#define F_OK        0   /* does file exist */
#define X_OK        1   /* is it executable by caller */
#define W_OK        2   /* writable by caller */
#define R_OK        4   /* readable by caller */
