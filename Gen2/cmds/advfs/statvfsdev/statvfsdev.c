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
 * Copyright (c) 2004 Hewlett-Packard Development Company, L.P.
 *
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    statvfsdev.c - This program is called by a file system daemon that takes
 *                   requests from libc for AdvFS specific information.
 *
 *
 */

#include <stdio.h>
#include <unistd.h>
#include <strings.h>
#include <libgen.h>
#include <stdlib.h>
#include <errno.h>
#include <dirent.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include <sys/fstyp.h>
#include <sys/fs/advfs_public.h>
#include <advfs/bs_ods.h>
#include <advfs/advfs_syscalls.h>
#include <sys/fsdaemon.h>

#include "statvfsdev_advfs_msg.h"

extern char *__chartoblock(char *charp, char *blockp);

/* MAIN */
int
main(int argc, char **argv)
{
    int                       i, ch, f_flg=0, fd=0, blkseek=0;
    extern char               *optarg;
    char		      in_dev_path[MAXPATHLEN+1];
    char		      tmp_dev_path[MAXPATHLEN+1];
    char*                     dev_path;
    struct stat               statbuf;
    statvfsdevReturnPacket_t  retBuf;
    arg_infoT                 *infop=NULL;
    adv_status_t	      sts;
    int			      type;

    bzero( &retBuf, sizeof( statvfsdevReturnPacket_t ) );

    while( ( ch = getopt( argc, argv, "bdf:" ) ) != EOF ) {
	switch( ch ) {
	    case 'b':
		blkseek = 1;
		break;
            case 'd':
                break;
	    case 'f':
		f_flg++;
		fd = atoi( optarg );
		break;
	    case ':':
		retBuf.common.retCode = EINVAL;
		goto _finish;
	    case '?':
		retBuf.common.retCode = EINVAL;
		goto _finish;
	}
    }

    /* Caller MUST use -f */
    if( f_flg != 1 ){
	retBuf.common.retCode = EINVAL;
	goto _finish;
    }

    /*
     * Translate the file descriptor to an AdvFS info pointer
     */
    if( fstat( fd, &statbuf ) < 0 ) {
	retBuf.common.retCode = errno;
	goto _finish;
    }

    type = (statbuf.st_mode & S_IFMT);

    if (devnm(type, statbuf.st_rdev, in_dev_path, MAXPATHLEN, 0) < 0) {
	/*
	 * In case devnm fails with errno of 0.
	 */
	if (errno == 0) {
		errno = EIO;
	}
	retBuf.common.retCode = errno;
	goto _finish;
    }

    /*  Make sure we weren't passed in a symlink...
     *  devnm() can (and does) return the /dev/advfs[_cfs]/xxx/.stg/xxx 
     *  symlink rather than the actual blockdev.  This is arguably a bug 
     *  in devnm(), but we can guard against it here easily enough.
     */
    strcpy(tmp_dev_path, in_dev_path);

    if (realpath(tmp_dev_path, in_dev_path) == NULL) {
        retBuf.common.retCode = errno;
        goto _finish;
    }

    /*  The device type must be a blockdev if we are to find it as
     *  part of an AdvFS filesystem.  
     */

    if (type == S_IFBLK) {
        dev_path = in_dev_path;
    } else if (type == S_IFCHR) {
        if ((__chartoblock(in_dev_path, tmp_dev_path)) == NULL) {
            retBuf.common.retCode = EINVAL;
            goto _finish;
        }
 
        dev_path = tmp_dev_path;
    } else {
            retBuf.common.retCode = EINVAL;
            goto _finish;
    }

    /*  /dev/root will never appear as a link as it's a link to the
     *  real device.  /dev/root will always be mounted on "/", so we 
     *  can use that instead.
     */
    if (strcmp(dev_path, "/dev/root") == 0) {  
        infop = process_advfs_arg("/");
    } else {
        infop = process_advfs_arg( dev_path );
    }

    if( infop == NULL ) {
	retBuf.common.retCode = EINVAL;
	goto _finish;
    }

    /*
     * Make a system call to get the statvfsdev info
     */
    sts = advfs_get_statvfs( infop->fsname, infop->fset, &retBuf.retStatvfs64 );
    if( sts == EOK ) {
        /* set the mount on name.  Utilities such as bdf require it.
         * Not an error if not mounted, leave the string blank
         */
        char* mntdir = NULL;

        sts = advfs_is_mounted(infop, NULL, NULL, &mntdir);
   
        if ((sts > 0) && (mntdir != NULL)) {
            strncpy(retBuf.retStatvfs64.f_fstr, mntdir,
                sizeof(retBuf.retStatvfs64.f_fstr));
            free(mntdir);
        }
    } else {
	/* 
	 * Indicate that ADVFS doesn't recognize this filesystem.
	 */
	retBuf.common.retCode = -1;
	goto _finish;
    }

#ifdef ST_DEBUG
    fprintf(stderr, "f_bsize:        %d\n", retBuf.retStatvfs64.f_bsize );
    fprintf(stderr, "f_frsize:       %d\n", retBuf.retStatvfs64.f_frsize );
    fprintf(stderr, "f_blocks:       %d\n", retBuf.retStatvfs64.f_blocks );
    fprintf(stderr, "f_bfree:        %d\n", retBuf.retStatvfs64.f_bfree );
    fprintf(stderr, "f_bavail:       %d\n", retBuf.retStatvfs64.f_bavail );
    fprintf(stderr, "f_files:        %d\n", retBuf.retStatvfs64.f_files );
    fprintf(stderr, "f_ffree:        %d\n", retBuf.retStatvfs64.f_ffree );
    fprintf(stderr, "f_favail:       %d\n", retBuf.retStatvfs64.f_favail );
    fprintf(stderr, "f_basetype:     %s\n", retBuf.retStatvfs64.f_basetype );
    fprintf(stderr, "f_namemax:      %d\n", retBuf.retStatvfs64.f_namemax );
    fprintf(stderr, "f_fstr:         %s\n", retBuf.retStatvfs64.f_fstr );
    fprintf(stderr, "f_magic:        %d\n", retBuf.retStatvfs64.f_magic );
    fprintf(stderr, "f_type:         %d\n", retBuf.retStatvfs64.f_type );
    fprintf(stderr, "f_featurebits:  %d\n", retBuf.retStatvfs64.f_featurebits );
    fprintf(stderr, "f_flag:         %d\n", retBuf.retStatvfs64.f_flag );
    fprintf(stderr, "f_fsindex:      %d\n", retBuf.retStatvfs64.f_fsindex );
    fprintf(stderr, "f_fsid:         %d\n", retBuf.retStatvfs64.f_fsid );
    fprintf(stderr, "f_time:         %d\n", retBuf.retStatvfs64.f_time );
    fprintf(stderr, "f_cnode:        %d\n", retBuf.retStatvfs64.f_cnode );
    fprintf(stderr, "f_pad:          %d\n", retBuf.retStatvfs64.f_pad );
    fprintf(stderr, "f_size:         %d\n", retBuf.retStatvfs64.f_size );
    fprintf(stderr, "f_spare:        %d\n", retBuf.retStatvfs64.f_spare );
    fflush(stderr);
#endif

_finish:

    if (infop) { 
        free(infop); 
    }

    fwrite( &retBuf, sizeof( statvfsdevReturnPacket_t ), 1, stdout );
    fflush( stdout );
    return( retBuf.common.retCode );
}
