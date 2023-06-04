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
 */

/* includes */
#include        <stdio.h>
#include        <errno.h>
#include        <stdlib.h>
#include        <string.h>
#include        <fcntl.h>
#include        <unistd.h>
#include        <mntent.h>
#include        <signal.h>
#include        <stdarg.h>    /* for usage() */
#include        <sys/wait.h>
#include        <sys/stat.h>
#include        <sys/ustat.h>
#include        <sys/types.h>
#include        <sys/mount.h>
#include        <sys/fs.h>
#include        <sys/vfs.h>
#include        <sys/inode.h>
#include        <sys/ino.h>
#include        <sys/fstyp.h>
#include        <sys/libfs.h>
#include        <sys/statvfs.h>
#include	<advfs/advfs_evm.h>
#include        <advfs/advfs_syscalls.h>
#include        <sys/fs/advfs_public.h>  /* for advfs_fake_superblock */
#include        <tcr/clu.h>
#include        "common.h"

static int            setInfoCnt = 0;
static setInfoT      *setInfoTbl = NULL;
static char           tagPathName[MAXPATHLEN+1];

/*
 *      NAME:
 *              usage()
 *
 *      DESCRIPTION:
 *              prints usage message
 *
 *      ARGUMENTS:
 *              <fmt> - string format for message, or NULL for default message
 *              <...> - arg list for format string
 *
 *      RETURN VALUES:
 *              none
 */

void
usage(const char *fmt, ...)
{
    int      call_end = 0;
    char     *saved_fmt = NULL;
    va_list   vargs;

    if (fmt) {
        va_start(vargs, fmt);
        call_end = 1;

        /* copy off the fmt string so the next
         * catgets() doesn't reset what the catalog
         * points to */
        saved_fmt = strdup(fmt);
    }

    if ( full_usage ) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, CUSTOM_USAGE,
            "Usage:\tfsadm (advfs)\n"));
    }

    fprintf( stderr, "\t" );
        
    if (saved_fmt) {
        vfprintf(stderr,saved_fmt,vargs);
        free(saved_fmt);
    } else {
        /* print default usage -- will also occur if strdup() fails */
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, DEFAULT_USAGE,
            "fsadm [-F advfs] command [-V] [specific_options...]\n"));
    }

    if (call_end) {
        va_end(vargs);
    }
}

/*
 *      NAME:
 *              check_device()
 *
 *      DESCRIPTION:
 *              checks whether *device is an actual device of the
 *              type requested via rawflag (rawflag == 1 means to
 *              check if *device is a character device, rawflag == 0
 *              means to check that *device is a block device)
 *
 *      ARGUMENTS:
 *              <device> -  name of the device to check
 *              <rawflag> - whether to check for char device or block device
 *
 *      RETURN VALUES:
 *              success = 0
 *              failure = 1 or errno
 */

int
check_device(char *device, char *Prog, int rawflag)
{        
        struct  stat    buf1;
        
        if ( stat( device, &buf1) < 0 ) {
                fprintf(stderr, "%s: ", Prog);
                perror(device);                
                return(errno);
        }

        if(rawflag) {
                if(!(S_ISCHR(buf1.st_mode)) &&
                   !(S_ISREG(buf1.st_mode))) {
                        fprintf(stderr,
                                catgets(catd,
                                        MS_FSADM_COMMON,
                                        NOT_CHRDEV,
                                        "%s: %s is not a valid character device\n"),
                                Prog, device);
                        return(1);
                }
        } else {
                if(!(S_ISBLK(buf1.st_mode))) {
                        fprintf(stderr,
                                catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_NOT_BLKDEV,
                                        "%s: %s is not a valid block device\n"),
                                Prog, device);
                        return(1);                  
                }
        }
        
        return(0);
}

/*
 *	NAME:
 *		check_root
 *
 *	DESCRIPTION:
 *		Verifies that the user is root
 *
 *	ARGUMENTS:
 *		Prog		Program name
 *
 *	RETURN VALUES:
 *		None
 *
 */
void
check_root( char *Prog )
{
    if( geteuid() ) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOROOT,
                                 "\nPermission denied - user must be privileged to run %s.\n\n" ), Prog);
        exit(1);
    }
}

/*
 *      NAME:
 *		Voption
 *
 *      DESCRIPTION:
 *		prints a verified comman line, however,  does not execute.
 *
 *      ARGUMENTS:
 *      <argc> - number of command line args
 *		<argv> - argument vector to use for -V option
 *               (should have been verified prior to calling Voption())
 *
 *      RETURN VALUES:
 *              success - command line string
 *              failure - NULL
 */

char *
Voption(int argc, char **argv)
{
        int i;
        char *cmdline = (char *)calloc(MAXPATHLEN+1, sizeof(char));
        char *position = cmdline;

        if(!cmdline) {
                return(NULL);
        }
        
        snprintf(position, MAXPATHLEN-strlen(cmdline),"fsadm ");
        position = cmdline + strlen(cmdline);
        
        if ( strcmp(fstype,MNTTYPE_ADVFS) == 0 ) {
                snprintf(position,MAXPATHLEN-strlen(cmdline),"-F %s %s ",MNTTYPE_ADVFS, argv[0]);
                position = cmdline + strlen(cmdline);
        }
        
        for(i = 1; i < argc; i++) {
                /* make sure not to print the -V option */
                if(strcmp(argv[i], "-V")) {
                        snprintf(position, MAXPATHLEN-strlen(cmdline), "%s ", argv[i]);
                        position = cmdline + strlen(cmdline);
                }
        }
        
        snprintf(position, MAXPATHLEN-strlen(cmdline), "\n");

        return(cmdline);
}

/*
 *      NAME:
 *              mntl_load()
 *
 *      DESCRIPTION:
 *              loads a current snapshot of the mount list from /etc/mnttab
 *
 *      ARGUMENTS:
 *              <err> - error value returned by called functions. 
 *
 *      RETURN VALUES:
 *              A pointer to the head of a mntlist_t linked list.
 */

mntlist_t *
mntl_load(int *err) 
{
    FILE *mntfile_p;
    struct mntent *mntent_p;
    mntlist_t *prev;
    mntlist_t *cur;
    mntlist_t *head;
    int first = 1;
    *err = 0;

    mntfile_p = setmntent(MNT_MNTTAB, "rb");
    if (mntfile_p == NULL) {
        *err = errno;
        return(NULL);
    }

    while(mntent_p = getmntent(mntfile_p)) {
        cur = (mntlist_t *)malloc(sizeof(mntlist_t));
        if (cur == NULL) {
            *err = ENOMEM;
            return(NULL);
        }
        cur->mntl_fromname = strdup(mntent_p->mnt_fsname);
        cur->mntl_ondir = strdup(mntent_p->mnt_dir);
        cur->mntl_type = strdup(mntent_p->mnt_type);
        cur->mntl_next_p = NULL;
        if (first) {
            head = cur;
            first = 0;
        }
        else {
            prev->mntl_next_p = cur;
        }
        prev = cur;
    }

    endmntent(mntfile_p);
    return(head);
}

/*
 *      NAME:
 *              mntl_free()
 *
 *      DESCRIPTION:
 *              frees the mntlist linked list. 
 *
 *      ARGUMENTS:
 *              <mnt_head> - head of mntlist linked list.
 *
 *      RETURN VALUES:
 *              none.
 */

void 
mntl_free(mntlist_t *mnt_head) {
    mntlist_t *cur = mnt_head;
    mntlist_t *next;

    if (mnt_head == NULL)
        return;

    for (; cur != NULL; cur = next) {
        next = cur->mntl_next_p;
        free(cur->mntl_fromname);
        free(cur->mntl_ondir);
        free(cur->mntl_type);
        free(cur);
    }
    mnt_head = NULL;
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

int
advfs_post_user_event(char *evname, advfs_ev advfs_event, char *Prog)
{
    int i, ret=0;

    EvmEvent_t      ev;
    EvmVarValue_t   value;
    EvmConnection_t evconn;

    ret = EvmEventCreateVa(&ev, EvmITEM_NAME,evname,EvmITEM_NONE);
    if (ret != EvmERROR_NONE)  {
        goto _error2;
    }

    if (advfs_event.special != NULL) {
        value.STRING = advfs_event.special;
        EvmVarSet(ev, "special", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.domain != NULL) {
        value.STRING = advfs_event.domain;
        EvmVarSet(ev, "domain", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileset != NULL) {
        value.STRING = advfs_event.fileset;
        EvmVarSet(ev, "fileset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileHLimit != NULL) {
        value.INT64 = advfs_event.fileHLimit;
        EvmVarSet(ev, "fileHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkHLimit != NULL) {
        value.INT64 = advfs_event.blkHLimit;
        EvmVarSet(ev, "blkHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.fileSLimit != NULL) {
        value.INT64 = advfs_event.fileSLimit;
        EvmVarSet(ev, "fileSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkSLimit != NULL) {
        value.INT64 = advfs_event.blkSLimit;
        EvmVarSet(ev, "blkSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.options != NULL) {
        value.STRING = advfs_event.options;
        EvmVarSet(ev, "options", EvmTYPE_STRING, value, 0, 0);
    }

    ret = EvmConnCreate(EvmCONNECTION_POST, EvmRESPONSE_WAIT,
            NULL, NULL, NULL, &evconn);
    if (ret != EvmERROR_NONE)  {
        goto _error1;
    }

    /* EvmEventPost waits for daemon response before returning,
       due to EvmRESPONSE_WAIT in EvmConnCreate */

    ret = EvmEventPost(evconn,ev);

    EvmConnDestroy(evconn);

_error1:
    EvmEventDestroy(ev);

_error2:

    if (ret) {
        if ( ret == EvmERROR_CONNECT ) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_EVM1,
                    "%s: informational: [%d] posting event: %s\n\tIf running in single user mode, EVM is not running.\n\tPlease ignore this posting.\n"),
                    Prog, ret, evname);
        } else {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_EVM2,
                    "%s: Error [%d] posting event: %s\n"),
                    Prog, ret, evname);
        }
    }
    return ret;
}

int
get_fsinfo_from_file(char *file, arg_infoT **fsInfo, int *err)
{
    struct stat fileStats; /* file stats */
    struct stat rootStats; /* file set's root stats */
    FILE*  mntFile = NULL;
    struct mntent* mntInfo;
    arg_infoT* infop = NULL;

    *err = 0;
    
    if ( stat(file, &fileStats) < 0) {
        *err = errno;
        return(-1);
    }

    /*
     * Use getmntent() to get the pathname of the mount point.
     */
    mntFile = setmntent(MOUNTED, "r");
    if (mntFile == NULL) {
        *err = errno;
        return(-1);
    }

    while (mntInfo = getmntent(mntFile)) {
        if ( stat(mntInfo->mnt_dir, &rootStats) == 0) {
            if (rootStats.st_dev == fileStats.st_dev) {
                /*  found a match */
                if (strcmp(mntInfo->mnt_type, MNTTYPE_ADVFS) != 0) {
                    /* not on an advfs filesystem, so we don't set *err,
                     * but we do return -1 for error */
                    endmntent(mntFile);
                    return(-1);
                }
                break;
            }
        }
    }
    
    endmntent(mntFile);

    /* wasn't found in the list */
    if( mntInfo == NULL ) {
	*err = ENOENT;
	return( -1 );
    }

    infop = process_advfs_arg(mntInfo->mnt_fsname);
    if(infop == NULL) {
        *err = errno;
        return(-1);
    }

    *fsInfo = infop;

    return(0);
}

/*
 * file_migrate
 *
 */
int
file_migrate ( char *Prog,      /* in */
         int fd,                /* in */
         vdIndexT srcVolIndex,  /* in, -1 means "any volume" */
         uint64_t srcFobOffset, /* in, -1 means "any offset" */
         uint64_t srcFobCnt,    /* in, -1 means the whole file */
         vdIndexT dstVolIndex,  /* in, -1 means "any volume" */
         uint64_t dstBlkOffset  /* in, -1 means "any offset" */
         )

{
    adv_bf_attr_t bfAttr = { 0 };
    adv_bf_info_t bfInfo = { 0 };
    uint64_t fobCnt = 0;
    uint64_t fobOffset = 0;
    uint64_t remainder = 0;
    adv_status_t sts = 0;
    uint32_t forceFlag = 0;

    fobOffset = srcFobOffset;
    if (fobOffset == (uint64_t)-1) {
        fobOffset = 0;
    }
    fobCnt = srcFobCnt;

    sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERRBFP,
                                 "%s: advfs_get_bf_params failed --- %s\n"),
                 Prog, BSERRMSG( sts ));
        return 1;
    }

    if (fobCnt == (uint64_t)-1) {
        fobCnt = bfInfo.mbfNextFob - fobOffset;
    }

    if (fobCnt == 0)
            return 0;

    /* otherwise, we need to round fobOffset and fobCnt to page boundaries */
    if((remainder = fobOffset % bfInfo.pageSize) != 0) {
        fobOffset -= remainder;
        /* we must add the remainder to the fobCnt to make sure we 
         * actually migrate the data requested by the user.
         * i.e it makes the 'fsadm migrate -k 7 -n 2 file' 
         * case work */
        fobCnt += remainder;
    }
    if((remainder = fobCnt % bfInfo.pageSize) != 0) {
        fobCnt += bfInfo.pageSize - remainder;
    }

 retry_migrate:
    sts = advfs_migrate (
                         fd,
                         srcVolIndex,
                         fobOffset,
                         fobCnt,
                         dstVolIndex,
                         dstBlkOffset,
                         forceFlag
                         );
    if (sts != EOK) {
        if ( (!forceFlag) && (sts == E_INVOLUNTARY_ABORT) ) {
            /* Try again, this time with forceFlag set */
            forceFlag = 1;
            goto retry_migrate;
        }
        else {
            fprintf( stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_MIGFAILED,
                                     "%s: advfs_migrate failed --- %s\n"),
                     Prog, BSERRMSG( sts ));
            return 1;
        }
    }

    return 0;

}  /* end file_migrate */


/*
 *    NAME:   invalidate_superblock()
 *
 *    DESCRIPTION:
 *            Given a block device specification, modifies the 
 *            AdvFS Magic mark in the volume superblock to indicate
 *            that the volume once had an AdvFS filesystem on it, but
 *            does no longer.
 *
 *    ARGUMENTS:
 *            devspec 		- (IN)  block device name 
 *
 *    RETURN VALUES:
 *            EOK if success
 *	      errno if failure 
 */
statusT
invalidate_superblock( char *devspec )
{
    int fd;                     /* device file handle */
    char sbbuf[SBSIZE];         /* buffer to read super block */
    struct fs *fsp;             /* super block structure */

    if ((fd = open(devspec, O_RDWR)) == -1) {
        return errno;
    }

    if (lseek(fd, (off_t) dbtoo(SBLOCK), SEEK_SET) == -1) {
        close(fd);
        return errno;
    }

    if (read(fd, sbbuf, SBSIZE) != SBSIZE) {
        close(fd);
        return errno;
    }

    fsp = (struct fs *)sbbuf;
    if ( !ADVFS_VALID_FS_MAGIC(fsp) ) {
        /*  Superblock already invalidated, not an error */
        close(fd);
        return EOK;
    }

    /*  Clear bit 31 */
    fsp->fs_magic &= ~(1 << 31);

    if (lseek(fd, (off_t) dbtoo(SBLOCK), SEEK_SET) == -1) {
        close(fd);
        return errno;
    }

    if (write(fd, sbbuf, SBSIZE) != SBSIZE) {
        close(fd);
        return errno;
    }


    close(fd);
    return EOK;


}

/*
 *    NAME:   revalidate_superblock()
 *
 *    DESCRIPTION:
 *            Given a block device specification, modifies the
 *            AdvFS Magic mark in the volume superblock to indicate
 *            that the volume has an AdvFS filesystem on it.
 *
 *    ARGUMENTS:
 *            devspec           - (IN)  block device name
 *
 *    RETURN VALUES:
 *            EOK if success
 *            errno or EINVAL if failure
 */
statusT
revalidate_superblock( char *devspec )
{
    int fd;                     /* device file handle */
    char sbbuf[SBSIZE];         /* buffer to read super block */
    struct fs *fsp;             /* super block structure */
    int32_t magic;		/* Current magic number in fs struct */

    if( ( fd = open( devspec, O_RDWR ) ) == -1 ) {
        return errno;
    }

    if( lseek( fd, (off_t) dbtoo( SBLOCK ), SEEK_SET ) == -1 ) {
        close( fd );
        return errno;
    }

    if( read( fd, sbbuf, SBSIZE ) != SBSIZE ) {
        close( fd );
        return errno;
    }

    fsp = (struct fs *)sbbuf;

    /* Superblock already validated, not an error */
    if( ADVFS_VALID_FS_MAGIC( fsp ) ) {
        close( fd );
        return EOK;
    }

    /* Verify current magic number */
    ADVFS_GET_FS_MAGIC( fsp, magic );
    if( magic != ( ADVFS_MAGIC & ~( 1 << 31 ) ) ) {
	/* Magic num wasn't properly invalidated, something's fishy */
	close( fd );
	return EINVAL;
    }

    /* Set the magic field */
    ADVFS_SET_FS_MAGIC( fsp );

    if( lseek( fd, (off_t) dbtoo( SBLOCK ), SEEK_SET ) == -1 ) {
        close( fd );
        return errno;
    }

    if( write( fd, sbbuf, SBSIZE ) != SBSIZE ) {
        close( fd );
        return errno;
    }

    close( fd );
    return EOK;
}

/*
 * open_dot_tags
 *
 * This function opens ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets cannot be unmounted during the remove volume operation.
 *
 * This function has the side effect of filling the bitfile set cache.
 *
 * This function assumes the caller has locked the domain.
 */

int
open_dot_tags(
    arg_infoT* infop,        /* in */
    bfTagT   rootBfSetTag,   /* in */
    char**   metarootpath,   /* out */
    char*    Prog            /* in */
)
{

    setInfoT*          setInfo;
    uint64_t           bfSetIndex;
    bfSetParamsT       bfSetParams;
    int                err = 0;
    int                i;
    bfSetIdT           mountBfSetId;
    char*              mountDmnName = NULL;
    int                setNotMountedFlag = 0;
    struct mntent*     mnt = NULL;
    FILE*              mnttab = NULL;
    adv_status_t       sts;
    uint32_t           userId;
    int                found = 0;
    arg_infoT*         tinfop = NULL;

    /*
     * For all mounted bitfile sets in the filesystem, add an entry
     * to the bitfile set cache and set the ".tags" path.
     */

    if (infop == NULL) {
        return(1);
    }

    mnttab = setmntent(MNT_MNTTAB, "r");
    while(mnt = getmntent(mnttab)) {

        if (strcmp (mnt->mnt_type, MNTTYPE_ADVFS) != 0) {
            continue;
        }

        if ((tinfop = process_advfs_arg(mnt->mnt_fsname)) == NULL) {
            continue;
        }

        if (strcmp (infop->fsname, tinfop->fsname) != 0) {
            free(tinfop);
            continue;
        }

        sts = advfs_fset_get_id (tinfop->fsname, tinfop->fset, &mountBfSetId);

        if (sts == EOK) {
            /*
             * The bitfile set is a member of the domain.
             */
            setInfo = insert_setTag (mountBfSetId.dirTag, Prog);
            if (setInfo == NULL) {
                /*  insert_setTag already printed an error */
                err = 1;
                free(tinfop);
                break;
            }

            setInfo->mountedFlag = 1;
            if (setInfo->mntOnName == NULL) {
                setInfo->mntOnName = strdup(mnt->mnt_dir);
                if (setInfo->mntOnName == NULL) {
                    fprintf (stderr, catgets(catd, MS_FSADM_COMMON,
                        COMMON_MEMNM,
                        "%s: Cannot allocate mount on name memory\n"));
                    err = 1;
                    free(tinfop);
                    break;
                }
            }

            if (setInfo->dotTagsPath == NULL) {
                int sz;
                /*
                 * New entry.  Set the ".tags" path.
                 */
                sz = strlen(mnt->mnt_dir) + strlen(DOT_TAGS) + 1;
                setInfo->dotTagsPath = malloc(sz);
                if (setInfo->dotTagsPath == NULL) {
                    fprintf (stderr, catgets(catd, MS_FSADM_COMMON,
                        COMMON_MEMPATH,
                        "%s: Cannot allocate path name memory\n"));
                    err = 1;
                    free(tinfop);
                    break;
                }

                snprintf(setInfo->dotTagsPath, sz, 
                    "%s%s", mnt->mnt_dir, DOT_TAGS);

                /* Save the first dotTagsPath so that we can later find
                 * all of the metadata (sbm and bmt) files. 
                 */
                if (metarootpath && (*metarootpath == NULL)) {
                    *metarootpath = strdup(setInfo->dotTagsPath);
                    if (*metarootpath == NULL) {
                        fprintf (stderr, catgets(catd, MS_FSADM_COMMON,
                            COMMON_MEMPATH,
                            "%s: Cannot allocate path name memory\n"));
                        err = 1;
                        free(tinfop);
                        break;
                    }
                    strcpy (*metarootpath, setInfo->dotTagsPath);
                }

            }
        } else {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_BADFSID,
                "%s: Cannot get file set id.\n"), Prog);
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSFSNM,
                "%s: File system name: %s, file set name: %s\n"),
                Prog, tinfop->fsname, tinfop->fset);
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
                "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            err = 1;
            free(tinfop);
            break;
        }

        free(tinfop);
    }  /* end while */
    endmntent(mnttab);

    if (err == 1) {
        return(err);
    }

    bfSetIndex = 0;

    sts = advfs_fset_get_info (infop->fsname, &bfSetIndex, &bfSetParams, &userId, 0);

    while (sts == EOK) {

        setInfo = insert_setTag(bfSetParams.bfSetId.dirTag, Prog);
        if (setInfo == NULL) {
            /*  insert_setTag already printed an error */
            return(1);
        }

        if (setInfo->mountedFlag == 0) {
            /*
             * Mount the bitfile set.  Not currently implemented.
             * Simply issues an error stating that the filesystem
             * ought to be mounted.
             */

            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSETNMNT, 
                "%s: File set not mounted\n"), Prog);
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSFSNM,
                "%s: File system name: %s, file set name: %s\n"),
                Prog, infop->fsname, infop->fset);
            /*
             * This flag prevents subsequent ".tags" opens.
             */
            setNotMountedFlag = 1;
        }

        if ((setInfo->fd < 0) && (setNotMountedFlag == 0)) {
            /*
             * Open the bitfile set's ".tags" directory so that the bitfile
             * set cannot be unmounted.
             */
            setInfo->fd = open (setInfo->dotTagsPath, O_RDONLY, 0);
            if (setInfo->fd < 0) {
                fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2, 
                    "%s: open of %s failed.\n"), Prog, setInfo->dotTagsPath);
                fprintf (stderr, catgets(catd, MS_FSADM_COMMON,
                    COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                    Prog, errno, ERRMSG(errno));
                return(1);
            }
        }

        sts = advfs_fset_get_info (infop->fsname, &bfSetIndex,
                                     &bfSetParams, &userId, 0);
    }  /* end while */

    if (sts != E_NO_MORE_SETS) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRFSINFO,
            "%s: Cannot get file set info for file system '%s'\n"),
            Prog, infop->fsname);
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
            "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        return(1);
    }

    return(setNotMountedFlag);

}  /* end open_dot_tags */

/*
 * close_dot_tags
 *
 * This function closes ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets can be unmounted.
 */

int
close_dot_tags(
    char* Prog
)
{
    uint32_t   i;
    int        retval = 0;

    if (setInfoTbl == NULL) {
        return 0;
    }

    for (i = 0; i < setInfoCnt; i++) {

        if (setInfoTbl[i].fd != -1) {
            if (close (setInfoTbl[i].fd)) {
                fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOCLOSE,
                    "%s: close of %s failed. [%d] %s\n"),
                         Prog, ".tags", errno, sys_errlist[errno] );
                retval = 1;
            }

        }
        if (setInfoTbl[i].mntOnName != NULL) {
            free (setInfoTbl[i].mntOnName);
        }
        if (setInfoTbl[i].dotTagsPath != NULL) {
            free (setInfoTbl[i].dotTagsPath);
        }

    }  /* end for */

    free (setInfoTbl);

    setInfoCnt = 0;
    setInfoTbl = NULL;

    return(retval);

}  /* end close_dot_tags */
/*
 * insert_setTag
 *
 * This function inserts a bitfile set tag in the set table.  This function
 * does not check for duplicate entries.
 */

setInfoT*
insert_setTag (
    bfTagT   setTag,
    char*    Prog
)
{

    int      i;
    setInfoT *newSetInfoTbl;
    setInfoT *setInfo;

    setInfo = find_setTag (setTag);
    if (setInfo != NULL) {
        return(setInfo);
    }

    if (setInfoTbl != NULL) {
        newSetInfoTbl = realloc(setInfoTbl, (setInfoCnt + 1) * sizeof (setInfoT));
        if (newSetInfoTbl == NULL) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_MEMEST,
                "%s: Cannot allocate memory for expanded set table\n"), Prog);
            return(NULL);
        }

        setInfoTbl = newSetInfoTbl;
    } else {
        setInfoTbl = malloc(sizeof (setInfoT));
        if (setInfoTbl == NULL) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_MEMNST,
                "%s: Cannot allocate memory for new set table\n"), Prog);
            return(NULL);
        }
    }

    setInfo = &(setInfoTbl[setInfoCnt]);
    setInfo->setTag = setTag;
    setInfo->mountedFlag = 0;
    setInfo->unmountFlag = 0;
    setInfo->mntOnName = NULL;
    setInfo->dotTagsPath = NULL;
    setInfo->fd = -1;

    setInfoCnt++;

    return(setInfo);

}  /* end insert_setTag */

/*
 * find_setTag
 *
 * This function finds a bitfile's set tag in the set table.
 */

setInfoT*
find_setTag (
    bfTagT setTag  
)
{

    int      i;

    if (setInfoTbl == NULL) {
        return NULL;
    }

    for (i = 0; i < setInfoCnt; i++) {
        if (BS_BFTAG_EQL (setTag, setInfoTbl[i].setTag)) {
            return &(setInfoTbl[i]);
        }
    }  

    return NULL;

}  /* end find_bitfile_set */

char*
get_tag_path(
    setInfoT*  inSetInfo, 
    bfTagT     bfTag,
    bfTagT     bfSetTag,
    int*       unknown     /* out */
)
{
    adv_status_t  sts;
    setInfoT*     setInfo = inSetInfo;

    *unknown= 0;
    tagPathName[0] = '\0';
    
    if (setInfo == NULL) {
        setInfo = find_setTag(bfSetTag);
    }

    if (setInfo == NULL) {
        /* never return a NULL path */
        strcpy(tagPathName, " ");
        *unknown = 1;
        return(tagPathName);
    }

    sts = tag_to_path(setInfo->mntOnName, bfTag, sizeof(tagPathName),
                      &(tagPathName[0]));

    if (sts != EOK) {
        /*
         * Can't convert the tag to a pathname.
         */
        snprintf (tagPathName, sizeof(tagPathName),
            "<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))",
            setInfo->setTag.tag_num,
            setInfo->setTag.tag_seq,
            setInfo->setTag.tag_num,
            setInfo->setTag.tag_seq,
            bfTag.tag_num,
            bfTag.tag_seq,
            bfTag.tag_num,
            bfTag.tag_seq
        );
        *unknown = 1;
    }

    return(tagPathName);

} /* end get_tag_path */

/* NAME: size_str_to_fobs(char *)
 *
 * DESCRIPTION:
 *
 *     This function takes a string passed in and converts it
 *     to FOBs.
 *
 * INPUT:
 *     usr_str : user specified size string.
 *        VALID format of string -
 *        %d - Assumes user entered size in 1K FOBs. 
 *        %dK - size in kilobytes
 *        %dM - size in megabytes
 *        %dG - size in gigabytes
 *        %dT - size in terabytes
 *
 * RETURN VALUES:
 *     Returns size in bytes.
 *     Returns -1 if the size string is not valid
 *
 */

uint64_t
size_str_to_fobs(const char *usr_str) {
    char *next_ptr = NULL;
    uint64_t user_size = 0;
    uint64_t num_fobs = 0;

    /* string check for negative values, fob counts can't be negative */
    if (usr_str != NULL && usr_str[0] == '-') {
        return (uint64_t)-1;
    }

    /* If errno has been set but not checked by a previous libc call
     * would errneously exit here if the user asked to migrate
     * ULONG_LONG_MAX fobs.  However, since that is what we use for
     * an error code in this function it's of marginal significance. 
     * Furthermore, the migrate system call see's '-1' as an indication
     * that it should use default values for fobCount, fobIndex, etc.
     * In the future it may be necessary to change this whole interface
     * should a customer wish to migrate the last kilobyte of a multi 
     * terabyte file */
    user_size = strtoull(usr_str, &next_ptr, 10);
    if (next_ptr == NULL || strlen(next_ptr) > 1 || 
        (user_size == ULONG_LONG_MAX && errno != 0)) {
        return (uint64_t)-1;
    }
    switch((int)next_ptr[0]) {
        case '\0':
        case 'K':
        case 'k':
            num_fobs = user_size;
            break;
        case 'M':
        case 'm':
            num_fobs = user_size * 1024;
            break;
        case 'G':
        case 'g':
            num_fobs = user_size * 1024 * 1024;
            break;
        case 'T':
        case 't':
            num_fobs = user_size * 1024 * 1024 * 1024;
            break;
        default:
            return (uint64_t)-1;
    }

    return num_fobs;
} /* size_str_to_fobs */

/*
 * tcr_fsname_in_use
 *
 * Checks that the specified fsname is not in use in any of the
 * of the member-specific areas of a TCR cluster.
 * 
 * Returns:
 *     -1  :  not used
 *     >0  :  member id
 *
 */
int 
tcr_fsname_in_use(
    char*    fsname                /* fsname to check */
)
{
    int                  i, ret;
    char                 tmpPath[MAXPATHLEN+1];
    struct stat          statBuf;
    struct clu_gen_info  *clugenptr = NULL;

    /* If this isn't a TCR cluster, just fail */
    ret = clu_get_info(&clugenptr);
    if ((ret != 0) || (clugenptr->clu_type != CLU_TYPE_CFS)) {
        return(-1);
    }

    /* ensure each member's private directory is searched */

    for (i = 1 ; i <= clugenptr->clu_num_of_members ; i++) {
        if (i == clugenptr->my_memberid) {
            /* don't bother looking at our own cluster member */
            continue;
        }
        sprintf(tmpPath, ADVFS_CLU_MEMBER_PATH"/%s", i, fsname);

        ret = stat(tmpPath, &statBuf);
        if (ret) {
            continue;
        }

        if (S_ISDIR(statBuf.st_mode)) {
            return(i);
        }
    }

    return(-1);
}
