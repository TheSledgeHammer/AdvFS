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
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <locale.h>
#include <advfs/advfs_syscalls.h>

#include "common.h"

static char *Prog = "fsadm migrate";

void
migrate_usage(void)
{
    usage(catgets(catd,
                  MS_FSADM_MIGRATE,
                  MIGRATE_USAGE,
                  "%s [-V] [-k fileoffset] [-n blkcount] [-s special]\n\t              [-d special] filename\n"),
          Prog);
}

migrate_main( int argc, char *argv[] )

{
    extern int optind;
    extern char *optarg;
    int fd;
    int k = 0, n = 0, s = 0, d = 0, c = 0, i = 0, Vflg = 0;
    int errorFlag = 0;
    int thisDmnLock = 0, err;
    int ssRunningState=0;
    int srcVols = 0, dstVols = 0;
    uint64_t dstBlkOffset = (uint64_t)-1;
    vdIndexT dstVolIndex = (uint32_t)-1;
    uint64_t srcFobCnt = (uint64_t)-1;
    uint64_t srcFobOffset = (uint64_t)-1;
    vdIndexT srcVolIndex = (uint32_t)-1;
    char *path;
    char fs_name[MAXPATHLEN+1] = { 0 };
    char *src_device = NULL;
    char *dst_device = NULL;
    struct stat   stats;
    ssDmnOpT ssDmnCurrentState=0;
    adv_status_t sts;
    adv_bf_dmn_params_t dmnParamStruct = { 0 };
    adv_bf_dmn_params_t *dmnParams = &dmnParamStruct;
    adv_bf_attr_t bfAttributes;
    adv_bf_info_t bfInfo;
    arg_infoT*  infop;

    /* check for root */
    check_root(Prog);
 
    /*
    ** Get user-specified command line arguments.
    */
 
    while ((c = getopt( argc, argv, "Vk:n:s:d:" )) != EOF) {
        switch (c) {
        case 'V':
            Vflg++;
            break;
            
        case 'k':
            k++;
            srcFobOffset = size_str_to_fobs(optarg);
            if (srcFobOffset == (uint64_t)-1) {
                fprintf(stderr, catgets(catd, MS_FSADM_MIGRATE,
                    MIGRATE_BAD_BLOCKCOUNT,
                    "%s: an error occurred processing block count of '%s'.\n"), Prog, optarg);
                migrate_usage();
                exit(1);
            }
            break;

        case 'n':
            n++;
            srcFobCnt = size_str_to_fobs(optarg);
            if (srcFobCnt == (uint64_t)-1) {
                fprintf(stderr, catgets(catd, MS_FSADM_MIGRATE,
                    MIGRATE_BAD_BLOCKCOUNT,
                    "%s: an error occurred processing block count of '%s'.\n"), Prog, optarg);
                migrate_usage();
                exit(1);
            }
            break;

        case 's':
            s++;
            /* check that the arg is a block device */
            if(check_device(optarg, Prog, 0)) {
                migrate_usage();
                exit(1);
            }
            src_device = strdup(optarg);
            break;
  
        case 'd':
            d++;
            /* check that the arg is a block device */
            if(check_device(optarg, Prog, 0)) {
                migrate_usage();
                exit(1);
            }
            dst_device = strdup(optarg);
            break;
            
        default:
            migrate_usage();
            exit( 1 );
        }
    }

    if (k > 1 || n > 1 || s > 1 || d > 1 ) {
        migrate_usage();
        exit( 1 );
    }

    if (optind != (argc - 1)) {
        /* missing required arg */
        migrate_usage();
        exit( 1 );
    }

    /* determine what domain the file resides within */
    /* and get the filesystem specifics */
    path = argv[optind];

    if(get_fsinfo_from_file(path, &infop, &err)) {
        if(err) {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_GEN_NUM_STR_ERR,
                            "%s: Error = [%d] %s\n"),
                    Prog, err, ERRMSG(err));
        }
        else {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_NOTADVFSFILE,
                            "%s: %s is not on an AdvFS file system.\n"),
                    Prog, path);
        }
        exit(1);
    }

    if (infop == NULL) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_NOFSPARAMS,
                        "%s: Cannot get file system parameters for %s\n"),
                Prog, path);
        exit( 1 );
    }

    sts = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if(sts != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_ERR,
                        "%s: Error = %s\n"),
                Prog, BSERRMSG(sts));
        exit( 1 );
    }

    /* now get the volume indices for Source and Destination */

    if (s && src_device != NULL) {
        sts = advfs_get_vol_index(infop->fsname, src_device, &srcVolIndex, NULL, NULL);
        if (sts != EOK) {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_ERR,
                            "%s: Error = %s\n"),
                    Prog, BSERRMSG(sts));
            exit( 1 );
        }
    }

    if (d && dst_device != NULL) {
        sts = advfs_get_vol_index(infop->fsname, dst_device, &dstVolIndex, NULL, NULL);
        if (sts != EOK) {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_ERR,
                            "%s: Error = %s\n"),
                    Prog, BSERRMSG(sts));
            exit( 1 );
        }
    }

    /* now, if -s or -d was specified and their indexes are still not set, then
       the disk was not in this volume set */
    if( s && srcVolIndex == 0 ) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_VOLNOTMEM,
                        "%s: volume '%s' is not a member of file system '%s'\n"),
                Prog, src_device, infop->fsname);
        exit(1);
    }
    if( d && dstVolIndex == 0) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_VOLNOTMEM,
                        "%s: volume '%s' is not a member of file system '%s'\n"),
                Prog, dst_device, infop->fsname);
        exit(1);
    }

    /* if we were passed the -V flag, print completed command line
     * and exit */
    if(Vflg) {
        printf("%s", Voption(argc, argv));
        return(0);
    }
    
    /*
     * Lock this domain.
     */
    thisDmnLock = advfs_fspath_lock(infop->fspath, FALSE, FALSE, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, 
                                     MS_FSADM_COMMON,
                                     COMMON_FSBUSY, 
                                     "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog);
        } else {
            fprintf(stderr, catgets(catd, 
                                    MS_FSADM_COMMON, 
                                    COMMON_ERRLOCK, 
                                    "%s: Error locking '%s'; [%d] %s\n"),
                    Prog, infop->fspath, err, ERRMSG (err) );
        }
        errorFlag = 1;
        goto _error1;
    }

    fd = open( path, O_RDONLY, 0 );
    if (fd < 0) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_NOOPEN,
                                 "%s: open of %s failed. [%d] %s\n"),
                 Prog, path, errno, sys_errlist[errno] );
        errorFlag  = 1;
        goto _error2;
    }
    
    if(file_migrate (Prog, fd, srcVolIndex, srcFobOffset, srcFobCnt, 
                     dstVolIndex, dstBlkOffset))
        errorFlag = 1;

    if (close( fd )) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_NOCLOSE,
                                 "%s: close of %s failed. [%d] %s\n"),
                 Prog, path, errno, sys_errlist[errno] );
        errorFlag = 1;
    }

 _error2:
    if (advfs_unlock( thisDmnLock, &err ) == -1) {
        fprintf(stderr, catgets(catd,
                                MS_FSADM_COMMON,
                                COMMON_ERRUNLOCK,"%s: Error unlocking '%s'; [%d] %s\n"),
                Prog, infop->fspath, err, ERRMSG( err ) );
        errorFlag = 1;
    }

 _error1:
    return errorFlag;

}  /* end main */
/* end migrate.c */
