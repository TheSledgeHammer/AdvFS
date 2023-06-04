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
 *
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    addvol.c - add a new volume to an active or inactive domain.
 *
 *    Note:  If addvol exits with error 1, an error message must be written 
 *           to stderr because of the rsh{ssh now} implementation of cluster 
 *           support. 
 *
 */
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statvfs.h>          /* for checking f_basetype */
#include <locale.h>
#include <syslog.h>
#include <tcr/clu.h>
#include <tcr/clua.h>
#include <tcr/cfs.h>
#include <cfs/cfs_user.h>

#include <advfs/advfs_syscalls.h>
#include <advfs/bs_error.h>
#include <advfs/bs_bmt.h>

#include "common.h"

#define BUFSIZE 8192
#define RETRY_MAX 8

  /*
   *  Function prototypes
   */
  
static int cfs_server(const char *node, const char *domain, char *server_name);

/*                           --> this is a hack until libclu implements this
 *                               function
 */
/* extern int clu_adm_lock(int lock_oper); */
static int
clu_adm_lock(int lock_oper)
{
    return 0;
}

/*
 *  Globals
 */   
static char *Prog = "fsadm addvol";

void
addvol_usage( void )
{
    usage(catgets(catd,
                  MS_FSADM_ADDVOL,
                  ADDVOL_USAGE,
                  "%s [-V] [-f] special fsname\n"),
          Prog);
}

int
addvol_main(int argc, char *argv[])
{    
    extern int    optind;
    extern int    opterr;
    extern char  *optarg;

    char  *linkName = NULL;
    char  *volName = NULL;
    char  *devType = NULL;
    char  *pp;
    char  *tmpp;
    char   tmp1[MAXPATHLEN];
    char   tmp2[MAXPATHLEN];
    char   volmgr_dev[MAXPATHLEN+1];
    char   *vol, *vg;
    char   tmpstr[MAXHOSTNAMELEN + 30];
    char   our_node[MAXHOSTNAMELEN + 1];
    char   server_name[MAXHOSTNAMELEN + 1];
    char   net_name[MAXHOSTNAMELEN + 1];
    char   newarg2[BS_DOMAIN_NAME_SZ + MAXPATHLEN + MAXPATHLEN + 25];
    char  *newargv[4];
    char   buffer[BUFSIZE];
    char  *sh_cmd="/sbin/sh -c ";
    char  *FullPathProg="/usr/sbin/fsadm -Fadvfs addvol";
    char   tmpName[MAXPATHLEN] = { 0 };

    char   volmgrVolGroup[64];
    char   volmgrVolName[MAXPATHLEN+1] = { 0 };
    char   volPathName[MAXPATHLEN+1] = { 0 };
    char  *subs = NULL;
    
    int    c;
    int    fflg = 0;
    int    sflg = 0;
    int    Vflg = 0;
    int    dmnLocked = 0;
    int    err;
    int    clu_locked = 0;
    int    thisDmnLock;
    int    i;
    int    proc_sts=0;
    int    fork_sts=0;
    int    retval=0;
    int    fd;
    int    cnt;
    int    retry_count = 0;              /* Used to limit CFS fail-over retries. */

    off_t     volSize = 0;
    uint32_t  volIndex = -1;
    int32_t   bmtXtntPgs = BS_BMT_XPND_PGS;
    int32_t   bmtPreallocPgs = 0;

    getDevStatusT    error;
    bfDomainIdT      bfDomainId;
    adv_status_t     sts, sts2;
    serviceClassT  volSvc = 0;
    arg_infoT*       infop;

    struct stat           stats;
    struct disklabel     *lab;
    struct clu_gen_info  *clugenptr = NULL;

    /*
     *  ignore interrupt signals
     */

    (void) signal( SIGINT, SIG_IGN );

    /*
     *  check for root
     */     
    check_root(Prog);
    
    /*
     *   Get user-specified command line arguments.
     */

    while ((c = getopt( argc, argv, "Vfs:" )) != EOF) {
        switch (c) {

        case 'V':
            Vflg++;
            break;

        case 'f':
            fflg++;
            break;
            
        case 's':
            sflg++;
            volSize = strtoul(optarg, NULL, 10);
            break;

        case '?':
        default:
            addvol_usage();
            exit( 1 );
        }
    }
 
    /*
     *  check for missing required args
     */
    
    if (optind != (argc - 2)) {
        addvol_usage();
        exit( 1 );
    }

    /* verify it's a valid block device */
    if(check_device(argv[optind], Prog, 0)) {
        addvol_usage();
        exit(1);
    }
    else {
        /* argument is a valid block device, so dup it */
        volName = strdup(argv[optind]);
    }

    /* now get the fsname */
    infop = process_advfs_arg(argv[optind + 1]);
    if(infop == NULL) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_NO_FS,
                        "%s: file system '%s' does not exist\n"),
                "addvol", argv[optind + 1]);

        return(1);
    }
    
    /* if we were passed the -V flag, print completed command line
     * and exit */
    if(Vflg) {
        printf("%s", Voption(argc, argv));
        return(0);
    }
    
    if (clu_is_member()) {
        
        retval = clu_get_info(&clugenptr);
        if (retval || (clugenptr == NULL)) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_CLU_FS_ERR,
                                    "%s: cluster file system error\n"),
                    Prog);
            goto _error; 
        }

        /*
         *  Make sure the volume is not part of the
         *  quorum disk
         */
        if (clugenptr->qdisk_name != NULL) {
            tmpp = strrchr( volName, '/' );
            tmpp = tmpp ? tmpp+1 : volName;
            strncpy(tmp1,tmpp,(strlen(tmpp))-1);
            tmp1[strlen(tmpp)-1] = NULL;
            strncpy(tmp2,clugenptr->qdisk_name,(strlen(clugenptr->qdisk_name))-1);
            tmp2[strlen(clugenptr->qdisk_name)-1] = NULL;
            if (strcmp(tmp1,tmp2) == 0) {
                fprintf(stderr, catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_QRM_DSK_NOT_STG,
                                        "%s: Cannot use any part of the quorum disk for file system storage\n"),
                        Prog);
                goto _error;
            }
        }

        bzero(our_node, MAXHOSTNAMELEN+1); 
        for (i=0; i<=clugenptr->clu_num_of_members-1; i++) {
            if (clugenptr->my_memberid == clugenptr->memblist[i].memberid) {
                if (clugenptr->memblist[i].cnx_nodename != NULL) {
                    strcpy(our_node, clugenptr->memblist[i].cnx_nodename);
                }
                break;
            }
        }

        if (our_node[0] == 0) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_CLU_FS_ERR,
                                    "%s: cluster file system error\n"),
                    Prog);
            goto _error; 
        }
    _retry:

        /*
         *  Limit retries to RETRY_MAX times for rsh{ssh now} failures
         */
           
        if (retry_count++ > RETRY_MAX) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_SSH_FAILURE,
                                    "%s: ssh failure, check that the remote node allows cluster alias access.\n"),
                    Prog);
            goto _error;
        }
        
        sts = cfs_server(our_node, infop->fsname, server_name);

        /*
         * if failover/relocation
         *   retry without being limited to RETRY_MAX iterations
         */
            
        if (sts == -2) {
            retry_count = 0;  
            goto _retry;

            /*
             *  else if a remote server
             *    use rsh{ssh now} to execute over on the server
             */
              
        } else if (sts == 0) {
            bzero(net_name, MAXHOSTNAMELEN+1); 
            for (i=0; i<=clugenptr->clu_num_of_members-1; i++) {
                if (clugenptr->memblist[i].cnx_nodename == NULL) {
                    continue;
                }
                if (!strcmp(clugenptr->memblist[i].cnx_nodename, server_name)) {
                    strcpy(net_name, clugenptr->memblist[i].cluintername);
                    break;
                }
            }

            if (net_name[0] == 0) {
                fprintf(stderr, catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_CLU_FS_ERR,
                                        "%s: cluster file system error\n"),
                        Prog);
                goto _error; 
            }
            /*
             *  The rsh{ssh now} command is used (temporarily) to provide cluster support.
             *  Because the exit code of the addvol rsh{ssh}'ed to the current server
             *  cannot be determined, we first try to redirect sterr of the rsh{ssh}'ed
             *  addvol to a temporary file where when can examine it to determine
             *  success or failure.  Since dealing with files can introduce many
             *  additional failure points, we try to create the temp file before
             *  actually doing the rsh{ssh}.  If we can't create the file (no space, etc),
             *  stderr is redirected to stout and we just live with the fact that
             *  the correct exit code cannot be determined (0 is returned).  If the
             *  temp file can be created, we redirect stderr to it and check it when
             *  the rsh{ssh} fork exits. To fix a potential security hole, the
             *  file is created with root privledges but is truncated
             *  before being passed as a parameter to the rsh{ssh}'d rmvol
             *  command.
             *
             *  The command here is:
             *    /usr/sbin/ssh rhost \
             *    "/sbin/sh -c '/usr/sbin/fsadm -Fadvfs addvol special fsname 2>>/tempfilename'"
             *  or ,
             *    /usr/sbin/ssh rhost \
             *    "/sbin/sh -c '/usr/sbin/fsadm -Fadvfs addvol special fsname 2>&1'"
             *              *
             *  (Double quotes are not used but using one argument for what's inside
             *  the double quotes yields the same effect.)
             */
#ifdef LATER_SSH
            newargv[0] = "/usr/bin/ssh";
#else
            newargv[0] = "/usr/bin/remsh";
#endif
            newargv[1] = net_name;
            sprintf(newarg2, "%s '%s ", sh_cmd, FullPathProg);
            for (i=1; i<argc; i++) {
                strcat(newarg2, argv[i]);
                strcat(newarg2, " ");
            }
            
            /*
             *  Compose the pathname of the temporary file
             */

            sprintf(tmpName, "/.cdsl_files/member%i/tmp/addvol.XXXXXX", clugenptr->my_memberid);

            /*
             *  Try and create the temp file now
             */

            retval = 0;
            if ((fd = mkstemp(tmpName)) < 0) {
                retval = fd;
                fprintf(stderr, catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_MKSTEMP_FAILED,
                                        "%s: Cannot create temporary file, errors from remote operation will not be reported\n"),
                        Prog);
            } else {
                /* Write to make sure space is available */
                retval = write(fd, buffer, BUFSIZE);
            }            

            /*
             *  If file created,
             *    redirect to the temp file            
             *  Else,
             *    redirect to stdout
             */
               
            if (retval > 0 ) {
                strcat(newarg2, "2>>");
                strcat(newarg2, tmpName);
                strcat(newarg2, "'");
                /* If we are going to use the file to get strerr */
                /* then truncate the length */
                ftruncate(fd, 0);
            } else {
                strcat(newarg2, "2>");
                strcat(newarg2, "&1'");
                tmpName[0] = 0;
            }           
            
            /* Close the file if fd is valid */
            if (fd > 0) {
                close ( fd );
            }
            
            newargv[2] = newarg2;
            newargv[3] = NULL;

            fork_sts = fork();
            if (!fork_sts) {

                /*
                 *  This is the child ... 
                 *  close stderr to prevent rsh error msgs 
                 *  addvol stderr redirected 
                 */

                close(2);
                execv(newargv[0], newargv); 

            } else if (fork_sts == -1) {

                /*
                 *  This is the parent ...
                 *    Bad fork
                 */
                  
                fprintf(stderr, catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_FORK_ERR,
                                        "%s: forking error\n"),
                        Prog);
                goto _error;
                 
            } else {

                /*
                 *  This is the parent ...
                 *    Wait for child to complete, check results
                 */
                     
                wait(&proc_sts);

                /*
                 *  If the rsh command failed, retry
                 */
                   
                if (proc_sts != 0) {                /* bad rsh */
                    goto _retry;

                    /*
                     *  rsh succeeded, check results
                     */
                   
                } else {

                    /*
                     *  If temp file couldn't be created
                     *    set exit code to 0
                     *    (stderr was redirected to stdout)
                     *
                     *  Else If temp file can't be opened
                     *    (the file should have been created,
                     *     even if it's empty)
                     *    output error message
                     *    set exit code to 1
                     *
                     *  Else, file exists
                     *    If non-zero length,
                     *      write contents to stderr
                     *      set exit code to 1
                     *    remove file
                     */

                    if (tmpName[0] == 0) {
                        retval = 0;
                    } else if ((fd = open(tmpName, O_RDONLY, 0)) < 0) {
                        fprintf(stderr, catgets(catd,
                                                MS_FSADM_COMMON,
                                                COMMON_CLU_FS_ERR,
                                                "%s: cluster file system error\n"), Prog);
                        retval = 1;
                    } else {
                        retval = 0;
                        while ((cnt = read(fd, buffer, BUFSIZE)) > 0) {
                            write(fileno(stderr), buffer, cnt);
                            retval = 1;
                        }
                        close (fd);

                        remove(tmpName);
                    }
                    exit(retval);
                }
            }

            /*
             *  else if cluster error
             *    inform and exit
             */
     
        } else if (sts == -1) { /* cluster error */
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_CLU_FS_ERR,
                                    "%s: cluster file system error\n"), Prog);
            goto _error; 
        }
    }

    /*
     *  sts == 1;
     *  Local server, continue to execute here
     */    
    /*
     *  Lock this domain.
     */

    thisDmnLock = advfs_fspath_lock (infop->fspath, FALSE, FALSE, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                     catgets(catd,
                             MS_FSADM_COMMON,
                             COMMON_FSBUSY,
                             "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERRLOCK2,
                                     "%s: Error locking '%s'\n"),
                     Prog, infop->fspath);
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_GEN_NUM_STR_ERR,
                                     "%s: Error = [%d] %s\n"),
                     Prog, err, sys_errlist[err]);
        }
        goto _error;
    }
    
    dmnLocked = 1;

    /*  Check to see if this storage domain is multi-volume enabled */
    if (!infop->is_multivol) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOTMULTI,
            "%s: Storage domain %s is not enabled for multi-volume operations.\nPlease run fsadm -F advfs multi\n"),
            Prog, infop->fsname);
        advfs_unlock(thisDmnLock, &err);
        free(infop);
        return(1);
    }

    /*
     * Build the symlink for the volume
     */
    if( !advfs_checkvol( volName, volmgrVolGroup, volmgrVolName ) ) {             
        /* This is a regular device */
        subs = (char *) strrchr (volName, '/');
        if (subs == NULL) {
            fprintf( stderr,
                     catgets( catd,
                              MS_FSADM_COMMON,
                              COMMON_SLASH,
                              "%s: Error looking for '/' in %s\n"),
                     Prog, volName );
            goto _error;
        }
        snprintf(volPathName, MAXPATHLEN, "%s%s", infop->stgdir, subs);
        
    } else {
        
        /* This is a volume manager volume.  Volume manager volumes 
         * are not supported in TCR/CFS clusters, so fail if this is
         * a TCR shared filesystem
         */
        if (infop->tcr_shared) {   
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_TCRLVM,
                "%s:  Volume manager device found (%s).\nVolume manager devices are not permitted in a cluster shared filesystem.\n"),
                Prog, volName);
            goto _error;
        }

        snprintf( volPathName, MAXPATHLEN-strlen(volPathName), "%s/%s.%s",
                  infop->stgdir, volmgrVolGroup, volmgrVolName );
    }
    
    linkName = strdup(volPathName);
    
    if (!lstat( linkName, &stats )) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_LINK_EXISTS,
                                 "%s: link '%s' already exists\n"),
                 Prog, linkName );
        goto _error;
    }
    
    if (!volume_avail( Prog, infop->fsname, volName )) {
        goto _error;
    }

    /* verify (as best possible) that this device isn't claimed by LVM 
     * or another filesystem */
    if(!fflg && isatty(fileno(stdin))) {
        /* used for confirmation while loop (below) */
        int i = 0;
        int confirm = 0;
        char buf[256] = { 0 };
        struct statvfs vfsbuf = { 0 };

        /* statvfsdev on the volume in an attempt to determine if it is
           used by another filesystem */
        if((statvfsdev(volName, &vfsbuf)) == 0) {
            fprintf(stdout,
                    catgets(catd,
                            MS_FSADM_ADDVOL,
                            ADDVOL_CONFIRM,
                            "The device '%s' appears to be marked as containing a file system of type %s.\n"),
                    volName, vfsbuf.f_basetype);
            confirm = 1;
        }
        else if(advfs_is_lvmdev(volName)) {
            fprintf(stdout,
                    catgets(catd,
                            MS_FSADM_ADDVOL,
                            ADDVOL_CONFIRM_LVM,
                            "The device '%s' appears to be marked as part of a logical volume.\n"),
                    volName);
            confirm = 1;
        }
        
        if(confirm) {
            do {            
                fprintf(stdout,
                        catgets(catd,
                                MS_FSADM_ADDVOL,
                                ADDVOL_CONFIRM2, "CONTINUE? [y/n] "));
            
                (void) fflush(stdout);
                if((fgets(buf, 256, stdin)) == NULL) {
                    goto _error;
                }
                i = 0;
                while (buf[i] != '\n' &&
                       buf[i] != 'Y' &&
                       buf[i] != 'y') {
                    if (buf[i] == 'n' || buf[i] == 'N') {
                        goto _error;
                    }
                    i++;
                }
            } while (buf[i] != 'y' && buf[i] != 'Y');
        }
    }

    if (!sflg || !volSize) {
        error = get_dev_size( volName, devType, &volSize );
        if (error != GDS_OK) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_ADDVOL,
                                    ADDVOL_NO_DEV_SIZE,
                                    "%s: Cannot get device size; %s"),
                    Prog, GDSERRMSG( error ) );
            if (errno != 0) {
                fprintf( stderr, "; %s", BSERRMSG( errno ) );
            }
            fprintf( stderr, "\n" );
            goto _error;
        }
    }

    /*
     *  Disable interrupts.
     */
     
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    /*
     *  If this is root domain in a cluster, get the locks to
     *  update cluster database.
     */

    if (clu_is_member() && (strcmp(infop->fsname, "cluster_root") == 0)) {

        if (clu_adm_lock(CLU_ADM_LOCK)) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_CLUROOT_LCK_FAIL,
                                    "%s: clu_adm_lock failed to get lock for cluster root\n"),
                    Prog);
            goto _error;
        }
        clu_locked = TRUE;
    }
    
    sts = advfs_add_volume(infop->fsname, 
                          volName,
                          &volSvc,
                          volSize,
                          bmtXtntPgs,
                          bmtPreallocPgs,
                          &bfDomainId,
                          &volIndex);

                        
    if (sts != EOK) {
        fprintf(stderr, catgets(catd,
                                MS_FSADM_COMMON,
                                COMMON_ERR,
                                "%s: Error = %s\n"),
                Prog, BSERRMSG(sts));
        goto _error;
    }

    /*
     *  Relinquish cluster database locks for cluster root.
     */

    if (clu_locked) {
        /* Run clu_bdmgr to update clu_bdmgr.conf files */
        if(system("/usr/sbin/clu_bdmgr -u") != 0)
            fprintf(stderr, catgets(catd, 1, 43, "%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"), Prog);
        clu_adm_lock(CLU_ADM_UNLOCK);
    }
    
    if (symlink( volName, linkName )) {
        fprintf(stderr, catgets(catd,
                                MS_FSADM_ADDVOL,
                                ADDVOL_SYMLINK_ERR,
                                "%s: Cannot create symlink '%s @--> %s'; [%d] %s\n"),
                Prog, linkName, volName, errno, sys_errlist[errno] );
        goto _error;
    }

    sts = advfs_add_vol_to_svc_class(infop->fsname, volIndex, volSvc);

    if (sts != EOK) {
        fprintf(stderr, catgets(catd,
                                MS_FSADM_ADDVOL,
                                ADDVOL_SVC_ERR,
                                "%s: Cannot add volume '%s' into svc class\n"),
                Prog, volName);
        fprintf(stderr, catgets(catd,
                                MS_FSADM_COMMON,
                                COMMON_ERR,
                                "%s: Error = %s\n"),
                Prog, BSERRMSG(sts));

        fprintf(stderr, catgets(catd,
                                MS_FSADM_ADDVOL,
                                ADDVOL_ACTIVATE,
                                "use 'fsadm -Fadvfs chio -o activate %s %s' to activate the volume\n"
                                ),
                volName, infop->fsname);
        
        goto _error;
    }

    (void)advfs_add_vol_done(infop->fsname, volName); /* Ignore status */

    /*  Lastly, set the multi-vol flag on this new device */
    (void)advfs_set_multivol(volName);

    return 0;

    /*
     *  Error exits
     */
     
 _error:

    if (clu_locked) {
        clu_adm_lock(CLU_ADM_UNLOCK);
    }

    if(thisDmnLock) {
        if(advfs_unlock( thisDmnLock, &err ) == -1) {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_ERRUNLOCK,
                            "%s: Error unlocking '%s'; [%d] %s\n"),
                    Prog, infop->fspath);
        }
    }

    fprintf(stderr, catgets(catd,
                            MS_FSADM_ADDVOL,
                            ADDVOL_FAILURE,
                            "%s: Cannot add volume '%s' to file system '%s'\n"),
            Prog, volName, infop->fsname); 
    exit(1);
}

/* 
 * cfs_server()
 *
 * Any changes to this routine: please see rmvol's cfs_server logic
 * also 
 *
 * Return values:
 *      -2: failover/relocation
 *      -1: cfs error
 *       0: remote server
 *       1: local server
 *
 *
 */

static int
cfs_server(const char *node, const char *fs_name, char *server_name)
{
    cfs_status_t cfs_status;
    cfs_server_info_t info;
    cfs_request_info_t reqInfo;

    reqInfo.cfs_req_ver  = CFS_REQ_INFO_V1;
    reqInfo.cfs_path = fs_name;
    reqInfo.cfs_mounted_to = NULL;
    reqInfo.cfs_mounted_from = NULL;
    reqInfo.cfs_fs_name = NULL;
    reqInfo.cfs_attribute = CFS_SERVER;
    reqInfo.cfs_op = CLU_GET;
    reqInfo.cfs_flags = CFS_DOMAIN_NAME;
    reqInfo.cfs_attr_buf = &info;
    reqInfo.cfs_attr_buf_len = sizeof(info);
    reqInfo.cfs_nodename = NULL;
    
    cfs_status = clu_filesystem(&reqInfo, CFS_REQ_INFO_V1_SIZE);

    if (cfs_status != CFS_S_OK) {
        syslog(LOG_ERR, "clu_filesystem error: %d\n", cfs_status);
        return -1;
    }

    if (info.cfs_header.cfs_status != CFS_S_OK) {
        switch(info.cfs_header.cfs_status) {
        case CFS_S_NOT_MOUNTED:           
            return 1;       /* local host will be server */
        case CFS_S_FAILOVER_IN_PROGRESS:
        case CFS_S_RELOCATION_IN_PROGRESS:
            return -2;      
        default:
            syslog(LOG_ERR, "CLU_CFS_STATUS: %d\n", info.cfs_header.cfs_status);
            return -1;
        }
    }

    if (strcmp(node, (strtok(info.cfs_name, ".")))) {
        strcpy(server_name, info.cfs_name);
        return 0;
    }

    return 1;
}


