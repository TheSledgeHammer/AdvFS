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
 *    AdvFS Storage System
 *
 * Abstract:
 *
 *    rmvol.c
 *    This module implements the "remove volume" utility.  It removes a
 *    volume from a filesystem by moving all the metadata and extents to
 *    other disks in the filesystem.
 *
 *    Note:  If rmvol exits with error 1, an error message must be written 
 *           to stderr because of the ssh implementation of cluster support. 
 *
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <locale.h>
#include <syslog.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/advfs_evm.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <tcr/clu.h>
#include <tcr/clua.h>
#include <tcr/cfs.h>
#include "common.h"

#define BUFSIZE 8192
#define RETRY_MAX 8

static int cfs_server(const char *node, const char *fsname, char *server_name);
/*                           --> this is a hack until libclu implements this
 *                               function
 */
/* extern int clu_adm_lock(int lock_oper); */
static int
clu_adm_lock(int lock_oper)
{
    return 0;
}

typedef struct {
    uint32_t fobOffset;
    uint32_t fobCnt;
    uint32_t holeFobCnt;
} xtntDescT;

char *sh_cmd="/sbin/sh -c ";

/* Full path name of rmvol.  For cluster use with ssh. */
static char *FullPathProg="/usr/sbin/fsadm -F advfs rmvol";

static int addVolSvcClassFlag = 0, clu_locked = 0;
static arg_infoT*  infop;
static bfDomainIdT domainId;
static char *dotTags = "/.tags";
static int forceFlag = 0;
static char *Prog = "fsadm rmvol";
static serviceClassT targetSvcClass;
static uint32_t targetVolIndex;
static int unmountFlag = 0;
static int verboseFlag = 0;
static int veryVerboseFlag = 0;
static int verifyFlag = 0;
static char *volName = NULL;
static char  specialBlock[MAXPATHLEN];
static char  specialChar[MAXPATHLEN];
static adv_bf_dmn_params_t dmnParams;

static advfs_ev advfs_event;

static struct {
        setInfoT *setHdr;
        bfTagT bfTag;
        char pathName[MAXPATHLEN+1];
        } currentPath;

/*
 * Private prototypes.
 */
static
int
remove_volume        (arg_infoT* infop,             /* in  */
                      char *volName);               /* in  */
  
static
int
move_metadata        (bfDomainIdT domainId,         /* in  */
                      uint32_t volIndex,            /* in  */
                      int *retFileCnt);             /* out */

static
adv_status_t
move_normal_metadata (int fd,                       /* in  */
                      bfTagT bfSetTag,              /* in  */
                      bfTagT bfTag,                 /* in  */
                      uint32_t volIndex,            /* in  */
                      metadataTypeT mtype);         /* in  */

static
adv_status_t
migrate              (int fd,                       /* in  */
                      uint32_t srcVolIndex,         /* in  */
                      uint32_t bfPageOffset,        /* in  */
                      uint32_t bfPageCnt,           /* in  */
                      uint32_t dstVolIndex);        /* in  */

static
adv_status_t
create_xtnt_desc_list (int fd,                      /* in  */
                       uint32_t volIndex,           /* in  */
                       int xtntMapIndex,            /* in  */
                       int *retXtntDescCnt,         /* out */
                       xtntDescT **retXtntDesc      /* out */
                       );

static
adv_status_t
extend_xtnt_desc_list (int *maxCnt,                 /* in/out */
                       xtntDescT **xtntDesc);       /* in/out */

static
void
display_tag           (bfTagT bfSetTag,             /* in  */
                       bfTagT bfTag);               /* in  */

static
int
cfs_server            (const char *node,            /* in  */
                       const char *fs_name,         /* in  */
                       char *server_name);          /* out */


/*
 * This is the remove volume signal handler.  It gives this program a chance
 * to cleanup before quiting.  Specifically, it unmounts bitfile sets, adds the
 * disk back into the service class and unlocks the filesystem.
 */

void sigHandler (int signal)                        /* in */

{

    int err;
    adv_status_t sts;

    fprintf (stderr, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_STOP,
                             "%s: Stopping rmvol operation...\n"), Prog);

    switch (signal) {

      case SIGINT:
      case SIGQUIT:
      case SIGPIPE:
      case SIGTERM:
        break;

      default:
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_UKNSIG,
                                 "%s: Unexpected signal: %d\n"), Prog, signal);
        break;
    }  /* end switch */

    if (addVolSvcClassFlag != 0) {
        addVolSvcClassFlag = 0;
        sts = advfs_add_vol_to_svc_class (
                                          infop->fsname,
                                          targetVolIndex,
                                          targetSvcClass
                                          );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_BADADDVOLSVCCLS,
                                     "%s: Cannot add volume '%s' back into svc class\n"),
                     Prog, volName); 
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_REACTIVATEVOL,
                                     "use 'fsadm -F advfs chio -o activate %s %s' to reactivate the volume\n"),
                     volName, infop->fsname);
        }
    }
    if (unmountFlag != 0) {
        unmountFlag = 0;
        (void)close_dot_tags(Prog);
    }

    fprintf (stderr, catgets(catd, MS_FSADM_RMVOL, RMVOL_NOTREMOVED,
        "%s: Volume '%s' not removed from file system '%s'\n"),
        Prog, volName, infop->fsname); 

    advfs_post_user_event(EVENT_FDMN_RMVOL_ERROR, advfs_event, Prog);
    
    exit (1);

}  /* end sigHandler */

void
rmvol_usage (void)
{
    usage (catgets(catd,
                   MS_FSADM_RMVOL,
                   RMVOL_USAGE,
                   "%s [-V][-v][-f] special fsname\n"),
           Prog);
}

int
rmvol_main (int argc,
            char *argv[])

{

    int c;
    int err;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    struct stat stats;
    char *newargv[4];
    char tmpstr[MAXHOSTNAMELEN + 30];
    char our_node[MAXHOSTNAMELEN + 1];
    char server_name[MAXHOSTNAMELEN + 1];   
    char net_name[MAXHOSTNAMELEN + 1];
    char newarg2[BS_DOMAIN_NAME_SZ + MAXPATHLEN + 25];
    struct clu_gen_info *clugenptr;
    int i, sts, proc_sts=0, fork_sts=0, retval=0;
    int cnt, fd;
    int retry_count = 0;
    char   buffer[BUFSIZE];
    char   tmpName[MAXPATHLEN] = { 0 };
    char lsm_dev[MAXPATHLEN+1];
    char *temp, *vol, *dg;

    /* check for root */
    check_root( Prog );

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt (argc, argv, "fvV")) != EOF) {
        switch (c) {

          case 'f':
            forceFlag = 1;
            break;

          case 'v':
            if(verboseFlag == 1) {
                /* if we have already seen the -v flag,
                 * set veryVerboseFlag */
                veryVerboseFlag = 1; 
            }
            verboseFlag = 1;
            break;

          case 'V':
            verifyFlag = 1;
            break;

          default:
            rmvol_usage ();
            exit (1);
        }
    }  /* end while */

    if (optind != (argc - 2)) {
        /* missing required args */
        rmvol_usage ();
        exit (1);
    }

    if(check_device(argv[optind], Prog, 0)) {
        rmvol_usage();
        exit(1);
    }
    volName = strdup(argv[optind]);

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
    if(verifyFlag) {
        printf("%s", Voption(argc, argv));
        return(0);
    }

    if (clu_is_member()) {

        /*
         * If in a cluster, we need to determine the server
         * and execute either locally or remotely as appropriate
         *
         * If any changes are made to this logic, please check
         * addvol's cfs logic also
         */

        retval = clu_get_info(&clugenptr);
        if (retval || (clugenptr == NULL)) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_CLU_FS_ERR,
                                    "%s: cluster file system error\n"),
                    Prog);
            goto _error1; 
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
            goto _error1;
        }

_retry:

        /*
         *  Limit retries to RETRY_MAX times for ssh failures
         */
           
        if (retry_count++ > RETRY_MAX) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_SSH_FAILURE,
                                    "%s: ssh failure, check that the remote node allows cluster alias access.\n"),
                    Prog);
            goto _error1;
        }

        sts = cfs_server(our_node, infop->fsname, server_name);
        
        if (sts == -2) {       /* failover/relocation */
            retry_count = 0;   /* no limit for failover retries */
            goto _retry;
            
        } else if (sts == 0) { /* remote server */
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
                goto _error1;
            }

            /*
             *  The ssh command is used (temporarily) to provide cluster support.
             *  Because the exit code of the rmvol ssh'ed to the current server
             *  cannot be determined, we first try to redirect sterr of the ssh'ed
             *  addvol to a temporary file where when can examine it to determine
             *  success or failure.  Since dealing with files can introduce many
             *  additional failure points, we try to create the temp file before
             *  actually doing the ssh.  If we can't create the file (no space, etc),
             *  stderr is redirected to stout and we just live with the fact that
             *  the correct exit code cannot be determined (0 is returned).  If the
             *  temp file can be created, we redirect stderr to it and check it when
             *  the ssh fork exits. To fix a  potential security hole, the file
             *  is created with root privledges but is truncated before being 
             *  passed as a parameter to the ssh'ed rmvol command.
             *
             *
             *  The command here is:
             *    /usr/sbin/ssh rhost "/sbin/sh -c '/usr/sbin/fsadm -Fadvfs rmvol special fsname 2>>/tempfilename'"
             *  or ,
             *    /usr/sbin/ssh rhost "/sbin/sh -c '/usr/sbin/fsadm -Fadvfs rmvol special fsname 2>&1'"
             * 
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

            sprintf(tmpName, "/.cdsl_files/member%i/tmp/rmvol.XXXXXX", clugenptr->my_memberid);

            /*
             *  Try and create the temp file now
             */

            if ((fd = mkstemp(tmpName)) < 0) {
              strcat(newarg2, "2>");
              strcat(newarg2, "&1'");
              fprintf(stderr, catgets(catd,
                                      MS_FSADM_COMMON,
                                      COMMON_MKSTEMP_FAILED,
                                      "%s: Cannot create temporary file, errors from remote operation will not be reported\n"),
                      Prog);
              tmpName[0] = 0;
            } else {
              strcat(newarg2, "2>>");
              strcat(newarg2, tmpName);
              strcat(newarg2, "'");
            } 
            
            /* Close file if the fd is valid */
            if (fd > 0) {
                close(fd);
            }

            newargv[2] = newarg2;
            newargv[3] = NULL;

            fork_sts = fork();
            if (!fork_sts) {

             /* 
              * close stderr to prevent ssh error msgs 
              * rmvol stderr redirected
              */

                close(2);       
                execv(newargv[0], newargv);

            } else if (fork_sts == -1) {
                fprintf(stderr, catgets(catd,
                                        MS_FSADM_COMMON,
                                        COMMON_FORK_ERR,
                                        "%s: forking error\n"),
                        Prog);
                goto _error1;
            } else {
                wait(&proc_sts);
                if (proc_sts != 0) {	/* bad ssh */
                    goto _retry;        

                /*
                 *  ssh succeeded, check results
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
                                                "%s: cluster file system error\n"),
                                Prog);
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
                                    "%s: cluster file system error\n"),
                    Prog);
            goto _error1;
        }
        /* sts == 1: Local server; continue */
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    err = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if (err != EOK) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ONDISK,
                                 "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        exit(1);
    }

    /* setup the advfs event for communication with EVM */
    init_event(&advfs_event);
    advfs_event.special = volName;
    advfs_event.domain = infop->fsname;
    
    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    fprintf (stdout, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_REMOVING,
                             "%s: Removing volume '%s' from file system '%s'\n"),
             Prog, volName, infop->fsname); 
    (void)fflush(stdout);
    
    err = remove_volume (infop, volName);
    if (err != 0) {
        goto _error;
    }

    fprintf (stdout, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_REMOVED,
                             "%s: Removed volume '%s' from file system '%s'\n"),
             Prog, volName, infop->fsname); 
    (void)fflush(stdout);

    /* post an EVM event */
    advfs_post_user_event(EVENT_FDMN_RMVOL_UNLOCK, advfs_event, Prog);
   
    return 0;

_error:

    /* post an event with EVM */
    advfs_post_user_event(EVENT_FDMN_RMVOL_ERROR, advfs_event, Prog);

 _error1:
    
    fprintf (stderr, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_BADREMOVE,
                             "%s: Cannot remove volume '%s' from file system '%s'\n"),
             Prog, volName, infop->fsname); 

    return 1;

}  /* end main */


/*
 * remove_volume
 *
 * This function removes the specified volume from the filesystem.
 */

static
int
remove_volume (
               arg_infoT *infop,  /* in */
               char *volName      /* in */
               )

{

    int thisDmnLock = -1;
    int err;
    int fileCnt;
    int foundFlag;
    int i;
    struct stat stats;
    adv_status_t sts;
    char *subs;
    adv_vol_counters_t volCounters;
    uint32_t *volIndex = NULL;
    int volIndexCnt;
    int volIndexI = BS_MAX_VDI + 1; /* set to invalid value, only reset if
                                       volume is found in the file system */
    adv_vol_info_t volInfo;
    adv_vol_ioq_params_t volIoQParams;
    char volLink[MAXPATHLEN+1] = { 0 };
    char volPathName[MAXPATHLEN + 1] = { 0 };
   
    adv_vol_prop_t volProp;
    int errorFlag;
    char volDiskGroup[64] = { 0 };
    char lsmName[MAXPATHLEN+1] = { 0 };
    uint32_t forceMigrateFlag = 0;
    char   lvmVolGroup[64] = { 0 };
    char   lvmVolName[MAXPATHLEN] = { 0 };
    
    /*
     * Lock this filesystem.
     */
    thisDmnLock = advfs_fspath_longrun_lock (infop->fspath, 
                                             FALSE, 
                                             TRUE, 
                                             &err);
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
                                     COMMON_ERRLOCK,
                                     "%s: Error locking '%s'; [%d] %s\n"),
                     Prog, infop->fspath, err, ERRMSG(err));
        }
        goto _error;
    } else {
        advfs_post_user_event(EVENT_FDMN_RMVOL_LOCK, advfs_event, Prog);
    }

    sts = advfs_get_dmnname_params (infop->fsname, &dmnParams);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_NOFSPARAMS,
                                 "%s: Cannot get file system parameters for %s\n"),
                 Prog, infop->fsname);
        if (sts != -1) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, BSERRMSG (sts));
        } else {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, ERRMSG (errno));
        }
        goto _error;
    }

    /*
     * Translate the volume name into a special device pathname.  Verify that
     * it is a valid device name.
     */
    if( !advfs_checkvol( volName, lvmVolGroup, lvmVolName ) ) {
        /* This is a regular device */
        subs = (char *) strrchr (volName, '/');
        if (subs == NULL) {
            fprintf( stderr,
                     catgets( catd,
                              MS_FSADM_COMMON,
                              COMMON_DEVPATH,
                              "%s: full device path must be specified.\n"),
                     Prog );
            goto _error;
        }
        /* This is NOT volume manager volume */
        snprintf( volPathName, MAXPATHLEN, "%s/%s",
                  infop->stgdir, subs+1 );
        
    } else {
        
        /* This is a volume manager volume */
        snprintf( volPathName, MAXPATHLEN, "%s/%s.%s",
                  infop->stgdir, lvmVolGroup, lvmVolName );
        
    }
    
    err = lstat (volPathName, &stats);
    if (err != 0) {
        if (errno == ENOENT) {
            fprintf(stderr, catgets(catd,
                                    MS_FSADM_COMMON,
                                    COMMON_VOLNOTMEM,
                                    "%s: volume '%s' is not a member of file system '%s'\n"),
                    Prog, volName, infop->fsname);
        } else {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_STATS,
                                     "%s: Error getting '%s' stats; [%d] %s\n"),
                     Prog, volPathName, errno, ERRMSG(errno));
        }
        goto _error;
    }
    
    err = readlink (volPathName, volLink, MAXPATHLEN+1);
    if (err < 0) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERRSYMLINK,
                                 "%s: Error getting '%s' symbolic link; [%d] %s\n"),
                 Prog, volPathName, errno, ERRMSG(errno));
        goto _error;
    }

    /*
     * Translate the special device pathname into a volume index.
     */

    volIndex = (uint32_t *) malloc (dmnParams.curNumVols * sizeof (uint32_t));
    if (volIndex == NULL) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_VOLARRAYERR,
                                 "%s: Cannot allocate memory for volume index array\n"),
                 Prog);
        goto _error;
    }

    sts = advfs_get_dmn_vol_list (
                                 dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 &(volIndex[0]),
                                 &volIndexCnt
                                 );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_FSMOUNT,
                                 "%s: file system must be mounted\n"),
                 Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_NOFSLIST,
                                 "%s: Cannot get file system's volume list\n"),
                 Prog);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERR,
                                 "%s: Error = %s\n"),
                 Prog, BSERRMSG (sts));
        goto _error;
    }

    if (volIndexCnt == 1) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ONLYONEVOL,
                                 "%s: Only one volume in file system\n"),
                 Prog);
        goto _error;
    }

    for (i = 0; i < volIndexCnt; i++) {
        sts = advfs_get_vol_params (
                                    dmnParams.bfDomainId,
                                    volIndex[i],
                                    &volInfo,
                                    &volProp,
                                    &volIoQParams,
                                    &volCounters
                                    );
        if (sts != EOK) {
            if (sts == EBAD_VDI) {
                /* not an error, array is sparse */
                continue;
            }

            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_NOVOLPARAMS,
                                     "%s: Cannot get volume parameters for %s\n"),
                     Prog, volName);
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, BSERRMSG (sts));
            goto _error;
        }
        if (strcmp (volLink, volProp.volName) == 0) {
            /*
             * Matching name!
             */
            volIndexI = i;
            break;  /* out of for */
        }
    }  /* end for */

    if (volIndexI >= volIndexCnt) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_VOLNOTMEM,
                                 "%s: volume '%s' is not a member of file system '%s'\n"),
                 Prog, volName, infop->fsname);
        goto _error;
    }

    /*
     * Mount all bitfile sets in the filesystem.  The function also opens each bitfile
     * set's ".tags" directory to ensure that the set stays mounted while the volume
     * is removed.
     */
    err = open_dot_tags(infop, dmnParams.bfSetDirTag, NULL, Prog);
    if (err != 0) {
        goto _error;
    }
    unmountFlag = 1;

    /*
     * Remove the volume from service so that mcells or storage can be allocated
     * for normal bitfiles.  We handle striped bitfiles later.
     */
    sts = advfs_remove_vol_from_svc_class (
                                           infop->fsname,
                                           volInfo.volIndex,
                                           volProp.serviceClass
                                           );
    if (sts != EOK) {
        if (sts == EBAD_VDI) {
            fprintf (stderr,
                     catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_INCOMPLETERMVOL,
                             "%s: An earlier rmvol did not completely remove volume '%s'.\n"),
                     Prog, volName);
            fprintf (stderr,
                     catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_REACTIVATEVOL,
                             "use 'fsadm -F advfs chio -o activate %s %s' to reactivate the volume\n"));
        } else {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_BADSVCREMOVE,
                                     "%s: Cannot remove volume from service; [%d] %s\n"),
                     Prog, sts, BSERRMSG(sts));
        }
        goto _error;
    }
    addVolSvcClassFlag = 1;

    /*
     * Set values needed by the signal handler and enable interrupts.
     */

    domainId = dmnParams.bfDomainId;
    targetVolIndex = volInfo.volIndex;
    targetSvcClass = volProp.serviceClass;

    signal (SIGINT, sigHandler);
    signal (SIGQUIT, sigHandler);
    signal (SIGPIPE, sigHandler);
    signal (SIGTERM, sigHandler);

    /*
     * Move all normal (non-reserved) metadata and storage off of the
     * disk.
     */
    err = move_metadata (dmnParams.bfDomainId, volInfo.volIndex, &fileCnt);
    while ((err == 0) && (fileCnt > 0)) {
        /*
         * FIX - Even though the disk has been removed from service,
         * the AdvFS kernel does not prevent the allocation of mcells
         * on this disk.  It does its best but some allocations slip thru
         * if the system is busy.  So, make multiple passes thru the bmt.
         * This should really be fixed in the kernel so that when the disk
         * is removed from the service class, the caller is guaranteed that
         * no mcells or storage can be allocated on the disk.
         */
        err = move_metadata (dmnParams.bfDomainId, volInfo.volIndex, &fileCnt);
    }

    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    if (err != 0) {
        goto _error;
    }

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
    
retry_rem_vol:
    sts = advfs_remove_volume (dmnParams.bfDomainId,
                               volInfo.volIndex,
                               forceMigrateFlag
                              );
    if (sts != EOK) {
        if (sts == E_ROOT_TAGDIR_ON_VOL) {
            if (switch_root_tagdir(volIndexI, volIndexCnt, volIndex))
                goto _error;
            else
                goto retry_rem_vol;
        } else if (sts == E_LOG_ON_VOL) {
            if (switch_log(volIndexI, volIndexCnt, volIndex))
                goto _error;
            else
                goto retry_rem_vol;
        }
        if ( (!forceMigrateFlag) && (sts == E_INVOLUNTARY_ABORT) ) {
            forceMigrateFlag = 1;
            goto retry_rem_vol;
        }
        if (sts == E_BMT_NOT_EMPTY) {
            /* FIX - one final pass to display what isn't off. */
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_BADREMOVE,
                                     "%s: Cannot remove volume '%s' from file system '%s'\n"),
                     Prog, volName, infop->fsname);
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, BSERRMSG (sts));
        } else {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_BADREMOVE,
                                     "%s: Cannot remove volume '%s' from file system '%s'\n"),
                     Prog, volName, infop->fsname);
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, BSERRMSG (sts));
        }
        goto _error;
    }

    if (clu_locked) {
	/* Run clu_bdmgr to update clu_bdmgr.conf files */
	if(system("/usr/sbin/clu_bdmgr -u") != 0)
	    fprintf(stderr, catgets(catd, 1, 103, "%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"), Prog);
        clu_adm_lock(CLU_ADM_UNLOCK);
    }
    
    addVolSvcClassFlag = 0;

    unmountFlag = 0;
    errorFlag = 0;
    if (close_dot_tags(Prog))
        errorFlag = 1;

    /*
     * Remove the symbolic link to the special device file.
     */

    err = unlink (volPathName);
    if (err < 0) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ERRDELSYMLINK,
                                 "%s: Error deleting '%s' symbolic link.\n"),
                 Prog, volPathName);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_GEN_NUM_STR_ERR,
                                 "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
        goto _error;
    }

    (void)advfs_rem_vol_done(infop->fsname, volName); /* Ignore status */

    /* mark superblock as invalid in a way to know that AdvFS used to be
     * on this device */
    (void)invalidate_superblock(volName);
    
    if(thisDmnLock != -1) {
        if ((advfs_unlock(thisDmnLock, &err)) < 0) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERRUNLOCK,
                                     "%s: Error unlocking '%s'; [%d] %s\n"),
                     Prog, infop->fspath, err, ERRMSG(err));
        }
        thisDmnLock = -1;
    }


    if (errorFlag)
        goto _error;

    if (volIndex != NULL) {
        free(volIndex);
        volIndex = NULL;
    }

    return 0;

_error:

    if (volIndex != NULL) {
        free(volIndex);
        volIndex = NULL;
    }

    if (clu_locked) {
        clu_adm_lock(CLU_ADM_UNLOCK);
    }
    
    if (addVolSvcClassFlag != 0) {
        sts = advfs_add_vol_to_svc_class (
                                          infop->fsname,
                                          volInfo.volIndex,
                                          volProp.serviceClass
                                          );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_BADADDVOLSVCCLS,
                                     "%s: Cannot add volume '%s' back into svc class\n"),
                     Prog, volName); 
        }
    }

    if (unmountFlag != 0) {
        (void)close_dot_tags(Prog);
    }

    if(thisDmnLock != -1) {
        if ((advfs_unlock (thisDmnLock, &err)) < 0) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERRUNLOCK,
                                     "%s: Error unlocking '%s'; [%d] %s\n"),
                     Prog, infop->fspath, err, ERRMSG(err));
        }
    }
    
    return 1;

}  /* end remove_volume */

/*
 * The maximum is intentionally kept small because we use a sequential
 * search algorithm when searching for an entry in a table of bitfile
 * descriptors.
 */
#define BF_DESC_MAX 10

/*
 * move_metadata
 *
 * This function moves all, non-reserved bitfile metadata off of the specified
 * disk.
 *
 * Metadata can be classified as either non-extent or extent metadata.  Extent
 * metadata describes a bitfile's allocated disk storage and is located on the
 * same disk as the storage.  The storage described by extent metadata must be
 * moved as well as the metadata itself.
 *
 * Non-extent metadata does not describe any storage and can be independently
 * moved.
 */

static
int
move_metadata (
               bfDomainIdT domainId,  /* in */
               uint32_t volIndex,  /* in */
               int *retFileCnt  /* out */
               )

{

    bsBfDescT bfDesc[BF_DESC_MAX];
    int bfDescCnt;
    char dotTagsPathName[MAXPATHLEN+1];
    int duplFlag;
    int err;
    int fd;
    int fileCnt = 0;
    int i;
    int j;
    bfMCIdT nextBfDescId = {0,0,0};
    setInfoT *setHdr;
    struct stat stats;
    adv_status_t sts;
    uint32_t totalPageCnt;
    bsBfDescT uniqBfDesc[BF_DESC_MAX];
    int uniqBfDescCnt;
    int unknown;
    char* curPathName;

    /*
     * NOTE:  If we successfully moved something, we shouldn't see it
     * again when calling advfs_get_vol_bf_descs().
     */
        
    sts = advfs_get_vol_bf_descs (
                                  domainId,
                                  volIndex,
                                  BF_DESC_MAX,
                                  &(bfDesc[0]),
                                  &nextBfDescId,
                                  &bfDescCnt
                                  );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERRBFDESCS,
                                 "%s: Cannot get volume file descriptors\n"),
                 Prog);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERR,
                                 "%s: Error = %s\n"),
                 Prog, BSERRMSG (sts));
        return 1;
    }
    while (bfDescCnt > 0) {
        /*
         * Assume there are duplicate entries.  Copy the unique entries
         * from the original list to the unique list.
         */
        uniqBfDesc[0] = bfDesc[0];
        uniqBfDescCnt = 1;

        for (i = 1; i < bfDescCnt; i++) {

            if (!BS_BFTAG_RSVD (bfDesc[i].bfTag)) {

                duplFlag = 0;

                for (j = 0; j < uniqBfDescCnt; j++) {
                    if ( BS_BFTAG_EQL(bfDesc[i].bfSetTag, uniqBfDesc[j].bfSetTag) &&
                         BS_BFTAG_EQL(bfDesc[i].bfTag, uniqBfDesc[j].bfTag) )
                    {
                        if ( bfDesc[i].metadataType == uniqBfDesc[j].metadataType ) {
                            /* These can't be two BMT_PRIME_MCELL_XTNT_METADATA cells, */
                            /* but from rmvol's point of view, it doesn't matter. */
                            duplFlag = 1;
                            break;
                        }

                        if ( uniqBfDesc[j].metadataType == BMT_PRIME_MCELL_XTNT_METADATA &&
                             bfDesc[i].metadataType == BMT_NORMAL_METADATA )
                        {
                            /* We already came across the prime mcell. */
                            /* When we call advfs_move_bf_metadata for the */
                            /* prime mcell it will  move this mcell. */
                            duplFlag = 1;
                            break;
                        }
                        if ( uniqBfDesc[j].metadataType == BMT_NORMAL_METADATA &&
                             bfDesc[i].metadataType == BMT_PRIME_MCELL_XTNT_METADATA )
                        {
                            /* We came across a non extent mcell first and */
                            /* now we've found the prime mcell. Move the */
                            /* prime mcell, this  mcell will follow. Replace */
                            /* the uniqBfDesc entry with the prime mcell. */
                            duplFlag = 2;
                            break;
                        }
                    }
                }  /* end for */

                if (duplFlag == 0) {
                    /*
                     * A new entry.
                     */
                    uniqBfDesc[uniqBfDescCnt] = bfDesc[i];
                    uniqBfDescCnt++;
                } else if ( duplFlag == 2 ) {
                    uniqBfDesc[j] = bfDesc[i];
                }
            }
        }  /* end for */

        for (i = 0; i < uniqBfDescCnt; i++) {

            if (BS_BFTAG_RSVD (uniqBfDesc[i].bfSetTag)) {
                /*
                 * This is a bitfile set.  I can't open its tag directory
                 * via ".tags" without making the bitfile unmountable.
                 * So, bitfile sets are moved by the advfs_remove_volume()
                 * syscall.
                 */
                continue;
            }

            /*
             * This is a normal bitfile.
             */

            setHdr = find_setTag (uniqBfDesc[i].bfSetTag);
            if (setHdr == NULL) {
                /* print out set name?? */
                fprintf (stderr, catgets(catd,
                                         MS_FSADM_RMVOL,
                                         RMVOL_ERRSETTAG,
                                         "%s: Cannot find file set hdr - tag %d.%d\n"),
                         Prog, uniqBfDesc[i].bfSetTag.tag_num, uniqBfDesc[i].bfSetTag.tag_seq);
                return 1;
            }

            currentPath.setHdr = setHdr;
            currentPath.bfTag = uniqBfDesc[i].bfTag;
            currentPath.pathName[0] = '\0';

            curPathName = get_tag_path(setHdr,
                                       currentPath.bfTag,
                                       uniqBfDesc[i].bfSetTag,
                                       &unknown);

            strcat(currentPath.pathName, curPathName);

            sprintf (
                     dotTagsPathName,
                     "%s/0x%x.0x%x",
                     setHdr->dotTagsPath,
                     uniqBfDesc[i].bfTag.tag_num,
                     uniqBfDesc[i].bfTag.tag_seq
                     );

            /*
             * If the file is not a directory or a regular file, skip it and
             * let the kernel (advfs_remove_volume()) move its metadata or
             * storage.  We skip the file because we may not be able to open it.
             * The file is a symbolic link, block special, character special,
             * socket or fifo (named pipe).
             */
            err = lstat (dotTagsPathName, &stats);
            if (err == 0) {
                if (!(S_ISDIR (stats.st_mode)) && !(S_ISREG (stats.st_mode))) {
                    continue;
                }
            } else {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    continue;
                } else {
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_COMMON,
                                             COMMON_STATS,
                                             "%s: Error getting '%s' stats; [%d] %s\n"),
                             Prog, curPathName, errno, ERRMSG(errno));
                    return 1;
                }
            }

            fd = open (dotTagsPathName, O_RDONLY, 0);
            if (fd < 0) {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    continue;
                } else {
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_COMMON,
                                             COMMON_NOOPEN2,
                                             "%s: open of %s failed.\n"),
                             Prog, curPathName);
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_COMMON,
                                             COMMON_GEN_NUM_STR_ERR,
                                             "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRMSG (errno));
                    return 1;
                }
            }

            if (uniqBfDesc[i].metadataType == BMT_NORMAL_METADATA) {
                /*
                 * Non-extent metadata.  Just move the metadata itself.
                 * This is most likely a BMTR_FS_STAT record for a directory.
                 */
                if (verboseFlag != 0) {
                    fprintf (stdout, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_MOVINGMETADATA,
                                             "%s: Moving file metadata\n"),
                             Prog);
                    fprintf (stdout, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_FILENAME,
                                             "    file name: %s\n"),
                             curPathName);
                    if (veryVerboseFlag != 0) {
                        display_tag (
                                     uniqBfDesc[i].bfSetTag,
                                     uniqBfDesc[i].bfTag
                                     );
                    }
		    (void)fflush(stdout);
                }

                /*
                 * If this is a file which supports extent information in
                 * the primary mcell and there is, in fact, extent information
                 * there at this time, the following call will return with
                 * the error E_STG_ADDED_DURING_MIGRATE_OPER.  This is because
                 * the extent metadata must be moved before the rest of the
                 * metadata can be.  We'll ignore this error since the 
                 * processing of the primary mcell 
                 * (BMT_PRIME_MCELL_XTNT_METADATA) will take care of moving
                 * all of the non-extent metadata.
                 */
                sts = advfs_move_bf_metadata (fd);
                if (sts != EOK && sts != E_STG_ADDED_DURING_MIGRATE_OPER) {
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_ERRMOVEMETADATA,
                                             "%s: Cannot move file %s metadata\n"),
                             Prog, curPathName);
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_COMMON,
                                             COMMON_ERR,
                                             "%s: Error = %s\n"),
                             Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
            } 
            else { /* BMT_PRIME_MCELL_XTNT_METADATA || BMT_XTNT_METADATA */
                /*
                 * Looking at metadata from the primary mcell.  If there
                 * are no extents described in the primary mcell, just move
                 * the primary mcell metadata.  Otherwise, first move the
                 * extents described there and then move the primary mcell
                 * metadata.
                 */
                sts = move_normal_metadata (
                                            fd,
                                            uniqBfDesc[i].bfSetTag,
                                            uniqBfDesc[i].bfTag,
                                            volIndex,
                                            uniqBfDesc[i].metadataType
                                            );
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_ERRMOVEMETADATA,
                                             "%s: Cannot move file %s metadata\n"),
                             Prog, curPathName);
                    (void) close (fd);
                    return 1;
                }
                
                if (verboseFlag != 0 && 
                    uniqBfDesc[i].metadataType == BMT_PRIME_MCELL_XTNT_METADATA) {
                    fprintf (stdout, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_MOVINGFILEPRIMECELL,
                                             "%s: Moving file prime mcell\n"),
                             Prog);
                    fprintf (stdout, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_FILENAME,
                                             "    file name: %s\n"),
                             curPathName);
                    if (veryVerboseFlag != 0) {
                        display_tag (
                                     uniqBfDesc[i].bfSetTag,
                                     uniqBfDesc[i].bfTag
                                     );
                    }
                    (void)fflush(stdout);
                }
                
                /*
                 * If another thread added storage to the file and used
                 * the extent descriptor in the primary mcell for the first
                 * time in between our calls to move_normal_metadata() and
                 * advfs_move_bf_metadata(), then we will get back the
                 * return code E_STG_ADDED_DURING_MIGRATE_OPER from this
                 * call.  
                 */
                sts = advfs_move_bf_metadata (fd);
                if (sts != EOK && sts != E_STG_ADDED_DURING_MIGRATE_OPER) {
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_RMVOL,
                                             RMVOL_ERRMOVEMETADATA,
                                             "%s: Cannot move file %s metadata\n"),
                             Prog, curPathName);
                    fprintf (stderr, catgets(catd,
                                             MS_FSADM_COMMON,
                                             COMMON_ERR,
                                             "%s: Error = %s\n"), 
                             Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
                /* Ignore possible E_STG_ADDED_DURING_MIGRATE_OPER error. */
                /* This mcell will be eximined again with the next call to */
                /* move_metadata. */
            }

            if (close (fd)) {
                fprintf( stderr, catgets(catd,
                                         MS_FSADM_COMMON,
                                         COMMON_NOCLOSE,
                                         "%s: close of %s failed. [%d] %s\n"),
                         Prog, dotTagsPathName, errno, ERRMSG(errno));
                return 1;
            }

            fileCnt++;

        }  /* end for */

        sts = advfs_get_vol_bf_descs (
                                      domainId,
                                      volIndex,
                                      BF_DESC_MAX,
                                      &(bfDesc[0]),
                                      &nextBfDescId,
                                      &bfDescCnt
                                      );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERRBFDESCS,
                                     "%s: Cannot get volume file descriptors\n"),
                     Prog);
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_ERR,
                                     "%s: Error = %s\n"),
                     Prog, BSERRMSG (sts));
            return 1;
        }
    }  /* end while */

    *retFileCnt = fileCnt;

    return 0;

}  /* end move_metadata */

/*
 * move_normal_metadata
 *
 * This function moves a normal file's extents off of the specified disk.
 */

static
adv_status_t
move_normal_metadata (
                      int fd,  /* in */
                      bfTagT bfSetTag,  /* in */
                      bfTagT bfTag,  /* in */
                      uint32_t volIndex,  /* in */
                      metadataTypeT mtype /* in */
                      )

{

    int i;
    adv_status_t sts;
    uint32_t totalFobCnt = 0;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt;

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_MOVINGFILE,
                                 "%s: Moving file\n"),
                 Prog);
        fprintf (stdout, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_FILENAME,
                                 "    file name: %s\n"),
                 currentPath.pathName);
        if (veryVerboseFlag != 0) {
            display_tag (bfSetTag, bfTag);
        }
        (void)fflush(stdout);
    }

    sts = create_xtnt_desc_list (fd, volIndex, 1, &xtntDescCnt, &xtntDesc);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERREXTLIST,
                                 "%s: Cannot create extent desc list\n"),
                 Prog);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERR,
                                 "%s: Error = %s\n"),
                 Prog, BSERRMSG (sts));
        goto _error;
    }

    for (i = 0; i < xtntDescCnt; i++) {

        if (xtntDesc[i].fobCnt > 0) {

            if (verboseFlag != 0) {
                fprintf (stdout, catgets(catd,
                                         MS_FSADM_RMVOL,
                                         RMVOL_MOVINGFOBS,
                                         "    moving fobs - "));
                fprintf (stdout, catgets(catd,
                                         MS_FSADM_RMVOL,
                                         RMVOL_FOBINFO,
                                         "fob offset: %d, fob count: %d\n"),
                         xtntDesc[i].fobOffset, xtntDesc[i].fobCnt);
		(void)fflush(stdout);
            }
                
            sts = migrate (
                           fd,
                           -1,  /* srcVolIndex */
                           xtntDesc[i].fobOffset,
                           xtntDesc[i].fobCnt,
                           -1  /* dstVolIndex */
                           );
            if (sts == EOK) {
                totalFobCnt = totalFobCnt + xtntDesc[i].fobCnt;
            } else {
                if ((sts == E_MIGRATE_IN_PROGRESS) || (sts == E_NO_SNAPSHOT_STG)) {
                    sts = EOK;
                }
                goto _error;
            }
        }
    }  /* end for  */

    free (xtntDesc);

    if (verboseFlag != 0) {
        if (totalFobCnt == 0) {
            fprintf (stdout, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_NOFOBSMOVED, "    no fobs moved\n"));
            (void)fflush(stdout);
        }
    }
    
    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    return sts;

}  /* end move_normal_metadata */


/*
 * migrate
 *
 * This function moves a file's page range off of the specified disk.
 */

static
adv_status_t
migrate (
         int fd,  /* in */
         uint32_t srcVolIndex,  /* in */
         uint32_t bfPageOffset,  /* in */
         uint32_t bfPageCnt,  /* in */
         uint32_t dstVolIndex  /* in */
         )

{

    adv_status_t sts;
    uint32_t forceMigrateFlag = 0;

retry_migrate:
    sts = advfs_migrate (
                         fd,
                         srcVolIndex,
                         bfPageOffset,
                         bfPageCnt,
                         dstVolIndex,
                         -1,  /* dstBlkOffset */
                         forceMigrateFlag
                         );
    switch (sts) {

      case EOK:

        break;

      case E_MIGRATE_IN_PROGRESS:

        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ERRMOVEFILEFOBS,
                                 "%s: Cannot move file %s fobs\n"),
                 Prog, currentPath.pathName);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_MIGACTIVE,
                                 "%s: Migrate already active\n"),
                 Prog);
        break;

      case ENO_MORE_BLKS:
                        
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_OUTOFSPACE,
                                 "%s: Ran out of free space to migrate data off volume\n"),
                 Prog);
        break;

      case E_INVOLUNTARY_ABORT:

        if (!forceMigrateFlag) {
            forceMigrateFlag = 1;
            goto retry_migrate;
        }
        /* Fall through. */

      default:

        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ERRMOVEFILEFOBS,
                                 "%s: Cannot move file %s fobs\n"),
                 Prog, currentPath.pathName);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERR,
                                 "%s: Error = %s\n"),
                 Prog, BSERRMSG (sts));
        break;

    }  /* end switch */

    return sts;

}  /* end migrate */

#define XTNT_DESC_MAX 100

/*
 * create_xtnt_desc_list
 *
 * This function creates a list of extents that are located on the
 * specified volume.  The page offset and count in each list entry
 * are bitfile relative.
 */

static
adv_status_t
create_xtnt_desc_list (
                       int fd,  /* in */
                       uint32_t volIndex,  /* in */
                       int xtntMapIndex,  /* in, 1 based */
                       int *retXtntDescCnt,  /* out */
                       xtntDescT **retXtntDesc  /* out */
                       )

{

    uint32_t allocVolIndex;
    uint32_t bfFobCnt;
    uint32_t bfFobOffset;
    int i;
    bsExtentDescT rawXtntDesc[XTNT_DESC_MAX];
    int rawXtntDescCnt;
    int startXtntDesc;
    adv_status_t sts;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt = 0;
    int xtntDescMaxCnt = 0;

    startXtntDesc = 0;

    sts = advfs_get_bf_xtnt_map (
                                fd,
                                xtntMapIndex,
                                startXtntDesc,
                                XTNT_DESC_MAX,
                                &(rawXtntDesc[0]),
                                &rawXtntDescCnt,
                                &allocVolIndex
                                );
    while (sts == EOK && rawXtntDescCnt > 0) {
        i = 0;

        /*
         * Find the first extent descriptor that describes an extent on
         * the source disk.
         */
        while ((i < rawXtntDescCnt) && (rawXtntDesc[i].bsed_vol_index != volIndex)) {
            i++;
        }  /* end while */

        while (i < rawXtntDescCnt) {

            bfFobOffset = rawXtntDesc[i].bsed_fob_offset;
            bfFobCnt = rawXtntDesc[i].bsed_fob_cnt;
            i++;

            /*
             * Find the next extent descriptor that describes an extent
             * on a disk other than the source disk.
             */
            while ((i < rawXtntDescCnt) && (rawXtntDesc[i].bsed_vol_index == volIndex)) {
                bfFobCnt = bfFobCnt + rawXtntDesc[i].bsed_fob_cnt;

                i++;
            }  /* end while */

            if (xtntDescCnt >= xtntDescMaxCnt) {
                sts = extend_xtnt_desc_list (&xtntDescMaxCnt, &xtntDesc);
                if (sts != EOK) {
                    goto _error;
                }
            }
            xtntDesc[xtntDescCnt].fobOffset = bfFobOffset;
            xtntDesc[xtntDescCnt].fobCnt = bfFobCnt;
            xtntDescCnt++;

            /*
             * Find the next extent descriptor that describes an extent on
             * the source disk.
             */
            while ((i < rawXtntDescCnt) && (rawXtntDesc[i].bsed_vol_index != volIndex)) {
                i++;
            }  /* end while */

        }  /* end while */

        startXtntDesc = startXtntDesc + rawXtntDescCnt;

        sts = advfs_get_bf_xtnt_map (
                                    fd,
                                    xtntMapIndex,
                                    startXtntDesc,
                                    XTNT_DESC_MAX,
                                    &(rawXtntDesc[0]),
                                    &rawXtntDescCnt,
                                    &allocVolIndex
                                    );
    }  /* end while */

    if (sts != EOK && sts != ENO_XTNTS) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 COMMON_ERREXTENT,
                                 "%s: Cannot get extent descriptors\n"),
                 Prog);
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_COMMON,
                                 COMMON_ERR,
                                 "%s: Error = %s\n"),
                 Prog, BSERRMSG (sts));
        goto _error;
    }

    *retXtntDescCnt = xtntDescCnt;
    *retXtntDesc = xtntDesc;

    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    return sts;

}  /* end create_xtnt_desc_list */

/*
 * extend_xtnt_desc_list
 *
 * This function extends the extent list.
 */

static
adv_status_t
extend_xtnt_desc_list (
                       int *maxCnt,  /* in/out */
                       xtntDescT **xtntDesc  /* in/out */
                       )

{

    int i;
    int newMaxCnt;
    xtntDescT *newXtntDesc;
    int oldMaxCnt;
    xtntDescT *oldXtntDesc;

    oldMaxCnt = *maxCnt;
    oldXtntDesc = *xtntDesc;

    newMaxCnt = oldMaxCnt + 100;
    if (oldXtntDesc != NULL) {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     COMMON_EXPMEMEXT,
                                     "%s: Cannot allocate memory for expanded xtnt desc table\n"),
                     Prog);
            return ENO_MORE_MEMORY;
        }
        for (i = 0; i < oldMaxCnt; i++) {
            newXtntDesc[i] = oldXtntDesc[i];
        }  /* end for */
        free (oldXtntDesc);
    } else {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_COMMON,
                                     COMMON_MEMXD,
                                     "%s: Cannot allocate memory for new xtnt desc table\n"),
                     Prog);
            return ENO_MORE_MEMORY;
        }
    }

    *maxCnt = newMaxCnt;
    *xtntDesc = newXtntDesc;

    return EOK;

}  /* end extend_xtnt_desc_list */

/*
 * display_tag
 *
 * This function displays the bitfile set tag and bitfile tag.
 */

static
void
display_tag (
             bfTagT bfSetTag,  /* in */
             bfTagT bfTag  /* in */
             )

{

    fprintf (stdout, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_SETTAGDISPLAY,
                             "    set tag: %d.%d (0x%x.0x%x)\n"),
             bfSetTag.tag_num, bfSetTag.tag_seq, bfSetTag.tag_num, bfSetTag.tag_seq);
    fprintf (stdout, catgets(catd,
                             MS_FSADM_RMVOL,
                             RMVOL_TAGDISPLAY,
                             "    tag: %d.%d (0x%x.0x%x)\n"),
             bfTag.tag_num, bfTag.tag_seq, bfTag.tag_num, bfTag.tag_seq);
    (void)fflush(stdout);

}  /* end display_tag */

switch_root_tagdir(int volIndexI, int volIndexCnt, uint32_t *volIndex)
{
    int i;
    adv_status_t sts = EOK;

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_SWITCHINGTAGDIR,
                                 "%s: Switching root tag directory\n"),
                 Prog);
        (void)fflush(stdout);
    }
    for (i = 0; i < volIndexCnt; i++) {
        if (i != volIndexI) {
            sts = advfs_switch_root_tagdir (infop->fsname, volIndex[i]);
            if ((sts != ENO_MORE_BLKS) && (sts != ENO_MORE_MCELLS)) {
                break;  /* out of for */
            }
        }
    }  /* end for */
    if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ERRSWITCHTAGDIR,
                                 "%s: Cannot switch root tag directory\n"),
                 Prog);
        if (sts == ENO_MORE_BLKS) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_NOSTORAGE,
                                     "%s: No one disk has enough available storage\n"),
                     Prog);
        } else {
            if (sts == ENO_MORE_MCELLS) {
                fprintf (stderr, catgets(catd,
                                         MS_FSADM_RMVOL,
                                         RMVOL_NOMETADATA,
                                         "%s: No one disk has enough available metadata\n"),
                         Prog);
            } else {
                fprintf (stderr, catgets(catd,
                                         MS_FSADM_COMMON,
                                         COMMON_ERR,
                                         "%s: Error = %s\n"),
                         Prog, BSERRMSG (sts));
            }
        }
        return 1;
    }
    return 0;
}

switch_log(int volIndexI, int volIndexCnt, uint32_t *volIndex)
{
    int i;
    adv_status_t sts = EOK;

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd, 1, 32, "%s: Switching log\n"), Prog);
        (void)fflush(stdout);
    }
    for (i = 0; i < volIndexCnt; i++) {
        if (i != volIndexI) {
            sts = advfs_switch_log (infop->fsname, volIndex[i], 0, 0);
            if ((sts != ENO_MORE_BLKS) && (sts != ENO_MORE_MCELLS)) {
                break;  /* out of for */
            }
        }
    }  /* end for */
    if (sts != EOK) {
        fprintf (stderr, catgets(catd,
                                 MS_FSADM_RMVOL,
                                 RMVOL_ERRSWITCHLOG,
                                 "%s: Cannot switch log\n"),
                 Prog);
        if (sts == ENO_MORE_BLKS) {
            fprintf (stderr, catgets(catd,
                                     MS_FSADM_RMVOL,
                                     RMVOL_NOSTORAGE,
                                     "%s: No one disk has enough available storage\n"),
                     Prog);
        } else {
            if (sts == ENO_MORE_MCELLS) {
                fprintf (stderr, catgets(catd,
                                         MS_FSADM_RMVOL,
                                         RMVOL_NOMETADATA,
                                         "%s: No one disk has enough available metadata\n"),
                         Prog);
            } else {
                fprintf (stderr, catgets(catd,
                                         MS_FSADM_COMMON,
                                         COMMON_ERR,
                                         "%s: Error = %s\n"),
                         Prog, BSERRMSG (sts));
            }
        }
        return 1;
    }
    return 0;
}

/* 
 * cfs_server()
 *
 * Any changes to this routine: please see addvol's cfs_server logic
 * also 
 *
 * Return values:
 *      -2: failover/relocation
 *      -1: cfs error
 *       0: remote server
 *       1: local server
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
