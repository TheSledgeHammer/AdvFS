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
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    addvol.c - add a new volume to an active or inactive domain.
 *
 *    Note:  If addvol exits with error 1, an error message must be written to stderr
 *           because of the rsh implementation of cluster support. 
 *
 * Date:
 *
 *    October 20, 1992
 */
/*
 * HISTORY:
 */

#ifndef lint
  static char rcsid[] = "@(#)$RCSfile: addvol.c,v $ $Revision: 1.1.26.7 $ (DEC) $Date: 2002/06/03 19:11:59 $";
#endif

#include <unistd.h>
#define  DKTYPENAMES                      /* Flag set for sys/disklabel.h */
#include <sys/disklabel.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <overlap.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/bs_error.h>
#include <msfs/bs_bmt.h>
#include <locale.h>
#include "addvol_msg.h"
#include <sys/clu.h>
#include <cfs/cfs_user.h>
#include <clua/clua.h>
#include <syslog.h>

#define BUFSIZE 8192
#define RETRY_MAX 8

  /*
   *  Function prototypes
   */
  
static void usage( void );
void print_error(char *special, int errcode, char *domain);
cfs_server(const char *node, const char *domain, char *server_name);
extern int clu_adm_lock(int lock_oper);

  /*
   *  Globals
   */
   
extern int    errno;
extern char  *sys_errlist[];

char         *Prog;
nl_catd       catd;

main(int argc, char *argv[]) {
 
  extern int    optind;
  extern int    opterr;
  extern char  *optarg;

  char   dmnName[MAXPATHLEN+1];
  char  *linkName;
  char  *domain;
  char  *volName = NULL;
  char  *devType = NULL;
  char  *pp;
  char  *tmpp;
  char   tmp1[MAXPATHLEN];
  char   tmp2[MAXPATHLEN];
  char   specialChar[MAXPATHLEN];
  char   specialBlock[MAXPATHLEN];
  char   volDiskGroup[64];
  char   lsmName[MAXPATHLEN];
  char   lsm_dev[MAXPATHLEN+1];
  char   *vol, *dg;
  char   tmpstr[MAXHOSTNAMELEN + 30];
  char   our_node[MAXHOSTNAMELEN + 1];
  char   server_name[MAXHOSTNAMELEN + 1];
  char   net_name[MAXHOSTNAMELEN + 1];
  char   newarg2[ML_DOMAIN_NAME_SZ + MAXPATHLEN + MAXPATHLEN + 25];
  char  *newargv[4];
  char   buffer[BUFSIZE];
  char  *sh_cmd="/sbin/sh -c ";
  char  *FullPathProg="/usr/sbin/addvol";
  char   tmpName[MAXPATHLEN];

  int    c;
  int    s = 0;
  int    F = 0;
  int    dirLocked = 0, dmnLocked = 0;
  int    lkHndl;
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
  long   zeroed_lsm_size = 0;          /* Used in islsm64() to check for an lsm volume. */

  u32T   volSize = 0;
  u32T   volIndex = -1;
  i32T   bmtXtntPgs = BS_BMT_XPND_PGS;
  i32T   bmtPreallocPgs = 0;

  getDevStatusT    error;
  mlBfDomainIdT    bfDomainId;
  mlStatusT        sts, sts2;
  mlServiceClassT  volSvc = 0;

  struct stat           stats;
  struct disklabel     *lab;
  struct clu_gen_info  *clugenptr = NULL;

    (void) setlocale(LC_ALL, "");

    catd = catopen(MF_ADDVOL, NL_CAT_LOCALE);

    /*
     *  ignore interrupt signals
     */

    (void) signal( SIGINT, SIG_IGN );


    /*
     *  store only the file name part of argv[0]
     */

    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     *  check for root
     */
     
    if (geteuid()) {
        fprintf(stderr, catgets(catd, 1, 31,
            "\nPermission denied - user must be root to run %s.\n\n"),
            argv[0]);
        usage();
        exit(1);
    }

    /*
     *   Get user-specified command line arguments.
     */

    while ((c = getopt( argc, argv, "Fs:x:p:" )) != EOF) {
        switch (c) {

            case 's':
                s++;
                volSize = strtoul( optarg, &pp, 0 );
                break;

            case 'F':
                F++;
                break;

            case 'x':
                bmtXtntPgs = strtol( optarg, &pp, 0 );
                if (bmtXtntPgs < BS_BMT_XPND_PGS) {
                    fprintf(stderr,
                            catgets(catd, 1, 32,
                            "%s: Error, the %s argument must be greater than or equal to %d\n"),
                            argv[0], "-x", BS_BMT_XPND_PGS);
                    exit(1);
                }
                break;

            case 'p':
                bmtPreallocPgs = strtol( optarg, &pp, 0 );
                if (bmtPreallocPgs < 0) {
                    fprintf(stderr,
                            catgets(catd, 1, 32,
                            "%s: Error, the %s argument must be greater than or equal to %d\n"),
                            argv[0], "-p", 0);
                    exit(1);
                }
                break;

            case '?':
            default:
                usage();
                exit( 1 );
        }
    }
 
    /*
     *  check for missing required args
     */
     
    if (optind != (argc - 2)) {
        usage();
        exit( 1 );
    }

    /*
     * Check the block special device name
     * The fsl_to_xx routines provide the functionality whereby
     * the user need not specify the fully qualified device path.
     * For example:  dsk2g instead of /dev/disk/dsk2g
     *
     * fsl_to_raw_name() will convert the input to a character
     * (raw) device as well as fully qualify the pathname if needed.
     * Unfortunaltey, it can return non zero status when it shouldn't.
     *
     * If the user specified just the special file name
     *   Use fsl_to_raw_name to process the input 
     *   Use fsl_to_block_name to convert back to block special
     *   If the fsl_to_block_name returned error
     *     just use the input arg as specified - volName points to argv[optind]
     *   else
     *     use input as processed  - volName points to specialBlock
     * else
     *   just pass along the argument as input
     */

    if ((volName = strrchr(argv[optind], '/' )) == NULL) {
        fsl_to_raw_name(argv[optind], specialChar, MAXPATHLEN);
        volName = fsl_to_block_name(specialChar, specialBlock, MAXPATHLEN);
        if (volName == NULL) {               
          if ((vol = strrchr(argv[optind], '.' )) != NULL) {
            strcpy(lsm_dev,argv[optind]);
            dg = strtok_r(lsm_dev, ".",&tmpp);
            sprintf(specialBlock, "/dev/vol/%s/%s", dg, vol+1);
            volName = specialBlock;
          } else {
            volName = argv[optind];
          }
        }

    } else {
        volName= argv[optind];
    }
    
    domain = argv[optind + 1];

    /* 
     * If in a cluster, we need to determine the server 
     * and execute either locally or remotely as appropriate
     *
     * If any changes are made to this logic, please check
     * rmvol's cfs logic also
     */

    if (clu_is_member()) {

        retval = clu_get_info(&clugenptr);
        if (retval || (clugenptr == NULL)) {
            fprintf(stderr, catgets(catd, 1, 38,
                    "%s: cluster filesystem error \n"), Prog);
            goto _error2; 
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
                fprintf(stderr, catgets(catd, 1, 44,
                        "%s: Can't use any part of the quorum disk for domain storage\n"), Prog);
                goto _error2;
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
            fprintf(stderr, catgets(catd, 1, 38,
                    "%s: cluster filesystem error \n"), Prog);
            goto _error2; 
        }
_retry:

        /*
         *  Limit retries to RETRY_MAX times for rsh failures
         */
           
        if (retry_count++ > RETRY_MAX) {
            fprintf(stderr, catgets(catd, 1, 37,
                    "%s: rsh failure, check that the /.rhosts file allows cluster alias access.\n"), Prog);
            goto _error2;
        }
        
        sts = cfs_server(our_node, domain, server_name);

         /*
          * if failover/relocation
          *   retry without being limited to RETRY_MAX iterations
          */
            
        if (sts == -2) {
            retry_count = 0;  
            goto _retry;

       /*
        *  else if a remote server
        *    use rsh to execute over on the server
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
                fprintf(stderr, catgets(catd, 1, 38,
                        "%s: cluster filesystem error \n"), Prog);
                goto _error2; 
            }

            /*
             *  The rsh command is used (temporarily) to provide cluster support.
             *  Because the exit code of the addvol rsh'ed to the current server
             *  cannot be determined, we first try to redirect sterr of the rsh'ed
             *  addvol to a temporary file where when can examine it to determine
             *  success or failure.  Since dealing with files can introduce many
             *  additional failure points, we try to create the temp file before
             *  actually doing the rsh.  If we can't create the file (no space, etc),
             *  stderr is redirected to stout and we just live with the fact that
             *  the correct exit code cannot be determined (0 is returned).  If the
             *  temp file can be created, we redirect stderr to it and check it when
             *  the rsh fork exits. To fix a potential security hole, the
             *  file is created with root privledges but is truncated
             *  before being passed as a parameter to the rsh'd rmvol
             *  command.
             *
             *  The command here is:
             *    /usr/sbin/rsh rhost "/sbin/sh -c '/usr/sbin/addvol special domain 2>>/tempfilename'"
             *  or ,
             *    /usr/sbin/rsh rhost "/sbin/sh -c '/usr/sbin/addvol special domain 2>&1'"
             *              *
             *  (Double quotes are not used but using one argument for what's inside
             *  the double quotes yields the same effect.)
             */

            newargv[0] = "/usr/bin/rsh";
            newargv[1] = net_name;
            sprintf(newarg2, "%s '%s ", sh_cmd, FullPathProg);
            for (i=1; i<argc; i++) {
                strcat(newarg2, argv[i]);
                strcat(newarg2, " ");
            }
            
            /*
             *  Compose the pathname of the temporary file
             */

            sprintf(tmpName, "/cluster/members/member%i/tmp/addvol.XXXXXX", clugenptr->my_memberid);

            /*
             *  Try and create the temp file now
             */

            retval = 0;
            if ((fd = mkstemp(tmpName)) < 0) {
              retval = fd;
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
                  
                fprintf(stderr, catgets(catd, 1, 39,
                        "%s: forking error \n"), Prog);
                goto _error2;
                 
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
                        fprintf(stderr, catgets(catd, 1, 100,
                                "%s: cluster filesystem error \n"), Prog);
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
            fprintf(stderr, catgets(catd, 1, 38,
                    "%s: cluster filesystem error \n"), Prog);
            goto _error2; 
        }
    }
 
    /*
     *  sts == 1;
     *  Local server, continue to execute here
     */    

_retry2:
 
    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf(stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                Prog, MSFS_DMN_DIR, err, sys_errlist[err] );
        goto _error;
    }

    dirLocked = 1;
    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, domain );

    if (lstat( dmnName, &stats )) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: domain '%s' doesn't exist\n"), Prog, dmnName );
        goto _error;
    }

    /*
     *  Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                     catgets(catd, 1, 5, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 6, "%s: error locking '%s'.\n"),
                     Prog, dmnName);
            fprintf (stderr, catgets(catd, 1, 7, "%s: Error = [%d] %s\n"),
                     Prog, err, sys_errlist[err]);
        }
        goto _error;
    }

    dmnLocked = 1;


    /*
     * we need to construct a linkName which is part 
     * diskgroup and part volume name.  This needs to be done because you
     * can have two volumes with the same name but in different diskgroups.
     */

    if (checklsm(volName, volDiskGroup, lsmName)) {
        char temp[MAXPATHLEN];

        sprintf(temp,"/%s.%s",volDiskGroup,lsmName);
        linkName = strcat( dmnName, temp);
    } else {
        tmpp = strrchr( volName, '/' );
        if (tmpp == NULL) {
            fprintf(stderr, catgets(catd, 1, 8, "%s: error looking for '/' in %s\n"),
                    Prog, volName );
            goto _error;
        }
        linkName = strcat( dmnName, tmpp );
    }

    if (!lstat( linkName, &stats )) {
        fprintf( stderr, catgets(catd, 1, 9, "%s: link '%s' already exists\n"), Prog, linkName );
        goto _error;
    }

    if (!volume_avail( Prog, domain, volName )) {
        goto _error;
    }

    sts = set_usage(volName, FS_ADVFS, F);
    if ((sts < 0) || sts > OV_ERR_MULT_FSTYPE_OVERLAP) {
          print_error(volName, sts, domain);
          goto _error1;
    } else {
        if (!volSize) {
            errno = 0;
            error = get_dev_size( volName, devType, &volSize );
            if (error != GDS_OK) {
                fprintf(stderr, catgets(catd, 1, 10,
                        "%s: can't get device size; %s"),
                        Prog, GDSERRMSG( error ) );
                if (errno != 0) {
                    fprintf( stderr, "; %s", BSERRMSG( errno ) );
                }
                fprintf( stderr, "\n" );
                goto _error;
            }
        }

        /* overlap - see if they want to force it */
        if (sts > 0) {
            char c;

            /*
             * At this point, /etc/fdmns should be locked.
             * We already know that any overlapping partition is not open.
             *
             * NOTE: OV_ERR_FDMNS_OVERLAP needs to be checked first,
             * as it is not taken care of in print_error().
             */

            sts2 = any_fdmns_overlap(volName);
            if (sts2 == OV_ERR_FDMNS_OVERLAP) {
                fprintf(stderr, catgets(catd, 1, 49,
                        "At least one overlapping partition belongs to "
                        "an existing AdvFS domain.\n"));
                fprintf(stderr, catgets(catd, 1, 52, "Quitting .... \n"));
                goto _error1;
            } else if ((sts2 < 0) || sts2 > OV_ERR_MULT_FSTYPE_OVERLAP) {
                print_error(volName, sts2, domain);
                goto _error1;
            } else {
                print_error(volName, sts, domain);
                if (!F && isatty(fileno(stdin))) {

                    /*
                     * Unlock the /etc/fdmns directory as well as the
                     * domain directory so that other utilities can
                     * proceed while the user is deciding whether or not
                     * he wants to continue.
                     */

                    if (unlock_file( thisDmnLock, &err ) == -1) {
                        fprintf(stderr,catgets(catd,1,12,
                                 "%s: error unlocking '%s'; [%d] %s\n"),
                                 Prog, dmnName, err, sys_errlist[err]);
                        dmnLocked = 0;
                        goto _error1;
                    }
                    dmnLocked = 0;

                    if (unlock_file( lkHndl, &err ) == -1) {
                        fprintf(stderr,catgets(catd,1,12,
                                 "%s: error unlocking '%s'; [%d] %s\n"),
                                 Prog, MSFS_DMN_DIR, err, sys_errlist[err]);
                        dirLocked = 0;
                        goto _error1;
                    }
                    dirLocked = 0;

                    do {
                        fprintf(stdout,catgets(catd,1,47, "CONTINUE? [y/n] "));

                        (void) fflush(stdout);
                        c = getc(stdin);
                        while (c != '\n' && getc(stdin) != '\n') {
                            if (feof(stdin))
                                goto _error1;
                        }
                        if (c == 'n' || c == 'N') {
                            goto _error1;
                        }
                    } while (c != 'y' && c != 'Y');
                    F = 1;

                    /*
                     * Since we released the /etc/fdmns lock, we really
                     * should start over to make sure we don't wipe out
                     * a different domain that was created while the user
                     * was trying to decide what to do.
                     */
                    goto _retry2;

                } else {
                    goto _error1;
                }
            }
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

    if (clu_is_member() && (strcmp(domain, "cluster_root") == 0)) {
	char *vname;
	int lsm_device = islsm64(volName, &zeroed_lsm_size);

	/*
	 * Verify that the usage type of the lsm volume is of type cluroot,
	 * otherwise we create an unmountable cluster_root.
	 */
	if (lsm_device) {
		char cbuf[128];	
		vname= rindex (volName, '/');
		if (vname)
			vname++;
		sprintf(cbuf,"%s %s | /sbin/grep cluroot > /dev/null 2>&1\n",
 			"/sbin/volprint -m -F %use_type", vname);
		if (system(cbuf) != 0) {
			fprintf(stderr, catgets(catd, 1, 48, "lsm volume %s must have usage type of cluroot for cluster root domain\n"), vname);
            		goto _error;
		}

	}

        if (clu_adm_lock(CLU_ADM_LOCK)) {
            fprintf(stderr, catgets(catd, 1, 35, "%s: clu_adm_lock failed to get lock for cluster root\n"), Prog);
            goto _error;
        }
        clu_locked = TRUE;
    }

    sts = msfs_add_volume(domain, 
                          volName,
                          &volSvc,
                          volSize,
                          bmtXtntPgs,
                          bmtPreallocPgs,
                          &bfDomainId,
                          &volIndex);

                        
    if (sts != EOK) {
        fprintf(stderr, catgets(catd, 1, 11, "%s: error = %s\n"), Prog, BSERRMSG(sts));
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
        fprintf(stderr, catgets(catd, 1, 12, "%s: can't create symlink '%s @--> %s'; [%d] %s\n"),
                Prog, linkName, volName, errno, sys_errlist[errno] );
        goto _error;
    }

    sts = advfs_add_vol_to_svc_class(domain, volIndex, volSvc);

    if (sts != EOK) {
        fprintf(stderr, catgets(catd, 1, 40, "%s: Can't add volume '%s' into svc class\n"),
                Prog, volName);
        fprintf(stderr, catgets(catd, 1, 11, "%s: error = %s\n"), Prog, BSERRMSG(sts));
        fprintf(stderr, catgets(catd, 1, 41, "use '/sbin/chvol -A %s %s' to activate the volume\n"),
                volName, domain); 
        goto _error1;
    }

    (void)msfs_add_vol_done(domain, volName); /* Ignore status */

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf(stderr, catgets(catd, 1, 13, "%s: error unlocking '%s'; [%d] %s\n"),
                Prog, MSFS_DMN_DIR, err, sys_errlist[err] );
        dirLocked = 0;
        goto _error1;
    }

    return 0;

    /*
     *  Error exits
     */
     
_error:

    sts = set_usage( volName, FS_UNUSED, 1 );

_error1:

    if (clu_locked) {
        clu_adm_lock(CLU_ADM_UNLOCK);
    }

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf(stderr, catgets(catd, 1, 13, "%s: error unlocking '%s'; [%d] %s\n"),
                    Prog, MSFS_DMN_DIR, err, sys_errlist[err] );
        }
    }

_error2:

    fprintf(stderr, catgets(catd, 1, 14, "%s: Can't add volume '%s' to domain '%s'\n"),
            Prog, volName, domain); 
    exit(1);
}



static void usage( void ) {

#if 0
    fprintf(stderr, catgets(catd, 1, USAGE1, "usage: %s [-s size] special domain\n"), Prog);
#else
    fprintf(stderr, catgets(catd, 1, USAGE2, "usage: %s [-F] [-x meta_extent_size] [-p meta_prealloc_size] special domain\n"), Prog);
#endif

}

void print_error(char *special, int errcode, char *domain) 
{
  unsigned long    zeroed_lsm_size = 0;
  int fatal=0; /* Set to 1, if application should quit. */


  switch(errcode) {
      case OV_ERR_MULT_FSTYPE_OVERLAP:
          fprintf(stderr, catgets(catd, 1, 28, "Specified partition %s is marked in use.\n"), special);
          fprintf(stderr, catgets(catd, 1, 29, "Also partition(s) which overlap %s are marked in use.\n"), special);
          fatal = 0;
          break;

      case OV_ERR_FSTYPE_OVERLAP:
          fprintf(stderr, catgets(catd, 1, 30, "Partition(s) which overlap %s are marked in use.\n"), special);
          fatal = 0;
          break;

      case OV_ERR_INVALID_DSKLBL:
          fprintf(stderr, catgets(catd, 1, 15, "The disklabel for %s does not exist or is corrupted.\n"), special);
          fatal=1;
          break;

      case OV_ERR_OPEN_OVERLAP:
          fprintf(stderr, catgets(catd, 1, 16, "%s or an overlapping partition is open.\n"), special);
          fatal=1;
          break;

      case OV_ERR_INVALID_DEV:
          fprintf(stderr, catgets(catd, 1, 17, "%s is an invalid device or cannot be opened\n"), special);
          fatal=1;
          break;

      case OV_ERR_DSKLBL_UPD:
          if (islsm64(special, &zeroed_lsm_size)) {
              fprintf(stderr, catgets(catd, 1, 45, "Error: The fstype for %s cannot be updated.\n"), special);
              if (clu_is_member())
                  fprintf(stderr, catgets(catd, 1, 46, "       Make sure node serving domain %s is running /sbin/vold.\n"), domain);
              else
                  fprintf(stderr, catgets(catd, 1, 47, "       Make sure /sbin/vold is running.\n"));
          } else {
              fprintf(stderr, catgets(catd, 1, 18, "The disklabel for %s cannot be updated.\n"), special);
          }
          fatal=1;
          break;

      case OV_ERR_ADVFS_CHECK:
          fprintf(stderr, catgets(catd, 1, 24, "Errors in checking %s with active AdvFS domains.\n"), special);
          fprintf(stderr, catgets(catd, 1, 25, "/etc/fdmns directory seems to be missing or wrong.\n"));
          fatal=1;
          break;

      case OV_ERR_SWAP_CHECK:
          fprintf(stderr, catgets(catd, 1, 26, "Errors in checking %s with active swap devices.\n"), special);
          fprintf(stderr, catgets(catd, 1, 27, "Special device files associated with swap device(s) are missing.\n"));
          fatal=1;
          break;

      default:
          if(errcode < FSMAXTYPES) {
              fprintf(stderr, catgets(catd, 1, 19, "Warning: %s is marked in use for %s.\n"), special, fstypenames[errcode]);
              fatal=0;
          } else {
              fprintf(stderr, catgets(catd, 1, 20, "unknown error code of %d\n"), errcode);
              fatal=1;
          }
    }

    /*
     * If the error code is a valid fstype or it is 256 the
     * user will be given a chance to override the warning.
     * Else addvol will exit.
     */
    if(!fatal) {
        fprintf(stderr, catgets(catd, 1, 50, "If you continue with "
                "the operation you can \n"));
        fprintf(stderr,catgets(catd,1,51,"possibly destroy existing data.\n"));
    } else {
        fprintf(stderr, catgets(catd, 1, 52, "Quitting .... \n"));
    }

    return;
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

cfs_server(const char *node, const char *domain, char *server_name) {

  cfs_server_info_t info;
  cfg_status_t cfg_status;

    cfg_status = clu_filesystem((char *) domain,
                                CFS_SERVER,
                                CLU_CFS_GET,
                                CFS_DOMAIN_NAME,
                                &info,
                                sizeof(info),
                                NULL);

    if (cfg_status != CFG_SUCCESS) {
        syslog(LOG_ERR, "clu_filesystem error: %d\n", cfg_status);
        return -1;
    }

    if (info.status != CFS_S_OK) {
        switch(info.status) {
            case CFS_S_NOT_MOUNTED:           
                return 1;       /* local host will be server */
            case CFS_S_FAILOVER_IN_PROGRESS:
            case CFS_S_RELOCATION_IN_PROGRESS:
                return -2;      
            default:
                syslog(LOG_ERR, "CLU_CFS_STATUS: %d\n", info.status);
                return -1;
        }
    }

    if (strcmp(node, (strtok(info.name, ".")))) {
        strcpy(server_name, info.name);
        return 0;
    }

    return 1;
}



