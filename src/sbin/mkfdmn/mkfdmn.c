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
 *      AdvFS - Advanced File System
 *
 * Abstract:
 *
 *      Create a new AdvFS file system domain
 *
 * Date:
 *
 *      Wed Sep 18 09:22:09 1991
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: mkfdmn.c,v $ $Revision: 1.1.50.3 $ (DEC) $Date: 2002/06/03 19:11:58 $";
#endif

#include <stdio.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#define DKTYPENAMES
#include <sys/disklabel.h>

#include <io/common/devgetinfo.h>
#include <sys/ioctl.h>
#include <sys/devio.h>

#include <stdarg.h>
#include <overlap.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <locale.h>
#include "mkfdmn_msg.h"
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <sys/clu.h>
#include <cfs/cfs_user.h>
#include <clua/clua.h>

#ifdef ESS
#include <ess/event_simulation.h>
#endif

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define SYSERRMSG( e ) sys_errlist[e]

char *Prog = "mkfdmn";

void print_error();

static void
usage( void )
{
printf( catgets(catd, 1, USAGE2, "usage: %s [-o] [-r] [-F] [-l num_log_pages] [-V version]\n              [-x meta_extent_size] [-p meta_prealloc_size]\n              special domain\n"), Prog ); 
}



main ( int argc, char *argv[] )
{
    char rmCmd[MAXPATHLEN+8] = "rm -rf ";
    char dmnName[MAXPATHLEN+1];
    mlBfDomainIdT bfDomainId;
    char *domain;
    char *linkName;
    int maxVols = 256;
    u32T logPgs = BS_DEF_LOG_PGS;
    mlServiceClassT logSvc = 0;
    mlServiceClassT tagSvc = 0;
    char *volName = NULL;
    char  specialChar[MAXPATHLEN];    
    char  specialBlock[MAXPATHLEN];
    char   lsm_dev[MAXPATHLEN+1];
    char   *vol, *dg;
    char volDiskGroup[64];
    char lsmName[MAXPATHLEN];
    mlServiceClassT volSvc = 0;
    u32T  volSize=0;
    i32T bmtXtntPgs = BS_BMT_XPND_PGS;
    i32T bmtPreallocPgs = 0;
    i32T domainVersion = 0;  /* Let system decide unless user overrides */
    mlStatusT sts, sts2;
    getDevStatusT error;
    int err, lkHndl, c, l = 0, t = 0, o = 0, s = 0, F = 0, x = 0,exists = 0, root = 0;
    char *pp, *cp;
    int dirLocked = 0;
    extern int optind;
    extern char *optarg;
    struct stat stats;
    struct dec_partid partid;
    char *tmpp;
    char tmp1[MAXPATHLEN];
    char tmp2[MAXPATHLEN];
    struct clu_gen_info  *clugenptr = NULL;
    
    void errprint(int code, ...);

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MKFDMN, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,30,
                "\nPermission denied - user must be root to run %s.\n\n"),
                Prog);
        usage();
        exit(1);
    }

    (void) signal( SIGINT, SIG_IGN ); /* ignore interrupt signals */
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "p:x:orFl:t:s:V:" )) != EOF) {
        switch (c) {
            case 'l':
                l++;
                logPgs = strtoul( optarg, &pp, 0 );
                break;

            case 's':
                s++;
                volSize = strtoul( optarg, &pp, 0 );
                break;

            case 'o':
                o++;
                break;

	    case 'r':
		root++;
		break;

	    case 'x':
                x++;
		bmtXtntPgs = strtol( optarg, &pp, 0 );
		if (bmtXtntPgs < BS_BMT_XPND_PGS) {
			bmtXtntPgs = BS_BMT_XPND_PGS;
        		fprintf(stderr, 
				catgets(catd, 1, 44, "%s: Invalid value for -x\n"),
               			argv[0]);
			fprintf(stderr,
				catgets(catd, 1, 45, "%s: Setting to minimum value of %d\n"),
               			argv[0], BS_BMT_XPND_PGS);
		}	
		break;

	    case 'p':
		bmtPreallocPgs = strtol( optarg, &pp, 0 );
		if (bmtPreallocPgs < 0) {
			bmtPreallocPgs = 0;
        		fprintf(stderr, 
				catgets(catd, 1, 46, "%s: Invalid value for -p\n"),
               			argv[0]);
			fprintf(stderr,
				catgets(catd, 1, 45, "%s: Setting to minimum value of %d\n"), argv[0], 0);
		}
		break;

            case 'F':
                F++;
                break;

	    case 'V':
                domainVersion = strtol( optarg, &pp, 0 );
                if ((domainVersion < FIRST_VALID_FS_VERSION) ||
                    (domainVersion > LAST_VALID_FS_VERSION)) {
                    fprintf(stderr, 
                           catgets(catd, 1, 48, 
                                   "%s: Invalid value for -V.  On-disk version must be between %d and %d.\n"), 
                                   argv[0], 
                                   FIRST_VALID_FS_VERSION,
                                   LAST_VALID_FS_VERSION);
                    exit(1);
                }
                break;

            case '?':

            default:
                usage();
                exit( 1 );
        }
    }

    if (l > 1 || o > 1 || s > 1) {
        usage();
        exit( 1 );
    }

    if (optind != (argc - 2)) {
        /* missing required args */
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

    if (strlen( domain ) >= ML_DOMAIN_NAME_SZ) {
        fprintf( stderr, 
                 catgets(catd, 1, 3, "%s: domain name '%s' exceeds limit of %d characters\n"),
                 Prog, domain, ML_DOMAIN_NAME_SZ - 1 );
        goto _error1;
    }

    if (cp = strpbrk( domain, "/# :*?\t\n\f\r\v" )) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: invalid character '%c' in domain name '%s'\n"),
                 Prog, *cp, domain );
        goto _error1;
    }
    /* 
     * If in a cluster,
     *   Make sure the volume is not part of the quorum disk
     */

    if (clu_is_member()) {
        if ( clu_get_info(&clugenptr) || (clugenptr == NULL) ) {
            fprintf(stderr, catgets(catd, 1, 49,
                    "%s: cluster filesystem error \n"), Prog);
            goto _error1; 
        }
        if (clugenptr->qdisk_name != NULL) {
            tmpp = strrchr( volName, '/' );
            tmpp = tmpp ? tmpp+1 : volName;
            strncpy(tmp1,tmpp,(strlen(tmpp))-1);
            tmp1[strlen(tmpp)-1] = NULL;
            strncpy(tmp2,clugenptr->qdisk_name,(strlen(clugenptr->qdisk_name))-1);
            tmp2[strlen(clugenptr->qdisk_name)-1] = NULL;
            if (strcmp(tmp1,tmp2) == 0) {
                fprintf(stderr, catgets(catd, 1, 50,
                        "%s: Can't use any part of the quorum disk for domain storage\n"), Prog);
                goto _error1;
            }
        }
    }

_retry:

    if (access( MSFS_DMN_DIR, R_OK | X_OK | W_OK )) {

        if (errno == ENOENT) {
            if (mkdir( MSFS_DMN_DIR, 0755 )) {
                fprintf( stderr, catgets(catd, 1, 5, "%s: can't create dir '%s'; [%d] %s\n"),
                         Prog, MSFS_DMN_DIR, errno, SYSERRMSG( errno ) );
                goto _error1;
            }

        } else {
            fprintf( stderr, catgets(catd, 1, 6, "%s: error access dir '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, errno, SYSERRMSG( errno ) );
            goto _error1;
        }
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 7, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        goto _error1;
    }

    dirLocked = 1;

    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, domain );

    if (!lstat( dmnName, &stats )) {
        if (!o) {
            fprintf( stderr, catgets(catd, 1, 8, "%s: domain '%s' already exists\n"), 
                     Prog, dmnName );
            goto _error1;
        }

        exists = 1;
    }

    if (!volume_avail( Prog, domain, volName )) {
        goto _error1;
    }

    sts = set_usage(volName, FS_ADVFS, F);
    if ((sts < 0) || sts > OV_ERR_MULT_FSTYPE_OVERLAP) {
        print_error(volName, sts);
        goto _error1;
    } else {

        if (!volSize) {
            errno = 0;
            error = get_dev_size( volName, (char *)NULL, &volSize );
            if (error != GDS_OK) {
                fprintf( stderr, catgets(catd, 1, 9,
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
                fprintf(stderr, catgets(catd, 1, 51,
                        "At least one overlapping partition belongs to "
                        "an existing AdvFS domain.\n"));
                fprintf(stderr, catgets(catd, 1, 23, "Quitting .... \n"));
                goto _error1;
            } else if ((sts2 < 0) || sts2 > OV_ERR_MULT_FSTYPE_OVERLAP) {
                print_error(volName, sts2);
                goto _error1;
            } else {
                print_error(volName, sts);
                if (!F && isatty(fileno(stdin))) {

                    /*
                     * Unlock the /etc/fdmns directory so that other
                     * utilities can proceed while the user is deciding
                     * whether or not he wants to continue.
                     */
                    if (unlock_file( lkHndl, &err ) == -1) {
                        fprintf(stderr,catgets(catd,1,12,"%s: error unlocking '%s'; [%d] %s\n"),
                                 Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
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
                    goto _retry;

                } else {
                    goto _error1;
                }
            }
        }
    }

    /* If logPgs or bmtXtntPgs is specified, verify acceptability */
    if (x != 0 || l != 0)
    {
        if (!(BMT_EXT_REQUEST_OK(bmtXtntPgs, logPgs, BS_CLUSTSIZE)))
        {
            fprintf(stderr, catgets(catd, 1, 31, "\nError:\n\t-x (extend size) parameter and -l (log size) relationship is unacceptable.\n"));
            fprintf(stderr, catgets(catd, 1, 32, "\t-x value cannot exceed 25%% of (size of log * %d)\n"), SBM_ADVFS_PGS(BS_CLUSTSIZE));
            fprintf(stderr, catgets(catd, 1, 33, "\tCurrent size of log (-l) = %d\n"), logPgs);
            fprintf(stderr, catgets(catd, 1, 34, "\tCurrent bmt extend size (-x) = %d\n "), bmtXtntPgs);
            fprintf(stderr, catgets(catd, 1, 35, "\nPlease modify bmt extend size (-x) or size of log (-l) parameter and try again.\n"));
            fprintf(stderr, catgets(catd, 1, 36, "Maximum -x value for log size (%d) is %d\n"),
                            logPgs,  (BMT_EXT_REQUEST_DEFAULT(logPgs, BS_CLUSTSIZE)));
            fprintf(stderr, catgets(catd, 1, 37, "Minimum -l value for bmt extend size (%d) is %d\n\n"),
                            bmtXtntPgs, BMT_EXT_LOGSZ_DEFAULT(bmtXtntPgs, BS_CLUSTSIZE));
            goto _error;
        }
    }

    sts = msfs_dmn_init( domain,
			 (root ? 1 : maxVols), /* One disk in root domain */
                         logPgs,
                         logSvc,
                         tagSvc,
                         volName,
                         volSvc,
                         volSize,
                         bmtXtntPgs,
                         bmtPreallocPgs,
                         domainVersion,
                         &bfDomainId );
    if (sts != EOK) {
        printf( catgets(catd, 1, 10, "%s: domain init error %s\n"), Prog,  BSERRMSG( sts ) );
        goto _error;
    }

/* Warn user if -x value is greater than 10,000 pages.  The time it will take to
 * actually extend and initialize the BMT could be significant.
 * Through empirical study the elapsed time to extend/init ~5000 pages is one minute.
 * This only a VERY rough esimate.  Time will of course vary according
 * to machine/configuration.  Print the warning if the time to extend/init the BMT
 * might be greater than 10 minutes.
 */

    if (bmtXtntPgs > 5000*2)
    {
        fprintf(stderr, catgets(catd,1,38, "\nWarning: domain '%s' created.  However extending the BMT\n"),
                domain);
        fprintf(stderr, catgets(catd,1,39, "with this -x value (%d) will take approximately %d minutes.\n"),
                bmtXtntPgs, (bmtXtntPgs / 5000));
        fprintf(stderr, catgets(catd,1,40, "\nIf this is unacceptable, run 'rmfdmn %s' and then\n"), domain);
        fprintf(stderr, catgets(catd,1,41, "re-create the domain with more reasonable -x or -l values.\n"));

        fprintf(stderr, catgets(catd,1,42, "\nIt will take approximately one minute of elapsed\n"));
        fprintf(stderr, catgets(catd,1,43, "time for every 5000 pages represented in the -x value.\n\n"));
    }

    if (exists && o) {
        strcat( rmCmd, dmnName );
        system( rmCmd );
    }

    if (mkdir( dmnName, 0755 )) {
       fprintf( stderr, catgets(catd, 1, 5, "%s: can't create dir '%s'; [%d] %s\n"),
                Prog, dmnName, errno, SYSERRMSG( errno ) );
       goto _error;
    }

    /*
     * If diskgroup volume we need to construct a linkName which is part 
     * diskgroup and part volume name.  This needs to be done because you 
     * can have two volumes with the same name but in different diskgroups
     */
    if (checklsm(volName, volDiskGroup, lsmName)) {
        char temp[MAXPATHLEN];

	sprintf(temp,
		"/%s.%s",
		volDiskGroup,
		lsmName);
	linkName = strcat( dmnName, temp);
    } else {
	linkName = strcat( dmnName, strrchr( volName, '/' ) );
    }

    if (symlink( volName, linkName )) {
       fprintf( stderr, catgets(catd, 1, 11, "%s: can't create symlink '%s @--> %s'; [%d] %s\n"),
                Prog, linkName, volName, errno, SYSERRMSG( errno ) );
       goto _error;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 12, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        dirLocked = 0;  /* Prevent another attempted (at _error:) unlocking. */
        goto _error;
    }

#ifdef ESS

    /* QA Test Case: perform failure when mkfdmn completed the process */
    ESS_MACRO_0(TCRGenPanicKernel, ADVFS_MKFDMN_PANIC);

#endif

    return 0;

_error:
    sts = set_usage( volName, FS_UNUSED, 1 );
_error1:
    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 12, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        }
    }

    fprintf( stderr, catgets(catd, 1, 13, "%s: can't create new domain '%s' \n"), Prog, domain );
    exit( 1 );
}

void
print_error(special, errcode)
char *special;
int errcode;
{
    int fatal=0; /* Set to 1, if application should quit. */


    switch(errcode) {
        case OV_ERR_MULT_FSTYPE_OVERLAP:
                fprintf(stderr, catgets(catd, 1, 28, "Specified partition %s is marked in use.\n"),
                                 special);
                fprintf(stderr, catgets(catd, 1, 29, "Also partition(s) which overlap %s are marked in use.\n"),
                                 special);
                fatal=0;
                break;
        case OV_ERR_FSTYPE_OVERLAP:
                fprintf(stderr, catgets(catd, 1, 14, "Partition(s) which overlap %s are marked in use.\n"),
                                 special);
                fatal=0;
                break;
        case OV_ERR_INVALID_DSKLBL:
                fprintf(stderr, catgets(catd, 1, 15, "The disklabel for %s does not exist or is corrupted.\n"),
                                 special);
                fatal=1;
                break;
        case OV_ERR_OPEN_OVERLAP:
                fprintf(stderr, catgets(catd, 1, 16, "%s or an overlapping partition is open.\n"),
                                 special);
                fatal=1;
                break;
        case OV_ERR_INVALID_DEV:
                fprintf(stderr, catgets(catd, 1, 17, "%s is an invalid device or cannot be opened\n"),
                                 special);
                fatal=1;
        case OV_ERR_DSKLBL_UPD:
                fprintf(stderr, catgets(catd, 1, 18, "The disklabel for %s cannot be updated.\n"),
				special);
                fatal=1;
                break;
        case OV_ERR_ADVFS_CHECK:
                fprintf(stderr, catgets(catd, 1, 24, "Errors in checking %s with active AdvFS domains.\n"),
                                 special);
                fprintf(stderr, catgets(catd, 1, 25, "/etc/fdmns directory seems to be missing or wrong.\n"));
                fatal=1;
                break;
        case OV_ERR_SWAP_CHECK:
                fprintf(stderr, catgets(catd, 1, 26, "Errors in checking %s with active swap devices.\n"),
                                 special);
                fprintf(stderr, catgets(catd, 1, 27, "Special device files associated with swap device(s) are missing.\n"));
                fatal=1;
                break;

        default:
                if(errcode < FSMAXTYPES) {
                        fprintf(stderr, 
				catgets(catd, 1, 19, "Warning: %s is marked in use for %s.\n"),
				special, fstypenames[errcode]);
			fatal = 0;
                } else {
                        fprintf(stderr, catgets(catd, 1, 20, "unknown error code of %d\n"),
                        errcode);
			fatal = 1;
		}
    }


        /*
         * If the error code is a valid fstype or it is 256 the
         * user will be given a chance to override the warning.
         * Else mkfdmn will exit.
         */
        if(!fatal) {
                fprintf(stderr, catgets(catd, 1, 21, "If you continue with the operation you can \n"));
                fprintf(stderr, catgets(catd, 1, 22, "possibly destroy existing data.\n"));
        }
        else {
                fprintf(stderr, catgets(catd, 1, 23, "Quitting .... \n"));
        }
        return;
}
