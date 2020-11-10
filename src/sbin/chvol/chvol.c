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
 *    chvol.c - change the attributes of a volume.
 *
 *        Undocumented flags: -q qtodevice,
 *                            -b blocking queue factor.
 *
 * Date:
 *
 *    October 20, 1992
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: chvol.c,v $ $Revision: 1.1.51.1 $ (DEC) $Date: 2004/10/06 16:38:18 $";
#endif

#include <stdio.h>
#include <sys/file.h>
#include <string.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <sys/errno.h>
#include <locale.h>
#include <overlap.h>
#include "chvol_msg.h"

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define ERRMSG( e ) sys_errlist[e]

char *Prog;

static void
usage( void  )
{
    fprintf(stderr, catgets(catd, 1, USAGE, "usage: %s [-l] [-r blks] [-w blks] [-t blks] [-c on | off] [-l] [-A] special domain\n"), Prog);
}

static
int
getvdparams(
    char *dmnName,                /* in  */
    char *volName,                /* in  */
    int flag,                     /* in  */
    mlBfDomainIdT *dmnId,         /* out */
    int *vdi,                     /* out */
    mlVolIoQParamsT *volp,        /* out */
    mlServiceClassT *volSvcClass  /* out */
    );

main(
    int argc,
    char *argv[]
    )
{
    int thr, rb, wb, bf, qd;
    char *pp = 0;
    int t, r, w, b, q, c, A, l;
    char *volName = NULL;
    char *domain;
    char specialBlock[MAXPATHLEN];
    char specialChar[MAXPATHLEN];
    char lsm_dev[MAXPATHLEN+1];
    char *tmpp, *vol, *dg;
    char *consol = NULL;
    int ch;
    int res;
    mlBfDomainIdT dmnId;
    int vdi;
    mlVolIoQParamsT volIoQParams;
    mlServiceClassT volSvcClass;
    mlStatusT sts;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    int retval=0;
    int rbmax, wbmax, rbpref, wbpref;

    t = r = w = b = q = c = A = l = 0;
    Prog = argv[0];

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_CHVOL, NL_CAT_LOCALE);
    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,31,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

    /*
     * Get user specified command line arguments.
     */

    while( (ch = getopt( argc, argv, "Alr:w:b:q:t:c:" )) != EOF ) {
        switch( ch ) {

            case 't':       /* ReadyLazyQ threshhold */
                t++;    
                thr = strtoul( optarg, &pp, 0 );
                break;

            case 'r':       /* Max read block consolidate */
                r++;
                rb = strtoul( optarg, &pp, 0 );
                break;

            case 'w':       /* Max write block consolidate */
                w++;
                wb = strtoul( optarg, &pp, 0 );
                break;

            case 'b':       /* Blocking queue factor */
                b++;
                bf = strtoul( optarg, &pp, 0 );
                break;

            case 'q':       /* Blocking queue factor */
                q++;
                qd = strtoul( optarg, &pp, 0 );
                break;

            case 'c':       /* Consolidate toggle */
                c++;
                consol = optarg;
                break;

            case 'A':       /* Add the volume back into the domain after a
                             * partial rmvol operation. */
                A++;
                break;

            case 'l':
                l++;
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if( optind != (argc - 2) ) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, ONDISK,
            "%s: This utility can not process the on-disk structures on this system.\n"), Prog);
        exit(1);
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

    res = getvdparams(
        domain,
        volName,
        A,
        &dmnId,
        &vdi,
        &volIoQParams,
        &volSvcClass
        );
    if( res < 0 ) {
        exit( 1 );
    }

    /*
     * Modify the required params.
     */

#define THRMAX 32768L
#define THRMIN 0L
#define RBMIN 16L
#define WBMIN 16L
#define BFMIN 1L
#define BFMAX 256L
#define QDMIN 0L
#define QDMAX 256L

    rbmax = volIoQParams.max_iosize_rd/DEV_BSIZE;
    rbpref = volIoQParams.preferred_iosize_rd/DEV_BSIZE;
    wbmax = volIoQParams.max_iosize_wr/DEV_BSIZE;
    wbpref = volIoQParams.preferred_iosize_wr/DEV_BSIZE;

    if( c ) {
        /*
         * xor to toggle the bit
         */
        if( consol && strcmp( "on", consol ) == 0 ) {
            volIoQParams.consolidate = 1;
        } else if( consol && strcmp( "off", consol ) == 0 ) {
            volIoQParams.consolidate = 0;
        } else {
            fprintf(stderr, catgets(catd, 1, 2, "%s: consol arg must be \"on\" or \"off\"\n"),
                     Prog);
            retval = 1;
            goto _end;
        }
    }

    if( t ) {
        if( (thr % 16) > 0 ) {
	    fprintf(stderr, catgets(catd, 1, 4, "%s: threshold not a multiple of 16\n"), Prog);
            retval = 1;
            goto _end;
        }

        if( thr >= THRMIN && thr <= THRMAX ) {
            volIoQParams.lazyThresh = thr / 16;
        } else {
            fprintf(stderr, catgets(catd, 1, 5, "%s: threshold not within range %d - %d\n"),
                     Prog, THRMIN, THRMAX);
            retval = 1;
            goto _end;
        }
    }
    if( r ) {
        if( rb >= RBMIN && rb <= rbmax ) {
            volIoQParams.rdMaxIo = rb;
        } else {
            fprintf(stderr, catgets(catd, 1, 6, "%s: read blocks not within range %d - %d\n"),
                     Prog, RBMIN, rbmax);
            retval = 1;
            goto _end;
        }
    }
    if( w ) {
        if( wb >= WBMIN && wb <= wbmax ) {
            volIoQParams.wrMaxIo = wb;
        } else {
            fprintf(stderr, catgets(catd, 1, 7, "%s: write blocks not within range %d - %d\n"),
                     Prog, WBMIN, wbmax);
            retval = 1;
            goto _end;
        }
    }
    if( b ) {
        if( bf >= BFMIN && bf <= BFMAX ) {
            volIoQParams.blockingFact = bf;
        } else {
            fprintf(stderr, catgets(catd, 1, 8, "%s: block fact not within range %d - %d\n"),
                     Prog, BFMIN, BFMAX);
            retval = 1;
            goto _end;
        }
    }
    if( q ) {
        if( qd >= QDMIN && qd <= QDMAX ) {
            volIoQParams.qtoDev = qd;
        } else {
            fprintf(stderr, catgets(catd, 1, 9, "%s: queue device not within range %d - %d\n"),
                     Prog, QDMIN, QDMAX);
            retval = 1;
            goto _end;
        }
    }

    /*
     * Print a warning if not consolidating but read or
     * write blocks were changed.
     */
    if( volIoQParams.consolidate == 0 && (r || w) ) {
        fprintf(stderr, 
                catgets(catd, 1, 10, "%s: -r or -w has no immediate effect without consolidate\n"), 
                Prog);
    }

    /*
     * Print the current parameters if nothing was changed.
     */
    if( !(A || c || t || r || w || b || q || l) ) {
        fprintf( stdout, catgets(catd, 1, 15, "rblks = %d  wblks = %d  cmode = %s, thresh = %d\n"),
            volIoQParams.rdMaxIo, volIoQParams.wrMaxIo,
            volIoQParams.consolidate ? "on" : "off",
            volIoQParams.lazyThresh * 16);
        exit( 0 );
    }


    if (r || w || t || b || q || c) {
        /*
         * Stuff the new values
         */
        sts = advfs_set_vol_ioq_params( dmnId, vdi, &volIoQParams );

        if (sts != EOK) {
            fprintf(stderr, catgets(catd, 1, 16, "%s: can't set I/O parameters\n"), Prog);
            if( sts != -1 ) {
                fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG (sts));
            } else {
                fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, ERRMSG (errno));
            }
            retval = 1;
            goto _end;
        }
    }

    retval = 0;

_end:

    if ( A ) {
     /*
      * Re-add the volume to the service class.  Here, it is first necessary
      * to remove the volume, in case it is still in the service class, because
      * adding it to the service class does not check to see if it is already
      * there.
      */
        sts = advfs_remove_vol_from_svc_class( domain, vdi, volSvcClass );
        if (sts != EOK && sts != EBAD_VDI) {
            fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG (sts));
            retval = 1;
        }

        sts = advfs_add_vol_to_svc_class( domain, vdi, volSvcClass );
        if (sts != EOK) {
            fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG (sts));
            retval = 1;
        }
    }

    if (l) {
        fprintf( stdout, catgets(catd, 1, 34, "rblks: min = %d  max = %d  pref = %d\nwblks: min = %d  max = %d  pref = %d\n"), RBMIN, rbmax, rbpref, WBMIN, wbmax, wbpref);
    }

    exit(retval);
}

/*
 * Get the domain id, the vd index, and the vd I/O params.
 * Return 0 if successful, -1 if an error occurs.
 */

static
int
getvdparams(
    char *dmnName,          /* in */
    char *volName,          /* in */
    int  flag,              /* in */
    mlBfDomainIdT *dmnId,   /* out */
    int *vdi,               /* out */
    mlVolIoQParamsT *volp,  /* out */
    mlServiceClassT *volSvcClass  /* out */
    )

{
    char dmnPathName[MAXPATHLEN+1];
    int err;
    struct stat stats;
    mlDmnParamsT dmnParams;
    mlStatusT sts;
    char *volPathName, *ovolPathName;    
    char *subs;
    u32T *volIndex = NULL;
    int volIndexCnt;
    mlVolInfoT volInfo;
    mlVolCountersT volCounters;
    mlVolPropertiesT volProp;
    char volLink[MAXPATHLEN+1];
    int volIndexI = -1;
    int i;
    int dmnLock, thisDmnLock;
    char volDiskGroup[64];
    char lsmName[MAXPATHLEN+1];

    /*
     * Lock the master domain directory.  This synchronizes us with domain and bitfile
     * set creation/deletion.
     */
    dmnLock = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (dmnLock < 0) {
        fprintf (stderr, catgets(catd, 1, 18, "%s: error locking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 19, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRMSG (err));
        goto _error;
    }

    /*
     * Verify that the domain exists and its pathname is in the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, dmnName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
        if( errno == ENOENT ) {
            fprintf(stderr, catgets(catd, 1, 20, "%s: Domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf(stderr, catgets(catd, 1, 21, "%s: error getting '%s' stats; [%d] %s\n"),
                     Prog, dmnPathName, errno, ERRMSG (errno));
        }
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                 catgets(catd, 1, 22, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 18, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 19, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    }
 
    sts = msfs_get_dmnname_params( dmnName, &dmnParams );
    if( sts != EOK ) {
        fprintf(stderr, catgets(catd, 1, 23, "%s: can't get domain parameters\n"), Prog);
        if( sts != -1 ) {
            fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG (sts));
        } else {
            fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, ERRMSG (errno));
        }
        goto _error;
    }

    /*
     * Translate the volume name into a special device pathname.  Verify that
     * it is a valid device name.
     */

    /*
     * Due to changes in addvol and mkfdmn We need to be 
     * make sure we are able to handle links.  We also
     * Need to make sure we can still handle the old format.
     */

    if (!checklsm(volName, volDiskGroup, lsmName)) {
    
	subs = (char *) strrchr (volName, '/');
	if (subs == NULL) {
	    fprintf(stderr, catgets(catd, 1, 24, "%s: error looking for '/' in %s\n"),
		    Prog, volName );
	    goto _error;
	}
	volPathName = (char *) strcat (dmnPathName, subs);
	
    } else {

        char temp[MAXPATHLEN+1];
        char tmpPathName[MAXPATHLEN+1];
	sprintf(temp,
		"/%s.%s",
		volDiskGroup,
		lsmName);
	volPathName = strcpy(tmpPathName, dmnPathName); 
	volPathName = (char *) strcat (tmpPathName, temp);

	err = lstat( volPathName, &stats );
    	if( err != 0 ) {
    	
            if( errno != ENOENT ) {

                fprintf(stderr, catgets(catd, 1, 21, "%s: error getting '%s' stats; [%d] %s\n"),
                     Prog, volPathName, errno, ERRMSG (errno));
        	goto _error;
        	
            } else {
                        
	        /*
	         * Construct ancestor vol path name and try that one too
	         */
	         
	        char otmpPathName[MAXPATHLEN+1];
               	sprintf(temp, "/%s", lsmName);		
		ovolPathName = strcpy(otmpPathName, dmnPathName);
		ovolPathName = (char *) strcat (otmpPathName, temp);    
		err = lstat( ovolPathName, &stats );
		            
	        if( err != 0 ) {
	            if( errno == ENOENT ) {
            		fprintf(stderr, catgets(catd, 1, 25, "%s: Volume directory '%s' does not exist\n"),
                     		Prog, volPathName);
        	    } else {
            	        fprintf(stderr, catgets(catd, 1, 21, "%s: error getting '%s' stats; [%d] %s\n"),
                    		Prog, volPathName, errno, ERRMSG (errno));
        	    }
        	    goto _error;
    	        }
    	        /*
    	         * Fixup the confirmed old style link name problem
    	         * and print fixup processing messages
    	         */
    	         
    	         fprintf(stderr, catgets(catd, 1, 35,"%s: A potentially ambiguous domain volume name was found.\n"), 
    	                 Prog);
    	         fprintf(stderr, catgets(catd, 1, 36,"    Changing from: %s\n"),        
    	         	 ovolPathName);
    	         err = rename(ovolPathName, volPathName);
    	         
  		 if( err !=0) {
  		     fprintf(stderr, catgets(catd, 1, 38, "   Error renaming as: %s\n"),
  		             volPathName);
  		     goto _error; 
  		 } else {
  		     fprintf(stderr, catgets(catd, 1, 37,"               to: %s\n"),   
  		             volPathName);	        
    	         }     	       
            }
        }
    }

    err = lstat( volPathName, &stats );
    if( err != 0 ) {
        if( errno == ENOENT ) {
            fprintf(stderr, catgets(catd, 1, 25, "%s: Volume directory '%s' does not exist\n"),
                     Prog, volPathName);
        } else {
            fprintf(stderr, catgets(catd, 1, 21, "%s: error getting '%s' stats; [%d] %s\n"),
                     Prog, volPathName, errno, ERRMSG (errno));
        }
        goto _error;
    }

    err = readlink( volPathName, volLink, MAXPATHLEN+1 );
    if( err < 0 ) {
        fprintf(stderr, catgets(catd, 1, 26, "%s: error getting '%s' symbolic link; [%d] %s\n"),
                 Prog, volPathName, errno, ERRMSG (errno));
        goto _error;
    }

    /* FIX - must I verify that special device file exist 
     * and is part of the domain? 
     */

    /*
     * Translate the special device pathname into a volume index.
     */

    volIndex = (u32T *)malloc( dmnParams.curNumVols * sizeof( u32T ) );
    if( volIndex == NULL ) {
        fprintf(stderr, catgets(catd, 1, 27, "%s: can't allocate memory for volume index array\n"),
            Prog);
        goto _error;
    }

    sts = msfs_get_dmn_vol_list(
                                 dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 &volIndex[0],
                                 &volIndexCnt
                                 );

    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, 1, 32, "%s: error: All filesets must be mounted\n"),Prog );
        goto _error;
    } else if( sts != EOK ) {
        fprintf(stderr, catgets(catd, 1, 28, "%s: can't get domain's volume list\n"), Prog);
        fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG( sts ));
        goto _error;
    }

    for( i = 0; i < volIndexCnt; i++ ) {
        if (!flag) {
            sts = advfs_get_vol_params(
                                    dmnParams.bfDomainId,
                                    volIndex[i],
                                    &volInfo,
                                    &volProp,
                                    volp,
                                    &volCounters
                                    );
        } else {
            sts = advfs_get_vol_params2(
                                    dmnParams.bfDomainId,
                                    volIndex[i],
                                    &volInfo,
                                    &volProp,
                                    volp,
                                    &volCounters,
                                    GETVOLPARAMS2_CHVOL_OP_A
                                    );
        }
        if( sts != EOK ) {
            fprintf(stderr, catgets(catd, 1, 29, "%s: can't get volume parameters\n"), Prog);
            fprintf(stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG (sts));
            goto _error;
        }
        if( strcmp( volLink, volProp.volName ) == 0 ) {
            /*
             * Matching name!
             */
            volIndexI = i;
            break;  /* out of for */
        }
    }  /* end for */

    if( (volIndexI == -1) || (volIndexI >= volIndexCnt )) {
        fprintf(stderr, catgets(catd, 1, 30, "%s: volume '%s' is not a member of domain '%s'\n"),
                 Prog, volName, dmnName);
        goto _error;
    }

    *dmnId =dmnParams.bfDomainId;
    *vdi = volInfo.volIndex;
    *volSvcClass = volProp.serviceClass;

    return( 0 );
_error:

    return( -1 );
}
