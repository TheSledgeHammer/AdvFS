/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
 */
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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      'show file domain' utility.
 *
 * Date:
 *
 *      Fri May  8 10:55:24 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: showfdmn.c,v $ $Revision: 1.1.34.2 $ (DEC) $Date: 2001/12/06 15:07:40 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <pwd.h>
#include <grp.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/mount.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "showfdmn_msg.h"

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define ERRMSG( e ) sys_errlist[e]

#define TRUE 1
#define FALSE 0

static char *Prog = "showfdmn";

#define MODE_STR_SZ 10
#define BUF_SZ 15

#define USER_QUOTA_FILE_TAG  4
#define GROUP_QUOTA_FILE_TAG 5


main( int argc, char *argv[] )
{
    char *domain;
    mlStatusT sts;
    mlDmnParamsT dmnParams;
    struct passwd *pwd;
    struct group *grp;
    char mode_str[MODE_STR_SZ];
    int err, lkHndl, dirLocked = 0;
    int k = 0, m = 0, c, ii, errflag;
    u32T dmnMetaTotal = 0;
    extern int optind;
    extern char *optarg;
    int errorFlag = 0;
    char domain_dir[256];
    struct stat stats;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_SHOWFDMN, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "km" )) != EOF) {
        switch (c) {
            case 'k':
                k++;
                break;

	    case 'm':
	        m++;
	        break;	

            default:
                usage();
                exit( 1 );
        }
    }

    if (k > 1 || m > 1) {
        usage();
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
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
    
    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_SH, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 17, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    for (ii = optind; ii < argc; ii++) {
        
        domain = argv[ii];
	errflag = 0;
	sprintf(domain_dir,"%s/%s", MSFS_DMN_DIR, domain);
	if (stat(domain_dir, &stats) < 0) { 
            fprintf( stderr, catgets(catd, 1, 18, "%s: no such domain '%s'\n"), 
                     Prog, domain );
	    goto _error;
	} else {
            if (!S_ISDIR( stats.st_mode )) {
                fprintf( stderr, catgets(catd, 1, 18, "%s: no such domain '%s'\n"), 
                     Prog, domain );
		goto _error;
 	    } else {
            	sts = msfs_get_dmnname_params( domain, &dmnParams );
            	if (sts != EOK) {
                    fprintf( stderr, catgets(catd, 1, 19, "%s: unable to get info for domain '%s'\n"), 
                        Prog, domain );
                    fprintf (stderr, catgets(catd, 1, 20, "%s: error = %s\n"), 
                             Prog, BSERRMSG (sts));
		    goto _error;
		}
	    }
	}
	    
	if (m) {

            /*
             * Domain metadata size request
             */

	    if (get_dmn_metadata_size( domain, &dmnParams, &dmnMetaTotal) !=  0 ) {
		/* error or no sets in the domain */	
		goto _error;
	    }
	    
    	    if ( k == 0 ) {
        	printf( catgets( catd, 1, 32,
                "Domain: %s, Metadata Size = %d Blocks\n"),
                 domain, dmnMetaTotal);
    	    } else {
        	printf( catgets( catd, 1, 33,
                "Domain: %s, Metadata Size = %d Blocks (1K)\n"),
                 domain, (dmnMetaTotal+1)/2);     
    	    }
            
	    

        } else {	    
	    
	    if (errflag == 0) {
                char s[32];
printf( catgets(catd, 1, 22, "\n               Id              Date Created  LogPgs  Version  Domain Name\n") );
                printf( "%08x.%08x  ", 
                        dmnParams.bfDomainId.tv_sec, 
                        dmnParams.bfDomainId.tv_usec );
                sprintf( s, "%s", ctime((time_t *) &dmnParams.bfDomainId.tv_sec) );
                s[strlen( s ) - 1] = '\0';
                printf( "%-24s  ", s );
                printf( "%6d  ", dmnParams.ftxLogPgs );
                printf("%7d  ", dmnParams.dmnVersion);
                printf( "%-s\n", domain );
    
                if (show_vols( dmnParams, k )) {
                    goto _error;
                }
            }
        }
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 23, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        dirLocked = FALSE;
        goto _error;
    }

return 0;

_error:
    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 23, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    exit( 1 );
}


char *
mode_to_str(
    mode_t mode,
    char *str
    )
{
    int i;
    for (i = 0; i < MODE_STR_SZ - 1; i++) {
        str[i] = '-';
    }
    str[9] = '\0';

    if (mode & S_IRUSR) {
        str[0] = 'r';
    } 
    if (mode & S_IWUSR) {
        str[1] = 'w';
    } 
    if (mode & S_IXUSR) {
        str[2] = 'x';
    } 
    if (mode & S_IRGRP) {
        str[3] = 'r';
    } 
    if (mode & S_IWGRP) {
        str[4] = 'w';
    } 
    if (mode & S_IXGRP) {
        str[5] = 'x';
    } 
    if (mode & S_IROTH) {
        str[6] = 'r';
    } 
    if (mode & S_IWOTH) {
        str[7] = 'w';
    } 
    if (mode & S_IXOTH) {
        str[8] = 'x';
    }

    return str;
}


usage( void )
{
    fprintf( stderr, catgets(catd, 1, USAGE, "usage: %s [-k] domain ...\n"), Prog );
}


int show_vols( 
    mlDmnParamsT dmnParams,
    int k
    )
{
    mlStatusT sts;
    char *tok;
    int vol, vols, volIndex, logVolIndex;
    mlVolInfoT volInfo;
    mlVolPropertiesT volProperties;
    mlVolIoQParamsT volIoQParams;
    mlVolCountersT volCounters;
    u32T *volIndexArray;
    long totalSpace = 0L, totalFree = 0L;
    int volError;
    int total = 1;

    logVolIndex = ML_BFTAG_VDI( dmnParams.ftxLogTag, dmnParams.dmnVersion );

    volIndexArray = (u32T*) malloc( dmnParams.curNumVols * sizeof(u32T) );
    if (volIndexArray == NULL) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: out of memory\n"), Prog );
        return 1;
    }

    sts = msfs_get_dmn_vol_list( dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 volIndexArray, 
                                 &vols );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stdout, catgets(catd, 1, 3, "%s: unable to display volume info; domain not active\n"), Prog );
        return 0;

    } else if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: get dmn vol list error %s\n"), 
                 Prog, BSERRMSG( sts ) );
        free( volIndexArray );
        return 1;
    }

    printf( "\n%5s%11s%12s%8s%7s%7s%7s  %-9s\n", catgets(catd, 1, 5, "Vol"), k ? catgets(catd, 1, 6, "1K-Blks") : catgets(catd, 1, 7, "512-Blks"), catgets(catd, 1, 8, "Free"),
        catgets(catd, 1, 9, "% Used"), catgets(catd, 1, 10, "Cmode"), catgets(catd, 1, 11, "Rblks"), catgets(catd, 1, 12, "Wblks"), catgets(catd, 1, 13, "Vol Name") );

    volError = FALSE;

    for (vol = 0; vol < vols; vol++) {
        sts = advfs_get_vol_params( dmnParams.bfDomainId, 
                                   volIndexArray[vol], 
                                   &volInfo, &volProperties,
                                   &volIoQParams, &volCounters );

        if (sts != EOK) {
	    if (sts != EBAD_VDI) {
                fprintf( stderr, catgets(catd, 1, 14, "%s: get vol params error %s\n"), 
                     Prog, BSERRMSG( sts ) );
                volError = TRUE;
	    }
            else {
                /* Try again to get info which doesn't require reading
                   the bmt.  If the vol is being removed (so the bmt is
                   unavailable), the service class will be nil.
                */
                sts = advfs_get_vol_params( dmnParams.bfDomainId, 
                                           volIndexArray[vol], 
                                           0, &volProperties,
                                           &volIoQParams, &volCounters );
                if (sts == EOK && volProperties.serviceClass == 0) {
                    fprintf( stderr, "%38s", catgets(catd, 1, 24, "-data unavailable-") );
                    printf( "%5s", volIoQParams.consolidate ? catgets(catd, 1, 15, "on") : catgets(catd, 1, 16, "off") );
                    printf( "%7d", volIoQParams.rdMaxIo );
                    printf( "%7d", volIoQParams.wrMaxIo );
                    printf( "  %-s\n", volProperties.volName );
                    total = 0;
                }
                else if (sts != EBAD_VDI) {
                    fprintf( stderr, catgets(catd, 1, 14, "%s: get vol params error %s\n"), 
                         Prog, BSERRMSG( sts ) );
                    volError = TRUE;
                }
            }
        } else {
            /* printf( "    " ); */
            if (volInfo.volIndex == logVolIndex) {
                printf( "%4uL ", volInfo.volIndex );
            } else {
                printf( "%4u  ", volInfo.volIndex );
            }
            if (k) {
                printf( "%10u  ", volInfo.volSize / 2 );
                printf( "%10u  ", volInfo.freeClusters * volInfo.stgCluster / 2 );
            } else {
                printf( "%10u  ", volInfo.volSize );
                printf( "%10u  ", volInfo.freeClusters * volInfo.stgCluster );
            }
            printf( "%5.0f%%  ", 100.0 - 
                    ((double) (volInfo.freeClusters * volInfo.stgCluster) / 
                     (double) volInfo.volSize * 100.0) );
            printf( "%5s", volIoQParams.consolidate ? catgets(catd, 1, 15, "on") : catgets(catd, 1, 16, "off") );
            printf( "%7d", volIoQParams.rdMaxIo );
            printf( "%7d", volIoQParams.wrMaxIo );
            printf( "  %-s\n", volProperties.volName );
            totalSpace += volInfo.volSize;
            totalFree += (volInfo.freeClusters * volInfo.stgCluster);
        }
    }

    if (vols > 1 && total && (volError == FALSE)) {
        printf( "      ----------  ----------  ------\n" );
        printf( "      " );
        if (k) {
            printf( "%10lu  ", totalSpace / 2 );
            printf( "%10lu  ", totalFree / 2 );
        } else {
            printf( "%10lu  ", totalSpace );
            printf( "%10lu  ", totalFree );
        }
        printf( "%5.0f%%\n", 100.0 - 
                ((double) totalFree / (double) totalSpace * 100.0) );
    }

    if (volError)
        return 1;
    else
        return 0;
}



int
get_dmn_metadata_size(
    char *dmnName,                /* in */
    mlDmnParamsT *dmnParams,      /* in */
    u32T *dmnMetaTotal		  /* out */    
    )
{
    u32T setIdx;
    mlBfSetIdT bfSetId;
    mlBfTagT bfTag;
    uid_t uid;
    gid_t gid;
    off_t size;
    time_t atime;
    mode_t mode;
    mlBfSetParamsT setParams;
    u32T userId;
    mlStatusT sts;
    int err, logVolIndex, vols;
    int mountCnt, unmounted = 0, i, ii;
    struct stat stBuf;
    struct statfs *mountTbl = NULL;
    char strBuf[MAXPATHLEN], dirName[MAXPATHLEN];
    char *fromDomain, *fromSet;
    char buf[BUF_SZ];
    u32T fsMetaTotal = 0, *volIndexArray;
    mlVolInfoT volInfo;
    mlVolPropertiesT volProperties;
    mlVolIoQParamsT volIoQParams;
    mlVolCountersT volCounters;
    

    setIdx = 0;            /* start with first file set */

    mountCnt = getfsstat (0, 0, MNT_NOWAIT);
    if (mountCnt <= 0) {
        fprintf (stderr, catgets(catd, 1, 26, "%s: getfsstat failed.\n"), Prog);
        return 1;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 27, "%s: can't malloc file system mount table.\n"),
		 Prog);
        return 1;
    }

    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 26, "%s: getfsstat failed.\n"), Prog);
	perror(Prog);
        return 1;
    }
        
    logVolIndex = ML_BFTAG_VDI( dmnParams->ftxLogTag, dmnParams->dmnVersion );    

    volIndexArray = (u32T*) malloc( dmnParams->curNumVols * sizeof(u32T) );
    if (volIndexArray == NULL) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: out of memory\n"), Prog );
        return 1;
    }

    sts = msfs_get_dmn_vol_list( dmnParams->bfDomainId, 
                                 dmnParams->curNumVols, 
                                 volIndexArray, 
                                 &vols );
    
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: get dmn vol list error %s\n"), 
                 Prog, BSERRMSG( sts ) );
        free( volIndexArray );
        return 1;
    }

    /*
     * Convert domain name to domain path name, then do stats on
     * all reserved files for each volume. 
     */
                                                                                                  
    for (i = 0; i < mountCnt; i++) {
    
	strcpy(strBuf, mountTbl[i].f_mntfromname);
	fromDomain = strtok(strBuf, "# ");
	
	/*
	 * Only do stat operations on the given domain
	 */
	 
        if (mountTbl[i].f_type == MOUNT_MSFS && !strcmp(dmnName, fromDomain) ) {
      
	    strcpy(dirName, mountTbl[i].f_mntonname);
		                          
            if (strcmp(mountTbl[i].f_mntonname, "/") == 0 ) {
                strcpy(dirName,"\0");
            } else {
                strcpy(dirName, mountTbl[i].f_mntonname);
            }
		    
	    strcat(dirName, "/.tags/M-");
	    
	    /*
	     * Get stats for each reserved file on each volume
	     */
	                             
            for (ii = 0; ii < dmnParams->curNumVols; ii++) {

        	sts = advfs_get_vol_params( dmnParams->bfDomainId, 
                                   	    volIndexArray[ii], 
                                   	    &volInfo, &volProperties,
                                   	    &volIoQParams, &volCounters );

        	if (sts != EOK) {	    	    
                    fprintf( stderr, catgets(catd, 1, 14, "%s: get vol params error %s\n"), 
                         Prog, BSERRMSG( sts ) );
                    return 1;
	        }

                strcpy(strBuf, dirName);
		snprintf(buf, sizeof(buf), "%d",
                                            volIndexArray[ii] * 6 + BFM_RBMT);
                strcat(strBuf, buf);

                if ( stat(strBuf, &stBuf) != 0) {
		    perror(Prog);
                    return 1;
                }
                *dmnMetaTotal += stBuf.st_blocks;

                strcpy(strBuf, dirName);
                snprintf(buf, sizeof(buf), "%d",
                                            volIndexArray[ii] * 6 + BFM_SBM);
                strcat(strBuf, buf);

                if ( stat(strBuf, &stBuf) != 0) {
		    perror(Prog);
                    return 1;
                }
                *dmnMetaTotal += stBuf.st_blocks;
                       
		/*
		 * Skip transaction log size, and root tag size.  This is consistent 
		 * with old vdf accounting, it is actually wrong if the log file has 
		 * been moved.  The old log and tag files are not removed.
		 */
                    
            	if (volInfo.volIndex == logVolIndex) {
                      
                    strcpy(strBuf, dirName);
                    snprintf(buf, sizeof(buf), "%d",
                                         volIndexArray[ii] * 6 + BFM_FTXLOG);
                    strcat(strBuf, buf);

                    if ( stat(strBuf, &stBuf) != 0) {
			perror(Prog);
                        return 1;
                    }
                    *dmnMetaTotal += stBuf.st_blocks;

                    strcpy(strBuf, dirName);
                    snprintf(buf, sizeof(buf), "%d",
                                         volIndexArray[ii] * 6 + BFM_BFSDIR);
                    strcat(strBuf, buf);

                    if ( stat(strBuf, &stBuf) != 0) {
			perror(Prog);
                        return 1;
                    }
                    *dmnMetaTotal += stBuf.st_blocks;

		} /* endif log file volume */
		      
		/*
                 *  There is no -(6*i +4) reserved tags in version 3 
		 */
		                                           
                if (dmnParams->dmnVersion >= FIRST_RBMT_VERSION) {
                           
                    strcpy(strBuf, dirName);
                    snprintf(buf, sizeof(buf), "%d",
                                         volIndexArray[ii] * 6 + BFM_BMT);
                    strcat(strBuf, buf);

                    if ( stat(strBuf, &stBuf) != 0) {
			perror(Prog);
                        return 1;
                    }
                    *dmnMetaTotal += stBuf.st_blocks;

                } /* endif RBMT present */
                                                
                strcpy(strBuf, dirName);
                snprintf(buf, sizeof(buf), "%d",
                                     volIndexArray[ii] * 6 + BFM_MISC);
                strcat(strBuf, buf);

                if ( stat(strBuf, &stBuf) != 0) {
		    perror(Prog);
                    return 1;
                }
                *dmnMetaTotal += stBuf.st_blocks;
                            
            }  /* end volume loop */                   
            break;                                       

        } /* endif domain type && name compare */
    }  /* end mount table search loop */
        
    do {

       /*
        * This routine gets the set params for the next set.
        * When 'setIdx' is zero it starts with the first set.
        * Each time the routine is called 'setIdx' is updated
        * to 'point' to the next set in the domain (the
        * update is done by msfs_fset_get_info()).
        */

        sts = msfs_fset_get_info( dmnName, &setIdx, &setParams, &userId, 0);

        unmounted = 1;

	if ( sts == EOK) {
	 
	    /*
	     * Search the mount table to convert domain name and fileset name
	     * to a mount path.
	     */
	
            for (i = 0; i < mountCnt; i++) {
                
		strcpy(strBuf, mountTbl[i].f_mntfromname);
		fromDomain = strtok(strBuf, "# ");
                fsMetaTotal = 0;
                fromSet = strtok((char *) NULL, "\n");
		
		/*
		 * Process the entry that corresponds to the domain and fileset name
		 */
		 
                if (mountTbl[i].f_type == MOUNT_MSFS && !strcmp(dmnName, fromDomain) &&
		    !strcmp(setParams.setName, fromSet)) {
		    if (strcmp(mountTbl[i].f_mntonname, "/") == 0 ) {
			strcpy(dirName,"\0");
		    } else {
			strcpy(dirName,mountTbl[i].f_mntonname);
		    }
			   
		    if ((setParams.cloneId) == 0) {
		        /*
			 * This is a real fileset, not a clone
			 *     Skipping this for clones is wrong, but is done
			 *     for consistency with original VDF output, this 
			 *     should be corrected when it is better understood
			 */
			 
			/*
			 * Get fileset's frag file overhead structure size
			 */
			        
			strcpy(strBuf, dirName);
                        strcat(strBuf,"/.tags/1");
			fsMetaTotal += 2 * get_frag_overhead(strBuf);
			
			/*
			 * Get fileset's quota.user file size
			 */
			       
			snprintf(strBuf, sizeof(strBuf), "%s/.tags/%d", dirName, 
			 			USER_QUOTA_FILE_TAG);
                        if ( stat(strBuf, &stBuf) != 0) {
			    perror(Prog);
                            return 1;
                        }
		       	fsMetaTotal += stBuf.st_blocks;

			/*
			 * Get fileset's quota.group file size
			 */		       	       

			snprintf(strBuf, sizeof(strBuf), "%s/.tags/%d", dirName, 
						GROUP_QUOTA_FILE_TAG);	
                        if ( stat(strBuf, &stBuf) != 0) {
			    perror(Prog);
                            return 1;
                        }
		       	fsMetaTotal += stBuf.st_blocks;
                               
		    }  /* endif for not a clone */

		    /*
		     * Get fileset's tag directory size
		     */

                    snprintf(strBuf, sizeof(strBuf), "%s/.tags/M%d", dirName,
                    			 setParams.bfSetId.dirTag.num);
                    if ( stat(strBuf, &stBuf) != 0) {
			perror(Prog);
                        return 1;
                    }
		    fsMetaTotal += stBuf.st_blocks;
		    unmounted = 0;
		    break;
		    
                }  /* end compare mount table from name with domain and fileset */
            }  /* end mountbl for loop */
	            
	    *dmnMetaTotal += fsMetaTotal;
      	}  /* endif fset_get_info sts == OK  */ 

        if (unmounted != 0 && sts == EOK) {
    	    fprintf(stderr, catgets(catd, 1, 50,"All filesets in domain must be mounted\n"));
            return ( 1 );
        } 
	     
    } while (sts == EOK);  /* end loop on all file sets */ 
            
    if (sts != E_NO_MORE_SETS) {
        fprintf( stderr, catgets(catd, 1, 31, "%s: can't show set info for domain '%s'\n"), Prog, dmnName );
        fprintf (stderr, catgets(catd, 1, 20, "%s: error = %s\n"), Prog, BSERRMSG (sts));
        return -1;
    }

    return 0;
}


int
get_frag_overhead(
    char *fragBf           
    )
{
    slotsPgT grppg;
    int waste, grps, maxFree, h, freeBlks, sts, tt, freeFrags, grpFType;
    int fd, pg, c, v = 0, fType = -1;    
    struct summary {
        int cnt;
        int badCnt;
        int freeFrags;
        int histogram[11];
    };
    struct stat stats;    
    struct summary grpSummary[8] = { 0 };
    void *xtntmap;
    long nextbyte, curbyte;
    fd = open( fragBf, O_RDONLY, 0 );
    if (fd < 0) {
        perror( catgets(catd, 1, 34, "open()") );
        exit( 1 );
    }

    if (fstat( fd, &stats )) {
        perror( catgets(catd, 1, 35, "fstat()") );
        exit( 1 );
    }

    xtntmap = advfs_xtntmap_open( fd, &sts, 0 );
    if (xtntmap == NULL) {
        printf( catgets(catd, 1, 36, "advfs_xtntmap_open() error; %s\n"), BSERRMSG( sts ) );
        return( 1 );
    }

    curbyte = -1;
    nextbyte = 0;

    advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte );

    pg = nextbyte / 8192;

    while ((read_pg( fd, pg, &grppg ) ) > 0) {
        sts = check_group( fd, pg, &grppg, fType, v, &freeFrags, &grpFType );
        if (sts > 0) {
            maxFree = FRAGS_PER_GRP( grpFType );
            grpSummary[ grpFType ].cnt++;
            grpSummary[ grpFType ].freeFrags += freeFrags;
            h = (int) ((double) freeFrags / (double) maxFree * 10.0);
            grpSummary[ grpFType ].histogram[ h ]++;


            if (sts == 2) {
                grpSummary[ grpFType ].badCnt++;
            }
        }

        curbyte = nextbyte + 16*8192 - 1;

        if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
            break;
        }
        
        pg = nextbyte / 8192;
    }

    freeBlks = 0;
    waste = 0;
    grps = 0;

    for (tt = 0; tt < 8; tt++) {
        int fragsPerGroup = FRAGS_PER_GRP( tt );

        grps += grpSummary[ tt ].cnt; 

        if (tt == 0) {
            freeBlks += grpSummary[ tt ].freeFrags;
        } else {
            waste += (127 - (fragsPerGroup) * (tt)) * grpSummary[ tt ].cnt;
            freeBlks += grpSummary[ tt ].freeFrags * (tt);
        }  
    }  

    advfs_xtntmap_close( xtntmap );
    close( fd );
    return (freeBlks + grps + waste);
}


int
read_pg(
    int fd,
    int pg,
    slotsPgT *pgp
    )
{
    off_t pos;

    pos = lseek( fd, pg * 8192L, SEEK_SET );
    if (pos < 0) {
        perror( catgets(catd, 1, 37, "lseek") );
        return pos;
    }

    return read( fd, pgp, 8192 );
}


int
check_group(
    int fd,
    int grpPg,
    slotsPgT *grpPgp,
    int fType,
    int v,
    int *grpFreeFrags,
    int *grpFragType
    )
{
    int sz, f, frag;
    grpHdrT *grpHdrp;
    slotsPgT fragPgBuf;
    bfFragT fragType;
    fragHdrT *fragHdrp;
    uint32T fragPg, fragSlot;
    uint32T nextFreeFrag;
    int freeFrags = 0;

    *grpFreeFrags = 0;

    grpHdrp = (grpHdrT *)grpPgp;

    if (grpHdrp->self != grpPg) {
        printf( catgets(catd, 1, 38, "invalid frag group, pg %d"), grpPg ); 
        return 0;
    }

    fragType = grpHdrp->fragType;

    *grpFragType = fragType;

    if ((fType != -1) && (fType != fragType)) {
        return 0;
    }

    if (fragType == 0) {
        *grpFreeFrags = FRAGS_PER_GRP( fragType );
        return 1;
    }

    freeFrags = 0;
    *grpFreeFrags = grpHdrp->freeFrags;

    if (grpHdrp->version == 0) {
        nextFreeFrag = grpHdrp->nextFreeFrag;

        while (nextFreeFrag != BF_FRAG_EOF) {
    
            freeFrags++;   
            fragPg   = FRAG2PG( nextFreeFrag );
            fragSlot = FRAG2SLOT( nextFreeFrag );
        
            if (read_pg( fd, fragPg, &fragPgBuf ) <= 0) {
                printf( catgets(catd, 1, 39, "read error: fragPg %d, errno %d\n"), fragPg, errno );
                break;
            }
    
            fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];
            nextFreeFrag = fragHdrp->nextFreeFrag;
    
        }
    } else {

        fragSlot = grpHdrp->freeList[ BF_FRAG_NEXT_FREE ];

        while (fragSlot != BF_FRAG_NULL) {

            freeFrags++;

            fragSlot = grpHdrp->freeList[ fragSlot ];

        }
    }

    if (freeFrags == grpHdrp->freeFrags) {
        return 1;
    }

    if (!v) {
        printf( catgets(catd, 1, 40, "\n--\ngroup pg = %6d, type = %d\n"), grpPg, fragType );
        printf( catgets(catd, 1, 41, "      nextFree = %5d, lastFree = %5d, numFree = %5d\n\n"),
                grpHdrp->nextFreeFrag, grpHdrp->lastFreeFrag, 
                grpHdrp->freeFrags);
    }

    printf( catgets(catd, 1, 42, "\n** inconsistency found **\n") );
    printf( catgets(catd, 1, 43, "** group header indicates %d free frags"), grpHdrp->freeFrags );
    printf( catgets(catd, 1, 44, ", free list has %d free frag(s)\n"), freeFrags );

    if (grpHdrp->version == 1) {
        return 2;
    }

    printf( catgets(catd, 1, 45, "** searching for additional free frags...\n") );

    freeFrags = 0;

    for (f = 0; f < FRAGS_PER_GRP( fragType ); f++) {
        frag     = grpPg * 8 + f * fragType + 1;
        fragPg   = FRAG2PG( frag );
        fragSlot = FRAG2SLOT(  frag );

        if (read_pg( fd, fragPg, &fragPgBuf ) <= 0) {
            printf( catgets(catd, 1, 46, "read error: fragPg %d, errno %d\n"), fragPg, errno );
            exit( 1 );
        }

        fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];

        if ((fragHdrp->self == frag) && (fragHdrp->fragType == fragType)) {
            freeFrags++;
            printf( catgets(catd, 1, 47, "\t** possible free frag: %5d, nextFree = %5d\n"),
                frag, fragHdrp->nextFreeFrag );
            if (!v) {
                printf( catgets(catd, 1, 48, "\t\tfrag = %5d, fragPg = %d, fragSlot = %d\n"), 
                        frag, fragPg, fragSlot );
            }
        }
    }

    printf( catgets(catd, 1, 49, "** search found %d possible free frag(s)\n"), freeFrags );

    return 2;
}
