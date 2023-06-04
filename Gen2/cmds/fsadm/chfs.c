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
#include        <sys/types.h>
#include        <sys/stat.h>
#include        <advfs/advfs_syscalls.h>
#include        <advfs/bs_bitfile_sets.h>

#include        "common.h"

/* options list set flags */
#define BLKCLEAR               0x0001
#define NOBLKCLEAR             0x0002
#define THRESHOLDUPPERLIMIT    0x0004
#define THRESHLODUPPERINTERVAL 0x0008
#define THRESHOLDLOWERLIMIT    0x0010
#define THRESHOLDLOWERINTERVAL 0x0020

/* extern and macro definitions for errno */
extern char *sys_errlist[];
#define ERRMSG(x) sys_errlist[x]

static char *Prog = "fsadm chfs";

/* function declarations */
void chfs_usage(void);

int
chfs_main(int argc, char **argv)
{
    int c;
    int err;
    int Vflg = 0, oflg = 0;
    int thresholdUpperLimit    = 0;
    int thresholdUpperInterval = 0;
    int thresholdLowerLimit    = 0;
    int thresholdLowerInterval = 0;
    int fs_lock = -1;
    uint64_t setIdx = 0;             /* "default" fileset */
    uint32_t userId = 0;
    adv_status_t sts = -1;
    bfSetParamsT setParams;
    adv_bf_dmn_params_t dmnParams;
    arg_infoT* infop;
    extern int optind;
    extern char *optarg;
    uint32_t chfsOptions = 0;

    /* Must be privileged to run */
    check_root( Prog );

    while((c = getopt( argc, argv, "Vo:" )) != EOF) {
        switch(c) {
        case 'V':
            Vflg++;
            break;
        case 'o':
            oflg++;
            if( parse_options( optarg,
                               &chfsOptions,
                               &thresholdUpperLimit,
                               &thresholdUpperInterval,
                               &thresholdLowerLimit,
                               &thresholdLowerInterval ) == FALSE ) {
                return(1);
            }
            break;
        }
    }

    if((oflg < 1) || (Vflg > 1)) {
        chfs_usage();
        return(1);
    }

    if((argc - optind) < 1) {
        chfs_usage();
        return(1);
    }

    /* determine the fsname */
    infop = process_advfs_arg(argv[optind]);

    if(infop == NULL) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_NO_FS,
                        "%s: file system '%s' does not exist\n"),
                Prog, argv[optind]);

        return(1);
    }

    if(Vflg) {
        /* print verified command line */
        printf("%s", Voption(argc, argv));
        return(0);
    }
    else if(!oflg) {
        chfs_usage();
        return(1);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ONDISK,
                "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        return (1);
    }

    /* lock the filesystem */
    fs_lock = advfs_fspath_lock(infop->fspath, FALSE, FALSE, &err);
    if(fs_lock < 0) {
        if(err == EWOULDBLOCK) {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_FSBUSY,
                            "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"),
                    Prog);
            return(1);
        }
        else {
            fprintf(stderr,
                    catgets(catd,
                            MS_FSADM_COMMON,
                            COMMON_ERRLOCK,
                            "%s: Error locking '%s'; [%d] %s\n"),
                    Prog, infop->fspath, errno, ERRMSG(errno));
            return(1);
        }
    }

    sts = advfs_get_dmnname_params(infop->fsname, &dmnParams);
    if (sts != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_NOFSPARAMS,
                        "%s: Cannot get file system parameters for %s\n"),
                Prog,
                infop->fsname);
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_ERR,
                        "%s: Error = %s\n"),
                Prog,
                BSERRMSG(sts));
        return(1);
    }

    /* get the fileset information */
    sts = advfs_fset_get_info( infop->fsname, &setIdx, &setParams, &userId, 0 );
    if(sts != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_NOFSPARAMS,
                        "%s: Cannot get file system parameters for %s\n"),
                Prog, infop->fsname);
        return(1);
    }

    /* set the appropriate parameter */
    if( chfsOptions & BLKCLEAR ) {
        setParams.bfSetFlags |= BFS_OD_OBJ_SAFETY;
    }

    if( chfsOptions & NOBLKCLEAR ) {
        setParams.bfSetFlags &= ~BFS_OD_OBJ_SAFETY;
    }

    if((chfsOptions & THRESHOLDUPPERLIMIT) &&
       (chfsOptions & THRESHOLDLOWERLIMIT))  {

        if((thresholdLowerLimit > thresholdUpperLimit) &&
           (thresholdUpperLimit != 0)) {

           fprintf(stderr,
                    catgets(catd,
                    MS_FSADM_CHFS,
                    CHFS_THRESHOLDLIMITS_ERR,
                    "%s: Limits must be 0-100 and upper cannot be less than lower unless setting to 0, deactivating that threshold.\n"),
                    Prog);
            return (1); 
        }

        dmnParams.dmnThreshold.threshold_upper_limit = thresholdUpperLimit;
        dmnParams.dmnThreshold.threshold_lower_limit = thresholdLowerLimit;

    } else if(chfsOptions & THRESHOLDLOWERLIMIT) {
        if( (thresholdLowerLimit >
             dmnParams.dmnThreshold.threshold_upper_limit) &&
            (dmnParams.dmnThreshold.threshold_upper_limit != 0) ) {

           fprintf(stderr,
                    catgets(catd,
                    MS_FSADM_CHFS,
                    CHFS_THRESHOLDLIMITS_ERR,
                    "%s: Limits must be 0-100 and upper cannot be less than lower unless setting to 0, deactivating that threshold.\n"),
                    Prog);
            return (1);
        }

        dmnParams.dmnThreshold.threshold_lower_limit = thresholdLowerLimit;

    } else if(chfsOptions & THRESHOLDUPPERLIMIT) {
        if( (dmnParams.dmnThreshold.threshold_lower_limit >
             thresholdUpperLimit) &&
            (thresholdUpperLimit != 0) ) {

           fprintf(stderr,
                    catgets(catd,
                    MS_FSADM_CHFS,
                    CHFS_THRESHOLDLIMITS_ERR,
                    "%s: Limits must be 0-100 and upper cannot be less than lower unless setting to 0, deactivating that threshold.\n"),
                    Prog);
            return (1);
        }

       dmnParams.dmnThreshold.threshold_upper_limit = thresholdUpperLimit; 
    }

    if( chfsOptions & THRESHLODUPPERINTERVAL ) {

        if ((thresholdUpperInterval < 0) || (thresholdUpperInterval > 10080)) {
            fprintf(stderr,
                    catgets(catd,
                    MS_FSADM_CHFS,
                    CHFS_THRESHOLDUINTERVAL_ERR,
                    "%s: uinterval is out of range. Acceptable values are 0-10080.\n"),
                    Prog);
            return (1);
        }
        dmnParams.dmnThreshold.upper_event_interval = thresholdUpperInterval;
    }

    if( chfsOptions & THRESHOLDLOWERINTERVAL ) {

        if ((thresholdLowerInterval < 0) || (thresholdLowerInterval > 10080)) {
            fprintf(stderr,
                    catgets(catd,
                    MS_FSADM_CHFS,
                    CHFS_THRESHOLDLINTERVAL_ERR,
                    "%s: linterval is out of range. Acceptable values are 0-10080.\n"),
                    Prog);
            return (1);
        }
        dmnParams.dmnThreshold.lower_event_interval = thresholdLowerInterval;
    }

    /* set and activate the filesystem changes */
    sts = advfs_set_dmnname_params( infop->fsname, &dmnParams );
    if(sts != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_FSPARAMS_ERR,
                        "%s: Cannot set file system parameters for '%s'.\n"),
                "chfs", infop->fsname);
        return(1);
    }

    /* set and activate the fileset changes */
    sts = advfs_set_bfset_params_activate( infop->fsname, &setParams );
    if(sts != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_FSPARAMS_ERR,
                        "%s: Cannot set file system parameters for '%s'.\n"),
                Prog, infop->fsname);
        return(1);
    }

    /* unlock the filesystem */
    if(advfs_unlock(fs_lock, &err) == -1) {
        fprintf(stderr,
                catgets(catd,
                        MS_FSADM_COMMON,
                        COMMON_ERRUNLOCK,
                        "%s: Error unlocking '%s'; [%d] %s\n"),
                Prog, infop->fspath, errno, ERRMSG(errno));
        return(1);
    }

    return(0);
}

/*
 *      NAME:
 *              parse_options()
 *
 *      DESCRIPTION:
 *              parse_options takes the string of options provided by the user
 *              and sets the appropriate fsadm chio flags.
 * 
 *              parse_options returns TRUE for success and FALSE if an invalid
 *              argument is specified.
 *
 *      ARGUMENTS:
 *              options                (in)  options list from chfs_main()
 *              chfsOptions            (out) specified options
 *              thresholdUpperLimit    (out) thresholdUpperLimit value
 *              thresholdUpperInterval (out) thresholdUpperInterval value
 *              thresholdLowerLimit    (out) thresholdLowerLimit value
 *              thresholdLowerInterval (out) thresholdLowerInterval value
 *
 *      RETURN VALUES:
 *              success                 TRUE
 *              Failure                 FALSE
 *
 */

int
parse_options(
    char *options,              /* in  */
    unsigned int *chfsOptions,  /* out */
    int *thresholdUpperLimit,         /* out */
    int *thresholdUpperInterval,      /* out */
    int *thresholdLowerLimit,         /* out */
    int *thresholdLowerInterval       /* out */
    )
{
    char *optptr;
    char *optbuf;

    /*
     * add a <mumble>Flg variable for all options with 
     * restrictions on thier usage.
     */
    int blkclearFlg   = 0;
    int noblkclearFlg = 0;
    int ULimitFlg     = 0;
    int UIntervalFlg  = 0;
    int LLimitFlg     = 0;
    int LIntervalFlg  = 0;
    char *ep;  /* used for strtoul character to int conversion */

    optbuf = (char *) malloc(strlen(options)+1);
    if (!optbuf) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
                "%s: insufficient memory.\n"), Prog );
        return FALSE;
    }
    strcpy(optbuf, options);

    for( optptr = strtok( optbuf, "," ); optptr;
        optptr = strtok( (char *)NULL, "," ) ) {

        if(strncmp(optptr,
                   "ulimit=",
                   strlen("ulimit=")) == 0) {

            ULimitFlg++;
            *chfsOptions |= THRESHOLDUPPERLIMIT;
            optptr = strchr( optptr, '=' );
            *thresholdUpperLimit = strtoul( optptr+1, &ep, 10 );

            if ( ((*thresholdUpperLimit == 0) && (*(optptr+1) != '0')) ||
                 (*thresholdUpperLimit < 0) || (*thresholdUpperLimit > 100) ||
                 (*ep != '\0') ) {

                fprintf(stderr,
                        catgets(catd,
                        MS_FSADM_CHFS,
                        CHFS_THRESHOLDULIMIT_ERR,
                        "%s: ulimit is out of range. Acceptable values are 0-100.\n"),
                        Prog);
                return FALSE;
            }

            /* 
             * Setting default upper interval to 5 minutes.  
             * Default upper interval value will be overridden 
             * by a user passed in value.
             */
            *chfsOptions |= THRESHLODUPPERINTERVAL;

            if (*thresholdUpperLimit > 0) {
                *thresholdUpperInterval = 5;
            } else {
                *thresholdUpperInterval = 0;
            }

        } else if (strncmp(optptr,
                           "llimit=",
                           strlen("llimit=")) == 0 ) {

            LLimitFlg++;
            *chfsOptions |= THRESHOLDLOWERLIMIT;
            optptr = strchr( optptr, '=' );
            *thresholdLowerLimit = strtoul( optptr+1, &ep, 10 );

            if ( ((*thresholdLowerLimit == 0) && (*(optptr+1) != '0')) ||
                 (*thresholdLowerLimit < 0) || (*thresholdLowerLimit > 100) ||
                 (*ep != '\0') ) {

                fprintf(stderr,
                        catgets(catd,
                        MS_FSADM_CHFS,
                        CHFS_THRESHOLDLLIMIT_ERR,
                        "%s: llimit is out of range. Acceptable values are 0-100.\n"),
                        Prog);
                return FALSE;
            }

            /* 
             * Setting default lower interval to 5 minutes.
             * Default lower interval value will be overridden
             * by a user passed in value.
             */
            *chfsOptions |= THRESHOLDLOWERINTERVAL;

            if (*thresholdLowerLimit > 0) {
                *thresholdLowerInterval = 5;
            } else {
                *thresholdLowerInterval = 0;
            }

        } else if (strncmp(optptr,
                           "uinterval=",
                           strlen("uinterval=")) == 0 ) {

            UIntervalFlg++; 
            *chfsOptions |= THRESHLODUPPERINTERVAL;
            optptr = strchr( optptr, '=' );
            *thresholdUpperInterval = strtoul( optptr+1, &ep, 10 );

            if ( ((*thresholdUpperInterval == 0) && (*(optptr+1) != '0')) ||
                 (*thresholdUpperInterval < 0) ||
                 (*ep != '\0') ) {

                fprintf(stderr,
                        catgets(catd,
                        MS_FSADM_CHFS,
                        CHFS_THRESHOLDUINTERVAL_ERR,
                        "%s: uinterval is out of range. Acceptable values are 0-10080.\n"),
                        Prog);
                return FALSE;
            }

        } else if (strncmp(optptr,
                           "linterval=",
                           strlen("linterval=")) == 0 ) {

            LIntervalFlg++;
            *chfsOptions |= THRESHOLDLOWERINTERVAL;
            optptr = strchr( optptr, '=' );
            *thresholdLowerInterval = strtoul( optptr+1, &ep, 10 );

            if ( ((*thresholdLowerInterval == 0) && (*(optptr+1) != '0')) ||
                 (*thresholdLowerInterval < 0) ||
                 (*ep != '\0') ) {

                fprintf(stderr,
                        catgets(catd,
                        MS_FSADM_CHFS,
                        CHFS_THRESHOLDLINTERVAL_ERR,
                        "%s: linterval is out of range. Acceptable values are 0-10080.\n"),
                        Prog);
                return FALSE;
            }

        } else if (strcmp(optptr, "blkclear") == 0 ) {

            blkclearFlg++;
            *chfsOptions |= BLKCLEAR;

        } else if (strcmp(optptr, "noblkclear") == 0 ) {

            noblkclearFlg++;
            *chfsOptions |= NOBLKCLEAR;

        } else {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_BADOARG,
                "%s: unknown -o argument '%s'.\n"), Prog, optptr );
            free(optbuf);
            return FALSE;
        }
    }

    free(optbuf);

    if ( ( (ULimitFlg     > 1) ||
           (LLimitFlg     > 1) ||
           (UIntervalFlg  > 1) ||
           (LIntervalFlg  > 1) ||
           (blkclearFlg   > 1) ||
           (noblkclearFlg > 1) )  ||

         ( (blkclearFlg == 1) && (noblkclearFlg == 1)) ) {

        fprintf( stderr, catgets(catd, MS_FSADM_CHFS, CHFS_TOOMANYARG,
                 "%s: invalid option list.\n"),
                 Prog);

        return FALSE;
    }

    return TRUE;
}


void
chfs_usage(void)
{
    usage(catgets(catd,
                  MS_FSADM_CHFS,
                  CHFS_USAGE,
                  "%s [-V] -o option_list { special | fsname }\n"),
          Prog);
}
