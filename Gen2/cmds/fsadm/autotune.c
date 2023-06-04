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
#include <stdarg.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/spinlock.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/vfast.h>
#include <advfs/advfs_evm.h>
#include <locale.h>
#include <advfs/bs_ods.h>
#include <string.h>
#include <strings.h>

#include <sys/versw.h>

#include "autotune.h"
#include "vods.h"
#include "common.h"

static char *Prog = "fsadm autotune";

/*
 *	NAME:
 *		update_free_space_date()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		clusterSize
 *		inHole
 *		currentHoleSize
 *		freeSpaceData
 *
 *	RETURN VALUES:
 *		none
 */

static
void
update_free_space_data (
    int clusterSize,
    int *inHole,
    int *currentHoleSize,
    volFreeSpaceDataT *freeSpaceData
    )
{
    int range;
    unsigned long currentHoleSizeInCluster = *currentHoleSize;

    /* End of the current hole. */
    if (freeSpaceData != NULL) {
        unsigned long currentHoleSizeKb =
                        (currentHoleSizeInCluster *
                         clusterSize * DEV_BSIZE) / 1024;
        for (range = 0; range < FREE_RANGE_COUNT; range++) {
            if (freeRangeTop[range] == 0 ||
                currentHoleSizeKb < freeRangeTop[range]) {
                freeSpaceData->rangeData[range].holeCount++;
                freeSpaceData->rangeData[range].freeSpace += currentHoleSizeKb;
                freeSpaceData->totalHoleCount++;
                freeSpaceData->totalFreeSpace += currentHoleSizeKb;
                break;
            }
        }
    }
    *inHole = FALSE;
    *currentHoleSize = 0;

} /* end update_free_space_data */


/*
 *	NAME:
 *		open_fs()
 *
 *	DESCRIPTION:
 *		retrieve the file system param information and mount bitfiles,
 *		set up structures to hold the volume information.
 *
 *	ARGUMENTS:
 *		retvolData	out	volume data
 *		fsParams	in	file systems parameters
 *
 *	RETURN VALUES:
 *		success
 *		failure
 */

static
int 
open_fs (
    volDataT **retvolData,		/* out */
    adv_bf_dmn_params_t *fsParams	/* in/out */
    )
{
    int i, vols, max_vdi=0;
    adv_status_t sts = EOK;
    uint32_t *volIndexArray = NULL;              /* local use */
    volDataT *volData = NULL;                    /* local use */

    /* Get file system params and lock the file system */
    prep_filesys( fsParams, &vols, TRUE );

    volIndexArray = (uint32_t *)malloc(fsParams->curNumVols * sizeof(uint32_t));
    if(volIndexArray == NULL) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM, 
                 "%s: insufficient memory.\n"), Prog);
        goto _error;
    }

    volData = (volDataT *)malloc(fsParams->curNumVols * sizeof(volDataT));
    if(volData == NULL) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM, 
                 "%s: insufficient memory.\n"), Prog);
        goto _error;
    }

    /* Get the file system's list of volumes. */
    sts = advfs_get_dmn_vol_list ( fsParams->bfDomainId, 
				  fsParams->curNumVols, 
				  volIndexArray,
				  &volIndexCnt );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSMOUNT, 
                 "%s: file system must be mounted\n"), Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSLIST, 
                 "%s: Cannot get file system's volume list\n"), Prog);
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR, 
                  "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }


    /* assign volume indices - without the "L" log designator */

    for (i = 0; i < volIndexCnt; i++) {
        volData[i].volIndex = volIndexArray[i];
        volData[i].volSize = 0;
        volData[i].volStgCluster = 0;
        volData[i].volFreeSize = 0;
        volData[i].freeSizeFract = 0;
        volData[i].totalHoleCount = 0;
        volData[i].extentCount = 0;
        volData[i].fileWithExtCount = 0;
        volData[i].wrMaxIo = 0;
        volData[i].totalIOCount = 0;
        volData[i].totalBestIOCount = 0;

	/* find the maximum volume index number - assign to max_vdi */
	if( volIndexArray[i] > max_vdi ) {
	    max_vdi = volIndexArray[i];
	}
    }

    /* Set up an array that enables us to, given a volume index, find the
     * corresponding subscript into the index array. */
    volSubscrArray = (int *)malloc((max_vdi + 1) * sizeof(int));
    if(volSubscrArray == NULL) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM, 
                 "%s: insufficient memory.\n"), Prog);
        goto _error;
    }

    /* initialize the array */
    for (i = 0; i <= max_vdi; i++) {
        volSubscrArray[i] = -1;
    }
    for (i = 0; i < volIndexCnt; i++) {
        volSubscrArray[volIndexArray[i]] = i;
    }

    /* assign structures to passed in pointer for use by calling program */
    *retvolData = volData;

    if(volIndexArray)    free(volIndexArray);

    return( fsParams->curNumVols );

_error:

    if(volIndexArray)    free(volIndexArray);
    if(volData)          free(volData);
    if(volSubscrArray)   free(volSubscrArray);

    abort_prog( catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
			"%s: open of %s failed.\n"), 
                        Prog, infop->fspath);

} /* end open_fs */

/*
 *	NAME:
 *		set_ui_params()
 *
 *	DESCRIPTION:
 *		set values of UI Options, they have already been verified.
 *
 *	ARGUMENTS:
 *		ssDmnParams	in	file system params
 *		uiParams	in	options list
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

static
int
set_ui_params (
    adv_ss_dmn_params_t *ssDmnParams,
    char *uiParams
    )
{
    char *cp, *nextcp;
    char *nump;
    char *p;
    int num;

    cp = uiParams;
    while (cp != NULL && *cp != '\0') {
        if ((nextcp = index(cp, ',')) != NULL) {
            *nextcp++ = '\0';
        }
        if ((nump = index(cp, '=')) != NULL) {
            *nump++ = '\0';
	    num = strtol( nump, &p, 10 );
        } else {
	    return 1;
        }

	/*
	 * DEFRAG_ALL
	 */
        if (!strcmp(cp, "defrag_all")) {
	    ssDmnParams->ssDmnDefragAll = (strcmp(nump, "enable") ? 0 : 1);
#ifdef DEFRAGALL_EVENTS
	    if( ssDmnParams->ssDmnDefragAll == 1 ) {
		advfs_post_user_event( EVENT_SS_DEFRAGALL_ENABLED,
		    advfs_event, Prog );
	    } else {
		advfs_post_user_event( EVENT_SS_DEFRAGALL_DISABLED,
		    advfs_event, Prog );
	    }
#endif /* DEFRAGALL_EVENTS */

	/*
	 * DEFRAG_ACTIVE
	 */
        } else if (!strcmp(cp, "defrag_active")) {
	    ssDmnParams->ssDmnDefragment = (strcmp(nump, "enable") ? 0 : 1);
	    if( ssDmnParams->ssDmnDefragment == 1 ) {
		advfs_post_user_event(EVENT_SS_DEFRAG_ENABLED,
		    advfs_event, Prog );
	    } else {
		advfs_post_user_event( EVENT_SS_DEFRAG_DISABLED,
		    advfs_event, Prog );
	    }

	/*
	 * TOPIOBALANCE
	 */
        } else if (!strcmp(cp, "topIObalance")) {
	    ssDmnParams->ssDmnSmartPlace = (strcmp(nump, "enable") ? 0 : 1);
	    if( ssDmnParams->ssDmnSmartPlace == 1 ) {
		advfs_post_user_event( EVENT_SS_TOPIOBAL_ENABLED,
		    advfs_event, Prog );
	    } else {
		advfs_post_user_event( EVENT_SS_TOPIOBAL_DISABLED,
		    advfs_event, Prog );
	    }

	/*
	 * BALANCE
	 */
        } else if (!strcmp(cp, "balance")) {
	    ssDmnParams->ssDmnBalance = (strcmp(nump, "enable") ? 0 : 1);
	    if( ssDmnParams->ssDmnBalance == 1 ) {
		advfs_post_user_event( EVENT_SS_BAL_ENABLED,
		    advfs_event, Prog );
	    } else {
		advfs_post_user_event( EVENT_SS_BAL_DISABLED,
		    advfs_event, Prog );
	    }

	/*
	 * DIRECT_IO
	 */
        } else if (!strcmp(cp, "direct_io")) {
	    ssDmnParams->ssDmnDirectIo = (strcmp(nump, "enable") ? 0 : 1);

	/*
	 * PERCENT_IOS_WHEN_BUSY
	 */
        } else if (!strcmp(cp, "percent_ios_when_busy")) {
	    ssDmnParams->ssMaxPercentOfIoWhenBusy = num;

	/*
	 * SSACCESSTHRESHHITS
	 */
        } else if (!strcmp(cp, "ssAccessThreshHits")) {
	    ssDmnParams->ssAccessThreshHits = num;

	/*
	 * STEADY_STATE
	 */
        } else if (!strcmp(cp, "steady_state")) {
	    ssDmnParams->ssSteadyState = num;

	/*
	 * SSMINUTESINHOTLIST
	 */
        } else if (!strcmp(cp, "ssMinutesInHotList")) {
	    ssDmnParams->ssMinutesInHotList = num;

	/*
	 * UNKNOWN - Even though this should have been weeded out in the
	 * verify_options routine, leave it in for sanity's sake
	 */
        } else {
            fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_UNKNOWNOPT,
                     "%s: unknown option: %s\n"),
                     Prog, cp);
	    return 1;
        }

        cp = nextcp;
    } /* end while */

    return 0; 

} /* end of set_ui_params */


/*
 *	NAME:
 *		lock_fs()
 *
 *	DESCRIPTION:
 *		Lock global file system directory while locking infop->fsname
 *		file system.  Global lock is dropped after file system is locked
 *
 *	ARGUMENTS:
 *		none
 *
 *	RETURN VALUES:
 *		none
 */

static
void
lock_fs ( void )
{
    int32_t err;

    /* Lock the global AdvFS file system directory and the file system itself */
    FSLock = advfs_fspath_lock( infop->fspath, TRUE, FALSE, &err);
    if (FSLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSBUSY,
		"%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog );
        } else {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRLOCK,
                     "%s: Error locking '%s'; [%d] %s\n" ),
                     Prog, infop->fsname, err, BSERRMSG( err ) );
        }
        exit(1);
    }

} /* end of lock_fs */


/*
 *	NAME:
 *		prep_filesys()
 *
 *	DESCRIPTION:
 *		Get the file system params, lock the file system (if flagged)
 *		and set volume count
 *
 *	ARGUMENTS:
 *		fsParams	out	file system params
 *		vols		out	number of volumes
 *		flags		in	lock the file system or not
 *
 *	RETURN VALUES:
 *		none
 */

static
void
prep_filesys (
    adv_bf_dmn_params_t *fsParams,
    int *vols,
    int flags
    )
{
    uint32_t *volIndexArray;
    adv_status_t sts;

    if (flags == TRUE) {
        lock_fs();
    }

    sts = advfs_get_dmnname_params( infop->fsname, fsParams );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOFSINFO,
                 "%s: unable to get info for file system '%s'\n"),
                 Prog, infop->fsname);
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
                 "%s: Error = %s\n"), Prog, BSERRMSG(sts));
    }

    volIndexArray = (uint32_t*) malloc(fsParams->curNumVols * sizeof(uint32_t));
    if (volIndexArray == NULL) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
		"%s: insufficient memory.\n"), Prog);
        exit(1);
    }

    sts = advfs_get_dmn_vol_list(fsParams->bfDomainId, 
                                fsParams->curNumVols, 
                                volIndexArray, 
                                vols);

    if((sts != EOK) && (sts != ENO_SUCH_DOMAIN)) {
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSLIST,
		"%s: Cannot get file system's volume list\n"),
		Prog );
        free(volIndexArray);
        exit(1);
    }

} /* end of prep_filesys */

/*
 *	NAME:
 *		display_extents_list()
 *
 *	DESCRIPTION:
 *		display a list of the worst performing files in the file system 
 *
 *	ARGUMENTS:
 *		fsParams	in	file system parameters
 *
 *	RETURN VALUES:
 *		none
 */

static
void
display_extents_list (
    adv_bf_dmn_params_t fsParams
    )
{
    int num_vols=0;
    uint32_t fraglist_buf_size;
    adv_frag_desc_t *ssFragArray;
    bfSetParamsT bfSetParams;
    adv_status_t sts;
    int ssFragCnt, i, j;
    volDataT *volData = NULL;
    adv_vol_info_t volInfo;
    adv_vol_ioq_params_t volIoQParams;
    adv_vol_prop_t volProperties;
    adv_vol_counters_t volCounters;
    int unknown;
    int err;
    char current_path_name[MAXPATHLEN];

    num_vols = open_fs( &volData, &fsParams );
    if(!num_vols) {
        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOVOLS,
                "%s: Unable to open file system.\n"), Prog);
        exit(1);
    }

    fraglist_buf_size = num_vols * SS_INIT_FRAG_LST_SZ;
    ssFragArray = (adv_frag_desc_t *) malloc(
                                         sizeof(adv_frag_desc_t) * 
                                         fraglist_buf_size
                                        );
    if (ssFragArray == NULL) {
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMEM_FRGARRAY,
                 "%s: Cannot alloc memory for frag array\n"), Prog);
        exit(1);
    }

    err = open_dot_tags(infop, fsParams.bfSetDirTag, NULL, Prog);
    if (err) {
        fprintf(stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMOUNT,
                "%s: Cannot mount bitfile sets\n"), Prog);
         abort_prog(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
                    "%s: open of %s failed.\n"), 
                    Prog, infop->fsname);
    }
    UnmountFlag = TRUE;

    for (j=0; j<num_vols; j++) {
        sts = advfs_get_vol_params(
                   fsParams.bfDomainId,
                   volData[j].volIndex,
                   &volInfo,
                   &volProperties,
                   &volIoQParams,
                   &volCounters);

        /*  the volIndex can have holes in it.  Don't fail
         *  on EBAD_VDI, just continue.
         */
        if( ( sts != EOK ) && ( sts != EBAD_VDI ) ) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOVOLPARAMS,
		"%s: Cannot get volume parameters for %s\n"), Prog, volProperties.volName);
            fflush(stdout);
            exit(1);
        } else if( sts == EBAD_VDI ) {
	    continue;
	}

        sts = advfs_ss_get_fraglist(infop->fsname,
                              volData[j].volIndex, 
                              0, 
                              fraglist_buf_size, 
                              &(ssFragArray[0]), 
                              &ssFragCnt);

        if (sts != EOK) {
            fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOFRAGLIST,
                    "%s: Cannot get frag list; %s\n"),
                    Prog, BSERRMSG(sts));
            exit(1);
        }

        fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_VOL,
              "%s: Volume %d\n\n"), infop->fsname, volData[j].volIndex);
        fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_XTNTCNT,
              "extent\ncount    fileset/file\n\n"));
        if (ssFragCnt == 0) {
            fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOXTNTS,
		"    No extents\n"));
        }

        for (i=0; i<=ssFragCnt; i++) {
            if (BS_BFTAG_REG(ssFragArray[i].ssFragBfSetId.dirTag)) {
                sts = advfs_get_bfset_params(
                                            ssFragArray[i].ssFragBfSetId, 
                                            &bfSetParams
                                           );
                if (sts != EOK) {
                    fprintf(stderr,
			catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOBFPARAMS,
			"%s: unable to get bf set params\n"), Prog);
                }

                strcpy( current_path_name, get_tag_path(
                    NULL,
                    ssFragArray[i].ssFragTag, 
                    ssFragArray[i].ssFragBfSetId.dirTag,
                    &unknown) );

		fprintf(stdout, "%5d\t", ssFragArray[i].ssXtntCnt);
		fprintf(stdout, "%s: ", bfSetParams.setName);
		fprintf(stdout, "%s\n", current_path_name );
            } /* end if REG files */
        }
        fprintf(stdout, "\n");
    }

    (void)close_dot_tags(Prog);
    if( ssFragArray != NULL )  free( ssFragArray );

} /* end of display_extents_list */
                        
/*
 *	NAME:
 *		process_bmtcell()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		bsMCp
 *		cn
 *		volFileWithXtntCnt
 *		volXtntCnt
 *
 *	RETURN VALUES:
 *		none
 */
static
void
process_bmtcell(bsMCT *bsMCp,
             int cn,
             uint64_t *volFileWithXtntCnt,
             uint64_t *volXtntCnt)
{
    bsMRT *bsMRp;
    int rn;
    int i,cnt;
    void *pdata;
    uint64_t extentCount=0;
    uint64_t filesWithExtents=0;

    bsMRp = (bsMRT *)bsMCp->bsMR0;

    if ( BS_BFTAG_RSVD(bsMCp->mcTag) ) {
        /* dont count reserved files */
        return;
    }

    rn=0;
    while ( bsMRp->type != 0 && bsMRp < (bsMRT *)&bsMCp->bsMR0[BSC_R_SZ] ) {
        pdata = ((char *) bsMRp) + sizeof(bsMRT);

        if (( bsMRp->bCnt == 0)  ||
            ( (char *)bsMRp + bsMRp->bCnt > &bsMCp->bsMR0[BSC_R_SZ])) {
            /* Bad mcell record, skip this mcell */
            break;
        }

        rn++;
        bsMRp = (bsMRT*)((char*)bsMRp + roundup(bsMRp->bCnt,sizeof(int)));
    }

    *volFileWithXtntCnt += filesWithExtents;
    *volXtntCnt += extentCount;
}

/*
 *	NAME:
 *		process_bmtpage()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		bfp
 *		pgnum
 *		volFileWithXtntCnt
 *		volXtntCnt
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

static
int
process_bmtpage(
    bitFileT *bfp, 
    int pgnum, 
    uint64_t *volFileWithXtntCnt,
    uint64_t *volXtntCnt)
{
    int cn;
    bsMPgT *bsMPgp;
    int ret=0;

    if ( read_page(bfp, pgnum) != OK ) {
        return 1;
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;

    for ( cn = 0; cn < BSPG_CELLS; cn++) {
        process_bmtcell(&bsMPgp->bsMCA[cn], 
                              cn, 
                              volFileWithXtntCnt,
                              volXtntCnt);
    }

    return(0);
}


/*
 *	NAME:
 *		process_sbmpage()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		bfp
 *		page
 *		sbmLastPg
 *		sbmlastPgWds
 *		sbmlastPgBits
 *		clusterSize
 *		inHole
 *		currentHoleSize
 *		freeSpaceData
 *
 *	RETURN VALUES:
 *		success
 *		failure
 */

static
int
process_sbmpage(bitFileT *bfp, 
                int page,
                uint32_t sbmLastPg,
                uint32_t sbmlastPgWds,
                uint32_t sbmlastPgBits,
                uint32_t clusterSize,
                int *inHole,
                int *currentHoleSize, 
                volFreeSpaceDataT *freeSpaceData)
{
    int index;
    bsStgBmT *bsStgBmp=(bsStgBmT*)bfp->pgBuf;
    int nextMapByte;
    uint32_t wd,bit,wordCnt,bitCnt;
    uint32_t curWord ;

    if ( read_page(bfp, page) ) {
        return (1);
    }

    if (page == sbmLastPg) {
        wordCnt = sbmlastPgWds;
    } else {
        wordCnt = SBM_LONGS_PG;
    }

    /*
     * Process all the words in the page.
     */
    bitCnt = SBM_BITS_LONG;
    for (wd = 0; wd < wordCnt; (wd)++) {
        curWord = bsStgBmp->mapInt[wd];

        if((page == sbmLastPg) && (wd == (wordCnt-1))) 
            bitCnt = sbmlastPgBits;

        for (bit = 0; bit < bitCnt; bit++) {

            if((page==0) && (wd==0) && 
               (bit==0) && (!IS_INUSE(bit, curWord))) *inHole = TRUE;

            if (IS_INUSE( bit, curWord )) {
                if(*inHole) {
                    /* end of a hole */
                    update_free_space_data (
                            clusterSize,
                            inHole,
                            currentHoleSize, 
                            freeSpaceData);
                }

            }  else {   
                 /* Add a byte to the current hole (or start a new one). */
                 *inHole = TRUE;
                 (*currentHoleSize)++;
            }
        }
    }

    return 0;
}

/*
 *	NAME:
 *		find_bmt()
 *
 *	DESCRIPTION:
 *		Given bfp->pvol, this will load extents for the selected (R)BMT
 *
 *		Input: bfp->pvol - volume on which to find the BMT
 *		Input: bfp->dmn->vols[] filled in
 *		Output: fileTag, setTag (vol 1 defaults for FLE & vol == 0)
 *		OutPut: ppage, pcell
 *		Return: 0 if all went OK.
 *		Return: 1 if volume does not exist in file system.
 *
 *	ARGUMENTS:
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

static
int
find_bmt(bitFileT *bfp)
{
    bsVdAttrT *bsVdAttrp;
    vdIndexT vol = bfp->pmcell.volume;

    bfp->setTag.tag_num = -2;
    bfp->pmcell.page = 0;

    bfp->pmcell.cell = BFM_BMT;

    if ( bfp->pmcell.volume == 0 ) {
        /* we haven't selected a volume. default to vdi 1 */
        bfp->fileTag.tag_num = RSVD_TAGNUM_GEN(1, bfp->pmcell.cell);
    } else
        bfp->fileTag.tag_num = RSVD_TAGNUM_GEN(bfp->pmcell.volume, bfp->pmcell.cell);

    return(load_xtnts(bfp));
}

/*
 *	NAME:
 *		find_sbm
 *
 *	DESCRIPTION:
 *		Input: bfp->pvol - volume on which to find the SBM
 *		Input: bfp->dmn->vols[] filled in
 *
 *	ARGUMENTS:
 *		bfp
 *
 *	RETURN VALUES:
 *		success
 *		failure
 */

static
int
find_sbm(bitFileT *bfp)
{
    int i;
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    int mc;

    mc = BFM_SBM;

    bfp->pmcell.page = 0;
    bfp->pmcell.cell = mc;
    bfp->fileTag.tag_num = RSVD_TAGNUM_GEN(bfp->pmcell.volume, mc);
    bfp->setTag.tag_num = -2;

    if ( bfp->pmcell.volume == 0 ) {
        return (0);
    }

    if ( bfp->dmn->vols[bfp->pmcell.volume] ) {
        return load_xtnts(bfp);
    }

    return(1);
}


/*
 *	NAME:
 *		display_extents_summary()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		none
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

static
int
display_extents_summary ()
{
    adv_bf_dmn_params_t fsParams;
    volFreeSpaceDataT freeSpaceData;
    uint64_t volFileWithXtntCnt=0;
    uint64_t volXtntCnt=0;
    int range;
    adv_status_t sts;
    adv_vol_info_t volInfo;
    adv_vol_ioq_params_t volIoQParams;
    adv_vol_prop_t volProperties;
    adv_vol_counters_t volCounters;
    int err;
    bitFileT bf;
    bitFileT *bfp = &bf;
    int ret=0;
    char *name = NULL;
    uint32_t pagenum;
    int vdi;
    uint32_t clusterSize;
    int currentHoleSize;
    int inHole;
    uint32_t numClusters;
    uint32_t sbmLastPg,        /* number of whole pages in sbm */
            sbmlastPgWds,     /* number of whole words in last page */
            sbmlastPgBits;    /* number of bits in last word */


    sts = advfs_get_dmnname_params (infop->fsname, &fsParams);
    if (sts != EOK) {
        abort_prog ( catgets(catd, MS_FSADM_COMMON, COMMON_NOFSPARAMS, 
		"%s: Cannot get file system parameters for %s\n"),
		Prog, infop->fsname);
    }

    err = open_dot_tags(infop, fsParams.bfSetDirTag, NULL, Prog);
    if (err) {
        fprintf(stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMOUNT,
                "%s: Cannot mount bitfile sets\n"), Prog);
        abort_prog(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
                   "%s: open of %s failed.\n"), 
                   Prog, infop->fsname);
    }
    UnmountFlag = TRUE;

    init_bitfile(bfp);

    /* Set tag to reserved so that if it's a saved file we can check the */
    /* length in resolve_fle. Default to -6. */
    bfp->fileTag.tag_num = RSVD_TAGNUM_GEN(1, BFM_RBMT);

    name = infop->fsname;
    bfp->type = DMN;
    ret = resolve_name(bfp, name, 1);
    if ( ret != OK ) {
        fprintf(stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOACCESS,
              "%s: Unable to access file system %s\n"), Prog, infop->fsname);
        fflush(stdout);
	(void)close_dot_tags(Prog);
        return(ret);
    }

    /* initialize array */
    memset( &(freeSpaceData.rangeData), 0, sizeof( freeSpaceData.rangeData ) );

    bfp->type |= VOL;
    for ( vdi = 1; vdi < BS_MAX_VDI; vdi++ ) {

        if ( bfp->dmn->vols[vdi] == NULL ) 
            continue;
        else
            bfp->pmcell.volume = vdi;

        /* Get the extents info here */
        sts = advfs_get_vol_params(
                   fsParams.bfDomainId,
                   vdi,
                   &volInfo,
                   &volProperties,
                   &volIoQParams,
                   &volCounters);

        /*  the volIndex can have holes in it.  Don't fail
         *  on EBAD_VDI
         */
        if (sts != EOK && sts != EBAD_VDI) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOVOLPARAMS,
                "%s: Cannot get volume parameters for %s\n"),
		Prog );
            fflush(stdout);
	    (void)close_dot_tags(Prog);
            return(sts);
        }

        bfp->fd = bfp->dmn->vols[vdi]->fd;

        if ( find_bmt(bfp) != OK ) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_READERR1,
                "%s: Error reading from bitmap file: %s\n"), Prog, "BMT");
            fflush(stdout);
	    (void)close_dot_tags(Prog);
            return(ret);
        }

        for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
            ret = process_bmtpage(bfp, 
                                pagenum, 
                                &volFileWithXtntCnt,
                                &volXtntCnt);
            if(ret != 0) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_BMT_NOREAD,
                    "%s: Unable to read bmt page %ld on volume %d"),
                    Prog, pagenum, vdi);
                fflush(stdout);
		(void)close_dot_tags(Prog);
                return(ret);
            }
        }

        /* Get the free space info here */

        if ( (ret = find_sbm(bfp)) != 0 ) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_READERR1,
                "%s: Error reading from bitmap file: %s\n"), Prog, "SBM");
            fflush(stdout);
	    (void)close_dot_tags(Prog);
            return(ret);
        }

        /* get the sbm dimensions */
        numClusters = volInfo.volSize/volInfo.sbmBlksBit;
        sbmLastPg   = numClusters / SBM_BITS_PG;
        sbmlastPgWds  = numClusters / SBM_BITS_LONG - sbmLastPg * SBM_LONGS_PG;
        sbmlastPgBits = numClusters - sbmLastPg * SBM_BITS_PG -
                      sbmlastPgWds * SBM_BITS_LONG;

        currentHoleSize = 0;
        inHole=0;
        for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
            ret = process_sbmpage(bfp, 
                                  pagenum,
                                  sbmLastPg,
                                  sbmlastPgWds,
                                  sbmlastPgBits,
                                  volInfo.sbmBlksBit,
                                  &inHole,
                                  &currentHoleSize, 
                                  &freeSpaceData);
            if(ret != 0) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_SBM_NOREAD,
                    "%s: Unable to read sbm page %ld on volume %d"),
                    Prog, pagenum, vdi);
                fflush(stdout);
		(void)close_dot_tags(Prog);
                return(ret);
            }
        }

        if(inHole) {
            /* end of a volume, count this last hole*/
            update_free_space_data (
                        volInfo.sbmBlksBit,
                        &inHole,
                        &currentHoleSize, 
                        &freeSpaceData);
        }
    }

    fprintf(stdout, "%s\n\n", infop->fsname);
    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_XTNTS,
               "    Extents:              %11ld\n"), volXtntCnt);
    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FXTNTS,
               "    Files w/extents:      %11ld\n"), volFileWithXtntCnt);

    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_AVGXTNTS,
               "    Avg exts per file w/exts: %7.2f\n"), 
                    (volFileWithXtntCnt==0) ? 0.0 :
                    ((double)volXtntCnt / (double)volFileWithXtntCnt));

    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FREESPFRAG,
               "    Free space fragments:      %6d\n"), 
                              freeSpaceData.totalHoleCount);
    fprintf(stdout, "                  ");
    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_BANNER,
               "   <100K     <1M    <10M    >10M\n"));

    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FREESP,
	"      Free space:"));
    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        fprintf(stdout, "    %3.0f%%", 
            freeSpaceData.totalFreeSpace ? 
            (100 * freeSpaceData.rangeData[range].freeSpace /
                   (double)freeSpaceData.totalFreeSpace):0.0);
    }

    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FRAGS,
	"\n      Fragments: "));
    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        fprintf(stdout, " %7d", freeSpaceData.rangeData[range].holeCount );
    }
    fprintf(stdout, "\n");

    (void)close_dot_tags(Prog);

    exit(ret);

} /* end of display_extents_summary */


/*
 *	NAME:
 *		display_hotfiles_list()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		fsParams
 *
 *	RETURN VALUES:
 *		none
 */

static
void
display_hotfiles_list (
    adv_bf_dmn_params_t fsParams
    )
{
    uint32_t hotlist_buf_size;
    int ssHotCnt=0;
    int i,j;
    adv_ss_hot_desc_t *ssHotArray;
    bfSetParamsT bfSetParams;
    adv_status_t sts;
    int unknown;
    int err;
    volDataT *volData = NULL;
    uint32_t partial_list_cnt;
    int num_vols=0;

    num_vols = open_fs( &volData, &fsParams );

    if(!num_vols) {
        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOVOLS,
                "%s: Unable to open file system.\n"), Prog);
        exit(1);
    }

    hotlist_buf_size = num_vols * SS_INIT_HOT_LST_GROW_SZ;
    ssHotArray = (adv_ss_hot_desc_t *) malloc (sizeof(adv_ss_hot_desc_t) * 
                                        hotlist_buf_size
                                       );

    if (ssHotArray == NULL) {
        sts = ENO_MORE_MEMORY;
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMEM_HOTARRAY,
                 "%s: Cannot alloc memory for hot array\n"), Prog);
        exit(1);
    }

    sts = advfs_ss_get_hotlist (
              fsParams.bfDomainId, 
              0, 
              hotlist_buf_size, 
              &(ssHotArray[0]), 
              &ssHotCnt
              );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOHOTLIST,
                 "%s: Cannot get hot list; %s\n"),
                 Prog, BSERRMSG(sts));
        exit(1);
    }

    if (ssHotCnt == 0) {
	fprintf( stdout, catgets( catd, NL_AUTOTUNE, AUTOTUNE_NOHOTCNT,
	    "%s: there are no items in the hot list\n" ), Prog );
	exit( 0 );
    }

    err = open_dot_tags(infop, fsParams.bfSetDirTag, NULL, Prog);
    if (err) {
        fprintf(stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMOUNT,
                "%s: Cannot mount bitfile sets\n"), Prog);
        abort_prog(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
                   "%s: open of %s failed.\n"), 
                   Prog, infop->fsname);
    }
    UnmountFlag = TRUE;

    partial_list_cnt = ssHotCnt * SS_HOT_LST_CROSS_PCT / 100;

    if (partial_list_cnt < num_vols) {
        partial_list_cnt = num_vols;
    }

    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_PASTWEEK,
		"\n Past Week\n"));
    fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_IOCNT, 
           "  IO Count     Volume    File\n\n"));

    for (i=0,j=0; i<ssHotCnt; i++) {

	/* Put a line after SS_HOT_LST_CROSS_PCT percent of entries */
        if ((j== partial_list_cnt) && (j!=0)) {
            printf("   -----------------------------------------"
                   "----------------------\n");
        }

        j++;

        if (BS_BFTAG_REG(ssHotArray[i].ssHotBfSetId.dirTag)) {
            get_tag_path(
                NULL,
                ssHotArray[i].ssHotTag, 
                ssHotArray[i].ssHotBfSetId.dirTag,
                &unknown);
            
            if (unknown) {
                fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
                fprintf(stdout, "    %5d",
                        (ssHotArray[i].ssHotPlaced!=0) ?
                            (signed short)ssHotArray[i].ssHotPlaced :
                            (signed short)ssHotArray[i].ssHotVdi);
                fprintf(stdout, "  %s  ",
                        (ssHotArray[i].ssHotPlaced>0) ? "(x)" : "   ");
                fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_UNKNOWN, 
                        "<unknown> setTag: %ld.%ld (0x%lx.0x%lx), tag: %ld.%ld (0x%lx.0x%lx)\n"),
                        ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                        ssHotArray[i].ssHotBfSetId.dirTag.tag_seq,
                        ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                        ssHotArray[i].ssHotBfSetId.dirTag.tag_seq,
                        ssHotArray[i].ssHotTag.tag_num,
                        ssHotArray[i].ssHotTag.tag_seq,
                        ssHotArray[i].ssHotTag.tag_num,
                        ssHotArray[i].ssHotTag.tag_seq);
            } else if (!unknown) {
                sts = advfs_get_bfset_params(ssHotArray[i].ssHotBfSetId, 
                                            &bfSetParams);
                if (sts != EOK) {
                    fprintf (stderr,
			catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOBFPARAMS,
			"%s: unable to get bf set params\n"), Prog);
                }

                fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
                fprintf(stdout, "    %5d",
                        (ssHotArray[i].ssHotPlaced!=0) ?
                            (signed short)ssHotArray[i].ssHotPlaced :
                            (signed short)ssHotArray[i].ssHotVdi);
                fprintf(stdout, "  %s  ",  /* placed or not? */
                        (ssHotArray[i].ssHotPlaced>0) ? "(x)" : "   ");

                switch(ssHotArray[i].ssHotTag.tag_num) {
                    case 1:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAG,
				"tag = "));
                        fprintf(stdout, "%ld, %ld, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                                    ssHotArray[i].ssHotTag.tag_num);
                        fprintf(stdout,
				catgets(catd, NL_AUTOTUNE, AUTOTUNE_FRAG,
				"FRAG\n"));
                        continue;
                    case 2:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAG,
				"tag = "));
                        fprintf(stdout, "%ld, %ld, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                                    ssHotArray[i].ssHotTag.tag_num);
                        fprintf(stdout,
				catgets(catd, NL_AUTOTUNE, AUTOTUNE_ROOTDIR,
				"ROOT DIRECTORY\n"));
                        continue;
                    case 3:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAG,
				"tag = "));
                        fprintf(stdout, "%ld, %ld, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                                    ssHotArray[i].ssHotTag.tag_num);
                        fprintf(stdout,
				catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAGS,
				".tags\n"));
                        continue;
                    case 4:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAG,
				"tag = "));
                        fprintf(stdout, "%ld, %ld, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                                    ssHotArray[i].ssHotTag.tag_num);
                        fprintf(stdout,
				catgets(catd, NL_AUTOTUNE, AUTOTUNE_QUOTAS,
				"quotas\n"));
                        continue;
                    case 5:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TAG,
				"tag = "));
                        fprintf(stdout, "%ld, %ld, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                                    ssHotArray[i].ssHotTag.tag_num);
                        fprintf(stdout,
				catgets(catd, NL_AUTOTUNE, AUTOTUNE_GRPUSAGE,
				"group.usage\n"));
                        continue;
                    default:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, "%s\n",
                                        get_tag_path(
                                            NULL,
                                            ssHotArray[i].ssHotTag, 
                                            ssHotArray[i].ssHotBfSetId.dirTag,
                                            &unknown));
                }
            }

        } else if (BS_BFTAG_RSVD(ssHotArray[i].ssHotBfSetId.dirTag)) {
            fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
            fprintf(stdout, "    %5d %s  ",
                    (ssHotArray[i].ssHotPlaced==0) ?
                        (signed short)ssHotArray[i].ssHotVdi :
                        (signed short)ssHotArray[i].ssHotPlaced,
                    (ssHotArray[i].ssHotPlaced==0) ?
                        "   " : "(x)");

            if (!BS_BFTAG_RSVD(ssHotArray[i].ssHotTag))
                fprintf(stdout, " %s: ", bfSetParams.setName);

            fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_RESERVEDTAG, 
                         "*** a reserved file, tag = "));
            fprintf(stdout, "%ld, %ld, ", 
                        ssHotArray[i].ssHotBfSetId.dirTag.tag_num,
                        ssHotArray[i].ssHotTag.tag_num);

            if (BS_BFTAG_RSVD(ssHotArray[i].ssHotTag)) {
                fprintf(stdout, "%s\n",
                       (abs(ssHotArray[i].ssHotTag.tag_num) % 6 == 0) ? "RBMT" :
                       (abs(ssHotArray[i].ssHotTag.tag_num) % 6 == 1) ? "SBM" :
                       (abs(ssHotArray[i].ssHotTag.tag_num) % 6 == 2) ? "TAG" :
                       (abs(ssHotArray[i].ssHotTag.tag_num) % 6 == 3) ? "LOG" :
                       (abs(ssHotArray[i].ssHotTag.tag_num) % 6 == 4) ? "BMT" :
                       " ");
            } else {
                fprintf(stdout, "TAGDIR\n");
            }
        }

    } /* end of loop on ssHotCnt */

    (void)close_dot_tags(Prog);

    if( ssHotArray != NULL )  free( ssHotArray );

} /* end of display_hotfiles_list */

/*
 *	NAME:
 *		display_hotfiles_summary()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		fsParams
 *
 *	RETURN VALUES:
 *		none
 */

static
void
display_hotfiles_summary (
    adv_bf_dmn_params_t fsParams
    )
{
    uint32_t hotlist_buf_size;
    int ssHotCnt=0;
    int i, j, k;
    adv_ss_hot_desc_t *ssHotArray;
    adv_status_t sts;
    uint32_t totalHotCnt=0;
    float max, min, iopcnt;
    volDataT *volData = NULL;
    uint32_t partial_list_cnt;
    float partial_list_percentage = 0.0;
    int num_vols=0;
    typedef struct {
        int vdCnt;
        vdIndexT vdi;
    } hotDistT;
    hotDistT *hotDist;

    num_vols = open_fs ( &volData, &fsParams );

    if(!num_vols) {
        fprintf(stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOVOLS,
                "%s: Unable to open file system.\n"), Prog);
        exit(1);
    }

    hotDist = (hotDistT *) malloc(sizeof(hotDistT) * num_vols);
    if (hotDist == NULL) {
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMEM_HOTFILE,
                 "%s, Cannot alloc memory for hot file count array\n"),
                 Prog);
        exit(1);
    }

    hotlist_buf_size = num_vols * SS_INIT_HOT_LST_GROW_SZ;
    ssHotArray = (adv_ss_hot_desc_t *) malloc (sizeof(adv_ss_hot_desc_t) * 
                                        hotlist_buf_size
                                       );

    if (ssHotArray == NULL) {
        sts = ENO_MORE_MEMORY;
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOMEM_HOTARRAY,
                 "%s: Cannot alloc memory for hot array\n"), Prog);
        exit(1);
    }

    sts = advfs_ss_get_hotlist(fsParams.bfDomainId, 
                         0, 
                         hotlist_buf_size, 
                         &(ssHotArray[0]), 
                         &ssHotCnt);

    if (sts != EOK) {
        fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOHOTLIST,
                "%s: Cannot get hot list; %s\n"),
                Prog, BSERRMSG(sts));
        exit(1);
    }

    if ((partial_list_cnt=ssHotCnt*SS_HOT_LST_CROSS_PCT/100) < num_vols) {
        partial_list_cnt = num_vols;
    }

    if (ssHotCnt != 0)
        partial_list_percentage = partial_list_cnt * 100.0 / ssHotCnt;

    /* initalize the array */
    for (i=0; i<=num_vols; i++) {
        hotDist[i].vdCnt = 0;
        hotDist[i].vdi = volData[i].volIndex;
    }

    for (i=0; i<partial_list_cnt; i++) {
        for (k=0; k<num_vols; k++) {
            if ((signed short)ssHotArray[i].ssHotVdi > 0) {
                if (hotDist[k].vdi == ssHotArray[i].ssHotVdi) {
                    hotDist[k].vdCnt++;
                }
            }
        }
        totalHotCnt++;
    }

    if (totalHotCnt != 0) {
        max = (min = 100.0 * hotDist[0].vdCnt / totalHotCnt);
    } else {
	fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_NOHOTCNT,
                "%s: there are no items in the hot list\n" ), Prog );
        exit(1);
    }
    fprintf(stdout, "\n" );
    fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FS,
             "  File system:                           %s\n"), fsParams.domainName);
    fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_TOPHOTFILES,
             "\n\n         Top %2.0f%% of Hot Files\n"), partial_list_percentage);
    fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_DIST,
             "Volume   Distribution by File System Volume\n"));
    for (j=0; j<num_vols; j++) {
        iopcnt = 100.0 * hotDist[j].vdCnt / totalHotCnt;
        fprintf(stdout,
                "%6d         %4.2f%%\n",
                volData[j].volIndex,
                iopcnt);

        max = (max > iopcnt) ? max : iopcnt;
        min = (min <= iopcnt) ? min : iopcnt;
    }

    if (num_vols > 1) {
        fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_GREATDIFF, 
              "\n         greatest difference between storage devices: %4.2f%%\n"), (max-min));
    }

    if( ssHotArray != NULL )   free( ssHotArray );
    if( hotDist != NULL )   free( hotDist );

} /* end of display_hotfiles_summary */


/*
 *	NAME:
 *		request_type()
 *
 *	DESCRIPTION:
 *		Command types for fsadm autotune
 *
 *	ARGUMENTS:
 *		requestString
 *
 *	RETURN VALUES:
 *		success
 *		failure
 */

static
int 
request_type (
    const char *requestString
    )
{
    static struct {
        char *str;
        ssUIDmnOpsT opType;
    } request_types[] = {
        { "restart"     , SS_UI_RESTART    },
        { "stop"        , SS_UI_STOP       },
        { "activate"    , SS_UI_ACTIVATE   },
        { "deactivate"  , SS_UI_DEACTIVATE },
        { "suspend"     , SS_UI_SUSPEND    },
        { "status"      , SS_UI_STATUS     },
        { "showhidden"  , SS_UI_SHOWHIDDEN },
        { "reset"       , SS_UI_RESET }
    };
    int i;

    for(i=sizeof(request_types)/sizeof(request_types[0]); --i >= 0; ) {
        if(strcmp(request_types[i].str,requestString) == 0)
            return(request_types[i].opType);
    }

    /* illegal request types */
    return SS_NOOP;
}


/*
 *	NAME:
 *		abort_prog()
 *
 *	DESCRIPTION:
 *
 *	ARGUMENTS:
 *		msg
 *
 *	RETURN VALUES:
 *		none
 */

static
void
abort_prog(
    char *msg, ...
          )
{
  int err;
  va_list ap;

    if (UnmountFlag)
        if(close_dot_tags(Prog))
            fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_ERRUNMNT,
		"%s: Cannot unmount bitfile sets\n" ), Prog );

    if (FSLock != -1) {
        if( unlock_file (FSLock, &err) ) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRUNLOCK,
                     "%s: Error unlocking '%s'; [%d] %s\n"),
                     Prog, infop->fsname, err, BSERRMSG( err ) );
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_GEN_NUM_STR_ERR,
                     "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG (err));
         }
        FSLock = -1;
    }

    va_start( ap, msg );
    fprintf( stderr,
             "\n\n************* PROGRAM ABORT **************\n\n" );
    vfprintf( stderr, msg, ap );
    fprintf( stderr, "\n\n" );
    va_end( ap );

    fflush( stderr );
    program_exit_status = 3;
    exit(program_exit_status);
}

/*
 *	NAME:
 *		autotune_usage
 *
 *	DESCRIPTION:
 *		prints out the usage statement
 *
 *	ARGUMENTS:
 *		none
 *
 *	RETURN VALUES:
 *		none
 *
 */
void
autotune_usage( void )
{
    /*
     *  The following options are not documented:
     *
     *  showhidden		for debug purposes
     *  start and stop		for system startup and shutdown
     */

    usage( catgets( catd, NL_AUTOTUNE, AUTOTUNE_USAGE,
	"%s [-V] activate|deactivate|suspend|status special|fsname\n\t%s [-V] -l|-L extents|hotfiles special|fsname\n\t%s [-V] -o option_list special|fsname\n" ),
        Prog, Prog, Prog );
}

/*
 *	NAME:
 *		switches
 *
 *	DESCRIPTION:
 *		performs operations specified by command line switches
 *
 *	ARGUMENTS:
 *		switch_flag	Flag indicating which operation to perform
 *		options		string of options for -o
 *
 *	RETURN VALUES:
 *		0		Success
 *		1		Failure
 *
 */
static int
switches( switch_flag_t switch_flag, char *options )
{
    int vols;
    adv_bf_dmn_params_t fsParams;
    adv_ss_dmn_params_t ssDmnParams;
    adv_status_t sts;

    if( ( switch_flag & SWITCH_O ) &&
      ( ( switch_flag & SWITCH_LXTNT1 ) ||
	( switch_flag & SWITCH_LXTNT2 ) ||
	( switch_flag & SWITCH_LHOT1 )  ||
	( switch_flag & SWITCH_LHOT2 ) ) ) {
        autotune_usage();
	return 1;
    }

    if( switch_flag & SWITCH_O ) {
	prep_filesys( &fsParams, &vols, FALSE );

	init_event( &advfs_event );
	sts = advfs_ss_get_params( infop->fsname, &ssDmnParams );
	if (sts != EOK) {
	    fprintf( stderr, catgets( catd, MS_FSADM_COMMON, COMMON_NOFSPARAMS,
		     "%s: Cannot get file system parameters for %s\n"),
		     Prog, infop->fsname);
	    return 1;
	}

	if( set_ui_params( &ssDmnParams, options ) ) {
	    autotune_usage();
	    return 1;
	}

	sts = advfs_ss_set_params( infop->fsname, ssDmnParams );
	if( sts != EOK ) {
	    fprintf( stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR3,
		     "%s: Cannot set %s on file system %s; %s\n"),
		     Prog, options, infop->fsname, BSERRMSG(sts));
	    return 1;
	}

    } else if( ( switch_flag & SWITCH_LXTNT1 ) ||
	       ( switch_flag & SWITCH_LXTNT2 ) ||
	       ( switch_flag & SWITCH_LHOT1 ) ||
	       ( switch_flag & SWITCH_LHOT2 ) ) {
	prep_filesys( &fsParams, &vols, FALSE );

	if( switch_flag & SWITCH_LXTNT1 ) {
	    display_extents_list( fsParams );
	} else if( switch_flag & SWITCH_LXTNT2 ) {
	    display_extents_summary();
	} else if( switch_flag & SWITCH_LHOT1 ) {
	    display_hotfiles_list( fsParams );
	} else if( switch_flag & SWITCH_LHOT2 ) {
	    display_hotfiles_summary( fsParams );
	}
    } else {
	autotune_usage();
	return 1;
    }

    return 0;
}

/*
 *	NAME:
 *		verify_options
 *
 *	DESCRIPTION:
 *		Verifies that the options specified for -o switch are valid
 *
 *	ARGUMENTS:
 *		options		string of options for -o
 *
 *	RETURN VALUES:
 *		0		Success
 *		1		Failure
 *
 */
static int
verify_options( char *cp )
{
    char *nextcp;
    char *nump;
    char *p;
    int num=0;

    while( cp != NULL && *cp != '\0' ) {

	if( ( nextcp = index( cp, ',' ) ) != NULL ) {
	    *nextcp++ = '\0';
	}
	if( ( nump = index( cp, '=' ) ) != NULL ) {
	    *nump++ = '\0';
	    num = strtol( nump, &p, 10 );
	} else {
	    return 1;
	}

	/* options requiring enable or disable */
	if( !strcmp( cp, "defrag_all" ) || !strcmp( cp, "defrag_active" ) ||
	    !strcmp( cp, "balance" ) || !strcmp( cp, "topIObalance" ) ||
	    !strcmp( cp, "direct_io" ) ) {

	    if( strcmp( nump, "enable" ) &&  strcmp( nump, "disable" ) ) {
		fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
		    "%s: bad value for %s option\n"), Prog, cp );
		return 1;
	    }

	/* options requiring numerical value */
	} else if( !strcmp( cp, "ssMinutesInHotList" ) || 
	           !strcmp( cp, "ssAccessThreshHits" ) ||
		   !strcmp( cp, "percent_ios_when_busy" ) ||
		   !strcmp( cp, "steady_state" ) ) {

	    /* make sure it's a number */
            if( p != nump + strlen( nump ) ) {
                fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
                    "%s: bad value for %s option\n" ), Prog, cp );
                return 1;
            }

	    /* check range for ssMinutesInHotList */
	    if( !strcmp( cp, "ssMinutesInHotList" ) &&
		( ( num > SS_MAX_MINUTES_HOT_LST ) ||
		  ( num < SS_MIN_MINUTES_HOT_LST ) ) ) {
                fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
                    "%s: bad value for %s option\n" ), Prog, cp );
                return 1;
            }

	    /* check range for ssAccessThreshHits */
	    if( !strcmp( cp, "ssAccessThreshHits" ) &&
		( ( num > SS_MAX_FILE_ACC_THRESH ) ||
		  ( num < SS_MIN_FILE_ACC_THRESH ) ) ) {
                fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
                    "%s: bad value for %s option\n" ), Prog, cp );
		return 1;
            }

	    /* check range for percent_ios_when_busy */
	    if( !strcmp( cp, "percent_ios_when_busy" ) &&
		( ( num > SS_MAX_PCT_IOS_WHEN_BUSY ) ||
		  ( num < SS_MIN_PCT_IOS_WHEN_BUSY ) ) ) {
                fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
                    "%s: bad value for %s option\n" ), Prog, cp );
                return 1;
            }

	    /* check range for steady_state */
	    if( !strcmp( cp, "steady_state" ) &&
		( ( num > SS_MAX_STEADY_STATE ) ||
		  ( num < SS_MIN_STEADY_STATE ) ) ) {
                fprintf( stderr, catgets( catd, NL_AUTOTUNE, AUTOTUNE_BADOPT,
		    "%s: bad value for %s option\n" ), Prog, cp );
                return 1;
            }
		

	/* option not recognized */
	} else {
            fprintf( stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_UNKNOWNOPT,
		"%s: unknown option: %s\n" ), Prog, cp );
            return 1;
	}

	cp = nextcp;
    }

    return 0;
}

/*
 *	NAME:
 *		autotune_main()
 *
 *	DESCRIPTION:
 *		main routine for fsadm autotune command
 *
 *	ARGUMENTS:
 *		argc	number of command line arguments
 *		argv	command line arguments
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

int
autotune_main(int argc,char **argv)
{
    int c, vols, ssRunningState=0, err, ret=0;
    switch_flag_t switch_flag;
    char *options=NULL;
    ssUIDmnOpsT opType;
    extern int optind;
    extern char *optarg;
    adv_status_t sts;
    adv_bf_dmn_params_t fsParams;
    adv_ss_dmn_params_t ssDmnParams;
    struct timeval curr_time;
    u_int time_remaining;
    ssDmnOpT ssDmnCurrentState=0;
   
    switch_flag = SWITCH_NONE;

    check_root( Prog );

    if (VERSW() != VERSW_ENABLE) {
	fprintf(stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_VERSION,
	    "Entire cluster not up to same version: fsadm autotune is not supported\n"));
  	exit(1);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = advfs_check_on_disk_version( BFD_ODS_LAST_VERSION_MAJOR,
                                      BFD_ODS_LAST_VERSION_MINOR );
    if (sts != EOK) {
          fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ONDISK,
          "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
          return( 1 );
    }

    while( ( c = getopt( argc, argv, "Vl:L:o:" ) ) != EOF ) {
        switch( c ) {

            case 'V':
		switch_flag |= SWITCH_V;
		break;

            case 'l':
                if( strcmp( optarg, "extents" ) == 0 ) {
		    switch_flag |= SWITCH_LXTNT1;
                } else if( strcmp( optarg, "hotfiles" ) == 0 ) {
		    switch_flag |= SWITCH_LHOT1;
                } else {
                    autotune_usage();
		    return 1;
                }
		optind--;
		break;

            case 'L':
                if( strcmp( optarg, "extents" ) == 0 ) {
		    switch_flag |= SWITCH_LXTNT2;
                } else if( strcmp( optarg, "hotfiles" ) == 0 ) {
		    switch_flag |= SWITCH_LHOT2;
                } else {
                    autotune_usage();
		    return 1;
                }
		optind--;
		break;

            case 'o':
		switch_flag |= SWITCH_O;
                options = (char *)strdup( optarg );
		optind--;
		break;

            case '?':

	    default:
		autotune_usage();
		return 1;

	} /* end switch */
    } /* end while getopt */

    /* Check for non character options */
    opType = request_type( argv[optind] );

    /*
     * No switches were specified, and the opType could not be determined
     * or only the V switch was specified and the opType could not be determined
     */
    if( ( ( switch_flag == SWITCH_V ) || !switch_flag ) &&
	( opType == SS_NOOP ) ) {
	autotune_usage();
	return 1;
    }

    /* Get file system name and path */
    infop = process_advfs_arg( argv[optind+1] );
    if( infop == NULL ) {
	fprintf( stderr, catgets( catd, MS_FSADM_COMMON, COMMON_NO_FS,
	    "%s: file system '%s' does not exist\n" ), Prog, argv[optind+1] );
	return 1;
    }

    /* Set Event Domain */
    advfs_event.domain = infop->fsname;

    /* Perform operations specified by switches */
    if( switch_flag ) {

	char *cp;
	cp = strdup( options );

	/* Validate options for -o switch */
	ret = verify_options( cp );
	free( cp );

	if( ret ) {
	    autotune_usage();
	    goto _finish;
	}

	/* print command line and return zero if V switch specified */
	if( switch_flag & SWITCH_V ) {
	    printf( "%s", Voption( argc, argv ) );
	    goto _finish;
	}

	ret = switches( switch_flag, options );
	goto _finish;
    }

    if( opType == SS_UI_ACTIVATE ) {
	/* Lock the file system */
	prep_filesys( &fsParams, &vols, TRUE );
    }

    if( ( opType != SS_UI_SHOWHIDDEN ) &&
	( opType != SS_UI_RESET ) ) {
	sts = advfs_ss_dmn_ops( infop->fsname,
				opType,
				&ssDmnCurrentState,
				&ssRunningState );
    }

    switch( opType ) {

        case SS_UI_RESTART:
            if( sts != EOK ) {
                if( sts == E_DOMAIN_NOT_ACTIVATED ) {
                    fprintf (stderr,
			catgets(catd, NL_AUTOTUNE, AUTOTUNE_NOTINITED,
			"%s: At least one file system must be mounted to start fsadm autotune.\n"),
			Prog);
                } else {
                    fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR4,
                         "%s: Cannot restart on file system %s; %s\n"),
                         Prog, infop->fsname, BSERRMSG(sts));
		}
		ret = 1;
		goto _finish;
            }
            break;

        case SS_UI_STOP:
            if( sts != EOK ) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR5,
                         "%s: Cannot stop on file system %s; %s\n"),
                         Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            }
            break;

        case SS_UI_ACTIVATE:
            if( sts != EOK ) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR6,
                         "%s: Cannot activate on file system %s; %s\n"),
                         Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            }
            advfs_post_user_event( EVENT_SS_ACTIVATED, advfs_event, Prog );
            break;

        case SS_UI_DEACTIVATE:
            if( sts != EOK ) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR7,
                         "%s: Cannot deactivate on file system %s; %s\n"),
                         Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            }
            advfs_post_user_event( EVENT_SS_DEACTIVATED, advfs_event, Prog );
            break;

        case SS_UI_SUSPEND:
            if( sts != EOK ) {
                fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR8,
                        "%s: Cannot suspend on file system %s; %s\n"),
                        Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            }
            advfs_post_user_event( EVENT_SS_SUSPENDED, advfs_event, Prog );
            break;

        case SS_UI_RESET:
	    sts = advfs_ss_get_params( infop->fsname, &ssDmnParams );
	    if( sts != EOK ) {
		fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR10,
		    "%s: params not available on file system %s; %s\n"),
		    Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
	    }

	    /* Reset Statistic params */
	    ssDmnParams.ssFilesDefraged = 0;
	    ssDmnParams.ssFobsDefraged = 0;
	    ssDmnParams.ssExtentsConsol = 0;
	    ssDmnParams.ssFilesIOBal = 0;
	    ssDmnParams.ssFobsBalanced = 0;
	    ssDmnParams.ssPagesConsol = 0;
	    sts = advfs_ss_reset_params( infop->fsname, ssDmnParams );
	    if( sts != EOK ) {
		fprintf( stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR3,
		    "%s: Cannot set %s on file system %s; %s\n"),
		    Prog, options, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
	    }

	    break;

        case SS_UI_STATUS:
            if( sts != EOK ) {
                fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOSTATUS,
                        "%s: status not available on file system %s; %s\n"),
                        Prog, infop->fsname, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            } else {
                sts = advfs_ss_get_params( infop->fsname, &ssDmnParams );
                if( sts != EOK ) {
                    fprintf (stderr, catgets(catd, NL_AUTOTUNE, AUTOTUNE_ERR10,
                        "%s: params not available on file system %s; %s\n"),
                        Prog, infop->fsname, BSERRMSG(sts));
		    ret = 1;
		    goto _finish;
                }

		/* Report summary status */
		fprintf( stdout, catgets( catd, NL_AUTOTUNE, AUTOTUNE_SUMMARY,
		    "\nSummary status for %s:\n\n" ), Prog );

		/* Report file system name */
		fprintf( stdout, catgets( catd, NL_AUTOTUNE, AUTOTUNE_FS,
		    "  File system:                           %s\n" ),
		    infop->fsname );

		/* Report status ( ssRunningState ) */
                if( ssRunningState == 0 ) {
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STAT_STOP,
		      "  Status:                                stopped\n"));
                } else {
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STAT_RUN,
		      "  Status:                                running\n"));
                }

		/* Report state ( ssDmnCurrentState ) */
                if( ssDmnCurrentState == 0 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STATE_DEACT,
		      "  State:                                 deactivated\n" ) );
                else if( ssDmnCurrentState == 1 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STATE_ACT,
		      "  State:                                 activated\n" ));
                else if( ssDmnCurrentState == 2 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STATE_SUSPEND,
		      "  State:                                 suspended\n" ));
                else
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_STATE_UNKNOWN,
		      "  State:                                 unknown\n" ) );

		/* Report ssDmnParams */

		/* DEFRAG_ALL */
		if( ssDmnParams.ssDmnDefragAll == 1 )
		    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DEFRAGALL_EN,
		      "  Defrag_all:                            enabled\n" ) );
		else
		    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DEFRAGALL_DIS,
		      "  Defrag_all:                            disabled\n" ) );

		/* DEFRAG_ACTIVE */
		if( ssDmnParams.ssDmnDefragment == 1 )
		    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DEFRAGACT_EN,
		      "  Defrag_active:                         enabled\n" ) );
		else
		    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DEFRAGACT_DIS,
		      "  Defrag_active:                         disabled\n" ) );

		/* BALANCE */
                if( ssDmnParams.ssDmnBalance == 1 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_BAL_EN,
		      "  Balance:                               enabled\n" ) );
                else
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_BAL_DIS,
		      "  Balance:                               disabled\n" ) );

		/* TOPIOBALANCE */
                if( ssDmnParams.ssDmnSmartPlace == 1 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_TOPIOBAL_EN,
		      "  Top IO balance:                        enabled\n" ) );
                else
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_TOPIOBAL_DIS,
		      "  Top IO balance:                        disabled\n" ) );

		/* DIRECT_IO */
                if( ssDmnParams.ssDmnDirectIo == 1 )
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DIRECTIO_EN,
		      "  Direct IO File Processing:             enabled\n"));
                else
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_DIRECTIO_DIS,
		      "  Direct IO File Processing:             disabled\n"));

		/* PERCENTIOSWHENBUSY */
                fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_PERCENTIOS,
		      "  Percent IOs Allowed When System Busy:  %d%%\n" ),
		      ssDmnParams.ssMaxPercentOfIoWhenBusy );

		/* STEADY_STATE */
                gettimeofday( &curr_time, NULL );
                time_remaining = ssDmnParams.ssSteadyState -
                    ((curr_time.tv_sec - ssDmnParams.ssFirstMountTime) / 3600);
		fprintf( stdout, catgets( catd, NL_AUTOTUNE,
		    AUTOTUNE_STEADYSTATE,
		    "  Default Hours Until Steady State:      %d\n" ),
		    ssDmnParams.ssSteadyState );
		fprintf( stdout, catgets( catd, NL_AUTOTUNE, AUTOTUNE_REMHOURS,
		    "    Hours Remaining:                     " ) );
                if( ssDmnParams.ssFirstMountTime != 0 ) {
                    fprintf( stdout, "%d\n",
			( (signed)time_remaining < 0 ) ? 0 : time_remaining );
                } else {
                    fprintf (stdout, catgets(catd, NL_AUTOTUNE,
		      AUTOTUNE_NO_NONROMNTS, "(No non-readonly mounts)\n")); 
		}

		/* Statistics */
		fprintf( stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_STATS,
		    "\nStatistics for %s:\n\n" ), Prog );

		/* DEFRAG */
		fprintf( stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_DEFRAG,
                    "  Defragment   Total files:                    %d\n" ),
		    ssDmnParams.ssFilesDefraged );
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_PGSMOVED,
                    "               Pages Moved:                    %ld\n" ), 
		    (ssDmnParams.ssFobsDefraged / ADVFS_METADATA_PGSZ_IN_FOBS));
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_XTNTSCOMB,
                    "               Extents Combined:               %ld\n\n" ),
		    ssDmnParams.ssExtentsConsol);

		/* BALANCE */
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_BALANCE,
		    "  Balance      Files Moved Due To IO:          %ld\n" ), 
		    ssDmnParams.ssFilesIOBal);
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_BALPGSMOVE,
                    "               Pages Moved:                    %ld\n\n"), 
		    (ssDmnParams.ssFobsBalanced / ADVFS_METADATA_PGSZ_IN_FOBS));

		/* FREE SPACE */
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_FREESPACE,
		    "  Free Space   Pages Moved For Consolidation:  %ld\n\n"),
		    ssDmnParams.ssPagesConsol );
                (void)fflush( stdout );
            }
            break;

        case SS_UI_SHOWHIDDEN:
            sts = advfs_ss_get_params( infop->fsname, &ssDmnParams );
            if( sts != EOK ) {
                fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_MSG36,
                        "%s: Cannot get params; %s\n"),
                        Prog, BSERRMSG(sts));
		ret = 1;
		goto _finish;
            }
            fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_MSG37,
                    "ssAccessThreshHits = %d\n"),
                    ssDmnParams.ssAccessThreshHits);
            fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_MSG38,
                    "ssSteadyState = %d\n"),
                    ssDmnParams.ssSteadyState);
            fprintf (stdout, catgets(catd, NL_AUTOTUNE, AUTOTUNE_MSG39,
                    "ssMinutesInHotList = %d\n"),
                    ssDmnParams.ssMinutesInHotList);
            break;
    }

_finish:

    if(FSLock != -1) {
	if( advfs_unlock( FSLock, &err ) ) {
	    fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRUNLOCK,
		     "%s: Error unlocking '%s'; [%d] %s\n"),
		     Prog, infop->fsname, err, BSERRMSG( err ) );
	    fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_GEN_NUM_STR_ERR,
		     "%s: Error = [%d] %s\n"),
		     Prog, err, ERRMSG (err));
	}
        FSLock = -1;
    }

    free(infop);
    return ret;

} /* end main */
