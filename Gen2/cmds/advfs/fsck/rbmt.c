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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#include "fsck.h"

/*
 * Global
 */
extern nl_catd _m_catd;

/*
 * local prototypes
 */

static int correct_domain_markers(domainT *domain);

static int correct_rbmt_pages(domainT *domain);

static int correct_rbmt_rbmt_mcell(domainT *domain,
				   pageT *pPage, 
				   volNumT volNum);

static int correct_rbmt_rsvd_mcell(pageT    *pPage, 
				   volNumT  volNum, 
				   domainT  *domain,
				   pageNumT pageNum,
				   lbnNumT  *nextPageLBN);

static int correct_rbmt_sbm_mcell(pageT   *pPage, 
				  domainT *domain,
				  volNumT volNum);

static int correct_rbmt_root_tag_mcell(pageT   *pPage,
				       domainT *domain,
				       volNumT volNum, 
				       volNumT *rootTagVolume);

static int correct_rbmt_log_mcell(pageT   *pPage, 
				  volNumT volNum, 
				  domainT *domain,
				  logVolT **logVolume);

static int correct_rbmt_misc_mcell(pageT    *pPage, 
				   volNumT  volNum,
				   pageNumT pageNumber,
				   domainT  *domain);

static int correct_rbmt_primary_bmt(pageT *pPage, 
				    volNumT volNum);

static int correct_rbmt_ext_mcells(pageT   *pPage, 
				   volNumT volNum, 
				   domainT *domain,
				   volNumT *mattrVol, 
				   tagSeqT *mattrSeq);

static int correct_rbmt_floaters(pageT *pPage);

static int correct_rbmt_other_mcell(pageT    *pPage, 
				    cellNumT mcellNum,
				    volNumT  volNum, 
				    domainT  *domain);

static int correct_rbmt_attr(pageT    *pPage, 
			     bsMRT    *pRecord, 
			     cellNumT mcellNum, 
			     int      dataSafety,
			     int      reqServices);

static int correct_rbmt_umt_thaw_time(pageT *pPage, 
				      bsMRT *pRecord, 
				      cellNumT mcellNum);

static int correct_rbmt_xtnts_defaults(pageT    *pPage,
				       bsMRT    *pRecord, 
				       cellNumT mcellNum);

static int correct_rbmt_vd_attr(domainT *domain, 
				volNumT volNum, 
				pageT *pPage, 
				bsMRT *pRecord);

static int correct_rbmt_vd_params(pageT *pPage, 
				  bsMRT *pRecord, 
				  cellNumT mcellNum);

static int correct_rbmt_ss_attr(pageT *pPage, 
				bsMRT *pRecord, 
				cellNumT mcellNum);


/*
 * Function Name: correct_domain_rbmt
 *
 * Description: 
 *              This is used to check and fix the RBMT.  It's called
 *              from the init sequence in main()
 *
 * Input parameters:
 *     domain: The domain being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE or NO_MEMORY
 */
int 
correct_domain_rbmt(domainT *domain)
{
    char   *funcName ="correct_domain_rbmt";
    int    status;
    int    fixedPages;      /* Flag to see if fixes have been done */
    int    foundVolCorrupt; /* Used to locate volume corruption */
    int    mountStatus;     /* The volumes mount status */
    volNumT volNum;          /* Counter for the volume loop */
    pageT  *pPage;
    bsMPgT *pBMTPage;       /* The current RBMT page we are working on */
    bsMCT  *pMcell;         /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;        /* The Current mcell record we are working on */
    bsVdAttrT  *pVdAttr;    /* Pointer to the BSR_VD_ATTR record */
    bsIdT      mountId;
    
    /*
     * Init variables.
     */
    fixedPages = FALSE;

    /* Loop on all entries in domain->volumes[] and verify that
     * there's a basically intact RBMT on each
     */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	/*
	 * read RBMT page 0.
	 */
	status = read_page_from_cache(volNum, RBMT_BLK_LOC, &pPage);
	if (SUCCESS != status) {
	    return status;
	}
      
	status = validate_bmt_page(domain, pPage, 0, TRUE);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_487, 
			     "RBMT on volume %s is too corrupt to fix.\n"), 
		     domain->volumes[volNum].volName);
	    corrupt_exit(NULL);
	}

	status = release_page_to_cache(volNum, RBMT_BLK_LOC, pPage,
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    } /* end volume loop */

    /*
     * Make the ODS Version, fsCookie, and domainID agree across all
     * volumes and load the definitive values in the domain struct.
     * Correct the RBMTs and superblock as needed.
     */

    status = correct_domain_markers(domain);

    if (status != SUCCESS) {
	if (status == UNSUPPORTED_VERSION) {
	    failed_exit();
	}
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_931, 
			 "Unable to correct the domain markers.\n"));
	corrupt_exit(NULL);
    }

    /*
     * Call correct_rbmt_pages to check/fix rbmt metadata.
     */
    status = correct_rbmt_pages(domain);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    fixedPages = TRUE;
	} else {
	    return status;
	}
    }

    /*	
     *	 Loop through all the volumes checking mount status.
     */
    foundVolCorrupt = FALSE;
    mountStatus     = -1;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	if (domain->volumes[volNum].mntStatus != mountStatus) {
	    if (mountStatus == -1) {
		/*
		 *	First Volume found, uses its value.
		 */
		mountStatus = domain->volumes[volNum].mntStatus;
	    } else {
		writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			 catgets(_m_catd, S_FSCK_1, FSCK_504,
				 "Volume %s's mntStatus is %d expecting %d.\n"),
			 domain->volumes[volNum].volName, 
			 domain->volumes[volNum].mntStatus, mountStatus);
		foundVolCorrupt = TRUE;
	    }
	}
    }

    if (foundVolCorrupt == TRUE) {
        /*	
	 * Found corruption on at least one volume.	
	 */

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (domain->volumes[volNum].volFD == -1) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }
	    if (mountStatus != domain->volumes[volNum].mntStatus) {
		int foundFixed;

		foundFixed = FALSE;
		domain->volumes[volNum].mntStatus = mountStatus;

		/* 
		 * Read this volume's RBMT
		 */
		status = read_page_from_cache(volNum, RBMT_BLK_LOC, &pPage);
		if (status != SUCCESS) {
		    return status;
		}

		/*
		 * Modify page so it has the correct mount status.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
		    
	        pMcell = &(pBMTPage->bsMCA[RBMT_RBMT]);

		pRecord = (bsMRT *)pMcell->bsMR0;
		/*
		 * Now loop through all records in this mcell.
		 */
		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

		    if (pRecord->type == BSR_VD_ATTR) {
			pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_505, 
							   "Modified volume mount status from %d to %d.\n"),
						   pVdAttr->state,
						   mountStatus);
			if (status != SUCCESS) {
			    return status;
			}
			pVdAttr->state = mountStatus;
			foundFixed = TRUE;
		    }
		    pRecord = NEXT_MREC(pRecord);

		} /* end while record loop */

		assert(TRUE == foundFixed);
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_BLK_LOC, 
					       pPage, TRUE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if mount status corrupt */
	} /* end volume loop */
    } /* end if found corruption */


    /*
     * Loop through all the volumes mount Id.
     * It doesn't matter what the value is as long as they are the same
     * so we are just going to pick the first one we find and set all
     * of them to that value.
     */
    foundVolCorrupt = FALSE;
    mountId.id_sec = -1;
    mountId.id_usec = -1;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	if ((mountId.id_sec != domain->volumes[volNum].mountId.id_sec) ||
	    (mountId.id_usec != domain->volumes[volNum].mountId.id_usec)) {
	    if (-1 == mountId.id_sec) {
		/*
		 *	First Volume found, uses its value.
		 */
		mountId.id_sec  = domain->volumes[volNum].mountId.id_sec;
		mountId.id_usec = domain->volumes[volNum].mountId.id_usec;
	    } else {
		writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			 catgets(_m_catd, S_FSCK_1, FSCK_506,
				 "Volume %s's mntId is %x.%x expecting %x.%x.\n"),
			 domain->volumes[volNum].volName, 
			 domain->volumes[volNum].mountId.id_sec, 
			 domain->volumes[volNum].mountId.id_usec,
			 mountId.id_sec,mountId.id_usec);
		foundVolCorrupt = TRUE;
	    }
	} 
    } /* end volume loop */

    if (foundVolCorrupt == TRUE) {
        /*	
	 * Found corruption on at least one volume.	
	 */

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (domain->volumes[volNum].volFD == -1) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }

	    if ((mountId.id_sec != domain->volumes[volNum].mountId.id_sec) ||
		(mountId.id_usec != domain->volumes[volNum].mountId.id_usec)) {
		int foundFixed;

		foundFixed = FALSE;
		domain->volumes[volNum].mountId.id_sec = mountId.id_sec; 
		domain->volumes[volNum].mountId.id_usec = mountId.id_usec;

		/* 
		 * Read this volume's RBMT.
		 */
		status = read_page_from_cache(volNum, RBMT_BLK_LOC, &pPage);
		if (status != SUCCESS) {
		    return status;
		}

		/*
		 * Modify page so it has the correct mount status.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
	        pMcell = &(pBMTPage->bsMCA[RBMT_RBMT]);

		pRecord = (bsMRT *)pMcell->bsMR0;
		/*
		 * Now loop through all records in this mcell.
		 */
		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

		    if (pRecord->type == BSR_VD_ATTR) {
			pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_507, 
							   "Modified volume mntId from %x.%x to %x.%x.\n"),
						   pVdAttr->vdMntId.id_sec,
 						   pVdAttr->vdMntId.id_usec,
						   mountId.id_sec,
						   mountId.id_usec);
			if (status != SUCCESS) {
			    return status;
			}
			pVdAttr->vdMntId.id_sec = mountId.id_sec;
			pVdAttr->vdMntId.id_usec = mountId.id_usec;
			foundFixed = TRUE;
		    }
		    pRecord = NEXT_MREC(pRecord);

		} /* end while record loop */

		assert(TRUE == foundFixed);
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_BLK_LOC, 
					       pPage, TRUE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if mount status corrupt */
	} /* end volume loop */
    } /* end if found corruption */

    if (fixedPages == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_domain_rbmt*/


/*
 * Function Name: correct_domain_markers
 *
 * Description: Each volume in the domain could have differing ODS
 *              version, domain ID, or fsCookie values, which is an
 *              error.  In the future it may be that different
 *              ODSversions would be supported within a single domain.
 *              If that happens, we'll need to make corresponding
 *              changes here.  For now, this function establishes what
 *              the "definitive" values are for all domain volumes and
 *              writes them in the domain structure.  The RBMT and
 *              superblocks for each volume are corrected accordingly.
 *              Later on, the individual metadata records will be
 *              forced to contain those values.
 *
 * Input parameters:
 *     domain: The domain we're working on
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE.
 */
static int 
correct_domain_markers(domainT *domain)
{
    char   *funcName ="correct_domain_markers";
    volNumT volNum;
    int    status;
    char   errorMsg[MAXERRMSG];
    bsMCT  *pMcell;         /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;        /* The Current mcell record we are working on */
    bsDmnAttrT *pDmnAttr;   /* Pointer to the BSR_DMN_ATTR record */
    volNumT i;
    volNumT firstVol;
    char answer[10];
    int domainIdErr = FALSE;
    int cookieErr = FALSE;
    int versionErr = FALSE;
    int headerErr = FALSE;
    int modified = FALSE;

    struct _domainInfo {
	bfDomainIdT       dmnId;      /* volume's domain Id */
	bfFsCookieT       fsCookie;   /* volume's FS cooke */
	pageT             *rbmtCache; /* the cache page adrs */
	bsMPgT            *rbmt;      /* volume's RBMT */
	bsDmnAttrT        *dmnAttr;   /* volume's RBMT domain attribute rec */
	pageT             *superCache; /* the cache page adrs */
	advfs_fake_superblock_t *super; /* volume's fake superblock */
    } *info;

    info = (struct _domainInfo *)
	ffd_calloc(sizeof(*info) * (domain->highVolNum +1), sizeof(char));

    /* record the first valid volume number */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}
	break;
    }

    firstVol = volNum;

    /* 
     * Load the array with the relevant domain info values from each
     * volume in the domain.
     */

    for (volNum = firstVol; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	/*
	 * read in the Fake Superblock page
	 */

	status = read_page_from_cache(volNum, ADVFS_FAKE_SB_BLK, 
				      &info[volNum].superCache);
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

	info[volNum].super = (advfs_fake_superblock_t *) 
	                     info[volNum].superCache->pageBuf;

	/*
	 * read in RBMT page 0.
	 */

	status = read_page_from_cache(volNum, RBMT_BLK_LOC,
				      &info[volNum].rbmtCache);
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}
      
	info[volNum].rbmt = (bsMPgT *)info[volNum].rbmtCache->pageBuf;

	/* validate the RBMT magic number */

	if (info[volNum].rbmt->bmtMagicNumber != RBMT_MAGIC) {

	    writemsg(SV_ERR | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_953, 
			     "Invalid Magic Number on volNum %d RBMT page 0.\n"),
		     volNum);
	    status = add_fixed_message(&(info[volNum].rbmtCache->messages),
				       RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_958, 
					       "Modified RBMT page 0 magicNumber.\n"));
	    info[volNum].rbmt->bmtMagicNumber = RBMT_MAGIC;
	    headerErr++;

	} /* end: if RBMT magic number is invalid */

	/* 
	 * extract the cookie and domainID values from the RBMT's
	 * domain attribute record.  (note: attribute record is in the
	 * RBMT Mcell's first listed record at nextMCId.cell)
	 */

	pMcell = &(info[volNum].rbmt->bsMCA[0]);
	pMcell = &(info[volNum].rbmt->bsMCA[pMcell->mcNextMCId.cell]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) && (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    /* found the domain attribute record, grab dmnId. */

	    if (pRecord->type == BSR_DMN_ATTR) {

		pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));
		info[volNum].dmnId = pDmnAttr->bfDomainId;
		info[volNum].fsCookie = pDmnAttr->MasterFsCookie;
		info[volNum].dmnAttr = pDmnAttr;
		break;
	    }

	    pRecord = NEXT_MREC(pRecord);

	} /* end while loop: scan RBMT for domain attributes */

	/* Check that the rbmt and superblock version info matches. */

	if (!VERSION_EQ(info[volNum].rbmt->bmtODSVersion,
			info[volNum].super->adv_ods_version)) {

	    writemsg (SV_ERR | SV_LOG_FOUND, 
		      catgets(_m_catd, S_FSCK_1, FSCK_952, 
			      "ODS Version info in superBlk and RBMT mismatch.\n"));
	    status = add_fixed_message(&(info[firstVol].rbmtCache->messages),
				       RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_957,
					       "Modified volume %s ODS Version.\n"),
				       domain->volumes[firstVol].volName);

	    info[volNum].rbmt->bmtODSVersion = 
		info[volNum].super->adv_ods_version;

	    memcpy((void *)&domain->volumes[volNum].super, 
		   (void *)info[volNum].super, sizeof(advfs_fake_superblock_t));

	    headerErr++;  /* assure that this gets written back to disk */

	} /* end: if superblock and RBMT version info confilcts... */

	/* verify that the ODS version number does not exceed our
	   highest supported version number. */

	if (VERSION_UNSUPPORTED(info[volNum].super->adv_ods_version)) {

	    writemsg (SV_ERR | SV_LOG_FOUND, 
		      catgets(_m_catd, S_FSCK_1, FSCK_914,
			      "ODS version %d.%d not supported by this version of %s.\n"),
		      info[volNum].super->adv_ods_version.odv_major,
		      info[volNum].super->adv_ods_version.odv_minor,
		      Prog);
	    return (UNSUPPORTED_VERSION);
	}

	/* 
	 * Next we match up the MasterFsCookie (info[i].fsCookie) and
	 * the RBMT's page header fsCookie.  If they mismatch, but one
	 * of them matches the domainId, majority rules and we select
	 * the domainID to be the fsCookie/MasterFsCookie.  Otherwise,
	 * the MasterFsCookie wins.
	 */

	if (!COOKIE_EQ(info[volNum].fsCookie, 
		       info[volNum].rbmt->bmtFsCookie)) {

	    writemsg (SV_ERR | SV_LOG_FOUND, 
		      catgets(_m_catd, S_FSCK_1, FSCK_951,
			      "RBMT internal fsCookie mismatch.\n"));
	    status = add_fixed_message(&(info[firstVol].rbmtCache->messages),
				       RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_978,
					       "Modified volume %s RBMT fsCookie.\n"),
				       domain->volumes[firstVol].volName);

		
	    if (COOKIE_EQ(info[volNum].fsCookie, info[volNum].dmnId)) {
		info[volNum].rbmt->bmtFsCookie = info[volNum].dmnId;
	    } else if (COOKIE_EQ(info[volNum].rbmt->bmtFsCookie, 
				 info[volNum].dmnId)) {
		info[volNum].fsCookie = info[volNum].dmnId;
	    } else {
		info[volNum].rbmt->bmtFsCookie = info[volNum].dmnId;
		info[volNum].fsCookie = info[volNum].dmnId;
	    }
	    headerErr++;

	} /* end: If attr rec and rbmt header cookies mismatch... */

    } /* end for loop: scan all volume's */

    /* 
     * We have now recorded the domain, fsCookie, and ODSVersion info
     * from each domain volume in the info array.  
     *
     * We now know that all the info in a specific volume is now
     * consistent within itself.  Next we check for consistency among
     * the volumes by comparing the remaining volumes against the
     * first one.  If they are inconsistent, we blindly use the first
     * volume values domain wide. 
     * 
     * FUTURE: Consider making a more informed choice about which
     * of the inconsistent values to replicate domain wide.
     */

    for (i = firstVol+1; i <= domain->highVolNum; i++) {

	if (domain->volumes[i].volFD == -1) {
	    continue;
	}

	if (!domainIdErr && 
	    !DOMAIN_EQ(info[firstVol].dmnId, info[i].dmnId)) {
	    domainIdErr++;
	}

	if (!cookieErr && 
	    !COOKIE_EQ(info[firstVol].fsCookie, info[i].fsCookie)) {
	    cookieErr++;
	}

	if (!versionErr && !VERSION_EQ(info[firstVol].super->adv_ods_version, 
				       info[i].super->adv_ods_version)) {
	    versionErr++;
	}

	if (!versionErr && 
	    !VERSION_EQ(info[i].rbmt->bmtODSVersion,
			info[i].super->adv_ods_version)) {
	    versionErr++;
	}
    }

    /* if domain ID's mismtach across volumes, deal with that */

    if (domainIdErr) {

	writemsg (SV_ERR | SV_LOG_FOUND, 
		  catgets(_m_catd, S_FSCK_1, FSCK_496, 
			  "The domain IDs of all the volumes do not match.\n"));
	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_497, 
			  "It is possible that you have a volume from a different\n"));

	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_498, 
			  "domain listed as part of this domain.\n"));

	writemsg (SV_ERR | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_499, 
			  "Set all volumes to the same domain ID? (y/[n])  "));

	status = prompt_user_yes_no(answer, "n");
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

	if (answer[0] == 'n') {
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FSCK_1, FSCK_491, 
			    "Verify the correct volumes are in the domain.\n"));
	    corrupt_exit(errorMsg);
	} 

	/* Consider the first dmnId of a valid volume to be
	   defintinive, then update all the rbmt domain attribute
	   records to reflect that. */

	for (volNum = firstVol+1; volNum <= domain->highVolNum; volNum++) {
	    if (domain->volumes[volNum].volFD == -1) {
		continue;
	    }

	    info[volNum].dmnAttr->bfDomainId = info[firstVol].dmnId;
	}
    } /* endif: domain ID mismatch among volumes */

    /* if fsCookies mismatch across volumes, deal with that */

    if (cookieErr) {

	writemsg (SV_ERR | SV_LOG_FOUND, 
		  catgets(_m_catd, S_FSCK_1, FSCK_929, 
			  "The fsCookies of all the volumes do not match.\n"));
	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_497, 
			  "It is possible that you have a volume from a different\n"));

	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_498, 
			  "domain listed as part of this domain.\n"));

	writemsg (SV_ERR | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_930, 
			  "Set all volumes to the same fsCookie? (y/[n])  "));

	status = prompt_user_yes_no(answer, "n");
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

	if (answer[0] == 'n') {
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FSCK_1, FSCK_491, 
			    "Verify the correct volumes are in the domain.\n"));
	    corrupt_exit(errorMsg);
	} 

	/* Consider the first fsCookie of a valid volume to be
	   defintinive, then update all the rbmt domain attribute
	   records to reflect that. */

	for (volNum = firstVol+1; volNum <= domain->highVolNum; volNum++) {
	    if (domain->volumes[volNum].volFD == -1) {
		continue;
	    }
	    info[volNum].dmnAttr->MasterFsCookie = info[firstVol].fsCookie;
	}
    }  /* endif: fscookie mismatch among volumes */

    /* load the each volume's superblock content in the corresponding
       domain->volume[] entry and correct any errors in the on-disk
       superblock via "info" */

    if (versionErr) {

	writemsg (SV_ERR | SV_LOG_FOUND, 
		  catgets(_m_catd, S_FSCK_1, FSCK_976, 
			  "The ODS Versions of all the volumes do not match.\n"));
	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_497, 
			  "It is possible that you have a volume from a different\n"));

	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_498, 
			  "domain listed as part of this domain.\n"));

	writemsg (SV_ERR | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_977, 
			  "Set all volumes to the same ODS Version? (y/[n])  "));

	status = prompt_user_yes_no(answer, "n");
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

	if (answer[0] == 'n') {
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FSCK_1, FSCK_491, 
			    "Verify the correct volumes are in the domain.\n"));
	    corrupt_exit(errorMsg);
	} 

	/* Consider the first ODS Version of a valid volume to be
	   defintinive, then update all the rbmt domain attribute
	   records to reflect that. */

	for (volNum = firstVol+1; volNum <= domain->highVolNum; volNum++) {
	    if (domain->volumes[volNum].volFD == -1) {
		continue;
	    }

	    info[volNum].super->adv_ods_version = 
		info[firstVol].super->adv_ods_version;

	    info[volNum].rbmt->bmtODSVersion = 
		info[firstVol].super->adv_ods_version;

	    memcpy((void *)&domain->volumes[volNum].super, 
		   (void *)info[volNum].super, sizeof(advfs_fake_superblock_t));
	}
	    
	domain->ODSVersion = info[firstVol].super->adv_ods_version;

    }  /* endif: ODS Version mismatch among volumes */

    /* 
     * We now know that the marker values are consistent throughout
     * the domain.  Load those values into the domain structure, they
     * will serve as the definitive values from now on.
     */

    domain->dmnId = info[firstVol].dmnId;
    domain->fsCookie = info[firstVol].fsCookie;
    domain->ODSVersion = info[firstVol].super->adv_ods_version;

    /* 
     * release the modified metadata pages back to the cache 
     */

    for (volNum = firstVol; volNum <= domain->highVolNum; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	if (domainIdErr || cookieErr || versionErr || headerErr) {
	    modified = TRUE;
	} else {
	    modified = FALSE;
	}

	status = release_page_to_cache(volNum, RBMT_BLK_LOC, 
				       info[volNum].rbmtCache, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

	status = release_page_to_cache(volNum, ADVFS_FAKE_SB_BLK, 
				       info[volNum].superCache, 
				       versionErr, FALSE);
	if (status != SUCCESS) {
	    free(info);
	    return status;
	}

    } /* end for loop: release pages to cache */

    free(info);
    return SUCCESS;

} /* end correct_domain_markers */


/*
 * Function Name: correct_rbmt_pages
 *
 * Description: Veirfy the existence and correctness of RBMT
 *              mcell records and repair them as needed.
 *
 * Input parameters:
 *     domain: The domain to perform the check and fixes on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE.
 */
static int 
correct_rbmt_pages(domainT *domain)
{
    char     *funcName ="correct_rbmt_pages";
    int      status;
    lbnNumT  currPageLBN;
    lbnNumT  nextPageLBN;    /* LBN of next RBMT page to check */
    int      modified;       /* Flag to indicate if page has been modified */
    pageNumT pageNumber;     /* The page number we are working on */
    cellNumT freeCounter;    /* How many free mcells on page */
    volNumT  rootTagVolume;  /* Which volume has the root Tag file */
    volNumT  mattrVol;       /* Which volume has current mattr */
    tagSeqT  mattrSeq;       /* Current sequence number of mattr */
    volNumT  volNum;         /* Loop counters */
    cellNumT mcell;          /* Loop counters */
    cellNumT priorMcell;     /* Previous mcell */
    int      found;
    xtntNumT extent;         /* Loop counters */
    xtntNumT numExtents;   
    pageT    *pPage;         /* The current cache page we are working on */
    bsMPgT   *RBMTpage;      /* The current RBMT page we are working on */
    bsMCT    *pMcell;        /* The current RBMT mcell we are working on */
    bsMRT    *pRecord;       /* The Current mcell record we are working on */
    volumeT  *volume;        /* Short cut to access the volume */
    logVolT  *logVolume;     /* linked list of vol which have log extents. */
    logVolT  *headLogVolume; /* Head of link list */
    logVolT  *tmpLogVolume;  /* pointer to link list */
    mcellUsage  freeArray[BSPG_CELLS];  /* which mcells are free */
    bsDmnMAttrT *pDmnMAttr;
    fobtoLBNT  *pExtents; 

    /*
     * Initialize once per domain.
     */
    logVolume = NULL;
    mattrVol  = 0;
    mattrSeq  = 0;

    /*
     * Loop through all the volumes.
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}
	currPageLBN = RBMT_BLK_LOC;
	  
	/* 
	 * Read this volume's RBMT page 0.
	 */
	status = read_page_from_cache(volNum, currPageLBN, &pPage);
	if (status != SUCCESS) {
	    return status;
	}
	  
	/*	
	 * Initialize once per volume
	 */
	modified      = FALSE;
	pageNumber    = 0;
	rootTagVolume = FALSE;

	RBMTpage = (bsMPgT *)pPage->pageBuf;


	/*
	 * Loops through all the RBMT pages in the current volume.
	 * Check/fix the page headers, mcells headers, and the
	 * reserved mcell (last mcell) record (which by convention
	 * holds xtnts for the next RBMT page, if any)
	 */
	
	while (pPage != NULL) {
	    /*
	     * Initialize nextPageLBN to default value.
	     */
	    nextPageLBN = XTNT_TERM;
	    
	    /*
	     * Fix the page header
	     */
	    status = correct_bmt_page_header(pageNumber,
					     volNum,
					     FALSE,/* Single page extent */
					     -1,   /* Single page extent */
					     (pageNumber + 1),
					     pPage,
					     domain,
					     TRUE); /* rbmtFlag */
	    if (status != SUCCESS) {
		if (status == FIXED) {
		    modified = TRUE;
		} else {
		    return status;
		}
	    }
	    
	    /*
	     * There is not a free list for RBMT pages so the
	     * nextFreePg entry in the page header is not used. We
	     * expect the nextFreePage entry to therefore be 0.  (The
	     * free list for mcells uses the normal BMT mechanism.)
	     */

	    if (RBMTpage->bmtNextFreePg != 0) {
	        writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_508, 
				 "Modifying volume %s RBMT page %ld's nextFreePg from %d to %d.\n"),
			 domain->volumes[volNum].volName, pageNumber, 
			 RBMTpage->bmtNextFreePg, 0);
		status = add_fixed_message(&(pPage->messages),
					   RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_509, 
						   "Modified RBMT page %ld's nextFreePg.\n"),
					   pageNumber);
		if (status != SUCCESS) {
		  return status;
		}

		RBMTpage->bmtNextFreePg = 0;
		modified = TRUE;
	    }

	    /*
	     * Check/fix the mcell headers for this RBMT page.
	     */
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {

		pMcell = &(RBMTpage->bsMCA[mcell]);
		
		if ((pMcell->mcTag.tag_num == 0) &&
		    (pMcell->mcBfSetTag.tag_num == 0)) {
		    /*
		     * mcell is empty, record that fact
		     */
		    freeArray[mcell] = FREE;
		    
		    /*
		     * linkSegment must be zero.
		     * mcNextMCId.volume must be the current volume.
		     * mcNextMCId.cell must be less than LAST_MCELL.
		     * mcNextMCId.page must equal the current page number,
		     *   unless this is the tail of the list (page 0 cell 0).
		     */
		    if ((pMcell->mcLinkSegment != 0) ||
			((pMcell->mcNextMCId.volume != volNum) &&
			 (pMcell->mcNextMCId.volume != 0)) ||
			(pMcell->mcNextMCId.cell >= LAST_MCELL) ||
			((pMcell->mcNextMCId.page != pageNumber) &&
			 ((pMcell->mcNextMCId.page != 0) ||
			  (pMcell->mcNextMCId.cell != 0)))) {

			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_510,
					 "Mcell (%d,%ld,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_511,
							   "Modified mcell (%d,%ld,%d) to be marked as empty.\n"),
						   volNum,
						   pageNumber,
						   mcell);
			if (status != SUCCESS) {
			    return status;
			}

			ZERO_MCID(pMcell->mcNextMCId);
			pMcell->mcLinkSegment = 0;
			modified = TRUE;
		    }
		} else {
		    /* Mcell is used, record that fact */
		    freeArray[mcell] = USED;

		    /* check/fix volume number */

		    if ((pMcell->mcNextMCId.volume != 0) &&
			(pMcell->mcNextMCId.volume != volNum)) {

			if ((pMcell->mcNextMCId.page == 0) &&
			    (pMcell->mcNextMCId.cell == 0)) {
			    pMcell->mcNextMCId.volume = 0;
			} else {
			    pMcell->mcNextMCId.volume = volNum;
			}

			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_510,
					 "Mcell (%d,%ld,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_512, 
							   "Modified mcell (%d,%ld,%d)'s volume to %d.\n"),
						   volNum, pageNumber, mcell,
						   pMcell->mcNextMCId.volume);
			if (status != SUCCESS) {
			    return status;
			}
			modified = TRUE;
		    }
		    
		    /* check/fix fileset tag number */

		    if (pMcell->mcBfSetTag.tag_num != (tagNumT)-BFM_BFSDIR) {
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_510,
					 "Mcell (%d,%ld,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_513, 
							   "Modified mcell (%d,%ld,%d)'s set tag from %ld to %ld.\n"),
						   volNum, pageNumber, mcell,
						   pMcell->mcBfSetTag.tag_num, -BFM_BFSDIR);
			if (status != SUCCESS) {
			    return status;
			}
			pMcell->mcBfSetTag.tag_num = -BFM_BFSDIR;
			modified = TRUE;
		    }		    
		    /* Will check pMcell.Tag below */

		} /* end if free/used */
	    } /* end loop of mcell headers */
	    
	    /*
	     * Free mcells in an RBMT page are listed via the
	     * nextfreeMCID linkage.  We check/fix that linkage here.
	     *
	     * First, check/fix the linkage head pointer
	     */

	    if ((RBMTpage->bmtNextFreeMcell &&
		 RBMTpage->bmtNextFreeMcell >= LAST_MCELL) ||
		freeArray[RBMTpage->bmtNextFreeMcell] != FREE) {

		for (mcell = 0; mcell < LAST_MCELL; mcell++)
		    if (freeArray[mcell] == FREE) {
			break;
		    }

		if (mcell >= LAST_MCELL) {
		    mcell = 0;
		}
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_954, 
				 "Modifying volume %s RBMT page %ld's nextfreeMcell from %d to %d.\n"),
			 domain->volumes[volNum].volName, pageNumber, 
			 RBMTpage->bmtNextFreeMcell, mcell);

		status = add_fixed_message(&(pPage->messages),
					   RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_955, 
						   "Modified RBMT page %ld's nextfreeMCId.\n"),
					   pageNumber);
		if (status != SUCCESS) {
		  return status;
		}

		RBMTpage->bmtNextFreeMcell = mcell;
		modified = TRUE;
	    }
	    
	    /* now that we know that RBMTpage->bmtNextFreeMcell is cool,
	       check/fix the free list */

	    freeCounter = 0;
	    priorMcell  = 0;
	    mcell = RBMTpage->bmtNextFreeMcell;

	    while (mcell != 0) {

		pMcell = &(RBMTpage->bsMCA[mcell]);
		if (freeArray[mcell] == FREE) {
		    /* 'ONLIST' means ON the free LIST.  We change the
		       status to indicate that we found the free mcell
		       in the nextfreeMCId list, as expected. */
		    freeArray[mcell] = ONLIST;
		    freeCounter++;
		} else {
		    /* Since this mcell is on the nextfreeMCId list,
		       we expected it to be FREE.  Since it isn't,
		       there's either an active cell on the free list
		       OR the free list has a loop.
		     */
		    if (priorMcell == 0) {
			/* 
			 * this is the first mcell on list, break the
			 * chain
			 */
			RBMTpage->bmtNextFreeMcell = 0;
		    } else {
			pMcell = &(RBMTpage->bsMCA[priorMcell]);
                        pMcell->mcNextMCId.volume = 0;
			pMcell->mcNextMCId.page = 0;
			pMcell->mcNextMCId.cell = 0;
		    }
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_510,
				     "Mcell (%d,%ld,%d) is corrupt.\n"),
			     volNum, pageNumber, mcell);
		    status = add_fixed_message(&(pPage->messages),
					       RBMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_514, 
						       "Modified RBMT mcell free list.\n"));
		    if (status != SUCCESS) {
			return status;
		    }
		    
		    /*
		     * Get out of loop.
		     */
		    modified = TRUE;
		    mcell = 0;
		    continue;
		} /* end if free */
		priorMcell = mcell;
		mcell = pMcell->mcNextMCId.cell;
	    } /* end while loop, free mcell chain  */
	    
	    /* Any entrys in the freeArray list we've just built up
	       that are still listed as FREE (not USED or ONLIST) are
	       free mcells that are not linked into the free mcell
	       list.  We loop through the freeArray and add these to
	       the free mcell list */

	    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		if (freeArray[mcell] == FREE) {
		    /*
		     * Free mcell not on list, so add it.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_515, 
				     "Mcell (%d,%ld,%d) should be on free list.\n"),
			     volNum, pageNumber, mcell);
		    status = add_fixed_message(&(pPage->messages),
					       RBMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_514, 
						       "Modified RBMT mcell free list.\n"));
		    if (status != SUCCESS) {
			return status;
		    }

		    pMcell = &(RBMTpage->bsMCA[mcell]);
		    pMcell->mcNextMCId.cell =  RBMTpage->bmtNextFreeMcell;
		    RBMTpage->bmtNextFreeMcell = mcell;
		    modified = TRUE;
		    freeCounter++;
		}
	    } /* end loop: add to free list */

	    /* check/fix the free mcell count for the current page */
	    
	    if (RBMTpage->bmtFreeMcellCnt != freeCounter) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_516, 
				 "Mcell free list incorrect on page %ld.\n"),
			 pageNumber);
		status = add_fixed_message(&(pPage->messages),
					   RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_514, 
						   "Modified RBMT mcell free list.\n"));
		if (status != SUCCESS) {
		    return status;
		}
		RBMTpage->bmtFreeMcellCnt = freeCounter;
		modified = TRUE;
	    }
	    
	    /* Check/fix the RBMT reserved mcell (last mcell), which
	       holds the extra extents that map the next RBMT page, if
	       any */

	    status = correct_rbmt_rsvd_mcell(pPage, 
					     volNum,
					     domain,
					     pageNumber,
					     &nextPageLBN);
	    if (status != SUCCESS) {
	        if (status == FIXED) {
		    modified = TRUE;
		} else {
		    return status;
		}
	    }

	    if (modified == TRUE) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_517, 
						   "Modified RBMT page.\n"));
		if (status != SUCCESS) {
		    return status;
		}
	    }
	    
	    status = release_page_to_cache(volNum, currPageLBN, pPage, 
					   modified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }

	    modified = FALSE;
	    
	    /* if there's another RBMT page get it and we'll continue
	       the above tests  */

	    if (nextPageLBN != XTNT_TERM) {
		currPageLBN = nextPageLBN;
		status = read_page_from_cache(volNum, currPageLBN, &pPage);
		if (status != SUCCESS) {
		    return status;
		}
		pageNumber++;
		/*
		 * Need to reset RBMTpage each time through the loop
		 */
		RBMTpage = (bsMPgT *)pPage->pageBuf;
	    } else {
		pPage = NULL;
	    }
	} /* End loop of RBMT pages */

	/*
	 * Now load the extents for the RBMT pages (do not add them to
	 * the SBM, that will be done later).
	 */
	status = collect_rbmt_extents(domain, volNum, FALSE);

	if (status != SUCCESS) {
	    return status;
	}
	
	/*
	 * Now we loop through the RBMT pages a second time to
	 * check/fix the individual mcell records.
	 */

	volume = &(domain->volumes[volNum]);
	pExtents = volume->volRbmt->rbmtLBN;
	numExtents = volume->volRbmt->rbmtLBNSize;
	pageNumber = 0;

	for (extent = 0; extent < numExtents; extent++) {

	    currPageLBN = pExtents[extent].lbn;

	    status = read_page_from_cache(volNum, currPageLBN, &pPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    RBMTpage = (bsMPgT *)pPage->pageBuf;

	    /*
	     * Check/fix the mcells and mcell records of RBMT.
	     */
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		pMcell = &(RBMTpage->bsMCA[mcell]);
		
		/*
		 * Specific mcells on page 0 are if a known type. We
		 * check/fix those here.
		 */

		if (pageNumber == 0) {

		    switch(mcell) {

		    case (RBMT_RBMT): /* Check mcell - RBMT */
			status = correct_rbmt_rbmt_mcell(domain,
							 pPage,
							 volNum);
			break;
			    
		    case (RBMT_SBM):/* Check mcell - SBM */
			status =  correct_rbmt_sbm_mcell(pPage,
							 domain,
							 volNum);
			break;
			    
		    case (RBMT_RTAG): /* Check mcell - Root Tag Dir */
			status = correct_rbmt_root_tag_mcell(pPage,
							     domain,
							     volNum,
							     &rootTagVolume);
			domain->volumes[volNum].rootTag = rootTagVolume;
			break;
			    
		    case (RBMT_LOG): /* Check mcell - Log */
			status = correct_rbmt_log_mcell(pPage,
							volNum,
							domain,
							&logVolume);
			break;
			    
		    case (RBMT_BMT):  
			/* Check mcell - BMT */
			status = correct_rbmt_primary_bmt(pPage, 
							  volNum);
			break;
			    
		    case (RBMT_MISC): /* Check mcell - Misc */
			status = correct_rbmt_misc_mcell(pPage,
							 volNum,
							 pageNumber,
							 domain);
			break;
			    
		    case (RBMT_RBMT_EXT):
			status = correct_rbmt_ext_mcells(pPage,
							 volNum,
							 domain,
							 &mattrVol,
							 &mattrSeq);
			break;

		    case (RBMT_RBMT_EXT1):
		    case (RBMT_RBMT_EXT2):
			/* these are checked as part of the RBMT chain
			   by RBMT_RBMT_EXT above */
			status = SUCCESS;
			break;

		    case (RBMT_RSVD_CELL):
			/* This was checked in a previous call to
			   correct_rbmt_rsvd_mcell() */
			status = SUCCESS;
			break;

		    default:
			/*
			 * All other mcells on RBMT are floaters,
			 * BSR_XTRA_XTNTS, or empty mcells.
			 */
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
			break;
			    
		    } /* end switch */

		    if (status != SUCCESS) {
			if (status == FIXED) {
			    modified = TRUE;
			} else {
			    return status;
			}
		    }
		    
		} else {
		    /* 
		     * Some page other than 0.
		     */
		    if (mcell != RBMT_RSVD_CELL) {
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
		    } else {
			/*
			 * Already checked
			 */
			status = SUCCESS;
		    }
		    if (status != SUCCESS) {
			if (status == FIXED) {
			    modified = TRUE;
			} else {
			    return status;
			}
		    }
		} /* end if page == 0 */
	    } /* End of mcell loop */
	    
	    if (modified == TRUE) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_517, 
						   "Modified RBMT page.\n"));
		if (status != SUCCESS) {
		    return status;
		}
	    }
	    
	    status = release_page_to_cache(volNum, currPageLBN, pPage, 
					   modified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = FALSE;
	    pageNumber++;
	} /* End loop of extents */
    } /* end volume loop */

    /* 
     * flag which volume is the root tag volume
     */
    rootTagVolume = 0;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}
	if (domain->volumes[volNum].rootTag != FALSE) {
	    if (rootTagVolume == 0) {
		rootTagVolume = volNum;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_518,
				 "Unable to continue, more than one root tag file.\n"));
		corrupt_exit(NULL);
	    }
	} /* end if root tag volume found */
    } /* end volume loop */

    if (rootTagVolume == 0) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_519,
			 "Unable to continue, not able to find a volume with a root tag file.\n")); 
	corrupt_exit(NULL);
    } else {
        domain->rootTagVol = rootTagVolume;
    }

    if (logVolume == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_520,
			 "Unable to continue, not able to find a volume with the transaction log.\n"));
	corrupt_exit(NULL);
    }

    /*
     * Make sure that we found a mutable attrs record.
     */
    if (mattrVol == 0) {
        /*
	 * Future: What if we found the logVolume, should we create the
	 *         mattr record?  More research is needed for this.
	 */
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_521,
			 "Unable to continue, can't find a volume with domain attributes.\n"));
	corrupt_exit(NULL);
    } else {

        found = FALSE;
	headLogVolume = logVolume;

	while ((found != TRUE) && (headLogVolume != NULL)) {
	    /*
	     *  Walk the logVolume chain looking for match to mattrVol.
	     */
	    if (headLogVolume->volume == mattrVol) {
	        domain->logVol = mattrVol;
		found = TRUE;
	    } else {
	        headLogVolume = headLogVolume->next;
	    }
	}
	
	if (found == FALSE) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_522,
			     "Unable to continue, can't find domain attributes on the log volume.\n")); 
	    corrupt_exit(NULL);
	}
    }

    /*
     * free log volume struture.
     */
    headLogVolume = logVolume;
    while (headLogVolume != NULL) {
        tmpLogVolume = headLogVolume;
	headLogVolume = headLogVolume->next;
	free(tmpLogVolume);
    }
    
    /*
     * The valid mutable attributes record (BSR_DMN_MATTR) is found in
     * the RBMT of the same volume that houses the log (this record
     * exists on all volumes as a placeholder but is only considered
     * valid on the log volume).  The BSR_DMN_MATTR record will have
     * already been found and checked in the correct_rbmt_ext_mcell()
     * routine, yet certain additional checks could not be done until
     * we know which volume the log is on.  Now that we know that, do
     * those checks.
     */

    currPageLBN = RBMT_BLK_LOC;
    status = read_page_from_cache(mattrVol, currPageLBN, &pPage);
    if (status != SUCCESS) {
        return status;
    }

    RBMTpage = (bsMPgT *)pPage->pageBuf;

    pMcell = &(RBMTpage->bsMCA[6]);

    pRecord = (bsMRT *)pMcell->bsMR0;
    found = FALSE;

    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) &&
	   (found == FALSE)) {

        if (pRecord->type == BSR_DMN_MATTR) {
	    pDmnMAttr = (bsDmnMAttrT *)((char *)pRecord + sizeof(bsMRT));
	    found = TRUE;
	    break;
	}	    

	pRecord = NEXT_MREC(pRecord);
    }

    /*
     * Based on earlier checks we should ALWAYS find the mattr record.
     */
    assert(found == TRUE);
    
    if (pDmnMAttr->bfSetDirTag.tag_num != 
	RSVD_TAGNUM_GEN(rootTagVolume, RBMT_RTAG)) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_523,
					   "Modified set dir tag's number from %ld to %ld.\n"),
				   pDmnMAttr->bfSetDirTag.tag_num,
				   RSVD_TAGNUM_GEN(rootTagVolume, RBMT_RTAG));
	if (status != SUCCESS) {
	    return status;
	}
        pDmnMAttr->bfSetDirTag.tag_num = RSVD_TAGNUM_GEN(rootTagVolume, 
							 RBMT_RTAG);
        modified = TRUE;
    }

    if (pDmnMAttr->vdCnt != domain->numVols) {
        char answer[10];  /* buffer for user input */
	char errorMsg[MAXERRMSG];
	int  status;

	writemsg(SV_ERR | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FSCK_1, FSCK_525, 
			 "The domain attributes on volume %d has a different number of\n"), 
		  volNum);
	writemsg(SV_ERR | SV_LOG_FOUND | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_526, 
			 "volumes compared to the number of volumes in /dev/advfs.\n"));

	writemsg(SV_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_527, 
			 "Set the number of volumes to %d? (y/[n])  "),
		 domain->numVols);
		      
	status = prompt_user_yes_no(answer, "n");
	if (status != SUCCESS) {
	    return status;
	}

	if (answer[0] == 'n') {
	    /*
	     * User does not want to continue.
	     */
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FSCK_1, FSCK_528, 
			    "Exit requested, try running 'advscan' to find all volumes.\n"));
	    corrupt_exit(errorMsg);
	}
	pDmnMAttr->vdCnt = domain->numVols;
	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_529, 
			 "Corruption in domain mattr record.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_530, 
					   "Modified RBMT domain mattr record.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }
    
    status = release_page_to_cache(mattrVol, currPageLBN, pPage,
				   modified, FALSE);
    if (status != SUCCESS) {
        return status;
    }

    /*
     * Free the RBMT extent array as it will be collected later, and
     * at that point we will need to collect SBM information.
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

	volume = &(domain->volumes[volNum]);

        if (volume->volFD == -1) {
	    continue;
	}

	free (volume->volRbmt->rbmtLBN);
	volume->volRbmt->rbmtLBN = NULL;
	volume->volRbmt->rbmtLBNSize = 0;
    } /* end of volume loop */

    return SUCCESS;
} /* end correct_rbmt_pages */


/*
 * Function Name: correct_rbmt_rbmt_mcell
 *
 * Description: This function check/fix all the linked mcells for the
 *              RBMT.  This includes mcell 0 and also the various 
 *              RBMT EXT mcells. 
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
static int 
correct_rbmt_rbmt_mcell(domainT *domain,
			pageT *pPage, 
			volNumT volNum)
{
    char   *funcName = "correct_rbmt_rbmt_mcell";
    bsMPgT *RBMTpage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;   /* The Current mcell record we are working */
    int    modified;   /* Flag to let us know if the page has been modified. */
    cellNumT mcellNum;
    tagNumT  tagNum;
    int      status;
    bsXtntRT    *pXtnt;

    /*
     * Init variables.
     */
    modified = FALSE;
    mcellNum = RBMT_RBMT;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    RBMTpage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(RBMTpage->bsMCA[0]);

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_531,
					   "Modified RBMT RBMT mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    if ((pMcell->mcNextMCId.page != 0) || 
	(pMcell->mcNextMCId.cell != 6) ||
        (pMcell->mcNextMCId.volume != volNum)) {
      status = add_fixed_message(&(pPage->messages), RBMT,
				 catgets(_m_catd, S_FSCK_1, FSCK_532, 
					 "Modified RBMT RBMT mcell's next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
				 pMcell->mcNextMCId.volume, 
				 pMcell->mcNextMCId.page,
				 pMcell->mcNextMCId.cell,
				 volNum, 0, 6);
      if (status != SUCCESS) {
	return status;
      }
      pMcell->mcNextMCId.volume = volNum;
      pMcell->mcNextMCId.page = 0;
      pMcell->mcNextMCId.cell = 6;
      modified = TRUE;
    } 

    pRecord = (bsMRT *)pMcell->bsMR0;

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 2, 0);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */

    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    if (pXtnt->xCnt != 2) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_543, 
					   "Modified RBMT RBMT mcell's xCnt from %d to %d.\n"),
				   pXtnt->xCnt, 2);
	if (status != SUCCESS) {
	    return status;
	}
        pXtnt->xCnt = 2;
	modified = TRUE;
    }

    /*
     * Now correct the xtnt values that are specific to the RBMT
     *
     * Can only have a single extent which starts at RBMT_BLK_LOC
     */
    if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	(pXtnt->bsXA[0].bsx_vd_blk != RBMT_BLK_LOC) ||
	(pXtnt->bsXA[1].bsx_fob_offset != ADVFS_METADATA_PGSZ_IN_FOBS) ||
	(pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM)) {
        status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_544, 
					   "Modified RBMT RBMT mcell's extents.\n"));
	if (status != SUCCESS) {
	    return status;
	}
	pXtnt->bsXA[0].bsx_fob_offset = 0;
	pXtnt->bsXA[0].bsx_vd_blk = RBMT_BLK_LOC;
	pXtnt->bsXA[1].bsx_fob_offset = ADVFS_METADATA_PGSZ_IN_FOBS;
	pXtnt->bsXA[1].bsx_vd_blk = XTNT_TERM;
	modified = TRUE;
    }

    /*
     * RBMT xtra extents are found in the RBMT reserved mcell (last
     * mcell)
     */
    if (pXtnt->mcellCnt != 1) {
        /*	
	 * more than 1 mcell in chain, chain better point to 
	 * (volNum,page 0,RBMT_RSVD_CELL).
	 */
        if ((pXtnt->chainMCId.volume != volNum) ||
	    (pXtnt->chainMCId.page != 0) ||
	    (pXtnt->chainMCId.cell != RBMT_RSVD_CELL)) {

	    bsMCT  *pTempMcell;    
	    bsMRT  *pTempRecord;   

	    pTempMcell  = &(RBMTpage->bsMCA[RBMT_RSVD_CELL]);
	    pTempRecord = (bsMRT *)pTempMcell->bsMR0;

	    /*
	     * Is chain pointer incorrect, or is mcellCnt incorrect?
	     *
	     * If mcell RBMT_RSVD_CELL is of type BSR_XTRA_XTNTS
	     * we will assume the chain is correct, otherwise
	     * mcellCnt is incorrect.
	     */
	    if (pTempRecord->type == BSR_XTRA_XTNTS) {
	        status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_545,
						       "Modified RBMT RBMT mcell's chain pointer from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   pXtnt->chainMCId.volume,
					   pXtnt->chainMCId.page,
					   pXtnt->chainMCId.cell,
					   volNum, 0, RBMT_RSVD_CELL);
		if (status != SUCCESS) {
		    return status;
		}
		pXtnt->chainMCId.volume = volNum;
		pXtnt->chainMCId.page = 0;
		pXtnt->chainMCId.cell = RBMT_RSVD_CELL;
	    } else {
	        status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_546, 
						   "Modified RBMT RBMT mcell's mcellCnt from %d to %d.\n"),
					   pXtnt->mcellCnt, 1);
		if (status != SUCCESS) {
		    return status;
		}
		pXtnt->mcellCnt = 1;
	    }
	    modified = TRUE;
	} /* end if chain pointer not correct */
    } /* end if more than 1 mcell in chain */

    pRecord = NEXT_MREC(pRecord);

    /*
     * This record must be of type BSR_VD_ATTR
     */

    status = correct_rbmt_vd_attr(domain, volNum, pPage, pRecord);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    pRecord = NEXT_MREC(pRecord);

    /*
     * This record must be of type BSR_DMN_UMT_THAW_TIME
     */
    status = correct_rbmt_umt_thaw_time(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should not have any more records.
     */
    if (pRecord->type != BSR_NIL) {
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_549, 
					   "Deleted records from RBMT's RBMT mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_550, 
			 "Found corruption in RBMT's RBMT mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_551, 
					   "Modified RBMT's RBMT mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_rbmt_mcell */


/*
 * Function Name: correct_rbmt_sbm_mcell
 *
 * Description: This function will check mcell 1 of the RBMT. 
 *              It will fix and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     domain: The domain we are working on.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
static int 
correct_rbmt_sbm_mcell(pageT *pPage, 
		       domainT *domain, 
		       volNumT volNum)
{
    char   *funcName = "correct_rbmt_sbm_mcell";
    bsMPgT *RBMTpage;     /* The current RBMT page we are working on */
    bsMCT  *pMcell;       /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;      /* The current mcell record we are working on */
    int       modified;   /* flag if the page has been modified. */
    int       status;
    cellNumT  mcellNum;
    tagNumT   tagNum;
    pageNumT  sbmPages;   /* SBM size in pages */
    fobNumT   sbmFobs;    /* SBM size in FOBs */
    fobNumT   volFobs;    /* volume zie in FOBs */
    ldiv_t    sbmDiv;     /* quotient & remainder in SBM calc */
    bsXtntRT  *pXtnt;
    lbnNumT   sbmFirstBlk;
    lbnNumT   vdBlkCnt;
    pageT     *pTempPage;

    /*
     * Init variables
     */
    modified = FALSE;
    RBMTpage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_SBM;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    pMcell   = &(RBMTpage->bsMCA[mcellNum]);
    volFobs = domain->volumes[volNum].volSize * ADVFS_FOBS_PER_DEV_BSIZE;


    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_552, 
					   "Modified RBMT SBM mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    pRecord = (bsMRT *)pMcell->bsMR0;

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 2, 0);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (pXtnt->xCnt != 2) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_561, 
					   "Modified RBMT SBM mcell's xCnt from %d to %d.\n"), 
				   pXtnt->xCnt, 2);
	if (status != SUCCESS) {
	    return status;
	}
        pXtnt->xCnt = 2;
	modified = TRUE;
    }

    assert (domain->volumes[volNum].volSize != 0);

    /*
     * RBMT unique values:
     *
     * The SBM starts at a disk block that is 40% of the way across
     * the volume, rounded up to the nearest cluster boundary.
     */

    vdBlkCnt = domain->volumes[volNum].volSize;
    sbmFirstBlk = roundup((vdBlkCnt / 100) * SBM_START_PCT, 
			  ADVFS_BS_CLUSTSIZE);

    if (pXtnt->bsXA[0].bsx_vd_blk != sbmFirstBlk) {

	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_719,
			 "Calculated SBM start blk and RBMT's start blk mismatch.\n"));

	/*
	 * Check to see if sbmFirstBlk is part of the SBM by checking
	 * the page checksum.
	 */
	status = read_page_from_cache(volNum, sbmFirstBlk, &pTempPage);
	if (status != SUCCESS) {
	    return status;
	}

	status = validate_sbm_page(pTempPage);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_562,
			     "Unable to find the starting block of the SBM.\n"));
	    corrupt_exit(NULL);
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_563, 
					   "Modified the block that the SBM starts on from %ld to %ld.\n"),
				   pXtnt->bsXA[0].bsx_vd_blk, sbmFirstBlk);
	if (status != SUCCESS) {
	    return status;
	}

	pXtnt->bsXA[0].bsx_vd_blk = sbmFirstBlk;

	status = release_page_to_cache(volNum, sbmFirstBlk, 
				       pTempPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;

    } /* end if starting block invalid */

    /*
     * Compute the SBM size in FOBs.
     */

    sbmDiv = ldiv(volFobs, SBM_FOBS_PER_PG(ADVFS_BS_CLUSTSIZE)); 
    sbmPages = sbmDiv.quot;

    if (sbmDiv.rem != 0) {
      sbmPages++;
    }

    sbmFobs = sbmPages * ADVFS_METADATA_PGSZ_IN_FOBS; 

    /*
     * Check the parts of the SBM extent which will be the same
     * regardless of where it starts:
     *
     *       0          XXXXXX
     *       sbmFobs   -1
     */
    if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	(pXtnt->bsXA[1].bsx_fob_offset != sbmFobs) ||
	(pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM)) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_564, 
					   "Modified SBM extent records from [%d,%d],[%d,%d] to [%d,%d],[%d,%d].\n"),
				   pXtnt->bsXA[0].bsx_fob_offset,
				   pXtnt->bsXA[0].bsx_vd_blk,
				   pXtnt->bsXA[1].bsx_fob_offset,
				   pXtnt->bsXA[1].bsx_vd_blk,
				   0, pXtnt->bsXA[0].bsx_vd_blk,
				   sbmFobs, XTNT_TERM);
	if (status != SUCCESS) {
	    return status;
	}

	pXtnt->bsXA[0].bsx_fob_offset = 0;
	pXtnt->bsXA[1].bsx_fob_offset = sbmFobs;
	pXtnt->bsXA[1].bsx_vd_blk = XTNT_TERM;
	modified = TRUE;
    }

    /*
     * FUTURE - Currently the SBM should have only 1 xtnt.  
     * WARNING: this will change in the future.
     */

    if ((pXtnt->mcellCnt != 1) ||
	(pXtnt->chainMCId.volume != 0)   ||
	(pXtnt->chainMCId.page != 0)     ||
	(pXtnt->chainMCId.cell != 0)) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_565,
					   "Modified RBMT SBM mcell's chain pointer from (%d,%ld,%d) to (%d,%ld,%d).\n"),
				   pXtnt->chainMCId.volume,
				   pXtnt->chainMCId.page,
				   pXtnt->chainMCId.cell,
				   0, 0, 0);
	if (status != SUCCESS) {
	    return status;
	}

	ZERO_MCID(pXtnt->chainMCId);
	pXtnt->mcellCnt = 1;
	modified = TRUE;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should have no more records.
     */
    if (pRecord->type != BSR_NIL) {
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_567, 
					   "Deleted records from RBMT SBM mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_568, 
			 "Found corruption in RBMT SBM mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_569,
					   "Modified RBMT SBM mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_sbm_mcell */


/*
 * Function Name: correct_rbmt_root_tag_mcell
 *
 * Description: This function will check mcell 2 of the RBMT. It will fix 
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     domain: The domain we are working on.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters:
 *     rootTagVolume: Does this volume have a root tag file?
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
static int 
correct_rbmt_root_tag_mcell(pageT *pPage, 
			    domainT *domain,
			    volNumT volNum, 
			    volNumT *rootTagVolume)
{
    char   *funcName = "correct_rbmt_root_tag_mcell";
    int    status;
    int    modified;   /* Has the page has been modified. */
    cellNumT mcellNum;
    tagNumT  tagNum;
    int      reqServices;
    bsMPgT   *RBMTpage;  /* The current RBMT page we are working on */
    bsMCT    *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT    *pRecord;   /* The current mcell record we are working on */
    bsBfAttrT *pBSRAttr;
    bsXtntRT  *pXtnt;
    lbnNumT   lbn;
    /*
     * Init variables
     */
    modified = FALSE;
    RBMTpage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_RTAG;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    pMcell   = &(RBMTpage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));
    *rootTagVolume = FALSE;
    reqServices = 0;

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_570, 
					   "Modified RBMT root tag mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /*
     * This field is set to 0 or DEF_SVC_CLASS.  It is set to '0' if
     * the volume was added via 'mkfdmn', and set to DEF_SVC_CLASS if
     * added via 'addvol'.  We have no way to know which way the
     * volume was added. We keep the value as DEF_SVC_CLASS if set to
     * that, otherwise we set it to 0.
     */
    if (DEF_SVC_CLASS == pBSRAttr->reqServices) {
	reqServices = DEF_SVC_CLASS;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 2, reqServices);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS) {
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */

    /*	
     * If xCnt = 1, then roottag not on volume.
     */
    if (pXtnt->xCnt != 2) {
        *rootTagVolume = FALSE;

        if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	    (pXtnt->bsXA[0].bsx_vd_blk != XTNT_TERM)) {
	    pageT *pTempPage;

	    /*
	     * We have an extent record, but xCnt says we shouldn't.
	     */

	    lbn = pXtnt->bsXA[0].bsx_vd_blk;

	    status = read_page_from_cache(volNum, lbn, &pTempPage);
	    if (status != SUCCESS) {
	        return status;
	    }

	    status = validate_tag_file_page(domain, pTempPage, 0, 
					    *rootTagVolume);
	    if (status != SUCCESS) {
	        /*
		 * This is not a tag file page, extent is corrupt..
		 */
		pXtnt->bsXA[0].bsx_fob_offset = 0;
		pXtnt->bsXA[0].bsx_vd_blk = XTNT_TERM;
		modified = TRUE;
	    } else {
	        /*
		 * xCnt is corrupt.
		 */
	        pXtnt->xCnt = 2;
		*rootTagVolume = TRUE;
	    }
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_580, 
					       "Modified the root tag extent map.\n"));
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = TRUE;	

	    status = release_page_to_cache(volNum, lbn, pTempPage, 
					   FALSE, FALSE);
	    if (status != SUCCESS) {
	        return status;
	    }
        }
    } else {  /* xCnt == 2 */
	pageT *pTempPage;

        *rootTagVolume = TRUE;

	/*
	 * Check to see if the page is actually a tag page.
	 */

	lbn = pXtnt->bsXA[0].bsx_vd_blk;

	status = read_page_from_cache(volNum, lbn, &pTempPage);
	if (status != SUCCESS) {
	    return status;
	}

	status = validate_tag_file_page(domain, pTempPage, 0, *rootTagVolume);
	if (status != SUCCESS) {
	    /*
	     * This is not a tag file page, xCnt is corrupt..
	     */
	    *rootTagVolume = FALSE;
	    pXtnt->xCnt = 1;

	    pXtnt->bsXA[0].bsx_fob_offset = 0;
	    pXtnt->bsXA[0].bsx_vd_blk = XTNT_TERM;
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_580, 
					       "Modified the root tag extent map.\n"));
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = TRUE;

	} else {

	    /* this is a tag file, check over the extent info */

	    if (pXtnt->bsXA[0].bsx_fob_offset != NULL ||
		pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM) {

		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_967, 
						   "Modified rootTag xtnt recs from [%d,%d],[%d,%d] to [%d,%d],[%d,%d].\n"),
					   pXtnt->bsXA[0].bsx_fob_offset,
					   pXtnt->bsXA[0].bsx_vd_blk,
					   pXtnt->bsXA[1].bsx_fob_offset,
					   pXtnt->bsXA[1].bsx_vd_blk,
					   0, pXtnt->bsXA[0].bsx_vd_blk,
					   pXtnt->bsXA[1].bsx_fob_offset,
					   XTNT_TERM);
		if (status != SUCCESS) {
		    return status;
		}

		pXtnt->bsXA[0].bsx_fob_offset = 0;
		pXtnt->bsXA[1].bsx_vd_blk = XTNT_TERM;
		modified = TRUE;
	    }
	}

	status = release_page_to_cache(volNum, lbn, pTempPage, 
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    } 

    /*
     * linkage will be checked later in the program.
     */

    /*
     * Set pRecord to next record of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should have no more records.
     */
    if (pRecord->type != BSR_NIL) {
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_582, 
					   "Deleted records from RBMT root tag mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_583,
			 "Found corruption in RBMT root tag mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_584, 
					   "Modified RBMT root tag mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_root_tag_mcell */


/*	
 * Function Name: correct_rbmt_log_mcell
 *
 * Description: This function will check mcell 3 of the RBMT. It will fix
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     logVolume: A link list of volumes with log extents.
 *
 * Output parameters: 
 *     logVolume: A link list of volumes with log extents.
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
static int 
correct_rbmt_log_mcell(pageT *pPage, 
		       volNumT volNum, 
		       domainT *domain, 
		       logVolT **logVolume)
{
    char      *funcName = "correct_rbmt_log_mcell";
    bsMPgT    *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT     *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT     *pRecord;   /* The current mcell record we are working on */
    int       modified;   /* Flag if the page has been modified. */
    cellNumT  mcellNum;
    tagNumT   tagNum;
    int       status;
    int       logOnVolume;
    int       reqServices;
    bsBfAttrT *pBSRAttr;
    bsXtntRT  *pXtnt;
    lbnNumT   lbn;

    /*
     * Init variables
     */
    modified = FALSE;
    logOnVolume = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_LOG;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_585,
					   "Modified RBMT log mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /*
     * Starting with wildcat this field is now set to LOG_SVC_CLASS
     */
    reqServices = 0;
    if (pBSRAttr->reqServices == LOG_SVC_CLASS) {
        reqServices = LOG_SVC_CLASS;
    }
   
    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 1, reqServices);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    logOnVolume = TRUE;

    if (pXtnt->xCnt == 1) {
        logOnVolume = FALSE;

        /*
         * verify we don't have an extent.
         */
        if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	    (pXtnt->bsXA[0].bsx_vd_blk != XTNT_TERM)) {

	    pageT  *pTempPage;

	    /* 
	     * We have an extent record, but xCnt says we shouldn't.	
	     * Either xCnt is wrong or the extent is wrong.
	     * So lets check the page the xtnt points to and see if it 
	     * is a log page.	
	     */
	    lbn = pXtnt->bsXA[0].bsx_vd_blk;

	    status = read_page_from_cache(volNum, lbn, &pTempPage);
	    if (status != SUCCESS) {
	        return status;
	    }

	    status = validate_log_file_page(domain, pTempPage);
	    if (status != SUCCESS) {
	        /*
		 * This is not a log file page, extent is corrupt..
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_595, 
						   "Truncated the log file extent.\n"));
		if (status != SUCCESS) {
		    return status;
		}
		pXtnt->bsXA[0].bsx_fob_offset = 0;
		pXtnt->bsXA[0].bsx_vd_blk  = XTNT_TERM;
	    } else {
	        /*
		 * xCnt is corrupt.
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_596, 
						   "Modified the log file extent count from %d to %d.\n"),
					   pXtnt->xCnt, 2);
		if (status != SUCCESS) {
		    return status;
		}
	        pXtnt->xCnt = 2;
		logOnVolume = TRUE;
	    }
	    modified = TRUE;	

	    status = release_page_to_cache(volNum, lbn, pTempPage, 
					   FALSE, FALSE);
	    if (status != SUCCESS) {
	        return status;
	    }
	} /* end if we have an extent */
    } /* end if pXtnt->xCnt == 1 */

    if (pXtnt->xCnt == 1) {
        /*
	 * we could have had it change in the last if block, so check again.
	 */
        logOnVolume = FALSE;

        if ((pXtnt->mcellCnt != 1) ||
	    (pXtnt->chainMCId.volume != 0)   ||
	    (pXtnt->chainMCId.page != 0)     ||
	    (pXtnt->chainMCId.cell != 0)) {
	    /*	
	     * As we don't have any extents we shouldn't have a chain.
	     */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_597, 
					       "Modified RBMT log mcell's chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
				       pXtnt->chainMCId.volume,
				       pXtnt->chainMCId.page,
				       pXtnt->chainMCId.cell,
				       0, 0, 0);
	    if (status != SUCCESS) {
		return status;
	    }

	    ZERO_MCID(pXtnt->chainMCId);
	    pXtnt->mcellCnt = 1;
	    modified = TRUE;
	}
    } else if ((pXtnt->xCnt != 1) &&  
	       (pXtnt->mcellCnt == 1)) {
        /*
	 * We have a log file with a single extent.
	 */

        if ((pXtnt->chainMCId.volume != 0)   ||
	    (pXtnt->chainMCId.page != 0) ||
	    (pXtnt->chainMCId.cell != 0)) {
	    /*	
	     * Either the chain is incorrect or mcellCnt is.
	     * As we don't have the RBMT extents yet we can only check
	     * the chain if it points to this page.  We can check if the
	     * volNum is correct.
	     */
	    if ((pXtnt->chainMCId.volume != volNum) ||
		(pXtnt->chainMCId.cell > BSPG_CELLS)) {
	        /*
		 * We have a corruption in the chain, so assume chain is
		 * the corruption.
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_597, 
						   "Modified RBMT log mcell's chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   pXtnt->chainMCId.volume,
					   pXtnt->chainMCId.page,
					   pXtnt->chainMCId.cell,
					   0, 0, 0);
		if (status != SUCCESS) {
		    return status;
		}
		ZERO_MCID(pXtnt->chainMCId);
		modified = TRUE;
	    } else {
	        bsMCT  *pTempMcell;
		pageT  *pTempPage;
		bsMPgT *pTempBMTPage;
		lbnNumT tmpLbn;
		volNumT tmpVol;

		if (pXtnt->chainMCId.page == 0) {
		    /*
		     * The mcell falls on this page so lets check it.
		     */
		    pTempMcell = &(pBMTPage->bsMCA[pXtnt->chainMCId.cell]);
		} else {
		    fobtoLBNT *rbmtLbn;
		    xtntNumT size;

		    /*
		     * mcell falls on another RBMT page.
		     */
		    rbmtLbn = domain->volumes[volNum].volRbmt->rbmtLBN;
		    size    = domain->volumes[volNum].volRbmt->rbmtLBNSize;

		    status = convert_page_to_lbn(rbmtLbn,
						 size,
						 pXtnt->chainMCId.page,
						 &tmpLbn, 
						 &tmpVol);
		    if (status != SUCCESS) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_598,
					 "Can't convert RBMT page %ld to LBN\n"), 
				 pXtnt->chainMCId.page);
			return FAILURE;
		    }
	
		    status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		    if (status != SUCCESS) {
		        return status;
		    }
		    pTempBMTPage = (bsMPgT *)pPage->pageBuf;
		    pTempMcell   = &(pTempBMTPage->bsMCA[pXtnt->chainMCId.cell]);		
		} /* end if on a different page */

		if (pTempMcell->mcTag.tag_num != tagNum) {
		    /*
		     * mcell does not belong to log file, So chain is corrupt.
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_597, 
						       "Modified RBMT log mcell's chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					       pXtnt->chainMCId.volume,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       0, 0, 0);
		    if (status != SUCCESS) {
			return status;
		    }
		    ZERO_MCID(pXtnt->chainMCId);
		    modified = TRUE;
		} else {
		    /*
		     * The mcell pointed at by the chain is part of the log.
		     * Set it to 2.  Later when we follow the chain we will
		     * verify this value.
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_599, 
						       "Modified RBMT log mcell's mcellCnt from %d to %d.\n"),
					       pXtnt->mcellCnt,
					       2);
		    if (status != SUCCESS) {
			return status;
		    }
		    pXtnt->mcellCnt = 2;
		    modified = TRUE;
		} /* end if tag.tag_num incorrect */
		
		if (pXtnt->chainMCId.page != 0) {
		    status = release_page_to_cache(tmpVol, tmpLbn,
						   pTempPage, FALSE, FALSE);
		    if (status != SUCCESS) {
		        return status;
		    }
		} /* end if page != 0 */
	    } /* end if chain falls on this page */
	} /* end if valid chain */
    } /* if/else xcnt */

    if (pXtnt->xCnt == 2 &&
	(pXtnt->bsXA[0].bsx_fob_offset != NULL ||
	 pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM)) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_968, 
					   "Modified Log xtnt recs from [%d,%d],[%d,%d] to [%d,%d],[%d,%d].\n"),
				   pXtnt->bsXA[0].bsx_fob_offset,
				   pXtnt->bsXA[0].bsx_vd_blk,
				   pXtnt->bsXA[1].bsx_fob_offset,
				   pXtnt->bsXA[1].bsx_vd_blk,
				   0, pXtnt->bsXA[0].bsx_vd_blk,
				   pXtnt->bsXA[1].bsx_fob_offset,
				   XTNT_TERM);
	if (status != SUCCESS) {
	    return status;
	}

	pXtnt->bsXA[0].bsx_fob_offset = 0;
	pXtnt->bsXA[1].bsx_vd_blk = XTNT_TERM;
	modified = TRUE;
    }
    
    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should have no more records.
     */
    if (pRecord->type != BSR_NIL) {
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_603, 
					   "Deleted records from RBMT log mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * If there are log extents on this volume then add an entry to
     * our list. 
     * 
     * Note: if there are log extents that indicates that there was
     * once a log on this volume.  It may or may not be that that log
     * is no longer valid (ie: was moved) but we use this as a cross
     * check with "mattrVol", which inidicates which volume contains
     * the valid BSR_DMN_MATTR record.  The valid mattr record is
     * always co-located with the valid log.
     */
    if (logOnVolume == TRUE) {
        logVolT *tmp;
        logVolT *head;

	tmp = (logVolT *)ffd_malloc(sizeof(logVolT));
	tmp->volume = volNum;
	tmp->next = NULL;

	if (*logVolume == NULL) {
	    *logVolume = tmp;
	} else {
	    head = *logVolume;
	    while (head->next != NULL) {
	        head = head->next;
	    }
	    head->next = tmp;
	}
    } /* end if log extents exist on this volume */
    
    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_604, 
			 "Found corruption in RBMT log mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_605, 
					   "Modified RBMT log mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_log_mcell */


/*
 * Function Name: correct_rbmt_primary_bmt
 *
 * Description: This function will check mcell 4 defines the BMT.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
static int 
correct_rbmt_primary_bmt(pageT *pPage, 
			 volNumT volNum)
{
    char     *funcName = "correct_rbmt_primary_bmt";
    bsMPgT   *pBMTPage;/* The current RBMT page we are working on */
    bsMCT    *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT    *pRecord; /* The Current mcell record we are working on */
    int      modified; /* Flag to let us know if the page has been modified. */
    int      status;
    cellNumT mcellNum;
    tagNumT  tagNum;
    bsXtntRT *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_BMT;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_606,
					   "Modified RBMT primary BMT mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 2, 0);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (pXtnt->xCnt != 2) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_617, 
					   "Modified RBMT primary BMT mcell's xCnt from %d to %d.\n"),
				   pXtnt->xCnt, 2);
	if (status != SUCCESS) {
	    return status;
	}
        pXtnt->xCnt = 2;
	modified = TRUE;
    }

    /*
     * The BMT extents are defined in this cell (one extent), then as
     * a chain of extra extents which begin in cell RBMT_BMT_EXT.  We
     * verify the linkasge here.
     */
    if (pXtnt->chainMCId.volume != volNum ||
	pXtnt->chainMCId.page != 0 ||
	pXtnt->chainMCId.cell != RBMT_BMT_EXT) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_618, 
					   "Modified RBMT primary BMT mcell's chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
				   pXtnt->chainMCId.volume,
				   pXtnt->chainMCId.page,
				   pXtnt->chainMCId.cell,
				   volNum, 0, 7);
	if (status != SUCCESS) {
	    return status;
	}
        pXtnt->chainMCId.volume = volNum;
	pXtnt->chainMCId.page = 0;
	pXtnt->chainMCId.cell = 7;
	modified = TRUE;
    }

    /*
     * check/fix the BMT primary extent
     */
    if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	(pXtnt->bsXA[0].bsx_vd_blk != BMT_START_BLK) ||
	(pXtnt->bsXA[1].bsx_fob_offset != ADVFS_METADATA_PGSZ_IN_FOBS) ||
	(pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM)) {

        pXtnt->bsXA[0].bsx_fob_offset = 0;
	pXtnt->bsXA[0].bsx_vd_blk  = BMT_START_BLK;
	pXtnt->bsXA[1].bsx_fob_offset = ADVFS_METADATA_PGSZ_IN_FOBS;
	pXtnt->bsXA[1].bsx_vd_blk  = XTNT_TERM;

	status = add_fixed_message(&(pPage->messages), RBMT,
		 catgets(_m_catd, S_FSCK_1, FSCK_619, 
			 "Modified RBMT primary BMT mcell's extents.\n"));
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should have no more records.
     */
    if (pRecord->type != BSR_NIL) {
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_621, 
					   "Deleted records from RBMT primary BMT mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_622, 
			 "Found corruption in RBMT primary BMT mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_623,
					   "Modified RBMT primary BMT mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_primary_BMT */



/*
 * Function Name:correct_rbmt_misc_mcell
 *
 * Description: This function will check mcell 5 of the RBMT. It will fix
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     pageNum: The page number in the RBMT this belongs to.
 *     domain: The domain we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
static int 
correct_rbmt_misc_mcell(pageT *pPage, 
			volNumT volNum,
			pageNumT pageNum, 
			domainT *domain)
{
    char   *funcName = "correct_rbmt_misc_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT  *pRecord; /* The Current mcell record we are working on */
    int    modified; /* Flag to let us know if the page has been modified. */
    cellNumT mcellNum;
    tagNumT  tagNum;
    int      status;
    int      foundCorruption;
    volNumT  chainVol;
    pageNumT chainPage;
    cellNumT chainMcell;
    bsXtntRT *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_MISC;
    tagNum   = RSVD_TAGNUM_GEN(volNum, mcellNum);
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_645,
					   "Modified RBMT misc mcell's tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_attr(pPage, pRecord, mcellNum, 2, 0);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     *
     * NOTE : Special case on this mcell's record size. 
     */
    status = correct_rbmt_xtnts_defaults(pPage, pRecord, mcellNum);
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */

    if (pXtnt->xCnt != 2) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_656, 
					   "Modified RBMT misc mcell's xCnt from %d to %d.\n"),
				   pXtnt->xCnt, 2);
	if (status != SUCCESS) {
	    return status;
	}
        pXtnt->xCnt = 2;
	modified = TRUE;
    }

    /*
     * Starting with wildcat and log isolation we can now have mcells
     * in the chain.  If the chain is valid then the mcells in it 
     * should be of tagNum 'misc'.  This mcell could be on a different
     * page so we might need to load a second page.
     */
    foundCorruption = FALSE;
    chainVol   = 0; /* Set to empty chain value */
    chainPage  = 0; /* Set to empty chain value */
    chainMcell = 0; /* Set to empty chain value */

    if ((pXtnt->chainMCId.page == 0) &&	(pXtnt->chainMCId.cell == 0)) {
	if (pXtnt->chainMCId.volume != 0) {
	    /*
	     * page and cell both are 0.  This means chain volume must
	     * be zero.
	     */
	    foundCorruption = TRUE;
	}
    } else {
	/*
	 * Else page or cell is set to something other than 0
	 */
	xtntNumT size;
	lbnNumT tmpLbn;
	volNumT tmpVol;
	tagNumT chainTagNum;
	fobtoLBNT *rbmtLbn;
	bsMCT  *pTempMcell;
	pageT  *pTempPage;
	bsMPgT *pTempBMTPage;

	/*
	 * Check to see if the cell is in the valid range.
	 */
	if (pXtnt->chainMCId.cell >= RBMT_RSVD_CELL) {
	    /*
	     * mcell chain is invalid.
	     */
	    foundCorruption = TRUE;
	} 

	if (pageNum != pXtnt->chainMCId.page) {
	    /*
	     * Now check to see if the page is valid.
	     */
	    rbmtLbn = domain->volumes[volNum].volRbmt->rbmtLBN;
	    size    = domain->volumes[volNum].volRbmt->rbmtLBNSize;
	    
	    status = convert_page_to_lbn(rbmtLbn, size, pXtnt->chainMCId.page,
					 &tmpLbn, &tmpVol);
	    if (status != SUCCESS) {
		/*
		 * Page is corrupt as it doesn't fall in the RBMT.
		 * If this happens an info/debug message is printed
		 * by convert_page_to_lbn
		 */
		foundCorruption = TRUE;
	    }
	}
	    
	/*
	 * Now if page and cell are valid, check if mcell belongs
	 * to the 'misc' chain.
	 */
	if (foundCorruption == FALSE) {
	    if (pageNum == pXtnt->chainMCId.page) {
		/*
		 * The mcell falls on this page so lets check it.
		 */
		pTempPage = pPage;
	    } else {
		/*
		 * mcell falls on another RBMT page.
		 */
		status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* end if/else status not SUCCESS */

	    pTempBMTPage = (bsMPgT *)pTempPage->pageBuf;
	    pTempMcell   = &(pTempBMTPage->bsMCA[pXtnt->chainMCId.cell]);
	    chainTagNum  = pTempMcell->mcTag.tag_num;

	    if (tagNum != chainTagNum) {
		/*
		 * The chainTag does not match this tag.
		 * Two possible corruptions:
		 *  A) The chain 
		 *  B) the chainTagNum.
		 *
		 * For now we will truncate the chain, FUTURE we might
		 * want to see if we can check/fix the chainTagNum.
		 */
		foundCorruption = TRUE;
	    } else {
		if (volNum != pXtnt->chainMCId.volume) {
		    /*
		     * The chainVd is corrupt, it MUST be on this 
		     * volume.  Make sure to set page and cell.
		     */
		    chainVol   = volNum;
		    chainPage  = pXtnt->chainMCId.page;
		    chainMcell = pXtnt->chainMCId.cell;
		    foundCorruption = TRUE;
		} /* end if chainMCId.volume is corrupt */
	    } /* end if/else tagNum doesn't match */

	    if (pageNum != pXtnt->chainMCId.page) {
		/*
		 * Need to release the second page.
		 */
		status = release_page_to_cache(tmpVol, tmpLbn,
					       pTempPage, FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* end if on different pages */
	} /* end if not corrupt */
    } /* end if/else page and cell equal 0 */

    if (foundCorruption == TRUE) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_657, 
					   "Modified RBMT misc mcell's chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
				   pXtnt->chainMCId.volume,
				   pXtnt->chainMCId.page,
				   pXtnt->chainMCId.cell,
				   chainVol, chainPage, chainMcell);
	if (status != SUCCESS) {
	    return status;
	}
	pXtnt->chainMCId.volume = chainVol;
	pXtnt->chainMCId.page = chainPage;
	pXtnt->chainMCId.cell = chainMcell;
	modified = TRUE;
	foundCorruption = FALSE;
    }

    /*
     * First extent reserves the superblock
     */
    if ((pXtnt->bsXA[0].bsx_fob_offset != 0) ||
	(pXtnt->bsXA[0].bsx_vd_blk != 0)  ||
	(pXtnt->bsXA[1].bsx_fob_offset != ADVFS_RESERVED_BLKS) ||
	(pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM)) {

        pXtnt->bsXA[0].bsx_fob_offset = 0;
	pXtnt->bsXA[0].bsx_vd_blk  = 0;
	pXtnt->bsXA[1].bsx_fob_offset = ADVFS_RESERVED_BLKS;
	pXtnt->bsXA[1].bsx_vd_blk  = XTNT_TERM;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_658,
					   "Modified RBMT misc mcell's extents.\n"));
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = NEXT_MREC(pRecord);

    /*
     * This mcell should have no more records.
     */
    if (pRecord->type != BSR_NIL) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_660, 
					   "Deleted records from RBMT misc mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_NIL;
	pRecord->bCnt = 0;
	modified = TRUE;
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_661, 
			 "Found corruption in RBMT misc mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_662, 
					   "Modified RBMT misc mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

  return SUCCESS;
} /* end correct_rbmt_misc_mcell */

/*
 * Function Name: correct_rbmt_ext_mcells
 *
 * Description: This function will check/fix BFM_RBMT_EXT mcell & records 
 *              and all additional mcells & records linked thru nextMCId.
 *
 * There are certain RBMT specific mcell records on each volume that
 * have valid content ONLY on the volume that contains the log.  On
 * non-log volumes we just check their type and size, ignoring the
 * contents.
 *
 *	    BSR_DMN_MATTR       (cell 6)
 *	    BSR_DMN_TRANS_ATTR  (cell 6)
 *          BSR_DMN_FREEZE_ATTR (cell 6)
 *          BSR_DMN_NAME        (cell 7)
 *          BSR_RUN_TIMES       (cell 8)
 *
 * The above records always appear in the exact sequence shown.
 *
 * Additional RBMT specific records are present only in certain
 * circumstances and do not fall in a particular sequence.  There
 * should never be more than one of each of these records per RBMT.
 * We call these records FLOATERS.  They are found and corrected in
 * correct_rbmt_floaters().
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     mattrVol: Which volume has current mattr
 *     mattrSeq: Current sequence number of mattr
 *
 * Output parameters: 
 *     mattrVol: Which volume has current mattr
 *     mattrSeq: Current sequence number of mattr
 *
 * Returns: SUCCESS, FIXED
 */
static int 
correct_rbmt_ext_mcells(pageT *pPage, 
			volNumT volNum, 
			domainT *domain, 
			volNumT *mattrVol, 
			tagSeqT *mattrSeq)
{
    char    *funcName = "correct_rbmt_ext_mcells";
    bsMPgT  *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT   *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT   *pRecord; /* The Current mcell record we are working on */
    uint32_t linkSegment;
    int     modified; /* Flag to let us know if the page has been modified.*/
    cellNumT mcellNum;
    tagNumT  tagNum;
    int      status;
    volumeT  *volume;
    div_t    logPages;
    int      isLogVol; /* Bool: the current volume contains the log */
    bsDmnAttrT    *pDmnAttr;
    bsDmnMAttrT   *pDmnMAttr;
    delLinkRT     *delLink;

    /*
     * Init variables
     */
    volume = &(domain->volumes[volNum]);
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = RBMT_RBMT_EXT;
    tagNum   = RSVD_TAGNUM_GEN(volNum, RBMT_RBMT);
    pMcell = &(pBMTPage->bsMCA[mcellNum]);
    pRecord = (bsMRT *)pMcell->bsMR0;
    linkSegment = 1;
    modified = FALSE;
    isLogVol = FALSE;

    /* this is the RBMT_EXT record, be sure that tag agrees with that */

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_624, 
					   "Modified RBMT mcell %d tag number from %ld to %ld.\n"),
				   pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /* check/fix the (root) fileset tag number */

    if (pMcell->mcBfSetTag.tag_num != (tagNumT)-BFM_BFSDIR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_940, 
					   "Modified RBMT EXT mcell's file system tag number from %ld to %ld.\n"),
				   pMcell->mcBfSetTag.tag_num, (tagNumT)-BFM_BFSDIR);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcBfSetTag.tag_num = RSVD_TAGNUM_GEN(volNum, ROOT_FILE_TAG);
	modified = TRUE;
    }

    /* since this is the 1st mcell linked through the RBMT_RBMT mcell
       (cell 0), we expect the link segment to be 1 */

    if (pMcell->mcLinkSegment != linkSegment) {
 	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_949,
					   "Modified RBMT mcell %d linkSegment from %d to %d.\n"),
				   mcellNum, pMcell->mcLinkSegment, linkSegment);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcLinkSegment = linkSegment;
	modified = TRUE;
    }

    /* This mcell always contains BSR_DMN_ATTR as it's first record.
       Check/fix that record here */

    if (pRecord->type != BSR_DMN_ATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_634,
					   "Modified RBMT BSR_DMN_ATTR record's type from %d to BSR_DMN_ATTR.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_ATTR;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsDmnAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_943, 
					   "Modified RBMT BSR_DMN_ATTR record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsDmnAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnAttrT));
	modified = TRUE;
    }

    pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));

    /* check the fileset directory tag */

    if (pDmnAttr->bfSetDirTag.tag_num != pMcell->mcBfSetTag.tag_num) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_641, 
					   "Modified RBMT BSR_DMN_ATTR record's bfSetDirTag from %ld to %ld.\n"),
				   pDmnAttr->bfSetDirTag.tag_num, 
				   pMcell->mcBfSetTag.tag_num);
	if (status != SUCCESS) {
	    return status;
	}
	pDmnAttr->bfSetDirTag = pMcell->mcBfSetTag;
	modified = TRUE;
    }

    /*
     * Check the max volumes number.  This is an ODS so use BS_MAX_VDI
     * not MAX_VOLUMES.
     */
    if (!((pDmnAttr->maxVds == 1) || (pDmnAttr->maxVds == BS_MAX_VDI))){
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_635, 
					   "Modified RBMT BSR_DMN_ATTR record's maxVds from %d to BS_MAX_VDI.\n"),
				   pDmnAttr->maxVds);
	if (status != SUCCESS) {
	    return status;
	}
	pDmnAttr->maxVds = BS_MAX_VDI;
	modified = TRUE;
    }

    pRecord = NEXT_MREC(pRecord);

    /*
     * The next record MUST be the BSR_DMN_MATTR mutable attributes
     * record.
     */

    if (pRecord->type != BSR_DMN_MATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_636,
					   "Modified RBMT BSR_DMN_MATTR record's type from %d to BSR_DMN_MATTR.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_MATTR;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsDmnMAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_944, 
					   "Modified RBMT BSR_DMN_MATTR record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, (sizeof(bsMRT) + sizeof(bsDmnMAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnMAttrT));
	modified = TRUE;
    }

    pDmnMAttr = (bsDmnMAttrT *)((char *)pRecord + sizeof(bsMRT));

    /* The BSR_DMN_MATTR record appears on all volumes, but is valid
       only on the volume that contains the log.  A non zero sequence
       number indicates a valid BSR_DMN_MATTR record. */

    if (pDmnMAttr->seqNum != 0) {

        writemsg(SV_DEBUG | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_601, 
			 "Found the transaction log on volume %s.\n"),
		 domain->volumes[volNum].volName);
	isLogVol = TRUE;

    } else {
        writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_602,
			 "No transaction log on volume %s.\n"),
		 domain->volumes[volNum].volName);
    }

    if (isLogVol) {

	/* need to return the volume number that is holding the log. */

	*mattrVol = BS_BFTAG_VDI(pDmnMAttr->ftxLogTag);
	*mattrSeq = pDmnMAttr->seqNum;

	/* check/fix the ODS version number */

	if (!VERSION_EQ(pDmnMAttr->ODSVersion, domain->ODSVersion)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_966,
					       "Modified RBMT BSR_DMN_MATTR record's ODS version from %d.%d to %d.%d\n"),
				       pDmnMAttr->ODSVersion.odv_major,
				       pDmnMAttr->ODSVersion.odv_minor,
				       domain->ODSVersion.odv_major,
				       domain->ODSVersion.odv_minor);
	    if (status != SUCCESS) {
		return status;
	    }

	    pDmnMAttr->ODSVersion = domain->ODSVersion;
	    modified = TRUE;
	}

	/* check/fix the dma domain state */

	if (pDmnMAttr->bs_dma_dmn_state != 0) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_638, 
					       "Modified RBMT BSR_DMN_MATTR record's bs_dma_dmn_state from %d to %d.\n"),
				       pDmnMAttr->bs_dma_dmn_state, 0);
	    if (status != SUCCESS) {
		return status;
	    }
	    pDmnMAttr->bs_dma_dmn_state = 0;
	    modified = TRUE;
	}

	/*
	 * we have a fileset that was either being deleted, or
	 * corruption saying a fileset is being deleted.  It will
	 * not hurt to set to 0, and let the user reissue the rmfset
	 * command after the domain is repaired.
	 */
	if (pDmnMAttr->delPendingBfSet.tag_num != 0) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_639, 
					       "Modified RBMT BSR_DMN_MATTR record's delPendingBfSet from %ld.%u to %ld.%u.\n"),
				       pDmnMAttr->delPendingBfSet.tag_num,
				       pDmnMAttr->delPendingBfSet.tag_seq,
				       0, 0);
	    if (status != SUCCESS) {
		return status;
	    }
	    pDmnMAttr->delPendingBfSet.tag_num = 0;
	    pDmnMAttr->delPendingBfSet.tag_seq = 0;
	    modified = TRUE;
	}

	/* assure that we are claiming at least the minumum log pages */

	if (pDmnMAttr->ftxLogPgs < BS_MIN_LOG_PGS) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_640, 
					       "Modified RBMT BSR_DMN_MATTR record's ftxLogPgs from %d to %d.\n"),
				       pDmnMAttr->ftxLogPgs, BS_MIN_LOG_PGS);
	    if (status != SUCCESS) {
		return status;
	    }
	    pDmnMAttr->ftxLogPgs = BS_MIN_LOG_PGS;
	    modified = TRUE;
	}

	logPages = div(pDmnMAttr->ftxLogPgs, 4);

	if (logPages.rem != 0) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_640, 
					       "Modified RBMT BSR_DMN_MATTR record's ftxLogPgs from %d to %d.\n"),
				       pDmnMAttr->ftxLogPgs, BS_MIN_LOG_PGS);
	    if (status != SUCCESS) {
		return status;
	    }
	    pDmnMAttr->ftxLogPgs = BS_MIN_LOG_PGS;
	    modified = TRUE;
	}
    }

    pRecord = NEXT_MREC(pRecord);

    /* the next record MUST be a BSR_DMN_TRANS_ATTR record. It is used
       to handle the addvol/rmvols which could be in process when the
       domain crashed. */

    if (pRecord->type != BSR_DMN_TRANS_ATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_637,
					   "Modified RBMT BSR_DMN_TRANS_ATTR record's type from %d to BSR_DMN_TRANS_ATTR.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_TRANS_ATTR;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsDmnTAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_945, 
					   "Modified RBMT BSR_DMN_TRANS_ATTR record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, (sizeof(bsMRT) + sizeof(bsDmnTAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnTAttrT));
	modified = TRUE;
    }

    pRecord = NEXT_MREC(pRecord);

    /* the next record MUST be a BSR_DMN_FREEZE_ATTR record. */

    if (pRecord->type != BSR_DMN_FREEZE_ATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_659,
					   "Modified RBMT BSR_DMN_FREEZE_ATTR record's type from %d to BSR_DMN_FREEZE_ATTR.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_TRANS_ATTR;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT)) != 
	pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_946, 
					   "Modified RBMT BSR_DMN_FREEZE_ATTR record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) +
				    sizeof(bsDmnFreezeAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT));
	modified = TRUE;

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_911,
					   "Corrected domain freeze record.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    pRecord = NEXT_MREC(pRecord);

    /* Any remaining record in this mcell should be a floater. Check
       that and remove non-floaters. */

    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (!IS_RBMT_FLOATER(pRecord->type)) {

	    /* Found an unsupported record. Delete it. */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_959, 
					       "Deleted unknown record type %d from RBMT page %ld mcell %d.\n"),
				       pRecord->type, pBMTPage->bmtPageId, 
				       mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    pRecord->type = BSR_NIL;
	    pRecord->bCnt = 0;
	    modified = TRUE;
	    break;
	}

	pRecord = NEXT_MREC(pRecord);
    }

    /* We're at the end of the RBMT_EXT cell and should be linked to
       the RBMT_EXT1 cell.  Check/fix that and then move on to
       RBMT_EXT1. */

    if (pMcell->mcNextMCId.volume != BS_BFTAG_VDI(pMcell->mcTag) ||
	pMcell->mcNextMCId.page != 0 ||	
	pMcell->mcNextMCId.cell != RBMT_RBMT_EXT1) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_607, 
					   "Modified RBMT mcell %d nextMCId from %d.%ld.%d to %d.%ld.%d\n"),
				   RBMT_RBMT_EXT,
				   pMcell->mcNextMCId.volume,
				   pMcell->mcNextMCId.page,
				   pMcell->mcNextMCId.cell,
				   BS_BFTAG_VDI(pMcell->mcTag), 
				   0,
				   RBMT_RBMT_EXT1);
	if (status != SUCCESS) {
	    return status;
	}

	pMcell->mcNextMCId.volume = BS_BFTAG_VDI(pMcell->mcTag);
	pMcell->mcNextMCId.page = 0;
	pMcell->mcNextMCId.cell = RBMT_RBMT_EXT1;
	modified = TRUE;
    }

    mcellNum = FETCH_MC_CELL(pMcell->mcNextMCId);
    pMcell = &(pBMTPage->bsMCA[mcellNum]);
    pRecord = (bsMRT *)pMcell->bsMR0;
    linkSegment++;

    /* 
     * check/fix the new mcell's tag and linkSegment numbers 
     */

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_624, 
					   "Modified RBMT mcell %d tag number from %ld to %ld.\n"),
				   mcellNum, pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    if (pMcell->mcLinkSegment != linkSegment) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_949,
					   "Modified RBMT mcell %d linkSegment from %d to %d.\n"),
				   mcellNum, pMcell->mcLinkSegment, linkSegment);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcLinkSegment = linkSegment;
	modified = TRUE;
    }

    /* The first record in this cell should always be the BSR_DMN_NAME
       record. */

    if (pRecord->type != BSR_DMN_NAME) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_608,
					   "Modified RBMT BSR_DMN_NAME record's type from %d to BSR_DMN_NAME.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_NAME;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsDmnNameT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_609, 
					   "Modified RBMT BSR_DMN_NAME record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsDmnNameT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnNameT));
	modified = TRUE;
    }

    /* Any remaining record in this mcell should be a floater. Check
       that and remove non-floaters. */

    pRecord = NEXT_MREC(pRecord);

    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (!IS_RBMT_FLOATER(pRecord->type)) {

	    /* Found an unsupported record. Delete it. */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_959, 
					       "Deleted unknown record type %d from RBMT page %ld mcell %d.\n"),
				       pRecord->type, pBMTPage->bmtPageId, 
				       mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    pRecord->type = BSR_NIL;
	    pRecord->bCnt = 0;
	    modified = TRUE;
	    break;
	}

	pRecord = NEXT_MREC(pRecord);
    }

    /* We're at the end of the RBMT_EXT1 cell and should be linked to
       the RBMT_EXT2 cell. */

    if (pMcell->mcNextMCId.volume != BS_BFTAG_VDI(pMcell->mcTag) ||
	pMcell->mcNextMCId.page != 0 ||	
	pMcell->mcNextMCId.cell != RBMT_RBMT_EXT2) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_607, 
					   "Modified RBMT mcell %d nextMCId from %d.%ld.%d to %d.%ld.%d\n"),
				   RBMT_RBMT_EXT1,
				   pMcell->mcNextMCId.volume,
				   pMcell->mcNextMCId.page,
				   pMcell->mcNextMCId.cell,
				   BS_BFTAG_VDI(pMcell->mcTag), 
				   0,
				   RBMT_RBMT_EXT2);
	if (status != SUCCESS) {
	    return status;
	}

	pMcell->mcNextMCId.volume = BS_BFTAG_VDI(pMcell->mcTag);
	pMcell->mcNextMCId.page = 0;
	pMcell->mcNextMCId.cell = RBMT_RBMT_EXT2;
	modified = TRUE;
    }

    mcellNum = FETCH_MC_CELL(pMcell->mcNextMCId);
    pMcell = &(pBMTPage->bsMCA[mcellNum]);
    pRecord = (bsMRT *)pMcell->bsMR0;
    linkSegment++;

    /* 
     * check/fix the new mcell's tag and linkSegment numbers 
     */

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_624, 
					   "Modified RBMT mcell %d tag number from %ld to %ld.\n"),
				   mcellNum, pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    if (pMcell->mcLinkSegment != linkSegment) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_949,
					   "Modified RBMT mcell %d linkSegment from %d to %d.\n"),
				   mcellNum, pMcell->mcLinkSegment, linkSegment);
	if (status != SUCCESS) {
	    return status;
	}
	pMcell->mcLinkSegment = linkSegment;
	modified = TRUE;
    }

    /* The first record in this cell should always be the BSR_RUN_TIMES
       record. */

    if (pRecord->type != BSR_RUN_TIMES) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_610,
					   "Modified RBMT BSR_RUN_TIMES record's type from %d to BSR_DMN_NAME.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_DMN_NAME;
	modified = TRUE;
    }

    /* check/fix the byte count */

    if ((sizeof(bsMRT) + sizeof(bsRunTimesRT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_611, 
					   "Modified RBMT BSR_RUN_TIMES record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsRunTimesRT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsRunTimesRT));
	modified = TRUE;
    }

    pRecord = NEXT_MREC(pRecord);

    /* Any remaining record in this mcell should be a floater. Check
       that and remove non-floaters. */

    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (!IS_RBMT_FLOATER(pRecord->type)) {

	    /* Found an unsupported record. Delete it. */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_959, 
					       "Deleted unknown record type %d from RBMT page %ld mcell %d.\n"),
				       pRecord->type, pBMTPage->bmtPageId, 
				       mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    pRecord->type = BSR_NIL;
	    pRecord->bCnt = 0;
	    modified = TRUE;
	    break;
	}

	pRecord = NEXT_MREC(pRecord);
    }

    /* 
     * We've reached the end of the RBMT_EXT2 mcell and should have
     * now found all the RBMT records we know about (except for
     * floaters, which are checked elsewhere).  If the mcell chain
     * continues, follow it and make any needed corrections.  If we
     * find any non-floater records, blow them away.
     */

    while (FETCH_MC_CELL(pMcell->mcNextMCId) != 0) {

	mcellNum = FETCH_MC_CELL(pMcell->mcNextMCId);
	pMcell = &(pBMTPage->bsMCA[mcellNum]);
	pRecord = (bsMRT *)pMcell->bsMR0;
	linkSegment++;
	
	/* 
	 * check/fix the new mcell's tag and linkSegment numbers 
	 */
	
	if (pMcell->mcTag.tag_num != tagNum) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_624, 
					       "Modified RBMT mcell %d tag number from %ld to %ld.\n"),
				       pMcell->mcTag.tag_num, tagNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    pMcell->mcTag.tag_num = tagNum;
	    pMcell->mcTag.tag_seq = 0;
	    modified = TRUE;
	}

	if (pMcell->mcLinkSegment != linkSegment) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_949,
					       "Modified RBMT mcell %d linkSegment from %d to %d.\n"),
				       mcellNum, pMcell->mcLinkSegment, linkSegment);
	    if (status != SUCCESS) {
		return status;
	    }
	    pMcell->mcLinkSegment = linkSegment;
	    modified = TRUE;
	}

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (!IS_RBMT_FLOATER(pRecord->type)) {

		/* Found an unsupported record. Delete it. */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_959, 
						   "Deleted unknown record type %d from RBMT page %ld mcell %d.\n"),
					   pRecord->type, pBMTPage->bmtPageId, 
					   mcellNum);
		if (status != SUCCESS) {
		    return status;
		}
		pRecord->type = BSR_NIL;
		pRecord->bCnt = 0;
		modified = TRUE;
		break;
	    }

	    pRecord = NEXT_MREC(pRecord);
	} /* end: loop through records */
    } /* end: loop through linked mcells */

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_643, 
			 "Found corruption in RBMT EXT mcell(s).\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_644, 
					   "Modified RBMT EXT mcell(s).\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

  return SUCCESS;

} /* end correct_rbmt_ext_mcells */


/*
 * Function Name:correct_rbmt_other_mcell
 *
 * Description: This function will check/fix the rest of the mcells
 *              in the RBMT, which are either extra extent mcells,
 *              are free, or are floaters.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     mcellNum: Which mcell to work on.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
static int 
correct_rbmt_other_mcell(pageT *pPage, 
			 cellNumT mcellNum, 
			 volNumT volNum, 
			 domainT *domain)
{
    char   *funcName = "correct_rbmt_other_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT  *pRecord; /* The current mcell record we are working on */
    bsXtraXtntRT *pXtra;
    int    modified; /* Flag to let us know if the page has been modified. */
    int    status;
    int    x;
    int    numXtnt;
    tagNumT tagNum;

    /*
     * Init variables
     */
    modified = FALSE;

    /* scan the page for floaters, that is, records that don't live in
       a specific mcell and may or may not be present, and check/fix
       them. */

    status = correct_rbmt_floaters(pPage);
    
    if (status == FIXED) {
	modified = TRUE;
    } else if (status != SUCCESS){
	return status;
    }

    /*
     * If either tag or setTag is 0, then make sure both are 0.
     */

    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * setTag and Tag should be either (0,0) or
     * (-BFM_BFSDIR, [-(volnum * 6) to -((volnum * 6) + 5)]).
     *
     * If setTag is -BFM_BFSDIR and tag is out of range, set tag to
     * belong to the BMT chain.
     *
     * If tag is within legal range and setTag is not -BFM_BFSDIR, set
     * it to -BFM_BFSDIR.
     *
     * Else, make sure tag and setTag are both set to zero.
     */
    if ((pMcell->mcBfSetTag.tag_num == -BFM_BFSDIR) &&
	((pMcell->mcTag.tag_num > -(uint64_t)(volNum * 6)) ||
	 (pMcell->mcTag.tag_num < -((uint64_t)((volNum * 6) + 5))))) {
	/*
	 * Tag number is out of range for this mcell based on 
	 * the volNum.  In almost all cases the tag should 
	 * belong to the BMT chain.  
	 *
	 * FUTURE: walk the chains in the RBMT to verify tagNum.
	 */

	tagNum = -((uint64_t)((volNum * 6) + BFM_BMT));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_663,
					   "Modified RBMT mcell %d's tag number from %ld.%u to %ld.%u.\n"),
				   mcellNum, pMcell->mcTag.tag_num, 
				   pMcell->mcTag.tag_seq,
				   tagNum, pMcell->mcTag.tag_seq);
	if (SUCCESS != status) {
	    return status;
	}
	pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;

    } /* end if setTag is -BFM_BFSDIR */
    else if ((pMcell->mcBfSetTag.tag_num != -BFM_BFSDIR) &&
	     ((pMcell->mcTag.tag_num <= -((uint64_t)(volNum * 6)) &&
	      (pMcell->mcTag.tag_num >= -((uint64_t)((volNum * 6) + 5)))))) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_664, 
					   "Modified RBMT mcell %d's set tag number from %ld.%u to %ld.%u.\n"),
				   mcellNum,
				   pMcell->mcBfSetTag.tag_num, 
				   pMcell->mcBfSetTag.tag_seq,
				   -BFM_BFSDIR, pMcell->mcBfSetTag.tag_seq);
	if (SUCCESS != status) {
	    return status;
	}
	pMcell->mcBfSetTag.tag_num = -BFM_BFSDIR;
	modified = TRUE;
    } /* End if tag is in valid range */
    else if ((pMcell->mcTag.tag_num == 0 && pMcell->mcBfSetTag.tag_num != 0) ||
	     (pMcell->mcTag.tag_num != 0 && pMcell->mcBfSetTag.tag_num == 0)) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_663, 
					   "Modified RBMT mcell %d's tag number from %ld.%u to %ld.%u.\n"),
				   mcellNum, pMcell->mcTag.tag_num, 
				   pMcell->mcTag.tag_seq,
				   0, 0);
	if (status != SUCCESS) {
	    return status;
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_664, 
					   "Modified RBMT mcell %d's set tag number from %ld.%u to %ld.%u.\n"),
				   mcellNum,
				   pMcell->mcBfSetTag.tag_num, 
				   pMcell->mcBfSetTag.tag_seq,
				   0, 0);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = 0;
        pMcell->mcTag.tag_seq = 0;
	pMcell->mcBfSetTag.tag_num = 0;
	pMcell->mcBfSetTag.tag_seq = 0;
	pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
    } else if ((pMcell->mcTag.tag_num != 0) && 
	       (pMcell->mcBfSetTag.tag_num != 0)) {

        /*
	 * Non empty mcell.
	 */

        pRecord = (bsMRT *)pMcell->bsMR0;

        /*
	 * This record could only legally be a BSR_XTRA_XTNTS record
	 * or a floater.  If it's not a floater, it MUST be a
	 * BSR_XTRA_XTNTS record, so force that.
	 */

	if (!IS_RBMT_FLOATER(pRecord->type)) {

	    if (pRecord->type != BSR_XTRA_XTNTS) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_665, 
						   "Modified RBMT mcell %d's record type from %d to BSR_XTRA_XTNTS.\n"),
					   mcellNum, pRecord->type);
		if (status != SUCCESS) {
		    return status;
		}
		pRecord->type = BSR_XTRA_XTNTS;
		modified = TRUE;
	    }

	    if ((sizeof(bsMRT) + sizeof(bsXtraXtntRT)) != pRecord->bCnt) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_666, 
						   "Modified RBMT mcell %d's record bCnt from %d to %d.\n"),
					   mcellNum, pRecord->bCnt, 
					   (sizeof(bsMRT) + sizeof(bsXtraXtntRT)));
		if (status != SUCCESS) {
		    return status;
		}
		pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsXtraXtntRT));
		modified = TRUE;
	    }

	    /*
	     * Need to make sure the xCnt is with in valid range.
	     */
	    if ((pXtra->xCnt < 1) || (pXtra->xCnt > BMT_XTRA_XTNTS)) {
		/*
		 * As we will be be setting the xCnt to the actual number in 
		 * the following block ofcode, all we need to do is set xCnt 
		 * to BMT_XTRA_XTNTS and set the last bsx_vd_blk to 0.
		 */
		pXtra->xCnt = BMT_XTRA_XTNTS;

		/* 
		 * The only valid values for the last vdblk are -1 and 0,
		 * but garbage has been known to show up here when the entry
		 * in the array is not in use.
		 *
		 * If bsx_vd_blk should not be 0 but instead -1, it will be set
		 * correctly in the next block of code.  
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_669, 
						   "Modified RBMT mcell %1$d's xCnt from %2$d to %3$d.\n"),
					   mcellNum, pXtra->xCnt, -1);
		if (status != SUCCESS) {
		    return status;
		}
		pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = 0;
		modified = TRUE;
	    }

	    /*
	     * Check the number of xtnts in record.
	     */
	    numXtnt = pXtra->xCnt;

	    if (pXtra->bsXA[numXtnt - 1].bsx_vd_blk != XTNT_TERM) {
		int x;
		int endExtent;
		int found;
	    
		/*
		 * Init variables
		 */
		found = 0;
		endExtent = 0;
	
		/*
		 * One of two possible error conditions:
		 *
		 * A) xCnt is wrong
		 * B) the extent pointed at by xCnt - 1 is wrong.
		 *
		 * The tool is going to assume that there is only a single
		 * corruption.
		 */
      
		/*
		 * Loop through all possible extents looking for an 'end' case.
		 *
		 * A) two extents in a row with vdblk -1 (hole on end).
		 * B) A bogus FOBnumber
		 * C) A bogus bsx_vd_blk
		 * 
		 * If we can not find one of the above, assume xCnt is correct
		 * and extent is wrong.
		 */

		/*
		 * loop through extents.
		 */
		for (x = 0; (x < BMT_XTRA_XTNTS && FALSE == found); x++) {
		    /*
		     * Order of 'if's  is important
		     */
		    if (pXtra->bsXA[x].bsx_fob_offset >= 
			pXtra->bsXA[x + 1].bsx_fob_offset) {
			/* 
			 * page number does not increment. - valid end case 
			 */
			endExtent = x;
			found = TRUE;
		    } else if (pXtra->bsXA[x].bsx_vd_blk == XTNT_TERM) {
			if (pXtra->bsXA[x + 1].bsx_vd_blk == XTNT_TERM) {
			    /* 
			     * Hole at end - valid end case 
			     */
			    endExtent = x + 1;
			    found = TRUE;
			}
		    } else if ((((pXtra->bsXA[x+1].bsx_fob_offset - pXtra->bsXA[x].bsx_fob_offset) * ADVFS_FOBS_PER_DEV_BSIZE) + pXtra->bsXA[x].bsx_vd_blk) > domain->volumes[volNum].volSize) {
			/*
			 * location on disk falls off end of domain 
			 */
			endExtent = x;
			found = TRUE;
		    } else {
			/*
			 * Any other??
			 */
		    }
		} /* end for loop */

		if (found == FALSE) {
		    /*
		     * Did not find an end, assume xCnt correct.
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_668, 
						       "Modified RBMT mcell %d's bsx_vd_blk from %ld to %ld.\n"),
					       mcellNum,
					       pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk,
					       XTNT_TERM);
		    if (status != SUCCESS) {
			return status;
		    }

		    pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = XTNT_TERM;
		    modified = TRUE;
		} else {
		    /* 
		     * Found an end point 
		     */
		    if (pXtra->bsXA[endExtent].bsx_vd_blk != XTNT_TERM) {
			if (endExtent + 1  < pXtra->xCnt) {
			    /*
			     * Double corruption.
			     */
			    status = add_fixed_message(&(pPage->messages), 
						       RBMT,
						       catgets(_m_catd, S_FSCK_1, FSCK_669, 
							       "Modified RBMT mcell %1$d's xCnt from %2$d to %3$d.\n"),
						       mcellNum, pXtra->xCnt,
						       endExtent + 1);
			    if (status != SUCCESS) {
				return status;
			    }
			    pXtra->xCnt = endExtent + 1;
			}

			status = add_fixed_message(&(pPage->messages), RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_668, 
							   "Modified RBMT mcell %d's bsx_vd_blk from %ld to %ld.\n"),
						   mcellNum,
						   pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk,
						   XTNT_TERM);
			if (status != SUCCESS) {
			    return status;
			}
			pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = XTNT_TERM;
		    } else {
			status = add_fixed_message(&(pPage->messages), RBMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_669,
							   "Modified RBMT mcell %1$d's xCnt from %2$d to %3$d.\n"),
						   mcellNum,
						   pXtra->xCnt, endExtent + 1);

			if (status != SUCCESS) {
			    return status;
			}
			pXtra->xCnt = endExtent + 1;
		    }
		    modified = TRUE;
		}
	    } /* end if bad last Xtnt */

	    numXtnt = pXtra->xCnt;
	    /*
	     * loop through extents 0 to (numXtnt - 2).
	     */
	    for (x = 0; x <= (numXtnt - 2); x++) {
		if (pXtra->bsXA[x].bsx_vd_blk == XTNT_TERM) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_670,
				     "RBMT xtnt map has a hole.\n"));
		    corrupt_exit(NULL);
		}

		/*
		 * bsx_vd_blk must be a multiple of the blocks per
		 * page of a metadata file.  If it's not a multiple,
		 * there's no way to determine the correct bsx_vd_blk
		 * value so we have no option but to corrupt_exit.
		 */
		if (pXtra->bsXA[x].bsx_vd_blk % ADVFS_METADATA_PGSZ_IN_BLKS) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_913,
				     "RBMT xtnt map has LBN %ld which is not on a page boundary.\n"),
			     pXtra->bsXA[x].bsx_vd_blk);
		    corrupt_exit(NULL);
		
		}

		if ((((pXtra->bsXA[x+1].bsx_fob_offset - pXtra->bsXA[x].bsx_fob_offset) * ADVFS_FOBS_PER_DEV_BSIZE) + pXtra->bsXA[x].bsx_vd_blk) > domain->volumes[volNum].volSize) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_671, 
				     "Extent size %d is larger than device size %d.\n"),
			     (((pXtra->bsXA[x+1].bsx_fob_offset - pXtra->bsXA[x].bsx_fob_offset) 
			       * ADVFS_FOBS_PER_DEV_BSIZE) + pXtra->bsXA[x].bsx_vd_blk),
			     domain->volumes[volNum].volSize);
		    corrupt_exit(NULL);
		}

		if (pXtra->bsXA[x].bsx_fob_offset > pXtra->bsXA[x + 1].bsx_fob_offset) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_672, 
				     "RBMT xtnt pages out of order.\n"));
		    corrupt_exit(NULL);
		}
	    }

	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = NEXT_MREC(pRecord);

	    /*
	     * This mcell should have no more records.
	     */
	    if (pRecord->type != BSR_NIL) {
		pRecord->type = BSR_NIL;
		pRecord->bCnt = 0;
		modified = TRUE;
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_673, 
						   "Deleted records from RBMT mcell %d.\n"),
					   mcellNum);
		if (status != SUCCESS) {
		    return status;
		}
	    }
	} /* end: if record is an extra extent */
    } else {
        /*
	 * Empty mcell
	 */
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_674,
			 "Found corruption in RBMT mcell %d.\n"),
		 mcellNum);

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_675, 
					   "Modified RBMT mcell %d.\n"),
				   mcellNum);
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;

} /* end correct_rbmt_other_mcell */


/*
 * Function Name: correct_rbmt_rsvd_mcell
 *
 * Description: The RBMT pages are not mapped using the normal linkage
 *              found in the BMT. Instead, RBMT page 0 is defined by
 *              the extent in cell 0, and if there is a following RBMT
 *              page, it's defined by extra extents located in
 *              RBMT_RSVD_CELL, which is the last cell in RBMT
 *              page 0. This function checks/fixes that cell.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     pageNum: The page number that is being worked on.
 *
 * Output parameters: 
 *     nextPageLBN: LBN of next RBMT page
 *
 * Returns:  SUCCESS, FIXED
 */
static int 
correct_rbmt_rsvd_mcell(pageT *pPage, 
			volNumT volNum, 
			domainT *domain, 
			pageNumT pageNum, 
			lbnNumT *nextPageLBN)
{
    char   *funcName = "correct_rbmt_rsvd_mcell";
    int      modified;   /* Flag if the page has been modified. */
    int      status;
    tagNumT  tagNum;
    cellNumT mcellNum;
    fobNumT  fobNum = PAGE2FOB(pageNum);
    bsMPgT   *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT    *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT    *pRecord;   /* The Current mcell record we are working on */
    bsXtraXtntRT *pXtra;

    /*
     * Init variables
     */
    modified = FALSE;
    mcellNum = RBMT_RSVD_CELL;
    tagNum   = RSVD_TAGNUM_GEN(volNum, RBMT_RBMT);
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
    *nextPageLBN = XTNT_TERM;

    if (pMcell->mcTag.tag_num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_676,
					   "Modified RBMT mcell %d's tag number from %ld to %ld.\n"),
				   mcellNum, pMcell->mcTag.tag_num, tagNum);
	if (status != SUCCESS) {
	    return status;
	}
        pMcell->mcTag.tag_num = tagNum;
        pMcell->mcTag.tag_seq = 0;
	modified = TRUE;
    }

    /*
     * If there is another RBMT page, the "nextMCId" element of this
     * mcell will link to it and it's block address will be found in a
     * BSR_XTRA_XTNTS record within this mcell.
     */

    if ((pMcell->mcNextMCId.page != 0) || (pMcell->mcNextMCId.cell != 0)) {

        /* This record must be of type BSR_XTRA_XTNTS */

        if (pRecord->type != BSR_XTRA_XTNTS) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_665, 
					       "Modified RBMT mcell %d's record type from %d to BSR_XTRA_XTNTS.\n"),
				       mcellNum, pRecord->type);
	    if (status != SUCCESS) {
		return status;
	    }
	    pRecord->type = BSR_XTRA_XTNTS;
	    modified = TRUE;
	}

	if ((sizeof(bsMRT) + sizeof(bsXtraXtntRT)) != pRecord->bCnt) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_666, 
					       "Modified RBMT mcell %d's record bCnt from %d to %d.\n"),
				       mcellNum, pRecord->bCnt, 
				       (sizeof(bsMRT) + sizeof(bsXtraXtntRT)));
	    if (status != SUCCESS) {
		return status;
	    }
	    pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsXtraXtntRT));
	    modified = TRUE;
	}

	/*
	 * Check the number of xtnts in record, should always be 2.
	 */
	if (pXtra->xCnt != 2) {	
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_939, 
					       "Modified RBMT rsvd mcell xCnt from %d to 2.\n"),
				       pXtra->xCnt);
	    if (status != SUCCESS) {
		return status;
	    }
	    pXtra->xCnt = 2;
	    modified = TRUE;
	}

	/* check/fix the extra extents */

	if (pXtra->bsXA[0].bsx_vd_blk > domain->volumes[volNum].volSize) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_678, 
			     "RBMT 'next' xtnt LBN is off the end of the volume.\n"));
	    corrupt_exit(NULL);
	}

	if (pXtra->bsXA[1].bsx_vd_blk != XTNT_TERM) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_961, 
					       "Modified RBMT rsvd mcell xtra xtnt terminator from 0x%lx to -1.\n"),
				       pXtra->bsXA[1].bsx_vd_blk);
	    if (status != SUCCESS) {
		return status;
	    }
	    pXtra->bsXA[1].bsx_vd_blk = XTNT_TERM;
	    modified = TRUE;
	}

	if (pXtra->bsXA[0].bsx_fob_offset != 
	    fobNum + ADVFS_METADATA_PGSZ_IN_FOBS) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_941, 
					       "Modified RBMT rsvd mcell fob_offset[0] from %ld to %ld.\n"),
				       pXtra->bsXA[0].bsx_fob_offset,
				       fobNum + ADVFS_METADATA_PGSZ_IN_FOBS);
	    if (status != SUCCESS) {
		return status;
	    }
	    pXtra->bsXA[0].bsx_fob_offset = 
	      fobNum + ADVFS_METADATA_PGSZ_IN_FOBS;
	    modified = TRUE;
	}

	if (pXtra->bsXA[1].bsx_fob_offset != 
	    fobNum + (2 * ADVFS_METADATA_PGSZ_IN_FOBS)) {

	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_942, 
					       "Modified RBMT rsvd mcell fob_offset[1] from %ld to %ld.\n"),
				       pXtra->bsXA[1].bsx_fob_offset,
				       fobNum + (2 * ADVFS_METADATA_PGSZ_IN_FOBS));
	    if (status != SUCCESS) {
		return status;
	    }
	    pXtra->bsXA[1].bsx_fob_offset = 
	      fobNum + (2 * ADVFS_METADATA_PGSZ_IN_FOBS);
	    modified = TRUE;
	}

	*nextPageLBN = pXtra->bsXA[0].bsx_vd_blk;

        /*
	 * Set pRecord to next record of pMcell.
	 */
	pRecord = NEXT_MREC(pRecord);

	/*
	 * This mcell should have no more records.
	 */
	if (pRecord->type != BSR_NIL) {
	    pRecord->type = BSR_NIL;
	    pRecord->bCnt = 0;
	    modified = TRUE;
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_679,
					       "Deleted records from RBMT reserved mcell.\n"));
	    if (status != SUCCESS) {
		return status;
	    }
	}
    }

    /*
     * We modified this page.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_680,
			 "Found corruption in RBMT reserved mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_681, 
					   "Modified RBMT reserved mcell.\n"));
	if (status != SUCCESS) {
	    return status;
	}
        return FIXED;
    }

  return SUCCESS;

} /* end correct_rbmt_rsvd_mcell */


/*
 * Function Name: correct_rbmt_attr
 *
 * Description: This function will check a RBMT BSR_ATTR record.
 *              It will correct any errors found.  
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *     mcellNum: Which mcell has this record.
 *     dataSafety : What value should this field have.
 *     reqServices: What value should this field have.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 */

static int 
correct_rbmt_attr(pageT *pPage, 
		  bsMRT *pRecord, 
		  cellNumT mcellNum, 
		  int dataSafety,
		  int reqServices)
{
    char *funcName = "correct_rbmt_attr";
    int  modified;   /* Has the record has been modified. */
    int  status;
    bsBfAttrT   *pBSRAttr;

    /*
     * Init variables
     */
    modified = FALSE;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_ATTR
     */

    if (pRecord->type != BSR_ATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_899, 
					   "Modified RBMT mcell %1$d's record type from %2$d to BSR_ATTR.\n"),
				   mcellNum, pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_ATTR;
	modified = TRUE;
    }

    /* verify that the record is the correct size */

    if ((sizeof(bsMRT) + sizeof(bsBfAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_962, 
					   "Modified RBMT mcell %d's record byte count from %d to %d.\n"),
				   mcellNum, pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsBfAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsBfAttrT));
	modified = TRUE;
    }

    /*
     * Check each field in BSR_ATTR looking for corruption:
     */
    if (pBSRAttr->state != BSRA_VALID) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_901, 
					   "Modified RBMT mcell %1$d's state from %2$d to %3$d.\n"),
				   mcellNum, pBSRAttr->state, BSRA_VALID);
	if (status != SUCCESS) {
	    return status;
	}
        pBSRAttr->state = BSRA_VALID;
	modified = TRUE;
    }

    if (pBSRAttr->bfPgSz != ADVFS_METADATA_PGSZ_IN_BLKS) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_902, 
					   "Modified RBMT mcell %1$d's bfPgSz from %2$d to %3$d.\n"), 
				   mcellNum, pBSRAttr->bfPgSz,
				   ADVFS_METADATA_PGSZ_IN_BLKS);
	if (status != SUCCESS) {
	    return status;
	}
        pBSRAttr->bfPgSz = ADVFS_METADATA_PGSZ_IN_BLKS;
	modified = TRUE;
    }

    if (reqServices != pBSRAttr->reqServices) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_898, 
					   "Modified RBMT mcell %1$d's reqServices from %2$d to %3$d.\n"),
				   mcellNum, pBSRAttr->reqServices, reqServices);
	if (status != SUCCESS) {
	    return status;
	}
        pBSRAttr->reqServices = reqServices;
	modified = TRUE;
    }

    if ((pBSRAttr->transitionId != 0) ||
	(pBSRAttr->bfat_del_child_cnt != 0) ||
	(pBSRAttr->bfat_orig_file_size != 0)) {
        /*
	 * These values must be 0.
	 */
        pBSRAttr->transitionId = 0;
        pBSRAttr->bfat_del_child_cnt = 0;
        pBSRAttr->bfat_orig_file_size = 0;

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_904, 
					   "Modified RBMT mcell %d, transition and snap fields.\n"),
				   mcellNum);
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }

} /* end correct_rbmt_attr */


/*
 * Function Name: correct_rbmt_xtnts_defaults
 *
 * Description: This function will check a RBMT BSR_XTNT record.
 *              It will correct any errors found.  
 *
 * Input parameters:
 *     pPage:	  The page on which the record is found.
 *     pRecord:	  The Smart Store record to check.
 *     mcellNum:  Which mcell has this record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 */

static int 
correct_rbmt_xtnts_defaults(pageT *pPage, 
			    bsMRT *pRecord, 
			    cellNumT mcellNum)
{
    char *funcName = "correct_rbmt_xtnts_defaults";
    int  modified;   /* Has the record has been modified. */
    int  status;
    int x;
    int byteCount = sizeof(bsXtntRT) + sizeof(bsMRT);

    bsXtntRT *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    if (pRecord->type != BSR_XTNTS) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_906,
					   "Modified RBMT mcell %1$d's record type from %2$d to BSR_XTNTS.\n"),
				   mcellNum, pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_XTNTS;
	modified = TRUE;
    }

    if (pRecord->bCnt != byteCount) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_962, 
					   "Modified RBMT mcell %d's record byte count from %d to %d.\n"),
				   mcellNum, pRecord->bCnt, byteCount);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->bCnt = byteCount;
	modified = TRUE;
    }

    /*
     * Rest of record will be 0.
     */
    if ((pXtnt->rsvd1 != 0) || (pXtnt->rsvd2 != 0)) {
	pXtnt->rsvd1 = 0;
	pXtnt->rsvd2 = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_903,
					   "Modified RBMT mcell %d, reset reserved fields.\n"),
				   mcellNum);
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }
	  
    /*
     * The deferred delete links in the rbmt will always be empty.
     */
    if ((pXtnt->delLink.nextMCId.page != 0) ||
	(pXtnt->delLink.nextMCId.cell != 0) ||
	(pXtnt->delLink.prevMCId.page != 0) ||
	(pXtnt->delLink.prevMCId.cell != 0) ||
	(pXtnt->delRst.mcid.page != 0)      ||
	(pXtnt->delRst.mcid.cell != 0)      ||
	(pXtnt->delRst.mcid.volume != 0)    ||
	(pXtnt->delRst.xtntIndex != 0)      ||
	(pXtnt->delRst.drtFobOffset != 0)   ||
	(pXtnt->delRst.drtQuotaBlks != 0)) {

        pXtnt->delLink.nextMCId.page = 0; 
	pXtnt->delLink.nextMCId.cell = 0;
	pXtnt->delLink.prevMCId.page = 0;
	pXtnt->delLink.prevMCId.cell = 0;

	ZERO_MCID(pXtnt->delRst.mcid);      

	pXtnt->delRst.xtntIndex = 0;     
	pXtnt->delRst.drtFobOffset = 0;        
	pXtnt->delRst.drtQuotaBlks = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_907, 
					   "Modified RBMT mcell %d, reset deferred delete links.\n"),
				   mcellNum);
	if (status != SUCCESS) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * bsx_vd_blk must either be -1 or a multiple of the number of
     * blocks in a metadata page (all metadata pages are created on
     * metadata page boundaries).
     *
     * Set vdBlk to "-1" hole if not.
     */
    for (x = 0; x < (pXtnt->xCnt - 1) && x < (BMT_XTNTS - 1); x++) { 

	if ((pXtnt->bsXA[x].bsx_vd_blk != XTNT_TERM) &&
	    (pXtnt->bsXA[x].bsx_vd_blk % ADVFS_METADATA_PGSZ_IN_BLKS)) {

	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_668, 
					       "Modified RBMT mcell %d's bsx_vd_blk from %ld to %ld.\n"),
				       mcellNum, pXtnt->bsXA[x].bsx_vd_blk, -1);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtnt->bsXA[x].bsx_vd_blk = -1;
        }
    } /* end extent loop */

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }
} /* correct_rbmt_xtnts_defaults */


/*
 * Function Name: correct_rbmt_vd_attr
 *
 * Description: This function will check an RBMT BSR_VD_ATTR record
 *              and correct any errors found.  
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 */

static int 
correct_rbmt_vd_attr(domainT *domain, 
		     volNumT volNum, 
		     pageT *pPage, 
		     bsMRT *pRecord)
{
    char    *funcName = "correct_rbmt_vd_attr";
    int status;
    int modified = FALSE;
    bsVdAttrT     *pVdAttr;

    if (pRecord->type != BSR_VD_ATTR) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_625, 
					   "Modified RBMT vol attr record type from %d to BSR_VD_ATTR.\n"),
				   pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_VD_ATTR;
	modified = TRUE;
    }

    /* check record size */

    if (pRecord->bCnt != (sizeof(bsMRT) + sizeof(bsVdAttrT))) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_626, 
					   "Modified RBMT vol attr record bCnt from %d to %d.\n"),
				   pRecord->bCnt,
				   (sizeof(bsMRT) + sizeof(bsVdAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsVdAttrT));
	modified = TRUE;
    }
    pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check fields that have known values.
     */
    if ((pVdAttr->state < BSR_VD_VIRGIN) || 
	(pVdAttr->state > BSR_VD_ZOMBIE)) {
        /*
	 * Value is out of valid range, set to the mounted state.
	 */
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_627, 
					   "Modified RBMT vol attr state from %d to BSR_VD_MOUNTED.\n"),
				   pVdAttr->state);
	if (status != SUCCESS) {
	    return status;
	}
	pVdAttr->state = BSR_VD_DISMOUNTED;
	modified = TRUE;
    }
	
    if (pVdAttr->vdIndex != volNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_628,
					   "Modified RBMT vol attr vdIndex from %d to %d.\n"),
				   pVdAttr->vdIndex, volNum);
	if (status != SUCCESS) {
	    return status;
	}
        pVdAttr->vdIndex = volNum;
	modified = TRUE;
    }

/*
 * TODO-ODS: Verify/fix  bsVdAttr->vdCookie
 */

    if (pVdAttr->metaAllocSz != ADVFS_METADATA_PGSZ_IN_FOBS) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_629, 
					   "Modified RBMT vol attr metaAllocSz from %d to %d.\n"),
				   pVdAttr->metaAllocSz, 
				   ADVFS_METADATA_PGSZ_IN_FOBS);
	if (status != SUCCESS) {
	    return status;
	}
        pVdAttr->metaAllocSz = ADVFS_METADATA_PGSZ_IN_FOBS;
	modified = TRUE;
    }

    domain->volumes[volNum].metaAllocSz = pVdAttr->metaAllocSz;

    /* XXX - would like to check/correct the userAllocSz here but we
       don't know definitively what the user page size is for this
       volume and there's nowhere to cross check it from... */

    domain->volumes[volNum].userAllocSz = pVdAttr->userAllocSz;

    /*
     * The service class can be 1.  If you run log isolation it will
     * change the value to 2.
     */

    if (pVdAttr->serviceClass == 2) {
	int foundLogIsolation;
	int newServiceClass;
	bsMPgT   *pBMTPage;
	bsMCT *pMiscMcell;  /* The misc mcell  */
	bsMRT *pMiscRecord; /* The mcell record we are working on */
	bsXtntRT *pXtnt;

	/*
	 * Special case. The log MAY have been isolated on this
	 * volume.  What we do know is that if log isolation has
	 * been done on this domain then you will have a 'misc'
	 * mcell chain.  So we can look to see if the chain exists.
	 */

	foundLogIsolation = FALSE;
	pBMTPage = (bsMPgT *)pPage->pageBuf;
	pMiscMcell = &(pBMTPage->bsMCA[BFM_MISC]);
	pMiscRecord  = (bsMRT *)pMiscMcell->bsMR0;

	while (pMiscRecord->type != BSR_NIL) {
	    if (pMiscRecord->type == BSR_XTNTS) {
		pXtnt = (bsXtntRT *)((char *)pMiscRecord + sizeof(bsMRT));
		if (pXtnt->chainMCId.volume != 0) {
		    /*
		     * Has a chain, so assume it has been isolated
		     */
		    foundLogIsolation = TRUE;
		}
	    } /* endif: extent record */

	    pMiscRecord = NEXT_MREC(pMiscRecord);

	} /* End while ((pMiscRecord->type != BSR_NIL) && ... */
	
	if (foundLogIsolation == FALSE) {
	    /*
	     * This volume does NOT have log isolation, so serviceClass
	     * is incorrect.  Set it to the value that makes the most
	     * sense.
	     */
	    newServiceClass = 1;
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_630, 
					   "Modified RBMT vol attr serviceClass from %d to %d.\n"),
				   pVdAttr->serviceClass, 
				   newServiceClass);
	if (SUCCESS != status) {
	    return status;
	}
	pVdAttr->serviceClass = newServiceClass;
	modified = TRUE;
    } else if (pVdAttr->serviceClass != 1) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_630, 
					   "Modified RBMT vol attr serviceClass from %d to %d.\n"),
				   pVdAttr->serviceClass, 1);
	if (SUCCESS != status) {
	    return status;
	}
	pVdAttr->serviceClass = 1;
	modified = TRUE;
    } 

    /* check/fix the SBM cluster size */

    if (pVdAttr->sbmBlksBit != ADVFS_BS_CLUSTSIZE) {
        status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_631,
					   "Modified RBMT vol attr cluster size %ld to %ld.\n"),
				   pVdAttr->sbmBlksBit, ADVFS_BS_CLUSTSIZE);
	if (status != SUCCESS) {
	    return status;
	}
	pVdAttr->sbmBlksBit = ADVFS_BS_CLUSTSIZE;
	modified = TRUE;
    }

    /* check/fix the volume size */

    assert (domain->volumes[volNum].volSize != 0);

    if (pVdAttr->vdBlkCnt != domain->volumes[volNum].volSize) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_632, 
					   "Modified RBMT vol attr volSize from %ld to %ld.\n"),
				   pVdAttr->vdBlkCnt,
				   domain->volumes[volNum].volSize);
	if (status != SUCCESS) {
	    return status;
	}
        pVdAttr->vdBlkCnt = domain->volumes[volNum].volSize;
	modified = TRUE;
    }

    /* check/fix the BMT expansion increment */

    if (pVdAttr->bmtXtntPgs == 0) {
        /*
	 * Only value which is a problem.
         */
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_633, 
					   "Modified RBMT vol attr bmtXtntPgs from %d to %d.\n"),
				   pVdAttr->bmtXtntPgs, BS_BMT_XPND_PGS);
	if (status != SUCCESS) {
	    return status;
	}
        pVdAttr->bmtXtntPgs = BS_BMT_XPND_PGS; /* default value */
	modified = TRUE;
    }

    if (pVdAttr->blkSz != DEV_BSIZE) {

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_960, 
					   "Modified RBMT vol attr blkSz from %d to %d.\n"),
				   pVdAttr->blkSz, DEV_BSIZE);
	if (status != SUCCESS) {
	    return status;
	}
        pVdAttr->blkSz = DEV_BSIZE; /* default value */
	modified = TRUE;
    }

    /* we can't really tell if the mount ID (mount time) is right or
       not, so we assume it is.  This shouldn't be a real problem even
       if it is corrupt. */

    domain->volumes[volNum].mountId.id_sec = pVdAttr->vdMntId.id_sec;
    domain->volumes[volNum].mountId.id_usec = pVdAttr->vdMntId.id_usec;
    domain->volumes[volNum].mntStatus = pVdAttr->state;
/*    
 * TODO-ODS: verify/fix pVdAttr->vdCookie - should match RBMT bmtVdCookie
 */
    domain->volumes[volNum].vdCookie = pVdAttr->vdCookie;

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }

} /* correct_rbmt_vd_attr */


/*
 * Function Name: correct_rbmt_umt_thaw_time
 *
 * Description: This function checks a RBMT BSR_DMN_UMT_THAW_TIME rec.
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The umt thaw record to check.
 *     mcellNum: Which mcell has this record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */

static int 
correct_rbmt_umt_thaw_time(pageT *pPage, 
			   bsMRT *pRecord, 
			   cellNumT mcellNum)
{
    int  modified;   /* Has the record has been modified. */
    int  status;

    /*
     * Init variables
     */
    modified = FALSE;

    /*
     * This record must be of type BSR_DMN_UMT_THAW_TIME
     */

    if (pRecord->type != BSR_DMN_UMT_THAW_TIME) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_896, 
					   "Modified RBMT mcell %d's record type from %d to BSR_DMN_UMT_THAW_TIME (%d).\n"),
				   mcellNum, pRecord->type, 
				   BSR_DMN_UMT_THAW_TIME);
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->type = BSR_ATTR;
	modified = TRUE;
    }

    /* verify that the record is the correct size */

    if ((sizeof(bsMRT) + sizeof(bsDmnUmntThawT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_897, 
					   "Modified RBMT mcell %d's BSR_DMN_UMT_THAW_TIME byte count from %d to %d.\n"),
				   mcellNum, pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsDmnUmntThawT)));
	if (status != SUCCESS) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsBfAttrT));
	modified = TRUE;
    }

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }

} /* end correct_rbmt_umt_thaw_time */


/*
 * Function Name: correct_rbmt_floaters
 *
 * Description: Some RBMT records are not tied to a specific position 

 *              in a specific mcell, but can exist in any mcell on any
 *              page in the RBMT.  In some cases, the records may not
 *              exist at all.  This function seaches the current RBMT
 *              page (all mcells) for these records.
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *
 *     mcellNum: Which mcell has this record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */

static int 
correct_rbmt_floaters(pageT *pPage)
{
    bsMPgT  *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT   *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT   *pRecord; /* The Current mcell record we are working on */
    cellNumT mcellNum;
    int      status;
    int      modified; /* Flag to let us know if the page has been modified.*/
    int      foundVdRecord;
    int      foundSSRecord;

    /*
     * Init variables
     */

    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    foundVdRecord = FALSE;
    foundSSRecord = FALSE;
    status = SUCCESS;

    /* loop through all the mcells in the page... */

    for (mcellNum = 0; mcellNum < BSPG_CELLS; mcellNum++) {

	pMcell = &(pBMTPage->bsMCA[mcellNum]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    switch (pRecord->type) {

	    case BSR_VD_IO_PARAMS:
		if (!foundVdRecord++) {
		    status = correct_rbmt_vd_params(pPage, pRecord, mcellNum);
		}
		break;

	    case BSR_DMN_SS_ATTR:
		if (!foundSSRecord++) {
		    status = correct_rbmt_ss_attr(pPage, pRecord, mcellNum);
		}
		break;
	    }

	    if (status == FIXED) {
		modified = TRUE;
	    } else if (status != SUCCESS){
		return status;
	    }

	    pRecord = NEXT_MREC(pRecord);

	} /* end: loop thru records in this mcell */
    } /* end: loop thru mcells */

    if (modified)
	return FIXED;

    return SUCCESS;

} /* end correct_rbmt_floaters */


/*
 * Function Name: correct_rbmt_vd_params
 *
 * Description: This function checks a RBMT BSR_DMN_UMT_THAW_TIME rec.
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *     mcellNum: Which mcell has this record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */

static int 
correct_rbmt_vd_params(pageT *pPage, 
		       bsMRT *pRecord, 
		       cellNumT mcellNum)
{
    int  modified;   /* Has the record has been modified. */
    int  status;

    /*
     * Init variables
     */
    modified = FALSE;

    /* verify that the record is the correct size */

    if ((sizeof(bsMRT) + sizeof(vdIoParamsT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_947, 
					   "Modified RBMT BSR_VD_IO_PARAMS record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(vdIoParamsT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(vdIoParamsT));
	modified = TRUE;
    }

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }

} /* end correct_rbmt_vd_params */


/*
 * Function Name: correct_rbmt_ss_attr
 *
 * Description: This function checks a RBMT BSR_DMN_SS_ATTR rec.
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *     mcellNum: Which mcell has this record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */

static int 
correct_rbmt_ss_attr(pageT *pPage, 
		     bsMRT *pRecord, 
		     cellNumT mcellNum)
{
    int  modified;   /* Has the record has been modified. */
    int  status;

    /*
     * Init variables
     */
    modified = FALSE;

    /* verify that the record is the correct size */

    if ((sizeof(bsMRT) + sizeof(bsSSDmnAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_900, 
					   "Modified RBMT BSR_DMN_SS_ATTR record's bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsSSDmnAttrT)));
	if (status != SUCCESS) {
	    return status;
	}
	pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsSSDmnAttrT));
	modified = TRUE;

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_883,
					   "Corrected vfast record.\n"));
	if (status != SUCCESS) {
	    return status;
	}
    }

    if (modified == TRUE) {
	return FIXED;
    } else {
	return SUCCESS;
    }

} /* end correct_rbmt_ss_attr */

/* end rbmt.c */
