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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 * 
 * Facility:
 * 
 *      Advanced File System
 * 
 * Abstract:
 * 
 *      Test specific mcell records.
 * 
 * Date:
 *      19-nov-03
 */

#include "fsck.h"

/* local protos */

static bsMRT *find_first_rec(bsMPgT *pageHeader, uint32_t type);
static bsMRT *find_next_rec(bsMPgT *pageHeader, bsMRT *pRecord);
static cellNumT rec_to_mcellNum(bsMPgT *pageHeader, bsMRT *pRecord);


/*
 * Function Name: smash_rbmt_header
 *
 * Description: This routine inserts errors in the page's header
 */

void
smash_rbmt_header(bsMPgT *pRBMT)
{
    /* smash RBMT page header */

    pRBMT->magicNumber = 1;
    pRBMT->fsCookie.id_sec = 5;
    pRBMT->bmtFreeMcellCnt = 200;
    pRBMT->nextfreeMCId.cell = 100;
    pRBMT->bmtNextFreePg = 10;
    pRBMT->bmtPageId++;
    pRBMT->bmtVdIndex = 0;
    pRBMT->bmtVdCookie = -1;

} /* smash_rbmt_header */


/*
 * Function Name: smash_bmt_header
 *
 * Description: This routine inserts errors in the page's header
 */

void
smash_bmt_header(bsMPgT *pBMT)
{
    /* smash BMT page header */

    pBMT->magicNumber = 1;
    pBMT->fsCookie.id_sec = 5;
    pBMT->bmtFreeMcellCnt = 200;
    pBMT->nextfreeMCId.cell = 100;
    
    /* fsck can't deal with more than one currupted BMT page ID
       number, so we just corrupt one. */

    if (pBMT->bmtPageId == 0)
	pBMT->bmtPageId = 300;

#ifdef FSCK_NOT_FIXED
    pBMT->bmtPageId = 400;
    pBMT->bmtNextFreePg = 99; /* fsck can't recover from this for BMT... */
#endif /* FSCK_NOT_FIXED */

    pBMT->bmtVdIndex = 0;
    pBMT->bmtVdCookie = -1;

} /* smash_bmt_header */


/*
 * Function Name: smash_attr
 *
 * Description: This routine inserts errors in the page's attribute
 *              records
 */

void
smash_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec;
    bsBfAttrT *pAttr;

    pRec = find_first_rec(pRBMT, BSR_ATTR);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "%s attribute record not found on page\n",
		 pRBMT->magicNumber == RBMT_MAGIC ? "RBMT" : "BMT");
	return;
    }

    while (pRec) {

	pAttr = (bsBfAttrT *)((char *)pRec + sizeof(bsMRT));

	pAttr->state = 200;

	/* FSCK_NOT_FIXED - in the case of a BMT page, only the state
	   field of the attr record is checked by fsck. */

	if (pRBMT->magicNumber == RBMT_MAGIC) {
	    pAttr->bfPgSz = 100;
	    pAttr->transitionId = 300;
	}

	pRec = find_next_rec(pRBMT, pRec);
    }

} /* smash_attr */


/*
 * Function Name: smash_vd_attr
 *
 * Description: This routine inserts errors in the page's volume
 *              attribute record.
 */

void
smash_vd_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_VD_ATTR);
    bsVdAttrT *pVd;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_ERR, "volume attribute record not found!\n");
	failed_exit();
    }

    pVd = (bsVdAttrT *)((char *)pRec + sizeof(bsMRT));

    pVd->state = 200;          /* vd state */
    pVd->vdIndex = 300;        /* this vd's index in the domain */
    pVd->vdBlkCnt = 400;       /* number blocks on this volume */
    pVd->sbmBlksBit = 500;     /* SBM blocks to bit */
    pVd->metaAllocSz = 800;    /* allocaiton unit for metadata in 1k fobs */
    pVd->blkSz = 900;          /* block size - typically DEV_BSIZE */
    pVd->serviceClass = 1000;
    pVd->vdMntId.id_sec = 100; /* vd mount ID (time of mount) */
    pVd->bmtVdIndex = 0;
    pVd->bmtVdCookie = -1;

#ifdef FSCK_NOT_FIXED
    pVd->bmtXtntPgs = 600;     /* number of pages per BMT extent */
    pVd->vdMntId.id_sec = 100; /* vd mount ID (time of mount) */
    pVd->userAllocSz = 700;    /* allocation unit for user data in 1k fobs */
    pVd->worldWideId = 1100;   /* Storage Device WWID */
#endif /* FSCK_NOT_FIXED */

} /* smash_vd_attr */


/*
 * Function Name: smash_dmn_attr
 *
 * Description: This routine inserts errors in the page's domain
 *              attribute record.
 */

void
smash_dmn_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DMN_ATTR);
    bsDmnAttrT *pDmn;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_ERR, "domain attribute record not found!\n");
	failed_exit();
    }

    pDmn = (bsDmnAttrT *)((char *)pRec + sizeof(bsMRT));

    pDmn->bfDomainId.id_sec = 5;     /* id of this bitfile domain */
    pDmn->MasterFsCookie.id_sec = 6; /* creation time as initial bfDomainId */
    pDmn->bfSetDirTag.tag_num = 7;   /* tag of root tag directory */
    pDmn->maxVds = BS_MAX_VDI+1;     /* maximum vd index */

} /* smash_dmn_attr */

/*
 * Function Name: smash_dmn_mattr
 *
 * Description: This routine inserts errors in the page's domain
 *              mutable attribute record.  There is one "valid"
 *              mutable attr record per domain, with validity
 *              established by the highest sequence number in the
 *              volume
 */

void
smash_dmn_mattr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DMN_MATTR);
    bsDmnMAttrT *pMA;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_ERR, "domain mutable attribute record not found!\n");
	failed_exit();
    }

    pMA = (bsDmnMAttrT *)((char *)pRec + sizeof(bsMRT));

    pMA->ODSVersion.odv_major = 11;
    pMA->seqNum = 0;             /* sequence number of this record */
    pMA->delPendingBfSet.tag_num = -1; /* head 'bf set delete pending' list */ 
    pMA->recoveryFailed = -1;    /* Recovery failed, don't activate */
    pMA->bfSetDirTag.tag_num = 300; /* tag of root bitfile-set tag directory */
    pMA->vdCnt = BS_MAX_VDI+1;   /* number of disks in the domain */ 
    pMA->ftxLogTag.tag_num = 400;/* tag of domain log */
    pMA->ftxLogPgs = -1;         /* number of pages in log */ 

#ifdef FSCK_NOT_FIXED
    pMA->uid = 100;              /* domain's owner */
    pMA->gid = 200;              /* domain's group */
    pMA->mode = 0;               /* domain's access permissions */
#endif /* FSCK_NOT_FIXED */

} /* smash_dmn_mattr */


/*
 * Function Name: smash_dmn_trans_attr
 *
 * Description: This routine inserts errors in the page's domain
 *              transaction attribute record.
 */

void
smash_dmn_trans_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DMN_TRANS_ATTR);
    bsDmnTAttrT *pTA;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "domain transient attribute record not found\n");
	return;
    }

#ifdef FSCK_NOT_FIXED
    pTA = (bsDmnTAttrT *)((char *)pRec + sizeof(bsMRT));

    pTA->chainMCId.volume = 0;
    pTA->op = -1;
    pTA->dev = -1;
#endif /* FSCK_NOT_FIXED */

} /* smash_dmn_trans_attr */


/*
 * Function Name: smash_dmn_freeze_attr
 *
 * Description: This routine inserts errors in the page's freeze
 *              attribute record.
 */

void
smash_dmn_freeze_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DMN_FREEZE_ATTR);
    bsDmnFreezeAttrT *pFA;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "domain freeze attribute record not found\n");
	return;
    }

#ifdef FSCK_NOT_FIXED
    pFA = (bsDmnFreezeAttrT *)((char *)pRec + sizeof(bsMRT));

    pFA->freezeBegin;       /* Time of last dmn freeze */
    pFA->freezeEnd;         /* Time of last dmn thaw   */
    pFA->freezeCount;       /* Number of times domain has been frozen */
#endif /* FSCK_NOT_FIXED */

} /* smash_dmn_freeze_attr */


/*
 * Function Name: smash_dmn_ss_attr
 *
 * Description: This routine inserts errors in the page's 
 *              smart store attribute record.
 */

void
smash_dmn_ss_attr(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DMN_SS_ATTR);
    bsSSDmnAttrT *pSS;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "domain smart store attribute record not found\n");
	return;
    }

#ifdef FSCK_NOT_FIXED
    pSS = (bsSSDmnAttrT *)((char *)pRec + sizeof(bsMRT));

    pSS->ssDmnState;        /* processing state that domain is in */
    /* UI visable configurable options */
    pSS->ssDmnDefragment;
    pSS->ssDmnDefragAll;
    pSS->ssDmnSmartPlace;
    pSS->ssDmnBalance;
    pSS->ssDmnVerbosity;
    pSS->ssDmnDirectIo;
    pSS->ssMaxPercentOfIoWhenBusy;
    /* output only variables */
    pSS->ssFilesDefraged;
    pSS->ssFobsDefraged;
    pSS->ssFobsBalanced;
    pSS->ssFilesIOBal;
    pSS->ssExtentsConsol;
    pSS->ssPagesConsol;
    /* UI unvisable configurable options */
    pSS->ssAccessThreshHits;
    pSS->ssSteadyState;
    pSS->ssMinutesInHotList;
#endif /* FSCK_NOT_FIXED */

} /* smash_dmn_ss_attr */


/*
 * Function Name: smash_vd_io_params
 *
 * Description: This routine inserts errors in the page's volume
 *              I/O parameter record,
 */

void
smash_vd_io_params(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_VD_IO_PARAMS);
    vdIoParamsT *pVP;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "domain volume I/O attribute record not found\n");
	return;
    }

#ifdef FSCK_NOT_FIXED
    pVP = (vdIoParamsT *)((char *)pRec + sizeof(bsMRT));

    pVP->rdMaxIo;      /* Current read/write IO transfer sizes */
    pVP->wrMaxIo; 
#endif /* FSCK_NOT_FIXED */

} /* smash_vd_io_params */


/*
 * Function Name: smash_def_del_mcell_list
 *
 * Description: This routine inserts errors in the page's 
 *              deferred delete mcell list record.
 */

void
smash_def_del_mcell_list(bsMPgT *pRBMT)
{
    bsMRT *pRec = find_first_rec(pRBMT, BSR_DEF_DEL_MCELL_LIST);
    delLinkRT *pDL;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "deferred delete list record not found\n");
	return;
    }

#ifdef FSCK_NOT_FIXED
    pDL = (delLinkRT *)((char *)pRec + sizeof(bsMRT));

    pDL->nextMCId;      /* next mcell in the list */  
    pDL->prevMCId;      /* preceding mcell in list */
#endif /* FSCK_NOT_FIXED */


} /* smash_def_del_mcell_list */


/*
 * Function Name: smash_bfs_attr
 *
 * Description: This routine inserts errors in the page's attribute
 *              records
 */

void
smash_bfs_attr(bsMPgT *pBMT)
{
    bsMRT *pRec;
    bsBfSetAttrT *pAttr;

    pRec = find_first_rec(pBMT, BSR_BFS_ATTR);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT BFS attribute record not found on page\n");
	return;
    }

    while (pRec) {

	pAttr = (bsBfSetAttrT *)((char *)pRec + sizeof(bsMRT));

	pAttr->bfSetId.domainId.id_sec = 1; /* bitfile-set's ID */
	pAttr->bfSetId.dirTag.tag_num = -1;
	pAttr->setName[0] = '\0';

#ifdef FSCK_NOT_FIXED
	pAttr->state;
	pAttr->nxtDelPendingBfSet;        /* next delete pending bf set */
	pAttr->flags;
	pAttr->fsDev;                     /* Unique ID */
	pAttr->uid;                       /* set's owner */
	pAttr->gid;                       /* set's group */
	pAttr->mode;                      /* set's permissions mode */
	pAttr->numberFiles;               /* ~ Number of file in fileset */
	pAttr->numberDirs;                /* ~ Number of dirs in fileset */
	pAttr->numberExtents;             /* ~ Number of extents in fileset */
#endif /* FSCK_NOT_FIXED */

	pRec = find_next_rec(pBMT, pRec);
    }

} /* smash_bfs_attr */

/*
 * Function Name: smash_bf_inherit_attr
 *
 * Description: This routine inserts errors in the page's inheritance
 *              records
 */

void
smash_bf_inherit_attr(bsMPgT *pBMT)
{
    bsMRT *pRec;
    bsBfInheritAttrT *pIA;

    pRec = find_first_rec(pBMT, BSR_BF_INHERIT_ATTR);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT inheritance attr rec not found on page\n");
	return;
    }

    while (pRec) {

	pIA = (bsBfInheritAttrT *)((char *)pRec + sizeof(bsMRT));
	pIA->reqServices = 100;

	pRec = find_next_rec(pBMT, pRec);
    }

} /* smash_bf_inherit_attr */

/*
 * Function Name: smash_bfs_quota_attr
 *
 * Description: This routine inserts errors in the page's inheritance
 *              records
 */

void
smash_bfs_quota_attr(bsMPgT *pBMT)
{
    bsMRT *pRec;
    bsQuotaAttrT *pQA;

    pRec = find_first_rec(pBMT, BSR_BFS_QUOTA_ATTR);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT quota attr rec not found\n");
	return;
    }

    pQA = (bsQuotaAttrT *)((char *)pRec + sizeof(bsMRT));

#ifdef FSCK_NOT_FIXED
    pQA->blkHLimit = 1;         /* hard block limit */          
    pQA->blkSLimit = 1;         /* soft block limit */
    pQA->fileHLimit = 1;        /* hard file limit */
    pQA->fileSLimit = 1;        /* soft file limit */
    pQA->blkTLimit = 1;         /* time limit for excessive disk blk use */    
    pQA->fileTLimit = 1;        /* time limit for excessive file use */
    pQA->quotaStatus = 1000;     /* quota system status */
#endif /* FSCK_NOT_FIXED */

} /* smash_bfs_quota_attr */

/*
 * Function Name: smash_bmtr_fs_dir_index_file
 *
 * Description: This routine inserts errors in the fs dir index
 *              records
 */

void
smash_bmtr_fs_dir_index_file(bsMPgT *pBMT)
{
    bsMRT *pRec;
    bfTagT *pDI;

    pRec = find_first_rec(pBMT, BMTR_FS_DIR_INDEX_FILE);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT dir index rec not found on page\n");
	return;
    }

    while (pRec) {

	pDI = (bfTagT *)((char *)pRec + sizeof(bsMRT));
	pDI->tag_num = -1;

	pRec = find_next_rec(pBMT, pRec);
    }

} /* smash_bmtr_fs_dir_index_file */


/*
 * Function Name: smash_bmtr_fs_dir_index_file
 *
 * Description: This routine inserts errors in the fs index records
 */

void
smash_bmtr_fs_index_file(bsMPgT *pBMT)
{
    bsMRT *pRec;
    bsIdxBmtRecT *pI;

    pRec = find_first_rec(pBMT, BMTR_FS_INDEX_FILE);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT fs index rec not found on page\n");
	return;
    }

    while (pRec) {

	pI = (bsIdxBmtRecT *)((char *)pRec + sizeof(bsMRT));

	pI->fname_page = -1;
	pI->ffree_page = -1;
	pI->fname_levels = -1;
	pI->ffree_levels = -1;
	pI->seqDirTag.tag_num = -1;

	pRec = find_next_rec(pBMT, pRec);
    }

} /* smash_bmtr_fs_index_file */

/*
 * Function Name: smash_bmtr_fs_time
 *
 * Description: This routine inserts errors in the FS time record
 */

void
smash_bmtr_fs_time(bsMPgT *pBMT)
{
    bsMRT    *pRec;
    bsMCT    *pMcell;    /* Pointer to current mcell. */
    cellNumT mcellNum;

    pRec = find_first_rec(pBMT, BMTR_FS_TIME);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT FS time rec not found on page\n");
	return;
    }

    mcellNum = rec_to_mcellNum(pBMT, pRec);
    pMcell = &(pBMT->bsMCA[mcellNum]);

    /* fsck response to incorrect tag is to delete the record */

    pMcell->tag.tag_num = -1;

} /* smash_bmtr_fs_time */

/*
 * Function Name: smash_bmtr_fs_undel_dir
 */

void
smash_bmtr_fs_undel_dir(bsMPgT *pBMT)
{
    /* fsck currently does nothing with this record */

} /* smash_bmtr_fs_undel_dir */

/*
 * Function Name: smash_bmtr_fs_data
 */

void
smash_bmtr_fs_data(bsMPgT *pBMT)
{
    /* fsck currently does nothing with this record */

} /* smash_bmtr_fs_data */

/*
 * Function Name: smash_bmtr_fs_stat
 */

void
smash_bmtr_fs_stat(bsMPgT *pBMT)
{
    /* fsck currently does nothing with this record */

} /* smash_bmtr_fs_stat */

/*
 * Function Name: smash_bsr_mcell_free_list
 *
 * Description: This routine inserts errors in the FS mcell free list
 *              record
 */

void
smash_bsr_mcell_free_list(bsMPgT *pBMT)
{
    bsMRT    *pRec;
    bsMcellFreeListT *pMcellFreeList;

    pRec = find_first_rec(pBMT, BSR_MCELL_FREE_LIST);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT FS mcell free list rec not found on page\n");
	return;
    }

    pMcellFreeList = (bsMcellFreeListT *)((char *)pRec + sizeof(bsMRT));
    pMcellFreeList->headPg = -1;

} /* smash_bsr_mcell_free_list */

/*
 * Function Name: smash_bsr_def_del_mcell_list
 */

void
smash_bsr_def_del_mcell_list(bsMPgT *pBMT)
{

    /* fsk does not check the contents of this record, beyond
       verifying that it is positioned at the start of the BMT. */

} /* smash_bsr_def_del_mcell_list */

/*
 * Function Name: smash_bsr_acls
 */

void
smash_bsr_acl(bsMPgT *pBMT)
{
    bsMRT    *pRec;
    bsr_acl_rec_t *pACL;

    pRec = find_first_rec(pBMT, BSR_ACL_REC);

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "BMT ACL not found on page\n");
	return;
    }

    pACL = (bsr_acl_rec_t *) MREC_TO_REC(pRec);
    pACL->bar_hdr.bar_acl_link_seg++;
    pACL->bar_hdr.bar_acl_total_cnt--;

} /* smash_bsr_acls */

/*
 * Function Name: smash_xtnts
 *
 * Description: This routine inserts errors in the page's primary
 *              extent records
 */

void
smash_xtnts(bsMPgT *pBMT,
	    int testType)
{
    bsMRT *pRec = find_first_rec(pBMT, BSR_XTNTS);
    bsXtntRT *pXtnt;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "no primary extents on page %ld\n", pBMT->bmtPageId);
	return;
    }
    
    pXtnt = (bsXtntRT *)((char *)pRec + sizeof(bsMRT));

    switch (testType) {
    case 1:
	/* corrupt the extent terminator */

	pXtnt->bsXA[pXtnt->xCnt - 1].bsx_fob_offset--;
	break;

    case 2:
	/* 
	 * This will cause an overlap if the previous block is
	 * allocated.  Don't do this when testing the RBMT,
	 * metadata file overlaps are fatal.
	 */

	pXtnt->bsXA[pXtnt->xCnt-2].bsx_vd_blk -= 1;
	break;

    case 3:
	/* corrupt the fob offset... */

	pXtnt->bsXA[pXtnt->xCnt-1].bsx_fob_offset += 1;
	break;

    default:
	break;
    }

} /* smash_xtnts */

/*
 * Function Name: smash_xtra_xtnts
 *
 * Description: This routine inserts errors in the page's extra 
 *              extents records.
 */

void
smash_xtra_xtnts(bsMPgT *pBMT,
		 int testType)
{
    bsMRT *pRec = find_first_rec(pBMT, BSR_XTRA_XTNTS);
    bsXtraXtntRT *pXtnt;

    if (pRec == (bsMRT *)NULL) {
	writemsg(SV_VERBOSE, "no extra extents on page %ld\n", pBMT->bmtPageId);
	return;
    }
    
    while (pRec) {

	pXtnt = (bsXtraXtntRT *)((char *)pRec + sizeof(bsMRT));

	switch (testType) {
	case 1:
	    /* corrupt the extent terminator */

	    pXtnt->bsXA[1].bsx_fob_offset--;
	    break;

	case 2:
	    /* 
	     * This will cause an overlap if the previous block is
	     * allocated.  Don't do this when testing the RBMT,
	     * reserved file overlaps are fatal.
	     */

	    pXtnt->bsXA[pXtnt->xCnt-2].bsx_vd_blk--;
	    break;

	case 3:
	    /* corrupt the fob offset of the last entry... */

	    pXtnt->bsXA[pXtnt->xCnt-1].bsx_fob_offset++;
	    break;

	default:
	    break;
	}

	pRec = find_next_rec(pBMT, pRec);
    }

} /* smash_xtra_xtnts */

/*****************************************************************
 *****************************************************************
 *****************************************************************
 *
 * SUPPORT ROUTINES...
 *
 *****************************************************************
 *****************************************************************
 ****************************************************************/

/*
 * Function Name: find_first_rec
 *
 * Description: This routine finds the first occurrance of record
 *              having a specified type in a specified RBMT/BMT page
 *
 * Return: the found record, or NULL if no such record found
 */

static bsMRT *
find_first_rec(bsMPgT *pageHeader, 
	       uint32_t type)
{
    bsMRT    *pRecord;
    bsMCT    *pMcell;
    cellNumT mcell;

    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {

	pMcell = &(pageHeader->bsMCA[mcell]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while (pRecord->type != type &&
	       pRecord->type != BSR_NIL &&
	       pRecord->bCnt != 0 &&
	       pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) {

	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t)));
	}

	if (pRecord->type == type) {
	    writemsg(SV_DEBUG, "rec type %d, mcell %d\n", type, mcell);
	    return (pRecord);
	}
    }

    return ((bsMRT *)NULL);

} /* find_first_rec */

/*
 * Function Name: find_next_rec
 *
 * Description: This routine finds the first next occurrance of record
 *              in a page having a specified type.  It is assumed that
 *              the record passed in was a record returned by either
 *              this function, or by find_first_rec().
 *
 * Return: the found record, or NULL if no such record found
 */

static bsMRT *
find_next_rec(bsMPgT *pageHeader,
	      bsMRT *pRecord)
{
    bsMCT *pMcell;
    cellNumT mcell = rec_to_mcellNum(pageHeader, pRecord);
    int type = pRecord->type;

    pMcell = &(pageHeader->bsMCA[mcell]);

    /* begin search from the record following the caller specified
       record */

    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(uint64_t)));
    do {

	while (pRecord->type != type &&
	       pRecord->type != BSR_NIL &&
	       pRecord->bCnt != 0 &&
	       pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) {

	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t)));
	}

	if (pRecord->type == type) {
	    writemsg(SV_DEBUG, "rec type %d, mcell %d\n", type, mcell);
	    return (pRecord);
	}

 	pMcell = &(pageHeader->bsMCA[++mcell]);
	pRecord = (bsMRT *)pMcell->bsMR0;

    } while (mcell < BSPG_CELLS);

    return ((bsMRT *)NULL);

} /* find_next_rec */

/*
 * Function Name: rec_to_mcellNum
 *
 * Description: This routine finds the mcell that contains a given
 *              record address.
 *
 * Return: the number of the containing mcell.  
 *         -1 if the record is not in the given page
 */

static cellNumT
rec_to_mcellNum(bsMPgT *pageHeader,
		bsMRT *pRecord)
{
    bsMCT *pMcell;
    int mcell;

    /* determine which mcell the specified record is in */

    for (mcell = 0; mcell <= BSPG_CELLS; mcell++) {
	if (pRecord < ((bsMRT *) &(pageHeader->bsMCA[mcell])))
	    break;
    }

    if (mcell > BSPG_CELLS)
	return (-1);

    return (--mcell);

} /* rec_to_mcellNum */

/* end: rec.c */
