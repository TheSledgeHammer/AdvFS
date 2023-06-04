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

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <assert.h>
#include <inttypes.h>
#include <time.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>

#include <advfs/ms_public.h>
#include <advfs/ms_privates.h>
#include <advfs/bs_public.h>
#include <advfs/bs_index.h>
#include <advfs/bs_ods.h>
#include <advfs/fs_dir.h>
#include <advfs/advfs_acl.h>

#include "vods.h"

/* private prototypes */

static void print_record(bsMRT *, int, int, bfTagT);
static void printBSR_XTNTS(bsXtntRT *, int, bfTagT);
static void printBMTR_FS_STAT(statT *, int);
static void printBSR_ATTR(bsBfAttrT *pdata, int vflg);
static void printBSR_VD_ATTR(bsVdAttrT *pdata, int vflg);
static void printBSR_DMN_ATTR(bsDmnAttrT *pdata, int vflg);
static void printBSR_XTRA_XTNTS(bsXtraXtntRT *pdata, int vflg);
static void printBSR_MCELL_FREE_LIST(bsMcellFreeListT *pdata, int vflg);
static void printBSR_BFS_ATTR(bsBfSetAttrT *pdata, int vflg);
static void printBSR_VD_IO_PARAMS(vdIoParamsT *pdata, int vflg);
static void printBSR_TERTIARY_XTNTS(bsTerXtntRT *pdata, int vflg);
static void printBSR_TERTIARY_XTRA_XTNTS(bsTerXtraXtntRT *pdata, int vflg);
static void printBSR_DEF_DEL_MCELL_LIST(delLinkRT *pdata, int vflg);
static void printBSR_DMN_MATTR(bsDmnMAttrT *pdata, int vflg);
static void printBSR_BF_INHERIT_ATTR(bsBfInheritAttrT *pdata, int vflg);
static void printBSR_BFS_QUOTA_ATTR(bsQuotaAttrT *pdata, int vflg);
static void printBSR_ACL_REC(bsr_acl_rec_t *pdata, int vflg);
static void printBSR_DMN_TRANS_ATTR(bsDmnTAttrT *pdata, int vflg);
static void printBSR_DMN_SS_ATTR(bsSSDmnAttrT *pdata, int vflg);
static void printBSR_DMN_FREEZE_ATTR(bsDmnFreezeAttrT *pdata, int vflg);
static void printBSR_RUN_TIMES( bsRunTimesRT *pdata, int vflg);
static void printBSR_DMN_NAME(bsDmnNameT *pdata, int vflg);
static void printBSR_DMN_UMT_THAW_TIME(bsDmnUmntThawT *pdata, int vflg);
static void printBMTR_FS_DIR_INDEX_FILE(bfTagT *bfTagp, int flags);
static void printBMTR_FS_INDEX_FILE(bsIdxBmtRecT *bsIdxBmtRecp, int flags);
static void printBMTR_FS_UNDEL_DIR(struct bsUndelDir *undel_dir, int vflg);
static void printBMTR_FS_TIME(int64_t *fs_time, int vflg);
static void printBMTR_FS_DATA(char *pdata, int vflg);

/***************************************************************************/
/* FIX. change to print_BMT_page_header() */
void
print_mcell_header ( bitFileT *bfp, bsMPgT *pdata, int vflg)
{
    print_header(bfp);

    if ( bfp->pgNum != BMT_PG_TERM && bfp->pgNum != pdata->bmtPageId ) {
        printf("pageId (%ld) does not match requested page %ld\n",
          pdata->bmtPageId, bfp->pgNum);
    }

    if ( vflg ) {
	char time_buf[40];

	printf("pageId %ld   ", pdata->bmtPageId);
        printf("nextFreePg %-3ld  ", pdata->bmtNextFreePg);

        if (pdata->bmtMagicNumber == RBMT_MAGIC) {
            printf("ODSVersion %d.%d   ",
                    pdata->bmtODSVersion.odv_major,
                    pdata->bmtODSVersion.odv_minor);
        } else {
            printf("rsvd1 %d   ", pdata->bmtRsvd1);
        }
        printf("magicNumber 0x%x\n", pdata->bmtMagicNumber);
        printf("freeMcellCnt %-3d  ", pdata->bmtFreeMcellCnt);
        printf("nextFreeMcell %-3d\n", pdata->bmtNextFreeMcell);

	if (ctime_r(&pdata->bmtFsCookie, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}

        printf("fsCookie 0x%lx.0x%lx (%s)\n", pdata->bmtFsCookie.id_sec,
                pdata->bmtFsCookie.id_usec, time_buf);

	if (ctime_r(&pdata->bmtVdCookie, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}

        printf("vdCookie 0x%lx (%s)   ", pdata->bmtVdCookie, time_buf);
        printf("vdIndex %-3d ", pdata->bmtVdIndex);
        
        printf("rsvd2 %d\n", pdata->bmtRsvd2);
    }
}

/******************************************************************************/
#define PRINT_RSVD_NAMES(tag)					        \
    switch ( (tag) % 6 ) {						\
      case 0:								\
        printf(" (RBMT)");		                                \
        break;								\
      case -1:								\
        printf(" (SBM)");						\
        break;								\
      case -2:								\
        printf(" (TAG)");						\
        break;								\
      case -3:								\
        printf(" (LOG)");						\
        break;								\
      case -4:								\
        printf(" (BMT)");		                                \
        break;								\
      case -5:								\
        printf(" (Misc)");						\
        break;								\
      default:								\
        break;								\
    }

void
print_cell_hdr(bsMCT *pdata, uint16_t cn, int vflg)
{
    int i;

    printf("CELL %2d", cn);
    if ( vflg ) {
        printf("     tag      %ld (0x%lx.%x)",
                pdata->mcTag.tag_num, pdata->mcTag.tag_num, pdata->mcTag.tag_seq);
        if ( BS_BFTAG_RSVD(pdata->mcTag) ) {
            PRINT_RSVD_NAMES((int64_t)pdata->mcTag.tag_num);
        }
        printf("  bfSetTag: %ld (%ld.%u)\n",
                pdata->mcBfSetTag.tag_num, pdata->mcBfSetTag.tag_num,
                pdata->mcBfSetTag.tag_seq);
        printf("linkSegment %d    ", pdata->mcLinkSegment);
        printf("nextMCId  volume,page,cell %d,%ld,%d\n",
                pdata->mcNextMCId.volume, pdata->mcNextMCId.page,
                pdata->mcNextMCId.cell);
        printf("numRecords %d     rsvd1 %d     rsvd2 %d\n",
                pdata->mcNumRecords, pdata->mcRsvd1, pdata->mcRsvd2);
    } else {
        printf("     tag: %ld (%ld.%u)",
                pdata->mcTag.tag_num, pdata->mcTag.tag_num, pdata->mcTag.tag_seq);
        if ( BS_BFTAG_RSVD(pdata->mcTag) ) {
            PRINT_RSVD_NAMES((int64_t)pdata->mcTag.tag_num);
        }
        printf("  bfSetTag: %ld (%ld.%u)\n",
                pdata->mcBfSetTag.tag_num, pdata->mcBfSetTag.tag_num,
                pdata->mcBfSetTag.tag_seq);
        printf("nextMCId  volume,page,cell %d,%ld,%d\n",
                pdata->mcNextMCId.volume, pdata->mcNextMCId.page,
                pdata->mcNextMCId.cell);
    }
}

/******************************************************************************/
void
print_cell(bsMCT *bsMCp, uint16_t cn, int flags)
{
    bsMRT *bsMRp;
    int rn;

    bsMRp = (bsMRT *)bsMCp->bsMR0;

    if ( !(flags & ONECELL) && !(flags & VFLG) && bsMRp->type == BSR_NIL ) {
        return;
    }

    if ( BS_BFTAG_RSVD(bsMCp->mcTag) ) {
        flags |= RSVDBF;
    }

    if ( flags & VFLG || cn != 0 && !(flags & ONECELL) )
        printf(SINGLE_LINE);

    print_cell_hdr(bsMCp, cn, flags & VFLG);

    rn=0;
    while ( bsMRp->type != BSR_NIL && bsMRp < (bsMRT *)&bsMCp->bsMR0[BSC_R_SZ] ) {
        print_record(bsMRp, rn, flags, bsMCp->mcTag);
        if ( (char *)bsMRp + bsMRp->bCnt > &bsMCp->bsMR0[BSC_R_SZ] ) {
            fprintf(stderr, "Bad mcell record: bCnt too large\n");
            break;
        }
        if ( bsMRp->bCnt == 0 ) {
            fprintf(stderr, "Bad mcell record %d. bCnt == 0\n", rn);
            break;
        }

        rn++;
        bsMRp = (bsMRT*)((char*)bsMRp + roundup(bsMRp->bCnt,sizeof(int64_t)));
    }
}

/******************************************************************************/
#define PRINT_REC_TYPE(type)						\
    switch ( type ) {							\
      case BSR_XTNTS:			/* 1 */				\
        printf("               BSR_XTNTS");				\
        break;								\
      case BSR_ATTR:			/* 2 */				\
        printf("                BSR_ATTR");				\
        break;								\
      case BSR_VD_ATTR:			/* 3 */				\
        printf("             BSR_VD_ATTR");				\
        break;								\
      case BSR_DMN_ATTR:		/* 4 */				\
        printf("            BSR_DMN_ATTR");				\
        break;								\
      case BSR_XTRA_XTNTS:		/* 5 */				\
        printf("          BSR_XTRA_XTNTS");				\
        break;								\
      case BSR_MCELL_FREE_LIST:		/* 6 */				\
        printf("     BSR_MCELL_FREE_LIST");				\
        break;								\
      case BSR_BFS_ATTR:		/* 7 */				\
        printf("            BSR_BFS_ATTR");				\
        break;								\
      case BSR_VD_IO_PARAMS:		/* 8 */				\
        printf("        BSR_VD_IO_PARAMS");				\
        break;								\
      case BSR_TERTIARY_XTNTS:			        		\
        printf("     BSR_TERTIARY_XTNTS");				\
        break;								\
      case BSR_TERTIARY_XTRA_XTNTS:	        			\
        printf(" BSR_TERTIARY_XTRA_XTNTS");				\
        break;								\
      case BSR_DEF_DEL_MCELL_LIST:	/* 9 */		        	\
        printf(" BSR_DEF_DEL_MCELL_LIST");				\
        break;								\
      case BSR_DMN_MATTR:		/* 10 */			\
        printf("          BSR_DMN_MATTR");				\
        break;								\
      case BSR_BF_INHERIT_ATTR:		/* 11 */			\
        printf("    BSR_BF_INHERIT_ATTR");				\
        break;								\
      case BSR_BFS_QUOTA_ATTR:		/* 12 */			\
        printf("     BSR_BFS_QUOTA_ATTR");				\
        break;								\
      case BSR_ACL_REC:		        /* 13 */			\
        printf("     BSR_ACL_REC");             		        \
        break;                                                          \
      case BSR_DMN_TRANS_ATTR:		/* 14 */			\
        printf("     BSR_DMN_TRANS_ATTR");				\
        break;								\
      case BSR_DMN_SS_ATTR:             /* 15 */                        \
        printf("     BSR_DMN_SS_ATTR");                                 \
        break;                                                          \
      case BSR_DMN_FREEZE_ATTR:         /* 16 */                        \
        printf("     BSR_DMN_FREEZE_ATTR");                             \
        break;                                                          \
      case BSR_RUN_TIMES:               /* 17 */                        \
        printf("     BSR_RUN_TIMES");					\
        break;								\
      case BSR_DMN_NAME:                /* 18 */			\
        printf("     BSR_DMN_NAME");					\
        break;								\
      case BSR_DMN_UMT_THAW_TIME:       /* 19 */                        \
        printf("  BSR_DMN_UMT_THAW_TIME");                              \
        break;                                                          \
      case BMTR_FS_DIR_INDEX_FILE:      /* 250 */                       \
        printf("BMTR_FS_DIR_INDEX_FILE");                               \
        break;                                                          \
      case BMTR_FS_INDEX_FILE:          /* 251 */                       \
        printf("    BMTR_FS_INDEX_FILE");                               \
        break;                                                          \
      case BMTR_FS_TIME:		/* 252 */			\
        printf("          BMTR_FS_TIME");				\
        break;								\
      case BMTR_FS_UNDEL_DIR:		/* 253 */			\
        printf("     BMTR_FS_UNDEL_DIR");				\
        break;								\
      case BMTR_FS_DATA:		/* 254 */			\
        printf("          BMTR_FS_DATA");				\
        break;								\
      case BMTR_FS_STAT:		/* 255 */			\
        printf("          BMTR_FS_STAT");				\
        break;								\
      default:								\
        printf("  *** unknown (%d) ***", type);				\
        break;								\
    }

static void
print_record(bsMRT *precord, int rn, int flags, bfTagT tag)
{
    int cnt;
    char *pdata;

    printf("\nRECORD %-2d  bCnt %-3d  ", rn, precord->bCnt);
    if ( flags & VFLG ) {
        printf("                      ");
    }

    PRINT_REC_TYPE(precord->type);
    if ( flags & VFLG ) {
        printf(" (type=%d)\n", precord->type);
    } else {
        printf("\n");
    }

    pdata = ((char *) precord) + sizeof(bsMRT);

    switch (precord->type) {
      case BSR_XTNTS:			/* 1 */
        printBSR_XTNTS((bsXtntRT *)pdata, flags, tag);
        break;
      case BSR_ATTR:			/* 2 */
        printBSR_ATTR((bsBfAttrT *)pdata, flags & VFLG);
        break;
      case BSR_VD_ATTR:			/* 3 */
        printBSR_VD_ATTR((bsVdAttrT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_ATTR:		/* 4 */
        printBSR_DMN_ATTR((bsDmnAttrT *)pdata, flags & VFLG);
        break;
      case BSR_XTRA_XTNTS:		/* 5 */
        printBSR_XTRA_XTNTS((bsXtraXtntRT *)pdata, flags & VFLG);
        break;
      case BSR_MCELL_FREE_LIST:		/* 6 */
        printBSR_MCELL_FREE_LIST((bsMcellFreeListT *)pdata, flags & VFLG);
        break;
      case BSR_BFS_ATTR:		/* 7 */
        printBSR_BFS_ATTR((bsBfSetAttrT *)pdata, flags & VFLG);
        break;
      case BSR_VD_IO_PARAMS:		/* 8 */
        printBSR_VD_IO_PARAMS((vdIoParamsT *)pdata, flags & VFLG);
        break;
      case BSR_TERTIARY_XTNTS:		
        printBSR_TERTIARY_XTNTS((bsTerXtntRT*)pdata, flags & VFLG);
        break;
      case BSR_TERTIARY_XTRA_XTNTS:	
        printBSR_TERTIARY_XTRA_XTNTS((bsTerXtraXtntRT*)pdata, flags & VFLG);
        break;
      case BSR_DEF_DEL_MCELL_LIST:	/* 9 */
        printBSR_DEF_DEL_MCELL_LIST((delLinkRT*)pdata, flags & VFLG);
        break;
      case BSR_DMN_MATTR:		/* 10 */
        printBSR_DMN_MATTR((bsDmnMAttrT *)pdata, flags & VFLG);
        break;
      case BSR_BF_INHERIT_ATTR:		/* 11 */
        printBSR_BF_INHERIT_ATTR((bsBfInheritAttrT *)pdata, flags & VFLG);
        break;
      case BSR_BFS_QUOTA_ATTR:		/* 12 */
        printBSR_BFS_QUOTA_ATTR((bsQuotaAttrT *)pdata, flags & VFLG);
        break;
      case BSR_ACL_REC:		        /* 13 */
	printBSR_ACL_REC((bsr_acl_rec_t *)pdata, flags & VFLG);
	break;
      case BSR_DMN_TRANS_ATTR:          /* 14 */
        printBSR_DMN_TRANS_ATTR((bsDmnTAttrT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_SS_ATTR:             /* 15 */
        printBSR_DMN_SS_ATTR((bsSSDmnAttrT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_FREEZE_ATTR:         /* 16 */
        printBSR_DMN_FREEZE_ATTR((bsDmnFreezeAttrT *)pdata, flags & VFLG);
        break;
      case BSR_RUN_TIMES:               /* 17 */
        printBSR_RUN_TIMES((bsRunTimesRT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_NAME:                /* 18 */
        printBSR_DMN_NAME((bsDmnNameT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_UMT_THAW_TIME:       /* 19 */
        printBSR_DMN_UMT_THAW_TIME((bsDmnUmntThawT *)pdata, flags);
        break;
      case BMTR_FS_DIR_INDEX_FILE:      /* 250 */
        printBMTR_FS_DIR_INDEX_FILE((bfTagT*)pdata, flags);
        break;
      case BMTR_FS_INDEX_FILE:          /* 251 */
        printBMTR_FS_INDEX_FILE((bsIdxBmtRecT*)pdata, flags);
        break;
      case BMTR_FS_TIME:		/* 252 */
        printBMTR_FS_TIME((int64_t *)pdata, flags & VFLG);
        break;
      case BMTR_FS_UNDEL_DIR:		/* 253 */
        printBMTR_FS_UNDEL_DIR((bsUndelDirT *)pdata, flags & VFLG);
        break;
      case BMTR_FS_DATA:		/* 254 */
        printBMTR_FS_DATA((char *)pdata, flags & VFLG);
        break;
      case BMTR_FS_STAT:		/* 255 */
        printBMTR_FS_STAT((statT *)pdata, flags & VFLG);
        break;
      default:
        cnt = precord->bCnt - sizeof(bsMRT);
        if (cnt <= BS_USABLE_MCELL_SPACE) {
            print_unknown(pdata, cnt);
        }
        break;
    }

}

/******************************************************************************/
/* mcell type 1 - BSR_XTNTS */

#define PRINT_BSXA(y, i) 						\
    if ( pdata->y[i].bsx_vd_blk == XTNT_TERM )                          \
        printf("bsXA[%2d]   bsx_fob_offset %9lu           bsx_vd_blk        -1\n",\
          i, pdata->y[i].bsx_fob_offset);                               \
    else if ( pdata->y[i].bsx_vd_blk == COWED_HOLE)               \
        printf("bsXA[%2d]   bsx_fob_offset %9lu           bsx_vd_blk        -2\n",\
          i, pdata->y[i].bsx_fob_offset);                               \
    else                                                                \
        printf("bsXA[%2d]   bsx_fob_offset %9lu           bsx_vd_blk %9lu (0x%lx)\n",\
          i, pdata->y[i].bsx_fob_offset, pdata->y[i].bsx_vd_blk,        \
          pdata->y[i].bsx_vd_blk)

static void
printBSR_XTNTS(bsXtntRT *pdata, int flags, bfTagT tag)
{
    int i;
    int cnt;

    printf("chain mcell volume page cell %d %ld %d\n",
      pdata->chainMCId.volume, pdata->chainMCId.page, pdata->chainMCId.cell);


    if ( flags & VFLG || flags & DDL ) {
        printf("delLink next volume,page,cell %d,%ld,%d\n        prev volume,page,cell %d,%ld,%d\n",
          pdata->delLink.nextMCId.volume, pdata->delLink.nextMCId.page,
          pdata->delLink.nextMCId.cell,
          pdata->delLink.prevMCId.volume, pdata->delLink.prevMCId.page,
          pdata->delLink.prevMCId.cell);
        printf("delRst volume,page,cell %d,%ld,%d  rsvd1 %d\n",
                pdata->delRst.mcid.volume, pdata->delRst.mcid.page,
                pdata->delRst.mcid.cell, pdata->delRst.rsvd1);
        printf("xtntIndex %d  drtFobOffset %ld  drtQuotaBlks %ld\n",
                pdata->delRst.xtntIndex, pdata->delRst.drtFobOffset,
                pdata->delRst.drtQuotaBlks);
    }

    if (flags & VFLG) {

        printf("mcellCnt %d  xCnt %d    rsvd1 %d  rsvd2 %d\n", 
	       pdata->mcellCnt, pdata->xCnt, pdata->rsvd1, pdata->rsvd2);

        if ( pdata->xCnt <= BMT_XTNTS ) {
            cnt = flags & VFLG ? BMT_XTNTS : pdata->xCnt;
        } else {
            fprintf(stderr,
              "**Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_XTNTS);
            cnt = BMT_XTNTS;
        }
    } else {
        printf("mcellCnt %d  xCnt %d\n", pdata->mcellCnt, pdata->xCnt);
	cnt = pdata->xCnt;
    }

    for (i = 0; i < cnt; i++) {
	PRINT_BSXA(bsXA,i);
    }

} /* printBSR_XTNTS */

/******************************************************************************/
/* mcell type 2 - BSR_ATTR */

#define PRINT_ATTR_STATE(state)						\
    switch ( state ) {							\
      case BSRA_INVALID:						\
        printf("BSRA_INVALID");						\
        break;								\
      case BSRA_CREATING:						\
        printf("BSRA_CREATING");					\
        break;								\
      case BSRA_DELETING:						\
        printf("BSRA_DELETING");					\
        break;								\
      case BSRA_VALID:							\
        printf("BSRA_VALID");						\
        break;								\
      default:								\
        printf("*** unknown (%d) ***", state);				\
        break;								\
    }

static void
printBSR_ATTR(bsBfAttrT *pdata, int vflg)
{
    printf("state ");
    PRINT_ATTR_STATE(pdata->state);
    if ( vflg ) {
        printf(" (%d)\n", pdata->state);
    } else {
        printf("\n");
    }

    if ( vflg ) {
        printf("bfPgSz %ld  ", pdata->bfPgSz);
        printf("transitionId %ld\n", pdata->transitionId);

        printf("bfat_orig_file_size %d  ", pdata->bfat_orig_file_size);
        printf("bfat_del_child_cnt %d\n", pdata->bfat_del_child_cnt);
	printf("\n");

        printf("reqServices %d  ", pdata->reqServices);
        printf("rsvd1 %ld\n", pdata->rsvd1);
    }

} /* printBSR_ATTR */

/******************************************************************************/
/* mcell type 3 - BSR_VD_ATTR */

static void
printBSR_VD_ATTR(bsVdAttrT *pdata, int vflg)
{
    char time_buf[40];

    if (ctime_r(&pdata->vdMntId, time_buf) == NULL) {
	strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    printf("Activation ID (vdMntId) 0x%lx.0x%lx  (%s)\n",
	   pdata->vdMntId.id_sec, pdata->vdMntId.id_usec, time_buf);

    if ( vflg ) {
        printf("state %d  ", pdata->state);
        printf("vdIndex %d  ", pdata->vdIndex);

	if (ctime_r(&pdata->vdCookie, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}

        printf("vdCookieT 0x%lx  (%s)\n", pdata->vdCookie, time_buf);

        printf("vdBlkCnt %ld  ", pdata->vdBlkCnt);
        printf("sbmBlksBit %ld  ", pdata->sbmBlksBit);
        printf("bmtXtntPgs %ld\n", pdata->bmtXtntPgs);
        printf("serviceClass %d  ", pdata->serviceClass);
        printf("userAllocSz %ld  ", pdata->userAllocSz);
        printf("metaAllocSz %ld  ", pdata->metaAllocSz);
        printf("blkSz %d\n", pdata->blkSz);
        printf("rsvd1 %d  ", pdata->rsvd1);
        printf("rsvd2 %d\n", pdata->rsvd2);
    } else {
        printf("vdIndex %d  ", pdata->vdIndex);
        printf("vdBlkCnt %d\n", pdata->vdBlkCnt);
    }
}

/******************************************************************************/
/* mcell type 4 - BSR_DMN_ATTR */

static void
printBSR_DMN_ATTR(bsDmnAttrT *pdata, int vflg)
{
    char time_buf[64];
    char time_buf2[64];

    if (ctime_r(&pdata->bfDomainId, time_buf) == NULL) {
	strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    if (ctime_r(&pdata->MasterFsCookie, time_buf2) == NULL) {
	strcpy(time_buf2, strerror(errno));
    } else {
	time_buf2[strlen(time_buf2) - 1] = '\0';
    }

    if ( vflg ) {
        printf("bfDomainId     %ld.%ld   (%s)\n",
          pdata->bfDomainId.id_sec, pdata->bfDomainId.id_usec, time_buf);
        printf("MasterFsCookie %ld.%ld   (%s)\n",
                pdata->MasterFsCookie.id_sec,
                pdata->MasterFsCookie.id_usec, time_buf2);
        printf("maxVds %d\n", pdata->maxVds);
        printf("bfSetDirTag %ld (%ld.%u)\n",
          pdata->bfSetDirTag.tag_num,
          pdata->bfSetDirTag.tag_num, pdata->bfSetDirTag.tag_seq);
        printf("rsvd1 %d   rsvd2 %d\n", pdata->rsvd1, pdata->rsvd2);
    } else {
        printf("bfDomainId 0x%lx.0x%lx   (%s)\n",
          pdata->bfDomainId.id_sec, pdata->bfDomainId.id_usec, time_buf);
    }
}

/******************************************************************************/
/* mcell type 5 - BSR_XTRA_XTNTS */

static void
printBSR_XTRA_XTNTS(bsXtraXtntRT *pdata, int vflg)
{
    int i;
    int cnt;

    if ( vflg ) {
        printf("xCnt %d\n", pdata->xCnt);

        if ( pdata->xCnt > BMT_XTRA_XTNTS ) {
            fprintf(stderr, "Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_XTRA_XTNTS);
        }

        for ( i = 0; i < BMT_XTRA_XTNTS; i++ ) {
            PRINT_BSXA(bsXA, i);
        }

        printf("rsvd1 %d   rsvd2 %d\n", pdata->rsvd1, pdata->rsvd2);
    } else {
        printf("xCnt %d\n", pdata->xCnt);
        if ( pdata->xCnt > BMT_XTRA_XTNTS ) {
            fprintf(stderr, "Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_XTRA_XTNTS);
            cnt = BMT_XTRA_XTNTS;
        } else {
            cnt = pdata->xCnt;
        }

        for ( i = 0; i < cnt; i++ ) {
            PRINT_BSXA(bsXA, i);
        }
    }
}

/******************************************************************************/
/* mcell type 6 - BSR_MCELL_FREE_LIST */

static void
printBSR_MCELL_FREE_LIST(bsMcellFreeListT *pdata, int vflg)
{
    printf("headPg %ld  rsvd1 %d\n", pdata->headPg, pdata->rsvd1);
}

/******************************************************************************/
/* mcell type 7 - BSR_BFS_ATTR */

#define PRINT_STATE(state)				\
    switch ( state ) {					\
      case BFS_INVALID:					\
        printf("BFS_INVALID");				\
        break;						\
      case BFS_READY:					\
        printf("BFS_READY");				\
        break;						\
      case BFS_BUSY:					\
        printf("BFS_BUSY");				\
        break;						\
      case BFS_DELETING:				\
        printf("BFS_DELETING");				\
        break;						\
      default:						\
        printf("*** unknown (%d) ***", state);		\
        break;						\
    }

#define PRINT_FLAGS(flags)				\
    if ( flags & BFS_OD_OUT_OF_SYNC )			\
        printf("BFS_OD_OUT_OF_SYNC ");                  \
    if ( flags & BFS_OD_HSM_MANAGED )	                \
        printf("BFS_OD_HSM_MANAGED ");                  \
    if ( flags & BFS_OD_HSM_MANAGED_REGIONS )	        \
        printf("BFS_OD_HSM_MANAGED_REGIONS ");          \
    if ( flags & BFS_OD_OBJ_SAFETY )                    \
        printf("BFS_OD_OBJ_SAFETY ");

static void
printBSR_BFS_ATTR(bsBfSetAttrT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    int prt_flg = 0;

    /* if in verbose mode */

    if ( vflg ) {

	if (ctime_r(&pdata->bfSetId.domainId, time_buf) == NULL) {
	    printf("bfSetId.domainId (time invalid)\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("bfSetId.domainId 0x%lx.%lx (%s)\n",
		   pdata->bfSetId.domainId.id_sec, 
		   pdata->bfSetId.domainId.id_usec,
		   time_buf);
	}
        printf("bfSetId.dirTag %ld (0x%lx.%x)\n",
	       pdata->bfSetId.dirTag.tag_num, pdata->bfSetId.dirTag.tag_num,
	       pdata->bfSetId.dirTag.tag_seq);

        printf("bfsaParentSnapSet.dirTag %ld (0x%lx.%x)\n",
	       pdata->bfsaParentSnapSet.dirTag.tag_num, 
	       pdata->bfsaParentSnapSet.dirTag.tag_num,
	       pdata->bfsaParentSnapSet.dirTag.tag_seq);

	if (ctime_r(&pdata->bfsaParentSnapSet.domainId, time_buf) == NULL) {
	    printf("bfsaParentSnapSet.domainId (time invalid)\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("bfsaParentSnapSet.domainId 0x%lx.%lx (%s)\n",
		   pdata->bfsaParentSnapSet.domainId.id_sec, 
		   pdata->bfsaParentSnapSet.domainId.id_usec,
		   time_buf);
	}

        printf("bfsaFirstChildSnapSet.dirTag %ld (0x%lx.%x)\n",
	       pdata->bfsaFirstChildSnapSet.dirTag.tag_num, 
	       pdata->bfsaFirstChildSnapSet.dirTag.tag_num,
	       pdata->bfsaFirstChildSnapSet.dirTag.tag_seq);

	if (ctime_r(&pdata->bfsaFirstChildSnapSet.domainId, time_buf) == NULL) {
	    printf("bfsaFirstChildSnapSet.domainId (time invalid)\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("bfsaFirstChildSnapSet.domainId 0x%lx.%lx (%s)\n",
		   pdata->bfsaFirstChildSnapSet.domainId.id_sec, 
		   pdata->bfsaFirstChildSnapSet.domainId.id_usec,
		   time_buf);
	}

        printf("bfsaNextSiblingSnap.dirTag %ld (0x%lx.%x)\n",
	       pdata->bfsaNextSiblingSnap.dirTag.tag_num, 
	       pdata->bfsaNextSiblingSnap.dirTag.tag_num,
	       pdata->bfsaNextSiblingSnap.dirTag.tag_seq);

        if (ctime_r(&pdata->bfsaNextSiblingSnap.domainId, time_buf) == NULL) {
	    printf("bfsaNextSiblingSnap.domainId  (time invalid)\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("bfsaNextSiblingSnap.domainId 0x%lx.%lx (%s)\n",
		   pdata->bfsaNextSiblingSnap.domainId.id_sec, 
		   pdata->bfsaNextSiblingSnap.domainId.id_usec,
		   time_buf);
	}

	printf("bfsaSnapLevel %d\n", pdata->bfsaSnapLevel);

        printf("nxtDelPendingBfSet %ld (0x%lx.%x)\n",
          pdata->nxtDelPendingBfSet.tag_num, pdata->nxtDelPendingBfSet.tag_num,
          pdata->nxtDelPendingBfSet.tag_seq);
        printf("state ");
        PRINT_STATE(pdata->state);
        printf("   ");
        printf("flags 0x%x ", pdata->flags);
        PRINT_FLAGS(pdata->flags);
        printf("\n");
        printf("fsDev 0x%lx   ", pdata->fsDev);
        printf("uid %d   ", pdata->uid);
        printf("gid %d   ", pdata->gid);
        printf("mode %04o   ", pdata->mode);
        printf("setName \"%s\"\n", pdata->setName);

	if (ctime_r(&pdata->bfsaFilesetCreate, time_buf) == NULL) {
	    printf("bfsaFilesetCreate (invalid time)\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("bfsaFilesetCreate (%s)\n", time_buf);
	}

    } else {

	/* not in verbose mode */

        printf("setName \"%s\"   ", pdata->setName);
        printf("state ");
        PRINT_STATE(pdata->state);
        printf("   ");
        printf("bfSetId.dirTag %ld\n", pdata->bfSetId.dirTag.tag_num);
        printf("rsvd1 %d   rsvd2 %d\n", pdata->rsvd1,
                pdata->rsvd2);

        if ( pdata->bfsaParentSnapSet.dirTag.tag_num != 0 ) {
            printf("bfsaParentSnapSet.dirTag %ld  ", 
		   pdata->bfsaParentSnapSet.dirTag.tag_num);
            prt_flg = 1;
        }
        if ( pdata->bfsaNextSiblingSnap.dirTag.tag_num != 0 ) {
            printf("bfsaNextSiblingSnap.dirTag %ld  ", 
		   pdata->bfsaNextSiblingSnap.dirTag.tag_num);
            prt_flg = 1;
        }
        if ( pdata->bfsaFirstChildSnapSet.dirTag.tag_num != 0 ) {
            printf("bfsaFirstChildSnapSet.dirTag %ld  ", 
		   pdata->bfsaFirstChildSnapSet.dirTag.tag_num);
            prt_flg = 1;
        }

        if ( prt_flg ) {
            printf("\n");
        }

	prt_flg = 0;

	if (pdata->bfsaSnapLevel) {
	    printf("bfsaSnapLevel %d  ", pdata->bfsaSnapLevel);
            prt_flg = 1;
	}

        if ( pdata->nxtDelPendingBfSet.tag_num != 0 ) {
            printf("nxtDelPendingBfSet %ld   ", 
		   pdata->nxtDelPendingBfSet.tag_num);
            prt_flg = 1;
        }

	if (pdata->bfsaFilesetCreate) {
	    if (ctime_r(&pdata->bfsaFilesetCreate, time_buf) == NULL) {
		printf("bfsaFilesetCreate (invalid time)\n");
	    } else {
		time_buf[strlen(time_buf) - 1] = '\0';
		printf("bfsaFilesetCreate (%s)\n", time_buf);
	    }
            prt_flg = 1;
	}

        if ( prt_flg ) {
            printf("\n");
        }
    }

} /* printBSR_BFS_ATTR */

/******************************************************************************/
/* mcell type 8 - BSR_VD_IO_PARAMS */

static void
printBSR_VD_IO_PARAMS(vdIoParamsT *pdata, int vflg)
{
    vflg = 1;		/* tmp */

    if ( vflg ) {
        printf("rdMaxIo %d\n", pdata->rdMaxIo);
        printf("wrMaxIo %d\n", pdata->wrMaxIo);
	printf("vdiop_reserved1 0x%x\n", pdata->vdiop_reserved1);
	printf("vdiop_reserved2 0x%x\n", pdata->vdiop_reserved2);
	printf("vdiop_reserved3 0x%x\n", pdata->vdiop_reserved3);
	printf("vdiop_reserved4 0x%x\n", pdata->vdiop_reserved4);
    } else {
        printf("rdMaxIo %d  wrMaxIo %d\n", pdata->rdMaxIo, pdata->wrMaxIo);
    }
}

/******************************************************************************/
/*  BSR_TERTIARY_XTNTS */

static void
printBSR_TERTIARY_XTNTS(bsTerXtntRT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    uint cnt;

    if ( vflg ) {
        printf("totalXtnts %d\n", pdata->totalXtnts);
        printf("xCnt %d\n", pdata->xCnt);
        for (i = 0; i < BMT_TERTIARY_XTNTS; i++) {
            printf("bsXA[%2d]  bsPage %5ld  mediaId %d  flags 0x%x\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId,
              pdata->bsXA[i].flags);

	    if (ctime_r(&pdata->bsXA[i].accessTime, time_buf) == NULL) {
		strcpy(time_buf, strerror(errno));
	    } else {
		time_buf[strlen(time_buf) - 1] = '\0';
	    }

            printf("           accessTime %s  mediaRefNum high,low %d,%d\n",
              time_buf, pdata->bsXA[i].mediaRefNum.high,
              pdata->bsXA[i].mediaRefNum.low);
        }
    } else {
        printf("totalXtnts %d\n", pdata->totalXtnts);
        printf("xCnt %d\n", pdata->xCnt);
        if ( pdata->xCnt <= BMT_TERTIARY_XTNTS )
            cnt = pdata->xCnt;
        else
            cnt = BMT_TERTIARY_XTNTS;

        for (i = 0; i < cnt; i++) {

	    if (ctime_r(&pdata->bsXA[i].accessTime, time_buf) == NULL) {
		strcpy(time_buf, strerror(errno));
	    } else {
		time_buf[strlen(time_buf) - 1] = '\0';
	    }

            printf("bsXA[%2d]  bsPage %5ld  mediaId %d  accessTime %s\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId, time_buf);
        }
    }
}

/******************************************************************************/
/* BSR_TERTIARY_XTRA_XTNTS */

static void
printBSR_TERTIARY_XTRA_XTNTS(bsTerXtraXtntRT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    uint cnt;

    if ( vflg ) {
        printf("xCnt %d\n", pdata->xCnt);
        for (i = 0; i < BMT_TERTIARY_XTRA_XTNTS; i++) {
            printf("bsXA[%2d]  bsPage %5ld  mediaId %d  flags 0x%x\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId,
              pdata->bsXA[i].flags);

	    if (ctime_r(&pdata->bsXA[i].accessTime, time_buf) == NULL) {
		strcpy(time_buf, strerror(errno));
	    } else {
		time_buf[strlen(time_buf) - 1] = '\0';
	    }
            printf("           accessTime %s  mediaRefNum high,low %d,%d\n",
              time_buf, pdata->bsXA[i].mediaRefNum.high,
              pdata->bsXA[i].mediaRefNum.low);
        }
    } else {
        printf("xCnt %d\n", pdata->xCnt);
        cnt = pdata->xCnt <= BMT_TERTIARY_XTRA_XTNTS ? pdata->xCnt : BMT_TERTIARY_XTRA_XTNTS;
        for (i = 0; i < cnt; i++) {
	    if (ctime_r(&pdata->bsXA[i].accessTime, time_buf) == NULL) {
		strcpy(time_buf, strerror(errno));
	    } else {
		time_buf[strlen(time_buf) - 1] = '\0';
	    }
            printf("bsXA[%2d]  bsPage %5ld  mediaId %d  accessTime %s\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId, 
	      time_buf);
        }
    }
}

/******************************************************************************/
/* mcell type 9 - BSR_DEF_DEL_MCELL_LIST */

static void
printBSR_DEF_DEL_MCELL_LIST(delLinkRT *pdata, int vflg)
{
    printf("nextMCId volume,page,cell %d,%ld,%d  prevMCId volume,page,cell %d,%ld,%d\n",
      pdata->nextMCId.volume, pdata->nextMCId.page, pdata->nextMCId.cell,
      pdata->prevMCId.volume, pdata->prevMCId.page, pdata->prevMCId.cell);
    if ( pdata->prevMCId.page != 0 || pdata->prevMCId.cell != 0 ) {
        fprintf(stderr, "** Bad prevMCId. Should be 0.\n");
    }

}

/******************************************************************************/
/* mcell type 10 - BSR_DMN_MATTR */

static void
printBSR_DMN_MATTR(bsDmnMAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("ODSVersion %d.%d  ", pdata->ODSVersion.odv_major,
                pdata->ODSVersion.odv_minor);
        printf("seqNum %d  ", pdata->seqNum);
        printf("delPendingBfSet %ld (0x%lx.%x)\n", pdata->delPendingBfSet.tag_num,
          pdata->delPendingBfSet.tag_num, pdata->delPendingBfSet.tag_seq);
        printf("uid %d, ", pdata->uid);
        printf("gid %d, ", pdata->gid);
        printf("mode %04o, ", pdata->mode);
        printf("vdCnt %d, ", pdata->vdCnt);
        printf("domain state %d\n", pdata->bs_dma_dmn_state);
        printf("bfSetDirTag %ld, ", pdata->bfSetDirTag.tag_num);
        printf("ftxLogTag %ld, ", pdata->ftxLogTag.tag_num);
        printf("ftxLogPgs %d\n", pdata->ftxLogPgs);
        printf("rsvd1  %ld    rsvd2  %ld\n", pdata->rsvd1, pdata->rsvd2);
    } else {
        printf("uid %d, ", pdata->uid);
        printf("gid %d, ", pdata->gid);
        printf("mode %04o, ", pdata->mode);
        printf("vdCnt %d\n", pdata->vdCnt);
        if ( pdata->bs_dma_dmn_state ) {
            printf("domain_state %d\n", pdata->bs_dma_dmn_state);
        }
        if ( pdata->delPendingBfSet.tag_num ) {
            printf("delPendingBfSet %ld\n", pdata->delPendingBfSet.tag_num);
        }
    }
}

/******************************************************************************/
/* mcell type 11 - BSR_BF_INHERIT_ATTR */

static void
printBSR_BF_INHERIT_ATTR(bsBfInheritAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("\n");
        printf("reqServices %d\n", pdata->reqServices);
    } else {
        printf("reqServices %d\n", pdata->reqServices);
    }
}

/******************************************************************************/
/* mcell type 12 - BSR_BFS_QUOTA_ATTR */

static void
printBSR_BFS_QUOTA_ATTR(bsQuotaAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("blkHLimit %ld \t",  pdata->blkHLimit);
        printf("blkSLimit %ld \t",  pdata->blkSLimit);
        printf("fileHLimit %ld\n", pdata->fileHLimit);
        printf("fileSLimit %ld \t", pdata->fileSLimit);
        printf("blkTLimit %ld \t",  pdata->blkTLimit);
        printf("fileTLimit %ld\n", pdata->fileTLimit); 
        printf("quotaStatus %d  ",  pdata->quotaStatus);
        printf("rsvd1 %d \trsvd2 %ld\n", pdata->rsvd1, pdata->rsvd2);
    } else {
        printf("Blocks hard limit %ld,  soft limit %ld,  blkTLimit %ld\n",
           pdata->blkHLimit, pdata->blkSLimit, pdata->blkTLimit);
        printf("Files hard limit %ld,  soft limit %ld,  fileTLimit %ld\n",
           pdata->fileHLimit, pdata->fileSLimit, pdata->fileTLimit);
    }
}

/******************************************************************************/
/* mcell type 13 - BSR_ACL_REC */
static void
printBSR_ACL_REC(bsr_acl_rec_t *pdata, int vflg)
{
    int i;

    printf("bar_acl_total_cnt %d  bar_acl_cnt %d  bar_acl_link_seg %d\n",
	   pdata->bar_hdr.bar_acl_total_cnt, 
	   pdata->bar_hdr.bar_acl_cnt, 
	   pdata->bar_hdr.bar_acl_link_seg);

    if (pdata->bar_hdr.bar_acl_type == ADVFS_ACL_TYPE) {
	printf("bar_acl_type  ADVFS_ACL_TYPE\n");
    } else {
	printf("bar_acl_type %d\n", pdata->bar_hdr.bar_acl_type);
    }

    printf("\n");

    for (i = 0; 
	 i < pdata->bar_hdr.bar_acl_cnt && i < ADVFS_ACLS_PER_MCELL; 
	 i++) {

	printf("ACL entry[%d]:  aa_a_type ", i);

	switch (pdata->bar_acl[i].aa_a_type) {
  
	case ADVFS_USER_OBJ:
	    printf("ADVFS_USER_OBJ   ");
	    break;
	case ADVFS_USER:
	    printf("ADVFS_USER       ");
	    break;
	case ADVFS_GROUP_OBJ:
	    printf("ADVFS_GROUP_OBJ  ");
	    break;
	case ADVFS_GROUP:
	    printf("ADVFS_GROUP      ");
	    break;
	case ADVFS_CLASS_OBJ:
	    printf("ADVFS_CLASS_OBJ  ");
	    break;
	case ADVFS_OTHER_OBJ:
	    printf("ADVFS_OTHER_OBJ  ");
	    break;
	case ADVFS_DEF_USER_OBJ:
	    printf("ADVFS_DEF_USER_OBJ ");
	    break;
	case ADVFS_DEF_USER:
	    printf("ADVFS_DEF_USER   ");
	    break;
	case ADVFS_DEF_GROUP_OBJ:
	    printf("ADVFS_DEF_GROUP_OBJ ");
	    break;
	case ADVFS_DEF_GROUP:
	    printf("ADVFS_DEF_GROUP  ");
	    break;
	case ADVFS_DEF_CLASS_OBJ:
	    printf("ADVFS_DEF_CLASS_OBJ ");
	    break;
	case ADVFS_DEF_OTHER_OBJ:
	    printf("ADVFS_DEF_OTHER_OBJ ");
	    break;
	default:
	    printf("0x%x  ", pdata->bar_acl[i].aa_a_type);
	    break;
	}

	printf("aa_a_id %d  aa_a_perm 0x%x\n", 
	       pdata->bar_acl[i].aa_a_id,
	       pdata->bar_acl[i].aa_a_perm);
    }

}  /* end printBSR_ACL_REC */

/* mcell type 14 - BSR_DMN_TRANS_ATTR */
static void
printBSR_DMN_TRANS_ATTR(bsDmnTAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("chainMCId volume, page,cell %d,%ld,%d\n", 
                pdata->chainMCId.volume, pdata->chainMCId.page,
                pdata->chainMCId.cell);
        printf("op %d\n", pdata->op);
        printf("dev 0x%lx\n", pdata->dev);
    } else {
        printf("\n");
    }
}

/******************************************************************************/
/* mcell type 15 - BSR_DMN_SS_ATTR */
static void
printBSR_DMN_SS_ATTR(bsSSDmnAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("ssDmnState                (%s)\n",
          (pdata->ssDmnState == 0) ? "SS_DEACTIVATED":
          (pdata->ssDmnState == 1) ? "SS_ACTIVATED":
          (pdata->ssDmnState == 2) ? "SS_SUSPEND":
          (pdata->ssDmnState == 3) ? "SS_ERROR":
          (pdata->ssDmnState == 4) ? "SS_CFS_RELOC":
          (pdata->ssDmnState == 5) ? "SS_CFS_MOUNT":
          (pdata->ssDmnState == 6) ? "SS_CFS_UMOUNT": "unknown??" );

        printf("ssDmnDefragment = %d \t", pdata->ssDmnDefragment);
        printf("ssDmnDefragAll  = %d \t", pdata->ssDmnDefragAll);
        printf("ssDmnSmartPlace = %d \t", pdata->ssDmnSmartPlace);
        printf("ssDmnBalance    = %d\n", pdata->ssDmnBalance);
        printf("ssDmnVerbosity  = %d \t", pdata->ssDmnVerbosity);
        printf("ssDmnDirectIo   = %d \t", pdata->ssDmnDirectIo);
        printf("ssMaxPercentOfIoWhenBusy = %d\n",
                pdata->ssMaxPercentOfIoWhenBusy);
        printf("ssFilesDefraged = %ld \t", pdata->ssFilesDefraged);
        printf("ssFobsDefraged  = %ld \t", pdata->ssFobsDefraged);
        printf("ssFobsBalanced  = %ld\n", pdata->ssFobsBalanced);
        printf("ssFilesIOBal    = %ld \t", pdata->ssFilesIOBal);
        printf("ssExtentsConsol = %ld \t", pdata->ssExtentsConsol);
        printf("ssPagesConsol   = %ld \n", pdata->ssPagesConsol);
        printf("ssAccessThreshHits = %d \t", pdata->ssAccessThreshHits);
        printf("ssSteadyState = %d  ", pdata->ssSteadyState);
        printf("ssMinutesInHotList = %d\n", pdata->ssMinutesInHotList);
        printf("ssReserved1     = %d \t", pdata->ssReserved1);
        printf("ssReserved2     = %d\n", pdata->ssReserved2);
        printf("ssReserved3     = %ld \t", pdata->ssReserved3);
        printf("ssReserved4     = %ld \n", pdata->ssReserved4);
    } else {
        printf("ssDmnState                (%s)\n",
                (pdata->ssDmnState == 0) ? "SS_DEACTIVATED":
                (pdata->ssDmnState == 1) ? "SS_ACTIVATED":
                (pdata->ssDmnState == 2) ? "SS_SUSPEND":
                (pdata->ssDmnState == 3) ? "SS_ERROR":
                (pdata->ssDmnState == 4) ? "SS_CFS_RELOC":
                (pdata->ssDmnState == 5) ? "SS_CFS_MOUNT":
                (pdata->ssDmnState == 6) ? "SS_CFS_UMOUNT": "unknown??" );
    }
}

/* mcell type 16 - BSR_DMN_FREEZE_ATTR */

static void
printBSR_DMN_FREEZE_ATTR(bsDmnFreezeAttrT *pdata, int vflg)
{

    char time_buf[40];

    if (0 == pdata->freezeBegin) {
	printf("freezeBegin 0x%lx\n", 
	       pdata->freezeBegin);
    } else {
	if (ctime_r(&pdata->freezeBegin, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}
	printf("freezeBegin %s\n", time_buf);
    }
    if (0 == pdata->freezeEnd) {
	printf("freezeEnd 0x%lx\n", 
	       pdata->freezeEnd);
    } else {
	if (ctime_r(&pdata->freezeEnd, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}
	printf("freezeEnd   %s\n", time_buf);
    }

    printf("freezeCount %d\n", pdata->freezeCount);

    if ( vflg ) {
        printf("rsvd1 %d  rsvd2 %d  rsvd3 %d\n",
          pdata->rsvd1, pdata->rsvd2, pdata->rsvd3);
    }
}


/******************************************************************************/
/* mcell type 17 - BSR_RUN_TIMES */

static void
printBSR_RUN_TIMES(bsRunTimesRT *pdata, int vflg)
{
    char time_buf[40];
    if (pdata->rt_active_fsck == 0) {
        printf("active_fsck %lx\n", pdata->rt_active_fsck);
    } else {
	if (ctime_r(&pdata->rt_active_fsck, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}
        printf("active_fsck %s\n", time_buf);
    }

    if (pdata->rt_unmounted_fsck == 0) {
        printf("unmounted_fsck %lx\n", pdata->rt_unmounted_fsck);
    } else {
	if (ctime_r(&pdata->rt_unmounted_fsck, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}
        printf("unmounted_fsck %s\n", time_buf);
    }

    if (pdata->rt_salvage == 0) {
        printf("salvage %lx\n", pdata->rt_salvage);
    } else {
	if (ctime_r(&pdata->rt_unmounted_fsck, time_buf) == NULL) {
	    strcpy(time_buf, strerror(errno));
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	}
        printf("salvage %s\n", time_buf);
    }

    printf("rsvd1  %ld   rsvd2  %ld   rsvd3  %ld\n",
            pdata->rt_rsvd1, pdata->rt_rsvd2, pdata->rt_rsvd3);
    printf("rsvd4  %ld   rsvd 5 %ld\n",
            pdata->rt_rsvd4, pdata->rt_rsvd5);

}



/******************************************************************************/
/* mcell type 18 - BSR_DMN_NAME */
static void
printBSR_DMN_NAME(bsDmnNameT *pdata, int vflg)
{
    printf("FileSystem Name \"%s\"\n", pdata->dmn_name);
}


/******************************************************************************/
/* mcell type 19 - BSR_DMN_UMT_THAW_TIME */
static void
printBSR_DMN_UMT_THAW_TIME(bsDmnUmntThawT *pdata, int vflg)
{
    char time_buf[40];
    ctime_r(&pdata->dmnUmntThawTime, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';
    printf("Last Unmount/Thaw time  0x%lx.0x%lx  (%s)\n",
            pdata->dmnUmntThawTime.id_sec, pdata->dmnUmntThawTime.id_usec,
            time_buf);

}


/******************************************************************************/
/* mcell type 250 - BMTR_FS_DIR_INDEX_FILE */
static void
printBMTR_FS_DIR_INDEX_FILE(bfTagT *bfTagp, int flags)
{
    printf("tag %ld (%ld.%u)\n", bfTagp->tag_num, bfTagp->tag_num, bfTagp->tag_seq);
}

/******************************************************************************/
/* mcell type 251 - BMTR_FS_INDEX_FILE */
static void
printBMTR_FS_INDEX_FILE(bsIdxBmtRecT *bsIdxBmtRecp, int flags)
{
    printf("fname_page %d  ", bsIdxBmtRecp->fname_page);
    printf("ffree_page %d  ", bsIdxBmtRecp->ffree_page);
    printf("fname_levels %d  ", bsIdxBmtRecp->fname_levels);
    printf("ffree_levels %d\n", bsIdxBmtRecp->ffree_levels);
    printf("seqDirTag %ld (%ld.%u)\n", bsIdxBmtRecp->seqDirTag.tag_num,
            bsIdxBmtRecp->seqDirTag.tag_num, bsIdxBmtRecp->seqDirTag.tag_seq);
}

/******************************************************************************/
/* mcell type 252 - BMTR_FS_TIME */
static void
printBMTR_FS_TIME(int64_t *fs_time, int vflg)
{
    char time_buf[128];

    if (ctime_r(fs_time, time_buf) == NULL) {
	printf("last sync: invalid time value\n");
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
	printf("last sync '%s'\n", time_buf);
    }

}

/******************************************************************************/
/* mcell type 253 - BMTR_FS_UNDEL_DIR */
static void
printBMTR_FS_UNDEL_DIR(bsUndelDirT *undel_dir, int vflg)
{
    printf("dir_tag %ld (%ld.%u)\n",
      undel_dir->dir_tag.tag_num, undel_dir->dir_tag.tag_num, undel_dir->dir_tag.tag_seq);
    printf("rsvd1 %d, rsvd2 %d\n", undel_dir->rsvd1, undel_dir->rsvd2);
}

/******************************************************************************/
/* mcell type 254 - BMTR_FS_DATA */

static void
printBMTR_FS_DATA(char *pdata, int vflg)
{
    char *str;
    int len;
    bsMRT *recHdr;

    /* this record holds the symbolic link string.  The string is not
       NULL terminated, which causes printf to print whatever garbage
       follows the string, which can wedge the terminal.  So we
       calculate how big the string is and print only that much. */

    recHdr = (bsMRT *)(pdata - sizeof(bsMRT));
    len = recHdr->bCnt - sizeof(bsMRT);
    str = malloc(len);

    strncpy(str, pdata, len);
    str[len] = '\0';

    printf("\"%s\"\n", str);

    free(str);
}

/******************************************************************************/
/* mcell type 255 - BMTR_FS_STAT */

#define PRINT_MODE(mode)						\
    switch ( mode & S_IFMT ) {						\
      case S_IFREG:							\
        printf("(S_IFREG)");						\
        break;								\
      case S_IFDIR:							\
        printf("(S_IFDIR)");						\
        break;								\
      case S_IFBLK:							\
        printf("(S_IFBLK)");						\
        break;								\
      case S_IFCHR:							\
        printf("(S_IFCHR)");						\
        break;								\
      case S_IFIFO:							\
        printf("(S_IFIFO)");						\
        break;								\
      case S_IFSOCK:                                                    \
        printf("(S_IFSOCK)");                                           \
        break;								\
      case S_IFLNK:                                                     \
        printf("(S_IFLNK)");						\
        break;								\
      default:								\
        printf("*** unknown file type (%o) ***", (mode & S_IFMT) >> 12);\
        break;								\
    }


static void
printBMTR_FS_STAT(statT *pdata, int vflg)
{
    char *s, *d;
    char time_buf[128];

    if ( vflg ) {
         printf("st_ino %ld.%d   ", pdata->st_ino.tag_num, 
                 pdata->st_ino.tag_seq);
        printf("st_mode %04o ", pdata->st_mode);
        PRINT_MODE(pdata->st_mode);
        printf("  st_nlink %lu   ", pdata->st_nlink);
        printf("st_size %lu\n", pdata->st_size); 
        printf("st_uid %d   ", pdata->st_uid);
        printf("st_gid %d   ", pdata->st_gid);
        if ((pdata->st_mode & S_IFMT) == S_IFBLK ||
                (pdata->st_mode & S_IFMT) == S_IFCHR ) {
            printf("st_rdev %d major %d minor %d\n", pdata->st_rdev,
                    major(pdata->st_rdev), minor(pdata->st_rdev));
        }
        if (ctime_r(&pdata->st_mtime, time_buf) == NULL) {
	    printf("invaid mtime value in stat data  ");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("st_mtime '%s'  ", time_buf);
	}

        printf("st_nmtime %d\n", pdata->st_nmtime);

	if (ctime_r(&pdata->st_atime, time_buf) == NULL) {
            printf("invaid atime value in stat data  ");
	} else {
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("st_atime '%s'  ", time_buf);
        }

        printf("st_natime %d\n", pdata->st_natime);

	if (ctime_r(&pdata->st_ctime, time_buf) == NULL) {
	    printf("invaid ctime value in stat data  ");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("st_ctime '%s'  ", time_buf);
	}

        printf("st_nctime %d\n", pdata->st_nctime);

        printf("st_flags %d\n", pdata->st_flags);
        printf("dir_tag %ld (%ld.%u)\n",
        pdata->dir_tag.tag_num, pdata->dir_tag.tag_num, pdata->dir_tag.tag_seq);

    } else {
        /* mode uid gid length (dev for special) */
        /* links parent_dir mtime */
        printf("st_mode %04o ", pdata->st_mode);
        PRINT_MODE(pdata->st_mode);
        printf("   ");

        printf("st_uid %d   ", pdata->st_uid);
        printf("st_gid %d   ", pdata->st_gid);

        if ( (pdata->st_mode & S_IFMT) == S_IFBLK ||
             (pdata->st_mode & S_IFMT) == S_IFCHR )
        {
            printf("st_rdev %d major %d minor %d\n", pdata->st_rdev,
              major(pdata->st_rdev), minor(pdata->st_rdev));
        } else {
            printf("st_size %lu\n", pdata->st_size); 
        }

        printf("st_nlink %lu   ", pdata->st_nlink);
        printf("dir_tag %ld  ", pdata->dir_tag.tag_num);

        if (ctime_r(&pdata->st_mtime, time_buf) == NULL) {
	    printf("invaid mtime value in stat data\n");
	} else {
	    time_buf[strlen(time_buf) - 1] = '\0';
	    printf("st_mtime '%s'\n", time_buf);
	}
    }
}
