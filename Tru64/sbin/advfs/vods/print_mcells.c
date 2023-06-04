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
 *      AdvFS
 *
 * Abstract:
 *
 *      View On-disk Structure
 *
 * Date:
 *
 *      Thu Nov 17 09:06:33 PST 1994
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: print_mcells.c,v $ $Revision: 1.1.27.5 $ (DEC) $Date: 2006/08/30 15:25:46 $"

#include <stdio.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_index.h>

#include "vods.h"

/******************************************************************************/
/* obsolete shelving definitions */

typedef struct bsTerXtraXtntR {
    uint32T xCnt;               /* Count of elements used in bsXA */
    bsTerXtntT bsXA[BMT_TERTIARY_XTRA_XTNTS];/* Array of disk extent desc */
} bsTerXtraXtntRT;

#define BSR_SET_SHELVE_ATTR      17

typedef struct {
    int32T          flags;
    int32T          smallFile;
    int32T          readAhead;
    int32T          readAheadIncr;
    int32T          readAheadMax;
    int32T          autoShelveThresh;
    uint32T         userId;
    mssShelfIdT     shelf;
} bfSetShelveAttrT;

#define MSS_ROOT_BLOCKED 0x1            /* get only - root users are       */
                                        /*   blocked waiting for mag space */
#define MSS_USER_BLOCKED 0x2            /* get only - non-root users are   */
                                        /*   blocked waiting for mag space */
#define MSS_NO_SHELVE    0x4            /* shelving is disabled for this   */
                                        /*   file set                      */
#define MSS_UPD_MEDIA    0x8            /* set only - really update shelfId*/
#define MSS_REBUILDING   0x10           /* get only - EFS rebuild op is in */
                                        /*   progress for this file set    */


/******************************************************************************/

extern int dmnVersion;          /* from vods.c */

/* private prototypes */
static print_record(bsMRT *, int, int, int, bfTagT);
static printBSR_XTNTS(bsXtntRT *, int, int, bfTagT);
static printBMTR_FS_STAT(statT *, int);

/******************************************************************************/
print_BMT_page_header ( bitFileT *bfp, bsMPgT *pdata, int vflg)
{
    char line[80];
    int  line_len = 0;
    char *volume;
    line[79] = '\0';		/* in case sprintf fails */

    print_header(bfp);

    if ( bfp->pgNum != -1 && bfp->pgNum != pdata->pageId ) {
        printf("pageId (%d) doesn't match requested page %d\n",
          pdata->pageId, bfp->pgNum);
    }

    if ( vflg ) {
        printf("pageId %d  ", pdata->pageId);
        printf("megaVersion %d\n", pdata->megaVersion);
        printf("freeMcellCnt %-3d  ", pdata->freeMcellCnt);
        printf("nextFreePg %-3d  ", pdata->nextFreePg);
        printf("nextfreeMCId page,cell %d,%d\n",
          pdata->nextfreeMCId.page, pdata->nextfreeMCId.cell);
    }
}

/******************************************************************************/
#define RBMT_VERSION(version) ( version >= FIRST_RBMT_VERSION )

#define PRINT_RSVD_NAMES(tag, version)					\
    switch ( (tag) % 6 ) {						\
      case 0:								\
        printf(RBMT_VERSION(version) ? "(RBMT)" : " (BMT)" );		\
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
        printf(RBMT_VERSION(version) ? " (BMT)" : " (???)");		\
        break;								\
      case -5:								\
        printf(" (Misc)");						\
        break;								\
      default:								\
        break;								\
    }

print_cell_hdr(bsMCT *pdata, int cn, int version, int vflg)
{
    char buf[80];
    int len;
    int i;

    printf("CELL %d  ", cn);
    if ( vflg ) {
        sprintf(buf, "%d%d%d %x%x%d %x%x", cn, pdata->linkSegment,
          pdata->bfSetTag.num, pdata->bfSetTag.num, pdata->bfSetTag.seq,
          pdata->tag.num, pdata->tag.num, pdata->tag.seq);
        len = strlen(buf);
        if ( BS_BFTAG_RSVD(pdata->tag) ) {
            len += 6;
        }

        if ( len < 32 ) {
            len = 32 - len;
        } else {
            len = 0;
        }

        printf("linkSegment %d  ", pdata->linkSegment);
        for ( i = 0; i < len; i++ ) {
            printf(" ");
        }

        printf("bfSetTag %d (%x.%x)  tag %d (%x.%x)",
          pdata->bfSetTag.num, pdata->bfSetTag.num, pdata->bfSetTag.seq,
          pdata->tag.num, pdata->tag.num, pdata->tag.seq);
        if ( BS_BFTAG_RSVD(pdata->tag) ) {
            PRINT_RSVD_NAMES((int)pdata->tag.num, version);
        }

        printf("\n");
        printf("next mcell volume page cell %d %d %d\n\n",
          pdata->nextVdIndex, pdata->nextMCId.page, pdata->nextMCId.cell);
    } else {
        sprintf(buf, "%d%d%d%d%d%d", cn,
          pdata->nextVdIndex, pdata->nextMCId.page, pdata->nextMCId.cell,
          pdata->bfSetTag.num, pdata->tag.num);
        len = strlen(buf);
        if ( BS_BFTAG_RSVD(pdata->tag) ) {
            len += 6;
        }

        if ( len < 20 ) {
            len = 20 - len;
        } else {
            len = 0;
        }

        for ( i = 0; i < len / 2; i++ ) {
            printf(" ");
        }

        printf("next mcell volume page cell %d %d %d   ",
          pdata->nextVdIndex, pdata->nextMCId.page, pdata->nextMCId.cell);
        for ( i = 0; i < len / 2; i++ ) {
            printf(" ");
        }

        if ( len & 0x1 ) {
            printf(" ");
        }

        printf("bfSetTag,tag %d,%d", pdata->bfSetTag.num, pdata->tag.num);
        if ( BS_BFTAG_RSVD(pdata->tag) ) {
            PRINT_RSVD_NAMES((int)pdata->tag.num, version);
        }

        printf("\n\n");
    }
}

/******************************************************************************/
print_cell(bsMCT *bsMCp, int cn, int flags, int dmnVersion)
{
    bsMRT *bsMRp;
    int rn;

    bsMRp = (bsMRT *)bsMCp->bsMR0;

    if ( !(flags & ONECELL) && !(flags & VFLG) && bsMRp->type == 0 ) {
        return;
    }

    if ( BS_BFTAG_RSVD(bsMCp->tag) ) {
        flags |= RSVDBF;
    }

    if ( flags & VFLG || cn != 0 && !(flags & ONECELL) )
        printf(SINGLE_LINE);

    print_cell_hdr(bsMCp, cn, dmnVersion, flags & VFLG);

    /* Special handling of Misc file with 3 primary extent entries. */
    if ( dmnVersion >= 3 &&
         (signed)bsMCp->bfSetTag.num == -2 &&
         flags & RSVDBF &&
         BS_BFTAG_CELL(bsMCp->tag) == BFM_MISC &&
         cn == BFM_MISC )
    {
        flags |= MISCF;
        if ( dmnVersion == 3 ) {
            flags |= V3DMN;
        }
    }

    rn=0;
    while ( bsMRp->type != 0 && bsMRp < (bsMRT *)&bsMCp->bsMR0[BSC_R_SZ] ) {
        print_record(bsMRp, rn, flags, dmnVersion, bsMCp->tag);
        if ( (char *)bsMRp + bsMRp->bCnt > &bsMCp->bsMR0[BSC_R_SZ] ) {
            fprintf(stderr, "Bad mcell record: bCnt too large\n");
            break;
        }
        if ( bsMRp->bCnt == 0 ) {
            fprintf(stderr, "Bad mcell record %d. bCnt == 0\n", rn);
            break;
        }

        rn++;
        bsMRp = (bsMRT*)((char*)bsMRp + roundup(bsMRp->bCnt,sizeof(int)));
    }
}

/******************************************************************************/
#define PRINT_REC_TYPE(type)						\
    switch ( type ) {							\
      case BSR_XTNTS:			/* 1 */				\
        printf("             BSR_XTNTS");				\
        break;								\
      case BSR_ATTR:			/* 2 */				\
        printf("              BSR_ATTR");				\
        break;								\
      case BSR_VD_ATTR:			/* 3 */				\
        printf("           BSR_VD_ATTR");				\
        break;								\
      case BSR_DMN_ATTR:		/* 4 */				\
        printf("          BSR_DMN_ATTR");				\
        break;								\
      case BSR_XTRA_XTNTS:		/* 5 */				\
        printf("        BSR_XTRA_XTNTS");				\
        break;								\
      case BSR_SHADOW_XTNTS:		/* 6 */				\
        printf("      BSR_SHADOW_XTNTS");				\
        break;								\
      case BSR_MCELL_FREE_LIST:		/* 7 */				\
        printf("   BSR_MCELL_FREE_LIST");				\
        break;								\
      case BSR_BFS_ATTR:		/* 8 */				\
        printf("          BSR_BFS_ATTR");				\
        break;								\
      case BSR_VD_IO_PARAMS:		/* 9 */				\
        printf("      BSR_VD_IO_PARAMS");				\
        break;								\
      case BSR_TERTIARY_XTNTS:		/* 12 */			\
        printf("    BSR_TERTIARY_XTNTS");				\
        break;								\
      case BSR_TERTIARY_XTRA_XTNTS:	/* 13 */			\
        printf("BSR_TERTIARY_XTRA_XTNTS");				\
        break;								\
      case BSR_DEF_DEL_MCELL_LIST:	/* 14 */			\
        printf("BSR_DEF_DEL_MCELL_LIST");				\
        break;								\
      case BSR_DMN_MATTR:		/* 15 */			\
        printf("         BSR_DMN_MATTR");				\
        break;								\
      case BSR_BF_INHERIT_ATTR:		/* 16 */			\
        printf("   BSR_BF_INHERIT_ATTR");				\
        break;								\
      case BSR_SET_SHELVE_ATTR:		/* 17 */			\
        printf("   BSR_SET_SHELVE_ATTR");				\
        break;								\
      case BSR_BFS_QUOTA_ATTR:		/* 18 */			\
        printf("    BSR_BFS_QUOTA_ATTR");				\
        break;								\
      case BSR_PROPLIST_HEAD:		/* 19 */			\
        printf("     BSR_PROPLIST_HEAD");				\
        break;								\
      case BSR_PROPLIST_DATA:		/* 20 */			\
        printf("     BSR_PROPLIST_DATA");				\
        break;								\
      case BSR_DMN_TRANS_ATTR:		/* 21 */			\
        printf("    BSR_DMN_TRANS_ATTR");				\
        break;								\
      case BSR_DMN_SS_ATTR:             /* 22 */                        \
        printf("    BSR_DMN_SS_ATTR");                                  \
        break;                                                          \
      case BSR_DMN_FREEZE_ATTR:         /* 23 */                        \
        printf("    BSR_DMN_FREEZE_ATTR");                              \
        break;                                                          \
      case BMTR_FS_DIR_INDEX_FILE:      /* 249 */                       \
        printf("BMTR_FS_DIR_INDEX_FILE");                               \
        break;                                                          \
      case BMTR_FS_INDEX_FILE:          /* 250 */                       \
        printf("    BMTR_FS_INDEX_FILE");                               \
        break;                                                          \
      case BMTR_FS_TIME:		/* 251 */			\
        printf("          BMTR_FS_TIME");				\
        break;								\
      case BMTR_FS_UNDEL_DIR:		/* 252 */			\
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

static
print_record(bsMRT *precord, int rn, int flags, int dmnVersion, bfTagT tag)
{
    int cnt;
    char *pdata;

    printf("RECORD %-2d  bCnt %-3d  ", rn, precord->bCnt);
    if ( flags & VFLG ) {
        printf("version %d                  ", precord->version);
    } else {
        printf("                               ");
    }

    PRINT_REC_TYPE(precord->type);
    if ( flags & VFLG ) {
        printf(" (%d)\n", precord->type);
    } else {
        printf("\n");
    }

    pdata = ((char *) precord) + sizeof(bsMRT);

    switch (precord->type) {
      case BSR_XTNTS:			/* 1 */
        printBSR_XTNTS((bsXtntRT *)pdata, flags, dmnVersion, tag);
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
      case BSR_SHADOW_XTNTS:		/* 6 */
        printBSR_SHADOW_XTNTS((bsShadowXtntT *)pdata, flags & VFLG);
        break;
      case BSR_MCELL_FREE_LIST:		/* 7 */
        printBSR_MCELL_FREE_LIST((bsMcellFreeListT *)pdata, flags & VFLG);
        break;
      case BSR_BFS_ATTR:		/* 8 */
        printBSR_BFS_ATTR((bsBfSetAttrT *)pdata, flags & VFLG);
        break;
      case BSR_VD_IO_PARAMS:		/* 9 */
        printBSR_VD_IO_PARAMS((vdIoParamsT *)pdata, flags & VFLG);
        break;
      case BSR_TERTIARY_XTNTS:		/* 12 */
        printBSR_TERTIARY_XTNTS((bsTerXtntRT*)pdata, flags & VFLG);
        break;
      case BSR_TERTIARY_XTRA_XTNTS:	/* 13 */
        printBSR_TERTIARY_XTRA_XTNTS((bsTerXtraXtntRT*)pdata, flags & VFLG);
        break;
      case BSR_DEF_DEL_MCELL_LIST:	/* 14 */
        printBSR_DEF_DEL_MCELL_LIST((delLinkRT*)pdata, flags & VFLG);
        break;
      case BSR_DMN_MATTR:		/* 15 */
        printBSR_DMN_MATTR((bsDmnMAttrT *)pdata, flags & VFLG);
        break;
      case BSR_BF_INHERIT_ATTR:		/* 16 */
        printBSR_BF_INHERIT_ATTR((bsBfInheritAttrT *)pdata, flags & VFLG);
        break;
      case BSR_SET_SHELVE_ATTR:		/* 17 */
        printBSR_SET_SHELVE_ATTR((bfSetShelveAttrT *)pdata, flags & VFLG);
        break;
      case BSR_BFS_QUOTA_ATTR:		/* 18 */
        printBSR_BFS_QUOTA_ATTR((bsQuotaAttrT *)pdata, flags & VFLG);
        break;
      case BSR_PROPLIST_HEAD:		/* 19 */
        if ( dmnVersion >= FIRST_NUMBERED_PROPLIST_VERSION ) {
            printBSR_PROPLIST_HEAD((bsPropListHeadT *)pdata, flags & VFLG);
        } else {
            printBSR_PROPLIST_HEAD_V3((bsPropListHeadT_v3 *)pdata, flags&VFLG);
        }
        break;
      case BSR_PROPLIST_DATA:		/* 20 */
        printBSR_PROPLIST_DATA((bsPropListPageT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_TRANS_ATTR:          /* 21 */
        printBSR_DMN_TRANS_ATTR((bsDmnTAttrT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_SS_ATTR:             /* 22 */
        printBSR_DMN_SS_ATTR((bsSSDmnAttrT *)pdata, flags & VFLG);
        break;
      case BSR_DMN_FREEZE_ATTR:         /* 23 */
        printBSR_DMN_FREEZE_ATTR((bsDmnFreezeAttrT *)pdata, flags & VFLG);
        break;
      case BMTR_FS_DIR_INDEX_FILE:      /* 249 */
        printBMTR_FS_DIR_INDEX_FILE((bfTagT*)pdata, flags);
        break;
      case BMTR_FS_INDEX_FILE:          /* 250 */
        printBMTR_FS_INDEX_FILE((bsIdxBmtRecT*)pdata, flags);
        break;
      case BMTR_FS_TIME:		/* 251 */
        printBMTR_FS_TIME((time_t *)pdata, flags & VFLG);
        break;
      case BMTR_FS_UNDEL_DIR:		/* 252 */
        printBMTR_FS_UNDEL_DIR((struct undel_dir_rec *)pdata, flags & VFLG);
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
    printf("\n");
}

/******************************************************************************/
/* mcell type 1 - BSR_XTNTS */

#define PRINT_BSXA(y, i) 						\
    if ( pdata->y[i].vdBlk == XTNT_TERM )                               \
        printf("bsXA[%2d]   bsPage %6u  vdBlk        -1\n",             \
          i, pdata->y[i].bsPage);                                       \
    else if ( pdata->y[i].vdBlk == PERM_HOLE_START )                    \
        printf("bsXA[%2d]   bsPage %6u  vdBlk        -2\n",             \
          i, pdata->y[i].bsPage);                                       \
    else                                                                \
        printf("bsXA[%2d]   bsPage %6u  vdBlk %9u (0x%x)\n",            \
          i, pdata->y[i].bsPage, pdata->y[i].vdBlk, pdata->y[i].vdBlk)

#define PRINT_XTNT_TYPE(type)						\
    switch ( type ) {							\
      case BSXMT_APPEND:						\
        printf("BSXMT_APPEND");						\
        break;								\
      case BSXMT_STRIPE:						\
        printf("BSXMT_STRIPE");						\
        break;								\
      default:								\
        printf("*** unknown (%d) ***", type);				\
        break;								\
    }

static
printBSR_XTNTS(bsXtntRT *pdata, int flags, int dmnVersion, bfTagT tag)
{
    int i;
    int cnt;

    if ( flags & VFLG ) {
        printf("type ");
        PRINT_XTNT_TYPE(pdata->type);
        printf(" (%d)\n", pdata->type);
        printf("chain mcell volume page cell %d %d %d\n",
          pdata->chainVdIndex, pdata->chainMCId.page, pdata->chainMCId.cell);
    } else {
        printf("type ");
        PRINT_XTNT_TYPE(pdata->type);
        printf("  ");
        printf("chain mcell volume page cell %d %d %d\n",
          pdata->chainVdIndex, pdata->chainMCId.page, pdata->chainMCId.cell);
    }

    if ( flags & VFLG ) {
        printf("blksPerPage %d  ", pdata->blksPerPage);
        printf("segmentSize %d\n", pdata->segmentSize);
    }

    if ( flags & VFLG || flags & DDL ) {
        printf("delLink next page,cell %d,%d  prev page,cell %d,%d\n",
          pdata->delLink.nextMCId.page, pdata->delLink.nextMCId.cell,
          pdata->delLink.prevMCId.page, pdata->delLink.prevMCId.cell);
        printf("delRst volume,page,cell %d,%d,%d  ", pdata->delRst.vdIndex,
          pdata->delRst.mcid.page, pdata->delRst.mcid.cell);
        printf("xtntIndex %d  offset %d  blocks %u\n", pdata->delRst.vdIndex,
          pdata->delRst.offset, pdata->delRst.blocks);
    }

    if ( flags & VFLG ) {
        /*
         * Files which don't have extent information in the primary mcell
         * have the firstXtnt field of the bsXtntRT formatted differently.
         * For those files, show these bytes in the older format.
         */
         if ( !BS_BFTAG_RSVD(tag) &&
              !FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, pdata->type))
         {
            printf("xu.nwf.nwPgCnt %d\n", 
                   ((bfNWnFragT *)(&pdata->firstXtnt))->nwPgCnt);
            printf("xu.nwf.nwrPage %d\n",
                   ((bfNWnFragT *)(&pdata->firstXtnt))->nwrPage);
            for (i = 0; i < BSX_NW_BITS / 32; i++) {
                printf("xu.nwf.notWrit[%d] %08x\n",
                       i, 
                       ((bfNWnFragT *)(&pdata->firstXtnt))->notWrit[i]);
            }
        }
    }

    /*
     * If there is extent information in the primary mcell, print it out.
     */
     if ( BS_BFTAG_RSVD(tag) ||
          FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, pdata->type))
     {
        printf("firstXtnt mcellCnt %d  ", pdata->firstXtnt.mcellCnt);
        printf("xCnt %d\n", pdata->firstXtnt.xCnt);

        if ( !(flags & MISCF) ) {
            if ( pdata->firstXtnt.xCnt <= BMT_XTNTS ) {
                cnt = flags & VFLG ? BMT_XTNTS : pdata->firstXtnt.xCnt;
            } else {
                fprintf(stderr,
                  "**Bad firstXtnt.xCnt (%d). Valid range: 0 - %d.\n",
                  pdata->firstXtnt.xCnt, BMT_XTNTS);
                cnt = BMT_XTNTS;
            }
        } else {
            /* Special treatment for Misc with 3 primary extent entries. */
            /* V3 domains can have 2 or 3 entries. */
            if ( pdata->firstXtnt.xCnt != 3 ||
                 flags & V3DMN && pdata->firstXtnt.xCnt < 2 ||
                 flags & V3DMN && pdata->firstXtnt.xCnt > 3 )
            {
                fprintf(stderr, "**Bad firstXtnt.xCnt (%d). ",
                  pdata->firstXtnt.xCnt);
                fprintf(stderr,
                "Misc file primary extent record should have 3 entries.\n");
            }
            cnt = 3;
        }

        for (i = 0; i < cnt; i++) {
            PRINT_BSXA(firstXtnt.bsXA,i);
        }
    }
}

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

#define PRINT_DATASAFETY(datasafety)					\
    switch ( datasafety ) {						\
      case BFD_NIL:							\
        printf("BFD_NIL");						\
        break;								\
      case BFD_NO_NWR:							\
        printf("BFD_NO_NWR");						\
        break;								\
      case BFD_FTX_AGENT:						\
        printf("BFD_FTX_AGENT");					\
        break;								\
      case BFD_SYNC_WRITE:						\
        printf("BFD_SYNC_WRITE");					\
        break;								\
      default:								\
        printf("*** unknown (%d) ***", datasafety);			\
        break;								\
    }

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
        printf("bfPgSz %d  ", pdata->bfPgSz);
        printf("transitionId %d\n", pdata->transitionId);
        printf("cloneId %d  ", pdata->cloneId);
        printf("cloneCnt %d  ", pdata->cloneCnt);
        printf("maxClonePgs %d\n", pdata->maxClonePgs);
        printf("deleteWithClone %d  ", pdata->deleteWithClone);
        printf("outOfSyncClone %d\n", pdata->outOfSyncClone);
        printf("cl.dataSafety ");
        PRINT_DATASAFETY(pdata->cl.dataSafety);
        printf(" (%d)\n", pdata->cl.dataSafety);
        printf("cl reqServices %d  ", pdata->cl.reqServices);
        printf("optServices %d  ", pdata->cl.optServices);
        printf("extendSize %d  ", pdata->cl.extendSize);
    /*  printf("clientArea %d  ", pdata->cl.clientArea); */
        printf("rsvd1 %d\n", pdata->cl.rsvd1);
        printf("rsvd2 %d  ", pdata->cl.rsvd2);
        printf("acl %d  ", pdata->cl.acl);
        printf("rsvd_sec1 %d  ", pdata->cl.rsvd_sec1);
        printf("rsvd_sec2 %d  ", pdata->cl.rsvd_sec2);
        printf("rsvd_sec3 %d\n", pdata->cl.rsvd_sec3);
    }
}

/******************************************************************************/
/* mcell type 3 - BSR_VD_ATTR */

#define PRINT_VD_ATTR_STATE(_state)					\
    switch ( _state ) {							\
      case BSR_VD_VIRGIN:						\
        printf("state BSR_VD_VIRGIN (%d)\n", _state);			\
        break;								\
      case BSR_VD_MOUNTED:						\
        printf("state BSR_VD_MOUNTED (%d)\n", _state);			\
        break;								\
      case BSR_VD_DISMOUNTED:						\
        printf("state BSR_VD_DISMOUNTED (%d)\n", _state);		\
        break;								\
      case BSR_VD_DMNT_INPROG:						\
        printf("state BSR_VD_DMNT_INPROG (%d)\n", _state);		\
        break;								\
      case BSR_VD_ZOMBIE:						\
        printf("state BSR_VD_ZOMBIE (%d)\n", _state);			\
        break;								\
      default:								\
        printf("*** unknown (%d) ***\n", _state);			\
        break;								\
    }

printBSR_VD_ATTR(bsVdAttrT *pdata, int vflg)
{
    char time_buf[40];

    ctime_r(&pdata->vdMntId, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';

    if ( vflg ) {
        printf("vdMntId %08x.%08x   (%s)\n",
          pdata->vdMntId.tv_sec, pdata->vdMntId.tv_usec, time_buf);
        PRINT_VD_ATTR_STATE(pdata->state);
        printf("vdIndex %d\n", pdata->vdIndex);
        printf("jays_new_field %d\n", pdata->jays_new_field);
        printf("vdBlkCnt %u\n", pdata->vdBlkCnt);
        printf("stgCluster %d\n", pdata->stgCluster);
        printf("maxPgSz %d\n", pdata->maxPgSz);
        printf("bmtXtntPgs %d\n", pdata->bmtXtntPgs);
        printf("serviceClass %d\n", pdata->serviceClass);
    } else {
        printf("vdMntId %08x.%08x   (%s)\n",
          pdata->vdMntId.tv_sec, pdata->vdMntId.tv_usec, time_buf);
        printf("vdIndex %d  ", pdata->vdIndex);
        printf("vdBlkCnt %u\n", pdata->vdBlkCnt);
    }
}

/******************************************************************************/
/* mcell type 4 - BSR_DMN_ATTR */

printBSR_DMN_ATTR(bsDmnAttrT *pdata, int vflg)
{
    char time_buf[40];

    ctime_r(&pdata->bfDomainId, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';

    if ( vflg ) {
        printf("bfDomainId %08x.%08x   (%s)\n",
          pdata->bfDomainId.tv_sec, pdata->bfDomainId.tv_usec, time_buf);
        printf("maxVds %d\n", pdata->maxVds);
        printf("bfSetDirTag %d (%x.%x)\n",
          pdata->bfSetDirTag.num,
          pdata->bfSetDirTag.num, pdata->bfSetDirTag.seq);
    } else {
        printf("bfDomainId %08x.%08x   (%s)\n",
          pdata->bfDomainId.tv_sec, pdata->bfDomainId.tv_usec, time_buf);
    }
}

/******************************************************************************/
/* mcell type 5 - BSR_XTRA_XTNTS */

printBSR_XTRA_XTNTS(bsXtraXtntRT *pdata, int vflg)
{
    int i;
    int cnt;

    if ( vflg ) {
        printf("blksPerPage %d\n", pdata->blksPerPage);
        printf("xCnt %d\n", pdata->xCnt);

        if ( pdata->xCnt > BMT_XTRA_XTNTS ) {
            fprintf(stderr, "Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_XTRA_XTNTS);
        }

        for ( i = 0; i < BMT_XTRA_XTNTS; i++ ) {
            PRINT_BSXA(bsXA, i);
        }
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
/* mcell type 6 - BSR_SHADOW_XTNTS */

printBSR_SHADOW_XTNTS(bsShadowXtntT *pdata, int vflg)
{
    int i;
    int cnt;

    if ( vflg ) {
        if ( pdata->allocVdIndex == (vdIndexT)(-1) ) {
            printf("allocVdIndex -1\n");
        } else {
            printf("allocVdIndex %d\n", pdata->allocVdIndex);
        }
        printf("mcellCnt %d\n", pdata->mcellCnt);
        printf("blksPerPage %d\n", pdata->blksPerPage);
        printf("xCnt %d\n", pdata->xCnt);
        if ( pdata->xCnt > BMT_SHADOW_XTNTS ) {
            fprintf(stderr, "Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_SHADOW_XTNTS);
        }
        for ( i = 0; i < BMT_SHADOW_XTNTS; i++ ) {
            PRINT_BSXA(bsXA, i);
        }
    } else {
        if ( pdata->allocVdIndex == (vdIndexT)(-1) ) {
            printf("allocVdIndex -1  ");
        } else {
            printf("allocVdIndex %d  ", pdata->allocVdIndex);
        }
        printf("mcellCnt %d  ", pdata->mcellCnt);
        printf("xCnt %d\n", pdata->xCnt);
        if ( pdata->xCnt > BMT_SHADOW_XTNTS ) {
            fprintf(stderr, "Bad xCnt (%d). Valid range: 0 - %d.\n",
              pdata->xCnt, BMT_SHADOW_XTNTS);
            cnt = BMT_SHADOW_XTNTS;
        } else {
            cnt = pdata->xCnt;
        }
        for ( i = 0; i < cnt; i++ ) {
            PRINT_BSXA(bsXA, i);
        }
    }
}

/******************************************************************************/
/* mcell type 7 - BSR_MCELL_FREE_LIST */

printBSR_MCELL_FREE_LIST(bsMcellFreeListT *pdata, int vflg)
{
    printf("headPg %d\n", pdata->headPg);
}

/******************************************************************************/
/* mcell type 8 - BSR_BFS_ATTR */

#define PRINT_STATE(state)				\
    switch ( state ) {					\
      case BFS_INVALID:					\
        printf("BFS_INVALID");				\
        break;						\
      case BFS_READY:					\
        printf("BFS_READY");				\
        break;						\
      case BFS_CLONING:					\
        printf("BFS_CLONING");				\
        break;						\
      case BFS_BUSY:					\
        printf("BFS_BUSY");				\
        break;						\
      case BFS_DELETING:				\
        printf("BFS_DELETING");				\
        break;						\
      case BFS_DELETING_CLONE:				\
        printf("BFS_DELETING_CLONE");			\
        break;						\
      default:						\
        printf("*** unknown (%d) ***", state);		\
        break;						\
    }

#define PRINT_FLAGS(flags)				\
    if ( flags & BFS_OD_OUT_OF_SYNC )			\
        printf("BFS_OD_OUT_OF_SYNC ");                  \
    if ( flags & BFS_OD_NOFRAG )	                \
        printf("BFS_OD_NOFRAG ");                       \
    if ( flags & BFS_OD_OBJ_SAFETY )                    \
        printf("BFS_OD_OBJ_SAFETY ");


printBSR_BFS_ATTR(bsBfSetAttrT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    int prt_flg = 0;

    if ( vflg ) {
        ctime_r(&pdata->bfSetId.domainId, time_buf);
        time_buf[strlen(time_buf) - 1] = '\0';
        printf("bfSetId.domainId %x.%x  (%s)\n",
          pdata->bfSetId.domainId.tv_sec, pdata->bfSetId.domainId.tv_usec,
          time_buf);
        printf("bfSetId.dirTag %d (%x.%x)\n",
          pdata->bfSetId.dirTag.num, pdata->bfSetId.dirTag.num,
          pdata->bfSetId.dirTag.seq);
        printf("fragBfTag %d (%x.%x)\n",
          pdata->fragBfTag.num, pdata->fragBfTag.num, pdata->fragBfTag.seq);
        printf("nextCloneSetTag %d (%x.%x)   ",
          pdata->nextCloneSetTag.num, pdata->nextCloneSetTag.num,
          pdata->nextCloneSetTag.seq);
        printf("origSetTag %d (%x.%x)\n",
          pdata->origSetTag.num, pdata->origSetTag.num, pdata->origSetTag.seq);
        printf("nxtDelPendingBfSet %d (%x.%x)\n",
          pdata->nxtDelPendingBfSet.num, pdata->nxtDelPendingBfSet.num,
          pdata->nxtDelPendingBfSet.seq);
        printf("state ");
        PRINT_STATE(pdata->state);
        printf("   ");
        printf("flags 0x%x ", pdata->flags);
        PRINT_FLAGS(pdata->flags);
        printf("\n");
        printf("cloneId %d   ", pdata->cloneId);
        printf("cloneCnt %d   ", pdata->cloneCnt);
        printf("numClones %d\n", pdata->numClones);
        printf("fsDev 0x%x   ", pdata->fsDev);
        printf("freeFragGrps %d   ", pdata->freeFragGrps);
        printf("oldQuotaStatus %d\n", pdata->oldQuotaStatus);
        printf("uid %d   ", pdata->uid);
        printf("gid %d   ", pdata->gid);
        printf("mode %04o   ", pdata->mode);
        printf("setName \"%s\"\n", pdata->setName);
        printf("fsContext[0], fsContext[1] %x.%x  (rootTag)\n", 
          pdata->fsContext[0], pdata->fsContext[1]);
        printf("fsContext[2], fsContext[3] %x.%x  (tagsTag)\n", 
          pdata->fsContext[2], pdata->fsContext[3]);
        printf("fsContext[4], fsContext[5] %x.%x  (userQuotaTag)\n", 
          pdata->fsContext[4], pdata->fsContext[5]);
        printf("fsContext[6], fsContext[7] %x.%x  (groupQuotaTag)\n", 
          pdata->fsContext[6], pdata->fsContext[7]);
        for (i = 0; i < BFS_FRAG_MAX; i++) {
            printf("fragGrps[%d] firstFreeGrp %5d lastFreeGrp %5d\n",
                i, pdata->fragGrps[i].firstFreeGrp, 
                pdata->fragGrps[i].lastFreeGrp);
        }
    } else {
        printf("setName \"%s\"   ", pdata->setName);
        printf("state ");
        PRINT_STATE(pdata->state);
        printf("   ");
        printf("bfSetId.dirTag %d\n", pdata->bfSetId.dirTag.num);
        if ( pdata->nextCloneSetTag.num != 0 ) {
            printf("nextCloneSetTag %d   ", pdata->nextCloneSetTag.num);
            prt_flg = 1;
        }
        if ( pdata->origSetTag.num != 0 ) {
            printf("origSetTag %d   ", pdata->origSetTag.num);
            prt_flg = 1;
        }
        if ( pdata->nxtDelPendingBfSet.num != 0 ) {
            printf("nxtDelPendingBfSet %d   ", pdata->nxtDelPendingBfSet.num);
            prt_flg = 1;
        }
        if ( prt_flg ) {
            printf("\n");
        }
    }
}

/******************************************************************************/
/* mcell type 9 - BSR_VD_IO_PARAMS */

printBSR_VD_IO_PARAMS(vdIoParamsT *pdata, int vflg)
{
    vflg = 1;		/* tmp */

    if ( vflg ) {
        printf("rdMaxIo %d\n", pdata->rdMaxIo);
        printf("wrMaxIo %d\n", pdata->wrMaxIo);
        printf("qtoDev %d\n", pdata->qtoDev);
        printf("consolidate %d\n", pdata->consolidate);
        printf("blockingFact %d\n", pdata->blockingFact);
        printf("lazyThresh %d\n", pdata->lazyThresh);
    } else {
        printf("rdMaxIo %d  wrMaxIo %d  qtoDev %d\n",
          pdata->rdMaxIo, pdata->wrMaxIo, pdata->qtoDev);
    }
}

/******************************************************************************/
/* mcell type 12 - BSR_TERTIARY_XTNTS */

printBSR_TERTIARY_XTNTS(bsTerXtntRT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    uint cnt;

    if ( vflg ) {
        printf("totalXtnts %d\n", pdata->totalXtnts);
        printf("xCnt %d\n", pdata->xCnt);
        for (i = 0; i < BMT_TERTIARY_XTNTS; i++) {
            printf("bsXA[%2d]  bsPage %5d  mediaId %d  flags 0x%x\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId,
              pdata->bsXA[i].flags);
            ctime_r(&pdata->bsXA[i].accessTime, time_buf);
            time_buf[strlen(time_buf) - 1] = '\0';
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
            ctime_r(&pdata->bsXA[i].accessTime, time_buf);
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("bsXA[%2d]  bsPage %5d  mediaId %d  accessTime %s\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId, time_buf);
        }
    }
}

/******************************************************************************/
/* mcell type 13 - BSR_TERTIARY_XTRA_XTNTS */

printBSR_TERTIARY_XTRA_XTNTS(bsTerXtraXtntRT *pdata, int vflg)
{
    int i;
    char time_buf[40];
    uint cnt;

    if ( vflg ) {
        printf("xCnt %d\n", pdata->xCnt);
        for (i = 0; i < BMT_TERTIARY_XTRA_XTNTS; i++) {
            printf("bsXA[%2d]  bsPage %5d  mediaId %d  flags 0x%x\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId,
              pdata->bsXA[i].flags);
            ctime_r(&pdata->bsXA[i].accessTime, time_buf);
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("           accessTime %s  mediaRefNum high,low %d,%d\n",
              time_buf, pdata->bsXA[i].mediaRefNum.high,
              pdata->bsXA[i].mediaRefNum.low);
        }
    } else {
        printf("xCnt %d\n", pdata->xCnt);
        cnt = pdata->xCnt <= BMT_TERTIARY_XTRA_XTNTS ? pdata->xCnt : BMT_TERTIARY_XTRA_XTNTS;
        for (i = 0; i < cnt; i++) {
            ctime_r(&pdata->bsXA[i].accessTime, time_buf);
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("bsXA[%2d]  bsPage %5d  mediaId %d  accessTime %s\n",
              i, pdata->bsXA[i].bsPage, pdata->bsXA[i].mediaId, time_buf);
        }
    }
}

/******************************************************************************/
/* mcell type 14 - BSR_DEF_DEL_MCELL_LIST */

printBSR_DEF_DEL_MCELL_LIST(delLinkRT *pdata, int vflg)
{
    printf("nextMCId page,cell %d,%d  prevMCId page,cell %d,%d\n",
      pdata->nextMCId.page, pdata->nextMCId.cell,
      pdata->prevMCId.page, pdata->prevMCId.cell);
    if ( pdata->prevMCId.page != 0 || pdata->prevMCId.cell != 0 ) {
        fprintf(stderr, "** Bad prevMCId. Should be 0.\n");
    }
}

/******************************************************************************/
/* mcell type 15 - BSR_DMN_MATTR */

printBSR_DMN_MATTR(bsDmnMAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("seqNum %d\n", pdata->seqNum);
        printf("delPendingBfSet %d (%x.%x)\n", pdata->delPendingBfSet.num,
          pdata->delPendingBfSet.num, pdata->delPendingBfSet.seq);
        printf("uid %d\n", pdata->uid);
        printf("gid %d\n", pdata->gid);
        printf("mode %04o\n", pdata->mode);
        printf("vdCnt %d\n", pdata->vdCnt);
        printf("recoveryFailed %d\n", pdata->recoveryFailed);
        printf("bfSetDirTag %d\n", pdata->bfSetDirTag);
        printf("ftxLogTag %d\n", pdata->ftxLogTag);
        printf("ftxLogPgs %d\n", pdata->ftxLogPgs);
    } else {
        printf("uid %d  ", pdata->uid);
        printf("gid %d  ", pdata->gid);
        printf("mode %04o\n", pdata->mode);
        printf("vdCnt %d\n", pdata->vdCnt);
        if ( pdata->recoveryFailed ) {
            printf("recoveryFailed %d\n", pdata->recoveryFailed);
        }
        if ( pdata->delPendingBfSet.num ) {
            printf("delPendingBfSet %d\n", pdata->delPendingBfSet.num);
        }
    }
}

/******************************************************************************/
/* mcell type 16 - BSR_BF_INHERIT_ATTR */

printBSR_BF_INHERIT_ATTR(bsBfInheritAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("dataSafety ");
        PRINT_DATASAFETY(pdata->dataSafety);
        printf("\n");
        printf("reqServices %d\n", pdata->reqServices);
        printf("optServices %d\n", pdata->optServices);
        printf("extendSize %d\n", pdata->extendSize);
        printf("clientArea %d %d %d %d\n", pdata->clientArea[0],
          pdata->clientArea[1], pdata->clientArea[2], pdata->clientArea[3]);
        printf("rsvd1 %d\n", pdata->rsvd1);
        printf("rsvd2 %d\n", pdata->rsvd2);
        printf("rsvd_sec1 %d\n", pdata->rsvd_sec1);
        printf("rsvd_sec2 %d\n", pdata->rsvd_sec2);
        printf("rsvd_sec3 %d\n", pdata->rsvd_sec3);
    } else {
        printf("dataSafety ");
        PRINT_DATASAFETY(pdata->dataSafety);
        printf("  ");
        printf("reqServices %d  ", pdata->reqServices);
        printf("extendSize %d\n", pdata->extendSize);
    }
}

/******************************************************************************/
/* mcell type 17 - BSR_SET_SHELVE_ATTR */

#define PRINT_SHELVE_FLAGS(flags)				\
    if ( flags & MSS_REBUILDING )				\
        printf("MSS_REBUILDING ");				\
    if ( flags & MSS_UPD_MEDIA )				\
        printf("MSS_UPD_MEDIA ");				\
    if ( flags & MSS_NO_SHELVE )				\
        printf("MSS_NO_SHELVE ");				\
    if ( flags & MSS_USER_BLOCKED )				\
        printf("MSS_USER_BLOCKED ");				\
    if ( flags & MSS_ROOT_BLOCKED )				\
        printf("MSS_ROOT_BLOCKED ");


printBSR_SET_SHELVE_ATTR(bfSetShelveAttrT *pdata, int vflg)
{
    if ( vflg )
    {
        printf("flags ");
        PRINT_SHELVE_FLAGS(pdata->flags);
        printf("(0x%x)\n", pdata->flags);
        printf("smallFile %d\n", pdata->smallFile);
        printf("readAhead %d\n", pdata->readAhead);
        printf("readAheadIncr %d\n", pdata->readAheadIncr);
        printf("readAheadMax %d\n", pdata->readAheadMax);
        printf("autoShelveThresh %d\n", pdata->autoShelveThresh);
        printf("userId %d\n", pdata->userId);
        printf("shelf %d\n", pdata->shelf);
    } else {
        printf("flags ");
        PRINT_SHELVE_FLAGS(pdata->flags);
        if ( pdata->flags == MSS_NO_SHELVE ) {
            printf("\n");
        } else {
            printf("  ");
            printf("smallFile %d  ", pdata->smallFile);
            printf("readAhead %d  ", pdata->readAhead);
            printf("readAheadIncr %d\n", pdata->readAheadIncr);
            printf("readAheadMax %d  ", pdata->readAheadMax);
            printf("autoShelveThresh %d  ", pdata->autoShelveThresh);
            printf("userId %d  ", pdata->userId);
            printf("shelf %d\n", pdata->shelf);
        }
    }
}

/******************************************************************************/
/* mcell type 18 - BSR_BFS_QUOTA_ATTR */

typedef union {
    struct {
        uint64T __long;
    } _long;
    struct {
        uint32T lo_int;
        uint32T hi_int;
    } _ints;
} long_n_2intsT;

printBSR_BFS_QUOTA_ATTR(bsQuotaAttrT *pdata, int vflg)
{
    long_n_2intsT bhlim;
    long_n_2intsT bslim;
    long_n_2intsT fhlim;
    long_n_2intsT fslim;

    bhlim._ints.lo_int = pdata->blkHLimitLo;
    bhlim._ints.hi_int = pdata->blkHLimitHi;
    bslim._ints.lo_int = pdata->blkSLimitLo;
    bslim._ints.hi_int = pdata->blkSLimitHi;
    fhlim._ints.lo_int = pdata->fileHLimitLo;
    fhlim._ints.hi_int = pdata->fileHLimitHi;
    fslim._ints.lo_int = pdata->fileSLimitLo;
    fslim._ints.hi_int = pdata->fileSLimitHi;

    if ( vflg ) {
        printf("blkHLimitHi,blkHLimitLo %x,%x (%d)\n",
          pdata->blkHLimitHi, pdata->blkHLimitLo, bhlim._long.__long);
        printf("blkSLimitHi,blkSLimitLo %x,%x (%d)\n",
          pdata->blkSLimitHi, pdata->blkSLimitLo, bslim._long.__long);
        printf("fileHLimitHi,fileHLimitLo %x,%x (%d)\n",
          pdata->fileHLimitHi, pdata->fileHLimitLo, fhlim._long.__long);
        printf("fileSLimitHi,fileSLimitLo %x,%x (%d)\n",
          pdata->fileSLimitHi, pdata->fileSLimitLo, fslim._long.__long);
        printf("blkTLimit %d,  fileTLimit %d,  quotaStatus %d\n",
          pdata->blkTLimit, pdata->fileTLimit, pdata->quotaStatus);
        printf("unused1 %d, unused2 %d, unused3 %d, unused4 %d\n",
          pdata->unused1, pdata->unused2, pdata->unused3, pdata->unused4);
    } else {
        printf("Blocks hard limit %d,  soft limit %d,  blkTLimit %d\n",
          bhlim._long.__long, bslim._long.__long, pdata->blkTLimit);
        printf("Files hard limit %d,  soft limit %d,  fileTLimit %d\n",
          fhlim._long.__long, fslim._long.__long, pdata->fileTLimit);
    }
}

/******************************************************************************/
/* mcell type 19 - BSR_PROPLIST_HEAD */

#define PRINT_PLFLAGS(pl_flgs)                             \
{                                                          \
    ulong flgs = pl_flgs;                                  \
    if ( (flgs & BSR_PL_RESERVED) == BSR_PL_RESERVED ) {   \
        printf("BSR_PL_RESERVED");                         \
        flgs &= ~BSR_PL_RESERVED;                          \
    }                                                      \
    if ( flgs & BSR_PL_LARGE ) {                           \
        printf("BSR_PL_LARGE");                            \
        flgs &= ~BSR_PL_LARGE;                             \
        if ( flgs ) {                                      \
            printf(" + ");                                 \
        }                                                  \
    }                                                      \
    if ( flgs & BSR_PL_PAGE ) {                            \
        printf("BSR_PL_PAGE");                             \
        flgs &= ~BSR_PL_PAGE;                              \
        if ( flgs ) {                                      \
            printf(" + ");                                 \
        }                                                  \
    }                                                      \
    if ( flgs & BSR_PL_DELETED ) {                         \
        printf("BSR_PL_DELETED");                          \
        flgs &= ~BSR_PL_DELETED;                           \
        if ( flgs ) {                                      \
            printf(" + ");                                 \
        }                                                  \
    }                                                      \
    if ( flgs ) {                                          \
        printf("*** unknown flag 0x%lx ****", flgs);       \
    }                                                      \
}

uint64T MsfsPlFlags;
static uint32T *MsfsPlFlags_l = &((uint32T *)&MsfsPlFlags)[0];
static uint32T *MsfsPlFlags_h = &((uint32T *)&MsfsPlFlags)[1];
#define FLAGS_READ(x1)                                \
  (                                                   \
   *MsfsPlFlags_l = ((uint32T *) &x1)[0],             \
   *MsfsPlFlags_h = ((uint32T *) &x1)[1],             \
   MsfsPlFlags                                        \
   )

printBSR_PROPLIST_HEAD(bsPropListHeadT *pdata, int vflg)
{
    bsMRT *mrecp = (bsMRT *)(((char *)pdata) - sizeof(bsMRT));
    uint propListSz = mrecp->bCnt - sizeof(bsMRT) - BSR_PROPLIST_HEAD_SIZE;

    printf("flags %16lx ", FLAGS_READ(pdata->flags));
    PRINT_PLFLAGS(MsfsPlFlags);
    printf("\n");
    printf("pl_num %d ", pdata->pl_num);
    printf("namelen %d ", pdata->namelen);
    printf("valuelen %d\n", pdata->valuelen);

    if ( pdata->namelen <= propListSz &&
         pdata->buffer[pdata->namelen - 1] == '\0' )
    {
        printf("propery list name is \"%s\"\n", pdata->buffer);
    }

    if ( vflg & VFLG ) {
        print_unknown(pdata->buffer, propListSz);
    }
}

printBSR_PROPLIST_HEAD_V3(bsPropListHeadT_v3 *pdata, int vflg)
{
    bsMRT *mrecp = (bsMRT *)(((char *)pdata) - sizeof(bsMRT));
    uint propListSz = mrecp->bCnt - sizeof(bsMRT) - BSR_PROPLIST_HEAD_SIZE_V3;

    printf("flags %16lx ", FLAGS_READ(pdata->flags));
    PRINT_PLFLAGS(MsfsPlFlags);
    printf("\n");
    printf("namelen %d ", pdata->namelen);
    printf("valuelen %d\n", pdata->valuelen);
    if ( pdata->namelen <= propListSz &&
         pdata->buffer[pdata->namelen - 1] == '\0' )
    {
        printf("propery list name is \"%s\"\n", pdata->buffer);
    }

    if ( vflg & VFLG ) {
        print_unknown(pdata->buffer, propListSz);
    }
}

/******************************************************************************/
/* mcell type 20 - BSR_PROPLIST_DATA */

printBSR_PROPLIST_DATA(bsPropListPageT *pdata, int vflg)
{
    print_unknown(pdata->buffer, BSR_PROPLIST_DATA_SIZE);
}


/******************************************************************************/
/* mcell type 21 - BSR_DMN_TRANS_ATTR */

printBSR_DMN_TRANS_ATTR(bsDmnTAttrT *pdata, int vflg)
{
    if ( vflg ) {
        printf("chainVdIndex %d\n", pdata->chainVdIndex);
        printf("chainMCId page,cell %d,%d\n", pdata->chainMCId.page,
            pdata->chainMCId.cell);
        printf("op %d\n", pdata->op);
        printf("dev 0x%x\n", pdata->dev);
    } else {
        printf("\n");
    }
}

/******************************************************************************/
/* mcell type 22 - BSR_DMN_SS_ATTR */

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

        printf("ssDmnDefragment           = %d\n", pdata->ssDmnDefragment);
        printf("ssDmnSmartPlace           = %d\n", pdata->ssDmnSmartPlace);
        printf("ssDmnBalance              = %d\n", pdata->ssDmnBalance);
        printf("ssDmnVerbosity            = %d\n", pdata->ssDmnVerbosity);
        printf("ssDmnDirectIo             = %d\n", pdata->ssDmnDirectIo);
        printf("ssMaxPercentOfIoWhenBusy  = %d\n", pdata->ssMaxPercentOfIoWhenBusy);
        printf("ssAccessThreshHits        = %d\n", pdata->ssAccessThreshHits);
        printf("ssSteadyState             = %d\n", pdata->ssSteadyState);
        printf("ssMinutesInHotList        = %d\n", pdata->ssMinutesInHotList);
        printf("ssFilesDefraged           = %d\n", pdata->ssFilesDefraged);
        printf("ssPagesDefraged           = %d\n", pdata->ssPagesDefraged);
        printf("ssPagesBalanced           = %d\n", pdata->ssPagesBalanced);
        printf("ssFilesIOBal              = %d\n", pdata->ssFilesIOBal);
        printf("ssExtentsConsol           = %d\n", pdata->ssExtentsConsol);
        printf("ssPagesConsol             = %d\n", pdata->ssPagesConsol);

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

/******************************************************************************/
/* mcell type 23 - BSR_DMN_FREEZE_ATTR */

printBSR_DMN_FREEZE_ATTR(bsDmnFreezeAttrT *pdata, int vflg)
{

    char time_buf[40];

    if (0 == pdata->freezeBegin) {
	printf("freezeBegin %x\n", 
	       pdata->freezeBegin);
    } else {
	ctime_r(&pdata->freezeBegin, time_buf);
	time_buf[strlen(time_buf) - 1] = '\0';
	printf("freezeBegin %s\n", time_buf);
    }
    if (0 == pdata->freezeEnd) {
	printf("freezeEnd %x\n", 
	       pdata->freezeEnd);
    } else {
	ctime_r(&pdata->freezeEnd, time_buf);
	time_buf[strlen(time_buf) - 1] = '\0';
	printf("freezeEnd   %s\n", time_buf);
    }

    printf("freezeCount %d\n", pdata->freezeCount);

    if ( vflg ) {
        printf("unused1 %d  unused2 %d  unused3 %d  unused4 %d\n",
          pdata->unused1, pdata->unused2, pdata->unused3, pdata->unused4);
    }
}

/******************************************************************************/
/* mcell type 249 - BMTR_FS_DIR_INDEX_FILE */

printBMTR_FS_DIR_INDEX_FILE(bfTagT *bfTagp, int flags)
{
    printf("tag %d (%x.%x)\n", bfTagp->num, bfTagp->num, bfTagp->seq);
}

/******************************************************************************/
/* mcell type 250 - BMTR_FS_INDEX_FILE */

printBMTR_FS_INDEX_FILE(bsIdxBmtRecT *bsIdxBmtRecp, int flags)
{
    printf("fname_page %d  ", bsIdxBmtRecp->fname_page);
    printf("ffree_page %d  ", bsIdxBmtRecp->ffree_page);
    printf("fname_levels %d  ", bsIdxBmtRecp->fname_levels);
    printf("ffree_levels %d\n", bsIdxBmtRecp->ffree_levels);
}

/******************************************************************************/
/* mcell type 251 - BMTR_FS_TIME */

printBMTR_FS_TIME(time_t *fs_time, int vflg)
{
    char time_buf[40];

    ctime_r(fs_time, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';
    printf("last sync %s\n", time_buf);
}

/******************************************************************************/
/* mcell type 252 - BMTR_FS_UNDEL_DIR */

printBMTR_FS_UNDEL_DIR(struct undel_dir_rec *undel_dir, int vflg)
{
    printf("dir_tag %d (%x.%x)\n",
      undel_dir->dir_tag.num, undel_dir->dir_tag.num, undel_dir->dir_tag.seq);
}

/******************************************************************************/
/* mcell type 254 - BMTR_FS_DATA */

printBMTR_FS_DATA(char *pdata, int vflg)
{
    printf("%s\n", pdata);
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

#define PRINT_FRAG_TYPE(frag_type)			\
    switch (frag_type) {				\
      case BF_FRAG_ANY:					\
        printf("BF_FRAG_ANY");				\
        break;						\
      case BF_FRAG_1K:					\
        printf("BF_FRAG_1K");				\
        break;						\
      case BF_FRAG_2K:					\
        printf("BF_FRAG_2K");				\
        break;						\
      case BF_FRAG_3K:					\
        printf("BF_FRAG_3K");				\
        break;						\
      case BF_FRAG_4K:					\
        printf("BF_FRAG_4K");				\
        break;						\
      case BF_FRAG_5K:					\
        printf("BF_FRAG_5K");				\
        break;						\
      case BF_FRAG_6K:					\
        printf("BF_FRAG_6K");				\
        break;						\
      case BF_FRAG_7K:					\
        printf("BF_FRAG_7K");				\
        break;						\
      case BF_FRAG_MAX:					\
        printf("BF_FRAG_MAX");				\
        break;						\
      case BF_FRAG_ALL:					\
        printf("BF_FRAG_ALL");				\
        break;						\
      default:						\
        printf("*** unknown (%d) ***", frag_type);	\
        break;						\
    }

static
printBMTR_FS_STAT(statT *pdata, int vflg)
{
    unsigned long x;
    unsigned int x2;
    char *s, *d;
    char time_buf[40];

    if ( vflg ) {
        /* FIX. obviously the following line is redundant. */
        if ( vflg ) {
            printf("st_ino %d   ", pdata->st_ino.num);	/* seq */
        } else {
            printf("dir_tag %d  ", pdata->dir_tag.num);
        }

        printf("st_mode %04o ", pdata->st_mode);
        PRINT_MODE(pdata->st_mode);
        memcpy((char *)&x2, (char *)&pdata->st_nlink, sizeof(unsigned int));
        printf("   st_nlink %u   ", x2);
        memcpy((char *)&x, (char *)&pdata->st_size, sizeof(unsigned long));
        printf("st_size %lu\n", x); 
        printf("st_uid %d   ", pdata->st_uid);
        printf("st_gid %d   ", pdata->st_gid);
        if ( vflg ||
            (pdata->st_mode & S_IFMT) == S_IFBLK ||
            (pdata->st_mode & S_IFMT) == S_IFCHR ) {

            printf("st_rdev %d major %d minor %d\n", pdata->st_rdev,
              major(pdata->st_rdev), minor(pdata->st_rdev));
        }

        ctime_r(&pdata->st_mtime, time_buf);
        time_buf[strlen(time_buf) - 1] = '\0';
        printf("st_mtime %s   ", time_buf);
        if ( vflg )
            printf("st_umtime %d\n", pdata->st_umtime);
        else
            printf("\n");

        if ( vflg ) {
            ctime_r(&pdata->st_atime, time_buf);
            /*FIX. test err */
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("st_atime %s   ", time_buf);
            printf("st_uatime %d\n", pdata->st_uatime);

            ctime_r(&pdata->st_ctime, time_buf);
            time_buf[strlen(time_buf) - 1] = '\0';
            printf("st_ctime %s   ", time_buf);
            printf("st_uctime %d\n", pdata->st_uctime);
        }

        if ( vflg ) {
            printf("fragId.frag %d   ", pdata->fragId.frag);
            printf("fragId.type %d ", pdata->fragId.type);
            PRINT_FRAG_TYPE(pdata->fragId.type);
            printf("   fragPageOffset %d\n", pdata->fragPageOffset);
        } else if ( pdata->fragId.type ) {
            printf("fragId.type ");
            PRINT_FRAG_TYPE(pdata->fragId.type);
            printf("  fragId.frag %d  ", pdata->fragId.frag);
        }

        if ( vflg ) {
            printf("dir_tag %d (%x.%x)  ",
              pdata->dir_tag.num, pdata->dir_tag.num, pdata->dir_tag.seq);
            printf("st_flags %d   ", pdata->st_flags);
            printf("st_unused_2 %d\n", pdata->st_unused_2);
        }
    } else {
        /* mode uid gid length (dev for special) */
        /* links parent_dir mtime */
        /* frag info */
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
            memcpy((char *)&x, (char *)&pdata->st_size, sizeof(unsigned long));
            printf("st_size %lu\n", x); 
        }

        memcpy((char *)&x2, (char *)&pdata->st_nlink, sizeof(unsigned int));
        printf("st_nlink %u   ", x2);

        printf("dir_tag %d  ", pdata->dir_tag.num);

        ctime_r(&pdata->st_mtime, time_buf);
        time_buf[strlen(time_buf) - 1] = '\0';
        printf("st_mtime %s\n", time_buf);

        if ( pdata->fragId.type ) {
            printf("fragId.type ");
            PRINT_FRAG_TYPE(pdata->fragId.type);
            printf("  fragId.frag %d\n", pdata->fragId.frag);
        }
    }
}
