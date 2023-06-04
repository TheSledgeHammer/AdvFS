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
 *      View On-Disk Structure
 *
 * Date:
 *
 *	Fri May  9 11:47:23 PDT 1997
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: print_log.c,v $ $Revision: 1.1.9.2 $ (DEC) $Date: 2006/08/30 15:25:45 $"

#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/mode.h>
#include <ufs/fs.h>
#include <assert.h>

#define ADVFS_SHELVE                    /* for mss_entry.h & bs_ods.h */
#include <msfs/ms_public.h>
typedef struct bfAccess bfAccessT;      /* for mss_entry.h */
#include <msfs/ms_privates.h>
#include <msfs/ftx_privates.h>

#include "vods.h"
#include "vlogpg.h"

/* private prototypes */

static void print_logrec_hdr( logRecHdrT*, int, int);
static void print_logrec_data( ftxDoneLRT*, uint32T, int);
static void print_contOrUndo( ftxAgentIdT, char*, int);
static void print_rootdone( ftxAgentIdT, char*, int);
static void print_redo( ftxAgentIdT, char*, int);
static void print_imageRedo( char**, int);
static char* tag_to_str( bfTagT, char*);
static char* setid_to_str( bfSetIdT, char*);
static void print_unlinkCloneUndoRec( char*, int);
static void print_delPendUndoRec( char*, int);
static void print_bmtrPutRecUndo( char*, int);
static void print_allocMcellUndoRec( char*, int);
static void print_allocLinkMcellUndoRec( char*, int);
static void print_linkUnlinkMcellsUndoRec( char*, int);
static void print_mcellUId( char*, int);
static void print_delOpRecT( char*, int);
static void print_defDelOpRec( char*, int);
static void print_mcellPtrRec( char*, int);
static void print_mcellCntUndoRec( char*, int);
static void print_updXtntRecUndoRec( char*, int);
static void print_insertRemoveXtntChainUndoRec( char*, int);
static void print_zeroUndoRec( char*, int);
static void print_bitmapUndoRec( char*, int);
static void print_bitfileTypeUndoRec( char*, int);
static void print_addStgUndoRec( char*, int);
static void print_xtntRecUndoRec( char*, int);
static void print_tagDataRec( char*, int);
static void print_tagUnInitPageRedo( char*, int);
static void print_insert_undo_rec( char*, int);
static void print_quotaRecObsolete( char*, int);
static void print_updatePtrUndoRec( char*, int);
static void print_appendUndoRec( char*, int);
static void print_updateTotalUndoRec( char*, int);
static void print_flagsUndoRec( char*, int);
static void print_setAccessUndoRec( char*, int);
static void print_vdIndex( char*, int);
static void print_delListRec( char*, int);
static void print_bfTag( char*, int);
static void print_quotaRec( char*, int);
static void print_delPropRootDone( char*, int);
static void print_idxUndoRecord( char*, int);

char *ftx_agents[] = {            /* NU = not used */ /* registered routines */
    "FTA_NULL",                          /* 0 */
    "FTA_BS_BFS_CREATE_V1",              /* 1 NU */   /* undo */
    "FTA_BS_BFS_UNLINK_CLONE_V1",        /* 2 */      /* undo */
    "FTA_BS_BFS_DEL_LIST_REMOVE_V1",     /* 3 */      /* undo */
    "FTA_BS_BFS_CLONE_V1",               /* 4 */
    "FTA_BS_BMT_PUT_REC_V1",             /* 5 */      /* undo */
    "FTA_BS_BMT_FREE_MCELL_V1",          /* 6 */
    "FTA_BS_BMT_ALLOC_MCELL_V1",         /* 7 */      /* undo */
    "FTA_BS_BMT_ALLOC_LINK_MCELL_V1",    /* 8 */      /* undo */
    "FTA_BS_BMT_LINK_MCELLS_V1",         /* 9 */      /* undo */
    "FTA_BS_BMT_UNLINK_MCELLS_V1",       /* 10 */     /* undo */
    "FTA_BS_BF_CREATE_V1",               /* 11 */     /* undo, rtdn */
    "FTA_BS_CRE_MCELL_V1",               /* 12 */     /* undo */
    "FTA_BS_DEL_DELETE_V1",              /* 13 */     /* undo, rtdn */
    "FTA_BS_DEL_DEFERRED_V1",            /* 14 */     /* undo */
    "FTA_BS_BMT_FREE_BF_MCELLS_V1",      /* 15 */     /* cont, rtdn */
    "FTA_BS_XTNT_UPD_MCELL_CNT_V1",      /* 16 */     /* undo */
    "FTA_BS_XTNT_UPD_REC_V1",            /* 17 */
    "FTA_BS_XTNT_CRE_REC",               /* 18 */
    "FTA_BS_XTNT_CRE_XTRA_REC_V1",       /* 19 */
    "FTA_BS_XTNT_CRE_SHADOW_REC_V1",     /* 20 */
    "FTA_BS_XTNT_INSERT_CHAIN_V1",       /* 21 */     /* undo */
    "FTA_BS_XTNT_REMOVE_CHAIN_V1",       /* 22 */     /* undo */
    "FTA_BS_XTNT_ZERO_MAP_V1_UNUSED",    /* 23 */
    "FTA_BS_MIG_MIGRATE_V1",             /* 24 */
    "FTA_BS_MIG_ALLOC_V1",               /* 25 NU */
    "FTA_BS_MIG_CNT_V1",                 /* 26 NU */
    "FTA_BS_MIG_INSERT_HEAD_V1",         /* 27 NU */
    "FTA_BS_MIG_REMOVE_HEAD_V1",         /* 28 NU */
    "FTA_BS_MIG_REMOVE_MIDDLE_V1",       /* 29 NU */
    "FTA_BS_MIG_ALLOC_LINK_V1",          /* 30 NU */
    "FTA_BS_SHA_SHADOW_V1",              /* 31 NU */
    "FTA_BS_SHA_BITFILE_TYPE_V1",        /* 32 NU */
    "FTA_BS_SBM_ALLOC_BITS_V1",          /* 33 */     /* undo, rtdn */
    "FTA_BS_STG_ADD_V1",                 /* 34 */     /* undo */
    "FTA_BS_STG_REMOVE_V1",              /* 35 */
    "FTA_BS_SBM_DEALLOC_BITS_V1",        /* 36 NU */  /* undo, rtdn */
    "FTA_BS_STG_COPY_V1",                /* 37 */
    "FTA_BS_STR_STRIPE_V1",              /* 38 */
    "FTA_BS_STR_UPD_XTNT_REC_V1",        /* 39 */     /* undo */
    "FTA_BS_TAG_WRITE_V1",               /* 40 */     /* undo */
    "FTA_BS_TAG_EXTEND_TAGDIR_V1",       /* 41 */     /* redo */
    "FTA_BS_TAG_PATCH_FREELIST_V1",      /* 42 */
    "FTA_FS_REMOVE_V1",                  /* 43 NU */
    "FTA_FS_UPDATE_V1",                  /* 44 NU */
    "FTA_FS_CREATE_V1 ",                 /* 45 NU */
    "FTA_FS_INSERT_V1",                  /* 46 */     /* undo */
    "FTA_FS_LINK_V1",                    /* 47 NU */
    "FTA_FS_RENAME_V1",                  /* 48 NU */
    "FTA_FS_DQUOT_SYNC_V1",              /* 49 NU */
    "FTA_TER_CRE_TER_V1",                /* 50 */
    "FTA_TER_CRE_XTRA_V1",               /* 51 */
    "FTA_TER_UPDATE_PTR_V1",             /* 52 */     /* undo */
    "FTA_TER_APPEND_TO_TER_V1",          /* 53 */     /* undo */
    "FTA_TER_APPEND_TO_XTRA_V1",         /* 54 */     /* undo */
    "FTA_TER_UPDATE_TOTAL_V1",           /* 55 */     /* undo */
    "FTA_TER_FLAGS_TER_V1",              /* 56 */     /* undo */
    "FTA_TER_FLAGS_XTRA_V1",             /* 57 */     /* undo */
    "FTA_TER_SET_MEDIA_V1",              /* 58 */
    "FTA_TER_SET_CLEAN_V1",              /* 59 */
    "FTA_TER_ADD_STG_V1",                /* 60 NU */
    "FTA_TER_ZERO_XTNT_MAP_V1",          /* 61 NU */
    "FTA_TER_TRUNC_MAP_V1",              /* 62 */
    "FTA_TER_SET_ACCESS_V1",             /* 63 */     /* undo */
    "FTA_TER_SET_DIRTY_V1",              /* 64 */
    "FTA_BS_CLOSE_V1",                   /* 65 */
    "FTA_BS_XTNT_REWRITE_MAP_V1",        /* 66 */
    "FTA_BS_STG_ALLOC_MCELL_V1",         /* 67 */
    "FTA_BS_COW_PG",                     /* 68 */
    "FTA_BS_COW",                        /* 69 */
    "FTA_BFS_CREATE",                    /* 70 */
    "FTA_BFS_DEL_PENDING_ADD",           /* 71 */
    "FTA_BFS_DELETE",                    /* 72 */
    "FTA_BFS_DELETE_CLONE",              /* 73 */
    "FTA_BFS_CLONE",                     /* 74 */
    "FTA_BS_RBMT_ALLOC_MCELL_V1",        /* 75 */     /* undo */
    "FTA_BS_BMT_UPDATE_REC_V1",          /* 76 */
    "FTA_BS_BMT_EXTEND_V1",              /* 77 */     /* redo */
    "FTA_BS_RBMT_EXTEND_V1",             /* 78 */     /* redo */
    "FTA_BS_BMT_CLONE_RECS_V1",          /* 79 */
    "FTA_BS_DEL_ADD_LIST_V1",            /* 80 */     /* undo */
    "FTA_BS_DEL_REM_LIST_V1",            /* 81 */     /* undo */
    "FTA_BS_DEL_FREE_PRIMARY_MCELL_V1",  /* 82 NU */
    "FTA_BS_DEL_FREE_MCELL_CHAIN_V1",    /* 83 NU */
    "FTA_BS_DEL_FTX_START_V1",           /* 84 */
    "FTA_BS_DELETE_6",                   /* 85 */
    "FTA_BS_DELETE_7",                   /* 86 NU */
    "FTA_BS_DELETE_8",                   /* 87 NU */
    "FTA_BS_DELETE_9",                   /* 88 NU */
    "FTA_BS_TAG_SWITCH_ROOT_TAGDIR_V1",  /* 89 NU */
    "FTA_BS_SET_NEXT_TAG_V1",            /* 90 */
    "FTA_FS_CREATE_1",                   /* 91 */
    "FTA_FS_CREATE_2",                   /* 92 */
    "FTA_FS_DIR_INIT_1",                 /* 93 */
    "FTA_FS_DIR_LOOKUP_1",               /* 94 NU */
    "FTA_FS_DIR_LOOKUP_2",               /* 95 NU */
    "FTA_FS_FILE_SETS_1",                /* 96 */
    "FTA_FS_INIT_QUOTAS_V1",             /* 97 */
    "FTA_FS_WRITE_ADD_STG_V1",           /* 98 */
    "FTA_FS_WRITE_V1",                   /* 99 NU */
    "FTA_OSF_SETATTR_1",                 /* 100 */
    "FTA_OSF_FSYNC_V1",                  /* 101 */
    "FTA_OSF_REMOVE_V1",                 /* 102 */
    "FTA_OSF_LINK_V1",                   /* 103 */
    "FTA_OSF_RENAME_V1",                 /* 104 */
    "FTA_OSF_RMDIR_V1",                  /* 105 */
    "FTA_FTX_LOG_DATA",                  /* 106 NU */
    "FTA_FS_SYSCALLS_1",                 /* 107 NU */
    "FTA_FS_SYSCALLS_2",                 /* 108 NU */
    "FTA_MSS_COMMON_1",                  /* 109 */
    "FTA_BS_MIG_MOVE_METADATA_V1",       /* 110 */
    "FTA_LGR_SWITCH_VOL",                /* 111 */
    "FTA_BS_XTNT_INSERT_CHAIN_LOCK_V1",  /* 112 NU */
    "FTA_BS_XTNT_REMOVE_CHAIN_LOCK_V1",  /* 113 NU */
    "FTA_BS_STR_UPD_XTNT_REC_LOCK_V1",   /* 114 NU */
    "FTA_FRAG_ALLOC",                    /* 115 */
    "FTA_FTX_SWITCH_LOG",                /* 116 */
    "FTA_FTX_CHECKPOINT_LOG",            /* 117 */
    "FTA_BS_BMT_CREATE_REC",             /* 118 */
    "FTA_FS_QUOTA_OFF_V1",               /* 119 */
    "FTA_BS_STG_SET_ALLOC_DISK_V1",      /* 120 */
    "FTA_BS_SET_BF_PARAMS",              /* 121 */
    "FTA_BS_SET_BF_IPARAMS",             /* 122 NU */
    "FTA_BS_SET_BFSET_PARAMS",           /* 123 */
    "FTA_BS_MIG_MOVE_METADATA_EXC_V1",   /* 124 */
    "FTA_FS_CREATE_ROOT_FILE",           /* 125 */
    "FTA_BS_SWITCH_ROOT_TAGDIR_V1",      /* 126 */    /* redo */
    "FTA_FS_GET_QUOTA_V1",               /* 127 NU */
    "FTA_FS_SET_QUOTA_V1",               /* 128 */
    "FTA_FS_SET_USE_V1",                 /* 129 */
    "FTA_FS_QUOTA_ON_V1",                /* 130 */
    "FTA_FS_DETACH_QUOTA_V1",            /* 131 NU */
    "FTA_FS_ATTACH_QUOTA_V1",            /* 132 */
    "FTA_FS_GET_QUOTA",                  /* 133 NU */
    "FTA_BFS_DEALLOC_FRAG",              /* 134 NU */
    "FTA_FS_CREATE_FRAG",                /* 135 NU */
    "FTA_BFS_CREATE_2",                  /* 136 */
    "FTA_OSF_SETATTR_2",                 /* 137 */
    "FTA_OSF_SYNCDATA_V1",               /* 138 NU */
    "FTA_FS_DQUOT_SYNC_V2",              /* 139 */    /* undo */
    "FTA_MSFS_SETPROPLIST",              /* 140 */    /* cont, rtdn */
    "FTA_MSFS_DELPROPLIST",              /* 141 */    /* cont, rtdn */
    "FTA_BS_BMT_CLONE_MCELL_V1",         /* 142 */
    "FTA_FRAG_ALLOC2",                   /* 143 */
    "FTA_FRAG_GRP_DEALLOC",              /* 144 */
    "FTA_BS_SET_VD_PARAMS",              /* 145 */
    "FTA_MSFS_ALLOC_MCELL",              /* 146 */    /* cont, rtdn */
    "FTA_BS_BMT_DEFERRED_MCELL_FREE",    /* 147 */    /* rtdn */
    "FTA_DATA_LOGGING_WRITE",            /* 148 */
    "FTA_TAG_TO_FREELIST",               /* 149 */
    "FTA_FS_WRITE_TRUNC",                /* 150 */
    "FTA_IDX_UNDO_V1",                   /* 151 */    /* undo */
    "FTA_BS_DEL_TRUNCATE_DDL"            /* 152 */
};

/*****************************************************************************/
#define PRINT_RECADDR(addr)                           \
    printf("%-13s page%6d offset%6d lsn%10d\n",       \
      "addr", pdata->addr.page, pdata->addr.offset,   \
      pdata->addr.lsn.num)

#define PRINTpgType(buf, type)                   \
    switch ( type ) {                            \
      case BFM_BMT_V3:                           \
        sprintf(buf, "BFM_BMT");                 \
        break;                                   \
      case BFM_SBM:                              \
        sprintf(buf, "BFM_SBM");                 \
        break;                                   \
      case BFM_BFSDIR:                           \
        sprintf(buf, "BFM_BFSDIR");              \
        break;                                   \
      case BFM_FTXLOG:                           \
        sprintf(buf, "BFM_FTXLOG");              \
        break;                                   \
      case BFM_BMT_EXT_V3:                       \
        sprintf(buf, "BFM_BMT_EXT");             \
        break;                                   \
      case BFM_MISC:                             \
        sprintf(buf, "BFM_MISC");                \
        break;                                   \
      default:                                   \
        sprintf(buf, "unknown type %d", type);   \
        break;                                   \
    }


void
print_loghdr( FILE *fp, logPgHdrT *pdata )
{
    char line[80];
    int line_len;
    char time_buf[40];
    char buf[80];

    ctime_r(&pdata->dmnId, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';
    sprintf(line, "dmnId %x.%x (%s)  ",
      pdata->dmnId.tv_sec, pdata->dmnId.tv_usec, time_buf);
    printf("%s", line);

    line_len = strlen(line);
    PRINTpgType(buf, pdata->pgType);
    sprintf(line, "pgType %s (%d)\n", buf, pdata->pgType);
    line_len += strlen(line);
    while ( line_len++ < 75 ) {
        printf(" ");
    }
    printf("%s", line);

    printf("pgSafe %d  ", pdata->pgSafe);
    printf("chkBit %d  ", pdata->chkBit);
    printf("curLastRec %-4d ", pdata->curLastRec);
    printf("prevLastRec %-4d ", pdata->prevLastRec);
    printf("thisPageLSN%10d\n", pdata->thisPageLSN.num);

    PRINT_RECADDR(firstLogRec);
    printf("\n");
}

/*****************************************************************************/
/* from ftx_recovery.c */
typedef struct {
    ftxRecRedoT pgdesc;         /* page descriptor */
    ftxRecXT recX[FTX_MX_PINR]; /* record extent list */
} pageredoT;

/* from  ms_logger.c */
#define MORE_SEGS 0x80000000

#define REC_HDR_LEN (REC_HDR_WORDS * sizeof(uint32T))
void
print_logrec( logRecHdrT *pdata, int offset, int flags)
{
    ftxDoneLRT *doneLRTp;
    uint32T wordCnt;

    if ( flags & BFLG ) {
        doneLRTp = (ftxDoneLRT*)((char *)pdata + REC_HDR_LEN);
        if ( (pdata->segment & ~MORE_SEGS) == 0 ) {
            printf("OFF %-4d ", offset);
            printf("ftxId %x ", doneLRTp->ftxId);
            if ( doneLRTp->agentId >= 0 &&
                 doneLRTp->agentId < sizeof(ftx_agents) / sizeof(char *) )
            {
                printf("%s ", ftx_agents[doneLRTp->agentId]);
            } else {
                printf("agentId %d ", doneLRTp->agentId);
            }
            printf("level %d ", doneLRTp->level);
            printf("member %d ", doneLRTp->member);
            printf("next %d %d\n", pdata->nextRec.page, pdata->nextRec.offset);
        }
        return;
    }

    print_logrec_hdr(pdata, offset, flags);
    doneLRTp = (ftxDoneLRT*)((char *)pdata + (REC_HDR_WORDS * sizeof(uint32T)));
    wordCnt = pdata->wordCnt;
    if ( (offset + pdata->wordCnt) > DATA_WORDS_PG ) {
        printf("ftx record goes beyond the end of the page!!\n");
        wordCnt = DATA_WORDS_PG - offset;
    }

    if ( (pdata->segment & ~MORE_SEGS) == 0 ) {
        print_logrec_data(doneLRTp, wordCnt, flags);
    } else {
        if ( flags & VFLG ) {
            print_unknown((char*)doneLRTp, wordCnt * sizeof(uint32T));
        }
    }
}

/*****************************************************************************/
static void
print_logrec_hdr( logRecHdrT *pdata, int offset, int flags)
{
    if ( (pdata->segment & MORE_SEGS) == 0 ) {
        printf("RECORD OFFSET %-4d wordCnt %-3d clientWordCnt %-3d segment %-3d ",
            offset, pdata->wordCnt, pdata->clientWordCnt, pdata->segment);
    } else {
        printf("OFFSET %-4d wordCnt %-3d clientWordCnt %-3d segment %d,MORE_SEGS ",
            offset, pdata->wordCnt, pdata->clientWordCnt,
            pdata->segment & ~MORE_SEGS);
    }

    printf("lsn%10d\n", pdata->lsn.num);
    PRINT_RECADDR(nextRec);
    PRINT_RECADDR(prevRec);
    PRINT_RECADDR(prevClientRec);
    PRINT_RECADDR(firstSeg);

    printf("\n");
}

/*****************************************************************************/
static void
print_logrec_data( ftxDoneLRT *doneLRTp, uint32T wordCnt, int flags)
{
    char *undoRecp, *rtdnRecp, *redoRecp, *redoop, *eor;
    uint undocnt = 0;
    uint rtdncnt = 0;
    uint redocnt = 0;

    eor = (char *)doneLRTp + (wordCnt - REC_HDR_WORDS) * sizeof(uint32T);
    if ( doneLRTp->agentId >= 0 &&
         doneLRTp->agentId < sizeof(ftx_agents) / sizeof(char *) )
    {
        printf("agentId %s (%d)\n",
          ftx_agents[doneLRTp->agentId], doneLRTp->agentId);
    } else {
        printf("agentId %d\n", doneLRTp->agentId);
    }

    printf("type %s, ", doneLRTp->type == ftxNilLR ?
                                       "ftxNilLR" :
                                       doneLRTp->type == ftxDoneLR ?
                                                      "ftxDoneLR" :
                                                      "?? unknown??");
    printf("level %d, ", doneLRTp->level);
    printf("atomicRPass %d, ", doneLRTp->atomicRPass);
    printf("member %d\n", doneLRTp->member);
    printf("ftxId %x, ", doneLRTp->ftxId);
    printf("bfDmnId %x.%x\n",
      doneLRTp->bfDmnId.tv_sec, doneLRTp->bfDmnId.tv_usec);
    printf("%-13s page%6d offset%6d lsn%10d\n",
      "crashRedo",doneLRTp->crashRedo.page,
      doneLRTp->crashRedo.offset,
      doneLRTp->crashRedo.lsn.num);
    printf("contOrUndoRBcnt %d, rootDRBcnt %d, opRedoRBcnt %d\n",
      doneLRTp->contOrUndoRBcnt, doneLRTp->rootDRBcnt, doneLRTp->opRedoRBcnt);

    if ( flags & VFLG ) {
        undoRecp = (char *)doneLRTp + sizeof(ftxDoneLRT);
        if ( doneLRTp->contOrUndoRBcnt ) {
            switch ( doneLRTp->agentId ) {
              case FTA_BS_BMT_FREE_BF_MCELLS_V1:
              case FTA_MSFS_SETPROPLIST:
              case FTA_MSFS_DELPROPLIST:
              case FTA_MSFS_ALLOC_MCELL:
                printf("  continuation record:\n");
                break;
              default:
                printf("  undo record:\n");
                break;
            }
            if ( doneLRTp->contOrUndoRBcnt > wordCnt * sizeof(uint32T) ) {
                undocnt = wordCnt * sizeof(uint32T);
                printf("contOrUndoRBcnt is too large (%d). ",
                  doneLRTp->contOrUndoRBcnt);
                printf("It is being reduced to %d\n", undocnt);
            } else {
                undocnt = doneLRTp->contOrUndoRBcnt;
            }
            assert(undocnt != 0);
            print_contOrUndo(doneLRTp->agentId, undoRecp, undocnt);
            undocnt = roundup(undocnt, sizeof(uint32T));
        }

        rtdnRecp = undoRecp + undocnt;
        if ( doneLRTp->rootDRBcnt ) {
            if ( doneLRTp->rootDRBcnt + undocnt > wordCnt * sizeof(uint32T) ) {
                assert(wordCnt * sizeof(uint32T) >= undocnt);
                rtdncnt = wordCnt * sizeof(uint32T) - undocnt;
                printf("rootDRBcnt is too large (%d). ", doneLRTp->rootDRBcnt);
                printf("It is being reduced to %d\n", rtdncnt);
            } else {
                rtdncnt = doneLRTp->rootDRBcnt;
            }
            if ( rtdncnt != 0 ) {
                printf("  root done record:\n");
                print_rootdone(doneLRTp->agentId, rtdnRecp, rtdncnt);
            }
            rtdncnt = roundup(rtdncnt, sizeof(uint32T));
        }

        redoRecp = rtdnRecp + rtdncnt;
        if ( doneLRTp->opRedoRBcnt ) {
            if ( doneLRTp->opRedoRBcnt + rtdncnt + undocnt >
                 wordCnt * sizeof(uint32T) )
            {
                assert(wordCnt * sizeof(uint32T) >= undocnt + rtdncnt);
                redocnt = wordCnt * sizeof(uint32T) - undocnt - rtdncnt;
                printf("opRedoRBcnt is too large (%d). ",doneLRTp->opRedoRBcnt);
                printf("It is being reduced to %d\n", redocnt);
            } else {
                redocnt = doneLRTp->opRedoRBcnt;
            }
            if ( redocnt != 0 ) {
                printf("  redo record:\n");
                print_redo(doneLRTp->agentId, redoRecp, redocnt);
            }
            redocnt = roundup(redocnt, sizeof(uint32T));
        }

        redoop = redoRecp + redocnt;

        if ( redoop < eor ) {
            printf("  image redo records:\n");
        }
        while ( redoop < eor ) {
            print_imageRedo(&redoop, eor - redoop);
        }
    } else {
        printf("\n");
    }
}

static void
print_contOrUndo( ftxAgentIdT agent, char *recp, int recCnt)
{
    switch (agent) {
      case FTA_BS_BFS_CREATE_V1:                        /* 1 */
        printf("**** this transaction is not used. ****\n");
        print_unlinkCloneUndoRec(recp, recCnt);
        break;
      case FTA_BS_BFS_UNLINK_CLONE_V1:                  /* 2 */
        print_unlinkCloneUndoRec(recp, recCnt);
        break;
      case FTA_BS_BFS_DEL_LIST_REMOVE_V1:               /* 3 */
        print_delPendUndoRec(recp, recCnt);
        break;
      case FTA_BS_BMT_PUT_REC_V1:                       /* 5 */
        print_bmtrPutRecUndo(recp, recCnt);
        break;
      case FTA_BS_BMT_ALLOC_MCELL_V1:                   /* 7 */
      case FTA_BS_BMT_DEFERRED_MCELL_FREE:              /* 147 */
        print_allocMcellUndoRec(recp, recCnt);
        break;
      case FTA_BS_BMT_ALLOC_LINK_MCELL_V1:              /* 8 */
        print_allocLinkMcellUndoRec(recp, recCnt);
        break;
      case FTA_BS_BMT_LINK_MCELLS_V1:                   /* 9 */
      case FTA_BS_BMT_UNLINK_MCELLS_V1:                 /* 10 */
        print_linkUnlinkMcellsUndoRec(recp, recCnt);
        break;
      case FTA_BS_BF_CREATE_V1:                         /* 11 */
      case FTA_BS_CRE_MCELL_V1:                         /* 12 */
        print_mcellUId(recp, recCnt);
        break;
      case FTA_BS_DEL_DELETE_V1:                        /* 13 */
        print_delOpRecT(recp, recCnt);
        break;
      case FTA_BS_DEL_DEFERRED_V1:                      /* 14 */
        print_defDelOpRec(recp, recCnt);
        break;
      case FTA_BS_BMT_FREE_BF_MCELLS_V1:                /* 15 */
        print_mcellPtrRec(recp, recCnt);       /* continuation */
        break;
      case FTA_BS_XTNT_UPD_MCELL_CNT_V1:                /* 16 */
        print_mcellCntUndoRec(recp, recCnt);
        break;
      case FTA_BS_XTNT_UPD_REC_V1:                      /* 17 */
        print_updXtntRecUndoRec(recp, recCnt);
        break;
      case FTA_BS_XTNT_CRE_REC:                         /* 18 */
        print_creXtntRecUndoRec(recp, recCnt);
        break;
      case FTA_BS_XTNT_INSERT_CHAIN_V1:                 /* 21 */
      case FTA_BS_XTNT_REMOVE_CHAIN_V1:                 /* 22 */
        print_insertRemoveXtntChainUndoRec(recp, recCnt);
        break;
      case FTA_BS_XTNT_ZERO_MAP_V1_UNUSED:              /* 23 */
        printf("Agent FTA_BS_XTNT_ZERO_MAP_V1_UNUSED is not used. ifdef 0\n");
        print_zeroUndoRec(recp, recCnt);
        break;
      case FTA_BS_SHA_SHADOW_V1:                        /* 31 */
        printf("Agent FTA_BS_SHA_SHADOW_V1 is not used. ifdef UNDO\n");
        print_bitfileTypeUndoRec(recp, recCnt);
        break;
      case FTA_BS_SHA_BITFILE_TYPE_V1:                  /* 32 */
        printf("Agent FTA_BS_SHA_BITFILE_TYPE_V1 is not used. ifdef UNDO\n");
        print_bitfileTypeUndoRec(recp, recCnt);
        break;
      case FTA_BS_SBM_ALLOC_BITS_V1:                    /* 33 */
        print_bitmapUndoRec(recp, recCnt);
        break;
      case FTA_BS_STG_ADD_V1:                           /* 34 */
        print_addStgUndoRec(recp, recCnt);
        break;
      case FTA_BS_SBM_DEALLOC_BITS_V1:                  /* 36 */
        printf("Agent FTA_BS_SBM_DEALLOC_BITS_V1 is not used.\n");
        print_bitmapUndoRec(recp, recCnt);
        break;
      case FTA_BS_STR_UPD_XTNT_REC_V1:                  /* 39 */
        print_xtntRecUndoRec(recp, recCnt);
        break;
      case FTA_BS_TAG_WRITE_V1:                         /* 40 */
        print_tagDataRec(recp, recCnt);
        break;
      case FTA_BS_TAG_EXTEND_TAGDIR_V1:                 /* 41 */
        print_tagUnInitPageRedo(recp, recCnt);
        break;
      case FTA_FS_INSERT_V1:                            /* 46 */
        print_insert_undo_rec(recp, recCnt);
        break;
      case FTA_FS_DQUOT_SYNC_V1:                        /* 49 */
        printf("Agent FTA_FS_DQUOT_SYNC_V1 is not used.\n");
        print_quotaRecObsolete(recp, recCnt);
        break;
      case FTA_TER_UPDATE_PTR_NULL:                     /* 52 */
        print_updatePtrUndoRec(recp, recCnt);
        break;
      case FTA_TER_APPEND_TO_TER_NULL:                  /* 53 */
      case FTA_TER_APPEND_TO_XTRA_NULL:                 /* 54 */
        print_appendUndoRec(recp, recCnt);
        break;
      case FTA_TER_UPDATE_TOTAL_NULL:                   /* 55 */
        print_updateTotalUndoRec(recp, recCnt);
        break;
      case FTA_TER_FLAGS_TER_NULL:                      /* 56 */
      case FTA_TER_FLAGS_XTRA_NULL:                     /* 57 */
        print_flagsUndoRec(recp, recCnt);
        break;
      case FTA_TER_SET_ACCESS_NULL:                     /* 63 */
        print_setAccessUndoRec(recp, recCnt);
        break;
      case FTA_BS_RBMT_ALLOC_MCELL_V1:                  /* 75 */
        print_allocMcellUndoRec(recp, recCnt);
        break;
      case FTA_BS_DEL_ADD_LIST_V1:                      /* 80 */
        print_delListRec(recp, recCnt);
        break;
      case FTA_BS_DEL_REM_LIST_V1:                      /* 81 */
        print_delListRec(recp, recCnt);
        break;
      case FTA_FS_DQUOT_SYNC_V2:                        /* 139 */
        print_quotaRec(recp, recCnt);
        break;
      case FTA_MSFS_SETPROPLIST:                        /* 140 */
      case FTA_MSFS_DELPROPLIST:                        /* 141 */
      case FTA_MSFS_ALLOC_MCELL:                        /* 146 */
        print_mcellPtrRec(recp, recCnt);           /* continuation */
        break;
      case FTA_IDX_UNDO_V1:                             /* 151 */
        print_idxUndoRecord(recp, recCnt);
        break;
      default:
        printf("Unexpected undo/continuation record for agent ID %d\n", agent);
        if ( recCnt > PAGE_SIZE ) {
            printf("record size is too large. Reduced to %d.\n", PAGE_SIZE);
            recCnt = PAGE_SIZE;
        }
        print_unknown(recp, recCnt);
        break;
    }
}

/*****************************************************************************/
static void
print_rootdone( ftxAgentIdT agent, char *recp, int recCnt)
{
    switch (agent) {
      case FTA_BS_BF_CREATE_V1:                         /* 11 */
        print_mcellUId(recp, recCnt);
        break;
      case FTA_BS_DEL_DELETE_V1:                        /* 13 */
        print_delOpRecT(recp, recCnt);
        break;
      case FTA_BS_BMT_FREE_BF_MCELLS_V1:                /* 15 */
        print_mcellPtrRec(recp, recCnt);
        break;
      case FTA_BS_SBM_ALLOC_BITS_V1:                    /* 33 */
      case FTA_BS_SBM_DEALLOC_BITS_V1:                  /* 36 */
        printf("Agent root done routine not coded.\n");
        break;
      case FTA_MSFS_SETPROPLIST:                        /* 140 */
      case FTA_MSFS_DELPROPLIST:                        /* 141 */
      case FTA_MSFS_ALLOC_MCELL:                        /* 146 */
        print_delPropRootDone(recp, recCnt);
        break;
      case FTA_BS_BMT_DEFERRED_MCELL_FREE:              /* 147 */
        print_allocMcellUndoRec(recp, recCnt);
        break;
      default:
        printf("Unexpected root done record for agent ID %d\n", agent);
        if ( recCnt > PAGE_SIZE ) {
            printf("record size is too large. Reduced to %d.\n", PAGE_SIZE);
            recCnt = PAGE_SIZE;
        }
        print_unknown(recp, recCnt);
        break;
    }
}

/*****************************************************************************/
static void
print_redo( ftxAgentIdT agent, char *recp, int recCnt)
{
    switch (agent) {
      case FTA_BS_TAG_EXTEND_TAGDIR_V1:                 /* 41 */
        print_tagUnInitPageRedo(recp, recCnt);
        break;
      case FTA_BS_BMT_EXTEND_V1:                        /* 77 */
        print_vdIndex(recp, recCnt);
        break;
      case FTA_BS_RBMT_EXTEND_V1:                       /* 78 */
        print_vdIndex(recp, recCnt);
        break;
      case FTA_BS_SWITCH_ROOT_TAGDIR_V1:                /* 126 */
        print_bfTag(recp, recCnt);
        break;
      default:
        printf("Unexpected redo record for agent ID %d\n", agent);
        if ( recCnt > PAGE_SIZE ) {
            printf("record size is too large. Reduced to %d.\n", PAGE_SIZE);
            recCnt = PAGE_SIZE;
        }
        print_unknown(recp, recCnt);
        break;
    }
}

/*****************************************************************************/
static void
print_imageRedo( char **redopa, int len)
{
    pageredoT *pgredop = (pageredoT*)*redopa;
    int i;
    char *datap;
    int numXtnts = pgredop->pgdesc.numXtnts;
    int bcnt;
    int pgBoff;

    datap = (char *)pgredop + sizeof(ftxRecRedoT) +
            pgredop->pgdesc.numXtnts * sizeof(ftxRecXT);

    printf("    bfsTag,tag %d,%d  page %d  numXtnts %d\n",
      pgredop->pgdesc.bfsTag.num, pgredop->pgdesc.tag.num,
      pgredop->pgdesc.page, pgredop->pgdesc.numXtnts);

    if ( numXtnts >= FTX_MX_PINR ) {
        fflush(stdout);
        fprintf(stderr,
          "Bad LOG. pageredoT.pgdesc.numXtnts can not exceed %d\n",
          FTX_MX_PINR);

        numXtnts = FTX_MX_PINR;
    }

    for ( i = 0; i < numXtnts; i++ ) {
        datap += pgredop->recX[i].pgBoff & 3;
        bcnt = pgredop->recX[i].bcnt;
        pgBoff = pgredop->recX[i].pgBoff;
        printf("      pgBoff %d  bcnt %d\n", pgBoff, bcnt);
        if ( pgBoff >= PAGE_SIZE ) {
            fflush(stdout);
            fprintf(stderr,
              "Bad LOG. pageredoT.pgdesc.recX[%d].pgBoff too large.\n", i);
        } else if ( pgBoff + bcnt > PAGE_SIZE ) {
            fflush(stdout);
            fprintf(stderr,
      "Bad LOG. pageredoT.pgdesc.recX[%d] page offset + count too large.\n", i);
        }

        if ( datap + bcnt > (char*)pgredop + len ) {
            bcnt = (char*)pgredop + len - datap;
        }

        print_unknown(datap, bcnt);
        datap = (char*)(((long)(datap + bcnt + 3) >> 2) << 2);
        if ( datap > (char*)pgredop + len ) {
            fflush(stdout);
            fprintf(stderr,
              "Bad LOG. Missing redo extents in this log record.\n");
            break;
        }
    }
    *redopa = datap;
}

/*****************************************************************************/
print_logtrlr( FILE *fp, logPgTrlrT *pdata)
{
    char line[80];
    int line_len;
    int i;
    char time_buf[40];
    char buf[80];

    printf(SINGLE_LINE);

    ctime_r(&pdata->dmnId, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';
    sprintf(line, "dmnId %x.%x (%s)  ",
      pdata->dmnId.tv_sec, pdata->dmnId.tv_usec, time_buf);
    printf("%s", line);

    line_len = strlen(line);
    PRINTpgType(buf, pdata->pgType);
    sprintf(line, "pgType %s (%d)\n", buf, pdata->pgType);
    line_len += strlen(line);
    while ( line_len++ < 75 ) {
        printf(" ");
    }

    printf("%s", line);
}

/*****************************************************************************/
static char*
tag_to_str( bfTagT tag, char *buf)
{
    sprintf(buf, "%d (%x.%x)", tag.num, tag.num, tag.seq);
    return buf;
}

/*****************************************************************************/
static char*
setid_to_str( bfSetIdT bfSetId, char *buf)
{
    char time_buf[40];

    ctime_r(&bfSetId.domainId, time_buf);
    time_buf[strlen(time_buf) - 1] = '\0';
    sprintf(buf, "%08x.%08x.%x.%x  (\"%s\", %d)",
      bfSetId.domainId.tv_sec, bfSetId.domainId.tv_usec,
      bfSetId.dirTag.num, bfSetId.dirTag.seq, time_buf, bfSetId.dirTag.num);

    return buf;
}

/******************************************************************************/
/* FTA_BS_BFS_CREATE_V1  1 not used */
/* FTA_BS_BFS_UNLINK_CLONE_V1  2 */

/* from bs_bitfile_sets.c */
typedef struct {
    bfSetIdT origSetId;
    bfTagT   nextCloneSetTag;
} unlinkCloneUndoRecT;

static void
print_unlinkCloneUndoRec( char *recp, int recCnt)
{
    unlinkCloneUndoRecT record;
    unlinkCloneUndoRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(unlinkCloneUndoRecT));

    if ( recCnt != sizeof(unlinkCloneUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(unlinkCloneUndoRecT));
    }

    printf("    origSetId %s  ", setid_to_str(rp->origSetId, buf));
    printf("nextCloneSetTag %s\n", tag_to_str(rp->nextCloneSetTag, buf));
}

/******************************************************************************/
/* FTA_BS_BFS_DEL_LIST_REMOVE_V1  3 */

/* from bs_bitfile_sets.c */
typedef struct {
    bfSetIdT setId;
} delPendUndoRecT;

static void
print_delPendUndoRec( char *recp, int recCnt)
{
    delPendUndoRecT record;
    delPendUndoRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(delPendUndoRecT));

    if ( recCnt != sizeof(delPendUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(delPendUndoRecT));
    }

    printf("    setId %s\n", setid_to_str(rp->setId, buf));
}

/******************************************************************************/
/* FTA_BS_BMT_PUT_REC_V1  5 */

/* from bs_bmt_util.c */
typedef struct {
    bfTagT  bfTag;
    bfSetIdT bfSetId;
    bfMCIdT mcid;
    uint32T recOffset;
    u_short vdIndex;
    u_short beforeImageLen;
    char beforeImage[BS_USABLE_MCELL_SPACE];
} bmtrPutRecUndoT;

static void
print_bmtrPutRecUndo( char *recp, int recCnt)
{
    bmtrPutRecUndoT record;
    bmtrPutRecUndoT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(bmtrPutRecUndoT));

    if ( recCnt != (long)rp->beforeImage - (long)rp + rp->beforeImageLen ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(bmtrPutRecUndoT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    bfTag %s  ", tag_to_str(rp->bfTag, buf));
    printf("vdIndex %d  ", rp->vdIndex);
    printf("mcid page,cell %d,%d\n", rp->mcid.page, rp->mcid.cell);
    printf("    recOffset %d  ", rp->recOffset);
    printf("beforeImageLen %d\n", rp->beforeImageLen);
    if ( rp->beforeImageLen > BS_USABLE_MCELL_SPACE ) {
        printf("    beforeImageLen is too large. ");
        printf("beforeImageLen reduced to %d\n", BS_USABLE_MCELL_SPACE);
        rp->beforeImageLen = BS_USABLE_MCELL_SPACE;
    }
    print_unknown(rp->beforeImage, rp->beforeImageLen);
}

/******************************************************************************/
/* FTA_BS_BMT_ALLOC_MCELL_V1  7 */
/* FTA_BS_BMT_ALLOC_PAGE0_MCELL_V1  75 */
/* FTA_BS_BMT_DEFERRED_MCELL_FREE  147 */

/* from bs_bmt_util.c */
typedef struct allocMcellUndoRec {
    uint16T newVdIndex;
    bfMCIdT newMcellId;
} allocMcellUndoRecT;

static void
print_allocMcellUndoRec( char *recp, int recCnt)
{
    allocMcellUndoRecT record;
    allocMcellUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(allocMcellUndoRecT));

    if ( recCnt != sizeof(allocMcellUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(allocMcellUndoRecT));
    }

    printf("    newVdIndex %d  ", rp->newVdIndex);
    printf("newMcellId page,cell %d,%d\n",
      rp->newMcellId.page, rp->newMcellId.cell);
}

/******************************************************************************/
/* FTA_BS_BMT_ALLOC_LINK_MCELL_V1  8 */

/* from bs_bmt_util.c */
typedef struct allocLinkMcellUndoRec {
    uint16T vdIndex;
    bfMCIdT mcellId;
    uint16T newVdIndex;
    bfMCIdT newMcellId;
} allocLinkMcellUndoRecT;

static void
print_allocLinkMcellUndoRec( char *recp, int recCnt)
{
    allocLinkMcellUndoRecT record;
    allocLinkMcellUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(allocLinkMcellUndoRecT));

    if ( recCnt != sizeof(allocLinkMcellUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(allocLinkMcellUndoRecT));
    }

    printf("    vdIndex %d  mcellId.page,cell %d %d\n",
      rp->vdIndex, rp->mcellId.page, rp->mcellId.cell);

    printf("    newVdIndex %d  newMcellId.page,cell %d %d\n",
      rp->newVdIndex, rp->newMcellId.page, rp->newMcellId.cell);
}

/******************************************************************************/
/* FTA_BS_BMT_LINK_MCELLS_V1  9 */
/* FTA_BS_BMT_UNLINK_MCELLS_V1  10 */

/* from bs_bmt_util.c */
/* this changes in rev 1.1.22.21 */
typedef struct old_linkUnlinkMcellsUndoRec {
    vdIndexT prevVdIndex;
    bfMCIdT prevMCId;
    vdIndexT prevNextVdIndex;
    bfMCIdT prevNextMCId;
    vdIndexT lastVdIndex;
    bfMCIdT lastMCId;
    vdIndexT lastNextVdIndex;
    bfMCIdT lastNextMCId;
} old_linkUnlinkMcellsUndoRecT;

typedef struct linkUnlinkMcellsUndoRec {
    bfTagT bfTag;
    vdIndexT prevVdIndex;
    bfMCIdT prevMCId;
    vdIndexT prevNextVdIndex;
    bfMCIdT prevNextMCId;
    vdIndexT lastVdIndex;
    bfMCIdT lastMCId;
    vdIndexT lastNextVdIndex;
    bfMCIdT lastNextMCId;
} linkUnlinkMcellsUndoRecT;


static void
print_linkUnlinkMcellsUndoRec( char *recp, int recCnt)
{
    old_linkUnlinkMcellsUndoRecT old_record;
    old_linkUnlinkMcellsUndoRecT *old_rp = &old_record;
    linkUnlinkMcellsUndoRecT record;
    linkUnlinkMcellsUndoRecT *rp = &record;
    char buf[80];

    if ( recCnt == sizeof(linkUnlinkMcellsUndoRecT) ) {
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(linkUnlinkMcellsUndoRecT));

        printf("    bfTag %s\n", tag_to_str(rp->bfTag, buf));
        printf("    prevVdIndex %d  prevMCId page,cell %d,%d\n",
          rp->prevVdIndex, rp->prevMCId.page, rp->prevMCId.cell);

        printf("    prevNextVdIndex %d  prevNextMCId page,cell %d,%d\n",
          rp->prevNextVdIndex, rp->prevNextMCId.page, rp->prevNextMCId.cell);

        printf("    lastVdIndex %d  lastMCId page,cell %d,%d\n",
          rp->lastVdIndex, rp->lastMCId.page, rp->lastMCId.cell);

        printf("    lastNextVdIndex %d  lastNextMCId page,cell %d,%d\n",
          rp->lastNextVdIndex, rp->lastNextMCId.page, rp->lastNextMCId.cell);
    } else if ( recCnt == sizeof(old_linkUnlinkMcellsUndoRecT) ) {
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)old_rp, (void*)recp, sizeof(old_linkUnlinkMcellsUndoRecT));

        printf("    prevVdIndex %d  prevMCId page,cell %d,%d\n",
          old_rp->prevVdIndex, old_rp->prevMCId.page, old_rp->prevMCId.cell);

        printf("    prevNextVdIndex %d  prevNextMCId page,cell %d,%d\n",
          old_rp->prevNextVdIndex,
          old_rp->prevNextMCId.page, old_rp->prevNextMCId.cell);

        printf("    lastVdIndex %d  lastMCId page,cell %d,%d\n",
          old_rp->lastVdIndex, old_rp->lastMCId.page, old_rp->lastMCId.cell);

        printf("    lastNextVdIndex %d  lastNextMCId page,cell %d,%d\n",
          old_rp->lastNextVdIndex,
          old_rp->lastNextMCId.page, old_rp->lastNextMCId.cell);
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(linkUnlinkMcellsUndoRecT));
    }
}

/******************************************************************************/
/* FTA_BS_BF_CREATE_V1  11 */
/* FTA_BS_CRE_MCELL_V1  12 */

/* from bs_public.h */
/*
typedef struct {
    bfMCIdT mcell;
    vdIndexT vdIndex;
    bfUTagT ut;
} mcellUIdT;
*/

static void
print_mcellUId( char *recp, int recCnt)
{
    mcellUIdT record;
    mcellUIdT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(mcellUIdT));

    if ( recCnt != sizeof(mcellUIdT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(mcellUIdT));
    }

    printf("    vdIndex %d  mcell page,cell %d,%d\n",
      rp->vdIndex, rp->mcell.page, rp->mcell.cell);

    printf("    ut.bfsid %s\n", setid_to_str(rp->ut.bfsid, buf));
    printf("    ut.tag %s\n", tag_to_str(rp->ut.tag, buf));
}

/******************************************************************************/
/* FTA_BS_DEL_DELETE_V1  13 */

#define PRINT_lkStatesT(lkStates) {              \
  switch ( lkStates ) {                          \
    case LKW_NONE:                               \
      printf("LKW_NONE");                        \
      break;                                     \
    case ACC_VALID:                              \
      printf("ACC_VALID");                       \
      break;                                     \
    case ACC_INVALID:                            \
      printf("ACC_INVALID");                     \
      break;                                     \
    case ACC_INIT_TRANS:                         \
      printf("ACC_INIT_TRANS");                  \
      break;                                     \
    case ACC_FTX_TRANS:                          \
      printf("ACC_FTX_TRANS");                   \
      break;                                     \
    case ACC_RECYCLE:                            \
      printf("ACC_RECYCLE");                     \
      break;                                     \
    case ACC_DEALLOC:                            \
      printf("ACC_DEALLOC");                     \
      break;                                     \
    case ACTIVE_DISK:                            \
      printf("ACTIVE_DISK");                     \
      break;                                     \
    case INACTIVE_DISK:                          \
      printf("INACTIVE_DISK");                   \
      break;                                     \
    case BLOCKED_Q:                              \
      printf("BLOCKED_Q");                       \
      break;                                     \
    case UNBLOCKED_Q:                            \
      printf("UNBLOCKED_Q");                     \
      break;                                     \
    case BUF_DIRTY:                              \
      printf("BUF_DIRTY");                       \
      break;                                     \
    case BUF_BUSY:                               \
      printf("BUF_BUSY");                        \
      break;                                     \
    case BUF_UNPIN_BLOCK:                        \
      printf("BUF_UNPIN_BLOCK");                 \
      break;                                     \
    case BUF_PIN_BLOCK:                          \
      printf("BUF_PIN_BLOCK");                   \
      break;                                     \
    case BUF_AVAIL:                              \
      printf("BUF_AVAIL");                       \
      break;                                     \
    case NO_BUF_AVAIL:                           \
      printf("NO_BUF_AVAIL");                    \
      break;                                     \
    default:                                     \
      printf("unknown lkStates %d", lkStates);   \
      break;                                     \
  }                                              \
}

#define PRINT_bfStatesT(bfStates) {              \
  switch ( bfStates ) {                          \
    case BSRA_INVALID:                           \
      printf("BSRA_INVALID");                    \
      break;                                     \
    case BSRA_CREATING:                          \
      printf("BSRA_CREATING");                   \
      break;                                     \
    case BSRA_DELETING:                          \
      printf("BSRA_DELETING");                   \
      break;                                     \
    case BSRA_VALID:                             \
      printf("BSRA_VALID");                      \
      break;                                     \
    default:                                     \
      printf("unknown bfStates %d", bfStates);   \
      break;                                     \
  }                                              \
}

/* from bs_delete.c */
typedef struct {
    bfTagT tag;
    bfMCIdT mcid;
    uint32T vdIndex;
    bfSetIdT bfSetId;
    lkStatesT prevAccState;
    bfStatesT prevBfState;
    uint32T setBusy;
} delOpRecT;

static void
print_delOpRecT( char *recp, int recCnt)
{
    delOpRecT record;
    delOpRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(delOpRecT));

    if ( recCnt != sizeof(delOpRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(delOpRecT));
    }

    printf("    vdIndex %d  ", rp->vdIndex);
    printf("mcid page,cell %d,%d\n", rp->mcid.page, rp->mcid.cell);

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    tag %s\n", tag_to_str(rp->tag, buf));

    printf("    prevAccState ");
    PRINT_lkStatesT(rp->prevAccState);
    printf("  prevBfState ");
    PRINT_bfStatesT(rp->prevBfState);
    printf("  setBusy %d\n", rp->setBusy);
}

/******************************************************************************/
/* FTA_BS_DEL_DEFERRED_V1  14 */

/* from bs_delete.c */
typedef struct {
    bfTagT tag;
    bfSetIdT bfSetId;
    short prevDeleteWithClone;
} defDelOpRecT;

static void
print_defDelOpRec( char *recp, int recCnt)
{
    defDelOpRecT record;
    defDelOpRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(defDelOpRecT));

    if ( recCnt != sizeof(defDelOpRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(defDelOpRecT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    tag %s  ", tag_to_str(rp->tag, buf));
    printf("prevDeleteWithClone %d\n", rp->prevDeleteWithClone);
}

/******************************************************************************/
/* FTA_BS_BMT_FREE_BF_MCELLS_V1  15 */
/* FTA_MSFS_SETPROPLIST  140 */
/* FTA_MSFS_DELPROPLIST  141 */
/* FTA_MSFS_ALLOC_MCELL  146 */

/* from bs_bmt.h */
typedef struct {
    bfMCIdT mcid;
    vdIndexT vdIndex;
} mcellPtrRecT;

static void
print_mcellPtrRec( char *recp, int recCnt)
{
    mcellPtrRecT record;
    mcellPtrRecT *rp = &record;
    int chainCnt = recCnt / sizeof(mcellPtrRecT);

    if ( chainCnt == 0 ) {
        chainCnt = 1;
    }
    if ( recCnt != chainCnt * sizeof(mcellPtrRecT) || chainCnt > 3 ) {
        if ( chainCnt > 3 ) {
            chainCnt = 3;
        }
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, chainCnt * sizeof(mcellPtrRecT));
    }

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, chainCnt * sizeof(mcellPtrRecT));

    do {
        printf("    vdIndex %d  mcid.page,cell %d,%d\n",
          rp->vdIndex, rp->mcid.page, rp->mcid.cell);

        chainCnt--;
        rp++;
    } while ( chainCnt > 0 );
}

/******************************************************************************/
/* FTA_BS_XTNT_UPD_MCELL_CNT_V1  16 */

#define PRINT_xtnt_type(type) {                \
    if ( type == BSR_XTNTS ) {                 \
        printf("BSR_XTNTS");                   \
    } else if ( type == BSR_SHADOW_XTNTS ) {   \
        printf("BSR_SHADOW_XTNTS");            \
    } else if ( type == BSR_XTRA_XTNTS ) {     \
        printf("BSR_XTRA_XTNTS");              \
    } else {                                   \
        printf("unknown type %d", type);       \
    }                                          \
}

/* from bs_extents.c */
typedef struct mcellCntUndoRec {
    uint32T type;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bfTagT bfTag;
    uint32T mcellCnt;
} mcellCntUndoRecT;

/* The mcellCntUndoRec struct was changed in bs_extents.c ver 1.1.16.15 */
typedef struct old_mcellCntUndoRec {
    uint32T type;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T mcellCnt;
} old_mcellCntUndoRecT;

static void
print_mcellCntUndoRec( char *recp, int recCnt)
{
    mcellCntUndoRecT record;
    mcellCntUndoRecT *rp = NULL;
    old_mcellCntUndoRecT old_record;
    old_mcellCntUndoRecT *old_rp = NULL;
    char buf[80];

    if ( recCnt == sizeof(mcellCntUndoRecT) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(mcellCntUndoRecT));
    } else if ( recCnt == sizeof(old_mcellCntUndoRecT) ) {
        old_rp = &old_record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)old_rp, (void*)recp, sizeof(old_mcellCntUndoRecT));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(mcellCntUndoRecT));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(mcellCntUndoRecT));
    }

    if ( rp != NULL ) {
        printf("    vdIndex %d  mCId page,cell %d,%d\n",
          rp->vdIndex, rp->mCId.page, rp->mCId.cell);
        printf("    bfTag %s  ", tag_to_str(rp->bfTag, buf));
        printf("type ");
        PRINT_xtnt_type(rp->type);
        printf("  mcellCnt %d\n", rp->mcellCnt);
    } else {
        printf("    vdIndex %d  mCId page,cell %d,%d\n",
          old_rp->vdIndex, old_rp->mCId.page, old_rp->mCId.cell);
        printf("    type ");
        PRINT_xtnt_type(old_rp->type);
        printf("  mcellCnt %d\n", old_rp->mcellCnt);
    }
}

/******************************************************************************/
/* FTA_BS_XTNT_UPD_REC_V1  17 */

/* from bs_extents.c */
typedef struct old_updXtntRecUndoRec {
    uint16T type;
    uint16T xCnt;
    uint16T index;
    uint16T cnt;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    union {
        bsXtntT xtnt[BMT_XTNTS];
        bsXtntT shadow[BMT_SHADOW_XTNTS];
        bsXtntT xtraXtnt[BMT_XTRA_XTNTS];
    } bsXA;
} old_updXtntRecUndoRecT;

/* change introduced in bs_extents.c, rev 1.1.16.15 */
typedef struct updXtntRecUndoRec
{
    uint16T type;
    uint16T xCnt;
    uint16T index;
    uint16T cnt;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bfTagT bfTag;
    union {
        bsXtntT xtnt[BMT_XTNTS];
        bsXtntT shadow[BMT_SHADOW_XTNTS];
        bsXtntT xtraXtnt[BMT_XTRA_XTNTS];
    } bsXA;
} updXtntRecUndoRecT;


static void
print_updXtntRecUndoRec( char *recp, int recCnt)
{
    old_updXtntRecUndoRecT old_record;
    old_updXtntRecUndoRecT *old_rp = &old_record;
    updXtntRecUndoRecT record;
    updXtntRecUndoRecT *rp = &record;
    int i;
    uint xcnt;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, recCnt);
    memcpy((void*)old_rp, (void*)recp, recCnt);

    if ( recCnt == (char*)&old_rp->bsXA.xtnt[old_rp->cnt] - (char*)old_rp ) {
        rp = NULL;
        xcnt = old_rp->cnt;
    } else if ( recCnt == (char*)&rp->bsXA.xtnt[rp->cnt] - (char*)rp ) {
        old_rp = NULL;
        xcnt = rp->cnt;
    } else {
        old_rp = NULL;

        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, (char*)&rp->bsXA.xtnt[rp->cnt] - (char*)rp);

        xcnt = (recCnt - ((char*)&rp->bsXA.xtnt[0] - (char*)&rp->type)) / 
          sizeof(bsXtntT);
        if ( xcnt != rp->cnt ) {
            printf("xtnt cnt is %d, calculated xtnt cnt is %d. using %d\n",
              rp->cnt, xcnt, MIN(rp->cnt, xcnt));
            xcnt = MIN(rp->cnt, xcnt);
        }
    }

    if ( rp ) {
        printf("    bfTag %s\n", tag_to_str(rp->bfTag, buf));
        printf("    vdIndex %d  mCId page,cell %d,%d  ",
          rp->vdIndex, rp->mCId.page, rp->mCId.cell);
        printf("index %d, ", rp->index);
        printf("cnt %d, ", rp->cnt);
        PRINT_xtnt_type(rp->type);
        printf(", xCnt %d\n", rp->xCnt);

        for (i = 0; i < xcnt; i++) {
            printf("      xtnt bsPage %d vdBlk %d\n",
              rp->bsXA.xtnt[i].bsPage, rp->bsXA.xtnt[i].vdBlk);
        }
    } else {
        printf("    vdIndex %d  mCId page,cell %d,%d  ",
          old_rp->vdIndex, old_rp->mCId.page, old_rp->mCId.cell);
        printf("index %d, ", old_rp->index);
        printf("cnt %d, ", old_rp->cnt);
        PRINT_xtnt_type(old_rp->type);
        printf(", xCnt %d\n", old_rp->xCnt);

        for (i = 0; i < xcnt; i++) {
            printf("      xtnt bsPage %d vdBlk %d\n",
              old_rp->bsXA.xtnt[i].bsPage, old_rp->bsXA.xtnt[i].vdBlk);
        }
    }
}

/******************************************************************************/
/* FTA_BS_XTNT_CRE_REC  18 */

/* added in bs_extents.c, rev 1.1.148.1 */
typedef struct creXtntRecUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mcellId;
} creXtntRecUndoRecT;

static void
print_creXtntRecUndoRec( char *recp, int recCnt)
{
    creXtntRecUndoRecT record;
    creXtntRecUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(creXtntRecUndoRecT));

    if ( recCnt != sizeof(creXtntRecUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(creXtntRecUndoRecT));
    }

    printf("    vdIndex %d  mcellId page,cell %d,%d\n",
      rp->vdIndex, rp->mcellId.page, rp->mcellId.cell);
}

/******************************************************************************/
/* FTA_BS_XTNT_INSERT_CHAIN_V1  21 */
/* FTA_BS_XTNT_REMOVE_CHAIN_V1  22 */

/* from bs_extents.c */
typedef struct insertRemoveXtntChainUndoRec {
    vdIndexT primVdIndex;
    bfMCIdT primMCId;
    vdIndexT primChainVdIndex;
    bfMCIdT primChainMCId;
    vdIndexT lastVdIndex;
    bfMCIdT lastMCId;
    vdIndexT lastNextVdIndex;
    bfMCIdT lastNextMCId;
} insertRemoveXtntChainUndoRecT;

static void
print_insertRemoveXtntChainUndoRec( char *recp, int recCnt)
{
    insertRemoveXtntChainUndoRecT record;
    insertRemoveXtntChainUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(insertRemoveXtntChainUndoRecT));

    if ( recCnt != sizeof(insertRemoveXtntChainUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(insertRemoveXtntChainUndoRecT));
    }

    printf("    primVdIndex %d  primMCId page,cell %d,%d\n",
      rp->primVdIndex, rp->primMCId.page, rp->primMCId.cell);

    printf("    primChainVdIndex %d  primChainMCId page,cell %d,%d\n",
      rp->primChainVdIndex, rp->primChainMCId.page, rp->primChainMCId.cell);

    printf("    lastVdIndex %d  lastMCId page,cell %d,%d\n",
      rp->lastVdIndex, rp->lastMCId.page, rp->lastMCId.cell);

    printf("    lastNextVdIndex %d  lastNextMCId page,cell %d,%d\n",
      rp->lastNextVdIndex, rp->lastNextMCId.page, rp->lastNextMCId.cell);
}

/******************************************************************************/
/* FTA_BS_XTNT_ZERO_MAP_V1  23 not used */
/* FTA_BS_XTNT_ZERO_MAP_V1_UNUSED  23 ifdef 0 */

/* from bs_extents.c */
typedef struct zeroUndoRec {
    uint16T type;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T mcellCnt;
    uint32T xCnt;
    bsXtntT xtntDesc;
} zeroUndoRecT;

static void
print_zeroUndoRec( char *recp, int recCnt)
{
    zeroUndoRecT record;
    zeroUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(zeroUndoRecT));

    if ( recCnt != sizeof(zeroUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(zeroUndoRecT));
    }

    printf("    type %d  vdIndex %d  mCId.page,cell %d,%d\n",
      rp->type, rp->vdIndex, rp->mCId.page, rp->mCId.cell);

    printf("    mcellCnt %d  xCnt %d  xtntDesc.bsPage,vdBlk %d,%d\n",
      rp->mcellCnt, rp->xCnt, rp->xtntDesc.bsPage, rp->xtntDesc.vdBlk);
}

/******************************************************************************/

/* from bs_shadow.c */
typedef struct {
    bfSetIdT setId;
    bfTagT tag;
    uint16T vdIndex;
    bfMCIdT mCId;
    bsXtntMapTypeT xtntMapType;
} bitfileTypeUndoRecT;

static void
print_bitfileTypeUndoRec( char *recp, int recCnt)
{
    bitfileTypeUndoRecT record;
    bitfileTypeUndoRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(bitfileTypeUndoRecT));

    if ( recCnt != sizeof(bitfileTypeUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(bitfileTypeUndoRecT));
    }

    printf("    setId %s\n", setid_to_str(rp->setId, buf));

    printf("    tag %s\n", tag_to_str(rp->tag, buf));

    printf("    vdIndex %d  mCId.page,cell %d,%d  xtntMapType %d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell, rp->xtntMapType);
}

/******************************************************************************/
/* FTA_BS_SBM_ALLOC_BITS_V1  33 */
/* FTA_BS_SBM_DEALLOC_BITS_V1  36 not used */

/* from bs_sbm.c */
typedef struct bitmapUndoRec {
    int type;                  /* ALLOC_BITS, DEALLOC_BITS */
    uint16T vdIndex;
    uint32T pageOffset;
    int startBit;
    int endBit;
} bitmapUndoRecT;

static void
print_bitmapUndoRec( char *recp, int recCnt)
{
    bitmapUndoRecT record;
    bitmapUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(bitmapUndoRecT));

    if ( recCnt != sizeof(bitmapUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(bitmapUndoRecT));
    }

    printf("    vdIndex %d  ", rp->vdIndex);
    printf("pageOffset %d  ", rp->pageOffset);
    printf("startBit %d  ", rp->startBit);
    printf("endBit %d\n", rp->endBit);
}

/******************************************************************************/
/* FTA_BS_STG_ADD_V1  34 */

/* from bs_stg.c */
typedef struct addStgUndoRec {
    bfSetIdT setId;
    bfTagT tag;
    uint32T nextPage;
    uint32T pageOffset;
    uint32T pageCnt;
} addStgUndoRecT;

static void
print_addStgUndoRec( char *recp, int recCnt)
{
    addStgUndoRecT record;
    addStgUndoRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(addStgUndoRecT));

    if ( recCnt != sizeof(addStgUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(addStgUndoRecT));
    }

    printf("    setId %s\n", setid_to_str(rp->setId, buf));
    printf("    tag %s\n", tag_to_str(rp->tag, buf));

    printf("    nextPage %d  ", rp->nextPage);
    printf("pageOffset %d  ", rp->pageOffset);
    printf("pageCnt %d\n", rp->pageCnt);
}

/******************************************************************************/
/* FTA_BS_STR_UPD_XTNT_REC_V1  39 */

/* from bs_stripe.c */
typedef struct xtntRecUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bsXtntMapTypeT type;
    uint32T segmentSize;
} xtntRecUndoRecT;

static void
print_xtntRecUndoRec( char *recp, int recCnt)
{
    xtntRecUndoRecT record;
    xtntRecUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(xtntRecUndoRecT));

    if ( recCnt != sizeof(xtntRecUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(xtntRecUndoRecT));
    }

    printf("    vdIndex %d  mCId page,cell %d,%d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell);

    printf("    type ");
    PRINT_xtnt_type(rp->type);
    printf("  ");
    printf("segmentSize %d\n", rp->segmentSize);
}

/******************************************************************************/
/* FTA_BS_TAG_WRITE_V1  40 */

/* from bs_tagdir.c */
typedef struct {
    bfTagT tag;
    bfSetIdT bfSetId;
    bsTMapT map;
} tagDataRecT;

static void
print_tagDataRec( char* recp, int recCnt)
{
    tagDataRecT record;
    tagDataRecT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(tagDataRecT));

    if ( recCnt != sizeof(tagDataRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(tagDataRecT));
    }

    printf("    tag %s\n", tag_to_str(rp->tag, buf));

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));

    printf("    map.tmSeqNo %x, map.tmVdIndex %d, map.tmBfMCId.page,cell %d,%d\n",
      rp->map.tmSeqNo, rp->map.tmVdIndex,
      rp->map.tmBfMCId.page, rp->map.tmBfMCId.cell);
}

/******************************************************************************/
/* FTA_BS_TAG_EXTEND_TAGDIR_V1  41 */

/* from bs_tagdir.c */
typedef struct {
    bfSetIdT bfSetId;
    uint32T unInitPage;
} tagUnInitPageRedoT;

static void
print_tagUnInitPageRedo( char* recp, int recCnt)
{
    tagUnInitPageRedoT record;
    tagUnInitPageRedoT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(tagUnInitPageRedoT));

    if ( recCnt != sizeof(tagUnInitPageRedoT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(tagUnInitPageRedoT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));

    printf("    unInitPage %d\n", rp->unInitPage);
}

/******************************************************************************/
/* FTA_FS_INSERT_V1  46 */

/* from fs_dir.h */
typedef struct {
    bfTagT dir_tag;
    bfTagT ins_tag;
    bfSetIdT bfSetId;
    uint32T page;
    uint32T byte;
} old_undo_headerT;

struct old_fs_insert_undo_rec {
    old_undo_headerT undo_header;
};

typedef struct old_fs_insert_undo_rec old_insert_undo_rec;

/* record was changed in fs_dir, rev 1.1.22.8 */
typedef struct {
    bfTagT dir_tag;
    bfTagT ins_tag;
    bfSetIdT bfSetId;
    uint32T page;
    uint32T byte;
    ulong old_size;
} new_undo_headerT;

struct new_fs_insert_undo_rec {
    new_undo_headerT undo_header;
};

typedef struct new_fs_insert_undo_rec new_insert_undo_rec;

static void
print_insert_undo_rec( char *recp, int recCnt)
{
    new_insert_undo_rec record;
    new_insert_undo_rec *rp = NULL;
    old_insert_undo_rec old_record;
    old_insert_undo_rec *old_rp = NULL;
    char buf[80];

    if ( recCnt == sizeof(new_insert_undo_rec) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(new_insert_undo_rec));
    } else if ( recCnt == sizeof(old_insert_undo_rec) ) {
        old_rp = &old_record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)old_rp, (void*)recp, sizeof(old_insert_undo_rec));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(new_insert_undo_rec));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(new_insert_undo_rec));
    }

    if ( rp ) {
        printf("    dir_tag %s  ", tag_to_str(rp->undo_header.dir_tag, buf));
        printf("ins_tag %s\n", tag_to_str(rp->undo_header.ins_tag, buf));
        printf("    bfSetId %s\n", setid_to_str(rp->undo_header.bfSetId, buf));
        printf("    page %d  byte %d  old_size %ld\n",
          rp->undo_header.page, rp->undo_header.byte, rp->undo_header.old_size);
    } else {
        printf("    dir_tag %s\n",tag_to_str(old_rp->undo_header.dir_tag, buf));
        printf("ins_tag %s\n", tag_to_str(old_rp->undo_header.ins_tag, buf));
        printf("    bfSetId %s\n",
          setid_to_str(old_rp->undo_header.bfSetId, buf));
        printf("    page %d  byte %d\n",
          old_rp->undo_header.page, old_rp->undo_header.byte);
    }
}

/******************************************************************************/
/* FTA_FS_DQUOT_SYNC_V1  49  not used */

/* from fs_quota.c */
typedef struct {
    bfSetIdT bfSetId;
    bfTagT tag;
    uint16T offset;
    uint32T page;
    int32T blkChng;
    int32T fileChng;
} quotaRecObsoleteT;

static void
print_quotaRecObsolete( char *recp, int recCnt)
{
    quotaRecObsoleteT record;
    quotaRecObsoleteT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(quotaRecObsoleteT));

    if ( recCnt != sizeof(quotaRecObsoleteT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(quotaRecObsoleteT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    tag %s\n", tag_to_str(rp->tag, buf));

    printf("    offset %d  page %d  blkChng %d  fileChng %d\n",
      rp->offset, rp->page, rp->blkChng, rp->fileChng);
}

/******************************************************************************/
/* FTA_TER_UPDATE_PTR_V1  52 */

/* from ter_ondisk_map.c */
typedef struct updatePtrUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mCId;
    vdIndexT tertiaryVdIndex;
    bfMCIdT tertiaryMCId;
} updatePtrUndoRecT;

static void
print_updatePtrUndoRec( char *recp, int recCnt)
{
    updatePtrUndoRecT record;
    updatePtrUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(updatePtrUndoRecT));

    if ( recCnt != sizeof(updatePtrUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(updatePtrUndoRecT));
    }

    printf("    vdIndex %d  mCId page,cell %d,%d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell);

    printf("    tertiaryVdIndex %d  tertiaryMCId page,cell %d,%d\n",
      rp->tertiaryVdIndex, rp->tertiaryMCId.page, rp->tertiaryMCId.cell);
}

/******************************************************************************/
/* FTA_TER_APPEND_TO_TER_V1  53 */
/* FTA_TER_APPEND_TO_XTRA_V1  54 */

/* from ter_ondisk_map.c */
typedef struct appendUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T xCnt;
} appendUndoRecT;

static void
print_appendUndoRec( char *recp, int recCnt)
{
    appendUndoRecT record;
    appendUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(appendUndoRecT));

    if ( recCnt != sizeof(appendUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(appendUndoRecT));
    }

    printf("    vdIndex %d  mCId page,cell %d,%d  xCnt %d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell, rp->xCnt);
}

/******************************************************************************/
/* FTA_TER_UPDATE_TOTAL_V1  55 */

/* from ter_ondisk_map.c */
typedef struct updateTotalUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T totalXtnts;
} updateTotalUndoRecT;

static void
print_updateTotalUndoRec( char *recp, int recCnt)
{
    updateTotalUndoRecT record;
    updateTotalUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(updateTotalUndoRecT));

    if ( recCnt != sizeof(updateTotalUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(updateTotalUndoRecT));
    }

    printf("    vdIndex %d  mCId page,cell %d,%d  totalXtnts %d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell, rp->totalXtnts);
}

/******************************************************************************/
/* FTA_TER_FLAGS_XTRA_V1  57 */
/* FTA_TER_FLAGS_TER_V1  56 */

#define PRINT_setclr_type(type) {          \
    if ( type == TER_CLR ) {               \
        printf("TER_CLR");                 \
    } else if ( type == TER_SET ) {        \
        printf("TER_SET");                 \
    } else {                               \
        printf("unknown type %d", type);   \
    }                                      \
}

/* from ter_ondisk_map.c */
typedef enum {
    TER_CLR,
    TER_SET
} terSetClrTypeT;

/* from ter_ondisk_map.c */
typedef struct flagsUndoRec {
    terSetClrTypeT type;
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T pageOffset;
    uint32T pageCnt;
    int32T flags;
} flagsUndoRecT;

static void
print_flagsUndoRec( char *recp, int recCnt)
{
    flagsUndoRecT record;
    flagsUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(flagsUndoRecT));

    if ( recCnt != sizeof(flagsUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(flagsUndoRecT));
    }

    printf("    type ");
    PRINT_setclr_type(rp->type);
    printf("  ");
    printf("vdIndex %d  mCId page,cell %d,%d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell);

    printf("    pageOffset %d  ", rp->pageOffset);
    printf("pageCnt %d  ", rp->pageCnt);
    /* FIX. decode flags */
    printf("flags 0x%x\n", rp->flags);
}

/******************************************************************************/
/* FTA_TER_SET_ACCESS_V1  63 */

#define PRINT_ter_xtnt_type(type) {                   \
    if ( type == BSR_TERTIARY_XTNTS ) {               \
        printf("BSR_TERTIARY_XTNTS");                 \
    } else if ( type == BSR_TERTIARY_XTRA_XTNTS ) {   \
        printf("BSR_TERTIARY_XTRA_XTNTS");            \
    } else {                                          \
        printf("unknown type %d", type);              \
    }                                                 \
}

/* from ter_ondisk_map.c */
typedef struct accessTimeEntry {
    uint32T index;
    uint32T accessTime;
} accessTimeEntryT;

/* from ter_ondisk_map.c */
typedef struct setAccessUndoRec {
    vdIndexT vdIndex;
    bfMCIdT mCId;
    uint32T type;
    uint32T cnt;
    accessTimeEntryT accessTime[BMT_TERTIARY_XTRA_XTNTS];
} setAccessUndoRecT;

static void
print_setAccessUndoRec( char *recp, int recCnt)
{
    setAccessUndoRecT record;
    setAccessUndoRecT *rp = &record;
    int i;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(setAccessUndoRecT));

    if ( recCnt != sizeof(setAccessUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(setAccessUndoRecT));
    }

    printf("    vdIndex %d  mCId page,cell %d,%d\n",
      rp->vdIndex, rp->mCId.page, rp->mCId.cell);

    printf("    type ");
    PRINT_ter_xtnt_type(rp->type);
    printf("  ");
    printf("cnt %d\n", rp->cnt);

    for ( i = 0; i < BMT_TERTIARY_XTRA_XTNTS; i++ ) {
        printf("    accessTime[%d] index %d accessTime %x\n", i,
          rp->accessTime[i].index, rp->accessTime[i].accessTime);
    }
}

/******************************************************************************/
/* FTA_BS_BMT_EXTEND_V1  77 */

static void
print_vdIndex( char *recp, int recCnt)
{
    vdIndexT record;
    vdIndexT *vdIndexp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)vdIndexp, (void*)recp, sizeof(vdIndexT));

    if ( recCnt != sizeof(vdIndexT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(vdIndexT));
    }

    printf("    vd index %d\n", *vdIndexp);
}

/******************************************************************************/
/* FTA_BS_DEL_ADD_LIST_V1  80 */
/* FTA_BS_DEL_REM_LIST_V1  81 */
/* FTA_BS_DEL_REM_LIST_V1  81 */

/* from bs_delete.c */
#define DEL_ADD 0
#define DEL_REMOVE 1

/* from bs_delete.c */
typedef struct {
    bfMCIdT mcid;
    uint32T vdIndex;
    uint32T type;
} delListRecT;

static void
print_delListRec( char *recp, int recCnt)
{
    delListRecT record;
    delListRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(delListRecT));

    if ( recCnt != sizeof(delListRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(delListRecT));
    }

    printf("    vdIndex %d  mcid page,cell %d,%d  ",
      rp->vdIndex, rp->mcid.page, rp->mcid.cell);
    if ( rp->type == DEL_ADD ) {
        printf("type DEL_ADD\n");
    } else if ( rp->type == DEL_REMOVE ) {
        printf("type DEL_REMOVE\n");
    } else {
        printf("unknown type %d\n", rp->type);
    }
}


/******************************************************************************/
/* FTA_BS_SWITCH_ROOT_TAGDIR_V1  126 */

static void
print_bfTag( char *recp, int recCnt)
{
    bfTagT record;
    bfTagT *bfTagp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)bfTagp, (void*)recp, sizeof(bfTagT));

    if ( recCnt != sizeof(bfTagT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(bfTagT));
    }

    printf("    new tag %s\n", tag_to_str(*bfTagp, buf));
}

/******************************************************************************/
/* FTA_FS_DQUOT_SYNC_V2  139 */
/* FTA_FS_DQUOT_SYNC_V2  139 */

#define USRQUOTA 0
#define GRPQUOTA 1
#define PRINT_quota_type(type) {         \
    if ( type == USRQUOTA ) {            \
        printf("USRQUOTA");              \
    } else if ( type == GRPQUOTA ) {     \
        printf("GRPQUOTA");              \
    } else {                             \
        printf("unknown type %d", type); \
    }                                    \
}

/* from fs_quota.c */
typedef struct {
    vm_offset_t fsp;
    bfSetIdT bfSetId;
    uint32T id;
    bfTagT tag;
    uint16T offset;
    uint16T type;
    uint32T page;
    int32T blkChng;
    int32T fileChng;
} old_quotaRecT;

/* quotaRecT was changed. */
typedef struct {
    uint64T unused;
    bfSetIdT bfSetId;
    uint32T id;
    bfTagT tag;
    uint16T offset;
    uint16T type;
    uint32T page;
    int32T blkChng;
    int32T fileChng;
    uint64T old_quota_file_size;
} quotaRecT;

static void
print_quotaRec( char *recp, int recCnt)
{
    old_quotaRecT old_record;
    old_quotaRecT *old_rp = NULL;
    quotaRecT record;
    quotaRecT *rp = NULL;
    vm_offset_t fspx;
    uint16T offsetx;
    uint16T typex;
    char buf[80];

    if ( recCnt == sizeof(quotaRecT) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(quotaRecT));
    } else if ( recCnt == sizeof(old_quotaRecT) ) {
        old_rp = &old_record;
        memcpy((void*)old_rp, (void*)recp, sizeof(old_quotaRecT));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(quotaRecT));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(quotaRecT));
    }

    if ( rp ) {
        printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
        printf("    tag %s  ", tag_to_str(rp->tag, buf));
        printf("type ");
        PRINT_quota_type(rp->type);
        printf("id %d  ", rp->id);
        printf("page %d  ", rp->page);
        printf("offset %d\n", rp->offset);
        printf("    blkChng %d  ", rp->blkChng);
        printf("fileChng %d  ", rp->fileChng);
        printf("old_quota_file_size %ld\n", rp->old_quota_file_size);
    } else {
        printf("    bfSetId %s\n", setid_to_str(old_rp->bfSetId, buf));
        printf("    tag %s  ", tag_to_str(old_rp->tag, buf));
        printf("type ");
        PRINT_quota_type(old_rp->type);
        printf("id %d  ", old_rp->id);
        printf("page %d  ", old_rp->page);
        printf("offset %d  ", old_rp->offset);
        printf("    fsp %lx  ", old_rp->fsp);
        printf("blkChng %d  ", old_rp->blkChng);
        printf("fileChng %d\n", old_rp->fileChng);
    }
}

/******************************************************************************/
/* FTA_MSFS_SETPROPLIST  140 */
/* FTA_MSFS_DELPROPLIST  141 */
/* FTA_MSFS_ALLOC_MCELL  146 */

/* from msfs_proplist.c */
typedef struct bsOdRecCur {
    vdIndexT VD;
    bfMCIdT  MC;
    uint32T  pgoff;
    vdIndexT prevVD;
    bfMCIdT  prevMC;
} bsOdRecCurT;

/* from msfs_proplist.c */
typedef struct delPropRootDone {
    bsOdRecCurT hdr;
    bsOdRecCurT end;
    ftxLkT *mcellList_lk;
    int cellsize;
} delPropRootDoneT;

static void
print_delPropRootDone( char *recp, int recCnt)
{
    delPropRootDoneT record;
    delPropRootDoneT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(delPropRootDoneT));

    if ( recCnt != sizeof(delPropRootDoneT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(delPropRootDoneT));
    }

/* FIX multiple records */
    printf("    hdr.VD %d  hdr.MC.page,cell %d,%d\n",
      rp->hdr.VD, rp->hdr.MC.page, rp->hdr.MC.cell);

    printf("    hdr.pgoff %d\n", rp->hdr.pgoff);

    printf("    hdr.prevVD %d  hdr.prevMC.page,cell %d,%d\n",
      rp->hdr.prevVD, rp->hdr.prevMC.page, rp->hdr.prevMC.cell);

    printf("    end.VD %d  end.MC.page,cell %d,%d\n",
      rp->end.VD, rp->end.MC.page, rp->end.MC.cell);

    printf("    end.pgoff %d\n", rp->end.pgoff);

    printf("    end.prevVD %d  end.prevMC.page,cell %d,%d\n",
      rp->end.prevVD, rp->end.prevMC.page, rp->end.prevMC.cell);

    printf("    mcellList_lk %lx  cellsize %d\n",
      rp->mcellList_lk, rp->cellsize);
}

/******************************************************************************/
/* FTA_IDX_UNDO_V1  149 */

/* from bs_index.h */
typedef struct idxNodeEntry {
    ulong search_key;
    union {
        ulong dir_offset;
        ulong node_page;
        ulong free_size;
        ulong any_field;
    }loc;
}idxNodeEntryT;

/* from bs_index.h */
typedef enum {
    IDX_INSERT_FILENAME_UNDO,
    IDX_REMOVE_FILENAME_UNDO,
    IDX_DIRECTORY_INSERT_SPACE_UNDO,
    IDX_DIRECTORY_GET_SPACE_UNDO,
    IDX_SETUP_FOR_TRUNCATION_INT_UNDO,
    IDX_CREATE_UNDO
} idxUndoActionT;

#define PRINTidxUndoAction(action)                    \
    switch( action ) {                                \
      case IDX_INSERT_FILENAME_UNDO:                  \
        printf("IDX_INSERT_FILENAME_UNDO");           \
        break;                                        \
      case IDX_REMOVE_FILENAME_UNDO:                  \
        printf("IDX_REMOVE_FILENAME_UNDO");           \
        break;                                        \
      case IDX_DIRECTORY_INSERT_SPACE_UNDO:           \
        printf("IDX_DIRECTORY_INSERT_SPACE_UNDO");    \
        break;                                        \
      case IDX_DIRECTORY_GET_SPACE_UNDO:              \
        printf("IDX_DIRECTORY_GET_SPACE_UNDO");       \
        break;                                        \
      case IDX_SETUP_FOR_TRUNCATION_INT_UNDO:         \
        printf("IDX_SETUP_FOR_TRUNCATION_INT_UNDO");  \
        break;                                        \
      case IDX_CREATE_UNDO:                           \
        printf("IDX_CREATE_UNDO");                    \
        break;                                        \
      default:                                        \
        printf("unknown action %d", action);          \
        break;                                        \
    }

typedef struct idxUndoRecord {      /* from bs_index.h */
    idxNodeEntryT nodeEntry;
    bfSetIdT bfSetId;
    bfTagT dir_tag;
    idxUndoActionT action;
} idxUndoRecordT;

static void
print_idxUndoRecord( char *recp, int recCnt)
{
    idxUndoRecordT record;
    idxUndoRecordT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(idxUndoRecordT));

    if ( recCnt != sizeof(idxUndoRecordT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(idxUndoRecordT));
    }

    printf("    nodeEntry.search_key %lu  nodeEntry.loc %lu\n",
      rp->nodeEntry.search_key, rp->nodeEntry.loc.dir_offset);

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    dir_tag %s  ", tag_to_str(rp->dir_tag, buf));
    printf("action ");
    PRINTidxUndoAction(rp->action);
    printf("\n");
}
