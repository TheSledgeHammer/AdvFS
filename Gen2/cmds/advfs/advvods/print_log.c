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

#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <assert.h>

#include <advfs/ms_public.h>
#include <advfs/ms_privates.h>
#include <advfs/ftx_privates.h>
#include <advfs/advfs_acl.h>
#include <advfs/bs_index.h>

#include "vods.h"

/* private prototypes */

static void print_logrec_hdr( logRecHdrT*, int, int);
static void print_logrec_data( ftxDoneLRT*, uint32_t, int);
static void print_contOrUndo( ftxAgentIdT, char*, int);
static void print_rootdone( ftxAgentIdT, char*, int);
static void print_redo( ftxAgentIdT, char*, int);
static void print_imageRedo( char**, int);
static char* tag_to_str( bfTagT, char*);
static char* setid_to_str( bfSetIdT, char*);
static void print_unlinkSnapUndoRec( char*, int);
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
static void print_addStgUndoRec( char*, int);
static void print_xtntRecUndoRec( char*, int);
static void print_tagDataRec( char*, int);
static void print_tagUnInitPageRedo( char*, int);
static void print_insert_undo_rec( char*, int);
static void print_delListRec( char*, int);
static void print_quotaRec( char*, int);
static void print_delPropRootDone( char*, int);
static void print_idxUndoRecord( char*, int);
static void print_aclUndoRecord(char *recp, int recCnt);

/* this array is indexed using the agent ID from records found in the
   log.  The names match the contant definitions in ftx_agent.h.  They
   need to be kept up to date with those definitions.  */

char *ftx_agents[] = {            /* NU = not used */ /* registered routines */
    "FTA_NULL",                        /*  0 */

                   /**** Bitfile sets or snapshot agents ****/
    "FTA_BS_BFS_CREATE_V1",            /*  1 */
    "FTA_BS_BFS_UNLINK_SNAP_V1",       /*  2 */
    "FTA_BS_BFS_DEL_LIST_REMOVE_V1",   /*  3 */
    "FTA_BS_BFS_SNAP_V1",              /*  4 */

                          /**** Bmt operations ****/
    "FTA_BS_BMT_PUT_REC_V1",           /*  5 */
    "FTA_BS_BMT_FREE_MCELL_V1",        /*  6 */
    "FTA_BS_BMT_ALLOC_MCELL_V1",       /*  7 */
    "FTA_BS_BMT_ALLOC_LINK_MCELL_V1",  /*  8 */
    "FTA_BS_BMT_LINK_MCELLS_V1",       /*  9 */
    "FTA_BS_BMT_UNLINK_MCELLS_V1",      /* 10 */

                         /**** bitfile creation ****/
    "FTA_BS_BF_CREATE_V1",              /* 11 */
    "FTA_FREE_SLOT_12   ",              /* 12 */     /* free slot */

                          /**** bitfile deletion */
    "FTA_BS_DEL_DELETE_V1",             /* 13 */
    "FTA_BS_DEL_DEFERRED_V1",           /* 14 */
    "FTA_BS_BMT_FREE_BF_MCELLS_V1",     /* 15 */

                      /**** extent map operations ****/
    "FTA_BS_XTNT_UPD_MCELL_CNT_V1",     /* 16 */
    "FTA_BS_XTNT_UPD_REC_V1",           /* 17 */
    "FTA_BS_XTNT_CRE_REC",              /* 18 */
    "FTA_BS_XTNT_CRE_XTRA_REC_V1",      /* 19 */
    "FTA_BS_XTNT_CRE_SHADOW_REC_V1",    /* 20 */
    "FTA_BS_XTNT_INSERT_CHAIN_V1",      /* 21 */
    "FTA_BS_XTNT_REMOVE_CHAIN_V1",      /* 22 */
    "FTA_BS_XTNT_ZERO_MAP_V1_UNUSED",   /* 23 */ /* unused */

                        /*** migrate operations ****/
    "FTA_BS_MIG_MIGRATE_V1",            /* 24 */
    "FTA_BS_MIG_ALLOC_V1",              /* 25 */
    "FTA_BS_MIG_CNT_V1",                /* 26 */
    "FTA_BS_MIG_INSERT_HEAD_V1",        /* 27 */
    "FTA_BS_MIG_REMOVE_HEAD_V1",        /* 28 */
    "FTA_BS_MIG_REMOVE_MIDDLE_V1",      /* 29 */
    "FTA_BS_MIG_ALLOC_LINK_V1",         /* 30 */

                        /**** shadow operations ****/
    "FTA_BS_SHA_SHADOW_V1",             /* 31 */
    "FTA_BS_SHA_BITFILE_TYPE_V1",       /* 32 */

                          /**** storage bitmap ****/
    "FTA_BS_SBM_ALLOC_BITS_V1",         /* 33 */

                        /**** storage management ****/
    "FTA_BS_STG_ADD_V1",                /* 34 */
    "FTA_BS_STG_REMOVE_V1",             /* 35 */
    "FTA_BS_SBM_DEALLOC_BITS_V1",       /* 36 */
    "FTA_BS_STG_COPY_V1",               /* 37 */

                        /**** striped bitfile management ****/
    "FTA_BS_STR_STRIPE_V1",             /* 38 */
    "FTA_BS_STR_UPD_XTNT_REC_V1",       /* 39 */

                          /**** tag directory ****/
    "FTA_BS_TAG_WRITE_V1",              /* 40 */
    "FTA_BS_TAG_EXTEND_TAGDIR_V1",      /* 41 */
    "FTA_BS_TAG_PATCH_FREELIST_V1",     /* 42 */

                           /**** file system ****/
    "FTA_FS_REMOVE_V1",                 /* 43 */
    "FTA_FS_UPDATE_V1",                 /* 44 */
    "FTA_FS_CREATE_V1 ",                /* 45 */
    "FTA_FS_INSERT_V1",                 /* 46 */
    "FTA_FS_LINK_V1",                   /* 47 */
    "FTA_FS_RENAME_V1",                 /* 48 */

                              /**** quotas ****/
    "FTA_FS_DQUOT_SYNC_V1",             /* 49 */

    "FTA_TER_CRE_TER_NULL",             /* 50 */
    "FTA_TER_CRE_XTRA_NULL",            /* 51 */
    "FTA_TER_UPDATE_PTR_NULL",          /* 52 */
    "FTA_TER_APPEND_TO_TER_NULL",       /* 53 */
    "FTA_TER_APPEND_TO_XTRA_NULL",      /* 54 */
    "FTA_TER_UPDATE_TOTAL_NULL",        /* 55 */
    "FTA_TER_FLAGS_TER_NULL",           /* 56 */
    "FTA_TER_FLAGS_XTRA_NULL",          /* 57 */
    "FTA_TER_SET_MEDIA_NULL",           /* 58 */
    "FTA_TER_SET_CLEAN_NULL",           /* 59 */
    "FTA_TER_ADD_STG_NULL",             /* 60 */
    "FTA_TER_ZERO_XTNT_MAP_NULL",       /* 61 */
    "FTA_TER_TRUNC_MAP_NULL",           /* 62 */
    "FTA_TER_SET_ACCESS_NULL",          /* 63 */
    "FTA_TER_SET_DIRTY_NULL",           /* 64 */

    "FTA_BS_CLOSE_V1",                  /* 65 */
    "FTA_BS_XTNT_REWRITE_MAP_V1",       /* 66 */
    "FTA_BS_STG_ALLOC_MCELL_V1",        /* 67 */
    "FTA_BS_COW_PG",                    /* 68 */
    "FTA_BS_COW",                       /* 69 */
    "FTA_BFS_CREATE",                   /* 70 */
    "FTA_BFS_DEL_PENDING_ADD",          /* 71 */
    "FTA_BFS_DELETE",                   /* 72 */
    "FTA_BFS_DELETE_SNAP",              /* 73 */
    "FTA_BFS_SNAP",                     /* 74 */
    "FTA_BS_RBMT_ALLOC_MCELL_V1",       /* 75 */
    "FTA_BS_BMT_UPDATE_REC_V1",         /* 76 */
    "FTA_BS_BMT_EXTEND_V1",             /* 77 */
    "FTA_BS_RBMT_EXTEND_V1",            /* 78 */
    "FTA_FREE_SLOT_79",                 /* 79 */     /* free slot */
    "FTA_BS_DEL_ADD_LIST_V1",           /* 80 */
    "FTA_BS_DEL_REM_LIST_V1",           /* 81 */
    "FTA_BS_DEL_FREE_PRIMARY_MCELL_V1 ",/* 82 */
    "FTA_BS_DEL_FREE_MCELL_CHAIN_V1",   /* 83 */
    "FTA_BS_DEL_FTX_START_V1",          /* 84 */
    "FTA_BS_DELETE_6",                  /* 85 */     /* temp used by rmvol bug fix */
    "FTA_BS_XTNT_RELOAD_ORIG_XTNTMAP",  /* 86 */
    "FTA_SET_BFSET_FLAG",               /* 87 */     
    "FTA_PUNCH_HOLE",                   /* 88 */     
    "FTA_BS_TAG_SWITCH_ROOT_TAGDIR_V1", /* 89 */
    "FTA_BS_SET_NEXT_TAG_V1",           /* 90 */
    "FTA_FS_CREATE_1",                  /* 91 */
    "FTA_FS_CREATE_2",                  /* 92 */
    "FTA_FS_DIR_INIT_1",                /* 93 */
    "FTA_FS_DIR_LOOKUP_1",              /* 94 */
    "FTA_FS_DIR_LOOKUP_2",              /* 95 */
    "FTA_FS_FILE_SETS_1",               /* 96 */
    "FTA_FS_INIT_QUOTAS_V1",            /* 97 */
    "FTA_FS_WRITE_ADD_STG_V1",          /* 98 */
    "FTA_FS_WRITE_V1",                  /* 99 */
    "FTA_OSF_SETATTR_1",                /* 100 */
    "FTA_OSF_FSYNC_V1",                 /* 101 */
    "FTA_OSF_REMOVE_V1",                /* 102 */
    "FTA_OSF_LINK_V1",                  /* 103 */
    "FTA_OSF_RENAME_V1",                /* 104 */
    "FTA_OSF_RMDIR_V1",                 /* 105 */
    "FTA_FTX_LOG_DATA",                 /* 106 */
    "FTA_FS_SYSCALLS_1",                /* 107 */
    "FTA_FS_SYSCALLS_2",                /* 108 */
    "FTA_MSS_COMMON_1",                 /* 109 */
    "FTA_BS_MIG_MOVE_METADATA_V1",      /* 110 */
    "FTA_LGR_SWITCH_VOL",               /* 111 */
    "FTA_BS_XTNT_INSERT_CHAIN_LOCK_V1", /* 112 */
    "FTA_BS_XTNT_REMOVE_CHAIN_LOCK_V1", /* 113 */
    "FTA_BS_STR_UPD_XTNT_REC_LOCK_V1",  /* 114 */
    "FTA_FREE_SLOT_115",                /* 115 */
    "FTA_FTX_SWITCH_LOG",               /* 116 */
    "FTA_FTX_CHECKPOINT_LOG",           /* 117 */
    "FTA_BS_BMT_CREATE_REC",            /* 118 */
    "FTA_FS_QUOTA_OFF_V1",              /* 119 */
    "FTA_BS_STG_SET_ALLOC_DISK_V1",     /* 120 */
    "FTA_BS_SET_BF_PARAMS",             /* 121 */
    "FTA_BS_SET_BF_IPARAMS",            /* 122 */
    "FTA_BS_SET_BFSET_PARAMS",          /* 123 */
    "FTA_BS_MIG_MOVE_METADATA_EXC_V1",  /* 124 */
    "FTA_FS_CREATE_ROOT_FILE",          /* 125 */
    "FTA_BS_SWITCH_ROOT_TAGDIR_V1",     /* 126 */
    "FTA_FS_GET_QUOTA_V1",              /* 127 */
    "FTA_FS_SET_QUOTA_V1",              /* 128 */
    "FTA_FS_SET_USE_V1",                /* 129 */
    "FTA_FS_QUOTA_ON_V1",               /* 130 */
    "FTA_FS_DETACH_QUOTA_V1",           /* 131 */
    "FTA_FS_ATTACH_QUOTA_V1",           /* 132 */
    "FTA_FS_GET_QUOTA",                 /* 133 */
    "FTA_FREE_SLOT_134",                /* 134 */
    "FTA_FREE_SLOT_135",                /* 135 */
    "FTA_BFS_CREATE_2",                 /* 136 */
    "FTA_OSF_SETATTR_2",                /* 137 */
    "FTA_OSF_SYNCDATA_V1",              /* 138 */
    "FTA_FS_DQUOT_SYNC_V2",             /* 139 */
    "FTA_FREE_SLOT_140",                /* 140 */
    "FTA_FREE_SLOT_141",                /* 141 */
    "FTA_FREE_SLOT_142",                /* 142 */
    "FTA_FREE_SLOT_143",                /* 143 */
    "FTA_FREE_SLOT_144",                /* 144 */
    "FTA_BS_SET_VD_PARAMS",	       /* 145 */
    "FTA_ADVFS_ALLOC_MCELL",	       /* 146 */
    "FTA_BS_BMT_DEFERRED_MCELL_FREE",   /* 147 */
    "FTA_DATA_LOGGING_WRITE",           /* 148 */
    "FTA_TAG_TO_FREELIST",              /* 149 */
    "FTA_FS_WRITE_TRUNC",               /* 150 */
    "FTA_IDX_UNDO_V1",                  /* 151 */
    "FTA_BS_DEL_TRUNCATE_DDL",          /* 152 */
    "FTA_BS_FREEZE",                    /* 153 */
    "FTA_BS_VD_EXTEND",                 /* 154 */
    "FTA_SS_DMN_DATA_WRITE",            /* 155 */
				/**** AdvFS ACLS ****/
    "FTA_ACL_UPDATE",                   /* 156 */
    "FTA_ACL_UPDATE_WITHOUT_UNDO",      /* 157 */
    "FTA_ACL_UPDATE_WITH_UNDO",         /* 158 */
    /* Snapshot transaction */
    "FTA_SNAP_OUT_OF_SYNC",             /* 159 */
    "FTA_SNAP_MCELL_V1",                /* 160 */
    "FTA_SNAP_RECS_V1",                 /* 161 */
    "FTA_SNAP_NEW_MCELL",               /* 162 */
    "FTA_META_SNAP",                    /* 163 */
    "FTA_GETPAGE_COW",                  /* 164 */

    "FTA_ADVFS_THRESHOLD_EVENT",        /* 165 */

    "FTX_MAX_AGENTS"                    /* 166 */
};


/*****************************************************************************/
#define PRINT_RECADDR(addr, addr_name)                   \
    printf("%s: page,offset,lsn (%d,%d,%d)\n",          \
      addr_name, (addr).page, (addr).offset, (addr).lsn.num)

#define PRINTpgType(buf, type)                   \
    switch ( type ) {                            \
      case BFM_SBM:                              \
        sprintf(buf, "BFM_SBM");                 \
        break;                                   \
      case BFM_BFSDIR:                           \
        sprintf(buf, "BFM_BFSDIR");              \
        break;                                   \
      case BFM_FTXLOG:                           \
        sprintf(buf, "BFM_FTXLOG");              \
        break;                                   \
      case BFM_MISC:                             \
        sprintf(buf, "BFM_MISC");                \
        break;                                   \
      default:                                   \
        sprintf(buf, "unknown type %d", type);   \
        break;                                   \
    }


void
print_loghdr( FILE *fp, logPgHdrT *pdata, int lbn, int flags)
{
    char line[80];
    int line_len;
    char time_buf[64];
    char buf[80];

    if (ctime_r(&pdata->fsCookie, time_buf) == NULL) {
        strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    sprintf(line, "fsCookie 0x%lx.%lx (%s)  ",
      pdata->fsCookie.id_sec, pdata->fsCookie.id_usec, time_buf);
    printf("%s", line);

    line_len = strlen(line);
    PRINTpgType(buf, pdata->pgType);
    sprintf(line, "pgType %s (%d)\n", buf, pdata->pgType);
    line_len += strlen(line);
    while ( line_len++ < 75 ) {
        printf(" ");
    }
    printf("%s", line);

    if ( flags & VFLG ) {
	printf("magicNumber 0x%x  ", pdata->magicNumber);
    }
    printf("pgSafe %d  ", pdata->pgSafe);
    printf("chkBit %d\n", pdata->chkBit);
    printf("curLastRec %-4d ", pdata->curLastRec);
    printf("prevLastRec %-4d ", pdata->prevLastRec);
    printf("thisPageLSN%10d\n", pdata->thisPageLSN.num);

    PRINT_RECADDR(pdata->firstLogRec, "1st log record");

}

/*****************************************************************************/
/* from ftx_recovery.c */
typedef struct {
    ftxRecRedoT pgdesc;         /* page descriptor */
    ftxRecXT recX[FTX_MX_PINR]; /* record extent list */
} pageredoT;

/* Each log page contains a series of records.  Each record is
   composed of a header (logRecHdrT), followed by a "transaction done"
   record (ftxDoneLRT), which is optionally followed by data specific
   to each log record (cont/undo, root done, or redo).  This routine
   manages the printing of the data for one record. */  

void
print_logrec( logRecHdrT *pdata, int offset, int flags)
{
    ftxDoneLRT *doneLRTp;
    uint32_t wordCnt;
    logRecHdrT *recHdr;
    size_t size = 0;

    /* The input value "*pdata" is a pointer to the start of a log
       record header, which is followed by data records.  Because
       records are packed into the log page on 32 bit address
       boundaries, the pdata pointer may not be 64-bit aligned.  If
       it's not and we use it as a structure pointer, we bus error
       crash. So we malloc memory (64 bit aligned by malloc) and byte
       copy the entire record there to assure that we reference the
       passed-in data as a 64 bit aligned structure. */

    size = pdata->wordCnt * sizeof(uint32_t);
    recHdr = (logRecHdrT *) malloc(size);
    memcpy(recHdr, pdata, size);

    if ( flags & TTFLG ) {
        doneLRTp = (ftxDoneLRT*)((char *)recHdr + REC_HDR_LEN);
        if ( (recHdr->segment & ~MORE_SEGS) == 0 ) {
            printf("OFF %-4d ", offset);
            printf("tree Id 0x%lx ", doneLRTp->ftxId);
            if ( doneLRTp->fdl_agentId >= 0 &&
                 doneLRTp->fdl_agentId < sizeof(ftx_agents) / sizeof(char *) )
            {
                printf("%s ", ftx_agents[doneLRTp->fdl_agentId]);
            } else {
                printf("agent Id %d ", doneLRTp->fdl_agentId);
            }
            printf("level %d ", doneLRTp->level);
            printf("member %d ", doneLRTp->member);
            printf("next %d %d\n", 
		   recHdr->nextRec.page, recHdr->nextRec.offset);
        }
	free(recHdr);
        return;
    }

    print_logrec_hdr(recHdr, offset, flags);
    doneLRTp = (ftxDoneLRT*)((char *)recHdr + REC_HDR_LEN);
    wordCnt = recHdr->wordCnt;
    if ( (offset + recHdr->wordCnt) > DATA_WORDS_PG ) {
        printf("ftx record goes beyond the end of the page!!\n");
        wordCnt = DATA_WORDS_PG - offset;
    }

    if ( (recHdr->segment & ~MORE_SEGS) == 0 ) {
        print_logrec_data(doneLRTp, wordCnt, flags);
    } else {
        if ( flags & VFLG ) {
            print_unknown((char*)doneLRTp, wordCnt * sizeof(uint32_t));
        }
    }
    free(recHdr);
}

/*****************************************************************************/
static void
print_logrec_hdr( logRecHdrT *pdata, int offset, int flags)
{
    if ( (pdata->segment & MORE_SEGS) == 0 ) {
        printf("RECORD OFFSET %d  wordCnt %d  clientWordCnt %d  segment %d  ",
            offset, pdata->wordCnt, pdata->clientWordCnt, pdata->segment);
    } else {
        printf("OFFSET %d wordCnt %d clientWordCnt %d segment %d,MORE_SEGS ",
            offset, pdata->wordCnt, pdata->clientWordCnt,
            pdata->segment & ~MORE_SEGS);
    }

    printf("lsn %d\n", pdata->lsn.num);
    PRINT_RECADDR(pdata->nextRec, "      nextRec");
    PRINT_RECADDR(pdata->prevRec, "      prevRec");
    PRINT_RECADDR(pdata->prevClientRec, "prevClientRec");
    PRINT_RECADDR(pdata->firstSeg, "     firstSeg");

    printf("\n");
}

/*****************************************************************************/

/* The log records may contain "client data", which is formatted
   uniquely for each record type.  The client data can be of type
   "continue or undo", "root done", or "redo".  There may be some data
   of each type, as indicated by the separate byte counts in the
   ftxDoneLRT for each type.  The data is packed into the space
   following the ftxDoneLRT record, with any cont/undo data, followed
   by and root done data, followed by any redo data. In addition, this
   client data may be followed by another data type that's neither
   cont/undo, root done, or redo, and we call that image data.  That
   data is printed simply as a series of hex words. */

static void
print_logrec_data( ftxDoneLRT *doneLRTp, uint32_t wordCnt, int flags)
{
    char *undoRecp, *rtdnRecp, *redoRecp, *redoop, *eor;
    uint undocnt = 0;   /* byte count for cont or undo data */
    uint rtdncnt = 0;   /* byte count for root done data */
    uint redocnt = 0;   /* byte count for redo data */
    char time_buf[64];

    /* print the info from the ftxDoneLRT record */

    eor = (char *)doneLRTp + ((wordCnt - REC_HDR_WORDS) * sizeof(uint32_t));
    if ( doneLRTp->fdl_agentId >= 0 &&
         doneLRTp->fdl_agentId < sizeof(ftx_agents) / sizeof(char *) )
    {
        printf("agent Id %s (%d)\n",
          ftx_agents[doneLRTp->fdl_agentId], doneLRTp->fdl_agentId);
    } else {
        printf("agent Id %d\n", doneLRTp->fdl_agentId);
    }

    printf("rec type %s, ", doneLRTp->type == ftxNilLR ?
                                       "ftxNilLR" :
                                       doneLRTp->type == ftxDoneLR ?
                                                      "ftxDoneLR" :
                                                      "<<unknown>>");
    printf("level %d, ", doneLRTp->level);
    printf("atomic recovery pass #%d, ", doneLRTp->atomicRPass);
    printf("member %d\n", doneLRTp->member);
    printf("tree Id %x, ", doneLRTp->ftxId);

    if (ctime_r(&doneLRTp->bfDmnId, time_buf) == NULL) {
        strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    printf("Domain ID 0x%lx.%lx  (%s)\n",
	   doneLRTp->bfDmnId.id_sec, doneLRTp->bfDmnId.id_usec, time_buf);

    PRINT_RECADDR(doneLRTp->crashRedo, "crashRedo");
    printf("contOrUndoRBcnt %d, rootDRBcnt %d, opRedoRBcnt %d\n",
      doneLRTp->contOrUndoRBcnt, doneLRTp->rootDRBcnt, doneLRTp->opRedoRBcnt);

    if ( flags & VFLG ) {
        undoRecp = (char *)doneLRTp + sizeof(ftxDoneLRT);

	/* print cont or undo data, if any */

        if ( doneLRTp->contOrUndoRBcnt ) {
            switch ( doneLRTp->fdl_agentId ) {
              case FTA_BS_BMT_FREE_BF_MCELLS_V1:
              case FTA_ADVFS_ALLOC_MCELL:
                printf("  continuation record:\n");
                break;
              default:
                printf("  undo record:\n");
                break;
            }
            if ( doneLRTp->contOrUndoRBcnt > wordCnt * sizeof(uint32_t) ) {
                undocnt = wordCnt * sizeof(uint32_t);
                printf("contOrUndoRBcnt is too large (%d). ",
                  doneLRTp->contOrUndoRBcnt);
                printf("It is being reduced to %d\n", undocnt);
            } else {
                undocnt = doneLRTp->contOrUndoRBcnt;
            }

            if (undocnt != 0) {
		print_contOrUndo(doneLRTp->fdl_agentId, undoRecp, undocnt);
		undocnt = roundup(undocnt, sizeof(uint32_t));
	    }
	}

	/* print root done data, if any */

        rtdnRecp = undoRecp + undocnt;
        if ( doneLRTp->rootDRBcnt ) {
            if ( doneLRTp->rootDRBcnt + undocnt > wordCnt * sizeof(uint32_t) ) {
                assert(wordCnt * sizeof(uint32_t) >= undocnt);
                rtdncnt = wordCnt * sizeof(uint32_t) - undocnt;
                printf("rootDRBcnt is too large (%d). ", doneLRTp->rootDRBcnt);
                printf("It is being reduced to %d\n", rtdncnt);
            } else {
                rtdncnt = doneLRTp->rootDRBcnt;
            }
            if ( rtdncnt != 0 ) {
                printf("  root done record:\n");
                print_rootdone(doneLRTp->fdl_agentId, rtdnRecp, rtdncnt);
            }
            rtdncnt = roundup(rtdncnt, sizeof(uint32_t));
        }

	/* print redo data, if any */

        redoRecp = rtdnRecp + rtdncnt;
        if ( doneLRTp->opRedoRBcnt ) {
            if ( doneLRTp->opRedoRBcnt + rtdncnt + undocnt >
                 wordCnt * sizeof(uint32_t) )
            {
                assert(wordCnt * sizeof(uint32_t) >= undocnt + rtdncnt);
                redocnt = wordCnt * sizeof(uint32_t) - undocnt - rtdncnt;
                printf("opRedoRBcnt is too large (%d). ",doneLRTp->opRedoRBcnt);
                printf("It is being reduced to %d\n", redocnt);
            } else {
                redocnt = doneLRTp->opRedoRBcnt;
            }
            if ( redocnt != 0 ) {
                printf("  redo record:\n");
                print_redo(doneLRTp->fdl_agentId, redoRecp, redocnt);
            }
            redocnt = roundup(redocnt, sizeof(uint32_t));
        }

	/* if there is any data other than cont/undo, root done, or
	   redo, print that as "image" data, a series of hex words. */ 

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

/*****************************************************************************/
#define FTA_BS_RBMT_ALLOC_MCELL_V1 75
#define FTA_BS_RBMT_EXTEND_V1      78
#define FTA_IDX_UNDO_V1            151

static void
print_contOrUndo( ftxAgentIdT agent, char *recp, int recCnt)
{
    switch (agent) {
      case FTA_BS_BFS_CREATE_V1:                        /* 1 */
        printf("**** this transaction is not used. ****\n");
        print_unlinkSnapUndoRec(recp, recCnt);
        break;
      case FTA_BS_BFS_UNLINK_SNAP_V1:                  /* 2 */
        print_unlinkSnapUndoRec(recp, recCnt);
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
      case FTA_SNAP_NEW_MCELL:                         /* 159 */
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
      case FTA_BS_XTNT_INSERT_CHAIN_V1:                 /* 21 */
      case FTA_BS_XTNT_REMOVE_CHAIN_V1:                 /* 22 */
        print_insertRemoveXtntChainUndoRec(recp, recCnt);
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
      case FTA_BS_TAG_WRITE_V1:                         /* 40 */
        print_tagDataRec(recp, recCnt);
        break;
      case FTA_BS_TAG_EXTEND_TAGDIR_V1:                 /* 41 */
        print_tagUnInitPageRedo(recp, recCnt);
        break;
      case FTA_FS_INSERT_V1:                            /* 46 */
        print_insert_undo_rec(recp, recCnt);
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
      case FTA_ADVFS_ALLOC_MCELL:                        /* 146 */
        print_mcellPtrRec(recp, recCnt);           /* continuation */
        break;
      case FTA_IDX_UNDO_V1:                             /* 151 */
        print_idxUndoRecord(recp, recCnt);
        break;
      case FTA_ACL_UPDATE:                              /* 156 */
      case FTA_ACL_UPDATE_WITHOUT_UNDO:                 /* 157 */
      case FTA_ACL_UPDATE_WITH_UNDO:                    /* 158 */
        print_aclUndoRecord(recp, recCnt);
	break;
      default:
	if (agent >= FTX_MAX_AGENTS) {
	    printf("Unexpected undo/cont record for unknown agent ID %d\n", 
		   agent);
	} else {
	    printf("Unexpected undo/cont record for agent ID %d (%s?)\n", 
		   agent, ftx_agents[agent]);
	}

        if ( recCnt > ADVFS_METADATA_PGSZ ) {
            printf("record size is too large. Reduced to %d.\n", 
		   ADVFS_METADATA_PGSZ);
            recCnt = ADVFS_METADATA_PGSZ;
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
      case FTA_ADVFS_ALLOC_MCELL:                        /* 146 */
        print_delPropRootDone(recp, recCnt);
        break;
      case FTA_BS_BMT_DEFERRED_MCELL_FREE:              /* 147 */
        print_allocMcellUndoRec(recp, recCnt);
        break;
      default:
        printf("Unexpected root done record for agent ID %d\n", agent);
        if ( recCnt > ADVFS_METADATA_PGSZ ) {
            printf("record size is too large. Reduced to %d.\n", 
		   ADVFS_METADATA_PGSZ);
            recCnt = ADVFS_METADATA_PGSZ;
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

      default:
        printf("Unexpected redo record for agent ID %d\n", agent);
        if ( recCnt > ADVFS_METADATA_PGSZ ) {
            printf("record size is too large. Reduced to %d.\n", 
		   ADVFS_METADATA_PGSZ);
            recCnt = ADVFS_METADATA_PGSZ;
        }
        print_unknown(recp, recCnt);
        break;
    }
}

/*****************************************************************************/
static void
print_imageRedo( char **redopa, int len)
{
    pageredoT *pgredop;
    int i;
    char *datap;
    int numXtnts;
    int bcnt;
    int pgBoff;

    /* the input value "*redopa" may not be 64 bit aligned, since it's
       pointing into a log page buffer at a 32 bit aligned structure.
       So we malloc memory (64 bit aligned by malloc) and byte copy
       the data there.  This allows us to reference the passed-in data
       as a 64 bit data aligned structure.  Otherwise we bus error
       with unaligned access and/or print bogus message data. */

    pgredop = (pageredoT *) malloc(len);
    memcpy(pgredop, *redopa, len);

    numXtnts = pgredop->pgdesc.numXtnts;

    datap = (char *)pgredop + sizeof(ftxRecRedoT) +
            pgredop->pgdesc.numXtnts * sizeof(ftxRecXT);

    printf("    bfsTag,tag %ld,%ld  page %ld  numXtnts %d\n",
      pgredop->pgdesc.bfsTag.tag_num, pgredop->pgdesc.tag.tag_num,
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
        if ( pgBoff >= ADVFS_METADATA_PGSZ ) {
            fflush(stdout);
            fprintf(stderr,
              "Bad LOG. pageredoT.pgdesc.recX[%d].pgBoff too large.\n", i);
        } else if ( pgBoff + bcnt > ADVFS_METADATA_PGSZ ) {
            fflush(stdout);
            fprintf(stderr,
      "Bad LOG. pageredoT.pgdesc.recX[%d] page offset + count too large.\n", i);
        }

        if ( datap + bcnt > (char*)pgredop + len ) {
            bcnt = (char*)pgredop + len - datap;
        }

        print_unknown(datap, bcnt);

	datap = (char *) (roundup(((uint64_t)datap) + bcnt, sizeof(uint32_t)));

        if ( datap > (char*)pgredop + len ) {
            fflush(stdout);
            fprintf(stderr,
              "Bad LOG. Missing redo extents in this log record.\n");
            break;
        }
    }

    free(pgredop);

    *redopa = datap;    
}

/*****************************************************************************/
void
print_logtrlr( FILE *fp, logPgTrlrT *pdata, uint64_t lbn)
{
    char line[80];
    int line_len;
    int i;
    char time_buf[64];
    char buf[80];

    printf(SINGLE_LINE);
    printf("LOG TRAILER:\n");

    if (ctime_r(&pdata->fsCookie, time_buf) == NULL) {
        strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    sprintf(line, "fsCookie 0x%lx.%lx (%s)  ",
      pdata->fsCookie.id_sec, pdata->fsCookie.id_usec, time_buf);
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
    sprintf(buf, "%ld (%ld.%d)", tag.tag_num, tag.tag_num, tag.tag_seq);
    return buf;
}

/*****************************************************************************/
static char*
setid_to_str( bfSetIdT bfSetId, char *buf)
{
    char time_buf[64];

    if (ctime_r(&bfSetId.domainId, time_buf) == NULL) {
        strcpy(time_buf, strerror(errno));
    } else {
	time_buf[strlen(time_buf) - 1] = '\0';
    }

    sprintf(buf, "0x%ld.%ld, %lx.%x  (%s)",
	    bfSetId.dirTag.tag_num, bfSetId.dirTag.tag_seq, 
	    bfSetId.domainId.id_sec, bfSetId.domainId.id_usec,
	    time_buf);

    return buf;
}

/******************************************************************************/
/* FTA_BS_BFS_CREATE_V1  1 not used */
/* FTA_BS_BFS_UNLINK_SNAP_V1  2 */

/* from bs_bitfile_sets.c */
typedef struct {
    bfSetIdT origSetId;
    bfTagT   nextSnapShotTag;
} unlinkCloneUndoRecT;

static void
print_unlinkSnapUndoRec( char *recp, int recCnt)
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
#ifdef SNAPSHOTS_NOTYET
    printf("nextCloneSetTag %s\n", tag_to_str(rp->nextCloneSetTag, buf));
#endif /* SNAPSHOTS */

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
    bfTagT  bfTag;              /* bitfile's tag */
    bfSetIdT bfSetId;           /* bitfile's set id */
    bfMCIdT mcid;               /* mcell id of record's mcell */
    uint32_t recOffset;          /* record's byte offset into mcell */
    uint16_t beforeImageLen;     /* bytes in 'beforeImage' */
    char beforeImage[BS_USABLE_MCELL_SPACE];  /* before image of mcell rec */
} bmtrPutRecUndoT;


static void
print_bmtrPutRecUndo( char *recp, int recCnt)
{
    bmtrPutRecUndoT record;
    bmtrPutRecUndoT *rp = &record;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(bmtrPutRecUndoT));

    if ( recCnt != (int64_t)rp->beforeImage - (int64_t)rp + rp->beforeImageLen ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(bmtrPutRecUndoT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    bfTag %s  ", tag_to_str(rp->bfTag, buf));
    printf("mcid (%d,%ld,%d)\n", 
	   rp->mcid.volume, rp->mcid.page, rp->mcid.cell);

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

    printf("newMcellId (%d,%ld,%d)\n",
	   rp->newMcellId.volume, rp->newMcellId.page, rp->newMcellId.cell);
}

/******************************************************************************/
/* FTA_BS_BMT_ALLOC_LINK_MCELL_V1  8 */

/* from bs_bmt_util.c */
typedef struct allocLinkMcellUndoRec {
    bfMCIdT mcellId;
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

    printf("    mcellId (%d,%ld,%d)  ",
	   rp->mcellId.volume, rp->mcellId.page, rp->mcellId.cell);
    printf("newMcellId (%d,%ld,%d)\n",
	   rp->newMcellId.volume, rp->newMcellId.page, rp->newMcellId.cell);
}

/******************************************************************************/
/* FTA_BS_BMT_LINK_MCELLS_V1  9 */
/* FTA_BS_BMT_UNLINK_MCELLS_V1  10 */

/* from bs_bmt_util.c */
typedef struct linkUnlinkMcellsUndoRec
{
    bfTagT bfTag;
    bfMCIdT prevMCId;
    bfMCIdT prevNextMCId;
    bfMCIdT lastMCId;
    bfMCIdT lastNextMCId;
} linkUnlinkMcellsUndoRecT;

static void
print_linkUnlinkMcellsUndoRec( char *recp, int recCnt)
{
    linkUnlinkMcellsUndoRecT record;
    linkUnlinkMcellsUndoRecT *rp = &record;

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, sizeof(linkUnlinkMcellsUndoRecT));

    if ( recCnt != sizeof(linkUnlinkMcellsUndoRecT) ) {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(linkUnlinkMcellsUndoRecT));
    }

    printf("    prevMCId (%d,%ld,%d)  ",
      rp->prevMCId.volume, rp->prevMCId.page, rp->prevMCId.cell);
    printf("prevNextMCId (%d,%ld,%d)  ",
      rp->prevNextMCId.volume, rp->prevNextMCId.page, rp->prevNextMCId.cell);
    printf("lastMCId (%d,%ld,%d)  ",
      rp->lastMCId.volume, rp->lastMCId.page, rp->lastMCId.cell);
    printf("lastNextMCId (%d,%ld,%d)\n",
      rp->lastNextMCId.volume, rp->lastNextMCId.page, rp->lastNextMCId.cell);
}

/******************************************************************************/
/* FTA_BS_BF_CREATE_V1  11 */
/* FTA_BS_CRE_MCELL_V1  12 */

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

    printf("    mcell (%d,%ld,%d)\n",
      rp->mcell.volume, rp->mcell.page, rp->mcell.cell);

    printf("    ut.bfsid %s\n", setid_to_str(rp->ut.bfsid, buf));
    printf("    ut.tag %s\n", tag_to_str(rp->ut.tag, buf));
}

/******************************************************************************/
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
    case ACC_RECYCLE:                            \
      printf("ACC_RECYCLE");                     \
      break;                                     \
    case ACC_DEALLOC:                            \
      printf("ACC_DEALLOC");                     \
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
    uint32_t vdIndex;
    bfSetIdT bfSetId;
    lkStatesT prevAccState;
    bfStatesT prevBfState;
    uint32_t setBusy;
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

    printf("    mcid (%d,%ld,%d)  ", 
	   rp->mcid.volume, rp->mcid.page, rp->mcid.cell);
    printf("bfSetId %s  ", setid_to_str(rp->bfSetId, buf));
    printf("tag %s\n", tag_to_str(rp->tag, buf));

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
/* FTA_ADVFS_ALLOC_MCELL  146 */

/* from bs_bmt.h */
typedef struct {
    bfMCIdT mcid;
} mcellPtrRecT;

static void
print_mcellPtrRec( char *recp, int recCnt)
{
    mcellPtrRecT *rp;

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

    rp = malloc(chainCnt * sizeof(mcellPtrRecT));

    memcpy((void*)rp, (void*)recp, chainCnt * sizeof(mcellPtrRecT));

    do {
        printf("    mcid (%d,%ld,%d)\n",
          rp->mcid.volume, rp->mcid.page, rp->mcid.cell);

        chainCnt--;
        rp++;
    } while ( chainCnt > 0 );

    free(rp);
}

/******************************************************************************/
/* FTA_BS_XTNT_UPD_MCELL_CNT_V1  16 */

#define PRINT_xtnt_type(type) {                \
    if ( type == BSR_XTNTS ) {                 \
        printf("BSR_XTNTS");                   \
    } else if ( type == BSR_XTRA_XTNTS ) {     \
        printf("BSR_XTRA_XTNTS");              \
    } else {                                   \
        printf("unknown type %d", type);       \
    }                                          \
}

/* from bs_extents.c */
typedef struct mcellCntUndoRec {
    uint32_t type;
    bfMCIdT mCId;
    bfTagT bfTag;
    uint32_t mcellCnt;
} mcellCntUndoRecT;

static void
print_mcellCntUndoRec( char *recp, int recCnt)
{
    mcellCntUndoRecT record;
    mcellCntUndoRecT *rp = &record;;
    char buf[80];

    memcpy((void*)rp, (void*)recp, sizeof(mcellCntUndoRecT));

    if ( recCnt == sizeof(mcellCntUndoRecT) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(mcellCntUndoRecT));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(mcellCntUndoRecT));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(mcellCntUndoRecT));
    }

    printf("    mCId (%d,%ld,%d)  ",
	   rp->mCId.volume, rp->mCId.page, rp->mCId.cell);
    printf("bfTag %s  ", tag_to_str(rp->bfTag, buf));
    printf("type ");
    PRINT_xtnt_type(rp->type);
    printf("  mcellCnt %d\n", rp->mcellCnt);
}

/******************************************************************************/
/* FTA_BS_XTNT_UPD_REC_V1  17 */

/* from bs_extents.c */

typedef struct updXtntRecUndoRec
{
    uint16_t type;
    uint16_t xCnt;
    uint16_t index;
    uint16_t cnt;
    bfMCIdT mCId;
    bfTagT bfTag;
    union {
        bsXtntT xtnt[BMT_XTNTS];
        bsXtntT xtraXtnt[BMT_XTRA_XTNTS];
    } bsXA;
} updXtntRecUndoRecT;

static void
print_updXtntRecUndoRec( char *recp, int undoBcnt)
{
    updXtntRecUndoRecT record;
    updXtntRecUndoRecT *rp = &record;
    int i;
    uint xcnt;
    int structSize;
    char buf[80];

    /* recp is 'int' aligned, not 'long' aligned. */
    memcpy((void*)rp, (void*)recp, undoBcnt);

    structSize = (char*)&rp->bsXA.xtnt[rp->cnt] - (char*)rp;

    printf("    bfTag %s\n", tag_to_str(rp->bfTag, buf));
    printf("    mCId (%d,%ld,%d)  ",
	   rp->mCId.volume, rp->mCId.page, rp->mCId.cell);
    printf("index %d, ", rp->index);
    printf("cnt %d, ", rp->cnt);
    PRINT_xtnt_type(rp->type);
    printf(", xCnt %d\n", rp->xCnt);

    if ( undoBcnt == structSize) {
        xcnt = rp->cnt;
    } else {
        printf("Unexpected record size. ");
        printf("Base structure size is %d, total record size is %d.\n",
	       structSize, undoBcnt);

        xcnt = (undoBcnt - structSize) / sizeof(bsXtntT);
        if ( xcnt != rp->cnt ) {
            printf("struct's count says %d xtnts, record has room for %d. Printing %d.\n",
              rp->cnt, xcnt, xcnt);
        }
    }

    for (i = 0; i < xcnt; i++) {
        printf("      xtnt bsx_fob_offset %ld  bsx_vd_blk %ld\n",
	       rp->bsXA.xtnt[i].bsx_fob_offset, rp->bsXA.xtnt[i].bsx_vd_blk);
    }
}

/******************************************************************************/
/* FTA_BS_XTNT_INSERT_CHAIN_V1  21 */
/* FTA_BS_XTNT_REMOVE_CHAIN_V1  22 */

/* from bs_bmt_util.c */
typedef struct insertRemoveXtntChainUndoRec
{
    bfMCIdT primMCId;
    bfMCIdT primChainMCId;
    bfMCIdT lastMCId;
    bfMCIdT lastNextMCId;
    bfTagT bfTag;
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

    printf("    primMCId (%d,%ld,%d)  ",
	   rp->primMCId.volume, rp->primMCId.page, rp->primMCId.cell);
    printf("primChainMCId (%d,%ld,%d)  ",
	   rp->primChainMCId.volume, rp->primChainMCId.page, 
	   rp->primChainMCId.cell);
    printf("lastMCId (%d,%ld,%d)  ",
	   rp->lastMCId.volume, rp->lastMCId.page, rp->lastMCId.cell);
    printf("lastNextMCId (%d,%ld,%d)\n",
	   rp->lastNextMCId.volume, rp->lastNextMCId.page, 
	   rp->lastNextMCId.cell);
}

/******************************************************************************/
/* FTA_BS_SBM_ALLOC_BITS_V1  33 */
/* FTA_BS_SBM_DEALLOC_BITS_V1  36 not used */

/* from bs_sbm.c */
typedef struct bitmapUndoRec
{
    int type;  /* ALLOC_BITS, DEALLOC_BITS */
    uint16_t vdIndex;
    bs_meta_page_t burSbmPage; /* Page to have bits startBit..endBit undone */
    uint32_t startBit;
    uint32_t endBit;
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
    printf("burSbmPage %ld  ", rp->burSbmPage);
    printf("startBit %u  ", rp->startBit);
    printf("endBit %u\n", rp->endBit);
}

/******************************************************************************/
/* FTA_BS_STG_ADD_V1  34 */

/* from bs_stg.c */
typedef struct addStgUndoRec
{
    bfSetIdT setId;             /* Set id */
    bfTagT tag;                 /* File that had storage added */
    bf_fob_t asurNextFob; 
                                /* Previous value of the
                                 * bfAcess->next_fob before the 
                                 * storage was added */
    bf_fob_t asurFobOffset;
                                /* File offset in 1k units were storage was
                                 * added */
    bf_fob_t asurFobCnt;
                                /* Count of 1k units that were added to the
                                 * file */
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

    printf("    asurNextFob %ld  ", rp->asurNextFob);
    printf("asurFobOffset %ld  ", rp->asurFobOffset);
    printf("asurFobCnt %ld\n", rp->asurFobCnt);
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

    printf("    map.tmSeqNo %x, tmBfMCId (%d,%ld,%d)\n",
      rp->map.tmSeqNo, rp->map.tmBfMCId.volume,
      rp->map.tmBfMCId.page, rp->map.tmBfMCId.cell);
}

/******************************************************************************/
/* FTA_BS_TAG_EXTEND_TAGDIR_V1  41 */

/* from bs_tagdir.c */
typedef struct {
    bfSetIdT       bfSetId;
    bs_meta_page_t unInitPage;
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
        printf("Record size is %d, structure size is %ld.\n",
          recCnt, sizeof(tagUnInitPageRedoT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));

    printf("    unInitPage %ld\n", rp->unInitPage);
}

/******************************************************************************/
/* FTA_FS_INSERT_V1  46 */

static void
print_insert_undo_rec( char *recp, int recCnt)
{
    insert_undo_rec record;
    insert_undo_rec *rp = NULL;
    char buf[80];

    if ( recCnt == sizeof(insert_undo_rec) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(insert_undo_rec));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(insert_undo_rec));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(insert_undo_rec));
    }

    printf("    dir_tag %s  ", tag_to_str(rp->undo_header.dir_tag, buf));
    printf("ins_tag %s\n", tag_to_str(rp->undo_header.ins_tag, buf));
    printf("    bfSetId %s\n", setid_to_str(rp->undo_header.bfSetId, buf));
    printf("    page %ld  byte %d  old_size %ld\n",
	   rp->undo_header.page, rp->undo_header.byte, rp->undo_header.old_size);
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
    uint32_t vdIndex;
    uint32_t type;
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

    printf("    mcid (%d,%ld,%d)  ",
      rp->mcid.volume, rp->mcid.page, rp->mcid.cell);
    if ( rp->type == DEL_ADD ) {
        printf("type DEL_ADD\n");
    } else if ( rp->type == DEL_REMOVE ) {
        printf("type DEL_REMOVE\n");
    } else {
        printf("unknown type %d\n", rp->type);
    }
}

/******************************************************************************/
/* FTA_FS_DQUOT_SYNC_V2  139 */
/* FTA_FS_DQUOT_SYNC_V2  139 */

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
    uint64_t     unused;
    bfSetIdT    bfSetId;
    uint32_t     id;
    bfTagT      tag;
    uint16_t     offset;
    uint16_t     type;
    bs_meta_page_t page;
    bf_fob_t    blkChng;
    bf_fob_t    fileChng;
    uint64_t     old_quota_file_size;
} quotaRecT;

static void
print_quotaRec( char *recp, int recCnt)
{
    quotaRecT record;
    quotaRecT *rp = NULL;
    uint16_t offsetx;
    uint16_t typex;
    char buf[80];

    if ( recCnt == sizeof(quotaRecT) ) {
        rp = &record;
        /* recp is 'int' aligned, not 'long' aligned. */
        memcpy((void*)rp, (void*)recp, sizeof(quotaRecT));
    } else {
        printf("    Unexpected record size. ");
        printf("Record size is %d, structure size is %d.\n",
          recCnt, sizeof(quotaRecT));

        rp = &record;
        memcpy((void*)rp, (void*)recp, sizeof(quotaRecT));
    }

    printf("    bfSetId %s\n", setid_to_str(rp->bfSetId, buf));
    printf("    tag %s  ", tag_to_str(rp->tag, buf));
    printf("type ");
    PRINT_quota_type(rp->type);
    printf("id %d  ", rp->id);
    printf("page %ld  ", rp->page);
    printf("offset %d\n", rp->offset);
    printf("    blkChng %ld  ", rp->blkChng);
    printf("fileChng %ld  ", rp->fileChng);
    printf("old_quota_file_size %ld\n", rp->old_quota_file_size);
}

/******************************************************************************/
/* FTA_ADVFS_ALLOC_MCELL  146 */

/*
 * from msfs_proplist.c
 *
 * msfs_proplist.c was removed, but we're keeping these typedefs around
 * to use the print_delPropRootDone routine for FTA_ADVFS_ALLOC_MCELL cases
 */

typedef struct bsOdRecCur {
  bfMCIdT  MC;
  bs_meta_page_t pgoff;
  bfMCIdT  prevMC;
} bsOdRecCurT;

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
    printf("    hdr.MC (%d,%ld,%d)  ",
	   rp->hdr.MC.volume, rp->hdr.MC.page, rp->hdr.MC.cell);
    printf("hdr.pgoff %ld  ", rp->hdr.pgoff);
    printf("hdr.prevMC (%d,%ld,%d)\n",
	   rp->hdr.prevMC.volume, rp->hdr.prevMC.page, rp->hdr.prevMC.cell);

    printf("    end.MC (%d,%ld,%d)  ",
	   rp->end.MC.volume, rp->end.MC.page, rp->end.MC.cell);
    printf("end.pgoff %ld  ", rp->end.pgoff);
    printf("end.prevMC (%d,%ld,%d)\n",
      rp->end.prevMC.volume, rp->end.prevMC.page, rp->end.prevMC.cell);

    printf("    mcellList_lk 0x%lx  cellsize %d\n",
      rp->mcellList_lk, rp->cellsize);
}

/******************************************************************************/
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


static void
print_aclUndoRecord(char *recp, int recBcnt)
{
    acl_update_undo_hdr_t *header;
    acl_update_undo_t *undoData;
    char buf[80];
    int i;

    header = (acl_update_undo_hdr_t *) recp;

    printf("    header.auuh_undo_count %d  header.auuh_bf_set_id:\n\t%s\n", 
	   header->auuh_undo_count,
	   setid_to_str(header->auuh_bf_set_id, buf));
    printf("    header.auuh_bf_tag %s\n",
	   tag_to_str(header->auuh_bf_tag, buf));

    undoData = (acl_update_undo_t *)(((char *)header) + sizeof(*header));

    for (i = 0; i < header->auuh_undo_count; i++) {
	printf("    RECORD %d: mcell_id (%d,%ld,%d)\n",  
	       i, 
	       undoData[i].auu_mcell_id.volume,
	       undoData[i].auu_mcell_id.page,
	       undoData[i].auu_mcell_id.cell);
    }
    printf("\n");

} /* print_aclUndoRecord */
