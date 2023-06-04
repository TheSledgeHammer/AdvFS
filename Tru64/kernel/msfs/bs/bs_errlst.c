/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992, 1993                            *
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
 *      ADVFS Storage System
 *
 * Abstract:
 *
 *      ADVFS status message strings.
 *
 * Date:
 *
 *      Mon Apr  6 11:09:50 1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_errlst.c,v $ $Revision: 1.1.51.2 $ (DEC) $Date: 2004/10/01 06:25:14 $"

#include <sys/types.h>
#include <sys/systm.h>
#ifdef OSF
#include <sys/errno.h>
#endif
#ifdef USER_MODE
#include <stdio.h>
#include <sys/errno.h>
#define ESUCCESS 0
#endif

/* THIS FILE MUST NOT INCLUDE ANY ADVFS FILES EXCEPT bs_error.h */
/* Otherwise you'll break user mode apps that link with this file */

#include <msfs/bs_error.h>

char *bs_errlist[] = {
    "ETAG_OVF (-1025)",
    "EHANDLE_OVF (-1026)",
    "EINVALID_HANDLE (-1027)",
    "E_DOMAIN_PANIC (-1028)",
    "EDUP_VD (-1029)",
    "EBAD_VDI (-1030)",
    "ENO_SUCH_DOMAIN (-1031)",
    "ENO_SUCH_TAG (-1032)",
    "ENO_MORE_DOMAINS (-1033)",
    "EBAD_DOMAIN_HANDLE (-1034)",
    "E_PAGE_NOT_MAPPED (-1035)",
    "ENO_XTNTS (-1036)",
    "ENO_BS_ATTR (-1037)",
    "EBAD_TAG (-1038)",
    "EXTND_FAILURE (-1039)",
    "ENO_MORE_BLKS (-1040)",
    "ENOT_SUPPORTED (-1041)",
    "EBAD_PARAMS (-1042)",
    "EBMTR_NOT_FOUND (-1043)",
    "ETOO_MANY_SERVICE_CLASSES (-1044)",
    "EBAD_SERVICE_CLASS (-1045)",
    "ESERVICE_CLASS_NOT_FOUND (-1046)",
    "ENO_SERVICE_CLASSES (-1047)",
    "ESERVICE_CLASS_NOT_EMPTY (-1048)",
    "EBAD_FTX_AGENTH (-1049)",
    "EBAD_PAR_FTXH (-1050)",
    "EFTX_TOO_DEEP (-1051)",
    "EBAD_FTXH (-1052)",
    "EBAD_STG_DESC (-1053)",
    "ENO_MORE_MEMORY (-1054)",
    "ENO_MORE_MCELLS (-1055)",
    "EALREADY_ALLOCATED (-1056)",
    "E_INVALID_LOG_DESC_POINTER (-1057)",
    "E_CANT_CREATE_LOG (-1058)",
    "E_VOLUME_COUNT_MISMATCH (-1059)",
    "E_LOG_EMPTY (-1060)",
    "ENO_SPACE_IN_MCELL (-1061)",
    "E_INVALID_REC_ADDR (-1062)",
    "W_LAST_LOG_REC (+1063)",
    "E_BAD_CLIENT_REC_ADDR (-1064)",
    "E_CANT_ACCESS_LOG (-1065)",
    "E_NO_MORE_RD_HANDLES (-1066)",
    "E_DOMAIN_NOT_ACTIVATED (-1067)",
    "E_DOMAIN_NOT_CLOSED (-1068)",
    "E_CANT_DISMOUNT_VD1_ACTIVE_DMN (-1069)",
    "E_BAD_VERSION (-1070)",
    "E_VD_BFDMNID_DIFF (-1071)",
    "I_LOG_REC_SEGMENT (1072)",
    "I_LAST_LOG_REC_SEGMENT (1073)",
    "E_RANGE (-1074)",
    "E_RDEV_MISMATCH (-1075)",
    "E_EXCEEDS_EXTENT (-1076)",
    "E_CANT_FIND_LOG_END (-1077)",
    "E_BAD_MCELL_LINK_SEGMENT (-1078)",
    "E_VD_DMNATTR_DIFF (-1079)",
    "E_MUST_BE_ROOT (-1080)",
    "E_RANGE_NOT_CLEARED (-1081)",
    "E_TAG_EXISTS (-1082)",
    "E_NOMAP_INDEX (-1083)",
    "E_RSVD_FILE_INWAY (-1084)",
    "E_ACTIVE_RANGE (-1085)",
    "E_DOMAIN_ALREADY_EXISTS (-1086)",
    "E_LAST_PAGE (-1087)",
    "E_1088 (-1088)",
    "E_1089 (-1089)",
    "E_CORRUPT_LIST (-1090)",
    "W_1091(-1091)",
    "E_1092 (-1092)",
    "E_NO_SUCH_BF_SET (-1093)",
    "E_BAD_BF_SET_POINTER (-1094)",
    "E_TOO_MANY_BF_SETS (-1095)",
    "E_1096 (-1096)",
    "E_BF_SET_NOT_CLOSED (-1097)",
    "E_1098 (-1098)",
    "E_BLKDESC_ARRAY_TOO_SMALL (-1099)",
    "E_TOO_MANY_ACCESSORS (-1100)",
    "E_1101 (-1101)",
    "E_XTNT_MAP_NOT_FOUND (-1102)",
    "E_ALREADY_SHADOWED (-1103)",
    "E_NOT_ENOUGH_DISKS (-1104)",
    "E_STG_ADDED_DURING_SHADOW_OPER (-1105)",
    "E_STG_ADDED_DURING_MIGRATE_OPER (-1106)",
    "E_BAD_DEV (-1107)",
    "E_ADVFS_NOT_INSTALLED (-1108)",
    "E_STGDESC_NOT_FOUND (-1109)",
    "E_FULL_INTERSECT (-1110)",
    "E_PARTIAL_INTERSECT (-1111)",
    "E_SPACE_ALREADY_RESERVED (-1112)",
    "E_SPACE_NOT_RESERVED (-1113)",
    "E_READ_ONLY (-1114)",
    "E_TOO_MANY_CLONES (-1115)",
    "E_OUT_OF_SYNC_CLONE (-1116)",
    "E_HAS_CLONE (-1117)",
    "E_TOO_MANY_BITFILES (-1118)",
    "E_TOO_MANY_BLOCKS (-1119)",
    "E_QUOTA_NOT_ENABLED (-1120)",
    "E_IO (-1121)",
    "E_NO_MORE_DQUOTS (-1122)",
    "E_NO_MORE_SETS (-1123)",
    "E_DUPLICATE_SET (-1124)",
    "E_CANT_CLONE_A_CLONE (-1125)",
    "E_ACCESS_DENIED (-1126)",
    "E_BLKOFFSET_NOT_PAGE_ALIGNED (-1127)",
    "E_BLKOFFSET_NOT_CLUSTER_ALIGNED (-1128)",
    "E_INVALID_BLK_RANGE (-1129)",
    "E_CANT_MIGRATE_HOLE (-1130)",
    "E_NOT_OWNER (-1131)",
    "E_MIGRATE_IN_PROGRESS (-1132)",
    "E_SHADOW_IN_PROGRESS (-1133)",
    "E_CANT_MIGRATE_SHADOW (-1134)",
    "E_INVALID_FS_VERSION(-1135)",
    "E_INVALID_ADVFS_STRUCT (-1136)",
    "E_NOT_DIRECTORY (-1137)",
    "I_FILE_EXISTS (-1138)",
    "I_INSERT_HERE (-1139)",
    "I_FOUND_DELETED (-1140)",
    "I_FILE_NOT_FOUND (-1141)",
    "E_LOOKUP_ERROR (-1142)",
    "E_1143 (-1143)",
    "E_PAGE_HOLE (-1144)",
    "E_TOO_MANY_VIRTUAL_DISKS (-1145)",
    "E_RESERVED_1 (-1146)",
    "E_RESERVED_2 (-1147)",
    "E_RESERVED_3 (-1148)",
    "E_RESERVED_4 (-1149)",
    "E_RESERVED_5 (-1150)",
    "E_RESERVED_6 (-1151)",
    "E_RESERVED_7 (-1152)",
    "E_RESERVED_8 (-1153)",
    "E_RESERVED_9 (-1154)",
    "E_RESERVED_10 (-1155)",
    "E_RESERVED_11 (-1156)",
    "E_RESERVED_12 (-1157)",
    "E_RESERVED_13 (-1158)",
    "E_RESERVED_14 (-1159)",
    "E_RESERVED_15 (-1160)",
    "E_RESERVED_16 (-1161)",
    "ENO_NAME (-1162)",
    "EDIR_NOT_ADVFS (-1163)", 
    "EUNDEL_DIR_NOT_ADVFS (-1164)", 
    "EDUPLICATE_DIRS (-1165)", 
    "EDIFF_FILE_SETS (-1166)",
    "E_BAD_MAGIC (-1167)",
    "E_BAD_PAGESIZE (-1168)",
    "E_NO_DMN_VOLS (-1169)",
    "E_BAD_TAGDIR (-1170)",
    "E_BAD_BMT (-1171)",
    "E_ALREADY_SWITCHING_LOGS (-1172)",
    "E_NO_UNDEL_DIR (-1173)",
    "E_QUOTA_NOT_MAINTAINED (-1174)",
    "E_BMT_NOT_EMPTY (-1175)",
    "E_ROOT_TAGDIR_ON_VOL (-1176)",
    "E_LOG_ON_VOL (-1177)",
    "E_VOL_NOT_IN_SVC_CLASS (-1178)",
    "E_WOULD_BLOCK (-1179)",
    "W_NOT_WRIT (+1180)",
    "E_FRAG_PAGE (-1181)",
    "E_ALREADY_STRIPED (-1182)",
    "E_NO_CLONE_STG (-1183)",
    "E_BAD_PAGE_RANGE (-1184)",
    "E_INVOLUNTARY_ABORT (-1185)",
    "W_1186 (+1186)",
    "E_RMVOL_ALREADY_INPROG (-1187)",
    "E_TOO_LARGE_SPARSE (-1188)",
    "E_NOT_ENOUGH_XTNTS (-1189)",
    "E_INDEX_PRUNE_FAILED (-1190)",
    "E_ILLEGAL_CLONE_OP (-1191)",
};

/*
 * This table could be maintained as part of the preceding table, by making
 * that table an array of structures.  That wasn't done for two reasons:
 * First, it breaks too much existing code (which doesn't use the BSERRMSG 
 * macro) and second, the mapping to errnos may be dependent on the 
 * operating system in which MegaSafe is embedded, and the above list is
 * OS independent.
 *
 * For now, the table is filled with ESUCCESS as a placeholder.  Fill in
 * errnos as you think of them (see <sys/errno.h>) for OSF errnos.
 */
int sts_to_errno_map[] = {
    EMFILE,      /* ETAG_OVF */
    ENFILE,      /* EHANDLE_OVF */
    EBADF,       /* EINVALID_HANDLE */
    EIO,         /* E_DOMAIN_PANIC */
    EINVAL,      /* EDUP_VD */
    EIO,         /* EBAD_VDI */
    EINVAL,      /* ENO_SUCH_DOMAIN */
    EINVAL,      /* ENO_SUCH_TAG */
    EMFILE,      /* ENO_MORE_DOMAINS */
    EBADF,       /* EBAD_DOMAIN_HANDLE */
    EIO,         /* E_PAGE_NOT_MAPPED */
    EIO,         /* ENO_XTNTS */
    EIO,         /* ENO_BS_ATTR */
    EBADF,       /* EBAD_TAG */
    EIO,         /* EXTND_FAILURE */
    ENOSPC,      /* ENO_MORE_BLKS */
    ENOSYS,      /* ENOT_SUPPORTED */
    EINVAL,      /* EBAD_PARAMS */
    EIO,         /* EBMTR_NOT_FOUND */
    EMFILE,      /* ETOO_MANY_SERVICE_CLASSES */
    EINVAL,      /* EBAD_SERVICE_CLASS */
    EINVAL,      /* ESERVICE_CLASS_NOT_FOUND */
    EIO,         /* ENO_SERVICE_CLASSES */
    EIO,         /* ESERVICE_CLASS_NOT_EMPTY */
    EIO,         /* EBAD_FTX_AGENTH */
    EIO,         /* EBAD_PAR_FTXH */
    EIO,         /* EFTX_TOO_DEEP */
    EIO,         /* EBAD_FTXH */
    EIO,         /* EBAD_STG_DESC */
    ENOMEM,      /* ENO_MORE_MEMORY */
    ENOSPC,      /* ENO_MORE_MCELLS */
    EIO,         /* EALREADY_ALLOCATED */
    EMFILE,      /* E_NO_MORE_LOG_DESC */
    EIO,         /* E_CANT_CREATE_LOG */
    EIO,         /* E_VOLUME_COUNT_MISMATCH */
    EIO,         /* E_LOG_EMPTY */
    EIO,         /* E_1061 */
    EIO,         /* E_INVALID_REC_ADDR */
    ESUCCESS,    /* W_LAST_LOG_REC */
    EIO,         /* E_BAD_CLIENT_REC_ADDR */
    EIO,         /* E_CANT_ACCESS_LOG */
    EIO,         /* E_NO_MORE_RD_HANDLES */
    EINVAL,      /* E_DOMAIN_NOT_ACTIVATED */
    EIO,         /* E_DOMAIN_NOT_CLOSED */
    EIO,         /* E_CANT_DISMOUNT_VD1_ACTIVE_DMN */
    EINVAL,      /* E_BAD_VERSION */
    EIO,         /* E_VD_BFDMNID_DIFF */
    ESUCCESS,    /* I_LOG_REC_SEGMENT */
    ESUCCESS,    /* I_LAST_LOG_REC_SEGMENT */
    EFAULT,      /* E_UNALLIGNED_ADDR */
    EFAULT,      /* E_UNALLIGNED_BCOUNT */
    EIO,         /* E_EXCEEDS_EXTENT */
    EIO,         /* E_CANT_FIND_LOG_END */
    EIO,         /* E_BAD_MCELL_LINK_SEGMENT */
    EIO,         /* E_VD_DMNATTR_DIFF */
    EPERM,       /* E_MUST_BE_ROOT */
    EIO,         /* E_1081 */
    EEXIST,      /* E_TAG_EXISTS */
    EIO,         /* E_CANT_CREATE_BR_CAT */
    EIO,         /* E_BR_CAT_DOMAIN_UNAVAILABLE */
    EIO,         /* E_CANT_ACCESS_BR_CAT */
    EIO,         /* E_DOMAIN_ALREADY_EXISTS */
    EIO,         /* E_CANT_CREATE_DMN_CAT */
    EIO,         /* E_CANT_CREATE_BF_CAT */
    EIO,         /* E_DOMAIN_NOT_REGISTERED */
    EIO,         /* E_CANT_ACCESS_BF_CAT */
    ESUCCESS,    /* W_DOMAIN_ALREADY_REGISTERED */
    EIO,         /* E_CANT_ALLOC_CAT_REC */
    ENOENT,      /* E_NO_SUCH_BF_SET */
    EIO,         /* E_BAD_BF_SET_POINTER */
    EMFILE,      /* E_TOO_MANY_BF_SETS */
    EINVAL,      /* E_BAD_BF_SET_TBL */
    EIO,         /* E_BF_SET_NOT_CLOSED */
    EIO,         /* E_BUF_NOT_UNPIN_BLOCKED */
    EIO,         /* E_BLKDESC_ARRAY_TOO_SMALL */
    EIO,         /* E_TOO_MANY_ACCESSORS */
    EIO,         /* E_1101 */
    EIO,         /* E_XTNT_MAP_NOT_FOUND */
    EINVAL,      /* E_ALREADY_SHADOWED */
    EINVAL,      /* E_NOT_ENOUGH_DISKS */
    EIO,         /* E_STG_ADDED_DURING_SHADOW_OPER */
    EIO,         /* E_STG_ADDED_DURING_MIGRATE_OPER */
    ENODEV,      /* E_BAD_DEV */
    ENOSYS,      /* E_ADVFS_NOT_INSTALLED */
    EIO,         /* E_STGDESC_NOT_FOUND */
    EIO,         /* E_FULL_INTERSECT */
    EIO,         /* E_PARTIAL_INTERSECT */
    EIO,         /* E_SPACE_ALREADY_RESERVED */
    EIO,         /* E_SPACE_NOT_RESERVED */
    EROFS,       /* E_READ_ONLY */
    EMFILE,      /* E_TOO_MANY_CLONES */
    EIO,         /* E_OUT_OF_SYNC_CLONE */
    EIO,         /* E_HAS_CLONE */
    EDQUOT,      /* E_TOO_MANY_BITFILES */
    EDQUOT,      /* E_TOO_MANY_BLOCKS */
    EINVAL,      /* E_QUOTA_NOT_ENABLED */
    EIO,         /* E_IO */
    EUSERS,      /* E_NO_MORE_DQUOTS */
    ESUCCESS,    /* E_NO_MORE_SETS */
    EEXIST,      /* E_DUPLICATE_SET */
    ESUCCESS,    /* E_CANT_CLONE_A_CLONE */
    EACCES,      /* E_ACCESS_DENIED */
    EFAULT,      /* E_BLKOFFSET_NOT_PAGE_ALIGNED */
    EFAULT,      /* E_BLKOFFSET_NOT_CLUSTER_ALIGNED */
    EINVAL,      /* E_INVALID_BLK_RANGE */
    ENOSYS,      /* E_CANT_MIGRATE_HOLE */
    EPERM,       /* E_NOT_OWNER */
    EBUSY,       /* E_MIGRATE_IN_PROGRESS */
    EBUSY,       /* E_SHADOW_IN_PROGRESS */
    ENOSYS,      /* E_CANT_MIGRATE_SHADOW */
    EINVAL,      /* E_INVALID_FS_VERSION */
    EINVAL,      /* E_INVALID_ADVFS_STRUCT */
    ENOTDIR,     /* E_NOT_DIRECTORY */
    EEXIST,      /* I_FILE_EXISTS */
    ENOENT,      /* I_INSERT_HERE */
    ENOENT,      /* I_FOUND_DELETED */
    ENOENT,      /* I_FILE_NOT_FOUND */
    ENOENT,      /* E_LOOKUP_ERROR */
    EACCES,      /* E_1143 */
    EINVAL,      /* E_PAGE_HOLE */
    EINVAL,      /* E_TOO_MANY_VIRTUAL_DISKS */
    EIO,         /* E_RESERVED_1                        (-1146) */
    EIO,         /* E_RESERVED_2                        (-1147) */
    EIO,         /* E_RESERVED_3                        (-1148) */
    EIO,         /* E_RESERVED_4                        (-1149) */
    EIO,         /* E_RESERVED_5                        (-1150) */
    EIO,         /* E_RESERVED_6                        (-1151) */
    EIO,         /* E_RESERVED_7                        (-1152) */
    EIO,         /* E_RESERVED_8                        (-1153) */
    EIO,         /* E_RESERVED_9                        (-1154) */
    EIO,         /* E_RESERVED_10                       (-1155) */
    EIO,         /* E_RESERVED_11                       (-1156) */
    EIO,         /* E_RESERVED_12                       (-1157) */
    EIO,         /* E_RESERVED_13                       (-1158) */
    EIO,         /* E_RESERVED_14                       (-1159) */
    EIO,         /* E_RESERVED_15                       (-1160) */
    EIO,         /* E_RESERVED_16                       (-1161) */
    ENOENT,      /* ENO_NAME                            (-1162) */
    ENOSYS,      /* EDIR_NOT_ADVFS                      (-1163) */
    ENOSYS,      /* EUNDEL_DIR_NOT_ADVFS                (-1164) */
    ENOSYS,      /* EDUPLICATE_DIRS                     (-1165) */
    ENOSYS,      /* EDIFF_FILE_SETS                     (-1166) */
    EINVAL,      /* E_BAD_MAGIC                         (-1167) */
    EINVAL,      /* E_BAD_PAGESIZE                      (-1168) */
    EINVAL,      /* E_NO_DMN_VOLS                       (-1169) */
    EIO,         /* E_BAD_TAGDIR                        (-1170) */
    EIO,         /* E_BAD_BMT                           (-1171) */
    EIO,         /* E_ALREADY_SWITCHING_LOGS            (-1172) */
    ENOENT,      /* E_NO_UNDEL_DIR                      (-1173) */
    EINVAL,      /* E_QUOTA_NOT_MAINTAINED              (-1174) */
    EINVAL,      /* E_BMT_NOT_EMPTY                     (-1175) */
    EINVAL,      /* E_ROOT_TAGDIR_ON_VOL                (-1176) */
    EINVAL,      /* E_LOG_ON_VOL                        (-1177) */
    EINVAL,      /* E_VOL_NOT_IN_SVC_CLASS              (-1178) */
    EWOULDBLOCK, /* E_WOULD_BLOCK                       (-1179) */
    ESUCCESS,    /* W_NOT_WRIT                          (+1180) */
    EIO,         /* E_FRAG_PAGE                         (-1181) */
    EINVAL,      /* E_ALREADY_STRIPED                   (-1182) */
    EINVAL,      /* E_NO_CLONE_STG                      (-1183) */
    EINVAL,      /* E_BAD_PAGE_RANGE                    (-1184) */
    EIO,         /* E_INVOLUNTARY_ABORT                 (-1185) */
    ESUCCESS,    /* W_1186                              (+1186) */
    EINVAL,      /* E_RMVOL_ALREADY_INPROG              (-1187) */
    EINVAL,      /* E_TOO_LARGE_SPARSE                  (-1188) */
    EINVAL,      /* E_NOT_ENOUGH_XTNTS                  (-1189) */
    ENOSPC,      /* ENO_MORE_BLKS                       (-1190) */
    EINVAL,      /* E_ILLEGAL_CLONE_OP                  (-1191) */
};

char *
advfs_errmsg(
    int sts
    )
{
    static char * errNo = "errno";
    static char * unknownErr = "unknown error";

    if ((abs( sts ) < MSFS_FIRST_ERR)) {
#ifdef KERNEL
        aprintf( "errno = %d\n", abs( sts ) );
        return errNo;
#else
        extern char *sys_errlist[];
        return sys_errlist[sts];
#endif

    } else if ((abs( sts ) > abs(MSFS_LAST_ERR))) {
#ifdef KERNEL
        aprintf( "unknown error = %d", sts );
        return unknownErr;
#else
        static char msg[256];
        sprintf( msg, "unknown error = %d", sts );
        return msg;
#endif

    } else {
        char *m;
        m =  bs_errlist[abs( sts ) - MSFS_FIRST_ERR];
        return m;
    }
}
