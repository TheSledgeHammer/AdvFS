/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1989, 1990, 1991, 1992, 1993          *
 */
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
 * @(#)$RCSfile: bs_error.h,v $ $Revision: 1.1.64.2 $ (DEC) $Date: 2004/10/01 06:25:16 $
 */

#ifndef _BS_ERROR_H_
#define _BS_ERROR_H_

/*
 **** THIS FILE MUST NOT INCLUDE ANY OTHER MSFS HEADER FILES ****
 */

#ifdef KERNEL
#define abs( a )    ((a) < 0 ? -(a) : (a))
#else
#include <stdlib.h>
#endif /* KERNEL */

#define MSFS_FIRST_ERR 1025

#define EOK                             (0)

#define ETAG_OVF                        (-1025)
#define EHANDLE_OVF                     (-1026)
#define EINVALID_HANDLE                 (-1027)
#define E_DOMAIN_PANIC                  (-1028)
#define EDUP_VD                         (-1029)
#define EBAD_VDI                        (-1030)
#define ENO_SUCH_DOMAIN                 (-1031)
#define ENO_SUCH_TAG                    (-1032)
#define ENO_MORE_DOMAINS                (-1033)
#define EBAD_DOMAIN_POINTER              (-1034)
#define E_PAGE_NOT_MAPPED               (-1035)
#define ENO_XTNTS                       (-1036)
#define ENO_BS_ATTR                     (-1037)
#define EBAD_TAG                        (-1038)
#define EXTND_FAILURE                   (-1039)
#define ENO_MORE_BLKS                   (-1040)
#define ENOT_SUPPORTED                  (-1041)
#define EBAD_PARAMS                     (-1042)
#define EBMTR_NOT_FOUND                 (-1043)
#define ETOO_MANY_SERVICE_CLASSES       (-1044)
#define EBAD_SERVICE_CLASS              (-1045)
#define ESERVICE_CLASS_NOT_FOUND        (-1046)
#define ENO_SERVICE_CLASSES             (-1047)
#define ESERVICE_CLASS_NOT_EMPTY        (-1048)
#define EBAD_FTX_AGENTH                 (-1049)
#define EBAD_PAR_FTXH                   (-1050)
#define EFTX_TOO_DEEP                   (-1051)
#define EBAD_FTXH                       (-1052)
#define EBAD_STG_DESC                   (-1053)
#define ENO_MORE_MEMORY                 (-1054)
#define ENO_MORE_MCELLS                 (-1055)
#define EALREADY_ALLOCATED              (-1056)
#define E_INVALID_LOG_DESC_POINTER      (-1057)
#define E_CANT_CREATE_LOG               (-1058)
#define E_VOLUME_COUNT_MISMATCH         (-1059)
#define E_LOG_EMPTY                     (-1060)
#define ENO_SPACE_IN_MCELL              (-1061)
#define E_INVALID_REC_ADDR              (-1062)
#define W_LAST_LOG_REC                  (1063)
#define E_BAD_CLIENT_REC_ADDR           (-1064)
#define E_CANT_ACCESS_LOG               (-1065)
#define E_NO_MORE_RD_HANDLES            (-1066)
#define E_DOMAIN_NOT_ACTIVATED          (-1067)
#define E_DOMAIN_NOT_CLOSED             (-1068)
#define E_CANT_DISMOUNT_VD1_ACTIVE_DMN  (-1069)
#define E_BAD_VERSION                   (-1070)
#define E_VD_BFDMNID_DIFF               (-1071)
#define I_LOG_REC_SEGMENT               (1072)
#define I_LAST_LOG_REC_SEGMENT          (1073)
#define E_RANGE                         (-1074)
#define E_RDEV_MISMATCH                 (-1075)
#define E_EXCEEDS_EXTENT                (-1076)
#define E_CANT_FIND_LOG_END             (-1077)
#define E_BAD_MCELL_LINK_SEGMENT        (-1078)
#define E_VD_DMNATTR_DIFF               (-1079)
#define E_MUST_BE_ROOT                  (-1080)
#define E_RANGE_NOT_CLEARED             (-1081)
#define E_TAG_EXISTS                    (-1082)
#define E_NOMAP_INDEX                   (-1083)
#define E_RSVD_FILE_INWAY               (-1084)
#define E_ACTIVE_RANGE                  (-1085)
#define E_DOMAIN_ALREADY_EXISTS         (-1086)
#define E_LAST_PAGE                     (-1087)
#define E_1088                          (-1088)
#define E_1089                          (-1089)
#define E_CORRUPT_LIST                  (-1090)
#define W_1091                          (1091)
#define E_1092                          (-1092)
#define E_NO_SUCH_BF_SET                (-1093)
#define E_BAD_BF_SET_POINTER            (-1094)
#define E_TOO_MANY_BF_SETS              (-1095)
#define E_1096                          (-1096)
#define E_BF_SET_NOT_CLOSED             (-1097)
#define E_1098                          (-1098)
#define E_BLKDESC_ARRAY_TOO_SMALL       (-1099)
#define E_TOO_MANY_ACCESSORS            (-1100)
#define E_1101                          (-1101)
#define E_XTNT_MAP_NOT_FOUND            (-1102)
#define E_ALREADY_SHADOWED              (-1103)
#define E_NOT_ENOUGH_DISKS              (-1104)
#define E_STG_ADDED_DURING_SHADOW_OPER  (-1105)
#define E_STG_ADDED_DURING_MIGRATE_OPER (-1106)
#define E_BAD_DEV                       (-1107)
#define E_ADVFS_NOT_INSTALLED           (-1108)
#define E_STGDESC_NOT_FOUND             (-1109)
#define E_FULL_INTERSECT                (-1110)
#define E_PARTIAL_INTERSECT             (-1111)
#define E_SPACE_ALREADY_RESERVED        (-1112)
#define E_SPACE_NOT_RESERVED            (-1113)
#define E_READ_ONLY                     (-1114)
#define E_TOO_MANY_CLONES               (-1115)
#define E_OUT_OF_SYNC_CLONE             (-1116)
#define E_HAS_CLONE                     (-1117)
#define E_TOO_MANY_BITFILES             (-1118)
#define E_TOO_MANY_BLOCKS               (-1119)
#define E_QUOTA_NOT_ENABLED             (-1120)
#define E_IO                            (-1121)
#define E_NO_MORE_DQUOTS                (-1122)
#define E_NO_MORE_SETS                  (-1123)
#define E_DUPLICATE_SET                 (-1124)
#define E_CANT_CLONE_A_CLONE            (-1125)
#define E_ACCESS_DENIED                 (-1126)
#define E_BLKOFFSET_NOT_PAGE_ALIGNED    (-1127)
#define E_BLKOFFSET_NOT_CLUSTER_ALIGNED (-1128)
#define E_INVALID_BLK_RANGE             (-1129)
#define E_CANT_MIGRATE_HOLE             (-1130)
#define E_NOT_OWNER                     (-1131)
#define E_MIGRATE_IN_PROGRESS           (-1132)
#define E_SHADOW_IN_PROGRESS            (-1133)
#define E_CANT_MIGRATE_SHADOW           (-1134)
#define E_INVALID_FS_VERSION            (-1135)
#define E_INVALID_MSFS_STRUCT           (-1136)
#define E_NOT_DIRECTORY                 (-1137)
#define I_FILE_EXISTS                   (-1138)
#define I_INSERT_HERE                   (-1139)
#define I_FOUND_DELETED                 (-1140)
#define I_FILE_NOT_FOUND                (-1141)
#define E_LOOKUP_ERROR                  (-1142)
#define E_1143                          (-1143)
#define E_PAGE_HOLE                     (-1144)
#define E_TOO_MANY_VIRTUAL_DISKS        (-1145)
#define E_RESERVED_1                    (-1146)
#define E_RESERVED_2                    (-1147)
#define E_RESERVED_3                    (-1148)
#define E_RESERVED_4                    (-1149)
#define E_RESERVED_5                    (-1150)
#define E_RESERVED_6                    (-1151)
#define E_RESERVED_7                    (-1152)
#define E_RESERVED_8                    (-1153)
#define E_RESERVED_9                    (-1154)
#define E_RESERVED_10                   (-1155)
#define E_RESERVED_11                   (-1156)
#define E_RESERVED_12                   (-1157)
#define E_RESERVED_13                   (-1158)
#define E_RESERVED_14                   (-1159)
#define E_RESERVED_15                   (-1160)
#define E_RESERVED_16                   (-1161)
#define ENO_NAME                        (-1162)
#define EDIR_NOT_ADVFS                  (-1163)
#define EUNDEL_DIR_NOT_ADVFS            (-1164)
#define EDUPLICATE_DIRS                 (-1165)
#define EDIFF_FILE_SETS                 (-1166)
#define E_BAD_MAGIC                     (-1167)
#define E_BAD_PAGESIZE                  (-1168)
#define E_NO_DMN_VOLS                   (-1169)
#define E_BAD_TAGDIR                    (-1170)
#define E_BAD_BMT                       (-1171)
#define E_ALREADY_SWITCHING_LOGS        (-1172)
#define E_NO_UNDEL_DIR                  (-1173)
#define E_QUOTA_NOT_MAINTAINED          (-1174)
#define E_BMT_NOT_EMPTY                 (-1175)
#define E_ROOT_TAGDIR_ON_VOL            (-1176)
#define E_LOG_ON_VOL                    (-1177)
#define E_VOL_NOT_IN_SVC_CLASS          (-1178)
#define E_WOULD_BLOCK                   (-1179)
#define W_NOT_WRIT                      (+1180)
#define E_FRAG_PAGE                     (-1181)
#define E_ALREADY_STRIPED               (-1182)
#define E_NO_CLONE_STG                  (-1183)
#define E_BAD_PAGE_RANGE                (-1184)
#define E_INVOLUNTARY_ABORT             (-1185)
#define W_1186                          (+1186)
#define E_RMVOL_ALREADY_INPROG          (-1187)
#define E_TOO_LARGE_SPARSE              (-1188)
#define E_NOT_ENOUGH_XTNTS              (-1189)
#define E_INDEX_PRUNE_FAILED            (-1190)
#define E_ILLEGAL_CLONE_OP              (-1191)
/* NOTE: Update MSFS_LAST_ERR when adding a new error */
#define MSFS_LAST_ERR                   (-1191)
/* NOTE: Update MSFS_LAST_ERR when adding a new error */

/*********************************************************************
 *********************************************************************
 ***** WHEN ADDING A NEW ERROR NUMBER BE SURE TO ADD A MESSAGE   *****
 ***** STRING TO BS_ERRLIST IN bs/bs_errlst.c.  ALSO ADD A       *****
 ***** MAPPING TO AN OSF ERRNO IN THE SAME FILE.                 *****
 ***** ***************************************************************
 *********************************************************************/

/*
 * bs_errlist - 
 *
 * This array can be used to print the error messages that
 * correspond the error numbers defined above.  The following illustrates
 * how bs_errlist can be used in a program:
 *
 *     printf( "create error %s\n", BSERRMSG( sts ) );
 *
 * The macro BSERRMSG() can be used to print the error messages.
 */

extern char *bs_errlist[];           /* bs_errlst.c */

#define BSERRMSG( sts ) advfs_errmsg( sts )

char * advfs_errmsg( int sts );


extern int sts_to_errno_map[];      /* bs_errlst.c */

#define BSERRMAP( sts ) ((abs( sts ) < MSFS_FIRST_ERR) ? \
                         sts : \
                         ((abs( sts ) > abs( MSFS_LAST_ERR )) ? \
                         sts : \
                         sts_to_errno_map[abs( sts ) - MSFS_FIRST_ERR]))

#endif /* _BS_ERROR_H_ */
