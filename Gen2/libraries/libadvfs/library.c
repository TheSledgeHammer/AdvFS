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
 *
 * Facility:
 *
 *      ADVFS Storage System
 *
 * Abstract:
 *
 *      ADVFS system call interface library.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <dirent.h>
#include <unistd.h>           /* needed for lseek */
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/fs.h>           /* need by advfs_valid_sblock */
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/pstat.h>        /* for pstat_getproc() */
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/diskio.h>       /* needed to query disk for size */
#include <lvm/pvres.h>        /* for advfs_is_lvmdev() */
#include <mntent.h>
#include <fcntl.h>
#include <limits.h>           /* Changed from sys/limits.h  1/7/03 */
#include <ftw.h>              /* needed for ftw() */
#include <time.h>
#include <sys/syscall.h>
#include <advfs/advfs_syscalls.h> 
#include <advfs/bs_error.h>  /* needed for error macros */
#include <tcr/clu.h>
#include "libadvfs_msg.h"

typedef struct _dName {
    char name[256];
    struct _dName *next;
} dNameT;

extern char *sys_errlist[];

#define advfs_syscall(a,b,c) syscall(SYS_advfs_syscall, (a), (b), (c))
#define ERRMSG( e ) sys_errlist[e]

/* __blocktochar() and __chartoblock() are libc functions that convert 
 * a blocks special device to a character special device and vice versa
 */
extern char *__blocktochar(char *blockp, char *charp);
extern char *__chartoblock(char *charp, char *blockp);
/*
 * All calling applications which could potentially corrupt
 * versions of metadata that were introduced after the
 * applications themselves were built must first go through 
 * advfs_check_on_disk_version() to set this to TRUE
 * before making other library calls.
 */
static int checked_on_disk_version = FALSE;

/* globals used when finding device minor number and creating AdvFS
 * pseudodevice 
 */
#define ADVDEV_MINOR_MAX 0x3FFFFF  /*  22 bits */
#define ADVDEV_IS_TCRDEV(x) ((x | (1 << 22)) == x)
#define ADVDEV_SET_TCRDEV(x) (x |= (1 << 22))
#define ADVDEV_CLEAR_TCRDEV(x) (x &= ~(1 << 22))
#define ADVDEV_IS_STRIPEDEV(x) ((x | (1 << 23)) == x)
#define ADVDEV_SET_STRIPEDEV(x) (x |= (1 << 23))
#define ADVDEV_CLEAR_STRIPEDEV(x) (x &= ~(1 << 23))
static uint32_t   hi_devno = 0;
static uint32_t   lo_devno = 0;
static uint32_t   hi_stripedevno = 0;
static uint32_t   lo_stripedevno = 0;

static int adv_get_minor_number(int);
static int adv_do_ftw(const char *obj_path, const struct stat *obj_stat,
                      int obj_flags, struct FTW obj_FTW);
static int do_lock_file(char *fileName, int lkMode, int lkType,
                        int block, int *error);
static int do_unlock_file(int lockHndl, int lkType, int *error);
static long find_min_fsize(int fd);

/* List of directories where AdvFS filesystems are
 * stored/created
 */
char* advfs_global_dirs[3] = {ADVFS_TCR_DMN_DIR, ADVFS_DMN_DIR, NULL};

/* 
 * Check that the caller knows about the on-disk structures
 * on this system.
 *
 * Since there is no locking done around checking and setting of
 * checked_on_disk_version, this library, or at least those calls that 
 * use this variable, are not thread-safe. There is no problem with 
 * this right now due to the check-once way it is used.
 */
adv_status_t 
advfs_check_on_disk_version( 
        uint16_t odv_major,
        uint16_t odv_minor
    )
{
    
/* NOTE: assumes that differences in the minor version number are handled
 *       or can safely be ignored
 */
    if (odv_major !=  BFD_ODS_LAST_VERSION_MAJOR) {
        return E_INVALID_FS_VERSION;
    }
    else {
        checked_on_disk_version = TRUE;
        return EOK;
    }
}

/* 
 * Allow the caller to bypass on-disk structure checks.
 */
adv_status_t 
advfs_skip_on_disk_version_check()
{
    checked_on_disk_version = TRUE;
    return EOK;
}

static adv_status_t 
libadvfs_syscall(
    opTypeT opType,
    libParamsT *parms,
    int parmsLen
    )
{
    nl_catd catd;
    adv_status_t sts;

#ifdef DEBUG
    printf("libadvfs_syscall: arg1 - %d arg2 - 0x%lx arg3 - %d\n",
            opType, parms, parmsLen);
#endif
    sts = advfs_syscall( opType, parms, parmsLen );

    if (sts == -1) {

        if (errno == ENOSYS) {
            return E_ADVFS_NOT_INSTALLED;

        } else {
            return errno;
        }

    } else {
        return sts;
    }
}

/* Packs the parameters from user mode and calls advfs_syscall to
   transfer them to kernel mode */


adv_status_t
advfs_undel_attach(
    char *dirName,
    char *undelDirName
    )
{
    libParamsT buf;
    char tmp_dirName[MAXPATHLEN+1];
    char tmp_undelDirName[MAXPATHLEN+1];

    if (realpath(dirName, tmp_dirName) == NULL)
	return(EBAD_PARAMS);

    if (realpath(undelDirName, tmp_undelDirName) == NULL)
	return(EBAD_PARAMS);

    bzero( &buf, sizeof(libParamsT));
    buf.undelAttach.dirName = tmp_dirName;
    buf.undelAttach.undelDirName = tmp_undelDirName;

    return libadvfs_syscall(ADVFS_UNDEL_ATTACH, &buf, sizeof( buf.undelAttach ));
}

adv_status_t
advfs_undel_detach(
    char *dirName
    )
{
    libParamsT buf;
    char tmp_dirName[MAXPATHLEN+1];

    if (realpath(dirName, tmp_dirName) == NULL)
	return(EBAD_PARAMS);

    bzero( &buf, sizeof(libParamsT));
    buf.undelDetach.dirName = tmp_dirName;

    return libadvfs_syscall(ADVFS_UNDEL_DETACH, &buf, sizeof( buf.undelDetach ));
}

adv_status_t 
advfs_undel_get(
    char   *dirName,          /* in  */
    bfTagT *undelDirTag       /* out */
    )
{
    libParamsT buf;
    adv_status_t  sts;
    char tmp_dirName[MAXPATHLEN+1];

    if (realpath(dirName, tmp_dirName) == NULL)
	return(EBAD_PARAMS);

    bzero( &buf, sizeof(libParamsT));
    buf.undelGet.dirName = tmp_dirName;
    buf.undelGet.undelDirTag = *undelDirTag;

    sts = libadvfs_syscall(ADVFS_UNDEL_GET, &buf, sizeof( buf.undelGet ));

    *undelDirTag = buf.undelGet.undelDirTag;

    return sts;
}


adv_status_t 
advfs_get_name(
    char *mp,           /* in  */
    bfTagT tag,         /* in */
    char *name,         /* out  */
    bfTagT *parentTag   /* out */
    )
{
    libParamsT   buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getName.mp = mp;
    buf.getName.name = name;
    buf.getName.tag = tag;
    buf.getName.parentTag = *parentTag;

    sts = libadvfs_syscall(ADVFS_GET_NAME, &buf, sizeof( buf.getName ));

    *parentTag = buf.getName.parentTag;

    return sts;
}


adv_status_t 
advfs_get_idx_bf_params (
    int          fd,
    adv_bf_attr_t  *bfAttributes,
    adv_bf_info_t* bfInfo
    ) 
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getBfParams.fd = fd;
    buf.getBfParams.bfAttributes = *bfAttributes;
    buf.getBfParams.bfInfo = *bfInfo;

    sts = libadvfs_syscall(ADVFS_GET_IDX_BF_PARAMS, &buf, sizeof(buf.getBfParams));

    *bfAttributes = buf.getBfParams.bfAttributes;
    *bfInfo = buf.getBfParams.bfInfo;

    return sts;
}

adv_status_t 
advfs_get_bf_params (
    int          fd,
    adv_bf_attr_t  *bfAttributes,
    adv_bf_info_t* bfInfo
    ) 
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getBfParams.fd = fd;
    buf.getBfParams.bfAttributes = *bfAttributes;
    buf.getBfParams.bfInfo = *bfInfo;

    sts = libadvfs_syscall(ADVFS_GET_BF_PARAMS, &buf, sizeof(buf.getBfParams));

    *bfAttributes = buf.getBfParams.bfAttributes;
    *bfInfo = buf.getBfParams.bfInfo;

    return sts;
}

adv_status_t 
advfs_set_bf_attributes (
    int          fd,
    adv_bf_attr_t  *bfAttributes
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setBfAttributes.fd = fd;
    buf.setBfAttributes.bfAttributes = *bfAttributes;

    sts = libadvfs_syscall(ADVFS_SET_BF_ATTRIBUTES,
                         &buf, 
                         sizeof( buf.setBfAttributes ));

    *bfAttributes = buf.setBfAttributes.bfAttributes;

    return sts;
}

adv_status_t 
advfs_get_bf_iattributes (
    int          fd,
    adv_bf_attr_t  *bfAttributes
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getBfIAttributes.fd = fd;

    sts = libadvfs_syscall(ADVFS_GET_BF_IATTRIBUTES,
                         &buf, 
                         sizeof( buf.getBfIAttributes ));

    *bfAttributes = buf.getBfIAttributes.bfAttributes;

    return sts;
}

adv_status_t 
advfs_set_bf_iattributes (
    int          fd,
    adv_bf_attr_t  *bfAttributes
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setBfIAttributes.fd = fd;
    buf.setBfIAttributes.bfAttributes = *bfAttributes;

    sts = libadvfs_syscall(ADVFS_SET_BF_IATTRIBUTES,
                         &buf, 
                         sizeof( buf.setBfIAttributes ));

    *bfAttributes = buf.setBfIAttributes.bfAttributes;

    return sts;
}

adv_status_t 
advfs_move_bf_metadata (
    int fd
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.moveBfMetadata.fd = fd;

    return libadvfs_syscall (
                         ADVFS_MOVE_BF_METADATA,
                         &buf, 
                         sizeof( buf.moveBfMetadata )
                         );
}

adv_status_t 
advfs_switch_log(
    char *domainName,
    vdIndexT volIndex,
    bf_fob_t logPgs, /* should be switched to logFobs */
    serviceClassT logSvc
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.switchLog.domainName = domainName;
    buf.switchLog.volIndex = volIndex;
    buf.switchLog.logPgs = logPgs;
    buf.switchLog.logSvc = logSvc;

    return libadvfs_syscall(ADVFS_SWITCH_LOG, &buf, sizeof( buf.switchLog ));
}

adv_status_t 
advfs_switch_root_tagdir(
    char *domainName,
    vdIndexT volIndex
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.switchRootTagDir.domainName = domainName;
    buf.switchRootTagDir.volIndex = volIndex;

    return libadvfs_syscall(ADVFS_SWITCH_ROOT_TAGDIR, &buf, sizeof( buf.switchRootTagDir ));
}

adv_status_t 
advfs_get_vol_params (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    adv_vol_info_t* volInfop,
    adv_vol_prop_t* volPropertiesp,
    adv_vol_ioq_params_t* volIoQParamsp,
    adv_vol_counters_t* volCountersp
    )
{
    libParamsT buf;
    adv_status_t sts;
    int flag = 0;

    bzero( &buf, sizeof(libParamsT));
    buf.getVolParams.bfDomainId = bfDomainId;
    buf.getVolParams.volIndex = volIndex;
    if (!volInfop) flag |= GETVOLPARAMS_NO_BMT;
    buf.getVolParams.flag = flag;

    sts = libadvfs_syscall(ADVFS_GET_VOL_PARAMS, 
                         &buf, 
                         sizeof( buf.getVolParams ));

    if (volInfop) *volInfop = buf.getVolParams.volInfo;
    *volPropertiesp = buf.getVolParams.volProperties;
    *volIoQParamsp = buf.getVolParams.volIoQParams;
    *volCountersp = buf.getVolParams.volCounters;

    return sts;
}

adv_status_t 
advfs_get_vol_params2 (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    adv_vol_info_t* volInfop,
    adv_vol_prop_t* volPropertiesp,
    adv_vol_ioq_params_t* volIoQParamsp,
    adv_vol_counters_t* volCountersp,
    int flag
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getVolParams.bfDomainId = bfDomainId;
    buf.getVolParams.volIndex = volIndex;
    if (!volInfop) flag |= GETVOLPARAMS_NO_BMT;
    buf.getVolParams.flag = flag;

    sts = libadvfs_syscall(ADVFS_GET_VOL_PARAMS2, 
                         &buf, 
                         sizeof( buf.getVolParams ));

    if (volInfop) *volInfop = buf.getVolParams.volInfo;
    *volPropertiesp = buf.getVolParams.volProperties;
    *volIoQParamsp = buf.getVolParams.volIoQParams;
    *volCountersp = buf.getVolParams.volCounters;

    return sts;
}

adv_status_t 
advfs_get_vol_bf_descs (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    int bfDescSize,
    bsBfDescT *bfDesc,
    bfMCIdT *nextBfDescId,
    int *bfDescCnt
    )

{
    libParamsT buf;
    adv_status_t sts;

    /*
     * This entry point, used by rmvol, defragment, and balance,
     * returns a representation of BMT mcells to the caller.
     * Since the meaning of this representation may change between
     * on-disk versions, the caller must first register which
     * domain versions it knows about.
     */
    if (!checked_on_disk_version) {
        return E_INVALID_FS_VERSION;
    }

    bzero( &buf, sizeof(libParamsT));
    buf.getVolBfDescs.bfDomainId = bfDomainId;
    buf.getVolBfDescs.volIndex = volIndex;
    buf.getVolBfDescs.bfDescSize = bfDescSize;
    buf.getVolBfDescs.bfDesc = bfDesc;
    buf.getVolBfDescs.nextBfDescId = *nextBfDescId;

    sts = libadvfs_syscall (
                   ADVFS_GET_VOL_BF_DESCS, 
                   &buf, 
                   sizeof( buf.getVolBfDescs )
                   );

    *nextBfDescId = buf.getVolBfDescs.nextBfDescId;
    *bfDescCnt = buf.getVolBfDescs.bfDescCnt;

    return sts;
}

adv_status_t
advfs_set_vol_ioq_params (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    adv_vol_ioq_params_t *volIoQParamsp
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.setVolIoQParams.bfDomainId = bfDomainId;
    buf.setVolIoQParams.volIndex = volIndex;
    buf.setVolIoQParams.volIoQParams = *volIoQParamsp;

    return libadvfs_syscall(ADVFS_SET_VOL_IOQ_PARAMS, 
                        &buf, 
                        sizeof( buf.setVolIoQParams ));
}

adv_status_t 
advfs_get_bf_xtnt_map (
    int fd,
    int startXtntMap,
    int startXtnt,
    int xtntArraySize,
    bsExtentDescT *xtntsArray,
    int *xtntCnt,
    vdIndexT *allocVolIndex
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getXtntMap.fd = fd;
    buf.getXtntMap.startXtntMap = startXtntMap;
    buf.getXtntMap.startXtnt = startXtnt;
    buf.getXtntMap.xtntArraySize = xtntArraySize;
    buf.getXtntMap.xtntsArray = xtntsArray;

    sts = libadvfs_syscall(ADVFS_GET_XTNT_MAP, 
                         &buf, 
                         sizeof( buf.getXtntMap ));

    *xtntCnt = buf.getXtntMap.xtntCnt;
    *allocVolIndex = buf.getXtntMap.allocVolIndex;

    return sts;
}

/*
 * This routine is pretty much the same thing as advfs_get_xtnt_map except
 * this returns the fob count.  The two should be merged but we don't want
 * to break compatability.
 */
adv_status_t 
advfs_get_bkup_xtnt_map (
    int fd,                    /* IN */
    int startXtntMap,          /* IN */
    int startXtnt,             /* IN */
    int xtntArraySize,         /* IN - If set to 0, then only returns count */
    bsExtentDescT *xtntsArray, /* OUT */
    int *xtntCnt,              /* OUT - Number of extent entries in map */
    uint32_t *allocVolIndex,   /* OUT - Disk on which next stg is alloc'd */
    bf_fob_t *num_fobs         /* OUT - Number of fobs */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getXtntMap.fd = fd;
    buf.getXtntMap.startXtntMap = startXtntMap;
    buf.getXtntMap.startXtnt = startXtnt;
    buf.getXtntMap.xtntArraySize = xtntArraySize;
    buf.getXtntMap.xtntsArray = xtntsArray;

    sts = libadvfs_syscall(ADVFS_GET_XTNT_MAP, 
                         &buf, 
                         sizeof( buf.getXtntMap ));

    *xtntCnt = buf.getXtntMap.xtntCnt;
    *allocVolIndex = buf.getXtntMap.allocVolIndex;
    *num_fobs = buf.getXtntMap.gxm_fob_cnt;

    return sts;
}

adv_status_t 
advfs_get_lock_stats( adv_lock_stats_t *lockStats )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    sts = libadvfs_syscall(ADVFS_GET_LOCK_STATS, 
                  &buf, 
                  sizeof( buf.getLockStats ));

    *lockStats = buf.getLockStats;

    return sts;
}

adv_status_t 
advfs_get_dmn_params(
    bfDomainIdT bfDomainId,
    adv_bf_dmn_params_t *dmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getDmnParams.bfDomainId = bfDomainId;
    buf.getDmnParams.dmnParams = *dmnParams;

    sts = libadvfs_syscall(ADVFS_GET_DMN_PARAMS, 
                         &buf, 
                         sizeof( buf.getDmnParams ));

    *dmnParams = buf.getDmnParams.dmnParams;

    return sts;
}

adv_status_t 
advfs_get_dmnname_params(
    char *domain,
    adv_bf_dmn_params_t *dmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    /*
     * This entry point requires that the on-disk version be
     * checked.  The only reason is that the verify utility,
     * which has the potential to corrupt newer version
     * domains, calls this.  So the checking here prevents
     * an older version of verify from corrupting a newer
     * domain if the -f flag is used.  This causes some
     * other utilities which use this entry point to call either
     * advfs_check_on_disk_version() or advfs_skip_on_disk_version_check().
     * It also prevents older versions of some innocuous 
     * utilities running on newer domains.
     */
    if (!checked_on_disk_version) {
        return E_INVALID_FS_VERSION;
    }

    bzero( &buf, sizeof(libParamsT));
    buf.getDmnNameParams.domain = domain;
    buf.getDmnNameParams.dmnParams = *dmnParams;

    sts = libadvfs_syscall(ADVFS_GET_DMNNAME_PARAMS, 
                    &buf, 
                    sizeof( buf.getDmnNameParams ));

    *dmnParams = buf.getDmnNameParams.dmnParams;

    return sts;
}

adv_status_t
advfs_set_dmnname_params(
    char *domain,
    adv_bf_dmn_params_t *dmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    /*
     * This entry point requires that the on-disk version be
     * checked.  The only reason is that the verify utility,
     * which has the potential to corrupt newer version
     * domains, calls this.  So the checking here prevents
     * an older version of verify from corrupting a newer
     * domain if the -f flag is used.  This causes some
     * other utilities which use this entry point to call either
     * advfs_check_on_disk_version() or advfs_skip_on_disk_version_check().
     * It also prevents older versions of some innocuous 
     * utilities such as showfdmn from running on newer domains.
     */
    if (!checked_on_disk_version) {
        return E_INVALID_FS_VERSION;
    }

    bzero( &buf, sizeof(libParamsT));
    buf.setDmnNameParams.domain    =  domain;
    buf.setDmnNameParams.dmnParams = *dmnParams;

    sts = libadvfs_syscall(ADVFS_SET_DMNNAME_PARAMS,
                    &buf,
                    sizeof( buf.setDmnNameParams ));

    *dmnParams = buf.setDmnNameParams.dmnParams;

    return sts;
}

adv_status_t
advfs_get_dmn_vol_list(
    bfDomainIdT bfDomainId,  /* in - the domain id */
    int volIndexArrayLen,    /* in - number of ints in array */
    vdIndexT volIndexArray[],/* out - list of volume indices */
    int *numVols             /* out - num volumes put in array */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getDmnVolList.bfDomainId = bfDomainId;
    buf.getDmnVolList.volIndexArrayLen = volIndexArrayLen;
    buf.getDmnVolList.volIndexArray = volIndexArray;

    sts = libadvfs_syscall(ADVFS_GET_DMN_VOL_LIST, 
                         &buf, 
                         sizeof( buf.getDmnVolList) );

    *numVols = buf.getDmnVolList.numVols;

    return sts;
}

adv_status_t 
advfs_get_bfset_params(
    bfSetIdT bfSetId,
    bfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getBfSetParams.bfSetId = bfSetId;
    buf.getBfSetParams.bfSetParams = *bfSetParams;

    sts = libadvfs_syscall(ADVFS_GET_BFSET_PARAMS, 
                         &buf, 
                         sizeof( buf.getBfSetParams ));

    *bfSetParams = buf.getBfSetParams.bfSetParams;

    return sts;
}

adv_status_t 
advfs_set_bfset_params(
    bfSetIdT bfSetId,
    bfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setBfSetParams.bfSetId = bfSetId;
    buf.setBfSetParams.bfSetParams = *bfSetParams;

    sts = libadvfs_syscall(ADVFS_SET_BFSET_PARAMS, 
                         &buf, 
                         sizeof( buf.setBfSetParams ));

    *bfSetParams = buf.setBfSetParams.bfSetParams;

    return sts;
}

adv_status_t
advfs_set_bfset_params_activate(
    char *dmnName,
    bfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setBfSetParamsActivate.dmnName = dmnName;
    buf.setBfSetParamsActivate.bfSetParams = *bfSetParams;

    sts = libadvfs_syscall(ADVFS_SET_BFSET_PARAMS_ACTIVATE,
                         &buf,
                         sizeof( buf.setBfSetParamsActivate ));

    *bfSetParams = buf.setBfSetParamsActivate.bfSetParams;

    return sts;
}


/* This routine will activate the domain and get the bfSetParams
 *  for the specified fileset
 */
adv_status_t 
advfs_lib_get_bfset_params_activate(
        char *dmnName,
        bfSetIdT bfSetId,
        bfSetParamsT *bfSetParams
        )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.getBfSetParamsAct.dmnName = dmnName;
    buf.getBfSetParamsAct.bfSetId = bfSetId;
    buf.getBfSetParamsAct.bfSetParams = *bfSetParams;

    sts = libadvfs_syscall(ADVFS_GET_BFSET_PARAMS_ACT, 
                         &buf, 
                         sizeof( buf.getBfSetParamsAct ));

    *bfSetParams = buf.getBfSetParamsAct.bfSetParams;

    return sts;
}

adv_status_t
advfs_fset_create( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    serviceClassT reqServ,    /* in - required service class */
    uint32_t fsetOptions,     /* in - fileset options */
    adv_gid_t quotaId,        /* in - group ID for quota files */
    bfSetIdT *bfSetId         /* out - bitfile set id */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setCreate.domain = domain;
    buf.setCreate.setName = setName;
    buf.setCreate.reqServ = reqServ;
    buf.setCreate.fsetOptions = fsetOptions;
    buf.setCreate.quotaId = quotaId;
    buf.setCreate.bfSetId = *bfSetId;

    sts = libadvfs_syscall(ADVFS_FSET_CREATE,
                         &buf, 
                         sizeof( buf.setCreate ));

    *bfSetId = buf.setCreate.bfSetId;

    return sts;
}

adv_status_t
advfs_fset_get_id( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    bfSetIdT *bfSetId         /* out - bitfile set id */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setGetId.domain = domain;
    buf.setGetId.setName = setName;
    buf.setGetId.bfSetId = *bfSetId;

    sts = libadvfs_syscall(ADVFS_FSET_GET_ID,
                         &buf, 
                         sizeof( buf.setGetId ));

    *bfSetId = buf.setGetId.bfSetId;

    return sts;
}

adv_status_t
advfs_fset_get_stats( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    adv_file_set_stats_t *fileSetStats
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setGetStats.domain = domain;
    buf.setGetStats.setName = setName;

    sts = libadvfs_syscall(ADVFS_FSET_GET_STATS,
                         &buf, 
                         sizeof( buf.setGetStats ));

    *fileSetStats = buf.setGetStats.fileSetStats;

    return sts;
}

adv_status_t
advfs_fset_delete(
    char *domain,             /* in - domain name */
    char *setName             /* in - set's name */
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.setCreate.domain = domain;
    buf.setCreate.setName = setName;

    return libadvfs_syscall(ADVFS_FSET_DELETE, &buf, sizeof( buf.setDelete ));
}

adv_status_t
advfs_lib_create_snapset(
        char *parent_dmn_name,          /* in - Parent fileset's domain name */
        char *parent_set_name,          /* in - Parent fileset name */
        char *snap_dmn_name,            /* in - Snapset's domain name */
        char *snap_set_name,            /* in - Snapset name */
        bf_snap_flags_t snap_flags,     /* in - Snapset creation flags */
        bfSetIdT *snap_set_id)        /* out - Snapset's id */
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.snapCreate.parent_dmn_name = parent_dmn_name;
    buf.snapCreate.parent_set_name = parent_set_name;
    buf.snapCreate.snap_dmn_name   = snap_dmn_name;
    buf.snapCreate.snap_set_name   = snap_set_name;
    buf.snapCreate.snap_flags      = snap_flags;
    buf.snapCreate.snap_set_id     = *snap_set_id;

    sts = libadvfs_syscall(ADVFS_SNAPSET_CREATE, 
            &buf, sizeof(buf.snapCreate));

    return sts;
}

adv_status_t 
advfs_fset_get_info(
    char           *domain,             /* in */
    uint64_t       *nextSetIdx,         /* in/out */
    bfSetParamsT *bfSetParams,        /* out */
    uint32_t       *userId,             /* out */
    int            flag                 /* in, 1=don't run recovery */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setGetInfo.domain = domain;
    buf.setGetInfo.nextSetIdx = *nextSetIdx;
    buf.setGetInfo.bfSetParams = *bfSetParams;
    buf.setGetInfo.flag = flag;

    sts = libadvfs_syscall(ADVFS_FSET_GET_INFO, 
                         &buf, 
                         sizeof( buf.setGetInfo ));

    *nextSetIdx = buf.setGetInfo.nextSetIdx;
    *bfSetParams = buf.setGetInfo.bfSetParams;
    *userId = buf.setGetInfo.userId;

    return sts;
}

adv_status_t
advfs_fset_rename(
    char      *domain,                /* in */
    char      *origSetName,           /* in */
    char      *newSetName             /* in */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.setRename.domain = domain;
    buf.setRename.origSetName = origSetName;
    buf.setRename.newSetName = newSetName;

    sts = libadvfs_syscall(ADVFS_FSET_RENAME,
                      &buf,
                      sizeof( buf.setRename ));
    return sts;
}

adv_status_t 
advfs_tag_stat(
    bfTagT         *tag,                /* in/out */
    bfSetIdT       setId,               /* in */
    adv_uid_t      *uid,                /* out */
    adv_gid_t      *gid,                /* out */
    adv_off_t      *size,               /* out */
    adv_time_t     *atime,              /* out */
    adv_mode_t     *mode                /* out */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.tagStat.setId = setId;
    buf.tagStat.tag = *tag;

    sts = libadvfs_syscall(ADVFS_TAG_STAT, &buf, sizeof( buf.tagStat ));

    *tag = buf.tagStat.tag;
    *uid = buf.tagStat.uid;
    *gid = buf.tagStat.gid;
    *size = buf.tagStat.size;
    *atime = buf.tagStat.atime;
    *mode = buf.tagStat.mode;

    return sts;
}

adv_status_t
advfs_dmn_init(
    char *domain,               /* in - bf domain name */
    int maxVols,                /* in - maximum number of virtual disks */
    bf_fob_t logFobs,           /* in - number of fob in log */
    bf_fob_t userAllocFobs,     /* in - number of fobs per user data page */
    serviceClassT logSvc,       /* in - log service attributes */
    serviceClassT tagSvc,       /* in - tag directory service attributes */
    char *volName,              /* in - block special device name */
    serviceClassT volSvc,       /* in - service class */
    bf_vd_blk_t volSize,        /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs to preallocate for BMT */
    adv_ondisk_version_t domainVersion, /* in - on-disk version of domain */
    bfDomainIdT *bfDomainId     /* out - domain id */
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.dmnInit.domain = domain;
    buf.dmnInit.maxVols = maxVols;
    buf.dmnInit.logFobs = logFobs;
    buf.dmnInit.userAllocFobs = userAllocFobs;
    buf.dmnInit.logSvc = logSvc;
    buf.dmnInit.tagSvc = tagSvc;
    buf.dmnInit.volName = volName;
    buf.dmnInit.volSvc = volSvc;
    buf.dmnInit.volSize = volSize;
    buf.dmnInit.bmtXtntFobs = bmtXtntFobs;
    buf.dmnInit.bmtPreallocFobs = bmtPreallocFobs;
    buf.dmnInit.domainVersion = domainVersion;

    sts = libadvfs_syscall(ADVFS_DMN_INIT, &buf, sizeof( buf.dmnInit ));

    *bfDomainId = buf.dmnInit.bfDomainId;

    return sts;
}

adv_status_t
advfs_get_statvfs(
    char *domainName,              /* in - domain name */
    char *setName,                 /* in - bitfile set name */
    struct statvfs *retStatvfs64   /* out - fill in for statvfsdev() */
    )
{
    libParamsT buf;
    adv_status_t sts;

    /*
     * The statvfs structure in user space is 8 bytes larger than the kernel
     * version; therefore, We'll create our own AdvFS internal version of
     * this structure to pass through the syscall.
     */

    bzero( &buf, sizeof(libParamsT));
    buf.getStatvfs.domainName = domainName;
    buf.getStatvfs.setName = setName;

    sts = libadvfs_syscall(ADVFS_GET_STATVFS,
			   &buf, sizeof( buf.getStatvfs ));
    if( sts != EOK ) {
	return sts;
    }

    retStatvfs64->f_blocks = buf.getStatvfs.advStatvfs.f_blocks;
    retStatvfs64->f_bfree = buf.getStatvfs.advStatvfs.f_bfree;
    retStatvfs64->f_bavail = buf.getStatvfs.advStatvfs.f_bavail;
    retStatvfs64->f_frsize = buf.getStatvfs.advStatvfs.f_frsize;
    retStatvfs64->f_files = buf.getStatvfs.advStatvfs.f_files;
    retStatvfs64->f_ffree = buf.getStatvfs.advStatvfs.f_ffree;
    retStatvfs64->f_type = buf.getStatvfs.advStatvfs.f_type;
    retStatvfs64->f_fsindex = buf.getStatvfs.advStatvfs.f_fsindex;
    retStatvfs64->f_magic = buf.getStatvfs.advStatvfs.f_magic;
    retStatvfs64->f_featurebits = buf.getStatvfs.advStatvfs.f_featurebits;
    retStatvfs64->f_bsize = buf.getStatvfs.advStatvfs.f_bsize;
    retStatvfs64->f_favail = buf.getStatvfs.advStatvfs.f_favail;
    retStatvfs64->f_namemax = buf.getStatvfs.advStatvfs.f_namemax;
    retStatvfs64->f_size = buf.getStatvfs.advStatvfs.f_size;
    retStatvfs64->f_time = buf.getStatvfs.advStatvfs.f_time;
    retStatvfs64->f_flag = buf.getStatvfs.advStatvfs.f_flag;
    retStatvfs64->f_fsid = buf.getStatvfs.advStatvfs.f_fsid;
    retStatvfs64->f_cnode = buf.getStatvfs.advStatvfs.f_cnode;
    /*
     * This field is not supported at this time.  It requires f_mnttoname.
     *
     * strcpy( retStatvfs64->f_fstr, buf.getStatvfs.advStatvfs.f_fsstr );
     */
    strcpy( retStatvfs64->f_basetype, buf.getStatvfs.advStatvfs.f_basetype );

    return sts;
}

adv_status_t 
advfs_migrate (
    int fd,
    vdIndexT srcVolIndex,
    bf_fob_t srcFobOffset, 
    bf_fob_t srcFobCnt, 
    uint32_t dstVolIndex,
    bf_vd_blk_t dstBlkOffset,
    uint32_t forceFlag
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.migrate.fd = fd;
    buf.migrate.srcVolIndex  = srcVolIndex;
    buf.migrate.srcFobOffset = srcFobOffset;
    buf.migrate.srcFobCnt    = srcFobCnt;
    buf.migrate.dstVolIndex  = dstVolIndex;
    buf.migrate.dstBlkOffset = dstBlkOffset;
    buf.migrate.forceFlag    = forceFlag;

    return libadvfs_syscall(ADVFS_MIGRATE, &buf, sizeof( buf.migrate ));
}

adv_status_t
advfs_add_volume( 
    char *domain,               /* in - domain name */
    char *volName,              /* in - block special device name */
    serviceClassT *volSvc,      /* in/out - service class */
    bf_vd_blk_t volSize,        /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs to be pre-allocated for BMT */
    bfDomainIdT *bfDomainId,    /* out - the domain id */
    vdIndexT *volIndex          /* out - the volume index */ 
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.addVol.domain          = domain;
    buf.addVol.volName         = volName;
    buf.addVol.volSvc          = *volSvc;
    buf.addVol.volSize         = volSize;
    buf.addVol.bmtXtntFobs     = bmtXtntFobs;
    buf.addVol.bmtPreallocFobs = bmtPreallocFobs;

    sts = libadvfs_syscall(ADVFS_ADD_VOLUME, &buf, sizeof(buf.addVol));

    *bfDomainId = buf.addVol.bfDomainId;
    *volIndex = buf.addVol.volIndex;
    *volSvc = buf.addVol.volSvc;

    return sts;
}

adv_status_t
advfs_add_vol_done(
    char *domainName,           /* in - domain name */
    char *volName               /* in - block special device name */
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.addRemVolDone.domainName = domainName;
    buf.addRemVolDone.volName = volName;
    buf.addRemVolDone.op = BSR_VD_ADDVOL;

    return libadvfs_syscall(ADVFS_ADD_REM_VOL_DONE, &buf, sizeof(buf.addRemVolDone));
}   

adv_status_t
advfs_rem_vol_done(
    char *domainName,           /* in - domain name */
    char *volName               /* in - block special device name */
    )
{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.addRemVolDone.domainName = domainName;
    buf.addRemVolDone.volName = volName;
    buf.addRemVolDone.op = BSR_VD_RMVOL;

    return libadvfs_syscall(ADVFS_ADD_REM_VOL_DONE, &buf, sizeof(buf.addRemVolDone));
}   

adv_status_t
advfs_remove_volume( 
    bfDomainIdT bfDomainId,   /* in */
    vdIndexT volIndex,        /* in */
    uint32_t forceFlag        /* in */
    )

{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.remVol.bfDomainId = bfDomainId;
    buf.remVol.volIndex = volIndex;
    buf.remVol.forceFlag = forceFlag;

    return libadvfs_syscall (
                    ADVFS_REM_VOLUME,
                    &buf,
                    sizeof(buf.remVol)
                    );
}

adv_status_t
advfs_add_vol_to_svc_class (
    char *domainName,
    vdIndexT volIndex,
    serviceClassT volSvc
    )

{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.addRemVolSvcClass.domainName = domainName;
    buf.addRemVolSvcClass.volIndex = volIndex;
    buf.addRemVolSvcClass.volSvc = volSvc;
    buf.addRemVolSvcClass.action = 0;

    return libadvfs_syscall (
                    ADVFS_ADD_REM_VOL_SVC_CLASS,
                    &buf,
                    sizeof(buf.addRemVolSvcClass)
                    );
}

adv_status_t
advfs_remove_vol_from_svc_class (
    char *domainName,
    vdIndexT volIndex,
    serviceClassT volSvc
    )

{
    libParamsT buf;

    bzero( &buf, sizeof(libParamsT));
    buf.addRemVolSvcClass.domainName = domainName;
    buf.addRemVolSvcClass.volIndex = volIndex;
    buf.addRemVolSvcClass.volSvc = volSvc;
    buf.addRemVolSvcClass.action = 1;

    return libadvfs_syscall (
                    ADVFS_ADD_REM_VOL_SVC_CLASS,
                    &buf,
                    sizeof(buf.addRemVolSvcClass)
                    );
}

adv_status_t
advfs_reset_free_space_cache (
        bfDomainIdT bfDomainId,
        vdIndexT volIndex,
        bf_vd_blk_t scanStartCluster)
{
    libParamsT buf;

        bzero( &buf, sizeof(libParamsT));
        buf.resetFreeSpaceCache.bfDomainId = bfDomainId;
        buf.resetFreeSpaceCache.volIndex = volIndex;
        buf.resetFreeSpaceCache.scanStartCluster = scanStartCluster;

        return libadvfs_syscall (
                                                 ADVFS_RESET_FREE_SPACE_CACHE,
                                                 &buf,
                                                 sizeof( buf.resetFreeSpaceCache)
                                                 );
}

adv_status_t
advfs_get_global_stats(
    GetGlobalStatsT *stats)      /* out */
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    sts = libadvfs_syscall (
                        ADVFS_GET_GLOBAL_STATS,
                        &buf,
                        sizeof(buf.getGlobalStats)
                       );
    *stats = buf.getGlobalStats;
    return sts;
}

adv_status_t
advfs_extendfs(
    char *domain,               /* in */
    char *blkdev,               /* in */
    uint64_t size,              /* in */
    int32_t flags,              /* in */
    bf_vd_blk_t *oldBlkSize,    /* out */
    bf_vd_blk_t *newBlkSize)    /* out */
{
    libParamsT buf;
    int status;

    bzero( &buf, sizeof(libParamsT));
    buf.extendFsParams.domain = domain;
    buf.extendFsParams.blkdev = blkdev;
    buf.extendFsParams.size = size;
    buf.extendFsParams.flags = flags;

    status = libadvfs_syscall(
                         ADVFS_EXTENDFS,
                         &buf,
                         sizeof( buf.extendFsParams )
                         );

    *oldBlkSize = buf.extendFsParams.oldBlkSize;
    *newBlkSize = buf.extendFsParams.newBlkSize;

    return status;
}

adv_status_t
advfs_ss_dmn_ops (
    char* domainName,
    int type,
    ssDmnOpT *ssDmnCurrentState,
    int *ssRunningState
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.ssDmnOps.domainName = domainName;
    buf.ssDmnOps.type = type;

    sts = libadvfs_syscall (
                         ADVFS_SS_DMN_OPS,
                         &buf,
                         sizeof( buf.ssDmnOps )
                         );

    *ssDmnCurrentState = buf.ssDmnOps.ssDmnCurrentState;
    *ssRunningState = buf.ssDmnOps.ssRunningState;

    return sts;
}

adv_status_t
advfs_ss_get_params (
    char* domainName,
    adv_ss_dmn_params_t *ssDmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.ssGetParams.domainName = domainName;
    buf.ssGetParams.ssDmnParams = *ssDmnParams;

    sts = libadvfs_syscall (
                         ADVFS_SS_GET_PARAMS,
                         &buf,
                         sizeof( buf.ssGetParams )
                         );

    *ssDmnParams = buf.ssGetParams.ssDmnParams;

    return sts;
}

adv_status_t
advfs_ss_set_params (
    char* domainName,
    adv_ss_dmn_params_t ssDmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.ssSetParams.domainName = domainName;
    buf.ssSetParams.ssDmnParams = ssDmnParams;

    sts = libadvfs_syscall (
                         ADVFS_SS_SET_PARAMS,
                         &buf,
                         sizeof( buf.ssSetParams )
                         );

    return sts;
}

adv_status_t
advfs_ss_reset_params (
    char* domainName,
    adv_ss_dmn_params_t ssDmnParams
    )
{
    libParamsT buf;
    adv_status_t sts;

    buf.ssSetParams.domainName = domainName;
    buf.ssSetParams.ssDmnParams = ssDmnParams;

    sts = libadvfs_syscall (
                         ADVFS_SS_RESET_PARAMS,
                         &buf,
                         sizeof( buf.ssSetParams )
                         );

    return sts;
}

adv_status_t
advfs_ss_get_fraglist (
    char*         domainName,
    vdIndexT      volIndex,
    int           ssStartFraglist,
    int           ssFragArraySize,
    adv_frag_desc_t   *ssFragArray,
    int           *ssFragCnt
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.ssGetFraglist.domainName = domainName;
    buf.ssGetFraglist.volIndex = volIndex;
    buf.ssGetFraglist.ssStartFraglist = ssStartFraglist;
    buf.ssGetFraglist.ssFragArraySize = ssFragArraySize;
    buf.ssGetFraglist.ssFragArray = ssFragArray;

    sts = libadvfs_syscall (
                         ADVFS_SS_GET_FRAGLIST,
                         &buf,
                         sizeof( buf.ssGetFraglist )
                         );

    buf.ssGetFraglist.ssFragArray = ssFragArray;
    *ssFragCnt = buf.ssGetFraglist.ssFragCnt;

    return sts;
}

adv_status_t
advfs_ss_get_hotlist (
    bfDomainIdT   bfDomainId,
    int           ssStartHotlist,
    int           ssHotArraySize,
    adv_ss_hot_desc_t    *ssHotArray,
    int           *ssHotCnt
    )
{
    libParamsT buf;
    adv_status_t sts;

    bzero( &buf, sizeof(libParamsT));
    buf.ssGetHotlist.bfDomainId = bfDomainId;
    buf.ssGetHotlist.ssStartHotlist = ssStartHotlist;
    buf.ssGetHotlist.ssHotArraySize = ssHotArraySize;
    buf.ssGetHotlist.ssHotArray = ssHotArray;

    sts = libadvfs_syscall (
                         ADVFS_SS_GET_HOTLIST,
                         &buf,
                         sizeof( buf.ssGetHotlist )
                         );

    buf.ssGetHotlist.ssHotArray = ssHotArray;
    *ssHotCnt = buf.ssGetHotlist.ssHotCnt;

    return sts;
}

/*
 * tag_to_path
 *
 * This function converts a bitfile tag to a file system pathname.
 * The pathname is relative to the specified mount point.
 *
 * The caller specifies a buffer and its size.  The pathname is
 * returned in the buffer.
 *
 * NOTE:  The buffer size is not currently checked.  So, it should
 * be the maximum path length.
 */

adv_status_t
tag_to_path (
             char *mntOnName,  /* in */
             bfTagT bfTag,  /* in */
             int bufSize,  /* in */
             char *buf  /* in, modified */
             )

{

    dNameT *tmp;
    dNameT *dNames = NULL, *dName;
    bfTagT parentTag, prevParentTag, dTag;
    struct stat rootStats; /* file set's root stats */
    adv_status_t sts;

    if (stat (mntOnName, &rootStats ) < 0) {
        return errno;
    }
    
    /*
     * The following loop calls advfs_get_name() to get the file/dir name 
     * string for a tag.  advfs_get_name() also returns the tag's parent
     * dir's tag.  So we keep calling advfs_get_name() until we've
     * collected the name strings for all the pathname components
     * of the complete pathname for the specified tag.
     *
     * The pathname components are linked together using dNameT
     * structures.  This is singly-linked list and the new elements
     * are inserted at the head.  Since we have to start with the
     * bitfile's tag and work our way backwards to the file set
     * root dir, using this list has the effect of reversing the
     * order of the pathname components so that a forward traversal
     * of the list gives us the correct pathname; the components
     * are in the correct forward (left to right) order.
     *
     * The loop is terminated when we reach the file set's root dir.
     */

    dTag = bfTag;

    prevParentTag.tag_num = 0;
    prevParentTag.tag_seq = 0;

    do {
        /* Get a new pathname component struct */

        dName = (dNameT *) malloc( sizeof( dNameT ) );

        /* Get the current dir's name its parent's tag */

        sts = advfs_get_name( mntOnName,
                            dTag, 
                            dName->name, 
                            &parentTag );
        if (sts != EOK) {
            goto _error;
        }
        if (BS_BFTAG_EQL(parentTag, prevParentTag)) {
            /*
             * Stops infinite loops.
             */
            sts = ENO_SUCH_TAG;
            goto _error;
        }
        prevParentTag = parentTag;

        /* Link the current dir's pathname component into the list */

        dName->next = dNames;
        dNames = dName;

        /* The current dir is set to the parent dir; do the parent next */

        dTag = parentTag;

    } while (dTag.tag_num != rootStats.st_ino);

    /*
     * Now we can print the full pathname of the bitfile.
     * First we print the mount point pathname and then we
     * print the remaining pathname components by traversing
     * the pathname components linked list.
     */

    /* The loop below will add "/" before each name, so if 
     * we're look at the root filesystem, don't start the 
     * buf with the slash.
     */

    if (strcmp(mntOnName, "/") != 0) {
        strcpy (buf, mntOnName);
    } else {
        *buf = '\0';
    }

    while (dNames != NULL) {

        if (dNames->name[0] != '/') {
            strcat (buf, "/");
        }

        strcat (buf, dNames->name);
        tmp = dNames;
        dNames = dNames->next;
        free( tmp );
    }

    return EOK;

_error:

    while (dNames != NULL) {

        tmp = dNames;
        dNames = dNames->next;
        free( tmp );
    }
    return sts;

}  /* end tag_to_path */

char *gds_errlst[] = {
    "GDS_OK",
    "unknown disk type (use -t option)", 
    "device name not an absolute path",
    "can't open device",
    "not a block device",
    "can't stat device",
    "can't access character device",
    "can't figure out logical volume size",
    "can't figure out file system partition",
    "can't read disk label",
    "unavailable partition",
    "volume size is larger than supported size"
};

/*                           ---> Changed block_offset parameter type from
 *                                u_int to off_t.  This will allow a 64-bit
 *                                block offset.
 */
/*
 * check_disk_size()
 *
 * Performs a read at the given offset on the device
 * to be sure that the given offset doesn't run past
 * the end of the device.  The given offset is in blocks.
 *
 * Return value:  Returns TRUE if given offset is on the
 *                device.
 *                Returns FALSE if the read fails.
 */
static int
check_disk_size(int fd, u_int block_size, off_t block_offset)
{
    off_t byte_offset = (block_offset - 1LL) * (off_t) block_size;
    void *buf = (void *)malloc(block_size);
    int retval = FALSE;

    if (buf != (void *)NULL) {
	if ((lseek(fd, byte_offset, SEEK_SET) == byte_offset) &&
	    (read(fd, buf, (size_t)block_size) == (size_t)block_size)) {
	    retval = TRUE;
	}
	free(buf);
    } /* END if */
    return(retval);
}


/* 
 * advfs_checkvol
 *
 * DESCRIPTION:
 *   This routine is a replacement for checklsm() on Tru64.
 *   The passed in pathname will be parsed and if it looks valid,
 *   the volume group name and the volume name are returned.
 *   NOTE: advfs_checkvol does not verify the passed in pathname
 *   is a valid device.  The caller must verify this.  Currently 
 *   advfs_checkvol is just an intelligent parser.
 *
 * INPUT:
 *   vm_path      : (IN)  device path name
 *   vm_diskGroup : (OUT) volume group for the logical volume.
 *   vm_volName   : (OUT) volume name
 * 
 * Assumptions:
 *   vm_diskGroup and vm_volName point to valid memory
 *
 * RETURN VALUES:       
 *   Returns 1 if a valid pathname is found and the vm_diskGroup and
 *   vm_volName fields are filled in.  Returns 0 otherwise.
 *
 */

int
advfs_checkvol(char *vm_path,         /* In  */
               char *vm_diskGroup,    /* Out */
               char *vm_volName)      /* Out */
{
    char volpath[MAXPATHLEN];
    char *tok;
    char *savept; /* Used for strtok_r for thread-safety */

    strncpy(volpath, vm_path, MAXPATHLEN);
    /* make sure it is an absolute path */
    if (volpath[0] != '/') {
        return 0;
    }
    savept = volpath;
    tok = strtok_r(volpath, "/", &savept);

    if (strcmp(tok, "dev")) {
        return 0;
    }
    tok = strtok_r(NULL, "/", &savept);
    if (tok  == NULL) {
        return 0;
    }
    if (strcmp(tok, "vx") == 0) {
        /* /dev/vx/.... */
        /* Assume passed in path belongs to LVM */
        if (strcmp(strtok_r(NULL, "/", &savept), "dsk")) {
            return 0;
        }
        if ((tok = strtok_r(NULL, "/", &savept)) == NULL) {
            return 0;
        }
        strcpy(vm_diskGroup, tok);
        if ((tok = strtok_r(NULL, "/", &savept)) == NULL) {
            return 0;
        }
        strcpy(vm_volName, tok);
    }
    else if (strcmp(tok, "dsk") == 0 || strcmp(tok, "disk") == 0) {
        /* /dev/dsk and /dev/disk are not a valid logical vol pathnames */
        return 0;
    }
    else {
        char temp_str[MAXPATHLEN];
        struct stat st;

        /* Assume the passed in path belongs to LVM */
        strcpy(vm_diskGroup, tok);

        snprintf(temp_str, MAXPATHLEN, "/dev/%s/group", vm_diskGroup);
        if (stat(temp_str, &st)) {
            return 0;
        }
        /* the "group" file should be a char device */
        if (!S_ISCHR(st.st_mode)) {
            return 0;
        }
        if ((tok = strtok_r(NULL, "/", &savept)) == NULL) {
            return 0;
        }
        strcpy(vm_volName, tok);
        if (strtok_r(NULL, "/", &savept) != NULL) {
            return 0;
        }
    }
    return 1;
}

#define NO_OF_LVMRECS  2          /* This define is for documentation
                                   * purpose, the number of lvmrecords will
                                   * never change.
                                   */

/* Name: find_min_fsize(int)
 *
 * DESCRIPTION:
 *     Returnes the minimum of the disk block size or DEV_BSIZE
 *
 * INPUT:
 *     fd : open file descriptor for device
 *
 * RETURN VALUES:
 *     DEV_BSIZE or long
 */

static long
find_min_fsize(int fd)
{
        disk_describe_type dsk_data;

        if (ioctl(fd, DIOC_DESCRIBE, &dsk_data) != -1 &&
            dsk_data.lgblksz > DEV_BSIZE)
                return((long) dsk_data.lgblksz);
        return DEV_BSIZE;
}

/* 
 * advfs_is_lvmdev
 *
 * DESCRIPTION:
 *     Evaluates if 'device' is a physical volume ( that
 *     may belong to a LVM ).  This check is as suggested by
 *     LVM folks.
 *
 *     Currently even a logical volume partition is checked
 *     for (which is not necessary), but it may not be possible
 *     to determine whether the device file refers to a logical
 *     volume partition.
 *
 *     Code taken from mkfs.
 *
 * INPUT:
 *     device : device path to check
 *
 * RETURN VALUES:
 *     1 if device is an lvm disk  
 *     0 if not an lvm disk
 *    -1 if SomethingBadHappened
 *
 */

int
advfs_is_lvmdev(char *device)
{
    off_t lvmrec_addr, blksize;
    int   ret_val = 0;
    int   fd, index, flags = 0;

    char buf[MAXBSIZE];
    lv_lvmrec_t *lv_rec = (lv_lvmrec_t *)buf;


    /*
     * O_NONBLOCK flag is used because, if we're is running on a
     * autochanger, if all drives of the autochanger is busy then
     * this call is queued up by the driver and it appears that
     * we're is hanging, it is better to exit with an appropriate
     * message when this condition is reached.
     */
    if ((fd = open(device,O_RDONLY | O_NONBLOCK)) == -1) {
        return(-1);
    }

    /*  Turn off O_NONBLOCK flag to fix DSDe426564. */

    if ((flags = fcntl(fd, F_GETFL)) != -1) {
        (void) fcntl(fd, F_SETFL, (flags & ~O_NONBLOCK));
    }

    blksize = find_min_fsize(fd);

    /*
     * PVRA_LVM_REC_SN1 is the block number at which we can find the
     * lvm record, the block size being DEV_BSIZE always.
     */
    lvmrec_addr = PVRA_LVM_REC_SN1 * DEV_BSIZE;
    for(index = 0; index < NO_OF_LVMRECS; index++) {
        if (index) {  /* Second record */
            lvmrec_addr = PVRA_LVM_REC_SN2 * DEV_BSIZE;
            memset(buf,0,DEV_BSIZE);
        }
        if (lseek(fd, (off_t) lvmrec_addr, SEEK_SET) == (off_t) -1) {
            close(fd);
            return(-1);
        }

        if (read(fd, buf, blksize) != blksize) {
            close(fd);
            return(-1);
        }
        if (strncmp(lv_rec->lvm_id, LVMREC_MARK,sizeof(LVMREC_MARK)-1) == 0) {
            /*
             * It is a lvm disk, check the second copy of lvmrecord.
             * LVM group adviced to check both copies, If any one of
             * them is bad we can use the disk.
             */
            ret_val = 1;
            continue;
        } else {
            ret_val = 0;
            break;
        }
    }

    /*
     * When operating on the autochanger it is better to leave
     * the device open.  So that for the rest of this command the device
     * remains loaded in the drive. (there is no harm in doing so).
     * This in no way ensures that the device is locked.
     close(fd);
     */

     return (ret_val);
}

/*
 * get_dev_size
 *
 * Given a device name this routine will return the device's partition
 * size in blocks.
 */

getDevStatusT
get_dev_size( 
    char  *devName,      /* in - device name */
    char  *devType,      /* in - optional device type ("rz55", etc or NULL) */
    off_t *devSize       /* out - device size */
    )
{
    struct stat st;
    char devPath[MAXPATHLEN], device[MAXPATHLEN];
    int pathLen, i, fd, partNum, d_secsize = 0;
    off_t iDevSize = 0;
    getDevStatusT error;
    capacity_type cap;             /* device size filled in by ioctl. */
    char partName; /* partition name; abcdefgh */
    unsigned long lsmDevSize = 0;
    char*  ptr;

    if (devName[0] != '/') {
        return( GDS_ABSPATH );
    }

    if (stat( devName, &st ) < 0) {
        return( GDS_CANTOPEN );
    }

    if ((st.st_mode & S_IFMT) != S_IFBLK) {
        return( GDS_BLOCKDEV );
    }

    /* Form the character device name. */

    ptr = __blocktochar(devName, device);
    if (ptr == NULL) {
       return(GDS_CANTOPEN);
    }

    /* Open the character device */

    fd = open( device, O_RDONLY );
    if (fd < 0) {
        return( GDS_CANTOPEN );
    }

    if (fstat( fd, &st ) < 0) {
        close( fd );
        return( GDS_CANTFSTAT );
    }

    if ((st.st_mode & S_IFMT) != S_IFCHR) {
        close( fd );
        return( GDS_NOCHR );
    }

    /* Query the device for the size */
    if (ioctl(fd, DIOC_CAPACITY, &cap) < 0) {
        /* This print statement is for debug purposes */
        fprintf(stderr, 
                "libadvfs get_dev_size: ioctl failed. '%s'\n", strerror(errno));
        return(GDS_BADPART);
    }
    if (cap.lba == 0) {
        return(GDS_UNAVAILPART);
    }
    iDevSize = (off_t)cap.lba;

    /*
     * Sanity check:
     * Does this partition run past the end of the disk?
     * This could happen if the disk had been incorrectly
     * labeled.
     */
    if (check_disk_size(fd, DEV_BSIZE, iDevSize) != TRUE) {
        close( fd );
        return( GDS_BADPART );
    }

    close( fd );

    *devSize = (off_t)iDevSize;

    return( GDS_OK );
}

/* advfs_global_lock
 *
 * Description:
 *     Locks /dev/advfs or /dev/advfs_cfs (if TCR cluster)
 *
 * Arguments:
 *     int32_t nowait -- in, whether to wait for the lock
 *     int32_t *error -- out, any error that occurs
 * 
 * Return:
 *     file descriptor of locked file on success
 *     -1 on error
 */
int32_t advfs_global_lock(int32_t nowait, int32_t *error)
{
    char*  dir_to_lock = ADVFS_DMN_DIR;
    char   file_to_lock[MAXPATHLEN+1] = {0};

    if (clu_type() == CLU_TYPE_CFS) {
        dir_to_lock = ADVFS_TCR_DMN_DIR;
    }

    snprintf(file_to_lock, MAXPATHLEN+1, "%s/%s", dir_to_lock, DOT_ADVFS_LOCK);

    if (nowait) {
        return (do_lock_file(file_to_lock, F_WRLCK, 0, 0, error));
    }
    else {
        return (do_lock_file(file_to_lock, F_WRLCK, 0, 1, error));
    }
}

/*
 * advfs_unlock
 *
 * Straight wrapper for unlock_file; added for consistency with
 * other advfs_* locking functions
 */
int32_t advfs_unlock(int32_t lock, int32_t *error)
{
    int ret;

    ret = do_unlock_file(lock, 0, error);

    return ret;
}

/* 
 * advfs_fspath_longrun_lock
 *
 * Description:
 *     Locks the long run lock for a filesystem, never waits
 * 
 *     Allows other commands to determine if the command already accessing
 *     the specified storage domain is a long-running command, or whether
 *     they should wait for the lock.
 *
 * Arguments:
 *     char *fspath -- in, the path to the filesystem
 *     int32_t *error -- out, any error that occurs
 * 
 * Return:
 *     file descriptor of locked file on success
 *     #  -- file descriptor of locked file (success)
 *     -1 -- failed to lock filesystem lock
 *     -2 -- failed to lock global lock
 *     -3 -- failed to unlock global lock
 *     -4 -- failed to lock longrun lock
 */
int32_t advfs_fspath_longrun_lock(
    char *fspath, 
    int32_t have_global,
    int32_t nowait,
    int32_t *error
)
{
    int32_t err = 0;
    int32_t global_lock = -1;
    int32_t fsname_lock = -1;

    if (fspath == NULL) {
        return -1;
    }

    /* take the global lock */
    if (!have_global && 
        ((global_lock = advfs_global_lock(nowait, error)) < 0)) {
        /* only returns on an error */
        return (-2);
    }
    
    /*  if the longrun lock is already held, bail */
    fsname_lock = do_lock_file(fspath, F_WRLCK, 1, 0, error);
    if (fsname_lock < 0) {
        if (*error == EACCES) {
            /* Correct error, longrun is already locked.  However, 
             * commands expect EWOULDBLOCK 
             */
            *error = EWOULDBLOCK;
        }
        if (global_lock != -1) {
            /* unlock the global */
            (void)advfs_unlock(global_lock, NULL);
        }
        return (-4);
    }

    /* Now lock the storage domain.  The previous lock will be released
     * during this lock request
     */
    if (nowait) {
        fsname_lock = do_lock_file(fspath, F_WRLCK, 0, 0, error);
    } else {
        fsname_lock = do_lock_file(fspath, F_WRLCK, 0, 1, error);
    }

    if (global_lock != -1) {
        /* if an error occurs unlocking the global lock, the caller
         * should see that and be able to respond... so we do return
         * the error */
        if(advfs_unlock(global_lock, &err) < 0) {
            /* unlock the filesystem lock, but ignore the error since
             * there is no good way to respond to it */
            if (fsname_lock != -1) {
                advfs_unlock(fsname_lock, NULL);
            }
            *error = err;
            return (-3);
        }
    }

    /* at this point we return with both longrun and fsname locks held */
    return (fsname_lock);
}

/*
 * advfs_fspath_lock
 *
 * Description:
 *     Locks the filesystem directory, optionally locking the global
 *     filesystem directory.
 *
 * Arguments:
 *     fspath -- in, path to storage domain directory to lock
 *     have_global -- in, whether the global lock should be aquired
 *     nowait -- in, whether to wait if lock is held by a 
 *               short running cmd
 *     *error -- out, error code if an error occurs
 *
 * Return:
 *     #  -- file descriptor of locked file (success)
 *     -1 -- failed to lock filesystem lock
 *     -2 -- failed to lock global lock
 *     -3 -- failed to unlock global lock
 *     -4 -- failed to lock longrun lock
 *     -5 -- failed to unlock longrun lock
 */
int32_t advfs_fspath_lock(
     char    *fspath, 
     int32_t have_global, 
     int32_t nowait, 
     int32_t *error
)
{
    int32_t err = 0;
    int32_t global_lock = -1;
    int32_t fsname_lock = -1;
    int32_t longrun_lock = -1;

    if (fspath == NULL) {
        return -1;
    }

    /* take the global lock */
    if (!have_global && 
        ((global_lock = advfs_global_lock(nowait, error)) < 0)) {
        /* only returns on an error */
        return (-2);
    }
    
    /* we must lock the long-runner lock prior to proceeding as
     * this allows commands to exit early if the long runner
     * lock is held */
    longrun_lock = do_lock_file(fspath, F_WRLCK, 1, 0, error);
    if (longrun_lock < 0) {
        if (global_lock != -1) {
            (void)advfs_unlock(global_lock, NULL);
        }
        return (-4);
    }

    if (nowait) {
        fsname_lock = do_lock_file(fspath, F_WRLCK, 2, 0, error);
    }
    else {
        fsname_lock = do_lock_file(fspath, F_WRLCK, 2, 1, error);
    }
     
    if (global_lock != -1) {
        /* if an error occurs unlocking the global lock, the caller
         * should see that and be able to respond... so we do return
         * the error */
        if(advfs_unlock(global_lock, &err) < 0) {
            /* unlock the filesystem lock, but ignore the error since
             * there is no good way to respond to it */
            advfs_unlock(fsname_lock, NULL);
            *error = err;
            return (-3);
        }
    }

    /* unlock the longrun lock */
    if (do_unlock_file(longrun_lock, 1, &err) < 0) {
        /* unlock the filesystem lock, but ignore the error since
         * there is no good way to respond to it */
        advfs_unlock(fsname_lock, NULL);
        *error = err;
        return (-5);
    }
    
    return (fsname_lock);
}

/*
 * advfs_fspath_lock_timed
 *
 * Description:
 *     Locks the filesystem directory, optionally locking the global
 *     filesystem directory.  Exits early after a timeout.
 *
 *     Because this function uses nanosleep(), it doesn't have the
 *     side-effect of screwing up any existing SIGALRM handlers.
 *     see the nanosleep(2) man page for details.
 *
 *  NOTE NOTE NOTE
 *     Testing has shown that nanosleep can interrupt any other 
 *     calls waiting on SIGALRM.  Use caution if depending on 
 *     SIGALRM elsewhere in your program.
 *
 *
 * Arguments:
 *     fsname -- in, filesystem to lock
 *     have_global -- in, whether the global lock should be aquired
 *     seconds -- in, how long to wait for the lock
 *     *error -- out, error code if an error occurs
 *
 * Return:
 *     see advfs_fspath_lock for return values.
 *
 */
int32_t advfs_fspath_lock_timed(
     char *fspath,
     int32_t have_global,
     int32_t seconds,
     int32_t *error
)
{
    int32_t          fspath_lock = -1;
    struct timespec  cur_time;
    struct timespec  sleep_time;
    time_t           end_time;

    if (fspath == NULL) {
        *error = EINVAL;
        return(-1);
    }

    clock_gettime(CLOCK_REALTIME, &cur_time);

    sleep_time.tv_sec = 1;  /* sleep in one second intervals */
    sleep_time.tv_nsec = 0;

    /* add an extra second for overhead */
    end_time = cur_time.tv_sec + seconds + 1; 

    while (cur_time.tv_sec < end_time) {
        fspath_lock = advfs_fspath_lock(fspath, have_global, TRUE, error);

        if (fspath_lock >= 0) {
            /* got it */
            break;
        }

        /* not checking status because spurious wakeups here are ok */
        (void)nanosleep(&sleep_time, NULL);

        clock_gettime(CLOCK_REALTIME, &cur_time);
    }

    return(fspath_lock);
}

int lock_file( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
)
{
    int  ret;

    ret = do_lock_file(fileName, lkMode, 0, 1, error);

    return(ret);
}

int lock_file_nb( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
)
{
    int  ret;

    ret = do_lock_file(fileName, lkMode, 0, 0, error);

    return(ret);
}

/*
 *  do_lock_file
 *
 *  Function that does the real work for file locking 
 *
 *  If lkType == 0, locks the entire file
 *            == 1, takes out longrun lock
 *            == 2, takes out shortrun lock 
 *
 *  Use lkType of ALL when not trying to synchronize access to a storage
 *  domain 
 */
static 
int do_lock_file( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int lkType,         /* in - type of lock - ALL, GLOBAL, FS */
    int block,          /* in - 1 = blocking lock, 0 = nonblocking */
    int *error          /* out - errno if return -1 */
)
{
    int fd = -1;
    int cmd;
    struct stat st;
    struct flock lockdesc;
    char lockFile[MAXPATHLEN+1];
    char *cp;
    
    /* Because we use advisory lock on directories which anyone can */
    /* access, we can have denial of service attacks.  The original */
    /* solution was if the file to lock happened to be a directory  */
    /* then create a lock file in the directory.                    */
    /*                                                              */
    /* This failed as "swapon" checks the domain directories and    */
    /* did not know how to handle the lock files.  Now we create    */
    /* the lock files in '/dev/advfs[_cfs]'.  The lock files must be*/
    /* unique so the soltion is to strip off the last part of the   */
    /* pathname and append it to the lock file.                     */
    /* The user will not be able to lock this file.                 */

    /* First check if file is a directory, if it isn't we don't do  */
    /* anything different.                                          */

    if (stat( fileName, &st ) < 0) {
        if(error != NULL) {
            *error = errno;
            if (*error == ENOENT) { /* file does not exist */
                *error = 0;
                st.st_mode = 0;
            } else {
                return -1;
            }
        }
    }

    /* set blocking or non */
    if (block) {
        cmd = F_SETLKW;
    } else {
        cmd = F_SETLK;
    }

    if (lkType == 1) {            /* longrun */
        lockdesc.l_start = 0;
        lockdesc.l_len = 1;
    } else if (lkType == 2) {     /* regular */
        lockdesc.l_start = 1;
        lockdesc.l_len = 1;
    } else {                      /* whole file */
        lockdesc.l_start = 0;
        if (S_ISDIR(st.st_mode) && (st.st_size < 2)) {
            lockdesc.l_len = 2;
        } else {                  /* 0 means lock the whole thing */
            lockdesc.l_len = 0;
        }
    }

    lockdesc.l_type = lkMode;
    lockdesc.l_whence = SEEK_SET;
    
    if (S_ISDIR(st.st_mode )) {
        char        tmpPath[MAXPATHLEN+1] = {0};
        char*       lockName;

        /* Save the original path because basename will insert a '\0' */
        strncpy(tmpPath, fileName, MAXPATHLEN);
         
        lockName = basename(tmpPath); 

        if ((strlen(fileName) + strlen(lockName) + strlen(DOT_ADVFS_LOCK) + 5) 
            > MAXPATHLEN) {
            return -1;
        }

        /* Create full file name from fileName                      */
        snprintf(lockFile, MAXPATHLEN+1, "%s/../%s_%s", fileName, 
            DOT_ADVFS_LOCK, lockName);
        
        /* Open file if it doesn't exist create it.  Make sure to   */
        /* give the new file the owner read only.                   */
        if ((fd = open( lockFile, O_RDWR | O_CREAT | O_SYNC | S_IRUSR )) < 0) {
            if (error != NULL) {
                *error = errno;
            }
            return -1;
        }

        /* Lock the lock file.  */

        if (fcntl(fd, cmd, &lockdesc) == -1) {
            if (error != NULL) {
                *error = errno;
            }
            close(fd);
            return -1;
        }
    }
    else {
        if ((fd = open(fileName, O_RDWR | O_CREAT | O_SYNC | S_IRUSR, 0)) < 0) {
            if (error != NULL) {
                *error = errno;
            }
            return -1;
        }
        
        /* Lock the lock file. */
        if (fcntl(fd, cmd, &lockdesc) == -1) {
            if (error != NULL) {
                *error = errno;
            }
            close(fd);
            return -1;
        }
    }
    
    return fd;
}

int unlock_file( 
    int lockHndl,       /* in - lock handle (really just the file desc) */
    int *error          /* out - errno if return -1 */
)
{
    int ret;

    ret = do_unlock_file(lockHndl, 0, error);

    return ret;
}

static
int do_unlock_file(
    int lockHndl,       /* in - lock handle (really just the file desc) */
    int lkType,         /* in - 0 = whole file, 1 = longrun only */
    int *error          /* out - errno if return -1 */
)
{
    struct flock lockdesc;

    /*  Note that closing any file descriptor associated with a file
     *  releases all locks.  Don't close if it's longrun only 
     */

    if (lkType == 1) {
        lockdesc.l_type = F_UNLCK;
        lockdesc.l_whence = SEEK_SET;
        lockdesc.l_start = 0;
        lockdesc.l_len = 1;
    
        if (fcntl(lockHndl, F_SETLK, &lockdesc) == -1) {
            if (error != NULL) {
                *error = errno;
            }
            return -1;
        }
    } else {
        close(lockHndl);
    }

    return 0;
}

int
volume_avail(
    char *prog,         /* in - program name (for error msgs) */
    char *dmnName,      /* in - domain name */
    char *volName       /* in - name of volume to add */
    )
{
    DIR           *dp;
    struct dirent *dirEnt;
    struct stat    volStats, entStats;
    char           dmnDir[MAXPATHLEN+1];
    int            i;
    nl_catd        catd;

    if (stat( volName, &volStats ) < 0) {
   	catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);	
        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY15, 
            "%s: cannot access device %s; [%d] %s\n"),
            prog, volName, errno, ERRMSG( errno ) );
	(void) catclose(catd);
        return FALSE;
    }

    for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
        dp = opendir(advfs_global_dirs[i]);
        if (dp == NULL) {
            continue;
        }
    
        /* 
         * Look in the filesystem's storage directory for the specified device
         */
 
        while ((dirEnt = readdir( dp )) != NULL) {
 
            if (!strcmp( dirEnt->d_name, "." ) ||
                !strcmp( dirEnt->d_name, "..") ||
                !strcmp( dirEnt->d_name, dmnName)) {
                /* skip . and .. */
                continue;
            }

            snprintf(dmnDir, MAXPATHLEN+1, "%s/%s/%s", advfs_global_dirs[i],
                dirEnt->d_name, DOT_ADVFS_STG);

            if (stat( dmnDir, &entStats ) < 0) {
                continue;
            }
   
            if (S_ISDIR( entStats.st_mode )) {
                if (volume_in_use( prog, dmnDir, &volStats )) {
                    closedir( dp );
                    return FALSE;
                }
            }
        }
        closedir( dp );
    }

    return TRUE;
}



int
volume_in_use( 
    char *prog,                 /* in - program name (for error msgs) */
    char *dmnDir,               /* in - domain dir's full path name */
    struct stat *volStats       /* in - new volume's stats */
    )
{
    DIR *dp;
    struct dirent *dirEnt;
    struct stat dmnVolStats;
    char dmnVol[MAXPATHLEN+1];
    char dmnVolLink[MAXPATHLEN+1];
    char* lptr;
    int len;
    nl_catd catd;

    dp = opendir( dmnDir );
    if (dp == NULL) {
   	catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);		
        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY16, 
            "%s: unable to open directory <%s>; [%d] %s\n"),
            prog, dmnDir, errno, ERRMSG( errno ) );
	(void) catclose(catd);
        return FALSE;
    }
    
    /* 
     * append a slash (/)  to the path name 
     */
 
    strcpy( dmnVol, dmnDir );
    strcat( dmnVol, "/" );
    len = strlen( dmnVol );
 
    while ((dirEnt = readdir( dp )) != NULL) {
 
        if (!strcmp( dirEnt->d_name, "." ) ||
            !strcmp( dirEnt->d_name, "..")) {
            /* skip . and .. */
            continue;
        }

        strcat( dmnVol, dirEnt->d_name );
   
        if (stat( dmnVol, &dmnVolStats ) < 0) {
            dmnVol[len] = '\0';                 /* chop off the file name */
            continue;
        }
   
        if (S_ISBLK( dmnVolStats.st_mode )) {
            if (dmnVolStats.st_rdev == volStats->st_rdev ) {
                closedir( dp );

                if (readlink(dmnVol, dmnVolLink, sizeof(dmnVolLink)) != -1) {
                    lptr = strrchr( dmnDir, '/' );
                    lptr++;
                        catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);
                        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY17, 
                            "%s: device in use by domain %s; device %s\n"),
                            prog, dmnDir, dmnVolLink );
                        (void) catclose(catd);
                    return TRUE;
                }
            }
        }
   
        dmnVol[len] = '\0';                     /* chop off the file name */
    }
    closedir( dp );

    return FALSE;
}

/* NAME: advfs_fsname(devpath_arg)
 *
 * DESCRIPTION:
 *   This routine is for generating an AdvFS file system name given a device
 *   name. This function does not check if the device name passed is valid.
 *   It handles normal device names along with logical volume device names.
 *
 *   To find the AdvFS file system that contains a device use dev_to_fsname.
 *
 *   example: /dev/disk/dsk5 -> advfs_dsk5
 *            /dev/vg00/lvol1 -> advfs_vg00.lvol1
 *   
 * INPUT:
 *   devpath_arg : device path name
 * 
 * RETURN VALUES:       
 *   the resulting private file system name is returned (char *).
 *   This value is malloc'd memory and should be freed by the caller.
 */

char *
advfs_fsname(char *devpath_arg)
{   
    char *devpath = NULL, *fsname = NULL;
    char name_gen[BS_DOMAIN_NAME_SZ+1]; /* Plus 1 for \O */
    char *str_tok, *leaf1 = NULL, *leaf2 = NULL;
    char vm_diskgroup[MAXPATHLEN];
    char vm_volname[MAXPATHLEN];

    if (!devpath_arg) {    
        return NULL;
    }

    if (strlen(devpath_arg) > MAXPATHLEN) {
        return NULL;
    }   

    if (!(devpath = strdup(devpath_arg))) {
        return NULL;
    }
    
    leaf2 = str_tok = strtok(devpath, "/");
    while ((str_tok = strtok(NULL, "/"))) {
        leaf1 = leaf2;
        leaf2 = str_tok;
    }
    if (leaf1 == NULL) {
        return NULL;
    }

    if (!strcmp(leaf1, "disk") || !strcmp(leaf1, "dsk")) {
        /* /dev/disk/dskxxxxx */
        snprintf(name_gen, BS_DOMAIN_NAME_SZ, "advfs_%s", leaf2);
    }
    else if (!strcmp(leaf1, "vol")) {
        /* /dev/vol/volxx */
        snprintf(name_gen, BS_DOMAIN_NAME_SZ, "advfs_rootvol.%s", leaf2);
    }
    else if (advfs_checkvol(devpath_arg, vm_diskgroup, vm_volname)) {
        snprintf(name_gen, BS_DOMAIN_NAME_SZ, "advfs_%s.%s", vm_diskgroup, 
                vm_volname);
    }
    else {
        return NULL;
    }

    /* copy generated name to name pointer */
    if (!(fsname = strdup(name_gen))) {
        return NULL;
    }

    free(devpath);
    return fsname;
}


/* NAME: dev_to_fsname(devpath_arg, fsinfop)
 *
 * DESCRIPTION:
 *   This routine takes a device name and returns the name of the AdvFS file
 *   system that contains that device name. It accomplishes this by doing
 *   a search through the AdvFS file system directory(ies) to find
 *   a link to the device name in question. It handles normal device names 
 *   along with logical volume device names.
 *
 *   This routine does not do file locking. It may return NULL, if a change to
 *   the file system occurs synchronously.
 *
 * INPUT:
 *   devpath_arg : device path name
 *   fsinfop     : (in/out) If no detailed information desired, pass in NULL
 * 
 * RETURN VALUES:       
 *   The resulting private file system name is returned (char *) or a NULL
 *   pointer if the name could not be found.
 *   This value is allocated memory and should be freed by the caller.
 *
 */
char *
dev_to_fsname(char*       devpath_arg,
              arg_infoT*  fsinfop
)
{   
    DIR *FD, *FDD;
    char fs_dev_path[MAXPATHLEN+1];
    char fs_path[MAXPATHLEN+1];
    struct dirent *devs;
    struct dirent *devss;
    int  i;
    int  ret;
    char* lptr;
    struct stat filestat;
    ino_t dev_inode;

    if (!devpath_arg) {
        return NULL;
    }

    if (strlen(devpath_arg) > MAXPATHLEN) {
        return NULL;
    }

    if (stat(devpath_arg, &filestat) != 0) {
        return NULL;
    }

    dev_inode = filestat.st_ino;

    for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
        /* open AdvFS directory */
        FD = opendir(advfs_global_dirs[i]);
        if (FD == NULL) {
            continue;
        }

        /* Scan the AdvFS directory and open each file system */
        while ((devs = readdir(FD)) != NULL) {
            /*  filesystems cannot begin with a ".", so we can safely 
             *  ignore these.  This skip covers all special AdvFS files
             */
            if (devs->d_name[0] == '.') {
                continue;
            }

            /* open the file system storage directory */
            snprintf(fs_path, MAXPATHLEN+1, "%s/%s/%s", advfs_global_dirs[i], 
                devs->d_name, DOT_ADVFS_STG);
            FDD = opendir(fs_path);
            if (FDD == NULL) {
                /* The directory is not a valid fs, skip */
                continue;
            }

            /* read each device link in the file system directory */
            while ((devss = readdir(FDD)) != NULL) {
                if (devss->d_name[0] == '.') {
                    continue;
                }

                snprintf(fs_dev_path, MAXPATHLEN+1, "%s/%s", fs_path, 
                    devss->d_name);

                /* Stat the destination of the symlink to get its inode number */
                if (stat(fs_dev_path, &filestat) != 0) {
                    /* Bogus symlink, ignore */
                    continue;
                }

                /* If inodes match, this is the file system */
                if (dev_inode == filestat.st_ino) {
                    /* Found a match */
                    /* strdup _before_ closing the directory */
                    char *d_handle = NULL;
                    if (fsinfop != NULL) {
                        sprintf(fsinfop->fspath, "%s/%s", advfs_global_dirs[i],
                            devs->d_name);
                        strcpy(fsinfop->fsname, devs->d_name);
                        strcpy(fsinfop->stgdir, fs_path);
                        if (strcmp(advfs_global_dirs[i], ADVFS_TCR_DMN_DIR) == 0) {
                            fsinfop->tcr_shared = 1;
                        }
                    }
                    d_handle = strdup(devs->d_name);
                    closedir(FDD);
                    closedir(FD);
                    return(d_handle);
                }
            }
            closedir(FDD);
        }
        closedir(FD);
    }
    return NULL;
}

/* NAME: mountpoint_to_fsname(raw_mntpoint, fsinfop)
 *
 * DESCRIPTION:
 *   This routine takes a mount point string and returns the name of the 
 *   AdvFS file system that is mounted on that mountpoint. It accomplishes 
 *   this by doing a search through the mounted list file (/etc/mnttab).
 *
 * INPUT:
 *   raw_mntpoint : absolute path to mount point
 *   fsinfop      : (in/out) If no detailed information desired, pass in NULL
 * 
 * RETURN VALUES:       
 *   The resulting private file system name is returned (char *) or a NULL
 *   pointer if the name could not be found.
 *   This value is allocated memory and should be freed by the caller.
 *
 */
char *
mountpoint_to_fsname(char*       raw_mntpoint, 
                     arg_infoT*  fsinfop
)
{
    char mnt[MAXPATHLEN+1];
    char *fsname = NULL;
    struct mntent *mp = NULL;
    FILE *mnttab = NULL;
    char dmn_dir[MAXPATHLEN+1];
    char *ptr = NULL;
    int  i;

    /* Clean /'s off the end */
    strcpy(mnt, raw_mntpoint);
    while ((strlen(mnt) > 1) && (mnt[strlen(mnt)-1] == '/')) {
        mnt[strlen(mnt)-1] = '\0';
    }

    /* Open the mounted list */
    mnttab = setmntent(MNT_MNTTAB, "r");
    if (mnttab == NULL) {
        return (NULL);
    }

    /*  ensure we don't falsely match on /dev/advfsfoo instead of
     *  /dev/advfs
     */

    /* Is it mounted */
    while (mp = getmntent(mnttab)) {
        if (!strcmp(mp->mnt_dir, mnt) && !strcmp(mp->mnt_type, MNTTYPE_ADVFS)) {
            for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
                snprintf(dmn_dir, MAXPATHLEN+1, "%s/", advfs_global_dirs[i]);

                if (strncmp(mp->mnt_fsname, dmn_dir, strlen(dmn_dir)) == 0) {
                    mp->mnt_fsname += strlen(dmn_dir);  
                } else {
                    continue;  /* wrong domain dir */
                }

                ptr = strchr(mp->mnt_fsname, '/');
                if (ptr == NULL) {
                    /*  should be a can't happen, but... */
                    continue;
                }

                *ptr = '\0';  /* strip off the fileset name */
                fsname = strdup(mp->mnt_fsname);
                if (fsinfop != NULL) {
                    sprintf(fsinfop->fspath, "%s/%s", advfs_global_dirs[i], 
                        fsname);
                    strcpy(fsinfop->fsname, fsname);
                    if (strcmp(advfs_global_dirs[i], ADVFS_TCR_DMN_DIR) == 0) {
                        fsinfop->tcr_shared = 1;
                    }
                    fsinfop->arg_type = MNTPT;
                }
                break;
            }
            
            if (fsname == NULL) {
                /* The device specification doesn't start with one of the
                 * AdvFS top-level directories.  Check to see if it's a 
                 * regular block device, not an AdvFS pseudo-device 
                 */
                fsname = dev_to_fsname(mp->mnt_fsname, fsinfop);
            }
        }

        if (fsname != NULL) {
            break;
        }
    }

    endmntent(mnttab);
    return ( fsname );
}


/* NAME: arg_to_fsname(arg, fsinfop)
 *
 * DESCRIPTION:
 *   This routine takes a arg parameter which can be a path to an AdvFS
 *   filesystem (in /dev/advfs[_cfs]/), an AdvFS filesystem name, or a device 
 *   path.  This routine determines which it is, figures out the fsname and 
 *   returns it. This routine also validates that the fs name actually exists 
 *   in /dev/advfs[_cfs] otherwise the return value will be NULL.
 *
 *   This routine does not do file locking. It may return NULL, if a change to
 *   the file system(s) occurs synchronously.
 *
 * INPUT:
 *   arg          : AdvFS fs path, fs name, device path, or mount point.
 *   fsinfop      : (in/out) If no detailed information desired, pass in NULL
 * 
 * RETURN VALUE:       
 *   The resulting private file system name is returned (char *) or a NULL
 *   pointer if the name could not be found.
 *   This value is allocated memory and should be freed by the caller.
 *
 */
char *
arg_to_fsname(char*       arg,
              arg_infoT*  fsinfop)
{
    char*       fsname = NULL;
    char        tmp_path[MAXPATHLEN+1];
    struct stat filestat;
    char*       ptr; 
    char*       lptr;
    char        advfs_dmn_dir[MAXPATHLEN+1];
    char        advfs_realdmn_dir[MAXPATHLEN+1];
    int         i;
   
    /* strip off any trailing slashes */

    strncpy(tmp_path, arg, MAXPATHLEN+1);
    tmp_path[MAXPATHLEN] = '\0';

    i = strlen(tmp_path) - 1;

    while ((i > 0) && (tmp_path[i] == '/')) {
        tmp_path[i] = '\0';
        i--;
    }

    if ((tmp_path[0] == '/') && (stat(tmp_path, &filestat) == 0)) {
        /*  Use trailing slash after the standard directory
         *  path to ensure someone doesn't enter something like
         *  /dev/advfs_foo/myfsname
         * 
         *  Additionally, compare against both the symlink (/dev/advfs,
         *  TCR directory) in case we've been passed something that's 
         *  been realpath()ed.
         */
        for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
            snprintf(advfs_dmn_dir, MAXPATHLEN, "%s/", advfs_global_dirs[i]);
            lptr = realpath(advfs_global_dirs[i], advfs_realdmn_dir);
            if (lptr != NULL) {
                strcat(advfs_realdmn_dir, "/");
            }

            if ((!strncmp(tmp_path, advfs_dmn_dir, strlen(advfs_dmn_dir))) ||
                ((lptr != NULL) && (!strncmp(tmp_path, lptr, strlen(lptr))))) {
                
                /* Arg is full fs path */

                if (major(filestat.st_rdev) == ADVFSDEV_MAJOR) {
                    /*  path to the fileset or snapshot provided
                     *  strip off the fileset name
                     */
                    ptr = strrchr(tmp_path, '/');
                    if (fsinfop != NULL) {
                        strcpy(fsinfop->fset, ptr+1);
                    }
                    *ptr = '\0';  
                } else if (S_ISDIR(filestat.st_mode)) {  
                    /* arg is a directory, but is it a valid fs dir? */
                    struct stat     tstStat;
                    char            tstPath[MAXPATHLEN+1];

                    snprintf(tstPath, MAXPATHLEN+1, "%s/%s", tmp_path, 
                        "default");

                    if ( !(stat(tstPath, &tstStat) == 0) ||
                         !(major(tstStat.st_rdev) == ADVFSDEV_MAJOR) ) {
                        /*  Nope, garbage passed in */
                        return NULL;
                    }
                }
                
                fsname = strdup(strrchr(tmp_path, '/')+1);
                if (fsinfop != NULL) {
                    sprintf(fsinfop->fspath, "%s/%s", advfs_global_dirs[i], 
                        fsname);
                    strcpy(fsinfop->fsname, fsname);
                    if (!strcmp(advfs_global_dirs[i], ADVFS_TCR_DMN_DIR)) {
                        fsinfop->tcr_shared = 1;
                    }
                    fsinfop->arg_type = ADVFS;
                }
                break;
            }
        }
        if (fsname == NULL) {                    /* didn't find it above */
            /* Arg is device or mount point */
            if (S_ISBLK(filestat.st_mode)) {
                fsname = dev_to_fsname(tmp_path, fsinfop);
                if (fsinfop != NULL) {
                    fsinfop->arg_type = BLKDEV;
                }
            }
            else if (S_ISCHR(filestat.st_mode)) {
                char   tmpdev[MAXPATHLEN+1];
                ptr = __chartoblock(tmp_path, tmpdev);
                if (ptr == NULL) {
                    return NULL;
                }
                fsname = dev_to_fsname(tmpdev, fsinfop);
                if (fsinfop != NULL) {
                    fsinfop->arg_type = CHRDEV;
                }
            }
            else if (S_ISDIR(filestat.st_mode)) {
                /*  Note that mountpoint_to_fsname() will set the 
                 *  fsinfop->arg_type if it finds a match.
                 */
                fsname = mountpoint_to_fsname(tmp_path, fsinfop);
            }
        }
    }
    else {
        /* Arg is short fs name.  Check to see if a fileset is 
         * appended to the path
         */
        char      fset[MAXPATHLEN] = "default";

        ptr = NULL;
        fsname = strdup(tmp_path);

        ptr = strchr(fsname, '/');
        if (ptr != NULL) {
            /* we have a fileset */
            strcpy(fset, ptr+1);
            if ((fsinfop != NULL) && (*(ptr+1) != '\0')) {
                strcpy(fsinfop->fset, fset);
            }
            *ptr = '\0';
        }
         
        if ((*fsname == '\0') || strlen(fsname) == 0) {
            /* this should be a can't happen */
            free(fsname);
            fsname = NULL;
        } else {
            int   found = 0;

            for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
                snprintf(tmp_path, MAXPATHLEN, "%s/%s", advfs_global_dirs[i], 
                     fsname);
                if ( (stat(tmp_path, &filestat) == 0) &&
                     S_ISDIR(filestat.st_mode) ) {
                    found = 1;
                    if (fsinfop != NULL) {
                        strcpy(fsinfop->fspath, tmp_path);
                        strcpy(fsinfop->fsname, fsname);
                        if (!strcmp(advfs_global_dirs[i], ADVFS_TCR_DMN_DIR)) {
                            fsinfop->tcr_shared = 1;
                        }
                        fsinfop->arg_type = ADVFS;
                    }
                    break;
                }
            }

            /* make sure this really exists */
            if (found) {
                strcat(tmp_path, "/");
                strcat(tmp_path, fset);
                if ( (stat(tmp_path, &filestat) != 0) ||
                     (major(filestat.st_rdev) != ADVFSDEV_MAJOR) ) {
                    /* bogus spec */
                    if (fsinfop != NULL) {
                        fsinfop->corrupt = NOFSETDEV;
                    }
                }
            }

            if (!found) {
                free(fsname);
                fsname = NULL;
            }
        }
    } /* short fsname */

    return(fsname);
}


/* NAME: process_advfs_arg(arg)
 *
 * DESCRIPTION:
 *   This routine is similar to arg_to_fsname, but it returns an arg_info
 *   structure with more information about the argument instead of just the 
 *   fsname.
 *
 *   This routine does not do file locking. It may return NULL, if a change to
 *   the file system(s) occurs synchronously.
 *
 * INPUT:
 *   arg : AdvFS fs path, fs name, or device path
 * 
 * RETURN VALUE:       
 *   A pointer to an arg_infoT type structure or a NULL pointer if the arg
 *   could not be resolved to a file system.
 *   This value is allocated memory and should be freed by the caller.
 *
 *   The blkfile returned in the arg_infoT structure is either:
 *       1.  the only block device in the filesystem specified by arg
 *       2.  the block device representation of arg if a non-AdvFS block
 *           or character device is specified
 *
 */
arg_infoT *
process_advfs_arg(char *arg)
{
    char *fsname = NULL;
    arg_infoT *infop = NULL;
    struct stat filestat;
    int i;
    int ret;
    char sarg[MAXPATHLEN+1];

    DIR *FDD;
    int num_links = 0;
    char fs_dev_path[MAXPATHLEN+1];
    char dev_link_path[MAXPATHLEN+1];
    struct dirent *devss;
    char* chkdev = NULL;

    if (arg == NULL) {
        return NULL;
    }

    /* strip off any trailing slashes */

    strncpy(sarg, arg, MAXPATHLEN+1);

    i = strlen(sarg) - 1;

    while ((i > 0) && (sarg[i] == '/')) {
        sarg[i] = '\0';
        i--;
    }

    infop = (arg_infoT *)calloc(1, sizeof(arg_infoT));

    if (!infop) {
        return NULL;
    }

    /* arg_to_fsname() fills in fspath, fsname, tcr_shared and, 
     * if provided, the fileset.
     */
    fsname = arg_to_fsname(sarg, infop);
    
    if (!fsname) {
        free(infop);
        return NULL;
    }

    /* The full path to the filesystem's storage area */
    snprintf(infop->stgdir, MAXPATHLEN, "%s/%s", infop->fspath, DOT_ADVFS_STG);

    if (infop->fset[0] == '\0') {
        sprintf(infop->fset, "default");
    }

    /* If possible, the block device special file */
    /* TODO: Make multi aware, not just directory scan */
    /* 
     * If the filesystem directory has a single valid device link,
     * follow the link to the device and fill in the blkfile.
     */
    if (infop->blkfile[0] == '\0') {
        if (FDD = opendir(infop->stgdir)) {
            char*  lptr;

            while ((devss = readdir(FDD)) != NULL) {
                if (devss->d_name[0] == '.') {
                    continue;
                }

                snprintf(fs_dev_path, MAXPATHLEN, "%s/%s", 
                            infop->stgdir, devss->d_name);
               
                ret = readlink(fs_dev_path, dev_link_path, 
                    sizeof(dev_link_path));
                if (ret == -1) { 
                    continue;
                }

                if (chkdev == NULL) {
                    chkdev = dev_link_path;
                }
                num_links++;
            }
            if (num_links == 1) {
                strcpy (infop->blkfile, dev_link_path);
            }
            closedir(FDD);
        }
    } else {
        chkdev = infop->blkfile;
    }

    /*  Determine if multi-volume-enabled */
    if (chkdev != NULL) {
        ret = advfs_is_multivol(chkdev, &i);
        if (ret == 1) {
            infop->is_multivol = 1;
        } else if (i != 0) {
            infop->corrupt = DEVERR;
        }
    } else {
        infop->corrupt = DEVERR;
    } 

    if (infop->corrupt == DEVERR) {
        /*  Do the best we can do determine if this fs is named
         *  or not, even though we can't read a volume superblock to 
         *  find out for sure.
         */
        char*    tmpfsname = NULL;

        /*  preference is to use device names, if we can find any
         *  rather than relying on link counts in the storage dir, which
         *  might be full of crud. 
         */
        if (infop->blkfile[0] != '\0') {
            tmpfsname = advfs_fsname(infop->blkfile);
        } else if (chkdev != NULL) {
            tmpfsname = advfs_fsname(chkdev);
        }

        if (tmpfsname != NULL) {
            /*  using the 'default' fsname? */
            if (strcmp(fsname, tmpfsname) != 0) {
                infop->is_multivol = 1;
            }
            free(tmpfsname);
        } else if (num_links > 1) {  /* fall back to link count */
            infop->is_multivol = 1;
        }
    }

    free(fsname);
    return (infop);
}


/* NAME: advfs_extendfs_generic(Prog, size, flags, argc, argv)
 *
 * DESCRIPTION:
 *   This routine is essentially the main function for both the AdvFS version
 *   of the "extendfs" command and the "fsadm extend" command. Those commands
 *   have a different initial parameter syntax which is munged into the size
 *   and flags arguments. The remaining parameter(s) are the same and are
 *   passed in via argc and argv in the following format:
 *      special
 *      OR
 *      [special] fsname
 *
 * INPUT:
 *   Prog:   the name of the calling program
 *   size:   the number of blocks to add to the volume(s)
 *   flags:  change the behavior of the extend (verbose, query)
 *   argc:   how many remaining arguments
 *   argv:   the remaining arguments
 *  
 * RETURN VALUE:       
 *   success:  0 
 *   failure:  non-zero
 *
 */
int
advfs_extendfs_generic(char *Prog, uint64_t size, int32_t flags, int argc, char **argv)
{
    arg_infoT * infop;
    char *fsname = NULL;
    char *fspath = NULL;
    char *blkfile = NULL;
    DIR *FD;
    char fs_dev_path[MAXPATHLEN];
    char dev_link_path[MAXPATHLEN];
    struct dirent *devs;
    int status = 0, ret = 0;
    int query = 0, verbose = 0;
    int fs_mode = 0;
    nl_catd catd;

    catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);

    /* 
     * Set flags. Verbose isn't relevant if we are only querying 
     * the device size 
     */
    if (flags & EXT_VFLAG) verbose = 1;
    if (flags & EXT_QFLAG) query = 1;
    if (query) verbose = 0;

    infop = process_advfs_arg(*argv);
    if (infop) {
        fsname = infop->fsname;
        fspath = infop->fspath;
        if (strlen(infop->blkfile) > 0) {
            blkfile =infop->blkfile;
        }
    }

    /*
     * Sanity check our arguments 
     */
    if (argc == 1 && !fsname) {
        fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY29,
                        "%s: error processing specified name %s\n"),
                Prog, *argv);
        return(1);
    }
    else if ((argc == 2) && ((! blkfile) ||
            ((strcmp(*(argv+1), fsname) != 0) &&
            (strcmp(*(argv+1), fspath) != 0)))) {
        fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY30,
                        "%s: filesystem and device mismatch: %s, %s\n"),
                Prog,
                *argv, *(argv+1));
        return(1);
    }
    else if ((argc == 1 && !blkfile) && (size != -1)) {
        fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY31,
                        "%s: size argument requires a device name\n"), Prog);
        return(1);
    }

    /* 
     * If only a filesystem was specified, extend all volumes to fill the
     * whole disk. If a device is specified, only extend that volume
     */
    if (argc == 1 && !blkfile) fs_mode = 1;

    if (fs_mode) {
        /* Force size to -1 (fill volume) */
        size = -1;

        /* open the file system directory */
        FD = opendir(infop->stgdir);
        if (FD == NULL) {
            fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY32,
                            "%s: Could not open %s\n"), Prog, infop->stgdir);
            return(1);
        }
    }

    /* There are cheeseburgers, so are there infinite loops in paradise? */
    while (1) {
        if (fs_mode) {
            /* read each device link in the file system directory */
            if ((devs = readdir(FD)) == NULL) {
                /* 
                 * For whole filesystem mode, we're done after 
                 * processing all volumes in the filesystem.
                 */
                break;
            }
            if (devs->d_name[0] == '.') {
                continue;
            }

            strcpy(fs_dev_path, infop->stgdir);
            strcat(fs_dev_path, "/");
            strcat(fs_dev_path, devs->d_name);
            /* Get the path to this device */
            if (readlink(fs_dev_path, dev_link_path, MAXPATHLEN) < 0) {
                fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY33,
                                "%s: Could not read link %s\n"), 
                        Prog, fs_dev_path);
                continue;
            }
            blkfile = dev_link_path;
        }

        if (flags & EXT_QFLAG) {
            long BlkSize;
            get_dev_size(blkfile, NULL, &BlkSize);
            fprintf(stdout, catgets(catd, S_LIBRARY1, LIBRARY34,
                            "%s: Disk size of %s is %ld\n"), 
                    Prog, blkfile, BlkSize);
        } else {
            bf_vd_blk_t oldBlkSize = 0;
            bf_vd_blk_t newBlkSize = 0;
            if((status = advfs_extendfs(fsname, blkfile, size, flags,
                                    &oldBlkSize, &newBlkSize)) != EOK) {
                /* At least one volume failed to extend */
                ret = 1;
                switch (status) {
                case EFAULT:
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY37,
                            "%s: Error extending %s: filesystem activation failure\n"),
                            Prog, blkfile);
                    break;
                case EINVAL:
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY38,
                            "%s: Error extending %s: extend size too small\n"),
                            Prog, blkfile);
                    break;
                case ENOTEMPTY:
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY39,
                            "%s: Error extending %s: device contains swap\n"), 
                            Prog, blkfile);
                    break;
                default:
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY35,
                            "%s: Error extending %s: %s (%ld)\n"), 
                            Prog, blkfile, BSERRMSG(status), status);

                }
            } else {
                if (oldBlkSize > 0) {
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY36,
                                    "%s: Extended %s from %ld to %ld blocks\n"),
                            Prog, blkfile, oldBlkSize, newBlkSize);
                } else {
                    /* if oldBlkSize is zero, than this means the extend
                     * happened right before node failure and we don't have
                     * the oldBlkSize after recovery */
                    fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY40,
                                    "%s: Extended %s to %ld blocks\n"),
                            Prog, blkfile, newBlkSize);
                }
            }
        }
        /* 
         * If a volume was specified (non file system mode), then we are done
         * after the first pass
         */
        if (! fs_mode) {
            break;
        }
    }

    if (fs_mode) {
        closedir(FD);
    }

    return(ret);
}


/* NAME: advfs_valid_sblock(dev_name, ret_error)
 *
 * DESCRIPTION:
 *   advfs_valid_sblock checks the superblock of the device specified
 *   for the AdvFS magic number.  This function assumes that the 
 *   AdvFS magic number is stored at the same byte offset as HFS.  Therefore,
 *   it uses the HFS superblock structure in order to read the superblock.
 *   
 * INPUT:
 *   dev_name : device path name
 *   error : set to errno if an error occurs.
 * 
 * RETURN VALUES:       
 *   0 : no valid AdvFS magic number found.
 *   1 : valid AdvFS magic number found.
 *  -1 : error occurred.  ret_error variable set to errno
 */

int 
advfs_valid_sblock(char *dev_name, int *ret_error) {
    int fd;                     /* device file handle */
    char sbbuf[SBSIZE];         /* buffer to read super block */
    struct fs *fsp;             /* super block structure */

    if ((fd = open(dev_name, O_RDONLY)) == -1) {
        *ret_error = errno;
        return -1;
    }

    if (lseek(fd, (off_t) dbtoo(SBLOCK), SEEK_SET) == -1) {
        *ret_error = errno;
        return -1;
    }

    if (read(fd, sbbuf, SBSIZE) != SBSIZE) {
        *ret_error = errno;
        return -1;
    }

    fsp = (struct fs *)sbbuf;
    if (ADVFS_VALID_FS_MAGIC(fsp)) {
        close(fd);
        return 1;
    }

    close(fd);
    return 0;
}

/*  This function tells the caller if the specified filesystem/fileset
 *  is mounted, and, optionally, returns mntargs.
 * 
 *  mntdir and mntopts, if requested, will be malloc()ed string and 
 *  should be free()d by the caller.
 *
 *  Returns:   0   Filesystem is not mounted
 *             1   Filesystem is mounted and AdvFS
 *             2   Filesystem is mounted and _not_ AdvFS
 *            -1   Something Bad Happened
 */
int
advfs_is_mounted(
    arg_infoT*  infop,		/* in, filesystem info struct */
    char*       fset,           /* in, optional, fileset name to check */
    char**      mntopts,        /* out, optional, mountopts of filesystem */
    char**      mntdir          /* out, optional, directory mounted on */   
)
{
    int             ret = 0;
    struct mntent*  mntp;
    FILE*           mtab = NULL;
    char            mntpath[MAXPATHLEN+1] = {0};

    if (!infop) {
        return(-1);
    }

    mtab = setmntent(MNT_MNTTAB, "r");
 
    if (mtab == NULL) {
        return(-1);
    }

    rewind(mtab);

    if (infop->is_multivol) {
        if (fset) {
            snprintf(mntpath, MAXPATHLEN+1, "%s/%s", infop->fspath, fset);
        } else {
            snprintf(mntpath, MAXPATHLEN+1, "%s/%s", infop->fspath, 
                infop->fset);
        }
    } else { 
        /* non-multivol filesystems should have 1 and only 1 device 
         * so this should never happen.  But just in case... 
         */
        if (infop->blkfile[0] == '\0') {
            endmntent(mtab);
            return(-1);
        }
        strcpy(mntpath, infop->blkfile);
    }

    while ((mntp = getmntent(mtab)) != NULL) {
        if (strcmp(mntpath, mntp->mnt_fsname) == 0) {
            if (strcmp(mntp->mnt_type, MNTTYPE_ADVFS) == 0) {
                ret = 1;
            } else {
                ret = 2;
            }
            if (mntopts != NULL) {
                *mntopts = strdup(mntp->mnt_opts);
            }

            if (mntdir != NULL) {
                *mntdir = strdup(mntp->mnt_dir);
            }
            break;
        }
    }

    endmntent(mtab);
    return(ret);
}

/*  
 *  This function tries to intelligently create the AdvFS directory structure 
 *  in /dev and /cdev.  
 *
 *  First, it checks that /cdev/advfs exists and is accessible.  If it is
 *  not, it is created.
 *  
 *  Next, if /dev/advfs exists, and is not a symlink to /cdev/advfs, it
 *  is renamed.
 *
 *  Lastly, the symlink is made between /cdev/advfs and /dev/advfs.
 *
 *  Returns:   0   Hierarchy successfully created
 *             1   Something Bad Happened
 *
 */
int
create_advfs_hierarchy(
    void
)
{
    struct stat    statBuf;
    int            ret = 0; 
    int            mkadvfs = 0;
    clu_type_t     clutype;
    char           devadvfs[MAXPATHLEN+1];
    char           tmppath[MAXPATHLEN+1];
    int            myerrno;
    nl_catd        catd;

    snprintf(tmppath, MAXPATHLEN+1, "%s_bad_%d", ADVFS_DMN_DIR, getpid());

    clutype = clu_type();

    if (clutype == CLU_TYPE_CFS) {
        strcpy(devadvfs, "/etc/cfs/dev/.cdsl_files/{memb}/advfs");
    } else {
        strcpy(devadvfs, ADVFS_CDMN_DIR);
    }

    ret = lstat(ADVFS_DMN_DIR, &statBuf);

    mkadvfs = stat(ADVFS_DMN_DIR, &statBuf);

    if (mkadvfs == 0) {
        if (!S_ISDIR(statBuf.st_mode)) {
            /* rename whatever this is out of the way */
            rename(ADVFS_DMN_DIR, tmppath);
            mkadvfs = 1;
        } 
    } else if (ret == 0) {
        rename(ADVFS_DMN_DIR, tmppath);
    }

    if (mkadvfs) {
        char*    pp = NULL;
        char*    ptr = NULL;
        char*    tokstr;

        ret = 0;

        tmppath[0] = '\0';
        tokstr = strdup(devadvfs);

        if (tokstr == NULL) {
            return 1;
        }

        ptr = strtok_r(tokstr, "/", &pp);

        while (ptr != NULL) {
            strcat(tmppath, "/");
            strcat(tmppath, ptr);

            ret = stat(tmppath, &statBuf);

            if (ret != 0) {
                if (errno == ENOENT) {
                    ret = mkdir(tmppath, 0755);
                }
            }

            if (ret != 0) {
                break;
            }

            ptr = strtok_r(NULL, "/", &pp);
        }

        free(tokstr);

        if (ret == 0) {
            ret = symlink(devadvfs, ADVFS_DMN_DIR);
        }

        if (ret != 0) {
            myerrno = errno;
   	    catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);	
            fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY18, 
                "Unable to create directory %s; [%d] %s\n"),
                devadvfs, myerrno, ERRMSG(myerrno));
            (void) catclose(catd);
            
            return 1;
        }
    } 

    if (clutype != CLU_TYPE_CFS) {
        /*  all set for base and SSI clusters */
        return 0;
    }

    if (clutype == CLU_TYPE_CFS) {
        char*    devadvfscfs = "/etc/cfs/dev/advfs_cfs";

        mkadvfs = stat(ADVFS_TCR_DMN_DIR, &statBuf);

        if (mkadvfs == 0) {
            if (!S_ISDIR(statBuf.st_mode)) {
                /* rename whatever this is out of the way */
                snprintf(tmppath, MAXPATHLEN+1, "bad_%s_%d", ADVFS_TCR_DMN_DIR, 
                    getpid());
                rename(ADVFS_TCR_DMN_DIR, tmppath);
                mkadvfs = 1;
            } 
        }

        if (mkadvfs == 0) {
            return 0;
        }

        ret = stat("/etc/cfs/dev", &statBuf);

        if ((ret != 0) && (errno == ENOENT)) {
            ret = mkdir("/etc/cfs/dev", 0755);
        }

        if (ret == 0) {
            ret = stat("/etc/cfs/dev/advfs_cfs", &statBuf);
            if ((ret != 0) && (errno == ENOENT)) {
                ret = mkdir(devadvfscfs, 0755);
            }
            if (ret == 0) {
                ret = symlink(devadvfscfs, ADVFS_TCR_DMN_DIR);
            }
        }

        if (ret != 0) {
            myerrno = errno;
   	    catd = catopen(MF_LIBADVFS, NL_CAT_LOCALE);	
            fprintf(stderr, catgets(catd, S_LIBRARY1, LIBRARY18, 
                "Unable to create directory %s; [%d] %s\n"),
                devadvfscfs, myerrno, ERRMSG(myerrno));
            (void) catclose(catd);
            
            return 1;
        }
    }

    return 0;
}

/*
 * get_minor_number():
 *     Determine the next available minor number for use with mkdev().
 *
 *     lock_file() needs to be called with DOT_MKFS_LOCK, before calling
 *     adv_get_minor_number() to prevent multiple mkfs/scan processes from
 *     trying to create the same device id.
 */

static int
adv_get_minor_number(int tcr)
{
    char       advfspath[MAXPATHLEN+1] = {0};
    uint32_t   devno = 0;
    int        st;

    if (!tcr) {
        snprintf(advfspath, MAXPATHLEN, "%s/", ADVFS_DMN_DIR);
    } else {
        snprintf(advfspath, MAXPATHLEN, "%s/", ADVFS_TCR_DMN_DIR);
    }

    st = nftw(advfspath, adv_do_ftw, OPEN_MAX, FTW_PHYS|FTW_MOUNT|FTW_SERR);
    if (st != 0) {
        /*  callers expect 0 on failure */
        return(0);
    }

    if (hi_devno < ADVDEV_MINOR_MAX) {
        devno = hi_devno + 1;
    } else if (lo_devno > 1) {
        devno = lo_devno - 1;
    }

    if (tcr && (devno > 0)) {
        ADVDEV_SET_TCRDEV(devno);
    }

    return(devno);
}

static int
adv_do_ftw(const char *obj_path,
       const struct stat *obj_stat,
       int obj_flags,
       struct FTW obj_FTW)
{
    uint32_t   devno;
    uint32_t   major_dev;

    if (obj_flags != FTW_F) {
        return(0);
    }

    major_dev = major(obj_stat->st_rdev);

    if (major_dev == ADVFSDEV_MAJOR) {
        devno = minor(obj_stat->st_rdev);

        /* clear the tcr dev id */
        ADVDEV_CLEAR_TCRDEV(devno);

        if (lo_devno == 0) {
            lo_devno = devno;
        }

        if (devno < lo_devno) {
            lo_devno = devno;
        }

        if (devno > hi_devno) {
            hi_devno = devno;
        }
    }

    return(0);
}

/* 
 *  advfs_make_blockdev()
 *
 *  This function makes an Advfs block device at the path
 *  specified.
 *
 *  Used by mkfs and fsadm scan
 *
 *  Caller is responsible for ensuring that is_tcr and is_stripe is
 *  set correctly.  No extra checking is done here.
 *
 *  Returns:
 *      0   Device was successfully created.
 *      1   Device creation failed.
 *
 */
int
advfs_make_blockdev(
    char*  pathname,
    char*  fsetname,
    int    is_tcr,
    int    is_stripe
)
{
    uint32_t newdevno = -1;          /* Minor number for pseudodevice */
    dev_t    newdev;                 /* dev_t for pseudodevice */
    int      lockfd = -1;            /* lock held while creating pseudodev */
    char     devpath[MAXPATHLEN+1];  /* used to construct device pathname */
    int      sts = 0;
    int      sts2;
    char*    topdir = ADVFS_DMN_DIR;
    char     lockfile[MAXPATHLEN+1] = {0};

    /* lock ADVFS_MKFS_LOCK while mkfs determines the minor
     * version number for the new pseudodevice and creates the pseudodevice
     */

    if (is_tcr) {
        topdir = ADVFS_TCR_DMN_DIR;
    }
  
    snprintf(lockfile, MAXPATHLEN+1, "%s/%s", topdir, DOT_MKFS_LOCK);

    lockfd = lock_file(lockfile, F_WRLCK, &sts);
    if (lockfd < 0)
    {
        return(1);
    }

    newdevno = adv_get_minor_number(is_tcr);

    if (newdevno == 0) {
        unlock_file(lockfd, &sts);
        return(1);
    }

    newdev = makedev(ADVFSDEV_MAJOR, newdevno);

    snprintf(devpath, MAXPATHLEN, "%s/%s", pathname, fsetname);
    sts = mknod(devpath, _S_IFBLK|S_IRUSR|S_IWUSR|S_IRGRP, newdev);

    /* release the lock */
    unlock_file(lockfd, &sts2);
 
    return(sts);
}

/*
 *  advfs_get_vol_index()
 *
 *  Function to return the volume index of a given device
 *
 *  Pass NULL for optional returns if they are undesired.
 *
 *  In:  
 *      char*            fsname         Filesystem name
 *      char*            volName        Volume name to be matched
 *      
 *  Out:
 *      vdIndexT*             index          Volume index in filesystem
 *      serviceClassT* volSvcClass    Optional.  Volume Service Class
 *      adv_vol_ioq_params_t* volIoQ         Optional.  Volume IOQ Params
 *
 */
adv_status_t
advfs_get_vol_index(char* fsname, 
                    char* volName, 
                    vdIndexT* index, 
                    serviceClassT* volSvcClass,
                    adv_vol_ioq_params_t *volIoQ
)
{
    vdIndexT       *volArray;
    adv_status_t    sts;
    adv_bf_dmn_params_t fsParams;
    int              numVols;
    int              i;
    adv_vol_info_t   volInfo;
    adv_vol_prop_t   volProps;
    adv_vol_ioq_params_t volIoQParams;
    adv_vol_counters_t   volCounters;

    if (fsname == NULL || volName == NULL) {
        return(EBAD_PARAMS);
    }

    sts = advfs_get_dmnname_params( fsname, &fsParams );
    if (sts != EOK) {
        return(sts);
    }

    *index = 0;  /* invalid index */

    volArray = malloc(fsParams.maxVols * sizeof(vdIndexT));

    if (volArray == NULL) {
        return(ENO_MORE_MEMORY);
    }

    sts = advfs_get_dmn_vol_list(fsParams.bfDomainId, 
                                fsParams.maxVols,
                                volArray,
                                &numVols);

    if (sts != EOK) {
        free(volArray);
        return(sts);
    }

    for (i = 0 ; i < numVols ; i++ ) {
        sts = advfs_get_vol_params( fsParams.bfDomainId,
                                    volArray[i],
                                    &volInfo,
                                    &volProps,
                                    &volIoQParams,
                                    &volCounters );
    
        if (sts != EOK) {
            continue;   /* shouldn't happen */
        }

        if (strcmp(volName, volProps.volName) == 0) {
            *index = volArray[i];
            if (volSvcClass != NULL) {
                *volSvcClass = volProps.serviceClass;
            }
            if (volIoQ != NULL) {
                memcpy(volIoQ, &volIoQParams, sizeof(volIoQParams));
            }
            break;
        }
    }

    free(volArray);
    return(sts);

}

/*
 *  advfs_get_all_mounted()
 *
 *  Checks for all filesystems associated with a domain and optionally
 *  returns an array containing the mounted filesystems and their 
 *  mntondirs.
 *
 *  In:  
 *      arg_info_t*      infop          Filesystem information struct 
 *      adv_mnton_t**    mntArray       Pointer to return mnton information,
 *                                      or NULL if not desired.
 *                                      Returned information must be free()d
 *                                      by the caller.
 *      
 *  Return value:  The number of filesystems found.  
 *                 0 if none
 *                 -1 if unable to read mntent or failure to allocate memory.
 */
int
advfs_get_all_mounted(
    arg_infoT*     infop,
    adv_mnton_t**  mntArray
)
{    
    struct mntent*  mntp;
    FILE*           mtab = NULL;
    char            mntpath[MAXPATHLEN+1];
    int             fsIncrement = 20;  /* size of each array allocation */
    adv_mnton_t*    tmpArray = NULL;   /* temporary storage of results */
    int             numfs = 0;         /* number of mounted fileystems */
    int             maxfs = fsIncrement; /* size of allocated tmpArray */

    if (!infop) {
        return(-1);
    }

    mtab = setmntent(MNT_MNTTAB, "r");
 
    if (mtab == NULL) {
        return(-1);
    }

    rewind(mtab);

    if (mntArray != NULL) {
        tmpArray = calloc(fsIncrement, sizeof(adv_mnton_t));
     
        if (tmpArray == NULL) {
            endmntent(mtab);
            return(-1);
        }
    }

    snprintf(mntpath, sizeof(mntpath), "%s/", infop->fspath);

    while ((mntp = getmntent(mtab)) != NULL) {
        /* Compare against both the fsname and the blkfile, as 
         * we don't know how this filesystem was mounted 
         */
        if ( (strncmp(mntpath, mntp->mnt_fsname, strlen(mntpath)) == 0) ||
             (infop->blkfile[0] != '\0' && 
              strcmp(infop->blkfile, mntp->mnt_fsname) == 0) ) {

            if (tmpArray != NULL) {
                if (numfs == maxfs) { /* time to realloc */
                    adv_mnton_t*  ptr = NULL;

                    maxfs += fsIncrement;

                    ptr = realloc(tmpArray, maxfs * sizeof(adv_mnton_t));
                    if (ptr == NULL)  {
                        /*  probably out of memory. */
                        endmntent(mtab);
                        free(tmpArray);
                        return(-1);
                    }
                    tmpArray = ptr;
                }
                snprintf(tmpArray[numfs].fspath, MAXPATHLEN+1, "%s", 
                    mntp->mnt_fsname);
                snprintf(tmpArray[numfs].mntdir, MAXPATHLEN+1, "%s", 
                    mntp->mnt_dir);
            }

            numfs++;
        }
    }

    endmntent(mtab);

    if (tmpArray != NULL) {
        *mntArray = tmpArray;
    }

    return(numfs);
}

/*
 * advfs_is_multivol
 *
 * This function checks to see whether the filesystem is multi-volume.
 *
 * Returns 0 if single volume, 1 if multi-volume.
 * On error, sets ret_error to errno.
 *
 */

int
advfs_is_multivol(
    char*    devspec,
    int*     ret_err
)
{
    int                      fd = -1;            /* device file handle */
    int                      ret;
    advfs_fake_superblock_t* superBlk = NULL;    /* super block structure */
    char*                    cdev;
    char*                    ptr;
    char                     tmpdev[MAXPATHLEN+1];
    struct stat              statbuf;
    size_t                   sbsize = ADVFS_SUPER_BLOCK_SZ;
    off_t                    sboff = ADVFS_FAKE_SB_BLK * DEV_BSIZE;

 
    *ret_err = 0;

    ret = stat(devspec, &statbuf);
    if (ret != 0) {
        *ret_err = errno;
        return 0;
    }

    if (S_ISBLK(statbuf.st_mode)) {
        ptr = __blocktochar(devspec, tmpdev);
        if (ptr == NULL) {
            *ret_err = errno;
            return 0;
        }
        cdev = tmpdev;
    } else {
        cdev = devspec;
    }

    superBlk = (advfs_fake_superblock_t *) malloc(sbsize);
    if (superBlk == NULL) {
        *ret_err = ENOMEM;
        return 0;
    }

    if ((fd = open(cdev, O_RDONLY)) == -1) {
        *ret_err = errno;
        goto multi_error;
    }

    if (lseek(fd, sboff, SEEK_SET) == -1) {
        *ret_err = errno;
        goto multi_error;
    }

    if (read(fd, superBlk, sbsize) != sbsize) {
        *ret_err = errno;
        goto multi_error;
    }

    if (!ADVFS_VALID_FS_MAGIC(superBlk)) {
        *ret_err = EINVAL;
        goto multi_error;
    }

    ret = (superBlk)->adv_multi_vol_flag;
    *ret_err = 0;

multi_error:
    free(superBlk);

    if (fd != -1) {
        close(fd);
    }

    return ret;
}

/*
 * advfs_set_multivol
 *
 * This function modifies the device superblock to indicate the AdvFS
 * filesystem is enabled for multiple volumes.  Should be called 
 * when a single volume storage domain is multi-volume enabled and then 
 * again for each volume added to the storage domain.
 *
 * The multiple volume flag is initialized to 0 (false) at filesystem 
 * initialization (mkfs).
 *
 * Returns 0 if success, errno if failure.
 *
 */
int
advfs_set_multivol(
    char*    devspec
)
{
    int                      fd;               /* device file handle */
    int                      ret;
    advfs_fake_superblock_t* superBlk;         /* super block structure */
    char*                    cdev;
    char                     tmpdev[MAXPATHLEN+1];
    struct stat              statbuf;
    size_t                   sbsize = ADVFS_SUPER_BLOCK_SZ;
    off_t                    sboff = ADVFS_FAKE_SB_BLK * DEV_BSIZE;
    char*                    ptr;

    ret = stat(devspec, &statbuf);
    if (ret != 0) {
        return errno;
    }

    if (S_ISBLK(statbuf.st_mode)) {
        ptr = __blocktochar(devspec, tmpdev);
        if (ptr == NULL) {
            return errno;
        }
        cdev = tmpdev;
    } else {
        cdev = devspec;
    }

    superBlk = (advfs_fake_superblock_t *) malloc(sbsize);
    if (superBlk == NULL) {
        return ENOMEM;
    }

    if ((fd = open(cdev, O_RDWR)) == -1) {
        ret = errno;
        goto multi_error;
    }

    if (lseek(fd, sboff, SEEK_SET) == -1) {
        ret = errno;
        goto multi_error;
    }

    if (read(fd, superBlk, sbsize) != sbsize) {
        ret = errno;
        goto multi_error;
    }

    /*  Set multi_vol_flag */
    (superBlk)->adv_multi_vol_flag = 1;

    if (lseek(fd, sboff, SEEK_SET) == -1) {
        ret = errno;
        goto multi_error;
    }

    if (write(fd, superBlk, sbsize) != sbsize) {
        ret = errno;
        goto multi_error;
    }

multi_error:
    free(superBlk);
    close(fd);

    return ret;
}


/* end library.c */

