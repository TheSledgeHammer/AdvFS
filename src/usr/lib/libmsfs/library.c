/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 *      ADVFS system call interface library.
 *
 * Date:
 *
 *      Wed Aug 14 15:30:39 1991
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: library.c,v $ $Revision: 1.1.92.5 $ (DEC) $Date: 2006/03/01 22:24:28 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#ifdef _OSF_SOURCE
#include <sys/disklabel.h>
#endif
#include <sys/limits.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <io/common/devgetinfo.h>
#include <overlap.h>
#include <msfs/advfs_evm.h>

#include "libmsfs_msg.h"

typedef struct _dName {
    char name[256];
    struct _dName *next;
} dNameT;

#define ERRMSG( e ) sys_errlist[e]
extern int errno;

/*
 * All calling applications which could potentially corrupt
 * versions of metadata that were introduced after the
 * applications themselves were built must first go through 
 * msfs_check_on_disk_version() to set this to TRUE
 * before making other library calls.
 */
static int checked_on_disk_version = FALSE;

/* 
 * Check that the caller knows about the on-disk structures
 * on this system.
 *
 * Since there is no locking done around checking and setting of
 * checked_on_disk_version, this library, or at least those calls that 
 * use this variable, are not thread-safe. There is no problem with 
 * this right now due to the check-once way it is used.
 */
mlStatusT 
msfs_check_on_disk_version( 
    int on_disk_version_known_to_caller
    )
{
    if (on_disk_version_known_to_caller < BFD_ODS_LAST_VERSION) {
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
mlStatusT 
msfs_skip_on_disk_version_check()
{
    checked_on_disk_version = TRUE;
    return EOK;
}

static int
advfs_syscall(
    opTypeT opType,
    libParamsT *parms,
    int parmsLen
    )
{
    nl_catd catd;
    int sts;

    sts = msfs_syscall( opType, parms, parmsLen );

    if (sts == -1) {

        if (errno == ENOSYS) {
            return E_ADVFS_NOT_INSTALLED;

        } else {
	    catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
            fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY1, "**** ADVFS warning ****\n") );
            fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY2, "The command failed.\n") );
            fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY3, "A valid error message cannot be reported.\n\n") );
	    (void) catclose(catd);
            return errno;
        }

    } else {
        return sts;
    }
}

/* Packs the parameters from user mode and calls msfs_syscall to
   transfer them to kernel mode */

mlStatusT 
advfs_remove_name( 
    int dirFd, 
    char *name
    )
{
    libParamsT buf;
    int  sts;

    buf.removeName.dirFd = dirFd;
    buf.removeName.name = name;

    return advfs_syscall(ADVFS_REM_NAME, &buf, sizeof( buf.removeName ));
}

mlStatusT
advfs_remove_bf(
    ino_t rem_ino,
    int fd,
    int flag 
    )
{
    libParamsT buf;
    nl_catd catd;

    buf.removeBf.ino = rem_ino;
    buf.removeBf.dirFd = fd;
    buf.removeBf.flag = flag;
    return advfs_syscall(ADVFS_REM_BF, &buf, sizeof(buf.removeBf));
}

mlStatusT
msfs_undel_attach(
    char *dirName,
    char *undelDirName
    )
{
    libParamsT buf;
    int  sts;

    buf.undelAttach.dirName = dirName;
    buf.undelAttach.undelDirName = undelDirName;

    return advfs_syscall(ADVFS_UNDEL_ATTACH, &buf, sizeof( buf.undelAttach ));
}

mlStatusT
msfs_undel_detach(
    char *dirName
    )
{
    libParamsT buf;
    int  sts;

    buf.undelDetach.dirName = dirName;

    return advfs_syscall(ADVFS_UNDEL_DETACH, &buf, sizeof( buf.undelDetach ));
}

mlStatusT 
msfs_undel_get(
    char *dirName,              /* in  */
    mlBfTagT *undelDirTag       /* out */
    )
{
    libParamsT buf;
    int  sts;

    buf.undelGet.dirName = dirName;
    buf.undelGet.undelDirTag = *undelDirTag;

    sts = advfs_syscall(ADVFS_UNDEL_GET, &buf, sizeof( buf.undelGet ));

    *undelDirTag = buf.undelGet.undelDirTag;

    return sts;
}


mlStatusT 
msfs_get_name(
    char *mp,           /* in  */
    mlBfTagT tag,       /* in */
    char *name,         /* out  */
    mlBfTagT *parentTag /* out */
    )
{
    libParamsT buf;
    int  sts;

    buf.getName.mp = mp;
    buf.getName.name = name;
    buf.getName.tag = tag;
    buf.getName.parentTag = *parentTag;

    sts = advfs_syscall(ADVFS_GET_NAME, &buf, sizeof( buf.getName ));

    *parentTag = buf.getName.parentTag;

    return sts;
}


mlStatusT 
advfs_get_idx_bf_params (
    int          fd,
    mlBfAttributesT  *bfAttributes,
    mlBfInfoT* bfInfo
    ) 
{
    libParamsT buf;
    int  sts;

    buf.getBfParams.fd = fd;
    buf.getBfParams.bfAttributes = *bfAttributes;
    buf.getBfParams.bfInfo = *bfInfo;

    sts = advfs_syscall(ADVFS_GET_IDX_BF_PARAMS, &buf, sizeof(buf.getBfParams));

    *bfAttributes = buf.getBfParams.bfAttributes;
    *bfInfo = buf.getBfParams.bfInfo;

    return sts;
}

mlStatusT
advfs_get_idxdir_bf_params (
    int          fd,
    mlBfAttributesT  *bfAttributes,
    mlBfInfoT* bfInfo
    )
{
    libParamsT buf;
    int  sts;

    buf.getBfParams.fd = fd;
    buf.getBfParams.bfAttributes = *bfAttributes;
    buf.getBfParams.bfInfo = *bfInfo;

    sts = advfs_syscall(ADVFS_GET_IDXDIR_BF_PARAMS, &buf,
                        sizeof(buf.getBfParams));

    *bfAttributes = buf.getBfParams.bfAttributes;
    *bfInfo = buf.getBfParams.bfInfo;

    return sts;
}

mlStatusT 
advfs_get_bf_params (
    int          fd,
    mlBfAttributesT  *bfAttributes,
    mlBfInfoT* bfInfo
    ) 
{
    libParamsT buf;
    int  sts;

    buf.getBfParams.fd = fd;
    buf.getBfParams.bfAttributes = *bfAttributes;
    buf.getBfParams.bfInfo = *bfInfo;

    sts = advfs_syscall(ADVFS_GET_BF_PARAMS, &buf, sizeof(buf.getBfParams));

    *bfAttributes = buf.getBfParams.bfAttributes;
    *bfInfo = buf.getBfParams.bfInfo;

    return sts;
}

mlStatusT 
advfs_set_bf_attributes (
    int          fd,
    mlBfAttributesT  *bfAttributes
    )
{
    libParamsT buf;
    int  sts;

    buf.setBfAttributes.fd = fd;
    buf.setBfAttributes.bfAttributes = *bfAttributes;

    sts = advfs_syscall(ADVFS_SET_BF_ATTRIBUTES,
                         &buf, 
                         sizeof( buf.setBfAttributes ));

    *bfAttributes = buf.setBfAttributes.bfAttributes;

    return sts;
}

mlStatusT 
advfs_get_bf_iattributes (
    int          fd,
    mlBfAttributesT  *bfAttributes
    )
{
    libParamsT buf;
    int  sts;

    buf.getBfIAttributes.fd = fd;

    sts = advfs_syscall(ADVFS_GET_BF_IATTRIBUTES,
                         &buf, 
                         sizeof( buf.getBfIAttributes ));

    *bfAttributes = buf.getBfIAttributes.bfAttributes;

    return sts;
}

mlStatusT 
advfs_set_bf_iattributes (
    int          fd,
    mlBfAttributesT  *bfAttributes
    )
{
    libParamsT buf;
    int  sts;

    buf.setBfIAttributes.fd = fd;
    buf.setBfIAttributes.bfAttributes = *bfAttributes;

    sts = advfs_syscall(ADVFS_SET_BF_IATTRIBUTES,
                         &buf, 
                         sizeof( buf.setBfIAttributes ));

    *bfAttributes = buf.setBfIAttributes.bfAttributes;

    return sts;
}

mlStatusT 
advfs_move_bf_metadata (
    int fd
    )
{
    libParamsT buf;

    buf.moveBfMetadata.fd = fd;

    return advfs_syscall (
                         ADVFS_MOVE_BF_METADATA,
                         &buf, 
                         sizeof( buf.moveBfMetadata )
                         );
}

mlStatusT 
advfs_switch_log(
    char *domainName,
    u32T volIndex,
    u32T logPgs,
    mlServiceClassT logSvc
    )
{
    libParamsT buf;

    buf.switchLog.domainName = domainName;
    buf.switchLog.volIndex = volIndex;
    buf.switchLog.logPgs = logPgs;
    buf.switchLog.logSvc = logSvc;

    return advfs_syscall(ADVFS_SWITCH_LOG, &buf, sizeof( buf.switchLog ));
}

mlStatusT 
advfs_switch_root_tagdir(
    char *domainName,
    u32T volIndex
    )
{
    libParamsT buf;

    buf.switchRootTagDir.domainName = domainName;
    buf.switchRootTagDir.volIndex = volIndex;

    return advfs_syscall(ADVFS_SWITCH_ROOT_TAGDIR, &buf, sizeof( buf.switchRootTagDir ));
}

mlStatusT 
advfs_get_vol_params (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    mlVolInfoT* volInfop,
    mlVolPropertiesT* volPropertiesp,
    mlVolIoQParamsT* volIoQParamsp,
    mlVolCountersT* volCountersp
    )
{
    libParamsT buf;
    int  sts;
    int flag = 0;

    buf.getVolParams.bfDomainId = bfDomainId;
    buf.getVolParams.volIndex = volIndex;
    if (!volInfop) flag |= GETVOLPARAMS_NO_BMT;
    buf.getVolParams.flag = flag;

    sts = advfs_syscall(ADVFS_GET_VOL_PARAMS, 
                         &buf, 
                         sizeof( buf.getVolParams ));

    if (volInfop) *volInfop = buf.getVolParams.volInfo;
    *volPropertiesp = buf.getVolParams.volProperties;
    *volIoQParamsp = buf.getVolParams.volIoQParams;
    *volCountersp = buf.getVolParams.volCounters;

    return sts;
}

mlStatusT 
advfs_get_vol_params2 (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    mlVolInfoT* volInfop,
    mlVolPropertiesT* volPropertiesp,
    mlVolIoQParamsT* volIoQParamsp,
    mlVolCountersT* volCountersp,
    int flag
    )
{
    libParamsT buf;
    int  sts;

    buf.getVolParams.bfDomainId = bfDomainId;
    buf.getVolParams.volIndex = volIndex;
    if (!volInfop) flag |= GETVOLPARAMS_NO_BMT;
    buf.getVolParams.flag = flag;

    sts = advfs_syscall(ADVFS_GET_VOL_PARAMS2, 
                         &buf, 
                         sizeof( buf.getVolParams ));

    if (volInfop) *volInfop = buf.getVolParams.volInfo;
    *volPropertiesp = buf.getVolParams.volProperties;
    *volIoQParamsp = buf.getVolParams.volIoQParams;
    *volCountersp = buf.getVolParams.volCounters;

    return sts;
}

mlStatusT 
advfs_get_vol_bf_descs (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    int bfDescSize,
    mlBfDescT *bfDesc,
    u32T *nextBfDescId,
    int *bfDescCnt
    )

{
    libParamsT buf;
    int  sts;

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

    buf.getVolBfDescs.bfDomainId = bfDomainId;
    buf.getVolBfDescs.volIndex = volIndex;
    buf.getVolBfDescs.bfDescSize = bfDescSize;
    buf.getVolBfDescs.bfDesc = bfDesc;
    buf.getVolBfDescs.nextBfDescId = *nextBfDescId;

    sts = advfs_syscall (
                   ADVFS_GET_VOL_BF_DESCS, 
                   &buf, 
                   sizeof( buf.getVolBfDescs )
                   );

    *nextBfDescId = buf.getVolBfDescs.nextBfDescId;
    *bfDescCnt = buf.getVolBfDescs.bfDescCnt;

    return sts;
}

mlStatusT
advfs_set_vol_ioq_params (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    mlVolIoQParamsT *volIoQParamsp
    )
{
    libParamsT buf;
    int  sts;

    buf.setVolIoQParams.bfDomainId = bfDomainId;
    buf.setVolIoQParams.volIndex = volIndex;
    buf.setVolIoQParams.volIoQParams = *volIoQParamsp;

    return advfs_syscall(ADVFS_SET_VOL_IOQ_PARAMS, 
                        &buf, 
                        sizeof( buf.setVolIoQParams ));
}

mlStatusT 
msfs_get_bf_xtnt_map (
    int fd,
    int startXtntMap,
    int startXtnt,
    int xtntArraySize,
    mlExtentDescT *xtntsArray,
    int *xtntCnt,
    u32T *allocVolIndex
    )
{
    libParamsT buf;
    int  sts;

    buf.getBfXtntMap.fd = fd;
    buf.getBfXtntMap.startXtntMap = startXtntMap;
    buf.getBfXtntMap.startXtnt = startXtnt;
    buf.getBfXtntMap.xtntArraySize = xtntArraySize;
    buf.getBfXtntMap.xtntsArray = xtntsArray;

    sts = advfs_syscall(ADVFS_GET_BF_XTNT_MAP, 
                         &buf, 
                         sizeof( buf.getBfXtntMap ));

    *xtntCnt = buf.getBfXtntMap.xtntCnt;
    *allocVolIndex = buf.getBfXtntMap.allocVolIndex;

    return sts;
}

mlStatusT 
advfs_get_bkup_xtnt_map (
    int fd,
    int startXtntMap,
    int startXtnt,
    int xtntArraySize,
    mlExtentDescT *xtntsArray,
    int *xtntCnt,
    u32T *allocVolIndex,
    int *num_pages
    )
{
    libParamsT buf;
    int  sts;

    buf.getBkupXtntMap.fd = fd;
    buf.getBkupXtntMap.startXtntMap = startXtntMap;
    buf.getBkupXtntMap.startXtnt = startXtnt;
    buf.getBkupXtntMap.xtntArraySize = xtntArraySize;
    buf.getBkupXtntMap.xtntsArray = xtntsArray;

    sts = advfs_syscall(ADVFS_GET_BKUP_XTNT_MAP, 
                         &buf, 
                         sizeof( buf.getBkupXtntMap ));

    *xtntCnt = buf.getBkupXtntMap.xtntCnt;
    *allocVolIndex = buf.getBkupXtntMap.allocVolIndex;
    *num_pages = buf.getBkupXtntMap.num_pages;

    return sts;
}

mlStatusT 
advfs_get_lock_stats( mlAdvfsLockStatsT *lockStats )
{
    libParamsT buf;
    int  sts;

    sts = advfs_syscall(ADVFS_GET_LOCK_STATS, 
                  &buf, 
                  sizeof( buf.getLockStats ));

    *lockStats = buf.getLockStats;

    return sts;
}

mlStatusT 
msfs_get_dmn_params(
    mlBfDomainIdT bfDomainId,
    mlDmnParamsT *dmnParams
    )
{
    libParamsT buf;
    int  sts;

    buf.getDmnParams.bfDomainId = bfDomainId;
    buf.getDmnParams.dmnParams = *dmnParams;

    sts = advfs_syscall(ADVFS_GET_DMN_PARAMS, 
                         &buf, 
                         sizeof( buf.getDmnParams ));

    *dmnParams = buf.getDmnParams.dmnParams;

    return sts;
}

mlStatusT 
msfs_get_dmnname_params(
    char *domain,
    mlDmnParamsT *dmnParams
    )
{
    libParamsT buf;
    int  sts;

    /*
     * This entry point requires that the on-disk version be
     * checked.  The only reason is that the verify utility,
     * which has the potential to corrupt newer version
     * domains, calls this.  So the checking here prevents
     * an older version of verify from corrupting a newer
     * domain if the -f flag is used.  This causes some
     * other utilities which use this entry point to call either
     * msfs_check_on_disk_version() or msfs_skip_on_disk_version_check().
     * It also prevents older versions of some innocuous 
     * utilities such as showfdmn from running on newer domains.
     */
    if (!checked_on_disk_version) {
        return E_INVALID_FS_VERSION;
    }

    buf.getDmnNameParams.domain = domain;
    buf.getDmnNameParams.dmnParams = *dmnParams;

    sts = advfs_syscall(ADVFS_GET_DMNNAME_PARAMS, 
                    &buf, 
                    sizeof( buf.getDmnNameParams ));

    *dmnParams = buf.getDmnNameParams.dmnParams;

    return sts;
}

mlStatusT
msfs_get_dmn_vol_list(
    mlBfDomainIdT bfDomainId,/* in - the domain id */
    int volIndexArrayLen,    /* in - number of ints in array */
    u32T volIndexArray[],    /* out - list of volume indices */
    int *numVols             /* out - num volumes put in array */
    )
{
    libParamsT buf;
    int  sts;

    buf.getDmnVolList.bfDomainId = bfDomainId;
    buf.getDmnVolList.volIndexArrayLen = volIndexArrayLen;
    buf.getDmnVolList.volIndexArray = volIndexArray;

    sts = advfs_syscall(ADVFS_GET_DMN_VOL_LIST, 
                         &buf, 
                         sizeof( buf.getDmnVolList) );

    *numVols = buf.getDmnVolList.numVols;

    return sts;
}

mlStatusT 
msfs_get_bfset_params(
    mlBfSetIdT bfSetId,
    mlBfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    int  sts;

    buf.getBfSetParams.bfSetId = bfSetId;
    buf.getBfSetParams.bfSetParams = *bfSetParams;

    sts = advfs_syscall(ADVFS_GET_BFSET_PARAMS, 
                         &buf, 
                         sizeof( buf.getBfSetParams ));

    *bfSetParams = buf.getBfSetParams.bfSetParams;

    return sts;
}

mlStatusT 
msfs_set_bfset_params(
    mlBfSetIdT bfSetId,
    mlBfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    int  sts;

    buf.setBfSetParams.bfSetId = bfSetId;
    buf.setBfSetParams.bfSetParams = *bfSetParams;

    sts = advfs_syscall(ADVFS_SET_BFSET_PARAMS, 
                         &buf, 
                         sizeof( buf.setBfSetParams ));

    *bfSetParams = buf.setBfSetParams.bfSetParams;

    return sts;
}

mlStatusT
msfs_set_bfset_params_activate(
    char *dmnName,
    mlBfSetParamsT *bfSetParams
    )
{
    libParamsT buf;
    int sts;

    buf.setBfSetParamsActivate.dmnName = dmnName;
    buf.setBfSetParamsActivate.bfSetParams = *bfSetParams;

    sts = advfs_syscall(ADVFS_SET_BFSET_PARAMS_ACTIVATE,
                         &buf,
                         sizeof( buf.setBfSetParamsActivate ));

    *bfSetParams = buf.setBfSetParamsActivate.bfSetParams;

    return sts;
}

mlStatusT
msfs_fset_create( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlServiceClassT reqServ,  /* in - required service class */
    mlServiceClassT optServ,  /* in - optional service class */
    u32T fsetOptions,         /* in - fileset options */
    gid_t quotaId,            /* in - group ID for quota files */
    mlBfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    libParamsT buf;
    int  sts;

    buf.setCreate.domain = domain;
    buf.setCreate.setName = setName;
    buf.setCreate.reqServ = reqServ;
    buf.setCreate.optServ = optServ;
    buf.setCreate.fsetOptions = fsetOptions;
    buf.setCreate.quotaId = quotaId;
    buf.setCreate.bfSetId = *bfSetId;

    sts = advfs_syscall(ADVFS_FSET_CREATE,
                         &buf, 
                         sizeof( buf.setCreate ));

    *bfSetId = buf.setCreate.bfSetId;

    return sts;
}

mlStatusT
msfs_fset_get_id( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlBfSetIdT *bfSetId       /* out - bitfile set id */
    )
{
    libParamsT buf;
    int  sts;

    buf.setGetId.domain = domain;
    buf.setGetId.setName = setName;
    buf.setGetId.bfSetId = *bfSetId;

    sts = advfs_syscall(ADVFS_FSET_GET_ID,
                         &buf, 
                         sizeof( buf.setGetId ));

    *bfSetId = buf.setGetId.bfSetId;

    return sts;
}

mlStatusT
advfs_fset_get_stats( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlFileSetStatsT *fileSetStats
    )
{
    libParamsT buf;
    int  sts;

    buf.setGetStats.domain = domain;
    buf.setGetStats.setName = setName;

    sts = advfs_syscall(ADVFS_FSET_GET_STATS,
                         &buf, 
                         sizeof( buf.setGetStats ));

    *fileSetStats = buf.setGetStats.fileSetStats;

    return sts;
}

mlStatusT
msfs_fset_delete(
    char *domain,             /* in - domain name */
    char *setName             /* in - set's name */
    )
{
    libParamsT buf;

    buf.setCreate.domain = domain;
    buf.setCreate.setName = setName;

    return advfs_syscall(ADVFS_FSET_DELETE, &buf, sizeof( buf.setDelete ));
}

mlStatusT
msfs_fset_clone(
    char *domain,             /* in - domain name */
    char *origSetName,        /* in - set's name */
    char *cloneSetName,       /* in - clone's name */
    mlBfSetIdT *cloneSetId    /* out - clone's id */
    )
{
    libParamsT buf;
    int  sts;

    buf.setClone.domain = domain;
    buf.setClone.origSetName = origSetName;
    buf.setClone.cloneSetName = cloneSetName;

    sts = advfs_syscall(ADVFS_FSET_CLONE,
                         &buf, 
                         sizeof( buf.setClone ));

    *cloneSetId = buf.setClone.cloneSetId;

    return sts;
}

mlStatusT 
msfs_fset_get_info(
    char           *domain,             /* in */
    u32T           *nextSetIdx,         /* in/out */
    mlBfSetParamsT *bfSetParams,        /* out */
    u32T           *userId,             /* out */
    int            flag                 /* in, 1=don't run recovery */
    )
{
    libParamsT buf;
    int  sts;

    buf.setGetInfo.domain = domain;
    buf.setGetInfo.nextSetIdx = *nextSetIdx;
    buf.setGetInfo.bfSetParams = *bfSetParams;
    buf.setGetInfo.flag = flag;

    sts = advfs_syscall(ADVFS_FSET_GET_INFO, 
                         &buf, 
                         sizeof( buf.setGetInfo ));

    *nextSetIdx = buf.setGetInfo.nextSetIdx;
    *bfSetParams = buf.setGetInfo.bfSetParams;
    *userId = buf.setGetInfo.userId;

    return sts;
}

mlStatusT
msfs_fset_rename(
    char      *domain,                /* in */
    char      *origSetName,           /* in */
    char      *newSetName             /* in */
    )
{
    libParamsT buf;
    int sts;

    buf.setRename.domain = domain;
    buf.setRename.origSetName = origSetName;
    buf.setRename.newSetName = newSetName;

    sts = advfs_syscall(ADVFS_FSET_RENAME,
                      &buf,
                      sizeof( buf.setRename ));
    return sts;
}


mlStatusT 
advfs_tag_stat(
    mlBfTagT       *tag,                /* in/out */
    mlBfSetIdT     setId,               /* in */
    uid_t          *uid,                /* out */
    gid_t          *gid,                /* out */
    off_t          *size,               /* out */
    time_t         *atime,              /* out */
    mode_t         *mode                /* out */
    )
{
    libParamsT buf;
    int  sts;

    buf.tagStat.setId = setId;
    buf.tagStat.tag = *tag;

    sts = advfs_syscall(ADVFS_TAG_STAT, &buf, sizeof( buf.tagStat ));

    *tag = buf.tagStat.tag;
    *uid = buf.tagStat.uid;
    *gid = buf.tagStat.gid;
    *size = buf.tagStat.size;
    *atime = buf.tagStat.atime;
    *mode = buf.tagStat.mode;

    return sts;
}

mlStatusT
msfs_dmn_init(
    char *domain,               /* in - bf domain name */
    int maxVols,                /* in - maximum number of virtual disks */
    u32T logPgs,                /* in - number of pages in log */
    mlServiceClassT logSvc,     /* in - log service attributes */
    mlServiceClassT tagSvc,     /* in - tag directory service attributes */
    char *volName,              /* in - block special device name */
    mlServiceClassT volSvc,     /* in - service class */
    u32T  volSize,              /* in - size of the virtual disk */
    u32T bmtXtntPgs,            /* in - number of pages per BMT extent */
    u32T bmtPreallocPgs,        /* in - number of pages to preallocate for BMT */
    u32T domainVersion,         /* in - on-disk version of domain */
    mlBfDomainIdT *bfDomainId   /* out - domain id */
    )
{
    libParamsT buf;
    int sts;

    buf.dmnInit.domain = domain;
    buf.dmnInit.maxVols = maxVols;
    buf.dmnInit.logPgs = logPgs;
    buf.dmnInit.logSvc = logSvc;
    buf.dmnInit.tagSvc = tagSvc;
    buf.dmnInit.volName = volName;
    buf.dmnInit.volSvc = volSvc;
    buf.dmnInit.volSize = volSize;
    buf.dmnInit.bmtXtntPgs = bmtXtntPgs;
    buf.dmnInit.bmtPreallocPgs = bmtPreallocPgs;
    buf.dmnInit.domainVersion = domainVersion;

    sts = advfs_syscall(ADVFS_DMN_INIT, &buf, sizeof( buf.dmnInit ));

    *bfDomainId = buf.dmnInit.bfDomainId;

    return sts;
}

mlStatusT 
advfs_migrate (
    int fd,
    u32T srcVolIndex,
    u32T srcPageOffset, 
    u32T srcPageCnt, 
    u32T dstVolIndex,
    u32T dstBlkOffset,
    u32T forceFlag
    )
{
    libParamsT buf;

    buf.migrate.fd = fd;
    buf.migrate.srcVolIndex = srcVolIndex;
    buf.migrate.srcPageOffset = srcPageOffset;
    buf.migrate.srcPageCnt = srcPageCnt;
    buf.migrate.dstVolIndex = dstVolIndex;
    buf.migrate.dstBlkOffset = dstBlkOffset;
    buf.migrate.forceFlag = forceFlag;

    return advfs_syscall(ADVFS_MIGRATE, &buf, sizeof( buf.migrate ));
}

mlStatusT
msfs_add_volume( 
    char *domain,               /* in - domain name */
    char *volName,              /* in - block special device name */
    mlServiceClassT *volSvc,    /* in/out - service class */
    u32T volSize,               /* in - size of the virtual disk */
    u32T bmtXtntPgs,            /* in - number of pages per BMT extent */
    u32T bmtPreallocPgs,        /* in - number of pages to be pre-allocated for BMT */
    mlBfDomainIdT *bfDomainId,  /* out - the domain id */
    u32T *volIndex              /* out - the volume index */ 
    )
{
    libParamsT buf;
    int  sts;

    buf.addVol.domain = domain;
    buf.addVol.volName = volName;
    buf.addVol.volSvc = *volSvc;
    buf.addVol.volSize = volSize;
    buf.addVol.bmtXtntPgs = bmtXtntPgs;
    buf.addVol.bmtPreallocPgs = bmtPreallocPgs;

    sts = advfs_syscall(ADVFS_ADD_VOLUME, &buf, sizeof(buf.addVol));

    *bfDomainId = buf.addVol.bfDomainId;
    *volIndex = buf.addVol.volIndex;
    *volSvc = buf.addVol.volSvc;

    return sts;
}

mlStatusT
msfs_add_vol_done(
    char *domainName,           /* in - domain name */
    char *volName               /* in - block special device name */
    )
{
    libParamsT buf;

    buf.addRemVolDone.domainName = domainName;
    buf.addRemVolDone.volName = volName;
    buf.addRemVolDone.op = ML_VD_ADDVOL;

    return advfs_syscall(ADVFS_ADD_REM_VOL_DONE, &buf, sizeof(buf.addRemVolDone));
}   

mlStatusT
msfs_rem_vol_done(
    char *domainName,           /* in - domain name */
    char *volName               /* in - block special device name */
    )
{
    libParamsT buf;

    buf.addRemVolDone.domainName = domainName;
    buf.addRemVolDone.volName = volName;
    buf.addRemVolDone.op = ML_VD_RMVOL;

    return advfs_syscall(ADVFS_ADD_REM_VOL_DONE, &buf, sizeof(buf.addRemVolDone));
}   

mlStatusT
advfs_remove_volume( 
    mlBfDomainIdT bfDomainId,   /* in */
    u32T volIndex,  /* in */
    u32T forceFlag /* in */
    )

{
    libParamsT buf;

    buf.remVol.bfDomainId = bfDomainId;
    buf.remVol.volIndex = volIndex;
    buf.remVol.forceFlag = forceFlag;

    return advfs_syscall (
                    ADVFS_REM_VOLUME,
                    &buf,
                    sizeof(buf.remVol)
                    );
}

mlStatusT
advfs_add_vol_to_svc_class (
    char *domainName,
    u32T volIndex,
    mlServiceClassT volSvc
    )

{
    libParamsT buf;

    buf.addRemVolSvcClass.domainName = domainName;
    buf.addRemVolSvcClass.volIndex = volIndex;
    buf.addRemVolSvcClass.volSvc = volSvc;
    buf.addRemVolSvcClass.action = 0;

    return advfs_syscall (
                    ADVFS_ADD_REM_VOL_SVC_CLASS,
                    &buf,
                    sizeof(buf.addRemVolSvcClass)
                    );
}

mlStatusT
advfs_remove_vol_from_svc_class (
    char *domainName,
    u32T volIndex,
    mlServiceClassT volSvc
    )

{
    libParamsT buf;

    buf.addRemVolSvcClass.domainName = domainName;
    buf.addRemVolSvcClass.volIndex = volIndex;
    buf.addRemVolSvcClass.volSvc = volSvc;
    buf.addRemVolSvcClass.action = 1;

    return advfs_syscall (
                    ADVFS_ADD_REM_VOL_SVC_CLASS,
                    &buf,
                    sizeof(buf.addRemVolSvcClass)
                    );
}

mlStatusT
advfs_set_bf_next_alloc_vol (
    int fd,
    u32T curVolIndex,
    u32T pageCntNeeded,
    int forceFlag
    )

{
    libParamsT buf;

    buf.setBfNextAllocVol.fd = fd;
    buf.setBfNextAllocVol.curVolIndex = curVolIndex;
    buf.setBfNextAllocVol.pageCntNeeded = pageCntNeeded;
    buf.setBfNextAllocVol.forceFlag = forceFlag;

    return advfs_syscall (
                    ADVFS_SET_BF_NEXT_ALLOC_VOL,
                    &buf,
                    sizeof(buf.setBfNextAllocVol)
                    );
}

mlStatusT 
advfs_rewrite_xtnt_map (
    int fd,
    int xtntMapIndex
    )
{
    libParamsT buf;

    buf.rewriteXtntMap.fd = fd;
    buf.rewriteXtntMap.xtntMapIndex = xtntMapIndex;

    return advfs_syscall (
                         ADVFS_REWRITE_XTNT_MAP,
                         &buf, 
                         sizeof( buf.rewriteXtntMap )
                         );
}

mlStatusT
advfs_reset_free_space_cache (
        mlBfDomainIdT bfDomainId,
        u32T volIndex,
        int scanStartCluster)
{
    libParamsT buf;

        buf.resetFreeSpaceCache.bfDomainId = bfDomainId;
        buf.resetFreeSpaceCache.volIndex = volIndex;
        buf.resetFreeSpaceCache.scanStartCluster = scanStartCluster;

        return advfs_syscall (
                                                 ADVFS_RESET_FREE_SPACE_CACHE,
                                                 &buf,
                                                 sizeof( buf.resetFreeSpaceCache)
                                                 );
}

mlStatusT
advfs_ss_dmn_ops (
    char* domainName,
    int type,
    mlSSDmnOpsT *ssDmnCurrentState,
    int *ssRunningState
    )
{
    libParamsT buf;
    int sts;

    buf.ssDmnOps.domainName = domainName;
    buf.ssDmnOps.type = type;

    sts = advfs_syscall (
                         ADVFS_SS_DMN_OPS,
                         &buf,
                         sizeof( buf.ssDmnOps )
                         );

    *ssDmnCurrentState = buf.ssDmnOps.ssDmnCurrentState;
    *ssRunningState = buf.ssDmnOps.ssRunningState;

    return sts;
}

mlStatusT
advfs_ss_get_params (
    char* domainName,
    mlSSDmnParamsT *ssDmnParams
    )
{
    libParamsT buf;
    int sts;

    buf.ssGetParams.domainName = domainName;
    buf.ssGetParams.ssDmnParams = *ssDmnParams;

    sts = advfs_syscall (
                         ADVFS_SS_GET_PARAMS,
                         &buf,
                         sizeof( buf.ssGetParams )
                         );

    *ssDmnParams = buf.ssGetParams.ssDmnParams;

    return sts;
}

mlStatusT
advfs_ss_set_params (
    char* domainName,
    mlSSDmnParamsT ssDmnParams
    )
{
    libParamsT buf;
    int sts;

    buf.ssSetParams.domainName = domainName;
    buf.ssSetParams.ssDmnParams = ssDmnParams;

    sts = advfs_syscall (
                         ADVFS_SS_SET_PARAMS,
                         &buf,
                         sizeof( buf.ssSetParams )
                         );

    return sts;
}

mlStatusT
advfs_ss_get_fraglist (
    char*         domainName,
    vdIndexT      volIndex,
    int           ssStartFraglist,
    int           ssFragArraySize,
    mlFragDescT   *ssFragArray,
    int           *ssFragCnt
    )
{
    libParamsT buf;
    int sts;

    buf.ssGetFraglist.domainName = domainName;
    buf.ssGetFraglist.volIndex = volIndex;
    buf.ssGetFraglist.ssStartFraglist = ssStartFraglist;
    buf.ssGetFraglist.ssFragArraySize = ssFragArraySize;
    buf.ssGetFraglist.ssFragArray = ssFragArray;

    sts = advfs_syscall (
                         ADVFS_SS_GET_FRAGLIST,
                         &buf,
                         sizeof( buf.ssGetFraglist )
                         );

    buf.ssGetFraglist.ssFragArray = ssFragArray;
    *ssFragCnt = buf.ssGetFraglist.ssFragCnt;

    return sts;
}

mlStatusT
advfs_ss_get_hotlist (
    mlBfDomainIdT bfDomainId,
    int           ssStartHotlist,
    int           ssHotArraySize,
    mlHotDescT    *ssHotArray,
    int           *ssHotCnt
    )
{
    libParamsT buf;
    int sts;

    buf.ssGetHotlist.bfDomainId = bfDomainId;
    buf.ssGetHotlist.ssStartHotlist = ssStartHotlist;
    buf.ssGetHotlist.ssHotArraySize = ssHotArraySize;
    buf.ssGetHotlist.ssHotArray = ssHotArray;

    sts = advfs_syscall (
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

mlStatusT
tag_to_path (
             char *mntOnName,  /* in */
             mlBfTagT bfTag,  /* in */
             int bufSize,  /* in */
             char *buf  /* in, modified */
             )

{

    dNameT *tmp;
    dNameT *dNames = NULL, *dName;
    mlBfTagT parentTag, prevParentTag, dTag;
    struct stat rootStats; /* file set's root stats */
    mlStatusT sts;

    if (stat (mntOnName, &rootStats ) < 0) {
        return errno;
    }
    
    /*
     * The following loop calls msfs_get_name() to get the file/dir name 
     * string for a tag.  msfs_get_name() also returns the tag's parent
     * dir's tag.  So we keep calling msfs_get_name() until we've
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

    prevParentTag.num = 0;
    prevParentTag.seq = 0;

    do {
        /* Get a new pathname component struct */

        dName = (dNameT *) malloc( sizeof( dNameT ) );

        /* Get the current dir's name its parent's tag */

        sts = msfs_get_name( mntOnName,
                            dTag, 
                            dName->name, 
                            &parentTag );
        if (sts != EOK) {
            goto _error;
        }
        if (ML_BFTAG_EQL(parentTag, prevParentTag)) {
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

    } while (dTag.num != rootStats.st_ino);

    /*
     * Now we can print the full pathname of the bitfile.
     * First we print the mount point pathname and then we
     * print the remaining pathname components by traversing
     * the pathname components linked list.
     */

    strcpy (buf, mntOnName);

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
check_disk_size(int fd, u_int block_size, u_int block_offset)
{
    off_t byte_offset = (off_t)(block_offset - 1) * (off_t)block_size;
    void *buf = (void *)malloc(block_size);
    int retval = FALSE;

    if (buf != (void *)NULL) {
	if ((lseek(fd, byte_offset, SEEK_SET) == byte_offset) &&
	    (read(fd, buf, (size_t)block_size) == (size_t)block_size)) {
	    retval = TRUE;
	}
	free(buf);
    }
    return(retval);
}

/*
 * get_disk_label
 *
 * Given the open file descriptor of a disk special device file
 * this routine will return the disk's label (partition info).
 */

static struct disklabel *
get_disk_label( char *devname, int fd, char *diskType, getDevStatusT *error )
{
    struct disklabel *lblp = malloc( sizeof( struct disklabel ) + BUFSIZ);

    *error = GDS_OK;

    if ((lblp) && (ioctl( fd, DIOCGDINFO, lblp ) < 0)) {

        /* Not labelled so use /etc/disktab */

	if (createlabel(devname, diskType, lblp, (char *)(&lblp[1]), BUFSIZ)) {
            free(lblp);
	    lblp = (struct disklabel *)NULL;
	}
    }

    return( lblp );
}

/**
 * Returns true if the device name given by lsm_path is an lsm volume.
 * If lsm_path does represent an lsm volume, volsize is filled in.  
 * Because volsize is an int, the max size of the lsm volume is 
 * 2TB if volsize is cast to a uint and 1TB if read as an int (assuming
 * 512 block size).  It is suggested that islsm64 be used instead of
 * islsm as it will correctly report large lsm volume sizes.
 */
   
int
islsm( char *lsm_path, int *volsize)
{
   int lsm_fd;
   DEVGEOMST volgeom;
   struct devget devget;
   struct stat st;
	device_info_t	*devinfop;

   /* Check if the device exists */
   
   if (stat(lsm_path, &st) < 0) {
       return(FALSE);
   }

   /* open the volume */
   if ((lsm_fd = open(lsm_path, O_RDONLY)) == -1) {
       return(FALSE);
   }

	/* malloc this as this is a large structure */
	devinfop = (device_info_t *)malloc(sizeof(device_info_t));
	/* if it fails - keep going but don't do the DEVGETINFO ioctl */

	/*
	 * First, attempt to use the DEVGETINFO ioctl (preferred),
	 * if that fails, try the backward-compatibility ioctl
	 * DEVIOCGET.
	 */
	if ((!devinfop) || (ioctl(lsm_fd, DEVGETINFO, (char *)devinfop) < 0) ||
			(devinfop->version != VERSION_1)) {
	    if (devinfop)
	        free(devinfop);
   	    if (ioctl(lsm_fd, DEVIOCGET , (char *)(&devget)) < 0){
       	        close(lsm_fd);
       	        return(FALSE);
   	    }
   	    if (!devget.dev_name ||
       	       (strncmp(devget.dev_name, "LSM", strlen("LSM")) != 0)) {
       	       close(lsm_fd);
       	       return(FALSE);
   	    }
	} else {
   	    if (strncmp(devinfop->v1.dev_name, "LSM", strlen("LSM")) != 0) {
		free(devinfop);
       	        close(lsm_fd);
       	        return(FALSE);
   	    }
       	    free(devinfop);
	}


   /* 
    * Now check if the filesystem size has already
    * been set.  If not we need to retrieve and set 
    * that information.
    */

   if (*volsize == 0) {
       /* 
        * Make ioctl call to get volume length.
        */
       if (ioctl(lsm_fd, DEVGETGEOM , (char *)(&volgeom)) == 0) {
           
           *volsize = (int) volgeom.geom_info.dev_size;

       } else {
           /*
            * ioctl call failed because lsm_fd does not refer to a
            * volume. Close down the file descriptor and return FALSE.
            */
           close(lsm_fd);
           return(FALSE);
       }
   }

   close(lsm_fd);   
   return(TRUE);
}


/**
 * Returns true if the device name given by lsm_path is an lsm volume.
 * If lsm_path does represent an lsm volume, volsize is filled in.  
 */
int
islsm64( char *lsm_path, unsigned long *volsize)
{
   int lsm_fd;
   DEVGEOMST volgeom;
   struct devget devget;
   struct stat st;
   device_info_t *devinfop;

   /* Check if the device exists */
   
   if (stat(lsm_path, &st) < 0) {
       return(FALSE);
   }

   /* open the volume */
   if ((lsm_fd = open(lsm_path, O_RDONLY)) == -1) {
       return(FALSE);
   }

   /* malloc this as this is a large structure */
   devinfop = (device_info_t *)malloc(sizeof(device_info_t));
   /* if it fails - keep going but don't do the DEVGETINFO ioctl */

   /*
    * First, attempt to use the DEVGETINFO ioctl (preferred),
    * if that fails, try the backward-compatibility ioctl
    * DEVIOCGET.
    */
    if ((!devinfop) || (ioctl(lsm_fd, DEVGETINFO, (char *)devinfop) < 0) ||
        (devinfop->version != VERSION_1)) {
	if (devinfop)
	    free(devinfop);
   	if (ioctl(lsm_fd, DEVIOCGET , (char *)(&devget)) < 0){
       	    close(lsm_fd);
       	    return(FALSE);
   	}
   	if (!devget.dev_name ||
       	   (strncmp(devget.dev_name, "LSM", strlen("LSM")) != 0)) {
       	    close(lsm_fd);
       	    return(FALSE);
   	}
   } else {
     if (!devinfop->v1.dev_name ||
        (strncmp(devinfop->v1.dev_name, "LSM", strlen("LSM")) != 0)) {
         free(devinfop);
       	 close(lsm_fd);
       	 return(FALSE);
     }
     free(devinfop);
   }


   /* 
    * Now check if the filesystem size has already
    * been set.  If not we need to retrieve and set 
    * that information.
    */

   if (*volsize == 0) {
       /* 
        * Make ioctl call to get volume length.
        */
       if (ioctl(lsm_fd, DEVGETGEOM , (char *)(&volgeom)) == 0) {
           
           *volsize = volgeom.geom_info.dev_size;

       } else {
           /*
            * ioctl call failed for an unknown reason.
            * Close down the file descriptor and return FALSE.
            */
           close(lsm_fd);
           return(FALSE);
       }
   }

   close(lsm_fd);   
   return(TRUE);
}

/*
 * checklsm
 *
 * This function takes a volume path and lets you know if it is an
 * lsm volume.  If it is it returns the diskgroup and the volume name.
 * 
 * This function will convert the block device to the raw device
 * so that it can operate on open devices.
 */
int
checklsm(char *lsm_path,         /* In  */
	 char *lsm_diskGroup,    /* Out */
	 char *lsm_volName)      /* Out */
{
    unsigned long lsm_size;
    char tempPath[MAXPATHLEN+1];
    char *tempDiskGroup;
    char *temp;
    char *savept; /* Used for strtok_r for thread-safety */
    char raw_vol_name[MAXPATHLEN+1];
    char *raw;

    /*
     * islsm64 needs to be able to open the volume even if it
     * is in use so we get the raw device name.
     */
    if((raw = fsl_to_raw_name(lsm_path, raw_vol_name, MAXPATHLEN)) == 0) {
	return(FALSE);
    }

    if (islsm64(raw, &lsm_size)) {
      /*
       *  we need to figure out the
       *  diskgroup.  the lsm_path will be in one of the two
       *  formats:
       *
       *  1) /dev/vol/vol01
       *  2) /dev/vol/diskgroup/vol01
       *
       *  In case 1, we know that group is rootdg.
       *  In case 2, it is 'diskgroup'.
       *  be changed.
       */
      strcpy(tempPath, lsm_path);
      savept = tempPath;

      tempDiskGroup  = strtok_r(tempPath, "/",&savept);
      tempDiskGroup  = strtok_r(NULL,"/",&savept);
      tempDiskGroup  = strtok_r(NULL,"/",&savept);
      temp           = strtok_r(NULL,"/",&savept); 

      if (NULL == temp){
	  /*
	   * In this case we have no listed diskgroup
	   * Therefore we know it is rootdg.
	   */
	  strcpy(lsm_volName,  tempDiskGroup);
	  strcpy(lsm_diskGroup, "rootdg");
      } else {
	  /*
	   * We have a diskgroup so handle it.
	   */
	  strcpy(lsm_diskGroup, tempDiskGroup);
	  strcpy(lsm_volName,   temp);
      }
      return 1;
    }
    else {
      return 0;
    }
}

/*
 * get_dev_size
 *
 * Given a device name this routine will return the device's partition
 * size in 512 byte blocks.
 */

getDevStatusT
get_dev_size( 
    char *devName,      /* in - device name */
    char *devType,      /* in - optional device type ("rz55", etc or NULL) */
    u32T *devSize       /* out - device size */
    )
{
    struct stat st;
    char devPath[MAXPATHLEN], device[MAXPATHLEN];
    int pathLen, i, fd, partNum, iDevSize = 0;
    getDevStatusT error;
    char partName; /* partition name; abcdefgh */
    struct disklabel *lblp;
    unsigned long lsmDevSize = 0;

    if (devName[0] != '/') {
        return( GDS_ABSPATH );
    }

    if (stat( devName, &st ) < 0) {
        return( GDS_CANTOPEN );
    }

    if ((st.st_mode & S_IFMT) != S_IFBLK) {
        return( GDS_BLOCKDEV );
    }

    if (islsm64( devName, &lsmDevSize )) {
        
        if (lsmDevSize > (unsigned int)(UINT_MAX))
            return( GDS_VOLTOOBIG );

        if (lsmDevSize == 0) {
            return( GDS_LVSIZE );
        }
        else iDevSize = (unsigned int)lsmDevSize;
                    
    } else {

        /* get size from disk label */

        /* Form the character device name. */

	if (NULL == fsl_to_raw_name(devName, device, MAXPATHLEN)) {
	    return( GDS_CANTOPEN );
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

        partName = devName[strlen( devName ) - 1]; /* last ch in dev name */

        if (strchr( "abcdefgh", partName ) == 0) {
            /* 
             * This should never happen unless the partition naming
             * convention changes.
             */
            close( fd );
            return( GDS_BADPART );
        }

        partNum = partName - 'a'; /* a = 0, b = 1, ... */

        lblp = get_disk_label( device, fd, devType, &error );
        if (lblp == NULL) {
            if (error != GDS_OK) {
                close( fd );
                return( error );
            } else {
                close( fd );
                return( GDS_ERRLABEL );
            }
        }

        if (partNum >= lblp->d_npartitions) {
            close( fd );
            return( GDS_BADPART );
        }

        iDevSize = lblp->d_partitions[partNum].p_size;
        if (iDevSize == 0) {
            close( fd );
            return( GDS_UNAVAILPART );
        }

	/*
	 * Sanity check:
	 * Does this partition run past the end of the disk?
	 * This could happen if the disk had been incorrectly
	 * labeled.
	 */
	if (check_disk_size(fd, lblp->d_secsize, iDevSize) != TRUE) {
            close( fd );
            return( GDS_BADPART );
	}

        close( fd );
    }

    *devSize = (unsigned int)iDevSize;

    return( GDS_OK );
}


int lock_file( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
)
{
    int fd;
    struct stat st;
    char lockFile[MAXPATHLEN+1];
    char *cp;

    /* Because we use advisory lock on directories which anyone can */
    /* access, we can have denial of service attacks.  The original */
    /* solution was if the file to lock happened to be a directory  */
    /* then create a lock file in the directory.                    */
    /*                                                              */
    /* This failed as "swapon" checks the domain directories and    */
    /* did not know how to handle the lock files.  Now we create    */
    /* the lock files in '/etc/fdmns'.  The lock files must be      */
    /* unique so the soltion is to strip off the last part of the   */
    /* pathname and append it to the lock file.                     */
    /* The user will not be able to lock this file.                 */

    /* First check if file is a directory, if it isn't we don't do  */
    /* anything different.                                          */

    if (stat( fileName, &st ) < 0) {
        *error = errno;
        return -1;
    }

    if (S_ISDIR(st.st_mode )) {
	/* We now know that the file passed in is a directory.      */
	
	/* Trim off last part of pathname, Load into dirName        */
	cp = strrchr( fileName, '/' );
	if (NULL == cp)	{
	    /* No slash is found, so use the fileName               */
	    cp = fileName;
	} 
	else {
	    /* Skip the slash that was found.                       */
	    cp++;

	    /* fileName could end with a '/', in this case we       */
	    /* actually want the '/' prior to this one.             */
	
	    if (NULL == cp)
	    {
	        cp--;   /* Move ch to '/' */
		while (--*cp != '/' && cp >= fileName) {}

		cp++;
	    }
	}


	/* Check to see if lock file pathname with be to large.     */
      	if ((strlen(cp) + strlen(ADVFS_LOCK_FILE)) > MAXPATHLEN)
	{
	    return -1;
	}
	/* Create full file name from fileName                      */
	strcpy( lockFile, ADVFS_LOCK_FILE );
	strcat( lockFile, cp );


	/* Open file if it doesn't exist create it.  Make sure to   */
	/* give the new file the owner read only.                   */
	if ((fd = open( lockFile, O_RDONLY | O_CREAT, S_IRUSR )) < 0) {
	    *error = errno;
	    return -1;
	}

	/* flock the lock file                                     */
	if (flock( fd, lkMode)) {
	    *error = errno;
	    close( fd );
	    return -1;
	}
    }
    else
    {
	if ((fd = open( fileName, O_RDONLY, 0 )) < 0) {
	    *error = errno;
	    return -1;
	}

	if (flock( fd, lkMode)) {
	    *error = errno;
	    close( fd );
	    return -1;
	}
    }

    return fd;
}

int lock_file_nb( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
)
{
    int fd;
    struct stat st;
    char lockFile[MAXPATHLEN+1];
    char *cp;

    /* Because we use advisory lock on directories which anyone can */
    /* access, we can have denial of service attacks.  The original */
    /* solution was if the file to lock happened to be a directory  */
    /* then create a lock file in the directory.                    */
    /*                                                              */
    /* This failed as "swapon" checks the domain directories and    */
    /* did not know how to handle the lock files.  Now we create    */
    /* the lock files in '/etc/fdmns'.  The lock files must be      */
    /* unique so the soltion is to strip off the last part of the   */
    /* pathname and append it to the lock file.                     */
    /* The user will not be able to lock this file.                 */

    /* First check if file is a directory, if it isn't we don't do  */
    /* anything different.                                          */

    if (stat( fileName, &st ) < 0) {
        *error = errno;
        return -1;
    }

    if (S_ISDIR(st.st_mode )) {
	/* We now know that the file passed in is a directory.      */
	
	/* Trim off last part of pathname, Load into dirName        */
	cp = strrchr( fileName, '/' );
	if (NULL == cp)	{
	    /* No slash is found, so use the fileName               */
	    cp = fileName;
	} 
	else {
	    /* Skip the slash that was found.                       */
	    cp++;

	    /* fileName could end with a '/', in this case we       */
	    /* actually want the '/' prior to this one.             */
	

	    if (NULL == cp)
	    {
	        cp--;   /* Move ch to '/' */
		while (--*cp != '/' && cp >= fileName) {}

		cp++;
	    }
	}

	/* Check to see if lock file pathname with be to large.     */
      	if ((strlen(cp) + strlen(ADVFS_LOCK_FILE)) > MAXPATHLEN)
	{
	    return -1;
	}
	/* Create full file name from fileName                      */
	strcpy( lockFile, ADVFS_LOCK_FILE );
	strcat( lockFile, cp );

	/* Open file if it doesn't exist create it.  Make sure to   */
	/* give the new file the owner read only.                   */
	if ((fd = open( lockFile, O_RDONLY | O_CREAT, S_IRUSR )) < 0) {
	    *error = errno;
	    return -1;
	}

	/* flock the lock file                                  */
	if (flock( fd, lkMode | LOCK_NB)) {
	    *error = errno;
	    close( fd );
	    return -1;
	}

    }
    else
    {
	if ((fd = open( fileName, O_RDONLY, 0 )) < 0) {
	    *error = errno;
	    return -1;
	}

	if (flock( fd, lkMode | LOCK_NB)) {
	    *error = errno;
	    close( fd );
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
    if (flock( lockHndl, LOCK_UN )) {
        *error = errno;
        return -1;
    }

    close( lockHndl );

    return 0;
}

int
volume_avail(
    char *prog,         /* in - program name (for error msgs) */
    char *dmnName,      /* in - domain name */
    char *volName       /* in - name of volume to add */
    )
{
#ifndef _OSF_SOURCE
    return TRUE;
#else
    DIR *dp;
    struct dirent *dirEnt;
    struct stat volStats, entStats;
    char dmnDir[MAXPATHLEN+1];
    int len;
    nl_catd catd;

    if (stat( volName, &volStats ) < 0) {
   	catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);	
        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY15, "%s: can't access device %s; [%d] %s\n"),
                 prog, volName, errno, ERRMSG( errno ) );
	(void) catclose(catd);
        return FALSE;
    }

    dp = opendir( MSFS_DMN_DIR );
    if (dp == NULL) {
        catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);	
        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY16, "%s: unable to open directory <%s>; [%d] %s\n"),
                 prog, MSFS_DMN_DIR, errno, ERRMSG( errno ) );
	(void) catclose(catd);
        return FALSE;
    }
    
    /* 
     * append a slash (/)  to the path name 
     */
 
    strcpy( dmnDir, MSFS_DMN_DIR "/" );
    len = strlen( dmnDir );
 
    while ((dirEnt = readdir( dp )) != NULL) {
 
        if (!strcmp( dirEnt->d_name, "." ) ||
            !strcmp( dirEnt->d_name, "..") ||
            !strcmp( dirEnt->d_name, dmnName)) {
            /* skip . and .. */
            continue;
        }

        strcat( dmnDir, dirEnt->d_name );
   
        if (stat( dmnDir, &entStats ) < 0) {
            dmnDir[len] = '\0';                 /* chop off the file name */
            continue;
        }
   
        if (S_ISDIR( entStats.st_mode )) {
            if (volume_in_use( prog, dmnDir, &volStats )) {
                closedir( dp );
                return FALSE;
            }
        }
   
        dmnDir[len] = '\0';                     /* chop off the file name */
    }
 
    closedir( dp );

    return TRUE;
#endif
}

int
volume_in_use( 
    char *prog,                 /* in - program name (for error msgs) */
    char *dmnDir,               /* in - domain dir's full path name */
    struct stat *volStats       /* in - new volume's stats */
    )
{
#ifndef _OSF_SOURCE
    return TRUE;
#else
    DIR *dp;
    struct dirent *dirEnt;
    struct stat dmnVolStats;
    char dmnVol[MAXPATHLEN+1];
    char dmnVolLink[MAXPATHLEN+1];
    int len;
    nl_catd catd;

    dp = opendir( dmnDir );
    if (dp == NULL) {
   	catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);		
        fprintf( stderr, catgets(catd, S_LIBRARY1, LIBRARY16, "%s: unable to open directory <%s>; [%d] %s\n"),
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

                readlink( dmnVol, dmnVolLink, sizeof( dmnVolLink ) );
                dmnVolLink[strlen( dmnVolLink )] = '\0';
                dmnDir = strrchr( dmnDir, '/' );
                dmnDir++;
		catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
                fprintf( stderr, 
                         catgets(catd, S_LIBRARY1, LIBRARY17, "%s: device in use by domain %s; device %s\n"),
                         prog, dmnDir, dmnVolLink );
		(void) catclose(catd);
                return TRUE;
            }
        }
   
        dmnVol[len] = '\0';                     /* chop off the file name */
    }
 
    closedir( dp );

    return FALSE;
#endif
}

int
init_event(advfs_ev *advfs_event)
{
    advfs_event->special = NULL;
    advfs_event->domain = NULL;
    advfs_event->fileset = NULL;
    advfs_event->directory = NULL;
    advfs_event->clonefset = NULL;
    advfs_event->renamefset = NULL;
    advfs_event->user = NULL;
    advfs_event->group = NULL;
    advfs_event->fileHLimit =  NULL;
    advfs_event->blkHLimit =  NULL;
    advfs_event->fileSLimit =  NULL;
    advfs_event->blkSLimit =  NULL;
    advfs_event->options = NULL;
    advfs_event->ev_info_str = NULL;
    return 0;
}

int
advfs_post_user_event(char *evname, advfs_ev advfs_event, char *Prog)
{       
    int i, ret=0;
    nl_catd catd;

    EvmEvent_t      ev;
    EvmVarValue_t   value;
    EvmConnection_t evconn;

    ret = EvmEventCreateVa(&ev, EvmITEM_NAME,evname,EvmITEM_NONE);
    if (ret != EvmERROR_NONE)  { 
        goto _error2;
    }

    if (advfs_event.special != NULL) {
        value.STRING = advfs_event.special;
        EvmVarSet(ev, "special", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.domain != NULL) {
        value.STRING = advfs_event.domain;
        EvmVarSet(ev, "domain", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileset != NULL) {
        value.STRING = advfs_event.fileset;
        EvmVarSet(ev, "fileset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event.fileHLimit != NULL) {
        value.INT64 = advfs_event.fileHLimit;
        EvmVarSet(ev, "fileHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkHLimit != NULL) {
        value.INT64 = advfs_event.blkHLimit;
        EvmVarSet(ev, "blkHLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.fileSLimit != NULL) {
        value.INT64 = advfs_event.fileSLimit;
        EvmVarSet(ev, "fileSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.blkSLimit != NULL) {
        value.INT64 = advfs_event.blkSLimit;
        EvmVarSet(ev, "blkSLimit", EvmTYPE_INT64, value, 0, 0);
        value.INT64 = NULL;
    }

    if (advfs_event.options != NULL) {
        value.STRING = advfs_event.options;
        EvmVarSet(ev, "options", EvmTYPE_STRING, value, 0, 0);
    }
    
    if (advfs_event.ev_info_str != NULL) {
        value.STRING = advfs_event.ev_info_str;
        EvmVarSet(ev, "ev_info_str", EvmTYPE_STRING, value, 0, 0);
    }   

    ret = EvmConnCreate(EvmCONNECTION_POST, EvmRESPONSE_WAIT, 
            NULL, NULL, NULL, &evconn);
    if (ret != EvmERROR_NONE)  { 
        goto _error1;
    }

    /* EvmEventPost waits for daemon response before returning,
       due to EvmRESPONSE_WAIT in EvmConnCreate */

    ret = EvmEventPost(evconn,ev);

    EvmConnDestroy(evconn);

_error1:
    EvmEventDestroy(ev);

_error2:

    if (ret) {
        catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
        if ( ret == EvmERROR_CONNECT ) {
            fprintf(stderr, catgets(catd, S_LIBRARY2, LIBRARY_EVM2,
                    "%s: informational: [%d] posting event: %s\n"
                    "\tIf running in single user mode, EVM is not running.\n"
                    "\tPlease ignore this posting.\n"),
                    Prog, ret, evname);
        } else {
            fprintf(stderr, catgets(catd, S_LIBRARY2, LIBRARY_EVM,
                    "%s: error [%d] posting event: %s\n"),
                    Prog, ret, evname);
        }
    }	

    return ret;
}

/* end library.c */

