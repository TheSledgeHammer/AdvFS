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
 *      AdvFS - bs_domain.c
 *
 * Abstract:
 *
 *      This module contains routines for managing
 *      bitfile domains.
 *
 */

#define ADVFS_MODULE    BS_DOMAIN

#include <sys/dirent.h>
#include <sys/types.h>
#include <sys/systm.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/mount.h>
#include <sys/buf.h>
#include <sys/conf.h>
#include <sys/vnode.h>          /* struct vnode */
#include <sys/spinlock.h>
#include <sys/diskio.h>
#include <h/kthread_access.h>   /* private: for KT_SEMA(), used by DEVSW() */
#include <fs/h/filesystems_private.h> /* extern dev_t block_to_raw(); */
#include <sys/pathname.h>       /* struct pathname */
#include <fs/vfs_ifaces.h>      /* struct nameidata, NAMEIDATA() macro */ 
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <bs_ods.h>
#include <bs_delete.h>
#include <bs_inmem_map.h>
#include <bs_migrate.h>
#include <advfs/advfs_syscalls.h>
#include <bs_public_kern.h>
#include <bs_access.h>
#include <bs_domain.h>
#include <tcr/clu.h>
#include <advfs_evm.h>
#include <bs_sbm.h>

/*  This sentinel pointer is init'd to the first advfs domain
 *  mounted.  It is used primarily for locating all domains
 *  on the system.  Use this pointer as a starting point when marching
 *  down the domainT->dmnFwd & domainT->dmnBwd linked lists and you
 *  do not have a domainT available.
*/
domainT *DmnSentinelP = NULL;

/* Dynamic hash table for domain structures */
static void *DomainHashTbl;

/* Count of existing domains.  This is still sync'd with
*  DmnHashTblLock and is modified ONLY in dmn_alloc()
*  and dmn_dealloc(). 
*/
static int DomainCnt = 0;

extern struct bsMPg *Dpgp;
extern struct bsMPg *GRpgp;

static vdDescT nilVdDesc = NULL_STRUCT;


/* flags for vd_remove */
#define ADVMTX_VD_STATE_CHANGE   0x0000000000000001LL
#define VD_REMOVE_PERM           0x0000000000000002LL
#define SET_UNMOUNT_TIME         0x0000000000000004LL


typedef enum {
    DMN_FULL_SWEEP,
    DMN_NAME_SWEEP
} dmn_sweep_type;

/*
 * module internal PROTOTYPES
 */

static statusT
bs_global_root_activate(
    char* domainName,
    uint64_t flags,
    bfDomainIdT* bfDomainId
);

static statusT
check_trans_attrs(
    domainT *dmnP,        /* in */
    char*   topDmnDir,    /* in */
    vdIndexT dmnMAttrIdx, /* in */
    uint16_t *onDiskCnt,  /* in/out */
    char* volPathName     /* out - pathname of vol link */
);

static statusT
bs_bfdmn_sweep(
    struct domain* dmnP,
    dmn_sweep_type sweep_type
);

static int 
advfs_dev_mounted(
    dev_t dev
);


static statusT
update_bfsetid(
    domainT* dmnP,
    bfMCIdT mcid,
    bfDomainIdT domainId
);

static statusT
set_vd_mounted(
    struct domain* dmnP,         /* IN - domain struct */
    struct vd* vdp               /* IN - vd struct */
);

static
statusT
get_raw_vd_attrs(
    char *vdDiskName,                 /* IN - disk device name */
    domainT *dmnP,                    /* IN - Domain pointer */
    vdIndexT vdi,                     /* IN - vd index */
    uint64_t flags,                   /* IN - flag */
    struct vnode** vnodepp,           /* OUT - vnode ptr */
    struct bsVdAttr** vdAttrpp,       /* OUT - vd attributes */
    bsDmnAttrT** dmnAttrpp,           /* OUT - domain attributes */
    bsDmnMAttrT** dmnMAttrpp,         /* OUT - log attributes */
    bsDmnNameT**  dmnNamepp,          /* OUT - domain name stored on disk */
    adv_ondisk_version_t *dmnVersion, /* OUT - domain version */
    struct bsMPg *pgp                 /* OUT - RBMT page 0 */
);

static
statusT
setup_vd(
    domainT* dmnP,               /* IN - domain pointer */
    struct vnode* vnp,           /* IN - ptr to vnode */
    char* vdDiskName,            /* IN - name of disk */
    struct bsVdAttr* vdAttrp,    /* IN - on disk vd attributes */
    uint64_t flags,              /* IN - mount flags/check DmnActivationLock? */
    vdT** vdPtr                  /* OUT - vd struct pointer */
);

static
vdDescT *
get_vdd(
    char *vdName,                /* in - virtual disk name */
    int *err                     /* out - error */
);

static void
vd_free(
    vdT *vdp,
    domainT *dmnP
);

static vdIndexT 
vd_alloc_index(
    domainT *dmnP
);

static
void
scan_rsvd_file_xtnt_map (
    bfAccessT *bfAccess,        /* in */
    vdIndexT vdIndex,           /* in */
    int *retFoundFlag           /* out */
);

static
statusT
clear_bmt (
    domainT *dmnP,              /* in */
    vdIndexT vdIndex,           /* in */
    uint32_t forceFlag,         /* in */
    bf_vd_blk_t startBlk,       /* in */
    bf_vd_blk_t numBlks         /* in */
);

static
statusT
wait_for_ddl_active_entry (
    domainT *domain,            /* in */
    vdT *vd,                    /* in */
    bfMCIdT mcellId             /* in */
);

static void
vd_remove(
    domainT* dmnP,              /* in */
    vdT* vdp,                   /* in */
    bsIdT    unmtTm,            /* in */
    uint64_t flags              /* in */
);

static
statusT
set_disk_attrs (
    domainT *domain,            /* in */
    bsVdOpT op,                 /* in */
    dev_t rdev                  /* in */
);

static
statusT
dmn_alloc(
    bfDomainIdT domainId,
    char *domainName,
    bfTagT dirTag,
    struct bsMPg *pgp,
    domainT **newdmnPP
);

static
void
dmn_dealloc(
    domainT *dmnP
);

static
domainT *
domain_id_lookup(
    bfDomainIdT bfDomainId,     /* in - domain to lookup */
    uint64_t flags              /* in - mount flags/check DmnActivationLock? */
);

static statusT
clear_trans_attrs(
    domainT *dmnP,           /* in */
    vdIndexT dmnMAttrIdx     /* in */
);


/***************************************************/
/*****  Bitfile Domain access/close functions  *****/
/***************************************************/

/*
 * bs_domain_access
 *
 * This routine must be called to acquire a domain
 * access handle prior to accessing or creating a bitfile.
 *
 * Returns ENO_SUCH_DOMAIN, E_DOMAIN_NOT_ACTIVIATED, or
 * EOK, or EIO (domain panic)
 */

statusT
bs_domain_access(
    domainT **dmnPP,         /* out */
    bfDomainIdT bfDomainId,  /* in */
    int deactivated_ok)      /* in */
{
    statusT sts;

    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    if( (*dmnPP = domain_id_lookup( bfDomainId, 0LL )) == 0 ) {
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        return( ENO_SUCH_DOMAIN );
    }

    /*
     * screen domain panic
     */
    if ((*dmnPP)->dmn_panic) {
        sts = E_DOMAIN_PANIC;
    } else if (((*dmnPP)->state != BFD_DEACTIVATED) ||
               (deactivated_ok)) {      
        (*dmnPP)->dmnAccCnt++;
        sts = EOK;
    } else {
        sts = ENO_SUCH_DOMAIN;
    }

    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    return sts;
}

/*
 * bs_domain_close
 *
 * This routine is called to close out use of a
 * domain access handle
 */

void
bs_domain_close(
    domainT *dmnP)
{
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    dmnP->dmnAccCnt--;
    if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
        dmnP->dmnRefWaiters = 0;
        cv_broadcast(&dmnP->dmnRefWaitersCV, NULL, CV_NULL_LOCK);
    }
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock ); 
    return;
}

/*
 * bs_bfdmn_id_activate
 *
 * Same as bs_bfdmn_tbl_activate() except the domain is activated
 * via its id instead of its domain table.  In order for this call
 * to have any effect, the domain must already be activated via 
 * bs_bfdmn_tbl_activate().  In other words, this routine can only 
 * be used to activate an already active domain.  This is
 * because the domain id alone is not enough to fully activate a
 * domain (one also needs the names of the disks).  So, if this
 * function were called on an existing but not active domain ID,
 * it would return E_NO_SUCH_DOMAIN.
 */

statusT
bs_bfdmn_id_activate(
    bfDomainIdT bfDomainId) /* in - domain id */
{
    domainT *dmnP = NULL;

    ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );
    dmnP = domain_id_lookup( bfDomainId, 0LL );
    if ((TEST_DMNP(dmnP) == EOK ) && (dmnP->state == BFD_ACTIVATED)) {
        /*
         * The domain is already active so simply return
         */
        dmnP->activateCnt++;
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        return EOK;
    } else {
        /*
         * The domain is not active so return an error.
         */
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        return ENO_SUCH_DOMAIN;
    }
}

/* bs_bfdmn_tbl_activate
 *
 * Take all steps necessary to activate the domain described by the
 * specified domain name.
 *
 * currently flag is only used to signify a forced mount (ie
 * don't run recovery). The calling sequence is msfs_mount,
 * advfs_mountfs, bs_bfset_activate, bs_bfdmn_tbl_activate,
 * bd_bfdmn_activate, lgr_open.
 */
statusT
bs_bfdmn_tbl_activate(
    char* domainName,            /* in - bf domain name */
    uint64_t flags,              /* in - flag */
    bfDomainIdT* bfDomainId)     /* out - domain id */
{
    statusT               sts;
    domainT              *dmnP;
    bfDmnDescT           *dmntbl;
    bfDomainIdT           domainId;
    bfDomainIdT           dualMountId;
    bfFsCookieT           dmnCookie;
    bsDmnMAttrT           dmnMAttr;
    adv_ondisk_version_t  dmnVersion;
    vdT                  *vdp;
    vdIndexT              dmnMAttrIdx;
    int32_t               closeVd;
    int32_t               dmnTblLocked;
    int32_t               error;
    vdIndexT              vdi;
    int32_t               id;
    int32_t               unlink_and_restart;
    uint64_t              isMount;
    uint64_t              isRoot;
    struct vnode         *vnp;
    char                 *volPathName;
    bfTagT                bfTag;
    int32_t               newId;
    char                 *topDmnDir = NULL;

start:
    /* Initialize variables */
    dmntbl             = NULL;
    domainId           = nilBfDomainId;
    dualMountId        = nilBfDomainId;
    dmnCookie          = nilFsCookie;
    closeVd            = FALSE;
    dmnTblLocked       = FALSE;
    isRoot             = flags & (DMNA_GLOBAL_ROOT | DMNA_LOCAL_ROOT);
    isMount            = flags & DMNA_MOUNTING;
    dmnMAttrIdx        = 0;
    vnp                = NULL;
    unlink_and_restart = FALSE;
    volPathName        = NULL;
    newId              = FALSE;
    bzero(&dmnMAttr, sizeof(bsDmnMAttrT));

    if (flags & DMNA_GLOBAL_ROOT) {
        sts = bs_global_root_activate(domainName, flags, bfDomainId);
        return sts;
    }

    ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );
    dmnTblLocked = TRUE;

    /*
     * If the domain is already mounted,
     *     bump the ref count and return it's Id.
     */
    dmnP = domain_name_lookup( domainName, flags );
    if ((TEST_DMNP(dmnP) == EOK) && (dmnP->state == BFD_ACTIVATED)) {
        if (flags & DMNA_CREATING) {
            /* If we are creating this domain then we had better
             * not have found it. NOTE unfortunately the caller
             * at this point has already done a disk_init so
             * the on-disk is totally corrupted.
             */
            sts = E_DOMAIN_ALREADY_EXISTS;
            goto get_table_error;
        }
        *bfDomainId = dmnP->domainId;
        dmnP->activateCnt++;
        if (dmnTblLocked) {
            ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        }
        return EOK;
    }
    
    dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );

    /*
     * Get the list of disks in this domain into a table.
     */
    sts = get_domain_disks( domainName, isRoot, dmntbl, &topDmnDir);
    if ( sts != EOK ) {
        goto get_table_error;
    }

    /*
     * If this domain is not currently active then verify that none of the
     * devices found via get_domain_disks are currently in use. Doing this
     * check now prevents IO to any devices until it is known that none
     * of these devices belong to an active domain.
     */
    if (dmnP == NULL) {
        for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
            if (advfs_volume_mounted(dmntbl->vddp[vdi]->vdName)) {
                sts = EDUP_VD;
                goto vd_add_error;
            }
        }
    }
    

    /*
     * Loop through all disks described by the table.
     */
    for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
        struct bsVdAttr  *vdAttrp;
        bsDmnAttrT       *dmnAttrp;
        bfDmnStatesT      dmnState;
        bsDmnMAttrT      *dmnMAttrp;
        bsDmnNameT       *dmnNamep;
        vdDescT          *vdd = dmntbl->vddp[vdi];

        sts = get_raw_vd_attrs(vdd->vdName,
                               dmnP,
                               vdi,
                               isRoot|isMount,
                               &vnp,
                               &vdAttrp,
                               &dmnAttrp,
                               &dmnMAttrp,
                               &dmnNamep,
                               &dmnVersion,
                               Dpgp);
        if ( sts != EOK ) {
            goto get_attrs_error;
        }

        if ( vdAttrp->state == BSR_VD_VIRGIN ) {
            dmnState = BFD_VIRGIN;
        } else {
            dmnState = BFD_DEACTIVATED;
        }

        /*
         *  If we don't have the domain Id yet (first time through)
         *      Set it
         */ 
        if ( BS_UID_EQL( domainId, nilBfDomainId) ) {
            /*
             * This must be the first disk, domainId is nil.  We haven't
             * initialized it yet.
             */
            domainT *n_dmnP;

            /*
             * Split-Mirror support
             * 
             * Filesystems often have mirror (also called shadow) copies
             * of disks/devices that are managed by hardware or a volume
             * manager.  All filesystem writes to disks are also written 
             * to the mirror copies.  All mirror IO is handled completely
             * by the mirror hardware/software and is transaparent to the
             * filesystem.  Mirror copies are often user for redundancy
             * purposes. They can also be used to facilitate backups.
             *
             * A Split-Mirror filesystem is created whenever the mirror
             * disks are "split" from the "original" disks. (Note that in
             * a split mirror the concept or original vs split has no real
             * meaning as they are exact copies - or should be). The "split"
             * breaks the mirror resulting in 2 copies of the filesystem.
             * If the filesystem was not mounted at the time of the split
             * then the filesystems are exact. If the split occurred while
             * the filesystem was active then the "copy" is for a specific
             * point in time. 
             *
             * To safely split an active filesystem it MUST be frozen at 
             * the time of the split, otherwise there may be inconsistent
             * metadata on the copy. Note that freeze only ensureis that the
             * metadata is inconsistent.
             * 
             * If the domain name returned from get_raw_vd_attrs() does not
             * match the name of the domain being activated then assume this
             * is an attempt to mount or activate a split-mirror domain. 
             * A request to mount or activate a split-mirror will generate
             * a new unique domainId and a bs_bfdmn_sweep() call to update
             * all volumes with the new domainId and new domain name.
             *
             * NOTE: The domain/filesystem MUST be unique in order to be
             *       mounted on the system or cluster. The system admin
             *       can create the new domain using the scan utility by
             *       providing a new domain name and list of devices.
             *
             * Using the domain name to identify a split-mirror presereves
             * the domainID for the original domain, otherwise a new domainID
             * will be applied to whichever domain version is mounted or
             * activated last. Preserving the domainID in the original domain
             * will help system managers when reviewing any AdvFS related
             * messages since the domainID is included. Otherwise, domainIDs
             * could swap back & forth between the original domain and a
             * split-mirror version depending on the mount or activation
             * order.
             * 
             * If the DMNA_RENAME flag is set then the domainId will remain
             * intact and the call to bs_bfdmn_sweep() will only update the
             * domain name on all volumes.
             */
            if (dmnNamep &&  (strcmp(domainName, dmnNamep->dmn_name) != 0) 
                         && !(flags & DMNA_RENAME)) {
                struct timeval ltime;
                newId = TRUE;
                ltime = get_system_time();
                /*
                 * For cluster, the domainID is unique in the cluster. The
                 * lower 8 bits of the member ID are combined in the time
                 * stamp to form the domainId and verified by CFS for
                 * uniqueness.
                 */
                if (clu_is_ready()) {
                    while (TRUE) {
                        ltime.tv_usec &= 0x00ffffff;
                        ltime.tv_usec |= ((clu_memberid & 0x000000ff)<< 24);
                        if (CLU_FIND_CLUSTER_DOMAIN_ID(&ltime) == 0) {
                            break;
                        }
                        ltime = get_system_time();
                    }
                }
                domainId.id_sec  = ltime.tv_sec;
                domainId.id_usec = ltime.tv_usec;
                dualMountId = dmnAttrp->bfDomainId;
            } else {
                domainId = dmnAttrp->bfDomainId;
            }

            dmnCookie = dmnAttrp->MasterFsCookie;

            /*
             * If the domain is already mounted by CFS in the cluster
             * and this is not a dual mount, fail the activation.
             */
            if (clu_is_ready() && !(flags & DMNA_FAILOVER)) {
                if (CLU_FIND_CLUSTER_DOMAIN_ID(&domainId)) {
                    sts = E_DOMAIN_ALREADY_EXISTS;
                    goto vd_add_error;
                }
            }

            if ((n_dmnP = domain_id_lookup( domainId, flags )) == 0) {

                /* domainId not active, allocate a domainT */
                /*
                 * NOTE: We pass a "static" root tag directory tag to
                 * dmn_alloc(). This allows us to move the root tag directory
                 * without tracking down each accessed bitfile set and updating
                 * its bitfile set id.  Also, we don't have to update each
                 * bitfile set's "bfSetTag" mcell field.
                 */

                sts = dmn_alloc( domainId,
                                 domainName,
                                 staticRootTagDirTag,
                                 Dpgp,
                                 &dmnP );
                if (sts != EOK) {
                    goto vd_add_error;
                }
                dmnP->dualMountId = dualMountId;

            } else if ( n_dmnP != dmnP ) {
                /*
                 * The domain found by name lookup and the domain
                 * found by id are not the same.  This is a problem!
                 */
                sts = E_DOMAIN_ALREADY_EXISTS;
                goto vd_add_error;
            }

            dmnP->userAllocSz = vdAttrp->userAllocSz;
            dmnP->metaAllocSz = vdAttrp->metaAllocSz; 

            /*
             * Initially, the closed list and free list age times are the
             * same.  Over time, the free age time is adjusted based on vm
             * callbacks for memory pressure while the closed age time is
             * affect by system access structure usage.
             */
            dmnP->dmnConfigCacheAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;
            dmnP->dmnSoftClosedAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;
            dmnP->dmnSoftFreeAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;

            if ( (dmnP->state != BFD_UNKNOWN) &&
                 (dmnP->state != BFD_DEACTIVATED) ) {
                /*
                 * There is another domain table entry already for
                 * this id and it's not a freshly allocated entry and
                 * it's not deactivated.
                 */
                sts = E_DOMAIN_ALREADY_EXISTS;
                goto vd_add_error;
            }

            /*
             * This is the first vd (not necessarily vd 1!) that we've seen
             * for this domain, so copy relevant parameters from the domain
             * attributes record to the domain structure.
             */
            dmnP->state = dmnState;
            dmnP->dmnVersion = dmnVersion;
            dmnP->bfDmnMntId = vdAttrp->vdMntId;
            if (newId) {
                dmnP->dmnFlag |= BFD_DUAL_MOUNT_IN_PROGRESS;
            }

        } else {
            /*
             * Check the domain attributes record for consistency with
             * the other virtual disks in the domain.
             */
            if ( !BS_UID_EQL( domainId, dmnAttrp->bfDomainId )) {
                if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
                    if ( !BS_UID_EQL(dmnP->dualMountId, dmnAttrp->bfDomainId)
                         && (vdAttrp->state != BSR_VD_DMNT_INPROG)) {
                        sts = E_VD_BFDMNID_DIFF;
                        goto vd_add_error;
                     }
                } else {
                    sts = E_VD_BFDMNID_DIFF;
                    goto vd_add_error;
                }
            }
            
            /* Check domain cookie for consistency */
            if ( !BS_COOKIE_EQL( dmnCookie, dmnAttrp->MasterFsCookie )) {
                 sts = E_VD_BFDMNCOOKIE_DIFF;
                 goto vd_add_error;
            }

            /*
             * If this vd's state is not BSR_VD_DISMOUNTED, then make sure
             * the vdMntId matches. Also, set the domain's state to reflect
             * this vd's state.
             */
            if (vdAttrp->state != BSR_VD_DISMOUNTED) {
                if (vdAttrp->state != BSR_VD_DMNT_INPROG) {
                    if ( !BS_UID_EQL( dmnP->bfDmnMntId, vdAttrp->vdMntId )) {
                        /*
                         * Check to see if the first volume we processed
                         * had a valid vdMntId. This field gets nulled
                         * upon an rmvol. So, if dmnP->bfDmnMntId is nil,
                         * the first volume was in the process of being
                         * removed. This will be caught later when
                         * (dmnP->vdCnt != dmnMAttr.vdCnt). For now, let's
                         * process all the volumes so we can find dmnMAttr.
                         */
                        if (BS_UID_EQL(dmnP->bfDmnMntId, nilBfDomainId)) {
                            dmnP->bfDmnMntId = vdAttrp->vdMntId;
                        } else {
                            sts = E_VD_DMNATTR_DIFF;
                            goto vd_add_error;
                        }
                    }
                }

                if (vdAttrp->state == BSR_VD_MOUNTED) {
                    /*
                     * Atleast one of the vd's was mounted in this
                     * domain. Set the domain's state accordingly so
                     * that we make the right decision in bs_bfdmn_activate
                     * on whether to call recovery or not for V3 domains.
                     */
                    dmnP->state = dmnState;
                }
            }

            /*
             * Check the domain version for consistency with
             * the other virtual disks in the domain.
             */
            if ((dmnP->dmnVersion.odv_major != dmnVersion.odv_major) || 
                (dmnP->dmnVersion.odv_minor != dmnVersion.odv_minor)) {
                sts = E_INVALID_FS_VERSION;
                goto vd_add_error;
            }
        }

        if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
            vdAttrp->state = BSR_VD_DMNT_INPROG;
            sts = advfs_raw_io(vnp, 
                               RBMT_BLK_LOC, 
                               ADVFS_METADATA_PGSZ_IN_FOBS * 
                                ADVFS_FOBS_PER_DEV_BSIZE,
                               RAW_WRITE, 
                               Dpgp);
            if (sts != EOK) {
                goto vd_add_error;
            }
        }

       /* 
        * Load domain attribute parameters to domain table.
        */
        dmnP->maxVds = dmnAttrp->maxVds;  
        dmnP->dmnCookie = dmnAttrp->MasterFsCookie;


        /*
         * If this disk contains a more recent copy of the log
         * attributes then save these log attributes for later use.
         */
        if ((dmnMAttrp != NULL) && (dmnMAttrp->seqNum > dmnMAttr.seqNum)) {
            bcopy( (char *)dmnMAttrp, (char *)&dmnMAttr, sizeof(bsDmnMAttrT) );
            dmnMAttrIdx = vdAttrp->vdIndex;
        }

        /* Check to see if this index is already in use */
        if ( vd_htop_if_valid(vdAttrp->vdIndex, dmnP, FALSE, FALSE) ) {
            sts = EDUP_VD;
            goto vd_add_error;
        }

        sts = setup_vd( dmnP,
                       vnp,
                       vdd->vdName,
                       vdAttrp,
                       flags,
                       &vdp );
        if ( sts != EOK ) {
            goto vd_add_error;
        }

        closeVd = TRUE;

        /* 
         * Add vdp to domain and domain's service classes.
         */
        sts = sc_add_vd( dmnP, vdp->serviceClass, vdAttrp->vdIndex );
        if (sts != EOK) {
            goto get_attrs_error;
        }
    }

    if (dmnMAttrIdx == 0) {
        uprintf("Domain %s is missing the log volume.\n",
                domainName);
        uprintf("Check %s/%s for missing links before mounting.\n",
                topDmnDir, domainName);
        sts = E_VD_DMNATTR_DIFF;
        goto get_attrs_error;
    }

    /*
     * check the vd count. If wrong, don't mount the domain.
     */
    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        uint16_t tmp_vdCnt;
        tmp_vdCnt = dmnMAttr.vdCnt;
        volPathName = ms_malloc( (size_t)(MAXPATHLEN+1) );
        sts = check_trans_attrs(dmnP, 
                                topDmnDir,
                                dmnMAttrIdx, 
                                &tmp_vdCnt, 
                                volPathName);
        dmnMAttr.vdCnt = (uint16_t)tmp_vdCnt;
        if (sts == E_TOO_MANY_VIRTUAL_DISKS) {
            /*
             * check_trans_attrs found trails of an interrupted
             * rmvol (/dev/advfs link name in volPathName). Attempt
             * to roll forward this rmvol.
             */
            unlink_and_restart = TRUE;
            goto get_attrs_error;
        }
    }

    /*
     * Recheck vd count, in case it got changed by the above.
     */
    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        uprintf("Volume count mismatch for domain %s. %s expects %d volumes,\n",
                       domainName, domainName, dmnMAttr.vdCnt);
        uprintf("%s/%s has %d links.\n", topDmnDir, domainName, dmnP->vdCnt);
        sts = E_VOLUME_COUNT_MISMATCH;
        goto get_attrs_error;
    }

    /*
     * Clear trans attr record. 
     */

    sts = clear_trans_attrs(dmnP, dmnMAttrIdx); 
    if (sts != EOK) goto get_attrs_error;


    /*
     * If the recoveryFailed flag is set in the mutable domain
     * attributes, and AdvfsFixUpSBM is not set, then if it is
     * the root domain, allow the mount to proceed, but output
     * a warning and an EVM message.  Otherwise, if not the root,
     * fail the mount.
     */
    if (!AdvfsFixUpSBM && (dmnMAttr.bs_dma_dmn_state & DMNS_RECOVERY_FAILED)) {
        if (isRoot) {
            /* 
             * If we are activating the root domain, allow mounting
             * so the system can still boot
             */          
            ms_uprintf("ADVFS : Domain %s activated despite an inconsistency\n",
                       domainName);
            ms_uprintf("found in the AdvFS SBM metadata file.\n"); 
            ms_uprintf("Recommend you follow the procedures described\n");
            ms_uprintf("in the 'Trouble Shooting: Can't Clear Bit Twice Error\n");
            ms_uprintf("Message' section of the AdvFS Administration Guide.\n");
            {
                advfs_ev advfs_event;
                bzero(&advfs_event, sizeof(advfs_ev));
                advfs_event.domain = domainName;
                advfs_post_kernel_event(EVENT_FDMN_CCBT_IGNORED, &advfs_event);
            }
        }
        else {
            /* If not the root, then don't allow the activation to occur */
            sts = E_DOMAIN_NOT_ACTIVATED;
            ms_uprintf("ADVFS : Domain %s not activated - inconsistency detected\n",
                      domainName);
            goto get_attrs_error;                                    
        }
    }

    dmnP->ftxLogTag = dmnMAttr.ftxLogTag;
    dmnP->ftxLogPgs = dmnMAttr.ftxLogPgs;
    dmnP->bfSetDirTag = dmnMAttr.bfSetDirTag;
    dmnP->uid = dmnMAttr.uid;
    dmnP->gid = dmnMAttr.gid;
    dmnP->mode = dmnMAttr.mode;

    dmnP->dmnThreshold.threshold_upper_limit =
        dmnMAttr.dmnThreshold.threshold_upper_limit;

    dmnP->dmnThreshold.threshold_lower_limit =
        dmnMAttr.dmnThreshold.threshold_lower_limit;

    dmnP->dmnThreshold.upper_event_interval  =
        dmnMAttr.dmnThreshold.upper_event_interval;

    dmnP->dmnThreshold.lower_event_interval  =
        dmnMAttr.dmnThreshold.lower_event_interval;

    dmnP->dmnThreshold.time_of_last_upper_event = 0;
    dmnP->dmnThreshold.time_of_last_lower_event = 0;

    /* Allocate and initialize the domain's transaction table. */
    ftx_init_table(dmnP);

    /*
     * TODO:  There needs to be a lock to prevent anyone from accessing
     * or activating this set until this activate is completed.  Should
     * be a per-domain state lock?!
     */

    sts = bs_bfdmn_activate( domainId, flags );

    if ( sts != EOK ) {
        goto get_attrs_error;
    }

    /*
     * Clear recovery failed flag unconditionally if set at this
     * point (recovery completed).
     */
    if (dmnMAttr.bs_dma_dmn_state & DMNS_RECOVERY_FAILED) {
        (void)set_recovery_failed(dmnP, DMNS_CLEAR_RECOVERY);
    }

    if (dmnTblLocked){
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    }
    dmnP->dmnFlag = BFD_NORMAL; /* Clear all bits */
    *bfDomainId = domainId;
    if (volPathName != NULL) {
        ms_free( volPathName );
    }
    free_vdds( dmntbl );
    ms_free( dmntbl );
    return EOK;

vd_add_error:

    if (clu_is_ready() && !(isMount)) {
        CLU_DEV_DLM_UNLOCK(vnp->v_rdev);
    }

    (void) closed(vnp->v_rdev, S_IFBLK, FREAD | FWRITE);

    /* Nothing done with the error return above now */
    VN_RELE( vnp );

get_attrs_error:
    if (closeVd && (TEST_DMNP(dmnP) == EOK)) {

        /* Close volumes that were opened above */

        statusT temp_sts;
        
         
        for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {

                /*
                 * Tru64 called bs_reclaim_cfs_rsvd_vn here on all rsvd
                 * files on the volume.  This should not be necessary with
                 * CFS's new design which releases PFS vnodes on close.
                 */
                if (clu_is_ready() && !(isMount)) {
                        CLU_DEV_DLM_UNLOCK(vdp->devVp->v_rdev);
                }

                vd_remove(dmnP, vdp, nilBfDomainId, 0);
            }
    }

    if (TEST_DMNP(dmnP) == EOK) {
        /* deallocate the domain handle/structure */
        if (dmnP->activateCnt == 0) {
            dmn_dealloc( dmnP );
        }
    }

get_table_error:
    if (dmnTblLocked) {
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    }

    if (dmntbl != NULL) {
        free_vdds( dmntbl );
        ms_free( dmntbl );
    }

    if (unlink_and_restart) {
        char *rem_vol;
        struct vnode *vp = NULL;
        struct vnode *dvp = NULL;

        rem_vol = ((char *)strrchr (volPathName, '/'));
        MS_SMP_ASSERT(rem_vol != NULL);

        rem_vol++;

        ms_uprintf("Interrupted rmvol of volume %s in filesystem %s detected.\n",
                   rem_vol, domainName);

        sts = lookupname(volPathName, UIOSEG_KERNEL, 0, &dvp, &vp);
        if (sts) {
            ms_free( volPathName );
            return sts;
        }
        /*
         * Per HPUX model, release the vnode before calling remove.
         */
        VN_RELE(vp);
        error = VOP_REMOVE(dvp, rem_vol, TNC_CRED() );

        /*
         * VOP_REMOVE should have VN_RELE'd the dvp.  We don't need to
         * concern ourselves with such things.
         */

        ms_free( volPathName );
        if (error) {
            return (statusT)error;
        }
        goto start;
    }

    if (volPathName != NULL) {
        ms_free( volPathName );
    }

    if (topDmnDir != NULL) {
        ms_free(topDmnDir);
    }

    return sts;
}  /* end bs_bfdmn_tbl_activate */

static statusT
bs_global_root_activate(
    char* domainName,          /* in - bf domain name */
    uint64_t flags,            /* in - flag */
    bfDomainIdT* bfDomainId)   /* out - domain id */
{
    statusT               sts;
    bfDmnDescT           *dmntbl;
    bfDomainIdT           domainId;
    bfFsCookieT           dmnCookie;
    int                   closeVd, error, id, restart;
    vdIndexT              vdi;
    int                   update_global_rootdev;
    vdT                  *vdp;
    domainT              *dmnP;
    bsDmnMAttrT           dmnMAttr;
    vdIndexT              dmnMAttrIdx;
    struct vnode         *vnp;
    struct nameidata     *ndp;
    extern dev_t          clu_global_rootdev[];
    extern uint32_t       clu_global_rootdev_count;
    extern dev_t          clu_global_rootdev_opinprog;
    adv_ondisk_version_t  dmnVersion;
    bsMPgT               *bmtPage;
    bfPageRefHT           pgPin;
    struct bfAccess      *mdap;
    bsDmnTAttrT          *dmnTAttr;
    bfTagT                bfTag;

start:
    /* Initialize variables */
    dmntbl    = (bfDmnDescT *)NULL;
    domainId  = nilBfDomainId;
    dmnCookie = nilFsCookie;
    closeVd   = FALSE;
    restart   = FALSE;
    update_global_rootdev = -1;
    bzero(&dmnMAttr, sizeof(bsDmnMAttrT));
    dmnMAttrIdx = 0;
    vnp         = NULL;
    ndp         = NAMEIDATA();


    /*
     * Don't lock the domain table for global root. DLM locks in CFS
     * prevent multiple global root activations. Taking this lock out
     * for root causes deadlocks during global root failover.
     */

    /*
     * Check if the domain is already mounted.
     */

    dmnP = domain_name_lookup( domainName, flags );

    if ((TEST_DMNP(dmnP) == EOK) && (dmnP->state == BFD_ACTIVATED)) {
        /*
         * The domain is already active so simply return its domain ID.
         */
        *bfDomainId = dmnP->domainId;
        dmnP->activateCnt++;
        return EOK;
    }
    
    dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );

    /*
     * Get the list of disks in this domain into a table.
     */

    sts = get_domain_disks( domainName, DMNA_GLOBAL_ROOT, dmntbl, NULL);

    if ( sts != EOK ) {
        goto get_table_error;
    }

    /*
     * Loop through all disks described by the table.
     */

    for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
        struct bsVdAttr *vdAttrp;
        bsDmnAttrT      *dmnAttrp;
        bfDmnStatesT     dmnState;
        bsDmnMAttrT     *dmnMAttrp;
        bsDmnNameT      *dmnNamep;
        vdDescT         *vdd = dmntbl->vddp[vdi];

        sts = get_raw_vd_attrs(vdd->vdName,
                               dmnP,
                               vdi,
                               DMNA_GLOBAL_ROOT|DMNA_MOUNTING,
                               &vnp,
                               &vdAttrp,
                               &dmnAttrp,
                               &dmnMAttrp,
                               &dmnNamep,
                               &dmnVersion,
                               GRpgp);
        if ( sts != EOK ) {
            /*
             * Handle discrepancy between cluster database and
             * on-disk information.
             */
            if (clu_global_rootdev[vdi] == clu_global_rootdev_opinprog) {
                update_global_rootdev = vdi;
                clu_global_rootdev_opinprog = (dev_t)NULL;
                continue;
            } else                      /* don't panic here; let CFS */
                goto get_attrs_error;   /* failover handle the error */
        }

        if ( vdAttrp->state == BSR_VD_VIRGIN ) {
            dmnState = BFD_VIRGIN;
        } else {
            dmnState = BFD_DEACTIVATED;
        }

        if ( BS_UID_EQL( domainId, nilBfDomainId) ) {
            domainT *n_dmnP;

            domainId = dmnAttrp->bfDomainId;
            dmnCookie = dmnAttrp->MasterFsCookie;

            /*
             * If the domain is already mounted by CFS in the cluster
             * fail the activation.
             */
            if (clu_is_ready() && !(flags & DMNA_FAILOVER)) {
                if (CLU_FIND_CLUSTER_DOMAIN_ID(&domainId)) {
                    sts = E_DOMAIN_ALREADY_EXISTS;
                    goto vd_add_error;
                }
            }

            if ((n_dmnP = domain_id_lookup( domainId, flags )) == 0) {

                /*
                 * NOTE: We pass a "static" root tag directory tag to
                 * dmn_alloc(). This allows us to move the root tag directory
                 * without tracking down each accessed bitfile set and updating
                 * its bitfile set id.  Also, we don't have to update each
                 * bitfile set's "bfSetTag" mcell field.
                 */

                sts = dmn_alloc( domainId,
                                 domainName,
                                 staticRootTagDirTag,
                                 GRpgp,
                                 &dmnP );
                if (sts != EOK) {
                    goto vd_add_error;
                }
            } else if ( n_dmnP != dmnP ) {
                /*
                 * The domain found by name lookup and the domain
                 * found by id are not the same.  This is a problem!
                 */
                sts = E_DOMAIN_ALREADY_EXISTS;
                goto vd_add_error;
            }

            dmnP->userAllocSz = vdAttrp->userAllocSz;
            dmnP->metaAllocSz = vdAttrp->metaAllocSz; 

            /*
             * Initially, the closed list and free list age times are the
             * same.  Over time, the free age time is adjusted based on vm
             * callbacks for memory pressure while the closed age time is
             * affect by system access structure usage.
             */
            dmnP->dmnConfigCacheAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;
            dmnP->dmnSoftClosedAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;
            dmnP->dmnSoftFreeAgeTime = ADVFS_ACCESS_AGE_TIME_HZ;

            if ( (dmnP->state != BFD_UNKNOWN) &&
                 (dmnP->state != BFD_DEACTIVATED) ) {
                /*
                 * There is another domain table entry already for
                 * this id and it's not a freshly allocated entry and
                 * it's not deactivated.
                 */
                sts = E_DOMAIN_ALREADY_EXISTS;
                goto vd_add_error;
            }

            /*
             * This is the first vd (not necessarily vd 1!) that we've seen
             * for this domain, so copy relevant parameters from the domain
             * attributes record to the domain structure.
             */

            dmnP->state = dmnState;
            dmnP->dmnVersion = dmnVersion;
            dmnP->bfDmnMntId = vdAttrp->vdMntId;
        } else {

            /*
             * Check the domain attributes record for consistency with
             * the other virtual disks in the domain.
             */

            if ( (!BS_UID_EQL( domainId, dmnAttrp->bfDomainId )) ||
                 (!BS_COOKIE_EQL( dmnCookie, dmnAttrp->MasterFsCookie ))) {
                if (clu_global_rootdev[0] == clu_global_rootdev_opinprog) {
                    goto cleanup_global_rootdev;
                } else if (clu_global_rootdev[vdi] == clu_global_rootdev_opinprog) {
                    update_global_rootdev = vdi;
                    clu_global_rootdev_opinprog = (dev_t)NULL;
                    continue;
                } else {
                    int i=0;
                    ms_uaprintf("dmnId=%llx.%llx, vdId=%llx.%llx, dmnFsCookie=%llx.%llx, vdk=%llx.%llx, vdvdi=%d, vdcnt=%d, rootcnt=%d, op=%x\n",
                            domainId.id_sec,
                            domainId.id_usec,
                            dmnAttrp->bfDomainId.id_sec,
                            dmnAttrp->bfDomainId.id_usec,
                            dmnCookie.id_sec,
                            dmnCookie.id_usec,
                            dmnAttrp->MasterFsCookie.id_sec,
                            dmnAttrp->MasterFsCookie.id_usec,
                            vdi,
                            dmntbl->vdCount,
                            clu_global_rootdev_count,
                            clu_global_rootdev_opinprog);
                    for(i=0; i<dmntbl->vdCount; i++) {
                        ms_uaprintf("%s, %d, %x, %x\n",
                            dmntbl->vddp[i]->vdName,
                            dmntbl->vddp[i]->serviceClass,
                            dmntbl->vddp[i]->device,
                            clu_global_rootdev[i]);
                    }
                    if (!BS_UID_EQL( domainId, dmnAttrp->bfDomainId )) {
                      ADVFS_SAD0("bs_bfdmn_tbl_activate: domain id mismatch for global root");
                    } else {
                        ADVFS_SAD0("bs_bfdmn_tbl_activate: domain fscookie mismatch for global root");
                    }
                }
            }

            /*
             * If this vd's state is not BSR_VD_DISMOUNTED, then make sure
             * the vdMntId matches. Also, set the domain's state to reflect
             * this vd's state.
             */

            if (vdAttrp->state != BSR_VD_DISMOUNTED) {
                if (vdAttrp->state != BSR_VD_DMNT_INPROG) {
                    if ( !BS_UID_EQL( dmnP->bfDmnMntId, vdAttrp->vdMntId )) {
                        if (clu_global_rootdev[0] == clu_global_rootdev_opinprog) {
                            goto cleanup_global_rootdev;
                        } else if (clu_global_rootdev[vdi] == clu_global_rootdev_opinprog) {
                            update_global_rootdev = vdi;
                            clu_global_rootdev_opinprog = (dev_t)NULL;
                            continue;
                        } else {
                            int i=0;
                            ms_uaprintf("dmnId=%x, vdId=%x, vdi=%d, vdcnt=%d, rootcnt=%d, op=%x\n",
                                    dmnP->bfDmnMntId,
                                    vdAttrp->vdMntId,
                                    vdi,
                                    dmntbl->vdCount,
                                    clu_global_rootdev_count,
                                    clu_global_rootdev_opinprog);
                            for(i=0; i<dmntbl->vdCount; i++) {
                                ms_uaprintf("%s, %d, %x, %x\n",
                                    dmntbl->vddp[i]->vdName,
                                    dmntbl->vddp[i]->serviceClass,
                                    dmntbl->vddp[i]->device,
                                    clu_global_rootdev[i]);
                            }
                            ADVFS_SAD0("bs_bfdmn_tbl_activate: domain mntId mismatch for global root");
                        }
                    }
                }

                if (vdAttrp->state == BSR_VD_MOUNTED) {
                    /*
                     * Atleast one of the vd's was mounted in this
                     * domain. Set the domain's state accordingly so
                     * that we make the right decision in bs_bfdmn_activate
                     * on whether to call recovery or not for V3 domains.
                     */
                    dmnP->state = dmnState;
                }
            }

            /*
             * Check the domain version for consistency with
             * the other virtual disks in the domain.
             */

            if ((dmnP->dmnVersion.odv_major != dmnVersion.odv_major) || 
                (dmnP->dmnVersion.odv_minor != dmnVersion.odv_minor)) {
                if (clu_global_rootdev[0] == clu_global_rootdev_opinprog) {
                    goto cleanup_global_rootdev;
                } else if (clu_global_rootdev[vdi] == clu_global_rootdev_opinprog) {
                    update_global_rootdev = vdi;
                    clu_global_rootdev_opinprog = (dev_t)NULL;
                    continue;
                } else {
                    ADVFS_SAD0("bs_bfdmn_tbl_activate: incorrect domain version for global root");
                }
            }
        }

        /* 
         * Load domain attribute parameters to domain table.
         */
        dmnP->maxVds = dmnAttrp->maxVds;  
        dmnP->dmnCookie = dmnAttrp->MasterFsCookie;
        
        /*
         * If this disk contains a more recent copy of the log
         * attributes then save these log attributes for later use.
         */

        if ((dmnMAttrp != NULL) && (dmnMAttrp->seqNum > dmnMAttr.seqNum)) {
            bcopy( (char *)dmnMAttrp, (char *)&dmnMAttr, sizeof(bsDmnMAttrT) );
            dmnMAttrIdx = vdAttrp->vdIndex;
        }

        /* Check to see if this index is already in use */
        if ( vd_htop_if_valid(vdAttrp->vdIndex, dmnP, FALSE, FALSE) ) {
            sts = EDUP_VD;
            goto vd_add_error;
        }

        sts = setup_vd( dmnP,
                       vnp,
                       vdd->vdName,
                       vdAttrp,
                       flags,
                       &vdp );
        if ( sts != EOK ) {
            if (clu_global_rootdev[vdi] == clu_global_rootdev_opinprog) {
                update_global_rootdev = vdi;
                clu_global_rootdev_opinprog = (dev_t)NULL;
                continue;
            } else {
                ADVFS_SAD0("bs_global_root_activate: can't set vd for global root");
            }
        }

        closeVd = TRUE;

        /*
         * Add vdp to domain and domain's service classes.
         */

        sts = sc_add_vd( dmnP, vdp->serviceClass, vdAttrp->vdIndex );
        if (sts != EOK) {
            goto get_attrs_error;
        }
    }

    if (dmnMAttrIdx == 0) {
        ms_uprintf("Global root is missing the log volume.\n");
        ms_uprintf("Check for missing links before mounting.\n");
        sts = E_VD_DMNATTR_DIFF;
        goto get_attrs_error;
    }

    /*
     * check the vd count. If wrong, don't mount the domain.
     */

    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        int inprog_idx, vd_idx, vd_count = 0;

        for (inprog_idx = 0; inprog_idx < dmntbl->vdCount; inprog_idx++) {
            if (clu_global_rootdev[inprog_idx] == clu_global_rootdev_opinprog) {
                update_global_rootdev = inprog_idx;
                for (vd_idx = 1; 
                     vd_count < dmnP->vdCnt && vd_idx <= BS_MAX_VDI; 
                     vd_idx++) {
                    if (vdp = vd_htop_if_valid(vd_idx, dmnP, FALSE, FALSE)) {
                        if (vdp->devVp->v_rdev == clu_global_rootdev_opinprog) {
                            vd_remove(dmnP, vdp, nilBfDomainId,
                                      ADVMTX_VD_STATE_CHANGE | VD_REMOVE_PERM);
                            break;
                        }
                        vd_count++;
                    }
                }
                clu_global_rootdev_opinprog = (dev_t)NULL;
                break;
            }
        }
    }

    /*
     * Recheck vd count, in case it got changed by the above.
     */
    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        ms_uprintf("Volume count mismatch for global root. Global root expects"
                " %d volumes,\n",
                dmnMAttr.vdCnt);
        ms_uprintf("global root config has %d devices.\n", dmnP->vdCnt);
        sts = E_VOLUME_COUNT_MISMATCH;
        goto get_attrs_error;
    }

    if (update_global_rootdev != -1) {
        /*
         * Found resolvable volume conflicts for cluster global root.
         * Update the global rootdev array to reflect this.
         */
        clu_global_rootdev_count--;
        if (update_global_rootdev != clu_global_rootdev_count) {
            for (vdi = update_global_rootdev; vdi < clu_global_rootdev_count;
                vdi++) {
                clu_global_rootdev[vdi] = clu_global_rootdev[vdi+1];
            }
        }
        clu_global_rootdev[clu_global_rootdev_count] = (dev_t)NULL;
    }

    /*
     * dmnP and CFS vol data has been reconciled. /dev/advfs will be
     * verified at mount update time. Check to see if domain's transient
     * record indicates that an addvol was interrupted. If so, remove
     * this volume out of the service class so that no storage allocations
     * are done from this "suspect" volume. If all the information is
     * ok, this will be added back into the service class in
     * bs_fix_root_dmn (called at mount update).
     */

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);
    mdap = vdp->rbmtp;

    sts = bs_pinpg (&pgPin, 
                   (void*)&bmtPage, 
                   mdap, 
                   0, 
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);

    if (sts != EOK) {
        goto get_attrs_error;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr) {
        if ((dmnTAttr->dev) && (dmnTAttr->op == BSR_VD_ADDVOL)) {
            int vdCnt = 0;

            for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) ) {
                    continue;
                }

                vdCnt++;

                if (vdp->devVp->v_rdev == dmnTAttr->dev) {
                    (void) sc_remove_vd(dmnP, vdp->serviceClass, vdp->vdIndex);
                    break;
                }
            }
        }
    }

    (void)bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);

    /*
     * If the recoveryFailed flag is set in the mutable domain
     * attributes, and AdvfsFixUpSBM is not set, allow the mount
     * to proceed, but send out an warning message and an EVM 
     * notice.
     */
    if (!AdvfsFixUpSBM && (dmnMAttr.bs_dma_dmn_state & DMNS_RECOVERY_FAILED)) {
        /* 
         * If we are activating the root domain, allow mounting
         * so the system can still boot
         */          
         ms_uprintf("ADVFS : Domain %s activated despite an inconsistency\n", domainName);
         ms_uprintf("found in the AdvFS SBM metadata file.\n"); 
         ms_uprintf("Recommend you follow the procedures described\n");
         ms_uprintf("in the 'Trouble Shooting: Can't Clear Bit Twice Error\n");
         ms_uprintf("Message' section of the AdvFS Administration Guide.\n");
         {
             advfs_ev advfs_event;
             bzero(&advfs_event, sizeof(advfs_ev));
             advfs_event.domain = domainName;
             advfs_post_kernel_event(EVENT_FDMN_CCBT_IGNORED, &advfs_event);
         }
    }

    dmnP->ftxLogTag = dmnMAttr.ftxLogTag;
    dmnP->ftxLogPgs = dmnMAttr.ftxLogPgs;
    dmnP->bfSetDirTag = dmnMAttr.bfSetDirTag;
    dmnP->uid = dmnMAttr.uid;
    dmnP->gid = dmnMAttr.gid;
    dmnP->mode = dmnMAttr.mode;

    /* Allocate and initialize the domain's transaction table. */
    ftx_init_table(dmnP);

    sts = bs_bfdmn_activate( domainId, flags );

    if ( sts != EOK ) {
        goto get_attrs_error;
    }

    /*
     * Clear recovery failed flag unconditionally if set at this
     * point (recovery completed).
     */
    if (dmnMAttr.bs_dma_dmn_state & DMNS_RECOVERY_FAILED) {
        (void)set_recovery_failed(dmnP, DMNS_CLEAR_RECOVERY);
    }

    dmnP->dmnFlag = BFD_NORMAL;
    *bfDomainId = domainId;

    free_vdds( dmntbl );
    ms_free( dmntbl );
    return EOK;

cleanup_global_rootdev:
    /*
     * We found global rootdev conflicts between vol1 and vol2, and
     * realized that vol1 was the stale volume. Update global rootdev
     * array, throw away data from stale volume and start all over.
     */
    if (vdi != 1) {
        ADVFS_SAD0("bs_global_root_activate: metadata conflicts for global root");
    }
    clu_global_rootdev_opinprog = (dev_t)NULL;
    clu_global_rootdev_count--;
    for (id = 0; id < clu_global_rootdev_count; id++) {
        clu_global_rootdev[id] = clu_global_rootdev[id+1];
    }
    clu_global_rootdev[clu_global_rootdev_count] = (dev_t)NULL;
    restart = TRUE;
    (void) closed(vnp->v_rdev, S_IFBLK, FREAD | FWRITE);

    goto get_attrs_error;

vd_add_error:
    (void) closed(vnp->v_rdev, S_IFBLK, FREAD | FWRITE);

    /* Nothing done with the error return above now */
    VN_RELE( vnp );

get_attrs_error:
    if (closeVd && (TEST_DMNP(dmnP) == EOK)) {

        /* Close volumes that were opened above */

        statusT temp_sts;

        for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
                /*
                 * Tru64 called bs_reclaim_cfs_rsvd_vn here on all rsvd
                 * files on the volume.  This should not be necessary with
                 * CFS's new design which releases PFS vnodes on close.
                 */
                vd_remove(dmnP, vdp, nilBfDomainId, 0);
            }
    }

    if (TEST_DMNP(dmnP) == EOK) {
        /* deallocate the domain handle/structure */
        if (dmnP->activateCnt == 0) {
            dmn_dealloc( dmnP );
        }
    }

get_table_error:
    if (dmntbl != NULL) {
        free_vdds( dmntbl );
        ms_free( dmntbl );
    }

    if (restart)
        goto start;

    return sts;
}


/*
 * advfs_verify_access_lists
 *
 * This function searches the free and closed list for any bfaps
 * and reports the first bfap belonging to the domain that it finds.
 * If none belong to the domain it returns NULL.  The function will search
 * the free list before the closed list.
 *
 */

#ifdef ADVFS_SMP_ASSERT

static bfAccessT*
advfs_verify_access_lists(
                 domainT *dmnP   /* in */
                 )
{
    bfAccessT *badBfap = NULL;
    bfAccessT *bfap;
    int i;


    for (i=0; i < ADVFS_RESOURCE_TABLE_SIZE; i++) {
        /*
         * Walk the free list and look for a bfap that belongs to dmnP
         */
        ADVSMP_FREE_LIST_LOCK( &advfs_access_free_lock[i] );
        bfap = advfs_free_acc_list[i].freeBwd;
    
        while (bfap != NULL && bfap != (bfAccessT *)(&advfs_free_acc_list[i])) {
            if (bfap->dmnP == dmnP) {
                ADVSMP_FREE_LIST_UNLOCK( &advfs_access_free_lock[i]);
                return bfap;
            }
            bfap = bfap->freeBwd;
        }
        ADVSMP_FREE_LIST_UNLOCK( &advfs_access_free_lock[i] );
    }
    for (i=0; i < ADVFS_RESOURCE_TABLE_SIZE; i++) {
        /*
         * Walk the closed list and look for a bfap that belongs to dmnP
         */
        ADVSMP_CLOSED_LIST_LOCK( &advfs_access_closed_lock[i] );
        bfap = advfs_closed_acc_list[i].freeBwd;

        while (bfap != (bfAccessT *)(&advfs_closed_acc_list[i])) {
            if (bfap->dmnP == dmnP) {
                ADVSMP_CLOSED_LIST_UNLOCK(&advfs_access_closed_lock[i]);
                return (bfap);
            }        
            bfap = bfap->freeBwd;
        }
        ADVSMP_CLOSED_LIST_UNLOCK(&advfs_access_closed_lock[i]);
    }
    
    /*
     * Found nothing, return NULL
     */
    return (NULL);
}

#endif /* #ifdef ADVFS_SMP_ASSERT */

/*****************************************************/
/*****  Major VD, bf domain management functions *****/
/*****************************************************/

/*
 * bs_bfdmn_deactivate
 *
 * Cleanly shut down the specified domain.  Free
 * the vd and domain structs.
 *
 * Returns ENO_SUCH_DOMAIN, E_DOMAIN_NOT_CLOSED, or EOK
 * Returns ENOMEM, E_TOO_MANY_ACCESSORS, or EOK 
 */

statusT
bs_bfdmn_deactivate(
    bfDomainIdT domainId,
    uint64_t flags)
{
    vdIndexT vdi;
    domainT* dmnP;
    vdT* vdp;
    int rem, tblLocked = FALSE;
    statusT sts = EOK;
    bfSetT *bfSetp;
    int vdCnt;
    bfTagT bfTag;
    int s;
    uint64_t isMount = flags & DMNA_MOUNTING;
    bfAccessT *badBfap;
    struct timeval ltime;
    bsIdT unmount_time;

    /*
     * Don't lock the domain table for global root. DLM locks in CFS
     * prevent multiple global root deactivations. Taking this lock out
     * for root causes deadlocks during global root failover.
     */
    if (!(flags & DMNA_GLOBAL_ROOT)) {
        ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );
        tblLocked = TRUE;
    }

    if( (dmnP = domain_id_lookup( domainId, flags )) == NULL ) {
        RAISE_EXCEPTION( ENO_SUCH_DOMAIN );
    }

    if (dmnP->activateCnt == 0) {
        RAISE_EXCEPTION( E_DOMAIN_NOT_ACTIVATED );
    }

    dmnP->activateCnt--;

    if (dmnP->activateCnt > 0) {
        /*
         * Only last deactivation request really deactivates the domain.
         */
        RAISE_EXCEPTION( EOK );
    }

    /* Before deactivating domain, flush vfast record to disk. */
    (void)ss_put_rec (dmnP);  /* ignore error, must not hold up ss deactivate */

    /*
     * Wait for dmnAccCnt threads to finish. Set BFD_DEACTIVATE_PENDING
     * for transaction code to not trip over the dmnP->state  After this, no new
     * activity should come into the domain. 
     */

    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    dmnP->dmnFlag |= BFD_DEACTIVATE_PENDING;
    /* Also forces any pending migrates to abort.  The dmnAccCnt is
     * held by vfast migrate thd and is released when the thread
     * aborts. The deactivation thread in this routine will block and
     * wait for vfast to perform a domain_close and force vfast's
     * portion of the dmnAccCnt to go to 0.
     */

    ss_dmn_deactivate(dmnP, TRUE);

    dmnP->state = BFD_DEACTIVATED;
    if (dmnP->dmnAccCnt > 0) {
        dmnP->dmnRefWaiters++;
        while (TRUE) {
            cv_wait(&dmnP->dmnRefWaitersCV, &DmnHashTblLock,
                            CV_MUTEX, CV_NO_RELOCK);
            if (!dmnP->dmnRefWaiters) {
                    break;
            }
            ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        }
    } else {
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    }

    /*
     * Set dmnP->dmnFlag to BFD_DEACTIVATE_IN_PROGRESS.  This is necessary
     * for underlyling transaction/log code to work, now that
     * dmnP->state is BFD_DEACTIVATED.  Transactions may take place 
     * in advfs_lgr_checkpoint_log(), and possibly elsewhere.
     */

    dmnP->dmnFlag = (dmnP->dmnFlag & (~(BFD_DEACTIVATE_PENDING))) 
        | BFD_DEACTIVATE_IN_PROGRESS;

    /*
     * Tru64 used to loop through each vd and call bs_reclaim_cfs_rsvd_vn
     * before continued.  CFS will now release the PFS vnode v_count on last
     * close, so this code should not longer be necessary.
     */

    /*
     * Flush all of the domain's buffers as well as trim the
     * log. After this, the log will have just one record - for the
     * exclusive transaction started by the checkpoint code. Since
     * all of the metadata buffers are flushed at this point, we need to
     * trim the log so that the old log records are not sitting around 
     * (to be reprocessed at next domain activation). 
     */

    advfs_bs_dmn_flush(dmnP, FLUSH_UPDATE_STATS);
    advfs_lgr_checkpoint_log(dmnP);
    
    /* close the tag directory */

    sts = bs_close(dmnP->bfSetDirAccp, MSFS_CLOSE_NONE);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfdmn_deactivate: bs_close of tag dir failed", sts );
    }

    dmnP->bfSetDirAccp = NULL;

    /*
     * Loop and flush extent caches.
     */

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        /* The DmnActivationLock prevents racing addvol/rmvol; no need to bump
         * the vdRefCnt here.
         */
        if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
            vdCnt++;
            sts = sbm_clear_cache( vdp );
            if ( sts != EOK ) {
                ADVFS_SAD1("bs_bfdmn_deactivate: sbm_clear_cache failure ",sts);
            }
        }
    }
    /* We saw as many volumes as there are in the domain. This ASSERTion
     * is OK since we have the DmnActivationLock and no racing threads can
     * add or remove disks while we go thru this loop.
     */
    MS_SMP_ASSERT(vdCnt == dmnP->vdCnt);
    
    /* close the log file */
    sts = lgr_close( dmnP->ftxLogP );

    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfdmn_deactivate: lgr_close of ftx log failed ", sts );
    }

    /*
     * Loop and dismount all virtual disks.
     * vd_remove should change the vd state to BSR_VD_DISMOUNTED,
     * this should not fail because vd is inaccessible.
     */
    ltime = get_system_time();
    unmount_time.id_sec  = ltime.tv_sec;
    unmount_time.id_usec = ltime.tv_usec;
    for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
        if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
            /*
             * In the cluster, for non-mount paths we release the dlm device locks.
             */
            if (clu_is_ready() && !(isMount)) {
                sts = CLU_DEV_DLM_UNLOCK(vdp->devVp->v_rdev);
                if (sts != EOK)
                    ADVFS_SAD2("bs_bfdmn_deactivate: could not unlock device %lx failure code %d \n",vdp->devVp->v_rdev, sts);
            }

            if (flags & DMNA_UNMOUNT_RQST) {
                vd_remove(dmnP, vdp, unmount_time, 
                          ADVMTX_VD_STATE_CHANGE | SET_UNMOUNT_TIME);
            } else {
                vd_remove(dmnP, vdp, nilBfDomainId, ADVMTX_VD_STATE_CHANGE);
            }
        }

    /*
     * Deallocate root fileset and free all the access structures associated
     * with it.
     */

    dmnP->bfSetDirp->fsRefCnt--;

    /*
     * Scan the closed list for any bfaps belonging to this domain.  And 
     * ASSERT if any are found.
     */

    ss_dealloc_dmn( dmnP );

    dmn_dealloc( dmnP );

    /*
     * Scan the closed list for any bfaps belonging to the deleted domain.  
     * And ASSERT if any are found.  This has to be done after the domain 
     * is deallocated, because the root fileset is deallocated during domain
     * deallocation.
     */

    MS_SMP_ASSERT((badBfap = advfs_verify_access_lists(dmnP)) == NULL);

HANDLE_EXCEPTION:
    if (tblLocked) {
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    }
    return sts;
}


/*
 * Number of transaction records in the log under which during the mounting
 * of AdvFS rootfs data flush will be delayed until mount update.
 */
#define LIMIT_RECOV_DELAY  5

/*
 * bs_bfdmn_activate
 *
 * This function is called after all of the virtual disks in the
 * bf domain have already been added to the domain.  This function
 * performs bf domain specific actions such as accessing the log file,
 * performing bf domain recovery, accessing the tag directory, then
 * finally marking the virtual disk and domain attribute records to
 * note that the domain has been activated.
 *
 * TODO: Some "domain state" enforcement to stop anything interesting
 * from happening when things aren't ready yet.  Timestamps in
 * addition/lieu of "MOUNTED" and "ACTIVATED" states.
 *
 * Assumes DmnTbl lock is held.
 *
 * Returns ENO_SUCH_DOMAIN, possible status from bs_refpg and
 * bs_deref_pg, or EOK
 */

statusT
bs_bfdmn_activate(
    bfDomainIdT domainId,      /* in */
    uint64_t flags)            /* in */
{
    int vdcnt, done, s;
    vdIndexT vdi;
    struct timeval ltime;
    struct timezone tz;
    int logOpen = FALSE, tagDirOpen = FALSE, setDesc = FALSE;
    domainT* dmnP;
    struct vd* vdp;
    statusT sts;
    logDescT *ldP;
    logRecAddrT nextlogaddr;
    bfAccessT *bfSetDirAccp;
    bfSetT *bfSetp;
    bfAccessT *logbfap;
    extern int MountWriteDelay;
    bfDmnStatesT orig_dmnP_state;

    /*
     * Assert that either we are global root or the state of the lock is not
     * RWL_UNLOCKED (ie, it's held).
     */
    MS_SMP_ASSERT((flags & DMNA_GLOBAL_ROOT) ? 1 : 
                 ADVFS_SMP_RW_LOCK_NEQL(&DmnActivationLock, RWL_UNLOCKED));
    if( (dmnP = domain_id_lookup( domainId, flags )) == 0 ) {
        return( ENO_SUCH_DOMAIN );
    }

#ifdef OSDEBUG
    /*
     * Initialize crashTest. Used by ADVFS_CRASH_RECOVERY_TEST to disable
     * flushing of metadata, in advfs_bs_dmn_flush_meta, during crash-recovery
     * log testing.
     */
    dmnP->crashTest = 0;
#endif

    if (BS_BFTAG_EQL(dmnP->ftxLogTag, NilBfTag)) {
        ms_uprintf( "bs_bfdmn_activate: domain has no log tag!!!!" );
        return( E_INVALID_MSFS_STRUCT );
    }

    if ( dmnP->state == BFD_VIRGIN ) {
        /*
         * This is a special case to set up the free space cache when
         * the domain is being activated for the very first time,
         * because the initial log open will allocate storage for the
         * log then.
         */
        vdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
        if ((sts = sbm_init( vdp )) != EOK) {
            return sts;
        }
    }

    /*********************************************************************
     *********  Note that the extent maps for the reserved (metadata)
     *********  bitfiles must be consistent prior to running recovery,
     *********  because they are accessed prior to executing recovery.
     *********  This means that their extent maps must be modified
     *********  using careful writing techniques.
     ********************************************************************/

    sts = lgr_open( &dmnP->ftxLogP,
                    &nextlogaddr,
                    &logbfap,
                    dmnP->ftxLogTag,
                    dmnP->bfSetDirp,
                    (flags & DMNA_FMOUNT) ? TRUE : FALSE );

    if (sts != EOK) {
        ms_uprintf("bs_bfdmn_activate: can't open log for domain '%s',\ntag = 0x%lx.0x%x: %s\n",
                dmnP->domainName, dmnP->ftxLogTag.tag_num, dmnP->ftxLogTag.tag_seq, BSERRMSG( sts ) );
        return( sts );
    }

    dmnP->logAccessp = logbfap;
    logOpen = TRUE;

    /* assert that the log has valid in-mem structures */
    MS_SMP_ASSERT(dmnP->logAccessp->xtnts.validFlag == XVT_VALID);

    /*
     * Access the reserved tag directory tag.
     */

    if (BS_BFTAG_EQL(dmnP->bfSetDirTag, NilBfTag)) {
        ms_uprintf( "bs_bfdmn_activate: domain has no tag dir tag!!" );
        sts = E_INVALID_MSFS_STRUCT;
        goto _error_cleanup;

    } else {
        vdi = BS_BFTAG_VDI(dmnP->bfSetDirTag);
        sts = bfm_open_ms(&bfSetDirAccp, dmnP, vdi, BFM_BFSDIR );
        if (sts != EOK) {
            ms_uprintf( "bs_bfdmn_activate: can't open domain bitfile set dir, "
                       "tag = 0x%lx.0x%x: %s",
                       dmnP->bfSetDirTag.tag_num, dmnP->bfSetDirTag.tag_seq,
                       BSERRMSG( sts ) );
            goto _error_cleanup;
        }
        tagDirOpen = TRUE;

        dmnP->bfSetDirAccp = bfSetDirAccp;
        bfSetp = dmnP->bfSetDirp;
        bfSetp->dirBfAp = dmnP->bfSetDirAccp;
        bfSetp->fsRefCnt++;
        setDesc = TRUE;

        sts = tagdir_get_info(bfSetp, &bfSetp->tagUnInPg,
                              &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                              &bfSetp->bfCnt);
        if (sts != EOK) {
            goto _error_cleanup;
        }
    }

    /*
     * If mounting root FS, decide whether we should delay writes by finding
     * out the size of the log.
     */
    if (flags & (DMNA_GLOBAL_ROOT | DMNA_LOCAL_ROOT)) {
        if (num_of_log_recs(dmnP, LIMIT_RECOV_DELAY) < LIMIT_RECOV_DELAY)
        MountWriteDelay = 1;
    }

    if (dmnP->state != BFD_VIRGIN) {
        sts = ftx_bfdmn_recovery( dmnP, flags );
        if (sts != EOK) {
            goto _error_cleanup;
        }
    }

    /*
     * If this is dual mount case, sweep the disks and write new
     * domain id.
     */
    if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
        sts = bs_bfdmn_sweep(dmnP, DMN_FULL_SWEEP);
        if (sts != EOK)
            goto _error_cleanup;
        dmnP->dmnFlag &= ~BFD_DUAL_MOUNT_IN_PROGRESS;
    } else {
        if (flags & DMNA_RENAME) {
            sts = bs_bfdmn_sweep(dmnP, DMN_NAME_SWEEP);
            if (sts != EOK)
                goto _error_cleanup;
            flags &= ~DMNA_RENAME;
        }
    }

    /*
     * The disks and domain are in a consistent state so finish the
     * mounting process.
     */
    dmnP->totalBlks = 0;
    dmnP->freeBlks = 0;

    /*
     * Establish new domain mount time.
     */
    ltime = get_system_time();
    /* field sizes don't match so copy individually  */
    dmnP->bfDmnMntId.id_sec  = ltime.tv_sec;
    dmnP->bfDmnMntId.id_usec = ltime.tv_usec;

    vdcnt = 0;
    for ( vdi = 1; vdcnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        /* No need to bump vdRefCnt here since DmnActivationLock is held to
         * prevent a racing rmvol.
         */
        if ( !(vdp = vd_htop_if_valid( vdi, dmnP, FALSE, FALSE)) ) {
                continue;
        }
        sts = set_vd_mounted( dmnP, vdp );
        if (sts != EOK) {
            domain_panic(dmnP, "corrupt volume on mount" );
            goto _error_cleanup;
        }
        vdcnt++;
    }
    /* We saw as many volumes as there are in the domain. OK:
     * DmnActivationLock is held to prevent racing addvol and rmvol.
     */
    MS_SMP_ASSERT(vdcnt == dmnP->vdCnt);

    /*
     * If not doing write delay then flush all dirty buffers and wait for
     * that i/o to complete
     */
    if (!MountWriteDelay) {
        advfs_bs_dmn_flush(dmnP, FLUSH_UPDATE_STATS);
    }

    orig_dmnP_state = dmnP->state;
    dmnP->state = BFD_ACTIVATED;
    dmnP->activateCnt++;

    /*
     * Remove any junk leftover by in-progress deletes.
     */
    if (!clu_is_ready() || !(flags & DMNA_FAILOVER)) {
        vdcnt = vdi = 1;
        while ((vdi <= BS_MAX_VDI) && (vdcnt <= dmnP->vdCnt )) {
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
                sts = del_clean_mcell_list (vdp, flags);
                vdcnt++;
            }
            vdi++;
        }
    }

    /*
     * Delete all 'delete pending' bitfile sets.
     */
    bfs_delete_pending_list_finish_all( dmnP );

    ss_dmn_activate(dmnP);

    if (!dmnP->dmn_panic) {
        return EOK;
    }

    /*
     * There has been a domain panic.  Restore the state and activateCnt.
     * Finally do an error cleanup and return E_DOMAIN_PANIC.
     */
     dmnP->activateCnt--;
     dmnP->state = orig_dmnP_state;
     sts = E_DOMAIN_PANIC;

_error_cleanup:
    if (setDesc) {
        bfSetp->fsRefCnt--;
    }

    if (tagDirOpen) {
        (void) bs_close(bfSetDirAccp, MSFS_CLOSE_NONE);
    }

    if (logOpen) {
        advfs_bs_dmn_flush(dmnP, FLUSH_UPDATE_STATS);
        (void) lgr_close( dmnP->ftxLogP );
    }

    return sts;
}

/*
 * bs_bfdmn_sweep
 *
 * Rewrite on disk domainid
 */

static statusT
bs_bfdmn_sweep(domainT * dmnP, dmn_sweep_type sweep_type)
{
    statusT sts;
    void *pgp;
    bfDomainIdT domainId = dmnP->domainId;
    int error;
    vdIndexT vdi, logVdI, tagdirVdI;
    bfPageRefHT rbmtPgRef=NilBfPageRefH;
    bfPageRefHT rootTagPgRef=NilBfPageRefH;
    int vdCnt;

    MS_SMP_ASSERT((sweep_type == DMN_FULL_SWEEP) ||
                   (sweep_type == DMN_NAME_SWEEP));
        
    if (BS_BFTAG_EQL(dmnP->ftxLogTag, NilBfTag)) {
        ms_uprintf( "bs_bfdmn_sweep: domain has no log tag!!!!" );
        return( E_INVALID_MSFS_STRUCT );
    }

    logVdI = BS_BFTAG_VDI(dmnP->ftxLogTag);
    tagdirVdI = BS_BFTAG_VDI(dmnP->bfSetDirTag);

    /*
     * Loop through all the disks in the domain, read all of the
     * RBMTs and update bsDmnAttrT and bsDmnNameT records
     */

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        vdT *vdp;
        bsDmnAttrT* dmnAttrp;
        bsDmnNameT* dmnNamep;
        struct bfAccess *mdap;

        /* We are still activating this disk, so no need to bump its
         * vdRefCnt.
         */
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) )
            continue;
        vdCnt++;

        mdap = vdp->rbmtp;
        if (error = bs_pinpg (&rbmtPgRef,
                              (void *)&pgp,
                              mdap,
                              0,
                              FtxNilFtxH,
                              MF_VERIFY_PAGE)) {
            sts = (statusT)error;
            goto err_cleanup;
        }


        dmnNamep = bmtr_find(&((bsMPgT *)pgp)->bsMCA[BFM_DMN_NAME],
                             BSR_DMN_NAME, dmnP);
                  
        /*
         * Allow failures for older filesystems that may not
         * have domain name records.
         */
        if (dmnNamep != 0) {
            strcpy(dmnNamep->dmn_name, dmnP->domainName);
        }

        if (sweep_type != DMN_FULL_SWEEP) {
            if (error = bs_unpinpg( rbmtPgRef, logNilRecord, BS_CLEAN)) {
                sts = (statusT)error;
                goto err_cleanup;
            }
            continue;
        }

        dmnAttrp = bmtr_find(&((bsMPgT *)pgp)->bsMCA[BFM_RBMT_EXT],
                             BSR_DMN_ATTR, dmnP);
        if (dmnAttrp == 0) {
            sts = E_INVALID_MSFS_STRUCT;
            goto err_cleanup;
        }

        dmnAttrp->bfDomainId = domainId;
                  
        if (vdi == tagdirVdI) {
            /*
             * This vd has the root tagdir on it. Update the
             * domainIdin each filesetid.
             */
            bsMRT* recp;
            bsXtntRT* primXtnt;
            uint64_t rootTagPg;

            recp = (bsMRT*)((bsMPgT *)pgp)->bsMCA[BFM_BFSDIR].bsMR0;

            if ( recp->type != BSR_ATTR ) {
                ms_uprintf("bs_bfdmn_sweep: first record is type %d\n",
                           recp->type);
                sts = ENO_BS_ATTR;
                goto err_cleanup;
            }

            if ( recp->bCnt != sizeof(bsBfAttrT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for "
                           "BSR_ATTR record", recp->bCnt);
            }

            recp = (bsMRT *)((char *)recp + roundup(recp->bCnt,
                        sizeof(uint64_t)));
            if ( recp->type != BSR_XTNTS ) {
                ms_uprintf("bs_bfdmn_sweep: second record is type %d\n",
                           recp->type);
                sts = ENO_XTNTS;
                goto err_cleanup;
            }

            if ( recp->bCnt != sizeof(bsXtntRT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for "
                           "BSR_XTNTS record", recp->bCnt);
            }

            primXtnt = (bsXtntRT *)((char*)recp + sizeof(struct bsMR));
            if ( primXtnt->xCnt > BMT_XTNTS || primXtnt->xCnt == 0) {
                ADVFS_SAD1("bs_bfdmn_sweep: N1 extents in the primary mcell\n",
                           primXtnt->xCnt);
            }

            if ( primXtnt->mcellCnt != 1) {
                ms_uprintf("Root tagdir over multiple mcells\n");
                sts = ENOT_SUPPORTED;
                goto err_cleanup;
            }

            for (rootTagPg = 0;
                 rootTagPg < (primXtnt->bsXA[1].bsx_fob_offset /
                              ADVFS_METADATA_PGSZ_IN_FOBS );
                 rootTagPg++) {
                        
                bsTDirPgT* rootTagPgP;

                if (error = bs_refpg(&rootTagPgRef,
                                     (void *)&rootTagPgP,
                                     dmnP->bfSetDirAccp,
                                     rootTagPg,
                                     FtxNilFtxH,
                                     MF_VERIFY_PAGE)) {
                    sts = (statusT)error;
                    goto err_cleanup;
                }

                if (rootTagPgP->tpNumAllocTMaps != 0) {
                    int i, start_indx;

                    /*
                     * Ignore TagPage0-Slot0 as it contains the freeList and
                     * unInitPg values and not a valid mcellId. A value of 1
                     * in the BS_TD_IN_USE bit would incorrectly identify
                     * TagPage0-Slot0 as having a valid mcellId. 
                     */
                    start_indx = (rootTagPg == 0) ? 1 : 0; 
                    for (i=start_indx; i<BS_TD_TAGS_PG; i++) {
                        bfMCIdT mcid;

                        if (rootTagPgP->tMapA[i].tmFlags & BS_TD_IN_USE) {
                            mcid = rootTagPgP->tMapA[i].tmBfMCId;
                            if (error = update_bfsetid(dmnP,
                                                       mcid,
                                                       domainId)) {
                                sts = (statusT)error;
                                goto err_cleanup;
                            }
                        }
                    }
                }

                if (error = bs_derefpg(rootTagPgRef, BS_RECYCLE_IT) ) {
                    sts = (statusT)error;
                    goto err_cleanup;
                }
                rootTagPgRef=NilBfPageRefH;
            }
        }

        if (error =  bs_unpinpg(rbmtPgRef, logNilRecord, BS_DIRTY)) {
            sts = (statusT)error;
            goto err_cleanup;
        }
        rbmtPgRef = NilBfPageRefH;
    }
        
    /* We saw as many volumes as there are in the domain. */
    MS_SMP_ASSERT(vdCnt == dmnP->vdCnt);
    
    return EOK;

err_cleanup:

    if (rbmtPgRef)
    {
        bs_unpinpg(rbmtPgRef, logNilRecord, BS_DIRTY);
    }
    if (rootTagPgRef)
    {
        bs_derefpg(rootTagPgRef, BS_RECYCLE_IT);
    }
    return(sts);
}

/*
 * update_bfsetid
 *
 * Update on disk bfset attributes with the new domainid
 */

static statusT
update_bfsetid(
               domainT* dmnP,
               bfMCIdT mcid,
               bfDomainIdT domainId
               )
{
    statusT sts = EOK;
    int error;
    bsMPgT* pagep;
    vdT* vdp = VD_HTOP(mcid.volume, dmnP);
    bsBfSetAttrT* bfsAttrp;
    bfPageRefHT bmtPgRef;

mcell_chain:
    if (sts = bs_pinpg (&bmtPgRef,
                        (void*)&pagep,
                        vdp->bmtp,
                        mcid.page,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) {
        return(sts);
    }

    bfsAttrp = bmtr_find(&pagep->bsMCA[mcid.cell],
                          BSR_BFS_ATTR, dmnP);
    if (bfsAttrp == NULL) {
        mcid = pagep->bsMCA[mcid.cell].mcNextMCId;
        if ( MCID_EQL(mcid, bsNilMCId))
        {
            sts = E_INVALID_MSFS_STRUCT;
            goto err_cleanup;
        }
        (void) bs_unpinpg (bmtPgRef, logNilRecord, BS_CLEAN);
        goto mcell_chain;
    }

    bfsAttrp->bfSetId.domainId = domainId;

    if ( !BS_UID_EQL(bfsAttrp->bfsaParentSnapSet.domainId, nilBfDomainId)) {
            bfsAttrp->bfsaParentSnapSet.domainId = domainId;
    }

    if ( !BS_UID_EQL(bfsAttrp->bfsaFirstChildSnapSet.domainId, nilBfDomainId)) {
            bfsAttrp->bfsaFirstChildSnapSet.domainId = domainId;
    }

    if ( !BS_UID_EQL(bfsAttrp->bfsaNextSiblingSnap.domainId, nilBfDomainId)) {
            bfsAttrp->bfsaNextSiblingSnap.domainId = domainId;
    }

err_cleanup:

    if (error = bs_unpinpg (bmtPgRef, logNilRecord, BS_DIRTY)) {
        sts = (statusT)error;
    }
    return(sts);
}

/*
 * set_vd_mounted
 *
 * Rewrite on disk vd attributes to set disk to mounted state.
 */

static
statusT
set_vd_mounted(
    struct domain* dmnP,      /* IN - domain struct */
    struct vd* vdp)           /* IN - vd struct */
{
    struct bsVdAttr* vdAttrp;
    struct bsXtntR*  bsXtntrp;
    bfPageRefHT bmtPgRef;
    struct bsMPg* bmtpgp;
    statusT sts;

    /*
     * Mark the disks and domain as mounted and active in their
     * on-disk structures (ie- the vd attributes and domain
     * attributes).  If the system crashes between here and
     * bs_domain_deactivate() then the next time the domain
     * is activated we'll know that we must recover the domain
     * since the on-disk attributes indicate that the system
     * stopped while the domain was active.
     */
    sts = bs_pinpg(&bmtPgRef,
                   (void *)&bmtpgp,
                   vdp->rbmtp,
                   0,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }

    vdAttrp = bmtr_find(&bmtpgp->bsMCA[BFM_RBMT],
                        BSR_VD_ATTR, vdp->dmnP);

    /* Get the mcell count from the primary mcell's BSR_XTNTS
     * record and use this to set vdp->lastRbmtPg.  This value is the
     * number of (mcells-1) since the first mcell is on page 0.
     */
    bsXtntrp = bmtr_find(&bmtpgp->bsMCA[BFM_RBMT],
                         BSR_XTNTS, vdp->dmnP);
    vdp->lastRbmtPg = bsXtntrp->mcellCnt - 1;

    if (vdAttrp == 0) {
        (void) bs_unpinpg( bmtPgRef, logNilRecord, BS_CLEAN );
        return E_INVALID_MSFS_STRUCT;
    }

    vdAttrp->state = BSR_VD_MOUNTED;
    vdAttrp->vdMntId = dmnP->bfDmnMntId;
    
    /* After the vdState is set to BSR_VD_MOUNTED and the vdStateLock is
     * dropped, other threads can see this disk as a valid volume.
     */
    ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
    vdp->vdState    = BSR_VD_MOUNTED;      
    vdp->vdSetupThd = NULL;
    ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );

    sts =  bs_unpinpg( bmtPgRef, logNilRecord, BS_WRITETHRU );
    if ( sts != EOK ) {
        return sts;
    }

    /*
     * Set the mcell free list head.
     */
    sts = bmt_set_mcell_free_list( vdp );
    if ( sts != EOK ) {
        return sts;
    }

    /*
     * Init the storage descriptors.
     */
    if ((sts = sbm_init( vdp )) != EOK) {
        return sts;
    }

    /*
     * Add this vd's free blocks to domain totals.
     */
    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));
    dmnP->totalBlks += (vdp->vdClusters * vdp->stgCluster); 
    dmnP->freeBlks  += (vdp->freeClust * vdp->stgCluster);
    ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));

    return EOK;
}


/*
 * This routine sets the dmnUmntThawTime value during a domain thaw.
 * The caller must hold an exclusive transaction to prevent updates to 
 * dmnUmntThawTime. The dmnUmntThawTime field is accessed only during a
 * clean unmount and during a domain thaw operation.
 *
 */

statusT
set_vd_time(
    domainT *dmnP)    /* IN - domain struct */
{
    struct bsDmnUmntThaw* dmnUmntThawp;
    bfPageRefHT rbmtPgRef;
    struct bsMPg* rbmtpgp;
    vdT *vdp;
    statusT sts;
    struct timeval ltime;
    bsIdT unmount_time;
    int vdi, vdCnt = 0;

    ltime = get_system_time();
    unmount_time.id_sec  = ltime.tv_sec;
    unmount_time.id_usec = ltime.tv_usec;

    for (vdi = 1; vdCnt <  dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {

            vdCnt++;

            sts = bs_pinpg(&rbmtPgRef,
                           (void *)&rbmtpgp,
                           vdp->rbmtp,
                           0,
                           FtxNilFtxH,
                           MF_VERIFY_PAGE);
            if (sts != EOK) {
                return sts;
            }


            dmnUmntThawp = bmtr_find(&rbmtpgp->bsMCA[BFM_RBMT],
                                     BSR_DMN_UMT_THAW_TIME, vdp->dmnP);

            /*
             * don't fail if record missing - allow for existing
             * filesystems
             */
            if (dmnUmntThawp == 0) {
                (void) bs_unpinpg( rbmtPgRef, logNilRecord, BS_CLEAN );
                return EOK;
            }

            dmnUmntThawp->dmnUmntThawTime = unmount_time;
    
            sts =  bs_unpinpg( rbmtPgRef, logNilRecord, BS_WRITETHRU );
            if ( sts != EOK ) {
                return sts;
            }
        }
    }

    return EOK;
}


/*
 * get_raw_vd_attrs
 *
 * This routine "opens" the specified disk, and reads the on-disk
 * bitfile metadata.
 *
 * Calls to this must be serialized because it uses the static global
 * buffer "Dpg" to read bmt page 0 into, which must remain valid for
 * the later call to setup_vd.
 *
 * Returns possible status from getvp, VOP_OPEN, VOP_CLOSE,
 * or EBUSY,EBAD_VDI, or EOK
 */

static
statusT
get_raw_vd_attrs(
    char                 *vdDiskName,     /* IN - disk device name */
    domainT              *dmnP,           /* IN - Domain pointer */
    vdIndexT             vdi,             /* IN - vd index */
    uint64_t             flags,           /* IN - flag */
    struct vnode         **vnodepp,       /* OUT - vnode ptr */
    struct bsVdAttr      **vdAttrpp,      /* OUT - vd attributes */
    bsDmnAttrT           **dmnAttrpp,     /* OUT - domain attributes */
    bsDmnMAttrT          **dmnMAttrpp,    /* OUT - log attributes */
    bsDmnNameT           **dmnNamepp,     /* OUT - domain name stored on disk */
    adv_ondisk_version_t *dmnVersion ,    /* OUT - domain version */
    struct bsMPg         *pgp)            /* OUT - RBMT page 0 */
{
    statusT                  sts;
    advfs_fake_superblock_t *superBlk;
    struct bsVdAttr         *vdAttrp;
    bsDmnAttrT              *dmnAttrp;
    bsDmnMAttrT             *dmnMAttrp;
    bsDmnNameT              *dmnNamep;
    int                      error;
    extern dev_t             rootdev;
    extern dev_t             clu_global_rootdev[BS_MAX_VDI];
    
    struct vnode    *vp      = NULL;
    struct vnode    *rvp     = NULL;
    uint64_t         isRoot  = flags & (DMNA_LOCAL_ROOT | DMNA_GLOBAL_ROOT);
    uint64_t         isMount = flags & DMNA_MOUNTING;
    dev_t            dev     = NODEV;

   /* get device's vnode */
    if (isRoot & DMNA_LOCAL_ROOT) {
        dev = rootdev;
    } else if (isRoot & DMNA_GLOBAL_ROOT) {
        dev = clu_global_rootdev[vdi];
    } else {
        if( error = advfs_getvp(&rvp, vdDiskName, UIOSEG_KERNEL, FOLLOW_LINK)) {
            return (statusT)error;
        }
        error = VOP_ACCESS( rvp, VREAD | VWRITE, TNC_CRED());
        if (error) {
            sts = (statusT)error;
            goto err_nodlmlock;
        }

        if( rvp->v_type != VBLK && rvp->v_type != VCHR ) {
            sts = (statusT) E_BAD_DEV;
            goto err_nodlmlock;
        }

        /* We are going to store a dev_vnode in the vd structure for
         * this disk.  This vnode only supports open, close, inactive
         * and strategy operations.  This differs from previous behaviour
         * where we used to store a spec_vnode in the vd structure.  However,
         * it is not possible to get one during mountroot so we have
         * to deal with a dev_vnode for all mounts/activations.
         */
        dev = rvp->v_rdev;
        VN_RELE(rvp);
        rvp = NULL;
    }

    /*
     * In the cluster, for paths other than addvol, rmvol, and mount
     * we acquire an exclusive DLM lock to serialize with other uses of
     * the block device. (The aforementioned cases do locking elsewhere.)
     */
    if (clu_is_ready() && !(isMount)) {
       error = CLU_DEV_DLM_LOCK(dev, 1);
       if( error ) {
           sts = (statusT) error;
           goto err_nodlmlock;
       }
    }

    /* This used to be a VOP_OPEN on a spec vnode, but we now store
     * a dev vnode.  In order to prevent multiple closes to the driver,
     * issued from kernel and user space, we need to open the device
     * via the spec layer opend() interface.  This is because
     * all other opens and closes to the device go through the spec
     * layer.
     */
    error = opend(&dev, S_IFBLK | IF_MI_DEV, FREAD | FWRITE, 0);

    if( error ) {
        sts = (statusT) error;
        goto err_vrele;
    }

    /* Get the device vnode.  We are limited to open, close, inactive
     * and strategy vnode ops on it.  However, open and close 
     * should go through the spec layer (opend and closed) rather
     * than the vnode op.
     */
    vp = devtovp(dev);

    superBlk = (advfs_fake_superblock_t *) ms_malloc( (size_t)ADVFS_METADATA_PGSZ );
    /*
     * Raw read of super block
     */
    sts = advfs_raw_io( vp, 
                        ADVFS_FAKE_SB_BLK, 
                        ADVFS_SUPER_BLOCK_SZ / DEV_BSIZE, 
                        RAW_READ,
                        superBlk );
    if (sts != 0) {
        goto err_freesblk;
    }

    if ( !ADVFS_VALID_FS_MAGIC(superBlk) ) {
        sts = E_BAD_MAGIC;
        goto err_freesblk;
    }

    ms_free( superBlk );
    superBlk = NULL;

    /*
     * Do a raw read of RBMT page 0
     */
    if( error = advfs_raw_io( vp, 
                              RBMT_BLK_LOC, 
                              ADVFS_METADATA_PGSZ_IN_FOBS /
                                ADVFS_FOBS_PER_DEV_BSIZE,
                              RAW_READ,
                              pgp) ) {
        sts = (statusT) error;
        goto err_closeall;
    }

    /*
     * Check the version number and page ID as minimal check on
     * whether this is a sensible bmt page.  There should be more
     * extensive checks here.
     */
    if ( pgp->bmtPageId != 0 ) {
        sts = E_INVALID_MSFS_STRUCT;
        goto err_closeall;
    }

    /*
     * check magic number for ALL kernel RAW_READs w advfs_raw_io
     */
    if (pgp->bmtODSVersion.odv_major > BFD_ODS_LAST_VERSION_MAJOR) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }
    *dmnVersion = pgp->bmtODSVersion;  
    /*
     * Find the vd attr record. It is now in the first RBMT mcell (page 0,
     * cell 0) to allow for easy access regardless of page size.
     */
    vdAttrp = bmtr_find(&pgp->bsMCA[BFM_RBMT], BSR_VD_ATTR, dmnP);
    if (vdAttrp == 0) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }

    if ( TEST_VDI_RANGE(vdAttrp->vdIndex) != EOK ) {
        sts = EBAD_VDI;
        goto err_closeall;
    }

    /*
     * Find the domain attr record. It is always linked into the first
     * extension mcell.
     */
    dmnAttrp = bmtr_find(&pgp->bsMCA[BFM_RBMT_EXT], BSR_DMN_ATTR, dmnP);
    if (dmnAttrp == 0) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }

    /*
     * Find the domain name record
     * NOTE: Older filesystems may not have a valid domain name record
     *       stored on disk so ignore those situations for now.
     */
    dmnNamep =  bmtr_find(&pgp->bsMCA[BFM_DMN_NAME], BSR_DMN_NAME, dmnP);
    

    /* Note that not all disks have log attributes */

    dmnMAttrp   = bmtr_find(&pgp->bsMCA[BFM_RBMT_EXT], BSR_DMN_MATTR,dmnP);
    *vnodepp    = vp;
    *vdAttrpp   = vdAttrp;
    *dmnAttrpp  = dmnAttrp;
    *dmnMAttrpp = dmnMAttrp;
    *dmnNamepp  = dmnNamep;
    return EOK;

err_freesblk:
    ms_free( superBlk );

err_closeall:
    (void) closed(dev, S_IFBLK, FREAD | FWRITE);

err_vrele:
    if (clu_is_ready() && !(isMount)) {
        CLU_DEV_DLM_UNLOCK(dev);
    }


err_nodlmlock:
    if (rvp)
        VN_RELE(rvp);

    if (vp)
        VN_RELE(vp);

    return sts;
}  /* end of get_raw_vd_attrs */

/* Global defaults for read/write maximum and preferred IO byte transfer size.
 * This is only used when the sizes are not available from the disk drivers.
 */
static uint64_t advfs_max_iosize_default = 1024 * DEV_BSIZE;
static uint64_t advfs_pref_iosize_default = 256 * DEV_BSIZE;

/*******************************/
/*
 * setup_vd
 *
 * Setup the in-memory structures for a vd.
 *
 * Notes:
 *
 *      1) This routine MUST NOT modify any on-disk structures since such
 *         modifications may be done only after the domain that contains
 *         the virtual disk has been recovered and activated.
 *
 *      Similarly, any state acquired from metadata files will likely change
 *      after recovery is run, so don't cache anything from those
 *      files here.
 *
 *      2) Assumes the DmnTbl lock is held.  The global buffer "Dpg"
 *      is assumed valid from the earlier call to get_raw_vd_attrs.
 *
 * Returns possible status from sc_add_vd,
 * or EBUSY, EBAD_VDI, ENO_MORE_DOMAINS, EDUP_VD, E_VD_DMNATTR_DIFF, or EOK
 */

static
statusT
setup_vd(
    domainT          *dmnP,        /* IN - domain pointer */
    struct vnode     *vnodep,      /* IN - ptr to vnode */
    char             *vdDiskName,  /* IN - name of disk */
    struct bsVdAttr  *vdAttrp,     /* IN - on disk vd attributes */
    uint64_t          flags,       /* IN - mount flags/
                                      check DmnActivationLock? */
    vdT             **vdPtr)       /* OUT - vd struct pointer */
{
    struct bfAccess  *metafileap;
    vdIndexT          vdIndex;
    struct vd        *vdp;
    statusT           sts;
    bfPageRefHT       bsPgRfH;
    bfPageRefHT       bmtPgRef;
    struct bsMPg     *bmtpgp;
    vdIoParamsT       vdIoParams;
    int32_t           result;
    int32_t           iosizeset = FALSE;
    uint64_t          min_iosize;
    disk_describe_type_ext_t disk_info;
    dev_t             char_dev; 
    sema_t            *save;        /* Required by DEVSW() */


    MS_SMP_ASSERT((flags & DMNA_GLOBAL_ROOT) ? 1 : 
            ADVFS_SMP_RW_LOCK_NEQL(&DmnActivationLock, RWL_UNLOCKED));

    /* Allocate a virtual disk memory structure and initialize its
     * locks and fields.  Ms_malloc() always zeros the memory.
     */
    vdp = (vdT *)ms_malloc(sizeof(vdT));

    ADVFTX_VDT_DELLIST_INIT(&vdp->del_list_lk, &dmnP->dmnMutex );
    ADVFTM_VDT_STGMAP_INIT(&vdp->stgMap_lk, &dmnP->dmnMutex);
    ADVFTM_VDT_MCELL_INIT(&vdp->mcell_lk, &dmnP->dmnMutex);
    ADVFTX_RBMT_MCELL_INIT(&vdp->rbmt_mcell_lk, &dmnP->dmnMutex);
    ADVRWL_DDLACTIVE_INIT(&vdp->ddlActiveLk);
    ADVMTX_VD_STATE_INIT(&vdp->vdStateLock);

    ss_init_vd(vdp);

    VD_TRACE(vdp, 0);

    vdp->ddlActiveWaitMCId = bsNilMCId;
    cv_init(&vdp->ddlActiveWaitCv, "ddlActivateWait CV", NULL, CV_WAITOK);
    vdp->vdState = (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) ?
                   BSR_VD_DMNT_INPROG : BSR_VD_VIRGIN;
    vdp->vdMagic = VDMAGIC;

    vdIndex      = vdAttrp->vdIndex;
    vdp->vdIndex = vdAttrp->vdIndex;
    vdp->vdCookie = vdAttrp->vdCookie; 
    MS_SMP_ASSERT(TEST_VDI_RANGE(vdIndex) == EOK);

    /*
     * Round vdSize so that it is an even multiple of 16 blocks.
     */
    vdp->vdSize       = (vdAttrp->vdBlkCnt & ~0x0f);
    vdp->devVp        = vnodep;
    vdp->rbmtp        = NULL;
    vdp->bmtp         = NULL;
    vdp->sbmp         = NULL;
    vdp->dmnP         = dmnP;
    vdp->bmtXtntPgs   = vdAttrp->bmtXtntPgs;
    vdp->serviceClass = vdAttrp->serviceClass;
    vdp->stgCluster   = vdAttrp->sbmBlksBit;
    vdp->vdClusters   = vdp->vdSize / vdp->stgCluster;
    strncpy( vdp->vdName, vdDiskName, BS_VD_NAME_SZ - 1 );
    vdp->vdSetupThd   = u.u_kthreadp;

    /* Assign default read/write IO transfer sizes to the vd.
     * The default values will likely change further down in this routine but
     * would be used if no sizes are available from the driver.
     * But this is necessary because the bfm_open_ms() path is going to use
     * these transfer sizes for metadata IO before setup_vd() has a 
     * a chance to retrieve the sizes from the disk.
     * The current_iosize_rd and current_iosize_wr vd fields represent the
     * current IO transfer byte sizes that customers can change and that
     * AdvFS uses for issuing IO.
     * Max_iosize represents the maximum IO transfer byte size
     * that the driver will handle. The preferred_iosize is the "preferred"
     * IO transfer byte size reported from the driver and represents the
     * initial transfer size that AdvFS should use.
     */
    vdp->current_iosize_rd = vdp->current_iosize_wr =
                                         advfs_pref_iosize_default;
    vdp->max_iosize =  advfs_max_iosize_default;
    vdp->preferred_iosize = advfs_pref_iosize_default;

    /* Can't move this down since the bfm_open_ms -> rbf_access_int ->
     * rbf_access_one_int -> tagdir_lookup() -> vd_htop_if_valid() path 
     * below needs the vdp in the vdpTbl[].  Note, however, that this 
     * vdp will not yet be found as a valid vdT struct by 
     * vd_htop_if_valid() for other threads since the vdState is not yet 
     * set to BSR_VD_MOUNTED.
     */
    dmnP->vdpTbl[vdIndex - 1] = vdp;
    dmnP->vdCnt++;

    /*
     * Open the RBMT, the BMT and the SBM.
     *
     * bfm_open_ms (eventually) calls bs_map_bf.
     *
     * At the time of this call, vdp->rbmtp and vdp->bmtp are NULL.  This 
     * will cause bs_map_bf to use the global page Dpg instead of 
     * attempting to use vdp->rbmtp / vdp->bmtp for I/O.
     */

    sts = bfm_open_ms(&metafileap, dmnP, vdIndex, BFM_RBMT);
    if (sts != EOK) {
        goto err_freevd;
    }
    vdp->rbmtp = metafileap;

    sts = bfm_open_ms(&metafileap, dmnP, vdIndex, BFM_BMT);
    if (sts != EOK) {
        goto err_close;
    }
    vdp->bmtp = metafileap;

    sts = bfm_open_ms(&metafileap, dmnP, vdIndex, BFM_SBM);
    if (sts != EOK) {
        goto err_close;
    }
    vdp->sbmp = metafileap;

    /* Get the disk I/O params. If they exist on disk, then use those
     * IO parameter values.
     */
    sts = bmtr_get_rec(vdp->rbmtp,
                       BSR_VD_IO_PARAMS,
                       &vdIoParams,
                       sizeof(vdIoParamsT));
    if (sts == EOK) {
        /* Use the IO transfer size parameters from disk. Convert the values
         * stored on disk from device blocks to bytes.
         */
        vdp->current_iosize_rd = vdIoParams.rdMaxIo * DEV_BSIZE;
        vdp->current_iosize_wr = vdIoParams.wrMaxIo * DEV_BSIZE;
        iosizeset = TRUE;
    }
   
    /* Get the disk driver's preferred and maximum IO transfer byte sizes
     * and assign them to the virtual disk.
     * Be prepared to use the default IO sizes set above when devices
     * can't respond or devices respond with zero transfer sizes.
     */
    bzero(&disk_info, sizeof(disk_describe_type_ext_t));

    /*
     * The device has been opened via the block dev_t.  We need to
     * have it open via the char dev_t as well to avoid a panic.
     */

    char_dev = block_to_raw(vdp->devVp->v_rdev);

    result = opend(&char_dev, S_IFCHR, FREAD, 0);

    if (result == 0) {
        DEVSW(cdevsw, major(char_dev), d_ioctl,
            (char_dev, DIOC_DESCRIBE_EXT, (caddr_t)&disk_info, FREAD | FWRITE),
             result, save);
        (void) closed(char_dev, S_IFCHR, FREAD);
    }

    if ((result == 0) && disk_info.pref_io_size && disk_info.max_io_size){
        vdp->max_iosize = disk_info.max_io_size;
        vdp->preferred_iosize = disk_info.pref_io_size;
    }

    /* If the IO transfer sizes were not retrieved from the AdvFS virtual
     * disk attributes from disk, then initialize the current read and write
     * IO transfer size using the preferred IO size that the disk driver
     * reported.
     * This assignment also happens when the domain's virtual disk is
     * first created.
     */
    if (iosizeset == FALSE) {
        vdp->current_iosize_rd = vdp->current_iosize_wr =
                                              vdp->preferred_iosize;
    }

    /* Check maximum IO transfer byte size setting.
     * Adjust the current read/write IO transfer sizes lower if they are
     * larger than device maximum.
     */
    if (vdp->current_iosize_rd > vdp->max_iosize)
        vdp->current_iosize_rd = vdp->max_iosize;
    if (vdp->current_iosize_wr > vdp->max_iosize)
        vdp->current_iosize_wr = vdp->max_iosize;

    /* Check minimum IO transfer byte size setting.
     * Adjust the current read/write IO transfer sizes higher if they are
     * smaller than AdvFS tolerates for this volume. The minimum size is
     * the larger of either the domain's user data or meta data
     * allocation unit size in terms of bytes.
     */
    min_iosize = (MAX(dmnP->userAllocSz, dmnP->metaAllocSz) /
                  ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE;
    if (vdp->current_iosize_rd < min_iosize)
        vdp->current_iosize_rd = min_iosize;
    if (vdp->current_iosize_wr < min_iosize)
        vdp->current_iosize_wr = min_iosize;

    *vdPtr = vdp;

    return EOK;

err_close:
    if (vdp->rbmtp)
        bs_close(vdp->rbmtp, MSFS_CLOSE_NONE);

    if (vdp->bmtp)
        bs_close(vdp->bmtp, MSFS_CLOSE_NONE);

    if (vdp->sbmp)
        bs_close(vdp->sbmp, MSFS_CLOSE_NONE);

err_freevd:
   
    dmnP->vdpTbl[vdIndex - 1] = NULL;
    dmnP->vdCnt--;
    vd_free(vdp, dmnP);

    return sts;

}  /* end of setup_vd */

/*******************************/
/*
 * advfs_dev_mounted
 * 
 * Checks to see if dev_t passed in is already part of an activated domain.
 *
 * Possible callers
 *
 *  advfs_volume_mounted()
 *  devtovfs()
 *
 * Return Values:
 *      0  Not part of an activated domain.
 *      1  Part of an activated domain.
 */

static int 
advfs_dev_mounted(
        dev_t dev
        )
{
    struct domain *dmnP;
    int vdi, vdcnt;
    vdT *vddp;

    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock ); 
    if (DmnSentinelP == NULL) {
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        return 0;
    }

    dmnP = DmnSentinelP;
    do {
        vdi   = 1;
        vdcnt = 0;
        while ((vdi <= BS_MAX_VDI) && (vdcnt < dmnP->vdCnt)) {
            if ((vddp = vd_htop_if_valid(vdi, dmnP, TRUE, TRUE)) != NULL) {
                if (vddp->devVp->v_rdev == dev) {
                    vd_dec_refcnt(vddp);
                    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
                    return 1;
                }
                vdcnt++;
                vd_dec_refcnt(vddp);
            }
            vdi++;
        }
        dmnP = dmnP->dmnFwd;
    } while (dmnP != DmnSentinelP);
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    return 0;
}

/*******************************/
/*
 * advfs_volume_mounted
 * 
 * Checks to see if volume path passed in is already part of an activated 
 * domain.
 *
 * Possible callers
 *
 *  bs_dmn_init()
 *  bs_vd_add_active()
 *
 * Return Values:
 *      0  Not part of an activated domain.
 *      1  Part of an activated domain.
 */

int
advfs_volume_mounted(
        char *volume_name
        )
{
    dev_t dev;
    struct vnode *vp;

    if(advfs_getvp(&vp, volume_name, UIOSEG_KERNEL, FOLLOW_LINK)) {
            return 0;
    }
    dev = vp->v_rdev;
    VN_RELE(vp);
    return advfs_dev_mounted(dev);
}


/*
 * Add a new virtual disk to an active domain.
 */

statusT
bs_vd_add_active(
    char *domainName,           /* in - bitfile domain name */
    char *vdName,               /* in - block special device name */
    serviceClassT *vdSvc,       /* in/out - service class */
    bf_vd_blk_t vdSize,         /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - num fobs to preallocate for the BMT */
    uint32_t flags,             /* in - M_MSFS_MOUNT may be set */
    bfDomainIdT *dmnId,         /* out - domain Id */
    vdIndexT *volIndex          /* out - volume index */
    )
{
    statusT sts = 0;
    bfDomainIdT domainId;
    domainT *dmnP;
    struct bsVdAttr* vdAttrp;
    bsDmnAttrT* dmnAttrp;
    int i;
    bfDmnParamsT *bfDmnParamsp = NULL;
    bsVdParamsT *bsVdParamsp = NULL;
    bsDmnMAttrT *dmnMAttrp;
    bsDmnNameT  *dmnNamep;
    int sz;
    serviceClassT tagSvc, logSvc;
    vdIndexT vdi;
    vdT *vdp;

    struct vnode *vnodep = NULL;
    int error;
    adv_ondisk_version_t dmnVersion;

    bfDmnParamsp = (bfDmnParamsT *) ms_malloc_no_bzero(sizeof( bfDmnParamsT ));
    bsVdParamsp = (bsVdParamsT *) ms_malloc( sizeof( bsVdParamsT ));

    /* Activate the domain. */
    if ((sts = bs_bfdmn_tbl_activate(domainName, flags, &domainId)) != EOK) {
        ms_free( bsVdParamsp );
        ms_free( bfDmnParamsp );
        return (sts);
    }

    /* Open the domain. */
    if ((sts = bs_domain_access(&dmnP, domainId, FALSE)) != EOK) {
        goto err_deactivate;
    }

    /*
     * This lock must be held across the calls to get_raw_vd_attrs and
     * setup_vd because the global buffer "Dpg" is used by both.
     */
    ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );

    /*
     * Now that the domain is opened and accessed, some parameters
     * (for example, uid) have to be read in from disk.  Maybe all the
     * parameters should be kept in the in core domain structure.
     */
    if ((sts = bs_get_dmn_params(dmnP, bfDmnParamsp, 0)) != EOK) {
        goto exit;
    }

    *dmnId = domainId;

    if (!bs_owner(bfDmnParamsp->uid)) {
        sts = E_NOT_OWNER;
        goto exit;
    }

    if (!bs_accessible(BS_ACC_WRITE,
                       bfDmnParamsp->mode,
                       bfDmnParamsp->uid,
                       bfDmnParamsp->gid)) {
        sts = E_ACCESS_DENIED;
        goto exit;
    }

    if ((bfDmnParamsp->maxVds == bfDmnParamsp->curNumVds) ||
        (bfDmnParamsp->curNumVds == BS_MAX_VDI)) {
        sts = E_TOO_MANY_VIRTUAL_DISKS;
        goto exit;
    }

    if (SC_EQL(*vdSvc, nilServiceClass)) {
        *vdSvc = defServiceClass;
    }

    /* Assign virtual disk index. */
    vdi = vd_alloc_index(dmnP);
    if ( vdi == 0 ) {
        /* The domain already contains the maximum virtual disks. */
        sts = E_TOO_MANY_VIRTUAL_DISKS;
        goto exit;
    }

    *volIndex = vdi;

    /* Fill in vdParamsp. */
    strncpy(bsVdParamsp->vdName, vdName, BS_VD_NAME_SZ - 1);
    bsVdParamsp->serviceClass = *vdSvc;
    bsVdParamsp->vdSize       = vdSize;
    bsVdParamsp->bmtXtntPgs   = roundup(bmtXtntFobs, 
                                        ADVFS_METADATA_PGSZ_IN_FOBS) /
                                    ADVFS_METADATA_PGSZ_IN_FOBS;
    bsVdParamsp->vdIndex      = vdi;
/* 
 * The following initialization of userAllocSz and metaAllocSz allows
 * different devices to support different allocation sizes.
 *
 * For now we only support domain-wide userdata page sizes, so we use the
 * value in the domainT structure.
 */
    bsVdParamsp->userAllocSz = dmnP->userAllocSz;
    bsVdParamsp->metaAllocSz  = ADVFS_METADATA_PGSZ_IN_FOBS;


    /*
     * Initialize the on-disk structures of the new virtual disk.  The
     * tag service class and log service class are nil because this
     * disk will not contain the tag or log mcells.
     */

    if (advfs_volume_mounted(vdName)) {
        sts = EDUP_VD;
        goto exit;
    }

    if ((sts = bs_disk_init(vdName, bfDmnParamsp, bsVdParamsp,
        nilServiceClass, nilServiceClass, bmtPreallocFobs,
        dmnP->dmnVersion)) != EOK) {
        goto exit;
    }

    sts = get_raw_vd_attrs(vdName,
                           dmnP,
                           vdi,
                           flags,
                           &vnodep,
                           &vdAttrp,
                           &dmnAttrp,
                           &dmnMAttrp,
                           &dmnNamep,
                           &dmnVersion,
                           dmnP->metaPagep);
    if ( sts != EOK ) {
        goto exit;
    }
    sts = setup_vd(
                   dmnP,
                   vnodep,
                   vdName,
                   vdAttrp,
                   0,
                   &vdp );
    if ( sts != EOK ) {
        goto exit;
    }

    sts = set_vd_mounted( dmnP, vdp );
    if (sts != EOK) {
        goto exit;
    }

    sts = set_disk_attrs (dmnP, BSR_VD_ADDVOL, vdp->devVp->v_rdev);
    /* Fall thru to "exit". */

    /*
     * FIX - if set_disk_attrs() does not return success, how do we remove
     * the effects of set_vd_mounted()? The error paths in this function
     * the function that it calls and the functions that call it must
     * be checked and cleaned up.
     */

exit:
    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    bs_domain_close(dmnP);

err_deactivate:
    (void) bs_bfdmn_deactivate(domainId, flags);

    ms_free( bsVdParamsp );
    ms_free( bfDmnParamsp );

    return sts;
}


static
void
vd_free(
    vdT *vdp,
    domainT *dmnP)
{
    int i;

    MS_SMP_ASSERT(vdp->vdMagic == VDMAGIC);

    ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
    MS_SMP_ASSERT( vdp->vdState == BSR_VD_ZOMBIE || 
                   vdp->vdState == BSR_VD_VIRGIN ||     /* error path state */
                   vdp->vdState == BSR_VD_DMNT_INPROG );/* error path state */
    MS_SMP_ASSERT( vdp->vdRefCnt == 0 );
    MS_SMP_ASSERT( vdp->vdRefWaiters == 0 );

    ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );

    ss_dealloc_vd(vdp);

    ADVMTX_VD_STATE_DESTROY( &vdp->vdStateLock );

    ADVFTM_VDT_STGMAP_DESTROY( &vdp->stgMap_lk);
    ADVFTM_VDT_MCELL_DESTROY( &vdp->mcell_lk.lock.mtx);
    ADVFTX_RBMT_MCELL_DESTROY( &vdp->rbmt_mcell_lk.lock.rw);
    ADVFTX_VDT_DELLIST_DESTROY( &vdp->del_list_lk.lock.rw);
    ADVRWL_DDLACTIVE_DESTROY( &vdp->ddlActiveLk );

    cv_destroy(&vdp->ddlActiveWaitCv);

    vdp->vdMagic |= MAGIC_DEALLOC;
    ms_free( vdp );
}

/*
 * bs_vd_remove_active
 *
 * This function removes a virtual disk from the specified active domain.
 *
 * Before removing the disk, this function determines if this disk contains
 * the log or the root tag directory.  If one or both are on this disk, it
 * moves them to another disk in the domain.
 *
 * The caller must ensure that no bitfile metadata is on this disk, exclusive
 * of the reserved bitfiles.  If this is not true, this function returns an
 * error.
 */

statusT
bs_vd_remove_active (
                     domainT *dmnP,      /* in */
                     vdIndexT vdIndex,   /* in */
                     uint32_t forceFlag  /* in */
                     )
{
    bsBfDescT bfDesc;
    int bfDescCnt;
    bf_vd_blk_t blksPerCluster;
    int foundFlag;
    bf_vd_blk_t freeClusters;
    bfMCIdT nextBfDescId;
    statusT sts;
    bf_vd_blk_t totalClusters;
    vdT *vdp;
    int dmntbl_lock = 0;
    bfTagT bfTag;
    int ss_was_activated = FALSE;
    int oldState;

    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));
    if (!(dmnP->dmnFlag & BFD_RMVOL_IN_PROGRESS)){
        dmnP->dmnFlag |= BFD_RMVOL_IN_PROGRESS;
        ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));
    } else {
        ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));
        return E_RMVOL_ALREADY_INPROG;
    }

    /* Since we are the only thread removing disks in this domain, there
     * is no need to bump the vdRefCnt in the call to vd_htop_if_valid().
     */
    vdp = vd_htop_if_valid(vdIndex, dmnP, FALSE, FALSE);
    if (!vdp) {
        RAISE_EXCEPTION ( EBAD_VDI );
    }

    scan_rsvd_file_xtnt_map (dmnP->bfSetDirAccp, vdIndex, &foundFlag);
    if ((vdIndex == BS_BFTAG_VDI (dmnP->bfSetDirTag)) || (foundFlag != 0)) {
        /*
         * Can't remove the disk because the root tag directory is still here.
         */
        RAISE_EXCEPTION (E_ROOT_TAGDIR_ON_VOL);
    }

    scan_rsvd_file_xtnt_map (dmnP->logAccessp, vdIndex, &foundFlag);
    if ((vdIndex == BS_BFTAG_VDI (dmnP->ftxLogTag)) || (foundFlag != 0)) {
        /*
         * Can't remove the disk because the log is still here.
         */
        RAISE_EXCEPTION (E_LOG_ON_VOL);
    }

    /*
     * Tru64 used to call bs_reclaim_cfs_rsvd_vn on each rsvd file on the
     * vd.  This shouldn't be necessary since CFS will now release the PFS
     * vnode on last close.
     */

    if(dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        /* deactivate/drain vfast operations */
        (void)advfs_ss_change_state(dmnP->domainName,
				    SS_SUSPEND, &oldState, 0);
        ss_was_activated = TRUE;
    }

    ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );
    dmntbl_lock = 1;
    /* coordinate with truncation */
    ADVRWL_RMVOL_TRUNC_WRITE(dmnP);

    /*
     * Move everything out of the bmt.
     */
    sts = clear_bmt (dmnP, vdIndex, forceFlag, 0, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Scan the bmt, looking for an mcell that belongs to a regular
     * (non-reserved) bitfile.  If an mcell is found, can't remove this disk.
     */
    nextBfDescId = bsNilMCId;
    sts = bmt_get_vd_bf_descs (
                               dmnP,
                               vdIndex,
                               1,  /* bfDesc array size */
                               &bfDesc,
                               &nextBfDescId,
                               &bfDescCnt
                               );
    if (sts == EOK) {
        if (bfDescCnt != 0) {
            /*
             * There is at least one mcell that belongs to a regular bitfile.
             * So, can't remove the disk.
             */
            RAISE_EXCEPTION (E_BMT_NOT_EMPTY);
        }
    } else {
        RAISE_EXCEPTION (sts);
    }

    /* Checkpoint log now so there are no undo or redo log records for
     * the bitfiles on this disk.  This prevents a recovery error if we
     * crash between this point and vd_remove().
     */
    advfs_lgr_checkpoint_log (dmnP);

    sts = set_disk_attrs (dmnP, BSR_VD_RMVOL, vdp->devVp->v_rdev);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    blksPerCluster = vdp->stgCluster;
    totalClusters = vdp->vdClusters;
    freeClusters = vdp->freeClust;

    vd_remove(dmnP, vdp, nilBfDomainId,
            ADVMTX_VD_STATE_CHANGE | VD_REMOVE_PERM);

    ss_rmvol_from_hotlst(dmnP,vdIndex);

    ADVRWL_RMVOL_TRUNC_UNLOCK(dmnP);
    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    dmntbl_lock = 0;

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        ss_rmvol_from_hotlst(dmnP,vdIndex);
        (void)advfs_ss_change_state(dmnP->domainName,
				    SS_ACTIVATED, &oldState, 0);
        ss_was_activated = FALSE;
    }

    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));

    /*
     * Subtract this vd's free blocks from the domain totals.
     */
    UINT64T_SUB (dmnP->totalBlks, totalClusters * blksPerCluster);
    UINT64T_SUB (dmnP->freeBlks, freeClusters * blksPerCluster);

    dmnP->dmnFlag &= ~BFD_RMVOL_IN_PROGRESS;

    ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));

    return sts;

HANDLE_EXCEPTION:

    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));
    dmnP->dmnFlag &= ~BFD_RMVOL_IN_PROGRESS;
    ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));

    if (dmntbl_lock == 1) {
        ADVRWL_RMVOL_TRUNC_UNLOCK(dmnP);
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        dmntbl_lock = 0;
    }

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        (void)advfs_ss_change_state(dmnP->domainName,
				    SS_ACTIVATED, &oldState, 0);
    }

    return sts;

}  /* end bs_vd_remove_active */

/*
 * bs_vd_add_rem_vol_done
 *
 * This function "commits" a prior addvol/rmvol. It looks for the
 * domain's transient record and removes the rdev corresponding to the
 * specified volume. 
 *
 * This function is called after the links have been updated in
 * /dev/advfs.
 */

statusT
bs_vd_add_rem_vol_done (
                        domainT *dmnP,  /* in */
                        char *volName,  /* in */
                        bsVdOpT op      /* in */
                        )
{
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    struct bfAccess *mdap;
    bsMPgT *bmtPage;
    bsDmnTAttrT *dmnTAttr;
    bfPageRefHT pgPin;
    struct pathname path_name;  /* Pathname for volume */
    struct vnode *vp = NULL;    /* vnode for volume */
    struct vnode *dvp = NULL;   /* parent vnode for volume */

    ADVRWL_DMNACTIVATION_WRITE( &DmnActivationLock );

    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    mdap = vd->rbmtp;
    sts = bs_pinpg (&pgPin, 
                   (void*)&bmtPage, 
                   mdap, 
                   0, 
                   FtxNilFtxH, 
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_CLEAN); /* Ignore status. */
        ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
        return EBMTR_NOT_FOUND;
    }

    /*
     * Check to see if dmnTAttr has a dev in it. If not, it got
     * wiped out during a domain activation - nothing to do here.
     */

    if (dmnTAttr->dev) {
        sts = pn_get( volName, UIOSEG_KERNEL, &path_name );
        if (sts) {
            bs_unpinpg (pgPin, logNilRecord, BS_CLEAN); /* Ignore status. */
            ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
            return sts;
        }

        sts = lookuppn( &path_name, FOLLOW_LINK, &dvp, &vp );

        if ( sts ) {
            bs_unpinpg (pgPin, logNilRecord, BS_CLEAN); /* Ignore status. */
            ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
            return sts;
        }
        /*
         * Now our dvp and vp both have elevated v_count's from lookuppn
         */

        /*
         * For now, there is only one rmvol/addvol record.
         */
        if ( (dmnTAttr->dev != vp->v_rdev) || (dmnTAttr->op != op) ) {
            VN_RELE(vp);
            VN_RELE(dvp);
            bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
            ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
            return E_RDEV_MISMATCH;
        }

        dmnTAttr->dev = 0;
        dmnTAttr->op = BSR_VD_NO_OP;

        sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
        VN_RELE(vp);
        VN_RELE(dvp);

    } else {
        sts = bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
    }

    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );


    return sts;
}

/*
 * scan_rsvd_file_xtnt_map
 *
 * This function scans the reserved file's exent map for an extent
 * that is located on the specified disk.  The scan stops if an extent
 * is found on the disk.
 */

static
void
scan_rsvd_file_xtnt_map (
                         bfAccessT *bfAccess,  /* in */
                         vdIndexT vdIndex,  /* in */
                         int *retFoundFlag  /* out */
                         )
{
    int foundFlag = 0;
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;

    ADVRWL_XTNTMAP_READ(&(bfAccess->xtntMap_lk));

    /*
     * FIX - This function assumes that reserved files are not shadowed.  This
     * will change.
     */

    imm_get_first_xtnt_desc (bfAccess->xtnts.xtntMap, &xtntDescId, &xtntDesc);

    while (xtntDesc.bsxdFobCnt > 0) {

        if (xtntDesc.volIndex == vdIndex) {
            foundFlag = 1;
            break;  /* out of while */
        }

        imm_get_next_xtnt_desc(bfAccess->xtnts.xtntMap, &xtntDescId, &xtntDesc);

    }  /* end while */

    ADVRWL_XTNTMAP_UNLOCK(&(bfAccess->xtntMap_lk));

    *retFoundFlag = foundFlag;

    return;

}  /* end scan_rsvd_file_xtnt_map */

/*
 * clear_bmt
 *
 * This function scans the specified disk's bmt.  If it finds any in-use mcells,
 * it tries to move their contents to another disk in the service class.
 *
 * FIX - can there be multiple ddl entries for one bitfile?  Specifically, one
 * or more truncate entries?  Or, one or more truncate entries and a delete
 * entry?  This function assumes one entry per bitfile.
 */

static
statusT
clear_bmt (
           domainT *dmnP,         /* in */
           vdIndexT vdIndex,      /* in */
           uint32_t forceFlag,    /* in */
           bf_vd_blk_t startBlk,  /* in */
           bf_vd_blk_t numBlks    /* in */
           )
{
    bfAccessT *bfap;
    bfSetT *bfSetp;
    bfSetIdT bfSetId;
    bfTagT bfSetTag;
    bfTagT bfTag;
    bmtHT bmtH;
    int closeBfSetFlag = 0;
    int closeBitfileFlag = 0;
    int closeBmtFlag = 0;
    vdT *ddlVd;
    int delFlag;
    bfMCIdT mcellId;
    metadataTypeT metadataType;
    int migFlag;
    bf_fob_t fobCnt;
    statusT sts;
    int unlockFlag = 0;
    enum acc_open_flags open_options;
    uint32_t do_vrele;
    struct vnode *nullvp;
    struct vfs *vfsp;

    bfSetId.domainId = dmnP->domainId;

    bmtH.curPg = 0;
    bmtH.curMcell = 0;

    sts = bmt_open (&bmtH, dmnP, vdIndex);
    if (sts != EOK) {
        if (sts == E_RANGE_NOT_MAPPED) {
            /*
             * The bmt has no pages.
             */
            sts = EOK;
        }
        RAISE_EXCEPTION (sts);
    }
    closeBmtFlag = 1;

    sts = bmt_read (&bmtH, &bfSetTag, &bfTag, &metadataType);
    while (sts == EOK) {

        bfSetId.dirTag = bfSetTag;
        open_options = BF_OP_IGNORE_DEL | BF_OP_INTERNAL;
        do_vrele=0;

        sts = bfs_open(&bfSetp, bfSetId, BFS_OP_IGNORE_DEL, FtxNilFtxH);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        closeBfSetFlag = 1;

        /*
         * Access the bitfile, regardless of its "delete" status
         */
        nullvp = NULL;
	vfsp = ADVGETVFS(bfSetp);
        if ((vfsp) && (vfsp->vfs_flag & VFS_CFSONTOP)) {
            /* 
             * If we are in a cluster we need to make this appear like
             * an external open (ie we need to get a vnode). Clusters
             * requires the vnode in order to synchronize with clients
             * that may have this files xtnt map cached. The migrate call
             * will revoke the CFS token but rquires a vnode to do this.
             * Since metadata files can not have cached extents don't
             * bother getting a vnode for these.
             */

            if (BS_BFTAG_REG(bfSetp->dirTag) &&
                (bfTag.tag_num > GROUP_QUOTA_FILE_TAG)) {
                
                /* All filesets must be mounted */
                MS_SMP_ASSERT(bfSetp->fsnp);

                open_options &= ~BF_OP_INTERNAL;     /* Make it look external, will put a v_count on vnode */
                do_vrele=1;
            }
        }
        
        sts = bs_access( &bfap, 
                         bfTag, 
                         bfSetp, 
                         FtxNilFtxH,
                         open_options,
                         &nullvp);
        if (sts == EOK) {
            closeBitfileFlag = 1;
            /*
             * We have the bitfile accessed.  The bitfile's storage can't be
             * truncated or deleted because these are done as part of the last
             * close and our access prevents the last close.  However, this
             * mcell may be associated with a previous truncate.  Also, the
             * bitfile can be migrated or be in the process of migrating.
             */

            if ((metadataType == BMT_NORMAL_METADATA) ||
                (metadataType == BMT_PRIME_MCELL_XTNT_METADATA)) {

                /*
                 * The mcell contains normal (non-extent) metadata.  It may
                 * or may not be on the disk's deferred-delete list.  We don't
                 * care, just move the metadata to another disk.
                 *
                 * If the mcell is on a list, it was put there by delete.
                 * Truncate has the metadata type of BMT_XTNT_METADATA.
                 */
                /*
                 * If bs_move_metadata() finds extent information in the
                 * primary mcell, it will return
                 *   E_STG_ADDED_DURING_MIGRATE_OPER
                 * in which case, we'll migrate the extents first and then
                 * move the metadata (see below).
                 * The migrated file will NOT have extent information in
                 * the primary mcell because we do it in two steps: first
                 * move the extents and then move the primary mcell
                 * (this should perhaps be fixed).
                 *
                 * In this case, it may well happen that the extent mcell
                 * ends up BEFORE the primary mcell on the disk. If we go
                 * through clear_bmt() again, we'll hit the extent mcell first:
                 * we'll skip this branch of the if and drop to the final else,
                 * which will check the DDL and, assuming that the file has
                 * NOT been deleted, will return E_NO_MORE_MCELLS
                 * will cause migFlag to be set to true and the second
                 * mig_migrate() to be called. Now the extents are migrated
                 * and we continue the scan until we hit the primary mcell.
                 * We will then come through this branch,
                 * bs_move_metadata() will
                 * move the primary mcell (and the rest of the mcell chain)
                 * and return EOK, which will cause mig_migrate() to be
                 * called AGAIN. Since it will not find any extents on this
                 * disk, it will now (12/14/2000 - smartstore change)
                 * return E_RANGE_NOT_MAPPED. That's why
                 * we need to ignore this error (stricly speaking, only
                 * after the second mig_migrate() call, but I don't think
                 * it is harmful to ignore it after the first call as well).
                 */
                sts = bs_move_metadata (bfap, NULL);
                if (sts == EOK) {
                    migFlag = 1;
                } else if (sts == E_STG_ADDED_DURING_MIGRATE_OPER) {
                    /*
                     * This is a file containing extent information in the
                     * primary mcell.  We must migrate the extents first
                     * and then move the metadata.   Migrate the extents to
                     * another volume if startBlk == 0, else send it to the
                     * same disk.
                     */
                    fobCnt= bfap->bfaNextFob;
                    if (fobCnt > 0) {
                        sts = mig_migrate (
                                           bfap,
                                           vdIndex,
                                           0,  /* fobOffset */
                                           fobCnt,
                                           startBlk ? vdIndex : -1,
                                           -1,  /* any disk block */
                                           forceFlag,
                                           BS_ALLOC_DEFAULT
                                           );
                        /*
                         * mig_migrate will return E_RANGE_NOT_MAPPED if
                         * there are no extents of the file on the given
                         * disk. This may happen because bmt_read()
                         * finds an XTRA_XTNTS mcell, migrates the extents
                         * and then finds the primary mcell and tries to
                         * migrate the extents again.
                         */
                        if (sts != EOK && sts != E_RANGE_NOT_MAPPED) {
                            RAISE_EXCEPTION (sts);
                        }
                    }
                    sts = bs_move_metadata (bfap,NULL);
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                    migFlag = 0;
                }
                else if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
                migFlag = 1;
            } else {
                /*
                 * Search all the deferred-delete lists.  If an entry is found,
                 * the mcell is part of a delete or a previous truncate.
                 *
                 * NOTE:  If successful, find_del_entry() returns with the
                 * disk's deferred-delete list active locked (exclusive).
                 */
                sts = find_del_entry (
                                      dmnP,
                                      bfSetTag,
                                      bfTag,
                                      &ddlVd,
                                      &mcellId,
                                      &delFlag
                                      );
                if (sts == EOK) {
                    unlockFlag = 1;
                    if (delFlag != 0) {
                        /*
                         * This mcell is part of a delete and is linked to a
                         * deferred-delete list.
                         */
                        unlockFlag = 0;
                        ADVRWL_DDLACTIVE_UNLOCK( &(ddlVd->ddlActiveLk) );
                        migFlag = 1;
                    } else {
                        /*
                         * This mcell is part of a previous truncate.  This
                         * function acquired the deferred-delete list active
                         * lock in the window after the entry was inserted onto
                         * the list and before truncate had a chance to process
                         * the entry.
                         *
                         * NOTE: wait_for_ddl_active_entry() releases the active
                         * lock.
                         */
                        sts = wait_for_ddl_active_entry(dmnP, ddlVd, mcellId);
                        if (sts != EOK) {
                            RAISE_EXCEPTION (sts);
                        }
                        unlockFlag = 0;
                        migFlag = 0;
                    }
                } else {
                    if (sts == ENO_SUCH_TAG) {
                        /*
                         * The mcell contains extent information and is not on
                         * or linked to any deferred-delete list.
                         */
                        migFlag = 1;
                    } else {
                        RAISE_EXCEPTION (sts);
                    }
                }
            }

            if (migFlag != 0) {
                /*
                 * Move all of the bitfile's storage on this disk to another
                 * disk in the service class.
                 *
                 * NOTE:  We assume that the bitfile has storage on this
                 * disk.  After the migrate, we will know for sure that it
                 * doesn't have storage on this disk.
                 */
                /*
                 * NOTE: Assume bitfile starts at offset 0.
                 */
                fobCnt = bfap->bfaNextFob;
                if (fobCnt > 0) {
                    sts = mig_migrate (
                                       bfap,
                                       vdIndex,
                                       0,  /* fobOffset */
                                       fobCnt,
                                       startBlk ? vdIndex : -1,
                                       -1,  /* any disk block */
                                       forceFlag,
                                       BS_ALLOC_DEFAULT
                                       );
                    /*
                     * mig_migrate will return E_RANGE_NOT_MAPPED if
                     * there are no extents of the file on the given
                     * disk. This may happen because bmt_read()
                     * finds an XTRA_XTNTS mcell, migrates the extents
                     * and then finds the primary mcell and tries to
                     * migrate the extents again.
                     */
                    if (sts != EOK && sts != E_RANGE_NOT_MAPPED) {
                        RAISE_EXCEPTION (sts);
                    }
                }
            }

            closeBitfileFlag = 0;
            if (do_vrele)
            {
                VN_RELE(&bfap->bfVnode);
            }
            else
            {
                sts = bs_close(bfap,MSFS_CLOSE_NONE);
            }

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            closeBfSetFlag = 0;
            sts = bs_bfs_close (bfSetp, FtxNilFtxH, BFS_OP_DEF);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

        } else {
            /*
             * We can't access the bitfile.
             */
            closeBfSetFlag = 0;
            bs_bfs_close (bfSetp, FtxNilFtxH, BFS_OP_DEF);

            if (sts == ENO_SUCH_TAG) {
                /*
                 * This mcell is either on or is linked onto a deferred-delete
                 * list.  It was put there by delete.  This function
                 * acquired the deferred-delete list active lock in the window
                 * after the last close happened and before close had a chance
                 * to process the entry.
                 */
                if ((metadataType == BMT_NORMAL_METADATA) ||
                    (metadataType == BMT_PRIME_MCELL_XTNT_METADATA)) {

                    /*
                     * The mcell contains normal metadata and should be
                     * on this disk's deferred-delete list.
                     */

                    ddlVd = VD_HTOP(vdIndex, dmnP);

                    ADVRWL_DDLACTIVE_WRITE( &(ddlVd->ddlActiveLk) );
                    unlockFlag = 1;

                    sts = del_find_del_entry (
                                              dmnP,
                                              ddlVd->vdIndex,
                                              bfSetTag,
                                              bfTag,
                                              &mcellId,
                                              &delFlag
                                              );
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                } else {
                    /*
                     * The mcell contains extent metadata and is linked onto
                     * a deferred-delete list.
                     *
                     * NOTE:  If successful, find_del_entry() returns with the
                     * disk's deferred-delete list active locked (exclusive).
                     */
                    sts = find_del_entry (
                                          dmnP,
                                          bfSetTag,
                                          bfTag,
                                          &ddlVd,
                                          &mcellId,
                                          &delFlag
                                          );
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                    unlockFlag = 1;
                }
                /*
                 * NOTE: wait_for_ddl_active_entry() releases the active
                 * lock.
                 */
                sts = wait_for_ddl_active_entry (dmnP, ddlVd, mcellId);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
                unlockFlag = 0;
            } else {
                RAISE_EXCEPTION (sts);
            }
        }

        sts = bmt_read (&bmtH, &bfSetTag, &bfTag, &metadataType);

    }  /* end while */

    if (sts != E_RANGE_NOT_MAPPED) {
        RAISE_EXCEPTION (sts);
    }

    closeBmtFlag = 0;
    sts = bmt_close (&bmtH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    return EOK;

HANDLE_EXCEPTION:
    if (unlockFlag != 0) {
        ADVRWL_DDLACTIVE_UNLOCK( &(ddlVd->ddlActiveLk) );
    }

    if (closeBitfileFlag != 0) {
        if (do_vrele)
        {
            VN_RELE(&bfap->bfVnode);
        }
        else
        {
            bs_close(bfap,MSFS_CLOSE_NONE);
        }
    }

    if (closeBfSetFlag != 0) {
        bs_bfs_close (bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (closeBmtFlag != 0) {
        bmt_close (&bmtH);
    }

    return sts;
}  /* end clear_bmt */

/*
 * find_del_entry
 *
 * This function searches every deferred-delete list until it finds the first
 * entry that matches the specified bitfile set tag and bitfile tag.  It returns
 * with the disk's deferred-delete list active lock held (exclusive).
 */

statusT
find_del_entry (
                domainT *dmnP,  /* in */
                bfTagT bfSetTag,  /* in */
                bfTagT bfTag,  /* in */
                vdT **delVd,  /* out */
                bfMCIdT *delMcellId,  /* out */
                int *delFlag  /* out */
                )
{
    int dFlag;
    bfMCIdT mcellId;
    statusT sts;
    vdT *vd = NULL;
    int vdCnt;
    vdIndexT vdIndex;

    vdIndex = 1;
    vdCnt = 0;

    while ( (vdIndex <= BS_MAX_VDI) && (vdCnt < dmnP->vdCnt) ) {

        if ( !(vd = vd_htop_if_valid(vdIndex, dmnP, FALSE, FALSE)) ) {
            vdIndex++;
            continue;
        }

        ADVRWL_DDLACTIVE_WRITE( &(vd->ddlActiveLk) );

        sts = del_find_del_entry( dmnP,
                                  vd->vdIndex,
                                  bfSetTag,
                                  bfTag,
                                  &mcellId,
                                  &dFlag);
        if ( sts == EOK ) {
            break;  /* out of while */
        } else {
            if ( sts != ENO_MORE_MCELLS ) {
                return sts;
            }
        }

        ADVRWL_DDLACTIVE_UNLOCK( &(vd->ddlActiveLk) );

        vdCnt++;
        vd = NULL;
        vdIndex++;
    }  /* end while */

    if (vd != NULL) {
        *delVd = vd;
        *delMcellId = mcellId;
        *delFlag = dFlag;
        sts = EOK;
    } else {
        sts = ENO_SUCH_TAG;
    }
    return sts;
}  /* end find_del_entry */

/*
 * wait_for_ddl_active_entry
 *
 * This function waits for the specified deferred-delete entry to be processed.
 *
 * This function assumes that only one remove volume can be active for one
 * domain at any given time.
 *
 * The caller must acquire the "ddlActiveLk" lock exclusively before calling
 * this function.
 */

static
statusT
wait_for_ddl_active_entry (
                           domainT *dmnP,  /* in */
                           vdT *vd,  /* in */
                           bfMCIdT mcellId  /* in */
                           )
{
    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));
    ADVRWL_DDLACTIVE_UNLOCK( &(vd->ddlActiveLk) );

    /*
     * Set the mcellId so that this entry's processing thread wakes up this
     * thread.
     */
    vd->ddlActiveWaitMCId = mcellId;

    /*
     * Wait for the entry to be processed.
     */
    cv_wait(&vd->ddlActiveWaitCv, &(dmnP->dmnMutex), CV_MUTEX, CV_DFLT_FLG);

    vd->ddlActiveWaitMCId = bsNilMCId;
    ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));

    return EOK;
}  /* end wait_for_ddl_active_entry */

/*
 * vd_remove
 *
 * cleanly shut down the specified virtual disk.
 *
 * If the flag indicates VD_REMOVE_PERM, the volume is permanently removed
 * from the domain and the volume can never again be part of this domain until
 * it is re-initialized.
 */

static void
vd_remove(
    domainT* dmnP,    /* in */
    vdT* vdp,         /* in */
    bsIdT unmtTm,     /* in */
    uint64_t flags)   /* in */
{
    bfTagT bfTag;
    struct bsVdAttr* vdAttr;
    statusT sts;
    bfPageRefHT pgref;
    struct bsMPg* rbmtpgp;
    int error;

    /* Set the vdState to BSR_VD_ZOMBIE to block any threads from getting
     * new access to this vdp.  Threads that have already bumped the 
     * vdRefCnt can continue until they drop their refcnt.  If there are
     * any other threads with access to this vdp, we must wait for them to
     * finish.
     */
    ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
    vdp->vdState = BSR_VD_ZOMBIE;
    vdp->vdSetupThd = u.u_kthreadp;
    if (vdp->vdRefCnt) {
        vdp->vdRefWaiters++;
        while (TRUE) {
            cv_wait(&vdp->vdRefWaitersCV, &vdp->vdStateLock,
                            CV_MUTEX, CV_NO_RELOCK);
            if (!vdp->vdRefWaiters) {
                    break;
            }
            ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
        }
    } else {
        ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );
    }

    /* Remove vd from the service class table; this will fail on the
     * rmvol case since there was a system call to explicitly remove this
     * volume from the service class before the system call to actually
     * remove the volume.  However, this is still needed in the domain
     * deactivation path.
     */
    (void) sc_remove_vd( dmnP, vdp->serviceClass, vdp->vdIndex );

    /* on normal umount or rmvol, change the on-disk state.  being unable
     * to do this vd update does not affect the rest of the domain and
     * it is safer to allow the caller to continue for cluster failover.
     * if the domain has paniced then we can not update vd state on disk.
     *
     * Note that bs_bfdmn_tbl_activate sets dmn_panic to prevent a
     * BSR_VD_VIRGIN domain from being changed to a BSR_VD_DISMOUNTED
     * domain before the log is created.
     */

    if (!flags || dmnP->dmn_panic) /* don't report deactivating a paniced dmn */
        sts = EOK;
    else {
        struct bfAccess *mdap;

        mdap = vdp->rbmtp;

        sts = bs_pinpg(&pgref, 
                       (void*)&rbmtpgp, 
                       mdap, 
                       0, 
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts == EOK) {
            vdAttr = bmtr_find(&rbmtpgp->bsMCA[ BFM_RBMT ], BSR_VD_ATTR,
                                                        vdp->dmnP);
            if (vdAttr == NULL) {
                bsUnpinModeT throwaway = { BS_RECYCLE_IT, BS_NOMOD };
                (void) bs_unpinpg(pgref, logNilRecord, throwaway);
                sts = ENO_BS_ATTR;
            } else {
                bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };

                if (flags & ADVMTX_VD_STATE_CHANGE)
                    vdAttr->state = BSR_VD_DISMOUNTED;

                if (flags & SET_UNMOUNT_TIME) {
                    struct bsDmnUmntThaw* dmnUmntThawp;
                    dmnUmntThawp =  bmtr_find(&rbmtpgp->bsMCA[ BFM_RBMT ], 
                                             BSR_DMN_UMT_THAW_TIME,
                                             vdp->dmnP);

                    /*
                     * don't fail if record missing - allow for existing
                     * filesystems
                     */
                    if (dmnUmntThawp != 0) {
                        dmnUmntThawp->dmnUmntThawTime = unmtTm;
                    }
                }

                /* for rmvol clear the disk's mount id so that it cannot
                 * be brought back into the domain. */
                if (flags & VD_REMOVE_PERM)
                    vdAttr->vdMntId = nilBfDomainId;

                sts = bs_unpinpg( pgref, logNilRecord, writethru );
            }
        }

        /* On permanent removal, try to checkpoint the log so there can be
         * no undo or redo for bitfiles on this disk.  This is a no-op for
         * a paniced domain but valid even if the vd is inaccessible.
         * NOTE: with addition of advfs_lgr_checkpoint_log() call in
         * bs_vd_remove_active(), this call may no longer be needed.
         */
        if (flags & VD_REMOVE_PERM)
            advfs_lgr_checkpoint_log (vdp->dmnP);
    }

    if (sts != EOK) {
        ms_uaprintf("An AdvFS function call in vd_remove has returned a\n");
        ms_uaprintf("status of %d.  vd_remove will succeed, however,\n", sts);
        ms_uaprintf("domain %s volume %d state is not set to %s.\n",
            dmnP->domainName, vdp->vdIndex,
            (flags & VD_REMOVE_PERM) ? "permanently removed" : "dismounted");

        if (flags & VD_REMOVE_PERM) {
           advfs_ev advfs_event;
           bzero(&advfs_event, sizeof(advfs_event));
           advfs_event.domain = dmnP->domainName;
           advfs_post_kernel_event(EVENT_FDMN_RMVOL_ERROR, &advfs_event);
        }
    }

    /*
     * Close the storage bitmap.
     */
    BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_SBM);
    bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->sbmp);
    vdp->sbmp = NULL;

    /*
     * Close the bitfile metadata table.
     */
    BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_RBMT);
    bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->rbmtp);
    vdp->rbmtp = NULL;
    BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_BMT);
    bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->bmtp);
    vdp->bmtp = NULL;


    /*
     * Remove vd struct from domain's vd table and decrement
     * number of vds associated with this domain.  Do this prior
     * to waiting for vdRefCnt to go to zero to prevent other
     * threads from trying to get a ref on this vd.
     */

    ADVMTX_DOMAINVDPTBL_LOCK( &dmnP->vdpTblLock );
    dmnP->vdCnt--;
    MS_SMP_ASSERT(TEST_VDI_RANGE(vdp->vdIndex) == EOK);
    dmnP->vdpTbl[vdp->vdIndex - 1] = NULL;
    ADVMTX_DOMAINVDPTBL_UNLOCK( &dmnP->vdpTblLock );

    /*
     * Wait for vd_dec_refcnt()
     */
    ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
    vdp->vdSetupThd = u.u_kthreadp;
    if (vdp->vdRefCnt) {
        vdp->vdRefWaiters++;
        while (TRUE) {
            cv_wait(&vdp->vdRefWaitersCV, &vdp->vdStateLock,
                            CV_MUTEX, CV_NO_RELOCK);
            if (!vdp->vdRefWaiters) {
                    break;
            }
            ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock);
        }
    } else {
        ADVMTX_VD_STATE_UNLOCK(&vdp->vdStateLock);
    }

    (void) closed(vdp->devVp->v_rdev, S_IFBLK, FREAD | FWRITE);

    /* Nothing done with the error return above now */
    VN_RELE( vdp->devVp );

    /* deallocate vd struct */
    vd_free(vdp, dmnP);

    return;
}

/*
 * vd_alloc_index
 *
 * Returns the index of a free vd entry in the domain's vd table.
 * Since the vd table is sparse we must search it until we find
 * a NULL entry.
 *
 * Note that the global DmnActivationLock, seized in bs_vd_add_active(),
 * prevents the slot returned by this routine from being used by another 
 * addvol thread until the vdpTbl[] slot is filled in with the pointer 
 * to the newly-allocated vdT struct.
 *
 * Returns either a vdIndexT (one based index) or 0.
 *
 * SMP - seizes the dmnP->vdpTblLock while searching the dmnP->vdpTbl[].
 */
static vdIndexT 
vd_alloc_index(
               domainT *dmnP
               )
{
    vdIndexT vdi;

    ADVMTX_DOMAINVDPTBL_LOCK( &dmnP->vdpTblLock );

    for (vdi = 1; vdi <= BS_MAX_VDI; vdi++) {
        if ( TEST_VDI_RANGE(vdi)      == EOK &&
             TEST_VDI_SLOT(vdi, dmnP) != EOK ) {
            ADVMTX_DOMAINVDPTBL_UNLOCK( &dmnP->vdpTblLock );
            return (vdi);
        }
    }

    ADVMTX_DOMAINVDPTBL_UNLOCK( &dmnP->vdpTblLock );
    return (0);
}


/*
 * set_disk_attrs
 *
 * This function increments or decrements the domain's disk count in the domain
 * modifiable attributes. This function cannot update the attributes by using 
 * a transaction because the "vdCnt" field is examined before recovery is run.
 *
 * This function also puts the dev_t of the device being added or removed in
 * the domain transient attribute record. The field is cleared upon a successful
 * completion of the operation.
 *
 * The caller must hold the domain table lock before calling this function.
 */
static
statusT
set_disk_attrs (
                domainT *dmnP,  /* in */
                bsVdOpT op,  /* in */
                dev_t rdev  /* in */
                )
{
    bsMPgT *bmtPage;
    bsDmnMAttrT *dmnMAttr;
    bsDmnTAttrT *dmnTAttr;
    bfPageRefHT pgPin;
    statusT sts = EOK;
    vdT *vd;
    vdIndexT vdIndex;
    struct bfAccess *mdap;
    ftxHT ftxH = FtxNilFtxH;
    int32_t ftx_started = 0;
    int32_t page_pinned = 0;  

    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&DmnActivationLock, RWL_UNLOCKED));
    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    /* 
     * Perform metadata update within an exclusive transaction, which will
     * block if the domain is frozen.  This avoids a condition where 
     * page 0 can get pinned during a second non exclusive transaction
     * during thaw.
     */
    sts = FTX_START_EXC( FTA_NULL, &ftxH, dmnP );    

    if (sts != EOK) {
        ADVFS_SAD1( "ftx start failed", sts );
    }
    ftx_started = 1;

    mdap = vd->rbmtp;
    sts = bs_pinpg (&pgPin, 
                    (void*)&bmtPage, 
                    mdap, 
                    0, 
                    FtxNilFtxH,
                    MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    page_pinned = 1;

    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_MATTR,
                                                              dmnP);

    if (dmnMAttr == NULL) {
        RAISE_EXCEPTION (EBMTR_NOT_FOUND);
    }

    if (op == BSR_VD_ADDVOL) {
        dmnMAttr->vdCnt++ ;
    } else {/* (op == BSR_VD_RMVOL) */
        dmnMAttr->vdCnt-- ;
    } 

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        /*
         * This domain was created before BSR_DMN_TRANS_ATTR was born.
         * Assign one now.
         */
        dmnTAttr = bmtr_assign( BSR_DMN_TRANS_ATTR,
                                sizeof( bsDmnTAttrT ),
                                &(bmtPage->bsMCA[BFM_RBMT_EXT]) );
        if (dmnTAttr == NULL) {
            RAISE_EXCEPTION (EBMTR_NOT_FOUND);
        }
        dmnTAttr->chainMCId = bsNilMCId;
        dmnTAttr->chainMCId.volume = VOL_TERM;
    }

    dmnTAttr->op = op;
    dmnTAttr->dev = rdev;

    bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    page_pinned = 0;

HANDLE_EXCEPTION:

    if (page_pinned) {
       bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
    }

    if (ftx_started) {
        ftx_done_n( ftxH, FTA_NULL );
    }

    return sts;

}  /* end set_disk_attrs */

/*
 * check_trans_attrs
 *
 * This routine is called when (dmnP->vdCnt != dmnMAttr.vdCnt). We
 * look in the domain's transient attribute record to see if a
 * previous addvol or rmvol was interrupted. If so, we attempt to
 * fix the domain.
 * 
 * In case of rmvol, we construct the symbolic link name in /dev/advfs
 * that needs to be removed. The calling routine cleans up the domain,
 * removes the link and starts all over again.
 *
 * In case of addvol, due to device naming restrictions, it is not
 * possible to construct the link in /dev/advfs from the dev_t. Instead,
 * we roll back the addvol and remove it from on-disk structures. Since
 * this volume hasn't been added to service class yet (see addvol 
 * command), no new storage allocations have been done from this volume.
 */
static statusT
check_trans_attrs(
                  domainT *dmnP,        /* in */
                  char*   topDmnDir,    /* in */
                  vdIndexT dmnMAttrIdx, /* in */
                  uint16_t *onDiskCnt,  /* in/out */
                  char* volPathName     /* out - pathname of vol link */
                  )
{
    vdT *vdp;
    bsMPgT *bmtPage;
    bfPageRefHT pgPin;
    statusT sts = EOK;
    struct bfAccess *mdap;
    bsDmnTAttrT *dmnTAttr;
    bsDmnMAttrT *dmnMAttr;
    int vdCnt = 0;
    vdIndexT vdi;
    char *subs;

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);

    mdap = vdp->rbmtp;

    sts = bs_pinpg (&pgPin, 
                   (void*)&bmtPage, 
                   mdap, 
                   0, 
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
        return EBMTR_NOT_FOUND;
    }

    sts = E_VOLUME_COUNT_MISMATCH;
    if (dmnTAttr->dev) {
        if ((dmnP->vdCnt > *onDiskCnt) && (dmnTAttr->op == BSR_VD_RMVOL)) {
            /*
             * The domain dir has an extra link because a prior rmvol did
             * not complete. Figure out which one and return it so that
             * it can be deleted.
             */
            for ( vdi = 0; vdCnt < dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi+1, dmnP, FALSE, FALSE)) ) {
                    continue;
                }

                vdCnt++;

                if (vdp->devVp->v_rdev == dmnTAttr->dev) {
                    strcpy(volPathName, topDmnDir);
                    strcat(volPathName, "/");
                    strcat(volPathName, dmnP->domainName);
                    strcat(volPathName, "/");
                    strcat(volPathName, DOT_ADVFS_STG);

                    subs = (char *)strrchr(dmnP->vdpTbl[vdi]->vdName, '/');

                    MS_SMP_ASSERT(subs != NULL);

                    strcat(volPathName, subs);
                    sts = E_TOO_MANY_VIRTUAL_DISKS;
                    break;
                }
            }
            /*
             * Don't write the page out until after /dev/advfs is fixed.
             */
            (void)bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        } else if ((dmnP->vdCnt < *onDiskCnt) && (dmnTAttr->op == BSR_VD_ADDVOL)) {
            /*
             * Not enough links in the domain dir, possibly because a prior
             * addvol did not complete. Decrement the ondisk vdCount to
             * reflect this.
             */
            dmnMAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]),
                                  BSR_DMN_MATTR, dmnP);
            MS_SMP_ASSERT(dmnMAttr);
            (*onDiskCnt)--;
            dmnMAttr->vdCnt--;
            sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
        } 
    } else {
        (void)bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
    }
    return sts;
}

/*
 * clear_trans_attrs
 *
 * This domain has reconciled all vdCnt related discrepancies, if any.
 * Clear out the transient attribute record.
 */
static statusT
clear_trans_attrs(
    domainT *dmnP,           /* in */
    vdIndexT dmnMAttrIdx)    /* in */
{
    vdT             *vdp;
    bsMPgT          *bmtPage;
    bfPageRefHT      pgPin;
    statusT          sts = EOK;
    struct bfAccess *mdap;
    bsDmnTAttrT     *dmnTAttr;

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);

    mdap = vdp->rbmtp;

    sts = bs_pinpg (&pgPin, 
                    (void*)&bmtPage, 
                    mdap, 
                    0, 
                    FtxNilFtxH,
                    MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find(&(bmtPage->bsMCA[BFM_RBMT_EXT]),
                         BSR_DMN_TRANS_ATTR,
                         dmnP);


    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
        return EOK;
    }

    dmnTAttr->dev = 0;
    dmnTAttr->op = BSR_VD_NO_OP;
    sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);

    return sts;
}

/*
 * bs_fix_root_dmn
 *
 * This function is called in the mount update (for root) path when
 * a discrepancy is detected between /dev/advfs/<root_domain> and 
 * on-disk data. Note that for root, the domain has already been
 * activated.
 */

statusT
bs_fix_root_dmn(
    bfDmnDescT *dmntbl,
    domainT *dmnP,
    char *dmnName,
    char *topDmnDir)
{
    vdT             *vdp;
    int              idx, idx2;
    vdIndexT         vdi;
    bfPageRefHT      pgPin;
    bsMPgT          *bmtPage;
    bsDmnTAttrT     *dmnTAttr;
    bsDmnMAttrT     *dmnMAttr;
    char             volPathName[MAXPATHLEN + 1];
    char            *subs;
    dev_t            dev;
    struct pathname  path_name;
    struct bfAccess *mdap;

    extern dev_t     clu_global_rootdev[];
    extern uint32_t  clu_global_rootdev_count;

    int              vdCnt   = 0;
    int              error   = 0;
    statusT          sts     = EOK;
    opIndexT         opIndex = OP_NONE;
    struct vnode    *vp      = NULL;    /* Vnode of device */
    struct vnode    *dvp     = NULL;   /* Parent vnode of device */
    
    vdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    mdap        = vdp->rbmtp;

    sts = bs_pinpg (&pgPin, 
                    (void*)&bmtPage, 
                    mdap, 
                    0, 
                    FtxNilFtxH,
                    MF_VERIFY_PAGE);

    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);
    /*
     * This routine is only called when a discrepancy is detected
     * with the number of vd's.  If the transient record is not
     * found or if its status is NO_OP we need to return an error
     * so that the caller can follow up and 'do the right thing'.
     */

    if ( (dmnTAttr == NULL) || (dmnTAttr->op == BSR_VD_NO_OP) ) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        return EBMTR_NOT_FOUND;
    }

    if (dmnTAttr->dev) {
        if ((dmntbl->vdCount > dmnP->vdCnt) && (dmnTAttr->op == BSR_VD_RMVOL)) {
            /*
             * /dev/advfs/<domain_name> has an extra link. Find which
             * one and remove it.
             */
            for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
                sts = pn_get( dmntbl->vddp[vdi]->vdName, UIOSEG_KERNEL, &path_name);
                if (sts) {
                    goto done;
                }

                sts = lookuppn( &path_name, FOLLOW_LINK, &dvp, &vp); 
                if (sts) {
                    goto done;
                }

                if (vp->v_rdev == dmnTAttr->dev) {
                    strcpy(volPathName, topDmnDir);
                    strcat(volPathName, "/");
                    strcat(volPathName, dmnName);
                    strcat(volPathName, "/");
                    strcat(volPathName, DOT_ADVFS_STG);

                    subs = (char *)strrchr(dmntbl->vddp[vdi]->vdName, '/');
                    MS_SMP_ASSERT(subs != NULL);
                    strcat(volPathName, subs);
        
                    ms_uprintf("Interrupted rmvol of volume %s in domain %s.\n", 
                            subs+1, dmnName);
#ifdef tru64_to_hpux_port
/*  This looks like an ancient and very incorrect printf to me.
 *  With what should it be replaced? 
 */
#endif /* tru64_to_hpux_port */
                    ms_uprintf("    To finish rmvol cleanup, type the command:\n");
                    ms_uprintf("    /sbin/disklabel -sF %s unused\n", subs+1);

                    VN_RELE( vp );

                    error = VOP_REMOVE(dvp, volPathName, TNC_CRED());

                    if ((sts = (statusT)error) != EOK) {
                        goto done;
                    }

                    /*
                     * Update dmntbl to reflect this change.
                     */
                    dmntbl->vdCount--;
                    ms_free(dmntbl->vddp[vdi]);
                    if (vdi != dmntbl->vdCount) {
                        for (idx = vdi; idx < dmntbl->vdCount; idx++) {
                            dmntbl->vddp[idx] = dmntbl->vddp[idx+1];
                        }
                    }
                    dmntbl->vddp[dmntbl->vdCount] = NULL;
                    break;
                }
            }
        } else if ((dmntbl->vdCount < dmnP->vdCnt) && (dmnTAttr->op ==
                                                          BSR_VD_ADDVOL)) {
            /*
             * /dev/advfs/<domain_name> has fewer links. A prior addvol
             * must not have completed. Remove this volume from on-disk,
             * domain and global_root data structures.
             */
            for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE))) {
                    continue;
                }

                vdCnt++;

                if (vdp->devVp->v_rdev == dmnTAttr->dev) {
                    MS_SMP_ASSERT( (vdi) != BS_BFTAG_VDI(dmnP->ftxLogTag));

                    /*
                     * Inform CFS of the upcoming on-disk change.
		     * NOTE: root can only be served by CFS in a SSI cluster.
                     */
                    if (clu_type() == CLU_TYPE_SSI) {
                        opIndex = ADVFS_REM_VOLUME;
                        if (error = CLU_DOMAIN_CHANGE_VOL_DEV(
                                                     &opIndex,
                                                     dmnName,
                                                     vdp->devVp->v_rdev)) {

                            bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
                            return (statusT)error;
                        }
                    }

                    /*
                     * Update on-disk count.
                     */
                    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), 
                                          BSR_DMN_MATTR, dmnP);
                    if (dmnMAttr == NULL) {
                        sts = EBMTR_NOT_FOUND;
                        goto done;
                    }

                    dmnMAttr->vdCnt--;

                    /* change vd on-disk state so it is permanently removed
                     * and update/remove corresponding in-memory data.
                     */
                    vd_remove(dmnP, vdp, nilBfDomainId, 
                            ADVMTX_VD_STATE_CHANGE | VD_REMOVE_PERM);

                    for (idx = 0; idx < clu_global_rootdev_count; idx++) {
                        if (clu_global_rootdev[idx] == dmnTAttr->dev)
                            break;
                    }

                    MS_SMP_ASSERT(idx < clu_global_rootdev_count);

                    /*
                     * Update global rootdev array.
                     */
                    clu_global_rootdev_count--;
                    if (idx != clu_global_rootdev_count) {
                        for (idx2 = idx; idx2 < clu_global_rootdev_count; idx2 ++) {
                            clu_global_rootdev[idx2] = clu_global_rootdev[idx2+1];
                        }
                    }
                    clu_global_rootdev[clu_global_rootdev_count] = (dev_t)NULL;
                    break;
                }
            }
        }
    }

done:

    dev = dmnTAttr->dev;  
    if (sts == EOK) {
        dmnTAttr->dev = 0;
        dmnTAttr->op = BSR_VD_NO_OP;
        sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    } else {
        (void)bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
    }

    if ((clu_type() == CLU_TYPE_SSI) && (opIndex == ADVFS_REM_VOLUME)) {
        (void)CLU_DOMAIN_CHANGE_VOL_DONE_DEV(&opIndex,
                                             dmnName,
                                             dev,
                                             sts);
    }


    return sts;
}

/*
 * bs_check_root_dmn_sc
 *
 * This function is called in the mount update (for root) path after the
 * the domain has already been activated. In case there was an entry in
 * the domain's transient record, the suspect volume wasn't added to the
 * service class table. Do it now.
 */
statusT
bs_check_root_dmn_sc(
                   domainT *dmnP
                   )
{
    statusT sts;
    vdT *vdp;
    struct bfAccess *mdap;
    int vdCnt = 0;
    vdIndexT vdi;
    bfPageRefHT pgPin;
    bsMPgT *bmtPage;
    bsDmnTAttrT *dmnTAttr;

    vdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    mdap = vdp->rbmtp;

    sts = bs_pinpg (&pgPin, 
                    (void*)&bmtPage, 
                    mdap, 
                    0, 
                    FtxNilFtxH,
                    MF_VERIFY_PAGE);

    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
        return EOK;
    }

    if ((dmnTAttr->dev) && (dmnTAttr->op == BSR_VD_ADDVOL)) {
        for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
            if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) ) {
                continue;
            }

            vdCnt++;

            if (vdp->devVp->v_rdev == dmnTAttr->dev) {
                sts = sc_add_vd( dmnP, vdp->serviceClass, vdp->vdIndex );
                if (sts != EOK) {
                    bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
                    return sts;
                }
                break;
            }
        }
        dmnTAttr->dev = 0;
        dmnTAttr->op = BSR_VD_NO_OP;
        sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
        return sts;
    } else {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        return EOK;
    }
}

/*
 * set_recovery_failed
 *
 * This function cannot update the attributes by using a transaction because the
 * "recoveryFailed" field is examined before recovery is run.
 *
 * The caller must hold the domain table lock before calling this function.
 */

statusT
set_recovery_failed(
                    domainT *dmnP,  /* in */
                    uint16_t value  /* in */
                    )
{
    bsMPgT *bmtPage;
    bsDmnMAttrT *dmnMAttr;
    bfPageRefHT pgPin;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    struct bfAccess *mdap;

    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    mdap = vd->rbmtp;
    sts = bs_pinpg (&pgPin, 
                    (void*)&bmtPage, 
                    mdap, 
                    0, 
                    FtxNilFtxH,
                    MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }

    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[BFM_RBMT_EXT]), BSR_DMN_MATTR,
                                                              dmnP);

    if (dmnMAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_CLEAN); /* Ignore status. */
        return EBMTR_NOT_FOUND;
    }

    dmnMAttr->bs_dma_dmn_state |= value;

    sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    if (sts != EOK) {
        return sts;
    }
    return EOK;
}  /* end set_recovery_failed */

/*******************************************************************/
/*****  Various utility routines supporting the major functions ****/
/*******************************************************************/

/*
 * dmn_dealloc
 *
 * Deallocates a domain structure and all structures that it points to.
 */
static
void
dmn_dealloc(
    domainT *dmnP)      /* in - domain table pointer */
{
    domainT *tmpdmnP;
    int      error;

    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    MS_SMP_ASSERT(DmnSentinelP != NULL);

    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );

    MS_SMP_ASSERT(dmnP->activateCnt == 0);

    if (dmnP->dmnFwd && dmnP->dmnBwd) {   /* remove from sentinel chain */
        /* unlink from other domains */
        if (dmnP == DmnSentinelP) {
            DmnSentinelP = dmnP->dmnFwd;
        }
        dmnP->dmnBwd->dmnFwd = dmnP->dmnFwd;
        dmnP->dmnFwd->dmnBwd = dmnP->dmnBwd;
        if ((--DomainCnt) == 0) {
            DmnSentinelP = NULL;
        }
        MS_SMP_ASSERT(DomainCnt >= 0);
        DOMAIN_HASH_REMOVE(dmnP, TRUE);   /* remove from dyn hash chain */
    }
    if (dmnP->dmnAccCnt) {
        dmnP->dmnRefWaiters++;
        while (TRUE) {
            cv_wait(&dmnP->dmnRefWaitersCV, &DmnHashTblLock,
                            CV_MUTEX, CV_NO_RELOCK);
            if (!dmnP->dmnRefWaiters) {
                    break;
            }
            ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        }
    } else {
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
    }

    MS_SMP_ASSERT( BFSET_VALID(dmnP->bfSetDirp) ) 
    ADVRWL_BFSETTBL_WRITE( dmnP );

    bfs_dealloc( dmnP->bfSetDirp, TRUE );
    dmnP->bfSetDirp = 0;     

    ADVRWL_BFSETTBL_UNLOCK(dmnP );

    /* Destroy the UFC metadata and user data VAS's. All cache flushing or
     * invalidation must be done and all mappings must be
     * removed prior to this destroy.
     */
    if (dmnP->metadataVas != NULL) {
        error = fcache_as_destroy(dmnP->metadataVas);
        /*
         * UFC ERS says that only 0 and EINVAL can be returned.
         * If we get back EINVAL, we passed bad parameters.
         */
        MS_SMP_ASSERT(error == 0);
    }
      
    if (dmnP->userdataVas != NULL) { 
        error = fcache_as_destroy(dmnP->userdataVas);
        /*
         * UFC ERS says that only 0 and EINVAL can be returned.
         * If we get back EINVAL, we passed bad parameters.
         */
        MS_SMP_ASSERT(error == 0);
    }

#ifdef ADVFS_SMP_ASSERT
    /* Run thru the access structure and the bsBuf hashtables and make
     * sure there are no structures left laying around with this
     * domain ID
     */
    {
        bfDomainIdT domainId;
        extern void bs_access_hashtable_dbug_compare();
        extern void bs_bsbuf_hashtable_dbug_compare();
        extern void *BsBufHashTable;

        domainId = dmnP->domainId;
        
        dyn_hash_debug_chains(BsAccessHashTbl,
                              &domainId,
                              bs_access_hashtable_dbug_compare);
        
        dyn_hash_debug_chains(BsBufHashTable,
                              &domainId,
                              bs_bsbuf_hashtable_dbug_compare);
    }
#endif

    if (dmnP->scTbl != NULL) {
        ms_free( dmnP->scTbl );
    }

    /* Cleanup the ftx stuff */
    if (dmnP->ftxTbld.tablep != NULL) {
        cv_destroy(&dmnP->ftxTbld.slotCv);
        cv_destroy(&dmnP->ftxTbld.trimCv);
        cv_destroy(&dmnP->ftxTbld.excCv);
        ms_free( dmnP->ftxTbld.tablep );
        ms_free( dmnP->ftxTbld.tableSltStatep);
    }

    ADVRWL_BFSETTBL_DESTROY(dmnP);
    ADVMTX_SC_TBL_DESTROY(dmnP);
    ADVRWL_RMVOL_TRUNC_DESTROY(dmnP);
    spin_destroy(&dmnP->lsnLock);
    spin_destroy(&dmnP->ftxTblLock);
    ADVMTX_DOMAIN_DESTROY( &dmnP->dmnMutex );
    ADVMTX_DOMAINVDPTBL_DESTROY( &dmnP->vdpTblLock );
    ADVMTX_DOMAINFREEZE_DESTROY( &dmnP->dmnFreezeMutex );
    ADVMTX_METAFLUSH_DESTROY( &dmnP->metaFlushLock );

    cv_destroy(&dmnP->dmnFreezeWaitingCV);
    cv_destroy(&dmnP->dmnRefWaitersCV);

#   ifdef ADVFS_SMP_ASSERT
        ADVRWL_DOMAIN_FTXSLOT_DESTROY(&dmnP->ftxSlotLock);
#   endif

    if (clu_is_ready()) {
        ADVRWL_XID_RECOVERY_WRITE(dmnP);
        cfs_xid_free_memory_int(dmnP);
        ADVRWL_XID_RECOVERY_UNLOCK(dmnP);
    }
    ADVRWL_XID_RECOVERY_DESTROY(dmnP);
    dmnP->dmnMagic |= MAGIC_DEALLOC;
    ms_free( dmnP );
}

/*
 * dmn_alloc
 *
 * Routine to initialize a new Domain struct * and add it to the DomainHashTbl
 * Assumes DmnTbl lock is held.
 * Returns a positive handle and EOK if successful.
 */

static
statusT
dmn_alloc(
    bfDomainIdT    domainId,
    char          *domainName,
    bfTagT         dirTag,
    struct bsMPg  *pgp,
    domainT      **newdmnPP)
{
    domainT       *dmnP = NULL;
    int            error = 0;
    int            fail = FALSE;
    bfSetIdT       rootBfSetId;
    statusT        sts = EOK;

    /* The domain structure comes back zeroed. */
    dmnP = (struct domain*) ms_malloc( sizeof( struct domain ) );

    ADVRWL_RMVOL_TRUNC_INIT(dmnP);
    ADVMTX_DOMAIN_INIT(&dmnP->dmnMutex);
    ADVSMP_DOMAINLSN_INIT(&dmnP->lsnLock);
    ADVMTX_DOMAINVDPTBL_INIT(&dmnP->vdpTblLock);
    ADVSMP_DOMAINFTXTBL_INIT(&dmnP->ftxTblLock);
   
    ADVMTX_DOMAINFREEZE_INIT(&dmnP->dmnFreezeMutex);
    ADVMTX_SC_TBL_INIT(dmnP);
    cv_init(&dmnP->dmnFreezeWaitingCV,
            "dmnFreezeMutex CV",
            NULL, CV_WAITOK);
    ADVMTX_METAFLUSH_INIT( &dmnP->metaFlushLock );

    dmnP->lsnList.bsb_metafwd = (struct bsBuf *)&dmnP->lsnList;
    dmnP->lsnList.bsb_metabwd = (struct bsBuf *)&dmnP->lsnList;
    dmnP->dmnFlag      = BFD_NORMAL;
    dmnP->dmnMagic     = DMNMAGIC;
    dmnP->dmnVersion   = BFD_ODS_NULL_VERSION;
    dmnP->mountCnt     = 0;
    dmnP->scTbl        = sc_init_sc_tbl();
    dmnP->fs_full_time = 0;
#   ifdef ADVFS_SMP_ASSERT
        ADVRWL_DOMAIN_FTXSLOT_INIT( &(dmnP->ftxSlotLock) );
#   endif

    /*
     * Initialize the ftx crash recovery data structures.
     * See ftx_init_table() for the initialization of the ftx table. 
     */
    ftx_init_recovery_logaddr( dmnP );
    ADVRWL_XID_RECOVERY_INIT(dmnP);
    dmnP->xidRecovery.xid_head              = NULL;
    dmnP->xidRecovery.xid_tail              = NULL;
    dmnP->xidRecovery.xid_current_free_slot = 0;
    dmnP->xidRecovery.xid_timestamp.tv_sec  = 0;
    dmnP->xidRecovery.xid_timestamp.tv_usec = 0;

    /* Create the domain's metadata and user data 
     * Virtual Address Structures (VAS) for UFC caching operations.
     * Metadata uses a fixed-size address mapping VAS.
     * User data uses a variable-size address mapping VAS.
     * Attribute assignments are done by the domain activation routines
     * after calling dmn_alloc().
     */
    dmnP->metadataVas = fcache_as_create(domainName,
                                         ADVFS_METADATA_PGSZ,
                                         FAC_DEFAULT,
                                         &error);
    if (error) {
        sts = error;    
        fail = TRUE; 
    }

    dmnP->userdataVas = fcache_as_create(domainName,
                                         0, /* Variable mappings */
                                         FAC_DEFAULT,
                                         &error);
    if (error) {
        sts = error;    
        fail = TRUE; 
    }

    /*
     * Fabricate the bitfile-set ID for the root bitfile-set and
     * create BF-set descriptor for it in the BF-set table.
     */
    rootBfSetId.domainId = domainId;
    rootBfSetId.dirTag = dirTag;

    ADVFTX_BFSETTBL_INIT(&dmnP->BfSetTblLock,
                         &dmnP->dmnMutex);

    dmnP->bfSetHead.bfsQfwd = &dmnP->bfSetHead;
    dmnP->bfSetHead.bfsQbck = &dmnP->bfSetHead;

    sts = bs_bfs_add_root( rootBfSetId, dmnP, &dmnP->bfSetDirp );
    if (sts != EOK) {
        fail = TRUE;
    }

    /* Set up pointer to RBMT Page 0 buffer */
    dmnP->metaPagep = pgp;

    /* Add domain to dynamic hash table */
    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    dmnP->domainId   = domainId;
    strncpy( dmnP->domainName, domainName, BS_DOMAIN_NAME_SZ - 1 );
    /*
     *  link to other domains
     */

    if (DmnSentinelP != NULL) {
        /*
         *  We already have a domain(s) in the list,
         *  link it to the end of the chain, this leaves DmnSentinelP
         *  as the first mounted advfs domain (possibly root )
         */
        dmnP->dmnBwd = DmnSentinelP->dmnBwd;
        (dmnP->dmnBwd)->dmnFwd = dmnP;
        dmnP->dmnFwd = DmnSentinelP;
        DmnSentinelP->dmnBwd = dmnP;
    } else {
        /*
         *  Must be first advfs domain -  link to itself
         *  and set DmnSentinelP
         */
        DmnSentinelP = dmnP->dmnFwd = dmnP->dmnBwd = dmnP;
    }
    dmnP->dmnHashlinks.dh_key = DOMAIN_GET_HASH_KEY( domainId );
    DOMAIN_HASH_INSERT( dmnP, TRUE );
    DomainCnt++;
    dmnP->dmnAccCnt=0;
    dmnP->dmnRefWaiters=0;
    cv_init(&dmnP->dmnRefWaitersCV, "dmnRefWaiters CV", NULL, CV_WAITOK);
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );

    if (fail) {
        dmn_dealloc( dmnP );
        return sts;
    }

    *newdmnPP = dmnP;
    return EOK;
}

/*
 * domain_id_lookup
 *
 * Return a pointer to the requested domain.
 * Assumes DmnTbl lock is held.
 * Returns zero if domain not found.
 */

static
domainT *
domain_id_lookup(
    bfDomainIdT bfDomainId,       /* in - domain to lookup */
    uint64_t flags)               /* in - mount flags/
                                     check DmnActivationLock? */
{
    uint64_t key;
    domainT *dmnP_start, *dmnP;

    MS_SMP_ASSERT((flags & DMNA_GLOBAL_ROOT) ? 1 : 
                     (ADVMTX_DOMAINHASHTABLE_OWNED( &DmnHashTblLock ) ||
                      (ADVFS_SMP_RW_LOCK_NEQL( &DmnActivationLock,
                                               RWL_UNLOCKED) ) ) );


    key = DOMAIN_GET_HASH_KEY( bfDomainId );
    dmnP_start = DOMAIN_HASH_LOCK( key, NULL);  /* get bucket */

    if (TEST_DMNP(dmnP_start) == EOK) {
        /* we have a bucket with domains - scan the chain for bfDomainId */
        dmnP = dmnP_start;
        do {
            if (BS_UID_EQL(dmnP->domainId, bfDomainId)) {
                DOMAIN_HASH_UNLOCK( key );
                MS_SMP_ASSERT(dmnP->bfSetDirp);
                return dmnP;
            }
            /* get next in chain */
            dmnP = dmnP->dmnHashlinks.dh_links.dh_next;
        } while (dmnP != dmnP_start);  /* chain is circular */
    }

    /* let go of bucket */
    DOMAIN_HASH_UNLOCK( key );

    /* domain not found - return zero */
    return NULL;
}

/*
 * domain_name_lookup
 *
 * Return a pointer to the requested domain.
 * Assumes DmnTbl lock is held.
 * Returns zero if domain not found.
 */

domainT*
domain_name_lookup(
    char* dmnName,    /* in - domain to lookup */
    uint64_t flags)   /* in - mount flags/check DmnActivationLock? */
{
    domainT *dmnP_Prev, *dmnP;

    if (!(flags & DMNA_GLOBAL_ROOT)) {
        MS_SMP_ASSERT(ADVFS_SMP_RW_LOCK_NEQL(&DmnActivationLock, RWL_UNLOCKED));
    }

    dmnP = DmnSentinelP;    /* grab starting point */
    do {
        if (TEST_DMNP(dmnP) == EOK) {
            if (strcmp(dmnP->domainName, dmnName) == 0) {
                return dmnP;
            }
            dmnP_Prev = dmnP;
            dmnP = dmnP->dmnFwd;
        } else {
            /* TEST_DMNP() failed.  Return a zero pointer. */
            return 0;
        }
        MS_SMP_ASSERT(dmnP_Prev == dmnP->dmnBwd);
    } while (dmnP != DmnSentinelP);

    /* domain not found - return zero */
    return 0;
}

/*
 * advfs_is_domain_name_mounted
 *
 * Called by Cluster code to see if domain is mounted w/o
 * it's direct knowledge (e.g. local mount).
 *
 */
int
advfs_is_domain_name_mounted(
    char* dmnName)    /* in - domain to lookup */
{
    domainT *dmnP;
    int sts;

    ADVRWL_DMNACTIVATION_READ( &DmnActivationLock );
    dmnP = domain_name_lookup( dmnName, (uint64_t)0 );
    sts = ((TEST_DMNP(dmnP) == EOK) && 
           (dmnP->state == BFD_ACTIVATED) &&
           (dmnP->mountCnt > 0))?TRUE:FALSE;
    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    return sts;
}
 
/*
 * advfs_is_domain_id_mounted
 *
 * Called by Cluster code to see if domain is mounted w/o
 * it's direct knowledge (e.g. local mount).
 *
 */
int
advfs_is_domain_id_mounted(
    bfDomainIdT bfDomainId)       /* in - domain to lookup */
{
    domainT *dmnP;
    int sts;

    ADVRWL_DMNACTIVATION_READ( &DmnActivationLock );
    dmnP = domain_id_lookup( bfDomainId, (uint64_t)0 );
    sts = ((TEST_DMNP(dmnP) == EOK) && 
           (dmnP->state == BFD_ACTIVATED) &&
           (dmnP->mountCnt > 0))?TRUE:FALSE;
    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock );
    return sts;

}



/*
 * bs_get_dmntbl_params
 *
 * An interface to bs_get_dmn_params() that takes the domain's table
 * name rather than an open domain's handle.  Exists to support
 * the system call interface.
 */

statusT
bs_get_dmntbl_params(
                     char *dmnTbl,             /* in - domain table file name */
                     bfDmnParamsT *dmnParams   /* out - domains parameters */
                     )
{
    statusT sts;
    bfDomainIdT dmnId;
    domainT *dmnP;
    int dmnActive = FALSE, dmnOpen = FALSE;

    sts = bs_bfdmn_tbl_activate( dmnTbl, 0, &dmnId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, dmnId, FALSE );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    sts = bs_get_dmn_params( dmnP, dmnParams, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_READ,
                        dmnParams->mode, dmnParams->uid, dmnParams->gid )) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    bs_domain_close( dmnP );
    (void) bs_bfdmn_deactivate( dmnId, 0 );

    return EOK;

_error:
    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, 0 );
    }

    return sts;
}

void
bs_domain_init(
               void
               )
{
    ADVMTX_DOMAINHASHTABLE_INIT(&DmnHashTblLock);
    ADVRWL_DMNACTIVATION_INIT(&DmnActivationLock);

    /* create and init dynamic hash table */
    DomainHashTbl = dyn_hashtable_init( DOMAIN_HASH_INITIAL_SIZE,
                                        DOMAIN_HASH_CHAIN_LENGTH,
                                        DOMAIN_HASH_ELEMENTS_TO_BUCKETS,
                                        DOMAIN_HASH_USECS_BETWEEN_SPLITS,
                                        advfs_offsetof(domainT, dmnHashlinks),
                                        NULL);

    if (DomainHashTbl == NULL) {
        ADVFS_SAD0("bs_domain_init: can't get space for domain hash table");
    }

}

/*
 * get_domain_disks
 *
 * Gets the list of disks that belong to the specified domain.  
 *
 * The basic idea is that each domain had a directory in either /dev/advfs
 * or /dev/advfs_cfs.  The directory name is the same as the domain name 
 * (in fact, the directory name defines the domain name since the domain name 
 * is not stored anywhere else).  So, the domain 'foo' has a directory named 
 * /dev/advfs[_cfs]/foo.   The top level domain directory /dev/advfs_cfs is
 * for TCR/CFS cluster shared filesystems only. On base and SSI cluster 
 * systems, all domains are under /dev/advfs.
 *
 */

statusT
get_domain_disks(
    char* domain,             /* in - domain name */
    uint64_t doingRoot,       /* in - flag */
    bfDmnDescT* dmnDescp,     /* out - ptr to bfdmn desc */
    char** dmnDir)            /* out - if not NULL, return top level dmn dir */
{
    int       error;
    int       i;
    int       len;
    int       dirOpen = FALSE;
    int       dirEntOffset = 0;
    vdIndexT  vdi = 0;
    int       vdNameLen = 0;
    int       dirbuflen = 0;
    char     *dirBuf = NULL;
    char     *vdName = NULL;
    char     *dmnName = NULL;
    vdDescT  *vddp;
    char*     globalDmnDirs[3] = {0};

    struct vnode      *vp = NULL;
    struct __dirent64 *dp = NULL;
    struct uio         auio;
    struct iovec       aiov;

    extern const char     advfs_default_rootname[];
    extern       uint32_t clu_global_rootdev_count;


    if (doingRoot & DMNA_LOCAL_ROOT) {
        vddp = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
        *vddp = nilVdDesc;
        strcpy(vddp->vdName, (char *) advfs_default_rootname);
        /*
         * Add disk descriptor to domain's list of disk descriptors.
         */
        dmnDescp->vddp[vdi] = vddp;
        dmnDescp->vdCount = 1;
        return EOK;
    } else if (doingRoot & DMNA_GLOBAL_ROOT) {
        for (vdi = 0; vdi < clu_global_rootdev_count; vdi++) {
            vddp = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
            *vddp = nilVdDesc;
            strcpy(vddp->vdName, (char *) advfs_default_rootname);
            dmnDescp->vddp[vdi] = vddp;
        }
        dmnDescp->vdCount = clu_global_rootdev_count;
        return EOK;
    }

    /*
     * Construct the full pathname for the domain's dir.  If this is a 
     * TCR/CFS cluster, we need to check both /dev/advfs/<domain> and
     * /dev/advfs_cfs/<domain>.
     */
    if (strlen( domain ) >= BS_DOMAIN_NAME_SZ) {
        error = EINVAL;
        goto _error;
    }

    if (clu_type() == CLU_TYPE_CFS) {
        globalDmnDirs[0] = ADVFS_TCR_DMN_DIR; 
        globalDmnDirs[1] = ADVFS_DMN_DIR;
    } else {
        globalDmnDirs[0] = ADVFS_DMN_DIR;
    }

    /*  caller wants to know resulting directory.  Ensure we don't
     *  try to free junk.
     */
    if (dmnDir != NULL) {
        *dmnDir = NULL;
    }

    for (i = 0 ; globalDmnDirs[i] != NULL ; i++ ) {
        /*  The +3 is for 2 slashes and trailing NULL */
        len = sizeof(globalDmnDirs[i]) + BS_DOMAIN_NAME_SZ + 
             sizeof(DOT_ADVFS_STG) + 3;
        dmnName = ms_malloc( len );
        sprintf( dmnName, len, "%s/%s/%s", globalDmnDirs[i], 
            domain, DOT_ADVFS_STG);

        /* get the vnode */
        if( error = advfs_getvp( &vp, dmnName, UIOSEG_KERNEL, FOLLOW_LINK ) ) {
            ms_free(dmnName);
            dmnName = NULL;
            continue;  /* try the next dir */
        }

        if (dmnDir != NULL) { 
            len = (uint32_t)strlen(globalDmnDirs[i]) + 1;
            *dmnDir = ms_malloc( len);
            strcpy(*dmnDir, globalDmnDirs[i]);
        }

        break;
    }

    if (dmnName == NULL) {  /* didn't find it */
        goto _error;
    }

    /*
     * Construct the full pathname to be used for opening the device links.
     * /dev/advfs/<domain>/.stg
     */

    dirBuf  = ms_malloc( (size_t)DIRBLKSIZ );
    vdName  = ms_malloc( (size_t)(MAXPATHLEN+1) );

    strcpy( vdName, dmnName );
    strcat( vdName, "/" );
    vdNameLen = strlen( vdName );

    /* do an access check on the dir */
    error = VOP_ACCESS( vp, VREAD, TNC_CRED());
    if( error ) {
        goto _error;
    }

    /* open the vnode */
    error = VOP_OPEN( &vp, FREAD, TNC_CRED() );
    if( error ) {
        goto _error;
    }
    dirOpen = TRUE;

    if (vp->v_type != VDIR) {
        /* /dev/advfs[_cfs]/<domain> must be directory */
        error = EINVAL;
        goto _error;
    }

    auio.uio_iov     = &aiov;
    auio.uio_iovcnt  = 1;
    auio.uio_seg     = UIOSEG_KERNEL;
    auio.uio_offset  = 0;
    auio.uio_fpflags = 0;

    /*
     * Scan for all the devices in the domain.
     */
    while (TRUE) {
        aiov.iov_base  = dirBuf;
        aiov.iov_len   = DIRBLKSIZ;
        auio.uio_resid = DIRBLKSIZ;

        error = VOP_READDIR3( vp, &auio, TNC_CRED() );
        if (error) {
            goto _error;
        }
        if ( auio.uio_resid == DIRBLKSIZ ) {
            /* no more directory data to read */
            break;
        } 

        dirEntOffset = 0;   /* set offset to first byte in the dir block */
        dirbuflen = DIRBLKSIZ - auio.uio_resid;

        /*
         * Scan the current directory block.
         */
        while (dirEntOffset < dirbuflen) {
            /*
             * Get a pointer to the next dir entry.
             */
            dp = (struct __dirent64 *) &dirBuf[ dirEntOffset ];
            dirEntOffset += dp->__d_reclen; /* move to next dir entry offset*/
            if (dp->__d_ino == 0) {
                /* skip deleted entries */
                continue;
            }

            if ((strcmp( dp->__d_name, ".") == 0) ||
                (strcmp( dp->__d_name, "..") == 0)) {
                /* skip . and .. */
                continue;
            }

            if (vdi == BS_MAX_VDI) {
                /* too many device entries in the directory */
                error = EINVAL;
                goto _error;
            }

            /*
             * Construct full pathname to device file link.
             * <domain_dir>/<d_name>
             */
            strcat( vdName, dp->__d_name );

            /*
             * Get the contents of the symbolic link; the real device name.
             */
            vddp = get_vdd( vdName, &error );
            if (vddp == NULL) {
                if (error != EINVAL) {
                    /* If error is ENODEV return error so problem can be 
                     * corrected 
                     */
                    goto _error;
                }
            } else {
                /*
                 * Add disk descriptor to domain's list of disk descriptors.
                 */
                dmnDescp->vddp[vdi] = vddp;
                dmnDescp->vdCount = ++vdi;
            }

            /*
             * Chop off current disk name.
             */
            vdName[vdNameLen] = '\0';
        }
    }
    error = VOP_CLOSE( vp, FREAD, TNC_CRED());

    VN_RELE( vp );
    ms_free( dirBuf );
    ms_free( vdName );
    ms_free( dmnName );

    if (dmnDescp->vdCount < 1) {
        /* Hmmm...  No device link names where found in the directory!? */
        return E_NO_DMN_VOLS;
    }

    return EOK;

_error:
    if ((dmnDir != NULL) && (*dmnDir != NULL)) {
        ms_free(*dmnDir);
        *dmnDir = NULL;
    }

    if (dirOpen) {
        i = VOP_CLOSE( vp, FREAD, TNC_CRED());
    }

    if (vp != NULL) {
        VN_RELE( vp );
    }

    if (dirBuf != NULL) {
        ms_free( dirBuf );
    }

    if (vdName != NULL) {
        ms_free( vdName );
    }

    if (dmnName != NULL) {
        ms_free( dmnName );
    }

    free_vdds( dmnDescp );

    return error;
}

/*
 * get_vdd
 *
 * Given the full pathname of a symbolic link to a device special file,
 * this routine will read the link and put its contents in to a (virtual)
 * disk descriptor.  The descriptor is returned to the caller.
 */

static
vdDescT *
get_vdd(
    char *vdName,       /* in - virtual disk name */
    int *err)           /* out - error */
{
    int error;
    int i;
    struct vnode *vp = NULL;
    vdDescT *vdd = NULL;
    struct iovec aiov;
    struct uio auio;

    vdd = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
    *vdd = nilVdDesc;

    /* get the vnode. don't follow so we can do some stuff if it's a symlink */
    if( error = advfs_getvp( &vp, vdName, UIOSEG_KERNEL, NO_FOLLOW ) ) {
        *err = error;
        goto _error;
    }

    if (vp->v_type == VLNK) {
        /* Get the value of the vd symlink and store that in the descriptor */
        aiov.iov_base = vdd->vdName;
        aiov.iov_len = sizeof(vdd->vdName);
        auio.uio_iov = &aiov;
        auio.uio_iovcnt = 1;
        auio.uio_offset = 0;
        auio.uio_seg = UIOSEG_KERNEL;
        auio.uio_resid = sizeof(vdd->vdName);
        auio.uio_fpflags = 0;
        error = VOP_READLINK(vp, &auio, TNC_CRED());
        if (error) {
            *err = error;
            goto _error;
        }
    }

    VN_RELE(vp);

    /* get the block special device vnode */
    if( error = advfs_getvp( &vp, vdName, UIOSEG_KERNEL, FOLLOW_LINK ) ) {
        *err = error;
        goto _error;
    }

    if(vp->v_type != VBLK) {
        *err = ENOTBLK;
        goto _error;
    }

    vdd->device = vp->v_rdev;       /* save for advfs_enum_domain_dev_ts() */
    if (vdd->vdName == NULL) {
        strcpy(vdd->vdName, vdName);
    }

    VN_RELE( vp );
    *err = 0;
    return vdd;

_error:

    if (vp != NULL) {
        VN_RELE( vp );
    }

    if (vdd != NULL) {
        ms_free( vdd );
    }

    return NULL;
}
/*
*   advfs_enum_domain_devts() - enumerate dev_ts for a domain
*
*   input:  domain = ptr to domain name
*           buffer = address of bsDevtList struct
*   output: filled in bsDevtListT struct
*           returns EOK on success otherwise
*           returns error from get_domain_disks()
*
*/
statusT
advfs_enum_domain_devts(
    char *domain,                /* in */
    bsDevtListT *DevtList)       /* out */
{
    bfDmnDescT *tmpDmnTbl;       /* table for get_domain_disks */
    statusT sts;
    int     i;

    tmpDmnTbl = (bfDmnDescT *)  ms_malloc(sizeof(bfDmnDescT));

    sts = get_domain_disks(domain, (uint64_t)0, tmpDmnTbl, NULL);
    if (sts != EOK ) {
        ms_free(tmpDmnTbl);
        return sts;
    }

    DevtList->devtCnt = tmpDmnTbl->vdCount;
    for (i=0; i < tmpDmnTbl->vdCount; i++) {
        DevtList->device[i] = (tmpDmnTbl->vddp[i])->device;
    }

    /* Free domain descriptor table and all of its vdDescT structures.*/
    free_vdds(tmpDmnTbl);
    ms_free(tmpDmnTbl);
    return EOK;
}

/*
 * free_vdds - deallocate the vd descriptor structs from the dmn
 * table.
 */
void
free_vdds(
    bfDmnDescT* dmnDescp)   /* in - ptr to bfdmn desc tbl */
{
    vdIndexT i;
    for ( i = 0; i < dmnDescp->vdCount; i++) {
        ms_free( dmnDescp->vddp[i] );
        dmnDescp->vddp[i] = NULL;
    }
}


/* Routine to see if a vdIndex describes a valid vdT structure, and, if so,
 * bumps the refcnt (if desired) on that vdT and returns its address.  
 * If it is not valid, NULL is returned, and obviously, vdRefCnt is not bumped.
 * Calling this function with bump_refcnt set to FALSE is handy when checking
 * if a vdIndex contains a valid vdT structure, but the caller is either
 * not going to use the structure or knows that there can be no racing rmvol
 * threads.  This avoids having to call vd_dec_refcnt().
 *
 * SMP - dmnP->vdpTblLock is used to guard the domain's vdpTbl[] array.
 *       vdp->vdStateLock is used to guard vdp->vdState and vdp->vdRefCnt.
 *       Neither lock is held on entry or exit.
 */
struct vd* 
vd_htop_if_valid(
    vdIndexT       vdi,             /* vdIndex to check */
    struct domain *dmnP,            /* domain */
    int            bump_refcnt,     /* if !FALSE, bump vdRefCnt */
    int            zombie_ok )      /* if TRUE, BSR_VD_ZOMBIE is OK*/
{
    ADVMTX_DOMAINVDPTBL_LOCK( &dmnP->vdpTblLock );
    if ( VDI_IS_VALID(vdi, dmnP) ) {
        vdT *vdp = dmnP->vdpTbl[vdi - 1];
        MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );
        ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
        ADVMTX_DOMAINVDPTBL_UNLOCK( &dmnP->vdpTblLock );

        /* vdp is considered valid if and only if one of the following is true:
         * 1. the state is BSR_VD_MOUNTED
         * 2. the state is (BSR_VD_VIRGIN, BSR_VD_ZOMBIE or BSR_VD_DMNT_INPROG)
         *    AND the caller is the thread setting up or removing the vdT.
         * 3. the state is BSR_VD_ZOMBIE,BSR_VD_DMNT_INPROG or 
         *    BSR_VD_VIRGIN AND zombie_ok is TRUE
         */
        if ( (vdp->vdState == BSR_VD_MOUNTED) ||
             ((vdp->vdSetupThd == u.u_kthreadp || zombie_ok) &&
              (vdp->vdState == BSR_VD_VIRGIN || 
               vdp->vdState == BSR_VD_ZOMBIE ||
               vdp->vdState == BSR_VD_DMNT_INPROG))) {
            /* If requested, bump the refcnt so that this vdT can't be
             * deallocated until all threads stop access to it.
             */
            if (bump_refcnt) {
                uint32_t dummy;
                /* Do not use the C language "++" or "--" syntax to change
                 * vdRefCnt because other code using the atomic increment
                 * macro states not to use that syntax.
                 */
                ATOMIC_FETCH_INCR(&vdp->vdRefCnt, &dummy);

            }
            ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );
            return vdp; 
        } else {
            /* This is not a valid vdp, so return NULL.  */
            ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );
            return (vdT *)(NULL);
        }
    } else {
        ADVMTX_DOMAINVDPTBL_UNLOCK( &dmnP->vdpTblLock );
        return (vdT *)(NULL);
    }
}

/* This routine is differs from vd_htop_if_valid() in that it assumes
 * and ASSERTs that the vdp is already valid. The caller must assure
 * this or call the former routine.  This routine is usually called
 * via the VD_HTOP() macro.
 */
struct vd * 
vd_htop_already_valid( vdIndexT vdi,          /* vdIndex to retrieve */
                       struct domain *dmnP,   /* domain */
                       int bump_refcnt )      /* if !FALSE, bump vdRefCnt */
{
    vdT *vdp = NULL;
    uint32_t dummy;

    /* We reduce lock contention by not seizing the dmnP->vdpTblLock in this
     * routine.  This is done since we call this routine only when we know
     * that this vdp cannot be removed.  
     */
    MS_SMP_ASSERT( VDI_IS_VALID(vdi, dmnP) ); 
    vdp = dmnP->vdpTbl[vdi - 1];
    MS_SMP_ASSERT( vdp );
    MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );

    if (bump_refcnt) {
      /* The vdp->vdStateLock normally protects the vdRefCnt change but
       * the locking is not necessary with this atomic increment.
       * But other places that change the value must not use the
       * the C language "++" or "--" syntax. Refer to macro's header file.
       */
        ATOMIC_FETCH_INCR(&vdp->vdRefCnt, &dummy);
    }

    return vdp; 
}

/* Routine to decrement the refcnt on a vdT structure.  This must be called
 * by a routine that has already called vd_htop_if_valid() or 
 * vd_htop_already_valid() with the bump_refcnt parameter as TRUE and 
 * got back a vdp pointer.  When the refcnt gets to zero, any waiters 
 * will be wakened.  Currently only a thread that is doing an rmvol on 
 * this disk will be waiting, and only one thread can be removing this volume.
 *
 * SMP - vdp->vdStateLock is used to guard vdp->vdState and vdp->vdRefCnt.
 *       This lock is not held on entry or exit.
 */
void
vd_dec_refcnt( vdT *vdp )
{

    uint32_t dummy;
    ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
    MS_SMP_ASSERT( vdp->vdRefCnt > 0 );
    /* Do not use the C language "++" or "--" syntax to change vdRefCnt
     * because other code using the atomic increment macro states not to
     * use that syntax.
     */
    ATOMIC_FETCH_DECR(&vdp->vdRefCnt, &dummy);

    if ( vdp->vdRefCnt == 0 && vdp->vdRefWaiters ) {
        MS_SMP_ASSERT( vdp->vdRefWaiters == 1 );   /* only 1 thread can wait */
        vdp->vdRefWaiters = 0;
        ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );
        cv_broadcast(&vdp->vdRefWaitersCV, NULL, CV_NULL_LOCK);
    }
    else
        ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );

    return;
}

/* end bs_domain.c */
