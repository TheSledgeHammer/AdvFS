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
 *      AdvFS - bs_domain.c
 *
 * Abstract:
 *
 *      This module contains routines for managing
 *      bitfile domains.
 *
 * Date:
 *
 *      Wed Jun  6 10:43:51 1990
 *
 */
/*
 * HISTORY
 * 
 * 
 * 
 * 
 */
#pragma ident "@(#)$RCSfile: bs_domain.c,v $ $Revision: 1.1.506.13 $ (DEC) $Date: 2007/05/30 15:28:01 $"

#define ADVFS_MODULE    BS_DOMAIN

#include <dirent.h>
#include <sys/types.h>
#include <sys/systm.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>
#include <msfs/bs_ods.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_migrate.h>
#include <msfs/msfs_syscalls.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/mount.h>
#include <sys/specdev.h>
#include <sys/buf.h>
#include <sys/conf.h>
#include <sys/ucred.h>
#include <sys/vnode.h>
#include <sys/prestoioctl.h>
#include <sys/lock_probe.h>
#include <sys/open.h>
#include <sys/versw.h>
#include <sys/clu.h>
#include <msfs/advfs_evm.h>

/*  This sentinel pointer is init'd to the first advfs domain
*   mounted.  It is used primarily for locating all domains
*   on the system.  Use this pointer as a starting point when marching
*   down the domainT->dmnFwd & domainT->dmnBwd linked lists and you
*   do not have a domainT available.
*/
domainT *DmnSentinelP = NULL;

/* Dynamic hash table for domain structures */
void *DomainHashTbl;
struct lockinfo *ADVDomainHashChainLock_lockinfo;

/* Count of existing domains.  This is still sync'd with
*  DmnTblMutex and is modified ONLY in dmn_alloc()
*  and dmn_dealloc().  domains included in this count may or
*  may not be active but are allocated.
*/
int DomainCnt = 0;

mutexT DmnTblMutex;              /* protects table and counter */
lock_data_t DmnTblLock;          /* syncs activation/deactivation */

struct bsMPg *Dpgp = NULL;
struct bsMPg *GRpgp = NULL;

vdDescT nilVdDesc = NULL_STRUCT;

decl_simple_lock_info(, ADVDmnTblMutex_lockinfo )
decl_simple_lock_info(, ADVdomainT_mutex_lockinfo )
decl_simple_lock_info(, ADVdomainT_lsnLock_lockinfo )
decl_simple_lock_info(, ADVdomainT_vdpTbl_lockinfo )
decl_simple_lock_info(, ADVdomainT_freeze_lockinfo )
decl_simple_lock_info(, ADVvdT_blockingQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_ubcReqQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_flushQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_waitLazyQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ0_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ1_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ2_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ3_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ4_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ5_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ6_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ7_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ8_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ9_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ10_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ11_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ12_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ13_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ14_ioQLock_info )
decl_simple_lock_info(, ADVvdT_smSyncQ15_ioQLock_info )
decl_simple_lock_info(, ADVvdT_tempQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_readyLazyQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_consolQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_devQ_ioQLock_info )
decl_simple_lock_info(, ADVvdT_vdIoLock_info )
decl_simple_lock_info(, ADVvdT_vdStateLock_info )
struct lockinfo * ADVvdT_del_list_lk_info;
struct lockinfo * ADVvdT_stgMap_lk_info;
struct lockinfo * ADVvdT_mcell_lk_info;
struct lockinfo * ADVvdT_rbmt_mcell_lk_info;
struct lockinfo * ADVvdT_ddlActiveLk_info;
struct lockinfo * ADVDmnTblLock_info;
struct lockinfo * ADVdomainT_scLock_info;
struct lockinfo * ADVdomainT_rmvolTruncLk_info;
struct lockinfo * ADVdomainT_xidRecoveryLk_info;
struct lockinfo * ADVBfSetTblLock_info;
#ifdef ADVFS_SMP_ASSERT
struct lockinfo *ADVdomainT_ftxSlotLock_lk_info;
#endif


#define  DMNTBL_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define DMNTBL_UNLOCK( sLk ) \
    lock_done( sLk );

/* flags for vd_remove */
#define VD_STATE_CHANGE         0x1
#define VD_REMOVE_PERM          0x10

/*
 * module internal PROTOTYPES
 */

statusT
bs_global_root_activate(
                        char* domainName,
                        u_long flag,
                        bfDomainIdT* bfDomainId
                        );

statusT
bs_bfdmn_sweep(
            struct domain* dmnP
            );

static statusT
update_bfsetid(
               domainT* dmnP,
               int vd,
               bfMCIdT mcid,
               bfDomainIdT domainId
               );

static statusT
set_vd_mounted(
               struct domain* dmnP,     /* IN - domain struct */
               struct vd* vdp           /* IN - vd struct */
               );

static statusT
get_raw_vd_attrs(
                 char *vdDiskName,              /* IN - disk device name */
                 domainT *dmnP,                 /* IN - Domain pointer */
                 int vdi,                       /* IN - vd index */
                 int flag,                      /* IN - flag */
                 struct vnode** vnodepp,        /* OUT - vnode ptr */
                 struct bsVdAttr** vdAttrpp,    /* OUT - vd attributes */
                 bsDmnAttrT** dmnAttrpp,        /* OUT - domain attributes */
                 bsDmnMAttrT** dmnMAttrpp,      /* OUT - log attributes */
                 int *dmnVersion,               /* OUT - domain version */
                 struct bsMPg *pgp              /* OUT - RBMT page 0 */
                 );

static statusT
setup_vd(
         domainT* dmnP,         /* IN - domain pointer */
         struct vnode* vnp,     /* IN - ptr to vnode */
         char* vdDiskName,      /* IN - name of disk */
         struct bsVdAttr* vdAttrp, /* IN - on disk vd attributes */
         u_long flag,           /* IN - mount flags/check DmnTblLock? */
         vdT** vdPtr            /* OUT - vd struct pointer */
         );

static vdDescT *
get_vdd(
        char *vdName,       /* in - virtual disk name */
        int *err            /* out - error */
        );

static int
get_domain_major(
                 char *domain,             /* in - domain name */
                 uint32T *dmnMajor         /* out - domain major number */
                 );

statusT
get_domain_disks(
                 char* domain,             /* in - domain name */
                 int doingRoot,            /* in - flag */
                 bfDmnDescT* dmnDescp      /* out - ptr to bfdmn desc */
                 );

void
free_vdds(
          bfDmnDescT* dmnDescp  /* in - ptr to bfdmn desc tbl */
          );

static statusT
vd_alloc(
         vdT **vdp,
         domainT *dmnP
         );

static void
vd_free(
        vdT *vdp,
        domainT *dmnP
        );

static int
vd_alloc_index(
               domainT *dmnP
               );

static
void
scan_rsvd_file_xtnt_map (
                         bfAccessT *bfAccess,  /* in */
                         vdIndexT vdIndex,  /* in */
                         int *retFoundFlag  /* out */
                         );

static
statusT
clear_bmt (
           domainT *dmnP,  /* in */
           vdIndexT vdIndex,  /* in */
           uint32T forceFlag, /* in */
           uint32T startBlk, /* in */
           uint32T numBlks  /* in */
           );

static
statusT
wait_for_ddl_active_entry (
                           domainT *domain,  /* in */
                           vdT *vd,  /* in */
                           bfMCIdT mcellId  /* in */
                           );
static
vd_remove(
          domainT* dmnP,  /* in */
          vdT* vdp,  /* in */
          int flags  /* in */
          );

static
statusT
set_disk_attrs (
                domainT *domain,  /* in */
                bsVdOpT op,  /* in */
                dev_t rdev  /* in */
                );
statusT
set_recovery_failed (
                     domainT *domain,  /* in */
                     int value  /* in */
                     );

static statusT
dmn_alloc(
          bfDomainIdT domainId,
          char *domainName,
          uint32T domainMajor,
          bfTagT dirTag,
          struct bsMPg *pgp,
          domainT **newdmnPP
          );

static void
dmn_dealloc(
            domainT *dmnP
            );

domainT *
domain_lookup(
              bfDomainIdT bfDomainId,    /* in - domain to lookup */
              u_long flag                /* in - mount flags/check DmnTblLock? */
              );

domainT *
domain_name_lookup(
                   char *dmnName,    /* in - domain to lookup */
                   u_long flag       /* in - mount flags/check DmnTblLock? */
                   );

void read_n_chk_last_rbmt_pg( vdT * );

/***************************************************/
/*****  Bitfile Domain access/close functions  *****/
/***************************************************/

#ifdef ADVFS_DOMAIN_TRACE
void
domain_trace( domainT *dmnP,
              uint16T module,
              uint16T line,
              void    *value)
{
    register dmnTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    dmnP->trace_ptr = (dmnP->trace_ptr + 1) % DOMAIN_TRACE_HISTORY;
    te = &dmnP->trace_buf[dmnP->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_DOMAIN_TRACE */

#ifdef ADVFS_VD_TRACE
void
vd_trace( vdT     *vdp,
          uint16T module,
          uint16T line,
          void    *value)
{
    register vdTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    vdp->trace_ptr = (vdp->trace_ptr + 1) % VD_TRACE_HISTORY;
    te = &vdp->trace_buf[vdp->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_VD_TRACE */

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
bs_domain_access( domainT **dmnPP,    /* out */
                  bfDomainIdT bfDomainId,  /* in */
                  int deactivated_ok)      /* in */
{
    statusT sts;

    mutex_lock( &DmnTblMutex );

    if( (*dmnPP = domain_lookup( bfDomainId, 0 )) == 0 ) {
        mutex_unlock( &DmnTblMutex );
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

    mutex_unlock( &DmnTblMutex );
    return sts;
}

/*
 * bs_domain_close
 *
 * This routine is called to close out use of a
 * domain access handle
 *
 * Returns EBAD_DOMAIN_HANDLE, or EOK
 */

void
bs_domain_close( domainT *dmnP )
{

    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);

    mutex_lock( &DmnTblMutex );

    dmnP->dmnAccCnt--;

    if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
        dmnP->dmnRefWaiters = 0;
        thread_wakeup((vm_offset_t)&dmnP->dmnRefWaiters);
    }

    mutex_unlock( &DmnTblMutex ); 

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
                     bfDomainIdT bfDomainId /* in - domain id */
                     )
{
    domainT *dmnP = NULL;
    void bs_kernel_init();

    /*
     * Make sure global locks and structure are set up before
     * trying to use the DmnTblLock.  We assume that if
     * we end up really doing AdvFS initialization, the
     * domain ID we are passed is is NOT the root domain.
     * That's because this function is only called in the 
     * msfs_real_syscall() path by AdvFS applications after 
     * the root has been mounted.  If the domain ID really
     * is the root filesystem, the call to bs_kernel_init()
     * would end up being a no-op anyway since the system
     * would have already initialized the data structures.
     */
    bs_kernel_init(FALSE);

    DMNTBL_LOCK_WRITE( &DmnTblLock )

    dmnP = domain_lookup( bfDomainId, 0 );

    if ((TEST_DMNP(dmnP) == EOK ) && (dmnP->state == BFD_ACTIVATED)) {
    
        /*
         * The domain is already active so simply return
         */

        dmnP->activateCnt++;

        DMNTBL_UNLOCK( &DmnTblLock )
        return EOK;

    } else {

        /*
         * The domain is not active so return an error.
         */
        DMNTBL_UNLOCK( &DmnTblLock )
        return ENO_SUCH_DOMAIN;
    }
}

/* bs_bfdmn_tbl_activate
 *
 * Take all steps necessary to activate the domain described by the
 * specified bitfile domain table.
 *
 * Returns status from smoke_bfdmn_tbl, get_raw_vd_attrs, setup_vd,
 * E_VD_BFDMNID_DIFF,
 * or EOK.
 *
 * currently flag is only used to signify a forced mount (ie
 * don't run recovery). The calling sequence is msfs_mount,
 * advfs_mountfs, bs_bfset_activate, bs_bfdmn_tbl_activate,
 * bd_bfdmn_activate, lgr_open.
 */
statusT
bs_bfdmn_tbl_activate(
                      char* domainName,       /* in - bf domain name */
                      u_long flag,            /* in - flag */
                      bfDomainIdT* bfDomainId /* out - domain id */
                      )
{
    statusT sts;
    bfDmnDescT *dmntbl;
    bfDomainIdT domainId;
    bfDomainIdT dualMountId;
    int closeVd, dmnTblLocked, error, vdi, id;
    int unlink_and_restart, isMount;
    u_long isRoot;
    vdT *vdp;
    domainT *dmnP;
    bsDmnMAttrT dmnMAttr;
    vdIndexT dmnMAttrIdx;
    struct vnode* vnp;
    struct nameidata *ndp;
    extern int AdvfsFixUpSBM;
    int dmnVersion;
    char *volPathName;
    void bs_kernel_init();
    bfTagT bfTag;

start:
    /* Initialize variables */
    dmntbl = (bfDmnDescT *)NULL;
    domainId = nilBfDomainId;
    dualMountId = nilBfDomainId;
    closeVd = FALSE;
    dmnTblLocked = FALSE;
    isRoot = flag & (M_LOCAL_ROOT | M_GLOBAL_ROOT | M_GLROOT_OTHER);
    isMount = flag & M_MSFS_MOUNT;
    bzero(&dmnMAttr, sizeof(bsDmnMAttrT));
    dmnMAttrIdx = 0;
    vnp = NULL;
    ndp = (struct nameidata *)&u.u_nd;
    unlink_and_restart = FALSE;
    volPathName = NULL;

    if (flag & M_GLOBAL_ROOT) {
        sts = bs_global_root_activate(domainName, flag, bfDomainId);
        return sts;
    }

    bs_kernel_init(isRoot);

    if(!(flag & M_GLROOT_OTHER)) {
        DMNTBL_LOCK_WRITE( &DmnTblLock )
        dmnTblLocked = TRUE;
    }

    /*
     * Check if the domain is already mounted.
     */

    dmnP = domain_name_lookup( domainName, flag );

    if ((TEST_DMNP(dmnP) == EOK) && (dmnP->state == BFD_ACTIVATED)) {

        /*
         * The domain is already active so simply return its domain ID.
         */

        *bfDomainId = dmnP->domainId;
        dmnP->activateCnt++;
        if (dmnTblLocked)
            DMNTBL_UNLOCK( &DmnTblLock )
        return EOK;
    }
    
    dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );
    if( dmntbl == NULL ) {
        if (dmnTblLocked)
            DMNTBL_UNLOCK( &DmnTblLock )
        return ENO_MORE_MEMORY;
    }

    /*
     * Get the list of disks in this domain into a table.
     */

    sts = get_domain_disks( domainName, isRoot, dmntbl);
    if ( sts != EOK ) {
        goto get_table_error;
    }

    /*
     * Loop through all disks described by the table.
     */

    for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
        struct bsVdAttr* vdAttrp;
        bsDmnAttrT* dmnAttrp;
        bfDmnStatesT dmnState;
        vdDescT* vdd = dmntbl->vddp[vdi];
        bsDmnMAttrT *dmnMAttrp;

        sts = get_raw_vd_attrs(vdd->vdName,
                               dmnP,
                               vdi,
                               isRoot|isMount,
                               &vnp,
                               &vdAttrp,
                               &dmnAttrp,
                               &dmnMAttrp,
                               &dmnVersion,
                               Dpgp);
        if ( sts != EOK ) {
            goto get_attrs_error;
        }

        if ( vdAttrp->state == BSR_VD_VIRGIN ) {
            dmnState = BFD_VIRGIN;
        } else if ( vdAttrp->state == BSR_VD_MOUNTED ) {
            dmnState = BFD_ACTIVATED;
        } else {
            dmnState = BFD_DEACTIVATED;
        }

        if ( BS_UID_EQL( domainId, nilBfDomainId) ) {
            domainT *n_dmnP;

            domainId = dmnAttrp->bfDomainId;

            /*
             * If the domain is already mounted by CFS in the cluster
             * and this is not a dual mount, fail the activation.
             */
            if (clu_is_ready() && !(flag & M_FAILOVER)) {
                if (CC_FIND_CLUSTER_DOMAIN_ID(&domainId)) {
                    if (!(flag & M_DUAL)) {
                        sts = E_DOMAIN_ALREADY_EXISTS;
                        goto vd_add_error;
                    }
                }
            }

            if ((n_dmnP = domain_lookup( domainId, flag )) == 0) {

                /* domain not cached, allocate one */
                /*
                 * NOTE: We pass a "static" root tag directory tag to
                 * dmn_alloc(). This allows us to move the root tag directory
                 * without tracking down each accessed bitfile set and updating
                 * its bitfile set id.  Also, we don't have to update each
                 * bitfile set's "bfSetTag" mcell field.
                 */

                if ((flag & M_DUAL) &&
                   !(clu_is_ready() && (flag & M_FAILOVER))) {

                    struct timeval ltime;
                    dualMountId = domainId;
                    TIME_READ(ltime);
                    domainId = ltime;
                }
                sts = dmn_alloc( domainId,
                                 domainName,
                                 dmntbl->dmnMajor,
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

                if ((flag & M_DUAL) &&
                   !(clu_is_ready() && (flag & M_FAILOVER))) {

                    struct timeval ltime;
                    dualMountId = domainId;
                    TIME_READ(ltime);
                    domainId = ltime;
                    sts = dmn_alloc( domainId,
                                 domainName,
                                 dmntbl->dmnMajor,
                                 staticRootTagDirTag,
                                 Dpgp,
                                 &dmnP );
                    if (sts != EOK) {
                        goto vd_add_error;
                    }
                    dmnP->dualMountId = dualMountId;
                }
                else {
                    sts = E_DOMAIN_ALREADY_EXISTS;
                    goto vd_add_error;
                }
            }

            DOMAIN_TRACE(dmnP, 0);

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
            if (flag & M_DUAL) {
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

            /*
             * If this vd's state is not BSR_VD_DISMOUNTED, then make sure
             * the vdMntId matches. Also, set the domain's state to reflect
             * this vd's state
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

            if (dmnP->dmnVersion != dmnVersion) {
                sts = E_INVALID_FS_VERSION;
                goto vd_add_error;
            }
        }

        if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
            vdAttrp->state = BSR_VD_DMNT_INPROG;
            sts = write_raw_bmt_page(vnp, MSFS_RESERVED_BLKS, Dpgp);
            if (sts != EOK) {
                goto vd_add_error;
            }
        }
       /* 
        * Load domain attribute parameter to domain table.
        */

        dmnP->maxVds = dmnAttrp->maxVds;  
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
                       flag,
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
        ms_uprintf("Domain %s is missing the log volume.\n",
                domainName);
        ms_uprintf("Check /etc/fdmns/%s for missing links before mounting.\n",
                domainName);
        sts = E_VD_DMNATTR_DIFF;
        goto get_attrs_error;
    }

    /*
     * check the vd count. If wrong, don't mount the domain.
     */
    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        volPathName = ms_malloc( MAXPATHLEN+1 );
        if (volPathName == NULL) {
            sts = ENOMEM;
            goto get_attrs_error;
        }
        sts = check_trans_attrs(dmnP, 
                                dmnMAttrIdx, 
                                &(dmnMAttr.vdCnt), 
                                volPathName);
        if (sts == E_TOO_MANY_VIRTUAL_DISKS) {
            /*
             * check_trans_attrs found trails of an interrupted
             * rmvol (/etc/fdmns link name in volPathName). Attempt
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
        ms_uprintf("Volume count mismatch for domain %s. %s expects %d volumes,\n",
                       domainName, domainName, dmnMAttr.vdCnt);
        ms_uprintf("/etc/fdmns/%s has %d links.\n", domainName, dmnP->vdCnt);
        sts = E_VOLUME_COUNT_MISMATCH;
        goto get_attrs_error;
    }

    /*
     * Clear trans attr record. 
     */

    sts = clear_trans_attrs(dmnP, dmnMAttrIdx); 
    if ( sts != EOK ) {
        goto get_attrs_error;
    }

    /*
     * If the recoveryFailed flag is set in the mutable domain
     * attributes, and AdvfsFixUpSBM is not set, then if it is
     * the root domain, allow the mount to proceed, but output
     * a warning and an EVM message.  Otherwise, if not the root,
     * fail the mount.
     */
    if (!AdvfsFixUpSBM && dmnMAttr.recoveryFailed) {
        if (isRoot) {
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

    /*
     * TODO:  There needs to be a lock to prevent anyone from accessing
     * or activating this set until this activate is completed.  Should
     * be a per-domain state lock?!
     */

    sts = bs_bfdmn_activate( domainId, flag );

    if ( sts != EOK ) {
        goto get_attrs_error;
    }

    /*
     * Clear recovery failed flag unconditionally if set at this
     * point (recovery completed).
     */
    if (dmnMAttr.recoveryFailed) {
        set_recovery_failed(dmnP, 0);
    }

    if (dmnTblLocked)
        DMNTBL_UNLOCK( &DmnTblLock )
    dmnP->dmnFlag = BFD_NORMAL; /* Clear all bits */
    *bfDomainId = domainId;
    if (volPathName != NULL) {
        ms_free( volPathName );
    }
    free_vdds( dmntbl );
    ms_free( dmntbl );
    return EOK;

vd_add_error:
    (void)setmount( vnp, SM_CLEARMOUNT );
    if (clu_is_ready() && !(isMount)) {
        CC_DEV_DLM_UNLOCK(vnp->v_rdev);
    }
    VOP_CLOSE( vnp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    /* Nothing done with the error return above now */
    vrele( vnp );

get_attrs_error:
    if (closeVd && (TEST_DMNP(dmnP) == EOK)) {

        /* Close volumes that were opened above */

        statusT temp_sts;
        
        for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
                /*
                 * reclaim any old reserved file CFS vnodes that are
                 * hanging around on vnode free list for any of the
                 * reserved files on the vd
                 */
                temp_sts = bs_reclaim_cfs_rsvd_vn(vdp->sbmp);
                if (temp_sts != EOK)
                    continue;

                if ( RBMT_THERE(dmnP) ) {
                    temp_sts = bs_reclaim_cfs_rsvd_vn (vdp->rbmtp);
                    if (temp_sts != EOK)
                        continue;
                }

                temp_sts = bs_reclaim_cfs_rsvd_vn (vdp->bmtp);
                if (temp_sts != EOK)
                    continue;

                if (clu_is_ready() && !(isMount)) {
                        CC_DEV_DLM_UNLOCK(vdp->devVp->v_rdev);
                }
                vd_remove(dmnP, vdp, 0);
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
        DMNTBL_UNLOCK( &DmnTblLock )
    }

    if (dmntbl != NULL) {
        free_vdds( dmntbl );
        ms_free( dmntbl );
    }

    if (unlink_and_restart) {
        char *rem_vol;

        ndp->ni_nameiop = DELETE | WANTPARENT;
        ndp->ni_segflg = UIO_SYSSPACE;
        ndp->ni_dirp = volPathName;

        rem_vol = ((char *)strrchr (volPathName, '/'));
        MS_SMP_ASSERT(rem_vol != NULL);
        
        ms_uprintf("Interrupted rmvol of volume %s in domain %s.\n", 
                rem_vol+1, domainName);
        ms_uprintf("    To finish rmvol cleanup, type the command:\n");
        ms_uprintf("    /sbin/disklabel -sF %s unused\n", rem_vol+1);

        if ( (sts = (statusT)namei(ndp)) != EOK ) {
            ms_free( volPathName );
            return sts;
        }
        VOP_REMOVE(ndp, error);
        ms_free( volPathName );
        if (error) {
            return (statusT)error;
        }
        goto start;
    }

    if (volPathName != NULL)
        ms_free( volPathName );

    return sts;
}  /* end bs_bfdmn_tbl_activate */

statusT
bs_global_root_activate(
                        char* domainName,       /* in - bf domain name */
                        u_long flag,            /* in - flag */
                        bfDomainIdT* bfDomainId /* out - domain id */
                        )
{
    statusT sts;
    bfDmnDescT *dmntbl;
    bfDomainIdT domainId;
    int closeVd, error, vdi, id, restart;
    int update_global_rootdev;
    vdT *vdp;
    domainT *dmnP;
    bsDmnMAttrT dmnMAttr;
    vdIndexT dmnMAttrIdx;
    struct vnode* vnp;
    struct nameidata *ndp;
    extern int AdvfsFixUpSBM;
    extern dev_t global_rootdev[];
    extern uint32T global_rootdev_count;
    extern struct vnode **GlobalRootVpp;
    extern dev_t global_rootdev_opinprog;
    int dmnVersion;
    void bs_kernel_init();
    bsMPgT *bmtPage;
    bfPageRefHT pgPin;
    struct bfAccess *mdap;
    bsDmnTAttrT *dmnTAttr;
    int mcell_index;
    bfTagT bfTag;

start:
    /* Initialize variables */
    dmntbl = (bfDmnDescT *)NULL;
    domainId = nilBfDomainId;
    closeVd = FALSE;
    restart = FALSE;
    update_global_rootdev = -1;
    bzero(&dmnMAttr, sizeof(bsDmnMAttrT));
    dmnMAttrIdx = 0;
    vnp = NULL;
    ndp = (struct nameidata *)&u.u_nd;

    bs_kernel_init(M_GLOBAL_ROOT);

    /*
     * Don't lock the domain table for global root. DLM locks in CFS
     * prevent multiple global root activations. Taking this lock out
     * for root causes deadlocks during global root failover.
     */

    /*
     * Check if the domain is already mounted.
     */

    dmnP = domain_name_lookup( domainName, flag );

    if ((TEST_DMNP(dmnP) == EOK) && (dmnP->state == BFD_ACTIVATED)) {
        /*
         * The domain is already active so simply return its domain ID.
         */
        *bfDomainId = dmnP->domainId;
        dmnP->activateCnt++;
        return EOK;
    }
    
    dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );

    if( dmntbl == NULL ) {
        return ENO_MORE_MEMORY;
    }

    /*
     * Get the list of disks in this domain into a table.
     */

    sts = get_domain_disks( domainName, M_GLOBAL_ROOT, dmntbl);

    if ( sts != EOK ) {
        goto get_table_error;
    }

    /*
     * Loop through all disks described by the table.
     */

    for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
        struct bsVdAttr* vdAttrp;
        bsDmnAttrT* dmnAttrp;
        bfDmnStatesT dmnState;
        vdDescT* vdd = dmntbl->vddp[vdi];
        bsDmnMAttrT *dmnMAttrp;

        sts = get_raw_vd_attrs(vdd->vdName,
                               dmnP,
                               vdi,
                               M_GLOBAL_ROOT|M_MSFS_MOUNT,
                               &vnp,
                               &vdAttrp,
                               &dmnAttrp,
                               &dmnMAttrp,
                               &dmnVersion,
                               GRpgp);
        if ( sts != EOK ) {
            /*
             * Handle discrepancy between cluster database and
             * on-disk information.
             */
            if (global_rootdev[vdi] == global_rootdev_opinprog) {
                update_global_rootdev = vdi;
                global_rootdev_opinprog = (dev_t)NULL;
                continue;
            } else                      /* don't panic here; let CFS */
                goto get_attrs_error;   /* failover handle the error */
        }

        if ( vdAttrp->state == BSR_VD_VIRGIN ) {
            dmnState = BFD_VIRGIN;
        } else if ( vdAttrp->state == BSR_VD_MOUNTED ) {
            dmnState = BFD_ACTIVATED;
        } else {
            dmnState = BFD_DEACTIVATED;
        }

        if ( BS_UID_EQL( domainId, nilBfDomainId) ) {
            domainT *n_dmnP;

            domainId = dmnAttrp->bfDomainId;

            /*
             * If the domain is already mounted by CFS in the cluster
             * fail the activation.
             */
            if (clu_is_ready() && !(flag & M_FAILOVER)) {
                if (CC_FIND_CLUSTER_DOMAIN_ID(&domainId)) {
                    sts = E_DOMAIN_ALREADY_EXISTS;
                    goto vd_add_error;
                }
            }

            if ((n_dmnP = domain_lookup( domainId, flag )) == 0) {

                /*
                 * NOTE: We pass a "static" root tag directory tag to
                 * dmn_alloc(). This allows us to move the root tag directory
                 * without tracking down each accessed bitfile set and updating
                 * its bitfile set id.  Also, we don't have to update each
                 * bitfile set's "bfSetTag" mcell field.
                 */

                sts = dmn_alloc( domainId,
                                 domainName,
                                 dmntbl->dmnMajor,
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

            DOMAIN_TRACE(dmnP, 0);

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

            if ( !BS_UID_EQL( domainId, dmnAttrp->bfDomainId )) {
                if (global_rootdev[0] == global_rootdev_opinprog) {
                    goto cleanup_global_rootdev;
                } else if (global_rootdev[vdi] == global_rootdev_opinprog) {
                    update_global_rootdev = vdi;
                    global_rootdev_opinprog = (dev_t)NULL;
                    continue;
                } else {
                    int i=0;
                    ms_uaprintf("dmnId=%x, vdId=%x, vdi=%d, vdcnt=%d, rootcnt=%d, op=%x\n",
                            domainId,
                            dmnAttrp->bfDomainId,
                            vdi,
                            dmntbl->vdCount,
                            global_rootdev_count,
                            global_rootdev_opinprog);
                    for(i=0; i<dmntbl->vdCount; i++) {
                        ms_uaprintf("%s, %d, %x, %x\n",
                            dmntbl->vddp[i]->vdName,
                            dmntbl->vddp[i]->serviceClass,
                            dmntbl->vddp[i]->device,
                            global_rootdev[i]);
                    }
                    ADVFS_SAD0("bs_bfdmn_tbl_activate: domain id mismatch for global root");
                }
            }

            /*
             * If this vd's state is not BSR_VD_DISMOUNTED, then make sure
             * the vdMntId matches. Also, set the domain's state to reflect
             * this vd's state
             */

            if (vdAttrp->state != BSR_VD_DISMOUNTED) {
                if (vdAttrp->state != BSR_VD_DMNT_INPROG) {
                    if ( !BS_UID_EQL( dmnP->bfDmnMntId, vdAttrp->vdMntId )) {
                        if (global_rootdev[0] == global_rootdev_opinprog) {
                            goto cleanup_global_rootdev;
                        } else if (global_rootdev[vdi] == global_rootdev_opinprog) {
                            update_global_rootdev = vdi;
                            global_rootdev_opinprog = (dev_t)NULL;
                            continue;
                        } else {
                            int i=0;
                            ms_uaprintf("dmnId=%x, vdId=%x, vdi=%d, vdcnt=%d, rootcnt=%d, op=%x\n",
                                    dmnP->bfDmnMntId,
                                    vdAttrp->vdMntId,
                                    vdi,
                                    dmntbl->vdCount,
                                    global_rootdev_count,
                                    global_rootdev_opinprog);
                            for(i=0; i<dmntbl->vdCount; i++) {
                                ms_uaprintf("%s, %d, %x, %x\n",
                                    dmntbl->vddp[i]->vdName,
                                    dmntbl->vddp[i]->serviceClass,
                                    dmntbl->vddp[i]->device,
                                    global_rootdev[i]);
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

            if (dmnP->dmnVersion != dmnVersion) {
                if (global_rootdev[0] == global_rootdev_opinprog) {
                    goto cleanup_global_rootdev;
                } else if (global_rootdev[vdi] == global_rootdev_opinprog) {
                    update_global_rootdev = vdi;
                    global_rootdev_opinprog = (dev_t)NULL;
                    continue;
                } else {
                    ADVFS_SAD0("bs_bfdmn_tbl_activate: incorrect domain version for global root");
                }
            }
        }

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
                       flag,
                       &vdp );
        if ( sts != EOK ) {
            if (global_rootdev[vdi] == global_rootdev_opinprog) {
                update_global_rootdev = vdi;
                global_rootdev_opinprog = (dev_t)NULL;
                continue;
            } else {
                ADVFS_SAD0("bs_bfdmn_tbl_activate: can't set vd for global root");
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
        ms_uprintf("Domain %s is missing the log volume.\n", domainName);
        ms_uprintf("Check /etc/fdmns/%s for missing links before mounting.\n",
                domainName);
        sts = E_VD_DMNATTR_DIFF;
        goto get_attrs_error;
    }

    /*
     * check the vd count. If wrong, don't mount the domain.
     */

    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        int inprog_idx, vd_idx, vd_count = 0;

        for (inprog_idx = 0; inprog_idx < dmntbl->vdCount; inprog_idx++) {
            if (global_rootdev[inprog_idx] == global_rootdev_opinprog) {
                update_global_rootdev = inprog_idx;
                for (vd_idx = 1; 
                     vd_count < dmnP->vdCnt && vd_idx <= BS_MAX_VDI; 
                     vd_idx++) {
                    if (vdp = vd_htop_if_valid(vd_idx, dmnP, FALSE, FALSE)) {
                        if (vdp->devVp->v_rdev == global_rootdev_opinprog) {
                            vd_remove(dmnP, vdp,
                                      VD_STATE_CHANGE | VD_REMOVE_PERM);
                            break;
                        }
                        vd_count++;
                    }
                }
                global_rootdev_opinprog = (dev_t)NULL;
                break;
            }
        }
    }

    /*
     * Recheck vd count, in case it got changed by the above.
     */
    if (dmnP->vdCnt != dmnMAttr.vdCnt) {
        ms_uprintf("Volume count mismatch for domain %s. %s expects %d volumes,\n",
                       domainName, domainName, dmnMAttr.vdCnt);
        ms_uprintf("/etc/fdmns/%s has %d links.\n", domainName, dmnP->vdCnt);
        sts = E_VOLUME_COUNT_MISMATCH;
        goto get_attrs_error;
    }

    if (update_global_rootdev != -1) {
        /*
         * Found resolvable volume conflicts for cluster global root.
         * Update the global rootdev array to reflect this.
         */
        global_rootdev_count--;
        if (update_global_rootdev != global_rootdev_count) {
            for (vdi = update_global_rootdev; vdi < global_rootdev_count;
                vdi++) {
                global_rootdev[vdi] = global_rootdev[vdi+1];
                GlobalRootVpp[vdi] = GlobalRootVpp[vdi+1];
            }
        }
        global_rootdev[global_rootdev_count] = (dev_t)NULL;
    }

    /*
     * dmnP and CFS vol data has been reconciled. /etc/fdmns will be
     * verified at mount update time. Check to see if domain's transient
     * record indicates that an addvol was interrupted. If so, remove
     * this volume out of the service class so that no storage allocations
     * are done from this "suspect" volume. If all the information is
     * ok, this will be added back into the service class in
     * bs_fix_root_dmn (called at mount update).
     */

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vdp->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }

    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);

    if (sts != EOK) {
        goto get_attrs_error;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
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
    if (!AdvfsFixUpSBM && dmnMAttr.recoveryFailed) {
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

    sts = bs_bfdmn_activate( domainId, flag );

    if ( sts != EOK ) {
        goto get_attrs_error;
    }

    /*
     * Clear recovery failed flag unconditionally if set at this
     * point (recovery completed).
     */
    if (dmnMAttr.recoveryFailed) {
        set_recovery_failed(dmnP, 0);
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
        ADVFS_SAD0("bs_bfdmn_tbl_activate: metadata conflicts for global root");
    }
    global_rootdev_opinprog = (dev_t)NULL;
    global_rootdev_count--;
    for (id = 0; id < global_rootdev_count; id++) {
        global_rootdev[id] = global_rootdev[id+1];
        GlobalRootVpp[id] = GlobalRootVpp[id+1];
    }
    global_rootdev[global_rootdev_count] = (dev_t)NULL;
    restart = TRUE;
    (void)setmount( vnp, SM_CLEARMOUNT );
    VOP_CLOSE( vnp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    goto get_attrs_error;

vd_add_error:
    (void)setmount( vnp, SM_CLEARMOUNT );
    VOP_CLOSE( vnp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    /* Nothing done with the error return above now */
    vrele( vnp );

get_attrs_error:
    if (closeVd && (TEST_DMNP(dmnP) == EOK)) {

        /* Close volumes that were opened above */

        statusT temp_sts;

        for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
                /*
                 * reclaim any old reserved file CFS vnodes that are
                 * hanging around on vnode free list for any of the
                 * reserved files on the vd
                 */
                temp_sts = bs_reclaim_cfs_rsvd_vn(vdp->sbmp);
                if (temp_sts != EOK)
                    continue;

                if ( RBMT_THERE(dmnP) ) {
                    temp_sts = bs_reclaim_cfs_rsvd_vn (vdp->rbmtp);
                    if (temp_sts != EOK)
                        continue;
                }

                temp_sts = bs_reclaim_cfs_rsvd_vn (vdp->bmtp);
                if (temp_sts != EOK)
                    continue;

                vd_remove(dmnP, vdp, 0);
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


#ifdef ADVFS_SMP_ASSERT

/*
 * check_closed_list
 *
 * This function searches the closed list for any remaining bfaps
 * and reports the first bfap belonging to the domain that it finds.
 * If none belong to the domain it returns NULL.
 *
 */
bfAccessT*
check_closed_list(
                 domainT *dmnP   /* in */
                 )
{
    bfAccessT *badBfap = NULL;
    bfAccessT *bfap;

    mutex_lock(&BfAccessFreeLock);

    bfap = ClosedAcc.freeFwd;
    while (bfap != (bfAccessT *)(&ClosedAcc)) {
        if (bfap->dmnP == dmnP) {
            mutex_unlock(&BfAccessFreeLock);
            return (bfap);
        }        
        bfap = bfap->freeFwd;
    }
    
    mutex_unlock(&BfAccessFreeLock);
    return (NULL);
}
#endif /* ADVFS_SMP_ASSERT */


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
 * Returns ENOMEM, E_TOO_MANY_ACCESSORS, or EOK via bs_reclaim_cfs_rsvd_vn()
 */

statusT
bs_bfdmn_deactivate(
                    bfDomainIdT domainId,
                    u_long flag
                    )
{
    int vdi;
    domainT* dmnP;
    vdT* vdp;
    int rem, tblLocked = FALSE;
    statusT sts = EOK;
    bfSetT *bfSetp;
    int vdCnt;
    bfTagT bfTag;
    int s;
    int isMount = flag & M_MSFS_MOUNT;
    bfAccessT *badBfap;

    /*
     * Don't lock the domain table for global root. DLM locks in CFS
     * prevent multiple global root deactivations. Taking this lock out
     * for root causes deadlocks during global root failover.
     */
    if (!(flag & M_GLOBAL_ROOT)) {
        DMNTBL_LOCK_WRITE( &DmnTblLock )
        tblLocked = TRUE;
    }

    if( (dmnP = domain_lookup( domainId, flag )) == NULL ) {
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
    ss_put_rec (dmnP);  /* ignore error, must not hold up ss deactivate */

    /*
     * Wait for dmnAccCnt threads to finish. Set BFD_DEACTIVATE_PENDING
     * for transaction code to not trip over the dmnP->state  After this, no new
     * activity should come into the domain except for the bs_io_thread(),
     * which may be needed later in deactivation processing.
     */

    mutex_lock( &DmnTblMutex );
    dmnP->dmnFlag |= BFD_DEACTIVATE_PENDING;
    /* Drop and reaquire lock across ss_dmn_deactivate, as function 
     * can block waiting for vfast threads to complete.
     */
    mutex_unlock( &DmnTblMutex );
    /* Also forces any pending migrates to abort.  The dmnAccCnt is
     * held by vfast migrate thd and is released when the thread
     * aborts. The deactivation thread in this routine will block and
     * wait for vfast to perform a domain_close and force vfast's
     * portion of the dmnAccCnt to go to 0.
     */
    ss_dmn_deactivate(dmnP, TRUE);
    mutex_lock( &DmnTblMutex );
    dmnP->state = BFD_DEACTIVATED;
    if (dmnP->dmnAccCnt > 0) {
        dmnP->dmnRefWaiters++;
        assert_wait_mesg( (vm_offset_t)&dmnP->dmnRefWaiters, FALSE,
            "dmnAccCnt" );
        mutex_unlock( &DmnTblMutex );
        thread_block();
    } else {
        mutex_unlock( &DmnTblMutex );
    }

    /*
     * Set dmnP->dmnFlag to BFD_DEACTIVATE_IN_PROGRESS.  This is necessary
     * for underlyling transaction/log code to work, now that
     * dmnP->state is BFD_DEACTIVATED.  Transactions may take place 
     * in cleanup thread, lgr_checkpoint_log(), and possibly elsewhere.
     */

    dmnP->dmnFlag = (dmnP->dmnFlag & (~(BFD_DEACTIVATE_PENDING))) 
        | BFD_DEACTIVATE_IN_PROGRESS;

    /*
     * Loop and check all virtual disks for user accessed access structs.
     * The vd_remove() below will actually perform a remove on all these
     * reserved files for this domain.
     */

    vdi = 1;

    while ((vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0)) {
        if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {

            /* reclaim any old reserved CFS vnodes that are
             * hanging around on vnode free list for any of the
             * reserved files on the vd
             */
            sts = bs_reclaim_cfs_rsvd_vn(vdp->sbmp);
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }

            if ( RBMT_THERE(dmnP) ) {
                sts = bs_reclaim_cfs_rsvd_vn (vdp->rbmtp);
                if (sts != EOK) {
                    RAISE_EXCEPTION( sts );
                }
            }

            sts = bs_reclaim_cfs_rsvd_vn (vdp->bmtp);
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }
        }
        vdi++;
    }

    /*
     * Instead of calling bs_bfdmn_flush<_sync>, issue a log checkpoint.
     * This will flush all of the domain's buffers as well as trim the
     * log. After this, the log will have just one record - for the
     * exclusive transaction started by the checkpoint code. Since
     * all of the metadata buffers are flushed at this point, we need to
     * trim the log so that the old log records are not sitting around 
     * (to be reprocessed at next domain activation). 
     */

    lgr_checkpoint_log(dmnP);
    
    /* close the tag directory */

    sts = bs_close(dmnP->bfSetDirAccp, 0);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_bfdmn_deactivate: bs_close of tag dir failed", sts );
    }

    dmnP->bfSetDirAccp = NULL;

    /*
     * Loop and flush extent caches.
     */

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        /* The DmnTblLock prevents racing addvol/rmvol; no need to bump
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
     * is OK since we have the DmnTblLock and no racing threads can add or
     * remove disks while we go thru this loop.
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
     * this should not fail because vd is inaccessible
     */
    for (vdi = 1; (vdi <= BS_MAX_VDI) && (dmnP->vdCnt > 0); vdi++)
        if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
            /*
             * In the cluster, for non-mount paths we release the dlm device locks.
             */
            if (clu_is_ready() && !(isMount)) {
                sts = CC_DEV_DLM_UNLOCK(vdp->devVp->v_rdev);
                if (sts != EOK)
                    ADVFS_SAD2("bs_bfdmn_deactivate: could not unlock device %lx failure code %d \n",vdp->devVp->v_rdev, sts);
            }
            vd_remove(dmnP, vdp, VD_STATE_CHANGE);
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

    MS_SMP_ASSERT((badBfap = check_closed_list(dmnP)) == NULL);

HANDLE_EXCEPTION:
    if (tblLocked) {
        DMNTBL_UNLOCK( &DmnTblLock )
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
                  bfDomainIdT domainId,   /* in */
                  u_long flag             /* in */
                  )
{
    int vdi, vdcnt, done, s;
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

    MS_SMP_ASSERT((flag & M_GLOBAL_ROOT) ? 1 : lock_holder(&DmnTblLock));
    if( (dmnP = domain_lookup( domainId, flag )) == 0 ) {
        return( ENO_SUCH_DOMAIN );
    }

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
                    (flag & M_FMOUNT) ? TRUE : FALSE );

    if (sts != EOK) {
        ms_uprintf( "bs_bfdmn_activate: can't open log for domain '%s',\ntag = 0x%x.0x%x: %s\n",
                dmnP->domainName, dmnP->ftxLogTag.num, dmnP->ftxLogTag.seq, BSERRMSG( sts ) );
        return( sts );
    }

    dmnP->logAccessp = logbfap;
    logOpen = TRUE;

    /* assert that the log has valid in-mem structures */
    MS_SMP_ASSERT(dmnP->logAccessp->xtnts.validFlag);
    /* then proceed to figure out which RAD the log's vd struct is on */
    mutex_lock( &dmnP->vdpTblLock );
    dmnP->logVdRadId = PA_TO_MID(vtop(current_processor(), 
                                 dmnP->vdpTbl[dmnP->logAccessp->xtnts.xtntMap->subXtntMap->vdIndex-1]));
    mutex_unlock( &dmnP->vdpTblLock );

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
                       "tag = 0x%x.0x%x: %s",
                       dmnP->bfSetDirTag.num, dmnP->bfSetDirTag.seq,
                       BSERRMSG( sts ) );
            goto _error_cleanup;
        }
        tagDirOpen = TRUE;

        dmnP->bfSetDirAccp = bfSetDirAccp;
        bfSetp = dmnP->bfSetDirp;
        bfSetp->dirBfAp = dmnP->bfSetDirAccp;
        bfSetp->fsRefCnt++;
        setDesc = TRUE;

        sts = tagdir_get_info( bfSetp, &bfSetp->tagUnInPg,
                               &bfSetp->tagUnMpPg, &bfSetp->tagFrLst,
                               &bfSetp->bfCnt
                             );
        if (sts != EOK) {
            goto _error_cleanup;
        }
    }

    /*
     * If mounting root FS, decide whether we should delay writes by finding
     * out the size of the log.
     */
    if (flag & (M_GLOBAL_ROOT | M_LOCAL_ROOT)) {
        if (num_of_log_recs(dmnP, LIMIT_RECOV_DELAY) < LIMIT_RECOV_DELAY)
        MountWriteDelay = 1;
    }

    if (dmnP->state != BFD_VIRGIN) {
        if ( (dmnP->dmnVersion >= FIRST_ALWAYS_RUN_RECOVERY_VERSION) ||
             (dmnP->state != BFD_DEACTIVATED) ) {

            /* 
             * Starting with AdvFS On-disk version 4, we do crash
             * recovery even for BFD_DEACTIVATED state.
             */

            sts = ftx_bfdmn_recovery( dmnP );
            if (sts != EOK) {
                goto _error_cleanup;
            }
        }
    }

    /*
     * If this is dual mount case, sweep the disks and write new
     * domain id.
     */
    if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
        sts = bs_bfdmn_sweep(dmnP);
        if (sts != EOK)
            goto _error_cleanup;
        dmnP->dmnFlag &= ~BFD_DUAL_MOUNT_IN_PROGRESS;
    }

    /*
     * The disks and domain are in a consistent state so finish the
     * mounting process.
     */

#ifdef __arch32__
    dmnP->totalBlks.low = 0;
    dmnP->totalBlks.high = 0;
    dmnP->freeBlks.low = 0;
    dmnP->freeBlks.high = 0;
#else  /* __arch32__ */
    dmnP->totalBlks = 0;
    dmnP->freeBlks = 0;
#endif  /* __arch32__ */

    /*
     * Establish new domain mount time.
     */

    /* alignment problem of dmnP->bfDmnMntId, not necessarily 64-bit aligned */
    TIME_READ(ltime);
    dmnP->bfDmnMntId = ltime;

    vdcnt = vdi = 1;

    vdcnt = 0;
    for ( vdi = 1; vdcnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        /* No need to bump vdRefCnt here since DmnTblLock is held to
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
    /* We saw as many volumes as there are in the domain. OK: DmnTblLock
     * is held to prevent racing addvol and rmvol.
     */
    MS_SMP_ASSERT(vdcnt == dmnP->vdCnt);

    /*
     * If not doing write delay then flush all dirty buffers and wait for
     * that i/o to complete
     */
    if (!MountWriteDelay) {
        bs_bfdmn_flush( dmnP );
        if ( done = bs_bfdmn_flush_sync( dmnP ) ) {
            ADVFS_SAD1("bs_bfdmn_activate: pinned buffers after activation",
                       done );
        }
    }

    orig_dmnP_state = dmnP->state;
    dmnP->state = BFD_ACTIVATED;

    dmnP->activateCnt++;

    /*
     * Remove any junk leftover by in-progress deletes.
     */

    if (!clu_is_ready() || !(flag & M_FAILOVER)) {
        vdcnt = vdi = 1;
        while ((vdi <= BS_MAX_VDI) && (vdcnt <= dmnP->vdCnt )) {
            if ( vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
                del_clean_mcell_list (vdp, flag);
                vdcnt++;
            }
            vdi++;
        }  /* end while */
    }

    /*
     * Delete all 'delete pending' bitfile sets.
     */

    bfs_delete_pending_list_finish_all( dmnP, flag );

    ss_dmn_activate(dmnP, flag);

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
        (void) bs_close(bfSetDirAccp, 0);
    }

    if (logOpen) {
        bs_bfdmn_flush (dmnP);
        (void) bs_bfdmn_flush_sync (dmnP);
        (void) lgr_close( dmnP->ftxLogP );
    }

    return sts;
}

/*
 * bs_bfdmn_sweep
 *
 * Rewrite on disk domainid
 */

statusT
bs_bfdmn_sweep(domainT * dmnP)
{
    statusT sts;
    uint32T *pgp;
    bfDomainIdT domainId = dmnP->domainId;
    int error, vdi, logVdI, tagdirVdI;
    bfPageRefHT bmtPgRef=NilBfPageRefH;
    bfPageRefHT rootTagPgRef=NilBfPageRefH;
    int vdCnt;

    if (BS_BFTAG_EQL(dmnP->ftxLogTag, NilBfTag)) {
        ms_uprintf( "bs_bfdmn_sweep: domain has no log tag!!!!" );
        return( E_INVALID_MSFS_STRUCT );
    }
    logVdI = BS_BFTAG_VDI(dmnP->ftxLogTag);

    tagdirVdI = BS_BFTAG_VDI(dmnP->bfSetDirTag);

    /*
     * Loop through all the disks in the domain, read all of the
     * RBMTs (BMT page0s for ver. 3 and earlier) and update bsDmnAttrT
     */

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        vdT *vdp;
        bsDmnAttrT* dmnAttrp;
        int mcell_index;
        struct bfAccess *mdap;

        /* We are still activating this disk, so no need to bump its
         * vdRefCnt.
         */
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, FALSE, FALSE)) )
            continue;
        vdCnt++;

        if ( RBMT_THERE(dmnP) ) {
            mdap = vdp->rbmtp;
            mcell_index = BFM_RBMT_EXT;
        } else {
            mdap = vdp->bmtp;
            mcell_index = BFM_BMT_EXT_V3;
        }
        if (error = bs_pinpg (&bmtPgRef, (void *)&pgp, mdap, 0, BS_NIL)) {
            sts = (statusT)error;
            goto err_cleanup;
        }

        dmnAttrp = bmtr_find(&((bsMPgT *)pgp)->bsMCA[mcell_index],
                             BSR_DMN_ATTR, dmnP);

        if (dmnAttrp == 0) {
            sts = E_INVALID_MSFS_STRUCT;
            goto err_cleanup;
        }

        dmnAttrp->bfDomainId = domainId;

        if (vdi == logVdI) {
            /*
             * This vd has the log on it. Update the domainId in each
             * log page header and trailer
             */
            uint32T *logpgp;
            bsDmnMAttrT* dmnMAttrp;
            struct vnode *vp = NULL;
            struct nameidata *ndp = &u.u_nd;
            int logpg;
            lbnT logBlk;
            bsMRT* recp;
            bsXtntRT* primXtnt;
            int mcell_index;

            logpgp = (uint32T *)ms_malloc( ADVFS_PGSZ );
            if (logpgp == NULL) {
                sts = ENOMEM;
                goto err_cleanup;
            }

            if ( RBMT_THERE(dmnP) ) {
                mcell_index = BFM_RBMT_EXT;
            } else {
                mcell_index = BFM_BMT_EXT_V3;
            }
            dmnMAttrp = bmtr_find(&((bsMPgT *)pgp)->bsMCA[mcell_index],
                                 BSR_DMN_MATTR, dmnP);

            if( error = getvp( &vp, vdp->vdName, ndp, UIO_SYSSPACE ) ) {
                sts = (statusT)error;
                    goto err_cleanup;
                }

            VOP_ACCESS( vp, VREAD | VWRITE, ndp->ni_cred, error );
            if (error) {
                sts = (statusT)error;
                vrele(vp);
                goto err_cleanup;
            }

            recp = (bsMRT *)((bsMPgT *)pgp)->bsMCA[BFM_FTXLOG].bsMR0;
            if ( recp->type != BSR_ATTR ) {
                ms_uprintf("bs_bfdmn_sweep: first record is type %d\n", recp->type);
                sts = ENO_BS_ATTR;
                vrele(vp);
                ms_free(logpgp);
                goto err_cleanup;
            }

            if ( recp->bCnt != sizeof(bsBfAttrT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for BSR_ATTR record", recp->bCnt);
            }

            recp = (bsMRT *)((char *)recp + roundup(recp->bCnt, sizeof(int)));
            if ( recp->type != BSR_XTNTS ) {
                ms_uprintf("bs_bfdmn_sweep: second record is type %d\n", recp->type);
                sts = ENO_XTNTS;
                vrele(vp);
                ms_free(logpgp);
                goto err_cleanup;
            }
            if ( recp->bCnt != sizeof(bsXtntRT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for BSR_XTNTS record", recp->bCnt);
            }

            primXtnt = (bsXtntRT *)((char*)recp + sizeof(struct bsMR));
            if ( primXtnt->firstXtnt.xCnt > BMT_XTNTS ||
                 primXtnt->firstXtnt.xCnt == 0) {
                ADVFS_SAD1("bs_bfdmn_sweep: N1 extents in the primary mcell\n",
                           primXtnt->firstXtnt.xCnt);
            }

            if ( primXtnt->firstXtnt.mcellCnt != 1) {
                ms_uprintf("Log over multiple mcells\n");
                sts = ENOT_SUPPORTED;
                vrele(vp);
                ms_free(logpgp);
                goto err_cleanup;
            }

            logBlk = primXtnt->firstXtnt.bsXA[0].vdBlk;

            for (logpg=0; logpg<dmnMAttrp->ftxLogPgs; logpg++) {
                error = read_raw_page(vp, logBlk, ADVFS_PGSZ_IN_BLKS, logpgp);
                if ( error ) {
                    sts = (statusT)error;
                    vrele(vp);
                    ms_free(logpgp);
                    goto err_cleanup;
                }

                ((logPgT *)logpgp)->hdr.dmnId = domainId;
                ((logPgT *)logpgp)->trlr.dmnId = domainId;

                error = write_raw_page(vp, logBlk, ADVFS_PGSZ_IN_BLKS, logpgp);
                if ( error ) {
                    sts = (statusT)error;
                    vrele(vp);
                    ms_free(logpgp);
                    goto err_cleanup;
                }
                logBlk += ADVFS_PGSZ_IN_BLKS;
            }
            vrele(vp);
            ms_free(logpgp);
        }

        if (vdi == tagdirVdI) {
            /*
             * This vd has the root tagdir on it. Update the domainId in each
             * filesetid.
             */
            bsMRT* recp;
            bsXtntRT* primXtnt;
            int rootTagPg;

            recp = (bsMRT*)((bsMPgT *)pgp)->bsMCA[BFM_BFSDIR].bsMR0;

            if ( recp->type != BSR_ATTR ) {
                ms_uprintf("bs_bfdmn_sweep: first record is type %d\n", recp->type);
                sts = ENO_BS_ATTR;
                goto err_cleanup;
            }

            if ( recp->bCnt != sizeof(bsBfAttrT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for BSR_ATTR record", recp->bCnt);
            }

            recp = (bsMRT *)((char *)recp + roundup(recp->bCnt, sizeof(int)));
            if ( recp->type != BSR_XTNTS ) {
                ms_uprintf("bs_bfdmn_sweep: second record is type %d\n", recp->type);
                sts = ENO_XTNTS;
                goto err_cleanup;
            }

            if ( recp->bCnt != sizeof(bsXtntRT) + sizeof(bsMRT) ) {
                ADVFS_SAD1("bs_bfdmn_sweep: wrong size (N1) for BSR_XTNTS record", recp->bCnt);
            }

            primXtnt = (bsXtntRT *)((char*)recp + sizeof(struct bsMR));
            if ( primXtnt->firstXtnt.xCnt > BMT_XTNTS ||
                 primXtnt->firstXtnt.xCnt == 0) {
                ADVFS_SAD1("bs_bfdmn_sweep: N1 extents in the primary mcell\n",
                           primXtnt->firstXtnt.xCnt);
            }

            if ( primXtnt->firstXtnt.mcellCnt != 1) {
                ms_uprintf("Root tagdir over multiple mcells\n");
                sts = ENOT_SUPPORTED;
                goto err_cleanup;
            }

            for (rootTagPg = 0;
                 rootTagPg < primXtnt->firstXtnt.bsXA[1].bsPage;
                 rootTagPg++) {
                bsTDirPgT* rootTagPgP;

                if (error = bs_refpg(&rootTagPgRef, (void *)&rootTagPgP,
                                     dmnP->bfSetDirAccp, rootTagPg, BS_NIL)) {
                    sts = (statusT)error;
                    goto err_cleanup;
                }

                if (rootTagPgP->tpNumAllocTMaps != 0) {
                    int i;

                    for (i=0; i<BS_TD_TAGS_PG; i++) {
                        int vd;
                        bfMCIdT mcid;

                        if (rootTagPgP->tMapA[i].tmSeqNo & BS_TD_IN_USE) {
                            vd = rootTagPgP->tMapA[i].tmVdIndex;
                            mcid = rootTagPgP->tMapA[i].tmBfMCId;
                            if (error = update_bfsetid(dmnP,
                                                       vd,
                                                       mcid,
                                                       domainId)) {
                                sts = (statusT)error;
                                goto err_cleanup;
                            }
                        }
                    }
                }

                if (error = bs_derefpg(rootTagPgRef, BS_NIL)) {
                    sts = (statusT)error;
                    goto err_cleanup;
                }
                rootTagPgRef=NilBfPageRefH;
            }
        }

        if (error =  bs_unpinpg(bmtPgRef, logNilRecord, BS_DIRTY)) {
            sts = (statusT)error;
            goto err_cleanup;
        }
        bmtPgRef = NilBfPageRefH;
    }
    /* We saw as many volumes as there are in the domain. */
    MS_SMP_ASSERT(vdCnt == dmnP->vdCnt);
    return EOK;

err_cleanup:

    if (!PGREF_EQL(bmtPgRef,NilBfPageRefH))
    {
        bs_unpinpg(bmtPgRef, logNilRecord, BS_DIRTY);
    }
    if (!PGREF_EQL(rootTagPgRef,NilBfPageRefH))
    {
        bs_derefpg(rootTagPgRef, BS_NIL);
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
               int vd,
               bfMCIdT mcid,
               bfDomainIdT domainId
               )
{
    statusT sts = EOK;
    int error;
    uint32T* pagep;
    vdT* vdp = VD_HTOP(vd, dmnP);
    bsBfSetAttrT* bfsAttrp;
    bfPageRefHT bmtPgRef;

mcell_chain:
    if (sts = bs_pinpg (&bmtPgRef, (void*)&pagep, vdp->bmtp, mcid.page,
                          BS_NIL)) {

        return(sts);
    }

    bfsAttrp = bmtr_find(&((bsMPgT *)pagep)->bsMCA[mcid.cell],
                          BSR_BFS_ATTR, dmnP);
    if (bfsAttrp == NULL) {
        mcid = ((bsMPgT *)pagep)->bsMCA[mcid.cell].nextMCId;
        if ((mcid.page == 0) && (mcid.cell == 0))
        {
            sts = E_INVALID_MSFS_STRUCT;
            goto err_cleanup;
        }
        (void) bs_unpinpg (bmtPgRef, logNilRecord, BS_CLEAN);
        goto mcell_chain;
    }

    bfsAttrp->bfSetId.domainId = domainId;

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

static statusT
set_vd_mounted(
               struct domain* dmnP,     /* IN - domain struct */
               struct vd* vdp           /* IN - vd struct */
               )
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
    if ( RBMT_THERE(dmnP) ) {
        sts = bs_pinpg( &bmtPgRef, (void *)&bmtpgp, vdp->rbmtp, 0, BS_NIL );
        if (sts != EOK) {
            return sts;
        }

        vdAttrp = bmtr_find(&bmtpgp->bsMCA[BFM_RBMT_EXT],
                            BSR_VD_ATTR, vdp->dmnP);

        /* Get the mcell count from the primary mcell's BSR_XTNTS
          * record and use this to set vdp->lastRbmtPg.  This value is the
          * number of (mcells-1) since the first mcell is on page 0.
          */
        bsXtntrp = bmtr_find(&bmtpgp->bsMCA[BFM_RBMT],
                     BSR_XTNTS, vdp->dmnP);
        vdp->lastRbmtPg = bsXtntrp->firstXtnt.mcellCnt - 1;
    }
    else {
        sts = bs_pinpg( &bmtPgRef, (void *)&bmtpgp, vdp->bmtp, 0, BS_NIL );
        if (sts != EOK) {
            return sts;
        }

        vdAttrp = bmtr_find(&bmtpgp->bsMCA[BFM_BMT_EXT_V3],
                            BSR_VD_ATTR, vdp->dmnP);
    }

    if (vdAttrp == 0) {
        (void) bs_unpinpg( bmtPgRef, logNilRecord, BS_CLEAN );
        return E_INVALID_MSFS_STRUCT;
    }

    vdAttrp->state = BSR_VD_MOUNTED;
    vdAttrp->vdMntId = dmnP->bfDmnMntId;
    
    /* After the vdState is set to BSR_VD_MOUNTED and the vdStateLock is
     * dropped, other threads can see this disk as a valid volume.
     */
    mutex_lock( &vdp->vdStateLock );
    vdp->vdState    = BSR_VD_MOUNTED;      
    vdp->vdSetupThd = NULL;
    mutex_unlock( &vdp->vdStateLock );

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
    mutex_lock (&(dmnP->mutex));
    UINT64T_ADD (dmnP->totalBlks, vdp->vdClusters * vdp->stgCluster);
    UINT64T_ADD (dmnP->freeBlks, vdp->freeClust * vdp->stgCluster);
    mutex_unlock (&(dmnP->mutex));

    if ( RBMT_THERE(dmnP) ) {
        read_n_chk_last_rbmt_pg( vdp );
    }

    return EOK;
}

void
read_n_chk_last_rbmt_pg( vdT *vdp )
{
    bfPageRefHT bmtPgRef;
    struct bsMPg* bmtpgp;
    bfMCIdT nextfreeMCId;
    statusT sts;

    sts = bmt_refpg( &bmtPgRef, (void *)&bmtpgp,
                     vdp->rbmtp, vdp->lastRbmtPg, BS_NIL );
    if ( sts != EOK ) {
        return;
    }

    lock_write( &vdp->rbmt_mcell_lk.lock );

    nextfreeMCId = bmtpgp->nextfreeMCId;

    (void)bs_derefpg( bmtPgRef, BS_CACHE_IT );

    if ( nextfreeMCId.cell == 0 && nextfreeMCId.page == 0 &&
         !(vdp->rbmtFlags & RBMT_EXTENSION_IN_PROGRESS) )
    {
        /* No room in this page, come back later; in case we crashed
         * before the rbmt got extended, and now we get to this point
         * after reboot, restart the rbmt extension.
         * Note that on any other page, cell 0 may be a valid free cell.
         */
        /* Setting this flags is protected by the rbmt_mcell_lk */
        vdp->rbmtFlags |= RBMT_EXTENSION_IN_PROGRESS;
        lock_done( &vdp->rbmt_mcell_lk.lock );
        (void)rbmt_extend( vdp->dmnP, vdp->vdIndex );
    } else {
        lock_done( &vdp->rbmt_mcell_lk.lock );
    }
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

static statusT
get_raw_vd_attrs(
                 char *vdDiskName,              /* IN - disk device name */
                 domainT *dmnP,                 /* IN - Domain pointer */
                 int vdi,                       /* IN - vd index */
                 int flag,                      /* IN - flag */
                 struct vnode** vnodepp,        /* OUT - vnode ptr */
                 struct bsVdAttr** vdAttrpp,    /* OUT - vd attributes */
                 bsDmnAttrT** dmnAttrpp,        /* OUT - domain attributes */
                 bsDmnMAttrT** dmnMAttrpp,      /* OUT - log attributes */
                 int *dmnVersion ,              /* OUT - domain version */
                 struct bsMPg *pgp              /* OUT - RBMT page 0 */
                 )
{
    statusT sts;
    uint32T *superBlk;
    struct bsVdAttr* vdAttrp;
    bsDmnAttrT* dmnAttrp;
    bsDmnMAttrT* dmnMAttrp;

    int error;
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
    int attridx = BFM_RBMT_EXT;
    int isRoot = flag & (M_LOCAL_ROOT | M_GLOBAL_ROOT);
    int isMount = flag & M_MSFS_MOUNT;
    extern struct vnode **GlobalRootVpp;

    /* get device's vnode */
    if (isRoot & M_LOCAL_ROOT) {
        vp = rootvp;
    } else if (isRoot & M_GLOBAL_ROOT) {
        vp = GlobalRootVpp[vdi];
    } else {
        if( error = getvp( &vp, vdDiskName, ndp, UIO_SYSSPACE ) ) {
            return (statusT)error;
        }
        VOP_ACCESS( vp, VREAD | VWRITE, ndp->ni_cred, error );
        if (error) {
            sts = (statusT)error;
            goto err_nodlmlock;
        }
        /*
         * For a cluster, to ensure the relocation of the cluster root fs,
         * we can't allow CFS vnodes to be used here since the relocation
         * will block trying to vrele device vnodes so instead we use the
         * VT_NON vnode as is done when mounting root initially.  This can
         * also be a problem for non root domains where an addvol is
         * performed using a device special file contained in the domain 
         * itself.
         */
        if(clu_is_ready())  {
            dev_t dev;
            dev = vp->v_rdev;
            vrele(vp);
            if (error = bdevvp(dev, &vp)) {
                return (statusT)error;
            }
        }
    }

    /* open the vnode
     * In SVR4, driver open and close routines expect to get the OTYP_MNT flag
     * if the open/close was done as the result of a mount/unmount. While OSF/1
     * can pass a flag parameter to device open/close routines, it is only
     * supported in spec_open() and spec_close(). These are the functions
     * invoked via VOP_OPEN and VOP_CLOSE for device special files. Therefore,
     * we need to inform spec_open()/spec_close() that we are doing a
     * mount/unmount.
     */

    /*
     * In the cluster, for paths other than addvol, rmvol, and mount
     * we acquire an exclusive DLM lock to serialize with other uses of
     * the block device. (The aforementioned cases do locking elsewhere.)
     */
    if (clu_is_ready() && !(isMount)) {
       error = CC_DEV_DLM_LOCK(vp->v_rdev, 1);
       if( error ) {
           sts = (statusT) error;
           goto err_nodlmlock;
       }
    }

    VOP_OPEN( &vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    if( error ) {
        sts = (statusT) error;
        goto err_vrele;
    }

    if( vp->v_type != VBLK && vp->v_type != VCHR ) {
       sts = (statusT) E_BAD_DEV;
       goto err_vclose;
    }

    if( setmount( vp, SM_OPEN | SM_MOUNTED | SM_SETMOUNT ) ) {
        sts = (statusT) EBUSY;
        goto err_vclose;
    }

    superBlk = (uint32T *) ms_malloc( ADVFS_PGSZ );
    if (superBlk == NULL) {
        sts = ENOMEM;
        goto err_closeall;
    }

    sts = read_raw_page( vp, MSFS_FAKE_SB_BLK, ADVFS_PGSZ_IN_BLKS, superBlk );
    if (sts != 0) {
        goto err_freesblk;
    }

    if (superBlk[MSFS_MAGIC_OFFSET / sizeof( uint32T )] != MSFS_MAGIC) {
        sts = E_BAD_MAGIC;
        goto err_freesblk;
    }

    ms_free( superBlk );
    superBlk = NULL;

    if( error = read_raw_bmt_page(vp, MSFS_RESERVED_BLKS, pgp) ) {
        sts = (statusT) error;
        goto err_closeall;
    }

    /*
     * Check the version number and page ID as minimal check on
     * whether this is a sensible bmt page.  There should be more
     * extensive checks here.
     */

    if ( pgp->pageId != 0 ) {
        sts = E_INVALID_MSFS_STRUCT;
        goto err_closeall;
    }

    if (pgp->megaVersion > BFD_ODS_LAST_VERSION) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }

    if (pgp->megaVersion < BFD_ODS_LAST_VERSION) {

        if ( (pgp->megaVersion == 2) && (BFD_ODS_LAST_VERSION >= 3) ) {

            /*
             * The change from version 2 to 3 was due to a change in
             * the on-disk format of the frag bitfile.  The fragbf
             * code uses its own versioning to differentiate between
             * the old and new format so 3 is a compatible upgrade from 2.
             * We don't want a kernel made for version 2 to use a file
             * system with the new fragbf format so we change the version
             * to 3 "on-the-fly" to prevent this from occuring.
             */

            ms_uprintf( "ADVFS: Upgrading on-disk version number from %d to %d for %s\n",
                       pgp->megaVersion, 3, vdDiskName );

            pgp->megaVersion = 3;

            error = write_raw_bmt_page(vp, MSFS_RESERVED_BLKS, pgp);
            if( error ) {
                sts = (statusT) error;
                goto err_closeall;
            }
        }
    }

    *dmnVersion = pgp->megaVersion;
    /*
     * Find the vd and domain attr records.  They are always linked
     * into the first extension mcell.
     */

    if ( pgp->megaVersion != FIRST_RBMT_VERSION ) {
        attridx = BFM_BMT_EXT_V3;
    }
    vdAttrp = bmtr_find(&pgp->bsMCA[attridx], BSR_VD_ATTR, dmnP);
    if (vdAttrp == 0) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }

    if ( TEST_VDI_RANGE(vdAttrp->vdIndex) != EOK ) {
        sts = EBAD_VDI;
        goto err_closeall;
    }

    dmnAttrp = bmtr_find(&pgp->bsMCA[attridx], BSR_DMN_ATTR, dmnP);
    if (dmnAttrp == 0) {
        sts = E_INVALID_FS_VERSION;
        goto err_closeall;
    }

    /* Note that not all disks have log attributes */

    dmnMAttrp = bmtr_find(&pgp->bsMCA[attridx], BSR_DMN_MATTR,dmnP);

    *vnodepp = vp;
    *vdAttrpp = vdAttrp;
    *dmnAttrpp = dmnAttrp;
    *dmnMAttrpp = dmnMAttrp;

    return EOK;

err_freesblk:
    ms_free( superBlk );

err_closeall:
    (void) setmount(vp, SM_CLEARMOUNT);

err_vclose:
    VOP_CLOSE(vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error);

err_vrele:
    if (clu_is_ready() && !(isMount)) {
        CC_DEV_DLM_UNLOCK(vp->v_rdev);
    }

err_nodlmlock:
    vrele(vp);

    return sts;
}  /* end of get_raw_vd_attrs */

/* Preferred IO byte transfer size default used only when 
 * the driver returns a value too large for efficient handling.
 * Sizes exceeding the 64MB threshold will be set to the lower default.
 * You may change the 256KB default size or 64MB threshold limit,
 * but choose an even multiple of 8192 bytes.
 */
int advfs_prefer_trans_default = (int)262144;
int advfs_prefer_trans_threshold = (int)(64 * (1<<20));

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

static statusT
setup_vd(
         domainT* dmnP,         /* IN - domain pointer */
         struct vnode* vnodep,  /* IN - ptr to vnode */
         char* vdDiskName,      /* IN - name of disk */
         struct bsVdAttr* vdAttrp, /* IN - on disk vd attributes */
         u_long flag,           /* IN - mount flags/check DmnTblLock? */
         vdT** vdPtr            /* OUT - vd struct pointer */
         )
{
    struct bfAccess *metafileap;
    int vdIndex;
    struct vd* vdp;
    statusT sts;
    bfPageRefHT bsPgRfH, bmtPgRef;
    struct bsMPg* bmtpgp;
    vdIoParamsT vdIoParams;
    int paramSize = sizeof( vdIoParamsT );
    DEVGEOMST geomst;                   /* DEVGETGEOM ioctl */
    struct devget devgetinfo;           /* DEVIOCGET ioctl */
    int i, result;
    extern int max_iosize_write;        /* default system-wide max write size */
    extern int max_iosize_read;         /* default system-wide max read size */
    extern long lbolt;

    MS_SMP_ASSERT((flag & M_GLOBAL_ROOT) ? 1 : lock_holder(&DmnTblLock));

    if ((sts = vd_alloc(&vdp, dmnP)) != EOK) {
        return sts;
    }

    vdIndex = vdAttrp->vdIndex;
    MS_SMP_ASSERT(TEST_VDI_RANGE(vdIndex) == EOK);
    vdp->vdIndex = vdIndex;

    /*
     * Round vdSize so that it is an even multiple of 16 blocks.
     */

    vdp->vdSize       = (vdAttrp->vdBlkCnt & ~0x0f);
    vdp->devVp        = vnodep;
    vdp->rbmtp        = NULL;
    vdp->bmtp         = NULL;
    vdp->sbmp         = NULL;
    vdp->dmnP         = dmnP;
    vdp->maxPgSz      = vdAttrp->maxPgSz;
    vdp->bmtXtntPgs   = vdAttrp->bmtXtntPgs;
    vdp->serviceClass = vdAttrp->serviceClass;
    vdp->stgCluster   = vdAttrp->stgCluster;
    vdp->vdClusters   = vdp->vdSize / vdp->stgCluster;
    strncpy( vdp->vdName, vdDiskName, BS_VD_NAME_SZ - 1 );
    vdp->vdSetupThd   = current_thread();

    /* Initialize the length limit of the device queue to a small,
     * finite value.  This value will be adjusted later as we sample
     * the I/O completion rate or get I/O statistics from the driver.
     * vd_lbolt will only be used if the driver does not provide
     * feedback.
     */
    vdp->devQ.lenLimit = 24;
    vdp->vd_lbolt      = lbolt;

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
     * For AdvFS on-disk version == FIRST_RBMT_VERSION, the global page
     * Dpg (initialized earlier by get_raw_vd_attrs) contains RBMT page0.
     * For previous versions, it has BMT page 0.
     *
     * At the time of this call, vdp->rbmtp and vdp->bmtp are NULL.  This 
     * will cause bs_map_bf to use the global page Dpg instead of 
     * attempting to use vdp->rbmtp / vdp->bmtp for I/O.
     */

    if ( RBMT_THERE(dmnP) ) {
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
    }
    else {
        sts = bfm_open_ms(&metafileap, dmnP, vdIndex, BFM_BMT_V3);
        if (sts != EOK) {
            goto err_freevd;
        }
        vdp->bmtp = metafileap;
    }

    sts = bfm_open_ms(&metafileap, dmnP, vdIndex, BFM_SBM);
    if (sts != EOK) {
        goto err_close;
    }
    vdp->sbmp = metafileap;

    /*
     * Get the disk I/O params
     */
    sts = bmtr_get_rec((RBMT_THERE(dmnP)) ? vdp->rbmtp : vdp->bmtp,
                       BSR_VD_IO_PARAMS,
                       &vdIoParams,
                       paramSize);

    /*
     * If the disk I/O params are there, use them.
     */
    if (sts == EOK) {
        vdp->rdmaxio = vdIoParams.rdMaxIo;
        vdp->wrmaxio = vdIoParams.wrMaxIo;
        vdp->qtodev = vdIoParams.qtoDev;
        vdp->consolidate = vdIoParams.consolidate;
        vdp->blockingFact = vdIoParams.blockingFact;
        vdp->readyLazyQ.lenLimit = vdIoParams.lazyThresh;
    }

    /*
     * determine max cluster i/o size for this device;
     * be prepared to default for devices which can't respond,
     * or devices that respond with zero transfer sizes.
     */
    bzero(&geomst, sizeof(DEVGEOMST));
    VOP_IOCTL(vnodep, DEVGETGEOM, (caddr_t)&geomst, FREAD,
              NOCRED, i, &result);
    if (i != 0 || geomst.geom_info.prefer_trans == 0 ||
        geomst.geom_info.max_trans == 0) {
        /*
         * default to system specified max i/o size
         */
        vdp->max_iosize_rd = vdp->preferred_iosize_rd = max_iosize_read;
        vdp->max_iosize_wr = vdp->preferred_iosize_wr = max_iosize_write;
    } else {
        vdp->max_iosize_rd = vdp->max_iosize_wr = geomst.geom_info.max_trans;
        vdp->preferred_iosize_rd = vdp->preferred_iosize_wr =
            geomst.geom_info.prefer_trans;

        /* If a driver returns a preferred IO byte transfer size
         * as large as the threshold or more, then assign a lower default size.
         * This keeps readahead or write consolidation from consuming
         * too much CPU or memory resouces to process a large transfer.
         */
        if (geomst.geom_info.prefer_trans >= advfs_prefer_trans_threshold){
                vdp->preferred_iosize_rd = vdp->preferred_iosize_wr = 
                    advfs_prefer_trans_default;
        }
    }

    /*
     * Set the sector size, default to DEV_BSIZE.
     */
    vdp->vdSectorSize = geomst.geom_info.sector_size == 0 ?
        DEV_BSIZE : geomst.geom_info.sector_size;

    /* Initialize the maximum read and write block transfer counts. */
    if (!vdp->rdmaxio)
       vdp->rdmaxio = vdp->preferred_iosize_rd / vdp->vdSectorSize;

    if (!vdp->wrmaxio)
       vdp->wrmaxio = vdp->preferred_iosize_wr / vdp->vdSectorSize;


    /*
     * Can't set I/O size to be greater than device max.
     */
    if (vdp->rdmaxio > (vdp->max_iosize_rd / vdp->vdSectorSize)) {
        ms_uprintf("Warning: %s: Decreasing maximum read transfer size to %d bytes\n",
                    vdp->vdName, vdp->max_iosize_rd);
            vdp->rdmaxio = MIN(vdp->rdmaxio,
                               (vdp->max_iosize_rd / vdp->vdSectorSize));
    }
    if (vdp->wrmaxio > (vdp->max_iosize_wr / vdp->vdSectorSize)) {
        ms_uprintf("Warning: %s: Decreasing maximum write transfer size to %d bytes\n",
                   vdp->vdName, vdp->max_iosize_wr);
        vdp->wrmaxio = MIN(vdp->wrmaxio,
                           (vdp->max_iosize_wr / vdp->vdSectorSize));
    }

    *vdPtr = vdp;

    return EOK;

err_close:
    if (vdp->rbmtp)
        bs_close(vdp->rbmtp, 0);

    if (vdp->bmtp)
        bs_close(vdp->bmtp, 0);

    if (vdp->sbmp)
        bs_close(vdp->sbmp, 0);

err_freevd:
    /*
     * Check (and wait) for outstanding I/O.
     */
    while( vdp->active.state == ACTIVE_DISK ||
           vdp->consolQ.ioQLen   ||
           vdp->flushQ.ioQLen    ||
           vdp->ubcReqQ.ioQLen   ||
           vdp->blockingQ.ioQLen ||
           vdp->devQ.ioQLen) {

        bs_startio( vdp, IO_FLUSH );

        /*
         * Wait for device to become quiescent.
         */
        mutex_lock( &vdp->devQ.ioQLock );
        lk_wait_for( &vdp->active, &vdp->devQ.ioQLock, INACTIVE_DISK );
        mutex_unlock( &vdp->devQ.ioQLock );
    }
   
    dmnP->vdpTbl[vdIndex - 1] = NULL;
    dmnP->vdCnt--;
    vd_free(vdp, dmnP);

    return sts;
}  /* end of setup_vd */


/*
 * Add a new virtual disk to an active domain.
 */

statusT
bs_vd_add_active(
    char *domainName,           /* in - bitfile domain name */
    char *vdName,               /* in - block special device name */
    serviceClassT *vdSvc,       /* in/out - service class */
    uint32T vdSize,                /* in - size of the virtual disk */
    uint32T bmtXtntPgs,         /* in - number of pages per BMT extent */
    uint32T bmtPreallocPgs,     /* in - num pages to preallocate for the BMT */
    uint32T flag,               /* in - M_MSFS_MOUNT may be set */
    bfDomainIdT *dmnId,         /* out - domain Id */
    uint32T *volIndex           /* out - volume index */
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
    int sz;
    serviceClassT tagSvc, logSvc;
    int vdi;
    vdT *vdp;

    struct vnode *vnodep = NULL;
    struct nameidata *ndp = &u.u_nd;
    int error;
    int dmnVersion;

    bfDmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (bfDmnParamsp == NULL) {
        return ( ENO_MORE_MEMORY );
    }
    bsVdParamsp = (bsVdParamsT *) ms_malloc( sizeof( bsVdParamsT ));
    if (bsVdParamsp == NULL) {
        ms_free( bfDmnParamsp );
        return ( ENO_MORE_MEMORY );
    }

    /* Activate the domain. */
    if ((sts = bs_bfdmn_tbl_activate(domainName, flag, &domainId)) != EOK) {
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
    DMNTBL_LOCK_WRITE( &DmnTblLock )

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
    bsVdParamsp->bmtXtntPgs   = bmtXtntPgs;
    bsVdParamsp->vdIndex      = vdi;

    /*
     * Initialize the on-disk structures of the new virtual disk.  The
     * tag service class and log service class are nil because this
     * disk will not contain the tag or log mcells.
     */

    if ((sts = bs_disk_init(vdName, bfDmnParamsp, bsVdParamsp,
        nilServiceClass, nilServiceClass, bmtPreallocPgs,
        dmnP->dmnVersion)) != EOK) {
        goto exit;
    }

    sts = get_raw_vd_attrs(vdName,
                           dmnP,
                           vdi,
                           flag,
                           &vnodep,
                           &vdAttrp,
                           &dmnAttrp,
                           &dmnMAttrp,
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
    DMNTBL_UNLOCK( &DmnTblLock )
    bs_domain_close(dmnP);

err_deactivate:
    (void) bs_bfdmn_deactivate(domainId, flag);

    ms_free( bsVdParamsp );
    ms_free( bfDmnParamsp );

    return sts;
}

/*
 * vd_alloc
 *
 * Allocate a new virtual disk and
 * initialize those fields which are independent of
 * the particular disk and domain involved.
 */

static statusT
vd_alloc(
         vdT **avdp,
         domainT *dmnP
         )
{
    vdT *vdp;
    int i;
    extern int AdvfsReadyQLim;

    if ((vdp = (vdT *)ms_malloc(sizeof(vdT))) == NULL) {
        return (ENO_MORE_MEMORY);
    }

    ftx_lock_init(&vdp->del_list_lk, &dmnP->mutex, ADVvdT_del_list_lk_info );
    ftx_lock_init( &vdp->stgMap_lk, &dmnP->mutex, ADVvdT_stgMap_lk_info);
    ftx_lock_init( &vdp->mcell_lk, &dmnP->mutex, ADVvdT_mcell_lk_info );
    ftx_lock_init(&vdp->rbmt_mcell_lk,&dmnP->mutex,ADVvdT_rbmt_mcell_lk_info);
    lock_setup( &vdp->ddlActiveLk, ADVvdT_ddlActiveLk_info, TRUE );
    mutex_init3(&vdp->vdIoLock, 0, "vdIoLock", ADVvdT_vdIoLock_info);
    mutex_init3(&vdp->vdStateLock, 0, "vdStateLock", ADVvdT_vdStateLock_info);

    ss_init_vd(vdp);

    VD_TRACE(vdp, 0);

    vdp->ddlActiveWaitMCId = bsNilMCId;
    cv_init(&vdp->ddlActiveWaitCv);

    if (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) {
        vdp->vdState = BSR_VD_DMNT_INPROG;
    }
    else {
        vdp->vdState = BSR_VD_VIRGIN;
    }

    /*
     * Init I/O queues, minimum initialization
     */
    mutex_init3(&vdp->devQ.ioQLock, 0, "devQ.ioQLock", ADVvdT_devQ_ioQLock_info);
    vdp->devQ.fwd = (ioDescT *)&vdp->devQ;
    vdp->devQ.bwd = (ioDescT *)&vdp->devQ;
    vdp->devQ.ioQLen = 0;
    vdp->devQ.lenLimit = 0;
    vdp->devQ.queue_cnt = 0;

    mutex_init3(&vdp->consolQ.ioQLock, 0, "consolQ.ioQLock", ADVvdT_consolQ_ioQLock_info);
    vdp->consolQ.fwd = (ioDescT *)&vdp->consolQ;
    vdp->consolQ.bwd = (ioDescT *)&vdp->consolQ;
    vdp->consolQ.ioQLen = 0;
    vdp->consolQ.lenLimit = 0;
    vdp->consolQ.queue_cnt = 0;

    mutex_init3(&vdp->blockingQ.ioQLock, 0, "blockingQ.ioQLock", ADVvdT_blockingQ_ioQLock_info);
    vdp->blockingQ.fwd = (ioDescT *)&vdp->blockingQ;
    vdp->blockingQ.bwd = (ioDescT *)&vdp->blockingQ;
    vdp->blockingQ.ioQLen = 0;
    vdp->blockingQ.lenLimit = 0;
    vdp->blockingQ.queue_cnt = 0;

    mutex_init3(&vdp->flushQ.ioQLock, 0, "flushQ.ioQLock", ADVvdT_flushQ_ioQLock_info);
    vdp->flushQ.fwd = (ioDescT *)&vdp->flushQ;
    vdp->flushQ.bwd = (ioDescT *)&vdp->flushQ;
    vdp->flushQ.ioQLen = 0;
    vdp->flushQ.lenLimit = 0;
    vdp->flushQ.queue_cnt = 0;

    mutex_init3(&vdp->ubcReqQ.ioQLock, 0, "ubcReqQ.ioQLock", ADVvdT_ubcReqQ_ioQLock_info);
    vdp->ubcReqQ.fwd = (ioDescT *)&vdp->ubcReqQ;
    vdp->ubcReqQ.bwd = (ioDescT *)&vdp->ubcReqQ;
    vdp->ubcReqQ.ioQLen = 0;
    vdp->ubcReqQ.lenLimit = 0;
    vdp->ubcReqQ.queue_cnt = 0;

    mutex_init3(&vdp->waitLazyQ.ioQLock, 0, "waitLazyQ.ioQLock", ADVvdT_waitLazyQ_ioQLock_info);
    vdp->waitLazyQ.fwd = (ioDescT *)&vdp->waitLazyQ;
    vdp->waitLazyQ.bwd = (ioDescT *)&vdp->waitLazyQ;
    vdp->waitLazyQ.ioQLen = 0;
    vdp->waitLazyQ.lenLimit = 0;
    vdp->waitLazyQ.queue_cnt = 0;

    mutex_init3(&vdp->readyLazyQ.ioQLock, 0, "readyLazyQ.ioQLock", ADVvdT_readyLazyQ_ioQLock_info);
    vdp->readyLazyQ.fwd = (ioDescT *)&vdp->readyLazyQ;
    vdp->readyLazyQ.bwd = (ioDescT *)&vdp->readyLazyQ;
    vdp->readyLazyQ.ioQLen = 0;
    /* chvol scales by 16 */
    vdp->readyLazyQ.lenLimit = AdvfsReadyQLim/16;
    vdp->readyLazyQ.queue_cnt = 0;

    mutex_init3(&vdp->tempQ.ioQLock, 0, "tempQ.ioQLock", ADVvdT_tempQ_ioQLock_info);
    vdp->tempQ.fwd = (ioDescT *)&vdp->tempQ;
    vdp->tempQ.bwd = (ioDescT *)&vdp->tempQ;
    vdp->tempQ.mfwd = &vdp->tempQ;
    vdp->tempQ.mbwd = &vdp->tempQ;
    vdp->tempQ.thd_id = NULL;
    vdp->tempQ.cnt = 0;

    vdp->vdIoOut = 0;

    vdp->gen_active = 0;
    vdp->start_io_posted_waiter = 0;
    vdp->start_io_posted = 0;

    vdp->blockingFact = BLOCKFACT;
    vdp->rdmaxio = 0;
    vdp->wrmaxio = 0;
    vdp->qtodev = QTODEV;
    vdp->consolidate = DEF_CONSOL;
    vdp->vdMagic = VDMAGIC;
    vdp->vdRefCnt = 0;
    vdp->vdRefWaiters = 0;
#ifdef ADVFS_DEBUG
    vdp->errorFlag = 0;
    vdp->errorCount = 0;
    vdp->errorRepeat = 0;
#endif /* ADVFS_DEBUG */

    mutex_lock( &vdp->devQ.ioQLock );
    lk_init( &vdp->active, &vdp->devQ.ioQLock, LKT_STATE, 0, LKU_VD_ACTIVE );
    (void)lk_set_state( &vdp->active, INACTIVE_DISK );
    mutex_unlock( &vdp->devQ.ioQLock );

    bzero((char *)&vdp->dStat, sizeof(struct dStat));

    mutex_init3(&vdp->smSyncQ0.ioQLock, 0, "smSyncQ0.ioQLock", ADVvdT_smSyncQ0_ioQLock_info);
    vdp->smSyncQ0.fwd = (ioDescT *)&vdp->smSyncQ0;
    vdp->smSyncQ0.bwd = (ioDescT *)&vdp->smSyncQ0;
    vdp->smSyncQ0.ioQLen = 0;
    vdp->smSyncQ0.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ1.ioQLock, 0, "smSyncQ1.ioQLock", ADVvdT_smSyncQ1_ioQLock_info);
    vdp->smSyncQ1.fwd = (ioDescT *)&vdp->smSyncQ1;
    vdp->smSyncQ1.bwd = (ioDescT *)&vdp->smSyncQ1;
    vdp->smSyncQ1.ioQLen = 0;
    vdp->smSyncQ1.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ2.ioQLock, 0, "smSyncQ2.ioQLock", ADVvdT_smSyncQ2_ioQLock_info);
    vdp->smSyncQ2.fwd = (ioDescT *)&vdp->smSyncQ2;
    vdp->smSyncQ2.bwd = (ioDescT *)&vdp->smSyncQ2;
    vdp->smSyncQ2.ioQLen = 0;
    vdp->smSyncQ2.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ3.ioQLock, 0, "smSyncQ3.ioQLock", ADVvdT_smSyncQ3_ioQLock_info);
    vdp->smSyncQ3.fwd = (ioDescT *)&vdp->smSyncQ3;
    vdp->smSyncQ3.bwd = (ioDescT *)&vdp->smSyncQ3;
    vdp->smSyncQ3.ioQLen = 0;
    vdp->smSyncQ3.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ4.ioQLock, 0, "smSyncQ4.ioQLock", ADVvdT_smSyncQ4_ioQLock_info);
    vdp->smSyncQ4.fwd = (ioDescT *)&vdp->smSyncQ4;
    vdp->smSyncQ4.bwd = (ioDescT *)&vdp->smSyncQ4;
    vdp->smSyncQ4.ioQLen = 0;
    vdp->smSyncQ4.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ5.ioQLock, 0, "smSyncQ5.ioQLock", ADVvdT_smSyncQ5_ioQLock_info);
    vdp->smSyncQ5.fwd = (ioDescT *)&vdp->smSyncQ5;
    vdp->smSyncQ5.bwd = (ioDescT *)&vdp->smSyncQ5;
    vdp->smSyncQ5.ioQLen = 0;
    vdp->smSyncQ5.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ6.ioQLock, 0, "smSyncQ6.ioQLock", ADVvdT_smSyncQ6_ioQLock_info);
    vdp->smSyncQ6.fwd = (ioDescT *)&vdp->smSyncQ6;
    vdp->smSyncQ6.bwd = (ioDescT *)&vdp->smSyncQ6;
    vdp->smSyncQ6.ioQLen = 0;
    vdp->smSyncQ6.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ7.ioQLock, 0, "smSyncQ7.ioQLock", ADVvdT_smSyncQ7_ioQLock_info);
    vdp->smSyncQ7.fwd = (ioDescT *)&vdp->smSyncQ7;
    vdp->smSyncQ7.bwd = (ioDescT *)&vdp->smSyncQ7;
    vdp->smSyncQ7.ioQLen = 0;
    vdp->smSyncQ7.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ8.ioQLock, 0, "smSyncQ8.ioQLock", ADVvdT_smSyncQ8_ioQLock_info);
    vdp->smSyncQ8.fwd = (ioDescT *)&vdp->smSyncQ8;
    vdp->smSyncQ8.bwd = (ioDescT *)&vdp->smSyncQ8;
    vdp->smSyncQ8.ioQLen = 0;
    vdp->smSyncQ8.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ9.ioQLock, 0, "smSyncQ9.ioQLock", ADVvdT_smSyncQ9_ioQLock_info);
    vdp->smSyncQ9.fwd = (ioDescT *)&vdp->smSyncQ9;
    vdp->smSyncQ9.bwd = (ioDescT *)&vdp->smSyncQ9;
    vdp->smSyncQ9.ioQLen = 0;
    vdp->smSyncQ9.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ10.ioQLock, 0, "smSyncQ10.ioQLock", ADVvdT_smSyncQ10_ioQLock_info);
    vdp->smSyncQ10.fwd = (ioDescT *)&vdp->smSyncQ10;
    vdp->smSyncQ10.bwd = (ioDescT *)&vdp->smSyncQ10;
    vdp->smSyncQ10.ioQLen = 0;
    vdp->smSyncQ10.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ11.ioQLock, 0, "smSyncQ11.ioQLock", ADVvdT_smSyncQ11_ioQLock_info);
    vdp->smSyncQ11.fwd = (ioDescT *)&vdp->smSyncQ11;
    vdp->smSyncQ11.bwd = (ioDescT *)&vdp->smSyncQ11;
    vdp->smSyncQ11.ioQLen = 0;
    vdp->smSyncQ11.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ12.ioQLock, 0, "smSyncQ12.ioQLock", ADVvdT_smSyncQ12_ioQLock_info);
    vdp->smSyncQ12.fwd = (ioDescT *)&vdp->smSyncQ12;
    vdp->smSyncQ12.bwd = (ioDescT *)&vdp->smSyncQ12;
    vdp->smSyncQ12.ioQLen = 0;
    vdp->smSyncQ12.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ13.ioQLock, 0, "smSyncQ13.ioQLock", ADVvdT_smSyncQ13_ioQLock_info);
    vdp->smSyncQ13.fwd = (ioDescT *)&vdp->smSyncQ13;
    vdp->smSyncQ13.bwd = (ioDescT *)&vdp->smSyncQ13;
    vdp->smSyncQ13.ioQLen = 0;
    vdp->smSyncQ13.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ14.ioQLock, 0, "smSyncQ14.ioQLock", ADVvdT_smSyncQ14_ioQLock_info);
    vdp->smSyncQ14.fwd = (ioDescT *)&vdp->smSyncQ14;
    vdp->smSyncQ14.bwd = (ioDescT *)&vdp->smSyncQ14;
    vdp->smSyncQ14.ioQLen = 0;
    vdp->smSyncQ14.queue_cnt = 0;

    mutex_init3(&vdp->smSyncQ15.ioQLock, 0, "smSyncQ15.ioQLock", ADVvdT_smSyncQ15_ioQLock_info);
    vdp->smSyncQ15.fwd = (ioDescT *)&vdp->smSyncQ15;
    vdp->smSyncQ15.bwd = (ioDescT *)&vdp->smSyncQ15;
    vdp->smSyncQ15.ioQLen = 0;
    vdp->smSyncQ15.queue_cnt = 0;

    vdp->syncQIndx = 0;

#ifdef ADVFS_SMP_ASSERT
    vdp->rmioq_cnt = 0;
    vdp->rmormvq_cnt = 0;
#endif

    vdp->vdRetryCount = 0;

    *avdp = vdp;

    return (EOK);
}

static void
vd_free(
        vdT *vdp,
        domainT *dmnP
        )
{
    int i;

    MS_SMP_ASSERT(vdp->vdMagic == VDMAGIC);

    mutex_lock( &vdp->vdStateLock );
    MS_SMP_ASSERT( vdp->vdState == BSR_VD_ZOMBIE || 
                   vdp->vdState == BSR_VD_VIRGIN ||     /* error path state */
                   vdp->vdState == BSR_VD_DMNT_INPROG );/* error path state */
    MS_SMP_ASSERT( vdp->vdRefCnt == 0 );
    MS_SMP_ASSERT( vdp->vdRefWaiters == 0 );

    mutex_unlock( &vdp->vdStateLock );

    sbm_clear_cache(vdp);

    ss_dealloc_vd(vdp);

    /* Let's make sure that the I/O queues for this disk have been
     * properly drained.
     */

    MS_SMP_ASSERT( vdp->waitLazyQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ0.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ1.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ2.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ3.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ4.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ5.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ6.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ7.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ8.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ9.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ10.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ11.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ12.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ13.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ14.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->smSyncQ15.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->readyLazyQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->consolQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->blockingQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->flushQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->ubcReqQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->devQ.ioQLen == 0 );
    MS_SMP_ASSERT( vdp->tempQ.cnt == 0 );

    lk_destroy(&vdp->active);
    mutex_destroy( &vdp->vdIoLock );
    mutex_destroy( &vdp->vdStateLock );
    mutex_destroy( &vdp->blockingQ.ioQLock );
    mutex_destroy( &vdp->flushQ.ioQLock );
    mutex_destroy( &vdp->ubcReqQ.ioQLock );
    mutex_destroy( &vdp->waitLazyQ.ioQLock );
    mutex_destroy( &vdp->readyLazyQ.ioQLock );
    mutex_destroy( &vdp->smSyncQ0.ioQLock );
    mutex_destroy( &vdp->smSyncQ1.ioQLock );
    mutex_destroy( &vdp->smSyncQ2.ioQLock );
    mutex_destroy( &vdp->smSyncQ3.ioQLock );
    mutex_destroy( &vdp->smSyncQ4.ioQLock );
    mutex_destroy( &vdp->smSyncQ5.ioQLock );
    mutex_destroy( &vdp->smSyncQ6.ioQLock );
    mutex_destroy( &vdp->smSyncQ7.ioQLock );
    mutex_destroy( &vdp->smSyncQ8.ioQLock );
    mutex_destroy( &vdp->smSyncQ9.ioQLock );
    mutex_destroy( &vdp->smSyncQ10.ioQLock );
    mutex_destroy( &vdp->smSyncQ11.ioQLock );
    mutex_destroy( &vdp->smSyncQ12.ioQLock );
    mutex_destroy( &vdp->smSyncQ13.ioQLock );
    mutex_destroy( &vdp->smSyncQ14.ioQLock );
    mutex_destroy( &vdp->smSyncQ15.ioQLock );
    mutex_destroy( &vdp->consolQ.ioQLock );
    mutex_destroy( &vdp->devQ.ioQLock );
    mutex_destroy( &vdp->tempQ.ioQLock );
    

    lock_terminate( &vdp->stgMap_lk.lock );
    lock_terminate(&vdp->mcell_lk.lock);
    lock_terminate(&vdp->rbmt_mcell_lk.lock);
    lock_terminate(&vdp->del_list_lk.lock);
    lock_terminate( &vdp->ddlActiveLk );

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
                     domainT *dmnP,  /* in */
                     vdIndexT vdIndex,  /* in */
                     uint32T forceFlag  /* in */
                     )
{
    bsBfDescT bfDesc;
    int bfDescCnt;
    uint32T blksPerCluster;
    int foundFlag;
    uint32T freeClusters;
    bfMCIdT nextBfDescId;
    statusT sts;
    uint32T totalClusters;
    vdT *vdp;
    int dmntbl_lock = 0;
    bfTagT bfTag;
    int ss_was_activated = FALSE;
    u_long dmnState;

    mutex_lock (&(dmnP->mutex));
    if (!(dmnP->dmnFlag & BFD_RMVOL_IN_PROGRESS)){
        dmnP->dmnFlag |= BFD_RMVOL_IN_PROGRESS;
        mutex_unlock (&(dmnP->mutex));
    } else {
        mutex_unlock (&(dmnP->mutex));
        return E_RMVOL_ALREADY_INPROG;
    }

    /* Since we are the only thread removing disks in this domain, there
     * is no need to bump the vdRefCnt in the call to vd_htop_if_valid().
     */
    vdp = vd_htop_if_valid(vdIndex, dmnP, FALSE, FALSE);
    if (!vdp) 
        RAISE_EXCEPTION ( EBAD_VDI );

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

    /* reclaim any old reserved CFS vnodes that are hanging around
     * on vnode free list for any of the reserved files on the vd
     */
    sts = bs_reclaim_cfs_rsvd_vn(vdp->sbmp);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if ( RBMT_THERE(dmnP) ) {
        sts = bs_reclaim_cfs_rsvd_vn (vdp->rbmtp);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
    }

    sts = bs_reclaim_cfs_rsvd_vn (vdp->bmtp);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if(dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        /* deactivate/drain vfast operations */
	dmnState = 0;
        ss_change_state(dmnP->domainName, SS_SUSPEND, &dmnState, 0);
        ss_was_activated = TRUE;
    }

    DMNTBL_LOCK_WRITE( &DmnTblLock )
    dmntbl_lock = 1;
    /* coordinate with truncation */
    RMVOL_TRUNC_LOCK_WRITE(dmnP);

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
    lgr_checkpoint_log (dmnP);

    sts = set_disk_attrs (dmnP, BSR_VD_RMVOL, vdp->devVp->v_rdev);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    blksPerCluster = vdp->stgCluster;
    totalClusters = vdp->vdClusters;
    freeClusters = vdp->freeClust;

    vd_remove(dmnP, vdp, VD_STATE_CHANGE | VD_REMOVE_PERM);

    ss_rmvol_from_hotlst(dmnP,vdIndex);

    RMVOL_TRUNC_UNLOCK(dmnP);
    DMNTBL_UNLOCK( &DmnTblLock )
    dmntbl_lock = 0;

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
        ss_rmvol_from_hotlst(dmnP,vdIndex);
	dmnState = 0;  
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
        ss_was_activated = FALSE;
    }

    mutex_lock (&(dmnP->mutex));

    /*
     * Subtract this vd's free blocks from the domain totals.
     */
    UINT64T_SUB (dmnP->totalBlks, totalClusters * blksPerCluster);
    UINT64T_SUB (dmnP->freeBlks, freeClusters * blksPerCluster);

    dmnP->dmnFlag &= ~BFD_RMVOL_IN_PROGRESS;

    mutex_unlock (&(dmnP->mutex));

    return sts;

HANDLE_EXCEPTION:

    mutex_lock (&(dmnP->mutex));
    dmnP->dmnFlag &= ~BFD_RMVOL_IN_PROGRESS;
    mutex_unlock (&(dmnP->mutex));

    if (dmntbl_lock == 1) {
        RMVOL_TRUNC_UNLOCK(dmnP);
        DMNTBL_UNLOCK( &DmnTblLock );
        dmntbl_lock = 0;
    }

    if(ss_was_activated == TRUE) {
        /* reactivate vfast operations */
	dmnState = 0;
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
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
 * /etc/fdmns.
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
    int mcell_index;
    bsMPgT *bmtPage;
    bsDmnTAttrT *dmnTAttr;
    bfPageRefHT pgPin;
    struct nameidata *ndp = &u.u_nd;

    DMNTBL_LOCK_WRITE( &DmnTblLock )

    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vd->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vd->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }
    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);
    if (sts != EOK) {
        DMNTBL_UNLOCK( &DmnTblLock )
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
        DMNTBL_UNLOCK( &DmnTblLock )
        return EBMTR_NOT_FOUND;
    }

    /*
     * Check to see if dmnTAttr has a dev in it. If not, it got
     * wiped out during a domain activation - nothing to do here.
     */

    if (dmnTAttr->dev) {
        ndp->ni_nameiop = LOOKUP | FOLLOW;
        ndp->ni_segflg = UIO_SYSSPACE;
        ndp->ni_dirp = volName;

        if ( sts = (statusT) namei(ndp) ) {
            bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
            DMNTBL_UNLOCK( &DmnTblLock )
            return sts;
        }

        /*
         * For now, there is only one rmvol/addvol record.
         */
        if ( (dmnTAttr->dev != ndp->ni_vp->v_rdev) || (dmnTAttr->op != op) ) {
            bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
            DMNTBL_UNLOCK( &DmnTblLock )
            return E_RDEV_MISMATCH;
        }

        dmnTAttr->dev = 0;
        dmnTAttr->op = BSR_VD_NO_OP;

        sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    } else {
        sts = bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
    }

done:
    DMNTBL_UNLOCK( &DmnTblLock )

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

    XTNMAP_LOCK_READ(&(bfAccess->xtntMap_lk))

    /*
     * FIX - This function assumes that reserved files are not shadowed.  This
     * will change.
     */

    imm_get_first_xtnt_desc (bfAccess->xtnts.xtntMap, &xtntDescId, &xtntDesc);

    while (xtntDesc.pageCnt > 0) {

        if (xtntDesc.volIndex == vdIndex) {
            foundFlag = 1;
            break;  /* out of while */
        }

        imm_get_next_xtnt_desc(bfAccess->xtnts.xtntMap, &xtntDescId, &xtntDesc);

    }  /* end while */

    XTNMAP_UNLOCK(&(bfAccess->xtntMap_lk))

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
           domainT *dmnP,  /* in */
           vdIndexT vdIndex,  /* in */
           uint32T forceFlag, /* in */
           uint32T startBlk, /* in */
           uint32T numBlks  /* in */
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
    uint32T pageCnt;
    statusT sts;
    int unlockFlag = 0;
    uint32T open_options,do_vrele;
    struct vnode *nullvp;
    struct mount *mp;

    bfSetId.domainId = dmnP->domainId;

    bmtH.curPg = 0;
    bmtH.curMcell = 0;

    sts = bmt_open (&bmtH, dmnP, vdIndex);
    if (sts != EOK) {
        if (sts == E_PAGE_NOT_MAPPED) {
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
        open_options = BF_OP_IGNORE_DEL;
        do_vrele=0;
        mp = NULLMT;

        sts = rbf_bfs_open (&bfSetp, bfSetId, BFS_OP_IGNORE_DEL, FtxNilFtxH);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        closeBfSetFlag = 1;

        /*
         * Access the bitfile, regardless of its "delete" status
         */
        nullvp = NULL;
        if (clu_is_ready()) {
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
                (bfTag.num > GROUP_QUOTA_FILE_TAG)) {
                
                /* All filesets must be mounted */
                MS_SMP_ASSERT(bfSetp->fsnp);

                mp = bfSetp->fsnp->mountp;
                open_options |= BF_OP_GET_VNODE;
                do_vrele=1;
            }
        }
        
        sts = bs_access( &bfap, 
                         bfTag, 
                         bfSetp, 
                         FtxNilFtxH,
                         open_options,
                         mp, 
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
                 * return E_PAGE_NOT_MAPPED. That's why
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
                    pageCnt = bfap->nextPage;
                    if (pageCnt > 0) {
                        sts = mig_migrate (
                                           bfap,
                                           vdIndex,
                                           0,  /* pageOffset */
                                           pageCnt,
                                           startBlk ? vdIndex : -1,
                                           -1,  /* any disk block */
                                           forceFlag,
                                           BS_ALLOC_DEFAULT
                                           );
                        /*
                         * mig_migrate will return E_PAGE_NOT_MAPPED if
                         * there are no pages of the file on the given
                         * disk. This may happen because bmt_read()
                         * finds an XTRA_XTNTS mcell, migrates the extents
                         * and then finds the primary mcell and tries to
                         * migrate the extents again.
                         */
                        if (sts != EOK && sts != E_PAGE_NOT_MAPPED) {
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
                        DDLACTIVE_UNLOCK( &(ddlVd->ddlActiveLk) )
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
                    if (sts == ENO_MORE_MCELLS) {
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
                 * NOTE: Assume bitfile starts at page 0.
                 */
                pageCnt = bfap->nextPage;
                if (pageCnt > 0) {
                    sts = mig_migrate (
                                       bfap,
                                       vdIndex,
                                       0,  /* pageOffset */
                                       pageCnt,
                                       startBlk ? vdIndex : -1,
                                       -1,  /* any disk block */
                                       forceFlag,
                                       BS_ALLOC_DEFAULT
                                       );
                    /*
                     * mig_migrate will return E_PAGE_NOT_MAPPED if
                     * there are no pages of the file on the given
                     * disk. This may happen because bmt_read()
                     * finds an XTRA_XTNTS mcell, migrates the extents
                     * and then finds the primary mcell and tries to
                     * migrate the extents again.
                     */
                    if (sts != EOK && sts != E_PAGE_NOT_MAPPED) {
                        RAISE_EXCEPTION (sts);
                    }
                }
            }

            closeBitfileFlag = 0;
            if (do_vrele)
            {
                vrele(bfap->bfVp);
            }
            else
            {
                sts = bs_close(bfap,0);
            }

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            closeBfSetFlag = 0;
            bs_bfs_close (bfSetp, FtxNilFtxH, BFS_OP_DEF);

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

                    DDLACTIVE_LOCK_WRITE( &(ddlVd->ddlActiveLk) )
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

    if ((sts != E_PAGE_NOT_MAPPED) && (sts != E_LAST_PAGE)) {
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
        DDLACTIVE_UNLOCK( &(ddlVd->ddlActiveLk) )
    }

    if (closeBitfileFlag != 0) {
        if (do_vrele)
        {
            vrele(bfap->bfVp);
        }
        else
        {
            bs_close(bfap,0);
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

        DDLACTIVE_LOCK_WRITE( &(vd->ddlActiveLk) )

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

        DDLACTIVE_UNLOCK( &(vd->ddlActiveLk) )

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
        sts = ENO_MORE_MCELLS;
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
    mutex_lock (&(dmnP->mutex));
    DDLACTIVE_UNLOCK( &(vd->ddlActiveLk) );

    /*
     * Set the mcellId so that this entry's processing thread wakes up this
     * thread.
     */
    vd->ddlActiveWaitMCId = mcellId;

    /*
     * Wait for the entry to be processed.
     */
    cond_wait(&vd->ddlActiveWaitCv, &(dmnP->mutex));

    vd->ddlActiveWaitMCId = bsNilMCId;
    mutex_unlock (&(dmnP->mutex));

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

static
vd_remove(
          domainT* dmnP,  /* in */
          vdT* vdp,  /* in */
          int flags  /* in */
          )
{
    bfTagT bfTag;
    struct bsVdAttr* vdAttr;
    statusT sts;
    bfPageRefHT pgref;
    struct bsMPg* rbmtpgp;
    int error;
    struct nameidata *ndp = &u.u_nd;

    /* Set the vdState to BSR_VD_ZOMBIE to block any threads from getting
     * new access to this vdp.  Threads that have already bumped the 
     * vdRefCnt can continue until they drop their refcnt.  If there are
     * any other threads with access to this vdp, we must wait for them to
     * finish.
     */
    mutex_lock( &vdp->vdStateLock );
    vdp->vdState = BSR_VD_ZOMBIE;
    vdp->vdSetupThd = current_thread();
    if (vdp->vdRefCnt) {
        vdp->vdRefWaiters++;
        assert_wait_mesg( (vm_offset_t)&vdp->vdRefWaiters, FALSE, "vdRefCnt" );
        mutex_unlock( &vdp->vdStateLock );
        thread_block();
    } else
        mutex_unlock( &vdp->vdStateLock );

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
        int mcell_index;
        struct bfAccess *mdap;

        if ( RBMT_THERE(dmnP) ) {
            mdap = vdp->rbmtp;
            mcell_index = BFM_RBMT_EXT;
        } else {
            mdap = vdp->bmtp;
            mcell_index = BFM_BMT_EXT_V3;
        }

        sts = bs_pinpg( &pgref, (void*)&rbmtpgp, mdap, 0, BS_NIL );
        if (sts == EOK) {
            vdAttr = bmtr_find(&rbmtpgp->bsMCA[mcell_index], BSR_VD_ATTR,
                                                        vdp->dmnP);
            if (vdAttr == NULL) {
                bsUnpinModeT throwaway = { BS_RECYCLE_IT, BS_NOMOD };
                (void) bs_unpinpg(pgref, logNilRecord, throwaway);
                sts = ENO_BS_ATTR;
            } else {
                bsUnpinModeT writethru = { BS_RECYCLE_IT, BS_MOD_SYNC };

                if (flags & VD_STATE_CHANGE)
                    vdAttr->state = BSR_VD_DISMOUNTED;

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
         * NOTE: with addition of lgr_checkpoint_log() call in
         * bs_vd_remove_active(), this call may no longer be needed.
         */
        if (flags & VD_REMOVE_PERM)
            lgr_checkpoint_log (vdp->dmnP);
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

    if ( RBMT_THERE(dmnP) ) {
        BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_RBMT);
        bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->rbmtp);
        vdp->rbmtp = NULL;
        BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_BMT);
        bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->bmtp);
        vdp->bmtp = NULL;
    } else {
        BS_BFTAG_RSVD_INIT (bfTag, vdp->vdIndex, BFM_BMT_V3);
        bs_invalidate_rsvd_access_struct (dmnP, bfTag, vdp->bmtp);
        vdp->bmtp = NULL;
    }

    /* Make sure all I/O has completed.  */

    while( vdp->active.state == ACTIVE_DISK ||
           vdp->consolQ.ioQLen   ||
           vdp->flushQ.ioQLen    ||
           vdp->ubcReqQ.ioQLen   ||
           vdp->blockingQ.ioQLen ||
           vdp->devQ.ioQLen) {

        bs_startio( vdp, IO_FLUSH );

        /* Wait for device to become quiescent.  */
        mutex_lock( &vdp->devQ.ioQLock );
        lk_wait_for( &vdp->active, &vdp->devQ.ioQLock, INACTIVE_DISK );
        mutex_unlock( &vdp->devQ.ioQLock );
    }

    /*
     * Remove vd struct from domain's vd table and decrement
     * number of vds associated with this domain.  Do this prior
     * to waiting for vdRefCnt to go to zero to prevent other
     * threads from trying to get a ref on this vd.
     */

    mutex_lock( &dmnP->vdpTblLock );
    dmnP->vdCnt--;
    MS_SMP_ASSERT(TEST_VDI_RANGE(vdp->vdIndex) == EOK);
    dmnP->vdpTbl[vdp->vdIndex - 1] = NULL;
    mutex_unlock( &dmnP->vdpTblLock );

    /*
     * Wait for bs_io_thread() vd_dec_refcnt()
     */
    mutex_lock( &vdp->vdStateLock );
    vdp->vdSetupThd = current_thread();
    if (vdp->vdRefCnt) {
        vdp->vdRefWaiters++;
        assert_wait_mesg((vm_offset_t)&vdp->vdRefWaiters, FALSE,"vdRefCnt");
        mutex_unlock(&vdp->vdStateLock);
        thread_block();
    } else {
        mutex_unlock(&vdp->vdStateLock);
    }

    /*
     *  Check and wait for start_io_posted to clear
     */
    mutex_lock( &vdp->devQ.ioQLock );
    if (vdp->start_io_posted) {
        vdp->start_io_posted_waiter = 1;
        assert_wait_mesg( (vm_offset_t) &vdp->start_io_posted_waiter, FALSE, "start_io_posted_waiter" );
        mutex_unlock( &vdp->devQ.ioQLock );
        thread_block();
    } else {
        mutex_unlock( &vdp->devQ.ioQLock );
    }

    (void)setmount(vdp->devVp, SM_CLEARMOUNT );

    VOP_CLOSE( vdp->devVp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );

    /* Nothing done with the error return above now */
    vrele( vdp->devVp );

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
 * Note that the global DmnTblLock, seized in bs_vd_add_active(),
 * prevents the slot returned by this routine from being used by another 
 * addvol thread until the vdpTbl[] slot is filled in with the pointer 
 * to the newly-allocated vdT struct.
 *
 * Returns either a vdIndexT (one based index) or 0.
 *
 * SMP - seizes the dmnP->vdpTblLock while searching the dmnP->vdpTbl[].
 */
static int
vd_alloc_index(
               domainT *dmnP
               )
{
    int vdi;

    mutex_lock( &dmnP->vdpTblLock );

    for (vdi = 1; vdi <= BS_MAX_VDI; vdi++) {
        if ( TEST_VDI_RANGE(vdi)      == EOK &&
             TEST_VDI_SLOT(vdi, dmnP) != EOK ) {
            mutex_unlock( &dmnP->vdpTblLock );
            return (vdi);
        }
    }

    mutex_unlock( &dmnP->vdpTblLock );
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
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    int mcell_index;
    struct bfAccess *mdap;

    MS_SMP_ASSERT(lock_holder(&DmnTblLock));
    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vd->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vd->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }
    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);
    if (sts != EOK) {
        return sts;
    }

    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_MATTR,
                                                              dmnP);

    if (dmnMAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
        return EBMTR_NOT_FOUND;
    }

    if (op == BSR_VD_ADDVOL) {
        dmnMAttr->vdCnt++ ;
    } else {/* (op == BSR_VD_RMVOL) */
        dmnMAttr->vdCnt-- ;
    } 

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        /*
         * This domain was created before BSR_DMN_TRANS_ATTR was born.
         * Assign one now.
         */
        dmnTAttr = bmtr_assign( BSR_DMN_TRANS_ATTR,
                                sizeof( bsDmnTAttrT ),
                                &(bmtPage->bsMCA[mcell_index]) );
        if (dmnTAttr == NULL) {
            bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
            return EBMTR_NOT_FOUND;
        }
        dmnTAttr->chainVdIndex = -1;
        dmnTAttr->chainMCId = bsNilMCId;
    }

    dmnTAttr->op = op;
    dmnTAttr->dev = rdev;

    sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    if (sts != EOK) {
        /*
         * FIX - Should we restore the previous value? Is the page still pinned?
         */
        return sts;
    }

    return EOK;
}  /* end set_disk_attrs */

/*
 * check_trans_attrs
 *
 * This routine is called when (dmnP->vdCnt != dmnMAttr.vdCnt). We
 * look in the domain's transient attribute record to see if a
 * previous addvol or rmvol was interrupted. If so, we attempt to
 * fix the domain.
 * 
 * In case of rmvol, we construct the symbolic link name in /etc/fdmns
 * that needs to be removed. The calling routine cleans up the domain,
 * removes the link and starts all over again.
 *
 * In case of addvol, due to device naming restrictions, it is not
 * possible to construct the link in /etc/fdmns from the dev_t. Instead,
 * we roll back the addvol and remove it from on-disk structures. Since
 * this volume hasn't been added to service class yet (see addvol 
 * command), no new storage allocations have been done from this volume.
 */
statusT
check_trans_attrs(
                  domainT *dmnP,  /* in */
                  int dmnMAttrIdx, /* in */
                  int *onDiskCnt, /* in/out */
                  char* volPathName /* out - pathname of vol link */
                  )
{
    vdT *vdp;
    bsMPgT *bmtPage;
    bfPageRefHT pgPin;
    statusT sts = EOK;
    struct bfAccess *mdap;
    bsDmnTAttrT *dmnTAttr;
    bsDmnMAttrT *dmnMAttr;
    int mcell_index;
    int vdi, vdCnt = 0;
    char *subs;

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vdp->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }

    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);
    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        return EBMTR_NOT_FOUND;
    }

    sts = E_VOLUME_COUNT_MISMATCH;
    if (dmnTAttr->dev) {
        if ((dmnP->vdCnt > *onDiskCnt) && (dmnTAttr->op == BSR_VD_RMVOL)) {
            /*
             * /etc/fdmns has an extra link because a prior rmvol did
             * not complete. Figure out which one and return it so that
             * it can be deleted.
             */
            for ( vdi = 0; vdCnt < dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi+1, dmnP, FALSE, FALSE)) ) {
                    continue;
                }

                vdCnt++;

                if (vdp->devVp->v_rdev == dmnTAttr->dev) {
                    strcpy(volPathName, MSFS_DMN_DIR);
                    strcat(volPathName, "/");
                    strcat(volPathName, dmnP->domainName);

                    subs = (char *)strrchr(dmnP->vdpTbl[vdi]->vdName, '/');

                    strcat(volPathName, subs);
                    sts = E_TOO_MANY_VIRTUAL_DISKS;
                    break;
                }
            }
            /*
             * Don't write the page out until after /etc/fdmns is fixed.
             */
            (void)bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        } else if ((dmnP->vdCnt < *onDiskCnt) && (dmnTAttr->op == BSR_VD_ADDVOL)) {
            /*
             * Not enough links in /etc/fdmns, possibly because a prior
             * addvol did not complete. Decrement the ondisk vdCount to
             * reflect this.
             */
            dmnMAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_MATTR,
                                                                  dmnP);
            MS_SMP_ASSERT(dmnMAttr);
            (*onDiskCnt)--;
            dmnMAttr->vdCnt--;
            sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
        } 
    } else {
        (void)bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
    }
    return sts;
}

/*
 * clear_trans_attrs
 *
 * This domain has reconciled all vdCnt related discrepancies, if any.
 * Clear out the transient attribute record.
 */
statusT
clear_trans_attrs(
                  domainT *dmnP,  /* in */
                  int dmnMAttrIdx  /* in */
                 )
{
    vdT *vdp;
    bsMPgT *bmtPage;
    bfPageRefHT pgPin;
    statusT sts = EOK;
    struct bfAccess *mdap;
    int mcell_index;
    bsDmnTAttrT *dmnTAttr;

    vdp = VD_HTOP(dmnMAttrIdx, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vdp->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }

    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);
    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
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
 * a discrepancy is detected between /etc/fdmns/<root_domain> and 
 * on-disk data. Note that for root, the domain has already been
 * activated.
 */

statusT
bs_fix_root_dmn(
                bfDmnDescT *dmntbl,
                domainT *dmnP,
                char *dmnName
                )
{
    statusT      sts=EOK;
    vdT         *vdp;
    int          error, mcell_index, vdi, idx, idx2, vdCnt = 0;
    bfPageRefHT  pgPin;
    bsMPgT      *bmtPage;
    bsDmnTAttrT *dmnTAttr;
    bsDmnMAttrT *dmnMAttr;
    char         volPathName[MAXPATHLEN + 1];
    char        *subs;
    opIndexT     opIndex = OP_NONE;
    dev_t        dev;
    
    extern dev_t   global_rootdev[];
    extern uint32T global_rootdev_count;
    
    struct nameidata *ndp = &u.u_nd;
    struct bfAccess  *mdap;

    extern struct vnode **GlobalRootVpp;

    vdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vdp->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }

    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);

    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
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
             * /etc/fdmns/<domain_name> has an extra link. Find which
             * one and remove it.
             */
            for ( vdi = 0; vdi < dmntbl->vdCount; vdi++) {
                ndp->ni_nameiop = LOOKUP | FOLLOW;
                ndp->ni_segflg = UIO_SYSSPACE;
                ndp->ni_dirp = dmntbl->vddp[vdi]->vdName;
                if( (sts = (statusT)namei(ndp)) != EOK ) {
                    goto done;
                }
                if (ndp->ni_vp->v_rdev == dmnTAttr->dev) {
                    strcpy(volPathName, MSFS_DMN_DIR);
                    strcat(volPathName, "/");
                    strcat(volPathName, dmnName);
                    subs = (char *)strrchr(dmntbl->vddp[vdi]->vdName, '/');
                    MS_SMP_ASSERT(subs != NULL);
                    strcat(volPathName, subs);
        
                    ms_uprintf("Interrupted rmvol of volume %s in domain %s.\n", 
                            subs+1, dmnName);
                    ms_uprintf("    To finish rmvol cleanup, type the command:\n");
                    ms_uprintf("    /sbin/disklabel -sF %s unused\n", subs+1);

                    ndp->ni_nameiop = DELETE | WANTPARENT;
                    ndp->ni_segflg = UIO_SYSSPACE;
                    ndp->ni_dirp = volPathName;

                    if ( (sts = (statusT)namei(ndp)) != EOK ) {
                        goto done;
                    }
                    VOP_REMOVE(ndp, error);
                    if ((sts = (statusT)error) != EOK) goto done;

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
             * /etc/fdmns/<domain_name> has fewer links. A prior addvol
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
                     */
                    if (clu_is_ready()) {
                        opIndex = ADVFS_REM_VOLUME;
                        if (error = CC_DOMAIN_CHANGE_VOL_DEV(
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
                    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), 
                                          BSR_DMN_MATTR, dmnP);
                    if (dmnMAttr == NULL) {
                        sts = EBMTR_NOT_FOUND;
                        goto done;
                    }

                    dmnMAttr->vdCnt--;

                    /* change vd on-disk state so it is permanently removed
                     * and update/remove corresponding in-memory data.
                     */
                    vd_remove(dmnP, vdp, VD_STATE_CHANGE | VD_REMOVE_PERM);

                    for (idx = 0; idx < global_rootdev_count; idx++) {
                        if (global_rootdev[idx] == dmnTAttr->dev)
                            break;
                    }

                    MS_SMP_ASSERT(idx < global_rootdev_count);

                    /*
                     * Update global rootdev array.
                     */
                    global_rootdev_count--;
                    if (idx != global_rootdev_count) {
                        for (idx2 = idx; idx2 < global_rootdev_count; idx2 ++) {
                            global_rootdev[idx2] = global_rootdev[idx2+1];
                            GlobalRootVpp[idx2] = GlobalRootVpp[idx2+1];
                        }
                    }
                    global_rootdev[global_rootdev_count] = (dev_t)NULL;
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

    if (clu_is_ready() && (opIndex == ADVFS_REM_VOLUME)) {
        (void)CC_DOMAIN_CHANGE_VOL_DONE_DEV(&opIndex,
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
    int mcell_index, vdi, vdCnt = 0;
    bfPageRefHT pgPin;
    bsMPgT *bmtPage;
    bsDmnTAttrT *dmnTAttr;

    vdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vdp->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }

    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);

    if (sts != EOK) {
        return sts;
    }

    dmnTAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_TRANS_ATTR,
                                                              dmnP);

    if (dmnTAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
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
                    int value        /* in */
                    )
{
    bsMPgT *bmtPage;
    bsDmnMAttrT *dmnMAttr;
    bfPageRefHT pgPin;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    int mcell_index;
    struct bfAccess *mdap;

    vdIndex = BS_BFTAG_VDI (dmnP->ftxLogTag);
    vd = VD_HTOP(vdIndex, dmnP);

    if ( RBMT_THERE(dmnP) ) {
        mdap = vd->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = vd->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }
    sts = bs_pinpg (&pgPin, (void*)&bmtPage, mdap, 0, BS_NIL);
    if (sts != EOK) {
        return sts;
    }

    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_MATTR,
                                                              dmnP);

    if (dmnMAttr == NULL) {
        bs_unpinpg (pgPin, logNilRecord, BS_DIRTY); /* Ignore status. */
        return EBMTR_NOT_FOUND;
    }

    dmnMAttr->recoveryFailed = value;

    sts = bs_unpinpg (pgPin, logNilRecord, BS_WRITETHRU);
    if (sts != EOK) {
        return sts;
    }
    return EOK;
}  /* end set_recovery_failed */

/*******************************************************************/
/*****  Various utility routines supporting the major functions ****/
/*******************************************************************/
void
bs_bfdmn_flush_bfrs(
                    domainT* dmnP
                    )
{
    int vdi;
    vdT* vdp;
    int vdCnt;

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        } 

        vdCnt++;
        bs_bflush( vdp );
        vd_dec_refcnt( vdp );
    }
}

/*
 * bs_bfdmn_flush( dmnP )
 *
 * Flush the log and all dirty buffers in
 * the bfdmn.  This queues the writes but does not wait for them.
 */
void
bs_bfdmn_flush(
               domainT* dmnP
               )
{
    if ( dmnP->ftxLogP) {
        /*
         * If the log is open, flush it.
         */

        lgr_flush( dmnP->ftxLogP );
    }

    bs_bfdmn_flush_bfrs( dmnP );
}

/*
 * bs_bfdmn_flush_sync
 *
 * Wait for all vd i/o queues to become
 * inactive.  Return a count of how many pinned buffers remain.  This
 * may change in the future, such that the access structures should be
 * scanned for pinned buffers instead, as this only catches buffers
 * that have already been unpinned dirty once and been subsequently
 * repinned.
 */
int
bs_bfdmn_flush_sync(
                    domainT* dmnP
                    )
{
    int vdi = 1;
    int rem = 0;
    vdT* vdp;
    int vdCnt;

    /*
     * Wait for the vd's to complete the flush.
     */

    vdCnt = 0;
    for ( vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {

        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        } 

        vdCnt++;
        rem += bs_bflush_sync( vdp );
        vd_dec_refcnt( vdp );
    }
    return rem;
}


/*
 * dmn_dealloc
 *
 * Deallocates a domain structure and all structures that it points to.
 */
static void
dmn_dealloc(
            domainT *dmnP    /* in - domain table pointer */
            )
{
    domainT *tmpdmnP;
    serviceClassTblT *scTbl;
    scEntryT *scEntry;
    int tblPos;

    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    MS_SMP_ASSERT(DmnSentinelP != NULL);

    mutex_lock( &DmnTblMutex );

    MS_SMP_ASSERT(dmnP->activateCnt == 0);

    if (dmnP->dmnFwd && dmnP->dmnBwd)    /* remove from sentinel chain */
    {
        /* unlink from other domains */
        if (dmnP == DmnSentinelP)
            DmnSentinelP = dmnP->dmnFwd;
        dmnP->dmnBwd->dmnFwd = dmnP->dmnFwd;
        dmnP->dmnFwd->dmnBwd = dmnP->dmnBwd;
        if ((--DomainCnt) == 0)
        {
            DmnSentinelP = NULL;
        }
        MS_SMP_ASSERT(DomainCnt >= 0);
        DOMAIN_HASH_REMOVE(dmnP, TRUE);   /* remove from dyn hash chain */
    }
    if (dmnP->dmnAccCnt) {
        dmnP->dmnRefWaiters++;
        assert_wait_mesg( (vm_offset_t)&dmnP->dmnRefWaiters, FALSE,
            "dmnAccCnt" );
        mutex_unlock( &DmnTblMutex );
        thread_block();
    } else {
        mutex_unlock( &DmnTblMutex );
    }

    MS_SMP_ASSERT( BFSET_VALID(dmnP->bfSetDirp) ) 

    BFSETTBL_LOCK_WRITE( dmnP )

    bfs_dealloc( dmnP->bfSetDirp, TRUE );
    dmnP->bfSetDirp = 0;     

    BFSETTBL_UNLOCK(dmnP )

    if (dmnP->scTbl != NULL) {
        scTbl = dmnP->scTbl;
        for (tblPos = 0; tblPos < scTbl->numClasses; tblPos++) {
            scEntry = &scTbl->classes[tblPos];
            if (scEntry->vdLst != NULL) {
                ms_free(scEntry->vdLst);
            }
        }

        ms_free( dmnP->scTbl );
    }

    if (dmnP->ftxTbld.tablep != NULL) {
        ms_free( dmnP->ftxTbld.tablep );
    }

    BFSETTBL_LOCK_DESTROY(dmnP);
    SC_TBL_LOCK_DESTROY(dmnP);
    RMVOL_TRUNC_LOCK_DESTROY(dmnP);
    mutex_destroy( &dmnP->mutex );
    mutex_destroy( &dmnP->lsnLock );
    mutex_destroy( &dmnP->vdpTblLock );
    mutex_destroy( &dmnP->dmnFreezeMutex );
#ifdef ADVFS_SMP_ASSERT
    lock_terminate(&dmnP->ftxSlotLock);
#endif

    if (clu_is_ready()) 
    {
        ulong domainId_cast;
        /* We are passing the actual domainId instead of a pointer
         * to it. Rather than try to cast a non-scalar just copy
         * it into a scalar container. See the timeout callers to
         * see why.
         */

        bcopy(&dmnP->domainId,&domainId_cast,sizeof(ulong));

        untimeout((void(*))cfs_xid_free_memory, 
                     (char *) domainId_cast);
 
        XID_RECOVERY_LOCK_WRITE(dmnP);
        cfs_xid_free_memory_int(dmnP);
        XID_RECOVERY_UNLOCK(dmnP);
    }
    XID_RECOVERY_LOCK_DESTROY(dmnP);
    dmnP->dmnMagic |= MAGIC_DEALLOC;
    ms_free( dmnP );
}

/*
 * dmn_alloc
 *
 * Routine to initialize a new Domain struct * and add it to the DomainHashTbl
 *
 * Assumes DmnTbl lock is held.
 *
 * Returns a positive handle and EOK if successful.
 */

static statusT
dmn_alloc(
    bfDomainIdT domainId,
    char *domainName,
    uint32T domainMajor,
    bfTagT dirTag,
    struct bsMPg *pgp,
    domainT **newdmnPP
    )
{
    domainT *dmnP = NULL;
    int i, slot = 0;
    bfSetIdT rootBfSetId;
    statusT sts = EOK;

    dmnP = (struct domain*) ms_malloc( sizeof( struct domain ) );
    if ( !dmnP ) {
        sts = ENO_MORE_MEMORY;
        goto _error_cleanup;
    }

    /*
     * "nilDomain" is all zeros so only non-zero fields need
     * to be initialized.
     */

    *dmnP = nilDomain;

    RMVOL_TRUNC_LOCK_INIT(dmnP);
    mutex_init3(&dmnP->mutex,   0, "DomainMutex",   ADVdomainT_mutex_lockinfo);
    mutex_init3(&dmnP->lsnLock, 0, "DomainLsnLock", ADVdomainT_lsnLock_lockinfo);
    mutex_init3(&dmnP->vdpTblLock, 0, "vdpTblLock", ADVdomainT_vdpTbl_lockinfo);
    mutex_init3(&dmnP->dmnFreezeMutex, 0, "dmnFreezeMutex", ADVdomainT_freeze_lockinfo);

    SC_TBL_LOCK_INIT(dmnP);
#ifdef ADVFS_SMP_ASSERT
    lock_setup(&dmnP->ftxSlotLock, ADVdomainT_ftxSlotLock_lk_info, TRUE);
#endif
    dmnP->lsnList.lsnFwd = (struct bsBuf *)&dmnP->lsnList;
    dmnP->lsnList.lsnBwd = (struct bsBuf *)&dmnP->lsnList;
    dmnP->writeToLsn = nilLSN;
    cv_init( &dmnP->pinBlockCv );
    dmnP->pinBlockWait = 0;
    dmnP->pinBlockBuf = NULL;
    dmnP->pinBlockRunning = FALSE;
    dmnP->contBits = 0;
    dmnP->dmnFlag = BFD_NORMAL;
    dmnP->dmnMagic = DMNMAGIC;
    dmnP->logVdRadId = -1; /* set the field to 'invalid' */
    dmnP->dmnVersion = 0;
    dmnP->mountCnt = 0;

    dmnP->scTbl = sc_init_sc_tbl();
    if (dmnP->scTbl == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error_cleanup;
    }

    bzero( (char *)&dmnP->bcStat, sizeof( struct bcStat ) );

    dmnP->fs_full_time = 0;

    /*
     * Allocate and initialize the ftx table
     * and associated variables.
     */

    dmnP->ftxTbld.rrSlots = FTX_DEF_RR_SLOTS;
    dmnP->ftxTbld.rrNextSlot = 0;
    dmnP->ftxTbld.nextNewSlot = FTX_DEF_RR_SLOTS; /* so ftx_fail() works */
    cv_init( &dmnP->ftxTbld.slotCv );
    cv_init( &dmnP->ftxTbld.trimCv );
    cv_init( &dmnP->ftxTbld.excCv );

    dmnP->ftxTbld.tablep =
        (ftxSlotT*) ms_malloc( sizeof( ftxSlotT ) * FTX_MAX_FTXH );

    if ( !dmnP->ftxTbld.tablep ) {
        sts = ENO_MORE_MEMORY;
        goto _error_cleanup;
    }

    for (slot = 0; slot < FTX_MAX_FTXH; slot++) {
        dmnP->ftxTbld.tablep[slot].ftxp  = NULL;

        if (slot < dmnP->ftxTbld.rrSlots) {
            dmnP->ftxTbld.tablep[slot].state = FTX_SLOT_AVAIL;
        } else {
            dmnP->ftxTbld.tablep[slot].state = FTX_SLOT_UNUSED;
        }
    }

    /*
     * Initialize the ftx crash recovery data structures.
     */
    ftx_init_recovery_logaddr( dmnP );

    XID_RECOVERY_LOCK_INIT(dmnP);
    dmnP->xidRecovery.head = NULL;
    dmnP->xidRecovery.tail = NULL;
    dmnP->xidRecovery.current_free_slot = 0;
    dmnP->xidRecovery.timestamp.tv_sec = 0;
    dmnP->xidRecovery.timestamp.tv_usec = 0;

    /*
     * Fabricate the bitfile-set ID for the root bitfile-set and
     * create BF-set descriptor for it in the BF-set table.
     */
    rootBfSetId.domainId = domainId;
    rootBfSetId.dirTag = dirTag;

    ftx_lock_init( &dmnP->BfSetTblLock, &dmnP->mutex, ADVBfSetTblLock_info);

    dmnP->bfSetHead.bfsQfwd = &dmnP->bfSetHead;
    dmnP->bfSetHead.bfsQbck = &dmnP->bfSetHead;

    sts = bs_bfs_add_root( rootBfSetId, dmnP, &dmnP->bfSetDirp );
    if (sts != EOK) {
        goto _error_cleanup;
    }

    /* Set up pointer to RBMT Page 0 buffer */
    dmnP->metaPagep = pgp;

    /* Add domain to dynamic hash table */
    mutex_lock( &DmnTblMutex );
    dmnP->domainId   = domainId;
    dmnP->majorNum   = domainMajor;
    strncpy( dmnP->domainName, domainName, BS_DOMAIN_NAME_SZ - 1 );
    /*
     *  link to other domains
     */

    if (DmnSentinelP != NULL)
    {
        /* here: we already have a domain(s) in the list,
        * link it to the end of the chain, this leaves DmnSentinelP
        * as the first mounted advfs domain (possibly root )
        */
        dmnP->dmnBwd = DmnSentinelP->dmnBwd;
        (dmnP->dmnBwd)->dmnFwd = dmnP;
        dmnP->dmnFwd = DmnSentinelP;
        DmnSentinelP->dmnBwd = dmnP;
    }
    else /* DmnSentinelP == NULL */
    {
        /* here: must be first advfs domain -  link to itself
        * and set DmnSentinelP
        */
        DmnSentinelP = dmnP->dmnFwd = dmnP->dmnBwd = dmnP;
    }
    dmnP->dmnHashlinks.dh_key = DOMAIN_GET_HASH_KEY( domainId );
    DOMAIN_HASH_INSERT( dmnP, TRUE );
    DomainCnt++;
    dmnP->dmnAccCnt=0;
    dmnP->dmnRefWaiters=0;
    mutex_unlock( &DmnTblMutex );

    *newdmnPP = dmnP;
    return EOK;

_error_cleanup:
    if (dmnP != NULL) {
        dmn_dealloc( dmnP );
    }

    return sts;
}

/*
 * domain_lookup
 *
 * Return a pointer to the requested domain.
 *
 * Assumes DmnTbl lock is held.
 *
 * Returns zero if domain not found.
 */

domainT *
domain_lookup(
              bfDomainIdT bfDomainId,    /* in - domain to lookup */
              u_long flag                /* in - mount flags/check DmnTblLock? */
              )
{
    int key;
    domainT *dmnP_start, *dmnP;

    MS_SMP_ASSERT((flag & M_GLOBAL_ROOT) ? 1 : 
                   SLOCK_HOLDER(&DmnTblMutex.mutex) || lock_holder(&DmnTblLock));

    key = DOMAIN_GET_HASH_KEY( bfDomainId );
    dmnP_start = DOMAIN_HASH_LOCK( key, NULL);  /* get bucket */

    if (TEST_DMNP(dmnP_start) == EOK)
    {
    /* we have a bucket with domains - scan the chain for bfDomainId */
        dmnP = dmnP_start;
        do
        {
            if (BS_UID_EQL(dmnP->domainId, bfDomainId))
            {
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
 * Return a handle for the requested domain.
 *
 * ASsumes DmnTbl lock is held.
 *
 * Returns zero if domain not found.
 */

domainT *
domain_name_lookup(
                   char* dmnName, /* in - domain to lookup */
                   u_long flag    /* in - mount flags/check DmnTblLock? */
                   )
{
    domainT *dmnP_Prev, *dmnP;

    if (!(flag & (M_GLOBAL_ROOT|M_GLROOT_OTHER))) {
        MS_SMP_ASSERT(lock_islocked(&DmnTblLock));
    }

    dmnP = DmnSentinelP;    /* grab starting point */
    do
    {
        if (TEST_DMNP(dmnP) == EOK)
        {
            if (strcmp(dmnP->domainName, dmnName) == 0)
            {
                return dmnP;
            }
            dmnP_Prev = dmnP;
            dmnP = dmnP->dmnFwd;
        }
        else
        {
            /* TEST_DMNP() failed.  Return a zero pointer. */
            return 0;
        }
    /*
     * Just in case there may be a problem with the pointers, check to
     * see that the dmnBwd pointers are correct.  Being a rare error,
     * we just assert the following.
     */
    MS_SMP_ASSERT(dmnP_Prev == dmnP->dmnBwd);
    } while (dmnP != DmnSentinelP);

    /* domain not found - return zero */
    return 0;
}

/*
 * get_bmt_pgptr
 *
 * Return addr of static bmt page
 */

struct bsMPg *
get_bmt_pgptr(
              domainT *dmnP
              )
{
    return (struct bsMPg *)dmnP->metaPagep;
}

/*
 * bs_dmn_change
 *
 * Used to change the owner, group and access mode of domain.
 */

statusT
bs_dmn_change(
              char *dmnTbl,       /* in - domain table file name */
              int newUid,         /* in - indicates if UID is to be changed */
              uid_t uid,          /* in - new UID */
              int newGid,         /* in - indicates if GID is to be changed */
              gid_t gid,          /* in - new GID */
              int newMode,        /* in - indicates if MODE is to be changed */
              mode_t mode         /* in - new MODE */
              )
{
    statusT sts;
    bfDomainIdT dmnId;
    domainT *dmnP;
    bfDmnParamsT *dmnParamsp = NULL;
    int dmnActive = FALSE, dmnOpen = FALSE;

    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

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

    sts = bs_get_dmn_params( dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_owner( dmnParamsp->uid )) {
        /* Only the owner of the domain can change it */
        sts = E_NOT_OWNER;
        goto _error;
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Owner must have write access to change the domain */
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    if (newUid) {
        dmnParamsp->uid = uid;
    }
    if (newGid) {
        dmnParamsp->gid = gid;
    }
    if (newMode) {
        dmnParamsp->mode = mode;
    }

    sts = bs_set_dmn_params( dmnP, dmnParamsp );
    if (sts != EOK) {
        goto _error;
    }

    bs_domain_close( dmnP );
    (void) bs_bfdmn_deactivate( dmnId, 0 );

    ms_free( dmnParamsp );
    return EOK;

_error:
    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, 0 );
    }

    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }
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
    mutex_init3( &DmnTblMutex, 0, "DmnTblMutex", ADVDmnTblMutex_lockinfo );
    lock_setup( &DmnTblLock, ADVDmnTblLock_info, TRUE );

    /* create and init dynamic hash table */
    DomainHashTbl = dyn_hashtable_init( DOMAIN_HASH_INITIAL_SIZE,
                                        DOMAIN_HASH_CHAIN_LENGTH,
                                        DOMAIN_HASH_ELEMENTS_TO_BUCKETS,
                                        DOMAIN_HASH_USECS_BETWEEN_SPLITS,
                                        offsetof(domainT, dmnHashlinks),
                                        ADVDomainHashChainLock_lockinfo,
                                        NULL);

    if (DomainHashTbl == NULL)
    {
        ADVFS_SAD0("bs_domain_init: can't get space for domain hash table");
    }

}

/*
 * get_domain_disks
 *
 * Gets the list of disks that belong to the specified domain.  Also
 * gets the domains fake device major number (see comments for
 * bs_bfset_get_dev()).
 *
 * The basic idea is that each domain had a directory in /etc/msfs.  The
 * directory name is the same as the domain name (in fact, the directory
 * name defines the domain name since the domain name is not stored
 * anywhere else).  So, the domain 'foo' has a directory named /etc/msfs/foo.
 * Each domain directory contains one or more symbolic links to the domain's
 * devices.  We use symbolic links because it makes it easy to use 'ls'
 * to see what devices are in a domain; hardlinks can be obscure.  Also,
 * symbolic links don't have any limitations with respect to crossing
 * filesystem mount boundaries; this allows /etc/msfs to be in a
 * different filesystem from the device's special file.
 *
 * So, this routine opens /etc/msfs/<domain>, scans the directory
 * and gets the names of the devices in the domain.
 */

statusT
get_domain_disks(
                 char* domain,             /* in - domain name */
                 int doingRoot,            /* in - flag */
                 bfDmnDescT* dmnDescp      /* out - ptr to bfdmn desc */
                 )
{
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
    struct uio auio;
    struct iovec aiov;
    int error, i, eofflag = 0, dirOpen = FALSE, dirEntOffset = 0, vdi = 0;
    int vdNameLen;
    vdDescT *vddp;
    char *dirBuf = NULL, *vdName = NULL, *dmnName = NULL;
    struct dirent *dp = NULL;
    int dirbuflen = 0;
    struct kdirent *kdp = NULL;
    extern const char default_rootname[];
    extern uint32T global_rootdev_count;

    if (doingRoot & M_LOCAL_ROOT) {
        vddp = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
        if ( !vddp ) {
            error = ENOMEM;
            goto _error;
        }

        *vddp = nilVdDesc;
        strcpy(vddp->vdName, (char *) default_rootname);
        /*
         * Add disk descriptor to domain's list of disk descriptors.
         */
        dmnDescp->vddp[vdi] = vddp;
        dmnDescp->vdCount = 1;
        dmnDescp->dmnMajor = nblkdev + 1;

        return EOK;
    } else if (doingRoot & M_GLOBAL_ROOT) {
        for (vdi = 0; vdi < global_rootdev_count; vdi++) {
            vddp = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
            if ( !vddp ) {
                error = ENOMEM;
                goto _error;
            }

            *vddp = nilVdDesc;
            strcpy(vddp->vdName, (char *) default_rootname);

            dmnDescp->vddp[vdi] = vddp;
        }
        dmnDescp->vdCount = global_rootdev_count;
        dmnDescp->dmnMajor = nblkdev + 1;

        return EOK;
    }

    dmnName = ms_malloc( sizeof( MSFS_DMN_DIR ) + 1 + BS_DOMAIN_NAME_SZ );
    if (dmnName == NULL) {
        error = ENOMEM;
        goto _error;
    }

    dirBuf = ms_malloc( BS_BLKSIZE );
    if (dirBuf == NULL) {
        error = ENOMEM;
        goto _error;
    }

    vdName = ms_malloc( MAXPATHLEN+1 );
    if (vdName == NULL) {
        error = ENOMEM;
        goto _error;
    }

    if (strlen( domain ) >= BS_DOMAIN_NAME_SZ) {
        error = EINVAL;
        goto _error;
    }

    error = get_domain_major( domain, &dmnDescp->dmnMajor );
    if (error != EOK) {
        goto _error;
    }

    /*
     * Construct the full pathname for the domain's dir; /etc/msfs/<domain>.
     */
    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcpy( &dmnName[strlen( dmnName )], domain );

    /*
     * Construct the full pathname to be used for opening the device links.
     * /etc/msfs/<domain>/
     */
    strcpy( vdName, dmnName );
    strcat( vdName, "/" );
    vdNameLen = strlen( vdName );

    /* get the vnode */
    if( error = getvp( &vp, dmnName, ndp, UIO_SYSSPACE ) ) {
        goto _error;
    }

    /* do an access check on the dir */
    VOP_ACCESS( vp, VREAD, ndp->ni_cred, error );
    if( error ) {
        goto _error;
    }

    /* open the vnode */

    VOP_OPEN( &vp, FREAD | OTYP_MNT, ndp->ni_cred, error );
    if( error ) {
        goto _error;
    }

    dirOpen = TRUE;

    if (vp->v_type != VDIR) {
        /* /etc/msfs/<domain> must be directory */
        error = EINVAL;
        goto _error;
    }

    auio.uio_iov    = &aiov;
    auio.uio_iovcnt = 1;
    auio.uio_rw     = UIO_READ;
    auio.uio_segflg = UIO_SYSSPACE;
    auio.uio_offset = 0;

    /*
     * Scan /etc/msfs/<domain> for all the devices in the domain.
     */

    while (!eofflag) {
        aiov.iov_base  = dirBuf;
        aiov.iov_len   = BS_BLKSIZE;
        auio.uio_resid = BS_BLKSIZE;

        VOP_READDIR( vp, &auio, ndp->ni_cred, &eofflag, error );
        if (error) {
            goto _error;
        }

        dirEntOffset = 0;   /* set offset to first byte in the dir block */

        dirbuflen = BS_BLKSIZE - aiov.iov_len;

        if (vp->v_mount->m_flag & M_NEWRDDIR) {
            /*
             * Scan the current directory block.
             */

            while (dirEntOffset < dirbuflen) {
                /*
                 * Get a poiner to the next dir entry.
                 */
                kdp = (struct kdirent *) &dirBuf[ dirEntOffset ];

                dirEntOffset += kdp->kd_reclen; /* next dir entry offset */

                if (kdp->kd_ino == 0) {
                    /* skip deleted entries */
                    continue;
                }

                if ((strcmp( kdp->kd_name, ".") == 0) ||
                    (strcmp( kdp->kd_name, "..") == 0)) {
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
                 * /etc/msfs/<domain>/<d_name>
                 */
                strcat( vdName, kdp->kd_name );

                /*
                 * Get the contents of the symbolic link; the real device name.
                 */
                vddp = get_vdd( vdName, &error );
                if (vddp == NULL) {
                    if (error != EINVAL) {
                        /* If error is ENODEV return error so problem can be corrected */
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
        } else {
            /*
             * Scan the current directory block.
             */

            while (dirEntOffset < dirbuflen) {
                /*
                 * Get a poiner to the next dir entry.
                 */
                dp = (struct dirent *) &dirBuf[ dirEntOffset ];

                dirEntOffset += dp->d_reclen; /* move to next dir entry offset*/

                if (dp->d_ino == 0) {
                    /* skip deleted entries */
                    continue;
                }

                if ((strcmp( dp->d_name, ".") == 0) ||
                    (strcmp( dp->d_name, "..") == 0)) {
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
                 * /etc/msfs/<domain>/<d_name>
                 */
                strcat( vdName, dp->d_name );

                /*
                 * Get the contents of the symbolic link; the real device name.
                 */
                vddp = get_vdd( vdName, &error );
                if (vddp == NULL) {
                    if (error != EINVAL) {
                        /* If error is ENODEV return error so problem can be corrected */
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
    }

    VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);
    vrele( vp );
    ms_free( dirBuf );
    ms_free( vdName );
    ms_free( dmnName );

    if (dmnDescp->vdCount < 1) {
        /* Hmmm...  No device link names where found in the directory!? */
        return E_NO_DMN_VOLS;
    }

    return EOK;

_error:
    if (dirOpen) {
        VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);

    }

    if (vp != NULL) {
        vrele( vp );
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

static vdDescT *
get_vdd(
        char *vdName,       /* in - virtual disk name */
        int *err            /* out - error */
        )
{
    int error, i, vdOpen = FALSE;
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
    struct uio auio;
    struct iovec aiov;
    vdDescT *vdd = NULL;

    vdd = (vdDescT*)ms_malloc( sizeof( vdDescT ) );
    if ( !vdd ) {
        *err = ENOMEM;
        goto _error;
    }

    *vdd = nilVdDesc;

    /* get the vnode */

    ndp->ni_nameiop = LOOKUP | NOFOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = vdName;

    if( error = namei( ndp ) ) {
        *err = error;
        goto _error;
    }

    vp = ndp->ni_vp;

    if (vp->v_type != VLNK) {
        /* it must be a symbolic link */
        *err = EINVAL;
        goto _error;
    }

    /* do an access check on the dir */

    VOP_ACCESS( vp, VREAD, ndp->ni_cred, error );
    if( error ) {
        goto _error;
    }

    /* open the vnode */

    VOP_OPEN( &vp, FREAD | OTYP_MNT, ndp->ni_cred, error );
    if( error ) {
        *err = error;
        goto _error;
    }

    vdOpen = TRUE;

    aiov.iov_base   = vdd->vdName;
    aiov.iov_len    = sizeof( vdd->vdName );
    auio.uio_iov    = &aiov;
    auio.uio_iovcnt = 1;
    auio.uio_offset = 0;
    auio.uio_resid  = sizeof( vdd->vdName );
    auio.uio_segflg = UIO_SYSSPACE;
    auio.uio_rw     = UIO_READ;

    VOP_READLINK( vp, &auio, ndp->ni_cred, error );
    if( error ) {
        *err = error;
        goto _error;
    }

    /* Follow the link to see if the device special file has been
     * create. The device could have been moved. If the file does not
     * exist report ENODEV. Exit through _error so a NULL vdd pointer is
     * returned.
     */
    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = vdName;

    if( error = namei( ndp ) ) {
        *err = ENODEV;
        goto _error;
    }

    if(ndp->ni_vp->v_type != VBLK) {
        *err = ENOTBLK;
        goto _error;
    }

    vdd->device = ndp->ni_vp->v_rdev;       /* save for advfs_enum_domain_dev_ts() */

    vrele( ndp->ni_vp );

    VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);

    vrele( vp );
    *err = 0;
    return vdd;

_error:
    if (vdOpen) {
        VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);
    }

    if (vp != NULL) {
        vrele( vp );
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
                  char *domain,               /* in */
                  bsDevtListT *DevtList       /* out */
                 )
{
bfDmnDescT *tmpDmnTbl;           /* table for get_domain_disks */
statusT sts;
int     i;

    tmpDmnTbl = (bfDmnDescT *)  ms_malloc(sizeof(bfDmnDescT));
    if (tmpDmnTbl == NULL)
    {
        return ENO_MORE_MEMORY;
    }

    sts = get_domain_disks(domain, 0, tmpDmnTbl);
    if (sts != EOK )
    {
        ms_free(tmpDmnTbl);
        return sts;
    }

    DevtList->devtCnt = tmpDmnTbl->vdCount;

    for (i=0; i < tmpDmnTbl->vdCount; i++)
    {
        DevtList->device[i] = (tmpDmnTbl->vddp[i])->device;
    }

    /* Free domain descriptor table and all of its vdDescT structures.*/
    free_vdds(tmpDmnTbl);
    ms_free(tmpDmnTbl);
    return EOK;
}


/*
 * bs_bfset_get_dev
 *
 * Overview:
 *
 * Currently, each UNIX filesystem's id consists of two 32-bit numbers; the
 * first is typically the dev_t of the disk partition that the FS resides
 * on and the second is typically the filesystem type obtained from mount.h.
 * This works fine when there is a direct relationship between a filesystem
 * and a single device (partition).  For MegaSafe this is not the case
 * because the relationship is between disk devices and domains not
 * file sets; each mounted file set cannot use the same device since
 * the filesystem ids for these sets would not be unique.
 *
 * To get around this problem we fabricate a unique dev_t for file sets.
 * Each set's dev_t must be unique for all sets on a system and they
 * must no collide with real dev_t's for real devices so that we don't
 * create some weird conflicts.  Note that we don't try to make sure that
 * a dev_t is not reused when a domain is deleted.  This has the same
 * behaviour as for UFS when someone creates a new filesystem on a disk
 * that had a filesystem on it.
 *
 * So, how do we fabricate a dev_t for each set?  First, there are 32 bits
 * in a dev_t.  The high 12 bits are the major number and the low 20 bits
 * are the minor number.  Major and minor numbers have meanings when used
 * for real devices.  For our purposes we just want to make sure that we
 * generate major numbers that don't conflict with real major numbers of real
 * devices (these are defined in conf.c and are of a relatively low magnitude).
 *
 * MegaSafe fabricates a major number for each domain (the major number
 * can be reused if a domain is deleted).  This is done in get_domain_major()
 * so you can read the comments in that routine to see how the major
 * number is fabricated.
 *
 * Each set's dev_t gets its major number from the set's domain major
 * number.  The minor number is obtained from the tag of the set's
 * tag directory; we use the low 16 bits of the tag.num, shift them
 * left 4 bits and OR that with the low 4 bits of the tag.seq.  We use
 * the 4 bits from the sequence number so that if a set is deleted and
 * new one is created, there will not be conflict between the two if the
 * new set gets the same tag (the sequence number would have been incremented
 * by one).  This prevents problems if the tag is reused less than 16 times.
 * This is mainly to prevent NFS clients from accidentally accessing
 * the new set when they think they are accessing the old set.  This
 * can happen since a filesystem's (set's in our case) dev_t is part
 * of the filesystem id that NFS uses to identify filesystems (it is
 * part of the NFS filesystem handle).
 *
 * To summarize:  Each set's dev_t obtains its major number from the
 * set's domain and it obtains its minor number from the tag.num and tag.seq
 * of the set's tag directory.
 *
 * Now, to maintain some sanity for binary images that only look at
 * the low 16 bits of dev_t (the old format), spread the bits around
 * so that the low part turns over in a reasonable way for up to 256
 * domains and/or filesets.  The following shows in hex the dev_t format:
 *
 *              0xDTTSDDTT  D = domain major number (high bit is always set)
 *                          T = set's tag number
 *                          S = set's tag sequence number
 *
 *
 */

dev_t
bs_bfset_get_dev(
                 bfSetT *bfSetp      /* in = bitfile set */
                 )
{
    if (bfSetp->dev == 0) {
        minor_t bfSetMinor = 0;
        major_t bfSetMajor = 0;
        unsigned int tagbits = bfSetp->dirTag.num & 0xffff;
        unsigned int majorbits = bfSetp->dmnP->majorNum & 0xfff;

        bfSetMinor = (tagbits & 0xff) |
                     ((majorbits & 0xff) << 8) |
                     (bfSetp->dirTag.seq & 0xf) << 16;
        bfSetMajor = (tagbits >> 8) |
                      (majorbits & 0xf00);

        bfSetp->dev = makedev( bfSetMajor, bfSetMinor );
    }
    return bfSetp->dev;
}

/*
 * get_domain_major
 *
 * Fabricates a unique major number for te specified domain.
 *
 * Overview:
 *
 * Given that every domain has a directory entry in /etc/msfs we
 * use the position of the domain's directory entry as the major number.
 *
 * Here is how:
 *
 * Each directory consists of some number of 512-byte blocks.  Each
 * block consists of some number of variable length directory entries.
 * Each directory entry in /etc/msfs contains the name of a domain.
 *
 * Each directory entry has a minimum size (MIN_ENT_SZ) so each dir page can be
 * divided into 512 / MIN_ENT_SZ slots.  Also, even though dir entries
 * are of variable length which typically exceeds the MIN_ENT_SZ, no two
 * entries can start within a single slot (because each slot is MIN_ENT_SZ
 * in size).  Also, entries don't move around in a directory even when
 * other entries are deleted/reused.
 *
 * So, the major number is obtained from the slot number of the domain's
 * directory entry.  This is calculated by taking the entry's byte offset into
 * a dir page and dividing it by MIN_ENT_SZ and adding it to the number of
 * dir slots in the previous dir pages (if the entry is not on the first
 * page).
 *
 * After the major number is computed we also set the high bit (bit 11).
 * This makes the effective range for major numbers 2048 - 4095.  We do
 * this to keep MegaSafe domain major numbers well out of range of any
 * real device major numbers.
 *
 * To summarize:
 *
 *     MIN_ENT_SZ <- minimum directory entry size (struct dirent header + 4)
 *     slots      <- dir entry slots per dir page (512 / MIN_ENT_SZ)
 *     ent_pg     <- dir page containing domain's dir entry
 *     ent_offet  <- dir entry's byte offset into current dir page
 *
 *     major      <- (ent_pg - 1 * slots) + (ent_offset / MIN_ENT_SZ)
 *
 * Also see comments for bs_bfset_get_dev().
 */

#define MIN_ENT_SZ (sizeof( struct dirent ) - 252 )

static int
get_domain_major(
                 char *domain,       /* in - domain name */
                 uint32T *dmnMajor   /* out - domain device major number */
                 )
{
    int eofflag = 0, ent, error, i, dirOpen = FALSE, done = FALSE;
    int dirEntOffset, skippedEnts = 0;
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
    struct uio auio;
    struct iovec aiov;
    char *dirBuf = NULL;
    struct dirent *dp = NULL;
    int dirbuflen = 0;
    struct kdirent *kdp = NULL;

    dirBuf = ms_malloc( BS_BLKSIZE );
    if (dirBuf == NULL) {
        error = ENOMEM;
        goto _error;
    }

    /* get the vnode */

    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = MSFS_DMN_DIR;

    if( error = namei( ndp ) ) {
        goto _error;
    }

    vp = ndp->ni_vp;

    /* do an access check on the dir */

    VOP_ACCESS( vp, VREAD, ndp->ni_cred, error );
    if( error ) {
        goto _error;
    }

    /* open the vnode */

    VOP_OPEN( &vp, FREAD | OTYP_MNT, ndp->ni_cred, error );

    if( error ) {
        goto _error;
    }

    dirOpen = TRUE;

    if (vp->v_type != VDIR) {
        error = EINVAL;
        goto _error;
    }

    auio.uio_iov    = &aiov;
    auio.uio_iovcnt = 1;
    auio.uio_rw     = UIO_READ;
    auio.uio_segflg = UIO_SYSSPACE;
    auio.uio_offset = 0;

    /*
     * Scan /etc/msfs for the domain name.
     */

    while (!eofflag) {
        aiov.iov_base  = dirBuf;
        aiov.iov_len   = BS_BLKSIZE;
        auio.uio_resid = BS_BLKSIZE;

        VOP_READDIR( vp, &auio, ndp->ni_cred, &eofflag, error );
        if (error) {
            goto _error;
        }

        dirEntOffset = 0;

        dirbuflen = BS_BLKSIZE - aiov.iov_len;

        if (vp->v_mount->m_flag & M_NEWRDDIR) {
            /*
             * Scan the current dir page for the domain name.
             */

            while (!done && dirEntOffset < dirbuflen) {
                /*
                 * Get a poiner to the next dir entry.
                 */
                kdp = (struct kdirent *) &dirBuf[ dirEntOffset ];

                ent = skippedEnts + (dirEntOffset / MIN_ENT_SZ);
                dirEntOffset += kdp->kd_reclen;

                if (kdp->kd_ino == 0) {
                    /* skip deleted entries */
                    continue;
                }

                if ((strcmp( kdp->kd_name, ".") == 0) ||
                    (strcmp( kdp->kd_name, "..") == 0)) {
                    /* skip . and .. */
                    continue;
                }

                if (strcmp( kdp->kd_name, domain ) == 0) {
                    /*
                     * Found it!
                     */
                    *dmnMajor = ent | 0x00000800;
                    done = eofflag = TRUE; /* done -- terminate loops */
                }
            }
        } else {
            /*
             * Scan the current dir page for the domain name.
             */

            while (!done && dirEntOffset < dirbuflen) {
                /*
                 * Get a poiner to the next dir entry.
                 */
                dp = (struct dirent *) &dirBuf[ dirEntOffset ];

                ent = skippedEnts + (dirEntOffset / MIN_ENT_SZ);
                dirEntOffset += dp->d_reclen;

                if (dp->d_ino == 0) {
                    /* skip deleted entries */
                    continue;
                }

                if ((strcmp( dp->d_name, ".") == 0) ||
                    (strcmp( dp->d_name, "..") == 0)) {
                    /* skip . and .. */
                    continue;
                }

                if (strcmp( dp->d_name, domain ) == 0) {
                    /*
                     * Found it!
                     */
                    *dmnMajor = ent | 0x00000800;
                    done = eofflag = TRUE; /* done -- terminate loops */
                }
            }
        }

        skippedEnts += BS_BLKSIZE / MIN_ENT_SZ;
        skippedEnts += dirbuflen / MIN_ENT_SZ;
    }

    VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);
    vrele( vp );
    ms_free( dirBuf );

    if (done) {
        return EOK;
    } else {
        return ENOENT;
    }

_error:
    if (dirOpen) {
        VOP_CLOSE( vp, FREAD | OTYP_MNT, ndp->ni_cred, i);
    }

    if (vp != NULL) {
        vrele( vp );
    }

    if (dirBuf != NULL) {
        ms_free( dirBuf );
    }

    return error;
}

/*
 * free_vdds - deallocate the vd descriptor structs from the dmn
 * table.
 */
void
free_vdds(
          bfDmnDescT* dmnDescp  /* in - ptr to bfdmn desc tbl */
          )
{
    int i;

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
struct vd * 
vd_htop_if_valid( vdIndexT vdi,                /* vdIndex to check */
                  struct domain *dmnP,         /* domain */
                  int      bump_refcnt,        /* if !FALSE, bump vdRefCnt */
                  int      zombie_ok )         /* if TRUE, BSR_VD_ZOMBIE is OK*/
{
    mutex_lock( &dmnP->vdpTblLock );
    if ( VDI_IS_VALID(vdi, dmnP) ) {

        vdT *vdp = dmnP->vdpTbl[vdi - 1];
        MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );
        mutex_lock( &vdp->vdStateLock );
        mutex_unlock( &dmnP->vdpTblLock );

        /* vdp is considered valid if and only if one of the following is true:
         * 1. the state is BSR_VD_MOUNTED
         * 2. the state is (BSR_VD_VIRGIN, BSR_VD_ZOMBIE or BSR_VD_DMNT_INPROG)
         *    AND the caller is the thread setting up or removing the vdT.
         * 3. the state is BSR_VD_ZOMBIE,BSR_VD_DMNT_INPROG or 
         *    BSR_VD_VIRGIN AND zombie_ok is TRUE
         */
        if ( (vdp->vdState == BSR_VD_MOUNTED) ||
             ((vdp->vdSetupThd == current_thread() || zombie_ok) &&
              (vdp->vdState == BSR_VD_VIRGIN || 
               vdp->vdState == BSR_VD_ZOMBIE ||
               vdp->vdState == BSR_VD_DMNT_INPROG)))
        {
            /* If requested, bump the refcnt so that this vdT can't be
             * deallocated until all threads stop access to it.
             */
            if (bump_refcnt)
                vdp->vdRefCnt++;
            mutex_unlock( &vdp->vdStateLock );
            return vdp; 
        } else {
            /* This is not a valid vdp, so return NULL.  */
            mutex_unlock( &vdp->vdStateLock );
            return (vdT *)(NULL);
        }
    } else {
        mutex_unlock( &dmnP->vdpTblLock );
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

    /* We reduce lock contention by not seizing the dmnP->vdpTblLock in this
     * routine.  This is done since we call this routine only when we know
     * that this vdp cannot be removed.  
     */
    MS_SMP_ASSERT( VDI_IS_VALID(vdi, dmnP) ); 
    vdp = dmnP->vdpTbl[vdi - 1];
    MS_SMP_ASSERT( vdp );
    MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );

    if (bump_refcnt) {
        mutex_lock( &vdp->vdStateLock );
        vdp->vdRefCnt++;
        mutex_unlock( &vdp->vdStateLock );
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

    mutex_lock( &vdp->vdStateLock );
    MS_SMP_ASSERT( vdp->vdRefCnt > 0 );
    vdp->vdRefCnt--;

    if ( vdp->vdRefCnt == 0 && vdp->vdRefWaiters ) {
        MS_SMP_ASSERT( vdp->vdRefWaiters == 1 );   /* only 1 thread can wait */
        vdp->vdRefWaiters = 0;
        thread_wakeup( (vm_offset_t)&vdp->vdRefWaiters );
    }
    mutex_unlock( &vdp->vdStateLock );

    return;
}


#define MEGA_SEC (5 * hz)
/*
 * mega_touch_presto
 */

mega_touch_presto( struct vnode *vp )
{
    struct presto_status prs;
    int result;
    long res2;
    VOP_IOCTL( vp, PRGETSTATUS, &prs, 0, NOCRED, result, &res2 );
    if( result == 0 ) {
        timeout( (void(*))mega_touch_presto, vp, MEGA_SEC );
    } else {
        aprintf("mega_touch_presto: exiting\n");
    }
}

/*
 * mega_presto_start
 *
 * If presto is on the system, get its status every
 * n seconds to prevent megasafe from getting wedged.
 * Follows the hallowed software coding practice of
 * answering every hack with an even bigger hack.
 */

void
mega_presto_start( char *pr_name, int doingRoot )
{
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
    int error;

    if (doingRoot) {
        /*
         * Ok, ok, its ugly. But if we are mounting an ADVfs
         * root, we can't call namei() yet.... Wait until
         * the update mount.
         */
        return;
    }

    /* get the vnode */

    ndp->ni_nameiop = LOOKUP | NOFOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = pr_name;

    if( error = namei( ndp ) ) {
        return;
    }

    vp = ndp->ni_vp;

    /* do an access check on the vnode */

    VOP_ACCESS( vp, VREAD, ndp->ni_cred, error );
    if( error ) {
        goto err_vrele;
    }

    /* open the vnode */

    VOP_OPEN( &vp, FREAD | OTYP_MNT, ndp->ni_cred, error );

    if( error ) {
        goto err_vrele;
    }

    timeout( (void(*))mega_touch_presto, vp, MEGA_SEC );

    return;

err_vrele:
    vrele(vp);
}
