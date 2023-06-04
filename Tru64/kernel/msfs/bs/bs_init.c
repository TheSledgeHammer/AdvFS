/*
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
 *      AdvFS
 *
 * Abstract:
 *
 *      Domain and disk initialization routines.
 *
 * Date:
 *
 *      Thu Feb  6 15:18:51 1992
 *
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: bs_init.c,v $ $Revision: 1.1.107.4 $ (DEC) $Date: 2006/04/12 16:59:34 $"

#include <sys/file.h>
#include <sys/time.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_delete.h>
#ifndef KERNEL
#include <stdio.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <strings.h>
extern int errno;
extern char *sys_errlist[];
#else /* KERNEL */
#include <sys/user.h>
#include <sys/kernel.h>
#include <sys/mount.h>
#include <sys/specdev.h>
#include <sys/buf.h>
#include <sys/ucred.h>
#include <sys/vnode.h>
#include <sys/mode.h>
#include <sys/lock_probe.h>
#include <sys/versw.h>
#include <sys/clu.h>
#include <sys/ioctl.h>
#include <sys/vnode.h>
#include <sys/disklabel.h>
#include <msfs/bs_vd.h>
#include <msfs/bs_extents.h>
#include <msfs/advfs_evm.h>

#include <sys/open.h>

#endif /* KERNEL */

#define ADVFS_MODULE BS_INIT

static void tagdir_init_pg0( bsTDirPgT *tdpgp );
static int del_init_mcell_list( bsMPgT *bmtp );
static int bmt_init_mcell_free_list( bsMPgT* bmtpgp );
static statusT vd_extend_add_sbm_pgs( vdT *, bsPageT, lbnT,
                                      lbnT, bsVdAttrT *, ftxHT);

#define INIT_RBMT_PGS 1
#define INIT_BMT_PGS 1
#define INIT_BMT_PGS_V3 2
#define INIT_TAGDIR_PGS 1

uint32T metaPhysLocPct = 40;    /* default physical loc for SBM, BMT, LOG */

static statusT
init_sbm(
#ifdef KERNEL
    struct vnode *vp,           /* in */
#else
    int fd,                     /* in */
#endif
    struct bsMPg *bmtpg,        /* in */
    struct bsVdAttr *vdattr,    /* in */
    uint32T blksPerPage,        /* in */
    uint32T rbmtBlks,           /* in */
    uint32T bmtBlks,            /* in */
    uint32T preallocBlks,       /* in */
    uint32T bootBlks,           /* in */
    uint32T tagBlks,            /* in */
    uint32T sbmFirstBlk,        /* in */
    uint32T *freeBlks           /* out */
    );

/* bs_dmn_init
 *
 * This service initializes all of the virtual disks for bitfile
 * domain as specified in a bitfile domain table file.
 *
 * Returns status from bs_disk_init, or EOK.
 *
 * Note that that all the functions that can add/remove a virtual disk
 * to a domain synchronize using the dmnTblLock semaphore.  Such a
 * high level lock is needed because (for one example) two threads could
 * be racing to add the same virtual disk to different domains.  The
 * net effect should be that the disk is added to one domain while
 * something like E_VIRTUAL_DISK_BUSY should be returned for the other
 * domain.
 */

statusT
bs_dmn_init(
    char *domain,               /* in - domain name */
    int maxVds,                 /* in - maximum number of virtual disks */
    uint32T logPgs,             /* in - number of pages in log */
    serviceClassT logSvc,       /* in - log service attributes */
    serviceClassT tagSvc,       /* in - tag directory service attributes */
    char *vdName,               /* in - block special device name */
    serviceClassT vdSvc,        /* in - service class */
    uint32T  vdSize,            /* in - size of the virtual disk */
    uint32T bmtXtntPgs,         /* in - number of pages per BMT extent */
    uint32T bmtPreallocPgs,     /* in - number of pages to be preallocated for the BMT */
    uint32T domainVersion,      /* in - on-disk version for domain */
    bfDomainIdT *domainId       /* out - unique domain id */
    )
{
    int s = 0;
    struct timeval ltime;
    struct timezone tz;
    statusT sts = EOK;
    bfDmnParamsT *bfdmnparamsp = NULL;
    bsVdParamsT *vdparamsp = NULL;
    void bs_kernel_init();

    bfdmnparamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if ( bfdmnparamsp == NULL ) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    vdparamsp = (bsVdParamsT *) ms_malloc( sizeof( bsVdParamsT ));
    if ( vdparamsp == NULL ) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }
    bs_kernel_init(0);
    TIME_READ(ltime);

    /*
     * For cluster, the domainID is unique in the cluster.  The lower
     * 8 bits of the member ID are combined in the time stamp to form the
     * domainId and verified by CFS for uniqueness.
     */
    if (clu_is_ready()) {
        while (TRUE) {
            ltime.tv_usec &= 0x00ffffff;
            ltime.tv_usec |= (memberid << 24);
            if (CC_FIND_CLUSTER_DOMAIN_ID(&ltime) == 0) {
                break;
            }
            TIME_READ(ltime);
        }
    }

    if (SC_EQL( vdSvc, nilServiceClass )) {
        /* TEMP - need real "default service class" from storage mgt? */
        vdSvc = defServiceClass;
    }

    BS_BFTAG_RSVD_INIT( bfdmnparamsp->bfSetDirTag, 1, BFM_BFSDIR );
    BS_BFTAG_RSVD_INIT( bfdmnparamsp->ftxLogTag, 1, BFM_FTXLOG );
    bfdmnparamsp->bfDomainId = ltime;
    bfdmnparamsp->maxVds     = maxVds;
    bfdmnparamsp->curNumVds  = 1;
    bfdmnparamsp->ftxLogPgs  = lgr_calc_num_pgs( logPgs );

    if (strlen( domain ) > (BS_DOMAIN_NAME_SZ - 1)) {
        RAISE_EXCEPTION( EINVAL );
    }

    strncpy( bfdmnparamsp->domainName, domain, BS_DOMAIN_NAME_SZ - 1 );

    strncpy( vdparamsp->vdName, vdName, BS_VD_NAME_SZ - 1 );
    vdparamsp->serviceClass = vdSvc;
    vdparamsp->vdSize       = vdSize;
    vdparamsp->bmtXtntPgs   = bmtXtntPgs;
    vdparamsp->vdIndex      = 1;

    if (BFD_ODS_LAST_VERSION > 4)
        ADVFS_SAD0("bs_dmn_init: what version should be used for this release?");

    /*
     * If the caller didn't specify what version domain he wanted,
     * use the BFD_ODS_LAST_VERSION default.  Otherwise, if he did specify,
     * and the version requested is not supported by the running version,
     * return an error.
     */
    if (domainVersion == 0) {
        domainVersion = BFD_ODS_LAST_VERSION;
    }
    else if ((domainVersion < FIRST_VALID_FS_VERSION) ||
             (domainVersion > LAST_VALID_FS_VERSION)) {
        RAISE_EXCEPTION(E_INVALID_FS_VERSION);
    }

    sts = bs_disk_init( vdName,
                        bfdmnparamsp,
                        vdparamsp,
                        tagSvc,
                        logSvc,
                        bmtPreallocPgs,
                        domainVersion );
    if ( sts != EOK ) {
        goto HANDLE_EXCEPTION;
    }

    *domainId = bfdmnparamsp->bfDomainId;

    ms_free( vdparamsp );
    ms_free( bfdmnparamsp );

    return EOK;

HANDLE_EXCEPTION:

    if ( vdparamsp != NULL )
        ms_free( vdparamsp );
    if ( bfdmnparamsp != NULL )
        ms_free( bfdmnparamsp );

    return sts;
}

/*
 *
 * bs_disk_init
 *
 * Initialize the specified disk partition as a ADVFS Virtual Disk
 *
 * Returns EBAD_VDI, EBAD_PARAMS (bad input parameter)
 * Returns EOK
 */


statusT
bs_disk_init(
           char *diskName,              /* in - disk device name */
           bfDmnParamsT* bfDmnParams,   /* in - domain parameters */
           bsVdParamsT *bsVdParams,     /* in/out - vd parameters */
           serviceClassT tagSvc,        /* in - tag service class */
           serviceClassT logSvc,        /* in - log service class */
           uint32T bmtPreallocPgs,      /* in - number of pages to be preallocated for the BMT */
           int dmnVersion               /* in - version to store on-disk */
           )
{
    struct bsMPg *rbmtpg = NULL;
    struct bsMPg *bmtpg = NULL;
    statusT sts;
    int fd, err, i, s, devOpen = FALSE;
    struct bsXtntR* xtntp;
    struct bsXtraXtntR* xtntp1;
    bsBfAttrT* atrp;
    struct bsVdAttr* vdattr;
    bfTagT ftxLogTag, bfSetDirTag;
    bsDmnAttrT *dmnAttrp;
    uint32T *superBlockPg = NULL, minBlks = 0, freeBlks = 0;
    uint32T rbmtBlks, bmtBlks, preallocBlks, tagBlks, bootBlks;
    uint32T mscFirstBlk, rbmtFirstBlk, bmtFirstBlk, tagFirstBlk, sbmFirstBlk;
    uint32T bootFirstBlk, bmtFirstPreallocBlk; 
    uint32T sbmPgs, sbmBlks, vdBlkCnt;
    struct bsMR *rp;

#ifdef KERNEL
    int error;
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;
#endif

    MS_SMP_ASSERT(bsVdParams->vdIndex > 0);

    if (dmnVersion < FIRST_RBMT_VERSION) {
        sts = bs_disk_init_v3(diskName,
                              bfDmnParams,
                              bsVdParams,
                              tagSvc,
                              logSvc,
                              bmtPreallocPgs,
                              dmnVersion
                              );
        return sts;
    }

    /* "Create" the virtual disk */

#ifdef KERNEL
    /* get device's vnode */

    if( error = getvp( &vp, diskName, ndp, UIO_SYSSPACE ) ) {
        RAISE_EXCEPTION( (statusT)error );
    }

    VOP_ACCESS( vp, VREAD | VWRITE, ndp->ni_cred, error );
    if (error) {
        RAISE_EXCEPTION( (statusT)error );
    }

    /* open the vnode
     * In SVR4, driver open and close routines expect to get the OTYP_MNT flag
     * if the open/close was done as a result of a mount/unmount. While OSF/1
     * can pass a flag parameter to device open/close routines, it is only
     * supported in spec_open() and spec_close(). These are the functions
     * invoked via VOP_OPEN and VOP_CLOSE for device special files.  Therefore,
     * we need to inform spec_open()/spec_close that we are doing a
     * mount/unmount.
     */

    VOP_OPEN( &vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    if( error ) {
        RAISE_EXCEPTION( (statusT)error );
    }
    devOpen = TRUE;

    if( vp->v_type != VBLK && vp->v_type != VCHR ) {
        RAISE_EXCEPTION( E_BAD_DEV );
    }
#else /* KERNEL */

#ifndef _XOPEN_SOURCE
    fd = open (diskName, O_RDWR | O_CREAT | O_TRUNC | O_FSYNC, /* O_EXCL|*/
                S_IRWXU | S_IRWXG | S_IRWXO);
#else
    fd = open (diskName, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, /* O_EXCL|*/
                S_IRWXU | S_IRWXG | S_IRWXO);
#endif
    if (fd <=0) {
        ms_uprintf( "bs_disk_init: can't create virtual disk file; [%d] %s",
                   errno, sys_errlist[errno] );
        RAISE_EXCEPTION( (statusT)E_BAD_DEV );
    }
    devOpen = TRUE;
#endif /* KERNEL */

   /*
    *    For AdvFS on-disk version = 4 (and probably greater), this
    *    routine is being modified to have a new data structure to
    *    have all of the reserved file metadata. This new structure
    *    will be referred to as the RBMT. This is being done primarily
    *    to address the BMT extent exhaustion problem - where on a
    *    heavily fragmented disk, the system runs out of mcells to
    *    represent the BMT. This is because the old data structure had
    *    a hard limit of only one BMT page (i.e., page 0) to represent
    *    all the reserved files. RBMT is a dynamically extensible
    *    structure. The last mcell on every RBMT page will be reserved
    *    to describe the next RBMT page. RBMT will be extended only one
    *    page at a time.
    *
    *    This routine will initialize RBMT page 0 (also page 0 on the
    *    virtual disk). This page will be similar to the old BMT page0,
    *    with a few differences. The first few key mcells will be
    *    pre-initialized as follows:
    *
    *    cell 0 (RBMT)
    *                         == First RBMT cell - contains the bfattr
    *                            and primary extend record for RBMT.
    *    cell 1 (SBM)
    *                         == First storage bitmap mcell (contains
    *                            bitfile attributes and extent map)
    *    cell 2 (BFSDIR)
    *                         == First root tag file mcell.
    *                            Contains the bf attributes and
    *                            primary extent record on the tagdir
    *                            virtual disk.  Invalid mcell on
    *                            other virtual disks.
    *    cell 3 (FTXLOG)
    *                         == First ftx log mcell - only on ftxlog
    *                            virtual disk; not used on other vd's.
    *                            Contains bitfile attributes and
    *                            primary extent map for ftxlog.
    *    cell 4 (BMT)
    *                         == First BMT mcell - contains just the
    *                            bf attr record and primary extent record.
    *    cell 5 (MISC)        == Misc bitfile - contains things like:
    *                            Unused blocks 0 to 15 so we don't overwrite
    *                            the disk label and boot blocks.
    *                            A fake super block (blks 16 to 19).
    *                            Unused (zero filled) blks (20 to 31).
    *                            Real boot blocks blks (64 to 95)
    *    cell 6 (RBMT_EXT)
    *                         == RBMT extension mcell - contains
    *                            the vd attributes, domain attributes.
    *                            This cell is linked to cell 0 (zero).
    *
    *    cell 7 thru cell (BSPG_CELLS - 2) RBMT page 0
    *                         == Reserved mcells for reserved bitfile
    *                            extent maps.
    *                            If bmtPreallocPgs is non-zero, mcell 7
    *                            will be used to set up a new extent for
    *                            the BMT.
    *    cell (BSPG_CELLS - 1)
    *                         == Last mcell on page 0 - reserved for
    *                            future RBMT extents.
    *
    *    Mcell 0 on page 0 of the BMT is pre-initialized as the free
    *    mcell list head for non-reserved bitfiles.
    *
    *    Storage allocation:
    *
    * During disk initialization we initialize some of the pages for
    * the above reserved bitfiles.  The following summarizes the storage
    * and pages initially allocated to the reserved bitfiles:
    *
    * - MISC bitfile occupies blks 0 - 31 (pages 0 and 1).
    *   and blks 64 - 95 (pages 2 and 3) for boot blocks
    * - RBMT bitfile occupies blks 32 - 47 (page 0).
    * - BMT bitfile occupies blocks 48 -63.  Any additional BMT blocks are 
    *     allocated near the middle of the volume.
    * - LOG bitfile occupies no blks (they are allocated and initialized
    *     the first time the log is opened; see lgr_open()).  These blocks
    *     will be near the middle of the volume.
    * - BFSDIR bitfile occupies blks 96 - 111 (page 0) if the disk being
    *     initialized is the BFSDIR's disk.  Otherwise the BFSDIR occupies
    *     no space (it's pages are allocated when needed in tagdir_alloc_tag().
    * - SBM bitfile occupies blocks near the middle of the volume (the number 
    *     of blks used is determined by the size of the disk; see init_sbm().
    * - BMT prealloc blocks are allocated after the SBM bitfile (the number
    *   of blocks is specified by the input parameter bmtPreallocPgs).
    */

    BS_BFTAG_RSVD_INIT( ftxLogTag, bsVdParams->vdIndex, BFM_FTXLOG );
    BS_BFTAG_RSVD_INIT( bfSetDirTag, bsVdParams->vdIndex, BFM_BFSDIR );

    rbmtBlks = roundup( ADVFS_PGSZ_IN_BLKS * INIT_RBMT_PGS, BS_CLUSTSIZE);
    bmtBlks = roundup( ADVFS_PGSZ_IN_BLKS * INIT_BMT_PGS, BS_CLUSTSIZE);

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        /* this is the tagdir's disk so allocate and initialize page 0 */
        tagBlks = roundup( ADVFS_PGSZ_IN_BLKS * INIT_TAGDIR_PGS, BS_CLUSTSIZE);
    } else {
        /* this is not the tagdir's disk so don't allocate any pages to it */
        tagBlks = 0;
    }

    bootBlks = 32;

    rbmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );
    if (rbmtpg == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

                                                /* start and end blocks */
    mscFirstBlk = 0;                            /*  0 - 31 */
    rbmtFirstBlk = MSFS_RESERVED_BLKS;          /* 32 - 47 */
    bmtFirstBlk = rbmtFirstBlk + rbmtBlks;      /* 48 - 63 */
    bootFirstBlk = MSFS_RESERVED_BLKS + 32;     /* 64 - 95 */
    tagFirstBlk = bootFirstBlk + bootBlks;      /* 96 - 111 (optionally) */

    if ((vdBlkCnt = bsVdParams->vdSize) == 0) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    metaPhysLocPct = MIN(metaPhysLocPct, 99);
    sbmFirstBlk = roundup((vdBlkCnt / 100) * metaPhysLocPct, BS_CLUSTSIZE);
    sbmFirstBlk = MAX(sbmFirstBlk, tagFirstBlk + tagBlks);
    sbmPgs = howmany( vdBlkCnt / BS_CLUSTSIZE, SBM_BITS_PG);
    sbmBlks = roundup( ADVFS_PGSZ_IN_BLKS * sbmPgs, BS_CLUSTSIZE );

    bmt_init_page( rbmtpg, 0, BFM_RBMT, dmnVersion );

    /********************************************************************
     * RBMT
     ********************************************************************/

    /*
     * Initialize reserved bitfile metadata. Cell 0 is for bitfile 0
     * bfattr and primary extent record.
     */

    rbmtpg->bsMCA[ BFM_RBMT ].linkSegment = 0;
    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_RBMT ].tag,
                        bsVdParams->vdIndex,
                        BFM_RBMT );
    rbmtpg->bsMCA[ BFM_RBMT ].bfSetTag = staticRootTagDirTag;

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_RBMT ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;

    /*
     * Init an xtnt record for the RBMT to map page 0. Page 0 is
     * allocated right after the MISC bitfile's pages.
     */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &rbmtpg->bsMCA[ BFM_RBMT ] );

    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;
    xtntp->firstXtnt.xCnt           = 2;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = rbmtFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = INIT_RBMT_PGS;
    xtntp->firstXtnt.bsXA[1].vdBlk  = -1;
    xtntp->firstXtnt.mcellCnt       = 1;

    /********************************************************************
     * RBMT EXTENSION
     ********************************************************************/

    /*
     * Link an extension cell into this right away for the vd attr and
     * domain records. This extension cell will immediately follow the
     * reserved primary mcells.
     */

    rbmtpg->bsMCA[ BFM_RBMT ].nextVdIndex = bsVdParams->vdIndex;
    rbmtpg->bsMCA[ BFM_RBMT ].nextMCId.cell = BFM_RBMT_EXT;
    rbmtpg->bsMCA[BFM_RBMT_EXT].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[BFM_RBMT_EXT].nextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_RBMT_EXT].linkSegment = 1;
    rbmtpg->bsMCA[BFM_RBMT_EXT].tag = rbmtpg->bsMCA[ BFM_RBMT ].tag;
    rbmtpg->bsMCA[BFM_RBMT_EXT].bfSetTag = rbmtpg->bsMCA[ BFM_RBMT ].bfSetTag;

    /* Set the vd attr record for this disk */

    vdattr = bmtr_assign(BSR_VD_ATTR,
                        sizeof(struct bsVdAttr),
                        &rbmtpg->bsMCA[BFM_RBMT_EXT]);
    if (vdattr == 0) {
        ms_uprintf( "bs_disk_init: can't create vd attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    vdattr->state = BSR_VD_VIRGIN;
    vdattr->maxPgSz = ADVFS_PGSZ_IN_BLKS;

    /* copy fields from the supplied vd params */

    if ( !( vdattr->vdIndex = bsVdParams->vdIndex )) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    vdattr->serviceClass = bsVdParams->serviceClass;
    if (SC_EQL( vdattr->serviceClass, nilServiceClass )) {
        /* TEMP - need real "default service class" from storage mgt? */
       vdattr->serviceClass = defServiceClass;
    }

    vdattr->vdBlkCnt = vdBlkCnt;
    vdattr->stgCluster = BS_CLUSTSIZE;

    if ((vdattr->bmtXtntPgs = bsVdParams->bmtXtntPgs) < BS_BMT_XPND_PGS) {
        vdattr->bmtXtntPgs = BS_BMT_XPND_PGS;
    }

    vdattr->jays_new_field = 0;

    /*
     * Set the domain attributes from the domain parameters.
     * The domain attributes record is replicated in each RBMT
     * extension mcell.  Some fields will be set now, others
     * will be set by set_dmn_params.
     */

    dmnAttrp = bmtr_assign( BSR_DMN_ATTR,
                            sizeof(bsDmnAttrT),
                            &rbmtpg->bsMCA[BFM_RBMT_EXT] );
    if (dmnAttrp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    *dmnAttrp = bsNilDmnAttr;
    dmnAttrp->bfDomainId  = bfDmnParams->bfDomainId;
    dmnAttrp->bfSetDirTag = staticRootTagDirTag;
    dmnAttrp->maxVds      = bfDmnParams->maxVds;

    /* setup domain's log attributes and transient attributes record */

    if (BS_BFTAG_EQL( bfDmnParams->ftxLogTag, ftxLogTag )) {
        /*
         * only if this is the disk that contains the log
         */
        bsDmnMAttrT      *dmnMAttrp;
        bsDmnTAttrT      *dmnTAttrp;
        bsDmnFreezeAttrT *dmnFreezeAttrp;

        dmnMAttrp = bmtr_assign( BSR_DMN_MATTR,
                                sizeof(bsDmnMAttrT),
                                &rbmtpg->bsMCA[BFM_RBMT_EXT] );
        if (dmnMAttrp == 0) {
            ms_uprintf( "bs_disk_init: can't create domain log attr record" );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        dmnMAttrp->seqNum = 1;
        dmnMAttrp->delPendingBfSet = NilBfTag;
        dmnMAttrp->ftxLogTag = bfDmnParams->ftxLogTag;
        dmnMAttrp->ftxLogPgs = bfDmnParams->ftxLogPgs;
        dmnMAttrp->bfSetDirTag = bfDmnParams->bfSetDirTag;
        dmnMAttrp->vdCnt = 1;
        dmnMAttrp->recoveryFailed = 0;
        dmnMAttrp->mode = S_IRWXU | S_IRGRP | S_IROTH;
#ifdef KERNEL
        dmnMAttrp->uid = u.u_nd.ni_cred->cr_uid;
        dmnMAttrp->gid = u.u_nd.ni_cred->cr_gid;
#else
        dmnMAttrp->uid = 0;
        dmnMAttrp->gid = 0;
#endif

        dmnTAttrp = bmtr_assign( BSR_DMN_TRANS_ATTR,
                                sizeof(bsDmnTAttrT),
                                &rbmtpg->bsMCA[BFM_RBMT_EXT] );

        if (dmnTAttrp == 0) {
            ms_uprintf( "bs_disk_init: can't create domain trans attr record" );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        dmnTAttrp->chainVdIndex = -1;
        dmnTAttrp->chainMCId = bsNilMCId;
        dmnTAttrp->op = BSR_VD_NO_OP;
        dmnTAttrp->dev = 0;

        /* 
         * Try to put a BSR_DMN_FREEZE_ATTR on the disk.
         * This record is for freezefs debugging only so we tolerate failure.
         */
        dmnFreezeAttrp = bmtr_assign( BSR_DMN_FREEZE_ATTR,
                                sizeof(bsDmnFreezeAttrT),
                                &rbmtpg->bsMCA[BFM_RBMT_EXT] );

        bzero ((char*)dmnFreezeAttrp, sizeof( bsDmnFreezeAttrT ));
    }

    /*
     * Remember the rbmt's first free mcell for reserved bitfiles.
     */

    rbmtpg->nextfreeMCId.cell = BFM_RSVD_CELLS;
    rbmtpg->freeMcellCnt = rbmtpg->freeMcellCnt - BFM_RSVD_CELLS;

    /********************************************************************
     * RBMT RESERVED CELL
     ********************************************************************/

    /*
     * Initialize the last mcell on rbmtpg for future extensions of the
     * RBMT.
     */

    rbmtpg->bsMCA[RBMT_RSVD_CELL].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].linkSegment = 2;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].tag = rbmtpg->bsMCA[ BFM_RBMT ].tag;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].bfSetTag = rbmtpg->bsMCA[ BFM_RBMT ].bfSetTag;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].nextMCId = bsNilMCId;

    /* nil record to be consistent with rbmt_extend when it adds a page */
    rp = (struct bsMR *) rbmtpg->bsMCA[RBMT_RSVD_CELL].bsMR0;
    rp->bCnt = sizeof(struct bsMR);
    rp->type = BSR_NIL;


    /********************************************************************
     * BMT
     ********************************************************************/

    /*
     * Initialize the primary mcell for the BMT. Set up bfattr record
     * and primary extent record.
     */

    rbmtpg->bsMCA[ BFM_BMT ].linkSegment = 0;
    rbmtpg->bsMCA[ BFM_BMT ].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[ BFM_BMT ].nextMCId = bsNilMCId;

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_BMT ].tag,
                        bsVdParams->vdIndex,
                        BFM_BMT );
    rbmtpg->bsMCA[ BFM_BMT ].bfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the BMT bf */
    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_BMT ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;

    /*
     * Init an xtnt record for the BMT to map page 0.
     * Page 0 is allocated right after the RBMT page 0.
     */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &rbmtpg->bsMCA[ BFM_BMT ] );

    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;
    xtntp->firstXtnt.xCnt           = 2;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = bmtFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = INIT_BMT_PGS;
    xtntp->firstXtnt.bsXA[1].vdBlk  = -1;
    xtntp->firstXtnt.mcellCnt       = 1;

    /********************************************************************
     * BMT PREALLOCATION
     ********************************************************************/

    /*
     * Set up BFM_RSVD_CELLS mcell for a BMT extent and pre-allocate the 
     * specified amount.  Always pre-allocate at least one page which
     * will be physically located metaPhysLocPct into the volume,
     */

    bmtPreallocPgs = MAX(bmtPreallocPgs, 1);

    /* Calculate the starting block for the pre-allocation range */

    bmtFirstPreallocBlk = sbmFirstBlk + sbmBlks;

    /* Initialize BFM_RSVD_CELLS mcell to belong to BMT */

    rbmtpg->bsMCA[BFM_RSVD_CELLS].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].nextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].linkSegment = 1;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].tag = rbmtpg->bsMCA[ BFM_BMT ].tag;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].bfSetTag = rbmtpg->bsMCA[ BFM_BMT ].bfSetTag;

    /* Update free list and free count */
    rbmtpg->nextfreeMCId.cell += 1;
    rbmtpg->freeMcellCnt -= 1;

    /* Assign and set up a new extent record for preallocation pages */
    /* These pages will be allocated after the SBM pages */

    xtntp1 = bmtr_assign( BSR_XTRA_XTNTS,
                          sizeof(struct bsXtraXtntR),
                          &rbmtpg->bsMCA[BFM_RSVD_CELLS] );

    if (xtntp1 == 0) {
        ms_uprintf( "bs_disk_init: can't create extent record for BMT preallocation" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp1->blksPerPage     = atrp->bfPgSz;
    xtntp1->xCnt            = 2;
    xtntp1->bsXA[0].bsPage = INIT_BMT_PGS;
    xtntp1->bsXA[0].vdBlk = bmtFirstPreallocBlk;
    xtntp1->bsXA[1].bsPage = (INIT_BMT_PGS + bmtPreallocPgs);
    xtntp1->bsXA[1].vdBlk = -1;

    /* Update the primary extent record of the BMT */

    xtntp->chainVdIndex     = bsVdParams->vdIndex;
    xtntp->chainMCId.cell   = BFM_RSVD_CELLS;
    xtntp->firstXtnt.mcellCnt++;

    preallocBlks = roundup( ADVFS_PGSZ_IN_BLKS * bmtPreallocPgs, BS_CLUSTSIZE);


    /********************************************************************
     * STG BITMAP
     ********************************************************************/

    /*
     * Iinitialize the storage bitmap primary mcell.
     * Set no next cell, assign reserved tag derived from vdindex.
     */

    rbmtpg->bsMCA[ BFM_SBM ].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[ BFM_SBM ].nextMCId = bsNilMCId;
    rbmtpg->bsMCA[ BFM_SBM ].linkSegment = 0;

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_SBM ].tag,
                        bsVdParams->vdIndex,
                        BFM_SBM );
    rbmtpg->bsMCA[ BFM_SBM ].bfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the sbm bf */

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_SBM ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create stg map attr record\n");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;

#ifdef KERNEL
    sts = init_sbm( vp,
#else
    sts = init_sbm( fd,
#endif
                    rbmtpg,
                    vdattr,
                    atrp->bfPgSz,
                    rbmtBlks,
                    bmtBlks,
                    preallocBlks,
                    bootBlks,
                    tagBlks,
                    sbmFirstBlk,
                    &freeBlks );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * minBlks is the minimum number of blocks that we need to have
     * a usable domain.
     */

    /* Find the minimum blocks needed for the BMT + one BMT expansion, */
    /* the LOG, 2 TAG files + TAG expansion and 128 pages for files. */
    minBlks = (BS_BMT_XPND_PGS + bmtPreallocPgs + bfDmnParams->ftxLogPgs +
               BS_TD_XPND_PGS + BS_TD_XPND_PGS + 128) * ADVFS_PGSZ_IN_BLKS;
    if (freeBlks < minBlks) {
        ms_uprintf( "bs_disk_init: disk is too small\n" );
        RAISE_EXCEPTION( ENO_MORE_BLKS );
    }

    /********************************************************************
     * TAG DIR
     ********************************************************************/

    /*
     * Initialize the reserved tag directory mcell on each vd.  If
     * this is the selected vd for the tag directory, init a valid mcell.
     * Set no next cell for the reserved tag directory cell.
     */

    rbmtpg->bsMCA[ BFM_BFSDIR ].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[ BFM_BFSDIR ].nextMCId = bsNilMCId;
    rbmtpg->bsMCA[ BFM_BFSDIR ].linkSegment = 0;

    /* set the synthesized tag for the tag dir reserved cell. */

    rbmtpg->bsMCA[ BFM_BFSDIR ].tag = bfSetDirTag;
    rbmtpg->bsMCA[ BFM_BFSDIR ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the tag directory. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_BFSDIR ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create tag dir attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;
    atrp->cl.reqServices = tagSvc;
    atrp->cl.optServices = nilServiceClass;

    /* init a null extent record for the tag directory. */

    xtntp = bmtr_assign(BSR_XTNTS,
                        sizeof(struct bsXtntR),
                        &rbmtpg->bsMCA[ BFM_BFSDIR ]);
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create tag dir extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        xtntp->firstXtnt.xCnt           = 2;
        xtntp->firstXtnt.bsXA[0].bsPage = 0;
        xtntp->firstXtnt.bsXA[0].vdBlk  = tagFirstBlk;
        xtntp->firstXtnt.bsXA[1].bsPage = INIT_TAGDIR_PGS;
        xtntp->firstXtnt.bsXA[1].vdBlk  = -1;
    } else {
        xtntp->firstXtnt.xCnt           = 1;
        xtntp->firstXtnt.bsXA[0].bsPage = 0;
        xtntp->firstXtnt.bsXA[0].vdBlk  = -1;
    }

    xtntp->firstXtnt.mcellCnt       = 1;

    /********************************************************************
     * LOG
     ********************************************************************/

    /*
     * Set no next cell for the reserved ftx log cell.  If this is the
     * selected vd for the ftx log, init the primary mcell for it now.
     */

    rbmtpg->bsMCA[ BFM_FTXLOG ].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[ BFM_FTXLOG ].nextMCId = bsNilMCId;

    /*
     * Initialize the ftx log primary mcell.
     */

    /* set the synthesized tag for the ftx log reserved cell. */

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_FTXLOG ].tag,
                        bsVdParams->vdIndex,
                        BFM_FTXLOG );

    rbmtpg->bsMCA[ BFM_FTXLOG ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the ftx log. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_FTXLOG ] );
    if (atrp == 0) {
       ms_uprintf( "bs_disk_init: can't create ftx log attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;

    /*
     * The LOG state must be set to BSRA_INVALID until it is opened to avoid
     * the race. The log placeholders (mcell 3 in added volumes)
     * state must be set to BSRA_VALID now since they will not be opened.
     * logSvc will be nilServiceClass when adding volumes
     */

    if ( logSvc ) {
        atrp->state = BSRA_INVALID;
    } else {
        atrp->state = BSRA_VALID;
    }
    atrp->cl.dataSafety = BFD_NO_NWR;   /* can't log the log!! */
    atrp->cl.reqServices = logSvc;
    atrp->cl.optServices = nilServiceClass;

    /* init a null extent record for the ftx log. */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &rbmtpg->bsMCA[ BFM_FTXLOG ] );

    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create ftx log extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;
    xtntp->firstXtnt.xCnt           = 1;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = -1;
    xtntp->firstXtnt.mcellCnt       = 1;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;

    /********************************************************************
     * MISC
     ********************************************************************/

    /*
     * Set no next cell for the reserved MISC BF cell.
     */

    rbmtpg->bsMCA[ BFM_MISC ].nextVdIndex = bsNilVdIndex;
    rbmtpg->bsMCA[ BFM_MISC ].nextMCId = bsNilMCId;

    /*
     * Initialize the MISC BF primary mcell.
     */

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_MISC ].tag,
                        bsVdParams->vdIndex,
                        BFM_MISC);

    rbmtpg->bsMCA[ BFM_MISC ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_MISC ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create misc bf attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;
    atrp->cl.reqServices = nilServiceClass;
    atrp->cl.optServices = nilServiceClass;

    /*
     * Since nothing follow the MISC mcell, we are adding some slop to the
     * end. The bootblock extent will take up one element. The rest
     * is for future expansion.
     */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR) + (10 * sizeof(bsXtntT)),
                         &rbmtpg->bsMCA[ BFM_MISC ] );
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create misc bf extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;

    /* Initialize to have two pages allocated at blocks 0 to 31 */
    /* Boot blocks are now allocated at blocks 64 to 95 */

    xtntp->firstXtnt.xCnt           = 3;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = mscFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = 2;
    xtntp->firstXtnt.bsXA[1].vdBlk  = bootFirstBlk;
    xtntp->firstXtnt.bsXA[2].bsPage = 4;
    xtntp->firstXtnt.bsXA[2].vdBlk  = -1;

    /* Write the fake super block.  It is zero-filled and contains magic # */

    superBlockPg = (uint32T *) ms_malloc( ADVFS_PGSZ );
    bzero( (char *)superBlockPg, ADVFS_PGSZ );
    superBlockPg[MSFS_MAGIC_OFFSET / sizeof( uint32T )] = MSFS_MAGIC;

#ifdef KERNEL
    err = write_raw_page (vp,
#else
    err = write_raw_page (fd,
#endif
                          MSFS_FAKE_SB_BLK,
                          ADVFS_PGSZ_IN_BLKS,
                          superBlockPg);
    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write fake superblock; %d\n", err );
        RAISE_EXCEPTION( err );
    }

    ms_free( superBlockPg );
    superBlockPg = NULL;

    /*
     * Write out rbmt page 0, init and write out bmt page 0, init and
     * write out bmt preallocated pages if any, and init and write out
     * tagdir page 0.
     */

#ifdef KERNEL
    err = write_raw_bmt_page (vp,
#else /* KERNEL */
    err = write_raw_bmt_page (fd,
#endif
                              MSFS_RESERVED_BLKS,
                              rbmtpg);
    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write rbmt page 0; %d\n", err);
        RAISE_EXCEPTION( err );
    }

    ms_free( rbmtpg );
    rbmtpg = NULL;

    bmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );
    if (bmtpg == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    bmt_init_page( bmtpg, 0, BFM_BMT, dmnVersion );
    if (!bmt_init_mcell_free_list( bmtpg )) {
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }
    del_init_mcell_list( bmtpg );

    if (bmtPreallocPgs) {
        bmtpg->nextFreePg = INIT_BMT_PGS;
    } else {
        bmtpg->nextFreePg = -1;
    }

#ifdef KERNEL
    err = write_raw_bmt_page( vp,
#else /* KERNEL */
    err = write_raw_bmt_page( fd,
#endif /* KERNEL */
                              bmtFirstBlk,
                              bmtpg );

    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write bmt page 0; %d\n", err);
        RAISE_EXCEPTION( err );
    }

    if (bmtPreallocPgs) {
        int pg, newpg=INIT_BMT_PGS;
        uint32T startBlk=bmtFirstPreallocBlk;

        for (pg=0; pg<bmtPreallocPgs; pg++) {
            bmt_init_page(bmtpg, newpg, BFM_BMT, dmnVersion);
            if (pg < (bmtPreallocPgs - 1)) {
                bmtpg->nextFreePg = ++newpg;
            } else {
                bmtpg->nextFreePg = -1;
            }
#ifdef KERNEL
            err = write_raw_bmt_page( vp,
#else /* KERNEL */
            err = write_raw_bmt_page( fd,
#endif /* KERNEL */
                                      startBlk,
                                      bmtpg );
            if ( err != 0) {
                ms_uprintf( "bs_disk_init: can't write raw bmt page %d, block %d, status %d\n", pg+1, startBlk, err);
                RAISE_EXCEPTION( err );
            }
            startBlk += ADVFS_PGSZ_IN_BLKS;
        }
    }

    ms_free( bmtpg );
    bmtpg = NULL;

    if (tagBlks > 0) {
        bsTDirPgT *tdpgp = (bsTDirPgT*) ms_malloc( sizeof(bsTDirPgT) );

        tagdir_init_pg0( tdpgp );

#ifdef KERNEL
        err = write_raw_page( vp,
#else /* KERNEL */
        err = write_raw_page( fd,
#endif /* KERNEL */
                              tagFirstBlk,
                              ADVFS_PGSZ_IN_BLKS,
                              tdpgp );
        if (err != 0) {
            ms_uprintf( "bs_disk_init: can't write root tagdir page 0 starting at disk block number %d. status was %d\n", tagFirstBlk, err);
            RAISE_EXCEPTION( err );
        }
        ms_free( tdpgp );
    }

#ifdef KERNEL
    VOP_CLOSE( vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, i );
    vrele(vp);
#else /* KERNEL */
    if (close(fd) != 0) perror("init CLOSE failed");
#endif

    return err;

HANDLE_EXCEPTION:

    if (devOpen) {
#ifdef KERNEL
    VOP_CLOSE( vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, i );
#else /* KERNEL */
        if (close(fd) != 0) perror("init CLOSE failed");
#endif /* KERNEL */
    }

    if (superBlockPg != NULL) {
        ms_free( superBlockPg );
    }

    if (rbmtpg != NULL) {
        ms_free( rbmtpg );
    }

    if (bmtpg != NULL) {
        ms_free( bmtpg );
    }

    return sts;

}


statusT
bs_disk_init_v3(
           char *diskName,              /* in - disk device name */
           bfDmnParamsT* bfDmnParams,   /* in - domain parameters */
           bsVdParamsT *bsVdParams,     /* in/out - vd parameters */
           serviceClassT tagSvc,        /* in - tag service class */
           serviceClassT logSvc,        /* in - log service class */
           uint32T bmtPreallocPgs,      /* in - number of pages to be preallocated for the BMT */
           int dmnVersion               /* in - domain version */
           )
{
    struct bsMPg *bmtpg = NULL;
    statusT sts;
    int fd, err, i, s, devOpen = FALSE;
    struct bsXtntR* xtntp;
    bsBfAttrT* atrp;
    struct bsVdAttr* vdattr;
    bfTagT ftxLogTag, bfSetDirTag;
    bsDmnAttrT *dmnAttrp;
    uint32T *superBlockPg = NULL, minBlks = 0, freeBlks = 0;
    uint32T rbmtBlks = 0, preallocBlks = 0;
    uint32T bmtBlks, tagBlks, bootBlks;
    uint32T mscFirstBlk, bmtFirstBlk, tagFirstBlk, sbmFirstBlk;
    uint32T bootFirstBlk, bmtFirstPreallocBlk;
    int error;
    struct vnode *vp = NULL;
    struct nameidata *ndp = &u.u_nd;

    /* ADD general parameter checks */

    /* "Create" the virtual disk */

    /* get device's vnode */

    if( error = getvp( &vp, diskName, ndp, UIO_SYSSPACE ) ) {
        RAISE_EXCEPTION( (statusT)error );
    }

    VOP_ACCESS( vp, VREAD | VWRITE, ndp->ni_cred, error );
    if (error) {
        RAISE_EXCEPTION( (statusT)error );
    }

    /* open the vnode
     * In SVR4, driver open and close routines expect to get the OTYP_MNT flag
     * if the open/close was done as a result of a mount/unmount. While OSF/1
     * can pass a flag parameter to device open/close routines, it is only
     * supported in spec_open() and spec_close(). These are the functions
     * invoked via VOP_OPEN and VOP_CLOSE for device special files. Therefore,
     * we need to inform spec_open()/spec_close that we are doing a
     * mount/unmount.
     */
    VOP_OPEN( &vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, error );
    if( error ) {
        RAISE_EXCEPTION( (statusT)error );
    }
    devOpen = TRUE;

    if( vp->v_type != VBLK && vp->v_type != VCHR ) {
        RAISE_EXCEPTION( E_BAD_DEV );
    }

   /*
    *    For AdvFS on-disk versions < 4,
    *    This routine mainly initializes the first page of the
    *    Bitfile Meta-data Table (called the BMT).
    *    This is also page 0 on the virtual disk.  First the
    *    page is initialized to consist of a linked list of free
    *    mcells (see bmt_init_page()).  Then several key mcells
    *    are pre-initialized as shown below:
    *
    *    cell 0 (BMT)
    *                         == First BMT mcell - contains just the
    *                            bf attr record and primary extent record.
    *    cell 1 (SBM)
    *                         == First storage bitmap mcell (contains
    *                            bitfile attributes and extent map)
    *    cell 2 (BFSDIR)
    *                         == First tag directory mcell.
    *                            Contains the bf attributes and
    *                            primary extent record on the tagdir
    *                            tag virtual disk.  Invalid mcell on
    *                            other virtual disks.
    *    cell 3 (FTXLOG)
    *                         == First ftx log mcell - only on ftxlog tag
    *                            virtual disk; not used on other vd's.
    *                            Contains bitfile attributes and
    *                            primary extent map for ftxlog.
    *
    *    cell 4 (BMT_EXT)
    *                         == BMT extension mcell - contains
    *                            the vd attributes, domain attributes.  This
    *                            cell is linked to cell 0 (zero).
    *
    *
    *    cell 5 (MISC)        == Misc bitfile - contains things like:
    *                            Unused blocks 0 to 15 so we don't overwrite
    *                                the disk label and boot blocks.
    *                            A fake super block (blks 16 to 19).
    *                            Unused (zero filled) blks (20 to 31).
    *                            Real boot blocks blks (64 to 95)
    *
    *    cell 6 and remaining cells in page 0
    *                         == Reserved mcells -
    *                            Reserved bitfile extent maps must be contained
    *                            on page zero.  Therefore, we reserve all
    *                            remaining mcells on page zero for these
    *                            extent maps (mcells that contain only
    *                            extent map records).  The real mcell free
    *                            list starts with the first mcell on page
    *                            one.  In other words, there are 2 "free mcell"
    *                            lists per BMT.  One exists solely on page zero
    *                            and it is used only to allocate mcells for
    *                            reserved bitfiles.  The other free list spans
    *                            the rest of the BMT's pages and is used for
    *                            non-reserved bitfiles.
    *
    *                           If bmtPreallocPgs is non-zero, we will use cell 6
    *                           to set up the first extent for the BMT.
    *
    * Storage allocation:
    *
    * During disk initialization we initialize some of the pages for
    * the above reserved bitfiles.  The following summarizes the storage
    * and pages initially allocated to the reserved bitfiles:
    *
    * - MISC bitfile occupies blks 0 - 31 (pages 0 and 1).
    *   and blks 64 - 95 (pages 2 and 3) for boot blocks
    * - BMT bitfile occupies blks 32 - 63 (pages 0 and 1).
    * - LOG bitfile occupies no blks (they are allocated and initialized
    *     the first time the log is opened; see lgr_open()).
    * - BFSDIR bitfile occupies blks 96 - 111 (page 0) if the disk being
    *     initialized is the BFSDIR's disk.  Otherwise the BFSDIR occupies
    *     no space (it's pages are allocated when needed in tagdir_alloc_tag()).
    * - SBM bitfile occupies the blocks after the BFSDIR (the number blks
    *     used is determined by the size of the disk; see init_sbm()).
    * - BMT prealloc blocks are allocated after the SBM bitfile (the number
    *   of blocks is specified by the input parameter bmtPreallocPgs).
    */

    BS_BFTAG_RSVD_INIT( ftxLogTag, bsVdParams->vdIndex, BFM_FTXLOG_V3 );
    BS_BFTAG_RSVD_INIT( bfSetDirTag, bsVdParams->vdIndex, BFM_BFSDIR_V3 );

    bmtBlks = roundup( ADVFS_PGSZ_IN_BLKS * INIT_BMT_PGS_V3, BS_CLUSTSIZE_V3);

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        /* this is the tagdir's disk so allocate and initialize page 0 */
        tagBlks = roundup(ADVFS_PGSZ_IN_BLKS*INIT_TAGDIR_PGS, BS_CLUSTSIZE_V3);
    } else {
        /* this is not the tagdir's disk so don't allocate any pages to it */
        tagBlks = 0;
    }

    bootBlks = 32;

    bmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );
    if (bmtpg == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

                                                /* start and end blocks */
    mscFirstBlk = 0;                            /*  0 - 31 */
    bmtFirstBlk = MSFS_RESERVED_BLKS;           /* 32 - 63 */
    bootFirstBlk = MSFS_RESERVED_BLKS + 32;     /* 64 - 95 */
    tagFirstBlk = bootFirstBlk + bootBlks;      /* 96 - 111 (optionally) */
    sbmFirstBlk = tagFirstBlk + tagBlks;        /* 96 - ?? or 112 - ?? */

    bmt_init_page( bmtpg, 0, BFM_BMT, dmnVersion );

    /********************************************************************
     * BMT
     ********************************************************************/

    /*
     * Initialize reserved bitfile metadata. Cell 0 is for bitfile 0
     * and contains records for vd context, vd attributes
     */

    /* assign reserved tag derived from vdindex */

    bmtpg->bsMCA[ BFM_BMT_V3 ].linkSegment = 0;

    BS_BFTAG_RSVD_INIT( bmtpg->bsMCA[ BFM_BMT_V3 ].tag,
                        bsVdParams->vdIndex,
                        BFM_BMT_V3 );
    bmtpg->bsMCA[ BFM_BMT_V3 ].bfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the BMT bf */

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &bmtpg->bsMCA[ BFM_BMT_V3 ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;

    /* Init an xtnt record for the BMT to map page 0 and 1 */
    /* Page 0 is allocated right after the MISC bitfile's pages */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &bmtpg->bsMCA[ BFM_BMT_V3 ] );
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;
    xtntp->firstXtnt.xCnt           = 2;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = bmtFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = INIT_BMT_PGS_V3;
    xtntp->firstXtnt.bsXA[1].vdBlk  = XTNT_TERM;
    xtntp->firstXtnt.mcellCnt       = 1;

    /********************************************************************
     * BMT EXTENSION
     ********************************************************************/

    /*
     * Link an extension cell into this right away for the vd attr and
     * domain records.  This lets us keep the
     * general mcell sized to hold typical extent and bf attr records.
     * This extension cell will immediately follow the reserved
     * primary mcells.  It must be on page 0 (?) so that vd and dmn
     * attribute records can be dead reckoned by the vd "mount" code.
     */

    bmtpg->bsMCA[ BFM_BMT_V3 ].nextVdIndex = bsVdParams->vdIndex;
    bmtpg->bsMCA[ BFM_BMT_V3 ].nextMCId.cell = BFM_BMT_EXT_V3;

    bmtpg->bsMCA[BFM_BMT_EXT_V3].nextVdIndex = bsNilVdIndex;
    bmtpg->bsMCA[BFM_BMT_EXT_V3].nextMCId = bsNilMCId;
    bmtpg->bsMCA[BFM_BMT_EXT_V3].linkSegment = 1;
    bmtpg->bsMCA[BFM_BMT_EXT_V3].tag = bmtpg->bsMCA[ BFM_BMT_V3 ].tag;
    bmtpg->bsMCA[BFM_BMT_EXT_V3].bfSetTag = bmtpg->bsMCA[ BFM_BMT_V3 ].bfSetTag;

    /* Set the vd attr record for this disk */

    vdattr = bmtr_assign(BSR_VD_ATTR,
                        sizeof(struct bsVdAttr),
                        &bmtpg->bsMCA[BFM_BMT_EXT_V3]);
    if (vdattr == 0) {
        ms_uprintf( "bs_disk_init: can't create vd attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    vdattr->state = BSR_VD_VIRGIN;
    vdattr->maxPgSz = ADVFS_PGSZ_IN_BLKS;

    /* copy fields from the supplied vd params */

    if ( !( vdattr->vdIndex = bsVdParams->vdIndex )) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    if ((vdattr->vdBlkCnt = bsVdParams->vdSize) == 0) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    vdattr->serviceClass = bsVdParams->serviceClass;
    if (SC_EQL( vdattr->serviceClass, nilServiceClass )) {
        /* TEMP - need real "default service class" from storage mgt? */
        vdattr->serviceClass = defServiceClass;
    }

    vdattr->stgCluster = BS_CLUSTSIZE_V3;

    if ((vdattr->bmtXtntPgs = bsVdParams->bmtXtntPgs) < BS_BMT_XPND_PGS) {
        vdattr->bmtXtntPgs = BS_BMT_XPND_PGS;
    }

    vdattr->jays_new_field = 0;

    /*
     * Remember the bmt's first free mcell for reserved bitfiles.
     */

    bmtpg->nextfreeMCId.cell = BFM_RSVD_CELLS_V3;
    bmtpg->freeMcellCnt = bmtpg->freeMcellCnt - BFM_RSVD_CELLS_V3;

    /*
     * Set the domain attributes from the domain parameters.
     * The domain attributes record is replicated in each primary BMT
     * mcell.  Some fields will be set now, others
     * will be set by set_dmn_params.
     */

    dmnAttrp = bmtr_assign( BSR_DMN_ATTR,
                            sizeof(bsDmnAttrT),
                            &bmtpg->bsMCA[BFM_BMT_EXT_V3] );
    if (dmnAttrp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    *dmnAttrp = bsNilDmnAttr;
    dmnAttrp->bfDomainId  = bfDmnParams->bfDomainId;
    dmnAttrp->bfSetDirTag = staticRootTagDirTag;
    dmnAttrp->maxVds      = bfDmnParams->maxVds;

    /* setup domain's log attributes and transient attributes record */

    if (BS_BFTAG_EQL( bfDmnParams->ftxLogTag, ftxLogTag )) {
        /*
         * only if this is the disk that contains the log
         */
        bsDmnMAttrT      *dmnMAttrp;
        bsDmnTAttrT      *dmnTAttrp;
        bsDmnFreezeAttrT *dmnFreezeAttrp;

        dmnMAttrp = bmtr_assign( BSR_DMN_MATTR,
                                sizeof(bsDmnMAttrT),
                                &bmtpg->bsMCA[BFM_BMT_EXT_V3] );
        if (dmnMAttrp == 0) {
            ms_uprintf( "bs_disk_init: can't create domain log attr record" );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        dmnMAttrp->seqNum = 1;
        dmnMAttrp->delPendingBfSet = NilBfTag;
        dmnMAttrp->ftxLogTag = bfDmnParams->ftxLogTag;
        dmnMAttrp->ftxLogPgs = bfDmnParams->ftxLogPgs;
        dmnMAttrp->bfSetDirTag = bfDmnParams->bfSetDirTag;
        dmnMAttrp->vdCnt = 1;
        dmnMAttrp->recoveryFailed = 0;
        dmnMAttrp->mode = S_IRWXU | S_IRGRP | S_IROTH;
        dmnMAttrp->uid = u.u_nd.ni_cred->cr_uid;
        dmnMAttrp->gid = u.u_nd.ni_cred->cr_gid;

        dmnTAttrp = bmtr_assign( BSR_DMN_TRANS_ATTR,
                                sizeof(bsDmnTAttrT),
                                &bmtpg->bsMCA[BFM_BMT] );

        if (dmnTAttrp == 0) {
            ms_uprintf( "bs_disk_init: can't create domain trans attr record" );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        dmnTAttrp->chainVdIndex = -1;
        dmnTAttrp->chainMCId = bsNilMCId;
        dmnTAttrp->op = BSR_VD_NO_OP;
        dmnTAttrp->dev = 0;

        /* 
         * Try to put a BSR_DMN_FREEZE_ATTR on the disk.
         * This record is for freezefs debugging only so we tolerate failure.
         */
        dmnFreezeAttrp = bmtr_assign( BSR_DMN_FREEZE_ATTR,
                                      sizeof(bsDmnFreezeAttrT),
                                      &bmtpg->bsMCA[BFM_BMT] );

        bzero ((char*)dmnFreezeAttrp, sizeof( bsDmnFreezeAttrT ));
    }

    /********************************************************************
     * BMT PREALLOCATION
     ********************************************************************/

    /* Check to see if pre-allocation is requested */
    /* If so, set up BFM_RSVD_CELLS_V3 mcell for a BMT extent and pre-allocate
       the specified amount */

    if (bmtPreallocPgs != 0) {
        struct bsXtraXtntR* xtntp1;
        uint32T sbmPgs, sbmBlks;

        /* Calculate the starting block for the pre-allocation range */

        sbmPgs = howmany( vdattr->vdBlkCnt / vdattr->stgCluster, SBM_BITS_PG);
        sbmBlks = roundup( ADVFS_PGSZ_IN_BLKS * sbmPgs, vdattr->stgCluster );
        bmtFirstPreallocBlk = sbmFirstBlk + sbmBlks;

        /* Initialize BFM_RSVD_CELLS_V3 mcell to belong to BMT */

        bmtpg->bsMCA[BFM_RSVD_CELLS_V3].nextVdIndex = bsNilVdIndex;
        bmtpg->bsMCA[BFM_RSVD_CELLS_V3].nextMCId = bsNilMCId;
        bmtpg->bsMCA[BFM_RSVD_CELLS_V3].linkSegment = 2;
        bmtpg->bsMCA[BFM_RSVD_CELLS_V3].tag = bmtpg->bsMCA[ BFM_BMT_V3 ].tag;
        bmtpg->bsMCA[BFM_RSVD_CELLS_V3].bfSetTag = bmtpg->bsMCA[ BFM_BMT_V3 ].bfSetTag;

        /* Update free list and free count */
        bmtpg->nextfreeMCId.cell += 1;
        bmtpg->freeMcellCnt -= 1;

        /* Assign and set up a new extent record for preallocation pages */
        /* These pages will be allocated after the SBM pages */

        xtntp1 = bmtr_assign( BSR_XTRA_XTNTS,
                              sizeof(struct bsXtraXtntR),
                              &bmtpg->bsMCA[BFM_RSVD_CELLS_V3] );

        if (xtntp1 == 0) {
            ms_uprintf( "bs_disk_init_v3: can't create extent record for BMT preallocation" );
            RAISE_EXCEPTION( EBMTR_NOT_FOUND );
        }

        xtntp1->blksPerPage     = atrp->bfPgSz;
        xtntp1->xCnt            = 2;
        xtntp1->bsXA[0].bsPage = INIT_BMT_PGS_V3;
        xtntp1->bsXA[0].vdBlk = bmtFirstPreallocBlk;
        xtntp1->bsXA[1].bsPage = (INIT_BMT_PGS_V3 + bmtPreallocPgs);
        xtntp1->bsXA[1].vdBlk = XTNT_TERM;

        /* Update the primary extent record */

        xtntp->chainVdIndex     = bsVdParams->vdIndex;
        xtntp->chainMCId.cell   = BFM_RSVD_CELLS_V3;
        xtntp->firstXtnt.mcellCnt++;

        preallocBlks = roundup(ADVFS_PGSZ_IN_BLKS*bmtPreallocPgs, BS_CLUSTSIZE_V3);
    } /* if (bmtPreallocPgs != 0) */

    /********************************************************************
     * STG MAP
     ********************************************************************/

    /* initialize the storage bitmap */
    /* set no next cell, assign reserved tag derived from vdindex */

    bmtpg->bsMCA[ BFM_SBM_V3 ].nextVdIndex = bsNilVdIndex;
    bmtpg->bsMCA[ BFM_SBM_V3 ].nextMCId = bsNilMCId;
    bmtpg->bsMCA[ BFM_SBM_V3 ].linkSegment = 0;


    BS_BFTAG_RSVD_INIT( bmtpg->bsMCA[ BFM_SBM_V3 ].tag,
                        bsVdParams->vdIndex,
                        BFM_SBM_V3 );
    bmtpg->bsMCA[ BFM_SBM_V3 ].bfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the sbm bf */

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &bmtpg->bsMCA[ BFM_SBM_V3 ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create stg map attr record\n" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;

    sts = init_sbm( vp,
                    bmtpg,
                    vdattr,
                    atrp->bfPgSz,
                    rbmtBlks,
                    bmtBlks,
                    preallocBlks,
                    bootBlks,
                    tagBlks,
                    sbmFirstBlk,
                    &freeBlks );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * minBlks is the minimum number of blocks that we need to have
     * a usable domain.
     */

    /* Find the minimum blocks needed for the BMT + one BMT expansion, */
    /* the LOG, 2 TAG files + TAG expansion and 128 pages for files. */
    minBlks = (BS_BMT_XPND_PGS + bmtPreallocPgs + bfDmnParams->ftxLogPgs +
               BS_TD_XPND_PGS + BS_TD_XPND_PGS + 128) * ADVFS_PGSZ_IN_BLKS;
    if (freeBlks < minBlks) {
        ms_uprintf( "bs_disk_init: disk is too small\n" );
        RAISE_EXCEPTION( ENO_MORE_BLKS );
    }

    /********************************************************************
     * TAG DIR
     ********************************************************************/

    /*
     * Initialize the reserved tag directory mcell on each vd.  If
     * this is the selected vd for the tag directory, init a valid mcell.
     * Set no next cell for the reserved tag directory cell.
     */

    bmtpg->bsMCA[ BFM_BFSDIR_V3 ].nextVdIndex = bsNilVdIndex;
    bmtpg->bsMCA[ BFM_BFSDIR_V3 ].nextMCId = bsNilMCId;

    /* set the synthesized tag for the tag dir reserved cell. */

    bmtpg->bsMCA[ BFM_BFSDIR_V3 ].tag = bfSetDirTag;
    bmtpg->bsMCA[ BFM_BFSDIR_V3 ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the tag directory. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &bmtpg->bsMCA[ BFM_BFSDIR_V3 ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create tag dir attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->state = BSRA_VALID;
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->cl.dataSafety = BFD_FTX_AGENT;
    atrp->cl.reqServices = tagSvc;
    atrp->cl.optServices = nilServiceClass;

    /* init a null extent record for the tag directory. */

    xtntp = bmtr_assign(BSR_XTNTS,
                        sizeof(struct bsXtntR),
                        &bmtpg->bsMCA[ BFM_BFSDIR_V3 ]);
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create tag dir extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        xtntp->firstXtnt.xCnt           = 2;
        xtntp->firstXtnt.bsXA[0].bsPage = 0;
        xtntp->firstXtnt.bsXA[0].vdBlk  = tagFirstBlk;
        xtntp->firstXtnt.bsXA[1].bsPage = INIT_TAGDIR_PGS;
        xtntp->firstXtnt.bsXA[1].vdBlk  = XTNT_TERM;
    } else {
        xtntp->firstXtnt.xCnt           = 1;
        xtntp->firstXtnt.bsXA[0].bsPage = 0;
        xtntp->firstXtnt.bsXA[0].vdBlk  = XTNT_TERM;
    }

    xtntp->firstXtnt.mcellCnt       = 1;

    /********************************************************************
     * LOG
     ********************************************************************/

    /*
     * Set no next cell for the reserved ftx log cell.  If this is the
     * selected vd for the ftx log, init the primary mcell for it now.
     */

    bmtpg->bsMCA[ BFM_FTXLOG_V3 ].nextVdIndex = bsNilVdIndex;
    bmtpg->bsMCA[ BFM_FTXLOG_V3 ].nextMCId = bsNilMCId;

    /*
     * Initialize the ftx log primary mcell.
     */

    /* set the synthesized tag for the ftx log reserved cell. */

    BS_BFTAG_RSVD_INIT( bmtpg->bsMCA[ BFM_FTXLOG_V3 ].tag,
                        bsVdParams->vdIndex,
                        BFM_FTXLOG_V3 );

    bmtpg->bsMCA[ BFM_FTXLOG_V3 ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the ftx log. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &bmtpg->bsMCA[ BFM_FTXLOG_V3 ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create ftx log attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    /*
     * The LOG state must be set to BSRA_INVALID until it is opened to avoid
     * the race. The log placeholders (mcell 3 in added volumes)
     * state must be set to BSRA_VALID now since they will not be opened.
     * logSvc will be nilServiceClass when adding volumes
     */
    if ( logSvc ) {
        atrp->state = BSRA_INVALID;
    } else {
        atrp->state = BSRA_VALID;
    }
    atrp->cl.dataSafety = BFD_NO_NWR;   /* can't log the log!! */
    atrp->cl.reqServices = logSvc;
    atrp->cl.optServices = nilServiceClass;

    /* init a null extent record for the ftx log. */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &bmtpg->bsMCA[ BFM_FTXLOG_V3 ] );
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create ftx log extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;
    xtntp->firstXtnt.xCnt           = 1;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = XTNT_TERM;
    xtntp->firstXtnt.mcellCnt       = 1;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;

    /********************************************************************
     * MISC
     ********************************************************************/

    /*
     * Set no next cell for the reserved MISC BF cell.
     */

    bmtpg->bsMCA[ BFM_MISC_V3 ].nextVdIndex = bsNilVdIndex;
    bmtpg->bsMCA[ BFM_MISC_V3 ].nextMCId = bsNilMCId;

    /*
     * Initialize the MISC BF primary mcell.
     */

    BS_BFTAG_RSVD_INIT( bmtpg->bsMCA[ BFM_MISC_V3 ].tag,
                        bsVdParams->vdIndex,
                        BFM_MISC_V3);

    bmtpg->bsMCA[ BFM_MISC_V3 ].bfSetTag = staticRootTagDirTag;

    /* set the bf attributes */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &bmtpg->bsMCA[ BFM_MISC_V3 ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create misc bf attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_PGSZ_IN_BLKS;
    atrp->state = BSRA_VALID;
    atrp->cl.dataSafety = BFD_FTX_AGENT;
    atrp->cl.reqServices = nilServiceClass;
    atrp->cl.optServices = nilServiceClass;

    /* init a null extent record for the ftx log. */
    /*
     * Since nothing follow the MISC mcell, we are adding some slop to the
     * end. The bootblock extent will take up one element. The rest
     * is for future expansion.
     */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR) + (10 * sizeof(bsXtntT)),
                         &bmtpg->bsMCA[ BFM_MISC_V3 ] );
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init_v3: can't create misc bf extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = atrp->bfPgSz;

    /* Initialize to have two pages allocated at blocks 0 to 31 */
    /* Boot blocks are now allocated at blocks 64 to 95 */

    xtntp->firstXtnt.xCnt           = 3;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = mscFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = 2;
    xtntp->firstXtnt.bsXA[1].vdBlk  = bootFirstBlk;
    xtntp->firstXtnt.bsXA[2].bsPage = 4;
    xtntp->firstXtnt.bsXA[2].vdBlk  = XTNT_TERM;

    /* Write the fake super block.  It is zero-filled and contains magic # */

    superBlockPg = (uint32T *) ms_malloc( ADVFS_PGSZ );
    bzero( (char *)superBlockPg, ADVFS_PGSZ );
    superBlockPg[MSFS_MAGIC_OFFSET / sizeof( uint32T )] = MSFS_MAGIC;

    err = write_raw_page (vp,
                          MSFS_FAKE_SB_BLK,
                          ADVFS_PGSZ_IN_BLKS,
                          superBlockPg);
    if (err != 0) {
        ms_uprintf( "bs_disk_init_v3: can't write fake superblock; %d\n", err );
        RAISE_EXCEPTION( err );
    }

    ms_free( superBlockPg );
    superBlockPg = NULL;

    /***********************************************************************
     * Finally write out page bmt page 0, init and write out bmt page 1,
     * init and write out BMT preallocated pages if any, and init and write
     * out tagdir page 0.
     */

    err = write_raw_bmt_page (vp, MSFS_RESERVED_BLKS, bmtpg);
    if (err != 0) {
        ms_uprintf( "bs_disk_init_v3: can't write raw bmt page 0; status was %d\n", err);
        RAISE_EXCEPTION( err );
    }

    bmt_init_page( bmtpg, 1, BFM_BMT, dmnVersion );
    if (!bmt_init_mcell_free_list( bmtpg )) {
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }
    del_init_mcell_list( bmtpg );

    if (bmtPreallocPgs) {
        int pg, newpg=INIT_BMT_PGS_V3;
        uint32T startBlk=bmtFirstPreallocBlk;

        bmtpg->nextFreePg = newpg;

        err = write_raw_bmt_page( vp,
                              MSFS_RESERVED_BLKS + ADVFS_PGSZ_IN_BLKS,
                              bmtpg );
        if (err != 0) {
            ms_uprintf( "bs_disk_init_v3: can't write raw bmt page 1; status was %d\n", err);
            RAISE_EXCEPTION( err );
        }

        for (pg=0; pg<bmtPreallocPgs; pg++) {
            bmt_init_page(bmtpg, newpg, BFM_BMT, dmnVersion);
            if (pg < (bmtPreallocPgs - 1)) {
                bmtpg->nextFreePg = ++newpg;
            }
            err = write_raw_bmt_page( vp, startBlk, bmtpg );
            if (err != 0) {
                ms_uprintf( "bs_disk_init_v3: can't write raw bmt page %d, block %d, status %d\n", pg+2, startBlk, err);
                RAISE_EXCEPTION( err );
            }
            startBlk += ADVFS_PGSZ_IN_BLKS;
        }
    }
    else {

        err = write_raw_bmt_page( vp,
                                  MSFS_RESERVED_BLKS + ADVFS_PGSZ_IN_BLKS,
                                  bmtpg );
        if (err != 0) {
            ms_uprintf( "bs_disk_init-v3: can't write raw bmt page 1; status was %d\n", err);
            RAISE_EXCEPTION( err );
        }
    }

    ms_free( bmtpg );
    bmtpg = NULL;

    if (tagBlks > 0) {
        bsTDirPgT *tdpgp = (bsTDirPgT*) ms_malloc( sizeof(bsTDirPgT) );

        tagdir_init_pg0( tdpgp );

        err = write_raw_page( vp, tagFirstBlk, ADVFS_PGSZ_IN_BLKS, tdpgp );
        if (err != 0) {
            ms_uprintf( "bs_disk_init_v3: can't write raw tag page 0. status was %d\n", err);
            RAISE_EXCEPTION( err );
        }

        ms_free( tdpgp );
    }

    VOP_CLOSE( vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, i );

    vrele(vp);

    return err;

HANDLE_EXCEPTION:

    if (devOpen) {
        VOP_CLOSE( vp, FREAD | FWRITE | OTYP_MNT, ndp->ni_cred, i );
    }

    if (superBlockPg != NULL) {
        ms_free( superBlockPg );
    }

    if (bmtpg != NULL) {
        ms_free( bmtpg );
    }

    return sts;

} /* end bs_disk_init_v3 */

/*
 * bmt_init_mcell_free_list
 *
 * Initialize the mcell free list head
 * which is contained in a bmt record in the first mcell on
 * page 1 of the bmt.
 * For AdvFS on-disk version 4, this list is conatined in the first
 * mcell of page 0 of the bmt.
 *
 * Returns zero of failed, 1 if succeeded.
 */


static int
bmt_init_mcell_free_list(
    bsMPgT* bmtpgp              /* in - pointer to bmt page 1/0 */
    )
{
    bsMcellFreeListT *mcellFreeListp;
    bsMCT* mcp;

    /*** get ptr to first mcell in page 1/0 ***/
    mcp = &bmtpgp->bsMCA[0];

    /*** allocate first mcell in page 1/0 ***/
    mcp->nextMCId = bsNilMCId;
    bmtpgp->nextfreeMCId.cell = 1;
    bmtpgp->freeMcellCnt--;

    /*** create the mcell free list record ***/
    mcellFreeListp = bmtr_assign( BSR_MCELL_FREE_LIST,
                                  sizeof( bsMcellFreeListT ),
                                  mcp );
    if (mcellFreeListp == NULL) {
        ms_uprintf("bmt_init_mcell_free_list: could not assign mcell record");
        return 0;
    }

    /*** mcell free list starts out empty ***/
    mcellFreeListp->headPg = bmtpgp->pageId;

    return 1;
}

/*
 * Initialize the deferred delete bitfile chain.  Designed to be called from
 * bmt_extend when page 1 of the BMT is initialized.
 *
 * Returns zero if failed, 1 if succeeded.
 */


static int
del_init_mcell_list(
    bsMPgT *bmtp
    )
{
    delLinkRT *dlp;
    bsMCT *mcp;

    mcp = &bmtp->bsMCA[MCELL_LIST_CELL];

    dlp = bmtr_assign(BSR_DEF_DEL_MCELL_LIST, sizeof(delLinkRT), mcp);
    if (dlp == NULL) {
        ms_uprintf("del_init_mcell_list: bmtr_assign failed.");
        return 0;
    }

    dlp->nextMCId = bsNilMCId;
    dlp->prevMCId = bsNilMCId;

    return 1;
}

/*
 * tagdir_init_pg0
 *
 * Initializized the root tagdir's page zero.  Note that tag map slot
 * zero is used for the free page list header.
 */

static void
tagdir_init_pg0(
    bsTDirPgT *tdpgp
    )
{
    int i;

    bzero( (char *) tdpgp, sizeof( bsTDirPgHdrT ) );
    tdpgp->tpCurrPage = 0;

    for (i = 0; i < BS_TD_TAGS_PG; i++) {
        /* ugly, but overlays seqNo and zeroes vdIndex */
        tdpgp->tMapA[ i ].tmFreeListHead = 1;
        tdpgp->tMapA[ i ].tmNextMap = i + 2;
    }

    tdpgp->tMapA[ BS_TD_TAGS_PG - 1 ].tmNextMap = 0;

    /*
     * Page 0 is special in that the first tagmap struct is used
     * to store some metadata information; the free page list header.
     */

    tdpgp->tpNextFreeMap = 2;
    tdpgp->tMapA[ 0 ].tmFreeListHead = 1;
    tdpgp->tMapA[ 0 ].tmUninitPg = 1;
}

/*
 * init_sbm
 *
 * Sets SBM bits for the disk blocks occupied by the reserved section,
 * the initial part of the BMT, TAGDIR, and the SBM itself.
 */

statusT
init_sbm(
#ifdef KERNEL
    struct vnode *vp,           /* in */
#else
    int fd,                     /* in */
#endif
    struct bsMPg *bmtpg,        /* in */
    struct bsVdAttr *vdattr,    /* in */
    uint32T blksPerPage,        /* in */
    uint32T rbmtBlks,           /* in */
    uint32T bmtBlks,            /* in */
    uint32T preallocBlks,       /* in */
    uint32T bootBlks,           /* in */
    uint32T tagBlks,            /* in */
    uint32T sbmFirstBlk,        /* in */
    uint32T *freeBlks           /* out */
    )
{
    /*
     * Initialize the bitmap bits.
     */

    uint32T sbmPgs, sbmBlks;
    uint32T sbmFirstPage, sbmFirstBit;
    uint32T first_setBlks, second_setBlks = 0;
    uint32T set_bits, first_bits_to_set, second_bits_to_set = 0; 
    int i, j, err;
    statusT sts;
    struct bsXtntR* xtntp;
    struct bsStgBm *sbm = NULL;

    sbmPgs = howmany( vdattr->vdBlkCnt / vdattr->stgCluster, SBM_BITS_PG);
    sbmBlks = roundup( ADVFS_PGSZ_IN_BLKS * sbmPgs, vdattr->stgCluster );

    if (rbmtBlks == 0) {   /* V3 domain */
        first_setBlks = MSFS_RESERVED_BLKS + bmtBlks + preallocBlks + 
                        bootBlks + tagBlks + sbmBlks;
        first_bits_to_set = first_setBlks / vdattr->stgCluster;
    } else {               /* V4 domain */
        first_setBlks = MSFS_RESERVED_BLKS + rbmtBlks + bmtBlks + 
                        bootBlks + tagBlks;
        first_bits_to_set = first_setBlks / vdattr->stgCluster;
        second_setBlks = preallocBlks + sbmBlks;
        second_bits_to_set = second_setBlks / vdattr->stgCluster;
        sbmFirstPage = (sbmFirstBlk / vdattr->stgCluster) / SBM_BITS_PG;
        sbmFirstBit = (sbmFirstBlk / vdattr->stgCluster) % SBM_BITS_PG;
    }

    sbm = (struct bsStgBm *) ms_malloc( sizeof( struct bsStgBm ) );
    if (sbm == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    if (first_setBlks  + second_setBlks > vdattr->vdBlkCnt) {
        ms_uprintf("bs_disk_init: disk is too small\n");
        RAISE_EXCEPTION( ENO_MORE_BLKS );
    }

    *freeBlks = vdattr->vdBlkCnt - (first_setBlks + second_setBlks);

    for (i = 0; i < sbmPgs; i++) {
        sbm->lgSqNm = 0;
        sbm->xor    = 0;

        for (j = 0; j < SBM_LONGS_PG; j++) {
            sbm->mapInt[j] = 0;
        }
        /*
         * Set bits for MSFS_RESERVED_BLKS, bmtBlks, bootBlks, and tagBlks.
         * And for V3 domains, set bits for BMT preallocBlks and sbmBlks.
         */
        if (first_bits_to_set > 0) {
            set_bits = MIN( SBM_BITS_PG, first_bits_to_set );
            sbm_set_pg_bits( 0, set_bits, sbm );
            first_bits_to_set -= set_bits;
        }

        /*
         * For V4 domains, set bits for BMT preallocBlks and sbmBlks.
         */
        if ((second_bits_to_set > 0) && (i >= sbmFirstPage)) {
            if (i == sbmFirstPage) {
                set_bits = MIN( SBM_BITS_PG - sbmFirstBit, second_bits_to_set );
                sbm_set_pg_bits( sbmFirstBit, set_bits, sbm );
            } else {
                set_bits = MIN( SBM_BITS_PG, second_bits_to_set );
                sbm_set_pg_bits( 0, set_bits, sbm );
            }
            second_bits_to_set -= set_bits;
        }

#ifdef KERNEL
        err = write_raw_sbm_page( vp,
#else
        err = write_raw_sbm_page( fd,
#endif
                                  sbmFirstBlk + (i * ADVFS_PGSZ_IN_BLKS),
                                  sbm );

        if (err != 0) {
            RAISE_EXCEPTION( err );
        }
    }

    ms_free( sbm );
    sbm = NULL;

    /* Init an xtnt record for the sbm. */

    xtntp = bmtr_assign(BSR_XTNTS,
                        sizeof(struct bsXtntR),
                        &bmtpg->bsMCA[ BFM_SBM ]);
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create stg map extents record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->type           = BSXMT_APPEND;
    xtntp->chainVdIndex   = bsNilVdIndex;
    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = bsNilVdIndex;
    xtntp->rsvd2          = bsNilMCId;
    xtntp->blksPerPage    = blksPerPage;
    xtntp->firstXtnt.xCnt           = 2;
    xtntp->firstXtnt.bsXA[0].bsPage = 0;
    xtntp->firstXtnt.bsXA[0].vdBlk  = sbmFirstBlk;
    xtntp->firstXtnt.bsXA[1].bsPage = sbmPgs;
    xtntp->firstXtnt.bsXA[1].vdBlk  = XTNT_TERM;
    xtntp->firstXtnt.mcellCnt       = 1;

    return EOK;

HANDLE_EXCEPTION:

    if (sbm != NULL) {
       ms_free( sbm );
    }

    return sts;
}

/*******************************************************************************
 *  
 *  vd_extend() - Extend the size of a vd.
 *
 *  This routine is called to extend the size of a vd.  It is
 *  invoked at mount time if the -o extend option is used and
 *  the size of the vd has increased.  It can be invoked in
 *  two ways:
 *              mount -o extend domain#fset mountpoint
 *              mount -u -o extend domain#fset mountpoint (if already mounted)
 *
 *      Arguments:       vdp - (in ) pointer to the virtual disk structure
 *      Pre-condition:   the mount -o extend option was used on a fileset
 *                       in the domain that is using this vd.
 *      Post -condition: the size of the virtual disk is extended if
 *                       there is unused disk space detected.
 *      Return codes:    EOK    if disk was extended,
 *                       -1     if the vd did not need to be extended
 *                       ENOSPC if the extend failed for other reasons. 
 * 
 *  Notes:
 *
 *    In V4 and V3 domains, each SBM page contains 65,472 mapping bits,
 *    each bit maps a storage cluster.  A cluster is one or more
 *    contiguous disk blocks.
 *
 *    In V4 domains, a cluster is 16 blocks (8k), so
 *        each V4 SBM page maps 1,047,552 blocks (~512 MB) of storage.
 *
 *    In V3 domains, a cluster is 2 blocks, (1k), so
 *        each V3 SBM page maps   130,944 blocks (~ 64 MB) of storage
 *
 */
/*
*  There are several volume sizes in-memory and on disk.
*  vdBlkCnt stored in the vdAttr record on disk is the true size of
*  the volume in blocks. It may not be a multiple of ADVFS_PGSZ.
*  This is the only volume size that might not be a multiple of ADVFS_PGSZ.
*  vdSize in the vdT is the size of the volume in blocks rounded down
*  to a page boundary.
*  vdClusters in the vdT is the size of the volume in clusters rounded
*  down to a page.
*  freeClust in the vdt is the number of free clusters within vdClusters.
*  totalBlks in the domainT is the total of vdSize for all the volumes in
*  the domain.
*  freeBlks in the domainT is the number of free blocks within totalBlks.
*/
int
vd_extend(struct vd *vdp)
{
    lbnT         volSz;
    lbnT         oldVdSize;
    lbnT         newVdSize;
    lbnT         growth;
    lbnT         sbmBlksMappedPerPage;
    lbnT         xtraBlksMappedInSbm;
    lbnT         blksToMapInNewSbm;
    lbnT         xtraBitsInSbm;
    bsPageT      newSbmPgs;
    uint32T      newSbmBitsToSet;
    uint32T      newFreeClusters;
    uint32T      sbmBitsToSet;                       
    int          ret;
    boolean_t    xtntMapLocked = FALSE;
    boolean_t    mcellListLocked = FALSE;
    boolean_t    stgMapLocked = FALSE;
    boolean_t    otherLksLocked = FALSE;
    bfAccessT    *sbmBfap = vdp->sbmp;
    bfAccessT    *mdBfap = RBMT_THERE(vdp->dmnP)? vdp->rbmtp : vdp->bmtp;
    ftxHT        ftxH;
    statusT      sts;
    bsVdAttrT    vdAttr;
    getioinfo_t  *getioinfop;
    advfs_ev     *advfs_event;
    struct vnode *vnp = NULL;

    /*
     *  Check for cluster rolling upgrade
     */
    if ( clu_is_ready() && (VERSW() != VERSW_ENABLE) ) {
        return 0;
    }

    getioinfop = (getioinfo_t*)ms_malloc( sizeof(getioinfo_t) );
    getioinfop->pi_version = PI_VERSION_0;

    /*
     *  Get the current vd size.
     */
    vnp = vdp->devVp;
    VOP_IOCTL( vnp, DIOCGETIOINFO, getioinfop, FREAD, NOCRED, sts, &ret );

    if ( sts == EOK ) {
        volSz = getioinfop->pi_un.pi_v0.pi_part.p_size;   /* true volume size */
        ms_free( getioinfop );
    } else if ( sts == EFBIG ) {
        ms_free( getioinfop );
        ms_uprintf("NOTICE: virtual disk %s is larger than AdvFS supports.\n"
                   "        All blocks over the limit will be wasted.\n" ,
                   vdp->vdName);
        volSz = UINT_MAX;
    } else {
        ms_free( getioinfop );
        return ENOSPC;
    }
    newVdSize = volSz & ~(ADVFS_PGSZ_IN_BLKS - 1);

    /*
     *  Start up a transaction.
     */
    sts = FTX_START_N( FTA_BS_VD_EXTEND, &ftxH, FtxNilFtxH, vdp->dmnP, 1 );
    if ( sts != EOK ) {
        return ENOSPC;
    }

    /*
     *  Load the extent map for the SBM.  Since this takes
     *  the mcell list lock, we need to do this before
     *  locking the mcell list for the (R)BMT to abide
     *  by the lock heirarchy rules. 
     */
    sts = x_load_inmem_xtnt_map( sbmBfap, X_LOAD_UPDATE );
    if ( sts ) {
        goto error;
    }

    /*
     *  Add the mcell list lock to the transaction
     *  and lock the extent map
     */
    FTX_ADD_LOCK( &sbmBfap->mcellList_lk, ftxH );
    XTNMAP_LOCK_WRITE( &sbmBfap->xtntMap_lk );
    xtntMapLocked = TRUE;

    /*
     *  Get the old vd size from our AdvFS disk info.
     *  Keep the mcell lock to prevent racing with another thread.
     */
    sts = bmtr_get_rec_n_lk( mdBfap,
                             BSR_VD_ATTR,
                             &vdAttr,
                             sizeof(bsVdAttrT),
                             BMTR_LOCK );
    if (sts != EOK) {
        goto error;
    }

    mcellListLocked = TRUE;
    oldVdSize = vdAttr.vdBlkCnt & ~(ADVFS_PGSZ_IN_BLKS - 1);  /* round down */

    /*
     * Must prevent volume shrinking below previous size and
     * if not adding more than 16 pages, nothing to do.
     */
    if ( (newVdSize < oldVdSize) ||
         (newVdSize - oldVdSize <= (16 * ADVFS_PGSZ_IN_BLKS)) )
    { 
        XTNMAP_UNLOCK(&sbmBfap->xtntMap_lk);
        MCELLIST_UNLOCK(&mdBfap->mcellList_lk);
        ftx_fail( ftxH );
        return -1;  /* this error return is ignored */
    }

    /*
     *  Calculate the size of the new sbm extent,
     *  and it's start block (page aligned), and a lot
     *  of other stuff.
     */
    growth = newVdSize - oldVdSize;
    sbmBlksMappedPerPage  = SBM_BITS_PG * vdAttr.stgCluster;
    xtraBlksMappedInSbm = sbmBfap->nextPage * sbmBlksMappedPerPage - oldVdSize;
    if ( growth > xtraBlksMappedInSbm ) {
        blksToMapInNewSbm = growth - xtraBlksMappedInSbm;
    } else {
        blksToMapInNewSbm = 0;
    }
    newSbmPgs = howmany( blksToMapInNewSbm, sbmBlksMappedPerPage );
    newSbmBitsToSet = newSbmPgs * ADVFS_PGSZ_IN_BLKS / vdAttr.stgCluster;
    newFreeClusters = growth / vdAttr.stgCluster - newSbmBitsToSet;

    if ( newSbmPgs == 0 ) {
        /* No additional SBM pages needed, just update the vd's size. */
        STGMAP_LOCK_WRITE( &vdp->stgMap_lk ); /* protect vd_size, vd_Clusters */
        stgMapLocked = TRUE;
    } else {
        /* New SBM pages required. */
        /* The code to grow the SBM requires locks that must be taken here */
        /* to preserve lock hierarchy. */
        XTNMAP_LOCK_WRITE( &vdp->bmtp->xtntMap_lk );  /* for hierarchy */
        lock_write( &vdp->rbmt_mcell_lk.lock );
        otherLksLocked = TRUE;
        STGMAP_LOCK_WRITE( &vdp->stgMap_lk ); /* protect vd_size, vd_Clusters */
        stgMapLocked = TRUE;
    
        xtraBitsInSbm = xtraBlksMappedInSbm / vdAttr.stgCluster;
        sts = vd_extend_add_sbm_pgs( vdp, newSbmPgs, newSbmBitsToSet,
                                     xtraBitsInSbm, &vdAttr, ftxH );
        if ( sts != EOK ) {
            goto error;
        }
    }

    /*
     *  The SBM has been updated, the last thing
     *  to do is to update the BSR_VD_ATTR, the vd,
     *  and the access structure.
     */
    vdAttr.vdBlkCnt = volSz;
    sts  = bmtr_update_rec_n_unlk( mdBfap,
                                   BSR_VD_ATTR,
                                   &vdAttr,
                                   sizeof(bsVdAttrT),
                                   ftxH,
                                   BMTR_UNLOCK );
    if ( sts ) {
        goto error;
    }

    vdp->vdSize           = newVdSize;
    vdp->vdClusters       = vdp->vdSize / vdAttr.stgCluster;
    vdp->freeClust       += newFreeClusters;
    vdp->dmnP->freeBlks  += newFreeClusters * vdAttr.stgCluster;
    vdp->dmnP->totalBlks += growth;

    vdp->bitMapPgs       += newSbmPgs;
    sbmBfap->nextPage    += newSbmPgs;
    MS_SMP_ASSERT(vdp->bitMapPgs == sbmBfap->nextPage);
    MS_SMP_ASSERT(imm_check_xtnt_map(sbmBfap->xtnts.xtntMap) == EOK);


    /*
     *  Do some housekeeping, end the transaction and done!
     */

    ftx_done_n( ftxH, FTA_BS_VD_EXTEND );

    if ( otherLksLocked ) {
        XTNMAP_UNLOCK( &vdp->bmtp->xtntMap_lk );
        lock_done( &vdp->rbmt_mcell_lk.lock );
    }
    STGMAP_UNLOCK( &vdp->stgMap_lk );
    XTNMAP_UNLOCK( &sbmBfap->xtntMap_lk );

    advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
    if ( advfs_event != NULL ) {
        advfs_event->special = vdp->vdName;
        advfs_event->domain = vdp->dmnP->domainName;
        advfs_post_kernel_event( EVENT_FDMN_VD_EXTEND, advfs_event );
        ms_free( advfs_event );
    }

    return ESUCCESS;

error:
    /*
     *  Error handler
     */

    if ( xtntMapLocked ) {
        XTNMAP_UNLOCK( &sbmBfap->xtntMap_lk );
    }

    if ( otherLksLocked ) {
        XTNMAP_UNLOCK( &vdp->bmtp->xtntMap_lk );
        lock_done( &vdp->rbmt_mcell_lk.lock );
    }

    if ( stgMapLocked ) {
        STGMAP_UNLOCK( &vdp->stgMap_lk );
    }

    if ( mcellListLocked ) {
        MCELLIST_UNLOCK( &mdBfap->mcellList_lk );
    }

    ftx_fail( ftxH );

    return ENOSPC;
}

static statusT
vd_extend_add_sbm_pgs( vdT *vdp,
                       bsPageT newSbmPgs,
                       lbnT newSbmBitsToSet,
                       lbnT xtraBitsInSbm,
                       bsVdAttrT *vdAttrp,
                       ftxHT ftxH )
{
    bfAccessT *sbmBfap = vdp->sbmp;
    bfAccessT *mdBfap = RBMT_THERE(vdp->dmnP) ? vdp->rbmtp : vdp->bmtp;
    lbnT oldVdSize = vdAttrp->vdBlkCnt & ~(ADVFS_PGSZ_IN_BLKS - 1);
    lbnT sbmBitsToSet;
    lbnT newSbmBlk;
    struct bsStgBm *sbmPgp = NULL;
    bsInMemXtntT *xtnts= &sbmBfap->xtnts;
    bsInMemXtntMapT *xtntMap = xtnts->xtntMap;
    bsInMemSubXtntMapT *origSubXtntMap;
    bsInMemSubXtntMapT *newSubXtntMap;
    struct vnode *vnp = vdp->devVp;
    statusT sts;
    uint i;
    uint j;

    newSbmBlk = oldVdSize;
    /*
     *  New SBM page(s) need to be allocated.
     *
     *  We need to remove the pages of the new SBM extent
     *  from the free list.  This information may be contained
     *  the last SBM page of the previous extent and/or
     *  the first page(s) of the new extent.
     *
     *  If there's extra mapping capacity in the current SBM
     *      Prepare to set the appropriate bits
     */

    if ( xtraBitsInSbm ) {
        sbmBitsToSet = MIN( xtraBitsInSbm, newSbmBitsToSet );
        sts = sbm_alloc_bits( vdp,
                              newSbmBlk / vdAttrp->stgCluster,
                              sbmBitsToSet,
                              ftxH );
        if ( sts ) {
            return sts;
        }
        newSbmBitsToSet -= sbmBitsToSet;
    }

    sbmPgp = (struct bsStgBm *)ms_malloc( sizeof(struct bsStgBm) );

    /*
     *  Initialize the new SBM pages.
     *  Check if we need to set bits in the newly allocated SBM blocks
     *  These updates do not need to be under transaction control as
     *  the data is only significant if the transaction to update the
     *  virtual disk size succeeds.
     */
    for ( i = 0; i < newSbmPgs; i++ ) {
        sbmPgp->lgSqNm = 0;
        sbmPgp->xor = 0;

        for ( j = 0; j < SBM_LONGS_PG; j++ ) {
            sbmPgp->mapInt[j] = 0;
        }

        if ( newSbmBitsToSet ) {
            sbmBitsToSet = MIN( SBM_BITS_PG, newSbmBitsToSet );
            sbm_set_pg_bits( 0, sbmBitsToSet, sbmPgp );
            newSbmBitsToSet -= sbmBitsToSet;
        }

        /*
         *  The new SBM pages will be in the newly added vd space.
         *  This guarantees that there will be space for the new SBM pages.
         */
        sts = write_raw_sbm_page( vnp, newSbmBlk, sbmPgp );
        if ( sts ) {
            ms_free( sbmPgp );
            return sts;
        }

        newSbmBlk += ADVFS_PGSZ_IN_BLKS;
    }

    ms_free( sbmPgp );

    /*
     *  The new SBM pages have been initialized.
     *  Get the pointer to the extent map and the pointer
     *  to the last valid sub extent map.  Insure that there
     *  is at least one unused sub extent map for the update.
     */

    xtntMap = sbmBfap->xtnts.xtntMap;
    origSubXtntMap = &xtntMap->subXtntMap[xtntMap->validCnt - 1];

    if ( origSubXtntMap->cnt < origSubXtntMap->onDiskMaxCnt ) {
        /* The new SBM extent descriptor will fit in existing mcell. */
        if ( origSubXtntMap->cnt == origSubXtntMap->maxCnt ) {
            sts = imm_extend_sub_xtnt_map( origSubXtntMap );
            if ( sts != EOK ) {
                return sts;
            }
        }

        /*
         *  Add the new extent descriptor.
         */
        i = origSubXtntMap->cnt;
        origSubXtntMap->bsXA[i - 1].vdBlk = oldVdSize;
        MS_SMP_ASSERT(sbmBfap->nextPage == origSubXtntMap->bsXA[i - 1].bsPage);
        origSubXtntMap->bsXA[i].bsPage = sbmBfap->nextPage + newSbmPgs;
        origSubXtntMap->bsXA[i].vdBlk = XTNT_TERM;

        origSubXtntMap->pageCnt += newSbmPgs;

        origSubXtntMap->updateStart = 0;
        origSubXtntMap->updateEnd = origSubXtntMap->cnt;
        origSubXtntMap->cnt++;

        /*
         *  Update the on disk data
         */
        sts = update_xtnt_rec( sbmBfap->dmnP,
                               sbmBfap->tag,
                               origSubXtntMap,
                               ftxH );
        if ( sts != EOK ) {
            origSubXtntMap->cnt--;
            origSubXtntMap->pageCnt -= newSbmPgs;
            return sts;
        }

        MS_SMP_ASSERT(sbmBfap->nextPage == xtntMap->nextValidPage);
        MS_SMP_ASSERT(sbmBfap->nextPage == xtnts->allocPageCnt);
        xtntMap->nextValidPage += newSbmPgs;
        xtnts->allocPageCnt    += newSbmPgs;
    } else {
        /* The new SBM extent won't fit in the existing mcells. */
        /* Allocate a new mcell and sub extent map. */
        if ( xtntMap->validCnt == xtntMap->maxCnt ) {
            sts = imm_extend_xtnt_map( xtntMap );
            if ( sts != EOK ) {
                return sts;
            }
        }

        origSubXtntMap = &xtntMap->subXtntMap[xtntMap->validCnt - 1];
        newSubXtntMap = &xtntMap->subXtntMap[xtntMap->validCnt];

        sts = bmt_alloc_mcell( sbmBfap,
                               origSubXtntMap->vdIndex,
                               RBMT_MCELL,
                               sbmBfap->bfSetp->dirTag,
                               sbmBfap->tag,
                               0,
                               ftxH,
                               &newSubXtntMap->mcellId,
                               FALSE );
        if ( sts != EOK ) {
            return sts;
        }

        sts = imm_init_sub_xtnt_map( newSubXtntMap,
                                     sbmBfap->nextPage,
                                     newSbmPgs,
                                     origSubXtntMap->vdIndex,
                                     newSubXtntMap->mcellId,
                                     BSR_XTRA_XTNTS,
                                     BMT_XTRA_XTNTS,
                                     0 );
        if ( sts != EOK ) {
            return sts;
        }

        /*
         *  Set up the extent descriptors for this extent map.
         */
        newSubXtntMap->bsXA[0].bsPage = sbmBfap->nextPage;
        newSubXtntMap->bsXA[0].vdBlk  = oldVdSize;
        newSubXtntMap->bsXA[1].bsPage = sbmBfap->nextPage + newSbmPgs;
        newSubXtntMap->bsXA[1].vdBlk  = XTNT_TERM;
        newSubXtntMap->cnt = 2;

        origSubXtntMap->updateStart = 0;
        origSubXtntMap->updateEnd   = 1;

        /*
         *  This subxtnt has a brand new Mcell, we must create its
         *  on-disk xtnt rec.
         */
        sts = odm_create_xtnt_rec( sbmBfap,
                                   xtntMap->allocVdIndex,
                                   newSubXtntMap,
                                   0,  /* no clone xfer xtnts here */
                                   ftxH );
        if ( sts != EOK ) {
            return sts;
        }

        newSubXtntMap->mcellState = INITIALIZED;

        sts = bmt_link_mcells( sbmBfap->dmnP,
                               sbmBfap->tag,
                               origSubXtntMap->vdIndex,
                               origSubXtntMap->mcellId,
                               newSubXtntMap->vdIndex,
                               newSubXtntMap->mcellId,
                               newSubXtntMap->vdIndex,
                               newSubXtntMap->mcellId,
                               ftxH );
        if ( sts != EOK ) {
            return sts;
        }

        /*
         *  Increment the count of mcells
         */
        sts = update_mcell_cnt( sbmBfap->dmnP,
                                sbmBfap->tag,
                                xtntMap->hdrVdIndex,
                                xtntMap->hdrMcellId,
                                xtntMap->hdrType,
                                1,
                                ftxH );
        if ( sts != EOK ) {
            return sts;
        }

        xtntMap->cnt++;
        xtntMap->validCnt = xtntMap->cnt;
        xtnts->allocPageCnt += newSbmPgs;
        xtntMap->nextValidPage += newSbmPgs;
    }

    return EOK;
}

/* end bs_init.c */

