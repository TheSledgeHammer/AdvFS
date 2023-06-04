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
 *
 * Facility:
 *
 *      AdvFS
 *
 * Abstract:
 *
 *      Domain and disk initialization routines.
 *
 */

#include <sys/file.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/kernel.h>
#include <sys/mount.h>
#include <sys/buf.h>
#include <sys/vnode.h>
#include <sys/stat.h>
#include <sys/cred.h>
#include <sys/ioctl.h>
#include <sys/fs.h>     /* for setting fields in real superblock */
#include <sys/diskio.h>
#include <sys/conf.h>
#include <sys/swap.h>
#include <h/kthread_access.h>   /* private: for DEVSW(), needs KT_SEMA() */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <bs_migrate.h>
#include <bs_delete.h>
#include <tcr/clu.h>
#include <bs_vd.h>
#include <bs_extents.h>
#include <advfs_evm.h>
#include <sys/fs/advfs_public.h>
#include <bs_domain.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_INIT

static void tagdir_init_pg0( bsTDirPgT *tdpgp, bfFsCookieT dmnCookie );
static int  del_init_mcell_list( bsMPgT *bmtp );
static int  bmt_init_mcell_free_list( bsMPgT* bmtpgp );

#define ADVFS_INIT_RBMT_FOBS          ADVFS_METADATA_PGSZ_IN_FOBS 
#define ADVFS_INIT_BMT_FOBS           ADVFS_METADATA_PGSZ_IN_FOBS 
#define ADVFS_INIT_TAGDIR_FOBS        ADVFS_METADATA_PGSZ_IN_FOBS 
/*
 * DEV_BSIZE blocks reserved for boot area of disk. (Superblock)
 */
#define ADVFS_INIT_BOOT_BLKS         16 
/*
 * 128 8k pages minimum for user storage.
 */
#define ADVFS_INIT_USER_STG_FOBS      128 * 8 

static uint32_t metaPhysLocPct = 40;   /* default physical loc for SBM, BMT, LOG */

static statusT
init_sbm(
    struct vnode *vp,            /* in */
    struct bsMPg *bmtpg,         /* in */
    struct bsVdAttr *vdattr,     /* in */
    bfFsCookieT dmnCookie,       /* in */
    bf_vd_blk_t rbmtBlks,        /* in - in DEV_BSIZE blks */
    bf_vd_blk_t bmtBlks,         /* in - in DEV_BISZE blks */
    bf_vd_blk_t preallocBlks,    /* in - in DEV_BSIZE blks */
    bf_vd_blk_t bootBlks,        /* in - in DEV_BSIZE blks */
    bf_vd_blk_t tagBlks,         /* in - in DEV_BSIZE blks */
    bf_vd_blk_t sbmFirstBlk,     /* in - in DEV_BSIZE blks */
    bf_vd_blk_t shadowBlks,      /* in - in DEV_BSIZE blks */
    bf_vd_blk_t shadowFirstBlk,  /* in - in DEV_BSIZE blks */
    bf_vd_blk_t shadowSecondBlk, /* in - in DEV_BSIZE blks */
    bf_vd_blk_t *freeBlks        /* out - in DEV_BSIZE blks */
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
    bf_fob_t logFobs,           /* in - number of fobs in log */
    bf_fob_t userAllocFobs,     /* in - number of fobs per user data page */
    serviceClassT logSvc,       /* in - log service attributes */
    serviceClassT tagSvc,       /* in - tag directory service attributes */
    char *vdName,               /* in - block special device name */
    serviceClassT vdSvc,        /* in - service class */
    bf_vd_blk_t vdSize,         /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs to be preallocated for the BMT */
    adv_ondisk_version_t domainVersion,/* in - on-disk version for domain */
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
    vdparamsp = (bsVdParamsT *) ms_malloc( sizeof( bsVdParamsT ));

    ltime = get_system_time();

    /*
     * For cluster, the domainID is unique in the cluster.  The lower
     * 8 bits of the member ID are combined in the time stamp to form the
     * domainId and verified by CFS for uniqueness.
     */
    if (clu_is_ready()) {
        while (TRUE) {
            ltime.tv_usec &= 0x00ffffff;
            ltime.tv_usec |= (clu_memberid << 24);
            if (CLU_FIND_CLUSTER_DOMAIN_ID(&ltime) == 0) {
                break;
            }
            ltime = get_system_time();
        }
    }

    if (SC_EQL( vdSvc, nilServiceClass )) {
        /* TEMP - need real "default service class" from storage mgt? */
        vdSvc = defServiceClass;
    }

    BS_BFTAG_RSVD_INIT( bfdmnparamsp->bfSetDirTag, 1, BFM_BFSDIR );
    BS_BFTAG_RSVD_INIT( bfdmnparamsp->ftxLogTag, 1, BFM_FTXLOG );
    /* field sizes don't match so copy individually  */
    bfdmnparamsp->bfDomainId.id_sec  = ltime.tv_sec;
    bfdmnparamsp->bfDomainId.id_usec = ltime.tv_usec;

    /* set the dmnCookie to bfDomainId since this is a new domain 
     * the dmnCookie must not change after this point
     */
    bfdmnparamsp->dmnCookie = bfdmnparamsp->bfDomainId;
    bfdmnparamsp->maxVds     = maxVds;
    bfdmnparamsp->curNumVds  = 1;
    /* Convert the fobs to pages (rounded up unless on a page 
     * boundary).  Currently we are using the metadata pgsize in fobs for 
     * the conversion.  This may need to change in the future if the logPgSz 
     * is changed.
     *
     *                 --> to be consistent with userspace, the bfDmnParamsT
     *                     now refers to "ftxLogFobs".  So this call to
     *                     lgr_calc_num_pgs() basically is just a roundup
     *                     then we multiply by ADVFS_METADATA_PGSZ_IN_FOBS.
     *                     bs_disk_init() was also changed because of this
     */
    bfdmnparamsp->ftxLogFobs = lgr_calc_num_pgs(howmany(logFobs, 
                ADVFS_METADATA_PGSZ_IN_FOBS),vdSize) * ADVFS_METADATA_PGSZ_IN_FOBS;

    if (strlen( domain ) > (BS_DOMAIN_NAME_SZ - 1)) {
        RAISE_EXCEPTION( EINVAL );
    }

    strncpy( bfdmnparamsp->domainName, domain, BS_DOMAIN_NAME_SZ - 1 );

    strncpy( vdparamsp->vdName, vdName, BS_VD_NAME_SZ - 1 );
    vdparamsp->serviceClass = vdSvc;
    vdparamsp->vdSize       = vdSize;
    /* convert fobs to metadata pages.  We round up to the next whole page */
    vdparamsp->bmtXtntPgs   = howmany(bmtXtntFobs, ADVFS_METADATA_PGSZ_IN_FOBS);
    vdparamsp->vdIndex      = 1;
#ifndef TEMPORARY_FOR_ETEST
    vdparamsp->userAllocSz  = userAllocFobs;
#else
    vdparamsp->userAllocSz  = 1;
#endif
    vdparamsp->metaAllocSz  = ADVFS_METADATA_PGSZ_IN_FOBS;

    /*
     * If the caller didn't specify what version domain he wanted,
     * use the BFD_ODS_LAST_VERSION default.  Otherwise, if he did specify,
     * and the version requested is not supported by the running version,
     * return an error.
     */
    /* Minor version number does not need to be checked here as such
     * changes must be forward & backward compatible.
     */
    if (domainVersion.odv_major == 0) { 
        domainVersion = BFD_ODS_LAST_VERSION;
    } else if ((domainVersion.odv_major < FIRST_VALID_FS_VERSION_MAJOR) ||
               (domainVersion.odv_major > LAST_VALID_FS_VERSION_MAJOR)) {
        RAISE_EXCEPTION(E_INVALID_FS_VERSION);
    }

    if (advfs_volume_mounted(vdName)) {
        sts = EDUP_VD;
        goto HANDLE_EXCEPTION;
    }

    sts = bs_disk_init( vdName,
                        bfdmnparamsp,
                        vdparamsp,
                        tagSvc,
                        logSvc,
                        bmtPreallocFobs,
                        domainVersion );
    if ( sts != EOK ) {
        goto HANDLE_EXCEPTION;
    }

    *domainId = bfdmnparamsp->bfDomainId;

    ms_free( vdparamsp );
    ms_free( bfdmnparamsp );

    return EOK;

HANDLE_EXCEPTION:

    if ( vdparamsp != NULL ) {
        ms_free( vdparamsp );
    }
    if ( bfdmnparamsp != NULL ) {
        ms_free( bfdmnparamsp );
    }

    return sts;
}

/*
 *
 * bs_disk_init
 *
 * Initialize the specified disk partition as a ADVFS Virtual Disk
 *
 * This function will deal with real disk blocks (DEV_BSIZE) rather than
 * fobs to make sure that alignment is correct on disk blocks.
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
           bf_fob_t bmtPreallocFobs,    /* in - number of fobs to be
                                         * preallocated for the BMT
                                         */
           adv_ondisk_version_t  dmnVersion   /* in - store version on-disk */
           )
{
    struct bsMPg *rbmtpg = NULL;
    struct bsMPg *bmtpg = NULL;
    struct bsMPg *srbmtpg = NULL;
    statusT sts;
    int fd, err, i, s, devOpen = FALSE;
    struct bsXtntR* xtntp;
    struct bsXtraXtntR* xtntp1;
    bsBfAttrT* atrp;
    struct bsVdAttr* vdattr;
    bfTagT ftxLogTag, bfSetDirTag;
    bsDmnAttrT *dmnAttrp;
    advfs_fake_superblock_t *superBlockPg = NULL;
    struct fs *realSuperBlock = NULL;
    bf_vd_blk_t minBlks = 0, freeBlks = 0;
    bf_vd_blk_t rbmtBlks, bmtBlks, preallocBlks, tagBlks, bootBlks;
    bf_vd_blk_t mscFirstBlk, rbmtFirstBlk, bmtFirstBlk, tagFirstBlk, sbmFirstBlk;
    bf_vd_blk_t bootFirstBlk, bmtFirstPreallocBlk; 
    bs_meta_page_t sbmPgs;
    bf_vd_blk_t sbmBlks, vdBlkCnt;
    uint32_t bmtPreallocPgs;
    struct bsMR *rp;
    vdCookieT vdCookie; 
    struct timeval ltime;
    bf_vd_blk_t shadowBlks, shadowFirstBlk, shadowSecondBlk;
    bs_meta_page_t rbmtPage = 0;
    bsDmnMAttrT      *dmnMAttrp;
    bsDmnTAttrT      *dmnTAttrp;
    bsDmnFreezeAttrT *dmnFreezeAttrp;
    bsDmnNameT    *dmnNamep;
    bsRunTimesRT  *runTimesp;
    bsDmnUmntThawT* dmnUmntThawp;
    int32_t cell;

    int error;
    struct vnode *vp = NULL;
    
    MS_SMP_ASSERT(bsVdParams->vdIndex > 0);

    /*
     * Minimum supported disk size is 8mb - this may change later
     */
    if (bsVdParams->vdSize < ADVFS_MIN_DISK_SIZE) {
        RAISE_EXCEPTION( E_DISK_TOO_SMALL );
    }
    
    /* "Create" the virtual disk */

    /* get device's vnode */

    if( error = advfs_getvp( &vp, diskName, UIOSEG_KERNEL, FOLLOW_LINK ) ) {
        RAISE_EXCEPTION( (statusT)error );
    }

    error = VOP_ACCESS( vp, VREAD | VWRITE, TNC_CRED() );
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

    error = VOP_OPEN( &vp, FREAD | FWRITE, TNC_CRED() );
    if( error ) {
        RAISE_EXCEPTION( (statusT)error );
    }
    devOpen = TRUE;

    if( vp->v_type != VBLK && vp->v_type != VCHR ) {
        RAISE_EXCEPTION( E_BAD_DEV );
    }

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
    *                            Also includes vdattr record so kernel
    *                            can determine page size in support
    *                            of variable page sizes.
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
    *    cell 5 (MISC) 
    *                         == Misc bitfile - contains things like:
    *                            Unused blocks 0 to 7 so we don't overwrite
    *                            the disk label and boot blocks.
    *                            A fake super block (blks 8 to 15).
    *                            Cluster Private Data Area (blks 32 to 159)
    *                            so we won't overwrite.
    *                            Real boot blocks blks (160 to 175)
    *    cell 6 (RBMT_EXT)
    *                         == RBMT extension mcell - contains
    *                            domain attributes.
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
    * - MISC bitfile occupies:
    *     blks   0 -  15 (pages 0 and 1) for fake SB 
    *     blks  32 - 159 (pages 2 thru 17) for Cluster Private Data Area
    *     blks 160 - 175 (pages 18 and 19) for boot blocks
    *
    * - RBMT bitfile occupies blks 16 - 23 (page 0).
    *   
    * - BMT bitfile occupies blocks 24 - 31.  Any additional BMT blocks are 
    *     allocated near the middle of the volume.
    *
    * - LOG bitfile occupies no blks (they are allocated and initialized
    *     the first time the log is opened; see lgr_open()).  These blocks
    *     will be near the middle of the volume.
    *
    * - BFSDIR bitfile occupies blks 176 - 183 (page 0) if the disk being
    *     initialized is the BFSDIR's disk.  Otherwise the BFSDIR occupies
    *     no space (it's pages are allocated when needed in tagdir_alloc_tag().
    *
    * - SBM bitfile occupies blocks near the middle of the volume (the number 
    *     of blks used is determined by the size of the disk; see init_sbm().
    *
    * - BMT prealloc blocks are allocated after the SBM bitfile (the number
    *   of blocks is specified by the input parameter bmtPreallocPgs).
    */

    BS_BFTAG_RSVD_INIT( ftxLogTag, bsVdParams->vdIndex, BFM_FTXLOG );
    BS_BFTAG_RSVD_INIT( bfSetDirTag, bsVdParams->vdIndex, BFM_BFSDIR );


    /* 
     * Assert that the number of fobs we want for the reserved files is a
     * multiple of the DEV_BSIZE so we get exactly what we want.
     */
    MS_SMP_ASSERT( ((ADVFS_INIT_RBMT_FOBS * ADVFS_FOB_SZ) % DEV_BSIZE) == 0 &&
                   ((ADVFS_INIT_BMT_FOBS * ADVFS_FOB_SZ) % DEV_BSIZE) == 0 &&
                   ((ADVFS_INIT_TAGDIR_FOBS * ADVFS_FOB_SZ) % DEV_BSIZE) == 0 &&
                   ((ADVFS_INIT_BOOT_BLKS * ADVFS_FOB_SZ) % DEV_BSIZE) == 0 );
    /*
     * Get the number of DEV_BSIZE blocks required for the rbmt and bmt.
     */
    rbmtBlks = roundup( ADVFS_INIT_RBMT_FOBS / ADVFS_FOBS_PER_DEV_BSIZE, ADVFS_BS_CLUSTSIZE);
    bmtBlks = roundup( ADVFS_INIT_BMT_FOBS / ADVFS_FOBS_PER_DEV_BSIZE, ADVFS_BS_CLUSTSIZE);

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        /* this is the tagdir's disk so allocate and initialize page 0 */
        tagBlks = roundup( ADVFS_INIT_TAGDIR_FOBS / ADVFS_FOBS_PER_DEV_BSIZE, ADVFS_BS_CLUSTSIZE);
    } else {
        /* this is not the tagdir's disk so don't allocate any pages to it */
        tagBlks = 0;
    }

    bootBlks = ADVFS_INIT_BOOT_BLKS;

    rbmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );
                                                /* start and end 1K blocks */
    mscFirstBlk = 0;                            /*   0 -  15 */
    rbmtFirstBlk = RBMT_BLK_LOC;                /*  16 -  23 */
    bmtFirstBlk = rbmtFirstBlk + rbmtBlks;      /*  24 -  31 */
    bootFirstBlk = ADVFS_RESERVED_BLKS +        /* 160 - 175 */
                   CPDA_RESERVED_BLKS + 16; 
    tagFirstBlk = bootFirstBlk + bootBlks;      /* 176 - 183 (optionally) */

    if ((vdBlkCnt = bsVdParams->vdSize) == 0) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    metaPhysLocPct = MIN(metaPhysLocPct, 99);
    sbmFirstBlk = roundup((vdBlkCnt / 100) * metaPhysLocPct, ADVFS_BS_CLUSTSIZE);
    sbmFirstBlk = MAX(sbmFirstBlk, tagFirstBlk + tagBlks);
    sbmPgs = howmany( vdBlkCnt / ADVFS_BS_CLUSTSIZE, SBM_BITS_PG);
    sbmBlks = roundup( (ADVFS_METADATA_PGSZ_IN_FOBS * sbmPgs) /
                         ADVFS_FOBS_PER_DEV_BSIZE, 
                        ADVFS_BS_CLUSTSIZE );

    /* 
     * Generate disk locations for 2 shadow RBMT stubs. The stubs will be
     * zero filled and their extent maps will be included within the MISC
     * file. This reserves the disk blocks until full shadow RBMT support
     * is provided. 
     *
     * The first shadow RBMT must be stored at a location that is a power
     * of 2 (8k units). Using a power of 2 with 8k sized units provides 
     * AdvFS tools & utilties with a simple way to quicky scan a device for
     * shadow RBMTs.
     *  NOTE: For stub purposes only, the location will be fixed at lbn 2048.
     * 
     * The second shadow RBMT is stored at the end of the filesystem.
     */

    shadowBlks = roundup(ADVFS_METADATA_PGSZ_IN_FOBS / ADVFS_FOBS_PER_DEV_BSIZE,
                         ADVFS_BS_CLUSTSIZE);
    shadowFirstBlk  = 2048;
    shadowSecondBlk = RBMT_END_SHADOW(vdBlkCnt);
    
    /* 
     * Generate a volume cookie to uniquely identify RBMT/BMT pages for
     * a specific volume.  All valid RBMT/BMT pages for a specific device
     * within a filesystem will have matching volume cookies. This will
     * help fsck and salvage distinguish between valid RBMT/BMT pages
     * and stale pages that may still be on the device as a result of
     * addvol and rmvol activity.
     */
    ltime = get_system_time();
    vdCookie = ltime.tv_sec;
    bmt_init_page( rbmtpg, rbmtPage, bsVdParams->vdIndex, BFM_RBMT, dmnVersion,
            bfDmnParams->dmnCookie, vdCookie );


    /********************************************************************
     * RBMT
     ********************************************************************/

    /*
     * Initialize reserved bitfile metadata. Cell 0 is for bitfile 0
     * bfattr and primary extent record.
     */

    rbmtpg->bsMCA[ BFM_RBMT ].mcLinkSegment = 0;
    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_RBMT ].mcTag,
                        rbmtpg->bmtVdIndex,
                        BFM_RBMT );
    rbmtpg->bsMCA[ BFM_RBMT ].mcBfSetTag = staticRootTagDirTag;

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_RBMT ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;
    atrp->state = BSRA_VALID;

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

    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;
    xtntp->xCnt           = 2;
    xtntp->bsXA[0].bsx_fob_offset = 0;
    xtntp->bsXA[0].bsx_vd_blk  = rbmtFirstBlk;
    xtntp->bsXA[1].bsx_fob_offset = ADVFS_INIT_RBMT_FOBS; 
    xtntp->bsXA[1].bsx_vd_blk  = -1;
    xtntp->mcellCnt       = 1;

    /*
     * Set the vd attr record for this disk - moved to first RBMT
     * mcell (page 0, cell 0) to easily determine page size with
     * variable page size support.
     */

    vdattr = bmtr_assign(BSR_VD_ATTR,
                        sizeof(struct bsVdAttr),
                        &rbmtpg->bsMCA[BFM_RBMT]);
    if (vdattr == 0) {
        ms_uprintf( "bs_disk_init: can't create vd attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    vdattr->state = BSR_VD_VIRGIN;
    vdattr->userAllocSz = bsVdParams->userAllocSz; 
    vdattr->metaAllocSz = bsVdParams->metaAllocSz;

    MS_SMP_ASSERT((vdattr->userAllocSz == 1) || (vdattr->userAllocSz == 2) ||
                  (vdattr->userAllocSz == 4) || (vdattr->userAllocSz == 8));
    MS_SMP_ASSERT(vdattr->metaAllocSz == ADVFS_METADATA_PGSZ_IN_FOBS);


    /* copy fields from the supplied vd params */

    if ( !( vdattr->vdIndex = rbmtpg->bmtVdIndex )) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    }

    vdattr->vdCookie = vdCookie; 
    vdattr->serviceClass = bsVdParams->serviceClass;
    if (SC_EQL( vdattr->serviceClass, nilServiceClass )) {
        /* TEMP - need real "default service class" from storage mgt? */
        vdattr->serviceClass = defServiceClass;
    }

    vdattr->vdBlkCnt = vdBlkCnt;
    vdattr->sbmBlksBit = ADVFS_BS_CLUSTSIZE;

    if ((vdattr->bmtXtntPgs = bsVdParams->bmtXtntPgs ) <
            BS_BMT_XPND_PGS) {
        vdattr->bmtXtntPgs= BS_BMT_XPND_PGS;
    }
     
    vdattr->blkSz = DEV_BSIZE;

    /*
     * Set the domain unmount_thaw rercord for this domain/disk. This
     * record will stored on all disks to assist scan when recreating
     * split-mirror filesystems.
     */

    dmnUmntThawp = bmtr_assign(BSR_DMN_UMT_THAW_TIME,
                               sizeof(struct bsDmnUmntThaw),
                               &rbmtpg->bsMCA[BFM_RBMT]);

    if (dmnUmntThawp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain unmount_thaw record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    dmnUmntThawp->dmnUmntThawTime.id_sec  = 0;
    dmnUmntThawp->dmnUmntThawTime.id_usec = 0; 


    /********************************************************************
     * RBMT EXTENSION
     ********************************************************************/

    /*
     * Link an extension cell into this right away for the domain records. 
     * This extension cell will immediately follow the reserved primary mcells.
     */

    rbmtpg->bsMCA[ BFM_RBMT ].mcNextMCId.cell = BFM_RBMT_EXT;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcLinkSegment = 1;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcTag = rbmtpg->bsMCA[ BFM_RBMT ].mcTag;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcBfSetTag = rbmtpg->bsMCA[ BFM_RBMT ].mcBfSetTag;


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
    dmnAttrp->MasterFsCookie = bfDmnParams->dmnCookie; 
    dmnAttrp->bfSetDirTag = staticRootTagDirTag;
    dmnAttrp->maxVds      = bfDmnParams->maxVds;
    dmnAttrp->rsvd1       = 0;
    dmnAttrp->rsvd2       = 0;

    /* Set up the following records on all devices to ensure available space
     * on RBMT page0.cell[BFM_RBMT_EXT].  A bug fix that allows new RBMT
     * mcells to be created/chained after initialization can consume space
     * in cell[BFM_RBMT_EXT] on non-log volumes.  Subsequent calls to 
     * lgr_switch_vol() could place these records in a chained mcell on RBMT
     * page0 or on another RBMT page. This presents a problem for tools and
     * utilities searching for these records at a known location. Not all
     * tools and utilities can follow mcell/page chains. Ensuring these
     * records are always on RBMT page0 also simplifies Shadow RBMT support.
     * 
     * NOTE: ONLY the device that contains the log file will have valid
     *       versions of these records. A non-zero value in the seqNum
     *       of the bsDmnMAttr record indicates the log device.  All other
     *       devices will have stale or unused versions of these records.
     *       A zero value in the seqNum indicates a device with no log file.
     *       This allows the current algorithm (max seqNum) to continue
     *       working in the kernel and in fsck with no changes.
     */

    dmnMAttrp = bmtr_assign( BSR_DMN_MATTR,
                             sizeof(bsDmnMAttrT),
                             &rbmtpg->bsMCA[BFM_RBMT_EXT] );
    if (dmnMAttrp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain log attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    dmnMAttrp->ODSVersion = dmnVersion;
    /*
     * ONLY the device with the log file has valid versions of these
     * records.
     */
    if (BS_BFTAG_EQL( bfDmnParams->ftxLogTag, ftxLogTag )) {    
        dmnMAttrp->seqNum = 1;
    } else {
        dmnMAttrp->seqNum = 0;
    }
    dmnMAttrp->delPendingBfSet = NilBfTag;
    dmnMAttrp->ftxLogTag = bfDmnParams->ftxLogTag;
    dmnMAttrp->ftxLogPgs = bfDmnParams->ftxLogFobs 
                              / ADVFS_METADATA_PGSZ_IN_FOBS;
    dmnMAttrp->bfSetDirTag = bfDmnParams->bfSetDirTag;
    dmnMAttrp->vdCnt = 1;
    dmnMAttrp->bs_dma_dmn_state = 0;
    dmnMAttrp->bs_dma_forced_mnt.tv_sec = 0;
    dmnMAttrp->bs_dma_forced_mnt.tv_usec = 0;
    dmnMAttrp->mode = S_IRWXU | S_IRGRP | S_IROTH;
    dmnMAttrp->uid = TNC_CRED()->cr_uid;
    dmnMAttrp->gid = TNC_CRED()->cr_gid;

    bzero( &dmnMAttrp->dmnThreshold, sizeof(adv_threshold_ods_t));
 
    dmnMAttrp->rsvd1 = 0;
    dmnMAttrp->rsvd2 = 0;

    dmnTAttrp = bmtr_assign( BSR_DMN_TRANS_ATTR,
                             sizeof(bsDmnTAttrT),
                             &rbmtpg->bsMCA[BFM_RBMT_EXT] );

    if (dmnTAttrp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain trans attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    dmnTAttrp->chainMCId = bsNilMCId;
    dmnTAttrp->chainMCId.volume = VOL_TERM;
        
    dmnTAttrp->op = BSR_VD_NO_OP;
    dmnTAttrp->dev = 0;

    dmnFreezeAttrp = bmtr_assign( BSR_DMN_FREEZE_ATTR,
                                sizeof(bsDmnFreezeAttrT),
                                &rbmtpg->bsMCA[BFM_RBMT_EXT] );
    if (dmnFreezeAttrp == 0) {
        ms_uprintf( "bs_disk_init: can't create domain freeze attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero ((char*)dmnFreezeAttrp, sizeof( bsDmnFreezeAttrT ));

    /*    
     * Create BSR_DMN_NAME (bsDmnNameT) record.  
     *
     * The BSR_DMN_NAME record is created for all RBMTs at initialization
     * time to ensure the record resides on RBMT page0 in a fixed location
     * (BFM_DMN_NAME). See previous comments on cell[BFM_RBMT_EXT]. 
     *
     * NOTE: The BSR_DMN_NAME record will be stored on all disks to assist
     *       tools such as scan when searching RBMT information to verify
     *       or rebuild domains.  The BSR_DMN_NAME record is a hint, not the
     *       unique ID.  The bsDmnAttr.bfDomainId is still the unique ID for
     *       a domain.
     */
        
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcNextMCId.volume = rbmtpg->bmtVdIndex;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcNextMCId.page   = rbmtpg->bmtPageId;
    rbmtpg->bsMCA[BFM_RBMT_EXT].mcNextMCId.cell   = BFM_DMN_NAME;
    
    rbmtpg->bsMCA[BFM_DMN_NAME].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_DMN_NAME].mcLinkSegment =
        rbmtpg->bsMCA[BFM_RBMT_EXT].mcLinkSegment + 1;
    rbmtpg->bsMCA[BFM_DMN_NAME].mcTag = rbmtpg->bsMCA[BFM_RBMT_EXT].mcTag;
    rbmtpg->bsMCA[BFM_DMN_NAME].mcBfSetTag =
        rbmtpg->bsMCA[BFM_RBMT_EXT].mcBfSetTag;
    
    dmnNamep =  bmtr_assign( BSR_DMN_NAME,
                             sizeof(bsDmnNameT),
                             &rbmtpg->bsMCA[BFM_DMN_NAME] );
    if (dmnNamep == 0) {
        ms_uprintf( "bs_disk_init: can't create storage domain name record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    strncpy(dmnNamep->dmn_name, bfDmnParams->domainName, BS_DOMAIN_NAME_SZ-1);

    /*
     *
     * Create BSR_RUN_TIMES (bsRunTimesRT) record.  
     *
     * The BSR_RUN_TIMES record is created for all RBMTs at initialization
     * time to ensure the record resides on RBMT page0 in a fixed location
     * (BFM_RUN_TIMES). See previous comments on cell[BFM_RBMT_EXT]. 
     *
     * NOTE: ONLY the device that contains the log file will have a
     *       valid BSR_RUN_TIMES record. See previous comments on
     *       cell[BFM_RBMT_EXT].
     */

    rbmtpg->bsMCA[BFM_DMN_NAME].mcNextMCId.volume = rbmtpg->bmtVdIndex;
    rbmtpg->bsMCA[BFM_DMN_NAME].mcNextMCId.page   = rbmtpg->bmtPageId;
    rbmtpg->bsMCA[BFM_DMN_NAME].mcNextMCId.cell   = BFM_RUN_TIMES;

    rbmtpg->bsMCA[BFM_RUN_TIMES].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_RUN_TIMES].mcLinkSegment =
        rbmtpg->bsMCA[BFM_DMN_NAME].mcLinkSegment + 1;
    rbmtpg->bsMCA[BFM_RUN_TIMES].mcTag = rbmtpg->bsMCA[BFM_DMN_NAME].mcTag;
    rbmtpg->bsMCA[BFM_RUN_TIMES].mcBfSetTag =
        rbmtpg->bsMCA[BFM_DMN_NAME].mcBfSetTag;

    runTimesp = bmtr_assign( BSR_RUN_TIMES,
                             sizeof(bsRunTimesRT),
                             &rbmtpg->bsMCA[BFM_RUN_TIMES] );

    if (runTimesp == 0) {
        ms_uprintf( "bs_disk_init: can't create utility run time record");
    }

    bzero ((char*)runTimesp, sizeof( bsRunTimesRT ));

    /*
     * Remember the rbmt's first free mcell for reserved bitfiles.
     */

    rbmtpg->bmtNextFreeMcell = BFM_RSVD_CELLS;
    rbmtpg->bmtFreeMcellCnt = rbmtpg->bmtFreeMcellCnt - BFM_RSVD_CELLS;

    /********************************************************************
     * RBMT RESERVED CELL
     ********************************************************************/

    /*
     * Initialize the last mcell on rbmtpg for future extensions of the
     * RBMT.
     */

    rbmtpg->bsMCA[RBMT_RSVD_CELL].mcLinkSegment = 1;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].mcTag =
        rbmtpg->bsMCA[ BFM_RBMT ].mcTag;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].mcBfSetTag =
        rbmtpg->bsMCA[ BFM_RBMT ].mcBfSetTag;
    rbmtpg->bsMCA[RBMT_RSVD_CELL].mcNextMCId = bsNilMCId;

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

    rbmtpg->bsMCA[ BFM_BMT ].mcLinkSegment = 0;
    rbmtpg->bsMCA[ BFM_BMT ].mcNextMCId = bsNilMCId;

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_BMT ].mcTag,
                        rbmtpg->bmtVdIndex,
                        BFM_BMT );
    rbmtpg->bsMCA[ BFM_BMT ].mcBfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the BMT bf */
    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_BMT ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create BMT attr record" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;
    atrp->state = BSRA_VALID;

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

    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;
    xtntp->xCnt           = 2;
    xtntp->bsXA[0].bsx_fob_offset = 0;
    xtntp->bsXA[0].bsx_vd_blk  = bmtFirstBlk;
    xtntp->bsXA[1].bsx_fob_offset = ADVFS_INIT_BMT_FOBS;
    xtntp->bsXA[1].bsx_vd_blk  = -1;
    xtntp->mcellCnt       = 1;

    /********************************************************************
     * BMT PREALLOCATION
     ********************************************************************/

    /*
     * Set up BFM_RSVD_CELLS mcell for a BMT extent and pre-allocate the 
     * specified amount.  Always pre-allocate at least one page which
     * will be physically located metaPhysLocPct into the volume,
     */

    bmtPreallocFobs = MAX(bmtPreallocFobs, ADVFS_METADATA_PGSZ_IN_FOBS);
    bmtPreallocPgs = (bmtPreallocFobs + 
            ADVFS_METADATA_PGSZ_IN_FOBS - 1)/ADVFS_METADATA_PGSZ_IN_FOBS;
    bmtPreallocPgs = MAX(bmtPreallocPgs, 1);

    /* Calculate the starting block for the pre-allocation range */

    bmtFirstPreallocBlk = sbmFirstBlk + sbmBlks;

    /* Initialize BFM_RSVD_CELLS mcell to belong to BMT */

    rbmtpg->bsMCA[BFM_RSVD_CELLS].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].mcLinkSegment = 1;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].mcTag = rbmtpg->bsMCA[ BFM_BMT ].mcTag;
    rbmtpg->bsMCA[BFM_RSVD_CELLS].mcBfSetTag = rbmtpg->bsMCA[ BFM_BMT ].mcBfSetTag;

    /* Update free list and free count */
    rbmtpg->bmtNextFreeMcell += 1;
    rbmtpg->bmtFreeMcellCnt -= 1;

    /* Assign and set up a new extent record for preallocation pages */
    /* These pages will be allocated after the SBM pages */

    xtntp1 = bmtr_assign( BSR_XTRA_XTNTS,
                          sizeof(struct bsXtraXtntR),
                          &rbmtpg->bsMCA[BFM_RSVD_CELLS] );

    if (xtntp1 == 0) {
        ms_uprintf( "bs_disk_init: can't create extent record for BMT preallocation" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp1->xCnt            = 2;
    xtntp1->rsvd1           = 0;
    xtntp1->rsvd2           = 0;
    xtntp1->bsXA[0].bsx_fob_offset = ADVFS_INIT_BMT_FOBS;
    xtntp1->bsXA[0].bsx_vd_blk = bmtFirstPreallocBlk;
    xtntp1->bsXA[1].bsx_fob_offset = (ADVFS_INIT_BMT_FOBS + bmtPreallocFobs );
    xtntp1->bsXA[1].bsx_vd_blk = -1;

    /* Update the primary extent record of the BMT */

    xtntp->chainMCId.volume = rbmtpg->bmtVdIndex;
    xtntp->chainMCId.page   = rbmtpg->bmtPageId;
    xtntp->chainMCId.cell   = BFM_RSVD_CELLS;
    xtntp->mcellCnt++;

    preallocBlks = roundup( bmtPreallocFobs / ADVFS_FOBS_PER_DEV_BSIZE,
                                ADVFS_BS_CLUSTSIZE);


    /********************************************************************
     * STG BITMAP
     ********************************************************************/

    /*
     * Iinitialize the storage bitmap primary mcell.
     * Set no next cell, assign reserved tag derived from vdindex.
     */

    rbmtpg->bsMCA[ BFM_SBM ].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[ BFM_SBM ].mcLinkSegment = 0;

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_SBM ].mcTag,
                        rbmtpg->bmtVdIndex,
                        BFM_SBM );
    rbmtpg->bsMCA[ BFM_SBM ].mcBfSetTag = staticRootTagDirTag;

    /* Set the bf attr record for the sbm bf */

    atrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), &rbmtpg->bsMCA[ BFM_SBM ]);
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create stg map attr record\n");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;
    atrp->state = BSRA_VALID;

    /*
     * All the values passed to init_sbm are in DEV_BSIZE units, not fobs.
     */
    sts = init_sbm( vp,
                    rbmtpg,
                    vdattr,
                    bfDmnParams->dmnCookie,
                    rbmtBlks,
                    bmtBlks,
                    preallocBlks,
                    bootBlks,
                    tagBlks,
                    shadowBlks,
                    sbmFirstBlk,
                    shadowFirstBlk,
                    shadowSecondBlk,
                    &freeBlks );
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * minBlks is the minimum number of blocks that we need to have
     * a usable domain.
     */

    /* Find the minimum DEV_BSIZE blocks needed for the BMT + one BMT expansion, */
    /* the LOG, 2 TAG files + TAG expansion and ADVFS_INIT_USER_STG_FOBS for files. */
    minBlks = ( ( ( (BS_BMT_XPND_PGS * ADVFS_METADATA_PGSZ_IN_FOBS) +  
                    bmtPreallocFobs ) + 
                 (bfDmnParams->ftxLogFobs) +
                 ( (BS_TD_XPND_PGS + BS_TD_XPND_PGS) * 
                  ADVFS_METADATA_PGSZ_IN_FOBS ) + ADVFS_INIT_USER_STG_FOBS ) /
                 ADVFS_FOBS_PER_DEV_BSIZE );
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

    rbmtpg->bsMCA[ BFM_BFSDIR ].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[ BFM_BFSDIR ].mcLinkSegment = 0;

    /* set the synthesized tag for the tag dir reserved cell. */

    rbmtpg->bsMCA[ BFM_BFSDIR ].mcTag = bfSetDirTag;
    rbmtpg->bsMCA[ BFM_BFSDIR ].mcBfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the tag directory. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_BFSDIR ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create tag dir attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS; 
    atrp->state = BSRA_VALID;
    atrp->reqServices = tagSvc;

    /* init a null extent record for the tag directory. */

    xtntp = bmtr_assign(BSR_XTNTS,
                        sizeof(struct bsXtntR),
                        &rbmtpg->bsMCA[ BFM_BFSDIR ]);
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create tag dir extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;

    if (BS_BFTAG_EQL( bfDmnParams->bfSetDirTag, bfSetDirTag )) {
        xtntp->xCnt           = 2;
        xtntp->bsXA[0].bsx_fob_offset = 0;
        xtntp->bsXA[0].bsx_vd_blk  = tagFirstBlk;
        xtntp->bsXA[1].bsx_fob_offset = ADVFS_INIT_TAGDIR_FOBS;
        xtntp->bsXA[1].bsx_vd_blk  = -1;
    } else {
        xtntp->xCnt           = 1;
        xtntp->bsXA[0].bsx_fob_offset = 0;
        xtntp->bsXA[0].bsx_vd_blk  = -1;
    }

    xtntp->mcellCnt       = 1;

    /********************************************************************
     * LOG
     ********************************************************************/

    /*
     * Set no next cell for the reserved ftx log cell.  If this is the
     * selected vd for the ftx log, init the primary mcell for it now.
     */

    rbmtpg->bsMCA[ BFM_FTXLOG ].mcNextMCId = bsNilMCId;

    /*
     * Initialize the ftx log primary mcell.
     */

    /* set the synthesized tag for the ftx log reserved cell. */

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_FTXLOG ].mcTag,
                        rbmtpg->bmtVdIndex,
                        BFM_FTXLOG );

    rbmtpg->bsMCA[ BFM_FTXLOG ].mcBfSetTag = staticRootTagDirTag;

    /* set the bf attributes for the ftx log. */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_FTXLOG ] );
    if (atrp == 0) {
       ms_uprintf( "bs_disk_init: can't create ftx log attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;

    /*
     * The LOG state must be set to BSRA_INVALID until it is opened to avoid
     * a race. The log placeholders (mcell 3 in added volumes)
     * state must be set to BSRA_VALID now since they will not be opened.
     * logSvc will be nilServiceClass when adding volumes.
     */

    if ( logSvc ) {
        atrp->state = BSRA_INVALID;
    } else {
        atrp->state = BSRA_VALID;
    }
    atrp->reqServices = logSvc;

    /* init a null extent record for the ftx log. */

    xtntp = bmtr_assign( BSR_XTNTS,
                         sizeof(struct bsXtntR),
                         &rbmtpg->bsMCA[ BFM_FTXLOG ] );

    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create ftx log extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->chainMCId      = bsNilMCId;
    xtntp->xCnt           = 1;
    xtntp->bsXA[0].bsx_fob_offset = 0;
    xtntp->bsXA[0].bsx_vd_blk  = -1;
    xtntp->mcellCnt       = 1;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;

    /********************************************************************
     * MISC
     ********************************************************************/

    /*
     * Set no next cell for the reserved MISC BF cell.
     */

    rbmtpg->bsMCA[ BFM_MISC ].mcNextMCId = bsNilMCId;

    /*
     * Initialize the MISC BF primary mcell.
     */

    BS_BFTAG_RSVD_INIT( rbmtpg->bsMCA[ BFM_MISC ].mcTag,
                        rbmtpg->bmtVdIndex,
                        BFM_MISC);

    rbmtpg->bsMCA[ BFM_MISC ].mcBfSetTag = staticRootTagDirTag;

    /* set the bf attributes */

    atrp = bmtr_assign( BSR_ATTR,
                        sizeof(bsBfAttrT),
                        &rbmtpg->bsMCA[ BFM_MISC ] );
    if (atrp == 0) {
        ms_uprintf( "bs_disk_init: can't create misc bf attr record" );
       RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    bzero( atrp, sizeof( bsBfAttrT ) );
    atrp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;
    atrp->state = BSRA_VALID;
    atrp->reqServices = nilServiceClass;

    xtntp = bmtr_assign( BSR_XTNTS, sizeof(struct bsXtntR),
                         &rbmtpg->bsMCA[ BFM_MISC ] );
    if (xtntp == 0) {
        ms_uprintf( "bs_disk_init: can't create misc bf extents record");
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;

    /* Initialize to have two pages allocated at blocks 0 to 16 */
    /* Boot blocks are now allocated at blocks 160 thru 175 */


    xtntp->xCnt           = 2;
    xtntp->bsXA[0].bsx_fob_offset = 0;
    xtntp->bsXA[0].bsx_vd_blk  = mscFirstBlk;
    xtntp->bsXA[1].bsx_fob_offset = ADVFS_RESERVED_BLKS;   /* change to fobs */
    xtntp->bsXA[1].bsx_vd_blk  = -1;
    xtntp->mcellCnt       = 1;
 
    /*
     * remove old hack of having multiple extents defined in bsXtntRT
     * 
     * Initialize MISC extra extents mcell for bootBlks and Cluster 
     * Private Data Area. Define as own extent to allow CPDA to be
     * more easily expanded if ever necessary.
     */
    cell = rbmtpg->bmtNextFreeMcell;
    xtntp1 = bmtr_assign( BSR_XTRA_XTNTS,
                          sizeof(struct bsXtraXtntR),
                          &rbmtpg->bsMCA[cell] ); 
    if (xtntp1 == 0) { 
        ms_uprintf( "bs_disk_init: can't create extent record for MISC" );
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }
    
    xtntp1->xCnt  = 5;
    xtntp1->rsvd1 = 0;
    xtntp1->rsvd2 = 0;
    xtntp1->bsXA[0].bsx_fob_offset = ADVFS_RESERVED_BLKS;
    xtntp1->bsXA[0].bsx_vd_blk     = bootFirstBlk;
    xtntp1->bsXA[1].bsx_fob_offset = 32;
    xtntp1->bsXA[1].bsx_vd_blk     = CPDA_BLK_LOC; 
    xtntp1->bsXA[2].bsx_fob_offset = xtntp1->bsXA[1].bsx_fob_offset +
                                     CPDA_RESERVED_BLKS;
    xtntp1->bsXA[2].bsx_vd_blk     = shadowFirstBlk; 
    xtntp1->bsXA[3].bsx_fob_offset = xtntp1->bsXA[2].bsx_fob_offset +
                                     ADVFS_METADATA_PGSZ_IN_FOBS;
    xtntp1->bsXA[3].bsx_vd_blk     = shadowSecondBlk; 
    xtntp1->bsXA[4].bsx_fob_offset = xtntp1->bsXA[3].bsx_fob_offset +
                                     ADVFS_METADATA_PGSZ_IN_FOBS;
    xtntp1->bsXA[4].bsx_vd_blk     = -1;

    rbmtpg->bsMCA[cell].mcNextMCId = bsNilMCId;
    rbmtpg->bsMCA[cell].mcLinkSegment = 1;
    rbmtpg->bsMCA[cell].mcTag = rbmtpg->bsMCA[ BFM_MISC ].mcTag;
    rbmtpg->bsMCA[cell].mcBfSetTag = rbmtpg->bsMCA[ BFM_MISC ].mcBfSetTag;

    /* Update the primary extent record of MISC */
    xtntp->chainMCId.volume = rbmtpg->bmtVdIndex;
    xtntp->chainMCId.page   = rbmtpg->bmtPageId;
    xtntp->chainMCId.cell   = cell; 
    xtntp->mcellCnt++;

    /* Update free list and free count */
    rbmtpg->bmtNextFreeMcell += 1;
    rbmtpg->bmtFreeMcellCnt -= 1;


    /* Write the fake super block.  */

    superBlockPg =
        (advfs_fake_superblock_t *) ms_malloc(sizeof(advfs_fake_superblock_t));

    MS_SMP_ASSERT(sizeof(advfs_fake_superblock_t) == ADVFS_SUPER_BLOCK_SZ );
    ADVFS_SET_FS_MAGIC(superBlockPg);
    superBlockPg->adv_rbmt_offset_bytes = (RBMT_BLK_LOC * DEV_BSIZE); 
    superBlockPg->adv_ods_version = dmnVersion;
    superBlockPg->adv_multi_vol_flag = 0;
    superBlockPg->adv_cpda_offset_bytes = (CPDA_BLK_LOC * DEV_BSIZE);
    superBlockPg->adv_cpda_size_bytes = CPDA_SIZE_BYTES;

    /* set fs_* fields for mkboot support.  These are located
     * within the "padding" of the AdvFS_SuperBlockT structure, so we typecast
     * to a struct fs
     */
    realSuperBlock = (struct fs *)superBlockPg;
    realSuperBlock->fs_size = bsVdParams->vdSize * ADVFS_FOBS_PER_DEV_BSIZE;
    realSuperBlock->fs_dsize = bsVdParams->vdSize * ADVFS_FOBS_PER_DEV_BSIZE;
    realSuperBlock->fs_bsize = bsVdParams->userAllocSz * ADVFS_FOB_SZ;
    realSuperBlock->fs_fsize = ADVFS_FOB_SZ;
    
    /*
     * Write the super block via raw io interface.
     */
    err = advfs_raw_io( vp,
                        ADVFS_FAKE_SB_BLK,
                        ADVFS_SUPER_BLOCK_SZ / DEV_BSIZE,
                        RAW_WRITE,
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
     * tagdir page 0.  We use the hard coded value of
     * ADVFS_METADATA_PGSZ_IN_FOBS and convert DEV_BSIZE blocks, but this
     * should be dynamic in the future (based on domain or vd specific
     * parameters that cause this volume to be initialized.)
     */
    err = advfs_raw_io( vp,
                        RBMT_BLK_LOC,
                        ADVFS_METADATA_PGSZ_IN_FOBS /
                         ADVFS_FOBS_PER_DEV_BSIZE,
                        RAW_WRITE,
                        rbmtpg);
    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write rbmt page 0; %d\n", err);
        RAISE_EXCEPTION( err );
    }

    ms_free( rbmtpg );
    rbmtpg = NULL;

    bmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );

    bmt_init_page( bmtpg, rbmtPage, bsVdParams->vdIndex,
            BFM_BMT, BFD_ODS_NULL_VERSION,
            bfDmnParams->dmnCookie, vdCookie );
    if (!bmt_init_mcell_free_list( bmtpg )) {
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }
    del_init_mcell_list( bmtpg );

    if (bmtPreallocFobs) {
        bmtpg->bmtNextFreePg = ADVFS_INIT_BMT_FOBS / ADVFS_METADATA_PGSZ_IN_FOBS;
    } else {
        bmtpg->bmtNextFreePg = BMT_PG_TERM;
    }

    /*
     * Initialize a page of the bmt.  This should be dynamic as the rbmt
     * above should be.
     */
    err = advfs_raw_io( vp,
                        bmtFirstBlk,
                        ADVFS_METADATA_PGSZ_IN_FOBS /
                         ADVFS_FOBS_PER_DEV_BSIZE,
                        RAW_WRITE,
                        bmtpg );

    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write bmt page 0; %d\n", err);
        RAISE_EXCEPTION( err );
    }

    if (bmtPreallocFobs) {
        bs_meta_page_t pg, newpg = ADVFS_INIT_BMT_FOBS / ADVFS_METADATA_PGSZ_IN_FOBS;
        bs_meta_page_t bmt_pages_to_init = bmtPreallocFobs / ADVFS_METADATA_PGSZ_IN_FOBS;
        bf_vd_blk_t startBlk=bmtFirstPreallocBlk;

        for (pg=0; pg < bmt_pages_to_init; pg++) {
            /*
             * Init in mem page 
             */
            bmt_init_page(bmtpg, newpg, bsVdParams->vdIndex, BFM_BMT,
                    BFD_ODS_NULL_VERSION, bfDmnParams->dmnCookie,
                    vdCookie );
            if (pg < (bmtPreallocPgs - 1)) {
                bmtpg->bmtNextFreePg = ++newpg;
            } else {
                bmtpg->bmtNextFreePg = BMT_PG_TERM;
            }
            /*
             * Write a particular page of the bmt.  This should dynamically
             * get the page size as above.
             */
            err = advfs_raw_io( vp,
                                startBlk,
                                ADVFS_METADATA_PGSZ_IN_FOBS /
                                 ADVFS_FOBS_PER_DEV_BSIZE,
                                RAW_WRITE,
                                bmtpg );
            if ( err != 0) {
                ms_uprintf( "bs_disk_init: can't write raw bmt page %d, block %d, status %d\n", pg+1, startBlk, err);
                RAISE_EXCEPTION( err );
            }
            startBlk += ADVFS_METADATA_PGSZ_IN_FOBS / ADVFS_FOBS_PER_DEV_BSIZE;
        }
    }

    ms_free( bmtpg );
    bmtpg = NULL;

    if (tagBlks > 0) {
        bsTDirPgT *tdpgp = (bsTDirPgT*) ms_malloc( sizeof(bsTDirPgT) );

        tagdir_init_pg0( tdpgp, bfDmnParams->dmnCookie );

        /* 
         * Do a raw write to the tag dir pages to initialize them.
         */
        err = advfs_raw_io( vp,
                            tagFirstBlk,
                            ADVFS_METADATA_PGSZ_IN_FOBS /
                             ADVFS_FOBS_PER_DEV_BSIZE,
                            RAW_WRITE,
                            tdpgp );

        if (err != 0) {
            ms_uprintf( "bs_disk_init: can't write root tagdir page 0 starting at disk block number %d. status was %d\n", tagFirstBlk, err);
            RAISE_EXCEPTION( err );
        }
        ms_free( tdpgp );
    }

    /*
     * TODO: remove/replace shadow rbmt stubs once full shadow rbmt support
     *       is provided
     */

    srbmtpg = (struct bsMPg *) ms_malloc( sizeof( struct bsMPg ) );
    
    err = advfs_raw_io( vp,
                        shadowFirstBlk,
                        ADVFS_METADATA_PGSZ_IN_FOBS /
                         ADVFS_FOBS_PER_DEV_BSIZE,
                        RAW_WRITE,
                        srbmtpg);

    if (err != 0) {
         ms_uprintf( "bs_disk_init: can't write first shadow rbmt at lbn = %d; err = %d\n", shadowFirstBlk, err);
         RAISE_EXCEPTION( err );
     }

    err = advfs_raw_io( vp,
                        shadowSecondBlk,
                        ADVFS_METADATA_PGSZ_IN_FOBS /
                         ADVFS_FOBS_PER_DEV_BSIZE,
                        RAW_WRITE,
                        srbmtpg);

    if (err != 0) {
        ms_uprintf( "bs_disk_init: can't write second shadow rbmt at lbn = %d; err = %d\n", shadowSecondBlk, err);
        RAISE_EXCEPTION( err );
    }

    ms_free( srbmtpg );
    srbmtpg = NULL;

    i = VOP_CLOSE( vp, FREAD | FWRITE, TNC_CRED() );
    VN_RELE(vp);

    return err;

HANDLE_EXCEPTION:

    if (devOpen) {
    i = VOP_CLOSE( vp, FREAD | FWRITE, TNC_CRED() );
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

    if (srbmtpg != NULL) {
        ms_free( srbmtpg );
    }
    
    return sts;

}


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
    mcp->mcNextMCId = bsNilMCId;
    bmtpgp->bmtNextFreeMcell = 1;
    bmtpgp->bmtFreeMcellCnt--;

    /*** create the mcell free list record ***/
    mcellFreeListp = bmtr_assign( BSR_MCELL_FREE_LIST,
                                  sizeof( bsMcellFreeListT ),
                                  mcp );
    if (mcellFreeListp == NULL) {
        ms_uprintf("bmt_init_mcell_free_list: could not assign mcell record");
        return 0;
    }

    /*** mcell free list starts out empty ***/
    mcellFreeListp->headPg = bmtpgp->bmtPageId;

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
    bsTDirPgT *tdpgp,
    bfFsCookieT dmnCookie
    )
{
    int i;

    bzero( (char *) tdpgp, sizeof( bsTDirPgHdrT ) );
    tdpgp->tpMagicNumber = RTTAG_MAGIC;
    tdpgp->tpFsCookie = dmnCookie;
    tdpgp->tpCurrPage = 0;

    for (i = 0; i < BS_TD_TAGS_PG; i++) {
        tdpgp->tMapA[ i ].tmFlags = 0;
        tdpgp->tMapA[ i ].tmSeqNo = 1;
        tdpgp->tMapA[ i ].tmNextMap = i + 2; 
    }

    tdpgp->tMapA[ BS_TD_TAGS_PG - 1 ].tmNextMap = 0;

    /*
     * Page 0 is special in that the first tagmap struct is used
     * to store some metadata information; the free page list header.
     */
    tdpgp->tMapA[ 0 ].tmFreeListHead = 1;
    tdpgp->tMapA[ 0 ].tmUninitPg = 1;

    tdpgp->tpNextFreeMap = 2;
}

/*
 * init_sbm
 *
 * Sets SBM bits for the disk blocks occupied by the reserved section,
 * the initial part of the BMT, TAGDIR, and the SBM itself.
 */


static statusT
init_sbm(
    struct vnode *vp,               /* in */
    struct bsMPg *bmtpg,            /* in */
    struct bsVdAttr *vdattr,        /* in */
    bfFsCookieT dmnCookie,          /* in */
    bf_vd_blk_t rbmtBlks,           /* in */
    bf_vd_blk_t bmtBlks,            /* in */
    bf_vd_blk_t preallocBlks,       /* in */
    bf_vd_blk_t bootBlks,           /* in */
    bf_vd_blk_t tagBlks,            /* in */
    bf_vd_blk_t shadowBlks,         /* in */
    bf_vd_blk_t sbmFirstBlk,        /* in */
    bf_vd_blk_t shadowFirstBlk,     /* in */
    bf_vd_blk_t shadowSecondBlk,    /* in */
    bf_vd_blk_t *freeBlks           /* out */
    )
{
    /*
     * Initialize the bitmap bits.
     */

    bs_meta_page_t sbmPgs;
    bf_vd_blk_t sbmBlks;
    bs_meta_page_t sbmFirstPage;
    bf_vd_blk_t sbmFirstBit;
    bf_vd_blk_t first_setBlks, second_setBlks = 0;
    uint32_t set_bits, first_bits_to_set, second_bits_to_set = 0; 
    int i, j, err;
    statusT sts;
    struct bsXtntR* xtntp;
    struct bsStgBm *sbm = NULL;
    bf_vd_blk_t srbmtFirstBit_1, srbmtFirstBit_2; 
    uint32_t srbmt_bits_to_set_1, srbmt_bits_to_set_2;
    uint32_t srbmt_start_sbm_pg_1, srbmt_start_sbm_pg_2;

    sbmPgs = howmany( vdattr->vdBlkCnt / vdattr->sbmBlksBit, SBM_BITS_PG);
    sbmBlks = roundup( (sbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS) /
                           ADVFS_FOBS_PER_DEV_BSIZE, 
                       vdattr->sbmBlksBit );

    first_setBlks = ADVFS_RESERVED_BLKS + CPDA_RESERVED_BLKS + 
                    rbmtBlks + bmtBlks + bootBlks + tagBlks;
    first_bits_to_set = first_setBlks / vdattr->sbmBlksBit;
    second_setBlks = preallocBlks + sbmBlks;
    second_bits_to_set = second_setBlks / vdattr->sbmBlksBit;
    sbmFirstPage = (sbmFirstBlk / vdattr->sbmBlksBit) / SBM_BITS_PG;
    sbmFirstBit = (sbmFirstBlk / vdattr->sbmBlksBit) % SBM_BITS_PG;

    /*
     * TODO: Modify ALL shadow rbmt stub related code (including interface)
     *       to init_sbm() when full shadow RBMT support is provided.
     */
    srbmt_bits_to_set_1 = shadowBlks / vdattr->sbmBlksBit;
    srbmt_start_sbm_pg_1 = (shadowFirstBlk / vdattr->sbmBlksBit) / SBM_BITS_PG;
    srbmtFirstBit_1 = (shadowFirstBlk / vdattr->sbmBlksBit) % SBM_BITS_PG;
  

    srbmt_bits_to_set_2 = shadowBlks / vdattr->sbmBlksBit;
    srbmt_start_sbm_pg_2 = (shadowSecondBlk / vdattr->sbmBlksBit) / SBM_BITS_PG;
    srbmtFirstBit_2 = (shadowSecondBlk / vdattr->sbmBlksBit) % SBM_BITS_PG;
    
    sbm = (struct bsStgBm *) ms_malloc( sizeof( struct bsStgBm ) );

    if (first_setBlks  + second_setBlks > vdattr->vdBlkCnt) {
        ms_uprintf("bs_disk_init: disk is too small\n");
        RAISE_EXCEPTION( ENO_MORE_BLKS );
    }

    *freeBlks = vdattr->vdBlkCnt - (first_setBlks + second_setBlks);

    for (i = 0; i < sbmPgs; i++) {
        sbm->magicNumber = SBM_MAGIC ;
        sbm->fsCookie = dmnCookie;
        sbm->pageNumber = i;
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


        if (srbmt_bits_to_set_1 > 0 && (i >= srbmt_start_sbm_pg_1)) {
            if (i==srbmt_start_sbm_pg_1) {
                 set_bits = MIN(SBM_BITS_PG - srbmtFirstBit_1,
                                srbmt_bits_to_set_1);
                 sbm_set_pg_bits(srbmtFirstBit_1, set_bits, sbm);
            } else {
                 set_bits = MIN( SBM_BITS_PG, srbmt_bits_to_set_1 );
                 sbm_set_pg_bits( 0, set_bits, sbm );
            }
            srbmt_bits_to_set_1 -= set_bits;
        }


        if (srbmt_bits_to_set_2 > 0 && (i >= srbmt_start_sbm_pg_2)) {
            if (i==srbmt_start_sbm_pg_2) {
                set_bits = MIN(SBM_BITS_PG - srbmtFirstBit_2,
                               srbmt_bits_to_set_2);
                sbm_set_pg_bits(srbmtFirstBit_2, set_bits, sbm);
            } else {
                set_bits = MIN( SBM_BITS_PG, srbmt_bits_to_set_2 );
                sbm_set_pg_bits( 0, set_bits, sbm );
            }
            srbmt_bits_to_set_2 -= set_bits;
        }
        
         
        /* 
         * Initialize page 'i' of the sbm. We calculate the start block as
         * the first sbm block plus the page times the number of fobs per
         * page, divided by the number of fobs in a DEV_BSIZE block.  This
         * gives us the DEV_BSIZE disk block that the page should be written
         * to.
         */
        err = advfs_raw_io( vp,
                            sbmFirstBlk + 
                             ( (i * ADVFS_METADATA_PGSZ_IN_FOBS) /
                                ADVFS_FOBS_PER_DEV_BSIZE),
                            ADVFS_METADATA_PGSZ_IN_FOBS /
                             ADVFS_FOBS_PER_DEV_BSIZE,
                            RAW_WRITE,
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

    xtntp->chainMCId      = bsNilMCId;
    xtntp->rsvd1          = 0;
    xtntp->rsvd2          = 0;
    xtntp->xCnt           = 2;
    xtntp->bsXA[0].bsx_fob_offset= 0;
    xtntp->bsXA[0].bsx_vd_blk  = sbmFirstBlk;
    xtntp->bsXA[1].bsx_fob_offset = sbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
    xtntp->bsXA[1].bsx_vd_blk  = XTNT_TERM;
    xtntp->mcellCnt       = 1;

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
 *  This routine is called to extend the size of a vd.
 *
 *      Arguments:       vdp        - (in) pointer to virtual disk structure
 *                       size       - (in) block to expand by or -1 for max
 *                       oldBlkSize - (out) original disk block size
 *                       newBlkSize - (out) new disk block size
 *                       xid        - (in) xid to use (if not 0)
 *
 *      Return codes:    EOK    if disk was extended,
 *                       EINVAL if the vd did not need to be extended
 *                       ENOMEM if not enough memory to proceed
 *                       ENOSPC if the extend failed for other reasons. 
 * 
 *  Notes:
 *
 *    newBlkSize is pre-calculated, so on a failure, newBlkSize will contain 
 *    the size that would have resulted assuming no failure.
 *
 */

int
vd_extend(
          struct vd   *vdp,          /* in */
          uint64_t     size,         /* in */
          bf_vd_blk_t *oldBlkSize,   /* out */
          bf_vd_blk_t *newBlkSize,   /* out */
          ftxIdT xid                 /* in */
          )
{

    uint32_t            mapIndex;
    bf_vd_blk_t         oldVdSize;
    bf_vd_blk_t         newVdSize;
    bf_vd_blk_t         devSize;
    capacity_type       capacity;
    dev_t               char_dev;
    sema_t             *save;

    uint32_t            sbmBlksMappedPerPage;
    uint32_t            xtraBlksMappedInSbm;
    uint32_t            xtraBitsInSbm;
    bf_vd_blk_t         blksToMapInNewSbm;
    bs_meta_page_t      newSbmPgs;
    bf_vd_blk_t         newSbmBlk;
    uint32_t            newSbmBitsToSet;
    bf_vd_blk_t         newFreeClusters = 0;
    uint32_t            sbmBitsToSet,                       
                        i,
                        j;
    bfAccessT           *sbmBfap = vdp->sbmp;
    bfAccessT           *mdBfap =  vdp->rbmtp;
    ftxHT               ftxH;
    statusT             sts;
    bsVdAttrT           vdAttr;
    bfMCIdT             newMcellId; 
   
    bsInMemXtntT       *xtnts;
    bsInMemXtntMapT    *newXtntMap;
    int32_t            release_newXtntMap = 0;
    bsInMemSubXtntMapT *origSubXtntMap;
    bsInMemSubXtntMapT *newSubXtntMap;
    advfs_ev           *advfs_event;
    
    struct bsStgBm     *sbmPgp = NULL;

    /*
     *  Check to make sure we do not extend a swap device 
     */
    for (i=0; i < (uint32_t) nswapdev; i++) {
        if (vdp->devVp->v_rdev == swdevt[i].sw_dev) {
            sts = ENOTEMPTY;
            return(sts);
        }

    }

    /*
     *  Get the current vd size.
     */
    char_dev = block_to_raw(vdp->devVp->v_rdev);
    DEVSW(cdevsw, major(char_dev), d_ioctl,
            (char_dev, DIOC_CAPACITY, (caddr_t)&capacity, FREAD),
            sts, save);
    if (sts != EOK) {
        return(sts);
    }
    devSize = capacity.lba;

    sbmPgp = (struct bsStgBm *) ms_malloc( sizeof( struct bsStgBm ) );

    /*
     *  Start up a transaction. If We have been provided with an xid to 
     *  use, then this makes the extend operation idempotent in a cluster.
     */
    sts = FTX_START_EXC_XID( FTA_BS_VD_EXTEND, &ftxH, vdp->dmnP, xid );
    if (sts != EOK) {
        ms_free( sbmPgp );
        return(sts);
    }

    /*
     *  Lock the extent map for the SBM.  Since this takes
     *  the mcell list lock, we need to do this before
     *  locking the mcell list for the (R)BMT to abide
     *  by the lock heirarchy rules. 
     */
    sts = x_lock_inmem_xtnt_map (sbmBfap, X_LOAD_UPDATE|XTNT_WAIT_OK );
    if (sts) {
        ms_free( sbmPgp );
        return(sts);
    }
    
    /* 
     * Make a copy of the SBM's extent map so that we can modify
     * it without holding the extent map lock until we are ready
     * to replace the original.
     */

    sts = imm_copy_xtnt_map(sbmBfap,sbmBfap->xtnts.xtntMap,&newXtntMap);
    if (sts != EOK){
        goto error;
    }
    newXtntMap->hdrType = sbmBfap->xtnts.xtntMap->hdrType;
    newXtntMap->hdrMcellId = sbmBfap->xtnts.xtntMap->hdrMcellId;
    newXtntMap->bsxmNextFob = sbmBfap->xtnts.xtntMap->bsxmNextFob;
    newXtntMap->allocVdIndex = sbmBfap->xtnts.xtntMap->allocVdIndex;
    newXtntMap->origStart = sbmBfap->xtnts.xtntMap->origStart;
    newXtntMap->origEnd = sbmBfap->xtnts.xtntMap->origEnd;
    newXtntMap->validCnt = sbmBfap->xtnts.xtntMap->validCnt;
    
    release_newXtntMap=1;

    /* Note that it is safe to read the extents without the xtntMap_lk
     * since we are holding the mcellList_lk and any modifiers would need that
     * lock.
     */

    /*
     *  Add the mcell list lock to the transaction
     *  and lock the extent map
     */
    FTX_ADD_LOCK ( &sbmBfap->mcellList_lk, ftxH );

    /*
     *  Get the old vd size from our AdvFS disk info.
     *  Keep the mcell lock to prevent racing with another thread.
     */
    sts = bmtr_get_rec_n_lk (mdBfap,
                             BSR_VD_ATTR,
                             &vdAttr,
                             sizeof(bsVdAttrT),
                             BMTR_LOCK );
    if (sts != EOK) {
        goto error;
    }
    oldVdSize = rounddown(vdAttr.vdBlkCnt, vdAttr.sbmBlksBit);

    if (size == -1) /* No size specified by user. Expand to use all of storage */
        newVdSize = devSize;
    else {
        newVdSize = vdAttr.vdBlkCnt + size;
        if (newVdSize > devSize)
        {
            sts = ENOSPC;
            goto error;
        }
    }

    /*
     *  We need extra space for possible metadata expansion so we make sure we
     *  are growing by more that 16 metadata pages. (Should be plenty)
     *  Remember the sizes are unsigned. The first check makes sure we don't
     *  wrap because we got a negative size other than -1.
     */
    if ((newVdSize < oldVdSize) ||
        (newVdSize - oldVdSize <= (16 * (ADVFS_METADATA_PGSZ_IN_FOBS / 
                                         ADVFS_FOBS_PER_DEV_BSIZE)))) { 
        sts = EINVAL;
        goto error;
    }

    /* Set output stats */
    if (oldBlkSize) *oldBlkSize = vdAttr.vdBlkCnt;
    if (newBlkSize) *newBlkSize = newVdSize;

    /*
     *  Calculate the size of the new sbm extent,
     *  and it's start block (page aligned), and a lot
     *  of other stuff.
     */
    sbmBlksMappedPerPage  = SBM_BITS_PG * vdAttr.sbmBlksBit;
    xtraBlksMappedInSbm   = ((sbmBfap->bfaNextFob / ADVFS_METADATA_PGSZ_IN_FOBS) * 
                             sbmBlksMappedPerPage) - oldVdSize;
    xtraBitsInSbm         = (xtraBlksMappedInSbm / vdAttr.sbmBlksBit);
    if ((newVdSize - oldVdSize) > xtraBlksMappedInSbm) {
        blksToMapInNewSbm = (newVdSize - oldVdSize) - xtraBlksMappedInSbm;
    } else {
        blksToMapInNewSbm = 0;
    }
    newSbmPgs             = howmany(blksToMapInNewSbm, sbmBlksMappedPerPage);
    newSbmBlk             = roundup(oldVdSize, vdAttr.sbmBlksBit );
    /*
     * Convert pages to DEV_BSIZE units, then figure out how many bits are
     * required by dividing by sbmBlksBit.
     */
    newSbmBitsToSet       = ((newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS) / 
                                ADVFS_FOBS_PER_DEV_BSIZE) / 
                            vdAttr.sbmBlksBit;
    newFreeClusters       = ((newVdSize - rounddown(oldVdSize, vdAttr.sbmBlksBit)) / 
                                vdAttr.sbmBlksBit) - newSbmBitsToSet;

    /*
     *  If no additional SBM pages needed,
     *      just update the vd's size
     */
    if (newSbmPgs == 0) {
        vdAttr.vdBlkCnt = newVdSize;
        sts  = bmtr_update_rec_n_unlk(mdBfap,
                                      BSR_VD_ATTR,
                                      &vdAttr,
                                      sizeof(bsVdAttrT),
                                      ftxH,
                                      BMTR_UNLOCK );
        if (sts) {
            goto error;
        }
        /*
         * Make sure our available space is truncated to sbmBlksBit
         * boundaries.
         */
        vdp->vdSize           = rounddown(vdAttr.vdBlkCnt, vdAttr.sbmBlksBit);
        vdp->vdClusters      += (newFreeClusters + newSbmBitsToSet);
        vdp->freeClust       += newFreeClusters;
        vdp->dmnP->freeBlks  += (newFreeClusters * vdAttr.sbmBlksBit);
        vdp->dmnP->totalBlks += (newVdSize - oldVdSize);

        ms_free( sbmPgp );
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_226);
        ftx_done_n( ftxH, FTA_BS_VD_EXTEND );
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_227);

        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->special = vdp->vdName;
        advfs_event->domain = vdp->dmnP->domainName;
        advfs_post_kernel_event(EVENT_FDMN_VD_EXTEND, advfs_event);
        ms_free(advfs_event);

        return(EOK);
    }

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
        sbmBitsToSet = MIN(xtraBitsInSbm,newSbmBitsToSet);
        sts = sbm_alloc_bits (vdp,
                              (newSbmBlk / vdAttr.sbmBlksBit),
                              sbmBitsToSet,
                              ftxH);
        if (sts != EOK) {
            goto error;
        }
        newSbmBitsToSet -= sbmBitsToSet;
    }

    /*
     *  Initialize the new SBM pages.
     *  Check if we need to set bits in the newly allocated SBM blocks
     *  These updates do not need to be under transaction control as
     *  the data is only significant if the transaction to update the
     *  virtual disk size succeeds.
     */

    for (i=0; i < newSbmPgs; i++) {
        sbmPgp->magicNumber = SBM_MAGIC;
        sbmPgp->fsCookie = vdp->dmnP->dmnCookie; 
        sbmPgp->pageNumber = vdp->bitMapPgs + i;
        sbmPgp->xor = 0;

        for (j=0; j < SBM_LONGS_PG; j++) {
            sbmPgp->mapInt[j] = 0;
        }

        if (newSbmBitsToSet) {
            sbmBitsToSet = MIN( SBM_BITS_PG, newSbmBitsToSet);
            sbm_set_pg_bits (0, sbmBitsToSet, sbmPgp);
            newSbmBitsToSet -= sbmBitsToSet;
        }

        /*
         *  The new SBM pages will be in the newly added vd space.
         *  This guarantees that there will be space for the new SBM pages.
         *  See the calculation in init_sbm for an expanation. 
         */

        sts = advfs_raw_io( vdp->devVp,
                            newSbmBlk + 
                             ((i * ADVFS_METADATA_PGSZ_IN_FOBS) /
                               ADVFS_FOBS_PER_DEV_BSIZE),
                            ADVFS_METADATA_PGSZ_IN_FOBS /
                             ADVFS_FOBS_PER_DEV_BSIZE,
                             RAW_WRITE,
                            sbmPgp );

        if (sts != EOK) {
            goto error;
        }
    }

    /*
     *  The new SBM pages have been initialized.
     *  Get the pointer to the extent map and the pointer
     *  to the last valid sub extent map.  Insure that there
     *  is at least one unused sub extent map for the update.
     */

    xtnts = &(sbmBfap->xtnts);
    origSubXtntMap = &(newXtntMap->subXtntMap[newXtntMap->validCnt - 1]);

    /*
     *  If there are available extent descriptors in the corresponding mcell,
     *    just add the new extent info
     */

    if (origSubXtntMap->cnt < origSubXtntMap->onDiskMaxCnt) {

        /*
         *  If we're at the in-memory max now, extend the sub extent
         */

        if (origSubXtntMap->cnt == origSubXtntMap->maxCnt) {
            sts = imm_extend_sub_xtnt_map (origSubXtntMap);
            if (sts != EOK) {
                goto error;
            }
        }

        /*
         *  Add the new extent descriptor.
         */
        origSubXtntMap->bsXA[origSubXtntMap->cnt-1].bsx_vd_blk = newSbmBlk;
        origSubXtntMap->bsXA[origSubXtntMap->cnt].bsx_fob_offset = 
            sbmBfap->bfaNextFob + (newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS);
        origSubXtntMap->bsXA[origSubXtntMap->cnt].bsx_vd_blk   = XTNT_TERM;

        origSubXtntMap->cnt++;
        origSubXtntMap->bssxmFobCnt += (newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS);

        origSubXtntMap->updateStart = 0;
        origSubXtntMap->updateEnd = origSubXtntMap->cnt-1;

        /*
         *  Update the on disk data
         */
        sts = update_xtnt_rec (sbmBfap->dmnP,
                               sbmBfap->tag,
                               origSubXtntMap,
                               ftxH);
        if (sts != EOK) {
            goto error;
        }

        /* Now that we have the extent map mapping the storage
         * the way we want it, we will put into the sbm's bfap.
         */

        ADVRWL_XTNTMAP_WRITE( &sbmBfap->xtntMap_lk );
        ADVRWL_VHAND_XTNTMAP_WRITE(&sbmBfap->vhand_xtntMap_lk);

        imm_delete_xtnt_map (xtnts->xtntMap);
        xtnts->xtntMap = newXtntMap;
        xtnts->validFlag = XVT_VALID;

        newXtntMap->bsxmNextFob += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        xtnts->bimxAllocFobCnt += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        sbmBfap->bfaNextFob   += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&sbmBfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK( &sbmBfap->xtntMap_lk );
        release_newXtntMap=0;
    /*
     *  The number of extents in the subXtntMap is at the maximum
     *  for the corresponding mcell.  Allocate a new mcell and sub extent map.
     */

    } else {
        if ( (newXtntMap->validCnt) == newXtntMap->maxCnt) {
            sts = imm_extend_xtnt_map (sbmBfap,newXtntMap,XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                goto error;
            }
        }
        origSubXtntMap = &(newXtntMap->subXtntMap[newXtntMap->validCnt - 1]);
        newSubXtntMap = &(newXtntMap->subXtntMap[newXtntMap->validCnt]);

        sts = bmt_alloc_mcell (sbmBfap,
                               origSubXtntMap->mcellId.volume,
                               RBMT_MCELL,
                               sbmBfap->bfSetp->dirTag,
                               sbmBfap->tag,
                               0,
                               ftxH,
                               &newMcellId,
                               FALSE);
        if (sts != EOK) {
            goto error;
        }
        MS_SMP_ASSERT(newMcellId.volume == origSubXtntMap->mcellId.volume);

        sts = imm_init_sub_xtnt_map (newSubXtntMap,
                                     sbmBfap->bfaNextFob,                                     
                                     newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS,
                                     newMcellId,
                                     BSR_XTRA_XTNTS,
                                     BMT_XTRA_XTNTS,
                                     0);
        if (sts != EOK) {
            goto error;
        }

        /*
         *  Set up the extent descriptors for this extent map.
         *
         */

        newSubXtntMap->bsXA[0].bsx_fob_offset = sbmBfap->bfaNextFob;
        newSubXtntMap->bsXA[0].bsx_vd_blk  = newSbmBlk;
        newSubXtntMap->bsXA[1].bsx_fob_offset = 
            sbmBfap->bfaNextFob + (newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS);
        newSubXtntMap->bsXA[1].bsx_vd_blk  = XTNT_TERM;
        newSubXtntMap->cnt++;

        origSubXtntMap->updateStart = 0;
        origSubXtntMap->updateEnd   = 1;

        /*
         *  This subxtnt has a brand new Mcell, we must create its
         *  on-disk xtnt rec.
         */

        sts = odm_create_xtnt_rec (sbmBfap,
                                   sbmBfap->bfSetp,
                                   newSubXtntMap,
                                   ftxH);
        if (sts != EOK) {
            goto error;
        }
        newSubXtntMap->mcellState = ADV_MCS_INITIALIZED;

        /*
         *  Link the mcells together
         */
        sts = bmt_link_mcells (sbmBfap->dmnP,
                               sbmBfap->tag,
                               origSubXtntMap->mcellId,
                               newSubXtntMap->mcellId,
                               newSubXtntMap->mcellId,
                               ftxH);

        if (sts != EOK) {
            goto error;
        }

        /*
         *  Increment the count of mcells
         */

        sts = update_mcell_cnt (sbmBfap->dmnP,
                                sbmBfap->tag,
                                newXtntMap->hdrMcellId,
                                newXtntMap->hdrType,
                                1,
                                ftxH);
        if (sts != EOK) {
            goto error;
        }

        newXtntMap->validCnt       = ++newXtntMap->cnt;
        /* Now that we have the extent map mapping the storage
         * the way we want it, we will put into the sbm's bfap.
         */

        ADVRWL_XTNTMAP_WRITE( &sbmBfap->xtntMap_lk );
        ADVRWL_VHAND_XTNTMAP_WRITE(&sbmBfap->vhand_xtntMap_lk);

        imm_delete_xtnt_map (xtnts->xtntMap);
        xtnts->xtntMap = newXtntMap;
        xtnts->validFlag = XVT_VALID;

        newXtntMap->bsxmNextFob += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        xtnts->bimxAllocFobCnt += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        sbmBfap->bfaNextFob   += newSbmPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&sbmBfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK( &sbmBfap->xtntMap_lk );
        release_newXtntMap=0;
    }

    /*
     *  The SBM has been updated, the last thing
     *  to do is to update the BSR_VD_ATTR, the vd,
     *  and the access structure.
     */

    vdAttr.vdBlkCnt = newVdSize;
    sts  = bmtr_update_rec_n_unlk (mdBfap,
                                   BSR_VD_ATTR,
                                   &vdAttr,
                                   sizeof(bsVdAttrT),
                                   ftxH,
                                   BMTR_UNLOCK);
    if (sts != EOK) {
        goto error;
    }

    vdp->vdSize           = rounddown(vdAttr.vdBlkCnt, vdAttr.sbmBlksBit);
    vdp->vdClusters       += (newFreeClusters + newSbmBitsToSet);
    vdp->freeClust        += newFreeClusters;
    vdp->bitMapPgs        += newSbmPgs;
    vdp->dmnP->freeBlks   += (newFreeClusters * vdAttr.sbmBlksBit);
    vdp->dmnP->totalBlks  += (newVdSize - oldVdSize);

    MS_SMP_ASSERT(imm_check_xtnt_map(xtnts->xtntMap) == EOK);

    /*
     *  Do some housekeeping, end the transaction and done!
     */

    ms_free( sbmPgp );

    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_368);
    ftx_done_n( ftxH, FTA_BS_VD_EXTEND );
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_228);

    advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
    advfs_event->special = vdp->vdName;
    advfs_event->domain = vdp->dmnP->domainName;
    advfs_post_kernel_event(EVENT_FDMN_VD_EXTEND, advfs_event);
    ms_free(advfs_event);

    return( EOK );

    /*
     *  Error handler
     */
     
error:

    ms_free( sbmPgp );
    if (release_newXtntMap){
        imm_delete_xtnt_map (newXtntMap);
    }
    ADVRWL_MCELL_LIST_UNLOCK( &(mdBfap->mcellList_lk) );
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_408);
    ftx_fail( ftxH );
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_409);

    return( sts );

}

/* end bs_init.c */
