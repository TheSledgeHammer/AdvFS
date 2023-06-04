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
 *                                                                          *
 *
 * Facility:
 *
 *      AdvFS
 *
 * Abstract:
 *
 *      File Set Management routines
 *
 *      These routines provide a 'file set' shell to several
 *      bitfile set routines.  Their main purpose is to support
 *      the system call interface and to make the appropriate
 *      security/access checks before calling their corresponding
 *      bitfile set routines.
 *
 */

#include <sys/param.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <fs_dir_routines.h>
#include <ms_osf.h>
#include <sys/stat.h>
#include <sys/user.h>
#include <sys/kernel.h>
#include <sys/mount.h>
#include <tcr/clu.h>
#include <bs_params.h>
#include <ms_assert.h>
#include <bs_bitfile_sets.h>

#define ADVFS_MODULE FS_FILE_SETS
#define OWNER_ONLY TRUE
#define ANYONE FALSE


/*
 * fs_fset_create
 *
 * Creates a file set.
 */

statusT
fs_fset_create(
    char *domain,               /* in - set's domain table */
    char *setName,              /* in - set's name */
    serviceClassT reqServices,  /* in - set's required services */
    uint32_t fsetOptions,       /* in - fileset options */
    gid_t quotaId,              /* in - group ID for quota files */
    bfSetIdT *retBfSetId,       /* out - set's id */
    ftxIdT xid1,                /* in - CFS transaction id */
    ftxIdT xid2                 /* in - CFS transaction id */
    )
{
    statusT sts;
    int fileSetCreated = FALSE, ftxStarted = FALSE;
    int dmnActive = FALSE, dmnOpen = FALSE, bfsOpen = FALSE;
    bfDomainIdT domainId;
    domainT *dmnP;
    bfSetT *bfSetp;
    bfDmnParamsT *dmnParamsp = NULL;
    ftxHT ftxH;

    dmnParamsp = (bfDmnParamsT *) ms_malloc_no_bzero( sizeof( bfDmnParamsT ));

    sts = bs_bfdmn_tbl_activate( domain, DMNA_CREATING, &domainId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    sts = bs_get_dmn_params( dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Must have write access to a domain to create a set in it */
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    
    ms_free( dmnParamsp );
    dmnParamsp = NULL;
    
    /*
     * Use an exclusive transaction.  This allows only one fileset
     * creation at a time and serializes with fileset clone and delete.
     */

    sts = FTX_START_EXC_XID(FTA_FS_FILE_SETS_1, &ftxH, dmnP, xid1 );
    if ( sts != EOK ) {
        goto _error;
    }

    ftxStarted = TRUE;

    sts = rbf_bfs_create( dmnP, 
                          reqServices, 
                          setName,
                          fsetOptions,
                          ftxH,
                          retBfSetId );
    if (sts != EOK) {
        goto _error;
    }

    fileSetCreated = TRUE;

    sts = bfs_open( &bfSetp, *retBfSetId, BFS_OP_DEF, ftxH );
    if (sts != EOK) {
        goto _error;
    }

    bfsOpen = TRUE;

    sts = fs_create_file_set( bfSetp, quotaId, ftxH );
    if (sts != EOK) {
        goto _error;
    }
    /* fs_create_file_set pinned records. No ftx_fail after this. */

    ftx_done_n( ftxH, FTA_FS_FILE_SETS_1 );

    ftxStarted = FALSE;

    if ( dmnP->dmn_panic ) {
        bfAccessT *bfap, *nextbfap;

        /* If the domain paniced before ftx_done_n was run, create_rtdn_opx */
        /* will not change the file bfState from ACC_CREATING and later */
        /* umount will hang. We must set the state here, after ftx_done_fs. */
        /* The files in question are the files created when a fileset is */
        /* created: root, root tag, quota.group, quota.user, .tags . */

start:
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);
        for (bfap = bfSetp->accessFwd;
             bfap != (bfAccessT *)(&bfSetp->accessFwd);
             bfap = nextbfap) {
        
            if ( bfap->stateLk.state != ACC_CREATING) {
                nextbfap = bfap->setFwd;
                continue;
            }
            /*
             * Try to lock the access structure.  If it is already
             * locked by another thread, back off to avoid deadlock
             * since this thread is violating the normal hierarchy
             * locking order.
             */
            if (!ADVSMP_BFAP_TRYLOCK(&bfap->bfaLock)) {
                ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);
                goto start;
            }

            MS_SMP_ASSERT(bfap->bfSetp == bfSetp);

            /*
             * If the access structure is in the process of
             * being recycled by another thread, skip it
             * since the recycling process will take it off
             * this fileset's list.
             */
            if (lk_get_state(bfap->stateLk) == ACC_RECYCLE) {
                nextbfap = bfap->setFwd;
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                continue;
            }


            bfap->bfState = BSRA_VALID;
            lk_signal(lk_set_state(&bfap->stateLk, ACC_INVALID), &bfap->stateLk);
            nextbfap = bfap->setFwd;
            /*
             * advfs_release_access(), by calling advfs_dealloc_access() will
             * drop the bfap lock.
             */
            DEC_REFCNT( bfap, DRC_DEALLOC );
        }       
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);

        sts = EIO;
        goto _error;
    }

    sts = bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    if (sts != EOK) {
        goto _error;
    }

    bfsOpen = FALSE;

    bs_domain_close( dmnP );

    dmnOpen = FALSE;

    sts = bs_bfdmn_deactivate( domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = FALSE;

    return EOK;

_error:

    if (bfsOpen) {
        if (ftxStarted) {
            (void) bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
        } else {
            (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        }
    }

    if (ftxStarted) {
        ftx_fail( ftxH );

    } else if (fileSetCreated) {
        (void) bs_bfs_delete( *retBfSetId, xid2 );
    }

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( domainId, 0 );
    }
 
    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }
    return sts;
}

/*
 * fs_fset_get_id
 *
 * Return a file set's id.
 */

statusT
fs_fset_get_id(
    char *domain,          /* in - name of set's domain table */
    char *setName,         /* in - name of set */
    bfSetIdT *retBfSetId   /* out - set's id */
    )
{
    bfSetT *bfSetp;
    
    statusT sts;
    int dmnActive = FALSE, setOpen = FALSE;
    bfDmnParamsT *dmnParamsp = NULL;
    bfSetParamsT *setParamsp = NULL;

    sts = bs_bfset_activate( domain, setName, 0, retBfSetId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bfs_open( &bfSetp, *retBfSetId, BFS_OP_IGNORE_OUT_OF_SYNC, 
            FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }

    setOpen = TRUE;

    dmnParamsp = (bfDmnParamsT *) ms_malloc_no_bzero( sizeof( bfDmnParamsT ));

    sts = bs_get_dmn_params( bfSetp->dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_READ,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Must have read access to a domain to get its id */
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    ms_free( dmnParamsp );
    dmnParamsp = NULL;
    
    setParamsp = (bfSetParamsT *) ms_malloc_no_bzero( sizeof( bfSetParamsT ));

    sts = bs_get_bfset_params( bfSetp, setParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_READ,
                        setParamsp->mode, setParamsp->uid, setParamsp->gid )) {
        /* Must have read access to a set to get its id */
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    ms_free( setParamsp );
    setParamsp = NULL;
    
    (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setOpen = FALSE;

    sts = bs_bfdmn_deactivate( retBfSetId->domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = FALSE;

    return EOK;

_error:
 
    if (setOpen) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( retBfSetId->domainId, 0 );
    }

    if (setParamsp != NULL) {
        ms_free( setParamsp );
    }
    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }
    return sts;
}

/*
 * fs_fset_get_stats
 *
 * Return a file set's stats.
 */

statusT
fs_fset_get_stats(
    char *domain,               /* in - name of set's domain */
    char *setName,              /* in - name of set */
    fileSetStatsT *fSetStats    /* out - set's stats */
    )
{
    statusT sts;
    int dmnActive = FALSE, setOpen = FALSE;
    bfSetT *bfSetp, *temp;
    struct fileSetNode *fsnp;
    bfSetIdT bfSetId;
    domainT *dmnP;
    uint64_t flags =0;
    bfsQueueT *entry;

    /*
     * try fast path -- in memory structures, no IO required,
     *                  works for mounted filesets.
     */

    ADVRWL_DMNACTIVATION_READ( &DmnActivationLock);
    dmnP = domain_name_lookup( domain, flags);

    bfSetp = NULL;
    if (TEST_DMNP(dmnP) == EOK) {
        ADVRWL_BFSETTBL_READ( dmnP );
        entry = dmnP->bfSetHead.bfsQfwd;
        while (entry!= &dmnP->bfSetHead) {
            temp = BFSET_QUEUE_TO_BFSETP(entry);
            if ((strcmp(temp->bfSetName, setName) == 0) && BFSET_VALID(temp) &&
                    (temp->fsRefCnt > 0) && (temp->fsnp != NULL)) {
                bfSetp = temp;
                break;
            }
            entry = entry->bfsQfwd;
        }
    }
     
    if (bfSetp != NULL) {
        fsnp = (struct fileSetNode *) bfSetp->fsnp;
        bcopy( &fsnp->fileSetStats, fSetStats, sizeof( fileSetStatsT ) );
    }

    if (TEST_DMNP(dmnP) == EOK) ADVRWL_BFSETTBL_UNLOCK(dmnP);        
    ADVRWL_DMNACTIVATION_UNLOCK( &DmnActivationLock);

    if (bfSetp != NULL) {
        return EOK;
    } 

    /*
     *  then slow path -- can take wall clock seconds -- only needed
     *                    when set is not mounted.
     */

    sts = bs_bfset_activate( domain, setName, 0, &bfSetId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }
    setOpen = TRUE;

    if ( ! BFSET_VALID(bfSetp) || (bfSetp->fsnp == NULL)) {
        sts = E_NO_SUCH_BF_SET;
        goto _error;
    }
    
    fsnp = (struct fileSetNode *) bfSetp->fsnp;
    bcopy( &fsnp->fileSetStats, fSetStats, sizeof( fileSetStatsT ) );

    (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setOpen = FALSE;

    sts = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    return EOK;

_error:
 
    if (setOpen) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    }
    
    return sts;
}

/*
 * fs_fset_delete
 *
 * Delete's a file set.
 */

statusT
fs_fset_delete(
    char *domain,       /* in - name of set's domain table */
    char *setName,      /* in - name of set to delete */
    ftxIdT xid          /* in - CFS transaction id */
    )
{
    bfSetIdT bfSetId;
    statusT sts;
    int dmnActive = FALSE;
        domainT *dmnP;

    sts = bs_bfset_activate( domain, setName, 0, &bfSetId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_bfs_delete( bfSetId, xid );
    if (sts != EOK) {
        goto _error;
    }

_error:

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    }
    return sts;
}

/*
 * fs_fset_get_info
 *
 * Used to get info of all sets in a domain.  See bs_bfs_get_info().
 */

statusT
fs_fset_get_info(
    char *domain,              /* in - domain table */
    uint64_t *nextSetIdx,      /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    uint32_t *userId,          /* out - bfset user id */
    int flag                   /* in - 1 to set F_FMOUNT and skip recovery */
    )
{
    bfDomainIdT domainId;
    domainT *dmnP;
    bfDmnParamsT *dmnParamsp = NULL;
    statusT sts, retSts;
    int dmnActive = FALSE, dmnOpen = FALSE;
    uint64_t fmflg = flag ? DMNA_FMOUNT : 0;

    dmnParamsp = (bfDmnParamsT *) ms_malloc_no_bzero( sizeof( bfDmnParamsT ));

    sts = bs_bfdmn_tbl_activate( domain, fmflg, &domainId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    sts = bs_get_dmn_params( dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_READ,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Must have read access to a domain to get set info */
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    ms_free( dmnParamsp );
    dmnParamsp = NULL;

    retSts = bs_bfs_get_info( nextSetIdx, bfSetParams, dmnP, userId );

    bs_domain_close( dmnP );

    dmnOpen = FALSE;

    sts = bs_bfdmn_deactivate( domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = FALSE;

    return retSts;

_error:

    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( domainId, 0 );
    }

    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }
    return sts;
}

/*
 * 
 * fs_fset_name_change
 *
 * Used to change a file set's name.
 */

statusT
fs_fset_name_change(
    char *domain,       /* in - domain table file name */
    char *origsetName,  /* in - set's name */
    char *newsetName,   /* in - set's new name */
    ftxIdT xid          /* in - CFS transaction id */
    )
{
    bfSetIdT bfSetId;
    bfSetT *bfSetp;
    statusT sts;
    int dmnActive = FALSE, setAccess = FALSE;
    int tblLocked = FALSE;
    bfSetParamsT *setParamsp = NULL;

    /*
     * try to activate the new set name. If success,
     * error out with E_DUPLICATE_SET
     */
    sts = bs_bfset_activate( domain, newsetName, 0, &bfSetId );
    if (sts == EOK) {
        /* ignore error */
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
        RAISE_EXCEPTION (E_DUPLICATE_SET);
    }
    sts = bs_bfset_activate( domain, origsetName, 0, &bfSetId );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    dmnActive = TRUE;

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    setAccess = TRUE;
    ADVRWL_BFSETTBL_WRITE( bfSetp->dmnP );
    tblLocked = TRUE;

    if (bfSetp->bfsParentSnapSet != NULL) {
        RAISE_EXCEPTION(ENOT_SUPPORTED);
    }

    /*
     * Check to see if fileset is mounted.  If so, return EBUSY.
     */

    if (bfSetp->fsnp) {
        RAISE_EXCEPTION (EBUSY);
    }

    setParamsp = (bfSetParamsT *) ms_malloc_no_bzero( sizeof( bfSetParamsT ));

    sts = bs_get_bfset_params( bfSetp, setParamsp, 0 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if (!bs_owner( setParamsp->uid )) {
        /* Must be owner to change it */
        RAISE_EXCEPTION (E_NOT_OWNER);
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        setParamsp->mode, setParamsp->uid, setParamsp->gid )) {
        /* Must have write access to a set to change it */
        RAISE_EXCEPTION (E_ACCESS_DENIED);
    }

    flmemcpy( newsetName, setParamsp->setName, BS_SET_NAME_SZ );

    sts = bs_set_bfset_params( bfSetp, setParamsp, xid );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    ADVRWL_BFSETTBL_UNLOCK( bfSetp->dmnP );
    tblLocked = FALSE;

    ms_free( setParamsp );
    setParamsp = NULL;

    (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setAccess = FALSE;
    
    sts = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    return EOK;

HANDLE_EXCEPTION:

    if (tblLocked) {
        ADVRWL_BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    if (setParamsp != NULL) {
        ms_free( setParamsp );
    }

    if (setAccess) {
        (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    }

    return sts;
}
/* end fs_file_sets.c */
