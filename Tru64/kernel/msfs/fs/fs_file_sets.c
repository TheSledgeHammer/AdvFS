/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 *      File Set Management routines
 *
 *      These routines provide a 'file set' shell to several
 *      bitfile set routines.  Their main purpose is to support
 *      the system call interface and to make the appropriate
 *      security/access checks before calling their corresponding
 *      bitfile set routines.
 *
 * Date:
 *
 *      Fri May  1 12:21:07 1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fs_file_sets.c,v $ $Revision: 1.1.149.6 $ (DEC) $Date: 2006/03/20 15:11:05 $"

#include <sys/lock_probe.h>
#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/fs_dir_routines.h>
#include <msfs/ms_osf.h>
#include <sys/stat.h>
#ifdef _OSF_SOURCE
#include <sys/mode.h>
#endif

#include <sys/user.h>
#include <sys/kernel.h>
#include <sys/ucred.h>
#include <sys/mount.h>
#include <sys/clu.h>
#include <msfs/bs_params.h>
#include <msfs/ms_assert.h>

#define ADVFS_MODULE FS_FILE_SETS


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
    serviceClassT optServices,  /* in - set's optional services */
    uint32T fsetOptions,        /* in - fileset options */
    gid_t quotaId,              /* in - group ID for quota files */
    bfSetIdT *retBfSetId,       /* out - set's id */
    long xid1,                  /* in - CFS transaction id */
    long xid2                   /* in - CFS transaction id */
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


    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

    sts = bs_bfdmn_tbl_activate( domain, 0, &domainId );
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

    sts = FTX_START_EXC_XID(FTA_FS_FILE_SETS_1, &ftxH, dmnP, 5, xid1 );
    if ( sts != EOK ) {
        goto _error;
    }

    ftxStarted = TRUE;

    sts = rbf_bfs_create( dmnP, 
                          reqServices, 
                          optServices, 
                          setName,
                          fsetOptions,
                          ftxH,
                          retBfSetId );
    if (sts != EOK) {
        goto _error;
    }

    fileSetCreated = TRUE;

    sts = rbf_bfs_access( &bfSetp, *retBfSetId, ftxH );
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
        /* will not change the file bfState from ACC_FTX_TRANS and later */
        /* umount will hang. We must set the state here, after ftx_done_fs. */
        /* The files in question are the files created when a fileset is */
        /* created: root, root tag, quota.group, quota.user, .tags . */

start:
        mutex_lock(&bfSetp->accessChainLock);
        for (bfap = bfSetp->accessFwd;
             bfap != (bfAccessT *)(&bfSetp->accessFwd);
             bfap = nextbfap) {
        
            if ( bfap->stateLk.state != ACC_FTX_TRANS ) {
                nextbfap = bfap->setFwd;
                continue;
            }
            /*
             * Try to lock the access structure.  If it is already
             * locked by another thread, back off to avoid deadlock
             * since this thread is violating the normal hierarchy
             * locking order.
             */
            if (!mutex_lock_try(&bfap->bfaLock.mutex)) {
                mutex_unlock(&bfSetp->accessChainLock);
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
                mutex_unlock(&bfap->bfaLock);
                continue;
            }

            bfap->bfState = BSRA_VALID;
            lk_signal(lk_set_state(&bfap->stateLk, ACC_VALID), &bfap->stateLk);

            nextbfap = bfap->setFwd;
            mutex_unlock(&bfap->bfaLock);
        }       
        mutex_unlock(&bfSetp->accessChainLock);

        sts = EIO;
        goto _error;
    }

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);

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

    if (ftxStarted) {
        ftx_fail( ftxH );
    } else if (fileSetCreated) {
        (void) bs_bfs_delete( *retBfSetId, dmnP, xid2, 0 );
    }

    if (bfsOpen) {
        if (ftxStarted) {
            bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
        } else {
            bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
        }
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

    sts = rbf_bfs_access( &bfSetp, *retBfSetId, FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }

    setOpen = TRUE;

    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

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
    
    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    if (setParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

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
    
    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setOpen = FALSE;

    sts = bs_bfdmn_deactivate( retBfSetId->domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = FALSE;

    return EOK;

_error:
 
    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
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
    extern domainT *DmnSentinelP;
    domainT *dmnP;
    u_long flags =0;
    bfsQueueT *entry;

    /*
     * try fast path -- in memory structures, no IO required,
     *                  works for mounted filesets.
     */

    DMNTBL_LOCK_READ( &DmnTblLock);
    dmnP = domain_name_lookup( domain, flags);

    bfSetp = NULL;
    if (TEST_DMNP(dmnP) == EOK) {
        BFSETTBL_LOCK_READ( dmnP )
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
        fsnp = bfSetp->fsnp;
        bcopy( &fsnp->fileSetStats, fSetStats, sizeof( fileSetStatsT ) );
    }

    if (TEST_DMNP(dmnP) == EOK) BFSETTBL_UNLOCK(dmnP);
    DMNTBL_UNLOCK( &DmnTblLock);

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

    sts = rbf_bfs_access( &bfSetp, bfSetId, FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }
    setOpen = TRUE;

    if ( ! BFSET_VALID(bfSetp) || (bfSetp->fsnp == NULL)) {
        sts = E_NO_SUCH_BF_SET;
        goto _error;
    }
    
    fsnp = bfSetp->fsnp;
    bcopy( &fsnp->fileSetStats, fSetStats, sizeof( fileSetStatsT ) );

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setOpen = FALSE;

    sts = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    return EOK;

_error:
 
    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
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
    long xid            /* in - CFS transaction id */
    )
{
    bfSetIdT bfSetId;
    bfSetT *bfSetp;
    statusT sts;
    int dmnActive = FALSE, setOpen = FALSE;
    bfDmnParamsT *dmnParamsp = NULL;
    bfSetParamsT *setParamsp = NULL;
    domainT *dmnP;

    sts = bs_bfset_activate( domain, setName, 0, &bfSetId );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = rbf_bfs_access( &bfSetp, bfSetId, FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }
    setOpen = TRUE;
    dmnP = bfSetp->dmnP;

    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

    sts = bs_get_dmn_params( bfSetp->dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Must have write access to a domain to delete a set from it */
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    
    ms_free( dmnParamsp );
    dmnParamsp = NULL;

    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    if (setParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

    sts = bs_get_bfset_params( bfSetp, setParamsp, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        setParamsp->mode, setParamsp->uid, setParamsp->gid )) {
        /* Must have write access to a set to delete it */
        sts = E_ACCESS_DENIED;
        goto _error;
    }
    
    ms_free( setParamsp );
    setParamsp = NULL;
    
    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setOpen = FALSE;

    sts = bs_bfs_delete( bfSetId, dmnP, xid, 0 );
    if (sts != EOK) {
        goto _error;
    }

    sts = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = FALSE;

    return EOK;

_error:

    if (setOpen) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
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
 * fs_fset_clone
 *
 * Creates a clone file set of an 'original' file set.
 */

statusT
fs_fset_clone(
    char *domain,                 /* in - name of set's domain */
    char *origSetName,            /* in - name of orig set */
    char *cloneSetName,           /* in - name of new clone set */
    bfSetIdT *retCloneBfSetId,    /* out - clone set's id */
    long xid                      /* in - CFS transaction id */
    )
{
    statusT sts;
    int origDmnActive = FALSE, origSetOpen = FALSE, origMounted = FALSE;
    bfSetIdT origBfSetId;
    bfSetT *origBfSetp;
    bfDmnParamsT *dmnParamsp = NULL;
    bfSetParamsT *setParamsp = NULL;
    domainT *dmnP;
    fsid_t origFsid;


    sts = bs_bfset_activate( domain, origSetName, 0, &origBfSetId );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    origDmnActive = TRUE;

    sts = rbf_bfs_access( &origBfSetp, origBfSetId, FtxNilFtxH );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    origSetOpen = TRUE;

    dmnP = origBfSetp->dmnP;
    
    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    sts = bs_get_dmn_params( origBfSetp->dmnP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    if (!bs_accessible( BS_ACC_WRITE,
                        dmnParamsp->mode, dmnParamsp->uid, dmnParamsp->gid )) {
        /* Must have write access to a domain to create a set in it */
        RAISE_EXCEPTION( E_ACCESS_DENIED );
    }

    ms_free( dmnParamsp );
    dmnParamsp = NULL;
    
    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    if (setParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }

    sts = bs_get_bfset_params( origBfSetp, setParamsp, 0 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    if (!bs_accessible( BS_ACC_READ,
                        setParamsp->mode, setParamsp->uid, setParamsp->gid )) {
        /* Must have read access to a set to clone it */
        RAISE_EXCEPTION( E_ACCESS_DENIED );
    }

    ms_free( setParamsp );
    setParamsp = NULL;

    if (origBfSetp->fsnp) {
        origFsid = origBfSetp->fsnp->mountp->m_stat.f_fsid;
        origMounted = TRUE;
    }

    bs_bfs_close(origBfSetp, FtxNilFtxH, BFS_OP_DEF);
    origSetOpen = FALSE;

    if (origMounted) {
        /*
         * Notify CFS about the clone.
         */
        CC_CFS_CLONE_NOTIFY(origFsid, CLONE_CREATE);
    }
    sts = bs_bfs_clone( origBfSetId,
                        cloneSetName,
                        retCloneBfSetId,
                        dmnP,
                        xid );
    if (sts != EOK) {
        if (origMounted) {
            CC_CFS_CLONE_NOTIFY(origFsid, CLONE_DELETE);
        }
        goto HANDLE_EXCEPTION;
    }

    sts = bs_bfdmn_deactivate( origBfSetId.domainId, 0 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }
    origDmnActive = FALSE;

    return EOK;

HANDLE_EXCEPTION:

    if (origSetOpen) {
        bs_bfs_close(origBfSetp, FtxNilFtxH, BFS_OP_DEF);
    }
    
    if (origDmnActive) {    
        (void) bs_bfdmn_deactivate( origBfSetId.domainId, 0 );
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
 * fs_fset_get_info
 *
 * Used to get info of all sets in a domain.  See bs_bfs_get_info().
 */

statusT
fs_fset_get_info(
    char *domain,              /* in - domain table */
    uint32T *nextSetIdx,       /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    uint32T *userId,           /* out - bfset user id */
    int flag                   /* in - 1 to set F_FMOUNT and skip recovery */
    )
{
    bfDomainIdT domainId;
    domainT *dmnP;
    bfDmnParamsT *dmnParamsp = NULL;
    statusT sts, retSts;
    int dmnActive = FALSE, dmnOpen = FALSE;
    u_long fmflg = flag ? M_FMOUNT : 0;


    dmnParamsp = (bfDmnParamsT *) ms_malloc( sizeof( bfDmnParamsT ));
    if (dmnParamsp == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

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
    long xid            /* in - CFS transaction id */
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

    sts = rbf_bfs_access( &bfSetp, bfSetId, FtxNilFtxH );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    setAccess = TRUE;
    BFSETTBL_LOCK_WRITE( bfSetp->dmnP )
    tblLocked = TRUE;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        RAISE_EXCEPTION(ENOT_SUPPORTED);
    }

    /*
     * Check to see if fileset is mounted.  If so, return EBUSY.
     */

    if (bfSetp->fsnp) {
        RAISE_EXCEPTION (EBUSY);
    }

    setParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    if (setParamsp == NULL) {
        RAISE_EXCEPTION( ENO_MORE_MEMORY );
    }


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

    bcopy( newsetName, setParamsp->setName, BS_SET_NAME_SZ );

    sts = bs_set_bfset_params( bfSetp, setParamsp, xid );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    BFSETTBL_UNLOCK( bfSetp->dmnP )
    tblLocked = FALSE;

    ms_free( setParamsp );
    setParamsp = NULL;

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    setAccess = FALSE;
    
    sts = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    return EOK;

HANDLE_EXCEPTION:

    if (tblLocked) {
        BFSETTBL_UNLOCK( bfSetp->dmnP )
    }

    if (setParamsp != NULL) {
        ms_free( setParamsp );
    }

    if (setAccess) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    }

    return sts;
}


#define FRAG_PERCENT 5

#define ONE_K 1024
#define SEVEN_K (7 * ONE_K)
#define EIGHT_K (8 * ONE_K)
/*
 * This symbol defines a file byte size upper limit for a file with
 * an 8k page size.  If the file's last page has only one byte used
 * and the file is the limit size or greater, the percentage of wasted
 * bytes represented by the last page if it is not fragged (8k - 1)
 * is FRAG_PERCENT or less.
 */
#define EIGHT_K_BYTE_LIMIT (((EIGHT_K - 1) * 100) / FRAG_PERCENT)


/*
 * fs_create_frag
 *
 * This function creates a frag for the specified file, if needed.  A frag is
 * created if the used space in the last page is 7k or less and the unused space
 * in the last page represents 5% or more of the file's total size.
 *
 * The caller must have exclusive access to the file.  No other thread can have
 * or get access to the file.  Because of this, this function needs no
 * synchronization.
 *
 * This function assumes non-sparse files.
 *
 * This function assumes that the file's page size is 8k.
 *
 * The "trunc" flag is set in the access structure if a frag was created.
 */

statusT
fs_create_frag (
                bfSetT *bfSetp,  /* in */
                bfAccessT* bfap,  /* in */
                struct ucred *cred,  /* in */
                ftxHT parentFtxH  /* in */
                )
{
    uint32T copyByteCnt;
    struct fileSetNode *fileSetNode;
    uint32T fragByteCnt;
    bfFragIdT fragId;
    bfFragT fragType;
    uint32T inuseByteCnt;
    char *lastPage = NULL;
    uint32T lastPageOffset;
    struct mount *mountPoint;
    bfPageRefHT pgRef;
    statusT sts;
    uint32T subFrag1ByteCnt;
    uint32T subFrag1ByteOffset;
    char *subFrag1Page;
    rbfPgRefHT subFrag1PgPin;
    uint32T subFrag2ByteCnt;
    char *subFrag2Page;
    rbfPgRefHT subFrag2PgPin;
    int wastedPercent;
    int qType;
    int adjustQuotaFlag = 0;
    struct fsContext *fileContext;

    fileContext = VTOC( bfap->bfVp );

    /*
     * If read-only file set just return.
     */

    mountPoint = VTOMOUNT (bfap->bfVp);
    fileSetNode = GETFILESETNODE (mountPoint);

    for (qType = 0; qType < MAXQUOTAS; qType++) {
        if (BS_BFTAG_EQL (bfap->tag, fileSetNode->qi[qType].qiTag)) {
            /*
             * Quota file.  Don't frag it.
             */
            return (statusT)1;
        }
    }

    if ((fileSetNode->fsFlags & FS_CLONEFSET) ||
            (mountPoint->m_flag & M_RDONLY)) {
        return (statusT)1;
    }

    /*
     * Assert no frags on direct I/O files.
     */
    MS_SMP_ASSERT(!(bfap->bfVp->v_flag & VDIRECTIO));

    inuseByteCnt = bfap->file_size % ADVFS_PGSZ;

    wastedPercent = (((ADVFS_PGSZ - inuseByteCnt) * 100) / bfap->file_size);
    if (wastedPercent < FRAG_PERCENT) {
        /*
         * The percentage of wasted space represented by the unused bytes in
         * the last page if it is not fragged is less than the frag percent.
         * Don't frag the last page.
         */
        return (statusT)1;
    }

    /*
     * Ref the last (frag) page.
     */

    lastPageOffset = bfap->file_size / ADVFS_PGSZ;
    sts = bs_refpg (
                    &pgRef,
                    (void*)&lastPage,
                    bfap,
                    lastPageOffset,
                    BS_NIL
                    );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    fragType = (inuseByteCnt + (ONE_K - 1)) / ONE_K;
    fragByteCnt = fragType * ONE_K;

    /*
     * If bs_frag_alloc() returns EOK, we can't fail.  Otherwise, SAD.
     * This is because bs_frag_alloc() pins records in parentFtxH's
     * transaction tree.
     *
     * WARNING: bs_frag_alloc() does not start a sub-transaction.  Also,
     * it acquires an ftx lock which isn't released until this function's
     * transaction completes.
     */
    sts = bs_frag_alloc (bfSetp, fragType, parentFtxH, &fragId);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    subFrag1ByteOffset = FRAG2SLOT (fragId.frag) * ONE_K;
    subFrag1ByteCnt = ADVFS_PGSZ - subFrag1ByteOffset;
    if (subFrag1ByteCnt > fragByteCnt) {
        subFrag1ByteCnt = fragByteCnt;
    }

    /*
     * Copy the original data into the frag.  Note that the frag can
     * span two pages.
     */

     sts = rbf_pinpg (
                      &subFrag1PgPin,
                      (void*)&subFrag1Page,
                      bfSetp->fragBfAp,
                      FRAG2PG (fragId.frag),
                      BS_NIL,
                      parentFtxH
                      );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    subFrag1Page = subFrag1Page + subFrag1ByteOffset;

    if (subFrag1ByteCnt < fragByteCnt) {

         sts = rbf_pinpg (
                          &subFrag2PgPin,
                          (void*)&subFrag2Page,
                          bfSetp->fragBfAp,
                          FRAG2PG (fragId.frag) + 1,
                          BS_NIL,
                          parentFtxH
                          );

        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        subFrag2ByteCnt = fragByteCnt - subFrag1ByteCnt;
    }

    bfap->fragId = fragId;
    bfap->fragPageOffset = lastPageOffset;
    fileContext->dir_stats.st_size = bfap->file_size;
    fileContext->dir_stats.fragId = fragId;
    fileContext->dir_stats.fragPageOffset = lastPageOffset;
    sts = bmtr_update_rec (
                           bfap,
                           BMTR_FS_STAT,
                           &(fileContext->dir_stats),
                           sizeof (fileContext->dir_stats),
                           parentFtxH,
                           0
                           );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    bfap->fragState = FS_FRAG_VALID;
    rbf_pin_record (subFrag1PgPin, subFrag1Page, subFrag1ByteCnt);
    if (inuseByteCnt <= subFrag1ByteCnt) {
        copyByteCnt = inuseByteCnt;
    } else {
        copyByteCnt = subFrag1ByteCnt;
    }
    bcopy (lastPage, subFrag1Page, copyByteCnt);
    bzero (subFrag1Page + copyByteCnt, subFrag1ByteCnt - copyByteCnt);

    if (subFrag1ByteCnt < fragByteCnt) {
        rbf_pin_record (subFrag2PgPin, subFrag2Page, subFrag2ByteCnt);
        copyByteCnt = inuseByteCnt - subFrag1ByteCnt;
        bcopy (lastPage + subFrag1ByteCnt, subFrag2Page, copyByteCnt);
        bzero (subFrag2Page + copyByteCnt, subFrag2ByteCnt - copyByteCnt);
    }

    bs_derefpg(pgRef, BS_CACHE_IT);

    bfap->trunc = 1;

    return EOK;

HANDLE_EXCEPTION:

    if (lastPage != NULL) {
        bs_derefpg(pgRef, BS_CACHE_IT);
    }
    
    return (statusT)1;

}  /* end fs_create_frag */


/*
 * fs_quick_frag_test
 *
 * This function does a quick test to determine if the file needs to
 * be fragged. If it does, return 1.  Otherwise, return 0;
 *
 * File context is assumed to be valid and initialized.
 */

int
fs_quick_frag_test (
                    bfAccessT *bfap  /* in */
                    )
{
    struct fileSetNode *fileSetNode;
    uint32T inuseByteCnt;
    struct mount *mountPoint;
    bfSetT* bfSetp;
    struct vnode *vnode = bfap->bfVp;
    MS_SMP_ASSERT(vnode);


    /* 
     * If any of the following conditions are true then
     * we can not frag the file */

    /* check if the file is a reserved file. */

    if ( (bfap->nextPage == 0) ||
         ((bfap->dataSafety != BFD_NIL) &&
          (bfap->dataSafety != BFD_SYNC_WRITE) &&
          (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY)) ||
         (bfap->fragState != FS_FRAG_NONE) ||
         (vnode == NULL) ||
         (BS_BFTAG_RSVD(bfap->tag)) ) {

        return FALSE;
    }

    /*
     *  Check if the fileset is marked for no fragging
     */

    if ( (bfSetp = bfap->bfSetp) && (bfSetp->bfSetFlags & BFS_OD_NOFRAG) ) {
        return FALSE;
    }
            
    mountPoint = VTOMOUNT (vnode);
    fileSetNode = GETFILESETNODE (mountPoint);

    if (fileSetNode == NULL) {
        return FALSE;
    }

    /*
     * Don't create a frag if file is using direct I/O.
     */
    if (bfap->bfVp->v_flag & VDIRECTIO) {
        return FALSE;
    }

    inuseByteCnt = bfap->file_size % ADVFS_PGSZ;

    if ((inuseByteCnt == 0) ||
        (inuseByteCnt > SEVEN_K) ||
        (bfap->file_size >= EIGHT_K_BYTE_LIMIT)) {
        /*
         * The inuse bytes in the last page is greater than the largest
         * frag or the file's byte size is the break even size or greater.
         */
        return FALSE;
    }

    if ((fileSetNode->fsFlags & FS_CLONEFSET) ||
                    (mountPoint->m_flag & M_RDONLY)) {
        /*
         * The file set is read-only.
         */
        return FALSE;
    }

    return TRUE;
    
}  /* end fs_quick_frag_test */


/*
 * fs_delete_frag
 *
 * This function deletes a frag for the specified file, if needed.
 *
 * The caller must own the file lock (exclusive) or must have exclusive
 * access to this file.
 *
 * FIX - clean up error paths.
 */

statusT
fs_delete_frag (
    bfSetT *bfSetp,  /* in */
    bfAccessT *bfap,    /* in */
    struct vnode *vnode,  /* in */
    struct ucred *cred,  /* in */
    int quotas_done,  /* in */
    ftxHT parentFtxH  /* in */
    )
{
    struct fsContext *fileContext;
    statusT sts;
    statusT sts2;


    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* nothing to do if it's a clone fileset */
        return EOK;
    }

    if (vnode == NULL) {
        /* called from bs_close during a bitfile set delete */
        struct fs_stat dir_stats;

        sts = bmtr_get_rec (
                           bfap,
                           BMTR_FS_STAT,
                           &dir_stats,
                           sizeof (dir_stats)
                           );
        if (sts != EOK) {
            /* read error or doesn't have file stats */
            return EOK;
        }

        if (dir_stats.fragId.frag == 0) {
            /* doesn't have a frag */
            return EOK;
        }
 
        bfap->fragId = dir_stats.fragId;
 
        bs_frag_dealloc(bfSetp, parentFtxH, dir_stats.fragId);

        bfap->fragId =  bsNilFragId;
        dir_stats.st_size = bfap->file_size;
        dir_stats.fragId = bsNilFragId;
        /* no need to set fragPageOffset */

        sts = bmtr_update_rec (
                               bfap,
                               BMTR_FS_STAT,
                               &dir_stats,
                               sizeof (dir_stats),
                               parentFtxH,
                               0
                               );
        if (sts != EOK) {
            domain_panic(bfap->dmnP, "fs_delete_frag: bmtr_update_rec (1) failed, return code = %d", sts);
            return E_DOMAIN_PANIC;
        }
        return EOK;
    }

    fileContext = VTOC (vnode);
    if (fileContext == NULL) {
        /*
         * Not yet opened thru the file system.
         */
        return EOK;
    }

    if (bfap->fragState == FS_FRAG_VALID) { 
        long quota_blocks = - bfap->fragId.type * 2;

        bs_frag_dealloc(bfSetp, parentFtxH, bfap->fragId);

        bfap->fragState = FS_FRAG_NONE;
        bfap->fragId =  bsNilFragId;
        fileContext->dir_stats.st_size = bfap->file_size;
        fileContext->dir_stats.fragId = bsNilFragId;
        /* no need to set fragPageOffset */

        sts = bmtr_update_rec (
                               bfap,
                               BMTR_FS_STAT,
                               &(fileContext->dir_stats),
                               sizeof (fileContext->dir_stats),
                               parentFtxH,
                               0
                               );
        if (sts != EOK) {
            domain_panic(bfap->dmnP, "fs_delete_frag: bmtr_update_rec (2) failed, return code = %d", sts);
            return E_DOMAIN_PANIC;
        }

        if (quotas_done == FALSE) {
            /* only an I/O error can make this fail and we just log that */
            sts = change_quotas(vnode, 0, quota_blocks,
                         NULL, cred, 0, parentFtxH);
        }
    }
    return EOK;

}  /* end fs_delete_frag */
