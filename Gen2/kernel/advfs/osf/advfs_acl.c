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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *                                                               *
 * *****************************************************************
 *
 *
 * Faculty:
 *
 *  AdvFS
 *
 * Abstract:
 *
 *  advfs_acl.c
 *  This is the main implementation module for the AdvFS 1024 ACLs functions.
 *
 */

#define ADVFS_MODULE ADVFS_ACLS

#include <sys/cred.h>
#include <sys/aclv.h>

#include <ms_public.h>
#include <ms_privates.h>
#include <ftx_privates.h>
#include <bs_index.h>

#include <advfs_acl.h>
#include <bs_extents.h>
#include <bs_snapshot.h>

/* MAJOR ACL MODULES */

/*
 *  NAME:		advfs_setacl
 *
 *  DESCRIPTION:	This routine is the AdvFS specific callout for
 *			VOP_SETACL.  It is a wrapper for advfs_setacl_int.
 *
 *  ARGUMENTS:		vp		File to set ACL on
 *			num_tuples	Number of ACL entries requested
 *			tupleset	Buffer with the new ACL entries
 *			acl_type	ACL type, should be SYSV_ACLS
 *
 *  RETURN VALUES:	SUCCESS		EOK
 *			FAILURE		E_BAD_HANDLE, EIO, ENOSYS,
 * 					EINVAL, EROFS, ENOMEM
 */

int
advfs_setacl(
    struct vnode *vp,
    int num_tuples,
    struct acl_tuple_user *tupleset,
    int acl_type )
{
    return advfs_setacl_int( FtxNilFtxH,
			     vp,
			     num_tuples,
			     (struct acl *)tupleset,
			     acl_type );
}

/*
 *  NAME:               advfs_getacl
 *
 *  DESCRIPTION:        This routine is the AdvFS specific callout for
 *                      VOP_GETACL.  A buffer will be provided by the caller
 *			to fill in with the ACL entries for a particular file.
 *
 *  ARGUMENTS:          vp              File to get ACL on
 *                      num_tuples      Number of ACL entries requested
 *                      tupleset	Buffer to add the new ACL entries to
 *                      acl_type        ACL type, should be SYSV_ACLS
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *                      FAILURE         E_BAD_HANDLE, EIO, ENOSYS,
 *                                      EINVAL, ENOMEM
 */

int
advfs_getacl(
    struct vnode *vp,
    int num_tuples,
    struct acl_tuple_user *tupleset,
    int acl_type )
{
    struct acl *acl = (struct acl *)tupleset;
    advfs_acl_t *advfs_acl;
    int i, tuple_cnt;
    bfAccessT *bfap;
    statusT sts;

    /* Verify ACL type requested */
    if( acl_type != ADVFS_ACL_TYPE ) {
	u.u_error = ENOSYS;
	return ENOSYS;
    }

    /* Convert vnode to bfAccess */
    bfap = VTOA( vp );

    /* Check for domain panic */
    if( bfap->dmnP->dmn_panic ) {
	u.u_error = EIO;
	return EIO;
    }

    /*
     * Verify that number of tuples requested is not more than max allowed by
     * AdvFS and that the number requested is not negative
     */
    if( ( num_tuples > ADVFS_MAX_ACL_ENTRIES ) || ( num_tuples < 0 ) ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /*
     * Filter files that cannot have an ACL set on them, otherwise, this will
     * result in panic trying to lock the fsContext mutex.
     *
     * Trying to filter out index files at ths point may cause a very tight race
     * condition during the IDX_INDEXING_ENABLED check; therefore, we'll only
     * filter then out in advfs_setacl_int.
     */
    if( BS_BFTAG_RSVD( bfap->tag ) ||
        bfap->real_bfap ||
        BS_BFTAG_EQL( bfap->bfSetp->bfSetId.dirTag, staticRootTagDirTag ) ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /*
     * We can not take advantage of the acl cache.  We'll have to go to disk
     * to retrieve the ACL entries.
     */
    if( bfap->bfa_acl_cache == NULL ) {

	/*
	 * Take the ACL lock so we can reset the bfa_acl_cache once we get
	 * the ACL from disk.  Take the mcell list lock before calling
	 * advfs_getacl_bmt in case we need to read the ACL from disk.
	 */
	ADVRWL_BFAP_BFA_ACL_LOCK_WRITE( &bfap->bfa_acl_lock );
	ADVRWL_MCELL_LIST_READ( &bfap->mcellList_lk );
	if( bfap->bfa_acl_cache == NULL ) {
	    /* advfs_getacl_bmt goes to disk for the ACL and resets the cache */
	    sts = advfs_getacl_bmt( &advfs_acl, &tuple_cnt, bfap );
	    if( sts != EOK ) {
		ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		u.u_error = sts;
		return sts;
	    }
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    ADVRWL_BFAP_BFA_ACL_DOWNGRADE( &bfap->bfa_acl_lock );

	} else {
	    /* The acl cache is valid */
	    tuple_cnt = bfap->bfa_acl_cache_len;
	    advfs_acl = bfap->bfa_acl_cache;
	}

    } else {

	ADVRWL_BFAP_BFA_ACL_LOCK_READ( &bfap->bfa_acl_lock );

	/* Woops, we lost the acl_cahce while trying to grab the lock */
	if( bfap->bfa_acl_cache == NULL ) {

	    /*
	     * Take the mcell list lock so we can read the ACL, and upgrade
	     * the ACL lock so we can reset the bfa_acl_cache once we get
	     * the ACL from disk.  Upgrade the ACL lock first to avoid the
	     * race condition where one thread may lock the mcellList_lk for
	     * read, then before it has a chance to upgrade the ACL lock,
	     * another thread locks the ACL lock for read.  Neither thread can
	     * then upgrade the ACL lock.
	     */
	    rwlock_upgrade( &bfap->bfa_acl_lock );
	    ADVRWL_MCELL_LIST_READ( &bfap->mcellList_lk );

	    /* advfs_getacl_bmt gets ACL from disk and resets the cache */
	    sts = advfs_getacl_bmt( &advfs_acl, &tuple_cnt, bfap );
	    if( sts != EOK ) {
		ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		u.u_error = sts;
		return sts;
	    }
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    ADVRWL_BFAP_BFA_ACL_DOWNGRADE( &bfap->bfa_acl_lock );

	} else {

	    /* The ACL cache is vaild */
	    tuple_cnt = bfap->bfa_acl_cache_len;
	    advfs_acl = bfap->bfa_acl_cache;
	}

    }

    /*
     * If num_tuples is greater than zero, fill in the acl buffer.
     * If num_tuples is zero, do not fill in the acl buffer
     */
    if( num_tuples > 0 ) {

	/*
	 * Error if number of tuples requested is less than number in the ACL
	 * (i.e. They sent us a buffer that's too small to fit the ACL in)
	 */
	if( num_tuples < tuple_cnt ) {
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    u.u_error = EINVAL;
	    return EINVAL;
	}

	/* plug the tuples into the acl buffer */
	for( i = 0; i < tuple_cnt; i++ ) {
	    acl[i].a_type = advfs_acl[i].aa_a_type;
	    acl[i].a_id = advfs_acl[i].aa_a_id;
	    acl[i].a_perm = advfs_acl[i].aa_a_perm;
	}
    }

    if( bfap->bfa_acl_cache == NULL )
	ms_free( advfs_acl );

    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );

    /*
     * The acl syscall is excpeting this return value in order to process
     * the acl buffer properly
     */
    u.u_rval2 = tuple_cnt;

    return EOK;
}


/*
 *  NAME:               advfs_access
 *
 *  DESCRIPTION:        This routine is the AdvFS specific callout for
 *                      VOP_ACCESS.  It will check the files permissions
 *			and ACL entries to determine if the user has access
 *			to the file.
 *
 *  ARGUMENTS:          vp              vnode of the file to check
 *			mode		requested mode of caller
 *			ucred		caller's credentials
 *
 *  RETURN VALUES:      SUCCESS         zero ( 0 )
 *			FAILURE		EACCES, EROFS, ETXTBSY, EINVAL
 */
int
advfs_access(
    struct vnode *vp,
    int mode,
    struct ucred *ucred )
{
    int m;
    mode_t fmode;
    struct fsContext *context_ptr;
    bfAccessT *bfap;

    /* Track the routine's access statistics */
    FILESETSTAT( vp, advfs_access );

    /* Get the fsContext info from the vnode */
    bfap = VTOA( vp );
    if( ( context_ptr=VTOC( vp ) ) == NULL ) {
	domain_panic( bfap->dmnP, "Could not get context structure" );
	u.u_error = EACCES;
	return EACCES;
    }

    /* Get this file's permission mode */
    fmode = context_ptr->dir_stats.st_mode;

    /* If the requested mode is WRITE, check a few things */
    if( mode & S_IWRITE ) {

	/*
	 * Do not allow write attempts on a read only file system, unless the
	 * file is a block or character device resident on the file system
	 */
	if( vp->v_vfsp->vfs_flag & VFS_RDONLY ) {
	    if( ( fmode & S_IFMT ) != S_IFCHR &&
	        ( fmode & S_IFMT ) != S_IFBLK ) {
		u.u_error = EROFS;
		return EROFS;
	    }
	}

	/*
	 * If there is shared text associated with the vnode, try to free it
	 * up once.  If we fail, we can not allow writing.
	 */
	if( vp->v_flag & VTEXT ) {
	    xrele( vp );
	    if( vp->v_flag & VTEXT ) {
		u.u_error = ETXTBSY;
		return ETXTBSY;
	    }
	}
    }

    /*
     * If you are the super user, you always get read/write access.  Also you
     * get execute access if it is a directory, or if any execute bit is set.
     */
    if( ( CR_EUID(TNC_CRED()) == 0 ) && ( ! ( mode & S_IEXEC ) ||
        ( ( fmode & S_IFMT ) == S_IFDIR ) || ( fmode & ANY_EXEC ) ) ) {
	return 0;
    }

    /* 
     * Now that we've gotten some preliminary checks out of the way, let's
     * move on to the Access Control List (ACL).
     * 
     * The routine advfs_bf_check_access will check the ACL and return the mode 
     * for the caller.  The mode will be a number between 0 and 7. Representing
     * the following permissions:
     *
     *	0 ---		4 r--
     *	1 --x		5 r-x
     *	2 -w-		6 rw-
     *	3 -wx		7 rwx
     *
     * If -1 is returned, then there was an error, return EINVAL
     */
    m = advfs_bf_check_access( vp, ucred );
    if( m == -1 ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /*
     * We're only interested in VREAD, VWRITE, and VEXEC bits.
     *
     * Since the mode returned will be denoted by 0-7, we'll also need to shift
     * the bits of the requested mode to compare it to our caller's mode
     * returned from advfs_bf_check_access.
     */
    mode &= ( VREAD | VWRITE | VEXEC );
    mode >>= 6;

    /* Grant access */
    if( ( m & mode ) == mode ) {
	return 0;
    }

    /* Deny access */
    u.u_error = EACCES;
    return EACCES;
}


/* ACL SUPPORT ROUTINES */

/*
 *  NAME:		advfs_setacl_int
 *
 *  DESCRIPTION:	This routine is called by the AdvFS specific callout
 *			for VOP_SETACL (advfs_setacl) as well as internal
 *			AdvFS routines that need to pass in a parent
 *			transation.  It will take a buffer containing ACL
 *			information of a file and store the information in
 *			the mcells.
 *
 *			This routine does not have an undo if the parent
 *			transaction is not Nil.
 *
 *  ARGUMENTS:		parent_ftx	Parent transaction
 *			vp		File to set ACL on
 *			num_tuples	Number of ACL entries requested
 *			tupleset	Buffer with the new ACL entries
 *			acl_type	ACL type, should be SYSV_ACLS
 *
 *  RETURN VALUES:	SUCCESS		EOK
 *			FAILURE		E_BAD_HANDLE, EIO, ENOSYS,
 * 					EINVAL, EROFS, ENOMEM
 */

int32_t
advfs_setacl_int(
    ftxHT parent_ftx,
    struct vnode *vp,
    int num_tuples,
    struct acl *tupleset,
    int acl_type )
{
    statusT sts;
    int i, imode, num_opt_tuples=0;
    int mcells_required, new_mcells, acls_left_to_write;
    int mcellList_lk_held;
    int acl_link_seg, mcells_requiring_undo, cur_undo_cnt,
        cur_mcell, acls_to_write, mcells_remaining;
    bfMCIdT cur_mcell_id, acl_mcell_start;
    bsMCT *mcell_ptr;
    advfs_acl_t *advfs_acl, *cur_acl_array_ptr;
    struct acl *tp;
    bfAccessT *bfap, *bmt_bfap;
    bfSetIdT bfSetId;
    bsMPgT *page_ptr=NULL;
    rbfPgRefHT pghdl;
    ftxHT ftx_acl_update, cur_ftx;
    char *undo_rec_ptr=NULL, *cur_undo_rec_ptr;
    bsMRT *header_ptr, *term_ptr;
    bsr_acl_rec_t *record_ptr;
    struct fs_stat *fsStats;
    struct fsContext *context_ptr;
    mcellPtrRecT freeMC[1];

    /* ASSERT that the SYSV_ACLS macro has not changed on us */
    MS_SMP_ASSERT( SYSV_ACLS == 2 );

    /* Verify ACL type requested */
    if( acl_type != ADVFS_ACL_TYPE ) {
        u.u_error = ENOSYS;
        return ENOSYS;
    }

    /* Convert vnode to bfAccess and get the bfSetId */
    bfap = VTOA( vp );
    bfSetId = VTOBNP( vp )->bfSetId;

    /* Filter metadata files that are not directories */
    if( ( bfap->dataSafety == BFD_METADATA ) && ( vp->v_type != VDIR ) ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /* If the file system is read only, advfs_setacl cannot be performed */
    if( vp->v_vfsp->vfs_flag & VFS_RDONLY ) {
        u.u_error = EROFS;
        return EROFS;
    }

    /* Check for domain panic */
    if( bfap->dmnP->dmn_panic ) {
        u.u_error = EIO;
        return EIO;
    }

    /* Must be the owner or su in order to do a setacl */
    context_ptr = VTOC( vp );
    if( ( CR_EUID(TNC_CRED()) != 0 ) && 
	( CR_EUID(TNC_CRED()) != context_ptr->dir_stats.st_uid ) ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /*
     * A request of less than zero or greater than ADVFS_MAX_ACL_ENTRIES
     * is invalid
     */
    if( ( num_tuples < 0 ) || ( num_tuples > ADVFS_MAX_ACL_ENTRIES ) ) {
	u.u_error = EINVAL;
	return EINVAL;
    }

    /*
     * Verify the ACL is ordered properly and it contains the 4 base entries
     * Also, make sure default entries are being set only on directories
     */
    sts = advfs_verify_acl( vp, tupleset, num_tuples );
    if( sts != EOK ) {
	u.u_error = EINVAL;
	return EINVAL;
    }


    /*
     * START TRANSACTION
     *
     * Guarantee the all of the next steps either happen or do not happen:
     * 1. New ACL entries are set or removed in the BMT
     * 2. BMT is updated with new base permissions
     * 3. Base mode bits are set
     * 4. ACL cache is cleared out and reset with new ACL
     */

     sts = FTX_START_N(	FTA_ACL_UPDATE,
			&ftx_acl_update,
			parent_ftx,
			bfap->dmnP );
    if( sts != EOK ) {
	u.u_error = sts;
	return sts;
    }

    /*
     * If any children may exist that don't have their own metadata, call
     * advfs_access_snap_children to COW the metadata and make it so we can
     * modify the ACLs without touching the children.
     */
    if( (bfap->bfaFlags & BFA_SNAP_CHANGE) ||
            (HAS_SNAPSHOT_CHILD( bfap ) && (bfap->bfaFirstChildSnap == NULL) ) ) {
        snap_flags_t snap_flags = SF_SNAP_NOFLAGS;
	advfs_access_snap_children( bfap, ftx_acl_update, &snap_flags );
    }

    freeMC[0].mcid = bsNilMCId;

    /* If num_tuples is zero, delete the ACL from the BMT */
    if( num_tuples == 0 ) {
	mcells_required = 0;
	new_mcells = 0;
	ADVRWL_BFAP_BFA_ACL_LOCK_WRITE( &bfap->bfa_acl_lock );
	ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
	mcellList_lk_held = TRUE;
	sts = advfs_acl_resize_mcells(	ftx_acl_update,
					bfap,
					mcells_required,
					&new_mcells,
					&acl_mcell_start,
					&freeMC[0] );
	if( sts != EOK ) {
	    ftx_fail( ftx_acl_update );
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    u.u_error = sts;
	    return sts;
	}

	MS_SMP_ASSERT( new_mcells == 0 );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );

	/* Remove the ACL cache if it exists */
	if( bfap->bfa_acl_cache != NULL ) {
	    ms_free( bfap->bfa_acl_cache );
	    bfap->bfa_acl_cache = NULL;
	    bfap->bfa_acl_cache_len = 0;

	    /* Set the bfaFlags to indicate there is no ACL on disk */
	    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	    bfap->bfaFlags |= BFA_ACL_NOT_ONDISK;
	    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
	} else {
	    /* If acl cache is NULL, BFA_ACL_NOT_ONDISK should be set */
	    MS_SMP_ASSERT( bfap->bfaFlags & BFA_ACL_NOT_ONDISK );
	}

	if( MCID_EQL( freeMC[0].mcid, bsNilMCId ) ) {
	    ftx_done_n( ftx_acl_update, FTA_ACL_UPDATE );
	} else {
	    ftx_done_urd( ftx_acl_update,
			  FTA_ACL_UPDATE,
			  0, 0,
			  sizeof( mcellPtrRecT ),
			  (void *)&freeMC[0] );
	}
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	return EOK;
    } /* end if num_tuples == 0 */

    /*
     * Initialize the new acl structure, ms_malloc space for the acl
     * based on the num_tuples size
     */
    advfs_acl = ( advfs_acl_t *)ms_malloc( sizeof( advfs_acl_t ) * num_tuples );

    /* Make a temporary copy of the tuple set */
    for( i=0, tp=tupleset; i<num_tuples; i++, tp++ ) {
	/* Are these valid tuples? */
	if( ( tp->a_id >= MAXUID || tp->a_id < 0 ) || ( tp->a_perm > 7 ) ) {
	    u.u_error = EINVAL;
	    ms_free( advfs_acl );
	    ftx_fail( ftx_acl_update );
	    return EINVAL;
	}

	/*
	 * BASE TUPLES:
	 * In the following section, we have found one of the base tuples
	 */
	if( tp->a_type == ADVFS_USER_OBJ ) {		/* Case: user base */

	    /* Set user base mode bits */
	    imode = ADVFS_SETBASEMODE( imode, tp->a_perm, ADVFS_ACL_USER );
	    advfs_acl[i].aa_a_type = tp->a_type;
	    advfs_acl[i].aa_a_id = tp->a_id;
	    advfs_acl[i].aa_a_perm = tp->a_perm;
	    continue;

	} else if( tp->a_type == ADVFS_GROUP_OBJ ) {	/* Case: group base */

	    /* Set the group base mode bits */
	    imode = ADVFS_SETBASEMODE( imode, tp->a_perm, ADVFS_ACL_GROUP );
	    advfs_acl[i].aa_a_type = tp->a_type;
	    advfs_acl[i].aa_a_id = tp->a_id;
	    advfs_acl[i].aa_a_perm = tp->a_perm;
	    continue;

	} else if( tp->a_type == ADVFS_CLASS_OBJ ) {	/* Case: class base */

	    advfs_acl[i].aa_a_type = tp->a_type;
	    advfs_acl[i].aa_a_id = tp->a_id;
	    advfs_acl[i].aa_a_perm = tp->a_perm;
	    continue;

	} else if( tp->a_type == ADVFS_OTHER_OBJ ) {	/* case: other base */

	    /* Set the other base mode bits */
	    imode = ADVFS_SETBASEMODE( imode, tp->a_perm, ADVFS_ACL_OTHER );
	    advfs_acl[i].aa_a_type = tp->a_type;
	    advfs_acl[i].aa_a_id = tp->a_id;
	    advfs_acl[i].aa_a_perm = tp->a_perm;
	    continue;
	}

	/*
	 * OPTIONAL TUPLE:
	 * In the following section we have found an optional tuple
	 */
	advfs_acl[i].aa_a_type = tp->a_type;
	advfs_acl[i].aa_a_id = tp->a_id;
	advfs_acl[i].aa_a_perm = tp->a_perm;
	num_opt_tuples++;

    } /* end for loop */

    /*
     * Take the bfa_acl_lock to protect the modification of the ACL.  This
     * lock is not aquired as part of the transaction, but must be dropped
     * after the ftx_done or ftx_fail.  When running recovery, the lock doesn't
     * need to be held since recovery is single threaded.  In the non-recovery
     * case, we will hold the lock until ftx_fail returns.
     */
    ADVRWL_BFAP_BFA_ACL_LOCK_WRITE( &bfap->bfa_acl_lock );

    acls_left_to_write = num_tuples;
    cur_acl_array_ptr = advfs_acl;
    mcellList_lk_held = FALSE;

    /*
     * There is no need to keep any ACL mcells if no optional tuples were
     * specified.  We'll change the mode bits and the BMT later for the
     * base tuples
     */
    if( num_opt_tuples == 0 ) {
	mcells_required = 0;
	new_mcells = 0;
	ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
	mcellList_lk_held = TRUE;
	sts = advfs_acl_resize_mcells(	ftx_acl_update,
					bfap,
					mcells_required,
					&new_mcells,
					&acl_mcell_start,
					&freeMC[0] );
	if( sts != EOK ) {
	    ms_free( advfs_acl );
	    ftx_fail( ftx_acl_update );
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    u.u_error = sts;
	    return sts;
	}

	MS_SMP_ASSERT( new_mcells == 0 );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	mcellList_lk_held = FALSE;

	/* Remove the ACL cache if it exists */
	if( bfap->bfa_acl_cache != NULL ) {
	    ms_free( bfap->bfa_acl_cache );
	    bfap->bfa_acl_cache = NULL;
	    bfap->bfa_acl_cache_len = 0;

	    /* Set bfaFlags to indicate there is no ACL on disk */
	    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	    bfap->bfaFlags |= BFA_ACL_NOT_ONDISK;
	    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
	} else {
	    /* If acl cache is NULL, BFA_ACL_NOT_ONDISK should be set */
	    MS_SMP_ASSERT( bfap->bfaFlags & BFA_ACL_NOT_ONDISK );
	}

    } else {

	mcells_required = ( num_tuples - 1 ) / ADVFS_ACLS_PER_MCELL + 1;
	new_mcells = 0;
	ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
	mcellList_lk_held = TRUE;
	sts = advfs_acl_resize_mcells(	ftx_acl_update,
					bfap,
					mcells_required,
					&new_mcells,
					&acl_mcell_start,
					&freeMC[0] );
	if( sts != EOK ) {
	    ms_free( advfs_acl );
	    ftx_fail( ftx_acl_update );
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    u.u_error = sts;
	    return sts;
	}

	/*
	 * If there are any new mcells for the ACL, then we must hold the lock
	 * for write.  If we don't hold the lock for write, another thread
	 * (wanting an mcell for something other than an ACL) might sneak in
	 * and assign a record in one of the new mcells we just allocated but
	 * haven't put a record in.
	 */
	if( new_mcells == 0 ) {
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    mcellList_lk_held = FALSE;
	}

	acl_link_seg = 0;
	sts = FTX_START_N( ACL_UPDATE_WITH_UNDO,
			   &cur_ftx,
			   ftx_acl_update,
			   bfap->dmnP );
	if( sts != EOK ) {
	    ms_free( advfs_acl );
	    ftx_fail( ftx_acl_update );
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	    u.u_error = sts;
	    return sts;
	}

	mcells_requiring_undo = mcells_required - new_mcells;

	undo_rec_ptr = (char *)ms_malloc(
		sizeof( acl_update_undo_hdr_t ) +
		( FTX_MX_PINP * FTX_MX_PINR * sizeof( acl_update_undo_t ) ) );
	cur_undo_rec_ptr = undo_rec_ptr + sizeof( acl_update_undo_hdr_t );
	cur_undo_cnt = 0;

	cur_mcell_id = acl_mcell_start;

	/*
	 * All the mcells that are being overwritten (weren't just allocated)
	 * need to be completely logged so the modifications can be undone.
	 */
	for( cur_mcell = 0; cur_mcell < mcells_requiring_undo; cur_mcell++ ) {
	    bmt_bfap = bfap->dmnP->vdpTbl[ cur_mcell_id.volume - 1 ]->bmtp;
	    sts = rbf_pinpg( &pghdl,
			     (void **)&page_ptr,
			     bmt_bfap,
			     cur_mcell_id.page,
			     BS_NIL,
			     cur_ftx,
			     MF_VERIFY_PAGE );
	    if( sts == E_MAX_PINS_EXCEEDED ) {
		MS_SMP_ASSERT( cur_undo_cnt >= FTX_MX_PINP );
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_undo_count =
		    cur_undo_cnt;
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_bf_set_id =
		    bfSetId;
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_bf_tag =
		    bfap->tag;
		ftx_done_u( cur_ftx,
			    FTA_ACL_UPDATE_WITH_UNDO,
			    ( sizeof( acl_update_undo_hdr_t ) +
				( FTX_MX_PINR * FTX_MX_PINR *
				sizeof( acl_update_undo_t ) ) ),
			    undo_rec_ptr );
		sts = FTX_START_N( FTA_ACL_UPDATE_WITH_UNDO,
				   &cur_ftx,
				   ftx_acl_update,
				   bfap->dmnP );
		if( sts != EOK ) {
		    ms_free( advfs_acl );
		    ms_free( undo_rec_ptr );
		    ftx_fail( ftx_acl_update );
		    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		    u.u_error = sts;
		    return sts;
		}
		cur_undo_rec_ptr =
		    undo_rec_ptr + sizeof( acl_update_undo_hdr_t );
		cur_undo_cnt = 0;

		sts = rbf_pinpg( &pghdl,
				 (void **)&page_ptr,
				 bmt_bfap,
				 cur_mcell_id.page,
				 BS_NIL,
				 cur_ftx,
				 MF_VERIFY_PAGE );
		if( sts != EOK ) {
		    /*
		     * We need to undo mcell list changes, so we need
		     * to hold the mcellList_lk for the fail case.
		     */
		    if( mcellList_lk_held == FALSE )
			ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
		    ftx_fail( cur_ftx );
		    ftx_fail( ftx_acl_update );
		    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		    ms_free( advfs_acl );
		    ms_free( undo_rec_ptr );
		    u.u_error = sts;
		    return sts;
		}
	    } /* end if sts == E_MAX_PINS_EXCEEDED */

	    if( !rbf_can_pin_record( pghdl, cur_ftx ) ) {
		MS_SMP_ASSERT( cur_undo_cnt >= FTX_MX_PINR );
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_undo_count =
		    cur_undo_cnt;
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_bf_set_id =
		    bfSetId;
		( (acl_update_undo_hdr_t *)undo_rec_ptr )->auuh_bf_tag =
		    bfap->tag;

		ftx_done_u( cur_ftx,
			    FTA_ACL_UPDATE_WITH_UNDO,
			    ( sizeof( acl_update_undo_hdr_t ) +
				( FTX_MX_PINR * FTX_MX_PINR *
				sizeof( acl_update_undo_t ) ) ),
			    undo_rec_ptr );
		sts = FTX_START_N( FTA_ACL_UPDATE_WITH_UNDO,
				   &cur_ftx,
				   ftx_acl_update,
				   bfap->dmnP );
		if( sts != EOK ) {
		    ftx_fail( ftx_acl_update );
		    ms_free( advfs_acl );
		    ms_free( undo_rec_ptr );
		    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		    u.u_error = sts;
		    return sts;
		}
		cur_undo_rec_ptr =
		    undo_rec_ptr + sizeof( acl_update_undo_hdr_t );
		cur_undo_cnt = 0;

		sts = rbf_pinpg( &pghdl,
				 (void **)&page_ptr,
				 bmt_bfap,
				 cur_mcell_id.page,
				 BS_NIL,
				 cur_ftx,
				 MF_VERIFY_PAGE );

		if( sts != EOK ) {
		    /*
		     * We need to undo mcell list changes, so we need
		     * to hold the mcellList_lk for the fail case.
		     */
		     if( mcellList_lk_held == FALSE ) 
			ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
		    ftx_fail( cur_ftx );
		    ftx_fail( ftx_acl_update );
		    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		    ms_free( advfs_acl );
		    ms_free( undo_rec_ptr );
		    u.u_error = sts;
		    return sts;
		}
	    } /* end if !rbf_can_pin_record */

	    /*
	     * We can go ahead and pin the record and log it for undo and
	     * modify it.
	     */
	    mcell_ptr = &( page_ptr->bsMCA[cur_mcell_id.cell] );
	    MS_SMP_ASSERT( ( (struct bsMR *)mcell_ptr->bsMR0 )->type ==
		BSR_ACL_REC );
	    record_ptr = (bsr_acl_rec_t *)( mcell_ptr->bsMR0 +
		sizeof( struct bsMR ) );
	    rbf_pin_record( pghdl,
			    (void *)record_ptr,
			    sizeof( bsr_acl_rec_t ) );

	    /*
	     * Setup the undo record for the changes we are about to make to
	     * the date in this record.  We will copy out the entire record
	     * prior to replacing the whole thing with a new record
	     */
	    ((acl_update_undo_t *)cur_undo_rec_ptr)->auu_mcell_id=cur_mcell_id;
	    bcopy( record_ptr,
		&((acl_update_undo_t *)cur_undo_rec_ptr)->auu_previous_record,
		sizeof( bsr_acl_rec_t ) );

	    cur_undo_rec_ptr += sizeof( acl_update_undo_t );
	    cur_undo_cnt++;

	    /*
	     * Get the current acl_link_seg so we can make sure to set the new
	     * mcells correctly.
	     */
	    acl_link_seg = record_ptr->bar_hdr.bar_acl_link_seg+1;
	    MS_SMP_ASSERT( record_ptr->bar_hdr.bar_acl_type == ADVFS_ACL_TYPE );
	    acls_to_write = MIN( ADVFS_ACLS_PER_MCELL, acls_left_to_write );
	    record_ptr->bar_hdr.bar_acl_cnt = acls_to_write;
	    record_ptr->bar_hdr.bar_acl_total_cnt = num_tuples;

	    /* Set the number of ACL entries in this record */
	    bcopy( cur_acl_array_ptr,
		   &((bsr_acl_rec_t *)record_ptr)->bar_acl,
		   acls_to_write * sizeof( advfs_acl_t ) );
	    acls_left_to_write -= acls_to_write;
	    cur_acl_array_ptr = (advfs_acl_t *)( (char *)cur_acl_array_ptr +
		( acls_to_write * sizeof( advfs_acl_t ) ) );

	    cur_mcell_id = mcell_ptr->mcNextMCId;
	} /* end for loop */

	/*
	 * We have now completed all the work for the mcells that needed to
	 * be logged.  cur_mcell_id is pointing to the next mcell in the
	 * ACL mcell chain, but now transactions are a little easier since
	 * we don't need to log undos (which really chew up space in the
	 * log :( )  We must complete the header for the undo_rec_ptr and
	 * finish the transation from above, then we wil start doing the new
	 * mcells in a new transaction.
	 */
	((acl_update_undo_hdr_t *)undo_rec_ptr)->auuh_undo_count = cur_undo_cnt;
	((acl_update_undo_hdr_t *)undo_rec_ptr)->auuh_bf_set_id = bfSetId;
	((acl_update_undo_hdr_t *)undo_rec_ptr)->auuh_bf_tag = bfap->tag;
	ftx_done_u( cur_ftx,
		    FTA_ACL_UPDATE_WITH_UNDO,
		    ( sizeof( acl_update_undo_hdr_t ) +
			( FTX_MX_PINP * FTX_MX_PINR *
			sizeof( acl_update_undo_t ) ) ),
		    undo_rec_ptr );

	/*
	 * Before we start any more transactions, make sure there is something
	 * to do.  Note that the acl lock is still held at this point and no one
	 * will change out contiguous range of ACL mcells.
	 */
	if( acls_left_to_write ) {
	    MS_SMP_ASSERT( new_mcells != 0 );
	    MS_SMP_ASSERT( mcellList_lk_held );
	    sts = FTX_START_N(  FTA_ACL_UPDATE_WITHOUT_UNDO,
				&cur_ftx,
				ftx_acl_update,
				bfap->dmnP );
	    if( sts != EOK ) {
		ftx_fail( ftx_acl_update );
		ms_free( advfs_acl );
		ms_free( undo_rec_ptr );
		ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		u.u_error = sts;
		return sts;
	    }

	    mcells_remaining = new_mcells;

	    /* cur_mcell_id was set to the current mcell by the previous loop */

	    /* Initialize all the remaining mcells to the new acl set */
	    for( cur_mcell = 0; cur_mcell < mcells_remaining; cur_mcell++ ) {
		MS_SMP_ASSERT( !MCID_EQL( cur_mcell_id, bsNilMCId ) );
		bmt_bfap = bfap->dmnP->vdpTbl[ cur_mcell_id.volume - 1 ]->bmtp;

		sts = rbf_pinpg( &pghdl,
				 (void **)&page_ptr,
				 bmt_bfap,
				 cur_mcell_id.page,
				 BS_NIL,
				 cur_ftx,
				 MF_VERIFY_PAGE );

		if( sts == E_MAX_PINS_EXCEEDED ) {
		    ftx_done_n( cur_ftx, FTA_ACL_UPDATE_WITHOUT_UNDO );
		    sts = FTX_START_N(  FTA_ACL_UPDATE_WITHOUT_UNDO,
					&cur_ftx,
					ftx_acl_update,
					bfap->dmnP );

		    if( sts != EOK ) {
			ftx_fail( ftx_acl_update );
			ms_free( advfs_acl );
			ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
			ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
			u.u_error = sts;
			return sts;
		    }

		    sts = rbf_pinpg( &pghdl,
				     (void **)&page_ptr,
				     bmt_bfap,
				     cur_mcell_id.page,
				     BS_NIL,
				     cur_ftx,
				     MF_VERIFY_PAGE );

		    if( sts != EOK ) {
			MS_SMP_ASSERT( mcellList_lk_held == TRUE );
			ftx_fail( cur_ftx );
			ftx_fail( ftx_acl_update );
			ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
			ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
			ms_free( advfs_acl );
			u.u_error = sts;
			return sts;
		    }
		} /* end if sts == E_MAX_PINS_EXCEEDED */
		if( !rbf_can_pin_record( pghdl, cur_ftx ) ) {
		    ftx_done_n( cur_ftx, FTA_ACL_UPDATE_WITHOUT_UNDO );
		    sts = FTX_START_N(  FTA_ACL_UPDATE_WITHOUT_UNDO,
					&cur_ftx,
					ftx_acl_update,
					bfap->dmnP );
		    if( sts != EOK ) {
			ftx_fail( ftx_acl_update );
			ms_free( advfs_acl );
			ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
			ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
			u.u_error = sts;
			return sts;
		    }

		    sts = rbf_pinpg( &pghdl,
				     (void **)&page_ptr,
				     bmt_bfap,
				     cur_mcell_id.page,
				     BS_NIL,
				     cur_ftx,
				     MF_VERIFY_PAGE );
		    if( sts != EOK ) {
			MS_SMP_ASSERT( mcellList_lk_held == TRUE );
			ftx_fail( cur_ftx );
			ftx_fail( ftx_acl_update );
			ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
			ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
			ms_free( advfs_acl );
			u.u_error = sts;
			return sts;
		    }
		} /* end if !rbf_can_pin_record */

		/* We can go ahead and pin the record and modify it */
		mcell_ptr = &( page_ptr->bsMCA[cur_mcell_id.cell] );
		/*
		 * Pin the header and the terminator too. The structure
		 * of this mcell is the mcell record header, ACL data record,
		 * and the mcell record terminator, where the header has a
		 * record type of BSR_ACL_REC and the terminator has a type
		 * of BSR_NIL
		 */
		rbf_pin_record( pghdl,
				mcell_ptr->bsMR0,
				2 * sizeof( struct bsMR ) +
				sizeof( bsr_acl_rec_t ) );

		/* Initialize header */
		header_ptr = (bsMRT *)mcell_ptr->bsMR0;
		MS_SMP_ASSERT( header_ptr->type == BSR_NIL );
		header_ptr->bCnt = sizeof( struct bsMR ) +
		sizeof( bsr_acl_rec_t );
		header_ptr->type = BSR_ACL_REC;

		/* Copy data portion of new record */
		record_ptr = (bsr_acl_rec_t *)( mcell_ptr->bsMR0 +
			     sizeof( struct bsMR ) );
		record_ptr->bar_hdr.bar_acl_link_seg = acl_link_seg;
		acl_link_seg++;
		record_ptr->bar_hdr.bar_acl_type = ADVFS_ACL_TYPE;
		acls_to_write = MIN( ADVFS_ACLS_PER_MCELL, acls_left_to_write );
		record_ptr->bar_hdr.bar_acl_total_cnt = num_tuples;
		record_ptr->bar_hdr.bar_acl_cnt = acls_to_write;
		bcopy(  cur_acl_array_ptr,
			&( ( bsr_acl_rec_t * )record_ptr )->bar_acl,
			acls_to_write * sizeof( advfs_acl_t ) );

		/* Initialize terminator */
		term_ptr = (bsMRT *)( mcell_ptr->bsMR0 +
			   sizeof( struct bsMR ) + sizeof( bsr_acl_rec_t ) );
		term_ptr->bCnt = 0;
		term_ptr->type = BSR_NIL;

		acls_left_to_write -= acls_to_write;
		cur_acl_array_ptr = (advfs_acl_t *)( (char *)cur_acl_array_ptr +
		    ( acls_to_write * sizeof( advfs_acl_t ) ) );
		cur_mcell_id = mcell_ptr->mcNextMCId;

	    } /* end for loop */

	    ftx_done_n( cur_ftx, FTA_ACL_UPDATE_WITHOUT_UNDO );

	} /* end if we have optional tuples */

	/*
	 * The ACL is now written out to the mcell records that it needs to be
	 * written to at least with respect to the log.  (The transactions are
	 * complete that describe the data in the mcells for the ACL).
	 */

    } /* end else - num_opt_tuples != 0 */


    /*
     * Get the primary mcell of the file and find the fs_stat record.
     * Then reset the base modes
     */
    sts = rbf_pinpg( &pghdl,
		     (void **)&page_ptr,
		     bfap->dmnP->vdpTbl[ bfap->primMCId.volume - 1 ]->bmtp,
		     bfap->primMCId.page,
		     BS_NIL,
		     ftx_acl_update,
		     MF_VERIFY_PAGE );
    if( sts != EOK ) {
	/*
	 * We may not need to undo mcell list changes, so we need to hold the
	 * mcellList_lk for the fail case.
	 */
	if( !mcellList_lk_held ) 
	    ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
	ftx_fail( ftx_acl_update );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	ms_free( advfs_acl );
	u.u_error = sts;
	return sts;
    }

    mcell_ptr = &( page_ptr->bsMCA[bfap->primMCId.cell] );

    /*
     * The mcellList_lk may already be held in write mode.  If we didn't take
     * it for write mode, we only need to take it for read now.
     */
    if( !mcellList_lk_held ) {
	ADVRWL_MCELL_LIST_READ( &(bfap->mcellList_lk ) );
	mcellList_lk_held = TRUE;
    }

    /*
     * If a parent transaction is passed in then we were called by
     * fs_create_file.  If the ftx_acl_update transaction fails, then
     * the file will be deleted, so there is no need for an undo on
     * the changes made to the stats structure.
     */

    fsStats = (struct fs_stat *)bmtr_find( mcell_ptr,
					   BMTR_FS_STAT,
					   bfap->dmnP );
    if( fsStats == NULL ) {
	/*
	 * We may need to undo mcell list changes, so we need to hold the
	 * mcellList_lk for the fail case.
	 */
	if( ( mcellList_lk_held == TRUE ) &&
	    !( rwlock_wrowned( &bfap->mcellList_lk.lock.rw ) ) )
	    if( !rwlock_upgrade( &bfap->mcellList_lk.lock.rw ) )
		/* upgrade failed.  Lock should be dropped */
		ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
		ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
	ftx_fail( ftx_acl_update );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	ms_free( advfs_acl );
	domain_panic( bfap->dmnP, "Could not get fs_stat from bmt" );
	u.u_error = ENOSYS;
	return ENOSYS;
    }

    /* Set the new mode - preserve the bits that don't need change */
    rbf_pin_record( pghdl, fsStats, sizeof( struct fs_stat ) );
    fsStats->st_mode &= ~00777;
    fsStats->st_mode |= imode & 00777;

    /* Set the mode - preserve the bits that don't need change */
    ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
    context_ptr->dir_stats.st_mode &= ~00777;
    context_ptr->dir_stats.st_mode |= imode & 00777;
    ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

    /* reset the ACL cache with the new acl if we have optional tuples */
    if( bfap->bfa_acl_cache != NULL ) {
	ms_free( bfap->bfa_acl_cache );
	bfap->bfa_acl_cache = NULL;
	bfap->bfa_acl_cache_len = 0;
    }
    if( num_opt_tuples != 0 ) {
	bfap->bfa_acl_cache = advfs_acl;
	bfap->bfa_acl_cache_len = num_tuples;

	/* Set bfaFlags to indicate there is an ACL on disk */
	ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	bfap->bfaFlags &= ~BFA_ACL_NOT_ONDISK;
	ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    } else {
	/* We can free this memory since we're not setting the acl cache */
	ms_free( advfs_acl );
    }

    /* END TRANSACTION */
    if( MCID_EQL( freeMC[0].mcid, bsNilMCId ) ) {
	ftx_done_n( ftx_acl_update, FTA_ACL_UPDATE );
    } else {
	ftx_done_urd( ftx_acl_update,
		      FTA_ACL_UPDATE,
		      0, 0,
		      sizeof( mcellPtrRecT ),
		      (void *)&freeMC[0] );
    }

    if( mcellList_lk_held )
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );

    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );

    if( undo_rec_ptr != NULL )
 	ms_free( undo_rec_ptr );

    return EOK;
} /* end advfs_setacl_int routine */

/*
 *  NAME:               advfs_verify_acl
 *
 *  DESCRIPTION:        This routine will insure that the acl buffer
 *			contains properly ordered ACL entries as well
 *			as containing all the base ACLS: user, group,
 *			class, and other.
 *
 *  ARGUMENTS:		vp		file we're working on.
 *			tupleset	buffer containing ACL entries to check
 *			num_tuples	number of ACL entries in the buffer
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *			FAILURE		EINVAL
 */

static
statusT
advfs_verify_acl(
    struct vnode *vp,
    struct acl *tupleset,
    int num_tuples )
{
    int verify_flag=0, i;

    /*
     * VERIFY BASE USER
     *
     * Base user should be the very first entry, if not, then we're already
     * in violation of the order, return error.
     */
    if( tupleset[ADVFS_USER_BASE_IDX].a_type != USER_OBJ ) {
	return EINVAL;
    } else {
	verify_flag = verify_flag | VERIFY_USER;
    }

    /*
     * VERIFY OPTIONAL USERS
     *
     * Loop until we hit the base Group
     */
    for( i=1; i<num_tuples && tupleset[i].a_type != GROUP_OBJ; i++ ) {
	/*
	 * If we hit one of these before the Base group, then we're in violation
	 * of the order
	 */
	if( ( tupleset[i].a_type == USER_OBJ ) ||
	    ( tupleset[i].a_type == GROUP ) ||
	    ( tupleset[i].a_type == CLASS_OBJ ) ||
	    ( tupleset[i].a_type == OTHER_OBJ ) ) {
	    return EINVAL;
	}
    }

    /*
     * VERIFY BASE GROUP
     *
     * Make sure we stopped on the base group
     */
    if( tupleset[i].a_type != GROUP_OBJ ) {
	return EINVAL;
    } else {
	verify_flag = verify_flag | VERIFY_GROUP;
    }

    /*
     * VERIFY OPTIONAL GROUPS
     *
     * Loop until we hit the base class
     */
    i++;
    for( ; i<num_tuples, tupleset[i].a_type != CLASS_OBJ; i++ ) {
	/*
	 * If we hit one of these before the Base class, then we're in
	 * violation of the order return error
	 */
	if( ( tupleset[i].a_type == USER_OBJ ) ||
	    ( tupleset[i].a_type == USER ) ||
	    ( tupleset[i].a_type == GROUP_OBJ ) ||
	    ( tupleset[i].a_type == OTHER_OBJ ) ) {
	    return EINVAL;
	}
    }

    /*
     * VERIFY BASE CLASS
     *
     * make sure we stopped on the base class
     */
    if( tupleset[i].a_type != CLASS_OBJ ) {
	return EINVAL;
    } else {
	verify_flag = verify_flag | VERIFY_CLASS;
    }

    /*
     * VERIFY BASE OTHER
     *
     * Next one should be base other
     */
    i++;
    if( tupleset[i].a_type != OTHER_OBJ ) {
	return EINVAL;
    } else {
	verify_flag = verify_flag | VERIFY_OTHER;
    }

    /*
     * If there are any other entries, they must be default entires and this
     * must be a directory
     */
    i++;
    if( i < num_tuples ) {
	if( vp->v_type != VDIR ) {
	    return ENOTDIR;
	} else {
	    /*
	     * Make sure only default entries are left in the buffer
	     * The setacl command already verifies that the correct
	     * number of default entries are specified, we're just
	     * verifying the order
	     */
	    for( ; i<num_tuples; i++ ) {
		if( !( tupleset[i].a_type & ADVFS_ACL_DEFAULT ) ) {
		    return EINVAL;
		}
	    }
	}
    }

    /* Make sure we got all the base entries */
    if( verify_flag != VERIFY_ALL ) {
	return EINVAL;
    }

    return EOK;
}

/*
 *  NAME:               advfs_acl_register_ftx_agents
 *
 *  DESCRIPTION:        This routine is called from bs_misc.c:bs_init() to
 *			register ACL transaction agents
 *
 *  ARGUMENTS:          none
 *
 *  RETURN VALUES:      SUCCESS         returns
 *                      FAILURE         panic if there are failures with
 *					ftx_register_agent_n2.
 */

void
advfs_acl_register_ftx_agents()
{
    statusT sts;

    sts = ftx_register_agent_n2( FTA_ACL_UPDATE,
				 NULL,
				 &free_mcell_chains_opx,
				 NULL,
				 &free_mcell_chains_opx );

    if (sts != EOK) {
	ADVFS_SAD1( "advfs_acl_register_ftx_agents: register failure", sts );
    }

    sts = ftx_register_agent( FTA_ACL_UPDATE_WITH_UNDO,
			      &advfs_acl_update_undo,
			      NULL );

    if (sts != EOK) {
	ADVFS_SAD1( "advfs_acl_register_ftx_agents: register failure", sts );
    }

    sts = ftx_register_agent( FTA_ACL_UPDATE_WITHOUT_UNDO,
			      NULL,
			      NULL );

    if (sts != EOK) {
	ADVFS_SAD1( "advfs_acl_register_ftx_agents: register failure", sts );
    }

} /* advfs_acl_register_ftx_agents */

/*
 *  NAME:               advfs_acl_resize_mcells
 *
 *  DESCRIPTION:        This routine adjusts the number of mcells dedicated
 *			towards the ACL.  The mcells will be in a contiguous
 *			chain, therefore, when mcells need to be added, the will
 *			be added to the end of the already existing chain.  When
 *			mcells need to be removed, the previous mcell will be
 *			unlinked from the left over mcells.  The left over
 *			mcells will be added to the free list as a continuation
 *			from the parent transaction.
 *
 *			The Link segment for the ACL mcells will start with the
 *			next available link segment, and repeat this number
 *			throughout the chain.  Any non-ACL mcells added after
 *			the ACL mcell chain will continue to increment.
 *
 *			Assumes both bfa_acl_lock and mcellList_lk are both
 *			held in WRITE mode.
 *
 *  ARGUMENTS:          parent_ftx	parent trasnaction id
 *			bfap		bfAccess struct of the file
 *			mcells_required	number of mcells required to the ACL
 *			new_mcells	number of new mcells required
 *			acl_mcell_start	starting point of the ACL mcells
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *                      FAILURE         Error if bmt_unlink_mcells(),
 *					allocate_link_new_mcell(), or
 *					reading pages of BMT fails.
 */

static
statusT
advfs_acl_resize_mcells(
    ftxHT parent_ftx, 
    bfAccessT* bfap,
    int mcells_required,
    int *new_mcells,
    bfMCIdT *acl_mcell_start,
    mcellPtrRecT *freeMC )
{
    bfMCIdT next_mcell_id = bfap->primMCId,
	    prev_mcell_id = bsNilMCId,
	    last_acl_mcell = bsNilMCId,
	    new_mcell_id,			/* new mcells to the chain */
	    unlink_start_mcell,			/* beg of unlink chain */
	    unlink_end_mcell;			/* end of unlink chain */
    int acl_mcell_cnt=0, page_refed=FALSE;
    uint32_t linkSeg=0;
    statusT sts;
    bfPageRefHT page_ref;
    bsMCT *mcell_ptr=NULL;

    *new_mcells = 0;
    *acl_mcell_start = bsNilMCId;

    /*
     * The following loop sets up the pointers and counts to see if we need
     * to add or remove or do nothing with the ACL mcell chain.
     */
    while( ( !MCID_EQL( next_mcell_id, bsNilMCId ) ) &&
	   ( acl_mcell_cnt <= mcells_required ) ) {
	/* Ref the next mcell and get the mcell pointer */
	sts = advfs_bmtr_mcell_refpg( bfap, 
				      next_mcell_id,
				      &mcell_ptr,
				      &page_ref );
	if( sts != EOK )
		return sts;
	page_refed = TRUE;

	if( bmtr_find( mcell_ptr, BSR_ACL_REC, bfap->dmnP ) != NULL ) {
	    /*
	     * found an ACL record, so we are within the contiguous ACL
	     * chain at this point
	     */
	    last_acl_mcell = next_mcell_id;

	    if( MCID_EQL( *acl_mcell_start, bsNilMCId ) ) {
		*acl_mcell_start = next_mcell_id;
		linkSeg = mcell_ptr->mcLinkSegment;
	    }

	    acl_mcell_cnt++;

	    /*
	     * We don't want to increment the previous mcell at this point 
	     * if we're about to break the loop
	     */
	    if( acl_mcell_cnt > mcells_required ) {
		next_mcell_id = mcell_ptr->mcNextMCId;
		if( page_refed ) {
		    bs_derefpg( page_ref, BS_CACHE_IT );
		    page_refed = FALSE;
		}
		break;
	    }

	    if( page_refed ) {
		bs_derefpg( page_ref, BS_CACHE_IT );
		page_refed = FALSE;
	    }
	} else if( !MCID_EQL( last_acl_mcell, bsNilMCId ) ) {
	    /* done with chain of ACL mcells required for this ACL */
	    if( page_refed ) {
		bs_derefpg( page_ref, BS_CACHE_IT );
		page_refed = FALSE;
	    }
	    break;
	}

	prev_mcell_id = next_mcell_id;
	next_mcell_id = mcell_ptr->mcNextMCId;

	if( page_refed ) {
	    bs_derefpg( page_ref, BS_CACHE_IT );
	    page_refed = FALSE;
	}
    }

    /* Append more acl mcells to contiguous chain */
    if( mcells_required > acl_mcell_cnt ) {

	if( MCID_EQL( last_acl_mcell, bsNilMCId ) ) {
	    /* If there is not chain already, start at the 
	     * end of the mcell chain. */
	    last_acl_mcell = prev_mcell_id;
	}
		
	while( acl_mcell_cnt + *new_mcells < mcells_required ) {

	    /*
	     * allocate_link_new_mcell will start one transaction
	     * for the mcell that is allocated and linked and may
	     * start additional transactions for a bmt_extend call.
	     */
	    if( linkSeg ) {
		sts = allocate_link_new_mcell(  bfap,
						last_acl_mcell,
						parent_ftx,
						&new_mcell_id,
						BMT_NORMAL_MCELL,
						&linkSeg );
	    } else {
		sts = allocate_link_new_mcell(  bfap,
						last_acl_mcell,
						parent_ftx,
						&new_mcell_id,
						BMT_NORMAL_MCELL,
						NULL );
	    }
	    if( sts != EOK )
		return sts;

	    last_acl_mcell = new_mcell_id;
	    if( MCID_EQL( *acl_mcell_start, bsNilMCId ) ) {
		*acl_mcell_start = new_mcell_id;
		/* mcell_ptr is the last non-ACL mcell, add 1 to linkSegment */
		linkSeg = mcell_ptr->mcLinkSegment+1;
	    }

	    (*new_mcells)++;
	}

    } else  {

	/* Remove any trailing acl mcells */
	if( MCID_EQL( last_acl_mcell, bsNilMCId ) ) {
	    /* There was no ACL found and none were added.*/
	    MS_SMP_ASSERT( mcells_required == 0);
	    return EOK;
	} else if( ( MCID_EQL( next_mcell_id, bsNilMCId ) ) &&
		   ( mcells_required == acl_mcell_cnt ) ) {
	    /* We don't have any trailing mcells to remove */
	    return EOK;
	}

	/* Get the start and end of the unlink chain */
	unlink_start_mcell = last_acl_mcell;

	sts = advfs_bmtr_mcell_refpg( bfap,
				      unlink_start_mcell,
				      &mcell_ptr,
				      &page_ref );
	if( sts != EOK )
	    return sts;
	page_refed = TRUE;

	/* Find the end of the unlink chain */
	while( bmtr_find( mcell_ptr, BSR_ACL_REC, bfap->dmnP ) ) {

	    if( page_refed ) {
		bs_derefpg ( page_ref, BS_CACHE_IT );
		page_refed = FALSE;
	    }

	    if( MCID_EQL( next_mcell_id, bsNilMCId ) ) {
		/* We are at the end of the chain, must be done */
		unlink_end_mcell = last_acl_mcell;
		break;

	    } else {
		/* We may need to continue.  Ref the next mcell in the chain */
		sts = advfs_bmtr_mcell_refpg( bfap,
					      next_mcell_id,
					      &mcell_ptr,
					      &page_ref );
		if (sts != EOK)
		    return sts;
		page_refed = TRUE;
	    }
	    last_acl_mcell = next_mcell_id;
	    next_mcell_id = mcell_ptr->mcNextMCId;
	} /* end while */

	/*
	 * We will unlink the previous mcell from the first unecessary ACL mcell
	 * and reattach it after the last unecessary ACL mcell.
	 *
	 * Once the unlink ACL mcell chain is seperated from the file's primary
	 * mcell chain, we can set the freeMC structure for the continuation
	 * that will add the left over mcells back on the free list after the
	 * parent transaction is complete.
	 *
	 * bmt_unlink_mcells will start a transaction to unlink the mcell.
	 */
	sts = bmt_unlink_mcells( bfap->dmnP,
				 bfap->tag,
				 prev_mcell_id,
				 unlink_start_mcell,
				 unlink_end_mcell,
				 parent_ftx,
				 NEXTMCID );
	if( sts != EOK )
	    return sts;

	/* Set the freeMC structure for the continuation */
	(*freeMC).mcid = unlink_start_mcell;
    }

    if( page_refed )
	bs_derefpg( page_ref, BS_CACHE_IT );

    return EOK;
}


/*
 *  NAME:               advfs_acl_update_undo
 *
 *  DESCRIPTION:        This routine does the undo for modifications to mcells.
 *			This routine is only required when the mcell being
 *			modified is not newly allocated.
 *
 *  ARGUMENTS:         ftx		transaction handle
 *		       undo_rec_size	Size of the undo records
 *		       undo_record_ptr	data that needs replacing in the mcell
 *
 *  RETURN VALUES:     SUCCESS		returns
 *                     FAILURE		Initiates domain panic to file system
 */

static
void
advfs_acl_update_undo(
    ftxHT ftx,
    int32_t undo_rec_size,
    void *undo_record_ptr )
{
    acl_update_undo_hdr_t acl_undo_hdr;
    acl_update_undo_t cur_undo_rec;
    int cur_undo_index;
    statusT sts;
    bfSetT *bfSetp;
    struct vnode *nullvp;
    rbfPgRefHT pghdl;
    bsMPgT *page_ptr=NULL;
    bfAccessT *bfap, *bmt_bfap;
    bsMCT *mcell_ptr;
    bsr_acl_rec_t *record_ptr;

    bcopy( undo_record_ptr, &acl_undo_hdr, sizeof( acl_update_undo_hdr_t ) );

    /* get the bitfile-set and the bfap */
    sts = bfs_open( &bfSetp,
		    acl_undo_hdr.auuh_bf_set_id,
		    BFS_OP_DEF,
		    ftx );
    if( sts != EOK ) {
        domain_panic( ftx.dmnP, "advfs_acl_update_undo: bfs_open failed" );
	return;
    }

    sts = bs_access( &bfap,
		     acl_undo_hdr.auuh_bf_tag,
		     bfSetp,
		     ftx,
		     BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX,
		     &nullvp);

    if( sts != EOK ) {
	bs_bfs_close( bfSetp, ftx, BFS_OP_DEF );
        domain_panic( ftx.dmnP, "advfs_acl_update_undo: bs_access() failed" );
	return;
    }

    /* If we're not in recovery mode, ASSERT we have the locks in write mode */
    if( bfap->dmnP->state == BFD_ACTIVATED ) {
	MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->bfa_acl_lock,
				       RWL_WRITELOCKED ) );
	MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->mcellList_lk.lock.rw,
				       RWL_WRITELOCKED ) );
    }

    undo_record_ptr = (char *)undo_record_ptr + sizeof( acl_update_undo_hdr_t );

    for( cur_undo_index = 0; cur_undo_index < acl_undo_hdr.auuh_undo_count;
	 cur_undo_index++ ) {

	bcopy( undo_record_ptr, &cur_undo_rec, sizeof( acl_update_undo_t ) );
	undo_record_ptr = (char *)undo_record_ptr + sizeof( acl_update_undo_t );
	/* 
 	 * Pin the mcell in question
	 */ 
	bmt_bfap =
	    bfap->dmnP->vdpTbl[ cur_undo_rec.auu_mcell_id.volume - 1 ]->bmtp;
	sts = rbf_pinpg( &pghdl,
			 (void **)&page_ptr,
			 bmt_bfap,
			 cur_undo_rec.auu_mcell_id.page,
			 BS_NIL,
			 ftx,
			 MF_VERIFY_PAGE );
	if (sts != EOK)
	    domain_panic( ftx.dmnP, "advfs_acl_update_undo: rbf_pinpg failed" );

	mcell_ptr = &( page_ptr->bsMCA[cur_undo_rec.auu_mcell_id.cell] );
	record_ptr = (bsr_acl_rec_t *)( mcell_ptr->bsMR0 +
		     sizeof(struct bsMR ) );
	rbf_pin_record( pghdl,
			(void *)record_ptr,
			sizeof( bsr_acl_rec_t ) );
	bcopy( &cur_undo_rec.auu_previous_record, record_ptr,
	    sizeof( bsr_acl_rec_t ) );
    }
	
    if( bfap->bfa_acl_cache != NULL ) {
	ms_free( bfap->bfa_acl_cache );
	bfap->bfa_acl_cache = NULL;
	bfap->bfa_acl_cache_len = 0;
    }

    bs_close( bfap, MSFS_CLOSE_NONE );
    bs_bfs_close( bfSetp, ftx, BFS_OP_DEF );

    return;
}


/*
 *  NAME:               advfs_getacl_bmt
 *
 *  DESCRIPTION:        Support routine for advfs_getacl.  It will go to disk
 *			to look for the ACL and fills in the buffer with the
 *			new ACL.
 *
 *			Assumes bfa_acl_lock is held in WRITE mode and
 *			mcellList_lk is held in READ mode.
 *			
 *
 *  ARGUMENTS:          advfs_acl	buffer for ACL
 *			tuple_cnt	number of tuples requested
 *			bfap		Current File's bfAccess structure
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *			FAILURE		Will ASSERT if the proper locks are
 *					not held.
 */

static
statusT
advfs_getacl_bmt(
    advfs_acl_t **advfs_acl,	/* out - buffer for ACL */
    int *tuple_cnt,		/* out - number of tuples */
    bfAccessT *bfap )		/* in/out - current file */
{
    advfs_acl_t *advfs_aclp, *cur_advfs_aclp;
    statusT sts;
    int page_refed = FALSE;
    bsr_acl_rec_t *record;
    bfMCIdT mcell_id;
    bsMCT *mcell_ptr;
    vdT *vdp;
    bfPageRefHT pgRef;
    struct fsContext *context_ptr;
    uint64_t fmode;
    uint32_t recOffset;
    uint16_t rSize;
    struct vnode *vp;

    /*
     * Make sure we have the acl lock for writing and
     * the mcell list lock for reading
     */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->bfa_acl_lock,
	RWL_WRITELOCKED ) );
    MS_SMP_ASSERT( !ADVFS_SMP_RW_LOCK_EQL( &bfap->mcellList_lk.lock.rw,
	RWL_UNLOCKED ) );

    *tuple_cnt = 0;

    /*
     * If the bfaFlags indicates we don't have an ACL on disk, then we can skip
     * a trip to disk and set the acl with the base entries
     * 
     * However, the first time we enter this routine, this flag will not be
     * set because it is initialized to zero.  This will force us to go to disk
     * the first time and verify there are no ACLs on disk.
     */
    if( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) {
	goto _not_ondisk;
    }

    /*
     * Get the ACL from the BSR_ACL_REC record in the BMT
     * Scan the mcells until we find the first one, then loop
     * for the rest of them
     */
    vdp  = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    mcell_id = bfap->primMCId;
    sts = bmtr_scan_mcells( &mcell_id,
			    &vdp,
			    (void *)&record,
			    &pgRef,
			    &recOffset,
			    &rSize,
			    BSR_ACL_REC,
			    bfap->tag );
    if( sts == EOK ) {

	/*
	 * We found the first mcell containing a BSR_ACL_REC, if anymore
	 * exist, they will be contiguous
	 */
	bs_derefpg( pgRef, BS_CACHE_IT );
	/* Get the mcell pointer for this mcell id */
	sts = advfs_bmtr_mcell_refpg( bfap,
				      mcell_id,
				      &mcell_ptr,
				      &pgRef );
	if( sts != EOK )
	    return sts;
	advfs_aclp = (advfs_acl_t *)ms_malloc_no_bzero( sizeof( advfs_acl_t ) *
	    record->bar_hdr.bar_acl_total_cnt );
	cur_advfs_aclp = advfs_aclp;
	do {
	    *tuple_cnt += record->bar_hdr.bar_acl_cnt;
	    bcopy( &(record->bar_acl),
		   cur_advfs_aclp,
		   ( sizeof( advfs_acl_t ) * record->bar_hdr.bar_acl_cnt ) );
	    cur_advfs_aclp = (advfs_acl_t *)( (char *)cur_advfs_aclp +
		( sizeof( advfs_acl_t ) * record->bar_hdr.bar_acl_cnt ) );
	    bs_derefpg( pgRef, BS_CACHE_IT );
	    page_refed = FALSE;
	    mcell_id = mcell_ptr->mcNextMCId;
	    if( mcell_id.volume == bsNilMCId.volume )
		break;
	    sts = advfs_bmtr_mcell_refpg( bfap,
					  mcell_id,
					  &mcell_ptr,
					  &pgRef );
	    page_refed = TRUE;
	    if( sts != EOK ) {
		ms_free( advfs_aclp );
		return sts;
	    }
	    record = bmtr_find( mcell_ptr, BSR_ACL_REC, bfap->dmnP );

	} while( record != NULL );

	/* reset the acl cache with the new ACL */
	MS_SMP_ASSERT( bfap->bfa_acl_cache == NULL );
	bfap->bfa_acl_cache = advfs_aclp;
	bfap->bfa_acl_cache_len = *tuple_cnt;

	/* bfaFlags should not have BFA_ACL_NOT_ONDISK set in this code path */
	MS_SMP_ASSERT( !( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) );

    } else {

	/*
	 * There were definitly no ACLs on disk, so we can set the bfaFlags
	 * to indicate this.  We are setting this flag before the goto label
	 * because we already know this flag is set if we're in the label.
	 */
	ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	bfap->bfaFlags |= BFA_ACL_NOT_ONDISK;
	ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

_not_ondisk:

	/*
	 * There was no BSR_ACL_REC.  Get the base modes from the stat
	 * structure
	 */
	vp = ATOV( bfap );
	context_ptr = VTOC( vp );
	ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
	fmode = context_ptr->dir_stats.st_mode;
	ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

	/* Malloc space for the base tuples in the ACL entry */
	advfs_aclp = (advfs_acl_t *)ms_malloc_no_bzero( sizeof( advfs_acl_t ) *
	    ADVFS_NUM_BASE_ENTRIES );
	*tuple_cnt = ADVFS_NUM_BASE_ENTRIES;

	/* Input the base user entry */
	advfs_aclp[ADVFS_USER_BASE_IDX].aa_a_type = ADVFS_USER_OBJ;
	advfs_aclp[ADVFS_USER_BASE_IDX].aa_a_id = 0;
	advfs_aclp[ADVFS_USER_BASE_IDX].aa_a_perm =
	ADVFS_GETBASEMODE( context_ptr->dir_stats.st_mode, ADVFS_ACL_USER );

	/* Input the base group entry */
	advfs_aclp[ADVFS_GROUP_BASE_IDX].aa_a_type = ADVFS_GROUP_OBJ;
	advfs_aclp[ADVFS_GROUP_BASE_IDX].aa_a_id = 0;
	advfs_aclp[ADVFS_GROUP_BASE_IDX].aa_a_perm =
	ADVFS_GETBASEMODE( context_ptr->dir_stats.st_mode, ADVFS_ACL_GROUP);

	/* Input the base class entry - which has the same mode as group */
	advfs_aclp[ADVFS_CLASS_BASE_IDX].aa_a_type = ADVFS_CLASS_OBJ;
	advfs_aclp[ADVFS_CLASS_BASE_IDX].aa_a_id = 0;
	advfs_aclp[ADVFS_CLASS_BASE_IDX].aa_a_perm =
	ADVFS_GETBASEMODE( context_ptr->dir_stats.st_mode, ADVFS_ACL_GROUP);

	/* Input the base other entry */
	advfs_aclp[ADVFS_OTHER_BASE_IDX].aa_a_type = ADVFS_OTHER_OBJ;
	advfs_aclp[ADVFS_OTHER_BASE_IDX].aa_a_id = 0;
	advfs_aclp[ADVFS_OTHER_BASE_IDX].aa_a_perm =
	ADVFS_GETBASEMODE( context_ptr->dir_stats.st_mode, ADVFS_ACL_OTHER);
    }

    *advfs_acl = advfs_aclp;

    if( page_refed )
	bs_derefpg( pgRef, BS_CACHE_IT );

    return EOK;
}


/*
 *  NAME:               advfs_bf_check_access
 *
 *  DESCRIPTION:        This routine will perform a more detailed check on
 *			permissions by looking at the ACL
 *
 *  ARGUMENTS:          vp		vnode of the file to check
 *			ucred		caller's credentials
 *
 *  RETURN VALUES:      SUCCESS         Permissions ( 0-7 )
 *                      FAILURE         Negative one ( -1 )
 */

static
int32_t
advfs_bf_check_access(
    struct vnode *vp,
    struct ucred *ucred )
{
    int32_t uid = ucred->cr_uid;	/* the caller's uid */
    int32_t gid = ucred->cr_gid;	/* the caller's gid */
    advfs_acl_t *advfs_acl = NULL;	/* ACL from cache */
    struct acl *acl;			/* ACL from getacl() */
    int i, tuple_cnt=0, acl_buf_size=0, match=FALSE;
    statusT sts;
    struct fsContext *context_ptr;
    bfAccessT *bfap;
    uid_t fuid;
    uid_t fgid;
    mode_t fmode, class_mode;

    /* Check the v_type, if it is not one of the following, deny access */
    switch( vp->v_type ) {
	case VDIR:
	case VREG:
	case VBLK:
	case VCHR:
	case VFIFO:
	case VSOCK:
	case VLNK:
	    break;
	default:
	    /* access denied */
	    return -1;
    }

    context_ptr = VTOC( vp );
    bfap = VTOA( vp );

    /* get the file's uid, gid, and base permissions */
    ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
    fuid = context_ptr->dir_stats.st_uid;
    fgid = context_ptr->dir_stats.st_gid;
    fmode = context_ptr->dir_stats.st_mode;
    ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

    /* Check if the caller's uid is the file's uid */
    if( fuid == uid )
	return ADVFS_GETBASEMODE( fmode, ADVFS_ACL_USER );

    /*
     * To get the current ACL for this file (acl), we'll check the acl
     * cache in the bfAccess structure.  This will eliminate trip to disk to get
     * the info.  If the cache does not exist, then we will call advfs_getacl
     * which also returns the base permissions in addition to the optional
     * ones.  The ACL count will be located in u.u_r.r_val1, making life much
     * easier for this routine.
     */
    if( bfap->bfa_acl_cache == NULL ) {

_tryagain:
	acl = NULL;

	/* Get the acl */
	acl = (struct acl *)ms_malloc(
	    sizeof( struct acl ) * ADVFS_MAX_ACL_ENTRIES );
	sts = advfs_getacl( vp,
			    ADVFS_MAX_ACL_ENTRIES,
			    (struct acl_tuple_user *)acl,
			    ADVFS_ACL_TYPE );
	if( sts != EOK ) {
	    ms_free( acl );
	    return -1;
	}
	tuple_cnt = u.u_rval2;
	MS_SMP_ASSERT( tuple_cnt > 0 );

    } else {

	ADVRWL_BFAP_BFA_ACL_LOCK_READ( &bfap->bfa_acl_lock );

	/* Woops, we lost the acl cache while trying to grab the lock. */
	if( bfap->bfa_acl_cache == NULL ) {

	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    goto _tryagain;

	} else {

	    /* The acl cache is valid, we have an advfs version of the acl */
	    advfs_acl = bfap->bfa_acl_cache;
	    tuple_cnt = bfap->bfa_acl_cache_len;
	}
    }

    /*
     * If advfs_acl is NULL, then there was no AdvFS ACL cache, and
     * we will use the aclv.h acl data structure returned from advfs_getacl
     * to check the permissions.  Otherwise, we will use the advfs_acl data
     * structure based on the cache
     */
    if( advfs_acl != NULL ) {

	/* Get the class entry */
	for( i=0; i<tuple_cnt; i++ ) {
	    if( advfs_acl[i].aa_a_type == ADVFS_CLASS_OBJ ) {
		class_mode = advfs_acl[i].aa_a_perm;
		break;
	    }
	}

	/*
	 * Loop through the user specified ids to see if there's an optional
	 * entry for the caller's uid, break if we hit the group base
	 */
	for( i=0; i<tuple_cnt, advfs_acl[i].aa_a_type != ADVFS_GROUP_OBJ; i++ ){
	    if( ( advfs_acl[i].aa_a_type == ADVFS_USER ) &&
		( advfs_acl[i].aa_a_id == uid ) ) {
		ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		return( advfs_acl[i].aa_a_perm & class_mode );
	    }
	}

	/* Check if the caller's gid is the file's gid */
	if( fgid == gid ) {
	    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	    return ADVFS_GETBASEMODE( fmode, ADVFS_ACL_GROUP );
	}

	/*
	 * Loop through the group specified ids to see if there's an optional
	 * entry for the caller's gid, break if we hit the other base.  There's
	 * no need to reset i, since the acl is sorted.
	 */
	for( ; i<tuple_cnt, advfs_acl[i].aa_a_type != ADVFS_OTHER_OBJ; i++ ) {
	    if( ( advfs_acl[i].aa_a_type == ADVFS_GROUP ) &&
		( advfs_acl[i].aa_a_id == gid ) ) {
		ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
		return( advfs_acl[i].aa_a_perm & class_mode );
	    }
	}

	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );

    } else {

	/* Use the aclv.h structure instead since we had no advfs ACL cache */
	for( i=0; i<tuple_cnt; i++ ) {
	    if( acl[i].a_type == CLASS_OBJ ) {
		class_mode = acl[i].a_perm;
		break;
	    }
	}

	for( i=0; i<tuple_cnt, acl[i].a_type != GROUP_OBJ; i++ ) {
	    if( ( acl[i].a_type == USER ) &&
		( acl[i].a_id == uid ) ) {
		ms_free( acl );
		return( acl[i].a_perm & class_mode );
	    }
	}

	if( fgid == gid ) {
	    ms_free( acl );
	    return ADVFS_GETBASEMODE( fmode, ADVFS_ACL_GROUP );
	}

	for( ; i<tuple_cnt, acl[i].a_type != OTHER_OBJ; i++ ) {
	    if( ( acl[i].a_type == GROUP ) &&
		( acl[i].a_id == gid ) ) {
		ms_free( acl );
		return( acl[i].a_perm & class_mode );
	    }
	}

	ms_free( acl );
    }


    /*
     * Return the file's other base mode since we didn't find anything
     * else in the ACL
     */
    return ADVFS_GETBASEMODE( fmode, ADVFS_ACL_OTHER );
}



/*
 *  NAME:               advfs_check_default_acls
 *
 *  DESCRIPTION:        Checks if the parent directory has default ACL
 *			entries and returns the appropriate mode
 *
 *  ARGUMENTS:          dvp		IN	vnode of the parent directory
 *			vp		IN	vnode of the new file
 *			acl		OUT	Buffer for the new file's ACL
 *			num_tuples	OUT	num of tuples in new buffer
 *			is_dir		IN	is the new file a directory?
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *                      FAILURE         Returns EINVAL if ACL was not accessable
 */
statusT
advfs_check_default_acls(
    struct vnode *dvp,
    struct vnode *vp,
    struct acl **acl,
    int *num_tuples,
    int is_dir )
{
    bfAccessT *bfap;			/* bfap of parent directory */
    struct fsContext *context_ptr;	/* context pointer for new file */
    mode_t mode;			/* Current mode of the new file */
    uid_t uid, gid;			/* uids of user and group of new file */
    advfs_acl_t *dadvfs_acl;		/* ACL buffer for parent directory */
    struct acl *aclp=NULL;		/* temp ACL buffer for file */
    int dnum_tuples;			/* Num ACL entries in parent dir ACL */
    int num_def_obj=0;			/* Num default base entries in parent */
    int i, j, beg_def_index=0, verify_flag=0, base_group_index=0;

    bfap = VTOA( dvp );

    ADVRWL_BFAP_BFA_ACL_LOCK_READ( &bfap->bfa_acl_lock );
    if( bfap->bfa_acl_cache == NULL ) {
        /* Assumes bfa_acl_cache is NULL if we have no ACL except base.
         * Nothing to do */
        ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
        return EOK;
    }

    context_ptr = VTOC( vp );
    ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
    uid = context_ptr->dir_stats.st_uid;
    gid = context_ptr->dir_stats.st_gid;
    mode = context_ptr->dir_stats.st_mode;
    ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

    dnum_tuples = bfap->bfa_acl_cache_len;
    dadvfs_acl = bfap->bfa_acl_cache;

    /*
     * Get the number of entries we need so we can malloc the new file's
     * ACL buffer, also keep track of exactly which default base entries we
     * have so we can build the proper buffer.  A default ACL entry should
     * NEVER start the ACL, ASSERT that this is the case.
     */
    MS_SMP_ASSERT( !( dadvfs_acl[0].aa_a_type & ADVFS_ACL_DEFAULT ) );
    *num_tuples=0;
    for( i=0; i<dnum_tuples; i++ ) {

	if( dadvfs_acl[i].aa_a_type & ADVFS_ACL_DEFAULT ) {

	    if( dadvfs_acl[i].aa_a_type == ADVFS_DEF_USER_OBJ ) {
		/* Default USER base */
		verify_flag |= VERIFY_USER;
		num_def_obj++;
		/*
		 * Make note of the beginning of the default entries so
		 * we don't have to loop through the non-default entries
		 * again when we build the new file's ACL buffer
		 */
		beg_def_index = i;

	    } else if( dadvfs_acl[i].aa_a_type == ADVFS_DEF_GROUP_OBJ ) {
		/* Default GROUP base */
		verify_flag |= VERIFY_GROUP;
		num_def_obj++;
		if( !beg_def_index )
		    beg_def_index = i;

	    } else if( dadvfs_acl[i].aa_a_type == ADVFS_DEF_CLASS_OBJ ) {
		/* Default CLASS base */
		verify_flag |= VERIFY_CLASS;
		num_def_obj++;
		if( !beg_def_index )
		    beg_def_index = i;

	    } else if( dadvfs_acl[i].aa_a_type == ADVFS_DEF_OTHER_OBJ ) {
		/* Default OTHER base */
		verify_flag |= VERIFY_OTHER;
		num_def_obj++;
		if( !beg_def_index )
		    beg_def_index = i;

	    } else {
		/* Default optional USER or GROUP */
		num_def_obj++;
		if( !beg_def_index )
		    beg_def_index = i;
		(*num_tuples)++;
	    }
	}
    }

    /* We had no default entries, nothing to do */
    if( !beg_def_index ) {
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	return EOK;
    }

    /* Get the total count of entries and malloc the buffer */
    *num_tuples += ADVFS_NUM_BASE_ENTRIES;
    /*
     * Add the default obj entries to the count if there were any,
     * and if this is a new dir
     */
    if( is_dir && num_def_obj )
	*num_tuples += num_def_obj;
    aclp = (struct acl *)ms_malloc( sizeof( struct acl ) * (*num_tuples) );


    /*
     * Build the buffer
     */

    j = 0;		/* index of new file's ACL buffer */
    i = beg_def_index;	/* Starting index of Parent's default ACL entries */

    /* BASE USER  */
    aclp[j].a_type = USER_OBJ;
    aclp[j].a_id = uid;
    if( verify_flag & VERIFY_USER ) {
	aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
	i++;
    } else 
	aclp[j].a_perm = ADVFS_GETBASEMODE( mode, ADVFS_ACL_USER );
    j++;

    /* OPTIONAL USERS */
    for( ; i<dnum_tuples, dadvfs_acl[i].aa_a_type == ADVFS_DEF_USER; i++ ) {
	aclp[j].a_type = USER;
	aclp[j].a_id = dadvfs_acl[i].aa_a_id;;
	aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
	j++;
    }

    /* BASE GROUP */
    aclp[j].a_type = GROUP_OBJ;
    aclp[j].a_id = gid;
    if( verify_flag & VERIFY_GROUP ) {
        aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
        i++;
    } else {
	if( verify_flag & VERIFY_CLASS )
	    /* We have to set this one to the class entry, make note of index */
	    base_group_index = j;
	else
	    aclp[j].a_perm = ADVFS_GETBASEMODE( mode, ADVFS_ACL_GROUP);
    }
    j++;

    /* OPTIONAL GROUPS */
    for( ; i<dnum_tuples, dadvfs_acl[i].aa_a_type == ADVFS_DEF_GROUP; i++ ) {
        aclp[j].a_type = GROUP;
        aclp[j].a_id = dadvfs_acl[i].aa_a_id;;
        aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
        j++;
    }

    /* BASE CLASS */
    aclp[j].a_type = CLASS_OBJ;
    aclp[j].a_id = 0;
    if( verify_flag & VERIFY_CLASS ) {
	aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
	if( base_group_index && ( verify_flag & VERIFY_GROUP ) )
	    aclp[base_group_index].a_perm = dadvfs_acl[i].aa_a_perm;
	i++;
    } else
	aclp[j].a_perm = ADVFS_GETBASEMODE( mode, ADVFS_ACL_GROUP);
    j++;


    /* BASE OTHER */
    aclp[j].a_type = OTHER_OBJ;
    aclp[j].a_id = 0;
    if( verify_flag & VERIFY_OTHER ) {
	aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
	i++;
    } else
	aclp[j].a_perm = ADVFS_GETBASEMODE( mode, ADVFS_ACL_OTHER );
    j++;

    /* DEFAULT ENTRIES - if this is a new directory */
    if( is_dir && num_def_obj ) {
	for( i=beg_def_index; i<dnum_tuples; i++ ) {
	    aclp[j].a_type = dadvfs_acl[i].aa_a_type;
	    aclp[j].a_id = dadvfs_acl[i].aa_a_id;
	    aclp[j].a_perm = dadvfs_acl[i].aa_a_perm;
	    j++;
	}
    }

    /*
     * There is no need to update the file's mode at this time as it will
     * be updated by advfs_setacl_int when it's called from fs_create_file.
     */

    *acl = aclp;

    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );

    return EOK;
}


/*
 *  NAME:               advfs_update_base_acls
 *
 *  DESCRIPTION:        Updates the on disk ACL and in memory ACL cache
 *			transactionally with the given base ACL entries.
 *			This routine is called by advfs_chown and advfs_chmod.
 *
 *  ARGUMENTS:          bfap		file to update
 *			new_owner	New owner ID
 *			new_group	New group ID
 *			new_mode	New access mode
 *
 *  RETURN VALUES:      SUCCESS         EOK
 *                      FAILURE         Initiates domain panic and returns
 *					errors from called AdvFS routines.
 */

int32_t
advfs_update_base_acls(
    ftxHT parent_ftx,
    bfAccessT *bfap,
    uid_t new_owner,
    uid_t new_group,
    mode_t new_mode )
{
    statusT sts;
    int i, class_index, page_pinned=FALSE, done=FALSE;
    ftxHT ftx_acl_owner_update;
    bfMCIdT first_acl_mcell, next_mcell;
    bfAccessT *bmt_bfap;
    vdT *vdp;
    bsr_acl_rec_t *record_ptr;
    bsMCT *mcellp=NULL;
    bsMPgT *page_ptr=NULL;
    bfPageRefHT pgRef;
    rbfPgRefHT pgHdl = { NULL, 0, 0 };
    struct fsContext *context_ptr;
    uint64_t fmode;
    uint32_t recOffset;
    uint16_t rSize;
    struct vnode *vp;


    ADVRWL_BFAP_BFA_ACL_LOCK_WRITE( &bfap->bfa_acl_lock );
    if( bfap->bfa_acl_cache == NULL ) {
	/* Assumes bfa_acl_cache is NULL if we have no ACL except base.  
	 * Nothing to do */
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	return EOK;
    }
    ADVRWL_MCELL_LIST_READ( &bfap->mcellList_lk );

    sts = FTX_START_N(  FTA_ACL_UPDATE,
			&ftx_acl_owner_update,
			parent_ftx,
			bfap->dmnP );


    if( sts != EOK ) {
	domain_panic( bfap->dmnP,
	    "advfs_update_base_acls: acls will be out of date" );
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	return sts; 
    }
    first_acl_mcell = bfap->primMCId;
    vdp = VD_HTOP( bfap->primMCId.volume, bfap->dmnP );
    sts = bmtr_scan_mcells( &first_acl_mcell,
			    &vdp,
			    (void **)&record_ptr,
			    &pgRef,
			    &recOffset,
			    &rSize,
			    BSR_ACL_REC,
			    bfap->tag );
    if( sts != EOK ) {
	ftx_fail( ftx_acl_owner_update );
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	domain_panic( bfap->dmnP,
	    "advfs_update_base_acls: have an ACL cache but no ACL on disk" );
	return sts;
    }

    bs_derefpg( pgRef, BS_CACHE_IT );
    sts = advfs_bmtr_mcell_refpg( bfap,
				  first_acl_mcell,
				  &mcellp,
				  &pgRef);
    if( sts != EOK ) {
	ftx_fail( ftx_acl_owner_update );
	ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
	ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
	domain_panic( bfap->dmnP,
	    "advfs_update_base_acls: acls out of date" );
	return sts;
    }
    record_ptr = bmtr_find( mcellp, BSR_ACL_REC, bfap->dmnP );
    /* We already found it in this mcell, better still be there */
    MS_SMP_ASSERT( record_ptr != NULL );
    next_mcell = first_acl_mcell;

    while( record_ptr != NULL ) {
	page_pinned = FALSE;
	for( i = 0; i < record_ptr->bar_hdr.bar_acl_cnt; i++ ) {
	    if(	record_ptr->bar_acl[i].aa_a_type == ADVFS_USER_OBJ ||
		record_ptr->bar_acl[i].aa_a_type == ADVFS_GROUP_OBJ ||
		record_ptr->bar_acl[i].aa_a_type == ADVFS_CLASS_OBJ ||
		record_ptr->bar_acl[i].aa_a_type == ADVFS_OTHER_OBJ ) {

		if( !page_pinned ) {
		    bs_derefpg( pgRef, BS_CACHE_IT );
		    bmt_bfap = bfap->dmnP->vdpTbl[next_mcell.volume - 1]->bmtp;
		    sts = rbf_pinpg( &pgHdl,
				     (void **)&page_ptr,
				     bmt_bfap,
				     next_mcell.page,
				     BS_NIL,
				     ftx_acl_owner_update,
				     MF_VERIFY_PAGE );
		    if( sts != EOK ) {
			ftx_fail( ftx_acl_owner_update );
			ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );
			ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
			domain_panic( bfap->dmnP,
			    "advfs_update_base_acls: rbf_pinpg failed" );
			return sts;
		    }
		    page_pinned = TRUE;
		    rbf_pin_record( pgHdl,
				    (void *)&record_ptr->bar_acl[i],
				    sizeof( advfs_acl_t ) );
		}
	    }
	    if( record_ptr->bar_acl[i].aa_a_type == ADVFS_USER_OBJ ) {
		record_ptr->bar_acl[i].aa_a_id = new_owner;
		record_ptr->bar_acl[i].aa_a_perm = ADVFS_GETBASEMODE(
		    new_mode, ADVFS_ACL_USER );
	    } else if( record_ptr->bar_acl[i].aa_a_type == ADVFS_GROUP_OBJ ) {
		record_ptr->bar_acl[i].aa_a_id = new_group;
		record_ptr->bar_acl[i].aa_a_perm = ADVFS_GETBASEMODE(
		    new_mode, ADVFS_ACL_GROUP );
	    } else if( record_ptr->bar_acl[i].aa_a_type == ADVFS_CLASS_OBJ ) {
		record_ptr->bar_acl[i].aa_a_perm = ADVFS_GETBASEMODE(
		    new_mode, ADVFS_ACL_GROUP );
	    } else if( record_ptr->bar_acl[i].aa_a_type == ADVFS_OTHER_OBJ ) {
		record_ptr->bar_acl[i].aa_a_perm = ADVFS_GETBASEMODE(
		    new_mode, ADVFS_ACL_OTHER );
		/* If we reached the other, then we are done processing */
		record_ptr = NULL;
		done = TRUE;
		break;
	    }
	}
	if( done )
	    break;
	next_mcell = mcellp->mcNextMCId;
	if( !page_pinned )
	    bs_derefpg( pgRef, BS_CACHE_IT );
	sts = advfs_bmtr_mcell_refpg( bfap,
				      next_mcell,
				      &mcellp,
				      &pgRef );
	record_ptr = bmtr_find( mcellp, BSR_ACL_REC, bfap->dmnP );
	if( record_ptr == NULL ) {
	    bs_derefpg( pgRef, BS_CACHE_IT );
	    break;
	}
    }
    ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );

    /* On disk is now updated, so just update the bfa_acl_cache. */
    for( i=0; i<bfap->bfa_acl_cache_len; i++ ) {
	if( bfap->bfa_acl_cache[i].aa_a_type == ADVFS_USER_OBJ ) {
	    bfap->bfa_acl_cache[i].aa_a_id = new_owner;
	    bfap->bfa_acl_cache[i].aa_a_perm = ADVFS_GETBASEMODE(
		new_mode, ADVFS_ACL_USER );
	} else if( bfap->bfa_acl_cache[i].aa_a_type == ADVFS_GROUP_OBJ ) {
	    bfap->bfa_acl_cache[i].aa_a_id = new_group;
	    bfap->bfa_acl_cache[i].aa_a_perm = ADVFS_GETBASEMODE(
		new_mode, ADVFS_ACL_GROUP );
	} else if( bfap->bfa_acl_cache[i].aa_a_type == ADVFS_CLASS_OBJ ) {
	    bfap->bfa_acl_cache[i].aa_a_perm = ADVFS_GETBASEMODE(
		new_mode, ADVFS_ACL_GROUP );
	} else if( bfap->bfa_acl_cache[i].aa_a_type == ADVFS_OTHER_OBJ ) {
	    bfap->bfa_acl_cache[i].aa_a_perm = ADVFS_GETBASEMODE(
		new_mode, ADVFS_ACL_OTHER );
	    break;
	}
    }

    /* 
     * Now update the fsStat ODS to make sure everything is in agreement 
     */
    vp = ATOV( bfap );
    fs_update_stats( vp, bfap, ftx_acl_owner_update, 0 );

    ftx_done_n( ftx_acl_owner_update, FTA_ACL_UPDATE );
    ADVRWL_BFAP_BFA_ACL_UNLOCK( &bfap->bfa_acl_lock );

    return EOK;
}
