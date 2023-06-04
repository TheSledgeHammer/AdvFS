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
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: public.c,v $ $Revision: 1.1.6.1 $ (DEC) $Date: 2001/11/01 14:52:14 $"


#include <sys/advfs_syscalls.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/mount.h>
#include <dirent.h>

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>

int
advfs_clonefset(
	char	*domainName,
	char	*filesetName,
	char	*cloneName
)
{
    int err, lkHndl, dirLocked = 0, error = 0;
    int thisDmnLock;
    mlStatusT sts;
    mlBfSetIdT bfSetId;
    char *p, *cp;
    extern void bs_init( void );
    char dmnPathName[MAXPATHLEN+1];
    struct stat stats;

    /*-----------------------------------------------------------------------*/

    /* check for root */
    if ( geteuid() ) {
	errno = EACCES;
	return -1;
    }


    if (domainName == NULL || filesetName == NULL ||
	cloneName == NULL || *cloneName == '\0') {
	errno = EINVAL;
	return -1;
    }

    if (strlen(domainName) >= BS_DOMAIN_NAME_SZ ||
	strlen(filesetName) >= BS_SET_NAME_SZ) {
	errno = ENOENT;
	return -1;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in
     * the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, domainName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
	errno = ENOENT;
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 2;

    if (cp = strpbrk( cloneName, "/# :*?\t\n\f\r\v" )) {
	errno = EINVAL;
        goto _error;
    }

    sts = msfs_fset_clone( domainName, filesetName, cloneName, &bfSetId );
    if (sts != EOK) {
	errno = BSERRMAP(sts);
	switch (sts) {
	  case E_TOO_MANY_BF_SETS:
	  case E_TOO_MANY_CLONES:
	  case E_HAS_CLONE:
	  case E_CANT_CLONE_A_CLONE:
	    errno = EEXIST;
	    break;
	  default:
	    break;
	}
        goto _error;
    }

    if (unlock_file( thisDmnLock, &err ) == -1) {
	dirLocked = 1;
	errno = EFAIL;
	goto _error;
    }
    if (unlock_file( lkHndl, &err ) == -1) {
        dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    errno = EOK;
    return 0;

_error:

    if (dirLocked == 2) {
	unlock_file(thisDmnLock, &err);
    }
    if (dirLocked) {
	unlock_file(lkHndl, &err);
    }

    return -1;
}


int
advfs_rmfset(
	char	*domainName,
	char	*filesetName
)
{
    mlStatusT sts;
    int error = 0, err, lkHndl, dirLocked = 0;
    int thisDmnLock;
    int goforit =0, c;
    char *p;
    char dmnPathName[MAXPATHLEN+1];
    struct stat stats;
    extern int optind;
    extern char *optarg;
    char response[20];
    FILE *Local_tty = stdin;

    /*----------------------------------------------------------------------*/

    /* check for root */
    if (geteuid())
    {
	errno = EACCES;
	return -1;
    }
 
    if (domainName == NULL || filesetName == NULL) {
	errno = EINVAL;
	return -1;
    }

    if (strlen(domainName) >= BS_DOMAIN_NAME_SZ ||
	strlen(filesetName) >= BS_SET_NAME_SZ) {
	errno = ENOENT;
	return -1;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, domainName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
	errno = ENOENT;
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
	errno = EALREADY;
        goto _error;
    }

    /* mark as unlocked now so unlock_file will not be retried if we
     * eventually get to _error.
     */
    dirLocked = 2;
    if (unlock_file( lkHndl, &err ) == -1) {
	errno = EFAIL;
        goto _error;
    }
    dirLocked = 3;

    sts = msfs_fset_delete( domainName, filesetName );
    if (sts != EOK) {
	errno = BSERRMAP(sts);
	switch (sts) {
	  case E_HAS_CLONE:
	    errno = EEXIST;
	    break;
	  case E_TOO_MANY_ACCESSORS:
	    errno = EBUSY;
	  default:
	    break;
	}
        goto _error;
    }

    if (unlock_file( thisDmnLock, &err ) == -1) {
	dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    errno = EOK;
    return 0;

_error:

    if (dirLocked == 2 || dirLocked == 3) {
        unlock_file( thisDmnLock, &err ) == -1;
    }

    if (dirLocked == 1 || dirLocked == 2) {
        unlock_file( lkHndl, &err ) == -1;
    }

    return -1;
}


int
advfs_get_fdmn_list(
	unsigned long	arraySize,
	unsigned long	*offset,
	unsigned long	*numDomains,
	domainInfoT	domainInfo[]
)
{
    int			lkHndl;
    int			err;
    int			dirLocked = 0;
    char		dmnDirName[MAXPATHLEN];
    DIR 		*fdmns = NULL;
    struct stat		dmnstatbuf;
    struct dirent	*curr_domain = NULL;
    mlDmnParamsT	dmnParams;
    unsigned long	arrayStart;

    if (offset == NULL || numDomains == NULL || domainInfo == NULL) {
	errno = EINVAL;
	return -1;
    }

    /* check for root */
    if ( geteuid() ) {
	errno = EACCES;
	return -1;
    }

    /*
     * Allow the caller to bypass the on-disk version checking.
     */
    if (msfs_skip_on_disk_version_check() != EOK) {
        errno = EFAIL;
        goto _error;
    }
    
    *numDomains = 0;
    arrayStart = *offset;

    fdmns = opendir(MSFS_DMN_DIR);
    if (fdmns == NULL) {	/* No /etc/fdmns, therefore no domains. */
	return 0;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_SH, &err );
    if (lkHndl < 0) {
	errno = EFAIL;
	goto _error;
    }
    dirLocked = 1;

    while ((curr_domain = readdir(fdmns)) != NULL) {
	if (curr_domain->d_name[0] == '.') {
	    if (curr_domain->d_name[1] == '\0' ||
		(curr_domain->d_name[1] == '.' &&
		 curr_domain->d_name[2] == '\0')) {
		continue;	/* Don't count "." or ".." as dmns. */
	    }
	}
	sprintf(dmnDirName, "%s/%s", MSFS_DMN_DIR, curr_domain->d_name);
	if (stat(dmnDirName, &dmnstatbuf) != 0) {
	    continue;
	}
	if (!S_ISDIR(dmnstatbuf.st_mode)) {
	    continue;
	}
	if (msfs_get_dmnname_params(curr_domain->d_name, &dmnParams) != EOK) {
		    /* Failed to get domain parameters for curr_domain.  */
		    /* Consider this to not be a domain and go on to the */
		    /* next one.					 */
	    continue;
	}

	/* We have a domain!!!  Winner! */	
	if (arrayStart <= *numDomains &&
	    *numDomains - arrayStart < arraySize) {
	    (*offset)++;
	    strcpy(domainInfo[*numDomains - arrayStart].dmnName,
		   curr_domain->d_name);
	    domainInfo[*numDomains - arrayStart].dmnId.tv_sec = 
		dmnParams.bfDomainId.tv_sec;
	    domainInfo[*numDomains - arrayStart].dmnId.tv_usec = 
		dmnParams.bfDomainId.tv_usec;
	}
	(*numDomains)++;
    }

    /*
     * Reset the offset if needed, simply to follow the functional spec
     */
    if (arrayStart == 0 && *numDomains == *offset) {
	*offset = 0;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    closedir(fdmns);

    errno = EOK;
    return 0;

_error:

    if (dirLocked) {
	unlock_file(lkHndl, &err);
    }

    closedir(fdmns);

    return -1;
}


int
advfs_get_fset_list(
	char		*domain,
	unsigned long	arraySize,
	unsigned long	*offset,
	unsigned long	*numFilesets,
	filesetInfoT	filesetInfo[]
)
{
    int			i;
    int			lkHndl;
    int			err;
    int			dirLocked = 0;
    unsigned int	setIdx;
    unsigned int	altSetIdx;
    unsigned int	userId;
    unsigned int	altUserId;
    char		temp_buffer[MNAMELEN];
    mlStatusT		sts;
    mlDmnParamsT	dmnParams;
    mlBfSetParamsT	setParams;
    mlBfSetParamsT	altSetParams;
    unsigned long	arrayStart;

    int			mountCnt;
    struct statfs	*mountTbl = NULL;


    /*************/

    if (domain == NULL || offset == NULL ||
	numFilesets == NULL || filesetInfo == NULL) {
	errno = EINVAL;
	return -1;
    }

    /* check for root */
    if ( geteuid() ) {
	errno = EACCES;
	return -1;
    }

    /*
     * Allow the caller to bypass the on-disk version checking.
     */
    if (msfs_skip_on_disk_version_check() != EOK) {
        errno = EFAIL;
        goto _error;
    }
    
    *numFilesets = 0;
    arrayStart = *offset;

    /*
     * get the mount table to quickly check every fileset for its mount point
     */
    mountCnt = getfsstat(0,0, MNT_NOWAIT);
    if (mountCnt < 0) {
	goto _error;
    }
    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
	goto _error;
    }
    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
	goto _error;
    }

    /******************************/

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_SH, &err );
    if (lkHndl < 0) {
	errno = EFAIL;
	goto _error;
    }
    dirLocked = 1;

    if (msfs_get_dmnname_params(domain, &dmnParams) != EOK) {
	errno = ENOENT;
	goto _error;
    }

    setIdx = 0; /* start with first set */
    /*
     * This routine gets the set params for the next set.
     * When 'setIdx' is zero it means to start with the
     * first set.  Each time the routine is called 'setIdx'
     * is updated to 'point' to the next set in the domain (the
     * update is done by msfs_fset_get_info()).
     */
	    
    sts = msfs_fset_get_info(domain, &setIdx,
			     &setParams, &userId, 0 );
	    
    while (sts == EOK) {
	if (arrayStart <= *numFilesets &&
	    *numFilesets - arrayStart < arraySize) {
	    (*offset)++;
	    filesetInfo[*numFilesets - arrayStart].filesetId.domainId.tv_sec =
		setParams.bfSetId.domainId.tv_sec;
	    filesetInfo[*numFilesets - arrayStart].filesetId.domainId.tv_usec =
		setParams.bfSetId.domainId.tv_usec;
	    filesetInfo[*numFilesets - arrayStart].filesetId.setTag.num =
		setParams.bfSetId.dirTag.num;
	    filesetInfo[*numFilesets - arrayStart].filesetId.setTag.seq =
		setParams.bfSetId.dirTag.seq;

	    strcpy(filesetInfo[*numFilesets - arrayStart].setName,
		   setParams.setName);

	    filesetInfo[*numFilesets - arrayStart].cloneId = setParams.cloneId;
	    filesetInfo[*numFilesets - arrayStart].hasClone =
		setParams.numClones;
	    filesetInfo[*numFilesets - arrayStart].relatedSet.origName[0] =
		'\0';
	    filesetInfo[*numFilesets - arrayStart].isMounted = 0;

	    if (setParams.cloneId) {			/* This is a clone */
		filesetInfo[*numFilesets - arrayStart].hasClone = 0;

		altSetIdx = setParams.origSetTag.num;
		sts = msfs_fset_get_info(domain, &altSetIdx, 
					 &altSetParams, &altUserId, 0 );
		if (sts == EOK) {
		    strcpy(filesetInfo[*numFilesets - arrayStart].relatedSet.origName,
			   altSetParams.setName);
		}
	    }
	    else if (filesetInfo[*numFilesets - arrayStart].hasClone) {
		altSetIdx = setParams.nextCloneSetTag.num;
		sts = msfs_fset_get_info( domain, &altSetIdx,
					  &altSetParams, &altUserId, 0);
		if (sts == EOK) {
		    strcpy(filesetInfo[*numFilesets - arrayStart].relatedSet.cloneName,
			   altSetParams.setName);
		}
	    }

	    filesetInfo[*numFilesets - arrayStart].mountPath[0] = '\0';
		    
	    sprintf(temp_buffer, "%s#%s", domain, setParams.setName);
	    for (i = 0 ; i < mountCnt ; i++) {
		if ((mountTbl[i].f_type == MOUNT_MSFS) &&
		    (!strcmp(temp_buffer, mountTbl[i].f_mntfromname))){
		    filesetInfo[*numFilesets - arrayStart].isMounted = 1;
		    strcpy(filesetInfo[*numFilesets - arrayStart].mountPath,
			   mountTbl[i].f_mntonname);
		}
	    }
	}
	(*numFilesets)++;
	sts = msfs_fset_get_info(domain, &setIdx, &setParams, &userId, 0 );
    }

    /*
     * Reset the offset if needed, simply to follow the functional spec
     */
    if (arrayStart == 0 && *numFilesets == *offset) {
	*offset = 0;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    if (mountTbl != NULL) {
        free(mountTbl);
    }

    errno = EOK;
    return 0;

_error:

    if (dirLocked) {
	unlock_file(lkHndl, &err);
    }

    if (mountTbl != NULL) {
        free(mountTbl);
    }

    return -1;
}


int 
advfs_get_fset_quotas (
	char		*domainName,
	char		*filesetName,
	filesetQuotasT	*filesetQuotas
)
{
    int sts;
    mlBfSetParamsT fsetParams;
    int setFound = 0;
    char dmnPathName[MAXPATHLEN];
    unsigned int setIdx;
    unsigned int userId;
    int lkHndl = 0;
    int dirLocked = 0;
    int err = 0;
    struct stat stats;

    if (domainName == NULL || filesetName == NULL || filesetQuotas == NULL) {
	errno = EINVAL;
	return -1;
    }

    /* check for root */
    if ( geteuid() ) {
	errno = EACCES;
	return -1;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in
     * the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, domainName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
	errno = ENOENT;
        goto _error;
    }

    /*
     * Scan the sets looking for our fileset.  This is done differently
     * from advfs_set_fset_quotas() so that inactive domain information
     * can be retrieved.
     */

    setIdx = 0; /* start with first set */

    do {
        /*
         * This routine gets the set params for the next set.
         * When 'setIdx' is zero it means to start with the
         * first set.  Each time the routine is called 'setIdx'
         * is updated to 'point' to the next set in the domain (the
         * update is done by msfs_fset_get_info()).
         */

        sts = msfs_fset_get_info( domainName, &setIdx,
                                     &fsetParams, &userId, 0 );
        if (sts == EOK) {
	    if (strcmp( filesetName, fsetParams.setName )) {
		continue;
	    } else {
		setFound = 1;
		break;
            }
	}
    } while (sts == EOK);

    if (sts != E_NO_MORE_SETS && setFound == 0) {
	errno = EFAIL;
	goto _error;
    }

    if (!setFound) {
	errno = ENOENT;
	goto _error;
    }

    filesetQuotas->blkHLimit = fsetParams.blkHLimit;
    filesetQuotas->blkSLimit = fsetParams.blkSLimit;
    filesetQuotas->fileHLimit = fsetParams.fileHLimit;
    filesetQuotas->fileSLimit = fsetParams.fileSLimit;

    if (unlock_file( lkHndl, &err ) == -1) {
        dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    errno = EOK;
    return 0;

_error:

    if (dirLocked) {
	unlock_file(lkHndl, &err);
    }

    return -1;
}


int 
advfs_set_fset_quotas (
	char		*domainName,
	char		*filesetName,
	filesetQuotasT	*filesetQuotas
)
{
    int sts;
    mlBfSetParamsT fsetParams;
    char dmnPathName[MAXPATHLEN];
    int lkHndl = 0;
    int dirLocked = 0;
    int thisDmnLock;
    int err = 0;
    struct stat stats;
    mlBfSetIdT	fsetId;

    if (domainName == NULL || filesetName == NULL || filesetQuotas == NULL) {
	errno = EINVAL;
	return -1;
    }

    /* check for root */
    if ( geteuid() ) {
	errno = EACCES;
	return -1;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in
     * the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, domainName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
	errno = ENOENT;
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
	errno = EALREADY;
        goto _error;
    }

    dirLocked = 2;

    sts = msfs_fset_get_id(domainName, filesetName, &fsetId);

    if (sts != EOK) {
	if (sts == E_NO_SUCH_BF_SET) {
	    errno = ENOENT;
	} else {
	    errno = EFAIL;
	}
	goto _error;
    }

    sts = msfs_get_bfset_params(fsetId, &fsetParams);

    if (sts != EOK) {
	if (sts == ENO_SUCH_DOMAIN) {
	    errno = EFAULT;
	} else {
	    errno = EFAIL;
	}
	goto _error;
    }

    if (fsetParams.cloneId != 0) {
	errno = EROFS;
	goto _error;
    }

    fsetParams.blkHLimit = filesetQuotas->blkHLimit;
    fsetParams.blkSLimit = filesetQuotas->blkSLimit;
    fsetParams.fileHLimit = filesetQuotas->fileHLimit;
    fsetParams.fileSLimit = filesetQuotas->fileSLimit;

    sts = msfs_set_bfset_params(fsetParams.bfSetId, &fsetParams);

    if (sts != EOK) {
	errno = EFAIL;
	goto _error;
    }

    filesetQuotas->blkHLimit = fsetParams.blkHLimit;
    filesetQuotas->blkSLimit = fsetParams.blkSLimit;
    filesetQuotas->fileHLimit = fsetParams.fileHLimit;
    filesetQuotas->fileSLimit = fsetParams.fileSLimit;

    if (unlock_file( thisDmnLock, &err ) == -1) {
	dirLocked = 1;
	errno = EFAIL;
	goto _error;
    }
    if (unlock_file( lkHndl, &err ) == -1) {
        dirLocked = 0;
	errno = EFAIL;
        goto _error;
    }

    errno = EOK;
    return 0;

_error:

    if (dirLocked == 2) {
	unlock_file(thisDmnLock, &err);
    }
    if (dirLocked) {
	unlock_file(lkHndl, &err);
    }

    return -1;
}


int
advfs_get_file_attributes (
	int		 fd,
	fileAttributesT* fileAttributes
) 
{
    int sts;
    mlBfInfoT dummyData;

    if (fileAttributes == NULL) {
	errno = EINVAL;
	return -1;
    }

    sts = advfs_get_bf_params(fd,
			      (struct mlBfAttributes *)fileAttributes,
			      &dummyData);

    switch(sts) {
	case EOK:
	    errno = 0;
	    return 0;
	case EPERM:
	    errno = EACCES;
	    return -1;
	case EINVALID_HANDLE:
	    errno = EBADF;
	    return -1;
	default:
	    errno = EFAIL;
	    return -1;
    }
}


int 
advfs_set_file_attributes (
	int		 fd,
	fileAttributesT* fileAttributes
) 
{
    int sts;
    fileAttributesT myAttr;

    sts = advfs_get_file_attributes(fd, &myAttr);

    if (sts != EOK) {
	return sts;
    }

    if (fileAttributes->writeOrder == ADVFS_WO_ASYNC_WRITE ||
	fileAttributes->writeOrder == ADVFS_WO_DATA_LOGGING ||
	fileAttributes->writeOrder == ADVFS_WO_SYNC_WRITE) {
	myAttr.writeOrder = fileAttributes->writeOrder;
    } else {
	errno = EINVAL;
	return -1;
    }

    if (fileAttributes->mapType == XMT_STRIPE) {

	if (fileAttributes->stripe.segmentCnt <= 1) {
	    errno = EINVAL;
	    return -1;
	}

	myAttr.mapType = XMT_STRIPE;
	myAttr.stripe.segmentCnt = fileAttributes->stripe.segmentCnt;
	myAttr.stripe.reserved = 8; /* Default as listed in
				       /usr/sbin/stripe/stripe.c */
    }

    sts = advfs_set_bf_attributes(fd, (struct mlBfAttributes *)&myAttr);

    switch (sts) {
	case EOK:
	    errno = 0;
	    return 0;
	case EPERM:
	    errno = EACCES;
	    return -1;
	case ENOT_SUPPORTED:
	case E_ALREADY_STRIPED:
	case E_NOT_ENOUGH_DISKS:
	    errno = EINVAL;
	    return -1;
	default:
	    errno = EFAIL;
	    return -1;
    }
}

