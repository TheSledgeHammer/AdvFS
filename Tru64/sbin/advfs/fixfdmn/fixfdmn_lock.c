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
 * Facility:
 *
 *      Advanced File System
 *
 * Abstract:
 *
 *      On-disk structure fixer.
 *
 * Date:
 *      Mon Jan 10 13:20:44 PST 2000
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn_lock.c,v $ $Revision: 1.1.4.5 $ (DEC) $Date: 2001/04/04 14:58:58 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: ffd_rwlock_create (B.2)
 *
 * Description: Creates a read/write lock.
 *
 * Input parameters:
 *     lock: A pointer to a pointer to the lock to be created.
 *
 * Returns:  SUCCESS, NO_MEMORY, or FAILURE.
 * 
 */
int ffd_rwlock_create(tis_rwlock_t **lock)
{
    char  *funcName = "ffd_rwlock_create";
    int status;

    *lock = (tis_rwlock_t *)ffd_malloc(sizeof(tis_rwlock_t));
    if (NULL == *lock) {	
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_318,
			 "read/write lock"));
	return NO_MEMORY;
    }

    status = tis_rwlock_init(*lock);
    if (ENOMEM == status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_319, 
			 "read/write lock init"));
	return NO_MEMORY;
    }
    else if (0 != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_320, 
			 "Failed to initialize read/write lock.\n"));
	return FAILURE;
    }

    return SUCCESS;
} /* end ffd_rwlock_create */



/*
 * Function Name: ffd_rwlock_destroy (B.3)
 *
 * Description: Destroys a read/write lock.
 *
 * Input parameters:
 *     lock: A pointer to the lock to be destroyed.
 *
 * Returns:  SUCCESS or FAILURE.
 * 
 */
int ffd_rwlock_destroy(tis_rwlock_t *lock)
{
    char  *funcName = "ffd_rwlock_destroy";
    int status;

    status = tis_rwlock_destroy(lock);
    if (0 != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_321, 
			 "Failed to destroy read/write lock.\n"));
	return FAILURE;
    }

    free(lock);
	  
    return SUCCESS;
} /* end ffd_rwlock_destroy */



/*
 * Function Name: ffd_mutex_create (B.4)
 *
 * Description: Creates a mutex lock.
 *
 * Input parameters:
 *     lock: A pointer to a pointer to the lock to be created.
 *
 * Output parameters:
 *     lock: A pointer to a pointer to the lock that has been created.
 *
 * Returns:  SUCCESS, NO_MEMORY, or FAILURE.
 * 
 */
int ffd_mutex_create(pthread_mutex_t **lock)
{
    char  *funcName = "ffd_mutex_create";
    int status;

    *lock = (pthread_mutex_t *)ffd_malloc(sizeof(pthread_mutex_t));
    if (NULL == *lock) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_322, 
			 "mutex lock"));
	return NO_MEMORY;
    }

    status = tis_mutex_init(*lock);
    if (ENOMEM == status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_323, 
			 "mutex lock init"));
	return NO_MEMORY;
    }
    else if (0 != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_324, 
			 "Failed to initialize mutex lock.\n"));
	return FAILURE;
    }

    return SUCCESS;
} /* end ffd_mutex_create */



/*
 * Function Name: ffd_mutex_destroy (B.5)
 *
 * Description: Destroys a mutex lock.
 *
 * Input parameters:
 *     lock: A pointer to the lock to be destroyed.
 *
 * Returns:  SUCCESS or FAILURE.
 * 
 */
int ffd_mutex_destroy(pthread_mutex_t *lock)
{
    char  *funcName = "ffd_mutex_destroy";
    int status;

    status = tis_mutex_destroy(lock);
    if (0 != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_325, 
			 "Failed to destroy mutex lock.\n"));
	return FAILURE;
    }

    free(lock);
	  
    return SUCCESS;
} /* end ffd_mutex_destroy */

/* end fixfdmn_lock.c */
