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
 */

/*
 * This file contains the configuration entrypoint for the advfs kernel
 * subsystem.
 */

#include <sys/types.h>
#include <sys/errno.h>
#include <ms_assert.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_delete.h>
#include <bs_inmem_map.h>
#include <bs_vd.h>

#ifdef ADVFS_SMP_ASSERT
    /*
     * This var allows MS_SMP_ASSERT() to be disabled on the fly.
     * default is ON
     */
    uint32_t AdvfsEnableAsserts = 1;    
#endif /* ADVFS_SMP_ASSERT */

extern uint32_t AdvfsAccessMaxPercent;
extern int      NumAccess;
extern int      MaxAccess;
extern int      AdvfsMinAccess;
extern int      MaxAccessEventPosted;
extern int      AdvfsSyncMmapPages;
extern int      AdvfsDomainPanicLevel;

int AdvfsMinAccess_request = 0;

/*
 * 0                           - Disables AdvFS IO error retry, the default.
 * 1 to ADVFS_IO_RETRIES_MAX   - The number of seconds that AdvFS will attempt
 *                               to retry an IO request. Refer to advfs_conf.h.
 */
uint64_t AdvfsIORetryControl = 0;



