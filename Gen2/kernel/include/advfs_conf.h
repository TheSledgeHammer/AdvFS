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
/* (C)Copyright 2002-2004 Hewlett-Packard Development Company, L.P. */

/* @(#) */
 
#ifndef _ADVFS_CONF_
#define _ADVFS_CONF_

/***********************************************************
 *
 * Min, Max, Default and Failsafe Defines for Tunables.
 *
 ***********************************************************/

/*
 * Manage quota behavior.
 */
#define ADVFS_QUOT_UNDRFLO_MIN      0
#define ADVFS_QUOT_UNDRFLO_COUNT    0 /* just count underflows */
#define ADVFS_QUOT_UNDRFLO_REPORT   1 /* print message */
#define ADVFS_QUOT_UNDRFLO_PANIC    2 /* panic unless manually reset */
#define ADVFS_QUOT_UNDRFLO_MAX      ADVFS_QUOT_UNDRFLO_PANIC 
#define ADVFS_QUOT_UNDRFLO_DEFAULT  ADVFS_QUOT_UNDRFLO_REPORT
#define ADVFS_QUOT_UNDRFLO_FAILSAFE ADVFS_QUOT_UNDRFLO_DEFAULT


/* 
 * Open file and access structure related.
 */
#define ADVFS_OPEN_MIN_MIN               50
#define ADVFS_OPEN_MIN_MAX               1000
#define ADVFS_OPEN_MIN_DEFAULT           ADVFS_OPEN_MIN_MIN
#define ADVFS_OPEN_MIN_FAILSAFE          ADVFS_OPEN_MIN_DEFAULT
#define ADVFS_OPEN_MAX_MIN               1000
#define ADVFS_OPEN_MAX_DEFAULT           ADVFS_OPEN_MAX_MIN
#define ADVFS_OPEN_MAX_FAILSAFE          ADVFS_OPEN_MAX_DEFAULT
#define ADVFS_ACC_CTRL_DEFAULT_MIN       100
#define ADVFS_ACC_CTRL_DEFAULT_SOFT_MAX  5000  /* Just a random guess */
#define ADVFS_ACC_CTRL_DEFAULT_CFG_MAX   5000  /* Just a random guess */


/* 
 * Time to freeze file system.
 */
#define SECONDS_PER_YEAR 31536000  /* 60*60*24*365 (expression not allowed) */
#define ADVFS_FREEZEFS_TIMEOUT_MIN      -1
#define ADVFS_FREEZEFS_TIMEOUT_MAX      SECONDS_PER_YEAR
#define ADVFS_FREEZEFS_TIMEOUT_DEFAULT  60
#define ADVFS_FREEZEFS_TIMEOUT_FAILSAFE ADVFS_FREEZEFS_TIMEOUT_DEFAULT


/* 
 * Time to retry retriable IO.
 */
#define ADVFS_IO_RETRIES_MIN             0
#define ADVFS_IO_RETRIES_MAX             SECONDS_PER_YEAR
#define ADVFS_IO_RETRIES_DEFAULT         ADVFS_IO_RETRIES_MIN
#define ADVFS_IO_RETRIES_FAILSAFE        ADVFS_IO_RETRIES_MIN

/******************* end tunable stuff **********************/


#endif /* _ADVFS_CONF_ */
