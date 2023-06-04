/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Device list routines.
 *
 * Date:
 *
 *      Wed Apr 10 14:26:35 1991
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: devlist_util.c,v $ $Revision: 1.1.17.1 $ (DEC) $Date: 2003/01/06 22:45:35 $";
#endif


#include "backup.h"
#include "util.h"
#include "devlist_util.h"
#include <stdarg.h>
#include <sys/signal.h>
#include "libvdump_msg.h"



/******************************************************************************
 *
 * Device List -
 *
 * The following routines are used to manage a simple linked list of devices.
 * The main purpose of the device list is to maintain a pool of devices
 * that threads can use to allocate and deallocated devices.
 *
 * Supported functions include: create and delete list, add device, allocate
 * device, dallocate a device.
 *
 *****************************************************************************/

/*
 * dev_entry_t - Device list entry
 */
typedef struct _dev_entry {
    char name[DEV_NAME_SZ];      /* name of device                            */
    int fd;                      /* device's open file descriptor             */
    struct _dev_entry *next_dev; /* pointer to next device in the list        */
} dev_entry_t;

/*
 * dev_list_t - Device list header
 */
typedef struct {
    t_mutex mutex;           /* synchronization mutex                         */
    t_cond  cv;              /* synchronization condition variable            */
    int waiters;             /* num threads waiting to allocate a device      */
    int dev_cnt;             /* number of devices in the list                 */
    dev_entry_t *avail_devs; /* list of devices available for allocating      */
    dev_entry_t *busy_devs;  /* list of allocated devices                     */
} dev_list_t;

static const dev_list_t nil_dev_list = { 0 };

/*
 * dev_list_create - Creates and empty device list.  dev_add() must be used
 * to add devices.
 *
 * returns ERROR or OKAY.
 */
int
dev_list_create(
   dev_list_handle_t *dev_list_h  /* out - hndl to new dev list */
   )
{
    dev_list_t *devl;

    /*-----------------------------------------------------------------------*/

    devl = (dev_list_t *) malloc( sizeof( dev_list_t ) );
    
    if (devl == NULL) {return ERROR;}

    *devl = nil_dev_list;

    mutex__create( &devl->mutex );
    cond__create( &devl->cv );

    *dev_list_h = (dev_list_handle_t) devl;

    return OKAY;
}

/*
 * dev_add - Adds a device to the device list.  The device must be open
 * using the open() system call before calling this routine.
 *
 * returns ERROR or OKAY.
 */
int
dev_add(
   dev_list_handle_t dev_list_h, /* in - dev list handle */
   int dev_fd,                   /* in - device's open file descriptor */
   char *dev_name                /* in - device's name */
   )
{
    dev_list_t *devl;
    dev_entry_t *devp;

    /*------------------------------------------------------------------------*/

    devl = (dev_list_t *) dev_list_h;

    /* Allocate a device list entry */
    devp = (dev_entry_t *) malloc( sizeof( dev_entry_t ) ) ;
    if (devp == NULL) {return ERROR;}

    /* Fill in the entry */
    devp->fd = dev_fd;
    strncpy( devp->name, dev_name, DEV_NAME_SZ );

    mutex__lock( devl->mutex );

    /* Add entry to the head of the list of available devices */
    devp->next_dev = devl->avail_devs;
    devl->avail_devs = devp;

    devl->dev_cnt++;

    /* 
     * If there are threads waiting to allocate a device we 
     * signal them (it) that there is a device available.
     */
    if (devl->waiters == 1) {
        cond__signal( devl->cv );
   } else if (devl->waiters > 1) {
       cond__broadcast( devl->cv );
   }

    mutex__unlock( devl->mutex );

    return OKAY;
}

/*
 * dev_alloc - Used to allocate a device.  If a device is available it is
 * moved to the busy list and it's open file descriptor and name are returned
 * to the caller.  If there are not devices available the calling thread
 * will be blocked until a device becomes available (via dev_add() or
 * dev_dealloc()).
 */
void
dev_alloc(
   dev_list_handle_t dev_list_h,   /* in - dev list handle */
   int *dev_fd,                    /* out - Device's open file descriptor */
   char *dev_name                  /* out - Device's name */
   )
{
    dev_list_t *devl;
    dev_entry_t *devp;

    /*------------------------------------------------------------------------*/

    devl = (dev_list_t *) dev_list_h;

    mutex__lock( devl->mutex );

    while (devl->avail_devs == NULL) {
        /* No devices are available.  Block until one becomes available */
        devl->waiters++;
        cond__wait( devl->cv, devl->mutex );
        devl->waiters--;
    }

    /* Move the devices from the avail list to the busy list */
    devp = devl->avail_devs;
    devl->avail_devs = devp->next_dev;

    devp->next_dev = devl->busy_devs;
    devl->busy_devs = devp;

    mutex__unlock( devl->mutex );

    *dev_fd = devp->fd;
    strcpy( dev_name, devp->name );
}

/*
 * dev_dealloc - Used to make a device available for allocation.  If there
 * are threads waiting for a device they will be signaled that a device
 * has been made available.  The device is moved from the busy list to the
 * avail list.
 *
 * returns ERROR or OKAY.
 */
int
dev_dealloc(
   dev_list_handle_t dev_list_h,
   int dev_fd                    /* in - Device's open file descriptor */
   )
{
    dev_list_t *devl;
    dev_entry_t *devp, *prev_devp;

    /*------------------------------------------------------------------------*/

    devl = (dev_list_t *) dev_list_h;

    mutex__lock( devl->mutex );

    /* 
     * Scan the busy list in search of the device entry.  Use device's
     * open file descriptor as the search key.
     */
    devp = devl->busy_devs;
    prev_devp = devp;

    while ((devp != NULL) && (devp->fd != dev_fd)) {
        prev_devp = devp;
        devp = devp->next_dev;
    }

    if (devp != NULL) {
        /* The device was found in the busy list.  Move it to the avail list. */
        if (devp == prev_devp) {
            devl->busy_devs = devp->next_dev;
        } else {
            prev_devp->next_dev = devp->next_dev;
        }

        devp->next_dev = devl->avail_devs;
        devl->avail_devs = devp;

        /* Signal any threads waiting to allocate a device */
        if (devl->waiters == 1) {
            cond__signal( devl->cv );
        } else if (devl->waiters > 1) {
            cond__broadcast( devl->cv );
        }
    }

    mutex__unlock( devl->mutex );

    if (devp == NULL) {
        /* Dev entry not found in busy dev list */
        return ERROR;
    } else {
        return OKAY;
    }
}

/* 
 * dev_list_delete - Deletes a device list.  It also closes the devices.
 */
void
dev_list_delete(
   dev_list_handle_t dev_list_h  /* in - dev list handle */
   )
{
    extern void (*close_device)( int fd ); /* device-dependent close() */
    dev_list_t *devl;
    dev_entry_t *devp, *next_devp;

    /*------------------------------------------------------------------------*/

    devl = (dev_list_t *) dev_list_h;

    mutex__delete( &devl->mutex );
    cond__delete( &devl->cv );

    /* Scan and close all busy devices... THERE SHOULDN'T BE ANY!! */
    devp = devl->busy_devs;

    while (devp != NULL) {
        next_devp = devp->next_dev;
        fprintf( stderr, catgets(comcatd, S_DEVLIST_UTIL1, DEVLIST_UTIL1, "%s: error -- closing busy device;<%d> <%s>\n"), 
                Prog, devp->fd, devp->name ); 
        close_device( devp->fd );
        free( devp );
        devp = next_devp;
    }

    /* Scan and close all available devices */
    devp = devl->avail_devs;

    while (devp != NULL) {
        next_devp = devp->next_dev;
        close_device( devp->fd );
        free( devp );
        devp = next_devp;
    }

    free( devl );
}

/* end devlist_util.c */
