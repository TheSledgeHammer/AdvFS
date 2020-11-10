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
#pragma ident "@(#)$RCSfile: msfs_cfg.c,v $ $Revision: 1.1.96.3 $ (DEC) $Date: 2008/01/18 14:23:10 $"

/*
 * This file contains the configuration entrypoint for the advfs kernel
 * subsystem.
 */

#include <sys/types.h>
#include <sys/sysconfig.h>
#include <sys/errno.h>
#include <mach/vm_param.h>
#include <machine/machlimits.h>
#include <msfs/ms_assert.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_vd.h>

#ifdef ADVFS_SMP_ASSERT
/* This var allows MS_SMP_ASSERT() to be disabled on the fly; default is ON */
unsigned int AdvfsEnableAsserts = 1;

/* 
 * This variable, when set to 1, causes all files in newly-created
 * filesets to have atomic write data logging turned on.  This is
 * for testing purposes only.
 */
unsigned int AdvfsAllDataLogging = 0;

extern int AdvfsCheckDDL;
#endif /* ADVFS_SMP_ASSERT */

extern unsigned int             AdvfsAccessMaxPercent;
extern int NumAccess;
extern int MaxAccess;
extern int AdvfsMinAccess;
extern int MaxAccessEventPosted;
extern int                      AdvfsSyncMmapPages;
extern int                      AdvfsDomainPanicLevel;
extern int                      vm_managed_pages;

int AdvfsMinAccess_request = 0;

/* This variable, when set to 0, disables the deallocation of frag groups */
extern unsigned int AdvfsFragGroupDealloc;

                                    /* chvol scales by 16 to convert between
                                       512-byte blocks and 8-kbyte buffers
                                    */
int AdvfsReadyQLim = IOTHRESHOLD*16;

/*
 * 0                       - Specifies that AdvFS should not attempt to retry.
 * 1 to ADVFS_IO_RETRY_MAX - Specifies the number of I/O retries AdvFS
 *                           should attempt. 
 * Future Direction Suggestion:
 *   AdvfsIORetryControl and the AdvFS I/O Retry logic was implemented
 *   as a way to get around some device driver issues that, in the
 *   spring of 2002, were non-trivial to fix in the drivers.  However,
 *   if the the drivers are not fixed _AND_ customers start to
 *   become dependent on AdvfsIORetryControl, then it is suggested
 *   that this be made into a per device control which is settable via
 *   the chvol command.  Maybe something like:
 *
 *      chvol -R nnn /dev/disk/dsk2c domain_xyz         # proposed cmd
 */
long AdvfsIORetryControl = 0;   /* default 0 to maintain old behavior */


cfg_subsys_attr_t advfs_attributes[]  = {
    {"AdvfsAccessMaxPercent",                   CFG_ATTR_UINTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsAccessMaxPercent, 5, 95, 0},
    {"AdvfsSyncMmapPages",                      CFG_ATTR_INTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsSyncMmapPages, 0, 1, 0},
#ifdef ADVFS_SMP_ASSERT
    {"AdvfsEnableAsserts",                      CFG_ATTR_UINTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsEnableAsserts, 0, 1, 0},
    {"AdvfsAllDataLogging",                     CFG_ATTR_UINTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE | CFG_HIDDEN_ATTR,
     (caddr_t) &AdvfsAllDataLogging, 0, 1, 0},
    {"AdvfsCheckDDL",                           CFG_ATTR_UINTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE | CFG_HIDDEN_ATTR,
     (caddr_t) &AdvfsCheckDDL, 0, 1, 1},
#endif /* ADVFS_SMP_ASSERT */
    {"AdvfsReadyQLim",                          CFG_ATTR_INTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsReadyQLim, 0, 32768, 0},
    {"AdvfsDomainPanicLevel",                   CFG_ATTR_INTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsDomainPanicLevel, 0, 32768, 0},
    {"AdvfsMinAccess",                      CFG_ATTR_INTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE | CFG_HIDDEN_ATTR,
     (caddr_t) &AdvfsMinAccess, 0, 200000000, 0},
    {"AdvfsIORetryControl",                     CFG_ATTR_ULONGTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE | CFG_HIDDEN_ATTR,
     (caddr_t) &AdvfsIORetryControl, 0,ADVFS_IO_RETRY_MAX,0},
    {"AdvfsFragGroupDealloc",                   CFG_ATTR_UINTTYPE,
     CFG_OP_QUERY | CFG_OP_CONFIGURE | CFG_OP_RECONFIGURE,
     (caddr_t) &AdvfsFragGroupDealloc, 0, 1, 0},
    {"", 0, 0, 0, 0, 0, 0}      /* must be the last element */
};

/*
 * Validate the value for AdvfsMinAccess.  It can never exceed MaxAccess
 * which can increase/decrease if AdvfsAccessMaxPercent is modified.
 * Always try to set AdvfsMinAccess to the requested value if possible. 
 * This routine must be called everytime/everywhere that MaxAccess is set
 * or changed. 
 * 
 */
void
validate_reset_AdvfsMinAccess(void)
{
    AdvfsMinAccess = AdvfsMinAccess_request;
    if (AdvfsMinAccess > MaxAccess) {
        AdvfsMinAccess = MaxAccess;
        printf("Advfs: attribute AdvfsMinAccess cannot be set to %d due to Advfs \n   memory constraints. \n", AdvfsMinAccess_request);
        printf("   AdvfsMinAccess has been set to %d.\n", AdvfsMinAccess); 
        printf("   Increasing AdvfsAccessMaxPercent will provide additional access \n   structure memory.\n");
    }
}

/*
 * This is the AdvFS callback configuration routine.  It is used to configure
 * AdvFS variables that can not be configured when advfs_configure() is
 * called.  This is typically because advfs_configure() is called very
 * early in the boot sequence.  If the configuration of certain AdvFS 
 * variables require that other subsystems already be initialized, they
 * are configured here.
 */
void 
advfs_configure_callback(void)
{
    MaxAccess = ((vm_managed_pages*PAGE_SIZE*AdvfsAccessMaxPercent)/100) /
                sizeof(bfAccessT);
    if (MaxAccessEventPosted && (NumAccess < MaxAccess)) {
        MaxAccessEventPosted = FALSE;
    }
    validate_reset_AdvfsMinAccess();
}

/*
 * Configure, reconfigure, or query AdvFS attributes (parameters).
 * The indata argument is a list of attributes.  
 * The indata_size argument gives the length of that list.
 * Note that not all AdvFS configurable parameters are explicitly
 * referenced in this code.  This code is only needed for parameters
 * which have to be checked for more complicated conditions than
 * the simple range checks which the generic subsystem configuration
 * code does for us.
 */
advfs_configure(
    cfg_op_t                    op,
    caddr_t                     indata,
    size_t                      indata_size,
    caddr_t                     outdata,
    size_t                      outdata_size)
{
    cfg_attr_t                  *attributes;
    int                         retval = ESUCCESS;
    int                         i;

    switch (op) {

        case CFG_OP_CONFIGURE:
        case CFG_OP_RECONFIGURE:

        /*
         * Loop through the list of variables to be (re)configured,
         * (re)configuring them in the order in which they are received.
         */
        for (i = 0, attributes = (cfg_attr_t *) indata; i < indata_size; i++) {

            if (!strcmp(attributes[i].name,"AdvfsAccessMaxPercent")) {
                /*
                 * We don't do any comparison checking here with other 
                 * configurable variables. 
                 * This is intentional.  It allows the system administrator
                 * at any time to lower the amount of memory that AdvFS will 
                 * use for access structures.  If AdvFS is already over that
                 * amount, this reconfiguration will have no immediate 
                 * effect.  However, if the access structure pool shrinks 
                 * below this new limit, this reconfiguration will prevent
                 * the access structure pool from growing to that size again.
                 */
                MaxAccess = 
                    ((vm_managed_pages*PAGE_SIZE*AdvfsAccessMaxPercent)/100) /
                    sizeof(bfAccessT);
                if (MaxAccessEventPosted && (NumAccess < MaxAccess)) {
                    MaxAccessEventPosted = FALSE;
                }
                validate_reset_AdvfsMinAccess();
            }
            else if (!strcmp(attributes[i].name,"AdvfsMinAccess")) {

                /*
                 * Save the AdvfsMinAccess value in AdvfsMinAccess_request since it
                 * cannot exceed MaxAccess.  MaxAccess can be increased/decreased by
                 * adjustng AdvfsAccessMaxPercent using the reconfigure option. Saving
                 * AdvfsMinAccess allows us to honor the original request (up to 
                 * MaxAccess) should MaxAccess increase.
                 * NOTE: The value for AdvfsMinAccess will also be validated and adjusted
                 *       if necessary in the advfs_configure_callback routine since
                 *       MaxAccess could be zero (default) here if there was no request 
                 *       to configure  AdvfsAccessMaxPercent.    
                 */
                AdvfsMinAccess_request = AdvfsMinAccess;
                if (MaxAccess)  
                    validate_reset_AdvfsMinAccess();
            }
        }

        /* Always do the callback on configuration. This ensures that items
         * limited by memory size get adjusted if needed.
         */
        if (op == CFG_OP_CONFIGURE) {
             register_callback(advfs_configure_callback, 
                               CFG_PT_LOCK_AVAIL,
                               CFG_ORD_DONTCARE,
                               0);
        }

    break;

    case CFG_OP_QUERY:

        /*
         * Nothing to do here.  We should be all set.
         * No queriable variables need to be calculated.
         */
        break;

    default:
        retval = ENOTSUP;
        break;
    }

    return retval;
}

