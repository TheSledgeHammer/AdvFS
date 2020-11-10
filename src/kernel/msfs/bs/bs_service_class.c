/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991                            *
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
 *      This module contains routines for manipulating service class
 *      tables.
 *
 * Date:
 *
 *      Thu Aug 16 14:37:49 1990
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_service_class.c,v $ $Revision: 1.1.57.3 $ (DEC) $Date: 2008/02/12 13:07:02 $"

#ifndef KERNEL
#include <stdio.h>
#endif /* KERNEL */
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>

static statusT sc_add_class(serviceClassTblT *scTbl, serviceClassT serviceClass, int *tblPos);
static int best_vd(domainT *dmnP, int vd, vdIndexT *vdi);

#define ADVFS_MODULE BS_SERVICE_CLASS

/* Prototypes for static functions. */

static int
sc_find_super_class( 
    serviceClassTblT *scTbl,    /* in - pointer to a service class table */
    serviceClassT serviceClass, /* in - service class (find its super class) */
    int tblPos                  /* in - search start index in to scTbl */
    );

static int
sc_find_alt_class( 
    serviceClassTblT *scTbl,    /* in - ptr to service class tbl */
    serviceClassT reqServices,  /* in - required services */
    serviceClassT optServices   /* in - optional services */
    );

static int
sc_binSearchForClass(
    serviceClassTblT *scTbl,    /* in - pointer to service class table */
    serviceClassT serviceClass  /* in - service class to find in table */
    );

static statusT
sc_add_class( 
    serviceClassTblT *scTbl,    /* in - ptr to service class table */
    serviceClassT serviceClass, /* in - service class to add to table */
    int *tblPos                 /* in - table position for new class */
    );

static statusT
sc_add_vd_to_class(
    scEntryT *scEntry,          /* in - ptr to service class' table entry */
    serviceClassT vdSvc,        /* in - vd's service class */
    uint16T vdIndex             /* in - vd's index */
    );

static statusT
sc_remove_class(
    serviceClassTblT *scTbl,    /* in - ptr to service class table */
    serviceClassT serviceClass  /* in - service class to remove */
    );

static scEntryT *
sc_select_class(
    serviceClassTblT *scTbl,    /* in - svc cl table */
    serviceClassT reqServices,  /* in - requried service class */
    serviceClassT optServices   /* in - optional service class */
    );

#define NEXT_VD( vd ) ((vd)+1 >= scEntry->numVds ? 0 : (vd)+1)

#define NO   0
#define YES  1


/*
 * sc_init_sc_tbl
 *
 * Allocates and initializes a service class table.  
 *
 * Returns a pointer to the table.
 */

serviceClassTblT *
sc_init_sc_tbl(
    void
    )
{
    serviceClassTblT *scTbl;

    scTbl  = (serviceClassTblT *) ms_malloc( sizeof( serviceClassTblT ) );
    if (scTbl == NULL) {
        return NULL;
    }
    *scTbl = nilServiceClassTbl;

    return scTbl;
}


/*
 * sc_find_super_class - 
 *
 * Searches the service class table for a class
 * that is a super set of the request class.  The returned super class
 * must contain virtual disks (ie- it can't be an unavailable class).
 * Returns -1 if no super class found.  If one is found, the table index
 * of the class is returned.
 */

static int
sc_find_super_class( 
    serviceClassTblT *scTbl,    /* in - pointer to a service class table */
    serviceClassT serviceClass, /* in - service class (find it's super class */
    int tblPos                  /* in - search start index in to scTbl */
    )
{
    /*
     * Scan linearly thru the service class table looking for a
     * class that is a super set of the requested class.  The scan starts
     * with the entry where the requested service class would be in the
     * table.  This is a good starting point since there can be no super 
     * classes that are less than the requested class.
     */

    for (/*tblPos*/; tblPos < scTbl->numClasses; tblPos++) {

        if (SC_SUBSET( serviceClass, scTbl->classes[tblPos].serviceClass )) {

            /*
             * Super class found; return its index into class table.
             */

            if (scTbl->classes[tblPos].numVds == 0) {

                /*
                 * The super class that we found does not contain
                 * any virtual disks so return its altClass index instead.
                 */

                return scTbl->classes[tblPos].altClass;

            } else {
                return tblPos;
            }
        }
    }

    /*
     * No super class found.
     */

    return -1;
}


#define IS_CLEAR(bit, wd) (!((wd) & (1 << (bit))))

/*
 * sc_find_alt_class - 
 *
 * Searches the service class table for a class
 * that is super class of the required services.  If there are several
 * super classes then pick the one that supports the highest number of
 * of the optional services.  The returned super class
 * must contain virtual disks (ie- it can't be an cached class).
 *
 * Returns the service class table index of the best super class.
 */

static int
sc_find_alt_class( 
    serviceClassTblT *scTbl,    /* in - ptr to service class tbl */
    serviceClassT reqServices,  /* in - required services */
    serviceClassT optServices   /* in - optional services */
    )
{
    int tblPos, curPos, bit, bits, bestEntPos = -1, bestEntBits = 0; 
    uint32T optServ;

    /*
     * Scan linearly thru the service class table looking for a super class
     * that supports the highest number of services specified by
     * optServices.
     */

    for (tblPos = 0; tblPos < scTbl->numClasses; tblPos++) {

        if (SC_SUBSET(reqServices, scTbl->classes[tblPos].serviceClass)) {

            if (scTbl->classes[tblPos].numVds == 0) {
                /*
                 * The suitable class that we found does not contain
                 * any virtual disks so use its altClass index instead.
                 */
                curPos = scTbl->classes[tblPos].altClass;
            } else {
                curPos = tblPos;
            }

            /* Count number of optional services that this class provides */
            bits = 0;
            optServ = (scTbl->classes[curPos].serviceClass & optServices);

            for (bit = 0; bit < 32; bit++) {
                if (!IS_CLEAR( bit, optServ )) {
                    bits++;
                }
            }

            if (bestEntPos < 0) {
                /* First time we find a class we need to init bestEntPos */
                bestEntPos = curPos;
            }

            if (bestEntBits < bits) {
                /* This class provides more of the optional services */
                bestEntBits = bits;
                bestEntPos = curPos;
            }
        }
    }

    return bestEntPos;
}


/* 
 * sc_binSearchForClass - 
 *
 * Does a binary search of class tbl entries for
 * 'serviceClass'.  If the entry is found it's index into the tbl is returned.
 * If the entry is not found then the one's complement of the last entry
 * searched is returned (so that the caller knows where to insert the entry).
 */

static int
sc_binSearchForClass(
    serviceClassTblT *scTbl,    /* in - pointer to service class table */
    serviceClassT serviceClass  /* in - service class to find in table */
    )
{
    int lwr = 0;                     /* lower bound index */
    int upr = scTbl->numClasses - 1; /* upper bound index */
    int mid;                         /* current middle entry index */

    if (scTbl->numClasses == 0) {
        return ~0;
    }

    while (TRUE) {
        mid = (lwr + upr) / 2;

        if (SC_EQL( serviceClass, scTbl->classes[mid].serviceClass )) {
            /*
             * entry found 
             */
            return mid;

        } else if (lwr == upr) {
            /*
             * entry not found
             */
            return ~mid;

        } else if (SC_LT( serviceClass, scTbl->classes[mid].serviceClass )) {
            /*
             * need to look in the lower half
             */
            if (mid == lwr) {
                return ~mid;
            }
            upr = mid - 1;

        } else {
            /*
             * need to look in the upper half
             */
            if (mid == upr) {
                return ~mid;
            }

            lwr = mid + 1;
        }
    }
}


/*
 * sc_add_class - 
 *
 * Adds a new class to the class table.  The classes are
 * maintained in sorted order so the new class is inserted into its
 * proper position.
 */

static statusT
sc_add_class( 
    serviceClassTblT *scTbl,    /* in - ptr to service class table */
    serviceClassT serviceClass, /* in - service class to add to table */
    int *tblPos                 /* in - table position for new class */
    )
{
    int i;

    if (scTbl->numClasses == BS_SC_ENTRIES) {
        return ETOO_MANY_SERVICE_CLASSES;
    }

    if (SC_EQL( serviceClass, scTbl->classes[*tblPos].serviceClass )) {
        return EBAD_SERVICE_CLASS;
    } 

    if (scTbl->numClasses > 0) {
        if (SC_GT( serviceClass, scTbl->classes[*tblPos].serviceClass )) {
            (*tblPos)++;
        }

        /*
         * shift down class entries to make room for new class
         */
        for (i = scTbl->numClasses; i > *tblPos; i--) {
            scTbl->classes[i] = scTbl->classes[i - 1];

            if (scTbl->classes[i].altClass > *tblPos) {
                /*
                 * Since altClass is a direct index into the class table
                 * we need to increment them since we are inserting a
                 * a new entry.
                 */
                scTbl->classes[i].altClass++;
            }
        }   

        /*
         * Now update the altClass for classes that
         * are less than the new class being inserted since they were
         * not done in the above loop.
         */
        for (i = 0; i < *tblPos; i++) {
            if (scTbl->classes[i].altClass >= *tblPos) {
                scTbl->classes[i].altClass++;
            }
        }
    }

    /*
     * initialize new class's entry and increment class count
     */
    scTbl->classes[*tblPos] = nilScEntry;
    scTbl->classes[*tblPos].serviceClass = serviceClass;
    scTbl->numClasses++;

    return EOK;
}


/*
 * sc_add_vd_to_class - adds a virtual disk to a class' vd list
 */

static statusT
sc_add_vd_to_class(
    scEntryT *scEntry,          /* in - ptr to service class' table entry */
    serviceClassT vdSvc,        /* in - vd's service class */
    uint16T vdIndex             /* in - vd's index */
    )
{
    int done = 0;
    int i;
    vdLstSgmntT *vdLstSeg;


    if (!SC_EQL( scEntry->serviceClass, vdSvc )) {
        return EBAD_SERVICE_CLASS;
    }

    /*
     * if the class' vd list is empty then allocate a new list segment
     */
    if (scEntry->vdLst == NULL) {
        scEntry->vdLst  = (vdLstSgmntT *) ms_malloc( sizeof( vdLstSgmntT ) );
        if (scEntry->vdLst  == NULL) {
            return ENO_MORE_MEMORY;
        }
        *scEntry->vdLst = nilVdLstSgmnt;
        scEntry->numSegs++;
        scEntry->altClass = -1; /* no longer need to use an alt class */
    }

    vdLstSeg = scEntry->vdLst;

    /* 
     * search the class' vd list for a free slot (contains a zero) and
     * put the new vd's id into that slot
     */

    while (!done) {
        /*
         * if the current vd list segment is not full then search it for
         * a free slot
         */

        if (vdLstSeg->numVds < BS_MAX_LIST_SEG_VDS) {

            for (i = 0; !done && (i < BS_MAX_LIST_SEG_VDS); i++) {

                if (vdLstSeg->vdi[i] == 0) {
                    /*
                     * a free vd list slot has been found so put the new
                     * vd's id into the slot and increment our counters
                     */
                    vdLstSeg->vdi[i] = vdIndex;
                    vdLstSeg->numVds++;
                    scEntry->numVds++;
                    done = 1;
                }
            }
        }

        /*
         * if we didn't find a free slot in the current segment then go
         * to the next segment (create a new one if necessary)
         */

        if (!done) {
            if (vdLstSeg->nextSeg == NULL) {
                vdLstSeg->nextSeg = 
                           (vdLstSgmntT *) ms_malloc( sizeof( vdLstSgmntT ) );
                if (vdLstSeg->nextSeg == NULL) {
                    return ENO_MORE_MEMORY;
               } 
                *vdLstSeg->nextSeg = nilVdLstSgmnt;
                scEntry->numSegs++;
            }

            vdLstSeg = vdLstSeg->nextSeg;
        }
    }

    return EOK;
}


/* 
 * sc_add_vd - adds a virtual disk to the service class table 
 */

statusT
sc_add_vd( 
    domainT *dmnp,                /* in - ptr to domain with sc table */
    serviceClassT vdSvc,          /* in - vd's service class */
    uint16T vdIndex               /* in - vd's index */
    )
{
    serviceClassTblT *scTbl = dmnp->scTbl;
    int tblPos;
    statusT sts;


    if (scTbl->numClasses == BS_SC_ENTRIES) {
        return ETOO_MANY_SERVICE_CLASSES;
    }

    SC_TBL_LOCK(dmnp);
    tblPos = sc_binSearchForClass( scTbl, vdSvc );

    if (tblPos < 0) {
        /*
         * this vd's service class is not in the table so add the class
         * to the table 
         */

        tblPos = ~tblPos;

        if ((sts = sc_add_class( scTbl, vdSvc, &tblPos )) !=EOK){
            SC_TBL_UNLOCK(dmnp);
            return sts;
        }
    }

    /*
     * add the vd to the class
     */

    sts = sc_add_vd_to_class( &scTbl->classes[tblPos], vdSvc, vdIndex );
    SC_TBL_UNLOCK(dmnp);

    return sts;
}


/*
 * sc_remove_class - Removes the requested service class from the service
 * class table (iff the class is empty).  Also, all cached service classes
 * that use the requested class as an alternate are removed.
 */

static statusT
sc_remove_class(
    serviceClassTblT *scTbl,    /* in - ptr to service class table */
    serviceClassT serviceClass  /* in - service class to remove */
    )
{
    int i;
    int tblPos;
    int classes_removed = 0;


    if (scTbl->numClasses == 0) {
        return ESERVICE_CLASS_NOT_FOUND;
    }
    if ((tblPos = sc_binSearchForClass( scTbl, serviceClass )) < 0) {
        return ESERVICE_CLASS_NOT_FOUND;
    }
    if (scTbl->classes[tblPos].numVds != 0) {
        return ESERVICE_CLASS_NOT_EMPTY;
    }

    /*
     * Scan the service class table and remove the service class that
     * was passed in to this routine and delete all cached service classes 
     * that use the requested service class as an alternate class.  
     * Arguably it might be more appropriate to find new alternates but since
     * select_vd() will do this anyway why do it here.  
     */

    for (i = 0; i < scTbl->numClasses; i++) {
        if (i == tblPos) {
            /* Found the requested class. */
            classes_removed++;

        } else if (scTbl->classes[i].altClass == tblPos) { 
            /* Found a class that uses the requested class as an alt class */

            if (scTbl->classes[i].numVds == 0) {
                /* Cached class */
                classes_removed++;
            } else {
                /* Hmmm.  This can't happen! */
                ADVFS_SAD0( "sc_remove_class:  Cached class has virt disks" );
            }

        } else if (classes_removed > 0) {
            int j;

            /*
             * We've found classes to remove so shift the current
             * class entry 'classes_removed' positions to the left.
             */
            scTbl->classes[i - classes_removed] = scTbl->classes[i];

            /*
             * Since we just moved a class entry we have to make sure
             * that all cached classes that use this entry as an alt
             * class point to this entry's new location in the table.
             */

            for (j = 0; j < scTbl->numClasses; j++) {
                if (scTbl->classes[j].altClass == i) { 
                    scTbl->classes[j].altClass -= classes_removed;
                }
            }
        }
    }

    /* Clear dead class entries */
    for (i = scTbl->numClasses - 1; 
         i >= (scTbl->numClasses - classes_removed);
         i--) {
        scTbl->classes[i] = nilScEntry;
    }

    scTbl->numClasses -= classes_removed;
    
    return EOK;
}


/* 
 * sc_remove_vd - Removes a virtual disk from service class table.
 */

statusT
sc_remove_vd(
    domainT *dmnp,             /* in - ptr to domain with sc table */
    serviceClassT vdSvc,       /* in - vd's service class */
    uint16T vdIndex            /* in - vd's index */
    )
{
    int tblPos;
    int done = 0;
    int i, j;
    statusT sts;
    scEntryT *scEntry;
    vdLstSgmntT *vdLstSeg;
    vdLstSgmntT *prevSeg;
    uint16T *deleted_vd;
    serviceClassTblT *scTbl = dmnp->scTbl;


    if (scTbl->numClasses == BS_SC_ENTRIES) {
        return ENO_SERVICE_CLASSES;
    }

    SC_TBL_LOCK(dmnp);
    if ((tblPos = sc_binSearchForClass( scTbl, vdSvc )) < 0) {
        SC_TBL_UNLOCK(dmnp);
        return EBAD_VDI;
    }

    /*
     * TODO - There also needs to be a check to make sure that the virtual
     * disk can be removed without affecting users.  I'm not sure what that
     * should be but it should be something similar to a check that will
     * be in 'unmount_vd' (or whatever we end up calling it).
     */

    scEntry  = &scTbl->classes[tblPos];
    vdLstSeg = scEntry->vdLst;

    /* 
     * Search the class' vd list for the vd that we want to remove.
     */

    while (!done) {
        if (vdLstSeg == NULL) {
            SC_TBL_UNLOCK(dmnp);
            return EBAD_VDI;
        }

        /*
         * Search the current vd list segment for the vd to remove.
         */

        for (i = 0; !done && (i < BS_MAX_LIST_SEG_VDS); i++) {
            if (vdLstSeg->vdi[i] == vdIndex) {
                /*
                 * Found the vd to remove.
                 */

                if (scEntry->numVds == 1) {
                    /* This is the last vd in the service class */
                    ms_free( scEntry->vdLst );
                    scEntry->vdLst = NULL;

                } else {
                    deleted_vd = &vdLstSeg->vdi[i];

                    if (scEntry->numSegs > 1) {
                        /*
                         * There are multiple list segments so we need to 
                         * find the last segment 
                         */
                        vdLstSeg = scEntry->vdLst;
                        prevSeg  = vdLstSeg;

                        for (j = 0;
                             j < ((scEntry->numVds - 1) / BS_MAX_LIST_SEG_VDS); 
                             j++) {
                            prevSeg  = vdLstSeg;
                            vdLstSeg = vdLstSeg->nextSeg;
                        }
                    }

                    /*  
                     * Move the last vd index into the slot of the deleted vd.
                     */

                    *deleted_vd = vdLstSeg->vdi[vdLstSeg->numVds-1];
                    vdLstSeg->vdi[vdLstSeg->numVds-1] = 0;
                    vdLstSeg->numVds--;
 
                    if ((scEntry->numSegs > 1) && (vdLstSeg->numVds == 0)) {
                        /* Remove the end list segment */
                        prevSeg->nextSeg = NULL;
                        scEntry->numSegs--;
                        ms_free( vdLstSeg );
                    }
                }
                scEntry->numVds--;
                if (scEntry->nextVd >= scEntry->numVds) {
                    /*
                     * The deleted entry was the last entry in the array and
                     * "nextVd" pointed to it.  Wrap to beginning of the array.
                     */
                    scEntry->nextVd = 0;
                }
                done = 1;
            }
        }

        if (!done) {
            vdLstSeg = vdLstSeg->nextSeg;  /* Go to the next segment. */
        }
    }

    if (scEntry->numVds == 0) {
        /*
         * The service class entry is empty so delete it.
         */
        sts = sc_remove_class( scTbl, scEntry->serviceClass ); 
        SC_TBL_UNLOCK(dmnp);

        return sts;
    }

    SC_TBL_UNLOCK(dmnp);
    return EOK;
}


/*
 * sc_select_class
 *
 * Given the desired service classes this routine will find the
 * matching (or super set) class entry and return a pointer to it.
 * NULL is returned if the class entry is not found.
 */

static scEntryT *
sc_select_class(
    serviceClassTblT *scTbl,    /* in - svc cl table */
    serviceClassT reqServices,  /* in - requried service class */
    serviceClassT optServices   /* in - optional service class */
    )
{
    int tblPos;        /* scTbl entry number for "serviceClass" */
    int altPos;
    statusT sts;


    if (scTbl->numClasses == 0) {
        return NULL;
    }

    /*
     * locate the requested service class in the domain's service class table
     */

    tblPos = sc_binSearchForClass( scTbl, reqServices | optServices);

    if (tblPos < 0) {
        /*
         * The requested service class is not available.  If there is
         * a class that is a super set then use it instead.
         */

        tblPos = ~tblPos;
        altPos = sc_find_super_class(scTbl, reqServices | optServices, tblPos);

        if (altPos < 0) {
            altPos = sc_find_alt_class( scTbl, reqServices, optServices );

            if (altPos < 0) {
                return NULL;
            }
        }

        /*
         * Although the requested service class is not available a suitable
         * alternate has been found.  Assuming that there will be other
         * requests for this unavailable class we create an empty class
         * entry in the class table.  It will contain no vds but its
         * altClass field will point to the alternate class that we found
         * above.  This caches unavailable classes that have suitable
         * alternates so that we don't have to search for the alternate
         * every time the unavailable class is requested.  This type of
         * service class entry is called a cached class in this module.
         */

        sts = sc_add_class( scTbl, reqServices | optServices, &tblPos );
        if (sts != EOK) {
            return NULL;
        }

        if (altPos >= tblPos) altPos++;
        scTbl->classes[tblPos].altClass = altPos;
    }

    if (scTbl->classes[tblPos].numVds == 0) {
        tblPos = scTbl->classes[tblPos].altClass;

        if (scTbl->classes[tblPos].numVds == 0) {
            /*
             * Sanity check.  This is actually an internal bug that
             * should never happen.
             */
            ADVFS_SAD0( "sc_select_class: altClass chain length is > 1" );
        }
    }

    return &scTbl->classes[tblPos];
}


/* 
 * sc_valid_vd - Verifies that the specifiec vd index supports the
 * specified services.
 *
 * Return values:
 *
 *    EOK - disk is in service class.
 *    E_VOL_NOT_IN_SVC_CLASS - disk not in service class.
 *    ESERVICE_CLASS_NOT_FOUND - service class does not exist.
 */

statusT
sc_valid_vd(
    domainT *dmnp,              /* in - ptr to domain with sc table */
    serviceClassT reqServices,  /* in - required services */
    serviceClassT optServices,  /* in - optional services */
    uint16T vdIndex             /* in - vd's index */
    )
{
    int tblPos;
    int done = 0;
    int i, j;
    statusT sts;
    scEntryT *scEntry;
    vdLstSgmntT *vdLstSeg;
    vdLstSgmntT *prevSeg;
    serviceClassTblT *scTbl = dmnp->scTbl;


    SC_TBL_LOCK(dmnp);
    scEntry  = sc_select_class ( scTbl, reqServices, optServices );
    if (scEntry == NULL) {
        SC_TBL_UNLOCK(dmnp);
        return ESERVICE_CLASS_NOT_FOUND;
    }
    vdLstSeg = scEntry->vdLst;

    /* 
     * Search the class' vd list for the vd.
     */

    while (!done) {
        if (vdLstSeg == NULL) { 
            /*
             * The class is empty.
             */
            SC_TBL_UNLOCK(dmnp);
            return E_VOL_NOT_IN_SVC_CLASS; 
        }

        /*
         * Search the current vd list segment for the vd.
         */

        for (i = 0; !done && (i < BS_MAX_LIST_SEG_VDS); i++) {

            if (vdLstSeg->vdi[i] == vdIndex) {
                /*
                 * Found the VD in the class.
                 */
                SC_TBL_UNLOCK(dmnp);
                return EOK;
            }
        }

        if (!done) {
            vdLstSeg = vdLstSeg->nextSeg;  /* Go to the next segment. */
        }
    }

    SC_TBL_UNLOCK(dmnp);
    /*
     * The VD is not in the class.
     */
    return E_VOL_NOT_IN_SVC_CLASS;
}


/*
 * sc_select_vd_for_mcell
 *
 * This routine is used to select a disk for an mcell allocation.  For
 * now we simply return the first disk that has available mcells or has
 * at least one BMT page of free space available.
 *
 * TODO: This routine is not fool proof.  Since the storage bitmap
 * is not locked the test for free space may not be accurate and we
 * could erroneously return a disk that has no available mcells
 * by the time the caller goes to allocate an mcell.  For that reason,
 * there is no point locking the mcell list either.
 *
 * Bumps vdRefCnt if vd is found.
 */

statusT
sc_select_vd_for_mcell(
    vdT **vdpa,                 /* out - selected vd */
    domainT *dmnP,              /* in - domain pointer */
    serviceClassTblT *scTbl,    /* in - service class table */
    serviceClassT reqServices,  /* in - requried service class */
    serviceClassT optServices   /* in - optional service class */
    )
{
    int i;             /* general index */
    int vd;            /* used to determine if scan has gone full circle */
    statusT sts;
    vdLstSgmntT *vdLstSeg;
    scEntryT *scEntry;
    vdT *vdp;
    int done = FALSE;
    uint16T vdi = 0;
    vdT *bestVdp = NULL;
    int bestVd;

    SC_TBL_LOCK(dmnP);
    scEntry  = sc_select_class ( scTbl, reqServices, optServices );
    if (scEntry == NULL) {
        SC_TBL_UNLOCK(dmnP);
        return ESERVICE_CLASS_NOT_FOUND;
    }
    vdLstSeg = scEntry->vdLst;
    if (vdLstSeg == NULL) {
        SC_TBL_UNLOCK(dmnP);
        return ESERVICE_CLASS_NOT_FOUND;
    }

    /*
     * locate the vd list segment that contains the "next VD" to select
     */

    for (i = 0; i < (scEntry->nextVd / BS_MAX_LIST_SEG_VDS); i++) {
        vdLstSeg = vdLstSeg->nextSeg;
    }

    /*
     * The variable 'vd' tracks the current virtual disk entry in the
     * service class's vd list.  The variable 'i' tracks the current
     * virtual disk entry in the service class's current vd list segment.
     */

    vd = scEntry->nextVd;
    i  = vd % BS_MAX_LIST_SEG_VDS;

    /*
     * Starting at scEntry->nextVd, search the vd list for a non-zero
     * entry.  When found, the contents of the vd list entry is returned
     * to the caller as the vd index to use for the next stg alloc.  Note
     * that the vd list is scanned circularly and if no entries are found
     * the scan will stop when the scan returns to scEntry->nextVd.
     */

    do {
        /*
         * scan the current vd list segment 
         */

        while (!done && (i < vdLstSeg->numVds)) {

            /* Check to see if this is the best volume for the
             * new Mcell.  If it is then we are done.
             */
            if (best_vd(dmnP, vdLstSeg->vdi[i], &vdi) == YES) {
                scEntry->nextVd = NEXT_VD( vd );
                /* Return found volume with ref count bumped. */
                *vdpa = vd_htop_already_valid(vdi, dmnP, TRUE);
                SC_TBL_UNLOCK(dmnP);
                return EOK;
            } else if ( vdi != 0 ) {
                vdp = VD_HTOP(vdi, dmnP);
                if ( bestVdp == NULL ) {
                    bestVdp = vdp;
                    bestVd = vd;
                } else if ( vdp->freeClust > bestVdp->freeClust ) {
                    bestVdp = vdp;
                    bestVd = vd;
                } else if ( vdp->freeClust == bestVdp->freeClust &&
                            bestVdp->nextMcellPg == EXTEND_BMT &&
                            vdp->nextMcellPg != EXTEND_BMT ) {
                    bestVdp = vdp;
                    bestVd = vd;
                }
            }
            i++;                     /* go to next vd in cur vd list sgmnt */
            vd = NEXT_VD( vd );      /* go to next vd in vd list */

            if (vd == scEntry->nextVd) {

                /* If we cycle through all the volumes and we have an
                 * OK volume (bestVdp is not NULL), pick this volume.
                 */
                if ( bestVdp != NULL ) {
                    /* already have one, not optimal, but will work */
                    scEntry->nextVd = NEXT_VD(bestVd);
                    /* Return found volume with ref count bumped. */
                    *vdpa = vd_htop_already_valid(bestVdp->vdIndex, dmnP, TRUE);
                    SC_TBL_UNLOCK(dmnP);
                    return EOK;
                } else {
                    done = TRUE;
                }
            }
        } /* end of scan of current list segment */

        /*
         * go to the next vd list segment (circularly)
         */

        if (vdLstSeg->nextSeg != NULL) {
            vdLstSeg = vdLstSeg->nextSeg;
        } else {
            vdLstSeg = scEntry->vdLst;
        }

        i = 0; /* use first vd list entry in new segment */

    } while (!done && (vd != scEntry->nextVd));

    /*
     ** We've scanned the entire list of vds and we haven't
     ** a usable disk.
     */

    SC_TBL_UNLOCK(dmnP);
    return ESERVICE_CLASS_NOT_FOUND;
}


/*
 * sc_select_vd_for_stg
 *
 * This routine is used to find an appropriate disk for a storage
 * allocation.  It first locates the service class table entry
 * that corresponds to the specified storage services.  If an entry
 * is found we find the 'best' disk associated with that entry.  The
 * best disk is the one that can best satisfy the storage request.
 * So, the first disk that has enough space to satisfy the request
 * is returned.  If no disk can fully satisfy the requested storage then
 * the one with the most free space is returned.
 *
 * If the selected disk does not have 'minDesiredBlks', no disk can satisfy
 * the storage request.
 *
 * The caller can specify a list of disks to exclude from the selection
 * process.
 *
 * Caller must hold the domain's scLock locked.
 */

statusT
sc_select_vd_for_stg(
    vdT **vdpa,                 /* out - selected volume */
    domainT *dmnP,              /* in - domain pointer */
    serviceClassT reqServices,  /* in - required service class */
    serviceClassT optServices,  /* in - optional service class */
    vdIndexT preferVd,          /* in - vd to try first */
    unsigned long *vd_skip_list,/* in - list of disks to skip */
    uint32T desiredBlks,        /* in - num disk blks needed */
    uint32T minDesiredBlks,     /* in - minimum num disk blks needed */
    int count_this_alloc        /* in - flag to count cluster allocation */
    )
{
    int i;             /* general index */
    int vd;            /* used to determine if scan has gone full circle */
    unsigned long curVdFreeBlks;
    unsigned long bestVdBlks = 0;
    unsigned long preferVdBlks = 0;
    vdIndexT bestVd = 0;
    statusT sts;
    vdLstSgmntT *vdLstSeg;
    scEntryT *scEntry;
    vdT *vdp = NULL;
    int done = FALSE;
    int start_vd;
    serviceClassTblT *scTbl = dmnP->scTbl;


    SC_TBL_LOCK(dmnP);
    scEntry  = sc_select_class ( scTbl, reqServices, optServices );
    if (scEntry == NULL) {
        SC_TBL_UNLOCK(dmnP);
        return ESERVICE_CLASS_NOT_FOUND;
    }
    vdLstSeg = scEntry->vdLst;
    if (vdLstSeg == NULL) {
        SC_TBL_UNLOCK(dmnP);
        return ESERVICE_CLASS_NOT_FOUND;
    }

    if (preferVd) {

        /* search each vd list segment in class to locate the vd */

        for (vd=0; vdLstSeg; vdLstSeg = vdLstSeg->nextSeg)
             for (i = 0; i < BS_MAX_LIST_SEG_VDS; i++, vd++)
                  if (vdLstSeg->vdi[i] == preferVd)
                      goto found_vd;

        /* not found in the class so just do normal locate below */
        vdLstSeg = scEntry->vdLst;
        /*
         * make it clear that we don't have a preferVd
         * (avoids kmf CLD DK_G09307)
         */
        preferVd = 0;

    }

    /*
     * locate the vd list segment that contains the "next VD" to select
     */

    for (i = 0; i < (scEntry->nextVd / BS_MAX_LIST_SEG_VDS); i++) {
        vdLstSeg = vdLstSeg->nextSeg;
    }

    /*
     * The variable 'vd' tracks the current virtual disk entry in the
     * service class's vd list.  The variable 'i' tracks the current
     * virtual disk entry in the service class's current vd list segment.
     */

    vd = scEntry->nextVd;
    i  = vd % BS_MAX_LIST_SEG_VDS;

    /*
     * Starting at start_vd, search the vd list for a non-zero entry.
     * When found, the contents of the vd list entry is returned
     * to the caller as the vd index to use for the next stg alloc.  Note
     * that the vd list is scanned circularly and if no entries are found
     * the scan will stop when the scan returns to start_vd.
     */

found_vd:
    start_vd = vd;

    do {
        /*
         * scan the current vd list segment
         */

        while (!done && (i < vdLstSeg->numVds)) {
            if (vdLstSeg->vdi[i] != 0 && (vd_skip_list == NULL ||
                   NOT_VD_SKIP(vd_skip_list, vdLstSeg->vdi[i])) ) {
                /*
                 * a potential vd was found
                 */
                vdp = VD_HTOP(vdLstSeg->vdi[i], dmnP);

                curVdFreeBlks = vdp->allocClust < vdp->freeClust ?
                     (vdp->freeClust - vdp->allocClust) * vdp->stgCluster : 0 ;

                /* use this disk if it has > 6.25 % free space and
                 * enough space to cover the desired number of blocks
                 * and at least 50% more space than the preferred disk
                 * (at this stage 50% means only 3% of preferred disk)
                 */
                if (desiredBlks <= curVdFreeBlks &&
                       (curVdFreeBlks > vdp->vdSize/16 &&
                        curVdFreeBlks > preferVdBlks + preferVdBlks/2) ) {

                    /* advance nextVd when we didn't have a preferred
                     * disk or preferred was already the nextVd
                     */
                    if ( !preferVd || (vd == scEntry->nextVd) ) {
                        scEntry->nextVd = NEXT_VD(vd);
                    }

                    /* Return found volume with ref count bumped. */
                    *vdpa = vd_htop_already_valid(vdLstSeg->vdi[i], dmnP, TRUE);

                    if (count_this_alloc) {
                        if (vdp->stgCluster == BS_CLUSTSIZE) {
                            vdp->allocClust += desiredBlks / BS_CLUSTSIZE;
                        } else {
                            MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
                            vdp->allocClust += desiredBlks / BS_CLUSTSIZE_V3;
                        }
                    }

                    SC_TBL_UNLOCK(dmnP);
                    return EOK;

                } else if (bestVdBlks < curVdFreeBlks) {

                    /* save preferred info in case we don't find any
                     * disk that has > 6.25 % to do second stage check,
                     * we check it first so it begins as the best too
                     */
                    if (preferVd && vd == start_vd) {
                        preferVdBlks = curVdFreeBlks;
                    }

                    /*
                     * We've found a better 'best' disk.
                     */
                    bestVdBlks = curVdFreeBlks;
                    bestVd = vdLstSeg->vdi[i];
                }
            }

            i++;                     /* go to next vd in cur vd list sgmnt */
            vd = NEXT_VD( vd );      /* go to next vd in vd list */

            if (vd == start_vd) {
                done = TRUE;
            }
        } /** end of scan of current list segment **/

        /*
         ** go to the next vd list segment (circularly)
         */

        if (vdLstSeg->nextSeg != NULL) {
            vdLstSeg = vdLstSeg->nextSeg;
        } else {
            vdLstSeg = scEntry->vdLst;
        }

        i = 0; /** use first vd list entry in new segment **/

    } while (!done && (vd != start_vd));

    /*
     ** we've scanned the entire list of vds and we haven't
     ** found the 'perfect' disk.  So return the 'best'
     ** disk's index; if one exists and if it has the minimum
     ** storage requirements.
     */

    if (bestVd && bestVdBlks >= minDesiredBlks) {
        /*
         * Return the 'best' disk.  It may have the specified
         * desired number of blocks at this point since now
         * getting here means all disks are at least 94% full.
         *
         * If they specified a disk preference, we assume they
         * did so for a good reason so if the preferred disk
         * is within a reasonable percent of the best disk,
         * then use the one they want, else use the best disk.
         * But if only bestVd has enough space for the desired
         * number of blocks, we forget about using preferVd.
         *
         * At this stage 50% more space is always less than 3%
         * of preferred disk and tightens as we approach 100% full.
         */
        if ( preferVd  &&
             (bestVdBlks <= preferVdBlks + preferVdBlks/2  &&
              (preferVdBlks >= desiredBlks || bestVdBlks < desiredBlks)) ) {
            bestVd = preferVd;
        }

        /* Return found volume with ref count bumped. */
        *vdpa = vdp = vd_htop_already_valid(bestVd, dmnP, TRUE);

        if (count_this_alloc) {
            if (vdp->stgCluster == BS_CLUSTSIZE) {
                vdp->allocClust += desiredBlks / BS_CLUSTSIZE;
            } else {
                MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
                vdp->allocClust += desiredBlks / BS_CLUSTSIZE_V3;
            }
        }

        SC_TBL_UNLOCK(dmnP);
        return EOK;
    }

    /*
     * service class can't be empty so no disk had minimum free blocks.
     */
    SC_TBL_UNLOCK(dmnP);
    return ENO_MORE_BLKS;
}


/*
 *
 *  best_vd
 *
 *  This function checks a volume index (vd) to see if it is 
 *  the best volume for a new mcell.  If it is the best then
 *  best_vd() returns YES, If it is not the best but good
 *  then best_vd() returns NO and vdi is set to the volume
 *  index.  If the vd is neither best nor good then best_vd()
 *  returns NO and it does not set vdi.
 *
 *  vdi is set if there are free mcells or stg.
 *  Returns YES if there is > 1/8 volume unused.
 */
static int 
best_vd (
    domainT *dmnP,   /* IN - Domain pointer */
    int vd,          /* IN - volume index to check */
    vdIndexT *vdi    /* OUT - set to vd if good or best */
)
{
    vdT *vdp;

    /* is the volume index zero, then reject */
    if (vd == 0) {
        *vdi = 0;
        return(NO);
    }

    vdp = VD_HTOP(vd, dmnP);

    /* If the nextMcellPg is NO_MORE_MCELLS, then there are no Mcells left
     * on this volume and its BMT can not be extended.  Reject it.
     */
    if (vdp->nextMcellPg == NO_MORE_MCELLS) {
        *vdi = 0;
        return(NO);
    }

    /* If the nextMcellPg is EXTEND_BMT, then we will have to extend its
     * BMT so also check if we have the space to extend the BMT.
     * If not then reject this one.
     */
    if ( vdp->nextMcellPg == EXTEND_BMT &&
         ((vdp->freeClust * vdp->stgCluster) < (8 * ADVFS_PGSZ_IN_BLKS)) ) {
        if ( vdp->freeClust != 0 ) {
            *vdi = vd;
        } else {
            *vdi = 0;
        }
        return(NO);
    }

    /* At this point the volume is good but may not be the best.
     * so set the return volume to vd.  If we can't find a best 
     * volume then this one will do.
     */
    *vdi = vd;

    /* Is there at least 1/8 (12.5%) of the volume free?
     * if not then reject it.
     */
    if (vdp->freeClust <= (vdp->vdClusters/8)) {
        return(NO);
    }

    /* the volume is the best one for a new Mcell */
    return(YES);
}
