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
 *
 *
 * Facility:
 *
 *    AdvFS 
 *
 * Abstract:
 *
 *    Service class definitions and prototypes.
 *
 */

#ifndef _SERVICE_CLASSES_
#define _SERVICE_CLASSES_

#define BS_MAX_LIST_SEG_VDS 32

typedef struct vdLstSgmnt {
    struct vdLstSgmnt *nextSeg;       /* pointer to next segment */
    int numVds;                       /* number of vds in segment */
    vdIndexT vdi[BS_MAX_LIST_SEG_VDS]; /* list of vd indices in the segment */ 
} vdLstSgmntT;

extern vdLstSgmntT nilVdLstSgmnt;

/*
 * scEntryT
 *
 * Service Class Table Entry
 */
typedef struct scEntry {
    serviceClassT serviceClass;    /* service class of this entry */
    vdIndexT nextVd;               /* last vd used for new mcell */
    int numVds;                    /* number of vds vdLst */
    int altClass;                  /* last alternate class used */
    int numSegs;                   /* number of vdLst segments */
    vdLstSgmntT *vdLst;            /* list of vd's in class */
} scEntryT;

extern scEntryT nilScEntry;

#define BS_SC_ENTRIES 128

/*
 * servicClassTblT
 *
 * Service Class Table.
 */
typedef struct serviceClassTbl {
    int numClasses;                  /* number of classes */
    scEntryT classes[BS_SC_ENTRIES]; /* service class entries */
} serviceClassTblT;

extern serviceClassTblT nilServiceClassTbl;

serviceClassTblT *
sc_init_sc_tbl(
               void
               );

statusT
sc_add_vd( 
          struct domain *dmnp,          /* in - ptr to domain with sc table */
          serviceClassT vdSvc,          /* in - vd's svc class */
          vdIndexT vdIndex               /* in - vd's index */
          );

statusT
sc_remove_vd(
             struct domain *dmnp,       /* in - ptr to domain with sc table */
             serviceClassT vdSvc,       /* in - vd's svc class */
             vdIndexT vdIndex            /* in - vd's index */
             );

statusT
sc_valid_vd(
    struct domain *dmnp,       /* in - ptr to domain with sc table */
    serviceClassT reqServices, /* in - required services */
    vdIndexT vdIndex            /* in - vd's index */
    );

statusT
sc_select_vd_for_mcell(
    struct vd **vdpa,          /* out - selected volume */
    struct domain *dmnP,       /* in - domain handle */
    serviceClassTblT *scTbl,   /* in - service class table */
    serviceClassT reqServices  /* in - requried service class */
    );

statusT
sc_select_vd_for_stg(
    struct vd **vdpa,          /* out - selected volume */
    struct domain *dmnP,       /* in - domain handle */
    serviceClassT reqServices, /* in - requried service class */
    int skipCnt,               /* in - size of skipVdIndex */
    vdIndexT *skipVdIndex,     /* in - list of disks to skip */
    bf_vd_blk_t desiredBlks,   /* in - num disk blks needed */
    bf_vd_blk_t minDesiredBlks,/* in - minimum num disk blks needed */
    int leaveStgMapLocked      /* in - flag to leave map locked on return */
    );


#endif /* _SERVICE_CLASSES_ */
