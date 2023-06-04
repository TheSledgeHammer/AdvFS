/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
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
 * @(#)$RCSfile: bs_service_classes.h,v $ $Revision: 1.1.18.1 $ (DEC) $Date: 2003/07/24 14:13:40 $
 */

#ifndef _SERVICE_CLASSES_
#define _SERVICE_CLASSES_

#define BS_MAX_LIST_SEG_VDS 32

typedef struct vdLstSgmnt {
    struct vdLstSgmnt *nextSeg;       /* pointer to next segment */
    int numVds;                       /* number of vds in segment */
    uint16T vdi[BS_MAX_LIST_SEG_VDS]; /* list of vd indices in the segment */ 
} vdLstSgmntT;

extern vdLstSgmntT nilVdLstSgmnt;

/*
 * scEntryT
 *
 * Service Class Table Entry
 */
typedef struct scEntry {
    serviceClassT serviceClass;    /* service class of this entry */
    int nextVd;                    /* last vd used for new mcell */
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
          uint16T vdIndex               /* in - vd's index */
          );

statusT
sc_remove_vd(
             struct domain *dmnp,       /* in - ptr to domain with sc table */
             serviceClassT vdSvc,       /* in - vd's svc class */
             uint16T vdIndex            /* in - vd's index */
             );

statusT
sc_valid_vd(
    struct domain *dmnp,       /* in - ptr to domain with sc table */
    serviceClassT reqServices, /* in - required services */
    serviceClassT optServices, /* in - optional services */
    uint16T vdIndex            /* in - vd's index */
    );

statusT
sc_select_vd_for_mcell(
    struct vd **vdpa,          /* out - selected volume */
    struct domain *dmnP,       /* in - domain handle */
    serviceClassTblT *scTbl,   /* in - service class table */
    serviceClassT reqServices, /* in - required service class */
    serviceClassT optServices  /* in - optional service class */
    );

statusT
sc_select_vd_for_stg(
    struct vd **vdpa,          /* out - selected volume */
    struct domain *dmnP,       /* in - domain handle */
    serviceClassT reqServices, /* in - required service class */
    serviceClassT optServices, /* in - optional service class */
    vdIndexT preferVd,         /* in - VdIndex of preferred disk */
    unsigned long *skip_vd_list,  /* in - list of disks to skip */
    uint32T desiredBlks,       /* in - num disk blks needed */
    uint32T minDesiredBlks,    /* in - minimum num disk blks needed */
    int count_this_alloc       /* in - flag to count cluster allocation */
    );

/* NOTE if BS_MAX_VDI is ever > 256, the next 2 defines must change */
#define NEW_VD_SKIP(list) unsigned long list[4] = {0, 0, 0, 0}
#define EMPTY_VD_SKIP(list) (list)[3]=(list)[2]=(list)[1]=(list)[0]=0
#define SET_VD_SKIP(list, vdi) (list)[((vdi)-1)/64] |= 1L << (((vdi)-1) & 63)
#define NOT_VD_SKIP(list, vdi) (((list)[((vdi)-1)/64] & 1L << (((vdi)-1) & 63)) == 0)

#endif /* _SERVICE_CLASSES_ */
