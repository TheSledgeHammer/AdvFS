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
#pragma ident "@(#)$RCSfile: msfs_proplist.c,v $ $Revision: 1.1.145.6 $ (DEC) $Date: 2005/03/04 19:58:08 $"

#define ADVFS_MODULE MSFS_PROPLIST

/*
 * INCLUDES
 */
#include <sys/security.h>
#include <sys/secpolicy.h>
#include <sys/sp_attr.h>

#include <sys/param.h>
#include <sys/systm.h>
#include <sys/user.h>
#include <sys/kernel.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/buf.h>
#include <sys/proc.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/conf.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <sys/specdev.h>
#include <sys/flock.h>
#include <ufs/quota.h>
#include <ufs/inode.h>
#include <ufs/fs.h>
#if	MACH
#include <sys/syslimits.h>
#include <kern/assert.h>
#endif
#include <mach/mach_types.h>
#include <vm/vm_page.h>
#include <vm/vm_vppage.h>
#include <vm/vm_mmap.h>
#include <vm/vm_debug.h>
#include <sys/proplist.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_ods.h>
#include <msfs/bs_access.h>
#include <msfs/bs_bmt.h>
#include <msfs/ms_osf.h>
#include <msfs/fs_dir.h>
#include <msfs/fs_dir_routines.h>
#include    <sys/lock_probe.h>
#include <msfs/msfs_syscalls.h>



extern struct sec_prop_attrs *sec_proplist(char *);

/*
 * MACROS
 */

/*
 * debugging
 */
#ifdef ADVFS_PROPLIST_DEBUG
int MsfsPlTraceLevel = 0;
#define DEBUG(level, action) \
MACRO_BEGIN			\
	if ((MsfsPlTraceLevel>=level))		\
            (action);		\
MACRO_END	
#else 
#define DEBUG(level, action)
#endif /* ADVFS_PROPLIST_DEBUG */


/*
 * name/value limits
 */
int MsfsPlMaxLen = BSR_PL_MAX_LARGE;
#define MSFS_PROPLIST_VALUE_MAX (16384)

/*
 * byte count macros
 */
#define ALLIGN(x1) (((x1) + 7) &~ 7)
#define MSFS_PL_ENTRY_SIZE(x1, x2) (ALLIGN(x1)+ALLIGN(x2))

/*
 * mcell index to byte offset within page
 */
#define CELL_TO_PGOFF(x1) \
  (sizeof(bsMPgT) - sizeof(bsMCT)*(BSPG_CELLS - (x1) - 1) - \
   BSC_R_SZ + sizeof(bsMRT))

/*
 * lock/unlock primary mcell chain, using access pnt & "unlock_action"
 */
#define LOCK_MCELL_CHAIN(x1)                          \

#define UNLOCK_MCELL_CHAIN(x1,x2)                     \

/*
 * to manipulate 64 bit flags field within 32 bit aligned records
 */
/* load x1 from x2; x1 must be long-word aligned; return x1 */
#define FLAGS_READ(x1,x2)                               \
  (                                                     \
   ((uint32T *) &(x1))[0] = ((u_int *)&(x2))[0],        \
   ((uint32T *) &(x1))[1] = ((u_int *)&(x2))[1],        \
   (x1)                                                 \
   )

/* load x1 from x2, x2 must be long */
#define FLAGS_ASSIGN(x1,x2)                             \
  (                                                     \
   ((uint32T *) &(x1))[0] = (u_int)((x2) & 0xffffffff), \
   ((uint32T *) &(x1))[1] = (u_int)((x2) >> 32)         \
   )

/*
 * RECORD TYPES
 */

/*
 * mcell record cursor, used to traverse mcell chain
 */
typedef struct bsRecCur {
  vdIndexT VD;
  bfMCIdT  MC;
  uint32T  pgoff;
  vdIndexT prevVD;
  bfMCIdT  prevMC;
  bfPageRefHT pgRef;
  int      is_pined;
  rbfPgRefHT pgPin;
  bsMPgT   *bmtp;
} bsRecCurT;

/*
 * on-disk record cursor, free of dynamic (in-memory) elements
 */
typedef struct bsOdRecCur {
  vdIndexT VD;
  bfMCIdT  MC;
  uint32T  pgoff;
  vdIndexT prevVD;
  bfMCIdT  prevMC;
} bsOdRecCurT;

#define RECCUR_ISNIL(x1) ((x1).VD == bsNilVdIndex)

/*
 * root-done record, used by set & del to delete entry & unlock chain
 */
typedef struct delPropRootDone {
  bsOdRecCurT hdr;
  bsOdRecCurT end;
  ftxLkT *mcellList_lk;
  int cellsize;
} delPropRootDoneT;

/*
 * CUR - routines to manipulate record cursor 
 */
static statusT
msfs_pl_init_cur(
		 bsRecCurT *cur,                /* out    */
		 vdIndexT  VD,                  /* in     */
		 bfMCIdT   MC                   /* in     */
		 );
static void*
msfs_pl_cur_to_pnt(
		   domainT *domain,             /* in     */
		   bsRecCurT *cur,              /* in/out */
                   statusT *ret                 /* out */
		   );

static void
msfs_pl_deref_cur(
		  bsRecCurT *cur                /* in/out */
		  );
static statusT
msfs_pl_pin_cur(
		domainT *domain,                /* in     */
		bsRecCurT *cur,                 /* in/out */
		int size,                       /* in     */
		ftxHT ftx                       /* in     */
		);
static statusT
msfs_pl_seek_cur(
		 domainT *domain,               /* in     */
		 bsRecCurT *cur,                /* in/out */
		 const int rtype                /* in     */ 
		 );
static statusT 
msfs_pl_next_mc_cur(
		    domainT *domain,            /* in     */
		    bsRecCurT *cur              /* in/out */
		    );

/*
 * SET - routines used to set entries
 */
int
msfs_setproplist(
		 struct vnode *vp,              /* in     */
		 struct uio *uiop,              /* in     */
		 struct ucred *cred             /* in     */
		 );		
static int	
msfs_pl_set_entry(
		  bfAccessT *bfAccess,          /* in     */
                  struct proplist_head *hp,     /* in     */
                  char *prop_data,              /* in     */
                  struct ucred *cred,		/* in     */
                  int sync
                 );
static int
msfs_pl_fill_hdr_image(
		       char *prop_data,            /* in     */
		       struct proplist_head *hp,   /* in     */
		       bsPropListHeadT *hdr_image, /* out    */
		       int cell_size,              /* in     */
		       int *name_resid,            /* out    */
		       int *data_resid,            /* out    */
		       unsigned long flags,         /* in     */
		       uint32T pl_num
		       );

static statusT
msfs_pl_create_rec(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *cur,               /* in/out */
		   ftxHT ftx,                    /* in     */
		   caddr_t obj,                  /* in     */
		   ushort size,                  /* in     */
		   ushort type                   /* in     */
		   );
static int
msfs_pl_alloc_mcell(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *cur,               /* in     */
		   ftxHT ftx,                    /* in     */
		   int size,                     /* in     */
		   ushort nmcell                 /* in     */
		   );
static int
msfs_pl_findhead_setdata(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *hdr,               /* in/out */
		   ftxHT ftx,                    /* in     */
		   int size,                     /* in     */
		   char *prop_data,              /* in     */
		   struct proplist_head *hp,     /* in     */
		   int name_resid,               /* in     */
		   int data_resid,               /* in     */
		   int large,                    /* in     */
		   uint32T pl_num,		 /* in     */
                   int *mcells_alloced            /* out    */
		   );



/*
 * GET - routines used to get entries
 */
int
msfs_getproplist(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 );

int
msfs_getproplist_int(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 );

static int
msfs_pl_get_entry(
		  domainT *domain,              /* in     */
		  bsRecCurT *hdr,               /* in     */
		  struct uio *uiop,             /* out    */
		  char **names,                 /* in     */
		  int *min_buf_size,            /* in/out */
		  int *just_sizing,             /* in/out */
		  int *total_bytes,             /* in/out */
		  int mask,                     /* in     */
		  struct ucred *cred,		/* in     */
		  struct vnode *vp,		/* in     */
		  proplist_sec_attrs_t *sec_info /* out   */
		  );

static statusT 
msfs_pl_get_name(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in/out */
		 char *buffer,                  /* out    */
		 int *last_resid,               /* in/out */
		 int cell_size                  /* in     */
		 );

static int
msfs_pl_get_data(
		 domainT *domain,               /* in     */
                 bsRecCurT *dat,                /* in/out */
		 struct uio *uiop,              /* out    */
		 proplist_sec_attrs_t *sec_info, /* in     */
		 struct vnode *vp,		/* in     */
		 struct ucred *cred,		/* in     */
		 int name_resid,                /* in     */
		 int data_resid,                /* in     */
		 int cell_size                 /* in     */
		 );

/*
 * DEL - routines used to delete entries
 */
int
msfs_delproplist(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int flag,			/* in	  */
                 struct ucred *cred,            /* in     */
		 long xid                       /* in     */
		 );

int
msfs_delproplist_int(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int flag,			/* in	  */
                 struct ucred *cred,            /* in     */
		 long xid,                      /* in     */
                 int setctime                   /* in     */
		 );

void
msfs_pl_register_agents();

static void
msfs_pl_del_root_done(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      );
static void
msfs_pl_del_root_done_int(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      );
static void
msfs_pl_del_data_chain(
		       domainT *domain,         /* in     */
		       bsRecCurT *hdr,          /* in     */
		       bsOdRecCurT *end,        /* in     */
		       ftxHT ftx                /* in     */
		       );
static statusT
msfs_pl_unlink_mcells(
		      domainT *domain,          /* in     */
		      vdIndexT prevVdIndex,     /* in     */
		      bfMCIdT prevMcellId,      /* in     */
		      vdIndexT firstVdIndex,    /* in     */
		      bfMCIdT firstMcellId,     /* in     */
		      vdIndexT lastVdIndex,     /* in     */
		      bfMCIdT lastMcellId,      /* in     */
		      ftxHT ftx                 /* in     */
		      );
int
msfs_pl_rec_validate(
                     struct bfAccess* bfap,   /* in */
                     bsPropListHeadT *hdr_rec /* in */
                     );

/*  
 ******************************************************
 * v3 routines 
 ******************************************************
 */

static int	
msfs_pl_set_entry_v3(
		  bfAccessT *bfAccess,          /* in     */
                  struct proplist_head *hp,     /* in     */
                  char *prop_data,              /* in     */
                  struct ucred *cred 		/* in     */
                 );
static int
msfs_pl_fill_hdr_image_v3(
		       char *prop_data,            /* in     */
		       struct proplist_head *hp,   /* in     */
		       bsPropListHeadT_v3 *hdr_image, /* out    */
		       int cell_size,              /* in     */
		       int *name_resid,            /* out    */
		       int *data_resid,            /* out    */
		       unsigned long flags        /* in     */
		       );

static int
msfs_pl_findhead_setdata_v3(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *hdr,               /* in/out */
		   ftxHT ftx,                    /* in     */
		   int size,                     /* in     */
		   char *prop_data,              /* in     */
		   struct proplist_head *hp,     /* in     */
		   int name_resid,               /* in     */
		   int data_resid,               /* in     */
		   int large,                    /* in     */
                   int *mcells_alloced           /* out    */
		   );
int
msfs_getproplist_int_v3(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 );
static int
msfs_pl_get_entry_v3(
		  domainT *domain,              /* in     */
		  bsRecCurT *hdr,               /* in     */
		  struct uio *uiop,             /* out    */
		  char **names,                 /* in     */
		  int *min_buf_size,            /* in/out */
		  int *just_sizing,             /* in/out */
		  int *total_bytes,             /* in/out */
		  int mask,                     /* in     */
		  struct ucred *cred,		/* in     */
		  struct vnode *vp,		/* in     */
		  proplist_sec_attrs_t *sec_info /* out   */
		  );
static statusT 
msfs_pl_get_name_v3(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in/out */
		 char *buffer,                  /* out    */
		 int *last_resid,               /* in/out */
		 int cell_size                  /* in     */
		 );
static int
msfs_pl_get_data_v3(
		 domainT *domain,               /* in     */
                 bsRecCurT *dat,                /* in/out */
		 struct uio *uiop,              /* out    */
		 proplist_sec_attrs_t *sec_info, /* in     */
		 struct vnode *vp,		/* in     */
		 struct ucred *cred,		/* in     */
		 int name_resid,                /* in     */
		 int data_resid,                /* in     */
		 int cell_size                 /* in     */
		 );
int
msfs_delproplist_int_v3(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int flag,			/* in	  */
                 struct ucred *cred,            /* in     */
		 long xid,                      /* in     */
                 int setctime                   /* in     */
		 );
static void
msfs_pl_del_root_done_int_v3(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      );
/*  
 ******************************************************
 * end v3 routine protos
 ******************************************************
 */

/* msfs_pl_get_locks  - locks for clones only
 * Lock the following locks recursively because they will be
 * taken out in the msfs_getattr code if the file passed in is
 * a clone.  The msfs_getattr code is called by the
 * SEC_PROP_VALIDATE code below.  This must be done prior to
 * the MCELLIST locking to avoid deadlocks with the COW locks
 * in copy-on-write code.
 */
void
msfs_pl_get_locks(
                bfAccessT *bfap,        /* in - possible clone */
                int *clu_clxtnt_locked, /* out - lock flag */
                int *cow_read_locked,   /* out - lock flag */
                int *trunc_xfer_locked  /* out - lock flag */
                )
{
    bfAccessT *orig_bfap=NULL;

    if(bfap->cloneId == 0) {
        /* file is an original or a clone  */
        /* without any changes             */
        if (bfap->bfSetp->cloneId != BS_BFSET_ORIG) {

            /* This is a clone that has not had any metadata
             * (or file pages) COWed. At open time the
             * original's extents were loaded into it's (the
             * clone's) in memory extent map.  This is
             * basically a bug (to be addressed at some future
             * time). We can not trust these extents since the
             * original could have been migrated (which does
             * not cause a COW).  So we need to use the
             * original's bfap not the clone's */

            orig_bfap=bfap->origAccp;
            MS_SMP_ASSERT(orig_bfap);

            if (clu_is_ready()) {
                /* Since we are going to use the original access structure
                 * for obtaining extent maps, we must protect against it
                 * being migrated. Migrate will get the clu_clonextnt_lk
                 * on the clone when migrating the original. So this
                 * will protect us here.
                 * the extent maps are obtained in msfs_getattr() called
                 * by the SEC_PROP_VALIDATE routine.
                 */

                 CLU_CLXTNT_READ_LOCK_RECURSIVE(&bfap->clu_clonextnt_lk);
                 *clu_clxtnt_locked=TRUE;
            }
            COW_READ_LOCK_RECURSIVE(&(orig_bfap->cow_lk) )
            *cow_read_locked=TRUE;
        }

    } else if (bfap->cloneId > 0) {
        /* file is a bonified clone */
        orig_bfap = bfap->origAccp;

        if (clu_is_ready()) {
            /* If this is a cluster then we need to block from getting the
             * clone xtnt map during migration.
             */
            CLU_CLXTNT_READ_LOCK_RECURSIVE(&bfap->clu_clonextnt_lk);
            *clu_clxtnt_locked=TRUE;
        }

        /* block access to the clone's extent maps if truncating the original */
        TRUNC_XFER_READ_LOCK_RECURSIVE(&orig_bfap->trunc_xfer_lk);
        *trunc_xfer_locked=TRUE;

        COW_READ_LOCK_RECURSIVE( &(orig_bfap->cow_lk) )
        *cow_read_locked=TRUE;
    }
    /* else file is a normal file and no locks are taken */

    return;
}


/*
 * REGISTER_AGENTS - called from bs_misc.c:bs_init() to register 
 *   root done and continuation agents used to delete entries
 */

void
msfs_pl_register_agents()
{
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_register_agents\n"));

  sts = ftx_register_agent_n2(
			      FTA_MSFS_SETPROPLIST,
			      NULL,
			      &msfs_pl_del_root_done,
			      NULL,
			      &free_mcell_chains_opx
			      );
  if (sts != EOK) {
    ADVFS_SAD1("msfs_pl_register_agents(1): register failure", sts);
  }

  sts = ftx_register_agent_n2(
			      FTA_MSFS_DELPROPLIST,
			      NULL,
			      &msfs_pl_del_root_done,
			      NULL,
			      &free_mcell_chains_opx
			      );
  if (sts != EOK) {
    ADVFS_SAD1("msfs_pl_register_agents(2): register failure", sts);
  }

  sts = ftx_register_agent_n2(
			      FTA_MSFS_ALLOC_MCELL,
			      NULL,
			      &msfs_pl_del_root_done,
			      NULL,
			      &free_mcell_chains_opx
			      );
  if (sts != EOK) {
    ADVFS_SAD1("msfs_pl_register_agents(3): register failure", sts);
  }
} /* msfs_pl_register_agents() */


/*
 * msfs_pl_rec_validate
 * Validate whatever possible in the proplist record
 * Return 0 if nothing is wrong, 1 if problem
 */
int
msfs_pl_rec_validate(struct bfAccess* bfap,        /* in - access ptr */
                     bsPropListHeadT *hdr_rec /* in - proplist hdr ptr */
                     )
{
  int error = 0, rounded_namelen;

  rounded_namelen = ALLIGN(hdr_rec->namelen);

  if ((hdr_rec->namelen > PROPLIST_NAME_MAX) ||
      (rounded_namelen > (PROPLIST_NAME_MAX+1)) ||
      (hdr_rec->namelen < 1) ||
      (MSFS_PL_ENTRY_SIZE(hdr_rec->namelen,
                          hdr_rec->valuelen) > MsfsPlMaxLen)) {
        error = 1;

  }
  return(error);

}
/*
 * INIT_CUR - initialize record cursor at first record in specified cell
 */

static statusT
msfs_pl_init_cur(
		 bsRecCurT *cur,                /* out    */
		 vdIndexT  VD,                  /* in     */
		 bfMCIdT   MC                   /* in     */
		 )
{

  DEBUG(2,
	printf("msfs_pl_init_cur\n"));

  cur->VD = VD;
  cur->MC = MC;
  cur->pgoff = CELL_TO_PGOFF(cur->MC.cell);
  cur->prevVD = bsNilVdIndex;
  cur->prevMC = bsNilMCId;
  cur->pgRef = NilBfPageRefH;
  cur->is_pined = FALSE;
  cur->bmtp = NULL;
  if((cur->pgoff > (ADVFS_PGSZ-sizeof(bsMRT))) ||
     (cur->MC.cell >= BSPG_CELLS))
       return E_CORRUPT_LIST;
  else
       return EOK;

} /* msfs_pl_init_cur() */

/*
 * CUR_TO_PNT - ref page if needed and return pointer to specified record
 */

static void*
msfs_pl_cur_to_pnt(
		   domainT *domain,             /* in     */
		   bsRecCurT *cur,              /* in/out */
                   statusT *ret                 /* out */
		   )
{
  statusT sts = EOK;

  DEBUG(2,
	printf("msfs_pl_cur_to_pnt\n"));

  if (cur->bmtp == NULL) {
    sts = bmt_refpg(
		   &cur->pgRef,
		   (void **)&cur->bmtp,
		   VD_HTOP(cur->VD, domain)->bmtp,
		   cur->MC.page,
		   BS_NIL
		   );
  }
  *ret = sts;

  if (sts != EOK)
      return (void *)NULL;
  else
      return (void *)(((caddr_t)cur->bmtp) + cur->pgoff);

} /* msfs_pl_cur_to_pnt() */

/*
 * DEREF_CUR - deref page for cursor if needed
 */

void
msfs_pl_deref_cur(
		  bsRecCurT *cur                /* in/out */
		  )
{
  DEBUG(2,
	printf("msfs_pl_deref_cur\n"));

  if (!PGREF_EQL(cur->pgRef, NilBfPageRefH)) {
    (void) bs_derefpg(cur->pgRef, BS_CACHE_IT);
    cur->pgRef = NilBfPageRefH;
  }
  cur->is_pined = FALSE;
  cur->bmtp = NULL;

} /* msfs_pl_deref_cur() */

/*
 * PIN_CUR - pin record and surrounding mcell record headers
 */

statusT
msfs_pl_pin_cur(
		domainT *domain,                /* in     */
		bsRecCurT *cur,                 /* in/out */
		int size,                       /* in     */
		ftxHT ftx                       /* in     */
		)
{
  statusT sts = EOK;

  DEBUG(2,
	printf("msfs_pl_pin_cur\n"));

  if (!cur->is_pined) {
    sts = rbf_pinpg(
		    &cur->pgPin,
		    (void **)&cur->bmtp,
		    VD_HTOP(cur->VD, domain)->bmtp,
		    cur->MC.page,
		    BS_NIL,
		    ftx
		    );
    if (sts != EOK) {
        return sts;
    }
    cur->is_pined = TRUE;
  }

  rbf_pin_record(
		 cur->pgPin, 
		 ((caddr_t) cur->bmtp) + cur->pgoff - sizeof(bsMRT), 
		 size + 2*sizeof(bsMRT) 
		 );
  return sts;
  
} /* msfs_pl_pin_cur() */

/*
 * SEEK_CUR - find next record in mcell chain of type "rtype"
 */

statusT
msfs_pl_seek_cur(
		 domainT *domain,               /* in     */
		 bsRecCurT *cur,                /* in/out */
		 const int rtype                /* in     */ 
		 )
{
  bsMRT   *mrecp;
  caddr_t    rec;
  int     first_pass = TRUE;
  statusT sts        = EOK;

  DEBUG(2,
	printf("msfs_pl_seek_cur\n"));

  while (!RECCUR_ISNIL(*cur)) {

    DEBUG(3,
	  printf("search page %d cell %d\n", cur->MC.page, cur->MC.cell));

    /*
     * search current mcell for next record of type rtype
     */
    rec = (caddr_t) msfs_pl_cur_to_pnt(domain, cur, &sts);

    /* Check for the status of the function call.  If there was some
     * error, then break out of the loop and return the error.
     */
    if (sts != EOK) 
        break;

    mrecp = REC_TO_MREC(rec);
    if (first_pass) {
      mrecp = NEXT_MREC(mrecp);
      first_pass = FALSE;
    }

    while ((mrecp->type != rtype) &&
	   (mrecp->type != BSR_NIL)) {
      mrecp = NEXT_MREC(mrecp);
    }

    /*
     * record found, return it
     */
    if (mrecp->type != BSR_NIL) {
      cur->pgoff = ((caddr_t)MREC_TO_REC(mrecp)) - ((caddr_t)cur->bmtp);
      DEBUG(3,
	    printf("found page %d cell %d\n", cur->MC.page, cur->MC.cell));
      
      if((cur->pgoff > (ADVFS_PGSZ-sizeof(bsMRT))) ||
         (cur->MC.cell >= BSPG_CELLS)) {
	   return E_CORRUPT_LIST;
      } else 
	   return sts;
    }

    /*
     * record not found, scan farther down chain
     */
    sts = msfs_pl_next_mc_cur(domain, cur);
    if (sts != EOK)
        break;

  } /* while (!RECCUR_ISNIL(*cur)) */

  DEBUG(3,
	printf("record not found\n"));
  return sts;
} /* msfs_pl_seek_cur() */

/*
 * NEXT_MC_CUR - advance cursor to the next mcell
 */

statusT 
msfs_pl_next_mc_cur(
		    domainT *domain,            /* in     */
		    bsRecCurT *cur              /* in/out */
		    )
{
  bsMCT *mcellp;
  vdIndexT nextVD;
  bfMCIdT  nextMC;
  statusT sts = EOK;
  
  DEBUG(2,
	printf("msfs_pl_next_mc_cur\n"));

  (void) msfs_pl_cur_to_pnt(domain, cur, &sts);
  if (sts != EOK) {
      return sts;
  }
  mcellp = &cur->bmtp->bsMCA[cur->MC.cell];

  if (mcellp->nextVdIndex == bsNilVdIndex) {

    /*
     * end of the line, clear cur 
     */
    msfs_pl_deref_cur(cur);
    sts = msfs_pl_init_cur(cur, bsNilVdIndex, bsNilMCId);
    if (sts != EOK) {
        return sts;
    }

  } else {

    /*
     * save next cell pointers
     */
    mcellp = &cur->bmtp->bsMCA[cur->MC.cell];
    nextVD = mcellp->nextVdIndex;
    nextMC = mcellp->nextMCId;

    /*
     * if cell on different page, lose ref
     */
    if (
	(cur->VD != mcellp->nextVdIndex) ||
	(cur->MC.page != mcellp->nextMCId.page)
	) {

      msfs_pl_deref_cur(cur);

    }

    /*
     * advance cur, keep consistent with msfs_pl_init_cur()
     */
    cur->prevVD = cur->VD;
    cur->prevMC = cur->MC;
    cur->VD = nextVD;
    cur->MC = nextMC;
    cur->pgoff = CELL_TO_PGOFF(cur->MC.cell);

  }
  if ((cur->pgoff > (ADVFS_PGSZ - sizeof(bsMRT))) ||
      (cur->MC.cell >= BSPG_CELLS)) {
	return E_CORRUPT_LIST;
  } else
  	return sts;
}

/*
 * ============================================================================
 * === system call entry point === VOP_SETPROPLIST
 * ============================================================================
 */

int
msfs_setproplist_int(
		 struct vnode *vp,              /* in     */
		 struct uio *uiop,              /* in     */
		 struct ucred *cred,            /* in     */
                 int setctime,                  /* in     */
                 int sync                       /* in     */
		 )

{
	struct proplist_head *proplist_headp;
	char *prop_data;
	int error, entry_resid, rounded_namelen;
	bfAccessT *bfAccess;
	struct fsContext *context_ptr = VTOC(vp);
        bfParamsT *bfparamsp = NULL;
        advfs_opT *advfs_op;
        mlBfAttributesT *bfattrp = NULL;

	DEBUG(1, 
	      printf("msfs_setproplist_int\n"));

	bfAccess = VTOA(vp);

	/*
	 * screen domain panic
	 */
	if (bfAccess->dmnP->dmn_panic) {
	    return EIO;
	}
	  
	/*
	 * do copy-on-write, if needed
	 */
        if ( bfAccess->bfSetp->cloneSetp != NULL ) {
	    bs_cow(bfAccess, COW_NONE, 0, 0, FtxNilFtxH);
        }

	proplist_headp = 
	       (struct proplist_head *)ms_malloc(sizeof(struct proplist_head));
	prop_data = (char *)0;

	error = 0;
	while (uiop->uio_resid >= SIZEOF_PROPLIST_HEAD) {
		/*
		 * Copyin header (not name/value)
		 */
		error = uiomove((caddr_t)proplist_headp,
				SIZEOF_PROPLIST_HEAD,
				uiop);
		if (error) {
			DEBUG(1,
			      printf("msfs_setproplist_int: header uiuomove\n"));
			goto out;
		}

		DEBUG(2,
		      printf("msfs_setproplist_int:flags %x namelen %d valuelen %d\n",
			     proplist_headp->pl_flags,
			     proplist_headp->pl_namelen,
			     proplist_headp->pl_valuelen));

		entry_resid = proplist_headp->pl_entrysize -
			SIZEOF_PROPLIST_HEAD;

		/*
		 * Validate header
		 */
		rounded_namelen = ALLIGN(proplist_headp->pl_namelen);
		if (
		    ((proplist_headp->pl_flags & BSR_PL_RESERVED) != 0) ||
		    (proplist_headp->pl_entrysize % 8) ||
		    (proplist_headp->pl_namelen > PROPLIST_NAME_MAX) ||
                    (rounded_namelen > (PROPLIST_NAME_MAX+1)) ||
		    (proplist_headp->pl_namelen < 1) ||
		    (proplist_headp->pl_entrysize <
		     SIZEOF_PROPLIST_ENTRY(proplist_headp->pl_namelen,
					   proplist_headp->pl_valuelen)) ||
		    (MSFS_PL_ENTRY_SIZE(proplist_headp->pl_namelen,
					proplist_headp->pl_valuelen) >
		     MsfsPlMaxLen) ||
		    (entry_resid > uiop->uio_resid)
		    ) {
			DEBUG(1,
			      printf("msfs_setproplist_int: Header invalid\n"));
			error = EINVAL;
			goto out;
		}

		/*
		 * Copy in the name leaving uiop integer aligned
		 */
		error = uiomove((caddr_t)proplist_headp->pl_name,
				rounded_namelen, uiop);
		if (error) {
			DEBUG(1,
			      printf("msfs_setproplist_int: name uiomove\n"));
			goto out;
		}
		entry_resid -= rounded_namelen;

		/*
		 * Validate name length with header
		 */
		if ((strlen(proplist_headp->pl_name) + 1) !=
		    proplist_headp->pl_namelen) {
			DEBUG(1,
			      printf("msfs_setproplist_int: Name invalid\n"));
			error = EINVAL;
			goto out;
		}

		if (proplist_headp->pl_valuelen) {

			prop_data = (char *)ms_malloc(ALLIGN(proplist_headp->pl_valuelen));

			if (error = uiomove(prop_data,
					    ALLIGN(proplist_headp->pl_valuelen),
					    uiop))
		     		goto out;
		}

                /*
                 * Check for the reserved property list name which is used
                 * to perform operations that must work both locally and
                 * across NFS.  These operations do not actually modify on-disk
                 * or in-memory property lists.  The property list mechanism
                 * is merely used since it is a convenient vehicle to get
                 * requests across NFS.  This code assumes that no other 
                 * property list values will be set in this call.
                 */
                if (!strcmp(proplist_headp->pl_name, "DEC_ADVFS_OPERATION")) {
          	    /* 
		     * ensure that the op was correctly given; 
		     * if no pl_value was supplied, then this pointer
		     * remains null
		     */
		    if (! prop_data ) {
			error = EINVAL; 
			goto out;
		    }
                    advfs_op = (advfs_opT *)prop_data;
                    switch (advfs_op->operation) {

                       /*
                        * Atomic write data logging and forced
                        * synchronous writes.
                        */
                        case ADVFS_AW_DATA_LOGGING:
                        case ADVFS_SYNC_WRITE:

                            /* 
                             * Make sure the caller is authorized to do this.
                             */
                            if ((advfs_op->action == ADVFS_ACTIVATE) ||
                                (advfs_op->action == ADVFS_DEACTIVATE)) {
                                /*
                                 * Create a dummy mlBfAttributesT structure
                                 * so that check_privilege() test for the
                                 * the current dataSafety setting being
                                 * different than bfap->dataSafety will
                                 * succeed.
                                 */
                                bfattrp = (mlBfAttributesT *)ms_malloc(
                                           sizeof( mlBfAttributesT ));

                                if (advfs_op->operation == ADVFS_AW_DATA_LOGGING) {
                                    bfattrp->dataSafety = BFD_FTX_AGENT;
                                } else {
                                    bfattrp->dataSafety = BFD_SYNC_WRITE;
                                }
                                error = check_privilege(bfAccess, bfattrp);
                                if (error != EOK) {
                                    error = EPERM;
                                    break;
                                }
                            }
        
                            /*
                             * Get some storage for the bitfile parameters 
                             * structure so we don't use too much stack.
                             */
                            bfparamsp = 
                                     (bfParamsT *) ms_malloc(sizeof(bfParamsT));
        
                            /*
                             * Get the current bitfile parameters.
                             */
                            error = bs_get_bf_params(bfAccess, 
                                                     bfparamsp, 0);
                            if (error != EOK) {
                                error = BSERRMAP(error);
                                break;
                            }
            
                            /*
                             * set the new dataSafety * value in the 
			     * BSR_ATTR record and in the access structure.
                             */
                            if (advfs_op->operation == ADVFS_AW_DATA_LOGGING) {
                                if (advfs_op->action == ADVFS_ACTIVATE) {
                                    bfparamsp->cl.dataSafety = BFD_FTX_AGENT;
                                }
                                else {
                                    /*
                                     * Request was to deactivate atomic 
                                     * write data logging.  Make sure it's 
                                     * on first.
                                     */
                                    if (bfparamsp->cl.dataSafety != 
                                        BFD_FTX_AGENT) {
                                        error = EINVAL;
                                        break;
                                    }
                                    bfparamsp->cl.dataSafety = BFD_NIL;
                                }
                            }
                            else {
   
                                /* Operation is ADVFS_SYNC_WRITE. */
 
                                if (advfs_op->action == ADVFS_ACTIVATE) {
                                    bfparamsp->cl.dataSafety = BFD_SYNC_WRITE;
                                }
                                else {
                                    /*
                                     * Request was to deactivate forced 
                                     * synchronous writes.  Make sure it's
                                     * on first.
                                     */
                                    if (bfparamsp->cl.dataSafety != 
                                        BFD_SYNC_WRITE) {
                                        error = EINVAL;
                                        break;
                                    }
                                    bfparamsp->cl.dataSafety = BFD_NIL;
                                }
                            }
        
                            error = bs_set_bf_params(bfAccess, bfparamsp);

                            if (error != EOK) {
                                error = BSERRMAP(error);
                                break;
                            }
                            break;
                        default:
                            error = EINVAL;
                            break;

                    }  /* switch */

                    goto out;
                }  /* AdvFS_operation */

                
		/*
		 * set entry
		 */

                if (bfAccess->dmnP->dmnVersion 
				>= FIRST_NUMBERED_PROPLIST_VERSION) {
		    error = msfs_pl_set_entry(
					  bfAccess,
					  proplist_headp,
					  prop_data,
					  cred,
                                          sync
					  );
                } else {
		    error = msfs_pl_set_entry_v3(
					  bfAccess,
					  proplist_headp,
					  prop_data,
					  cred
					  );
                }



		if (error) 
		  goto out;

		if (prop_data) {
			ms_free(prop_data);
			prop_data = (char *)0;
		}
	}

        if (setctime)
        {
	    mutex_lock( &context_ptr->fsContext_mutex);
  	    context_ptr->fs_flag |= MOD_CTIME;
    	    context_ptr->dirty_stats = TRUE;
	    mutex_unlock( &context_ptr->fsContext_mutex);
         }
     
out:

  if (bfattrp) {
    ms_free(bfattrp);
  }
  if (bfparamsp) {
    ms_free(bfparamsp);
  }
  if (proplist_headp) {
    ms_free(proplist_headp);
  }
  if (prop_data) {
    ms_free(prop_data);
  }

  return(error);

} /* msfs_setproplist_int() */

msfs_setproplist(
		 struct vnode *vp,              /* in     */
		 struct uio *uiop,              /* in     */
		 struct ucred *cred             /* in     */
		 )
{
    struct vattr    check_vattr;
    int	            error;
    bfAccessT *bfap = VTOA(vp);

        if ( BS_BFTAG_RSVD(bfap->tag)){
            /* Reserved files can't have property lists. */
            return EINVAL;
        }


	VOP_GETATTR(vp, &check_vattr, cred, error);
	if (error) {
	    return error;
        }

	if (cred->cr_uid != check_vattr.va_uid &&
#if SEC_PRIV
	    !privileged(SEC_OWNER, EACCES)
#else
	    suser(cred, &u.u_acflag)
#endif
	    )
	    error = EACCES;
	else
	    error = msfs_setproplist_int(vp, uiop, cred, SET_CTIME, 0);

	return BSERRMAP(error);
}


/*
 * SET_ENTRY - use name and header in hp and data in uiop to set entry
 */

int
msfs_pl_set_entry(
       		  bfAccessT *bfAccess,          /* in     */
                  struct proplist_head *hp,     /* in     */
                  char *prop_data,              /* in     */
		  struct ucred *cred,		/* in     */
		  int sync                      /* in     */
                 )

{
  int error = 0, root_done = FALSE, ftx_started = FALSE;
  char *name_buf = NULL;
  int valuelen, entry_size, large, cell_size, 
      page, name_resid, data_resid;
  bsRecCurT hdr, tmp_hdr;
  statusT sts = EOK;
  ftxHT ftx;
  delPropRootDoneT rdr;
  bsPropListHeadT *hdr_rec;
  char *hdr_image = NULL;
  proplist_sec_attrs_t sec_info;
  uint64T flags;
  uint32T largest_pl_num, pl_num;
  int got_name=0, mcells_alloced=0;
  int clu_clxtnt_locked=FALSE,
      cow_read_locked=FALSE,
      trunc_xfer_locked=FALSE;

  DEBUG(1,
	printf("msfs_pl_set_entry\n"));

  sec_info.sec_type = NULL;

  hdr_image = (char *) ms_malloc( BSR_PROPLIST_PAGE_SIZE );

  name_buf = (char *) ms_malloc( PROPLIST_NAME_MAX + 1 );

  /*
   * start transaction
   *
   * We have to start the transaction before locking the mcell
   * list for ordering consistency.
   */
  sts = FTX_START_N(FTA_MSFS_SETPROPLIST, &ftx, FtxNilFtxH, bfAccess->dmnP, 1);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  ftx_started = TRUE;

  /* lock down the clone mcells, if existing */
  msfs_pl_get_locks( bfAccess,
                     &clu_clxtnt_locked,
                     &cow_read_locked,
                     &trunc_xfer_locked);

  MCELLIST_LOCK_WRITE( &(bfAccess->mcellList_lk) )

  /* indicates to root-done code routine that cell_size is based on hdr flag */
  rdr.cellsize = 0;

  /*
   * search mcell chain for name
   *
   * Also, determine largest_pl_num if not already stored
   * in bfAccess->largest_pl_num
   */

  largest_pl_num = bfAccess->largest_pl_num; 

  sts = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  while (1) {
    error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
    if (error)
        goto out;
    if (RECCUR_ISNIL(hdr))
      break;

    hdr_rec = (bsPropListHeadT *) msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, &sts);
    if (sts != EOK) {
        error = sts;
        goto out;
    }
    if (msfs_pl_rec_validate(bfAccess,hdr_rec)){
        error = E_CORRUPT_LIST;
        goto out;
    }

    if (hdr_rec->pl_num > largest_pl_num)
        largest_pl_num = hdr_rec->pl_num;

    if (got_name) 
        continue;
    
    if ((FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0) {
      /* save header location incase we need to delete it */
      rdr.hdr = *((bsOdRecCurT *) &hdr);

      /* get name portion */
      entry_size = MSFS_PL_ENTRY_SIZE(hdr_rec->namelen, hdr_rec->valuelen);
      valuelen = ALLIGN(hdr_rec->valuelen);
      MS_SMP_ASSERT(valuelen <= MSFS_PROPLIST_VALUE_MAX);
      large = (entry_size > BSR_PL_MAX_SMALL);
      page  = (entry_size > BSR_PL_MAX_LARGE);

      cell_size = (large ? 
		   (page ? BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE) : 
		   BSR_PROPLIST_HEAD_SIZE + entry_size);

      /*
       * msfs_pl_get_name may advance the cursor to a proplist data record,
       * so hdr_rec cannot be used after this call.
       */
      pl_num = hdr_rec->pl_num;
      sts = msfs_pl_get_name(bfAccess->dmnP, &hdr, name_buf, 
		       &name_resid, cell_size);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      if (strcmp(hp->pl_name, name_buf) == 0) {
          got_name = 1;
          tmp_hdr = hdr;
          if (bfAccess->largest_pl_num == largest_pl_num)
              break;
      }
    }
  } /* end while(1) */


  /*
   * if found, delete entry with root done
   */

  if (got_name) {

    DEBUG(2,
	  printf("msfs_pl_set_entry: found existing %s\n", name_buf));

    /*
     * advance hdr to beyond end of current entry,
     * new entry goes after
     */

    if (RECCUR_ISNIL(hdr)) {
        tmp_hdr.bmtp = NULL;	/* page is no longer referenced */
    }

    hdr = tmp_hdr;
    error = msfs_pl_get_data(bfAccess->dmnP, &hdr, NULL, &sec_info,
			     bfAccess->bfVp ,cred, name_resid, valuelen, cell_size);

    if (error) {
      goto out;
    }

    rdr.end = *((bsOdRecCurT *) &hdr);

    DEBUG(2,
	  printf("msfs_pl_set_entry: del prev %d hdr %d end %d\n", 
		 rdr.hdr.prevMC.cell, rdr.hdr.MC.cell, rdr.end.MC.cell));

    rdr.mcellList_lk = &bfAccess->mcellList_lk;
    root_done = TRUE;

  } else {
      largest_pl_num++;
  }

  /*
   * deref curs for call to findhead_setdata
   */
  msfs_pl_deref_cur(&hdr);


  if(!error && (sec_info.sec_type = sec_proplist(hp->pl_name))) {

    /*
     * If this is a security attribute, determine if the process 
     * has the ability to retrieve it.
     */
  
    SEC_PROP_ACCESS(error, sec_info, bfAccess->bfVp, cred, ATTR_SET);
    if(error)
      goto out;
  
   
    /*
     * If this is a security attribute, then build the
     * policy specfic structure.
     */
    
    SEC_PROP_BUILD_SEC(error, sec_info, hp->pl_name, bfAccess->bfVp,
	                 cred, prop_data, hp->pl_valuelen, ATTR_SET);

    /*
     * Verify that if the attribute is a security
     * attribute that it is valid according to the
     * policy.
     */

     if(!error)
        SEC_PROP_VALIDATE(error, sec_info, bfAccess->bfVp, ATTR_SET);
     if (error)
	goto out;
  }


  /*
   * allocate resources
   */
  entry_size = MSFS_PL_ENTRY_SIZE(hp->pl_namelen, hp->pl_valuelen);
  large = (entry_size > BSR_PL_MAX_SMALL);
  page  = (entry_size > BSR_PL_MAX_LARGE);
  cell_size = (large ? 
	       (page ? BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE) : 
	       BSR_PROPLIST_HEAD_SIZE + entry_size);



  error = msfs_pl_fill_hdr_image(
				 prop_data,
				 hp,
				 (bsPropListHeadT *) hdr_image,
				 cell_size,
				 &name_resid,
				 &data_resid,
				 hp->pl_flags | 
				 (large ? BSR_PL_LARGE : 0) |
				 (page  ? BSR_PL_PAGE  : 0),
                                 (got_name ? pl_num : largest_pl_num)
				 );

  if (error) {
    goto out;
  }

  error = msfs_pl_findhead_setdata(
				   bfAccess,
				   &hdr,
				   ftx,
				   cell_size,
				   prop_data,
				   hp,
				   name_resid,
				   data_resid,
				   large,
                                   (got_name ? pl_num : largest_pl_num),
                                   &mcells_alloced
				   );
  if (error) {
    goto out;
  }

  /*
   * write header image
   */
  sts = msfs_pl_pin_cur(bfAccess->dmnP, &hdr, cell_size, ftx);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  hdr_rec = (bsPropListHeadT *) msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, &sts);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  bcopy(hdr_image, hdr_rec, cell_size);

  bfAccess->largest_pl_num = largest_pl_num;

 out:
  if (error == E_CORRUPT_LIST) {
    aprintf("Repairable AdvFs corruption detected, consider running fixfdmn\n");
    ms_uaprintf("Warning: Found corrupted property list record chain\n");
    ms_uaprintf("         Domain: %s\n",bfAccess->dmnP->domainName);
    ms_uaprintf("         Fileset: %s\n",bfAccess->bfSetp->bfSetName);
    ms_uaprintf("         File tag: %d \n",bfAccess->tag.num);
    ms_uaprintf("         Current Disk Index: %d, Previous Disk Index: %d\n",hdr.VD+1,hdr.prevVD+1);
    ms_uaprintf("         Current Page: %d, Previous Page: %d\n",hdr.MC.page,hdr.prevMC.page);
    ms_uaprintf("         Current Mcell: %d, Previous Mcell: %d\n",hdr.MC.cell,hdr.prevMC.cell);
  }
  msfs_pl_deref_cur(&hdr);

  if (ftx_started) {

    if ((error != EOK) && (mcells_alloced)) {
      /* failure -
       * mcells_alloced, which comes only from findhead_setdata, indicates
       * that some mcells were allocated, and must now be deallocated.
       *
       * Don't execute the root done tx - don't want to delete the
       * existing duplicate mcell set since we failed this transaction.
       *
       * Clean up newly allocated mcells explicitly with the subtx's 
       * rootdone, which has already been logged by FTA_MSFS_ALLOC_MCELL's 
       * successful completion.
       */

      DEBUG(1,
	    printf("msfs_pl_set_entry: deleting alloc'ed mcells\n"));
      if (sync) {
          ftx_special_done_mode(ftx, FTXDONE_LOGSYNC);
      }
      ftx_done_urd(ftx, FTA_MSFS_SETPROPLIST, 0, NULL, 0, NULL);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
    }

    else if (error != EOK) {
      /* failure with no mcell allocation.
       * NO mcells were allocated by FTA_MSFS_ALLOC_MCELL.
       */
      ftx_fail(ftx);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
    }

    else if (root_done) {
      /* success - delete old property list duplicates chain
       * with the root done.
       */
      DEBUG(1,
	    printf("msfs_pl_set_entry: deleting existing entry\n"));
      if (sync) {
          ftx_special_done_mode(ftx, FTXDONE_LOGSYNC);
      }
      ftx_done_urd(ftx, FTA_MSFS_SETPROPLIST, 0, NULL,
		   sizeof(rdr), &rdr);

    } else {
      /* success - no old property list chain to clean up. */
      if (sync) {
          ftx_special_done_mode(ftx, FTXDONE_LOGSYNC);
      }
      ftx_done_u(ftx, FTA_MSFS_SETPROPLIST, 0, NULL);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
    }
  } 


  ms_free( name_buf );
  ms_free( hdr_image );

  /*
   * If this a security attribute perform the
   * the required completion operations.
   */

  if(!error)
   SEC_PROP_COMPLETION(error, sec_info, ATTR_SET, bfAccess->bfVp);

  /*
   * If we have space allocated by SEC_PROP_BUILD_SEC
   * release it.
   */

  SEC_PROP_DATA_FREE(sec_info);

  if(cow_read_locked==TRUE) {
      COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
      cow_read_locked=FALSE;
  }
  if(trunc_xfer_locked==TRUE) {
      TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
      trunc_xfer_locked=FALSE;
  }
  if(clu_clxtnt_locked== TRUE) {
      CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
      clu_clxtnt_locked=FALSE;
  }

  return error;

} /* msfs_pl_set_entry() */




/*
 * FILL_HDR_IMAGE - use info in hp to create in-memory image of on-disk
 *   header record
 */

int
msfs_pl_fill_hdr_image(
		       char *prop_data,             /* in     */
		       struct proplist_head *hp,    /* in     */
		       bsPropListHeadT *hdr_image,  /* out    */
		       int cell_size,               /* in     */
		       int *name_resid,             /* out    */
		       int *data_resid,             /* out    */
		       unsigned long flags,         /* in     */
                       uint32T pl_num               /* in     */
		       )
{
  int error = 0, name_xfer, data_xfer;

  DEBUG(1,
	printf("msfs_pl_fill_hdr_image\n"));

  /*
   * header info
   */
  hdr_image->flags = flags;

  DEBUG(2,
	printf("hdr_image->flags %lx\n", hdr_image->flags));

  hdr_image->pl_num = pl_num; 

  hdr_image->namelen = hp->pl_namelen;
  hdr_image->valuelen = hp->pl_valuelen;

  /*
   * name portion
   */
  *name_resid = ALLIGN(hp->pl_namelen);
  name_xfer = MIN(*name_resid, cell_size - BSR_PROPLIST_HEAD_SIZE);
  if (name_xfer > 0) {
    bcopy(hp->pl_name, hdr_image->buffer, name_xfer);
    *name_resid -= name_xfer;
  }

  *data_resid = ALLIGN(hp->pl_valuelen);
  data_xfer = MIN(*data_resid, cell_size - BSR_PROPLIST_HEAD_SIZE - name_xfer);
  if (data_xfer > 0) {
    bcopy(prop_data, (char *)&hdr_image->buffer[name_xfer], data_xfer);
    *data_resid -= data_xfer;      
  }

  return error;
}



static statusT
msfs_pl_create_rec(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *cur,               /* in/out */
		   ftxHT ftx,                    /* in     */
		   caddr_t obj,                  /* in     */
		   ushort size,                  /* in     */
		   ushort type                   /* in     */
		   )
{
    caddr_t dat_rec;
    bsMRT *mrecp;
    statusT sts;

    DEBUG(1,
	  printf("msfs_pl_create_rec\n"));
    DEBUG(2,
	  printf("obj %lx size %d type %d\n", obj, size, type));

    /*
     * move cur onto it and pin
     */
    sts = msfs_pl_next_mc_cur(bfAccess->dmnP, cur);
    if (sts != EOK) {
        return sts;
    }
    sts = msfs_pl_pin_cur(
		    bfAccess->dmnP,
		    cur,
		    size,
		    ftx
		    );
    if (sts != EOK) {
        return sts;
    }

    /*
     * fill in record header and empty record after
     */
    dat_rec = (caddr_t) msfs_pl_cur_to_pnt(bfAccess->dmnP, cur, &sts);
    DEBUG(3,
	  printf("dat_rec %lx\n", dat_rec));
    if (sts == EOK) {
        mrecp = REC_TO_MREC(dat_rec);
        mrecp->type = type;
        mrecp->bCnt = size + sizeof(bsMRT);
        mrecp->version = 0;
        mrecp = NEXT_MREC(mrecp);
        mrecp->type = 0;
        mrecp->bCnt = 0;
        mrecp->version = 0;
    
        /*
         * fill in data
         */
        bcopy(obj, dat_rec, size);

        DEBUG(2,
              printf("msfs_pl_create_rec cell %d size %d type %d\n", 
                     cur->MC.cell, 
                     size,
                     type));
    }
    return sts;
}



/*
 * find or create an empty header record of size "size"
 * fill remaining name and value info into data 
 *   (type=BSR_PROPLIST_DATA) cells
 */

int
msfs_pl_findhead_setdata(
		       bfAccessT *bfAccess,           /* in     */
		       bsRecCurT *hdr,                /* in/out */
		       ftxHT ftx,                     /* in     */
		       int size,                      /* in     */
		       char *prop_data,               /* in     */
		       struct proplist_head *hp,      /* in     */
		       int name_resid,                /* in     */
		       int data_resid,                /* in     */
		       int large,                     /* in     */
                       uint32T pl_num,                /* in     */
                       int *mcells_alloced             /* out    */
		       )
{

  int              error = 0, 
                   space,
                   alloced = 0,
                   i;
  bsOdRecCurT      org;
  bsPropListHeadT *hdr_rec;
  bsMRT           *mrecp, 
                  *top;
  int              search_size = size + sizeof(bsMRT), 
                   first = TRUE;
  bsPropListPageT *dat_rec;
  int              name_xfer, 
                   data_xfer;
  bsRecCurT        dat;
  ushort           n_mcell_alloc;
  uint64T          flags;
  statusT          sts=EOK;
  caddr_t          recp;

  DEBUG(1,
	printf("msfs_pl_findhead_setdata\n"));

  *mcells_alloced = 0;

  if (large) {
    dat_rec = (bsPropListPageT *) ms_malloc(sizeof(bsPropListPageT));
  }

  /*
   * if entry not found, insert at top of chain, else after previous entry
   */
  if (RECCUR_ISNIL(*hdr)) {
    error = msfs_pl_init_cur(hdr, bfAccess->primVdIndex, bfAccess->primMCId);
    org = *((bsOdRecCurT *) hdr);
  } else {
    org = *((bsOdRecCurT *) hdr);
    error = msfs_pl_init_cur(hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  }
  if (error) {
      if (large) {
        ms_free(dat_rec);
      }
      return error;
  }

  /*
   * search prim mcell chain for deleted header record of the right size
   */

  do {
    error = msfs_pl_seek_cur(bfAccess->dmnP, hdr, BSR_PROPLIST_HEAD);
    if (error) {
	if (large) {
	  ms_free(dat_rec);
	}
        return error;
    }

  } while (
	   (!RECCUR_ISNIL(*hdr)) &&
	   (
	    hdr_rec = (bsPropListHeadT *) 
	    msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr, &sts),
	    (sts == EOK) && ((REC_TO_MREC(hdr_rec)->bCnt != search_size) ||
	    ((FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0))
	    )
	   );

  if (sts != EOK) {
      if (large) {
        ms_free(dat_rec);
      }
      return sts;
  }

  /* Determine how many mcells need to be allocated; just data chain
     counted here.
  */

  if (large && name_resid+data_resid)
    n_mcell_alloc = (name_resid+data_resid-1)/(size - NUM_SEG_SIZE) + 1;
  else n_mcell_alloc = 0;


  /* Note - this block contains the first instance of pinning a record.
     Make sure all allocations have been done prior to this.  No ftx_fail
     calls (or error returns from this routine) allowed once the record is
     pinned.
  */
  if (RECCUR_ISNIL(*hdr)) {
      
    /*
     * record not found, create new one after original search position
     */
    sts = msfs_pl_init_cur(hdr, org.VD, org.MC);
    if (sts != EOK) {
        error = sts;
        goto out;
    }

    /*
     * look for spot in existing mcell
     */
    do {
      if (!first) {
	sts = msfs_pl_next_mc_cur(bfAccess->dmnP, hdr);
        if (sts != EOK) {
            error = sts;
            goto out;
        }
      }
      first = FALSE;
      if (!RECCUR_ISNIL(*hdr)) {
	recp = (caddr_t) msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr, &sts);
        if (sts != EOK) {
            error = sts;
            goto out;
        }
        top = mrecp = REC_TO_MREC(recp);
	while (mrecp->type != BSR_NIL) {
	  mrecp = NEXT_MREC(mrecp);
	}
	space = BSC_R_SZ - ((caddr_t)mrecp - (caddr_t)top) - 2*sizeof(bsMRT);
	DEBUG(2,
	      printf("free space in VD %d page %d cell %d is %d\n",
		     hdr->VD, hdr->MC.page, hdr->MC.cell, space));
      }
    } while (
	     (!RECCUR_ISNIL(*hdr)) &&
	     (space < size)
	     );

    if (RECCUR_ISNIL(*hdr)) {
      
      /*
       * no spot in existing cell, create record in new cell
       */

      hdr_rec = (bsPropListHeadT *) ms_malloc(size);

      FLAGS_ASSIGN(hdr_rec->flags,BSR_PL_DELETED);
      sts = msfs_pl_init_cur(hdr, org.VD, org.MC);
      if (sts != EOK) {
	  ms_free(hdr_rec);
          error = sts;
          goto out;
      }

      /*
       * allocate all the mcells needed for this proplist entry
       */
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				hdr,
				ftx,
				size,
				++n_mcell_alloc
				);
      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
	  ms_free(hdr_rec);
          error = ENOMEM;
          goto out;
      }

      (void)msfs_pl_create_rec(
			       bfAccess,
			       hdr,
			       ftx,
			       (caddr_t) hdr_rec,
			       size,
			       BSR_PROPLIST_HEAD
			       );
      ms_free(hdr_rec);

    } else {

      /*
       * allocate all the mcells needed for this proplist entry
       */
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				hdr,
				ftx,
				size,
				n_mcell_alloc
				);

      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
          error = ENOMEM;
          goto out;
      }

      /*
       * move cur onto it and pin
       */
      hdr->pgoff = (((caddr_t)mrecp) - (caddr_t)hdr->bmtp)
	+ sizeof(bsMRT);
      if (hdr->pgoff > (ADVFS_PGSZ - sizeof(bsMRT))) {
	   error = E_CORRUPT_LIST;
	   goto out;
      } 
      msfs_pl_pin_cur(
		      bfAccess->dmnP,
		      hdr,
		      size,
		      ftx
		      );
      
      /*
       * fill in record header and empty record after
       */
      hdr_rec = (bsPropListHeadT *) msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr, &sts);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      mrecp = REC_TO_MREC(hdr_rec);
      mrecp->type = BSR_PROPLIST_HEAD;
      mrecp->bCnt = size + sizeof(bsMRT);
      mrecp->version = 0;
      mrecp = NEXT_MREC(mrecp);
      mrecp->type = 0;
      mrecp->bCnt = 0;
      mrecp->version = 0;
    
      /*
       * fill in data
       */
      FLAGS_ASSIGN(hdr_rec->flags,BSR_PL_DELETED);

    }

  }
  else {
      /*
       * allocate all the mcells needed for this proplist entry
       */
      sts = msfs_pl_init_cur(&dat, hdr->VD, hdr->MC);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				&dat,
				ftx,
				size,
				n_mcell_alloc
				);
      msfs_pl_deref_cur(&dat);
      *mcells_alloced = alloced;

      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
          error = ENOMEM;
          goto out;
      }
  }


  if (!large) {
    goto out;
  }


  /*
   * use separate cur to leave hdr cur pointing to header
   */
  sts = msfs_pl_init_cur(&dat, hdr->VD, hdr->MC);
  if (sts != EOK) {
      error = sts;
      goto out;
  }

  /*
   * fill data cells
   */
  i = 0;
  while (((name_resid + data_resid) > 0) && i++ < alloced) {

    DEBUG(2,
	  printf("name_resid %d data_resid %d\n", name_resid, data_resid));


    dat_rec->pl_num = pl_num;
    dat_rec->pl_seg = i-1;

    /*
     * name portion
     */

    name_xfer = MIN(name_resid, size-NUM_SEG_SIZE);
    bcopy(
	  hp->pl_name + ALLIGN(hp->pl_namelen) - name_resid,
	  dat_rec->buffer,
	  name_xfer
	  );
    name_resid -= name_xfer;

    /*
     * value portion 
     */

    data_xfer = MIN(data_resid, size - name_xfer - NUM_SEG_SIZE);
    if (data_xfer > 0) {
      bcopy(prop_data + ALLIGN(hp->pl_valuelen) - data_resid,
            &dat_rec->buffer[name_xfer],
            data_xfer);
      data_resid -= data_xfer;      
    }

    /*
     * allocate and link new mcell at end of chain
     */
    (void) msfs_pl_create_rec(
			       bfAccess,
			       &dat,
			       ftx,
			       (caddr_t) dat_rec,
			       size,
			       BSR_PROPLIST_DATA
			       );
  } /* while */

  /*
   * dereference dat cur, hdr cur still pointing to header
   */
  msfs_pl_deref_cur(&dat);

out:
  *mcells_alloced = alloced;

  if (large) {
    ms_free(dat_rec);
  }

  return (alloced == n_mcell_alloc ? error : ENOSPC);

} /* msfs_pl_findhead_setdata() */





/*
 * This routine returns # mcells allocated.  A return of 0 indicates
 * no mcells alloc'ed.  A return value different from the requested
 * number, nmcell, indicates a failure of allocation.
 */
static int
msfs_pl_alloc_mcell(
		   bfAccessT *bfAccess,          /* in     */
		   bsRecCurT *cur,               /* in     */
		   ftxHT ftx,                    /* in     */
		   int size,                     /* in     */
		   ushort nmcell                 /* in     */
		   )
{
    statusT sts = EOK;
    bfMCIdT MC = bsNilMCId;
    vdIndexT VD = bsNilVdIndex;
    ftxHT subftx;
    delPropRootDoneT rdr;
    int i;

  DEBUG(1,
	printf("msfs_pl_alloc_mcell: %u mcells\n", nmcell));

  if (nmcell == 0) return 0;

  sts = FTX_START_N(FTA_MSFS_ALLOC_MCELL, &subftx, ftx, bfAccess->dmnP, 0);
  if (sts != EOK)
    return 0;

  for (i = 0; i < nmcell; i++) {
    /*
     * create new cell after current cell
     */
    sts = allocate_link_new_mcell( 
				  bfAccess,
				  cur->VD,
				  cur->MC,
				  subftx,
				  &VD,
				  &MC,
				  (size > BSR_PROPLIST_DATA_SIZE ?
				   BMT_NORMAL_MCELL_PAGE : BMT_NORMAL_MCELL)
				  );
    if (sts == EOK)  DEBUG(1,
      printf ( "old VD.MC-page.cell = %d.%d.%d   new VD.MC-page.cell = %d.%d.%d\n",
      cur->VD, cur->MC.page, cur->MC.cell, VD, MC.page, MC.cell ));
    else DEBUG(1,
      printf ( "allocate_link_new_mcell failed - %d\n", sts ));

    /* save end of chain in case dealloc necessary */
    if (i == 0) {
        rdr.end.VD = VD;
        rdr.end.MC = MC;
        rdr.hdr.VD = bsNilVdIndex;
        rdr.hdr.MC = bsNilMCId;
    }

    /* set up for root-done operation to dealloc mcells */
    if (sts != EOK) {
      if (i) {
        rdr.cellsize     = size;
        rdr.hdr.prevVD   = cur->VD;
        rdr.hdr.prevMC   = cur->MC;
        rdr.hdr.VD       = VD;
        rdr.hdr.MC       = MC;
        rdr.mcellList_lk = &bfAccess->mcellList_lk;
        ftx_done_urd(subftx, FTA_MSFS_ALLOC_MCELL, 0, NULL, sizeof(rdr), &rdr);
        return i;
      } else {
        /* didn't succeed on first pass, nothing to dealloc */
        ftx_done_u(subftx, FTA_MSFS_ALLOC_MCELL, 0, NULL);
        return i;
      }
    }

  }

  ftx_done_u(subftx, FTA_MSFS_ALLOC_MCELL, 0, NULL);
  return i;
}



/*
 * ============================================================================
 * === system call entry point === VOP_GETPROPLIST
 * ============================================================================
 */

int
msfs_getproplist(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 )
{

    int error;
    bfAccessT *bfap;
    int namlen, namelen, vallen, valuelen, entry_size;
    u_int flags;
    bfParamsT *bfparamsp = NULL;

    bfap = VTOA(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
	return EIO;
    }

    if ( BS_BFTAG_RSVD(bfap->tag)) {
        /* Reserved files can't have property lists. */
        return EINVAL;
    }


    /*
     * Check for the reserved property list name which is used
     * to perform operations that must work both locally and
     * across NFS.  These operations do not actually retrieve
     * property list information.  The property list mechanism
     * is merely used since it is a convenient vehicle to get
     * requests across NFS.  This code assumes that no other
     * property list values will be queried in this call.
     */
    if (names && !strcmp(names[0], "DEC_ADVFS_IOMODE")) {
        /*
         * Get some storage for the bitfile parameters
         * structure so we don't use too much stack.
         */
         bfparamsp = (bfParamsT *) ms_malloc(sizeof(bfParamsT));

	/*
         * Get the current bitfile parameters.
         */
         error = bs_get_bf_params(bfap, bfparamsp, 0);
         if (error != EOK) {
            error = BSERRMAP(error);
	    goto out;
	 }

	/* load the proplist header in the uiop */
	 namlen = strlen("DEC_ADVFS_IOMODE") + 1;
	 namelen = PROPLIST_ALLIGN(namlen);
	 flags = 0;
	 vallen = sizeof(bfDataSafetyT);
	 valuelen = PROPLIST_ALLIGN(vallen);
	 entry_size = SIZEOF_PROPLIST_ENTRY(namelen,valuelen);

	 error = uiomove((caddr_t)&entry_size, sizeof(int), uiop);
	 if (error)
	     goto out;

	 error = uiomove((caddr_t)&flags, sizeof(int), uiop);
	 if (error)
	     goto out;

	 error = uiomove((caddr_t)&namlen, sizeof(int), uiop);
	 if (error)
	     goto out;

         error = uiomove((caddr_t)&vallen, sizeof(int), uiop);
         if (error)
	     goto out;
      
         error = uiomove((caddr_t)names[0], namelen, uiop);
         if (error)
	     goto out;

         /* copy the on-disk dataSafety value */
         error = uiomove((caddr_t)&(bfparamsp->cl.dataSafety), vallen, uiop);

         /* copy the in-memory dataSafety value */
         error = uiomove((caddr_t)&(bfap->dataSafety), vallen, uiop);

	 *min_buf_size = 0;

         goto out;
    }

    if (bfap->dmnP->dmnVersion >= FIRST_NUMBERED_PROPLIST_VERSION) {
        error = msfs_getproplist_int(vp, names, uiop, min_buf_size, mask, cred);
    } else {
        error = msfs_getproplist_int_v3(vp, names, uiop, min_buf_size, mask, cred);
    }

out:

    if (bfparamsp) {
        ms_free(bfparamsp);
    }


    return BSERRMAP(error);
}

int
msfs_getproplist_int(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 )

{
	int       error = 0;
	bfAccessT *bfAccess;
	bsPropListHeadT *hdr_rec;
	int       just_sizing, total_bytes;
	bsRecCurT hdr;
	proplist_sec_attrs_t sec_info;
	uint64T flags;
	struct uio uioLcl;
	struct iovec iovLcl;
	char *bbuf = NULL;
        statusT sts = EOK;
        int clu_clxtnt_locked=FALSE,
            cow_read_locked=FALSE,
            trunc_xfer_locked=FALSE;

	DEBUG(1,
	      printf("msfs_getproplist_int\n"));

	bfAccess = VTOA(vp);

	/*
	 * initialize stuff for get_entry
	 */
	just_sizing = FALSE;
	total_bytes = 0;
	*min_buf_size = 0;

	/*
	 * Set up space to collect user's data.
	 * Once the mcellList lock is held, it's necessary to avoid 
	 * copyout's to user space (lock hierarchy).
         * If we're just sizing the property list, don't malloc any storage.
	 */
        if (uiop->uio_iov->iov_len > 0) {
            bbuf = ms_malloc(uiop->uio_iov->iov_len);
        }
	iovLcl.iov_base = bbuf;
	iovLcl.iov_len = uiop->uio_iov->iov_len;
	uioLcl.uio_iov = &iovLcl;
	uioLcl.uio_offset = 0;
	uioLcl.uio_iovcnt = 1;
	uioLcl.uio_resid = uiop->uio_iov->iov_len;
	uioLcl.uio_segflg = UIO_SYSSPACE;
	uioLcl.uio_rw = UIO_READ;

        /* lock down the clone mcells, if existing */
        msfs_pl_get_locks( bfAccess,
                           &clu_clxtnt_locked,
                           &cow_read_locked,
                           &trunc_xfer_locked);

        MCELLIST_LOCK_READ( &(bfAccess->mcellList_lk) )

	/*
	 * search mcell chain for name
	 */
	sts = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
        if (sts != EOK) {
            if (sts == E_CORRUPT_LIST) {
              ms_uaprintf("Warning: Found corrupted property list record chain\n");
              ms_uaprintf("         Domain: %s\n",bfAccess->dmnP->domainName);
              ms_uaprintf("         Fileset: %s\n",bfAccess->bfSetp->bfSetName);
              ms_uaprintf("         File tag: %d \n",bfAccess->tag.num);
              ms_uaprintf("         Current Disk Index: %d, Previous Disk Index: %d\n",hdr.VD+1,hdr.prevVD+1);
              ms_uaprintf("         Current Page: %d, Previous Page: %d\n",hdr.MC.page,hdr.prevMC.page);
              ms_uaprintf("         Current Mcell: %d, Previous Mcell: %d\n",hdr.MC.cell,hdr.prevMC.cell);
            }  
            msfs_pl_deref_cur(&hdr);
            MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
            if(cow_read_locked==TRUE) {
                COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
                cow_read_locked=FALSE;
            }
            if(trunc_xfer_locked==TRUE) {
                TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
                trunc_xfer_locked=FALSE;
            }
            if(clu_clxtnt_locked== TRUE) {
                CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
                clu_clxtnt_locked=FALSE;
            }
	    if (bbuf != NULL) {
		ms_free(bbuf);
	    }
            return(sts);
        }
	do {
	  error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
          if (error) break;
	  if (
	      (!RECCUR_ISNIL(hdr)) &&
	      (hdr_rec = (bsPropListHeadT *)
	       msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, &sts),
               (sts == EOK) &&
	       (FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0)
	      ) {
	    bzero((char *)&sec_info, sizeof (sec_info));
	    error = msfs_pl_get_entry(
				      bfAccess->dmnP,
				      &hdr,
				      &uioLcl,
				      names,
				      min_buf_size,
				      &just_sizing,
				      &total_bytes,
				      mask,
				      cred,
				      vp,
				      &sec_info
				      );

		/*
                 * If we have space allocated by SEC_PROP_BUILD_SEC
                 * release it.
                 */

  	  	SEC_PROP_DATA_FREE(sec_info);
	  }
          if (sts != EOK) {
              error = sts;
              break;
          }
          
	  if (error) break;

	} while (!RECCUR_ISNIL(hdr));
        if (error == E_CORRUPT_LIST) {
          ms_uaprintf("Warning: Found corrupted property list record chain\n");
          ms_uaprintf("         Domain: %s\n",bfAccess->dmnP->domainName);
          ms_uaprintf("         Fileset: %s\n",bfAccess->bfSetp->bfSetName);
          ms_uaprintf("         File tag: %d \n",bfAccess->tag.num);
          ms_uaprintf("         Current Disk Index: %d, Previous Disk Index: %d\n",hdr.VD+1,hdr.prevVD+1);
          ms_uaprintf("         Current Page: %d, Previous Page: %d\n",hdr.MC.page,hdr.prevMC.page);
          ms_uaprintf("         Current Mcell: %d, Previous Mcell: %d\n",hdr.MC.cell,hdr.prevMC.cell);
        }  

	msfs_pl_deref_cur(&hdr);
	MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )

        if(cow_read_locked==TRUE) {
            COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
            cow_read_locked=FALSE;
        }
        if(trunc_xfer_locked==TRUE) {
            TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
            trunc_xfer_locked=FALSE;
        }
        if(clu_clxtnt_locked== TRUE) {
            CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
            clu_clxtnt_locked=FALSE;
        }

	if (just_sizing && !error) {
	  *min_buf_size = total_bytes;
	}

	if(error == EPERM)
		error = 0;

	if (!just_sizing && !error) {
	  error = uiomove(bbuf, uioLcl.uio_offset, uiop);
	  uiop->uio_resid = uioLcl.uio_resid; /* upper layer uses uio_resid */
	}
  
        if (bbuf != NULL) {
          ms_free(bbuf);
        }

	return(error);

} /* msfs_getproplist_int() */


/*
 * GET_ENTRY - write name/value info out to uiop, called with
 *   hdr pointed to the desired entry
 */

int
msfs_pl_get_entry(
		  domainT *domain,              /* in     */
		  bsRecCurT *hdr,               /* in     */
		  struct uio *uiop,             /* out    */
		  char **names,                 /* in     */
		  int *min_buf_size,            /* in/out */
		  int *just_sizing,             /* in/out */
		  int *total_bytes,             /* in/out */
		  int mask,                     /* in     */
		  struct ucred *cred,		/* in     */
		  struct vnode	*vp,		/* in     */
  		  proplist_sec_attrs_t *sec_info /* out   */
		  )

{
  char      name_buf[PROPLIST_NAME_MAX+1];
  int       entry_size, flags, namelen, valuelen;
  bsPropListHeadT *hdr_rec;
  int       error = 0, name_resid, cell_size;
  uint64T rdflags;
  statusT sts = EOK;
  bfAccessT *bfAccess = VTOA(vp);

  DEBUG(1,
	printf("msfs_pl_get_entry\n"));

  /*
   * if entry deleted, exit
   */
  hdr_rec = (bsPropListHeadT *) msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  if (msfs_pl_rec_validate(bfAccess,hdr_rec)){
        error = E_CORRUPT_LIST;
        goto out;
  }

  MS_SMP_ASSERT(REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD);
  if (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_DELETED) {
    goto out;
  }

  /*
   * calculate cell size for larger elements
   */
  cell_size = (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_PAGE ? 
	       BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE);

  /*
   * get name from disk into disk_buffer
   */
  sts = msfs_pl_get_name(domain, hdr, name_buf, &name_resid, cell_size);
  if (sts != EOK) {
      error = sts;
      goto out;
  }

  sec_info->sec_type = NULL;

  /*
   * handle entry
   */
  if (proplist_entry_match(
			   name_buf, 
			   names, 
			   (FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED),
			   mask
			   )) {

    /*
     * found one
     */
    DEBUG(2,
	  printf("msfs_pl_get_entry found %s\n",name_buf));
  
    sec_info->sec_type = sec_proplist(name_buf);

  /*
   * If this is a security attribute, determine if
   * the process can retrieve it.
   * If namelist is null, do not return error, as get_proplist operation
   * should continue with other entries.
   */

   SEC_PROP_ACCESS(error, *sec_info, vp, cred, ATTR_RETRIEVE);
   if(error) {
	if(names == NULL || *names == NULL) error = 0;
	goto out;
   }

    flags = FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED;
    namelen = ALLIGN(hdr_rec->namelen);
    valuelen = ALLIGN(hdr_rec->valuelen);
    entry_size = SIZEOF_PROPLIST_ENTRY(hdr_rec->namelen, hdr_rec->valuelen);
    *total_bytes += entry_size;
    if (!(*just_sizing) &&
	entry_size > uiop->uio_resid) {
      *just_sizing = TRUE;
    }

    /*
     * copy entry
     */
    if (!(*just_sizing)) {

      /*
       * get header info
       */
      error = uiomove((caddr_t)&entry_size,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&flags,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&hdr_rec->namelen,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&hdr_rec->valuelen,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)name_buf,
		      namelen,
		      uiop);
      if (error) {
	goto out;
      }

      /*
       * get value from disk to output
       */

      error = msfs_pl_get_data(domain, hdr, uiop, sec_info, vp, cred,
			       name_resid, valuelen, cell_size);
  
    }

  }

 out:
  
  return (error);

} /* msfs_pl_get_entry() */



/*
 * GET_NAME - read name field of entry pointed to by hdr into buffer
 */

statusT 
msfs_pl_get_name(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in/out */
		 char *buffer,                  /* out    */
		 int *last_resid,               /* in/out */
		 int cell_size                  /* in     */
		 )
{
  bsPropListHeadT *hdr_rec;
  bsPropListPageT *dat_rec;
  int resid, xfer, namelen;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_get_name\n"));

  /*
   * portion in the header
   */
  hdr_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
      return sts;
  }

  MS_SMP_ASSERT(REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD);
  resid = namelen = ALLIGN(hdr_rec->namelen);
  xfer = MIN(resid, cell_size - BSR_PROPLIST_HEAD_SIZE);
  bcopy(hdr_rec->buffer, buffer, xfer);
  *last_resid = resid;
  resid -= xfer;

  /*
   * get rest of name from data chain
   */
  while (resid > 0) {
    sts = msfs_pl_seek_cur(domain, hdr, BSR_PROPLIST_DATA);
    if (sts != EOK) break;
    dat_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
    if (sts != EOK) break;
    MS_SMP_ASSERT(REC_TO_MREC(dat_rec)->type == BSR_PROPLIST_DATA);

    xfer = MIN(resid, cell_size-NUM_SEG_SIZE);
    bcopy(
	  dat_rec->buffer, 	
	  (caddr_t) &buffer[namelen - resid],
	  xfer
	  );
    *last_resid = resid;
    resid -= xfer;
  }

  if (sts != EOK)
      DEBUG(2, printf("error encountered. sts = %d\n", sts));
  else 
      DEBUG(2, printf("name is %s\n", buffer));

  return sts;

} /* msfs_pl_get_name() */


/*
 * GET_DATA - read data portion into uiop, hdr is already pointing
 *   to the first cell that contains data with name_resid bytes of
 *   name data in the current cell that has already been read
 */

int
msfs_pl_get_data(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in     */
		 struct uio *uiop,              /* out    */
  		 proplist_sec_attrs_t *sec_info, /* in     */
		 struct vnode *vp,		/* in     */
		 struct ucred *cred,		/* in     */
		 int name_resid,                /* in     */
		 int data_resid,                /* in     */
		 int cell_size                  /* in     */
		 )
{
  int error = 0, data_xfer, first = TRUE;
  bsPropListHeadT *hdr_rec;
  bsPropListPageT *dat_rec;
  char *sec_buff=NULL;
  int sec_data_offset = 0;
  int nbytes = data_resid;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_get_data\n"));


  /*
   * get portion in header record
   */
  MS_SMP_ASSERT(!RECCUR_ISNIL(*hdr));
  hdr_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
      return sts;
  }

  /*
   *  Allocate space for the attribute if we are dealing
   *  with a security attribute.  This allows us to keep
   *  track of attributes larger then an MCELL so we may 
   *  validate and cache them.
   *
   *  Note:
   * 	If we were called by msfs_setproplist or
   *	msfs_delproplist or this is 
   *	not a security attribute then sec_info->sec_type
   *	is NULL.
   *
   */


  if(sec_info->sec_type) {
	sec_buff = (char *) ms_malloc(nbytes);
  }

  if (REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD) {
    data_xfer = MIN(data_resid, 
		    cell_size - BSR_PROPLIST_HEAD_SIZE - name_resid);
    if ((uiop != NULL) && (data_xfer != 0)) {
      if(sec_buff) {
	bcopy((caddr_t) &hdr_rec->buffer[name_resid],sec_buff+sec_data_offset,data_xfer);
	sec_data_offset = sec_data_offset+data_xfer;
      } else {
         error = uiomove(
		      (caddr_t) &hdr_rec->buffer[name_resid],
		      data_xfer,
		      uiop
		      );
          if (error) {
	      return error;
          }
      }
    }
    name_resid = 0;
    data_resid -= data_xfer;
    first = FALSE;
  }

  /*
   * get portion in data cells, we are assured to be on the last
   * cell containing name data
   */
  while (data_resid > 0) {

    DEBUG(2,
	  printf("reading data chain, data_resid %d\n", data_resid));

    if (!first) {
      error = msfs_pl_seek_cur(domain, hdr, BSR_PROPLIST_DATA);
      if (error != EOK) {
          return error;
      }
      MS_SMP_ASSERT(!RECCUR_ISNIL(*hdr));
    }
    dat_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
    if (sts != EOK) {
        return sts;
    }
    if ( (dat_rec == NULL) || 
	(REC_TO_MREC(dat_rec)->type != BSR_PROPLIST_DATA) ) {
        domain_panic(domain, 
                "msfs_pl_get_data(1) - invalid prop list data on disk. Run fixfdmn.");
        return E_DOMAIN_PANIC;
    }

    /* the DATA mcell may start with remnants of the proplist name */
    data_xfer = MIN(data_resid, cell_size - NUM_SEG_SIZE - name_resid);

    if (uiop != NULL) {
      if(sec_buff) {
	  bcopy((caddr_t) &dat_rec->buffer[name_resid],sec_buff+sec_data_offset,data_xfer);
	  sec_data_offset = sec_data_offset+data_xfer;
      } else {
          error = uiomove(
		      (caddr_t) &dat_rec->buffer[name_resid],
		      data_xfer,
		      uiop
		      );
          if (error) {
	      return error;
          }
       }
    }
    name_resid = 0;
    data_resid -= data_xfer;
    first = FALSE;
  }

  if (!error) {

      /*
       * If this is a security attribute, then build the
       * policy specfic structure.
       */
        if (sec_info && sec_info->sec_type)
           SEC_PROP_BUILD_SEC(error, *sec_info,
                   	      sec_info->sec_type->attr_name, vp,
                   	      cred, sec_buff, hdr_rec->valuelen,
			      ATTR_RETRIEVE);
   }
   if (error) {
      sec_info->sec_type = NULL;   /* Failed, set type to be NULL
                                    * so we do not attempt any security
                                    * operations using this data.
                                    */
    }

 /*
  * Verify that if the attribute is a security
  * attribute that it is valid according to the
  * policy.
  */

  SEC_PROP_VALIDATE(error, *sec_info, vp, ATTR_RETRIEVE);

  /*
   * If this a security attribute perform the
   * the required completion operations.
   */

  if(!error)
	SEC_PROP_COMPLETION(error, *sec_info, ATTR_RETRIEVE, vp);

  if(sec_buff) {
      if((uiop != NULL && !error))
          error = uiomove(
		      sec_buff,
		      nbytes,
		      uiop
		      );

      ms_free(sec_buff);
  }

  return error;

} /* msfs_pl_get_data() */



/*
 * ============================================================================
 * === system call entry point === VOP_DELPROPLIST
 * ============================================================================
 */

int
msfs_delproplist(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int mask,			/* in     */
                 struct ucred *cred,            /* in     */
		 long xid                       /* in     */
		 )
{
    struct vattr check_vattr;
    int error;
    bfAccessT *bfap;

    VOP_GETATTR(vp, &check_vattr, cred, error);

    if (error || cred->cr_uid != check_vattr.va_uid &&
#if SEC_PRIV
        !privileged(SEC_OWNER,EACCES)
#else
	suser(cred, &u.u_acflag)
#endif
       )
    {
            return (EACCES);
    } 

    bfap = VTOA(vp);
    if (bfap->dmnP->dmnVersion >= FIRST_NUMBERED_PROPLIST_VERSION) {
        error = msfs_delproplist_int(vp, names, mask, cred, xid, SET_CTIME);
    } else {
        error = msfs_delproplist_int_v3(vp, names, mask, cred, xid, SET_CTIME);
    }
    
    return BSERRMAP(error);
}

int
msfs_delproplist_int(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int mask,			/* in     */
                 struct ucred *cred,            /* in     */
		 long xid,                      /* in, MBZ iff !CFS */
                 int setctime                   /* in     */
		 )
{
  int error=0,sec_access,access_error=0;
  bfAccessT *bfAccess;
  char *name_buf = NULL;
  int valuelen, name_resid;
  bsRecCurT hdr;
  statusT sts = EOK;
  ftxHT ftx;
  delPropRootDoneT rdr;
  bsPropListHeadT *hdr_rec;
  int entry_size, large, page, cell_size;
  uint64T flags, rdflags;
  proplist_sec_attrs_t sec_info;
  struct bfNode *bnp;
  struct fsContext *rem_context;
  struct vattr check_vattr;
  int clu_clxtnt_locked=FALSE,
      cow_read_locked=FALSE,
      trunc_xfer_locked=FALSE;

  DEBUG(1,
	printf("msfs_pl_delproplist_int\n"));

  name_buf = (char *) ms_malloc( PROPLIST_NAME_MAX + 1);

  sec_info.sec_struct = (void *) NULL;
  bnp = (struct bfNode *)&vp->v_data[0];
  rem_context = bnp->fsContextp;

  name_buf[0] = '\0';
  bfAccess = VTOA(vp);

  /*
   * screen domain panic
   */
  if (bfAccess->dmnP->dmn_panic) {
      ms_free( name_buf );
      return EIO;
  }
	  
  /*
   * do copy-on-write, if needed
   */
  if ( bfAccess->bfSetp->cloneSetp != NULL ) {
      bs_cow(bfAccess, COW_NONE, 0, 0, FtxNilFtxH);
  }


start:
  /*
   * transaction to release root done
   *
   * We have to start the transaction before locking the mcell
   * list for ordering consistency.
   *
   * Note: if not running under CFS,
   * xid must be zero to guarantee uniqueness of
   * ftxId in the transaction log.
   */
  sts = FTX_START_XID(FTA_MSFS_DELPROPLIST, &ftx, FtxNilFtxH, 
                      bfAccess->dmnP, 0, xid);
  if (sts != EOK) {
      ms_free(name_buf);
      return sts;
  }

  /* lock down the clone mcells, if existing */
  msfs_pl_get_locks( bfAccess,
                     &clu_clxtnt_locked,
                     &cow_read_locked,
                     &trunc_xfer_locked);

  MCELLIST_LOCK_WRITE( &(bfAccess->mcellList_lk) )

  /*
   * search mcell chain for name
   */
  error = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  if (error != EOK) 
      goto out;
  
  /*
   * match items in the list
   */
  do { /* while (!RECCUR_ISNIL(hdr)) */

    /*
     * find next match
     */
    while (1) {
      error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
      if (error != EOK) 
          goto out;
      if (RECCUR_ISNIL(hdr)) 
          break;
      
      hdr_rec = (bsPropListHeadT *)msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, 
                                                      &sts);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      if (msfs_pl_rec_validate(bfAccess,hdr_rec)){
        error = E_CORRUPT_LIST;
        goto out;
      }

      if ((FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_DELETED) == 0) {

	/* save header entries for latter processing */
	flags = FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED;
	valuelen = ALLIGN(hdr_rec->valuelen);

	/* save header location in case we need to delete it */
	rdr.hdr = *((bsOdRecCurT *) &hdr);

	/* get name portion */
	entry_size = MSFS_PL_ENTRY_SIZE(hdr_rec->namelen, hdr_rec->valuelen);
	large = (entry_size > BSR_PL_MAX_SMALL);
	page  = (entry_size > BSR_PL_MAX_LARGE);
	cell_size = (large ? 
		     (page ? BSR_PROPLIST_PAGE_SIZE : 
		      BSR_PROPLIST_DATA_SIZE) : 
		     BSR_PROPLIST_HEAD_SIZE + entry_size);
	sts = msfs_pl_get_name(bfAccess->dmnP, &hdr, name_buf, 
			 &name_resid, cell_size);
        if (sts != EOK) {
            error = sts;
            goto out;
        }
	if (proplist_entry_match(name_buf, names, flags, mask))
	  break;
      }
    }

    /*
     * if found, delete entry
     */
    if (!RECCUR_ISNIL(hdr)) {

      sec_info.sec_type = sec_proplist(name_buf);

      /*
       * If this is a security attribute, determine if
       * the process can delete it.
       */

      SEC_PROP_ACCESS(sec_access, sec_info, vp, cred, ATTR_DELETE);

	if(sec_access) {
	   if(sec_access != EPERM) {
	       access_error = sec_access;
	   }
	    continue;
        }
 
      SEC_PROP_BUILD_SEC(error, sec_info, name_buf, vp, cred, NULL,
                         0,ATTR_DELETE);
 
      if(error) 
       goto out;
 
      SEC_PROP_VALIDATE(error, sec_info, vp, ATTR_DELETE);
 
      if(error)  {
         SEC_PROP_DATA_FREE(sec_info);
       goto out;
      }
 
      SEC_PROP_COMPLETION(error, sec_info, ATTR_DELETE, vp);
      SEC_PROP_DATA_FREE(sec_info);
 
      sec_info.sec_type = NULL; /* Set to NULL so we do not perform any 
				 * security operarions the initial 
				 * retriving of data.
				 */
      /*
       * advance hdr to beyond end of current entry,
       * new entry goes after
       */
      error = msfs_pl_get_data(bfAccess->dmnP, &hdr, NULL, &sec_info, vp,
			       cred, name_resid, valuelen, cell_size);
      if (error) {
	goto out;
      }
     
       

      rdr.end = *((bsOdRecCurT *) &hdr);
      rdr.mcellList_lk = &bfAccess->mcellList_lk;
      rdr.cellsize = 0; /* use hdr flag to denote size */
      ftx_done_urd(ftx, FTA_MSFS_DELPROPLIST, 0, NULL,
        sizeof(rdr), &rdr);

      msfs_pl_deref_cur(&hdr);

      if(cow_read_locked==TRUE) {
          COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
          cow_read_locked=FALSE;
      }
      if(trunc_xfer_locked==TRUE) {
          TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
          trunc_xfer_locked=FALSE;
      }
      if(clu_clxtnt_locked== TRUE) {
          CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
          clu_clxtnt_locked=FALSE;
      }
      goto start;

    }

  } while (!RECCUR_ISNIL(hdr)); /* match items in list */

  if (setctime == SET_CTIME) {
      mutex_lock( &rem_context->fsContext_mutex);
      rem_context->fs_flag |= MOD_CTIME;
      rem_context->dirty_stats = TRUE;
      mutex_unlock( &rem_context->fsContext_mutex);
  }

 out:
  if (error == E_CORRUPT_LIST) {
    aprintf("Repairable AdvFs corruption detected, consider running fixfdmn\n");
    ms_uaprintf("Warning: Found corrupted property list record chain\n");
    ms_uaprintf("         Domain: %s\n",bfAccess->dmnP->domainName);
    ms_uaprintf("         Fileset: %s\n",bfAccess->bfSetp->bfSetName);
    ms_uaprintf("         File tag: %d \n",bfAccess->tag.num);
    ms_uaprintf("         Current Disk Index: %d, Previous Disk Index: %d\n",hdr.VD+1,hdr.prevVD+1);
    ms_uaprintf("         Current Page: %d, Previous Page: %d\n",hdr.MC.page,hdr.prevMC.page);
    ms_uaprintf("         Current Mcell: %d, Previous Mcell: %d\n",hdr.MC.cell,hdr.prevMC.cell);
  }
  msfs_pl_deref_cur(&hdr);

  /*
   * cancel final xac
   */
  ftx_quit(ftx);
  MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )

  if(cow_read_locked==TRUE) {
      COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
      cow_read_locked=FALSE;
  }
  if(trunc_xfer_locked==TRUE) {
      TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
      trunc_xfer_locked=FALSE;
  }
  if(clu_clxtnt_locked== TRUE) {
      CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
      clu_clxtnt_locked=FALSE;
  }

  ms_free( name_buf );

  if(error) 
     return error;

  return access_error;

} /* msfs_delproplist_int() */



/*
 * DEL_ROOT_DONE - used by msfs_setproplist() & msfs_delproplist() to
 *   delete an entry
 */

void
msfs_pl_del_root_done(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      )
{

    domainT *domain = ftx.dmnP;;


    if (domain->dmnVersion >= FIRST_NUMBERED_PROPLIST_VERSION) {
        msfs_pl_del_root_done_int(ftx, size, address);
    } else {
        msfs_pl_del_root_done_int_v3(ftx, size, address);
    }
}


void
msfs_pl_del_root_done_int(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      )
{
  bsPropListHeadT *hdr_rec;
  delPropRootDoneT rdr;
  bsRecCurT hdr;
  bsOdRecCurT *hdrp;
  uint64T flags, rdflags;
  ftxLkT *mcellList_lk;
  int large;
  domainT *domain = ftx.dmnP;
  statusT sts = EOK;

 /* TODO: modify to return status */ 

  DEBUG(1,
	printf("msfs_pl_del_root_done\n"));

  /*
   * recover pointer root-done and domain pointers
   */
  bcopy(address, &rdr, sizeof(delPropRootDoneT));

  DEBUG(2,
	printf(
	       "hdr %d\n",
	       rdr.hdr.MC.cell 
	       ));
  /*
   * add mcell chain lock to locks that are released after root done,
   * avoid unlocking during recovery
   */
  if (domain->state == BFD_ACTIVATED) {
    mcellList_lk = (ftxLkT *) FLAGS_READ(rdflags,rdr.mcellList_lk);
    if (mcellList_lk != NULL) {
      FTX_ADD_LOCK(mcellList_lk, ftx)
    }
  }


  sts = msfs_pl_init_cur(&hdr, rdr.hdr.VD, rdr.hdr.MC);
  if (sts != EOK) {
      goto end;
  }
  hdrp = (bsOdRecCurT *) &hdr;
  *hdrp = rdr.hdr;
  /*
   * if rdr.cellsize was specified (non-zero), use that value to determine
   * how to deallocate mcells.  rdr.cellsize will be specified when 
   * deallocating mcells due to an allocation failure.
   */
  if (rdr.cellsize) {
      large = rdr.cellsize > BSR_PL_MAX_SMALL;
  }
  else {
      hdr_rec = msfs_pl_cur_to_pnt(domain, &hdr, &sts);
      if (sts != EOK) {
         goto end;
      }
      large = (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_LARGE) ? 1 : 0;
  }

  if (large) {

    /*
     * delete hdr and associated data cells
     */
    msfs_pl_del_data_chain(
			   domain,
			   &hdr,
			   &rdr.end,
			   ftx
			   );

  } else {

    /*
     * pin rec to set deleted flag 
     */
    sts = msfs_pl_pin_cur(domain, &hdr, BSR_PROPLIST_HEAD_SIZE, ftx);
    if (sts != EOK) {
        goto end;
    }
    hdr_rec = msfs_pl_cur_to_pnt(domain, &hdr, &sts);
    if (sts != EOK) {
        goto end;
    }
    flags = FLAGS_READ(rdflags,hdr_rec->flags) | BSR_PL_DELETED;
    FLAGS_ASSIGN(hdr_rec->flags,flags);

  }

  /*
   * deref cursors
   */
end:
  msfs_pl_deref_cur(&hdr);

} /* msfs_pl_del_root_done_int() */



/*
 * DEL_DATA_CHAIN - delete a large entry by deleting its cells
 */

void
msfs_pl_del_data_chain(
		       domainT *domain,         /* in     */
		       bsRecCurT *hdr,          /* in     */
		       bsOdRecCurT *end,        /* in     */
		       ftxHT ftx                /* in     */
		       )
{
  statusT sts = EOK;
  mcellPtrRecT fbfm[1];

  DEBUG(1,
	printf("msfs_pl_del_data_chain: prev %d.%d.%d hdr %d.%d.%d end %d.%d.%d\n",
	       hdr->prevVD, hdr->prevMC.page, hdr->prevMC.cell,
	       hdr->VD, hdr->MC.page, hdr->MC.cell,
               end->VD, end->MC.page, end->MC.cell));
       
  /*
   * remove data cells from prim chain
   */
  sts = msfs_pl_unlink_mcells(
			      domain,
			      hdr->prevVD,
			      hdr->prevMC,
			      hdr->VD,
			      hdr->MC,
			      end->VD,
			      end->MC,
			      ftx
			      );
  if (sts != EOK) {
    ADVFS_SAD1("msfs_pl_del_data_chain(2) - failed to unlink mcells", sts);
  }

  /*
   * restore cells to free lists using bs_bmt_util.c:free_mcell_chains_opx(),
   *   started it off as a continuation because I got "too many pin'ed pages"
   *   calling it directly here
   */
  fbfm[0].vdIndex = hdr->VD;
  fbfm[0].mcid = hdr->MC;
  ftx_set_continuation(
		       ftx,
		       sizeof(mcellPtrRecT),
		       (void *) fbfm
		       );

} /* msfs_pl_del_data_chain() */

/*
 * UNLINK_MCELLS - version of unlink that can be called from a root done
 */

statusT
msfs_pl_unlink_mcells(
		      domainT *domain,       /* in */
		      vdIndexT prevVdIndex,  /* in */
		      bfMCIdT prevMcellId,   /* in */
		      vdIndexT firstVdIndex, /* in */
		      bfMCIdT firstMcellId,  /* in */
		      vdIndexT lastVdIndex,  /* in */
		      bfMCIdT lastMcellId,   /* in */
		      ftxHT ftx              /* in */
		      )
{
  bsMPgT *bmt;
  bsMCT *lastMcell;
  rbfPgRefHT lastPgPin;
  bsMCT *prevMcell;
  rbfPgRefHT prevPgPin;
  statusT sts = EOK;
  vdT *vd;

  DEBUG(1,
	printf("msfs_pl_unlink_mcells\n"));

  vd = VD_HTOP(prevVdIndex, domain);

  sts = rbf_pinpg (
		   &prevPgPin,
		   (void*)&bmt,
		   vd->bmtp,
		   prevMcellId.page,
		   BS_NIL,
		   ftx
		   );
  if (sts != EOK) {
      return sts;
  }

  prevMcell = &(bmt->bsMCA[prevMcellId.cell]);

  if ((firstVdIndex != prevMcell->nextVdIndex) ||
      (firstMcellId.page != prevMcell->nextMCId.page) ||
      (firstMcellId.cell != prevMcell->nextMCId.cell)) {

      domain_panic(domain,
              "msfs_pl_unlink_mcells: prev mcell does not point to first mcell. run fixfdmn");
      return E_DOMAIN_PANIC;
  }

  vd = VD_HTOP(lastVdIndex, domain);

  sts = rbf_pinpg (
		   &lastPgPin,
		   (void*)&bmt,
		   vd->bmtp,
		   lastMcellId.page,
		   BS_NIL,
		   ftx
		   );
  if (sts != EOK) {
      return sts;
  }

  lastMcell = &(bmt->bsMCA[lastMcellId.cell]);

  rbf_pin_record (
		  prevPgPin,
		  &(prevMcell->nextVdIndex),
		  sizeof (prevMcell->nextVdIndex)
		  );

  prevMcell->nextVdIndex = lastMcell->nextVdIndex;

  rbf_pin_record (
		  prevPgPin,
		  &(prevMcell->nextMCId),
		  sizeof (prevMcell->nextMCId)
		  );

  prevMcell->nextMCId = lastMcell->nextMCId;

  rbf_pin_record (
		  lastPgPin,
		  &(lastMcell->nextVdIndex),
		  sizeof (lastMcell->nextVdIndex)
		  );

  lastMcell->nextVdIndex = bsNilVdIndex;

  rbf_pin_record (
		  lastPgPin,
		  &(lastMcell->nextMCId),
		  sizeof (lastMcell->nextMCId)
		  );

  lastMcell->nextMCId = bsNilMCId;

  return sts;

}

int
msfs_pl_set_entry_v3(
       		  bfAccessT *bfAccess,          /* in     */
                  struct proplist_head *hp,     /* in     */
                  char *prop_data,              /* in     */
		  struct ucred *cred		/* in     */
                 )

{
  int error = 0, root_done = FALSE, ftx_started = FALSE;
  char *name_buf = NULL;
  int valuelen, entry_size, large, cell_size, 
      page, name_resid, data_resid;
  bsRecCurT hdr;
  statusT sts = EOK;
  ftxHT ftx;
  delPropRootDoneT rdr;
  bsPropListHeadT_v3 *hdr_rec;
  char *hdr_image = NULL;
  proplist_sec_attrs_t sec_info;
  uint64T flags;
  int mcells_alloced=0;
  int clu_clxtnt_locked=FALSE,
      cow_read_locked=FALSE,
      trunc_xfer_locked=FALSE;

  DEBUG(1,
	printf("msfs_pl_set_entry_v3\n"));

  sec_info.sec_type = NULL;

  hdr_image = (char *) ms_malloc( BSR_PROPLIST_PAGE_SIZE );

  name_buf = (char *) ms_malloc( PROPLIST_NAME_MAX + 1 );

  /*
   * start transaction
   *
   * We have to start the transaction before locking the mcell
   * list for ordering consistency.
   */
  sts = FTX_START_N(FTA_MSFS_SETPROPLIST, &ftx, FtxNilFtxH, bfAccess->dmnP, 1);
  if (sts != EOK) {
    ADVFS_SAD1("msfs_pl_set_entry_v3(1) - failed to create xact", sts);
  }
  ftx_started = TRUE;

  /* lock down the clone mcells, if existing */
  msfs_pl_get_locks( bfAccess,
                     &clu_clxtnt_locked,
                     &cow_read_locked,
                     &trunc_xfer_locked);

  MCELLIST_LOCK_WRITE( &(bfAccess->mcellList_lk) )

  /* indicates to root-done code routine that cell_size is based on hdr flag */
  rdr.cellsize = 0;

  /*
   * search mcell chain for name
   */
  error = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  if (error != EOK)
      goto out;

  while (1) {
    error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
    if (error != EOK)
        goto out;
    if (RECCUR_ISNIL(hdr))
      break;
    hdr_rec = (bsPropListHeadT_v3 *)msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, 
                                                       &sts);
    if (sts != EOK) {
        error = sts;
        goto out;
    }
    if ((FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0) {
      /* save header location incase we need to delete it */
      rdr.hdr = *((bsOdRecCurT *) &hdr);

      /* get name portion */
      entry_size = MSFS_PL_ENTRY_SIZE(hdr_rec->namelen, hdr_rec->valuelen);
      valuelen = ALLIGN(hdr_rec->valuelen);
      MS_SMP_ASSERT(valuelen <= MSFS_PROPLIST_VALUE_MAX);
      large = (entry_size > BSR_PL_MAX_SMALL);
      page  = (entry_size > BSR_PL_MAX_LARGE);
      cell_size = (large ? 
		   (page ? BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE) : 
		   BSR_PROPLIST_HEAD_SIZE_V3 + entry_size);
      /*
       * msfs_pl_get_name may advance the cursor to a proplist data record,
       * so hdr_rec cannot be used after this call.
       */
      sts = msfs_pl_get_name_v3(bfAccess->dmnP, &hdr, name_buf, 
		       &name_resid, cell_size);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      if (strcmp(hp->pl_name, name_buf) == 0)
	break;
    }
  }

  /*
   * if found, delete entry with root done
   */
  if (!RECCUR_ISNIL(hdr)) {

    DEBUG(2,
	  printf("msfs_pl_set_entry_v3: found existing %s\n", name_buf));

    /*
     * advance hdr to beyond end of current entry,
     * new entry goes after
     */

    error = msfs_pl_get_data_v3(bfAccess->dmnP, &hdr, NULL, &sec_info,
			     bfAccess->bfVp ,cred, name_resid, valuelen, cell_size);

    if (error) {
      goto out;
    }

    rdr.end = *((bsOdRecCurT *) &hdr);

    DEBUG(2,
	  printf("msfs_pl_set_entry_v3: del prev %d hdr %d end %d\n", 
		 rdr.hdr.prevMC.cell, rdr.hdr.MC.cell, rdr.end.MC.cell));

    rdr.mcellList_lk = &bfAccess->mcellList_lk;
    root_done = TRUE;
  }

  /*
   * deref curs for call to findhead_setdata
   */
  msfs_pl_deref_cur(&hdr);


  if(!error && (sec_info.sec_type = sec_proplist(hp->pl_name))) {

    /*
     * If this is a security attribute, determine if the process 
     * has the ability to retrieve it.
     */
  
    SEC_PROP_ACCESS(error, sec_info, bfAccess->bfVp, cred, ATTR_SET);
    if(error)
      goto out;
  
   
    /*
     * If this is a security attribute, then build the
     * policy specfic structure.
     */
    
    SEC_PROP_BUILD_SEC(error, sec_info, hp->pl_name, bfAccess->bfVp,
	                 cred, prop_data, hp->pl_valuelen, ATTR_SET);

    /*
     * Verify that if the attribute is a security
     * attribute that it is valid according to the
     * policy.
     */

     if(!error)
        SEC_PROP_VALIDATE(error, sec_info, bfAccess->bfVp, ATTR_SET);
     if (error)
	goto out;
  }


  /*
   * allocate resources
   */
  entry_size = MSFS_PL_ENTRY_SIZE(hp->pl_namelen, hp->pl_valuelen);
  large = (entry_size > BSR_PL_MAX_SMALL);
  page  = (entry_size > BSR_PL_MAX_LARGE);
  cell_size = (large ? 
	       (page ? BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE) : 
	       BSR_PROPLIST_HEAD_SIZE_V3 + entry_size);

  error = msfs_pl_fill_hdr_image_v3(
				 prop_data,
				 hp,
				 (bsPropListHeadT_v3 *) hdr_image,
				 cell_size,
				 &name_resid,
				 &data_resid,
				 hp->pl_flags | 
				 (large ? BSR_PL_LARGE : 0) |
				 (page  ? BSR_PL_PAGE  : 0)
				 );

  if (error) {
    goto out;
  }

  error = msfs_pl_findhead_setdata_v3(
				   bfAccess,
				   &hdr,
				   ftx,
				   cell_size,
				   prop_data,
				   hp,
				   name_resid,
				   data_resid,
				   large,
                                   &mcells_alloced
				   );
  if (error) {
    goto out;
  }

  /*
   * write header image
   */
  sts = msfs_pl_pin_cur(bfAccess->dmnP, &hdr, cell_size, ftx);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  hdr_rec = (bsPropListHeadT_v3 *) msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr,
                                                      &sts);
  if (sts != EOK) 
      error = sts;
  else 
      bcopy(hdr_image, hdr_rec, cell_size);

 out:
  if (error == E_CORRUPT_LIST) {
    aprintf("Repairable AdvFs corruption detected, consider running fixfdmn\n");
    ms_uaprintf("Warning: Found corrupted property list record chain\n");
    ms_uaprintf("         Domain: %s\n",bfAccess->dmnP->domainName);
    ms_uaprintf("         Fileset: %s\n",bfAccess->bfSetp->bfSetName);
    ms_uaprintf("         File tag: %d \n",bfAccess->tag.num);
    ms_uaprintf("         Current Disk Index: %d, Previous Disk Index: %d\n",hdr.VD+1,hdr.prevVD+1);
    ms_uaprintf("         Current Page: %d, Previous Page: %d\n",hdr.MC.page,hdr.prevMC.page);
    ms_uaprintf("         Current Mcell: %d, Previous Mcell: %d\n",hdr.MC.cell,hdr.prevMC.cell);
  }
  msfs_pl_deref_cur(&hdr);

  if (ftx_started) {

    if ((error != EOK) && (mcells_alloced)) {
      /* failure -
       * mcells_alloced, which comes only from findhead_setdata, indicates
       * that some mcells were allocated, and must now be deallocated.
       *
       * Don't execute the root done tx - don't want to delete the
       * existing duplicate mcell set since we failed this transaction.
       *
       * Clean up newly allocated mcells explicitly with the subtx's 
       * rootdone, which has already been logged by FTA_MSFS_ALLOC_MCELL's 
       * successful completion.
       */
      DEBUG(1,
	    printf("msfs_pl_set_entry_v3: deleting alloc'ed mcells\n"));
      ftx_done_urd(ftx, FTA_MSFS_SETPROPLIST, 0, NULL, 0, NULL);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
    }

    else if (error != EOK) {
      /* failure with no mcell allocation.
       * NO mcells were allocated by FTA_MSFS_ALLOC_MCELL.
       */
      ftx_fail(ftx);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
    }

    else if (root_done) {
      /* success - delete old property list duplicates chain
       * with the root done.
       */
      DEBUG(1,
	    printf("msfs_pl_set_entry_v3: deleting existing entry\n"));
      
      ftx_done_urd(ftx, FTA_MSFS_SETPROPLIST, 0, NULL,
		   sizeof(rdr), &rdr);

    } else {
      /* success - no old property list chain to clean up. */

      ftx_done_u(ftx, FTA_MSFS_SETPROPLIST, 0, NULL);
      MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )

    }

  } 

  if(cow_read_locked==TRUE) {
      COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
      cow_read_locked=FALSE;
  }
  if(trunc_xfer_locked==TRUE) {
      TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
      trunc_xfer_locked=FALSE;
  }
  if(clu_clxtnt_locked== TRUE) {
      CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
      clu_clxtnt_locked=FALSE;
  }

  ms_free( name_buf );
  ms_free( hdr_image );

  /*
   * If this a security attribute perform the
   * the required completion operations.
   */

  if(!error)
   SEC_PROP_COMPLETION(error, sec_info, ATTR_SET, bfAccess->bfVp);

  /*
   * If we have space allocated by SEC_PROP_BUILD_SEC
   * release it.
   */

  SEC_PROP_DATA_FREE(sec_info);

  return error;

} /* msfs_pl_set_entry_v3() */

int
msfs_pl_fill_hdr_image_v3(
		       char *prop_data,             /* in     */
		       struct proplist_head *hp,    /* in     */
		       bsPropListHeadT_v3 *hdr_image,  /* out    */
		       int cell_size,               /* in     */
		       int *name_resid,             /* out    */
		       int *data_resid,             /* out    */
		       unsigned long flags          /* in     */
		       )
{
  int error = 0, name_xfer, data_xfer;

  DEBUG(1,
	printf("msfs_pl_fill_hdr_image_v3\n"));

  /*
   * header info
   */
  hdr_image->flags = flags;

  DEBUG(2,
	printf("hdr_image->flags %lx\n", hdr_image->flags));

  hdr_image->namelen = hp->pl_namelen;
  hdr_image->valuelen = hp->pl_valuelen;

  /*
   * name portion
   */
  *name_resid = ALLIGN(hp->pl_namelen);
  name_xfer = MIN(*name_resid, cell_size - BSR_PROPLIST_HEAD_SIZE_V3);
  if (name_xfer > 0) {
    bcopy(hp->pl_name, hdr_image->buffer, name_xfer);
    *name_resid -= name_xfer;
  }

  *data_resid = ALLIGN(hp->pl_valuelen);
  data_xfer = MIN(*data_resid, cell_size - BSR_PROPLIST_HEAD_SIZE_V3 - name_xfer);
  if (data_xfer > 0) {
    bcopy(prop_data, (char *)&hdr_image->buffer[name_xfer], data_xfer);
    *data_resid -= data_xfer;      
  }

  return error;
}

int
msfs_pl_findhead_setdata_v3(
		       bfAccessT *bfAccess,           /* in     */
		       bsRecCurT *hdr,                /* in/out */
		       ftxHT ftx,                     /* in     */
		       int size,                      /* in     */
		       char *prop_data,               /* in     */
		       struct proplist_head *hp,      /* in     */
		       int name_resid,                /* in     */
		       int data_resid,                /* in     */
		       int large,                     /* in     */
                       int *mcells_alloced             /* out    */
		       )
{

  int error = 0, space;
  bsOdRecCurT org;
  bsPropListHeadT_v3 *hdr_rec;
  bsMRT *mrecp, *top;
  int search_size = size + sizeof(bsMRT), first = TRUE;
  bsPropListPageT_v3 *dat_rec;
  
  int name_xfer, data_xfer;
  bsRecCurT dat;
  ushort n_mcell_alloc;
  uint64T flags;
  int alloced=0, i;
  statusT sts=EOK;

  DEBUG(1,
	printf("msfs_pl_findhead_setdata_v3\n"));

  *mcells_alloced = 0;

  if (large) {
    dat_rec = (bsPropListPageT_v3 *) ms_malloc(sizeof(bsPropListPageT_v3));
  }

  /*
   * if entry not found, insert at top of chain, else after previous entry
   */
  if (RECCUR_ISNIL(*hdr)) {
    error = msfs_pl_init_cur(hdr, bfAccess->primVdIndex, bfAccess->primMCId);
    org = *((bsOdRecCurT *) hdr);
  } else {
    org = *((bsOdRecCurT *) hdr);
    error = msfs_pl_init_cur(hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  }
  if (error != EOK) {
      if (large) {
        ms_free(dat_rec);
      }
      return error;
  } 

  /*
   * search prim mcell chain for deleted header record of the right size
   */
  do {
    error = msfs_pl_seek_cur(bfAccess->dmnP, hdr, BSR_PROPLIST_HEAD);
    if (error != EOK) {
      if (large) {
        ms_free(dat_rec);
      }
      return error;
    } 
  } while (
	   (!RECCUR_ISNIL(*hdr)) &&
	   (
	    hdr_rec = (bsPropListHeadT_v3 *) 
	    msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr, &sts),
            (sts == EOK) &&
	    ((REC_TO_MREC(hdr_rec)->bCnt != search_size) ||
	    ((FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0))
	    )
	   );

  if (sts != EOK) {
      if (large) {
        ms_free(dat_rec);
      }
      return sts;
  }

  /* Determine how many mcells need to be allocated; just data chain
     counted here.
  */
  if (large && name_resid+data_resid)
    n_mcell_alloc = (name_resid+data_resid-1)/size + 1;
  else n_mcell_alloc = 0;


  /* Note - this block contains the first instance of pinning a record.
     Make sure all allocations have been done prior to this.  No ftx_fail
     calls (or error returns from this routine) allowed once the record is
     pinned.
  */
  if (RECCUR_ISNIL(*hdr)) {
      
    /*
     * record not found, create new one after original search position
     */
    sts = msfs_pl_init_cur(hdr, org.VD, org.MC);
    if (sts != EOK ) {
	if (large) {
	  ms_free(dat_rec);
	}
        return sts;
    }

    /*
     * look for spot in existing mcell
     */
    do {
      if (!first) {
	sts = msfs_pl_next_mc_cur(bfAccess->dmnP, hdr);
        if (sts != EOK ) {
	  if (large) {
	    ms_free(dat_rec);
	  }
          return sts;
        }
      }
      first = FALSE;
      if (!RECCUR_ISNIL(*hdr)) {
	top = mrecp = REC_TO_MREC(msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr, 
                                                     &sts));
        if (sts != EOK) {
	  if (large) {
	    ms_free(dat_rec);
	  }
          return sts;
        }

	while (mrecp->type != BSR_NIL) {
	  mrecp = NEXT_MREC(mrecp);
	}
	space = BSC_R_SZ - ((caddr_t)mrecp - (caddr_t)top) - 2*sizeof(bsMRT);
	DEBUG(2,
	      printf("free space in VD %d page %d cell %d is %d\n",
		     hdr->VD, hdr->MC.page, hdr->MC.cell, space));
      }
    } while (
	     (!RECCUR_ISNIL(*hdr)) &&
	     (space < size)
	     );

    if (RECCUR_ISNIL(*hdr)) {
      

      /*
       * no spot in existing cell, create record in new cell
       */

      hdr_rec = (bsPropListHeadT_v3 *) ms_malloc(size);

      FLAGS_ASSIGN(hdr_rec->flags,BSR_PL_DELETED);
      error = msfs_pl_init_cur(hdr, org.VD, org.MC);
      if(error != EOK) {
	  ms_free(hdr_rec);
          goto out;
      }

      /*
       * allocate all the mcells needed for this proplist entry
       */
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				hdr,
				ftx,
				size,
				++n_mcell_alloc
				);
      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
	  ms_free(hdr_rec);
          error = ENOMEM;
          goto out;
      }

      (void)msfs_pl_create_rec(
			       bfAccess,
			       hdr,
			       ftx,
			       (caddr_t) hdr_rec,
			       size,
			       BSR_PROPLIST_HEAD
			       );
      ms_free(hdr_rec);

    } else {

      /*
       * allocate all the mcells needed for this proplist entry
       */
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				hdr,
				ftx,
				size,
				n_mcell_alloc
				);

      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
          error = ENOMEM;
          goto out;
      }

      /*
       * move cur onto it and pin
       */
      hdr->pgoff = (((caddr_t)mrecp) - (caddr_t)hdr->bmtp)
	+ sizeof(bsMRT);
      if (hdr->pgoff > (ADVFS_PGSZ - sizeof(bsMRT))) {
	   error = E_CORRUPT_LIST;
	   goto out;
      }
      sts = msfs_pl_pin_cur(
		      bfAccess->dmnP,
		      hdr,
		      size,
		      ftx
		      );
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      
      /*
       * fill in record header and empty record after
       */
      hdr_rec = (bsPropListHeadT_v3 *) msfs_pl_cur_to_pnt(bfAccess->dmnP, hdr,
                                                          &sts);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      mrecp = REC_TO_MREC(hdr_rec);
      mrecp->type = BSR_PROPLIST_HEAD;
      mrecp->bCnt = size + sizeof(bsMRT);
      mrecp->version = 0;
      mrecp = NEXT_MREC(mrecp);
      mrecp->type = 0;
      mrecp->bCnt = 0;
      mrecp->version = 0;
    
      /*
       * fill in data
       */
      FLAGS_ASSIGN(hdr_rec->flags,BSR_PL_DELETED);

    }

  }
  else {
      /*
       * allocate all the mcells needed for this proplist entry
       */
      sts = msfs_pl_init_cur(&dat, hdr->VD, hdr->MC);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      alloced = msfs_pl_alloc_mcell(
				bfAccess,
				&dat,
				ftx,
				size,
				n_mcell_alloc
				);
      msfs_pl_deref_cur(&dat);

      /* failed to allocate requested mcells */
      if (alloced != n_mcell_alloc) {
          error = ENOMEM;
          goto out;
      }
  }




  if (!large) {
    goto out;
  }


  /*
   * use separate cur to leave hdr cur pointing to header
   */
  sts = msfs_pl_init_cur(&dat, hdr->VD, hdr->MC);
  if (sts != EOK) {
      error = sts;
      goto out;
  }

  /*
   * fill data cells
   */
  i = 0;
  while (((name_resid + data_resid) > 0) && i++ < alloced) {

    DEBUG(2,
	  printf("name_resid %d data_resid %d\n", name_resid, data_resid));

    /*
     * name portion
     */
    name_xfer = MIN(name_resid, size);
    bcopy(
	  hp->pl_name + ALLIGN(hp->pl_namelen) - name_resid,
	  dat_rec->buffer, 
	  name_xfer
	  );
    name_resid -= name_xfer;

    /*
     * value portion 
     */
    data_xfer = MIN(data_resid, size - name_xfer);
    if (data_xfer > 0) {
      bcopy(prop_data + ALLIGN(hp->pl_valuelen) - data_resid,
            &dat_rec->buffer[name_xfer],
            data_xfer);
      data_resid -= data_xfer;      
    }

    /*
     * allocate and link new mcell at end of chain
     */
    (void) msfs_pl_create_rec(
			       bfAccess,
			       &dat,
			       ftx,
			       (caddr_t) dat_rec,
			       size,
			       BSR_PROPLIST_DATA
			       );
  } /* while */

  /*
   * dereference dat cur, hdr cur still pointing to header
   */
  msfs_pl_deref_cur(&dat);

out:
  *mcells_alloced = alloced;

  if (large) {
    ms_free(dat_rec);
  }

  return (alloced == n_mcell_alloc ? error : ENOSPC);

} /* msfs_pl_findhead_setdata_v3() */

int
msfs_getproplist_int_v3(
		 struct vnode *vp,              /* in     */
		 char **names,                  /* in     */
		 struct uio *uiop,              /* out    */
		 int *min_buf_size,             /* in/out */
		 int mask,                      /* in     */
		 struct ucred *cred             /* in     */
		 )

{
	int       error = 0;
	bfAccessT *bfAccess;
	bsPropListHeadT_v3 *hdr_rec;
	int       just_sizing, total_bytes;
	bsRecCurT hdr;
	proplist_sec_attrs_t sec_info;
	uint64T flags;
	struct uio uioLcl;
	struct iovec iovLcl;
	char *bbuf = NULL;
        statusT sts=EOK;
        int clu_clxtnt_locked=FALSE,
            cow_read_locked=FALSE,
            trunc_xfer_locked=FALSE;

	DEBUG(1,
	      printf("msfs_getproplist_int_v3\n"));

	bfAccess = VTOA(vp);

	/*
	 * initialize stuff for get_entry
	 */
	just_sizing = FALSE;
	total_bytes = 0;
	*min_buf_size = 0;

	/*
	 * Set up space to collect user's data.
	 * Once the mcellList lock is held, it's necessary to avoid 
	 * copyout's to user space (lock hierarchy).
         * If we're just sizing the property list, don't malloc any storage.
	 */
        if (uiop->uio_iov->iov_len > 0) {
            bbuf = ms_malloc(uiop->uio_iov->iov_len);
        }
	iovLcl.iov_base = bbuf;
	iovLcl.iov_len = uiop->uio_iov->iov_len;
	uioLcl.uio_iov = &iovLcl;
	uioLcl.uio_offset = 0;
	uioLcl.uio_iovcnt = 1;
	uioLcl.uio_resid = uiop->uio_iov->iov_len;
	uioLcl.uio_segflg = UIO_SYSSPACE;
	uioLcl.uio_rw = UIO_READ;

        /* lock down the clone mcells, if existing */
        msfs_pl_get_locks( bfAccess,
                           &clu_clxtnt_locked,
                           &cow_read_locked,
                           &trunc_xfer_locked);

        MCELLIST_LOCK_READ( &(bfAccess->mcellList_lk) )

	/*
	 * search mcell chain for name
	 */
	error = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
        if(error != EOK) {
	    msfs_pl_deref_cur(&hdr);
            MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )
            if(cow_read_locked==TRUE) {
                COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
                cow_read_locked=FALSE;
            }
            if(trunc_xfer_locked==TRUE) {
                TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
                trunc_xfer_locked=FALSE;
            }
            if(clu_clxtnt_locked== TRUE) {
                CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
                clu_clxtnt_locked=FALSE;
            }
            if (bbuf != NULL) {
		ms_free(bbuf);
            }
            return error;
        }
	do {
	  error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
          if (error != EOK) break;
	  if (
	      (!RECCUR_ISNIL(hdr)) &&
	      (hdr_rec = (bsPropListHeadT_v3 *)
	       msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr, &sts),
               (sts == EOK) &&
	       (FLAGS_READ(flags,hdr_rec->flags) & BSR_PL_DELETED) == 0)
	      ) {
	    bzero((char *)&sec_info, sizeof (sec_info));
	    error = msfs_pl_get_entry_v3(
				      bfAccess->dmnP,
				      &hdr,
				      &uioLcl,
				      names,
				      min_buf_size,
				      &just_sizing,
				      &total_bytes,
				      mask,
				      cred,
				      vp,
				      &sec_info
				      );

		/*
                 * If we have space allocated by SEC_PROP_BUILD_SEC
                 * release it.
                 */

  	  	SEC_PROP_DATA_FREE(sec_info);
	  }
          if (sts != EOK) {
              error = sts;
              break;
          }
	  if (error) break;

	} while (!RECCUR_ISNIL(hdr));

	msfs_pl_deref_cur(&hdr);
	MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )

        if(cow_read_locked==TRUE) {
            COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
            cow_read_locked=FALSE;
        }
        if(trunc_xfer_locked==TRUE) {
            TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
            trunc_xfer_locked=FALSE;
        }
        if(clu_clxtnt_locked== TRUE) {
            CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
            clu_clxtnt_locked=FALSE;
        }

	if (just_sizing && !error) {
	  *min_buf_size = total_bytes;
	}

	if(error == EPERM)
		error = 0;

	if (!just_sizing && !error) {
	  error = uiomove(bbuf, uioLcl.uio_offset, uiop);
	  uiop->uio_resid = uioLcl.uio_resid; /* upper layer uses uio_resid */
	}

        if (bbuf != NULL) {
          ms_free(bbuf);
        }

	return(error);

} /* msfs_getproplist_int_v3() */

int
msfs_pl_get_entry_v3(
		  domainT *domain,              /* in     */
		  bsRecCurT *hdr,               /* in     */
		  struct uio *uiop,             /* out    */
		  char **names,                 /* in     */
		  int *min_buf_size,            /* in/out */
		  int *just_sizing,             /* in/out */
		  int *total_bytes,             /* in/out */
		  int mask,                     /* in     */
		  struct ucred *cred,		/* in     */
		  struct vnode	*vp,		/* in     */
  		  proplist_sec_attrs_t *sec_info /* out   */
		  )

{
  char      name_buf[PROPLIST_NAME_MAX+1];
  int       entry_size, flags, namelen, valuelen;
  bsPropListHeadT_v3 *hdr_rec;
  int       error = 0, name_resid, cell_size;
  uint64T rdflags;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_get_entry_v3\n"));

  /*
   * if entry deleted, exit
   */
  hdr_rec = (bsPropListHeadT_v3 *) msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
      error = sts;
      goto out;
  }

  MS_SMP_ASSERT(REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD);
  if (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_DELETED) {
    goto out;
  }

  /*
   * calculate cell size for larger elements
   */
  cell_size = (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_PAGE ? 
	       BSR_PROPLIST_PAGE_SIZE : BSR_PROPLIST_DATA_SIZE);

  /*
   * get name from disk into disk_buffer
   */
  sts = msfs_pl_get_name_v3(domain, hdr, name_buf, &name_resid, cell_size);
  if (sts != EOK) {
      error = sts;
      goto out;
  }

  sec_info->sec_type = NULL;

  /*
   * handle entry
   */
  if (proplist_entry_match(
			   name_buf, 
			   names, 
			   (FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED),
			   mask
			   )) {

    /*
     * found one
     */
    DEBUG(2,
	  printf("msfs_pl_get_entry_v3 found %s\n",name_buf));
  
    sec_info->sec_type = sec_proplist(name_buf);

  /*
   * If this is a security attribute, determine if
   * the process can retrieve it.
   * If namelist is null, do not return error, as get_proplist operation
   * should continue with other entries.
   */

   SEC_PROP_ACCESS(error, *sec_info, vp, cred, ATTR_RETRIEVE);
   if(error) {
	if(names == NULL || *names == NULL) error = 0;
	goto out;
   }

    flags = FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED;
    namelen = ALLIGN(hdr_rec->namelen);
    valuelen = ALLIGN(hdr_rec->valuelen);
    entry_size = SIZEOF_PROPLIST_ENTRY(hdr_rec->namelen, hdr_rec->valuelen);
    *total_bytes += entry_size;
    if (!(*just_sizing) &&
	entry_size > uiop->uio_resid) {
      *just_sizing = TRUE;
    }

    /*
     * copy entry
     */
    if (!(*just_sizing)) {

      /*
       * get header info
       */
      error = uiomove((caddr_t)&entry_size,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&flags,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&hdr_rec->namelen,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)&hdr_rec->valuelen,
		      sizeof(int),
		      uiop);
      if (error) {
	goto out;
      }
      error = uiomove((caddr_t)name_buf,
		      namelen,
		      uiop);
      if (error) {
	goto out;
      }

      /*
       * get value from disk to output
       */

      error = msfs_pl_get_data_v3(domain, hdr, uiop, sec_info, vp, cred,
			       name_resid, valuelen, cell_size);
  
    }

  }

 out:
  
  return (error);

} /* msfs_pl_get_entry_v3() */

statusT 
msfs_pl_get_name_v3(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in/out */
		 char *buffer,                  /* out    */
		 int *last_resid,               /* in/out */
		 int cell_size                  /* in     */
		 )
{
  bsPropListHeadT_v3 *hdr_rec;
  bsPropListPageT_v3 *dat_rec;
  int resid, xfer, namelen;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_get_name_v3\n"));

  /*
   * portion in the header
   */
  hdr_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
      goto end;
  }
  MS_SMP_ASSERT(REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD);
  resid = namelen = ALLIGN(hdr_rec->namelen);
  xfer = MIN(resid, cell_size - BSR_PROPLIST_HEAD_SIZE_V3);
  bcopy(hdr_rec->buffer, buffer, xfer);
  *last_resid = resid;
  resid -= xfer;

  /*
   * get rest of name from data chain
   */
  while (resid > 0) {
    sts = msfs_pl_seek_cur(domain, hdr, BSR_PROPLIST_DATA);
    if (sts != EOK)
        goto end;
    dat_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
    if (sts != EOK) 
        goto end;

    MS_SMP_ASSERT(REC_TO_MREC(dat_rec)->type == BSR_PROPLIST_DATA);
    xfer = MIN(resid, cell_size);
    bcopy(
	  dat_rec->buffer, 
	  (caddr_t) &buffer[namelen - resid],
	  xfer
	  );
    *last_resid = resid;
    resid -= xfer;
  }

end:
  if (sts != EOK)
      DEBUG(2, printf("error occured. error code = %d\n", sts));
  else
      DEBUG(2, printf("name is %s\n", buffer));

  return sts;

} /* msfs_pl_get_name_v3() */



int
msfs_pl_get_data_v3(
		 domainT *domain,               /* in     */
		 bsRecCurT *hdr,                /* in     */
		 struct uio *uiop,              /* out    */
  		 proplist_sec_attrs_t *sec_info, /* in     */
		 struct vnode *vp,		/* in     */
		 struct ucred *cred,		/* in     */
		 int name_resid,                /* in     */
		 int data_resid,                /* in     */
		 int cell_size                  /* in     */
		 )
{
  int error = 0, data_xfer, first = TRUE;
  bsPropListHeadT_v3 *hdr_rec;
  bsPropListPageT_v3 *dat_rec;
  char *sec_buff=NULL;
  int sec_data_offset = 0;
  int nbytes = data_resid;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_get_data_v3\n"));


  /*
   * get portion in header record
   */
  MS_SMP_ASSERT(!RECCUR_ISNIL(*hdr));
  hdr_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
  if (sts != EOK) {
    return sts;
  }

  /*
   *  Allocate space for the attribute if we are dealing
   *  with a security attribute.  This allows us to keep
   *  track of attributes larger then an MCELL so we may 
   *  validate and cache them.
   *
   *  Note:
   * 	If we were called by msfs_setproplist or
   *	msfs_delproplist or this is 
   *	not a security attribute then sec_info->sec_type
   *	is NULL.
   *
   */


  if(sec_info->sec_type) {
	sec_buff = (char *) ms_malloc(nbytes);
  }

  if (REC_TO_MREC(hdr_rec)->type == BSR_PROPLIST_HEAD) {
    data_xfer = MIN(data_resid, 
		    cell_size - BSR_PROPLIST_HEAD_SIZE_V3 - name_resid);
    if ((uiop != NULL) && (data_xfer != 0)) {
      if(sec_buff) {
	bcopy((caddr_t) &hdr_rec->buffer[name_resid],sec_buff+sec_data_offset,data_xfer);
	sec_data_offset = sec_data_offset+data_xfer;
      } else {
         error = uiomove(
		      (caddr_t) &hdr_rec->buffer[name_resid],
		      data_xfer,
		      uiop
		      );
          if (error) {
	      return error;
          }
      }
    }
    name_resid = 0;
    data_resid -= data_xfer;
    first = FALSE;
  }

  /*
   * get portion in data cells, we are assured to be on the last
   * cell containing name data
   */
  while (data_resid > 0) {

    DEBUG(2,
	  printf("reading data chain, data_resid %d\n", data_resid));

    if (!first) {
      sts = msfs_pl_seek_cur(domain, hdr, BSR_PROPLIST_DATA);
      if (sts != EOK) {
        return sts;
      }

      MS_SMP_ASSERT(!RECCUR_ISNIL(*hdr));
    }
    dat_rec = msfs_pl_cur_to_pnt(domain, hdr, &sts);
    if (sts != EOK) {
        return sts;
    }
    if ( (dat_rec == NULL) || 
	(REC_TO_MREC(dat_rec)->type != BSR_PROPLIST_DATA) ) {
        domain_panic(domain, 
                "msfs_pl_get_data_v3(1) - invalid prop list data on disk. Run fixfdmn.");
        return E_DOMAIN_PANIC;
    }
    data_xfer = MIN(data_resid, cell_size);
    if (uiop != NULL) {
      if(sec_buff) {
	  bcopy((caddr_t) &dat_rec->buffer[name_resid],sec_buff+sec_data_offset,data_xfer);
	  sec_data_offset = sec_data_offset+data_xfer;
      } else {
          error = uiomove(
		      (caddr_t) &dat_rec->buffer[name_resid],
		      data_xfer,
		      uiop
		      );
          if (error) {
	      return error;
          }
       }
    }
    name_resid = 0;
    data_resid -= data_xfer;
    first = FALSE;
  }

  if (!error) {

      /*
       * If this is a security attribute, then build the
       * policy specfic structure.
       */
        if (sec_info && sec_info->sec_type)
           SEC_PROP_BUILD_SEC(error, *sec_info,
                   	      sec_info->sec_type->attr_name, vp,
                   	      cred, sec_buff, hdr_rec->valuelen,
			      ATTR_RETRIEVE);
   }
   if (error) {
      sec_info->sec_type = NULL;   /* Failed, set type to be NULL
                                    * so we do not attempt any security
                                    * operations using this data.
                                    */
    }

 /*
  * Verify that if the attribute is a security
  * attribute that it is valid according to the
  * policy.
  */

  SEC_PROP_VALIDATE(error, *sec_info, vp, ATTR_RETRIEVE);

  /*
   * If this a security attribute perform the
   * the required completion operations.
   */

  if(!error)
	SEC_PROP_COMPLETION(error, *sec_info, ATTR_RETRIEVE, vp);

  if(sec_buff) {
      if((uiop != NULL && !error))
          error = uiomove(
		      sec_buff,
		      nbytes,
		      uiop
		      );

      ms_free(sec_buff);
  }

  return error;

} /* msfs_pl_get_data_v3() */

int
msfs_delproplist_int_v3(
		 struct vnode *vp,              /* in     */
                 char **names,                  /* in     */
		 int mask,			/* in     */
                 struct ucred *cred,            /* in     */
		 long xid,                      /* in, MBZ iff !CFS */
                 int setctime                   /* in     */
		 )
{
  int error=0,sec_access,access_error=0;
  bfAccessT *bfAccess;
  char *name_buf = NULL;
  int valuelen, name_resid;
  bsRecCurT hdr;
  statusT sts = EOK;
  ftxHT ftx;
  delPropRootDoneT rdr;
  bsPropListHeadT_v3 *hdr_rec;
  int entry_size, large, page, cell_size;
  uint64T flags, rdflags;
  proplist_sec_attrs_t sec_info;
  struct bfNode *bnp;
  struct fsContext *rem_context;
  struct vattr check_vattr;
  int clu_clxtnt_locked=FALSE,
      cow_read_locked=FALSE,
      trunc_xfer_locked=FALSE;

  DEBUG(1,
	printf("msfs_pl_delproplist_int_v3\n"));

  name_buf = (char *) ms_malloc( PROPLIST_NAME_MAX + 1);

  sec_info.sec_struct = (void *) NULL;
  bnp = (struct bfNode *)&vp->v_data[0];
  rem_context = bnp->fsContextp;

  name_buf[0] = '\0';
  bfAccess = VTOA(vp);

  /*
   * screen domain panic
   */
  if (bfAccess->dmnP->dmn_panic) {
      ms_free( name_buf );
      return EIO;
  }
	  
  /*
   * do copy-on-write, if needed
   */
  if ( bfAccess->bfSetp->cloneSetp != NULL ) {
      bs_cow(bfAccess, COW_NONE, 0, 0, FtxNilFtxH);
  }


start:
  /*
   * transaction to release root done
   *
   * We have to start the transaction before locking the mcell
   * list for ordering consistency.
   *
   * Note: if not running under CFS,
   * xid must be zero to guarantee uniqueness of
   * ftxId in the transaction log.
   */
  sts = FTX_START_XID(FTA_MSFS_DELPROPLIST, &ftx, FtxNilFtxH, 
                      bfAccess->dmnP, 0, xid);
  if (sts != EOK) {
    ADVFS_SAD1("msfs_delproplist_int_v3(1) - failed to create xact", sts);
  }

  /* lock down the clone mcells, if clone exists */
  msfs_pl_get_locks( bfAccess,
                     &clu_clxtnt_locked,
                     &cow_read_locked,
                     &trunc_xfer_locked);

  MCELLIST_LOCK_WRITE( &(bfAccess->mcellList_lk) )

  /*
   * search mcell chain for name
   */
  sts = msfs_pl_init_cur(&hdr, bfAccess->primVdIndex, bfAccess->primMCId);
  if (sts != EOK) {
      error = sts;
      goto out;
  }
  
  /*
   * match items in the list
   */
  do { /* while (!RECCUR_ISNIL(hdr)) */

    /*
     * find next match
     */
    while (1) {
      error = msfs_pl_seek_cur(bfAccess->dmnP, &hdr, BSR_PROPLIST_HEAD);
      if (error != EOK)
          goto out;
      if (RECCUR_ISNIL(hdr))
	break;
      hdr_rec = (bsPropListHeadT_v3 *)msfs_pl_cur_to_pnt(bfAccess->dmnP, &hdr,
                                                         &sts);
      if (sts != EOK) {
          error = sts;
          goto out;
      }
      if ((FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_DELETED) == 0) {

	/* save header entries for latter processing */
	flags = FLAGS_READ(rdflags,hdr_rec->flags) & ~BSR_PL_RESERVED;
	valuelen = ALLIGN(hdr_rec->valuelen);

	/* save header location in case we need to delete it */
	rdr.hdr = *((bsOdRecCurT *) &hdr);

	/* get name portion */
	entry_size = MSFS_PL_ENTRY_SIZE(hdr_rec->namelen, hdr_rec->valuelen);
	large = (entry_size > BSR_PL_MAX_SMALL);
	page  = (entry_size > BSR_PL_MAX_LARGE);
	cell_size = (large ? 
		     (page ? BSR_PROPLIST_PAGE_SIZE : 
		      BSR_PROPLIST_DATA_SIZE) : 
		     BSR_PROPLIST_HEAD_SIZE_V3 + entry_size);
	sts = msfs_pl_get_name_v3(bfAccess->dmnP, &hdr, name_buf, 
			 &name_resid, cell_size);
        if (sts != EOK) {
            error = sts;
            goto out;
        }
	if (proplist_entry_match(name_buf, names, flags, mask))
	  break;
      }
    }

    /*
     * if found, delete entry
     */
    if (!RECCUR_ISNIL(hdr)) {

      sec_info.sec_type = sec_proplist(name_buf);

      /*
       * If this is a security attribute, determine if
       * the process can delete it.
       */

      SEC_PROP_ACCESS(sec_access, sec_info, vp, cred, ATTR_DELETE);

	if(sec_access) {
	   if(sec_access != EPERM) {
	       access_error = sec_access;
	   }
	    continue;
        }
 
      SEC_PROP_BUILD_SEC(error, sec_info, name_buf, vp, cred, NULL,
                         0,ATTR_DELETE);
 
      if(error) 
       goto out;
 
      SEC_PROP_VALIDATE(error, sec_info, vp, ATTR_DELETE);
 
      if(error)  {
         SEC_PROP_DATA_FREE(sec_info);
       goto out;
      }
 
      SEC_PROP_COMPLETION(error, sec_info, ATTR_DELETE, vp);
      SEC_PROP_DATA_FREE(sec_info);
 
     sec_info.sec_type = NULL;  /* Set to NULL so we do not perform any 
				 * security operarions the initial 
				 * retriving of data.
				 */
      /*
       * advance hdr to beyond end of current entry,
       * new entry goes after
       */
      error = msfs_pl_get_data_v3(bfAccess->dmnP, &hdr, NULL, &sec_info, vp,
			       cred, name_resid, valuelen, cell_size);
      if (error) {
	goto out;
      }
     
       

      rdr.end = *((bsOdRecCurT *) &hdr);
      rdr.mcellList_lk = &bfAccess->mcellList_lk;
      rdr.cellsize = 0; /* use hdr flag to denote size */
      ftx_done_urd(ftx, FTA_MSFS_DELPROPLIST, 0, NULL,
        sizeof(rdr), &rdr);

      msfs_pl_deref_cur(&hdr);

      if(cow_read_locked==TRUE) {
          COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
          cow_read_locked=FALSE;
      }
      if(trunc_xfer_locked==TRUE) {
          TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
          trunc_xfer_locked=FALSE;
      }
      if(clu_clxtnt_locked== TRUE) {
          CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
          clu_clxtnt_locked=FALSE;
      }

      goto start;

    }

  } while (!RECCUR_ISNIL(hdr)); /* match items in list */

  if (setctime == SET_CTIME) {
      mutex_lock( &rem_context->fsContext_mutex);
      rem_context->fs_flag |= MOD_CTIME;
      rem_context->dirty_stats = TRUE;
      mutex_unlock( &rem_context->fsContext_mutex);
  }

 out:
  msfs_pl_deref_cur(&hdr);

  /*
   * cancel final xac
   */
  ftx_quit(ftx);
  MCELLIST_UNLOCK( &(bfAccess->mcellList_lk) )

  if(cow_read_locked==TRUE) {
      COW_READ_UNLOCK_RECURSIVE( &(bfAccess->origAccp->cow_lk) )
      cow_read_locked=FALSE;
  }
  if(trunc_xfer_locked==TRUE) {
      TRUNC_XFER_UNLOCK_RECURSIVE( &(bfAccess->origAccp->trunc_xfer_lk) )
      trunc_xfer_locked=FALSE;
  }
  if(clu_clxtnt_locked== TRUE) {
      CLU_CLXTNT_UNLOCK_RECURSIVE(&bfAccess->clu_clonextnt_lk);
      clu_clxtnt_locked=FALSE;
  }

  ms_free( name_buf );

  if(error)
     return error;

  return access_error;

} /* msfs_delproplist_int_v3() */


void
msfs_pl_del_root_done_int_v3(
		      ftxHT ftx,                /* in     */
		      int size,                 /* in     */
		      void *address             /* in     */
		      )
{
  bsPropListHeadT_v3 *hdr_rec;
  domainT *domain = ftx.dmnP;
  delPropRootDoneT rdr;
  bsRecCurT hdr;
  bsOdRecCurT *hdrp;
  uint64T flags, rdflags;
  ftxLkT *mcellList_lk;
  int large;
  statusT sts;

  DEBUG(1,
	printf("msfs_pl_del_root_done\n"));

  /*
   * recover pointer root-done and domain pointers
   */
  bcopy(address, &rdr, sizeof(delPropRootDoneT));

  DEBUG(2,
	printf(
	       "hdr %d\n",
	       rdr.hdr.MC.cell 
	       ));
  /*
   * add mcell chain lock to locks that are released after root done,
   * avoid unlocking during recovery
   */
  if (domain->state == BFD_ACTIVATED) {
    mcellList_lk = (ftxLkT *) FLAGS_READ(rdflags,rdr.mcellList_lk);
    if (mcellList_lk != NULL) {
      FTX_ADD_LOCK(mcellList_lk, ftx)
    }
  }


  sts = msfs_pl_init_cur(&hdr, rdr.hdr.VD, rdr.hdr.MC);
  if (sts != EOK) 
      goto end;

  hdrp = (bsOdRecCurT *) &hdr;
  *hdrp = rdr.hdr;
  /*
   * if rdr.cellsize was specified (non-zero), use that value to determine
   * how to deallocate mcells.  rdr.cellsize will be specified when 
   * deallocating mcells due to an allocation failure.
   */
  if (rdr.cellsize) {
      large = rdr.cellsize > BSR_PL_MAX_SMALL;
  }
  else {
      hdr_rec = msfs_pl_cur_to_pnt(domain, &hdr, &sts);
      if (sts != EOK) 
          goto end;
      large = (FLAGS_READ(rdflags,hdr_rec->flags) & BSR_PL_LARGE) ? 1 : 0;
  }

  if (large) {

    /*
     * delete hdr and associated data cells
     */
    msfs_pl_del_data_chain(
			   domain,
			   &hdr,
			   &rdr.end,
			   ftx
			   );

  } else {

    /*
     * pin rec to set deleted flag 
     */
    sts = msfs_pl_pin_cur(domain, &hdr, BSR_PROPLIST_HEAD_SIZE_V3, ftx);
    if (sts != EOK) 
        goto end;
    hdr_rec = msfs_pl_cur_to_pnt(domain, &hdr, &sts);
    if (sts != EOK) 
        goto end;
    flags = FLAGS_READ(rdflags,hdr_rec->flags) | BSR_PL_DELETED;
    FLAGS_ASSIGN(hdr_rec->flags,flags);

  }

end:
  /*
   * deref cursors
   */
  msfs_pl_deref_cur(&hdr);

} /* msfs_pl_del_root_done_int_v3() */
