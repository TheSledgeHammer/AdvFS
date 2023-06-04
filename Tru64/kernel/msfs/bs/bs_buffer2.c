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
 * 
 */

#pragma ident "@(#)$RCSfile: bs_buffer2.c,v $ $Revision: 1.1.625.18 $ (DEC) $Date: 2006/10/10 12:14:42 $"

#define ADVFS_MODULE BS_BUFFER2

#include <sys/types.h>
#include <sys/time.h>
#ifdef _OSF_SOURCE
#include <machine/machparam.h>
#else
#include <machine/param.h>
#endif /* _OSF_SOURCE */
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_logger.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_public.h>
#include <msfs/ms_assert.h>
#include <msfs/ms_osf.h>
#include <sys/syslog.h>
#include <sys/vnode.h>
#include <sys/buf.h>
#include <sys/radset.h>
#include <mach/std_types.h>
#include <vm/vm_ubc.h>
#include <vm/vm_numa.h>
#include <kern/rad.h>
#include <sys/lock_probe.h>
#include <msfs/vfast.h>
#include <machine/clock.h>

extern void rm_from_lazyq( struct bsBuf *bp, ioDescT **ioList, int *listLen, int *noqfnd);
extern void advfs_page_dirty(vm_page_t);

static const bsUnpinModeT UNPIN_DIO_DIRTY = {BS_RECYCLE_IT, BS_MOD_DIRECT};
static const bsUnpinModeT UNPIN_DIO_CLEAN = {BS_RECYCLE_IT, BS_NOMOD_DIRECT};

int DoReadAhead = 1;

struct flags {
 unsigned short getflag:1,      /* getpage caller info */ 
                cowLkLocked:1,  /* copy-on-write lock flag */
                useOrigMap:1,   /* used if clone bfap doesn't map the page */
                unused:13
};

/* Global counters to track AdvFS's UBC cache lookup hit and miss rates.
 * Patch advfsstats = 1 to enable and 0 to disable.
 * These are optional until they are made NUMA RAD-specific in the future.
 */
unsigned long AdvfsUbcHit = 0;
unsigned long AdvfsUbcMiss = 0;
int advfsstats = 0;

/* lockinfo struct for bsBuf.bufLock */
decl_simple_lock_info(, ADVbufLock_lockinfo)

/*
 * Tracing vars
 */
char *PrintAction[] = {
    "Ref     ",
    "Deref   ",
    "Pin     ",
    "Unpin   ",
    "Devread ",
    "Devwrite" };

char *UnpinAction[] = {
    "Immed",
    "Clean",
    "Dirty",
    "Log  " };

extern unsigned TrFlags;

/*
 * Forward references
 */

static
void
prefetch(
      bfAccessT *bfap,        /* in */
      unsigned long bsPage,   /* in - bf page number to start with */
      int pageCnt,            /* in - num pages to prefetch */
      int *listLen,           /* out - len of output ioDesc list */
      ioDescT **ioListp,      /* out - list of ioDesc */
      int doRead,             /* in - do we (TRUE) or caller (FALSE) do read */
      short metaCheck,        /* in - metadata check type */
      vm_policy_t vmp,        /* in - vm page locality */
      vm_offset_t offset,     /* in - starting offset */
      vm_size_t len,          /* in - number of bytes */
      int ubc_flags           /* in - ubc control */
      );

static
statusT
bs_refpg_int(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out - address of bf data */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         int getflag,                   /* in - is getpage the caller? */
         int fetchPages,                /* in */
         short metaCheck,               /* in - metadata check type */
         vm_policy_t vmp,               /* in - vm page locality */
         vm_offset_t offset,            /* in - starting offset */
         vm_size_t len,                 /* in - number of bytes */
         int ubc_flags                  /* in - ubc control */
         );

static
statusT
bs_pinpg_clone( bfPageRefHT *bfPageRefH,    /* out */
         void **bfPageAddr,                 /* out - address of bf data */
         struct bfAccess* bfap,             /* in */
         unsigned long bsPage,              /* in - bf page number */
         bfPageRefHintT refHint,            /* in - hint to do readahead */
         ftxHT ftxH,                        /* in */
         int flag,                          /* in */
         short metaCheck,                   /* in - metadata check type */
         vm_policy_t vmp,                   /* in - vm page locality */
         vm_offset_t offset,                /* in - starting offset */
         vm_size_t len,                     /* in - number of bytes */
         int ubc_flags                      /* in - ubc control */
         );

static
statusT
bs_pinpg_found( bfPageRefHT *bfPageRefH,      /* out */
               void **bfPageAddr,             /* out */
               struct bfAccess *bfap,         /* in */
               unsigned long bsPage,          /* in */
               bfPageRefHintT refHint,        /* in */
               vm_page_t pp,                  /* in */
               int flag,                      /* in */
               short metaCheck,               /* in - metadata check type */
               vm_policy_t vmp,               /* in - vm page locality */
               vm_offset_t offset,            /* in - starting offset */
               vm_size_t len,                 /* in - number of bytes */
               int ubc_flags                  /* in - ubc control */
               );

static
statusT
bs_pinpg_newpage( bfPageRefHT *bfPageRefH,      /* out */
                 void **bfPageAddr,             /* out */
                 struct bfAccess *bfap,         /* in */
                 unsigned long bsPage,          /* in */
                 bfPageRefHintT refHint,        /* in */
                 vm_page_t pp,                  /* in */
                 int flag,                      /* in */
                 short metaCheck,               /* in - metadata check type */
                 vm_policy_t vmp,               /* in - vm page locality */
                 vm_offset_t offset,            /* in - starting offset */
                 vm_size_t len,                 /* in - number of bytes */
                 int ubc_flags                  /* in - ubc control */
                 );

static
statusT
bs_pinpg_one_int( bfPageRefHT *bfPageRefH,  /* out */
         void **bfPageAddr,                 /* out */
         struct bfAccess* bfap,             /* in */
         unsigned long bsPage,              /* in */
         bfPageRefHintT refHint,            /* in */
         int flag,                          /* in */
         short metaCheck,                   /* in - metadata check type */
         vm_policy_t vmp,                   /* in - vm page locality */
         vm_offset_t offset,                /* in - starting offset */
         vm_size_t len,                     /* in - number of bytes */
         int ubc_flags                      /* in - ubc control */
         );

void
call_logflush( domainT *dmnP,
                lsnT lsn,
                int wait );

static
void
seq_ahead_cont(
    bfAccessT *bfap,        /* in */
    struct bsBuf *bp,       /* in */
    unsigned long bsPage,   /* in - bf page number */
    short metaCheck,        /* in - metadata check type */
    vm_policy_t vmp,        /* in - vm page locality */
    vm_offset_t offset,     /* in - starting offset */
    vm_size_t len,          /* in - number of bytes */
    int ubc_flags           /* in - ubc control */
    );

static
void
seq_ahead_start(
    bfAccessT *bfap,        /* in */
    struct bsBuf *bp,       /* in */
    int *listLen,           /* out - len of output ioDesc list */
    ioDescT **ioListp,      /* out - list of ioDesc */
    unsigned long bsPage,   /* in - bf page number */
    short metaCheck,        /* in - metadata check type */
    vm_policy_t vmp,        /* in - vm page locality */
    vm_offset_t offset,     /* in - starting offset */
    vm_size_t len,          /* in - number of bytes */
    int ubc_flags           /* in - ubc control */
    );

static
void
seq_ahead( struct bfAccess *ap,         /* in */
            unsigned long bsPage,       /* in */
            int maxBlks,                /* in */
            int *listLen,               /* in/out */
            ioDescT **ioListp,          /* out */
            short metaCheck,            /* in - metadata check type */
            vm_policy_t vmp,            /* in - vm page locality */
            vm_offset_t offset,         /* in - starting offset */
            vm_size_t len,              /* in - number of bytes */
            int ubc_flags               /* in - ubc control */
    );

#ifdef ADVFS_BSBUF_TRACE
void
bsbuf_trace( struct bsBuf *bp,
             uint16T      module,
             uint16T      line,
             void         *value)
{
    register bsbufTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    bp->trace_ptr = (bp->trace_ptr + 1) % BSBUF_TRACE_HISTORY;
    te = &bp->trace_buf[bp->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_BSBUF_TRACE */

/*
 * bs_get_bsbuf
 *
 * Dynamically allocates and initializes a bsBuf structure.
 * The bsBuf bufLock is initialized.
 * Returns a new bsBuf structure pointer or NULL if no memory available.
 */

struct bsBuf *
bs_get_bsbuf(int rad_id, int wait)
{
    struct bsBuf *bp;

    /* Malloc zeroes the memory. 
     * A null bp return means no memory is available.
     */
    if (wait)
        bp = (struct bsBuf *) ms_rad_malloc(sizeof(struct bsBuf),
                                            M_PREFER, rad_id);
    else
        bp = (struct bsBuf *) ms_rad_malloc_no_wait(sizeof(struct bsBuf),
                                                           M_PREFER, rad_id);

    if (bp) {
        mutex_init3(&bp->bufLock, 0, "bsBufLock", ADVbufLock_lockinfo);
        lk_init( &bp->lock, &bp->bufLock, LKT_BUF, 0, LKU_BUFFER );
        bp->bufMagic = BUFMAGIC;
    }
    return( bp );
}

#ifdef ADVFS_SMP_ASSERT
extern int AdvfsDomainPanicLevel;
#define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3  AdvfsDomainPanicLevel=3
#else
#define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3
#endif

/*
 * bs_free_bsbuf()
 * Deallocate a bsBuf memory structure, its ioDesc strucuture and locks.
 */

void
bs_free_bsbuf(
    struct bsBuf *bp    /* in - buffer header to free */
    )
{
    MS_SMP_ASSERT(!(bp->lock.state & ACC_DIRTY));
    bp -> bufMagic |= MAGIC_DEALLOC;
    if( bp->ioList.maxCnt > BUFIODESC ) {
        ms_free( bp->ioList.ioDesc );
    }
    lk_destroy(&bp->lock);
    mutex_destroy( &bp->bufLock );

    ms_free(bp);
}


/*
 * advfs_page_get - front-end for ubc_fs_page_get() 
 */

int
advfs_page_get(struct bsBuf *bp, int flags)
{
    vm_ubc_object_t vop = bp->bfAccess->bfObj;
    vm_page_t       pp = bp->vmpage;
    vm_offset_t     offset;
    int             result;

    offset = bp->bfPgNum * ADVFS_PGSZ;

    result = ubc_fs_page_get(&pp, bp->bfAccess->bfObj, offset, bp, flags);
    if (!pp && !(flags & ADVFS_GET_NOCACHE))
        ADVFS_SAD2("Advfs can't find expected ubc page", result, (long)bp);

#ifdef ADVFS_SMP_ASSERT
    if (pp != bp->vmpage)
        bp->last_vmpage = bp->vmpage;
#endif
    /* update in case the page was exchanged while it was not busy/held,
     * it is OK if this is invalid on a failure, it may be useful later.
     */
    bp->vmpage = pp;
    return(result);
}


/*
 * blkMap
 *
 * Interface to x_page_to_iolist.  Allocate a larger
 * array of ioDesc if one is not enough.
 *
 * SMP - Called with no mutexes held; may sleep in ms_malloc(),
 *       but the bsBuf is marked as BUSY or IO_TRANS if it is visible
 *       to other threads. 
 */
#define STEP 3
#define STEPLIM 20


static statusT
blkMap( struct bsBuf *bp, bfAccessT *bfap )
{
    int step = 0;
    statusT res;
    int i;

    /*
     * If we come into this routine with more than one
     * ioDesc, free the list and set up for BUFIODESC ioDesc.
     * This could happen when we've been asked to remap
     * a buffer.
     */
    if( bp->ioList.maxCnt > BUFIODESC ) {
#ifndef UBC_PROJECT_DEBUG
        for (i = 0; i < bp->ioList.maxCnt; i++) {
            MS_SMP_ASSERT(bp->ioList.ioDesc[i].ioQ == NONE);
        }
#endif
        ms_free( bp->ioList.ioDesc );
    }
    bp->ioList.maxCnt = BUFIODESC;
    bp->ioList.ioDesc = &bp->ioDesc;

    res = x_page_to_iolist ( bfap, bp->bfPgNum, &bp->ioList);

    while( res == E_BLKDESC_ARRAY_TOO_SMALL && step < STEPLIM ) {

        if( step ) {
#ifndef UBC_PROJECT_DEBUG
            for (i = 0; i < bp->ioList.maxCnt; i++) {
                MS_SMP_ASSERT(bp->ioList.ioDesc[i].ioQ == NONE);
            }
#endif
            ms_free( bp->ioList.ioDesc );
        }
        step += STEP;

        bp->ioList.ioDesc = (ioDescT *)ms_malloc( step * sizeof( ioDescT ) );
        bp->ioList.maxCnt = step;

        res = x_page_to_iolist ( bfap, bp->bfPgNum, &bp->ioList);
        /* bp->ioList will never need to be expanded beyond 2. */
        MS_SMP_ASSERT(res != E_BLKDESC_ARRAY_TOO_SMALL);
    }

    if ( res == EOK || res == E_PAGE_NOT_MAPPED ) {
        for( i = 0; i < bp->ioList.maxCnt; i++ ) {
            IODESC_CLR( bp, i );
            bp->ioList.ioDesc[i].numBlks = ADVFS_PGSZ_IN_BLKS;
        }
    }
    return( res );
}

/*
 * blkmap_direct()
 *
 * Description:
 *
 * Setup an ioDesc/ioList which describes the disk LBN mapping of
 * the requested buffer.  The buffer is described by a starting
 * block number (0 is beginning of the file) and the number of bytes
 * to transfer.  Contiguous pages will be combined.  There is no
 * limit to the number of bytes which may be mapped.
 *
 * Parameters and Flags
 *    struct bsBuf *bp              Pointer to bsBuf which describes
 *                                  the transfer and contains the ioList
 *                                  pointer (in and out)
 *    bfAccessT *bfap               Pointer to the file's bfap (in).
 *    unsigned long starting_block  File's starting block to map (in).
 *    int nbytes                    Number of bytes to map (in).
 *    struct ucred *cred            User's creditials.  Need for writes if
 *                                  storage needs to be added.
 */
statusT
blkmap_direct(struct bsBuf *bp,
              bfAccessT *bfap,
              unsigned long starting_block,
              int nbytes,
              struct ucred *cred)
{
    uint32T page;
    unsigned long next_block;
    int bytes_left = nbytes;
    int bytes_mapped;
    statusT res;
    ioDescT *ioDesc_Array = (ioDescT *)NULL;
    int ioDesc_count = 0;
    vdIndexT vdIndex;
    int sector_size;
    bfAccessT *origbfap = (bfAccessT *)NULL;
    bfSetT *bfSetp = bfap->bfSetp;
    int curxfersize;
    int max_transfer_size;
    vdT *vdp;


    /*
     * Handle cloned files if we are reading.  We may need to
     * read from the original's bitfile.
     */
    if ((bp->lock.state & READING) && (bfSetp->cloneId != BS_BFSET_ORIG)) {

        /*
         * Determine the original's bfap.
         */
        origbfap = bfap->origAccp;
    }

    /*
     * Determine the starting page.
     */
    page = (uint32T)(starting_block / ADVFS_PGSZ_IN_BLKS);

    /*
     * Map the first page.
     */
    bzero(&bp->ioList, sizeof(ioListT));
    bzero(&bp->ioDesc, sizeof(ioDescT));
    bp->ioList.maxCnt = BUFIODESC;
    bp->ioList.ioDesc = &bp->ioDesc;
    bp->ioDesc.bsBuf = bp;

    /*
     * Map the page using the ioDesc supplied in the bp.
     */
    res = x_page_to_iolist(
              (origbfap && !page_is_mapped(bfap, page, NULL,TRUE)) ? origbfap : bfap,
              page,
              &bp->ioList);
    if (res != EOK) {

        /* If we need to add storage and we are writing, do it now. */
        if (res == E_PAGE_NOT_MAPPED) {
            if (bp->lock.state & WRITING) {

                /* Even though fs_write() should have arranged to add 
                 * this storage before we reached this point, we could
                 * have been racing with a truncate when we dropped the
                 * file lock to obtain the active range for this write.
                 * In that case a truncate could have removed our storage,
                 * so we need to have code right here to add it back. 
                 */
                struct fsContext* contextp = VTOC(bfap->bfVp);
                MS_SMP_ASSERT( contextp ); /* cannot be an internal open */
                /* Object safety already has lock */
                if ( !bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY ){
                    FS_FILE_WRITE_LOCK( contextp );
                }
                res = fs_write_add_stg( bfap,
                                        (ulong_t) page,
                                        (ulong_t)bytes_left,
                                        bfap->file_size,
                                        0,       
                                        cred,
                                        0,
                                        NULL);
                if (res == EOK) {
                    /* Set this so storage added in a hole will still
                     * trigger the stats to get updated in fs_write().
                     */
                    contextp->dirty_alloc = TRUE;
                }
                /* Object safety s on - do not release lock */
                if ( !bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY ){
                    FS_FILE_UNLOCK( contextp );
                }

                if (res != EOK)
                    return(res);

                /* Try one more time to map the page if fs_write_add_stg()
                 * succeeded.
                 */
                res = x_page_to_iolist((origbfap &&
                                        !page_is_mapped(bfap, page, NULL,TRUE)) ?
                                         origbfap : bfap,
                                       page,
                                       &bp->ioList);
            }

            /*
             * If E_PAGE_NOT_MAPPED and reading then this page has
             * never been written.  Set the data_never_written flag
             * so bs_refpg_direct will zero out this section of the
             * user's buffer instead of doing a read.
             */
            else {
                res = EOK;
                bp->ioDesc.data_never_written = TRUE;
            }
        }

        if (res != EOK)
            return(res);
    }

    /*
     * Everything's cool, setup the ioDesc to the array.  It is possible
     * that we got E_PAGE_NOT_MAPPED above and don't have a vd.  In this
     * case default the sector_size to BS_BLKSIZE.
     */
    vdIndex = bp->ioDesc.blkDesc.vdIndex;
    if ( vdIndex != 0 ) {
        vdp = VD_HTOP(bp->ioDesc.blkDesc.vdIndex, bfap->dmnP);
        sector_size = vdp->vdSectorSize;
    } else {
        vdp = NULL;
        sector_size = BS_BLKSIZE;
    }

    bp->ioDesc.numBlks =
        min((ADVFS_PGSZ_IN_BLKS - ((int)starting_block % ADVFS_PGSZ_IN_BLKS)) *
            sector_size, bytes_left);
    bp->ioDesc.blkDesc.vdBlk += (uint32T)((int)starting_block % ADVFS_PGSZ_IN_BLKS);


    /*
     * How many bytes do we still need to map?
     */
    bytes_left -= bp->ioDesc.numBlks;

    /*
     * Allocate the initial ioDesc array.  Start with just
     * one entry.  Copy the ioDesc to this array.
     */
    if (bytes_left) {
        ioDesc_Array = (ioDescT *)ms_malloc(sizeof(ioDescT) * ++ioDesc_count);

        bcopy(&bp->ioDesc, ioDesc_Array, sizeof(ioDescT));
        next_block = bp->ioDesc.blkDesc.vdBlk +
                     (bp->ioDesc.numBlks / sector_size);
    }

    /* DirectIO policy is to permit IO request sizes up to the maximum
     * IO transfer byte size for an AdvFS volume that the underlying driver
     * will handle. Requests larger than the maximum size must be split
     * into multiple IO requests.
     */
    curxfersize = bp->ioDesc.numBlks;
    if ( vdp != NULL ) {
        if ( bp->lock.state & WRITING ) {
            max_transfer_size = vdp->max_iosize_wr;
        } else {
            max_transfer_size = vdp->max_iosize_rd;
        }
    } else {
        max_transfer_size = RDMAXIO;
    }

    while(bytes_left) {
        /*
         * Increment the page number and map it.
         */
        page++;
        res = x_page_to_iolist(
                   (origbfap && !page_is_mapped(bfap, page, NULL,TRUE)) ? origbfap : bfap,
                   page,
                   &bp->ioList);

        if (res != EOK) {
            /* If we need to add storage and we are writing,  do it now.  */
            if (res == E_PAGE_NOT_MAPPED) {
                if (bp->lock.state & WRITING) {

                    struct fsContext* contextp = VTOC(bfap->bfVp);
                    MS_SMP_ASSERT( contextp );
                    /* Object safety already has lock */
                    if ( !bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY ){
                        FS_FILE_WRITE_LOCK( contextp );
                    }
                    res = fs_write_add_stg(bfap,
                                           (ulong_t)page,
                                           (ulong_t)bytes_left,
                                           (ulong_t)(bfap->file_size +
                                             (ulong_t) (nbytes - bytes_left)),
                                           0,
                                           cred,
                                           0,
                                           NULL);
                    if (res == EOK) {
                        /* Set this so storage added in a hole will still
                         * trigger the stats to get updated in fs_write().
                         */
                        contextp->dirty_alloc = TRUE;
                    }
                    /* Object safety s on - do not release lock */
                    if ( !bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY ){
                        FS_FILE_UNLOCK( contextp );
                    }

                    /* Try one more time to map the page if fs_write_add_stg()
                     * succeeded.
                     */
                    if (res == EOK)
                        res = x_page_to_iolist(
                                  (origbfap &&
                                   !page_is_mapped(bfap, page, NULL,TRUE)) ?
                                      origbfap : bfap,
                                  page,
                                  &bp->ioList);
                } else {
                    /* If E_PAGE_NOT_MAPPED and reading then this page has
                     * never been written so mark it data_never_written.
                     */
                    res = EOK;
                    bp->ioDesc.data_never_written = TRUE;
                }
            }

            if (res != EOK) {
                /*
                 * We hit some kind of error which is preventing us
                 * from mapping all the data.  Some data was mapped above
                 * so return EOK and let fs_read/write() try again for
                 * the rest.
                 */
                res = EOK;
                break;
            }
        }

        /*
         * How many bytes were mapped in this page?  Note that
         * it may be less than the entire page.
         */
        bytes_mapped = min(bytes_left, ADVFS_PGSZ);

        /* Is this page contiguous and less than or equal to the max IO
         * transfer size? If not, add another ioDesc to our array and
         * fill it in.  It's contiguous if it is the same virtual drive,
         * next_block matches the previous page and the page_is_mapped
         * status from x_page_to_iolist() matches.  The vdIndex for an
         * unmapped pages is 0 so comparing them checks data_never_written.
         */
        if ((vdIndex != bp->ioDesc.blkDesc.vdIndex) ||
            (next_block != bp->ioDesc.blkDesc.vdBlk) ||
            (curxfersize + bytes_mapped > max_transfer_size)) {

            ioDescT *tmp_array;

            /*
             * It's possible that we got E_PAGE_NOT_MAPPED above and
             * don't have a vd.  In this case default the sector_size
             * to BS_BLKSIZE.
             */
            vdIndex = bp->ioDesc.blkDesc.vdIndex;
            if ( vdIndex != 0 ) {
                vdp = VD_HTOP(bp->ioDesc.blkDesc.vdIndex, bfap->dmnP);
                sector_size = vdp->vdSectorSize;
            } else {
                vdp = NULL;
                sector_size = BS_BLKSIZE;
            }

            curxfersize = 0;

            if ( vdp != NULL ) {
                if ( bp->lock.state & WRITING ) {
                    max_transfer_size = vdp->max_iosize_wr;
                } else {
                    max_transfer_size = vdp->max_iosize_rd;
                }
            } else {
                max_transfer_size = RDMAXIO;
            }

            tmp_array = (ioDescT *)ms_malloc(sizeof(ioDescT) * ++ioDesc_count);

            bcopy(ioDesc_Array, tmp_array,
                  sizeof(ioDescT) * (ioDesc_count - 1));
            ms_free(ioDesc_Array);
            ioDesc_Array = tmp_array;

            bp->ioDesc.numBlks = bytes_mapped;

            bcopy(&bp->ioDesc, &ioDesc_Array[ioDesc_count - 1],
                  sizeof(ioDescT));
            next_block = bp->ioDesc.blkDesc.vdBlk + ADVFS_PGSZ_IN_BLKS;
        } else {
        /*
         * Its contiguous.
         */
            ioDesc_Array[ioDesc_count - 1].numBlks += bytes_mapped;
            next_block += ADVFS_PGSZ_IN_BLKS;
        }

        bytes_left -= bytes_mapped;

        /* Increment the current byte tranfer count. Reset if greater than
         * maximum IO transfer size.
         */

        curxfersize += bytes_mapped;
    }

    /*
     * Update the bp's ioList and ioDesc.
     */
    if (ioDesc_count == 1) {
        bcopy(ioDesc_Array, &bp->ioDesc, sizeof(ioDescT));
        ms_free(ioDesc_Array);
    }
    else if (ioDesc_count > 1){
        bp->ioList.maxCnt = ioDesc_count;
        bp->ioList.ioDesc = ioDesc_Array;
    }

    return(res);
}

/*
 * bs_refpg_get
 *
 * Version of bs_refpg called from msfs_getpage.
 */

statusT
bs_refpg_get(
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         vm_page_t *pp,                 /* out - vm_page struct pointer */
         vm_policy_t vmp,               /* in - vm page locality */
         vm_offset_t offset,            /* in - future use */
         vm_size_t len,                 /* in - future use */
         int ubc_flags                  /* in - ubc control */
         )
{
    struct bsBuf *bp;
    void *bfPageAddr;
    statusT sts;
    
    sts = bs_refpg_int( (bfPageRefHT *)&bp, &bfPageAddr,
                         bfap, bsPage, refHint, TRUE, 0, BSBUF_CHK_NONE,
                         vmp, offset, len, ubc_flags);

    if (sts == EOK)
        *pp = bp->vmpage;
    else
        *pp = NULL;

    return(sts);
}

statusT
bs_refpg(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out - address of bf data */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint         /* in - hint to do read ahead */
         )
{
    return bs_refpg_int( bfPageRefH, bfPageAddr,
                         bfap, bsPage, refHint, FALSE, 0, BSBUF_CHK_NONE,
                         NULL, 0, 0, B_READ);
}

statusT
bs_refpg_fetch(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out - address of bf data */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         int fetchPages                 /* in - hint to do prefetch */
         )
{
    return bs_refpg_int( bfPageRefH, bfPageAddr,
                         bfap, bsPage, refHint, FALSE, fetchPages,
                         BSBUF_CHK_NONE, NULL, 0, 0, B_READ);
}

/*
 * Wrapper for reading (R)BMT pages.  Tests page validity.
 * Can't be called during domain init when bfap is NULL. see bs_map_bf
 */
statusT
bmt_refpg(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out - address of bf data */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint         /* in - hint to do read ahead */
         )
{
    statusT sts;

    /* bfap, bfap->dmnP, and dmnVersion must be set up */
    MS_SMP_ASSERT(bfap->dmnP->dmnVersion);

    /* callers read in a loop until they hit a non-existent bmt page,
     * detected by (sts == E_PAGE_NOT_MAPPED),
     * which probably isn't a great idea, but it is what it is.
     *
     * TO DO: test this code as an improvement here
     *
     *      if (bsPage && bsPage >= bfap->nextPage)
     *          return(E_PAGE_NOT_MAPPED);
     */

    sts = bs_refpg_int( bfPageRefH, bfPageAddr,
                        bfap, bsPage, refHint, FALSE, 0, BSBUF_CHK_BMTPG,
                        NULL, 0, 0, B_READ);
    return(sts);
}


/* Reference a page for *reading* given a bitfile handle and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address.
 * Called from bs_refpg_int() when the desired page is not found in
 * UBC and had to be allocated, UBC page must be held on entry.
 * Enter and leave with no locks held.
 */

static
statusT
bs_refpg_newpage(
                 bfPageRefHT *bfPageRefH,     /* out */
                 void **bfPageAddr,           /* out - address of bf data */
                 struct bfAccess *bfap,       /* in */
                 unsigned long bsPage,        /* in - bf page number */
                 bfPageRefHintT refHint,      /* in - hint to do read ahead */
                 struct flags flag,           /* in */
                 vm_page_t pp,                /* in - UBC page pointer */
                 int fetchPages,              /* in - # of pages to prefetch */
                 short metaCheck,             /* in - metadata check type */
                 vm_policy_t vmp,             /* in - vm page locality */
                 vm_offset_t offset,          /* in - starting offset */
                 vm_size_t len,               /* in - number of bytes */
                 int ubc_flags                /* in - ubc control */
                )
{
    ioDescT *ioListp;
    ioDescT *ioDescp;
    bfSetT *bfSetp;
    struct bsBuf *bp;
    int s;
    int listLen = 0;
    statusT sts = EOK;

    /* Initialize a new buffer descriptor. 
     * Other threads are blocked on the vm_page's pg_busy flag 
     * while this thread initializes the page.
     */
    bp = bs_get_bsbuf(PAGE_TO_MID(pp), TRUE);

    bp->ln = SET_LINE_AND_THREAD(__LINE__);
    bp->lock.state |= IO_TRANS | BUSY | READING;
    bp->bfPgNum = bsPage;
    bp->bfAccess = bfap;
    bp->ioCount = 1;
    bp->ioDesc.numBlks = ADVFS_PGSZ_IN_BLKS;
    bp->vmpage = pp;
    bp->ubc_flags = ubc_flags;
    bp->metaCheck = metaCheck;

    pp->pg_opfs = (unsigned long) bp;

    /* Map bf page to vd block. */
    sts = blkMap( bp, (flag.useOrigMap) ? bfap->origAccp : bfap);

    /*
     *  Check that the volume is still there.  This can happen if
     *  a reserved file is open via the .tags directory and the
     *  volume that contained that reserved file has been removed.
     *  If so, then return a error status that the user program
     *  will receive as a return value from the system call.
     *  It is possible that the volume would be removed after
     *  this test.
     */
    if (sts == EOK) { /* make sure blkMap succeeded */
        vdIndexT vdi = bp->ioList.ioDesc->blkDesc.vdIndex;
        MS_SMP_ASSERT( TEST_VDI_RANGE( vdi ) == EOK );
        if (!vd_htop_if_valid( vdi, bp->bfAccess->dmnP, FALSE, FALSE )) {
            sts = EIO;
        }
    }

    if ( sts != EOK ) {
        /*
         * Set the error result into the buffer header and call I/O completion
         * to wake up any waiters on this buffer.  This must be executed for
         * any condition that will not attempt i/o, otherwise the BUSY
         * condition would not be cleared and waiters awoken.
         */

        /* Getpage read special case handling. */
        if (sts == E_PAGE_NOT_MAPPED && flag.getflag) {
            /* See if we need to do frag processing (it might be a hole) */
            if ( bfap->fragState == FS_FRAG_VALID &&
                 bsPage == bfap->fragPageOffset ) {

                bfSetp = bfap->bfSetp;
                sts = copy_frag_into_vm_page( bfap->bfObj,
                                              offset,
                                              bfSetp->fragBfAp,
                                              bfap->fragId,
                                              ADVFS_PGSZ,
                                              pp );
                if (sts) {
                    /* set error result into buffer and call pseudo I/O
                     * completion. Set the UNMAPPED buffer flag and zero
                     * the page since the the page is not mapped to storage. 
                     */
                    ubc_page_zero(pp, 0, ADVFS_PGSZ);
                    mutex_lock( &bp->bufLock );
                    bp->lock.state |= UNMAPPED;
                    bp->result = sts;
                    MS_SMP_ASSERT(bp->lsnFwd == NULL);
                    MS_SMP_ASSERT(bp->directIO == 0);
                    /* have bs_io_complete() drop UBC pg_hold reference */
                    bp->ubc_flags |= B_BUSY;
                    bs_io_complete( bp, &s );
                    *bfPageRefH = NULL;
                    *bfPageAddr = NULL;
                    return sts;
                }
            } 
            else {
                /* Zero the UBC page and return successfully.
                 *
                 * The getpage faulted on a hole in the file or the page is at
                 * an offset greater than the frag page offset. Getpage already
                 * checks for reads starting past the end of file.
                 * However for NFS, a prior NFS setattr request may have 
                 * grown the file size but the file may not have been written
                 * up to that EOF yet. So the frag page offset may not have
                 * been changed.
                 */
                ubc_page_zero(pp,0,PAGE_SIZE);
            }
            mutex_lock( &bp->bufLock );
            bp->lock.state |= UNMAPPED;
            bp->result = EOK;
            sts = EOK;
            bs_io_complete(bp, &s);
            /* UBC page (pg_hold counter) reference remains until bs_derefpg.*/
            *bfPageRefH = (bfPageRefHT) bp;
            *bfPageAddr = (unsigned char *)ubc_load( bp->vmpage, 0, 0 );
            return sts;
       }
       else {
            /* Set the UNMAPPED buffer flag and zero the page since 
             * the page is not mapped to storage. 
             */
            ubc_page_zero(pp, 0, ADVFS_PGSZ);
            mutex_lock( &bp->bufLock );
            bp->lock.state |= UNMAPPED;
            bp->result = (sts == E_PAGE_NOT_MAPPED) ? EOK : sts;
            MS_SMP_ASSERT(bp->lsnFwd == NULL);
            MS_SMP_ASSERT(bp->directIO == 0);
            /* have bs_io_complete() drop UBC pg_hold reference */
            bp->ubc_flags |= B_BUSY;
            bs_io_complete(bp, &s);
            *bfPageRefH = NULL;
            *bfPageAddr = NULL;
            return sts;
       }
    }

    /* If reading ahead, attempt to create a read ahead list of buffers.  */
    if( DoReadAhead && refHint == BS_SEQ_AHEAD) {
        if( fetchPages ) {
            prefetch(bfap, bsPage + 1, fetchPages, &listLen, &ioListp, FALSE,
                     metaCheck, vmp, offset, len, ubc_flags);
        } else {
            seq_ahead_start(bfap, bp, &listLen, &ioListp, bsPage, metaCheck,
                            vmp, offset, len, ubc_flags);
        }
    }

    /* Perform the I/O; bsBuf not locked, but it is BUSY & READING. */
    ioDescp = &bp->ioList.ioDesc[bp->ioList.read];
 
    /* account for buffer we set up here */
    listLen++;

    if( listLen > 1 ) {
        /* Add local ioDesc into the list created by seq_ahead. */
        ioListp->bwd->fwd = ioDescp;
        ioDescp->bwd = ioListp->bwd;
        ioDescp->fwd = ioListp;
        ioListp->bwd = ioDescp;
        ioListp = ioDescp;
        bs_q_list( ioListp, listLen, bs_q_blocking );
    }
    else {
        if( listLen == 1 ) {
            ioDescp->fwd = ioDescp->bwd = ioDescp;
            bs_q_blocking( ioDescp, 1 );
        }
    }

    /* Wait for the I/O to complete. */
    ubc_page_wait(pp);

    /* Significant that we test buffer result and load final return
     * status from it when no i/o was attempted, as it deliberately
     * diverged from the local sts above.
     */
    if ( (sts = bp->result) != EOK ) {
        /* Drop UBC page (pg_hold counter) reference.*/
        ubc_page_release(pp, ubc_flags);
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
    }
    else {
        /* UBC page (pg_hold counter) reference remains until bs_derefpg. */
        *bfPageRefH = (bfPageRefHT) bp;
        *bfPageAddr = (unsigned char *)ubc_load( bp->vmpage, 0, 0 );
    }
    return(sts);
}

/*
 * bs_refpg_int
 *
 * Reference a page for *reading* given a bitfile handle and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address.
 *
 * NOTE:  This routine can return with the orig bitfile's COW lock locked.
 *        It will be released in derefpg().
 *
 * SMP:  Enter with no locks held.
 */

static
statusT
bs_refpg_int(
         bfPageRefHT *bfPageRefH,    /* out */
         void **bfPageAddr,          /* out - address of bf data */
         struct bfAccess *bfap,      /* in */
         unsigned long bsPage,       /* in - bf page number */
         bfPageRefHintT refHint,     /* in - hint to do read ahead */
         int getflag,                /* in - is msfs_getpage() the caller? */
         int fetchPages,             /* in */
         short metaCheck,            /* in - metadata check type */
         vm_policy_t vmp,            /* in - vm page allocation locality */
         vm_offset_t offset,         /* in - starting offset for read */
         vm_size_t len,              /* in - number of bytes to read */
         int ubc_flags               /* in - ubc control */
         )
{
    struct bsBuf *bp = NULL;
    struct bfAccess *origBfAp;
    vm_page_t pp;
    ioDescT *ioListp;
    struct flags flag;
    statusT sts;
    int cowLkLocked = FALSE;
    int useOrigMap = FALSE;
    int flags;
    int listLen;
    extern REPLICATED int SS_is_running;
    struct timeval new_time;
    int wday;
    uint32T totHotCnt;

    MS_SMP_ASSERT(bfap->refCnt != 0);

    if (bfap->bfSetp->cloneId != BS_BFSET_ORIG) {
        if (bfap->outOfSyncClone) {
            /*
             * The clone bitfile is out of sync with the orig.  Most likely
             * because we ran out of disk space during a copy-on write
             * operation.
             */
            sts = E_OUT_OF_SYNC_CLONE;
            goto exit;
        }

        origBfAp = bfap->origAccp;

        /*
         * Lock the 'copy on write' lock.  This serializes us with
         * other threads modifying the original bitfile.  The lock
         * is typically released by derefpg().
         */

        COW_LOCK_READ( &(origBfAp->cow_lk) )
        cowLkLocked = TRUE;

        /*
         * This is a clone bitfile.  If it doesn't map the page being
         * ref'd then we use the orig bitfiles access struct to
         * get the mapping.
         */

        if ( !page_is_mapped(bfap, (uint32T)bsPage, NULL,TRUE) ) {
            useOrigMap = TRUE;
        }
    }

    if( TrFlags & trRef )
        bfr_trace( bfap->tag, bsPage, Ref, 0 );

    /* Search UBC cache for file page. */
    flags = ubc_flags;
    sts = ubc_lookup(bfap->bfObj,
                     bsPage * ADVFS_PGSZ, /* Pg byte offset */
                     ADVFS_PGSZ,      /* Block size*/
                     ADVFS_PGSZ,      /* Request byte length */
                     &pp,             /* UBC page */
                     &flags,
                     vmp);            /* vm policy */

    if ( sts ) {
        goto exit;
    }

    /* Page found in the UBC cache. A cache hit. */
    if ((flags & B_NOCACHE) == 0) {
        /* Update statistics counters. */
        if (advfsstats) 
            AdvfsUbcHit++;
        bfap->dmnP->bcStat.ubcHit++;
        bfap->dmnP->bcStat.refHit++;

        bp = (struct bsBuf *)pp->pg_opfs;
        mutex_lock( &bp->bufLock );

        /* update in case the vm_page-to-bsBuf assignment changed */
        bp->vmpage = pp;

        /* This is an early exit on detection of I/O error on this buffer */
        if ((sts = bp->result) != EOK && !(sts >= MSFS_FIRST_ERR)) {
            mutex_unlock( &bp->bufLock );
            /* Clear the hold on the page acquired in ubc_lookup. */
            ubc_page_release(pp, ubc_flags);
            goto exit;
        }

        if (bp->lock.state & UNMAPPED) { 
            mutex_unlock(&bp->bufLock);
            /* getpage callers are given the clean page from the
             * original ref/pin that did the ubc_page_alloc.
             * internal callers can not reference a page with
             * no storage, specifically this is needed because
             * the bmt_read paths are really ugly and scan
             * until they hit the E_PAGE_NOT_MAPPED error.
             * besides, this makes the result identical for the
             * four cases of ref-new, ref-found, pin-new, pin-found.
             */
            if (!getflag) {
                ubc_page_release(pp, ubc_flags);
                sts = E_PAGE_NOT_MAPPED;
                goto exit;
            }
        } else {
            mutex_unlock(&bp->bufLock);
            /* Do read ahead if the buffer is mapped to storage. */
            if (DoReadAhead && refHint == BS_SEQ_AHEAD) {
                if ( fetchPages ) {
                    prefetch(bfap, bsPage + 1, fetchPages, &listLen, &ioListp,
                             TRUE, metaCheck, vmp, offset, len, ubc_flags);
                } else if (bsPage) {
                    seq_ahead_cont(bfap, bp, bsPage, metaCheck,
                                   vmp, offset, len, ubc_flags);
                }
            }
        }

        /* UBC page (pg_hold counter) reference remains until bs_derefpg. */
        *bfPageRefH = (bfPageRefHT) bp;
        *bfPageAddr = (void *)ubc_load(bp->vmpage, 0, 0);
    }
    else {
        /* Page was not found in the UBC cache. A cache miss.
         * Setup a new buffer page.
         */
        flag.getflag = (getflag == TRUE);
        flag.useOrigMap = (useOrigMap == TRUE);

        sts = bs_refpg_newpage(bfPageRefH, bfPageAddr, bfap, bsPage, refHint,
                               flag, pp, fetchPages, metaCheck,
                               vmp, offset, len, ubc_flags);
        /* Update statistics counters. */
        if (advfsstats) 
            AdvfsUbcMiss++;

        if (SS_is_running &&
            bfap->dmnP->ssDmnInfo.ssDmnSmartPlace &&
            (bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

            /* send cache miss message to smartstore if enough have occurred */
            TIME_READ(new_time);
            GETCURRWDAY(new_time.tv_sec,wday);
            atomic_incl(&bfap->ssHotCnt[wday]);
            for(totHotCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                totHotCnt += bfap->ssHotCnt[wday];
            }
            if(totHotCnt > bfap->dmnP->ssDmnInfo.ssAccessThreshHits) {
                ss_snd_hot(HOT_ADD_UPDATE_MISS, bfap);
            }
        }
    }
exit:
    if (cowLkLocked) {
        COW_UNLOCK( &(origBfAp->cow_lk) )
    }

    if (sts) {
        /* Return values */
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
    }
    /* Return values set by bs_refpg_newpage or cache hit path EOK case.*/
    return( sts );
}

/*
 * bs_refpg_direct()
 *
 * Description:
 *
 * Transfer data from disk to the user's buffer.  If non-AIO this
 * function will wait for the I/O to complete.  This function will
 * accept a request of any size, although the transfer must start
 * on a disk block boundary.
 *
 * This function will check the UBC and any requested data
 * which is found in the cache will be copied from the cache
 * to the user's buffer, avoiding a disk access.
 *
 * A bsBuf will be allocated via ms_malloc() instead of using the pool
 * of buffers.  This buffer will be released when the transfer has
 * completed.
 *
 * If the buffer is in userspace it will be wired down (via 
 * vm_map_pageable()) to prevent it from being swapped out.  This
 * will not be the case if this request is from the AIO interface since
 * the buffer will have already been mapped down.
 *
 * This routine will always be called with the request starting on a
 * disk sector boundary, but there is no requirement for it to end on
 * a sector boundary.  This means that the number of bytes which are
 * being read may not be evenly divisible by the sector size.  To handle
 * this, the numBlks field of the IoDesc will actually contain the number
 * of bytes instead of the number of blocks.
 *
 * Parameters:
 *
 *    void *addr                       Pointer to the users Buffer (in)
 *    int number_to_read               Number of bytes to transfer (in)
 *    struct bfAccess *bfap            Access pointer (in)
 *    unsigned long bsBlock            File's starting block number (in)
 *    int seg_flag                     Type of data, UIO_USERSPACE or
 *                                     UIO_SYSSPACE (in)
 *    struct buf *aio_bp               BP for AIO request, NULL
 *                                     otherwise (in)
 *    int *number_read                 number of bytes actually setup to be
 *                                     read (there could still be an I/O error)
 *    int *aio_flag                    0 if there is no outstanding I/O,
 *                                     1 if there is (out)
 */

statusT
bs_refpg_direct(void *addr,                    /* in */
                int number_to_read,            /* in */
                struct bfAccess *bfap,         /* in */
                unsigned long bsBlock,         /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_read,              /* out */
                int *aio_flag)                 /* out */
{
    struct bfAccess *origbfap = (struct bfAccess *)NULL;
    struct bsBuf *bp;
    statusT sts = EOK;
    unsigned long i;
    int npages, nbytes;
    ioDescT *desc_flink, *desc_blink;
    int list_cnt;
    int wait;
    unsigned long firstPage;
    unsigned long lastPage;
    vm_map_t mymap = (vm_map_t)NULL;
    bfSetT *bfSetp = bfap->bfSetp;
    int clear_bufs = 0;
    vm_offset_t start;
    vm_offset_t end;
    bfPageRefHT bfPageRefH;
    char *bfPageAddr;
    unsigned int dirty;
    int total_blocks = howmany(number_to_read, BS_BLKSIZE);
    int blks_to_end_of_first_page = ADVFS_PGSZ_IN_BLKS -
                                    (bsBlock % ADVFS_PGSZ_IN_BLKS);
    int blks_in_first_page = min(total_blocks, blks_to_end_of_first_page);
    struct processor *myprocessor;        /* used by mark_bio_wait */

    if (bfap->refCnt == 0) {
        /*
         * This is a racy test.
         */
        ADVFS_SAD0("bs_refpg_direct: bfAccess refCnt zero");
    }

    /*
     * Initialize number_read
     */
     *number_read = 0;

    /*
     * Determine the first and last pages being read.
     */
    firstPage = bsBlock / ADVFS_PGSZ_IN_BLKS;
    lastPage = (bsBlock + total_blocks - 1) / ADVFS_PGSZ_IN_BLKS;
    if (TrFlags & trRef)
        bfr_trace(bfap->tag, firstPage, Ref, 0);

    /*
     * How many pages (including partial pages) are being read?
     * Also check that our calculated range is within the actRange
     * that is currently held.
     */
    npages = lastPage - firstPage + 1;
    MS_SMP_ASSERT( bsBlock >= arp->arStartBlock && 
                   bsBlock <= arp->arEndBlock);
    MS_SMP_ASSERT( (bsBlock + total_blocks - 1) >= arp->arStartBlock &&
                   (bsBlock + total_blocks - 1) <= arp->arEndBlock);

    /*
     * Is this a clone?  If so, we may need to read from the
     * original bitfile.
     */
    if (bfSetp->cloneId != BS_BFSET_ORIG) {

        /*
         * The clone bitfile is out of sync with the orig.  Most likely
         * because we ran out of disk space during a copy-on write
         * operation.
         */
        if (bfap->outOfSyncClone) {
            return(E_OUT_OF_SYNC_CLONE);
        }

        /*
         * Lock the 'copy on write' lock.  This serializes us with
         * other threads modifying the original bitfile.
         */
        origbfap = bfap->origAccp;
        COW_LOCK_READ(&(origbfap->cow_lk))
    }

    /*
     * Are any needed pages in the cache?   If the request spans
     * multiple pages, say 1, 2, 3, and 4, there are 2 possible 
     * scenarios: First, page 1 is found in the cache but the remaining
     * pages are not.  In this case, the data from the first page is 
     * copied into the application buffer, and the remaining 3 pages will
     * be read from disk.  In the second sceanario, the first page is NOT
     * in the cache, but any of the remaining pages are in the cache.  In
     * this case, if any of these page are clean, nothing needs to be done.
     * If they are dirty, then they are flushed synchronously to disk.  
     * After all the pages are checked, the entire range is read from disk.
     * Notice that none of the pages are invalidated in this path. 
     * All this is done to prevent the request from becoming segmented 
     * and requiring multiple reads.
     */
    for (i = firstPage; i < (firstPage + npages); i++) {

        /*
         * Call bs_pinpg_one_int() with a flag of PINPG_DIRECTIO
         * to find and gain access to the page if it's in the
         * cache.
         */
        bfPageAddr = (char *)NULL;
        sts = bs_pinpg_one_int(&bfPageRefH, (void **)&bfPageAddr, bfap,
                               i, BS_NIL, PINPG_DIRECTIO, BSBUF_CHK_NONE,
                               NULL, 0, 0, (B_READ | B_CACHE));
        if ( sts == E_PAGE_NOT_MAPPED) {
            /* This is in the cache but it is not part of the file's extent
             * map, likely because it has been ref'd but no storage has been
             * associated with it yet.  blkmap_direct() will mark the page
             * data_never_written and because there is nothing to read, this
             * routine will treat the page as a sparse hole and provide zeros
             * back to the user.  
             */
            sts = EOK;
            clear_bufs = 1;
            continue;
        } else if (sts != EOK)
            break;

        if (bfPageAddr == (char *)NULL) {
            /* The buffer was not in the UBC.
             * Remember to flush any buffers which may be found after 
             * this one so we can do one, hopefully consolidated,
             * I/O to retrieve the data.
             */
            clear_bufs = 1;
            continue;
        }

        /* This page is in the cache. If previous pages in the range 
         * were not in the cache, and this page is dirty, flush it to
         * disk synchronously.  (We will later get the data in a
         * read from the disk with the rest of the page range). If
         * the page is clean, no need to flush, so just unpin it.
         * Note that since we hold the range lock for this part of the
         * file, it is perfectly safe to read the data from disk knowing
         * that the in-memory copy will not change because a writer will
         * have to wait for the range lock to be dropped.
         */
        if (clear_bufs) {
            (void)bs_get_bufstate (bfPageRefH, DIRTY, &dirty);
            sts = bs_unpinpg(bfPageRefH, logNilRecord,
                             dirty ? UNPIN_DIO_DIRTY : UNPIN_DIO_CLEAN);
            if (sts != EOK) break;
            continue;
        }

        /*
         * How many bytes should we read from this page?
         * No more than "number_to_read" and no more than
         * left in the page.
         */
        nbytes =
            min((ADVFS_PGSZ_IN_BLKS - (bsBlock % ADVFS_PGSZ_IN_BLKS)) *
                BS_BLKSIZE, number_to_read - *number_read);

        /*
         * Copy the data from the cache to the user's buffer.
         */
        if (seg_flag == UIO_USERSPACE) {
            if (copyout(bfPageAddr +
                        (bsBlock % ADVFS_PGSZ_IN_BLKS) * BS_BLKSIZE,
                        addr, (int)nbytes) != 0) {
                sts = EFAULT;
            }
        }
        else {
            bcopy(bfPageAddr + (bsBlock % ADVFS_PGSZ_IN_BLKS) *
                  BS_BLKSIZE, addr, (int)nbytes);
        }

        /*
         * Unpin the page.
         */
        (void)bs_unpinpg(bfPageRefH, logNilRecord, UNPIN_DIO_CLEAN);

        if ( sts != EOK )
            break;

        /*
         * Update the bfPageAddr, the number of bytes remaining
         * to be read, and the total number of bytes read.
         */
        addr = (char *)addr + nbytes;
        *number_read += nbytes;
        bsBlock += nbytes / BS_BLKSIZE;
    }

    /*
     * Exit if there isn't anything left to transfer or
     * if an error has occured?
     */
    if ((number_to_read == *number_read) || (sts != EOK)) {
        if (origbfap)
            COW_UNLOCK(&(origbfap->cow_lk));
        return(sts);
    }

    /*
     * Update the starting page.
     */
    firstPage = bsBlock / ADVFS_PGSZ_IN_BLKS;

    /*
     * Allocate a buffer.
     */
    bp = bs_get_bsbuf(CURRENT_MID(), FALSE);
    if (bp == NULL) {
        if (origbfap)
            COW_UNLOCK(&(origbfap->cow_lk));
        return(ENO_MORE_MEMORY);
    }

    /*
     * Fill in buffer descriptor fields.
     */
    bp->lock.state = IO_TRANS | BUSY | READING | RAWRW;
    bp->ln = SET_LINE_AND_THREAD(__LINE__);
    bp->bfPgNum = firstPage;
    bp->bfAccess = bfap;
    bp->directIO = 1;
    bp->procp = u.u_procp;
    bp->aio_bp = aio_bp;
    bp->bufDebug = BSBUF_DIRECTIO;

    /*
     * Map bf page to vd block.
     */
    sts = blkmap_direct(bp, bfap, bsBlock, number_to_read - *number_read,
                        (struct ucred *)NULL);

    /*
     * Exit now on error.
     */
    if (sts != EOK) {
 on_error:
        /* Release the buffer. */
        bs_free_bsbuf(bp);
        if (origbfap)
            COW_UNLOCK(&(origbfap->cow_lk));
        return(sts);
    }

    /* Wire the users's pages if we are reading into user
     * space and this is not a clone.  In the case of clones,
     * we have already wired these pages in fs_read() to avoid
     * a lock hierarchy violation here.  We don't expect an AIO
     * call to come here with a seg_flag of UIO_USERSPACE, so
     * check for this in testing, but handle it in the field.
     */
    MS_SMP_ASSERT( seg_flag == UIO_USERSPACE ? !aio_bp : 1 );
    if ( (seg_flag == UIO_USERSPACE) && !(bfap->origAccp) && !(aio_bp) ) {
        mymap = current_task()->map;
        start = (vm_offset_t)trunc_page(addr);
        end = (vm_offset_t)round_page((vm_offset_t)addr + number_to_read -
                                      *number_read);
        if (vm_map_pageable(mymap, start, end, VM_PROT_WRITE) != KERN_SUCCESS){
            sts = EFAULT;
            goto on_error;
        }
    }

    nbytes = 0;
    desc_flink = desc_blink = (ioDescT *)NULL;
    list_cnt = 0;

    for (i=0; i < bp->ioList.maxCnt; i++) {

        /*
         * Make sure that this page has been written before.
         * If not, data_never_written will be set by blkmap_direct.
         */
        if (bp->ioList.ioDesc[i].data_never_written) {
            /*
             * In this case zero out the user's buffer.
             */
            bzero((char *)addr + nbytes, bp->ioList.ioDesc[i].numBlks);
        }

        else {

            /*
             * Add the descriptor to our list and set it up.
             */
            if (desc_flink == NULL) {
              bp->ioList.ioDesc[i].bwd = &bp->ioList.ioDesc[i];
              bp->ioList.ioDesc[i].fwd = &bp->ioList.ioDesc[i];
              desc_flink = desc_blink = &bp->ioList.ioDesc[i];
            }
            else {
              bp->ioList.ioDesc[i].bwd = desc_blink;
              desc_blink->fwd = &bp->ioList.ioDesc[i];
              desc_blink = &bp->ioList.ioDesc[i];
            }

            /* We don't pass vmpage, we pass the user's mapped address */
            bp->ioList.ioDesc[i].targetAddr = (unsigned char *)addr + nbytes;
            list_cnt++;
        }

        nbytes += bp->ioList.ioDesc[i].numBlks;
    }

    /*
     * Update number_read
     */
    *number_read += nbytes;

    /*
     * Is there anything to read?
     */
    if (list_cnt) {

        /*
         * Finish linking the list.
         */
        desc_flink->bwd = desc_blink;
        desc_blink->fwd = desc_flink;

        /*
         * Set the bp's list count (ioCount).
         */
        bp->ioCount = list_cnt;

        /*
         * If this is an aio request, update the aio_bp's resid
         * field.  This must be done here since we cannot touch the
         * aio_bp after the IO has been queued.  If the IO fails 
         * after this, an error will be returned to the application 
         * via the aio interface, but the original aio_read() will 
         * return success.
         */
        if (aio_bp) {
            aio_bp->b_resid -= *number_read;
        }

        /* Set a pointer to the actRange in the buffer, and increment the
         * # of IosOutstanding for this active Range before enqueuing this
         * ioDescriptor. Seizing of the actRangeLock is only necessary
         * because arIosOutstanding will be decremented in bs_osf_complete(),
         * and we never know what thread will be doing that.  There can be
         * many buffers per active range.
         */
        bp->actRangep = arp;
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        mutex_lock( &bp->bfAccess->actRangeLock );
        arp->arIosOutstanding++;
        mutex_unlock( &bp->bfAccess->actRangeLock );

        /*
         * Queue the I/O
         */
        bs_q_list(desc_flink, list_cnt, bs_q_blocking);

        /*
         * If this is an Async I/O then don't wait for the
         * completion.  Bs_io_complete() will perform the call back
         * to the user's iodone routine and free the bp.
         */
        if (aio_bp) {
            MS_SMP_ASSERT( !mymap );  /* We should NOT have wired a page. */
            if (origbfap)
                COW_UNLOCK(&(origbfap->cow_lk));
            *aio_flag = 1;
            return(sts);
        }

        /*
         * Wait for the I/O to complete
         */
        mutex_lock(&bp->bufLock);
        wait = 0;
        mark_bio_wait;
        while (bp->lock.state & (BUSY | IO_TRANS)) {
            state_block(bp, &wait);
        }
        unmark_bio_wait;
        mutex_unlock(&bp->bufLock);
        sts = bp->result;       /* pass I/O return code to caller */
    }

    if (origbfap)
        COW_UNLOCK(&(origbfap->cow_lk));

    /* Unwire the user's buffer.  */
    if (mymap != (vm_map_t)NULL) {
        (void)vm_map_pageable(mymap, start, end, VM_PROT_NONE);
    }

    /* Release the buffer. */
    bs_free_bsbuf(bp);
    return(sts);
}


/*
 * bs_derefpg
 *
 * Given a reference handle, dereference the buffer page.
 *
 * SMP: enter with no explicit locks held.
 */


statusT
bs_derefpg(
           bfPageRefHT bfPageRefH,      /* in */
           bfPageCacheHintT cacheHint   /* in */
           )
{
    struct bsBuf *bp;

    /* Convert handle. */
    bp = (struct bsBuf *) bfPageRefH;
    MS_SMP_ASSERT(bp->bfAccess->refCnt);

    if( TrFlags & trRef ) {
        bfr_trace( bp->bfAccess->tag, bp->bfPgNum, Deref, 0 );
    }

    /* Update statistics counters. */
    if ((bp->bfAccess->dataSafety == BFD_FTX_AGENT) ||
        (bp->bfAccess->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {
        bp->bfAccess->dmnP->bcStat.derefFileType.ftx++;
    }
    if ((int)bp->bfAccess->tag.num < 0) {
        bp->bfAccess->dmnP->bcStat.derefFileType.meta++;
    }
    bp->bfAccess->dmnP->bcStat.derefCnt++;

    /* Drop UBC page (pg_hold counter) reference. */
    ubc_page_release(bp->vmpage, B_READ);
    return( EOK );
}


/*
 * bs_pinpg_get
 *
 * Indirectly services modify mmap page fault requests for VM.
 * Special buffer page pin path for msfs_getpage only
 * that does not require a subsequent bs_unpinpg call.
 * UBC page is setup for modification. But the AdvFS writeRef
 * counter remains unchanged.
 */

statusT
bs_pinpg_get(
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         vm_page_t *pp,                 /* out - vm_page struct pointer */
         vm_policy_t vmp,               /* in - vm page locality */
         vm_offset_t offset,            /* in - future use */
         vm_size_t len,                 /* in - future use */
         int ubc_flags                  /* in - ubc control */
         )
{
    statusT sts;
    struct bsBuf *bp;
    void *bfPageAddr;
    void (* q_fn)(ioDescT *, int);

    /* Can't pin clone pages; this was already checked in msfs_getpage() */
    MS_SMP_ASSERT(bfap->bfSetp->cloneId == BS_BFSET_ORIG);

retry_lookup:

    /* This pin avoids the cowing code; already done in msfs_getpage() */
    sts = bs_pinpg_one_int( (bfPageRefHT *)&bp, &bfPageAddr,
                            bfap, bsPage, refHint, PINPG_NOFLAG,
                            BSBUF_CHK_NONE, vmp, offset, len, ubc_flags );

    /* Now that the vm_page_t is pinned, decrement the 
     * associated bsBuf's write counter since no bs_unpinpg() is 
     * called by getpage callers. Getpage callers eventually call
     * ubc_page_release() directly and not through AdvFS.
     */

    if (sts) {
        *pp = NULL;
    } else {

        /* 
         * Wait for IO_TRANS state to clear and then set it.
         * This allows us to coordinate with set_block_map()
         * in the migrate path.  While IO_TRANS is set here,
         * set_block_map() is prevented from examining the
         * DIRTY flag.  And while we wait for IO_TRANS to
         * clear, we are prevented from examining the REMAP flag.
         * IO_TRANS also protects us from another mmapper comming in
         * while we are flushing below and thinking it needs to flush.
         */
 
        mutex_lock(&bp->bufLock);
        set_state( bp, __LINE__, IO_TRANS );

        /*
         * Check if the buffer has been remapped, in the process of migrating.  
         * If REMAP is set, it means that the bsBuf currently has only one
         * ioDesc and it is "pointing to" the destination location for the
         * migration.  Since any modifications to the page should go both to
         * the destination and source of the migration, we call blkMap() to
         * get two new ioDescs.  That way, any change that the caller of
         * bs_pinpg_get() makes (that is, any modifications made by a
         * program that has the page mmapped) will be written to both the
         * source location and the destination location of the migrate.
         * This is necessary so that if the migration fails, the changes
         * made by the mmapper will still exist in the original file location.
         */
        if (bp->lock.state & REMAP) {
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
            mutex_unlock( &bp->bufLock );
            blkMap( bp, bp->bfAccess );
            mutex_lock( &bp->bufLock );
            bp->lock.state &= ~REMAP;
        }

        /*
         * Decrement writeRef.  If we are the last ref'er and the bsBuf is
         * dirty, and somebody is waiting to flush it, we need to set it up
         * for I/O similar to bs_unpinpg() so they don't hang.  But we also
         * need to keep the ubc page dirty for the mmapper.
         *
         * In all cases when we exit we want GETPAGE_WRITE and DIRTY set.
         * The GETPAGE_WRITE is for bfflush() and bs_pinpg_put().  The DIRTY
         * flag must be set so that set_block_map() in the migrate path will
         * recognize that the page is dirty and double map it.
         *
         * On exit the bsBuf is never on an ioQ.
         */
        if ((--bp->writeRef == 0) && (bp->accFwd)) {

            /* must mark it now so if we fail and end up in bs_pinpg_put,
             * bs_invalidate_pages or bs_io_complete without doing I/O
             * we can handle it correctly since it is not on any queue.
             * dirtyBufList removal requires WRITING be set.
             */
            bp->lock.state |= (GETPAGE_WRITE | DIRTY | WRITING);

            if (bp->rflList ||
                SEQ_LTE(bp->flushSeq, bp->bfAccess->hiWaitLsn)) {

                /* Setup the UBC page for IO.  Note we keep the page
                 * on the ubc dirty list to prevent races.  We expect
                 * to always be able to make the page busy, but handle
                 * an unexpected failure by restarting a new lookup.
                 * We hope this gives ubc a chance to clean up whatever
                 * is wrong so we don't just keep looping, which is
                 * still probably better than taking down the domain.
                 */
                if ((sts = advfs_page_get(bp,
                      UBC_GET_BUSY | UBC_GET_HAVEHOLD | UBC_GET_KEEPDIRTY))) {
#ifdef ADVFS_SMP_ASSERT
                    ADVFS_SAD2("bs_pinpg_get: failed advfs_page_get",
                               sts, (long)bp);
#endif
                    clear_state( bp, IO_TRANS );
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                    mutex_unlock( &bp->bufLock );
                    ubc_page_release(bp->vmpage, B_WRITE);
                    goto retry_lookup;
                }

                bp->lock.state |= (BUSY | KEEPDIRTY);
                bp->ubc_flags = 0;      /* we keep our hold */
#ifdef ADVFS_SMP_ASSERT
                bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                bp->ioqLn = -1;
#endif

                link_write_req( bp );
                mutex_unlock( &bp->bufLock );
                bs_q_list( &bp->ioList.ioDesc[bp->ioList.write],
                           bp->ioList.writeCnt,
                           bs_q_blocking);

                /* Wait for the IO completion since the getpage caller
                 * will be modifying the page.  bs_io_complete() removes
                 * the bsBuf from the dirtyBufList but does not clear
                 * GETPAGE_WRITE and DIRTY because we set KEEPDIRTY.
                 */
                ubc_page_wait(bp->vmpage);
                mutex_lock(&bp->bufLock);
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
            }
            else {
                /* we leave here with the bsBuf on the dirtyBufList */
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                clear_state( bp, IO_TRANS );
            }
        }
        else {
            /* Here the bsBuf may be on the dirtyBufList if writeRef > 0
             * or not on the dirtyBufList if it wasn't dirty before.
             */
            bp->lock.state |= (GETPAGE_WRITE | DIRTY);
            clear_state( bp, IO_TRANS );
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        *pp = bp->vmpage;
        mutex_unlock(&bp->bufLock);
    }

    return sts;
}

statusT
bs_pinpg( bfPageRefHT *bfPageRefH,      /* out */
         void **bfPageAddr,             /* out */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in */
         bfPageRefHintT refHint )       /* in */
{
    return( bs_pinpg_clone( bfPageRefH, bfPageAddr,
                            bfap, bsPage, refHint, FtxNilFtxH, PINPG_NOFLAG,
                            BSBUF_CHK_NONE, NULL, 0, 0, B_WRITE ) );
}

statusT
bs_pinpg_ftx( bfPageRefHT *bfPageRefH,       /* out */
              void **bfPageAddr,             /* out */
              struct bfAccess* bfap,         /* in */
              unsigned long bsPage,          /* in */
              bfPageRefHintT refHint,        /* in */
              ftxHT ftxH )                   /* in */
{
    return( bs_pinpg_clone( bfPageRefH, bfPageAddr,
                            bfap, bsPage, refHint, ftxH, PINPG_NOFLAG,
                            BSBUF_CHK_NONE, NULL, 0, 0, B_WRITE ) );
}

/* 
 * bs_pinpg_put
 *
 * Writes given pages to disk immediately but does not wait for
 * the IO completion.
 * Special buffer page pin path for msfs_putpage only
 * that does not require a subsequent bs_unpinpg call.
 *
 * Assumptions: 
 *     The putpage caller must setup the UBC pages for IO processing.
 *     This means the UBC pages' pg_busy flags are set, the pg_dirty
 *     flags are clear, and the pages are moved from the UBC object's
 *     dirty to clean list.
 */

statusT
bs_pinpg_put(vm_page_t plp,             /* in */
             int plcnt,                 /* in */
             int ubc_flags)             /* in */
{
    struct bsBuf * bp;
    statusT sts;
    void (* q_fn)(ioDescT *, int) = bs_q_lazy;
    int listLen = 0;
    ioDescT *ioListp;
    int noqfnd;
    vm_page_t curpg;
    ioDescT *ioList=0, *save;
    int listLenMaster=0,
        pageCount;
    long prevLn;
#ifdef ADVFS_SMP_ASSERT
    vm_object_t first_pg_object = plp->pg_object;
#endif

    for (curpg = plp, pageCount = 0 ;
         pageCount < plcnt;
         curpg = plp, pageCount++) {

        /* check why does this really matter? */
        MS_SMP_ASSERT(curpg->pg_object == first_pg_object);

        /* save next page in chain and (paranoid) clean up the list */
        plp = curpg->pg_pnext;
        curpg->pg_pprev = curpg->pg_pnext = NULL;

        bp =  (struct bsBuf *)curpg->pg_opfs;
        ioListp = NULL;
        listLen = 0;
        mutex_lock( &bp->bufLock );
    
        /* Wait for IO_TRANS state to clear, mainly for migrate. */
        prevLn = bp->ln;
        set_state( bp, __LINE__, IO_TRANS );
    
        MS_SMP_ASSERT(!bp->writeRef);
    
        /* Update statistics counter. */
        bp->bfAccess->dmnP->bcStat.unpinCnt.blocking++;

        /* update in case the vm_page-to-bsBuf assignment changed */
        bp->vmpage = curpg;

        if (bp->accFwd) {
    
            /* The bsBuf is on the dirtyBufList for the access structure.
             * Pull any ioDesc's belonging to it off the lazy queues.
             */
            bp->lock.state |= REMOVE_FROM_IOQ;
            rm_from_lazyq( bp, &ioListp, &listLen, &noqfnd);
            bp->lock.state &= ~REMOVE_FROM_IOQ;
            MS_SMP_ASSERT(bp->vmpage->pg_busy);
            MS_SMP_ASSERT(bp->lock.state & IO_TRANS);

            if (listLen) {
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
            }
            else if ((noqfnd) && (bp->lock.state & GETPAGE_WRITE)) {

                /* As of bigpages, bs_pinpg_get will leave
                 * any previously-dirty bsBuf on the dirtyBufList so
                 * unless another unpinner/flusher intervenes, this
                 * is where we will eventually write the data once
                 * all mmap holds have been released and the page
                 * ages sufficiently to be sync'd or recycled.
                 *
                 * We need to link together the bsBuf's ioDescs
                 * and send them out to disk.
                 */
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                link_write_req( bp );
                listLen = bp->ioCount;
                ioListp = bp->ioList.ioDesc;
            }
            else {

                /* Prevent unidentifiable corruption.  The only legal way
                 * we can be in this path is because of a domain_panic
                 * that left a dirty buffer off the queues.  Force crash
                 * in development or domain_panic and I/O completion.
                 * must release lock to get live dump then retake it
                 */
                int s;
                unsigned int dump_state = bp->lock.state;
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(bp->bfAccess->dmnP,
                    "bs_pinpg_put(1): bad state %x bp %p ln %p pg %p sz %x\n",
                    dump_state, bp, prevLn, curpg, curpg->pg_size);
                mutex_lock(&bp->bufLock);
                bp->lock.state |= (BUSY | WRITING);
                bp->ubc_flags = ubc_flags;
                bs_io_complete(bp, &s);
            }
        } 
        else {

            /* This page was on the dirty list for the UBC object but not
             * on the bfap->dirtyBufList.  That usually means it was being
             * modified by an mmapper and we should have the bsBuf marked
             * as such.  UBC moves the page to the clean list before the
             * call to putpage.
             *
             * Pin requests to sparse hole pages will exist on the object
             * dirty list.  Don't flush, just call UBC to clear the pg_busy.
             *
             * We can also have a page which could not be read on the object
             * dirty list.  This is because ubc marks a page dirty on the
             * lookup for write.  It remains dirty since we don't signal the
             * error to ubc because we don't want to keep retrying.  In that
             * case we have a page of trash and we don't want to write it
             * since the write can succeed and then we have on-disk
             * corruption (unreadable is better than unknown updates).
             */

            if ((bp->lock.state & UNMAPPED) || bp->result) {

                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                clear_state(bp, IO_TRANS);
                mutex_unlock(&bp->bufLock);

                ubc_page_release(curpg, ubc_flags | B_DONE);
            }
            else if (bp->lock.state & GETPAGE_WRITE) {

                /* The mmap path marked the bsbuf GETPAGE_WRITE so put
                 * it on the bfap->dirtyBuflist for AdvFS I/O.
                 */
                mutex_lock( &bp->bfAccess->bfIoLock );
                LSN_INCR( bp->bfAccess->nextFlushSeq );
                bp->flushSeq = bp->bfAccess->nextFlushSeq;
                ADD_DIRTYACCESSLIST( bp, FALSE );
                mutex_unlock( &bp->bfAccess->bfIoLock );
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                link_write_req( bp );
                listLen = bp->ioCount;
                ioListp = bp->ioList.ioDesc;
            } 
            else {

                /* Prevent unidentifiable corruption.  The only legal way
                 * we can be in this path is because of a domain_panic
                 * (probably in unpin) that left a dirty buffer off the
                 * dirtyBufList.  Force crash in development or
                 * domain_panic and page release.
                 * must release lock to get live dump then retake it
                 */
                unsigned int dump_state = bp->lock.state;
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(bp->bfAccess->dmnP,
                    "bs_pinpg_put(2): bad state %x bp %p ln %p pg %p sz %x\n",
                    dump_state, bp, prevLn, curpg, curpg->pg_size);
                mutex_lock(&bp->bufLock);
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                clear_state(bp, IO_TRANS);
                mutex_unlock( &bp->bufLock );
                ubc_page_release(curpg, ubc_flags | B_DONE);
            }
        }

        /* 
         * If we have ioDescs to add to the growing master
         * list, add them to the list now.
         */ 
        if (listLen) {
            listLenMaster += listLen;
        
            if (ioList == NULL) {
                ioList = ioListp;
            } else {
                /*
                 * Add ioDesc(s) to the end of the list.
                 */
                ioList->bwd->fwd = ioListp;
                ioListp->bwd->fwd = ioList;
                save = ioList->bwd;
                ioList->bwd = ioListp->bwd;
                ioListp->bwd = save;
            }
            /* Save UBC flags to pass to IO completion. */
            bp->ubc_flags = ubc_flags;
            bp->lock.state |= (BUSY | WRITING);
            mutex_unlock( &bp->bufLock );
        }

    } /* end for loop */

    /* Send the master list of buffers to disk using the
     * dedicated ubcreq queue.
     */
    if (listLenMaster) {
        bs_q_list( ioList, listLenMaster, bs_q_ubcreq );
    }

    return(EOK);
}

/*
 * bs_pinpg_clone
 *
 * Reference a page for *writing* given a bitfile handle and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address, if any.
 */

static
statusT
bs_pinpg_clone( bfPageRefHT *bfPageRefH,   /* out */
         void **bfPageAddr,                /* out */
         struct bfAccess* bfap,            /* in */
         unsigned long bsPage,             /* in */
         bfPageRefHintT refHint,           /* in */
         ftxHT ftxH,                       /* in */
         int flag,                         /* in */
         short metaCheck,                  /* in - metadata check type */
         vm_policy_t vmp,                  /* in - vm page locality */
         vm_offset_t offset,               /* in - starting offset */
         vm_size_t len,                    /* in - number of bytes */
         int ubc_flags                     /* in - ubc control */
         )
{
    statusT sts = EOK;
    bfSetT *bfSetp;

    bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    if ( bfSetp->cloneSetp ) {
        /*
         * The set has been cloned at least once.  Check to
         * see if we need to do any 'copy on write' processing.
         */
        bs_cow( bfap, COW_PINPG, bsPage, 1, ftxH );
    }

    return( bs_pinpg_one_int( bfPageRefH, bfPageAddr, bfap,
                              bsPage, refHint, flag, metaCheck,
                              vmp, offset, len, ubc_flags ) );
}

/*
 * bs_wakeup_flush_threads
 *
 * Notify any threads that are flushing this file and are waiting
 * for this buffer to be flushed that this buffer has been flushed.
 *
 * The caller must hold the bp->bufLock.
 * The caller must hold the bp->bfAccess->bfIoLock.
 */

void
bs_wakeup_flush_threads(struct bsBuf *bp,   /* in - Buffer being released */
                       int recordDiskError  /* in - TRUE if got an I/O error */
                      )
{
    bfAccessT *bfap = bp->bfAccess;
    flushWaiterT *curFlushWaiter, *nextFlushWaiter;
    struct bsBuf *prevbp;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfIoLock.mutex));

    if (recordDiskError) {
        if( bfap->dkResult == EOK ) {
            bfap->dkResult = bp->result;
        }
        if( bfap->miDkResult == EOK ) {
            bfap->miDkResult = bp->result;
        }
    }

    /*
     **************************************************************
     * START OF LOG FILE-SPECIFIC WAKEUP CODE
     *
     * This code wakes up threads waiting on the bfap->flushWaiterQ.
     * Formerly, all file flush threads would do this.  Now, a
     * new algorithm is used, which does not use this queue.
     * The only code that still uses this flushWaiterQ is the
     * logfile-specific routines which call bfflush_sync().
     **************************************************************
     */

    /* If this is the first buffer on the dirty list, we can wake up
     * any threads waiting for this LSN to be flushed.
     */
    if ( bp == bfap->dirtyBufList.accFwd ) {
        bfap->hiFlushLsn = bp->flushSeq;

        /* Wakeup all flush sync threads whose IO is finished by
         * checking the wait LSN.
         */
        curFlushWaiter = bfap->flushWaiterQ.head;

        MS_SMP_ASSERT(!LSN_EQ_NIL(bp->flushSeq));

        while ((curFlushWaiter != (flushWaiterT *) &bfap->flushWaiterQ) &&
               SEQ_LTE(curFlushWaiter->waitLsn, bp->flushSeq)) {

            /* Deque the current waiter from the flushWaiter queue. */
            nextFlushWaiter = curFlushWaiter->fwd;

            if (bfap->flushWaiterQ.tail == curFlushWaiter) {
                bfap->flushWaiterQ.tail = (flushWaiterT *)&bfap->flushWaiterQ;
            }
            else {
                nextFlushWaiter->bwd = (flushWaiterT *) &bfap->flushWaiterQ;
            }
            bfap->flushWaiterQ.head = nextFlushWaiter;

            if (AdvfsLockStats)
                AdvfsLockStats->bfFlushBroadcast++;
            bfap->flushWaiterQ.cnt--;

            thread_wakeup((vm_offset_t) curFlushWaiter);

            ms_free(curFlushWaiter);
            curFlushWaiter = nextFlushWaiter;
        }
    } else {
        /*
         * We are about to give the previous buffer's LSN
         * the value of the current buffer's LSN.  If the
         * previous buffer's LSN is the same as the hiWaitLsn
         * for this file, then we also need to give the
         * hiWaitLsn the value of the current buffer's LSN.
         * This is so bs_unpinpg() will still put the
         * previous buffer onto the blocking queue.  If we
         * did not bump hiWaitLsn here, then at the completion
         * of this routine, the hiWaitLsn would be less than
         * the previous buffer's LSN and we would end up putting
         * the previous buffer onto the lazy queue in bs_unpinpg().
         * This would be bad because bfflush_sync() would hang,
         * waiting for that buffer to be written.
         */
        prevbp = bp->accBwd;

        MS_SMP_ASSERT(prevbp->bfAccess->accMagic == ACCMAGIC);
        if (LSN_EQL(prevbp->flushSeq, bfap->hiWaitLsn)) {
            bfap->hiWaitLsn = bp->flushSeq;
        }
        
        MS_SMP_ASSERT(LSN_EQ_NIL(prevbp->flushSeq) ||
                      LSN_GT(bp->flushSeq,prevbp->flushSeq));
        prevbp->flushSeq = bp->flushSeq;
    }

    /*
     **************************************************************
     * END OF LOG FILE-SPECIFIC WAKEUP CODE
     **************************************************************
     */

    /*
     **************************************************************
     * START OF GENERAL PURPOSE WAKEUP CODE
     *
     * Currently, all file flush logic except for logfile and
     * migrate-specific paths use the rangeFlushT structure
     * algorithm which follows.
     **************************************************************
     */

    /*
     * If one or more threads are waiting for this buffer to complete
     * its I/O as part of a file or range flush, walk the rangeFlushLinkT
     * list for this buffer, waking up any range flush waiters if
     * this is the last I/O they were waiting for.  Deallocate
     * each rangeFlushLinkT as we use it.
     */
    if (bp->rflList) {
        rangeFlushLinkT *rflp;
        rangeFlushT *rfp;

        do {
            rflp = bp->rflList;
            rfp = rflp->rfp;
            mutex_lock(&rfp->rangeFlushLock);
            MS_SMP_ASSERT(rfp->outstandingIoCount > 0);

#ifdef ADVFS_SMP_ASSERT
            /*
             * The following assertion cannot be applied to metadata
             * files or clones since their bfap->file_size field, from which
             * rfp->lastPage is derived on a full-file flush, is
             * not maintained consistently.
             */
            if (!BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag) &&
                (bfap->bfSetp->cloneId == BS_BFSET_ORIG)) {
                MS_SMP_ASSERT(rfp->outstandingIoCount <= ((rfp->lastPage - rfp->firstPage) + 1));
            }
#endif
            if (--rfp->outstandingIoCount == 0) {
                thread_wakeup_one((vm_offset_t)&rfp->outstandingIoCount);
            }
            mutex_unlock(&rfp->rangeFlushLock);

            bp->rflList = rflp->rflFwd;
            ms_free(rflp);
        } while (bp->rflList);
    }

    /*
     **************************************************************
     * END OF GENERAL PURPOSE WAKEUP CODE
     **************************************************************
     */

}

/*
 * bs_pinpg_int - no clone stuff
 *
 * Reference a page for *writing* given an access structure and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address, if any.  Doesn't include any clone checking.
 */

/* TO DO: it would be nice to pass the check in, but too much work now! */
statusT
bs_pinpg_int(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in */
         bfPageRefHintT refHint,        /* in */
         int flag )                     /* in */

{
    return( bs_pinpg_one_int( bfPageRefH, bfPageAddr,
                              bfap, bsPage, refHint, flag, BSBUF_CHK_NONE,
                              NULL, 0, 0, B_WRITE ) );
}


/*
 * bs_pinpg_one_int - no clone handling
 *
 * Reference a page for *writing* given a bitfile handle and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address, if any.  Doesn't include any clone checking.
 *
 * A note on several of the flags:
 * 1. PINPG_DIRECTIO signals bs_pinpg_one_int() to NOT
 *    drag this page into the cache if it is not already
 *    in the UBC.
 * 2. The PINPG_DIO_REMOVE flag indicates that the calling
 *    thread will remove this page from the cache if it is
 *    returned.  This is used to synchronize racing threads so
 *    that they will wait for this page to be removed from the
 *    cache before proceeding.  The actRange structs no longer
 *    protect this race since several active ranges can now span
 *    an underlying 8k page.
 *
 * SMP:  Enter and leave with no locks held.  The bsBuf will
 *       be locked and released internally.
 */

static
statusT
bs_pinpg_one_int(
                 bfPageRefHT *bfPageRefH,       /* out */
                 void **bfPageAddr,             /* out */
                 struct bfAccess *bfap,         /* in */
                 unsigned long bsPage,          /* in */
                 bfPageRefHintT refHint,        /* in */
                 int flag,                      /* in */
                 short metaCheck,               /* in - metadata check type */
                 vm_policy_t vmp,               /* in - vm page locality */
                 vm_offset_t offset,            /* in - future use */
                 vm_size_t len,                 /* in - future use */
                 int ubc_flags                  /* in - ubc control */
                )
{
    struct bsBuf *bp = NULL;
    statusT sts = EOK;
    vm_page_t pp;
    int uflags, directIO;
    extern REPLICATED int SS_is_running;
    struct timeval new_time;
    int wday;
    uint32T totHotCnt;

    if( TrFlags & trPin )
        bfr_trace( bfap->tag, bsPage, Pin, 0 );

    /* look for the page in UBC */

    directIO = flag & PINPG_DIRECTIO;
    uflags = ubc_flags;
    sts = ubc_lookup(bfap->bfObj,
                     bsPage * ADVFS_PGSZ, /* Pg byte offset */
                     ADVFS_PGSZ,      /* Block size*/
                     ADVFS_PGSZ,      /* Request byte length */
                     &pp,             /* UBC page */
                     &uflags,
                     vmp);            /* vm policy */

    /*  Exit on error or when DirectIO requests encounter a cache miss.
     *  DirectIO only wants existing cache pages.
     */
    if (sts || (directIO && (uflags & B_NOCACHE))) {
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return( sts );
    }

    /* Page found in the UBC cache. A cache hit. */
    if ((uflags & B_NOCACHE) == 0) {

        /* If this was a directIO request indicating that the page
         * will be removed, and we found a clean page in the cache, 
         * we want to invalidate it and return the status of 
         * 'cache miss' to the caller.  If the page is dirty
         * (ubc_lookup set B_DIRTY in uflags if the page is dirty),
         * we fall into bs_pinpg_found() to handle the page flushing.
         */
        if ( (flag & PINPG_DIO_REMOVE) && !(uflags & B_DIRTY) ) {
            vm_offset_t offset = pp->pg_offset;

            /* release it the same way we got it to keep counts correct */
            ubc_page_release(pp, ubc_flags);
            ubc_invalidate( bfap->bfObj, offset, ADVFS_PGSZ, B_INVAL );
            *bfPageRefH = NULL;
            *bfPageAddr = NULL;
            return( sts );
        }

        sts = bs_pinpg_found(bfPageRefH, bfPageAddr, bfap, bsPage, refHint,
                             pp, flag, metaCheck, vmp, offset, len, ubc_flags);
        /* Update statistics counters. */
        if (advfsstats) 
            AdvfsUbcHit++;
        bp = (struct bsBuf *)*bfPageRefH;
        MS_SMP_ASSERT((bp==NULL)|| bp->vmpage->pg_dirty);
    }
    else {
        /* Page is not in the UBC cache. A cache miss.
         * Setup a new buffer page.
         */
        sts = bs_pinpg_newpage(bfPageRefH, bfPageAddr, bfap, bsPage, refHint,
                               pp, flag, metaCheck,
                               vmp, offset, len, ubc_flags);
        /* Update statistics counters. */
        if (advfsstats) 
            AdvfsUbcMiss++;
        bp = (struct bsBuf *)*bfPageRefH;
        MS_SMP_ASSERT((bp==NULL)|| bp->vmpage->pg_dirty);

        if (SS_is_running &&
            bfap->dmnP->ssDmnInfo.ssDmnSmartPlace &&
            (bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

            /* send cache miss message to smartstore if enough have occurred */
            TIME_READ(new_time);
            GETCURRWDAY(new_time.tv_sec,wday);
            atomic_incl(&bfap->ssHotCnt[wday]);
            for(totHotCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                totHotCnt += bfap->ssHotCnt[wday];
            }
            if(totHotCnt > bfap->dmnP->ssDmnInfo.ssAccessThreshHits) {
                ss_snd_hot(HOT_ADD_UPDATE_MISS, bfap);
            }
        }
    }

    return(sts);
}

/* Reference a page for *writing* given a bitfile handle and
 * the page number in the bitfile.
 * Called from bs_pinpg_one_int() when the desired page is found
 * in the UBC and has a valid buffer associated with it. 
 * Acquires and releases the bufLock internally.
 *
 * A successful return keeps the UBC page's pg_hold counter reference
 * up until bs_derefpg.
 */
static
statusT
bs_pinpg_found(
               bfPageRefHT *bfPageRefH,       /* out */
               void **bfPageAddr,             /* out */
               struct bfAccess *bfap,         /* in */
               unsigned long bsPage,          /* in */
               bfPageRefHintT refHint,        /* in */
               vm_page_t pp,                  /* in */
               int flag,
               short metaCheck,               /* in - metadata check type */
               vm_policy_t vmp,               /* in - vm page locality */
               vm_offset_t offset,            /* in - future use */
               vm_size_t len,                 /* in - future use */
               int ubc_flags                  /* in - ubc control */
              )
{
    int wait = 0;
    int listLen = 0;
    ioDescT *ioListp;
    statusT sts = EOK;
    struct bsBuf *bp;
    int noqfnd;

    /* Page was found in UBC and has a valid buffer. */
    bp = (struct bsBuf *)pp->pg_opfs;

    /* Update statistics counters. */
    bfap->dmnP->bcStat.ubcHit++;
    bfap->dmnP->bcStat.pinHit++;

    mutex_lock( &bp->bufLock );

    /* update in case the vm_page-to-bsBuf assignment changed */
    bp->vmpage = pp;

    /* If this is a directIO check of whether the page is in
     * the cache, and this page is being removed from the cache
     * by another directIO thread, then merely wait for that
     * page to be removed by the other thread. Otherwise, mark
     * it so that other threads know that we will remove it.
     */
    if ( flag & PINPG_DIRECTIO ) {
        /* The order of the next if..else stmt is significant!
         * It is legitimate for several threads to call here with
         * PINPG_DIO_REMOVE for the same page if their active
         * ranges overlap on that page.  In this case, the first
         * thread does the invalidation and the subsequent threads
         * must wait and return a NULL bfPageAddr.
         */
        if ( bp->lock.state & DIO_REMOVING ) {
            /* Wait for DIO_REMOVING to be cleared. It will only be
             * cleared after this buffer has been flushed to disk.
             */
            wait = 0;
            while( bp->lock.state & DIO_REMOVING ) {
                state_block( bp, &wait );
            }
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold counter) reference. 
             * Note that even though this thread may have caused 
             * buffer to be placed onto the object's dirty list
             * when ubc_lookup() was called, the thread that 
             * invalidates the page is actually responsible for
             * moving it back to the clean list.  This happens
             * in bs_unpinpg() when the page is staged for I/O.
             */
            ubc_page_release(pp, ubc_flags);
            *bfPageRefH = NULL;
            *bfPageAddr = NULL;
            return sts;
         } else if ( flag & PINPG_DIO_REMOVE ) {
             bp->ln = SET_LINE_AND_THREAD(__LINE__);
             bp->lock.state |= DIO_REMOVING;
         }
    }

    /*
     * Increment the writeRef so a racer cannot unpin it and mark 
     * it busy by the time we wake up from buffer busy below.
     */
    bp->writeRef++;

    /* For migrate callers only, wait for FLUSH state to clear before
     * setting flag. Setting the FLUSH flag allows migrate to setup
     * a page remap or copy while temporarily blocking other routines
     * wanting to flush the page. Migrate will explicitly clear this
     * state prior to unpinning page. However, this pinpg must clear
     * the FLUSH flag state and wakeup waiters when handling an error.
     */
    if (flag & PINPG_SET_FLUSH) {
        set_state( bp, __LINE__, FLUSH);
    }

    /* Set the REMOVE_FROM_IOQ bit so that any IO descriptors that are
     * on the lazy queues now, will not be moved onto the device queue.
     * This prevents having to wait for IO to complete for this buffer
     * below. Any ioDesc structs already on the blocking queue will get
     * moved to the device queue and have IO done anyway.
     */
    bp->lock.state |= REMOVE_FROM_IOQ;
 
    /* Wait for any pending I/O to complete. */
    mutex_unlock(&bp->bufLock);
    ubc_page_wait(pp);
    mutex_lock(&bp->bufLock);

    /* Wait for any transition of bsBuf. */
    if (bp->lock.state & IO_TRANS) {
        bfap->dmnP->bcStat.pinHitWait++;
        wait = 0;
        while (bp->lock.state & IO_TRANS) {
            state_block( bp, &wait );
        }
    }

    /*
     * A bs_unpinpg(NOMOD,MOD_LAZY) thread may have snuck in after we
     * called ubc_lookup(B_WRITE) but before we took the bufLock.  That
     * thread may have moved the bsBuf from the object's dirty list to
     * the object's clean list. This occurs if the NOMOD unpin
     * perceived that the page was clean. The MOD_LAZY unpin case 
     * sees the writeRef count go to zero after decrementing it and
     * determines that a flush sync is waiting, so the unpin issues
     * IO on the page.
     * But we are pinning this page to write it and so it must be
     * on the object's dirty list.  Move the page to the dirty list
     * if necessary.
     */
    advfs_page_dirty(pp);

    /* Remove buffer from IO queue if necessary. */
    if ( bp->accFwd)  {
        bp->lock.state |= IO_TRANS;
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        mutex_unlock( &bp->bufLock );
        rm_ioq( bp );
        mutex_lock( &bp->bufLock );
        bp->lock.state &= ~REMOVE_FROM_IOQ;
        clear_state( bp, IO_TRANS );
    }
    else {
        bp->lock.state &= ~REMOVE_FROM_IOQ;
    }

    /* its possible a frag or hole was read via getpage, and the page is
     * in UBC without associated storage, and then a filesystem write is done
     * which allocates the missing storage and calls us - we still need to do
     * the blkMap for this case
     */
    if (bp->lock.state & UNMAPPED) {
        mutex_unlock( &bp->bufLock );
        sts = blkMap( bp, bfap );
        if (sts != EOK) {
            mutex_lock( &bp->bufLock );
            bp->writeRef--;
            if (flag & PINPG_SET_FLUSH) {
                clear_state( bp, FLUSH);
            }
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold counter) reference. */
            ubc_page_release( pp, ubc_flags);
            *bfPageRefH = NULL;
            *bfPageAddr = NULL;
            return(sts);
        } 
        mutex_lock( &bp->bufLock );
        bp->lock.state &= ~UNMAPPED;
        bp->result = EOK;
    }

    /* On I/O error, return the status. */
    if ( (sts = bp->result) != EOK && !( sts >= MSFS_FIRST_ERR) ) {
        uint32T clear_flags = 0;

        bp->writeRef--;
        if (flag & PINPG_SET_FLUSH) {
            clear_flags |= FLUSH;
        }
        if ( flag & PINPG_DIO_REMOVE ) {
            /* we set this above; clear it now and wake any waiters */
            clear_flags |= DIO_REMOVING;
        }
        if ( clear_flags ) {
            clear_state( bp, clear_flags );
        }
        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(pp, ubc_flags);
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return( sts );
    }

    /*
     * Special LOG handling: 
     *   In order to properly handle the LSNs of the log we need to
     *   place pinned pages on the dirtyBuf list. This is only true
     *   for the log and if a dirty page is repinned on other files.
     *   In the future this should be changed for consistency.
     */
    if(bfap->dmnP->logAccessp == bfap)
    {
        mutex_lock( &bfap->bfIoLock );
        ADD_DIRTYACCESSLIST( bp, FALSE );  /* put at end of dirty list */
        bp->flushSeq = nilLSN;
        mutex_unlock( &bfap->bfIoLock );
    }

    mutex_unlock( &bp->bufLock );

    /* Check to do read ahead. */
    if (DoReadAhead && refHint == BS_SEQ_AHEAD && bsPage) {
        seq_ahead_cont(bfap, bp, bsPage, metaCheck,
                       vmp, offset, len, ubc_flags);
    }

    /* UBC page (pg_hold counter) reference remains until bs_unpinpg. */
    *bfPageRefH = (bfPageRefHT)bp;
    *bfPageAddr = (void *)ubc_load(bp->vmpage, 0, 0);
    return( sts );
}

/* Reference a page for *writing* given a bitfile handle and the 
 * page number in the bitfile. Called from bs_pinpg_one_int() when 
 * the desired page is not in UBC and had to be allocated,
 * Acquires and releases the bufLock internally.
 *
 * A successful return keeps the UBC page's pg_hold counter reference
 * up until bs_derefpg.
 */

static
statusT
bs_pinpg_newpage(
                 bfPageRefHT *bfPageRefH,       /* out */
                 void **bfPageAddr,             /* out */
                 struct bfAccess *bfap,         /* in */
                 unsigned long bsPage,          /* in */
                 bfPageRefHintT refHint,        /* in */
                 vm_page_t pp,                  /* in */
                 int flag,
                 short metaCheck,               /* in - metadata check type */
                 vm_policy_t vmp,               /* in - vm page locality */
                 vm_offset_t offset,            /* in - future use */
                 vm_size_t len,                 /* in - future use */
                 int ubc_flags                  /* in - ubc control */
                )
{
    struct bsBuf *bp = NULL;
    statusT sts = EOK;
    int s, listLen = 0;
    ioDescT *ioListp;

    /* Initialize a new buffer descriptor. 
     * Other threads are blocked on the vm_page's pg_busy flag 
     * while this thread initializes the page.
     */
    bp = bs_get_bsbuf(PAGE_TO_MID(pp), TRUE);

    bp->ln = SET_LINE_AND_THREAD(__LINE__);
    /* Set the FLUSH flag at migrate routine's request. Migrate
     * will explicitly clear this flag state prior to unpinning page.
     * If we error out of this code, we must be sure to clear the
     * FLUSH flag if we set it.
     */
    bp->lock.state |= ((flag & PINPG_SET_FLUSH) ?
                          (IO_TRANS | BUSY | READING | FLUSH) :
                          (IO_TRANS | BUSY | READING));
    bp->bfPgNum = bsPage;
    bp->bfAccess = bfap;
    bp->writeRef = 1;
    bp->ioCount = 1;
    bp->ioDesc.numBlks = ADVFS_PGSZ_IN_BLKS;
    bp->vmpage = pp;
    bp->ubc_flags = ubc_flags;
    bp->metaCheck = metaCheck;

    pp->pg_opfs = (unsigned long) bp;

    /* Map bf page to vd block.  We need to do this before we put
     * any pages on the dirtybuflist since the iolist is not
     * initialized in the bsbuf until the blkmap.
     */
    sts = blkMap( bp, bfap );

    /*
     * Special LOG handling: 
     *   In order to properly handle the LSNs of the log we need to
     *   place pinned pages on the dirtyBuf list. This is only true
     *   for the log and if a dirty page is repinned on other files.
     *   In the future this should be changed for consistency.
     */

    if (bfap->dmnP->logAccessp == bfap)
    {
        mutex_lock( &bp->bufLock );
        mutex_lock( &bfap->bfIoLock );
        ADD_DIRTYACCESSLIST( bp, FALSE );  /* put at end of dirty list */
        bp->flushSeq = nilLSN;
        mutex_unlock( &bfap->bfIoLock );
        mutex_unlock( &bp->bufLock );
    }

    if ( sts != EOK ) {
        /* Mark buffer as UNMAPPED and zero the page since the 
         * page is not mapped to storage.
         */
        ubc_page_zero(pp, 0, ADVFS_PGSZ);
        mutex_lock( &bp->bufLock );
        bp->lock.state |= UNMAPPED;
        bp->lock.state &= ~FLUSH;
        bp->writeRef--;
        bp->ioCount = 0;
        bp->result = (sts == E_PAGE_NOT_MAPPED) ? EOK : sts;
        /* have bs_io_complete() drop UBC pg_hold reference */
        bp->ubc_flags |= B_BUSY;
        bs_io_complete( bp, &s );
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return(sts);
    }

    if (refHint == BS_OVERWRITE) {
        /* Call I/O complete to wake up any waiters on this buffer.
         * This must be executed for any condition that will not attempt
         * I/O, otherwise the BUSY condition would not be cleared and
         * waiters awoken.
         */
        mutex_lock(&bp->bufLock);
        bp->result = EOK;
        bs_io_complete( bp, &s );

        /* TO DO: if a page is not read in yet, a racing read would
         * think they had a good page - fix this by having racing
         * readers wait on new overwriting flag on buffer
         * that is set here, and cleared in bs_unpinpg() (they
         * would wait till after the uiomove() was done). This is
         * problematic only for the overwriting case. 
         */

        /* UBC page (pg_hold counter) reference remains until bs_unpinpg. */
        *bfPageRefH = (bfPageRefHT)bp;
        *bfPageAddr = (void *)ubc_load(bp->vmpage, 0, 0);
        return( bp->result);
    }

    /* If reading ahead, attempt to create a read ahead list of buffers. */
    if ( DoReadAhead && refHint == BS_SEQ_AHEAD ) {
        seq_ahead_start(bfap, bp, &listLen, &ioListp, bsPage, metaCheck,
                        vmp, offset, len, ubc_flags);
    }

    /* Perform the I/O;  ioList is protected by IO_TRANS state. */
    {
        ioDescT *ioDescp = &bp->ioList.ioDesc[bp->ioList.read];

        listLen++;
        if ( listLen > 1 ) {
            /* Add local ioDesc into the list created by seq_ahead. */
            ioListp->bwd->fwd = ioDescp;
            ioDescp->bwd = ioListp->bwd;
            ioDescp->fwd = ioListp;
            ioListp->bwd = ioDescp;
            ioListp = ioDescp;
            bs_q_list( ioListp, listLen, bs_q_blocking );
        } else if ( listLen == 1 ) {
            ioDescp->fwd = ioDescp->bwd = ioDescp;
            bs_q_blocking( ioDescp, 1 );
        } 

        /* Wait for the I/O to complete. */
        ubc_page_wait(pp);
    }

    /* If there is an IO error, drop the writeRef count and release the page.*/
    if ( bp->result != EOK ) {
        mutex_lock(&bp->bufLock);
        bp->writeRef--;
        /* We need to call clear_state here to wake up any waiters */
        if (flag & PINPG_SET_FLUSH) {
            clear_state( bp, FLUSH);
        }
        mutex_unlock(&bp->bufLock);
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(pp, ubc_flags);
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return( bp->result );
    }

    /* UBC page (pg_hold counter) reference remains until bs_unpinpg. */
    *bfPageRefH = (bfPageRefHT)bp;
    *bfPageAddr = (void *)ubc_load(bp->vmpage, 0, 0);
    return( bp->result);
}

/*
 * bs_pinpg_direct()
 *
 * Description:
 *
 * Transfer data from the user's buffer to disk.  If non-AIO this
 * function will wait for the I/O to complete.  This function will
 * accept a request of any size, although the transfer must start
 * and end on a disk block boundary.
 *
 * This function will check the buffer cache.  All pages which will
 * be completely overwritten will be invalidated and any pages which
 * will be partially overwritten will be flushed to disk.
 *
 * A bsBuf will be allocated via ms_malloc() instead of using the pool
 * of buffers.  This buffer will be released when the transfer has
 * completed.
 *
 * If the buffer is in userspace it will be wired down (via
 * vm_map_pageable()) to prevent it from being swapped out.  Note that
 * this will not be the case for AIO since the user's buffer will
 * already have been mapped down by aio_rw().
 *
 * Blkmap_direct() will be called to determine the disk logical
 * block mapping.
 *
 * The FS_FILE_WRITE_LOCK() must be taken before calling this function.
 * This will guarantee that the writeRef will be zero, which
 * otherwise could cause bs_invalidate_pages() to panic.
 *
 * Parameters:
 *
 *    void *addr                       Pointer to the user's buffer (in)
 *    int number_to_write              Number of bytes to transfer (in)
 *    struct bfAccess *bfap            Access struct pointer (in)
 *    unsigned long bsBlock            File's starting block number (in)
 *    int seg_flag                     Type of data, UIO_USERSPACE or
 *                                     UIO_SYSSPACE (in)
 *    struct buf *aio_bp               Bp for AIO request, NULL
 *                                     otherwise (in)
 *    struct actRange *arp             The range of blocks in a file that are
 *                                     in use.  This will not be NULL for any
 *                                     callers except for a non-direct IO usage
 *                                     by bs_zero_fill_pages
 *    int *number_written              Number of bytes actually setup
 *                                     (there could still be an I/O error)
 *    int *aio_flag                    Set on return.  0 if no outstanding
 *                                     I/O, 1 otherwise (out)
 *    int cowingDone                   TRUE if we want to have the page range
 *                                     verified for correct cowing.
 *    struct ucred *cred               User's creditials.  Used by
 *                                     blkmap_direct().
 */

statusT
bs_pinpg_direct(void *addr,
                int number_to_write,           /* in */
                struct bfAccess *bfap,         /* in */
                unsigned long bsBlock,         /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_written,           /* out */
                int *aio_flag,                 /* out */
                int cowingDone,                /* in */
                struct ucred *cred)            /* in */
{
    struct bsBuf *bp;
    statusT sts = EOK;
    unsigned long i;
    int wait = 0;
    bfSetT *cloneSetp = NULL;
    vm_map_t mymap = (vm_map_t)NULL;
    unsigned long firstPage;
    unsigned long lastPage;
    unsigned long npages;
    int nbytes;
    ioDescT *desc_flink, *desc_blink;
    int list_cnt;
    vm_offset_t start;
    vm_offset_t end;
    fsFragStateT frag_state_before;

    bfPageRefHT bfPageRefH;
    char *bfPageAddr;
    struct processor *myprocessor;        /* used by mark_bio_wait */

    int flush_it;
    int total_blocks = howmany(number_to_write, BS_BLKSIZE);
    int blks_to_end_of_first_page = ADVFS_PGSZ_IN_BLKS -
                                    (bsBlock % ADVFS_PGSZ_IN_BLKS);
    int blks_in_first_page = min(total_blocks, blks_to_end_of_first_page);
    int blks_in_last_page = (total_blocks - blks_in_first_page)
                             % ADVFS_PGSZ_IN_BLKS;

    /*
     * Initialize number_written
     */
    *number_written = 0;

    /*
     * Determine the first and last pages being writtern.
     */
    firstPage = bsBlock / ADVFS_PGSZ_IN_BLKS;
    lastPage  = (bsBlock + total_blocks - 1) / ADVFS_PGSZ_IN_BLKS;
    if( TrFlags & trPin )
        bfr_trace( bfap->tag, firstPage, Pin, 0 );

    /* How many pages (including partial pages) are being written? */

    npages = lastPage - firstPage + 1;

    /* Do not do any of this if we are coming from fs_zero_fill_pages.
     * We are coming from there if arp = NULL.
     */
    if ( arp != NULL) {

        /* Check our calculated range is within the actRanges currently held */
        MS_SMP_ASSERT( bsBlock >= arp->arStartBlock &&
                       bsBlock <= arp->arEndBlock);
        MS_SMP_ASSERT( (bsBlock + total_blocks - 1) >= arp->arStartBlock &&
                       (bsBlock + total_blocks - 1) <= arp->arEndBlock);


        /*
         * Make sure that none of the pages we are writing are
         * in the UBC.
         */
        for (i = firstPage; i < (firstPage + npages); i++) {

            /*
             * Call bs_pinpg_one_int() to find and gain access to 
             * the page if it's in the UBC.  The flag PINPG_DIRECTIO 
             * signals bs_pinpg_one_int() to NOT drag this page into 
             * the cache if it is not already in the UBC. The 
             * PINPG_DIO_REMOVE flag indicates that this thread will 
             * remove this page from the cache if it is returned.  
             * This is used to synchronize racing threads so that they 
             * will wait for this page to be removed from the cache 
             * before proceeding.  The actRange structs no longer 
             * protect this race since several active ranges can now 
             * span an underlying 8k page.
             * Also, pass B_READ rather than B_WRITE so ubc_lookup()
             * won't mark the page dirty.  B_CACHE causes ubc_lookup()
             * to return the page only if it is already in the cache
             * (no new page is created).
             */
            sts = bs_pinpg_one_int(&bfPageRefH, (void **)&bfPageAddr, bfap,
                               i, BS_NIL, (PINPG_DIRECTIO | PINPG_DIO_REMOVE),
                               BSBUF_CHK_NONE, NULL, 0, 0, (B_READ | B_CACHE));
            if ( sts == E_PAGE_NOT_MAPPED ) {
                /* Just invalidate this page. It is in the cache but
                 * it is not mapped, likely because it has been ref'd but
                 * no storage has been associated with it yet.  So
                 * expunge its very existence.
                 */
                bs_invalidate_pages(bfap, (uint32T)i, 1, 0);
                sts = EOK;
                continue;
            } else if ( sts == EIO ) {
                /* A page in the cache cannot be flushed to disk because of
                 * a real I/O error, so we need to get rid of it and report
                 * the error. 
                 */
                bs_invalidate_pages(bfap, (uint32T)i, 1, 0);
                break;
            } else if (sts != EOK)
                break;
            else if (bfPageAddr == (char *)NULL) {
                /* The page is not in the UBC */
                continue;
            }

            /*
             * We found a page in the cache.  We need to flush it
             * to disk if we are doing a partial overwrite.
             */
            flush_it = 0;

            /*
             * Is it the first page and are we only going to partially
             * overwrite it?
             */
            if (i == firstPage && blks_in_first_page != ADVFS_PGSZ_IN_BLKS) {

                nbytes = blks_in_first_page * BS_BLKSIZE;
                flush_it = 1;

                /*
                 * Copy user's data into cache before the flush.
                 */
                if (seg_flag == UIO_USERSPACE) {
                    if (copyin(addr, bfPageAddr +
                               ((bsBlock % ADVFS_PGSZ_IN_BLKS) * BS_BLKSIZE),
                               nbytes)) {
                        bs_unpinpg(bfPageRefH, logNilRecord,
                                 (((struct bsBuf *)bfPageRefH)->lock.state & DIRTY) ? 
                                   UNPIN_DIO_DIRTY : UNPIN_DIO_CLEAN);
                        bs_invalidate_pages(bfap, (uint32T)i, 1, 0);
                        sts = EFAULT;
                        break;
                    }
                }
                else {
                    bcopy(addr, bfPageAddr +
                          ((bsBlock % ADVFS_PGSZ_IN_BLKS) * BS_BLKSIZE),
                          nbytes);
                }
                addr = (char *)addr + nbytes;
                *number_written += nbytes;
                bsBlock += blks_in_first_page;
            }

            /*
             * Is it the last page and are we only going to partially
             * overwrite it?
             */
            else if ((i == (firstPage + npages - 1)) &&
                     (blks_in_last_page != ADVFS_PGSZ_IN_BLKS)) {

                nbytes = blks_in_last_page * BS_BLKSIZE;
                flush_it = 1;

                /*
                 * Copy user's data into cache before the flush.
                 */
                if (seg_flag == UIO_USERSPACE) {
                    if (copyin((char *)addr + number_to_write - *number_written
                               - nbytes, bfPageAddr, (int)nbytes)) {
                        bs_unpinpg(bfPageRefH, logNilRecord,
                                 (((struct bsBuf *)bfPageRefH)->lock.state & DIRTY) ? 
                                  UNPIN_DIO_DIRTY : UNPIN_DIO_CLEAN);
                        bs_invalidate_pages(bfap, (uint32T)i, 1, 0);
                        sts = EFAULT;
                        break;
                    }
                }
                else {
                    bcopy((char *)addr + number_to_write -
                          *number_written - nbytes, bfPageAddr, (int)nbytes);
                }
                *number_written += nbytes;
            }

            /* Unpin and invalidate the page.  Any threads waiting on 
             * DIO_REMOVING in bs_pinpg_found() will be awakened:
             * 1) in the unpin code if the page is clean, or 2) upon
             * IO completion if the page is being flushed. In the 
             * latter case, the now-clean page will be in the ubc briefly 
             * after the IO has completed but before bs_invalidate_pages()
             * is called.  New pinners of this page may race with this
             * thread to invalidate the page in bs_pinpg_one_int(), but
             * this race is atypical and benign (the first thread will 
             * invalidate the page and the others will merely find that 
             * there is no longer a page to invalidate).
             */
            sts = bs_unpinpg(bfPageRefH,
                             logNilRecord,
                             ((((struct bsBuf *)bfPageRefH)->lock.state & DIRTY) || flush_it) ? 
                             UNPIN_DIO_DIRTY : UNPIN_DIO_CLEAN);
            bs_invalidate_pages(bfap, (uint32T)i, 1, 0);

            /*
             * Get out if any problems.
             */
            if (sts != EOK)
                break;
        }

        /*
         * Were there any problems or did we transfer all the data?
         */
        if ((sts != EOK) || (number_to_write == *number_written)) {
            return(sts);
        }
    }

    /*
     * Allocate a buffer.
     */
    bp = bs_get_bsbuf(CURRENT_MID(), FALSE);
    if (bp == NULL) {
        /* Out of memory! */
        return(ENO_MORE_MEMORY);
    }

    /*
     * Fill in fields of new buffer descriptor
     */
    bp->lock.state = IO_TRANS | BUSY | WRITING | RAWRW;
    bp->ln = SET_LINE_AND_THREAD(__LINE__);
    bp->bfPgNum = bsBlock / ADVFS_PGSZ_IN_BLKS;
    bp->writeRef = 1;
    bp->bfAccess = bfap;
    bp->directIO = 1;
    bp->procp = u.u_procp;
    bp->aio_bp = aio_bp;
    bp->bufDebug = BSBUF_DIRECTIO;

    frag_state_before = bfap->fragState;

    /*
     * Map bf page to vd block.  Note that blkmap_direct() may
     * not be able to map the entire request.
     */
    sts = blkmap_direct(bp, bfap, bsBlock,
                        number_to_write - *number_written, cred);
    if (sts != EOK) {
        lk_destroy( &bp->lock );
        mutex_destroy( &bp->bufLock );
        ms_free(bp);
        return(sts);
    }

    /* If the blkmap_direct() caused the frag page to be copied
     * in for the file, then we must get rid of that page from the
     * buffer cache since we are bypassing it on the write.
     */
    if (frag_state_before == FS_FRAG_VALID &&
        bfap->fragState   == FS_FRAG_NONE) {
        bs_invalidate_pages(bfap, bp->bfPgNum, 1, 0);
    }

    /*
     * If the buffer is in userspace, wire it down so it can't be
     * swapped out.  Please note that this will never be the
     * case for AIO since the aio_rw() routine has already
     * wired the users buffer.  We don't expect an AIO
     * call to come here with a seg_flag of UIO_USERSPACE, so
     * check for this in testing, but handle it in the field.
     */
    MS_SMP_ASSERT( seg_flag == UIO_USERSPACE ? !aio_bp : 1 );
    if ( seg_flag == UIO_USERSPACE && !aio_bp ) {

        mymap = current_task()->map;
        start = (vm_offset_t)trunc_page((vm_offset_t)addr);
        end = (vm_offset_t)round_page((vm_offset_t)addr +
                                      number_to_write - *number_written);

        /* Wire the user's data buffer.  */
        if (vm_map_pageable(mymap, start, end, VM_PROT_READ) != KERN_SUCCESS) {
            bs_free_bsbuf(bp);
            return(EFAULT);
        }
    }

    nbytes = 0;
    desc_flink = desc_blink = (ioDescT *)NULL;
    list_cnt = 0;

    for (i=0; i < bp->ioList.maxCnt; i++) {
      /*
       * Add the descriptor to our list and set it up.
       */
      if (desc_flink == NULL) {
        bp->ioList.ioDesc[i].bwd = &bp->ioList.ioDesc[i];
        bp->ioList.ioDesc[i].fwd = &bp->ioList.ioDesc[i];
        desc_flink = desc_blink = &bp->ioList.ioDesc[i];
      } else {
        bp->ioList.ioDesc[i].bwd = desc_blink;
        desc_blink->fwd = &bp->ioList.ioDesc[i];
        desc_blink = &bp->ioList.ioDesc[i];
      }

      /* We don't pass vmpage, we pass the user's mapped address */
      bp->ioList.ioDesc[i].targetAddr = (unsigned char *)addr + nbytes;
      list_cnt++;

      nbytes += bp->ioList.ioDesc[i].numBlks;
    }

    /*
     * Update number_written
     */
    *number_written += nbytes;

    /*
     * Is there anything to write?
     */
    if (list_cnt) {

        /*
         * Finish linking the list.
         */
        desc_flink->bwd = desc_blink;
        desc_blink->fwd = desc_flink;

        /*
         * Set the bp's list count (ioCount).
         */
        bp->ioCount = list_cnt;

        /*
         * If this is an aio request, update the aio_bp's resid
         * field.  This must be done here since the I/O could complete
         * and perform the callback before we get back to fs_write_direct().
         * It's possible that there was an unaligned block after this request
         * which was already written.  Its count has been stored in b_resid.
         * So if this write and the previous unaligned write were both
         * successful, set b_resid to 0 so the AIO driver knows that all
         * bytes have been transferred.  If either was unsuccessful, we
         * need to report the value that were not successfully written.
         * One tricky thing is that if the current write is not successful,
         * we must also report that the unaligned write was not successful,
         * so these values are added.  The field 
         * aio_bp->b_driver_un_1.longvalue is overloaded for containing the
         * success (0) or failure (1) of the unaligned trailing write.  This
         * is confusing, but it is one field in the struct buf that I know 
         * is safe for us to use.
         * If longvalue == 3 we are only here to overwrite sparse storage
         * on a synchronous write that failed and b_resid is already set.
         */
        if (aio_bp && aio_bp->b_driver_un_1.longvalue != 3) {
            int resid = number_to_write - *number_written;
            if ( !resid && !aio_bp->b_driver_un_1.longvalue ) {
                /* This write and any previous trailing unaligned write
                 * were both successful: no b_resid to report.
                 */
                aio_bp->b_resid = 0;
            } else {
                /* There is some b_resid value to preserve */
                aio_bp->b_resid += resid;
            }
        }

        /* Set a pointer to the actRange in the buffer, and increment the
         * # of IosOutstanding for this active Range before enqueuing this
         * ioDescriptor. Seizing of the actRangeLock is only necessary
         * because arIosOutstanding will be decremented in bs_osf_complete(),
         * and we never know what thread will be doing that.  There can be
         * many buffers per active range.
         */
        if ( arp != NULL ) {
            bp->actRangep = arp;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
            mutex_lock( &bp->bfAccess->actRangeLock );
            arp->arIosOutstanding++;
            mutex_unlock( &bp->bfAccess->actRangeLock );
        }

        /*
         * Queue the I/O
         */
        bs_q_list(desc_flink, list_cnt, bs_q_blocking);

        /*
         * If this is an Async I/O then don't wait for the
         * completion.  Bs_io_complete() will perform the call back
         * to the user's iodone routine and free the bp.
         */
        if (aio_bp) {
            MS_SMP_ASSERT( !mymap );  /* We should NOT have wired a page. */
            *aio_flag = 1;
            return(sts);
        }

        /* Wait for the I/O to complete. */
        mutex_lock(&bp->bufLock);
        wait = 0;
        mark_bio_wait;
        while (bp->lock.state & (BUSY | IO_TRANS)) {
            state_block(bp, &wait);
        }
        unmark_bio_wait;
        mutex_unlock(&bp->bufLock);
        sts = bp->result;       /* pass I/O return code to caller */
    }

    /* Unwire the user's buffer.  */
    if (mymap != (vm_map_t)NULL) {
        (void)vm_map_pageable(mymap, start, end, VM_PROT_NONE);
    }

    /* Free the buffer. */
    bs_free_bsbuf(bp);
    return( sts );
}

/*
 * bs_unpinpg
 *
 * Given a page handle and the type of unpin,
 * ( clean, dirty, or immediate ) unpin the page.
 */

statusT
bs_unpinpg(
           bfPageRefHT bfPageRefH,      /* in */
           logRecAddrT wrtAhdLogAddr,   /* in - LOG case only */
           bsUnpinModeT modeNcache      /* in */
           )
{
    struct bsBuf *bp;
    statusT sts;
    bfPgRlsModeT pgRlsMode = modeNcache.rlsMode;
    bfPageCacheHintT cacheHint = modeNcache.cacheHint;
    struct bfAccess *logbfap;
    lsnT lsn;
    void (* q_fn)(ioDescT *, int) = bs_q_lazy;
    u_long pageNum;
    extern REPLICATED int SS_is_running;
    struct timeval new_time;
    int wday;
    uint32T totHotCnt;

    /* Convert the buffer handle. */
    bp = (struct bsBuf *)bfPageRefH;

    if( TrFlags & trPin )
        bfr_trace( bp->bfAccess->tag, bp->bfPgNum, Unpin, (int)pgRlsMode );

    if( bp->writeRef == 0 ) {

        /*
         * Do not remove this panic, as it can prevent data corruption
         * if bp->writeRef really is 0.
         */

        ADVFS_SAD0("bs_unpinpg: called with buffer not pinned");
    }

    mutex_lock( &bp->bufLock );

    if (SS_is_running &&
        bp->bfAccess->dmnP->ssDmnInfo.ssDmnSmartPlace &&
        (bp->bfAccess->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
         bp->bfAccess->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

        /* send a page write message to smartstore if enough have occurred */
        TIME_READ(new_time);
        GETCURRWDAY(new_time.tv_sec,wday);
        atomic_incl(&bp->bfAccess->ssHotCnt[wday]);
        for(totHotCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
            totHotCnt += bp->bfAccess->ssHotCnt[wday];
        }
        if(totHotCnt >
           bp->bfAccess->dmnP->ssDmnInfo.ssAccessThreshHits) {
            ss_snd_hot(HOT_ADD_UPDATE_WRITE, bp->bfAccess);
        }
    }

    /* Wait for IO_TRANS state to clear, mainly for migrate. */
    set_state( bp, __LINE__, IO_TRANS );

    if ((bp->bfAccess->dataSafety == BFD_FTX_AGENT) ||
        (bp->bfAccess->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {
        bp->bfAccess->dmnP->bcStat.unpinFileType.ftx++;
    }

    if ((int)bp->bfAccess->tag.num < 0) {
        bp->bfAccess->dmnP->bcStat.unpinFileType.meta++;
    }

    switch( pgRlsMode ) {

    case BS_MOD_SYNC:
    case BS_MOD_DIRECT:

        bp->bfAccess->dmnP->bcStat.unpinCnt.blocking++;

        MS_SMP_ASSERT(bp->writeRef == 1);

        /*
         * Add modified page to dirtyBufList if it is not listed.
         * Assign a sequence number to the buffer, used in bitfile flush.
         */
        if (!bp->accFwd) {
            mutex_lock( &bp->bfAccess->bfIoLock );
            /* Is bp->accFwd still NULL? */
            if (!bp->accFwd) {
                LSN_INCR( bp->bfAccess->nextFlushSeq );
                bp->flushSeq = bp->bfAccess->nextFlushSeq;
                ADD_DIRTYACCESSLIST( bp, FALSE );
            }
            mutex_unlock( &bp->bfAccess->bfIoLock );
        }

        bp->lock.state |= (DIRTY | WRITING);

        /* Setup UBC page for IO.  We can fail here because an mmapped
         * page is wired by an application so we can not change protection
         * to make the page clean.  In that case just write the page while
         * keeping it dirty.  Any other failure should be impossible so
         * just prevent further damage and crash it in development.
         */ 
        if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
            if (sts == UBC_GET_VM_PROTECT &&
                    bp->bfAccess->bfVp &&
                    (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                sts = advfs_page_get(bp,
                          UBC_GET_BUSY | UBC_GET_HAVEHOLD | UBC_GET_KEEPDIRTY);
                if (!sts) {
                    bp->lock.state |= (BUSY | KEEPDIRTY);
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                    bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                    bp->ioqLn = -1;
#endif
                }
            }

            if (sts) {
                /* release lock to get live dump then retake it */
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(bp->bfAccess->dmnP,
                         "bs_unpinpg: %s advfs_page_get(%p) returned %x",
                         pgRlsMode==BS_MOD_DIRECT ? "DIRECT" : "SYNC",
                         bp, sts);
                mutex_lock(&bp->bufLock);

                bp->writeRef--;
                clear_state( bp, IO_TRANS );
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                mutex_unlock( &bp->bufLock );
                ubc_page_release(bp->vmpage, 
                             pgRlsMode == BS_MOD_DIRECT ? B_READ : B_WRITE);
                /* nothing more caller can do so report success */
                return(0);
            }
        } else {
            bp->lock.state |= BUSY;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
            bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
            bp->ioqLn = -1;
#endif
        }

        /*
         * Check if the buffer has been remapped,
         * in the process of migrating.
         */
        if ( bp->lock.state & REMAP ) {
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
            mutex_unlock( &bp->bufLock );
            sts = blkMap( bp, bp->bfAccess );
            mutex_lock( &bp->bufLock );
            bp->lock.state &= ~REMAP;
        }

        /*
         * Create a circular linked list of the I/O
         * descriptors to hand off to bs_q_list.
         */
        link_write_req( bp );

        /* Since pg_busy and BUSY flag are set, other pinners will wait
         * for these to clear, so it should be safe to release this lock
         * before checking currentLogRec.  Must release it prior
         * to calling call_logflush().
         */
        bp->writeRef--;
        mutex_unlock( &bp->bufLock );

        /*
         * If this is a file under transaction control,
         * check that buffer's log page has been
         * flushed before we write.  If not, flush the log.
         */
        if ( !LSN_EQ_NIL( bp->currentLogRec.lsn ) ) {
            logbfap = bp->bfAccess->dmnP->logAccessp;
            mutex_lock( &logbfap->bfIoLock );

            if ( LSN_GT( bp->currentLogRec.lsn,
                        logbfap->hiFlushLsn ) ) {

                call_logflush( bp->bfAccess->dmnP,
                               bp->currentLogRec.lsn,
                               TRUE );
            }
            MS_SMP_ASSERT(LSN_GTE(logbfap->hiFlushLsn,
                                  bp->currentLogRec.lsn));
            mutex_unlock( &logbfap->bfIoLock );
        }

        bs_q_list( &bp->ioList.ioDesc[bp->ioList.write],
                   bp->ioList.writeCnt,
                   bs_q_blocking );

        /* Wait for the I/O to complete. */
        ubc_page_wait(bp->vmpage);

        sts = bp->result; /* on release bsBuf may disappear */
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(bp->vmpage, 
                         pgRlsMode == BS_MOD_DIRECT ? B_READ : B_WRITE);

        if (sts != EOK) {
            return( sts );
        }

        break;

    case BS_MOD_COPY:
    case BS_MOD_LAZY:

        /* bp->bufLock is locked; IO_TRANS is set */
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        bp->bfAccess->dmnP->bcStat.unpinCnt.lazy++;

        /*
         * Check if the buffer has been remapped,
         * in the process of migrating.
         */
        if( pgRlsMode == BS_MOD_LAZY && bp->lock.state & REMAP ) {
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
            mutex_unlock( &bp->bufLock );
            blkMap( bp, bp->bfAccess );
            mutex_lock( &bp->bufLock );
            bp->lock.state &= ~REMAP;
        }

        MS_SMP_ASSERT(bp->writeRef > 0);
        MS_SMP_ASSERT(!(bp->vmpage->pg_busy));
        MS_SMP_ASSERT(bp->vmpage->pg_dirty);

        bp->lock.state |= DIRTY;

        /*
         * Insert page onto dirtyBufList and assign a sequence number 
         * if necessary.  Bitfile flush uses the sequence number.
         */
        if (!bp->accFwd) {
            mutex_lock( &bp->bfAccess->bfIoLock );
            /* Is bp->accFwd still NULL? */
            if (!bp->accFwd) {
                LSN_INCR( bp->bfAccess->nextFlushSeq );
                bp->flushSeq = bp->bfAccess->nextFlushSeq;
                ADD_DIRTYACCESSLIST( bp, FALSE );
            }
            mutex_unlock( &bp->bfAccess->bfIoLock );
        }

        /*
         * Last unpin enqueues ioDesc
         */
        if( --bp->writeRef == 0 ) {

            bp->lock.state |= WRITING;

            link_write_req( bp );

            /*
             * If someone is waiting on the page put it on blocking queue.
             * For migrate unpins, queue to the flush queue for immediate
             * action and set ubc_flags so bs_io_complete will make it go
             * to the head of the ubc clean list to minimize ubc usage. 
             */
             
            if (bp->rflList || SEQ_LTE(bp->flushSeq, bp->bfAccess->hiWaitLsn))
                q_fn = bs_q_blocking;
            else if (pgRlsMode == BS_MOD_LAZY)
                q_fn = bs_q_lazy;
            else {
                mutex_lock(&bp->bfAccess->bfIoLock);
                bp->bfAccess->migPagesPending++;
                mutex_unlock(&bp->bfAccess->bfIoLock);
                bp->lock.state |= THROTTLE;
                q_fn = bs_q_flushq;
                bp->ubc_flags = B_AGE;
            }

            if (q_fn == bs_q_blocking || q_fn == bs_q_flushq) {

                /*
                 * If this is a file under transaction control,
                 * check that buffer's log page has been
                 * flushed before we write.  If not, flush the log.
                 */
                if ( !LSN_EQ_NIL( bp->currentLogRec.lsn ) ) {
                    mutex_unlock( &bp->bufLock );
                    logbfap = bp->bfAccess->dmnP->logAccessp;
                    mutex_lock( &logbfap->bfIoLock );
    
                    if ( LSN_GT( bp->currentLogRec.lsn,
                                logbfap->hiFlushLsn ) ) {
    
                        call_logflush( bp->bfAccess->dmnP,
                                       bp->currentLogRec.lsn,
                                       TRUE );
                    }
                    MS_SMP_ASSERT(LSN_GTE(logbfap->hiFlushLsn,
                                          bp->currentLogRec.lsn));
                    mutex_unlock( &logbfap->bfIoLock );
                    mutex_lock( &bp->bufLock );
                }

                /* Setup UBC page for IO.  We can fail here because an
                 * mmapped page is wired by an application so we can not
                 * change protection to make the page clean.  In that case
                 * just write the page while keeping it dirty.  Any other
                 * failure should be impossible so just prevent further
                 * damage and crash it in development.
                 */ 
                if ((sts = advfs_page_get(bp,
                               UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                    if (sts == UBC_GET_VM_PROTECT &&
                            bp->bfAccess->bfVp &&
                            (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                        sts = advfs_page_get(bp,
                                  UBC_GET_BUSY | UBC_GET_HAVEHOLD |
                                  UBC_GET_KEEPDIRTY);
                        if (!sts) {
                            bp->lock.state |= (BUSY | KEEPDIRTY);
                            bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                            bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                            bp->ioqLn = -1;
#endif
                        }
                    }

                    if (sts) {
                        /* release lock to get live dump then retake it */
                        mutex_unlock(&bp->bufLock);
                        ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                        domain_panic(bp->bfAccess->dmnP,
                            "bs_unpinpg: %s advfs_page_get(%p) returned %x",
                            pgRlsMode==BS_MOD_COPY ? "COPY" : "LAZY", bp, sts);
                        mutex_lock(&bp->bufLock);

                        bp->writeRef--;
                        clear_state( bp, IO_TRANS );
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
                        mutex_unlock( &bp->bufLock );
                        ubc_page_release(bp->vmpage, B_WRITE);
                        /* nothing caller can do, already a domain panic */
                        return(0); /* so just say it was ok */
                    }
                } else {
                   bp->lock.state |= BUSY;
                   bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                   bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                   bp->ioqLn = -1;
#endif
                }
            }

            mutex_unlock( &bp->bufLock );
            bs_q_list( &bp->ioList.ioDesc[bp->ioList.write],
                bp->ioList.writeCnt,
                q_fn );
            mutex_lock( &bp->bufLock );
        }

        if( q_fn == bs_q_lazy ) {
            MS_SMP_ASSERT(!(bp->vmpage->pg_busy));
            MS_SMP_ASSERT(bp->vmpage->pg_dirty);
            clear_state( bp, IO_TRANS );
        }

        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(bp->vmpage, B_WRITE);
        break;

    case BS_NOMOD:
    case BS_NOMOD_DIRECT:

        MS_SMP_ASSERT(bp->writeRef > 0);

        /* bp->bufLock is locked; IO_TRANS is set */
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        bp->bfAccess->dmnP->bcStat.unpinCnt.clean++;

        /*
         * If the last unpin is done and the buffer is
         * dirty, enqueue it onto either the lazy or the
         * blocking queue, depending on whether or not
         * some thread is waiting for this.  If it is not
         * dirty, some thread may still be waiting for it.
         */
        if (--bp->writeRef == 0) {
            if (bp->accFwd) {
                bp->lock.state |= WRITING;

                link_write_req( bp );

                q_fn = (SEQ_LTE(bp->flushSeq, bp->bfAccess->hiWaitLsn) ||
                        (bp->rflList) ?
                        bs_q_blocking : bs_q_lazy);

                if (q_fn == bs_q_blocking) {

                    /*
                     * If this is a file under transaction control,
                     * check that buffer's log page has been
                     * flushed before we write.  If not, flush the log.
                     */
                    if ( !LSN_EQ_NIL( bp->currentLogRec.lsn ) ) {
                        mutex_unlock( &bp->bufLock );
                        logbfap = bp->bfAccess->dmnP->logAccessp;
                        mutex_lock( &logbfap->bfIoLock );
        
                        if ( LSN_GT( bp->currentLogRec.lsn,
                                    logbfap->hiFlushLsn ) ) {
        
                            call_logflush( bp->bfAccess->dmnP,
                                           bp->currentLogRec.lsn,
                                           TRUE );
                        }
                        MS_SMP_ASSERT(LSN_GTE(logbfap->hiFlushLsn,
                                              bp->currentLogRec.lsn));
                        mutex_unlock( &logbfap->bfIoLock );
                        mutex_lock( &bp->bufLock );
                    }

                    /* Setup UBC page for IO.  We can fail here because an
                     * mmapped page is wired by an application so we can not
                     * change protection to make the page clean.  In that case
                     * just write the page while keeping it dirty.  Any other
                     * failure should be impossible so just prevent further
                     * damage and crash it in development.
                     */ 
                    if ((sts = advfs_page_get(bp,
                                   UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                        if (sts == UBC_GET_VM_PROTECT &&
                                bp->bfAccess->bfVp &&
                                (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                            sts = advfs_page_get(bp,
                                      UBC_GET_BUSY | UBC_GET_HAVEHOLD |
                                      UBC_GET_KEEPDIRTY);
                            if (!sts) {
                                bp->lock.state |= (BUSY | KEEPDIRTY);
                                bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                                bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                                bp->ioqLn = -1;
#endif
                            }
                        }

                        if (sts) {
                            /* release lock to get live dump then retake it */
                            mutex_unlock(&bp->bufLock);
                            ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                            domain_panic(bp->bfAccess->dmnP,
                                "bs_unpinpg: %s advfs_page_get(%p) returned %x",
                                pgRlsMode==BS_NOMOD_DIRECT ?
                                "DIO_NOMOD" : "NOMOD", bp, sts);
                            mutex_lock(&bp->bufLock);

                            clear_state( bp, IO_TRANS );
                            bp->ln = SET_LINE_AND_THREAD(__LINE__);
                            mutex_unlock( &bp->bufLock );
                            ubc_page_release(bp->vmpage, 
                                pgRlsMode == BS_NOMOD_DIRECT ?
                                B_READ : B_WRITE);
                            /* nothing caller can do, already a domain panic */
                            return(0); /* so just say it was ok */
                        }
                    } else {
                        bp->lock.state |= BUSY;
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                        bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                        bp->ioqLn = -1;
#endif
                    }
                }

                mutex_unlock( &bp->bufLock );
                bs_q_list( &bp->ioList.ioDesc[bp->ioList.write],
                    bp->ioList.writeCnt,
                    q_fn );
                mutex_lock( &bp->bufLock );
            }
            else {
                /* Change UBC page to be clean without doing IO.  We can
                 * fail here because an mmapped page is wired by an
                 * application so we can not change protection to make the
                 * page clean.  In that case ignore the failure.  Any other
                 * failure should be impossible and it may be we could also
                 * ignore it here. But since we are not certain, just
                 * prevent further damage and crash it in development.
                 *
                 * If the DIO_REMOVING flag was set in bs_pinpg_one_int(),
                 * the thread that set it may come through this path if
                 * the page was not modified.  In this case, be sure that
                 * this flag is cleared and any waiters are awakened.  The
                 * waking will occur below in clear_state() for IO_TRANS.
                 */
                if ((sts = advfs_page_get(bp, UBC_GET_MAKECLEAN))) {
                    if (sts == UBC_GET_VM_PROTECT &&
                            bp->bfAccess->bfVp &&
                            (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
                        sts = 0;
                    } else {
                        /* release lock to get live dump then retake it */
                        mutex_unlock(&bp->bufLock);
                        ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                        domain_panic(bp->bfAccess->dmnP,
                        "bs_unpinpg: %s CLEAN advfs_page_get(%p) returned %x",
                            pgRlsMode==BS_NOMOD_DIRECT ? "DIO_NOMOD" : "NOMOD",
                            bp, sts);
                        mutex_lock(&bp->bufLock);

                        bp->lock.state &= ~DIO_REMOVING;
                        clear_state( bp, IO_TRANS );
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
                        mutex_unlock( &bp->bufLock );
                        ubc_page_release(bp->vmpage, 
                            pgRlsMode == BS_NOMOD_DIRECT ? B_READ : B_WRITE);
                        /* nothing caller can do, already a domain panic */
                        return(0); /* so just say it was ok */
                    }
                } else {
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                    bp->lock.state &= ~DIO_REMOVING;
                }
            }
        }

        if( q_fn == bs_q_lazy )
            clear_state( bp, IO_TRANS );

        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(bp->vmpage, 
                         pgRlsMode == BS_NOMOD_DIRECT ? B_READ : B_WRITE);
        break;

    case BS_LOG_PAGE:

        MS_SMP_ASSERT(bp->writeRef > 0);

        /* bp->bufLock is locked; IO_TRANS is set */
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        bp->bfAccess->dmnP->bcStat.unpinCnt.log++;

        if ( cacheHint == BS_LOG_NOMOD) {
            /*
             * Logger doesn't think the page is dirty.  The lsn
             * of this page is the same as it was in the previous unpin
             * so we don't want to mark this page dirty to not mess up
             * flush/flush_sync which don't like to see the same log lsn
             * twice.
             */
           --bp->writeRef;
            MS_SMP_ASSERT(!bp->writeRef);

            /* If the page is not previously modified, then tell UBC
             * to move the page from the UBC object dirty to clean list.
             */
            if( !(bp->lock.state & DIRTY) ) {
                MS_SMP_ASSERT(bp->accFwd);
                if ( bp->lock.state & ACC_DIRTY ) {
                    mutex_lock( &bp->bfAccess->bfIoLock );
                    MS_SMP_ASSERT(bp->accFwd);
                    RM_ACCESSLIST( bp, FALSE );         /* take off dirty */
                    mutex_unlock( &bp->bfAccess->bfIoLock );

                    /* Change UBC page to be clean without doing IO.
                     * Failure should be impossible and it may be we could
                     * ignore a failure here. But since we are not certain,
                     * prevent further damage and crash it in development.
                     */
                    if ((sts = advfs_page_get(bp, UBC_GET_MAKECLEAN))) {
                        /* release lock to get live dump then retake it */
                        mutex_unlock(&bp->bufLock);
                        ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                        domain_panic(bp->bfAccess->dmnP,
                        "bs_unpinpg: LOG CLEAN advfs_page_get(%p) returned %x",
                           bp, sts);
                        mutex_lock(&bp->bufLock);
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
                        /* fall through, clean up, return success */
                    }
                }
            }

            clear_state( bp, IO_TRANS );
            mutex_unlock( &bp->bufLock );

            /* Drop UBC page (pg_hold counter) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            break;
        }

        /*
         * The flushSeq must be set before writeRef is decremented.
         * There is no lock that ties these two together yet cache_logflush 
         * and bs_logflush_start both scan the dirty list depend on this 
         * order. In the other order, bs_logflush_start can find a 
         * different end of the dirtylist than cache_logflush due to 
         * writeRef going to zero, but the flushSeq has not been updated, 
         * so bs_logflush_start panics since flushSeq is too small.
         * Because of above dependency hold bfIoLock while setting flushSeq
         * and decrementing writeRef.  Instruction order of independent
         * operations not guaranteed in alpha.
         */
        
        /*
         * Assumes caller is properly ordering these unpins according to
         * increasing larger LSN values.
         */
        mutex_lock( &bp->bfAccess->bfIoLock );

        /* Give incoming LSN to page only if it is large than the
         * existing LSN. This could happen if at io completion we
         * serviced a page in front of this page on the dirty buf
         * list, in which case we would have put its lsn into this
         * page's flushSeq. 
         */

        if (LSN_EQ_NIL(bp->flushSeq))
        {
            bp->flushSeq = wrtAhdLogAddr.lsn;
        }
        else MS_SMP_ASSERT(LSN_GT(bp->flushSeq, wrtAhdLogAddr.lsn));

        /* Log pages are placed on the dirtybuf list at pin time
         * in order to keep track of lsn's. So there is no need
         * to put it on the list here.
         */
        MS_SMP_ASSERT(bp->accFwd);

        mutex_unlock( &bp->bfAccess->bfIoLock );

        /*
         * This should never happen as the log file should only
         * be moved via switchlog functionality, not via migrate.
         */
        MS_SMP_ASSERT(!(bp->lock.state & REMAP));

        --bp->writeRef;
        MS_SMP_ASSERT(!bp->writeRef);
        logbfap = bp->bfAccess->dmnP->logAccessp;
        lsn = bp->flushSeq;
        pageNum = bp->bfPgNum;

        if( ! (bp->lock.state & DIRTY) ) {
            bp->lock.state |= (DIRTY | WRITING | UPDATE_LSN | LOG_PAGE);
        }

        /* 
         * We get here with a dirty log page. Check if a flush is underway and
         * if we are part of this range. If so then put this page on the blocking 
         * queue now. This allows log flushes to have pinned pages in the range
         * since they will now be flushed at unpin time.
         */
        /* We need to be careful here that the flush lsn may live on this
         * log page. If so we need to peak in the log page on grab the
         * first LSN on the page.
         */

        if ( (SEQ_LTE(bp->flushSeq, bp->bfAccess->logWriteTargetLsn)) ||
             (SEQ_GTE(bp->bfAccess->logWriteTargetLsn,
                      ((logPgT *)ubc_load(bp->vmpage, 0, 0))->hdr.thisPageLSN))
             ) {
            link_write_req( bp );
            MS_SMP_ASSERT(bp->writeRef==0);

            /* Setup UBC page for IO. Assumes only log code can start IO
             * since UBC cannot initiate IO on metadata.  Prevent any
             * further damage and crash it in development.
             */
            if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                /* release lock to get live dump then retake it */
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(bp->bfAccess->dmnP,
                             "bs_unpinpg: LOG advfs_page_get(%p) returned %x",
                             bp, sts);
                mutex_lock(&bp->bufLock);

                clear_state( bp, IO_TRANS );
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                mutex_unlock( &bp->bufLock );
                ubc_page_release(bp->vmpage, B_WRITE);
                /* nothing caller can do, already a domain panic */
                return(0); /* so just say it was ok */
            }

            bp->lock.state |= BUSY;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
            bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
            bp->ioqLn = -1;
#endif
            mutex_unlock( &bp->bufLock );
            bs_q_list( &bp->ioList.ioDesc[bp->ioList.write],
                       bp->ioList.writeCnt, 
                       bs_q_blocking );
            /* Drop UBC page (pg_hold counter) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            break;
        }

        clear_state( bp, IO_TRANS );
        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold counter) reference. */
        ubc_page_release(bp->vmpage, B_WRITE);

        /*
         * Flush the log every n pages
         * TODO: parameterize LOG_FLUSH_THRESHOLD
         *
         * Flush after 7, then 15, 23, 31, ....
         * The first flush is at 7 so we don't need to special case pageNum=0.
         * Then we can do every 8 after that.
         * 
         */
        if ( (pageNum + 1) % LOG_FLUSH_THRESHOLD == 0 ) {
            (void)bs_logflush_start( logbfap, lsn );
        }

        break;

    default:
        /* This error path does not release the page. */
        /* bp->bufLock is locked; IO_TRANS is set */
        clear_state( bp, IO_TRANS );
        mutex_unlock( &bp->bufLock );
        return(EBAD_PARAMS);
        break;
    }

    return( EOK );
}


/*
 * bs_find_page
 *
 * Determine whether the page of the given bitfile
 * is in the cache.  Return 1 if the page is found,
 * 0 if the page is not found in the cache.
 */

int
bs_find_page( struct bfAccess *bfap,          /* in */
              unsigned long bfPage,           /* in */
              int block                       /* in */
            )
{
    int rflags;
    int error;
    vm_page_t pp;


    rflags = B_READ | B_CACHE;

    error = ubc_lookup(
                       bfap->bfObj,
                       bfPage * ADVFS_PGSZ,
                       PAGE_SIZE,
                       PAGE_SIZE,
                       &pp,
                       &rflags,
                       0 ); /* only checking existence so default vm_policy */

    if (error) {
        return( 0 );
    }

    if (rflags & B_NOCACHE) {
        return( 0 );
    }

    ubc_page_release(pp, B_READ);
    return( 1 );
}


/*
 * state_block
 *
 * Called to block on bp->lock.state == BUSY, etc.
 *
 * SMP - the bsBuf.bufLock is used to guard the state.
 */

void
_state_block(
#ifndef ADVFS_DEBUG
    struct bsBuf *bp,   /* in - buffer on which to block */
    int *wait           /* in/out - waited previously? */
#else
    struct bsBuf *bp,   /* in - buffer on which to block */
    int *wait,          /* in/out - waited previously? */
    int ln,
    char *fn
#endif /* ADVFS_DEBUG */
    )
{

#ifdef ADVFS_DEBUG
    if (!bp->bufLock.locked) {
        printf( "_state_block: ln = %d, fn = %s\n", ln, fn );
        ADVFS_SAD0( "_state_block: bsBuf.bufLock not locked" );
    }

    bp->lock.hdr.try_line_num = ln;
    bp->lock.hdr.try_file_name = fn;
#endif /* ADVFS_DEBUG */

    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    if (AdvfsLockStats) {
        if (*wait) {
            AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].reWait++;
            AdvfsLockStats->bufReWait++;
        } else {
            *wait = 1;
        }

        AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].wait++;
        AdvfsLockStats->bufWait++;
    }

    /*
     * Block until condition variable is signaled.
     */
    bp->lock.waiting++;
    cond_wait( &bp->lock.bufCond, &bp->bufLock );
    bp->lock.waiting--;

#ifdef ADVFS_DEBUG
    bp->lock.hdr.line_num = ln;
    bp->lock.hdr.file_name = fn;
    bp->lock.hdr.use_cnt++;
#endif /* ADVFS_DEBUG */
}


/*
 * bs_set_bufstate
 *
 * Optionally wait for the specified buffer state to clear.  Then, set
 * the state to the specified value.
 *
 * Return a bad status if the ref handle is invalid.
 */

statusT
bs_set_bufstate(
                bfPageRefHT bfPageRefH,  /* in */
                int ln,  /* in */
                uint32T stateBit,  /* in */
                int waitFlag  /* in */
                )
{
    struct bsBuf *bp;

    /* Convert the buffer handle. */
    bp = (struct bsBuf *)bfPageRefH;

    mutex_lock( &bp->bufLock );

    if (waitFlag != 0) {
        /*
         * Wait for the state to become clear, and then set it.
         */
        set_state (bp, ln, stateBit);
    } else {
        /*
         * Unconditionally set the state.
         */
        bp->lock.state |= stateBit;
        bp->ln = SET_LINE_AND_THREAD(ln);
    }

    mutex_unlock( &bp->bufLock );
    return( EOK );
}

/*
 * bs_clear_bufstate
 *
 * Clears the specified buffer state and wakes waiters.
 *
 * Return a bad status if the ref handle is invalid.
 */

statusT
bs_clear_bufstate(
                  bfPageRefHT bfPageRefH,  /* in */
                  uint32T stateBit  /* in */
                  )
{
    struct bsBuf *bp;

    bp = (struct bsBuf *)bfPageRefH;

    mutex_lock( &bp->bufLock );
    clear_state (bp, stateBit);
    mutex_unlock( &bp->bufLock );

    return( EOK );
}

/*
 * bs_get_bufstate
 *
 * Return the single bit in the buffer state corresponding
 * to the given bit.
 *
 * Return a bad status if the ref handle is invalid.
 */

statusT
bs_get_bufstate(
    bfPageRefHT bfPageRefH,      /* in */
    unsigned whichStateBit,      /* in */
    unsigned *stateBitValue      /* out */
    )
{
    struct bsBuf *bp;

    bp = (struct bsBuf *)bfPageRefH;

    mutex_lock( &bp->bufLock );
    *stateBitValue = whichStateBit & bp->lock.state;
    mutex_unlock( &bp->bufLock );

    return( EOK );
}


/*
 * set_state
 *
 * Wait for the specified state to clear, then set that state.
 * bsBuf.bufLock must be held when called.
 */

void
set_state (
           struct bsBuf *bp,  /* in */
           int ln,  /* in */
           uint32T state  /* in */
           )
{
    int wait = 0;

    /*
     * First wait until the buffer is
     * not in the specified state.
     */
    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    while( bp->lock.state & state ) {
        state_block( bp, &wait );
    }
    bp->lock.state |= state;
    bp->ln = SET_LINE_AND_THREAD(ln);

    return;
}

/*
 * wait_state
 *
 * Wait for the specified state to clear.
 * bsBuf.bufLock must be held when called.
 */

void
wait_state (
            struct bsBuf *bp,  /* in */
            uint32T state  /* in */
            )

{
    int wait = 0;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    while( bp->lock.state & state ) {
        state_block( bp, &wait );
    }
    return;
}

/*
 * clear_state
 *
 * Clear the specified state and wake up waiters.
 * bsBuf.bufLock must be held when called.
 */

void
clear_state (
             struct bsBuf *bp,  /* in */
             uint32T state  /* in */
             )
{
    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    if( bp->lock.state & state ) {
        bp->lock.state &= ~state;

        if( bp->lock.waiting > 0 ) {
            if( bp->lock.waiting == 1 ) {
                if (AdvfsLockStats) {
                    AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].signal++;
                    AdvfsLockStats->bufSignal++;
                }

                cond_signal( &bp->lock.bufCond );
            } else {
                if (AdvfsLockStats) {
                    AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].broadcast++;
                    AdvfsLockStats->bufBroadcast++;
                }

                cond_broadcast( &bp->lock.bufCond );
            }
        }
    }
    return;
}


/*
 * buf_remap
 *
 * Allow a buffer whose associated page is already in the UBC 
 * to be remapped to a different disk location.  Used by
 * the migrate code.  This code assumes that the buffer
 * is pinned.
 */

statusT
buf_remap(
   bfPageRefHT bfPageRefH,      /* in */
   blkMapT *blkMap )            /* in */
{
    struct bsBuf *bp;
    ioDescT *tmp;
    int i;
    int descCnt;

    /* Convert the buffer handle */
    bp = (struct bsBuf *)bfPageRefH;

    /*
     * Allocate space for the new ioDesc's
     */
    tmp = (ioDescT *)ms_malloc(
                            blkMap->maxCnt *
                            sizeof( ioDescT ) );

    if( bp->ioList.maxCnt > BUFIODESC ) {
        ms_free( bp->ioList.ioDesc );
    }

    mutex_lock( &bp->bufLock );

    bp->ioList.ioDesc = tmp;
    bp->ioList.read = blkMap->read;
    bp->ioList.readCnt = blkMap->readCnt;
    bp->ioList.write = blkMap->write;
    MS_SMP_ASSERT(bp->ioList.write == 0);
    bp->ioList.writeCnt = blkMap->writeCnt;
    bp->ioList.maxCnt = blkMap->maxCnt;
    descCnt = blkMap->maxCnt;

    for( i = 0; i < descCnt; i++ ) {
        IODESC_CLR( bp, i );
        bp->ioList.ioDesc[i].blkDesc = blkMap->blkDesc[i];
        bp->ioList.ioDesc[i].numBlks = ADVFS_PGSZ_IN_BLKS;
    }

    mutex_unlock( &bp->bufLock );

    return( EOK );
}

/* limit a single read-ahead to 5% of the buffers in the buffer cache. */
unsigned int AdvfsReadAheadMaxBufPercent = 5;

/* Number of consolidated I/Os that seq_ahead_cont() will attempt to get
 * started if there are sufficient buffers. seq_ahead_start() attempts to
 * start twice this number of I/Os.
 */
unsigned int AdvfsReadAheadNumIOs = 1;


static
void
seq_ahead_cont(
    bfAccessT *bfap,        /* in */
    struct bsBuf *bp,       /* in */
    unsigned long bsPage,   /* in - bf page number */
    short metaCheck,        /* in - metadata check type */
    vm_policy_t vmp,        /* in - vm page locality */
    vm_offset_t offset,     /* in - starting offset */
    vm_size_t len,          /* in - number of bytes */
    int ubc_flags           /* in - ubc control */
    )
{
    struct vd *vdp;
    int rdmaxio;
    unsigned long startPage;
    ioDescT *ioDescp;
    uint32T local_vd_index;
    int listLen = 0;
    ioDescT *ioListp;
    long nPages;
    unsigned long endPage, maxPages, numBlocks;

    /* This routine gets called a lot of times when we are not on a 
     * trigger page, so do as little processing as possible when we
     * are not on a trigger page.
     */
    if (bsPage == 0)
        return;
    mutex_lock( &bfap->bfIoLock );
    if (bsPage != bfap->raHitPage ) {
        mutex_unlock( &bfap->bfIoLock );
        return;
    } else
        mutex_unlock( &bfap->bfIoLock );

    /* Seize bufLock here to prevent reading the values from the 
     * buffer's ioDesc.blkDesc while it is being modified in
     * buf_remap().
     */
    mutex_lock( &bp->bufLock );
    ioDescp = &bp->ioList.ioDesc[bp->ioList.read];
    local_vd_index = ioDescp->blkDesc.vdIndex;
    mutex_unlock( &bp->bufLock );

    vdp = VD_HTOP(local_vd_index, bfap->dmnP);
    rdmaxio = vdp->rdmaxio;

    mutex_lock( &bfap->bfIoLock );
    if( bsPage != 0 && bsPage == bfap->raHitPage ) {

        /* We are at a trigger page; we need to read-ahead some more
         * pages.  Start at page indicated in raStartPage and read
         * the number of pages that can be read in a predetermined
         * number of consolidated I/Os (default = 1).  Be sure to scale 
         * this number back to exceed neither the number of pages 
         * remaining in the file, nor the preset limit for # of buffers 
         * to use for one read-ahead.
         */
        maxPages = AdvfsReadAheadNumIOs * howmany(rdmaxio, ADVFS_PGSZ_IN_BLKS);
        maxPages = MIN(maxPages, (CURRENT_UC()->ubc_maxpages/(100/AdvfsReadAheadMaxBufPercent)));

        /* Don't try to read-ahead past the end of the file, and don't
         * read-ahead into a frag page. 
         */
        if ((bfap->bfSetp->cloneId != BS_BFSET_ORIG) && bfap->cloneId) {
            endPage = bfap->maxClonePgs - 1;
        }
        else {
            endPage = bfap->nextPage - 1;
        }

        if ( bfap->fragState == FS_FRAG_VALID && 
             bfap->fragPageOffset             &&
             endPage >= bfap->fragPageOffset) {
                endPage = bfap->fragPageOffset - 1;
        }

        nPages  = (endPage - bfap->raStartPage) + 1;
        if (nPages <= 0) {
            /* No pages to read-ahead */
            mutex_unlock( &bfap->bfIoLock );
            return;
        } else if ( nPages > maxPages ) {
            nPages = maxPages;
        }

        /* Set up new trigger and start pages for next read-ahead. */
        startPage = bfap->raStartPage;          /* page to start reading */
        bfap->raHitPage = startPage - 1;        /* next trigger page */
        bfap->raStartPage = startPage + nPages; /* next start page */

        /* Convert Pages to 512-byte blocks */
        numBlocks = nPages * ADVFS_PGSZ_IN_BLKS; /* # 512-byte blocks to read */

        mutex_unlock( &bfap->bfIoLock );

        seq_ahead(bfap, startPage, numBlocks, &listLen, &ioListp, metaCheck,
                  vmp, offset, len, ubc_flags);

        if( listLen ) {
            if( listLen > 1 ) {
                bs_q_list( ioListp, listLen, bs_q_blocking );
            } else {
                bs_q_blocking( ioListp, 1 );
            }
        }
    } else {
        mutex_unlock( &bfap->bfIoLock );
    }
}


static
void
prefetch(
    bfAccessT *bfap,        /* in */
    unsigned long bsPage,   /* in - bf page number to start with */
    int pageCnt,            /* in - num pages to prefetch */
    int *listLen,           /* out - len of output ioDesc list */
    ioDescT **ioListp,      /* out - list of ioDesc */
    int doRead,             /* in - do we (TRUE) or caller (FALSE) do read */
    short metaCheck,        /* in - metadata check type */
    vm_policy_t vmp,        /* in - vm page locality */
    vm_offset_t offset,     /* in - starting offset */
    vm_size_t len,          /* in - number of bytes */
    int ubc_flags           /* in - ubc control */
    )
{
    int blkCnt;

    *listLen = 0;

    blkCnt = pageCnt * ADVFS_PGSZ_IN_BLKS;

    /*
     * Get a list of buffers to pre-fetch, mapped
     * and ready to go.
     */

    seq_ahead(bfap, bsPage, blkCnt, listLen, ioListp, metaCheck,
              vmp, offset, len, ubc_flags);

    if ( doRead && *listLen ) {
        if( *listLen > 1 ) {
            bs_q_list( *ioListp, *listLen, bs_q_blocking );
        } else {
            bs_q_blocking( *ioListp, 1 );
        }
    }
}


/* Set up the buffer-cache for the next set of pages to be read.
 * If we can, read the # of pages that can be transferred in 2 (default) 
 * I/Os.  If this would read past the end of file, or would use up too 
 * many buffers from the buffer pool, scale back the number of buffers 
 * to be read.
 */
static
void
seq_ahead_start(
    bfAccessT *bfap,        /* in */
    struct bsBuf *bp,       /* in */
    int *listLen,           /* out - len of output ioDesc list */
    ioDescT **ioListp,      /* out - list of ioDesc */
    unsigned long bsPage,   /* in - bf page number */
    short metaCheck,        /* in - metadata check type */
    vm_policy_t vmp,        /* in - vm page locality */
    vm_offset_t offset,     /* in - starting offset */
    vm_size_t len,          /* in - number of bytes */
    int ubc_flags           /* in - ubc control */

    )
{
    struct vd *vdp;
    int rdmaxio;
    ioDescT *ioDescp;
    uint32T local_vd_index;
    long nPages;
    unsigned long endPage, maxPages, numBlocks;


    /* Seize bufLock here to prevent reading the values from the 
     * buffer's ioDesc.blkDesc while it is being modified in
     * buf_remap().
     */
    mutex_lock( &bp->bufLock );
    ioDescp = &bp->ioList.ioDesc[bp->ioList.read];
    local_vd_index = ioDescp->blkDesc.vdIndex;
    mutex_unlock( &bp->bufLock );

    vdp = VD_HTOP(local_vd_index, bfap->dmnP);
    rdmaxio = vdp->rdmaxio;

    mutex_lock( &bfap->bfIoLock );

    /* Initially, the max # pages to read is the number that can be read
     * in a predefined number of consolidated I/Os for this disk.  Here
     * we start twice the number of I/Os as will be started each time
     * we hit a trigger page in seq_ahead_cont().
     */
    maxPages = 2 * AdvfsReadAheadNumIOs * howmany(rdmaxio, ADVFS_PGSZ_IN_BLKS);

    /* Don't allow this read-ahead to use up all the buffer-pool buffers,
     * particularly when the disk can do large transfers, but the buffer
     * pool is relatively small.
     */
    maxPages = MIN(maxPages, (CURRENT_UC()->ubc_maxpages / (100 / AdvfsReadAheadMaxBufPercent)));

    /* Don't try to read-ahead past the end of the file, and don't
     * read-ahead into a frag page. 
     */
    if ((bfap->bfSetp->cloneId != BS_BFSET_ORIG) && bfap->cloneId) {
        endPage = bfap->maxClonePgs - 1;
    }
    else {
        endPage = bfap->nextPage - 1;
    }

    if ( bfap->fragState == FS_FRAG_VALID && 
         bfap->fragPageOffset             &&
         endPage >= bfap->fragPageOffset  ) {
            endPage = bfap->fragPageOffset - 1;
    }

    nPages  = endPage - bsPage;
    if (nPages <= 0) {
        /* No pages to read-ahead */
        mutex_unlock( &bfap->bfIoLock );
        return;
    } 

    /* Set # pages to the lesser of the pages to the end of the file
     * or the maximum that can be read.
     */
    if ( nPages < maxPages ) {
        /* Read-ahead limited by file size; raHitPage points to last page */
        bfap->raHitPage = bsPage + nPages;
        bfap->raStartPage = bfap->raHitPage + 1;
    } else {
        /* Normal path; raHitPage is set to the middle of the range currently
         * being read as a trigger page to start the next read-ahead;
         * raStartPage is set to the next page to read-ahead in seq_ahead_cont.
         */
        nPages = maxPages;
        bfap->raHitPage   = bsPage + ( nPages / 2 );
        bfap->raStartPage = bsPage + nPages + 1;
    }


    /* Convert Pages to 512-byte blocks */
    numBlocks = nPages * ADVFS_PGSZ_IN_BLKS; /* # 512-byte blocks to read */

    mutex_unlock( &bfap->bfIoLock );

    seq_ahead(bfap, bsPage + 1, numBlocks, listLen, ioListp, metaCheck,
              vmp, offset, len, ubc_flags);

}


/*
 * seq_ahead
 *
 * This routine is called to prepare a list of
 * buffer headers for pre-fetching.  Given a
 * starting page in a bitfile and a limit (expressed
 * in terms of disk blocks) we fill in a buffer
 * header for each page so that the read can be
 * completed.
 *
 * Basically, we want to do things similarly to those
 * done in bs_refpg where the buffer was not found in
 * the cache.
 */

void
seq_ahead( struct bfAccess *bfap,       /* in */
            unsigned long bsPage,       /* in */
            int maxBlks,                /* in */
            int *listLen,               /* in/out */
            ioDescT **ioListp,          /* out */
            short metaCheck,            /* in - metadata check type */
            vm_policy_t vmp,            /* in - vm page locality */
            vm_offset_t offset,         /* in - starting offset */
            vm_size_t len,              /* in - number of bytes */
            int ubc_flags               /* in - ubc control */
          )
{
    struct bsBuf *bp = NULL;
    int numBlks = 0;
    int s;
    statusT sts;
    ioDescT *ioDescp;
    int endPage;
    int cowLkLocked = FALSE;
    int useOrigMap = FALSE;
    bfSetT *bfSetp;
    struct bfAccess *origBfap = NULL;
    vm_page_t pp;
    int flags;
    struct timeval new_time;
    int wday;
    uint32T totHotCnt;


    *ioListp = NULL;
    endPage = bsPage + (maxBlks / ADVFS_PGSZ_IN_BLKS) - 1;

    /*
     * Clone code
     */

    bfSetp = bfap->bfSetp;
    if( bfSetp->cloneId != BS_BFSET_ORIG ) {
        origBfap = bfap->origAccp;
    }

    /*
     * A frag page might be in the UBC.  Check to see if the endPage needs 
     * to be adjusted to avoid the possiblity of clobbering a frag page already 
     * in the UBC
     */
    if (bfap->fragState == FS_FRAG_VALID && endPage >= bfap->fragPageOffset) {
        if (bfap->fragPageOffset)
            /*
             * stop at the page before the frag page
             */
            endPage = bfap->fragPageOffset - 1;
        else {
            /*
             * the file doesn't have any pages other than the frag,
             * so return
             */
            return;
        }
    }

    while( numBlks < maxBlks && bsPage <= endPage ) {

        if( bfSetp->cloneId != BS_BFSET_ORIG ) {

            /*
             * This is a clone bitfile.  If it doesn't map the page being
             * ref'd then we use the orig bitfiles access struct to
             * get the mapping.
             * This does not get called from pin, it should have been
             * flagged as an error before the code reached here.
             * We have already checked for an outOfSyncClone in
             * bs_refpg_int.
             */

            if ( !page_is_mapped(bfap, (uint32T)bsPage, NULL,TRUE) ) {
                useOrigMap = TRUE;
            } else {
                useOrigMap = FALSE;
            }
        }

        /* If the page already exists in the UBC, then skip this page.
         * This is a racy check, since the object is locked only within
         * ubc_incore(). UBC might expel the page from the cache just after
         * returning from this call. For read ahead and prefetch, the worst 
         * case is that a thread may need to wait on IO, but this is
         * expected to be rare.  At any rate, continue on to look at the
         * rest of the pages in the requested range.
         */
        if (ubc_incore(bfap->bfObj, bsPage * ADVFS_PGSZ, ADVFS_PGSZ)) {
            numBlks += ADVFS_PGSZ_IN_BLKS;
            bsPage++;
            continue;
        }

        /* Must not use callers ubc_flags here.
         * There might be some flags that need extracted later, but
         * for now just do B_READ.  We definitely do not want B_WRITE
         * since that makes all the prefetched pages dirty even though
         * the caller may never actually touch them.  If they later pin
         * any prefetched page, then that ubc_lookup will dirty it.
         */
        flags = ubc_flags = B_READ;
        sts = ubc_lookup(bfap->bfObj,
                         bsPage * ADVFS_PGSZ, /* Pg byte offset */
                         ADVFS_PGSZ,      /* Block size*/
                         ADVFS_PGSZ,      /* Request byte length */
                         &pp,             /* UBC page */
                         &flags,
                         vmp);            /* vm policy */

        if (sts) {
            return;
        }

        /* If the page is found in the UBC, skip it but continue to
         * check succeeding pages. 
         */
        if (!(flags & B_NOCACHE)) {
            /* Update statistics counters. */
            if (advfsstats) 
                AdvfsUbcHit++;
            ubc_page_release(pp, ubc_flags);
            numBlks += ADVFS_PGSZ_IN_BLKS;
            bsPage++;
            continue;
        }

        /* Initialize a new bsBuf. */
        bp = bs_get_bsbuf(PAGE_TO_MID(pp), TRUE);

        /* Fill in fields of new buffer descriptor. 
         * The bufLock is not held since other threads are blocked on the
         * vm_page's pg_busy flag until this thread's page IO completes.
         */
        bp->lock.state = IO_TRANS | BUSY | READING;
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        bp->bfPgNum = bsPage;
        bp->bfAccess = bfap;
        bp->ioCount = 1;
        bp->ioDesc.numBlks = ADVFS_PGSZ_IN_BLKS;
        bp->vmpage = pp;
        bp->ubc_flags = ubc_flags;
        bp->metaCheck = metaCheck;

        pp->pg_opfs = (unsigned long) bp;

        /* Map bf page to vd block. */
        sts = blkMap( bp, (useOrigMap) ? origBfap : bfap);

        if ( sts != EOK ) {
            ubc_page_zero(pp, 0, ADVFS_PGSZ);
            mutex_lock( &bp->bufLock );
            bp->lock.state |= UNMAPPED;
            if ( sts == E_PAGE_NOT_MAPPED ) {
                MS_SMP_ASSERT(bp->result == EOK);
            } else {
                bp->result = sts;
            }
            /* have bs_io_complete() drop UBC pg_hold reference */
            bp->ubc_flags |= B_BUSY;
            bs_io_complete( bp, &s );
            return;
        }

        mutex_lock( &bp->bufLock );

        /*
         * Link together the I/O descriptors
         */
        ioDescp = &bp->ioList.ioDesc[bp->ioList.read];

        if( *ioListp == NULL ) {
            *ioListp = ioDescp;
            ioDescp->fwd = ioDescp->bwd = ioDescp;
        } else {
            (*ioListp)->bwd->fwd = ioDescp;
            ioDescp->bwd = (*ioListp)->bwd;
            (*ioListp)->bwd = ioDescp;
            ioDescp->fwd = (*ioListp);
        }
        (*listLen)++;

        /* Update statistics counters. */
        if (advfsstats) 
            AdvfsUbcMiss++;
        bp->bfAccess->dmnP->bcStat.raBuf++;

        numBlks += ioDescp->numBlks;

        if (SS_is_running &&
            bp->bfAccess->dmnP->ssDmnInfo.ssDmnSmartPlace &&
            (bp->bfAccess->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             bp->bfAccess->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

            /* send cache miss message to smartstore if enough have occurred */
            TIME_READ(new_time);
            GETCURRWDAY(new_time.tv_sec,wday);
            atomic_incl(&bp->bfAccess->ssHotCnt[wday]);
            for(totHotCnt=0,wday=0; wday<SS_HOT_TRACKING_DAYS; wday++) {
                totHotCnt += bp->bfAccess->ssHotCnt[wday];
            }
            if(totHotCnt >
               bp->bfAccess->dmnP->ssDmnInfo.ssAccessThreshHits) {
                ss_snd_hot(HOT_ADD_UPDATE_MISS, bp->bfAccess);
            }
        }

        mutex_unlock( &bp->bufLock );
        bsPage++;

        /* Since this is an asynchronous page IO, release the page to
         * decrement the pg_hold counter. The page's pg_busy flag remains
         * set to block other references until the IO completion.
         */
        ubc_page_release(pp, ubc_flags);
    } /* end while */

}


/*
 * bs_invalidate_pages
 *
 * If pageCnt is 0, invalidate all of the bitfile's pages.
 * Otherwise, invalidate the page range.  It is required that all
 * pages being invalidated are "don't care" and can be purged or
 * written to disk without harm in either case.
 *
 * SMP: 1. bfap->bfIoLock is held while walking dirtyBufList.
 *      2. bp->bufLock for each dirty buffer is held while modifying.
 *
 * NOTE: Currently this routine only purges buffers from the lazy
 * queues since some callers that are flushing pages to disk place
 * them on the blocking type queues and then call here before those
 * pages have completed.  This should be changed.
 */

void
bs_invalidate_pages (
                     bfAccessT *bfap,  /* in */
                     uint32T pageOffset,  /* in */
                     uint32T pageCnt,  /* in */
                     int invalflag     /* in - CURRENTLY UNUSED */
                     )
{
    struct bsBuf *bp, *purge_buffer;
    long skipped_buffers, preempt_limit, max_cpu_ticks;
    long *ptr_to_cpu_ticks;
    int s;
    uint32T found_pages = 0;
    thread_t th;


    /* Since we could spend a long time in the loop purging pages
     * setup a max cpu hold time so we preempt and threads bound
     * to our cpu won't time out.  The arbitrary limit is set close
     * to 20 milliseconds while allowing a shift instead of divide.
     * With rt_preempt_enabled, we preempt on simple_unlock if the
     * locks held drops to 0 so no need to do it ourselves.
     */
    if (!rt_preempt_enabled) {
        max_cpu_ticks = 20 * hz / 1024;
        th = current_thread();
        /* give cpu to any waiting lightweight contexts before we start */
        if (lwc_pending())
            thread_preempt( th, FALSE );
        /* anytime we block in thread_preempt() we can change processors */
        ptr_to_cpu_ticks = &(current_processor()->p_lbolt);
        preempt_limit = *ptr_to_cpu_ticks + max_cpu_ticks;
    } else {
        /* disable limit test so it can never succeed */
        ptr_to_cpu_ticks = &preempt_limit;
        preempt_limit = 0;
    }

startover:
    skipped_buffers = 0;
    purge_buffer = NULL;
    mutex_lock( &bfap->bfIoLock );

    for ( bp = bfap->dirtyBufList.accFwd;
          bp != (struct bsBuf *)&bfap->dirtyBufList;
          bp = bp->accFwd ) {

        /* Skip pages outside the page range. */
        if (pageCnt &&
            ((bp->bfPgNum < pageOffset) ||
             (bp->bfPgNum >= (pageOffset + pageCnt)))) {
            continue;
        }

        /* If the I/O is at the device layer, we can't stop it.  This
         * state can change to true while we don't have the buffer lock,
         * but can not change to false while we hold the chain lock.
         * So we save time by not locking when it is already true.
         */
        if ( bp->lock.state & BUSY ) { /* true for all non-lazy queues */
            continue;
        }

        /* Try to lock the buffer in out-of-hierarchy order since if it
         * succeeds, this is the simplest thing to do.  If it fails, try
         * the next buffer but remember we need to start again.
         */
        if ( !mutex_lock_try(&bp->bufLock.mutex) ) {
            skipped_buffers++;
            continue;
        }

        /* Drop chain lock now so we don't hold it too long.  There is
         * some timeout risk if there are many buffers on the dirty
         * list before we get here, but no easy solution to fix it.
         * In reality, if simple_locks are enabled we are at risk
         * even though the lock is dropped.  The problem is we may
         * retake it after only a few instructions and another thread
         * never gets the chance unless it is spinning on the lock
         * since the miss code makes it less likely to get the lock.
         */
        mutex_unlock( &bfap->bfIoLock );

        /* Lost the race - the buffer is at the device layer and we can't
         * remove it so move on to the next buffer.  Re-get the chain lock
         * while we still have the buffer locked so the chain remains
         * valid since both are needed to remove this buffer.  We don't
         * care that other threads could have removed or inserted other
         * buffers on the list, all we need is our own buffer to stay on
         * the list so bp->accFwd is valid once we relock the bfIoLock.
         */
        if ( bp->lock.state & BUSY ) {
            mutex_lock( &bfap->bfIoLock );
            mutex_unlock( &bp->bufLock );
            continue;
        }

        /* Don't wait for IO_TRANS to clear since we don't known
         * what the thread that has it is doing.  If the buffer is
         * going onto one of the blocking queues, IO_TRANS remains
         * set until I/O is complete.  On a future scan of the chain,
         * either the buffer should transition to BUSY or be gone or
         * have IO_TRANS clear.
         */
        if ( bp->lock.state & IO_TRANS ) {
            mutex_lock( &bfap->bfIoLock );
            mutex_unlock( &bp->bufLock );
            skipped_buffers++;
            continue;
        }

        /* Transition vm_page to busy state so it will block UBC
         * from freeing our bsBuf after we drop locks.  The busy
         * state also keeps UBC from starting a putpage or doing
         * a lookup.  Since we set busy we don't need to hold the
         * vm_page so we get much better performance (see below).
         * And we need to change the page to busy anyway before
         * calling bs_io_complete.  We also count on the buffer
         * state set below to keep advfs threads from stealing it
         * or sending it to the device.  The page can only be busy
         * here if UBC is initiating a putpage, in which case we
         * just continue processing the chain.
         */
        if (advfs_page_get(bp, UBC_GET_BUSY)) {
            mutex_lock( &bfap->bfIoLock );
            mutex_unlock( &bp->bufLock );
            continue;
        }

        MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);

        if (bp->writeRef ) {
            ADVFS_SAD0("bs_invalidate_pages(1): buf pinned");
        }

        /* Set the REMOVE_FROM_IOQ bit so the IO descriptors won't
         * be moved onto the device queue when we drop the lock to
         * do the rm_ioq.  Since this buffer can't already be on a
         * blocking type queue, IO_TRANS keeps other threads away.
         */
        bp->lock.state |= (REMOVE_FROM_IOQ | IO_TRANS | BUSY);
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        mutex_unlock( &bp->bufLock );
        /* Remove any ioDesc's that may be on IO queues */
        rm_ioq( bp );

        /* Do a fake I/O completion on the last buffer we found on a
         * previous pass.  Hang onto the buffer we just marked so it
         * keeps our position in the dirty buffer chain and we don't
         * have to restart at the beginning and waste time when we
         * are truncating part of a file with many dirty buffers.
         * Note we only marked the vm_page busy, we didn't hold it.
         * So we don't release it here, which saves time and cuts the
         * vm object locking in half.  Since the page and bsBuf can
         * disappear as soon as bs_io_complete calls ubc_page_release,
         * we must not touch either again.
         */
        if (purge_buffer) {
           mutex_lock( &purge_buffer->bufLock );
           MS_SMP_ASSERT(purge_buffer->lock.state & WRITING);
#ifdef ADVFS_SMP_ASSERT
           purge_buffer->busyLn = SET_LINE_AND_THREAD(__LINE__);
           purge_buffer->ioqLn = -1;
#endif
           purge_buffer->lock.state &= ~REMOVE_FROM_IOQ;
           bs_io_complete( purge_buffer, &s );
        }

        /* do the buffer we just found on a subsequent pass */
        purge_buffer = bp;

        /* need this so threads bound to our cpu don't time out */
        if ( *ptr_to_cpu_ticks > preempt_limit ) {
            thread_preempt( th, FALSE );
            ptr_to_cpu_ticks = &(current_processor()->p_lbolt);
            preempt_limit = *ptr_to_cpu_ticks + max_cpu_ticks;
        }

        mutex_lock( &bfap->bfIoLock );

        /* stop looking if we found all we were suppose to do */
        found_pages++;
        if (found_pages == pageCnt) {
            skipped_buffers = 0;
            break;
        }
    }

    /* Because we do out-of-order locking, we need to restart after
     * dropping the chain lock if there were buffers we skipped due
     * to the buffer being locked or being in transition.
     */
    mutex_unlock( &bfap->bfIoLock );
    if (purge_buffer) {
       mutex_lock( &purge_buffer->bufLock );
       MS_SMP_ASSERT(purge_buffer->lock.state & WRITING);
#ifdef ADVFS_SMP_ASSERT
       purge_buffer->busyLn = SET_LINE_AND_THREAD(__LINE__);
       purge_buffer->ioqLn = -1;
#endif
       purge_buffer->lock.state &= ~REMOVE_FROM_IOQ;
       bs_io_complete( purge_buffer, &s );
    }

    /* If we need to restart the loop, other threads are probably
     * waiting for the bfIoLock, but we have not dropped the lock
     * for enough time to let them get it.  Block for a tick so they
     * can do their thing, which also releases our cpu.
     */
    if (skipped_buffers)
    {
        assert_wait_mesg_timo(NULL, FALSE, "skipped_buffers", 1);
        thread_block();
        /* Never do cpu time limit check when real time preempt is on. */
        if (!rt_preempt_enabled) {
            ptr_to_cpu_ticks = &(current_processor()->p_lbolt);
            preempt_limit = *ptr_to_cpu_ticks + max_cpu_ticks;
        }
        goto startover;
    }

    /* Preemption check before we let ubc do its thing. */
    if ( *ptr_to_cpu_ticks > preempt_limit ) {
        thread_preempt( th, FALSE );
    }

    /* We are not holding any locks that other threads need to complete
     * I/O so let ubc_invalidate() wait to remove all busy pages from
     * the dirty and clean list and wait for wired pages to be unwired.
     */

    if ( pageCnt == 0 ) {
        (void)ubc_invalidate( bfap->bfObj, (vm_offset_t)0,
                              (vm_size_t)0, B_INVAL);
    } else {
        (void)ubc_invalidate( bfap->bfObj,
                (vm_offset_t)pageOffset * ADVFS_PGSZ,
                (vm_size_t)pageCnt * ADVFS_PGSZ,
                B_INVAL);
    }

}


/*
 * msfs_flush_and_invalidate
 *
 * This function flushes and optionally invalidates all
 * dirty data for an AdvFS file.  
 * 
 * Note: This routine was originally made for CFS only.
 *       Callers must guarantee that no racing threads will
 *       create new dirty data since this routine will not
 *       tolerate new dirty UBC pages after the flush.
 */

void
msfs_flush_and_invalidate(
                          bfAccessT *bfap,
                          int fiflags)
{
    struct actRange *arp = NULL;

    /*
     * There are several conditions under which we should do no work here:
     *
     * 1. The passed-in access structure is a shadow access structure.
     * 2. The passed-in access structure is for a quota file and the
     *    caller has not explicitly said to go ahead and flush/invalidate
     *    the quota file.
     * 3. The passed-in access structure has no object.  This would be
     *    typical of VBLK, VCHR, and VFIFO vnodes.
     *
     * Here are some more in-depth notes on why these cases are to
     * be handled specially:
     * 
     * When accessing metadata from the .tags directory, a shadow access
     * structure is created and real_bfap points to the access structure
     * in the root bfSet.  If we are in this function with a vnode that
     * has a shadow access structure, it should only be because we are
     * recycling the vnode.  In that case, there is no need to flush
     * or invalidate any pages because the real_bfap continues
     * to be active.
     * 
     * Disallow flush and invalidation on metadata-controlled
     * quota.user (tag 4) and quota.group (tag 5) files.
     * This avoids having CFS directIO open requests call this
     * routine causing memory corruption due to dirty buffer
     * cache being prematurely freed. CFS cannot detect updates
     * to quota files. Those modifications would get lost
     * during the invalidation but AdvFS would still have
     * references to them on various buffer lists and IO queues.
     * If caller specifies the INVALIDATE_QUOTA_FILES flag,
     * we will bypass the default behavior and go ahead and
     * perform the flush and invalidate logic on the quota files.
     * This is currently done only when a quota file access structure
     * is going away.
     */
    if ((BS_BFTAG_PSEUDO(bfap->tag)) || (!bfap->bfObj) ||
        (((bfap->tag.num == USER_QUOTA_FILE_TAG) ||
          (bfap->tag.num == GROUP_QUOTA_FILE_TAG)) &&
         (!(fiflags & INVALIDATE_QUOTA_FILES)))) {
        return;
    }

    /* It has been shown that CFS can call here with a migrate running
     * in the kernel so we need to lock them out to prevent corruption.
     * Seize an active range for the whole file.  It is really only the
     * invalidation that is critical to coordinate with migrate, but we
     * might as well prevent any unneccessary flushing too.
     *
     * We only want the range if CFS is the caller since advfs callers
     * take the migTrunkLk and will deadlock due to different locking
     * order in some paths.  Advfs callers protect without the range.
     *
     * CFS invalidation is the exclusive user of INVALIDATE_UNWIRED,
     * and always uses it or NO_INVALIDATE which is safe with no range.
     */
    if (fiflags & INVALIDATE_UNWIRED) {
        arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));
        /* arp->arStartBlock and other fields 0'd by ms_malloc */
        arp->arEndBlock = ~0L;       /* biggest possible block */
        arp->arState = AR_INVALIDATE;
        arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
        insert_actRange_onto_list( bfap, arp, NULL );
    }

    /* 
     * Flush the file to disk.  For files under transaction control 
     * (directory and data logged files), bfflush() must be called to flush
     * the log prior to the file's transaction controlled dirty
     * data to maintain the write-ahead log rule.  
     * For all other files, we must flush to make sure busy pages are pushed
     * out to disk (in addition to dirty pages).
     */
    bfflush(bfap, 0, 0, fiflags & FLUSH_PREALLOC ?
                        (FLUSH_INTERMEDIATE | FLUSH_PREALLOCATED_PAGES) :
                         FLUSH_INTERMEDIATE);

    /*
     * Flush all dirty pages that remain on the object.  These include
     * dirty mmapped pages and/or vm_pages in sparse areas of the file.
     * We are doing this in a while loop until all dirty page have been
     * flushed to prevent invalidating dirty pages during token downgrade.
     * There is a chance that dirty pages could be skipped by ubc_flush_dirty 
     * and invalidated by ubc_invalidate if we do not check for them.
     *
     * DO NOT REMOVE this loop, it is here to prevent races from page faults
     * which are outside of CFS/ADVFS and can not be locked-out.
     */
    if(!(bfap->bfObj->vu_flags & UBC_NOFLUSH)) {
        /* Don't call ubc_flush_dirty if file is set to noflush, advfs will
         * take care of the flushing if this is the case.
         * Files marked noflush are not mmapped anyway because they are
         * advfs metadata files or are data logged.
         */
        do {
            ubc_flush_dirty(bfap->bfObj, B_DONE);
            if (bfap->bfObj->vu_dirtypl || bfap->bfObj->vu_dirtywpl) {
                assert_wait_mesg((vm_offset_t)0,FALSE,"flush_and_invalidate");
                thread_set_timeout(1);
                thread_block();
            }
        } while ((bfap->bfObj->vu_dirtypl || bfap->bfObj->vu_dirtywpl));
    }

    /* 
     * Invalidate the file's UBC pages upon request. 
     */
    if (fiflags & (INVALIDATE_UNWIRED | INVALIDATE_ALL)) {
        MS_SMP_ASSERT(!(fiflags & NO_INVALIDATE));
        /* pass in B_DONE to flush any dirty pages before invalidating pages */
        ubc_invalidate(bfap->bfObj, (vm_offset_t)0, (vm_size_t)0,
                       fiflags & INVALIDATE_UNWIRED ?
                       (B_DONE | B_ACTIVE) : B_DONE);
    }

    if (arp) {
        mutex_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }
}

/*
 * bfr_trace
 *
 * Trace ref's, pins, deref's, and unpins.
 */

void
bfr_trace(
    bfTagT tag,             /* in */
    u_long bsPage,          /* in */
    TraceActionT type,      /* in - ref, pin, deref, unpin */
    unsigned pgRlsMode      /* in - for unpin only */
    )
{
    int action;

    for( action = 0; action < sizeof( unsigned ) * 8; action++ ) {
        if( type & 1 )
            break;
        type >>= 1;
    }

    trace_hdr();

    if( pgRlsMode ) {

        log( LOG_MEGASAFE | LOG_INFO,
            "%10s:     %8s         %8d pag     %8d tag\n",
            PrintAction[ action ], UnpinAction[ pgRlsMode-1 ], bsPage,
            BS_BFTAG_IDX(tag) );

    } else {
        log( LOG_MEGASAFE | LOG_INFO,
            "%10s:     - - - - -        %8d pag     %8d tag\n",
            PrintAction[ action ], bsPage, BS_BFTAG_IDX(tag) );
    }
}
