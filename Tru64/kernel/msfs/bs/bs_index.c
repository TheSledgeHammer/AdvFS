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
 * 
 * 
 */
#pragma ident "@(#)$RCSfile: bs_index.c,v $ $Revision: 1.1.92.8 $ (DEC) $Date: 2007/08/10 11:43:50 $"

#define ADVFS_MODULE BS_INDEX

/* These are internal status */

#define INSERT 1
#define DONE 2
#define TRUNC 3
#define REMOVE 4
#define MODIFY 5
#define UPDATE 6
#define CONTINUE 7

#include <sys/lock_probe.h>
#include <msfs/fs_dir_routines.h>
#include <msfs/ms_public.h>
#include <msfs/bs_public.h>
#include <msfs/ftx_public.h>
#include <msfs/fs_dir.h>
#include <sys/stat.h>
#include <msfs/fs_quota.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_access.h>
#include <msfs/ms_osf.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/bs_index.h>

#define MIN_DIR_ENTRY_SIZE (sizeof(directory_header) + 4 + sizeof(bfTagT))

/* These defines are only used by the pruning routines */

#define FNAME 0
#define FFREE 1

statusT
idx_create_index_file_int(
    bfAccessT   *dir_bfap,
    ftxHT   ftxH
    );

uint32T
idx_bsearch_int(
    idxNodeT    *node,
    ulong       search_key,
    int         last_node
    );

ulong
idx_hash_filename_int(
    char *filename
    );

statusT
idx_index_get_free_pgs_int(
    bfAccessT       *idx_bfap,
    uint32T         count,
    idxPinnedPgsT   *pptr,
    ftxHT           parentFtxH,
    int		    forceflag
    );

void
idx_split_node_int(
    ulong        search_key,
    ulong        offset,
    ulong        index,
    idxNodeT    *src_node_ptr,
    idxNodeT    *dst_node_ptr,
    rbfPgRefHT   src_pgref,
    rbfPgRefHT   dst_pgref
    );

statusT
idx_insert_filename_int(
    uint32T        node_page,
    ulong          level,
    ulong          *search_key,
    ulong          *offset,
    bfAccessT      *idx_bfap,
    uint32T            *insert,
    int        *split_count,
    idxPinnedPgsT  *pptr,
    ftxHT           ftxH
    );

statusT
idx_remove_filename_int(
    uint32T     page_to_search,
    ulong      level,
    ulong      search_key,
    ulong      offset,
    bfAccessT  *idx_bfap,
    int        *found,
    ftxHT      ftxH
    );

statusT
idx_lookup_node_int(
    uint32T     node_page,
    uint32T         level,
    ulong       search_key,
    bfAccessT   *idx_bfap,
    idxNodeT    **found_ptr,
    void        *found_pgref,
    uint32T     *found_index,
    uint32T     pin_flag,
    int         *page,
    ftxHT       ftxH
    );

int
idx_directory_insert_space_int(
        uint32T node_page,
        ulong level,
        ulong *search_key,
        ulong *size,
        bfAccessT *idx_bfap,
        ftxHT     ftxH,
        int *split_count,
        idxPinnedPgsT *pptr,
        int forceflag
    );

int
idx_directory_get_space_int(
    uint32T page_to_search,
    uint32T level,
    ulong   *size,
    ulong   *file_offset,
    bfAccessT *idx_bfap,
    ftxHT ftxH
    );

long
idx_setup_for_truncation_int(
    bfAccessT *idx_bfap,
    uint32T page_to_search,
    uint32T level,
    ulong   *undo_space_start,
    int operation,
    ftxHT ftxH
    );

int
idx_glom_entry_int(
    bfAccessT   *idx_bfap,
    idxNodeT    *node_ptr,
    rbfPgRefHT   pgref,
    ulong        start_addr,
    ulong        size,
    uint32T      index,
    ftxHT       ftxH
    );

void
dir_sort_int(
    idxNodeT *node_ptr,
    ulong size
    );

int
idx_prune_btree_int (
    uint32T    page_to_search,
    uint32T    level,
    ulong      *search_key,
    uint32T    *page,
    uint32T    root_level,
    bfAccessT  *idx_bfap,
    uint32T    tree,
    ftxHT       ftxH
    );

statusT
idx_remove_page_of_storage_int(
    bfAccessT *idx_bfap,
    uint32T page,
    ftxHT ftxH
    );

statusT
idx_directory_insert_space_undo(
    idxUndoRecordT *recp,
    bfAccessT *idx_bfap,
    ftxHT ftxH
    );

/* We use the clean up thread for truncation and pruning */
extern msgQHT CleanupMsgQH;

/* Two useful macros */

#define RBF_PIN_FIELD( h, f ) rbf_pin_record( h, &(f), sizeof( f ) )
#define ROUND_UP(v,t) (((v) + (t) - 1) & ~((t) - 1))

/*******************************************************************
 *
 * Global statistic counters
 *
 * These will keep track of rare situations to aid in debugging.
 *
 *
 *******************************************************************/

struct
{
    ulong insert_space_failed;
    ulong get_space_failed;
    ulong prune_btree_incomplete;
    ulong prune_btree_failed;
    ulong get_free_pgs_failed;
    ulong remove_file_failed;
    ulong setup_for_truncation_failed;
    ulong remove_page_of_storage_failed;
    ulong redistribute_right;
    ulong prune_root_1_failed;
    ulong prune_root_2_failed;
}idxStats;


statusT
bf_idx_file_test(bsUnkIdxRecT *idxrec, ulong flagval)
{
	statusT retval = FALSE;
	if (idxrec && (idxrec != (bsUnkIdxRecT *) -1)) {
		if ((idxrec->flags & IDX_INDEX_FILE) == flagval){
			retval = TRUE;
		}
	}
	return(retval);
}


/*********************************************************************
 *
 * This routine initializes the index file agent's ftx operations
 *
 ********************************************************************/

init_idx_index_opx( void )
{
    statusT sts;

    sts = ftx_register_agent(FTA_IDX_UNDO_V1,
                             &idx_undo_opx, /* undo */
                             NULL
                             );
    if (sts != EOK) {
        ADVFS_SAD1 ("init_idx_index_opx: ftx register failure", sts);
    }
}


/*********************************************************************
 *
 * This routine will create an index file and associate it with a
 * directory. It will be called when a directory fills a page with
 * filenames. At that point the index file will be created.
 *
 * The pages added to the index file will be charged against the
 * directory owner. Since there will be no fs_context area associted
 * with the index file, the directry's context area will be used in
 * the call to chk_blk_quota.
 *
 * The index file will not be visible to the user. This implies that
 * many of the standard file creation calls will not be made.
 *
 * The tag for the index file will be kept in the MCELL of the
 * directory.
 *
 ********************************************************************/

statusT
idx_create_index_file_int(
    bfAccessT   *dir_bfap,   /* directory access structure */
    ftxHT   parentFtxH             /* parent's transaction Handle */
    )
{
    bfTagT  indexTag;
    bfAccessT *idx_bfap;
    bfParamsT bfParams;
    bsDirIdxRecT *dir_idx_params=NULL;
    bsIdxRecT *idx_params;
    statusT sts;
    idxUndoRecordT undorec;
    ftxHT create_ftxH;
    struct vnode *nullvp = NULL;

    /* Create the index file on-disk. */

    /* Put the creation of the index under a subftx so we can have an
     * undo routine clean up bogus MCELL records.
     */

    sts = FTX_START_N(FTA_IDX_UNDO_V1,
                      &create_ftxH, parentFtxH, parentFtxH.dmnP, 2);
    if (sts != EOK)
    {
        domain_panic(parentFtxH.dmnP,
                     "idx_create_index_file_int: ftx_start failed");
        return(sts);
    }

    bfParams.pageSize = ADVFS_PGSZ_IN_BLKS;
    bfParams.cl.dataSafety = BFD_FTX_AGENT;
    bfParams.cl.reqServices = nilServiceClass;
    bfParams.cl.optServices = nilServiceClass;

    sts = rbf_create(&indexTag,dir_bfap->bfSetp,&bfParams,create_ftxH, 1);

    if (sts != EOK)
    {
        ftx_fail(create_ftxH);
        return(sts);
    }

    /* Open the index file for initialization */

    sts = bs_access( &idx_bfap,
                     indexTag,
                     dir_bfap->bfSetp,
                     create_ftxH,
                     0,
                     NULLMT,
                     &nullvp
                     );

    if (sts != EOK)
    {
        ftx_fail(create_ftxH);
        return(sts);
    }

    /* Allocate 2 pages of storage for the index file starting at page
     * 0. Charge the space against the directory since the index
     * context area is not maintained. */

    sts = change_quotas(dir_bfap->bfVp, 0, 2 * ADVFS_PGSZ_IN_BLKS, NULL,
                        u.u_cred, 0, create_ftxH);

    if (sts != EOK) goto idx_create_error;

    sts = rbf_add_stg( idx_bfap, 0, 2, create_ftxH, 0 );

    if (sts != EOK) goto idx_create_error;

    /* Get memory for the index parameters that hang off the
     * directory's access structure. The memory for this will be
     * released in grab_bsacc when the access structure is recycled */

    dir_idx_params = (bsDirIdxRecT *) ms_malloc(sizeof(bsDirIdxRecT));

    if(dir_idx_params == NULL)
    {
        sts = ENO_MORE_MEMORY;
        goto idx_create_error;
    }

    dir_idx_params->flags = 0;
    dir_idx_params->idx_bfap = idx_bfap;
    dir_idx_params->index_tag = indexTag;

    dir_bfap->idx_params = (void *) dir_idx_params;

    /* Get memory for the parameters that hang off the index file's
     * access structure. The memory for this will also be released in
     * grab_bsacc when the access structure is recycled.  */

    idx_params = (bsIdxRecT *) ms_malloc(sizeof(bsIdxRecT));

    if(idx_params == NULL)
    {
        sts = ENO_MORE_MEMORY;
        goto idx_create_error;
    }

    idx_params->flags=IDX_INDEX_FILE;
    idx_params->bmt.fname_page=0;
    idx_params->bmt.ffree_page=1;
    idx_params->bmt.fname_levels=0;
    idx_params->bmt.ffree_levels=0;
    idx_params->dir_bfap = dir_bfap;
    idx_params->prune_key_ffree=0;
    idx_params->prune_key_fname=0;

    /* Store these in the index files access structure. */

    idx_bfap->idx_params = (void *) idx_params;

    /* Write the index b-tree info out to the index MCELL */

    sts = bmtr_put_rec( idx_bfap,
                  BMTR_FS_INDEX_FILE,
                  &idx_params->bmt,
                  sizeof(bsIdxBmtRecT),
                  create_ftxH);

    if (sts != EOK)
    {
        if (sts != ENO_MORE_MCELLS)
        {
            domain_panic(create_ftxH.dmnP,
                         "idx_create_index_file: bmtr_put_rec failed");
        }
        goto idx_create_error;
    }

    /* Fill in the undo record */

    undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
    undorec.dir_tag = dir_bfap->tag;
    undorec.action = IDX_CREATE_UNDO;

    ftx_done_u (create_ftxH, FTA_IDX_UNDO_V1,
                  sizeof(idxUndoRecordT), &undorec);

    /* This deserves some explaination. The bmtr_put_rec routine
     * does not supply and undo for the creation of a record. Since
     * it itself starts a subftx, the record could get out to disk at
     * the done of that subftx. Thus the undoing of this record must
     * actually be setup prior to putting it out. In this way the undo
     * routine can attempt to read the record and if successful put in
     * a Nil tag value. If it is not sucessful then the undoing is skipped.
     *
     * If we were to put the record out prior to the done then a small
     * window exits between putting the record out and doing the done.
     * This could lead to the record reaching the disk without the
     * undo routine running.
     */

    sts = bmtr_put_rec( dir_bfap,
                  BMTR_FS_DIR_INDEX_FILE,
                  (void *) &indexTag,
                  sizeof(bfTagT ),
                  parentFtxH);

    if (sts != EOK)
    {
        if (sts != ENO_MORE_MCELLS)
        {
            domain_panic(parentFtxH.dmnP,
                         "idx_create_index_file: bmtr_put_rec failed");
        }
        bs_close(idx_bfap, 0);
        if (dir_idx_params != NULL)
        {
            ms_free(dir_idx_params);
            dir_bfap->idx_params = NULL;
        }
    }

    return(sts);

idx_create_error:

    bs_close(idx_bfap, 0);
    if (dir_idx_params != NULL)
    {
        ms_free(dir_idx_params);
        dir_bfap->idx_params = NULL;
    }
    ftx_fail(create_ftxH);

    return (sts);
}

/*********************************************************************
 * This will be called from msfs_rmdir when the directory is removed.
 * The index file will only be removed when the directory itself is
 * removed. Since the index file is not visible to users, it will be
 * assumed that the directory use count will always match the index
 * file use count.
 *
 * Similarly calls from the index file code will be kept at a high
 * enough level such that the clone processing will be done correctly.
 *
 * This routine will close and mark the index file for deletion. The deletion
 * will actually not take palce until the last close.
 *********************************************************************/

statusT
idx_remove_index_file
(
            bfAccessT *dir_bfap, /* Directory access structure */
            ftxHT ftxH            /* Parent transaction */
            )
{
    bfAccessT *idx_bfap;
    bfTagT index_tag;
    statusT sts;

    /* Get the index file's access pointer from the directory */

    idx_bfap = ((bsDirIdxRecT *) dir_bfap->idx_params)->idx_bfap;

    sts = rbf_delete(idx_bfap,ftxH);

    return(sts);
}

/*********************************************************************
 *
 * This routine will open the index file.
 *
 * The directory's idx_params pointer in its access structure is
 * used as follows:
 *
 *   idx_params = NULL This is the value placed in here when an
 *                access sturcutre is first initialized. It is also
 *                placed there when it is recycled. It indicates that
 *                the file's MCELL should be read to see if an index file
 *                is associtaed with this directory.
 *
 *   idx_params = -1 This is a pointer value that is always invalid. It
 *                indicates that the directory does not have an index
 *                and it's MCELL does not need to be checked. This saves
 *                a read every time the dir is openned.
 *
 *   NOTE: this code does not concern itself with idx_params = -1. It is
 *         tested in a wrapper MACRO prior to calling this function.
 *
 * if the access structure has been recycled and if so then get memory
 * and re-read from disk the info for access the index file. Next it
 * will check to see if the idx_bfap pointer is NULL and if so open
 * the index file. Otherwise this info will already be cached in the
 * access structures.
 *
 * The memory associated with the idx_params will be released when the
 * access strucutre is recyled or released.
 *********************************************************************/

statusT
idx_open_index_file(
    bfAccessT*  dir_bfap,    /* directory's access structure. */
    ftxHT ftxH            /* Parent transaction */
    )
{
    bfTagT index_tag;
    bfAccessT *idx_bfap;
    bsIdxRecT *idx_params;
    bsDirIdxRecT *dir_idx_params;
    statusT sts=EOK;
    struct vnode *nullvp = NULL;

    /* Check to see if the directory access structure passed in still
     * has the index file parameters cached.  */

    if ((bsDirIdxRecT*) dir_bfap->idx_params == NULL)
    {

        /* Read in the tag of the index file from the MCELL record of
         * the directory. Place in the access structure. The
         * dir_context lock should be enough protection for the access
         * structure since it is no longer on the free list (we
         * shouldn't need the bfalock).  */

        sts = bmtr_get_rec( dir_bfap,
                            BMTR_FS_DIR_INDEX_FILE,
                            &index_tag,
                            sizeof( bfTagT )
            );

        if ( (sts != EOK) || (BS_BFTAG_EQL(index_tag,NilBfTag)) )
        {
            /* This directory does not have an associated index.
             * Indicate to future opens that this is a non-indexed dir.
             * This will change only when a index is created or the access
             * strucutre is recycled.
             */
            /* Return an EOK status since no index file is not an error
             * condition. The callers can check the idx_params field to
             * find out if an index was found or not.
             */

            dir_bfap->idx_params = (void *) -1;
            return(EOK);
        }

        MS_SMP_ASSERT(index_tag.num >  0);

        /*
         * Get memory for the index parameters that hang off the
         * directory's access structure. The memory for this will be
         * released in grab_bsacc when the access structure is
         * recycled
         */

        dir_idx_params = (bsDirIdxRecT *) ms_malloc(sizeof(bsDirIdxRecT));

        if (dir_idx_params == NULL)
        {
            return(ENO_MORE_MEMORY);
        }

        dir_idx_params->flags = 0;
        dir_bfap->idx_params = (void *) dir_idx_params;
        dir_idx_params->index_tag = index_tag;
    }

    /* The close of the directory (and index) will put a NULL in the
     * idx_bfap indicating that the index must be re-openned. If it is
     * not NULL then it must already be openned and the association setup.
     */

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    if (idx_bfap == NULL)
    {

        /* Generally the ftxH will be NULL and this call could be
         * replaced by bs_access. But the undo routines need to call
         * this as well and must pass an ftxH in. This avoids a hang
         * in bs_access_one where the index file is in the process
         * of being created and the code doesn't realize we were the
         * creators, and waits for it to transition.
         */

        sts = bs_access( &idx_bfap,
                        ((bsDirIdxRecT *)dir_bfap->idx_params)->index_tag,
                        dir_bfap->bfSetp,
                        ftxH,
                        0,
                        NULLMT,
                        &nullvp);

        if (sts != EOK)
        {
            /* This could fail due to lack of memory */
            ms_free(dir_bfap->idx_params);

            /* The index file is deleted before the directory so we may
             * have caught it in that state.  The cluster must be
             * allowed to access the dir it had open or it will crash.
             */
            if (sts == ENO_SUCH_TAG && dir_bfap->bfState == BSRA_DELETING)
            {
                dir_bfap->idx_params = (void *) -1;
                return(EOK);
            }

            dir_bfap->idx_params = NULL;
            return(sts);
        }

        /* Check and see if the index file's access has been recycled and
         * if so re-read in the index file parameters.  */
    }

    if (idx_bfap->idx_params == NULL)
    {
        idx_params =  (bsIdxRecT *) ms_malloc(sizeof(bsIdxRecT));

        if(idx_params == NULL)
        {
            ms_free(dir_bfap->idx_params);
            dir_bfap->idx_params = NULL;
            bs_close(idx_bfap, 0);
            return(ENO_MORE_MEMORY);
        }

        sts = bmtr_get_rec(idx_bfap,
                           BMTR_FS_INDEX_FILE,
                           &idx_params->bmt,
                           sizeof(bsIdxBmtRecT)
            );

        if (sts != EOK)
        {
            ms_free(dir_bfap->idx_params);
            dir_bfap->idx_params = NULL;
            bs_close(idx_bfap, 0);
            domain_panic(ftxH.dmnP,"idx_open_index_file: bmtr_get_rec failed");
            return(sts);
        }

        idx_bfap->idx_params = (void *) idx_params;
        idx_params->flags = IDX_INDEX_FILE;
        idx_params->prune_key_ffree=0;
        idx_params->prune_key_fname=0;

    }
    /* We must always set the dir_bfap into the idx_params for the index
     *file */

    ((bsIdxRecT *)idx_bfap->idx_params)->dir_bfap = dir_bfap;
    ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap = idx_bfap;

    return(sts);
}


/*********************************************************************
 *
 * This routine will close the index file. This will only be called on
 * the last close of the directory. This avoids a directory being
 * openned multiple times and then an index file being created
 * later. Each close of the directory must not attempt to close the
 * index. Thus by only closing on the last close of the directory we
 * are assured of coherency.
 *
 * We must also mark the directory as being closed so that the next
 * open also opens the index file. This will be done by setting
 * the idx_bfap in the directory's access structure to NULL. The
 * idx_params will only be deallocated when an access strucutre is
 * recycled (in grab_bsacc) or released.
 *
 * Clone processing will work without needing to associate the original
 * dir and index together. Only the clone dir and index will need to be
 * associated.
 *
 *********************************************************************/

void
idx_close_index_file(
    bfAccessT*  dir_bfap,     /* index file's access structure */
    idxCloseActionT action     /* Close circumstances */
    )
{
    bfAccessT *idx_bfap;
    statusT sts;

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    MS_SMP_ASSERT(((bsIdxRecT *) idx_bfap->idx_params)->dir_bfap == dir_bfap);

    /* The directory is being removed. We need to break the association
     * between the two. This takes care of the POSIX standard that the
     * . and .. entries must be removed even if the dir is still open.
     * This is the case where you cd into the dir and then remove it.
     * The . and .. are not removed from the index so we don't want any
     * lookups finding them.
     */
    switch (action)
    {

    case IDX_RECOVERY :

        /* In the case of recovery we must rip out the cached index
         * parameters and force the next openner of the index to
         * reread them from the BMT record. This is because during a
         * previous call to undo_idx_opx we could have changed the
         * on-disk BMT record. Fortunately this can only happen during
         * recovery since only splitting and prunning can cause this
         * record to change and these operations are never failed.  
         */
        
        ms_free(idx_bfap->idx_params);
        idx_bfap->idx_params = NULL;
        ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap = NULL;
        break;

    case IDX_NORMAL_CLOSE :
        /* Indicate to any future dir openners to also open the index file */
	if(idx_bfap->idx_params != NULL)
		((bsIdxRecT *)idx_bfap->idx_params)->dir_bfap = NULL;
        ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap = NULL;
        break;

    case IDX_REMOVING_DIR :

        ms_free(dir_bfap->idx_params);
        dir_bfap->idx_params = (void *) -1;

        /* Currently the only way to remove and index is to 
         * also remove the associated directory. Thus we will
         * account for the quota's at the last close of the
         * directory. This is because of the difficulties of
         * the index using the directories context structure
         * for the call to chk_blk_quota. Since the directory
         * is being removed we can not guarantee its existence
         * at the last close of the index (ie the index is held
         * open via .tags). Thus we will include the quota blocks
         * in the dir's access structure so it can update the 
         * quota's at last close.
         */

        dir_bfap->idxQuotaBlks = 
            (long)idx_bfap->xtnts.allocPageCnt * ADVFS_PGSZ_IN_BLKS;

        break; 
    }

    /* Now we close the index file */

    sts = bs_close(idx_bfap, 0);

    if (sts != EOK)
        domain_panic(dir_bfap->dmnP,"idx_close_index_file: bs_close failed");

    return;
}

/*********************************************************************
 * This routine will perform a binary search of the passed in node for
 * the passed in key and return the index of the leftmost matching key
 * (leftmost is only relative if collisions are detected) or the index
 * of the first element that is greater than or equal to the passed in
 * key.
 *
 * If the node is a leaf node then the returned index corresponds to
 * the slot that this search key should occupy.  If it is not a leaf
 * node then the index corresponds to the slot containing the location
 * of the next node to be searched. Each slot represents those values
 * that are less than or equal to the search key.
 *
 * Note that there is no search key associated with the last slot of
 * the last node of a given level(expcept for the leaf level 0). This
 * slot represents all values greater than any of the other slots. Thus
 * the value in that slot is not valid and should not be looked at.
 *
 * Assumes that the node is either referenced or pinned.
 *********************************************************************/

uint32T
idx_bsearch_int(
    idxNodeT *node,      /* Node to binary search */
    ulong search_key,    /* value to search for */
    int last_node        /* flag indicating if last node
                            of an internal level */
    )
{
    int start;
    int end;
    int i;
    ulong cmp;

    start = 0;
    MS_SMP_ASSERT(node->total_elements <= IDX_MAX_ELEMENTS);
    if (node->total_elements == 0)
    {
        MS_SMP_ASSERT(node->page_right < 0);
        return(0);
    }

    if (last_node)
    {
        if (node->total_elements == 1)
        {
           return(0);
        }
        end = node->total_elements-2;
    }
    else end = node->total_elements-1;

    while(start <= end)
    {
    /* Start a binary search in the middle of the array. */
        i=(start+end)>>1;
        cmp = node->data[i].search_key;
        if(search_key == cmp) break;
        if (search_key < cmp) end=i-1;
        else start=i+1;
    }

    if (cmp < search_key) i++;
    else
    {
        /* Pull back to the leftmost collision */
        while((i> 0)&&(search_key == node->data[i-1].search_key))
        {
            i--;
        }
    }

    return(i);
}

/*********************************************************************
 * This will return the 64 bit hash value of the passed in
 * filename. The hash function used is known as the ELF hash function
 * developed at AT&T bell labs.
 *
 * Some experimentation was done by hashing filenames on production
 * servers and observing the number of collisions as well as
 * generating random character names and observing collisions. In
 * either case the collisions were under .005 percent of the number of
 * hash values
 *********************************************************************/

ulong
idx_hash_filename_int(
    char *filename  /*pointer to the filename to hash */
    )
{
    unsigned long h = 0,g;

    while( *filename)
    {
        h = (h << 4) + *filename++;
        g = h & 0xF000000000000000;
        if (g) h ^= g >> 56;
        h &= ~g;
    }

    return (h<<8);/* save a byte for future sequence numbering */
}

/*********************************************************************
 * This routine gets free pages (disk blocks) for the index file
 * itself. The minimum allocation size is one page. This allows
 * allocations and deallocations to take place without having to
 * maintain a free list or suffer fragmentation with in the index
 * file.
 *
 * Since the index file is not visible to the user, we will not be
 * very aggressive in filling holes within the file. If the entire
 * request can fit in a hole then will will use it otherwise the file
 * will be extended. The pages must be pinned individually and will be
 * returned in the passed in array.
 *
 * Since this routine is only called for splitting a node and only one
 * node per level will be split, the maximum number of pages ever
 * requested will be equal to the maximum number of levels in the
 * b-tree plus one. Thus we will use an array to pass back the pinned
 * pages.
 *
 * If storage can not be obtained we will return status to our
 * caller. The caller will then decide whether to fail the
 * transaction. This specifically addresses the case when we are
 * deleting a file and adding free space back to the free space tree,
 * which could cause a node split and require storage via this
 * routine. Obviously we can not fail a delete of a file because there
 * is not enough space (see idx_directory_insert_space).
 *
 *********************************************************************/

statusT
idx_index_get_free_pgs_int(
    bfAccessT       *idx_bfap,  /* Index access structure ptr */
    uint32T         count,      /* Number of pages required */
    idxPinnedPgsT   *pptr,       /* out - pages we obtained */
    ftxHT           ftxH,       /* Parent transaction */
    int 	forceflag      /* sometimes we need to ignore quotas */
    )
{
    uint32T i;
    uint32T holePg,newPg;
    uint32T holePgCnt;
    statusT sts;

    /* Extend the index file filling holes if possible */

    if (count > IDX_MAX_BTREE_LEVELS)
    {
        /* We have hit the pinned pages per sub-ftx limit. This
         * requeset means we want to split the tree past the
         * largest allowable size. This restricition exists becasue
         * of nummber of pinned pages per sub ftx not due to b-tree
         * limitaions. It also means the caller has over 4 billion
         * files in this directory! */

        return (ENO_MORE_BLKS);
    }

    /* Normally one should call x_load_imm_xtnt_map here but 
     * that is only necessary in case the file was truncated. Otherwise
     * the maps were brought in via bs_map_bf. Since index files can not
     * be truncated we only need to get the xtntMap lock.
     * 
     * It is not necesary to get the XTNTMAP_LOCK_READ here because
     * all callers of this function currently hold the MIGTRUNC lock
     * exclusively. This is enough to prevent the xtntmap from changing
     * out from under us.
     */
 
    imm_get_first_hole( &idx_bfap->xtnts, &holePg, &holePgCnt );

    if (holePgCnt < count)
    {
        /* Extend the file, no holes were found.*/
        newPg = idx_bfap->nextPage;
    }
    else newPg = holePg;

    sts = change_quotas(((bsIdxRecT *)idx_bfap->idx_params)->dir_bfap->bfVp,
                        0, (long)count * ADVFS_PGSZ_IN_BLKS, NULL,
                        u.u_cred, forceflag ? FORCE : 0, ftxH);
    if (sts != EOK)
        return(sts);

    sts = rbf_add_stg( idx_bfap, newPg, count, ftxH, 1 );

    if (sts != EOK)
    {

        /* Unavailability of storage may not consitiute failing the
         * entire transaction. So just the current ftx and below then
         * return the status to our caller and let them decide what to
         * do. Note this will undo the quota increment above */

        change_quotas(((bsIdxRecT *)idx_bfap->idx_params)->dir_bfap->bfVp,
                      0, -(long)count * ADVFS_PGSZ_IN_BLKS, NULL,
                      u.u_cred, 0, ftxH);

        idxStats.get_free_pgs_failed++;

        return(sts);
    }

    /* Put the pages in the array for pinning after all the
     * storage has been obtained. This is necessary in case of an
     * error because we can not remove a page that has been pinned
     * so we most postpone pinning until after we are sure we wont
     * fail.  */

    for (i=0;i<count;i++)
    {
        /* Bring in each page via:*/

        pptr[i].pgno=newPg;
        sts = rbf_pinpg( &pptr[i].pgref,
                         (void *)&pptr[i].pgptr,
                         idx_bfap,
                         newPg,
                         BS_OVERWRITE,
                         ftxH );
        if (sts!= EOK)
        {
            domain_panic(ftxH.dmnP,
                         "idx_index_get_free_pgs_int: rbf_pinpg failed");
            return(sts);
        }
        newPg++;
    }

    return(sts);
}

/**********************************************************************
 * This routine will split a node into two nodes. The storage for the
 * new nodes is passed in. The original node will be split in
 * half. This will insure that the leftmost node will always remain
 * the leftmost node. The element to be inserted will be incorporated
 * into the split to avoid copying twice.  For efficiency bcopy will
 * be used since it will deal with overlapping src and dst.
 *********************************************************************/

void
idx_split_node_int(
    ulong        search_key,    /* value to be inserted */
    ulong        value,        /* value to be inserted */
    ulong        index,         /* where to put it before split */
    idxNodeT    *src_node_ptr,  /* node to be split */
    idxNodeT    *dst_node_ptr,  /* node to receive split*/
    rbfPgRefHT   src_pgref,     /* page ref */
    rbfPgRefHT   dst_pgref      /* page ref */
    )
{
    uint32T idx_src, idx_dst, idx_middle;

    /* Determine if the element to be inserted lies in the first half
     * of the node.  */

    MS_SMP_ASSERT(src_node_ptr->total_elements == IDX_MAX_ELEMENTS);

    idx_middle = (IDX_MAX_ELEMENTS)>>1;

    if (index <= idx_middle)
    {
        idx_src = idx_middle;
        idx_dst = (IDX_MAX_ELEMENTS) - idx_middle;

        /* Pin the destination nodes records.*/

        rbf_pin_record(dst_pgref,
                       (char *)&(dst_node_ptr->data[0]),
                       (idx_dst * sizeof(idxNodeEntryT)));

        /* Move the later half over to the destination node.*/

        bcopy((char *)&(src_node_ptr->data[idx_middle]),
              (char *)&(dst_node_ptr->data[0]),
              (idx_dst * sizeof(idxNodeEntryT)));

        /* Update the element count in the destination node and pin
         * it.  */

        RBF_PIN_FIELD( dst_pgref,  dst_node_ptr->total_elements);
        dst_node_ptr->total_elements = idx_dst;
        MS_SMP_ASSERT(dst_node_ptr->total_elements < IDX_MAX_ELEMENTS);

        /* Insert the new element by moving successive elements down
         * then pin the records (including the inserted element).  */

        rbf_pin_record(src_pgref,
                       (char *)&(src_node_ptr->data[index]),
                       (idx_src - index + 1)*sizeof(idxNodeEntryT));

        bcopy((char *)&(src_node_ptr->data[index]),
              (char *)&(src_node_ptr->data[index+1]),
              (idx_src - index)*sizeof(idxNodeEntryT));

        src_node_ptr->data[index].search_key=search_key;
        src_node_ptr->data[index].loc.any_field = value;

        /* Update the new size in the record and pin it */
        RBF_PIN_FIELD( src_pgref, src_node_ptr->total_elements);
        src_node_ptr->total_elements = idx_src+1;
        MS_SMP_ASSERT(src_node_ptr->total_elements < IDX_MAX_ELEMENTS);
    }

    /* If the element to be inserted lies in the second half then move
     * the half in two steps inserting the new element in between. Pin
     * the whole thing afterwards.  */

    else
    {
        /* Pin the array and sizes. */

        idx_dst = idx_middle;
        idx_src = (IDX_MAX_ELEMENTS) - idx_middle;

        rbf_pin_record (dst_pgref,
                        (char *)&(dst_node_ptr->data[0]),
                        (idx_dst+1)*sizeof(idxNodeEntryT));

        RBF_PIN_FIELD( dst_pgref, dst_node_ptr->total_elements);
        RBF_PIN_FIELD( src_pgref, src_node_ptr->total_elements);

        bcopy((char *)&(src_node_ptr->data[idx_src]),
              (char *)&(dst_node_ptr->data[0]),
              (index - idx_src)*sizeof(idxNodeEntryT));

        dst_node_ptr->data[index - idx_src].search_key = search_key;

        dst_node_ptr->data[index - idx_src].loc.any_field= value;

        if (index < (IDX_MAX_ELEMENTS))
        {
            bcopy((char *)&(src_node_ptr->data[index]),
                  (char *)&(dst_node_ptr->data[index - idx_src + 1]),
                  ( (IDX_MAX_ELEMENTS)- index)*sizeof(idxNodeEntryT));
        }


        /* Update the sizes */

        dst_node_ptr->total_elements = idx_dst+1;
        src_node_ptr->total_elements = idx_src;
        MS_SMP_ASSERT(dst_node_ptr->total_elements < IDX_MAX_ELEMENTS);
        MS_SMP_ASSERT(src_node_ptr->total_elements < IDX_MAX_ELEMENTS);
    }

    return;
}

/**********************************************************************
 * This routine is the outer wrapper for the insert routine. It will
 * start a sub transaction.  This routine must also handle the special
 * case of the root node splitting.  Since the internal insert routine
 * is recursive it is generic and does not handle this special case.
 *
 * It is assumed that the filename we are inserting has previously
 * been determined to not exist and the proper locks are held to
 * protect its lack of existence (the dir context lock).
 *
 * All b-tree modifications are done completely within this sub
 * transaction. Thus if the subtransaction completes and is later
 * failed we can leave the b-tree split and just remove the filename
 * that was inserted.
 *
 * A hint is stored away in the index params off the index access struct.
 * This hint is setup during the lookup. If it matches the search key
 * we can avoid walking the b-tree and go directly to the leaf node.
 * The hint would not be set if the node could split.
 *
 * NOTE:
 *      Once we have successfully inserted into the b-tree we disable
 *      the undoing of any sub-ftx's below us. This is because the b-tree
 *      has been properly setup and is really indepdent of the filename
 *      insertion and can remain intact with only the undoing of
 *      the filename. In other words if the b-tree split we can leave
 *      it split.
 *********************************************************************/

statusT
idx_insert_filename(
    bfAccessT *dir_bfap,   /* Directory's access structure */
    struct nameidata *ndp, /* pointer to nameidata */
    int page,              /* Page to insert onto */
    long count,            /* Page offset to insert to */
    ftxHT  parentFtx       /* Parent Ftx */
    )
{
    ulong search_key;
    ulong root_page;
    uint32T level;
    ulong location;
    bfAccessT *idx_bfap;
    ftxHT ftxH;
    int split_count;
    uint32T insert;
    idxNodeT *new_root;
    idxPinnedPgsT pinned_pages[IDX_MAX_BTREE_LEVELS];
    idxUndoRecordT undorec;
    statusT sts;

    /* Hash the filename to get the search key to insert into the
     * index b-tree.  */

    search_key = idx_hash_filename_int(ndp->ni_dent.d_name);

    /* Get the idx_file's access structure and setup for recursive
     * call.  */

    idx_bfap = ((bsDirIdxRecT *) dir_bfap->idx_params)->idx_bfap;

    /* See if the hint can be used, and if so use the leaf node page
     * instead of the root, thus avoiding having to traverse the
     * tree.
     *
     * There is no point using the hint if the tree has only
     * one node (level =0). Furthermore, this takes care of
     * initizing the hint. A rename can do 2 nameis on a non-indexed
     * directory and then the insert causes the dir to get an index
     * The remove will then be called with an uninitialized hint.
     * By not usinging this early we'll get a chance to init it. */

    level = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels;
    if ((level > 0) && (long)ndp->ni_hint >= 0)
    {
        root_page = (long)ndp->ni_hint;
        level = 0;
        ndp->ni_hint = (caddr_t)-1;
    }
    else
    {
        root_page =((bsIdxRecT *) idx_bfap->idx_params)->bmt.fname_page;
    }

    location = IDX_GET_ADDRESS(page,count);
    split_count = -1; /* indicates to the recursive routine that
                         we are the first caller */

    /* Start the sub-transaction */

    sts = FTX_START_N( FTA_IDX_UNDO_V1,&ftxH, parentFtx,
                       dir_bfap->dmnP, 0);
    if (sts != EOK)
    {
        domain_panic(parentFtx.dmnP,"idx_insert_filename: ftx_start failed");
        return(sts);
    }

    /* Call the recursive insertion routine */

    sts = idx_insert_filename_int(root_page,
                                  level,
                                  &search_key,
                                  &location,
                                  idx_bfap,
                                  &insert,
                                  &split_count,
                                  pinned_pages,
                                  ftxH);
    if (sts != EOK)
    {
        ftx_fail(ftxH);
        return(sts);
    }

    if (insert)
    {
        /* An insert value of TRUE indicates that the b-tree split all
         * the way up and a new root node must be created. The newpage
         * number is in location and search_key contains the new
         * search_key. The pinned_pages array has the page already
         * pinned for us.
         *
         * Increment level to indicate that the b-tree has increased
         * in size.  Place the new elements in to the new root node and
         * pin the record.  It is not necessary to update the search
         * key of the last element of a root node since this contains all
         * values greater than the previous element.  */

        level = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels+1;

        /* Pin the records. */

        new_root = pinned_pages[level].pgptr;

        rbf_pin_record (pinned_pages[level].pgref,
                        &new_root->data[0],
                        2*sizeof(idxNodeEntryT));

        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->total_elements);

        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->page_right);

        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->sib.leftmost);
        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->reserved[0]);

        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->reserved[1]);

        new_root->total_elements=2;
        new_root->page_right=-1;
        new_root->sib.leftmost=TRUE;
        new_root->reserved[0]=0;
        new_root->reserved[1]=0;
        new_root->data[0].loc.node_page = root_page;
        new_root->data[1].loc.node_page = location;
        new_root->data[0].search_key = search_key;

        /* Place the update root node information in the record off
         * the directory's access structure. We must push this out to
         * the BMT now since we are in a subtransaction that can be
         * failed and don't want to undo this.  */

        ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_page =
            pinned_pages[level].pgno;
        ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels++;

        sts = bmtr_put_rec(idx_bfap,
                     BMTR_FS_INDEX_FILE,
                     &((bsIdxRecT *)idx_bfap->idx_params)->bmt,
                     sizeof(bsIdxBmtRecT),
                     ftxH);
        if (sts != EOK)
        {
            /*
             * We can not call ftx_fail here since we have pinned
             * records. We will just call ftx_done and return the
             * status up to our caller.
             */

            if (sts != ENO_MORE_MCELLS)
            {
                domain_panic(ftxH.dmnP,
                             "idx_insert_filename: bmtr_put_rec failed");
            }
            ftx_done_n(ftxH,FTA_IDX_UNDO_V1);
            return(sts);
        }
    }

    /* Set up the undo record. */

    undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
    undorec.dir_tag = dir_bfap->tag;
    undorec.nodeEntry.search_key = idx_hash_filename_int(ndp->ni_dent.d_name);
    undorec.nodeEntry.loc.dir_offset =
        IDX_GET_ADDRESS(page,count);
    undorec.action = IDX_INSERT_FILENAME_UNDO;

    /* We have reached a point now where it is safe to stop all
     * undo's below us. The b-tree is in a consistent state and
     * does not need to be undone if this ftx is failed. We will
     * provide our own undo routine to remove the filename only
     * NOTE: This will not disable the undo for this current ftx.
     */

    ftx_special_done_mode ( ftxH, FTXDONE_SKIP_SUBFTX_UNDO);
    ftx_done_u( ftxH,
                FTA_IDX_UNDO_V1,
                sizeof(idxUndoRecordT),
                (void *) &undorec);

    return(sts);
}

/*******************************************************************
 * This routine will be called for the filename tree.
 *
 * This routine will insert a data element in the leaf node of the
 * b-tree. The routine will recursively call itself until the leaf
 * node is reached. If a node fills up it will be split and a new node
 * created.
 *
 * In order for this routine to be under transaction control, all
 * resources need to be obtained up front prior to pinning any
 * records. Furthermore this will need to be within a sub-transaction
 * and have an undo routine.
 *
 * At each level of the b-tree it will be determined whether a new
 * node will be needed if the leaf node were to split. This is the
 * case only if the current node is full. As we traverse down the tree
 * any non-full node we encounter will cause the split_count to reset.
 *
 * Once the leaf node is reached the file will be extended for the
 * number of pages/nodes we need. Each child node will use the
 * resources it needs and return the remaining resources up to its
 * caller until the root is reached.
 *
 ******************************************************************/

statusT
idx_insert_filename_int(
    uint32T        node_page,     /* in - starting node page*/
    ulong          level,         /* caller's level in the tree */
    ulong          *search_key,   /* in - key to insert
                                     out - key to insert*/
    ulong          *offset,       /* in - location to store in directory
                                     out - location to store in node*/
    bfAccessT      *idx_bfap,     /* b-tree access structure */
    uint32T        *insert,        /* boolean indicating if an insertion
                                     to this node needs to be done */
    int            *split_count,  /* number of pages needed for split */
    idxPinnedPgsT  *pptr,          /* pin page parameters */
    ftxHT           ftxH          /* parent transaction */
    )
{
    uint32T index;
    idxNodeT *node_ptr;
    rbfPgRefHT pgref;
    ulong temp_page;
    statusT sts;

    /* Pin the page into memory. Since we don't know at this point
     * whether the page is going to be modified due to a split, just
     * pin it instead of reffing it. If we never do an rbf_pin_record
     * then the page will not be marked dirty.  */

    sts = rbf_pinpg( &pgref, (void *) &node_ptr, idx_bfap,
                     node_page, BS_NIL,
                     ftxH);
    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_insert_filename_int: rbf_pinpg failed");
        return(sts);
    }

    if (node_ptr->total_elements > IDX_MAX_ELEMENTS)
    {
        domain_panic(ftxH.dmnP,"idx_insert_filename_int: Bad node count");
        return(E_RANGE);
    }

    /* Search the current node for the search_key */

    index = idx_bsearch_int(node_ptr,
                            *search_key,
                            ((node_ptr->page_right<0)&&(level>0)));

    MS_SMP_ASSERT((level==0) || (index < node_ptr->total_elements));

    /* Check if this node would need to be split if the node below it
     * were split and if so increment the split_count, if not reset
     * the split_count.  Recursively call insert again if this is not
     * a leaf node.  */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);

        if (node_ptr->total_elements == IDX_MAX_ELEMENTS)
        {
            /* If this is the first caller (split_count < 0) and we
             * may need to split then ask for an extra page since our
             * caller will need to create a new root node.  */

            if (*split_count < 0) *split_count=1;
            /* get and extra for root */
            (*split_count)++;
        }
        else *split_count=0;

        /* There is a potential of pinning as many as 9 pages within
         * this transaction (this in fact will occur if the tree
         * reaches 5 levels).Each recursive call could be made into a
         * sub-transaction but the lowest ftx will pin all the pages
         * and pass them back up. Thus an ftx_done can not happen
         * until those pages are used since the done will cause the
         * pages to become un-pinned
         *
         * The work around is to bump the max allowed pinned pages
         * per sub-transaction from 7 to 9 or rework my
         * idx_index_get_free_pgs_int to not pin the pages and start
         * a sub-transaction for each recursive call.  */

        sts = idx_insert_filename_int (node_ptr->data[index].loc.node_page,
                                       level-1,
                                       search_key,
                                       offset,
                                       idx_bfap,
                                       insert,
                                       split_count,
                                       pptr,
                                       ftxH);
        if (sts != EOK)
            return(sts);

        /* If insert is true we must update the nodes ranges to
         * reflect the fact that the node below us has split.  A new
         * range slot will be added to this node and the original slot
         * (index) will be rippled up with the new offset and the
         * new slot gets the passed back search_key. The split caused
         * the existing node to now have a new search key and the new
         * node to have the original search key.  */

        if (*insert)
        {
            temp_page = node_ptr->data[index].loc.node_page;
            node_ptr->data[index].loc.node_page = *offset;
            *offset = temp_page;
        }
    }
    else
    {

        /* This is the leaf node so get any resources that will be
         * needed to be passed back up, before we pin any records and
         * are still able to fail.  */

        *insert=TRUE; /* we know at least the leaf must perform
                         an insertion */

        if (node_ptr->total_elements ==  IDX_MAX_ELEMENTS)
        {
            if (*split_count < 0) *split_count=1;
                           /* We are the root node get two */
            (*split_count)++;
            sts=idx_index_get_free_pgs_int(idx_bfap,
                                           *split_count,
                                           pptr,
                                           ftxH, 0);
            if (sts != EOK)
                return(sts);
        }
        else *split_count=0;
    }

    /* Check if an insertion needs to be done. Split the node if it
     * needs it and insert the new element.  */

    if (*insert)
    {
        if (node_ptr->total_elements ==  IDX_MAX_ELEMENTS)
        {
            idx_split_node_int(
                               *search_key,
                               *offset,
                               index,
                               node_ptr,           /*src*/
                               pptr[level].pgptr, /*dst*/
                               pgref,             /*src*/
                               pptr[level].pgref  /*dst*/
                               );

            /* Update the return arguments so the caller can insert
             * our new node into its search array. The offset refers
             * to the new node while the search_key refers to the old
             * node.  */

            *offset = pptr[level].pgno;
            *search_key =
                node_ptr->data[node_ptr->total_elements - 1].search_key;

            /* We maintain a singly linked list amongst the nodes of
             * the tree.  Set up the new split nodes to point
             * right. The leftmost bit will always be FALSE since we
             * always split to the right.  */

            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->sib.leftmost);
            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->page_right);
            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->reserved[0]);
            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->reserved[1]);

            RBF_PIN_FIELD(pgref,node_ptr->page_right);

            pptr[level].pgptr->sib.leftmost=FALSE;
            pptr[level].pgptr->page_right=node_ptr->page_right;
            pptr[level].pgptr->reserved[0]=0;
            pptr[level].pgptr->reserved[1]=0;

            node_ptr->page_right = pptr[level].pgno;

            /* Indicate to the caller that an insertion must be done.  */

            *insert=TRUE;
        }
        else
        {
            MS_SMP_ASSERT((level == 0) ||
                          (index < node_ptr->total_elements));

            /* Insert the element into the node using bcopy then pin
             * the record.  Bcopy will handle the overlap.  */

            IDX_INC_TOTAL(pgref,node_ptr);

            rbf_pin_record(pgref,
                           &node_ptr->data[index],
                           (node_ptr->total_elements-index)
                           *(sizeof(idxNodeEntryT)));

            if (index < node_ptr->total_elements-1)
                bcopy((char *)&node_ptr->data[index],
                      (char *)&node_ptr->data[index+1],
                      (node_ptr->total_elements-index-1)
                      *(sizeof(idxNodeEntryT)));

            node_ptr->data[index].loc.node_page = *offset;
            node_ptr->data[index].search_key = *search_key;

            *insert=FALSE;
        }
    }
    else
    {
        rbf_deref_page(pgref,BS_CACHE_IT);
    }
    return(sts);
}

/*******************************************************************
 * This is the external wrapper for removing a filename from the index
 * file.  This will call the recursive removal routine.  This routine
 * will start a sub-transaction.
 *******************************************************************/

statusT
idx_remove_filename(
    bfAccessT *dir_bfap, /* Directory's access structure */
    struct nameidata *ndp,
    int page,            /* Directory page where filename lives */
    long count,          /* Offset on page where filename lives */
    ftxHT  parentFtx     /* Parent ftx */
    )
{
    ulong search_key;
    uint32T level;
    bfAccessT *idx_bfap;
    ulong location,root_page;
    ftxHT  ftxH;
    idxUndoRecordT undorec;
    statusT sts;
    int using_hint=0,found=0;

    /* Hash the filename to get the search key to remove from the
     * index b-tree. */

    search_key = idx_hash_filename_int(ndp->ni_dent.d_name);

    /* Get the idx_file's access structure and setup for recursive
     * call.  */

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    /* See if the hint can be used, and if so use the leaf node page
     * instead of the root, thus avoiding having to traverse the
     * tree
     *
     * There is no point using the hint if the tree has only
     * one node (level =0). Furthermore, this takes care of
     * initizing the hint. A rename can do 2 nameis on a non-indexed
     * directory and then the insert causes the dir to get an index
     * The remove will then be called with an uninitialized hint.
     * By not usinging this early we'll get a chance to init it. */

    level = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels;
    if ((level > 0) && (long)ndp->ni_hint >= 0)
    {
        root_page = (long)ndp->ni_hint;
        level = 0;
        ndp->ni_hint = (caddr_t)-1;
        using_hint = TRUE;
    }
    else
    {
        root_page =((bsIdxRecT *) idx_bfap->idx_params)->bmt.fname_page;
    }

    location = IDX_GET_ADDRESS(page,count);

    /* Start the subtransaction */

    sts = FTX_START_N( FTA_NULL, &ftxH, parentFtx, dir_bfap->dmnP, 0);
    if (sts != EOK)
    {
        domain_panic(parentFtx.dmnP,"idx_remove_filename: ftx_start failed");
        return(sts);
    }

    /* Call the recursive removal routine */

    do
    {
        sts=idx_remove_filename_int(root_page,
                                    level,
                                    search_key,
                                    location,
                                    idx_bfap,
                                    &found,
                                    ftxH);
        if (sts != EOK)
        {
            ftx_fail(ftxH);
            return(sts);
        }

        if(!found)
        {
            if(!using_hint)
            {
                /* Normally this would be a domain panic situation but this
                 * could be called from a specialty routine trying to "fix"
                 * the domain. It will be the callers responsibilty to deal
                 * with the error
                 */

                ftx_fail(ftxH);
                idxStats.remove_file_failed++;
                return(I_FILE_NOT_FOUND);
            }
            root_page =((bsIdxRecT *) idx_bfap->idx_params)->bmt.fname_page;
            level = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels;
            using_hint=FALSE;
        }
    } while (!found);

    /* Set up the undo record */

    undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
    undorec.dir_tag = dir_bfap->tag;
    undorec.nodeEntry.search_key = search_key;
    undorec.nodeEntry.loc.dir_offset = location;
    undorec.action = IDX_REMOVE_FILENAME_UNDO;

    /* The root done routine is our pruning routine. Lead it to the
     * node that needs pruning via the search_key.  */

    ftx_done_u( ftxH,
                FTA_IDX_UNDO_V1,
                sizeof(idxUndoRecordT),
                (void *) &undorec);

    /* Check to see if the removal triggered a node to need pruning and if
     * start one up!.
     */

    if (((bsIdxRecT *)idx_bfap->idx_params)->prune_key_fname)
    {
        /* Don't bother checking for status. Let the caller
         * fail the transaction if necessary */

        sts=idx_prune_start(idx_bfap,parentFtx);
    }

    return(sts);
}

/*******************************************************************
 * This routine removes the specified element from the filename
 * b-tree. It is assumed that a previous lookup guarantees that the
 * element to be removed exists in the index file. It is also assumed
 * that the directory's context lock is held.
 *
 * This accepts a search_key and a file offset. These will be searched
 * for and removed from the index file. These values will need to have
 * been obtained via a previous lookup and protected from changing via
 * the dir context lock. Since the B-tree being searched allows
 * collisions, the offset will be used to uniquefy the element.
 *
 * This routine will recursively call itself reffing nodes until the
 * leaf node is reached at which time the page will be pinned instead
 * of reffed. The undo record will be setup to insert the removed
 * entry back into the tree if the transaction is failed.
 *
 * The undo of this routine must insure that no sub-transactions are
 * started and no storage is obtained. That is guaranteed because the
 * removed entries space in the node will not be reused unless the
 * root transaction that called this routine is finished (which would
 * not be true if a ftx_fail is called). Furthermore the space will
 * not be pruned unless the root done transaction is called in which
 * case the root transaction would have been committed and an ftx_fail
 * would not occur as well.
 *******************************************************************/

statusT
idx_remove_filename_int(
    uint32T     page_to_search,       /* node's page number */
    ulong      level,                /* level of b-tree calling */
    ulong      search_key,           /* value to search for */
    ulong      offset,               /* Uniquifier */
    bfAccessT  *idx_bfap,             /* index file access struct */
    int        *found,                /* indicates if file was found */
    ftxHT      ftxH                  /* Parent ftx */
    )
{
    idxNodeT *node_ptr;
    rbfPgRefHT rbfpgref;
    bfPageRefHT bfpgref;
    uint32T index;
    statusT sts;

    /* If we are not at the leaf node then ref the page otherwise pin
     * it since we will be writing it back out.  */

    if (level > 0)
    {
        sts = bs_refpg (&bfpgref,
                        (void *) &node_ptr,
                        idx_bfap,
                        page_to_search,
                        BS_NIL);
    }
    else
    {
        sts = rbf_pinpg( &rbfpgref,
                         (void *) &node_ptr,
                         idx_bfap,
                         page_to_search,
                         BS_NIL,
                         ftxH);
    }

    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_remove_filename_int: rbf_pinpg failed");
        return(sts);
    }

    if (node_ptr->total_elements > IDX_MAX_ELEMENTS)
    {
        domain_panic(ftxH.dmnP,"idx_remove_filename_int: Bad node count");
        if (level > 0)  /* caller handles unpin but we must do unref */
            bs_derefpg(bfpgref,BS_CACHE_IT);
        return(E_RANGE);
    }

    /* Locate the index of the search_key */

    index = idx_bsearch_int(node_ptr,
                            search_key,
                            ((node_ptr->page_right<0)&&(level>0)));

    MS_SMP_ASSERT((level==0) || (index < node_ptr->total_elements));

    /* If we are not at the leaf node then recursively call ourselves.  */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);
        sts=idx_remove_filename_int( node_ptr->data[index].loc.node_page,
                                     level-1,
                                     search_key,
                                     offset,
                                     idx_bfap,
                                     found,
                                     ftxH);
    }
    else
    {
        /* We are at the leaf node so locate the element to be
         * removed. Since collisions are allowed check the offset to
         * make sure this is it.  Once found remove the entry from the
         * node and pin it.
         *
         * idx_bsearch_int leaves us pointing at the leftmost entry if
         * there are collisions, so start checking to the right for
         * the element to be deleted. Since we maintain a singly
         * linked list read in the next node if necessary and
         * continue.
         *
         * Since collisions could have existed and then were removed
         * we may be pointing at the end of a node. This is not wrong
         * it's just that the collisions are on the other node so we
         * will just start there.  */

        while ((index == node_ptr->total_elements) ||
               (node_ptr->data[index].search_key == search_key))
        {

            /* If we're at the end of the node then pin the next page
             * and start with it.  */

            if (index == node_ptr->total_elements)
            {
                if(node_ptr->page_right < 0)
                {
                    *found=FALSE;
                    return(EOK); /* see comment below */
                }

                rbf_deref_page(rbfpgref, BS_NIL);
                sts=rbf_pinpg(&rbfpgref,
                           (void *) &node_ptr,
                          idx_bfap,
                          node_ptr->page_right,
                          BS_NIL,
                          ftxH);
                if (sts != EOK)
                {
                    domain_panic(ftxH.dmnP,
                                 "idx_remove_filename_int: rbf_pinpg failed");
                    return(sts);
                }

                index=0;

                /* A special case exists in which a rename first does
                 * the insertion of the new filename, which can cause
                 * the node to split. This may invalidate the remove
                 * hint if the filename to be removed split over to a
                 * different node. In this case we will return FALSE
                 * and cause the b-tree to be searched.  */

                if(node_ptr->data[index].search_key != search_key)
                {
                    *found=FALSE;
                    return(EOK);
                }
            }

            /* This is the one to be removed so remove it by rippling
             * down the elements and pining.  */

            if (node_ptr->data[index].loc.dir_offset == offset)
            {
                if (index < node_ptr->total_elements-1)
                {
                    rbf_pin_record(rbfpgref,
                                   &node_ptr->data[index],
                                   sizeof(idxNodeEntryT)*
                                   (node_ptr->total_elements -
                                    index - 1));

                    bcopy((char *)&node_ptr->data[index+1],
                          (char *)&node_ptr->data[index],
                          sizeof(idxNodeEntryT)*
                          (node_ptr->total_elements -
                           index - 1));
                }

                IDX_DEC_TOTAL(rbfpgref,node_ptr);

                /* Determine if a the potential of a tree pruning
                 * exists and indicate so in the index access
                 * strucuture*/

                if ((node_ptr->total_elements <= IDX_COMPRESS_ME) &&
                    (node_ptr->page_right >= 0))
                {
                     ((bsIdxRecT *)idx_bfap->idx_params)->prune_key_fname =
                         search_key;
                }

                *found=TRUE;
                return(EOK);
            }
            index++;
        }

        /* A special case exists in which a rename first does the insertion
         * of the new filename, which can cause the node to split. This
         * may invalidate the remove hint if the filename to be removed
         * split over to a different node. In this case we will return
         * FALSE and cause the b-tree to be searched.
         */

        *found=FALSE;
        return(EOK);
    }

    /* Deref the intermediate node's page and return up any status.  */

    bs_derefpg(bfpgref,BS_CACHE_IT);
    return(sts);
}

/*******************************************************************
 * This routine will be called to lookup the existence of a
 * filename. 
 *
 * The returned arguments are intended to match the returned arguments
 * to seq_search so that this routine can basically replaced it
 * internally.
 *
 * The caller can indicate if they want the directory page that the
 * filename should or does reside on, pinned.
 *
 * Outputs:
 *
 *  ENOENT - the passed in filename was not found in the directory
 *  I_FILE_EXITS - a match was found.
 *******************************************************************/

statusT
idx_lookup_filename(
    bfAccessT   *dir_bfap,       /* bfap of the directory */
    struct nameidata *ndp,
    bfTagT      *found_bs_tag,   /* if found files tag # */
    fs_dir_entry **found_buffer, /* pointer to directory record */
    rbfPgRefHT  *pgref,          /* pgref for pinning record */
    int         flag,            /* flag: PIN_PAGE return pointer to
                                 /* dir entry with page pinned */
    ftxHT  ftxH                  /* parent ftx */
    )
{

    ulong search_key;
    bfAccessT *idx_bfap;
    idxNodeT *node_ptr;
    rbfPgRefHT rbfpgref_dir;
    bfPageRefHT bfpgref_dir;
    bfPageRefHT pgref_node;
    void *dir_ptr;
    fs_dir_entry *dir_rec_ptr;
    uint32T root_level,root_page;
    int node_page;
    char *filename;
    uint16T filename_len;
    uint32T found, index;
    bfTagT *tagp;
    statusT status,sts;

    status = I_INSERT_HERE;
    filename = ndp->ni_dent.d_name;
    filename_len = ndp->ni_dent.d_namlen;
    /* Hash the filename to get the search key to insert into the
     * index b-tree.  */

    search_key = idx_hash_filename_int(filename);

    /* Get the idx_file's access structure and setup for recursive
     * call.  */

    idx_bfap = ((bsDirIdxRecT *) dir_bfap->idx_params)->idx_bfap;
    root_page =((bsIdxRecT *) idx_bfap->idx_params)->bmt.fname_page;
    root_level = ((bsIdxRecT *) idx_bfap->idx_params)->bmt.fname_levels;

    sts = idx_lookup_node_int(root_page,
                              root_level,
                              search_key,
                              idx_bfap,
                              &node_ptr,
                              (void *)&pgref_node,
                              &index,
                              FALSE,
                              &node_page,
                              ftxH);
    if (sts != EOK)
        return(sts);

    found=FALSE;
    /*
     * We'll assume this node is the node that the insertion
     * or deletion will take place. If collsions cause us to
     * change our mind then only in the deletion case should this
     * hint be updated. The hint can only be used if there is no
     * chance of the node splitting (in the case of an insertion).
     */

    if (node_ptr->total_elements < IDX_MAX_ELEMENTS)
    {
        ndp->ni_hint = (caddr_t)node_page;
    }
    else
        ndp->ni_hint = (caddr_t)-1;

    while(!found)
    {
        if (index == node_ptr->total_elements)
        {
            /* We are at the end of the node. We must look at the next
             * node because of potential collisions.  */

            node_page = node_ptr->page_right;
            if (node_page < 0)
            {
                /* File not found */
                break;
            }

            bs_derefpg(pgref_node,BS_NIL);

            sts = bs_refpg(&pgref_node, (void *) &node_ptr, idx_bfap,
                     node_page, BS_NIL);

            if (sts != EOK)
            {
                /* If we run out of space attempting to clone the directory
                 * then we will not be able to read the direectory. Don't bring 
                 * down the domain, just return an EIO error to the caller
                 * which will not allow access to the directory
                 */
                if (sts != E_OUT_OF_SYNC_CLONE)
                {
                    /* Ok we've got problems bring the domain down */
                    
                    domain_panic(dir_bfap->dmnP,
                                 "idx_lookup_filename: bs_refpg failed");
                }

                return(sts);
            }

            if(node_ptr->total_elements == 0)
            {
                /* This can only happend with the right most node*/
                MS_SMP_ASSERT(node_ptr->page_right < 0);
                break;
            }
            index = 0;
        }

        if (search_key == node_ptr->data[index].search_key)
        {

            /* We have matching hash values. This does not mean the
             * filename strings match so we must look at the string in
             * the directory file that this hash value corresponds to.
             * */

            if (flag&PIN_PAGE)
            {
                sts = rbf_pinpg(&rbfpgref_dir,
                                (void *) &dir_ptr,
                                dir_bfap,
                       IDX_GET_PAGE_NO(node_ptr->data[index].loc.dir_offset),
                                BS_NIL,
                                ftxH);
                if (sts != EOK)
                {
                    domain_panic(dir_bfap->dmnP,
                                 "idx_lookup_filename: rbf_pinpg failed");
                    status=sts;
                    goto exit_lookup;
                }
            }
            else
            {
                sts=bs_refpg(&bfpgref_dir, (void *) &dir_ptr,dir_bfap,
                         IDX_GET_PAGE_NO(node_ptr->data[index].loc.dir_offset),
                         BS_NIL);

                if (sts != EOK)
                {
                    /* If we run out of space attempting to clone the directory
                     * then we will not be able to read the direectory. Don't bring 
                     * down the domain, just return to the caller
                     * which will not allow access to the directory
                     */
                    if (sts != E_OUT_OF_SYNC_CLONE)
                    {
                            /* Ok we've got problems bring the domain down */

                            domain_panic(dir_bfap->dmnP,
                                         "idx_lookup_filename: bs_refpg failed");
                    }

                    status=sts;
                    goto exit_lookup;
                }

            }

            dir_rec_ptr =  (fs_dir_entry *)((char *)
                            dir_ptr +
                        IDX_GET_OFFSET(node_ptr->data[index].loc.dir_offset));

            /* Test the filename length first to avoid the string
             * compare */

            if ((dir_rec_ptr->fs_dir_header.fs_dir_namecount
                 == filename_len) &&
                bcmp(dir_rec_ptr->fs_dir_name_string,
                 filename, filename_len) == 0)
            {
                tagp = GETTAGP(dir_rec_ptr);

                *found_bs_tag = *tagp;
                MS_SMP_ASSERT(dir_rec_ptr->fs_dir_header.fs_dir_bs_tag_num);

                /* set the location in the nameidata just like
                 * seq_search does */

                ndp->ni_count =
                    IDX_GET_PAGE_NO(node_ptr->data[index].loc.dir_offset);
                ndp->ni_offset =
                    IDX_GET_OFFSET(node_ptr->data[index].loc.dir_offset);

                if (flag&PIN_PAGE)
                {
                    *found_buffer = dir_rec_ptr;
                    *pgref = rbfpgref_dir;
                }
                else
                {
                    bs_derefpg(bfpgref_dir,BS_CACHE_IT);
                }
                status = I_FILE_EXISTS;
                /*
                 * In case there were collisions across a node we need to
                 * update the hint for the deletion case. We can always
                 * update the hint here since an insert will fail if the
                 * file exitst and the deletions will never cause the
                 * filename tree to split
                 */
                ndp->ni_hint = (caddr_t)node_page;

                break;
            }

            /* Filename's did not match see if more collisions exist */

            if(flag&PIN_PAGE)
            {
                rbf_deref_page(rbfpgref_dir, BS_CACHE_IT);
            }
            else
            {
                bs_derefpg(bfpgref_dir,BS_CACHE_IT);
            }

            index++;
        }
        else
        {
            break;
        }
    }

exit_lookup:

    bs_derefpg(pgref_node,BS_CACHE_IT);
    return(status);
}

/*******************************************************************
 *
 * This routine will be called to locate the leaf node that contains
 * first occurance of the passed in search key
 *
 *******************************************************************/

statusT
idx_lookup_node_int(
    uint32T     node_page,      /* btree node to search */
    uint32T         level,      /* level of recursion */
    ulong       search_key,     /* hash vale to find */
    bfAccessT   *idx_bfap,      /* index file access pointer */
    idxNodeT    **found_ptr,    /* Leaf node */
    void        *found_pgref,   /* Page ref if pin occurred */
    uint32T     *found_index,   /* offset in node */
    uint32T      pin_flag,      /* indicates if returned node
                                   should be pinned */
    int         *page,          /* node page containing key */
    ftxHT       ftxH
    )
{
    rbfPgRefHT pgref_pin;
    uint32T index;
    idxNodeT *node_ptr;
    bfPageRefHT pgref;
    statusT sts;
    /* Pin the passed in page */

    sts = bs_refpg(&pgref, (void *) &node_ptr,idx_bfap,node_page, BS_NIL);

    if (sts != EOK)
    {
        /* If we ran out of space attempting to clone the index file
         * then we will not be able to read the index. Don't bring 
         * down the domain, just return to the caller
         * which will not allow access to the directory
         */

        if (sts != E_OUT_OF_SYNC_CLONE)
        {
            domain_panic(idx_bfap->dmnP,"idx_lookup_node_int: bs_refpg failed");
        }
        
        return(sts);
    }

    index = idx_bsearch_int(node_ptr,
                            search_key,
                            ((node_ptr->page_right<0)&&(level>0)));

    MS_SMP_ASSERT((level==0) || (index < node_ptr->total_elements));

    /* If we are not at the leaf node then recursively call ourselves */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);

        sts=idx_lookup_node_int(node_ptr->data[index].loc.node_page,
                                level-1,
                                search_key,
                                idx_bfap,
                                found_ptr,
                                found_pgref,
                                found_index,
                                pin_flag,
                                page,
                                ftxH);
    }
    else
    {
        /* We have reached the leaf node. Let the wrapper routine
         * actually look for the filename.  */

        if (pin_flag)
        {
            bs_derefpg(pgref,BS_CACHE_IT);

            sts=rbf_pinpg(&pgref_pin,
                      (void *) &node_ptr,
                      idx_bfap,
                      node_page,
                      BS_NIL,
                      ftxH);

            if (sts != EOK)
            {
                domain_panic(idx_bfap->dmnP,
                             "idx_lookup_node_int: rbf_pinpg failed");
                return(sts);
            }

            *(rbfPgRefHT *)found_pgref= pgref_pin;
        }
        else
        {
            *(bfPageRefHT *)found_pgref= pgref;
        }
        *found_ptr=node_ptr;
        *found_index=index;
        *page = node_page;
        return(EOK);
    }

    bs_derefpg(pgref,BS_CACHE_IT);
    return(sts);
}

/*******************************************************************
 * This routine is the outer wrapper for the insert routine. It will
 * start a sub transaction. .  This routine must also handle the
 * special case of the root node splitting.  Since the internal insert
 * routine is recursive it is generic and does not handle this special
 * case.
 *
 * The space that we are inserting into the free space b-tree
 * represents space that was used in a directory to hold a filename
 * and is now available to be re-used. This routine will allow for
 * insertion of overlapping space. This is because in order to meet
 * compatibility issues the directory internals must be
 * maintained. Thus the existing directory code will glom adjacent
 * free space together and call this routine to insert the entire
 * piece into the free space b-tree. The b-tree code will detect the
 * overlap and adjust either the size or starting address of the
 * existing entry.
 *
 * The glomming by the existing directory code should be removed.
 * Since it has been decided that this code only runs on newly created
 * domains, the old glomming can be removed. This code will work as is
 * (except some assertions will need to be changed).
 *
 * This could fail to put the free space back into the tree if it can
 * not get storage. It is possible for the tree to need to split and
 * not have enough disk space to satisfy extending the index file. We
 * will return success to our caller and skip the insertion. This
 * should be fine since the next time the directory inserts a free
 * space adjacent to this entry it will glom together and insert the
 * entire chunk into the tree.
 *
 * The only time we could get a mismatch is if the directory handed us
 * an entry it glommed for which we already had one of the pieces in
 * our tree. If we then failed to put the glommed entry in our tree we
 * might hand out a partial piece to the directory.  Fortunately this
 * can not happen, because the glommed entry the directory handed us
 * can not cause the tree to split, since we are reusing an entry that
 * already exists! (we either increase its size or decrease its
 * starting address)
 *
 * Actually the above is not entirely true. There exists the
 * possiblity of permanently losing space in the directory. This is
 * because the existing dir code only gloms up to 512 byte
 * boundaries. There is a proposal for setting a flag in the node that
 * has this "lost" space and on an insertion reconstructing that node
 * based on the current directory contents. This is not of high
 * priority at the moment so ...
 *
 * If this is called from an undo a subtransaction can not be started.
 * A subtransaction is started if a node is split during the call to
 * rbf_add_stg but a node will never need to split in an undo call
 * since we are inserting back an element we removed and there for the
 * space will be available in the node since we are holding the
 * directory lock and no other thread could have used the slot we
 * removed (also prune could not have run since it is a root done and
 * we are undoing, meaning the transaction didn't complete, and
 * therefore root done could not have run!).  Outputs:
 *
 * NOTE:
 *      Once we have successfully inserted into the b-tree we disable
 *      the undoing of any sub-ftx's below us. This is because the b-tree
 *      has been properly setup and is really independent of the space
 *      insertion and can remain intact with only the undoing of
 *      the insertion. In other words if the b-tree split we can leave
 *      it split.
 *******************************************************************/

statusT
idx_directory_insert_space(
    bfAccessT *dir_bfap, /* Directories Access Structure */
    ulong size,         /* size of space to insert */
    int insert_page,     /* Page where space lives */
    uint32T insert_pgoff,    /* Offset where space lives */
    uint32T unglom_offset,  /* offset to be undone */
    uint32T unglom_size,    /* size to be undone */
    ftxHT  parentFtx,      /* Parent Ftx */
    int forceflag         /* sometimes we need to ignore quotas */
    )
{
    ulong search_key;
    uint32T root_page;
    uint32T level;
    ulong location;
    bfAccessT *idx_bfap;
    int split_count;
    idxNodeT *new_root;
    statusT sts;
    idxPinnedPgsT pinned_pages[IDX_MAX_BTREE_LEVELS];
    ftxHT ftxH, truncFtxH;
    idxUndoRecordT undorec;

    /* Get the idx_file's access structure and setup for recursive
     * call. Here the search key is the address of the free space.  */

    idx_bfap = ((bsDirIdxRecT *) dir_bfap->idx_params)->idx_bfap;
    root_page =((bsIdxRecT *) idx_bfap->idx_params)->bmt.ffree_page;
    location = IDX_GET_ADDRESS(insert_page,insert_pgoff);
    search_key = location;

    /* This may seem confusing but the leaf nodes have the address of
     * the storage as their search keys and the size as the size. The
     * intermediate nodes have the next node's page number in the size
     * field (or union is range).*/

    location = size;

    split_count = -1; /* indicates we are the first caller */

    /* Start the subtransaction */

    sts = FTX_START_N( FTA_NULL, &ftxH, parentFtx, idx_bfap->dmnP, 0);
    if (sts != EOK) 
    {
        domain_panic(parentFtx.dmnP,
                     "idx_directory_insert_space: could not start a ftx");
        return(sts);
    }

    /* Call the recursive insertion routine */

    sts = idx_directory_insert_space_int(root_page,
                    ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels,
                                         &search_key,
                                         &location,
                                         idx_bfap,
                                         ftxH,
                                         &split_count,
                                         pinned_pages,
                                         forceflag);

    if (sts < 0 )
    {
        /* We will fail the transaction but allow the caller to decide
         * the fate of the domain
         */
        ftx_fail(ftxH);
        idxStats.insert_space_failed++;
        return(sts);
    }

    if (sts==INSERT)
    {

        /* A status of INSERT indicates that the b-tree split all the
         * way up and a new root node must be created. The newpage
         * number is in dir_page and search_key contains the new
         * search_key. The pinned_pages array has the page already
         * pinned for us.
         *
         * Increment level to indicate that the b-tree has increased
         * in size.  Place the new elements in to the new root node
         * and pin the record.  It is not necessary to update the
         * search key of the last element of a node since this
         * contains all values greater than the previous element.  */

        level = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels + 1;

        /* Pin the records */

        new_root = pinned_pages[level].pgptr;

        rbf_pin_record (pinned_pages[level].pgref,
                        &new_root->data[0],
                        2*sizeof(idxNodeEntryT));

        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->total_elements);
        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->page_right);
        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->sib.page_left);
        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->reserved[0]);
        RBF_PIN_FIELD(pinned_pages[level].pgref,
                      new_root->reserved[1]);

        new_root->total_elements=2;
        new_root->page_right=-1;
        new_root->sib.page_left=-1;
        new_root->reserved[0]=0;
        new_root->reserved[1]=0;
        new_root->data[0].loc.node_page=root_page;
        new_root->data[1].loc.node_page=location;
        new_root->data[0].search_key=search_key;

        /* Place the update root node information in the record off
         * the directory's access structure. We must push this out to
         * the BMT now since we are in a subtransaction that can fail
         * and don't want to undo this.  */

        ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_page =
            pinned_pages[level].pgno;
        ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels++;

         sts = bmtr_put_rec(idx_bfap,
                            BMTR_FS_INDEX_FILE,
                            &((bsIdxRecT *)idx_bfap->idx_params)->bmt,
                            sizeof(bsIdxBmtRecT),
                            ftxH);

        if (sts != EOK)
        {
            /*
             * We can not call ftx_fail here since we have pinned
             * records. We will just call ftx_done and return the
             * status up to our caller.
             */

            if (sts != ENO_MORE_MCELLS)
            {
                domain_panic(ftxH.dmnP,
                             "idx_directory_insert_space: bmtr_put_rec failed");
            }
            ftx_done_n(ftxH,FTA_IDX_UNDO_V1);
            return(sts);
        }
    }

    undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
    undorec.dir_tag = dir_bfap->tag;
    undorec.nodeEntry.search_key = IDX_GET_ADDRESS(insert_page,unglom_offset);
    undorec.action = IDX_DIRECTORY_INSERT_SPACE_UNDO;
    undorec.nodeEntry.loc.free_size = unglom_size;

    /* We have reach a point now where it is save to stop all
     * undo's below us. The b-tree is in a consistent state and
     * does not need to be undone if this ftx is failed. We will
     * provide our own undo routine to remove the space only */

    ftx_special_done_mode ( ftxH, FTXDONE_SKIP_SUBFTX_UNDO);
    ftx_done_u( ftxH,
                FTA_IDX_UNDO_V1,
                sizeof(idxUndoRecordT),
                (void *) &undorec);

    /* Check if a prune needs to be done and start one up */

    sts=EOK;

    if (((bsIdxRecT *)idx_bfap->idx_params)->prune_key_ffree)
    {
        sts=idx_prune_start(idx_bfap,parentFtx);
        if ((sts == ENO_MORE_BLKS)||(sts == ENO_MORE_MCELLS))
        {
            /* We need do distinguish errors here for callers above
             * us. Since we could have failed attempting to insert space
             * due to a node split requesting blocks.
             * Different courses of action may need to be taken.
             */

            sts = E_INDEX_PRUNE_FAILED;
        }
    }

    return(sts);
}

/*******************************************************************
 * This routine will insert a data element in the leaf node of the
 * b-tree. The routine will recursively call itself until the leaf
 * node is reached. If a node fills up it will be split and a new node
 * created.
 *
 * In order for this routine to be under transaction control all
 * resources need to be obtained up front prior to pinning any
 * records. Furthermore this will need to be a subtransaction and have
 * an undo routine.
 *
 * At each level of the b-tree it will be determined whether a new
 * node will be needed if the leaf node were to split. This is the
 * case only if the current node is full. As we traverse down the tree
 * any non-full node we encounter will cause the split_count to reset.
 *
 * Once the leaf node is reached the file will be extended for the
 * number of pages/nodes we need, pinning each page. Each child node
 * will use the resources it needs and return the remaining resources
 * up to its caller until the root is reached.
 *
 * The back_link flag causes the code to maintain a double linked
 * list. Normally a single linked list is maintained but in the case
 * of the free space tree it is more efficient to maintain a double
 * linked list.
 *
 * The use of size is confusing here. The definition of the node data
 * is a union of size or range. In the case of the leaf nodes size is
 * used.  In the intermediate nodes it refers to range.
 *    Outputs:
 *
 *       INSERT - Indicates that the caller must insert the passed
 *                back search_key and size into the node at this level.
 *
 *       DONE - Indicates that the insertion took place and no splits
 *              occurred.
 *
 *       FAILED - Indicates that storage could not be obtained so we
 *                must fake that the insertion was successful.
 *
 *******************************************************************/

int
idx_directory_insert_space_int(
        uint32T node_page,  /* in - starting node */
        ulong level,        /* level in the tree */
        ulong *search_key,  /* in - key to insert */
                            /* out - key to insert*/
        ulong *size,        /* in - size to store*/
                            /* out - location to store*/
        bfAccessT *idx_bfap,    /* b-tree access structure */
        ftxHT     ftxH,     /* parent transaction */
        int *split_count,   /* number of pages needed for split */
        idxPinnedPgsT *pptr, /* pinned pages array pointer  */
        int forceflag        /* sometimes we need to ignore quotas */
    )
{
    rbfPgRefHT pgref, right_pgref;
    idxNodeT *right_pgptr,*node_ptr;
    ulong temp_size;
    uint32T index;
    uint32T ret;
    statusT sts;

    /* Pin the page into memory. Since we don't know at this point
     * whether the page is going to be modified due to a split, just
     * pin it instead of reffing it. If we never do an rbf_pin_record
     * then the page will not be marked dirty.  */

    sts=rbf_pinpg( &pgref,  (void *) &node_ptr, idx_bfap,
                   node_page, BS_NIL,
                   ftxH);
    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,
                     "idx_directory_insert_space_int: rbf_pinpg failed");
        return(sts);
    }

    /* Search the current node for the search_key */

    if (node_ptr->total_elements == 0) index=0;
    else
    index = idx_bsearch_int(node_ptr,
                            *search_key,
                            ((node_ptr->page_right<0)&&(level>0)));

    MS_SMP_ASSERT((level==0) || (index < node_ptr->total_elements));

    /* Check if this node would need to be split if the node below it
     * were split and if so increment the split_count, if not reset
     * the count. If this is the first caller (split_count < 0) and we
     * may need to split then ask for an extra page since our caller
     * will need to create a new root node. Recursively call insert
     * again if this is not a leaf node.  */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);
        if (node_ptr->total_elements == IDX_MAX_ELEMENTS)
        {
            if (*split_count < 0) *split_count=1;
            /* get and extra for root */
            (*split_count)++;
        }
        else *split_count=0;

        /* There exists the potential to pin too many pages here. I
         * believe that increasing the number of allowed pins to 9
         * should cover all the cases. Also I do not think the free
         * space tree can grow very large (due to glomming).  */

        ret =
            idx_directory_insert_space_int(node_ptr->data[index].loc.node_page,
                                           level-1,
                                           search_key,
                                           size,
                                           idx_bfap,
                                           ftxH,
                                           split_count,
                                           pptr,
                                           forceflag);

        /* If an insertion needs to be done then we must update the
         * existing entry.  This is because the node below us has
         * split and needs to insert the new node into our search
         * array. The split caused the existing node to now have a new
         * search key and the new node to have the original search
         * key.  */

        if (ret == UPDATE)
        {
            /* This is a special case. The search_key that sends us
             * to our child is too great. A glomming caused our search_key
             * to now partially reside on the node next to our child. We
             * must update the range. */

            RBF_PIN_FIELD(pgref,node_ptr->data[index].search_key);
            node_ptr->data[index].search_key = *search_key;
            if (index == node_ptr->total_elements-1)
            {
                return(UPDATE);
            }

            return(DONE);
        }
        if (ret == INSERT)
        {
            temp_size = node_ptr->data[index].loc.free_size;
            node_ptr->data[index].loc.free_size = *size;
            *size = temp_size;
        }
    }
    else
    {
        /* This is the leaf node processing. Check to see if we can
         * avoid an insert by glomming adjacent entries */

        ret = idx_glom_entry_int(idx_bfap,node_ptr,pgref,
                                 *search_key,*size,index,ftxH);

        if (ret == UPDATE)
        {
            /* This is a special case. The search_key that sends us
             * to our child is too great. A glomming caused our search_key
             * to now partially reside on the node next to our child. We
             * must inform our parent */

            *search_key = node_ptr->data[node_ptr->total_elements-1].search_key;
            return(UPDATE);
        }

        if (ret != INSERT)
        {

            /* We were able to glom an entry together. No insert is
             * needed. Return our status up.  */

            return(ret);
        }

        /* Get any resources that will be needed to be passed back up,
         * before we pin any records and are still able to
         * fail.
         *
         * If we are unable to get storage for the split then return
         * this to our caller since we must act like the insert was
         * successful.  */

        if (node_ptr->total_elements ==  IDX_MAX_ELEMENTS)
        {
            if (*split_count < 0) *split_count=1;
            /* get and extra for root */
            (*split_count)++;

            sts = idx_index_get_free_pgs_int( idx_bfap,
                                              *split_count,
                                              pptr,
                                              ftxH, forceflag);
            if (sts != EOK)
                return(sts);
        }
    }

    /* All leaf nodes do the following processing.
     *
     * Check if an insertion needs to be done. Split the node if it
     * needs it and insert new element. rbf_pin_record any ranges that
     * were modified.  */

    if (ret == INSERT)
    {
        if (node_ptr->total_elements == IDX_MAX_ELEMENTS)
        {
            idx_split_node_int(
                               *search_key,
                               *size,
                               index,
                               node_ptr,           /*src*/
                               pptr[level].pgptr,   /*dst*/
                               pgref,             /*src*/
                               pptr[level].pgref  /*dst*/
                );

            /* Update the return arguments so the caller can insert
             * our new node into its search array. The size (range)
             * refers to the new node while the search_key refers to
             * the old node.  */

            *size = pptr[level].pgno;
            *search_key =
                node_ptr->data[node_ptr->total_elements - 1].search_key;

            /* Maintain the linked list amongst b-tree nodes.  */

            if (node_ptr->page_right >= 0)
            {
                sts=rbf_pinpg (&right_pgref,
                           (void *)&right_pgptr,
                           idx_bfap,
                           node_ptr->page_right,
                           BS_NIL,
                           ftxH);

                if (sts != EOK)
                {
                    domain_panic(ftxH.dmnP,
                             "idx_directory_insert_space_int: rbf_pinpg failed");
                    return(sts);
                }

                RBF_PIN_FIELD(right_pgref,right_pgptr->sib.page_left);
                right_pgptr->sib.page_left=pptr[level].pgno;
            }

            RBF_PIN_FIELD(pptr[level].pgref,pptr[level].pgptr->sib.page_left);
            pptr[level].pgptr->sib.page_left = node_page;

            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->page_right);
            pptr[level].pgptr->page_right=node_ptr->page_right;

            RBF_PIN_FIELD(pgref,node_ptr->page_right);
            node_ptr->page_right =pptr[level].pgno;

            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->reserved[0]);
            RBF_PIN_FIELD(pptr[level].pgref,
                          pptr[level].pgptr->reserved[1]);

            pptr[level].pgptr->reserved[0]=0;
            pptr[level].pgptr->reserved[1]=0;


            /* Indicate to the caller that an insertion must be done. */

            return (INSERT);
        }
        else
        {
            /* Insert the element into the node using bcopy then pin
             * the record.  Bcopy will handle the overlap.  */

            MS_SMP_ASSERT(node_ptr->total_elements < IDX_MAX_ELEMENTS);

            rbf_pin_record(pgref,&node_ptr->data[index],
                           (node_ptr->total_elements-index+1)*
                           (sizeof(idxNodeEntryT)));

            IDX_INC_TOTAL(pgref,node_ptr);

            if (index < node_ptr->total_elements - 1)
                bcopy((char *)&node_ptr->data[index],
                      (char *)&node_ptr->data[index+1],
                      (node_ptr->total_elements-index-1)*
                      (sizeof(idxNodeEntryT)));

            node_ptr->data[index].loc.free_size = *size;

            MS_SMP_ASSERT((level > 0) ||
                          (node_ptr->data[index].loc.free_size
                           >= MIN_DIR_ENTRY_SIZE));
            node_ptr->data[index].search_key = *search_key;

            return(DONE);
        }
    }
    return(ret);
}

/*******************************************************************
 * This routine is called when space is needed in the directory file
 * for storing a filename. This routine searches the free space b-tree
 * and returns the location in the directory file of where the
 * filename should be placed.  This is the external wrapper for
 * internal space routine for the free space b-tree. This routine will
 * conditionally start a sub-transaction and create an undo record.
 *
 * If the returned size is zero then a FALSE value will be returned to
 * the caller indicating that storage must be added to the directory.
 *
 * If the returned size is greater than a block, an insertion of the
 * remaining space must be made back to the b-tree.
 *
 * If the return status from the internal call is TRUE then the
 * pruning root done routine will be hooked into the ftx_done.
 *
 * Note: As in the case of the filename tree, if the caller of this
 * routine is itself an undo routine then a transaction can not be
 * started. We are insured that the space that needs to be removed
 * (undone) from the tree will not cause a pruning since we have been
 * holding the directory lock.
 *
 * The existing directory code will glom two adjacent free entries
 * together into one but never cross a 512 byte boundary. It will then
 * call the idx_directory_insert_space routine to place the location
 * of this entry into our free space b-tree. The b-tree code may then
 * further glom this entry into multiple 512 byte blocks.
 *
 * The call to the internal routine will return two sizes. The function
 * return is the size of space obtained from the b-tree. The size argument
 * will contain the size required to satisfy the request. This may not
 * be the requested size due to padding requirements. If the handed back
 * space is near a block boundary then this may include that space if it
 * is below the minimum directory entry size. We need to pass this up
 * because in some cases this routine may need to re-insert space back
 * into the tree. Thus we need to know how much.
 *
 *    Outputs:
 *        FALSE - The storage was obtained.
 *        TRUE - The directory file must be extended.
 *        ENO_MORE_BLOCKS
 *
 *******************************************************************/

int
idx_directory_get_space(
    bfAccessT *dir_bfap, /* The directory's access structure */
    ulong size,        /* Requested size to obtain */
    int *insert_page,    /* out - page where storage resides */
    long *insert_pgoff,   /* out - offset where storage resides */
    ftxHT  parentFtx   /* The parent transaction */
    )
{
    ulong root_page;
    ulong location, insert_loc;
    bfAccessT *idx_bfap;
    ftxHT ftxH;
    long found_size;
    idxUndoRecordT undorec;
    statusT sts;

    /* Get the idx_file's access structure and setup for recursive
     * call.  */

    MS_SMP_ASSERT(size <= DIRBLKSIZ);

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;
    root_page = ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_page;

    /* Start the sub-transaction */
    sts = FTX_START_N( FTA_NULL, &ftxH, parentFtx, idx_bfap->dmnP, 0);
    if (sts != EOK)
    {
        domain_panic(parentFtx.dmnP,
                     "idx_directory_get_space: FTX_START_N failed");
        return(sts);
    }

    /* Call the recursive removal routine */

    found_size = idx_directory_get_space_int(root_page,
                     ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels,
                                        &size,
                                        &location,
                                        idx_bfap,
                                        ftxH);

    /* Set up the undo record if anything was done*/
    if (found_size > 0)
    {
        undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
        undorec.dir_tag = dir_bfap->tag;
        undorec.nodeEntry.search_key = location;
        undorec.nodeEntry.loc.free_size = found_size;

        undorec.action = IDX_DIRECTORY_GET_SPACE_UNDO;

        /* We need to finish the transaction here in case we end up calling
         * idx_directory_insert_space. Since idx_directory_insert_space could
         * fail we want our caller to be able to fail us..
         */

        ftx_done_u(ftxH,
                   FTA_IDX_UNDO_V1,
                   sizeof(idxUndoRecordT),
                   (void *)  &undorec);
    }
    else
    {
        if (found_size == 0)
        {
            /* No space available inform the caller to extend the
             * directory
             * No undo routine is necessary just finish the ftx */

            ftx_done_n(ftxH,FTA_IDX_UNDO_V1);
            return(TRUE);
        }
        else
        {
            /* Looks like we took an error and domain paniced
             * send the status up
             */
            ftx_fail(ftxH);
            idxStats.get_space_failed++;
            return(found_size);
        }
    }

    /* It is possible that we need break apart the returned space and
     * re-insert some back into the tree. This is because it was the
     * last element on a node and requires an insert back into the
     * b-tree.  */

    MS_SMP_ASSERT(found_size >= MIN_DIR_ENTRY_SIZE);


    if (found_size >  size)
    {
        MS_SMP_ASSERT(found_size - size >= MIN_DIR_ENTRY_SIZE);

        /* We know that the first piece is the one that satisfies the
         * request, so peel that out and insert the rest into the
         * tree.  */

        insert_loc = location + size;
        size = found_size - size;

        /* Since we are putting back a part of what we pulled out
         * there should be no directory truncation needed.*/

        sts=idx_directory_insert_space(dir_bfap,
                                       size,
                                       IDX_GET_PAGE_NO(insert_loc),
                                       IDX_GET_OFFSET(insert_loc),
                                       IDX_GET_OFFSET(insert_loc),
                                       size,
                                       parentFtx,
                                       0 ); /* forceflag to ignore quota */

        if (sts != EOK)
            return(sts);
    }

    *insert_page=IDX_GET_PAGE_NO(location);
    *insert_pgoff=IDX_GET_OFFSET(location);

    sts=FALSE;

    if (((bsIdxRecT *)idx_bfap->idx_params)->prune_key_ffree)
    {
        sts=idx_prune_start(idx_bfap,parentFtx);
    }

    return(sts);
}

/*******************************************************************
 * This is both the lookup and removal routine for the free space
 * b-tree.  Because of the nature of the tree these two functions can
 * be combined into a single routine.
 *
 * A call to this routine results when a free space to store a
 * filename is needed. The caller will pass in a desired size and the
 * tree will be searched starting from the leftmost node and working
 * to the right until a free element that is equal to or greater than
 * the passed in size is found. This is a first fit algorithm
 *
 * The idea is to reuse the space at the beginning of the directory
 * file in hopes that we may truncate it later.
 *
 * The search will be linear through the leaf nodes.
 *
 * The left most node will be located and searched moving on to the
 * next leaf if necessary until a piece of free space is found. The
 * piece will then be removed from the tree and passed back to the
 * caller.
 *
 * If the requested size can not be located a size of zero will be
 * returned indicating that the caller should extend the directory
 * file and add a new page of storage.
 *
 *   Outputs:
 *
 *      The size of the storage given back.
 *      this will be greater than or equal to the
 *      requested size. A size of 0 means the directory
 *      needs storage added to it.
 *
 *******************************************************************/

int
idx_directory_get_space_int(
    uint32T page_to_search,
    uint32T level,
    ulong *size,
    ulong *file_offset,
    bfAccessT *idx_bfap,
    ftxHT ftxH
    )
{
    rbfPgRefHT rbfpgref;
    bfPageRefHT bfpgref;
    idxNodeT *node_ptr;
    long found_size;
    int next_node;
    uint32T index;
    ulong required_size, free_size, rounded_free_size, requested_size;
    ulong dirRec_adj;
    ulong start_addr;
    statusT sts;

    /* If we are not at the leaf node then reff the page otherwise pin
     *    it since we will be writing it back out.  */

    if (level > 0)
    {
        sts=bs_refpg(&bfpgref,
                 (void *)&node_ptr,
                 idx_bfap,
                 page_to_search,
                 BS_NIL);
    }
    else
    {
        sts=rbf_pinpg(&rbfpgref,
                  (void *)&node_ptr,
                  idx_bfap,
                  page_to_search,
                  BS_NIL,
                  ftxH);
    }

    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_directory_get_space_int: rbf_pinpg failed");
        return(E_DOMAIN_PANIC);
    }

    if((level == 0)&&(node_ptr->total_elements == 0))
    {
        return(0);
    }
    /* Start up the search with the leftmost index */

    index = 0;

    /* If we are not at the leaf node then recursively call ourselves */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);

        found_size =
            idx_directory_get_space_int(node_ptr->data[index].loc.node_page,
                                        level-1,
                                        size,
                                        file_offset,
                                        idx_bfap,
                                        ftxH);

        /* We only get here if we are not at the leaf node so just
         * keep passing the status back up */

        bs_derefpg(bfpgref,BS_CACHE_IT);
        return(found_size);
    }

    /* We are at the leaf node so locate the first element that
     * satisfies our size requirements.  */

    requested_size = *size;

    while ( TRUE )
    {
        if (index == node_ptr->total_elements)
        {
            /* We reached the end of the node so read in the next
             * node.
             */

            index=0;
            next_node = node_ptr->page_right;
            rbf_deref_page(rbfpgref, BS_NIL);
            if (next_node > 0)
            {
                sts=rbf_pinpg(&rbfpgref,
                              (void *) &node_ptr,
                              idx_bfap,
                              next_node,
                              BS_NIL,
                              ftxH);

                if (sts != EOK)
                {
                    domain_panic(ftxH.dmnP,
                               "idx_directory_get_space_int: rbf_pinpg failed");
                    return(E_DOMAIN_PANIC);
                }

                if (node_ptr->total_elements == 0)
                {
                    MS_SMP_ASSERT(node_ptr->page_right < 0);
                    return(0); /* No Space here */
                }
            }
            else
            {
                return(0);
            }
        }

        if (node_ptr->data[index].loc.free_size >= requested_size)
        {
            /* We think we found space that meets our requirements.  Since
             * the directory file deals in 512 byte blocks we must only
             * hand back space that is within a 512 byte block. It can not
             * cross the 512 boundary ( This restriction is artifical. It is a
             * UFSism that we do not need to maintain because the library
             * routines do not handle filenames crossing a 512 byte boundary
             * when returned for getdirents)
             *
             * The free space tree glomms together as much space it can
             * with no regard for block or page sizes. Thus we must only
             * hand out space up to the nearest 512 byte block and if that
             * happens to not be enough then see if the second half is
             * enough and if thats not enough keep looking.  */

            required_size = requested_size;

            MS_SMP_ASSERT(required_size > 0);

            start_addr = node_ptr->data[index].search_key;
            free_size =  (ulong) node_ptr->data[index].loc.free_size;
            MS_SMP_ASSERT(free_size > 0);
            rounded_free_size =
                min(DIRBLKSIZ - (start_addr&(DIRBLKSIZ-1)), free_size);

            /* Because we have to carry around this cookie at the end of
             * each directory page, the dir_record we must account for it
             * in our calculation but hand it out as free space */

            if ((start_addr + rounded_free_size) & (ADVFS_PGSZ - 1))
            {
                dirRec_adj = 0;
            }
            else
            {
               dirRec_adj =  sizeof(dirRec);
            }

            if ( required_size <= (rounded_free_size - dirRec_adj))
            {
                *file_offset = start_addr;

                found_size = required_size;

                /* We have the space to satisfy our request. Now try to
                 * save work by peeling out the exact amount we need (this
                 * may be more that the requested size).
                 *
                 * We have certain restrictions that will force us to
                 * return a large chunk of free space and make the calling
                 * routine re-insert the smaller pieces. This is because
                 * we search the leaf nodes linearly and can not return to
                 * the proper parent. So any modifications done now must
                 * not result in the parent node needing a modification.
                 * If it does then a formal insert must be done.
                 *
                 * No shortcut can be performed if any of the following
                 * hold true.
                 *
                 * 1) The leaf search_key will change and we are the last
                 * entry in the node.
                 *
                 * 2) An insertion to this node must take place and the
                 * node is already full.  */

                /* We can not cross the 512 byte boundary, so we will
                 * peel out what we need and return the rest back to the
                 * free tree unless we are the last element in the
                 * node in which case we'll return the whole
                 * thing. Except (*sigh* so many exceptions) when the
                 * node happens to be the last node (or only node) in
                 * the tree then we can safely alter the last entry in
                 * the node. This is because the last node represents
                 * everything greater than the previous node and we
                 * will only be increasing the starting address
                 */

                /*
                 * The free space must encompass the last dir record of a page
                 * if present and any space that is too small to hold a valid
                 * file name.
                 */

                if ((rounded_free_size - required_size - dirRec_adj) < 
                    MIN_DIR_ENTRY_SIZE )
                {

                    /* This may be more space than requested, but not enough
                     * to break into two. So we'll give it all back.
                     */

                    found_size=required_size = rounded_free_size;
                }

                if (required_size < free_size)
                {
                    if ((index < node_ptr->total_elements - 1) ||
                        (node_ptr->page_right < 0))
                    {
                        /* Take a short cut here and do the insertion
                         * ourselves.  */

                        /* We now know exactly how much space we will
                         now check to see if we can save work by
                         * adjusting the existing chunk of free space if
                          it is larger that what we need.  */

                        node_ptr->data[index].search_key += required_size;
                        node_ptr->data[index].loc.free_size -= required_size;

                        MS_SMP_ASSERT(node_ptr->data[index].loc.free_size >=
                                      MIN_DIR_ENTRY_SIZE);

                        RBF_PIN_FIELD(rbfpgref,node_ptr->data[index]);

                        /* Return the exact size to the caller indicating that
                         * all work has been done. 
                         */
                        
                        *size = required_size;

                        return(required_size); /* No pruning necessary */
                    }

                    found_size = free_size; /* return all of it */
                }

                /* We need to pull the whole thing out of the tree. This is
                 * because either it exactly meets our needs or we were unable
                 * to adjust the existing entry and a re-insertion is required.
                 */

                if(index < node_ptr->total_elements - 1)
                {
                    rbf_pin_record(rbfpgref, &node_ptr->data[index],
                                   (node_ptr->total_elements - index - 1) *
                                   sizeof(idxNodeEntryT));

                    bcopy(&node_ptr->data[index+1],&node_ptr->data[index],
                          (node_ptr->total_elements - index - 1) *
                          sizeof(idxNodeEntryT));

                }

                IDX_DEC_TOTAL(rbfpgref,node_ptr);

                if ((node_ptr->total_elements <= IDX_COMPRESS_ME) &&
                    (node_ptr->page_right >= 0))
                {
                    ((bsIdxRecT *)idx_bfap->idx_params)->prune_key_ffree =
                        node_ptr->data[index].search_key;
                }

                /* Update the requested size in case it needed to encompass
                 * space that is less that the min dir size or that last
                 * record on a page. This will inform the initial caller
                 * to account that the returned space is slightly larger
                 * than requested.
                 */

                *size=required_size;

                return(found_size);
            }

            /* The first half of the space did not satisfy the request so
             * look at the second half (if there even is one) */

            required_size = requested_size;
            start_addr += rounded_free_size;

            MS_SMP_ASSERT(free_size >= rounded_free_size);

            rounded_free_size = free_size - rounded_free_size;

            /* Because we have to carry around this cookie at the end of
             * each directory page, the dir_record we must account for it
             * in our calculation but hand it out as free space 
             * NOTE since we are dealing with the second half we know
             * we will never have to include the dirRec since we are at
             * the beginning of a 512 boundary and the filename length MAX
             * is FS_DIR_MAX_NAME = 255. I will keep the dirRec_adj in for
             * completeness but it is unecessary*/

            if ( (start_addr +  min(DIRBLKSIZ, free_size) ) & (ADVFS_PGSZ - 1))
            {
                dirRec_adj = 0;
            }
            else
            {
                dirRec_adj = sizeof(dirRec);
            }

            if ((rounded_free_size != 0) &&
                (required_size <= (rounded_free_size - dirRec_adj)))
            {
                *file_offset = start_addr;

                /* The second half will satisfy the request. So at least
                 * get that ready.
                 */

                RBF_PIN_FIELD(rbfpgref,node_ptr->data[index].loc.free_size);
                node_ptr->data[index].loc.free_size-=rounded_free_size;
                MS_SMP_ASSERT(node_ptr->data[index].loc.free_size >=
                              MIN_DIR_ENTRY_SIZE);

                /* Two cases exist.
                 *
                 * 1) The second half should be returned as is.
                 *
                 * 2) The second half needs to be truncated.
                 *
                 * The short cut can only be taken if we are not
                 * effecting our parent. This will be the case as
                 * long as we do not change the last slot in the
                 * node or we do not have a right sibling.
                 *
                 * Since we ripple the elements up one slot
                 * the current index can not be the last slot.
                 */

                if ((rounded_free_size - required_size - dirRec_adj) < 
                    MIN_DIR_ENTRY_SIZE)
                {
                    /* Great! We exactly match the space
                       available. All is done.*/

                    /* Make sure to include in the returned size every
                     * thing taken out of the tree (this may include
                     * anything less than MIN_DIR_ENTRY_SIZE) so the
                     * undo record knows exactly what to put back !  */

                    *size=rounded_free_size;

                    return(rounded_free_size);
                }

                if ((node_ptr->total_elements < IDX_MAX_ELEMENTS) &&
                    ((index < node_ptr->total_elements - 2) ||
                     (node_ptr->page_right < 0)))
                {

                    rbf_pin_record(rbfpgref,
                                   &node_ptr->data[index],
                                   (node_ptr->total_elements -
                                    index + 1) *
                                   sizeof(idxNodeEntryT));

                    if (index < node_ptr->total_elements - 1)
                    {
                        bcopy(&node_ptr->data[index+1],
                              &node_ptr->data[index+2],
                              (node_ptr->total_elements - index - 1) *
                              sizeof(idxNodeEntryT));
                    }

                    node_ptr->data[index+1].search_key = 
                        start_addr+required_size;
                    node_ptr->data[index+1].loc.free_size =
                        rounded_free_size - required_size;

                    MS_SMP_ASSERT(node_ptr->total_elements < IDX_MAX_ELEMENTS);
                    MS_SMP_ASSERT(node_ptr->data[index+1].loc.free_size >=
                                   MIN_DIR_ENTRY_SIZE);

                    IDX_INC_TOTAL(rbfpgref,node_ptr);

                    return(requested_size);
                }
                return(rounded_free_size);
            }
        }
        index++; /* check next piece */
    }
    MS_SMP_ASSERT(FALSE);
}

/*******************************************************************
 * This is the outer wrapper for the recursive truncation routine.
 * It will be called by external routines to check if the directory
 * is in need of truncation.
 *
 * The truncation flag will be set when space is added to the free
 * space tree and then later checked at file insertion time to see if
 * a truncation can take place.. The reason for the seperation is that
 * when a file is removed the directory page is pinned and the storage
 * can not be removed until that transaction has completed.
 *
 * If the insertion uses the page that needs to be truncated then we
 * will not truncate and we have save ourselves some work. If an insert
 * does not take place for a while and the flag is lost because the
 * access structure is recylced, then on the next deletion it will be
 * set again and eventually truncated (hopefully).
 *
 * This routine will adjust the b-tree to reflect the truncation and
 * return to the caller the number of bytes eligible for truncation.
 * The caller must start the removal of storage within the same root
 * ftx. So that if the whole ftx is failed the undo for this
 * truncation can put back the ranges into the b-tree.
 *******************************************************************/

long
idx_setup_for_truncation(
    bfAccessT *dir_bfap,
    int operation,
    ftxHT parentFtxH
    )
{
    long bytes_to_trunc;
    bfAccessT *idx_bfap;
    ulong undo_space_start;
    statusT sts;
    idxUndoRecordT undorec;
    ftxHT ftxH;

    /* Calculate the number of pages that can be truncated and remove
     * them from the free space b-tree.  Start a subtransaction so we
     * can supply an undo function. We will allow the stg_remove_stg
     * to put the storage back if it is failed and we will do
     * likewise.  */

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    if (operation == TRUNC_UPDATE)
    {	
        sts = FTX_START_N( FTA_NULL, &ftxH, parentFtxH, idx_bfap->dmnP, 0);
        if (sts != EOK)
        {
            domain_panic(parentFtxH.dmnP,
                         "idx_setup_for_truncation: FTX_START_N failed");
            return(sts);
        }
    }

    bytes_to_trunc =
        idx_setup_for_truncation_int (idx_bfap,
                       ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_page,
                       ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels,
                                      &undo_space_start,
                                      operation,
                                      ftxH);

    if (operation == TRUNC_QUERY)
    {
        if (bytes_to_trunc == 0)
        {

            /* Here's the deal. A prior filename deletion noticed that
             * the   directory   could   be   truncated  and  set  the
             * IDX_TRUNCATE flag.
             *
             * A few things could have taken place before us, multiple
             * deletions (which will  not change the need to truncate,
             * a failed truncation (we  could fail to  remove  storage
             * because we  couldn't get an  mcell) in  which case  the
             * flag will still be set and the truncation still needed,
             * an insert into the page that was about to be truncated,
             * or an insert anywhere but the truncation region.
             *
             * If  the  truncation  were to fail  because we could not
             * remove  storage, we  will not  fail the  insertion that
             * triggered the  truncation.  This  means if we are tight
             * on  space,  many  insertions  can  take  place  without
             * truncation   succeeding.   But   we   will   keep   the
             * IDX_TRUNCATE flag set in hopes of eventually succeeding
             * the truncation.
             *
             * We  must make  sure that the  first attempt to truncate
             * which  detects that  the  directory  can  no  longer be
             * truncated  ( because we inserted  into  the last page )
             * clears the flag.
             *
             * The  danger  here is a  full  page at  the  end  of the
             * directory could mistakenly be  truncated off. If  there
             * was an empty page  proceeded by a  completely full page
             * we would  think the  directory  could  be truncated. By
             * clearing the flag at the first insert into that page we
             * can avoid this.  
             */
            
            ((bsIdxRecT *)idx_bfap->idx_params)->flags &= ~(IDX_TRUNCATE);
        }

        return (bytes_to_trunc);
    }
        
    if (bytes_to_trunc < 0)
    {
        /* This double duties as a status. We seem to be in an error
         * state. Return to the caller 
         */

        ftx_fail( ftxH );
        idxStats.setup_for_truncation_failed++;
        return(bytes_to_trunc);
    }

    MS_SMP_ASSERT(bytes_to_trunc > 0);

    /* Put the number of pages we truncated into the undo
     * record.
     */

    undorec.bfSetId = ((bfSetT *)idx_bfap->bfSetp)->bfSetId;
    undorec.dir_tag=dir_bfap->tag;
    undorec.nodeEntry.search_key=undo_space_start;
    undorec.nodeEntry.loc.free_size=bytes_to_trunc;
    undorec.action = IDX_SETUP_FOR_TRUNCATION_INT_UNDO;

    ftx_done_u( ftxH,
                FTA_IDX_UNDO_V1,
                sizeof(idxUndoRecordT),
                (void *) &undorec);

    ((bsIdxRecT *)idx_bfap->idx_params)->flags &= ~(IDX_TRUNCATE);

    return(bytes_to_trunc);
}

/*******************************************************************
 * This routine will be called when the idx_directory_insert_space
 * indicates that a directory truncation needs to take place. When
 * free space is inserted back to the free space tree it will be
 * glommed into the largest contiguous chunk. If this chunk is at
 * least a page in size and encompasses the last page of the directory
 * file then this routine is called.
 *
 * This routine will remove the entry from the free space tree that
 * represents the space being truncated from the directory. The entry
 * may not be a page increment and will therefor need to have any
 * remaining space inserted back into the free space tree.
 *
 * This routine will set in the namei structure the last useful page
 * which will be passed to the dir_tunc_start routine. This routine
 * will also check to see if the node can be pruned and return a
 * status indicating that the prune routine should be invoked.
 *
 * Since the caller of this routine is in a subtransaction, an undo
 * for this routine must be supplied. This is because a record is
 * being pinned and would be lost.
 *******************************************************************/

long
idx_setup_for_truncation_int(
    bfAccessT *idx_bfap,
    uint32T page_to_search,
    uint32T level,
    ulong *undo_space_start,
    int operation,
    ftxHT ftxH
    )
{
    rbfPgRefHT rbfpgref;
    bfPageRefHT bfpgref;
    idxNodeT *node_ptr;
    ulong num_bytes;
    ulong bytes_to_trunc;
    uint32T index;
    statusT sts;
    /* If we are not at the leaf node then reff the page otherwise pin
     * it since we will be writing it back out.  */

    if ((level > 0) || (operation == TRUNC_QUERY))
    {
        sts=bs_refpg(&bfpgref,
                 (void *)&node_ptr,
                 idx_bfap,
                 page_to_search,
                 BS_NIL);
    }
    else
    {
        sts=rbf_pinpg(&rbfpgref,
                  (void *) &node_ptr,
                  idx_bfap,
                  page_to_search,
                  BS_NIL,
                  ftxH);
    }

    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_setup_for_truncation_int: rbf_pinpg failed");
        return(sts);
    }

    /* Since the free tree is ordered by offset within the directory,
     * always get the right most node.  */

    index = node_ptr->total_elements-1;

    /* If we are not at the leaf node then recursively call ourselves.  */

    if (level > 0)
    {
        MS_SMP_ASSERT(node_ptr->total_elements > 0);
        bytes_to_trunc =
            idx_setup_for_truncation_int (idx_bfap,
                                         node_ptr->data[index].loc.node_page,
                                          level-1,
                                          undo_space_start,
                                          operation,
                                          ftxH);
    }
    else
    {
        /* We are at the leaf node. The right most entry corresponds
         * to the free space to be truncated. Figure out if it is a
         * multiple of a page and if not remove all but that amount
         * otherwise decrement the total_elements in the node.
         *
         * We know that the storage is contiguous to the end of the
         * directory file so any remainder is at the beginning of the
         * piece of storage.  */


        /* Pin_record any changes made and return up the number of
         * pages to be truncated.  */

        if (node_ptr->total_elements == 0)
        {
            /* This is the last node of the tree and it has  zero
             * elements due to our pruning policy. Bring in the left
             * sibling and look at the last element of it for the
             * space to truncate
             */

            MS_SMP_ASSERT(node_ptr->page_right < 0);

            page_to_search = node_ptr->sib.page_left;

            if (operation == TRUNC_QUERY)
            {

                bs_derefpg(bfpgref,BS_CACHE_IT);

                sts=bs_refpg(&bfpgref,
                             (void *)&node_ptr,
                             idx_bfap,
                             page_to_search,
                             BS_NIL);
            }
            else
            {
                rbf_deref_page(rbfpgref,BS_CACHE_IT);

                sts=rbf_pinpg(&rbfpgref,
                              (void *) &node_ptr,
                              idx_bfap,
                              page_to_search,
                              BS_NIL,
                              ftxH);
            }

            if (sts != EOK)
            {
                domain_panic(ftxH.dmnP,
                             "idx_setup_for_truncation_int: rbf_pinpg or refpg failed");
                return(sts);
            }

            MS_SMP_ASSERT(node_ptr->total_elements > 0);

            index = node_ptr->total_elements - 1;
        }

        num_bytes = node_ptr->data[index].loc.free_size;

        /* Kick out if the truncation can no longer be performed */

        if (num_bytes < ADVFS_PGSZ)
        {
            MS_SMP_ASSERT(operation == TRUNC_QUERY);
            bs_derefpg(bfpgref,BS_CACHE_IT);

            return(0);
        }

        if ((num_bytes & (ADVFS_PGSZ - 1)) > 0)
        {
            /* This is not a page leave the remainder and remove
             * the rest */

            if (operation == TRUNC_UPDATE)
            {
                RBF_PIN_FIELD(rbfpgref,node_ptr->data[index].loc.free_size);
                node_ptr->data[index].loc.free_size = num_bytes & (ADVFS_PGSZ - 1);
                MS_SMP_ASSERT(node_ptr->data[index].loc.free_size >=
                              MIN_DIR_ENTRY_SIZE);
                num_bytes -= node_ptr->data[index].loc.free_size;

                /* Help out the undo routine by telling it the last offset in
                 * the directory of where this should be returned if we are
                 * failed. It should be glommed onto this entry  */
                
                *undo_space_start = node_ptr->data[index].search_key +
                    node_ptr->data[index].loc.free_size;
            }
            else
            {
                
                num_bytes -= (num_bytes & (ADVFS_PGSZ - 1));

            }
        }
        else
        {
            if (operation == TRUNC_UPDATE)
            {
                /* This is exactly a page multiple. Remove the entry from
                 * the node by decrementing the total_elements; */

                IDX_DEC_TOTAL(rbfpgref,node_ptr);

                /* Help out the undo routine by telling it the last offset in
                 * the directory of where this should be returned if we are
                 * failed. The entire entry should be put back  */
                
                *undo_space_start = node_ptr->data[index].search_key;
            }

        }

        if (operation == TRUNC_QUERY)
            bs_derefpg(bfpgref,BS_CACHE_IT);

        return(num_bytes);
    }

    bs_derefpg(bfpgref,BS_CACHE_IT);
    return(bytes_to_trunc);
}

/*******************************************************************
 * This routine is called with a search_key and a node_ptr. The
 * node_ptr is the first node found that is greater than or equal to
 * our search key. This routine will determine first if the node to
 * our left can be combined with the search_key and size (if they are
 * adjacent or overlap) then we will look at the node to our right
 * (the passed in node) to determine if we can also combine with it.
 *
 * We must also be careful to check if we are pointing past the last
 * entry in the passed in node. If we are then this means we are
 * greater than any search key in that node but less than the first
 * entry in the adjacent node to our right. If this is the case then
 * we must also bring in the node to our right and check if we can
 * glom with it.
 *
 * The directory manipulation routines (previous to this design) will
 * glom together entries that are adjacent it the directory file
 * itself. This needs to continue in order to support backwards
 * compatibility (otherwise I would change it in a second). When the
 * directory routine is done glomming it will call our insertion
 * routine with the glommed chunk of free space.  This means that
 * there will exist overlap in our b-tree which must be detected.
 *
 * The reason for calling us with the larger chunk rather than handing
 * us the smaller one and letting us glom it is that is gives us some
 * fault tolerance. The possibility exists in which a file could be
 * deleted from a directory and we go to put the free space on our
 * b-tree and the node needs to split, but there is not enough space
 * to split the node. We can hardly return ENOSPACE when the caller is
 * trying to delete a file!
 *
 * The solution is to skip adding the free space to our tree. This
 * will be lost space until an adjacent directory entry is deleted in
 * which case the directory could will glom it together and pass us
 * the entire chunk.
 *
 * In the normal case we will already have the adjacent piece in our
 * b-tree and the passed in chunk will overlap. It will then only be
 * necessary to increase the size and maybe the starting address
 * (depending on which side we are adjacent to). It will not be
 * necessary to look past the chunk in our tree for glomming since any
 * glomming would have taken place when it was placed in our tree the
 * first time.
 *
 * If the directory is eligible for truncation, set the flag in the
 * index parameters of the index's access structure. This will be
 * checked later.
 *
 *  Outputs:
 *       The following values could be returned:
 *
 *      DONE - The entry was glommed and no more work needs to be done.
 *      INSERT - No glom was possible do the insertion.
 *      UPDATE - Our parent needs to change its ranges.
 *******************************************************************/

int
idx_glom_entry_int(
    bfAccessT   *idx_bfap,      /* index files access structure */
    idxNodeT    *node_ptr,      /* Leaf node to examine */
    rbfPgRefHT   pgref,         /* page ref for pinrec */
    ulong        start_addr,    /* free space start    */
    ulong        size,          /* free space size     */
    uint32T      index,         /* node insertion point */
    ftxHT        ftxH
    )
{
    bfAccessT *dir_bfap;
    idxNodeEntryT *reuse_ptr = NULL;
    idxNodeT *right_node_ptr;
    idxNodeT *left_node_ptr;
    rbfPgRefHT left_pgref,right_pgref;
    uint32T right_index=-1;
    uint32T left_index;
    uint32T update_warning=FALSE;
    statusT sts;

    /* Check the right and left entries to see if we can glom them
     * together with the passed in entry. */

    /* Start by glomming with the entry to our left. It may be
     * necessary to bring in an entire node.  */

    dir_bfap=((bsIdxRecT *) idx_bfap->idx_params)->dir_bfap;

    /* The passed in index is really the right node that we
     * must check for glomming, except for the case below.
     */

    if ((index < node_ptr->total_elements) &&
        (node_ptr->data[index].search_key == start_addr))
        /* We need to allow for total overlap. We must check our right
         * in case the passed in size is larger. */
           index++;

    if (index == 0) /* Our left neighbor lies on another node ?*/
    {
        if (node_ptr->sib.page_left >= 0) /* yep */
        {

            /* If the previous entry is on another node then bring it
             * in and check the last entry in that node. Note as
             * discussed above we will check for greater than or equal
             * to in case of overlap.  */

            sts=rbf_pinpg(&left_pgref,
                          (void *) &left_node_ptr,
                          idx_bfap,
                          node_ptr->sib.page_left,
                          BS_NIL,
                          ftxH);

            if (sts != EOK)
            {
                domain_panic(ftxH.dmnP,"idx_glom_entry_int: rbf_pinpg failed");
                return(sts);
            }

            left_index = left_node_ptr->total_elements - 1;
                                                  /* Our left neighbor */
        }
        else left_node_ptr = NULL; /* We have no left sibling */
    }
    else
    {
        left_node_ptr = node_ptr;
        left_index = index-1;
        left_pgref = pgref;
    }

    /* The following tests if the entry being glommed overlaps
     * with the previous entry. If it does then it should either
     * totally overlap or straddle a 512 byte boundary
     */

    if ((left_node_ptr != NULL) &&
        (left_node_ptr->data[left_index].search_key +
         left_node_ptr->data[left_index].loc.free_size >= start_addr))
    {
        MS_SMP_ASSERT((start_addr ==
                       left_node_ptr->data[left_index].search_key) ||
                      ((start_addr & DIRBLKSIZ - 1) == 0));

        MS_SMP_ASSERT(left_node_ptr->data[left_index].search_key <= start_addr);

        reuse_ptr= &left_node_ptr->data[left_index];
        reuse_ptr->loc.free_size =
            start_addr -
            left_node_ptr->data[left_index].search_key +
            size;
    }

    /* Now check the node to the right (this is actually the passed in
     * node) to see if we can glom with it.  */

    if (index == node_ptr->total_elements)
    {
        /* We landed on the last entry of the left node. This because
         * we are either the last node in the tree or deletions caused
         * our node to be depleted lower than our parent's search key.
         * bring in the next node and set the warning flag.
         */

        if(node_ptr->page_right >= 0)
        {
            sts=rbf_pinpg( &right_pgref,
                       (void *) &right_node_ptr,
                       idx_bfap,
                       node_ptr->page_right,
                       BS_NIL,
                       ftxH);

            if (sts != EOK)
            {
                domain_panic(ftxH.dmnP,"idx_glom_entry_int: rbf_pinpg failed");
                return(sts);
            }

            if (right_node_ptr->total_elements == 0) right_node_ptr = NULL;
            right_index = 0;
            update_warning=TRUE;
        }
        else right_node_ptr = NULL; /* We have no right neighbor */
    }
    else
    {
        right_node_ptr = node_ptr;
        right_pgref = pgref;
        right_index = index;
    }

    if (reuse_ptr != NULL)
    {
        /* We know we are glomming to our left so if the right node
         * needs glomming then we will be removing it and adding the
         * removed size to the left node entry. */

        /* Subtract off the size of node to glom with the starting
         * address of the node to our right and see if we are less
         * than or equal which indicates that we either are
         * adjacent or overlap in which case glomming can be done */

        if((right_node_ptr != NULL) &&
           (right_node_ptr->data[right_index].search_key - start_addr
            <= size))
        {
            /*
             * This is a rather costly assertion so I will remove it for
             * now
             *  MS_SMP_ASSERT(((right_node_ptr->data[right_index].search_key -
             *              start_addr == size) &&
             *             ((right_node_ptr->data[right_index].search_key &
             *               DIRBLKSIZ-1) == 0))
             *            ||
             *            (right_node_ptr->data[right_index].search_key -
             *             start_addr +
             *             right_node_ptr->data[right_index].loc.free_size
             *             == size)
             *                        ||
             *            ((right_node_ptr->data[right_index].search_key -
             *              start_addr +
             *              right_node_ptr->data[right_index].loc.free_size
             *              > size)&&((start_addr+size)&DIRBLKSIZ-1)==0));
             */

            reuse_ptr->loc.free_size =
                right_node_ptr->data[right_index].search_key -
                reuse_ptr->search_key +
                right_node_ptr->data[right_index].loc.free_size;

            /* We were able to glom the right node and the left
             * node so delete the entry from the right node.  */

            if (right_index < right_node_ptr->total_elements - 1)
            {

                rbf_pin_record(right_pgref,
                               (char *)&right_node_ptr->data[right_index],
                               (sizeof(idxNodeEntryT)*
                                (right_node_ptr->total_elements -
                                 right_index - 1)));

                bcopy((char *)&right_node_ptr->data[right_index+1],
                      (char *)&right_node_ptr->data[right_index],
                      (sizeof(idxNodeEntryT)*
                       (right_node_ptr->total_elements -
                        right_index - 1)));
            }

            IDX_DEC_TOTAL(right_pgref,right_node_ptr);

            if ((right_node_ptr->total_elements <= IDX_COMPRESS_ME) &&
                (right_node_ptr->page_right >= 0))
            {
                /* We glommed entry from the right node over to the
                 * left node. In the process we notice that the right
                 * node dropped below the compression threshold and
                 * needs to be prunned. Since the original search brought
                 * us to the left node we need to force the prune to land on
                 * the right node so we must use a search key from the right
                 * node.
                 */

                  ((bsIdxRecT *)idx_bfap->idx_params)->prune_key_ffree = 
                      right_node_ptr->data[0].search_key;
            }
        }

        /* We must pin the node entry that we are reusing since we
         * have altered its size */

        RBF_PIN_FIELD(left_pgref,reuse_ptr->loc.free_size);
        MS_SMP_ASSERT(reuse_ptr->loc.free_size >=
                      MIN_DIR_ENTRY_SIZE);

        /* Check to see if directory truncation should be
         * performed. This test used to be done because the directory
         * was sequentially walked. Now we must sense it. We will
         * cause a directory truncation if the glommed entry is a page
         * or more and is the last entry in the b-tree (and therefore
         * the last page of the directory file).  */

        if ((reuse_ptr->loc.free_size >= ADVFS_PGSZ) &&
            (reuse_ptr->search_key +
             reuse_ptr->loc.free_size == dir_bfap->file_size))
        {
            ((bsIdxRecT *)idx_bfap->idx_params)->flags |= IDX_TRUNCATE;
        }

        return(DONE);
    }

    /*
     * Here we know that we did not glom to our left. We may however
     * need to glom to our right.
     *
     * Since we may end up glomming to our right we will need to
     * change the search key to match our search key. In other words
     * the free space we are glomming comes before the free space that
     * is located in the b-tree. The subtlety here is that by changing
     * the search key we may be affecting the search key in the parent
     * above us.
     *
     * By reducing its value we could actually go below the search
     * threshold for the node to our left causing us to be missed on a
     * lookup!
     *
     * The only time this matters is when we span two nodes. In this
     * case we are changing the first entry of the right node to be a
     * value that should be in the left_node. We must inform out parent
     * to reset its range.
     */

    if (right_node_ptr == NULL) /* we're the last entry in the tree */
    {
        return(INSERT);
    }

    if ( right_node_ptr->data[right_index].search_key - start_addr <= size )
    {
        /*
         *  MS_SMP_ASSERT(((right_node_ptr->data[right_index].search_key -
         *             start_addr == size) &&
         *             ((right_node_ptr->data[right_index].search_key &
         *              DIRBLKSIZ-1) == 0))
         *            ||
         *            (right_node_ptr->data[right_index].search_key -
         *             start_addr +
         *             right_node_ptr->data[right_index].loc.free_size
         *             == size)
         *            ||
         *            ((right_node_ptr->data[right_index].search_key -
         *             start_addr +
         *             right_node_ptr->data[right_index].loc.free_size
         *             > size)&&((start_addr+size)&DIRBLKSIZ-1)==0));
         */

        RBF_PIN_FIELD(right_pgref,right_node_ptr->data[right_index].search_key);
      RBF_PIN_FIELD(right_pgref,right_node_ptr->data[right_index].loc.free_size);
        right_node_ptr->data[right_index].loc.free_size +=
            right_node_ptr->data[right_index].search_key -
            start_addr;

        right_node_ptr->data[right_index].search_key = start_addr;
        MS_SMP_ASSERT(right_node_ptr->data[right_index].loc.free_size >=
                      MIN_DIR_ENTRY_SIZE);

        if ((right_node_ptr->data[right_index].loc.free_size >= ADVFS_PGSZ) &&
            (right_node_ptr->data[right_index].search_key +
             right_node_ptr->data[right_index].loc.free_size ==
             dir_bfap->file_size))
        {
            ((bsIdxRecT *)idx_bfap->idx_params)->flags |= IDX_TRUNCATE;
        }
        if ((right_index == 0) && (update_warning))
        {
            /* Oops we spanned two nodes. We must inform our parent to
             * update its range.
             */
            return(UPDATE);
        }

        return(DONE);
    }

    return(INSERT); /* No glomming possible */
}

/*******************************************************************
 * This routine will convert a directory to use an index file. The
 * conversion will consist of creating the index file and setting up
 * the directory's access structure.
 *
 * An important assumption is that this routine is only called when a
 * directory grows to a second page. In other words mapping the
 * directory entries can not use more than a SINGLE b-tree node !!!
 *
 * This will be a tailored routine to setup the index file. Rather
 * than go thru the insertion routine and start a sub transaction for
 * each insertion, the leaf node will be constructed under the
 * assumption that no splits will occur. This is why it is imperative
 * that the directory not exceed a page in size.
 *******************************************************************/

statusT
idx_convert_dir(
    bfAccessT *dir_bfap,
    struct nameidata *ndp,
    ftxHT ftxH
    )
{
    bfAccessT *idx_bfap;
    rbfPgRefHT file_pgref,free_pgref;
    bfPageRefHT dir_pgref;
    idxNodeT *filename_node, *freespace_node;
    uint32T free_idx, file_idx;
    uint32T last_freespace_end;
    uint32T pg_off;
    fs_dir_entry *dir_p, *first_p, *last_p;
    int n, offset;
    char *p;
    uint32T pageSz;
    dirRec *dirRecp;
    unsigned int last_ent_offset;
    statusT sts;

    /* Create the index file */

    sts = idx_create_index_file_int(dir_bfap,ftxH);

    if (sts != EOK)
    {
        /* Let our caller fail us. Pass the status up.
         */

        return(sts);
    }

    /* Bring in the filename node and the free space node */

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    sts = rbf_pinpg(&file_pgref,
                    (void *)&filename_node,
                    idx_bfap,
                    0,
                    BS_OVERWRITE,
                    ftxH
                    );
    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_convert_dir: rbf_pinpg failed");
        return(sts);
    }

    sts = rbf_pinpg(&free_pgref,
                    (void *)&freespace_node,
                    idx_bfap,
                    1,
                    BS_OVERWRITE,
                    ftxH
                    );
    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_convert_dir: rbf_pinpg failed");
        return(sts);
    }

    /* Bring in the directory's page */

    sts = bs_refpg(&dir_pgref,
                   (void *)&dir_p,
                   dir_bfap,
                   0,
                   BS_SEQ_AHEAD
                   );
    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_convert_dir: rbf_refpg failed");
        return(sts);
    }

    /* Up to this point we can be failed (meaning we can return a non-OK
     * status to our caller and they can fail us). Now we can no longer
     * tolerate a failure
     */

    /* Initialize each field in the root node. The NULL value for a
     *  page is -1.  These are unsigned so it corresponds to the
     *  largest page number which should never be reached in the index
     *  file.  */

    RBF_PIN_FIELD(file_pgref, filename_node->total_elements);
    RBF_PIN_FIELD(file_pgref, filename_node->page_right);
    RBF_PIN_FIELD(file_pgref, filename_node->sib.leftmost);
    RBF_PIN_FIELD(file_pgref, filename_node->reserved[0]);
    RBF_PIN_FIELD(file_pgref, filename_node->reserved[1]);

    filename_node->total_elements=0;
    filename_node->page_right=-1;
    filename_node->reserved[0] = 0;
    filename_node->reserved[1] = 0;

    /* The leftmost flag will only be set TRUE in a root node. When a
     *  new root is added this node will always be the leftmost since
     *  pruning and splitting always take place to the right.  */

    filename_node->sib.leftmost=TRUE;

    /* Similarly initialize each field in the free space b-tree's root
     *  node.  */

    RBF_PIN_FIELD(free_pgref, freespace_node->total_elements);
    RBF_PIN_FIELD(free_pgref, freespace_node->page_right);
    RBF_PIN_FIELD(free_pgref, freespace_node->sib.page_left);
    RBF_PIN_FIELD(free_pgref, freespace_node->reserved[0]);
    RBF_PIN_FIELD(free_pgref, freespace_node->reserved[1]);

    freespace_node->total_elements=0;
    freespace_node->page_right=-1;
    freespace_node->sib.page_left=-1;
    freespace_node->reserved[0] = 0;
    freespace_node->reserved[1] = 0;

    /*
     * get the pointer to the last entry in the page
     */
    first_p = dir_p;
    n = ADVFS_PGSZ - sizeof(dirRec);
    dirRecp = (dirRec *)((char *)dir_p + n);
    last_ent_offset = dirRecp->lastEntry_offset;
    last_p = (fs_dir_entry *)((char *)dir_p + last_ent_offset);

    /* Loop through each entry in the directory. If it is a filename
     * place it's hash value in the filename node, if it is free than
     * place it in the free space node. We know here that we do not
     * have to deal with glomming 512 byte chunks since this routine
     * is only called when a second page is needed for the directory
     * and that would not be the case if there were two 512 byte
     * chunks that needed glomming.  */

    free_idx=0;
    file_idx=0;

    while (dir_p <= last_p) {
        /* if the entry was deleted, check if it is a big enough slot
         * (in case we need one). if so, save it. Set new_space_flag
         * to FALSE to tell insert_seq that we are re-using an entry
         * (unless it's the pages last entry, then just return the
         * space...  */

        /* dir_p is a memory address so ignore the page since this is
         * page 0 of the directory we will just use the offset into
         * the page */

        pg_off = ((uint32T)dir_p) & (ADVFS_PGSZ - 1);

        if (dir_p->fs_dir_header.fs_dir_bs_tag_num == 0)
        {
            /* This goes in the free space tree. First make sure that
             * the previous free space entry was not the last piece of
             * a 512 byte boundary in which case we can glom these two
             * together.  */

            if ((free_idx > 0) &&
                (last_freespace_end == pg_off))
            {
                free_idx--;
                freespace_node->data[free_idx].loc.free_size +=
                    dir_p->fs_dir_header.fs_dir_size;
                last_freespace_end += dir_p->fs_dir_header.fs_dir_size;
                free_idx++;
            }
            else
            {

                /* If its the last entry in the page then add the
                 * entire remaining space (including the last record)
                 * and get out */

                if ((int)((char *)dir_p - (char *)first_p) ==
                    dirRecp->lastEntry_offset)
                {
                   freespace_node->data[free_idx].search_key = pg_off;
                   freespace_node->data[free_idx].loc.free_size =
                       ADVFS_PGSZ - pg_off;
                   free_idx++;
                   break;
                }

                /* Add an entry to the free space node */

                freespace_node->data[free_idx].search_key = pg_off;
                freespace_node->data[free_idx].loc.free_size =
                    (uint32T) dir_p->fs_dir_header.fs_dir_size;
                last_freespace_end = pg_off +
                    dir_p->fs_dir_header.fs_dir_size;
                free_idx++;
            }
        }
        else
        {
            /* This is a filename. Hash it and place it in the
             * filename node for now. It will be sorted later.  */

            filename_node->data[file_idx].search_key =
                idx_hash_filename_int (dir_p->fs_dir_name_string);
            filename_node->data[file_idx].loc.dir_offset = pg_off;
            file_idx ++;
        }

        /*
         * increment dir_p to the next dir entry
         */
        n = (signed)dir_p->fs_dir_header.fs_dir_size;
        p = (char *) dir_p;
        offset = (char *)dir_p - (char *)first_p;
        if ( FS_DIR_SIZE_OK(n, offset)) {
            /* there has been a minor data corruption, inform and error out */
            fs_dir_size_notice(dir_bfap, n, 0, offset);    
           /*
             * Resume processing on the next directory block boundary.
             */

            n = (DIRBLKSIZ + offset) & ~(DIRBLKSIZ-1);
            p = (char *)first_p;
        }
        dir_p = (fs_dir_entry *)(p + n);

        MS_SMP_ASSERT(free_idx <= IDX_MAX_ELEMENTS);
        MS_SMP_ASSERT(file_idx <= IDX_MAX_ELEMENTS);
    }

    /* end of 'while' loop */

    /* The free space node is already sorted but the filename node
     * needs to be sorted so do so now ... */

    dir_sort_int(filename_node,file_idx);

    /* Update the node header information and pin record the data. */

    rbf_pin_record(file_pgref,
                   &filename_node->data[0],
                   sizeof(idxNodeEntryT)*file_idx);

    RBF_PIN_FIELD(file_pgref,filename_node->total_elements);

    filename_node->total_elements = file_idx;

    rbf_pin_record(free_pgref,
                   &freespace_node->data[0],
                   sizeof(idxNodeEntryT)*free_idx);

    RBF_PIN_FIELD(free_pgref,freespace_node->total_elements);

    freespace_node->total_elements = free_idx;
    MS_SMP_ASSERT(freespace_node->total_elements <= IDX_MAX_ELEMENTS);

    /* We must initialize the nameidata hint. This will be set in
     * the initial lookup call followed by the call to insert or
     * delete. This needs to be set up noe because the insertion that
     * is causing this routine to run did not do a lookup via the index
     * file but will do an insertion into the index file and check the hint
     * if the hint is not initialized here bad things could happen.
     */

    ndp->ni_hint = (caddr_t)-1;

    /* Deref the dir page */

    bs_derefpg(dir_pgref, BS_CACHE_IT);

    return(sts);
}

/* This is a little utility routine to perform a sequential sort of an
 * array of hash file names */

void
dir_sort_int(
    idxNodeT *node_ptr,
    ulong size
    )
{
    ulong smallest;
    ulong i,j,si;
    idxNodeEntryT tmp;

    for(i=0;i<size;i++)
    {
        smallest = node_ptr->data[i].search_key;
        si = i;
        for(j=i;j<size;j++)
        {
            if (node_ptr->data[j].search_key < smallest)
            {
                smallest = node_ptr->data[j].search_key;
                si = j;
            }
        }
        if (si > i)
        {
            tmp = node_ptr->data[i];
            node_ptr->data[i] = node_ptr->data[si];
            node_ptr->data[si] = tmp;
        }
    }
}

/*******************************************************************
 * This routine will be part of a root transaction. It will call
 * a recursive function to actually walk and prune the b-tree
 * itself. Upon return from this recursive routine a message may be
 * sent to the clean up thread to finish the storage removal outside
 * of the root transaction.
 *
 * This routine MUST be called from a root transaction !!!!
 * No undo routine is provided for this (nor is one neeeded).
 *
 * If the root node has changed then the BMT MCELL for the index file
 * will be updated to reflect this.
 *******************************************************************/

statusT
idx_prune_start(
    bfAccessT *idx_bfap,
    ftxHT parentFtxH
    )
{
    statusT sts;
    ulong search_key;
    uint32T levels;
    uint32T old_root,new_root,pg_cnt;
    bsIdxRecT *idx_params;
    uint32T i;
    void * msg_array;
    ftxHT ftxH;

    idx_params = (bsIdxRecT *)idx_bfap->idx_params;

    /* Call the routine to do the work.*/

    /* Loop through both b-trees */

    for(i=FNAME;i<=FFREE;i++)
    {
        if (i == FNAME)
        {
            search_key = idx_params->prune_key_fname;
            idx_params->prune_key_fname=0;
            old_root = idx_params->bmt.fname_page;
            levels = idx_params->bmt.fname_levels;
        }
        else
        {
            search_key = idx_params->prune_key_ffree;
            idx_params->prune_key_ffree=0;
            old_root = idx_params->bmt.ffree_page;
            levels = idx_params->bmt.ffree_levels;
        }

        /* we must be careful not to exceed the maximum allowed pin
         * pages in an ftx This is currently FTX_MX_PINP. Rename may
         * cause us to pin pages (2) for the rename and then
         * potentially prune both b-trees. This can easily cause us to
         * exceed the FTX_MX_PINP. So start a sub ftx and disable any
         * undo's that way as long as this sub-ftx completes the node
         * will remain pruned regardless of whether or not the root
         * ftx is failed. This is fine since the b-trees will
         * logically be correct.  */

        if (search_key > 0)
        {
            sts = FTX_START_N( FTA_IDX_UNDO_V1,&ftxH, parentFtxH,
                               idx_bfap->dmnP, 0);
            if (sts != EOK)
            {
                domain_panic(parentFtxH.dmnP,
                             "idx_prune_start: ftx_start failed");
                return(sts);
            }

            new_root=old_root;
            sts=idx_prune_btree_int(old_root,
                                    levels,
                                    &search_key,
                                    &new_root,
                                    levels,
                                    idx_bfap,
                                    i,
                                    ftxH);
            if (sts < 0)
            {
                /*
                 * A failure below us is more than likely due to
                 * stg_remove_stg_finish. We can not call ftx_fail
                 * since we have pinned records. We will finish off
                 * the ftx and let our caller deal with the error.
                 *
                 * Turn off the undo's and allow whatever work was
                 * done to remain done.
                 *
                 * The caller will fail the ftx.
                 */

                /* We double duty status. If it is less than zero then
                 * we have and error condition and need to fail out
                 */

                goto finish_prune;
            }

            MS_SMP_ASSERT(sts != CONTINUE);
            sts=EOK;
            /* Also check to see if our root node has changed and if
             * so update the BMT MCELL.*/

            if (new_root != old_root)
            {
                /* We can tell which btree by comparing the root page to the
                 * passed in page since the root nodes can not occupy the same
                 * page.*/

                if (i == FNAME)
                {
                    idx_params->bmt.fname_page = new_root;
                    idx_params->bmt.fname_levels--;
                }
                else
                {
                    idx_params->bmt.ffree_page = new_root;
                    idx_params->bmt.ffree_levels--;
                }

                /* Push the new root page out to the MCELL record.*/

                sts = bmtr_put_rec(idx_bfap,
                                   BMTR_FS_INDEX_FILE,
                                   &((bsIdxRecT *)idx_bfap->idx_params)->bmt,
                                   sizeof( bsIdxBmtRecT ),
                                   ftxH);
                if (sts != EOK)
                {
                    if (sts != ENO_MORE_MCELLS)
                    {
                        domain_panic(ftxH.dmnP,
                                 "idx_prune_btree_start: bmtr_put_rec failed");
                    }
                    goto finish_prune;
                }
            }

            /* As explained above, disable the undoing of this sub ftx
             * upon completion
             */

        finish_prune:
            ftx_special_done_mode ( ftxH, FTXDONE_SKIP_SUBFTX_UNDO);
            ftx_done_n( ftxH,FTA_IDX_UNDO_V1);

            /* Since we just disabled undo of any storage removal
             * it is now save to send the cleanup thread a message
             * to call stg_remove_stg_finish. Normally this must be
             * done after the root ftx but in this case disabling
             * the undos is about the same thing.
             */

            if (idx_bfap->dirTruncp != NULL)
            {
                idx_prune_finish(idx_bfap);
            }
            if (sts != EOK)
                return(sts);
        }
    }

    return(EOK);
}

/*******************************************************************
 * It will use the passed in search key to locate the leaf node that
 * needs pruning. It will be determined if the node can be pruned and
 * if so the pruning will take place. This could propagate up to the
 * parent node and so forth.
 *
 * Because of the nature of the leftmost bit in the filename b-tree it
 * is important that the left most node always be pruned into. Also
 * The leftmost node will be the node that is pinned.
 *
 * The pruning algorithm will be to look at the right node to
 * determine if it can be copied into the current node. We will use
 * the right node since right pointers are maintained in both
 * trees. If the node can not be combined with its right neighbor then
 * the two nodes will be reorganized such that each node has an equal
 * number of elements.
 *
 * The rightmost node of any level will NOT be compressed. This is
 * because left pointers are not maintained in the filename tree and
 * much cost and complexity is added to find the left sibling. This
 * may cause a potential pruning to not occur but will not affect the
 * correctness of the b-tree. Eventually a left sibling will be hit
 * causing the prune to occur.
 *
 * There will be no undo routines since this is part of a root done
 * transaction.  If the transaction is failed none of the pruning
 * takes place.
 *
 * Level pruning will be done by the caller of this routine.  Level
 * pruning will take place from the top down. In other words after the
 * leaf pruning takes place and we have returned to the root node it
 * will be determined if the root nodes has only one child and if this
 * is the case then the root node will be deleted and the child will
 * become the new root node. The nature of this algorithm is such that
 * only a single level pruning will need to take place per call into
 * the prune routine.
 *
 * Another note is that we must be careful not to pin a page that is
 * going to be removed. Thus we will initially reference a page and if
 * we end up just redistributing elements the reference will be
 * upgraded to a pinpg.
 *
 *  True - Indicates that the node below caused the ranges
 *         at this node to need updating. This is only used
 *         in the recursion.
 *
 *  False - No ranges need to be updated.
 *******************************************************************/

int
idx_prune_btree_int (
    uint32T    node_page,
    uint32T    level,
    ulong      *search_key,
    uint32T    *page,
    uint32T    root_level,
    bfAccessT  *idx_bfap,
    uint32T     tree,
    ftxHT       ftxH
    )
{
    rbfPgRefHT dst_pgref,right_pgref,right2_pgref;
    uint32T index,index_to_remove;
    uint32T copy_index;
    idxNodeT *dst_node_ptr,*right_node_ptr,*right2_node_ptr;
    int32T adjust_range;
    uint32T new_root_page;
    int32T copy_cnt;
    uint32T status, redistribute;
    int right_page_right, right_page_left;
    statusT sts;

    /* We can pin each node as we traverse down the tree since all
     * compression will be into the left node from the right node,
     * except the root node which may be removed all together.
     * It may later be dereffed */

    MS_SMP_ASSERT(root_level > 0);

    sts=rbf_pinpg(&dst_pgref,
                  (void *)&dst_node_ptr,
                  idx_bfap,
                  node_page,
                  BS_NIL,
                  ftxH);

    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_prune_btree_int: rbf_pinpg failed");
        return(sts);
    }

    adjust_range = DONE;

    if (level > 0)
    {
        /* Search for the entry that will get us to the node that
         * contains our search key.*/

        MS_SMP_ASSERT(dst_node_ptr->total_elements > 0);

        index = idx_bsearch_int(dst_node_ptr,
                                *search_key,
                                (dst_node_ptr->page_right<0));

        MS_SMP_ASSERT(index < dst_node_ptr->total_elements);

        /*
         * Although close to impossible (i think), if the hashvalues
         * are the same for multiple nodes and even multiple parent
         * nodes, it must be handled. I tested this by forcing the hash
         * function to be the same for every file.
         *
         * Because this routine relies on returning to the proper parent
         * we can not follow the sibling links. We must return up
         * to the parent in order to access our sibling. Yes this is
         * very inefficient, but only in the massive collision case.
         */

        do
        {
            adjust_range = 
                idx_prune_btree_int(
                                    dst_node_ptr->data[index].loc.node_page,
                                    level-1,
                                    search_key,
                                    page,
                                    root_level,
                                    idx_bfap,
                                    tree,
                                    ftxH);
            if (adjust_range < 0)
            {
                /* We are in trouble. The value in adjust_range is an error
                 * status. Pass it back up
                 */
                return(adjust_range);
            }

            if(adjust_range == CONTINUE)
            {
                if (index < dst_node_ptr->total_elements)
                    index++;
                else
                {
                    /* We don't want too many pinned pages. */
                    rbf_deref_page(dst_pgref,BS_NIL);
                    return(CONTINUE);
                }
            }

        } while(adjust_range == CONTINUE);
    }

    if ((level == root_level) &&
        (adjust_range == REMOVE) &&
        (dst_node_ptr->total_elements <= 2))
    {
        /* This is the special case situation where the root has only
         * 1 child. We now can prune the tree depth by removing this
         * root node.  The first time we come in here, the root will
         * have two elements.  If the attempt to prune the root itself
         * fails, we'll drop to one element in the root.  Later, if
         * the one remaining child drops below the prune threshold, we
         * will come back into this branch of code but with only one
         * element in the root.  
         */

        new_root_page = dst_node_ptr->data[0].loc.node_page;
        rbf_deref_page(dst_pgref,BS_CACHE_IT);

        /* remove the page of storage from the b-tree */

        sts = idx_remove_page_of_storage_int(idx_bfap,node_page,ftxH);
        if (sts != EOK)
        {
            /* We can get an ENO_MORE_MEMORY here so just
             * and pass the status up.
             */

            /* 
             * If the root has two elements and we've successfully
             * pruned one of its children, we need to reduce the
             * root's total_elements to remove any reference to the
             * now-removed child.
             */
            if (dst_node_ptr->total_elements == 2) {
                sts=rbf_pinpg(&dst_pgref,
                              (void *)&dst_node_ptr,
                              idx_bfap,
                              node_page,
                              BS_NIL,
                              ftxH);

                if (sts != EOK) {
                    domain_panic(ftxH.dmnP,
                                 "idx_prune_btree_int: root re-pin failed");
                    return(sts);
                 }
                 RBF_PIN_FIELD(dst_pgref, dst_node_ptr->total_elements);
                 dst_node_ptr->total_elements = 1;
                 idxStats.prune_root_2_failed++;
            }
            else {
                 idxStats.prune_root_1_failed++;
            }
            return (sts);
        }

        /* Inform that a new root must be stored away in the bfap.*/

        *page=new_root_page;

        return(DONE);
    }

    /* Check to see if this node can be compressed. If its size is
     * less than 1/4 full then compression will take place.
     * If it is a leaf node and we are above the threshold then
     * we probably have have a collision spanning two nodes in
     * which case the search key actually is for the right sibling
     */

    if ((dst_node_ptr->page_right >= 0) &&
        (((level == 0) && (dst_node_ptr->total_elements <= IDX_COMPRESS_ME)) ||
         ((adjust_range == REMOVE) &&
          (((dst_node_ptr->total_elements <= IDX_COMPRESS_ME) ||
            (index == dst_node_ptr->total_elements - 1))))))
    {
        /* We need to force a compression when adjust_range == REMOVE
         * and we are at the last slot of the node. This is because the
         * slot that needs to be removed actually lives on our sibling.
         * by removing it from our sibling we could cause it to drop below
         * the prune threshold. By forcing a compression we will insure the
         * sibling never drops below.
         */

        /* This node needs compression. Bring in the right sibling*/

        sts=rbf_pinpg (&right_pgref,
                   (void *)&right_node_ptr,
                   idx_bfap,
                   dst_node_ptr->page_right,
                   BS_NIL,
                   ftxH);

        if (sts != EOK)
        {
            domain_panic(ftxH.dmnP,"idx_prune_btree_int: rbf_pinpg failed");
            return(sts);
        }

         /* The only way we can be a non-leaf node that needs to
          * be compressed, is if a child below us was removed causing
          * this node to need an entry removed (at index). Incorporate this
          * into the compression*/

        /* We must not fill a node when compressing. This is due to
         * the fact that the prune disables all undos. Thus if the
         * root transaction fails but the prune subftx was sucessfull,
         * the element that caused the node to be pruned will be
         * undone (reinserted) but the pruning will not be. This would
         * result in the prunned node being inserted into. We must
         * make sure that this insertion can fit into the node without
         * causing it to split, since we can not obtain storage from
         * an undo transaction.
         */

        redistribute = ((dst_node_ptr->total_elements + 
                         right_node_ptr->total_elements + 1)
                        >= IDX_MAX_ELEMENTS);

        copy_index=0;

        if(adjust_range == REMOVE)
        {
            /* If the node below us that was removed had its parent
             * as our sibling then we will catch it here */

            index_to_remove = index+1;

            if (index_to_remove <= dst_node_ptr->total_elements-1)
            {
                 /* The removal lies entirely on this node */

                 if (index_to_remove < dst_node_ptr->total_elements-1)
                 {
                     /* Only pin and copy if necessary */

                     rbf_pin_record(dst_pgref,
                                    &dst_node_ptr->data[index_to_remove],
                                (dst_node_ptr->total_elements-index_to_remove-1)*
                                    sizeof(idxNodeEntryT));

                     bcopy(&dst_node_ptr->data[index_to_remove+1],
                           &dst_node_ptr->data[index_to_remove],
                           (dst_node_ptr->total_elements-index_to_remove-1)*
                           sizeof(idxNodeEntryT));
                 }

                 IDX_DEC_TOTAL(dst_pgref,dst_node_ptr);
             }
             else
             {
                 /* The index to remove lies on the other node. This is yet
                  * another special case situation. We need to be careful here
                  * not to allow the sibling node to drop below the prune
                  * threshold. If we did then eventually it could reach zero
                  * which can not be tolerated
                  *
                  * Since this is a relatively rare case we will force a
                  * pruning or redistribution regardless of wether the current
                  * node needs it. This avoids a lot of special caseing.
                  *
                  * We must perform the redistribution here if the right_node is
                  * smaller than the current node. Otherwise we can let it fall
                  * thru.
                  */

                 MS_SMP_ASSERT(right_node_ptr->total_elements > 0);

                 if ((redistribute) &&
                     (dst_node_ptr->total_elements >= 
                      right_node_ptr->total_elements))
                 {
                     /* We will first update the search key. Since we allow the
                      * copy_cnt to be zero we need to pin this node record.
                      */

                     dst_node_ptr->data[index].search_key = *search_key;
                   RBF_PIN_FIELD(dst_pgref,dst_node_ptr->data[index].search_key);

                     idxStats.redistribute_right++;

                     copy_cnt = (((right_node_ptr->total_elements-1 +
                                   dst_node_ptr->total_elements) >>1) -
                                 (right_node_ptr->total_elements-1));

                     MS_SMP_ASSERT(copy_cnt >= 0);

                     /* Make room at the beginning of the node by
                      * moving the existing slots down by the numer we
                      * will copy in. Since we are removing the first
                      * slot, We will not both moving it down.
                      */

                     rbf_pin_record(right_pgref,
                                    &right_node_ptr->data[0],
                                    (copy_cnt + 
                                     right_node_ptr->total_elements - 1)*
                                    sizeof(idxNodeEntryT));

                     RBF_PIN_FIELD(right_pgref,right_node_ptr->total_elements);
                     RBF_PIN_FIELD(dst_pgref,dst_node_ptr->total_elements);

                     if (copy_cnt != 1)
                     {
                         bcopy(&right_node_ptr->data[1],
                               &right_node_ptr->data[copy_cnt],
                               (right_node_ptr->total_elements-1)*
                               sizeof(idxNodeEntryT));
                     }
                     /* The left sibling can now be copied over. */

                     if (copy_cnt > 0)
                     {
                         bcopy(&dst_node_ptr->data[dst_node_ptr->total_elements 
                               - copy_cnt],
                               &right_node_ptr->data[0],
                               copy_cnt*sizeof(idxNodeEntryT));

                         dst_node_ptr->total_elements -= copy_cnt;
                     }

                     right_node_ptr->total_elements += (copy_cnt - 1);
                     MS_SMP_ASSERT(right_node_ptr->total_elements <= 
                                   IDX_MAX_ELEMENTS);

                     /* Inform our parent that the range must be adjusted.*/

                   *search_key = 
                   dst_node_ptr->data[dst_node_ptr->total_elements-1].search_key;
                     return(MODIFY);
                 }

                 copy_index = 1; /* The index to remove lies on the
                                    other node */
             }
         }

         if (redistribute)
         {
             /* We can only redistribute between these nodes. So set up
              * the proper copy counts
              */

             copy_cnt = (((right_node_ptr->total_elements - copy_index +
                           dst_node_ptr->total_elements) >>1) -
                         dst_node_ptr->total_elements);

         }
         else
         {
             copy_cnt = right_node_ptr->total_elements - copy_index;
         }

         MS_SMP_ASSERT(copy_cnt >= 0);

         /* The right sibling can now be copied over in rare cases the
          * copy count could be zero, we will let the routines handle
          * it rather than special case it*/

         if (copy_cnt > 0)
         {
             rbf_pin_record(dst_pgref,
                            &dst_node_ptr->data[dst_node_ptr->total_elements],
                            copy_cnt*sizeof(idxNodeEntryT));
             bcopy(&right_node_ptr->data[copy_index],
                   &dst_node_ptr->data[dst_node_ptr->total_elements],
                   copy_cnt*sizeof(idxNodeEntryT));

             dst_node_ptr->total_elements += copy_cnt;
             MS_SMP_ASSERT(dst_node_ptr->total_elements <= IDX_MAX_ELEMENTS);
         }

         status = REMOVE;

         if (redistribute)
         {

             /* We were only able to redistribute elements evenly
              * amongst two nodes Now we must ripple the source nodes
              * elments down */

             rbf_pin_record(right_pgref,
                            &right_node_ptr->data[0],
                            (right_node_ptr->total_elements-
                             (copy_cnt+copy_index))*sizeof(idxNodeEntryT));
             RBF_PIN_FIELD(right_pgref,right_node_ptr->total_elements);
             bcopy(&right_node_ptr->data[copy_cnt+copy_index],
                   &right_node_ptr->data[0],
               (right_node_ptr->total_elements-
                (copy_cnt + copy_index))*sizeof(idxNodeEntryT));

             right_node_ptr->total_elements -= (copy_cnt+copy_index);
             MS_SMP_ASSERT(dst_node_ptr->total_elements <= IDX_MAX_ELEMENTS);

             status = MODIFY;
         }

         if(adjust_range == REMOVE)
         {
             /* Now we will update the search key */
             RBF_PIN_FIELD(dst_pgref,dst_node_ptr->data[index].search_key);
             dst_node_ptr->data[index].search_key = *search_key;
         }

         /* This pin should be moved if before image logging is ever
          * done. For now it is here to avoid multiple pin records
          * of the same field */

         RBF_PIN_FIELD(dst_pgref,dst_node_ptr->total_elements);

         if (status == REMOVE)
         {
             /* Before we remove the node we must save off any data
              * that we need
              */

             node_page = dst_node_ptr->page_right; /* page to delete */
             right_page_right = right_node_ptr->page_right;

             /* the next assignment might be bogus but it doesn't
              * matter
              */

             right_page_left = right_node_ptr->sib.page_left;

             /* To cut down on the number of concurrent pins, we will save any
              * info we need and unpin the page that is about to be removed.
              */

             rbf_deref_page(right_pgref, BS_NIL);

             /* remove the page of storage from the b-tree */

             if (level == 0)
             {
                 /* This is a bit confusing but, removing storage is a
                  * difficult matter, we know that if the leaf node
                  * removes storage, parent nodes may also and if we
                  * don't they wont. Anyway, Get an array of storage
                  * for placing the messages for the cleanup thread
                  * for sending later outside of a root ftx.
                  *
                  * We will place this in the index access structure
                  * piggy backing the existing dirTruncp mechanism
                  * already in place. If a previous Trunc package
                  * is already present just reuse it.
                  */
                 if (idx_bfap->dirTruncp == NULL)
                 {
                     idx_bfap->dirTruncp = 
                         (void *)ms_malloc(sizeof(idxPruneMsgsT));
                     if (idx_bfap->dirTruncp != NULL)
                     {
                         ((idxPruneMsgsT *)idx_bfap->dirTruncp)->msg_cnt=0;
                     }
                     else
                     {
                         /* This is all we need to do to retore the page. Just
                          * put the count back, since our right sibling is
                          * already correct.
                          */

                         RBF_PIN_FIELD(dst_pgref,dst_node_ptr->total_elements);
                         dst_node_ptr->total_elements -= copy_cnt;

                         /* If we can't get memory skip the truncation */
                         return(ENO_MORE_MEMORY);
                     }
                 }
             }

             sts=idx_remove_page_of_storage_int(idx_bfap,
                                                node_page, /* from above */
                                                ftxH);
             if (sts != EOK)
             {
                 /* This is royally confusing. If we fail to remove the storage
                  * we want to get out of here without panicing. In order to do
                  * this we must not fail any transactions, since we have
                  * already pinned pages.
                  * We will put back together the page we just tried to remove.
                  * Then we will return an error up. Any pages below us that may
                  * have been removed can stay that way since we haven't really
                  * changed the logic of the b-tree.
                  *
                  * If the level below us was able to remove storage but we are
                  * not, it is ok since we have already update our node to
                  * reflect this. Our parent does not have anything to do unless
                  * we ourselves were to compress.
                  */

                 /* This is all we need to do to retore the page. Just put the
                  * count back, since our right sibling is already correct.
                  */

                 RBF_PIN_FIELD(dst_pgref,dst_node_ptr->total_elements);
                 dst_node_ptr->total_elements -= copy_cnt;

                 /* We can get an ENO_MORE_MEMORY here so just
                  * and pass the status up.
                  */

                 /*
                  * Abort the prune without returning an error if the count
                  * is greater than one. We can allow a node to not be prunned
                  * as long as the count stays above zero. I am picking 4 as
                  * a minimum node size because during a single transaction
                  * the node can have multiple elements removed from it. So
                  * by not allowng a node to go below 4 we should insure that
                  * during a single transacation is stays above zero.
                  *
                  */

                 if(dst_node_ptr->total_elements > 4)
                 {
                     idxStats.prune_btree_incomplete++;
                     return(DONE);
                 }

                 idxStats.prune_btree_failed++;
                 return (sts);
             }

             /* OK, we were able to remove the page. Update the
              * sibling pointers with the data that we squirreled
              * away prior to removing the page.
              */

             RBF_PIN_FIELD(dst_pgref,dst_node_ptr->page_right);
             dst_node_ptr->page_right = right_page_right;

             if(tree == FFREE)
             {
                 /* In this tree we maintain back pointers so we must
                  * update our right's siblings right sibling (which
                  * is now our right sibling */

                 if(right_page_right >= 0)
                 {
                     sts=rbf_pinpg (&right2_pgref,
                                    (void *)&right2_node_ptr,
                                    idx_bfap,
                                    dst_node_ptr->page_right,
                                    BS_NIL,
                                    ftxH);

                     if (sts != EOK)
                     {
                         domain_panic(ftxH.dmnP,
                                      "idx_prune_btree_int: rbf_pinpg failed");
                         return(sts);
                     }

                     right2_node_ptr->sib.page_left = right_page_left;
                     RBF_PIN_FIELD(right2_pgref,right2_node_ptr->sib.page_left);
                 }
             }
         }

         /* Inform our parent that the range must be adjusted.*/
         *search_key = 
             dst_node_ptr->data[dst_node_ptr->total_elements-1].search_key;
         return(status);
    }
    else
    {
        if ((level == 0) &&
            (dst_node_ptr->total_elements > IDX_COMPRESS_ME) &&
            (dst_node_ptr->page_right >= 0))
        {
            /* This case should only be hit when we have collisions
             * that wrap across nodes. Since we only pass the search key
             * we land on the leftmost node containing collisions. If the
             * node to be prunned actually is our right sibling we must
             * return to our parent to bring it in.
             */
            rbf_deref_page(dst_pgref,BS_NIL);
            return(CONTINUE);
        }
    }

    if (adjust_range == REMOVE)
    {
        /* This node is not a candidate for compression but we do need to
         * remove the range for our child that was removed.
         *
         * If the index is at the end of the node then the child that
         * was removed actually lives on the sibling to our right, otherwise
         * we only need to remove it from this node
         */

        MS_SMP_ASSERT (index < dst_node_ptr->total_elements-1);

        /* This node needs updating, Lets reuse some of our local
         * variables */

        right_node_ptr = dst_node_ptr;
        right_pgref = dst_pgref;
        index_to_remove = index+1;

        /* Ripple down the slots into the removed slot and pin the changes */

        if (index_to_remove < dst_node_ptr->total_elements - 1)
        {

            rbf_pin_record(right_pgref,
                           &right_node_ptr->data[index_to_remove],
                           (right_node_ptr->total_elements-index_to_remove-1)*
                           sizeof(idxNodeEntryT));
            bcopy(&right_node_ptr->data[index_to_remove+1],
                  &right_node_ptr->data[index_to_remove],
                  (right_node_ptr->total_elements-index_to_remove-1)*
                  sizeof(idxNodeEntryT));
        }

        IDX_DEC_TOTAL(right_pgref,right_node_ptr);

        /* Fall thru to the modify code below */

        adjust_range = MODIFY;
    }
    if (adjust_range == MODIFY)
    {
        RBF_PIN_FIELD(dst_pgref,dst_node_ptr->data[index].search_key);
        dst_node_ptr->data[index].search_key = *search_key;

        if (index == dst_node_ptr->total_elements-1)
        {
            /* We just updated (increased - an important distinction) the
             * the last entry in a node. This means that our parent MUST
             * update its entry for us since we now contain elements greater
             * than the search key our parent has for us !
             */

            return(MODIFY);
        }
    }
    return(DONE);
}

/********************************************************************
 * This is a utility to start the removal of a page of storage in the
 * b-tree. This will then load up and array that lives in the index
 * access stucture (put there by the leaf node). This
 * array will then be used outside of the root ftx to send message
 * to the cleanup thread to finish removing the storage.
 *******************************************************************/

statusT
idx_remove_page_of_storage_int(
    bfAccessT *idx_bfap,
    uint32T page_to_remove,
    ftxHT ftxH
    )
{
    uint32T delCnt;
    void *delList;
    idxPruneMsgsT *pmsgs;
    statusT sts;

    pmsgs = (idxPruneMsgsT *)idx_bfap->dirTruncp;

    /* Don't let stg_remove_stg_start release the quota's
     * (relquota argument is FALSE) since we need to use the
     * directory's bfap*/

    sts = stg_remove_stg_start (idx_bfap,
                                page_to_remove,
                                1,
                                FALSE,   /* Don't adjust quotas! */
                                ftxH,
                                &pmsgs->msgs[pmsgs->msg_cnt].delCnt,
                                &pmsgs->msgs[pmsgs->msg_cnt].delList,
                                TRUE       /* do COW */
                                );

    if ( sts != EOK )
    {
        /* An interesting case here. We can actually run out of space trying
         * to give it back. Pass this up to the caller after failing this ftx.
         */

        idxStats.remove_page_of_storage_failed++;
        return(sts);
    }


    /* adjust the quotas via the real directory context */
    sts = change_quotas(((bsIdxRecT *)idx_bfap->idx_params)->dir_bfap->bfVp,
                        0, -(long)ADVFS_PGSZ_IN_BLKS, NULL, u.u_cred, 0, ftxH);
    
    if ( sts != EOK )
    {
        /* Not sure this should ever fail legitimately */
        return(sts);
    }

    /* First phase of storage removal is done; remainder must be done
     * outside of the current transaction.  Save the info required to
     * complete the storage deallocation in a struct hanging off
     * the bfAccess.  The calling routine will invoke idx_prune_finish()
     * once this entire transaction has been committed.
     */

    pmsgs->msg_cnt++;
    return(EOK);
}

/********************************************************************
 * This routine is called after the root transaction that might have
 * started a index pruning successfully commits.  This routine checks
 * if there is index pruning info associated with the bfAccess
 * struct. If so, it is packaged into message(s), and sent to the
 * cleanup thread.
 *
 * Note that there can be only one idxPruneMsgs struct hanging off the
 * access structure at a time.  If the root transaction commits, then
 * this record is removed and deallocated by this routine.  If the
 * root transaction aborts, then this routine is never called, and the
 * record is left dangling, never to be used.  If the access structure
 * is recycled, the dangling record is deallocated; if there is a
 * subsequent prune on the same index file before the access structure
 * is recycled, then the old record is overwritten with the new
 * information in idx_prune_start.
 *
 * NOTE:
 *  1.  The mecahnism for starting and finishing the storage removal is
 *      borrowed from the existing directory truncation code.
 *
 *  2.  This routine MUST be called after the transaction that called
 *      idx_prune_start is committed. This ensures that the removal
 *      of storage can be failed.
 *
 *********************************************************************/

void
idx_prune_finish(
    bfAccessT *idx_bfap
    )
{
    idxPruneMsgsT *pmsgs;
    clupThreadMsgT *msg;
    extern msgQHT CleanupMsgQH;

    MS_SMP_ASSERT(idx_bfap->dirTruncp);

    pmsgs = (idxPruneMsgsT *)idx_bfap->dirTruncp;
    idx_bfap->dirTruncp = (void *)NIL;

    while(pmsgs->msg_cnt > 0)
    {
        /* Send a message to the cleanup thread to do the actual work */
        msg = (clupThreadMsgT *)msgq_alloc_msg( CleanupMsgQH );
        if (msg) {
            msg->msgType = FINSH_DIR_TRUNC;
            msg->body.dtinfo.delCnt = pmsgs->msgs[pmsgs->msg_cnt-1].delCnt;
            msg->body.dtinfo.delList = pmsgs->msgs[pmsgs->msg_cnt-1].delList;
            msg->body.dtinfo.domain.id = idx_bfap->dmnP->domainId;
            msgq_send_msg( CleanupMsgQH, msg );
        }
        pmsgs->msg_cnt--;
    }
    ms_free(pmsgs);
    return;
}

/********************************************************************
 * This is the general undo routine for
 *
 * Any space that was added to the filename b-tree because of this
 * insertion will not be undone. Only the logical removal of the
 * filename needs to be undone. Since the addition of storage will not
 * be undone (this was set up), and the entire operation takes place
 * within a single subtransaction, as long as the subtransaction was
 * completed the b-tree will remain consistent.
 ********************************************************************/

void
idx_undo_opx(
    ftxHT ftxH,
    int opRecSz,
    void *opRec
    )
{
    idxUndoRecordT recp;
    statusT sts;
    bfSetT *bfSetp;
    bfAccessT *idx_bfap, *dir_bfap;
    bsIdxRecT *idx_params;
    uint32T insert;
    int found;
    int split_count=-1; /* this must be initialized */
    idxPinnedPgsT pinpgs[IDX_MAX_BTREE_LEVELS];
    bfTagT invalid_tag=NilBfTag;
    domainT *dmnP;
    struct vnode *nullvp = NULL;

    /* The undo transaction can make no assumptions about what has
     * happened so far. So first open the fileset itself.*/

    MS_SMP_ASSERT(opRecSz == sizeof(idxUndoRecordT));

    bcopy(opRec,&recp,opRecSz);

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(recp.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(recp.bfSetId.domainId, dmnP->dualMountId)) )
        {
            recp.bfSetId.domainId = dmnP->domainId;
        } else {
            ADVFS_SAD0("idx_undo_opx: domainId mismatch");
        }
    }

    sts = rbf_bfs_open( &bfSetp, recp.bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) {
        domain_panic(ftxH.dmnP,"idx_undo_opx: rbf_bfs_open failed");
        return;
    }

    /* Similarly we must also open the directory.*/

    sts = bs_access( &dir_bfap,
                     recp.dir_tag,
                     bfSetp,
                     ftxH,
                     0,
                     NULLMT,
                     &nullvp
                     );

    if (sts != EOK) {
        domain_panic(ftxH.dmnP,"idx_undo_opx: bs_access failed");
        return;
    }

    /* We know that :
     *
     * 1) If this is a result of recovery being run, the index file
     *    has not yet been opened.
     * 2) If this is a call to ftx_fail, the index file MUST already
     *    be open.
     *
     * Given the above facts, The index will only be opened and closed
     * by this routine if this is during recovery. The directory will
     * be opened and closed regardless. The call to idx_open_index_file
     * will only open the index if it isn't already.
     *
     * This needs to be done explicitly because the close of the directory
     * will not trigger a close of the index file. This will only be triggered
     * via the close of an external open of the directory.
     */

    sts = idx_open_index_file(dir_bfap,ftxH);

    if (sts != EOK)
    {
        domain_panic(ftxH.dmnP,"idx_undo_opx: idx_open_index_file failed");
        return;
    }

    if ( dir_bfap->idx_params == (void*)-1 && recp.action == IDX_CREATE_UNDO ) {
        /* The BMT record did not get out to disk. The putting of
         * the BMT record is purposefully outside of the index creation
         * just for this window. Just return since there is nothing to
         * undo.  */
        goto idx_undo_exit;
    }

    idx_bfap = ((bsDirIdxRecT *)dir_bfap->idx_params)->idx_bfap;

    switch(recp.action)
    {
    case IDX_INSERT_FILENAME_UNDO :
        /* Remove the filename (hashvalue) from the b-tree by directly
         * calling the internal routine. We can not start a
         * subtransaction. We can ignore the pruning status return since
         * the state of the node could not have changed since the
         * insertion took place.*/

        sts=idx_remove_filename_int(
              ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_page,
              ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels,
              recp.nodeEntry.search_key,
              recp.nodeEntry.loc.dir_offset,
              idx_bfap,
              &found,
              ftxH);
        if ((sts != EOK) || !(found))
        {
            domain_panic(ftxH.dmnP,"idx_undo_opx: remove_filename_int failed");
        }

        break;

    case IDX_REMOVE_FILENAME_UNDO :
        /* Insert the filename (hashvalue) to the b-tree by directly
         * calling the internal routine. We can not start a
         * subtransaction. We can ignore the split parameters since we are
         * inserting an entry that we just removed which guarantees that
         * space is available and no split will happen.*/

        sts=idx_insert_filename_int(
            ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_page,
            ((bsIdxRecT *)idx_bfap->idx_params)->bmt.fname_levels,
            &recp.nodeEntry.search_key,
            &recp.nodeEntry.loc.dir_offset,
            idx_bfap,
            &insert,
            &split_count,      /* Not used */
            pinpgs,      /* Not used */
            ftxH);

        if (sts != EOK)
        {
            domain_panic(ftxH.dmnP,
                         "idx_undo_opx: idx_insert_filename_int failed");
        }

        MS_SMP_ASSERT((split_count == 0) && (insert == 0));
        break;

    case IDX_DIRECTORY_INSERT_SPACE_UNDO :

        /* This is a specialized undo routine for removing a specific
         * piece of space. Normally we remove the first piece that
         * fits.
         */

        sts=idx_directory_insert_space_undo(&recp,idx_bfap,ftxH);
        break;

    case IDX_SETUP_FOR_TRUNCATION_INT_UNDO :
    case IDX_DIRECTORY_GET_SPACE_UNDO :

        /* Insert the space to the b-tree by directly calling the internal
         * routine. We can not start a subtransaction. We can ignore the
         * pruning status return since the state of the node could not
         * have changed since the insertion took place.*/

        sts=idx_directory_insert_space_int(
            ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_page,
            ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels,
            &recp.nodeEntry.search_key,
            &recp.nodeEntry.loc.free_size,
            idx_bfap,
            ftxH,
            &split_count,
            pinpgs,
            0
            );
        if (sts < 0)
        {
            domain_panic(ftxH.dmnP,
                         "idx_undo_opx: idx_directory_insert_space_int failed");
        }
        MS_SMP_ASSERT(sts == DONE);
        break;

    case IDX_CREATE_UNDO :

        /* To reach here we must have failed creating the index file. Probably
         * because we are out of space. At this point the the directory's
         * access structure has the idx_params set up to associate it with
         * the index. We must remove this association as well as indicate
         * on disk that there is no index file. Unfortunately there is no
         * way to just remove a record from an MCELL without removing the
         * entire MCELL. So, we will put out a NIL tag value and check it
         * whenever openning the index file.
         *
         * Since we break the association with the index file, we must
         * manually close it here.
         */

        MS_SMP_ASSERT(dir_bfap->idx_params != NULL);

        ms_free(dir_bfap->idx_params);
        dir_bfap->idx_params = (void *) -1;

        bs_close(idx_bfap, 0);

        sts = bmtr_update_rec( dir_bfap,
                               BMTR_FS_DIR_INDEX_FILE,
                               (void *) &invalid_tag,
                               sizeof(bfTagT ),
                               ftxH,
                               0);

        /* It may not be there in which case our job is done */

        if (sts != EOK)
        {
            domain_panic(ftxH.dmnP,"idx_undo_opx: bmtr_put_rec");
        }

        goto idx_undo_exit;
    }

    /* We do not need to explicitly close the index file. If it is the
     * last close on the directory then bs_close will call msfs_inactive
     * which will close the index file
     */

    if(dmnP->state != BFD_ACTIVATED)
    {
        /* As mentioned above, since we opened the directory via an
         * internal open, we must explicitly close the index. The
         * closing of the directory in this case will not trigger it
         */

        idx_close_index_file(dir_bfap,IDX_RECOVERY);
    }

    idx_undo_exit:

    sts = bs_close(dir_bfap, 0);

    if (sts != EOK) {
        domain_panic(ftxH.dmnP,"bs_close error in idx_undo_opx");
        return;
    }
    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

    return;
}

/*******************************************************************
 * This is the undo routine for the insertion of free space into the
 * free space b-tree. It will remove free space entry from the free
 * space b-tree. Other undo routines will undo their pieces.
 *
 * This routine can not call the counter part routine because it
 * searches for the first fit. this may not be the same space that was
 * inserted and needs to be removed. We must therefore do the work
 * ourselves.
 *
 * Any pages that were added to the free space b-tree because of this
 * insertion will not be undone. Only the logical removal of the
 * filename space needs to be undone. Since the addition of storage
 * will not be undone (this was set up), and the entire operation
 * takes place within a single subtransaction, as long as the
 * subtransaction was completed the b-tree will remain consistent.
 ********************************************************************/

statusT
idx_directory_insert_space_undo(
    idxUndoRecordT *recp,
    bfAccessT *idx_bfap,
    ftxHT ftxH
    )
{
    idxNodeT *right_node_ptr;
    idxNodeT *node_ptr;
    rbfPgRefHT pgref,right_pgref;
    uint32T index;
    ulong new_search_key;
    ulong start_addr, entry_sa;
    ulong size,entry_sz;
    int node_page;
    statusT sts;

    /* Remove the entry from the b-tree by directly calling the
     * internal routine. We can not start a subtransaction. We can
     * ignore the pruning status return since the state of the node
     * could not have changed since the insertion took place.*/

    size = recp->nodeEntry.loc.free_size;

    start_addr = recp->nodeEntry.search_key;

    /* Get the node containing the entry to be removed.*/

    right_node_ptr=NULL;

    sts=idx_lookup_node_int(((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_page,
                        ((bsIdxRecT *)idx_bfap->idx_params)->bmt.ffree_levels,
                        start_addr,
                        idx_bfap,
                        &node_ptr,
                        &pgref,
                        &index,
                        TRUE,
                        &node_page,
                        ftxH);

    if (sts != EOK)
        return(sts);

    /* Since performance is not an issue, we will remove the entry and
     * if it was glommed insert any neighboring pieces back in. We
     * should be safe here since the insertions will only be necessary
     * if the transaction that is being undone caused the glom to
     * happen in which case we are only undoing what we did.*/

    if ((index == node_ptr->total_elements) ||
        (node_ptr->data[index].search_key > start_addr))
    {
        /* A glom occured to our left so we will need to peel
         * our piece out */

        index = index - 1;
    }

    if (index == -1)
    {
        /* A unique situation. We inserted space which needed glomming
         * to both its left and right neighbors, and the neighbors
         * spanned two nodes. This caused the glommed space to move to
         * the left node.  Bring in the left node and peel out the
         * space leaving the left half on the left node and inserting
         * the right half back on to the right node.
         */

        MS_SMP_ASSERT(node_ptr->sib.page_left != -1);

        right_node_ptr = node_ptr;
        right_pgref = pgref;
        sts=rbf_pinpg(&pgref,
                      (void *) &node_ptr,
                      idx_bfap,
                      node_ptr->sib.page_left,
                      BS_NIL,
                      ftxH);

        if (sts != EOK)
        {
            domain_panic(ftxH.dmnP,
                         "idx_directory_insert_space_undo: rbf_pinpg failed");
            return(sts);
        }

        index = node_ptr->total_elements - 1;

        MS_SMP_ASSERT((node_ptr->data[index].search_key <= start_addr) &&
                      ((node_ptr->data[index].search_key+
                        node_ptr->data[index].loc.free_size) >=
                       (start_addr+size)));
    }

    entry_sa = node_ptr->data[index].search_key;
    entry_sz = node_ptr->data[index].loc.free_size;

    if (entry_sa < start_addr)
    {
        /* We glommed to our left so adjust this entry's size */

        RBF_PIN_FIELD(pgref,node_ptr->data[index].loc.free_size);
        node_ptr->data[index].loc.free_size = start_addr - entry_sa;
        entry_sz -= start_addr - entry_sa;
        entry_sa = start_addr;

        if(entry_sz > size)
        {
            /* We also glommed to our right so we must
             * now do an insertion (we are safe here of not
             * splitting a node because we know the node was
             * condensed in this transaction.)
             */

            if (right_node_ptr != NULL)
            {
                /* The insertion actually must take place on the
                 * node to our right. So use that instead
                 */

                node_ptr = right_node_ptr;
                pgref = right_pgref;
                index=0;
            }
            else
            {
                index++;
            }
            MS_SMP_ASSERT(index < IDX_MAX_ELEMENTS);
            if(index < node_ptr->total_elements)
            {
                /* Make room for the new entry */

                bcopy(&node_ptr->data[index],&node_ptr->data[index+1],
                      (node_ptr->total_elements - index) *
                      sizeof(idxNodeEntryT));
                rbf_pin_record(pgref,&node_ptr->data[index],
                               (node_ptr->total_elements - index) *
                               sizeof(idxNodeEntryT));
            }

            node_ptr->data[index].search_key = start_addr + size;
            node_ptr->data[index].loc.free_size = entry_sz - size;

            IDX_INC_TOTAL(pgref,node_ptr);
        }
    }
    /* Note: There is the special case where an insertion caused its
     * parent to update its ranges. This does not need to be undone.
     * the update ranges will be correct even if we remove this
     * insertion */

    else if (entry_sz > size)
    {
        /* We glommed to our right only. Just update the start address
         * and size. Once again (see note above) we do not need to
         * worry if this is the last entry in the node and we are
         * increasing the start address, since the parent would not
         * have had its range reduced due to this insert */

        RBF_PIN_FIELD(pgref, node_ptr->data[index].search_key);
        RBF_PIN_FIELD(pgref, node_ptr->data[index].loc.free_size);

        node_ptr->data[index].search_key += size;
        node_ptr->data[index].loc.free_size -= size;

    }
    else
    {
        MS_SMP_ASSERT(start_addr == entry_sa);

        /* No glomming occured with this insertion. Just
         * remove the entry
         */

        if(index < node_ptr->total_elements - 1)
        {
            rbf_pin_record(pgref,&node_ptr->data[index],
                           (node_ptr->total_elements - index -1) *
                           sizeof(idxNodeEntryT));
            bcopy(&node_ptr->data[index+1],&node_ptr->data[index],
                  (node_ptr->total_elements - index - 1) *
                  sizeof(idxNodeEntryT));
        }

        IDX_DEC_TOTAL(pgref,node_ptr);
    }

    return(EOK);
}
