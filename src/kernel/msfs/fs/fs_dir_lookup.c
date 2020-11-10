/*
 *****************************************************************************
 **                                                                          *
 **  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992                      *
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
 *   MegaSafe Storage System
 *
 * Abstract:
 *
 *   Directory lookup, insert, remove procedures 
 *
 * Date:
 *
 *   19-Apr-1990
 *                     
 */
/*
 * HISTORY
 * 
 */

#pragma ident "@(#)$RCSfile: fs_dir_lookup.c,v $ $Revision: 1.1.125.4 $ (DEC) $Date: 2006/03/20 15:11:01 $"

#include <sys/lock_probe.h>
#include <msfs/fs_dir.h>
#include <msfs/ms_public.h>
#include <msfs/fs_dir_routines.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <sys/kernel.h>
#include <sys/mount.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>
#include <sys/vnode.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/bs_index.h>
#include <msfs/advfs_evm.h>

#define ADVFS_MODULE FS_DIR_LOOKUP

/* A few statistic counters for some rare events */

ulong lost_filename_space;
ulong recovered_filename_space;

/* Global save area for last inconsistent directory message */

domainT* BsLastDomainP = NULL;
bfSetT*  BsLastFsetP = NULL;
uint     BsLastDirTagNum = NULL;
uint     BsLastDirPgNm = NULL;

/* global master switch to disable dir trunc */
int dirTruncEnabled = 1;

#ifdef ADVFS_SMP_ASSERT
/* Set this to non-zero to force remove_dir_ent() to return an error
 * this is useful for testing to force the cleanup path through
 * bs_delete_undo_opx.
 */
int ForceRemoveDirEntFailure = 0;
#endif /* ADVFS_SMP_ASSERT */


/*
 * insert_seq
 *
 *      Insert a directory entry.
 *      This function inserts the new entry in a directory.
 *      directory. It re-uses an existing deleted space if one
 *      exists, otherwise it inserts at the end.
 *
 * Return value:
 *
 *   I_FILE_EXISTS - the new file name is the same as an existing
 *          file name
 *   EOK - success
 *   EIO - An I/O error occurred when reading the directory page.
 *
 */


statusT
insert_seq(
           struct vnode* dvp,      /* in - directory vnode */
           bfTagT new_bs_tag,      /* in - tag of the new entry */
           struct vnode* nvp,      /* in - new file vnode */
           int flag,               /* in - HARD_LINK - entry is being created for
                                      a hard link to an existing file,
                                      so BMT the stats
                                      START_FTX - create a
                                      subtransation */
           struct nameidata *ndp,  /* in - nameidata pointer */
           bfSetT *bfSetp,         /* in - bitfile-set desc pointer */
           struct mount *mp,       /* in - pointer to mount structure */
           ftxHT ftx_handle        /* in - transaction handle */
           )
{
    int insert_page, new_space_flag;
    long insert_byte;
    int curr_pages;
    fs_dir_entry *dir_p, *dir_pp, *dir_buffer;
    char *pp;
    char *begin_page;
    unsigned int entry_size, last_size, real_entry_size, actual_size;
    char *insert_here;
    statusT ret;
    int update_last_offset, end_flag;
    rbfPgRefHT page_ref;
    bfTagT found_bs_tag;
    struct bfAccess *dir_access;
    struct fsContext *dir_context_ptr, *file_context_ptr;
    ftxHT ins_ftx_handle;
    insert_undo_rec undo_rec;
    dirRec *dirRecp;
    domainT *dmnP;
    int extended_dir_here = FALSE;

    dmnP = GETDOMAINP(mp);

    undo_rec.undo_header.old_size=0;

    dir_access = VTOA(dvp);
    dir_context_ptr = VTOC( dvp );

    if (dir_context_ptr->dir_stats.st_nlink < 2) {
        return (ENOENT);
    }
    file_context_ptr = VTOC( nvp );
    update_last_offset = TRUE;
    end_flag = FALSE;
    /*
     * start sub ftx
     */
    ret = FTX_START_N(FTA_FS_INSERT_V1,
                      &ins_ftx_handle,
                      ftx_handle,
                      dmnP,
                      4
        );
    if (ret != EOK) {
        ADVFS_SAD1("ftx_start error in insert_seq", ret);
    }

    /* calculate the actual entry size */
    actual_size = sizeof(directory_header) + ndp->ni_dent.d_namlen
        + (4 - (ndp->ni_dent.d_namlen % 4)) + sizeof(bfTagT);

    /*
     * check to see if the directory has been modified since the
     * lookup was performed. If yes, do another lookup. if not, just
     * fill in the page, byte and re_use_flag info from nameidata (it
     * was put there during lookup).
     */

index_try_again:

    if (ndp->ni_dirstamp == dir_context_ptr->dirstamp
        && ! (ndp->ni_nameiop & BADDIRSTAMP))
    {
        if (IDX_INDEXING_ENABLED(dir_access))
        {

            new_space_flag = idx_directory_get_space(dir_access,
                                                     actual_size,
                                                     &insert_page,
                                                     &insert_byte,
                                                     ins_ftx_handle);
            if(new_space_flag < 0)
            {
                ftx_fail( ins_ftx_handle );
                return (new_space_flag);
            }
        }
        else
        {
            new_space_flag = ndp->ni_resid;
            insert_page = ndp->ni_count;
            insert_byte = ndp->ni_offset;
        }

        /*
         * if new_space_flag, there is no room in the dir. go add
         * another page.
         */
        if (new_space_flag == TRUE) {
            goto add_stg;
        }

        /*
         * We are using a deleted or empty space for this file.
         * Pin the page to be inserted.
         */
        ret = rbf_pinpg(
                        &page_ref,
                        (void *)&dir_buffer,
                        dir_access,
                        insert_page,
                        BS_NIL,
                        ins_ftx_handle
                        );
        if (ret != EOK) {
            ret = EIO;
            goto error;
        }

        /*
         * Fill in the page and byte in the context area and the
         * undo record. Pin the new record slot.
         */
        reuse_entry:
        undo_rec.undo_header.page = insert_page;
        undo_rec.undo_header.byte = insert_byte;
        begin_page = (char *)dir_buffer;
        update_last_offset = FALSE;
        insert_here = (char *)dir_buffer;
        insert_here += insert_byte;
        dir_p = (fs_dir_entry *)insert_here;
        /* save the old size so that the re-used entry can be
           split up later */
        entry_size = dir_p->fs_dir_header.fs_dir_size;

        rbf_pin_record(
                       page_ref,
                       insert_here,
                       actual_size
                       );
        goto insert_entry;
    } else {
        /*
         * the dir has changed (ie the timestamp changed)
         *
         * call seq_search to find where the new entry goes in the directory.
         * seq_search returns the page (insert_page) and byte (insert_byte)
         * to insert the entry. It returns with the page pinned.
         * If the entry is not found and will be placed in an existing slot,
         * then ndp->ni_endoff will contain the page # with the last real
         * directory entry.
         */

        ret = seq_search(
                         dvp,
                         ndp,
                         &found_bs_tag,
                         &dir_buffer,
                         &page_ref,
                         PIN_PAGE,        /* pin the pages searched */
                         ins_ftx_handle
                         );

        /*
         * if the file has a current entry, or an error occured,
         * just get out of here...
         */
        if ( (ret == I_FILE_EXISTS) || (ret == EIO) ) {
            goto error;
        }

        if (IDX_INDEXING_ENABLED(dir_access))
        {
            /* We know that the dir is safe since we are holding the
             * write lock so set up the time stamps to match and jump
             * back up.
             */

            ndp->ni_dirstamp = dir_context_ptr->dirstamp;
            ndp->ni_nameiop &= ~BADDIRSTAMP;
            goto index_try_again;
        }
        insert_page = ndp->ni_count;
        insert_byte = ndp->ni_offset;
        new_space_flag = ndp->ni_resid;

        if (new_space_flag == FALSE) {
            /*
             * Using a deleted or empty entry. Go back to the
             * 're-using an entry' code. First put last_useful_pg value + 1
             * into 'ndp' where the insert code expects it to be.  This
             * is the actual number of pages, and will always be > 0.
             */
            ndp->ni_endoff++;
            goto reuse_entry;
        }
    }

    /*
     * there was no space in the dir for the new entry. Add a page
     * to the dir.
     */
add_stg:
    ret = change_quotas(dvp, 0, (long)ADVFS_PGSZ_IN_BLKS, NULL,
                        u.u_cred, 0, ins_ftx_handle);
    if (ret != EOK) {
        goto error;
    }

    curr_pages = howmany(dir_access->file_size, ADVFS_PGSZ);

    MS_SMP_ASSERT((IDX_INDEXING_ENABLED(dir_access)) ||
                  (insert_page == curr_pages-1));

    ret = rbf_add_stg(dir_access, curr_pages, 1, ins_ftx_handle, TRUE);

    if (ret != EOK) {
        goto error;
    }

    insert_page = curr_pages;
    insert_byte = 0;

    extended_dir_here = TRUE;

    /* set up for possible undo */
    undo_rec.undo_header.page = insert_page;
    undo_rec.undo_header.byte = 0;

    /* reference the new page */
    ret = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_buffer,
                    dir_access,
                    insert_page,
                    BS_NIL,
                    ins_ftx_handle
                    );
    if (ret != EOK) {
        ret = EIO;
        goto error;
    }

    if ((dir_access->dmnP->dmnVersion >=
         FIRST_INDEXED_DIRECTORIES_VERSION) &&
        (curr_pages == 1) && 
        (!(IDX_INDEXING_ENABLED(dir_access))))
    {
        /* Need to make sure this isn't a directory and that index the
         * hasn't dipped back to 1 page. If so the index is already in place.
         * This may go away if removal of index files below a threshold is
         * implemented */

        /* The directory can now benefit from an index file so if
         * this is a newer domain do the conversion to indexing */

        ret = idx_convert_dir(dir_access,ndp,ins_ftx_handle);
        if (ret != EOK)
            goto error;
    }

    /*
     * pin_record the new page and init it (ie add an empty entry in each
     * sub_page and init the dirRec)
     */
    if (IDX_INDEXING_ENABLED(dir_access))
    {
        /* Put back the page minus the block into the index free space
         * tree We need to do this prior to pinning any records in case
         * we fail to put the space into the b-tree.*/

        ret = idx_directory_insert_space(dir_access,
                                         ADVFS_PGSZ - actual_size, /* size to insert */
                                         insert_page,          /* Page where space lives */
                                         actual_size,          /* Offset where space lives */
                                         actual_size,          /* Offset where space lives */
                                         ADVFS_PGSZ - actual_size, /* size to insert */
                                         ins_ftx_handle,
                                         0 ); /* forceflag to ignore quota */
        if (ret != EOK)
        {
            goto error;
        }
    }

    rbf_pin_record(
                   page_ref,
                   dir_buffer,
                   ADVFS_PGSZ
                   );
    bzero(dir_buffer, ADVFS_PGSZ);
    dir_p = (fs_dir_entry *)dir_buffer;
    while ((char *)dir_p < (char *)dir_buffer + ADVFS_PGSZ) {
        dir_p->fs_dir_header.fs_dir_bs_tag_num = 0;
        dir_p->fs_dir_header.fs_dir_size = DIRBLKSIZ;
        dir_p = (fs_dir_entry *)((char *)dir_p + DIRBLKSIZ);
    }
    dirRecp = (dirRec *)((char *)dir_buffer + ADVFS_PGSZ - sizeof(dirRec));
    dirRecp->pageType = SeqType;
    dirRecp->largestFreeSpace = 0;
    dirRecp->lastEntry_offset = ADVFS_PGSZ - DIRBLKSIZ;
    /*
     * it now looks like we are re-using a 512-byte entry
     */
    new_space_flag = FALSE;
    entry_size = DIRBLKSIZ;

    /*
     * incr the size field in the stat structure, since the directory is
     * now 1 page longer
     */
    /* let the undo routine know to put back the old file size */
    undo_rec.undo_header.old_size = dir_access->file_size;
    dir_access->file_size += ADVFS_PGSZ;


    /* reset the buffer pointers and setup for the new entry on the
       new page */
    insert_here = (char *)dir_buffer;
    begin_page = (char *)dir_buffer;
    /* last offset is already OK */
    update_last_offset = FALSE;

    /*
     * insert the new entry
     */
insert_entry:

    (void) fs_assemble_dir(
                           insert_here,
                           new_bs_tag,
                           ndp->ni_dent.d_name
                           );
    /*
     * if over-writing a deleted entry, see if we can split off the
     * end into a separate deleted space... the end is useful if it
     * can hold at least a minimum directory entry. If not, just set
     * the new entry size to the size of the whole deleted space. If
     * the re-used entry is the last entry in the page, then it must
     * contain the dirRec at the end of the page...
     */
    dir_p = (fs_dir_entry *)insert_here;
    dirRecp = (dirRec *)((char *)begin_page + ADVFS_PGSZ - sizeof(dirRec));
    if ((char *)dir_p == begin_page + dirRecp->lastEntry_offset) {
        real_entry_size = entry_size - sizeof(dirRec);
        end_flag = TRUE;
    } else {
        real_entry_size = entry_size;
    }

    if (dir_p->fs_dir_header.fs_dir_size < entry_size) {
        if ((real_entry_size - dir_p->fs_dir_header.fs_dir_size) >=
            sizeof (directory_header) + 4 + sizeof(bfTagT)) {
            /* point pp to the space after the new entry */
            /* pin_recd it */
            pp = insert_here;
            pp += dir_p->fs_dir_header.fs_dir_size;
            rbf_pin_record(page_ref,
                           (void *)pp,
                           sizeof(directory_header)
                );
            dir_pp = (fs_dir_entry *)pp;
            dir_pp->fs_dir_header.fs_dir_bs_tag_num = 0;
            dir_pp->fs_dir_header.fs_dir_size = entry_size -
                dir_p->fs_dir_header.fs_dir_size;
            dir_pp->fs_dir_header.fs_dir_namecount = 0;
            if (end_flag) {
                last_size = dir_p->fs_dir_header.fs_dir_size;
                update_last_offset = TRUE;
            }
        } else {
            dir_p->fs_dir_header.fs_dir_size = entry_size;
        }
    }

    /*
     * update dirrec.lastEntry_offset by the new entry size (if it was
     * the new last entry in the page)
     */

    if (update_last_offset) {
        rbf_pin_record(
                       page_ref,
                       (void *)dirRecp,
                       sizeof(dirRec)
                       );
        dirRecp->lastEntry_offset += last_size;
    }
    /*
     * set up the in-memory fsContext structure for the new file with the
     * new tag.
     * if flag is HARD_LINK, then this insert is a hard link, and the
     * fsContext is already set up.
     */

    if (!(flag & HARD_LINK)) {
        file_context_ptr->bf_tag = new_bs_tag;
    }
    /*
     * set up the undo record in case the caller fails
     * and done the ftx
     */
    undo_rec.undo_header.dir_tag = dir_context_ptr->bf_tag;
    undo_rec.undo_header.ins_tag = new_bs_tag;
    undo_rec.undo_header.bfSetId = GETBFSET(mp);
    ftx_done_u(
               ins_ftx_handle,
               FTA_FS_INSERT_V1,
               sizeof(undo_rec),
               &undo_rec
               );

    if (IDX_INDEXING_ENABLED(dir_access))
    {
        /* Place the filename into the index filename b-tree for fast
         * lookups later */

        ret = idx_insert_filename(dir_access,
                                  ndp,
                                  insert_page,
                                  insert_byte,
                                  ftx_handle);
        if (ret != EOK)
            return(ret);
    }

    /* If we did not have to allocate more storage, check to see if the
     * directory can be truncated; if so, do it. Do not use new_space_flag
     * since the sense of it is reversed after new storage is allocated.
     */
    if ((!extended_dir_here) && dirTruncEnabled)
    {
        unsigned long num_useful_pages;
        long bytes_to_trunc,delCnt=0;
        /* convert current size in bytes to # pages */
        curr_pages = howmany(dir_access->file_size, ADVFS_PGSZ);

        if(IDX_INDEXING_ENABLED(dir_access))
        {
            /* Since the truncation can fail due to lack of storage (how odd?)
             * first query the index for the number of byte to truncate
             * the after the removal is successful, update the index
             */


            IDX_QUERY_TRUNC_DIRECTORY( dir_access, bytes_to_trunc,ftx_handle);

            if (bytes_to_trunc < 0)
            {
                /* A negtive value indicates and error
                 * The returned value is actually a
                 * status code.
                 */
                
                return((statusT) bytes_to_trunc);
            }
                
            num_useful_pages = curr_pages - howmany(bytes_to_trunc, ADVFS_PGSZ);
        }
        else  num_useful_pages = ndp->ni_endoff;

        if ( num_useful_pages &&
            (curr_pages > num_useful_pages) &&
            (insert_page < num_useful_pages) ) {
             delCnt=dir_trunc_start( dvp, num_useful_pages, ftx_handle);
        }
        
        if(IDX_INDEXING_ENABLED(dir_access) && (delCnt > 0) )
        {
            /* We removed the storage from the directory now
             * reflect this fact in the index
             */

            IDX_UPDATE_TRUNC_DIRECTORY( dir_access, bytes_to_trunc,ftx_handle);

            if (bytes_to_trunc < 0)
            {
                /* A negtive value indicates and error
                 * The returned value is actually a
                 * status code.
                 */
                
                return((statusT) bytes_to_trunc);
            }

        }

        /* reset # useful pages to 'invalid' whether it was used or not */
        ndp->ni_endoff = 0;
    }

    return (EOK);

error:

    ftx_fail( ins_ftx_handle );
    return (ret);

} /* end insert_seq */


/*
 * remove_dir_ent
 *
 * Remove an entry from a directory. 
 *
 * A new on-disk status has been added to the deleted
 * entry. A -1 in the tag.seq number which is appended to
 * the end of the directory entry can now contain a -1. 
 * This indicates that this particular entry is lost. This
 * status should only exist for indexed directories. This
 * will be set if the disk is out of space and we are unable
 * to insert the space into the b-tree. The -1 will be looked
 * for on subsequent removals in an attempt to reclaim this
 * space.
 * 
 */


statusT
remove_dir_ent(
               struct nameidata *ndp,  /* in - nameidata structure pointer */
               ftxHT ftx_handle        /* in - ftx handle from msfs_remove */
               )
{
    statusT ret;
    fs_dir_entry *dir_p;
    struct fsContext *dir_context, *file_context;
    rbfPgRefHT page_ref;
    char *dir_buffer;
    bfTagT *tagp;
    int page;
    long byte_offset;
    uint32T entry_size;
    uint32T orig_entry_size;
    uint32T glom_flags=0;
    struct bfAccess *dir_access;
    struct vnode *dvp = NULL;


#ifdef ADVFS_SMP_ASSERT
    if (ForceRemoveDirEntFailure) {
        return (EIO);
    }
#endif /* ADVFS_SMP_ASSERT */

    dvp = ndp->ni_dvp;
    dir_access = VTOA(dvp);
    dir_context = VTOC( dvp );
    file_context = VTOC(ndp->ni_vp);

    /*
     * purge the vnode from the cache
     */
    cache_purge(ndp->ni_vp);
    page = ndp->ni_count;
    byte_offset = ndp->ni_offset;
    /*
     * pin the dir page with the entry to remove
     */
    ret = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_buffer,
                    dir_access,
                    page,
                    BS_NIL,
                    ftx_handle
                    );
    if (ret != EOK) {
#ifdef ADVFS_DEBUG
        ADVFS_SAD3("rbf_pinpg error in remove_dir_ent N1 acc = N2 pg = N3",
                 ret, dir_access, page);
#else
        return (ret);
#endif
    }
    /*
     * Check that we are pointing to the correct tag.
     * if not, print a message and EIO out of here.
     */

    dir_p = (fs_dir_entry *)(dir_buffer + byte_offset);
    tagp = GETTAGP(dir_p);
    if (!(BS_BFTAG_EQL(*tagp, file_context->bf_tag))) {

        ms_uaprintf("Bad dir format in remove_dir_ent dir tag = %d, file tag = %d\n",
                    dir_context->bf_tag.num,
                    file_context->bf_tag.num);
        MS_SMP_ASSERT(0);  /* Force a panic in-house */
        return(EIO);
    } 

    /* Setup for glomming the directory entries together. This routine will not
     * pin any pages which allows us to call ftx_fail in the calls after. This
     * basically tells us what the entry size will look like after the glom. We
     * will put this size in the btree. This may also pick up any space that may
     * have been lost on a previous remove (a very rare occurance). 
     */

    orig_entry_size = dir_p->fs_dir_header.fs_dir_size;

    ret = setup_for_glom_dir_entries(dir_buffer,
                                     &dir_p,
                                     &entry_size,
                                     &glom_flags,
                                     dir_access);
    if (ret != EOK)
    {
        /*
         * if the size field is bogus, print out a message and
         * return EIO.
         */
            
        ms_uaprintf("Dir size error in remove_dir_ent dir tag = %d, file tag = %d\n",
                    dir_context->bf_tag.num,
                    file_context->bf_tag.num);
        return(EIO);
    }

    /*
     * Remove the filename from the b-tree. If this is an indexed
     * directory.
     */

    if (IDX_INDEXING_ENABLED(dir_access))
    {
        ret=idx_remove_filename(dir_access,
                                ndp,
                                page,
                                byte_offset,
                                ftx_handle);

        if (ret != EOK) 
            return(ret);
        
        /* Add the deleted space to the index free space tree. */

        ret = idx_directory_insert_space( dir_access,
                                          entry_size,
                                          page,
                                         (unsigned int)dir_p & (ADVFS_PGSZ - 1),
                                          byte_offset,
                                          orig_entry_size,
                                          ftx_handle,
                                          1 ); /* forceflag to ignore quota */
        
        if (ret != EOK)
        {
            if ((ret == ENO_MORE_BLKS) || (ret == ENO_MORE_MCELLS))
            {

                /* The insertion to the b-tree caused a node to split,
                 * which in turn needed to obtain space for which there 
                 * was none. This is really bad luck.
                 * We do not want to return to the caller an out of
                 * space error when they are removing a file! So ignore the
                 * error and let the space be lost. We will flag it on disk
                 * as lost so that a later glom has a chance of picking it
                 * up.
                 */

                ret=EOK;

                /* Reset the entry pointer in case glom changed it */
                
                dir_p = (fs_dir_entry *)(dir_buffer + byte_offset);

                rbf_pin_record(page_ref,
                               tagp,
                               sizeof(bfTagT));

                tagp->seq = (uint32T)(-1);
                
                /* It should not be the case that a glom occured and
                 * we needed space (ie a node split) for the b-tree. This is
                 * because a glom means the tree only needed to have an existing
                 * entry updated instead of a new entry inserted (only an insert
                 * can cause a node to split). Unless there is lost space next to
                 * it. Then we could have this situation. 
                 * Since we are about to lose this space turn off any glomming. 
                 */
                
                MS_SMP_ASSERT(!(glom_flags & SPACE_NEEDS_GLOMMING) ||
                              (glom_flags & LOST_SPACE_FOUND));

                glom_flags &= ~(SPACE_NEEDS_GLOMMING);

                lost_filename_space++;

            }
            else
            {
                /* Return the error and let the caller fail the ftx,*/
                return(ret);
            }
        }            
    }

    /*
     * Now we have hit the point of no failure. 
     * We may now start pinning records
     */

    if (dir_p  == (fs_dir_entry *)(dir_buffer + byte_offset))
    {
        /* We don't need to update these fields if we were glommed
         * with a previous entry.
         */
        rbf_pin_record(
                       page_ref,
                       &dir_p->fs_dir_header.fs_dir_bs_tag_num,
                       sizeof(uint32T)
                      );

        dir_p->fs_dir_header.fs_dir_bs_tag_num = 0;
    }

    if (glom_flags & SPACE_NEEDS_GLOMMING)
    {
        /* Glom the deleted entry with its neighbor(s) */

        if (glom_flags & LOST_SPACE_FOUND)
        {
            /* Keep some stats */
            MS_SMP_ASSERT(IDX_INDEXING_ENABLED(dir_access));
            recovered_filename_space++;
        }

        glom_dir_entries(dir_buffer,
                         dir_p,
                         entry_size,
                         glom_flags,
                         page_ref);
    }

    /*
     * update the mtime for the directory,
     * incr dirstamp to signal that the dir has changed */
    mutex_lock( &dir_context->fsContext_mutex );
    dir_context->fs_flag |= (MOD_MTIME | MOD_CTIME);
    dir_context->dirty_stats = TRUE;
    dir_context->dirstamp++;
    mutex_unlock( &dir_context->fsContext_mutex );
    
    /*
     * update the dirs stats, release the dir vnode.
     */

    ret = fs_update_stats(dvp, dir_access, ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("update_stats error in remove_dir_ent", ret);
    }

    return (0);

} /* end remove_dir_ent */


/*
 * remove_dots
 *
 * Remove the dot and dotdot entries from a directory that is being
 * deleted. This routine doesn't fail.
 */

statusT
remove_dots(
            struct vnode *dvp,  /* in - dir vnode pointer */
            ftxHT ftx_handle        /* in - ftx handle from msfs_remove */
            )
{
    statusT ret;
    fs_dir_entry *dir_p;
    rbfPgRefHT page_ref;
    char *dir_buffer, *p;
    bfAccessT *dir_access;

    dir_access = VTOA(dvp);

    /*
     * pin dir page 0
     */
    ret = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_buffer,
                    dir_access,
                    0,
                    BS_NIL,
                    ftx_handle
                    );
    if (ret != EOK) {
        ms_uaprintf("rbf_pinpg error in remove_dots\n");
        return ret;
    }

    /*
     * pin the word to be changed in the page for the '.' entry.
     * check for valid dir before doing anything.
     */
    dir_p = (fs_dir_entry *)(dir_buffer);
    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_header.fs_dir_bs_tag_num,
                   sizeof(uint32T)
                   );

    /*
     * set the bf tag to 0, signalling deleted in the dir entry
     */

    dir_p->fs_dir_header.fs_dir_bs_tag_num = 0;

    /*
     * pin the word to be changed in the page for the '..' entry.
     */

    if ( FS_DIR_SIZE_OK(dir_p->fs_dir_header.fs_dir_size, 0)) {
        fs_dir_size_notice(((bfNodeT*)dvp->v_data)->accessp, 
                           dir_p->fs_dir_header.fs_dir_size,
                           0, 0);
        return EIO;
    }

    p = (char *)dir_p;
    p += dir_p->fs_dir_header.fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_header.fs_dir_bs_tag_num,
                   sizeof(uint32T)
                   );

    /*
     * set the bf tag to 0, signalling deleted in the dir entry
     */

    dir_p->fs_dir_header.fs_dir_bs_tag_num = 0;

    return EOK;

} /* end remove_dots */

struct seq_search_stats {
        u_long page_hit;
        u_long pass_hit[3];     /* start: 0=from begin, 1=from save, 2=rescan */
        u_long bad_save;        /* saved page/offset was bad */
        u_long pass2_start;     /* using saved page/offset */
} seq_search_stats = {0L,0L,0L,0L,0L,0L};


/*
 * seq_search
 *
 *      Find the spot in the directory in which to insert the new entry
 *
 * Return value:
 *
 *      I_FILE_EXISTS - the file name is the same as an existing
 *          file name. return the file's bf tag, the buffer address of the
 *          directory page where the file was found, and the page_reference
 *      I_INSERT_HERE - the file_name was not found in the
 *          directory. The page and byte within the page where the
 *          entry would go if inserted is returned.
 *      EIO - returned if ref'ing pages and get an error or if the
 *            directory format is bogus.
 */


statusT
seq_search(
           struct vnode* dir_vp,           /* in - dir's vnode ptr */
           struct nameidata *ndp,
           bfTagT *found_bs_tag,           /* out - if found:  file's tag */
           fs_dir_entry **found_buffer,    /* out - if found:  buffer address of page
                                              on which entry was found */
           rbfPgRefHT *found_page_ref,     /* out - if found:  page ref
                                              for found_buffer if under ftx */
           int flag,                       /* in - flag: PIN_PAGE - pin pages
                                              searched,
                                              REF_PAGE - ref pages searched,
                                              USE_OFFSET - use the offset hint */
           ftxHT ftx_handle                /* in - transaction handle */
           )
{
    char *p;
    fs_dir_entry *dir_p, *last_p, *first_p, *save_buffer;
    statusT return_value, ret;
    int n, j;
    int last_page, entry_size, offset;
    unsigned int last_ent_offset;
    struct fsContext *dir_context_pointer;
    rbfPgRefHT page_ref;
    bfPageRefHT page_ref1;
    int first_time, find_slot;
    dirRec *dirRecp;
    bfTagT *tagp;
    uint32T saved_page;
    long saved_offset=0;
    struct bfAccess  *dir_access;
    int numDirPasses;   /* start: 0=from begin, 1=from save, 2=rescan */
    unsigned long last_useful_page;  /* used for determining end of entries */
    uint16T file_name_len;
    char *file_name;

    return_value = I_INSERT_HERE;
    ndp->ni_resid = TRUE;
    find_slot = 1;
    first_time = TRUE;
    last_useful_page = 0;

    file_name_len=ndp->ni_dent.d_namlen;
    file_name=ndp->ni_dent.d_name;

    /* get the new entry size */
    n = sizeof(directory_header) + file_name_len;
    entry_size = n + (4 - (n % 4));
    entry_size += sizeof(bfTagT);
    /*
     * perform a sequential search of the pages of a directory for a
     * particular entry.
     *
     */
    dir_access = VTOA(dir_vp);
    dir_context_pointer = VTOC( dir_vp );

    /*
     * The hint passed in via USE_OFFSET and last_offset is not used yet.
     * This needs to be incorporated into the index search code later. For
     * now we will always walk the b-tree.
     */

    if(IDX_INDEXING_ENABLED(dir_access))
    {
        ret = idx_lookup_filename(dir_access,
                                  ndp,
                                  found_bs_tag,
                                  found_buffer,
                                  found_page_ref,
                                  flag,
                                  ftx_handle);

        /* Callers of seq_search expect only 4 return codes.
         * catch and others here and return EIO instead. Presumably
         * we have already domain paniced in the index code and
         * are sending up the error.
         */
        
        if ((ret == I_FILE_EXISTS) || (ret == I_INSERT_HERE))
        {
            return(ret);
        }
        else if (ret != E_OUT_OF_SYNC_CLONE) {
            return(EIO);
        }
        /* Fall through if the index is out of sync. The
         * directory may well be out of sync too but give the
         * look up a chance of maybe seeing the directory
         * without the index.
         */
    }

    last_page = howmany(dir_access->file_size, ADVFS_PGSZ);
    /*
     * if lookup set the use_offset flag, then use the offset
     * stored in the context area from the last lookup as a guess.
     * This speeds up utilities that do a readdir
     * then open every file in order.
     */
    saved_page = saved_offset = 0;
    numDirPasses = 0;

    if (flag & USE_OFFSET) {
        saved_page = dir_context_pointer->last_offset / ADVFS_PGSZ;
        saved_offset = dir_context_pointer->last_offset % ADVFS_PGSZ;
        /*
         * check to see if it's there at all or bogus
         *
         * `last_page' is actually the page after the last
         * page (ie a non-existant page).
         */

        MS_SMP_ASSERT(saved_offset >= 0);
        if (saved_page >= last_page || saved_offset < 0) {
            saved_page = saved_offset = 0;
            seq_search_stats.bad_save++;
        }
    }
dir_loop:
    /*
     * loop thru the whole directory, starting at page `saved_page`
     */
    ndp->ni_offset = saved_page;
    for (j = saved_page; j < last_page; j++) {
        if (flag & PIN_PAGE) {
            if (!first_time) {
                rbf_deref_page(
                               page_ref,
                               BS_CACHE_IT
                               );
            }
            ret = rbf_pinpg(
                            &page_ref,
                            (void *)&dir_p,
                            dir_access,
                            j,
                            BS_NIL,
                            ftx_handle
                            );
            if (ret != EOK) {
                return (EIO);
            }
        } else {
            if (!first_time) {
                bs_derefpg(page_ref1, BS_CACHE_IT);
            }
            ret = bs_refpg(
                           &page_ref1,
                           (void *)&dir_p,
                           dir_access,
                           j,
                           BS_SEQ_AHEAD
                           );
            if (ret != EOK) {
                    return (EIO);
            }
        }
        first_time = FALSE;
        save_buffer = dir_p;
        /*
         * get the pointer to the last entry in the page
         */
        first_p = dir_p;
        n = ADVFS_PGSZ - sizeof(dirRec);
        dirRecp = (dirRec *)((char *)dir_p + n);
        last_ent_offset = dirRecp->lastEntry_offset;
        if ( !last_ent_offset || last_ent_offset >= (unsigned)n ) {
            goto dir_error;
        }
        last_p = (fs_dir_entry *)((char *)dir_p + last_ent_offset);

        /*
         * loop through the page
         *
         * Start looking at the cached directory lookup entry if present
         */
        if (numDirPasses == 0 && (saved_page || saved_offset)) {
                numDirPasses++; /* start from saved */
                /* just in case saved_offset isn't on a block boundary,
                 * set dir_p to the beginning of the 512-byte dirblock.
                 */
                p = (char *)dir_p;
                p += (saved_offset & (~(DIRBLKSIZ - 1)));
                dir_p = (fs_dir_entry *) p;
                seq_search_stats.pass2_start++;
        }

        while (dir_p <= last_p) {
            /*
             * if the entry was deleted, check if it is a big enough
             * slot (in case we need one). if so, save it. Set
             * new_space_flag to FALSE to tell insert_seq that we are
             * re-using an entry (unless it's the pages last entry,
             * then just return the space...
             */
            if (dir_p->fs_dir_header.fs_dir_bs_tag_num == 0) {
                if (find_slot) {
                    /*
                     * if this was the last entry on the page, there
                     * might be enough room anyway...check it out
                     * including any unused space between the old entry
                     * and the dirRec.
                     */
                    if ((int)((char *)dir_p - (char *)first_p) ==
                        dirRecp->lastEntry_offset) {
                        if ((char *)dir_p + entry_size < (char *)dirRecp) {
                            ndp->ni_offset = (long)((char *)dir_p - (char *)first_p);
                            find_slot = 0;
                            ndp->ni_count = j;
                            ndp->ni_resid = FALSE;
                            *found_buffer = save_buffer;
                            if (flag & PIN_PAGE) {
                                *found_page_ref = page_ref;
                            }
                        }
                    } else {
                        /*
                         * well it's not the last entry on the page,
                         * so just see if it's big enough...
                         */
                        if (dir_p->fs_dir_header.fs_dir_size >= entry_size) {
                            ndp->ni_offset = (long)((char *)dir_p - (char *)first_p);
                            find_slot = 0;
                            ndp->ni_count = j;
                            ndp->ni_resid = FALSE;
                            *found_buffer = save_buffer;
                            if (flag & PIN_PAGE) {
                                *found_page_ref = page_ref;
                            }
                        }
                    }
                }
            } else {
                if ((dir_p->fs_dir_header.fs_dir_namecount ==
                     file_name_len) && bcmp(dir_p->fs_dir_name_string,
                     file_name, file_name_len) == 0) {
                    /*
                     * found the entry, return bf tag and offset and
                     * get out of here
                     */
                    tagp = GETTAGP(dir_p);
                    *found_bs_tag = *tagp;
                    ndp->ni_offset = (long)((char *)dir_p - (char *)first_p);
                    return_value = I_FILE_EXISTS;
                    ndp->ni_count = j;
                    *found_buffer = save_buffer;
                    if (flag & PIN_PAGE) {
                        *found_page_ref = page_ref;
                    }
                    /* count how we found this, i.e. via numDirPasses */
                    seq_search_stats.pass_hit[numDirPasses]++;
                    break;
                } else {
                    /* Remember the location of this useful data. We are
                     * iteratively finding the last piece of useful data
                     * in the directory so that we can potentially truncate
                     * empty pages beyond.  'j' contains current page number.
                     */
                    if (last_useful_page < j)
                        last_useful_page = j;
                }
            }
            /*
             * increment dir_p to the next dir entry
             */
            n = (signed)dir_p->fs_dir_header.fs_dir_size;
            p = (char *) dir_p;
            offset = (char *)dir_p - (char *)save_buffer;
            if ( FS_DIR_SIZE_OK( n, offset) ) {
                /* there has been a minor data corruption, inform and error out */
                fs_dir_size_notice(((bfNodeT*)dir_vp->v_data)->accessp, 
                                   n, j, offset);
                /*
                 * If error, setup to resume search at the next block boundary.
                 */

                p = (char *)save_buffer;
                n = (offset + DIRBLKSIZ) & ~(DIRBLKSIZ-1);
            }
            dir_p = (fs_dir_entry *)(p + n);
        } /* end of 'for' loop for this dir page */

        if (return_value == I_FILE_EXISTS) {
            break;
        }

    } /* end of 'for' loop reading dir pages*/

    /* we're at the end of the dir;  so if we're not rescanning
     * i.e. numDirPasses==2, let's remember the last page.
     */
    if ( numDirPasses < 2 && return_value != I_FILE_EXISTS) {
        /*
         * if we never found the entry or a big enough slot, make sure we
         * return the last page
         */
        if ((return_value == I_INSERT_HERE) && (find_slot == 1)) {
            ndp->ni_offset = (long)dirRecp->lastEntry_offset;
            ndp->ni_offset += (long)last_p->fs_dir_header.fs_dir_size;
            ndp->ni_count = j - 1;
            *found_buffer = save_buffer;
            if (flag & PIN_PAGE) {
                *found_page_ref = page_ref;
            }
            find_slot = 2;
        }

        if (numDirPasses == 1) { /* we originally started from saved */
                ++numDirPasses;
                last_page = saved_page + 1;
                /* XXX we could further optimize by only searching up to our
                 * XXX starting saved_offset.
                 */
                saved_page = 0;
                goto dir_loop; /* rescan from beginning (to saved_page) */
        }
    }

    /* We are done scanning.  If we will reuse a slot, return the value of
     * last_useful_page for insert_seq() to use as a point after which the
     * directory can be truncated.
     */
    if ((return_value == I_INSERT_HERE) && (find_slot == 0)) {
            ndp->ni_endoff = last_useful_page;
    }

    /*
     * if ref'ing pages (ie called from lookup) deref the final page.
     * if pinning and the page we are returning isn't currently pinned,
     * better do it
     */
    if (flag & REF_PAGE) {
        bs_derefpg(page_ref1, BS_CACHE_IT);
    } else {
        if (ndp->ni_count != j - 1) {
            rbf_deref_page(
                           page_ref,
                           BS_CACHE_IT
                           );
            ret = rbf_pinpg(
                            &page_ref,
                            (void *)&dir_p,
                            dir_access,
                            ndp->ni_count,
                            BS_NIL,
                            ftx_handle
                            );
            if (ret != EOK) {
                return (EIO);
            }
            *found_buffer = dir_p;
            *found_page_ref = page_ref;
        }
    }
    return (return_value);

dir_error:
    if ( flag & PIN_PAGE ) {
        rbf_deref_page( page_ref, BS_CACHE_IT );
    } else {
        bs_derefpg(page_ref1, BS_CACHE_IT);
    }

    return (EIO);
} /* end of seq_search */


void
fs_dir_size_notice(
                   struct bfAccess* dirBfap, /* in - dir's access ptr     */
                   int entrySize,            /* in - entry's size         */
                   uint pgNm,                /* in - dir's page number    */
                   uint offset               /* in - offset in dir's page */
                   )
{                        
    advfs_ev advfs_event;

    uprintf("Warning: Some file(s) of AdvFS fileset = \"%s\" in domain = \"%s\"\n",
                dirBfap->bfSetp->bfSetName, dirBfap->dmnP->domainName);
    uprintf("belonging to directory tag %d are inaccessible.\n", dirBfap->tag.num);
    uprintf("There is a corrupted directory entry size: on directory page %d, at offset 0x%x\n",
                pgNm, offset);
    uprintf("  bad size = 0x%x,\n ", entrySize);

    if (dirBfap->badDirEntry == FALSE) {
        dirBfap->badDirEntry = TRUE;
        aprintf("Repairable AdvFs corruption detected, consider running fixfdmn.\n");
        aprintf("Warning: Some file(s) of AdvFS fileset = \"%s\" in domain = \"%s\"\n",
                    dirBfap->bfSetp->bfSetName, dirBfap->dmnP->domainName);
        aprintf("belonging to directory tag %d are inaccessible.\n", dirBfap->tag.num);
        aprintf("There is a corrupted directory entry size: on directory page %d, at offset 0x%x\n",
                     pgNm, offset);
        aprintf("  bad size = 0x%x,\n ", entrySize);

        bzero(&advfs_event, sizeof(advfs_ev));
        advfs_event.domain = dirBfap->dmnP->domainName;
        advfs_event.fileset = dirBfap->bfSetp->bfSetName;
        advfs_event.dirTag = dirBfap->tag.num;
        advfs_event.dirPg = pgNm;
        advfs_event.pgOffset = offset;
        advfs_post_kernel_event(EVENT_FSET_INCONSISTENT_DIR_ENTRY, &advfs_event);
    }

    return;
}


/*
 * remove_bf
 *
 * remove a bf that has no dir entry
 *
 */

statusT
remove_bf(
            ino_t ino,
            bfAccessT *tag_access,
            int just_delete_flag
            )
{
    statusT ret;
    struct fsContext *tag_context, *file_context, *dir_context;
    bfSetT *bfSetp;
    bfTagT rem_tag;
    struct mount *mp;
    struct vnode *dir_vp, *nvp;
    ftxHT ftx_handle;
    int file_open = FALSE, dir_open = FALSE;

    tag_context = VTOC(ATOV(tag_access));
    bfSetp = tag_context->fileSetNode->bfSetp;
    mp = VTOMOUNT(ATOV(tag_access));

    /* open the bf */
    rem_tag.num = ino;
    rem_tag.seq = 0;
    ret = bs_get_current_tag(
                    bfSetp,
                    &rem_tag
                    );
    if (ret != EOK ) {
    /* no file with that tag, get out */
        return EOK;
    }
    ret = bf_get(
                 rem_tag,
                 0,
                 DONT_UNLOCK,
                 mp,
                 &nvp
                 );
    if (ret != EOK) {
        /* it's not there, get out */
        return EOK;
    }
    file_open = TRUE;
    /*
     * start a transaction.
     */
    file_context = VTOC(nvp);

    /* Prevent races with migrate code paths */
    /* No need to take the quota locks because there will not be any
     * storage allocation to the quota files here, only when a file is
     * created or chown'ed or a quota entry is explicitly set through
     * advfs_set_quota and advfs_set_use.
     *
     * The potential danger path is
     * rbf_delete --> chk_blk_quota --> dqsync --> rbf_add_stg 
     * but it's tame here.
     */
    ret = FTX_START_N(
                      FTA_OSF_REMOVE_V1,
                      &ftx_handle,
                      FtxNilFtxH,
                      file_context->fileSetNode->dmnP,
                      9
                      );
    if (just_delete_flag) goto delete_it;

    /*
     * get its parent dir and make sure there is no entry for it
     */
    ret = bf_get(
                 file_context->dir_stats.dir_tag,
                 0,
                 DONT_UNLOCK,
                 mp,
                 &dir_vp);
    if (ret != EOK) {
    /* there's probably no dir, just delete the file */
        ret = EOK;
        goto delete_it;
    }
    dir_open = TRUE;
    dir_context = VTOC(dir_vp);
    /*
     * exclusively lock the directory,
     * call tag_search to find the entry
     */
    FS_FILE_WRITE_LOCK(dir_context);
    ret = tag_search(
                     dir_vp,
                     file_context->bf_tag,
                     ftx_handle
                     );
    FS_FILE_UNLOCK(dir_context);
    /* ret is from tag_search */
    if (ret == EIO) {
    /* if error in parent dir, delete it anyway */
        ret = EOK;
        goto delete_it;
    }
    if (ret == I_FILE_EXISTS) {
        /*
         * the file is there, clean up and return
         */
        goto out;
    }
    /*
     * Make sure this is the index file itself
     */
    if (IDX_INDEXING_ENABLED(VTOA(dir_vp)))
    {
        bfAccessT *dir_bfap;
        bfTagT idx_tag;

        dir_bfap = VTOA(dir_vp);
        idx_tag = ((bsDirIdxRecT *)dir_bfap->idx_params)->index_tag;

        if (BS_BFTAG_EQL(rem_tag,idx_tag))
        {
            ret = I_FILE_EXISTS;
            goto out;
        }
    }

delete_it:
    rbf_delete(VTOA(nvp), ftx_handle);
out:
    ftx_done_n( ftx_handle, FTA_OSF_REMOVE_V1 );
    if (file_open) {
        vrele(nvp);
    }
    if (dir_open) {
        vrele(dir_vp);
    }
    return ret;
}


statusT
tag_search(
    struct vnode *dir_vp,
    bfTagT bf_tag,
    ftxHT ftx_handle
    )
{
    /*
     * return_status -
     *  EOK - didn't find the file
     *  I_FILE_EXISTS - found the the file
     *  EIO - error from refpg
     */
    statusT sts, return_status = EOK;
    int last_page, i, n, offset;
    bfPageRefHT page_ref;
    char *dir_buffer, *p;
    fs_dir_entry *dir_p, *last_p;
    struct bfAccess  *dir_access;
    bfTagT *tagp;
    dirRec *dirRecp;

    /*
     * ref the dir pages and look for the dir entry
     */
    dir_access = VTOA(dir_vp);
    last_page = howmany(dir_access->file_size, ADVFS_PGSZ);

    for (i = 0; i < last_page; i++) {
        sts = bs_refpg(
                       &page_ref,
                       (void *)&dir_buffer,
                       dir_access,
                       i,
                       BS_NIL
                       );
        if (sts != EOK) {
            return_status = EIO;
            break;
        }
        dir_p = (fs_dir_entry *)dir_buffer;
        dirRecp = (dirRec *)(dir_buffer + ADVFS_PGSZ -
                             sizeof(dirRec));
        last_p = (fs_dir_entry *)(dir_buffer +
                                  dirRecp->lastEntry_offset);
        while (dir_p <= last_p) {
            p = (char *)dir_p;
            n = (signed)dir_p->fs_dir_header.fs_dir_size;
            offset = (char *)dir_buffer - (char *)dir_p;
            if ( FS_DIR_SIZE_OK(n, offset)) {
                /*
                 * There is a minor data corruption, inform and
                 * resume on next block boundary 
                 */
                fs_dir_size_notice(((bfNodeT*)dir_vp->v_data)->accessp, 
                                    n, i, offset);
                p = (char *)dir_buffer;
                n =  (DIRBLKSIZ + (char *)dir_p - (char *)dir_buffer) & ~(DIRBLKSIZ-1);
            }
            if (dir_p->fs_dir_header.fs_dir_bs_tag_num != 0) {
                tagp = GETTAGP(dir_p);
                if (bf_tag.num == tagp->num) {
                    if (bf_tag.seq ==
                        tagp->seq) {
                        return_status = I_FILE_EXISTS;
                    }
                    break;
                }
            }
            dir_p = (fs_dir_entry *)(p + n);
        }

        bs_derefpg(page_ref, BS_NIL);
        if (return_status != EOK) {
            break;
        }
    }
    return (return_status);
}


/* remove_entry
 *
 * remove an entry that has no matching tag
 *
 */

statusT
remove_entry(
            bfAccessT *dir_access,
            char *file_name
            )
{
    statusT ret;
    bfTagT found_bs_tag, *tagp;
    char *buffer;
    fs_dir_entry *dir_p;
    rbfPgRefHT found_page_ref;
    struct fsContext *dir_context;
    struct vnode *dir_vp;
    ftxHT ftx_handle;
    bfAccessT *bfap;
    bfAccessT *idx_bfap = NULL;
    struct nameidata *ndp;
    uint32T entry_size;
    uint32T orig_entry_size;
    uint32T glom_flags=0;
    struct vnode *nullvp = NULL;
    int type;
    fileSetNodeT *fsnp = NULL;


    dir_vp = ATOV(dir_access);
    dir_context = VTOC(dir_vp);
    /* dummy up a namei structure to pass to seq_search */

    ndp = (struct nameidata *)ms_malloc(sizeof(struct nameidata));

    ndp->ni_dent.d_namlen = strlen(file_name);
    strcpy(ndp->ni_dent.d_name,file_name);

    /*
     * start a transaction.
     * exclusively lock the directory,
     * the call seq_search to find the entry
     * to delete
     */
    FS_FILE_WRITE_LOCK(dir_context);

    /* Prevent races with migrate code paths */

    /* Need the index lock because of call chain
     * idx_directory_insert_space --> idx_directory_insert_space_int -->
     * idx_index_get_free_pgs_int --> rbf_add_stg
     */
    IDX_GET_BFAP(dir_access, idx_bfap);
    if (idx_bfap != NULL) {
        MIGTRUNC_LOCK_READ( &(idx_bfap->xtnts.migTruncLk) );
    }

    /* this is called from verify so the fileset is already mounted.
     */
    fsnp = dir_access->bfSetp->fsnp;
    MS_SMP_ASSERT(fsnp != NULL);
    /* Need the quota locks because of call chain
     * idx_directory_insert_space --> idx_directory_insert_space_int -->
     * idx_index_get_free_pgs_int --> chk_blk_quota --> dqsync -->
     * rbf_add_stg
     * this is a VERY unlikely scenario: this call would not normally
     * add storage to the quota files, but this function is called
     * when verify finds something broken - in which case, the quota
     * file might be broken too and not contain the appropriate entry.  
     */
    for (type = 0; type < MAXQUOTAS; type++) {
        MIGTRUNC_LOCK_READ( &(fsnp->qi[type].qiAccessp->xtnts.migTruncLk) );
    }

    ret = FTX_START_N(
                      FTA_OSF_REMOVE_V1,
                      &ftx_handle,
                      FtxNilFtxH,
                      dir_context->fileSetNode->dmnP,
                      9
                      );
    ret = seq_search(
                     dir_vp,
                     ndp,
                     &found_bs_tag,
                     (fs_dir_entry **)&buffer,
                     &found_page_ref,
                     PIN_PAGE, /* pin the page */
                     ftx_handle
                     );
    if (ret == EIO) {
        goto out;
    }
    if (ret == I_INSERT_HERE) {
        /*
         * the file isn't here, clean up and return
         */
        ret = ENOENT;
        goto out;
    }
    /* anything else is a coding error */
    if (ret != I_FILE_EXISTS) {
        ret = ENOENT;
        goto out;
    }
    /*
     * make sure the file REALLY isn;t there by
     * opening it (only if the seqeuence number
     * isn;t 0)
     */
    if (found_bs_tag.seq != 0)
    {
        ret = bs_access(
                        &bfap,
                        found_bs_tag,
                        dir_context->fileSetNode->bfSetp,
                        FtxNilFtxH,
                        0,
                        NULLMT, 
                        &nullvp
                        );
        if (ret == EOK) {
            /* it's there, close it and get out */
            ret = EEXIST;
            bs_close(bfap, 0);
            goto out;
        }
    }
    /*
     * there is no file.
     * get the entry, delete it. Do this by setting the
     * tag number of the entry to 0.
     */

    dir_p = (fs_dir_entry *)(buffer + ndp->ni_offset);
    tagp = GETTAGP(dir_p);

    /* Setup for glomming the directory entries together. This routine
     * will not pin any pages which allows us to call ftx_fail in the
     * calls after. This basically tells us what the entry size will
     * look like after the glom. We will put this size in the
     * btree. This may also pick up any space that may have been lost
     * on a previous remove (a very rare occurance).  */

    orig_entry_size = dir_p->fs_dir_header.fs_dir_size;

    ret = setup_for_glom_dir_entries(buffer,
                                     &dir_p,
                                     &entry_size,
                                     &glom_flags,
                                     dir_access);
    if (ret != EOK)
    {
        /*
         * if the size field is bogus, return
         */
        goto out;
    }

    /*
     * Remove the filename from the b-tree. If this is an indexed
     * directory.
     */

    if (IDX_INDEXING_ENABLED(dir_access))
    {
        ret=idx_remove_filename(dir_access,
                                ndp,
                                ndp->ni_count,
                                ndp->ni_offset,
                                ftx_handle);
        if (ret != EOK)
            goto out;

        ret = idx_directory_insert_space( dir_access,
                                          entry_size,
                                          ndp->ni_count,
                                         (unsigned int)dir_p & (ADVFS_PGSZ - 1),
                                          ndp->ni_offset,
                                          orig_entry_size,
                                          ftx_handle,
                                          1 ); /* forceflag to ignore quota */
        if (ret != EOK)
        {
            if ((ret == ENO_MORE_BLKS) || (ret == ENO_MORE_MCELLS))
            {
                /* if there wasn't enough space to insert the space
                 * then ignore the error. It may get picked up later
                 */

                ret=EOK;
                dir_p = (fs_dir_entry *)(buffer + ndp->ni_offset);
                rbf_pin_record(
                               found_page_ref,
                               tagp,
                               sizeof(bfTagT)
                    );

                tagp->seq = (uint32T)(-1);

                /* It should not be the case that a glom occured and we
                 * needed space (ie a node split) for the b-tree. This
                 * is because a glom means the tree only needed to
                 * have an existing entry updated instead of a new
                 * entry inserted (only an insert can cause a node to
                 * split). Unless there is lost space next to it. Then
                 * we could have this situation.  Since we are about
                 * to lose this space turn off any glomming.  */
                  
                MS_SMP_ASSERT(!(glom_flags & SPACE_NEEDS_GLOMMING) ||
                              (glom_flags & LOST_SPACE_FOUND));

                glom_flags &= ~(SPACE_NEEDS_GLOMMING);

                lost_filename_space++;
            }
            else
            {
                goto out;
            }
        }            
    }
    if (dir_p  == (fs_dir_entry *)(buffer + ndp->ni_offset))
    {
        /* We don't need to update these fields if we were glommed
         * with a previous entry.
         */

        rbf_pin_record(
                       found_page_ref,
                       &dir_p->fs_dir_header.fs_dir_bs_tag_num,
                       sizeof(uint32T)
                      );
        dir_p->fs_dir_header.fs_dir_bs_tag_num = 0;
    }

    /*
     * glom the deleted entry onto any next to it
     */
    if (glom_flags & SPACE_NEEDS_GLOMMING)
    {
        if (glom_flags & LOST_SPACE_FOUND)
        {
            /* Keep some stats */
            MS_SMP_ASSERT(IDX_INDEXING_ENABLED(dir_access));
            recovered_filename_space++;
        }

        glom_dir_entries(
                         buffer,
                         dir_p,
                         entry_size,
                         glom_flags,
                         found_page_ref);
        
    }
    
    ftx_done_n(
               ftx_handle,
               FTA_OSF_REMOVE_V1
               );

    for (type = 0; type < MAXQUOTAS; type++) {
        MIGTRUNC_UNLOCK( &(fsnp->qi[type].qiAccessp->xtnts.migTruncLk) );
    }

    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }

    ms_free(ndp);

    FS_FILE_UNLOCK(dir_context);
    return EOK;

out:
    for (type = 0; type < MAXQUOTAS; type++) {
        MIGTRUNC_UNLOCK( &(fsnp->qi[type].qiAccessp->xtnts.migTruncLk) );
    }
    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }
    ms_free(ndp);
    ftx_fail(ftx_handle);
    FS_FILE_UNLOCK(dir_context);
    return ret;
}


statusT
setup_for_glom_dir_entries(
                           char *dir_buffer,
                           fs_dir_entry **start_dir_p,
                           uint32T *entry_size,
                           uint32T *flags,
                           bfAccessT *dir_access
                          )
{
    dirRec *dirRecp;
    int subpage, n;
    fs_dir_entry *dir_p,*dir_pp, *prev_p, *post_p;
    char *pp;
    bfTagT *tagp;
    uint32T skip_glom;

    dir_p = *start_dir_p;

    /*
     * check the previous entry for deleted (if current entry not on 512 byte
     * boundary). If so, glom the 2 deleted entries together.
     */
    *entry_size = dir_p->fs_dir_header.fs_dir_size;
    dirRecp = (dirRec *)(dir_buffer + ADVFS_PGSZ - sizeof(dirRec));
    if ((((char *)dir_p - dir_buffer) % DIRBLKSIZ) != 0) {
        subpage = ((char *)dir_p - dir_buffer) / DIRBLKSIZ;
        /*
         * dir_pp points to the first entry in the 512-byte subpage
         * find the entry previous to the one deleted.
         */
        dir_pp = (fs_dir_entry *)(dir_buffer + (subpage * DIRBLKSIZ));

        /*
         * Validate fs_dir_entry for this directory page.
         */

        while (dir_pp < dir_p) {
            pp = (char *)dir_pp;
            prev_p = dir_pp;

            pp += dir_pp->fs_dir_header.fs_dir_size;

            /*
             * if the size field is bogus, return an error.
             */

            n = (signed)dir_pp->fs_dir_header.fs_dir_size;
            if ( FS_DIR_SIZE_OK( n, 0 ) ) {
                fs_dir_size_notice(dir_access, n, -1,
                                  (char *)dir_p - (char *)dir_buffer);
                return(EIO);
            }
            dir_pp = (fs_dir_entry *)pp;
        }

        /*
         * The following code is attempting to glom the current dir
         * entry with its neighboring adjacent entries. In the case
         * of an indexed directory, under rare circumstances, we may
         * stumble across some lost space. If it is adjacent to us then
         * we can glom it too. Care must be taken though to also glom
         * any legitimate free space that may be after the lost space.
         * This is necesary first to have the largest space available
         * for handing out, but more importantly, keeping in sync with
         * the index.

        /*
         * the previous entry is deleted, so setup to glom the 2 entries
         * together
         */
        if (prev_p->fs_dir_header.fs_dir_bs_tag_num == 0) 
        {
            uint32T prev_p_size;
            prev_p_size = prev_p->fs_dir_header.fs_dir_size;
            skip_glom=FALSE;
            if (IDX_INDEXING_ENABLED(dir_access))
            {
                tagp=GETTAGP(prev_p);
                if ((int)tagp->seq == -1)
                {
                    if (*flags & SKIP_LOST_SPACE_GLOM)
                    {
                        skip_glom=TRUE;
                    }
                    else
                    {
                        fs_dir_entry *prev_prev_p;

                        /* Let the caller know */
                        *flags |= LOST_SPACE_FOUND;

                        /* We found some lost space. See if we are allowed
                         * to reclaim it.
                         */
                        /* We need to gather up all space that can be glommed
                         * This includes unlost free space as well as more lost
                         * space. Although not very efficient, the rarity of
                         * this code being excersized justifies it.
                         *
                         * We keep looking until we either hit used space,
                         * unlost space (which we will pick up) or the
                         * start of a 512 byte boundary.
                         */
                        while(1)
                        {
                            dir_pp =
                          (fs_dir_entry *)(dir_buffer + (subpage * DIRBLKSIZ));
                            /* If we are at the 512 byte boundary then we are done */
                            if (prev_p == dir_pp) break;

                            while (dir_pp < prev_p) 
                            {
                                pp = (char *)dir_pp;
                                prev_prev_p = dir_pp;
                                
                                pp += dir_pp->fs_dir_header.fs_dir_size;
                                dir_pp = (fs_dir_entry *)pp;
                            }

                            tagp=GETTAGP(prev_prev_p);
                            if ((int)tagp->seq == -1)
                            {
                                /* There is some adjacent lost space. Pick up
                                 * the space by updating the pointer and adding
                                 * in the size. Then continue checking for more
                                 * to glom.
                                 */
                                prev_p = prev_prev_p;
                                prev_p_size += prev_p->fs_dir_header.fs_dir_size;
                                continue;
                            }
                            if (prev_prev_p->fs_dir_header.fs_dir_bs_tag_num == 0) 
                            {
                                /* We found some real free space. This is a
                                 * stopping point. Add the space in and get out
                                 * of the loop.
                                 */
                                prev_p = prev_prev_p;
                                prev_p_size += prev_p->fs_dir_header.fs_dir_size;
                                break;
                            }
                            /* If we get here then we hit some used space. This is
                             * no more adjacent space to glom.
                             */
                            break;
                        }
                    }
                }
            }

            if (!skip_glom)
            {
                *entry_size += prev_p_size;
                MS_SMP_ASSERT((*entry_size <= DIRBLKSIZ) && (*entry_size > 0));

                /* if the newly-deleted entry was the last entry and
                 * we just glom'ed it, then inform the caller we need
                 * to re-set the lastEntry_offset for the page 
                 */

                if (dirRecp->lastEntry_offset == ((char *)dir_p - dir_buffer) ) 
                {
                    *flags |= UPDATE_LAST_ENTRY;
                }
                /*
                 * the entry pointed to by dir_p no longer exists...
                 */
                dir_p = prev_p;
                *flags |= SPACE_NEEDS_GLOMMING;
            }
        }
    }

    /*
     * now check the subsequent entry for being deleted (if its in the
     * same sub_page).
     */
    post_p = (fs_dir_entry *)((char *)dir_p + *entry_size);
    if ((((char *)post_p - dir_buffer) % DIRBLKSIZ) != 0)
    {
        if (post_p->fs_dir_header.fs_dir_bs_tag_num == 0) 
        {
            uint32T post_p_size;
            post_p_size = post_p->fs_dir_header.fs_dir_size;
            /*
             * it is, see if it can be glommed
             */

            skip_glom=FALSE;
            
            /*
             * Check if this is lost space 
             */
            
            if (IDX_INDEXING_ENABLED(dir_access))
            {
                tagp=GETTAGP(post_p);
                if ((int)tagp->seq == -1)
                {
                    /* We found some lost space. See if we are allowed
                     * to reclaim it
                     */
                    if (*flags & SKIP_LOST_SPACE_GLOM)
                    {
                        skip_glom=TRUE;
                    }
                    else
                    {
                        fs_dir_entry *post_post_p;

                        /* Let the caller know */
                        *flags |= LOST_SPACE_FOUND;

                        /* Same deal as for the previous entries. We
                         * need to check for glommable entries up until
                         * we find real free space or used space.
                         */
                        
                        post_post_p = post_p;

                        while(1)
                        {
                            post_post_p = (fs_dir_entry *)((char *)post_post_p + 
                                                           post_post_p->fs_dir_header.fs_dir_size);

                            /* We are at the 512 byte boundary bail out */
                            if ((((char *)post_post_p - dir_buffer) % DIRBLKSIZ
) == 0)
                                break;

                            tagp=GETTAGP(post_post_p);
                            if ((int)tagp->seq == -1)
                            {
                                /* There is some adjacent lost space. Pick up
                                 * the space by adding in the size. Then
                                 * continue checking for more to glom.
                                 */
                                 post_p_size += post_post_p->fs_dir_header.fs_dir_size;
                                continue;
                            }

                            if (post_post_p->fs_dir_header.fs_dir_bs_tag_num == 0) 
                            {
                                /* We found some real free space. This is a
                                 * stopping point. Add the space in and get out
                                 * of the loop.
                                 */
                                post_p_size += post_post_p->fs_dir_header.fs_dir_size;
                                break;
                            }
                            /* If we get here then we hit some used space. This is
                             * no more adjacent space to glom.
                             */

                            break;
                        }
                    }
                }
            }

            if (!skip_glom)
            {
                *entry_size += post_p_size;
                MS_SMP_ASSERT((*entry_size <= DIRBLKSIZ) && (*entry_size > 0));

                /*
                 * if it was the last entry, reset the last entry offset...
                 */
                
                if ((char *)post_p >= dir_buffer +
                    dirRecp->lastEntry_offset) 
                {
                    *flags |= UPDATE_LAST_ENTRY;
                }
                *flags |= SPACE_NEEDS_GLOMMING;
            }
        }
    }

    *start_dir_p=dir_p;
    return(EOK);
}


void
glom_dir_entries(char *dir_buffer,
                 fs_dir_entry *dir_p,
                 uint32T entry_size,
                 uint32T flags,
                 rbfPgRefHT page_ref
                 )
{
    dirRec *dirRecp;
    bfTagT *tagp;

    if (flags & LOST_SPACE_FOUND)
    {
        /* If this glommed entry contains some lost space
         * then unconditionaly reset the hidden tag field
         * just in case the lost space was tacked on to the
         * end
         */

        tagp = GETTAGP(dir_p);
        rbf_pin_record(page_ref,
                       tagp,
                       sizeof(bfTagT));
        tagp->seq=0;
    }

    rbf_pin_record(page_ref,
                   &dir_p->fs_dir_header.fs_dir_size,
                   sizeof(dir_p->fs_dir_header.fs_dir_size));

    dir_p->fs_dir_header.fs_dir_size = entry_size;

    if (flags & UPDATE_LAST_ENTRY)
    {
        dirRecp = (dirRec *)(dir_buffer + ADVFS_PGSZ - sizeof(dirRec));

        rbf_pin_record(page_ref,
                       &dirRecp->lastEntry_offset,
                       sizeof(dirRecp->lastEntry_offset));

        dirRecp->lastEntry_offset = (char *)dir_p - dir_buffer;
    }
}

/* This routine only decreases directory storage, never increases it.
 * The 'size_desired' passed in is the # of pages that this directory
 * should be when we return.  The directory must be WRITE-locked when this
 * routine is called.
 */

uint32T
dir_trunc_start( struct vnode *dvp,
                 unsigned long size_desired,
                 ftxHT ftx_handle )
{
    uint32T    delCnt = 0;
    long       file_size_in_bytes;
    void      *delList;
    bfAccessT *bfap;
    struct fsContext *contextp;
    dtinfoT *dtinfop = NULL;
    off_t   old_file_size;


    /* Sanity check; prevent truncation to zero pages */
    if (size_desired < 1) {
        return(delCnt);
    }

    bfap = VTOA(dvp);
    file_size_in_bytes = size_desired * ADVFS_PGSZ;
    if ( bfap->file_size <= file_size_in_bytes ) {
        /* no need to truncate; just return */
        return(delCnt);
    }

    /* Be sure that there is a write lock held on the dir */
    contextp = VTOC(dvp);
    MS_SMP_ASSERT(lock_holder(&contextp->file_lock));

    /* update the size of the file in the bfAccess */
    old_file_size = bfap->file_size;
    bfap->file_size = file_size_in_bytes;

    /* Call bf_setup_truncation() to start the truncate process.
     * Note: no one can have any pages to be truncated REF'd or PIN'd!
     */

    delCnt = bf_setup_truncation(bfap, ftx_handle, &delList, FALSE);

    /* First phase of storage removal is done; remainder must be done
     * outside of the current transaction.  Save the info required to
     * complete the storage deallocation in a dtinfoT struct hanging off
     * the bfAccess.  The calling routine will invoke dir_trunc_finish()
     * once this entire transaction has been committed.
     */
    if ( delCnt ) {
        if (!bfap->dirTruncp) {
            dtinfop = (dtinfoT *)ms_malloc(sizeof(dtinfoT));
            if (!dtinfop) {
                return(delCnt); /* It'll get deleted at reboot time */
            }
        } else {
            /* might still exist from uncommitted transaction */
            if (((dtinfoT *)(bfap->dirTruncp))->magic == DT_INFO_MAGIC_NUM)
                dtinfop = (dtinfoT *)bfap->dirTruncp;
            else {
                /* This is bad; found something hanging off the bfAccessT
                 * that is not a dtinfoT struct.
                 */
                domain_panic(bfap->dmnP,
                             "dir_trunc_start: bad bfap->dirTruncp");
            }
        }
        dtinfop->magic = DT_INFO_MAGIC_NUM;
        dtinfop->delCnt = delCnt;
        dtinfop->delList = delList;
        dtinfop->domain.dmnp = bfap->dmnP;

        /* not seizing a mutex since dir is locked and only these 2 routines
         * manipulate this value
         */
        bfap->dirTruncp = (void *)dtinfop;
    }
    else {
        /*
         * The attempt to truncate failed.  Put bfap->file_size back.
         */
        bfap->file_size = old_file_size;
    }

    return(delCnt);
}

/* This routine is called after the root transaction that might have started
 * a directory truncation successfully commits.  This routine checks if there
 * is directory truncation info associated with the bfAccess struct. If so,
 * it is packaged into a message, and sent it to the cleanup thread.  This
 * is a performance enhancement;  if the cleanup thread did not start, then
 * this routine finishes the truncation before returning to the caller.
 * Note that there can be only one dtinfo struct hanging off the access
 * structure at a time.  If the root transaction commits, then this
 * record is removed and deallocated by the dir_trunc_finish() routine.
 * If the create transaction aborts, then dir_trunc_finish() is never
 * called, and the record is left dangling, never to be used.  If the
 * access structure is recycled, the dangling record is deallocated; if
 * there is a subsequent truncation on the same directory before the
 * access structure is recycled, then the old record is overwritten with
 * the new information in dir_trunc_start().
 */

void dir_trunc_finish( struct vnode *dvp )
{
    bfAccessT *bfap;
    clupThreadMsgT *msg;
    extern msgQHT CleanupMsgQH;

    bfap = VTOA(dvp);

    if (bfap->dirTruncp) {
        dtinfoT *dtinfop = (dtinfoT *)bfap->dirTruncp;
        bfap->dirTruncp = (void *)NIL;

        /* Send a message to the cleanup thread to do the actual work */
        msg = (clupThreadMsgT *)msgq_alloc_msg( CleanupMsgQH );
        if (msg) {
            msg->msgType = FINSH_DIR_TRUNC;
            msg->body.dtinfo.delCnt = dtinfop->delCnt;
            msg->body.dtinfo.delList = dtinfop->delList;
            msg->body.dtinfo.domain.id = dtinfop->domain.dmnp->domainId;
            msgq_send_msg( CleanupMsgQH, msg );
        }
        ms_free(dtinfop);
    }

    return;
}
