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
 *   AdvFS 
 *
 * Abstract:
 *
 *   Directory lookup, insert, remove procedures 
 *
 */

#include <sys/stat.h>
#include <sys/errno.h>
#include <sys/kernel.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <fs/vfs_ifaces.h>            /* struct nameidata */

#include <fs_dir.h>
#include <ms_public.h>
#include <fs_dir_routines.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <ms_assert.h>
#include <bs_msg_queue.h>
#include <bs_index.h>
#include <advfs_evm.h>
#include <bs_extents.h>
#include <bs_snapshot.h>

#define ADVFS_MODULE FS_DIR_LOOKUP

/*
 * INSERT_SEQ_NEEDS_STORAGE is a ni_offset magic value set by seq_search()
 * to tell insert_seq that there is not enough space in the current directory
 * to store the file name and more storage will need to be allocated.
 */
#define INSERT_SEQ_NEEDS_STORAGE (-1)

/* A few statistic counters for some rare events */

static uint64_t lost_filename_space;
static uint64_t recovered_filename_space;


#ifdef ADVFS_SMP_ASSERT
/* Set this to non-zero to force remove_dir_ent() to return an error
 * this is useful for testing to force the cleanup path through
 * bs_delete_undo_opx.
 */
static int ForceRemoveDirEntFailure = 0;
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
           struct vnode*     dvp,       /* in - directory vnode */
                  bfTagT     new_bs_tag,/* in - tag of the new entry */
           struct vnode*     nvp,       /* in - new file vnode */
                  char      *name,      /* in - new file name */
                  int        flag,      /* in - HARD_LINK -  entry is being
                                                created for a hard link to an
                                                existing file, so BMT the stats
                                                START_FTX - create a 
                                                subtransation */
           struct nameidata *ndp,       /* in - nameidata pointer */
                  bfSetT    *bfSetp,    /* in - bitfile-set desc pointer */
                  fileSetNodeT *fsnp,   /* in - fileSetNode pointer */
                  ftxHT      ftx_handle /* in - transaction handle */
           )
{
    bs_meta_page_t insert_page;
    int new_space_flag = FALSE;
    off_t insert_byte;
    bs_meta_page_t curr_pages;
    fs_dir_entry *dir_p, *dir_pp, *dir_buffer;
    char *pp;
    char *begin_page;
    uint32_t entry_size, actual_size;
    char *insert_here;
    statusT ret;
    rbfPgRefHT page_ref;
    bfTagT found_bs_tag;
    struct bfAccess *dir_access;
    struct fsContext *dir_context_ptr, *file_context_ptr;
    ftxHT ins_ftx_handle;
    insert_undo_rec undo_rec;
    domainT *dmnP;
    size_t pageSz;      /* Page size in bytes. */
    int extended_dir_here = FALSE;
    int namelen;

    namelen = strlen(name);

    dmnP = fsnp->dmnP;

    undo_rec.undo_header.old_size=0;

    dir_access = VTOA(dvp);
    dir_context_ptr = VTOC( dvp );

    pageSz = dir_access->bfPageSz * ADVFS_FOB_SZ;

    if (dir_context_ptr->dir_stats.st_nlink < 2) {
        return (ENOENT);
    }
    file_context_ptr = VTOC( nvp );
    /*
     * start sub ftx
     */
    ret = FTX_START_N(FTA_FS_INSERT_V1,
                      &ins_ftx_handle,
                      ftx_handle,
                      dmnP);
    if (ret != EOK) {
        ADVFS_SAD1("ftx_start error in insert_seq", ret);
    }

    /* calculate the actual entry size */
    actual_size = DIR_HEADER_SIZE + namelen
        + (4 - (namelen % 4)) + sizeof(tagSeqT);
    actual_size = (actual_size + 7) & ~7;

    /*
     * check to see if the directory has been modified since the
     * lookup was performed. If yes, do another lookup. if not, just
     * fill in the page, byte and re_use_flag info from nameidata (it
     * was put there during lookup).
     */

index_try_again:

    if (ndp->ni_dirstamp == dir_context_ptr->dirstamp
        && ! (ndp->ni_nameiop & NI_BADDIRSTAMP))
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
            insert_page = ndp->ni_count;
            insert_byte = ndp->ni_offset;
            if ( ndp->ni_offset == INSERT_SEQ_NEEDS_STORAGE ) {
                new_space_flag = TRUE;
            }
        }

        /*
         * if new_space_flag, there is no room in the dir. go add
         * another page.
         */
        if (new_space_flag == TRUE) {
            goto insert_seq_add_stg;
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
                        ins_ftx_handle,
                        MF_VERIFY_PAGE
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
        insert_here = (char *)dir_buffer;
        insert_here += insert_byte;
        dir_p = (fs_dir_entry *)insert_here;
        /* save the old size so that the re-used entry can be
           split up later */
        MS_SMP_ASSERT(dir_p->fs_dir_bs_tag_num == 0);

        entry_size = dir_p->fs_dir_size;

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
         * then dvp's fscontext's fsc_last_dir_fob can be used to get the
         * page # with the last real directory entry.
         */

        ret = seq_search(
                         dvp,
                         ndp,
                         name,
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
            ndp->ni_nameiop &= ~NI_BADDIRSTAMP ;
            goto index_try_again;
        }
        insert_page = ndp->ni_count;
        insert_byte = ndp->ni_offset;
        if ( ndp->ni_offset == INSERT_SEQ_NEEDS_STORAGE ) {
            new_space_flag = TRUE;
        }

        if (new_space_flag == FALSE) {
            /*
             * Using a deleted or empty entry. Go back to the
             * 're-using an entry' code. First put last_useful_pg value + 1
             * into 'ndp' where the insert code expects it to be.  This
             * is the actual number of pages, and will always be > 0.
             */
            VTOC(dvp)->fsc_last_dir_fob = VTOC(dvp)->fsc_last_dir_fob + 
                                          VTOA(dvp)->bfPageSz;
            goto reuse_entry;
        }
    }

    /*
     * there was no space in the dir for the new entry. Add a page
     * to the dir.
     */
insert_seq_add_stg:
    ret = chk_blk_quota(dvp,
                        dir_access->bfPageSz,
                        TNC_CRED(),
                        0,
                        ins_ftx_handle);

    if (ret != EOK) {
        goto error;
    }

    curr_pages = howmany(dir_access->file_size, pageSz);

    MS_SMP_ASSERT((IDX_INDEXING_ENABLED(dir_access)) ||
                  (insert_page == curr_pages-1));

    ret = rbf_add_stg(dir_access, 
                      curr_pages * dir_access->bfPageSz, /* start fob */
                      dir_access->bfPageSz,              /* adding one pg */
                      ins_ftx_handle, STG_MIG_STG_HELD);

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
                    ins_ftx_handle,
                    MF_OVERWRITE | MF_NO_VERIFY
                    );
    if (ret != EOK) {
        ret = EIO;
        goto error;
    }

    if ((curr_pages == 1) && 
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
        {
            goto error;
        }
    }

    /*
     * pin_record the new page and init it (ie add an empty entry in each
     * sub_page)
     */
    if (IDX_INDEXING_ENABLED(dir_access))
    {
        /* Put back the page minus the block into the index free space
         * tree We need to do this prior to pinning any records in case
         * we fail to put the space into the b-tree.*/

        ret = idx_directory_insert_space(dir_access,
                                         pageSz - actual_size, /* size to insert */
                                         insert_page,          /* Page where space lives */
                                         actual_size,          /* Offset where space lives */
                                         actual_size,          /* Offset where space lives */
                                         pageSz - actual_size, /* size to insert */
                                         ins_ftx_handle);
        if (ret != EOK)
        {
            goto error;
        }
    }

    rbf_pin_record(
                   page_ref,
                   dir_buffer,
                   pageSz
                   );
    bzero(dir_buffer, pageSz);
    dir_p = (fs_dir_entry *)dir_buffer;
    while ((char *)dir_p < (char *)dir_buffer + pageSz) {
        dir_p->fs_dir_bs_tag_num = 0;
        dir_p->fs_dir_size = DIRBLKSIZ;
        dir_p = (fs_dir_entry *)((char *)dir_p + DIRBLKSIZ);
    }
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
    dir_access->file_size += pageSz;


    /* reset the buffer pointers and setup for the new entry on the
       new page */
    insert_here = (char *)dir_buffer;
    begin_page = (char *)dir_buffer;

    /*
     * insert the new entry
     */
insert_entry:

    (void) fs_assemble_dir(
                           insert_here,
                           new_bs_tag,
                           name
                           );
    /*
     * if over-writing a deleted entry, see if we can split off the
     * end into a separate deleted space... the end is useful if it
     * can hold at least a minimum directory entry. If not, just set
     * the new entry size to the size of the whole deleted space.
     */

    dir_p = (fs_dir_entry *)insert_here;
    if (dir_p->fs_dir_size < entry_size) {
        if ((entry_size - dir_p->fs_dir_size) >= MIN_DIR_ENTRY_SIZE) { 
            /* point pp to the space after the new entry */
            /* pin_recd it */
            pp = insert_here;
            pp += dir_p->fs_dir_size;
            rbf_pin_record(page_ref,
                           (void *)pp,
                           DIR_HEADER_SIZE 
                );
            dir_pp = (fs_dir_entry *)pp;
            dir_pp->fs_dir_bs_tag_num = 0;
            dir_pp->fs_dir_size = entry_size -
                dir_p->fs_dir_size;
            dir_pp->fs_dir_namecount = 0;
        } else {
            dir_p->fs_dir_size = entry_size;
        }
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
    undo_rec.undo_header.bfSetId = fsnp->bfSetId;
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
                                  name,
                                  insert_page,
                                  insert_byte,
                                  ftx_handle);
        if (ret != EOK)
        {
            return(ret);
        }
    }

    /* If we did not have to allocate more storage, check to see if the
     * directory can be truncated; if so, do it. Do not use new_space_flag
     * since the sense of it is reversed after new storage is allocated.
     */
    if ( !extended_dir_here )
    {
        bs_meta_page_t num_useful_pages;
        int64_t bytes_to_trunc,delCnt=0;
        /* convert current size in bytes to # pages */
        curr_pages = howmany(dir_access->file_size, 
                                dir_access->bfPageSz * ADVFS_FOB_SZ);

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
                
            num_useful_pages = curr_pages
                             - howmany(bytes_to_trunc, 
                                       dir_access->bfPageSz * ADVFS_FOB_SZ);
        }
        else {
            num_useful_pages =
                howmany(VTOC(dvp)->fsc_last_dir_fob, dir_access->bfPageSz);
        }

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
        VTOC(dvp)->fsc_last_dir_fob = 0;
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
 * entry. A -1 in the tag.tag_seq number which is appended to
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
               struct vnode     *dvp,  /* in - vnode of dir to remove from */
               struct nameidata *ndp,  /* in - nameidata structure pointer */
               char             *name, /* in - name of file to remove. */
               ftxHT ftx_handle,       /* in - ftx handle from msfs_remove */
               int call_dnlc_remove    /* in - TRUE - remove name from dnlc */
               )
{
    statusT ret;
    fs_dir_entry *dir_p;
    struct fsContext *dir_context, *file_context;
    rbfPgRefHT page_ref;
    char *dir_buffer;
    bfTagT tag;
    tagSeqT *seqp;
    bs_meta_page_t page;
    off_t dir_offset,new_dir_offset;
    uint32_t entry_size;
    uint32_t orig_entry_size;
    uint32_t glom_flags=0;
    struct bfAccess *dir_access;


#ifdef ADVFS_SMP_ASSERT
    if (ForceRemoveDirEntFailure) {
        return (EIO);
    }
#endif /* ADVFS_SMP_ASSERT */

    dir_access = VTOA(dvp);
    dir_context = VTOC( dvp );
    file_context = VTOC(ndp->ni_vp);

    if (call_dnlc_remove) {
        /*
         * purge the vnode from the cache
         */
        dnlc_remove(dvp, name);
    }
    page = ndp->ni_count;
    dir_offset = ndp->ni_offset;
    /*
     * pin the dir page with the entry to remove
     */
    ret = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_buffer,
                    dir_access,
                    page,
                    BS_NIL,
                    ftx_handle,
                    MF_VERIFY_PAGE
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

    dir_p = (fs_dir_entry *)(dir_buffer + dir_offset);
    GETTAG(tag,dir_p);
    seqp = GETSEQP(dir_p);
    if (!(BS_BFTAG_EQL(tag, file_context->bf_tag))) {

        ms_uaprintf("Bad dir format in remove_dir_ent dir tag = %ld, file tag = %ld\n",
                    dir_context->bf_tag.tag_num,
                    file_context->bf_tag.tag_num);
        MS_SMP_ASSERT(0);  /* Force a panic in-house */
        return(EIO);
    } 

    /* Setup for glomming the directory entries together. This routine will not
     * pin any pages which allows us to call ftx_fail in the calls after. This
     * basically tells us what the entry size will look like after the glom. We
     * will put this size in the btree. This may also pick up any space that may
     * have been lost on a previous remove (a very rare occurance). 
     */

    orig_entry_size = dir_p->fs_dir_size;

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
            
        ms_uaprintf("Dir size error in remove_dir_ent dir tag = %ld, file tag = %ld\n",
                    dir_context->bf_tag.tag_num,
                    file_context->bf_tag.tag_num);
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
                                name,
                                page,
                                dir_offset,
                                ftx_handle);

        if (ret != EOK) 
        {
            return(ret);
        }
        
        /* The call to setup_for_glom_dir_entries could have adjusted the
         * amount of freespace we are giving back. This adjustment may have
         * been to the start and/or the end.
         */

        new_dir_offset = (char *) dir_p - dir_buffer;

        MS_SMP_ASSERT(new_dir_offset <= dir_offset);


        /* Add the deleted space to the index free space tree. */

        ret = idx_directory_insert_space( dir_access,
                                          entry_size,
                                          page,
                                          new_dir_offset,
                                          dir_offset,
                                          orig_entry_size,
                                          ftx_handle);
        
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
                
                dir_p = (fs_dir_entry *)(dir_buffer + dir_offset);

                rbf_pin_record(page_ref,
                               seqp,
                               sizeof(*seqp));

                *seqp = (tagSeqT)(-1); 
                
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

    if (dir_p  == (fs_dir_entry *)(dir_buffer + dir_offset))
    {
        /* We don't need to update these fields if we were glommed
         * with a previous entry.
         */
        rbf_pin_record(
                       page_ref,
                       &dir_p->fs_dir_bs_tag_num,
                       sizeof(dir_p->fs_dir_bs_tag_num)
                      );

        dir_p->fs_dir_bs_tag_num = 0;
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
                         page_ref,
                         dir_access->bfPageSz * ADVFS_FOB_SZ);
    }

    /*
     * update the mtime for the directory,
     * incr dirstamp to signal that the dir has changed */
    ADVSMP_FILESTATS_LOCK( &dir_context->fsContext_lock );
    dir_context->fs_flag |= (MOD_MTIME | MOD_CTIME);
    dir_context->dirty_stats = TRUE;
    dir_context->dirstamp++;
    ADVSMP_FILESTATS_UNLOCK( &dir_context->fsContext_lock );
    
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
                    ftx_handle,
                    MF_VERIFY_PAGE
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
                   &dir_p->fs_dir_bs_tag_num,
                   sizeof(dir_p->fs_dir_bs_tag_num)
                   );

    /*
     * set the bf tag to 0, signalling deleted in the dir entry
     */

    dir_p->fs_dir_bs_tag_num = 0;

    /*
     * pin the word to be changed in the page for the '..' entry.
     */

    if ( FS_DIR_SIZE_OK(dir_p->fs_dir_size, 0)) {
        fs_dir_size_notice(((bfNodeT*)dvp->v_data)->accessp, 
                           dir_p->fs_dir_size,
                           0, 0);
        return EIO;
    }

    p = (char *)dir_p;
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_bs_tag_num,
                   sizeof(dir_p->fs_dir_bs_tag_num)
                   );

    /*
     * set the bf tag to 0, signalling deleted in the dir entry
     */

    dir_p->fs_dir_bs_tag_num = 0;

    return EOK;

} /* end remove_dots */

struct seq_search_stats {
        uint64_t page_hit;
        uint64_t pass_hit[3];     /* start: 0=from begin, 1=from save, 2=rescan */
        uint64_t bad_save;        /* saved page/offset was bad */
        uint64_t pass2_start;     /* using saved page/offset */
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
           struct vnode         *dir_vp,         /* in -dir's vnode ptr */
           struct nameidata     *ndp,
                  char          *file_name,      /* in  - name of file */
                  bfTagT        *found_bs_tag,   /* out - if found: file tag */
                  fs_dir_entry **found_buffer,   /* out - if found: buffer
                                                    address of page on which
                                                    entry was found */
                  rbfPgRefHT    *found_page_ref, /* out - if found:  page ref
                                                    for found_buffer if
                                                    under ftx */
                  int flag,                      /* in  - flag:
                                                  PIN_PAGE - pin pages searched,
                                                  REF_PAGE - ref pages searched,
                                                  USE_OFFSET use offset hint */
                  ftxHT          ftx_handle      /* in  - transaction handle */
           )
{
    char *p, *dir_page_end;
    fs_dir_entry *dir_p, *last_p, *first_p, *save_buffer;
    statusT return_value, ret;
    int n, j;
    bs_meta_page_t last_page;
    size_t entry_size;
    off_t offset;
    uint32_t last_ent_offset;
    struct fsContext *dir_context_pointer;
    rbfPgRefHT page_ref;
    bfPageRefHT page_ref1;
    int first_time, find_slot;
    bfTagT tag;
    bs_meta_page_t saved_page;
    int64_t saved_offset=0;
    struct bfAccess  *dir_access;
    int numDirPasses;   /* start: 0=from begin, 1=from save, 2=rescan */
    size_t pageSz;      /* Page size in bytes */
    bs_meta_page_t last_useful_page;  /* used for determining end of entries */
    uint16_t file_name_len;

    return_value = I_INSERT_HERE;
    find_slot = 1;
    first_time = TRUE;
    last_useful_page = 0;

    file_name_len = strlen(file_name);

    /* get the new entry size */
    n = DIR_HEADER_SIZE + file_name_len;
    entry_size = n + (4 - (n % 4));
    entry_size += sizeof(tagSeqT);
    entry_size = (entry_size + 7) & ~7;
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
                                  file_name,
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
        else if (ret != E_OUT_OF_SYNC_SNAPSHOT) {
            return(EIO);
        }
        /* Fall through if the index is out of sync. The
         * directory may well be out of sync too but give the
         * look up a chance of maybe seeing the directory
         * without the index.
         */
    }

    pageSz = dir_access->bfPageSz * ADVFS_FOB_SZ;
    last_page = howmany( dir_access->file_size, pageSz );
    /*
     * if lookup set the use_offset flag, then use the offset
     * stored in the context area from the last lookup as a guess.
     * This speeds up utilities that do a readdir
     * then open every file in order.
     */
    saved_page = saved_offset = 0;
    numDirPasses = 0;

    if (flag & USE_OFFSET) {
        saved_page = dir_context_pointer->last_offset / pageSz;
        saved_offset = dir_context_pointer->last_offset % pageSz;
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
                            ftx_handle,
                            MF_VERIFY_PAGE
                            );
            if (ret != EOK) {
                return (EIO);
            }
        } else {
            if (!first_time) {
                ret = bs_derefpg(
                                 page_ref1,
                                 BS_CACHE_IT
                                 );

                if (ret != EOK) {
                    ADVFS_SAD1("derefpg error in seq_search 1\n", ret);
                }
            }
            ret = bs_refpg(
                           &page_ref1,
                           (void *)&dir_p,
                           dir_access,
                           j,
                           FtxNilFtxH,
                           MF_VERIFY_PAGE);
            if (ret != EOK) {
                    return (EIO);
            }
        }
        first_time = FALSE;
        save_buffer = dir_p;
        first_p = dir_p;

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

        dir_page_end = ((char *)dir_p + pageSz);
        while ((char *)dir_p < dir_page_end) {
            /*
             * if the entry was deleted, check if it is a big enough
             * slot (in case we need one). if so, save it. Set
             * new_space_flag to FALSE to tell insert_seq that we are
             * re-using an entry.
             */
            if (dir_p->fs_dir_bs_tag_num == 0) {
                if (find_slot) {
                    /*
                     * see if it's big enough...
                     */
                    if (dir_p->fs_dir_size >= entry_size) {
                        ndp->ni_offset = (int64_t)((char *)dir_p - (char *)first_p);
                        find_slot = 0;
                        ndp->ni_count = j;
                        *found_buffer = save_buffer;
                        if (flag & PIN_PAGE) {
                            *found_page_ref = page_ref;
                        }
                    }
                }
            } else {
                if ((dir_p->fs_dir_namecount ==
                     file_name_len) && bcmp(dir_p->fs_dir_name_string,
                     file_name, file_name_len) == 0) {
                    /*
                     * found the entry, return bf tag and offset and
                     * get out of here
                     */
                    GETTAG(tag,dir_p);
                    *found_bs_tag = tag;
                    ndp->ni_offset = (int64_t)((char *)dir_p - (char *)first_p);
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
            n = (signed)dir_p->fs_dir_size;
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
    if ( return_value == I_INSERT_HERE ) {
        if ( find_slot == 0 ) {
            dir_context_pointer->fsc_last_dir_fob = 
                last_useful_page * dir_access->bfPageSz;
        }
        else {
            ndp->ni_offset = INSERT_SEQ_NEEDS_STORAGE;
        }
    }

    /*
     * if ref'ing pages (ie called from lookup) deref the final page.
     * if pinning and the page we are returning isn't currently pinned,
     * better do it
     */
    if (flag & REF_PAGE) {
        ret = bs_derefpg(
                         page_ref1,
                         BS_CACHE_IT
                         );

        if (ret != EOK) {
            ADVFS_SAD1("derefpg error in seq_search end", ret);
        }
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
                            ftx_handle,
                            MF_VERIFY_PAGE
                            );
            if (ret != EOK) {
                return (EIO);
            }
            *found_buffer = dir_p;
            *found_page_ref = page_ref;
        }
    }
    return (return_value);
} /* end of seq_search */


void
fs_dir_size_notice(
                   struct bfAccess* dirBfap, /* in - dir's access ptr     */
                   int entrySize,            /* in - entry's size         */
                   bs_meta_page_t pgNm,      /* in - dir's page number    */
                   uint32_t offset           /* in - offset in dir's page */
                   )
{                        
    advfs_ev advfs_event;

    uprintf("Warning: Some file(s) of AdvFS fileset = \"%s\" in domain = \"%s\"\n",
                dirBfap->bfSetp->bfSetName, dirBfap->dmnP->domainName);
    uprintf("belonging to directory tag %ld are inaccessible.\n", dirBfap->tag.tag_num);       
    uprintf("There is a corrupted directory entry size: on directory page %d, at offset 0x%x\n",
                pgNm, offset); 
    uprintf("  bad size = 0x%x,\n ", entrySize);

    if (dirBfap->badDirEntry == FALSE) {
        dirBfap->badDirEntry = TRUE;
        printf("Repairable AdvFs corruption detected, consider running fixfdmn.\n"); 
        printf("Warning: Some file(s) of AdvFS fileset = \"%s\" in domain = \"%s\"\n",
                    dirBfap->bfSetp->bfSetName, dirBfap->dmnP->domainName);
        printf("belonging to directory tag %ld are inaccessible.\n", dirBfap->tag.tag_num);       
        printf("There is a corrupted directory entry size: on directory page %d, at offset 0x%x\n",
                     pgNm, offset); 
        printf("  bad size = 0x%x,\n ", entrySize);

        bzero(&advfs_event, sizeof(advfs_ev));
        advfs_event.domain = dirBfap->dmnP->domainName;
        advfs_event.fileset = dirBfap->bfSetp->bfSetName; 
        advfs_event.dirTag = dirBfap->tag.tag_num;
        advfs_event.dirPg = pgNm;
        advfs_event.pgOffset = offset;
        advfs_post_kernel_event(EVENT_FSET_INCONSISTENT_DIR_ENTRY, &advfs_event);
    }

    return;
}


statusT
setup_for_glom_dir_entries(
                           char *dir_buffer,
                           fs_dir_entry **start_dir_p,
                           uint32_t *entry_size,
                           uint32_t *flags,
                           bfAccessT *dir_access
                          )
{
    int subpage, n;
    fs_dir_entry *dir_p,*dir_pp, *prev_p, *post_p;
    char *pp;
    bfTagT tag;
    uint32_t skip_glom;

    dir_p = *start_dir_p;

    /*
     * check the previous entry for deleted (if current entry not on 512 byte
     * boundary). If so, glom the 2 deleted entries together.
     */
    *entry_size = dir_p->fs_dir_size;
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

            pp += dir_pp->fs_dir_size;

            /*
             * if the size field is bogus, return an error.
             */

            n = (signed)dir_pp->fs_dir_size;
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
         */

        /*
         * the previous entry is deleted, so setup to glom the 2 entries
         * together
         */
        if (prev_p->fs_dir_bs_tag_num == 0) 
        {
            uint32_t prev_p_size;
            prev_p_size = prev_p->fs_dir_size;
            skip_glom=FALSE;
            if (IDX_INDEXING_ENABLED(dir_access))
            {
                GETTAG(tag,prev_p);
                if ((int)tag.tag_seq == -1)
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
                                
                                pp += dir_pp->fs_dir_size;
                                dir_pp = (fs_dir_entry *)pp;
                            }

                            GETTAG(tag,prev_prev_p);
                            if ((int)tag.tag_seq == -1)
                            {
                                /* There is some adjacent lost space. Pick up
                                 * the space by updating the pointer and adding
                                 * in the size. Then continue checking for more
                                 * to glom.
                                 */
                                prev_p = prev_prev_p;
                                prev_p_size += prev_p->fs_dir_size;
                                continue;
                            }
                            if (prev_prev_p->fs_dir_bs_tag_num == 0) 
                            {
                                /* We found some real free space. This is a
                                 * stopping point. Add the space in and get out
                                 * of the loop.
                                 */
                                prev_p = prev_prev_p;
                                prev_p_size += prev_p->fs_dir_size;
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
        if (post_p->fs_dir_bs_tag_num == 0) 
        {
            uint32_t post_p_size;
            post_p_size = post_p->fs_dir_size; 
            /*
             * it is, see if it can be glommed
             */

            skip_glom=FALSE;
            
            /*
             * Check if this is lost space 
             */
            
            if (IDX_INDEXING_ENABLED(dir_access))
            {
                GETTAG(tag,post_p);
                if ((int)tag.tag_seq == -1)
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
                                                           post_post_p->fs_dir_size);

                            /* We are at the 512 byte boundary bail out */
                            if ((((char *)post_post_p - dir_buffer) % DIRBLKSIZ
) == 0)
                                break;

                            GETTAG(tag,post_post_p);
                            if ((int)tag.tag_seq == -1)
                            {
                                /* There is some adjacent lost space. Pick up
                                 * the space by adding in the size. Then
                                 * continue checking for more to glom.
                                 */
                                 post_p_size += post_post_p->fs_dir_size;
                                continue;
                            }

                            if (post_post_p->fs_dir_bs_tag_num == 0) 
                            {
                                /* We found some real free space. This is a
                                 * stopping point. Add the space in and get out
                                 * of the loop.
                                 */
                                post_p_size += post_post_p->fs_dir_size;
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
                 uint32_t entry_size,
                 uint32_t flags,
                 rbfPgRefHT page_ref,
                 size_t dir_page_size       /* Directory page size in bytes */ 
                 )
{
    tagSeqT *seqp;

    if (flags & LOST_SPACE_FOUND)
    {
        /* If this glommed entry contains some lost space
         * then unconditionaly reset the hidden tag field
         * just in case the lost space was tacked on to the
         * end
         */

        seqp = GETSEQP(dir_p);
        rbf_pin_record(page_ref,
                       seqp,
                       sizeof(*seqp));
        *seqp = 0;
    }

    rbf_pin_record(page_ref,
                   &dir_p->fs_dir_size,
                   sizeof(dir_p->fs_dir_size));

    dir_p->fs_dir_size = entry_size;

}

/* This routine only decreases directory storage, never increases it.
 * The 'size_desired' passed in is the # of pages that this directory
 * should be when we return.  The directory must be WRITE-locked when this
 * routine is called.
 */

uint32_t
dir_trunc_start( struct vnode *dvp,
                 bs_meta_page_t size_desired,
                 ftxHT ftx_handle )
{
    uint32_t  delCnt = 0;
    int64_t   file_size_in_bytes;
    void      *delList;
    bfAccessT *bfap;
    struct fsContext *contextp;
    dtinfoT *dtinfop = NULL;
    off_t   old_file_size;
    statusT sts;

    /* Sanity check; prevent truncation to zero pages */
    if (size_desired < 1) {
        return(delCnt);
    }

    bfap = VTOA(dvp);
    file_size_in_bytes = size_desired * (bfap->bfPageSz * ADVFS_FOB_SZ);
    if ( bfap->file_size <= file_size_in_bytes ) {
        /* no need to truncate; just return */
        return(delCnt);
    }

    if ( HAS_SNAPSHOT_CHILD( bfap ) ) {
        /*
         * Make sure to COW the part of the directory to be COWed.  Note
         * that this is being done in a transaction, so we will pass an FTX
         * but limit the amount of data that can be truncated.  This will
         * prevent a log half full possibliity from COWing too much data.
         */
        off_t start_of_cow = file_size_in_bytes;
        SNAP_STATS( dir_trunc_start_forcing_cow );
        if (bfap->file_size - start_of_cow > 
                (int64_t)((AdvfsMaxAllocFobCnt * ADVFS_FOB_SZ) / 
                          bfap->bfSetp->bfsNumSnapChildren) ) {
            SNAP_STATS( dir_trunc_start_limiting_truncate );

            /*
             * Set file_size_in_bytes to the current file size minus as much
             * as we can COW this go.
             */
            file_size_in_bytes = bfap->file_size - 
                ((AdvfsMaxAllocFobCnt * ADVFS_FOB_SZ) / bfap->bfSetp->bfsNumSnapChildren);
            start_of_cow = file_size_in_bytes;
            MS_SMP_ASSERT( bfap->file_size > file_size_in_bytes );
            MS_SMP_ASSERT( file_size_in_bytes > 0 );
        }

        sts = advfs_force_cow_and_unlink( bfap,
                                        start_of_cow,
                                        bfap->file_size - start_of_cow,
                                        SF_NO_UNLINK,
                                        ftx_handle);
        if (sts != EOK) {
            SNAP_STATS( dir_trunc_start_cow_failed );
            sts = EOK;
        }
    }

    /* Be sure that there is a write lock held on the dir */
    contextp = VTOC(dvp);
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&contextp->file_lock, RWL_UNLOCKED));

    /* update the size of the file in the bfAccess */
    old_file_size = bfap->file_size;
    bfap->file_size = file_size_in_bytes;

    /* Call bf_setup_truncation() to start the truncate process.
     * Note: no one can have any pages to be truncated REF'd or PIN'd!
     */

    sts = bf_setup_truncation( bfap, ftx_handle, 0L, &delList, &delCnt );

    /* This should never be, but lets catch it in debug if it does. */
    MS_SMP_ASSERT(sts == EOK);

    /* First phase of storage removal is done; remainder must be done
     * outside of the current transaction.  Save the info required to
     * complete the storage deallocation in a dtinfoT struct hanging off
     * the bfAccess.  The calling routine will invoke dir_trunc_finish()
     * once this entire transaction has been committed.
     */
    if ( delCnt ) {
        if (!bfap->dirTruncp) {
            dtinfop = (dtinfoT *)ms_malloc(sizeof(dtinfoT));
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
    advfs_handyman_thread_msg_t *msg;
    extern msgQHT advfs_handyman_msg_queue;

    bfap = VTOA(dvp);

    if (bfap->dirTruncp) {
        dtinfoT *dtinfop = (dtinfoT *)bfap->dirTruncp;
        bfap->dirTruncp = (void *)NIL;

        /* Send a message to the cleanup thread to do the actual work */
        msg = (advfs_handyman_thread_msg_t *)msgq_alloc_msg( advfs_handyman_msg_queue );
        if (msg) {
            msg->ahm_msg_type = AHME_FINISH_DIR_TRUNC;
            msg->ahm_data.ahm_dtinfo.delCnt = dtinfop->delCnt;
            msg->ahm_data.ahm_dtinfo.delList = dtinfop->delList;
            msg->ahm_data.ahm_dtinfo.domain.id = dtinfop->domain.dmnp->domainId;
            msgq_send_msg( advfs_handyman_msg_queue, msg );
        }
        ms_free(dtinfop);
    }

    return;
}
