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
 *   Directory initialization routines 
 *   Kernel mode file system dismount routine
 *
 */

#include <fs_dir.h>
#include <fs_dir_routines.h>
#include <ms_public.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <sys/param.h>
#include <sys/kernel.h>
#include <sys/time.h>
#include <bs_params.h>
#include <ms_assert.h>
#include <bs_msg_queue.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>

#define ROOTMASK 0755
#define TAGSMASK 0700
#define QUOTMASK 0640
static char dot_name[] = ".";
static char dot_dot_name[] = "..";
#define ADVFS_MODULE FS_DIR_INIT

static statusT
create_root_file(
    bfTagT *tagp,
    bfAccessT **outbfap,
    bfSetT *bfSetp,
    bfTagT rootTag,
    bfParamsT *bfParams,
    char *name,
    off_t size,
    mode_t mode,
    int nlinks,
    gid_t gid,
    ftxHT ftxH
    );

/*
 * fs_init_directory
 *
 * Initialize a new directory with entries for '.' and '..'
 */


void
fs_init_directory(
                  char *buffer_pointer,  /* in - ptr to buf for new entries */
                  bfTagT bs_tag,         /* in - directory's tag */
                  bfTagT parent_bs_tag,  /* in - parent directory's tag */
                  bf_fob_t dir_page_size /* in - size of directory
                                            pages in 1k file offset blocks */
    )
{
    fs_dir_entry *dir_p;
    char *p, *q;

    /*
     * add the entry for "."
     */

    fs_assemble_dir(
                    buffer_pointer,
                    bs_tag,
                    dot_name
                    );

    /*
     * point p to after the '.' entry
     */

    p = buffer_pointer;
    dir_p = (fs_dir_entry *)buffer_pointer;
    p += dir_p->fs_dir_size;

    /*
     * add the ".." entry
     */

    fs_assemble_dir(
                    p,
                    parent_bs_tag,
                    dot_dot_name
                    );
    /*
     * add the empty entry for the first sub_page
     */
    dir_p = (fs_dir_entry *)p;
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    dir_p->fs_dir_bs_tag_num = 0;
    dir_p->fs_dir_size = DIRBLKSIZ - (p - buffer_pointer);

    /*
     * add the empty entries for the rest of the subpages
     */
    dir_p = (fs_dir_entry *)(buffer_pointer + DIRBLKSIZ);
    while ((char *)dir_p < buffer_pointer + (dir_page_size * ADVFS_FOB_SZ)) {
        dir_p->fs_dir_bs_tag_num = 0;
        dir_p->fs_dir_size = DIRBLKSIZ;
        dir_p = (fs_dir_entry *)((char *)dir_p + DIRBLKSIZ);
    }

}    /* fs_init_directory */


/*
 * fs_assemble_dir
 *
 *      assemble a directory entry
 */

void
fs_assemble_dir(
                char *dir_buffer,     /* in - pointer to where to create the entry */
                bfTagT new_bs_tag,    /* in - tag of the new entry */
                char *new_file_name   /* in - new file name string */
                )
{
    struct directory_entry *dir_p;
    char *p;
    int remain;
    tagSeqT *seqp;

    /*
     * for now :
     * fill in file name, tag, name count, total size
     * add at least one 0-byte at the end of the name (round to a longword)
     */
    dir_p = (struct directory_entry *) dir_buffer;
    dir_p->fs_dir_bs_tag_num = new_bs_tag.tag_num;
    (void) strcpy (dir_p->fs_dir_name_string, new_file_name);
    dir_p->fs_dir_namecount = strlen (new_file_name);

    /*
     * add up the size
     * its the size up to the name string, plus the name string
     */

    dir_p->fs_dir_size = DIR_HEADER_SIZE +
                 dir_p->fs_dir_namecount;

    /*
     * null-fill the name up to the nearest 4 byte boundary so that
     * the hidden tag_seq will be properly aligned
     *
     * if the record already ends on a 4 byte boundary then add nulls
     * to ensure the name string is null terminated 
     */

    p = (char *) dir_p;
    p += dir_p->fs_dir_size;
    remain = 4 - ((dir_p->fs_dir_size) % 4);

    while (remain--){
        *p++ = '\0';
        dir_p->fs_dir_size += 1;
    }
    /*
     * put the tag's sequence number after the null-filled namestring
     */
    p = dir_buffer + dir_p->fs_dir_size;
    seqp = (tagSeqT *)p;
    *seqp = new_bs_tag.tag_seq;
    dir_p->fs_dir_size += sizeof(tagSeqT);

    /* the first field in a directory_entry record is the fs_dir_bs_tag_num
     * which is a 64 bit value and must be aligned accordingly
     *  - ensure 8 byte alignment for the following directory_entry record 
     */
    dir_p->fs_dir_size = (dir_p->fs_dir_size + 7) & ~7;

} /* end fs_assemble_dir */


/*
 * dir_empty
 *
 * check to see if a directory is empty
 * this means look at every entry, since deleted entries
 * are still in the directory...
 *
 * TODO - instead of doing this scan, can't we keep some sort of
 * "empty" state in the directories stats??
 */

statusT
dir_empty(
          bfAccessT *bfap,              /* in - dir's access structure */
          struct fsContext *dir_context /* in - ptr to dir's context area */
          )
{
    bs_meta_page_t j, n;
    bs_meta_page_t num_pages;
    statusT ret;
    fs_dir_entry *dir_p;
    bfPageRefHT page_ref;
    size_t pageSz;      /* dir page size in bytes. */
    char *dir_page_end;

    pageSz = bfap->bfPageSz * ADVFS_FOB_SZ;
    num_pages = howmany( bfap->file_size, pageSz );
    /*
     * Scan all dir pages
     */
    for (j = 0; j < num_pages; j++) {

        ret = bs_refpg(
                       &page_ref,
                       (void *)&dir_p,
                       bfap,
                       j,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (ret != EOK) {
            return (EIO);
        }

        dir_page_end = ((char *)dir_p + pageSz);

        /*
         * Scan all entries
         */
        while ((char *)dir_p < dir_page_end) {

            n = strcmp("..", dir_p->fs_dir_name_string);

            if (n != 0) {
                n = strcmp(".", dir_p->fs_dir_name_string);

                if (n != 0) {
                    if (dir_p->fs_dir_bs_tag_num != 0) {
                        /*
                         * The directory is not empty.  Release dir
                         * page and return I_FILE_EXISTS.
                         */
                        ret = bs_derefpg(
                                         page_ref,
                                         BS_CACHE_IT
                                         );
                        if (ret != EOK) {
                            ADVFS_SAD1("derefpg(1) error in dir_empty", ret);
                        }

                        return (I_FILE_EXISTS);
                    }
                }
            }

            n = (signed)dir_p->fs_dir_size;
            if ( n <= 0 ) {
                goto dir_error;
            }
            dir_p = (fs_dir_entry *)((char *)dir_p + n);

        } /* end of while loop */

        ret = bs_derefpg(
                         page_ref,
                         BS_CACHE_IT
                         );
        if (ret != EOK) {
            ADVFS_SAD1("derefpg(2) error in dir_empty", ret);
        }
    } /* end of for loop */

    /*
     * it is empty
     */
    return (EOK);

dir_error:
    bs_derefpg( page_ref, BS_CACHE_IT );
    return (EIO);
}

/*
 * fs_create_file_set
 *
 * Creates the root and .tags directories in a new file set.
 */


statusT
fs_create_file_set(
    bfSetT *bfSetp,  /* in - file set's bitfile set desc pointer */
    gid_t quotaId,   /* in - group ID for quota files */
    ftxHT parentFtxH /* in - transaction handle */
    )
{
    unsigned char *buffer;
    char *p;
    statusT sts;
    rbfPgRefHT rootPg, tagsPg;
    fs_dir_entry *dir_p;
    bfParamsT *bfParamsp = NULL;
    bfSetParamsT *bfSetParamsp = NULL;
    bfTagT rootTag, tagsTag;
    bfTagT userQuotaTag, groupQuotaTag;
    int rootOpen = 0, tagsOpen = 0;
    int userQuotaOpen = 0,  groupQuotaOpen = 0;
    bfAccessT *rootbfap, *tagsbfap, *userQuotabfap, *groupQuotabfap;
    ftxHT ftxH;


    bfSetParamsp  = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));
    bfParamsp  = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));

    bfParamsp->bfpPageSize = ADVFS_METADATA_PGSZ_IN_FOBS;

    sts = create_root_file(&rootTag,
                           &rootbfap,
                           bfSetp,
                           NilBfTag,
                           bfParamsp,
                           "/",
                           ADVFS_METADATA_PGSZ,
                           S_IFDIR | ROOTMASK,
                           3,
                           (gid_t) 0,
                           parentFtxH);
    if (sts != EOK) {
        goto _error;
    }
    rootOpen = 1;

    MS_SMP_ASSERT(BS_BFTAG_IDX(rootTag) == ROOT_FILE_TAG);
    sts = create_root_file(&tagsTag,
                           &tagsbfap,
                           bfSetp,
                           rootTag,
                           bfParamsp,
                           ".tags",
                           ADVFS_METADATA_PGSZ,
                           S_IFDIR | TAGSMASK,
                           2,
                           (gid_t) 0,
                           parentFtxH);

    if (sts != EOK) {
        goto _error;
    }
    tagsOpen = 1;

    MS_SMP_ASSERT(BS_BFTAG_IDX(tagsTag) == TAGS_FILE_TAG);
    bfParamsp->bfpPageSize = ADVFS_METADATA_PGSZ_IN_FOBS;

    sts = create_root_file(&userQuotaTag,
                           &userQuotabfap,
                           bfSetp,
                           rootTag,
                           bfParamsp,
                           quotaFileNames[USRQUOTA],
                           ADVFS_METADATA_PGSZ,
                           S_IFREG | QUOTMASK,
                           1,
                           quotaId,
                           parentFtxH);

    if (sts != EOK) {
        goto _error;
    }
    userQuotaOpen = 1;

    MS_SMP_ASSERT(BS_BFTAG_IDX(userQuotaTag) == USER_QUOTA_FILE_TAG);

    sts = create_root_file(&groupQuotaTag,
                           &groupQuotabfap,
                           bfSetp,
                           rootTag,
                           bfParamsp,
                           quotaFileNames[GRPQUOTA],
                           ADVFS_METADATA_PGSZ,
                           S_IFREG | QUOTMASK,
                           1,
                           quotaId,
                           parentFtxH);

    if (sts != EOK) {
        goto _error;
    }
    groupQuotaOpen = 1;

    MS_SMP_ASSERT(BS_BFTAG_IDX(groupQuotaTag) == GROUP_QUOTA_FILE_TAG);

    sts = bs_get_bfset_params(bfSetp, bfSetParamsp, 0);
    if (sts != EOK) {
        goto _error;
    }

    bfSetParamsp->quotaStatus = QSTS_DEFAULT;

    sts = rbf_set_bfset_params(bfSetp, bfSetParamsp, parentFtxH, 0);
    if (sts != EOK) {
        goto _error;
    }

    sts = FTX_START_N(FTA_FS_DIR_INIT_1, &ftxH, parentFtxH,
                      bfSetp->dmnP);
    if (sts != EOK) {
        goto _error;
    }

    /*********************************************************************
     *  NOTE:  Beyond this point we can't fail because we will pin and
     *         modify pages.  We ensured that the common things that
     *         could fail (like adding storage) have already been done.
     *********************************************************************/

    /*
     * create the root directory and make . and .. point to itself
     */

    sts = rbf_pinpg( &rootPg, 
                     (void *)&buffer, 
                     rootbfap, 
                     0, 
                     BS_NIL, 
                     ftxH,
                     MF_NO_VERIFY);
    if (sts != EOK) {
        ADVFS_SAD1( "fs_create_file_set: pin pg failed;", sts);
    }

    rbf_pin_record( rootPg, buffer, bfParamsp->bfpPageSize * ADVFS_FOB_SZ);
    fs_init_directory( (char*)buffer, rootTag, rootTag, bfParamsp->bfpPageSize );

    /*
     * Skip over entries for "." and "..".
     */
    dir_p = (fs_dir_entry *)buffer;
    p = (char *)dir_p;
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    p = (char *)dir_p;
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;

    /*
     * Add the directory entry for ".tags".
     */
    fs_assemble_dir( (char *)dir_p, tagsTag, ".tags");
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;

    /*
     * Add the directory entry for user quotas.
     */
    fs_assemble_dir( (char *)dir_p, userQuotaTag, quotaFileNames[0]);
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;

#ifdef WHEN_HPUX_SUPPORTS_GROUP_QUOTAS
    /*
     * Add the directory entry for group quotas.
     */
    fs_assemble_dir( (char *)dir_p, groupQuotaTag, quotaFileNames[1]);
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;
#endif

    /*
     * Fill up the sub_page with an empty entry
     */
    dir_p->fs_dir_bs_tag_num = 0;
    dir_p->fs_dir_size = DIRBLKSIZ -
                                       ((char *)dir_p - (char *)buffer);

    /*
     * Create the .tags directory (init its "." and "..").
     */
    sts = rbf_pinpg( &tagsPg, 
                     (void *)&buffer, 
                     tagsbfap, 
                     0, 
                     BS_NIL, 
                     ftxH,
                     MF_NO_VERIFY);
    if (sts != EOK) {
        ADVFS_SAD1( "fs_create_file_set: pin pg failed", sts);
    }

    rbf_pin_record( tagsPg, buffer, tagsbfap->bfPageSz * ADVFS_FOB_SZ);
    fs_init_directory( (char*)buffer, tagsTag, rootTag, ADVFS_METADATA_PGSZ_IN_FOBS);

    sts = quota_files_init(userQuotabfap, groupQuotabfap, ftxH);
    if (sts != EOK) {
        ADVFS_SAD1( "fs_create_file_set: quota files init failed", sts);
    }

    ftx_done_n(ftxH, FTA_FS_DIR_INIT_1 );

_error:
    if (rootOpen) (void) bs_close(rootbfap, MSFS_CLOSE_NONE);
    if (tagsOpen) (void) bs_close(tagsbfap, MSFS_CLOSE_NONE);
    if (userQuotaOpen) (void) bs_close(userQuotabfap, MSFS_CLOSE_NONE);
    if (groupQuotaOpen) (void) bs_close(groupQuotabfap, MSFS_CLOSE_NONE);

    if (bfParamsp != NULL) {
        ms_free( bfParamsp );
    }
    if (bfSetParamsp != NULL) {
        ms_free( bfSetParamsp );
    }
    return sts;
}

/*
 * Create one of the automatically create files in a new fileset.
 *
 * These files include "/", ".tags", "quota.user", and "quota.group".
 *
 * If the "size" parameter is greater than 0, then the corresponding
 * number of fobs is allocated and zeroed.
 *
 * Note that this function has no undo because all the work is
 * done in called functions which have undo routines.  There is
 * an exception in the bzeroing of the data page of the created file,
 * but this is OK, since we really don't care if that is undone.
 */


static statusT
create_root_file(
    bfTagT *tagp,               /* out - tag of created bitfile */
    bfAccessT **outbfap,        /* out - access structure of created bitfile */
    bfSetT *bfSetp,             /* in - bf set desc pointer */
    bfTagT rootTag,             /* in - tag of parent dir */
    bfParamsT *bfParams,        /* in - parameters for create */
    char *name,                 /* in - file name */
    off_t size,                 /* in - size of file in bytes */
    mode_t mode,                /* in - mode of created file */
    int nlinks,                 /* in - number of links */
    gid_t gid,                  /* in - group ID */
    ftxHT parentFtxH            /* in - transaction handle */
    )
{
    statusT sts;
    bfAccessT *bfap;
    struct timeval createTime;
    ftxHT ftxH;
    rbfPgRefHT pgH;
    void *buffer;
    statT *dir_stats;
    struct vnode *nullvp = NULL;

    statusT bs_inherit_init(bfAccessT *bfap, ftxHT ftxH);

    dir_stats = (statT *)ms_malloc( sizeof(statT) );

    createTime = get_system_time();
    if ((sts = FTX_START_N(FTA_FS_CREATE_ROOT_FILE, &ftxH, parentFtxH,
                           bfSetp->dmnP)) != EOK) {
        ms_free(dir_stats);
        return sts;
    }

    if ((sts = rbf_create(tagp, bfSetp, bfParams, ftxH, CRT_INTERNAL,
                          BFD_METADATA)) != EOK) {
        ftx_fail(ftxH);
        ms_free(dir_stats);
        return sts;
    }

    sts = bs_access(&bfap, *tagp, bfSetp, ftxH, BF_OP_INTERNAL, &nullvp);
    if ( sts != EOK ) {
        ftx_fail(ftxH);
        ms_free(dir_stats);
        return sts;
    }

    if (size > 0) {
        bf_fob_t fobCnt;

        fobCnt = howmany(size, ADVFS_FOB_SZ);

        if (fobCnt > ADVFS_METADATA_PGSZ_IN_FOBS) {
            /*
             * No caller should ever want to add more than a page
             * of storage.  If we ever want to add this functionality
             * we'll have to bzero them in an loop, starting a new
             * subtransaction every few pages.
             */
            ADVFS_SAD0("create_root_file: page count > 1");
        }

        if ((sts = rbf_add_stg(bfap, 0L, fobCnt, ftxH, STG_NO_FLAGS)) != EOK) {
            goto _error;
        }

        if ((sts = rbf_pinpg(&pgH, 
                             &buffer, 
                             bfap, 
                             0L, 
                             BS_NIL, 
                             ftxH,
                             MF_NO_VERIFY)) != EOK) {
            goto _error;
        }

        rbf_pin_record(pgH, (void *)buffer, bfParams->bfpPageSize * ADVFS_FOB_SZ);
        bzero((char *) buffer, size);
    }

    /* Set up stats and write them to disk */

    dir_stats->st_ino = *tagp;
    /* no need to set fragPageOffset */
    dir_stats->dir_tag = rootTag;
    dir_stats->st_atime = createTime.tv_sec;
    dir_stats->st_mtime = createTime.tv_sec;
    dir_stats->st_ctime = createTime.tv_sec;
    dir_stats->st_size = size;
    bfap->file_size = size;
    dir_stats->st_mode = mode;
    dir_stats->st_nlink = nlinks;
    dir_stats->st_uid = 0;
    dir_stats->st_gid = gid;

    sts = bmtr_put_rec(bfap, BMTR_FS_STAT, dir_stats, sizeof(statT), ftxH);

    if (BS_BFTAG_EQL(rootTag, NilBfTag)) {
        /*
         * Root tag should only be Nil if we are creating the
         * root directory itself.
         */
        rootTag = *tagp;

        /*
         * Init root's inheritable attributes.
         */
        if ( (sts = bs_inherit_init(bfap, ftxH)) != EOK ) {
            goto _error;
        }
    }


    ftx_done_n(ftxH, FTA_FS_CREATE_ROOT_FILE);

    *outbfap = bfap;
    ms_free(dir_stats);

    return sts;

_error:
    (void)bs_close(bfap, MSFS_CLOSE_NONE);
    ftx_fail(ftxH);
    ms_free(dir_stats);
    return sts;
}



int
fs_init_ftx (void)

/*
 * register the agents for the fs layer
 */

{
    statusT retval;

    retval = ftx_register_agent(
                                FTA_FS_UPDATE_V1,
                                NIL,
                                NIL
                                );
    if (retval != EOK) {
        ADVFS_SAD1("fs_init_ftx: ftx_register_agent(1) error", retval);
    }
    retval = ftx_register_agent(
                                FTA_FS_INSERT_V1,
                                fs_insert_undo,
                                NIL
                                );
    if (retval != EOK) {
        ADVFS_SAD1("fs_init_ftx: ftx_register_agent(2) error", retval);
    }
    return (0);
}


/*
 * This function assumes that the fsContext is valid and locked, or that 
 * the domain is not activated.
 */
void
fs_insert_undo(
               ftxHT ftxH,
               int opRecSz,
               void *opRec
               )
{
    statusT sts;
    fs_dir_entry *dir_p;
    bfAccessT *dir_accessp;
    struct fsContext *context_ptr;
    char *dir_buffer;
    insert_undo_rec in_rec;
    bfSetT *bfSetp=NULL;
    domainT *dmnP;
    rbfPgRefHT page_ref;
    bfTagT tag;
    uint32_t glom_flags;
    uint32_t entry_size;
    struct vnode *nullvp = NULL;
    

    /* The following is to allow backward compatablility. Since a
     * field was added to the end of the undo record, old versions of
     * the kernel will not be effected since the size was never
     * verified by this routine.  The old kernels will just not know
     * of the additional field. For a new kernel running recovery on
     * an old domain, the field will be initialized to a benign value
     * (zero) and then the passed in stucture will be bcopied in, only
     * overwritting the addtional field if it exists.
     * For future compatabilty the min of the passed in size and the
     * size of the expected strucutre will be bcopied.
     */

    in_rec.undo_header.old_size = 0;

    bcopy(opRec,&in_rec,MIN(opRecSz,sizeof(insert_undo_rec)));

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(in_rec.undo_header.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(in_rec.undo_header.bfSetId.domainId,
                         dmnP->dualMountId)) ) {
            in_rec.undo_header.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("fs_insert_undo: domainId mismatch");
        }
    }

    sts = bfs_open( &bfSetp, in_rec.undo_header.bfSetId,
                        BFS_OP_IGNORE_DEL, ftxH );

    if (sts != EOK) {
      domain_panic(ftxH.dmnP,"bfs_open error in fs_insert_undo");
      RAISE_EXCEPTION(sts);
    }

    sts = bs_access(
                    &dir_accessp,
                    in_rec.undo_header.dir_tag,
                    bfSetp,
                    ftxH,
                    BF_OP_INTERNAL|BF_OP_OVERRIDE_SMAX,
                    &nullvp
                    );
    if (sts != EOK) {
      domain_panic(ftxH.dmnP,"bs_access error in fs_insert_undo");
      RAISE_EXCEPTION(sts);
    }

    ADVFS_ACCESS_LOGIT( dir_accessp, "fs_insert_undo called" );

    /*
     * if the dir has an initialized context area, then make sure
     * it's locked and up its dir stamp.  
     */
    context_ptr = VTOC ( &dir_accessp->bfVnode );
    MS_SMP_ASSERT((dmnP->state != BFD_ACTIVATED) ||
                  (ADVFS_SMP_RW_LOCK_NEQL(&context_ptr->file_lock, RWL_UNLOCKED)));

    sts = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_buffer,
                    dir_accessp,
                    in_rec.undo_header.page,
                    BS_NIL,
                    ftxH,
                    MF_NO_VERIFY 
                    );
    if (sts != EOK) {
        ADVFS_SAD1("rbf_pinpg error in fs_insert_undo", sts);
    }
    dir_p = (fs_dir_entry *)(dir_buffer+in_rec.undo_header.byte);

    GETTAG(tag,dir_p);

    if (!BS_BFTAG_EQL(tag, in_rec.undo_header.ins_tag)) {

/*        printf("fs_undo_insert - tag %d not in place in dir %d\n",
               BS_BFTAG_IDX(in_rec.undo_header.ins_tag),
               BS_BFTAG_IDX(in_rec.undo_header.dir_tag));
*/
        MS_SMP_ASSERT(FALSE);

        goto finish;
    }

    /* Since this is an undo routine we can not call the
     * index routines because they start ftx's. Indicate to
     * the glom routine to not try to recover lost space if found
     */

    glom_flags = SKIP_LOST_SPACE_GLOM;

    sts = setup_for_glom_dir_entries(dir_buffer,
                                     &dir_p,
                                     &entry_size,
                                     &glom_flags,
                                     dir_accessp
                                     );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_bs_tag_num,
                   sizeof(dir_p->fs_dir_bs_tag_num)
                   );
    dir_p->fs_dir_bs_tag_num = 0;

    /* Need this for indexing to work, but also should be here
     * for non-indexing, otherwise dir space could be lost.
     */

    if (glom_flags & SPACE_NEEDS_GLOMMING)
    {
        glom_dir_entries(dir_buffer,
                         dir_p,
                         entry_size,
                         glom_flags,
                         page_ref,
                         dir_accessp->bfPageSz * ADVFS_FOB_SZ
                        );
    }

    context_ptr->dirstamp++;

finish:

    /*
     * We have no way of knowing if storage was added and the undoing
     * of the storage will done after the undoing of the insertion 
     * thus we need to pass in an indicator. 
     *
     * If storage was added during this transcation (old_size > 0) then
     * we must reflect that the storage is being backed out. The passed
     * in size must be reduced by a page and stored into the access
     * structure.
     */

    if ( in_rec.undo_header.old_size > 0 )
    {
        /*
         * We should only be decrementing file_size by one page.
         */
        MS_SMP_ASSERT(((dir_accessp->bfaNextFob * ADVFS_FOB_SZ) - 
		       in_rec.undo_header.old_size) == 
                      (dir_accessp->bfPageSz * ADVFS_FOB_SZ));
        dir_accessp->file_size = in_rec.undo_header.old_size;
    }
    else
    {
        /* Directory truncation can also be failed at insertion time
         * (under rare conditions during a rename and low space and indexes)
         * This fortunately we can detect by examining the alloc page count
         * since the remove_stg_undo has already run (it is the last thing
         * we do in insert_seq and therefor the first thing to be undone.
         */

        /*
         * Recalulate the file size in case we are failing a 
         * directory truncation and cleanup any left over truncation
         * mess
         */
        if (dir_accessp->dirTruncp) {
            dtinfoT *dtinfop = (dtinfoT *)dir_accessp->dirTruncp;
            dir_accessp->dirTruncp = (void *)NIL;
            ms_free(dtinfop);
            dir_accessp->file_size = dir_accessp->bfaNextFob * ADVFS_FOB_SZ;
        }
    }

    context_ptr->dirty_stats = TRUE;
        
    /*
     * update the dirs stats
     */

    sts = fs_update_stats(ATOV(dir_accessp), dir_accessp, ftxH, 0);
    if (sts != EOK) {
        ADVFS_SAD1("update_stats error in fs_insert_undo", sts);
    }

    sts = bs_close(dir_accessp, MSFS_CLOSE_NONE);
    if (sts != EOK) {
        domain_panic(ftxH.dmnP,"bs_close error in fs_insert_undo");
        RAISE_EXCEPTION(sts);
    }
HANDLE_EXCEPTION:
    if (bfSetp) {
        sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"bs_bfs_close error in fs_insert_undo");
        }
    }
    return;
}





