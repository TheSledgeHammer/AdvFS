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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      advfs_lookup. lookup an entry in a directory and return a vnode
 *      for the entry it found.
 */

#include <limits.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/vnode.h>
#include <sys/mount.h>
#include <sys/errno.h>
#include <sys/kern_svcs.h>
#include <fs/vfs_ifaces.h>            /* struct nameidata,  NAMEIDATA macro */
#include <sys/kthread_sd.h>
#include <sys/dnlc.h>
#include <fs_dir_routines.h>
#include <ms_public.h>
#include <fs_dir.h>
#include <ms_privates.h>
#include <ms_osf.h>


#define ADVFS_MODULE ADVFS_LOOKUP

#define isspace(x) (((x) == ' '  || (x) == '\t' || \
                     (x) == '\r' || (x) == '\n' || (x) == '\f') ? 1 : 0)
#define isdigit(x) (((x) >= '0' && (x) <= '9') ? 1 : 0)
#define islower(x) (((x) >= 'a' && (x) <= 'z') ? 1 : 0)
#define isupper(x) (((x) >= 'A' && (x) <= 'Z') ? 1 : 0)
#define isalnum(x) ((isdigit(x) || islower(x) || isupper(x)) ? 1 : 0)
#define tolower(x) ((isupper(x)) ? (x) + 'a' - 'A' : (x))
#define DIGIT(x) (isdigit((int)x)? ((x)-'0'): (10+tolower((int)x)-'a'))
#define MBASE 36



/*
 * advfs_lookup
 *
 * ENTRY CONDITION: nm_dvp has a v_count on it from the caller.
 * advfs_lookup should not alter this v_count.  
 *
 * EXIT CONDITION: if found, the vpp returned should have a v_count on it.
 * If the vnode was also put in the dnlc, it should have a v_count and a
 * v_scount (assuming it is still in there when this function returns).  If
 * we are doing a lookup on '.' then vpp should have it's v_count bumped
 * (which is in contradiction to not bumping the v_count of nm_dvp).
 */
int
advfs_lookup( 
    struct vnode  *nm_dvp,     /* in  - vnode of the directory we need to
                                *       search in order to find nm.  See
                                *       access_dvp for the relationship
                                *       between nm_dvp and access_dvp        */
    char          *nm,         /* in  - file name to be found in nm_dvp      */
    struct vnode **vpp,        /* out - vnode of found file                  */
    struct ucred  *cred,       /* in  - credentials                          */
    struct vnode  *access_dvp) /* in  - access check vnode.
                                *       access_dvp and nm_dvp
                                *       are generally equal.  They are _NOT_
                                *       equal _IF AND ONLY IF_ nm is ".."
                                *       and the directory is the mount
                                *       point.  When this is true, nm_dvp is
                                *       the mounted-on vnode
                                *       (vfs_vnodecovered) which has the
                                *       desired ".." and access_dvp contains
                                *       the access permissions for the mount
                                *       point.
                                */
{
    int                 error;
    struct nameidata   *ndp;
    advfs_lookup_t      flags = ALI_NO_FLAGS;

    ADVFS_VOP_LOCKSAFE;

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);

    error = advfs_lookup_int( flags, ndp, nm_dvp, nm, cred, access_dvp );

    *vpp = ndp->ni_vp;

#define	ISVDEV(t) \
        (((t) == VBLK) || ((t) == VCHR) || ((t) == VFIFO) )
    if ((*vpp != NULL) && ISVDEV((*vpp)->v_type)) {
#undef ISVDEV
        struct vnode *newvp;

        newvp = specvp(*vpp, (*vpp)->v_rdev); /* LU */
        VN_RELE(*vpp);
        (*vpp) = newvp;
    }
#ifdef ADVFS_ACCESS_TRACE
    if (*vpp && ((*vpp)->v_fstype == VADVFS) ) {
        ADVFS_ACCESS_LOGIT(VTOA(*vpp), "advfs_lookup: returning");
    }
#endif

    ADVFS_VOP_LOCKUNSAFE;
    return(BSERRMAP(error));
}

/*
 * advfs_lookup_int()
 */
int
advfs_lookup_int( 
    advfs_lookup_t    flags,      /* in     - Internal AdvFS flags            */
    struct nameidata *ndp,        /* in/out - nameidata used to optimize
                                   *          some operations.
                                   *        - ndp->ni_vp will be the vnode
                                   *          of the found file and returned
                                   *          with a VN_HOLD, or NULL will
                                   *          be returned if the file is not
                                   *          found.                          */
    struct vnode     *nm_dvp,     /* in     - nm_dvp is the vnode of the
                                   *          directory we need to search in
                                   *          order to find nm.  See access_dvp for the
                                   *          relationship between nm_dvp and
                                   *          access_dvp.
                                   *        - ni_dvp must be VN_HOLD.            */
    char             *nm,         /* in     - file name to be found in nm_dvp */
    struct ucred     *cred,       /* in     - credentials                     */
    struct vnode     *access_dvp) /* in     - access check vnode.
                                   *          access_dvp and nm_dvp are
                                   *          generally equal.  They are
                                   *          _NOT_ equal _IF AND ONLY IF_
                                   *          nm is ".." and the directory
                                   *          is the mount point.  When this
                                   *          is true, nm_dvp is the
                                   *          mounted-on vnode
                                   *          (vfs_vnodecovered) which has
                                   *          the desired ".." and
                                   *          access_dvp contains the access
                                   *          permissions for the mount
                                   *          point.  This is all done by
                                   *          the VFS layer when it calls
                                   *          VOP_LOOKUP().
                                   *        - access_dvp must be VN_HOLD.     */
{
    int                 error;
    statusT             ret;
    struct fsContext   *dir_context;
    struct fsContext   *file_context;
    int                 pin;
    bfTagT              found_bs_tag;
    bfTagT              bf_tag;
    fs_dir_entry       *found_buffer;
    rbfPgRefHT          found_page_ref;
    struct vnode       *found_vp        = NULL;
    struct vnode       *nvp             = NULL;
    struct fileSetNode *fsnp;
    char               *p               = NULL;
    struct bfNode      *bnp;
    struct bfNode      *new_bnp;
    ni_nameiop_t        flag;
    int                 bf_flag         = DONT_UNLOCK;
    int                 set_nocross     = 0;
    int                 dot_lookup      = FALSE;

    ADVFS_ACCESS_LOGIT(VTOA(nm_dvp), "advfs_lookup_int dir vnode entry");

    lock_t *sv_lock;
    ndp->ni_vp = NULL;
    ndp->ni_nameiop |= NI_BADDIRSTAMP;
    fsnp = VTOFSN(nm_dvp);
    fsnp->fileSetStats.advfs_lookup++;

    /* get the operation flag (lookup, create, rename, delete)*/
    flag = ndp->ni_nameiop & NI_OPFLAG;

    /*
     * screen domain panic
     */
    if (VTOA(nm_dvp)->dmnP->dmn_panic) {
        return EIO;
    }

    /*
     * Make sure it is a directory
     */
    bnp = VTOBNP(nm_dvp);
    dir_context = bnp->fsContextp;

    if (dir_context == NIL) {
        ADVFS_SAD0("Error getting dir_context_ptr in advfs_lookup");
    }

    if ((dir_context->dir_stats.st_mode & S_IFMT) != S_IFDIR) {
        return (ENOTDIR);
    }
    MS_SMP_ASSERT(nm_dvp->v_type == VDIR);

    /*
     * Check accessibility of directory (for exec).  access_dvp is used
     * because if this a ".." lookup _AND_ if the directory is VROOT (top
     * directory of this mounted file system) then access_dvp will contain
     * the vnode of the top directory and nm_dvp will contain the mounted-on
     * directory (access_dvp->v_vfsp->vfs_vnodecovered).  In this case we
     * would need to get the ".." directory from the covered vnode, but we
     * most definitely do not want to use it for the security check.  The
     * decision to provide a different access_dvp is up to the caller
     * (generally lookuppnvp() is the only one that plays this game;
     * everyone else is generally not asking to lookup "..").
     */
    if ( cred == NULL )
        cred = TNC_CRED();
    if (error = VOP_ACCESS(access_dvp, S_IEXEC, cred)) {
       goto _exit;
    }

    /*
     * This is an area where I am concerned about
     * nameidata getting properly setup by _ALL_
     * potential callers to VOP_LOOKUP().  What if
     * a 3rd party file system calls VOP_LOOKUP()
     * with out setting up the ndp->ni_nameiop
     * values correctly?  Would we use an old
     * value?
     *
     * For this specific check, I guess we could
     * turn the tables and store a state flag in
     * nameidata to say that this file was access
     * via the ./.tags/ directory and as such, we
     * should not allow create, delete, or rename
     * operations.
     *
     * However, that would mean making the
     * nameidata structure even more AdvFS specific
     * or at least having non-generic information
     * stored in it.
     */

    /*
     * Check for special by-tag lookup in ./.tags/ dir. allow lookups only
     */
    if (BS_BFTAG_EQL(bnp->tag, fsnp->tagsTag)) {
        if (flag != NI_LOOKUP) {
            error = EACCES;
            goto _exit;
        }

        /*
         * See if this is a lookup for dot (ourselves, which we have just
         * established is the ./.tags/ directory
         */
        if ( nm[0] == '.' && nm[1] == '\0' ) {
            bf_tag = bnp->tag;
        } 
        else {
            /*
             * See if we are looking for dot dot (our parent), which because
             * this is the ./.tags/ directory will be the root directory of
             * this fileset.
             */
            if ( nm[0] == '.' && nm[1] == '.' && nm[2] == '\0' ) {
                bf_tag = fsnp->rootTag;
            }
            else {
                char *name = nm;
                set_nocross = 1; /* set NI_NOCROSSMOUNT if this entry is a dir*/
                if (name[0] == 'M') {
                    /* we are opening a metadata bitfile via ./.tags/ */ 
                    bf_flag |= META_OPEN;
                    name++;
                }

                BS_BFTAG_IDX(bf_tag) = strtoul( name, &p, 0 );

                /*
                 *  Expected format of last component of pathname at this
                 *  point is "tag" or "tag.seq".  These values are normally
                 *  passed in as hex, i.e., 0x33.0x11, although octal
                 *  (063.021) or decimal (51.17) are also acceptable.
                 */
                if(*p == '.') {
                    ++p;
                    BS_BFTAG_SEQ(bf_tag) = strtoul( p, &p, 0 );
                    if (*p != '\0') {
                        /*
                         * The seq value must be a number (decimal (51), hex
                         * (0x33), or octal(021)).  It can not have anything
                         * following it.
                         */
                        error = ENOENT;
                        goto _exit;
                    }
                } 
                else if (*p == '\0') {
                    /*
                     * The caller wants whatever file currently uses this
                     * tag.  Since there is no sequence number, it is
                     * possible that the file could have been deleted, and
                     * the tag reused and we would still return something,
                     * but it might not be the exact same file that was
                     * originally requested.  Having a seq number insures
                     * the caller is told if the file got deleted since a
                     * given tag.seq pair will not be reused.
                     */
                    BS_BFTAG_SEQ(bf_tag) = 0;
                }
                else {
                    /*
                     * The name can be either a numeric tag or tag.seq.  If
                     * tag is not all by itself, or followed by a dot, then
                     * it is an invalid tag number.
                     */
                    error = ENOENT;
                    goto _exit;
                }

                if (BS_BFTAG_RSVD(bf_tag)) {
                    /* we are opening a metadata bitfile via ./.tags/ */ 
                    bf_flag |= META_OPEN;
                }

                if (BS_BFTAG_SEQ(bf_tag) == 0) {
                    bfSetT *bfSetp = ADVGETBFSETP(nm_dvp->v_vfsp);

                    if (bf_flag & META_OPEN) {
                       bfSetp = bfSetp->dmnP->bfSetDirp;
                    }

                    {
                        bfMCIdT bfMCId; /* Dummy params for tagdir lookup */

                        if (tagdir_lookup( bfSetp, 
                                           &bf_tag,
                                           &bfMCId) != EOK) {

                            error = ENOENT;
                            goto _exit;
                        }
                    }
                }
            }
        }

        /*
         * For META_OPEN, use dir_context lock to avoid racing. Otherwise
         * don't lock dir_context lest the caller tries to access
         * .tags/3 and bf_get would try to lock fsContext(=dir_context)
         * and would cause panic since it is already locked here!
         *
         * NOTE: We know that we are currently working in the ./.tags/
         *       directory so our nm_dvp and access_dvp are going to be the
         *       same, thus it doesn't matter whether dir_context was
         *       obtained via nm_dvp or access_dvp.
         *       Since this is a .tags lookup, we won't pass a name so
         *       bf_get won't try to put it in the DNLC.
         */
        if (bf_flag & META_OPEN) {
            ADVRWL_FILECONTEXT_WRITE_LOCK( dir_context);
        }
        error = bf_get(bf_tag, 0, bf_flag, fsnp, &found_vp, NULL, NULL);
        if (bf_flag & META_OPEN) {
            ADVRWL_FILECONTEXT_UNLOCK(dir_context);
        }
        if (error) {
            error = ENOENT;
            goto _exit;
        } else {
            /*
             * Lookup in ./.tags/ for a reserved file is done.  (In this
             * context, a reserved file is any file whose bitfile set is the
             * root tag directory.  This includes the log, the BMTs, the
             * SBMs and the tag directories.)
             *
             * The vnode found is on the same mount point with the ./.tags/
             * directory.  The access structure associated with the vnode is
             * a shadow to the real_bfap of the bitfile in the root tag
             * directory.  The real_bfap recorded in the shadow access
             * structure is used for I/O.  
             *
             * The hidden vnode of the reserved file, if exists, is on the
             * dead_mount and is never returned here.
             */
            ndp->ni_vp = found_vp;
            MS_SMP_ASSERT(ADVGETFILESETNODE(found_vp->v_vfsp) ==
			  ADVGETFILESETNODE(nm_dvp->v_vfsp));

            if (found_vp->v_count == 0) {
                ADVFS_SAD1("bf_get(1) returned bad vp N1 = tag", bf_tag.tag_num);
            }

            if (set_nocross && found_vp->v_type == VDIR) {
           /*
            * Set NI_NOCROSSMOUNT if current target is a dir entry in
            * ./.tags/ else don't because we need to allow namei to
            * resolve the path while traversing 'up' to '\' in order to
            * honor /bin/pwd, but we don't want to allow file or dir
            * references through the .tags dir (ie ./.tags/<seq>/...)
            * where: <seq> is the tag seq# of another dir in the current
            * advfs fileset which may be mounted on and may be a
            * different filesystem type.
            */
                ndp->ni_nameiop |= NI_NOCROSSMOUNT;
            }

            error = 0;
            goto _exit;
        }
    }   /* end of ./.tags/tag.seq special AdvFS processing */

    /*
     * Check the cache here.  The vnode returned will have a VN_SOFTHOLD()
     * on it.  Don't bother putting "." into the cache or looking it up.
     * We pass NOCRED because the entry was entered with NOCRED.
     */

    /*
     * dnlc_lookup will put an additional v_scount (VN_SOFTHOLD) on the
     * vnode.  The soft count is a dlnc-ism.  The original dnlc_enter put a
     * v_scount on the vnode that will keep the vnode in existance until it
     * is removed from the dnlc.  At this point, we probalby have 2 v_scount
     * references (at least) one for the lookup and one for the original
     * enter.  There could be a race that purges the vnode from the dnlc
     * just after the lookup, in which case v_scount could drop back to 1.
     * Regardless, this dnlc_lookup should not be sufficient to keep the
     * vnode in the dnlc cache, therefore, we will want to do a VN_SOFTRELE
     * of the vnode before returning if we actually find it in the cache.
     * If we don't find it in the cahce, we don't want to do any VN_RELE or
     * VN_SOFTRELE.  Either case, we don't want to touch the v_scount until
     * after we have called bs_access since bs_access will bump the regular
     * v_count and we want to make sure to always have one or the other up
     * to prevent the dealloc/recycle access code from running.
     */
    if ( nm[0] == '.' && nm[1] == '\0' ) {
        dot_lookup = TRUE;
        nvp = nm_dvp;
    } else {
        if (!(flags & ALI_NO_DNLC)) {
            
            /* 
             * ALI_NO_DNLC is passed by advfs VOPs when this is the last 
             * file in the path _AND_ we intend to delete the file.  The 
             * delete operation needs to have ni_offset and ni_count 
             * properly setup by seq_search() so we do not want to get
             * the value from the DNLC.
             */

            nvp = dnlc_lookup(nm_dvp, nm, NOCRED);
        }
    }

    if ( nvp != NULL ) {
        int vpid = 0;
        
        ADVFS_ACCESS_LOGIT(VTOA(nvp), "advfs_lookup_int: dnlc_lookup success ");
        /*
         * We found the vnode in the dnlc name cache.  We MUST VN_SOFTRELE
         * this vound before we return, but not until after we have setup
         * the access structure and put a v_count on it (bf_get -> bs_access).
         */
        ndp->ni_vp = nvp;

        bnp = VTOBNP(nvp);
        bf_tag = bnp->tag;
        error = bf_get(bf_tag,
                       dir_context,
                       bf_flag,
                       fsnp,
                       &found_vp,
                       NULL,
                       NULL);

        /* If we raced a delete(error == ENO_SUCH_TAG). We got to the
         * dnlc before the vnode was purged.  The vnode is now gone
         * and we no longer have a softhold. Just treat this like we
         * didn't find it in the dnlc
         */
        if ((error != ENO_SUCH_TAG) && (!dot_lookup)) {
            if (error != EOK) {
                ADVFS_ACCESS_LOGIT( VTOA(nvp), "advfs_lookup_int: bf_get failed");
            }
            /*
             * If we found the vnode, drop the v_scount from the lookup (leave
             * the v_scount from the dlnc reference if there was one before
             */
            VN_SOFTRELE(nvp);
            MS_SMP_ASSERT( nvp->v_scount >= 0 );
        }
        if ( !error ) {
            MS_SMP_ASSERT( found_vp == nvp );
            
            fsnp->fileSetStats.lookup.hit++;

            if (found_vp->v_count == 0) {
                ADVFS_SAD1("bf_get(2) returned bad vp N1 = tag", bf_tag.tag_num);
            }
            error = 0;
            ADVFS_ACCESS_LOGIT(VTOA(nvp), "advfs_lookup_int: bf_get success ");

            goto _exit;
        } else {
            ADVFS_ACCESS_LOGIT( VTOA(nvp), "advfs_lookup_int: bf_get failed 2");

            ndp->ni_vp = NULL;
        }
    }

    /*
     * We didn't find the vnode in the dnlc so there are no references on
     * the vnode.  We must search the directory for it and set it up.  We
     * still need to put on a v_count, but not necessarily a v_scount.  If
     * the file is put into the dnlc, a v_scount will be put on by
     * dnlc_enter.
     */

    bf_flag = 0;
    fsnp->fileSetStats.lookup.miss++;

    pin = REF_PAGE;

    /*
     * call seq_search to search the directory for the entry.
     * get the name to search for from nameidata.
     */
    if (flag == NI_LOOKUP) {
        pin |= USE_OFFSET;
    }

    ADVRWL_FILECONTEXT_READ_LOCK(dir_context);

    ndp->ni_dirstamp = dir_context->dirstamp;
    ndp->ni_nameiop &= ~NI_BADDIRSTAMP ;

    ret = seq_search(
                     nm_dvp,
                     ndp,
                     nm,
                     &found_bs_tag,
                     &found_buffer,
                     &found_page_ref,
                     pin,
                     FtxNilFtxH
                     );

    /*
     * if EIO, bail out
     */
    if (ret == EIO) {
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
        ndp->ni_vp = NULL;
        error = ret;
        goto _exit;
    }
    /*
     * check to see if the file existed in the directory. A return of
     * I_INSERT_HERE means the entry did not exist.
     */

    if (ret == I_INSERT_HERE) {

        ndp->ni_vp = NULL;

        /* Save # pages in use for possibly truncating the directory */
        VTOC(nm_dvp)->fsc_last_dir_fob = VTOC(nm_dvp)->fsc_last_dir_fob +
                                         VTOA(nm_dvp)->bfPageSz;

        /*
         * File not found, return ENOENT
         */
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
        error = ENOENT;
        goto _exit;
    }
    /*
     * The file was found in the directory (from the original call to
     * seq_search)
     */

    if (ret != I_FILE_EXISTS) {
        ADVFS_SAD1("Unknown return from seq_search", ret);
    }


    /*
     * Pick up the dirstamp while the directory is still locked.
     */

    ndp->ni_dirstamp = dir_context->dirstamp;
    ndp->ni_nameiop &= ~NI_BADDIRSTAMP ;

    /*
     * Do a bf_get on the file.  Note that this releases the shared lock on
     * the directory (ADVRWL_FILECONTEXT_UNLOCK(dir_context)) to avoid potential
     * deadlock initializing the context area of the file. We need to pass
     * the name down to bf_get so we can insert the vnode into the dnlc.
     */
    error = bf_get(found_bs_tag,
                   dir_context,
                   bf_flag, 
                   fsnp,
                   &found_vp,
                   (flag == NI_DELETE && (ndp->ni_nameiop & NI_END_OF_PATH)) ?
                   NULL : nm_dvp,
                   nm);
    if ( error ) {
#ifdef ADVFS_DEBUG
        if ( error == ENO_SUCH_TAG ) {
            printf("error from bf_get in lookup\n");
        }
#endif
        ndp->ni_vp = NULL;
        goto _exit;
    }
    if (found_vp->v_count == 0) {
        ADVFS_SAD1("bf_get(3) returned bad vp N1 = tag", found_bs_tag.tag_num);
    }

    ADVFS_ACCESS_LOGIT(VTOA(found_vp), 
            "advfs_lookup_int: seq_search bf_get success ");

    ndp->ni_vp = found_vp;

    error = 0;

_exit:

#ifdef ADVFS_ACCESS_TRACE
    if (ndp->ni_vp) {
        ADVFS_ACCESS_LOGIT(VTOA(ndp->ni_vp), "advfs_lookup_int: returning");
    }
#endif

    return(BSERRMAP(error));
}

/*
 * check_path_back
 *
 * Given a source and a target directory, check that the source is not
 * anywhere in the path of the target. Used by advfs_rename when mv'ing
 * a directory.
 */

check_path_back(
                struct vnode *source_vp,  /* in - vnode of the source directory */
                struct vnode *target_vp,  /* in - vnode of the target directory */
                struct ucred *cred        /* in - cred - credentials of target */
                )
{
    struct bfNode *sbnp, *tbnp;
    bfTagT root_tag, dir_tag;
    struct fsContext *t_context, *s_context;
    fileSetNodeT *fsnp;
    statusT ret;
    bfSetT *bfSetp;
    bfPageRefHT page_ref;
    bfAccessT *bfap;
    fs_dir_entry *dir_p;
    char *p;
    int first_time, n;
    int error = 0;
    int dont_unlock = FALSE;
    struct vnode *nullvp;


    sbnp = VTOBNP(source_vp);
    tbnp = VTOBNP(target_vp);
    t_context =  tbnp->fsContextp;
    s_context = sbnp->fsContextp;
    fsnp = ADVGETFILESETNODE(target_vp->v_vfsp);
    root_tag = fsnp->rootTag;

    /* check for target and source same dir */
    if (BS_BFTAG_EQL(sbnp->tag, tbnp->tag)) {
        error = EEXIST;
        goto out;
    }
    /* if we are moving to the root, we are done */
    if (BS_BFTAG_EQL(tbnp->tag, root_tag)) {
        goto out;
    }

    bfap = VTOA(target_vp);
    bfSetp = bfap->bfSetp;

    dir_tag = tbnp->tag;
    first_time = TRUE;

    for (;;) {
        /*
         * read dir's page 0
         */
        ret = bs_refpg(
                       &page_ref,
                       (void *)&dir_p,
                       bfap,
                       0,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (ret != EOK) {
            error = EIO;
            if (!first_time) {
                if (!dont_unlock) {
                    ADVRWL_FILECONTEXT_UNLOCK(t_context);
                }
                VN_RELE(&bfap->bfVnode);
            }
            goto out;
        }
        /*
         * find the ".." entry and get its's bf tag
         */

        n = (signed)dir_p->fs_dir_size;
        
        if ( FS_DIR_SIZE_OK(dir_p->fs_dir_size, 0) ) {
            /* there has been a minor data corruption,
             * inform and error out
             */
            fs_dir_size_notice(bfap, n, (long)0, 0);
            error = EIO;
            ret = bs_derefpg(
                             page_ref,
                             BS_RECYCLE_IT
                             );
            if (ret != EOK) {
                ADVFS_SAD1("derefpg error in check_path_back", ret);
            }
            if (!first_time) {
                if (!dont_unlock) {
                    ADVRWL_FILECONTEXT_UNLOCK(t_context);
                }
                VN_RELE(&bfap->bfVnode);
            }
            goto out;
        }
        p = (char *)dir_p;
        p += dir_p->fs_dir_size;
        dir_p = (fs_dir_entry *)p;
        GETTAG(dir_tag,dir_p);

        ret = bs_derefpg(
                         page_ref,
                         BS_RECYCLE_IT
                         );
        if (ret != EOK) {
            ADVFS_SAD1("derefpg error in check_path_back", ret);
        }
        /* unlock and close the current dir */
        if (first_time) {
            first_time = FALSE;
        } else {
            if (dont_unlock) {
                dont_unlock = FALSE;
            } else {
                ADVRWL_FILECONTEXT_UNLOCK(t_context);
            }
            VN_RELE(&bfap->bfVnode);
            if (ret != EOK) {
                error = EIO;
                break;
            }
        }

        /* do the check for which we came all this way */
        if (BS_BFTAG_EQL(dir_tag, sbnp->tag)) {
            error = EINVAL;
            break;
        }
        /* is the parent the root - if so done here */
        if (BS_BFTAG_EQL(dir_tag, root_tag)) {
            break;
        }
        /* not the root so access it's parent and loop again */
        nullvp = NULL;
        ret = bs_access(&bfap, dir_tag, bfSetp, FtxNilFtxH,
                        BF_OP_NO_FLAGS, &nullvp);
        if (ret != EOK) {
            error = EIO;
            break;
        }
        t_context = VTOC ( ATOV ( bfap ) );
        MS_SMP_ASSERT( t_context != NULL );

        /*
         * share lock the dir. make sure before we lock that the dir
         * is not the parent of the source (it will already be locked)
         */
        if (BS_BFTAG_EQL(s_context->dir_stats.dir_tag, dir_tag)) {
            dont_unlock = TRUE;
        } else {
            ADVRWL_FILECONTEXT_READ_LOCK(t_context);
        }
    }

out:
    return (error);
}

/*
 * new_parent
 *
 * When moving a directory to a new directory (meaning that it has a
 * new parent), update the source directory's ".." entry to reflect
 * the new parent directory. Used by rename.
 *
 */

new_parent(
           struct vnode *source_vp,  /* in - vnode of the source directory */
           ftxHT ftx_handle,         /* in - ftx handle */
           bfTagT new_parent_tag     /* in - tag of new parent directory */
           )
{
    struct bfNode *sbnp;
    struct fsContext *s_context;
    statusT ret;
    rbfPgRefHT page_ref;
    struct bfAccess  *bfap;
    fs_dir_entry *dir_p;
    char *p;
    tagSeqT *seqp;

    sbnp = VTOBNP(source_vp);
    s_context =  sbnp->fsContextp;
    bfap = VTOA(source_vp);

    /*
     * the source directory is already accessed, just pin page 0...
     */

    ret = rbf_pinpg(
                    &page_ref,
                    (void *)&dir_p,
                    bfap,
                    0,
                    BS_NIL,
                    ftx_handle,
                    MF_VERIFY_PAGE
                    );
    if (ret != EOK) {
        ADVFS_SAD1("unable to ref page 0 in new_parent error N1", ret);
    }
    /*
     * find the ".." entry and update its bf tag
     */
    if ( FS_DIR_SIZE_OK(dir_p->fs_dir_size, 0) ) {
        return(EIO);
    }
    p = (char *)dir_p;
    p += dir_p->fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    seqp = GETSEQP(dir_p);
    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_bs_tag_num,
                   sizeof(dir_p->fs_dir_bs_tag_num)
                   );
    dir_p->fs_dir_bs_tag_num = new_parent_tag.tag_num;

    rbf_pin_record(
                   page_ref,
                   seqp,
                   sizeof(*seqp)
                   );
    *seqp = new_parent_tag.tag_seq;

    ADVSMP_FILESTATS_LOCK(&s_context->fsContext_lock);
    s_context->fs_flag |= MOD_MTIME;
    s_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK(&s_context->fsContext_lock);

    return(0);
}
/* end advfs_lookup.c */
