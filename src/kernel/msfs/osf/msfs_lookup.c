/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 *      msfs_lookup. lookup an entry in a directory and return a vnode
 *      for the entry it found.
 *
 * Date:
 *
 *      Mon Aug 19 11:46:20 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: msfs_lookup.c,v $ $Revision: 1.1.118.2 $ (DEC) $Date: 2008/02/12 13:07:11 $"

#include <sys/param.h>
#include <sys/time.h>
#include <sys/vnode.h>
#include <sys/mount.h>
#include <sys/namei.h>
#include <msfs/fs_dir_routines.h>
#include <msfs/ms_public.h>
#include <sys/mode.h>
#include <sys/errno.h>
#include <msfs/fs_dir.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>

#define ADVFS_MODULE MSFS_LOOKUP

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
 * NAME: strtoul
 *
 * FUNCTION: Converts a string to an integer
 *
 * RETURNS: returns an unsigned long integer formed from *nptr
 *          returns 0 if an integer could not be formed
 *          ULONG_MAX if overflow occurs on conversion
 *
 * NOTE: this came from the standard c-library since this
 *       routine (strtoul) doesn't exist in the kernel (until now!).
 */

static unsigned long int
strtoul(const char *nptr, char * *endptr, int base)
{
    unsigned long val;
    int xx;
    int signchar = 0;      /* true if a "+" precedes the number */
    int digits = 0;        /* number of valid digits read */
    unsigned long maxval;
    char *ptr = (char *) nptr;

    val = 0L;
    if (base >= 0 || base <= MBASE) {
        while(isspace(*ptr))
            ++ptr;

        if (*ptr == '+')
            ++ptr;
        else if (*ptr == '-'){
            ++ptr;
            signchar = 1;
        }

        if (base == 0) {
            if (*ptr == '0') {
                ++ptr;
                if (*ptr == 'x' || *ptr == 'X') {
                    ++ptr;
                    base = 16;
                } else {
                    ++nptr;
                    base = 8;
                }
            } else
                base = 10;

        } else if (base == 16)
            if (ptr[0] == '0' && (ptr[1] == 'x' || ptr[1] == 'X'))
                ptr += 2;
        /*
         * for any base > 10, the digits incrementally following
         *      9 are assumed to be "abc...z" or "ABC...Z"
         */

        /* Check for overflow.  If the current value (before
         * adding current xx digit) is already greater than
         * ULONG_MAX / base, we know that another digit will
         * not fit.  Also if after the current digit is added,
         * if the new value is less than the old value, we
         * know that overflow will occur.
         */

        maxval = ULONG_MAX / (unsigned long)base;
        while(isalnum((int)*ptr) && (xx=DIGIT(*ptr)) < base) {
            if ((val > maxval) || (base*val + xx < val)) {
                val = ULONG_MAX;
                break;
            }
            val = base*val + xx;
            ++ptr;
            ++digits;
        }
    }

    if (signchar == 1)
        val = -val;

    if (endptr != 0)
        if ((digits == 0) && (signchar == 0))
            *endptr = (char *) nptr;
        else if (digits > 0)
            *endptr = (char *) ptr;

    return(val);
}


/*
 * msfs_lookup
 *
 * Convert a component of a filename path to a vnode
 * If a directory page lookup is performed, return either the
 * placement of the found entry, or the place to insert the entry,
 * in the nameidata structure (ndp->).
 *   ndp->ni_count = page number
 *   ndp->ni_offset = byte offset
 *   ndp->ni_resid = flag that says new space or re-used space
 *
 * if the call to msfs_lookup results in a directory search, it is
 * performed under shared directory lock. To ensure that between the
 * search and the subsequent update of the namei cache the directory
 * hasn't changed, the code performs the following:
 *
 * if the search resulted in file_not_found:
 *            if the call to msfs_lookup was a create, then return (ENOENT).
 *            anything else -
 *                  release the shared dir lock.
 *                  take the dir excl lock.
 *                  check the ndp and dir context timestamps.
 *                          dir hasn't changed -
 *                              create negative cache entry.
 *                              release dir lock.
 *                              return (ENOENT).
 *                          dir has changed -
 *                              perform another search.
 *                              file still not found -
 *                                  create negative cache entry.
 *                                  release dir lock.
 *                                  return (ENOENT).
 *                              file is now found -
 *                                  goto 'found_it'.
 *
 * if the search resulted in file_exists:
 *
 *            release shared dir lock.
 *            take the excl lock.
 *            check the dirstamps.
 *                  the dir hasn't changed -
 *                     found_it:
 *                        bf_get the file.
 *                        if not delete insert positive cache entry.
 *                        return success.
 *                  the dir has changed -
 *                        the page where the entry was found is still ref'ed.
 *                        the entry is still there -
 *                            goto 'found_it".
 *                        the entry is no longer there -
 *                            if creat or rename, just return ENOENT.
 *                            otherwise create a negative cache entry
 *                            and return ENOENT.
 */

msfs_lookup(
            struct vnode *vp,  /* in - vnode of the directory in which
                                  to look */
            register struct nameidata *ndp /* in - namei data -
                                              contains the file name
                                              to lookup */
            )
{
    int error;
    bfAccessT *dir_accessp;
    struct fsContext *dir_context, *file_context;
    int pin;
    bfTagT found_bs_tag, bf_tag;
    fs_dir_entry *found_buffer;
    rbfPgRefHT found_page_ref;
    statusT ret;
    struct vnode *found_vp = NULL, *nvp = NULL;
    struct mount *mp;
    struct fileSetNode *fsn;
    char *p = NULL;
    struct bfNode *bnp, *new_bnp;
    int flag, bf_flag = DONT_UNLOCK;
    int set_nocross = 0;

    ndp->ni_dvp = vp;
    ndp->ni_vp = NULL;
    mp = VTOMOUNT(vp);
    fsn =  GETFILESETNODE(mp);
    fsn->fileSetStats.msfs_lookup++;

    /* get the operation flag (lookup, create, rename, delete)*/
    flag = ndp->ni_nameiop & OPFLAG;

    /*
     * Make sure it is a directory
     */
    dir_accessp = VTOA(vp);
    bnp = (struct bfNode *)&vp->v_data[0];
    dir_context = bnp->fsContextp;

    /*
     * screen domain panic
    if (dir_accessp->dmnP->dmn_panic) {
        return EIO;
    }
     */

    if (dir_context == NIL) {
        ADVFS_SAD0("Error getting dir_context_ptr in msfs_lookup");
    }

    if ((dir_context->dir_stats.st_mode & S_IFMT) !=
        S_IFDIR) {
        return (ENOTDIR);
    }


    /*
     * Check accessibility of directory (for exec). If processing a
     * '..' and the '..' spans a mount point, don't check the mode
     * bits for exec access if NOCROSSMOUNT false.
     */

    if ((ndp->ni_nameiop & SKIPACCESS) ||
            ((ndp->ni_isdotdot && (vp->v_mountedhere != NULLMOUNT) &&
            !(ndp->ni_nameiop & NOCROSSMOUNT)))) {
        /* no access check */
    } else {
        if (error = msfs_access(vp, S_IEXEC, ndp->ni_cred)) {
            goto _exit;
        }
    }
    /*
     * check for special by-tag lookup in .tags dir. allow
     * lookups only
     */

    if (BS_BFTAG_EQL(bnp->tag, fsn->tagsTag)) {
        if (flag != LOOKUP) {
            error = EACCES;
            goto _exit;
        }
        if ((ndp->ni_namelen == 1 && ndp->ni_dent.d_name[0] == '.')) {
            bf_tag = bnp->tag;
        } else {
            if ((ndp->ni_namelen == 2 && ndp->ni_dent.d_name[0] ==
                 '.' && ndp->ni_dent.d_name[1] == '.')) {
                bf_tag = fsn->rootTag;
            } else {
                char *name = ndp->ni_dent.d_name;
                set_nocross = 1;  /* set NOCROSSMOUNT if this entry is a dir */

                if (name[0] == 'M') {
                    /* we need to open a metadata bitfile */
                    bf_flag |= META_OPEN;
                    name++;
                }

                BS_BFTAG_IDX(bf_tag) = strtoul( name, &p, 0 );
                if (p == NULL) {
                    error = ENOENT;
                    goto _exit;
                }

                /*
                 *  Expected format of last component of pathname at this
                 *  point is tag.seq.  These values are normally passed in
                 *  as hex, i.e., 0x3.0x1, although octal or decimal are also
                 *  acceptable.
                 */
                if (*p == '.') {
                    ++p;
                    BS_BFTAG_SEQ(bf_tag) = strtoul( p, &p, 0 );
                } else {
                    BS_BFTAG_SEQ(bf_tag) = 0;
                }

                if (BS_BFTAG_RSVD(bf_tag)) {
                    /* we need to open a metadata bitfile */
                    bf_flag |= META_OPEN;
                }

                if (BS_BFTAG_SEQ(bf_tag) == 0) {
                    bfSetT *bfSetp = GETBFSETP(mp);

                    if (bf_flag & META_OPEN) {
                       bfSetp = bfSetp->dmnP->bfSetDirp;
                    }

                    if (bs_get_current_tag( bfSetp, &bf_tag ) != EOK) {
                        error = ENOENT;
                        goto _exit;
                    }
                }
            }
        }

        /*
         * For META_OPEN, use dir_context lock to avoid racing. Otherwise
         * don't lock dir_context lest the caller tries to access
         * .tags/3 and bf_get would try to lock fsContext(=dir_context)
         * and would cause panic since it is already locked here!
         */
        if (bf_flag & META_OPEN) {
            FS_FILE_WRITE_LOCK( dir_context);
        }
        if (error = bf_get(bf_tag, 0, bf_flag, mp, &found_vp)) {
            if (bf_flag & META_OPEN) {
                FS_FILE_UNLOCK(dir_context);
            }
            error = ENOENT;
            goto _exit;
        } else {
            if (bf_flag & META_OPEN) {
                FS_FILE_UNLOCK(dir_context);
            }
            /*
             * Lookup in .tags for a reserved file is done.  (In this context,
             * a reserved file is any file whose bitfile set is the root tag
             * directory.  This includes the log, the bmts, the sbms and the
             * tag directories.)
             *
             * The vnode found is on the same mount point with the .tags
             * directory.  The access structure associated with the vnode is a
             * shadow to the real_bfap of the bitfile in the root tag directory.
             * The real_bfap recorded in the shadow access structure is used for
             * I/O.  The hidden vnode of the reserved file, if exists, is on the
             * dead_mount and is never returned here.
             */
            ndp->ni_vp = found_vp;
            MS_SMP_ASSERT(found_vp->v_mount == vp->v_mount);

            if (found_vp->v_usecount == 0) {
                ADVFS_SAD1("bf_get(1) returned bad vp N1 = tag", bf_tag.num);
            }
            /*
             * if the tag open was a VLNK, tell namei not to follow it
             */
            if (found_vp->v_type == VLNK) {
                ndp->ni_nameiop &= ~FOLLOW;
            } else {
               /*
                * Set NOCROSSMOUNT if current target is a dir entry in .tags
                * else don't because we need to allow namei to resolve the path
                * while traversing 'up' to '\' in order to honor /bin/pwd, but
                * we don't want to allow file or dir references through the
                * .tags dir (ie /.tags/<seq>/...) where: <seq> is the tag seq#
                * of another dir in the current advfs fileset which may be
                * mounted on and may be a different filesystem type.
                */
                if (set_nocross && found_vp->v_type == VDIR ) {
                        ndp->ni_nameiop |= NOCROSSMOUNT;
                }
            }
            /* prevent cfs or any other fs layered on top of us
             * from caching .tags lookups in the client dnlc
             */
            ndp->ni_makeentry = 0;

            error = 0;
            goto _exit;
        }
    }

    /*
     * Check the cache here.
     * ENOENT means the entry doesn't exist (negative caching).
     * 0 means the lookup failed.
     * -1 means the lookup succeeded, the vnode is returned in ni_vp.
     */
    if (error = cache_lookup(ndp)) {
        int vpid;

        if (error == ENOENT) {
            fsn->fileSetStats.lookup.hit_not_found++;
            goto _exit;
        }

        nvp = ndp->ni_vp;
        vpid = ndp->ni_vpid;

        /* Call vget_cache_nowait() before picking up the tag from the 
         * vnode pointed to by the namei cache entry.  Getting the wrong, 
         * but still valid tag, will be caught by the v_id test.  Getting 
         * a now invalid tag will cause bf_get to fail. 
         */
        VN_LOCK(nvp);
        if (error = vget_cache_nowait(nvp)) {
            VN_UNLOCK(nvp);
            ndp->ni_vp = NULL;
            goto lookup;
        }
        VN_UNLOCK(nvp);

        /* If the vnode has been recycled since cache_lookup saved its
         * v_id in the ndp then don't call bf_get() since the tag may
         * be bogus.  Instead call seq_search() to search the directory.
         */
        if (nvp->v_id != vpid) {
            vrele(nvp);
            ndp->ni_vp = NULL;
            goto lookup;
        } 

        bnp = (struct bfNode *)&nvp->v_data[0];
        bf_tag = bnp->tag;
        error = bf_get(bf_tag, dir_context, bf_flag, mp, &found_vp );

        vrele(nvp);

        /*
         * Check if bf_get() returned the same vnode as the cache entry.
         * If not, then just purge the old cache entry.  This must have
         * happened when the access structure was re-cycled.  Since the
         * new access structure didn't have a vnode, a new one was allocated.
         */
        if ( !error ) {
            if (nvp != found_vp) {
                cache_purge(nvp);
                ndp->ni_vp = found_vp;
                MS_SMP_ASSERT(new_bnp = (struct bfNode *)&found_vp->v_data[0]);
                MS_SMP_ASSERT(BS_BFTAG_EQL(bf_tag, new_bnp->tag));
            }

            fsn->fileSetStats.lookup.hit++;

            if (found_vp->v_usecount == 0) {
                ADVFS_SAD1("bf_get(2) returned bad vp N1 = tag", bf_tag.num);
            }
            error = 0;
            goto _exit;
        }
        else {
            ndp->ni_vp = NULL;
        }

#ifdef ADVFS_DEBUG
        if ( error == ENO_SUCH_TAG ) {
            printf("lookup could not find cached entry\n");
        }
#endif
    }

    /*
     * well the cache lookup failed, so do a regular search of the
     * directory.
     */

lookup:
    bf_flag = 0;
    fsn->fileSetStats.lookup.miss++;

    pin = REF_PAGE;

    /*
     * call seq_search to search the directory for the entry.
     * get the name to search for from nameidata.
     */
    if (flag == LOOKUP) {
        pin |= USE_OFFSET;
    }

    FS_FILE_READ_LOCK(dir_context);

    ndp->ni_dirstamp = dir_context->dirstamp;
    ndp->ni_nameiop &= ~BADDIRSTAMP ;

    ret = seq_search(
                     vp,
                     ndp,
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
        FS_FILE_UNLOCK(dir_context);
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
        ndp->ni_endoff++;

        /* ni_next == 0 means end of path */
        if ((flag == CREATE || flag == RENAME) && *ndp->ni_next == 0 && 
             dir_context->dir_stats.st_nlink != 0) {

            /*
             * check for write access to the directory for a create or rename.
             */

            FS_FILE_UNLOCK(dir_context);

            if (error = msfs_access(vp, S_IWRITE, ndp->ni_cred)) {
                /* if cannot write, return error */
                goto _exit;
            }

            /*
             * leave the dir accessed but not locked
             */

            error = ENOENT;
            goto _exit;

        } else { /* all other cases of not found, including ni_next !=
                    0 or deleting file... make negative entry*/
            if (ndp->ni_makeentry) {
                cache_enter(ndp);
            }

            FS_FILE_UNLOCK(dir_context);

            error = ENOENT;
            goto _exit;
        }
    }
    /*
     * the file was found in the directory (from the original call to seq_search)
     */

    if (ret != I_FILE_EXISTS) {
        ADVFS_SAD1("Unknown return from seq_search", ret);
    }
    /*
     * if lookup and end of path, save the location for a future lookup
     */
    if (*ndp->ni_next == '\0' && flag == LOOKUP) {
        dir_context->last_offset =
            ((ulong_t)ndp->ni_count * ADVFS_PGSZ) + ndp->ni_offset;
    }


    if ((flag == DELETE || flag == RENAME) && *ndp->ni_next == 0) {
        /*
         * check for write access to directory
         */
        if (error = msfs_access(vp, S_IWRITE, ndp->ni_cred)) {
            FS_FILE_UNLOCK(dir_context);
            ndp->ni_vp = NULL;
            goto _exit;
        }
    }

    /*
     * Deny attempts to rename onto "."
     */
    if (flag == RENAME && ndp->ni_nameiop & WANTPARENT &&
    *ndp->ni_next == 0 && BS_BFTAG_EQL(bnp->tag,found_bs_tag)) {
        FS_FILE_UNLOCK(dir_context);
        ndp->ni_vp = NULL;
        error = EISDIR;
        goto _exit;
    }

    /*
     * Pick up the dirstamp while the directory is still locked.
     */

    ndp->ni_dirstamp = dir_context->dirstamp;
    ndp->ni_nameiop &= ~BADDIRSTAMP ;

    /*
     * Do a bf_get on the file.  Note that this releases the shared
     * lock on the directory to avoid potential deadlock initializing
     * the context area of the file.
     */

    if ((flag == DELETE && *ndp->ni_next == 0)) {
        error = bf_get(found_bs_tag, dir_context, bf_flag, mp, &found_vp);
    } else {
        error = bf_get_l(found_bs_tag, dir_context, bf_flag, 
                                                    mp, &found_vp, ndp);
    }
    if ( error ) {
#ifdef ADVFS_DEBUG
        if ( error == ENO_SUCH_TAG ) {
            printf("error from bf_get in lookup\n");
        }
#endif
        ndp->ni_vp = NULL;
        goto _exit;
    }
    if (found_vp->v_usecount == 0) {
        ADVFS_SAD1("bf_get(3) returned bad vp N1 = tag", found_bs_tag.num);
    }

    ndp->ni_vp = found_vp;

    if (!(flag == DELETE && *ndp->ni_next == 0)) {
        /*
         * Make an entry in the namei cache for this file if it's not the
         * end of the path on a delete lookup.  Note that we've released
         * the shared directory lock, but the file is accessed and can't
         * go away.
         */
    } else {
        /*
         * if delete and end of path then do some
         * access checking
         * if directory is sticky, use must own the directory, or
         * the file in it, to delete it, unless root.
         */
        if ((dir_context->dir_stats.st_mode & S_ISVTX) &&
            ndp->ni_cred->cr_uid != 0 &&
            ndp->ni_cred->cr_uid !=
            dir_context->dir_stats.st_uid) {

            bnp = (struct bfNode *)&found_vp->v_data[0];
            file_context = bnp->fsContextp;
            /*
             * not locking the file_context here. this code is
             * racy with setattr anyway, the lock wouldn't
             * help.
             */
            if (file_context->dir_stats.st_uid !=
                ndp->ni_cred->cr_uid){
                vrele(found_vp);
                ndp->ni_vp = NULL;
                error = EPERM;
                goto _exit;
            }
        }
        error = 0;
        goto _exit;
    }

    error = 0;

_exit:
    return(BSERRMAP(error));
}


/*
 * check_path_back
 *
 * Given a source and a target directory, check that the source is not
 * anywhere in the path of the target. Used by msfs_rename when mv'ing
 * a directory.
 */

check_path_back(
                struct vnode *source_vp,  /* in - vnode of the source directory */
                struct vnode *target_vp,  /* in - vnode of the target directory */
                struct ucred *cred        /* in - cred - credentials of target */
                )
{
    struct bfNode *sbnp, *tbnp;
    bfTagT root_tag, dir_tag, *tagp;
    struct fsContext *t_context, *s_context;
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


    sbnp = (struct bfNode *)&source_vp->v_data[0];
    tbnp = (struct bfNode *)&target_vp->v_data[0];
    t_context =  tbnp->fsContextp;
    s_context = sbnp->fsContextp;
    root_tag = GETROOT(VTOMOUNT(target_vp));

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
                       BS_NIL
                       );
        if (ret != EOK) {
            error = EIO;
            if (!first_time) {
                if (!dont_unlock) {
                    FS_FILE_UNLOCK(t_context);
                }
                bs_close(bfap, MSFS_DO_VRELE);
            }
            goto out;
        }
        /*
         * find the ".." entry and get its's bf tag
         */

        n = (signed)dir_p->fs_dir_header.fs_dir_size;
        if ( FS_DIR_SIZE_OK(n, 0) ) {
            /* there has been a minor data corruption, inform and error out */
            fs_dir_size_notice(bfap, n, 0, 0);    
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
                    FS_FILE_UNLOCK(t_context);
                }
                bs_close(bfap, MSFS_DO_VRELE);
            }
            goto out;
        }
        p = (char *)dir_p;
        p += dir_p->fs_dir_header.fs_dir_size;
        dir_p = (fs_dir_entry *)p;
        tagp = GETTAGP(dir_p);
        dir_tag = *tagp;

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
                FS_FILE_UNLOCK(t_context);
            }
            ret = bs_close(bfap, MSFS_DO_VRELE);
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
                        BF_OP_GET_VNODE, NULLMT, &nullvp);
        if (ret != EOK) {
            error = EIO;
            break;
        }
        t_context = VTOC ( ATOV ( bfap ) );
        if (t_context == NIL) {
            t_context = vnode_fscontext_allocate(ATOV(bfap));
        }
        /*
         * share lock the dir. make sure before we lock that the dir
         * is not the parent of the source (it will already be locked)
         */
        if (BS_BFTAG_EQL(s_context->dir_stats.dir_tag, dir_tag)) {
            dont_unlock = TRUE;
        } else {
            FS_FILE_READ_LOCK(t_context);
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
    bfTagT *tagp;

    sbnp = (struct bfNode *)&source_vp->v_data[0];
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
                    ftx_handle
                    );
    if (ret != EOK) {
        ADVFS_SAD1("unable to ref page 0 in new_parent error N1", ret);
    }
    /*
     * find the ".." entry and update its bf tag
     */
    if ( FS_DIR_SIZE_OK(dir_p->fs_dir_header.fs_dir_size, 0) ) {
        return(EIO);
    }
    p = (char *)dir_p;
    p += dir_p->fs_dir_header.fs_dir_size;
    dir_p = (fs_dir_entry *)p;
    tagp = GETTAGP(dir_p);
    rbf_pin_record(
                   page_ref,
                   &dir_p->fs_dir_header.fs_dir_bs_tag_num,
                   sizeof(uint32T)
                   );
    dir_p->fs_dir_header.fs_dir_bs_tag_num = new_parent_tag.num;

    rbf_pin_record(
                   page_ref,
                   tagp,
                   sizeof(bfTagT)
                   );
    *tagp = new_parent_tag;

    mutex_lock(&s_context->fsContext_mutex);
    s_context->fs_flag |= MOD_MTIME;
    s_context->dirty_stats = TRUE;
    mutex_unlock(&s_context->fsContext_mutex);

    return(0);
}
