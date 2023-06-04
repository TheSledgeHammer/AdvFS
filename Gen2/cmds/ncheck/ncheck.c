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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 */
/*
 * HISTORY
 */

/*
 * ncheck -- obtain file names from reading filesystem
 */
#include <stdio.h>
#include <string.h>
#include <fstab.h>
#include <unistd.h>
#include <mntent.h>             /* for MNTTYPE_ADVFS */
#include <fcntl.h>
#include <nl_types.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/vnode.h>
#include <advfs/advfs_syscalls.h>

#include "vods.h"
#include "ncheck_advfs_msg.h"

#define MNTTYPE_ADVFS "advfs"

struct ilist {
    ino_t	ino;
    adv_mode_t	mode;
    adv_uid_t	uid;
    adv_gid_t	gid;
    int     cleanme;
    struct ilist *next;
} *ilist;
struct ilist *ilist_cur;

struct	htab {
    ino_t	h_ino;
    ino_t	h_pino;
    char	*h_name;
} *htab;

char *strngtab;
int hsize;
int strngloc;

struct dirstuff_advfs {
    off_t loc;                  /* 64-bits */
    int fd;
    /* dbuf needs to align along 64-bits since we are going to be casting
       structs out of it, otherwise we'll be getting SIGBUS errors */
    int pad;
    char dbuf[ADVFS_METADATA_PGSZ];
};

int	iflg = 0; /* number of inodes being searched for */
int Fflg = 0;
int Vflg = 0;
int	aflg = 0;
int	sflg = 0;
int	mflg = 0;
int rflg = 0;

int	fi;
ino_t	ino;
int	nhent;
int	nxfile = 0;
int	dev_bsize = 1;

/*
 * Current AdvFs fileset set ID and mount point.
 */
bfSetIdT setId;
char* fs_file = NULL;

/*
 * bitFileT for tag file -- used for raw operations
 * validTags == max number of tag for fileset
 */
bitFileT bf;
bitFileT tagBf;
uint32_t validTags;

/*
 * .tags path to open an advfs directory.
 */
char dir_path[MAXPATHLEN+1] = { 0 };
int	nerror = 0;

/* natural language support */
nl_catd catd;

/* local prototypes */
void usage(void);
void pname(ino_t i, int lev);
void check_raw(arg_infoT* infop);
void check_mounted(arg_infoT* infop, char *device);
void check(char *file);
void pass1(void);
void pass2(void);
void pass3(void);
int dotname(register struct dirent *dp);
struct htab *lookup(ino_t i, int ef);
struct dirent *nreaddir(register struct dirstuff_advfs *dirp, bitFileT *bfp);
int raw_readdir(struct dirstuff_advfs *dirp, bitFileT *bfp);
struct ilist *new_ilist_entry(void);
void Voption(int optind, int argc, char **argv);
void clean_ilist(void);

int main(int argc, char **argv)
{
    int c;
    long n;
    char *ino_ptr = NULL;
    char *ino_cur = NULL;

    catd = catopen(MF_NCHECK_ADVFS, NL_CAT_LOCALE);
    
    while((c = getopt(argc, argv, "F:i:Vasmr")) != EOF) {
        switch (c) {

        case 'i':
            /* check for list of tag numbers */
            ino_cur = optarg;
            while (ino_cur) {
                if (ino_ptr = strchr(ino_cur, ',')) {
                    *ino_ptr = '\0';
                    ino_ptr++;
                }
                n = atol(ino_cur);
                if (n == 0) {
                    fprintf (stderr,
                             catgets(catd,
                                     MS_NCHECK,
                                     NCHECK_BAD_IFLG,
                                     "ncheck (advfs): -i must be followed by inode numbers\n"));
                    usage();
                    exit(nerror + 1);
                }
                if(ilist == NULL) {
                    ilist = new_ilist_entry();
                    ilist_cur = ilist;
                }
                ilist_cur->ino  = n;
                ilist_cur->next = new_ilist_entry();
                ilist_cur = ilist_cur->next;
                ino_cur = ino_ptr;
            }
            iflg++;
            break;

        case 'a':
            aflg++;
            break;
                        
        case 's':
            sflg++;
            break;
                        
        case 'm':
            mflg++;
            break;
            
        case 'r':
            rflg++;
            break;

        case 'F':       /* fstype verification */
                        /* The -F FSType option is parsed by the MFS wrapper. */
                        /* However, it has been added here also, so that     */
                        /* the (advfs specific) ncheck code is a complete unit by */
                        /* itself (especially, if the wrapper is not installed) */
            if (strcmp(optarg, MNTTYPE_ADVFS)) {
                fprintf(stderr,
                        catgets(catd,
                                MS_NCHECK,
                                NCHECK_INVALID_FSTYPE,
                                "ncheck (advfs): invalid fstype %s\n"),
                        optarg);
                usage();
                exit(nerror + 1);
            }
            Fflg++;
            break;

        case 'V':
            Vflg++;
            break;
                        
        default:
            fprintf(stderr,
                    catgets(catd,
                            MS_NCHECK,
                            NCHECK_BAD_FLAG,
                            "ncheck (advfs): bad flag %c\n"),
                    (char)c);
            usage();
            exit(1);
        }
    }

    if(mflg && !(iflg || sflg)) {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_BAD_MFLG,
                        "ncheck (advfs): '-m' can only be used with '-i' or '-s'\n"));
        usage();
        exit(1);
    }
    
    if(Vflg && !nerror) {
        Voption(optind, argc, argv);
        exit(0);
    }

    if(optind == argc) {
        /* no device specified, operate on all AdvFS filesystems in /etc/fstab */
        struct mntent *fs;
        FILE *fstab;
        fstab = setmntent(MNT_CHECKLIST, "r");
        if (fstab == NULL) {
            fprintf(stderr,
                    catgets(catd,
                            MS_NCHECK,
                            NCHECK_NO_FILE,
                            "ncheck (advfs): no %s file\n"),
                    MNT_CHECKLIST);
            exit(1);
        }
        
        while (fs = getmntent(fstab)) {
            if (!strcmp(fs->mnt_type, MNTTYPE_ADVFS)) {
                check(fs->mnt_fsname);
                if(htab != NULL) {
                    free(htab);
                }
                if(strngtab != NULL) {
                    free(strngtab);
                }
                clean_ilist();
            }
        }
        endmntent(fstab);
    }
    
    while (optind < argc) {
        check(argv[optind]);
        optind++;
    }
    return(nerror);
}

void usage(void) {
    fprintf(stderr,
            catgets(catd,
                    MS_NCHECK,
                    NCHECK_USAGE,
                    "Usage:\tncheck (advfs)\n\tncheck [-F advfs] [-i numbers] [-V|-a|-s|-m|-r] {special...|fsname...}\n"));
}

void Voption(int optind, int argc, char **argv)
{
    int i;
    struct ilist *ilistp = ilist;

    printf("ncheck ");

    if (Fflg) {
        printf("-F %s ", MNTTYPE_ADVFS);
    }
    if(iflg) {
        printf("-i ");
        for(ilistp = ilist; ilistp->ino != 0; ilistp = ilistp->next) {
            printf("%d", ilistp->ino);
            if(ilistp->next->ino != 0) {
                printf(",");
            }
        }
    }
    printf(" ");
    if(aflg) {
        printf("-a ");
    }
    if(sflg) {
        printf("-s ");
    }
    if(mflg) {
        printf("-m ");
    }
    if(rflg) {
        printf("-r ");
    }

    while(optind < argc) {
        printf("%s ", argv[optind]);
        optind++;                
    }
    printf("\n");
}

void check(char *file)
{
    register int i;
    char *fsname = NULL;
    char *fsname_tmp = NULL;
    char blkfile[MAXPATHLEN + 1] = { 0 };
    arg_infoT *argInfop = NULL;
    struct stat filestat;
    statusT sts;

    nhent = 0;

    argInfop = process_advfs_arg(file);
    if(argInfop->fsname == NULL) {
        fprintf(stderr, catgets(catd,
                                MS_NCHECK,
                                NCHECK_BAD_FS,
                                "ncheck (advfs): %s not an advfs filesystem\n"),
                file);
        nerror++;
        return;
    }
    
    if(rflg) {
        check_raw(argInfop);
        return;
    } else {    
        check_mounted(argInfop, argInfop->blkfile);
    }
}

void check_mounted(arg_infoT* infop, char *device)
{
    register struct mntent *fs;
    char fspec[MAXPATHLEN + 1] = { 0 };
    bfSetParamsT setParams;
    statusT sts;
    int  ret;

    /* get the fileset information */
    if ((sts = advfs_fset_get_id(infop->fsname, infop->fset, &setId)) != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_BAD_FILE_SET,
                        "ncheck (advfs): could not get file set ID: %s\n")
                ,BSERRMSG(sts));
        nerror++;
        return;
    }

    /*
     * Find the fileset mount point 
     */

    ret = advfs_is_mounted(infop, NULL, NULL, &fs_file);
    
    if(ret == 0) {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_NO_FSPEC,
                        "ncheck (advfs): %s not in %s\n"),
                infop->fsname, MNT_MNTTAB);
        nerror++;
        return;
    }
    else if (ret == 1) {
        printf("%s:\n", fs_file);
    }
    else {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_BAD_FS,
                        "ncheck (advfs): %s not an advfs filesystem\n"),
                fspec);
        nerror++;
        return;
    }
        
    /*
     * Determine the number of bitfiles in the fileset.
     */
    if ((sts = advfs_get_bfset_params(setId, &setParams)) != EOK) {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_BAD_BFSET_PARAMS,
                        "ncheck (advfs): get bitfile set parameters: %s\n"),
                BSERRMSG(sts));
        nerror++;
        return;
    }

    hsize = setParams.filesUsed + 1;

    htab = (struct htab *)calloc((unsigned)hsize, sizeof(struct htab));
    strngtab = (char *)calloc(30, hsize);
    if (htab == 0 || strngtab == 0) {
        (void) fprintf(stderr,
                       catgets(catd,
                               MS_NCHECK,
                               NCHECK_NO_MEM,
                               "ncheck (advfs): out of memory\n"));
        nerror++;
        return;
    }
        
    pass1();
        
    pass2();
        
    pass3();
        
}

void check_raw(arg_infoT* infop)
{
    bitFileT *bfp = &bf;
    bitFileT *tagBfp = &tagBf;
    bfTagT tag;
    int flags = VFLG;
    int v, p, c;
    int ret;
    int i;
    bitFileT head, *current, *last = &head;

    /***************************/
    char *defaultfs = "default";
    int tmp = 0;
    /**************************/

    init_bitfile(bfp);
        
    bfp->type = DMN;
    ret = resolve_name(bfp, infop->fsname, flags);
    if ( ret != OK ) {
        exit(2);
    }
    optind++;

    tagBf = bf;
    find_TAG(tagBfp);

    if ( get_set(tagBfp, &defaultfs, 1, &tmp, bfp) != OK )
        {
            fprintf(stderr,
                    catgets(catd,
                            MS_NCHECK,
                            NCHECK_BAD_FILE_SET,
                            "ncheck (advfs): could not get file set ID: %s\n"),
                    defaultfs);
            exit(2);
        }
    *tagBfp = *bfp;
    newbf(bfp, &head);

    /* Get number of valid tags */
    if (read_page(tagBfp, 0)) {
        fprintf(stderr,
                catgets(catd,
                        MS_NCHECK,
                        NCHECK_BAD_TAGDIR_PAGE,
                        "ncheck (advfs): cannot read tag directory page %ld\n"),
                0);
        exit(2);
    }
    
    validTags =
        (BS_TD_TAGS_PG *
         ((bsTDirPgT *)(tagBfp->pgBuf))->tMapA[0].tmUninitPg) - 1;

    hsize = validTags + 2;
        
    htab = (struct htab *)malloc((unsigned)hsize * sizeof(struct htab));
    strngtab = (char *)calloc(30, hsize);
    if (htab == 0 || strngtab == 0) {
        (void) fprintf(stderr,
                       catgets(catd,
                               MS_NCHECK,
                               NCHECK_NO_MEM,
                               "ncheck (advfs): out of memory\n"));
        nerror++;
        return;
    }
    
    printf("%s:\n", infop->fspath);
    
    pass1();

    pass2();

    pass3();
    
}

void pass1(void)
{
    bfTagT tag;
    statusT sts;
    adv_uid_t uid;
    adv_gid_t gid;
    adv_off_t size;
    adv_time_t atime;
    adv_mode_t mode;
    struct ilist *ilistp = NULL;
    int i;
    
    tag.tag_num=0; tag.tag_seq = 0;

    if(rflg) {
        sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                 &size, &atime, &mode);
    }
    else {  
        sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                             &size, &atime, &mode);
    }
    
    while (sts == EOK) {
    
        if (BS_BFTAG_RSVD(tag)) goto nextloop;
    
        if (mflg) {
            for (ilistp = ilist; ilistp != NULL; ilistp = ilistp->next)
                if (tag.tag_num == ilistp->ino) {
                    ilistp->mode = mode;
                    ilistp->uid = uid;
                    ilistp->gid = gid;
                }
        }
        if (!S_ISDIR(mode)) {
            if (sflg == 0) {
                goto nextloop;
            }
            if (S_ISBLK(mode) || S_ISCHR(mode) || mode & (S_ISUID|S_ISGID)) {
                if(ilist == NULL) {
                    ilist = new_ilist_entry();
                    ilist_cur = ilist;
                }
                ilist_cur->ino = tag.tag_num;
                ilist_cur->mode = mode;
                ilist_cur->uid = uid;
                ilist_cur->gid = gid;
                ilist_cur->cleanme = 1;
                ilist_cur->next = new_ilist_entry();
                ilist_cur = ilist_cur->next;
                goto nextloop;
            }
        }
        lookup(tag.tag_num, 1);

    nextloop:
        /* reissue tag stats */
        if(rflg) {
            sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                     &size, &atime, &mode);
        }
        else {  
            sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                                 &size, &atime, &mode);
        }
    }
}

void pass2(void)
{
    bfTagT tag;
    statusT sts;
    adv_uid_t uid;
    adv_gid_t gid;
    adv_off_t size;
    adv_time_t atime;
    adv_mode_t mode;
    struct htab *hp;
    int fd;
    struct dirent *dp = NULL;
    struct dirstuff_advfs dirp;
    bitFileT bf;
    
    tag.tag_num=0; tag.tag_seq = 0;

    if(rflg) {
        sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                 &size, &atime, &mode);
    }
    else {  
        sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                             &size, &atime, &mode);
    }
    
    while (sts == EOK) {
        
        memset(&dirp, 0, sizeof(struct dirstuff_advfs));

        if (BS_BFTAG_RSVD(tag) || !S_ISDIR(mode)) goto nextloop;

        if(!rflg) {
            sprintf(dir_path, "%s/.tags/%d", fs_file, tag.tag_num);  
            if ((dirp.fd = open(dir_path, O_RDONLY, 0444)) == -1) {
                nerror++;
                return;
            }
        }
        
        for (dp = nreaddir(&dirp, &bf);
             dp != NULL;
             dp = nreaddir(&dirp, &bf)) {
            if (dp->d_ino == 0)
                continue;
            hp = lookup(dp->d_ino, 0);
            if (hp == 0) {
                continue;
            }
            if (dotname(dp))
                continue;
            hp->h_pino = tag.tag_num;
            hp->h_name = &strngtab[strngloc];
            strngloc += dp->d_namlen + 1;
            (void) strncpy(hp->h_name, dp->d_name, dp->d_namlen);
            strngtab[strngloc - 1] = '\0';
        }
        
    nextloop:
        /* every loop should issue a close, else we'll hit fd limits */
        if (dirp.fd > 0) {
            close(dirp.fd);
        }
        
        /* reissue tag stats */
        if(rflg) {
            sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                     &size, &atime, &mode);
        }
        else {  
            sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                                 &size, &atime, &mode);
        }        
    }
}

void pass3(void)
{
    bfTagT tag;
    statusT sts;
    adv_uid_t uid;
    adv_gid_t gid;
    adv_off_t size;
    adv_time_t atime;
    adv_mode_t mode;
    struct dirent *dp;
    struct htab *hp;
    int fd;
    struct dirstuff_advfs dirp;
    int k;
    struct ilist *ilistp = NULL;
    bitFileT bf;
    
    tag.tag_num=0; tag.tag_seq = 0;

    if(rflg) {
        sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                 &size, &atime, &mode);
    }
    else {  
        sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                             &size, &atime, &mode);
    }

    while(sts == EOK) {

        memset(&dirp, 0, sizeof(struct dirstuff_advfs));

        if (BS_BFTAG_RSVD(tag) || !S_ISDIR(mode)) goto nextloop;

        if(!rflg) {
            sprintf(dir_path, "%s/.tags/%d", fs_file, tag.tag_num);  
            if ((dirp.fd = open(dir_path, O_RDONLY, 0444)) == -1) {
                nerror++;
                return;
            }
        }
        
        for (dp = nreaddir(&dirp, &bf);
             dp != NULL;
             dp = nreaddir(&dirp, &bf)) {
            if (aflg == 0 && dotname(dp))
                continue;
            if(sflg == 0 && iflg == 0)
                goto pr;
            
            for (ilistp = ilist; ilistp->ino != 0; ilistp = ilistp->next) {
                if (ilistp->ino == dp->d_ino) {
                    break;
                }
            }
            if (ilistp->ino == 0)
                continue;
            if (mflg)
                (void) printf("mode %-6o uid %-5d gid %-5d ino ",
                              ilistp->mode, ilistp->uid, ilistp->gid);
        pr:
            (void) printf("%-5lu\t", dp->d_ino);
            pname(tag.tag_num, 0);
            (void) printf("/%s", dp->d_name);
            if (lookup(dp->d_ino, 0))
                (void) printf("/.");
            (void) printf("\n");
        }
        
    nextloop:
        /* every loop should issue a close, else we'll hit fd limits */
        if (dirp.fd > 0) {
            close(dirp.fd);
        }

        /* reissue the tag stats */
        if(rflg) {
            sts = advfs_tag_stat_raw(&tag, &tagBf, &bf, &uid, &gid, 
                                     &size, &atime, &mode);
        }
        else {  
            sts = advfs_tag_stat(&tag, setId, &uid, &gid, 
                                 &size, &atime, &mode);
        }
    }
}

/*
 * get next entry in an advfs directory.
 */
struct dirent *nreaddir(struct dirstuff_advfs *dirp, bitFileT *bfp)
{
    ssize_t bytes;
    struct dirent *dp;

    while (1) {
        if (dirp->loc % ADVFS_METADATA_PGSZ == 0) {
            if (rflg && bfp &&
                (bytes = raw_readdir(dirp, bfp)) <= 0) {
                return NULL;
            }
            if (!rflg &&
                ((bytes = read(dirp->fd, dirp->dbuf, ADVFS_METADATA_PGSZ)) <= 0)) {
                return NULL;
            }
        }
        
        dp = (struct dirent *)&dirp->dbuf[dirp->loc & (ADVFS_METADATA_PGSZ - 1)];
        dirp->loc += dp->d_reclen;
        if (dp->d_ino == 0) continue;

        return (dp);
    }
}

int dotname(register struct dirent *dp)
{

    if (dp->d_name[0]=='.')
        if (dp->d_name[1]==0 ||
            (dp->d_name[1]=='.' && dp->d_name[2]==0))
            return(1);
    return(0);
}

void pname(ino_t i, int lev)
{
    register struct htab *hp;

    if (i==ROOTINO)
        return;
    if ((hp = lookup(i, 0)) == 0) {
        (void) printf("???");
        return;
    }
    if (lev > 10) {
        (void) printf("...");
        return;
    }
    pname(hp->h_pino, ++lev);
    (void) printf("/%s", hp->h_name);
}

struct htab * lookup(ino_t i, int ef)
{
    register struct htab *hp;
    ino_t initialh_ino = 0;
        
    for (hp = &htab[i%hsize]; hp->h_ino;) {
                
        /* if inode not found in tab, return(0); */
        if(initialh_ino  == 0)
            initialh_ino=hp->h_ino;
        else if (initialh_ino == hp->h_ino)
            return(0);
        
        if (hp->h_ino==i)
            return(hp);
        if (++hp >= &htab[hsize])
            hp = htab;
    }
    if (ef==0)
        return(0);
    if (++nhent > hsize) {
        (void) fprintf(stderr,
                       catgets(catd,
                               MS_NCHECK,
                               NCHECK_HASH_TOO_SMALL,
                               "ncheck (advfs): tag hash size of %ld is too small\n"),
                       hsize);
        exit(1);
    }
    hp->h_ino = i;
    return(hp);
}

struct ilist *new_ilist_entry(void)
{
    struct ilist *p;
        
    p = (struct ilist *)calloc(1, sizeof (struct ilist));
    if (!p)
        {
            fprintf (stderr,
                     catgets(catd,
                             MS_NCHECK,
                             NCHECK_NO_MEM,
                             "ncheck (advfs): out of memory\n"));
            exit(1);
        }
        
    return p;        
}

void clean_ilist(void) {
    struct ilist *p = NULL;
    struct ilist *tmp = NULL;
    
    p = ilist;

    if(!p) {
        return;
    }
    
    while(p->next != NULL) {
        if(p->next->cleanme) {
            tmp = p->next;
            p->next = p->next->next;
            free(tmp);
        }
    }
}

int raw_readdir(struct dirstuff_advfs *dirp, bitFileT *bfp)
{
    int i = dirp->loc / ADVFS_METADATA_PGSZ; /* trucates */
    int found = 0;
    statT *recp = NULL;
    
    /* verify we aren't past the end of the directory */
    if( i >= bfp->pages ) {
        return(0);
    }
    
    /* read dir page */
    if ( read_page(bfp, i) ) {
        return(0);
    }
    
    memcpy(dirp->dbuf, bfp->pgBuf, ADVFS_METADATA_PGSZ);
    
    return(ADVFS_METADATA_PGSZ);
}

adv_status_t
advfs_tag_stat_raw(
                   bfTagT         *tag,                /* in/out */
                   bitFileT       *tagBfp,             /* in */
                   bitFileT       *bfp,                /* out */
                   adv_uid_t      *uid,                /* out */
                   adv_gid_t      *gid,                /* out */
                   adv_off_t      *size,               /* out */
                   adv_time_t     *atime,              /* out */
                   adv_mode_t     *mode                /* out */
                   )
{
    statT *recp = NULL;
    bitFileT bfp2;
    bitFileT *current = &bfp2;
    
    if(bfp) {
        current = bfp;
    }
    
    newbf(tagBfp, current);
    
    if(tag->tag_num < 2) {
        tag->tag_num = 2;
    } else {
        /* for sequential calls to tag_stat_raw */
        tag->tag_num++;
    }
    
    while( tag->tag_num < validTags ) {
        if ( get_file_by_tag(tagBfp, tag->tag_num, current) ) {
            tag->tag_num++;
            continue;
        }
        break;
    }

    if( !(tag->tag_num < validTags) ) {
        return ENO_SUCH_TAG;
    }
    
    if ( find_rec(current, BMTR_FS_STAT, (char**)&recp) ) {
        return EBAD_TAG;
    }
    
    *uid = recp->st_uid;
    *gid = recp->st_gid;
    *size = recp->st_size;
    *atime = recp->st_atime;
    *mode = recp->st_mode;
    
    return EOK;
}
