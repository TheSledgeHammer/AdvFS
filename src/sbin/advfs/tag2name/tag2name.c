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
#pragma ident "@(#)$RCSfile: tag2name.c,v $ $Revision: 1.1.14.1 $ (DEC) $Date: 2003/11/11 21:22:20 $"

/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*   NOTE:  This program, /sbin/advfs/tag2name, is reference by hardcoded     */
/*          pathname in src/kernel/msfs/osf/msfs_io.c in bs_osf_complete().   */
/*          If you change this program's name or location, please make the    */
/*          corresponding change to bs_osf_complete()!                        */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/

#include <stdio.h>
#include "../vods/vods.h"
#include <msfs/fs_dir.h>
#include <msfs/bs_public.h>
#include <msfs/bs_ods.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/fcntl.h>
#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "tag2name_msg.h"
nl_catd catd;

extern int errno;
extern char *sys_errlist[];

static char *Prog;

usage()
{
    char *errstr;

    errstr = catgets(catd, 1, USAGE, "usage: %s [-r] <domain> {[-S] <fileset> | [-T] <fileset_tag>} <file_tag>\n");
    fprintf(stderr, errstr, Prog);
    errstr = catgets(catd, 1, 2, "       %s <mount_point>/.tags/<file_tag>\n");
    fprintf(stderr, errstr, Prog);

    exit(1);
}

/******************************************************************************/
main(int argc, char *argv[])
{
    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    (void)setlocale(LC_ALL, "");
    catd = catopen(MF_TAG2NAME, NL_CAT_LOCALE);

    if ( argc >= 4 ) {
        raw_t2n(argc, argv);
    }

    if ( argc == 2 ) {
        fs_t2n(argc, argv);
    }

    usage();
}

/******************************************************************************/
raw_t2n(int argc, char *argv[])
{
    bitFileT bf;
    bitFileT *bfp = &bf;
    bitFileT tagBf;
    bitFileT *tagBfp = &tagBf;
    bfTagT tag;
    int flags = 0;
    int v, p, c;
    int ret;
    int i;
    uint32T validTags;
    bitFileT head, *current, *last = &head;
    long longnum;
    extern int optind;

    init_bitfile(bfp);

    if ( strncmp(argv[optind], "-r", 2) == 0 ) {
        optind++;
        flags = VFLG;
    }

    bfp->type = DMN;
    ret = resolve_name(bfp, argv[optind], flags);
    if ( ret != OK ) {
        exit(2);
    }
    optind++;

    tagBf = bf;
    find_TAG(tagBfp);

    if ( get_set(tagBfp, argv, argc, &optind, bfp) != OK )
    {
        fprintf(stderr, "set not found\n");
        exit(2);
    }
    *tagBfp = *bfp;
    newbf(bfp, &head);

    if ( optind == argc ) {
	fprintf(stderr, "Missing tag number.\n");
        usage();
    }

    if ( getnum(argv[optind], &longnum) != OK ) {
        fprintf(stderr, "badly formed tag number (%s)\n", argv[optind]);
        exit(1);
    }
    head.fileTag = longnum;

    optind++;
    if ( argc > optind ) {
        fprintf(stderr, "Too many arguments.\n");
        usage();
    }

    /* Get number of valid tags */
    if (read_page(tagBfp, 0)) {
       fprintf(stderr, "tag2name: cannot read tag directory page 0\n");
       exit(2);
    }
    validTags =
       (BS_TD_TAGS_PG *
          ((bsTDirPgT *)(tagBfp->pgBuf))->tMapA[0].tmUninitPg) - 1;

    if ( head.fileTag == 0 || head.fileTag > validTags ) {
        fprintf(stderr, "Invalid tag (%d). ", longnum);
        fprintf(stderr, " Valid tag values are 1 to %d.\n", validTags);
        exit(2);
    }

    if ( head.fileTag == 1 ) {
        printf("Tag 1 is the FRAG file.\n");
        exit(0);
    }

    head.next = NULL;
    current = &head;

    /*
    ** from tag and root tag file find primary mcell
    ** from primary mcell find parent tag in BMTR_FS_STAT record
    ** loop until parent tag is 2 (root directory tag)
    ** create a linked list of bitFileT, one for each component of the path
    */
    do {
        statT *recp;

        if ( current->fileTag == 0 ) {
            /* FIX. error exit w/o describing error */
            exit(3);
        }

        if ( get_file_by_tag(tagBfp, current->fileTag, current) ) {
            fprintf(stderr, "tag %d is not in use.\n", current->fileTag);
            exit(3);
        }

        if ( find_rec(current, BMTR_FS_STAT, (char**)&recp) ) {
            if ( !find_rec(current, BMTR_FS_INDEX_FILE, (char**)&recp) ) {
                fprintf(stderr, catgets(catd, 1, 16,
                        "Tag %d is a directory index file.\n"),
                        current->fileTag);
                exit(0); 
            } else {
                fprintf(stderr, "Can't find BMTR_FS_STAT record for tag %d\n",
                   current->fileTag);
                exit(3);
            }
        }
        last = current;
        current = malloc(sizeof(bitFileT));
        if ( current == NULL ) {
            perror("raw_t2n");
            exit(4);
        }

        newbf(last, current);
        current->next = last;
        current->fileTag = recp->dir_tag.num;
    } while ( current->fileTag != 2);

    /*------------------------------------------------------------------
    ** start at the head (first path component) of the linked bitFileTs
    ** read the diretory
    ** search for tag of next path component
    ** print the component name
    */
    while ( current->next ) {
        int found;

        found = 0;
        if ( get_file_by_tag(tagBfp, current->fileTag, bfp) ) {
            exit(3);
        }

        /* read file set root dir looking for file name */
        for ( i = 0; i < bfp->pages; i++ ) {
            fs_dir_entry  *dir_ent_p;

            /* read dir page */
            if ( read_page(bfp, i) ) {
                fprintf(stderr, "read_page failed\n");
                exit(3);
            }
            dir_ent_p = (fs_dir_entry*)(bfp->pgBuf);

            /* scan dir page looking for file */
            while ( (char*)dir_ent_p < &bfp->pgBuf[PAGE_SIZE] ) {
                if ( dir_ent_p->fs_dir_header.fs_dir_namecount >= FS_DIR_MAX_NAME ) {
                    printf("dir err\n");
                    exit(3);
                }
                dir_ent_p->fs_dir_name_string[dir_ent_p->fs_dir_header.fs_dir_namecount] = '\0';

                if ( current->next->fileTag == dir_ent_p->fs_dir_header.fs_dir_bs_tag_num ) {
                    found = 1;
                    /* current->name = dir_ent_p->fs_dir_name_string; */
                    printf("%s", dir_ent_p->fs_dir_name_string);
                    break;
                }
                dir_ent_p = (fs_dir_entry*)((char*)dir_ent_p +
                                        dir_ent_p->fs_dir_header.fs_dir_size);
            }
            if ( found ) {
                break;
            }
        }
        current = current->next;
        if ( current->next ) {
            printf("/");
        } else {
            printf("\n");
        }
    }

    exit(0);
}

/******************************************************************************/
typedef struct name {
    char name[NAME_MAX+1];
    struct name *next;
} nameT;

fs_t2n(int argc, char *argv[])
{
    char *fname;
    char dirname[MAXPATHLEN];
    char tagstring[20];
    char *cp, *badcp;
    long tagnum;
    int errorno;
    int fd;
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    mlStatusT sts;
    mlBfTagT fTag, parentTag;
    struct statfs fsStats; /* file set's fstats */
    struct stat rootStats; /* file set's root stats */
    nameT *names = NULL;
    nameT *nm;
    char *errstr;
    extern int optind;
    extern char *optarg;
    int first_time = TRUE;

    /* check for root */
    if ( geteuid() ) {
        errstr = catgets(catd,1,9,
                    "\nPermission denied - user must be root to run %s.\n\n");
        fprintf(stderr, errstr, argv[0]);
        usage();
        exit(1);
    }

    fname = argv[optind];
    if (fname == NULL) {
       usage();
       exit(1);
    }

    /* Get last component */
    cp = strrchr(fname, '/');
    if ( cp == NULL ) {
        cp = fname;
    } else {
        cp++;
        if (*cp == 0) {
           usage();
           exit(1);
        }
    }

    errno = 0;
    tagnum = strtol(cp, &badcp, 10);
    errorno = errno;


    /* Check for numeracy */
    if (badcp - cp != strlen(cp)) {

        /* Check for valid option (none here, but -r is legal in raw mode) */
        if ((cp == fname)  &&  (*cp == '-')) {
            if (strcmp(cp, "-r")) {
                errstr = catgets(catd, 1, 14, "%s: illegal option -- %s\n");
                fprintf(stderr, errstr, Prog, cp+1);
            }
            usage();
            exit(1); 
        }

        errstr = catgets(catd, 1, 12, "%s: Tags must be numeric.\n");
        fprintf(stderr, errstr, Prog);
        exit(1);
    }

    /* Check for range */
    if ((errorno == ERANGE)  ||  (tagnum < 2)  ||  (tagnum > INT_MAX)) {
        errstr = catgets(catd, 1, 13,
            "%s: Tags cannot be less than 2, or greater than %d.\n");
        fprintf(stderr, errstr, Prog, INT_MAX);
        exit(1);
    }

again:
    fd = open(fname, O_RDONLY);
    if ( fd < 0 ) {
        errstr = catgets(catd, 1, 6, "%s: open(%s) error; [%d] %s\n");
        fprintf(stderr, errstr, Prog, fname, errno, BSERRMSG(errno));
        exit(2);
    }

    if ( fstatfs(fd, &fsStats) < 0 ) {
        errstr = catgets(catd, 1, 4, "%s: fstatfs(%s) error; [%d] %s\n");
        fprintf(stderr, errstr, Prog, fname, errno, BSERRMSG(errno));
        exit(2);
    }

    /* make sure this is AdvFS */
    if ( fsStats.f_type != MOUNT_MSFS ) {
        errstr = catgets(catd, 1, 10,
          "File specified is not in an AdvFS fileset\n");
        fprintf(stderr, errstr);
        usage();
    }

    /* find the parent directory which should be .tags */
    cp = strrchr(fname, '/');
    if ( cp != NULL ) {
        *cp = '\0';
    } else {
        fname[0] = '.';
        fname[1] = '\0';
    }

    if ( stat(fname, &rootStats) < 0 ) {
        errstr = catgets(catd, 1, 5, "%s: stat(%s) error; %s\n");
        fprintf(stderr, errstr, Prog, fname, BSERRMSG(errno));
        exit(2);
    }

    /* The .tags directory is tag number 3 */
    if ( rootStats.st_ino != 3 ) {
        errstr = catgets(catd, 1, 11,
          "%s is not the special .tags directory.\n");
        fprintf(stderr, errstr, fname);
        usage();
    }

    if ( stat(fsStats.f_mntonname, &rootStats) < 0 ) {
        errstr = catgets(catd, 1, 5, "%s: stat(%s) error; %s\n");
        fprintf(stderr, errstr, Prog, fsStats.f_mntonname, BSERRMSG(errno));
        exit(2);
    }

    sts = advfs_get_bf_params(fd, &bfAttr, &bfInfo);
    if ( sts != EOK ) {
        errstr = catgets(catd,1, 7, "%s: can't get file attributes; [%d] %s\n");
        fprintf(stderr, errstr, Prog, sts, BSERRMSG(sts));
        exit(3);
    }

    fTag = bfInfo.tag;
    /*
     * Make sure that this tag is usable.  Only non-metadata tags are allowed.
     */
    if ( (int)fTag.num < 2 ) {
        errstr = catgets(catd, 1, 12,
                  "%s: Tags must be numeric and greater than or equal to 2.\n");
        fprintf(stderr, errstr, Prog);
        exit(1);
    }

    /*
     * The following loop calls msfs_get_name() to get the file/dir name
     * string for a tag.  msfs_get_name() also returns the tag's parent
     * dir's tag.  So we keep calling msfs_get_name() until we've
     * collected the name strings for all the pathname components
     * of the complete pathname for the trashcan dir.
     *
     * The pathname components are linked together using dNameT
     * structures.  This is singly-linked list and the new elements
     * are inserted at the head.  Since we have to start with the
     * trashcan dir's tag and work our way backwards to the file set
     * root dir, using this list has the effect of reversing the
     * order of the pathname components so that a forward traversal
     * of the list gives us the correct pathname; the components
     * are in the correct forward (left to right) order.
     *
     * The loop is terminated when we reach the file set's root dir.
     */

    do {
        /* Get a new pathname component struct */

        nm = (nameT *)malloc(sizeof(nameT));
        if ( nm == NULL ) {
            perror("fs_t2n");
            exit(4);
        }

        sts = msfs_get_name(fsStats.f_mntonname, fTag, nm->name, &parentTag);
        if ( sts != EOK ) {
            /*
             * If this is the first call to msfs_get_name(), see if the
             * error is due to the fact that the file is an index file.
             * If it is, it will have an associated directory, which will
             * have a name.
             */
            /*
             * If advfs_get_idxdir_bf_params() does not exist, we will
             * have that function return ENOT_SUPPORTED.  Then we print
             * the message we want.
             */
            if (first_time) {
                sts = advfs_get_idxdir_bf_params(fd, &bfAttr, &bfInfo);
                if ( sts != EOK ) {
                    errstr = catgets(catd, 1, 8, "%s: msfs_get_name() error.\nTry this:  %s [-r] <domain> {[-S] <fileset> | [-T] <fileset_tag>} <file_tag>\n");
                    fprintf(stderr, errstr, Prog, Prog );
                    exit(3);
                }
                else {
                    fprintf(stderr, catgets(catd, 1, 15,
                            "%d is a directory index file.\n"), fTag);
                    fprintf(stderr, "The directory it indexes is ");
                    strcpy(dirname, fname);
                    strcat(dirname, "/");
                    sprintf(tagstring, "%d", bfInfo.tag.num);
                    strcat(dirname, tagstring);
                    strcpy(fname, dirname);
                    first_time = FALSE;
                    goto again;
                }
            }
            else {
                errstr = catgets(catd, 1, 8, "%s: msfs_get_name() error.\nTry this:  %s [-r] <domain> {[-S] <fileset> | [-T] <fileset_tag>} <file_tag>\n");
                fprintf(stderr, errstr, Prog, Prog );
                exit(3);
            }
        }

        first_time = FALSE;
        nm->next = names;
        names = nm;

        fTag = parentTag;

    } while ( fTag.num != rootStats.st_ino );

    if (strcmp(fsStats.f_mntonname, "/"))
        printf("%s", fsStats.f_mntonname);

    while ( names != NULL ) {
        nameT *tmp;

        if ( names->name[0] != '/' ) {
            printf("/");
        }

        printf("%s", names->name);
        tmp = names;
        names = names->next;
        free(tmp);
    }

    printf("\n");

    close(fd);
    exit(0);
}
