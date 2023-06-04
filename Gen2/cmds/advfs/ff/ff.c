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
 *   ff_advfs 
 *
 *   This utility lists file names and statistics, garnered from the 
 *   AdvFS .tags directory entries, for the given AdvFS filesystems.
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <libgen.h>
#include <errno.h>
#include <ftw.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <advfs/advfs_syscalls.h>
#include <locale.h>
#include <nl_types.h>
#include "ff_advfs_msg.h"

nl_catd catd;

extern char *sys_errlist[];

static char *Prog;

/* struct to handle time comparison */
typedef enum {
    OLDER,
    NEWER,
    EXACT
}  deltaType_t;

#define DAY (60 * 60 * 24)      /* seconds per day */

#define ERR_BAD_FS 32	        

#define MAXUSRTAGS 1024         /* Maximum number of tags that can be 
                                 * specified on the command line.
                                 */

typedef struct {
    int64_t      deltaVal;
    deltaType_t  deltaType;
} delta_t;

/* globals for cmd line opts */
delta_t      aTime = {0}, cTime = {0}, mTime = {0};
int          sFlag = 0, uFlag = 0, lFlag = 0, IFlag = 0;
char*        prefix = NULL;
char*        refFile = NULL;
struct stat  refStats;
tagNumT      tags[MAXUSRTAGS];
int          iNum = 0;
time_t       curTime;
int          printed_something;
tagNumT      ftw_tagnum;

/* function prototypes */
void usage(
);

int checkval(
    char*     valStr,
    delta_t*  delta
);

int check_time(
    time_t        refTime,
    time_t        fileTime,
    int           refDays,
    deltaType_t   type
);

void do_print(
    char*          fileName,
    tagNumT        in_tag,
    struct stat    statBuf
);

int ff_do_ftw(
    const char *obj_path,
    const struct stat *obj_stat,
    int obj_flags,
    struct FTW obj_FTW
);

void check_and_print(
    bfTagT      tag,
    char*       mountdir,
    bfSetIdT    setId
);

int fst2name(
    arg_infoT* infop,
    char* mntdir
);

#define MSGSTR(n,s) catgets(catd,MS_FF,n,s)


/******************************************************************************/
void usage()
{
    char *errstr;

    errstr = MSGSTR(USAGE, "usage: %s [-F advfs] [-a num] [-c num] [-VIlsu] [-i tag-list]");
    fprintf(stderr, errstr, Prog);
    errstr = MSGSTR(USAGE2, "          [-m num] [-n file] [-p prefix] {special | fsname} ...\n");
    fprintf(stderr, errstr);

    exit(1);
}

/******************************************************************************/
main(int argc, char *argv[])
{
    char          ch;
    int           st, i;
    int           VFlag = 0;
    int           FFlag = 0;
    extern int    optind;
    extern char*  optarg;
    char*         tmpArg = NULL;
    char*         tmpVal = NULL;
    char*         errstr;
    int           exit_status = 0;
    char*         badchars;

    (void)setlocale(LC_ALL, "");
    catd = catopen(MF_FF_ADVFS, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    Prog = basename(argv[0]);

    /* check for root */
    if ( geteuid() ) {
        errstr = MSGSTR(NOPERM, "%s: Permission denied - user must be privileged to run %s.\n");
        fprintf(stderr, errstr, Prog);
        usage();
        exit(1);
    }

    /*  Initialize the time checking structs to invalid values */
    aTime.deltaVal = -1;
    cTime.deltaVal = -1;
    mTime.deltaVal = -1;

    /*  curTime is time of program invocation */
    curTime = time(NULL);

    while ((ch = getopt(argc, argv, "F:a:c:i:m:n:p:VIlsu")) != EOF) {
        switch(ch) {
            case 'F':
                if (strcasecmp(optarg, MNTTYPE_ADVFS)) {
                    usage();
                    break;
                }
                FFlag++;
                break;
            case 'a':
                st = checkval(optarg, &aTime);
                if (st == -1) {
                    errstr = MSGSTR(NOTNUM, "%s: Argument to option %s must be a number\n");
                    fprintf(stderr, errstr, Prog, ch);
                    exit(1);
                }
                break;
            case 'c':
                st = checkval(optarg, &cTime);
                if (st == -1) {
                    errstr = MSGSTR(NOTNUM, "%s: Argument to option %s must be a number\n");
                    fprintf(stderr, errstr, Prog, ch);
                    exit(1);
                }
                break;
            case 'i':
                if (optarg == NULL) {
                    errstr = MSGSTR(NOTNUM, "%s: Argument to option %s must be a number\n");
                    fprintf(stderr, errstr, Prog, ch);
                    exit(1);
                }

                tmpArg = strtok(optarg, ",");

                for (iNum = 0; iNum < MAXUSRTAGS, tmpArg != NULL; iNum++) {
                    tags[iNum] = strtoul(tmpArg, &badchars, 10);
                    if (errno != 0 || *badchars != '\0') {
                        /* bad user input for tagnum, ignore it */
                        errstr = MSGSTR(NOTNUM,
                            "%s: Argument to option %s must be a number\n");
                        fprintf(stderr, errstr, Prog, ch);
                        iNum--;
                    }
                    tmpArg = strtok(NULL, ",");
                }

                if (iNum == 0) {  /* no valid tags found */
                    exit(1);
                }

                break;
            case 'm':
                st = checkval(optarg, &mTime);
                if (st == -1) {
                    errstr = MSGSTR(NOTNUM, "%s: Argument to option %s must be a number\n");
                    fprintf(stderr, errstr, Prog, ch);
                    exit(1);
                }
                break;
            case 'n':
                refFile = optarg;
                break;
            case 'p':
                prefix = optarg;
                break;
            case 'V':
                VFlag++;
                break;  
            case 'I':
                IFlag++;
                break;
            case 'l':
                lFlag++;
                break;
            case 's':
                sFlag++;
                break;
            case 'u':
                uFlag++;
                break;
            default:
                usage();
                break;  /* notreached */
        }
    }

    if ((argc - optind) < 1) {  /* what, no filesystems? */
        usage();
    }

    /*  set up reference file information, if requested */
    if (refFile) {
        i = stat(refFile, &refStats);
        if (i != 0) {
            st = errno;
            errstr = MSGSTR(ERRSTAT, "%s: stat(%s) error; %s\n");
            fprintf(stderr, errstr, Prog, refFile, strerror(st)); 
            exit(1);
        }
    }

    if (VFlag) {
        if (!FFlag) {
            fprintf(stdout, "%s -F advfs ", Prog);
        } else {
            fprintf(stdout, "%s ", Prog);
        }

        for (st = 1; st < argc ; st++) {
            if (strcmp(argv[st], "-V") == 0) {
                continue;
            }
            fprintf(stdout, "%s ", argv[st]);
        }
        fprintf(stdout, "\n");
        exit(0);
    }

    argc -= optind;
    argv += optind;


    while (argc > 0) {
        char*        file = argv[argc-1];
        int          found;
        arg_infoT*   infop = NULL;
        char*        mntdir;

        infop = process_advfs_arg(file);

        if (infop == NULL) {
            fprintf(stderr, MSGSTR(FSNERR, "%s: error processing specified device or filesystem name %s\n"), Prog, file);
            argc--;
            exit_status = ERR_BAD_FS;  
            continue;
        }

        /*  check that the filesystem is mounted */
        found = advfs_is_mounted(infop, NULL, NULL, &mntdir);

        switch(found) {
            case 1:  
                st = fst2name(infop, mntdir);
                if (st != 0) {
                    exit_status = st;
                }
                free(mntdir);
                break;
            case 2:
                errstr = MSGSTR(NOTMS, "%s: Filesystem %s is not AdvFS\n");
                fprintf(stderr, errstr, Prog, file);
                free(mntdir);
                exit_status = ERR_BAD_FS;
                break;
            default:
                fprintf(stderr, MSGSTR(BADID, "%s: Could not get filesystem id for %s\n"), Prog, file);
                exit_status = ERR_BAD_FS;
                break;
        }

        argc--;
        free(infop);
    }

    exit(exit_status);

}

/******************************************************************************/
int checkval(
    char*     valStr,
    delta_t*  delta
) 
{
    int    retval;
    char*  badchars;

    if (valStr == NULL) {
        return -1;
    }

    if (valStr[0] == '+') {   
        valStr++;
        delta->deltaType = OLDER;
    } else if (valStr[0] == '-') {
        valStr++;
        delta->deltaType = NEWER;
    } else {
        delta->deltaType = EXACT;
    }

    delta->deltaVal = strtol(valStr, &badchars, 10);
    if (errno != 0 || *badchars != '\0') {
        return(-1);
    }

    if (delta->deltaVal == 0) {
        /*  assume a value of 0 means "older than now" */
        delta->deltaType = OLDER;
    }

    return 0;
}
         

/******************************************************************************/
int check_time(
    time_t        refTime,
    time_t        fileTime,
    int           refDays,
    deltaType_t   type
)
{
    int     retval = 1;

    switch (type) {
        case OLDER:
            if ( (refTime - fileTime) > refDays * DAY ) {
                retval = 0;
            }
            break;
        case NEWER:
            if ( (refTime - fileTime) < refDays * DAY ) {
                retval = 0;
            }
            break;
        case EXACT:
            if (labs(refTime - fileTime) / DAY == refDays) {
                retval = 0;
            }
            break;
        default:
            break;
    }

    return retval;
}

void do_print(
    char*          fileName,
    tagNumT        in_tag,
    struct stat    statBuf
)
{
    printed_something = 1;

    if (prefix) {
        fprintf(stdout, "%s", prefix);
    } else {
        /* default prefix is "." */
        fprintf(stdout, ".");
    }

    fprintf(stdout, "%s", fileName);

    if (!IFlag) {
        fprintf(stdout, "\t%ld", in_tag);
    }
    if (sFlag) {
        fprintf(stdout, "\t%ld", statBuf.st_size);
    }
    if (uFlag) {
        struct passwd*     pwd;

        pwd = getpwuid(statBuf.st_uid);

        if (pwd) {
            fprintf(stdout, "\t%s", pwd->pw_name);
        }
    }
    fprintf(stdout, "\n");

    return;
}

int ff_do_ftw(
    const char *obj_path,
    const struct stat *obj_stat,
    int obj_flags,
    struct FTW obj_FTW
)
{

    /*  only pass through regular files that match the tag we're 
     *  looking for 
     */
    if ((obj_flags == FTW_F) && obj_stat->st_ino == ftw_tagnum) {
       do_print(obj_path, ftw_tagnum, *obj_stat);
    }

    return(0);
}

/******************************************************************************/
void check_and_print(
    bfTagT      tag,
    char*       mountdir,
    bfSetIdT    setId
)
{
    adv_status_t   sts;
    int            ret, i;
    char           fileName[MAXPATHLEN+1];
    struct stat    statBuf;
    int            doPrint = 1;
    time_t         refTime = curTime;
    
    sts = tag_to_path(mountdir, tag, sizeof(fileName), &fileName[0]);
    
    if (sts != EOK) {
        return;
    }

    ret = stat(fileName, &statBuf);
    if (ret != 0) {
        return;
    }

    if (aTime.deltaVal != -1) {
        ret = check_time(curTime, statBuf.st_atime, aTime.deltaVal, 
            aTime.deltaType);
        if (ret) {
            return;
        }
    }

    if (cTime.deltaVal != -1) {
        ret = check_time(curTime, statBuf.st_ctime, cTime.deltaVal, 
            cTime.deltaType);
        if (ret) {
            return;
        }
    }

    if (mTime.deltaVal != -1) {
        ret = check_time(curTime, statBuf.st_mtime, mTime.deltaVal, 
            mTime.deltaType);
        if (ret) {
            return;
        }
    }

    if (refFile) {
        ret = check_time(refStats.st_mtime, statBuf.st_mtime, 0, NEWER);
        if (ret) {
            return;
        }
    }

    /*  ok, that's all the conditions met.  If we've got -l, then we 
     *  need to find all files in the filesystem that are hardlinks to 
     *  the tag we just looked up.  The ff_to_ftw() function will print
     *  all the files with that tagnum.
     */
    if (lFlag && (S_ISREG(statBuf.st_mode)) && (statBuf.st_nlink > 1)) {
        ftw_tagnum = tag.tag_num;
        nftw(mountdir, ff_do_ftw, OPEN_MAX, FTW_PHYS|FTW_MOUNT);
    } else {
        do_print(fileName, tag.tag_num, statBuf);
    }

    return;
}

/******************************************************************************/

int fst2name(
    arg_infoT* infop,
    char* mntdir
)
{
    adv_status_t   sts;
    struct stat    rootStats; /* file set's root stats */
    char           *errstr;
    char           tagDir[MAXPATHLEN+1] = {0};
    bfTagT         tag;
    bfSetIdT       setId;
    adv_uid_t      uid;
    adv_gid_t      gid;
    adv_off_t      size;
    adv_time_t     atime;
    adv_mode_t     mode;
    int            i;
    int            ret_stat = 0;



    /*  The tags directory is "mntdir/.tags", and must have inode of "3" */
    if (strlen(mntdir) > MAXPATHLEN-7) {
        errstr = MSGSTR(BADPATH, "%s: Pathname %s is too long\n");
        fprintf(stderr, errstr, Prog, mntdir);
        return(1);
    }

    strcpy(tagDir, mntdir);
    strcat(tagDir, "/.tags");
    
    if ( stat(tagDir, &rootStats) < 0 ) {
        errstr = MSGSTR(ERRSTAT, "%s: stat(%s) error; %s\n");
        fprintf(stderr, errstr, Prog, tagDir, BSERRMSG(errno));
        return(2);
    }

    if ( rootStats.st_ino != 3 ) {
        errstr = MSGSTR(ETAGDIR, "%s: %s is not the special .tags directory.\n");
        fprintf(stderr, errstr, Prog, tagDir);
        return(2);
    }

    if (advfs_fset_get_id(infop->fsname, infop->fset, &setId) != EOK) {
        /* print error */
        errstr = MSGSTR(BADID, "%s: Could not get filesystem id for %s\n");
        fprintf(stderr, errstr, Prog, infop->fsname);
        return(ERR_BAD_FS);
    }

    tag.tag_num = 0;
    tag.tag_seq = 0;

    if (iNum == 0) {             /* no tags on command line */
        while ((sts = advfs_tag_stat(&tag, setId, &uid, &gid,
                          &size, &atime, &mode)) == EOK) {
            if (BS_BFTAG_RSVD(tag)) {
                continue;
            }
            check_and_print(tag, mntdir, setId);
        }
    } else { 

        for ( i = 0 ; i < iNum ; i++ ) {
            printed_something = 0;
            tag.tag_num = tags[i];
            tag.tag_seq = 0;

            if (BS_BFTAG_RSVD(tag)) {
                continue;
            }
            check_and_print(tag, mntdir, setId);

            if (printed_something == 0) {
                /*  the passed-in tag was invalid, error */
                errstr = MSGSTR(NOTAG, 
                    "%s: Requested tag %ld not found\n");
                fprintf(stderr, errstr, Prog, tags[i]);
    
                ret_stat = 1;
            }
        }
    }

    return(ret_stat);
}
