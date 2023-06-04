/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 *      View On-Disk Structure
 *
 * Date:
 *
 *      Thu Nov 17 09:06:33 PST 1994
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vods.c,v $ $Revision: 1.1.10.4 $ (DEC) $Date: 2006/08/30 15:25:50 $"

#include <stdio.h>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>
#include <overlap.h>
#include <sys/ioctl.h>       /* for get_vol_blocks */
#include <sys/disklabel.h>   /* for get_vol_blocks */
#include <io/common/devgetinfo.h>
#include <errno.h>

#include <msfs/bs_public.h>
#include <msfs/fs_dir.h>
#include "vods.h"

/* from msfs/bs/ms_privates.c */
bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};

#define IS_RBMT (bfp->dmn->dmnVers > 3 && bfp->fileTag % BFM_RSVD_TAGS == BFM_RBMT)
#define PRINT_BMT_OR_RBMT (IS_RBMT ? "RBMT" : "BMT")

/* 
 * Global domain version; filled in by open_vol().
 */
int dmnVersion = -1;

/* TODO. provide check_page() routine to check page against bfp->pages */

#ifdef _VODS_NOPRINTS
#undef assert
#define assert(x)  {};
#define fprintf no_fprintf
#define perror  no_perror
int no_fprintf(FILE *stream, const char *format, ...) {return(0);}
void no_perror(const char *s) {}
#endif


void init_bitfile(bitFileT*);
void newbf(bitFileT*, bitFileT*);
int resolve_src(int, char**, int*, char*, bitFileT*, int*);
static int getopts(int, char* const*, char const*);
static int init_dmn(bitFileT*);
static int test_dmn(char*, char*);
static int open_dmn(char*, bitFileT*, char*, int);
static int mkraw(const char*, char*, dev_t);
static int make_n_test_devname(const char*, char*);
static void check_vol(bitFileT*, vdIndexT, bfDomainIdT*, int*);
static int open_vol(bitFileT*, char*, vdIndexT*);
static int test_advfs_magic_number(int, char*);
static void get_vol_blocks(volT*);
static int is_lsm(volT*);
static void read_disk_label(volT*);
static int find_vdi(bitFileT*, bsMPgT*, char*, vdIndexT*);
static int load_rbmt_xtnts(bitFileT*);
static int load_V3bmt_xtnts(bitFileT*);
static int read_vol_at_blk(bitFileT*);
int load_xtnts(bitFileT*);
static int get_stripe_parms(bitFileT*, int*, int*);
static int get_stripe_xtnts(bitFileT*, vdIndexT, bfMCIdT, int);
static void un_stripe(bitFileT*, segMapT*);
static int load_clone_xtnts(bitFileT*);
static void merge_clone_xtnts(bitFileT*, bitFileT*);
static int get_bsr_xtnts(bitFileT*, int*, vdIndexT*, bfMCIdT*, int*);
static int load_bsr_xtnts(bitFileT*, int*, bsMCT*, vdIndexT*, bfMCIdT*, int*);
static int get_shadow_xtnts(bitFileT*, int*, vdIndexT*, bfMCIdT*, int*);
static int get_xtra_xtnts(bitFileT*, int, vdIndexT*, bfMCIdT*, int, int);
static int load_xtra_xtnts(bitFileT*, int*, bsMCT*, vdIndexT*, bfMCIdT*, int);
static void get_xtnts(bitFileT*, int*, vdIndexT, bsXtntT*, int);
static int resolve_vol(bitFileT*, char*);
static int resolve_fle(bitFileT*, char*, struct stat*);
static int open_savset(char*, bitFileT*, int);
static int open_savset_vol(bitFileT*, char*);
int dumpBitFile(bitFileT*, char*);
int find_TAG(bitFileT*);
static int find_saveset_TAG(bitFileT*);
int get_set_n_file(bitFileT*, char**, int, int*);
int get_set(bitFileT*, char**, int, int*, bitFileT*);
static int find_set_by_name(bitFileT*, char*, bitFileT*);
int get_max_tag_pages(bitFileT*, int*);
int read_tag_page(bitFileT*, int, bsTDirPgT**);
static int get_orig_set(bitFileT*, int, bitFileT*);
int get_file(bitFileT*, char**, int, int*, bitFileT*);
int get_file_by_tag(bitFileT*, int, bitFileT*);
static int get_prim_from_tagdir(bitFileT*, bfTagT*, bitFileT*);
static int find_file_by_name(bitFileT*, char*, bitFileT*);
static int find_prim_from_dot_tags(char*, bitFileT*, bitFileT*, bitFileT*);
static int find_rsrv_prim_from_tag(bitFileT*, bfTagT);
int get_chain_mcell(bitFileT*, vdIndexT*, bfMCIdT*);
int get_next_mcell(bitFileT*, vdIndexT*, bfMCIdT*);
static int search_dir(bitFileT*, bitFileT*, char*);
static int get_savset_file(bitFileT*, char*);
int find_rec(bitFileT*, int, char**);
int find_bmtr(bsMCT*, int, char**);
int getnum(char*, long*);
void print_header(bitFileT*);
void print_unknown(char*, int);
int page_mapped(bitFileT*, int);
int read_page(bitFileT*, int);
static int test_bmt_page(bitFileT*, char*, int);
static int test_mcell(bitFileT*, char*, vdIndexT, bfMCIdT, bsMCT*);
static int test_mcid(bitFileT*, char*, vdIndexT, bfMCIdT);
static int test_vdi(bitFileT*, char*, vdIndexT);
static int test_blk(bitFileT*, vdIndexT, uint32T);

/*****************************************************************************/
/*
** Input: bitfile pointer
** Output: bitfile is initialized
**         pointers to malloced space are nulled (dmn, xmap.xtnt)
**         type set to allow all types
**         other fields set to illegal values
** Errors: no errors
** Return: none
*/
void
init_bitfile(bitFileT *bfp)
{
    assert(bfp);

    bfp->type = FLE | DMN | VOL | SAV;
    bfp->fileName[0] = '\0';
    bfp->setName[0] = '\0';
    bfp->pvol = 0;
    bfp->ppage = -1;
    bfp->pcell = -1;
    bfp->pages = -1;
    bfp->fileTag = 0;
    bfp->setTag = 0;
    bfp->setp = NULL;
    bfp->origSetp = NULL;
    bfp->dmn = NULL;
    bfp->xmap.cnt = 0;
    bfp->xmap.xtnt = NULL;
    bfp->fd = -1;
    bfp->pgLbn = XTNT_TERM;
    bfp->pgNum = -1;
    bfp->pgVol = 0;
    bfp->pgBuf = NULL;
    bfp->next = NULL;
}

/*****************************************************************************/
/*
** Copies some of the contents of a bitFile to another bitFile.
**
** Input: srcBfp is set up. type, dmn & setTag have been set up
** Output: destBfp gets srcBfp type, dmn & setTag. other values are initialized
** Errors: no errors
** Return: none
*/
void
newbf(bitFileT *srcBfp, bitFileT *destBfp)
{
    assert(srcBfp);
    assert(destBfp);
    assert(srcBfp->type != 0);
    assert(srcBfp->dmn);

    init_bitfile(destBfp);

    destBfp->type = srcBfp->type;
    destBfp->dmn = srcBfp->dmn;
    destBfp->setTag = srcBfp->setTag;
}

/******************************************************************************/
/*
** Looks at the first few option flags and resolves the source.
** The source is a domain or volume or saved file.
**
** Input: bfp - initialized
**        argv, argc - command line arguments
**        optindp - pointer to argv index
**        optstr - string of accepted options
** Outout: bfp->type - DMN, VOL, FLE
**         bfp->domain - filled in if source is a domain
**         flagp - set for options flags found  VFLG, RBMT, FFLG, BFLG
** Errors: prints and exist for format errors
**         prints error for all errors
** Return: OK - proper structure for domain, volume, file
**         ENOENT - no such domain, volume, file
**         EACCES - can't access "name" due to permissions
**         EBUSY - block device is mounted
**         BAD_DISK_VALUE - bad magic or other values not as expected
*/
int
resolve_src(int argc, char **argv, int *optindp, char *optstr, bitFileT *bfp,
            int *flagp)
{
    int ret;
    char c;
    int typeFlg = 0;
    int rawFlg = 0;
    char *name = NULL;
    long longnum;

    while ( (c = getopts( argc, argv, optstr)) != EOF ) {
        switch ( c ) {
          case 'D':	/* next argument is a domain name */
            if ( typeFlg ) {
                fprintf(stderr,
                  "Illegal -D flag. %s \"%s\" already specified. \n",
                  typeFlg == DMN ? "Domain" :
                                    typeFlg == VOL ? "Volume" : "File", name);
                exit(1);
            }

            typeFlg = DMN;
            if ( optarg == NULL ) {
                fprintf(stderr, "Missing domain name\n");
                usage();
            }

            name = optarg;
            bfp->type = DMN;
            ret = resolve_name(bfp, name, rawFlg);
            if ( ret != OK && ret != BAD_DISK_VALUE ) {
                exit(ret);
            }

            break;

          case 'V':		/* next argument is a volume name */
            if ( typeFlg && typeFlg != VOL ) {
                fprintf(stderr,
                  "Illegal -V flag. %s \"%s\" already specified. \n",
                  typeFlg == DMN ? "Domain" : "File", name);
                exit(1);
            }
            if ( typeFlg ) {	/* temp. later allow multiple -V volume */
                fprintf(stderr,
                  "Illegal -V flag. Volume \"%s\" already specified. \n", name);
                exit(1);
            }

            typeFlg = VOL;
            if ( optarg == NULL ) {
                fprintf(stderr, "Missing volume name\n");
                usage();
            }

            name = optarg;
            bfp->type = VOL;
            ret = resolve_name(bfp, name, 0);
            if ( ret != OK && ret != BAD_DISK_VALUE ) {
                exit(ret);
            }

            break;

          case 'F':		/* next argument is a saved file or saved set */
            if ( typeFlg && typeFlg != FLE ) {
                fprintf(stderr,
                  "Illegal -F flag. %s \"%s\" already specified. \n",
                  typeFlg == DMN ? "Domain" : "Volume", name);
                exit(1);
            }
            if ( typeFlg ) {	/* temp. later allow multiple -V volume */
                fprintf(stderr,
                  "Illegal -F flag. File \"%s\" already specified. \n", name);
                exit(1);
            }

            typeFlg = FLE;
            if ( optarg == NULL ) {
                fprintf(stderr, "Missing file name\n");
                usage();
            }

            name = optarg;
            bfp->type = FLE;
            ret = resolve_name(bfp, name, 0);
            if ( ret != OK && ret != BAD_DISK_VALUE ) {
                exit(ret);
            }
            break;

          case 'v':		/* verbose */
            *flagp |= VFLG;	/* print all information in the mcells */
            break;

          case 'r':		/* raw */
            rawFlg = 1;
            break;

          case 'R':
            *flagp |= RBMT;
            break;

          case 'f':
            *flagp |= FFLG;
            break;

          case 'B':             /* brief output */
            *flagp |= BFLG;
            break;

          default:
            usage();
        }
    }

    if ( name == NULL ) {
        if ( *optindp >= argc ) {
            fprintf(stderr, "Domain, volume or file name required.\n");
            usage();
        }

        if ( argv[*optindp][0] == '-' ) {
            if ( strlen(argv[*optindp]) == 2 ) {
                fprintf(stderr, "Unknown option \"%s\"\n", argv[*optindp]);
            } else {
                fprintf(stderr, "Unknown option flag '%c' in \"%s\"\n",
                  optopt, argv[*optindp]);
            }
            usage();
        }

        name = argv[*optindp];
        (*optindp)++;

        ret = resolve_name(bfp, name, rawFlg);
    }

    /* Check ret from resolve_name(). Accessability errors are caught here. */
    /* AdvFS volume format errors are caught after allowing the -b option. */
    if ( ret != OK && ret != BAD_DISK_VALUE ) {
        exit(ret);
    }

    if ( !(bfp->type & DMN) && *flagp & RFLG ) {
        fprintf(stderr, "-r flag ignored unless a domain is specified\n");
    }

    /* Check if a volume index was supplied with the domain name.
     * If so, process that volume only.
     */
    if ( bfp->type & DMN &&
         argc > *optindp &&
         (bfp->fileTag == RSVD_TAGNUM_GEN(1, BFM_RBMT) ||
          bfp->fileTag == RSVD_TAGNUM_GEN(1, BFM_SBM)) &&
         getnum(argv[*optindp], &longnum) == OK )
    {
        /* For one-per-volume files (RBMT, BMT, SBM), if the source is a */
        /* domain, the next argument is a  volume index or a fileset. */
        /* If it is a number, assume it is a volume index. */

        /* FIX. assert(vdi == 0);  /* just checking */
        (*optindp)++;
        bfp->pvol = longnum;

        /* FIX. test_vdi(bfp, bfp->pvol); */
        if ( bfp->pvol >= BS_MAX_VDI || longnum == 0 ) {
            fprintf(stderr, "Invalid vdi (%d). ", longnum);
            fprintf(stderr, "Valid range is 1 to %d\n", BS_MAX_VDI);
            exit(BAD_PARAM);
        }

        assert(bfp->dmn);
        if ( bfp->dmn->vols[bfp->pvol] == NULL ) {
            fprintf(stderr, "No volume %d in domain \"%s\".\n",
              bfp->pvol, name);
            exit(BAD_PARAM);
        }

        bfp->fd = bfp->dmn->vols[bfp->pvol]->fd; 
        bfp->type |= VOL;
    } else {
        /* For one-per-domain files (TAG, LOG), if the source is a domain, */
        /* the next arg is a vdindex ONLY if the following arg is -b. */
        /* In that case we are indentifying the volume to look at raw blocks. */
        /* Otherwise a number after the domain name identifies a page number. */
        if ( bfp->type & DMN &&
             argc > optind + 1 &&
             strncmp(argv[optind + 1], "-b", 2) == 0 )
        {
            if ( getnum(argv[optind], &longnum) == OK ) {
                bfp->pvol = longnum;
                optind++;
            } else {
                fprintf(stderr, "Badly formed vdi \"%s\".\n", argv[optind]);
                usage();
            }

            if ( bfp->pvol >= BS_MAX_VDI ) {
                fprintf(stderr, "Invalid vdi (%d). ", longnum);
                fprintf(stderr, "Valid index from 1 to %d.\n", BS_MAX_VDI);
                exit(BAD_PARAM);
            }

            if ( bfp->dmn->vols[bfp->pvol] ) {
                bfp->fd = bfp->dmn->vols[bfp->pvol]->fd;
            } else {
                fprintf(stderr, "Invalid vdi (%d). No such volume.\n",
                  bfp->pvol);
                exit(BAD_PARAM);
            }
            bfp->type |= VOL;
        }
    }


    /* "-b" must work even on non AdvFS volumes. */
    if ( bfp->type & VOL &&
         argc > *optindp && strncmp(argv[*optindp], "-b", 2) == 0 )
    {
        return ret;
    }

    /* Test the BMT file */
    if ( bfp->type & DMN &&
         argc > optind && strcmp(argv[optind], "-X") == 0 )
    {
        return ret;
    }

    /* Again check ret from resolve_name() */
    /* At this point if the volume serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    return OK;
}

/*****************************************************************************/
/*
** Match as many options as possible then return EOF
**
** Input: argv, argc - command line options
**        optstring - string of valid options
**        global optind - index to argv
** Output: global optarg - points to required option
**         global optind - may be incremented
**         global opopt - option character found
** Error: no printf
** Return: the option found
**         EOF - no more recognized options
*/
static int
getopts( int argc, char * const *argv, char const *optstring)
{
    static int stringind = 1;
    int c;
    char *cp;

    /* if no more args or no more - options or dash w/o option then EOF */
    if ( stringind == 1 ) {
        if ( optind >= argc ||
             argv[optind] == NULL ||
             argv[optind][0] != '-' ||
             argv[optind][1] == '\0')
        {
            return EOF;
        }
    }

    optopt = c = argv[optind][stringind];
    if ( (cp = strchr(optstring, c)) == NULL ) {
        /* check for unrecognized options */
        stringind = 1;
        return EOF;
    }

    if ( *++cp == ':' ) {
        /* Option requires a parameter.  There may or may not be spaces */
        /* between the option and the parameter. */
        if ( argv[optind][stringind+1] != '\0' )
            /* no blanks separate option and parameter */
            optarg = (char*)&argv[optind++][stringind+1];
        else if ( ++optind >= argc ) {
            /* No option but one is required. Set optarg to null. */
            optarg = NULL;
        } else {
            optarg = (char*)argv[optind++];
        }
        stringind = 1;
    } else if ( *cp == ';' ) {
        /* Option may have an optional parameter. */
        /* Parameter (if it exists) follows option with no spaces. */
        if ( argv[optind][stringind+1] != '\0' ) {
            optarg = (char*)&argv[optind][stringind+1];
        } else {
            optarg = NULL;
        }
        stringind = 1;
        optind++;
    } else {                        /* parameter not needed */
        /* if c is the last option update optind */
        if (argv[optind][++stringind] == '\0') {
            stringind = 1;
            optind++;
        }
        optarg = NULL;
    }

    return c;
}

/*****************************************************************************/
/*
** Malloc and init the dmn element of a bitFileT structure.
**
** Input: bfp = the bitFileT in which to init the dmn element
** Output: bfp->dmn malloced and its elements initialized
** Errors: diagnostic output to stderr for all errors
** Return: OK - all went well
**         ENOMEM - malloc failed
*/
static int
init_dmn(bitFileT *bfp)
{
    int i;

    assert(bfp->dmn == NULL);

    bfp->dmn = malloc(sizeof(DmnT));
    if ( bfp->dmn == NULL ) {
        perror("init_dmn: malloc failed");
        return ENOMEM;
    }

    bfp->dmn->dmnVers = 0;
    bfp->dmn->dmnName[0] = '\0';
    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        bfp->dmn->vols[i] = NULL;
    }

    return OK;
}

/*****************************************************************************/
/*
** This routine decides if "name" is a valid domain or a volume
** name or a file. Domain names are the directories at /etc/fdmns.
** All vods utilities call resolve_name early on to decide if
** they are looking at a domain, volume or file (or save set).
**
** If the name is a domain, resolve_name will malloc and fill in
** bfp->dmn and bfp->dmn->vols[] for all volumes. bfp->type will be set to DMN.
** The bmt extents and rbmt extents (if present) will be read for all volumes.
**
** If the name is a volume, resolve_name will malloc and fill in
** bfp->dmn and bfp->dmn->vols[] for the one volume. The index will
** be the vdi read from the volume. If the volume isn't a good
** AdvFS volume, the vdi will be 0. bfp->type will be set to VOL.
**
** If the name is a file, resolve_name does not malloc bfp->dmn.
** The fd is set to the fd of the file.
**
** The calling sequence is:
**     resolve_name => open_dmn => open_vol
**     resolve_name => resolve_vol => open_vol
**     resolve_name => resolve_fle
**
** Input: bfp->type is allowed type of name (DMN, VOL, FLE or SAV)
**        "name" is domain, volume, file or directory
**        rawflag - open volumes in domain as character devices
** Outout: bfp->type
**         if DMN, malloc bfp->dmn. malloc bfp->dmn->vols[] for all volumes
**                 open all volumes. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[]->fd, blocks, 
**                 malloc & init bfp->dmn->vols[]->rbmt
**                 malloc and fill bfp->dmn->vols[]->rbmt->xmap
**         if VOL, malloc bfp->dmn. malloc bfp->dmn->vols[] for one volume
**                 open volume. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[vdi]->fd, blocks, 
**         if FLE, bfp->fd, pages
** Errors: diagnostic output to stderr for all errors
** Return: OK
**         ENOENT - no such domain, volume, file
**         EACCES - can't access "name" due to permissions
**         EBUSY - block device is mounted
**         BAD_DISK_VALUE - bad magic or other values not as expected
*/
int
resolve_name(bitFileT *bfp, char *name, int rawflg)
{
    int  fd;
    int stat_ret;
    struct stat  statbuf;
    char devname[MAXPATHLEN];
    char dirname[MAXPATHLEN];
    int ret;

    stat_ret = stat(name, &statbuf);

    /*-----------------------------------------------------------------------*/
    /* first choice is as a domain */
    if ( bfp->type & DMN ) {
        ret = test_dmn(dirname, name);
        if ( ret == EACCES ) {
            fprintf(stderr, "resolve_name: Can't open \"%s\".\n", name);
            return EACCES;
        }
        if ( ret == OK ) {
            /* load all domain volumes */
            bfp->type = DMN;
            return open_dmn(dirname, bfp, name, rawflg);
        }
        if ( bfp->type == DMN ) {
            fprintf(stderr, "No such domain: \"%s\"\n", name);
            return ENOENT;
        }
    }

    /*-----------------------------------------------------------------------*/
    /* second choice: try to open a volume */
    if ( bfp->type & VOL ) {
        if ( stat_ret == -1 ) {
            /* file not in this dir. look in /dev */
            if ( make_n_test_devname(name, devname) == OK ) {
                return resolve_vol(bfp, devname);
            }
        } else if ( S_ISBLK(statbuf.st_mode) ) {
            strcpy(devname, name);
            return resolve_vol(bfp, devname);
        } else if ( S_ISCHR(statbuf.st_mode) ) {
            strcpy(devname, name);
            return resolve_vol(bfp, devname);
        }

        if ( bfp->type == VOL ) {
            fprintf(stderr, "No such volume: \"%s\"\n", name);
            return ENOENT;
        }
    }

    /*-----------------------------------------------------------------------*/
    /* third choice is to interpret 'name' as a file, however it must be REG */
    if ( bfp->type & FLE ) {
        if ( stat_ret != -1 && S_ISREG(statbuf.st_mode) ) {
            return resolve_fle(bfp, name, &statbuf);
        }

        if ( bfp->type == FLE ) {
            fprintf(stderr, "No such file: \"%s\"\n", name);
            return ENOENT;
        }
    }

    /*-----------------------------------------------------------------------*/
    /* forth choice is to interpret 'name' as a save set, must be DIR */
#ifdef NOTYET
    if ( bfp->type & SAV )
#else
    if ( FALSE )
#endif
    {
        if ( stat_ret != -1 && S_ISDIR(statbuf.st_mode) ) {
            /* load all domain volumes */
            if ( open_savset(name, bfp, rawflg) != OK ) {
                return ENOENT;
            }
            bfp->type = SAV;
            strcpy(bfp->dmn->dmnName, name);    /* check for NULL reseults */
            if ( bfp->pvol != 0 ) {
                bfp->type |= VOL;
            }
            return OK;
        }

        if ( bfp->type == SAV ) {
            fprintf(stderr, "bad save dir name %s\n", name);
            return ENOENT;
        }
    }

    bfp->type = 0;
    if ( stat_ret != -1 && !S_ISREG(statbuf.st_mode) ) {
        fprintf(stderr, "\"%s\" is not a regular file.\n", name);
    } else {
        fprintf(stderr, "No such file, domain or volume: \"%s\"\n", name);
    }

    return ENOENT;
}

/*****************************************************************************/
/*
** Test that "name" is simple (no '/').
** Test that name is a direstory in /etc/fdmns.
**
** Input: name - name of domain
** Output: dirname - full path to /etc/fdmn/<domain_name>
** Errors: status return only. No diagnostic printf
** Return: OK if "name" is a directory in /etc/fdmns
**         ENOENT - no such domain found
**         EACCES - can't access domain
*/

#define DMN_DIR "/etc/fdmns/"

static int
test_dmn(char *dirname, char *name)
{
    struct stat statBuf;

    /* no '/' allowed in a domain name */
    if ( strchr(name, '/') != NULL ) {
        return ENOENT;
    }

    strcpy(dirname, DMN_DIR);
    strcat(dirname, name);
    if ( stat(dirname, &statBuf) == -1 ) {
        if ( errno == EACCES ) {
            return EACCES;
        } else {
            return ENOENT;
        }
    }

    if ( !S_ISDIR(statBuf.st_mode) ) {
        return ENOENT;
    }

    return OK;
}

/*****************************************************************************/
/*
** Open /etc/fdmns/<dmnName> and read all the contents.
** Malloc the bfp->dmn structure. For each block device, open the volume
** and fill in bfp->dmn->vols[vdi]. If the rwflg is set open the character
** device for each volume instead.
** open_dmn is called by resolve_name and calls open_vol.
**
** Input: dmnDir full path name to /etc/fdmns/<domain_name>
** Input: dmnName - last component of "dmnDir". The name of the domain
** Input: rawflg - if set, open character devices instead of block devices
** Output: bfp->fd set to -1			WHY
**         malloc bfp->dmn
**         malloc and fill in bfp->dmn->vols[]  for each volume
**         fill in bfp->dmn->dmnName
** Errors: diagnostic output to stderr for all errors
** Return: OK - if all goes well
**         EBUSY - block device is mounted
**         ENOENT - no such block device
**         ENOMEM - if malloc fails
**         BAD_DISK_VALUE - bad magic or other unexpected value
*/
static int
open_dmn(char *dmnDir, bitFileT *bfp, char *dmnName, int rawflg)
{
    DIR *dirStrm;
    struct dirent *dp;
    char *cp;
    char errstr[80];
    char dirname[MAXPATHLEN];   /* used to make /etc/fdmns/<dmn>/<vol> path */
    char devname[MAXPATHLEN];   /* used to make domain volume blk dev name */
    char rawname[MAXPATHLEN];   /* used to make domain volume char dev name */
    vdIndexT vdi;
    struct stat statBuf;
    dev_t dev;
    bfDomainIdT dmnId;
    int volCnt;
    int volsOpened;
    int ret;

    errstr[0] = '\0';       /* just in case sprintf fails */
    dmnId.tv_sec = 0;
    dmnId.tv_usec = 0;
    volCnt = 0;
    volsOpened = 0;

    bfp->fd = -1;
    ret = init_dmn(bfp);
    if ( ret != OK ) {      /* init_dmn can fail with ENOMEM */
        return ret;
    }

    strcpy(bfp->dmn->dmnName, dmnName);   /* FIX. check for NULL reseults */

    dirStrm = opendir(dmnDir);   /* open /etc/fdmns/<dmn> directory */
    if ( dirStrm == NULL ) {
        sprintf(errstr, "open_dmn: opendir \"%s\" failed", dmnDir);
        perror(errstr);
        return ENOENT;
    }

    /* Read the contents of the /etc/fdmns/<dmn> directory. */
    for ( dp = readdir(dirStrm); dp != NULL; dp = readdir(dirStrm) ) {

        /* Idgnore "." & "..", just keep reading. */
        if ( strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 ) {
            continue;
        }

        /* Construct the full path path name of each domain volume. */
        strcpy(dirname, dmnDir);
        strcat(dirname, "/");
        cp = &dirname[strlen(dirname)];
        *cp = '\0';
        strcat(dirname, dp->d_name);

        /* stat the dir entry. symbolic links stat thru to their target. */
        if ( stat(dirname, &statBuf) == -1 ) {
            closedir(dirStrm);
            sprintf(errstr, "open_dmn: stat failed for %s", dirname);
            perror(errstr);
            continue;
        }

        /* Ignore regular files, eg locks, just pay attention to block files. */
        if ( !S_ISBLK(statBuf.st_mode) ) {
            continue;
        }

        dev = statBuf.st_rdev;   /* used to compare to raw dev in mkraw */

        if ( lstat(dirname, &statBuf) == -1 ) {
            closedir(dirStrm);
            sprintf(errstr, "open_dmn: lstat failed for %s", dirname);
            perror(errstr);
            continue;
        }

        if ( !S_ISLNK(statBuf.st_mode) ) {
            fprintf(stderr,
     "WARNING: open_dmn: block special device \"%s\" in %s is not a symlink.\n",
              dirname, dmnDir);
            /* Even though this is not a symlink, use it as a volume. */
            strcpy(devname, dirname);
        } else {
            if ( readlink(dirname, devname, MAXPATHLEN - 1) == -1 ) {
                sprintf(errstr, "open_dmn: readlink on %s failed", dirname);
                perror(errstr);
                continue;
            }
        }

        if ( rawflg ) {
            mkraw(devname, rawname, dev);
            strcpy(devname, rawname);
        }

        ret = open_vol(bfp, devname, &vdi);
        if ( ret != OK ) {
            closedir(dirStrm);
            return ret;
        }

        volsOpened++;
        check_vol(bfp, vdi, &dmnId, &volCnt);
    }

    if ( volCnt == 0 ) {
        fprintf(stderr, "WARNING: Domain vdCnt not found\n");
    } else if ( volsOpened != volCnt ) {
        fprintf(stderr, "WARNING: Domain should have %d volumes. ", volCnt);
        fprintf(stderr, "%d opened. ", volsOpened);
    }

    closedir(dirStrm);

    return OK;
}

/*****************************************************************************/
/*
** Make raw (char) device from block device by adding an 'r',
** eg change /dev/rz5a to /dev/rrz5a.
** Test that raw device exists and is a character device.
** Test that the device number is the same as the block device.
** Return full path name of character device at rawname.
**
**    /dev/rz5c        => /dev/rrz5c
**    /dev/disk/dsk5c  => /dev/rdisk/dsk5c
**    /dev/vol/grp/dsk => /dev/rvol/grp/dsk
**
** Input: blkname - full path name of block device
**        dev - device number of block device.
** Output: rawname - full path name of char device
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         OUT_OF_RANGE if name can't be changed to raw device name
**         ENOENT if raw name doesn't exist or is not the correct device
*/
static int
mkraw(const char *blkname, char *rawname, dev_t dev)
{
    int i;
    char  *cp;
    char  *lastslash;
    struct stat  statBuf;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    /* let fsl_to_raw_name find the raw name. */
    if ( NULL == fsl_to_raw_name(blkname, rawname, MAXPATHLEN - 1) ) {
        sprintf(errstr, "mkraw: fsl_to_raw_name of %s failed", blkname);
        perror(errstr);
        return ENOENT;
    }

    /* Test that "rawname" exists. */
    if ( stat(rawname, &statBuf) == -1 ) {
        sprintf(errstr, "mkraw: stat failed for raw volume \"%s\"\n", rawname);
        perror(errstr);

        strcpy(rawname, blkname);
        return ENOENT;
    }

    /* Test that the proper naming convention was used, and the manufactured */
    /* name is in fact a raw device. */
    if ( !S_ISCHR(statBuf.st_mode) ) {
        fprintf(stderr,
          "mkraw: WARNING: mkraw returned non raw volume \"%s\".\n", rawname);

        strcpy(rawname, blkname);
        return ENOENT;
    }


    return OK;
}

/*****************************************************************************/
/*
** Change partial device name to a full path name.
**
** Input: name - partial device name (grp/vol)
** Output: devname - full path name of character device
** Errors: status return only. No diagnostic printf
** Return: OK if name is succesfully changed to devname
**         ENOENT if no such device exists
*/
static int
make_n_test_devname(const char *name, char *devname)
{
    struct stat  statbuf;


    if ( NULL == fsl_find_raw_name(name, devname, MAXPATHLEN) ) {
        return ENOENT;
    }

    if ( stat(devname, &statbuf) == -1 ) {
        return ENOENT;
    }

    if ( S_ISBLK(statbuf.st_mode) ) {
        return OK;
    }

    /* Even if fsl_find_raw_name makes a blk vol, let it pass. */
    if ( S_ISCHR(statbuf.st_mode) ) {
        return OK;
    }

    return ENOENT;

}

/*****************************************************************************/
/*
** Read the BSR_DMN_ATTR and BSR_DMN_MATTR records to get the
** vdCnt and bfDomainId. Compare bfDomainId to previous volumes.
**
** Input:  bfp->dmn->vols[vdi] filled in
**         dmnIdp - domain version from previous call to check_vol
**         volCntp - number of volumes in domain from previous call to check_vol
** Output: dmnIdp - domain version if not previously set
**         volCntp - number of volumes in domain if not previously set
** Errors: diagnostic printf for all errors
** Return: no error returns
*/
static void
check_vol(bitFileT *bfp, vdIndexT vdi, bfDomainIdT *dmnIdp, int *volCntp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint32T mc;
    bsDmnAttrT *bsDmnAttrp;
    bsDmnMAttrT *bsDmnMAttrp;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[vdi]);

    if ( RBMT_PRESENT ) {
        mdBfp = bfp->dmn->vols[vdi]->rbmt;
        mc = BFM_RBMT_EXT;      /* mcell 6 in RBMT page 0 */
    } else {
        mdBfp = bfp->dmn->vols[vdi]->bmt;
        mc = BFM_BMT_EXT_V3;    /* mcell 4 in BMT page 0 */
    }
    assert(mdBfp);

    ret = read_page(mdBfp, 0);
    if ( ret != OK ) {
        fprintf(stderr,
          "check_vol: Read volume %d (%s) %s page 0 (lbn %u) failed.\n",
          mdBfp->pgVol, bfp->dmn->vols[vdi]->volName,
          RBMT_PRESENT ? "RBMT" : "BMT", mdBfp->pgLbn);
        return;
    }

    bsMPgp = (bsMPgT*)mdBfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[mc];
    ret = find_bmtr(bsMCp, BSR_DMN_ATTR, (char**)&bsDmnAttrp);
    if ( ret != OK ) {
        /* Every volume should have a BSR_DMN_ATTR record. */
        fprintf(stderr,
     "check_vol: %s vol,page,mcell %d 0 %d - No BSR_DMN_ATTR record found.\n",
          RBMT_PRESENT ? "RBMT" : "BMT", mdBfp->pgVol, mc);
    }

    if ( dmnIdp->tv_sec == 0 && dmnIdp->tv_usec == 0 ) {
        /* Domain Id not yet set. Set it now. */
        *dmnIdp = bsDmnAttrp->bfDomainId;
    } else if ( dmnIdp->tv_sec != bsDmnAttrp->bfDomainId.tv_sec ||
                dmnIdp->tv_usec != bsDmnAttrp->bfDomainId.tv_usec )
    {
        fprintf(stderr,
          "Volume %d has a different domain ID than the previous volume.\n",
          vdi);
        fprintf(stderr, "Domain ID on volume %d is %x.%x.\n", vdi,
          bsDmnAttrp->bfDomainId.tv_sec, bsDmnAttrp->bfDomainId.tv_usec);
        fprintf(stderr, "The previous domain ID was %x.%x.\n",
          dmnIdp->tv_sec, dmnIdp->tv_usec);
    }

    ret = find_bmtr(bsMCp, BSR_DMN_MATTR, (char**)&bsDmnMAttrp);
    if ( ret == OK ) {
        if ( *volCntp != 0 ) {
            fprintf(stderr,
              "WARNING: Another BSR_DMN_MATTR record found on volume %d.\n",
              vdi);
            if ( *volCntp != bsDmnMAttrp->vdCnt ) {
                fprintf(stderr, "Previous vdCnt was %d, new vdCnt is %d.\n",
                  *volCntp, bsDmnMAttrp->vdCnt);
            }
        }
        *volCntp = bsDmnMAttrp->vdCnt;
    }
}

/******************************************************************************/
/* Open the volume and fill in the volume specific parts of bfp.
** Malloc bfp->dmn->vols[0]. Setup bfp->dmn->vols[0] until the volume index
** can be determined then move vols[0] to vols[vdi]. 
** Setup vols[0]->volName & vols[0]->fd. Fill in the vols[0]->blocks, the
** size of the volume. Malloc and fill in vols[0]->rbmt. After the version
** of the domain is determined, if the version is < 4, move rbmt to bmt and
** leave rbmt = NULL. Read the page at block 32. Find vdi and the domain
** version number. For V4 domains, setup vols[vdi]->bmt.
** Test the volume AdvFS magic number but just issue a warning if not OK.
**
** This is a central routine during the setup if reading a domain or volume.
** It is called by resolve_name => open_dmn or resolve_name => resolve_vol.
** FIX. not OK => return BAD_DISK_VALUE. This is ok to read a raw block
**
** Input: dirname = synlink name in /etc/fdmns/<dmn>
**        devname = name of special block (character) device to open
**        bfp->dmn is malloced
** OutPut: malloc and fill in bfp->dmn->vols[vol] (volName, index, fd)
**         malloc and fill in vols[vol]->rbmt {type, blocks, pvol, ppage, 
**                                             pcell, xtnts, dmn}
**         malloc and fill in vols[vol]->bmt
**         set pvol. NO, open_dmn doesn't want pvol set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - no such volume
**         EBUSY - block device is mounted
**         EACCES - no permissions to access volume
**         ENOMEM - malloc failed
**         BAD_DISK_VALUE - bad magic or other unexpected disk contents
*/
static int
open_vol(bitFileT *bfp, char *devname, vdIndexT *vdip)
{
    int volFd;
    char errstr[80];
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMPgT *bsMPgp;
    bitFileT *rbmt;

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->dmn);

    volFd = open(devname, O_RDONLY);
    if ( volFd == -1 ) {
        sprintf(errstr, "open_vol: open for volume \"%s\" failed", devname);
        perror(errstr);
        if ( errno == EACCES || errno == EBUSY || errno == ENOENT ) {
            return errno;
        } else {
            return ENOENT;
        }
    }

    /* Setup vdi 0 for now. We'll change it after we find the real vdi. */
    bfp->dmn->vols[0] = malloc(sizeof(volT));
    if ( bfp->dmn->vols[0] == NULL ) {
        perror("open_vol: malloc vols[0] failed");
        return ENOMEM;
    }

    strcpy(bfp->dmn->vols[0]->volName, devname);
    bfp->dmn->vols[0]->fd = volFd;
    get_vol_blocks(bfp->dmn->vols[0]);

    /* TODO. use different test for OK volume based just on BMT page 0 data. */
    if ( (ret = test_advfs_magic_number(volFd, devname)) != OK ) {
        /* Return ENOMEM or BAD_DISK_VALUE */
        /* BAD_DISK_VALUE is OK if we are looking at a block on the disk */
        /* We have not set up vols[0]->rbmt */
        return ret;
    }

    /* At this point we don't yet know if this domain has a RBMT or not. */
    /* Setup RBMT for now. After we read block 32 and find the domain */
    /* version, we will know if the file we are setting up is the RBMT */
    /* or BMT. If it's the BMT we'll move ->rbmt to ->bmt later. */
    bfp->dmn->vols[0]->rbmt = malloc(sizeof(bitFileT));
    if ( bfp->dmn->vols[0]->rbmt == NULL ) {
        perror("open_vol: malloc rbmt failed");
        return ENOMEM;
    }

    rbmt = bfp->dmn->vols[0]->rbmt;

    /* init rbmt (including pvol,pgVol = 0) & */
    /* set rbmt->type, rbmt->dmn, rbmt->setTag */
    newbf(bfp, rbmt);
    rbmt->setTag = -BFM_BFSDIR;   /* -2 */

    /* read first RBMT or BMT page at pgVol (= 0) and pgLbn */
    rbmt->pgLbn = MSFS_RESERVED_BLKS;              /* block 32 */
    if ( read_vol_at_blk(rbmt) != OK ) {
        return BAD_DISK_VALUE;
    }

    bsMPgp = (bsMPgT*)rbmt->pgBuf;
    rbmt->pgNum = 0;
    (void)test_bmt_page(rbmt, "open_vol", TRUE);

    /* Set global domain version for other routines to reference. */
    dmnVersion = bsMPgp->megaVersion;

    /* newbf() set rbmt->dmn to bfp->dmn */
    if ( bfp->dmn->dmnVers == 0 ) {
        /* This is the first volume opened. Set dmnVers now. */
        bfp->dmn->dmnVers = bsMPgp->megaVersion;
    } else {
        /* Volumes already opened. dmnVers already set. */
        if ( bfp->dmn->dmnVers != bsMPgp->megaVersion ) {
            fprintf(stderr,
   "Domain version (%d) on volume \"%s\" does not match previously read version (%d)\n",
              bsMPgp->megaVersion, devname, bfp->dmn->dmnVers);
        }
    }

    /* Read BSR_VD_ATTR record to find the volume index of this disk. */
    ret = find_vdi(bfp, bsMPgp, devname, vdip);
    if ( ret != OK ) {
         return ret;
    }

    /* now that we know the volume index, mv vols[0] there. */
    bfp->dmn->vols[*vdip] = bfp->dmn->vols[0];
    bfp->dmn->vols[0] = NULL;

    bfp->dmn->vols[*vdip]->rbmt->pvol = *vdip;
    bfp->dmn->vols[*vdip]->rbmt->ppage = 0;
    bfp->dmn->vols[*vdip]->rbmt->pcell = RBMT_PRESENT ? BFM_RBMT : BFM_BMT_V3;
    bfp->dmn->vols[*vdip]->rbmt->fileTag = RSVD_TAGNUM_GEN(*vdip, BFM_RBMT);

    bfp->dmn->vols[*vdip]->rbmt->dmn = bfp->dmn;

    if ( RBMT_PRESENT ) {
        ret = load_rbmt_xtnts(bfp->dmn->vols[*vdip]->rbmt);
        if ( ret != OK && ret != BAD_DISK_VALUE ) {
            return ret;
        }

        bfp->dmn->vols[*vdip]->bmt = malloc(sizeof(bitFileT));
        if ( bfp->dmn->vols[*vdip]->bmt == NULL ) {
            sprintf(errstr, "open_vol: malloc failed");
            perror(errstr);
            return ENOMEM;
        }

        newbf(bfp->dmn->vols[*vdip]->rbmt, bfp->dmn->vols[*vdip]->bmt);
        bfp->dmn->vols[*vdip]->bmt->pvol = *vdip;
        bfp->dmn->vols[*vdip]->bmt->ppage = 0;
        bfp->dmn->vols[*vdip]->bmt->pcell = BFM_BMT;
        bfp->dmn->vols[*vdip]->bmt->fd = volFd;
        bfp->dmn->vols[*vdip]->bmt->fileTag = RSVD_TAGNUM_GEN(*vdip, BFM_BMT);
        ret = load_xtnts(bfp->dmn->vols[*vdip]->bmt);
        if ( ret != OK ) {
            return ret;
        }

        bfp->dmn->vols[*vdip]->bmt->dmn = bfp->dmn;
    } else {
        bfp->dmn->vols[*vdip]->bmt = bfp->dmn->vols[*vdip]->rbmt;
        bfp->dmn->vols[*vdip]->rbmt = NULL;

        ret = load_V3bmt_xtnts(bfp->dmn->vols[*vdip]->bmt);
        if ( ret != OK ) {
            return ret;
        }
    }

    return OK;
}

/*****************************************************************************/
/* Read the super block and check the AdvFS magic number. This routine does
** not need or use extent maps. 
**
** Input: fd - lseek and read this file
** Input: devname used only to print warning
** Output: none
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM - if malloc of buffer to read page failed
**         BAD_DISK_VALUE - bad magic number
*/
static int
test_advfs_magic_number(int fd, char *devname)
{
    ssize_t read_ret;
    uint32T *superBlk;

    superBlk = (uint32T *)malloc(ADVFS_PGSZ);
    if (superBlk == NULL) {
        return ENOMEM;
    }

    if ( lseek(fd, (off_t)MSFS_FAKE_SB_BLK * BS_BLKSIZE, SEEK_SET) == -1 ) {
        perror("test_advfs_magic_number: lseek failed");
        free(superBlk);
        return BAD_DISK_VALUE;
    }

    read_ret = read(fd, superBlk, (size_t)ADVFS_PGSZ);
    if ( read_ret != (ssize_t)ADVFS_PGSZ ) {
        free(superBlk);
        if ( read_ret == (ssize_t)-1 ) {
            perror("test_advfs_magic_number: read superblock failed");
        } else {
            fprintf(stderr,
                "read superblock failed: read %ld bytes. wanted %d\n",
                read_ret, ADVFS_PGSZ);
        }
        return BAD_DISK_VALUE;
    }

    if ( superBlk[MSFS_MAGIC_OFFSET / sizeof(uint32T)] != MSFS_MAGIC ) {
        fprintf(stderr, "Bad AdvFS magic number on %s.\n", devname);
        free(superBlk);
        return BAD_DISK_VALUE;
    }

    free(superBlk);
    return OK;
}

/*****************************************************************************/
/*
** if the device is a diskgroup volume ask for
** partition size. If the device is not, then read the disk label.
** See get_dev_size/islsm/get_disk_label in library.c.
** Any errors leaves vol->blocks initialized to zero.
**
** Input: vol->volName
**        vol->volFd is open
** Outout: vol->blocks - set to partition size or 0
** Errors: diagnostic output to stderr for all errors
** Return: none
*/
static void
get_vol_blocks(volT *vol)
{
    off_t seek_off;
    void *buf;

    vol->blocks = 0;

    /* First assume volume and get the size. */
    if ( is_lsm(vol) == FALSE ) {
        /* If that fails then get the volume size from the disk label. */
        read_disk_label(vol);
    }

    if ( vol->blocks == 0 ) {
        /* If we failed to get the volume size, c'est la vie. */
        return;
    }

    /* test that the last block is really there */
    buf = (void *)malloc(BS_BLKSIZE);
    if ( buf == NULL ) {
        perror("get_vol_blocks: malloc fail");
        return;
    }

    seek_off = (off_t)(vol->blocks - 1) * BS_BLKSIZE;
    if ( pread(vol->fd, buf, (size_t)BS_BLKSIZE, seek_off) != BS_BLKSIZE ) {
        perror("get_vol_blocks: pread fail");
    }

    free(buf);

    return;
}

/*****************************************************************************/
/*
** Check to see if diskgroup. If it is set volp->blocks.
**
** Input:  volp->fd is open
** Outout: volp->blocks - set to partition size
** Return: TRUE if diskgroup, else return FALSE
*/
static int
is_lsm( volT *volp )
{
   DEVGEOMST volgeom;
   struct devget devget;
   device_info_t  devinfo;

    /*
     * First, attempt to use the DEVGETINFO ioctl (preferred),
     * if that fails, try the backward-compatibility ioctl
     * DEVIOCGET.
     */
    if ( ioctl(volp->fd, DEVGETINFO, (char *)&devinfo) != -1 &&
         devinfo.version == VERSION_1 )
    {
        if ( strncmp(devinfo.v1.dev_name, "LSM", strlen("LSM")) != 0 ) {
            return FALSE;
        }
    } else if ( ioctl(volp->fd, DEVIOCGET , (char *)&devget) != -1 &&
                devget.dev_name )
    {
        if ( strncmp(devget.dev_name, "LSM", strlen("LSM")) != 0 ) {
            return FALSE;
        }
    } else {
        return FALSE;
    }

    /* Make ioctl call to get volume length. */
    if ( ioctl(volp->fd, DEVGETGEOM , (char *)&volgeom) == 0 ) {
        if ( volgeom.geom_info.dev_size < 0x100000000L ) {
            volp->blocks = (lbnT)volgeom.geom_info.dev_size;
        } else {
            fprintf(stderr, "diskgroup is %lu blocks which is > 2TB.\n",
              volgeom.geom_info.dev_size);
            volp->blocks = 0xffffffff;
        }
    } else {
        /* ioctl call failed because fd does not refer to diskgroup. */
        return FALSE;
    }

    return TRUE;
}

/*****************************************************************************/
/*
** Make a node and open the special block device for the first partition
** on the disk specified by the device number.
** Read the first block to get the disklabel.
** If ioctl fails, try each partition in turn.
** Find the size in blocks of the partition specified by the device number
** and record it in the blocks field of vol.
** Any errors leaves vol->blocks alone (initialized to zero).
**
** Input: volp->volName
**        volp->fd is open
** Outout: vol->blocks - set to partition size
** Errors: diagnostic output to stderr for all errors
** Return: none
*/
static void
read_disk_label(volT *volp)
{
    struct disklabel lp;
    int partition = volp->volName[strlen(volp->volName) - 1] - 'a';
    int fd;
    char disk[MAXPATHLEN];

    strcpy(disk, volp->volName);
    disk[strlen(disk) - 1] = 'a';    /* Start with the "a" partition. */

    while (disk[strlen(disk) - 1] <= 'h') {
        if (-1 == (fd = open(disk, O_RDONLY))) {
            perror("read_disk_label: open failed");
            return;
        }

        if (ioctl(fd, DIOCGDINFO, &lp) != -1) {
            volp->blocks = (lbnT)lp.d_partitions[partition].p_size;
            close(fd);
            return;
        }

        disk[strlen(disk) - 1]++;    /* try partiton "b" - "h" */
        close(fd);
    }

    fprintf(stderr, "No valid disk label found on disk %s.\n", volp->volName); 
}

/*****************************************************************************/
/* Given page 0 of the (R)BMT, look at the mcell with the vdIndex.
** Check the tag and set tag of the mcell. Look for BSR_VD_ATTR record.
** Check the vdIndex.
**
** Input: Global dmnVersion has been set
**        bmtpgp - pointer to (R)BMT page 0
**        bfp->dmn->vols[] - filled in. used to check vol previously found
** Output: vdip - returns volume index
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad disk contents or BSR_VD_ATTR not found
*/
static int
find_vdi(bitFileT *bfp, bsMPgT *bmtpgp, char *devname, vdIndexT *vdip)
{
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    uint32T mc;
    vdIndexT vdi;
    int ret;

    /* check version */
    if ( RBMT_PRESENT ) {
        mc = BFM_RBMT_EXT;      /* mcell 6 in RBMT page 0 */
    } else {
        mc = BFM_BMT_EXT_V3;    /* mcell 4 in BMT page 0 */
    }
    mcellp = &bmtpgp->bsMCA[mc];

    /* Check for correct set tag */
    if ( (signed)mcellp->bfSetTag.num != -BFM_BFSDIR ||
         mcellp->bfSetTag.seq != 0 )
    {
        fprintf(stderr, "find_vdi: Bad fileset tag (%d,%d) in mcell %d. ",
          (signed)mcellp->bfSetTag.num, mcellp->bfSetTag.seq, mc);
        fprintf(stderr, "Should be -2,0\n");
    }

    /* Check for correct file tag */
    if ( (signed)mcellp->tag.num >= 0 ||
         (signed)mcellp->tag.num % BFM_RSVD_TAGS != 0 ||
         mcellp->tag.seq != 0 )
    {
        fprintf(stderr, "find_vdi: Bad file tag (%d,%d) in mcell %d.\n",
          (signed)mcellp->tag.num, mcellp->tag.seq, mc);
        fprintf(stderr, "File tag should be negative, and divisible by 6. ");
        fprintf(stderr, " Sequence number should be 0\n");
    }

    ret = find_bmtr(mcellp, BSR_VD_ATTR, (char**)&bsVdAttrp);
    if ( ret != OK ) {    /* can fail with BAD_DISK_VALUE or ENOENT */
        fprintf(stderr,
          "find_vdi: Could not find BSR_VD_ATTR record in cell %d.\n", mc);
        return BAD_DISK_VALUE;
    }

    if ( bsVdAttrp->vdBlkCnt != bfp->dmn->vols[0]->blocks ) {
        if ( bfp->dmn->vols[0]->blocks == 0 ) {
            fprintf(stderr, "Volume size ioctl failed, vdBlkCnt %u blocks.\n",
              bsVdAttrp->vdBlkCnt);
        } else if ( bsVdAttrp->vdBlkCnt > bfp->dmn->vols[0]->blocks ) {
            fprintf(stderr,
            "ERROR: Advfs volume size is larger than the virtual disk size.\n");
            fprintf(stderr,
            "Advfs volume size is %u blocks, virtual disk size is %u blocks.\n",
              bsVdAttrp->vdBlkCnt, bfp->dmn->vols[0]->blocks);
            fprintf(stderr,
            "The smaller virtual disk size will be used for block checking.\n");
        } else {
            fprintf(stderr,
              "Advfs volume does not use the entire virtual disk.\n");
            if ( bfp->dmn->vols[0]->blocks == 0xffffffff ) {
                /* blocks has been squashed by get_vol_blocks to fit */
                fprintf(stderr,
             "Advfs volume size %u blocks, virtual disk size >= 2^32 blocks.\n",
                  bsVdAttrp->vdBlkCnt);
            } else {
                fprintf(stderr,
            "Advfs volume size is %u blocks, virtual disk size is %u blocks.\n",
                  bsVdAttrp->vdBlkCnt, bfp->dmn->vols[0]->blocks);
            }
            bfp->dmn->vols[0]->blocks = bsVdAttrp->vdBlkCnt;
        }
    }

    vdi = bsVdAttrp->vdIndex;

    if ( vdi >= BS_MAX_VDI ) {
        fprintf(stderr,
          "find_vdi: Bad vdi (%d) read from BSR_VD_ATTR record. ", vdi);
        fprintf(stderr, "Must be <= %d.\n", BS_MAX_VDI);
        return BAD_DISK_VALUE;
    }

    if ( bfp->dmn->vols[vdi] ) {
        fprintf(stderr, "find_vdi: vdi %d already found. ", vdi);
        fprintf(stderr, "Skipping %s\n", devname);
        return OK;
    }

    *vdip = vdi;

    return OK;
}

/*****************************************************************************/
/* Load the RBMT extent map, following the chain of RBMT pages.
** Each page of the RBMT file contains, in cell 27, a BSR_XTNT
** record whose extent describes the next page of the RBMT. The
** chain and next also point to the next RBMT page.
** Called from open_vol.
**
** Input: Global dmnVersion set and >= 4
** Input: bfp->dmn->vols[]
**        bfp->pcell set to 0
**        bfp->pgBuf already read, page 0 of RBMT
** Output: bfp->xmap - filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM - malloc fails
**         BAD_DISK_VALUE - lseek or read fails or unexpected disk values
*/
static int
load_rbmt_xtnts(bitFileT *bfp)
{
    int ret;
    vdIndexT vdi;    /* volume index */
    bsXtntRT *rbmtXtntRp;
    int  idx;                     /* index of next xmap.xtnt[] to be filled */
    bsXtntRT  *bsXtntRp;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    bfMCIdT bfMCId;
    int mcCnt;
    int mcellCnt;
    int savCnt;
    int savret = OK;

    assert(RBMT_PRESENT);
    assert(bfp->pcell == BFM_RBMT);
    assert(bfp->fileTag == RSVD_TAGNUM_GEN(bfp->pvol, BFM_RBMT));
    assert(bfp->pgBuf);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->ppage == 0);

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[bfp->pcell];
    ret = load_bsr_xtnts(bfp, &idx, bsMCp, &vdi, &bfMCId, &mcCnt);
    if ( ret != OK ) {
        return ret;
    }

    if ( bfMCId.page != 0 ) {
        fprintf(stderr,"RBMT first BSR_XTRA_XTNTS cell is not on page 0 (%d)\n",
          bfMCId.page);
        bfMCId.page = 0;
        savret = BAD_DISK_VALUE;
    }

    mcellCnt = 1;   /* count the primary mcell */

    /* vdi from the chain mcell will be zero if there are no BSR_XTRA_XTNTS */
    /* extents. But the extent chain ends with a cell with no record. */
    /* The last BSR_XTRA_XTNTS cell's next points to an empty cell. */
    while ( vdi > 0 ) {

        /* test additional restriction on vdi */
        if ( vdi != bfp->pvol ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr, "RBMT file extents change volume.\n");
            vdi = bfp->pvol;
            savret = BAD_DISK_VALUE;
        }

        /* test additional restriction on cell */
        if ( bfMCId.cell != RBMT_RSVD_CELL ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr, "RBMT mcell is not the last on the page.\n");
            bfMCId.cell = RBMT_RSVD_CELL;
            savret = BAD_DISK_VALUE;
        }

        bsMCp = &bsMPgp->bsMCA[bfMCId.cell];

        ret = load_xtra_xtnts(bfp, &idx, bsMCp, &vdi, &bfMCId, TRUE);
        if ( ret == ENOENT ) {
            /* No BSR_XTRA_XTNTS record in this cell. End of the chain. */
            break;
        }
        if ( ret != OK ) {
            return ret;
        }

        /* Test additional restriction on page. One addtional mcell per page. */
        if ( vdi != 0 && bfMCId.page != mcellCnt ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "Bad RBMT mcell %d on page %d (vol %d, blk %u): next page %d, expected %d\n",
              RBMT_RSVD_CELL, mcellCnt - 1, bfp->pvol,
              bfp->xmap.xtnt[mcellCnt - 1].blk, bfMCId.page, mcellCnt);
            bfMCId.page = mcellCnt;
            savret = BAD_DISK_VALUE;
        }

        mcellCnt++;

        if ( mcellCnt % 100 == 0 ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              " reading %d mcells, %d mcells read\r", mcCnt, mcellCnt);
        }

        /* Only allow read_page to look at mcellCnt extents. */
        savCnt = bfp->xmap.cnt;
        bfp->xmap.cnt = mcellCnt;

        if ( ret = read_page(bfp, bfMCId.page) ) {
            bfp->xmap.cnt = savCnt;
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "load_rbmt_xtnts: Read RBMT page %d on volume %d failed.\n",
              bfMCId.page, vdi);
            return ret;
        }

        /* Restore actual number of extents allocated. */
        bfp->xmap.cnt = savCnt;

        bsMPgp = (bsMPgT*)bfp->pgBuf;
        (void)test_bmt_page(bfp, "load_rbmt_xtnts", TRUE);

        bsMCp = &bsMPgp->bsMCA[bfMCId.cell];
        (void)test_mcell(bfp, "load_rbmt_xtnts", vdi, bfMCId, bsMCp);
    }

    fprintf(stderr, CLEAR_LINE);

    if ( mcellCnt != mcCnt ) {
        fprintf(stderr,
          "load_rbmt_xtnts: Bad mcellCnt. Expected %d, actual %d\n",
          mcCnt, mcellCnt);
    }

    /* needed for page_mapped and read_page */
    bfp->xmap.xtnt[idx].page = 0;
    bfp->pages = bfp->xmap.xtnt[idx - 1].page;    /* last real page # */
    bfp->xmap.cnt = idx;

    return savret;
}

/*****************************************************************************/
/* In a V3 domain, load the BMT extents. We dont have the extents for the
** BMT file so we cna't use load_xtnts(). Load the extents by hand. We know
** that all the extents will be on the page at block 32.
**
** Input: bfp - BMT bitfile
**        bfp->pcell - set to primary mcell of BMT (0)
**        bfp->pgBuf - contains page 0 (block 32)
** Output: bfp->xmap, pages - filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad mcell contents
**         errors from load_bsr_xtnts, load_xtra_xtnts
*/
static int
load_V3bmt_xtnts(bitFileT *bfp)
{
    int ret;
    vdIndexT vdi;    /* volume index */
    bsXtntRT *rbmtXtntRp;
    int  idx;                     /* index of next xmap.xtnt[] to be filled */
    bsXtntRT  *bsXtntRp;
    bsMPgT *bsMPgp;
    bsMCT *mcp;
    bfMCIdT bfMCId;
    int mcCnt;

    assert(!RBMT_PRESENT);
    assert(bfp->pcell == BFM_RBMT);
    assert(bfp->fileTag == RSVD_TAGNUM_GEN(bfp->pvol, BFM_BMT_V3));
    assert(bfp->pgBuf);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->ppage == 0);

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    mcp = &bsMPgp->bsMCA[bfp->pcell];
    ret = load_bsr_xtnts(bfp, &idx, mcp, &vdi, &bfMCId, &mcCnt);
    if ( ret != OK ) {
       return ret;
    }

    /* TODO. consider making new routine called get_bmt_xtra_xtnts() */
    while ( vdi > 0 ) {

        /* test for valid vdi */
        if ( vdi != bfp->pvol ) {
            fprintf(stderr, "Reserved file extents change volume.\n");
            return BAD_DISK_VALUE;
        }
        /* test for valid page; not restricted to page 0 as of Version 4 */
        if ( bfMCId.page != 0 ) {
            printf("bmt extra extents not on page 0!\n");
            return BAD_DISK_VALUE;
        }
        /* test for valid mcell number */
        if ( bfMCId.cell >= BSPG_CELLS )
        {
            fprintf(stderr, "bad mcell\n");
            return BAD_DISK_VALUE;
        }

        mcp = &bsMPgp->bsMCA[bfMCId.cell];

        ret = load_xtra_xtnts(bfp, &idx, mcp, &vdi, &bfMCId, FALSE);
        if ( ret != OK ) {
            return ret;
        }
    }

    /* needed for page_mapped and read_page */
    bfp->xmap.xtnt[idx].page = 0;
    bfp->pages = bfp->xmap.xtnt[idx - 1].page;    /* last real page # */
    bfp->xmap.cnt = idx;

    return OK;
}

/*****************************************************************************/
/* Read a page into pgBuf at the specified block from the specified volume.
**
** Input: bfp->pgVol, bfp->pgLbn - volume and block to read
**        dmn->vols[pgVol]->fd - file descriptor of opened volume
** Output: bfp->pgBuf may be malloc'd
**         bfp->pgBuf filled
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM if malloc failed
**         BAD_DISK_VALUE - lseek or read fails
*/
static int
read_vol_at_blk(bitFileT *bfp)
{
    int fd;
    ssize_t read_ret;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->pgLbn % ADVFS_PGSZ_IN_BLKS == 0);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn->vols[bfp->pgVol]);
    assert(bfp->dmn->vols[bfp->pgVol]->fd > 2);

    fd = bfp->dmn->vols[bfp->pgVol]->fd;

    if ( test_blk(bfp, bfp->pgVol, bfp->pgLbn) == OUT_OF_RANGE ) {
        fprintf(stderr, "WARNING: Block %u is too large for volume %d\n",
          bfp->pgLbn, bfp->pgVol);
    }

    /* BMT page 0 or RBMT start at lbn 32 on each volume */
    if ( lseek(fd, (off_t)bfp->pgLbn * DEV_BSIZE, SEEK_SET) == (off_t)-1 ) {
        perror("read_vol_at_blk: lseek failed");
        return BAD_DISK_VALUE;
    }

    if ( bfp->pgBuf == NULL ) {
        bfp->pgBuf = malloc(PAGE_SIZE);
        if ( bfp->pgBuf == NULL ) {
            perror("read_vol_at_blk: malloc failed");
            return ENOMEM;
        }
    }

    read_ret = read(fd, bfp->pgBuf, (size_t)PAGE_SIZE);
    if ( read_ret != (ssize_t)PAGE_SIZE ) {
        if ( read_ret == (ssize_t)-1 ) {
            perror("read_vol_at_blk: read failed");
        } else {
            fprintf(stderr,
                "read_vol_at_blk: read failed: read %ld bytes. wanted %d\n",
                read_ret, PAGE_SIZE);
        }
        return BAD_DISK_VALUE;
    }

    return OK;
}
/*****************************************************************************/
/* Follow the chain of mcells and load all the extents in bfp.
**
** V3 non reserved files & striped files have no extents in BSR_XTNTS.
** The first extents are in BSR_SHADOW_XTNTS. The next extents are in
** BSR_XTRA_XTNTS.
**
** Reserved files and V4 non reserved, non striped files have one extent in
** BSR_XTNTS. The next extents are in BSR_XTRA_XTNTS.
**
** For RBMT and BMT just copy already existing extents in
** bfp->dmn->vols[]->(r)bmt.
**
** Input: bfp->fileTag - use RBMT vs BMT for mcells
**        bfp->pvol, bfp->ppage, bfp->pcell - primary mcell in (R)BMT
** Output: bfp->xmap.xtnts loaded
**         bfp->pages set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM if malloc of xtnts fails
**         BAD_DISK_VALUE - extents or mcells not as expected
/* TODO: Rename to load_bf_xtnts()? */
int
load_xtnts(bitFileT *bfp)
{
    int ret;
    int idx;
    vdIndexT vdi;
    bfMCIdT bfMCId;
    int strFlg;
    int segSz;
    int mcCnt;

    assert(bfp);
    assert(bfp->fileTag);
    assert(bfp->setTag);
    assert(bfp->pvol != 0);
    assert(bfp->ppage != -1);
    assert(bfp->pcell != -1l);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);

    /* special case for BMT & RBMT. use preloaded extents */
    if ( RBMT_PRESENT && bfp->fileTag == RSVD_TAGNUM_GEN(bfp->pvol, BFM_RBMT) )
    {
        /* special case for RBMT. Just "link" to RBMT in dmn. */
        assert(bfp->dmn->vols[bfp->pvol]->rbmt->xmap.xtnt);
        bfp->xmap.xtnt = bfp->dmn->vols[bfp->pvol]->rbmt->xmap.xtnt;
        bfp->xmap.cnt = bfp->dmn->vols[bfp->pvol]->rbmt->xmap.cnt;
        bfp->pages = bfp->dmn->vols[bfp->pvol]->rbmt->pages;
        return OK;
    }

    if ( RBMT_PRESENT &&
         bfp->fileTag == RSVD_TAGNUM_GEN(bfp->pvol, BFM_BMT) &&
         bfp->dmn->vols[bfp->pvol]->bmt->xmap.xtnt )
    {
        /* special case for BMT. Just "link" to BMT in dmn. */
        bfp->xmap.xtnt = bfp->dmn->vols[bfp->pvol]->bmt->xmap.xtnt;
        bfp->xmap.cnt = bfp->dmn->vols[bfp->pvol]->bmt->xmap.cnt;
        bfp->pages = bfp->dmn->vols[bfp->pvol]->bmt->pages;
        return OK;
    }

    if ( !RBMT_PRESENT &&
         bfp->fileTag == RSVD_TAGNUM_GEN(bfp->pvol, BFM_BMT_V3) )
    {
        /* special case for BMT. Just "link" to BMT in dmn. */
        assert(bfp->dmn->vols[bfp->pvol]->bmt->xmap.xtnt);
        bfp->xmap.xtnt = bfp->dmn->vols[bfp->pvol]->bmt->xmap.xtnt;
        bfp->xmap.cnt = bfp->dmn->vols[bfp->pvol]->bmt->xmap.cnt;
        bfp->pages = bfp->dmn->vols[bfp->pvol]->bmt->pages;
        return OK;
    }

    ret = get_stripe_parms( bfp, &strFlg, &segSz);
    if ( ret != OK ) {
        return ret;
    }

    /*  really V4 since this is xtnts in prim. remember stripe doesn't do this*/
    if ( bfp->fileTag < 0 || (RBMT_PRESENT && !strFlg) ) {
        ret = get_bsr_xtnts(bfp, &idx, &vdi, &bfMCId, &mcCnt);
        if ( ret != OK ) {
            return ret;
        }

        if ( vdi == 0 ) {
            bfp->xmap.cnt = idx;
            return OK;
        }

        ret = get_xtra_xtnts(bfp, idx, &vdi, &bfMCId, mcCnt, FALSE);
        if ( ret != OK ) {
            return ret;
        }

        return OK;
    }

    ret = get_chain_mcell(bfp, &vdi, &bfMCId);
    if ( ret != OK ) {
        return ret;
    }

    if ( vdi == 0 ) {
        bfp->xmap.cnt = 0;
        return OK;
    }

    if ( strFlg ) {
        return get_stripe_xtnts(bfp, vdi, bfMCId, segSz);
    }

    ret = get_shadow_xtnts(bfp, &idx, &vdi, &bfMCId, &mcCnt);
    if ( ret != OK ) {
        return ret;
    }

    if ( vdi == 0 ) {
        bfp->xmap.cnt = idx;
        return OK;
    }

    ret = get_xtra_xtnts(bfp, idx, &vdi, &bfMCId, mcCnt, FALSE);
    if ( ret != OK ) {
        return ret;
    }

    return OK;
}

/*****************************************************************************/
/*
** Input:  bfp->pvol, bfp->ppage, bfp->pcell
**         bfp->dmn->vols[bfp-pvol] filled in
** Output: strFlgp - if the file striped
**         segSzp - width of the stripe in pages
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if the read failed or find_bmtr failed
**         ENOMEM if read_page can't maloc a buffer
*/
static int
get_stripe_parms(bitFileT *bfp, int *strFlgp, int *segSzp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bmtpgp;
    bsMCT *mcellp;
    bsXtntRT *bsXtntRp;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);

    if ( USE_RBMT ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[bfp->pvol]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfp->pvol]->bmt;
    }
    assert(mdBfp);

    ret = read_page(mdBfp, bfp->ppage);
    if ( ret != OK ) {
        fprintf(stderr,
          "get_stripe_parms: Read volume %d (%s) %s page %d (lbn %u) failed.\n",
          mdBfp->pgVol, bfp->dmn->vols[bfp->pvol]->volName,
          USE_RBMT ? "RBMT" : "BMT", bfp->ppage, mdBfp->pgLbn);
        return ret;
    }

    bmtpgp = (bsMPgT*)mdBfp->pgBuf;
    mcellp = &bmtpgp->bsMCA[bfp->pcell];

    ret = find_bmtr(mcellp, BSR_XTNTS, (char**)&bsXtntRp);
    if ( ret != OK ) {
        fprintf(stderr,
          "get_stripe_parms: Bad mcell %s vol %d page %d cell %d: ",
          USE_RBMT ? "RBMT" : "BMT", bfp->pvol, bfp->ppage, bfp->pcell);
        fprintf(stderr, "No BSR_XTNTS record in primary mcell.\n");
        return BAD_DISK_VALUE;
    }

    *strFlgp = bsXtntRp->type == BSXMT_STRIPE;
    *segSzp = bsXtntRp->segmentSize;

    return OK;
}

/*****************************************************************************/

/* from bs_stripe.h */
#define BFPAGE_2_MAP(pO, sC, sS) ( ( (pO) % ((sC) * (sS)) ) / (sS) )
#define BFPAGE_2_XMPAGE(pO, sC, sS) \
    ( ( ( (pO) / ((sC) * (sS)) ) * (sS) ) + ((pO) % (sS)) )
#define XMPAGE_2_BFPAGE(sI, pO, sC, sS) \
  ( ((sI) * (sS)) + ( ((pO) / (sS)) * ((sC) * (sS)) ) + ((pO) % (sS)) )

#define FPG(xpg) \
    XMPAGE_2_BFPAGE(segidx, (xpg), segMapp->segCnt, segMapp->segSz)
#define XPG(fpg) \
    BFPAGE_2_XMPAGE((fpg), segMapp->segCnt, segMapp->segSz)

/*
** Input:  bfp->dmn, bfp->dmn->vols[i]
**         vdi, bfMCId - SHADOW mcell ID
**         segSz - width of the stripe in pages
** Output: bfp->pages, bfp->xmap.xtnt filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
*/
static int
get_stripe_xtnts(bitFileT *bfp, vdIndexT vdi, bfMCIdT bfMCId, int segSz)
{
    int segidx;
    int idx;
    int xtnts;
    segMapT segMap;
    int ret;
    int mcCnt;
    int lastPg;

    assert(vdi != 0);    /* vdi of first SHADOW extent map */
    segMap.segXtnt = NULL;

    for ( segidx = 0; ; segidx++ ) {
        /* get an extent map for each stripe */
        segMap.segXtnt = realloc(segMap.segXtnt, (segidx+1) * sizeof(segXtntT));
        if ( segMap.segXtnt == NULL ) {
            perror("get_stripe_xtnts: realloc failed");
            exit(1);
        }

        ret = get_shadow_xtnts(bfp, &idx, &vdi, &bfMCId, &mcCnt);
        if ( ret != OK ) {
            return ret;
        }

        if ( vdi == 0 ) {
            /* The last stripe had only a SHADOW record. */
            bfp->xmap.cnt = idx;
            segMap.segXtnt[segidx].segXMap = bfp->xmap;
            bfp->xmap.xtnt = NULL;
            bfp->xmap.cnt = 0;
            break;
        }

        ret = get_xtra_xtnts(bfp, idx, &vdi, &bfMCId, mcCnt, TRUE);
        if ( ret == ENOENT ) {
            /* no more XTRA mcells, must be more SHADOW mcells */
            assert(vdi != 0);
            bfp->xmap.cnt = idx;
            segMap.segXtnt[segidx].segXMap = bfp->xmap;
            bfp->xmap.xtnt = NULL;
            bfp->xmap.cnt = 0;
            continue;
        }
        if ( ret != OK ) {
            return ret;
        }

        /* The last stripe had XTRA records. */
        /* This is the end of the last stripe. */
        bfp->xmap.cnt = idx;
        segMap.segXtnt[segidx].segXMap = bfp->xmap;
        bfp->xmap.xtnt = NULL;
        bfp->xmap.cnt = 0;
        break;
    }

    segMap.segCnt = segidx + 1;
    segMap.segSz = segSz;
    xtnts = 0;
    bfp->pages = 0;
    for ( segidx = 0; segidx < segMap.segCnt; segidx++ ) {
        extentMapT *extentMapp;

        extentMapp = &segMap.segXtnt[segidx].segXMap;

        /* Worst case number of extents once unstriped. */
        xtnts += extentMapp->cnt;
        xtnts += extentMapp->xtnt[extentMapp->cnt - 1].page / segMap.segSz;
        lastPg = XMPAGE_2_BFPAGE( segidx,
                                  extentMapp->xtnt[extentMapp->cnt - 1].page,
                                  segMap.segCnt,
                                  segMap.segSz);
        if ( lastPg > bfp->pages ) {
            bfp->pages = lastPg;
        }
    }

    bfp->xmap.cnt = xtnts + 1;
    bfp->xmap.xtnt = malloc((xtnts + 1) * sizeof(extentT));
    if ( bfp->xmap.xtnt == NULL ) {
        perror("get_stripe_xtnts: malloc failed");
        exit(1);
    }

    un_stripe(bfp, &segMap);
    free(segMap.segXtnt);
    return OK;
}

/*****************************************************************************/
/*
** Make separate extent for each stripe segment.
**
** Input:  bfp->pages
**         segMapp - extent maps for all the stripes
** Output: bfp->xmap set up
** Errors: No diagnostic printouts
** Return: No error returns
*/
static void
un_stripe(bitFileT *bfp, segMapT *segMapp)
{
    int idx = 0;      /* extent index for xmap.xtnt[] */
    int segidx;       /* stripe index for segMapp->segXtnt[] */
    uint32T page = 0;

    assert(bfp->xmap.cnt);
    assert(bfp->xmap.xtnt);
    for ( segidx = 0; segidx < segMapp->segCnt; segidx++ ) {
        segMapp->segXtnt[segidx].segXtntIdx = 0;
    }

    /* Process the entire file. */
    while ( page < bfp->pages ) {
        /* Across all stripes, within a segment width */
        for ( segidx = 0; segidx < segMapp->segCnt; segidx++ ) {
            int segXtntIdx = segMapp->segXtnt[segidx].segXtntIdx;
            int nextSegPg = page + segMapp->segSz;
            segXtntT *segXtntp = &segMapp->segXtnt[segidx];
            assert(page % segMapp->segSz == 0);

            /* Special handling for holes. Concatenate them. */
            /* This is not required if read_page handled consecutive holes. */
            /* TODO. could this test be blk < 0 */
            /* TODO. change all internal holes to -1 */
            if ( idx != 0 && bfp->xmap.xtnt[idx - 1].blk == XTNT_TERM ||
                 idx != 0 && bfp->xmap.xtnt[idx - 1].blk == PERM_HOLE_START )
            {
                if ( segXtntp->segXMap.xtnt[segXtntIdx].blk == XTNT_TERM ||
                     segXtntp->segXMap.xtnt[segXtntIdx].blk == PERM_HOLE_START )
                {
                    int lastXpg;

                    if ( segXtntIdx == segXtntp->segXMap.cnt - 1 ) {
                        /* no more extents in this stripe */
                        page = nextSegPg;
                        /* go to next stripe */
                        continue;
                    }

                    assert(segXtntIdx + 1 < segXtntp->segXMap.cnt);
                    lastXpg = segXtntp->segXMap.xtnt[segXtntIdx + 1].page - 1;
                    if ( FPG(lastXpg) < nextSegPg ) {
                        /* extent ends before segment */
                        page = FPG(lastXpg) + 1;
                        segXtntIdx++;
                        segMapp->segXtnt[segidx].segXtntIdx = segXtntIdx;
                    } else {
                        /* segment ends before extent */
                        page = nextSegPg;
                        /* go to next stripe */
                        continue;
                    }
                }
            }

            /* Within one stripe segment. */
            while ( page < nextSegPg ) {
                extentT *extentp;
                int lastXpg;

                extentp = &segXtntp->segXMap.xtnt[segXtntIdx];

                if ( idx >= bfp->xmap.cnt ) {
                    bfp->xmap.xtnt = realloc( bfp->xmap.xtnt,
                                              (idx + 1) * sizeof(extentT));
                    if ( bfp->xmap.xtnt == NULL ) {
                        perror("un_stripe: realloc fail");
                        exit(1);
                    }
                }
                bfp->xmap.xtnt[idx].page = page;
                bfp->xmap.xtnt[idx].vol = extentp->vol;
                bfp->xmap.xtnt[idx].blk = extentp->blk +
                               (XPG(page) - extentp->page) * ADVFS_PGSZ_IN_BLKS;
                idx++;

                if ( segXtntIdx == segXtntp->segXMap.cnt - 1 ) {
                   page = nextSegPg;
                   /* go to next stripe */
                   break;
                }

                assert(segXtntIdx + 1 < segXtntp->segXMap.cnt);
                lastXpg = (extentp + 1)->page - 1;
                if ( FPG(lastXpg) < nextSegPg ) {
                    /* extent ends before segment ends */
                    page = FPG(lastXpg) + 1;
                    segXtntIdx++;
                    segMapp->segXtnt[segidx].segXtntIdx = segXtntIdx;
                } else {
                    /* segment ends before extent ends */
                    page = nextSegPg;
                    /* go to next stripe */
                    break;
                }
            }
        }
    }

    bfp->xmap.cnt = idx;
}

/*****************************************************************************/
/*
** If this file is a clone, find the original file and merge the clone
** extents with the original extents.
**
** Input:  bfp->setp - zero for reserved fliles, otherwise setup
**         bfp->pvol, bfp->ppage, bfp->pcell
**         bfp->dmn->vols[bfp->pvol] set up
** Output: bfp->pages, bfp->xmap.xtnt
** Errors: diagnostic printf for errors
** Return: OK if all went well
**         ENOMEM if malloc fails
*/
static int
load_clone_xtnts(bitFileT *bfp)
{
    int ret;
    bitFileT origBfp;
    bsBfAttrT *bsBfAttrp;
    bitFileT *mdBfp;
    bsMPgT *bmtpgp;
    bsMCT *mcellp;

    ret = load_xtnts(bfp);
    if ( ret != OK ) {
        return ret;
    }

    if ( bfp->setp == NULL ) {
        /* This is a reserved file. It is not a clone. */
        assert(bfp->setTag == -BFM_BFSDIR);    /* -2 */
        return OK;
    }

    if ( bfp->setp->origSetp == NULL ) {
        /* This file is not in a clone fileset. Return w/ orig extents */
        return OK;
    }

    ret = find_rec(bfp, BSR_ATTR, (char**)&bsBfAttrp);
    if ( ret != OK ) {
        if ( ret != BAD_DISK_VALUE ) {
            fprintf(stderr,
         "Primary mcell at vol %d, page %d, cell %d: can't find BSR_ATTR rec\n",
              bfp->pvol, bfp->ppage, bfp->pcell);
        }
        return OK;
    } else {
        if ( bsBfAttrp->cloneId != 0 ) {
            if ( bfp->pages > bsBfAttrp->maxClonePgs ) {
                fprintf(stderr,
     "WARNING: load_clone_xtnts: Too many clone extent pages. > maxClonePgs\n");
            }
            bfp->pages = bsBfAttrp->maxClonePgs;
        }
        /* else the clone has no meta data. just read the original BSR_ATTR */
    }

    if ( bfp->pages == 0 ) {
        return OK;
    }

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);
    assert (bfp->fileTag > 0);  /* no clone reserved files */

    if ( USE_RBMT ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[bfp->pvol]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfp->pvol]->bmt;
    }
    assert(mdBfp);

    ret = read_page(mdBfp, bfp->ppage);
    if ( ret != OK ) {
        fprintf(stderr, "load_clone_xtnts: Read volume %d %s page %d failed.\n",
          bfp->pvol, USE_RBMT ? "RBMT" : "BMT",
          bfp->ppage);
        return ret;
    }

    bmtpgp = (bsMPgT*)mdBfp->pgBuf;
    mcellp = &bmtpgp->bsMCA[bfp->pcell];
    if ( mcellp->bfSetTag.num != bfp->setTag ) {
        /* This primary mcell belongs to the original file. */
        /* The clone file has no metadata of its own. */
        bfp->pvol = 0;
        bfp->ppage = -1;
        bfp->pcell = -1;
        if ( mcellp->bfSetTag.num != bfp->setp->origSetp->fileTag ) {
            fprintf(stderr,
              "load_clone_xtnts: Bad %s mcell (vol,page,cell) %d %d %d.\n",
              USE_RBMT ? "RBMT" : "BMT",
              bfp->pvol, bfp->ppage, bfp->pcell);
            fprintf(stderr,
             "Bad mcell set tag (%d). Clone fileset %d. Original fileset %d.\n",
              mcellp->bfSetTag.num, bfp->setTag, bfp->setp->origSetp->fileTag);
        }
    }

    newbf(bfp, &origBfp);

    ret = get_file_by_tag(bfp->setp->origSetp, bfp->fileTag, &origBfp);
    if ( ret == ENOENT ) {
        goto done;
    }
    if ( ret != OK ) {
        fprintf(stderr,
         "load_clone_xtnts: Error in retrieving orig extents for file tag %d\n",
          bfp->fileTag);
        goto done;
    }

    merge_clone_xtnts(bfp, &origBfp);

done:
    free(origBfp.xmap.xtnt);

    return OK;
}

/*****************************************************************************/
/*
** Merge the original file's extents into the clone extents.
** If the clone maps a page (including permanent holes) use the clone's extent.
** Else if the original maps the page, use the original's extent.
** Else leave a hole in the extent map.
**
** Input:  cloneBfp->xmap.xtnt, origBfp->xmap.xtnt, cloneBfp->pages
** Output: cloneBfp->xmap.xtnt modified
** Errors: none
** Return: OK if all went well
**         ENOMEM - if no mem while extending the clone's xmap.xtnt
*/
static void
merge_clone_xtnts(bitFileT *cloneBfp, bitFileT *origBfp)
{
    uint32T page = 0;
    int cloneXi = 0;
    int origXi = 0;
    int mergeXi = 0;
    extentT *xtnts;
    uint32T nextClonePage;
    uint32T nextOrigPage;
    int xtntCnt = cloneBfp->xmap.cnt + origBfp->xmap.cnt;

    xtnts = malloc(xtntCnt * sizeof(extentT));
    if ( xtnts == NULL ) {
        perror("merge_clone_xtnts: malloc failed");
        exit(1);
    }

    while ( page < cloneBfp->pages ) {
        if ( cloneXi < cloneBfp->xmap.cnt &&
             cloneBfp->xmap.xtnt[cloneXi].blk != XTNT_TERM )
        {
            /* We have a clone extent (maybe a perm hole). */
            /* Use the clone extent. We use the orig extent only where the */
            /* clone has a hole. */
            xtnts[mergeXi] = cloneBfp->xmap.xtnt[cloneXi];
            cloneXi++;
            assert(cloneXi < cloneBfp->xmap.cnt);
            mergeXi++;
            assert(mergeXi < xtntCnt);
            page = cloneBfp->xmap.xtnt[cloneXi].page;
            continue;
        }

        /* The clone extent at cloneXi is a hole or the end of clone extents. */
        assert(cloneBfp->xmap.xtnt[cloneXi].blk == XTNT_TERM);
        assert(cloneBfp->xmap.xtnt[cloneXi].page <= page);

        if ( origXi >= origBfp->xmap.cnt - 1 ) {
            /* There are no more extents in the original file. */
            /* Copy the rest of the clone's extents and we're done. */
            while ( cloneXi < cloneBfp->xmap.cnt - 1 ) {
                xtnts[mergeXi] = cloneBfp->xmap.xtnt[cloneXi];
                if ( cloneBfp->xmap.xtnt[cloneXi].page < page ) {
                    xtnts[mergeXi].page = page;
                }
                cloneXi++;
                mergeXi++;
                assert(mergeXi < xtntCnt);
            }

            page = cloneBfp->xmap.xtnt[cloneXi].page;
            break;
        }

        /* The original file still has more extents to look at. */
        if ( origBfp->xmap.xtnt[origXi + 1].page <= page ) {
            /* The orig does not overlap the clone hole. It is completely */
            /* before the hole. Move the orig extent pointer forward. */
            /* This orig extent maps pages already merged into clone. */
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            continue;
        }

        /* The original extent at origXi covers "page" */
        assert(origBfp->xmap.xtnt[origXi].page <= page);
        assert(origBfp->xmap.xtnt[origXi + 1].page > page);

        xtnts[mergeXi].vol = origBfp->xmap.xtnt[origXi].vol;
        xtnts[mergeXi].page = page;
        if ( origBfp->xmap.xtnt[origXi].blk == XTNT_TERM ) {
            xtnts[mergeXi].blk = XTNT_TERM;
        } else {
            xtnts[mergeXi].blk = origBfp->xmap.xtnt[origXi].blk +
              (page - origBfp->xmap.xtnt[origXi].page) * ADVFS_PGSZ_IN_BLKS;
        }
        mergeXi++;
        assert(mergeXi < xtntCnt);

        if ( cloneXi < cloneBfp->xmap.cnt - 1 ) {
            nextClonePage = cloneBfp->xmap.xtnt[cloneXi + 1].page;
        } else {
            nextClonePage = cloneBfp->pages;
        }
        assert(origXi + 1 < origBfp->xmap.cnt);
        nextOrigPage = origBfp->xmap.xtnt[origXi + 1].page;

        if ( nextOrigPage < nextClonePage ) {
            /* We used the entire orig extent. Move on to the next. */
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            page = nextOrigPage;
        } else if ( nextOrigPage > nextClonePage ) {
            /* We passed the entire clone hole. Move on to the next extent. */
            cloneXi++;
            page = nextClonePage;
        } else {
            /* The orig extent is used up, the clone hole is passed. Move on. */
            cloneXi++;
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            page = nextOrigPage;
        }
    }

    /* Write a terminator in the merged extents. */
    assert(mergeXi > 0);
    xtnts[mergeXi].vol = xtnts[mergeXi - 1].vol;
    xtnts[mergeXi].page = page;
    xtnts[mergeXi].blk = XTNT_TERM;
    mergeXi++;
    assert(mergeXi < xtntCnt);

    xtnts = realloc( xtnts, mergeXi * sizeof(extentT) );
    free( cloneBfp->xmap.xtnt );
    cloneBfp->xmap.xtnt = xtnts;
    cloneBfp->xmap.cnt = mergeXi;
}

/*****************************************************************************/
/* Read the (R)BMT page with the prim mcell. Find the prim mcell and the
** BSR_XTNTS record. Load the extent from the record. Reserved files
** and non striped files in a V4 domain have extents in the primary
** mcell. V3 nonreserved files and striped files do not have extents
** in the primary mcell. In files that have extents in the primary mcell
** the BSR_XTNTS record tell how many mcells have extenst. Thsi is used
** to malloc the extent array in bfp.
**
** must have tag (file or set) since ppage==0 could be reg file in V4
** Input: bfp->fileTag - use RBMT for negative fileTag, else use BMT
**        bfp->dmn, bfp->dmn->vols[pvol]->(r)bmt - extents of (R)BMT
**        bfp->pvol, bfp->ppage, bfp->pcell - location of prim mcell
** Output: bfp->xmap.xtnts[0] filled in
**         bfp->pages set.
**         cvdi, cmcid - location of chain mcell
**         idxp - next location in bfp->xmap.xtnts[] to fill
**         mcCntp - number of extent mcells in the chain
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE - extents or mcells not as expected
**         ENOMEM - malloc failed
*/
static int
get_bsr_xtnts(bitFileT *bfp, int *idxp, vdIndexT *cvdi, bfMCIdT *cmcid, int *mcCntp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bmtpgp;
    bsMCT *mcellp;
    bfMCIdT bfMCId;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pvol]);

    if ( USE_RBMT ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[bfp->pvol]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfp->pvol]->bmt;
    }
    assert(mdBfp);

    ret = read_page(mdBfp, bfp->ppage);
    if ( ret != OK ) {
        fprintf(stderr, "get_bsr_xtnts: read_page (tag %d, page %d) failed.\n",
          mdBfp->fileTag, bfp->ppage);
        return ret;
    }

    bmtpgp = (bsMPgT*)mdBfp->pgBuf;
    (void)test_bmt_page(mdBfp, "get_bsr_xtnts", USE_RBMT);

    mcellp = &bmtpgp->bsMCA[bfp->pcell];
    bfMCId.page = bfp->ppage;
    bfMCId.cell = bfp->pcell;
    (void)test_mcell(bfp, "get_bsr_xtnts", bfp->pvol, bfMCId, mcellp);

    ret = load_bsr_xtnts(bfp, idxp, mcellp, cvdi, cmcid, mcCntp);
    return ret;
}

/*****************************************************************************/
/* Given a mcell, find the BSR_XTNTS record and load the extent into
** bfp->xmap.xtnts[0]. Set idxp to point to next (1) array entry.
**
** Input: mcellp - mcell contents with BSR_XTNTS record
** Output: bfp->xmap.xtnts[0] filled in
**         bfp->pages set.
**         idxp - set to 1 if BSR_XTNTS had one extent
**         cvdi, cmcid - pointer to chain mcell on disk
**         mcCntp - number of extent mcells in the chain
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if bad mcell contents.
**         ENOMEM - malloc failed
*/
static int
load_bsr_xtnts(bitFileT *bfp, int *idxp, bsMCT *mcellp, vdIndexT *cvdi,
               bfMCIdT *cmcid, int *mcCntp)
{
    bsXtntRT *bsXtntRp;
    int ret;
    int idx;

    ret = find_bmtr(mcellp, BSR_XTNTS, (char**)&bsXtntRp);
    if ( ret != OK ) {
        fprintf(stderr, "load_bsr_xtnts: Bad mcell %s vol %d page %d cell %d: ",
          RBMT_PRESENT ? "RBMT" : "BMT", bfp->pvol, bfp->ppage, bfp->pcell);
        fprintf(stderr, "No BSR_XTNTS record in primary mcell.\n");
        return BAD_DISK_VALUE;
    }

    /* xCnt can be 1 or 2 */
    if ( bsXtntRp->firstXtnt.xCnt > BMT_XTNTS ||
         bsXtntRp->firstXtnt.xCnt == 0) {
        fprintf(stderr, "load_bsr_xtnts: Bad mcell %s vol %d page %d cell %d: ",
          RBMT_PRESENT ? "RBMT" : "BMT", bfp->pvol, bfp->ppage, bfp->pcell);
        fprintf(stderr, "%d extents in primary mcell.\n",
          bsXtntRp->firstXtnt.xCnt);
        return BAD_DISK_VALUE;
    }

    *mcCntp = bsXtntRp->firstXtnt.mcellCnt;

    bfp->xmap.cnt = BMT_XTNTS;
    if ( bsXtntRp->firstXtnt.mcellCnt == 0 ) {
        bfp->xmap.cnt += BMT_XTRA_XTNTS;
    } else {
        bfp->xmap.cnt += (bsXtntRp->firstXtnt.mcellCnt - 1) * BMT_XTRA_XTNTS;
    }

    /* get one more than we need. see read_page */
    bfp->xmap.xtnt = malloc((bfp->xmap.cnt + 1) * sizeof(extentT));
    if ( bfp->xmap.xtnt == NULL ) {
        perror("load_bsr_xtnts: malloc failed");
        return ENOMEM;
    }

    /* load extents from primary mcell */
    idx = 0;
    get_xtnts(bfp, &idx, bfp->pvol,
              bsXtntRp->firstXtnt.bsXA, bsXtntRp->firstXtnt.xCnt);

    *idxp = idx;
    *cvdi = bsXtntRp->chainVdIndex;
    *cmcid = bsXtntRp->chainMCId;
    if ( *cvdi != 0 ) {
        if ( test_vdi(bfp, "load_bsr_xtnts", *cvdi) != OK )
            return BAD_DISK_VALUE;
        if ( test_mcid(bfp, "load_bsr_xtnts", *cvdi, *cmcid) != OK ) {
            fprintf(stderr,
              "load_bsr_xtnts: Bad mcell %s vol %d page %d cell %d: ",
              RBMT_PRESENT ? "RBMT" : "BMT", bfp->pvol, bfp->ppage, bfp->pcell);
            return BAD_DISK_VALUE;
        }
    }

    return OK;
}

/*****************************************************************************/
/* Raed the BMT page with the chain mcell. Find the chain mcell and the
** BSR_SHADOW_XTNTS record. Malloc extents array in bfp and load the extents
** into bfp->xmap->xtnts[]. V3 non reserved files and striped files use
** BSR_SHADOW_XTNTS records. This record tell how many mcells have extents.
**
** Input: *vdip, *pagep, cellp = shadow cell
** Output: bfp->xmap.xtnts filled in
**         bfp->pages set.
**         *idxp = next index in xtnts to fill in.
**         *vdip, *pagep, cellp = first xtra cell
*          *mcCntp = number of extent mcells in the chain
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad mcell contents
**         ENOMEM if malloc of extent array fails
*/
static int
get_shadow_xtnts(bitFileT *bfp, int *idxp, vdIndexT *vdip, bfMCIdT *bfMCIdp, int *mcCntp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bmtPgBuf;
    bsMCT *mcellp;
    bsShadowXtntT *shdwXtnt;
    int idx;

    assert(bfp);
    assert(bfp->fileTag);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn);
    /* TODO. assert(test_vdi(*vdip) == EOK) */
    /* TODO. assert(test_mcid(*bfMCIdp) == EOK) */

    if ( USE_RBMT ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[*vdip]->rbmt;
    } else {
        /* normal files use the bmt for their extents */
        mdBfp = bfp->dmn->vols[*vdip]->bmt;
    }
    assert(mdBfp);

    /* read the BMT page with the file's BSR_SHADOW_XTNTS mcell */
    if ( ret = read_page(mdBfp, bfMCIdp->page) ) {
        fprintf(stderr, "Read BMT page %d on volume %d failed.\n",
          bfMCIdp->page, *vdip);
        return ret;
    }

    bmtPgBuf = (bsMPgT*)mdBfp->pgBuf;
    (void)test_bmt_page(mdBfp, "get_shadow_xtnts", USE_RBMT);

    mcellp = &bmtPgBuf->bsMCA[bfMCIdp->cell];
    (void)test_mcell(bfp, "get_shadow_xtnts", *vdip, *bfMCIdp, mcellp);

    ret = find_bmtr(mcellp, BSR_SHADOW_XTNTS, (char**)&shdwXtnt);
    if ( ret != OK ) {
        fprintf(stderr, "get_shadow_xtnts: ");
        fprintf(stderr, "Bad mcell %s vol %d page %d cell %d: ",
          USE_RBMT ? "RBMT" : "BMT", *vdip, bfMCIdp->page, bfMCIdp->cell);
        fprintf(stderr, "No BSR_SHADOW_XTNTS record in mcell.\n");
        return BAD_DISK_VALUE;
    }

    if ( shdwXtnt->xCnt > BMT_SHADOW_XTNTS ) {
        fprintf(stderr, "get_shadow_xtnts: ");
        fprintf(stderr, "Bad mcell %s vol %d page %d cell %d: ",
          USE_RBMT ? "RBMT" : "BMT", *vdip, bfMCIdp->page, bfMCIdp->cell);
        fprintf(stderr, "%d extents in BSR_SHADOW_XTNTS record.\n",
          shdwXtnt->xCnt);
        return BAD_DISK_VALUE;
    }

    *mcCntp = shdwXtnt->mcellCnt;

    bfp->xmap.cnt = BMT_SHADOW_XTNTS;
    if ( shdwXtnt->mcellCnt == 0 ) {
        bfp->xmap.cnt += BMT_XTRA_XTNTS;
    } else {
        bfp->xmap.cnt += (shdwXtnt->mcellCnt - 1) * BMT_XTRA_XTNTS;
    }

    /* FIX. see read_page. get one more than we need, not needed */
    bfp->xmap.xtnt = malloc((bfp->xmap.cnt + 1) * sizeof(extentT));
    if ( bfp->xmap.xtnt == NULL ) {
        perror("get_shadow_xtnts: malloc failed");
        return ENOMEM;
    }

    if ( shdwXtnt->xCnt > bfp->xmap.cnt ) {
        fprintf(stderr, "too many extents\n");
        printf("get_shadow_xtnts: shdwXtnt->xCnt %d, bfp->xmap.cnt %d\n",
          shdwXtnt->xCnt,bfp->xmap.cnt);
        return BAD_DISK_VALUE;
    }

    idx = 0;
    get_xtnts(bfp, &idx, *vdip, shdwXtnt->bsXA, shdwXtnt->xCnt);

    *idxp = idx;
    *vdip = mcellp->nextVdIndex;
    *bfMCIdp = mcellp->nextMCId;
    if ( *vdip != 0 ) {
        if ( test_vdi(bfp, "get_shadow_xtnts", mcellp->nextVdIndex) != OK )
            return BAD_DISK_VALUE;
        ret = test_mcid(bfp, "get_shadow_xtnts",
                        mcellp->nextVdIndex, mcellp->nextMCId);
        if ( ret != OK )
            return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Read each (R)BMT page that contains a mcell in the list of xtra mcell.
** Find the xtra mcell and the BSR_XTRA_XTNTS record. Load the extents
** into bfp->xmap.xtnts.
** Since some files can have many extra mcells, a progress report is
** printed every 100 mcells read.
**
** Input: idx = index in bfp->xmap to start filling at
**        vol, pn, cn = first xtra mcell
**        stflg - striped file, non XTRA cell after chain of XTRA cells is OK
**        mcCnt - number of extent mcells in the chain
** Output: bfp->xmap.xtnts filled in
**         bfp->pages set.
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if mcell contents are bad
**         ENOMEM - malloc failed
*/
static int
get_xtra_xtnts(bitFileT *bfp, int idx, vdIndexT *vdip, bfMCIdT *bfMCIdp,
               int mcCnt, int stflg)
{
    int ret = OK;
    bitFileT *mdBfp;
    uint32T mcellCnt;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint32T bsXACnt;
    bsXtntT *bsXA;
    uint32T origMcCnt = mcCnt;
    bsXtraXtntRT  *xtraXtntRecp;

    assert(bfp);
    assert(bfp->fileTag);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn);

    /* TODO. assert idx OK */
    assert(test_vdi(bfp, NULL, *vdip) == OK);
    assert(test_mcid(bfp, NULL, *vdip, *bfMCIdp) == OK);

    mcellCnt = 1;       /* count the BSR_SHADOW_XTNTS mcell */

    while (*vdip > 0) {

        if ( USE_RBMT ) {
            /* reserved files use the rbmt for their extents */
            mdBfp = bfp->dmn->vols[*vdip]->rbmt;
        } else {
            /* normal files use the bmt for their extents */
            mdBfp = bfp->dmn->vols[*vdip]->bmt;
        }
        assert(mdBfp);

        if ( mcellCnt >= mcCnt ) {
            /*
            ** The chain of mcells is longer than expected from the mcellCnt
            ** in the BSR_XTNTS or BSR_SHADOW_XTNTS record.
            ** For a BSXMT_APPEND file, this means the short mcellCnt wrapped
            ** and we need to get more extent entrys in our array.
            ** For a BSXMT_STRIPE file the chain of mcells normaly extends
            ** beyond the mcellCnt from the BSR_SHADOW_XTNTS record for all
            ** but the last stripe. Each stripe just continues the chain of
            ** extent mcells and starts with a new BSR_SHADOW_XTNTS record.
            ** So if the next mcell contains a BSR_SHADOW_XTNTS record it is
            ** just a striped file. If the next mcell contains a BSR_XTRA_XTNTS
            ** record, it means the stripe has more than 64K mcells and
            ** mcellCnt wrapped for that stripe.
            */
            if ( ret = read_page(mdBfp, bfMCIdp->page) ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr,
                  "get_xtra_xtnts: Read BMT page %d on volume %d failed.\n",
                  bfMCIdp->page, *vdip);
                return ret;
            }

            bsMPgp = (bsMPgT*)mdBfp->pgBuf;

            bsMCp = &bsMPgp->bsMCA[bfMCIdp->cell];

            ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)&xtraXtntRecp);
            if ( ret != OK ) {
                break;
            }

            /* This means the on disk ushort mcellCnt wrapped. */
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "get_xtra_xtnts: mcellCnt wrapped. Adding room for 64K more.\n");
            bfp->xmap.cnt += 0x10000 * BMT_XTRA_XTNTS;
            bfp->xmap.xtnt = realloc(bfp->xmap.xtnt,
                bfp->xmap.cnt * sizeof(extentT));
            mcCnt += 0x10000;
        }

        if ( mcellCnt % 100 == 0 ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              " reading %d mcells, %d mcells read\r", mcCnt, mcellCnt);
        }

        if ( ret = read_page(mdBfp, bfMCIdp->page) ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "get_xtra_xtnts: Read BMT page %d on volume %d failed.\n",
              bfMCIdp->page, *vdip);
            return ret;
        }

        bsMPgp = (bsMPgT*)mdBfp->pgBuf;
        (void)test_bmt_page(mdBfp, "get_xtra_xtnts", USE_RBMT);

        bsMCp = &bsMPgp->bsMCA[bfMCIdp->cell];
        (void)test_mcell(bfp, "get_xtra_xtnts", *vdip, *bfMCIdp, bsMCp);

        ret = load_xtra_xtnts(bfp, &idx, bsMCp, vdip, bfMCIdp, stflg);
        if ( ret != OK ) {
            break;
        }

        mcellCnt++;
    }

    fprintf(stderr, CLEAR_LINE);
    bfp->xmap.cnt = idx;

    if ( mcellCnt != origMcCnt ) {
        fprintf(stderr,
          "get_xtra_xtnts: Bad mcellCnt. Expected %d, actual %d\n",
          origMcCnt, mcellCnt);
    }

    return ret;
}

/*****************************************************************************/
/* Given a mcell, find the BSR_XTRA_XTNTS record and load the extents into
** bfp->xmap.xtnts. Test the mcell contents. Set bfp->pages.
** If we didn't malloc enough when the BSR_XTNTS/BSR_SHADOW_XTNTS record
** was read, get some more now. This only happens if the mcell count
** in the BSR_XTNTS/BSR_SHADOW_XTNTS record is bad.
**
** Input: idxp - index in extent array to start loading
**        bsMCp - mcell contents
**        stflg - striped file, expect non  XTRA cell after chain of XTRA cells
** Output: bfp->xmap.xtnts filled in
**         bfp->pages set
**         idxp - incremented
**         vnp, bfMCIdp - next xtra mcell pointer
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT if no BSR_XTRA_XTNTS record found. Used for stripes
**         BAD_DISK_VALUE if mcell contents bad
**         ENOMEM if malloc fails
*/
static int
load_xtra_xtnts(bitFileT *bfp, int *idxp, bsMCT *bsMCp,
                vdIndexT *vnp, bfMCIdT *bfMCIdp, int stflg)
{
    bsXtraXtntRT  *xtraXtntRecp;
    uint32T bsXACnt;
    bsXtntT *bsXA;
    int ret;

    ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)&xtraXtntRecp);
    if ( ret != OK ) {
        if ( stflg ) {
            return ENOENT;
        }

        fprintf(stderr, "Bad %s mcell at volume, page, cell: %d %d %d\n",
          PRINT_BMT_OR_RBMT, *vnp, bfMCIdp->page, bfMCIdp->cell);
        fprintf(stderr, " Can't find BSR_XTRA_XTNTS record.\n");
        return BAD_DISK_VALUE;
    }

    bsXACnt = xtraXtntRecp->xCnt;
    bsXA = xtraXtntRecp->bsXA;
    if ( xtraXtntRecp->xCnt > BMT_XTRA_XTNTS ) {
        fprintf(stderr, "load_xtra_xtnts: %d extents in an xtra extent cell\n",
          xtraXtntRecp->xCnt);
        return BAD_DISK_VALUE;
    }

    /* TODO. Use realloc */
    if ( *idxp + bsXACnt > bfp->xmap.cnt ) {
        extentT *tmp;
        fprintf(stderr,
          "load_xtra_xtnts: more extents found (%d) than expected (%d).\n",
          *idxp + bsXACnt, bfp->xmap.cnt);
        tmp = malloc((*idxp + bsXACnt) * sizeof(extentT));
        if ( tmp == NULL ) {
            perror("load_xtra_xtnts: malloc failed");
            return ENOMEM;
        }

        bcopy(bfp->xmap.xtnt, tmp, bfp->xmap.cnt * sizeof(extentT));
        free(bfp->xmap.xtnt);
        bfp->xmap.xtnt = tmp;
        bfp->xmap.cnt = *idxp + bsXACnt;
    }

    get_xtnts(bfp, idxp, *vnp, bsXA, bsXACnt);

    *vnp = bsMCp->nextVdIndex;
    *bfMCIdp = bsMCp->nextMCId;
    if ( *vnp != 0 ) {
        if ( test_vdi(bfp, "load_xtra_xtnts", *vnp) != OK )
            return BAD_DISK_VALUE;
        if ( test_mcid(bfp, "load_xtra_xtnts", *vnp, *bfMCIdp) != OK )
            return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Given a disk extent array and a count, load the bfp->xmap.xtnts array.
** Test the extent contents.
**
** Input: bsXA = array of extents from disk
**        cnt = number of entries in bsXA
**        *idxp = start filling bfp->xmap here
** Output: bfp->xmap.xtnts (partially) filled in
**         bfp->pages - one page past last page (so far).
**         *idxp = one page past last page filled.
** Errors: diagnostic output to stderr for all errors
** Return: none
*/ 
/* TODO: rename to load_xtnts() only after load_xtnts is renamed */
static void
get_xtnts(bitFileT *bfp, int *idxp, vdIndexT vdi, bsXtntT *bsXA, int cnt)
{
    int i;
    uint32T lastp;

    /* TODO?     check cnt + *idxp <= bfp->xmap.cnt */
    if ( *idxp == 0 ) {
        lastp = 0;
        if ( bsXA[0].bsPage != 0 ) {
            fprintf(stderr, "Bad extent map: doesn't start at page 0\n");
        }
    } else {
        lastp = bfp->xmap.xtnt[*idxp - 1].page;
        if ( bsXA[0].bsPage == lastp ) {
            if ( bfp->xmap.xtnt[*idxp - 1].blk == XTNT_TERM ) {
                /* Collapse the two identical entries at the end of one */
                /* extent map and the beginning of the next extent map. */
                (*idxp)--;
                /* fool the test in the for loop */
                lastp--;
            } else {
                fprintf(stderr,
          "Bad extent map: extent in previous map at page %d not terminated.\n",
                  lastp);
            }
        } else {
            fprintf(stderr, "Bad extent map: fileset tag %d, file tag %d, ",
              bfp->setTag, bfp->fileTag);
            fprintf(stderr, "has a gap between extents after page %d.\n",
              lastp);
        }
    }

    for ( i = 0; i < cnt; i++, (*idxp)++ ) {
        bfp->xmap.xtnt[*idxp].vol = vdi;
        bfp->xmap.xtnt[*idxp].page = bsXA[i].bsPage;
        bfp->xmap.xtnt[*idxp].blk = bsXA[i].vdBlk;

        if ( *idxp != 0 && bsXA[i].bsPage <= lastp ) {
            fprintf(stderr,
      "Bad extent map: fileset tag %d, file tag %d, page %d follows page %d.\n",
              bfp->setTag, bfp->fileTag, bfp->xmap.xtnt[*idxp].page, lastp);
        }

        lastp = bsXA[i].bsPage;

        if ( test_blk(bfp, vdi, bsXA[i].vdBlk) != OK ) {
            fprintf(stderr, "Bad extent map: bad block (%u) at page %d\n",
              bfp->xmap.xtnt[*idxp].blk, bsXA[i].bsPage);
        }
    }

    bfp->pages = lastp;
}

/*****************************************************************************/
/* If this is the first volume opened, malloc and init bfp->dmn.
** Malloc and setup bfp->dmn->vols[vdi].
**
** Input: devname - device to open
** Output: bfp->type set to VOL
**         bfp->dmn - may be malloced
**         bfp->fd - set
**         vdi - volume index found by reading volume
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - volume doesn't exist
**         EBUSY - block device is mounted
**         EACCES - insufficient permissions
**         BAD_DISK_VALUE - bad magic or unexpected disk value
**         ENOMEM - malloc failed
*/
static int
resolve_vol(bitFileT *bfp, char *devname)
{
    bsDmnMAttrT *dmattr;
    int ret;

    bfp->type = VOL;
    if ( bfp->dmn == NULL ) {
        ret = init_dmn(bfp);
        if ( ret != OK ) {
            return ret;
        }
    }

    /* After this call, we know the Version of the on-disk structures */
    ret = open_vol(bfp, devname, &bfp->pvol);

    if ( bfp->dmn->vols[bfp->pvol] != NULL )
        bfp->fd = bfp->dmn->vols[bfp->pvol]->fd;

    return ret;
}

/*****************************************************************************/
/* Given a file name, open the file and test its length.
** All saved reserved files are multiples of 8K. Only the metadata
** of a frag file is saved. The metadata is 1K per 16 pages of frag file.
** So saved frag files should be multiples of 1K and we fake the original
** frag file lenght by multiplying each 1K by 16.
**
** Input: name - file pathname to open
**        statp - stats of file
**        bfp->fileTag - used to test length
** Output: bfp->type set to FLE
**         bfp->fd set to fd of file
**         bfp->pages set to number to 8K pages in file
**         bfp->fileName set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - no such file or bad size
**         EACCES - can't access "name" due to permissions
*/
static int
resolve_fle(bitFileT *bfp, char *name, struct stat *statp)
{
    int  fd;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    fd = open(name, O_RDONLY);
    if ( fd == -1 ) {
        sprintf(errstr, "open %s failed", name);
        perror(errstr);
        if ( errno == EACCES ) {
            return EACCES;
        } else {
            return ENOENT;
        }
    }

    bfp->type = FLE;
    strcpy(bfp->fileName, name);        /* check for NULL reseults */
    bfp->fd = fd;

    if ( statp->st_size == 0 ) {
            fprintf(stderr, "Bad saved file. File is 0 bytes long.\n");
            return ENOENT;
    }

    bfp->pages = statp->st_size / PAGE_SIZE;
    if ( bfp->fileTag < 0 ) {
        if ( bfp->pages * PAGE_SIZE != statp->st_size ) {
            fprintf(stderr, "Bad saved file. ");
            fprintf(stderr, "File is not a multiple of 8192 bytes long.\n");
            return ENOENT;
        }
    } else if ( bfp->fileTag == 1 ) {
        /* FIX. BF_FRAG_SLOT_BYTES (1024) needs msfs/bs_bitfile_sets.h */
        bfp->pages = statp->st_size / 1024;
        if ( bfp->pages * 1024 != statp->st_size ) {
            fprintf(stderr, "Bad saved FRAG file. ");
            fprintf(stderr, "File is not a multiple of 1024 bytes long.\n");
            return ENOENT;
        }
        /* FIX. BF_FRAG_GRP_PGS (16) reqires bs_bitfile_sets.h */
        bfp->pages *= 16;
    }

    return OK;
}

/*****************************************************************************/
/*
** return table
** return dev name
** Todo: check (but dont crash) for vdCnt, and dmnId
*/
static int
open_savset(char *name, bitFileT *bfp, int rawflg)
{
    int volFd;
    DIR *dirStrm;
    struct dirent *dp;
    char *cp;
    char errstr[80];
    char dirname[MAXPATHLEN];
    char rawname[MAXPATHLEN];
    char dname[MAXPATHLEN];
    int vnamel;
    int vol;
    int ret;

    errstr[0] = '\0';       /* just in case sprintf fails */

    bfp->fd = -1;
    ret = init_dmn(bfp);
    if ( ret != OK ) {
        return ret;
    }

    dirStrm = opendir(name);
    if ( dirStrm == NULL ) {
        fprintf(stderr, "bad domain name %s\n", name);
        return ENOENT;
    }

    for ( dp = readdir(dirStrm); dp != NULL; dp = readdir(dirStrm) ) {
        struct stat statBuf;
        dev_t dev;

        if ( strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 )
        {
            continue;
        }

        strcpy(dirname, name);
        strcat(dirname, "/");
        cp = &dirname[strlen(dirname)];
        *cp = '\0';
        strcat(dirname, dp->d_name);
        if ( stat(dirname, &statBuf) == -1 ) {
            closedir(dirStrm);
            sprintf(errstr, "open_savset: stat failed for volume %s", dirname);
            perror(errstr);
            return ENOENT;
        }

        if ( !S_ISDIR(statBuf.st_mode) ) {
            continue;
        }

        if ( open_savset_vol(bfp, dirname) != OK )
        {
            continue;
        }
    }

    closedir(dirStrm);

    return OK;
}

/*****************************************************************************/
static int
open_savset_vol(bitFileT *bfp, char *name)
{
    struct stat statBuf;
    char dirname[MAXPATHLEN];
    DIR *dirStrm;
    struct dirent *dp;
    bsMPgT bmt0;
    char *cp;
    char errstr[80];
    int bmt_found = 0;
    int fd;
    int ret;
    int vol;
    int attr_mcell = BFM_RBMT_EXT;       /* mcell with vol attributes */
    ssize_t read_ret;

    errstr[0] = '\0';       /* just in case sprintf fails */

    dirStrm = opendir(name);
    if ( dirStrm == NULL ) {
        fprintf(stderr, "bad domain name %s\n", name);
        return EINVAL;
    }

    for ( dp = readdir(dirStrm); dp != NULL; dp = readdir(dirStrm) ) {
        if ( strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 ) {
            continue;
        }

        if ( strcmp(dp->d_name, "bmt") == 0 ) {
            bmt_found = 1;
            break;
        }
    }

    closedir(dirStrm);
    if ( bmt_found == 0 ) {
        return BAD_DISK_VALUE;
    }

    strcpy(dirname, name);
    strcat(dirname, "/");
    cp = &dirname[strlen(dirname)];
    *cp = '\0';
    strcat(dirname, dp->d_name);
    if ( stat(dirname, &statBuf) == -1 ) {
        sprintf(errstr, "open_savset_vol: stat failed for volume %s", dirname);
        perror(errstr);
        return ENOENT;
    }

    if ( !S_ISREG(statBuf.st_mode) ) {
        fprintf(stderr, "bmt is not a regular file\n");
        return BAD_DISK_VALUE;
    }

    /* if size not mult 8k, error */
    fd = open(dirname, O_RDONLY);
    if ( fd == -1 ) {
        sprintf(errstr, "open_savset_vol: open fail for %s\n", dirname);
        perror(errstr);
        return ENOENT;
    }

    read_ret = read(fd, &bmt0, (size_t)sizeof(bmt0));
    if ( read_ret != sizeof(bmt0) ) {
        if ( read_ret == (ssize_t)-1 ) {
            perror("open_savset_vol: read page 0 failed");
        } else {
            fprintf(stderr, "read page 0 failed: read %ld bytes. wanted %d\n",
              read_ret,sizeof(bmt0));
        }
        return BAD_DISK_VALUE;
    }

    /* adjust for V3 domain */
    if ( bmt0.megaVersion < FIRST_RBMT_VERSION ) {
        attr_mcell = BFM_BMT_EXT_V3;
    }

    vol = ((bsVdAttrT *)&bmt0.bsMCA[attr_mcell].bsMR0[sizeof(bsMRT)])->vdIndex;
    /* FIX. test for vol out of bounds */
    if ( bfp->dmn->vols[vol] ) {
        printf("vol %d already found. ", vol);
        printf("Skipping %s\n", dirname);
        return OK;
    }

    bfp->dmn->vols[vol] = malloc(sizeof(volT));
    if ( bfp->dmn->vols[vol] == NULL ) {
        perror("open_savset_vol: malloc failed");
        return ENOMEM;
    }

    strcpy(bfp->dmn->vols[vol]->volName, dirname); /* ??? file name */

    bfp->dmn->vols[vol]->fd = fd;
    bfp->dmn->vols[vol]->blocks = (lbnT)statBuf.st_blocks;
    bfp->dmn->vols[vol]->rbmt = malloc(sizeof(bitFileT));
    if ( bfp->dmn->vols[vol]->rbmt == NULL ) {
        perror("open_savset_vol: malloc failed");
        return ENOMEM;
    }

    init_bitfile(bfp->dmn->vols[vol]->rbmt);
    bfp->dmn->vols[vol]->rbmt->type = FLE;
    bfp->dmn->vols[vol]->rbmt->pcell = BFM_BMT_V3;
    bfp->dmn->vols[vol]->rbmt->fd = fd;
    if ( ret = load_rbmt_xtnts(bfp->dmn->vols[vol]->rbmt) ) {
        return ret;
    }

    return OK;
}

/*****************************************************************************/
/*
** Dump bitfile "bfp" to file.
**
** Input: bfp->xmap - liat of all pages of file
**        bfp->dmn->vols[] - open volumes of domain on which pages reside
**        dfile = pathname of file to dump bitfile to
** Output: write file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         EACCES - any error in creat, lseek, read or write
**         BAD_DISK_VALUE - bad vdIndex or block number in exetnts
*/
/* todo: if dfile is a dir, put file under that */
int
dumpBitFile(bitFileT *bfp, char *dfile)
{
    int dumpFd;
    int fd = bfp->fd;
    int i;
    int pages;
    int totalPages = 0;
    int ret;
    char page[PAGE_SIZE];
    char errstr[80];
    long fileSize = 0;
    statT fs_stat;
    statT *statp;
    size_t writeSz = PAGE_SIZE;

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->type != FLE);
    dumpFd = creat(dfile, 0644);
    if ( dumpFd == -1 ) {
        sprintf(errstr, "dumpBitFile: creat \"%s\" failed", dfile);
        perror(errstr);
        return EACCES;
    }

    if ( bfp->setTag > 0 &&
         find_rec(bfp, BMTR_FS_STAT, (char**)&statp) == OK )
    {
        /* statp is unaligned! */
        bcopy(statp, &fs_stat, sizeof(fs_stat));
        statp = &fs_stat;
        fileSize = statp->st_size;
        printf("fileSize %ld\n", fileSize);
        if ( bfp->xmap.xtnt[bfp->xmap.cnt - 1].page - 1 > fileSize / PAGE_SIZE )
        {
            fprintf(stderr, "more pages than fileSize, dump all pages\n");
            fileSize = 0;
        }
    }

    for ( i = 0; i < bfp->xmap.cnt; i++ ) {
        off_t seek_off;

        if ( bfp->xmap.xtnt[i].blk == XTNT_TERM ) {
            /* Are these really here? */
            continue;
        }

        if ( bfp->type & DMN ) {
            if ( bfp->dmn->vols[bfp->xmap.xtnt[i].vol] == NULL ) {
                if ( bfp->type & DMN )
                    printf("No volume %d in domain %s\n",
                           bfp->xmap.xtnt[i].vol, bfp->dmn->dmnName);
                else
                    printf("No information about volume %d\n",
                           bfp->xmap.xtnt[i].vol);
                return BAD_DISK_VALUE;
            }
            fd = bfp->dmn->vols[bfp->xmap.xtnt[i].vol]->fd;
        }

        seek_off = (off_t)bfp->xmap.xtnt[i].blk * DEV_BSIZE;
        if ( lseek(fd, seek_off, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "dumpBitFile: lseek %u blocks failed",
                    bfp->xmap.xtnt[i].blk);
            perror(errstr);
            if ( errno == EACCES ) {
                return EACCES;
            } else {
                return BAD_DISK_VALUE;
            }
        }

        seek_off = (off_t)bfp->xmap.xtnt[i].page * PAGE_SIZE;
        if ( lseek(dumpFd, seek_off, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "dumpBitFile: lseek %d pages failed",
                    bfp->xmap.xtnt[i].page);
            perror(errstr);
            return EACCES;
        }

        pages = bfp->xmap.xtnt[i+1].page - bfp->xmap.xtnt[i].page;
        for ( ; pages; pages-- )
        {
            ssize_t io_ret;

            if ( (io_ret = read(fd, page, (size_t)PAGE_SIZE)) != PAGE_SIZE ) {
                fprintf(stderr, "read number %d from bitfile failed\n",
                        bfp->xmap.xtnt[i+1].page - pages);
                if ( io_ret == -1 ) {
                    perror("dumpBitFile: read failed");
                } else {
                    fprintf(stderr,
                      "dumpBitFile: read %d bytes, expected %d bytes\n",
                      io_ret, PAGE_SIZE);
                }
                return EACCES;
            }

            if ( fileSize != 0 && pages == 1 && i == bfp->xmap.cnt - 2 &&
                 fileSize < PAGE_SIZE * bfp->xmap.xtnt[bfp->xmap.cnt - 1].page )
            {
                /* last page. partial write */
                writeSz = fileSize % PAGE_SIZE;
            }

            if ( (io_ret = write(dumpFd, page, writeSz)) != writeSz ) {
                fprintf(stderr, "write number %d to file failed\n",
                        bfp->xmap.xtnt[i+1].page - pages);
                if ( io_ret == -1 ) {
                    perror("dumpBitFile: write failed");
                } else {
                    fprintf(stderr,
                      "dumpBitFile: wrote %d bytes, expected %d bytes\n",
                      io_ret, writeSz);
                }
                return EACCES;
            }

            totalPages++;
            if ( totalPages % 100 == 0 ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr, " dumping %d pages, %d pages dumped\r",
                  bfp->pages, totalPages);
            }
        }
    }

    fprintf(stderr, CLEAR_LINE);
    if ( writeSz == PAGE_SIZE ) {
        printf("%d 8K pages dumped.\n", totalPages);
    } else {
        printf("%d 8K pages + %d bytes dumped.\n", totalPages - 1, writeSz);
    }

    if ( close(dumpFd) == -1 ) {
        perror("dumpBitFile: close failed");
    }

    return OK;
}

/*****************************************************************************/
/* Search thru all volumes in domain to find volume with root TAG file.
** Read page 0 of (R)BMT on each volume and find BSR_XTNTS record
** in primary mcell for TAG file. If it has extents, we found it.
**
** Input: bfp->dmn->vols[]
** Output: bfp->xmap.xtnts loaded
**         bfp->pvol, bfp->ppage, bfp->pcell - set to prim mcell of TAG file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if TAG file not found
*/
int
find_TAG(bitFileT *bfp)
{
    bitFileT *mdBfp;
    bsXtntRT *xtntrp;
    bsMCT *mcellp;
    bsDmnMAttrT *bufDmnMAttrp;
    bsDmnMAttrT *memDmnMAttrp;
    int recFound = 0;
    int vdi;
    int roottagdirvdi;
    int mc;
    int dmamc;
    int maxSeqNum = 0;

    assert(bfp);
    if ( bfp->type & FLE ) {
        bfp->setTag = -BFM_BFSDIR;  /* -2 */
        bfp->ppage = 0;
        bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_BFSDIR);
        bfp->pcell = BFM_BFSDIR;
        return OK;
    }

    assert(bfp->dmn);
    assert(bfp->dmn->dmnVers != 0);

    if ( bfp->type & SAV ) {
        return find_saveset_TAG(bfp);
    }

    memDmnMAttrp = malloc(sizeof(bsDmnMAttrT));  /* freed at program exit */
    if ( memDmnMAttrp == NULL ) {
        return ENOMEM;
    }

    /* The root TAG primary mcell is at cell 2 for both versions. */
    mc = RBMT_PRESENT ? BFM_BFSDIR : BFM_BFSDIR_V3;
    /* The domain modifiable attributes mcell is different */
    dmamc = RBMT_PRESENT ? BFM_RBMT_EXT : BFM_BMT_EXT_V3;

    for ( vdi = 0; vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->dmn->vols[vdi] == NULL ) {
            continue;
        }

        mdBfp = RBMT_PRESENT ? bfp->dmn->vols[vdi]->rbmt :
                                bfp->dmn->vols[vdi]->bmt;
        assert(mdBfp);

        if ( read_page(mdBfp, 0) != OK ) {
            fprintf(stderr,
             "find_TAG: Warning. read_page 0 failed for %s on volume %d.\n",
              RBMT_PRESENT ? "RBMT" : "BMT", vdi);
            continue;
        }

        /* the mcell of the root tag dir - sanity check*/
        mcellp = &((bsMPgT *)PAGE(mdBfp))->bsMCA[mc];
        if ( (signed)mcellp->tag.num != RSVD_TAGNUM_GEN(vdi, mc) ) {
            fprintf(stderr,
            "WARNING. Bad tag (%d) in cell %d page 0 vol %d %s. Expected %d.\n",
              (signed)mcellp->tag.num, mc, vdi, RBMT_PRESENT ? "RBMT" : "BMT",
              RSVD_TAGNUM_GEN(vdi, mc));
            continue;
        }

        /* the mcell of the domain modifiable attributes */
        mcellp = &((bsMPgT *)PAGE(mdBfp))->bsMCA[dmamc];
        if ( !recFound ) {
            if ( find_bmtr(mcellp, BSR_DMN_MATTR, (char**)&bufDmnMAttrp) != OK){
                /* No BSR_DMN_MATTR on this volume. */
                continue;
            }

            recFound = 1;
            /* Save the BSR_DMN_MATTR record. */
            *memDmnMAttrp = *bufDmnMAttrp;
            maxSeqNum = bufDmnMAttrp->seqNum;
            roottagdirvdi = BS_BFTAG_VDI(bufDmnMAttrp->bfSetDirTag);
            if ( bfp->dmn->vols[roottagdirvdi] == NULL ) {
                fprintf(stderr, "WARNING: BSR_DMN_MATTR record says root TAG is on vol %d which does not exist.\n", roottagdirvdi);
                fprintf(stderr, "Try looking for root TAG on vol of BSR_DMN_MATTR record (%d).\n", vdi);
                roottagdirvdi = vdi;
            }

            if ( roottagdirvdi != vdi ) {
                fprintf(stderr, "WARNING: BSR_DMN_MATTR record on vol %d says root TAG is on vol %d.\n", vdi, roottagdirvdi);
            }
        } else {
            if ( find_bmtr(mcellp, BSR_DMN_MATTR, (char**)&bufDmnMAttrp) == OK){
                fprintf(stderr, "More than one BSR_DMN_MATTR record.\n");
            }

            if ( bufDmnMAttrp && bufDmnMAttrp->seqNum > maxSeqNum ) {
                /* Found another BSR_DMN_MATTR record. Save the latest. */
                maxSeqNum = bufDmnMAttrp->seqNum;
                *memDmnMAttrp = *bufDmnMAttrp;
                roottagdirvdi = BS_BFTAG_VDI(bufDmnMAttrp->bfSetDirTag);
            }

            continue;
        }
    }

    if ( !recFound ) {
        fprintf(stderr, "*** No BSR_DMN_MATTR record.\n");
    }

    if ( maxSeqNum == 0 ) {
        return BAD_DISK_VALUE;
    }

    /* we found the BSR_DMN_MATTR record with the maxSeqNum */
    bfp->pcell = mc;          /* this is mcell 2 */
    bfp->pvol = roottagdirvdi;
    bfp->ppage = 0;
    bfp->fileTag = memDmnMAttrp->bfSetDirTag.num;
    bfp->setTag = -BFM_BFSDIR;   /* -2 */

    return load_xtnts(bfp);
}

/*****************************************************************************/
static int
find_saveset_TAG(bitFileT *bfp)
{
    DIR *dirStrm;
    char dirname[MAXPATHLEN];
    struct dirent *dp;
    char *cp;
    int fd;
    int ret;
    char errstr[80];
    struct stat statBuf;

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->dmn->dmnName != NULL);
    dirStrm = opendir(bfp->dmn->dmnName);
    if ( dirStrm == NULL ) {
        fprintf(stderr, "bad domain name %s\n", bfp->dmn->dmnName);
        return EACCES;
    }

    for ( dp = readdir(dirStrm); dp != NULL; dp = readdir(dirStrm) ) {

        if ( strcmp(dp->d_name, "tag") == 0 ) {
            closedir(dirStrm);
            strcpy(dirname, bfp->dmn->dmnName);
            strcat(dirname, "/");
            cp = &dirname[strlen(dirname)];
            *cp = '\0';
            strcat(dirname, dp->d_name);
            if ( stat(dirname, &statBuf) == -1 ) {
                sprintf(errstr, "find_saveset_TAG: stat failed for volume %s",
                  dirname);
                perror(errstr);
                return BAD_DISK_VALUE;
            }

            if ( !S_ISREG(statBuf.st_mode) ) {
                printf("bmt is not a regular file\n");
                return BAD_DISK_VALUE;
            }

            ret = resolve_fle(bfp, dirname, &statBuf);

            return ret;
        }
    }

    closedir(dirStrm);

    return ENOENT;
}

/*****************************************************************************/
/* Find a the primary extents and load the extent for a file in a fileset.
** The fileset and/or the file can be specified by tag.
**
** Input: argv, argc, optindp - filset name/tag and file name/tag
** Output: bfp->xmap.xtnts filled in
**         bfp->pages filed in
**         bfp->pvol, bfp->ppage, bfp->pcell - set to prim mcell of file
**         optindp points to next input arg
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_SYNTAX - too many args, args not the right kind (int vs string)
**         EINVAL - bad arg range
**         ENOENT - no such set or file
**         BAD_DISK_VALUE - bad extents or tag file or other unexpected value
**         ENOMEM - malloc failed
*/
int
get_set_n_file(bitFileT *bfp, char *argv[], int argc, int *optindp)
{
    bitFileT *setBfp;
    bitFileT *rootBfp;
    int ret;

    assert(bfp->dmn);

    setBfp = malloc(sizeof(bitFileT));
    if ( setBfp == NULL ) {
        return ENOMEM;
    }
    rootBfp = malloc(sizeof(bitFileT));
    if ( rootBfp == NULL ) {
        return ENOMEM;
    }

    newbf(bfp, setBfp);
    newbf(bfp, rootBfp);

    assert(argc > *optindp);

    /* load xtnts of root TAG file */
    if ( (ret = find_TAG(rootBfp)) != OK ) {
        fprintf(stderr, "Couldn't find the root TAG file\n");
        return ret;
    }

    if ( (ret = get_set(rootBfp, argv, argc, optindp, setBfp)) != OK ) {
        return ret;
    }

    if ( (ret = get_file(setBfp, argv, argc, optindp, bfp)) != OK ) {
        return ret;
    }

    return OK;
}

/*****************************************************************************/
/* Given the definition of the root TAG file and the name or tag of a file set,
** find the primary mcell and load the extents of the fileset.
** The fileset can be specified by name or tag.
** "bfp" is filled in to represet the fileset's TAG file.
**
** Input: argv, argc, optindp - fileset name or tag
**        rootTag - definition of root TAG file
** Output: bfp->xmap.xtnts - filled in for fileset
**         bfp->pages - set
**         bfp->pvol, bfp->ppage, bfp->pcell - primary mcell of fileset
**         bfp->setTag, bfp->fileTag - set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_SYNTAX - command line syntax errors
**         EINVAL - out of range numbers
**         ENOENT - no such set
**         BAD_DISK_VALUE - bad extents or tag file or other unexpected value
**         ENOMEM - malloc failed
*/
int
get_set(bitFileT *rootTag, char *argv[], int argc, int *optindp, bitFileT *bfp)
{
    bfTagT tag;
    long longnum;
    int ret;
    bsBfSetAttrT *bsBfSetAttrp;

    bfp->setTag = -BFM_BFSDIR;  /* -2 */

    /* get set by tag */
    if ( strcmp(argv[*optindp], "-T") == 0 ) {
        (*optindp)++;
        if ( argc == *optindp ) {
            fprintf(stderr, "Missing fileset tag number\n");
            return BAD_SYNTAX;
        }

        /* get set tag */
        if ( getnum(argv[*optindp], &longnum) != OK ) {
            fprintf(stderr, "Badly formed set tag number: \"%s\"\n",
              argv[*optindp]);
            (*optindp)++;
            return BAD_SYNTAX;
        }

        (*optindp)++;

        if ( longnum < 1 || longnum > MAXTAG ) {
            fprintf(stderr, "Invalid fileset tag number (%d). ", longnum);
            fprintf(stderr, "Valid range is 1 through %d\n", MAXTAG);
            return EINVAL;
        }

        ret = get_file_by_tag(rootTag, (uint32T)longnum, bfp);
        if ( ret == ENOENT ) {
            fprintf(stderr, "Fileset tag %d is not in use.\n", (int)longnum);
            return ret;
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Fileset tag %d has corruption in it's mcells.\n", (int)longnum);
            return ret;
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding fileset tag %d.\n", (int)longnum);
            return ret;
        }

        /* get_file_by_tag clobbers setTag */
        bfp->setTag = -BFM_BFSDIR;  /* -2 */

        ret = find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp);
        if ( ret != OK ) {
            /* FIX. let find_rec print for BAD_DISK. print here for ENOENT */
            fprintf(stderr,
    "Primary mcell at vol %d, page %d, cell %d: can't find BSR_BFS_ATTR rec\n",
              bfp->pvol, bfp->ppage, bfp->pcell);
            return ret;
        }

        strcpy(bfp->setName, bsBfSetAttrp->setName);
        if ( bsBfSetAttrp->origSetTag.num != 0 ) {
            bfp->origSetp = malloc(sizeof(bitFileT));
            if ( bfp->origSetp == NULL ) {
                perror("malloc origSet");
                goto done;
            }

            newbf(bfp, bfp->origSetp);
            ret = get_file_by_tag(rootTag, bsBfSetAttrp->origSetTag.num, bfp->origSetp);
            if ( ret == ENOENT ) {
                fprintf(stderr, "Fileset tag %d is not in use.\n", (int)longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
                  "Fileset tag %d has corruption in it's mcells.\n", (int)longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding fileset tag %d.\n", (int)longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            }
            /* get_file_by_tag clobbers this */
            bfp->origSetp->setTag = -BFM_BFSDIR;  /* -2 */
        }
done:
        return OK;

    } else if ( bfp->type == FLE ) {
        printf("Can't get fileset by name from saved tag file.\n");
        return ENOENT;
    } else { /* get set by name */
        if ( strcmp(argv[*optindp], "-S") == 0 ) {
            (*optindp)++;
            if ( argc == *optindp ) {
                fprintf(stderr, "Missing fileset name\n");
                return BAD_SYNTAX;
            }
        }

        /* replace root TAG def in bfp with def of set tag bf */
        ret = find_set_by_name(rootTag, argv[*optindp], bfp);
        if ( ret == ENOENT ) {
            /* get fileset tag */
            if ( getnum(argv[*optindp], &longnum) != OK ) {
                fprintf(stderr, "No such fileset: \"%s\"\n", argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            if ( longnum < 1 || longnum > MAXTAG ) {
                fprintf(stderr, "No such fileset tag number or name (%s).\n",
                  argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            ret = get_file_by_tag(rootTag, (uint32T)longnum, bfp);
            if ( ret == ENOENT ) {
                fprintf(stderr,
                  "There is no fileset with name or tag %s in this domain.\n",
                  argv[*optindp]);
                  (*optindp)++;
                  return ret;
            } else if ( ret == BAD_DISK_VALUE ) {
                fprintf(stderr,
                  "Fileset tag %d has corruption in it's mcells.\n",
                  (int)longnum);
                (*optindp)++;
                return ret;
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding fileset tag %d.\n",
                  (int)longnum);
                (*optindp)++;
                return ret;
            }

            /* get_file_by_tag clobbers this */
            bfp->setTag = -BFM_BFSDIR;  /* -2 */

            ret = find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp);
            if ( ret != OK ) {
                /* find_rec prints error. don't do here */
                fprintf(stderr,
    "Primary mcell at vol %d, page %d, cell %d: can't find BSR_BFS_ATTR rec\n",
                  bfp->pvol, bfp->ppage, bfp->pcell);
                (*optindp)++;
                return ret;
            }

            strcpy(bfp->setName, bsBfSetAttrp->setName);
            if ( bsBfSetAttrp->origSetTag.num != 0 ) {
                bfp->origSetp = malloc(sizeof(bitFileT));
                if ( bfp->origSetp == NULL ) {
                    perror("malloc origSet");
                    goto done1;
                }

                newbf(bfp, bfp->origSetp);
                ret = get_file_by_tag(rootTag, bsBfSetAttrp->origSetTag.num,
                  bfp->origSetp);
                if ( ret == ENOENT ) {
                    fprintf(stderr, "Fileset tag %d is not in use.\n",
                      (int)longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                } else if ( ret == BAD_DISK_VALUE ) {
                    fprintf(stderr,
                      "Fileset tag %d has corruption in it's mcells.\n",
                      (int)longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                } else if ( ret != OK ) {
                    fprintf(stderr, "Error in finding fileset tag %d.\n",
                      (int)longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                }
                /* get_file_by_tag clobbers this */
                bfp->origSetp->setTag = -BFM_BFSDIR;  /* -2 */
            }
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Fileset %s has corruption in it's mcells.\n", argv[*optindp]);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding fileset %s.\n", argv[*optindp]);
        }

done1:
        /* FIX. move up */
        (*optindp)++;   /* count the fileset name */
    }

    return ret;
}

/*****************************************************************************/
/* Read each root TAG page. For each fileset, read the (R)BMT page of the
** primary mcell and find the BSR_BFS_ATTR record. Compare the given name
** with the fileset name in the BSR_BFS_ATTR record. If it matches load
** the fileset extents.
**
** Input: name = fileset name
**        rootBfp - root TAG file
** Output: bfp->xmap.xtnts, bfp->pages - set
**         bfp->setName - set
**         bfp->pvol, ppage, pcell - set to primary mcell of fileset TAG file
**         bfp->setTag - set to root set tag (-2)
**         bfp->fileTag - set to fileset tag in root TAG file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went OK
**         ENOENT if no such fileset
**         BAD_DISK_VALUE - bad extents or tag file or directory
**         ENOMEM - malloc failed
*/
static int
find_set_by_name(bitFileT *rootBfp, char *name, bitFileT *bfp)
{
    int j;
    int vi;
    int pg;
    int mc;
    bsMPgT *page;
    bsBfSetAttrT *bsBfSetAttrp;
    bfMCIdT bfMCId;
    int i;
    bsTDirPgT *pdata;
    bsXtntRT *bsXtntRp;
    int xtntOff;
    bsMRT *recordp;
    int ret;
    bfTagT tag;

    ret = get_max_tag_pages(rootBfp, &pg);
    if ( ret != OK ) {
        return ret;
    }

    tag.seq = 0;
    /* for each tag (fileset) in the root tag file */
    for ( tag.num = 1; tag.num < pg * BS_TD_TAGS_PG; tag.num++ ) {
        ret = get_prim_from_tagdir(rootBfp, &tag, bfp);
        if ( ret != OK ) {
            continue;
        }

        if ( find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp) != OK ) {
            fprintf(stderr,
    "Primary mcell at vol %d, page %d, cell %d: can't find BSR_BFS_ATTR rec\n",
              bfp->pvol, bfp->ppage, bfp->pcell);
            continue;
        }

        /*  cmp name to setName */
        if ( strcmp(bsBfSetAttrp->setName, name) != 0 ) {
            continue;
        }

        strcpy(bfp->setName, name);
        if ( bsBfSetAttrp->origSetTag.num != 0 ) {
            if ( bsBfSetAttrp->origSetTag.num > 0x7fffffff ) {
                fprintf(stderr, "bad set number\n");
            } else {
                (void)get_orig_set(rootBfp, bsBfSetAttrp->origSetTag.num, bfp);
            }
        }

        if ( bfp->type & SAV ) {
            get_savset_file(bfp, name);
        } else {
            ret = load_xtnts(bfp);
            if ( ret != OK ) {
                fprintf(stderr,
                  "find_set_by_name: load_xtnts_from_prim failed\n");
                return ret;
            }
        }
        return OK;
    }

    return ENOENT;
}

/*****************************************************************************/
/*
** Find the number of good tag pages. When a tag file is expanded it grows
** by 8 pages at a time. However the pages are not all initialized.
** The first entry on the first page tells how many pages are initialized
** ie howmany valid pages there are in the file.
**
** Input: tagBfp is the tag (root or fileset) tag file
** Output: pgs is the returned number of valid pages in the tag file.
** Errors: diagnostic output to stderr for bad values
** Return: OK if all is well
**         BAD_DISK_VALUE if tag page header has a bad page number
**         ENOMEM if malloc fails
*/
int
get_max_tag_pages(bitFileT *tagBfp, int *pgs)
{
    bsTDirPgT *tagDirPg;
    int ret;

    /*
     * Get a page in the tagfile and check the page header number.
     * If this can't be done, return a failure code.
     *
     * Then, if the first uninitialized page has a bad value, then use
     * the value from the tagfile.  Both values could be wrong, but
     * read_tag_page() has already performed a basic sanity check on
     * the bitFile struct.  A kernel routine should err on the side of
     * caution to prevent data corruption.  Vods tools, however, need
     * to be able to report on on-disk metadata (particularly bad
     * metadata), so try to work around a bad first uninitialized page.
     */

    ret = read_tag_page(tagBfp, 0, &tagDirPg);
    if ( ret != OK ) {
        return ret;
    }

    if ( tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg > tagBfp->pages ) {
        fprintf(stderr, "get_max_tag_pages: Bad unInitPg %d. Must be < %d\n",
          tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg, tagBfp->pages);

        *pgs = tagBfp->pages;
    } else {

        *pgs = tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg;
    }
 
    return OK;
}

/*****************************************************************************/
/*
** Read a page in the tag file and check the page header page number
**
** Input: tagBfp is the tag (root or fileset) tag file
**        pg is the page number to read.
** Output: retBsTDirPg is the returned tag page
** Errors: diagnostic output to stderr for bad values
** Return: OK if all is well
**         BAD_DISK_VALUE if tag page header has a bad page number
**         ENOMEM if malloc fails
*/
int
read_tag_page(bitFileT *tagBfp, int pg, bsTDirPgT **retBsTDirPg)
{
    bsTDirPgT *tagDirPg;
    int ret;

    ret = read_page(tagBfp, pg);
    if ( ret != OK ) {
        fprintf(stderr,
          "read_tag_page: can't read page %d of the tag file\n", pg);
        return ret;
    }

    tagDirPg = (bsTDirPgT*)tagBfp->pgBuf;
    *retBsTDirPg = tagDirPg;

    if ( tagDirPg->tpPgHdr.currPage != pg ) {
        fprintf(stderr, "Bad tag page. Read page %d, currPage is %d.\n",
          pg, tagDirPg->tpPgHdr.currPage);
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Given the tag of the original set for the clone set, find the original
** set and attach it to the clone set bitFile structure.
**
** Input:  rootBfp - root TAG file
**         tagnum - tag number of original set
**         bfp - clone set
** Output: bfp->origSetp - setup
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went OK
**         ENOENT if no such fileset
**         BAD_DISK_VALUE - bad extents or tag file or directory
**         ENOMEM - malloc failed
*/
static int
get_orig_set(bitFileT *rootBfp, int tagnum, bitFileT *bfp)
{
    int ret;

    bfp->origSetp = malloc(sizeof(bitFileT));
    if ( bfp->origSetp == NULL ) {
        perror("malloc origSet");
        return ENOMEM;
    }
    newbf(bfp, bfp->origSetp);

    ret = get_file_by_tag( rootBfp, tagnum, bfp->origSetp);
    /* get_file_by_tag clobbers this */
    bfp->origSetp->setTag = -BFM_BFSDIR;  /* -2 */

    if ( ret == ENOENT ) {
        fprintf(stderr, "Fileset tag %d is not in use.\n", tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Fileset tag %d has corruption in it's mcells.\n",
             tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    } else if ( ret != OK ) {
        fprintf(stderr, "Error in finding fileset tag %d.\n", tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    }

    return OK;
}

/******************************************************************************/
/* Find the primary mcell and load the extents of a file given the fileset TAG
** file and a file path name or a file tag. The path name is relative to the
** root of the fileset. Ie it does not begin with a /, nor does it include
** the mount point (especially since the fileset is probably not mounted).
**
** Input: argv, argc, optindp - file pathname or file tag number
**        setBfp is the fileset TAG file
** Output: bfp->pvol, bfp->ppage, bfp->pcell - set to primary mcell of file
**         bfp->xtnts - filled in for file in fileset
** Errors: diagnostic output to stderr for all errors
** Return: Ok if all went well
**         BAD_SYNTAX for cammand line syntax errors
**         EINVAL for out of range numbers
**         ENOENT - no such file
**         BAD_DISK_VALUE - bad extents, tag file, directory, etc
**         EINVAL - tag number out of range
**         ENOMEM - malloc failed
*/
/* TODO: Do we need -f <name> ? else how to handle file named "-t" */
int
get_file(bitFileT *setBfp, char *argv[], int argc, int *optindp,
               bitFileT *bfp)
{
    long longnum;
    bfTagT tag;
    int ret;

    /* get file by tag */
    if ( strcmp(argv[*optindp], "-t") == 0 ) {
        (*optindp)++;   /* count the -t */

        if ( argc == *optindp ) {
            fprintf(stderr, "Missing file tag number.\n");
            return BAD_SYNTAX;
        }

        /* get file tag number */
        if ( getnum(argv[*optindp], &longnum) != OK ) {
            fprintf(stderr, "Badly formed set tag number: \"%s\"\n",
              argv[*optindp]);
            (*optindp)++;   /* count the tag number */
            return BAD_SYNTAX;
        }

        (*optindp)++;   /* count the tag number */

        if ( longnum < 1 || longnum > MAXTAG ) {
            fprintf(stderr, "Invalid file tag number (%d). ", longnum);
            fprintf(stderr, "Valid range is 1 through %d\n", MAXTAG);
            return EINVAL;
        }

        ret = get_file_by_tag(setBfp, (int)longnum, bfp);
        if ( ret == ENOENT ) {
            fprintf(stderr, "File tag %d is not in use.\n", (int)longnum);
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "File tag %d has corruption in it's mcells.\n", (int)longnum);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding file tag %d.\n", (int)longnum);
        }
    } else { /* get file by name */
        if ( bfp->type & FLE ) {        /* sb SAV */
            printf("don't have dir to lookup file\n");
            return EINVAL;
        }

        if ( strcmp(argv[*optindp], "-F") == 0 ) {
            (*optindp)++;
            if ( argc == *optindp ) {
                fprintf(stderr, "Missing fileset name\n");
                return BAD_SYNTAX;
            }
        }

        ret = find_file_by_name(setBfp, argv[*optindp], bfp);
        if ( ret == ENOENT ) {
            /* Couldn't find a file called 123. Look for tag 123. */
            if ( getnum(argv[*optindp], &longnum) != OK ) {
                fprintf(stderr, "No such file: \"%s\"\n", argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            if ( longnum < 1 || longnum > MAXTAG ) {
                fprintf(stderr, "No such file tag number or name (%s).\n",
                  argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            ret = get_file_by_tag(setBfp, (int)longnum, bfp);
            if ( ret == ENOENT ) {
                fprintf(stderr,
                  "There is no file with name or tag %s in this fileset.\n",
                  argv[*optindp]);
            } else if ( ret == BAD_DISK_VALUE ) {
                fprintf(stderr,
                  "File tag %d has corruption in it's mcells.\n", (int)longnum);
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding file tag %d.\n",
                  (int)longnum);
            }
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Found corrupted metadata while searching for \"%s\".\n",
              argv[*optindp]);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding file %s.\n", argv[*optindp]);
        }

        (*optindp)++;   /* count the file name */
    }

    return ret;
}

/*****************************************************************************/
/* Given a root or fileset TAG file and a tag, fill in bfp and load the extents.
**
** Input: tagBfp - TAG file
**        tagnum - tag of file in the TAG file
** Output: bfp - file to init
** Errors: status return only. No diagnostic printf
** Return: OK if all goes well
**         ENOENT if tag not in use in TAG file
**         BAD_DISK_VALUE - bad extents
**         ENOMEM - malloc failed
*/
int
get_file_by_tag(bitFileT *tagBfp, int tagnum, bitFileT *bfp)
{
    bfTagT tag;
    int ret;

    tag.num = (uint32T)tagnum;
    tag.seq = 0;

    bfp->setTag = tagBfp->fileTag;
    bfp->setp = tagBfp;

    /* get the prim mcell of the file/set from the given TAG file */
    if ( get_prim_from_tagdir(tagBfp, &tag, bfp) != OK ) {
        return ENOENT;
    }

    return load_clone_xtnts(bfp);
}

/*****************************************************************************/
/*
** Find the prime mcell given the fileset tag and the file tag.
** Reads the fileset tag file to obtain the prime mcell information.
**
** Input: tagBfp is the tag (root or fileset) tag file
**        tag is the tag to look up in the tag file
**        tag.seq - if 0, just match num. else match seq also
** Output: bfp->pvol, ppage, pcell - set to the prim mcell
**         bfp->fileTag
** Errors: diagnostic output to stderr for all errors
** Return: OK if all goes well
**         BAD_DISK_VALUE if some value read from disk is out of bounds
**         ENOENT if tag is not in use
**         ENOMEM malloc failed
*/
static int
get_prim_from_tagdir(bitFileT *tagBfp, bfTagT *tag, bitFileT *bfp)
{
    bsTDirPgT *tagDirPg;
    int ret;
    int pgs;

    if ( get_max_tag_pages(tagBfp, &pgs) == OK && TAGTOPG(tag) >= pgs ) {
        fprintf(stderr, "Tag %d is beyond the valid pages in the tag file.\n",
          tag->num);
        return ENOENT;
    }

    ret = read_tag_page(tagBfp, TAGTOPG(tag), &tagDirPg);
    if ( ret != OK ) {
        return ret;
    }

    if ( !(tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s2.seqNo & BS_TD_IN_USE) )
        return ENOENT;

    bfp->pvol = tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.vdIndex;
    bfp->ppage = tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.bfMCId.page;
    bfp->pcell = tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.bfMCId.cell;
    bfp->fileTag = tag->num;

    if ( tag->seq &&
         tag->seq != tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.seqNo )
    {
        fprintf(stderr,
          "file tag %d: dir seq number %d does not match tag file seqNo %d.\n",
          tag->num, tag->seq, tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.seqNo);
    }

    /* test primary mcell vdi */
    if ( test_vdi(bfp, NULL, bfp->pvol) != OK ) {
        fprintf(stderr, "get_prim_from_tagdir: bad vdi (%d)\n", bfp->pvol);
        return BAD_DISK_VALUE;
    }

    /* FIX. use test_mcid */
    /* test primary mcell page */
    if ( bfp->ppage >= tagBfp->dmn->vols[bfp->pvol]->bmt->pages ) {
        fprintf(stderr, "Fileset %d tag file, file %d. ",
          tagBfp->fileTag, tag->num);
        fprintf(stderr, "Bad prim mcell page %d. ", bfp->ppage);
        fprintf(stderr, "Valid range: 0 - %d.\n",
           tagBfp->dmn->vols[bfp->pvol]->bmt->pages);
        return BAD_DISK_VALUE;
    }

    /* test primary mcell number */
    if ( bfp->pcell >= BSPG_CELLS ) {
        fprintf(stderr, "Tag file read for tag %d:\n", tag->num);
        fprintf(stderr, "primary mcell number (%d) is too large.\n",
                bfp->pcell);
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/*
** TODO: rename to get_file_prim_from_set() OR add load_xtnts()
*/
/* Find the primary mcell given a set tag and a fileset relative path name.
** Reads the fileset tag file to find the root dir prime mcell.
** Loads the root tag extents and reads the directory.
** For each component of the path: find the tag in the directory, find
** the prime mcell in the fileset tag file and load the extents.
** At the last component return the prime mcell. Does not load extents of
** found file.
**
** Input: tagBfp is tag file
**        path is fileset relative path
** Output: bfp->pvol, bfp->ppage, bfp->pcell - primary mcell of found file
** Errors: diagnostic output to stderr for all errors
** Return: OK if file found in set
**         ENOENT - file does not exist
**         BAD_DISK_VALUE - bad extents, tag file or directory
**         EINVAL - badly formed .tags pathname
**         ENOMEM - malloc failure
*/
#define TAG3 98

static int
find_file_by_name(bitFileT *tagBfp, char *path, bitFileT *bfp)
{
    bfTagT tag;
    bitFileT dirBf;
    long tagnum = 0;
    char *component;
    char *component_end;
    char *path_end;
    int ret;
    bitFileT *dirBfp = &dirBf;    /* Used for each dir in the path */

    bfp->setTag = tagBfp->fileTag;
    bfp->setp = tagBfp;

    if ( strchr(path, (int)'/') == NULL ) {
        getnum(path, &tagnum);
    }

    newbf(bfp, &dirBf);
    dirBf.setp = bfp->setp;
    /* tag 2 is the fileset root directory */
    tag.num = (uint32T)2;
    tag.seq = 0;
    ret = get_prim_from_tagdir(tagBfp, &tag, &dirBf);
    if ( ret != OK ) {
        return ret;
    }

    component = path;
    path_end = &path[strlen(path)];
    component_end = strchr(path, (int)'/');
    if ( component_end ) {
        *component_end = '\0';
    }

    while(1) {
        ret = load_clone_xtnts(&dirBf);
        if ( ret != OK ) {
            return ret;
        }

        ret = search_dir(tagBfp, dirBfp, component);
        if ( ret == TAG3 ) {
            ret = find_prim_from_dot_tags(component, dirBfp, tagBfp, bfp);
        }
        if ( ret != OK ) {
            return ret;
        }

        if ( &component[strlen(component)] == path_end ) {
            *bfp = *dirBfp;
            break;
        }

        component = &component[strlen(component)];
        *component = '/';
        component++;
        component_end = strchr(component, (int)'/');
        if ( component_end ) {
            *component_end = '\0';
        } else {
            /* last component in path */
            if ( ret == TAG3 )
                return ret;
        }
    }

    return load_clone_xtnts(bfp);
}

/*****************************************************************************/
/* The next path component in not in the directory.
** It is in the .tags directory. It is a tag.
** Called from find_file_by_name
**
** Input: component - path name in .tags dir
**        dirBfp - ???
**        tagBfp - tag file description
** Output: bfp - primary mcell filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK - component found and resolved
**         ENOENT - no such component
**         BAD_SYNTAX - not a number in .tags dir
**         EINVAL - invalid number in .tags
*/
static int
find_prim_from_dot_tags(char *component, bitFileT *dirBfp, bitFileT *tagBfp,
                       bitFileT *bfp)
{
    bfTagT tag;
    long longNum;

    /* this component is a tag. ie 3 or M-6 */
    /* get the prim mcell from the fileset tag file */
    if ( component[0] == 'M' ) {
        component++;
    }

    if ( getnum(component, &longNum) != OK ) {
        fprintf(stderr, "Badly formed set tag number: \"%s\"\n", component);
        return BAD_SYNTAX;
    }

    if ( longNum == 0 ) {
        fprintf(stderr, "Invalid tag number (0).\n");
        return EINVAL;
    }

    tag.num = (int)longNum;
    tag.seq = 0;
    if ( (signed int)tag.num < 0 ) {
        if ( find_rsrv_prim_from_tag(dirBfp, tag) != OK ) {
            fprintf(stderr, "tag %d does not exist\n", tag.num);
            return ENOENT;
        }

        *bfp = *dirBfp;
        return OK;
    }

    if ( get_prim_from_tagdir(tagBfp, &tag, dirBfp) != OK ) {
        fprintf(stderr, "tag %d does not exist\n", tag.num);
        return ENOENT;
    }

    *bfp = *dirBfp;
    return OK;
}

/*****************************************************************************/
/* Given the tag of a reserved file, find the primary mcell.
** The tag gives the volume and the reserved file.
** Reads every volume and lloks for BSR_VD_ATTR record with matching vdIndex.
** Called from find_prim_from_dot_tags.
**
** Input: bfp->dmn->vols[]->(r)bmt - (R)BMT files on volumes in domain
**        tag - tag of reserved file
** Output: bfp->fd - fd of found volume
**         bfp->pvol, bfp->ppage, bfp-pcell = primary mcell of reserved file
**         bfp->xmap setup
** Errors: diagnostic output to stderr for all errors
** Return: OK if reserved file found
**         EINVAL for bad tag number
**         ENOENT for no such reserved file tag found
**         errors from read_vol_at_blk
*/
static int
find_rsrv_prim_from_tag(bitFileT *bfp,  bfTagT tag)
{
    int vol = -(signed int)tag.num / BFM_RSVD_TAGS;
    int cell = -(signed int)tag.num % BFM_RSVD_TAGS;
    int i;
    int attr_mcell = BFM_RBMT_EXT;
    int ret;
    bsMPgT *bsMPgp;
    bsMCT *mcellp;
    bsVdAttrT *bsVdAttrp;
    bitFileT *mdBfp;

    assert(bfp);
    assert(bfp->dmn);
    assert(tag.num < 0);

    if ( cell == BFM_RBMT_EXT   ||
         (! RBMT_PRESENT && cell == BFM_BMT_EXT_V3) )
    {
        printf("No reserved file with tag %d\n", -(cell) );
        return EINVAL;
    }

    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        if ( bfp->dmn->vols[i] ) {

            if ( RBMT_PRESENT ) {
                /* reserved files use the rbmt for their extents */
                mdBfp = bfp->dmn->vols[i]->rbmt;
            } else {
                mdBfp = bfp->dmn->vols[i]->bmt;
            }

            mdBfp->pgLbn = MSFS_RESERVED_BLKS;  /* block 32 */
            ret = read_vol_at_blk(mdBfp);
            if ( ret != OK ) {
                return ret;
            }
            bsMPgp = (bsMPgT*)mdBfp->pgBuf;

            if ( !RBMT_PRESENT ) {
                attr_mcell = BFM_BMT_EXT_V3;
            }

            mcellp = &bsMPgp->bsMCA[attr_mcell];
            ret = find_bmtr(mcellp, BSR_VD_ATTR, (char**)&bsVdAttrp);
            if ( ret != OK ) {
                fprintf(stderr,
     "find_rsrv_prim_from_tag: Could not find BSR_VD_ATTR record in cell %d.\n",
                  attr_mcell);
                return ret;
            }

            if ( vol != bsVdAttrp->vdIndex ) {
                continue;
            }

            bfp->fd = bfp->dmn->vols[i]->fd;
            bfp->ppage = 0;
            bfp->pcell = cell;
            bfp->pvol = i;
            /* FIX. notice this only works for BMT, not any other rsv file. */
            bfp->xmap = bfp->dmn->vols[bfp->pvol]->rbmt->xmap;
            bfp->pages = bfp->dmn->vols[bfp->pvol]->rbmt->pages;

            return OK;
        }
    }

    printf("no volume %d corresponding to reserved tag %d\n", vol, tag.num);

    return ENOENT;
}

/******************************************************************************/
/* Given a mcell pointer (vol, page, mcell), read the page, find the mcell,
** find and test the chain pointer.
** Called from nvbmtpg.c & load_xtnts_from_prim.
**
** Input: bfp->pvol, bfp->ppage, bfp->pcell - point to the primary mcell
** Output: vol, mcidp - point to the chain mcell
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad mcell contents
*/
/* TODO: set setTag
**       if filetag == 0 set it
*/
int
get_chain_mcell(bitFileT *bfp, vdIndexT *vol, bfMCIdT *mcidp)
{
    bsMPgT    *mcpage;
    bsMRT     *recordp;
    int       xtntOff;
    bsXtntRT  *bsXtntRp;
    int ret;
    bitFileT *mdBfp;

    assert(bfp);
    if ( !(bfp->type & FLE) ) {
        assert(bfp->dmn);
        assert(bfp->pvol < BS_MAX_VDI);
        assert(bfp->dmn->vols[bfp->pvol]);
        assert(bfp->fileTag);

        if ( USE_RBMT ) {
            mdBfp = bfp->dmn->vols[bfp->pvol]->rbmt;
        } else {
            mdBfp = bfp->dmn->vols[bfp->pvol]->bmt;
        }
        assert(mdBfp);
        assert(bfp->ppage < mdBfp->pages);
        assert(bfp->pcell < BSPG_CELLS);

        /* read primary mcell from RBMT */
        if ( ret = read_page(mdBfp, bfp->ppage) ) {
            fprintf(stderr, "get_chain_mcell: read_page fail\n");
            return ret;
        }

        mcpage = (bsMPgT*)mdBfp->pgBuf;
    } else {
        /* read  page specified from the dump file */
        if ( ret = read_page(bfp, bfp->ppage) ) {
            fprintf(stderr, "get_chain_mcell: read_page fail\n");
            return ret;
        }

        mcpage = (bsMPgT*)bfp->pgBuf;
    }

    ret = find_bmtr(&mcpage->bsMCA[bfp->pcell], BSR_XTNTS, (char**)&bsXtntRp);
    if ( ret != OK ) {
        return BAD_DISK_VALUE;
    }

    *vol = bsXtntRp->chainVdIndex;
    *mcidp = bsXtntRp->chainMCId;

    if ( *vol == 0 ) {
        if ( mcidp->page != 0 ) {
            fprintf(stderr,
              "%s vol %d page %d cell %d: bad chainMCId.page (%d) for vdi=0\n",
              USE_RBMT ? "RBMT" : "BMT",
              bfp->pvol, bfp->ppage, bfp->pcell, mcidp->page);
            return BAD_DISK_VALUE;
        }
        if ( mcidp->cell != 0 ) {
            fprintf(stderr,
              "%s vol %d page %d cell %d: bad chainMCId.cell (%d) for vdi=0\n",
              USE_RBMT ? "RBMT" : "BMT",
              bfp->pvol, bfp->ppage, bfp->pcell, mcidp->page);
            return BAD_DISK_VALUE;
        }
        return OK;
    }

    if ( !(bfp->type & FLE) ) {
        if ( test_vdi(bfp, NULL, bsXtntRp->chainVdIndex) != OK ) {
            fprintf(stderr,
              "%s vol %d page %d cell %d: bad nextVdIndex %d\n",
              USE_RBMT ? "RBMT" : "BMT",
              bfp->pvol, bfp->ppage, bfp->pcell, bsXtntRp->chainVdIndex);
            return BAD_DISK_VALUE;
        }

        if ( test_mcid(bfp, "get_chain_mcell", *vol, *mcidp) != OK ) {
            fprintf(stderr,
             "%s vol %d page %d cell %d: bad chainMCId.\n",
              USE_RBMT ? "RBMT" : "BMT",
              bfp->pvol, bfp->ppage, bfp->pcell);
            return BAD_DISK_VALUE;
        }
    } else {
        if ( bsXtntRp->chainVdIndex != bfp->pvol ) {
            fprintf(stderr,
    "%s vol %d page %d cell %d: chainVdIndex (%d) indicates a different file. ",
                  USE_RBMT ? "RBMT" : "BMT",
                  bfp->pvol, bfp->ppage, bfp->pcell, bsXtntRp->chainVdIndex);
        }
    }
 
    return OK;
}

/******************************************************************************/
/* Given a mcell pointer (vdi, pg, cn) read the cell and get the next cell
** pointer. Test the next cell pointer.
**
** Input: bfp->type - saved file or disk
**        bfp->fileTag - for disk, select RBMT or BMT for mcells
**        bfp->dmn->vols[*vdi]->(r)bmt - pointers to BMT file
**        vdip, mcidp - point to current mcell
** Output: bfp->pgBuf - filled with next mcell page
**         vdip, mcidp - point to next mcell
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if cell has bad contents
*/
/* TODO: why read next page here? */
int
get_next_mcell(bitFileT *bfp, vdIndexT *vdip, bfMCIdT *mcidp)
{
    int ret;
    bsMCT *cellp;
    uint32T savev, savep, savec;
    bitFileT *mdBfp;

    if ( bfp->type & FLE ) {
        mdBfp = bfp;
    } else {
        assert(*vdip != 0);
        assert(*vdip < BS_MAX_VDI);
        assert(mcidp->cell < BSPG_CELLS);
        assert(bfp->dmn);
        assert(bfp->dmn->vols[*vdip]);
        assert(bfp->fileTag != 0);

        if ( USE_RBMT ) {
            mdBfp = bfp->dmn->vols[*vdip]->rbmt;
        } else {
            mdBfp = bfp->dmn->vols[*vdip]->bmt;
        }
        assert(mdBfp);
        assert(mcidp->page < mdBfp->pages);
    }

    ret = read_page(mdBfp, mcidp->page);
    if ( ret != OK ) {
        return ret;
    }

    cellp = &((bsMPgT*)mdBfp->pgBuf)->bsMCA[mcidp->cell];

#ifdef NOTYET        /* bmt doesn't set tag. tag = 0 */
    if ( cellp->tag.num != bfp->fileTag ) {
        fprintf(stderr, "Vdi %d, page %d, mcell %d: bad tag %d, expected %d\n",
          *vdip, mcidp->page, mcidp->cell,
          (signed int)cellp->tag.num, (signed int)bfp->fileTag);
        return BAD_DISK_VALUE;
    }
#endif

    /* test next vdi */
    if ( cellp->nextVdIndex >= BS_MAX_VDI ) {
        fprintf(stderr, "Vdi %d, page %d, mcell %d: bad nextVdIndex %d\n",
          *vdip, mcidp->page, mcidp->cell, cellp->nextVdIndex);
        return BAD_DISK_VALUE;
    }

    if ( !(bfp->type & FLE) ) {
        /* test next page */
        if ( cellp->nextMCId.page >= mdBfp->pages ) {
            fprintf(stderr, "Vdi %d, page %d, mcell %d: bad nextMCId.page %d\n",
              *vdip, mcidp->page, mcidp->cell, cellp->nextMCId.page);
            return BAD_DISK_VALUE;
        }
    }

    /* test next mcell number */
    if ( cellp->nextMCId.cell >= BSPG_CELLS ) {
        fprintf(stderr, "Vdi %d, page %d, mcell %d: bad nextMCId.cell %d\n",
          *vdip, mcidp->page, mcidp->cell, cellp->nextMCId.cell);
        return BAD_DISK_VALUE;
    }

    /* If the next mcell is in another page, read in that page */
    if ( cellp->nextVdIndex && (cellp->nextMCId.page != mcidp->page) ) {

        /* save these values; cellp points to new page after read_page */
        savev = cellp->nextVdIndex;
        savep = cellp->nextMCId.page;
        savec = cellp->nextMCId.cell;

        ret = read_page(mdBfp, cellp->nextMCId.page);
        if ( ret != OK ) {
            return ret;
        } else {
            *vdip = savev;
            mcidp->page  = savep;
            mcidp->cell  = savec;
            return OK;
        }
    }

    *vdip = cellp->nextVdIndex;
    *mcidp = cellp->nextMCId;

    return OK;
}

/*****************************************************************************/
/* Given the tag file for the fileset and a directory in the fileset and
** a path conponent (ie directory entry), find the tag of the component.
** On by one, read the directory pages and for each dir entry, test the
** component name. If found, return the tag of the component in bfp->fileTag.
**
** Input: dirbfp is set up for the dir to search
**        component - file or next dir to find in dirbfp
** Output: dirbfp->fileTag -  component tag
** Errors: diagnostic output to stderr for all errors
** Return: OK if component found
**         TAG3 if found conponent is tag 3 (.tags)
**         ENOENT if component does not exist
**         BAD_DISK_VALUE for mangled directory entries
*/
static int
search_dir(bitFileT *tagBfp, bitFileT *dirbfp, char *component)
{
    vdIndexT v;
    uint32T p,c;
    int i;
    int ret;

    /* read file set root dir looking for file name */
    for ( i = 0; i < dirbfp->pages; i++ ) {
        fs_dir_entry  *dir_ent_p;

        /* read dir page */
        ret = read_page(dirbfp, i);
        if ( ret != OK ) {
            fprintf(stderr,
              "search_dir: read directory (tag %d set tag %d) page %d failed\n",
              dirbfp->fileTag, dirbfp->setTag, i);
            return ret;
        }
        dir_ent_p = (fs_dir_entry*)(dirbfp->pgBuf);

        /* scan dir page looking for file  */
        while ( (char*)dir_ent_p < &dirbfp->pgBuf[PAGE_SIZE] )
        {
            if ( dir_ent_p->fs_dir_header.fs_dir_namecount > FS_DIR_MAX_NAME )
            {
                printf("dir err\n");
                return BAD_DISK_VALUE;
            }
            dir_ent_p->fs_dir_name_string[dir_ent_p->fs_dir_header.fs_dir_namecount] = '\0';

            /* look for file */
            if ( strcmp(component, dir_ent_p->fs_dir_name_string) == 0 ) {
                if ( dir_ent_p->fs_dir_header.fs_dir_bs_tag_num != 0 &&
                     dir_ent_p->fs_dir_header.fs_dir_bs_tag_num !=
                     GETTAGP(dir_ent_p)->num )
                {
                     fprintf(stderr,
          "fs_dir_bs_tag_num (%d) doesn't match GETTAGP(dir_ent_p)->num (%d)\n",
                       dir_ent_p->fs_dir_header.fs_dir_bs_tag_num,
                       GETTAGP(dir_ent_p)->num);
                }

                ret = get_prim_from_tagdir(tagBfp, GETTAGP(dir_ent_p), dirbfp);
                if ( ret != OK ) {
                    return ret;
                }

                /* if tag == 3 => .tags */
                /* next component is a tag */
                if ( GETTAGP(dir_ent_p)->num == 3 ) {
                    return TAG3;
                }

                dirbfp->fileTag = GETTAGP(dir_ent_p)->num;
                return OK;
            }
            dir_ent_p =
       (fs_dir_entry*)((char*)dir_ent_p + dir_ent_p->fs_dir_header.fs_dir_size);
        }
    }

    return ENOENT;
}

/******************************************************************************/
/*
** for sav sets
** get set file from save dir
** Input: bfp->type = SAV
**        bfp->dmn->dmnName
*/
static int
get_savset_file(bitFileT *bfp, char *name)
{
    DIR *dirStrm;
    char dirname[MAXPATHLEN];
    struct dirent *dp;
    char *cp;
    int fd;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->dmn->dmnName != NULL);
    strcpy(dirname, bfp->dmn->dmnName);
    strcat(dirname, "/");
    strcat(dirname, name);
    dirStrm = opendir(dirname);
    if ( dirStrm == NULL ) {
        fprintf(stderr, "bad set name %s\n", dirname);
        if ( errno == EACCES ) {
            return EACCES;
        } else {
            return ENOENT;
        }
    }

    for ( dp = readdir(dirStrm); dp != NULL; dp = readdir(dirStrm) ) {
        struct stat statBuf;

        if ( strcmp(dp->d_name, "tag") == 0 )
        {
            closedir(dirStrm);
            strcat(dirname, "/");
            strcat(dirname, dp->d_name);
            if ( stat(dirname, &statBuf) == -1 ) {
                sprintf(errstr, "get_savset_file: stat failed for volume %s",
                  dirname);
                perror(errstr);
                if ( errno == EACCES ) {
                    return EACCES;
                } else {
                    return ENOENT;
                }
            }

            if ( !S_ISREG(statBuf.st_mode) )
            {
                printf("bmt is not a regular file\n");
                return BAD_DISK_VALUE;
            }

            bfp->pgLbn = XTNT_TERM;

            return resolve_fle(bfp, dirname, &statBuf);
        }
    }

    closedir(dirStrm);

    return ENOENT;
}

/*****************************************************************************/
/* Reads primary mcell (bfp->pvol, bfp->ppage, bfp->pcell) and looks for
** the record type. If not found, read the next mcell and look there.
**
** Input: bfp->dmn - defines the domain and BMT extents
**        bfp->fileTag - negative tag uses RBMT (if V4 domain)
**        bfp->pvol, bfp->ppage, bfp->pcell - primary mcell
**        recpa - address of pointer to found record
** Output: recpa - pointer to the found record. NULL is not found
** Errors: diagnostic output to stderr for all errors
** Return: OK if record found
**         BAD_DISK_VALUE for bad mcell contents
**         ENOENT if record is not found
**         errors from find_bmtr
*/
/* FIX. pass in mcid to id bad cell in fprintf */
int
find_rec(bitFileT *bfp, int type, char **recpa)
{
    bitFileT *mdBfp;
    bsMCT *mcp;
    bfTagT setTag;
    bfTagT fileTag;
    int first = 1;
    int ret;
    vdIndexT vdi;
    bfMCIdT mcid;

    assert(bfp != NULL);
    assert(bfp->fileTag != 0);
    assert(bfp->dmn);

    vdi = bfp->pvol;
    assert(bfp->dmn->vols[vdi] != NULL);

    mcid.page = bfp->ppage;
    mcid.cell = bfp->pcell;
    *recpa = NULL;

    if ( USE_RBMT ) {
        /* reserverved file: look in rbmt for mcells */
        mdBfp = bfp->dmn->vols[vdi]->rbmt;
    } else {
        /* non reserved file: look in bmt for mcells */
        mdBfp = bfp->dmn->vols[vdi]->bmt;
    }
    assert(mdBfp);

    while ( vdi ) {
        ret = read_page(mdBfp, mcid.page);
        if ( ret != OK ) {
            fprintf(stderr, "read_page fail\n");
            return ret;
        }

        mcp = &((bsMPgT*)mdBfp->pgBuf)->bsMCA[mcid.cell];

        if ( first ) {
            setTag = mcp->bfSetTag;
            if ( bfp->setTag != setTag.num ) {
                if ( bfp->setp == NULL || bfp->setp->origSetp == NULL ||
                     bfp->setp->origSetp->fileTag != setTag.num )
                {
                    fprintf(stderr, "wrong set tag\n");
                    return BAD_DISK_VALUE;
                }
            }

            fileTag = mcp->tag;
            if ( bfp->fileTag != fileTag.num ) {
                fprintf(stderr, "wrong tag\n");
                return BAD_DISK_VALUE;
            }

            first = 0;
        } else {
            if ( !BS_BFTAG_EQL(mcp->bfSetTag, setTag) ) {
                fprintf(stderr, "wrong set tag\n");
                return BAD_DISK_VALUE;
            }

            if ( !BS_BFTAG_EQL(mcp->tag, fileTag) ) {
                fprintf(stderr, "wrong tag\n");
                return BAD_DISK_VALUE;
            }
        }

        ret = find_bmtr(mcp, type, recpa);
        if ( ret == OK ) {
            return OK;
        }
        if ( ret != ENOENT ) {
            return ret;
        }

        ret = get_next_mcell(bfp, &vdi, &mcid);
        if ( ret != OK ) {
            return ret;
        }
    }

    return ENOENT;
}

/*****************************************************************************/
/*
** Find record type in given mcell.
**
** Input: bsMCp - pointer to mcell in memory
**        rtype - record type for which to search
**        recpa - address of pointer to found record
** Output: recpa - pointer to found record. NULL is record not found
** Errors: diagnostic output to stderr for all errors
** Return: OK if record found
**         BAD_DISK_VALUE for bad mcell contents
**         ENOENT if record was not found
*/
int
find_bmtr(bsMCT *bsMCp, int rtype, char **recpa)
{
    bsMRT *rp;
    int rn = 0;

    *recpa = NULL;

    /* scan cell for desired record type.  all cells contain a */
    /* NIL type record as a "stopper".  */

    rp = (bsMRT *)bsMCp->bsMR0;

    while ( rp->type != BSR_NIL ) {
        if ( (char *)rp + rp->bCnt > &bsMCp->bsMR0[BSC_R_SZ] ) {
            printf("Bad mcell record %d: bCnt too large %d\n", rn, rp->bCnt);
            return BAD_DISK_VALUE;
        }

        /* This check prevents infinite loop on a bogus record */
        if ( rp->bCnt == 0 ) {
            printf("Bad mcell record %d: bCnt == 0\n", rn);
            return BAD_DISK_VALUE;
        }

        if ( rp->type == rtype ) {
            *recpa = (char *)rp + sizeof(bsMRT);
            return OK;
        }

        rp = (bsMRT *)((char *)rp + roundup(rp->bCnt, sizeof(int)));
    }

    return ENOENT;
}

/*****************************************************************************/
/* Change the number in the character string to a long.
** Accept octal, hex or decimal. If the string has any bad numbers, return
** an error without outputing a number.
**
** Input: character string cp with octal, decimal or hexadecimal number.
** Output: long at "num" iff conversion is OK. 
** Errors: status return only. No diagnostic printf
** Return: OK if converstion went well
**         EINVAL for badly formed number
*/
int
getnum(char *cp, long *num)
{
    char *endptr;
    int base = 10;
    long number;

    assert(NULL != cp);

    if ( cp[0] == '0' ) {
        if ( cp[1] == 'x' || cp[1] == 'X' ) {
            base = 16;
        } else {
            base = 8;
        }
    }

    number = strtol(cp, &endptr, base);

    if ( endptr != cp + strlen(cp) ) {
        return EINVAL;
    }

    *num = number;
    return OK;
}

/*****************************************************************************/
#define PRINT_NAME(bfp) {                                               \
    if ( bfp->fileTag < 0 ) {                                           \
        switch ( -bfp->fileTag % BFM_RSVD_TAGS ) {                      \
            case BFM_RBMT:   /* also BFM_BMT_V3 */                      \
                printf(RBMT_PRESENT ? "RBMT " : "BMT ");                \
                break;                                                  \
            case BFM_SBM:                                               \
                printf("SBM ");                                         \
                break;                                                  \
            case BFM_BFSDIR:                                            \
                printf("root TAG ");                                    \
                break;                                                  \
            case BFM_FTXLOG:                                            \
                printf("LOG ");                                         \
                break;                                                  \
            case BFM_BMT:                                               \
                printf(RBMT_PRESENT ? "BMT " :"??? ");                  \
                break;                                                  \
            default:                                                    \
                break;                                                  \
        }                                                               \
    } else if ( bfp->fileTag > 0 && bfp->setTag == -BFM_BFSDIR ) {      \
        if ( bfp->setName[0] != '\0' )                                  \
            printf("\"%s\" TAG ", bfp->setName);                        \
        else                                                            \
            printf("set tag %d TAG ", bfp->setTag);                     \
    } else if ( bfp->fileTag == 1 && bfp->setTag != -BFM_BFSDIR ) {     \
        if ( bfp->setName[0] != '\0' )                                  \
            printf("\"%s\" FRAG ", bfp->setName);                       \
        else if ( bfp->setTag != 0 )                                    \
            printf("set tag %d FRAG ", bfp->setTag);                    \
        else                                                            \
            printf("FRAG ");                                            \
    }                                                                   \
}

/*****************************************************************************/
/* Prints a header for the page of data displayed.
** Prints a one line description of the source of the formated data.
** For a domain it prints the domain name, the volume and the block number.
** Then it prints the bitfile name (BMT, SBM, etc) and the bitfile page number.
** For a volume it prints the volume name, block, bitfile name and page.
** For a saved file it prints the file name and the page number.
** If no page number is specified (block on disk is specified) then just
** print the given block number.
** A double line "====" precedes the description and a single line follows it.
**
** DOMAIN "---" VDI # ("<vol>") LBN #  <file> PAGE #
** VOLUME "---" (VDI #) LBN #  <file> PAGE #
** FILE "---"  PAGE #
**
** <file> = BMT, SBM, LOG, ROOT TAG, "file_set" TAG, "file_set" FRAG, "file"
**
** DOMAIN "---"  VDI # ("<vol>")  LBN #
** VOLUME "---" (VDI #)  LBN #
** VOLUME "---"  LBN #
**
** SAVE SET "---"  VDI #  SAVE FILE "---"  PAGE #
**
** Input: bfp->type = DMN, VOL or FLE
**        bfp->fileTag - selects file name
**        bfp->pgVol - if DMN or VOL then VDI to print
**        bfp->dmn->vols[bfp->pgVol]->volName - if DMN or VOL then volume name
**        bfp->pgLbn - if != -1 then LBN to print
**        bfp->pgNum - if != -1 then file page number to print
** Output: prints volume page header
** Errors: no errors
** Return: none
*/
void
print_header(bitFileT *bfp)
{
    char line[80];
    int  line_len = 0;
    char *volume;
    line[79] = '\0';        /* in case sprintf fails */

    assert(bfp);
    assert(bfp->type & (DMN | VOL | FLE | SAV));

    printf(DOUBLE_LINE);
    if ( bfp->type & DMN ) {
        assert(bfp->dmn);
        printf("DOMAIN \"%s\"  ", bfp->dmn->dmnName);
        if ( bfp->pgVol ) {
            assert(bfp->dmn->vols[bfp->pgVol]);
            volume = bfp->dmn->vols[bfp->pgVol]->volName;
            printf("VDI %d (%s)  ", bfp->pgVol, volume);
            if ( bfp->pgLbn != XTNT_TERM )
                printf("lbn %u   ", bfp->pgLbn);
            if ( bfp->pgNum != -1 ) {
                PRINT_NAME(bfp);
                printf("page %d", bfp->pgNum);
            }
        }
    } else if ( bfp->type & VOL ) {
        assert(bfp->dmn);
        volume = bfp->dmn->vols[bfp->pgVol]->volName;
        printf("VOLUME \"%s\" ", volume);
        if ( bfp->pgVol ) {
            printf("(VDI %d) ", bfp->pgVol);
        }

        if ( bfp->pgLbn != XTNT_TERM ) {
            printf(" lbn %u   ", bfp->pgLbn);
        }

        if ( bfp->pgNum != -1 ) {
            PRINT_NAME(bfp);
            printf("page %d", bfp->pgNum);
        }
    } else if ( bfp->type & FLE ) {
        printf("FILE \"%s\"   ", bfp->fileName);
        if ( bfp->pgNum != -1 ) {
            PRINT_NAME(bfp);
            printf("page %d", bfp->pgNum);
        }
    }

    if ( bfp->type & SAV ) {
    }

    printf("\n");
    printf(SINGLE_LINE);
}

/*****************************************************************************/
/* Prints groups of 4 bytes as an int until < 4 bytes left to print,
** then prints bytes, all in hex.
**
** Input: pdata - data to be displayed. No alignment required
**        size - number of bytes to displayed
** Output: print to stdout
** Errors: no errors
** Return: none
*/

#define INTSZ 4   /* sizeof(uint32_t) */
#define BPL 16    /* bytes of data per line */

void
print_unknown( char *pdata, int size )
{
    int lines = size / BPL;
    int ln;
    uchar_t *line;
    uchar_t *cp;
    uint32T x;

    for ( ln = 0; ln < lines; ln++)
    {
        line = (uchar_t*)&pdata[ln * BPL];
        printf("        ");
        for ( cp = line; cp < (uchar_t*)&pdata[ln * BPL + BPL]; cp += INTSZ )
        {
            bcopy(cp, (char *)&x, sizeof(uint32T));
            printf("%08x ", x);
        }
        printf("\n");
    }

    line = (uchar_t*)&pdata[ln * BPL];
    if ( line != (uchar_t*)&pdata[size] )
    {
        printf("        ");
        for ( cp = line; cp < (uchar_t*)&pdata[size - (INTSZ-1)]; cp += INTSZ )
        {
            bcopy(cp, (char *)&x, sizeof(uint32T));
            printf("%08x ", x);
        }

        if ( cp != (uchar_t*)&pdata[size] )
        {
            for ( ; cp < (uchar_t*)&pdata[size]; cp++ )
            {
                printf("%02x ", *cp);
            }
        }
        printf("\n");
    }
}

/*****************************************************************************/
/* Test whether "page" exists in "bfp".
**
** Input: bfp->type 
**        bfp->pages - if bfp->type == FLE or SAV
**        bfp->xmap - if bfp->type == VOL or DMN
** Output: none
** Errors: return status only. no errors
** Return: TRUE if page is mapped otherwise FALSE
*/
int
page_mapped(bitFileT *bfp, int page)
{
    int low;
    int high;
    int idx;
    int lastidx;

    if ( bfp->type & FLE || bfp->type & SAV )
    {
        if ( page < bfp->pages )
            return TRUE;
        else
            return FALSE;
    }

    if ( bfp->xmap.cnt == 0 )
        return FALSE;

    low = 0;
    high = bfp->xmap.cnt;
    idx = high / 2;

    while ( (idx > 0) &&
            ((page < bfp->xmap.xtnt[idx].page) ||
             ((idx < bfp->xmap.cnt - 1) &&
              (page >= bfp->xmap.xtnt[idx + 1].page))) )
    {

        lastidx = idx;
        if ( page < bfp->xmap.xtnt[idx].page ) {
            high = idx;
            idx = low + (idx - low) / 2;
        } else {
            low = idx;
            idx = idx + (high - idx) / 2;
        }

        assert(idx != lastidx);
    }
    assert(idx >= 0);
    assert(idx < bfp->xmap.cnt);

    if ( bfp->xmap.xtnt[idx].blk == XTNT_TERM ||
         bfp->xmap.xtnt[idx].blk == PERM_HOLE_START )
    {
        /* page not mapped */
        return FALSE;
    }

    return TRUE;
}

/*****************************************************************************/
/* Read a page from the bitFile into the bitFile page buffer.
** Maintain a simple one page cache. If pgVol & pgLbn as calculated from page
** and xmap are aready set in bfp then pgBuf already contains the page.
**
** input: bfp->xmap hold extents for file
**        bfp->type determines whether to use xtnts or fd
**        bfp->fd - read from this fd is type is FLE or VOL
**        bfp->pgVol, pgNum, pgLbn - if same as requested, don't read again
**        page is bitFile page to read
** output: bfp->pgBuf - a page of data is loade here
**         bfp->pgBuf - if NULL, malloc a page buffer
**         bfp->pgVol, pgNum, pgLbn - set to reflect where pgBuf read from
** Errors: diagnostic output to stderr for all errors
** return OK - if read succeeds
**        BAD_DISK_VALUE - block is not mapped or volume does not exist
**        ENOMEM - malloc failed
*/
/* TODO: use read_vol_at_blk. */
int
read_page(bitFileT *bfp, int page)
{
    unsigned long lbn;
    int vol = 0;
    int fd, low = 0, high = bfp->xmap.cnt, idx = high / 2;
    int lastidx;
    ssize_t read_ret;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    if ( page >= bfp->pages ) {
        fprintf(stderr,
          "read_page: Requested page (%d) is beyond last page (%d).\n",
          page, bfp->pages - 1);
    }

    if ( bfp->type & FLE || bfp->type & SAV ) {
        /* working with save file;  page # is real offset into the file */
        if ( bfp->pgBuf && page * ADVFS_PGSZ_IN_BLKS == bfp->pgLbn ) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }
        fd = bfp->fd;
        if ( lseek(fd, (off_t)page * PAGE_SIZE, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_page: lseek to page %d failed", page);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    } else {
        assert ( bfp->xmap.cnt != 0 );
        while ( (idx > 0) &&
                ((page < bfp->xmap.xtnt[idx].page) ||
                 ((idx < bfp->xmap.cnt - 1) &&
                  (page >= bfp->xmap.xtnt[idx + 1].page))) ) {

            lastidx = idx;
            if (page < bfp->xmap.xtnt[idx].page) {
                high = idx;
                idx = low + (idx - low) / 2;
            } else {
                low = idx;
                idx = idx + (high - idx) / 2;
            }

            /* is this a test that the page is off the end? return err */
            assert(idx != lastidx);
        }
        assert(idx >= 0);

        if ( bfp->xmap.xtnt[idx].blk == XTNT_TERM ||
             bfp->xmap.xtnt[idx].blk == PERM_HOLE_START )
        {
            /* page not mapped */
            return BAD_DISK_VALUE;
        }

        if ( !(bfp->type & FLE) ) {
            vol = bfp->xmap.xtnt[idx].vol;
            if ( bfp->dmn->vols[vol] == NULL ) {
                fprintf(stderr,
                  "Bad volume (%d) for extent %d of set tag %d, file tag %d\n",
                  vol, idx, bfp->setTag, bfp->fileTag);
                fprintf(stderr,
                  "Page %d defined in extent %d is on volume index %d.\n",
                   page, idx, bfp->xmap.xtnt[idx].vol);
                fprintf(stderr, "Domain \"%s\" doesn't have this volume.\n",
                   bfp->dmn->dmnName);
                return BAD_DISK_VALUE;
            }
            fd = bfp->dmn->vols[vol]->fd;
        } else {
            /* this is a BMT. It is self contained on this volume */
            fd = bfp->fd;
        }

        lbn = (unsigned long)bfp->xmap.xtnt[idx].blk +
              ADVFS_PGSZ_IN_BLKS * (page - bfp->xmap.xtnt[idx].page);
        if ( bfp->pgBuf && lbn == bfp->pgLbn && bfp->pgVol == vol ) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }

        bfp->pgVol = vol;
        bfp->pgLbn = lbn;
        if ( lseek(fd, lbn * DEV_BSIZE, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_page: lseek to block %u failed", lbn);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    }

    if ( bfp->pgBuf == NULL ) {
        bfp->pgBuf = malloc(PAGE_SIZE);
        if ( bfp->pgBuf == NULL ) {
            perror("read_page: malloc failed");
            return ENOMEM;
        }
    }

    read_ret = read(fd, bfp->pgBuf, (size_t)PAGE_SIZE);
    if ( read_ret != (ssize_t)PAGE_SIZE ) {
        if ( read_ret == (ssize_t)-1 ) {
            sprintf(errstr, "read_page: Read page %d failed.", page);
            perror(errstr);
        } else {
            fprintf(stderr,
              "read_page: Truncated read on page %d, expected %d, got %ld\n",
              page, PAGE_SIZE, read_ret);
        }
        return BAD_DISK_VALUE;
    }

    bfp->pgNum = page;

    return OK;
}

/*****************************************************************************/
/* Test the BMT page.
** Test nextfreeMCId, nextFreePg, freeMcellCnt, pageId, megaVersion
** Example message:
** open_vol: Corrupted RBMT page 5 (lbn 564) on volume 2 (/dev/disk/dsk8c).
** Bad pageId (23).
**
** Input:  bfp->pgBuf - page to check
**         bfp->pgNum - page number
**         bfp->pages - number of pages in the (R)BMT if known
** Output: none
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if a value is bogus
*/
static int
test_bmt_page(bitFileT *bfp, char *func_name, int rbmtflg)
{
    bsMPgT *bsMPgp;
    int ret = OK;

    /* if not FLE then pgLbn set up */
    assert(bfp->type & FLE || bfp->pgLbn != XTNT_TERM);
    /* if not FLE the dmn, vols & volName filled in */
    assert(bfp->type & FLE || bfp->dmn);
    assert(bfp->type & FLE || bfp->dmn->vols[bfp->pgVol]);
    assert(bfp->type & FLE || bfp->dmn->vols[bfp->pgVol]->volName);

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    if ( bsMPgp->pageId != bfp->pgNum ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %d ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != XTNT_TERM);
                fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad pageId (%d).\n", bsMPgp->pageId);
        }
        ret = BAD_DISK_VALUE;
    }

    if ( bsMPgp->megaVersion > BFD_ODS_LAST_VERSION ||
         bsMPgp->megaVersion < 2 )
    {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %d ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != XTNT_TERM);
                fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad megaVersion (%d).\n", bsMPgp->megaVersion);
        }
        ret = BAD_DISK_VALUE;
    }

    if ( bsMPgp->freeMcellCnt >= BSPG_CELLS ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %d ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != XTNT_TERM);
                fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad freeMcellCnt (%d).\n", bsMPgp->freeMcellCnt);
        }
        ret = BAD_DISK_VALUE;
    } 

    if ( bsMPgp->freeMcellCnt > 0 ) {
        /* V3 BMT freelist terminate with nextFreePg = 0 */
        /* V4 BMT freelist terminate with nextFreePg = -1 */
        /* RBMT nextFreePg always = 0 */
        if ( bfp->pages &&
             (RBMT_PRESENT && !rbmtflg && (signed)bsMPgp->nextFreePg != -1 ||
              (!RBMT_PRESENT || rbmtflg) && bsMPgp->nextFreePg != 0) &&
             bsMPgp->nextFreePg >= bfp->pages )
        {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextFreePg (%d).\n", bsMPgp->nextFreePg);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->nextfreeMCId.page != bfp->pgNum ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextfreeMCId.page (%d).\n",
                  bsMPgp->nextfreeMCId.page);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->nextfreeMCId.cell >= BSPG_CELLS ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextfreeMCId.cell (%d).\n",
                  bsMPgp->nextfreeMCId.cell);
            }
            ret = BAD_DISK_VALUE;
        }
    } else {  /* no free mcells on this page */
        if ( (!RBMT_PRESENT || rbmtflg) && bsMPgp->nextFreePg != 0 ||
             !rbmtflg && RBMT_PRESENT && (signed)bsMPgp->nextFreePg != -1 )
        {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr,
                  "Bad nextFreePg (%d) on page with no free mcells.\n",
                        bsMPgp->nextFreePg);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->nextfreeMCId.page != 0 ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextfreeMCId.page (%d).\n",
                  bsMPgp->nextfreeMCId.page);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->nextfreeMCId.cell != 0 ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %d ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != XTNT_TERM);
                    fprintf(stderr, "(lbn %u) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextfreeMCId.cell (%d).\n",
                  bsMPgp->nextfreeMCId.cell);
            }
            ret = BAD_DISK_VALUE;
        }
    }

    return ret;
}

/*****************************************************************************/
/* Test fields in an mcell.
** tag and setTag in mcell must match fields bitFileT.
**
** Input:  bfp->fileTag, bfp->setTag
**         bsMCp - pointer to mcell contents
**         func_name - routine that called. If NULL then no fprintf.
**         vdi, bfMCId - used to ID mcell in diag printf
** Output: none
** Errors: diagnostic output to stderr for all errors if func_name not NULL
** Return: OK if all went well
**         BAD_DISK_VALUE if the mcell has errors
*/
static int
test_mcell(bitFileT *bfp, char *func_name, vdIndexT vdi, bfMCIdT bfMCId,
           bsMCT *bsMCp)
{
    int ret = OK;

    if ( (signed)bsMCp->tag.num != bfp->fileTag ) {
        if ( func_name != NULL ) {
            fprintf(stderr,
              "%s: Bad tag (%d) in mcell %d %d %d, expected %d\n",
              func_name, (signed)bsMCp->tag.num, vdi, bfMCId.page, bfMCId.cell,
              bfp->fileTag);
        }
        ret = BAD_DISK_VALUE;
    }

    if ( (signed)bsMCp->bfSetTag.num != bfp->setTag &&
         (bfp->setp == NULL ||
          bfp->setp->origSetp &&
          (signed)bsMCp->bfSetTag.num != bfp->setp->origSetp->fileTag) )
    {
        if ( func_name != NULL ) {
            fprintf(stderr,
              "%s: Bad set tag (%d) in mcell %d %d %d, expected %d\n",
              func_name, (signed)bsMCp->bfSetTag.num, vdi,
              bfMCId.page, bfMCId.cell, bfp->setTag);
        }
        ret = BAD_DISK_VALUE;
    }

    return ret;
}

/*****************************************************************************/
/* Test mcell address. page and cell must be within range for BMT.
**
** Input:  bfMCId.page, bfMCId.cell - tested
**         vdi - volume mcell lives on
**         bfp->dmn->vols[vdi]->[r]bmt - test page agaisnt this BMT
**         func_name - used in diag printf. if NULL, no printf
** Output: none
** Errors: diagnostic output to stderr for all errors if func_name not NULL
** Return: OK if all went well
**         BAD_DISK_VALUE if the mcell id is out of range
*/
static int
test_mcid(bitFileT *bfp, char *func_name, vdIndexT vdi, bfMCIdT bfMCId)
{
    bitFileT *mdBfp;
    int ret = OK;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[vdi]);

    if ( USE_RBMT ) {
        mdBfp = bfp->dmn->vols[vdi]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[vdi]->bmt;
    }
    assert(mdBfp);

    if ( bfMCId.page >= mdBfp->pages ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Bad mcell ID page (%d).  ",
              func_name, bfMCId.page);
            fprintf(stderr, "%d %s in %s.\n", mdBfp->pages,
              mdBfp->pages == 1 ? "page" : "pages",
              USE_RBMT ? "RBMT" : "BMT");
        }
        ret = BAD_DISK_VALUE;
    }

    if ( bfMCId.cell >= BSPG_CELLS ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Bad mcell ID cell (%d).\n",
              func_name, bfMCId.cell);
        }
        ret = BAD_DISK_VALUE;
    }

    return ret;
}

/*****************************************************************************/
/* Test the specified volume index.
**
** Input:  bfp->dmn->vols[] - array of valid volumes in domain
**         vdi - volume index to test
**         func_name - used in diag printf. no printf if NULL
** Output: none
** Errors: diagnostic printf if func_name not NULL
** Return: OK if vdi passes all tests
**         OUT_OF_RANGE if vdi is too large
**         BAD_DISK_VALUE if vdi is not in this domain
*/
static int
test_vdi(bitFileT *bfp, char *func_name, vdIndexT vdi)
{
    if ( vdi >= BS_MAX_VDI ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: vdIndex (%d) out of range.\n", func_name, vdi);
        }
        return OUT_OF_RANGE;
    }

    assert(bfp->type != FLE);
    assert(bfp->dmn != NULL);
    if ( bfp->dmn->vols[vdi] == NULL ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Bad vdIndex (%d).\n", func_name, vdi);
        }
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Test the specified block. Compare the block number to the number of
** blocks in the volume. Test to see if the block is on a page (16 block)
** boundary. The number of blocks in a volume may not be set up. If not
** it will be zero. In order to test extents, blk -1 & -2 are "OK".
**
** Input: bfp->dmn->vols[vdi]->blocks - blocks in the volume
**        vdi - volume number on which to test block
**        blk - block to test
** Output: none
** Errors: status return only. No diagnostic printf
** Return: OK if block passes all tests
**         BAD_DISK_VALUE if block is not a multiple of 16
**         OUT_OF_RANGE if block is too large for the volume
*/
static int
test_blk(bitFileT *bfp, vdIndexT vdi, lbnT blk)
{
    uint32T pg = blk / ADVFS_PGSZ_IN_BLKS;

    if ( blk == XTNT_TERM )         /* -1 */
        return OK;
    if ( blk == PERM_HOLE_START )   /* -2 */
        return OK;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[vdi]);

    if ( bfp->dmn->vols[vdi]->blocks &&
         blk >= bfp->dmn->vols[vdi]->blocks )
        return OUT_OF_RANGE;

    if ( pg * ADVFS_PGSZ_IN_BLKS != blk )
        return BAD_DISK_VALUE;

    return OK;
}
