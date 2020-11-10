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
 *      On-disk structure checker.
 *
 * Date:
 *
 *      Wed Oct 06 10:14:10 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: verify.c,v $ $Revision: 1.1.148.2 $ (DEC) $Date: 2004/10/21 17:54:51 $";
#endif

#include <stdio.h>
#include <sys/file.h>
#include <strings.h>
#include <sys/errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <overlap.h>
#include <sys/syslimits.h>
#include <sys/types.h>
#include <sys/fs_types.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <unistd.h>
#include <sys/param.h>
#include <signal.h>
#include <msfs/bs_public.h>
#include <sys/mount.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <msfs/bs_ims.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/fs_dir.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_public.h>
#include <msfs/bs_error.h>
#include <msfs/advfs_evm.h>
#include <msfs/bs_index.h>

#include <locale.h>
#include "verify_msg.h"
nl_catd catd;

extern int errno;
extern char *sys_errlist[];

#define BITS_PER_CELL 32
#define BSR_FILE_SYSTEM BSR_MAX
#define ODS_MAX (BSR_MAX + 1)
#define BLOCKSIZE 512L
#define PAGESIZE (BLOCKSIZE * 16)


/*
 * These initial values must match the values in ms_privates.c and ms_public.c
 */

u32T mlNilVdIndex = 0;
bfMCIdT mlNilMCId = { 0, 0 };
mlBfTagT mlNilBfTag = { 0 };

/*
 * Global error counter.
 */
int errcnt = 0;

#define CORRUPTED_FILESET 	-1


/*
 * Private types.
 */

typedef enum { 
    FREE,  /* this must always be first */
    INUSE,
    PRIM,
    SEC,
    XTNT,
    OTHER,
    BAD    /* this must always be last */
} mcellTypeT;

typedef struct mcellHdr {
    mcellTypeT type;
    u32T nextVdIndex;
    bfMCIdT nextMcellId;
    mlBfTagT setTag;
    mlBfTagT tag;
    u32T position;
    u32T typebits;
} mcellHdrT;    

typedef struct vdHdr {
    u32T vdIndex;
    int bmtFd;
    u32T bmtPageSize;
    int bmtPageValid;
    bsMPgT *bmtPage;
    mcellTypeT typeCnt[BAD + 1];
    u32T cnt;
    mcellHdrT *mcellHdr;
} vdHdrT;    

typedef struct mcellPtr {
    u32T vdIndex;
    bfMCIdT mcellId;
}  mcellPtrT;

typedef struct tagHdr {
    struct tagHdr *nextTagHdr;
    mlBfTagT tag;
    int onDiskFlag;
    u32T primIndex;
    u32T xtntIndex;
    u32T cnt;
    u32T maxCnt;
    mcellPtrT *mcellPtr;
}  tagHdrT;

typedef struct setHdr {
    mlBfTagT setTag;
    int cloneFlag;
    mlBfTagT origSetTag;
    u32T tagCnt;
    u32T nonNilCnt;
    u32T hashTblLen;
    tagHdrT **tagHashTbl;
}  setHdrT;

typedef struct mounts {
    char *setName;
    int fd;
    u32T is_mounted;
    mlBfSetIdT bfsetid;
    int mountDev;
} mounted_sets;

typedef struct bad_frags {
    u32T page;
    u32T re_inited;
} bad_frag_pages;

/*
 * The following two structures are copied from
 * libmsfs/xtntmap.c.
 */
typedef struct xtntMap {
    struct xtntMap *nextMap;
    int xtntMapNum;
    int curXtnt;
    int xtntCnt;
    mlExtentDescT *xtnts;
} xtntMapT;

typedef struct xtntMapDesc {
    int fd;
    struct stat stats;
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    struct xtntMap *nextMap;
} xtntMapDescT;

/*
 * Macros
 */
/* Bitmap clusters are 2 disk blocks (1K). AdvFS pages are 8K. */
/* Files are allocated in pages so the bitmap has 8 bit groups set. */
#define CLUST_PER_PAGE 8

#define ERRMSG( eno ) sys_errlist[( eno )]

#define TAG_HASH_TBL_LEN 256
#define HASH_FUNC(tag, hashTblLen) \
  ((BS_BFTAG_IDX((tag)) + BS_BFTAG_SEQ((tag))) % (hashTblLen))

#define MCELL_IDX(mcellPtr) \
  (((mcellPtr).mcellId.page * (long)BSPG_CELLS) + (mcellPtr).mcellId.cell)

#define MCELL_HDR(mcellPtr) \
  (&(vdHdrTbl[(mcellPtr).vdIndex - 1].mcellHdr[MCELL_IDX(mcellPtr)]))
 
#define INODE_TBL_SZ 16384
#define INODE_TBL_HASH( ino ) (ino % INODE_TBL_SZ)

/* number of pages per group */
#define BF_FRAG_GRP_PGS 16

/* bytes per frag slot */
#define BF_FRAG_SLOT_BYTES 1024

/* bytes per pg */
#define BF_FRAG_PG_BYTES 8192

/* slots per pg */
#define BF_FRAG_PG_SLOTS (BF_FRAG_PG_BYTES / BF_FRAG_SLOT_BYTES)

/* slots per group */
#define BF_FRAG_GRP_SLOTS \
    (BF_FRAG_GRP_PGS * BF_FRAG_PG_BYTES / BF_FRAG_SLOT_BYTES)

/* max slots (2^31-1 to allow 0xffffffff as the end of slot number)
*/
#define BF_FRAG_MAX_SLOTS 0xfffffffe

/* max groups */
#define BF_FRAG_MAX_GRPS (BF_FRAG_MAX_SLOTS / BF_FRAG_GRP_SLOTS)

/* max pages per frag bf */
#define BF_FRAG_MAX_PGS (BF_FRAG_MAX_GRPS * BF_FRAG_GRP_PGS)

#define FRAG2PG( frag ) ((frag) / BF_FRAG_PG_SLOTS)
#define FRAG2SLOT( frag ) ((frag) % BF_FRAG_PG_SLOTS)
#define FRAG2GRP( frag ) (( frag ) / BF_FRAG_GRP_SLOTS * BF_FRAG_GRP_PGS)
#define FRAG2GRPSLOT( frag ) ((frag) % BF_FRAG_GRP_SLOTS)
/*
 * inodeT
 */

typedef struct inode {
    ino_t ino;                  /* the file's inode number */
    uint_t dirLinkCnt;         /* number times found during dir scan */
    uint_t tagLinkCnt;           /* should always be 1 after tag scan */
    int realLinkCnt;            /* as obtained from stat */
    struct inode *nxt;          /* ptr to next hash tbl entry */
} inodeT;

static inodeT *InodeTbl[ INODE_TBL_SZ ];
/*
 * Global data
 */

static char *Prog = "verify";

static char *dotTagsPath = ".tags";  /* FIX - derive this from the domain */

mlDmnParamsT dmnParams;
int rootTagDirFd = -1;
int bmtFd = -1;
static int bitMapFd = -1;
static u32T *bitMap = NULL;
static long bitMapSize = 0;
static mlBfDomainIdT domainId;
static int domainVersion;
static u32T *filetags = NULL;
static long filetagsSize = 0;
static int file_tags;
static int set_tags;
static u32T *settags = NULL;
static long settagsSize = 0;
static bsStgBmT *bitMapPage = NULL;
static long bMPSize = 0;
static char *mcellTypeStr[] = {
    "Free",
    "Inuse",
    "Primary",
    "Secondary",
    "Extent",
    "Other",
    "Bad"
  };
static int vdHdrCnt = 0;
static vdHdrT *vdHdrTbl = NULL;
static u32T inuseMcellCnt = 0;
static int setHdrCnt = 0;
static setHdrT *setHdrTbl = NULL;
static advfs_ev advfs_event;
static posted_lock=0;
static mlSSDmnOpsT savedDmnCurrentState=-1;

char *dmnName;
char *setName;
char dmn_pnt[ MAXPATHLEN ] = "/etc/fdmns/";
char old_cwd[ MAXPATHLEN ] = "\0";
mounted_sets *mounted = NULL;         /* array of mounted fileset info */
char **mount_point = NULL;            /* Array of mount points */
char *fragpg;
bad_frag_pages bad_frags[10];
int num_bad_frags = 0;
int lkHndl;
int lkHndl2;
int dir_locked=0;
int dmn_locked=0;
int numSets = 0;
int maxSets = 100; /* Initial value - dynamically increased if more needed */
int tOpt = 0, quiet = 0, verbose = 0, del = 0, lost = 0, force = 0;
int fix = 0, active = 0, dual = 0;
int mountDev;                
long memory_usage = 0;

struct sbmIndexEntry {
    unsigned int sbmIndex;
    unsigned int pageIndex;
    struct sbmIndexEntry *nxt;
};


/*
 * Private protos
 */

void
umount_it();

static
mlStatusT
check_disk (
            mlBfDomainIdT domainId,  /* in */
            vdHdrT *vdHdr  /* in, modified */
            );

static
void
get_mcell_rec (
               bsMCT *mcell,  /* in */
               int recIndex,  /* in */
               u16T *recType,  /* out */
               void **recAddr,  /* out */
               u16T *recSize  /* out */
               );

static
mlStatusT
check_bmt_page (
                vdHdrT *vdHdr,  /* in */
                u32T pageId,  /* in */
                bsMPgT *bmtPage,  /* in */
                mcellHdrT *mcellHdr  /* in, modified */
                );

static
mlStatusT
check_mcells (
              vdHdrT *vdHdr,  /* in */
              bsMPgT *bmtPage,  /* in */
              mcellHdrT *mcellHdr  /* in, modified */
              );

static
void
display_mcell (
               bsMCT *mcell  /* in */
               );

static
mlStatusT
check_free_mcell_list (
                       u32T vdIndex,  /* in */
                       bsMPgT *bmtPage  /* in */
                       );

static
mlStatusT
check_mcell_recs (
                  u32T vdIndex,  /* in */
                  bfMCIdT mcellId,  /* in */
                  bsMCT *mcell,  /* in */
                  u32T* typebits,
                  mcellTypeT *mcellType  /* out */
                  );

static
void
check_rec (
           u32T vdIndex,  /* in */
           bfMCIdT mcellId,  /* in */
           bsMRT *recHdr,  /* in */
           bsMCT *mcell,  /* in */
           int *type  /* in, modified */
           );

static
void
set_mcell_type (
                int *type,  /* in */
                mcellTypeT *mcellType  /* in */
                );

static
void
display_mcell_totals (
                      vdHdrT *vdHdr  /* in */
                      );

static
mlStatusT
bitmap_init (
             u32T vdIndex  /* in */
             );

static
mlStatusT
remove_bad_bf(
	     mlBfTagT bad_tag, /* in */
	     mlBfTagT set_tag /* in */
	     );

static
void
set_bitmap (
            u32T vdIndex,  /* in */
            bfMCIdT mcellId,  /* in */
            bsMCT *mcell,  /* in */
            u32T clusterSize,  /* in */
            u32T maxCluster,  /* in */
            u32T *bitMap, /* in */
            struct sbmIndexEntry **badSBMIndices /* in / out */
            );

static
mlStatusT
check_bitmap (
              mlBfDomainIdT domainId,  /* in */
              u32T vdIndex, /* in */
              struct sbmIndexEntry **badSBMIndices /* in / out */
              );

static
void
bitmap_cleanup (
                );

static
mlStatusT
create_tag_hash_table (
                       );

static
mlStatusT
insert_mcell_ptr (
                  tagHdrT *tagHdr,  /* in */
                  u32T vdIndex,  /* in */
                  bfMCIdT mcellId  /* in */
                  );

static
tagHdrT *
insert_tag (
            mlBfTagT setTag,  /* in */
            mlBfTagT tag  /* in */
            );

static
tagHdrT *
find_tag (
          mlBfTagT setTag,  /* in */
          mlBfTagT tag  /* in */
          );

static
setHdrT *
insert_set_tag (
                mlBfTagT setTag  /* in */
                );

static
setHdrT *
find_set_tag (
              mlBfTagT setTag  /* in */
              );

static
void
delete_tag_hash_table (
                       );

static 
void
check_bitfile_mcell_list (
                          );

static
mlStatusT
check_one_bitfile_mcell_list (
                              setHdrT *setHdr,  /* in */
                              tagHdrT *tagHdr  /* in */
                              );

static
void
set_mcell_position (
                    setHdrT *setHdr,  /* in */
                    tagHdrT *tagHdr,  /* in */
                    u32T firstPosition,  /* in */
                    u32T bfVdIndex,  /* in */
                    bfMCIdT bfMcellId,  /* in */
                    u32T *nextPosition  /* out */
                    );

static
void
check_mcell_position_field (
                            );

static
void
display_tag (
             mlBfTagT setTag,  /* in */
             mlBfTagT tag  /* in */
             );

static
void
check_tagdirs (
               mlBfTagT rootTagDirTag  /* in */
               );

static
mlStatusT
check_one_tagdir (
                  mlBfTagT setSetTag,  /* in */
                  mlBfTagT openSetTag,  /* in */
                  mlBfTagT setTag  /* in */
                  );

static
int
check_tagdir_page (
                   mlBfTagT setSetTag,  /* in */
                   mlBfTagT setTag,  /* in */
                   u32T currPage,  /* in */
                   bsTDirPgT *tagDirPage,  /* in */
                   u32T *retFreeTagMapCnt  /* out */
                   );

static
void
display_tag_map (
                 u32T currPage,  /* in */
                 u32T index,  /* in */
                 bsTMapT *tagMap  /* in */
                 );

int
fix_overlapping_frag (
    int fd,
    char *filename,
    int mountpathlength,
    mlExtentDescT *overlappingextent,
    uint32T fragpage
    );


static
int
fix_tagdir_hdr (
    int tagDirFd,
    u32T currPage,
    bsTDirPgT *tagDirPage,
    int tagDirSize
    );

static
void
main_terminate(
               int sig
               );

int
read_pg(
    int fd,
    long pg,
    char *pgp
    );

int
write_pg(
    int fd,
    long pg,
    char *pgp
    );

struct sigaction term_sigaction = {
        main_terminate,
        (sigset_t)0,
        0
};

/* I've made sure I closed the reserved files since currently the system */
/* will crash if they are open when we umount the fileset. To do the */
/* complete job we need to make sure all files are closed when we umount. */
static
void
main_terminate(int sig)
{
    if (bmtFd != -1) {
        close (bmtFd);
    }
    if (bitMapFd != -1) {
        close (bitMapFd);
    }
    if (rootTagDirFd != -1) {
        close (rootTagDirFd);
    }
    umount_it();
    verify_exit( -1 );
}

void usage( void )
{

    fprintf ( stderr, catgets(catd, 1, 1, "exiting...\n"));

    fprintf ( stderr, catgets(catd, 1, USAGE, "usage: %s [-l | -d] [-v | -q] [-t] [-a | -f] [-F] [-D] domainName\n"), Prog);
    fprintf ( stderr, "\n");
    fprintf ( stderr, catgets(catd, 1, 249, "All filesets in the domain must be unmounted.\n"));
    fprintf ( stderr, "\n");
    fprintf ( stderr, catgets(catd, 1, 250, "-l  Creates a symbolic link to a lost file in the /???/lost+found directory.\n"));
    fprintf ( stderr, catgets(catd, 1, 251, "-d  Deletes lost files.\n"));
    fprintf ( stderr, catgets(catd, 1, 252, "-v  Verbose.\n"));
    fprintf ( stderr, catgets(catd, 1, 253, "-q  Quiet.\n"));
    fprintf ( stderr, catgets(catd, 1, 254, "-t  Displays the mcell totals.\n"));
    fprintf ( stderr, catgets(catd, 1, 255, "-a  Check an active domain.  All filesets in the domain must be mounted.\n"));
    fprintf ( stderr, catgets(catd, 1, FIXFLAG, "-f  Attempt to fix any problems found.\n"));
    fprintf ( stderr, catgets(catd, 1, 264, "-F  Use this option with caution; runs the verify command without recovery.\n"));
    fprintf ( stderr, catgets(catd, 1, CONT_264, "    (similar to running the mount command with -d option).\n"));
    fprintf ( stderr, catgets(catd, 1, DUALFLAG, "-D  Check a dual mounted domain.\n"));
    fflush( stderr );
}


/*
 * If an inconsistency is found, it attempts to fix the problem in-memory so that
 * the mcell is consistent.
 */

main( int argc, char *argv[] )

{
    int c;
    char *funcName = "main";
    extern int getopt();
    u32T i;
    mcellTypeT j;
    extern int optind;
    mlStatusT sts;
    u32T *vdIndex = NULL;
    u32T vdIndexI;
    int vdIndexCnt = 0;
    struct rlimit rlimit;
    mlSSDmnOpsT ssDmnCurrentState=0;
    int ssRunningState=0;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_VERIFY, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

     /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,262,
		"\nPermission denied - user must be root to run %s.\n\n"),
		argv[0]);
        usage();
        verify_exit( -1 );
    }

    /*
     * Need to make sure we have access to all the memory we
     * can get access to.  This is important for filesets with
     * large number of files.
     */
    rlimit.rlim_max = RLIM_INFINITY;
    rlimit.rlim_cur = RLIM_INFINITY;

    if (setrlimit(RLIMIT_DATA, &rlimit))
    {
	fprintf(stderr, catgets(catd,1,285,
				"setrlimit failed\n"));
	perror(Prog);
	verify_exit( -1 );
    }

    /*
     * Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "qvfldtFDa" )) != EOF) {
        switch (c) {
            case 't':
                tOpt = 1;
                break;

            case 'q':
                quiet++;
                break;

            case 'l':
                lost++;
                break;

	    case 'd':
		del++;
		break;

            case 'v':
                verbose++;
                break;

	    case 'F':
		force++;
		break;

	    case 'f':
		fix++;
		break;

	    case 'a':
		active++;
		break;

	    case 'D':
		dual++;
		break;

            default:
                usage();
                verify_exit( -1 );
        }
    }

    if (argc < 2) {
        usage();
        verify_exit( -1 );
    }
    if (verbose > 1 || quiet > 1 || fix > 1 || del > 1 || lost > 1 || 
	active > 1) {
        usage();
        verify_exit( -1 );
    }

    if (verbose && quiet) {
        usage();
        verify_exit( -1 );
    }

    if (lost && del) {
	usage();
	verify_exit( -1 );
    }

    if (active && fix) {
	usage();
	verify_exit( -1 );
    }

    if (optind != (argc - 1)) {
        usage();
        verify_exit( -1 );
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, ONDISK,
            "%s: This utility can not process the on-disk structures on this system.\n"), Prog);
        verify_exit( -1 );
    }

    dmnName = argv[optind];
    setName = argv[optind + 1];
 
    sts = advfs_ss_dmn_ops (
                            dmnName,
                            SS_UI_STATUS,
                            &savedDmnCurrentState,
                            &ssRunningState );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, SSERROR,
                 "%s: vfast status not available on domain %s; %s\n"),
                 Prog, dmnName, BSERRMSG(sts));
        savedDmnCurrentState = -1;
        verify_exit( -1 );
    }

    switch(savedDmnCurrentState) {
        case SS_DEACTIVATED:
                    savedDmnCurrentState = SS_UI_DEACTIVATE;
                    break;
        case SS_ACTIVATED:
                    savedDmnCurrentState = SS_UI_ACTIVATE;
                    break;
        case SS_SUSPEND:
                    savedDmnCurrentState = SS_UI_SUSPEND;
                    break;
        default:
                    savedDmnCurrentState = -1;
                    break;
    }

    /* suspend smartstore processing while verify runs */
    if(savedDmnCurrentState == SS_UI_ACTIVATE) {
        sts = advfs_ss_dmn_ops (
                            dmnName,
                            SS_UI_SUSPEND,
                            &ssDmnCurrentState,
                            &ssRunningState );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, SSERROR,
                 "%s: vfast status not available on domain %s; %s\n"),
                 Prog, dmnName, BSERRMSG(sts));
            verify_exit( -1 );
        }
    }

    (void)sigaction(SIGINT, &term_sigaction, (struct sigaction *)0);

    init_event(&advfs_event);
    advfs_event.domain = dmnName;
    advfs_event.fileset = " "; 		/* setName should be NULL */
    advfs_post_user_event(EVENT_FSET_BACKUP_LOCK, advfs_event, Prog);
    posted_lock = 1;

    /*
     * mount the filesets
     */
    sts = mount_it();
    if (sts != EOK) {
	umount_it();
	fprintf( stdout, catgets(catd, 1, 1, "exiting...\n"));
	fflush( stdout );
	verify_exit( -1 );
    }

    /*
     * set the cwd to the mount point of the first mounted fileset
     * for the rest of verify to work (it's just convenient to
     * use that one)
     */
    getcwd(&old_cwd[0], MAXPATHLEN);
    chdir(mount_point[0]);
    fprintf( stdout, catgets(catd, 1, 2, "+++ Domain verification +++\n\n"));
    sts = msfs_get_dmnname_params (dmnName, &dmnParams);
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 3, 
		 "%s: unable to get info for domain '%s'\n"), 
		 funcName, dmnName);
        fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
	errcnt = -1;
        RAISE_EXCEPTION (0);
    }
    domainId = dmnParams.bfDomainId;
    domainVersion = dmnParams.dmnVersion;
    fprintf( stdout, catgets(catd, 1, 5, "Domain Id %08x.%08x\n"), 
	    domainId.tv_sec, domainId.tv_usec);

    vdIndex = (u32T *)malloc (dmnParams.curNumVols * sizeof (u32T));
    if (vdIndex == NULL) {
        fprintf ( stderr, catgets(catd, 1, 6, 
		 "%s: can't allocate memory for disk index array\n"), 
		 funcName);
	errcnt = -1;
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		dmnParams.curNumVols * sizeof (u32T) , memory_usage);

        RAISE_EXCEPTION (0);
    }

    memory_usage += dmnParams.curNumVols * sizeof (u32T);
    sts = msfs_get_dmn_vol_list (
                                 dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 &(vdIndex[0]),
                                 &vdIndexCnt
                                 );
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 7, 
                  "%s: unable to get the volume list\n"), funcName);
        fprintf ( stderr, catgets(catd, 1, 4, 
                  "    error: %d, %s\n"), sts, BSERRMSG (sts));
	memory_usage -= dmnParams.curNumVols * sizeof (u32T);
        free (vdIndex);
	errcnt = -1;
        RAISE_EXCEPTION (0);
    }

    /*
     * Adjust for removed volumes.
     */
    vdHdrCnt = vdIndex[vdIndexCnt - 1];

    vdHdrTbl = (vdHdrT *) malloc (vdHdrCnt * sizeof (vdHdrT));
    if (vdHdrTbl == NULL) {
        fprintf ( stderr, catgets(catd, 1, 8, 
                  "%s: can't allocate memory for disk header array\n"), 
		 funcName);
	errcnt = -1;
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
	        "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		vdHdrCnt * sizeof (vdHdrT), memory_usage);
        RAISE_EXCEPTION (0);
    }
    memory_usage += vdHdrCnt * sizeof (vdHdrT);

    for (i = 0; i < vdHdrCnt; i++) {
        vdHdrTbl[i].vdIndex = 0;
        vdHdrTbl[i].bmtFd = -1;
        vdHdrTbl[i].cnt = 0;
        vdHdrTbl[i].mcellHdr = NULL;
    }  /* end for */

    fprintf( stdout, catgets(catd, 1, 9, "\nChecking disks ...\n\n") );

    for (i = 0; i < vdIndexCnt; i++) {
        vdIndexI = vdIndex[i] - 1;
        vdHdrTbl[vdIndexI].vdIndex = vdIndex[i];
        vdHdrTbl[vdIndexI].bmtFd = -1;
        vdHdrTbl[vdIndexI].bmtPageSize = 0;
        vdHdrTbl[vdIndexI].bmtPageValid = 0;
        vdHdrTbl[vdIndexI].bmtPage = NULL;
        for (j = FREE; j <= BAD; j++) {
            vdHdrTbl[vdIndexI].typeCnt[j] = 0;
        }  /* end for */
        vdHdrTbl[vdIndexI].cnt = 0;
        vdHdrTbl[vdIndexI].mcellHdr = NULL;
        sts = check_disk (
                          dmnParams.bfDomainId,
                          &(vdHdrTbl[vdIndexI])
                          );
        if (sts == EOK) {
            if (tOpt != 0) {
                display_mcell_totals (&(vdHdrTbl[vdIndexI]));
            }
            /* 
	     * check entire free mcell list starting at the vd's free 
	     * mcell listhead 
	     */
        }
        else {
            errcnt++;
        }
        fprintf ( stdout, "\n");
    }  /* end for */


    sts = create_tag_hash_table ();
    if (sts == EOK) {
        fprintf( stdout, catgets(catd, 1, 10, "Checking mcell list ...\n") );
        check_bitfile_mcell_list ();
        fprintf( stdout, catgets(catd, 1, 11, 
		"\nChecking that all in-use mcells are attached to a file's metadata mcell chain...\n"));
        check_mcell_position_field ();
        fprintf( stdout, catgets(catd, 1, 12, 
                 "\nChecking tag directories ...\n")  );
        check_tagdirs (dmnParams.bfSetDirTag);
    }
    else {
        errcnt = -1;
        RAISE_EXCEPTION (sts);
    }
    fprintf( stdout, catgets(catd, 1, 13, 
             "\n+++ Fileset verification +++\n\n"));

    sts = check_dirs_and_files();
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Don't delete the tag hash table until now because
     * check_all_files() needs it around to look up 
     * fileset entries.
     */
    delete_tag_hash_table ();

    for (i = 0; i < vdHdrCnt; i++) {
        if (vdHdrTbl[i].bmtFd != -1) {
            close (vdHdrTbl[i].bmtFd);
        }
    }  /* end for */

    fflush( stdout );

    /* Fall thru to exception handler */

HANDLE_EXCEPTION:
    fflush( stderr );
    umount_it();
    verify_exit( errcnt );

}  /* end main */

/*
 * check_disk
 *
 * This function verifies the consistency of the on-disk structures
 * for one disk in the domain.
 */

static
mlStatusT
check_disk (
            mlBfDomainIdT domainId,  /* in */
            vdHdrT *vdHdr  /* in, modified */
            )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    bsMPgT *bmtPage = NULL;
    u32T bmtPageCnt;
    u32T bmtPageSize;
    ssize_t byteCnt;
    char fileName[MAXPATHLEN];
    char *funcName = "check_disk";
    int i;
    int j;
    bsMCT *mcell;
    mcellHdrT *mcellHdr = NULL;
    u32T mcellHdrCnt;
    bfMCIdT mcellId;
    mlStatusT sts;
    mlBfTagT tag;
    mlVolCountersT volCounters;
    mlVolInfoT volInfo;
    mlVolIoQParamsT volIoQParams;
    mlVolPropertiesT volProp;
    struct sbmIndexEntry *badSBMIndices = NULL;
    int restarted = 0;
    int meta_mcell;
    int not_done, first_pass;
    bsMcellFreeListT *free_list_ptr;
    int next_free_page,
        free_list_head_page,
        free_list_terminator;
    off_t lseek_status;

    sts = advfs_get_vol_params (
                                domainId,
                                vdHdr->vdIndex,
                                &volInfo,
                                &volProp,
                                &volIoQParams,
                                &volCounters
                                );
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 14, 
                 "%s: unable to get volume's %d properties\n"), 
		 funcName, vdHdr->vdIndex);
        fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
	fflush( stderr );
        return sts;
    }

    fprintf ( stdout, catgets(catd, 1, 15, 
             "Checking storage allocated on disk %s\n"),
	     volProp.volName);

restart:
    /*
     * If this domain has an RBMT, open it, read it, and set bits in
     * the in-memory storage bitmap from the extent records there.
     * That is, account for the storage used by the reserved files.
     * Then, do the same for the BMT.  For domains which only have
     * a BMT, just process that.
     */
    for (not_done = TRUE, first_pass = TRUE;
         not_done;
         not_done = (domainVersion >= FIRST_RBMT_VERSION && first_pass) ? 
                    TRUE : FALSE,
         first_pass = FALSE) {
         
        /*
         * Create a tag for the BMT or RBMT.
         */
        if (domainVersion >= FIRST_RBMT_VERSION) {
            if (first_pass) {
                meta_mcell = BFM_RBMT;
            }
            else {
                meta_mcell = BFM_BMT;
            }
        }
        else {
            meta_mcell = BFM_BMT_V3;
        }
        BS_BFTAG_RSVD_INIT (tag, vdHdr->vdIndex, meta_mcell);

        /*
         * Open the BMT or RBMT.
         */
        sprintf (fileName, "%s/0x%08x", dotTagsPath, tag.num);
        bmtFd = open (fileName, O_RDONLY, 0);
        if (bmtFd < 0) {
            fprintf ( stderr, catgets(catd, 1, 16, "%s: can't open '%s'\n"), 
		     funcName, fileName);
            fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
		     errno, sys_errlist[errno]);
            RAISE_EXCEPTION (E_IO);
        }

        /*
         * Get the page size and count for the BMT or RBMT.
         */
        sts = advfs_get_bf_params (bmtFd, &bfAttr, &bfInfo);
        if (sts != EOK) {
            fprintf ( stderr, catgets(catd, 1, 18, 
                      "%s: can't get '%s' bitfile parameters\n"),
		     funcName, fileName);
            fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		     sts, BSERRMSG (sts));
            fflush( stderr );
            RAISE_EXCEPTION (E_IO);
        }

        bmtPageCnt = bfInfo.numPages;
        bmtPageSize = bfInfo.pageSize * BS_BLKSIZE;

        /*
         * Allocate a buffer to read BMT or RBMT pages into.
         */
        bmtPage = (bsMPgT *) malloc (bmtPageSize);
        if (bmtPage == NULL) {
            fprintf ( stderr, catgets(catd, 1, 19, 
                      "%s: can't allocate memory for bmt buffer\n"), funcName);
            fflush( stderr );
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    bmtPageSize, memory_usage);

            RAISE_EXCEPTION (ENO_MORE_MEMORY);
        }
	memory_usage += bmtPageSize;

        /*
         * Seek to the beginning of the BMT or RBMT.
         */
	errno = 0;
        lseek_status = lseek( bmtFd, 0, SEEK_SET );
        if (lseek_status == (off_t) -1) {
            fprintf ( stderr, catgets(catd, 1, 120, 
		      "%s: bad bmt lseek\n"), funcName);
            fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
		     errno, sys_errlist[errno]);
            fflush( stderr );
            RAISE_EXCEPTION(E_IO);
        }

        /*
         * Initialize the in-memory storage bitmap the first time through.
         */
        if (first_pass) {
            sts = bitmap_init (vdHdr->vdIndex);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }

        /*
         * Allocate storage to hold all of the mcell headers.
         */
        mcellHdrCnt = bmtPageCnt * (long)BSPG_CELLS;
        mcellHdr = (mcellHdrT *) malloc (mcellHdrCnt * sizeof (mcellHdrT));
        if (mcellHdr == NULL) {
            fprintf ( stderr, catgets(catd, 1, 20, 
                     "%s: can't allocate memory for mcell header array\n"), 
		     funcName);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    mcellHdrCnt * sizeof (mcellHdrT) , memory_usage);
            fflush( stderr );
            RAISE_EXCEPTION (ENO_MORE_MEMORY);
        }
	memory_usage += mcellHdrCnt * sizeof (mcellHdrT);

        /*
         * Set bits in the in-memory storage bitmap from the extent records in
         * the BMT or RBMT pages.
         */
        for (i = 0; i < bmtPageCnt; i++) {
    
            /*
             * Read the next page in the BMT or RBMT.
             */
            byteCnt = read (bmtFd, (char *)bmtPage, bmtPageSize);
            if (byteCnt != bmtPageSize) {
                fprintf ( stderr, catgets(catd, 1, 21, "%s: bad bmt read\n"), 
			 funcName);
                if (byteCnt >= 0) {
                    fprintf ( stderr, catgets(catd, 1, 22, 
			     "    bytes count - real: %d, expected: %d\n"),
			     byteCnt, bmtPageSize);
                } else {
                    fprintf ( stderr, catgets(catd, 1, 17, 
                             "    errno: %d, %s\n"), 
			     errno, sys_errlist[errno]);
                }
		fflush( stderr );
                RAISE_EXCEPTION (E_IO);
            }
    
            /*
             * Check the consistency of the BMT or RBMT page.
             */
            sts = check_bmt_page (vdHdr, i, bmtPage, mcellHdr);
            /* ignore sts */
            mcellId.page = i;

            /*
             * Walk through the mcells in this page.  For any extent-type
             * records, set the corresponding bits in the in-memory
             * storage bitmap.
             */
            for (j = 0; j < BSPG_CELLS; j++) {
    
                mcell = &(bmtPage->bsMCA[j]);
                mcellId.cell = j;
                if (!BS_BFTAG_EQL (mcell->tag, mlNilBfTag) &&
                    !BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) {
                    set_bitmap (
                                vdHdr->vdIndex,
                                mcellId,
                                mcell, 
                                volInfo.stgCluster,
                                volInfo.totalClusters - 1,  /* max cluster */
                                bitMap,
                                &badSBMIndices
                                );
                }
            }  /* end for loop (record scan) */
        }  /* end for loop (page scan) */

        /*
         * If this is the RBMT pass, close the RBMT and free the storage 
         * we allocated.  We'll allocate new storage in the BMT pass.
         */
        if (domainVersion >= FIRST_RBMT_VERSION && first_pass) {
            close (bmtFd);
            bmtFd = -1;
	    memory_usage -= mcellHdrCnt * sizeof (mcellHdrT);
            free (mcellHdr);
            mcellHdr = NULL;
	    memory_usage -= bmtPageSize;
            free (bmtPage);
            bmtPage = NULL;
        }

    }  /* end for loop (for BMT and RBMT) */

    if (!restarted) {
        sts = check_bitmap (domainId, vdHdr->vdIndex, &badSBMIndices );

        if (badSBMIndices != NULL) {
            bitmap_cleanup ();
            restarted = 1;

            /*
             * Close the BMT and free allocated storage.  We'll
             * reopen the BMT and get new storage when we restart.
             */
            close (bmtFd);
            bmtFd = -1;
	    memory_usage -= mcellHdrCnt * sizeof (mcellHdrT);
            free (mcellHdr);
            mcellHdr = NULL;
	    memory_usage -= bmtPageSize;
            free (bmtPage);
            bmtPage = NULL;
            goto restart;
        }
    }

    /*
     * Make sure that every BMT page that is on the BMT free mcell
     * list does, in fact, have at least one free mcell.  The RBMT
     * is not checked.  The free mcell list starts at mcell 0 in
     * page 1 for V3 domains and at mcell 0 of page 0 for V4 domains.
     * Also, for V3 domains, the free list ends when the nextFreePg
     * field in the BMT page header is 0.  For V4 domains, the free
     * list ends when the nextFreePg field in the BMT page header is
     * -1.
     */
    if (domainVersion >= FIRST_RBMT_VERSION) {
        free_list_head_page = 0;
        free_list_terminator = -1;
    }
    else {
        free_list_head_page = 1;
        free_list_terminator = 0;
    }

    /*
     * Read page 0 or 1 of the BMT.
     */
    byteCnt = read_pg (bmtFd, free_list_head_page, (char *)bmtPage);
    if (byteCnt != bmtPageSize) {
        fprintf ( stderr, catgets(catd, 1, 21, "%s: bad bmt read\n"), 
		 funcName);
        if (byteCnt >= 0) {
            fprintf ( stderr, catgets(catd, 1, 22, 
                     "    bytes count - real: %d, expected: %d\n"),
		     byteCnt, bmtPageSize);
        } else {
            fprintf ( stderr, catgets(catd, 1, 17, 
		     "    errno: %d, %s\n"), errno, sys_errlist[errno]);
        }
        fflush( stderr );
        RAISE_EXCEPTION (E_IO);
    }

    /*
     * The free list pointer is in the first record of the first mcell.
     */
    free_list_ptr = (bsMcellFreeListT *)
                    ((char *)(&bmtPage->bsMCA[0].bsMR0[0])+sizeof(bsMRT));
    next_free_page = free_list_ptr->headPg;
    while (next_free_page != free_list_terminator) {
        /*
         * Read in next page on free list
         */
        byteCnt = read_pg (bmtFd, next_free_page, (char *)bmtPage);
        if (byteCnt != bmtPageSize) {
            fprintf ( stderr, catgets(catd, 1, 21, "%s: bad bmt read\n"), 
		     funcName);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
                         "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, bmtPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, 
                         "    errno: %d, %s\n"), errno, sys_errlist[errno]);
            }
            fflush( stderr );
            RAISE_EXCEPTION (E_IO);
        }

        if (bmtPage->freeMcellCnt == 0) {
            fprintf(stdout, catgets(catd, 1, BADFREE,
                    "BMT page %d is on the mcell free list but has no free mcells.\n"), 
		    next_free_page);
            fprintf(stdout, catgets(catd, 1, BADBMT,
                    "The BMT metadata file is corrupted!\n"));
            fprintf(stdout, catgets(catd, 1, FIXUP1,
                    "To avoid a crash, give the following commands:\n"));
            fprintf(stdout, catgets(catd, 1, FIXUP2,
                    "    dbx -k /vmunix\n"));
            fprintf(stdout, catgets(catd, 1, FIXUP3,
                   "    (dbx) a AdvfsFixUpBMT=1"));
	    errcnt++;
        }
        next_free_page = bmtPage->nextFreePg;
    }
    
    /* ignore sts */
    /*
     * Cleanup.
     */

    bitmap_cleanup ();

    vdHdr->bmtFd = bmtFd;
    vdHdr->bmtPageSize = bmtPageSize;
    vdHdr->bmtPage = bmtPage;
    vdHdr->cnt = mcellHdrCnt;
    vdHdr->mcellHdr = mcellHdr;

    fflush( stdout );

    return EOK;

HANDLE_EXCEPTION:

    if (mcellHdr != NULL) {
        memory_usage -=  mcellHdrCnt * sizeof (mcellHdrT);
        free (mcellHdr);
    }

    bitmap_cleanup ();

    if (bmtPage != NULL) {
        memory_usage -= bmtPageSize;
        free (bmtPage);
    }
    if (bmtFd != -1) {
        close (bmtFd);
    }

    fflush( stderr );

    return sts;

}  /* end check_disk */

/*
 * get_mcell_rec
 *
 * This function scans the mcell for the record specified by the zero-based
 * record index.  It returns the record type, address and size.
 *
 * This function assumes that the mcell record array is consistent.
 */

static
void
get_mcell_rec (
               bsMCT *mcell,  /* in */
               int recIndex,  /* in */
               u16T *recType,  /* out */
               void **recAddr,  /* out */
               u16T *recSize  /* out */
               )
{
    int i;
    bsMRT *recHdr;

    recHdr = (bsMRT *)&(mcell->bsMR0[0]);
    i = 0;
    while ((recHdr->type != BSR_NIL) && (recHdr->bCnt != 0)) {
        if (i == recIndex) {
            *recType = recHdr->type;
            *recAddr = (void *)((char *) recHdr + sizeof (bsMRT));
            *recSize = recHdr->bCnt - sizeof (bsMRT);
            return;
        }
        recHdr = (bsMRT *)&(mcell->bsMR0[recHdr->bCnt]);
        i++;
    }  /* end while */

    *recType = BSR_NIL;

    return;

}  /* end get_mcell_rec */

/*
 * check_bmt_page
 *
 * This function checks the bmt page's consistency.  It verifies the page id,
 * the version, the mcells and the free mcell list.
 */

static
mlStatusT
check_bmt_page (
                vdHdrT *vdHdr,  /* in */
                u32T pageId,  /* in */
                bsMPgT *bmtPage,  /* in */
                mcellHdrT *mcellHdr  /* in, modified */
                )
{
    char *funcName = "check_bmt_page";
    mlStatusT sts = EOK;

    if (bmtPage->pageId != pageId) {
        fprintf ( stdout, catgets(catd, 1, 28, "%s: bad page id\n"), funcName);
        fprintf ( stdout, catgets(catd, 1, 29, "    disk: %d, bmt page: %d\n"),
		 vdHdr->vdIndex, pageId);
        fprintf ( stdout, catgets(catd, 1, 30, 
                 "    real page id: %d, expected page id: %d\n"),
		 bmtPage->pageId, pageId);
	errcnt++;
        return E_BAD_BMT;
    }

    if (bmtPage->megaVersion < 2 ||
	bmtPage->megaVersion > 4) {
        fprintf (stdout, catgets(catd, 1, 31, "%s: bad file system version\n"),
		 funcName);
        fprintf (stdout, catgets(catd, 1, 29, "    disk: %d, bmt page: %d\n"),
		 vdHdr->vdIndex, pageId);
        fprintf (stdout, catgets(catd, 1, 32, "    version found: %d\n"),
		 bmtPage->megaVersion);
	errcnt++;
        return E_BAD_BMT;
    }

    sts = check_mcells (vdHdr, bmtPage, mcellHdr);
    /* ignore sts */

    sts = check_free_mcell_list (vdHdr->vdIndex, bmtPage);
    /* ignore sts */

    fflush( stdout );
    return sts;

}  /* check_bmt_page */

/*
 * check_mcells
 *
 * This function checks the bmt page's mcells.  If an mcell is inconsistent,
 * it marks the mcell as free but it does not insert the mcell onto the page's
 * free mcell list.
 */

static
mlStatusT
check_mcells (
              vdHdrT *vdHdr,  /* in */
              bsMPgT *bmtPage,  /* in */
              mcellHdrT *mcellHdr  /* in, modified */
              )
{
    char *funcName = "check_mcells";
    int i;
    bsMCT *mcell;
    u32T mcellHdrIndex;
    bfMCIdT mcellId;
    mlStatusT sts = EOK;
    mcellTypeT mcellType;

    /*
     * Buzz thru all the mcells in the page.  For each allocated mcell, check
     * its record array.  For each mcell that is not allocated, check it to
     * determine if it is a valid free mcell.
     */

    mcellId.page = bmtPage->pageId;
    for (i = 0; i < BSPG_CELLS; i++) {

        mcell = &(bmtPage->bsMCA[i]);
        mcellId.cell = i;

        mcellHdrIndex = (mcellId.page * (long)BSPG_CELLS) + mcellId.cell;
        mcellHdr[mcellHdrIndex].nextVdIndex = mcell->nextVdIndex;
        mcellHdr[mcellHdrIndex].nextMcellId = mcell->nextMCId;
        mcellHdr[mcellHdrIndex].setTag.num = mcell->bfSetTag.num;
        mcellHdr[mcellHdrIndex].setTag.seq = mcell->bfSetTag.seq;
        mcellHdr[mcellHdrIndex].tag.num = mcell->tag.num;
        mcellHdr[mcellHdrIndex].tag.seq = mcell->tag.seq;
        mcellHdr[mcellHdrIndex].position = 0;
        mcellHdr[mcellHdrIndex].typebits = 0;

        if ((!BS_BFTAG_EQL (mcell->tag, mlNilBfTag) &&
            !BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) ||
            (BS_BFTAG_RSVD (mcell->tag) &&
             BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag))) {
            /*
             * The mcell is allocated.
             */
            sts = check_mcell_recs (vdHdr->vdIndex, mcellId, mcell,
                                    &mcellHdr[mcellHdrIndex].typebits,
                                    &mcellType);
            /* ignore sts */
            mcellHdr[mcellHdrIndex].type = mcellType;
            vdHdr->typeCnt[INUSE] = vdHdr->typeCnt[INUSE] + 1;
            vdHdr->typeCnt[mcellType] = vdHdr->typeCnt[mcellType] + 1;
            /* entry setTag, tag, mcell ptr */
        } else {
            if (BS_BFTAG_EQL (mcell->tag, mlNilBfTag) &&
                BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) {
                if ((mcell->nextVdIndex == mlNilVdIndex) &&
                    (mcell->linkSegment == 0)) {
                    /*
                     * The mcell is free.
                     */
                    mcellHdr[mcellHdrIndex].type = FREE;
                    vdHdr->typeCnt[FREE] = vdHdr->typeCnt[FREE] + 1;
                } else {
                    fprintf(stdout, catgets(catd, 1, 33, 
			     "%s: mcell appears free but has\n"), funcName);
                    fprintf(stdout, catgets(catd, 1, 34, 
			    "    a bad next disk index and/or link segment\n"));
                    fprintf(stdout, catgets(catd, 1, 35, 
			    "    disk: %d, mcell id (page.cell): %d.%d\n"),
                            vdHdr->vdIndex, mcellId.page, mcellId.cell);
                    display_mcell (mcell);
                    mcellHdr[mcellHdrIndex].type = BAD;
                    vdHdr->typeCnt[BAD] = vdHdr->typeCnt[BAD] + 1;
                    if (mcell->nextVdIndex != mlNilVdIndex) {
                        fprintf ( stdout, catgets(catd, 1, 36, 
				 "    Setting next disk index to nil.\n"));
                        mcell->nextVdIndex = mlNilVdIndex;
                    }
                    if (mcell->linkSegment != 0) {
                        fprintf ( stdout, catgets(catd, 1, 37, 
                                 "    Setting link segment to zero.\n"));
                        mcell->linkSegment = 0;
                    }
		    errcnt++;
                    sts = E_INVALID_MSFS_STRUCT;
                }
            } else {
                /*
                 * The mcell is not allocated and is not free.  Fix it so
                 * that it is free.  Note that it is not inserted onto the
                 * page's mcell free list.
                 */
                if (!BS_BFTAG_EQL (mcell->tag, mlNilBfTag) &&
                    BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) {
                    fprintf ( stdout, catgets(catd, 1, 38, 
                             "%s: tag is non-nil but set tag is nil\n"), 
			     funcName);
                } else {
                    fprintf ( stdout, catgets(catd, 1, 39, 
                             "%s: tag is nil but set tag is non-nil\n"), 
			     funcName);
                }
                fprintf ( stdout, catgets(catd, 1, 35, 
			 "    disk: %d, mcell id (page.cell): %d.%d\n"),
			 vdHdr->vdIndex, mcellId.page, mcellId.cell);
                display_mcell (mcell);
                mcellHdr[mcellHdrIndex].type = BAD;
                vdHdr->typeCnt[BAD] = vdHdr->typeCnt[BAD] + 1;
#if 0
                if ((mcell->nextMCId.page != mlNilMCId.page) ||
                    (mcell->nextMCId.cell != mlNilMCId.cell)) {
                    fprintf ( stdout, catgets(catd, 1, 40, 
			     "    Setting next mcell id to nil.\n"));
                    mcell->nextMCId = mlNilMCId;
                }
                if (mcell->nextVdIndex != mlNilVdIndex) {
                    fprintf ( stdout, catgets(catd, 1, 36, 
                             "    Setting next disk index to nil.\n"));
                    mcell->nextVdIndex = mlNilVdIndex;
                }
                if (mcell->linkSegment != 0) {
                    fprintf ( stdout, catgets(catd, 1, 37, 
                             "    Setting link segment to zero.\n"));
                    mcell->linkSegment = 0;
                }
                if (!BS_BFTAG_EQL (mcell->tag, mlNilBfTag)) {
                    fprintf ( stdout, catgets(catd, 1, 41, 
                             "    Setting tag to nil.\n"));
                    mcell->tag = mlNilBfTag;
                }
                if (!BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) {
                    fprintf ( stdout, catgets(catd, 1, 42, 
                             "    Setting set tag to nil.\n"));
                    mcell->bfSetTag = mlNilBfTag;
                }
#endif
		errcnt++;
                sts = E_INVALID_MSFS_STRUCT;
            }
        }  /* end if */
    }  /* end while */

    fflush( stdout );
    return sts;

}  /* end check_mcells */

/*
 * display_mcell
 *
 * This function displays the contents of the mcell.
 */

static
void
display_mcell (
               bsMCT *mcell  /* in */
               )
{
    fprintf ( stdout, catgets(catd, 1, 43, 
             "    next mcell id (page.cell): %d.%d\n"),
	     mcell->nextMCId.page, mcell->nextMCId.cell);
    fprintf ( stdout, catgets(catd, 1, 44, 
             "    next disk index: %d\n"), mcell->nextVdIndex);
    fprintf ( stdout, catgets(catd, 1, 45, 
             "    link segment: %d\n"), mcell->linkSegment);
    fprintf ( stdout, catgets(catd, 1, 46, 
             "    tag: %d.%d (0x%08x.0x%08x)\n"),
	     mcell->tag.num, mcell->tag.seq,
	     mcell->tag.num, mcell->tag.seq);
    fprintf ( stdout, catgets(catd, 1, 47, 
             "    set tag: %d.%d (0x%08x.0x%08x)\n"),
	     mcell->bfSetTag.num, mcell->bfSetTag.seq,
	     mcell->bfSetTag.num, mcell->bfSetTag.seq);

    fflush( stdout );
    return;

}  /* end display_mcell */

/*
 * check_free_mcell_list
 *
 * This function checks the bmt page's free mcell list and count.
 */

static
mlStatusT
check_free_mcell_list (
                       u32T vdIndex,  /* in */
                       bsMPgT *bmtPage  /* in */
                       )
{
    char *funcName = "check_free_mcell_list";
    bsMCT *mcell = NULL;
    int mcellCnt;
    bfMCIdT mcellId;
    bfMCIdT prevMcellId;
    mlStatusT sts = EOK;

    /*
     * The while loop test below does not work if cell 0 of page 0 is
     * free.  This should never be the case, unless something has changed.
     * A cell of 0 and page of 0 means the end of a list.
     */

    prevMcellId = mlNilMCId;
    mcellId = bmtPage->nextfreeMCId;
    mcellCnt = 0;

    /*
     * Check the free mcell list and the free mcell count.
     */

    while ((mcellId.page != mlNilMCId.page) || 
	   (mcellId.cell != mlNilMCId.cell)) {
        if ((mcellId.page == bmtPage->pageId) && 
	    (mcellId.cell < BSPG_CELLS)) {
	    mcell = &(bmtPage->bsMCA[mcellId.cell]);
            if (!BS_BFTAG_EQL (mcell->tag, mlNilBfTag) &&
                !BS_BFTAG_EQL (mcell->bfSetTag, mlNilBfTag)) {
                fprintf ( stdout, catgets(catd, 1, 48, 
                         "%s: the mcell is not free but is on the free mcell list\n"),
			 funcName);
                fprintf ( stdout, catgets(catd, 1, 35, 
                         "    disk: %d, mcell id (page.cell): %d.%d\n"),
			 vdIndex, mcellId.page, mcellId.cell);
                fprintf ( stdout, catgets(catd, 1, 49, 
                         "    prev mcell id (page.cell): %d.%d\n"),
			 prevMcellId.page, prevMcellId.cell);
                prevMcellId = mcellId;
                mcellId = mcell->nextMCId;
		errcnt++;
            }
            mcellCnt++;
            prevMcellId = mcellId;
            mcellId = mcell->nextMCId;
        } else {
            fprintf ( stdout, catgets(catd, 1, 50, 
		     "%s: the next free mcell is not on this page\n"),
		     funcName);
            fprintf ( stdout, catgets(catd, 1, 35, 
                     "    disk: %d, mcell id (page.cell): %d.%d\n"),
		     vdIndex, mcellId.page, mcellId.cell);
            fprintf ( stdout, catgets(catd, 1, 43, 
		     "    next mcell id (page.cell): %d.%d\n"),
		     mcell->nextMCId.page, mcell->nextMCId.cell);
            mcellId = mlNilMCId;  /* can't continue */
	    errcnt++;
        }
    }  /* end while */

    if (bmtPage->freeMcellCnt != mcellCnt) {
        fprintf ( stdout, catgets(catd, 1, 51, 
		 "%s: bad free mcell count\n"), funcName);
        fprintf ( stdout, catgets(catd, 1, 29, 
		 "    disk: %d, bmt page: %d\n"), vdIndex, bmtPage->pageId);
        fprintf ( stdout, catgets(catd, 1, 52, 
                 "    free mcell count - real: %d, expected: %d\n"),
		 bmtPage->freeMcellCnt, mcellCnt );
	errcnt++;
        sts = E_INVALID_MSFS_STRUCT;
    }

    fflush( stdout );
    return sts;
    
}  /* end check_free_mcell_list */

/*
 * check_mcell_recs
 *
 * This function verifies the consistency of the mcell's record array.
 */

static
mlStatusT
check_mcell_recs (
                  u32T vdIndex,  /* in */
                  bfMCIdT mcellId,  /* in */
                  bsMCT *mcell,  /* in */
                  u32T* typebits,
                  mcellTypeT *mcellType  /* out */
                  )
{
    char *funcName = "check_mcell_recs";
    bsMRT *nextRecHdr;
    int recCnt = 0;
    int i;
    bsMRT *recHdr;
    mlStatusT sts = EOK;
    int type[BMTR_FS_STAT] = {0};  /* Hack: Need to define a new max in
                                    * bs_ods.h? */

    recHdr = (bsMRT *)&(mcell->bsMR0[0]);
    if ((recHdr->type == BSR_NIL) || (recHdr->bCnt == 4)) {
        int a;
        a = 1;
    }
    while ((recHdr->type != BSR_NIL) && (recHdr->bCnt != 4)) {

        check_rec (
                   vdIndex,
                   mcellId,
                   recHdr,
                   mcell,
                   &(type[0])
                   );

        nextRecHdr = (bsMRT *)((char *)recHdr + 
			       roundup (recHdr->bCnt, sizeof (int)));
        if (nextRecHdr <= (bsMRT *)&(mcell->bsMR0[BSC_R_SZ - sizeof (bsMRT)])) {
            recHdr = nextRecHdr;
        } else {
            if (nextRecHdr >= (bsMRT *)&(mcell->bsMR0[BSC_R_SZ])) {
                fprintf ( stdout, catgets(catd, 1, 53, 
			 "%s: record extends past end of mcell\n"), funcName);
            } else {
                fprintf ( stdout, catgets(catd, 1, 54, 
                         "%s: no room for trailing nil record header\n"), 
			 funcName);
            }
            fprintf ( stdout, catgets(catd, 1, 35, 
		     "    disk: %d, mcell id (page.cell): %d.%d\n"),
                    vdIndex, mcellId.page, mcellId.cell);
            fprintf ( stdout, catgets(catd, 1, 55, 
		     "    record #: %d\n"), recCnt);
            fprintf ( stdout, catgets(catd, 1, 56, 
		     "    record type: %d\n"), recHdr->type);
            fprintf ( stdout, catgets(catd, 1, 57, 
		     "    record size: %d\n"), recHdr->bCnt);
            fprintf ( stdout, catgets(catd, 1, 58, 
                     "    Setting record type to nil.\n"));
            fprintf ( stdout, catgets(catd, 1, 59, 
		     "    Setting record size to %d.\n"), sizeof (bsMRT));
            recHdr->type = BSR_NIL;
            recHdr->bCnt = sizeof (bsMRT);
	    errcnt++;
            sts = E_INVALID_MSFS_STRUCT;
        }
        recCnt++;
    }  /* end while */

    set_mcell_type (&(type[0]), mcellType);

    for ( i = 0; i < ODS_MAX; ++i) {
        if ( type[i] ) {
            *typebits |= 1 << i;
        }
    }

    fflush( stdout );
    return sts;

}  /* end check_mcell_recs */

/*
 * check_rec
 *
 * This function verifies the consistency of an mcell record.
 */

static
void
check_rec (
           u32T vdIndex,  /* in */
           bfMCIdT mcellId,  /* in */
           bsMRT *recHdr,  /* in */
           bsMCT *mcell,  /* in */
           int *type  /* in, modified */
           )
{
    u32T byteCnt;
    unsigned recSize;

    char *funcName = "check_rec";

    recSize = recHdr->bCnt - sizeof(bsMRT);

    switch (recHdr->type) {

      case BSR_XTNTS:

        type[BSR_XTNTS] = 1;
	byteCnt = sizeof (bsXtntRT);
	/*
	 * Cribbed from bs_init.c :
	 *
	 * Since nothing follow the MISC mcell, we are adding some slop to the
	 * end. The bootblock extent will take up one element. The rest
	 * is for future expansion.
	 */
        if ((mcellId.cell == BFM_MISC && mcellId.page == 0) &&
            (byteCnt < recSize)) {
            byteCnt += (10 * sizeof(bsXtntT));
        }
        break;


      case BSR_ATTR: {
        bsBfAttrT *ap;
        char *str;
        char *stateNames[] = {"INVALID", "CREATING", "DELETING", "VALID"};

        type[BSR_ATTR] = 1;
        byteCnt = sizeof (bsBfAttrT);
        ap = (bsBfAttrT *)((char *)recHdr + sizeof(bsMRT));

        if (ap->state != BSRA_VALID) {
            if (ap->state == BSRA_INVALID || ap->state == BSRA_CREATING ||
                ap->state == BSRA_DELETING) {
                str = stateNames[ap->state];  
            }
            else {
                str = catgets(catd, 1, 60, "BOGUS");
            }
	    
	    if ((!active && (!ap->state == BSRA_DELETING)) || (verbose))
	    {
	        /*
		 * The most common situation for this error on an
		 * active domain is a file on the deffered delete
		 * list.  Don't print the error in that case.
		 */
		fprintf ( stdout, catgets(catd, 1, 61, 
			 "%s: bad bitfile state - %s\n"), funcName, str);
                fprintf ( stdout, catgets(catd, 1, 35, 
			 "    disk: %d, mcell id (page.cell): %d.%d\n"),
			 vdIndex, mcellId.page, mcellId.cell);
		display_tag (*(mlBfTagT *)(&mcell->bfSetTag), 
			     *(mlBfTagT *)(&mcell->tag));
		errcnt++;
	    }
        }
        break;
      }
      case BSR_VD_ATTR:

        type[BSR_VD_ATTR] = 1;
        byteCnt = sizeof (bsVdAttrT);
        break;

      case BSR_DMN_ATTR:

        type[BSR_DMN_ATTR] = 1;
        byteCnt = sizeof (bsDmnAttrT);
        break;

      case BSR_XTRA_XTNTS:

        type[BSR_XTRA_XTNTS] = 1;
        byteCnt = sizeof (bsXtraXtntRT);
        break;

      case BSR_SHADOW_XTNTS:

        type[BSR_SHADOW_XTNTS] = 1;
        byteCnt = sizeof (bsShadowXtntT);
        break;

      case BSR_MCELL_FREE_LIST:

        type[BSR_MCELL_FREE_LIST] = 1;
        byteCnt = sizeof (bsMcellFreeListT);
        break;

      case BSR_BFS_ATTR:

        type[BSR_BFS_ATTR] = 1;
        byteCnt = sizeof (bsBfSetAttrT);
        break;

      case BSR_VD_IO_PARAMS:

        type[BSR_VD_IO_PARAMS] = 1;
        byteCnt = sizeof (vdIoParamsT);
        break;

      case BSR_RSVD11:

        type[BSR_RSVD11] = 1;
        byteCnt = recSize;
        break;

      case BSR_RSVD12:

        type[BSR_RSVD12] = 1;
        byteCnt = recSize;
        break;

      case BSR_RSVD13:

        type[BSR_RSVD13] = 1;
        byteCnt = recSize;
        break;

      case BSR_RSVD17:
        type[BSR_RSVD17] = 1;
        byteCnt = recSize;
        break;

      case BSR_DMN_MATTR:

        type[BSR_DMN_MATTR] = 1;
        byteCnt = sizeof (bsDmnMAttrT);
        break;

      case BSR_BF_INHERIT_ATTR:

        type[BSR_BF_INHERIT_ATTR] = 1;
        byteCnt = sizeof (bsBfInheritAttrT);
        break;

      case BSR_BFS_QUOTA_ATTR:

        type[BSR_BFS_QUOTA_ATTR] = 1;
        byteCnt = sizeof (bsQuotaAttrT);
        break;

      case BSR_PROPLIST_HEAD:
	type[BSR_PROPLIST_HEAD] = 1;
	/* This is variable data.  So, assume it is correct. */
	byteCnt = recSize;
	break;

      case BSR_PROPLIST_DATA:
	type[BSR_PROPLIST_DATA] = 1;
	/* This is variable data.  So, assume it is correct. */
	byteCnt = recSize;
	break;

      case BSR_DMN_TRANS_ATTR:
        type[BSR_DMN_TRANS_ATTR] = 1;
        byteCnt = sizeof (bsDmnTAttrT);
        break;

      case BSR_DMN_SS_ATTR:   /* vfast record */
        type[BSR_DMN_SS_ATTR] = 1;
        byteCnt = sizeof (bsSSDmnAttrT);
        break;

      case BSR_DMN_FREEZE_ATTR:   /* freezefs/thawfs record */
        /* Tough to verify date/time stamps.  Do nothing for now. */
        type[BSR_DMN_FREEZE_ATTR] = 1;
        byteCnt = sizeof(bsDmnFreezeAttrT);
        break;

      case (u16T) BMTR_FS_STAT:

        type[BSR_FILE_SYSTEM] = 1;
        byteCnt = sizeof (struct fs_stat);
        break;

      case (u16T) BMTR_FS_DATA:

        type[BSR_FILE_SYSTEM] = 1;
        /* This is variable data.  So, assume it is correct. */
        byteCnt = recSize;
        break;

      case (u16T) BMTR_FS_UNDEL_DIR:

        type[BSR_FILE_SYSTEM] = 1;
        byteCnt = sizeof (struct undel_dir_rec);
        break;

      case (u16T) BMTR_FS_TIME:

        type[BSR_FILE_SYSTEM] = 1;
        byteCnt = sizeof (int);
        break;

      case (u16T) BMTR_FS_INDEX_FILE:

        type[BMTR_FS_INDEX_FILE] = 1;
        byteCnt = sizeof (bsIdxBmtRecT);
        break;

      case (u16T) BMTR_FS_DIR_INDEX_FILE:

        type[BMTR_FS_DIR_INDEX_FILE] = 1;
        byteCnt = sizeof (bfTagT);
        break;

      default:
        fprintf ( stdout, catgets(catd, 1, 62, 
                 "%s: unknown record type\n"), funcName);
        fprintf ( stdout, catgets(catd, 1, 35, 
                 "    disk: %d, mcell id (page.cell): %d.%d\n"),
                vdIndex, mcellId.page, mcellId.cell);
        fprintf ( stdout, catgets(catd, 1, 63, 
                 "    record type - %d\n"), recHdr->type);
	errcnt++;
        return;
    }

    if (recSize != byteCnt) {
        fprintf ( stdout, catgets(catd, 1, 64, 
                 "%s: bad record size\n"), funcName);
        fprintf ( stdout, catgets(catd, 1, 35, 
                 "    disk: %d, mcell id (page.cell): %d.%d\n"),
		 vdIndex, mcellId.page, mcellId.cell);
        fprintf ( stdout, catgets(catd, 1, 63, 
                 "    record type - %d\n"), recHdr->type);
        fprintf ( stdout, catgets(catd, 1, 65, 
                 "    record size - real: %d, expected: %d\n"), 
		 recSize, byteCnt);
	errcnt++;
    }

    fflush( stdout );
    return;

}  /* end check_rec */

/*
 * set_mcell_type
 *
 * This function sets the mcell type.
 */

static
void
set_mcell_type (
                int *type,  /* in */
                mcellTypeT *mcellType  /* in */
                )
{
    if ((type[BSR_ATTR] != 0) && (type[BSR_XTNTS] != 0)) {
        *mcellType = PRIM;
        inuseMcellCnt = inuseMcellCnt + 1;
    } else {
        if ((type[BSR_XTRA_XTNTS] != 0) || (type[BSR_SHADOW_XTNTS] != 0)) {
            *mcellType = XTNT;
        } else {
            if (type[BSR_FILE_SYSTEM] != 0) {
                *mcellType = SEC;
            } else {
                *mcellType = OTHER;
            }
        }
    }
    return;

}  /* end set_mcell_type */

/*
 * display_mcell_totals
 *
 * This function displays the count of each mcell type.
 */

static
void
display_mcell_totals (
                      vdHdrT *vdHdr  /* in */
                      )
{
    char *funcName = "display_mcell_totals";
    mcellTypeT j;
    u32T totalCnt = 0;

    fprintf ( stdout, catgets(catd, 1, 66, 
             "%s: mcell count breakdown\n"), funcName);
    fprintf ( stdout, "    %s:%u\n", 
	     mcellTypeStr[INUSE], vdHdr->typeCnt[INUSE]);
    totalCnt = totalCnt + vdHdr->typeCnt[INUSE];

    for (j = PRIM; j <= BAD; j++) {
        fprintf ( stdout, "        %s:%u\n", 
		 mcellTypeStr[j], vdHdr->typeCnt[j]);
    }  /* end for */

    fprintf ( stdout, "    %s:%u\n", 
	     mcellTypeStr[FREE], vdHdr->typeCnt[FREE]);
    totalCnt = totalCnt + vdHdr->typeCnt[FREE];

    fprintf ( stdout, "    %s:%u\n", catgets(catd, 1, 67, "Total"), totalCnt);

    fflush( stdout );
    return;

}  /* end display_mcell_totals */

/*
 * bitmap_init
 *
 * This function opens the on-disk storage bitmap and allocates the in-memory
 * bitmap and bitmap buffer page.
 */

static
mlStatusT
bitmap_init (
             u32T vdIndex  /* in */
             )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    u32T bitMapPageCnt;
    u32T bitMapPageSize;
    u32T cellsPerPage;
    char fileName[MAXPATHLEN];
    char *funcName = "bitmap_init";
    u32T i;
    mlStatusT sts;
    mlBfTagT tag;

    /*
     * Allocate a buffer to read the on-disk bitmap pages into.
     */

    BS_BFTAG_RSVD_INIT (tag, vdIndex, BFM_SBM);
    sprintf (fileName, "%s/0x%08x", dotTagsPath, tag.num);
    bitMapFd = open (fileName, O_RDWR, 0);
    if (bitMapFd < 0) {
        fprintf ( stderr, catgets(catd, 1, 16, 
		 "%s: can't open '%s'\n"), funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 17, 
                 "    errno: %d, %s\n"), errno, sys_errlist[errno]);
	fflush( stderr );
        RAISE_EXCEPTION (E_IO);
    }

    sts = advfs_get_bf_params (bitMapFd, &bfAttr, &bfInfo);
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 68, 
                 "%s: can't get %s bitfile parameters\n"),
		 funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 4, 
                 "    error: %d, %s\n"), sts, BSERRMSG (sts));
	fflush( stderr );
        RAISE_EXCEPTION (E_IO);
    }

    bitMapPageCnt = bfInfo.numPages;
    bitMapPageSize = bfInfo.pageSize * BS_BLKSIZE;

    bMPSize = bitMapPageSize;
    bitMapPage = (bsStgBmT *) malloc (bitMapPageSize);
    if (bitMapPage == NULL) {
        fprintf ( stderr, catgets(catd, 1, 69, 
                 "%s: can't allocate memory for bitmap file buffer\n"), 
		 fileName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		bitMapPageSize, memory_usage);
	fflush( stderr );
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    memory_usage += bMPSize;

    /*
     * Calculate the on-disk storage bitmap size, allocate an in-memory
     * storage bitmap having the same size and zero it.  We set bits in
     * the in-mem bitmap from the metadata storage allocation information
     * and compare the in-mem and on-disk storage bitmaps.
     */
    bitMapSize = bitMapPageCnt * (long)bitMapPageSize;
    bitMap = (u32T *) malloc (bitMapSize);

    if (bitMap == NULL) {
        fprintf ( stderr, catgets(catd, 1, 70, 
		 "%s: can't allocate memory for inmem bitmap\n"), fileName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		bitMapPageCnt * (long)bitMapPageSize, memory_usage);
	fflush( stderr );
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    memory_usage += bitMapSize;
    cellsPerPage = bitMapPageSize / sizeof (u32T);
    for (i = 0; i < (bitMapPageCnt * (long)cellsPerPage); i++) {
        bitMap[i] = 0;
    }  /* end for */
    /*
     * Now use up lots of memory. Allocate space big enough to hold all of the
     * tag.nums and settag.nums for every bit set in the above bitmap array.
     * If can't malloc that much, proceed anyway without it.
     */
    file_tags = FALSE;
    set_tags = FALSE;

    filetagsSize = (long)bitMapPageCnt * cellsPerPage * sizeof(bfTagT);
    filetags = (u32T *) malloc (filetagsSize);

    if (filetags == NULL) {
        fprintf ( stderr, catgets(catd, 1, 71, 
		 "%s: can't allocate memory for inmem file tags\n"), fileName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		(long)bitMapPageCnt * cellsPerPage * sizeof(bfTagT),
		memory_usage);
	fflush( stderr );
    } else {
        memory_usage += filetagsSize;
        file_tags = TRUE;
        for (i = 0; i < ((long)bitMapPageCnt * cellsPerPage); i++) {
        filetags[i] = 0;
        }
    }

    settagsSize = (long)bitMapPageCnt * cellsPerPage * sizeof(bfTagT);
    settags = (u32T *) malloc (settagsSize);

    if (settags == NULL) {
        fprintf ( stderr, catgets(catd, 1, 72, 
                 "%s: can't allocate memory for inmem set tags\n"), fileName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		(long)bitMapPageCnt * cellsPerPage * sizeof(bfTagT), 
		memory_usage);
	fflush( stderr );
    } else {
        memory_usage += settagsSize;
        set_tags = TRUE;
        for (i = 0; i < ((long)bitMapPageCnt * cellsPerPage); i++) {
            settags[i] = 0;
        }
    }

    return EOK;

HANDLE_EXCEPTION:

    fflush( stderr );
    bitmap_cleanup ();
    return sts;

}  /* end bitmap_init */

/*
 * remove_bad_bf
 *
 * this function will remove a bitfile that has a storage anomaly
 */

static
mlStatusT
remove_bad_bf(
	      mlBfTagT bad_tag, /* in */
	      mlBfTagT set_tag /* in */
	     )
{
    int root_fd, i;

    /*
     * find the fileset and open the root to provide an fd. This is 
     * because an fd is a required parameter to remove_bf.
     *
     * Only checked i == 0, then returned, instead of 
     * looping through all filesets until the correct one is found.
     */
    for (i = 0; i < numSets; i++) {
	if (set_tag.num == mounted[i].bfsetid.dirTag.num) {

	    root_fd = open(mount_point[i], O_RDONLY, 0);
	    if (root_fd <= 0) {
		break;
	    }
	    advfs_remove_bf( bad_tag.num, root_fd, 1);
	    close(root_fd);
	    fprintf( stdout, catgets(catd, 1, 73, "Removed bad file tag %d.%d\n"),
		    bad_tag.num, bad_tag.seq);
	    fflush( stdout );
	    errcnt++;
	    return(0);
	}
    }

    fprintf( stderr, catgets(catd, 1, 74, 
            "Can't remove bad bf, root not open\n"));
    fflush( stderr );
    return -1;
}

/*
 * set_bitmap
 *
 * This function sets the bits in the in-memory storage bitmap based on 
 * the storage allocation information in the mcell.
 */

static
void
set_bitmap (
            u32T vdIndex,  /* in */
            bfMCIdT mcellId,  /* in */
            bsMCT *mcell,  /* in */
            u32T clusterSize,  /* in */
            u32T maxCluster,  /* in */
            u32T *bitMap, /* in */
            struct sbmIndexEntry **badSBMIndices /* in / out */
            )
{
    u32T allSet = 0xffffffff;
    u32T blkCnt;
    u32T blksPerPage;
    bsXtntT *bsXA;
    u32T bsXACnt;
    u32T firstCluster;
    u32T firstIndex;
    char *funcName = "set_bitmap";
    int highBits;
    u32T i;
    u32T j;
    u32T lastCluster;
    u32T lastIndex;
    int lowBits;
    u32T mask;
    void *recAddr;
    u16T recSize;
    u16T recType;
    bsShadowXtntT *shadowRec;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    mlStatusT sts;
    u32T firstPageIndex;
    u32T lastPageIndex;
    bfTagT rbmtTag;

    /*
     * Get record 0 in the mcell.
     */
    get_mcell_rec (mcell, 0, &recType, &recAddr, &recSize);

    switch (recType) {

      case BSR_ATTR:

        /*
         * We expect that the next record will be a BSR_XNTS record.
         * Get record 1 in the mcell.
         */
        get_mcell_rec (mcell, 1, &recType, &recAddr, &recSize);
        if (recType == BSR_XTNTS) {
            xtntRec = (bsXtntRT *)recAddr;
      
            /* 
             * If this is not a reserved file or a file which contains
             * extent information in the primary mcell, there is nothing
             * to do.
             */
            if (!((FIRST_XTNT_IN_PRIM_MCELL(domainVersion, xtntRec->type)) ||
                  (BS_BFTAG_RSVD (mcell->tag)))) {
                return;
            }
            blksPerPage = xtntRec->blksPerPage;
            bsXA = &(xtntRec->firstXtnt.bsXA[0]);
            bsXACnt = xtntRec->firstXtnt.xCnt;

            /*
             * Make sure that there are no more than BMT_XTNTS extent
             * descriptors in this record.  If there are more, we've
             * probably overwritten the next record in the mcell.
             * The only exception to this rule is the miscellaneous
             * bitfile which overstuffs the bsXA array.
             */
            if ((bsXACnt > BMT_XTNTS) &&
                !(mcellId.cell == BFM_MISC && mcellId.page == 0)) {
                fprintf( stdout, catgets(catd, 1, 303, 
			"Found corrupted BMT metadata for tag %d in fileset %d.\nThere are %d extent descriptors in mcell %d of page %d of the BMT\nbut there should be no more than %d.\n"),
			mcell->tag.num,
			mcell->bfSetTag.num,
			bsXACnt,
			mcellId.cell,
			mcellId.page,
			BMT_XTNTS);
		errcnt++;
            }
        } else {
            fprintf ( stdout, catgets(catd, 1, 75, 
                     "%s: extent record not found in primary mcell\n"), 
		     funcName);
            fprintf ( stdout, catgets(catd, 1, 35, 
                     "    disk: %d, mcell id (page.cell): %d.%d\n"),
		     vdIndex, mcellId.page, mcellId.cell);
            fprintf ( stdout, catgets(catd, 1, 56, 
                     "    record type: %d\n"), recType);
            fprintf ( stdout, catgets(catd, 1, 57, 
                     "    record size: %d\n"), recSize);

	    fflush( stdout );
	    errcnt++;
            return;
        }
        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = (bsXtraXtntRT *)recAddr;
        blksPerPage = xtraXtntRec->blksPerPage;
        bsXA = &(xtraXtntRec->bsXA[0]);
        bsXACnt = xtraXtntRec->xCnt;
        break;

      case BSR_SHADOW_XTNTS:

        shadowRec = (bsShadowXtntT *)recAddr;
        blksPerPage = shadowRec->blksPerPage;
        bsXA = &(shadowRec->bsXA[0]);
        bsXACnt = shadowRec->xCnt;
        break;

      /*
       * If this domain has an RBMT, the RBMT might have been extended.
       * The mcell record chaining used by the RBMT is different from 
       * all other files and metadata files.  It uses a chain of BSR_XTNTS
       * records, one per mcell.  So we check for that here.
       */
      case BSR_XTNTS:
         BS_BFTAG_RSVD_INIT(rbmtTag, vdIndex, BFM_RBMT);
         if ((domainVersion >= FIRST_RBMT_VERSION) &&
             (BS_BFTAG_EQL(mcell->tag, rbmtTag))) {
             xtntRec = (bsXtntRT *)recAddr;
             blksPerPage = xtntRec->blksPerPage;
             bsXA = &(xtntRec->firstXtnt.bsXA[0]);
             bsXACnt = xtntRec->firstXtnt.xCnt;
         }
         else {
            fprintf(stdout, catgets(catd, 1, 292, 
		    "%s: BSR_XTNTS record at first slot in mcell; possible data corruption.\n"), 
		    funcName);
	    errcnt++;
         }
         break;
      
      default:

        /*
         * No extent-type records in this mcell so no bits to set in
         * the in-memory storage bitmap.
         */
        return;
    }

    if (bsXACnt == 0) {
        fprintf ( stdout, catgets(catd, 1, 76, 
		 "%s: extent record found but no extents\n"), funcName);
        fprintf ( stdout, catgets(catd, 1, 35, 
                 "    disk: %d, mcell id (page.cell): %d.%d\n"),
                vdIndex, mcellId.page, mcellId.cell);
        fprintf ( stdout, catgets(catd, 1, 56, "    record type: %d\n"), 
		 recType);
        fprintf ( stdout, catgets(catd, 1, 77, "    count: %d\n"), bsXACnt);

	fflush( stdout );
	errcnt++;
        return;
    }

    /*
     * Set bits in the bit map based on the storage allocation information
     * found in the mcell.
     */

    for (i = 0; i < (bsXACnt - 1); i++) {
        int k;
	int invalidStgMap = 0;
        uint32T page = bsXA[i].bsPage;
        /* Check for the same page, redundantly mapped */

        for ( k = i+1; k < (bsXACnt - 1); k += 1) {
            if ( bsXA[k].bsPage == page && bsXA[k].vdBlk != XTNT_TERM ) {
	        fprintf( stdout, catgets(catd, 1, 78, 
                        "page %d in xtnt slots %d and %d, tag %d, vd%d\n"),
			page, i, k, mcell->tag.num, vdIndex );
		errcnt++;
            }
        }

        if (bsXA[i].vdBlk == XTNT_TERM) {
            continue;
        }
        if (bsXA[i].vdBlk == PERM_HOLE_START) {
            continue;
        }
        /*
         * Allocated extent
         */
        blkCnt = (bsXA[i + 1].bsPage - bsXA[i].bsPage) * (long)blksPerPage;
        firstCluster = bsXA[i].vdBlk / clusterSize;
        /* FIX - check firstCluster cluster alignment?? */
        lastCluster = (firstCluster + (blkCnt / clusterSize)) - 1;
        if (lastCluster > maxCluster) {
            /*
             * The last cluster is beyond the end of the bitmap.
             */
            if (firstCluster > maxCluster) {
                fprintf ( stdout, catgets(catd, 1, 98, 
                         "%s: entire extent exceeds bitmap\n"), funcName);
            } else {
                fprintf ( stdout, catgets(catd, 1, 99, 
                         "%s: end of extent exceeds bitmap\n"), funcName);
            }
            fprintf ( stdout, catgets(catd, 1, 35, 
                     "    disk: %d, mcell id (page.cell): %d.%d\n"),
		     vdIndex, mcellId.page, mcellId.cell);
            fprintf ( stdout, catgets(catd, 1, 100, 
                     "    block offset: %u, block count: %u\n"),
		     bsXA[i].vdBlk, blkCnt);
            fprintf ( stdout, catgets(catd, 1, 90, 
                     "    extent clusters - start: %u, end: %u\n"),
                    firstCluster, lastCluster);
            fprintf ( stdout, catgets(catd, 1, 101, 
                     "    bitmap clusters - start: 0, end: %u\n"), maxCluster);
	    errcnt++;
            continue;
        }
        /*
         * The extent's clusters are within the bitmap.
         */

        /*
         * "firstIndex" is the cell that contains the first
         * cluster.
         *
         * "lastIndex" is the cell that contains the last
         * cluster.
         */

        firstIndex = firstCluster / BITS_PER_CELL;
        firstPageIndex = (firstCluster % BITS_PER_CELL) / CLUST_PER_PAGE;
        lastIndex = lastCluster / BITS_PER_CELL;
        lastPageIndex = (lastCluster % BITS_PER_CELL) / CLUST_PER_PAGE;
        if (*badSBMIndices != NULL) {
            struct sbmIndexEntry *curIdx = *badSBMIndices;
            struct sbmIndexEntry *prevIdx = *badSBMIndices;

            while (curIdx != NULL) {

                if ( (firstIndex < curIdx->sbmIndex ||
                       firstIndex == curIdx->sbmIndex &&
                         firstPageIndex <= curIdx->pageIndex) &&
                     (lastIndex > curIdx->sbmIndex ||
                       lastIndex == curIdx->sbmIndex &&
                         lastPageIndex >= curIdx->pageIndex) ) {

		    if ((!active) || (verbose)) {
		        /*
			 * Another error which is fairly common when running
			 * on an active domain.
			 */
		        fprintf( stdout, catgets(catd, 1, 81, 
                               "%s: found tag for invalid stg map index %d\n"),
				funcName, firstIndex );
			fprintf( stdout, catgets(catd, 1, 82, 
				"    tag = 0x%08x.%04x, ino = %d\n    set tag = 0x%08x.%04x\n"), 
				mcell->tag.num, mcell->tag.seq, mcell->tag.num, 
				mcell->bfSetTag.num, mcell->bfSetTag.seq );
		    }
		    invalidStgMap++;
		    errcnt++;
                    if (fix) {
                        if ((mcell->tag.num > 5) && 
			    !(BS_BFTAG_RSVD(mcell->tag))) {
                            sts = remove_bad_bf(*(mlBfTagT *)(&mcell->tag),
                                              *(mlBfTagT *)(&mcell->bfSetTag));
                        } else {
                            fprintf( stdout, catgets(catd, 1, 83, 
                                    "Can't delete; special file\n"));
			    fflush( stdout );
                        }
                    }
                    if (curIdx == *badSBMIndices) {
                        *badSBMIndices = curIdx->nxt;
                    } else {
                        prevIdx = curIdx->nxt;
                    }
		    memory_usage -= sizeof(struct sbmIndexEntry);
                    free( curIdx );
                    break; /* get out of while loop */

                } else {
                    prevIdx = curIdx;
                    curIdx = curIdx->nxt;
                }
            }
            continue;
        }
	
	if (active && invalidStgMap) {
	    fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_1,
		    "Found %d tags which had an invalid stg map index.\n"),
		    invalidStgMap);
	    fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_0,
		    "Most likely this is from file activity on the active domain.\n\n"));
	}

        /*
         * "lowBits" is the number of bits that should not be
         * set in the first cell.  The first cell is indexed
         * by "firstIndex".
         *
         * "highBits" is the number of bits that should not be
         * set in the last cell.  The last cell is indexed
         * by "lastIndex".
         */

        lowBits = firstCluster % BITS_PER_CELL;
        highBits = (BITS_PER_CELL - 1) - (lastCluster % BITS_PER_CELL);

        if (firstIndex == lastIndex) {
            mask = (allSet << lowBits) & (allSet >> highBits);
            if ((mask & bitMap[firstIndex]) != 0) {
	        errcnt++;
                fprintf ( stdout, catgets(catd, 1, 85, 
                         "%s: double storage cluster allocation\n"), funcName);
                fprintf ( stdout, catgets(catd, 1, 86, 
                         "    disk: %d, mcell id (page.cell): %d.%d tag 0x%08x.%04x ino %d\n"),
			 vdIndex, mcellId.page, mcellId.cell,
			 mcell->tag.num, mcell->tag.seq, mcell->tag.num);
                if (file_tags) {
                    fprintf( stdout, catgets(catd, 1, 87, 
			    "    first allocation file tag 0x%08x "),
			    filetags[firstIndex]); 
                    if (set_tags) {
                        fprintf( stdout, catgets(catd, 1, 88, 
                                "set tag 0x%08x\n"), settags[firstIndex]);
                    } else { 
                        fprintf( stdout, "\n");
                    }
                }
                if (fix) {
                    if ((mcell->tag.num > 5) && !(BS_BFTAG_RSVD(mcell->tag))) {
                        sts = remove_bad_bf(*(mlBfTagT *)(&mcell->tag),
                                            *(mlBfTagT *)(&mcell->bfSetTag));
                    } else {
                        fprintf( stdout, catgets(catd, 1, 83, 
                                "Can't delete; special file\n"));
			fflush( stdout );
                    }
                }
 
                fprintf ( stdout, catgets(catd, 1, 89, 
                         "    extent blocks - offset : %u, count: %u\n"),
			 bsXA[i].vdBlk, blkCnt);
                fprintf ( stdout, catgets(catd, 1, 90, 
                         "    extent clusters - start: %u, end: %u\n"),
			 firstCluster, lastCluster);
                fprintf ( stdout, catgets(catd, 1, 91, 
                         "    bitmap clusters - start: %u, end: %u\n"),
			 firstIndex * BITS_PER_CELL,
			 (firstIndex * BITS_PER_CELL) + BITS_PER_CELL - 1);
                fprintf ( stdout, catgets(catd, 1, 92, 
			 "    bits to set:      0x%08x\n"), mask);
                fprintf ( stdout, catgets(catd, 1, 93, 
                         "    bits already set: 0x%08x\n"), 
			 bitMap[firstIndex]);
            }
            bitMap[firstIndex] = bitMap[firstIndex] | mask;
            if (file_tags) filetags[firstIndex] = (u32T)mcell->tag.num;
            if (set_tags) settags[firstIndex] = (u32T)mcell->bfSetTag.num;
        } else {
            /*
             * First cell.
             */
            mask = allSet << lowBits;
            if ((mask & bitMap[firstIndex]) != 0) {
	        errcnt++;
                fprintf ( stdout, catgets(catd, 1, 85, 
                         "%s: double storage cluster allocation\n"), 
			 funcName);
                fprintf ( stdout, catgets(catd, 1, 96, 
			 "    disk: %d, mcell id (page.cell): %d.%d tag.num 0x%08x.%04x ino %d\n"),
			 vdIndex, mcellId.page, mcellId.cell,
			 mcell->tag.num, mcell->tag.seq, mcell->tag.num);
                if (fix) {
                    if ((mcell->tag.num > 5) && !(BS_BFTAG_RSVD(mcell->tag))) {
                        sts = remove_bad_bf(*(mlBfTagT *)(&mcell->tag),
                                            *(mlBfTagT *)(&mcell->bfSetTag));
                    } else {
                        fprintf( stdout, catgets(catd, 1, 83, 
				"Can't delete; special file\n"));
			fflush( stdout );
                    }
                }
                if (file_tags) {
                    fprintf( stdout, catgets(catd, 1, 87, 
                            "    first allocation file tag 0x%08x "),
                    filetags[firstIndex]); 
                    if (set_tags) {
                        fprintf( stdout, catgets(catd, 1, 88, 
                                "set tag 0x%08x\n"), settags[firstIndex]);
                    } else { 
                        fprintf( stdout, "\n");
                    }
                }
                fprintf ( stdout, catgets(catd, 1, 89, 
                         "    extent blocks - offset : %u, count: %u\n"),
                        bsXA[i].vdBlk, blkCnt);
                fprintf ( stdout, catgets(catd, 1, 90, 
                         "    extent clusters - start: %u, end: %u\n"),
                        firstCluster, lastCluster);
                fprintf ( stdout, catgets(catd, 1, 91, 
                         "    bitmap clusters - start: %u, end: %u\n"),
                        firstIndex * BITS_PER_CELL,
                        (firstIndex * BITS_PER_CELL) + BITS_PER_CELL - 1);
                fprintf ( stdout, catgets(catd, 1, 92, 
                         "    bits to set:      0x%08x\n"), mask);
                fprintf ( stdout, catgets(catd, 1, 93, 
                         "    bits already set: 0x%08x\n"), 
			 bitMap[firstIndex]);
            }
            bitMap[firstIndex] = bitMap[firstIndex] | mask;
            if (file_tags) filetags[firstIndex] = (u32T)mcell->tag.num;
            if (set_tags) settags[firstIndex] = (u32T)mcell->bfSetTag.num;

            /*
             * Middle cells.
             */
            for (j = firstIndex + 1; j < lastIndex; j++) {
                if ((allSet & bitMap[j]) != 0) {
		    errcnt++;
                    fprintf ( stdout, catgets(catd, 1, 85, 
			     "%s: double storage cluster allocation\n"), 
			     funcName);
                    fprintf ( stdout, catgets(catd, 1, 96, 
			     "    disk: %d, mcell id (page.cell): %d.%d tag.num 0x%08x.%04x ino %d\n"),
			     vdIndex, mcellId.page, mcellId.cell, 
			     mcell->tag.num, mcell->tag.seq, mcell->tag.num);
                    if (fix) {
                        if ((mcell->tag.num > 5) && 
			    !(BS_BFTAG_RSVD(mcell->tag))) {
                            sts = remove_bad_bf(*(mlBfTagT *)(&mcell->tag),
                                              *(mlBfTagT *)(&mcell->bfSetTag));
                        } else {
                            fprintf( stdout, catgets(catd, 1, 83, 
                                    "Can't delete; special file\n"));
			    fflush( stdout );
                        }
                    }
                    if (file_tags) {
                        fprintf( stdout, catgets(catd, 1, 87, 
                                "    first allocation file tag 0x%08x "),
				filetags[firstIndex]); 
                        if (set_tags) {
                            fprintf( stdout, catgets(catd, 1, 88, 
                                    "set tag 0x%08x\n"), settags[firstIndex]);
                        } else { 
                            fprintf( stdout, "\n");
                        }
                    }
                    fprintf ( stdout, catgets(catd, 1, 89, 
                             "    extent blocks - offset : %u, count: %u\n"),
			     bsXA[i].vdBlk, blkCnt);
                    fprintf ( stdout, catgets(catd, 1, 90, 
                             "    extent clusters - start: %u, end: %u\n"),
			     firstCluster, lastCluster);
                    fprintf ( stdout, catgets(catd, 1, 91, 
                             "    bitmap clusters - start: %u, end: %u\n"),
			     j * BITS_PER_CELL,
			     (j * BITS_PER_CELL) + BITS_PER_CELL - 1);
                    fprintf ( stdout, catgets(catd, 1, 92, 
                             "    bits to set:      0x%08x\n"), mask);
                    fprintf ( stdout, catgets(catd, 1, 93, 
                             "    bits already set: 0x%08x\n"), bitMap[j]);
                }
                bitMap[j] = bitMap[j] | allSet;
                if (file_tags) filetags[firstIndex] = (u32T)mcell->tag.num;
                if (set_tags) settags[firstIndex] = (u32T)mcell->bfSetTag.num;
            }

            /*
             * Last cell.
             */
            mask = allSet >> highBits;
            if ((mask & bitMap[lastIndex]) != 0) {
	        errcnt++;
                fprintf ( stdout, catgets(catd, 1, 85, 
                         "%s: double storage cluster allocation\n"), funcName);
                fprintf ( stdout, catgets(catd, 1, 96, 
                          "    disk: %d, mcell id (page.cell): %d.%d tag.num 0x%08x.%04x ino %d\n"),
			 vdIndex, mcellId.page, mcellId.cell, 
			 mcell->tag.num, mcell->tag.seq, mcell->tag.num);
                if (fix) {
                    if ((mcell->tag.num > 5) && !(BS_BFTAG_RSVD(mcell->tag))) {
                        sts = remove_bad_bf(*(mlBfTagT *)(&mcell->tag),
                                            *(mlBfTagT *)(&mcell->bfSetTag));
                    } else {
                        fprintf( stdout, catgets(catd, 1, 83, 
				"Can't delete; special file\n"));
			fflush( stdout );
                    }
                }
                if (file_tags) {
                    fprintf( stdout, catgets(catd, 1, 87, 
                            "    first allocation file tag 0x%08x "),
                            filetags[firstIndex]); 
                    if (set_tags) {
                        fprintf( stdout, catgets(catd, 1, 88, 
                                "set tag 0x%08x\n"), settags[firstIndex]);
                    } else { 
                        fprintf( stdout, "\n");
                    }
                }
                fprintf ( stdout, catgets(catd, 1, 89, 
                         "    extent blocks - offset : %u, count: %u\n"),
			 bsXA[i].vdBlk, blkCnt);
                fprintf ( stdout, catgets(catd, 1, 90, 
			 "    extent clusters - start: %u, end: %u\n"),
			 firstCluster, lastCluster);
                fprintf ( stdout, catgets(catd, 1, 91, 
                         "    bitmap clusters - start: %u, end: %u\n"),
			 lastIndex * BITS_PER_CELL,
			 (lastIndex * BITS_PER_CELL) + BITS_PER_CELL - 1);
                fprintf ( stdout, catgets(catd, 1, 92, 
                         "    bits to set:      0x%08x\n"), mask);
                fprintf ( stdout, catgets(catd, 1, 93, 
                         "    bits already set: 0x%08x\n"), bitMap[lastIndex]);
            }
            bitMap[lastIndex] = bitMap[lastIndex] | mask;
            if (file_tags) filetags[firstIndex] = (u32T)mcell->tag.num;
            if (set_tags) settags[firstIndex] = (u32T)mcell->bfSetTag.num;
        }
    }  /* end for */

    fflush( stdout );
    return;

}  /* end set_bitmap */

/*
 * check_bitmap
 *
 * This function verifies the consistency of the storage bitmap for
 * the specified disk.
 */

static
mlStatusT
check_bitmap (
              mlBfDomainIdT domainId,  /* in */
              u32T vdIndex, /* in */
              struct sbmIndexEntry **badSBMIndices /* in / out */
              )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    u32T bitMapPageCnt;
    u32T bitMapPageSize;
    ssize_t byteCnt;
    char *funcName = "check_bitmap";
    int i;
    int inMem;
    int onDisk;
    int badSBM = 0;
    mlStatusT sts;
    int re_write = FALSE;
    u32T pagei;

    sts = advfs_get_bf_params (bitMapFd, &bfAttr, &bfInfo);
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 102, 
		 "%s: can't get disk %d's bitfile parameters\n"),
                funcName, vdIndex);
        fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
	fflush( stderr );
        return E_IO;
    }

    bitMapPageCnt = bfInfo.numPages;
    bitMapPageSize = bfInfo.pageSize * BS_BLKSIZE;

    /*
     * Compare the in-mem and on-disk storage bitmaps.
     *
     * FIX - what if the last page of the bitmap is a partial page?
     * Probably not a problem if the bitmap pages were zeroed when the
     * disk was made.
     */

    for (i = 0; i < bitMapPageCnt; i++) {
        u32T xor = 0;

        byteCnt = read (bitMapFd, (char *)bitMapPage, bitMapPageSize);
        if (byteCnt != bitMapPageSize) {
            fprintf ( stderr, catgets(catd, 1, 103, 
		     "%s: bad stg bitmap read\n"), funcName);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
                         "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, bitMapPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, 
			  "    errno: %d, %s\n"), errno, sys_errlist[errno]);
            }
	    fflush( stderr );
            return E_IO;
        }

///////////////////////////////////////////////////////////////////////////////
//        /*
//         * NOTE:  It would be nice to verify the 'xor' field of the SBM
//         *        page right here but we don't for two reasons:
//         *        1. In version 3 domains, this field was not maintained
//         *           under transaction control in the kernel so it is
//         *           not reliable.
//         *        2. In version 4+ domains, the 'xor' field is, indeed,
//         *           maintained under transaction control and is reliable
//         *           but the activation of this domain at the beginning of
//         *           the program would have already hit any 'xor'
//         *           inconsistencies and the domain would have been 
//         *           paniced, making the mounting and verification of it
//         *           impossible.
//         *        If we ever change the verify algorithm to not mount the
//         *        filesets in the domain, it would be a good idea to do
//         *        something like the following here to check the 'xor'
//         *        fields of version 4+ domains:
//         *          
//         * Only check the xor fields of the SBM for
//         * those domains that carefully maintain these
//         * fields under transaction control.
//         *
//        if (domainVersion >= FIRST_VALID_SBM_XOR_VERSION) {
//
//            /* calculate and compare xor on disk */
//            for (onDisk = 0; onDisk < SBM_LONGS_PG; onDisk++){
//                xor ^= bitMapPage->mapInt[onDisk];
//            }
//            if (bitMapPage->xor != xor){
//                fprintf(stdout, catgets(catd, 1, 263, 
//                        "bitmap xor error: page %d, (index %d-%d) on disk %08x, calculated %08x\n"), 
//                        i, i * SBM_LONGS_PG, (i+1) * SBM_LONGS_PG - 1, 
//                        bitMapPage->xor, xor);
//                fprintf(stdout, catgets(catd, 1, 293, 
//                        "The SBM metadata file appears to be corrupted.\n"));
//	          errcnt++;
//            }
//        }
///////////////////////////////////////////////////////////////////////////////

        for (onDisk = 0, inMem = i * SBM_LONGS_PG;
             onDisk < SBM_LONGS_PG;
             onDisk++, inMem++) {

	    if (bitMap[inMem] != bitMapPage->mapInt[onDisk]) {
                struct sbmIndexEntry *sbmIdx = NULL;

		badSBM++;
		errcnt++;
		
		if ((!active) || (verbose)) {
		    /*
		     * Another error which is fairly common when running
		     * on an active domain.
		     */

		    fprintf ( stdout, catgets(catd, 1, 104, 
			     "%s: bitmap mismatch\n"), funcName);
		    fprintf ( stdout, catgets(catd, 1, 105, 
			     "    disk     : %d\n"), vdIndex);
		    fprintf ( stdout, catgets(catd, 1, 106, 
			     "    map index: %d\n"), inMem);
		    fprintf ( stdout, catgets(catd, 1, 107, 
			     "    clusters - start: %u, end: %u\n"),
			     inMem * BITS_PER_CELL,
			     (inMem * BITS_PER_CELL) + BITS_PER_CELL - 1);
		    fprintf ( stdout, catgets(catd, 1, 108,
			     "    real     : 0x%08x\n"), 
			     bitMapPage->mapInt[onDisk]);
		    fprintf ( stdout, catgets(catd, 1, 109, 
			     "    expected : 0x%08x\n"), bitMap[inMem]);
		    fprintf ( stdout, catgets(catd, 1, 110, 
			     "Bitmap page = %d\n"),
			     i);
		}

                for ( pagei = 0; pagei < sizeof(u32T); pagei++ ) {
                    if ((bitMap[inMem] ^ bitMapPage->mapInt[onDisk]) & 
			(0xff << (pagei * CLUST_PER_PAGE)) ) {

                        sbmIdx = (struct sbmIndexEntry *)malloc( sizeof( struct sbmIndexEntry ) );
                        if (sbmIdx != NULL) {
			    memory_usage += sizeof( struct sbmIndexEntry );
                            sbmIdx->sbmIndex = inMem;
                            sbmIdx->pageIndex = pagei;
                            sbmIdx->nxt = *badSBMIndices;
                            *badSBMIndices = sbmIdx;
                        } else {
                            fprintf( stderr, catgets(catd, 1, 256,
                                   "malloc failed for badSBMIndex\n"));
			    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
				   "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
				    sizeof( struct sbmIndexEntry ), 
				    memory_usage);
			    fflush( stderr );
                        }

		        if (fix) {
		            bitMapPage->mapInt[onDisk] = bitMap[inMem];
		            re_write = TRUE;
		        }
                    }
		} /* end for */
            }
        }  /* end for */
        if (re_write) {
	    fprintf(stdout, catgets(catd, 1, 111, 
		    "Rewriting page %d of the SBM.\n"), i);
	    errcnt++;
            if (lseek(bitMapFd, -bitMapPageSize, SEEK_CUR) == (off_t) -1) {
                fprintf(stderr, catgets(catd, 1, 286, 
                        "Lseek failed: could not fix SBM.\n"));
            }
            else if ((byteCnt = write (bitMapFd, (char *)bitMapPage, 
                                       bitMapPageSize)) != bitMapPageSize) {
                fprintf(stderr, catgets(catd, 1, 287, 
                        "Write failed: could not fix SBM.\n"));
            }
	    re_write = FALSE;
	}		
    }  /* end for */

    if (active && badSBM) {
        fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_2,
		"\nFound %d possible corrupted SBM pages.\n"),
		badSBM);
        fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_0,
		"Most likely this is from file activity on the active domain.\n\n"));
    }


    fflush( stdout );
    return EOK;
}  /* end check_bitmap */

/*
 * bitmap_cleanup
 *
 * This function closes the current on-disk storage bitmap and deallocates
 * the in-memory bitmap and bitmap buffer page.
 */

static
void
bitmap_cleanup (
                )
{
    if (bitMap != NULL) {
        memory_usage -= bitMapSize;
        free (bitMap);
        bitMap = NULL;
    }
    if (bitMapPage != NULL) {
        memory_usage -= bMPSize;
        free (bitMapPage);
        bitMapPage = NULL;
    }
    if (bitMapFd != -1) {
        close (bitMapFd);
        bitMapFd = -1;
    }
    if (filetags != NULL) {
        memory_usage -= filetagsSize;
	free (filetags);
        filetags = NULL;
    }
    if (settags != NULL) {
        memory_usage -= settagsSize;
        free (settags);
        settags = NULL;
    }
    return;

}  /* end bitmap_cleanup */

/*
 * create_tag_hash_table
 *
 * This function creates the tag hash table.
 */

static
mlStatusT
create_tag_hash_table (
                       )
{
    int i;
    int j;
    int k;
    mcellHdrT *mcellHdr;
    bfMCIdT mcellId;
    mlStatusT sts;
    tagHdrT *tagHdr;
    vdHdrT *vdHdr;

    for (i = 0; i < vdHdrCnt; i++) {

        vdHdr = &(vdHdrTbl[i]);

        for (j = 0; j < (vdHdr->cnt / BSPG_CELLS); j++) {

            for (k = 0; k < BSPG_CELLS; k++) {

                mcellHdr = &(vdHdr->mcellHdr[(j * BSPG_CELLS) + k]);
                if ((mcellHdr->type != FREE) && (mcellHdr->type != BAD)) {
                    tagHdr = find_tag (mcellHdr->setTag, mcellHdr->tag);
                    if (tagHdr == NULL) {
                        tagHdr = insert_tag (mcellHdr->setTag, mcellHdr->tag);
                        if (tagHdr == NULL) {
                            continue; 
                        }
                    }
                    mcellId.page = j;
                    mcellId.cell = k;
                    sts = insert_mcell_ptr (tagHdr, vdHdr->vdIndex, mcellId);
                    if (sts != EOK) {
                        return sts;
                    }
                }
            }  /* end for */
        }  /* end for */
    }  /* end for */

    return EOK;

}  /* end create_tag_hash_table */

/*
 * insert_mcell_ptr
 *
 * This function inserts an mcell pointer into a tag header's mcell pointer
 * list.  This function does not check for duplicate entries.
 */

#define MCELL_PTR_INC 3

static
mlStatusT
insert_mcell_ptr (
                  tagHdrT *tagHdr,  /* in */
                  u32T vdIndex,  /* in */
                  bfMCIdT mcellId  /* in */
                  )
{
    int i;
    char *funcName = "insert_mcell_ptr";
    mcellPtrT *newMcellPtr;

    if (tagHdr->cnt == tagHdr->maxCnt) {
        newMcellPtr = (mcellPtrT *) malloc ((tagHdr->maxCnt + MCELL_PTR_INC) *
                                            sizeof (mcellPtrT));
        if (newMcellPtr == NULL) {
            fprintf ( stderr, catgets(catd, 1, 112, 
		     "%s: can't allocate memory for expanded mcell ptr tbl\n"),
		     funcName);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
				    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    (tagHdr->maxCnt + MCELL_PTR_INC) * sizeof (mcellPtrT),    
		    memory_usage);

	    fflush( stderr );
            return ENO_MORE_MEMORY;
        }
	memory_usage += (tagHdr->maxCnt + MCELL_PTR_INC) * sizeof (mcellPtrT);
        for (i = 0; i < tagHdr->cnt; i++) {
            newMcellPtr[i] = tagHdr->mcellPtr[i];
        }  /* end for */
        memory_usage -= tagHdr->cnt * sizeof (mcellPtrT);
        free (tagHdr->mcellPtr);
        tagHdr->mcellPtr = newMcellPtr;
        tagHdr->maxCnt = tagHdr->maxCnt + MCELL_PTR_INC;
    }

    tagHdr->mcellPtr[tagHdr->cnt].vdIndex = vdIndex;
    tagHdr->mcellPtr[tagHdr->cnt].mcellId = mcellId;
    tagHdr->cnt++;

    return EOK;

}  /* end insert_mcell_ptr */

/*
 * insert_tag
 *
 * This function inserts a tag in the tag hash table.  This function does
 * not check for duplicate entries.
 */

static
tagHdrT *
insert_tag (
            mlBfTagT setTag,  /* in */
            mlBfTagT tag  /* in */
            )
{
    char *funcName = "insert_tag";
    setHdrT *setHdr;
    tagHdrT *tagHdr;
    tagHdrT **tagHdrListHead;

    setHdr = find_set_tag (setTag);
    if (setHdr == NULL) {
        setHdr = insert_set_tag (setTag);
        if (setHdr == NULL) {
            fprintf ( stdout, catgets(catd, 1, 113, 
		     "%s: can't insert set tag\n"), funcName);
	    errcnt++;
            display_tag (setTag, tag);
	    fflush( stdout );
            return NULL;
        }
    }

    tagHdr = (tagHdrT *) malloc (sizeof (tagHdrT));
    if (tagHdr == NULL) {
        fprintf ( stderr, catgets(catd, 1, 114, 
		 "%s: can't allocate memory for tag header\n"), funcName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		sizeof (tagHdrT), memory_usage);
	fflush( stderr );
        return NULL;
    }
    memory_usage += sizeof (tagHdrT);

    tagHdr->tag = tag;
    tagHdr->onDiskFlag = 0;
    tagHdr->primIndex = 0;
    tagHdr->xtntIndex = 0;
    tagHdr->cnt = 0;
    tagHdr->maxCnt = 3;
    tagHdr->mcellPtr = (mcellPtrT *) malloc (tagHdr->maxCnt * sizeof (mcellPtrT));
    if (tagHdr->mcellPtr == NULL) {
        free (tagHdr);
        fprintf ( stderr, catgets(catd, 1, 115, 
		 "%s: can't allocate memory for tag hdr's mcell ptr list\n"),
		 funcName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
                "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		tagHdr->maxCnt * sizeof (mcellPtrT), memory_usage);
        memory_usage -= sizeof (tagHdrT);
	fflush( stderr );
        return NULL;
    }
    memory_usage += tagHdr->maxCnt * sizeof (mcellPtrT);

    tagHdrListHead = &(setHdr->tagHashTbl[HASH_FUNC(tag, setHdr->hashTblLen)]);
    tagHdr->nextTagHdr = *tagHdrListHead;
    *tagHdrListHead = tagHdr;

    setHdr->tagCnt++;
    if (tagHdr->nextTagHdr == NULL) {
        /* hash table entry was empty */
        setHdr->nonNilCnt++;
    }

    return tagHdr;

}  /* end insert_tag */

/*
 * find_tag
 *
 * This function finds a tag in the tag hash table.
 */

static
tagHdrT *
find_tag (
          mlBfTagT setTag,  /* in */
          mlBfTagT tag  /* in */
          )
{
    setHdrT *setHdr;
    tagHdrT *tagHdr;

    setHdr = find_set_tag (setTag);
    if (setHdr == NULL) {
        return NULL;
    }

    tagHdr = setHdr->tagHashTbl[HASH_FUNC(tag, setHdr->hashTblLen)];
    while (tagHdr != NULL) {
        if (BS_BFTAG_EQL (tagHdr->tag, tag)) {
            break;  /* out of while */
        }
        tagHdr = tagHdr->nextTagHdr;
    }  /* end while */

    return tagHdr;

}  /* end find_tag */

/*
 * insert_set_tag
 *
 * This function inserts a bitfile set tag in the set table.  This function
 * does not check for duplicate entries.
 */

static
setHdrT *
insert_set_tag (
                mlBfTagT setTag  /* in */
                )
{
    mlBfSetParamsT bfSetParams;
    char *funcName = "insert_set_tag";
    int i;
    setHdrT *newSetHdrTbl;
    setHdrT *setHdr;
    mlBfSetIdT bfSetId;
    mlStatusT sts;

    setHdr = find_set_tag (setTag);
    if (setHdr != NULL) {
        return setHdr;
    }
        
    if (setHdrTbl != NULL) {
        newSetHdrTbl = (setHdrT *) malloc ((setHdrCnt + 1) * sizeof (setHdrT));
        if (newSetHdrTbl == NULL) {
            fprintf ( stderr, catgets(catd, 1, 116, 
		     "%s: can't allocate memory for expanded set table\n"), 
		     funcName);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    (setHdrCnt + 1) * sizeof (setHdrT), memory_usage);
	    fflush( stderr );
            return NULL;
        }
        for (i = 0; i < setHdrCnt; i++) {
            newSetHdrTbl[i] = setHdrTbl[i];
        }  /* end for */

        free (setHdrTbl);
        /* 
         * Update memory used:
         *     (newSetHdrTbl size = (setHdrCnt + 1) * sizeof (setHdrT)) 
         *   - (setHdrTbl size = setHdrCnt * sizeof (setHdrT))
         *   =  sizeof (setHdrT)
         */
        memory_usage += sizeof (setHdrT);     
        setHdrTbl = newSetHdrTbl;
        setHdrCnt++;
    } else {
        setHdrTbl = (setHdrT *) malloc (sizeof (setHdrT));
        if (setHdrTbl == NULL) {
            fprintf ( stderr, catgets(catd, 1, 117, 
                     "%s: can't allocate memory for new set table\n"), 
		     funcName);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    sizeof (setHdrT), memory_usage);

	    fflush( stderr );
            return NULL;
        }
	memory_usage += sizeof (setHdrT);
        setHdrCnt = 1;
    }

    setHdr = &(setHdrTbl[setHdrCnt - 1]);
    setHdr->setTag = setTag;

    if (setTag.num != 0xfffffffe) {
        /*
         * This is not the "static" root tag dir tag.
         */
        bfSetId.domainId = domainId;
        bfSetId.dirTag = setTag;
        sts = msfs_get_bfset_params (bfSetId, &bfSetParams);
        if (sts != EOK) {
            fprintf ( stderr, catgets(catd, 1, 118, 
                     "%s: can't get bitfile set params\n"), funcName);
            fprintf ( stderr, catgets(catd, 1, 4,
                     "    error: %d, %s\n"), sts, BSERRMSG (sts));
            fprintf ( stderr, catgets(catd, 1, 47, 
                     "    set tag: %d.%d (0x%08x.0x%08x)\n"),
		     setTag.num, setTag.seq, setTag.num, setTag.seq);
            setHdrCnt--;
            return NULL;
        }
        if (bfSetParams.cloneId == 0) {
            setHdr->cloneFlag = 0;
            setHdr->origSetTag = mlNilBfTag;
        } else {
            setHdr->cloneFlag = 1;
            setHdr->origSetTag = bfSetParams.origSetTag;
        }
    } else {
        setHdr->cloneFlag = 0;
        setHdr->origSetTag = mlNilBfTag;
    }

    setHdr->tagCnt = 0;
    setHdr->nonNilCnt = 0;
    /*
     * This calculation assumes that all tags have the same bitfile set tag.
     */
    setHdr->hashTblLen = (inuseMcellCnt + 10) / 10;
    setHdr->tagHashTbl = (tagHdrT **) malloc (setHdr->hashTblLen * sizeof (tagHdrT *));
    if (setHdr->tagHashTbl == NULL) {
        fprintf ( stderr, catgets(catd, 1, 119, 
                 "%s: can't allocate memory for set's tag hash table\n"), 
		 funcName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		setHdr->hashTblLen * sizeof (tagHdrT *), memory_usage);
	fflush( stderr );
        setHdrCnt--;
        return NULL;
    }
    memory_usage += (setHdr->hashTblLen * sizeof (tagHdrT *));
    for (i = 0; i < setHdr->hashTblLen; i++) {
        setHdr->tagHashTbl[i] = NULL;
    }  /* end for */

    fflush( stdout );
    return setHdr;
}  /* end insert_set_tag */

/*
 * find_set_tag
 *
 * This function finds a bitfile's set tag in the set table.
 */

static
setHdrT *
find_set_tag (
              mlBfTagT setTag  /* in */
              )
{
    int i;
    setHdrT *setHdr;

    if (setHdrTbl == NULL) {
        return NULL;
    }

    setHdr = NULL;
    for (i = 0; i < setHdrCnt; i++) {
        if (BS_BFTAG_EQL (setTag, setHdrTbl[i].setTag)) {
            return &(setHdrTbl[i]);
        }
    }  /* end for */

    return NULL;

}  /* end find_set_tag */

/*
 * delete_tag_hash_table
 *
 * This function deletes the tag hash table.
 */

static
void
delete_tag_hash_table (
                       )
{
    int i;
    u32T j;
    tagHdrT *nextTagHdr;
    setHdrT *setHdr;
    tagHdrT *tagHdr;

    for (i = 0; i < setHdrCnt; i++) {

        setHdr = &(setHdrTbl[i]);

        for (j = 0; j < setHdr->hashTblLen; j++) {

            tagHdr = setHdr->tagHashTbl[j];
            while (tagHdr != NULL) {
	        memory_usage -= tagHdr->maxCnt * sizeof (mcellPtrT);
		free (tagHdr->mcellPtr);
                nextTagHdr = tagHdr->nextTagHdr;
	        memory_usage -= sizeof (tagHdrT);
                free (tagHdr);
                tagHdr = nextTagHdr;
            }  /* end while */

        }  /* end for */
	memory_usage -= setHdr->hashTblLen * sizeof (tagHdrT *);
        free (setHdr->tagHashTbl);

    }  /* end for */

    memory_usage -= sizeof (setHdrT);
    free (setHdrTbl);
    setHdrTbl = NULL;
    setHdrCnt = 0;

    return;

}  /* end delete_tag_hash_table */

/*
 * check_bitfile_mcell_list
 *
 * This function checks each bitfile mcell list and extent mcell list.  The
 * tags in the hash table identify which bitfiles are checked.
 */

static
void
check_bitfile_mcell_list (
                          )
{
    int i;
    u32T j;
    setHdrT *setHdr;
    mlStatusT sts;
    tagHdrT *tagHdr;

    for (i = 0; i < setHdrCnt; i++) {

        setHdr = &(setHdrTbl[i]);

        if (setHdr->tagCnt > 0) {

            for (j = 0; j < setHdr->hashTblLen; j++) {

                tagHdr = setHdr->tagHashTbl[j];
                while (tagHdr != NULL) {
                    sts = check_one_bitfile_mcell_list (setHdr, tagHdr);
                    if (sts != EOK) {
                        errcnt++;
                    }
                    tagHdr = tagHdr->nextTagHdr;
                }  /* end while */

            }  /* end for */
        }

    }  /* end for */

    return;

}  /* end check_bitfile_mcell_list */

/*
 * check_one_bitfile_mcell_list
 *
 * This function checks the specified bitfile's mcell list and extent
 * mcell list.
 */

static
mlStatusT
check_one_bitfile_mcell_list (
                              setHdrT *setHdr,  /* in */
                              tagHdrT *tagHdr  /* in */
                              )
{
    ssize_t byteCnt;
    int foundFlag;
    char *funcName = "check_one_bitfile_mcell_list";
    u32T i;
    bsMCT *mcell;
    mcellHdrT *mcellHdr;
    bfMCIdT mcellId;
    u32T position;
    void *recAddr;
    u16T recSize;
    u16T recType;
    mlStatusT sts;
    vdHdrT *vdHdr;
    u32T vdIndex = 0;
    bsXtntRT *xtntRec;
    off_t lseek_status;

    /*
     * Set the primary mcell index.
     */

    foundFlag = 0;
    for (i = 0; i < tagHdr->cnt; i++) {
        mcellHdr = MCELL_HDR (tagHdr->mcellPtr[i]);
        if (mcellHdr->type == PRIM) {
            tagHdr->primIndex = i;
            vdIndex = tagHdr->mcellPtr[tagHdr->primIndex].vdIndex;
            mcellId = tagHdr->mcellPtr[tagHdr->primIndex].mcellId;
            foundFlag = 1;
            break;  /* out of for */
        }
    }  /* end for */

    if (foundFlag != 0) {

        /*
         * Follow the bitfile's mcell list and set each mcell's bitfile
         * position.
         */

        set_mcell_position (setHdr, tagHdr, 1, vdIndex, mcellId, &position);

        /*
         * Set the chain mcell index, if any.
         */

        vdHdr = &(vdHdrTbl[vdIndex - 1]);
        if ((vdHdr->bmtPageValid == 0) ||
            ((vdHdr->bmtPageValid != 0) && 
	     (mcellId.page != vdHdr->bmtPage->pageId))) {
	    off_t pos;

	    pos   = mcellId.page * 8192L;
	    errno = 0;

            lseek_status = lseek (vdHdr->bmtFd, pos, SEEK_SET);
            if (lseek_status == (off_t) -1) {
                fprintf ( stderr, catgets(catd, 1, 120, 
			 "%s: bad bmt lseek\n"), funcName);
                fprintf ( stderr, catgets(catd, 1, 17, 
			 "    errno: %d, %s\n"), errno, sys_errlist[errno]);
		fflush( stderr );
                return E_IO;
            }
            byteCnt = read (vdHdr->bmtFd, (char *)vdHdr->bmtPage, 
			    vdHdr->bmtPageSize);
            if (byteCnt != vdHdr->bmtPageSize) {
                fprintf ( stderr, catgets(catd, 1, 21, "%s: bad bmt read\n"), 
			 funcName);
                if (byteCnt >= 0) {
                    fprintf ( stderr, catgets(catd, 1, 22,
			     "    bytes count - real: %d, expected: %d\n"),
                            byteCnt, vdHdr->bmtPageSize);
                } else {
                    fprintf ( stderr, catgets(catd, 1, 17, 
			     "    errno: %d, %s\n"), errno, 
			     sys_errlist[errno]);
                }
		fflush( stderr );
                vdHdr->bmtPageValid = 0;
                return E_IO;
            }
            vdHdr->bmtPageValid = 1;
        }

        mcell = &(vdHdr->bmtPage->bsMCA[mcellId.cell]);
        get_mcell_rec (mcell, 1, &recType, &recAddr, &recSize);
        if (recType == BSR_XTNTS) {
            xtntRec = (bsXtntRT *)recAddr;
        } else {
	    if ((!active) || (verbose)) {
	        /*
		 * Fairly common on an active domain.
		 */
	        fprintf ( stdout, catgets(catd, 1, 121, 
			 "%s: can't find primary extent rec\n"), funcName);
		fprintf ( stdout, catgets(catd, 1, 35, 
		       "    disk: %d, mcell id (page.cell): %d.%d\n"),
		     vdIndex, mcellId.page, mcellId.cell);
		display_tag (setHdr->setTag, tagHdr->tag);
		fflush( stdout );
		errcnt++;
	    }
            return ENO_XTNTS;
        }

        foundFlag = 0;
        for (i = 0; i < tagHdr->cnt; i++) {
            if ((xtntRec->chainVdIndex == tagHdr->mcellPtr[i].vdIndex) &&
                (xtntRec->chainMCId.page == tagHdr->mcellPtr[i].mcellId.page) &&
                (xtntRec->chainMCId.cell == tagHdr->mcellPtr[i].mcellId.cell)) {
                tagHdr->xtntIndex = i;
                vdIndex = tagHdr->mcellPtr[tagHdr->xtntIndex].vdIndex;
                mcellId = tagHdr->mcellPtr[tagHdr->xtntIndex].mcellId;
                foundFlag = 1;
                break;  /* out of for */
            }
        }  /* end for */

        if (foundFlag != 0) {
            /*
             * Follow the bitfile's extent mcell list and set each mcell's
             * bitfile position.
             */
            set_mcell_position (setHdr, tagHdr, position, 
				vdIndex, mcellId, &position);
        }
    } else {
        /* no primary mcell */
        fprintf ( stdout, catgets(catd, 1, 122, "%s: no primary mcell\n"), 
		 funcName);
        display_tag (setHdr->setTag, tagHdr->tag);
	errcnt++;
    }

    /*
     * Verify that each mcell associated with the bitfile has a valid
     * bitfile position.
     */

    for (i = 0; i < tagHdr->cnt; i++) {

        mcellHdr = MCELL_HDR (tagHdr->mcellPtr[i]);
        if (mcellHdr->position == 0) {
	    if ((!active) || (verbose)) {
	        /*
		 * This error can show up hundreds of times on an
		 * active domain.  So at this time do not print
		 * the message if found on active domain.
		 */
	        fprintf ( stdout, catgets(catd, 1, 123, 
			 "Metadata corruption in the BMT of disk %d:  found mcell labelled as belonging\nto file with tag %d in fileset \"%s\" but which is not attached to\nthe metadata mcell chains for that file:\n"), 
			 tagHdr->mcellPtr[i].vdIndex, tagHdr->tag.num, 
			 mounted[setHdr->setTag.num-1].setName);
		fprintf ( stdout, catgets(catd, 1, 35, 
			 "    disk: %d, mcell id (page.cell): %d.%d\n"),
			 tagHdr->mcellPtr[i].vdIndex,
			 tagHdr->mcellPtr[i].mcellId.page,
			 tagHdr->mcellPtr[i].mcellId.cell);
		display_tag (setHdr->setTag, tagHdr->tag);
		errcnt++;
	    }
        }
    }  /* end for */

    fflush( stdout );
    return EOK;

}  /* end check_one_bitfile_mcell_list */

/*
 * set_mcell_position
 *
 * This function sets each specified mcell's bitfile position.  The mcell's
 * position is recorded in the mcell's header.  Each mcell's pointer must be
 * in the tag header's mcell pointer list.  After an mcell's position is set,
 * the next mcell is specified by the mcell header's next mcell pointer.
 */

static
void
set_mcell_position (
                    setHdrT *setHdr,  /* in */
                    tagHdrT *tagHdr,  /* in */
                    u32T firstPosition,  /* in */
                    u32T bfVdIndex,  /* in */
                    bfMCIdT bfMcellId,  /* in */
                    u32T *nextPosition  /* out */
                    )
{
    int foundFlag;
    char *funcName = "set_mcell_position";
    int i;
    mcellHdrT *mcellHdr;
    bfMCIdT mcellId;
    u32T position;
    u32T vdIndex;

    vdIndex = bfVdIndex;
    mcellId = bfMcellId;
    position = firstPosition;

    /*
     * NOTE:  Normally, I also check for a non-nil page or cell.
     * But, bmt page 0, cell 0 must work.  So, the vdIndex check is
     * sufficient.
     */

    while (vdIndex != mlNilVdIndex) {

        foundFlag = 0;
        for (i = 0; i < tagHdr->cnt; i++) {

            if ((vdIndex == tagHdr->mcellPtr[i].vdIndex) &&
                (mcellId.page == tagHdr->mcellPtr[i].mcellId.page) &&
                (mcellId.cell == tagHdr->mcellPtr[i].mcellId.cell)) {
                foundFlag = 1;
                mcellHdr = MCELL_HDR (tagHdr->mcellPtr[i]);
                if (mcellHdr->position == 0) {
                    mcellHdr->position = position;
                    position++;
                } else if ((mcellId.page == 0) &&
                           (mcellId.cell >= 6) && (mcellId.cell <= 27)) {
		    /*
		     * With V4.0 domains created with the '-p' flag,
		     * bs_disk_init creates the BSR_XTR_XTNTS record
		     * chained off of mcell 4, rather than just the
		     * first chain mcell from the BMT BSR_XTNTS record
		     * of mcell 0.
		     */
                } else {
                    fprintf ( stdout, catgets(catd, 1, 124, 
			     "%s: position field already set\n"), funcName);
                    fprintf ( stdout, catgets(catd, 1, 35, 
                             "    disk: %d, mcell id (page.cell): %d.%d\n"),
			     vdIndex, mcellId.page, mcellId.cell);
                    display_tag (setHdr->setTag, tagHdr->tag);
		    errcnt++;
		}
                break;  /* out of for */
            }
        }  /* end for */

        if (foundFlag != 0) {
            vdIndex = mcellHdr->nextVdIndex;
            mcellId = mcellHdr->nextMcellId;
        } else {
            fprintf ( stdout, catgets(catd, 1, 125, 
                     "%s: mcell ptr not found in tag's mcell ptr list\n"),
		     funcName);
            fprintf ( stdout, catgets(catd, 1, 35, 
		     "    disk: %d, mcell id (page.cell): %d.%d\n"),
		     vdIndex, mcellId.page, mcellId.cell);
            display_tag (setHdr->setTag, tagHdr->tag);
            vdIndex = mlNilVdIndex;
            mcellId = mlNilMCId;
	    errcnt++;
        }

    }  /* end while */

    *nextPosition = position;

    fflush( stdout );
    return;

}  /* set_mcell_position */

/*
 * check_mcell_position_field
 *
 * This function checks every mcell on every volume in the domain and if
 * it is not free or marked as being bad, it checks that the 'position'
 * field is not zero.  If it were, that would mean that the mcell was not
 * linked into an mcell chain for any file.
 */

static
void
check_mcell_position_field (
                            )
{
    char *funcName = "check_mcell_position_field";
    int i,k;
    u32T j;
    mcellHdrT *mcellHdr;
    vdHdrT *vdHdr;

    for (i = 0; i < vdHdrCnt; i++) {

        vdHdr = &(vdHdrTbl[i]);

        for (j = 0; j < vdHdr->cnt; j++) {

            mcellHdr = &(vdHdr->mcellHdr[j]);
            if ((mcellHdr->type != FREE) && (mcellHdr->type != BAD)) {
                if (mcellHdr->position == 0) {

		    if ((!active) || (verbose)) {
                        /*
                         * This error can show up hundreds of times on an
                         * active domain.  So at this time do not print
                         * the message if found on active domain.
                         */
                        if ((mcellHdr->setTag.num >= 0) && 
                            (mcellHdr->setTag.num < numSets)) {
                            fprintf(stdout, catgets(catd, 1, 123, 
                                    "Metadata corruption in the BMT of disk %d:  found mcell labelled as belonging\nto file with tag %d in fileset \"%s\" but which is not attached to\nthe metadata mcell chains for that file:\n"), 
                                    vdHdr->vdIndex, mcellHdr->tag.num, 
                                    mounted[mcellHdr->setTag.num-1].setName);
                        } 
                        else {
                            /*
                             * This mcell has an invalid setTag in its header.
                             * Don't use it as an index into mounted[].
                             */
                            fprintf(stdout, catgets(catd, 1, BADSETTAG, 
                                    "Metadata corruption in the BMT of disk %d:  found mcell labelled as belonging\nto file with tag %d in fileset %d but which is not attached to\nthe metadata mcell chains for that file:\n"), 
                                    vdHdr->vdIndex, mcellHdr->tag.num, 
                                    mcellHdr->setTag.num);
                        }
			fprintf ( stdout, catgets(catd, 1, 35, 
				 "    disk: %d, mcell id (page.cell): %d.%d\n"),
				 vdHdr->vdIndex, j / BSPG_CELLS, j % BSPG_CELLS);
			fprintf ( stdout, catgets(catd, 1, 127, "    type: %s"),
				 mcellTypeStr[mcellHdr->type]);
			for ( k = 0; k < ODS_MAX; ++k ) {
                            if ( mcellHdr->typebits & (1 << k) ) {
                                fprintf( stdout," %d", k);
			    }
			}
			fprintf( stdout, "\n");
			display_tag (mcellHdr->setTag, mcellHdr->tag);
			errcnt++;
		    }
                }
            }
        }  /* end for */
    }  /* end for */

    fflush( stdout );
    return;

}  /* end check_mcell_position_field */

/*
 * check_tagdirs
 *
 * This function checks each bitfile set's tag directory.
 */

static
void
check_tagdirs (
               mlBfTagT rootTagDirTag  /* in */
               )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    ssize_t byteCnt;
    char fileName[MAXPATHLEN];
    char *funcName = "check_tagdirs";
    u32T i;
    u32T j;
    u32T numPages;
    int  numMissingEntries = 0;
    bsTDirPgT *rootTagDirPage = NULL;
    u32T rootTagDirPageSize;
    setHdrT *setHdr;
    u32T startTagNum;
    mlBfTagT staticRootTagDirTag;
    mlStatusT sts;
    mlBfTagT tagDirTag;
    tagHdrT *tagHdr;
    bsTMapT *tagMap;
    off_t   lseek_status;

    staticRootTagDirTag.num = -2;
    staticRootTagDirTag.seq = 0;

    /*
     * Check the root tag directory.
     */

    sts = check_one_tagdir (staticRootTagDirTag, rootTagDirTag, 
			    staticRootTagDirTag);
    if (sts != EOK) {
        errcnt++;
        return;
    }

    /*
     * Check the normal tag directories.  The tags for these are found in the
     * root tag directory.
     */

    sprintf (fileName, "%s/0x%08x", dotTagsPath, rootTagDirTag.num);
    rootTagDirFd = open (fileName, O_RDONLY, 0);
    if (rootTagDirFd < 0) {
        fprintf ( stderr, catgets(catd, 1, 16, "%s: can't open '%s'\n"), 
		 funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
		 errno, sys_errlist[errno]);
        RAISE_EXCEPTION (E_IO);
    }

    sts = advfs_get_bf_params (rootTagDirFd, &bfAttr, &bfInfo);
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 68, 
		 "%s: can't get %s bitfile parameters\n"),
		 funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
        RAISE_EXCEPTION (E_IO);
    }

    rootTagDirPageSize = bfInfo.pageSize * (long)BS_BLKSIZE;

    rootTagDirPage = (bsTDirPgT *) malloc (rootTagDirPageSize);
    if (rootTagDirPage == NULL) {
        fprintf ( stderr, catgets(catd, 1, 128, 
		 "%s: can't allocate memory for root tag dir file buffer\n"),
		 funcName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		rootTagDirPageSize, memory_usage);
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    memory_usage += rootTagDirPageSize;

    byteCnt = read (rootTagDirFd, (char *)rootTagDirPage, rootTagDirPageSize);
    if (byteCnt != rootTagDirPageSize) {
        fprintf ( stderr, catgets(catd, 1, 129, 
		 "%s: bad root tag dir read\n"), funcName);
        if (byteCnt >= 0) {
            fprintf ( stderr, catgets(catd, 1, 22, 
                     "    bytes count - real: %d, expected: %d\n"),
		     byteCnt, rootTagDirPageSize);
        } else {
            fprintf ( stderr, catgets(catd, 1, 17, 
		     "    errno: %d, %s\n"), errno, sys_errlist[errno]);
        }
        RAISE_EXCEPTION (E_IO);
    }
    numPages = rootTagDirPage->tMapA[0].tmUninitPg;

    errno = 0;
    lseek_status = lseek (rootTagDirFd, 0, SEEK_SET);
    if (lseek_status == (off_t) -1) {
        fprintf ( stderr, catgets(catd, 1, 130, 
                 "%s: root tag dir lseek\n"), funcName);
        fprintf ( stderr, catgets(catd, 1, 17, 
                 "    errno: %d, %s\n"), errno, sys_errlist[errno]);
        RAISE_EXCEPTION (E_IO);
    }

    for (i = 0; i < numPages; i++) {

        byteCnt = read (rootTagDirFd, (char *)rootTagDirPage, 
			rootTagDirPageSize);
        if (byteCnt != rootTagDirPageSize) {
            fprintf ( stderr, catgets(catd, 1, 129, 
                     "%s: bad root tag dir read\n"), funcName);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
                         "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, rootTagDirPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, 
                         "    errno: %d, %s\n"), errno, sys_errlist[errno]);
            }
            RAISE_EXCEPTION (E_IO);
        }

        tagMap = &(rootTagDirPage->tMapA[0]);

        startTagNum = i * BS_TD_TAGS_PG;

        for (j = 0; j < BS_TD_TAGS_PG; j++) {

            if ((tagMap[j].tmSeqNo & BS_TD_IN_USE) != 0) {
                tagDirTag.num = startTagNum + j;
                tagDirTag.seq = tagMap[j].tmSeqNo;
                sts = check_one_tagdir (staticRootTagDirTag, tagDirTag, 
					tagDirTag);
                if (sts != EOK) {
                    errcnt++;
                }
            }
        }  /* end for */
    }  /* end for */
    memory_usage -= rootTagDirPageSize;
    free (rootTagDirPage);

    close (rootTagDirFd);

    /*
     * Verify that each in-mem tag header's tag is a valid tag in the on-disk
     * tag directories.
     */

    for (i = 0; i < setHdrCnt; i++) {

        setHdr = &(setHdrTbl[i]);

        for (j = 0; j < setHdr->hashTblLen; j++) {

            if (setHdr->tagHashTbl[j] != NULL) {

                tagHdr = setHdr->tagHashTbl[j];
                while (tagHdr != NULL) {
                    /*
                     * NOTE:  A reserved bitfile's tag is not stored on disk.
                     */
                    if ((!BS_BFTAG_RSVD (tagHdr->tag)) &&
			(tagHdr->onDiskFlag == 0)) {
		        if ((!active) || (verbose)) {
			    /*
			     * Another error which is fairly common when 
			     * running on an active domain.
			     */

			    fprintf ( stdout, catgets(catd, 1, 131, 
				     "%s: Found a reference to a file that cannot be found in any directory\n"),
				     funcName);
			    display_tag (setHdr->setTag, tagHdr->tag);
			}
			errcnt++;
			numMissingEntries++;
		    }
                    tagHdr = tagHdr->nextTagHdr;
                }  /* end while */
            }
        }  /* end for */
    }  /* end for */

    if ((active) && (numMissingEntries)) {
        fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_3,
		"\nFound %d references to files that cannot be found in any directory.\n"),
		numMissingEntries);
        fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_0,
		"Most likely this is from file activity on the active domain.\n\n"));
    }


    fflush( stdout );
    return;

HANDLE_EXCEPTION:
    if (sts != EOK) {
        errcnt++;
    }

    if (rootTagDirPage != NULL) {
        memory_usage -= rootTagDirPageSize;
        free (rootTagDirPage);
    }
    if (rootTagDirFd != -1) {
        close (rootTagDirFd);
    }

    fflush( stderr );
    return;

}  /* check_tagdirs */

/*
 * check_one_tagdir
 *
 * This function checks the specified bitfile set's tag directory.
 */

static
mlStatusT
check_one_tagdir (
                  mlBfTagT setSetTag,  /* in */
                  mlBfTagT openSetTag,  /* in */
                  mlBfTagT setTag  /* in */
                  )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    ssize_t byteCnt;
    char fileName[MAXPATHLEN];
    u32T freeListCnt;
    u32T freeTagMapCnt;
    char *funcName = "check_one_tagdir";
    u32T i;
    u32T nextFreePage;
    mlStatusT sts;
    int tagDirFd = -1;
    bsTDirPgT *tagDirPage = NULL;
    u32T tagDirPageSize;
    u32T totalFreeTagMapCnt;
    u32T numPages;
    off_t lseek_status;

    /*
     * Open the tag directory and allocate a buffer to read tag dir pages into.
     */

    sprintf (fileName, "%s/M0x%08x.0x%08x", dotTagsPath, 
	     openSetTag.num, openSetTag.seq);
    tagDirFd = open (fileName, O_RDWR, 0);
    if (tagDirFd < 0) {
        fprintf ( stderr, catgets(catd, 1, 16, "%s: can't open '%s'\n"), 
		 funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
		 errno, sys_errlist[errno]);
        RAISE_EXCEPTION (E_IO);
    }

    sts = advfs_get_bf_params (tagDirFd, &bfAttr, &bfInfo);
    if (sts != EOK) {
        fprintf ( stderr, catgets(catd, 1, 68, 
		 "%s: can't get %s bitfile parameters\n"),
		 funcName, fileName);
        fprintf ( stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
        RAISE_EXCEPTION (E_IO);
    }

    if (bfInfo.numPages == 0) {
        RAISE_EXCEPTION (EOK);
    }

    tagDirPageSize = bfInfo.pageSize * (long)BS_BLKSIZE;

    tagDirPage = (bsTDirPgT *) malloc (tagDirPageSize);
    if (tagDirPage == NULL) {
        fprintf ( stderr, catgets(catd, 1, 133, 
		 "%s: can't allocate memory for tag dir file buffer\n"), 
		 funcName);
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		tagDirPageSize, memory_usage);
        display_tag (setSetTag, setTag);
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    memory_usage += tagDirPageSize;

    /*
     * Get the number of initialized pages and check each tag dir page.
     */

    byteCnt = read (tagDirFd, (char *)tagDirPage, tagDirPageSize);
    if (byteCnt != tagDirPageSize) {
        fprintf ( stderr, catgets(catd, 1, 134, "%s: bad tag dir read\n"), 
		 funcName);
        display_tag (setSetTag, setTag);
        if (byteCnt >= 0) {
            fprintf ( stderr, catgets(catd, 1, 22, 
                     "    bytes count - real: %d, expected: %d\n"),
		     byteCnt, tagDirPageSize);
        } else {
            fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
		     errno, sys_errlist[errno]);
        }
        RAISE_EXCEPTION (E_IO);
    }

    numPages = tagDirPage->tMapA[0].tmUninitPg;

    if (numPages > bfInfo.nextPage) {
        fprintf ( stdout, catgets(catd, 1, 135, 
                 "%s: uninitialized page > next page to allocate\n"), 
		 funcName);
        display_tag (setSetTag, setTag);
        fprintf ( stdout, catgets(catd, 1, 136, 
		 "    uninitialized page: %d, nextpage: %d\n"),
		 numPages, bfInfo.nextPage);
	errcnt++;
        RAISE_EXCEPTION (E_INVALID_MSFS_STRUCT);
    }

    sts = check_tagdir_page (setSetTag, setTag, 0, tagDirPage, 
			     &totalFreeTagMapCnt);

    if ((0 != sts) && (fix)) {
        /* Found error on tag page.  Need to fix */
        sts = fix_tagdir_hdr(tagDirFd, 0, tagDirPage, tagDirPageSize);
	if (0 != sts) {
	    RAISE_EXCEPTION (E_IO);
	}
    }

    numPages = tagDirPage->tMapA[0].tmUninitPg;

    for (i = 1; i < numPages; i++) {

        byteCnt = read (tagDirFd, (char *)tagDirPage, tagDirPageSize);
        if (byteCnt != tagDirPageSize) {
            fprintf ( stderr, catgets(catd, 1, 134, "%s: bad tag dir read\n"),
		     funcName);
            display_tag (setSetTag, setTag);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
			 "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, tagDirPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
			 errno, sys_errlist[errno]);
            }
            RAISE_EXCEPTION (E_IO);
        }

        sts = check_tagdir_page (setSetTag, setTag, i, tagDirPage, 
				 &freeTagMapCnt);

	if ((0 != sts) && (fix)) {
	    /* Found error on tag page.  Need to fix */
	    sts = fix_tagdir_hdr(tagDirFd, 0, tagDirPage, tagDirPageSize);
	    if (0 != sts) {
	        RAISE_EXCEPTION (E_IO);
	    }
	}

        totalFreeTagMapCnt = totalFreeTagMapCnt + freeTagMapCnt;

    }  /* end for */

    /*
     * Check free list.
     */

    errno = 0;
    lseek_status = lseek (tagDirFd, 0, SEEK_SET);
    if (lseek_status == (off_t) -1) {
        fprintf ( stderr, catgets(catd, 1, 137, "%s: bad tag dir lseek\n"), 
		 funcName);
        display_tag (setSetTag, setTag);
        fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
		 errno, sys_errlist[errno]);
        RAISE_EXCEPTION (E_IO);
    }

    byteCnt = read (tagDirFd, (char *)tagDirPage, tagDirPageSize);
    if (byteCnt != tagDirPageSize) {
        fprintf ( stderr, catgets(catd, 1, 134, "%s: bad tag dir read\n"), 
		 funcName);
        display_tag (setSetTag, setTag);
        if (byteCnt >= 0) {
            fprintf ( stderr, catgets(catd, 1, 22, 
                     "    bytes count - real: %d, expected: %d\n"),
		     byteCnt, tagDirPageSize);
        } else {
            fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
		     errno, sys_errlist[errno]);
        }
        RAISE_EXCEPTION (E_IO);
    }
    nextFreePage = tagDirPage->tMapA[0].tmFreeListHead;

    freeListCnt = 0;

    while (nextFreePage != 0) {

        nextFreePage--;  /* 1 based - convert to zero based */

	errno = 0;
        lseek_status = lseek (tagDirFd, nextFreePage * (long)tagDirPageSize, SEEK_SET);
        if (lseek_status == (off_t) -1) {
            fprintf ( stderr, catgets(catd, 1, 137, "%s: bad tag dir lseek\n"),
		     funcName);
            display_tag (setSetTag, setTag);
            fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
		     errno, sys_errlist[errno]);
            RAISE_EXCEPTION (E_IO);
        }

        byteCnt = read (tagDirFd, (char *)tagDirPage, tagDirPageSize);
        if (byteCnt != tagDirPageSize) {
            fprintf ( stderr, catgets(catd, 1, 134, "%s: bad tag dir read\n"),
		     funcName);
            display_tag (setSetTag, setTag);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
			 "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, tagDirPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
			 errno, sys_errlist[errno]);
            }
            RAISE_EXCEPTION (E_IO);
        }

        freeListCnt = freeListCnt +
          (BS_TD_TAGS_PG -
           (tagDirPage->tpPgHdr.numAllocTMaps + 
	    tagDirPage->tpPgHdr.numDeadTMaps));

        if (tagDirPage->tpPgHdr.currPage == 0) {
            /* One less available tag map on page 0 */
            freeListCnt--;
        }

        nextFreePage = tagDirPage->tpPgHdr.nextFreePage;

    }  /* end while */

    if (freeListCnt != totalFreeTagMapCnt) {
        if ((!active) || (verbose)) {
	    /*
	     * This is EXTREMELY common on an active domain,
	     * so I am not even going to consider it an error.
	     */
	    fprintf ( stdout, catgets(catd, 1, 138, 
                     "%s: bad free tag count\n"), 
		     funcName);
	    display_tag (setSetTag, setTag);
	    fprintf ( stdout, catgets(catd, 1, 139, 
		     "    real count: %d, expected count: %d\n"),
		     freeListCnt, totalFreeTagMapCnt);
	    errcnt++;
	}
    }

    /*
     * Cleanup.
     */
    memory_usage -= tagDirPageSize;
    free (tagDirPage);

    close (tagDirFd);

    fflush( stdout );
    return EOK;

HANDLE_EXCEPTION:

    if (tagDirPage != NULL) {
        memory_usage -= tagDirPageSize;
        free (tagDirPage);
    }
    if (tagDirFd != -1) {
        close (tagDirFd);
    }

    fflush( stderr );
    return sts;

}  /* check_one_tagdir */


/*
 * check_tagdir_page
 *
 * This function checks one tag directory's page.
 */

static
int
check_tagdir_page (
                   mlBfTagT setSetTag,  /* in */
                   mlBfTagT setTag,  /* in */
                   u32T currPage,  /* in */
                   bsTDirPgT *tagDirPage,  /* in */
                   u32T *retFreeTagMapCnt  /* out */
                   )
{
    int allocTagMapCnt;
    int deadTagMapCnt;
    int freeListCnt;
    int freeTagMapCnt;
    char *funcName = "check_tagdir_page";
    bsTDirPgHdrT *hdr;
    int i;
    mcellHdrT *mcellHdr;
    bfMCIdT mcellId;
    mcellPtrT mcellPtr;
    int overHeadTagMapCnt;
    setHdrT *setHdr;
    int startI;
    u32T startTagNum;
    mlBfTagT tag;
    tagHdrT *tagHdr;
    bsTMapT *tagMap;
    int totTagMapCnt;
    u32T vdIndex;
    int badNextFreeMap = FALSE;
    int fixable_error = 0;

    hdr = &(tagDirPage->tpPgHdr);
    tagMap = &(tagDirPage->tMapA[0]);

    if (hdr->currPage != currPage) {
        fprintf ( stdout, catgets(catd, 1, 140, 
		 "%s: bad tag dir current page\n"), funcName);
        display_tag (setSetTag, setTag);
        fprintf ( stdout, catgets(catd, 1, 141, 
                 "    real current page: %d, expected current page: %d\n"),
		 hdr->currPage, currPage);
	errcnt++;
	fixable_error = 1;
    }

    if (hdr->nextFreeMap > BS_TD_TAGS_PG) {
        fprintf ( stdout, catgets(catd, 1, 142, 
                 "%s: bad next free tag map pointer\n"), funcName);
        display_tag (setSetTag, setTag);
        fprintf ( stdout, catgets(catd, 1, 143, 
                 "    tag directory page: %d\n"), currPage);
        fprintf ( stdout, catgets(catd, 1, 144, 
                 "    next free tag: %d, valid range: 0-%d\n"),
		 hdr->nextFreeMap, BS_TD_TAGS_PG);
        badNextFreeMap = TRUE;
	errcnt++;
    }

    allocTagMapCnt = 0;
    freeTagMapCnt = 0;
    deadTagMapCnt = 0;

    startTagNum = currPage * (long)BS_TD_TAGS_PG;         /* not one based */

    if (currPage != 0) {
        startI = 0;
        overHeadTagMapCnt = 0;
    } else {
        startI = 1;
        overHeadTagMapCnt = 1;
    }

    for (i = startI; i < BS_TD_TAGS_PG; i++) {

        tag.num = startTagNum + i;
        tag.seq = tagMap[i].tmSeqNo;
        if ((tag.seq & BS_TD_IN_USE) != 0) {
            if (tag.seq == BS_TD_DEAD_SLOT) {
                deadTagMapCnt++;
                fprintf ( stdout, catgets(catd, 1, 145, 
			 "%s: tag is both dead and in use "), funcName);
                display_tag (setTag, tag);
                display_tag_map (currPage, i, &(tagMap[i]));
		errcnt++;
            }      
            allocTagMapCnt++;
            tagHdr = find_tag (setTag, tag);
            if (tagHdr != NULL) {
                tagHdr->onDiskFlag = 1;
                vdIndex = tagHdr->mcellPtr[tagHdr->primIndex].vdIndex;
                mcellId = tagHdr->mcellPtr[tagHdr->primIndex].mcellId;
                mcellHdr = MCELL_HDR (tagHdr->mcellPtr[tagHdr->primIndex]);
                if (mcellHdr->type == PRIM) {
                    if ((tagMap[i].tmVdIndex != (u16T)vdIndex) ||
                        (tagMap[i].tmBfMCId.page != mcellId.page) ||
                        (tagMap[i].tmBfMCId.cell != mcellId.cell)) {
                        fprintf ( stdout, catgets(catd, 1, 146, 
                                 "%s: bad primary mcell ptr\n"), funcName);
                        display_tag (setTag, tag);
                        display_tag_map (currPage, i, &(tagMap[i]));
                        fprintf ( stdout, catgets(catd, 1, 147,
                                 "    real - disk: %d, mcell id (page.cell): %d.%d\n"),
				 tagMap[i].tmVdIndex, tagMap[i].tmBfMCId.page,
				 tagMap[i].tmBfMCId.cell);
                        fprintf ( stdout, catgets(catd, 1, 148, 
                                 "    expected - disk: %d, mcell id (page.cell): %d.%d\n"),
				 vdIndex, mcellId.page, mcellId.cell);
			errcnt++;
                    }
                    /*
                     * If the set is the root tag directory, create a set 
		     * header for each set found.  Some headers may have been 
		     * created during the bmt scan.
                     *
                     * NOTE: This is a hack but it is convenient to do here.  
		     * This helps when the domain has a clone.
                     */
                    if (setTag.num == 0xfffffffe) {
                        setHdr = insert_set_tag (tag);
                        if (setHdr == NULL) {
                            fprintf ( stdout, catgets(catd, 1, 113, 
				     "%s: can't insert set tag\n"), funcName);
                            display_tag (setTag, tag);
                            display_tag_map (currPage, i, &(tagMap[i]));
			    errcnt++;
                        }
                    }
                } else {
                    fprintf ( stdout, catgets(catd, 1, 149, 
                             "%s: in-use tag found but in-mem tag hdr does "),
			     funcName);
                    fprintf ( stdout, catgets(catd, 1, 150, 
                             "not point to a primary mcell\n"));
                    display_tag (setTag, tag);
                    display_tag_map (currPage, i, &(tagMap[i]));
		    errcnt++;
                }
            } else {
                /*
                 * The tag does not have a matching in-mem tag header.
                 * If the bitfile set is a clone, check if the tag entry's
                 * mcell pointer points to an mcell header and if the
                 * mcell header's bitfile set tag is the same as the clone's
                 * original bitfile set's tag.
                 */
                setHdr = find_set_tag (setTag);
                if (setHdr != NULL) {
                    if (setHdr->cloneFlag != 0) {
                        mcellPtr.vdIndex = tagMap[i].tmVdIndex;
                        mcellPtr.mcellId = tagMap[i].tmBfMCId;
                        mcellHdr = MCELL_HDR (mcellPtr);
                        if (mcellHdr->type == PRIM) {
                            if (!BS_BFTAG_EQL (setHdr->origSetTag, 
					       mcellHdr->setTag)) {
                                fprintf ( stdout, catgets(catd, 1, 151, 
                                         "%s: in-use on-disk clone tag's set tag does "),
					 funcName);
                                fprintf ( stdout, catgets(catd, 1, 152, 
                                         " not match original tag's set tag\n"));
                                display_tag_map (currPage, i, &(tagMap[i]));
                                fprintf ( stdout, catgets(catd, 1, 153, 
                                         "        <clone tag>\n"));
                                display_tag (setTag, tag);
                                fprintf ( stdout, catgets(catd, 1, 154, 
                                         "        <original tag>\n"));
                                display_tag (mcellHdr->setTag, mcellHdr->tag);
				errcnt++;
                            }
                            if (!BS_BFTAG_EQL (tag, mcellHdr->tag)) {
                                fprintf ( stdout, catgets(catd, 1, 155, 
                                         "%s: in-use on-disk clone tag does not match "),
					 funcName);
                                fprintf ( stdout, catgets(catd, 1, 156,
                                         " original's tag\n"));
                                display_tag_map (currPage, i, &(tagMap[i]));
                                fprintf ( stdout, catgets(catd, 1, 153, 
                                         "        <clone tag>\n"));
                                display_tag (setTag, tag);
                                fprintf ( stdout, catgets(catd, 1, 154, 
                                         "        <original tag>\n"));
                                display_tag (mcellHdr->setTag, mcellHdr->tag);
				errcnt++;
                            }
                        } else {
                            fprintf ( stdout, catgets(catd, 1, 157, 
                                     "%s: in-use on-disk clone tag does not point to "),
				     funcName);
                            fprintf ( stdout, catgets(catd, 1, 158, 
                                     "in-mem primary mcell\n"));
                            display_tag_map (currPage, i, &(tagMap[i]));
                            display_tag (setTag, tag);
                            fprintf ( stdout, catgets(catd, 1, 159, 
                                     "        <mcell hdr info>\n"));
                            fprintf ( stdout, catgets(catd, 1, 35, 
                                     "    disk: %d, mcell id (page.cell): %d.%d\n"),
				     mcellPtr.vdIndex,
				     mcellPtr.mcellId.page,
				     mcellPtr.mcellId.cell);
                            fprintf ( stdout, catgets(catd, 1, 160, 
                                     "    mcell type: %s\n"),
				     mcellTypeStr[mcellHdr->type]);
                            display_tag (mcellHdr->setTag, mcellHdr->tag);
			    errcnt++;
                        }
                    } else {
		        if ((!active) || (verbose)) {
			    /*
			     * Another error which shows hundreds of times on
			     * an active domain.
			     */

			    fprintf ( stdout, catgets(catd, 1, 161, 
				     "%s: in-use on-disk tag does not have matching in-mem "),
				     funcName);
			    fprintf ( stdout, catgets(catd, 1, 162, "tag hdr\n"));
			    display_tag (setTag, tag);
			    display_tag_map (currPage, i, &(tagMap[i]));
			    errcnt++;
			}
                    }
                } else {
                    fprintf( stdout, catgets(catd, 1, 163, 
			    "%s: in-use on-disk tag does not have an in-mem "),
			    funcName);
                    fprintf ( stdout, catgets(catd, 1, 164, "set tag hdr\n"));
                    display_tag (setTag, tag);
                    display_tag_map (currPage, i, &(tagMap[i]));
		    errcnt++;
                }
            }
        } else {
            if (tag.seq == BS_TD_DEAD_SLOT) {
                deadTagMapCnt++;
            }       
            else {
                freeTagMapCnt++;
            }
        }

    }  /* end for */

    if (hdr->numAllocTMaps != allocTagMapCnt) {
        if ((!active) || (verbose)) {
	    /*
	     * This is EXTREMELY common on an active domain,
	     * so I am not even going to consider it an error.
	     */
	    fprintf ( stdout, catgets(catd, 1, 165, 
		     "%s: bad allocated tag count\n"), funcName);
	    display_tag (setSetTag, setTag);
	    fprintf ( stdout, catgets(catd, 1, 143, 
		     "    tag directory page: %d\n"), currPage);
	    fprintf ( stdout, catgets(catd, 1, 139, 
		     "    real count: %d, expected count: %d\n"),
		     hdr->numAllocTMaps, allocTagMapCnt);
	    fixable_error = 1;
	    errcnt++;
	}
    }

    if (hdr->numDeadTMaps != deadTagMapCnt) {
        fprintf ( stdout, catgets(catd, 1, 166, 
		 "%s: bad dead tag count\n"), funcName);
	display_tag (setSetTag, setTag);
	fprintf ( stdout, catgets(catd, 1, 143, 
		 "    tag directory page: %d\n"), currPage);
	fprintf ( stdout, catgets(catd, 1, 139, 
		 "    real count: %d, expected count: %d\n"),
		 hdr->numDeadTMaps, deadTagMapCnt);
	fixable_error = 1;
	errcnt++;
    }

    totTagMapCnt = allocTagMapCnt + freeTagMapCnt + 
                   deadTagMapCnt + overHeadTagMapCnt;
    if (totTagMapCnt != BS_TD_TAGS_PG) {
        fprintf ( stdout, catgets(catd, 1, 167, 
		 "%s: bad total tag count\n"), funcName);
        display_tag (setSetTag, setTag);
        fprintf ( stdout, catgets(catd, 1, 143, 
                 "    tag directory page: %d\n"), currPage);
        fprintf ( stdout, catgets(catd, 1, 139, 
                 "    real count: %d, expected count: %d\n"),
                BS_TD_TAGS_PG, totTagMapCnt);
	fixable_error = 1;
	errcnt++;
    }

    freeListCnt = 0;
    if (!badNextFreeMap) {
        i = hdr->nextFreeMap;
        while (i != 0) {
            i--;  /* Index in tag map is 1 based */
            if ((tagMap[i].tmSeqNo & BS_TD_IN_USE) == 0) {
                freeListCnt++;
            } else {
	        fprintf ( stdout, catgets(catd, 1, 168, 
			 "%s: bad free tag sequence number\n"), funcName);
		display_tag (setSetTag, setTag);
		fprintf ( stdout, catgets(catd, 1, 143, 
			 "    tag directory page: %d\n"), currPage);
		display_tag_map (currPage, i, &(tagMap[i]));
		errcnt++;
                break;  /* out of for */
            }
            i = tagMap[i].tmNextMap;
            if (i > BS_TD_TAGS_PG) {
                fprintf ( stdout, catgets(catd, 1, 261, 
                         "%s: bad free tag nextMap number %d\n"), funcName, i);
                display_tag (setSetTag, setTag);
                fprintf ( stdout, catgets(catd, 1, 143, 
                         "    tag directory page: %d\n"), currPage);
                display_tag_map (currPage, i, &(tagMap[i]));
		errcnt++;
                break;  /* out of for */
            }
        }  /* end while */
    }

    if (freeListCnt != freeTagMapCnt) {
        if ((!active) || (verbose)) {
	    /*
	     * This is EXTREMELY common on an active domain,
	     * so I am not even going to consider it an error.
	     */

	    fprintf ( stdout, catgets(catd, 1, 138, 
		     "%s: bad free tag count\n"), funcName);
	    display_tag (setSetTag, setTag);
	    fprintf ( stdout, catgets(catd, 1, 143, 
		     "    tag directory page: %d\n"), currPage);
	    fprintf ( stdout, catgets(catd, 1, 139, 
		     "    real count: %d, expected count: %d\n"),
		     freeListCnt, freeTagMapCnt);
	    fixable_error = 1;
	    errcnt++;
	}
    }

    *retFreeTagMapCnt = freeTagMapCnt;
    fflush( stdout );

    return fixable_error;

}  /* check_tagdir_page */

/*
 * display_tag
 *
 * This function diplays the bitfile's tag as well as its bitfile set's tag.
 */

static
void
display_tag (
             mlBfTagT setTag,  /* in */
             mlBfTagT tag  /* in */
             )
{
    fprintf ( stdout, catgets(catd, 1, 47, 
	     "    set tag: %d.%d (0x%08x.0x%08x)\n"),
	     setTag.num, setTag.seq,
	     setTag.num, setTag.seq);
    fprintf ( stdout, catgets(catd, 1, 46, 
	     "    tag: %d.%d (0x%08x.0x%08x)\n"),
	     tag.num, tag.seq,
	     tag.num, tag.seq);
    fflush( stdout );
    return;

}  /* end display_tag */

/*
 * display_tag_map
 *
 * This function diplays the tag map's contents.
 */

static
void
display_tag_map (
                 u32T currPage,  /* in */
                 u32T index,  /* in */
                 bsTMapT *tagMap  /* in */
                 )
{
    fprintf ( stdout, catgets(catd, 1, 143, 
	     "    tag directory page: %d\n"), currPage);
    fprintf ( stdout, catgets(catd, 1, 169, 
             "    tag map entry: %d\n"), index);
    fprintf ( stdout, catgets(catd, 1, 170, 
             "    seqNo: 0x%04x (%d)\n"), tagMap->tmSeqNo, tagMap->tmSeqNo);
    if ((tagMap->tmSeqNo & BS_TD_IN_USE) != 0) {
        fprintf ( stdout, catgets(catd, 1, 171, 
                 "    vdIndex: %d\n"), tagMap->tmVdIndex);
        fprintf ( stdout, catgets(catd, 1, 172, 
                 "    bfMCId (page.cell):  %d.%d\n"),
		 tagMap->tmBfMCId.page, tagMap->tmBfMCId.cell);
    } else {
        fprintf ( stdout, catgets(catd, 1, 173,
                 "    unused:  %d\n"), tagMap->tm_u.tm_s2.unused);
        fprintf ( stdout, catgets(catd, 1, 174, 
                 "    nextMap: %d\n"), tagMap->tmNextMap);
    }

    fflush( stdout );
    return;

}  /* end display_tag_map */

/*
 * Allocates 50% larger 'mounted' array than what is currently needed by
 * maxSets.  Then if the current array has previous allocations, the
 * existing data is copied over to the new array (realloc() provides this
 * service). 
 *
 * Knows about and uses/modifies the globals maxSets and mounted.
 */
statusT
reallocate_bf_mounted_array(void)
{
    size_t size;


    if ( mounted != NULL ) {
	maxSets = (maxSets * 150) / 100;  /* increase allocation by 50% */
    }

    /*
     * allocate mounted_sets array
     */
    size = maxSets * sizeof(mounted_sets);
    mounted = (mounted_sets *)realloc( mounted, size );

    if ( mounted == NULL ) {
	fprintf( stderr, catgets(catd, 1, ALLOC1, 
	    "%s: can't allocate memory for fileset mounted array\n"), Prog);
	fflush( stderr );
	return (-1);
    }

    return (EOK);
}
/*
 * create a mount point in /etc/fdmns/dmnName, mount the filesets
 * in this dir
 */

statusT
mount_it(
    )
{
   char dmnset[MAXPATHLEN] = " ";
   char mntpt[MAXPATHLEN+1];
   char *funcName = "mount_it";
   struct advfs_args mount_args;
   int ret, err, i, n;
   mlBfSetParamsT setParams;
   u32T userId;
   u32T setIdx;


   /*
    * Lock /etc/fdmns.
    */
   lkHndl2 = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
   if (lkHndl < 0) {
       fprintf( stderr, catgets(catd, 1, 175, 
               "%s: error locking '%s'; [%d] %s\n"),
	       Prog, MSFS_DMN_DIR, err, BSERRMSG( err ) );
       goto _error;
   }
   dmn_locked = 1;

   /*
    * lock the specified domain dir
    */
    strcat(dmn_pnt, dmnName);

    lkHndl = lock_file_nb( dmn_pnt, LOCK_EX, &err );
    if (lkHndl < 0) {
        if (err == EWOULDBLOCK) {
	    fprintf (stderr, 
		     catgets(catd, 1, 290, 
			     "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
	    fprintf (stderr, catgets(catd, 1, 175, 
				     "%s: error locking '%s'; [%d] %s\n"),
		    Prog, dmn_pnt, err, BSERRMSG( err ) );
	}
	goto _error;
    }
    dir_locked = 1;

   
   /*
    * Release /etc/fdmns lock
    */
   if (unlock_file( lkHndl2, &err ) == -1) {
       fprintf(stderr, catgets(catd, 1, 291, 
			       "%s: error unlocking '%s'; [%d] %s\n"),
	       Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
       goto _error;
   }
   dmn_locked = 0;

   /*
    * Allocate the initial space for the fileset data.
    */
    if ( reallocate_bf_mounted_array() != EOK )
	return (-1);


   /*
    * find each fileset.
    * create a mount point for each fileset to be mounted on.
    */
    setIdx = 0; /* start with first fileset */
    do {
        /*
         * This routine gets the set params for the next set.
         * When 'setIdx' is zero it means to start with the
         * first set.  Each time the routine is called 'setIdx'
         * is updated to 'point' to the next set in the domain (the
         * update is done by msfs_fset_get_info()).
         *
         * If force == 1, domain recovery will be skipped.
         */

        ret = msfs_fset_get_info(dmnName, &setIdx, &setParams, &userId, force);
        if (ret == EOK) {
            numSets++;
	    if ( numSets > maxSets ) {
		if ( reallocate_bf_mounted_array() != EOK )
		    return (-1);
	    }

	    /*
	     * Initialize the new entry. (at the minimum want to get
	     * is_mounted initialized).
	     */
	    memset( &mounted[numSets -1], 0, sizeof(mounted_sets) );

	    /*
	     * allocate storage for the fileset name
	     */
	    n = strlen(setParams.setName) + 1;
	    if ( n < 2 ) n = 2;	/* minimum needed if root - see below */
	    mounted[numSets -1].setName = (char *)malloc( n );

	    if ( mounted[numSets -1].setName == NULL ) {
		fprintf( stderr, catgets(catd, 1, ALLOC3, 
		    "%s: can't allocate memory for fileset name\n"), Prog);
		fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		        "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
			n, memory_usage);
		fflush( stderr );
		return (-1);
	    }
	    memory_usage += n;
            strcpy(mounted[numSets -1].setName, setParams.setName);

	    mounted[numSets -1].bfsetid.domainId = setParams.bfSetId.domainId;
	    mounted[numSets -1].bfsetid.dirTag.num = setParams.bfSetId.dirTag.num;
	    mounted[numSets -1].bfsetid.dirTag.seq = setParams.bfSetId.dirTag.seq;
	}

    } while (ret == EOK);

    if (ret != E_NO_MORE_SETS) {
        fprintf( stderr, catgets(catd, 1, 176, 
                "%s: can't get set info for domain '%s'\n"),
		Prog, dmnName );
        fprintf( stderr, catgets(catd, 1, 177, "%s: error = %s\n"), 
		Prog, BSERRMSG( ret) );
	fflush( stderr );
    }

   /*
    * If there are no filesets in the domain, exit.
    */
   if (numSets == 0) {
       fprintf(stderr, catgets(catd, 1, 304,
               "%s: there are no filesets in domain %s\n"), Prog, dmnName);
       fflush(stderr);
       exit(1);
   }

   /*
    * Allocate array of pointers to the mount point strings.
    */
    mount_point = (char **)malloc( numSets * sizeof(char *) );
    if ( mount_point == NULL ) {
		fprintf( stderr, catgets(catd, 1, ALLOC2, 
	   		"%s: can't allocate memory for fileset mount_point array\n"), Prog);
		fprintf(stderr, catgets(catd,1,MEMORY_USED, 
			"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
			numSets * sizeof(char *), memory_usage);
		fflush( stderr );
		return (-1);
    }

    /*
     * not all of mount_point[] values are getting initialized to null.
     * We make sure they are.
     */
    for ( i = 0; i <= numSets; i++) {
        mount_point[i] = NULL;
    }
    memory_usage += numSets * sizeof(char *);

    /*
     * Verify is going to be running on an ACTIVE domain.  If we attempt
     * metadata fixes we could cause corruptions.  So we need to be very
     * careful when run with this flag.
     */
    if (active) {
	char domainplushash[MAXPATHLEN];
	int root_mount = 0;
	int mountCnt, i;
	struct statfs *mountTbl = NULL;

	strcpy(domainplushash, dmnName);
	strcat(domainplushash, "#");

	/*
	 * get the mount table.
	 */
	mountCnt = getfsstat(0,0, MNT_NOWAIT);
	if (mountCnt < 0) {
	    fprintf( stderr, catgets(catd, 1, 289, 
		    "%s: unable to retrieve statfs information.\n"), Prog);
	    fflush( stderr );
	    return (-1);
	}
	mountTbl = (struct statfs *) malloc(mountCnt * sizeof (struct statfs));

	if (mountTbl == NULL) {
	    fprintf( stderr, catgets(catd, 1, 199, 
		    "%s: can't allocate memory\n"), Prog);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
                    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    mountCnt * sizeof (struct statfs), memory_usage);
	    fflush( stderr );
	    return (-1);
	}
	memory_usage += (mountCnt * sizeof (struct statfs));

	err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), 
			 MNT_NOWAIT);
	if (err < 0) {
	    fprintf( stderr, catgets(catd, 1, 289, 
		    "%s: unable to retrieve statfs information.\n"), Prog);
	    fflush( stderr );
	    return (-1);
	}

	/*
	 * We need to know what the mountpoint is for the fileset.
	 */
	for (i = 0 ; i < mountCnt ; i++) {
	    int j = 0;
	    int len = 0;

	    len = strlen(domainplushash);

	    /*
	     * First check, Is it AdvFS?
	     */
	    if (mountTbl[i].f_type == MOUNT_MSFS) {
	        /*
		 * Second check, is in this domain?
		 */
	        if (0 == strncmp(domainplushash, mountTbl[i].f_mntfromname, 
				 len)) {
		    /*
		     * Now check ALL filesets for match
		     */
		    for (j = 0 ; j < numSets ; j++) {
		        /*
			 * Check for exact match.
			 */
		        if ( 0 == strcmp(mounted[j].setName,
					 mountTbl[i].f_mntfromname + len)) {
			    mounted[j].is_mounted = 0;

			    n = strlen(mountTbl[i].f_mntonname) + 1;
			    mount_point[j] = (char *) malloc (n);
			    if (mount_point[j] == NULL) {
			        fprintf ( stderr, catgets(catd, 1, 259, 
				         "%s: can't allocate memory for mount_point array\n"),
					 funcName);
				fprintf(stderr, catgets(catd,1,MEMORY_USED, 
					"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
					n, memory_usage);
				fflush( stderr );
				return (-1);
			    }
			    memory_usage += n;
			    strcpy(mount_point[j], mountTbl[i].f_mntonname);
			    break; /* for j loop */
			}
		    }
		}
	    }
	}
	return (0);
    }

    /*
     * create a directory for each fileset -
     * /etc/fdmns/dmn_name/setName_verify_XXXXXX, were XXXXXX is a unique 
     * identifier, and mount the fileset on it
     */
    for (i = 0; i < numSets; i++) {

#define VERIFY_UNIQUE "_verify_XXXXXX"

	strcpy(mntpt, dmn_pnt);
	strcat(mntpt, "/\0");
	strcat(mntpt, mounted[i].setName);
	strcat(mntpt, VERIFY_UNIQUE);

	n = strlen(mntpt) + 1;
	mount_point[i] = (char *) malloc (n);

	if (mount_point[i] == NULL) {
	    fprintf ( stderr, catgets(catd, 1, 259, 
                     "%s: can't allocate memory for mount_point array\n"), 
		     funcName);
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    n, memory_usage);
	    fflush( stderr );
	    return (-1);
	}
	memory_usage += n;
	strcpy(mount_point[i], mntpt);
	
	if (NULL == mktemp(mount_point[i])) {
	    fprintf ( stderr, catgets(catd, 1, 260, 
                     "%s: error mktemp failed\n"), funcName);
	    fflush( stderr );
	    return (-1);
	}

	ret = mkdir(mount_point[i], 0755);
    	if (ret < 0) {
    	    fprintf(stderr, catgets(catd, 1, 283, 
                    "%s: error mkdir failed on %s: %d\n"),
		    Prog, mount_point[i], errno);
	    fprintf( stderr, catgets(catd, 1, 284, 
                    "Root fileset must be writeable.\n"));
	    fflush( stderr );
	    return (-1);
    	}

    	/* make the dmn#set string */

    	strcpy(&dmnset[0], dmnName);
    	strcat(dmnset, "#\0");
    	strcat(dmnset, mounted[i].setName);
    	mount_args.fspec = &dmnset[0];
	if (force) {
	    mount_args.exflags = M_FMOUNT;
	} else {
    	    mount_args.exflags = 0;
	}

    	ret = mount(MOUNT_MSFS, mount_point[i], 0, &mount_args); 
    	if (ret < 0) {
    	    fprintf( stderr, catgets(catd, 1, 179, "Error mounting %s: %d\n"),
		    dmnset, errno);
	    fprintf( stderr, catgets(catd, 1, 180, 
		    "All filesets must be unmounted.\nIf this is an active domain, use the -a option.\n"));
	    fflush( stderr );
	    return (-1);
    	}
	mounted[i].is_mounted = 1;
    }
    
    fflush( stdout );
    return(0);
_error:

    if (dmn_locked) {
        unlock_file( lkHndl2, &err );
    }

    if (dir_locked) {
        unlock_file( lkHndl, &err );
    }

    fflush( stderr );
    return(-1);
}

/*
 * set the cwd back to what it was. close all of
 * the frag files and unmount the filesets.
 * delete the mount points.
 */
void
umount_it()
{
    int i, err;

    chdir(old_cwd);
    if (dir_locked) {
        unlock_file( lkHndl, &err );
    }
     /* run thru the sets in reverse since */
     /*  order is important to clone/original pairs */
    for (i = numSets - 1; i >= 0 ; i--) {
	if (mounted[i].fd != 0) {
	    close(mounted[i].fd);
	}

	if (active) {
	    break;
	}

        if (mounted[i].is_mounted) {
	    if ( umount(mount_point[i], MNT_NOFORCE) == -1 ) {
                fprintf( stderr, catgets(catd, 1, 257,
			"failed to umount %s\n"), mount_point[i]);
            }
	}
	if ( mount_point != NULL && mount_point[i] != NULL ) {
	    if ( rmdir(mount_point[i]) == -1 ) { 
		fprintf( stderr, 
		    catgets(catd, 1, 258, "failed to rmdir %s\n"),
			mount_point[i]);
	    }
	}
	fflush( stderr );
    }	
    return;
}

int
read_pg(
    int fd,
    long pg,
    char *pgp
    )
{
    off_t pos;

    errno = 0;
    pos = lseek( fd, pg * 8192L, SEEK_SET );
    if (pos == (off_t) -1) {
        perror( catgets(catd, 1, 181, "lseek") );
        return pos;
    }

    return read( fd, pgp, 8192 );
}

int
write_pg(
    int fd,
    long pg,
    char *pgp
    )
{
    off_t pos;

    errno = 0;
    pos = lseek( fd, pg * 8192L, SEEK_SET );
    if (pos == (off_t) -1) {
        perror( catgets(catd, 1, 181, "lseek") );
        return pos;
    }

    return write( fd, pgp, 8192 );
}

	
statusT
check_frag_file(
    char *fragBf,
    int index
    )
{
    char *pp;
    extern int optind;
    extern char *optarg;
    long pg, last_page_read;
    int fType = -1;
    struct stat stats;
    slotsPgT grppg;
    int sts, freeFrags, grpFType,
        no_errors_found_in_this_chain;
    void *xtntmap;
    long nextbyte, curbyte;
    mlBfSetParamsT bfSetParams;
    grpHdrT *grpHdrp;
    bfTagT expectedSetTag;
    char *funcName = "check_frag_file";

    fprintf( stdout, catgets(catd, 1, 182, 
	    "Checking frag file headers ...\n\n"));
    mounted[index].fd = open( fragBf, O_RDONLY, 0 );
    if (mounted[index].fd < 0) {
        perror( catgets(catd, 1, 183, "open()") );
	verify_exit( 1 );
    }

    if (fstat( mounted[index].fd, &stats )) {
        perror( catgets(catd, 1, 184, "fstat()") );
	verify_exit( 1 );
    }

    xtntmap = advfs_xtntmap_open( mounted[index].fd, &sts, 0 );
    if (xtntmap == NULL) {
        fprintf( stderr, catgets(catd, 1, 185, 
		"advfs_xtntmap_open() error; %s\n"), BSERRMSG( sts ) );
	fflush( stderr );
        return( 1 );
    }

    curbyte = -1;
    nextbyte = 0;

    advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte );

    pg = nextbyte / 8192;

    while ((read_pg( mounted[index].fd, pg, (char *)&grppg ) ) > 0) {
        sts = check_group( mounted[index].fd, pg, &grppg, fType, 
			  &freeFrags, &grpFType );
        /*
         * If the fileset is hosed, go to the next fileset.
         */
        if (sts == CORRUPTED_FILESET) {
            return (sts);
        }

        curbyte = nextbyte + 16*8192 - 1;

        if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
            break;
        }
        
        pg = nextbyte / 8192;
    }
    
    advfs_xtntmap_close( xtntmap );

    fprintf( stdout, catgets(catd, 1, 186, 
            "Checking frag file type lists ...\n\n"));
    sts = msfs_get_bfset_params (mounted[index].bfsetid, &bfSetParams);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 118, 
                 "%s: can't get bitfile set params\n"), funcName);
        fprintf (stderr, catgets(catd, 1, 4, "    error: %d, %s\n"), 
		 sts, BSERRMSG (sts));
        fprintf (stderr, catgets(catd, 1, 47, 
                 "    set tag: %d.%d (0x%08x.0x%08x)\n"),
		 mounted[index].bfsetid.dirTag.num,
		 mounted[index].bfsetid.dirTag.seq,
		 mounted[index].bfsetid.dirTag.num,
		 mounted[index].bfsetid.dirTag.seq);
        return (sts);
    }

    if (bfSetParams.cloneId) {
        expectedSetTag.num = bfSetParams.origSetTag.num;
        expectedSetTag.seq = bfSetParams.origSetTag.seq;
    }
    else {
        expectedSetTag.num = bfSetParams.bfSetId.dirTag.num;
        expectedSetTag.seq = bfSetParams.bfSetId.dirTag.seq;
    }

    /*
     * Validate all of the frag types.
     *
     * Don't bother if we're active - these checks are mostly invalid,
     * and can cause an infinite loop on an extremely active domain.
     */
    for (grpFType = 0; grpFType < BF_FRAG_MAX && !active ; grpFType++) {

        /*
         * Walk the chain of frag groups of this type to make sure it
         * is valid.
         */
        for (pg = bfSetParams.fragGrps[grpFType].firstFreeGrp,
             last_page_read = BF_FRAG_EOF,
             no_errors_found_in_this_chain = TRUE;
             pg != BF_FRAG_EOG;
             last_page_read = pg, pg = grpHdrp->nextFreeGrp) {

            /*
             * Read in the first page of the next frag group on this list.
             */
            if (read_pg(mounted[index].fd, pg, (char *)&grppg) !=
                BF_FRAG_PG_BYTES) {
                fprintf(stderr, catgets(catd, 1, 188, 
                        "read error: fragPg %d, errno %d\n"), pg, errno );
                fprintf(stdout, catgets(catd, 1, 293, 
                        "Corruption in frag file type list for frag type %d\n"), 
			grpFType);
                no_errors_found_in_this_chain = FALSE;
                break;
            }

            grpHdrp = (grpHdrT *)&grppg;

            /*
             * Make sure we got the page we expected to get.
             */
            if (grpHdrp->self != pg) {
                fprintf(stdout, catgets(catd, 1, 293,
                        "Corruption in frag file type list for frag type %d\n"),
                         grpFType);
                fprintf(stdout, catgets(catd, 1, BADSELF,
                        "Expected to find page %d but found page %d\n"),
                        pg, grpHdrp->self);
                no_errors_found_in_this_chain = FALSE;
                errcnt++;
                break;
            }

            /*
             * Make sure this is the correct type of frag group.
             */
            if (grpFType != grpHdrp->fragType) {
	        fprintf(stdout, catgets(catd, 1, 293, 
                        "Corruption in frag file type list for frag type %d\n"), 
			grpFType);
                fprintf(stdout, catgets(catd, 1, 294, 
                        "Wrong type of frag in frag group list.  Expected %d but found %d\n"), 
			grpFType, grpHdrp->fragType);
                no_errors_found_in_this_chain = FALSE;
		errcnt++;
		break;
            }

            /*
             * Only check the bfSetIds if the dual flag is not set.
             * For a dual mounted domain, the original domainid has been
             * replaced by a new one, but the frag groups still have the
             * original one. So, we can skip the domainid check for such
             * domains.
             */
            if ( !BS_BFTAG_EQL(grpHdrp->setId.dirTag, expectedSetTag) ||
                !(dual || BS_UID_EQL(grpHdrp->setId.domainId, 
                            bfSetParams.bfSetId.domainId)) ) {
                fprintf(stdout, catgets(catd, 1, 293,
                        "Corruption in frag file type list for frag type %d\n"),
                         grpFType);
                fprintf(stdout, catgets(catd, 1, BADSETID,
                        "Expected to find set ID %08x.%08x.%x.%04x but found set ID %08x.%08x.%x.%04x\n"),
                        bfSetParams.bfSetId.domainId.tv_sec,
                        bfSetParams.bfSetId.domainId.tv_usec,
                        expectedSetTag.num,
                        expectedSetTag.seq,
                        grpHdrp->setId.domainId.tv_sec,
                        grpHdrp->setId.domainId.tv_usec,
                        grpHdrp->setId.dirTag.num,
                        grpHdrp->setId.dirTag.seq);
                no_errors_found_in_this_chain = FALSE;
                errcnt++;
                break;
            }
        }

        /*
         * Verify that the lastFreeGroup field in the bitfile set
         * metadata for this type of frag is correct.  Don't check
         * this for group type 0 as the lastFreeGroup field is not
         * maintained for that type of frag group.
         */
        if (no_errors_found_in_this_chain &&
            grpFType != BF_FRAG_FREE_GRPS &&
            bfSetParams.fragGrps[grpFType].lastFreeGrp != last_page_read) {
            fprintf(stdout, catgets(catd, 1, 293, 
                    "Corruption in frag file type list for frag type %d\n"), 
		    grpFType);
            fprintf(stdout, catgets(catd, 1, 295, 
		    "Found incorrect lastFreeGrp field.  Expected %d but found %d\n."),
		    bfSetParams.fragGrps[grpFType].lastFreeGrp,
		    last_page_read);
	    errcnt++;
        }
    }

    fflush( stdout );
}

int
check_group(
    int fd,
    long grpPg,
    slotsPgT *grpPgp,
    int fType,
    int *grpFreeFrags,
    int *grpFragType
    )
{
    int sz, f, frag;
    grpHdrT *grpHdrp;
    slotsPgT fragPgBuf;
    bfFragT fragType;
    fragHdrT *fragHdrp;
    long    fragPg;
    uint32T fragSlot;
    uint32T nextFreeFrag;
    int freeFrags = 0;

    *grpFreeFrags = 0;

    grpHdrp = (grpHdrT *)grpPgp;

    if (grpHdrp->self != grpPg) {
        fprintf( stdout, catgets(catd, 1, 187, "invalid frag group, pg %d\n"),
		grpPg ); 
	errcnt++;
	goto bad_page;
    }

    fragType = grpHdrp->fragType;

    *grpFragType = fragType;

    if ((fType != -1) && (fType != fragType)) {
	goto bad_page;
    }

    if (fragType == 0) {
        *grpFreeFrags = FRAGS_PER_GRP( fragType );
        return 1;
    }

    freeFrags = 0;
    *grpFreeFrags = grpHdrp->freeFrags;

    if (grpHdrp->version == 0) {
        nextFreeFrag = grpHdrp->nextFreeFrag;

        while (nextFreeFrag != BF_FRAG_EOF) {
    
            freeFrags++;
    
            fragPg   = FRAG2PG( nextFreeFrag );
            fragSlot = FRAG2SLOT( nextFreeFrag );
        
            if (read_pg( fd, fragPg, (char *)&fragPgBuf ) != BF_FRAG_PG_BYTES){
                fprintf( stderr, catgets(catd, 1, 188, 
			"read error: fragPg %d, errno %d\n"), fragPg, errno );
		fflush( stderr );
                break;
            }
    
            fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];
            nextFreeFrag = fragHdrp->nextFreeFrag;
    
        }
    } else {

        fragSlot = grpHdrp->freeList[ BF_FRAG_NEXT_FREE ];

        while (fragSlot != BF_FRAG_NULL) {
            if (freeFrags > BF_FRAG_GRP_SLOTS) {
                /*
                 * Must be a loop...
                 */
                fprintf(stdout, catgets(catd, 1, FRAGLOOP,
                        "Loop found in list of free frags on page %d of the frags file.\nFrags metadata file is corrupted!\n\n"), grpPg);
                fprintf(stdout, catgets(catd, 1, BADFILESET,
                        "Fileset is corrupted!  Attempts to use this fileset may result in\nsystem crashes or user data loss!\n\n"));
                errcnt++;
                return (CORRUPTED_FILESET);
            }

            freeFrags++;
            fragSlot = grpHdrp->freeList[ fragSlot ];
        }
    }
    if (freeFrags == grpHdrp->freeFrags) {
        return 1;
    }

    fprintf( stdout, catgets(catd, 1, 189, 
            "\n--\ngroup pg = %6d, type = %d\n"), grpPg, fragType );
    fprintf( stdout, catgets(catd, 1, 190, 
            "      nextFree = %5d, lastFree = %5d, numFree = %5d\n\n "),
        grpHdrp->nextFreeFrag, grpHdrp->lastFreeFrag,
        grpHdrp->freeFrags);

    fprintf( stdout, catgets(catd, 1, 191, "\n** inconsistency found **\n") );
    fprintf( stdout, catgets(catd, 1, 192, 
            "** group header indicates %d free frags"), grpHdrp->freeFrags );
    fprintf( stdout, catgets(catd, 1, 193, 
            ", free list has %d free frag(s)\n"), freeFrags );
    if (grpHdrp->lastFreeFrag == BF_FRAG_BAD_GROUP) {
        fprintf(stdout, catgets(catd, 1, BADFRAG0, 
        "The kernel has noted this inconsistency and has removed the frag group from use.\n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG1, 
            "NOTE: This inconsistency in itself is NOT critical and AdvFS can\n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG2, 
            "      work around it.  It does not indicate potential loss of\n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG3, 
            "      data or metadata, nor will it cause a system crash.\n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG4, 
            "      If no errors of other types are reported on this domain, \n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG5, 
            "      it is not necessary to take corrective action.  The domain\n"));
        fprintf( stdout, catgets(catd, 1, BADFRAG6, 
            "      may continue to be used safely.\n"));
    }
    else {
        errcnt++;
    }
    fflush( stdout );

    if (grpHdrp->version == 1) {
	goto bad_page;
    }

    fprintf( stdout, catgets(catd, 1, 194, 
            "** searching for additional free frags...\n") );

    freeFrags = 0;

    for (f = 0; f < FRAGS_PER_GRP( fragType ); f++) {
        frag     = grpPg * 8 + f * fragType + 1;
        fragPg   = FRAG2PG( frag );
        fragSlot = FRAG2SLOT(  frag );

        if (read_pg( fd, fragPg, (char *)&fragPgBuf ) != BF_FRAG_PG_BYTES) {
            fprintf( stderr, catgets(catd, 1, 188, 
                    "read error: fragPg %d, errno %d\n"), fragPg, errno );
	    fflush( stderr );
	    verify_exit( 1 );
        }

        fragHdrp = (fragHdrT *)&fragPgBuf.slots[ fragSlot ];

        if ((fragHdrp->self == frag) && (fragHdrp->fragType == fragType)) {
            freeFrags++;
            fprintf( stdout, catgets(catd, 1, 195, 
                    "\t** possible free frag: %5d, nextFree = %5d\n"),
		    frag, fragHdrp->nextFreeFrag );
            fprintf( stdout, catgets(catd, 1, 196, 
                    "\t\tfrag = %5d, fragPg = %d, fragSlot = %d\n"), 
		    frag, fragPg, fragSlot );
	    errcnt++;
        }
    }

    fprintf( stdout, catgets(catd, 1, 197, 
            "** search found %d possible free frag(s)\n"), freeFrags );
    errcnt++;
    fflush( stdout );

    return 2;
bad_page:
    bad_frags[num_bad_frags].page = grpPg;
    bad_frags[num_bad_frags].re_inited = 0;
    num_bad_frags++;
	
    fflush( stderr );
    return (0);
}

/*
 * check the directories and file and bmt for each fileset
 * for consistency. Also check each file's frag for
 * consistency.
 */
statusT
check_dirs_and_files()
{
    int i, len, len1, sts;
    char fragBf[MAXPATHLEN];
    char file[MAXPATHLEN];
    char *funcName = "check_dirs_and_files";

    for (i = 0; i < numSets; i++) {
	/* create the frag file path and check frag file */

	strcpy(fragBf, mount_point[i]);
	strcat(fragBf, "/.tags/1\0");

	fprintf( stdout, catgets(catd, 1, 198, "  +++++ Fileset %s +++++\n\n"),
		mounted[i].setName);
 	sts = check_frag_file(fragBf, i);
        if (sts == CORRUPTED_FILESET) {
            continue;
        }

	/* check the dirs and files */
	/* malloc a page for frag hdrs */
	fragpg = malloc(8192);
	if (fragpg == NULL) {
	    fprintf( stderr, catgets(catd, 1, 199, 
                    "%s: can't allocate memory\n"),funcName);
	    fflush( stderr );
	    fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		    "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		    8192, memory_usage);
	   /* do something other than return here */
	    verify_exit( 1 );
	}
	memory_usage += 8192;
    	strcpy(file, mount_point[i]);
	
 	check_all_files(file, i);
	/* close the fileset's frag file */
	close(mounted[i].fd);
    }
    fflush( stdout );
    return EOK;
}

void
find_lost_files(
    char *mountName,
    int which_fileset
    )
{
    int idx, cnt = 0;
    inodeT *curEnt;
    int madeLostAndFound = 0;
    char lf[ MAXPATHLEN ]; 
    char lostFile[ MAXPATHLEN ]; 
    char lostFileLink[ MAXPATHLEN ]; 
    struct stat stats;
    int tag_fd;
    mlStatusT sts;
    setHdrT *setHdr;
    int cloneFileset = FALSE;
    int lostFiles = 0;
    int badLinkFiles = 0;

    /*
     * See if this is a clone fileset.
     */
    setHdr = find_set_tag(mounted[which_fileset].bfsetid.dirTag);
    if ((setHdr) && (setHdr->cloneFlag == 1)) {
        cloneFileset = TRUE;
    }

    /* use the frag file fd if needed */
    tag_fd = mounted[which_fileset].fd;
    for (idx = 0; idx < INODE_TBL_SZ; idx++) {
        curEnt = InodeTbl[ idx ];

        while (curEnt != NULL) {
	    cnt++;
            if (verbose) {
                fprintf( stdout,catgets(catd, 1, 200, 
			"ino = %8d, dirLinks = %4u, tagLinks = %4u\n"), 
			curEnt->ino, curEnt->dirLinkCnt, curEnt->tagLinkCnt );

            } else if (!quiet) {
                if ((cnt % 100) == 0) {
                    fprintf( stdout, "%8d\r", cnt );
                    fflush( stdout );
                }
            }

	    /*
	     * Active mount points will have a dirLinkCnt of 0 and
	     * a tagLinkCnt of 1.  We do not want to consider this
	     * a lost file.  Special check for the root directory,
             * which will always have a tag of 2.
	     */

	    /*
	     * On an active domain we could get hundreds if not thousands
	     * of these.  As files can be created and deleted on the fly.
	     * So instead of printing hundreds of these messages, print
	     * ONE (1) warning message at the end saying how many were
	     * found.  The user can then decided if they wish to run verify
	     * in multiuser mode.
	     *
	     * As we can't be both 'active' and 'lost' (or 'del') no need
	     * to block those messages.
	     */

            if (curEnt->dirLinkCnt == 0 && curEnt->ino != 2) {

                /* TODO : create a real directory entry and fix link count */

	        if ((!active) || (verbose)) {
		    /*
		     * Another error which is fairly common when 
		     * running on an active domain.
		     */

		    fprintf( stdout, "\n" );
		    fprintf( stdout, catgets(catd, 1, 201, 
			    "Found possible lost file: ino/tag = %d\n"), 
			    curEnt->ino );
		}
		errcnt++;
                /*
                 * Don't attempt any repairs on clone filesets.
                 * They are read-only.
                 */
                if (!cloneFileset) {
		    lostFiles++;
                    if (!madeLostAndFound) {
                        sprintf( lf, catgets(catd, 1, 202, "%s/lost+found"), 
				mountName );

                        if ((lstat( lf, &stats ) < 0) && (errno == ENOENT)) {
                            fprintf( stdout, catgets(catd, 1, 203, 
                                    "Creating %s\n"), lf );
       
                            if ((mkdir( lf, 0500 ) < 0) && (errno != EEXIST)) {
                                fprintf( stderr, catgets(catd, 1, 204, 
					"%s: unable to create %s; [%d] %s\n"),
					Prog, lf, errno, BSERRMSG( errno ) );
				fflush( stderr );
                            }
                        }
                        madeLostAndFound = 1;
                    }
                    if (del) {
                        /*
                         * delete the bitfile rather than creating a link
                         */
			sprintf( lostFile, "%s/.tags/%d", mountName, 
				 curEnt->ino );
                        fprintf(stdout, catgets(catd, 1, 205, 
                                "Deleting file %s\n"), lostFile);
                        sts = advfs_remove_bf(curEnt->ino, tag_fd, 0);
                        if (sts != EOK) {
                            if (sts == I_FILE_EXISTS) {
                                fprintf(stderr, catgets(catd, 1, 206, 
                                        "%s: File %s not deleted, exists\n"),
                                        Prog, lostFile);
                            } else {
                                fprintf(stderr, catgets(catd, 1, 207, 
                                        "%s: File %s not deleted %s\n"),
                                        Prog, lostFile, BSERRMSG(sts));
                            } 
			    fflush( stderr );
                         }
		    } else {
		        if (lost) {
			    sprintf( lostFile, "%s/.tags/%d", mountName, 
				    curEnt->ino );
        	            sprintf( lostFileLink, catgets(catd, 1, 209,
                                    "%s/lost+found/file_%08d"),
				    mountName, curEnt->ino );
    
			    /*
			     * Instead of a symlink, create
			     * a hardlink.  This should work UNLESS the file 
			     * being linked is a directory.  If the link call
			     * fails because it is a directory then create a 
			     * symlink instead.
			     */
			    fprintf( stdout, catgets(catd, 1, HARDLINK, 
				    "Creating hardlink %s @-> %s\n"), 
				    lostFileLink, lostFile );
    
			    /*
			     * Check to see if link already exists.
			     */
			    if (stat( lostFileLink, &stats ) == 0) {
			        fprintf( stdout, catgets(catd, 1, LINK_EXISTS, 
					"Link %s @-> %s already exists\n"), 
					lostFileLink, lostFile );
			    } 
			    else if (link( lostFile, lostFileLink ) < 0) {
			        if (errno == EPERM) {
				    /*
				     * Make links use relative path
				     * instead of abosolute path.  The link 
				     * is created in $(mounpoint)/lost+found,
				     * so the relative path should be 
				     * ../.tags/###
				     */

				    sprintf( lostFile, "../.tags/%d", 
					    curEnt->ino );

				    fprintf(stdout, catgets(catd, 1, LINK_DIR,
					    "Unable to hardlink a directory.\n"));					   
				    fprintf( stdout, catgets(catd, 1, 210, 
					    "Creating symlink %s @-> %s\n"), 
					    lostFileLink, lostFile );
				    if (symlink(lostFile, lostFileLink) < 0) {
				        fprintf( stderr, catgets(catd, 1, 212, 
						"%s: unable to create %s @-> %s; [%d] %s\n"),
						Prog, lostFileLink, lostFile, 
						errno, BSERRMSG( errno ) );
				    }
				} 
				else {
				    fprintf( stderr, catgets(catd, 1, 212, 
					    "%s: unable to create %s @-> %s; [%d] %s\n"),
					    Prog, lostFileLink, lostFile, 
					    errno, BSERRMSG( errno ) );
				}
			    }
			}
		    }
		}
            } else if (curEnt->dirLinkCnt == 0) {
		/* TODO : This most likely is an active mount point  */
		/*        do we want to handle this case             */

	    } else if (curEnt->dirLinkCnt != curEnt->realLinkCnt) {
	        badLinkFiles++;
		errcnt++;
		if ((!active) || (verbose)) {
		    /*
		     * Another error which is fairly common when 
		     * running on an active domain.
		     */

		    fprintf( stdout, catgets(catd, 1, 213, 
			    "%s: bad link count for ino/tag %d\n"),
			    Prog, curEnt->ino );
		    fprintf( stdout, catgets(catd, 1, 214, 
			    "%s: link count = %u, expected %d\n"),
			    Prog, curEnt->dirLinkCnt, curEnt->realLinkCnt );
		}
            }

            curEnt = curEnt->nxt;
        }
    }

    fprintf( stdout, "\n" );

    if (0 != lostFiles) {
        fprintf(stdout, catgets(catd, 1, 298, 
		"Found %d lost files out of %d checked.\n"), lostFiles, cnt);
    }

    if (0 != badLinkFiles) {
        fprintf(stdout, catgets(catd, 1, 299,
		"Found %d files with incorrect link count out of %d checked.\n"), 
		badLinkFiles, cnt);
    }

    if ((active) && (lostFiles || badLinkFiles)){
        fprintf(stdout, catgets (catd, 1, ACTIVE_WARNING_0,
		"Most likely this is from file activity on the active domain.\n\n"));
    }

    if ((0 == lostFiles) && (0 == badLinkFiles)) {
        fprintf(stdout, catgets(catd, 1, 300, 
		"Searched %d tags, no lost files found.\n\n"), cnt);
    }

    fflush( stdout );
    close(tag_fd);
}

/*
 * inode_tbl_dir_link
 */

void
inode_tbl_dir_link(
    ino_t ino
    )
{
    int idx = -1;
    inodeT *curEnt = NULL, *newEnt = NULL;

    idx = INODE_TBL_HASH( ino );

    /* Search for inode in table */

    curEnt = InodeTbl[ idx ];

    while (curEnt != NULL) {
        if (curEnt->ino == ino) {

            /* the inode already exists; just increment the link count */
            curEnt->dirLinkCnt++;
            return;
        }

        curEnt = curEnt->nxt;
    }

    /* Inode was not found so we add it to the table */

    newEnt = (inodeT *) malloc( sizeof( inodeT ) );
    if (newEnt == NULL) {
        fprintf( stderr, catgets(catd, 1, 215, 
                "%s: out of memory in inode_tbl_dir_link()\n"), Prog );
	fprintf(stderr, catgets(catd,1,MEMORY_USED, 
		"Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		sizeof( inodeT ), memory_usage);
	fflush( stderr );
        verify_exit( 1 );
    }
    memory_usage += sizeof( inodeT );

    newEnt->ino = ino;
    newEnt->dirLinkCnt = 1;
    newEnt->tagLinkCnt = 0;

    /* Add the entry to the linked list associated with it's hash bucket */

    newEnt->nxt = InodeTbl[ idx ];
    InodeTbl[ idx ] = newEnt;
}

/*
 * inode_tbl_tag_link
 */

void
inode_tbl_tag_link(
    ino_t ino,
    char *mountName
    )
{
    int idx = -1;
    inodeT *curEnt = NULL, *newEnt = NULL;
    char tagName[ MAXPATHLEN ]; 
    struct stat tagStats;

    idx = INODE_TBL_HASH( ino );

    /* Search for inode in table */

    curEnt = InodeTbl[ idx ];

    while (curEnt != NULL) {
        if (curEnt->ino == ino) {

            /* the inode already exists; just increment the link count */

            curEnt->tagLinkCnt++;

            sprintf( tagName, "%s/.tags/%d", mountName, ino );

            if (lstat( tagName, &tagStats ) < 0) {
                fprintf( stderr, catgets(catd, 1, 216, 
                        "%s: can't get stats for %s; [%d] %s\n"),
			Prog, tagName, errno, BSERRMSG( errno ) );
		fflush( stderr );
            } else {
                curEnt->realLinkCnt = tagStats.st_nlink;
            }

            return;
        }

        curEnt = curEnt->nxt;
    }

    /* Inode was not found so we add it to the table */

    newEnt = (inodeT *) malloc( sizeof( inodeT ) );

    if (newEnt == NULL) {
        fprintf( stderr, catgets(catd, 1, 217, 
		"%s: out of memory in inode_tbl_tag_link()\n"), Prog );
	fprintf(stderr, catgets(catd,1,MEMORY_USED, "Unable to malloc an additional %ld bytes, currently using %ld\n"), 
		sizeof( inodeT ), memory_usage);
	fflush( stderr );
        verify_exit( 1 );
    }
    memory_usage += sizeof( inodeT );

    newEnt->ino = ino;
    newEnt->dirLinkCnt = 0;
    newEnt->tagLinkCnt = 1;

    /* Add the entry to the linked list associated with it's hash bucket */

    newEnt->nxt = InodeTbl[ idx ];
    InodeTbl[ idx ] = newEnt;

    sprintf( tagName, "%s/.tags/%d", mountName, ino );

    if (lstat( tagName, &tagStats ) < 0) {
        fprintf( stderr, catgets(catd, 1, 216, 
                "%s: can't get stats for %s; [%d] %s\n"),
		Prog, tagName, errno, BSERRMSG( errno ) );
	fflush( stderr );
    } else {
        newEnt->realLinkCnt = tagStats.st_nlink;
    }
}

void
scan_fileset( 
    char *pathName, 
    int which_fileset,
    int *cnt
    )
{
    void *dp;
    struct dirent *dirEntry;
    struct stat dirStats, stats;
    int len = strlen( pathName );
    int fileFd, dirFd = -1;
    mlStatusT sts;
    int ret;
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    grpHdrT *fragHdr;
    int slot, i, j;
    xtntMapDescT *xtntmapdesc;
    xtntMapT *xtntmap;
    mlExtentDescT xtnt;
    char corruptpath[MAXPATHLEN],
         fileName[MAXPATHLEN];
    long fragPg;
    char *funcName = "scan_fileset";
    char zeroFilledPage[BF_FRAG_PG_BYTES];

    bzero(zeroFilledPage, BF_FRAG_PG_BYTES);

    (*cnt)++;

    if (lstat( pathName, &dirStats ) < 0) {
        fprintf( stderr, catgets(catd, 1, 218, 
                "%s: error accessing directory <%s>; [%d] %s\n"),
		Prog, pathName, errno, ERRMSG( errno ) );
        fprintf( stderr, "\n" );
	fflush( stderr );
        return;
    }

    if (verbose) {
        fprintf( stdout, catgets(catd, 1, 219, 
                "ino = %8d, size = %10d, mode = 0%06o %s/\n"), 
		dirStats.st_ino, dirStats.st_size, 
		dirStats.st_mode, pathName );

    } else if (!quiet) {
        if ((*cnt % 100) == 0) {
            fprintf( stdout, "%8d\r", *cnt );
            fflush( stdout );
        }
    }

    if (!S_ISDIR( dirStats.st_mode )) {
        fprintf( stdout, catgets(catd, 1, 220, 
                "%s: <%s> not a directory\n"), Prog, pathName );
	fflush( stdout );
        return;
    }

    if (dirStats.st_dev != mountDev) {
        if ((!active) || (verbose)) {
	    /*
	     * This is expected on an active domain.  Not an error, but 
	     * can't call check_dir on it.
	     */
	    fprintf( stderr, catgets(catd, 1, 221, 
		    "%s: warning, encountered a fileset mounted on <%s>\n"),
		    Prog, pathName );
	    errcnt++;

	    /* Removed the check in lost files for mountpoints, so the 
	     * following error message is currently not needed.
	      fprintf( stderr, catgets(catd, 1, 222, 
		      "%s: mount points will appear as lost files\n"), Prog );
	     *
	     */
	    fflush( stderr );
	  }
	return;
    }

    /*
     * scan the directory block-wise first for consistency
     */
    if ( (ret = check_dir( pathName)) != EOK) {
        return;
    }

    dirFd = open( pathName, O_RDONLY );
    if (dirFd < 0) {
        fprintf( stderr, catgets(catd, 1, 223, 
                "%s: unable to open directory <%s>; [%d] %s\n"),
		Prog, pathName, errno, ERRMSG( errno ) );
	fflush( stderr );
        return;
    }

    dp = opendir( pathName );
    if (dp == NULL) {
        fprintf( stderr, catgets(catd, 1, 223, 
                "%s: unable to open directory <%s>; [%d] %s\n"),
		Prog, pathName, errno, ERRMSG( errno ) );
	fflush( stderr );
        close( dirFd );
        return;
    }
    
    /* 
     * append a slash (/)  to the path name 
     */
    pathName[len] = '/';
    pathName[len + 1] = '\0';
 
    /* 
     * scan the directory 
     */
 
    while ((dirEntry = readdir( dp )) != NULL) {
 
        if (!strcmp( dirEntry->d_name, "." ) ||
            !strcmp( dirEntry->d_name, "..")) {
            inode_tbl_dir_link( dirStats.st_ino );
            continue;
        }

        /* 
         * append the file name to the path name 
         */
        pathName = strcat( pathName, dirEntry->d_name );

        if (lstat( pathName, &stats ) < 0) {

            if ((errno == ENOENT) || (errno == EINVAL)) {
	        fprintf( stdout, catgets(catd, 1, 224, 
                        "%s: POTENTIAL INCOMPLETE UNLINK <%s>; file does not exist\n"),
			Prog, pathName );
	        errcnt++;

                if (fix) {
                    sts = advfs_remove_name( dirFd, &pathName[ len + 1 ] );

                    if (sts == EOK) {
                        fprintf( stdout, catgets(catd, 1, 225, 
				"%s: unlink completed\n"), Prog );

                    } else if (sts == EEXIST) {
                        fprintf( stdout, catgets(catd, 1, 226, 
				"%s: directory entry verified as okay\n"), 
				Prog );
                        fprintf( stdout, catgets(catd, 1, 227, 
				"%s: ensure that the fileset is inactive when running %s\n"),
				Prog, Prog );

                    } else {
                        fprintf( stdout, catgets(catd, 1, 228, 
				"%s: incomplete unlink not fixed; error = %s\n"),
				Prog, BSERRMSG( sts ) );
                    }

                } else {
                    fprintf( stdout, catgets(catd, 1, 229, 
			    "%s: incomplete unlink not fixed; use '-f' to fix it\n"),
			    Prog );
                }

            } else {
                fprintf( stderr, catgets(catd, 1, 230, 
                        "%s: unable to get info for file <%s>; [%d] %s\n"),
			Prog, pathName, errno, ERRMSG( errno ) );
		fflush( stderr );
            }

            pathName[len + 1] = '\0';            /* chop off the file name */
            continue;
        }

        switch( stats.st_mode & S_IFMT ) {

            case S_IFDIR:
                inode_tbl_dir_link( dirStats.st_ino );
                scan_fileset( pathName, which_fileset, cnt );
                break;

            case S_IFREG:
		fileFd = open( pathName, O_RDONLY);
		if (fileFd < 0) {
		    fprintf( stderr, catgets(catd, 1, 231, 
                            "Can't open %s to check frag\n"), pathName );
		    fflush( stderr );
		} else {
                   /*
                    * Create a file name that the user will recognize, one
                    * that does not start with 
                    * "/etc/fdmns/<domain>/verify_xxxxx".
                    */
                   strcpy(fileName, catgets(catd, 1, 297, "<MOUNT_POINT>"));
                   strcat(fileName, &pathName[strlen(mount_point[which_fileset])]);
		    sts = advfs_get_bf_params(fileFd, &bfAttr,
			&bfInfo);
                    if (sts != EOK) {
                        fprintf (stderr, catgets(catd, 1, 18, 
                                 "%s: can't get '%s' bitfile parameters\n"),
                                funcName, fileName);
                        fprintf (stderr, catgets(catd, 1, 4,
				 "    error: %d, %s\n"), sts, BSERRMSG (sts));
                    }
                    else if (bfInfo.fragId.type != BF_FRAG_ANY) {
			if (verbose) {
		    	    fprintf( stdout, catgets(catd, 1, 232, 
                                    "file %s fragId %d type %d\n"), pathName,
				    bfInfo.fragId.frag, bfInfo.fragId.type);
			}
		    	/* read the corresponding frag group header */
                        fragPg = (long)FRAG2GRP(bfInfo.fragId.frag);
		    	sts = read_pg(
			    mounted[which_fileset].fd,
                            fragPg,
			    (char *)fragpg);
                
                        if (sts != BF_FRAG_PG_BYTES) {
                            fprintf(stdout, catgets(catd, 1, 296, 
				    "Cannot read fileset frag file page %d to cross check frag information\nfor file %s.\nEither %s \nor the fileset frag file may be corrupted.\n"), 
				    fragPg, fileName, fileName);
			    errcnt++;
                        }
                        else {
                            fragHdr = (grpHdrT *)fragpg;
			    /* check for version 1 of frag file */
			    if (fragHdr->version == 1) {
		  	        /* check the group header for same type */
    			        if (fragHdr->fragType != bfInfo.fragId.type) {
			            /* the frag group header doesn't match. 
                                    /* check to see if this header is on the 
                                    /* damaged list */

				    fprintf(stdout, catgets(catd, 1, 233, 
					    "The fileset frag file group header type, %d, does not match the frag type that\nfile %s expected, %d\n.  Either %s or\nthe fileset frag file may be corrupted."), 
					    fragHdr->fragType, fileName, 
					    bfInfo.fragId.type, fileName);
				    errcnt++;
			        }
			        /* run the list looking for the slot 
                                /* (it shouldn't be there). freeList[0] points 
                                /* to the first free slot */
			        slot = FRAG2GRPSLOT(bfInfo.fragId.frag);
    			        i = 0;
			        j = 0;
		   	        while(i < fragHdr->freeFrags) {
			    	    if (fragHdr->freeList[j] == slot) {
					fprintf(stdout, catgets(catd, 1, 234, 
                                                "The frag in slot %d of page %d is on the free list of frags but the file\nwith tag %d is using it!  This may result in loss of user data for this file.\n"), 
						slot, fragPg, bfInfo.tag.num);
					errcnt++;
				        break;
			    	    }
				    j = fragHdr->freeList[j];
				    i++;
			        }
			    }
                            else {
                                /*
                                 * If the frag file page is zero-filled,
                                 * we're probably looking at a hole in
                                 * the frag file.
                                 */
                                if (!bcmp(fragpg,zeroFilledPage,
                                          BF_FRAG_PG_BYTES)) {
                                    fprintf(stdout, catgets(catd, 1,
                                            ZEROPAGE,
                                            "Found zero-filled fileset frag file page %d while cross-checking\nfrag information for file %s.\nEither %s \nor the fileset frag file may be corrupted.\n"),
                                            fragPg, fileName, fileName);
                                    errcnt++;
                                }
                            }
                        }

                        /*
                         * Now check to see if frag overlaps with an extent.
                         */
                        xtntmapdesc = advfs_xtntmap_open(fileFd, &sts, 1);
                        if (xtntmapdesc == NULL) {
                            fprintf( stderr, catgets(catd, 1, 185, 
                                    "advfs_xtntmap_open() error; %s\n"), 
				    BSERRMSG( sts ) );
                        }
                        else {
                            xtntmap = xtntmapdesc->nextMap;
                            for (i = 0; i < xtntmap->xtntCnt; i++) {
                                xtnt = xtntmap->xtnts[i];
                                if (xtnt.bfPageCnt == 0) {
                                    break;
                                }
                                else if (xtnt.bfPage + xtnt.bfPageCnt - 1 >= 
                                         bfInfo.fragOff) {

                                    errcnt++;
                                    sprintf(corruptpath, "<mount point>");
                                    strcat(corruptpath, 
                                    &(pathName[strlen(mount_point[which_fileset])]));
				    fprintf(stdout, catgets(catd, 1, 265, 
                                            "Overlapping frag data corruption detected in:\n\tFile: %s\n\tPage: %d\n"), 
					    corruptpath, bfInfo.fragOff);
				    errcnt++;

                                    if (fix) {
                                        sts = fix_overlapping_frag(fileFd,
                                                               pathName,
                                                               strlen(mount_point[which_fileset]),
                                                               &xtnt,
                                                               bfInfo.fragOff);
                                        if (!sts) {
					    errcnt++;
                                            fprintf(stdout, catgets(catd, 1, 
266, "Temporary files created representing the two versions of\n"));
                                            fprintf(stdout, catgets(catd, 1, 
267, "page %d of file '%s'.  The temporary file with the .frag\n" ), 
						    bfInfo.fragOff, 
						    corruptpath);
                                            fprintf(stdout, catgets(catd, 1, 
CONT_267B, "extension contains the readable page; the file with the\n"));
                                            fprintf(stdout, catgets(catd, 1,
CONT_267C, ".ext extension contains the hidden page.\n" ));
                                            fprintf(stdout, catgets(catd, 1, 
268, "Refer to the AdvFS documentation for a description\n")); 
                                            fprintf(stdout, catgets(catd, 1, 
270, "of how to use these temporary files to recover\n"));
                                            fprintf(stdout, catgets(catd, 1, 
271, "from this overlapping frag corruption problem.\n"));
                                        }
                                        else {
                                            fprintf(stdout, catgets(catd, 1, 272, 
						    "Could not enable recovery of %s\n"), corruptpath);
                                        }
                                    }
                                    else {
                                        fprintf(stdout, catgets(catd, 1, 273, 
						"Run verify -f on this domain to enable recovery of this data.\n"));
					errcnt++;
                                    }
                                    break;
                                }
                            }
                            advfs_xtntmap_close(xtntmapdesc);
                        }
		    }
		    close(fileFd);
		}
            case S_IFLNK:
            case S_IFIFO:
            case S_IFCHR:
            case S_IFBLK:
            case S_IFSOCK:

                inode_tbl_dir_link( stats.st_ino );

                if (verbose) {
                    fprintf( stdout, catgets(catd, 1, 235, 
			     "ino = %8d, size = %10d, mode = 0%06o %s\n"), 
                             stats.st_ino, stats.st_size, 
                             stats.st_mode, pathName );
                } 

                break;

            default:
                fprintf( stdout, catgets(catd, 1, 236, 
			"%s: <%s> unknown file type\n"), Prog, pathName );
		errcnt++;
		fflush( stdout );
        } /* end switch */
        
        pathName[len + 1] = '\0';               /* chop off the file name */
    }
 
    pathName[len] = '\0';                       /* remove slash "/"        */
 
    closedir( dp );
    close( dirFd );
    fflush( stdout );
}


int
check_dir(
         char *pathName
         )
{
    int fd, n, bytes;
    statusT ret;
    char buffer[8192];
    int page = 0;
    char *p;
    fs_dir_entry *dir_p, *last_p;
    unsigned int last_entry_offset;
    dirRec *dirRecp;
    bfTagT *tagp;

    if ((fd = open (pathName, O_RDONLY)) < 0) {
        fprintf( stderr, catgets(catd, 1, 237, 
	        "Could not open dir %s in check_dir\n"), pathName);
	fflush( stderr );
        return (-1);
    }

    while (( bytes = read( fd, buffer, 8192)) == 8192) {
        dir_p = (fs_dir_entry *)buffer;
        n = 8192 - sizeof(dirRec);
        dirRecp = (dirRec *)((char *)buffer + n);
        last_entry_offset = dirRecp->lastEntry_offset;

        if ( !last_entry_offset || last_entry_offset >= (unsigned)n) {
            fprintf( stdout, catgets(catd, 1, 238, 
                    "Error in dirRec on page %d; %s is corrupted\n"), 
		    page, pathName);
	    errcnt++;
            goto _error;
        }
        last_p = (fs_dir_entry *)((char *)dir_p + last_entry_offset);

        while (dir_p <= last_p) {

            /* check for 0 size field */
            if (dir_p->fs_dir_header.fs_dir_size == 0) {

                fprintf( stdout, catgets(catd, 1, 239, 
			"Found zero entry size on page %d; %s is corrupted\n"),
			page, pathName);
		errcnt++;
                goto _error;
            }

            /* check for a bigger than normal size */
            if (dir_p->fs_dir_header.fs_dir_size > 512) {
                fprintf( stdout, catgets(catd, 1, 240, 
                        "Found entry size too large on page %d; %s is corrupted\n"),
			page, pathName);
		errcnt++;
                goto _error;
            }

            /* if not a deleted entry, check tags match */
            if (dir_p->fs_dir_header.fs_dir_bs_tag_num != 0) {
                tagp = GETTAGP(dir_p);
                if (tagp->num != dir_p->fs_dir_header.fs_dir_bs_tag_num) {
                    fprintf( stdout, catgets(catd, 1, 241, 
                            "Found bad tag match on page %d; %s is corrupted\n"),
			    page, pathName);
		    errcnt++;
                    goto _error;
                }
            }

            /* incr to the next on the page */
            n = (signed)dir_p->fs_dir_header.fs_dir_size;
            p = (char *) dir_p;
            dir_p = (fs_dir_entry *)(p + n);
        } /* end of while for this page */

        page++;
    } /* end of while reading pages */

    close(fd);
    return (EOK);

_error:
    errcnt++;
    close(fd);
    fflush( stdout );
    return(-1);
}


int
scan_tags(
    char *mountName,
    mlBfSetIdT setId
    )
{
    mlBfTagT tag = { 0 };
    int sts;
    mlStatusT ret;
    uid_t uid;
    gid_t gid;
    off_t size;
    time_t atime;
    mode_t mode;
    int cnt = 0;

    while ((ret = advfs_tag_stat( &tag, setId, &uid, &gid, 
                                  &size, &atime, &mode)) == EOK) {

        if ((tag.num < 0) || (tag.num == 1)) {
            /* ignore reserved and frag bf tags */
            continue;
        }

	cnt++;

        if (verbose) {
            fprintf( stdout, catgets(catd, 1, 242, 
		    "ino = %8d, size = %10d, mode = 0%06o\n"), 
		    tag.num, size, mode );

        } else if (!quiet) {
            if ((cnt % 100) == 0) {
                fprintf( stdout, "%8d\r", cnt );
                fflush( stdout );
            }
        }

        inode_tbl_tag_link( tag.num, mountName );
    }
    if ((ret == EBAD_PARAMS) || (ret == E_BAD_VERSION) || 
	(ret == E_NO_SUCH_BF_SET)) {
        fprintf( stderr, catgets(catd, 1, 243, 
		"%s: error from advfs_tag_stat [%d] \n"), Prog, ret);
	fflush( stderr );
        return EBAD_PARAMS;
    }

    fprintf( stdout, "\n" );

    fprintf(stdout, catgets(catd, 1, 301, 
	    "Scanned a total of %d tags.\n\n"), cnt);
    fflush( stdout );

    return EOK;
}


int
check_all_files(
    char *fsName,
    int which_fileset 
    )
{
    int ret;
    struct statfs fsStats;
    struct stat rootStats;
    int cnt = 0, i;
    mlBfSetIdT fsId;
    char *p;

    fprintf( stdout, catgets(catd, 1, 245, 
	    "Scanning directories and files ...\n") );

    cnt = 1;

    for (i = 0; i < INODE_TBL_SZ; i++) {
	InodeTbl[i] = NULL;
    }

    /*
     * We do not want this to cross mount points so we set the variable
     * mountDev here.
     */
    if (lstat( fsName, &rootStats ) < 0) {
        fprintf( stderr, catgets(catd, 1, 218, 
                "%s: error accessing directory <%s>; [%d] %s\n"),
		Prog, fsName, errno, ERRMSG( errno ) );
        fprintf( stderr, "\n" );
	fflush( stderr );
        return 1;
    }

    mountDev = rootStats.st_dev;

    scan_fileset( fsName, which_fileset, &cnt );
    fprintf( stdout, "\n" );

    fprintf( stdout, catgets(catd, 1, 302, 
            "Scanned %d directories.\n\n"), cnt);

    fprintf( stdout, catgets(catd, 1, 246, "Scanning tags ...\n") );

    ret = scan_tags( fsName, mounted[which_fileset].bfsetid );
    if (ret != EOK) {
        return ret;
    }

    fprintf( stdout, catgets(catd, 1, 247, "Searching for lost files ...\n") );

    find_lost_files( fsName, which_fileset );

    fflush( stdout );
    return 0;
}


/*
 * This function attempts to give the user the ability to recover
 * from the overlapping frag data corruption problem.
 * This function creates two temporary files.  The first contains
 * the contents of page 'fragpage' as given by the extent map.
 * The second contains the contents of page 'fragpage' as given
 * by the overlapping frag.  Both files are placed into the same
 * directory as the corrupted file.  The filenames given to the
 * temp files are <corrupted_file_name>.page_<overlap_page>.ext
 * and <corrupted_file_name>.page_<overlap_page>.frag, respectively.
 * It is up to the user to use these two temp files and the original
 * corrupted file to reconstruct a good version of the corrupted file.
 */
int
fix_overlapping_frag (
    int filefd,                      /* Fd of corrupted file */
    char *filename,                  /* Name of corrupted file */
    int mountpathlength,             /* Length of mount path */
    mlExtentDescT *overlappingextent,/* Extent which overlaps frag */
    uint32T fragpage                 /* Page number in corrupted file of frag */
    )
{
    int sts = 0,                      /* Return status */
        rawfd,                        /* Fd for raw device for volume */
        extfilefd,                    /* Fd for temp file for extent map page */
        fragfilefd,                   /* Fd for temp file for frag file page */
        raw_opened = FALSE,           /* TRUE if raw device was opened */
        extfile_created = FALSE,      /* TRUE if .ext file was created */
        fragfile_created = FALSE;     /* TRUE if .frag file was created */
    long rawpage;                     /* Page to read from raw device */
    mlVolInfoT volInfo;               /* Required by advfs_get_vol_params() */
    mlVolPropertiesT volProperties;   /* Contains volume's device name */
    mlVolIoQParamsT volIoQParams;     /* Required by advfs_get_vol_params() */
    mlVolCountersT volCounters;       /* Required by advfs_get_vol_params() */
    char rawdevice[MAXPATHLEN],       /* Name of raw device for volume */
         extfile[MAXPATHLEN],         /* Filename for .ext file */
         exttrunc[MAXPATHLEN],        /* Truncated name for .ext file */
         fragfile[MAXPATHLEN],        /* Filename for .frag file */
         fragtrunc[MAXPATHLEN],       /* Truncated name for .frag file */
         corrtrunc[MAXPATHLEN],       /* Truncated name for corrupted file */
         *rawd,                       /* Returned by fsl_rawname() */
         buf[PAGESIZE];               /* Read/write page buffer */
    struct stat statbuf;              /* For stat() call */

    /*
     * Get the name of the device for the virtual disk
     * containing the overlapped page.
     */
    if (sts = advfs_get_vol_params(dmnParams.bfDomainId,
                                   overlappingextent->volIndex,
                                   &volInfo,
                                   &volProperties,
                                   &volIoQParams,
                                   &volCounters)) {
        fprintf(stderr, catgets(catd, 1, 274,
                "Could not get params for vol %d: sts = %d\n"), 
                overlappingextent->volIndex, sts);
        goto _error;
    }

    /*
     * Open the raw device corresponding to the volume on
     * which the overlapping page resides.
     */
    rawd = fsl_to_raw_name(volProperties.volName, rawdevice, MAXPATHLEN);
    if (!rawd) {
        fprintf(stderr, catgets(catd, 1, 275,
                "Could not get raw device name for %s\n"), 
                volProperties.volName);
        sts = EINVAL;
        goto _error;
    }

    rawfd = open(rawdevice, O_RDONLY, 0);
    if (rawfd < 0) {
        fprintf(stderr, catgets(catd, 1, 276, "Could not open %s\n"), 
                rawdevice);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
                errno, sys_errlist[errno]);
        sts = rawfd;
        goto _error;
    }
    raw_opened = TRUE;

    /*
     * Create the temp file for the overlapping page as
     * seen from the extent map.
     */
    if (strlen(filename) > MAXPATHLEN - 20) {
        strncpy(extfile, filename, MAXPATHLEN - 20);
        sprintf(extfile, "%s.page_%d.%s", extfile,fragpage,"ext");
    }
    else {
        sprintf(extfile, "%s.page_%d.%s", filename,fragpage,"ext");
    }
    sprintf(exttrunc, "<mount_point>%s", &extfile[mountpathlength]);
    sts = stat(extfile, &statbuf);
    if (sts == 0) {
        fprintf(stderr, catgets(catd, 1, 277, "%s already exists.\n"), 
                exttrunc);
        fprintf(stderr, catgets(catd, 1, 278, 
                "Remove or rename this file and run verify -f again.\n"));
        sts = EEXIST;
        goto _error;
    }
    else if (errno != ENOENT) {
        fprintf(stderr, catgets(catd, 1, 279, "Cannot stat %s\n"), exttrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), 
                errno, sys_errlist[errno]);
        goto _error;
    }
    extfilefd = open(extfile, O_CREAT|O_EXCL|O_WRONLY, 0600);
    if (extfilefd < 0) {
        fprintf(stderr, catgets(catd, 1, 280, "Could not create %s\n"), 
                exttrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        sts = extfilefd;
        goto _error;
    }
    extfile_created = TRUE;

    /*
     * Create the temp file for the overlapping page as
     * seen from the overlapping frag.
     */
    if (strlen(filename) > MAXPATHLEN - 20) {
        strncpy(fragfile, filename, MAXPATHLEN - 20);
        sprintf(fragfile, "%s.page_%d.%s", fragfile,fragpage,"ext");
    }
    else {
        sprintf(fragfile, "%s.page_%d.%s", filename,fragpage,"frag");
    }
    sprintf(fragtrunc, "<mount_point>%s", &fragfile[mountpathlength]);
    sts = stat(fragfile, &statbuf);
    if (sts == 0) {
        fprintf(stderr, catgets(catd, 1, 277, "%s already exists.\n"), 
                fragtrunc);
        fprintf(stderr, catgets(catd, 1, 278, 
                "Remove or rename this file and run verify -f again.\n"));
        sts = EEXIST;
        goto _error;
    }
    else if (errno != ENOENT) {
        fprintf(stderr, catgets(catd, 1, 279, "Cannot stat %s\n"), fragtrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        goto _error;
    }
    fragfilefd = open(fragfile, O_CREAT|O_EXCL|O_WRONLY, 0600);
    if (fragfilefd < 0) {
        fprintf(stderr, catgets(catd, 1, 280, "Could not create %s\n"), 
                fragtrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        sts = fragfilefd;
        goto _error;
    }
    fragfile_created = TRUE;

    /*
     * Read the overlapping page as seen from the extent map page 
     * directly from the raw device.
     */
    rawpage = (overlappingextent->volBlk / (PAGESIZE / BLOCKSIZE)) + 
              (fragpage - overlappingextent->bfPage);
    if ((sts = read_pg(rawfd, rawpage, buf)) != PAGESIZE) {
        fprintf(stderr, catgets(catd, 1, 281, 
		"Could not read page %d of %s\n"),
                fragpage, rawdevice);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        goto _error;
    };

    /*
     * Put this version of the overlapping page into the .ext file.
     */
    if ((sts = write(extfilefd, buf, PAGESIZE)) != PAGESIZE) {
        fprintf(stderr, catgets(catd, 1, 282, "Could not write to %s\n"), 
                exttrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        goto _error;
    }

    /*
     * Read the overlapping page as seen from the frag
     * from the corrupted file.
     */
    if ((sts = read_pg(filefd, (long)fragpage, buf)) != PAGESIZE) {
        sprintf(corrtrunc, "<mount_point>%s", &filename[mountpathlength]);
        fprintf(stderr, catgets(catd, 1, 281, 
		"Could not read page %d of %s\n"),
                fragpage, corrtrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        goto _error;
    };

    /*
     * Put this version of the overlapping page into the .frag file.
     */
    if ((sts = write(fragfilefd, buf, PAGESIZE)) != PAGESIZE) {
        fprintf(stderr, catgets(catd, 1, 282, "Could not write to %s\n"), 
                fragtrunc);
        fprintf(stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"), errno, 
                sys_errlist[errno]);
        goto _error;
    }
    
    sts = 0;

    _error:

    if (raw_opened) {
        close(rawfd);
    }
    if (extfile_created) {
        if (sts)
            unlink(extfile);
        close(extfilefd);
    }
    if (fragfile_created) {
        if (sts)
            unlink(fragfile);
        close(fragfilefd);
    }
    return sts;
}


/*
 * fix_tagdir_hdr
 *
 * Fixes some of the fields in the tag page hdr.
 */

static
int 
fix_tagdir_hdr ( int tagDirFd, /* in */
                 u32T currPage,  /* in */
                 bsTDirPgT *tagDirPage, /* in */
		 int tagDirPageSize /* in */
	       )
{
    char *funcName = "fix_tagdir_hdr";
    int allocTagMapCnt;
    int deadTagMapCnt;
    int freeTagMapCnt;
    bsTDirPgHdrT *hdr;
    bsTMapT *tagMap;
    int i;
    int startI;
    mlBfTagT tag;
    int fixed_error = 0;
    int byteCnt;

    /* tagDirPage has already been loaded no reason to read it again.*/

    /* Local pointer to header */
    hdr = &(tagDirPage->tpPgHdr);

    /* local pointer to tag array */
    tagMap = &(tagDirPage->tMapA[0]);

    /* Fix the currPage in the header if wrong */
    if (hdr->currPage != currPage) {
        hdr->currPage = currPage;
	fixed_error++;
    }

    /* Count number of tags in each state. */
    
    allocTagMapCnt = 0;
    freeTagMapCnt = 0;
    deadTagMapCnt = 0;

    /* Handle page 0, different from other pages. */
    if (currPage != 0) {
        startI = 0;
    } else {
        startI = 1;
    }

    /* Loop through all entries on page */
    for (i = startI; i < BS_TD_TAGS_PG; i++) {
        tag.seq = tagMap[i].tmSeqNo;

        if ((tag.seq & BS_TD_IN_USE) != 0) {
            allocTagMapCnt++;
        } else if (tag.seq == BS_TD_DEAD_SLOT) {
            deadTagMapCnt++;
        } else {
            freeTagMapCnt++;
	}
    }  /* end for */

    /* Fix header for number of allocated TMaps. */
    if (hdr->numAllocTMaps != allocTagMapCnt) {
        hdr->numAllocTMaps = allocTagMapCnt;
	fixed_error++;
    }

    /* Fix header for number of dead TMaps. */
    if (hdr->numDeadTMaps != deadTagMapCnt) {
        hdr->numDeadTMaps = deadTagMapCnt;
	fixed_error++;
    }

    /* Have found an error and modified the page, so write it out */
    if (0 != fixed_error)
    {
        byteCnt = write_pg (tagDirFd, currPage, (char *)tagDirPage);
        if (byteCnt != tagDirPageSize) {
            fprintf ( stderr, catgets(catd, 1, BAD_WRITE, "%s: bad write\n"),
		     funcName);
            if (byteCnt >= 0) {
                fprintf ( stderr, catgets(catd, 1, 22, 
                         "    bytes count - real: %d, expected: %d\n"),
			 byteCnt, tagDirPageSize);
            } else {
                fprintf ( stderr, catgets(catd, 1, 17, "    errno: %d, %s\n"),
			 errno, sys_errlist[errno]);
            }
	    return 1;
        }
    }

    fprintf(stdout, catgets(catd, 1, FIXED_TAGPG_HDR, 
	    "%s: Corrected on-disk error on page %d of the tag file.\n"),
	    Prog, currPage);

    return 0;
}  /* fix_tagdir_hdr */



verify_exit(int sts) 
{
    mlSSDmnOpsT ssDmnCurrentState=0;
    int ssRunningState=0;

    /* reset smartstore back to original setting */
    if(savedDmnCurrentState == SS_UI_ACTIVATE) {
        sts = advfs_ss_dmn_ops (
                            dmnName,
                            savedDmnCurrentState,
                            &ssDmnCurrentState,
                            &ssRunningState );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, SSERROR,
                 "%s: vfast status not available on domain %s; %s\n"),
                 Prog, dmnName, BSERRMSG(sts));
        }
    }

    if (posted_lock) {
        if (sts<0) {
            advfs_post_user_event(EVENT_FSET_BACKUP_ERROR, advfs_event, Prog);
        } else {
            advfs_post_user_event(EVENT_FSET_BACKUP_UNLOCK, advfs_event, Prog);
        }
    }

    exit( sts );
}
