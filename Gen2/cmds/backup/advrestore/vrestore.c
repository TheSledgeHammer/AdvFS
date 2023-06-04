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
 * Abstract:
 *
 *      Implements the advrestore program.
 *
 */

#include <pthread.h>
#include <signal.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <mntent.h>
#include <advfs/fs_quota.h>
#include <ctype.h>
#include <advfs/backup.h>
#include <advfs/tapefmt.h>
#include <advfs/util.h>
#include <advfs/blkpool_util.h>
#include <rblkpool_util.h>
#include <advfs/lzw.h>
#include <advfs/devlist_util.h>
#include <dirmgt.h>
#include <termio.h>
#include <aclv.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/fs_dir.h>
#include <locale.h>
#include "vrestore_msg.h"

nl_catd catd;
Dev_type_t Dev_type = NONE;
extern char *sys_errlist[];

#define MAX_FILE_SPECS 256
#define FQUOTAS "fileset quotas"


typedef enum { R_UNKNOWN, R_CRC_AND_XOR, R_NONE } reliability_t;
   
typedef enum { OW_YES, OW_NO, OW_ASK } overwrite_t;

#define CLOSED -1
#define FILE_OPEN( fd ) ((fd) > CLOSED)

/* 
 * Function variables for device-specific functions 
 */
int (*read_buf)( int fd, char *blk, int cnt ) = NULL;
static int (*reset_save_set)( int fd ) = NULL;
void (*close_device)( int fd ) = NULL;

/* function prototypes */

void
create_dir(
   int dir_fd,               /* in - logical directory handle */
   struct dattr_t *dir_attr, /* in - dir attributes record */
   char *dir_name            /* in - dir name */
   );

void
restore_dir_data ( 
   int dir_fd,            /* in - logical directory handle */
   int record_size,       /* in - Number of bytes in the data record */
   struct blk_t *blk,     /* in - The block containing the data record */
   int data_idx,          /* in - Offset into 'blk' of the data record */
   int compressed_data,   /* in - Save-set compressed data flag */
   lzw_desc_handle_t lzwh /* in - Compression handle */
   );

int
close_file( 
   struct fattr_t *,
   int *fd,               /* in - in/out file descriptor */
   char *file_name,       /* in - name of restored file */
   int compressed,        /* in - flag if compressed */
   int bad_file,          /* in - flag if not complete */
   lzw_desc_handle_t lzwh /* in - lzw desc tbl handle */
   );

int
restore_file_data ( 
   int fd,
   uint16_t record_size,
   uint16_t flags,
   uint64_t d_offset,
   uint64_t *tot_bytes_restored,
   struct blk_t *blk,
   int data_idx,
   int compressed_data,
   lzw_desc_handle_t lzwh
   );

int 
restore_quota_data(
   record_type_t type,
   void *data,
   int qfd,
   bfSetParamsT *oldSetParams,
   uint64_t *file_cnt,
   uint64_t *total_bytes_restored
   );

void
zero_quota_range( 
   int qfd,
   int firstId,
   int lastId,
   int dqsize,
   int quotactl_cmd
   );

void
create_device_file(
   struct dev_attr_t *dev_attr, /* in - dev attributes record */
   char *name,                  /* in - dev name */
   uint64_t *file_cnt,          /* in/out - Number of files restored counter */
   char *last_name		/* out - name created */
   );

void
create_hard_link(
    struct hlink_t *hlink,
    char *fname,
    char *lname,
    uint64_t *file_cnt,
    char *last_name
    );

void
create_sym_link(
    struct slink_t *slink,
    char *lname,
    char *link,
    uint64_t *file_cnt,         /* in/out - Number of files restored counter */
    char *last_name		/* out - name created */
    );

int
create_file( 
   struct fattr_t *,
   int *,
   char *,
   char *,
   uint64_t *,
   int,
   lzw_desc_handle_t
   );

int
create_file_dir(
   struct dattr_t *dir_attr, /* in - dir attributes record */
   char *dir_name,           /* in - dir name */
   char *last_name
   );

void
set_file_dir_attr(
   struct dattr_t *dir_attr,    /* in - dir attributes record */
   adv_bf_attr_t *bfAttr,       /* in - dir's bitfile attributes */
   adv_bf_attr_t *bfIAttr,      /* in - dir's inheritable attributes */
   char *name                   /* in - dir name */
   );

void usage( );

int vrestore_open_device( char *source );

int get_save_set_attr( 
    int s_fd, 
    uint32_t *blk_size,
    int *xor_blks, 
    reliability_t *reliability, 
    int *compressed_data  
    );

void
restore( 
    int xor_blks, 
    reliability_t reliability, 
    int compressed_data,
    recov_blk_pool_handle_t rblkh,
    uint64_t *file_cnt,
    uint64_t *total_bytes_restored
    );

void
list_blk_hdr(
    struct bheader_t *bhdr  ,
    FILE           *std
    );

void
dump_blk_hdr(
    struct bheader_t *bhdr  
    );

void
list_rec_hdr(
    char *msg,              
    struct rheader_t *rhdr  ,
    FILE        *std
    );

void tape_bind( );
void file_bind( );
void pipe_bind( );

void
file_compare_error(
    char         *file_name,		   
    char 	 *file_type,         
    struct stat  stat_buf);

/*
 * Signal handler routines
 */
void prepare_signal_handlers( void );
void* signal_thread( void *arg );
int want_abort( void );
void abort_now( void );

/* 
 * global variables -- for this module (static)
 */

/* these are set according to the command line args passed to restore */
static int 
    File_spec_cnt = 0,
    Tape_number = 1,
    File_spec = FALSE,
    Verbose = FALSE,
    Show = FALSE, /* If true, user just wants to see what's in the saveset */
    List = FALSE;

static int Abort_restore = 0; /* If 1 then user requested to abort */
static int AlreadyPrintedNoQuotas = 0; /* If 1 then already printed this message*/

double
    Version = 0.0;

static overwrite_t 
    Overwrite = OW_YES;

static char 
    *File_specs[MAX_FILE_SPECS],
    *Source = NULL,
    *Target = NULL;

fs_info_t Fs_info;

/* 
 * global variables -- for all modules (not static)
 */

char Tmp_fname[30]; /* used to hold temporary name until file can be renamed */
int Debug = 0;
int Src_fd = -1;
uint64_t Bytes_read = 0;
int First_dir_seen = 0;
int Check_this_dir = 0;
uint32_t Blk_size = -1;
int Changing_tapes = FALSE;
int Use_stdin = FALSE;
int Silent_mode = FALSE;
int Quota_ignore = FALSE;
int Interactive = FALSE;
int Preserve_modes = TRUE;
int Screen_width = 80; /* default is 80 columns */
FILE *Local_tty = stdin;
FILE *Shell_tty = stdin;
int Quota_data_present = FALSE;
int User_Quota_data_present = FALSE;
int Group_Quota_data_present = FALSE;
pthread_mutex_t tty_mutex = PTHREAD_MUTEX_INITIALIZER;
/* guards the close_device global variable */
pthread_mutex_t dev_mutex = PTHREAD_MUTEX_INITIALIZER;
int Pause_restore = FALSE;      /* stop processing records */
int Stdout_to_tty;              /* stdout is a tty */
char *remote_user_name = NULL;
char *remote_host_name = NULL;

#define RPROG "advrrestore"

char *Prog = NULL;
int Vdump = 0;
int Rewind = FALSE;

int File_incomplete_error = FALSE;

int
get_screen_width( void )
{
    char *col_str;
    struct winsize win;
    int num_cols = 80;

    if ((col_str = getenv( "COLUMNS" )) != NULL) {
        num_cols = atoi(col_str);
    } else if (Stdout_to_tty) {
        if ((ioctl( 1, TIOCGWINSZ, &win ) != -1) && (win.ws_col != 0))  {
            num_cols = win.ws_col;
        }
    }

    return num_cols;
}


/*
 * Function:
 *
 *      main
 *
 * Function description:
 *
 *      Restore's main block.  Parses command line arguments (argc and
 *      argv) and initiates the restoration of files from a save-set.
 *
 * Return (exit) value:
 *
 *      ERROR - If an error occurred.
 *      0     - If the function completed successfully
 */
main(
     int argc,      /* in - argument count */
     char *argv[]   /* in - array of ptrs to arg strings */
     )
{
    /* argv variables */
    int g = 0, c, l = 0, i = 0, t = 0, s = 0, r = 0, D = 0, q = 0, Q = 0,
        v = 0, m = 0, w = 0, o = 0, f = 0, x = 0, h = 0, n = 0, e = 0;
    extern int optind;
    extern char *optarg;
    char *p;
    char *overwrite;
    char *Device_name_ptr = NULL;
    char *tmp_ptr = NULL;

    int j,
        done = FALSE,
        device = FALSE,
        help = FALSE,
        show_stats = FALSE,
        error = FALSE,
        compressed_data = FALSE,
        xor_blks = 0;

    reliability_t reliability = R_UNKNOWN;

    struct timeval start_time;
    struct timezone tz;
    uint64_t bytes_read = 0, bytes_written = 0, file_cnt = 0;

    int result;

    /*-----------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_VRESTORE, NL_CAT_LOCALE);
 
    snprintf( Tmp_fname, (sizeof(Tmp_fname) - 1), "%s.%d", 
        "_advrestore_tmp", getpid() );

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    Stdout_to_tty = isatty(1);

    /*
     * Initialize start_time
     */
    gettimeofday( &start_time, &tz );

    /*
     * Process command args
     */

    while ((c = getopt( argc, argv, "ghilmqQtvVxo:f:D:") ) != EOF) {

        switch (c) {
        case 'g':
             Debug++;
             break;

        case 'D':
             /* get file system path name */
             D++;
             Target = optarg;
             break;
 
        case 'l':
             /* no params required for list*/
             l++;
             List = 1;
             break;
 
        case 'i':
             /* no params required for interactive */
             i++;
             Interactive = TRUE;
             break;
 
        case 'q':
             /* no params required for interactive */
             q++;
             Silent_mode = TRUE;
             break;
 
        case 'Q':
             /* no params required for ignore quotafiles */
             Q++;
             Quota_ignore = TRUE;
             break;
 
        case 't':
             /* user just wants to see what's in the saveset */
             /* no params required */
             t++;
             Show = TRUE;
             break;
 
        case 'v':
             /* no params required for verbose */
             v++;
             Verbose = TRUE;
             break;
 
        case 'V':
             /* no params required for version of advrestore*/
             fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE1, "%s  Version %s\n"), Prog, VERSION_STRING );
             goto _end;
             break;
 
        case 'm':
             /* no params required for modes */
             m++;
             Preserve_modes = FALSE;
             break;
 
        case 'o':
             /* one required param for overwrite */
             o++;
             overwrite = optarg;
 
             if ((strcmp( overwrite, "yes" ) == 0) ||
                 (strcmp( overwrite, "y" ) == 0) ) {
                 Overwrite = OW_YES;
  
             } else if ((strcmp( overwrite, "no" ) == 0) ||
                        (strcmp( overwrite, "n" ) == 0) ) {
                 Overwrite = OW_NO;
  
             } else if (strcmp( overwrite, "ask" ) == 0) {
                 Overwrite = OW_ASK;
  
             } else {
                 fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE2, 
                     "\n%s: invalid parameter for option <-o>\n"), Prog );
                 o++;
             }
             break;
 
        case 'f':
             /* one required param for file */
             f++;
             Source = optarg; 
 
             if (Source == NULL) {
                 fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE3, 
                     "\n%s: missing parameter for option <-f>\n"), Prog );
                 f++;
                 break;
             }

             device = TRUE;

             if (strcmp( Source, "-" ) == 0) {
                 Use_stdin = TRUE;
 
             } else {
                 if(strlen(Source) > (MAXUSERNAMELEN + MAXHOSTNAMELEN + MAXPATHLEN)  ) {
                     fprintf( stderr,
                           catgets(catd, S_VRESTORE1, VRESTORE235,
                           "\n%s: parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"),
                           Prog, MAXHOSTNAMELEN, MAXPATHLEN);
                     goto bad_end;
                 } 
             }
             break;
 
        case 'x':
             /* at least one, or more, params for extract */
             x++;
             File_spec = TRUE;
             break;
           
        case 'h':
	     /*
	      * don't fall through to default for -h since we
	      * need to return the correct status
	      */
             usage();
	     exit( 0 );
	     break;
 
        default:
             usage();
             exit( 1 );
 
        }  /* end switch */

    } /* end while */

    if (File_spec) {
        /*
         * Must get the files to extract at the end of arg list
         */
        while (optind != argc) {
            File_specs[File_spec_cnt] = argv[optind];
            File_spec_cnt++;
  
            if (File_spec_cnt >= MAX_FILE_SPECS) {
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE4, 
                    "%s: too many files (%d) to extract;\n"), 
                    Prog, MAX_FILE_SPECS);
                exit(1);
            }
            optind++;
        }
    }

    if ( l>1 || i>1 || q>1 || Q>1 || t>1 || v>1 || m>1 || o>1 ||
        f>1 || x>1 || h>1 || w>1 || D>1 ) {
       usage();
       exit(1);
    }

    if ((i + x + l + t) > 1) {
        /* mutually exclusive options */
        usage();
        exit(1);
    }

    if ((i + x + l + t) == 0) {
        /* must have one of these options */
        usage();
        exit(1);
    }

    /*
     * Open a local /dev/tty for prompting the user during an error.
     * If the data to be restored is piped in through stdin, also use
     * /dev/tty for interactive mode shell commands. If the fopen() fails,
     * Local_tty will be NULL.
     */
    Local_tty = fopen("/dev/tty", "r");
    if (Use_stdin)
        Shell_tty = Local_tty;

    /*
     * Must prepare to get any signal interrupts.
     */
    if (!Interactive)
        prepare_signal_handlers();

    if (Target == NULL) {
        Target = ".";
    }

    if (!device) {
        if (strcmp(Prog, RPROG) == 0) {
            /* no media was specified so error out */
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE221,
                "%s: Need option 'f' followed by remote device \"hostname:path\"\n"), Prog );
            goto _end;
        } else {  
            /* no media was specified so use a default */
            Source = DEFTAPE;	/* "/dev/rmt/0m" in mtio.h */
        }
    }

    /* decode Source in format host:tape into */
    /* remote_host_name and Device_name_ptr components */

    remote_host_name = strtok(Source, ":");
    Device_name_ptr = strtok(NULL, ":");

    if (Device_name_ptr == NULL) { 
        if (strcmp(Prog, RPROG) == 0) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE221,
                "%s: Need option 'f' followed by remote device \"hostname:path\"\n"), Prog );
            goto bad_end;
        } else {
            Device_name_ptr = remote_host_name;
            remote_host_name = NULL;
        }
    }

    if(strlen(remote_host_name) > MAXHOSTNAMELEN) {
        fprintf( stderr,
                catgets(catd, S_VRESTORE1, VRESTORE237,
                "\n%s: hostname parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"),
                Prog, MAXHOSTNAMELEN, MAXPATHLEN);
        errno = ENAMETOOLONG;
        goto bad_end;
    }
    if(strlen(Device_name_ptr) > MAXPATHLEN) {
        fprintf( stderr,
            catgets(catd, S_VRESTORE1, VRESTORE236,
            "\n%s: path parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"),
            Prog, MAXHOSTNAMELEN, MAXPATHLEN);
        errno = ENAMETOOLONG;
        goto bad_end;
    }

    if (remote_host_name) {
        /* establish connection to remote host */
        rmthost(&remote_host_name, remote_user_name);
        /* rmthost() is the only reason to be setuid */
        (void) setuid(geteuid());
    }

    sync();

    if ((Src_fd = vrestore_open_device( Source )) == ERROR) {
        exit(1);
    }

    get_fs_info( Target, &Fs_info, FALSE, FALSE );

    if (chdir( Target ) < 0) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE5, 
                 "%s: error accessing target directory <%s>; [%d] %s\n"),
                 Prog, Target, errno, ERR_MSG );
        abort_prog("%s: can't perform chdir\n", Prog);
    }

    if (get_save_set_attr( Src_fd, 
                           &Blk_size,
                           &xor_blks,   
                           &reliability, 
                           &compressed_data ) == ERROR) {
        abort_prog("%s: can't obtain fileset attributes\n", Prog);
    } else {
        blk_pool_handle_t blkh;
        recov_blk_pool_handle_t rblkh;
        int err;

        Screen_width = get_screen_width();

        err = blk_pool_create( &blkh, 2 /* 2 blocks initially */, Blk_size );
        if (err == ERROR) {
            abort_prog( catgets(catd, S_VRESTORE1, VRESTORE6, 
                "%s: cannot create block pool\n"), Prog );
        }
    
        if (reliability == R_CRC_AND_XOR) {
            /* use at least two groups to increase throughput */
            err = rblk_pool_create( &rblkh,
                                    blkh, 
                                    TRUE, /* using XOR recovery */
                                    xor_blks + 1, /* blocks */
                                    2, /* groups */ 
                                    0 );
        } else {
            /* use at least 8 blocks to increase throughput */
            err = rblk_pool_create( &rblkh, 
                                    blkh, 
                                    FALSE, /* not using XOR recovery */
                                    8, /* blocks */ 
                                    1, /* groups */
                                    0 );
        }

        if (err == ERROR) {
            abort_prog( catgets(catd, S_VRESTORE1, VRESTORE6, 
                "%s: cannot create block pool\n"), Prog );
        }

        post_event_backup_lock(Prog);
    
        restore( xor_blks, 
                 reliability, 
                 compressed_data,
                 rblkh,
                 &file_cnt,
                 &bytes_written);

        rblk_pool_delete( rblkh );
        blk_pool_delete( blkh );
    }

    close_device( Src_fd );            /* close the save-set               */

_end:

    sync();sync();

    if (show_stats) {
            show_statistics( Bytes_read,
                             bytes_written, 
                             file_cnt,
                             start_time);
    }

    if ( Dev_type == TAPE && !Rewind) {
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE219, 
                 "%s: rewinding tape\n"), Prog );
    }

    if (File_incomplete_error) {
        /* At least one file was not recovered incomplete. */
        post_event_backup_error(Prog);
        exit(1);
    }

    post_event_backup_unlock(Prog);
    exit(0);

bad_end:

    abort_now();

}
/* end main */

int
get_extraction_list( int dir_fd )
{
    int f;
    int status;

    if (File_spec_cnt == 0) {
        /* -x was specified but no files were given; restore all files */

        status = dir_add_to_restore( dir_fd, "." );

        if (status != OKAY) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE7, 
                "%s: file extraction list is invalid\n"), Prog );
            return FALSE;
        }

        return TRUE;
    }

    for (f = 0; f < File_spec_cnt; f++) {
        status = dir_add_to_restore( dir_fd, File_specs[f] );

        if (status != OKAY) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE7, 
                "%s: file extraction list is invalid\n"), Prog );
            return FALSE;
        }
    }

    return TRUE;
}


typedef enum { 
    RS_IDLE, 
    RS_ERROR,
    RS_SUMMARY, 
    RS_FILLER,
    RS_END_OF_VOL_SET,
    RS_DIR_ATTR, 
    RS_BF_DIR_ATTR, 
    RS_DIR_HDR, 
    RS_DIR_DATA, 
    RS_FILE_ATTR, 
    RS_BF_FILE_ATTR, 
    RS_FILE_DATA,
    RS_HARD_LINK,
    RS_SYM_LINK,
    RS_DEV_ATTR,
    RS_FQUOTA,
    RS_UQUOTA,
    RS_GQUOTA,
    RS_ACL
} restore_state_t;

/*
 * STATE_TRANSITION -
 *
 * This macro makes sense only in the context of restore()
 */
#define STATE_TRANSITION( old_state, new_state ) \
        state_transition( (old_state), \
                          (new_state),\
                          compressed_data,\
                          lzwh,\
                          dir_fd, \
                          &restore_fd,\
                          restore_fname, \
                          &fattr.fattr )

/*
 * state_transition -
 *
 * This routine implements restore state transitions from 'old_state' to
 * 'new_state'.  It defines which transitions are legal and it also
 * performs the necessary work to do the state transition; some transitions
 * require no work at all.
 * 
 * Using the -l option it is possible to observe the types of records
 * which make up the states addressed in this table.
 * The record types that translate into states begin after the initial
 * headers for the archive (reported by -l as BLOCK HEADERs). The backup
 * is then divided into two sections, containing first the <dirs> and then
 * the <files> (modeled after the dump program). Having this brief directory
 * section in the front of the dump is convenient for allowing the interactive
 * mode to show the directory tree structure.
 * The <dirs> section contains directory info which takes on the following
 * order:
 *      RT_DIR_HDR (and if the directory contains data)-> RT_DIR_DATA
 *      but if the directory has ACLs attched then
 *      RT_DIR_HDR -> RT_ACL -> RT_DIR_DATA (if the dir has data)
 * The <dirs> section usually ends with an RT_FILLER record and then the
 * <files> section is introduced by a BLOCK HEADER with flags = <files>
 * The <files> section contains both the directories and their files and
 * attributes and characteristics for any of them:
 *      RT_DIR_ATTR -> (optional) RT_ACL -> 
 *	  (optional - only advfs) RT_BF_DIR_ATTR -> RT_FILE_ATTR ->
 *        (optional) RT_ACL -> (optional) RT_BF_FILE_ATTR -> 
 *	  (optional) RT_FILE_DATA
 */
restore_state_t
state_transition(
                 restore_state_t old_state,
                 restore_state_t new_state,
                 int compressed_data,
                 lzw_desc_handle_t lzwh,
                 int dir_fd,
                 int *restore_fd,
                 char *restore_fname,
                 struct fattr_t *fattr
                 )
{
    int t;

    if (old_state == RS_END_OF_VOL_SET) {
        abort_prog( catgets(catd, S_VRESTORE1, VRESTORE8, 
            "%s: state transition after 'end of vol set'\n"), Prog );
    }

    switch (new_state) {
      case RS_FILE_DATA:
        /* on AdvFS, the RS_FILE_DATA will always follow itself or 
         * RS_BF_FILE_ATTR but under other filesystem types, it could follow 
         * itself or RS_FILE_ATTR or RS_ACL
         */
        if ((old_state != RS_FILE_DATA) &&
            (old_state != RS_BF_FILE_ATTR) &&
            (old_state != RS_FILE_ATTR) &&
            (old_state != RS_ACL)
	    ) {
            new_state = RS_ERROR;
        }
        break;

      case RS_BF_FILE_ATTR:
        if ((old_state != RS_FILE_ATTR) &&
            (old_state != RS_ACL)
	    ) {
            new_state = RS_ERROR;
        }
        break;

      case RT_BF_DIR_ATTR:
        if ((old_state != RS_DIR_ATTR) &&
            (old_state != RS_ACL)
	    ) {
            new_state = RS_ERROR;
        }
        break;

      case RS_ACL:
        if ((old_state != RS_FILE_ATTR) &&
            (old_state != RS_DIR_ATTR) && 
            (old_state != RS_DIR_HDR)) {
            new_state = RS_ERROR;
        }
 
        break;

      case RS_FQUOTA:
        if (old_state != RS_DIR_DATA)
            new_state = RS_ERROR;
      break;

      case RS_UQUOTA:
        if ((old_state != RS_FQUOTA) &&
            (old_state != RS_UQUOTA)) {
            new_state = RS_ERROR;
        }
      break;

      case RS_GQUOTA:
        if ((old_state != RS_FQUOTA) &&
            (old_state != RS_GQUOTA)) {
            new_state = RS_ERROR;
        }
      break;

      default:
        switch (old_state) {
          case RS_DIR_DATA:
            if (compressed_data) {
                int uc_cnt;
                char buf[MAX_BLK_SZ];

                uc_cnt = finish_uncompress( lzwh, buf );
                logical_dir_write( dir_fd, buf, uc_cnt );
            }
            break;

          case RS_FILE_ATTR:
          case RS_BF_FILE_ATTR:
          case RS_FILE_DATA:
            /* 
             * Close previously restored file 
             */
            if (FILE_OPEN( *restore_fd )) {
                close_file( fattr,
                            restore_fd, 
                            restore_fname, 
                            compressed_data, 
                            new_state == RS_ERROR,
                            lzwh );
            }
            break;

            default:
            break; /* do nothing */
        }

        break;
    }

    return new_state;
}
/* end state_transition */

/*
 * restore -
 *
 * This routine reads save-set blocks and calls the appropriate
 * routines to restore directories and files and other related structures.
 *
 */

void
restore( 
   int xor_blks,                  /* in - Number of blocks in an XOR group */
   reliability_t reliability,     /* in - The save-set's reliabilty mode */
   int compressed_data,           /* in - Compressed save-set data flag */
   recov_blk_pool_handle_t rblkh, /* in - Recoverable block pool handle */
   uint64_t *file_cnt,            /* out - Number of files restored */
   uint64_t *total_bytes_restored /* out - num bytes restored */
   )
{
    enum { RESTORING_FILES, RESTORING_DIRS } mode;

    int 
      restore_fd = CLOSED,      /* file desc. of file being restored        */
      uq_fd = CLOSED,		/* file desc. of user quota file            */
      gq_fd = CLOSED,		/* file desc. of group quota file           */
      dp = CLOSED,
      dir_restored = FALSE,
      block_num = 1,
      expected_vol_num = 1,
      expected_blk_num = 1,
      restore_uquotas = FALSE,	/* TRUE if user quotas are to be restored   */
      restore_gquotas = FALSE,	/* TRUE if group quotas are to be restored  */
      qwarned_already = FALSE,  /* TRUE if user has been warned re. quotas  */
      user_is_root = FALSE,  	/* TRUE if user is root (uid = 0)           */
      target_q_on = TRUE,	/* TRUE if target fs has enabled quotas     */
      blk_id = -1,              /* current blocks id                        */
      data_idx,                 /* block data index variable                */
      eob,                      /* end-of-block flag                        */
      eovs = FALSE,             /* end-of-vol-set flag                      */
      overwrite,                /* overwrite the file?                      */
      ret_val,
      err, 
      status = OKAY,            /* status returned by rblk_get              */
      seg_decrCnt;              /* keeps track of the number of stripes     */

    struct blk_t *blk;           /* pointer to the current block            */
    union record_header_t rhdr;  /* record header                           */
    char restore_fname[MAXPATHLEN+1];
    char dir_name[MAXPATHLEN+1];
    char name[MAXPATHLEN+1];
    char lname[MAXPATHLEN+1];
    char last_name[MAXPATHLEN+1];
    char uqfile_name[MAXPATHLEN+1],    /* User quota file name */
         gqfile_name[MAXPATHLEN+1];    /* Group quota file name*/
    lzw_desc_handle_t lzwh;

    struct bf_attr_rec_t bf_attr;      /* bitfile attributes record         */
    struct bf_attr_rec_t i_attr;       /* inheritable attributes record     */
    union file_attr_rec_t fattr;       /* file attributes record            */
    union dir_attr_rec_t dir_hdr;      /* directory attributes record       */
    union dir_attr_rec_t dir_attr;     /* directory attributes record       */
    union hard_link_rec_t hlink;       /* hard link attributes record       */
    union symbolic_link_rec_t slink;   /* sym link attributes record        */
    union dev_attr_rec_t dev_attr;     /* device attributes record          */

    restore_state_t state = RS_IDLE;
    int dir_fd = -1;
    int first_time = 1;
    char msg[30];
    int interactive_shell( int, int * );
    bfTagT   qfTag;           		/* Needed to get quota file names   */
    struct fstab *fs;			/* For reading through /etc/fstab   */
    char *opt, *cp = NULL;		/* For parsing /etc/fstab entries   */
    bfSetParamsT oldSetParams;	        /* Current bfset parms for target fs*/


    /*----------------------------------------------------------------------*/

    mode = RESTORING_DIRS;

    dir_fd = logical_dir_creat( "./_advrestore_dir" );
    if (compressed_data) {
        status = init_uncompress( &lzwh );
        if (status != 0) {
            fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE243,
                "%s: Can not decompress dump data.\n"));
            Abort_restore = 1;
        }
    }

    /* 
     * Read blocks and restore files until done or abort is requested
     */

    while (!eovs && (0 == Abort_restore)) {

        status = rblk_get( rblkh, &blk, &blk_id );
        if (status < 0) {

           if ( status == END_OF_BLKS ) {
               strcpy(msg, catgets(catd, S_VRESTORE1, VRESTORE9, 
                   "Reached end of file"));
           } else if ( status == ERR_BAD_BLK ) {
               strcpy(msg, catgets(catd, S_VRESTORE1, VRESTORE10, 
                   "Read a bad block"));
           }

           /* Severe error from rblk_get().  Terminate. */
           fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE11, 
               "%s: unexpected error from rblk_get(): [%d] %s\n"), 
               Prog, status, msg );
           break;
        }

        if (status == ERR_BAD_BLK) {
            /* 
             * The block is corrupt and we cannot recover it 
             */
            fprintf( stderr,
                    catgets(catd, S_VRESTORE1, VRESTORE12, 
                    "%s: unable to restore block <%d>; corrupt block\n"), 
                    Prog, block_num );
            dump_blk_hdr( &blk->bhdr.bheader );
            rblk_free( rblkh, blk_id ); /* release the current block */
            state = STATE_TRANSITION( state, RS_ERROR );
            block_num++;
            continue; /* skip this block */
        } 

        if (List) {
            list_blk_hdr( &blk->bhdr.bheader, stdout );
        }

        if (blk->bhdr.bheader.flags & BF_XOR_BLOCK) {
            rblk_free( rblkh, blk_id ); /* release the current block */
            block_num++;
            continue; /* skip this block */
        }

        if ((mode == RESTORING_DIRS) && 
            !(blk->bhdr.bheader.flags & BF_DIRECTORIES)) {
            int resume = TRUE;

            /* transition from restoring dirs to restoring files */

            /*
             * See if there is quota data in the saveset.  We need
             * to check this before starting the interactive shell.
             */
            if (blk->bhdr.bheader.flags & BF_QUOTA_DATA)
                Quota_data_present = TRUE;

	    if (blk->bhdr.bheader.flags & BF_USER_QUOTA_DATA)
                User_Quota_data_present = TRUE;

	    if (blk->bhdr.bheader.flags & BF_GROUP_QUOTA_DATA)
                Group_Quota_data_present = TRUE;

            if (List) {
                resume = TRUE;

            } else if (Root_ino == 0) {
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE13, 
                    "%s: empty save-set\n"), Prog );
                resume = FALSE;

            } else if (Interactive) {
                /*
                 * Make sure that there is a tty associated with
                 * this process.  If not, abort.
                 */
		if (Shell_tty) {
		    if (resume = interactive_shell( dir_fd, &Verbose ))
			prepare_signal_handlers();
		} else
                    abort_prog( catgets(catd, S_VRESTORE1, VRESTORE169, 
                        "%s: Cannot use interactive shell in batch mode\n"), 
                        Prog );

            } else if (File_spec) {
                resume = get_extraction_list( dir_fd );

            } else {
                if (dir_add_to_restore( dir_fd, "/" ) == OKAY) {
                    resume = TRUE;
                } else {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE14, 
                        "%s: cannot access '/'\n"), Prog );
                    resume = FALSE;
                }
            }

            if (!resume) {
                fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE15, 
                    "%s: nothing will be restored\n"), Prog );
                eovs = 1;  /* terminate */
            }

            mode = RESTORING_FILES;

            /*
             * Decide which, if any, quota files should be restored
             * and set up to restore them.
             */

            if (getuid() == 0)
                user_is_root = TRUE;

            if (!Quota_ignore) {

            	if (List) {
                  if (user_is_root) {
                      restore_uquotas = TRUE;
                  }
                } else {
	 	  if (Quota_data_present) {
		    int	type,                   /* Loop counter              */
		        qinode,                 /* Quota file inode number   */
                        *restore_switch,        /* TRUE if restoring quotas  */
                        *qfd;                   /* Open file descriptor      */
		    char *real_qfile_name,      /* Real quota file name      */
                        *prototype_qfile_name;  /* Prototypical quota file   */

		    for (type = USRQUOTA; type < MAXQUOTAS; type++) {
			switch (type) {
		            case USRQUOTA:
                                 qinode = USR_QUOTA_INO;
				 restore_switch = &restore_uquotas;
				 real_qfile_name = uqfile_name;
				 prototype_qfile_name = USR_QUOTA_FILE;
				 qfd = &uq_fd;
				 break;
#ifdef GROUP_QUOTAS
			    case GRPQUOTA:
                                 qinode = GRP_QUOTA_INO;
				 restore_switch = &restore_gquotas;
				 real_qfile_name = gqfile_name;
				 prototype_qfile_name = GRP_QUOTA_FILE;
				 qfd = &gq_fd;
				 break;
#endif /* GROUP_QUOTAS */
                             default:
				 continue;
			     }

			if (dir_restore_me(qinode, Root_ino)) {
			    if (Show)
				    *restore_switch = TRUE;
			    else 
                               if (!user_is_root) {
				  if (!qwarned_already) {
				     fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE172, "%s: Must be root user to restore quota information.\n"), Prog);
				     qwarned_already = TRUE;
				  }
			       } else {
				/* 
				 * User is root.  Find the name of the quota 
				 * file on the target file system and try to 
				 * open it.  If it exists, see if the user 
				 * wants to overwrite it.
				 */
				if (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) == 0){
				    qfTag.tag_num = qinode;
				    qfTag.tag_seq = 0;
    
				    if (tag_to_path(Fs_info.path,
						    qfTag,
						    MAXPATHLEN+1,
						    real_qfile_name)) {
					fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE171, "%s: Could not obtain %s quota file name: [%d] %s.\nQuotas not included in saveset may not be removed.\n"), Prog, 
#ifdef GROUP_QUOTAS
						qfextension[type], errno, ERR_MSG);
#else
						"user", errno, ERR_MSG);
#endif /* GROUP_QUOTAS */
				    }
				} else {
				    /*
				     * We are restoring to a non-AdvFS file system.  Unlike
				     * AdvFS, it probably does not constantly maintain quota
				     * structures.  Therefore, any attempt to call
				     * quotactl() to update quotas for the file
				     * system will fail if quotas are not on.  
				     */
				    if (Fs_info.quotas_on == FALSE) {
					fprintf(stderr, 
						catgets(catd, S_VRESTORE1, VRESTORE173, "%s: %s quotas must be enabled in order to restore %s quota file.\n"),
#ifdef GROUP_QUOTAS
						Prog, qfextension[type], 
						qfextension[type]);
#else
						Prog, "user", "user");
#endif /* GROUP_QUOTAS */
					target_q_on = FALSE;
				    } else {
					/*
					 * on HPUX, quotafile names are fixed
					 */
                                        char tmp_qfilename[MAXPATHLEN+1];
                                        snprintf(real_qfile_name, 
                                            sizeof(tmp_qfilename), "%s/%s", 
                                            Fs_info.path, prototype_qfile_name);
                                        real_qfile_name = strdup(tmp_qfilename);
				    }
				}
				
				if (target_q_on) {
				    *qfd = open( real_qfile_name, O_RDONLY, 0 );
				    if (*qfd == CLOSED) {
					fprintf(stderr, 
						catgets(catd, S_VRESTORE1, VRESTORE174, "%s: Open of %s quota file failed: [%d] %s.\n%s quotas cannot be restored.\n"), 
#ifdef GROUP_QUOTAS
						Prog, qfextension[type], 
						errno, ERR_MSG, qfextension[type]);
#else
						Prog, "user", "user");
#endif /* GROUP_QUOTAS */
				    } else {
					if (!check_overwrite(prototype_qfile_name)) 
						*restore_switch = TRUE;
				    }
				}
			    }
			}
		    }
                }
	     }
          }
        } else if ((mode == RESTORING_FILES) &&
                   !(blk->bhdr.bheader.flags & BF_FILES)) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE16, 
                    "%s: encountered unexpected block type; terminating\n"), 
                    Prog );
            dump_blk_hdr( &blk->bhdr.bheader );
            break;
        }

        /* 
         * block is good so process its records 
         */

        data_idx = eob = 0;

        /* 
         * while there are records, process them 
         */

        while ((data_idx < MAX_DATA_SIZE( Blk_size )) && !eob && !eovs )  {
            /*
             * check if need to pause processing
             */
            if (Pause_restore) {
                pthread_mutex_lock(&tty_mutex);
                pthread_mutex_lock(&tty_mutex);
            }
            /* 
             * Extract record header 
             */
            flmemcpy( &blk->data[data_idx], (char*) &rhdr, sizeof( rhdr ) );
            data_idx += sizeof( rhdr );

            if (Version < 6.0) {
                /*  Tru64 saveset, switch the endianness */
                
                rhdr.rheader.type = SWAP32(rhdr.rheader.type);
                rhdr.rheader.d_offset_lo = SWAP32(rhdr.rheader.d_offset_lo);
                rhdr.rheader.size = SWAP16(rhdr.rheader.size);
                rhdr.rheader.flags = SWAP16(rhdr.rheader.flags);
                rhdr.rheader.d_offset_hi = SWAP32(rhdr.rheader.d_offset_hi);
            }

            if (rhdr.rheader.size > MAX_DATA_FREE( Blk_size )) {
                /* oops! didn't pass sanity check */
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE17, 
                    "%s: *** bad save-set ***, "), Prog );
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE18, 
                    "record size <%d> in block <%d> is bad, "),
                        rhdr.rheader.size, block_num );
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE19, 
                    "*** cannot restore this block ***\n") );
                eob      = TRUE;
                state    = STATE_TRANSITION( state, RS_ERROR );
                continue;
            }

            /* 
             * Based on the mode and record type 
             * perform the appropriate record-specific actions.
             */

            switch (mode) {

              case RESTORING_DIRS:

                /* 
                 ** Handle the record types that are valid while
                 ** restoring dirs.
                 */

                switch (rhdr.rheader.type) {
    
                  case RT_DIR_HDR:         /* directory header record */
                    state = STATE_TRANSITION( state, RS_DIR_HDR );
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE20, 
                            "directory header"), &rhdr.rheader, stdout);
                    }
		    
		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &dir_hdr, 
			       sizeof( dir_hdr ) );
		      flmemcpy( &blk->data[data_idx + sizeof( dir_hdr )], 
			       (char *) dir_name, 
			       dir_hdr.dattr.dname_bytes );
		    } else {
		      union old_dir_attr_rec_t old_dir_hdr;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_dir_hdr, 
			       sizeof( old_dir_hdr ) );

		      /*
		       * Load dir_hdr with values from old_dir_hdr.
		       */
		      dir_hdr.dattr.dir_stat.st_dev = old_dir_hdr.dattr.dir_stat.st_dev;
		      dir_hdr.dattr.dir_stat.st_ino = old_dir_hdr.dattr.dir_stat.st_ino;
		      dir_hdr.dattr.dir_stat.st_mode = old_dir_hdr.dattr.dir_stat.st_mode;
		      dir_hdr.dattr.dir_stat.st_nlink = old_dir_hdr.dattr.dir_stat.st_nlink;
		      dir_hdr.dattr.dir_stat.st_uid = old_dir_hdr.dattr.dir_stat.st_uid;
		      dir_hdr.dattr.dir_stat.st_gid = old_dir_hdr.dattr.dir_stat.st_gid;
		      dir_hdr.dattr.dir_stat.st_rdev = old_dir_hdr.dattr.dir_stat.st_rdev;
		      dir_hdr.dattr.dir_stat.st_size = old_dir_hdr.dattr.dir_stat.st_size;
		      dir_hdr.dattr.dir_stat.st_atime = old_dir_hdr.dattr.dir_stat.st_atime;
		      dir_hdr.dattr.dir_stat.st_blksize = old_dir_hdr.dattr.dir_stat.st_blksize;
		      dir_hdr.dattr.dir_stat.st_mtime = old_dir_hdr.dattr.dir_stat.st_mtime;
		      dir_hdr.dattr.dir_stat.st_ctime = old_dir_hdr.dattr.dir_stat.st_ctime;
		      dir_hdr.dattr.parent_ino = old_dir_hdr.dattr.parent_ino;
		      dir_hdr.dattr.dname_bytes = old_dir_hdr.dattr.dname_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_dir_hdr )], 
			       (char *) dir_name, 
			       dir_hdr.dattr.dname_bytes );
		    }

                    if (Version < 6.0) {
                        /*  TODO  Fix the endianness */
                    }

		    create_dir( dir_fd, &dir_hdr.dattr, dir_name );
                    if (compressed_data) {start_uncompress( lzwh );}
                    break;
    
                  case RT_DIR_DATA:         /* directory data record */
                    state = STATE_TRANSITION( state, RS_DIR_DATA );
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE21, 
                            "directory data"), &rhdr.rheader, stdout);
                    } else if (state == RS_DIR_DATA) {
                        restore_dir_data( dir_fd,
                                          rhdr.rheader.size, 
                                          blk, 
                                          data_idx,
                                          compressed_data,
                                          lzwh );
                    }
                    break;
                  case RT_ACL:         /* ACL record */
                    state = STATE_TRANSITION( state, RS_ACL);
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE25,
                                              "ACL"),
                                     &rhdr.rheader, stdout);
                    }

                    if (state == RS_DIR_DATA) {
                        ret_val = restore_acl(dir_name, 
                                              rhdr,
                                              blk,
                                              data_idx,
                                              compressed_data,
                                              lzwh);

                    }

		    if (ret_val < 0) {
			fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE26,
                            "%s warning: ACLs not supported on destination file system\n"),
			    Prog);
		    }
                    break;

                  case RT_FILLER:              /* filler record          */
                    /* not consider a state transition */
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE22, 
                            "filler"), &rhdr.rheader, stdout);
                    }
                    eob = TRUE;
                    break;
    
                  case RT_END_OF_VOL_SET:      /*  end of vol set record */
                    state = STATE_TRANSITION( state, RS_END_OF_VOL_SET );
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE23, 
                            "end of save-set"), &rhdr.rheader, stdout);
                    }
                    eovs = TRUE;
                    break;
    
                  default:                     /* oops!!                 */
                    state = STATE_TRANSITION( state, RS_ERROR );
                    list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE24, 
                        "unknown/unexpected"), &rhdr.rheader, 
			stderr);
                    break;
                }
    
                break;

              case RESTORING_FILES:

                /* 
                 ** Handle the record types that are valid while
                 ** restoring files.
                 */

                switch (rhdr.rheader.type) {
    
                  case RT_ACL:         /* ACL record */
                    state = STATE_TRANSITION( state, RS_ACL);
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE25,
                                              "ACL"),
                                     &rhdr.rheader, stdout);
                    }

                    ret_val = restore_acl(restore_fname, 
                                          rhdr,
                                          blk,
                                          data_idx,
                                          compressed_data,
                                          lzwh);

		    if (ret_val < 0) {
			fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE26,
                            "%s warning: ACLs not supported on destination file system\n"),
			    Prog);
		    }
                    break;
                    
                  case RT_PROPLIST:         /* extended attributes record */
                    /*  If it's a Tru64 dump file, could have RT_PROPLIST
                     *  records.  Since property lists aren't supported
                     *  on HP-UX, don't do anything, but we need this 
                     *  case to avoid failures
                     */
                    break;

                  case RT_DIR_ATTR:         /* directory attributes record */
                    state = STATE_TRANSITION( state, RS_DIR_ATTR );

                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE28, 
                            "directory attributes"), &rhdr.rheader, stdout);
                    }
		    
		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &dir_attr, 
			       sizeof( dir_attr ) );
		      flmemcpy( &blk->data[data_idx + sizeof( dir_attr )], 
			       (char *) name, 
			       dir_attr.dattr.dname_bytes );
		    } else {
		      union old_dir_attr_rec_t old_dir_attr;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_dir_attr, 
			       sizeof( old_dir_attr ) );

		      /*
		       * Load dir_attr with values from old_dir_attr.
		       */
		      dir_attr.dattr.dir_stat.st_dev = old_dir_attr.dattr.dir_stat.st_dev;
		      dir_attr.dattr.dir_stat.st_ino = old_dir_attr.dattr.dir_stat.st_ino;
		      dir_attr.dattr.dir_stat.st_mode = old_dir_attr.dattr.dir_stat.st_mode;
		      dir_attr.dattr.dir_stat.st_nlink = old_dir_attr.dattr.dir_stat.st_nlink;
		      dir_attr.dattr.dir_stat.st_uid = old_dir_attr.dattr.dir_stat.st_uid;
		      dir_attr.dattr.dir_stat.st_gid = old_dir_attr.dattr.dir_stat.st_gid;
		      dir_attr.dattr.dir_stat.st_rdev = old_dir_attr.dattr.dir_stat.st_rdev;
		      dir_attr.dattr.dir_stat.st_size = old_dir_attr.dattr.dir_stat.st_size;
		      dir_attr.dattr.dir_stat.st_atime = old_dir_attr.dattr.dir_stat.st_atime;
		      dir_attr.dattr.dir_stat.st_mtime = old_dir_attr.dattr.dir_stat.st_mtime;
		      dir_attr.dattr.dir_stat.st_ctime = old_dir_attr.dattr.dir_stat.st_ctime;
		      dir_attr.dattr.dir_stat.st_blksize = old_dir_attr.dattr.dir_stat.st_blksize;
		      dir_attr.dattr.parent_ino = old_dir_attr.dattr.parent_ino;
		      dir_attr.dattr.dname_bytes = old_dir_attr.dattr.dname_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_dir_attr )], 
			       (char *) name, 
			       dir_attr.dattr.dname_bytes );
		    }

                    dir_restored = create_file_dir( &dir_attr.dattr, name,
						    last_name );

		    if (!First_dir_seen){ /* First directory is special */
                        if (strcmp(name, ".") == 0){ /* Non-restoreable ? */
			    Check_this_dir = 1;
                        }
                        First_dir_seen = 1;/* never again */
                    }
                    break;

                 case RT_BF_DIR_ATTR:   /* bitfile dir attributes record */
                    state = STATE_TRANSITION( state, RS_BF_DIR_ATTR );

                    if (!List && !Show && dir_restored &&
                        (state == RS_BF_DIR_ATTR) &&
                        (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) == 0)) {

                        flmemcpy( &blk->data[data_idx],
                                 (char *) &bf_attr, 
                                 sizeof (bf_attr) );
    
                        flmemcpy( &blk->data[data_idx + 
                                          sizeof(struct bf_attr_rec_t)],
                                 (char *) &i_attr, 
                                 sizeof ( i_attr ) );
    
                        flmemcpy( &blk->data[data_idx + 
                                          (2 * sizeof(struct bf_attr_rec_t))],
                                  (char *) name, 
                                  dir_attr.dattr.dname_bytes );
    
                        /* Restore the file's bitfile attributes if restoring
                         * to an advfs file system
                         */
    
                        set_file_dir_attr( &dir_attr.dattr, 
                                           &i_attr.bfAttr,
                                           &i_attr.bfAttr,
                                           name );
                    }
                    break;

                  case RT_HARD_LINK:           /* hard link attributes rec */
                    state = STATE_TRANSITION( state, RS_HARD_LINK );
    
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE29, 
                            "hard link attributes"), &rhdr.rheader, stdout);
                    }

		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &hlink, 
			       sizeof( hlink ) );
		      flmemcpy( &blk->data[data_idx + sizeof( hlink )], 
			       (char *) name, 
			       hlink.hlink.fname_bytes );
		      flmemcpy( &blk->data[data_idx + 
					   sizeof( hlink ) + 
					   hlink.hlink.fname_bytes], 
			       (char *) lname, 
			       hlink.hlink.lname_bytes );
		    } else {
		      union old_hard_link_rec_t old_hlink;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_hlink, 
			       sizeof( old_hlink ) );

		      /*
		       * Load hlink with values from old_hlink.
		       */
		      hlink.hlink.file_stat.st_dev = old_hlink.hlink.file_stat.st_dev;
		      hlink.hlink.file_stat.st_ino = old_hlink.hlink.file_stat.st_ino;
		      hlink.hlink.file_stat.st_mode = old_hlink.hlink.file_stat.st_mode;
		      hlink.hlink.file_stat.st_nlink = old_hlink.hlink.file_stat.st_nlink;
		      hlink.hlink.file_stat.st_uid = old_hlink.hlink.file_stat.st_uid;
		      hlink.hlink.file_stat.st_gid = old_hlink.hlink.file_stat.st_gid;
		      hlink.hlink.file_stat.st_rdev = old_hlink.hlink.file_stat.st_rdev;
		      hlink.hlink.file_stat.st_size = old_hlink.hlink.file_stat.st_size;
		      hlink.hlink.file_stat.st_atime = old_hlink.hlink.file_stat.st_atime;
		      hlink.hlink.file_stat.st_mtime = old_hlink.hlink.file_stat.st_mtime;
		      hlink.hlink.file_stat.st_ctime = old_hlink.hlink.file_stat.st_ctime;
		      hlink.hlink.file_stat.st_blksize = old_hlink.hlink.file_stat.st_blksize;
		      hlink.hlink.file_ino = old_hlink.hlink.file_ino;
		      hlink.hlink.file_parent_ino = old_hlink.hlink.file_parent_ino;
		      hlink.hlink.fname_bytes = old_hlink.hlink.fname_bytes;
		      hlink.hlink.link_parent_ino = old_hlink.hlink.link_parent_ino;
		      hlink.hlink.lname_bytes = old_hlink.hlink.lname_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_hlink )], 
			       (char *) name, 
			       hlink.hlink.fname_bytes );
		      flmemcpy( &blk->data[data_idx + 
					   sizeof( old_hlink ) + 
					   hlink.hlink.fname_bytes], 
			       (char *) lname, 
			       hlink.hlink.lname_bytes );
		    }

                    create_hard_link( &hlink.hlink, name, lname, file_cnt,
				      last_name );

                    break;

                  case RT_SYM_LINK:           /* sym link attributes rec */
                    state = STATE_TRANSITION( state, RS_SYM_LINK );
    
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE30, 
                            "sym link attributes"), &rhdr.rheader, stdout);
                    }

		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &slink, 
			       sizeof( slink ) );
		      flmemcpy( &blk->data[data_idx + sizeof( slink )], 
			       (char *) lname, 
			       slink.slink.lname_bytes );
		      flmemcpy( &blk->data[data_idx + 
					   sizeof( slink ) + 
					   slink.slink.lname_bytes], 
			       (char *) name, 
			       slink.slink.link_bytes );
		    } else {
		      union old_symbolic_link_rec_t old_slink;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_slink, 
			       sizeof( old_slink ) );

		      /*
		       * Load slink with values from old_slink.
		       */
		      slink.slink.link_stat.st_dev = old_slink.slink.link_stat.st_dev;
		      slink.slink.link_stat.st_ino = old_slink.slink.link_stat.st_ino;
		      slink.slink.link_stat.st_mode = old_slink.slink.link_stat.st_mode;
		      slink.slink.link_stat.st_nlink = old_slink.slink.link_stat.st_nlink;
		      slink.slink.link_stat.st_uid = old_slink.slink.link_stat.st_uid;
		      slink.slink.link_stat.st_gid = old_slink.slink.link_stat.st_gid;
		      slink.slink.link_stat.st_rdev = old_slink.slink.link_stat.st_rdev;
		      slink.slink.link_stat.st_size = old_slink.slink.link_stat.st_size;
		      slink.slink.link_stat.st_atime = old_slink.slink.link_stat.st_atime;
		      slink.slink.link_stat.st_mtime = old_slink.slink.link_stat.st_mtime;
		      slink.slink.link_stat.st_ctime = old_slink.slink.link_stat.st_ctime;
		      slink.slink.link_stat.st_blksize = old_slink.slink.link_stat.st_blksize;


		      slink.slink.ino = old_slink.slink.ino;
		      slink.slink.parent_ino = old_slink.slink.parent_ino;
		      slink.slink.lname_bytes = old_slink.slink.lname_bytes;
		      slink.slink.link_bytes = old_slink.slink.link_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_slink )], 
			       (char *) lname, 
			       slink.slink.lname_bytes );
		      flmemcpy( &blk->data[data_idx + 
					   sizeof( old_slink ) + 
					   slink.slink.lname_bytes], 
			       (char *) name, 
			       slink.slink.link_bytes );
		    }

                    create_sym_link( &slink.slink, lname, name, file_cnt,
				     last_name );

                    break;

                  case RT_DEV_ATTR:           /* dev attributes record */
                    state = STATE_TRANSITION( state, RS_DEV_ATTR );
    
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE31, 
                            "device attributes"), &rhdr.rheader, stdout);
                    }

		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &dev_attr, 
			       sizeof( dev_attr ) );
		      flmemcpy( &blk->data[data_idx + sizeof( dev_attr )], 
			       (char *) name, 
			       dev_attr.dattr.dname_bytes );
		    } else {
		      union old_dev_attr_rec_t old_dev_attr;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_dev_attr, 
			       sizeof( old_dev_attr ) );

		      /*
		       * Load dev_attr with values from old_dev_attr.
		       */
		      dev_attr.dattr.dev_stat.st_dev = old_dev_attr.dattr.dev_stat.st_dev;
		      dev_attr.dattr.dev_stat.st_ino = old_dev_attr.dattr.dev_stat.st_ino;
		      dev_attr.dattr.dev_stat.st_mode = old_dev_attr.dattr.dev_stat.st_mode;
		      dev_attr.dattr.dev_stat.st_nlink = old_dev_attr.dattr.dev_stat.st_nlink;
		      dev_attr.dattr.dev_stat.st_uid = old_dev_attr.dattr.dev_stat.st_uid;
		      dev_attr.dattr.dev_stat.st_gid = old_dev_attr.dattr.dev_stat.st_gid;
		      dev_attr.dattr.dev_stat.st_rdev = old_dev_attr.dattr.dev_stat.st_rdev;
		      dev_attr.dattr.dev_stat.st_size = old_dev_attr.dattr.dev_stat.st_size;
		      dev_attr.dattr.dev_stat.st_atime = old_dev_attr.dattr.dev_stat.st_atime;
		      dev_attr.dattr.dev_stat.st_mtime = old_dev_attr.dattr.dev_stat.st_mtime;
		      dev_attr.dattr.dev_stat.st_ctime = old_dev_attr.dattr.dev_stat.st_ctime;
		      dev_attr.dattr.dev_stat.st_blksize = old_dev_attr.dattr.dev_stat.st_blksize;
		      dev_attr.dattr.parent_ino = old_dev_attr.dattr.parent_ino;
		      dev_attr.dattr.dname_bytes = old_dev_attr.dattr.dname_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_dev_attr )], 
			       (char *) name, 
			       dev_attr.dattr.dname_bytes );
		    }

                    create_device_file( &dev_attr.dattr, name, file_cnt,
				        last_name );
            
                    break;
    
                  case RT_FILE_ATTR:           /* file attributes record */
                    state = STATE_TRANSITION( state, RS_FILE_ATTR );

                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE32, 
                            "file attributes"), &rhdr.rheader, stdout);
                    }
		    
		    if (Version >= 5.0) {
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &fattr, 
			       sizeof( fattr ) );
		      flmemcpy( &blk->data[data_idx + sizeof( fattr )], 
			       (char *) name, 
			       fattr.fattr.fname_bytes );
		    } else {
		      union old_file_attr_rec_t old_fattr;

		      /*
		       * To keep from getting unaligned access error, copy
		       * into old struct.  Then move into new struct.
		       */
		      flmemcpy( &blk->data[data_idx], 
			       (char *) &old_fattr, 
			       sizeof( old_fattr ) );

		      /*
		       * Load fattr with values from old_fattr.
		       */
		      fattr.fattr.file_stat.st_dev = old_fattr.fattr.file_stat.st_dev;
		      fattr.fattr.file_stat.st_ino = old_fattr.fattr.file_stat.st_ino;
		      fattr.fattr.file_stat.st_mode = old_fattr.fattr.file_stat.st_mode;
		      fattr.fattr.file_stat.st_nlink = old_fattr.fattr.file_stat.st_nlink;
		      fattr.fattr.file_stat.st_uid = old_fattr.fattr.file_stat.st_uid;
		      fattr.fattr.file_stat.st_gid = old_fattr.fattr.file_stat.st_gid;
		      fattr.fattr.file_stat.st_rdev = old_fattr.fattr.file_stat.st_rdev;
		      fattr.fattr.file_stat.st_size = old_fattr.fattr.file_stat.st_size;
		      fattr.fattr.file_stat.st_atime = old_fattr.fattr.file_stat.st_atime;
		      fattr.fattr.file_stat.st_mtime = old_fattr.fattr.file_stat.st_mtime;
		      fattr.fattr.file_stat.st_ctime = old_fattr.fattr.file_stat.st_ctime;
		      fattr.fattr.file_stat.st_blksize = old_fattr.fattr.file_stat.st_blksize;
		      fattr.fattr.parent_ino = old_fattr.fattr.parent_ino;
		      fattr.fattr.fname_bytes = old_fattr.fattr.fname_bytes;

		      flmemcpy( &blk->data[data_idx + sizeof( old_fattr )], 
			       (char *) name, 
			       fattr.fattr.fname_bytes );
		    }
    
                    ret_val = create_file( &fattr.fattr, 
                                           &restore_fd, 
                                           name, 
                                           restore_fname,
                                           file_cnt,
                                           compressed_data, 
                                           lzwh );
		    /* 
		     * create_file returns OKAY
		     * when List is performed AND
		     * when a file is indeed created or can be overwritten
		     * If a file already exists and cannot be overwritten
		     * then 1 is returned
		     */
                    if (ret_val != OKAY) {
                        if (ret_val == 1) {
                            overwrite = 0;
                        } else {
                            state = STATE_TRANSITION( state, RS_ERROR );
                        }
                    } else {
                        overwrite = 1;
                    }
                    break;

                  case RT_FILE_DATA:           /* file data record       */
                    state = STATE_TRANSITION( state, RS_FILE_DATA );

                    if (List) {
                            list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE34, "file data"), &rhdr.rheader,
				      stdout);
                    } else {
			if ((overwrite) && (state == RS_FILE_DATA)) {
                            uint64_t d_offset;
                            d_offset = rhdr.rheader.d_offset_lo;

                            ret_val = restore_file_data( 
                                                restore_fd,
                                                rhdr.rheader.size, 
                                                rhdr.rheader.flags, 
                                                d_offset, 
                                                total_bytes_restored,
                                                blk, 
                                                data_idx,
                                                compressed_data, 
                                                lzwh );
                            if (ret_val != OKAY) {
                                state = STATE_TRANSITION( state, RS_ERROR );
                            }
                        }
                    }
                    break;

                  case RT_BF_FILE_ATTR:       /* bitfile attributes record */
                      /*  On Tru64, this was used only for striped files. 
                       *  Since striped files are not supported on HPUX, this
                       *  case is basically a no-op, but remains here to 
                       *  effect the required state transition when restoring
                       *  dumps made on Tru64.
                       */
                      state = STATE_TRANSITION( state, RS_BF_FILE_ATTR );

                      break;

                  case RT_FILLER:              /* filler record          */
                    /* not consider a state transition */
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE22, 
                            "filler"), &rhdr.rheader, stdout);
                    }
                    eob = TRUE;
                    break;
    
                  case RT_END_OF_VOL_SET:      /*  end of vol set record */
                    state = STATE_TRANSITION( state, RS_END_OF_VOL_SET );
                    if (List) {
                        list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE23, 
                            "end of save-set"), &rhdr.rheader, stdout);
                    }
                    eovs = TRUE;
                    break;
    
                  case RT_FQUOTA:      		/* Fileset quota record */
                    state = STATE_TRANSITION( state, RS_FQUOTA );

                    /*
                     * This is a fileset quota record.  What we do with
                     * it depends on several things:
                     * 
                     * 1. If the user wishes only to see what files are in 
                     *    the saveset (Show is TRUE) then just skip this 
                     *    record as it does not represent a file.
                     * 2. If the user wishes to do a real restore (List is
                     *    FALSE) and the destination file system is not
                     *    an AdvFS fileset, then skip this record since 
                     *    only AdvFS filesets have fileset quotas.
                     * 3. If the user wishes to do a real restore to an
                     *    AdvFS fileset or wishes to see the contents of
                     *    the saveset (List is TRUE) but is not root,
                     *    print out a warning.
                     * 4. Finally, if the user wishes to do a real restore
                     *    to an AdvFS fileset or wishes to see the contents
                     *    of the saveset and he is root, proceed as requested.
		     * 5. User may have specified -Q Quota_ignore.
                     */

                    if ((Show) || 
                        (!List && (strcmp(Fs_info.fs_type, MNTTYPE_ADVFS) != 0))
                       ) 
                        ; /* Do nothing; see note above. */
                    else if (!Quota_ignore) {

		      if (!user_is_root) {
                         if (!qwarned_already) {
                            fprintf(stderr, catgets(catd, S_VRESTORE1, 
                                VRESTORE172, 
                                "%s: Must be root user to restore quota information.\n"), Prog);
                            qwarned_already = TRUE;
                         }
                      } else {
                        if (List) {
                            ret_val = restore_quota_data(rhdr.rheader.type,
                                                         &blk->data[data_idx],
                                                         CLOSED,
                                                         NULL,
                                                         file_cnt,
                                                         total_bytes_restored
                                                         );
                            if (ret_val != OKAY)
                                state = STATE_TRANSITION( state, RS_ERROR );
                        } else {
                            /*
                             * Get the fileset quotas for the target fileset.
                             */
                            ret_val = advfs_get_bfset_params(
                                                          Fs_info.advfs_set_id,
                                                          &oldSetParams
                                                           );
                            if (ret_val != OKAY) {
                                state = STATE_TRANSITION( state, RS_ERROR );
                                fprintf(stderr, 
			                catgets(catd, S_VRESTORE1, VRESTORE175, "%s: Could not obtain fileset parameters: [%d] %s\nFileset quotas cannot be restored.\n"),
			                Prog, errno, ERR_MSG);
                            } else {
                                /*
                                 * See if the fileset quotas in the saveset
                                 * differ from those in the target fileset.
                                 * If so, overwrite the existing fileset 
                                 * quotas if the user wants to do so.
                                 */
                                struct fquota_t *fqp = (struct fquota_t *)
                                                       &blk->data[data_idx];
                                if (oldSetParams.blkHLimit != fqp->blkHLimit ||
                                   oldSetParams.blkSLimit != fqp->blkSLimit || 
                                   oldSetParams.fileHLimit != fqp->fileHLimit ||
                                   oldSetParams.fileSLimit != fqp->fileSLimit ||
                                   oldSetParams.blkTLimit != fqp->blkTLimit || 
                                   oldSetParams.fileTLimit != fqp->fileTLimit) {
                                    if (!check_overwrite(FQUOTAS)) {
                                        ret_val = restore_quota_data(
                                                         rhdr.rheader.type,
                                                         fqp,
                                                         CLOSED,
                                                         &oldSetParams,
                                                         file_cnt,
                                                         total_bytes_restored
                                                         );
                                            if (ret_val != OKAY)
                                                 state = STATE_TRANSITION( state, RS_ERROR );
                                    }
                                }
                            }
                         }
                      }
                    }
                    break;
    
                  case RT_UQUOTA32:    		/* User quota record */
                  case RT_UQUOTA64:    		/* User quota record */
                    state = STATE_TRANSITION( state, RS_UQUOTA );
                    if ( !Quota_ignore && restore_uquotas) {
                        ret_val = restore_quota_data(rhdr.rheader.type,
                                                     &blk->data[data_idx],
                                                     uq_fd,
                                                     &oldSetParams,
                                                     file_cnt,
                                                     total_bytes_restored
                                                     );
                        if (ret_val != OKAY)
                            state = STATE_TRANSITION( state, RS_ERROR );
                    }
                    break;
    
                  case RT_GQUOTA32:    		/* Group quota record */
                  case RT_GQUOTA64:    		/* Group quota record */
                    state = STATE_TRANSITION( state, RS_GQUOTA );
                    if ( !Quota_ignore && restore_gquotas) {
                        ret_val = restore_quota_data(rhdr.rheader.type,
                                                     &blk->data[data_idx],
                                                     gq_fd,
                                                     &oldSetParams,
                                                     file_cnt,
                                                     total_bytes_restored
                                                     );
                        if (ret_val != OKAY)
                            state = STATE_TRANSITION( state, RS_ERROR );
                    }
                    break;
    
                  default:                     /* oops!!                 */
                    state = STATE_TRANSITION( state, RS_ERROR );
                    list_rec_hdr( catgets(catd, S_VRESTORE1, VRESTORE24, 
                        "unknown/unexpected"), &rhdr.rheader, stderr);
                    break;
                }
                break;

              default:
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE35, 
                        "%s: unexpected mode [%d]; terminating\n"), 
                        Prog, mode );
                break;
            }

            /* position index to next record header */
            data_idx += rhdr.rheader.size;
        } /* end while */

        rblk_free( rblkh, blk_id ); /* release the current block */
        block_num++;
    } /* end while */

    if (eovs) {
        /* we still need to read the end-of-file marker */
        if (Dev_type == TAPE && Rewind) {
            int st = 0;

	    struct mtop t_op = { MTFSF, 1 }; /* MTFSF forward to next file */

	    /*
	     * Use ioctl to find eof marker, instead of reading 1 byte.
	     */
            if (remote_host_name) {
	        st = rmtioctl(MTFSF,1);
            } else {
	        st = ioctl( Src_fd, (int) MTIOCTOP, (char *) &t_op);
            }

            if (st < 0) {
	        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE227, 
			"%s: unable to forward tape to next file; [%d] %s\n"), 
		         Prog, errno, ERR_MSG );
	    }
        }
    } else if (0 == Abort_restore) {
        state = STATE_TRANSITION( state, RS_ERROR );
    }

    if (compressed_data) {end_uncompress( lzwh );}

    if (uq_fd != CLOSED)
        close(uq_fd);

    if (gq_fd != CLOSED)
        close(gq_fd);

    logical_dir_close( dir_fd );
}

/* end restore */

/*
 * Function:
 *
 *      usage
 *
 * Function description:
 *
 *      Displays the command line usage of Restore.
 */
void
usage( )

{
    fprintf( stderr, "\n" );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE36, "Usage:\n\n"));
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE37, "%s -h\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE38, "%s -V\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE39, "%s -t [-f device]\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE40, "%s -l [-Q] [-f device]\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE41, "%s -i [-mqQv] [-f device] [-D pathname] [-o opt]\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE42, "%s -x [-mqQv] [-f device] [-D pathname] [-o opt] [filename ...]\n"), Prog);
    if (strcmp(Prog, RPROG) == 0) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE222, "\twhere the device is specified as hostname:device.\n") );
    }
    fprintf( stderr, "\n" );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE43, "Options:\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE44, "   -D <pathname>\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE45, "\tSpecifies the destination path of where to restore\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE46, "\tthe files.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE47, "   -V \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE48, "\tDisplays the current %s version.\n"), Prog );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE241, "   -Q \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE242, "\tSpecifies that quota files should not be restored.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE49, "   -f <device> \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE50, "\tSpecifies the save set device or file.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE51, "   -h \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE52, "\tDisplays usage help for %s.\n"), Prog);
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE53, "   -i \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE54, "\tRuns the interactive shell to select files.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE55, "   -l \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE56, "\tLists the save set structure.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE57, "   -m \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE58, "\tDoes not preserve owner, group, or modes from device.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE59, "   -o <yes | no | ask> \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE60, "\tSpecifies the action to take when file already exists.\n"));
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE61, "\tThe default is 'yes' (overwrite existing files).\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE62, "   -q \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE63, "\tDoes not display unnecessary, informative messages.\n"));
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE68, "   -t \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE69, "\tShow the files in the save set.\n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE70, "   -v \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE71, "\tDisplays the names of files being restored.\n"));
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE72, "   -x [other options] [filename ...] \n") );
    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE73, "\tSpecifies the files to restore.\n") );
    fprintf( stderr, "\n" );

}

/* end usage */

/*
 * Function:
 *
 *      list_blk_hdr
 *
 * Function description:
 *
 *      Displays the contents of a save-set block header for the
 *      list (-l) option.
 */
void
list_blk_hdr(
             struct bheader_t *bhdr,  /* in - ptr to block header */
	     FILE * std
             )
{
    time_t tmptime;                    /* used to convert saved 32 bit value
                                        * of time in saveset header to the 64
                                        * bit time_t HPUX requires.
                                        */

    if (bhdr->flags & BF_XOR_BLOCK) {
        fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE74, "\nXOR BLOCK HEADER\n") );
    } else {
        fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE75, "\nBLOCK HEADER\n") );
    }

    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE76, " block size : %d\n"), bhdr->block_size );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE77, " flags      : ") );
    if (bhdr->flags == 0) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE78, "none") );}
    if (bhdr->flags & BF_COMPRESSED) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE79, "<compressed>") );}
    if (bhdr->flags & BF_XOR_BLOCK) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE80, "<XOR block>") );}
    if (bhdr->flags & BF_FILES) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE81, "<files>") );}
    if (bhdr->flags & BF_DIRECTORIES) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE82, "<dirs>") );}
    if (bhdr->flags & BF_QUOTA_DATA) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE176, "<quotas>") );}
    if (bhdr->flags & BF_USER_QUOTA_DATA) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE225, "<user quotas>") );}
    if (bhdr->flags & BF_GROUP_QUOTA_DATA) {fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE226, "<group quotas>") );}
    fprintf( std, "\n" );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE83, " volume set : %d\n"), bhdr->vol_set_num );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE84, " volume sets: %d\n"), bhdr->vol_sets );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE85, " volume num : %d\n"), bhdr->volume_num );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE86, " block num  : %d\n"), bhdr->block_num );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE87, " header CRC : 0x%04x\n"), bhdr->header_crc );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE88, " block CRC  : 0x%04x\n"), bhdr->block_crc );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE89, " xor blocks : %d\n"), bhdr->xor_blocks );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE90, " xor blk num: %d\n"), bhdr->xor_block_num );
    tmptime = bhdr->ss_id;
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE91, " id         : %s"), ctime( &tmptime));
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE92, " version    : %s\n"), bhdr->version );
}

/* end list_blk_hdr */


/*
 * Function:
 *
 *      dump_blk_hdr
 *
 * Function description:
 *
 *      Displays the contents of a save-set block header.
 */

void
dump_blk_hdr(
             struct bheader_t *bhdr  /* in - ptr to block header */
             )
{
    int i, *ip = (int *) bhdr;

    list_blk_hdr( bhdr , stderr );

    fprintf( stderr, "\n" );

    for (i = 0; i < BLOCK_HEADER_SIZE / 4; i += 4) {
        fprintf( stderr, "%4d 0x%08x 0x%08x 0x%08x 0x%08x\n",
                 i, ip[ i ], ip[ i + 1 ], ip[ i + 2 ], ip[ i + 3 ] );
    }

    fprintf( stderr, "\n" );
}

/* end dump_blk_hdr */


/*
 * Function:
 *
 *      list_rec_hdr
 *
 * Function description:
 *
 *      Displays the contents of a save-set record header for the
 *      list (-l) option.
 */
void
list_rec_hdr(
             char *msg,               /* in */
             struct rheader_t *rhdr,  /* in - ptr to record header */
	     FILE * std
             )
{
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE93, 
        "\nRECORD HEADER (%s)\n"), msg );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE94, 
        " record type  : %d\n"), rhdr->type );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE96, 
        " data offset  : %d\n"), rhdr->d_offset_lo );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE97, 
        " record size  : %d\n"), rhdr->size );
    fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE98, 
        " flags        : ") );
    if (rhdr->flags == 0) {
        fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE99, "<none>") );
    }
    if (rhdr->flags == RF_DATA_CONTINUED) {
        fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE100, 
            "<data-continued>") );
    }

    if (rhdr->flags == RF_SPAN_RECORD) {
        fprintf( std, catgets(catd, S_VRESTORE1, VRESTORE209, 
            "<data-split>") );
    }
    
    fprintf( std, "\n" );
}

/* end list_rec_hdr */


/*
 * Function:
 *
 *      list_file_attr
 *
 * Function description:
 *
 *      Displays the contents of a file attributes record for the
 *      list (-l) option.
 */
void
list_file_attr(
               struct fattr_t *fattr,  /* in - ptr to file attributes */
               char *file_name         /* in - file name */
               )
{
    char *cp;

    fprintf( stdout, " --\n" );

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE101B, 
            " file name : "));


        for (cp = file_name; *cp !='\0'; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        putc('\n', stdout);
    } else {
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE101A, 
            " file name : %s\n"),
file_name  );

    }

    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE102, 
        " file inode: %d\n"), fattr->file_stat.st_ino  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE103, 
        " file mode : 0%o\n"), fattr->file_stat.st_mode  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE104, 
        " file owner: %d\n"), fattr->file_stat.st_uid  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE105, 
        " file group: %d\n"), fattr->file_stat.st_gid  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE106, 
        " file links: %u\n"), fattr->file_stat.st_nlink  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE107, 
        " file size : %ld\n"), 
        ((int64_t)(fattr->file_stat.st_blocks)) * fattr->file_stat.st_blksize);
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE108, 
        " parent ino: %d\n"), fattr->parent_ino  );
}

/* end list_file_attr */


/*
 * Function:
 *
 *      show_file_attr
 *
 * Function description:
 *
 *      Displays the contents of a file attributes record for the
 *      show (-t) option.
 */
void
show_file_attr(
               struct fattr_t *fattr,  /* in - ptr to file attributes */ 
               char *file_name         /* in - file name */
               )
{
    char *cp;

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        for (cp = file_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        fprintf( stdout, ", %ld\n", 
            ((int64_t)(fattr->file_stat.st_blocks)) * fattr->file_stat.st_blksize);
    } else {
        fprintf( stdout, "%s, %ld\n", file_name, 
            ((int64_t)(fattr->file_stat.st_blocks)) * fattr->file_stat.st_blksize);
    }
}

/* end show_file_attr */

/*
 * Function:
 *
 *      list_hard_link_attr
 *
 * Function description:
 *
 *      Displays the contents of a hard link attributes record for the
 *      list (-l) option.
 */
void
list_hard_link_attr(
    struct hlink_t *hlink,  /* in - ptr to hard link attributes */
    char *file_name,        /* in - file name */
    char *link_name         /* in - link name */
    )
{
    char *cp;

    fprintf( stdout, " --\n" );

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE109B,
                                           "      link name : "));
        for (cp = link_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        fprintf( stdout, " --> ");
        for (cp = file_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        putc('\n', stdout);
    } else {
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE109A,
            "      link name : %s --> %s\n"), link_name, file_name );

    }

    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE110, 
        "     link inode : %d\n"), hlink->file_ino  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE111, 
        " link parent ino: %d\n"), hlink->link_parent_ino  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE112, 
        " file parent ino: %d\n"), hlink->file_parent_ino  );
}
/* end list_hard_link_attr */

/*
 * Function:
 *
 *      show_hard_link_attr
 *
 * Function description:
 *
 *      Displays the contents of a hard link attributes record for the
 *      show (-t) option.
 */

void
show_hard_link_attr(
    struct hlink_t *hlink,  /* in - ptr to hard link attributes */
    char *file_name,        /* in - file name */
    char *link_name         /* in - link name */
    )
{
    char *cp;

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        for (cp = link_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        fprintf( stdout, " --> ");
        for (cp = file_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        putc('\n', stdout);
    } else {
        fprintf( stdout, "%s --> %s\n", link_name, file_name );
    }
}

/* end show_hard_link_attr */


/*
 * Function:
 *
 *      list_sym_link_attr
 *
 * Function description:
 *
 *      Displays the contents of a sym link attributes record for the
 *      list (-l) option.
 */
void
list_sym_link_attr(
    struct slink_t *slink,  /* in - ptr to sym link attributes */
    char *link_name,        /* in - link name */
    char *link              /* in - link */
    )
{
    char *cp;

    fprintf( stdout, " --\n" );

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE113B,
            " link name : "));
        for (cp = link_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        fprintf( stdout, " @-> ");
        for (cp = link; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        putc('\n', stdout);
    } else {
         fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE113A, 
             " link name : %s @-> %s\n"), link_name, link  );

    }

    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE114, 
        "     inode : %d\n"), slink->ino  );
    fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE108, 
        " parent ino: %d\n"), slink->parent_ino  );
}
/* end list_sym_link_attr */

/*
 * Function:
 *
 *      show_sym_link_attr
 *
 * Function description:
 *
 *      Displays the contents of a sym link attributes record for the
 *      show (-t) option.
 */

void
show_sym_link_attr(
    struct slink_t *slink,  /* in - ptr to sym link attributes */
    char *link_name,        /* in - link name */
    char *link              /* in - link */
    )
{
    char *cp;

    if (Stdout_to_tty) {
        /*
         * print non-printable characters as '?' when going to tty
         */
        for (cp = link_name; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        fprintf( stdout, " @-> ");
        for (cp = link; *cp !='\0' ; cp++)
            putc(isprint(*cp) ? *cp : '?', stdout);
        putc('\n', stdout);
    } else {
        fprintf( stdout, "%s @-> %s\n", link_name, link );
    }
}

/* end show_sym_link_attr */


char *
cvtstoa(time_t time)
{
    static char buf[20];

    if (time >= (24 * 60 * 60)) {
        time /= 24 * 60 * 60;
        sprintf(buf, "%d %s", time, time == 1 ? 
                catgets(catd, S_VRESTORE1, VRESTORE177, "day") : 
                catgets(catd, S_VRESTORE1, VRESTORE178, "days"));
    } else if (time >= (60 * 60)) {
        time /= 60 * 60;
        sprintf(buf, "%d %s", time, time == 1 ? 
                catgets(catd, S_VRESTORE1, VRESTORE179, "hour") : 
                catgets(catd, S_VRESTORE1, VRESTORE180, "hours"));
    } else if (time >= 60) {
        time /= 60;
        sprintf(buf, "%d %s", time, time == 1 ? 
                catgets(catd, S_VRESTORE1, VRESTORE181, "minute") : 
                catgets(catd, S_VRESTORE1, VRESTORE182, "minutes"));
    } else
        sprintf(buf, "%d %s", time, time == 1 ? 
                catgets(catd, S_VRESTORE1, VRESTORE183, "second") : 
                catgets(catd, S_VRESTORE1, VRESTORE184, "seconds"));
    return (buf);
}


/*
 * Function:
 *
 *      list_quota_attr
 *
 * Function description:
 *
 *      Displays the contents of a quota record for the list (-l) option.
 */
void
list_quota_attr(
    record_type_t type,
    void *data
    )
{
    struct ugquota64_t *ugp64;
    struct fquota_t  *fp;

    fprintf( stdout, " --\n" );
    if (type == RT_FQUOTA) {
        fp = (struct fquota_t *)data;
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE185, 
                " Block hard limit : %ld\n"), fp->blkHLimit);
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE186, 
                " Block soft limit : %ld\n"), fp->blkSLimit);
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE187, 
                " File hard limit : %ld\n"), fp->fileHLimit);
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE188, 
                " File soft limit : %ld\n"), fp->fileSLimit);
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE189,
                " Block grace period : %s\n"), 
                cvtstoa(fp->blkTLimit));
        fprintf(stdout, 
                catgets(catd, S_VRESTORE1, VRESTORE190,
                " File grace period : %s\n"), 
                cvtstoa(fp->fileTLimit));
    }
    else if ((type == RT_UQUOTA64) || (type == RT_GQUOTA64)) {

        ugp64 = (struct ugquota64_t *)data;
        if (type == RT_UQUOTA64)
            fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE191,
                    " User ID : %d\n"), ugp64->id);
        else
            fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE192,
                    " Group ID : %d\n"), ugp64->id);

        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE233,
                " Block hard limit : %lu\n"), ugp64->dqb_bhardlimit);
        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE234,
                " Block soft limit : %lu\n"), ugp64->dqb_bsoftlimit);
        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE195,
                " File hard limit : %d\n"), ugp64->dqb_fhardlimit);
        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE196,
                " File soft limit : %d\n"), ugp64->dqb_fsoftlimit);
        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE197,
                " Block grace period : %s\n"), 
                cvtstoa(ugp64->dqb_btimelimit));
        fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE198,
                " File grace period : %s\n"), 
                cvtstoa(ugp64->dqb_ftimelimit));
    }
}
/* end list_quota_attr */

/*
 * Function:
 *
 *      get_save_set_attr
 *
 * Function description:
 *
 *      Reads the first block chunk of the save-set and determines the
 *      reliability type (R_NONE or R_CRC_AND_XOR) of the save-set.
 *      A side benefit of this routine is that it also determines whether 
 *      or not a valid save-set is being read.
 *
 *      The first block of a save-set contains only information
 *      about the save-set (ie - a summary record).  So, if this
 *      routine is successfull, it will have the save-set possitioned
 *      on the second block.
 *
 *      Data in the summary record includes the "source pathname" for
 *      the save set, as well as a flag in the struct indicating
 *      whether pathname data are present. If a pathname is present,
 *      it is printed out.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */

int
get_save_set_attr( 
    int s_fd,                   /* in - saveset file desc */
    uint32_t *blk_size,         /* out - save-set block size */
    int *xor_blks,              /* out - num blks in xor grp */
    reliability_t *reliability, /* out - reliable type */
    int *compressed_data        /* out - compressed flag */
    )
{
    struct blk_t blk;
    int rcnt;
    int rsize;
    int data_idx = 0;                  /* Index ptr for finding rec data  */
    union record_header_t   *rhdrp;    /* Ptr to read summary rec header */
    union summary_rec_t     *srecp;    /* Ptr to read summary rec data   */
    int nchunks_to_read = 2;           /* Enough to read new summary rec */
    int fixed_blksize = 0;	       /* Flag to indicate tape type     */
    read_prop_type devinfo;            /* Struct for tape characteristics */
    time_t tmptime;                    /* used to convert saved 32 bit value
                                        * of time in saveset header to the 64
                                        * bit time_t HPUX requires.
                                        */

    /*----------------------------------------------------------------------*/

    /*
     * If this is a tape, we first need to find out if this is a
     * fixed-blocksize tape or a variable blocksize tape.  A fixed
     * blocksize tape acts like a disk as far as we're concerned.
     */

    if (Dev_type == TAPE) {
        if (ioctl(s_fd, MT_READ_PROPERTY, &devinfo) >= 0) {
            if (devinfo.read_prop.fixed_block_enabled) {
                fixed_blksize = 1;
            }
        } 
    }
    
    if (Dev_type == TAPE && fixed_blksize == 0) {
        rsize = MAX_CHUNKS * BLK_CHUNK_SZ;
    } else {
        rsize = nchunks_to_read * BLK_CHUNK_SZ;
    }

    if (remote_host_name) {
        rcnt = rmtread( (char *) &blk, rsize );
    } else { 
        rcnt = read( s_fd, (char *) &blk, rsize );
    }

    if ( (rcnt == ERROR) ||
         ((Dev_type != TAPE || fixed_blksize == 1) && (rcnt != rsize)) ) {

        /*
         ** Read error.  Fail.
         */
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE161, 
                        "%s: unable to read from save-set; [%d] %s\n"),
                        Prog, errno, ERR_MSG );
        return ERROR;
    } 

    /*  Initialize reliability */
     
     *reliability = R_UNKNOWN;

    /* 
     * Must compare the version of advrestore to the advdump version of
     * the saveset to ensure upward compatibility only.  Version is used
     * for backward compatibility (struct stat), restoring Tru64 dumps, etc.
     */
    Version = atof( blk.bhdr.bheader.version );

    if (Version > atof( VERSION_STRING )) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE118, 
            "%s: Need %s V%s to restore contents; terminating\n"),
	    Prog, Prog, blk.bhdr.bheader.version);
        return ERROR;
    } 

    if (hdr_crc_ok( &blk )) {

        if (blk.bhdr.bheader.block_size > 0) {

            /*
             ** The block header is valid.
             */
    
            if (blk.bhdr.bheader.block_crc != 0) {
                /*
                 ** Since the 'block_crc' is not zero we know that
                 ** the save-set does have XOR blocks and CRCs
                 ** per block.
                 */
                *reliability = R_CRC_AND_XOR;
                *xor_blks    = blk.bhdr.bheader.xor_blocks;
    
            } else {
                *reliability = R_NONE;
            }
    
            *blk_size = blk.bhdr.bheader.block_size;
            *compressed_data = blk.bhdr.bheader.flags & BF_COMPRESSED;
        }

    } 

    if (*reliability == R_UNKNOWN) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE115, 
            "%s: unable to use save-set; invalid or corrupt format\n"), Prog );
        return ERROR;
    }

    /* display the dump date */

    tmptime = blk.bhdr.bheader.dump_date.tv_sec;

    silent_fprintf( catgets(catd, S_VRESTORE1, VRESTORE119, "%s: Date of the advdump save-set: %s"),
                    Prog, ctime(&tmptime) );

    /* Display the save-set source directory */

    rhdrp = (union record_header_t *)blk.data;

    if (Version < 6.0) {
        /*  more endian-swapping */
        rhdrp->rheader.type = SWAP32(rhdrp->rheader.type);
        rhdrp->rheader.d_offset_lo = SWAP32(rhdrp->rheader.d_offset_lo);
        rhdrp->rheader.size = SWAP16(rhdrp->rheader.size);
        rhdrp->rheader.flags = SWAP16(rhdrp->rheader.flags);
        rhdrp->rheader.d_offset_hi = SWAP32(rhdrp->rheader.d_offset_hi);
    }

    if ( rhdrp->rheader.type == RT_SUMMARY ) {

        data_idx += sizeof( union record_header_t );
        srecp = (union summary_rec_t *)&blk.data[data_idx];

        if (Version < 6.0) {
            /* and a little more swapping */
            srecp->summary.pathname_present = 
                SWAP32(srecp->summary.pathname_present);
            srecp->summary.not_used2 = SWAP32(srecp->summary.not_used2);
            srecp->summary.not_used3 = SWAP32(srecp->summary.not_used3);
            srecp->summary.not_used4 = SWAP32(srecp->summary.not_used4);
        }

        if ( srecp->summary.pathname_present == 1 ) {   /* If not, do nothing */
            if ( srecp->summary.source_dir != NULL ) {                /* Good */
                silent_fprintf( catgets(catd, S_VRESTORE1, VRESTORE224,
                                    "%s: Save-set source directory: %s\n"),
                                     Prog, srecp->summary.source_dir );
            }
            else {                                                     /* Bad */
                fprintf( stderr,
                     catgets(catd, S_VRESTORE1, VRESTORE115, 
                     "%s: unable to use save-set; invalid or corrupt format\n"),
                     Prog );
                return ERROR;
            }
        }
    }
    else {                 /* Not a summary rec - something seriously wrong */
        fprintf( stderr,
                 catgets(catd, S_VRESTORE1, VRESTORE115, 
                 "%s: unable to use save-set; invalid or corrupt format\n"),
                 Prog );
        return ERROR;
    }

    if (List) {
        list_blk_hdr( &blk.bhdr.bheader , stdout);
    }

    if (Dev_type != TAPE || fixed_blksize == 1) {
        /*
         ** Everything is cool so skip over enough block chunks to
         ** position ourselves at the second block of the save-set.
         */
        int remaining_chunks = *blk_size / BLK_CHUNK_SZ - nchunks_to_read;

        if (remaining_chunks > 0) {
            rcnt = read_buf( s_fd,
                             (char *) &blk,
                             BLK_CHUNK_SZ * remaining_chunks );
        }
    }

    return OKAY;
}
/* end get_save_set_attr */

/*
 * mkpath -
 *
 * Given a file_name, this routine uses mkdir() to make sure that the
 * path upto the last component of the file_name exists.  For example, if
 * file_name is "a/b/c/d", mkpath() will ensure that the path "a/b/c"
 * exists.
 */
int
mkpath(
       char *file_name
       )
{
    char path[FILE_NAME_SIZE];
    char *token;
    char *slash;
    char *delimiter = "/";
    int status, retry;
    char *strtok_buf;

    slash = strrchr( file_name, '/' );

    if ((slash == NULL) || (slash == file_name)) {
        return -1;
    }   
    slash[0] = '\0';
    strcpy( path, file_name );
    slash[0] = '/';

    token = strtok( path, delimiter );

    do {
        retry = 0;

        status = mkdir( path, 0777 );

        if (status != 0) {
            if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                        Prog, Target, errno, ERR_MSG );
                if (want_abort())
                    abort_now();
                retry = 1;
    
            } else if (errno != EEXIST) {
                fprintf( stderr, 
                         catgets(catd, S_VRESTORE1, VRESTORE120, 
                         "%s: unable to create directory <%s>; [%d] %s\n"),
                         Prog, path, errno, ERR_MSG );
                return -1;
            }
        }
    } while (retry);

    while ((token = strtok( NULL, delimiter )) != NULL) {
        *(--token) = '/';

        do {
            retry = 0;

            status = mkdir( path, 0777 );

            if (status != 0) {
                if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                    fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                            Prog, Target, errno, ERR_MSG );
                    if (want_abort())
                        abort_now();
                    retry = 1;
    
                } else if (errno != EEXIST) {
                    fprintf( stderr, 
                            catgets(catd, S_VRESTORE1, VRESTORE120, 
                            "%s: unable to create directory <%s>; [%d] %s\n"),
                            Prog, path, errno, ERR_MSG );
                    return -1;
                }
            }
        } while (retry); 
    }

    return 0;
}
/* end mkpath */

/*
 * check_overwrite - 
 *
 * Given the name of the file to restore when the file already exists,
 * check_overwrite checks the Overwrite flag and returns -1 if it shouldn't
 * overwrite the existing file and 0 if it should overwrite the existing 
 * file.
 */

int
check_overwrite (char *file_name)
{
    char ow_answer[10];          /* contains overwrite answer */
    int not_valid = 1;           /* assume overwrite answer isn't valid */
    int fset_quotas = FALSE;     /* TRUE if file_name is fileset quotas */

    fset_quotas = (strcmp(file_name, FQUOTAS) ? FALSE : TRUE);
    if (Overwrite == OW_YES) {
        return 0;

    } else if (Overwrite == OW_NO) {
        if (fset_quotas) {
            fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE199,
                    "%s: As requested, fileset quotas not restored.\n"),
                    Prog);
        }
        else {
            fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE121, 
                "%s: %s not restored; file already exists\n"),
                Prog, file_name);
        }
        return -1;

    } else {                        /* ask user to overwrite or not */
        while (not_valid) {         /* answer must be yes or no */
            pthread_mutex_lock(&tty_mutex);
            if (fset_quotas) {
                fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE200,
                "%s: Fileset quotas in saveset differ with those in use; overwrite? "), Prog);
            }
            else {
                fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE122, 
                    "%s: <%s> already exists; overwrite? "),
                    Prog, file_name);
            }

            /*
             * We need an answer from the user.  If there is no way to
             * get one (Local_tty is NULL), then abort the job.  
             */
            if (Local_tty) {
                if (fgets (ow_answer, 6, Local_tty) == NULL) {
                    pthread_mutex_unlock(&tty_mutex);
                    abort_prog( catgets(catd, S_VRESTORE1, VRESTORE170, 
                        "%s: user interaction required and no tty present\n"), 
                        Prog );
                }
            }
            else {
                pthread_mutex_unlock(&tty_mutex);
                abort_prog( catgets(catd, S_VRESTORE1, VRESTORE170, 
                    "%s: user interaction required and no tty present\n"), 
                    Prog );
            }

            pthread_mutex_unlock(&tty_mutex);

            if (ow_answer[0] == 'y') {
                return 0;

            } else if (ow_answer[0] == 'n') {
                if (fset_quotas) {
                    silent_fprintf(catgets(catd, S_VRESTORE1, VRESTORE201,
                        "%s: Fileset quotas not restored.\n"), Prog);
                }
                else {
                    silent_fprintf(catgets(catd, S_VRESTORE1, VRESTORE123, 
                        "%s: %s not restored.\n"), Prog, file_name);
                }
                return -1;

            } else {
                fprintf(stderr, "\n");
            }
        }
    }

}  /* end check_overwrite */

/*
 * Function:
 *
 *      close_file
 *
 * Function description:
 *
 *      This routine closes the file that has just been restored.
 *
 * Return value:
 *
 *      ERROR - If an error occurred.
 *      OKAY  - If the function completed successfully
 */

int
close_file( 
    struct fattr_t *fattr, /* in - File attributes of file to be restored */
    int *fd,               /* in - in/out file descriptor */
    char *file_name,       /* in - name of restored file */
    int compressed,        /* in - flag if compressed */
    int bad_file,          /* in - flag if not complete */
    lzw_desc_handle_t lzwh /* in - lzw desc tbl handle */
    )

{
    int uc_cnt;                      /* bytes returned by finish_uncompress */
    int wcnt, retry;
    char buf[MAX_BLK_SZ];
    struct timeval file_times[3];
    char *restore_name = NULL;
    struct stat newFstat;
    int zeroByte = 0;
 
    /*----------------------------------------------------------------------*/

    if (compressed) {
        /*
         * if the data was compressed then get any remaing uncompressed bytes
         * and write them to the restored file
         */
        uc_cnt = finish_uncompress( lzwh, buf );

        if (!bad_file) {
            do {
                retry = 0;

                wcnt = write( *fd, buf, uc_cnt );

                if (wcnt != uc_cnt) {
                    if ((wcnt >= 0) || 
                        (errno == ENOSPC) || 
                        (errno == EDQUOT)) {     

                        if (wcnt >= 0) {
                            fprintf(stderr, catgets(catd, S_VRESTORE1, 
                                VRESTORE124, "\n%s: <%s>; Out of disk space\n"),
                                Prog, Target );
                        } else {
                            fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                                    Prog, Target, errno, ERR_MSG );
                        }
                        if (want_abort())
                            abort_now();
                        uc_cnt -= wcnt;
                        retry = 1;

                    } else {
                        bad_file = TRUE;
                    }
                }
            } while (retry);
        }
    }

    if (fstat( *fd, &newFstat ) < 0) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE125, 
            "%s: close file fstat(%s) failed; [%d] %s\n"),
            Prog, file_name, errno, ERR_MSG );
    } else {
        if (newFstat.st_size < fattr->file_stat.st_size ) {
            if (Debug) {
                fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE126, 
                    "%s: extending file <%s> from %li to %li\n"), 
                    Prog, file_name, newFstat.st_size, 
                    fattr->file_stat.st_size );
            }

            if (lseek( *fd, fattr->file_stat.st_size - 1, SEEK_SET ) < 0) {
                fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE127, 
                    "%s: close file lseek(%s, %li) failed; [%d] %s\n"),
                    Prog, file_name, fattr->file_stat.st_size - 1, errno, 
                    ERR_MSG );
            } else {
                write( *fd, &zeroByte, 1 );
            }
        }
    }

    if(close( *fd ) < 0)
    {
        fprintf(stderr,
             catgets(catd, S_VRESTORE1, VRESTORE240,
                     "\n%s: unable to properly close file <%s>; [%d] %s\n"),
                     Prog, file_name, errno, ERR_MSG );
    }
    *fd = CLOSED;

    /*
     * If we are in the process of aborting, just remove the temporary
     * file and return. Otherwise unnecessary error messages may be printed.
     */

    if (1 == Abort_restore) {
        remove( Tmp_fname );
        return OKAY;
    }
        
    if (bad_file) {
        char file_name_incomplete[MAXPATHLEN];

        strcpy( file_name_incomplete, file_name );
        strcat( file_name_incomplete, ".INCOMPLETE" );

        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE128, 
            "%s: incomplete file <%s> restored as <%s>\n"), 
            Prog, file_name, file_name_incomplete );

        restore_name = file_name_incomplete;

	File_incomplete_error = TRUE;
    } else {
        restore_name = file_name;
    }

    if (Preserve_modes) {
        chown( Tmp_fname,
               fattr->file_stat.st_uid, fattr->file_stat.st_gid );

        file_times[0].tv_sec = fattr->file_stat.st_atime;
        file_times[0].tv_usec = 0;
        file_times[1].tv_sec = fattr->file_stat.st_mtime;
        file_times[1].tv_usec = 0;
        if (utimes( Tmp_fname, file_times ))
            perror(catgets(catd, S_VRESTORE1, VRESTORE129, "utimes"));
    }

    chmod( Tmp_fname, fattr->file_stat.st_mode & 077777);

    /* rename the restored file to its real name */

    if (rename( Tmp_fname, restore_name ) < 0) {  
        if (errno == ENOENT) {                   /* ENOENT = 2 */
            /*
             * this is more serious than originally thought
             * if we don't have the correct path in place
             * then we missed the opportunity to set proper
             * protection codes and ACLs. Rather than remove
             * the call (leaving them in a lurch), we warn
             * of potential problems
             */
            fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE215, 
                "%s: Unexpectedly, path does not exist for file %s, creating path without regard to protections\n"), 
                Prog, restore_name);

            if (mkpath( restore_name ) == 0) {
                if (rename( Tmp_fname, restore_name ) == 0) {
                    return OKAY;
                }
            }
        }

        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE130, 
            "%s: cannot rename temp file <%s> to <%s>; [%d] %s\n"),
            Prog, Tmp_fname, restore_name, errno, ERR_MSG );
        remove( Tmp_fname );
    }

    return OKAY;
}

/* end close_file */

/*
 * create_dir -
 *
 * This routine creates the file directory in restore's logical directory
 * If the directory already exists then the existing directory
 * is left unchanged and no new directory is created.
 *
 */
void
create_dir(
   int dir_fd,               /* in - logical directory handle */
   struct dattr_t *dir_attr, /* in - dir attributes record */
   char *dir_name            /* in - dir name */
   )
{
    int status, retry;
    char *cp;

    if (List) {
        fprintf( stdout, " --\n" );

        if (Stdout_to_tty) {
            /*
             * print non-printable characters as '?' when going to tty
             */
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE131B,
                                           " dir name  : ") );
            for (cp = dir_name; *cp !='\0' ; cp++)
                putc(isprint(*cp) ? *cp : '?', stdout);
            fprintf( stdout, "/\n");
        } else {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE131A,
                                           " dir name  : %s/\n"), dir_name  );
        }

        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE132, 
            " dir inode : %d\n"), dir_attr->dir_stat.st_ino  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE133, 
            " dir mode  : 0%o\n"), dir_attr->dir_stat.st_mode  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE134, 
            " dir owner : %d\n"), dir_attr->dir_stat.st_uid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE135, 
            " dir group : %d\n"), dir_attr->dir_stat.st_gid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE108, 
            " parent ino: %d\n"), dir_attr->parent_ino  );
    } else {

        do {
            retry = 0;

            status = dir_mk( dir_fd, 
                             dir_name, 
                             &dir_attr->dir_stat, 
                             dir_attr->parent_ino );
        
            if (status != OKAY) {
                /*
                 * The directory could not be created.
                 */
    
                if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                    fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                            Prog, Target, errno, ERR_MSG );
                    if (want_abort())
                        abort_now();
                    retry = 1;
           
                } else if (status != EEXIST) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE136, 
                        "%s: unable to create logical dir <%s>; errno [%d]\n"),
                        Prog, dir_name, status );
                }
            }
        } while (retry);
    }
}
/* end create_dir */

/*
 * restore_dir_data -
 *
 *      This routine restores logical directory data from a dir data record.
 */

void
restore_dir_data ( 
   int dir_fd,            /* in - logical directory handle */
   int record_size,       /* in - Number of bytes in the data record */
   struct blk_t *blk,     /* in - The block containing the data record */
   int data_idx,          /* in - Offset into 'blk' of the data record */
   int compressed_data,   /* in - Save-set compressed data flag */
   lzw_desc_handle_t lzwh /* in - Compression handle */
   )
{
    int total_bytes_decoded, bytes_decoded, bytes_xferred;
    char buf[MAX_BLK_SZ];

    /*------------------------------------------------------------------------*/

    if (compressed_data) {
        /* 
         * Uncompress the data in the data record and write it to
         * the logical directory.  This has to be done in a loop since
         * it is impossible to determine the buffer size needed for the
         * uncompressed data.  So the uncompression is done incrementally.
         */
        total_bytes_decoded = 0;

        while (total_bytes_decoded < record_size) {
            uncompress( lzwh, 
                       &blk->data[data_idx + total_bytes_decoded],
                       record_size - total_bytes_decoded,
                       buf, 
                       sizeof( buf ),
                       &bytes_decoded, 
                       &bytes_xferred );

            total_bytes_decoded += bytes_decoded;

            logical_dir_write( dir_fd, buf, bytes_xferred );
        }
    } else {
        /*
         * Copy non-compressed data from the save-set block to a
         * buffer (allocated from the buffer pool) and write it
         * to the logical directory.
         */
        flmemcpy( &blk->data[data_idx], buf, record_size );

        logical_dir_write( dir_fd, buf, record_size );
    }
}

/* end restore_dir_data */

/*
 * create_file_dir -
 *
 * This routine creates the file directory represented by the dir_attr 
 * record.  If the directory already exists then the existing directory
 * is left unchanged and no new directory is created.
 */
int
create_file_dir(
   struct dattr_t *dir_attr, /* in - dir attributes record */
   char *name,               /* in - dir name */
   char *last_name	     /* out - name created */
   )
{
    int status, retry;
    char dir_name[MAXPATHLEN+1], *cp;
    struct stat stat_buf;

    if (List) {
        fprintf( stdout, " --\n" );
        if (Stdout_to_tty) {
            /*
             * print non-printable characters as '?' when going to tty
             */
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE131B,
                                           " dir name  : ") );
            for (cp = name; *cp !='\0' ; cp++)
                putc(isprint(*cp) ? *cp : '?', stdout);
            fprintf( stdout, "/\n");
        } else {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE131A,
                                           " dir name  : %s/\n"), name  );
        }

        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE132, 
            " dir inode : %d\n"), dir_attr->dir_stat.st_ino  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE133, 
            " dir mode  : 0%o\n"), dir_attr->dir_stat.st_mode  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE134, 
            " dir owner : %d\n"), dir_attr->dir_stat.st_uid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE135, 
            " dir group : %d\n"), dir_attr->dir_stat.st_gid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE108, 
            " parent ino: %d\n"), dir_attr->parent_ino  );
        return 0;
    }

    dir_name[0] = '.';
    dir_name[1] = '\0';
    
    if (dir_get_path( dir_attr->parent_ino, dir_name ) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE137, 
            "%s: cannot get path for dir inode <%d>, dir <%s>\n"),
            Prog, dir_attr->parent_ino, name );
        return 0;
    } 

    strcat( dir_name, name );
    
    if (Show) {
        if (dir_attr->dir_stat.st_ino == dir_attr->parent_ino) {
            return 0;
        }
        if (Stdout_to_tty) {
            for (cp = dir_name; *cp !='\0' ; cp++)
                putc(isprint(*cp) ? *cp : '?', stdout);
            fprintf( stdout, "/\n");
        } else
            fprintf( stdout, "%s/\n", dir_name );

    } else {
        if (dir_attr->dir_stat.st_ino == dir_attr->parent_ino) {
            /* don't need to restore root as this is really our 'Target' */
            return 0;
        }

        if (!dir_restore_me(dir_attr->dir_stat.st_ino, dir_attr->parent_ino)){
            /* dir was not selected for restoration */
            return 0;
        }

        if (lstat( dir_name, &stat_buf ) >= 0) {

            /*
             * Check to see if the existing file can be overwritten
             */

            if ( check_overwrite( dir_name ) == -1 ) {
                return 0;
            }

	    /*
	     * Check to see if the existing file is a directory
	     */

	    if (!S_ISDIR(stat_buf.st_mode)) {
	        char file_type[64];
		strcpy (file_type,catgets(catd, S_VRESTORE1, VRESTORE215, 
					   "directory"));

	        file_compare_error(dir_name,
				   file_type,
				   stat_buf);
	        return 0;
	    }
        }

        /*
         * Use mkdir() to create the new directory.  The only expected
         * error is EEXISTS (the directory already exists); this error
         * is ignored.
         */

        do {
            retry = 0;

            status = mkdir( dir_name, 0777 );
        
            if (status != 0) {
                /*
                 * The directory could not be created.
                 */
    
                if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                    fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                            Prog, Target, errno, ERR_MSG );
                    if (want_abort())
                        abort_now();
                    retry = 1;
    
                } else if (errno != EEXIST) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE120, 
                        "%s: unable to create directory <%s>; [%d] %s\n"),
                        Prog, dir_name, errno, ERR_MSG );
                    return 0;
                }
            }
        } while (retry);

        /*
         * The directory was created successfully or it exists.
         */

        if (Verbose) {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE138, 
                "r  %s/%s/\n"), Target, dir_name );
        }

        chown( dir_name, 
               dir_attr->dir_stat.st_uid, 
               dir_attr->dir_stat.st_gid );
        chmod( dir_name, dir_attr->dir_stat.st_mode & 07777 );
    }

    strcpy( last_name, dir_name );
    return 1;
}
/* end create_file_dir */

/*
 * set_file_dir_attr -
 */
void
set_file_dir_attr(
   struct dattr_t *dir_attr,    /* in - dir attributes record */
   adv_bf_attr_t *bfAttr,     /* in - dir's bitfile attributes */
   adv_bf_attr_t *bfIAttr,    /* in - dir's inheritable attributes */
   char *name                   /* in - dir name */
   )
{
    int dfd, status;
    char dir_name[MAXPATHLEN+1];

    dir_name[0] = '.';
    dir_name[1] = '\0';
    
    if (dir_get_path( dir_attr->parent_ino, dir_name ) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE137, 
            "%s: cannot get path for dir inode <%d>, dir <%s>\n"),
            Prog, dir_attr->parent_ino, name );
        return;
    } 

    strcat( dir_name, name );
    
    dfd = open( dir_name, O_RDONLY, 0 );

    if (dfd >= 0) {
        status = advfs_set_bf_attributes( dfd, bfAttr );
        if (status != 0) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE139, 
                "%s: unable to set bitfile attributes for dir <%s>; %s\n"),
                Prog, dir_name, BSERRMSG( status ));
        }

        status = advfs_set_bf_iattributes( dfd, bfAttr );
        if (status != 0) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE140, 
                "%s: unable to set inheritable attributes for dir <%s>; %s\n"),
                Prog, dir_name, BSERRMSG( status ));
        }

        close(dfd);
    }
}
/* end set_file_dir_attr */

void
create_device_file(
   struct dev_attr_t *dev_attr, /* in - dev attributes record */
   char *name,                  /* in - dev name */
   uint64_t *file_cnt,          /* in/out - Number of files restored counter */
   char *last_name		/* out - name created */
   )
{
    int status, retry;
    struct stat stat_buf;
    char dev_name[MAXPATHLEN+1], *cp;
    struct timeval dev_times[3];

    /*----------------------------------------------------------------------*/

    if (List) {
        (*file_cnt)++;
        fprintf( stdout, " --\n" );
        if (Stdout_to_tty) {
            /*
             * print non-printable characters as '?' when going to tty
             */
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE141B, 
                " dev name  : "));
            for (cp = name; *cp !='\0' ; cp++)
                putc(isprint(*cp) ? *cp : '?', stdout);
            putc('\n', stdout);
        } else {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE141A, 
                " dev name  : %s\n"), name  );
        }

        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE142, 
            " dev inode : %d\n"), dev_attr->dev_stat.st_ino  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE143, 
            " device    : 0%o\n"), dev_attr->dev_stat.st_rdev );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE144, 
            " dev mode  : 0%o\n"), dev_attr->dev_stat.st_mode  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE145, 
            " dev owner : %d\n"), dev_attr->dev_stat.st_uid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE146, 
            " dev group : %d\n"), dev_attr->dev_stat.st_gid  );
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE108, 
            " parent ino: %d\n"), dev_attr->parent_ino  );
        return;
    }

    dev_name[0] = '.';
    dev_name[1] = '\0';
    
    if (dir_get_path( dev_attr->parent_ino, dev_name ) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE137, 
            "%s: cannot get path for dir inode <%d>, dir <%s>\n"),
            Prog, dev_attr->parent_ino, name );
        return;
    } 

    strcat( dev_name, name );
    
    if (Show) {
        (*file_cnt)++;
        if (Stdout_to_tty) {
            for (cp = dev_name; *cp !='\0' ; cp++)
                putc(isprint(*cp) ? *cp : '?', stdout);
            putc('\n', stdout);
        } else
            fprintf( stdout, "%s\n", dev_name );
    } else {
        if (!dir_restore_me(dev_attr->dev_stat.st_ino, dev_attr->parent_ino)){
            /* device was not selected for restoration */
            return;
        }

        if (lstat( dev_name, &stat_buf ) >= 0) {
            /*
             * Check to see if the existing file can be overwritten
             */

            if (check_overwrite(dev_name) == -1) {
                return;
            }

	    /*
	     * Check to see if the existing file is a device file
	     */
	    if ((!S_ISFIFO(stat_buf.st_mode)) &&
		(!S_ISCHR(stat_buf.st_mode)) &&
		(!S_ISBLK(stat_buf.st_mode))) {

	        char file_type[64];
		strcpy (file_type,catgets(catd, S_VRESTORE1, VRESTORE216, 
					  "device"));

	        file_compare_error(dev_name,
				   file_type,
				   stat_buf);
	        return;
	    }

        } /* end if */

        (*file_cnt)++;

        remove( dev_name );

        do {
            retry = 0;

            if (S_ISFIFO( dev_attr->dev_stat.st_mode )) {
                status = mkfifo( dev_name, 
                                 dev_attr->dev_stat.st_mode );
            } else {
                status = mknod( dev_name, 
                                dev_attr->dev_stat.st_mode, 
                                dev_attr->dev_stat.st_rdev );
            }
            
            if (status != 0) {
                /*
                 * The device could not be created.
                 */
    
                if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                    fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                            Prog, Target, errno, ERR_MSG );
                    if (want_abort())
                        abort_now();
                    retry = 1;
    
                } else if (errno != EEXIST) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE147, 
                        "%s: unable to create device file <%s>; [%d] %s\n"),
                        Prog, dev_name, errno, ERR_MSG );
                    return;
                }
            }
        } while (retry);

        /*
         * The device was created successfully or it exists.
         */

        if (Verbose) {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE138, 
                "r  %s/%s/\n"), Target, dev_name );
        }

        chown( dev_name, 
               dev_attr->dev_stat.st_uid, 
               dev_attr->dev_stat.st_gid );

        chmod( dev_name, dev_attr->dev_stat.st_mode & 07777 );

        dev_times[0].tv_sec = dev_attr->dev_stat.st_atime;
        dev_times[0].tv_usec = 0;
        dev_times[1].tv_sec = dev_attr->dev_stat.st_mtime;
        dev_times[1].tv_usec = 0;

        if (utimes( dev_name, dev_times ))
            perror(catgets(catd, S_VRESTORE1, VRESTORE129, "utimes"));
    }
    strcpy( last_name, dev_name );
}

void
create_hard_link(
    struct hlink_t *hlink,
    char *fname,
    char *lname,
    uint64_t *file_cnt,  /* in/out - Number of files restored counter */
    char *last_name	 /* out - name created */
    )
{
    char link_name[MAXPATHLEN+1], file_name[MAXPATHLEN+1];
    struct stat stat_buf;
    int retry;

    if (List) {
        (*file_cnt)++;
        list_hard_link_attr( hlink, fname, lname );
        return;
    }

    file_name[0] = '.';
    file_name[1] = '\0';
    
    if (dir_get_path( hlink->file_parent_ino, file_name) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE148, 
            "%s: cannot get path for dir inode <%d>, file <%s>\n"),
            Prog, hlink->file_parent_ino, fname );
        return;
    }

    strcat( file_name, fname );
        
    link_name[0] = '.';
    link_name[1] = '\0';
    
    if (dir_get_path( hlink->link_parent_ino, link_name) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE148, 
            "%s: cannot get path for dir inode <%d>, file <%s>\n"),
            Prog, hlink->file_parent_ino, lname );
        return;
    }

    strcat( link_name, lname );
        
    if (Show) {
        (*file_cnt)++;
        show_hard_link_attr( hlink, file_name, link_name );
        return;
    } 

    if (!dir_restore_me( hlink->file_ino, hlink->link_parent_ino )) {
        return;
    }

    (*file_cnt)++;

    if (Verbose) {
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE149, 
            "r  %s/%s --> %s/%s\n"), 
            Target, link_name, Target, file_name ); 
    }

    /*
     * Have to compare the stats of the existing file (file_name) to 
     * the file we are restoring (hlink).
     */
    if (lstat( link_name, &stat_buf ) >= 0) {
        /*
         * Check to see if the existing file can be overwritten
         */
        if ( check_overwrite( link_name ) == -1 ) {
            return;
        }
    } /* end if */

    remove( link_name );

    do {
        retry = 0;

        if (link( file_name, link_name ) < 0) {

            if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                        Prog, Target, errno, ERR_MSG );
                if (want_abort())
                    abort_now();
                retry = 1;

            } else {
                 fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE150, 
                     "%s: error creating link %s --> %s; [%d] %s\n"),
                     Prog, link_name, file_name, errno, ERR_MSG );
                return;
            }
        }
    } while (retry);
    strcpy( last_name, link_name );
}

void
create_sym_link(
    struct slink_t *slink,
    char *lname,
    char *link,
    uint64_t *file_cnt, /* in/out - Number of files restored counter */
    char *last_name	/* out - name created */
    )
{
    char link_name[MAXPATHLEN+1];
    struct stat stat_buf;
    int retry;
    int ret_val;
    mode_t tmp_umask;

    if (List) {
        (*file_cnt)++;
        list_sym_link_attr( slink, lname, link );
        return;
    }

    link_name[0] = '.';
    link_name[1] = '\0';
    
    if (dir_get_path( slink->parent_ino, link_name) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE148, 
            "%s: cannot get path for dir inode <%d>, file <%s>\n"),
            Prog, slink->parent_ino, lname );
        return;
    }

    strcat( link_name, lname );
        
    if (Show) {
        (*file_cnt)++;
        show_sym_link_attr( slink, link_name, link );
        return;
    } 

    if (!dir_restore_me( slink->ino, slink->parent_ino )) {
        return;
    }

    (*file_cnt)++;

    if (Verbose) {
        fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE151, 
            "r  %s/%s @-> %s\n"), Target, link_name, link );
    }

    if (lstat( link_name, &stat_buf ) >= 0) {
        /*
         * Check to see if the existing file can be overwritten
         */

        if ( check_overwrite( link_name ) == -1 ) {
            return;
        }

	/*
	 * Check to see if the existing file is a symbolic link
	 */

	if (!S_ISLNK(stat_buf.st_mode)) {
	    char file_type[64];
	    strcpy (file_type,catgets(catd, S_VRESTORE1, VRESTORE217, 
					  "symbolic link"));

	    file_compare_error(link_name,
			       file_type,
			       stat_buf);
	    return;
	}
    } /* end if */

    remove( link_name );

    do {
        retry = 0;

        /*
         * Change the process mask to the mode we want for the
         * sym link we are creating.
         */
        tmp_umask = umask( ~slink->link_stat.st_mode );
        ret_val = symlink( link, link_name );

        /* restore the original process umask */
        umask( tmp_umask );

        if (ret_val < 0) {

            if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                        Prog, Target, errno, ERR_MSG );
                if (want_abort())
                    abort_now();
                retry = 1;

            } else {
                 fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE152, 
                      "%s: error creating symbolic link %s @-> %s; [%d] %s\n"),
                      Prog, link_name, link, errno, ERR_MSG );
                return;
            }
        }
    } while (retry);

    /* change the ownership of the symbolic link but not the target file */
    lchown( link_name, slink->link_stat.st_uid, slink->link_stat.st_gid);

    strcpy( last_name, link_name );
}

/*
 * create_file -
 *
 * This routine creates the file to be restored.  The file is initialy
 * created with the name defined by Tmp_fname.  Once the file is
 * successfully restored it renamed to its real name in close_file().
 */

int
create_file( 
   struct fattr_t *fattr, /* in - File attributes of file to be restored */
   int *fd,               /* out - File descriptor of file to be restored */
   char *name,            /* in - Name of file to be restored */
   char *file_name,       /* out - file name and its path pre-appended */
   uint64_t *file_cnt,    /* in/out - Number of files restored counter */
   int compressed,        /* in - Save-set compressed data flag */
   lzw_desc_handle_t lzwh /* in - Compression handle */
   )
{
    int ret_val = OKAY, retry;
    struct stat stat_buf;

    if (List) {
        (*file_cnt)++;
        list_file_attr( fattr, name );
        return ret_val;
    }

    file_name[0] = '.';
    file_name[1] = '\0';
    
    if (dir_get_path( fattr->parent_ino, file_name) == NULL) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE148, 
            "%s: cannot get path for dir inode <%d>, file <%s>\n"),
            Prog, fattr->parent_ino, name );
        return ERROR;
    }

    strcat( file_name, name );

    if (Show) {
        (*file_cnt)++;
        show_file_attr( fattr, file_name );

    } else {
        if (!dir_restore_me( fattr->file_stat.st_ino, fattr->parent_ino )) {
            return ERROR;
        }

        if (lstat( file_name, &stat_buf ) >= 0) {
            /*
             * Check to see if the existing file can be overwritten
             */

            if ( check_overwrite( file_name ) == -1 ) {
		/* it cannot be overwritten */
                return 1;
            }

	    /*
	     * Check to see if the existing file is a regular file
	     */
	    if (!S_ISREG(stat_buf.st_mode)) {
	        char file_type[64];
		strcpy (file_type,catgets(catd, S_VRESTORE1, VRESTORE214, 
					  "file"));

	        file_compare_error(file_name,
				   file_type,
				   stat_buf);
	        return 1;
	    }
        } /* end if */

        (*file_cnt)++;

        if (Verbose) {
            fprintf( stdout, catgets(catd, S_VRESTORE1, VRESTORE153, 
                "r  %s/"), Target ); 
            show_file_attr( fattr, file_name );
        }

        /* 
         * Create the file 
         */

        if (lstat( Tmp_fname, &stat_buf ) >= 0) {
            /*
             * For some reason restore's temporary file already exists
             * so we delete it.  This probably requires operator intervention
             * since some other application may be using the same file name.
             * TODO - Add operator intervention.
             */
            chmod( Tmp_fname, S_IWUSR );
            remove( Tmp_fname );
        }

        /* 
         * TODO - Ensure that Tmp_fname is created in the correct
         * filesystem.  Like, use the path part of 'file_name'.
         * This will ensure that the rename in close_file() works.
         * May need to add call to mkpath()?!
         */

        do {
            retry = 0;

            *fd = open( Tmp_fname, O_CREAT | O_TRUNC | O_WRONLY, 0666 );
    
            if (*fd == ERROR) {
    
                if ((errno == ENOSPC) || (errno == EDQUOT)) {     
                    fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                            Prog, Target, errno, ERR_MSG );
                    if (want_abort())
                        abort_now();
                    retry = 1;

                } else {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE154, 
                        "%s: unable to create file <%s>; [%d] %s\n"),
                        Prog, Tmp_fname, errno, ERR_MSG );
                    *fd = CLOSED;
                    return ERROR;
                }
            }
        } while (retry);
    
        if (compressed) {
            start_uncompress( lzwh );
        }
    }

    return ret_val;
}

/* end create_file */

/*
 * restore_file_data -
 *
 *      This routine restores file data from a file data record.
 */
int
restore_file_data ( 
   int fd,
   uint16_t record_size,               /* in - bytes in data record */
   uint16_t flags,                     /* in - data record flags */
   uint64_t d_offset,                  /* in - offset into file for rec */
   uint64_t *tot_bytes_restored,       /* in/out - total bytes or all recs */
   struct blk_t *blk,                  /* in - saveset block containing rec */
   int data_idx,                       /* in - Offset into 'blk' of the rec */
   int compressed_data,                /* in - Saveset compressed data flag */
   lzw_desc_handle_t lzwh              /* in - Compression handle */
   )
{
    int retry, wcnt, total_bytes_decoded, bytes_decoded, bytes_xferred;
    int bytes_2_write, bytes_written, uc_cnt;
    char buf[MAX_BLK_SZ];

    /*----------------------------------------------------------------------*/

    if (Show) {
        return OKAY;
    }

    if ((Version >= 1.3) && !(flags > RF_NULL)) {

        if (compressed_data) {
            uc_cnt = finish_uncompress( lzwh, buf );

            if (uc_cnt > 0) {
                *tot_bytes_restored += uc_cnt;
    
                do {
                    retry = 0;
    
                    wcnt = write( fd, buf, uc_cnt );
    
                    if (wcnt != uc_cnt) {
                        if ((wcnt >= 0) || 
                            (errno == ENOSPC) || 
                            (errno == EDQUOT)) {     
    
                            if (wcnt >= 0) {
                                fprintf(stderr, 
                                    catgets(catd, S_VRESTORE1, VRESTORE124, 
                                    "\n%s: <%s>; Out of disk space\n"),
                                    Prog, Target );
                            } else {
                                fprintf(stderr, 
                                        "\n%s: <%s>; [%d] %s\n",
                                        Prog, Target, errno, ERR_MSG );
                            }
    
                            if (want_abort())
                                abort_now();
                            uc_cnt -= wcnt;
                            retry = 1;
    
                        } else {
                            fprintf( stderr, 
                                catgets(catd, S_VRESTORE1, VRESTORE155, 
                                "\n%s: Error writing to file; [%d] %s\n"),
                                Prog, errno, ERR_MSG );
                            return ERROR;
                        }
                    }
                } while (retry);
            }
	  
            start_uncompress( lzwh );
        }
    }

    /* skip the sparse file hole - if one */
    if ((d_offset != 0) && (flags != RF_DATA_CONTINUED)) {
        if (lseek( fd, d_offset, SEEK_SET ) < 0) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE156, 
                "%s: lseek error; [%d] %s\n"), Prog, errno, ERR_MSG );
            return ERROR;
        }
    }

    if (compressed_data) {
        /* 
         * Uncompress the data in the file data record and write it to
         * the file being restore.  This has to be done in a loop since
         * it is impossible to determine the buffer size needed for the
         * uncompressed data.  So the uncompression is done incrementally.
         */
        total_bytes_decoded = 0;

        while (total_bytes_decoded < record_size) {
            uncompress( lzwh, 
                        &blk->data[data_idx + total_bytes_decoded],
                        record_size - total_bytes_decoded,
                        buf, 
                        Blk_size,
                        &bytes_decoded, 
                        &bytes_xferred );

            bytes_2_write = bytes_xferred;
            bytes_written = 0;

            do {
                retry = 0;

                wcnt = write( fd, &buf[ bytes_written ], bytes_2_write );

                if (wcnt != bytes_2_write) {

                    if ((wcnt >= 0) || 
                        (errno == ENOSPC) || 
                        (errno == EDQUOT)) {     

                        if (wcnt >= 0) {
                            fprintf(stderr, 
                                catgets(catd, S_VRESTORE1, VRESTORE124, 
                                "\n%s: <%s>; Out of disk space\n"),
                                Prog, Target );
                        } else {
                            fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                                    Prog, Target, errno, ERR_MSG );
                        }

                        if (want_abort())
                            abort_now();

                        if (wcnt > 0) {
                            bytes_2_write -= wcnt;
                            bytes_written += wcnt;
                        }
                        retry = 1;

                    } else {
                        fprintf( stderr, 
                            catgets(catd, S_VRESTORE1, VRESTORE155, 
                            "\n%s: Error writing to file; [%d] %s\n"),
                            Prog, errno, ERR_MSG );
                        return ERROR;
                    }
                }
            } while (retry);

            total_bytes_decoded += bytes_decoded;
            *tot_bytes_restored += bytes_xferred;
        }

    } else {
        /*
         * Copy non-compressed file data from the save-set block to a
         * buffer (allocated from the buffer pool)
         */

        flmemcpy( &blk->data[data_idx], buf, record_size );

        bytes_2_write = record_size;
        bytes_written = 0;

        do {
            retry = 0;

            wcnt = write( fd, &buf[ bytes_written ], bytes_2_write );

            if (wcnt != bytes_2_write) {

                if ((wcnt >= 0) || 
                    (errno == ENOSPC) || 
                    (errno == EDQUOT)) {     

                    if (wcnt >= 0) {
                        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE124,
                            "\n%s: <%s>; Out of disk space\n"),
                            Prog, Target );
                    } else {
                        fprintf(stderr, "\n%s: <%s>; [%d] %s\n",
                                Prog, Target, errno, ERR_MSG );
                    }

                    if (want_abort())
                        abort_now();

                    if (wcnt > 0) {
                        bytes_2_write -= wcnt;
                        bytes_written += wcnt;
                    }
                    retry = 1;

                } else {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE155, 
                        "\n%s: Error writing to file; [%d] %s\n"),
                        Prog, errno, ERR_MSG );
                    return ERROR;
                }
            }
        } while (retry);

        *tot_bytes_restored += record_size;
    }

    return OKAY;
}

/* end restore_file_data */

/*
 * restore_quota_data -
 *
 *      This routine restores AdvFS quota data from a quota record.
 */
int
restore_quota_data ( 
   record_type_t type,
   void *data,
   int qfd,
   bfSetParamsT *oldSetParams,
   uint64_t *file_cnt,
   uint64_t *total_bytes_restored
   )
{
    int is_eof_record = FALSE,          /* TRUE if cur. rec is EOF rec */
        quotactl_cmd,                   /* Command to give to quotactl() */
        dqsize,                         /* Size of disk quota structure */
        status = 0;                     /* Return status from function */
    off_t qfile_size;                   /* Size of quota file in bytes */
    struct ugquota64_t *ug64_recp;      /* User/group rec. pointer */
    struct fquota_t  *f_recp;           /* Fileset rec. pointer */
    static int first_user_rec = TRUE,   /* TRUE if this is first user rec */
               first_group_rec = TRUE,  /* TRUE if this is first group rec */
               last_id_restored = -1;   /* ID of last quota rec restored */
    struct dqblk64 dqb64,               /* Current on-disk quota block */
                   new_dqb64;           /* New on-disk quota structure */

    /*  initialize quota related vars.  May need updating when 
     *  group quotas are supported
     */
    dqsize = sizeof(struct dqblk64);
    quotactl_cmd = Q_SETQUOTA64;

    /*
     * Determine if this is an end-of-file record.
     */
    if (type != RT_FQUOTA) {
        ug64_recp = (struct ugquota64_t *)data;
        if (ug64_recp->id == -1)
            is_eof_record = TRUE;
        else
            is_eof_record = FALSE;
    }

    /*
     * If user has chosen the -t option, show the name of the
     * quota files when we restore the first record of each type.
     */
    if (Show) {
        if (first_user_rec && ((type == RT_UQUOTA32) || (type == RT_UQUOTA64))) {
            fprintf( stdout, "%s\n", USR_QUOTA_FILE );
            first_user_rec = FALSE;
        }
        else if (first_group_rec && ((type == RT_GQUOTA32) || (type == RT_GQUOTA64))) {
            fprintf( stdout, "%s\n", GRP_QUOTA_FILE );
            first_group_rec = FALSE;
        }
        return (0);
    }

    /*
     * If user has chosen the -l option and this is not an
     * end-of-file record, print out the fields of the record.
     */
    if (List) {
        if (!is_eof_record) {
            if (type == RT_FQUOTA)
                list_rec_hdr(catgets(catd, S_VRESTORE1, VRESTORE202,
                             "Fileset quota data"), 
                             (struct rheader_t *)((char *)(data) -
                             sizeof(union record_header_t)), stdout);
            else if ((type == RT_UQUOTA32) || (type == RT_UQUOTA64))
                list_rec_hdr(catgets(catd, S_VRESTORE1, VRESTORE203,
                             "User quota data"), 
                             (struct rheader_t *)((char *)(data) -
                             sizeof(union record_header_t)), stdout);
            else if ((type == RT_GQUOTA32) || (type == RT_GQUOTA64))
                list_rec_hdr(catgets(catd, S_VRESTORE1, VRESTORE204,
                             "Group quota data"), 
                             (struct rheader_t *)((char *)(data) -
                             sizeof(union record_header_t)), stdout);
            list_quota_attr(type, data);
        }
        return (0);
    }

    /*
     * If this is a fileset quota record, replace the quota fields
     * in the current set parameters from the quota record and
     * update the fileset.
     */
    if (type == RT_FQUOTA) {
        f_recp = (struct fquota_t *)data;
        oldSetParams->blkHLimit = f_recp->blkHLimit;
        oldSetParams->blkSLimit = f_recp->blkSLimit;
        oldSetParams->fileHLimit = f_recp->fileHLimit;
        oldSetParams->fileSLimit = f_recp->fileSLimit;
        oldSetParams->blkTLimit = f_recp->blkTLimit;
        oldSetParams->fileTLimit = f_recp->fileTLimit;
        status = advfs_set_bfset_params(oldSetParams->bfSetId, oldSetParams);
    }
    else {
        /*
         * This is a user or group quota record.
         * If this is the first user or group quota record to
         * be restored, increment the global files-restored count.
         */
        switch (type) {
          case RT_UQUOTA32:
          case RT_UQUOTA64:
            if (first_user_rec) {
                (*file_cnt)++;
                first_user_rec = FALSE;
                if (Verbose && !Show) 
                    fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE208, 
                            "r  %s\n"), USR_QUOTA_FILE);
            }

            break;
          case RT_GQUOTA32:
          case RT_GQUOTA64:
#ifdef GROUP_QUOTAS
            if (first_group_rec) {
                (*file_cnt)++;
                first_group_rec = FALSE;
                if (Verbose && !Show) 
                    fprintf(stdout, catgets(catd, S_VRESTORE1, VRESTORE208, 
                            "r  %s\n"), GRP_QUOTA_FILE);
            }

            }
#endif /* GROUP_QUOTAS */
            break;
        }

        if (!is_eof_record) {
            /*
             * See if there is a gap between the quota record we are
             * restoring and the last quota record restored.  If so,
             * that means that when the saveset was created, the quota
             * records for the IDs between the current ID and that of
             * the last quota record restored were all zeros.  In order
             * to make the quotas look exactly like they did when the
             * backup was done, we need to make sure that the quota
             * limits of those IDs are reset to all zeros.
             */
            if ((ug64_recp->id != last_id_restored + 1) && (qfd != -1)) {

                zero_quota_range (qfd, 
                                  last_id_restored + 1, 
                                  ug64_recp->id,
                                  dqsize,
                                  quotactl_cmd);
            }

            /*
             * Set up and call quotactl() to restore the current quota record.
             */
            bzero(&new_dqb64, sizeof(new_dqb64));
            last_id_restored = ug64_recp->id;
            new_dqb64.dqb64_bhardlimit = ug64_recp->dqb_bhardlimit;
            new_dqb64.dqb64_bsoftlimit = ug64_recp->dqb_bsoftlimit;
            new_dqb64.dqb64_fhardlimit = ug64_recp->dqb_fhardlimit;
            new_dqb64.dqb64_fsoftlimit = ug64_recp->dqb_fsoftlimit;
            new_dqb64.dqb64_btimelimit = ug64_recp->dqb_btimelimit;
            new_dqb64.dqb64_ftimelimit = ug64_recp->dqb_ftimelimit;

            if (quotactl(quotactl_cmd,
                             Fs_info.filesys,
                             ug64_recp->id,
                             &new_dqb64)) {

                if(errno == EOPNOTSUPP) {
                    if(!AlreadyPrintedNoQuotas) {
                        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE239,
                            "%s: warning: advdump/advrestore of quotas not supported for non local filesystems.\n"),
                            Prog);
                        AlreadyPrintedNoQuotas = 1;
                    }
                } else {
                    fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE207,
                        "%s: quotactl() failed: [%d] %s.\nQuotas could not be restored for %s %d.\n"),
                        Prog, errno, ERR_MSG,
#ifdef GROUP_QUOTAS
                        qfextension[quotactl_cmd & SUBCMDMASK], last_id_restored);
#else
                        "user", last_id_restored);
#endif /* GROUP_QUOTAS */
                }
                status = -1;
            }
        }
        else {
            /*
             * This is an end-of-file record.  That is, this was the last
             * non-zero quota record in the file at the time that the
             * backup was done.  We need to see if other quota records
             * have been added to the end of the file since the backup was done.
             * If so, zero them out to recapture the "functional image"
             * of the quota file at the time the backup was done.
             */

            if (last_id_restored != -1)
                *total_bytes_restored += last_id_restored * dqsize;

            /* 
             * See how big the quota file is now.
             */
            if ((qfile_size = lseek(qfd, 0, SEEK_END)) < 0) {
                status = -1;
                fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE205,
                    "%s: lseek() on quota file failed: [%d] %s.\nSome quotas may not be restored.\n"),
                    Prog, errno, ERR_MSG);
            }
            else if ((int64_t)(qfile_size) > 
                     (int64_t)(last_id_restored * dqsize)) {
                /*
                 * Quotas have been added to the end of the file since
                 * the backup was done.  Go through those new quota
                 * records and set them to zero.
                 */
                zero_quota_range (qfd, 
                                  last_id_restored + 1, 
                                  qfile_size / dqsize,
                                  dqsize,
                                  quotactl_cmd);

            }  /* file larger than when backed up */

            last_id_restored = -1;
 
        }  /* EOF record */
    } /* user or group quota record */

    return status;
}

/* end restore_quota_data */

/*
 * zero_quota_range -
 *
 *      This routine zero AdvFS quota data in a specified range.
 */
void zero_quota_range ( 
   int qfd,
   int firstId,
   int lastId,
   int dqsize,
   int quotactl_cmd
   )
{
    void *xtntmap;
    int status = 0;
    int currentId;
    off_t nextbyte, curbyte;
    struct dqblk64 zero_dqb64 = {0,0,0,0,0,0,0,0}; 

    xtntmap = advfs_xtntmap_open( qfd, &status, 0 );
    if (xtntmap == NULL) {
        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE231,
                "%s: advfs_xtntmap_open() on quota file failed: [%d] %s.\n"),
                Prog, errno, ERR_MSG);
        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE232,
                "%s: unable to reinitialize %s quotas from %d to %d.\n"),
                Prog, "user", firstId, lastId);
        return;
    }
    curbyte = (int64_t)(firstId) * dqsize - 1;
    nextbyte = 0;

    if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) >= 0) {

        while (nextbyte < (int64_t)(lastId) * dqsize) {
            /* Assume that most quotas are non-zero.  So instead of 
             * reading the quota file to see if this quota is already 
             * zero, call quotactl to set it to zero.
             */
            currentId = nextbyte / dqsize;
            if (quotactl(quotactl_cmd,
                         Fs_info.filesys,
                         currentId,
                         &zero_dqb64)) {

                if(errno == EOPNOTSUPP) {
                    if(!AlreadyPrintedNoQuotas) {
                        fprintf(stderr, catgets(catd, S_VRESTORE1, VRESTORE239,
                            "%s: warning: advdump/advrestore of quotas not supported for non local filesystems.\n"),
                            Prog);
                        AlreadyPrintedNoQuotas = 1;
                    }
                } else {
                    fprintf(stderr,
                        catgets(catd, S_VRESTORE1,
                        VRESTORE207,
                        "%s: quotactl() failed: [%d] %s.\nQuotas could not be restored for %s %d.\n"),
                        Prog, errno, ERR_MSG, "user", currentId);
                }
            }
            curbyte = nextbyte + dqsize - 1;
            if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
                break;
            }
        }
    }
    advfs_xtntmap_close( xtntmap );
    return;
}

/* end zero_quota_range */

  /*
   * Function:
   *
   *      vrestore_open_device
   *
   * Function description:
   *
   *      Opens the source save-set and binds the appropriate device-specific
   *      routines.
   *
   * Arguments:
   *
   *      source (in)
   *          - The name of the device/file containing the save-set.
   *
   * Return value:
   *
   *      The file descriptor of the open save-set device/file.
   *      ERROR - If an error occurred.
   *
   */
int
vrestore_open_device( char *source )

{
    int s_fd = 0,dev_type;
    struct stat s_stat;
    Dev_type = NONE;

    if (remote_host_name) {
        if (rmtopen_device(RESTORE, source) < 0) {
            abort_prog("Cannot open remote device file.\n");
        }
        return s_fd;
    } 

    /*  Local Device */

    /* check to see if a pipe or stdin */
    if (((stat( source, &s_stat ) >= 0 ) &&
        ((s_stat.st_mode & S_IFMT) == S_IFIFO)) || (Use_stdin)) {

        s_fd = 0;
        pipe_bind();
    
        return s_fd;
    }

    if ((stat( source, &s_stat ) >= 0 ) &&
        (((s_stat.st_mode & S_IFMT) == S_IFCHR) ||
          (s_stat.st_mode & S_IFMT) == S_IFBLK))
    {
        /* source is a block or character special device */

        if ((s_fd = open( source, O_RDONLY, 0 )) == ERROR) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE157,
                  "%s: unable to open save-set <%s>; [%d] %s\n"),
                  Prog, source, errno, ERR_MSG );
            return ERROR;
        }

        /*
         * get the device type being written to and bind
         * the appropriate functions for the device
         */
        if( (dev_type = get_dev_type(s_fd, source)) == ERROR ) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE157,
                   "%s: unable to open save-set <%s>; [%d] %s\n"),
                   Prog, source, errno, ERR_MSG );
            return ERROR;
        } else {
            Rewind = (dev_type ? TRUE : FALSE );
        }
    } else {
        /* source is a disk file */
        Dev_type = DEV_FILE;

        if ((s_fd = open( source, O_RDONLY )) == ERROR) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE157,
                  "%s: unable to open save-set <%s>; [%d] %s\n"),
                  Prog, source, errno, ERR_MSG );
            return ERROR;
        }

        /* bind file-specific I/O routines */
        file_bind();
    }

    return s_fd;
}

/* end vrestore_open_device */

int
file_read_buf( int fd, char *blk, int cnt )

  /*
   * Function:
   *
   *      file_read_buf
   *
   * Function description:
   *
   *      Reads a save-set block from a disk file.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *      blk (out)
   *          - Pointer to a block buffer.
   *      cnt (in)
   *          - Number of bytes to read.
   *
   * Return value:
   *
   *      Number of bytes read.
   *      ERROR - If an error occurred.
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

{
    int rcnt;

    rcnt = read( fd, blk, cnt );

        if (rcnt < cnt) {

            if (rcnt < 0) {
                /*
                 * We can get this condition if the user hit ^C to abort
                 * and the file has been closed but the main thread is
                 * still trying to read the file. So only print an error
                 * message if we aren't in the process of aborting.
                 */
                if (0 == Abort_restore) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE161, 
                        "%s: unable to read from save-set; [%d] %s\n"),
                        Prog, errno, ERR_MSG );
                }
            } else if (rcnt > 0) {
                /*
                 * We can also get this condition if the user hit ^C to
                 * abort and the file is closed during the read. So only
                 * print an error if we aren't in the process of aborting.
                 */
                if (0 == Abort_restore) {
                    fprintf( stderr, 
                        catgets(catd, S_VRESTORE1, VRESTORE162, 
                        "%s: unable to read from save-set; incomplete read %d < %d\n"),
                        Prog, rcnt, cnt );
                    rcnt = -1;
                }
            }
        }
    return rcnt;
}

/* end file_read_buf */


int
file_reset_save_set( int fd )

  /*
   * Function:
   *
   *      file_reset_save_set
   *
   * Function description:
   *
   *      Resets the file pointer to the start of the file.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *
   * Return value:
   *
   *      ERROR - If an error occurred.
   *      OKAY  - If the function completed successfully
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

   {
   if (lseek( fd, 0, L_SET ) == ERROR)
      {
      fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE163, 
          "%s: unable to reset save-set; [%d] %s\n"), 
          Prog, errno, ERR_MSG );
      return ERROR;
      }
   else
      return OKAY;
   }

/* end file_reset_save_set */


void
file_close_device( int fd )

  /*
   * Function:
   *
   *      file_close_device
   *
   * Function description:
   *
   *      Closes the save-set file.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *
   * Return value:
   *
   *      None
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

{
    if (fd < 0) {
        return;
    }

   close( fd );
}

/* end file_close_device */


void
file_bind( )

  /*
   * Function:
   *
   *      file_bind
   *
   * Function description:
   *
   *      Binds the disk file specific routines to the device-specific
   *      I/O function pointers.
   *
   * Arguments:
   *
   *      None 
   *
   * Return value:
   *
   *      None
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

   {
   read_buf       = file_read_buf;

   pthread_mutex_lock(&dev_mutex);
   close_device   = file_close_device;
   pthread_mutex_unlock(&dev_mutex);

   reset_save_set = file_reset_save_set;
   }

/* end file_bind */

int
pipe_read_buf( int fd, char *blk, int cnt )

  /*
   * Function:
   *
   *      pipe_read_buf
   *
   * Function description:
   *
   *      Reads a save-set block from a pipe.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *      blk (out)
   *          - Pointer to a block buffer.
   *      cnt (in)
   *          - Number of bytes to read.
   *
   * Return value:
   *
   *      Number of bytes read.
   *      ERROR - If an error occurred.
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

{
    int rcnt, bytes_to_read = cnt, bytes_read = 0;

    while (bytes_read < cnt) {

        rcnt = read( fd, &blk[ bytes_read ], bytes_to_read );

        if (rcnt < 0) {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE161, 
                "%s: unable to read from save-set; [%d] %s\n"),
                Prog, errno, ERR_MSG );
            return -1;

        } else if (rcnt == 0) {
            /* end-of-file */
            return 0;
        }

        bytes_read += rcnt;
        bytes_to_read -= rcnt;
    }

    return cnt;
}

/* end pipe_read_buf */

void
pipe_close_device( int fd )

  /*
   * Function:
   *
   *      pipe_close_device
   *
   * Function description:
   *
   *      Closes the pipe.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *
   * Return value:
   *
   *      None
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

{
   if (fd != 0) {             /* don't close stdin */
       close( fd );
   }
}

/* end pipe_close_device */


void
pipe_bind( )

  /*
   * Function:
   *
   *      pipe_bind
   *
   * Function description:
   *
   *      Binds the pipe specific routines to the device-specific
   *      I/O function pointers.
   *
   * Arguments:
   *
   *      None 
   *
   * Return value:
   *
   *      None
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

   {
       read_buf       = pipe_read_buf;
       pthread_mutex_lock(&dev_mutex);
       close_device   = pipe_close_device;
       pthread_mutex_unlock(&dev_mutex);
   }

/* end pipe_bind */


int
tape_read_buf( int _not_used_fd, char *blk, int cnt )

  /*
   * Function:
   *
   *      tape_read_buf
   *
   * Function description:
   *
   *      Reads a save-set block from tape.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *      blk (out)
   *          - Pointer to a block buffer.
   *      cnt (in)
   *          - Number of bytes to read.
   *
   * Return value:
   *
   *      Number of bytes read.
   *      ERROR - If an error occurred.
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

{
    int rcnt, valid_read = FALSE;
    struct mtget devstat;

    while (!valid_read) {

        if (remote_host_name) {
            rcnt = rmtread( blk, cnt );
        } else {
            rcnt = read( Src_fd, blk, cnt );
        }

        if (rcnt > 0) {
            valid_read = TRUE;

        } else if ((errno == ENOSPC) || (rcnt == 0)) {

            /* [28] No space left on device */

            tape_unload( Src_fd, Tape_number );

            if (remote_host_name) {
                rmtclose();
            } else {
                close( Src_fd );
            }
            Src_fd = -1;

            Src_fd = tape_get_new( RESTORE, Source, Tape_number, 
                remote_host_name );

            if (Src_fd >= 0) {
                Tape_number++;

            } else {
		remove( Tmp_fname );               /* remove "_tmp" file */

                pthread_mutex_lock(&dev_mutex);
		if (close_device != NULL) {
		    /* 
		     * since close_device is a function pointer we don't
		     * want to call it if it hasn't been initialized yet.
		     */
		    close_device( Src_fd );        /* close the save-set */
		}
                pthread_mutex_unlock(&dev_mutex);

                post_event_backup_error(Prog);

                exit( 1 );
            }

        } else {
            fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE164,
                     "\n%s: unable to read from device <%s>; [%d] %s\n"),
                     Prog, Source, errno, ERR_MSG );

            /* get device status of source, if it's a local device */

            if (!remote_host_name) {
                if (ioctl(Src_fd, MTIOCGET, &devstat) == ERROR) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE165,
                        "%s: unable to get device <%s> status; [%d] %s\n"),
                        Prog, Source, errno, ERR_MSG );
                } else if (!GMT_ONLINE(devstat.mt_gstat)) {
                    fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE158,
                        "%s: device <%s> is OFFLINE\n"), Prog, Source);
                }
            }

            if (!op_retry()) {
		remove( Tmp_fname );               /* remove "_tmp" file */

                pthread_mutex_lock(&dev_mutex);
		if (close_device != NULL) {
		    /* 
		     * since close_device is a function pointer we don't
		     * want to call it if it hasn't been initialized yet.
		     */
		    close_device( Src_fd );        /* close the save-set */
		}
                pthread_mutex_unlock(&dev_mutex);

                post_event_backup_error(Prog);

                exit( 1 );
            }
        } /* end unable to read */
    } /* end while */

    if ((rcnt < cnt) && (rcnt > 0)) {
        fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE166, 
                "%s: unable to read from device '%s'; incomplete read %d < %d\n"),
                Prog, Source, rcnt, cnt );
        return ERROR;
    }

    return rcnt;
}

/* end tape_read_buf */


  /*
   * Function:
   *
   *      tape_reset_save_set
   *
   * Function description:
   *
   *      Rewinds the save-set tape to the beginning of the tape.
   *      Should be beginning of save-set - see sys/mtio.h.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *
   * Return value:
   *
   *      ERROR - If an error occurred.
   *      OKAY  - If the function completed successfully
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */
int
tape_reset_save_set( int fd )

{
   int st = 0;

   /* MTBSF doesn't backup successfully for the first save-set but will
    * work when the save-set is not the first dump on the tape.
    */
   struct mtop t_op = { MTREW, 1 };     /* MTREW rewinds to begin of tape */

   if (remote_host_name) {
       st = rmtioctl(MTREW,1);
   } else { 
       st = ioctl( fd, (int) MTIOCTOP, (char *) &t_op);
   }

   if (st < 0) {
      fprintf( stderr, catgets(catd, S_VRESTORE1, VRESTORE167,
               "%s: unable to rewind tape; [%d] %s\n"),
               Prog, errno, ERR_MSG );
      return ERROR;
      }
   else
      return OKAY;
}

/* end tape_reset_save_set */


/*
 * Function:
 *
 *      tape_close_device
 *
 * Function description:
 *
 *      Closes that tape device file.
 *
 * Arguments:
 *
 *      fd (in)
 *          - Save-set's file descriptor.
 */
void
tape_close_device( int fd )
{

    if (remote_host_name) {
        rmtclose();
        return;
    }

    /* local device */
    if (close( fd ) < 0){
        /* report a close error, it's serious */
        fprintf(stderr,
             catgets(catd, S_VRESTORE1, VRESTORE223,
                     "\n%s: unable to properly close device <%s>; [%d] %s\n"),
                     Prog, Source, errno, ERR_MSG );
    }
} /* end tape_close_device */


void
tape_bind( )

  /*
   * Function:
   *
   *      tape_bind
   *
   * Function description:
   *
   *      Binds the tape-specific routines to the device-specific
   *      I/O function pointers.
   *
   * Arguments:
   *
   *      None 
   *
   * Return value:
   *
   *      None
   *
   * Side effects:
   *
   *      None
   *
   * Notes:
   *
   */

   {
       read_buf       = tape_read_buf;
       pthread_mutex_lock(&dev_mutex);
       close_device   = tape_close_device;
       pthread_mutex_unlock(&dev_mutex);
       reset_save_set = tape_reset_save_set;
   }

/* end tape_bind */


void
prepare_signal_handlers( void )
{
    pthread_t       st_handle;

    /* Spawn a new thread to handle signals; new thread runs signal_thread() */
    pthread_create( &st_handle, 
                    NULL,
                    signal_thread,
                    (void*)NULL );
}


void*
signal_thread( void *arg )
{
    int      abort = 0;                 /* TRUE = abort advrestore operation */
    int      sig_delivered;
    int      ret;
    sigset_t newset;
    sigset_t *newsetp = &newset;
    struct sigaction newint;
    struct sigaction oldint;
    struct sigaction newquit;
    struct sigaction oldquit;

    /* This thread is responsible for catching any asynchronous signals that are
     * delivered to this process and handling them in an appropriate fashion.
     * Signals currently handled are:  SIGINT, SIGQUIT, SIGTERM
     */
    sigemptyset(newsetp);
    sigaddset(newsetp, SIGINT);
    sigaddset(newsetp, SIGQUIT);
    sigaddset(newsetp, SIGTERM);
    sigprocmask (SIG_SETMASK, newsetp, NULL);

    newint.sa_handler = SIG_IGN;
    sigemptyset(&newint.sa_mask);
    newint.sa_flags   = 0;
    newquit.sa_handler = SIG_IGN;
    sigemptyset(&newquit.sa_mask);
    newquit.sa_flags   = 0;

    while (!abort) {
        /* now wait for one of the above signals to be delivered */
        ret = sigwait(newsetp, &sig_delivered);

        if (ret != 0) {
            /* error in sigwait(), no signal delivered... */
            continue;
        }

        if (sig_delivered == SIGINT || sig_delivered == SIGQUIT) {
            /* Ignore any new asynch signals that come in while
             * we do an interactive query to abort the process.
             */
            sigaction(SIGINT, &newint, &oldint);
            sigaction(SIGQUIT, &newquit, &oldquit);
            abort = want_abort();
            sigaction(SIGQUIT, &oldquit, NULL);
            sigaction(SIGINT, &oldint, NULL);
        } else {
            /* non-interactive or unblocked signal received; terminate */
            abort = 1;
        }
    }
    abort_now();
    return OKAY;
}


/* Interactive query to abort; return TRUE if abort desired, else FALSE.
 * If stdin is being used for input, or the user cannot respond, then this 
 * routine will return TRUE. This routine may be called from threads other 
 * than the signal_thread to prompt user for abort.
 */
int want_abort( )
{
    char     abort_answer[10];          /* contains overwrite answer */
    int valid = 0;

    if (!Local_tty) { /* can't prompt user; no local tty */
    	Abort_restore = 1;
        return(1);
    }

    while (!valid ) {            /* answer must be yes or no */
        pthread_mutex_lock(&tty_mutex);
        Pause_restore = TRUE;
        fprintf (stderr,
            catgets(catd, S_VRESTORE1, VRESTORE168, 
            "\n%s: Do you want to abort the restore? (yes or no) "),
            Prog);

        /*
         * We need an answer from the user.  If there is no way to
         * get one (Local_tty is NULL), then we returned above.
         * Use Local_tty if Use_stdin is set, otherwise just use
         * stdin to allow a response to be set in scripts.  
         */
        if (Use_stdin) {
            if (fgets (abort_answer, 6, Local_tty) == NULL) {
                abort_answer[0] = 'y';
            }
        }
        else {
            if (fgets (abort_answer, 6, stdin) == NULL) {
                abort_answer[0] = 'y';
            }
        }
        Pause_restore = FALSE;
        pthread_mutex_unlock(&tty_mutex);

        if ((abort_answer[0] == 'y') || (abort_answer[0] == 'Y')) {
            valid = 1;
            Abort_restore = 1;
        } else if ((abort_answer[0] == 'n') || (abort_answer[0] == 'N')) {
            valid = 1;
        } else {
            fprintf(stderr, "\n");
        } 
    } 
    return( Abort_restore );
}

void abort_now()
{
    /* Abort time.  Unlike in advdump, this routine actually exits. */
    remove( Tmp_fname );               /* remove "_tmp" file */
    pthread_mutex_lock(&dev_mutex);
    if (close_device != NULL) {
        /* since close_device is a function pointer we don't
         * want to call it if it hasn't been initialized yet.
         */
        close_device( Src_fd );            /* close the save-set */
    }
    pthread_mutex_unlock(&dev_mutex);

    post_event_backup_error(Prog);

    exit( 1 );
}



/*
 * restore_acl 
 *
 *      This routine restores ACLs.
 */
int
restore_acl(
   char *path,
   union record_header_t rhdr,  /* record header             */
   struct blk_t *blk,           /* in - saveset block containing rec */
   int data_idx,                /* in - Offset into 'blk' of the rec */
   int compressed,	  	/* in - Save-set compressed data flag */
   lzw_desc_handle_t lzwh 	/* in - Compression handle */
		  )
{
    char *acl_buffer;           /* To prevent unaligned access */
    int  err = 0;
    int  num_ents;
    int  total_bytes_decoded = 0, total_bytes_xferred = 0; 
    int  bytes_decoded, bytes_xferred;
    char buf[MAX_BLK_SZ];

    if (rhdr.rheader.size == 0) {
	/* avoid lots of needless code on empty records
	 * and malloc'ing zero bytes
	 * (that would look like a malloc failure)
	 */
	if (List) {
	    list_rec_hdr(catgets(catd, S_VRESTORE1, VRESTORE25,
                           "ACL"), &rhdr.rheader, stdout);
	}
	return(err);

    }

    if (compressed){
	start_uncompress( lzwh );
    	
        /*
         * Uncompress the data in the data record.
         * This has to be completed before we can
	 * malloc a transfer buffer
         */
        while (total_bytes_decoded < rhdr.rheader.size) {
            err = uncompress( lzwh,
                       &blk->data[data_idx + total_bytes_decoded],
                       rhdr.rheader.size - total_bytes_decoded,
                       buf,
                       sizeof( buf ),
                       &bytes_decoded,
                       &bytes_xferred );

	    if (err == -1) {
		/* when uncompress doesn't like the data
	 	 * its useless to continue with this record
		 */
            	finish_uncompress( lzwh, buf);
        	fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE216, 
                   "%s: Unexpected termination (%d) of ACL record, skipping\n"),  
                   Prog, 2);
		/* avoid confusion from undefined errno */
		errno = 0;
		if (List) {
	    	    list_rec_hdr(catgets(catd, S_VRESTORE1, VRESTORE25,
                        "ACL"), &rhdr.rheader, stdout);
		}
		return(-1);
    	    }

            total_bytes_decoded += bytes_decoded;
            /* we've transferred some translated data to the buffer */
            total_bytes_xferred += bytes_xferred;
        }
        /* data to be processed is in buf */
    } else {
        /* not compressed */
        total_bytes_xferred = rhdr.rheader.size;
        /* data to be processed is in   &blk->data[data_idx] */
    } /* acl data is now ready to be used to set acls */
    
    /*
     * Malloc space for the possibly combined buffer
     */
    acl_buffer = malloc(total_bytes_xferred);
    if (acl_buffer == NULL){
    	fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE214, 
		"%s: unable to malloc space for ACL\n"), Prog);
        exit ( 1 );
    }

    if (compressed) {
        /* from the tranlation buffer */
        memcpy(acl_buffer, buf, total_bytes_xferred) ;
    } else {
        /* from the read buffer */
        memcpy(acl_buffer, &blk->data[data_idx], total_bytes_xferred);
    }

    /* try to set the acl.  Can legitimately fail if SYSV ACLs not 
     * supported by the target filesystem 
     */
    num_ents = sizeof(acl_buffer) / sizeof(struct acl);

    if (num_ents > 0) {
        err = acl(path, ACL_SET, num_ents, (struct acl*)acl_buffer);
    }

    if (err == -1) {
printf("acl_set failed with errno %d\n", errno);
        if ((errno == EOPNOTSUPP) || (errno == ENOSYS)) {
            err = 0;
        } 
    }

    free(acl_buffer);

    return(err);

} /* end restore_acl */

/*
 * file_compare_error -
 *
 *      This routine prints out an error message.
 */
void
file_compare_error(
    char         *file_name,		   
    char 	 *file_type,         
    struct stat  stat_buf)
{
  char        file_type2[64];

  switch (stat_buf.st_mode & S_IFMT) {
      case S_IFDIR:
                   strcpy(file_type2,catgets(catd, S_VRESTORE1, VRESTORE215,
					     "directory"));
		   break;
      case S_IFREG:
		   strcpy(file_type2,catgets(catd, S_VRESTORE1, VRESTORE214,
					     "file"));
		   break;
      case S_IFLNK:
		   strcpy(file_type2,catgets(catd, S_VRESTORE1, VRESTORE217,
					     "symbolic link"));
		   break;
      case S_IFIFO:
      case S_IFCHR:
      case S_IFBLK:
		   strcpy(file_type2,catgets(catd, S_VRESTORE1, VRESTORE216,
					     "device"));
		   break;
      default:
		   strcpy(file_type2,catgets(catd, S_VRESTORE1, VRESTORE218,
					     "unknown"));
  }

  fprintf(stderr, 
	  catgets(catd, S_VRESTORE1, VRESTORE213, 
		  "%s: cannot overwrite %s with %s, already exists as %s.\n"),
	  Prog, file_name, file_type, file_type2);
  return;
}
/* end vrestore.c */
