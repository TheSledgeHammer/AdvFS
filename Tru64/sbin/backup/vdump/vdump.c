/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992                      *
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
 *
 *      File System
 *
 * Abstract:
 *
 *      Implements the backup program.
 *
 * Date:
 *
 *      Thu Apr 19 11:45:08 1990
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: vdump.c,v $ $Revision: 1.1.148.11 $ (DEC) $Date: 2004/11/09 06:18:17 $";
#endif
static char *rcsdate = "$Date: 2004/11/09 06:18:17 $";

#include "../shared/backup.h"
#include "../shared/tapefmt.h"
#include "../shared/util.h"
#include "../shared/devlist_util.h"
#include "../shared/blkpool_util.h"
#include "../shared/lzw.h"
#include <ctype.h>
#include <signal.h>
#include <sys/fs_types.h>
#ifdef XTENDED_ATTRBS
#include <sys/proplist.h>
#endif
#include <msfs/bs_error.h>
#include <io/common/devgetinfo.h>
#include <locale.h>
#include <sys/fcntl1.h>
#include <sys/param.h>
#include "vdump_msg.h"
#include "../shared/libvdump_msg.h"
#include <msfs/bs_ods.h>

/* #include <autofs/autofs.h> */
#define AUTOFS_INTERCEPT        0x10000000 /* intercept point */

nl_catd catd;
nl_catd comcatd;

/*
 * save_set_t - used to describe the state of save-set blocks 
 */
typedef struct {
   struct blk_t *blk;                 /* current block                      */
   struct blk_t *xor_blk;             /* current XOR block                  */
   int data_idx;                      /* index into data part of a block    */
   int block_size;                    /* bytes per block                    */
   int blk_num;                       /* current save-set block number      */
   int vol_set_num;
   int vol_sets;
   time_t ss_id;
   lzw_desc_handle_t lzwh; 
   u_int save_mode;                   /* see blk hdr flags in tapefmt.h     */
   int xor_block_num;                 /* zero-relative                      */
   int var_len_rec_idx;               /* index into data part of a block of */
                                      /* the current variabl length record's*/
                                      /* header                             */
   int var_len_rec_in_progress;       /* if TRUE we are processing a        */
                                      /* variable length record             */
   union old_record_header_t var_len_rec_hdr; /* cur var length record's hdr    */
} save_set_t;

const save_set_t nil_save_set = { 0 };

char months[] = "JanFebMarAprMayJunJulAugSepOctNovDec";

struct timeval start_time;

/*
 * msg_type_t - defines the various message types passed among the threads
 */
typedef enum {
   MSG_WRITE_BLK,
   MSG_TERMINATE
} msg_type_t;

/*
 * mesg_t - used to pass messages among threads
 */
typedef struct msg {
   msg_type_t type;
   struct blk_t *blk;
} mesg_t;

static const mesg_t Terminate_msg = { MSG_TERMINATE };
msg_q_handle_t Wt_mq = NULL;          /* write_thread message queue */

/* 
 * function variables for device-specific functions 
 */
static int (*_write_blk)( int fd, char *blk, int cnt );


void (*close_device)( int fd ) = NULL;

/*
 * prototypes
 */

t_address write_thread( t_address input_ptr );
int write_blk( struct blk_t **blk, int cnt );
void backup( char *source, save_set_t *ss, unsigned long *file_cnt );
void mv2blk( save_set_t *ss, char *buf, int bytes2mv, int no_span );
int link_tbl_add( ino_t, char *, ino_t  );
int link_tbl_lookup( ino_t ino, char **name, ino_t *parent_ino );
static int hardlink_tbl_add(ino_t, uint_t );
static void lookup_for_missed_hardlinks( void );
static void free_hardlink_tbl(void);
static int check_if_mountpoint(const char *, const char *);

extern int tape_unload( int devFd, int tape_num );
extern int tape_rewind( int devFd, int tape_num );
extern int tape_get_new( char *device_name, int tape_num, char *remote_host_name );
extern int tape_write_eof( int devFd, int tape_num );
extern int tape_clear_error( int devFd, int tape_num );

void
start_backup( 
   char *source,
   unsigned long *file_cnt,
   unsigned long *bytes_read,
   unsigned long *bytes_written
   );

void
start_var_len_rec( 
   save_set_t *ss, 
   union old_record_header_t *rec_hdr 
   );

void finish_var_len_rec(save_set_t *ss);
void write_summary_rec(save_set_t *ss, char *source);
void tape_bind( );
void file_bind( );
char *make_norewind_dev (char *dev);
int check_for_norewind (char *dev);
void usage( );
void list_fs_to_backup();
void find_tape_size();
void backup_advfs_quotas(save_set_t *ss, char *ufilename, char *gfilename,
                         struct stat *ustatp, struct stat *gstatp);
void
write_dir_attr_rec(
   save_set_t *ss,              /* in - save-set descriptor */
   char *dir_name,              /* in - directory's name */
   struct stat *dir_stat,       /* in - directory's stats */
   ino_t parent_ino,            /* in - directory's parent's inode */
   int as_attr                  /* in - flag indicates if the attr rec is */
   ) ;

#ifdef XTENDED_ATTRBS
int
backup_proplist(
		int fd,                           /* in     */
		char *path,                       /* in     */
		int follow,			  /* in	    */
		save_set_t *ss                    /* out    */
		);
#endif

/*
 * Signal handler routines 
 */
void prepare_signal_handlers( void );
t_start_routine signal_thread( void * );
int want_abort( void );
void abort_now( void );

/* 
 * global variables -- for all modules (not static)
 */
Dev_type_t Dev_type;
int Debug = FALSE;
int No_rewind = FALSE;
int No_unload = FALSE;
int Show_resources = FALSE;
int saved_err = 0;		/* store error occurrence for later reporting */
				/* 0 = success, 1 = failure, 3 = warning */
int Silent_mode = FALSE;
char *Prog = NULL;
int Vdump = 1;
char *Dump_hist_file_name = "/etc/vdumpdates";
int Blk_size = 0;          
char Device_name[MAXPATHLEN + MAXHOSTNAMELEN + MAXUSERNAMELEN + 1];
char *remote_host_name = NULL;
char *device_name_ptr = NULL;

/* tty_mutex is used around informational messages to prevent and
 * error message from trampling the informational message in the
 * middle of its output.  And tty_mutex is also used to allow a
 * user to respond to a single question and not error message
 * questions spit out by another thread.
 */
t_mutex  tty_mutex;		/* Global; used by libvdump routines */
t_mutex  abrt_mutex;	/* used to synch access to abort_wanted & Wt_mq vars */

static int OutFd = -1; 
static long Bytes_written = 0L;
static long Bytes_read = 0L;
FILE *Local_tty = stdin;
static int  abort_wanted = 0;	/* Set to TRUE if abort is desired */

extern char os_name[];

/* 
 * global variables -- for this module (static)
 */

static int 
   Use_stdout = FALSE,
   Reliable = FALSE, 
   Block_mode = FALSE, 
   Compress_data = FALSE, 
   Blocks = DEFAULT_BLOCKS,
   Verbose = FALSE,
   Xor_blocks = DEFAULT_XOR_BLOCKS,
   Update_file = TRUE,
   Dump_level = 9, 
   Use_previous_format = FALSE,
   Backup_zeros = TRUE,		   /* last minute change; see history */
   Tape_number = 1,
   Backup_subtree = FALSE;

static long
   Tape_Size = 0,
   Tape_density = 0,
   Total_dump_time = 0,            /* static time_t??? */
   Last_est_finish_time = 0,
   Last_kbytes_backed_up = 0,
   Alarm_time = 300,               /* want 5 min (300 secs) */
   Num_dirs_to_backup = 0,
   Num_files_to_backup = 0,
   Bytes_backed_up = 0,
   Bytes_to_backup = 0,
   Zeros_skipped = 0,
   Sparse_skipped = 0,
   Last_files = 0,
   Num_files_backed_up = 0,
   Num_dirs_backed_up = 0;

double     
   Avg_kb_file_rate = 0.0;

static time_t
   Last_start_time = 0,
   Time_amount = 0;

static blk_pool_handle_t Blk_pool_h;
static buf_pool_handle_t Buf_pool_h;

fs_info_t Fs_info;

/* 
 * Buffer for read() calls in backup_file().
 */
static unsigned long buf[ LBUF_SIZE ];

/* 
 * Block I/O defines
 */
#define DATA_FREE( bsize, data_idx ) \
    (MAX_DATA_SIZE( bsize ) - (data_idx) - OLD_RECORD_HEADER_SIZE)

#define NO_SPAN    1
#define SPAN_OK     0

struct itimerval timeval,
                 *timeret;

   unsigned long file_cnt = 0;     /* TODO: put back in main */



/*
 * main -
 *
 * Parse command args (argv[]) and start the backup
 */
main( int argc, char *argv[] )
{
    /* argv variables */
    int c, C = 0, D = 0, N = 0, P = 0, U = 0, V = 0, F = 0, Z = 0, level = 0,
       f = 0, m = 0, q = 0, t = 0, r = 0, u = 0, v = 0, w = 0,
       x = 0, b = 0, i = 0, h = 0, T = 0, g = 0;
    int B = 0, S = 0, d = 0, s=0;    /* allow these options to be given */

    extern int optind, opterr;
    extern char *optarg;

   int 
      j,       
      done, 
      backup_blocks = FALSE,      
      help = FALSE,               
      file_spec = FALSE,          
      device = FALSE;

   char *p;
   char *f_file = NULL;          /* write dump to f_file */
   char *source = NULL;
   char *src_dir_name = NULL;
   char *Device_name_ptr = NULL;   /* used for remote dumps only */
   char *remote_user_name = NULL;
   char *tmp_ptr = NULL;

   /* misc variables */
   char cwd[MAX_PATH_SZ];
   struct stat stat_buf,src_dir_stat_buf;
   struct stat backup_stat_buf;
   struct statfs fs_stats_tmp;
   int link_Mount = FALSE;
   int auto_Mount = FALSE;
   char pathname[MAX_PATH_SZ];
   mlBfSetParamsT setParams;

   /* statistics variables */
   unsigned long bytes_read = 0, bytes_written = 0;
   struct timezone tz;
   time_t current_time;

   char *getcwd( char *buf, size_t size );

   /*------------------------------------------------------------------------*/
 
   (void) setlocale(LC_ALL, "");
   catd = catopen(MF_VDUMP, NL_CAT_LOCALE);
   /* common catalog descriptor for shared messages
    */
   comcatd = catopen(MF_LIBVDUMP, NL_CAT_LOCALE);

   /*check_struct_sizes( );*/

   if (getcwd( cwd, sizeof( cwd ) ) == NULL) {
       perror(catgets(catd, S_VDUMP1, VDUMP1, "cwd"));
       exit( 1 );
   }
   strcat( cwd, "/" );

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    /* 
     * When no args are given, then default options are 9u.  If any other
     * options are given, the 'u' option must be explicitly specified.
     */

    if (argc > 2) {
        Update_file = FALSE;
    }

    /* Create a mutex to guard console interaction among threads.  Don't want
     * the user to have 2 questions to answer at one time.  Also create one
     * to synchronize access to two global variables.
     */
    mutex__create( &tty_mutex );
    mutex__create( &abrt_mutex );
    
    /*
     * Must prepare to get any signal interrupts and to initiate the
     * timer used to estimate the number of kilobytes left to backup.
     */
    prepare_signal_handlers(); 

    if ((argc <= 2) || posix_style (argc, argv)) { /* if POSIX-style of args */
        
      /* 
       * Posix uses dash and the value follows immediately after the option.
       */
      while ((c = getopt( argc, argv, 
                     "CDNPU0123456789f:gmqtruvVwx:b:F:T:hB:d:S:s:Z"
                     ) ) != EOF) {

        switch (c) {
           case '-': 
               break;
           case '0':
               level++;
               Dump_level = 0;
               break;

           case '1':
               level++;
               Dump_level = 1;
               break;

           case '2':
               level++;
               Dump_level = 2;
               break;

           case '3':
               level++;
               Dump_level = 3;
               break;

           case '4':
               level++;
               Dump_level = 4;
               break;

           case '5':
               level++;
               Dump_level = 5;
               break;

           case '6':
               level++;
               Dump_level = 6;
               break;

           case '7':
               level++;
               Dump_level = 7;
               break;

           case '8':
               level++;
               Dump_level = 8;
               break;

           case '9':
               level++;
               Dump_level = 9;
               break;

           case 'B':
               /* single optional param for block device */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP2, "The -B option is ignored.\n"));
               Tape_density = strtoul (optarg, &p, 0 );
               break;

           case 'd':
               /* single optional param for tape density (bytes per inch) */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP3, "The -d option is ignored.\n"));
               Tape_density = strtoul (optarg, &p, 0 );
               break;

           case 'S':
               /* single param required for output file size */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP4, "The -S option is ignored.\n"));
               Tape_Size = strtoul (optarg, &p, 0 );
               break;

           case 's':
               /* single param required for output file size */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP5, "The -s option is ignored.\n"));
               Tape_Size = strtoul (optarg, &p, 0 );
               break;

           case 'C':
               /* no params required for compression */
               C++;
               Compress_data = 1;
               break;

           case 'D':
               /* no params required for back-up subtree */
               D++;
               Backup_subtree = 1;
               break;

           case 'N':
               /* no params required for no tape rewind when done */
               N++;
               No_rewind = TRUE;
               break;

           case 'U':
               /* no params required for no tape unload when done */
               U++;
               No_unload = TRUE;
               break;

           case 'P':
               /* no params required for converting to previous format */
               P++;
               Use_previous_format = TRUE;
               break;

           case 'Z':
               /* no params required for backup zero blocks */
               Z++;
               Backup_zeros = TRUE;
               break;

           case 'f':
               /* one required param for files */
               f++;
               f_file = optarg;

               if (strcmp( f_file, "-" ) == 0) {
                   Use_stdout = TRUE;
                   device = TRUE;
                   strcpy( Device_name, f_file );

               } else if (f_file == NULL) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP6, "\n%s: missing parameter for option <-f>\n"), Prog );
                   f--;

               } else {
                   device = TRUE;
                   if(strlen(f_file) > (MAXHOSTNAMELEN+MAXPATHLEN+MAXUSERNAMELEN)  ) {
                       fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP164,
                           "\n%s: parameter for option <-f> greater than allowed. ([user@[%d]]host[%d]::path[%d])\n"),
                           Prog, MAXUSERNAMELEN, MAXHOSTNAMELEN, MAXPATHLEN);
                       goto bad_end;
                   } else {
                       strcpy( Device_name, f_file );
                       device_name_ptr = optarg;
                   }
               }
               break;

           case 'g':
               Debug = TRUE;
               break;

           case 'q':
               /* no params required for quiet, silent mode */
               q++;
               Silent_mode = TRUE;
               break;

           case 'r':
               /* no params required for resources */
               r++;
               Show_resources = TRUE;
               break;

           case 'u':
               /* no params required for update */
               u++;
               Update_file = TRUE;
               break;

           case 'v':
               /* no params required for verbose */
               v++;
               Verbose = TRUE;
               break;

           case 'V':
               /* no params required for vdump version */
               fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP7, "%s  Version %s\n"), rcsdate, VERSION_STRING );
               goto _end;
               break;

           case 'w':
               /* no params required */
               list_fs_to_backup();
               goto _end;
               break;

           case 'x':
               /* single optional param for xorblocks */
               x++;
               Reliable = TRUE;
               Xor_blocks = strtoul (optarg, &p, 0 );

               if ((Xor_blocks < MIN_XOR_BLOCKS) || 
                   (Xor_blocks > MAX_XOR_BLOCKS)) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP8, "%s: param for option <-x> out of range [%d..%d]\n"), 
                           Prog, MIN_XOR_BLOCKS, MAX_XOR_BLOCKS );
                  x++;

               }
               break;

           case 'b':
               /* single optional param for block size */
               b++;
               Blk_size = strtoul (optarg, &p, 0 );

               if ((Blk_size < MIN_CHUNKS) || 
                   (Blk_size > OLD_MAX_CHUNKS)) {
                    fprintf( stderr,
                            catgets(catd, S_VDUMP1, VDUMP9, "%s: param for option <-b> out of range [%d..%d]\n"), 
                            Prog, MIN_CHUNKS, OLD_MAX_CHUNKS );
                    b++;
               }
               Blk_size = Blk_size * BLK_CHUNK_SZ;

               break;

           case 'F':
               /* single param required for number of in-memory buffers */
               F++;
               Blocks = strtoul (optarg, &p, 0 );
               backup_blocks = TRUE;
   
               if ((Blocks < MIN_BLOCKS) || (Blocks > MAX_BLOCKS)) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP10, "%s: param for option <-F> out of range [%d..%d]\n"), 
                           Prog, MIN_BLOCKS, MAX_BLOCKS );
                   F++;    /* signal that error occurred */
               }
               break;

           case 'T':
               /* single param required for output file size */
               T++;
               Tape_number = strtoul (optarg, &p, 0 );

               if (Tape_number < 0) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP11, "%s: param for option <-T> is less than zero (0)\n"), 
                           Prog );
                   T++;
               }
               break;

           case 'h':
               /* no params required for help */

           default:
               usage();
               exit (1);

           }  /* end switch */
     }  /* end while */

    if ( C>1 || D>1 || N>1 || U>1 || level>1 || T>1 ||
         f>1 || m>1 || q>1 || t>1 || r>1 || u>1 || v>1 || 
         x>1 || b>1 || F>1 || h>1 || Z>1 ) {
       usage();
       exit (1);
    }

    /* don't need because doesn't have to have options
    if ( (B + C + D + N + P + U + level + f + m + q + t + r + u + v + x +
          b + F + S + T + h + d) < 0 ) {
        usage();
        exit (1);
    }
    */

    if (optind != (argc - 1)) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    /* get file system path name */
    source = argv[optind];

 } else {                  /* BSD style of arguments */
/* ************************************************** */
     /* BSD style uses first argument string to determine the
      * meaning of the following arguments. In this case, the
      * value doesn't follow immediately the option.
      */
     char *arg;
     
     if(argc > 1) {
         argv++; argc--;
         arg = *argv;
         if (*arg == '-')
             argc++;
     } else {
         usage();
         exit (1);
     }

     while(*arg) {
         switch (*arg++) {

           case '0':
               Dump_level = 0;
               break;

           case '1':
               Dump_level = 1;
               break;

           case '2':
               Dump_level = 2;
               break;

           case '3':
               Dump_level = 3;
               break;

           case '4':
               Dump_level = 4;
               break;

           case '5':
               Dump_level = 5;
               break;

           case '6':
               Dump_level = 6;
               break;

           case '7':
               Dump_level = 7;
               break;

           case '8':
               Dump_level = 8;
               break;

           case '9':
               Dump_level = 9;
               break;

           case 'B':
               /* single optional param for block device */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP2, "The -B option is ignored.\n"));
               argv++; argc--;
               break;

           case 'd':
               /* single optional param for tape density (bytes per inch) */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP3, "The -d option is ignored.\n"));
               argv++; argc--;
               break;

           case 'S':
               /* single param required for output file size */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP4, "The -S option is ignored.\n"));
               argv++; argc--;
               break;

           case 's':
               /* single param required for output file size */
               fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP5, "The -s option is ignored.\n"));
               argv++; argc--;
               break;

           case 'C':
               /* no params required for compression */
               Compress_data = TRUE;
               break;

           case 'D':
               /* no params required for back-up subtree */
               Backup_subtree = TRUE;
               break;

           case 'N':
               /* no params required for no tape rewind when done */
               No_rewind = TRUE;
               break;

           case 'P':
               /* no params required for converting to previous format */
               Use_previous_format = TRUE;
               break;

           case 'U':
               /* no params required for no unload when done */
               No_unload = TRUE;
               break;

           case 'f':
               /* one required param for files */
               if (argc > 1) {
                 argv++; argc--;
                 f_file = *argv;
               }

               if (strcmp( f_file, "-" ) == 0) {
                   Use_stdout = TRUE;

               } else if (f_file == NULL) {
                   fprintf( stderr,
                            catgets(catd, S_VDUMP1, VDUMP6, "\n%s: missing parameter for option <-f>\n"), 
                            Prog );
                   usage();
                   exit (1);
               }

               device = TRUE;
               if(strlen(f_file) > (MAXHOSTNAMELEN+MAXPATHLEN)  ) {
                   fprintf( stderr,
                       catgets(catd, S_VDUMP1, VDUMP164,
                       "\n%s: parameter for option <-f> greater than allowed. ([user@[%d]]host[%d]::path[%d])\n"),
                       Prog, MAXUSERNAMELEN, MAXHOSTNAMELEN, MAXPATHLEN);
                   goto bad_end;
               } else {
                   strcpy( Device_name, f_file );
                   device_name_ptr = *argv;
               }
               break;

           case 'q':
               /* no params required for quiet, silent mode */
               q++;
               Silent_mode = TRUE;
               break;

           case 'r':
               /* no params required for resources */
               Show_resources = TRUE;
               break;

           case 'u':
               /* no params required for update */
               Update_file = TRUE;
               break;

           case 'v':
               /* no params required for verbose */
               Verbose = TRUE;
               break;

           case 'Z':
               /* no params required for zeros */
               Backup_zeros = TRUE;
               break;

           case 'V':
               /* no params required for vdump version */
               fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP7, "%s  Version %s\n"), rcsdate, VERSION_STRING );
               goto _end;
               break; 

           case 'w':
               /* no params required */
               list_fs_to_backup();
               goto _end;
               break;

           case 'x':
               /* single optional param for xorblocks */
               if (argc > 1) {
                   argv++; argc--;
                   Xor_blocks = atoi(*argv);
               }
               Reliable = TRUE;

               if ((Xor_blocks < MIN_XOR_BLOCKS) || 
                   (Xor_blocks > MAX_XOR_BLOCKS)) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP8, "%s: param for option <-x> out of range [%d..%d]\n"), 
                           Prog, MIN_XOR_BLOCKS, MAX_XOR_BLOCKS );
                   goto bad_end;

               }
               break;

           case 'b':
               /* single optional param for block size */
               if (argc > 1) {
                   argv++; argc--;
                   Blk_size = atoi(*argv);
               }

               if ((Blk_size < MIN_CHUNKS) || 
                   (Blk_size > OLD_MAX_CHUNKS)) {
                    fprintf( stderr,
                            catgets(catd, S_VDUMP1, VDUMP9, "%s: param for option <-b> out of range [%d..%d]\n"), 
                            Prog, MIN_CHUNKS, OLD_MAX_CHUNKS );
                    goto bad_end;
               }
               Blk_size = Blk_size * BLK_CHUNK_SZ;

               break;

           case 'F':
               /* single param required for number of in-memory buffers */
               if (argc > 1) {
                   argv++; argc--;
                   Blocks = atoi(*argv);
               }
               backup_blocks = TRUE;
   
               if ((Blocks < MIN_BLOCKS) || (Blocks > MAX_BLOCKS)) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP10, "%s: param for option <-F> out of range [%d..%d]\n"), 
                           Prog, MIN_BLOCKS, MAX_BLOCKS );
                    goto bad_end;
               }
               break;

           case 'T':
               /* single param required for output file size */
               Tape_number = -1;
               if (argc > 1) {
                   argv++; argc--;
                   /*
                    * special case because atoi returns 0 when it
                    * encounters an error so check if user meant 0.
                    */
                   if ( strcmp(*argv, "0") == 0 ) {
                       Tape_number = 0;
                       break;
                   }
                   Tape_number = atoi(*argv);
               }

               if (Tape_number < 1) {
                   fprintf( stderr,
                           catgets(catd, S_VDUMP1, VDUMP11, "%s: param for option <-T> is less than zero (0)\n"), 
                           Prog );
                    goto bad_end;
               }
               break;

           case 'h':
               /* no params required for help */

           default:
               usage();
               exit (1);

           }  /* end switch */
       }  /* end while */

       if (argc > 1) {
           argv++; argc--;
           if (argc == 1) {
               source = *argv;
           } else {
               fprintf(stderr,catgets(catd, S_VDUMP1, VDUMP12, 
                       "%s: Can dump only one file system (%s) at a time.\n"),
                       Prog, *argv);
               goto bad_end;
           }
       }
   
    }  /* end BSD style of arguments */
    
    if (source == NULL) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP13, 
                 "%s: missing filesystem\n"), Prog );
        usage();
        exit (1);
    }

    if (lstat( source, &stat_buf ) == ERROR) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14, 
                 "%s: error accessing file system <%s>; [%d] %s\n"),
                 Prog, source, errno, ERR_MSG );
        goto bad_end;
    }
#ifdef REMOTE
        /* decode device_name_ptr in format [user@]host:tape into */
        /* remote_host_name and Device_name components */
        tmp_ptr = (char *) index(device_name_ptr, '@');
        if (tmp_ptr) {
                if (tmp_ptr != device_name_ptr) {
                        *tmp_ptr = '\0';
                        ++tmp_ptr;
                        remote_user_name = device_name_ptr;
                        device_name_ptr = tmp_ptr;
                }
                else {
                        fprintf( stderr, catgets(catd, S_VDUMP1, NEEDKEY,
                            "%s: Need option 'f' followed by remote device \"[user@]host:path\"\n"),
                            Prog );
                        goto bad_end;

                        /* NOT REACHED */
                }
        }
        remote_host_name = device_name_ptr;

        device_name_ptr = (char *) index(remote_host_name, ':');
        if (device_name_ptr == NULL || device_name_ptr == remote_host_name)
        {
           fprintf( stderr, catgets(catd, S_VDUMP1, NEEDKEY,
                    "%s: Need option 'f' followed by remote device \"[user@]host:path\"\n"), Prog );
           goto bad_end;
        }
        *device_name_ptr = '\0';
        ++device_name_ptr;

        /* now check all parameters for length too large */
        if((remote_user_name) && (strlen(remote_user_name) > MAXUSERNAMELEN)) {
            fprintf( stderr,
                    catgets(catd, S_VDUMP1, VDUMP167,
                    "\n%s: user parameter for option <-f> greater than allowed. (user[%d])\n"),
                    Prog, MAXUSERNAMELEN);
            errno = ENAMETOOLONG;
            goto bad_end;
        }
        if(strlen(remote_host_name) > MAXHOSTNAMELEN) {
            fprintf( stderr,
                catgets(catd, S_VDUMP1, VDUMP166,
                "\n%s: host parameter for option <-f> greater than allowed. (host[%d]::path[%d])\n"),
                Prog, MAXHOSTNAMELEN, MAXPATHLEN);
            errno = ENAMETOOLONG;
            goto bad_end;
        }
        strcpy(Device_name, device_name_ptr);
        if(strlen(Device_name) > MAXPATHLEN) {
            fprintf( stderr,
                catgets(catd, S_VDUMP1, VDUMP165,
                "\n%s: path parameter for option <-f> greater than allowed. (host[%d]::path[%d])\n"),
                Prog, MAXHOSTNAMELEN, MAXPATHLEN);
            errno = ENAMETOOLONG;
            goto bad_end;
        }


        /* establish connection to remote host */
        rmthost(&remote_host_name, remote_user_name);

        (void) setuid(getuid());        /* rmthost() is the only reason to be setuid */

#else /* !REMOTE */
    if (!device) {
      /* no media was specified so use a default device */

      if (No_rewind) {
	  strcpy(Device_name, DEFTAPE_NH); /*"/dev/ntape/tape0_d1" in mtio.h*/
      } else {
	  strcpy(Device_name, DEFTAPE);    /*"/dev/tape/tape0_d1" in mtio.h */
      }

    } else
#endif /* REMOTE */
    {
      /*
       * If the device is not /dev/null and if the No_rewind
       * flag is set, check to see if the user specified a 
       * rewind device, if yes convert it to a norewind device
       */
      if ((strcmp (Device_name, "/dev/null") != 0) &&  No_rewind ) {
	/*
	 * If user specifies no rewind tape (-N) but gives device
	 * /dev/rmt??, then must make it /dev/nrmt?? so closing
	 * the device will not force it to rewind.
	 */
           
	strcpy( Device_name, make_norewind_dev( Device_name ) );
      }
 
    }

    gettimeofday( &start_time, &tz );

    if (vdump_open_device( &OutFd, Device_name ) == ERROR) {
      goto bad_end;
    }

#ifndef REMOTE
    if(strcmp(Device_name,"-")) {
    /* if output device is not stdout */
    /* 
     * Backup 'device' (file) exists in the
     * the fileset to backed up.  This causes the vdump to be twice 
     * the size it needs to be.  The fix will be to check to see if 
     * the backup device is in the fileset being backed up and if so 
     * warn the user.  
     * 
     * One way we could check this is to see if the 'source' is a parent 
     * of the Device_name.  This fails in some cases.  
     * 
     * Example Success: Source = /demo, Device_name = /demo/vdump.file 
     * 
     * Example Failure: Source = /, Device_name = /dev/rmt0h 
     * 
     * So we will need to check to see if the 'device_name' being backed 
     * up to is a file or a device.  If it is a device ignore this check.  
     * 
     * Another case is were you have a fileset mounted on the fileset 
     * your backing up, and your backup file is on the second fileset 
     * 
     * Example Failure: Source = /, Device_name = /usr/tmp/vdump.file 
     * 
     * So we also need to check if the file in question is part of the 
     * original fileset, so a quick strcmp is out of the question.  
     * 
     * What we can do is an lstat of the both the Source and the 
     * Device_name.  If the Device_name is a file type then check to 
     * to see if they are mounted on the the same device.  
     */ 

        if (lstat(Device_name, &backup_stat_buf ) == ERROR) { 
            fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP147,
			    "%s: error accessing device <%s>; [%d] %s\n"),
                            Prog, Device_name, errno, ERR_MSG );
            goto bad_end;
        }

        /*
         * Step one - check to see if Device_name is a file.
         */
    
        if (S_ISREG(backup_stat_buf.st_mode)) {
              /*
               * We now know that the Device_file is a reg file.
               * 
               * Step two - check if the file is on the same device being backed up.
               *
               * Prior to V4.0 the following check might not work.  I remember
               * that someone made a fix to AdvFS to make sure that each fileset
               * returned unique st_dev for that release.  If this is backported
               * this will have to be verified.
               */

          if (stat_buf.st_dev == backup_stat_buf.st_dev) {
	    /*
	     * We now have a problem.  The file is of type reg, and it
	     * is located on the same device as the backup.  We now need
	     * to allow the user to decide if we should continue.
	     */

	    fprintf(stderr,catgets(catd, S_VDUMP1, VDUMP148,
                    "%s: %s is on the same device as %s, this could\n"),
		    Prog, Device_name, source);
	    fprintf(stderr,catgets(catd, S_VDUMP1, VDUMP149,
                    "%s: cause recursive back up problems.\n"), Prog);
            if (want_abort()) {
	      abort_now();
	      return ERROR;
	    }
          }
        }
    } /* end if not stdout */
#endif

    get_fs_info( source, &Fs_info, Backup_subtree, TRUE );


    /* see if source is really a link pointing to a mounted directory. 
     * Indentation corrected after code modification.
     */
    if( (S_ISLNK(stat_buf.st_mode)) && (realpath(source, pathname)) ) { /* get real path of the source */

        /* check for automount point */
        src_dir_name = dirname(source);
        if ( src_dir_name  == NULL ) {
            /*
             * We didn't pass the name with directory component
             */
            src_dir_name = ".";
        }

        if (lstat( src_dir_name, &src_dir_stat_buf ) == ERROR) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14, "%s: error accessing file system <%s>; [%d] %s\n"),
                    Prog, source, errno, ERR_MSG );
            goto bad_end;
        }

        if (src_dir_stat_buf.st_dev != stat_buf.st_dev)
            auto_Mount = TRUE;

        if( !statfs( pathname, &fs_stats_tmp ) )  {   /* get mount point */

            if( check_if_mountpoint(pathname, fs_stats_tmp.f_mntonname) ) {
                /* is mount point same as real */
                link_Mount = TRUE;
            }

        } else { 
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14, "%s: error accessing file system <%s>; [%d] %s\n"),
                    Prog, pathname, errno, ERR_MSG );
            goto bad_end;
        }
    }

    if(auto_Mount || link_Mount) {
       /*
        * AUTOMOUNT NFS link directory or symbolic link to filset was input
        *
        * (usually /tmp_mnt/"linkname", but not always for automounts).
        * (usually /cluster/members/{memb}/"linkname", for clusters).
        *
        * Process them as a regular file system and not a subtree.
        *
        * Perform a normal backup on the mounted file system.
        */

        if (Backup_subtree)     /* if they entered a -D perform a levl 0 dump. */
           Dump_level = 0;

        if (chdir( pathname ) < 0) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP168,
                     "%s: error accessing link directory <%s> actual mount point <%s>; [%d] %s\n"),
                     Prog, source, pathname, errno, ERR_MSG );
            goto bad_end;
        }

    } else {

        /* non-nfs automounted directory was input */
        /* regular nfs mounts(not automounted) come here too */

        if (Backup_subtree) {    /* if not backing up whole file sys */
           /*
            * Don't come in here if source is a link to a mounted filesystem.
            */

           if (chdir( source ) < 0) {
               fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14,
                        "%s: error accessing file system <%s>; [%d] %s\n"),
                        Prog, source, errno, ERR_MSG );
               goto bad_end;
           }

           Dump_level = 0;

        } else {                       /* find file system and back it up */
            /* Indentation corrected for the else part.*/
            if(!check_if_mountpoint(source, Fs_info.path)) {
                fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP142,
                            "%s: %s is not a mounted fileset; mount fileset or use -D to dump.\n"),
                        Prog, source);
                goto bad_end;
            }

            if (chdir( Fs_info.path ) < 0) {
                fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14,
                            "%s: error accessing file system <%s>; [%d] %s\n"),
                        Prog, Fs_info.path, errno, ERR_MSG );
                goto bad_end;

            }
       }
   }

   post_event_backup_lock(Prog);

   start_backup( source, &file_cnt, &bytes_read, &bytes_written );

   if (close_device)		
       close_device( OutFd );

   /*
    * If we are in the process of aborting, print out a terminating
    * message and return an error exit status from bad_end.
    */

   mutex__lock( abrt_mutex );
   if (abort_wanted) {
       mutex__unlock( abrt_mutex );
       fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP36, "%s: terminating\n"),
                Prog );
       goto bad_end;
   }
   mutex__unlock( abrt_mutex);

   current_time = time(NULL);
   (void) print_progress( current_time );
   
   /* 
    * To display the compression ratio in verbose mode when -C is on
    * and there are bytes to compress.
    * note: Compression is not always possible
    */
    
   if( Compress_data && Bytes_to_backup && v) {
	if ( Bytes_to_backup > bytes_written ) {
		silent_fprintf(catgets(catd, S_VDUMP1, VDUMP171, "%s: Compression ratio %4.1f%%\n"), Prog, (((bytes_written > 0) ? ((Bytes_to_backup - bytes_written)*100.0 / Bytes_to_backup ) : (100.0))));
	} else {
		silent_fprintf(catgets(catd, S_VDUMP1, VDUMP172, "%s: Compression not effective\n"), Prog);
        }
   }

   silent_fprintf(catgets(catd, S_VDUMP1, VDUMP15, "%s: Dump completed at %s"), Prog, ctime( &current_time ));

_end:

   if (Debug) {
      show_statistics(bytes_read, bytes_written, file_cnt, start_time);
   }
/*
   mutex__delete( &tty_mutex );
   mutex__delete( &abrt_mutex );
*/
   if (saved_err) {
        post_event_backup_error(Prog);
	exit(saved_err);
   } else {
        post_event_backup_unlock(Prog);
  	exit(0);
   }
bad_end:

   post_event_backup_error(Prog);

   if (Debug) {
	show_statistics(bytes_read, bytes_written, file_cnt, start_time);
   }
   mutex__delete( &tty_mutex );
   mutex__delete( &abrt_mutex );
   exit(1);
}

/* end main */

void
start_backup( 
    char* source,                 /* in - path name of filesystem */
    unsigned long *file_cnt,      /* in/out - number of files backed up */
    unsigned long *bytes_read,    /* in/out - number of bytes read */
    unsigned long *bytes_written  /* in/out - number of bytes written */
    )
{
    int status;
    save_set_t ss;
    pthread_attr_t thread_attr;
    pthread_t wt_handle;
 
    /*-----------------------------------------------------------------------*/
 
    status = blk_pool_create( &Blk_pool_h, Blocks, Blk_size );
    if (status == ERROR) {
        abort_prog( catgets(catd, S_VDUMP1, VDUMP16, "%s: can't create block pool\n"), Prog );
    }
 
    ss = nil_save_set;
    ss.block_size = Blk_size;
    ss.blk_num = 1;
    ss.vol_set_num = 1;
    ss.vol_sets = 1;
    ss.ss_id = time( 0 );
   
    mutex__lock( abrt_mutex );
    /* Before doing all this, see if user wants to stop */
    if (abort_wanted) {
        mutex__unlock( abrt_mutex );
        return;
    }

    status = msg_q_create( &Wt_mq );
    mutex__unlock( abrt_mutex );
    if (status == ERROR) {
        abort_prog(catgets(catd, S_VDUMP1, VDUMP17, "%s: can't create write thread's msg queue\n"), Prog);
    }
  
    thread__attr_create( &thread_attr );
    thread__create( &wt_handle,
                    thread_attr,
                    write_thread,
                    (any_t) &Wt_mq );
    thread__yield();

    if (Compress_data) {
        init_compress( &ss.lzwh );
    }
 
    backup( source, &ss, file_cnt ); 

    msg_snd( Wt_mq, (char *) &Terminate_msg, sizeof( Terminate_msg ) );

    thread__join( wt_handle, NULL );

    msg_q_delete( Wt_mq, catgets(catd, S_VDUMP1, VDUMP18, "WriteThread") );

    *bytes_read = Bytes_read;
    *bytes_written = Bytes_written;
 
    blk_pool_delete( Blk_pool_h );
} 

  /*
   * Function:
   *
   *      flush_blk
   *
   * Function description:
   *
   *      Prepares a 'ready' save-set block to be written to the save-set
   *      and then writes the block out.  'Preparation' involves CRC code
   *      calculations and XORing the block with the XOR block.  This 
   *      routine also writes out the XOR blocks and initializes a new
   *      XOR block when an XOR group is completed (written to the save-set).
   *
   * Return value:
   *
   *      ERROR - If an error occurred.
   *      OKAY  - If the function completed successfully
   *
   */

int
flush_blk( 
    save_set_t *ss               /* in - save-set descriptor */
    )
{
    union old_record_header_t filler;
 
    if ((ss->blk == NULL) || (ss->data_idx == 0)) {
        /* There is no current block or it is empty so just return. */
        return OKAY;
    }
 
    /* write filler record to pad out to end of block */
    bzero( &filler, sizeof( filler ) );
    filler.rheader.type = RT_FILLER;
    filler.rheader.size = DATA_FREE( Blk_size, ss->data_idx );
    bcopy( (char*) &filler, 
              &ss->blk->data[ss->data_idx], 
              sizeof( filler ) );
 
    if (Reliable) {
        ss->blk->bhdr.bheader.xor_block_num = ss->xor_block_num + 1;
    }
 
    /* calc CRC16 values for the header (and block for 'reliable' backup) */
    ss->blk->bhdr.bheader.block_crc  = 0;
    ss->blk->bhdr.bheader.header_crc = 0;
    ss->blk->bhdr.bheader.header_crc = crc16( (u_char *) &ss->blk->bhdr, 
                                              sizeof( ss->blk->bhdr ) );
 
    if (Reliable) {
        ss->blk->bhdr.bheader.block_crc = 
                crc16( (u_char *) ss->blk, ss->block_size );
  
        if (ss->xor_block_num == 0) {
            /* initialize xor block */
            blk_allocate( Blk_pool_h, &ss->xor_blk );
            bzero( ss->xor_blk, ss->block_size );
            ss->xor_blk->bhdr.bheader.flags |= BF_XOR_BLOCK;
        }
  
        xor_bufs( (u_char*) ss->xor_blk, 
                  (u_char*) ss->blk, 
                  ss->block_size );
  
        /* circularly increment xor block numer */
        ss->xor_block_num = (ss->xor_block_num + 1) % Xor_blocks; 
    }
    
    /* write the block */
    if (write_blk( &ss->blk, ss->block_size ) == ERROR) {
        return ERROR;
    }
 
    ss->blk = NULL;
 
    if (Reliable && (ss->xor_block_num == 0)) {
        /* write the xor block */
        if (write_blk( &ss->xor_blk, ss->block_size ) == ERROR) {
            return ERROR;
        }
    }
 
    return OKAY;
}

/* end flush_blk */

  /*
   * Function:
   *
   *      finish_vol_set
   *
   * Function description:
   *
   *      This routine is called to flush all blocks in progress.
   *
   * Return value:
   *
   *      ERROR - If an error occurred.
   *      OKAY  - If the function completed successfully
   */

int
finish_vol_set( 
   save_set_t *ss              /* in - save-set descriptor */
   )
{
    union old_record_header_t end_of_vs;
 
    /* write end of vol-set record */
 
    bzero( &end_of_vs, sizeof( end_of_vs ) );
    end_of_vs.rheader.type = RT_END_OF_VOL_SET;
    end_of_vs.rheader.size = 0;
 
    mv2blk( ss, (char *) &end_of_vs, sizeof( end_of_vs ), NO_SPAN );
 
    if (ss->data_idx > 0) {
        /* the last block has data in it so write it out */
        if (flush_blk( ss ) == ERROR) {
            return ERROR;
        }
    }
    
    if (Reliable && (ss->xor_block_num != 0)) {
        /* the xor block needs to be written out */
        if (write_blk( &ss->xor_blk, ss->block_size ) == ERROR) {
            return ERROR;
        }
    }
 
    return OKAY;
}

/* end finish_vol_set */

  /*
   * Function:
   *
   *      init_blk
   *
   * Function description:
   *
   *      Initializes the block header in a block.
   */

void
init_blk( 
    save_set_t *ss      /* in - save-set descriptor */
    )
{
    /* initialize the block and the associated counters/indices */

    bzero( (char *) ss->blk, BLOCK_HEADER_SIZE );
    ss->blk->bhdr.bheader.block_size   = ss->block_size;
    ss->blk->bhdr.bheader.block_num    = ss->blk_num;
    ss->blk->bhdr.bheader.vol_set_num  = ss->vol_set_num;
    ss->blk->bhdr.bheader.vol_sets     = ss->vol_sets;
    ss->blk->bhdr.bheader.volume_num   = 0 /* TODO: Tape_number */;
    ss->blk->bhdr.bheader.ss_id        = ss->ss_id;
    ss->blk->bhdr.bheader.dump_date    = start_time;
    ss->blk->bhdr.bheader.flags       |= ss->save_mode;
 
    strncpy( ss->blk->bhdr.bheader.version, OLD_ARCHIVE_VERSION_STRING, VERSION_SIZE );
 
    if (Compress_data) {
        ss->blk->bhdr.bheader.flags |= BF_COMPRESSED;
    }
 
    if (Reliable) {
        ss->blk->bhdr.bheader.xor_blocks = Xor_blocks;
    }
 
    ss->blk_num++;
    ss->data_idx = 0;
}

/* end init_blk */

/*
 * new_blk
 * 
 * flushes the current block to the backup media.  initializes
 * a new block.
 */

void
new_blk(
    save_set_t *ss               /* in - save-set descriptor */
    )
{
    /*
     * Write the block out to the backup media
     */
   
    if (flush_blk( ss ) == ERROR) {
        abort_prog( catgets(catd, S_VDUMP1, VDUMP19, "%s: new_blk can't flush a block\n"), Prog );
    }
 
    if (blk_allocate( Blk_pool_h, &ss->blk ) == ERROR) {
        abort_prog( catgets(catd, S_VDUMP1, VDUMP20, "%s: new_blk can't allocate a block\n"), Prog );
    }

    /*
     * Start a new block.
     */

    init_blk( ss );
   
    if (ss->var_len_rec_in_progress) {

        /* 
         * Write new record's header 
         */

        ss->var_len_rec_hdr.rheader.size = 0;
        ss->var_len_rec_hdr.rheader.flags |= RF_DATA_CONTINUED;
        ss->var_len_rec_idx = ss->data_idx;
 
        bcopy( (char *) &ss->var_len_rec_hdr, 
                  &ss->blk->data[ss->var_len_rec_idx], 
                  sizeof( ss->var_len_rec_hdr ) );

        ss->data_idx += sizeof( ss->var_len_rec_hdr );
        ss->var_len_rec_hdr.rheader.d_offset_lo = 0;
        ss->var_len_rec_hdr.rheader.d_offset_hi = 0;
    }
}

  /*
   *      mv2blk
   *
   *      Copies 'bytes2mv' bytes from 'buf' to the current block.  If
   *      there are more bytes to move from 'buf' than can fit in the
   *      block, then the current block is filled and written to the
   *      save-set and a new block is allocated.  The remaining bytes
   *      in 'buf' are moved to the newly allocated block.
   */

void
mv2blk( 
    save_set_t *ss,              /* in - save-set descriptor */
    char *buf,                   /* in - buffer to move to a block */
    int bytes2mv,                /* in - num bytes to move from buf to block */
    int no_span                  /* in - if T, rec can't span blocks */
    )
{
    int xfer_size = 0, bytes_moved = 0, bytes_xferred, bytes_compressed;
    int start_new_blk = FALSE, err;
    int orig_bytes2mv = bytes2mv;
    int hold = 0;

    /*-----------------------------------------------------------------*/
 
    if (no_span && 
        (bytes2mv > DATA_FREE( Blk_size, ss->data_idx )) && 
        (bytes2mv > MAX_DATA_FREE( Blk_size))) {

        /* the request cannot fit into current block or a new block */
        abort_prog( catgets(catd, S_VDUMP1, VDUMP21, "%s: internal error; invalid mv2blk request\n"), Prog );
    }
  
    if (ss->blk == NULL) {
        if (blk_allocate( Blk_pool_h, &ss->blk ) == ERROR) {
            abort_prog( catgets(catd, S_VDUMP1, VDUMP22, "%s: mv2blk can't allocate a block\n"), Prog );
        }

        init_blk( ss );
    }
 
    while (bytes2mv > 0) {
	/*
	 * we have more data to move but it's time  
	 * to start a new block if
	 * 1) there isn't room left in the output buffer
	 * and we aren't dealing with compression and an
	 * already started property list record
	 * or
	 * 2) other code has already determined that it
	 * is time to start a new block
	 * or
	 * 3) these bytes aren't allowed to span and there
	 * are more bytes to move than space left in the buffer	
	 * Note:
	 * Property list records, if they span blocks,
	 * must be accompanied by the SPAN flag, since
	 * vrestore can't setproplist in pieces, whereas
	 * data and directory data records can be written
	 * in pieces. We must skip to code that can set
	 * the flag 
	 */
        if (((DATA_FREE( Blk_size, ss->data_idx ) == 0) && 
	     !(Compress_data && 
	       ss->var_len_rec_in_progress && 
	       (ss->var_len_rec_hdr.rheader.type == RT_PROPLIST)) ) ||
            (start_new_blk) ||
            (no_span && (bytes2mv > DATA_FREE( Blk_size, ss->data_idx )))) {

            /* 
             * The block is empty OR we have to start a new block OR
             * we can't span this rec across blocks and it won't fit
             * in the current block, SO start a new block!
             */

	    if (ss->var_len_rec_in_progress) {
	      ss->var_len_rec_hdr.rheader.flags = hold;

	    }

            new_blk( ss );

            start_new_blk = FALSE;
        }

        /* copy data from buf to Blk.data */
  
        if (Compress_data && ss->var_len_rec_in_progress) {

            err = compress( ss->lzwh, &buf[bytes_moved], bytes2mv, 
                            &ss->blk->data[ss->data_idx], 
                            DATA_FREE( Blk_size, ss->data_idx ),
                            &bytes_compressed, &bytes_xferred );

            if ((bytes_xferred == 0) && (bytes_compressed == 0)) {
                /* 
		 * block not big enough; start new one and
	         * flag the old record header as SPAN if necessary 
		 */
	        if (ss->var_len_rec_hdr.rheader.type == RT_PROPLIST){
		  /* PROPLIST records MUST show when spanned */
	          hold = ss->var_len_rec_hdr.rheader.flags;
		  ss->var_len_rec_hdr.rheader.flags = RF_SPAN_RECORD;
                  /* update current record's header in output buffer */
                  bcopy( (char *) &ss->var_len_rec_hdr, 
                      &ss->blk->data[ss->var_len_rec_idx], 
                      sizeof( ss->var_len_rec_hdr ) );
	        }
                start_new_blk = TRUE;

            } else {
                bytes_moved += bytes_compressed;
                bytes2mv -= bytes_compressed;
                ss->data_idx += bytes_xferred;
                xfer_size = bytes_xferred;
            }

        } else {
            xfer_size = MIN( bytes2mv, DATA_FREE( Blk_size, ss->data_idx ) );

	    if (bytes2mv >  DATA_FREE( Blk_size, ss->data_idx )){
	      hold = ss->var_len_rec_hdr.rheader.flags;
	      ss->var_len_rec_hdr.rheader.flags = RF_SPAN_RECORD;
	    }

            bcopy( &buf[bytes_moved], 
                      &ss->blk->data[ss->data_idx], 
                      xfer_size );
            bytes_moved += xfer_size;
            bytes2mv -= xfer_size;
            ss->data_idx += xfer_size;
        }
  
        if (ss->var_len_rec_in_progress && !start_new_blk) {
            /* update current record's header */
            ss->var_len_rec_hdr.rheader.size += xfer_size;

            bcopy( (char *) &ss->var_len_rec_hdr, 
                      &ss->blk->data[ss->var_len_rec_idx], 
                      sizeof( ss->var_len_rec_hdr ) );
        }

    }        /* while */
}

/* end mv2blk */

  /*
   *      start_var_len_rec
   *
   *      This routine is used to initiate a variable length record.  Such
   *      records are used when it is known that data will span several 
   *      blocks; the data portion of a file is a good example.  When
   *      a variable length record is being processed the routine 'mv2blk'
   *      fills each block with a portion of the data so that each block
   *      contains one record containing the data (ie- it writes the
   *      appropriate record header to the beginning of each block).
   *      The record header used is supplied by the caller of this routine.
   *
   *      Variable length records can be created as follows:
   *         1.  call start_var_len_rec
   *         2.  loop until all data bytes have been transferred
   *         4.     call mv2blk to transfer the data
   *         5.  call finish_var_len_rec
   */

void
start_var_len_rec( 
    save_set_t *ss,                /* in - save-set descriptor */
    union old_record_header_t *rec_hdr /* in - ptr to var len rec's header */
    )
{
    ss->var_len_rec_hdr = *rec_hdr;
 
    mv2blk( ss,
            (char *) &ss->var_len_rec_hdr, 
            sizeof( ss->var_len_rec_hdr ), 
            NO_SPAN );
 
    ss->var_len_rec_idx = ss->data_idx - sizeof( ss->var_len_rec_hdr );
    ss->var_len_rec_in_progress = TRUE;
 
    if (Compress_data) {
        start_compress( ss->lzwh );
    }
}

/* end start_var_len_rec */

  /*
   *      finish_var_len_rec
   *
   *      Ends the processing of a variable length record.
   */

void
finish_var_len_rec( 
   save_set_t *ss       /* in - save-set descriptor */
   )
{
    char last_compression_code[2];
 
    if (Compress_data) {
        finish_compress( ss->lzwh, last_compression_code );
  
        Compress_data = FALSE;
  
        mv2blk( ss, 
                last_compression_code, 
                sizeof( last_compression_code ), 
                SPAN_OK );
  
        Compress_data = TRUE;
    }
 
    ss->var_len_rec_in_progress = FALSE;
}

/* end finish_var_len_rec */

/* 
 *  update_dumpdate_file
 *
 *  After the directories and files are backed up, this routine
 *  records the file system, the level of dump, and the date of
 *  completion into the dump history file.  If backing up a MSFS
 *  file system, the history file is /etc/vdumpdates; otherwise
 *  it is /etc/dumpdates.
 *
 *  If that level dump does not exist for the file system or if the 
 *  /etc/vdumpdates file was created, then it will append the new 
 *  record onto the file.  If that level dump does exist for the fs,
 *  it reads the contents of /etc/vdumpdates into memory, overwrites
 *  a matching file system and dump level record with a new date OR
 *  appends a new record onto the end, and then writes the new memory
 *  contents to /etc/vdumpdates.  Level 0 is a special case in that
 *  it will always read the contents into memory and search for it.
 *
 *  Sets saved_err if errors are encountered and continuing
 */

void
update_dumpdate_file ( char *path, time_t last_date, int *found_exact_level )
{
    FILE *hist_file_fp;
    int ret;
    int created_file = 0;
    int found_record = 0;           /* record found in history file */
    char *fs_token;
    char *dump_token;
    char tmpbuffer[256];

    /*
     * hist_rec_t - contains all of the records in the vdumpdates 
     * history file.
     */
    struct hist_rec_t {
        char            buffer[256];
        struct hist_rec_t  *next;
    };
    struct hist_rec_t *hist_rec = NULL;
    struct hist_rec_t *hist_rec_last = NULL;
    struct hist_rec_t *hist_rec_first = NULL;

#ifdef _OSF_SOURCE
    struct statfs fs_stats;
#else
    extern int statfs( char *, struct fs_data * );
    struct fs_data fs_stats;
#endif

    if ( access(Dump_hist_file_name, F_OK) == 0) {    /* file exists */
        if (*found_exact_level) {     /* want to scan file for record */
            hist_file_fp = fopen(Dump_hist_file_name, "r+"); 
        } else {                      /* want to append a record on end */
            hist_file_fp = fopen(Dump_hist_file_name, "a"); 
        }
    } else {                                          /* create file */
        hist_file_fp = fopen(Dump_hist_file_name, "w");
        created_file = 1;
    }

    if (hist_file_fp == NULL) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP23, "Cannot open dump history file %s for writing; [%d] %s\n"),
                Dump_hist_file_name, errno, ERR_MSG);
	saved_err = 1;
        return;
    }

    /* wait until no other process has either a shared or exclusive */
    /* lock on the dump history file then get an exclusive lock on */
    /* it so we can write it */

    (void) flock(fileno(hist_file_fp), LOCK_EX);

    ret = statfs( path, &fs_stats );

    /*
     *  If the /etc/vdumpdates was just created, write the new record
     *  into the file.
     */

    if (created_file || !*found_exact_level ) {
        fprintf(hist_file_fp, "%-s %d %s", fs_stats.f_mntfromname,
                Dump_level, ctime(&start_time.tv_sec));

        /* close this file after releasing the exclusive lock */
        (void) flock((int) fileno(hist_file_fp), LOCK_UN);
        fclose(hist_file_fp);
        return;
    }

    /* 
     *  If the /etc/vdumpdates file exists or the dump level for that file
     *  system already exists, then load contents into memory.
     */

    hist_rec = (struct hist_rec_t *) calloc (1, sizeof (struct hist_rec_t));
    if (hist_rec == NULL) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP24, "Cannot update the %s file: not enough memory\n"),
                Dump_hist_file_name);
	saved_err = 1;	
        return;
    }

    if (fgets(hist_rec->buffer, 256, hist_file_fp) != hist_rec->buffer) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP25, "Cannot read history file\n"));
	saved_err = 1;
        return;
    }
    hist_rec_last = hist_rec;
    hist_rec_first = hist_rec;

    while (1) {
        hist_rec = (struct hist_rec_t *) calloc (1, sizeof (struct hist_rec_t));
        if (hist_rec == NULL) {
            fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP24, "Cannot update the %s file: not enough memory\n"),
                    Dump_hist_file_name);
	    saved_err = 1;
            return;
        }


        if (fgets(hist_rec->buffer, 256, hist_file_fp) != hist_rec->buffer) {
            /* Done reading records from history file */
            break;
        }
        hist_rec_last->next = hist_rec;
        hist_rec_last = hist_rec;
    }

    /*
     * Now search the linked list for the file system and dump level.
     */
    hist_rec = hist_rec_first;

    while (hist_rec != NULL) {
        strcpy (tmpbuffer, hist_rec->buffer);
        fs_token = strtok (tmpbuffer, " ");    /* get the file system */

        if (strcmp(fs_stats.f_mntfromname, fs_token) == 0) {
            dump_token = strtok (NULL, " ");         /* get the dump level */

            if (atoi(dump_token) == Dump_level) {
                /* Need to overwrite this record */
                sprintf(hist_rec->buffer, "%-s %d %s",
                        fs_stats.f_mntfromname, Dump_level,
                        ctime(&start_time.tv_sec));
                found_record = 1;
                break;
            } 
        }
        hist_rec_last = hist_rec;
        hist_rec = hist_rec->next;
    }

    if (!found_record) {
        /*
         * Need to append the record onto the end of the history file 
         */
        hist_rec = (struct hist_rec_t *) calloc (1, sizeof (struct hist_rec_t));
        if (hist_rec == NULL) {
            fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP24, "Cannot update the %s file: not enough memory\n"),
                    Dump_hist_file_name);
	    saved_err = 1;
            return;
        }

        sprintf(hist_rec->buffer,  "%-s %d %s",
                fs_stats.f_mntfromname, Dump_level,
                ctime(&start_time.tv_sec));
        hist_rec_last->next = hist_rec;
        hist_rec->next = NULL;
    }

    /* 
     * Now write the memory contents of the linked list back to the 
     * history file.  First we must rewind to the beginning of the 
     * /etc/vdumpdates file so we can overwrite it.  
     */

    if (fseek(hist_file_fp, 0L, 0) < 0) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP26, "Cannot seek to the beginning of dump history file %s\n"),
                Dump_hist_file_name);
	saved_err = 1;
    }

    hist_rec = hist_rec_first;

    while (hist_rec != NULL) {
        fprintf(hist_file_fp, "%s", hist_rec->buffer);
        hist_rec_first = hist_rec;
        hist_rec = hist_rec->next;
        free(hist_rec_first);
    }

    /* close this file after releasing the exclusive lock */
    (void) flock((int) fileno(hist_file_fp), LOCK_UN);
    fclose(hist_file_fp); 

} /* end update_dumpdate_file */

/* 
 *  get_month_num
 *
 *  Given the abbreviated month, get_month_num returns a number (0-11)
 *  corresponding to its abbreviation (i.e. Nov = 10).
 */

int
get_month_num ( char *mon )
{
    char *nextmon;

    for (nextmon = months; *nextmon != '\0'; nextmon += 3) {
        if (strncmp(nextmon, mon, 3) == 0) {
            return ( (nextmon - months) /3 );
        }
    }
    return ERROR;

} /* end get_month_num */

/* 
 *  get_last_backup_date
 *
 *  Opens or creates the /etc/vdumpdates file.  If the file system exists
 *  in the history file, this routine will return the date that corresponds
 *  to the most recent backup of that file system in all n-1 levels.  The
 *  date it returns is in units of seconds since the epoch time.  If the file
 *  system does not exist in the history file or there are no lower dump levels
 *  for that file system, the beginning of time is assumed, which does a 
 *  full backup of all of the files.  If backing up a MSFS file system, the
 *  history file is /etc/vdumpdates.      
 */

time_t
get_last_backup_date ( char *path, int *found_exact_level )
{
    FILE *hist_file_fp = NULL;
    long seek_count = 0;        /* seek in multiples of long words */
    int ret;

    /*
     * last_date - records the highest dump date signified by that device
     * and level in the vdumpdates file.  This date is compared to 
     * the files and directories as they are backed up.
     */
    time_t  last_date;

    /*
     * hist_file_rec_t - contains all of the records in the vdumpdates 
     * history file.
     */
    struct hist_file_rec_t {
        char file_device[256];   /* dmn and set can be 63 chars long */
        int file_dump_level;
        char tm_wday[256];
        char tm_mon[256];
        struct tm t;
        int tm_year;
    } hist_file_rec;

#ifdef _OSF_SOURCE
    struct statfs fs_stats;
#else
    extern int statfs( char *, struct fs_data * );
    struct fs_data fs_stats;
#endif

    /* open or create the dump history file */
    if ( access(Dump_hist_file_name, F_OK) == 0) {    /* file exists */

        hist_file_fp = fopen(Dump_hist_file_name, "r");
        if (hist_file_fp == NULL) {
            fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP27, "Cannot open dump history file %s for reading; [%d] %s\n"),
                    Dump_hist_file_name, errno, ERR_MSG);
            return ERROR;
        }

    } else {                                          /* create file */

        hist_file_fp = fopen(Dump_hist_file_name, "w");
        fclose(hist_file_fp);
        return (0);          /* return beginning of time if source not found */
    }

    /* wait until no other processes have an exclusive lock on the */
    /* dump history file and then put a shared lock on it */
    
    (void) flock((int) fileno(hist_file_fp), LOCK_SH);

    last_date = (time_t) 0;

    ret = statfs( path, &fs_stats );

    while (1) {
        ret = fscanf(hist_file_fp, "%s%d%s%s%d%d:%d:%d%d",
                     hist_file_rec.file_device,
                     &hist_file_rec.file_dump_level,
                     hist_file_rec.tm_wday,
                     hist_file_rec.tm_mon,
                     &hist_file_rec.t.tm_mday,
                     &hist_file_rec.t.tm_hour,
                     &hist_file_rec.t.tm_min,
                     &hist_file_rec.t.tm_sec,
                     &hist_file_rec.tm_year
                     );

        /* 
         * if fscanf cannot find nine items in the hist file record then there 
         * are no more file systems.  Must drop out of the while loop.
         * Or, there is an error in /etc/vdumpdates.
         */
        if (ret <= 0) {
            break;

        } else if (ret != 9) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP28, "%s: error in /etc/vdumpdates\n"), Prog );
            /* skip bad line in /etc/vdumpdates */
	    saved_err = 1;
            continue;
        }

        /* if devices match then compare dump level and date */
        if (strcmp(hist_file_rec.file_device, fs_stats.f_mntfromname) == 0) {   

            if ( hist_file_rec.file_dump_level < Dump_level ) { 
                hist_file_rec.t.tm_wday = atoi(hist_file_rec.tm_wday);
                hist_file_rec.t.tm_mon = get_month_num(hist_file_rec.tm_mon);
                hist_file_rec.t.tm_year = hist_file_rec.tm_year - 1900;
		/* To have mktime compute daylight savings use '-1' */
                hist_file_rec.t.tm_isdst = -1;   

                if (mktime(&hist_file_rec.t) > last_date) { 
                    /*
                     * want highest dump level found in vdumpdates file
                     * if the exact dump level does not exist.
                     */
                    last_date = mktime(&hist_file_rec.t);
                    fprintf(stderr, "%s", ctime(&last_date));
                }  /* end if */
            
            } else if ( hist_file_rec.file_dump_level == Dump_level ) {
                    *found_exact_level = 1;
            } /* end if - if found <n dump level */
        }  /* end if - strcmp */

    } /* end while */

    /* close this file after releasing the shared lock */
    (void) flock((int) fileno(hist_file_fp), LOCK_UN);
    fclose(hist_file_fp);

    if (last_date == 0) {
        silent_fprintf(catgets(catd, S_VDUMP1, VDUMP29, "%s: Date of last level %d dump: the start of the epoch\n"), 
                       Prog, Dump_level);
    } else {
        silent_fprintf(catgets(catd, S_VDUMP1, VDUMP30, "%s: Date of last level %d dump: %s"), 
                       Prog, Dump_level, ctime(&last_date));
    }
    return (last_date);

} /* end get_last_backup_date */

#ifdef XTENDED_ATTRBS
/*
 * backup_proplist       - backup extended attributes
 *
 * fd   - file handel, ignored if path is non-null
 * path - file path, null indicates fd should be used
 * ss   - save set pointer
 *
 * returns: TRUE if error encountered
 */
int
backup_proplist(
		int fd,                           /* in     */
		char *path,                       /* in     */
		int follow,			  /* in	    */
		save_set_t *ss                    /* out    */
		)
{
  /* buffer management */
  static char *buffer = NULL;
  static int buf_size = 0;

  extern int errno;
  int ind, ret;
  int min_buf_size = BUF_SIZE;
  struct proplistname_args args;
  union old_record_header_t proplist_rhdr = { 0 };

  /*
   * get all extended attributes into buffer
   */
  args.pl_numnames = 0;
  args.pl_mask = PLE_FLAG_ALL;
  args.pl_names = NULL;

  /*
   * try larger buffers for 1000 tries
   */
  ret = 0;
  for (ind = 0; (ret == 0) && (min_buf_size != 0) && (ind < 1000); ind++) {

   
    /*
     * enlarge buffer to 8K bound, if needed
     */
    if (buf_size < min_buf_size) {
      if (buffer != NULL) {
	free(buffer);
      }
      buf_size = (min_buf_size + 8191) & ~8191;
      buffer = (char *)malloc(buf_size);
      if (buffer == NULL) {
	return TRUE;
      }
    }

    /*
     * get prop list, using name or fd
     */
    if (strlen(path) == 0) {
      ret = fgetproplist(fd, &args, buf_size, buffer,
			 &min_buf_size);
    } else {
      ret = getproplist(path, follow, &args, buf_size, buffer,
			&min_buf_size);
    }

    /*
     * process error returns
     */
    if (ret < 0) {
      if (errno == EOPNOTSUPP) {
	return FALSE;
      } else {
	return TRUE;
      }
    }
  }

  /*
   * exit conditions
   */
  if (ret == 0) {
    if (min_buf_size != 0) {	
      return TRUE;  /* failed 1000 attempts */
    } else {
      return FALSE; /* no propery list entries */
    }
  }

  /*
   * if attributes exist, store in var len RT_PROPLIST record
   */
  proplist_rhdr.rheader.type = RT_PROPLIST;

  start_var_len_rec(ss, &proplist_rhdr);
  mv2blk(
	 ss,
	 (char *)buffer,
	 ret,
	 SPAN_OK
	 );
  finish_var_len_rec(ss);

  return FALSE;

}
#endif /* XTENDED_ATTRBS */

/*
 * backup_symbolic_link
 *
 * Writes a save-set record describing a symbolic link to the
 * save-set. 
 *	Returns bytecount or ERROR. Sets saved_err as well.
 */

long
backup_symbolic_link( 
    save_set_t *ss,             /* in - save-set descriptor */
    char *link_name,            /* in - symbolic link's file name */
    struct stat *link_stat,     /* in - link's file stats */
    ino_t parent_ino,           /* in - link's directory's inode number */
    unsigned long *file_cnt     /* in/out - files backed up counter */
    )
{
    /* 
     * NOTE - the 'names' array below will contain the link's file name and
     * the link concatenated together (but separated by a null).  The
     * whole array is null-terminated.  Refer to 'tapefmt.h'.
     */

    struct {
        union old_record_header_t rec_hdr;
        union symbolic_link_rec_t link;
        char names[FILE_NAME_SIZE + MAX_PATH_SZ + 1];
    } link_rec;

    char *lname, link[MAX_PATH_SZ];
    int rcnt;

    /*----------------------------------------------------------------------*/

    (*file_cnt)++;

    /*
     * Get the symbolic link.
     */
    rcnt = readlink( link_name, link, sizeof( link ) );
    if (rcnt < 0) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP31, 
                 "%s: error backing up symbolic link <%s>; [%d] %s\n"),
                 Prog, link_name, errno, ERR_MSG );
	saved_err = 1;
	return ERROR;
    }

    link[rcnt] = '\0'; /* readlink() doesn't terminate the link with a null */

    if (Verbose) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP32, "bl %s @-> %s\n"), link_name, link );
    }

    /* initialize link's save-set record */
    bzero( &link_rec, sizeof( link_rec) );
 
    lname = get_name( link_name );
 
    link_rec.rec_hdr.rheader.type = RT_SYM_LINK;
    link_rec.rec_hdr.rheader.size = sizeof( union symbolic_link_rec_t ) +
                                    strlen( lname ) + strlen( link ) + 2; 
    link_rec.link.slink.ino = link_stat->st_ino;
    link_rec.link.slink.parent_ino = parent_ino;

    /* assemble the 'names' array (link file name <null> link <null>) */
    link_rec.link.slink.lname_bytes = strlen( lname ) + 1;
    strcpy( link_rec.names, lname );
    link_rec.link.slink.link_bytes = strlen( link ) + 1;
    strcpy( &link_rec.names[strlen( lname ) + 1], link );
 
    link_rec.link.slink.link_stat = *link_stat;

    /* copy record to save-set block */
    mv2blk( ss, 
            (char *) &link_rec, 
            sizeof( union old_record_header_t ) + link_rec.rec_hdr.rheader.size,
            NO_SPAN );

#ifdef XTENDED_ATTRBS
    /* backup extended attributes */
    if (backup_proplist(0, link_name, 0, ss)) {
        fprintf(
                stderr,
                catgets(catd, S_VDUMP1, VDUMP33, "%s: error reading extend attributes for symbolic link <%s>\n"),
                Prog,
                link_name
                );
	saved_err = 1;
    }
#endif

    Bytes_read += rcnt;

    return (long) rcnt;
}

/*
 * backup_hard_link
 *
 * Writes a save-set record describing a hard link to the
 * save-set.
 */

void
backup_hard_link(
    save_set_t *ss,             /* in - save-set descriptor */
    char *link_name,            /* in - link's file name */
    struct stat *link_stat,     /* in - link's file stats */
    ino_t link_parent_ino       /* in - link's directory's inode number */
    )
{
    /* 
     * NOTE - the 'names' array below will contain the link's file name and
     * the file name it links to concatenated together (but separated by 
     * a null).  The whole array is null-terminated.  Refer to 'tapefmt.h'.
     */

    struct {
        union old_record_header_t rec_hdr;
        union hard_link_rec_t link;
        char names[2*FILE_NAME_SIZE];
    } link_rec;

    char *fname, *file_name, *lname;
    ino_t file_parent_ino;
    struct stat file_stat;

    /*----------------------------------------------------------------------*/

    /*
     * Get the name of the file that this hard link refers to.  Actually
     * it is the first name that we encountered for an inode that has
     * several hard links.  The first one we encountered was backed up
     * as if it were a normal file.  All other names that refer to this
     * inode are considered to be hard links.  We also need the file's
     * directory's inode.  This info maintained in the Link Table; it
     * was put there when backup_file() backed up the first hard link
     * for this inode.
     */

    if (!link_tbl_lookup( link_stat->st_ino, &file_name, &file_parent_ino )) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP34, "%s: internal error; can't lookup link inode\n"),
                 Prog );
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP35, "%s: link inode = %d, link name = %s\n"),
                 Prog, link_stat->st_ino, link_name );
        abort_prog( catgets(catd, S_VDUMP1, VDUMP36, "%s: terminating\n"), Prog );
    }

    if (Verbose) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP37, "bh %s --> %s\n"), link_name, file_name );
    }

    /* initialize record */
    bzero( &link_rec, sizeof( link_rec) );
 
    fname = get_name( file_name );
    lname = get_name( link_name );
 
    link_rec.rec_hdr.rheader.type = RT_HARD_LINK;
    link_rec.rec_hdr.rheader.size = sizeof( union hard_link_rec_t ) +
                                    strlen( lname ) + strlen( fname ) + 2; 
    link_rec.link.hlink.file_ino = link_stat->st_ino;
    link_rec.link.hlink.file_parent_ino = file_parent_ino;
    link_rec.link.hlink.link_parent_ino = link_parent_ino;

    /* setup the names array (file name <null> link name <null>) */
    link_rec.link.hlink.fname_bytes = strlen( fname ) + 1;
    strcpy( link_rec.names, fname );
    link_rec.link.hlink.lname_bytes = strlen( lname ) + 1;
    strcpy( &link_rec.names[strlen( fname ) + 1], lname );

    if (lstat( file_name, &file_stat ) == ERROR) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP38, "%s: error accessing file  <%s>; [%d] %s\n"),
               Prog, file_name, errno, ERR_MSG );
	saved_err = 1;
        return;
    }
    link_rec.link.hlink.file_stat = file_stat;

    /* copy record to save-set block */
    mv2blk( ss, 
            (char *) &link_rec, 
            sizeof( union old_record_header_t ) + link_rec.rec_hdr.rheader.size,
            NO_SPAN );

#ifdef XTENDED_ATTRBS
    /* backup extended attributes */
    if (backup_proplist(0, file_name, 1, ss)) {
        fprintf(
                stderr,
                catgets(catd, S_VDUMP1, VDUMP39, "%s: error reading extend attributes for hard link <%s>\n"),
                Prog,
                link_name
                );
	saved_err = 1;
    }
#endif

}

/*
 * backup_device_file
 *
 * Writes a save-set record describing a device file to the
 * save-set.
 */

void
backup_device_file( 
    save_set_t *ss,             /* in - save-set descriptor */
    char *dev_name,             /* in - dev's file name */
    struct stat *dev_stat,      /* in - dev's stats */
    ino_t parent_ino,           /* in - dev's directory's inode */
    unsigned long *file_cnt     /* in/out - files backed up counter */
    )
{
    char *name;

    struct {
        union old_record_header_t rec_hdr;
        union dev_attr_rec_t dev_attr;
        char dev_name[MAX_PATH_SZ];
    } dattr_rec;

    /*-----------------------------------------------------------------*/
 
    (*file_cnt)++;
 
    if (dev_stat->st_nlink > 1) {
        /* The dev has several names (hard links) */

        if (link_tbl_add( dev_stat->st_ino, dev_name, parent_ino )) {
            /* 
             * We've already backed up the dev so just backup this
             * 'dev' as a hard link.
             */
            backup_hard_link( ss, dev_name, dev_stat, parent_ino );
            return;
        }
    }

    if (Verbose) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP40, "bs %s\n"), dev_name );
    }

    /* initialize dev attributes record */
    bzero( &dattr_rec, sizeof( dattr_rec) );
 
    name = get_name( dev_name );
 
    dattr_rec.rec_hdr.rheader.type = RT_DEV_ATTR;
    dattr_rec.rec_hdr.rheader.size = 
          sizeof( union dev_attr_rec_t ) + strlen( name ) + 1; 
    dattr_rec.dev_attr.dattr.dev_stat = *dev_stat;
    dattr_rec.dev_attr.dattr.parent_ino = parent_ino;
 
    dattr_rec.dev_attr.dattr.dname_bytes = strlen( name ) + 1;
    strcpy( dattr_rec.dev_name, name );
 
    /* copy dev attributes record to a save-set block */
    mv2blk( ss, 
            (char *) &dattr_rec, 
            sizeof( union old_record_header_t ) + dattr_rec.rec_hdr.rheader.size,
            NO_SPAN );

#ifdef XTENDED_ATTRBS
    /* backup extended attributes */
    if (backup_proplist(0, dev_name, 1, ss)) {
        fprintf(
                stderr,
                catgets(catd, S_VDUMP1, VDUMP41, "%s: error reading extend attributes for device file <%s>\n"),
                Prog,
                dev_name
                );
	saved_err = 1;
    }
#endif

}

#define PERCENT(x1,x2)                       \
  (((x2)>0) ?                                \
   ((double) (x1) / (double) (x2) * 100.0) : \
   (100.0))

time_t
print_progress(
    time_t current_time
    )
{
    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP42, "\n%s: Status at %s"), Prog, ctime(&current_time));

    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP43, "%s: Dumped  %ld of %ld bytes; %4.1f%% completed\n"), 
                   Prog, Bytes_backed_up, Bytes_to_backup,
		   PERCENT(Bytes_backed_up,Bytes_to_backup));

    if (!Backup_zeros && (Zeros_skipped > 0L)) {
        silent_fprintf(catgets(catd, S_VDUMP1, VDUMP44, "%s: Skipped %ld null bytes of %ld bytes dumped; %4.1f%% \n"), 
                       Prog, Zeros_skipped, Bytes_backed_up, 
		       PERCENT(Zeros_skipped,Bytes_backed_up));
    }

    if (Debug && (Sparse_skipped > 0L)) {
        silent_fprintf(catgets(catd, S_VDUMP1, VDUMP45, "%s: Skipped %ld sparse bytes of %ld bytes dumped; %4.1f%% \n"), 
                       Prog, Sparse_skipped, Bytes_backed_up, 
		       PERCENT(Sparse_skipped,Bytes_backed_up));
    }

    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP46, "%s: Dumped  %ld of %ld directories; %4.1f%% completed\n"),
                   Prog, Num_dirs_backed_up, Num_dirs_to_backup,
		   PERCENT(Num_dirs_backed_up,Num_dirs_to_backup));

    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP47, "%s: Dumped  %ld of %ld files; %4.1f%% completed\n"),
                   Prog, Num_files_backed_up, Num_files_to_backup,
		   PERCENT(Num_files_backed_up,Num_files_to_backup));

    return time( NULL );
}

/*
 * backup_file
 *
 * Opens a file, transfers its data to save-set blocks and closes the file.
 *
 * This routine does not backup blocks of zeros.  For non-ADVFS filesystems
 * the block of zeros is detected by searching the block buffer.  For
 * ADVFS the routine first uses the ADVFS library routines to skip
 * sparse holes in the file (most common reason for a block of zeros).
 * Then, after skipping sparse holes it also does the scan for for zeros
 * just as is done for non-ADVFS filesystems.
 *
 * In general, if an error occurs while backing up a file we stop
 * working on the file and return to the caller which will go to
 * next file. We use saved_err to record for later.
 */

#define SPARSEMAPSIZE 1024

long 
backup_file( 
    save_set_t *ss,             /* in - save-set descriptor */
    char *file_name,            /* in - file's name */
    struct stat *file_stat,     /* in - file's stats */
    ino_t parent_ino,           /* in - file's directory's inode */
    unsigned long *file_cnt     /* in/out - files backed up counter */
    )
{
    int s_fd, rcnt, done = FALSE;
    char *name;
    int result;
    mlBfInfoT   bfInfo;
    unsigned long d_offset = 0L;
    long curbyte, nextbyte, bcnt = 0L;
    int prev_all_zeros;
    int rec_started = FALSE;
    struct attr_timbuf time_buf;     /* time buffer for files */

    mlStatusT sts;
    time_t current_time;

    union old_record_header_t data_rhdr = { 0 };

    struct {
        union old_record_header_t rec_hdr;
        struct bf_attr_rec_t  bf_attr;
    } bfattr_rec = { 0 };

    struct {
        union old_record_header_t rec_hdr;
        union file_attr_rec_t file_attr;
        char file_name[MAX_PATH_SZ];
    } fattr_rec = { 0 };
    
    struct extentmap sparsemap;
    struct extentmapentry array[SPARSEMAPSIZE];
    int getmap_failed = 1;
    unsigned long cur_xtnt, cur_offset, cur_size;

    /*-----------------------------------------------------------------*/
 
    sparsemap.arraysize  = SPARSEMAPSIZE;
    sparsemap.numextents = 0;
    sparsemap.offset     = 0;
    sparsemap.extent     = array;

    (*file_cnt)++;
 
    if (file_stat->st_nlink > 1) {
        /* The file has several names (hard links) */

        if (link_tbl_add( file_stat->st_ino, file_name, parent_ino )) {
            /* 
             * We've already backed up the file so just backup this
             * 'file' as a hard link.
             */
            backup_hard_link( ss, file_name, file_stat, parent_ino );
		return ((long)(0)); /* no Bytes_to_backup for hardlink */ 
        }
    }

    if (Verbose) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP48, "bf %s, %ld\n"), file_name, ((((long)(file_stat->st_blocks)) * S_BLKSIZE) > file_stat->st_size) ?
                        file_stat->st_size : 
                        (((long)(file_stat->st_blocks)) * S_BLKSIZE));
    }
     
    /* open the file to be backed up */

#ifndef _OSF_SOURCE
    s_fd = open( file_name, O_RDONLY | O_NONBLOCK | O_BLKINUSE, 0 );
#else
    s_fd = open( file_name, O_RDONLY | O_NONBLOCK /*| O_NSHARE*/, 0 );
#endif
    if (s_fd == ERROR) {
        if ((errno == EWOULDBLOCK) || (errno == EAGAIN)) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP49, "%s: can't open <%s>; file is busy\n"),
                     Prog, file_name );
        } else {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP50, "%s: can't open <%s>; [%d] %s\n"),
                     Prog, file_name, errno, ERR_MSG );
        }
	saved_err = 1;
        return ERROR;
    }
 
    /* initialize file attributes record */
 
    name = get_name( file_name );
 
    fattr_rec.rec_hdr.rheader.type = RT_FILE_ATTR;
    fattr_rec.rec_hdr.rheader.size = 
          sizeof( union file_attr_rec_t ) + strlen( name ) + 1; 
    fattr_rec.file_attr.fattr.file_stat = *file_stat;
    fattr_rec.file_attr.fattr.parent_ino = parent_ino;
 
    fattr_rec.file_attr.fattr.fname_bytes = strlen( name ) + 1;
    strcpy( fattr_rec.file_name, name );
 
    /* copy file attributes record to a save-set block */
    mv2blk( ss, 
            (char *) &fattr_rec, 
            sizeof( union old_record_header_t ) + fattr_rec.rec_hdr.rheader.size,
            NO_SPAN );

#ifdef XTENDED_ATTRBS
    /* backup extended attributes */
    if (backup_proplist(s_fd, "", 1, ss)) {
        fprintf(
		stderr, 
		catgets(catd, S_VDUMP1, VDUMP51, "%s: error reading extend attributes for file <%s>\n"),
		Prog, 
		file_name
		);
	saved_err = 1;
    }
#endif

    /* Back up bitfile attributes only if advfs file system */

    if ( Fs_info.fs_type == MOUNT_MSFS ) {             
        bfattr_rec.rec_hdr.rheader.type = RT_BF_FILE_ATTR;
        bfattr_rec.rec_hdr.rheader.size = sizeof( struct bf_attr_rec_t ); 

        /* get the bitfile attributes for this file */
        result = advfs_get_bf_params (s_fd, &bfattr_rec.bf_attr.bfAttr, &bfInfo);

        if (result == 0) {
            /* copy bitfile attributes record to a save-set block */
            mv2blk( ss, 
                   (char *) &bfattr_rec, 
                   sizeof( union old_record_header_t ) + 
                        sizeof( struct bf_attr_rec_t ),
                   NO_SPAN );
        }
    }

   /* In case of CDFS and DVDFS, stat on small files
    * (<512 bytes), returns zero blocks. The following 'if' conditional is 
    * modified to take care of this bug.
    */
    if ((file_stat->st_blocks > 0L) || (file_stat->st_size > 0L))  {

        /*
         * Check if amount of time spent backing up files is over the
         * Alarm_time limit (5 minutes).
         */

        current_time = time( NULL );
        Time_amount = difftime( current_time, Last_start_time );

        if ( Time_amount >= Alarm_time ) {
            Last_start_time = print_progress( current_time );
        }

        /* initialize record header for data */
        data_rhdr.rheader.type = RT_FILE_DATA;

	/* Get file sparseness.  This currently only works for AdvFS & UFS. */

	getmap_failed = fcntl(s_fd, F_GETMAP, &sparsemap);

	while (( !getmap_failed ) &&
	       (sparsemap.numextents > sparsemap.arraysize)) {

	    /*
	     * We don't have enough extents for this file.  Free the
	     * current extentmap and malloc a new one large enough
	     * for the number of extents in this file.
	     */

	    if (sparsemap.extent != array) {
		free(sparsemap.extent);
	    }
	    sparsemap.extent = (struct extentmapentry *)
		malloc(sizeof(struct extentmapentry) * sparsemap.numextents);
	    if (sparsemap.extent == NULL) {
		/* The malloc failed.  Pretend that the fcntl() failed
		 * instead. */
		sparsemap.arraysize = SPARSEMAPSIZE;
		sparsemap.extent = array;
		getmap_failed = TRUE;
	    }
	    else {
		sparsemap.arraysize = sparsemap.numextents;
		sparsemap.offset = 0;
		getmap_failed = fcntl(s_fd, F_GETMAP, &sparsemap);
	    }
	}
	if (getmap_failed) {
	    /* Generate a fake extentmap covering the entire file.   */
	    /* This will fill in pages of zeroes when we aren't sure */
	    /* the file is sparse.                                   */
	    sparsemap.numextents = 1;
	    sparsemap.offset = 0;
	    sparsemap.extent[0].offset = 0;
	    sparsemap.extent[0].size = file_stat->st_size;
	}


	for (cur_xtnt = 0 ; cur_xtnt < sparsemap.numextents ; cur_xtnt++) {
	    cur_offset = sparsemap.extent[cur_xtnt].offset;
	    cur_size   = sparsemap.extent[cur_xtnt].size;

	    if (cur_offset > d_offset) {
		prev_all_zeros = TRUE;

		if (Debug) {
		    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP53, "%s: skipping sparse %6li - %8li in <%s>\n"),
			     Prog, d_offset, cur_offset - 1, file_name );
		}

		Sparse_skipped += (cur_offset - d_offset);
		if (lseek( s_fd, cur_offset, SEEK_SET ) < 0L) {
		    fprintf( stderr,
			     catgets(catd, S_VDUMP1, VDUMP55, "%s: unable to lseek to byte %lu in file <%s>; [%d] %s\n"),
			     Prog, cur_offset, file_name, errno, ERR_MSG );
		    saved_err = 1;
		    break; /* Exit for loop */
		}
	    } else {
		prev_all_zeros = FALSE;
	    }

	    d_offset = cur_offset;
	    done = FALSE;

	    while (!done) {

		/* Read next block from file */

		if ( (d_offset + BUF_SIZE) < (cur_offset + cur_size) ) {
		    result = read( s_fd, buf, BUF_SIZE );
		} else {
		    result = read( s_fd, buf,
				   (cur_offset + cur_size) - d_offset );
		}

		if (result > 0) {

		    /* There is data to write to the backup media */

		    if (prev_all_zeros) {

			/*
			 * We've been skipping zeros (or sparse holes).
			 * We need to finish an outstanding variable length
			 * record and start a new one.  This ensures that
			 * that the record has the correct data offset
			 * that compressed data is flushed properly.
			 */

			if (rec_started) {
			    finish_var_len_rec( ss );
			    rec_started = FALSE;
			}

			data_rhdr.rheader.size = 0;
			data_rhdr.rheader.flags = 0;
#ifdef __alpha
			data_rhdr.rheader.d_offset_lo = 
                            d_offset & 0x00000000ffffffff;
			data_rhdr.rheader.d_offset_hi = 
                            (d_offset >> 32L) & 0x00000000ffffffff;
#else
			data_rhdr.rheader.d_offset_lo = d_offset;
#endif
			start_var_len_rec( ss, &data_rhdr );
			rec_started = TRUE;
		    } /* end if prev_all_zeros */

		    if (!rec_started) {
			start_var_len_rec( ss, &data_rhdr );
			rec_started = TRUE;
		    }


		    /* Move data buffer to temp save-set block */

		    mv2blk( ss, (char *) buf, result, SPAN_OK );

		    Bytes_read += result;
		    bcnt += result;
		    d_offset += result;
		    prev_all_zeros = FALSE;

		} else if (result < 0) {
		    /* read failure */
		    fprintf( stderr,
			     catgets(catd, S_VDUMP1, VDUMP55, "%s: unable to read file <%s>; [%d] %s\n"),
			     Prog, file_name, errno, ERR_MSG );
		    saved_err = 1;
		    done = TRUE;

		} else /* result == 0 */ {
		    done = TRUE;
		}

	    } /* end while */

	} /* end for */
  

        if (sparsemap.extent != array && sparsemap.extent != NULL) {
            free(sparsemap.extent);
        }


        if (rec_started) {
            finish_var_len_rec( ss );     /* finish the data record */
        }
    }

    /* 
     * Get the current times for the file, but reset using atimes 
     * from the stat buffer. This will keep mtime consistent in case
     * someone changed the file while we were reading it.
     */
    if ( (Fs_info.fs_type != MOUNT_NFS) &&
         (Fs_info.fs_type != MOUNT_NFS3) ) {
        /* ignore nfs mounts as we don't want to adjust remote machine times */

        if (-1 == fcntl (s_fd, F_GETTIMES, &time_buf)) {
            /* Use the times from file_stat if we got an error */
            time_buf.mtime.tv_sec = file_stat->st_mtime;
            time_buf.mtime.tv_usec = file_stat->st_umtime;
            time_buf.ctime.tv_sec = file_stat->st_ctime;
            time_buf.ctime.tv_usec = file_stat->st_uctime;
        }
        
        time_buf.atime.tv_sec = file_stat->st_atime;
        time_buf.atime.tv_usec = file_stat->st_uatime;
    
        if (-1 == fcntl (s_fd, F_SETTIMES, &time_buf)) {
            if ((EPERM != errno) && (EACCES != errno) && (EROFS != errno)) {
                fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP162, 
                         "%s: can't reset atime for <%s>; [%d] %s\n"),
                         Prog, file_name, errno, ERR_MSG );
                saved_err = 1;
            }
        }
    }
 
    close( s_fd );                               /* close source file      */

    /*
     * This is a potentially faulty check.  Trying to compare the number
     * of user-visible bytes with the number of underlying blocks used
     * by the filesystem is imperfect at best.  Nonetheless, it is usually
     * going to be valid.  Since -g is a hidden option, this check remains.
     * This is most often a problem because AdvFS stores files in units
     * of 1k.  If a file has, for example, 32 bytes from a user's perspective,
     * it will have 1024 bytes from the AdvFS perspective.
     */
    if (Debug && (bcnt != (((long)(file_stat->st_blocks)) * S_BLKSIZE))) {
        if (bcnt < (((long)(file_stat->st_blocks)) * S_BLKSIZE)) {
            ; /* Probably not a problem. */
        }
        else {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP56, "%s: bcnt: %li, ((long)(st_blocks)) * S_BLKSIZE: %li, diff: %li%s\n"), 
                     Prog, bcnt, (((long)(file_stat->st_blocks)) * S_BLKSIZE), (((long)(file_stat->st_blocks)) * S_BLKSIZE) - bcnt, file_name );
        }
    }

    return bcnt;
}

/* end backup_file */


void
backup_advfs_quotas(save_set_t *ss, char *ufilename, char *gfilename,
                    struct stat *ustatp, struct stat *gstatp)
{
    /* 
     * Record layouts for user, group, and fileset quota records.
     * Includes standard record headers.  Also, define some
     * shorthand notation to make life easier.
     */
    struct {
        union old_record_header_t rec_hdr;
        union ug_quota32_rec_t ugquota_fields;
    } ugquota32_rec;
#define ugq32_r ugquota32_rec.ugquota_fields.ugquota32

    struct {
        union old_record_header_t rec_hdr;
        union ug_quota64_rec_t ugquota_fields;
    } ugquota64_rec;
#define ugq64_r ugquota64_rec.ugquota_fields.ugquota64

    struct {
        union old_record_header_t rec_hdr;
        union f_quota_rec_t fquota_fields;
    } fquota_rec;
#define fq_r fquota_rec.fquota_fields.fquota


    mlBfSetParamsT setParams;           /* Existing fileset parameters */
    FILE *qf;
    int qfd,                            /* File descriptor for quota file */
        type,                           /* Loop counter */
        id;                             /* ID of user or group quota record */
    struct dQBlk32 dqbuf32;             /* A single on-disk u/g quota record */
    struct dQBlk64 dqbuf64;             /* A single on-disk u/g quota record */
    char *filename;                     /* Quota file's name */
    struct stat *statp;                 /* Stat() struct. for u/g quota file */
    int large_limits;                   /* TRUE if fileset supports large quotas */
    int dqsize;                         /* Size of on disk quota structure */
    long curbyte, nextbyte;             
    void *xtntmap = NULL;
    mlStatusT sts;

    /* 
     * First, get the fileset quota information.
     */
    if (msfs_get_bfset_params(Fs_info.advfs_set_id, &setParams)) {
        saved_err = 1;
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP143, "%s: Could not read fileset quotas; [%d] %s\n"), Prog, errno, ERR_MSG);
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP161, "%s: unable to dump any %s quota information.\n"), Prog, "");
        return;
    }
    else {
        fquota_rec.rec_hdr.rheader.type = RT_FQUOTA;
        fquota_rec.rec_hdr.rheader.size = F_QUOTA_REC_SIZE;
        fquota_rec.rec_hdr.rheader.d_offset_lo = 0;
        fquota_rec.rec_hdr.rheader.d_offset_hi = 0;
        fquota_rec.rec_hdr.rheader.flags = 0;
        fq_r.blkHLimit = setParams.blkHLimit;
        fq_r.blkSLimit = setParams.blkSLimit;
        fq_r.fileHLimit = setParams.fileHLimit;
        fq_r.fileSLimit = setParams.fileSLimit;
        fq_r.blkTLimit = setParams.blkTLimit;
        fq_r.fileTLimit = setParams.fileTLimit;
        mv2blk(ss, (char *) &fquota_rec, sizeof( fquota_rec ), NO_SPAN);
    }

    /* 
     * Next, get the user and quota information.
     */
    large_limits = setParams.quotaStatus & QSTS_LARGE_LIMITS;

    if (large_limits && !Use_previous_format) {
        ugquota64_rec.rec_hdr.rheader.d_offset_lo = 0;
        ugquota64_rec.rec_hdr.rheader.d_offset_hi = 0;
        ugquota64_rec.rec_hdr.rheader.flags = 0;
        ugquota64_rec.rec_hdr.rheader.size = UG_QUOTA64_REC_SIZE;
    } else {
        ugquota32_rec.rec_hdr.rheader.d_offset_lo = 0;
        ugquota32_rec.rec_hdr.rheader.d_offset_hi = 0;
        ugquota32_rec.rec_hdr.rheader.flags = 0;
        ugquota32_rec.rec_hdr.rheader.size = UG_QUOTA32_REC_SIZE;
    }

    for (type = USRQUOTA; type < MAXQUOTAS; type++) {
        switch (type) {
            case USRQUOTA:
                filename = ufilename;
                statp = ustatp;
                if (large_limits && !Use_previous_format)
                    ugquota64_rec.rec_hdr.rheader.type = RT_UQUOTA64;
                else ugquota32_rec.rec_hdr.rheader.type = RT_UQUOTA32;
                break;
            case GRPQUOTA:
                filename = gfilename;
                statp = gstatp;
                if (large_limits && !Use_previous_format) 
                    ugquota64_rec.rec_hdr.rheader.type = RT_GQUOTA64;
                else ugquota32_rec.rec_hdr.rheader.type = RT_GQUOTA32;

                break;
            default:
                saved_err = 1;
                continue;
        }
        if (filename[0] != '\0') {
            if (Verbose) {
                if (type == USRQUOTA) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP48, 
                             "bf %s, %ld\n"), USR_QUOTA_FILE, 
                             (((long)(statp->st_blocks)) * S_BLKSIZE) );
                }
                else {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP48, 
                             "bf %s, %ld\n"), GRP_QUOTA_FILE, 
                             (((long)(statp->st_blocks)) * S_BLKSIZE));
                }
            }
            qfd = open(filename, O_RDONLY | O_NONBLOCK, 0 );
            if (qfd == ERROR) {
                saved_err = 1;
                if ((errno == EWOULDBLOCK) || (errno == EAGAIN)) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP49, "%s: can't open <%s>; file is busy\n"),
                             Prog, filename );
                } else {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP50, "%s: can't open <%s>; [%d] %s\n"),
                             Prog, filename, errno, ERR_MSG );
                }
            }
            else {
                /* Use ADVFS extent map routines to skip sparse holes in quota file */
                xtntmap = advfs_xtntmap_open( qfd, &sts, 1 );
                if (xtntmap == NULL) {
                    fprintf( stderr,
                    catgets(catd, S_VDUMP1, VDUMP52, "%s: unable to open extent map for <%s>; %s\n"),
                    Prog, filename, BSERRMSG( sts ) );
                    fprintf( stderr, 
                    catgets(catd, S_VDUMP1, VDUMP161, "%s: unable to dump any %s quota information.\n"), Prog, qfextension[type]);
                    saved_err = 1;
                    close (qfd);
                    continue;
                } else {
                    id = 0;
                    curbyte = -1;
                    nextbyte = 0;
  
                    /* Get first allocated byte in quota file */
    
                    advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte );
                }
                if (large_limits) {
                    if (Use_previous_format) {
                        dqsize = sizeof(struct dQBlk32);
                    } else {
                        dqsize = sizeof(struct dQBlk64);
                    }
                    while (1) {
                        id = nextbyte / sizeof( struct dQBlk64 );
                        if (lseek( qfd, nextbyte, SEEK_SET ) < 0) {
                            break;
                        }
                        if (read( qfd, &dqbuf64, sizeof(struct dQBlk64)) < 0) {
                            fprintf( stderr,
                                catgets(catd, S_VDUMP1, VDUMP55, 
                                "%s: unable to read file <%s>; [%d] %s\n"),
                                Prog, filename, errno, ERR_MSG );
                            saved_err = 1;
                            break;
                        }
                        if (dqbuf64.dqb_bhardlimit != 0 ||
                            dqbuf64.dqb_bsoftlimit != 0 ||
                            dqbuf64.dqb_ihardlimit != 0 ||
                            dqbuf64.dqb_isoftlimit != 0 ||
                            dqbuf64.dqb_btime != 0 ||
                            dqbuf64.dqb_itime != 0) {
        
                            if (Use_previous_format) {
                                ugq32_r.id = id;
                                if (dqbuf64.dqb_bhardlimit > QUOTA32_MAX) {
                                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP158,
                                        "%s: quota overflow in file %s for %s %d.\n"), 
                                        Prog, filename, qfextension[type], id);
                                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP159,
                                        "\tHard block limit has been truncated from %lu to %lu.\n"),
                                        dqbuf64.dqb_bhardlimit, QUOTA32_MAX);
                                    dqbuf64.dqb_bhardlimit = QUOTA32_MAX;
				}
                                ugq32_r.dqb_bhardlimit = (unsigned int)dqbuf64.dqb_bhardlimit;
                                if (dqbuf64.dqb_bsoftlimit > QUOTA32_MAX) {
                                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP158,
                                        "%s: quota overflow in file %s for %s %d.\n"), 
                                        Prog, filename, qfextension[type], id);
                                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP160,
                                        "\tSoft block limit has been truncated from %lu to %lu.\n"),
                                        dqbuf64.dqb_bsoftlimit, QUOTA32_MAX);
                                    dqbuf64.dqb_bsoftlimit = QUOTA32_MAX;
				}
                                ugq32_r.dqb_bsoftlimit = (unsigned int)dqbuf64.dqb_bsoftlimit;
                                ugq32_r.dqb_ihardlimit = dqbuf64.dqb_ihardlimit;
                                ugq32_r.dqb_isoftlimit = dqbuf64.dqb_isoftlimit;
                                ugq32_r.dqb_btime = dqbuf64.dqb_btime;
                                ugq32_r.dqb_itime = dqbuf64.dqb_itime;
                                mv2blk(ss, 
                                    (char *) &ugquota32_rec, 
                                     sizeof( ugquota32_rec ), 
                                     NO_SPAN);
                            } else {
                                ugq64_r.id = id;
                                ugq64_r.dqb_bhardlimit = dqbuf64.dqb_bhardlimit;
                                ugq64_r.dqb_bsoftlimit = dqbuf64.dqb_bsoftlimit;
                                ugq64_r.dqb_ihardlimit = dqbuf64.dqb_ihardlimit;
                                ugq64_r.dqb_isoftlimit = dqbuf64.dqb_isoftlimit;
                                ugq64_r.dqb_btime = dqbuf64.dqb_btime;
                                ugq64_r.dqb_itime = dqbuf64.dqb_itime;
                                mv2blk(ss, 
                                    (char *) &ugquota64_rec, 
                                     sizeof( ugquota64_rec ), 
                                     NO_SPAN);
                            }
                        }
                        curbyte = nextbyte + dqsize - 1;
                        if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
                            Num_files_backed_up++;
                            Bytes_backed_up += 
                                ((long)(statp->st_blocks)) * S_BLKSIZE;
                            break;
                        }
                    } /* end while */
                } else {
                    while (1) {
                        id = nextbyte / sizeof( struct dQBlk32 );
                        if (lseek( qfd, nextbyte, SEEK_SET ) < 0) {
                            break;
                        }
                        if (read( qfd, &dqbuf32, sizeof(struct dQBlk32)) < 0) {
                            fprintf( stderr,
                                catgets(catd, S_VDUMP1, VDUMP55, 
                                "%s: unable to read file <%s>; [%d] %s\n"),
                                Prog, filename, errno, ERR_MSG );
                            saved_err = 1;
                            break;

                        }
                        if (dqbuf32.dqb_bhardlimit != 0 ||
                            dqbuf32.dqb_bsoftlimit != 0 ||
                            dqbuf32.dqb_ihardlimit != 0 ||
                            dqbuf32.dqb_isoftlimit != 0 ||
                            dqbuf32.dqb_btime != 0 ||
                            dqbuf32.dqb_itime != 0) {
        
                            ugq32_r.id = id;
                            ugq32_r.dqb_bhardlimit = dqbuf32.dqb_bhardlimit;
                            ugq32_r.dqb_bsoftlimit = dqbuf32.dqb_bsoftlimit;
                            ugq32_r.dqb_ihardlimit = dqbuf32.dqb_ihardlimit;
                            ugq32_r.dqb_isoftlimit = dqbuf32.dqb_isoftlimit;
                            ugq32_r.dqb_btime = dqbuf32.dqb_btime;
                            ugq32_r.dqb_itime = dqbuf32.dqb_itime;
                            mv2blk(ss, 
                                (char *) &ugquota32_rec, 
                                 sizeof( ugquota32_rec ), 
                                 NO_SPAN);
                        }
                        curbyte = nextbyte + sizeof( struct dQBlk32 ) - 1;
                        if (advfs_xtntmap_next_byte( xtntmap, curbyte, &nextbyte ) < 0) {
                            Num_files_backed_up++;
                            Bytes_backed_up += 
                                ((long)(statp->st_blocks)) * S_BLKSIZE;
                            break;
                        }
                    } /* end while */
                }
    
                /* 
                 * Write a bogus, end-of-file record that vrestore can
                 * recognize and use.
                 */
                if (large_limits && !Use_previous_format) { 
                    ugq64_r.id = -1;
                    mv2blk(ss,  
                           (char *) &ugquota64_rec, 
                           sizeof( ugquota64_rec ), 
                           NO_SPAN);
                } else {
                    ugq32_r.id = -1;
                    mv2blk(ss,  
                           (char *) &ugquota32_rec, 
                           sizeof( ugquota32_rec ), 
                           NO_SPAN);
                }
                advfs_xtntmap_close( xtntmap );
                close (qfd);
            }
        }
    }
}

/* end backup_advfs_quotas */


/*
 * write_summary_rec
 *
 * The new summary record struct contains a char array for the "source"
 * string, plus a flag indicating whether the source_dir string is
 * present (for backwards compatibility).
 */

void
write_summary_rec( 
    save_set_t *ss,              /* in - save-set descriptor */
    char *source                 /* in - source mountpoint/pathname  */
    )
{
    struct {
        union old_record_header_t rec_hdr;
        union summary_rec_t summ;
    } summ_rec;
    
    /*-----------------------------------------------------------------------*/
 
    /* initialize record */
    bzero( &summ_rec, sizeof( summ_rec) );
 
    summ_rec.rec_hdr.rheader.type = RT_SUMMARY;
 
    summ_rec.rec_hdr.rheader.size = sizeof( union summary_rec_t ); 
 
    /* Put the source dir string in the record */
    strncpy( summ_rec.summ.summary.source_dir, source, strlen(source) );
    summ_rec.summ.summary.pathname_present = 1;

    /* copy directory attributes record to save-set block */
    mv2blk( ss, 
            (char *) &summ_rec, 
            sizeof( summ_rec ),
            NO_SPAN );
}

/*
 * write_dir_attr_rec - 
 *
 * Writes a record that describes a directory (not its contents) to
 * the save-set.
 */

void
write_dir_attr_rec( 
   save_set_t *ss,              /* in - save-set descriptor */
   char *dir_name,              /* in - directory's name */
   struct stat *dir_stat,       /* in - directory's stats */
   ino_t parent_ino,            /* in - directory's parent's inode */
   int as_attr                  /* in - flag indicates if the attr rec is */
   )                            /*      to be marked as an ATTR vs a HDR */
{
    int fd, result;
    char *name;
    struct {
        union old_record_header_t rec_hdr;
        union dir_attr_rec_t dir_attr;
        char dir_name[MAX_PATH_SZ];
    } dir_attr_rec;

    mlBfInfoT   bfInfo;
    struct {
        union old_record_header_t rec_hdr;
        struct bf_attr_rec_t  bf_attr;
        struct bf_attr_rec_t  i_attr;
        char dir_name[MAX_PATH_SZ];
    } bfattr_rec;
    
    /*-----------------------------------------------------------------------*/
 
    /* initialize directory attributes record */
    bzero( &dir_attr_rec, sizeof( dir_attr_rec) );
 
    name = get_name( dir_name );
 
    if (as_attr) {
        dir_attr_rec.rec_hdr.rheader.type = RT_DIR_ATTR;
    } else {
        dir_attr_rec.rec_hdr.rheader.type = RT_DIR_HDR;
    }

    dir_attr_rec.rec_hdr.rheader.size = 
          sizeof( union dir_attr_rec_t ) + strlen( name ) + 1; 
    dir_attr_rec.dir_attr.dattr.dir_stat = *dir_stat;
    dir_attr_rec.dir_attr.dattr.parent_ino = parent_ino;
    dir_attr_rec.dir_attr.dattr.dname_bytes = strlen( name ) + 1;
    strcpy( dir_attr_rec.dir_name, name );
 
    /* copy directory attributes record to blk.data */
    mv2blk( ss, 
            (char *) &dir_attr_rec, 
            sizeof( union old_record_header_t ) +
                sizeof( union dir_attr_rec_t ) + strlen( name ) + 1,
            NO_SPAN );

    /* backup extended attributes only in files pass. */
#ifdef XTENDED_ATTRBS
    if( as_attr ) {
	if (backup_proplist(0, dir_name, 1, ss)) {
		fprintf(
			stderr, 
			catgets(catd, S_VDUMP1, VDUMP57, "%s: error reading extend attributes for directory <%s>\n"),
			Prog, 
			dir_name
			);
		saved_err = 1;
	}
    }
#endif

    /*
     * Back up bitfile and inheritable attributes only if advfs
     * file system.
     */

    if (as_attr && (Fs_info.fs_type == MOUNT_MSFS)) {

        bfattr_rec.rec_hdr.rheader.type = RT_BF_DIR_ATTR;
        bfattr_rec.rec_hdr.rheader.size = (2 * sizeof( struct bf_attr_rec_t ))
                                          + strlen( name ) + 1; ; 
        strcpy( bfattr_rec.dir_name, name );

        if ((fd = open( dir_name, O_RDONLY, 0 )) == ERROR) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP58, "%s: can't open directory <%s>; [%d] %s\n"),
                    Prog, dir_name, errno, ERR_MSG );
	    saved_err = 1;
            return;
        }

        /* get the bitfile attributes for this file */
        result = advfs_get_bf_params (fd, &bfattr_rec.bf_attr.bfAttr, &bfInfo);

        if (result == 0) {
            /* get the inheritable attributes for this file */
            result = advfs_get_bf_iattributes (fd, &bfattr_rec.i_attr.bfAttr);

            if (result == 0) {
                /* copy bitfile attributes record to a save-set block */
                mv2blk( ss, 
                       (char *) &bfattr_rec, 
                       sizeof( union old_record_header_t ) +
                       (2 * sizeof( struct bf_attr_rec_t )) +
                       strlen( name ) + 1,
                       NO_SPAN );
            }
        } /* end bitfile attr */
        close(fd);
    }  /* end advfs check */

}
/* end write_dir_attr_rec */

  /*
   *      backup_dir
   *
   *      This routine backs up a directory (its entries, not the files 
   *      that the entries describe).  After it has backed up all the entries
   *      in the directory, this routine calls itself recursively to
   *      backup its subdirectories and all their descendents.  
   *
   *      The diagram bellow shows how a directory tree is backed up;
   *      how it ends up in the save-set.
   *
   *                           dir1
   *                           /  \
   *           tree:        dir2  dir3
   *                         /
   *                       dir4
   *
   *                           ||
   *                           \/  
   *
   *           ss:   <dir1> <dir2> <dir3> <dir4>
   */

void
backup_dir( 
    save_set_t *ss,       /* in - save-set descriptor */
    char *path_name,      /* in/out - directory's path (must be MAX_PATH_SZ) */
    struct stat *dir_stat,/* in - directory's stats */
    ino_t dir_ino,        /* in - directory's inode */
    ino_t parent_ino,     /* in - directory's parent's inode */
    int mount_dev,        /* in - device root dir is mounted on */
    time_t last_dump_date /* in - last time files backed up at this level */
    )
{
    void *dp;
    struct dirent *dir_entry;
    struct stat stat_buf;
    int len = strlen( path_name ), blk_bytes_avail;
    long basep = 0;
    union old_record_header_t data_rhdr;
    static char zerobuf[DIRBLKSIZ]; /* buf of zeros */
    
    /*-----------------------------------------------------------------------*/
 
    /* 
     * PASS1 - save the directory a dir_buf chunk at a time.
     */

    Num_dirs_to_backup++;

    if (dir_stat->st_dev != mount_dev) {
        /* we've crossed over to a different filesystem */
        dir_stat->st_ino = dir_ino;
        write_dir_attr_rec( ss, path_name, dir_stat, parent_ino, FALSE );
        return;
    }

    dp = opendir( path_name );
    if (dp == NULL) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP59, "%s(bd): unable to open directory <%s>; [%d] %s\n"),
                 Prog, path_name, errno, ERR_MSG );
	saved_err = 1;
        return;
    }
    
    write_dir_attr_rec( ss, path_name, dir_stat, parent_ino, FALSE );

    /* initialize record header for data */
    bzero( &data_rhdr, sizeof( data_rhdr ) );
    data_rhdr.rheader.type = RT_DIR_DATA;
 
    /* start the data record */
    start_var_len_rec( ss, &data_rhdr );
 
    /* 
     * append a slash (/)  to the path name 
     */
 
    path_name[len] = '/';
 
    blk_bytes_avail = DIRBLKSIZ;

    while ((dir_entry = readdir( dp )) != NULL) {
 
        /* 
         * chop off previous file name
         */
        path_name[len + 1] = '\0';

        /* 
         * append the file name to the path name 
         */
        path_name = strcat( path_name, dir_entry->d_name );
   
        if (lstat( path_name, &stat_buf ) == ERROR) {
            fprintf( stderr, 
                     catgets(catd, S_VDUMP1, VDUMP60, "%s: unable to get info for file <%s>; [%d] %s\n"),
                     Prog, path_name, errno, ERR_MSG );
	    saved_err = 1;
            continue;
        }
   
        switch( stat_buf.st_mode & S_IFMT ) {
            case S_IFDIR:
                /* always save dir info; see backup_dir_files */

                if (blk_bytes_avail < dir_entry->d_reclen) {
                    mv2blk( ss, zerobuf, 
                            blk_bytes_avail, SPAN_OK );
                    blk_bytes_avail = DIRBLKSIZ;
                }

                mv2blk( ss, (char *)dir_entry, 
                        dir_entry->d_reclen, SPAN_OK );

                blk_bytes_avail -= dir_entry->d_reclen;
                break;

            case S_IFREG:
            case S_IFLNK:
            case S_IFIFO:
            case S_IFCHR:
            case S_IFBLK:

                if ( mount_dev != stat_buf.st_dev ) {
                        /*
                         * avoid backing up symbolic links
                         * created by automount to point
                         * to /tmp_mnt
                         * We can detect them by comparing devices
                         * Automount would break if it was told
                         * to mount on top of a symbolic link
                         * but has no problem if nothing is there
                         */
                    break;
                }

                if ((Fs_info.fs_type == MOUNT_MSFS) &&            
                    ((stat_buf.st_ino == USR_QUOTA_INO) ||
                     (stat_buf.st_ino == GRP_QUOTA_INO))) {
                    /* skip quota files */
                    break;
                }

                if ((stat_buf.st_mtime >= last_dump_date) ||
                    (stat_buf.st_ctime >= last_dump_date)) {
                    
                    int add_bytes = TRUE;

                    Num_files_to_backup++;

                    /* 
                     * Add entry to hardlink table only if 
                     * no. of links are more than 1, since it is used
                     * only to keep track of hardlinks.
                     */
                    if (stat_buf.st_nlink > 1) {
                        add_bytes = hardlink_tbl_add(stat_buf.st_ino, 
                                    stat_buf.st_nlink);
                    }
                    
                    /* 
                     * The number of bytes to backup for this file
                     * is generally difficult to determine given
                     * only the stats.  This is especially true
                     * if the file is sparse and has "chunks" that
                     * do not fall on block size boundaries.
                     * Therefore, the number below really is an
                     * estimate.  
                     */
                    if (add_bytes) {

                        long size;                         

			/* If the number of blocks is zero, add the size of link to
			 * Bytes_to_backup.
			 */

			if ( stat_buf.st_blocks == 0 ) {
 			     size =  stat_buf.st_size;	 /* Estimation in case of symbolic link */
                        } else {

                             size = (stat_buf.st_blocks*S_BLKSIZE);

                             /* Estimation in case of Sparse files */

                          }

			 Bytes_to_backup += (( size > stat_buf.st_size ) ? stat_buf.st_size : size );

                    }
                    if (blk_bytes_avail < dir_entry->d_reclen) {
                        mv2blk( ss, zerobuf, 
                                blk_bytes_avail, SPAN_OK );
                        blk_bytes_avail = DIRBLKSIZ;
                    }

                    mv2blk( ss, (char *)dir_entry, 
                            dir_entry->d_reclen, SPAN_OK );

                    blk_bytes_avail -= dir_entry->d_reclen;

                }
                break;

            default:
                /* Ignore other types, we won't back them up anyway */
                break;

        }
    }
 
    path_name[len] = '\0';                       /* remove slash "/"       */

    closedir( dp );
 
    if (blk_bytes_avail < DIRBLKSIZ) {
        mv2blk( ss, zerobuf, blk_bytes_avail, SPAN_OK );
    }

    finish_var_len_rec( ss );     /* finish the data record */
 
    /* 
     * PASS2 - scan the directory and recursively backup each sub directory 
     */
    dp = opendir( path_name );
    if (dp == NULL) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP59, "%s(bd): unable to open directory <%s>; [%d] %s\n"),
                 Prog, path_name, errno, ERR_MSG );
	saved_err = 1;
        return;
    }
    
    /* 
     * append a slash (/)  to the path name 
     */
 
    path_name[len] = '/';
 
    while ((dir_entry = readdir( dp )) != NULL) {
 
        if (!strcmp( dir_entry->d_name, "." ) ||
            !strcmp( dir_entry->d_name, "..") ||
            !strcmp( dir_entry->d_name, ".tags" ) ) {
            /* skip . and .. and .tags */
            continue;
        }

        /* 
         * chop off previous file name
         */
        path_name[len + 1] = '\0';

        /* 
         * append the file name to the path name 
         */
        path_name = strcat( path_name, dir_entry->d_name );
   
        if (lstat( path_name, &stat_buf ) == ERROR) {
            fprintf( stderr, 
                     catgets(catd, S_VDUMP1, VDUMP60, "%s: unable to get info for file <%s>; [%d] %s\n"),
                     Prog, path_name, errno, ERR_MSG );
	    saved_err = 1;
            continue;
        }

	/* don't backup AutoFS auto-mounted file systems found along the way */ 
        if (S_ISDIR( stat_buf.st_mode ) && (!(stat_buf.st_flags & AUTOFS_INTERCEPT)) ) {
            backup_dir( ss, path_name, &stat_buf, 
                        dir_entry->d_ino, dir_stat->st_ino,
                        mount_dev, last_dump_date );
        }
    }
 
    path_name[len] = '\0';                        /* remove slash "/"        */
 
    closedir( dp );
}

/* end backup_dir */

  /*
   *      backup_dir_files
   *
   *      Opens a directory, calls 'backup_file' for each 
   *      file in the directory, closes the directory.  The
   *      effect of this routine is that all files in the directory
   *      and it descendent directories are backed up.
   */

void
backup_dir_files( 
    save_set_t *ss,             /* in - save-set descriptor */
    char *path_name,            /* in/out - dir's path (MAX_PATH_SZ) */
    struct stat *dir_stat,      /* in - directory's stats */
    ino_t dir_ino,              /* in - directory's inode */
    ino_t parent_ino,           /* in - directory's parent's inode */
    int mount_dev,              /* in - device root dir is mounted on */
    time_t last_dump_date,      /* in - last time files backed up at this level */
    unsigned long *file_cnt     /* in/out - files backed up counter */
    )
{
    void *dp;
    struct dirent *dir_entry;
    struct stat stat_buf;
    int len = strlen( path_name );
    long ret;
    time_t current_time;

    /*-----------------------------------------------------------------------*/

    Num_dirs_backed_up++;

    if (Verbose) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP61, "bd %s/\n"), path_name );
    }

    if (dir_stat->st_dev != mount_dev) {
        /* we've crossed over to a different filesystem */
        dir_stat->st_ino = dir_ino;
        write_dir_attr_rec( ss, path_name, dir_stat, parent_ino, TRUE );
        return;
    }
    
    dp = opendir( path_name );

    if (dp == NULL) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP62, "%s: unable to open directory <%s>; [%d] %s\n"),
                 Prog, path_name, errno, ERR_MSG );
	saved_err = 1;
        return;
    }
    
    write_dir_attr_rec( ss, path_name, dir_stat, parent_ino, TRUE );
 
    /* 
     * append a slash (/)  to the path name 
     */
    path_name[len] = '/';
 
    /* 
     * scan the directory and backup each file in it.  On each pass,
     * check the abort_wanted boolean that could be set by the signal thread
     * to indicate that we are going to stop prematurely.
     */
 
    while ((dir_entry = readdir( dp )) != NULL) {

        mutex__lock( abrt_mutex );
        if (abort_wanted) {
            mutex__unlock( abrt_mutex );
            break;
        } else 
            mutex__unlock( abrt_mutex );
 
        if (!strcmp( dir_entry->d_name, "." ) ||
            !strcmp( dir_entry->d_name, "..") ||
            !strcmp( dir_entry->d_name, ".tags")) {
            /* we skip '.', '..' and '.tags' */
            continue;
        }

        /*
         * Check if amount of time spent backing up files is over the
         * Alarm_time limit (5 minutes).
         */
        current_time = time( NULL );
        Time_amount = difftime( current_time, Last_start_time );

        if ( Time_amount >= Alarm_time ) {
            Last_start_time = print_progress( current_time );
        }

        /* 
         * chop off previous file name 
         */
        path_name[len + 1] = '\0';

        /* 
         * append the file name to the path name 
         */
        path_name = strcat( path_name, dir_entry->d_name );

        if (lstat( path_name, &stat_buf ) == ERROR) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP60,
                     "%s: unable to get info for file <%s>; [%d] %s\n"),
                     Prog, path_name, errno, ERR_MSG );
	    saved_err = 1;
            continue;
        }

        /* since nfs mounts have inodes from the remote nfs server's mount point,
         * and that mount point could be a file system of type ufs, and ufs
         * allows inodes of 4 or 5, it would result in not backing up the nfs
         * directory that happens to have an inode of 4 or 5.  To protect
         * from this condition, test that the quota files we are trying to
         * skip here are of regular type (S_IFREG) and not a IFDIR like an nfs
         * mounted directory is.
         */
        if ((Fs_info.fs_type == MOUNT_MSFS) &&            /* fs being dumped is advfs */
            ( (stat_buf.st_mode & S_IFMT) == S_IFREG ) && /* file is a regular file type, */
                                                          /* meaning not an nfs mount with inode==4,5 */
            ((stat_buf.st_ino == USR_QUOTA_INO) ||
             (stat_buf.st_ino == GRP_QUOTA_INO))) {       /* file is a advfs quota file */
            /* skip quota files */
            continue;
        }

        /*
         *  Check if the modification time (open, read, and write) or 
         *  the status change time (chown, chgrp, chmod) has changed.
         *  Or if it is a directory (they must always be backed up since
         *  we don't know if a subdir will contain a file that we want
         *  to backup).
         */

        if ((stat_buf.st_mtime >= last_dump_date) ||
            (stat_buf.st_ctime >= last_dump_date) ||
            ((stat_buf.st_mode & S_IFMT) == S_IFDIR)) {
            
            /* 
             * The various file types are backed up using various
             * methods.  So, depending on the file type, perform
             * the appropriate method of backing it up.
             */

            switch( stat_buf.st_mode & S_IFMT ) {
                case S_IFDIR:
		    /* avoid creating entries for AutoFS intercept points */
		    if (!(stat_buf.st_flags & AUTOFS_INTERCEPT))
                    	backup_dir_files( ss, path_name, &stat_buf, 
                                          dir_entry->d_ino,
                                          dir_stat->st_ino, 
                                          mount_dev, last_dump_date,
                                          file_cnt);
                    break;

                case S_IFREG:

                    ret = backup_file( ss, path_name, &stat_buf, 
                                       dir_stat->st_ino, file_cnt );

                    if (ret < 0) {
                        break;
                    }

                    Num_files_backed_up++;
                    Bytes_backed_up += ret; 

                    break;

                case S_IFLNK:
		    /* avoid saving AutoFS symlinks */
	            if ( mount_dev == stat_buf.st_dev && 
				!(stat_buf.st_flags & AUTOFS_INTERCEPT)) {
                        /*
                         * avoid backing up symbolic links
                         * created by automount to point
                         * to /tmp_mnt
                         * We can detect them by comparing devices
                         * Automount would break if it was told
                         * to mount on top of a symbolic link
                         * but has no problem if nothing is there
                         */

		         ret = backup_symbolic_link( ss, path_name, &stat_buf, 
                                                dir_stat->st_ino, file_cnt );

                         if (ret < 0) {
				 break;
                         }

			 Num_files_backed_up++;
                         Bytes_backed_up += ret; 


                    }
		    break;

                case S_IFIFO:
                case S_IFCHR:
                case S_IFBLK:
                    Num_files_backed_up++;

                    backup_device_file( ss, path_name, &stat_buf, 
                                        dir_stat->st_ino, file_cnt );
                    break;

                case S_IFSOCK:
                    if (Verbose) {
                        fprintf( stderr,
                                 catgets(catd, S_VDUMP1, VDUMP63, "%s: <%s> not backed up; skipped socket\n"), 
                                 Prog, path_name );
                    }
                    break;

                default:
                    fprintf( stderr,
                             catgets(catd, S_VDUMP1, VDUMP64, "%s: <%s> not backed up; unknown file type\n"), 
                             Prog, path_name );
		    saved_err = 1;
            } /* end switch */            
        }    /* end if */
    } /* end while */
 
    path_name[len] = '\0';                        /* remove slash "/"        */

    closedir( dp );
}

/* end backup_dir_files */

  /*
   *      backup
   *
   *      This is the main backup routine.  It writes a summary record
   *      to the first save-set block.  This block is flushed to force
   *      the directories that follow to start on a new block boundary
   *      (restore depends on this).  After obtaining the date from the
   *      previous backup that occurred at the specified level or lower,
   *      all directories in the source filesystem are backed up.  The
   *      last block of directories is flushed (again, restore relies on
   *      the fact that the backed up files start on a block boundary).
   *      Then the files are backed up.  If the user has write access
   *      privileges, the /etc/vdumpdates file is updated to contain 
   *      information on the dump.
   */

void
backup( 
    char *source,               /* in - path name of file/dir to backup */
    save_set_t *ss,             /* in - save-set descriptor */
    unsigned long *file_cnt     /* in/out - files backed up counter */
    )
{
    struct stat stat_buf,
                uqstat_buf,
                gqstat_buf;
    char dir_name[MAX_PATH_SZ+FILE_NAME_SIZE+1],  /* DON'T MAKE SMALLER */
         uqfile_name[MAX_PATH_SZ+FILE_NAME_SIZE+1],
         gqfile_name[MAX_PATH_SZ+FILE_NAME_SIZE+1];
    time_t last_dump_date = 0;
    int found_exact_level = 0;
    int user_is_root = FALSE;
    int mount_dev;
    struct timezone tz;
    mlBfTagT fTag;
    char *orig_source;

    /*----------------------------------------------------------------------*/
 
    /*
     * Save the unaltered source_dir string, set "source" to the value ".".
     * This used to be done in main() prior to calling "start_backup()", but
     * we need the orig string for insertion into the new summary rec struct.
     */
    orig_source = source;
    source = ".";

    if (lstat( source, &stat_buf ) == ERROR) {
       fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP60, "%s: unable to get info for file <%s>; [%d] %s\n"),
                Prog, source, errno, ERR_MSG );
       saved_err = 1;
       return;
    }
 
    write_summary_rec( ss, orig_source );
 
    if (flush_blk( ss ) == ERROR) {
        return;
    }
 
    mount_dev = stat_buf.st_dev;
 
    if (!S_ISDIR( stat_buf.st_mode )) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP65, "%s: <%s> not backed up; not a directory\n"), 
                 Prog, source );
        return;
    }

    if ((strlen( dir_name ) == 1) && (dir_name[0] == '/') ) {
        dir_name[0] = '\0';
    }

    strcpy( dir_name, source );/* copy to a big buffer */

    /*  Obtain the date from the previous backup that occurred at a lower
     *  level.  If that level does not exist then obtain the date from
     *  next lowest level of the same file system.
     */

    last_dump_date = get_last_backup_date( source, &found_exact_level );
    if (last_dump_date == ERROR) {
	saved_err = 1;
        return;
    }

    ss->save_mode = BF_DIRECTORIES;
    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP66, "%s: Dumping directories\n"), Prog);

    backup_dir( ss, 
                dir_name, 
                &stat_buf, 
                stat_buf.st_ino, 
                stat_buf.st_ino,
                mount_dev, 
                last_dump_date );
    
    /* 
     * Since the fileset will be consistant with its inodes and
     * the hard links, we lookup for missed hard links only 
     * when backing-up unmounted directories using '-D' option.
     */
    if (Backup_subtree)
        lookup_for_missed_hardlinks();

    /* we'll free the memory allocated for hardlink table */
    free_hardlink_tbl();

    if (flush_blk( ss ) == ERROR) {
        return;
    }
  
    ss->save_mode = BF_FILES;

    /*
     * If this is an AdvFS fileset and the user is root and we are backing
     * up the entire fileset, we need to add the quota files into the 
     * sum of bytes and files to be backed up.
     */
    if (!Backup_subtree && Fs_info.fs_type == MOUNT_MSFS) {
        if (!getuid()) {
            user_is_root = TRUE;
            /*
             * Sequence number for quota file will always be the same.
             */
            fTag.seq = 1 | BS_TD_IN_USE;

            fTag.num = USR_QUOTA_INO;
            if (tag_to_path(Fs_info.path, 
                            fTag,
                            sizeof(uqfile_name),
                            uqfile_name)) {
                fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP144, "%s: Could not obtain %s quota file name; [%d] %s\n"), Prog, qfextension[USRQUOTA],
                    errno, ERR_MSG);
                saved_err = 1;
            }
            else {
                if (lstat(uqfile_name, &uqstat_buf)) {
                    saved_err = 1;
                    fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP145, "%s: lstat() failed on %s quota file; [%d] %s\n"), Prog, qfextension[USRQUOTA],
                        errno, ERR_MSG);
                }
                else {
                    if ((uqstat_buf.st_mtime >= last_dump_date) ||
                        (uqstat_buf.st_ctime >= last_dump_date)) {
                        Num_files_to_backup++;
                        Bytes_to_backup += 
                            (((long)(uqstat_buf.st_blocks)) * S_BLKSIZE);
                    }
                    else {
                        /*
                         * This will tell backup_advfs_quotas() not
                         * to backup this file.
                         */
                        uqfile_name[0] = '\0';
                    }
                }
            }

            fTag.num = GRP_QUOTA_INO;
            if (tag_to_path(Fs_info.path, 
                            fTag,
                            sizeof(gqfile_name),
                            gqfile_name)) {
                fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP144, "%s: Could not obtain %s quota file name; [%d] %s\n"), Prog, qfextension[GRPQUOTA],
                    errno, ERR_MSG);
                saved_err = 1;
            }
            else {
                if (lstat(gqfile_name, &gqstat_buf)) {
                    saved_err = 1;
                    fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP145, "%s: lstat() failed on %s quota file; [%d] %s\n"), Prog, qfextension[GRPQUOTA],
                        errno, ERR_MSG);
                }
                else {
                    if ((gqstat_buf.st_mtime >= last_dump_date) ||
                        (gqstat_buf.st_ctime >= last_dump_date)) {
                        Num_files_to_backup++;
                        Bytes_to_backup += 
                            (((long)(gqstat_buf.st_blocks)) * S_BLKSIZE);
                    }
                    else {
                        /*
                         * This will tell backup_advfs_quotas() not
                         * to backup this file.
                         */
                        gqfile_name[0] = '\0';
                    }
                }
            }
        }
    }

    silent_fprintf( catgets(catd, S_VDUMP1, VDUMP67, "%s: Dumping %ld bytes, %ld directories, %ld files\n"), 
                    Prog,
                    Bytes_to_backup, Num_dirs_to_backup, Num_files_to_backup);

    silent_fprintf(catgets(catd, S_VDUMP1, VDUMP68, "%s: Dumping regular files\n"), Prog);

    /* Record the time before starting the backup. Used for estimations. */
    Last_start_time = time( NULL );

    /* 
     * If this is a backup of a full AdvFS fileset, back up the
     * quota files first.
     */
    if (!Backup_subtree && Fs_info.fs_type == MOUNT_MSFS) {
        if (user_is_root) {
            ss->save_mode |= BF_QUOTA_DATA;

	    if (gqfile_name[0] != '\0') {
	        ss->save_mode |= BF_GROUP_QUOTA_DATA;
	    }

	    if (uqfile_name[0] != '\0') {
	        ss->save_mode |= BF_USER_QUOTA_DATA;
	    }

            backup_advfs_quotas(ss,
                                uqfile_name, 
                                gqfile_name, 
                                &uqstat_buf,
                                &gqstat_buf);

            ss->save_mode &= ~BF_QUOTA_DATA;
            ss->save_mode &= ~BF_GROUP_QUOTA_DATA;
            ss->save_mode &= ~BF_USER_QUOTA_DATA;
        }
        else {
            fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP146, "%s: Non-root users cannot backup quota files.\n"), Prog);
        }
    }

    backup_dir_files( ss, 
                      dir_name, 
                      &stat_buf, 
                      stat_buf.st_ino,
                      stat_buf.st_ino, 
                      mount_dev, 
                      last_dump_date, 
                      file_cnt);

    /*
     * If we are aborting, don't update /etc/vdumpdates.
     */
    mutex__lock( abrt_mutex );
    if (!abort_wanted) {
        mutex__unlock( abrt_mutex );
 
        finish_vol_set( ss );

        if ( Update_file ) {
            update_dumpdate_file( source, last_dump_date, &found_exact_level );
        }
    }
    else {
        mutex__unlock( abrt_mutex );
    }

}

/* end backup */

int
file_write_blk( int fd, char *blk, int cnt )

  /*
   * Function:
   *
   *      file_write_blk
   *
   * Function description:
   *
   *      Writes a block to a disk file.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *      blk (in)
   *          - Pointer to the block to be written.
   *      cnt (in)
   *          - Number of bytes to be written.
   *
   * Return value:
   *	sets saved_err if an error occurs.
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
   int rcnt;
   int not_valid_write = 1;

   while (not_valid_write) {
     if (rcnt = write( fd, blk, cnt ) == ERROR) {
        if (errno == ENOSPC) {            /* [28] No space left on device */
          fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP73, 
                  "%s: No more space left on device %s\n"), 
                  Prog, Device_name);
            if (want_abort()) {
                abort_now();
                return ERROR;
            }
        } else {
          fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP74, 
                   "\n%s: unable to write to save-set; [%d] %s\n"),
                   Prog, errno, ERR_MSG );
          saved_err = 1;
          return ERROR;
        } /* end if */

     } else {
         /* successfully wrote block so get out of the loop */
         not_valid_write = 0;

     } /* end if write */

   } /* end while */

   return OKAY;

} /* end file_write_blk */

void
file_close_device( int fd )

  /*
   * Function:
   *
   *      file_close_device
   *
   * Function description:
   *
   *      Closes a disk file (save-set).
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
    if (fd != 1) {     /* don't close stdout */
        fsync( fd );
#ifdef REMOTE
        rmtclose();
#else
        if (close( fd ) < 0){
		/* report a close error, it's serious */
		fprintf(stderr,
		     catgets(catd, S_VDUMP1, VDUMP75, "\n%s: unable to properly close device <%s>; [%d] %s\n"),
                     Prog, Device_name, errno, ERR_MSG );
		saved_err = 1;
	}
#endif
    }

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
   *      Binds the file-specific I/O routines.
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
   *      Modifies global function pointers '_write_blk' and 'close_device'.
   *
   * Notes:
   *
   */

{
   _write_blk   = file_write_blk;
   close_device = file_close_device;
}

/* end file_bind */

/*
 * make_norewind_dev
 *	takes a device spec and attempts to alter it by inserting
 *	the "n" that makes it a no-rewind device
 *	Gave some flexibility to allow the device to exist outside
 *	of /dev.
 *	note: if not a DEC style tape device name ("rmt"), warn that
 *	we will not alter the name (leave it up to the user to specify
 *	a no-rewind tape drive with the -N option).
 */

char *
make_norewind_dev (char *devname)
{
    static char 	temp_name[MAXPATHLEN+1];
    char 		init_blk[MAXPATHLEN+1],end_blk[MAXPATHLEN+1];
    char 		*p_c,*p_c1,*p_c2;

     strcpy(temp_name,devname);
     if( strlen(temp_name) >= MAXPATHLEN )
          return(devname);

     if ( (p_c1 = strstr(temp_name,"/dev")) == NULL ) {
	/* doesn't have either the OSF style tape drive name
         * or the new style device name, so warn user and do nothing
         */
        fprintf( stderr,
                 catgets(catd, S_VDUMP1, VDUMP140, "%s: Cannot convert output filespec <%s> to no-rewind device\n"),Prog,devname);
        return (devname);
     }

     p_c1++;

     if((p_c2 = strpbrk(p_c1, "/")) == NULL)
         return(devname);
     else {
         *p_c2 = '\0';
          strcpy(init_blk,temp_name);
          p_c1=++p_c2;
          strcpy(end_blk,p_c1);
          if((p_c2 = strpbrk(p_c1, "/")) != NULL)
          *p_c2 = '\0';
     }

     if ((strncmp(p_c1, "nrmt",4)  == 0) || (strcmp(p_c1, "ntape") == 0 ))
        /* It's already a no-rewind tape, can't do better than that */
              return(devname);


     if ((strncmp(p_c1, "rmt",3)  != 0) && (strcmp(p_c1, "tape") != 0 )) {
	 /* doesn't have either the OSF style tape drive name
          * or the new style device name, so warn user and do nothing
          */
	 fprintf( stderr,
catgets(catd, S_VDUMP1, VDUMP140, "%s: Cannot convert output filespec <%s> to no-rewind device\n"),Prog,devname); 
         return (devname);
     }

     sprintf(temp_name,"%s%s%s",init_blk,"/n",end_blk);
     fprintf(stderr,"The name of the modified device is %s\n",temp_name);
     return(temp_name);

}

int
check_for_norewind (char *devname)
{
    if (strncmp (devname, "/dev/n", 6) == 0) {
        return (1);           /* found a norewind device */
    } else {
        return (0);
    }
}

int
tape_write_blk( int _not_used_fd, char *blk, int cnt )

  /*
   * Function:
   *
   *      tape_write_blk
   *
   * Function description:
   *
   *      Writes a block to a tape.
   *
   * Arguments:
   *
   *      fd (in)
   *          - Save-set's file descriptor.
   *      blk (in)
   *          - Pointer to the block to be written.
   *      cnt (in)
   *          - Number of bytes to be written.
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
    struct devget devstat;
    int ret;
    int valid_write = FALSE;
    int changed_tape = FALSE;
    struct mtop  mt;
    device_info_t *devinfop;
    long      bytes_written;

#ifdef TEST_MULTI_TAPES
    static int write_cnt = 1;
    static int tape_changes = 0;
    static int max_tape_changes = 2;
#endif
 
    /* malloc this as this is a large structure */
    devinfop = (device_info_t *)malloc(sizeof(device_info_t));
    /* if it fails - keep going but don't do the DEVGETINFO ioctl */

    while (!valid_write) {

#ifdef TEST_MULTI_TAPES
        if (((write_cnt % 3) == 0) &&
            (tape_changes < max_tape_changes)) {
            write_cnt++;
            tape_changes++;
            goto _fake_enospc;
        }
#endif

#ifdef REMOTE
        if ((bytes_written = rmtwrite( blk, cnt )) == cnt) {
#else
        if (write( OutFd, blk, cnt ) != ERROR) {
#endif
            /* successfully wrote block so get out of the loop */
            valid_write = TRUE;
#ifdef TEST_MULTI_TAPES
            write_cnt++;
#endif
   
        } else if (errno == ENOSPC) {         
            /* [28] No space left on device */
 
#ifdef TEST_MULTI_TAPES
_fake_enospc:
            tape_write_eof( OutFd, Tape_number );
#endif

#ifdef REMOTE
            rmtclose();
#else
            if (close( OutFd ) < 0){
            /* report a close error, it's serious */
                fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP75, 
		"\n%s: unable to properly close device <%s>; [%d] %s\n"),
                Prog, Device_name, errno, ERR_MSG );
	        saved_err = 1;
            }
#endif
            OutFd = -1;
            if (vdump_open_device( &OutFd, Device_name ) == ERROR) {
   		post_event_backup_error(Prog);
	        mutex__delete( &tty_mutex );
                mutex__delete( &abrt_mutex );
                exit(1);
            }

            tape_unload( OutFd, Tape_number );

#ifdef REMOTE
            rmtclose();
#else
            if (close( OutFd ) < 0){
            /* report a close error, it's serious */
                fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP75,
                "\n%s: unable to properly close device <%s>; [%d] %s\n"),
                Prog, Device_name, errno, ERR_MSG );
                saved_err = 1;
            }
#endif
            OutFd = -1;

            /* 
             * Commented the following isatty() call to 
             * support vdump running as cron/batch jobs.
             */
#if 0
            if (!isatty(0)) {
                fprintf(stderr, 
                        catgets(catd, S_VDUMP1, VDUMP76, "%s: out of space and unable to prompt input for new tape; [%d] %s\n"),
                        Prog, errno, ERR_MSG );
                if (devinfop)
                    free(devinfop);
                post_event_backup_error(Prog);
                exit ( 1 );
            }
#endif 
            OutFd = tape_get_new( Device_name, Tape_number, remote_host_name );
            if (OutFd >= 0) {
                changed_tape = TRUE;
 
            } else {
		if (devinfop)
                   free(devinfop);
                post_event_backup_error(Prog);
                exit( 1 );
            }
   
        } else {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP77, 
                     "\n%s: unable to write to device <%s>; [%d] %s\n"),
                     Prog, Device_name, errno, ERR_MSG );
	    saved_err = 1;    
            /* get device status of target */
#ifdef REMOTE
            if ((strncmp(os_name,"ULTRIX",6) == 0) && (Dev_type == TAPE)) {
                ultrix_rmtdevice_stat(Device_name); /* we can check if ultrix on other end */
            }
#else
            /*
             * First, attempt to use the DEVGETINFO ioctl (preferred),
             * if that fails, try the backward-compatibility ioctl
             * DEVIOCGET.
             */
            if ((devinfop) &&
                    (ioctl(OutFd, DEVGETINFO, (char *)devinfop) >= 0) &&
                    (devinfop->version == VERSION_1)) { /* success */

                if (devinfop->v1.devinfo.tape.unit_status & TPDEV_OFFLINE) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP70, 
                             "%s: device <%s> is OFFLINE\n"), 
                             Prog, Device_name );

                } else if (devinfop->v1.devinfo.tape.media_status & TPDEV_WRTPROT) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP78, 
                             "%s: device <%s> is WRITE-LOCKED\n"), 
                             Prog, Device_name );
                }

            } else { /* DEVGETINFO failure */

                if (ioctl( OutFd, DEVIOCGET, &devstat ) == ERROR) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP79, 
                             "%s: unable to get device status; [%d] %s\n"),
                             Prog, errno, ERR_MSG );

                } else if (devstat.stat & DEV_OFFLINE) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP70, 
                             "%s: device <%s> is OFFLINE\n"), 
                             Prog, Device_name );

                } else if (devstat.stat & DEV_WRTLCK) {
                    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP78, 
                             "%s: device <%s> is WRITE-LOCKED\n"), 
                             Prog, Device_name );
                }

            }
#endif 

            /* 
             * Commented the following isatty() and 
             * op_retry() call to support vdump running as cron/batch jobs. 
             */
#if 0
            if (!isatty(0)) {
                fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP80, 
                            "%s: unable to prompt input for retry on device; [%d] %s\n"),
                        Prog, errno, ERR_MSG );
                if (devinfop)
                    free(devinfop);
                post_event_backup_error(Prog);
                exit ( 1 );
            }
#endif
            if (!op_retry()) {
		if (devinfop)
                   free(devinfop);
                post_event_backup_error(Prog);
                exit( 1 );
            }
        } 
    } /* end while */
 
    if (changed_tape) {
        Tape_number++;
    }

    if (devinfop)
       free(devinfop);
    return OKAY;
} /* end tape_write_blk */

void
tape_close_device( int fd )

  /*
   * Function:
   *
   *      tape_close_device
   *
   * Function description:
   *
   *      Closes a tape (save-set).
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

    tape_write_eof( fd, Tape_number );

    if (!No_rewind) {
        if(No_unload) {
            silent_fprintf(catgets(catd, S_PATCH1, PATCH_VDUMP81,
                           "%s: Rewinding tape\n"), Prog);
            tape_rewind( fd, Tape_number );
        } else {
            silent_fprintf(catgets(catd, S_VDUMP1, VDUMP81, 
                           "%s: Rewinding and unloading tape\n"), Prog);
            tape_unload( fd, Tape_number );
        }
    }

    /* close the tape device file */
#ifdef REMOTE
    rmtclose();
#else
    if (close( fd ) < 0){
	/* report a close error, it's serious */
	fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP75, 
                "\n%s: unable to properly close device <%s>; [%d] %s\n"),
                Prog, Device_name, errno, ERR_MSG );
	saved_err = 1;
    }
#endif
}

/* end tape_close_device */


void
tape_bind( )

  /*
   * Function:
   *
   *      tape_bind
   *
   * Function description:
   *
   *      Binds the tape-specific I/O routines to the I/O function pointers.
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
   *      Modifies global function pointers '_write_blk' and 'close_device'.
   *
   * Notes:
   *
   */

{
   _write_blk   = tape_write_blk;
   close_device = tape_close_device;
}

/* end tape_bind */

  /*
   * Function:
   *
   *      list_fs_to_backup
   *
   * Function description:
   *`
   *      Displays the file systems that need to be backed up.  For 
   *      every file system listed in the /etc/fstab file, this routine
   *      finds the last backup of the same file system in /etc/vdumpdates.
   *      If its date is greater than 24 hours (86400 seconds), it is
   *      listed to standard output.
   */

void
list_fs_to_backup( )
{
    FILE *fstab_file_fp,
         *dump_file_fp;
/*    int ret;  */
    pid_t pid;
    int *waitstatus;
    char *arglist[2];
    long seek_count = 1;       /* seek in multiples of long words */
    int first_time = 1,        /* first time to look at vdumpdates file */
        first_hour = 1,        /* first time to look at t.tm_hour */
        not_found = 1;         /* device file found in vdumpdates file */
    time_t todays_date;        /* today's date in seconds */
    struct tm t;
    int file_dump_level,
        tm_year;
    char fstab_device[52],
         fstab_path[52],
         fstab_type[12],
         fstab_rest[LINE_MAX],
         dump_device[17],
         tm_wday[4],
         tm_mon[4];

    /*
     * open the /etc/fstab file for reading from beginning of file
     */
    fstab_file_fp = fopen("/etc/fstab", "r"); 
    if (fstab_file_fp == NULL) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP82, "Cannot open /etc/fstab for reading; [%d] %s\n"),
               errno, ERR_MSG);
	saved_err = 1;
        return;
    }

    /* wait until no other processes have an exclusive lock on the */
    /* /etc/fstab file and then put a shared lock on it */

    (void) flock((int) fileno(fstab_file_fp), LOCK_SH);

    /*
     * open the /etc/vdumpdates file for reading from end of file
     */
    dump_file_fp = fopen(Dump_hist_file_name, "r");
    if (dump_file_fp == NULL) {
        fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP27, 
                "Cannot open dump history file %s for reading; [%d] %s\n"),
                Dump_hist_file_name, errno, ERR_MSG);
	saved_err = 1;
        fclose(fstab_file_fp);
        return;
    }

    /* wait until no other processes have an exclusive lock on the */
    /* dump history file and then put a shared lock on it */

    (void) flock((int) fileno(dump_file_fp), LOCK_SH);

    todays_date = time(NULL);         /* get today's date in seconds */

    fprintf(stderr, catgets(catd, S_VDUMP1, VDUMP83, "Dump these file systems:\n"));
    while (!feof(fstab_file_fp)) {    /* for each file system in fstab */

        /* 
         * if cannot retrieve three items from fstab then done
         */
        if ( fscanf(fstab_file_fp, "%s%s%s", 
                    fstab_device, fstab_path, fstab_type) != 3 ) {
            fprintf(stderr, "-----------------------------------\n");
            break;
        } else {
            fgets(fstab_rest, LINE_MAX, fstab_file_fp);
        } 

        if ( (strcmp(fstab_type, "msfs") == 0) ||
             (strcmp(fstab_type, "advfs") == 0) ) { 

           /*
            * if not nfs mounted
            */
            not_found = 1;         /* device not found in /etc/vdumpdates */
            seek_count = 1;        /* reset the vdumpdates file pointer */

            while (not_found) {
               /*
                * while there exist records in /etc/vdumpdates or
                * until found the exact file system.
                */

                if (fseek(dump_file_fp, seek_count, SEEK_END) == 0) {

                    if (first_time) {
                        first_time--;
                    } else {
                        fscanf(dump_file_fp, "%s%d%s%s%d%d:%d:%d%d",
                               dump_device, &file_dump_level, tm_wday,
                               tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min,
                               &t.tm_sec, &tm_year);

                        /* 
                         * if devices match then compare device's date 
                         * to today's date (must be less than 86400=1day
                         * seconds) 
                         */

                        if (strcmp(dump_device, fstab_device) == 0) {    
                            t.tm_wday = atoi(tm_wday);
                            t.tm_mon = get_month_num(tm_mon);
                            t.tm_year = tm_year - 1900;

                            if (difftime(todays_date, mktime(&t)) > 86400) {
                                fprintf(stderr,
                                        catgets(catd, S_VDUMP1, VDUMP84, "%s (%s) Last dump: Level %d, Date %s %s %d %d:%d\n"),
                                        dump_device, fstab_path, file_dump_level,
                                        tm_wday, tm_mon, t.tm_mday, 
                                        t.tm_hour, t.tm_min);
                            }
                            not_found = 0;   /* found the exact file system */
                        } /* end if for strncmp*/
                    } /* end if for first_time */
                    seek_count = seek_count -1;  /* search for next record */

                } else {                         /* end of vdumpdates file */
                    not_found = 0;   /* didn't find file system so list it */
                    fprintf(stderr,
                            catgets(catd, S_VDUMP1, VDUMP85, "%s (%s) Last dump: Never backed up\n"),
                            fstab_device, fstab_path);
                } /* end if for fseek */
            } /* end while for vdumpdates */
        } /* end if for strstr */
    } /* end while for fstab */

    /* closing these files also releases the shared locks */
    fclose(fstab_file_fp);
    fclose(dump_file_fp);

} /* end list_fs_to_backup */

void
usage( )

  /*
   * Function:
   *
   *      usage
   *
   * Function description:
   *
   *      Displays on stdout the usage parameters for backup.
   *
   */

{
    fprintf( stderr, "\n" );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP86, "Usage:\n\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP87, "%s [-0..9] [-CDNPVhquvw] [-F num_buffers] [-T tape_num]\n"), Prog);

#ifdef REMOTE
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP147, "\t[-b size] -f device [-x num_blocks] fileset\n"));
    fprintf( stderr, catgets(catd, S_VDUMP1, USAGE14, "\twhere the device is specified as [user@]machine\:device.\n"));
#else /* !REMOTE */
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP88, "\t[-b size] [-f device] [-x num_blocks] fileset\n"));
#endif

    fprintf( stderr, "\n" );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP89, "Options:\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP90, "   -0..9\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP91, "\tSpecifies the dump level (full or incremental backup).\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP92, "   -C \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP93, "\tCompresses the data to minimize the save set size.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP94, "   -D \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP95, "\tBacks up the specified subdirectory of a fileset.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP96, "   -F <num_buffers>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP97, "\tSpecifies the number of in-memory buffers to use. The\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP98, "\tvalid range is 2 - 64; default is 8.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP99, "   -N \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP100, "\tDoes not rewind the tape.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP155, "   -U \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP156, "\tDoes not unload the tape.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP101, "   -T <tape_num>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP102, "\tSpecifies the starting number for first tape. The\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP103, "\tdefault is 1.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP104, "   -V \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP105, "\tDisplays the current %s version.\n"), Prog );
#if 0
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP106, "   -Z\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP107, "\tBackup pages of zeros\n"), Prog );
#endif
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP108, "   -b <size>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP109, "\tSpecifies the block size in 1024-byte units. The\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP170, "\tvalid range is %d - %d; default is 60.\n"),MIN_CHUNKS,OLD_MAX_CHUNKS );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP111, "   -f <device>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP112, "\tSpecifies the destination save-set device or file.\n") );
#ifdef REMOTE
    fprintf( stderr, catgets(catd, S_VDUMP1, USAGE14, "\twhere the device is specified as [user@]machine\:device.\n"));
#endif
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP113, "   -h \n") );
#ifdef REMOTE
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP150, "\tDisplays usage help for rvdump.\n") );
#else
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP114, "\tDisplays usage help for vdump.\n") );
#endif
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP115, "   -q \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP116, "\tDisplays only error messages; not information messages.\n"));
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP117, "   -u \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP118, "\tUpdates the /etc/vdumpdates file.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP119, "   -v \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP120, "\tDisplays the names of files being backed up.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP121, "   -w \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP122, "\tDisplays the filesets not backed up in the past week.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP123, "   -x <num_blocks> \n") );
    fprintf( stderr, 
             catgets(catd, S_VDUMP1, VDUMP124, "\tEnables xor save set format. The valid range is 2 - 32;\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP125, "\tdefault is 8.\n") );

#if 0
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP126, "   -r \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP127, "\tDisplays resource usage statistics.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP128, "   -t \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP129, "\tDisplays CPU, elapsed times, and transfer rates.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP130, "   -B \n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP131, "\tSpecifies the block mode device in 1024-byte blocks\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP132, "\tinstead of tape length in feet, which is the default.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP133, "   -S <feet_tape_size>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP134, "\tSpecifies output file size in feet.\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP135, "   -d <density>\n") );
    fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP136, "\tSpecifies the write density of device in bits per inch.\n") );
#endif
    fprintf( stderr, "\n" );
}

/* end usage */


int
write_blk( 
    struct blk_t **blk, 
    int cnt 
    )
{
    mesg_t msg; 
 
    /* set up message to the write thread */
    msg.type = MSG_WRITE_BLK;
    msg.blk = *blk;
 
    /* reset the block pointer to NULL to prevent accidental reuse */
    *blk = NULL;
 
    /* send message to the write thread */
    msg_snd( Wt_mq, (char *) &msg, sizeof( msg ) );

#if 0
    _write_blk( OutFd, 
                (char *) *blk, 
                (*blk)->bhdr.bheader.block_size );

    blk_release( Blk_pool_h, *blk );
    *blk = NULL;

#endif

    Bytes_written += cnt;

    return OKAY;
}

/* end write_blk */


/*************************************************************************
 * Link Hash Table Routines
 *
 * The Link Hast Table is used to keep track of files that have
 * multiple names (hard links).  
 *************************************************************************/

#define LINK_TBL_SZ 512
#define LINK_TBL_HASH( ino ) (ino % LINK_TBL_SZ)

/*
 * link_entry_t
 *
 * Defines a Link Hash Table entry.  It describes a file that has
 * several links (names).  The name in the entry is the first name
 * that backup found for this files.  It is considered to be the
 * primary name and all other names for this file are considered
 * to be links that refer to the primary name.
 */

typedef struct link_entry {
    ino_t ino;                  /* the file's inode number */
    char *name;                 /* the file's name */
    ino_t parent_ino;           /* the file's directory's inode number */
    struct link_entry *nxt;     /* ptr to next hash tbl entry */
} link_entry_t;

static link_entry_t *Link_tbl[LINK_TBL_SZ];

/*
 * link_tbl_add
 *
 * Looks up the inode in the table.  If it is already in the
 * table the routine returns TRUE.  Otherwise, the inode is
 * added to the Link Table and the routine returns FALSE.
 */

int
link_tbl_add(
    ino_t ino,          /* in - file's inode number */
    char *name,         /* in - file's primary name */
    ino_t parent_ino    /* in - file's directory's inode number */
    )
{
    int idx = -1;
    link_entry_t *cur_ent = NULL, *new_ent = NULL;

    idx = LINK_TBL_HASH( ino );

    /* Search for inode in table */

    cur_ent = Link_tbl[idx];

    while (cur_ent != NULL) {
        if (cur_ent->ino == ino) {
            return TRUE;  /* we already know about this inode */
        }

        cur_ent = cur_ent->nxt;
    }

    /* Inode was not found so we add it to the table */

    new_ent = (link_entry_t *) malloc( sizeof( link_entry_t ) );
    if (new_ent == NULL) {
        abort_prog( catgets(catd, S_PATCH1, PATCH_VDUMP173, "%s: Not enough memory to generate link table.\n"), Prog );
    }

    new_ent->name = malloc( strlen( name ) + 1 );
    if (new_ent->name == NULL) {
        abort_prog( catgets(catd, S_PATCH1, PATCH_VDUMP173, "%s: Not enough memory to generate link table.\n"), Prog );
    }

    strcpy( new_ent->name, name );
    new_ent->ino = ino;
    new_ent->parent_ino = parent_ino;

    /* Add the entry to the linked list associated with it's hash bucket */
    new_ent->nxt = Link_tbl[idx];
    Link_tbl[idx] = new_ent;

    return FALSE; /* we didn't already know about this inode */
}

/*
 * link_tbl_lookup
 *
 * Looks up the inode in the table.  If it is already in the
 * table the routine returns TRUE.  Otherwise, FALSE is returned.
 * If the inode is found then the inode's primary file name and
 * directory's inode number are also returned.
 */

int
link_tbl_lookup(
    ino_t ino,          /* in - file's inode number */
    char **name,        /* out - file's primary file name */
    ino_t *parent_ino   /* out - file's directory's inode number */
    )
{
    int idx = -1;
    link_entry_t *cur_ent = NULL, *new_ent = NULL;

    idx = LINK_TBL_HASH( ino );

    /* Search for inode in table */

    cur_ent = Link_tbl[idx];

    while (cur_ent != NULL) {

        if (cur_ent->ino == ino) {
            *parent_ino = cur_ent->parent_ino;
            *name = cur_ent->name; /* don't strcpy since entries never die */
            return TRUE;  /* we found this inode */
        }

        cur_ent = cur_ent->nxt;
    }

    return FALSE; /* we didn't find this inode */
}


/*
 * hardlink_tbl_entry_t
 *
 * Defines a hardlink hash table entry to keep track of the hardlinks
 * only. The inodes in the entry is the first of the inodes 
 * that backup found for the file. The linked list is used to identify 
 * and report the existence of hard links in the backing directory.
 */

typedef struct hardlink_tbl_entry {
    ino_t ino;                  /* the file's inode number */
    uint_t nlink_count;        /* st_nlink count for a hard linked file. */
    uint_t links_found;        /* No. of hard links found for this file.*/
    struct hardlink_tbl_entry *nxt;     /* ptr to next hash tbl entry */
} hardlink_tbl_entry_t;

static hardlink_tbl_entry_t *Hardlink_tbl_entry[LINK_TBL_SZ];

/*
 * hardlink_tbl_add
 *
 * This table is used to keep track of hardlinks in the fileset 
 * and estimate correct value of Bytes_to_backup for hardlinks.
 * Looks up the inode in the table. If it is already present,
 * increments the links_found count by one and routine returns FALSE. 
 * Otherwise, the inode is added to the hardlink table and the 
 * routine returns TRUE.  
 */

int
hardlink_tbl_add(
    ino_t ino,          /* in - file's inode number */
    uint_t nlink_count     /* in - file's link count */
    )
{
    int idx = -1;
    hardlink_tbl_entry_t *cur_ent = NULL, *new_ent = NULL;

    idx = LINK_TBL_HASH( ino );

    /* Search for inode in table */

    cur_ent = Hardlink_tbl_entry[idx];

    while (cur_ent != NULL) {
        if (cur_ent->ino == ino) {
            cur_ent->links_found++;
            /* At any given point in time the no. of hard links 
             * found for a file should be less than OR equal to the 
             * no. of st_nlinks.
             */ 
            if (cur_ent->links_found > cur_ent->nlink_count)
                fprintf (stderr, catgets(catd, S_PATCH1, PATCH_VDUMP175, "ERROR: Hard link count, for tag/inode number (%d), inconsistent.\n"), 
                        cur_ent->ino); 
           
            return FALSE;  /* we already know about this inode */
        }

        cur_ent = cur_ent->nxt;
    }

    /* Inode was not found so we add it to the table */

    new_ent = (hardlink_tbl_entry_t*) malloc(sizeof(hardlink_tbl_entry_t));
    if (new_ent == NULL) {
        abort_prog( catgets(catd, S_PATCH1, PATCH_VDUMP173, "%s: Not enough memory to generate link table.\n"), Prog );
    }

    new_ent->ino = ino;
    new_ent->nlink_count = nlink_count; 
    new_ent->links_found = 1;  /*because current inode is one of the links.*/

    /* Add the entry to the linked list associated with it's hash bucket */
    new_ent->nxt = Hardlink_tbl_entry[idx];
    Hardlink_tbl_entry[idx] = new_ent;

    return TRUE; /* we didn't already know about this inode, 
                    inserted a new table entry */
}

/* free_hardlink_tbl
*
* This function frees the memory allocated by hardlink_tbl_add
* function. 
*/
void free_hardlink_tbl(void)
{
    int idx = 0;
    hardlink_tbl_entry_t *link_ent = NULL, *tmp = NULL;

	for ( idx = 0; idx < LINK_TBL_SZ; idx++ ) {
	
        link_ent = Hardlink_tbl_entry[idx];
	
	    while (link_ent != NULL) {
		      tmp = link_ent->nxt;
		      free (link_ent);
		      link_ent = tmp;
        }
    }

} 

/* lookup_for_missed_hardlinks
*
* This function checks for the probable mismatch in the link count
* for a file and the actual hardlinks found during the estimation 
* navigation. This mismatch may be due to hard links beyond dump directory.
* The function is called when all the dirs are navigated in estimation stage.
*/

void lookup_for_missed_hardlinks ( void )
{
    int idx = 0, status = 0;
    hardlink_tbl_entry_t *lookup_tbl = NULL;
    struct statfs fs_stats;

    /* Since we have already cd'ed into the source dir
     * we can statfs on the pwd and know the mount point.
     * We need to print the message with mount point 
     * case hard links are missed.
     */
    status = statfs(".", &fs_stats); 

    /* Search for inode in table */

    for ( idx = 0; idx < LINK_TBL_SZ ; idx++ ) {
        lookup_tbl = Hardlink_tbl_entry[idx];

        while ( lookup_tbl != NULL ) {
            /* Found a valid Hash table entry. Now chk for link count */

            if ( lookup_tbl->nlink_count != lookup_tbl->links_found ) {

                fprintf (stderr, catgets(catd, S_PATCH1, PATCH_VDUMP176, "WARNING: (%u) out of (%u) hard links, for tag/inode number (%d), \n\tcannot be backed up via the specified path.\n"), (lookup_tbl->nlink_count - lookup_tbl->links_found), (lookup_tbl->nlink_count), lookup_tbl->ino);

                if (status != -1) {
                    fprintf (stderr, catgets(catd, S_PATCH1, PATCH_VDUMP177, "\tUse 'find %s -i %d' \n\tto get complete list of pathnames for the hard link.\n"),fs_stats.f_mntonname, lookup_tbl->ino);
                }
            }

            /* we may have a bucket entry to chk. */
            lookup_tbl = lookup_tbl->nxt;
        }
    } /* end of for loop */
}


void
prepare_signal_handlers( void )
{
    t_thread        st_handle;
    pthread_attr_t  thread_attr; 

    /* Spawn a new thread to handle signals; new thread runs signal_thread() */
    thread__attr_create( &thread_attr );
    thread__create( &st_handle, 
                     thread_attr,
                     signal_thread,
                     (any_t) NULL );
    thread__yield();
    
}


t_start_routine
signal_thread( void *arg )
{
    int      abort = 0;                 /* TRUE = abort vdump operation */
    int      sig_delivered;
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
    sigprocmask(SIG_SETMASK, newsetp, NULL);

    newint.sa_handler = SIG_IGN;
    newint.sa_mask    = 0;
    newint.sa_flags   = 0;
    newquit.sa_handler = SIG_IGN;
    newquit.sa_mask    = 0;
    newquit.sa_flags   = 0;

    while (!abort) {
        /* now wait for one of the above signals to be delivered */
	if (sigwait(newsetp, &sig_delivered))
		perror("vdump:sigwait");
	else if (sig_delivered == SIGINT || sig_delivered == SIGQUIT) {
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

    /* ignore any INT or QUIT signals while we terminate */
    sigaction(SIGINT, &newint, NULL);
    sigaction(SIGQUIT, &newquit, NULL);
    return OKAY;
}


/* Interactive query to abort; return TRUE if abort desired, else FALSE.
 * If stdout is being used for output, this will return TRUE.  This routine
 * may be called from threads other than the signal-thread.
 */
int want_abort( )
{
    char     abort_answer[10];          /* contains overwrite answer */
    char *answerp = NULL;           /* User's answer */
    int valid = 0;
    int abort = 0;

    if (Use_stdout || !Local_tty) {
        /* can't prompt user; stdout being used for output */
        return(1);
    }

    while (!valid ) {            /* answer must be yes or no */
        mutex__lock( tty_mutex );
        fprintf (stderr,
            catgets(catd, S_VDUMP1, VDUMP138, 
            "\n%s: Do you want to abort the dump? (yes or no) "),
            Prog);

        /*
         * We need an answer from the user.  If there is no way to
         * get one (Local_tty is NULL), then abort the job.  Otherwise,
         * it may be that Local_tty is still initialized to stdin but
         * a read on stdin may not be possible.  This would be the
         * case if we were running through cron.  Therefore, it is
         * crucial to check the return value from fgets().
         */
        if ((answerp = fgets (abort_answer, 6, Local_tty)) == NULL) {
            /* user can't respond; abort it */
            abort_answer[0] = 'y';
		}
        mutex__unlock( tty_mutex );

        if ((abort_answer[0] == 'y') || (abort_answer[0] == 'Y')) {
            valid = 1;
            abort = 1;
        } else if ((abort_answer[0] == 'n') || (abort_answer[0] == 'N')) {
            valid = 1;
        } else {
            fprintf(stderr, "\n");
        } 
    } 
    return( abort );
}

void abort_now()
{
   /* Set the abort flag so other threads know to stop processing. */
   mutex__lock( abrt_mutex );
   abort_wanted = TRUE;		/* Other threads will detect this */
   mutex__unlock( abrt_mutex );
}

/* This thread handles the I/O for the main thread.  
 * The 'writes_are_ok' variable is normally TRUE;  it is turned off when:
 *   a. there is an error on the output and the user signified that they 
 *      wanted to abort, or
 *   b. the signal_thread signifies an abort is pending.
 * This variable is manipulated only in this routine, and is therefore 
 * not protected by a mutex.
 * This turning off of actual I/O allows this thread  to continue to run 
 * until the main thread signals it to terminate.  When processing large files,
 * the main thread may require the write thread to recycle some buffers before
 * it can detect that an abort is desired, and keeping this thread running 
 * without doing I/O speeds that process up dramatically.
 */

t_address
write_thread( t_address input_ptr )
{
    mesg_t msg;
    int retval;
    static int writes_are_ok = 1;
 
    /*-----------------------------------------------------------------------*/
 
    /*
     * receive and process messages
     */
 
    while (TRUE) {
        msg_recv( Wt_mq, (char *) &msg, sizeof( mesg_t ) );
  
        if (msg.type == MSG_WRITE_BLK) {

            if (writes_are_ok) {
                retval = _write_blk( OutFd, 
                         (char *) msg.blk, 
                         msg.blk->bhdr.bheader.block_size );
                /* On output error, if user wants abort, do no more output */
                if (retval == ERROR) {
                    mutex__lock( abrt_mutex );
                    if (abort_wanted)
                        writes_are_ok = 0;
                    mutex__unlock( abrt_mutex );
                }
            }
   
            blk_release( Blk_pool_h, msg.blk );

        } else if (msg.type == MSG_TERMINATE) {
            break;
        } else {
            fprintf( stderr, 
                     catgets(catd, S_VDUMP1, VDUMP139, 
                     "%s: received bad message; WriteThread terminating\n"),
                     Prog );
            break;
        }
    }
 
    return NULL;

} /* end write_thread */

  /*
   *      vdump_open_device
   *
   *      Opens the target save-set device and binds the appropriate
   *      save-set device-specific I/O routines to the I/O for vdump.
   *      function pointers.
   *
   * Return value:
   *
   *      OKAY - If successful.
   *      ERROR - If an error occurred.
   *
   */
int
vdump_open_device(
    int *dev_fd,        /* out - open file desc of 'dev' */
    char *dev           /* in - device name */
    )
{
    int fd, dup_fd,dev_type;
    struct stat t_stat;

#ifdef REMOTE

    if (rmtopen_device(dev) < 0) {
                        abort_prog("Cannot open remote device file.\n");
    }

#else /* NOT REMOTE */

    if (Use_stdout) {

        fd = 1;                   /* STDOUT_FILENO */

        if (Blk_size == 0) {
            Blk_size = BUF_SIZE; /* keep it small for pipes */
        }

        file_bind();

        *dev_fd = fd;
        return OKAY;
    }

    Dev_type = NONE;
    if ((stat( dev, &t_stat ) >= 0 ) &&
        (S_ISCHR( t_stat.st_mode ) || S_ISBLK( t_stat.st_mode ))) {

        /*
         * dev is a block or character special device
         */

        if ((fd = open( dev, O_WRONLY, 0 )) == ERROR) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP69,
                     "%s: can't open dev <%s>; [%d] %s\n"),
                     Prog, dev, errno, ERR_MSG );
            return ERROR;
        }

        /*
         * If device is /dev/null, don't get the type as the
         * ioctl call will fail.
         */
        if (0 == strcmp (dev, "/dev/null")) {
            Dev_type = DISK;
            file_bind();
        }
        else {
            /*
             * get the device type being written to and bind
             * the appropriate functions for the device
             */
            
            if( (dev_type = get_dev_type(fd,dev)) == ERROR ) {
                fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP69,
                         "%s: can't open dev <%s>; [%d] %s\n"),
                         Prog, dev, errno, ERR_MSG );
                return ERROR;
            } else {

               No_rewind  = (dev_type ? TRUE : FALSE);
               
            } 
        }

    } else {
        /*
         * dev is a disk file
         */
        Dev_type = DEV_FILE;

        fd = open( dev, O_WRONLY | O_CREAT | O_TRUNC | O_NDELAY, 0666 );
        if (fd == ERROR) {
            fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP69,
                     "%s: can't open dev <%s>; [%d] %s\n"),
                     Prog, dev, errno, ERR_MSG );
            return ERROR;
        }

        /* bind file-specific I/O routines */
        file_bind();

        /*
         * if no block size for the file is specified, then default
         * to the file's optimal block size for i/o operations.
         */
        if (Blk_size == 0) {
            Blk_size = 60 * BLK_CHUNK_SZ;
        }

    } /* end if */

    *dev_fd = fd;

#endif /* !REMOTE */

    /*
     * if no block size for the file is specified, then default
     * to the file's optimal block size for i/o operations.
     */

    if (Blk_size == 0) {
        Blk_size = 60 * BLK_CHUNK_SZ;
    }

    return OKAY;
} /* end vdump_open_device */


/*
 * Check if path pointed to by the pathname is a mount point.
 * In case of CDSL we have link target name different from the 
 * mount point name, even tough CDSL point to the mount point.
 * A simple strcmp won't work in that case.
 */
static int check_if_mountpoint(const char *pathname, const char *mount_point)
{
    struct stat stat_pathname, stat_mountpoint;
    /*
     * Only do the expensive stat when names are different
     */
    if( strcmp(pathname, mount_point) == 0 ) 
        return TRUE;

    if( stat(pathname, &stat_pathname) < 0 ) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14, "%s: error accessing file system <%s>; [%d] %s\n"),
                Prog, pathname, errno, ERR_MSG );
        return FALSE;
    }

    if( stat(mount_point, &stat_mountpoint) < 0 ) {
        fprintf( stderr, catgets(catd, S_VDUMP1, VDUMP14, "%s: error accessing file system <%s>; [%d] %s\n"),
                Prog, mount_point, errno, ERR_MSG );
        return FALSE;
    }

    /* 
     * Additional checks to make sure the pathname and mountpoint are 
     * referring to the same mounted dir.
     */
    if( (stat_pathname.st_ino == stat_mountpoint.st_ino ) && 
            (stat_pathname.st_dev == stat_mountpoint.st_dev ) ) {
        return TRUE;

    } else {
        return FALSE;
    }
}

/* end vdump.c */
