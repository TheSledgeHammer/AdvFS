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
#pragma ident "@(#)$RCSfile: shblk.c,v $ $Revision: 1.1.9.1 $ (DEC) $Date: 2001/10/10 21:25:46 $"

#include <stdio.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <locale.h>
#include "shblk_msg.h"

nl_catd catd;
static char *Prog_name = "shblk";

void
usage( void )
{
   fprintf( stderr, catgets(catd, 1, USAGE, "usage: %s [-startblk N] [-blkcnt N] filename\n"), Prog_name );
   fprintf( stderr, catgets(catd, 1, 2, "usage: %s [-sb N] [-bc N] filename\n"), Prog_name );
}

main( int argc, char *argv[] )
{
   int error = 0;
   ulong start_blk = 0;
   ulong cur_blk, max_blk;
   uint blk[512/sizeof(uint)];
   ulong blk_cnt = 1;
   int fd = -1;
   long pos, i;
   char *file_name = "UNK";
   struct stat stats;
   int n;

   (void) setlocale(LC_ALL, "");
   catd = catopen(MF_SHBLK, NL_CAT_LOCALE);

/* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,13,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

   if (argc < 2) {
      usage();
      exit( 1 );
   }

   /* store only the file name part of argv[0] */
   if ((Prog_name = strrchr( argv[0], '/' )) == NULL)
      Prog_name = argv[0];
   else
      Prog_name++;

   /* move past program name */
   argv++;
   argc--;

   if (argc <= 0) {
      usage();
      exit( 1 );
   }

   /*
    ** Get user-specified command line arguments.
    */

   while (!error && (argc > 0) && ((*argv)[0] == '-')) {

      if ((strcmp( *argv, "-sb" ) == 0) ||
          (strcmp( *argv, "-startblk" ) == 0)) {
         /* single param required */
         argv++; argc--;

         if ((argc < 1) || ((*argv)[0] == '-')) {
            fprintf( stderr, catgets(catd, 1, 3, "\n%s: missing parameter for option <-sb>\n"), Prog_name );
            error = 1;
         } else {
            start_blk = atoi( *argv );

            argv++; argc--;
         }

      } else if ((strcmp( *argv, "-bc" ) == 0) ||
                 (strcmp( *argv, "-blkcnt" ) == 0)) {
         /* single param required */
         argv++; argc--;

         if ((argc < 1) || ((*argv)[0] == '-')) {
            fprintf( stderr, catgets(catd, 1, 4, "\n%s: missing parameter for option <-bc>\n"), Prog_name );
            error = 1;
         } else {
            blk_cnt = atoi( *argv );
            argv++; argc--;
         }

      } else {
         fprintf( stderr, catgets(catd, 1, 5, "%s: illegal option <%s>\n"), Prog_name, *argv );
         error = 1;
      }
   }

   if (error) {
      usage();
      exit( 1 );
   } else if (argc > 1) {
      fprintf( stderr, catgets(catd, 1, 6, "%s: bad arg <%s>\n"), Prog_name, *argv );
      exit( 1 );
   } else if ((argc < 1) || ((*argv)[0] == '-')) {
      fprintf( stderr, catgets(catd, 1, 7, "%s: missing file name\n"), Prog_name );
      exit( 1 );
   }

   file_name = *argv;

   fd = open( file_name, O_RDONLY );
   if (fd < 0) {perror( catgets(catd, 1, 8, "open") ); exit( 1 );}

   if (fstat( fd, &stats ) < 0) {perror( catgets(catd, 1, 9, "fstat") ); exit( 1 );}

   pos = lseek( fd, start_blk * sizeof(blk), SEEK_SET );
   if (pos < 0) {perror( catgets(catd, 1, 10, "lseek") ); exit( 1 );}

   if (S_ISREG( stats.st_mode )) {
      max_blk = ( stats.st_size + sizeof(blk) -1 ) / sizeof(blk);
   } else {
      max_blk = (0x7fffffffffffffff) / sizeof(blk) ;
   }

   if (start_blk > max_blk) {
      fprintf( stderr, catgets(catd, 1, 11, "%s: startblk <%d> exceeds file's max blk <%d>\n"),
              Prog_name, start_blk, max_blk );
      exit( 1 );
   }

   if (start_blk + blk_cnt > max_blk) {
      blk_cnt = max_blk - start_blk;
   }

   for (cur_blk = start_blk; cur_blk < (start_blk + blk_cnt); cur_blk++){
      n = read( fd, blk, sizeof(blk) );
      if ( n == 0) {
          fprintf( stderr, catgets(catd, 1, 14, "End-of-file reached\n") );
          break;
      }
      else if (n < 0) {
         perror( catgets(catd, 1, 12, "read") ); exit( 1 );
      }


      /*
       * We need to zero out any bytes that were not read in for a short block.
       * Also terminate when we have a short block.
       */
      if ( n != sizeof(blk) ) {
          /*
           * Only part of the block was filled with new data.
           * Zero out the unread portion.
           */
          char *p = (char *)&blk[0];
          for( i = n; i < sizeof(blk); i++ ) {
              p[i] = 0;
          }
      }

      /*
       * Now display the hexadecimal dump of the data that was read.
       */
      for (i = 0; i < ( n + sizeof(blk[0]) - 1) / sizeof(blk[0] ); i++) {
          if ((i % 8) == 0) {printf( "\n%4d: ", cur_blk );}
          printf( "%08x ", blk[i] );
      }

      printf( "\n" );
   }

   close( fd );
}
