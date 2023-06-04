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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/param.h>

static char *prog;            /* progname */
static char *subprog;         /* sub program name */

void usage();

static void bmt_usage(int);
static void log_usage(int);
static void tag_usage(int);
static void sbm_usage(int);
static void file_usage(int);
static void usage_all();
static void operand_usage();


main(argc, argv)
int argc;
char *argv[];
{
    char *pC;

    /* store only the file name part of argv[0] */

    if ((prog = strrchr( argv[0], '/' )) == NULL) 
      {
        prog = argv[0];
      } else {
        prog++;
      }

    /* Must be root to run */
    if( geteuid() ) {
	fprintf(stderr,
	    "\nPermission denied - user must be priviledged to run %s.\n\n",
	    prog);

	exit(1);
    }

    /* convert the subcommand to lower case for case insensitive parsing */

    for (pC = argv[1]; *pC; pC++)
      *pC = tolower(*pC);

    subprog = strdup(argv[1]);

    /* print usage message? */

    if ( argc < 2 ) 
      usage_all();

    if (argc == 2)
      usage();

    /* call the subcommand functions */

    if (!strcmp(subprog, "bmt") || 
	!strcmp(subprog, "rbmt"))
      {
	bmt_main(argc -1, &argv[1]);
	exit(0);
      }

    if (!strcmp(subprog, "log"))
      {
	log_main(argc -1, &argv[1]);
	exit(0);
      }

    if (!strcmp(subprog, "sbm"))
      {
	sbm_main(argc -1, &argv[1]);
	exit(0);
      }

    if (!strcmp(subprog, "tag") || 
	!strcmp(subprog, "rtag"))
      {
	tag_main(argc -1, &argv[1]);
	exit(0);
      }

    if (!strcmp(subprog, "file"))
      {
	file_main(argc -1, &argv[1]);
	exit(0);
      }

    fprintf(stderr, "%s: subcommand '%s' is unknown\n", prog, subprog);

} /* main */


/************************************************************************
 *
 * usage...
 *
 * print a sub-program specific usage message.
 *
 ***********************************************************************/

void
usage()
{
    fprintf(stderr, "\nusage:\n");

    if (!strncmp(subprog, "bmt", 3) || 
	!strncmp(subprog, "rbmt", 4))
      {
	bmt_usage(1);
	exit(1);
      }

    if (!strncmp(subprog, "log", 3))
      {
	log_usage(1);
	exit(1);
      }

    if (!strncmp(subprog, "sbm", 3))
      {
	sbm_usage(1);
	exit(1);
      }

    if (!strncmp(subprog, "tag", 3) || 
	!strncmp(subprog, "rtag", 4))
      {
	tag_usage(1);
	exit(1);
      }

    if (!strncmp(subprog, "file", 4))
      {
	file_usage(1);
	exit(1);
      }

    usage_all();

} /* usage */

/*****************************************************************************/

static void 
usage_all()
{
  /* 
     changes from old Tru64 VODS utility options ( old == new )
       nvbmtpg: -s == -L, -f == -n
       vfilepg: -f d == -f dir
       nvlogpg: -B == -T,  -f == -c
       vsbmpg: -v == -C
  */

  fprintf(stderr, "\nusage:\n");

  bmt_usage(NULL);
  log_usage(NULL);
  sbm_usage(NULL);
  tag_usage(NULL);
  file_usage(NULL);
  operand_usage();

  exit(1);

} /* usage_all */

static void
bmt_usage(int operands)
{
  fprintf(stderr, "%s {rbmt | bmt}\n", prog);
  fprintf(stderr, "\t[-v] <fs> [-n]                  print summary [of free mcells]\n");
  fprintf(stderr, "\t[-v] <src> [-n]                 print summary [of free mcells]\n");
  fprintf(stderr, "\t[-v] <src> -p page [-n]         print [free] mcells on page N\n");
  fprintf(stderr, "\t[-v] <src> -p page mcell [-c]   print mcell N [chain] on page N\n");
  fprintf(stderr, "\t[-v] <src> [-a]                 print summary [all] of mcells\n");
  fprintf(stderr, "\t[-v] <fs> [file] [-c]           print primary [chain] mcells [of file]\n");
  fprintf(stderr, "\t[-v] <src> -L b=block [-c]      print [chain] mcell(s) mapping block N\n");
  fprintf(stderr, "\t[-v] <src> -L t=tag [-c]        print [chain] mcell(s) mapping tag N\n");
  fprintf(stderr, "\t[-v] <vol> -l                   print deferred delete list (bmt only)\n");
  fprintf(stderr, "\t[-v] <vol> -b block [-n]        print [free] mcells starting at block N\n");
  fprintf(stderr, "\t[-v] <vol> -b block mcell [-c]  print [chain] mcell N at block N\n");
  fprintf(stderr, "\t[-v] <src> <snap> [file] [-c]   print snapshot [file] mcell [chain]\n");
  fprintf(stderr, "\t[-v] <src> <snap> -L t=tag      print snapshot file having tag\n");
  fprintf(stderr, "\t<vol> -d dump_file              dump BMT/RBMT to a file\n\n");
  fprintf(stderr, "\n");

  if (operands)
    {
      operand_usage();
    }

} /* bmt_usage */

static void
log_usage(int operands)
{
  fprintf(stderr, "%s log\n", prog);
  fprintf(stderr, "\t[-v | -T] <any> [-a]                   print summary [of all recs]\n");
  fprintf(stderr, "\t[-v | -T] <any> -R                     print active recs in log\n");
  fprintf(stderr, "\t[-v | -T] <any> -p page [rec_off [-c]] print page [rec_off [w/ subs]]\n");
  fprintf(stderr, "\t[-v | -T] <any> {-s | -e} [-c]         print start/end rec [w/ subs]\n");
  fprintf(stderr, "\t[-v | -T] <any> {-s | -e} pg_off [rec_off [-c]] print start/end rec\n");
  fprintf(stderr, "\t                                                at page [rec] offets\n");
  fprintf(stderr, "\t[-v | -T] <vol> -b block               print log w/ block N as start\n");
  fprintf(stderr, "\t{<fs> | <vol>} -d dump_file            dump log to a file\n");
  fprintf(stderr, "\n");

  if (operands)
    {
      operand_usage();
    }

} /* log_usage */

static void
sbm_usage(int operands)
{
  fprintf(stderr, "%s sbm\n", prog);
  fprintf(stderr, "\t[-C] <fs>                print summary all volumes [verify checksums]\n");
  fprintf(stderr, "\t[-C] <vol>               print summary volume N [verify checksums]\n");
  fprintf(stderr, "\t<src> [-a]               print summary [all pages]\n");
  fprintf(stderr, "\t<src> -p page [index]    print page [page relative indexed entry]\n");
  fprintf(stderr, "\t<src> -i index           print one SBM map entry at index #\n");
  fprintf(stderr, "\t<src> -B block           print the SBM entry that maps block N\n");
  fprintf(stderr, "\t<vol> -b block           print SBM with block N as starting block\n");
  fprintf(stderr, "\t<vol> -d dump_file       dump SBM to a file\n");
  fprintf(stderr, "\n");

  if (operands)
    {
      operand_usage();
    }

} /* sbm_usage */

static void
tag_usage(int operands)
{
  fprintf(stderr, "%s {tag | rtag}\n", prog);
  fprintf(stderr, "\t[-v] {<fs> | dump_file} [-a]     print tag summary [all pages]\n");
  fprintf(stderr, "\t[-v] {<fs> | dump_file} -p page  print page summary\n");
  fprintf(stderr, "\t[-v] <vol> -b block              print a block as a tag page\n");
  fprintf(stderr, "\t[-v] <fs> -d dump_file           dump vol info to a file\n");
  fprintf(stderr, "\n%s tag cmd only:\n", prog);

  fprintf(stderr, "\t[-v] <fs> <file>            print tag for file\n");
  fprintf(stderr, "\t[-v] <any> <snap> [-a]      print snapshot tag summary [all pgs]\n");
  fprintf(stderr, "\t[-v] <any> <snap> -p page   print snapshot tag page\n");
  fprintf(stderr, "\t[-v] <any> <snap> filename  print tag entry for file\n");

  fprintf(stderr, "\n");

  if (operands)
    {
      operand_usage();
    }

} /* tag_usage */

static void
file_usage(int operands)
{
  fprintf(stderr, "%s file\n", prog);
  fprintf(stderr, "\t{<fs> | dump} filename -a [-D]       print all FOBs [as a dir]\n"); 
  fprintf(stderr, "\t{<fs> | dump} filename [-o FOB] [-D] print a FOB [N] [as a dir]\n"); 
  fprintf(stderr, "\t<vol> -b block                       print a block from the volume\n");
  fprintf(stderr, "\t<fs> filename dump_file              dump to a file\n");
  fprintf(stderr, "\n");

  if (operands)
    {
      operand_usage();
    }

} /* file_usage */

static void
operand_usage()
{
  fprintf(stderr, "<fs> = [-r] filesys_name\n");
  fprintf(stderr, "<vol> = { special_file | <fs> vol_num}\n");
  fprintf(stderr, "<src> = {<vol> | -f dump_file}\n");
  fprintf(stderr, "<snap> = {-S snap_name | -t snap_tag}\n");
  fprintf(stderr, "<any> = {<fs> | <vol> | [-f] dump_file}\n");

} /* operand_usage */


