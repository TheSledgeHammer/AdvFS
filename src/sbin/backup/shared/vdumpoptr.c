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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Routines for communicating with backup operator.
 *
 * Date:
 *
 *      June 12, 1996
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vdumpoptr.c,v $ $Revision: 1.1.8.1 $ (DEC) $Date: 2003/01/06 22:45:41 $"

#include	<errno.h>
#include	"util.h"
#include	<utmp.h>
#include	"libvdump_msg.h"

/*
 * Query the operator; This piece of code requires an exact response.
 * It is intended to protect dump aborting by inquisitive people
 * banging on the console terminal to see what is happening which might cause
 * dump to croak, destroying a large number of hours of work.
 *
 * Every 2 minutes we reprint the message, alerting others that dump needs
 * attention.
 */

#define X_FINOK		0	/* normal exit */
#define X_FINBAD	1	/* bad exit */
#define DIALUP_PREFIX	"ttyd"	/* prefix for dialups */

static void		alarmcatch();
static void		pose_question();
static void		sendmes();
static struct fstab    *allocfsent();

static char	       *attn_message;	/* attention message */

char 		*yes_str = "yes";
char 		*no_str = "no";
int             yes_flag = FALSE;
int             no_flag = FALSE;
int             notify_flag = FALSE;       /* notify operators */



int
query(question)
	char	       *question;
{

	char		replybuffer[256];
	int		answer;
	FILE	       *tty_fp;
	int		i;
	char	       *ptr;
	void	      (*save_alarm)();

	if ((tty_fp = fopen("/dev/tty", "r")) == NULL)
	{
		fprintf(stderr,catgets(comcatd, RVDUMP, FOPENTTY, 
                    "Cannot fopen /dev/tty for reading\n"));
		abort_prog("query(): fopen()");
		/* NOTREACHED */
	}

	attn_message = question;

	save_alarm = signal(SIGALRM, alarmcatch);
        pose_question();

	for (;;)
	{
		(void) alarm(2 * MINUTE);

		if (fgets(replybuffer, sizeof(replybuffer) - 1, tty_fp) == NULL && ferror(tty_fp) != 0)
		{
			clearerr(tty_fp);
			continue;
		}

		ptr = (char *) strchr(replybuffer, '\n');
		if (ptr != NULL)
		{
			*ptr = '\0';
		}

		/*
		 * use rpmatch only if both the environment strings are
		 * defined.
		 */

		if (yes_flag == TRUE && no_flag == TRUE)
		{
			i = rpmatch(replybuffer);
		}
		else
		{
			if (strcmp(replybuffer, yes_str) == 0)
			{
				i = 1;
			}
			else if (strcmp(replybuffer, no_str) == 0)
			{
				i = 0;
			}
			else
			{
				i = -1;
			}
		}

		if (i == 1)
		{
			answer = YES;
			break;
		}
		else if (i == 0)
		{
			answer = NO;
			break;
		}

		fprintf(stderr,catgets(comcatd, RVDUMP, YESNO1, 
                    "\"%s\" or \"%s\"?\n\n"), yes_str, no_str);

	}

	/*
	 * Turn off the alarm, and reset the signal to what it was before
	 */

	(void) alarm(0);
	(void) signal(SIGALRM, save_alarm);

	(void) fclose(tty_fp);

	return(answer);
}

/*
 * Alert the console operator, and enable the alarm clock to sleep for 2
 * minutes in case nobody comes to satisfy dump
 */

static void
alarmcatch()
{
	/* if this routine has been called, then the two minute alarm */
	/* has expired and all operators should be notified */
	void alarmcatch();

	if (notify_flag == TRUE)
	{
		broadcast(catgets(comcatd, RVDUMP, NEEDATTB, 
                         "DUMP NEEDS ATTENTION!\7\7\n"));
	}

	pose_question();

}

static void
pose_question()
{

	fprintf(stderr,catgets(comcatd, RVDUMP, NEEDATT1, 
            "\nNEEDS ATTENTION: %s?: (\"%s\" or \"%s\") "), 
                                          attn_message, yes_str, no_str);

}

/*
 * This is from /usr/include/grp.h That defined struct group, which
 * conflicts with the struct group defined in param.h
 */

struct Group
{                               /* see getgrent(3) */
        char           *gr_name;
        char           *gr_passwd;
        int             gr_gid;
        char          **gr_mem;
};

static struct Group    *gp = NULL;

/*
 * We fork a child to do the actual  ing, so that the process control
 * groups are not messed up
 */

void
broadcast(message)
	char	       *message;
{
	struct utmp    *utmp;
	char	      **grp_mem_ptr;
	int		pid;
	int		dummy;

	switch (pid = fork())
	{
	case -1:
		fprintf(stderr,catgets(comcatd, RVDUMP, CNTFK, 
                        "Cannot fork to broadcast message\n"));
		perror("broadcast(): fork()");
		return;

	case 0:
		break;

	default:
		while (wait(&dummy) != pid)
		{
			;
		}
		return;
	}

	setutent();	/* open/rewind utmp */

        errno = 0;
	while (utmp = getutent()) {
	
	    if (utmp->ut_name[0] == '\0')
	    {
		continue;
	    }

	    /*
	     * Do not send messages to operators on dialups
	     */

	    if (strncmp(utmp->ut_line, DIALUP_PREFIX, strlen(DIALUP_PREFIX)) == 0)
	    {
		continue;
	    }

	    for (grp_mem_ptr = gp->gr_mem; *grp_mem_ptr != NULL; ++grp_mem_ptr)
	    {
		if (strncmp(*grp_mem_ptr, utmp->ut_name, sizeof(utmp->ut_name)) != 0)
		{
		    continue;
		}


		sendmes(utmp->ut_line, message);
	    }
	    errno = 0;
	}

	if (errno == ENOENT || errno == EACCES) {
	    fprintf(stderr, catgets(comcatd, RVDUMP, COUTMP,
	            "Cannot open %s for reading\n"), UTMP_FILE);
	    perror("broadcast():");
	    exit(X_FINBAD);
	    /* NOTREACHED */
	}
	
	endutent();		/* close utmp file */

	exit(X_FINOK);		/* the wait in this same routine will catch
				 * this */
	/* NOTREACHED */
}

extern struct tm       *localtime();

static void
sendmes(tty_name, message)
	char	       *tty_name, *message;
{
	time_t		clock;
	struct tm      *localclock;
	char		tty_path[256];
	FILE	       *tty_fp;
	char		buf[BUFSIZ];
	register char  *cp;

	clock = time(NULL);
	localclock = localtime(&clock);

	(void) strcpy(tty_path, "/dev/");
	(void) strcat(tty_path, tty_name);

	if ((tty_fp = fopen(tty_path, "w")) != NULL)
	{
		setbuf(tty_fp, buf);

		(void) fprintf(tty_fp, "\a\a\a\n");
		(void) fprintf(tty_fp, catgets(comcatd, RVDUMP, MFD1, 
                               "Message from the dump program to all operators "));
		(void) fprintf(tty_fp, catgets(comcatd, RVDUMP, MFD2, 
                       "at %d:%02d ... \r\n\n"), localclock->tm_hour, localclock->tm_min);

		for (cp = message; *cp != '\0'; ++cp)
		{
			if (*cp == '\n')
			{
				(void) putc('\r', tty_fp);
			}
			(void) putc(*cp, tty_fp);
		}

		(void) fclose(tty_fp);
	}
}
