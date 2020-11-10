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
 *      Routines for i/o with remote rmt server.
 *
 * Date:
 *
 *      June 12, 1996
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vdumprmt.c,v $ $Revision: 1.1.18.3 $ (DEC) $Date: 2004/04/21 06:06:50 $"

/*
 * vdumprmt.c
 *
 */

#include	"util.h"
#include	<sys/types.h>
#include	<sys/ioctl.h>
#include	<sys/mtio.h>
#include	<sys/socket.h>
#include	<netinet/in.h>
#include	<pwd.h>
#include	<netdb.h>
#include	"libvdump_msg.h"

#define	TS_CLOSED	0
#define	TS_OPEN		1

/*
 * Path of remote daemon (rmt)
 *
 * There is an incompatability with the executable path for the rmt
 * command between OSF/1 and other vendors including SUN and ULTRIX.  The
 * rmt command is found in /usr/sbin/rmt for OSF/1 and /etc/rmt or
 * /usr/etc/rmt for other platforms.
 */

#define REMOTE_CMD      "uname -s;sh -c 'PATH=/usr/sbin:/etc:/usr/etc;rmt'"
#ifdef  DECOSF
#define SERVER_PATH     "sh -c 'PATH=/usr/sbin:/etc:/usr/etc;rmt'"
#else
#define SERVER_PATH     "/usr/sbin/rmt"         /* path of the remote daemon */
#endif  /* DECOSF */

static char		rmt_getb();
static int		rmt_call();
static int		rmt_reply();
static struct mtget    *rmt_status();
static void		rmt_abort();
static void		rmt_error();
static void		rmt_gets();
static void		rmt_lostconn();
static void		rmt_perror();

static int		rmt_state = TS_CLOSED;
static int		rmt_socket_fd;


static void
rmt_lostconn()
{

	rmt_abort(catgets(comcatd, RVDUMP, LOSTCON, "Lost connection to remote peer\n"));

	/* NOTREACHED */
}

static void
rmt_error()
{
	rmt_abort(catgets(comcatd, RVDUMP, RERR, "Communication error in connection to remote peer\n"));

	/* NOTREACHED */
}

static void
rmt_perror(where)
	char	       *where;
{

	perror(where);

}

static void
rmt_abort(msg)
        char    	*msg;
{

	abort_prog(msg);

	/* NOTREACHED */
}

/* The arg to rmthost is a pointer to the pointer to the remote host name. */
/* This is done so that rcmd can change the pointer to the standard name of */
/* the remote host. */

void
rmthost(rmt_peer_name, rmt_user_name)
        char          **rmt_peer_name;
        char          *rmt_user_name;
{
        void            rmt_lostconn();
        int             size;

        struct servent *sp;
        struct passwd  *pw;
        char           *local_user = NULL;
        char           *remote_user = NULL;
        char           remote_host2[MAXHOSTNAMELEN];
        char           *remote_host2_ptr = NULL;

        (void) signal(SIGPIPE, rmt_lostconn);

        strcpy(remote_host2,*rmt_peer_name);  /* save peer name for reuse in second rcmd call, */
        remote_host2_ptr = remote_host2;      /* first call trashes rmt_peer_name         */

        /* get the entry of the shell/tcp service so we know what port */
        /* number it is on */

        sp = getservbyname("shell", "tcp");
        if (sp == NULL)
        {
		rmt_abort(catgets(comcatd, RVDUMP, UNKSERV, 
                          "rvdump: shell/tcp: unknown service\n"));

                /* NOTREACHED */
        }

        /* get the uid name string of the current process so we can */
        /* rcmd() on the remote machine under that name */

        pw = getpwuid(getuid());
        if (pw != NULL && pw->pw_name != NULL)
                local_user = pw->pw_name;
        else
                local_user = "root";

        if (rmt_user_name)
                remote_user = rmt_user_name;
        else
                remote_user = local_user;

        /* establish a connection to a rmt command on the remote machine */
        /* check for fallback to simple rmt command (no ULTRIX/OSF detection) */
        fprintf(stderr, catgets(comcatd, RVDUMP, INFO1,
                   "Trying to establish connection to host %s using username %s...\n"),
                   *rmt_peer_name,remote_user);

        if (getenv("OSF_RDUMP_SIMP_RCMD") == NULL){
          /* use new complex command, allowing detection */
          rmt_socket_fd = rcmd_af(rmt_peer_name, sp->s_port, local_user, 
				remote_user, REMOTE_CMD, (int *) 0, AF_INET6);
          if (rmt_socket_fd < 0)
          {
            /* if uname leading rcmd fails go back to old cmd */
            rmt_socket_fd = rcmd_af(&remote_host2_ptr, sp->s_port, local_user, 
				remote_user, SERVER_PATH, (int *)0, AF_INET6);
            if (rmt_socket_fd < 0){
                rmt_perror("rmthost(): rcmd()");
                rmt_abort(catgets(comcatd, RVDUMP, NOCON,
                          "Cannot establish connection to remote rmt command\nPossibly allow root connection via .rhosts on remote host. (See rmt(8) manpage)"));

                /* NOTREACHED */
            }
          }else{
                rmt_gets(os_name,OSNAMEBUFSIZE);
                /* if it succeeds it returns */
                if((strncmp(os_name,"ULTRIX",6)==0)||
                   (strncmp(os_name,"OSF1",4)==0)){
                        magtape_ioctl_safe = 1;
                }
          }
        }
        else
        /* drop back to simplest rcmd form */
        {
          rmt_socket_fd = rcmd_af(rmt_peer_name, sp->s_port, local_user,
				remote_user, "/etc/rmt", 0, AF_INET6);
          if (rmt_socket_fd < 0)
          {
            fprintf(stderr, catgets(comcatd, RVDUMP, NOCON1, 
                   "Cannot establish connection to remote %s command, trying %s\n"),
                   "/etc/rmt","/usr/etc/rmt");
            rmt_socket_fd = rcmd_af(&remote_host2_ptr, sp->s_port, local_user,
				remote_user, "/usr/etc/rmt\n", 0, AF_INET6);
            if (rmt_socket_fd < 0)
            {
                fprintf(stderr, catgets(comcatd, RVDUMP, NOCON1, 
                       "Cannot establish connection to remote %s command, trying %s\n"),
                        "/etc/rmt","/usr/etc/rmt");
                rmt_socket_fd = rcmd_af(rmt_peer_name, sp->s_port, local_user,
				   remote_user, "/usr/sbin/rmt\n", 0, AF_INET6);
                if (rmt_socket_fd < 0){
                    rmt_abort("Cannot establish connection to remote \"/usr/etc/rmt\" command\n");
                }
                /* NOTREACHED */
            }
          }
        }

    return ;

}

int
rmtopen(tape_device, flags)
	char	       *tape_device;
	int		flags;
{
	char		buf[MAXPATHLEN+64];
	int		open_value;

	(void) sprintf(buf, "O%s\n%d\n", tape_device, flags);
	open_value = rmt_call("open()", buf);
	if (open_value >= 0)
	{
		rmt_state = TS_OPEN;
	}
	return(open_value);
}

void
rmtclose()
{
	if (rmt_state != TS_OPEN)
	{
		return;
	}
	(void) rmt_call("close()", "C\n");
	rmt_state = TS_CLOSED;
}

int
rmtread(buf, count)
	char	       *buf;
	int		count;
{
	char		line[256];
	int		n, i, cc;

	(void) sprintf(line, "R%d\n", count);
	n = rmt_call("read()", line);
	if (n < 0)
	{
		errno = n;
		return(-1);
	}
	for (i = 0; i < n; i += cc)
	{
		cc = read(rmt_socket_fd, buf + i, n - i);
		if (cc <= 0)
		{
                        fprintf(stderr, catgets(comcatd, RVDUMP, RRBN, 
                                "Cannot read buffer from socket\n"));
			rmt_perror("rmtread(): read()");
			rmt_error();

			/* NOTREACHED */
		}
	}
	return(n);
}

int
rmtwrite(buf, count)
	char	       *buf;
	int		count;
{
	char		line[256];

	(void) sprintf(line, "W%d\n", count);
	if (write(rmt_socket_fd, line, strlen(line)) != strlen(line))
	{
                fprintf(stderr, catgets(comcatd, RVDUMP, RWWLN, 
                    "Cannot write command line to socket\n"));
		rmt_perror("rmtwrite(): write()");
		rmt_error();

		/* NOTREACHED */
	}
	if (write(rmt_socket_fd, buf, count) != count)
	{
                fprintf(stderr, catgets(comcatd, RVDUMP, RWBN, 
                    "Cannot write buffer to socket\n"));
		rmt_perror("rmtwrite(): write()");
		rmt_error();

		/* NOTREACHED */
	}
	return(rmt_reply("write()"));
}

#ifdef NULL
/* left in here in case it is needed someday */
int
rmtseek(offset, pos)
	int		offset, pos;
{
	char		line[256];

	(void) sprintf(line, "L%d\n%d\n", offset, pos);
	return(rmt_call("seek()", line));
}


static struct mtget   *
rmt_status()
{
	register int	i;
	register char  *cp;
	static struct mtget	mts;

	if (rmt_state != TS_OPEN)
	{
		return(NULL);
	}
	(void) rmt_call("status()", "S\n");
	for (i = 0, cp = (char *) &mts; i < sizeof(struct mtget); ++i)
	{
		*cp++ = rmt_getb();
	}
	return(&mts);
}
#endif
/* 
 * Perform DEVIOCGET ioctl to get Remote Device Status from ULTRIX
 * (Contains handler for OSF sized devget in case we decide to support
 * it). If it's ULTRIX we map the string, and move each offset into an 
 * OSF devget structure.
 */
struct  ult_devget ultrix_gen_info;
struct	devget gen_info;
struct devget *
rmtgenioctl()
{
        register int i,n;
        register char *cp;

        if (rmt_state != TS_OPEN)
                return (0);
        n = rmt_call("general status", "D\n");
        switch (n) {
           case sizeof(ultrix_gen_info): /* For ULTRIX sized device structure */
                /* Easy case; just read normally */
                for (i = 0, cp = (char *)&ultrix_gen_info; i < n; i++)
                        *cp++ = rmt_getb();
                /* Map the ULTRIX devget struct components into 
		 * OSF style devget struct 
		 */
                gen_info.category       = ultrix_gen_info.category;
                gen_info.bus            = ultrix_gen_info.bus;
		for (i = 0; i < DEV_SIZE; i++)
                  gen_info.interface[i] = ultrix_gen_info.interface[i];
		for (i = 0; i < DEV_SIZE; i++)
                  gen_info.device[i] = ultrix_gen_info.device[i];
                gen_info.adpt_num       = ultrix_gen_info.adpt_num;
                gen_info.nexus_num      = ultrix_gen_info.nexus_num;
                gen_info.bus_num        = ultrix_gen_info.bus_num;
                gen_info.ctlr_num       = ultrix_gen_info.ctlr_num;
                gen_info.rctlr_num      = ultrix_gen_info.rctlr_num;
                gen_info.slave_num      = ultrix_gen_info.slave_num;
		for (i = 0; i < DEV_SIZE; i++)
                  gen_info.dev_name[i] = ultrix_gen_info.dev_name[i];
                gen_info.unit_num       = ultrix_gen_info.unit_num;
                gen_info.soft_count     = ultrix_gen_info.soft_count;
                gen_info.hard_count     = ultrix_gen_info.hard_count;
                gen_info.stat           = (long)ultrix_gen_info.stat;
                gen_info.category_stat  = (long)ultrix_gen_info.category_stat;

                break;

           default:
                /* If the size returned is not what we expect
                   return as if an error occured */
                fprintf(stderr, catgets(comcatd, RVDUMP, RMTGENSIZ, 
                    "RMTGENIOCTL: unexpected size from remote system\n"));
                return (NULL);
                break;
        }

        /* If we didn't return the null we need to return gen_info */
        return (&gen_info);
}

int
rmtioctl(cmd, count)
	int		cmd, count;
{
	char		buf[256];

	if (count < 0)
	{
		return(-1);
	}
	(void) sprintf(buf, "I%d\n%d\n", cmd, count);
	return(rmt_call("ioctl()", buf));
}

static int
rmt_call(cmd, buf)
	char	       *cmd, *buf;
{
	int		len_to_write;

	len_to_write = strlen(buf);
	if (write(rmt_socket_fd, buf, len_to_write) != len_to_write)
	{
                fprintf(stderr, catgets(comcatd, RVDUMP, RWBN, 
                    "Cannot write buffer to socket\n"));
		rmt_perror("rmt_call(): write()");
		rmt_error();

		/* NOTREACHED */
	}
	return(rmt_reply(cmd));
}

static int
rmt_reply(cmd)
	char	       *cmd;
{
	char		code[256], emsg[BUFSIZ];

	/* ignore attention records */

	for(; ; )
	{
		rmt_gets(code, sizeof(code));
		if (code[0] != 'B')
		{
			break;
		}
	}

	if (code[0] == 'E' || code[0] == 'F')
	{
		rmt_gets(emsg, sizeof(emsg));

                /* skip E25 since its an error that is expected when device on remote system
                 * is a file and not a tape.  It occurs when rmtioctl is trying to detect
                 * whether the remote device specified is a file or storage device like a tape.
		 * Also suppress the error E6 emerging from a magnetic tape operation being
                 * performed on a non-tape device.
                 *
                 * Report all other errors as they are except the error
                 * "No such file or directory".  This error will occur when the rmt server on
                 * the remote system is an older version that will not handle 1024 names and
                 * only handles 64 length pathnames.  We will backport this 1024 into rmt for
                 * new releases but cannot garantee it will make it
                 * into previous releases.  For this reason the error message for errno==2,
                 * "No such file or directory", will have appended to it the text "or the
                 * filepath could be too long" in order to alert admins to the real reason for
                 * the failure.
                 */
                if(!((strncmp(cmd, "ioctl()", 7) == 0) && ((strcmp(code,"E25")==0) || (strcmp(code,"E6")==0)))) {
                        if (strcmp(code,"E2")==0)
                   		fprintf(stderr, "remote error from %s is: %s \nor the filepath could be too \
long for remote rmt version\n(older versions only allowed 64 characters): errno %s\n",
                           cmd, emsg, code + 1);
                	else
                  		fprintf(stderr, "remote error from %s is: %s: errno %s\n", cmd, emsg, code + 1);
		}

                rmt_state = TS_CLOSED;
		errno = atoi(code+1);
		return(-1);
	}

	if (code[0] != 'A')
	{
                fprintf(stderr, catgets(comcatd, RVDUMP, PROTO, 
                    "rmt_reply(): Protocol to remote tape server botched (code %s)\n"), code);
		rmt_error();

		/* NOTREACHED */
	}
	return(atoi(&code[1]));
}

static char
rmt_getb()
{
	char		c;

	if (read(rmt_socket_fd, &c, 1) != 1)
	{
                fprintf(stderr, catgets(comcatd, RVDUMP, RRSCN, 
                    "Cannot read single character from socket\n"));
		rmt_perror("rmt_getb(): read()");
		rmt_error();

		/* NOTREACHED */
	}
	return(c);
}

static void
rmt_gets(cp, len)
	char	       *cp;
	int		len;
{
	while (len > 1)
	{
		*cp = rmt_getb();
		if (*cp == '\n')
		{
			*cp = '\0';
			return;
		}
		++cp;
		--len;
	}
        fprintf(stderr, catgets(comcatd, RVDUMP, PROTO1, 
            "rmt_gets(): Protocol to remote tape server botched\n"));
	rmt_error();

	/* NOTREACHED */
}
