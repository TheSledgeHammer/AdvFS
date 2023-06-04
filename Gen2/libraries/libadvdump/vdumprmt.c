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
 * Facility:
 *
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Routines for i/o with remote rmt server.
 *
 */

/*
 * vdumprmt.c
 *
 */

#include	<sys/types.h>
#include	<sys/ioctl.h>
#include	<sys/mtio.h>
#include	<netinet/in.h>
#include	<pwd.h>
#include	<netdb.h>
#include	"libvdump_msg.h"
#include	"util.h"

#define	TS_CLOSED	0
#define	TS_OPEN		1

/*
 * Path of remote daemon (rmt)
 *
 * There is an incompatability with the executable path for the rmt
 * command between OSF/1 and other vendors including SUN and ULTRIX.  The
 * rmt command is found in /usr/sbin/rmt for OSF/1 and /etc/rmt or
 * /usr/etc/rmt for other platforms.  HP-UX uses /usr/sbin/rmt.
 */

#define REMOTE_CMD  "/usr/bin/uname -s;sh -c 'PATH=/usr/sbin:/etc:/usr/etc;rmt'"
#define SERVER_PATH "/sh -c 'PATH=/usr/sbin:/etc:/usr/etc;rmt'"

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

extern nl_catd catd;

static void
rmt_lostconn()
{

	rmt_abort(catgets(catd, RVDUMP, LOSTCON, 
            "Lost connection to remote peer\n"));

	/* NOTREACHED */
}

static void
rmt_error()
{
	rmt_abort(catgets(catd, RVDUMP, RERR, 
            "Communication error in connection to remote peer\n"));

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
		rmt_abort(catgets(catd, RVDUMP, UNKSERV, 
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
        fprintf(stderr, catgets(catd, RVDUMP, INFO1,
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
                rmt_abort(catgets(catd, RVDUMP, NOCON,
                          "Cannot establish connection to remote rmt command\nPossibly allow user connection via .rhosts on remote host. (See rmt(8) manpage)"));

                /* NOTREACHED */
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
            fprintf(stderr, catgets(catd, RVDUMP, NOCON1, 
                   "Cannot establish connection to remote %s command, trying %s\n"),
                   "/etc/rmt","/usr/etc/rmt");
            rmt_socket_fd = rcmd_af(&remote_host2_ptr, sp->s_port, local_user,
				remote_user, "/usr/etc/rmt\n", 0, AF_INET6);
            if (rmt_socket_fd < 0)
            {
                fprintf(stderr, catgets(catd, RVDUMP, NOCON1, 
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
                        fprintf(stderr, catgets(catd, RVDUMP, RRBN, 
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
                fprintf(stderr, catgets(catd, RVDUMP, RWWLN, 
                    "Cannot write command line to socket\n"));
		rmt_perror("rmtwrite(): write()");
		rmt_error();

		/* NOTREACHED */
	}
	if (write(rmt_socket_fd, buf, count) != count)
	{
                fprintf(stderr, catgets(catd, RVDUMP, RWBN, 
                    "Cannot write buffer to socket\n"));
		rmt_perror("rmtwrite(): write()");
		rmt_error();

		/* NOTREACHED */
	}
	return(rmt_reply("write()"));
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
                fprintf(stderr, catgets(catd, RVDUMP, RWBN, 
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

    for(; ; ) {
        rmt_gets(code, sizeof(code));
        if (code[0] != 'B') {
            break;
        }
    }

    if (code[0] == 'E' || code[0] == 'F') {
        rmt_gets(emsg, sizeof(emsg));

        /* skip E25 since its an error that is expected when device on remote 
         * system is a file and not a tape.  Skip E6 as it's an expected
         * error when the remote device is a disk device.  It occurs when 
         * rmtioctl is trying to detect whether the remote device specified 
         * is a file or storage device like a tape.  
         *
         * Report all other errors as they are except the error
         * "No such file or directory".  This error will occur when the rmt 
         * server on the remote system is an older version that will not handle
         * 1024 names and only handles 64 length pathnames.  For this 
         * reason the error message for errno==2, "No such file or directory", 
         * will have appended to it the text "or the filepath could be too long"
         * in order to alert admins to the real reason for the failure.
         */
        if(!((strncmp(cmd, "ioctl()", 7) == 0) && ((strcmp(code,"E25") ==0) || (strcmp(code,"E6")==0)))) {

            if(strncmp(code,"E2",2)==0)
                fprintf(stderr, "remote error from %s is: %s \nor the filepath could be too \
long for remote rmt version\n(older versions only allowed 64 characters): errno %s\n",
                           cmd, emsg, code + 1);
            else
                fprintf(stderr, "remote error from %s is: %s: errno %s\n", 
                    cmd, emsg, code + 1);
        }

        rmt_state = TS_CLOSED;
        errno = atoi(code+1);
        return(-1);
    }

    if (code[0] != 'A') {
        fprintf(stderr, catgets(catd, RVDUMP, PROTO, 
            "rmt_reply(): Protocol to remote tape server botched (code %s)\n"),
            code);
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
                fprintf(stderr, catgets(catd, RVDUMP, RRSCN, 
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
        fprintf(stderr, catgets(catd, RVDUMP, PROTO1, 
            "rmt_gets(): Protocol to remote tape server botched\n"));
	rmt_error();

	/* NOTREACHED */
}
