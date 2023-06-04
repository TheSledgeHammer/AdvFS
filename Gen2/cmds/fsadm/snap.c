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
 */

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <advfs/advfs_syscalls.h>
#include "common.h"

nl_catd catd;

#define MSG_SP(n,s) catgets(catd,MS_FSADM_SNAP,n,s)
#define MSG_CM(n,s) catgets(catd,MS_FSADM_COMMON,n,s)

static char *Prog = "fsadm snap";
int Vflag = 0;

int snap_create(arg_infoT *, char *, bf_snap_flags_t);

void
snap_usage(void)
{
    usage(MSG_SP(SNAP_USAGE, "%s [-V] parent_filesystem snapshot_name\n"), 
            Prog);
}

int
snap_main(int argc, char *argv[])
{
    int c;
    arg_infoT *infop;
    int ret;
    char *cp;

    /* Must be privileged to run */
    check_root( Prog );

    bf_snap_flags_t snap_flags = SF_NO_FLAGS;

    snap_flags |= SF_RDONLY;

    while ((c = getopt(argc, argv, "V")) != EOF) {
        switch (c) {
            case 'V':
                Vflag++;
                break;
            default:
                snap_usage();
                return 1;
        }
    }

    if (Vflag > 1) {
        snap_usage();
        return 1;
    }

    if ((argc - 2 - optind) != 0) {
        snap_usage();
        return 1;
    }

    infop = process_advfs_arg(argv[optind]);
    if (infop == NULL) {
        fprintf(stderr, 
                MSG_CM(COMMON_FSNAME_ERR, 
                    "%s: Error processing specified name %s\n"),
                Prog, argv[optind]);
        return 1;
    }

    if (cp = strpbrk(argv[optind+1], "/# :*?\t\n\f\r\v")) {
        fprintf(stderr, MSG_SP(SNAP_INVCHAR, 
                    "%s: invalid character '%c' in snapshot name '%s'\n"),
                Prog, *cp, argv[optind+1]);
        return 1;
    }

    if (infop->is_multivol != 1) {
        fprintf(stderr, MSG_SP(SNAP_NOBLK, 
                    "%s: Cannot snap an unnamed file system\n"), Prog);
        return 1;
    }

    if (Vflag) {
        printf("%s\n", Voption(argc, argv));
        return 0;
    }

    ret = snap_create(infop, argv[optind+1], snap_flags);

    free(infop);
    return ret;
}

int 
snap_create(arg_infoT *infop, char *snap_name, bf_snap_flags_t flags)
{
    struct stat stbuf;
    bfSetIdT setId;
    adv_status_t sts;

    /*  create the AdvFS block device */
    sts = advfs_make_blockdev(infop->fspath, snap_name, infop->tcr_shared, 0);

    if (sts) {
        fprintf(stderr, MSG_SP(SNAP_ERRDEV, 
                    "%s: Unable to create Advfs device %s/%s. %s\n"),
                Prog, infop->fspath, snap_name, BSERRMSG(sts));
        free(infop);
        return 1;
    }

    sts = advfs_lib_create_snapset(infop->fsname,
            infop->fset,
            infop->fsname,
            snap_name,
            flags,
            &setId);

    if (sts) {
        char unlink_path[MAXPATHLEN];

        switch (sts) {
            case E_DUPLICATE_SET:
                fprintf(stderr, 
                        MSG_SP(SNAP_DUPSNAP,"%s: %s already exists.\n"), 
                        Prog, snap_name);
                break;
            default:
                fprintf(stderr, 
                        MSG_SP(SNAP_ERRCREATE, 
                            "%s: Error creating snapshot %s. %s\n"), 
                        Prog, snap_name, BSERRMSG(sts));
                break;
        }
        snprintf(unlink_path, MAXPATHLEN, "%s/%s", infop->fspath, snap_name);
        unlink(unlink_path);
        free(infop);
        return 1;
    }

    free(infop);
    return 0;
}
