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
 * Copyright (C) 2004 Hewlett-Packard Company
 */

/*
 * Include files
 */

#include <sys/stat.h>			/* for S_ISBLK macro */
#include <sys/buf.h>
#include <io/gio.h>
#include <sys/uio.h>
#include <sys/mknod.h>
#include <sys/user.h>
#include <sys/sem_alpha.h>
#include <advfs/advfs_syscalls.h>

/*
 * External variables
 */

/*
 * Forward declarations
 */

static int advfsdev_open __((dev_t, int, intptr_t, int));
static int advfsdev_close __((dev_t, int, int));
static int advfsdev_char_ioctl __((dev_t, int, caddr_t, int));
static int advfsdev_read  __((dev_t dev, struct uio *uio));
static int advfsdev_write  __((dev_t dev, struct uio *uio));
static int advfsdev_strategy __((struct buf *));

/*
 * Local variables
 */

static drv_ops_t advfsdev_ops = {
    advfsdev_open,			/* d_open */
    advfsdev_close,			/* d_close */
    advfsdev_strategy,			/* d_strategy */
    NULL,				/* d_dump */
    NULL,				/* d_psize */
    NULL,				/* reserved0 */
    advfsdev_read,			/* d_read */
    advfsdev_write,			/* d_write */
    advfsdev_char_ioctl,		/* d_ioctl */
    NULL,				/* d_select */
    NULL,				/* d_option1 */
    NULL,				/* pfilter */
    NULL,				/* reserved1 */
    NULL,				/* reserved2 */
    NULL,				/* d_aio_ops */
    C_MGR_IS_MP				/* d_flags */
};


static drv_info_t advfsdev_info = {
    "advfsdev",
    "pseudo",
    DRV_BLOCK | DRV_CHAR | DRV_MP_SAFE,
    ADVFSDEV_MAJOR,
    ADVFSDEV_MAJOR,
    NULL,
    NULL,
    NULL
};

/*
 * Global variables
 */

/*
 *  The initial entry point for advfsdev 
 */
void
advfsdev_link(void)
{

    (void) install_driver (&advfsdev_info, &advfsdev_ops);

    return;
}


static int
advfsdev_open(dev_t dev, int oflags, intptr_t dummy, int mode)
{
    dev_t ldevt;
    int status = 0;
    sema_t *save = NULL;

    return status;
}

static int 
advfsdev_close(dev_t dev, int flags, int mode)
{
    dev_t ldevt;
    int status = 0;
    sema_t *save = NULL;

    return status;
}

static int
advfsdev_char_ioctl(dev_t dev, int cmd, caddr_t data, int flags)
{
    dev_t ldevt_char;
    int status = EOPNOTSUPP;
    sema_t *save = NULL;

    return status;
}

static int 
advfsdev_read(dev_t dev, struct uio *uio)
{

    int status = EOPNOTSUPP;

    return status;
}

static int 
advfsdev_write(dev_t dev, struct uio *uio)
{

    int status = EOPNOTSUPP;

    return status;
}

static int
advfsdev_strategy(struct buf *bp)
{
    int status = EOPNOTSUPP;

    bp->b_flags |= B_ERROR;
    biodone (bp);

    return status;
}

