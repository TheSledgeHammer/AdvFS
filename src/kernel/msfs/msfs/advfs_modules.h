/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
 */
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
 * @(#)$RCSfile: advfs_modules.h,v $ $Revision: 1.1.15.1 $ (DEC) $Date: 2001/12/14 16:50:45 $
 */

/*
 * NOTE: This list must match, in order, the list in bs/ms_privates.c
 */

typedef enum {
    NO_MODULE,
    BS_ACCESS,                  /*  1 */
    BS_BITFILE_SETS,            /*  2 */
    BS_BMT_UTIL,                /*  3 */
    BS_BUFFER2,                 /*  4 */
    BS_COPY,                    /*  5 */
    BS_CREATE,                  /*  6 */
    BS_DELETE,                  /*  7 */
    BS_DOMAIN,                  /*  8 */
    BS_ERRLST,                  /*  9 */
    BS_EXTENTS,                 /* 10 */
    BS_INIT,                    /* 11 */
    BS_INMEM_MAP,               /* 12 */
    BS_MIGRATE,                 /* 13 */
    BS_MISC,                    /* 14 */
    BS_MSG_QUEUE,               /* 15 */
    BS_PARAMS,                  /* 16 */
    BS_QIO,                     /* 17 */
    BS_SBM,                     /* 18 */
    BS_SERVICE_CLASS,           /* 19 */
    BS_STG,                     /* 20 */
    BS_STRIPE,                  /* 21 */
    BS_TAGDIR,                  /* 22 */
    FTX_RECOVERY,               /* 23 */
    FTX_ROUTINES,               /* 24 */
    MS_GENERIC_LOCKS,           /* 25 */
    MS_LOGGER,                  /* 26 */
    MS_MODE,                    /* 27 */
    MS_PRIVATESC,               /* 28 */
    MS_PUBLICC,                 /* 29 */
    FS_CREATE,                  /* 30 */
    FS_DIR_INIT,                /* 31 */
    FS_DIR_LOOKUP,              /* 32 */
    FS_FILE_SETS,               /* 33 */
    FS_QUOTA,                   /* 34 */
    FS_READ_WRITE,              /* 35 */
    MSFS_CONFIG,                /* 36 */
    MSFS_IO,                    /* 37 */
    MSFS_LOOKUP,                /* 38 */
    MSFS_MISC,                  /* 39 */
    MSFS_SYSCALLS,              /* 40 */
    MSFS_VFSOPS,                /* 41 */
    MSFS_VNOPS,                 /* 42 */
    MSFS_PROPLIST,              /* 43 */
    BS_INDEX,                   /* 44 */
    VFAST                       /* 45 */
} advfsModulesT;
