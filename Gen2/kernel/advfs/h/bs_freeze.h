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
 *      AdvFS - bs_freeze.h
 *
 * Abstract:
 *
 *      This is the include file for bs_freeze.c
 *
 */

#ifndef _BS_FREEZE_H_
#define _BS_FREEZE_H_

#include <advfs/ms_generic_locks.h>

/*
 *  Data structures
 */
 
typedef struct {
    bfDomainIdT        frzmDomainId;   /* domain                  */
    uint64_t           frzmFlags;      /* flags                   */
    int64_t            frzmTimeout;    /* timeout value           */
    fileSetNodeT      *frzmFSNP;       /* fileSetNode frozen      */
    int32_t            frzmStatus;     /* status return code      */
    cv_t               frzmStatusCV;
    void              *frzmLink;
} advfsFreezeMsgT;

extern advfsFreezeMsgT  *AdvfsFreezeMsgs;
extern mutexT            AdvfsFreezeMsgsLock;
extern cv_t              AdvfsFreezeMsgsCV;
extern int64_t           AdvfsFreezefsTimeout;
/*
 *
 */
typedef struct {
    bfDomainIdT        frziDomainId;   /* domain                  */
    uint64_t           frziFlags;      /* flags                   */
    int64_t            frziTimeout;    /* timeout value           */
    fileSetNodeT      *frziFSNP;       /* fileSetNode frozen      */    
    int32_t            frziStatus;     /* status return code      */
    void              *frziLink;       /* list of frozen domains  */
    ftxHT              frziFtxH;       /* transaction id          */
}advfsFreezeInfoT;

 
#endif
