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
 * @(#)$RCSfile: bs_freeze.h,v $ $Revision: 1.1.6.1 $ (DEC) $Date: 2001/09/19 15:13:07 $
*/

#ifndef _BS_FREEZE_H_
#define _BS_FREEZE_H_

/*
 *  Data structures
 */
 
typedef struct {
    bfDomainIdT        frzmDomainId;   /* domain                  */
    u_long             frzmFlags;      /* flags                   */
    int                frzmTimeout;    /* timeout value           */
    struct mount      *frzmMP;         /* mount point frozen      */
    int                frzmStatus;     /* status return code      */
    void              *frzmLink;
} advfsFreezeMsgT;

typedef struct {
    bfDomainIdT        frziDomainId;   /* domain                  */
    u_long             frziFlags;      /* flags                   */
    int                frziTimeout;    /* timeout value           */
    struct mount      *frziMP;         /* mount point frozen      */    
    int                frziStatus;     /* status return code      */
    void              *frziLink;       /* list of frozen domains  */
    ftxHT              frziFtxH;       /* transaction id          */
}advfsFreezeInfoT;

 
#endif /* _BS_FREEZE_H_ */
