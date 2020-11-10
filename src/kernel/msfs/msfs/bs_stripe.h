/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
 * @(#)$RCSfile: bs_stripe.h,v $ $Revision: 1.1.7.4 $ (DEC) $Date: 1998/12/04 23:39:55 $
 */

#ifndef _BS_STRIPE_H_
#define _BS_STRIPE_H_

/*
 * This macro determines which stripe extent map maps the specified bitfile
 * relative page.
 */
#define BFPAGE_TO_MAP(pageOffset, segmentCnt, segmentSize) \
  ( ( (pageOffset) % ((segmentCnt) * (segmentSize)) ) / (segmentSize) )

/*
 * This macro returns the index of the next stripe extent map.
 */
#define NEXT_MAP(mapIndex, segmentCnt)  (((mapIndex) + 1) % (segmentCnt))

/*
 * This macro converts a bitfile relative page to an extent map
 * relative page.  Before it does the conversion, it must determine
 * which stripe extent map maps the specified bitfile page.
 */
#define BFPAGE_TO_XMPAGE(pageOffset, segmentCnt, segmentSize) \
  ( ( ( (pageOffset) / ((segmentCnt) * (segmentSize)) ) * (segmentSize) ) + \
   ((pageOffset) % (segmentSize)) )

/*
 * This macro converts an extent map relative page to a bitfile relative
 * page.
 */
#define XMPAGE_TO_BFPAGE(mapIndex, pageOffset, segmentCnt, segmentSize) \
  ( ((mapIndex) * (segmentSize)) + \
   ( ((pageOffset) / (segmentSize)) * ((segmentCnt) * (segmentSize)) ) + \
   ((pageOffset) % (segmentSize)) )

/*
 * This macro determines the bitfile page offset of the next stripe
 * segment's first page.
 */
#define NEXT_SEGMENT_PAGE(pageOffset, segmentSize) \
  ( (pageOffset) + ( (segmentSize) - ((pageOffset) % (segmentSize)) ) )


void
str_register_stripe_agent (
                           );

statusT
str_stripe (
            bfAccessT *bfAccess,  /* in */
            uint32T segmentCnt,   /* in */
            uint32T segmentSize,  /* in */
            long xid              /* in */
            );

statusT
str_stripe_clone (
                  bfAccessT *bfap,      /* in - clone access */
                  bsInMemXtntT *oXtntp, /* in - original xtnt map */
                  ftxHT pftxH           /* in - parent ftxH */
                  );

statusT
str_create_stripe_hdr (
                       uint32T segmentCnt,          /* in */
                       uint32T segmentSize,         /* in */
                       bsStripeHdrT **newStripeHdr  /* out */
                       );

void
str_delete_stripe_hdr (
                       bsStripeHdrT *stripeHdr  /* in */
                       );

void
str_calc_page_alloc (
                     uint32T bfPageOffset,    /* in */
                     uint32T bfPageCnt,       /* in */
                     bsStripeHdrT *stripeHdr  /* in */
                     );

statusT
str_create_bf_rel_xtnt_map (
                            bsStripeHdrT *stripeHdr,       /* in */
                            uint32T mapIndex,              /* in */
                            bsInMemXtntMapT *xmXtntMap,    /* in */
                            bsXtntMapTypeT bsXtntMapType,  /* in */
                            bsInMemXtntMapT **bfXtntMap    /* out */
                            );

#endif  /* _BS_STRIPE_H_ */
