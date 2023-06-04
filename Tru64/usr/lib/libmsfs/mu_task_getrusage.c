/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
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
 *      MSFS
 *
 * Abstract:
 *
 *      Defines routines for obtaining resouce usage information
 *      for a task.
 *
 * Functions:
 *
 *      first_function
 *
 * Date:
 *
 *      Fri May 17 11:03:56 1991
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: mu_task_getrusage.c,v $ $Revision: 1.1.6.3 $ (DEC) $Date: 1995/04/28 18:31:08 $";
#endif

#include <stdio.h>

#ifdef _OSF_SOURCE
#include <sys/resource.h>
#include <mach.h>
#endif /* _OSF_SOURCE */

#include "libmsfs_msg.h"

#ifdef _OSF_SOURCE
/*
 * task_getrusage 
 *
 * Finds the resource usage of the task -
 * terminated threads, and also of presently running threads
 * largely based on the w utility and the man pages
 */

void task_getrusage( 
                    struct rusage * ru  /* in */
                    )


{
        thread_array_t            thread_list;
        unsigned int              thread_count;
        thread_basic_info_t       athread_info;
        thread_basic_info_data_t  athread_info_data;
        task_basic_info_data_t    atask_info;
        kern_return_t             ret_value;
        unsigned int              task_infoCnt;
        unsigned int              thread_infoCnt;
        time_value_t              allthreads_utime = {0,0};
        time_value_t              allthreads_stime = {0,0};
        int i;
#ifdef TASK_GETRUSAGE_DEBUG
        double                    total_time, utime, stime;
#endif
	nl_catd catd;

/*
 * first find out past task usage - only includes terminated
 * threads
 */

        task_infoCnt = TASK_BASIC_INFO_COUNT;
        if (task_info(task_self(), TASK_BASIC_INFO, (task_info_t)&atask_info,
                         &task_infoCnt) != KERN_SUCCESS) {
	   catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
           fprintf(stderr,catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE1, "task_threads:task_getrusage task_info call error\n"));
	   (void) catclose(catd);
        }
        allthreads_utime.seconds      = atask_info.user_time.seconds;
        allthreads_utime.microseconds = atask_info.user_time.microseconds;
        allthreads_stime.seconds      = atask_info.system_time.seconds;
        allthreads_stime.microseconds = atask_info.system_time.microseconds;

#ifdef TASK_GETRUSAGE_DEBUG
	catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
        printf(catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE2, "\n debug info begins..\n"));
        utime = (atask_info.user_time.seconds + (double)
                            atask_info.user_time.microseconds / 1000000.0);
        stime = (atask_info.system_time.seconds + (double)
                            atask_info.system_time.microseconds / 1000000.0);
        total_time = utime + stime;
        printf(catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE3, "  CPU Seconds : %10.6f  (Usr: %10.6f,  Sys: %10.6f) TASK\n"),
                  total_time, utime, stime );
	(void) catclose(catd);
#endif /* TASK_GETRUSAGE_DEBUG */

/*
 * find out  present task usage - only includes running
 * threads
 */
        ret_value = task_threads( task_self(), &thread_list, &thread_count);
        if ((ret_value) != KERN_SUCCESS) {
	     catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
             fprintf(stderr,catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE4, "task_threads:task_getrusage\n"));
	     (void) catclose(catd);
        }
        thread_infoCnt = THREAD_BASIC_INFO_COUNT;
        athread_info = &athread_info_data;

        for (i=0 ; i < thread_count; i ++) {
            ret_value = thread_info(thread_list[i], THREAD_BASIC_INFO,
                              (thread_info_t)athread_info, &thread_infoCnt);
            if ((ret_value) != KERN_SUCCESS) {
	       catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);	
               fprintf(stderr,catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE5, "skipping %d: task_getrusage\n"),i);
	       (void) catclose(catd);
               continue;
            }

#ifdef TASK_GETRUSAGE_DEBUG
            utime = (athread_info->user_time.seconds + (double)
                            athread_info->user_time.microseconds / 1000000.0);
            stime = (athread_info->system_time.seconds + (double)
                            athread_info->system_time.microseconds / 1000000.0);
            total_time = utime + stime;
	        catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);  
                printf(catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE6, "  CPU Seconds : %10.6f  (Usr: %10.6f,  Sys: %10.6f) THREAD\n"),
                  total_time, utime, stime );
	        (void) catclose(catd);
#endif /* TASK_GETRUSAGE_DEBUG */
            time_value_add(&allthreads_utime, &(athread_info->user_time));
            time_value_add(&allthreads_stime, &(athread_info->system_time));
       }
#ifdef TASK_GETRUSAGE_DEBUG
		      
	catd = catopen(MF_LIBMSFS, NL_CAT_LOCALE);
        printf(catgets(catd, S_MU_TASK_GETRUSAGE1, MU_TASK_GETRUSAGE7, "\n debug info enbs..\n"));
	(void) catclose(catd);
#endif
   ru->ru_utime.tv_sec  = allthreads_utime.seconds;
   ru->ru_utime.tv_usec = allthreads_utime.microseconds;
   ru->ru_stime.tv_sec  = allthreads_stime.seconds;
   ru->ru_stime.tv_usec = allthreads_stime.microseconds;;
}

#endif /* _OSF_SOURCE */

/* end bs_task_getrusage.c */
