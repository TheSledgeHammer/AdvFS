#!/usr/bin/posix/sh
#
# Must use POSIX shell, else in parallel mode the various
# defrag_this_domain subshells will overwrite each other in
# the defragcron.log file.
# 
# =======================================================================
#   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of version 2 the GNU General Public License as
#   published by the Free Software Foundation.
#   
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#   
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# =======================================================================
#
#
# HISTORY
# 
# @(#)$RCSfile: defragcron.sh,v $ $Revision: 1.1.22.1 $ (DEC) $Date: 2004/02/04 17:13:03 $
# 
 #-----------------------------------------------------------------------------
 # defragcron - this script will attempt to defragment the specified AdvFS
 #       domains or if no domains are specified, all the domains which 
 #       currently have mounted file sets.
 #
 #       This script must be run by root.
 #
 #       This script is ideally suited to be run by 'cron' on a daily basis.
 #       For example:
 #
 #           1 1 * * * /usr/sbin/defragcron -p
 #       or
 #           1 1 * * 1,3,5 /usr/sbin/defragcron -p usr_domain tmp_domain
 #           1 1 * * 2,4 /usr/sbin/defragcron -p root_domain users_domain
 #           1 5 * * 0,6 /usr/sbin/defragcron -p -b 05:00 -a 05:00
 #
 #       The first example defragments all domains which have mounted file
 #       sets starting at 1:01 AM and ending at the default 5:00 AM later
 #       that morning.  
 #
 #       In the 2nd example, on Monday, Wednesday, and Friday, the usr_domain
 #       and tmp_domain are defragmented.  On Tuesday and Thursday the
 #       root_domain and users_domain are defragmented.  And on Saturday and
 #       Sunday all domains are defragmented starting first thing in the
 #       morning. 
 #
 #       Domains will only be defragmented if the defragment utility thinks
 #       the "Aggregate I/O Performance" is less than the
 #       'def_defrag_threshold' variable (see below).  By default the
 #       threshold is set to 100% which tells the script to always perform
 #       the requested defragment.  Use the -t option to specify an alternate
 #       defragment threshold.  Note:  if the domain is lightly fragmented,
 #       it may take almost as long to check if the domain should be
 #       defragmented as it would to just defragment it. 
 #
 #       Domains will only be defragmented if the current time is within the
 #       time period specified by the 'dont_run_before' and the
 #       'dont_run_after' variables (see below).  Use the -a and -b
 #       options to change the allowed range.  
 #
 #       The script's -T option will override any -a or -b options and force
 #       the script to run the defragment utility for the specified number of
 #       minutes regardless of the time of day, as long as the threshold
 #       requirements are met.  The more flexible -a and -b options allow the
 #       script to calculate the number of minutes that the defragment
 #       utility is allowed to run.  By default the script calculates the
 #       number of minutes to run based on the 'def_dont_run_after' time
 #       limit. 
 #
 #       The defragment utility itself is run using its -T option (not to be
 #       confused with this scripts -T option mentioned above).  The
 #       defragment -T option makes sure that the defragment utility stops
 #       consuming system resources at the desired cut off time.  When the
 #       number of minutes elapses, the defragmentation procedure stops, even
 #       if it is performing an operation. 
 #
 #       This script should not be modified.  If possible modify its
 #       behavior using the command line arguments (see the usage message
 #       below).  If that is not possible, it is better to create a copy of
 #       the script, then modify and use the copy. 
 #-----------------------------------------------------------------------------
def_parallel=0                  # -p        - execute defrag's in parallel
def_quiet=0                     # -q        - suppress informational messages
logdirset=0                     # -l        - specify logging directory
def_defrag_threshold=100        # -t nn     - Aggregate I/O Perf threshold 0-100
def_exact_time_interval=0       # -T nn     - explicit number of minutes to run
def_dont_run_before="01:00"     # -b hh:mm  - don't run before this time
def_dont_run_after="05:00"      # -a hh:mm  - don't run after this time
 #                              #             00:00 == midnight
 #                              #             05:30 == 5:30 AM
 #                              #             17:30 == 5:30 PM
def_defrag_options=""
 #-----------------------------------------------------------------------------
E="display"

display() {
    if [ $logdirset -eq 1 ]
    then
	echo $* >> ${logdir}/defragcron.log 2>&1
    else
	echo $*
    fi
}
 #-----------------------------------------------------------------------------
 #
usage() {
echo 1>&2 ""
echo 1>&2 "Usage: $progname [-p] [-q] [-l logdir] [-t nn] [-b hh:mm] [-a hh:mm]"
echo 1>&2 "           [dmn [dmn...]]"
echo 1>&2 "       $progname [-p] [-q] [-l logdir] [-t nn] [-T mm] [dmn [dmn...]]"
echo 1>&2 ""
echo 1>&2 "                 Defragments the specified domains or if no"
echo 1>&2 "                 arguments are specified, it defragments all domains"
echo 1>&2 "                 which have mounted file sets."
echo 1>&2 ""
echo 1>&2 "                 Must be root to run this script."
echo 1>&2 ""
echo 1>&2 "       -p        - defragment the domains in parallel."
ans=`if [ $def_parallel -eq 0 ]; then echo False; else echo True; fi`
echo 1>&2 "                   Default: $ans"
echo 1>&2 ""
ans=`if [ $def_quiet -eq 0 ]; then echo False; else echo True; fi`
echo 1>&2 "       -q        - suppress informational messages."
echo 1>&2 "                   Default: $ans"
echo 1>&2 ""
echo 1>&2 "       -l logdir - Directory to put log files in."
echo 1>&2 "                   Defaults to none:  uses stdout for all the logs."
echo 1>&2 ""
ans=`if [ $def_defrag_threshold -eq 100 ]; then echo "[always defragment]"; else ""; fi`
echo 1>&2 "       -t nn     - Aggregate I/O Performance threshold (0 - 100)."
echo 1>&2 "                   If a 'defragment -nv' has a lower or equal value,"
echo 1>&2 "                   the domain will be defragmented, otherwise"
echo 1>&2 "                   it will be skipped."
echo 1>&2 "                   Default: $def_defrag_threshold $ans"
echo 1>&2 ""
ans=`if [ $def_exact_time_interval -eq 0 ]; then echo "[no default]"; else echo "$def_exact_time_interval minutes"; fi`
echo 1>&2 "       -T mm     - Force an exact time interval (in minutes) for the"
echo 1>&2 "                   defragment utility to run.  This will override the"
echo 1>&2 "                   don't run time range checking, even if an"
echo 1>&2 "                   explicit -a or -b option was specified."
echo 1>&2 "                   Default: $ans"
echo 1>&2 ""
echo 1>&2 "       -b hh:mm  - Do not defragment before this time."
echo 1>&2 "                   Default: $def_dont_run_before"
echo 1>&2 ""
echo 1>&2 "       -a hh:mm  - Do not defragment after this time."
echo 1>&2 "                   Default: $def_dont_run_after"
echo 1>&2 ""
echo 1>&2 "                   where: 00:00 == midnight"
echo 1>&2 "                          05:30 == 5:30 AM"
echo 1>&2 "                          17:30 == 5:30 PM"
exit 1
}
 #-----------------------------------------------------------------------------
 #
time_usage() {
    $E 1>&2 ""
    $E 1>&2 "$progname: Invalid time format.  Use 24 hour clock."
    $E 1>&2 "\t00:00 is midnight."
    $E 1>&2 "\t05:30 is 30 minutes after 5 O'Clock in the morning."
    $E 1>&2 "\t17:00 is 5 O'Clock in the evening."
    $E 1>&2 ""
    $E 1>&2 "\tOne of the following is bad:"
    $E 1>&2 ""
    $E 1>&2 "\t\tDon't run before: $dont_run_before"
    $E 1>&2 "\t\tDon't run after:  $dont_run_after"
    $E 1>&2 "\t\tcurrent time:     `/usr/bin/date +%H:%M`"
    usage;
}
 #-----------------------------------------------------------------------------
dont_run_opt_warning() {
    $E 1>&2 \
    "$progname: $1 $2 ignored because -T $exact_time_interval option specified"
}
 #-----------------------------------------------------------------------------
exact_time_opt_warning() {
    msg="-a $dont_run_after and -b $dont_run_before ignored"
    msg="$msg because -T $exact_time_interval was specified."
    $E 1>&2 "$progname: $msg"
}
 #-----------------------------------------------------------------------------
 #
 # function to figure out if the domain should be defragmented.
 #
time_to_defragment() {
    if [ "$defrag_threshold" -lt 100 ]
    then
        eval $E $quiet ""
        eval $E $quiet defragment -nv $1
        perf=`/usr/sbin/defragment -nv $1 | /usr/bin/awk '
            /I.O perf/ {split($0,line); sub("%","",line[NF]); print line[NF]}
        '`
        if [ "${perf:-:}" = ":" ]
        then
            #
            #  no perf information returned.  Something must have gone wrong.
            #
            if [ "${quiet:-:}" != ":" ]
            then
                /usr/bin/cat $quiet_file 1>&2
            fi
            $E 1>&2 "$progname: could not get domain '$1' performance information"
            return 1    # something went wrong, so we are not going to defrag
        fi
        #
        #  See if the performance value returned is low enough to defrag.
        #
        if [ "$perf" -gt "$defrag_threshold" ]
        then
            return 1    # we are not going to defrag this domain.
        fi
    fi
    return 0    # we are going to defrag this domain.
}
 #-----------------------------------------------------------------------------
 #
 #  Figure out how many minutes we are allowed to run.
 #
how_long_to_defrag() {
    if [ "$exact_time_interval" -gt 0 ]
    then
        #
        # an explicit -T mm number of minutes was specified, so ignore the
        # time limits and defragment the selected domains.
        #
        echo "$exact_time_interval"
        return 0
    fi
    #
    # See if the current time is within the 'dont_run_before' and the
    # 'dont_run_after' time range.  If it is, then calculate the number of
    # minutes until the 'dont_run_after' time and return that value.
    #
    now=`/usr/bin/date +%H:%M`
    how_long=`/usr/bin/awk -v now="$now" \
                  -v before="$dont_run_before" \
                  -v after="$dont_run_after" \
                  '
        BEGIN {
            before = time(before);
            after = time(after);
            now = time(now);
            #
            #  Check for ranges that cross midnight.
            #
            if ( before >= after ) {
                if ( now >= before && now <= time("24:00") ) {
                    print (time("24:00") - now) + after;
                    exit 0;
                }
                else if ( now >= time("00:00") && now < after ) {
                    print after - now;
                    exit 0;
                }
            }
            #
            #  Process a range that starts and ends on the same day.
            #
            else {
                if ( now >= before && now < after ) {
                    print after - now;
                    exit 0;
                }
            }
            #
            #  If we reach this point, then the current time must be outside
            #  of the allowable range.
            #
            print 0;
            exit 0;
        }
        #
        #  awk function to convert the hh:mm time to an single value of
        #  minutes since midnight.  It does minimum sanity checking on the
        #  numbers.
        #
        function time(time_in) {
            split(time_in,a,":");
            if ( int(a[1]) > 24 || int(a[1]) < 0 || 
                 int(a[2]) > 59 || int(a[2]) < 0 ) {
                print -1;       # invalid time value
                exit 0;
            }
            t = (int(a[1]) * 60) + int(a[2]);
            return(t);
        }
    '`
    echo "$how_long"
    return 0
}
 #-----------------------------------------------------------------------------
 #
 #  function to defragment the domain.  It also determines if the current
 #  time is within the allowable time period and how many minutes the
 #  defragment is allowed to run. 
 #
defrag_this_domain() {
    if time_to_defragment $1
    then
        #
        #  Figure out how many minutes we are allowed to run.
        #
        how_long=`how_long_to_defrag`
        #
        #  Defragment the file for the specified period of time.
        #
        if [ "$how_long" -gt 0 ]
        then
            eval $E $quiet "Starting 'defragment -T $how_long $def_defrag_options $1' on `/usr/bin/date`"
	    if [ $logdirset -eq 1 ]
	    then
		/usr/bin/touch ${logdir}/${1}.log
		echo "" >> ${logdir}/${1}.log 2>&1
		echo "Starting 'defragment -T $how_long $def_defrag_options $1' on `/usr/bin/date`" >> ${logdir}/${1}.log 2>&1
		eval /usr/sbin/defragment -T $how_long $def_defrag_options $1 >> ${logdir}/${1}.log 2>&1
	    else
		eval /usr/sbin/defragment -T $how_long $def_defrag_options $1 $quiet
	    fi

            if [ $? -eq 0 ]
            then
                $E "Finished defragment of domain '$1' on `/usr/bin/date`"
		if [ $logdirset -eq 1 ]
		then
		    echo "Finished defragment of domain '$1' on `/usr/bin/date`" >> ${logdir}/${1}.log 2>&1
		fi
	    else
                $E "Defragment of domain '$1' failed on `/usr/bin/date`"
		if [ $logdirset -eq 1 ]
		then
		    echo "Defragment of domain '$1' failed on `/usr/bin/date`" >> ${logdir}/${1}.log 2>&1
		fi
            fi
        elif [ "$how_long" -eq 0 ]
        then
            now=`/usr/bin/date +%H:%M`
            $E 1>&2 "$progname: can only run be between the hours of"
            $E 1>&2 "\t$dont_run_before and $dont_run_after.  It is now $now"
        elif [ "$how_long" -lt 0 ]
        then
            time_usage;
        fi
    else
        eval $E $quiet "domain \'$1\' skipped.  It does not meet the defragment threshold."
    fi
}
 #-----------------------------------------------------------------------------
 #
 #  main
 #
progname=`/usr/bin/basename $0`
 #
if [ "`/usr/bin/id -u`" -ne 0 ]
then
    $E 1>&2 ""
    $E 1>&2 "$progname: Must be root to run this script."
    usage
    exit 1
fi
 #
 #  Ensure that /usr/sbin/defragment executable exists.
 #
if [ ! -x /usr/sbin/defragment ]
then
    # The OSFADVFS subset must not be installed.  Exit quietly.
    exit 0
fi
 #
 #  Parse the command line options.
 #
parallel="$def_parallel"
defrag_threshold="$def_defrag_threshold"
exact_time_interval="$def_exact_time_interval"
dont_run_before="$def_dont_run_before"
dont_run_after="$def_dont_run_after"
 #
quiet_file="/tmp/$progname.XXXXXXX"
quiet_file=`/usr/bin/mktemp $quiet_file`
if [ $? -ne 0 ]
then
    echo "Could not create quiet file. Continuing without suppressing
    error messages"
fi

if [ "$def_quiet" -ne 0 ]
then
    quiet=">>$quiet_file 2>&1"
else
    quiet=""
fi

 #
logdir=""
 #
dont_run_opt=0
exact_time_opt=0
 #
while true
do
    case $1 in
    -p) parallel=1
        shift
        ;;
    -q) quiet=">>$quiet_file 2>&1"
        shift
        ;;
    -l) logdir="$2"
	logdirset=1
	eval /usr/bin/mkdir -p ${logdir} > /dev/null 2>&1
 	if [ ! -d ${logdir} ]
 	then
 	    logdir=""
 	    $E 1>&2 ""
 	    $E 1>&2 "$progname: -l logdir must specify a directory.  $2 is"
 	    $E 1>&2 "$progname: not a directory and cannot be created."
 	    usage;
 	    exit 1
 	fi
        shift 2
        ;;
    -t) defrag_threshold="$2"
        shift 2 
        ;;
    -T) exact_time_interval="$2"
        exact_time_opt=1
        if [ "$dont_run_opt" -ne 0 ]
        then
            exact_time_opt_warning
        fi
        shift 2 
        ;;
    -b) dont_run_before="$2"
        dont_run_opt=1
        if [ "$exact_time_opt" -ne 0 ]
        then
            dont_run_opt_warning $1 $2
        fi
        shift 2
        ;;
    -a) dont_run_after="$2"
        dont_run_opt=1
        if [ "$exact_time_opt" -ne 0 ]
        then
            dont_run_opt_warning $1 $2
        fi
        shift 2
        ;;
    -*) $E 1>&2 "$progname: unknown flag: -$1"
        usage
        break
        ;;
    *)  break
        ;;
    esac
done
 #
 #  See if this procedure is even being run during the correct time interval
 #
how_long=`how_long_to_defrag`
if [ "$how_long" -eq 0 ]
then
    now=`/usr/bin/date +%H:%M`
    $E 1>&2 ""
    $E 1>&2 "$progname: can only run between the hours of"
    $E 1>&2 "\t$dont_run_before and $dont_run_after.  It is now $now"
    usage
    exit 1
elif [ "$how_long" -lt 0 ]
then
    time_usage;
fi
 #
 #  See if there are domains specified on the command line.
 #
if [ $# -gt 0 ]
then
    #
    #  Defragment the specified domains.
    #
    domains="$*"
else
    #
    #  Defragment all the domains with mounted file sets.
    #
    domains=`/sbin/mount | /usr/bin/awk '
        /type advfs.*(rw)/ {
            split($0,a,"#");
            print a[1];
        }
    ' | /usr/bin/sort | /usr/bin/uniq`
fi
 #
 # Log the time defragcron starts.
 #
$E $quiet ""
$E $quiet "Starting $progname on `/usr/bin/date`"
 #
 #  Defragment the compiled list of domains.
 #
for dmn in $domains
do
    if [ "$parallel" -ne 0 ]
    then
        defrag_this_domain $dmn &
    else
        defrag_this_domain $dmn
    fi
done
if [ "$parallel" -ne 0 ]
then
    wait
fi
 #
 # do some clean-up
 #
if [ -e "$quiet_file" ]
then
    /bin/rm "$quiet_file"
fi

 #
 # Log the time defragcron exits.
 #
$E $quiet "Exiting $progname on `/usr/bin/date`"
