#!/sbin/sh
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
# @(#)$RCSfile: savemeta.sh,v $ $Revision: 1.1.13.1 $ (DEC) $Date: 2006/08/30 15:25:47 $
#

#
#           Notes on modifying this script
#
# This script is now written using the Bourne (sh) shell so that savemeta
# may be used in single user mode.  This is important when you want to run
# savemeta on /usr.  This script MUST be tested in single user mode to verify
# that there are no hidden reliances on utilities in /usr.  Utilities like
# awk, fgrep, getopt(s) are all located in /usr/sbin and may not be used.
#
# Only use commands that are located in /sbin.  The 'expr' command is very
# helpful in providing string and math services.

# save a snapshot of the metadata from a domain in a directory
#
# The directory will have the following structure
# dir save_dir / volume_dir / bmt_file
#                           / sbm_file
#                           / rbmt_file       # for V4 and above domains
#              / log_file
#              / tag_file
#              / fileset_dir / frag_file
#                            / tag_file
#
# nvbmtpg and other programs that use this saved structure will use "save_dir"
# as the domain name

usage() {
    echo 1>&2 savemeta [-L] [-S] [-T] [-t] [-r] [-f fileset] domain save_dir
    exit 1
}

#
# The following definition of PATH should not be changed.  This is a sanity
# check that this script is only getting its utilities from /sbin.  This will
# help ensure that the script will run in single user mode.
#
PATH=/sbin
export PATH

VODSPATH=/sbin/advfs/
NVBMTPG=${VODSPATH}nvbmtpg
VSBMPG=${VODSPATH}vsbmpg
NVLOGPG=${VODSPATH}nvlogpg
NVTAGPG=${VODSPATH}nvtagpg
NVFRAGPG=${VODSPATH}nvfragpg

LFLG=1
SFLG=1
TFLG=1
tFLG=1
FFLG=0
rFLG=0

last_ret_val=0

# Check for root permission.  We use a function so as to not destroy the
# argument list for savemeta.
check_path() {
	set -- `ps o uid -p $$`
	pid=$2
        if [ $pid != 0 ]
        then
	    echo 1>&2 Permission denied - user must be root to run savemeta
	    exit 1
	fi
}

check_path

while [ X`expr "Z$1" : 'Z\(.\).*'` = "X-" ]
do
    case $1 in
    -L)	# don't save LOG file
	LFLG=0;;
    -S)	# don't save SBM file
	SFLG=0;;
    -T)	# don't save root TAG file
	TFLG=0;;
    -t)	# don't save fileset tag
	tFLG=0;;
    -r) # save metadata from the raw device rather than block device
	rFLG=1;;
    -f) # save frag for all filesets
	fragset=$2
	shift
	FFLG=1;;
    -f*) # -f with fragset name appended, as in "-fgeorge" vs "-f george"
	fragset=`expr $1 : '-f\(.*\)'`	# extract fragset name
	FFLG=1;;
    --) # -- indicates end of options; break out of the while loop
	shift
	break;;
    -[LSTtr]?*) # multiple arguments lumped together
	arg1=`expr $1 : '\(-.\).*'`	# extract dash and 1st letter
	arg2=-`expr $1 : '-.\(.*\)'`	# extract remainder of letters
	shift				# throw away $1 and build new arglst
	set -- junk_to_be_shifted_out_at_bottom_of_while_loop $arg1 $arg2 $*
	;;
    *)	usage;;
    esac
    shift	# Get the next flag
done

if [ $# -ne 2 ]; then
    usage	# should only have domain and directory left as args
fi

DOMAIN=$1
DIR=$2

# test domain name
if [ ! -d /etc/fdmns/$DOMAIN ]
then
	echo 1>&2 $DOMAIN is not a domain defined in /etc/fdmns
	exit 1
fi

# make directory
if [ ! -d $DIR ]
then
	mkdir $DIR
	if [ $? != 0 ]
	then
		echo 1>&2 failed to make directory $DIR
		exit 1
	fi
fi

if [ ! -w $DIR ]
then
	echo 1>&2 no write permission in $DIR
	exit 1
fi

# Determine Disk Version
new_version=4

set -- `$VSBMPG -r $DOMAIN | grep "This domain version"`
version=$4

# save all the BMT files and SBM files
for vol in `ls /etc/fdmns/$DOMAIN`
do
	if [ ! -L /etc/fdmns/$DOMAIN/$vol ]
	then
		continue
	fi

	set -- `ls -l /etc/fdmns/$DOMAIN/$vol`
	shift 10	# shift 1 less than the desired field to put data in $1
	dev=$1

        if [ $rFLG = 1 ]
        then
             if [ X`expr "$dev" : '\(/dev/disk/\).*'` = "X/dev/disk/" ]
             then
                 name=`expr "$dev" : '/dev/disk/\(.*\)'`
                 dev=/dev/rdisk/$name
             elif [ X`expr "$dev" : '\(/dev/vol/\).*'` = "X/dev/vol/" ]
             then
                 name=`expr "$dev" : '/dev/vol/\(.*\)'`
                 dev=/dev/rvol/$name
             elif [ X`expr "$dev" : '\(/dev/rz/\).*'` = "X/dev/rz/" ]
             then
                 name=`expr "$dev" : '/dev/rz\(.*\)'`
                 dev=/dev/rrz$name
             else
                 echo 1>&2 could not find raw device for $dev
                 continue
             fi
	fi

	mkdir $DIR/$vol
	if [ $? != 0 ]
	then
		echo 1>&2 failed to make directory $DIR
		exit 1
	fi

        if [ $version -eq $new_version ]
        then
            echo $NVBMTPG -R $dev -d $DIR/$vol/rbmt
            $NVBMTPG -R $dev -d $DIR/$vol/rbmt
            if [ $? != 0 ]
            then
                last_ret_val=1
            fi
            echo $NVBMTPG $dev -d $DIR/$vol/bmt
            $NVBMTPG $dev -d $DIR/$vol/bmt
            if [ $? != 0 ]
            then
                last_ret_val=1
            fi
        else
            echo $NVBMTPG $dev -d $DIR/$vol/bmt
            $NVBMTPG $dev -d $DIR/$vol/bmt
            if [ $? != 0 ]
            then
                last_ret_val=1
            fi
        fi

        if [ $SFLG = 1 ]
        then
                echo $VSBMPG $dev -d $DIR/$vol/sbm
                $VSBMPG $dev -d $DIR/$vol/sbm
                if [ $? != 0 ]
                then
                    last_ret_val=1
                fi
        fi
done

roption=''
if [ $rFLG = 1 ]
then
        roption='-r'
fi

if [ $LFLG = 1 ]
then
        echo $NVLOGPG $roption $DOMAIN -d $DIR/log
        $NVLOGPG $roption $DOMAIN -d $DIR/log
        if [ $? != 0 ]
        then
            last_ret_val=1
        fi
fi

if [ $TFLG = 1 ]
then
        echo $NVTAGPG $roption $DOMAIN -d $DIR/tag
        $NVTAGPG $roption $DOMAIN -d $DIR/tag
        if [ $? != 0 ]
        then
            last_ret_val=1
        fi
fi

$NVTAGPG $roption $DOMAIN | grep 'primary mcell' |
while read x
do
	set -- $x
	fset=$1

	mkdir $DIR/$fset
	if [ $? != 0 ]
	then
		echo 1>2& failed to make directory $DIR/$fset
		exit 1
	fi
	if [ $tFLG = 1 ]
	then
                echo $NVTAGPG $roption $DOMAIN $fset -d $DIR/$fset/tag
                $NVTAGPG $roption $DOMAIN $fset -d $DIR/$fset/tag
                if [ $? != 0 ]
                then
                    last_ret_val=1
                fi
	fi

	if test $FFLG -eq 1 -a $fset = "$fragset"
	then
                echo $NVFRAGPG $roption $DOMAIN $fset -d $DIR/$fset/frag
                $NVFRAGPG $roption $DOMAIN $fset -d $DIR/$fset/frag
                if [ $? != 0 ]
                then
                    last_ret_val=1
                fi
	fi
done

if [ $last_ret_val != 0 ]
then
    exit 1
fi

exit 0
