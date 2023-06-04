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
# Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
#
#           Notes on modifying this script
#
# This script is now written using the Bourne (sh) shell so that savemeta
# may be used in single user mode.  This is important when you want to run
# savemeta on /usr.  This script MUST be tested in single user mode to verify
# that there are no hidden reliances on utilities in /usr.
#
# Only use commands that are located in /sbin.
#
# save a snapshot of the metadata from a filesystem in a directory
#
# The directory will have the following structure
# dir save_dir / volume_dir / bmt_file
#                           / sbm_file
#                           / rbmt_file
#              / log_file
#              / root_tag_file
#              / default_tag_file
#	       / snap_tag_file
#	       / ...
#
# advvods in using this saved structure will use "save_dir" as the
# filesystem name


#
# The following definition of PATH should not be changed.  This is a sanity
# check that this script is only getting its utilities from /sbin.  This will
# help ensure that the script will run in single user mode.
#
PATH=/sbin
export PATH

VODSPATH=/sbin/fs/advfs/
VODSCMD=${VODSPATH}advvods
FSDIR1=/dev/advfs
FSDIR2=/dev/advfs_cfs
PROG=advsavemeta

LFLG=1
SFLG=1
TFLG=1
tFLG=1
rFLG=0

last_ret_val=0

usage() {
    echo 1>&2 usage: $PROG [-L] [-S] [-T] [-t] [-r] filesystem save_dir
    exit 1
}

#
# Parse command line arguments
#
done=0
while [[ $done -eq 0 ]]
do
    case $1 in

	-L)        # don't save LOG file
	    LFLG=0;;
	-S)        # don't save SBM file
	    SFLG=0;;
	-T)        # don't save root TAG file
	    TFLG=0;;
	-t)        # don't save fileset tag
	    tFLG=0;;
	-r)        # read metadata from a mounted file system
	    rFLG=1;;
	-h)	   # help - issue usage
	    usage;;
	-?)	   # help - issue usage
	    usage;;
	 *)	   # Not a switch, shift and break out of the loop
	    done=1
	    break
	    ;;
    esac
    shift      # Get the next flag
done

if [ $# -ne 2 ]; then
    usage	# should only have filesystem and directory left as args
fi

FS=$1
DIR=$2

# Check for a possibly promoted filesystem 
FSDIR=$FSDIR1
if [ ! -d $FSDIR1/$FS ]
then
	FSDIR=$FSDIR2
fi

# test filesystem name
if [ ! -d $FSDIR/$FS ]
then
	echo 1>&2 $PROG: $FS is not a filesystem defined in $FSDIR1 or $FSDIR2
	exit 1
fi

# make directory
if [ ! -d $DIR ]
then
	mkdir $DIR
	if [ $? != 0 ]
	then
		echo 1>&2 $PROG: failed to make directory $DIR
		exit 1
	fi
fi

STGDIR=$FSDIR/$FS/.stg
if [ ! -d $STGDIR ]
then
	echo 1>&2 $PROG: Storage directory $STGDIR for $FS does not exist.
	exit 1
fi

roption=''
if [ $rFLG = 1 ]
then
        roption='-r'
fi

# save the volume specific RBMT/BMT and SBM files
for vol in `ls $STGDIR`
do
	if [ ! -L $STGDIR/$vol ]
	then
		continue
	fi

	set -- `ls -l $STGDIR/$vol`
	shift 10	# shift 1 less than the desired field to put data in $1
	dev=$1

	mkdir $DIR/$vol
	if [ $? != 0 ]
	then
		echo 1>&2 $PROG: failed to make directory $DIR
		exit 1
	fi

	#
	# If this call to advvods fails, then we remove the directory and bail
	#
	echo $VODSCMD rbmt $roption $dev -d $DIR/$vol/rbmt
	$VODSCMD rbmt $roption $dev -d $DIR/$vol/rbmt
	if [ $? != 0 ]
	then
	    rm -rf $DIR
	    exit 1
	fi

	echo $VODSCMD bmt $roption $dev -d $DIR/$vol/bmt
	$VODSCMD bmt $roption $dev -d $DIR/$vol/bmt
	if [ $? != 0 ]
            then
	    last_ret_val=1
	fi

        if [ $SFLG = 1 ]
	    then
	    echo $VODSCMD sbm $roption $dev -d $DIR/$vol/sbm
	    $VODSCMD sbm $roption $dev -d $DIR/$vol/sbm
	    if [ $? != 0 ]
                then
		last_ret_val=1
	    fi
        fi
done

if [ $LFLG = 1 ]
then
        echo $VODSCMD log $roption $FS -d $DIR/log
	$VODSCMD log $roption $FS -d $DIR/log
        if [ $? != 0 ]
        then
            last_ret_val=1
        fi
fi

if [ $TFLG = 1 ]
then
        echo $VODSCMD rtag $roption $FS -d $DIR/root_tag
	$VODSCMD rtag $roption $FS -d $DIR/root_tag
        if [ $? != 0 ]
        then
            last_ret_val=1
        fi
fi

#
# Create a directory for each file system (default and snapshots) and dump the
# bmt (and tag, if it's requested) information for these file systems
#
for fs in `ls $FSDIR/$FS/`
do
    if [ ! -b $FSDIR/$FS/$fs ]
    then
	continue
    fi

    mkdir $DIR/$fs
    if [ $? != 0 ]
    then
	echo 1>&2 $PROG: failed to make directory $DIR
	exit 1
    fi

#
# Get the bmt page for the individual file sets for each volume in the
# storage domain
#
    vol_index=1
    for vol in `ls $STGDIR`
    do
	echo $VODSCMD bmt $roption $FS $vol_index -S $fs -d $DIR/$fs/bmt_${vol}
	$VODSCMD bmt $roption $FS $vol_index -S $fs -d $DIR/$fs/bmt_${vol}
	if [ $? != 0 ]
	then
	    last_ret_val=1
	fi

	vol_index=`echo $vol_index | awk '{ print ($0 + 1) }'`
    done

    if [ $tFLG = 1 ]
    then

	echo $VODSCMD tag $roption $FS -S $fs -d $DIR/$fs/tag
	$VODSCMD tag $roption $FS -S $fs -d $DIR/$fs/tag
	if [ $? != 0 ]
	then
	    last_ret_val=1
	fi
    fi
done

#
# Error occured, but we won't remove the directory in case some useful
# info was saved
#
if [ $last_ret_val != 0 ]
then
    exit 1
fi

exit 0
