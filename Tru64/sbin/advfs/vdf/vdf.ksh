#!/usr/bin/ksh -p
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
# Facility:
#	Advanced File System
#
# Abstract:
#	Displays disk usage statistics within AdvFS domains and filesets
#
# HISTORY
# 
# @(#)$RCSfile: vdf.ksh,v $ $Revision: 1.1.10.1 $ (DEC) $Date: 2000/03/10 18:43:46 $
#


#######################################################################
#
#       Name:           vdf
#
#       Usage:          vdf [-kl] domain | domain#fileset
#
#       Function:       Displays statistics on free space within
#			AdvFS domains and filesets
#
#       Notes:          Root privilege required.
#                       All filesets in the domain must be mounted.
#			May not be used on NFS-mounted filesets.
#
#			Clone fileset space not calculated; 
#			warning message if clones present in domain
#
#######################################################################


SHOWFSETS="eval LC_MESSAGES=C /sbin/showfsets"
SHOWFDMN="eval LC_MESSAGES=C /sbin/showfdmn"
SHFRAGBF="eval LC_MESSAGES=C /sbin/advfs/shfragbf"
LS="eval LC_MESSAGES=C /sbin/ls"
ID="eval LC_MESSAGES=C /usr/bin/id"
DF="eval LC_MESSAGES=C /sbin/df"

DF_OUT=/sbin/df
FGREP=/usr/bin/fgrep
AWK=/usr/bin/awk
DSPMSG=/usr/bin/dspmsg
WC=/usr/bin/wc

kflag=""
long_listing=0
clone=0

#######################################################################
# Error messages
#######################################################################

usage()
{
	$DSPMSG 1>&2 vdf.cat 1 "usage: vdf [-kl] domain | domain#fileset\n"
	exit 1
}

error_mount()
{
	$DSPMSG 1>&2 vdf.cat 3 "vdf: All filesets in the domain must be mounted.\n"
	exit 1
}

error_domain()	
# argument: domain
{
	$DSPMSG 1>&2 vdf.cat 4 'vdf: No such domain \"%s\"\n' $1
	usage
	exit 1
}

error_fset()	
# argument: fset
{
	$DSPMSG 1>&2 vdf.cat 5 'vdf: No such fileset \"%s\"\n' $1
	usage
	exit 1
}

error_no_fsets()
{
	$DSPMSG 1>&2 vdf.cat 16 'vdf: Domain \"%s\" has no filesets\n' $1
	usage
	exit 1
}

#######################################################################
# Check arguments
#######################################################################

if [ $# = 0 ]
then
	usage
fi

integer arg; arg=0
for j
do
	case $1 in
	-k) 
		if [ "X$kflag" != "X" ]
		then
			usage
		fi
		kflag="-k"
		shift
		;;
	-l) 
		if [ $long_listing = 1 ]
		then
			usage
		fi
		long_listing=1
		shift
		;;
	-kl|-lk)
		if [ "X$kflag" != "X" -o $long_listing = 1 ]
		then
			usage
		fi
		kflag="-k"
		long_listing=1
		shift
		;;
	-*) 
		usage
		;;
	*)  argument=$1
		arg=$arg+1
		shift
		break;
		;;
	esac
done

if [ $# -ne 0 ]
then
	usage
fi 

if [ $arg != 1 ]
then
	usage
fi



#######################################################################
# Check if domain/fileset exists
#######################################################################
domain=`echo $argument | $AWK -F# '{print $1}'`
fset=`echo $argument | $AWK -F# '{print $2}'`
# Note that fset is null in the case only a domain was specified

if [ "X`echo $domain | $FGREP /`" != "X" ]
then
	error_domain $domain;
fi

if [ "X`echo $argument | $FGREP \#`" != "X"  -a "X$fset" = "X" ]
then
	usage
fi

if [ ! -d /etc/fdmns/$domain ]
then 
	error_domain $domain;
fi 

if [ "X$fset" != "X" ]		
# if a fileset was specified
then
	match=0
	for j in `$SHOWFSETS -b $domain 2>/dev/null`
	do
		case $j in
			$fset) match=1 ;;
		esac
	done

	if [ $match -eq 0 ]
	then
 		error_fset $domain#$fset;
 	else
 		fset=$argument
	fi				
fi

fsets=`$SHOWFSETS -b $domain 2>/dev/null`
if [ "X$fsets" = "X" ]		
then
    error_no_fsets $domain
fi

mounted=`$SHOWFDMN $domain|fgrep "Vol Name" 2>/dev/null`
if [ "X$mounted" = "X" ]		
then
    error_mount;
fi

#######################################################################
# If just a fileset argument (no long listing)
#######################################################################

if [ "X$fset" != "X"  -a $long_listing = 0 ]
then
	$DF_OUT $kflag $fset
	exit
fi


#######################################################################
# Calculation of maxwidth.  Continued in fileset loop.  (Formatting)
#######################################################################

#
# maxwidth: max(Domain+1, Fileset+1, domain name, fileset names)
# For i18n, use "17" for length of first field instead of len(Fileset+1)
#
firstfield=17

maxwidth=$firstfield
len=${#domain}
if [ $len -gt $maxwidth ]
then
	maxwidth=$len
fi

#######################################################################
# metadata/data calculation: done in 1024.  Later converted
#######################################################################

#
# Determining Disk Version
#

integer new_version any_old_version
new_version=4
any_old_version=$new_version-1
num_fields=`$SHOWFDMN $domain |$FGREP "Date Created"|$WC -w`

version=$any_old_version
if [ $num_fields -eq 7 ]
then
    version=`$SHOWFDMN $domain |$FGREP $domain|awk '{print $8}'`
fi


#
# fset calculation
#
integer fset_used fset_subused fset_submeta
integer quotagroup quotauser M1 frag_free frag_overhead frag_wasted

for i in $fsets; do
	tmp=`$DF -k $domain#$i 2>/dev/null |tail -1 `

	#
	# If fset not mounted, error
	#
	if [ "X$tmp" = "X" ]
	then
		error_mount;
		break;
	fi

	set -- $tmp
	fset_subused=$3

	#
	# Calculation of maxwidth 
	#
	len=${#i}
	if [ $len -gt $maxwidth ]
	then
		maxwidth=$len
	fi

	#
	# Clones in domain?  Get metasize only
	#
	cloneof=`$SHOWFSETS $domain $i |$FGREP "Clone of" |$AWK '{print $4}' 2> /dev/null`
	if [ "X$cloneof" != "X" ]	
	then
		clone=1
		continue;
	fi
	
	#
	# Excludes used data from the cloned fileset
	#
	
	fset_used=$fset_used+$fset_subused
done

#
# Get DOMAIN metadata size
#
integer domain_metadata

set -- `$SHOWFDMN -mk  $domain | tail -1 2> /dev/null`

domain_metadata=$6

#
# Detect current logfile volume
#
integer  num_vol  

num_vol=0

vol_str=`$SHOWFDMN $domain | $AWK '
    NF == 8 && match($4,"%") {
            sub("L"," ",$1)
            print $1
    }
'`

for j in $vol_str; do
        num_vol=$num_vol+1
done
        
#
# Detect current logfile volume
#

integer log_vol 
log_vol=`$SHOWFDMN $domain | $AWK '
    NF == 8 && match($1,"L") {
            sub("L"," ",$1)
            print $1
    }
'`

#
# Domain Totals and Conversion
#
integer domain_used domain_total

domain_used=$fset_used
if [ "X$kflag" = "X" ]
then
	domain_metadata=$domain_metadata*2
	domain_used=$domain_used*2
fi

set -- `$SHOWFDMN $kflag $domain | tail -1 2> /dev/null`
if [ $num_vol -gt 1 ]
then
	domain_total=$1
	domain_available=$2
	domain_capacity=$3
else
	domain_total=$2
	domain_available=$3
	domain_capacity=$4
fi

#######################################################################
# Formatting output
#######################################################################

$DSPMSG vdf.cat 6 "Domain            "

integer count
count=$maxwidth-$firstfield
while [ $count -gt 0 ]
do
	printf " "
	count=$count-1
done

if [ "X$kflag" = "X" ] 
then
	$DSPMSG vdf.cat 7 " 512-blocks    Metadata        Used   Available Capacity\n"
else	
	$DSPMSG vdf.cat 8 "1024-blocks    Metadata        Used   Available Capacity\n"
fi

formatD="%-${maxwidth}.${maxwidth}s%12s%12s%12s%12s%9s"

$AWK -v formatD=$formatD -v a=$domain -v b=$domain_total -v c=$domain_metadata -v d=$domain_used -v e=$domain_available -v f=$domain_capacity 'BEGIN { printf formatD, a, b, c, d, e, f }' < /dev/null
echo "\n"


if [ $long_listing = 1 ]
then
	$DSPMSG vdf.cat 9 "Fileset           "
	count=$maxwidth-$firstfield
	while [ $count -gt 0 ]
	do
	    printf " "
	    count=$count-1
	done

	$DSPMSG vdf.cat 10 " QuotaLimit                    Used   Available Capacity\n"

	formatF="%-${maxwidth}.${maxwidth}s%12s%24s%12s%9s"

	for i in $fsets; do

		set -- `$DF $kflag $domain#$i | tail -1`	
		fset_total=$2
		fset_used=$3
		fset_available=$4
		fset_capacity=$5

		# See if quotas are enabled
		fset_total=`$SHOWFSETS -q $domain $i |tail -1 | $AWK -v total=$fset_total '{if (($4 == 0) && ($5 == 0)) print "-"; else print total }'`

		$AWK -v formatF=$formatF -v a=$i -v b=$fset_total -v c=$fset_used -v d=$fset_available -v e=$fset_capacity 'BEGIN { printf formatF, a, b, c, d, e }' < /dev/null
		echo ""

	done	
fi

if [ $clone = 1 ]
then
	$DSPMSG vdf.cat 15 "\nClone fileset(s) in this domain; totals may not be accurate\n"
fi

exit 0

