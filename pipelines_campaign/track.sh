#!/bin/bash

submit_dir=$1
next_yaml=$2
headless=$3

usage() {
	echo "$0 <submit_dir> <next_yaml> [<headless>]"
	echo "Where <submit_dir> is the run directory of a current BPS job,"
	echo "<next_yaml> is the BPS Yaml of a dependent job,"
	echo "<headless> is an optional boolean for headless mode, in which only the final echo is printed."
}

if [ `echo $submit_dir | grep -c "/"` -eq 0 ];
then
	usage
	exit 1
fi

if [ `echo $next_yaml | grep -c "yaml"` -eq 0 ];
then
        usage
        exit 1
fi

if [ $headless != true ];
then
	echo "Tracking $submit_dir and waiting to submit dependent yaml $next_yaml."
fi

while true;
do
	status=$(bps status --id $submit_dir 2>&1| awk '{print $NF}')
	if [ $headless != true ];
	then 
		echo $status
	fi
	if [ $status == SUCCEEDED ];
	then
		break
	elif [ $status == FAILED ];
	then
		exit 1
	elif [ $status == UNKNOWN ];
	then
		exit 1
	fi
	sleep 30
done
next_submit_dir=`bps submit $next_yaml 2>&1| grep "Submitting from directory" | awk '{print $NF}'`
echo $next_submit_dir
exit 0

