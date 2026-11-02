#!/bin/bash

submit_dir=$1
resubmit_yaml=$2
loop=$3

usage() {
	echo "$0 <submit_dir> <resubmit_yaml> <loop>"
	echo "Where <submit_dir> is the run directory of a current BPS job,"
	echo "<resubmit_yaml> is the BPS Yaml of the job to resubmit on failure,"
	echo "<loop> is a boolean, set true to keep resubmitting until SUCCESS, false otherwise."
}

# Comment out the first-try extraQgraphOptions line ("# extraQgraphOptions: ...")
# and uncomment the second retry line ("extraQgraphOptions: ... --skip-existing-in ... --clobber-outputs").
enable_retry_qgraph_options() {
	local yaml="$1"
	[ ! -f "$yaml" ] && return 1
	awk '
	{
		lines[NR] = $0
	}
	END {
		done_retry = 0
		for (i = 1; i <= NR; i++) {
			if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:/ && lines[i] !~ /--skip-existing-in/) {
				base_line = lines[i]
				sub(/^[[:space:]]*/, "", base_line)
				print "# " base_line
				if (i < NR && lines[i+1] ~ /^[[:space:]]*#[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/) {
					retry_line = lines[i+1]
					sub(/^[[:space:]]*#[[:space:]]*/, "", retry_line)
					print retry_line
					i++
				} else {
					retry_line = base_line
					sub(/"[[:space:]]*$/, " --skip-existing-in {output} --clobber-outputs\"", retry_line)
					print retry_line
				}
				done_retry = 1
			}
			else if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/) {
				if (done_retry == 0) {
					print lines[i]
					done_retry = 1
				} else {
					sub_line = lines[i]
					sub(/^[[:space:]]*/, "", sub_line)
					print "# " sub_line
				}
			}
			else if (done_retry == 0 && lines[i] ~ /^[[:space:]]*#[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/ && (i == 1 || lines[i-1] !~ /extraQgraphOptions:/)) {
				retry_line = lines[i]
				sub(/^[[:space:]]*#[[:space:]]*/, "", retry_line)
				print retry_line
				done_retry = 1
			}
			else {
				print lines[i]
			}
		}
	}
	' "$yaml" > "${yaml}.tmp" && mv "${yaml}.tmp" "$yaml"
}

if [ `echo $submit_dir | grep -c "/"` -eq 0 ];
then
	usage
	exit 1
fi

if [ `echo $resubmit_yaml | grep -c "yaml"` -eq 0 ];
then
        usage
        exit 1
fi

if [ -z $loop ];
then
        usage
        exit 1
fi

end=false

while true;
do
	status=$(bps status --id $submit_dir 2>&1| awk '{print $NF}')
	echo $status
	if [ $status == SUCCEEDED ];
	then
		end=true
		break
	elif [ $status == FAILED ];
	then
		break
	elif [ $status == UNKNOWN ];
	then
		exit 1
	fi
	sleep 30
done
if [ $end == "true" ];
then
	echo "SUCCESS"
	exit 0
elif [ $loop == "true" ];
then
	echo "FAILED - enabling retry extraQgraphOptions (--skip-existing-in ... --clobber-outputs), resubmitting and tracking"
	enable_retry_qgraph_options "$resubmit_yaml"
	resubmit_submit_dir=`bps submit $resubmit_yaml 2>&1| grep "Submitting from directory" | awk '{print $NF}'`
	$0 $resubmit_submit_dir $resubmit_yaml true
fi
exit 0
