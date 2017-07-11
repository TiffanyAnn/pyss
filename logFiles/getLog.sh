#!/bin/bash

# This script collects job information from slurm's sacct log and generates a workload trace

if [ "$#" -ne 4 ]; then
   echo "usage: ./getLog.sh <rawLog> <outfile> <startDate> <endDate>
   rawlog - filename for unprocessed log output
   outfile - filename for processed log file
   start - begin date in YYYY-MM-DD format
   end - end date in YYYY-MM-DD format"
  exit 1
fi

rawLog=$1
outfile=$2
startDate=$3
endDate=$4

sacct --start=${startDate} --end=${endDate} --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular,debug --noheader -X --duplicates -a > $rawLog

# convert requested time limit into seconds. Format may be either D-HH:MM:SS or HH:MM:SS
awk '{ print $6 }' $rawLog | sed 's/-/:/' | awk -F: '{ if ($4 != "" ) print ($1 * 86400) + ($2 * 3600) + ($3 * 60) + $4 ; else print($1 * 3600) + ($2 *60) + $3; }' > timeConversion
awk 'NR==FNR{a[NR]=$0;next} {$6=a[FNR]}1' timeConversion $rawLog > tmp.log
# add labels to each column entry
awk '{print "JobId="$1 " UserId="$2 " Name="$3 " JobState="$4 " Partition="$5 " TimeLimit="$6 " StartTime="$7 " EndTime="$8 " NNode="$9 " NodeCnt="$10}' tmp.log > ${outfile}.log

# remove temp files
rm tmp.log timeConversion

# convert to Standard Workload Format
./slurm2swf.pl ${outfile}.log > ${outfile}.swf
