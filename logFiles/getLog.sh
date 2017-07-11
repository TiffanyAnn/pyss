#!bin/bash

if [ "$#" -ne 4 ]; then
   echo "usage: ./getLog.sh rawLog outfile start end \n
         \trawlog - filename for unprocessed log output\n
         \toutfile - filename for processed log file\n
         \tstart - begin date in YYYY-MM-DD format\n
         \tend - end date in YYYY-MMM-DD format \n"

rawLog=$1
outfile=$2
start=$3
end=$4

sacct --start=2017-01-01 --end=2017-01-31 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog
#sacct --start=2017-02-01 --end=2017-02-28 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog
#sacct --start=2017-03-01 --end=2017-03-31 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog
#sacct --start=2017-04-01 --end=2017-04-30 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog
#sacct --start=2017-05-01 --end=2017-05-31 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog
#sacct --start=2017-06-01 --end=2017-06-30 --format=job,user,jobname,state,partition,TimeLimit,start,end,AllocNodes,NNodes --state=CD,TO,F,NF -p --delimiter=' ' --partition=regular --noheader -X --duplicates -a >> $rawLog

awk '{ print $6 }' $rawLog | sed 's/-/:/' | awk -F: '{ if ($4 != "" ) print ($1 * 86400) + ($2 * 3600) + ($3 * 60) + $4 ; else print($1 * 3600) + ($2 *60) + $3; }' > timeConversion

awk 'NR==FNR{a[NR]=$0;next} {$6=a[FNR]}1' timeConversion $rawLog > tmp.log

awk '{print "JobId="$1 " UserId="$2 " Name="$3 " JobState="$4 " Partition="$5 " TimeLimit="$6 " StartTime="$7 " EndTime="$8 " NNode="$9 " NodeCnt="$10}' tmp.log > $outfile

rm tmp.log

#awk '{ print $6 }' $rawLog | sed 's/-/:/' | awk -F: '{ if ($4 != "" ) print ($1 * 86400) + ($2 * 3600) + ($3 * 60) + $4 ; else print($1 * 3600) + ($2 *60) + $3; }' > timeConversion

#awk 'NR==FNR{a[NR]=$0;next} {$6=a[FNR]}1' timeConversion coriLog > coriLog2017
