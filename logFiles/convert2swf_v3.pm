#!/usr/bin/perl
#
# this is a general module for converting parallel workload data into SWF format.
# SWF (the standard workload format) is described at URL
# http://www.cs.huji.ac.il/labs/parallel/workload/swf.html.
#
# Written by Dror Feitelson in July 2006, based on various earlier scripts,
# some contributed by Dan Tsafrir.
# Version 2 done in January 2007.
#
# this module exports 3 functions:
# 1) init
# 2) convert
# 3) print
# it is assumed to be used by a workload-specific script that parses
# the original log and calls these functions with the appropriate data.
#
# the function interfaces are:
#
# init($tz_str, $max_p, $queue_list)
#    $tz_str: scalar, standard timezone identifier such as "Asia/Jerusalem"
#    $max_p: scalar, number of processors in machine
#    $queue_list: reference to array of queue names.
#        if interactive jobs are included in the log, the first entry
#        should be "interactive"
#
# convert($jobs_data, $format_bugs, $sort_users)
#    $jobs_data: reference to an array of references to hashes that describe
#        the different jobs. the fields allowed for each job are listed
#        below.
#    $format_bugs: a (possibly long) string with data about parsing problems.
#        this is dumped to the conversion log as is.
#    $sort_users: optional argument to renumber users/groups/apps/partitions
#        by submit order.
#
# print($header)
#    $header: a (long) string with the SWF header, up to but not including
#        the list of queues. this will be dumped as is at the head of the
#        generated output file, with the queues added based on the data
#        provided above.
#
# the supported fields for each job are
#  1) submit    (UTC timestamp)
#  2) start     (UTC timestamp)
#  3) end       (UTC timestamp)
#  4) run       (int; alternative for end-start, new in ver 2)
#  5) procs     (int; note that procs, not nodes)
#  6) cpu       (seconds per proc)
#  7) mem       (KB per proc)
#  8) req_procs (int; note that procs, not nodes)
#  9) req_time  (seconds, wallclock or CPU per proc)
# 10) req_mem   (KB per proc)
# 11) status    (1=ok, 0=fail, 5=cancel)
# 12) user      (int)
# 13) group     (int)
# 14) app       (int)
# 15) queue     (int)
# 16) partition (int)
# if a given log does not contain certain data, the field should
# be left undefined.

package SWF;
use warnings;
use strict;
use Time::Local;
use POSIX;

use Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(&init &convert &print);
our $VERSION = 3;


#
# initializations
#
open_bug_log();    # done before setting timezone in "init"

# globals keep track of users, groups, and apps for numbering
my (%users, %groups, %apps, %queues, %partitions);
$users{-1}      = -1;
$groups{-1}     = -1;
$apps{-1}       = -1;
$queues{-1}     = -1;
$partitions{-1} = -1;
my $users_cnt      = 0;
my $groups_cnt     = 0;
my $apps_cnt       = 0;
my $queues_cnt     = 0;
my $partitions_cnt = 0;

# other globals
my %jobs_to_print;
my ($zero, $maxtime, $max_procs);
$zero    = -1;
$maxtime = -1;


sub init {
##########
# init data from parser
    my ($tz_str, $max_p, $queue_list, $partition_list) = @_;

    if ($tz_str eq "") {
    die "time-zone string MUST be specified in variable \$tz_str";
    }
    $ENV{TZ} = $tz_str;
    POSIX::tzset();

    $max_procs = $max_p;

    foreach my $q (@$queue_list) {

    if ($q =~ /interactive/) {
        $queues{$q} = 0;
        if ($q ne "interactive") {
        warn ">>> queue $q mapped to 0 (interactive)";
        }
    }
    else {
        $queues{$q} = ++$queues_cnt;
    }
    }

    if (defined($partition_list)) {
    foreach my $p (@$partition_list) {

        $partitions{$p} = ++$partitions_cnt;
    }
    }
}


sub convert {
#############
# get jobs from parser and do actual conversion into SWF, including checks
    my ($jobs_data, $format_bugs, $sort_users) = @_;

    print_format_bugs( $format_bugs );

    check_fields($jobs_data, $sort_users);

    foreach my $job (@$jobs_data) {

    my ($arrival, $job_string) = handle_job( $job );
    if ($arrival != -1) {
        $jobs_to_print{$arrival} .= $job_string;
    }
    else {
        warn ">>> job deleted!!! $job->{trace}";
    }
    }

    return ($zero, $maxtime);
}


sub check_fields {
##################
# check that fields contain meaningful data.
# also setup for anonymization of users, groups, and apps.
    my ($jobs_data, $sort_users) = @_;

    ### ver 3 make sure we can sort (this can be approximate).
    ###       if not, retain (approx) submit time from previous job.
    my $prev_submit;
    foreach my $job (@$jobs_data){

        if (defined($job->{submit}) && ($job->{submit} != -1)) {
        $job->{isubmit} = $job->{submit};
    }
    elsif (defined($job->{start}) && ($job->{start} != -1)) {
        $job->{isubmit} = $job->{start};
    }
    else {
        $job->{isubmit} = $prev_submit;
    }
    $prev_submit = $job->{isubmit};
    }

    my @sorted_jobs;
    if (defined($sort_users) && ($sort_users)) {
    @sorted_jobs = sort {$a->{isubmit} <=> $b->{isubmit}} @$jobs_data;
    }
    else {
    # retain order as in original log for backwards compatability
    @sorted_jobs = @$jobs_data
    }

    foreach my $job (@sorted_jobs) {

    if (defined($job->{user})) {
        my $user = $job->{user};
        if (! defined($users{$user})) {
        $users{$user} = ++$users_cnt;
        }
    }

    if (defined($job->{group})) {
        my $group = $job->{group};
        if (! defined($groups{$group})) {
        $groups{$group} = ++$groups_cnt;
        }
    }

    if (defined($job->{app})) {
        my $app = $job->{app};
        if (! defined($apps{$app})) {
        $apps{$app} = ++$apps_cnt;
        }
    }

    if (defined($job->{queue})) {
        my $queue = $job->{queue};
        if (! defined($queues{$queue})) {
        $queues{$queue} = ++$queues_cnt;
        }
    }

    if (defined($job->{partition})) {
        my $partition = $job->{partition};
        if (! defined($partitions{$partition})) {
        $partitions{$partition} = ++$partitions_cnt;
        }
    }
    }
}


my $isubmit = 0;
sub handle_job {
################
# check and format the data about a single job
# input: hash with fields as described above
# output: SWF-like string describing the job

    my $job = shift;
    my $swf;

    #
    # submit, wait, and run (based on start and end)
    #
    my $submit = (defined($job->{submit})) ? $job->{submit} : -1;
    my $start  = (defined($job->{start}))  ? $job->{start}  : -1;
    my $end    = (defined($job->{end}))    ? $job->{end}    : -1;

    my ($wait, $run);

    ### ver 2 changed from <=0 to catch situations where the epoch does not
    ###       translate to time 0 due to local time
    $submit = -1 if ($submit <= 86400);
    $start  = -1 if ($start  <= 86400);
    $end    = -1 if ($end    <= 86400);

    ### ver 2 new logic to handle missing submit and/or start and/or end.
    ###       in particular, if only start is missing, assign run=end-submit
    ###       instead of wait=end-submit.
    ###       also, support direct input of runtime as an alternative.

    # wait time reported only if have reliable data
    if (($submit != -1) && ($start != -1)) {

    $wait = $start - $submit;

    if ($wait < 0) {
        bug_log( "wait_neg", "", "Negative wait times occured %d times; they were changed to 0
\n" );
        # this actually happens, probably unsynch'd clocks
        if ($wait < -3900) {
        bug_log( "wait_neg_huge", $job->{trace},
                 "  in %d cases the negative wait times were larger than 1 hr 5 min; these were changed to -1\n" );
        $wait = -1;
        }
        else {
        if ($wait < -60) {
            bug_log( "wait_neg_big", $job->{trace},
                 "  in %d cases negative wait times were larger than 1 min\n" );
        }
        $submit = $start;    # debatable; was like this in previous version
        $wait = 0;
        }
    }
    }
    else {
    # wait not useful for simulations, so no reason to improvise if data is missing
    $wait = -1;
    }

    # try to approximate if missing start time
    if ($start == -1) {

    bug_log( "start_undef", $job->{trace}, "%d jobs had undefined start times\n" );

    if ($submit != -1) {
        $start = $submit;    # used to approximate runtime
        bug_log( "start_undef_submit", $job->{trace},
             "  in %d cases the undefined start times were replaced by the submit time\n" );
    }
    }

    # report run time based on reliable or approximate data
    if (($start != -1) && ($end != -1)) {

    $run = $end - $start;
    if ($run < 0) {
        bug_log( "run_neg", "", "Negative run times occured %d times; they were changed to 0\n" );
        # this actually happens, probably unsynch'd clocks
        if ($run < -3900) {
        bug_log( "run_neg_huge", $job->{trace},
                 "  in %d cases the negative run times were larger than 1 hr 5 min; these were changed to -1\n" );
        $run = -1;
        }
        else {
        if ($run < -60) {
            bug_log( "run_neg_big", $job->{trace},
                 "  in %d cases negative run times were larger than 1 min\n" );
        }
        $run = 0;
        }
    }
    }
    else {
    $run = (defined($job->{run})) ? $job->{run} : -1;
    }

    # record additional missing data
    if ($end == -1) {

    bug_log( "end_undef", $job->{trace}, "%d jobs had undefined end times\n" );
    }

    # try to approximate submit because used to sort the log
    if ($submit == -1) {

    bug_log( "submit_undef", $job->{trace}, "%d jobs had undefined submit times\n" );

    if ($start != -1) {
        $isubmit = $submit = $start;
        bug_log( "submit_undef_start", $job->{trace},
             "  in %d cases the undefined submit times were replaced by the start time\n" );
    }
    elsif ($end != -1) {
        $isubmit = $submit = $end;
        bug_log( "submit_undef_end", $job->{trace},
             "  in %d cases the undefined submit times were replaced by the end time\n" );
    }
    else {
        bug_log( "submit_undef_none", $job->{trace},
             "  in %d cases jobs with undefined submit times did not have start or end times;
        these jobs appear in the converted file in a location that corresponds to the previous job\n" );
        # leave $isubmit as it was from previous job
    }
    }
    else {
    $isubmit = $submit;    # used for sorting
    }

    ### end ver 2 modification


    $swf = sprintf("%d %6d %6d ", $submit, $wait, $run);

    #
    # number of processors
    #
    my $procs = (defined( $job->{procs} )) ? $job->{procs} : -1;

    if ($procs == 0) {
    $procs = -1;
    bug_log( "proc_zero", "", "%d jobs were recorded as using 0 processors; this was changed to -1\n" );
    # check
    if (defined( $job->{status} )) {
        if (($job->{status} == 0) || ($job->{status} == 5)) {
        bug_log( "proc_zero_fail", "", "  of the jobs using 0 processors, %d had \"failed\" status\n" );
        }
        if ($job->{status} == 1) {
        # successful job with 0 processors -- this shouldn't happen
        bug_log( "proc_zero_success", $job->{trace}, "  of the jobs using 0 processors, %d had \"success\" status\n" );
        }
    }
    }

    if (($max_procs > 0) && ($procs > $max_procs)) {
    bug_log( "proc_too_many", $job->{trace}, "%d jobs were recorded as using more processors than the machine size; this was changed to the machine size\n" );
    $procs = $max_procs;
    }

    $swf .= sprintf("%4d ", $procs);

    #
    # CPU time
    #
    my $cpu = (defined( $job->{cpu} )) ? $job->{cpu} : -1;

    if ($cpu == 0) {
    $cpu = -1;
    bug_log( "cpu_zero", "", "%d jobs were recorded as using 0 CPU time; this was changed to -1\n" );
    }
    if (($cpu > $run) && ($run != -1)) {
    bug_log( "cpu_gt_run", $job->{trace}, "%d jobs had an average CPU time higher than their runtime\n" );
    }

    if ($cpu == -1) {
    $swf .= "    -1 ";
    }
    elsif ($cpu < 1000) {
    $swf .= sprintf("%6.2f ", $cpu);
    }
    else {
    $swf .= sprintf("%6d ", $cpu);
    }

    #
    # memory usage
    #
    my $mem = (defined( $job->{mem} )) ? $job->{mem} : -1;

    if ($mem == 0) {
    $mem = -1;
    bug_log( "mem_zero", $job->{trace}, "%d jobs were recorded as using 0 memory; this was changed to -1\n" );
    }

    $swf .= sprintf("%5d ", $mem);

    #
    # requested number of processors
    #
    my $req_procs = (defined( $job->{req_procs} )) ? $job->{req_procs} : -1;

    if ($req_procs == 0) {
    $req_procs = -1;
    bug_log( "proc_req_zero", "", "%d jobs were recorded as requesting 0 processors; this was changed to -1\n" );
    }
    if (($procs < $req_procs) && ($procs != -1)) {
    bug_log( "proc_got_less", "", "%d jobs got less processors than they requested\n" );
    }
    if (($procs > $req_procs) && ($req_procs != -1)) {
    bug_log( "proc_got_more", "", "%d jobs got more processors than they requested\n" );
    }

    $swf .= sprintf("%4d ", $req_procs);

    #
    # requested runtime
    #
    my $req_time = (defined( $job->{req_time} )) ? $job->{req_time} : -1;

    if ($req_time == 0) {
    $req_time = -1;
    bug_log( "run_req_zero", "", "%d jobs were recorded as requesting 0 runtime; this was changed to -1\n" );
    }
    if (($run > $req_time) && ($req_time != -1)) {
    bug_log( "run_got_more", "", "%d jobs got more runtime than they requested\n" );
    if ($run > $req_time + 60) {
        bug_log( "run_got_much_more", "",
                 "  in %d cases the extra runtime was larger than 1 min\n" );
    }
    }

    $swf .= sprintf("%6d ", $req_time);

    #
    # requested memory
    #
    my $req_mem = (defined( $job->{req_mem} )) ? $job->{req_mem} : -1;

    if ($req_mem == 0) {
    $req_mem = -1;
    bug_log( "mem_req_zero", "", "%d jobs were recorded as requesting 0 memory; this was changed to -1\n" );
    }
    if (($mem > $req_mem) && ($req_mem != -1)) {
    bug_log( "got_more_mem", "", "%d jobs got more memory than they requested\n" );
    }

    $swf .= sprintf("%5d ", $req_mem);

    #
    # job status
    #
    my $status = (defined( $job->{status} )) ? $job->{status} : -1;

    $swf .= sprintf("%2d ", $status);

    #
    # user, group, and app IDs have been anonymized
    #
    my $user = (defined( $job->{user} )) ? $job->{user} : -1;

    if ($users_cnt <= 1) {
    $user = -1;
    }

    $swf .= sprintf("%3d ", $users{$user});


    my $group = (defined( $job->{group} )) ? $job->{group} : -1;

    if ($groups_cnt <= 1) {
    $group = -1;
    }

    $swf .= sprintf("%3d ", $groups{$group});


    my $app = (defined( $job->{app} )) ? $job->{app} : -1;

    if ($apps_cnt <= 1) {
    $app = -1;
    }

    $swf .= sprintf("%3d ", $apps{$app});

    #
    # same applies for queue and partition
    #
    my $queue = (defined( $job->{queue} )) ? $job->{queue} : -1;

    if ($queues_cnt <= 1) {
    $queue = -1;
    }

    $swf .= sprintf("%2d ", $queues{$queue});


    my $partition = (defined( $job->{partition} )) ? $job->{partition} : -1;

    if ($partitions_cnt <= 1) {
    $partition = -1;
    }

    $swf .= sprintf("%2d ", $partitions{$partition});

    #
    # assume no data on preceding job
    #
    $swf .= "-1 -1\n";

    #
    # record minimal submit/start time = zero in the log, and maximal end time
    #
    if ((($isubmit != -1) && ($isubmit < $zero)) || ($zero <= 0)) {
    $zero = $isubmit;
    }

    if (($end == -1) && ($run != -1)) {
    if ($submit != -1) {
        $end = $submit + $run;
    }
    if ($start != -1) {
        $end = $start + $run;
    }
    }
    if ($end > $maxtime) {
    $maxtime = $end;
    }

    return ($isubmit, $swf);
}


sub print {
###########
# print out the SWF file, including header obtained from parser.
# also print conversion log.
    my ($header, $queue_desc, $partition_desc) = @_;

    printf $header;

    print_list(\%queues,     $queue_desc,     "Queue");

    print_list(\%partitions, $partition_desc, "Partition");

    print_workload();

    print_translations();

    print_bug_summary();
}


sub print_list {
################
# append list of queues or partitions with their descriptions to the header
    my ($hash, $desc, $label) = @_;

    my (@item_list, $item, $item_cnt, $max_len);

    $item_cnt = $max_len = 0;
    foreach $item (keys %$hash) {

    # turn  $hash{<name>}==<number>  into  $item_list[<number>]==<name>
    if ($item eq "-1") {
        next;
    }
    if (! defined($item_list[ $hash->{$item} ])) {
        $item_cnt++;
    }
    $item_list[ $hash->{$item} ] = $item;

    my $len = length($item);
    $max_len = $len if ($max_len < $len);
    }

    if ($item_cnt > 0) {
        printf("; Max%ss: %d\n", $label, $item_cnt);
    }

    my %seen;
    $seen{-1} = 1;
    foreach $item (sort {$a<=>$b;} values %$hash) {
    if ($seen{$item}) {
        # avoid repetition if several queues map together (e.g. for variants of "interactive")
        next;
    }

    my $desc = (defined($desc->[$item])) ? $desc->[$item] : "";

    if ($item eq $item_list[$item]) {
        printf("; %s: %2d  %s\n", $label, $item, $desc);
    }
    else {
        printf("; %s: %2d %-".$max_len."s %s\n", $label, $item, $item_list[$item], $desc);
    }

    $seen{$item} = 1;
    }
}


sub print_workload {
####################
# sort and print the jobs
# note that in principle there can be multiple jobs at the same time

    print ";\n";

    my $serial_num;

    foreach my $time (sort {$a <=> $b;} keys %jobs_to_print) {

    my @joblist = split(/\n/, $jobs_to_print{$time});

    foreach my $job (@joblist) {

        # adjust arrival time to start from 0
        my ($submit, $rest) = split(/ /, $job, 2);
        if ($submit != -1) {
        $submit -= $zero;
        }

        printf("%5d %8d %s\n", ++$serial_num, $submit, $rest);
    }
    }
}


############################
#
# ERROR LOGGING
#

my %bug_list;
my %bug_msg;
my $first_bug = 1;

sub open_bug_log {
##################
# open log file

    open LOG, ">convert.log" || die "can't open bug-log file";

    printf(LOG "Conversion done on %s\n",
       strftime("%A, %d %b %Y, at %H:%M:%S", localtime()));
    printf(LOG "Using convert2swf.pm version no. %s\n\n", $VERSION);
}

sub print_format_bugs {
#######################
# dump the problems found during parsing
    my $format_bugs = shift;

    if ($format_bugs) {
    printf LOG "FORMAT BUGS:\n\n$format_bugs\n\n";
    }
    else {
    printf LOG "NO FORMAT BUGS!\n\n"
    }
}

sub bug_log {
#############
# record a problem situation found during the conversion
# msg is a format string for printf, including a %d for the number of occurences

    my ($bug, $line, $msg) = @_;

    if ($line ne "") {

    if ($first_bug) {
        printf LOG "CONVERSION BUGS:\n\n";
        $first_bug = 0;
    }

    printf LOG "$bug at $line\n";
    }

    $bug_list{$bug}++;
    $bug_msg{$bug} = $msg;
}

sub print_translations {
########################
# dump translation tables used in sanitization

    printf LOG "\nUser translations:\n";
    foreach my $i (keys %users) {
    if ($i eq "-1") {
        next;
    }
    printf LOG "  $i => $users{$i}\n";
    }

    printf LOG "\nGroup translations:\n";
    foreach my $i (keys %groups) {
    if ($i eq "-1") {
        next;
    }
    printf LOG "  $i => $groups{$i}\n";
    }

    printf LOG "\nApplication translations:\n";
    foreach my $i (keys %apps) {
    if ($i eq "-1") {
        next;
    }
    printf LOG "  $i => $apps{$i}\n";
    }

    printf LOG "\nQueue translations:\n";
    foreach my $i (keys %queues) {
    if ($i eq "-1") {
        next;
    }
    printf LOG "  $i => $queues{$i}\n";
    }

    printf LOG "\nPartition translations:\n";
    foreach my $i (keys %partitions) {
    if ($i eq "-1") {
        next;
    }
    printf LOG "  $i => $partitions{$i}\n";
    }
    printf LOG "\n\n";
}

sub print_bug_summary {
#######################
# dump statistics of all recorded problems

    my $bug;

    if ($first_bug) {
    printf LOG "NO SERIOUS CONVERSION BUGS!\n";
    }

    printf LOG "\n";

    foreach $bug (sort keys %bug_list) {
    printf(LOG $bug_msg{$bug}, $bug_list{$bug});
    }
}

1;
