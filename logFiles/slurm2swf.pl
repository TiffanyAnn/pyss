#!/usr/bin/perl

# Modified from the LLNL Thunder log converter: http://www.cs.huji.ac.il/labs/parallel/workload/l_llnl_thunder/index.html
# scan job execution log files from Slurm format, and use
# convert2swf_v2.pm to convert the data into the standard workload format.
#

use warnings;
use strict;
use Time::Local;
use POSIX;
use convert2swf_v3;


############################
#
# DO THE CONVERSION:
#

my $tz_str    = "US/Pacific";
my $proc_per_node = 64;
my $max_nodes = 2388;
my $max_procs = $max_nodes * $proc_per_node;

my @queues = ();

SWF::init( $tz_str, $max_procs, \@queues );

my $format_bugs;
my $jobs_data = get_jobs();

my ($zero, $maxtime) = SWF::convert( $jobs_data, $format_bugs );

my $header = generate_header( $zero, $maxtime, $max_procs );

SWF::print( $header );

################################
# the format of each entry includes the following space-separated fields:
#  0 JobId=<number>
#  1 UserId=<string>
#  2 Name=<string> - name of executable (script)
#  3 JobState=<status>
#  4 Partition=<string>
#  5 TimeLimit=<number> - in seconds
#  6 StartTime=<date and time>
#  7 EndTime=<date and time>
#  8 NNode=<number>
#  9 NodeCnt=<number>

my $job_cnt;
my $problem1;
my $problem2;
my $problem3;
my $problem4;

my $completed;
my $failed;
my $cancelled;
my $timeout;
my $other_status;
my $node_fail;

my $nalloc;

my $short;
my $shortfailed;

my %cores;
sub get_jobs {
##############
# parse input format and create a hash for each job

  my @list_of_jobs;
  $job_cnt   = 0; #counter for the jobs
  $problem1  = 0; #StartTime > EndTime
  $problem2  = 0; #jobs with zero recorded runtime, but a non-FAILED status
  $problem3  = 0; #jobs that exceeded their TimeLimit
  $problem4  = 0; #jobs who weren't allocated any processors, but are non-FAILED
  $short     = 0;
  $shortfailed = 0;

  #Status variables
  $completed    = 0;
  $failed       = 0;
  $cancelled    = 0;
  $timeout      = 0;
  $other_status = 0;
  $node_fail    = 0;

  $nalloc = 0; #Number of jobs who requested, but weren't allocated any (or insufficient number of) processors.

  #
  # scan trace and collect job info
  #
  while (<>) {
    if (/^\s*$/) {        # empty line
      next;
    }

    $_ =~ /\s*(.*)\s*/;        # get rid of white space at ends
    $_ = $1;
    my @line = split('\s+');    # split into fields

    if ($#line != 9) {
      $format_bugs .= "format_problem: $#line fields in $_\n";
      warn(">>>Format problem on $_\ngot $#line fields (should be 9)");
      next;
    }

    my %nj;

    #
    # set the desired fields as specified in convert2swf.pl
    #

    $nj{trace}  = $_;    # used for error messages


    my ($stime, $etime);
    my $j;

    #Start Time (two possible formats)
    my @begtmarr;
    if ($line[6] =~ /^StartTime=(\d+)(\W)(\d+)-(\d+):(\d+):(\d+)(.*)$/) {
      $begtmarr[5] = 2007 - 1900;
      $begtmarr[4] = $1 - 1;
    }
    elsif ($line[6] =~ /^StartTime=(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.*)$/) {
      $begtmarr[5] = $1 - 1900;
      $begtmarr[4] = $2 - 1;
    }
    else {
      warn ">>> can't parst start time $line[6]";
    }
    $begtmarr[3] = $3;
    $begtmarr[2] = $4;
    $begtmarr[1] = $5;
    $begtmarr[0] = $6;
    $stime = &timelocal(@begtmarr);
    if ($line[6] !~ /12\/31-16:00:00/) {    # logging bug!
      $nj{start}  = $stime;
    }
    #no data on submit time


    #End Time (two possible formats)
    my @endtmarr;
    if ($line[7] =~ /^EndTime=(\d+)(\W)(\d+)-(\d+):(\d+):(\d+)(.*)$/) {
      $endtmarr[5] = 2007 - 1900;
      $endtmarr[4] = $1 - 1;
    }
    elsif ($line[7] =~ /^EndTime=(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.*)$/) {
      $endtmarr[5] = $1 - 1900;
      $endtmarr[4] = $2 - 1;
    }
    $endtmarr[3] = $3;
    $endtmarr[2] = $4;
    $endtmarr[1] = $5;
    $endtmarr[0] = $6;
    $etime = &timelocal(@endtmarr);
    if ($line[7] !~ /12\/31-16:00:00/) {    # logging bug!
      $nj{end}    = $etime;
    }

    #wait = start - submit is identically 0 for this log;
    #run = end - start;

    #Job Status
    my $status_id;
    if ($line[3] =~ /^JobState=(.*)/) {
      my $status = $1;
      if ($status eq "COMPLETED") {
        $status_id = 1;
        $completed++;
      }
      elsif ($status eq "FAILED") {
        $status_id = 0;
        $failed++;
      }
      elsif ($status eq "CANCELLED") {
        $status_id = 5;
        $cancelled++;
      }
      elsif ($status eq "TIMEOUT") {
        $status_id = 0;
        $timeout++;
      }
      elsif ($status eq "NODE_FAIL") {
        $status_id = 0;
        $node_fail++;
      }
      else {
        $status_id = -1;
        $other_status++;
      }
      $nj{status} = $status_id;
    }

    if (defined($etime) && defined($stime) && (($etime - $stime) < 5)) {
      $short++;
      if ($status_id == 0) {
    $shortfailed++;
      }
    }

    #Allocated and Requested Processors
     if ($line[8] =~ /NNode=(.*)/) {
     my $allocated_procs = $1;
      $allocated_procs *= $proc_per_node;
      if ($allocated_procs!=0) {
        $nj{procs} = $allocated_procs;
      }
    }

    # Requested
    if ($line[9] =~ /^NodeCnt=(.*)/) {
    my $req_procs = $1 * $proc_per_node;
      if ($req_procs != 0) {
        $nj{req_procs} = $req_procs;
      }
    }

    #Requested Time (given in minutes)
    my $req_time;
    if ($line[5] =~ /^TimeLimit=(\d+)/) {
      $req_time = $1;
      if ($req_time != 4294967294) {    # unsigned -1 value
    $nj{req_time} = $req_time;
      }
    }

    #User ID
      my $user = $1;
      $nj{user}  = $user;

    #Executable (Application) Number
    if ($line[2] =~ /^Name=(.*)/) {
      my $app = $1;
      if ($app ne "") {
      $nj{app} = $app;
      }
    }

    #Partition
    if ($line[4] =~ /^Partition=(.*)/) {
      my $partition = $1;
      $nj{partition} = $partition;
    }

    #no data on CPU time
    #no data on memory
    #no data on requested memory
    #no data on queue
    #no data on group

    $list_of_jobs[$job_cnt++] = \%nj;



    #SOME CHECKS:


    #How many jobs were recorded with zero runtime, but a non-FAILED status?
    if (defined($nj{start}) && (defined($nj{end})) && ($etime == $stime) && ($status_id != 0)) {
      $problem2++;
    }

    #How many jobs exceeded their TimeLimit?
    if (defined($etime) && defined($stime) && defined($req_time) &&
    (($etime - $stime) > $req_time)) {
      $problem3++;
    }

    #Is the number of allocated processors sufficient?
    if (defined($nj{procs}) && defined($nj{req_procs}) && ($nj{procs} < $nj{req_procs})) {
      $nalloc++;
    }

  }

  $format_bugs .= "StartTime > EndTime:  $problem1\n";
  $format_bugs .= "RunTime is zero, but the job wasn't FAILED:  $problem2\n";
  $format_bugs .= "Number of jobs whose RunTime exceeds TimeLimit:  $problem3\n";
  $format_bugs .= "Number of jobs who weren't allocated any processors, but were non-FAILED: $problem4\n";
  $format_bugs .= "Number of jobs who requested, but weren't allocated any (or insufficient number of) processors: $nalloc\n";

  return \@list_of_jobs;
}


sub generate_header {
  my ($zero, $maxtime, $max_procs) = @_;

  my ($sec, $min, $hr, $mday, $mon, $year, $wday, $yday, $isdst);

  $header  = "; Version: 1.0\n";
  $header .= "; Computer: Cori\n";
  $header .= "; Installation: NERSC - Lawrence Berkeley National Laboratory\n";

  $header .= "; Information: http://www.nersc.gov/users/computational-systems/cori/\n";

  $header .= sprintf("; Conversion: %s\n",
                     strftime("%d %b %Y", localtime()));

  $header .= sprintf("; MaxJobs: %d\n", $job_cnt);
  $header .= sprintf("; MaxRecords: %d\n", $job_cnt);
  $header .= "; Preemption: No\n";

  $header .= sprintf("; UnixStartTime: %d\n", $zero);
  $header .= sprintf("; TimeZoneString: %s\n", $tz_str);

  $header .= sprintf("; StartTime: %s\n",
                     strftime("%a %b %2d %H:%M:%S %Z %Y", localtime($zero)));

  $header .= sprintf("; EndTime:   %s\n",
                     strftime("%a %b %2d %H:%M:%S %Z %Y", localtime($maxtime)));

  $header .= "; MaxNodes: $max_nodes\n";
  $header .= "; MaxProcs: $max_procs\n";

  $header .= "; Note: Scheduler is Slurm (https://computing.llnl.gov/linux/slurm/)\n";

  return $header;
}
