#!/usr/bin/perl
#
# Copyright © 2026 STRATO GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Rebuild what the metrics would have been from the logs chorus already
# wrote, and feed it to graphite under the paths the running instances use.
# For a migration that is over there is nothing left to scrape, but its logs
# hold most of what the counters would have counted.
#
# Usage: chorus-log-backfill.pl --prefix <graphite prefix> [options] [log...]
#   ./chorus-log-backfill.pl --prefix s3mig.mig22.proxy log.2026-09-07
#   ./chorus-log-backfill.pl --prefix s3mig.mig22.proxy --count 20 \
#       --carbon graphite.example.com:2003 log.2026-09-07   # a first look
#   ./chorus-log-backfill.pl --prefix s3mig.mig22.proxy --step 60 \
#       --carbon graphite.example.com:2003 /var/log/chorus/mig22*.log.gz
#   zcat mig22.log.gz | ./chorus-log-backfill.pl --prefix s3mig.mig22.proxy
#
# The prefix names one instance, the same one chorus-scrape-carbon.pl is
# given when that instance is polled, so the backfilled series continue the
# live ones instead of colliding with them. Logs of different instances
# therefore have to be fed in separate runs.
#
# Nothing is held for the whole run. The logs are in time order, so an
# interval can be written as soon as the reader has moved past it, and the
# points start flowing within a window of the first line. What is kept is one
# running total per series, the few intervals still inside the window, and
# the copies read during it. That makes the memory a function of the window
# and the rate, not of the log: tens of gigabytes go through in tens of
# megabytes, and the files can be fed in any number and any size.
#
# The window is how long an interval stays open, see --lag-window. Only the
# completion of a copy says when its task was created, so what was waiting in
# an interval is not known until the tasks that waited there have finished. A
# task that waited longer than the window arrives after its interval has been
# written and can no longer add to it; the backlog of a queue that stays
# behind by more than the window therefore reads low, while the age it
# reports stays true.
#
# Counters are emitted cumulative, the way the prometheus ones are, starting
# at zero with the first interval of the run. A dashboard that reads them
# with nonNegativeDerivative() sees the rate; the step from the live series
# into a backfilled one looks like a process restart, which that function
# already copes with.
#
# What can be rebuilt and what cannot is in docs/monitoring.md.

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Getopt::Long;
use GraphitePath;
use IO::Socket::INET;
use Time::HiRes qw(time);
use Time::Local qw(timegm_modern);

# The json parser of the system where there is one: the prefilter below keeps
# all but a fraction of the lines away from it, but that fraction is still
# millions of lines on a real log.
my $JSON = eval {
	require JSON::XS;
	JSON::XS->new()->utf8();
} || do {
	require JSON::PP;
	JSON::PP->new()->utf8();
};

# Log messages worth parsing. Checked as substrings before a line is handed
# to the json parser, which is what keeps this fast enough for tens of GB.
my @MARKERS = (
	'"proxy: request done"',
	'"proxy: request failed"',
	'"read from destination failed',
	'"object sync: done"',
	'"migration obj copy: done"',
	'"process task failed',
	'"starting obj copy"',
);

# The worker says this when a task has used its last retry. The attempts
# before it carry the same message with a different ending.
my $DROPPED = 'process task failed. task will be dropped';

# The phases of an object copy the worker times separately, as the field it
# logs them under and the value of the phase label.
my @COPY_PHASES = (
	[ 'copy_duration', 'copy' ],
	[ 'acl_duration',  'acl' ],
	[ 'tags_duration', 'tags' ],
);

# Queue name prefixes, as pkg/tasks names them, mapped to the phase label the
# replication metrics use.
my %PHASES = (
	migr_copy_obj => 'init',
	migr_list_obj => 'init',
	event         => 'event',
);

# The flow a copy of each phase is recorded under, as xctx names it: the
# bytes of a copy land in the same counters as the bytes of a proxied
# request, told apart by this label.
my %FLOWS = (init => 'migration', event => 'event');

my %opt = (
	prefix      => undef,
	step        => 60,
	carbon      => undef,
	'drop-labels' => 'bucket,queue',
	count       => 0,
	'lag-window' => 900,
);
GetOptions(\%opt, 'prefix=s', 'step=i', 'carbon=s', 'drop-labels=s',
	'count=i', 'lag-window=i') or usage();
defined $opt{prefix} or usage();

sub usage {
	print STDERR <<'END';
usage: chorus-log-backfill.pl --prefix <graphite prefix> [options] [log...]
  --prefix P      graphite prefix of the instance these logs are from
  --step N        seconds per point, match the whisper retention step
  --carbon H:P    send to this carbon plaintext receiver instead of stdout
  --drop-labels L labels to leave out of the paths, comma separated
  --count N       stop after N datapoints, for a first look
  --lag-window S  how long an interval stays open for the copies still
                  waiting in it
END
	exit 2;
}

my $PREFIX = $opt{prefix};
$PREFIX =~ s/\.+\z//;
my $STEP = $opt{step};
my $WINDOW = $opt{'lag-window'} > $STEP ? $opt{'lag-window'} : $STEP;
my $DROP = GraphitePath::drop_set($opt{'drop-labels'});
my $LIMIT = $opt{count};

# --- where the datapoints go ------------------------------------------------

my $CARBON;
my @BATCH;
my $WRITTEN = 0;
my $FULL = 0;

if (defined $opt{carbon}) {
	my ($host, $port) = split /:/, $opt{carbon}, 2;
	$port = 2003 if !defined $port || !length $port;
	$CARBON = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port,
		Proto => 'tcp', Timeout => 30)
		or die "unable to reach carbon at $opt{carbon}: $!\n";
}

sub carbon_flush {
	return if !@BATCH;
	print {$CARBON} join("\n", @BATCH), "\n";
	@BATCH = ();
}

sub emit {
	my ($path, $value, $stamp) = @_;
	return if $FULL;
	if ($LIMIT && $WRITTEN >= $LIMIT) {
		$FULL = 1;
		return;
	}
	$WRITTEN++;
	my $line = sprintf '%s %g %d', $path, $value, $stamp;
	if ($CARBON) {
		push @BATCH, $line;
		carbon_flush() if @BATCH >= 1000;
	} else {
		print "$line\n";
	}
}

# --- the line on the terminal -----------------------------------------------

# Off unless the terminal is free to carry it: with the datapoints going to a
# terminal as well there would be nothing to read. The clock is only read
# every so many lines, since a run reads millions of them and the answer is
# needed once a second.
my $PROGRESS = (-t STDERR) && ($CARBON || !(-t STDOUT)) ? 1 : 0;
my $PROGRESS_CHECK = 8192;
my $progress_pending = 0;
my $progress_last = 0;
my $progress_width = 0;

sub progress_step {
	return if !$PROGRESS;
	return if ++$progress_pending < $PROGRESS_CHECK;
	$progress_pending = 0;
	my $now = time();
	return if $now - $progress_last < 1.0;
	$progress_last = $now;
	my $line = sprintf '%.2f GB  proxy %d  copies %d  points %d',
		$main::READ_BYTES / 1e9, $main::STATS{proxy} || 0,
		$main::STATS{copies} || 0, $WRITTEN;
	printf STDERR "\r%-*s", $progress_width, $line;
	$progress_width = length $line if length $line > $progress_width;
}

sub progress_clear {
	return if !$PROGRESS || !$progress_width;
	print STDERR "\r", ' ' x $progress_width, "\r";
}

# --- the series -------------------------------------------------------------

# Cumulative series, kept as one running total per path plus the deltas of
# the intervals that are still open.
my %total;
my %open;
my %path_cache;

sub path_of {
	my ($metric, $labels) = @_;
	my $key = join "\0", $metric, map { "$_=$labels->{$_}" } sort keys %$labels;
	my $built = $path_cache{$key};
	if (!defined $built) {
		$built = GraphitePath::build($PREFIX, $metric, $labels, $DROP);
		$path_cache{$key} = $built;
	}
	return $built;
}

sub count {
	my ($bucket, $metric, $labels, $value) = @_;
	$value = 1 if !defined $value;
	$open{$bucket}->{path_of($metric, $labels)} += $value;
}

# Gauges counted from the stretches of time something was occupied. Whatever
# was busy from one moment to another is counted at every interval boundary
# in between, which is where a live gauge would have been sampled. Only two
# numbers per stretch are kept, so nothing is held per request or per task.
my %span_edges;
my %span_value;

sub boundary {
	my ($when) = @_;
	my $b = int($when / $STEP);
	$b++ if $b * $STEP < $when;
	return $b * $STEP;
}

sub span {
	my ($began, $ended, $metric, $labels, $weight) = @_;
	$weight = 1 if !defined $weight;
	my $floor = defined $main::NEXT_BUCKET ? $main::NEXT_BUCKET : 0;
	my $path = path_of($metric, $labels);
	$span_value{$path} = 0 if !exists $span_value{$path};
	my $first = boundary($began);
	$first = $floor if $floor > $first;
	my $last = boundary($ended);
	return 0 if $last <= $first;
	$span_edges{$path}->{$first} += $weight;
	$span_edges{$path}->{$last} -= $weight;
	return 1;
}

# --- the backlog of one replication ----------------------------------------

# The depth is the stretch each task spent waiting, counted like any other
# span. The age of the oldest one waiting needs the tasks themselves, since
# the one that has been waiting longest is not the one that finishes next.
# They are held in a heap by creation and dropped as they stop counting. A
# task is only known once it has finished, so the heap holds what was read
# during the window rather than the whole backlog, however deep that grew.
my %backlog;
my %backlog_path;

sub heap_push {
	my ($heap, $entry) = @_;
	push @$heap, $entry;
	my $i = $#$heap;
	while ($i > 0) {
		my $parent = int(($i - 1) / 2);
		last if $heap->[$parent]->[0] <= $heap->[$i]->[0];
		@$heap[$parent, $i] = @$heap[$i, $parent];
		$i = $parent;
	}
}

sub heap_pop {
	my ($heap) = @_;
	my $top = $heap->[0];
	my $last = pop @$heap;
	if (@$heap) {
		$heap->[0] = $last;
		my $i = 0;
		while (1) {
			my ($left, $right) = (2 * $i + 1, 2 * $i + 2);
			my $small = $i;
			$small = $left
				if $left <= $#$heap
				&& $heap->[$left]->[0] < $heap->[$small]->[0];
			$small = $right
				if $right <= $#$heap
				&& $heap->[$right]->[0] < $heap->[$small]->[0];
			last if $small == $i;
			@$heap[$small, $i] = @$heap[$i, $small];
			$i = $small;
		}
	}
	return $top;
}

sub backlog_of {
	my ($labels) = @_;
	my $key = join "\0", map { "$_=$labels->{$_}" } sort keys %$labels;
	my $entry = $backlog{$key};
	if (!defined $entry) {
		$entry = {
			heap    => [],
			pending => path_of('replication_queue_pending', $labels),
			latency => path_of('replication_queue_latency_seconds', $labels),
			labels  => {%$labels},
		};
		$backlog{$key} = $entry;
		$backlog_path{$entry->{pending}} = 1;
	}
	return $entry;
}

sub backlog_record {
	my ($labels, $created, $done) = @_;
	my $entry = backlog_of($labels);
	return if !span($created, $done, 'replication_queue_pending', $labels);
	heap_push($entry->{heap}, [$created, boundary($done)]);
}

# --- reading the logs -------------------------------------------------------

our $NEXT_BUCKET;
our $READ_BYTES = 0;
our %STATS;
my %running;
my %backoff;

sub bucket_of {
	my ($when) = @_;
	return int($when / $STEP) * $STEP;
}

# Seconds since the epoch from an RFC3339 stamp. Go writes nanoseconds, of
# which the first six are all that is kept here.
my %TIME_CACHE;

sub parse_time {
	my ($text) = @_;
	my ($date, $frac, $zone) = $text =~
		/\A(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)\z/
		or die "unparsable timestamp: $text\n";
	my $seconds = $TIME_CACHE{"$date$zone"};
	if (!defined $seconds) {
		my ($y, $mo, $d, $h, $mi, $s) = $date =~
			/\A(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\z/;
		$seconds = timegm_modern($s, $mi, $h, $d, $mo - 1, $y);
		if ($zone ne 'Z') {
			my ($sign, $zh, $zm) = $zone =~ /\A([+-])(\d\d):(\d\d)\z/;
			my $offset = $zh * 3600 + $zm * 60;
			$seconds += $sign eq '+' ? -$offset : $offset;
		}
		# one entry per second of log, which a long run would grow without
		# an end; the log moves forward, so the old ones are of no use
		%TIME_CACHE = () if keys %TIME_CACHE > 100_000;
		$TIME_CACHE{"$date$zone"} = $seconds;
	}
	return defined $frac ? $seconds + substr("0.$frac", 0, 8) : $seconds;
}

# Phase and replication labels from a queue name, which pkg/tasks builds as
# <prefix>:<from>:<from bucket>:<to>:<to bucket>. Undef for a queue that
# belongs to no replication, such as the api queue.
sub replication_labels {
	my ($queue) = @_;
	my @parts = split /:/, $queue;
	return undef if @parts != 5 || !exists $PHASES{$parts[0]};
	return {
		phase       => $PHASES{$parts[0]},
		from        => $parts[1],
		from_bucket => $parts[2],
		to          => $parts[3],
		to_bucket   => $parts[4],
	};
}

# Write every interval the reader has left behind by more than the window.
sub advance {
	my ($now) = @_;
	my $bucket = bucket_of($now);
	if (!defined $NEXT_BUCKET) {
		$NEXT_BUCKET = $bucket;
		return;
	}
	while ($NEXT_BUCKET + $WINDOW <= $bucket) {
		write_bucket($NEXT_BUCKET);
		$NEXT_BUCKET += $STEP;
		return if $FULL;
	}
}

sub write_bucket {
	my ($bucket) = @_;

	# a counter that stands still still has a value, so every series known
	# so far is written at every interval
	my $deltas = delete $open{$bucket};
	if ($deltas) {
		$total{$_} += $deltas->{$_} foreach keys %$deltas;
	}
	emit($_, $total{$_}, $bucket) foreach sort keys %total;

	foreach my $key (sort keys %backlog) {
		my $entry = $backlog{$key};
		my $heap = $entry->{heap};
		emit($entry->{pending}, span_value($entry->{pending}, $bucket),
			$bucket);
		heap_pop($heap) while @$heap && $heap->[0]->[1] <= $bucket;
		# a task read ahead of this interval is younger than anything
		# already waiting in it, so it only ever wins the heap when nothing
		# waits, and then its age is negative and clamped away
		my $oldest = @$heap ? $bucket - $heap->[0]->[0] : 0;
		emit($entry->{latency}, $oldest > 0 ? $oldest : 0, $bucket);
	}

	# the depth of a replication is a span too, and the loop above has
	# written it already
	foreach my $path (sort keys %span_value) {
		next if $backlog_path{$path};
		emit($path, span_value($path, $bucket), $bucket);
	}
}

sub span_value {
	my ($path, $bucket) = @_;
	my $edges = $span_edges{$path};
	if ($edges && exists $edges->{$bucket}) {
		$span_value{$path} += delete $edges->{$bucket};
	}
	my $value = $span_value{$path} || 0;
	return $value > 0 ? $value : 0;
}

# Drain what is still inside the window once the logs end.
sub finish {
	return if !defined $NEXT_BUCKET;
	my $last = $NEXT_BUCKET;
	foreach my $bucket (keys %open) {
		$last = $bucket if $bucket > $last;
	}
	foreach my $path (keys %span_edges) {
		foreach my $bucket (keys %{$span_edges{$path}}) {
			$last = $bucket if $bucket > $last;
		}
	}
	while ($NEXT_BUCKET <= $last && !$FULL) {
		write_bucket($NEXT_BUCKET);
		$NEXT_BUCKET += $STEP;
	}
}

# The counters a finished proxy request feeds, which is most of what the
# proxy exports while it runs.
sub proxy_request {
	my ($entry) = @_;
	my $when = parse_time($entry->{time});
	my $bucket = bucket_of($when);
	my $method = $entry->{method} || '';
	my $storage = $entry->{stor_name} || 'none';

	count($bucket, 'proxy_requests_total', {method => $method});
	if (defined $entry->{status}) {
		my $status = "$entry->{status}";
		count($bucket, 'proxy_response_status', {status => $status});
		# the same status again, this time against the storage that
		# answered with it, which the middleware counting the one above
		# cannot see
		count($bucket, 'proxy_storage_status_total',
			{storage => $storage, status => $status});
	}

	# zerolog writes durations in milliseconds, the metrics are in seconds
	foreach my $pair (['duration', 'proxy_request_duration_seconds'],
		['internal_duration', 'proxy_request_internal_duration_seconds']) {
		my ($field, $metric) = @$pair;
		next if !defined $entry->{$field};
		my $labels = {method => $method, storage => $storage};
		count($bucket, $metric . '_sum', $labels, $entry->{$field} / 1000);
		count($bucket, $metric . '_count', $labels);
	}

	# every connection the request got that was not taken from the idle
	# pool was opened for it
	my $opened = ($entry->{conns} || 0) - ($entry->{conns_reused} || 0);
	count($bucket, 'storage_connections_opened_total',
		{storage => $storage, layer => 'http'}, $opened) if $opened > 0;

	# the request occupied the storage for as long as chorus was waiting on
	# it, which is what the in flight gauge counts
	span($when - $entry->{internal_duration} / 1000, $when,
		'storage_requests_in_flight',
		{storage => $storage, method => $method})
		if defined $entry->{internal_duration};

	my $size = $entry->{bytes};
	return if !$size;
	my $labels = {flow => $entry->{flow} || '', storage => $storage,
		bucket => $entry->{bucket} || ''};
	if ($method eq 'GetObject') {
		count($bucket, 'storage_bucket_bytes_download', $labels, $size);
	} elsif ($method eq 'PutObject' || $method eq 'UploadPart') {
		count($bucket, 'storage_bucket_bytes_upload', $labels, $size);
	}
}

# A request that ended in an error. The status is there when a storage
# answered with one; a request that reached no storage has none, and only the
# count of it survives.
sub proxy_failed {
	my ($entry) = @_;
	my $bucket = bucket_of(parse_time($entry->{time}));
	my $storage = $entry->{stor_name} || 'none';
	count($bucket, 'proxy_requests_total', {method => $entry->{method} || ''});
	return if !$entry->{status};
	count($bucket, 'proxy_storage_status_total',
		{storage => $storage, status => "$entry->{status}"});
}

# The size of the object a copy moved, which the two handlers report in two
# places.
sub copy_size {
	my ($entry) = @_;
	return $entry->{obj_size} if defined $entry->{obj_size};
	my $payload = $entry->{task_payload};
	return $payload->{ObjSize} if $payload && defined $payload->{ObjSize};
	return 0;
}

# A copy the worker picked up. Nothing is written yet: how long it was
# running is only known once it ends.
sub task_started {
	my ($entry) = @_;
	my $task = $entry->{task_id};
	$running{$task} = parse_time($entry->{time}) if defined $task;
}

# A task that stopped running or stopped waiting for a retry, which closes
# whichever stretch it was in.
sub task_ended {
	my ($entry, $when, $size) = @_;
	my $task = $entry->{task_id};
	return if !defined $task;

	my $began = delete $running{$task};
	if (defined $began) {
		my $task_type = $entry->{task_type} || '';
		span($began, $when, 'worker_in_progress_tasks',
			{task_type => $task_type});
		span($began, $when, 'rclone_file_num_processing', {});
		span($began, $when, 'rclone_file_size_processing', {}, $size)
			if $size;
	}

	my $waited = delete $backoff{$task};
	return if !defined $waited;
	my $labels = replication_labels($entry->{task_queue} || '');
	span($waited, $when, 'replication_queue_retry', $labels) if $labels;
}

# The content, acl and tag phases of a copy, which the initial migration
# times one by one and logs with the copy.
sub copy_phases {
	my ($entry, $labels, $finished) = @_;
	my $bucket = bucket_of($finished);
	foreach my $pair (@COPY_PHASES) {
		my ($field, $phase) = @$pair;
		next if !defined $entry->{$field};
		my $phase_labels = {phase => $phase, from => $labels->{from},
			to => $labels->{to}};
		count($bucket, 'migration_copy_phase_duration_seconds_sum',
			$phase_labels, $entry->{$field} / 1000);
		count($bucket, 'migration_copy_phase_duration_seconds_count',
			$phase_labels);
	}
}

# A copy the worker finished, in either phase: the event that keeps a
# destination up to date, and the one the initial listing found. They are two
# log messages of two handlers, and the queue the task came from says which
# phase it belongs to.
sub object_copied {
	my ($entry) = @_;
	my $labels = replication_labels($entry->{task_queue} || '');
	if (!$labels) {
		$STATS{skipped}++;
		return;
	}
	my $finished = parse_time($entry->{time});
	count(bucket_of($finished), 'replication_queue_processed_total', $labels);
	$STATS{copies}++;

	copy_phases($entry, $labels, $finished);
	my $size = copy_size($entry);
	task_ended($entry, $finished, $size);
	if ($size) {
		# read off one storage and written to the other, the way the copy
		# itself records it
		my $flow = $FLOWS{$labels->{phase}};
		my $bucket = bucket_of($finished);
		count($bucket, 'storage_bucket_bytes_download',
			{flow => $flow, storage => $labels->{from},
				bucket => $labels->{from_bucket}}, $size);
		count($bucket, 'storage_bucket_bytes_upload',
			{flow => $flow, storage => $labels->{to},
				bucket => $labels->{to_bucket}}, $size);
	}

	my $created = $entry->{task_payload}
		? $entry->{task_payload}->{CreatedAt} : undef;
	if (!$created) {
		# a copy task carries no creation time since its payload was
		# stripped, so this one is counted but never waited anywhere
		$STATS{undated}++;
		return;
	}
	backlog_record($labels, parse_time($created), $finished);
}

# An attempt at a task that ended in an error. Every one of them counts as a
# failure the way the worker middleware counts it; the last one, where the
# task is dropped, is also what leaves it archived in its queue for good.
sub task_failed {
	my ($entry) = @_;
	my $when = parse_time($entry->{time});
	my $bucket = bucket_of($when);
	my $queue = $entry->{task_queue} || '';
	count($bucket, 'worker_failed_tasks_total',
		{queue => $queue, task_type => $entry->{task_type} || ''});
	$STATS{failures}++;

	# the attempt is over either way, and unless it was the last one the
	# task now waits in its backoff until it is picked up again
	task_ended($entry, $when, 0);
	my $task = $entry->{task_id};
	my $dropped = ($entry->{message} || '') eq $DROPPED;
	$backoff{$task} = $when if defined $task && !$dropped;
	return if !$dropped;

	my $labels = replication_labels($queue);
	return if !$labels;
	# counted rather than sampled: a dropped task stays archived in its
	# queue, so the running total is what the gauge would have read, as long
	# as nobody cleared them out in between
	count($bucket, 'replication_queue_failed', $labels);
	$STATS{dropped}++;
}

sub read_stream {
	my ($fh) = @_;
	LINE: while (my $line = <$fh>) {
		$READ_BYTES += length $line;
		progress_step();
		my $wanted = 0;
		foreach my $marker (@MARKERS) {
			if (index($line, $marker) >= 0) {
				$wanted = 1;
				last;
			}
		}
		next LINE if !$wanted;

		my $entry = eval { $JSON->decode($line) };
		if (!$entry) {
			# a rotated log can start mid line, and a truncated one end
			# mid line
			$STATS{unparsed}++;
			next LINE;
		}

		my $ok = eval {
			my $message = $entry->{message} || '';
			if ($message eq 'proxy: request done') {
				advance(parse_time($entry->{time}));
				proxy_request($entry);
				$STATS{proxy}++;
			} elsif ($message eq 'proxy: request failed') {
				advance(parse_time($entry->{time}));
				proxy_failed($entry);
				$STATS{proxy_failed}++;
			} elsif ($message eq 'object sync: done'
				|| $message eq 'migration obj copy: done') {
				advance(parse_time($entry->{time}));
				object_copied($entry);
			} elsif ($message eq 'starting obj copy') {
				advance(parse_time($entry->{time}));
				task_started($entry);
			} elsif (index($message, 'process task failed') == 0) {
				advance(parse_time($entry->{time}));
				task_failed($entry);
			} elsif (index($message, 'read from destination failed') == 0) {
				my $when = parse_time($entry->{time});
				advance($when);
				count(bucket_of($when),
					'proxy_destination_read_fallbacks_total',
					{storage => $entry->{destination_storage} || ''});
				$STATS{fallbacks}++;
			}
			1;
		};
		$STATS{unparsed}++ if !$ok;
		return if $FULL;
	}
}

# --- the run ----------------------------------------------------------------

if (@ARGV) {
	foreach my $path (@ARGV) {
		last if $FULL;
		my $fh;
		if ($path =~ /\.gz\z/) {
			open $fh, '-|', 'gzip', '-dc', $path
				or die "unable to read $path: $!\n";
		} else {
			open $fh, '<', $path or die "unable to read $path: $!\n";
		}
		read_stream($fh);
		close $fh;
	}
} else {
	read_stream(\*STDIN);
}
finish();
carbon_flush() if $CARBON;
close $CARBON if $CARBON;
progress_clear();

my $undated = $STATS{undated}
	? sprintf(' (%d of them without a creation time)', $STATS{undated}) : '';
printf STDERR "proxy requests: %d (%d failed), object copies: %d%s, "
	. "failed attempts: %d (%d dropped), destination fallbacks: %d, "
	. "unparsable lines: %d, lines of other queues: %d\n",
	$STATS{proxy} || 0, $STATS{proxy_failed} || 0, $STATS{copies} || 0,
	$undated, $STATS{failures} || 0, $STATS{dropped} || 0,
	$STATS{fallbacks} || 0, $STATS{unparsed} || 0, $STATS{skipped} || 0;
printf STDERR "datapoints written: %d%s\n", $WRITTEN,
	$FULL ? ' (limited by --count)' : '';
