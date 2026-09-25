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

# Read the prometheus endpoint of one chorus instance and write what it says
# as carbon plaintext, one line per series, for whatever already polls these
# hosts to hand on to graphite.
#
# Usage: chorus-scrape-carbon.pl --prefix <graphite prefix> [options]
#   curl -s http://127.0.0.1:9090/metrics |
#       ./chorus-scrape-carbon.pl --prefix s3mig.mig21
#   ./chorus-scrape-carbon.pl --prefix s3mig.mig21 \
#       --url http://127.0.0.1:9090/metrics | nc -q1 graphite 2003
#
# The prefix names the instance and the caller knows which one it polled, so
# nothing about the instance has to be configured in chorus itself. Paths are
# built the way chorus-log-backfill.pl builds them, so a scrape and a
# backfilled period are the same series.
#
# Counters are passed on as they are, cumulative and resetting when the
# process restarts, which is what nonNegativeDerivative() in a graphite query
# expects. Histograms keep their sum and their count; their buckets are one
# series per boundary and stay out unless asked for.

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

use Getopt::Long;
use GraphitePath;
use HTTP::Tiny;

my %opt = (
	prefix        => undef,
	url           => undef,
	timeout       => 10,
	timestamp     => undef,
	'drop-labels' => 'bucket,queue',
	'histogram-buckets' => 0,
	exclude       => '',
);
GetOptions(\%opt, 'prefix=s', 'url=s', 'timeout=f', 'timestamp=i',
	'drop-labels=s', 'histogram-buckets', 'exclude=s') or usage();
defined $opt{prefix} or usage();

sub usage {
	print STDERR <<'END';
usage: chorus-scrape-carbon.pl --prefix <graphite prefix> [options]
  --prefix P           graphite prefix of the instance being polled
  --url U              metrics endpoint to read, stdin if not given
  --timeout S          seconds to wait for the endpoint
  --timestamp T        seconds since the epoch to stamp the points with
  --drop-labels L      labels to leave out of the paths, comma separated.
                       Series that fall together are added up
  --histogram-buckets  also write the buckets of histograms, one series per
                       boundary. The sum and the count give the exact mean
                       without them
  --exclude P,Q        metric name prefixes to skip, e.g. go_,process_
END
	exit 2;
}

my $PREFIX = $opt{prefix};
$PREFIX =~ s/\.+\z//;
my $DROP = GraphitePath::drop_set($opt{'drop-labels'});
my $KEEP_BUCKETS = $opt{'histogram-buckets'};
my @EXCLUDE = grep { length } split /,/, $opt{exclude};
my $STAMP = defined $opt{timestamp} ? $opt{timestamp} : time();

# name{label="value",...} value [timestamp], as the prometheus text format
# writes one sample.
my $SAMPLE = qr/
	\A([a-zA-Z_:][a-zA-Z0-9_:]*)		# name
	(?:\{(.*)\})?				# labels
	[ \t]+(\S+)				# value
	(?:[ \t]+-?[0-9]+)?[ \t]*\z		# the optional timestamp
/x;
my $LABEL = qr/([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/;
my %UNESCAPE = ('\\\\' => "\\", '\\"' => '"', '\\n' => "\n");

# The labels of one sample, with the escapes the text format uses.
sub labels_of {
	my ($text) = @_;
	my %labels;
	return \%labels if !defined $text;
	while ($text =~ /$LABEL/g) {
		my ($name, $value) = ($1, $2);
		$value =~ s/(\\\\|\\"|\\n)/$UNESCAPE{$1}/g;
		$labels{$name} = $value;
	}
	return \%labels;
}

# Every series of one scrape, as a path and a value. Series that fall
# together once their labels are dropped are added up, the way the sum of a
# counter over the buckets of a storage is still a counter.
my %total;
my @order;

sub read_stream {
	my ($fh) = @_;
	while (my $line = <$fh>) {
		next if index($line, '#') == 0;
		chomp $line;
		my ($name, $labels, $value) = $line =~ $SAMPLE
			or next;
		next if grep { index($name, $_) == 0 } @EXCLUDE;
		next if !$KEEP_BUCKETS && $name =~ /_bucket\z/;
		# carbon has nowhere to put these, and whisper reads them back as
		# a gap either way
		next if $value !~ /\A[-+]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][-+]?[0-9]+)?\z/;

		my $parsed = labels_of($labels);
		delete $parsed->{le} if !$KEEP_BUCKETS;
		my $path = GraphitePath::build($PREFIX, $name, $parsed, $DROP);
		push @order, $path if !exists $total{$path};
		$total{$path} += $value;
	}
}

if (defined $opt{url}) {
	my $http = HTTP::Tiny->new(timeout => $opt{timeout});
	my $response = $http->get($opt{url});
	$response->{success}
		or die "unable to read $opt{url}: $response->{status} "
			. "$response->{reason}\n";
	open my $fh, '<', \$response->{content}
		or die "unable to read the response: $!\n";
	read_stream($fh);
	close $fh;
} else {
	read_stream(\*STDIN);
}

printf "%s %g %d\n", $_, $total{$_}, $STAMP foreach @order;
printf STDERR "%d series\n", scalar @order;
