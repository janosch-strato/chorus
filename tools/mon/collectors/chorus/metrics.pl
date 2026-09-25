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

# The metrics of the chorus instances on this host, as a collector for the
# mon bucket: --config names one run per instance, and a run scrapes that
# instance and writes carbon plaintext for the manager to pass on.
#
# Every directory under the root gets a run, whether an instance is up in it
# or not. A migration that is stopped, started, disabled or given another
# port therefore needs no render: the run asks the instance itself where its
# metrics are, each time, and ends quietly where there is nothing to ask.
# Only a directory appearing or going away changes the config at all.
#
# Ending quietly means exactly that, with nothing written anywhere: an
# instance that is prepared but not running is the ordinary state of a host
# between migrations, and a line per instance per interval saying so would
# bury the log. It also matters that the exit code stays zero, since the
# manager takes anything else as a reason to tear its worker down, and one
# migration restarting is no reason to stop watching the others.
#
# The prefix an instance is written under is s3mig.<instance>, which is where
# its series already are: the dashboards and everything backfilled from the
# logs of finished migrations use it.

use strict;
use warnings;

use FindBin;
use Getopt::Long qw(:config posix_default no_ignore_case bundling);

# where the instances live, overridable so that this can be tried out
# somewhere other than a machine that runs them
my $ROOT = $ENV{CHORUS_ROOT} || '/opt/s3float';
my $SCRAPE = "$FindBin::Bin/../../bin/chorus-scrape-carbon.pl";
my $INTERVAL = '60s';

my $instance;
GetOptions(
	'config' => sub { print_config(); exit 0; },
	'instance|i=s' => \$instance,
) or exit 1;

# One run per directory, named after it. Nothing printed means nothing runs,
# which is what a host without chorus on it wants.
sub print_config {
	foreach my $dir (sort glob("$ROOT/*")) {
		-d $dir or next;
		my $id = $dir;
		$id =~ s{.*/}{};
		print "interval=$INTERVAL args=--instance $id\n";
	}
}

# The port an instance serves its metrics on, asked of the instance itself:
# the config it runs with is the one that answers, and it prints that config
# on demand without starting anything. Zero for an instance that is not
# there, or that keeps its metrics to itself.
sub metrics_port {
	my ($id) = @_;
	my $binary = "$ROOT/$id/bin/chorus.$id";
	my $config = "$ROOT/$id/etc/chorus/config.yaml";
	-x $binary && -e $config or return 0;

	open my $fh, '-|', $binary, '-config', $config, 'print-config'
		or return 0;
	my ($in_metrics, $enabled, $port) = (0, 0, 0);
	while (my $line = <$fh>) {
		chomp $line;
		# the sections below metrics carry ports of their own, so only
		# what is indented under this one counts
		if ($line =~ /\A\S/) {
			$in_metrics = $line =~ /\Ametrics:/ ? 1 : 0;
			next;
		}
		next if !$in_metrics;
		$enabled = 1 if $line =~ /\A\s+enabled:\s*true\s*\z/;
		$port = $1 if !$port && $line =~ /\A\s+port:\s*(\d+)\s*\z/;
	}
	close $fh;
	return $enabled ? $port : 0;
}

defined $instance or exit 1;
my $port = metrics_port($instance) or exit 0;

# The scrape says what it did on stderr and gives up loudly on a port that
# does not answer; here both are noise, so the child keeps its complaints to
# itself and the exit code is ours to give.
my $pid = fork();
defined $pid or exit 0;
if ($pid == 0) {
	open STDERR, '>', '/dev/null' or exit 127;
	exec $SCRAPE, '--prefix', "s3mig.$instance",
		'--url', "http://127.0.0.1:$port/metrics";
	exit 127;
}
waitpid $pid, 0;
exit 0;
