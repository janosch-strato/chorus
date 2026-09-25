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

# Prometheus names and labels turned into graphite paths. Used by both tools
# beside it, the scrape of a running instance and the backfill from the logs
# of a finished one, so that the two write the same series and a migration
# reads as one line across the seam.

package GraphitePath;

use strict;
use warnings;

# Escape one component of a path. Everything that is not a letter, a digit,
# an underscore or a dash goes, the colon of a task type included: carbon can
# be told to reject a metric name holding anything else, and it then drops
# the point without writing a file or saying so in its reply. A run of
# underscores collapses into one and a space separates.
sub sanitize {
	my ($text) = @_;
	my @out;
	my $previous_underscore = 0;
	foreach my $char (split //, $text) {
		if ($char eq ' ') {
			$char = '.';
		} elsif ($char !~ /\A[A-Za-z0-9_-]\z/) {
			$char = '_';
		}
		if ($char eq '_') {
			next if $previous_underscore;
			$previous_underscore = 1;
		} else {
			$previous_underscore = 0;
		}
		push @out, $char;
	}
	return join '', @out;
}

# The path of one series: the prefix, the metric, then the labels it kept,
# sorted by name and written as name and value.
sub build {
	my ($prefix, $metric, $labels, $drop) = @_;
	my @parts = ($prefix, sanitize($metric));
	my @kept = grep { !$drop->{$_} } keys %$labels;
	my $order = sub { "$_[0] $labels->{$_[0]}" };
	foreach my $name (sort { $order->($a) cmp $order->($b) } @kept) {
		push @parts, sanitize($name), sanitize($labels->{$name});
	}
	return join '.', @parts;
}

# The labels to leave out, from a comma separated list.
sub drop_set {
	my ($spec) = @_;
	return { map { $_ => 1 } grep { length } split /,/, $spec };
}

1;
