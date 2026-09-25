# Monitoring chorus

Every chorus process serves its metrics itself.

## Configuration

```yaml
metrics:
  enabled: true
  port: 9090
```

## The endpoints

If metrics are enabled in the config, each process serves the following endpoints
on the configured port:

| path             | what                                                    |
|------------------|---------------------------------------------------------|
| `/metrics`       | prometheus text format                                  |
| `/health`        | up                                                      |
| `/ready`         | up and not shutting down                                |
| `/version`       | version, commit, build date, app and app id             |
| `/debug/pprof/`  | heap, goroutine, mutex, block, cpu and trace profiles   |

## What is exported

Every metric describes itself in `/metrics`; this is the map of the families
and the labels that matter when querying them.

**Proxy requests.** `proxy_requests_total{method}` and
`proxy_response_status{status}` count what clients asked of the s3 proxy, the
second by the status they were answered with. `proxy_storage_status_total`
counts the same statuses one step earlier, as the storage gave them, by
`storage` and `status`: the middleware behind `proxy_response_status` cannot
see which storage was asked, and also counts the requests that reached none.
Chorus treats every status outside 200, 204 and 206 as an error, so a 404 or
503 from a storage ends the request early and is counted on that path.
`proxy_request_duration_seconds{method,storage}` is the request as the client
saw it, both transfers included, so a slow client makes it grow;
`proxy_request_internal_duration_seconds` ends when the storage answered and
is the one to judge a storage by. `proxy_destination_reads_total`,
`proxy_destination_read_fallbacks_total` and
`proxy_destination_reads_skipped_total{reason}` follow reads served from the
replication destination, and `proxy_head_bucket_cache_total{result}` the
bucket existence cache.

**Storage requests.** `storage_requests_total{flow,storage,method,status}`
counts the api calls chorus makes, with `status` one of `ok`, `client_error`,
`server_error`, `network_error` or `internal_error` — a request that failed is
counted, not skipped. The last two are worth keeping apart: `network_error` is
a storage that could not be reached, while `internal_error` is a call that
never left this process or was cancelled, and only the first says anything
about the storage. `storage_request_duration_seconds` carries the same labels
and times a call that failed as well, so an average can be read per outcome.
`storage_requests_in_flight{storage,method}` is what a storage currently owes
an answer for, `storage_http_duration_seconds` covers the sdk
calls that never go through the proxy, such as acl and tag sync, and
`storage_bucket_bytes_upload` / `_download{flow,storage,bucket}` the bytes
moved.

**Connections.** `storage_connections_opened_total`, `_closed_total` and the
`storage_connections_open` gauge, by `storage` and `layer`, from an
instrumented dialer in the per-storage transport. rclone builds its own
transport, so its pooling shows up as `rclone_fs_cache_entries` instead, one
entry per cached Fs.

**Queues, by queue.** A collector reads the asynq inspector at scrape time:
`queue_tasks{queue,state}` with the state one of pending, active, scheduled,
retry, archived, completed, aggregating; `queue_oldest_pending_seconds`;
`queue_memory_usage_bytes`; `queue_paused`. A queue is named
`<prefix>:<from>:<from bucket>:<to>:<to bucket>`, so this is the raw view, one
series per queue and state.

**Queues, by replication.** The same backlog aggregated to the replication and
its phase — `init` for the copies the listing found, `event` for what keeps
the destination up to date afterwards — labelled `from`, `from_bucket`, `to`,
`to_bucket`:

| metric                                | what                                                      |
|---------------------------------------|-----------------------------------------------------------|
| `replication_queue_pending`           | tasks left: new, in progress, and waiting for a retry     |
| `replication_queue_processed_total`   | tasks finished                                            |
| `replication_queue_retry`             | tasks that failed and will be retried                     |
| `replication_queue_failed`            | tasks that gave up after their last retry                 |
| `replication_queue_latency_seconds`   | age of the oldest waiting task                            |
| `replication_queue_memory_bytes`      | what the queues take in redis                             |

`replication_queue_latency_seconds{phase="event"}` is how far a destination is
behind its source, and `replication_queue_failed` is what needs someone to
look at it. These overlap the `queue_*` family on purpose: that one is per
queue and complete on states, this one is per replication and already summed
over the queues of a phase.

**Replications.** What the queues cannot say, same labels:

| metric                        | what                                                        |
|-------------------------------|-------------------------------------------------------------|
| `replication_listing_started` | 1 once the listing of the source bucket has begun           |
| `replication_init_done`       | 1 once the listing started and nothing is left to copy      |
| `replication_live_sync`       | 1 once the initial sync recorded itself as finished         |
| `replication_paused`          | 1 while at least one queue of the replication is paused     |
| `replication_archived`        | 1 for a replication that neither emits nor syncs events     |

`replication_init_done` is the state of the queues at the moment of the read
and can flip back when work arrives; `replication_live_sync` is the durable
fact and stays true.

All `replication_*` series are read on `metrics.redisReadInterval` (30s by
default, 0 turns them off), not per scrape, because reading them costs a few
redis round trips per replication. While a read fails the last state stays
exported and `replication_collect_errors_total` grows. The user is not part of
a queue name, so two replications that differ only in their user are exported
once.

**Workers, locks, rate limits.** `worker_processed_tasks_total`,
`worker_failed_tasks_total`, `worker_in_progress_tasks` and
`worker_task_duration_seconds` by `queue` and `task_type`;
`migration_copy_phase_duration_seconds{phase,from,to}` for the content, acl
and tag phases of an object copy; `lock_acquire_total{kind,result}`,
`lock_acquire_duration_seconds`, `lock_held{kind}` and
`lock_release_retry_total{kind}` with the kind from the lock key prefix;
`ratelimit_acquire_total{name,result}` and `ratelimit_in_use{name}` from both
semaphores; `agent_requests_total{status}`; and the rclone gauges
`rclone_calc_mem_usage`, `rclone_file_size_processing`,
`rclone_file_num_processing`.

## Sending it to graphite

Chorus serves its metrics and nothing else; what visits the endpoint and
hands the result on is the monitoring already installed on these hosts. The
files under `tools/mon` are shaped for it: a collector, the scrape it runs,
and the path rules both tools share.

```
tools/mon/collectors/chorus/metrics.pl   the collector
tools/mon/bin/chorus-scrape-carbon.pl    one scrape, as carbon plaintext
tools/mon/bin/chorus-log-backfill.pl     the backfill, see below
tools/mon/lib/GraphitePath.pm            the paths both of them build
```

**The collector.** Run with `--config` it names one run per directory under
`/opt/s3float`, which is what the bucket's render does:

```
$ collectors/chorus/metrics.pl --config
interval=60s args=--instance mig21
interval=60s args=--instance mig26
interval=60s args=--instance mig30
```

Nothing beyond the name of the directory goes into that config, so a
migration that is stopped, started, disabled or moved to another port never
needs a render; only a directory appearing or going away does. The run asks
the instance itself, through its own `print-config`, whether it serves
metrics and on which port, and scrapes it under `s3mig.<instance>`, where a
backfill of that migration's logs writes it too.

An instance that is not running, not built, has its metrics disabled or does
not answer ends the run with nothing written anywhere and an exit code of
zero: a prepared but idle instance is the ordinary state of a host between
migrations, a line per instance per interval saying so would bury the log,
and the manager takes a non-zero exit as a reason to tear its worker down.

**By hand**, the scrape works on its own, reading stdin or fetching:

```sh
curl -s http://127.0.0.1:9090/metrics |
    tools/mon/bin/chorus-scrape-carbon.pl --prefix s3mig.mig21
```

or, letting it fetch:

```sh
tools/mon/bin/chorus-scrape-carbon.pl --prefix s3mig.mig21 \
    --url http://127.0.0.1:9090/metrics | nc -q1 graphite.example.com 2003
```

The prefix names the instance, and the caller knows which instance it polled,
so nothing about the naming lives in the config of the instance itself. Two
instances cannot collide in the way they could if each carried a prefix of
its own that someone had to keep unique.

A path is built from the prefix, the metric name and the labels, sorted by
label name:

```
s3mig.mig21.storage_requests_total.flow.migration.method.GetObject.status.ok.storage.source
```

Label values reach the path with everything outside letters, digits,
underscores and dashes replaced, the colon of a task type or a queue name
included: carbon can be configured to refuse a metric name that carries one,
and it then drops the point without writing a file or answering back, so the
series simply never appears.

Things worth knowing before the first scrape:

- **The interval is the poller's.** It should match the finest retention step
  of the whisper storage schema: faster and whisper discards points, slower
  and it has gaps to fill.
- **Counters are passed on cumulative** and reset when a process restarts,
  which is what `nonNegativeDerivative()` expects. Rates belong in the query.
- **A whisper backend keeps a file per series.** Two labels grow without a
  bound the deployment controls: `bucket` on the byte counters, and `queue` on
  `queue_tasks` and the worker metrics, which is a series per queue and state.
  Both are in `--drop-labels` by default, which removes them and adds up the
  series that collapse into one. The per-replication view survives that either
  way, since `replication_queue_*` carries the replication in labels of its
  own rather than in a queue name.
- **Histogram buckets stay out** unless `--histogram-buckets` is given, since
  they are a series per bucket boundary and label combination. The sum and the
  count are always written and give the exact mean:
  `divideSeries(nonNegativeDerivative(...duration_seconds_sum),
  nonNegativeDerivative(...duration_seconds_count))`.
- **The runtime of the process is in there too**, as `go_*` and `process_*`.
  `--exclude go_,process_,promhttp_` leaves them out where they are not
  wanted.

Where these rules change, they change in the script, for every instance at
once and without a new chorus build. Both tools build their paths with
`lib/GraphitePath.pm`, so a scrape and a backfill of the same instance cannot
drift onto different series.

## Backfilling a migration that is already over

A migration that has finished has nothing left to scrape, but its logs hold
most of what the counters would have counted. `tools/mon/bin/chorus-log-backfill.pl`
reads them and writes the same graphite paths a running instance writes:

```sh
tools/mon/bin/chorus-log-backfill.pl --prefix s3mig.mig22.proxy \
    --carbon graphite.example.com:2003 /var/log/chorus/mig22*.log.gz
```

The prefix names one instance, the same one the scrape of a running instance
is given, so a backfilled series continues the live one instead of colliding
with it — which means one run per instance, with that instance's logs. Without
`--carbon` the carbon plaintext goes to stdout, which is the way to look at it
first. It reads plain files, `.gz` files, or stdin when no file is named.

Nothing is held for the whole run. What is kept is a running total per series
plus the intervals and copies inside a configurable lag-window. The memory
follows the window and the rate rather than the log: a 400 MB log of 2 million
copies goes through in 14 MB, and feeding it more files or bigger ones does
not change that.

A status-line is written when the terminal is free to carry it, which means
either `--carbon` or a redirected stdout. It is erased before the closing
summary, and nothing about it reaches a pipe or a file.

`--lag-window` is how long an interval stays open, 900s by default. Only the
completion of a copy says when its task was created, so what waited in an
interval is not known until those tasks have finished. A task that waited
longer than the window arrives after its interval was written and can no
longer add to it, which makes the backlog of a queue that stays behind by more
than the window read low. The age that queue reports stays true. Raising the
window costs memory in proportion.

`--count N` stops after N datapoints, for a first run that only needs to show
where the points land in the backend before the rest follows:

```sh
tools/mon/bin/chorus-log-backfill.pl --prefix s3mig.mig22.proxy \
    --count 20 --carbon graphite.example.com:2003 log.2026-09-07
```

Those N come off the front of the output, which is in time order, so a small
count covers the earliest intervals of every series that is active in them.

What it rebuilds:

| from                          | series                                                              |
|-------------------------------|---------------------------------------------------------------------|
| `proxy: request done`         | `proxy_requests_total`, `proxy_response_status`                     |
| `proxy: request failed`       | `proxy_requests_total`, and `proxy_storage_status_total` when a      |
|                               | storage answered. Logs written before that status was recorded have |
|                               | the request counted and its outcome missing                         |
|                               | `proxy_storage_status_total`                                        |
|                               | `proxy_request_duration_seconds` sum and count                      |
|                               | `proxy_request_internal_duration_seconds` sum and count             |
|                               | `storage_bucket_bytes_download` / `_upload`                         |
|                               | `storage_requests_in_flight`                                        |
|                               | `storage_connections_opened_total`                                  |
| `read from destination failed`| `proxy_destination_read_fallbacks_total`                            |
| `object sync: done`           | `replication_queue_processed_total` with `phase="event"`            |
|                               | `replication_queue_pending`, `replication_queue_latency_seconds`    |
| `migration obj copy: done`    | the same three with `phase="init"`                                  |
|                               | `migration_copy_phase_duration_seconds` sum and count               |
| either copy message           | `storage_bucket_bytes_download` off the source and `_upload` onto   |
|                               | the destination, under the flow of that phase                       |
| `process task failed …`       | `worker_failed_tasks_total`                                         |
|                               | `replication_queue_failed`, for the attempt that was the last one   |
|                               | `replication_queue_retry`, until the task is picked up again        |
| `starting obj copy`           | `worker_in_progress_tasks`, with the copy that ends it              |
|                               | `rclone_file_num_processing`, `rclone_file_size_processing`         |

A copy is logged by one of two handlers, the one that keeps a destination up
to date and the one that works off what the listing found, and the queue the
task came from says which phase it belongs to. Only the second logs how long
the content, the acl and the tag part of a copy took, which is where the copy
phase durations come from.

The queue depth and the lag are swept out of the completions rather than read
from a log line. A copy task is pending from the moment it was created — which
the task payload carries, to the nanosecond — until the moment the worker
finished it, so the finished tasks alone describe the queue over time: how many
were waiting at each step, and how old the oldest of them was. For a migration
that ran to the end that is the whole queue. Anything that never finished wrote
no completion and is in neither curve, so a migration that was abandoned
mid-flight reads as emptier than it was.

`replication_queue_failed` is counted rather than sampled: each dropped task
raises it by one and it is written as the running total, which is what the
gauge would have read as long as nobody cleared the archived tasks out in
between. Tasks that failed on the rate limit are logged at debug level, so a
log written at info level does not have them and `worker_failed_tasks_total`
reads lower there than the live one would.

A copy task whose payload holds no creation time still counts towards the
processed counter but waits nowhere, and the closing summary says how many of
those there were. Payloads of the initial migration stopped carrying one when
the copy task was reduced to what its queue name and task id do not already
say; the events kept theirs.

A gauge can be rebuilt where the logs say when something began and when it
ended, since what was busy across an interval boundary is what a live gauge
would have sampled there. That is where `storage_requests_in_flight`,
`worker_in_progress_tasks`, `replication_queue_retry` and the two rclone
gauges come from, each with a limit worth knowing:

- **In flight** is taken from `internal_duration`, so it counts a request from
  the moment chorus was left waiting on the storage, not from the moment the
  client asked.
- **In progress** and the **rclone gauges** only see tasks that log the start
  of a copy, which is what rclone does. Task types that never copy are missing
  from them, while the live gauges have every task.
- **Retry** counts a task from the attempt that failed until the next line
  that mentions it. A task still in its backoff when the log ends is in no
  interval at all, and one whose backoff started before the log did is counted
  from the first interval still open.

What cannot come back: what is only ever a measurement, with no beginning and
end to read. The queue collector's `queue_tasks` by state, its memory usage
and paused flags, `storage_connections_open`, the lock and rate limit
families, `rclone_calc_mem_usage`, and the `replication_*` flags —
`listing_started`, `init_done`, `live_sync`, `archived`. Histogram buckets are
gone too; only the sum and the count survive, which is enough for the mean and
not for a quantile.

Counters are written cumulative, starting at zero with the first interval of
the run, so `nonNegativeDerivative()` reads them the same way it reads the
live ones — the seam between a live series and a backfilled one looks like a
process restart, which that function already handles. Two things about the
whisper side are worth checking before a large run: the retention has to still
cover the period being written, since carbon silently drops points that fall
off the end of the archive, and `--step` has to match the finest retention
step, for the same reason it does for the live push.
