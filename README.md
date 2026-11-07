# Screamer... (S)panner (C)hange st(REAM) read(ER)

[![Test](https://github.com/anicoll/screamer/actions/workflows/go.yml/badge.svg)](https://github.com/anicoll/screamer/actions/workflows/go.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/anicoll/screamer.svg)](https://pkg.go.dev/github.com/anicoll/screamer)

Cloud Spanner Change Streams Subscriber for Go

---

## Synopsis

This library is an implementation to subscribe a change stream's records of Google Cloud Spanner in Go.
It is heavily inspired by the SpannerIO connector of the [Apache Beam SDK](https://github.com/apache/beam) and is compatible with the PartitionMetadata data model.

## Motivation

To read a change stream, Google Cloud offers a [Dataflow connector](https://cloud.google.com/spanner/docs/change-streams/use-dataflow) as a scalable and reliable solution, but in some cases the abstraction and capabilities of Dataflow pipelines can be too much (or is simply too expensive).
This library aims to make reading change streams native for non beam/dataflow use cases.

## Upgrading

Recent releases change how partitions are scheduled and owned: parent gating, leases, parking idle partitions and fair share are all on by default, and the Spanner metadata table has new columns. `RunMigrations` adds them. Read [MIGRATION.md](MIGRATION.md) before upgrading, especially if you manage the schema yourself, have your own `PartitionStorage`, or run old and new runners side by side.

## Example Usage

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	runnerID := "runner-1"

	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "foo-project", "bar-instance", "baz-database")
	spannerClient, err := spanner.NewClient(ctx, database)
	if err != nil {
		panic(err)
	}
	defer spannerClient.Close()

	partitionMetadataTableName := "PartitionMetadata_FooStream"

	partitionStorage := partitionstorage.NewSpanner(spannerClient, partitionMetadataTableName)
	if err := partitionStorage.RunMigrations(ctx); err != nil {
		panic(err)
	}
	if err := partitionStorage.RegisterRunner(ctx, runnerID); err != nil {
		panic(err)
	}

	changeStreamName := "FooStream"
	subscriber := screamer.NewSubscriber(spannerClient, changeStreamName, runnerID, partitionStorage, screamer.WithLogLevel("debug"))

	fmt.Fprintf(os.Stderr, "Reading the stream...\n")
	logger := &Logger{out: os.Stdout}
	if err := subscriber.Subscribe(ctx, logger); err != nil && !errors.Is(ctx.Err(), context.Canceled) {
		panic(err)
	}
}

type Logger struct {
	out io.Writer
	mu  sync.Mutex
}

// []byte is marshalled screamer.DataChangeRecord
func (l *Logger) Consume(change []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.out).Encode(change)
}
```

## CLI

### Installation

The CLI lives in `cmd`, so build it from a clone:

```console
$ git clone https://github.com/anicoll/screamer && cd screamer
$ make build   # writes .bin/screamer
```

`go install github.com/anicoll/screamer/cmd@latest` also works, but names the binary `cmd`.

### Usage

```
NAME:
   screamer screamer

USAGE:
   screamer screamer [options]

OPTIONS:
   --dsn string                      [$DSN]
   --stream string                   [$STREAM]
   --metadata-table string           [$METADATA_TABLE]
   --start string                   (default: Start timestamp with RFC3339 format, default: current timestamp) [$START]
   --end string                     (default: End timestamp with RFC3339 format default: indefinite) [$END]
   --heartbeat-interval duration    (default: 3s) [$HEARTBEAT_INTERVAL]
   --max-concurrent-partitions int  Maximum partitions this runner reads at once (0 = unlimited). Also sizes the gRPC channel pool for change stream reads. (default: 0) [$MAX_CONCURRENT_PARTITIONS]
   --lease-ttl duration             How long a partition lease lasts without renewal (0 = default, 30s). A runner that dies is taken over after this long. (default: 0s) [$LEASE_TTL]
   --park                           Park idle partitions and read them in short catch-up queries, so a runner can look after more partitions than it streams at once. --park=false streams every partition. [$PARK]
   --park-interval duration         How long a parked partition waits between catch-up reads, and so the most extra delay for a change to an idle partition (0 = default, 10s). (default: 0s) [$PARK_INTERVAL]
   --park-idle-after duration       How long a partition delivers only heartbeats before it is parked (0 = default, 1m). (default: 0s) [$PARK_IDLE_AFTER]
   --fair-share                     Even out streaming partitions across runners: a runner holding more than its share hands its quietest partitions to others. --fair-share=false lets each runner keep what it claims. [$FAIR_SHARE]
   --partition-dsn string           (default: Database dsn for use by the partition metadata table. If not provided, the main dsn will be used.) [$PARTITION_DSN]
   --help, -h                       show help
```

`--park` and `--fair-share` are on unless you pass `=false`. Without `--metadata-table`, partition metadata is kept in memory, so only one runner can read the stream and it starts from scratch on every restart.

---

## Built-in Distributed Locking for Scalability

Screamer includes built-in distributed locking and runner liveness tracking. When you scale up and run multiple Screamer instances, partitions are automatically assigned to available runners, and failover is handled transparently. This enables robust, distributed processing of change streams with high availability. See [Partition leases](#partition-leases) for how ownership moves between runners.

---

## Understanding Partition Scaling

A common source of confusion when operating Screamer (or any change stream consumer) is how the number of partitions relates to your Spanner instance size and workload. The short answer: **it is neither linear nor predictable, and it has nothing to do with the rate of CRUD operations on your tables.**

### How partitions are determined

Cloud Spanner change stream partitions map directly to Spanner's internal **key-range splits**. Spanner continuously and autonomously splits (and occasionally merges) key ranges across its storage layer to balance data volume and hotspots across servers. Each time a split occurs in a key range covered by a change stream, Spanner emits a `ChildPartitionsRecord` containing new partition tokens. Screamer receives these records and automatically begins processing the new child partitions.

See: [Change streams details — Partitions and ChildPartitionsRecord](https://cloud.google.com/spanner/docs/change-streams/details)

This means:

- **Partitions always start at one.** Every change stream subscription begins with a single root partition. Child partitions are added over time as Spanner issues split events — they are not pre-allocated.
- **The number of partitions grows with instance capacity, but not linearly.** A larger instance (more processing units / nodes) gives Spanner more servers to spread data across, which typically results in more splits over time. However, there is no formula of the form `partitions = f(nodes)`. Spanner decides when and where to split based on its own internal heuristics. See: [Database splits](https://cloud.google.com/spanner/docs/schema-and-data-model#database-splits)
- **Partition count is unpredictable at any given moment.** Splits and merges happen asynchronously in the background. You cannot query for the current or future number of partitions; you can only observe them as they arrive via `ChildPartitionsRecord` events. See: [Reading change streams](https://docs.cloud.google.com/spanner/docs/change-streams/#reading_change_streams)

### What does NOT drive partition count

- **Write or read throughput on your tables.** Spanner splits based on data distribution across the key space and its internal load-balancing needs — not on the number of inserts, updates, or deletes per second flowing through a monitored table. A heavily-written table does not necessarily produce more partitions; a lightly-written table that holds a large key range may produce many. See: [Life of a Cloud Spanner read and write](https://docs.cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes)
- **The number of Screamer instances you run.** Scaling out Screamer runners increases your processing capacity, but does not influence how many partitions Spanner creates. Conversely, having more partitions than runners is safe: partitions over a runner's limit wait their turn, idle ones are parked, and partitions whose runner stopped or died are picked up through their leases.

### Implications for capacity planning

Because partition count is externally driven and non-linear, you should design your deployment to handle a **dynamic and variable** number of concurrent partitions:

- Do not hard-code an expected partition count.
- Size your runner pool conservatively and rely on Screamer's automatic partition assignment to distribute work.
- Watch the [scaling metrics](#scaling-metrics) (`screamer.partitions.by_state`, `screamer.runners.desired`, `screamer.watermark.lag`) to see the actual number of partitions over time, and scale on those rather than projecting from instance size alone. [Autoscaling on Kubernetes](#autoscaling-on-kubernetes) shows how.

---

## Running at scale

### Clients

Every open partition is a long-lived streaming query. Give change stream reads their own Spanner client, sized for your partition limit, and give the `PartitionStorage` a separate client:

```go
reads, err := screamer.NewChangeStreamClient(ctx, database, 400) // 400 partitions → 8 gRPC channels
metadata, err := spanner.NewClient(ctx, database)
storage := partitionstorage.NewSpanner(metadata, "PartitionMetadata_FooStream")
subscriber := screamer.NewSubscriber(reads, "FooStream", runnerID, storage,
	screamer.WithMaxConcurrentPartitions(400))
```

The Spanner Go client opens 4 gRPC channels by default, and each channel carries a limited number of concurrent streams (Screamer plans for 50). Past that, new queries queue. If metadata writes share those channels, runner heartbeats stall and other runners take over partitions that are still being read. The CLI does this split automatically; set `--max-concurrent-partitions` to size it.

### Failed partitions

A partition that fails for a reason other than the consumer, such as a dropped stream or a failed metadata write, is retried from its last watermark with exponential backoff (1s doubling to 30s), up to 5 times in a row. The count resets whenever the partition makes progress. Other partitions on the runner keep going. Consumer errors (a failed `Consume`, a nack, an ack timeout) still stop the subscriber. Tune with `WithPartitionMaxRetries(n)`; `0` restores the old stop-on-first-error behaviour.

### Ordering across splits and merges

When a partition splits or merges, its children carry on from where it ended. A merged child has several parents, and they can finish at very different times, for example while one of them is catching up. Screamer reads a child only once every one of its parents has finished, so changes to each key are delivered in commit order. This is parent gating, and it is on by default.

`WithParentGating(false)` turns it off and schedules a child as soon as any parent reports it, as releases before it did. Children then start sooner, but a key can be delivered out of order when one parent is behind. Parent gating needs a `PartitionStorage` that implements `screamer.ParentGatedScheduler`; both built-in storages do, and [MIGRATION.md](MIGRATION.md) covers custom ones.

### Partition leases

Each runner owns the partitions it reads through a lease stored on the partition's row: an owner, an epoch and an expiry time. A runner renews all its leases every 10 seconds, in the same write as its checkpoints, and a lease lasts 30 seconds without renewal (`WithLeaseTTL`, or `--lease-ttl` on the CLI).

- **A runner that stops cleanly** (a rollout, a scale-down) releases its leases with its last watermarks. Other runners pick the partitions up on their next claim, about a second later.
- **A runner that dies** (killed, OOM, lost its node) keeps its leases until they expire. After about 30 seconds, other runners claim its partitions and resume from its last checkpoint. The records after that checkpoint are delivered again, as with any restart.
- **A runner that freezes** for longer than the TTL (a long GC pause, a suspended VM, a network partition) loses its leases while it is frozen. Each claim increments the partition's epoch, and every watermark write is conditional on the epoch, so when the frozen runner wakes up its next checkpoint is rejected. It stops reading those partitions and writes nothing for them: the new owner's progress is never overwritten. The frozen runner carries on and claims other work. Its last in-flight batches may still reach your consumer, which is the usual at-least-once redelivery.
- **A pause shorter than about 20 seconds** changes nothing.

Lease expiry is compared with Spanner's clock, so runners' clocks don't need to agree. `screamer.leases.held`, `screamer.leases.lost` and `screamer.leases.takeovers` show what is happening; `lost` should stay near zero, and a spike in `takeovers` means runners died or were stopped.

Before leases, a runner kept a partition for as long as it refreshed its `Runner` row, and any other runner took over a partition whose runner hadn't refreshed for 3 seconds. A pause longer than 3 seconds gave the partition two owners, and nothing stopped the old one writing an older watermark. `WithLeases(false)` goes back to that behaviour. Leases need the `Owner`, `Epoch` and `LeaseExpiresAt` columns, which `RunMigrations` adds; see [MIGRATION.md](MIGRATION.md).

### Parking idle partitions

On a large instance most partitions carry no writes at any moment, but each one still holds a change stream query open, and a runner can only hold so many (`WithMaxConcurrentPartitions`, gRPC streams, memory). Before parking, a partition over the limit was simply never read until it split, merged or the stream ended.

Now a partition that has delivered nothing but heartbeats for a minute is **parked**: its query is closed and its lease released. Every 10 seconds, any runner claims it and reads it in a short catch-up query that ends at the current time. If that finds changes, the runner goes back to streaming the partition; if not, it parks it again. Runners claim whatever is furthest behind first, whether that is a new child, a parked partition that is due, or a partition whose lease expired, so the limit becomes a queue that drains.

In the simulator, one runner with a limit of 20 reads all 200 partitions of a mostly idle stream. Without parking, more than half of them are never read.

#### Latency vs capacity

The cost is latency on idle partitions: a change to a parked partition is delivered up to one park interval (10s) later than it would be if it were streaming. Partitions with steady traffic never park, so their latency doesn't change. The first change after a quiet spell is the one that waits; once a catch-up read finds data, the partition streams again.

- `WithParkInterval(d)` / `--park-interval` sets the worst case extra delay. Shorter means more catch-up queries.
- `WithParkIdleAfter(d)` / `--park-idle-after` sets how long a partition must be quiet before it parks (default one minute).
- `WithParking(false)` / `--park=false` streams every partition, as before. Use it when every change must arrive within a second or two and you can afford a query per partition.

Parking needs leases. `screamer.watermark.lag` includes parked partitions, so expect it to sit at around the park interval on a quiet stream.

### Fair share across runners

A runner claims whatever is ready and it has slots for, so the first runner to start, or the one that happens to be polling when partitions split, would otherwise keep most of the work. Every 15 seconds each runner now reads how many runners are live and how many partitions are streaming or waiting to, and aims to hold at most `ceil(partitions / runners × 1.1)`. A runner over that hands its quietest partitions back with their watermarks, at most 5% of them every 15 seconds, and doesn't claim those back for a minute, so they go to runners with room. It still claims anything else, so fair share never leaves a partition unread: a runner that stops or dies is taken over as quickly as without it.

In the simulator, a runner that joins a cluster of one busy runner with 40 partitions holds 18 of them three minutes later. Without fair share it holds none until something splits. `WithFairShare(false)` / `--fair-share=false` turns it off. Runners are counted per metadata table, so streams that share a database and its `Runner` table don't distort each other's shares.

### Checkpoints

After each batch is delivered, Screamer records the partition's watermark, the point it resumes from after a restart or a takeover. Watermarks are buffered in memory and written together once a second, in one transaction of up to 500 partitions, rather than once per batch per partition. With thousands of partitions that each heartbeat every few seconds, that turns thousands of small writes a second into one. Watermarks only ever move forward, and are written one last time when the subscriber stops.

The trade-off: after a crash, a partition resumes from its last written watermark, so up to one interval of changes can be delivered again. Delivery was already at-least-once. Tune it with `WithCheckpointInterval(d)`; `0` writes after every batch, as before.

### Scaling metrics

Pass an OpenTelemetry `MeterProvider` with `WithMeterProvider` (the global provider is used otherwise). Screamer ships no exporter; use the OpenTelemetry Prometheus exporter or an OTLP exporter in your application. The CLI doesn't set one up, so it reports no metrics; use the library if you need them.

Every metric carries a `stream` attribute (the change stream name). Runner metrics also carry `runner_id`, and `screamer.partitions.by_state` carries `state`.

| Metric | Scope | Meaning |
| --- | --- | --- |
| `screamer.runners.desired` | cluster | `ceil(demand / (WithMaxConcurrentPartitions × 0.75))`, where demand is one slot per partition that streams or waits to, plus the share of a slot each parked partition's catch-up reads use (measured slice time ÷ park interval). Only reported when a partition limit is set. |
| `screamer.runners.live` | cluster | Runners that refreshed recently. |
| `screamer.partitions.unfinished` | cluster | Partitions not yet finished. |
| `screamer.partitions.by_state` | cluster | Unfinished partitions by `state`: `CREATED` (waiting), `SCHEDULED` and `RUNNING` (streaming), `PARKED` (idle). A `CREATED` count that stays high means you are short of capacity. |
| `screamer.watermark.lag` | cluster | Seconds the oldest unfinished partition is behind now. If it keeps growing, you are not keeping up. |
| `screamer.partitions.active` | runner | Partitions this runner is reading. |
| `screamer.partitions.capacity` | runner | This runner's partition limit. |
| `screamer.partition.retries` | runner | Partition reads retried after a failure. |
| `screamer.leases.held` | runner | Partition leases this runner holds. |
| `screamer.leases.lost` | runner | Leases this runner found taken by another runner, after a pause longer than the lease TTL. Should stay near zero. |
| `screamer.leases.takeovers` | runner | Partitions this runner claimed after another runner's lease expired or was released. |
| `screamer.partitions.parks` | runner | Times this runner parked an idle partition, including after a catch-up read found nothing. |
| `screamer.partitions.fair_share` | runner | The most partitions this runner aims to hold (0 = no limit). |
| `screamer.partitions.handovers` | runner | Partitions this runner handed to others because it held more than its fair share. |

Cluster metrics are read from the metadata table every 15 seconds, and every runner reports the same values, so aggregate them with `max`, not `sum`. Each runner also logs a warning when `desired_runners` is above `live_runners`.

### Autoscaling on Kubernetes

`screamer.runners.desired` already is a replica count, so the autoscaler doesn't need a formula of its own. Set `WithMaxConcurrentPartitions` (or `--max-concurrent-partitions`) on every runner, since desired runners is only reported with a limit, and export the metrics to Prometheus.

With KEDA:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: screamer-foostream
spec:
  scaleTargetRef:
    name: screamer-foostream        # the Deployment
  minReplicaCount: 2                # keep a spare so a dead runner is covered
  maxReplicaCount: 20
  pollingInterval: 30
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 60
        scaleDown:
          stabilizationWindowSeconds: 600   # every scale-down moves partitions
          policies:
            - type: Pods
              value: 1
              periodSeconds: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring:9090
        query: max(screamer_runners_desired{stream="FooStream"})
        threshold: "1"
```

A plain HPA with prometheus-adapter works the same way: an `External` metric on `max(screamer_runners_desired{stream="FooStream"})` with an `AverageValue` target of 1.

Things that make this work well:

- **Scale up fast, down slowly.** A new runner gets its share within a minute or two through fair share. A scale-down hands partitions back, which costs a second of delay on each and some redelivery, so don't do it every few minutes.
- **Let runners stop cleanly.** On SIGTERM, cancel the context passed to `Subscribe` (the CLI does). The runner releases its leases with its last watermarks and others take over in about a second. Give pods a `terminationGracePeriodSeconds` of at least 30. A runner that is killed outright is taken over after the lease TTL (30 seconds) instead.
- **Use the pod name as the runner ID** if you want to match metrics to pods. Runner rows expire after a day, so nothing builds up.
- **Alert on lag, not on runner counts.** `screamer.watermark.lag` near the change stream's retention period means changes are about to be lost. `screamer.runners.desired > screamer.runners.live` for more than ten minutes means the autoscaler can't keep up (at its max, or pods are pending). `screamer.leases.lost` above zero means runners are pausing for longer than the lease TTL.

---

## Testing with the simulator

The Spanner emulator only ever returns one partition, which never splits, so it cannot test partition scaling. `pkg/simulator` is an in-process change stream that generates a partition topology (initial partitions, then splits and merges over time) and a stream of writes, and serves it through the `screamer.ChangeStreamReader` interface:

```go
sim := simulator.New(simulator.Config{
	InitialPartitions:     500,
	MeanPartitionLifetime: 2 * time.Minute,
	MergeFraction:         0.3,
	WritesPerSecond:       200,
	HotKeyFraction:        0.01, // 1% of keys get 90% of writes, so most partitions are idle
	HotWriteShare:         0.9,
	MaxConcurrentStreams:  400,  // model running out of gRPC streams
})
checker := simulator.NewChecker(sim) // a Consumer that checks delivery and per-key order
sub := screamer.NewSubscriber(nil, "Stream", "runner-1", partitionstorage.NewInmemory(),
	screamer.WithChangeStreamReader(sim))
```

Run it inside a `testing/synctest` bubble so simulated minutes take milliseconds. `checker.Report(cutoff)` returns missing, duplicate and out-of-order deliveries. See `pkg/simulator/baseline_test.go` for examples.

## Running the tests

| Command | What it runs |
| --- | --- |
| `make test` | Everything. |
| `make test-unit` | Unit tests in every package except the simulator. No Docker needed. |
| `make test-integration` | `TestIntegrationTestSuite` and `TestSpannerTestSuite`, which start the Spanner emulator with testcontainers. Needs Docker. |
| `make test-simulator` | `pkg/simulator`, including the baseline scale tests. |

The three `test-*` targets each print a per-package table (pass, fail, skip, time, coverage) and the output of any failed test, and write `test-<group>.json` and `coverage-<group>.out`. CI runs them as separate jobs and posts the tables as one comment on the pull request.

---

## Further Reading

- [Change streams overview](https://cloud.google.com/spanner/docs/change-streams) — high-level introduction to Cloud Spanner change streams
- [Change streams details — Partitions and ChildPartitionsRecord](https://cloud.google.com/spanner/docs/change-streams/details) — in-depth explanation of partition tokens, splits, merges, and the child partitions record format
- [Reading change streams](https://cloud.google.com/spanner/docs/change-streams/read-change-stream) — documents the root partition, heartbeat flow, and how to consume partition tokens
- [Database splits](https://cloud.google.com/spanner/docs/schema-and-data-model#database-splits) — explains how and why Spanner autonomously splits and merges key ranges
- [Life of a Cloud Spanner read and write](https://docs.cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes) — whitepaper covering Spanner's internal load-balancing and how it relates to data distribution rather than DML throughput
- [Google Cloud blog: Change streams for Cloud Spanner](https://cloud.google.com/blog/products/databases/track-and-integrate-change-data-with-spanner-change-streams) — launch blog post with an accessible overview of the partition model
- [Apache Beam SpannerIO source](https://github.com/apache/beam/tree/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/changestreams) — the connector that inspired this library; contains extensive inline documentation on partition lifecycle management

## Credits

Heavily inspired by [spream](https://github.com/toga4/spream).
