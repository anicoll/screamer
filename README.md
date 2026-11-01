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

## Example Usage

```go
package screamer


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

```console
$ go install github.com/anicoll/screamer@latest
```

### Usage

```
NAME:
   screamer screamer

USAGE:
   screamer screamer [command options]

OPTIONS:
   --dsn value                  [$DSN]
   --stream value               [$STREAM]
   --metadata-table value       [$METADATA_TABLE]
   --start value               (default: Start timestamp with RFC3339 format, default: current timestamp) [$START]
   --end value                 (default: End timestamp with RFC3339 format default: indefinite) [$END]
   --heartbeat-interval value  (default: 3s) [$HEARTBEAT_INTERVAL]
   --partition-dsn value       (default: Database dsn for use by the partition metadata table. If not provided, the main dsn will be used.) [$PARTITION_DSN]
   --help, -h                  show help
```

---

## Built-in Distributed Locking for Scalability

Screamer includes built-in distributed locking and runner liveness tracking. When you scale up and run multiple Screamer instances, partitions are automatically assigned to available runners, and failover is handled transparently. This enables robust, distributed processing of change streams with high availability.

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
- **The number of Screamer instances you run.** Scaling out Screamer runners increases your processing capacity, but does not influence how many partitions Spanner creates. Conversely, having more partitions than runners is safe — unassigned or interrupted partitions are picked up automatically via the built-in distributed locking mechanism.

### Implications for capacity planning

Because partition count is externally driven and non-linear, you should design your deployment to handle a **dynamic and variable** number of concurrent partitions:

- Do not hard-code an expected partition count.
- Size your runner pool conservatively and rely on Screamer's automatic partition assignment to distribute work.
- Monitor the `PartitionMetadata` table to observe the actual number of active partitions over time, and use this to inform scaling decisions rather than projecting from instance size alone.

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

### Checkpoints

After each batch is delivered, Screamer records the partition's watermark, the point it resumes from after a restart or a takeover. Watermarks are buffered in memory and written together once a second, in one transaction of up to 500 partitions, rather than once per batch per partition. With thousands of partitions that each heartbeat every few seconds, that turns thousands of small writes a second into one. Watermarks only ever move forward, and are written one last time when the subscriber stops.

The trade-off: after a crash, a partition resumes from its last written watermark, so up to one interval of changes can be delivered again. Delivery was already at-least-once. Tune it with `WithCheckpointInterval(d)`; `0` writes after every batch, as before.

### Scaling metrics

Pass an OpenTelemetry `MeterProvider` with `WithMeterProvider` (the global provider is used otherwise). Screamer ships no exporter; use the OpenTelemetry Prometheus exporter or an OTLP exporter in your application.

| Metric | Scope | Meaning |
| --- | --- | --- |
| `screamer.runners.desired` | cluster | `ceil(unfinished partitions / (WithMaxConcurrentPartitions × 0.75))`. Only reported when a partition limit is set. |
| `screamer.runners.live` | cluster | Runners that refreshed recently. |
| `screamer.partitions.unfinished` | cluster | Partitions not yet finished. |
| `screamer.watermark.lag` | cluster | Seconds the oldest unfinished partition is behind now. If it keeps growing, you are not keeping up. |
| `screamer.partitions.active` | runner | Partitions this runner is reading. |
| `screamer.partitions.capacity` | runner | This runner's partition limit. |
| `screamer.partition.retries` | runner | Partition reads retried after a failure. |

Cluster metrics are read from the metadata table every 15 seconds, and every runner reports the same values, so aggregate them with `max`, not `sum`. Each runner also logs a warning when `desired_runners` is above `live_runners`.

On Kubernetes, `screamer.runners.desired` already is a replica count. An HPA with an `AverageValue` target of 1 on `max(screamer_runners_desired{stream="FooStream"})` (via prometheus-adapter or KEDA) keeps that many runners running. Note that a partition over a runner's limit is not read until a slot frees up, so `desired_runners > live_runners` means some partitions are falling behind.

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
