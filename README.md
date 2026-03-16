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
