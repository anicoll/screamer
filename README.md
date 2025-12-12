# Screamer... (S)panner (C)hange st(REAM) read(ER)

[![Test](https://github.com/anicoll/screamer/actions/workflows/go.yml/badge.svg)](https://github.com/anicoll/screamer/actions/workflows/go.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/anicoll/screamer.svg)](https://pkg.go.dev/github.com/anicoll/screamer)

Cloud Spanner Change Streams Subscriber for Go

---

## New: Built-in Distributed Locking for Scalability

Screamer now includes built-in distributed locking and runner liveness tracking. When you scale up and run multiple Screamer instances, partitions are automatically assigned to available runners, and failover is handled transparently. This enables robust, distributed processing of change streams with high availability.

---

## new: Lease-Based Partition Assignment

Screamer uses a lease-based mechanism to assign partitions to runners. This ensures:
- **Exclusivity**: A partition is processed by only one runner at a time.
- **Failover**: If a runner dies, its leases expire and are picked up by other runners.
- **Load Balancing**: Partitions are distributed among runners.

### Configuration Options

You can configure the behavior of the lease mechanism using functional options in `NewSubscriber`:

- `WithMaxConnections(int)`: Limits the number of partitions a single runner instance can claim. Default is 400.
  > [!NOTE]
  > The default Spanner client library has a maximum of 400 sessions. Going beyond this limit may degrade performance. See [Configure the number of sessions](https://docs.cloud.google.com/spanner/docs/sessions#configure_the_number_of_sessions_and_grpc_channels_in_the_pools) for more details.
- `WithLeaseDuration(time.Duration)`: Sets the duration of the lease. Runners must heartbeat within this interval to keep the lease. Default is 30s.
- `WithRebalancingInterval(time.Duration)`: Sets how often the runner checks if it should release partitions to balance the load.


### Sypnosis

This library is an implementation to subscribe a change stream's records of Google Cloud Spanner in Go.
It is heavily inspired by the SpannerIO connector of the [Apache Beam SDK](https://github.com/apache/beam) and is compatible with the PartitionMetadata data model.

### Motivation

To read a change streams, Google Cloud offers [Dataflow connector](https://cloud.google.com/spanner/docs/change-streams/use-dataflow) as a scalable and reliable solution, but in some cases the abstraction and capabilities of Dataflow pipelines can be too much (or is simply too expensive).
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
	subscriber := screamer.NewSubscriber(
		spannerClient,
		changeStreamName,
		runnerID,
		partitionStorage,
		screamer.WithLogLevel("debug"),
		screamer.WithMaxConnections(100),            // Limit to 100 partitions per instance
		screamer.WithLeaseDuration(30*time.Second),  // 30s lease duration
	)

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

### Example

## Credits

Heavily inspired by below projects.

- spream (https://github.com/toga4/spream)
