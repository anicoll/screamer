// Package soak runs Screamer against a real Spanner database for hours or days
// and checks that every write is delivered, in order per key, while partitions
// split, merge, park and move between runners.
//
// A soak has four parts, each a subcommand of cmd/soak:
//
//   - setup creates the tables and the change stream.
//   - write updates many keys, each with a sequence number that goes up by one
//     on every write, in a pattern of always-busy keys and bursts on the rest.
//   - run is a runner: a Screamer subscriber that records every delivered write
//     in a deliveries table before it acknowledges it.
//   - check compares the deliveries with the writes and reports missing,
//     duplicate and out-of-order deliveries, and delivery lag.
//
// drill runs several runners as child processes and keeps stopping, killing,
// freezing and adding them, so failover is tested without Kubernetes.
package soak

import (
	"context"
	"fmt"
	"math"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
)

// Tables names the tables and change stream a soak uses.
type Tables struct {
	// Events is the table the writer updates and the change stream watches.
	Events string
	// Stream is the change stream on Events.
	Stream string
	// Deliveries is where runners record what they delivered.
	Deliveries string
	// Metadata is Screamer's partition metadata table.
	Metadata string
}

// DefaultTables are the names used unless flags say otherwise.
var DefaultTables = Tables{
	Events:     "SoakEvents",
	Stream:     "SoakStream",
	Deliveries: "SoakDeliveries",
	Metadata:   "SoakMetadata",
}

// Setup creates whatever of the soak's tables and change stream does not
// exist yet. eventsDSN holds the events table and the change stream;
// deliveriesDSN holds the deliveries table, and may be the same database.
// retention is the change stream's retention period, such as "1d" or "7d".
func Setup(ctx context.Context, eventsDSN, deliveriesDSN string, t Tables, retention string) error {
	if err := createMissing(ctx, eventsDSN, []object{
		{kind: "table", name: t.Events, ddl: fmt.Sprintf(`CREATE TABLE %s (
	Id INT64 NOT NULL,
	Seq INT64 NOT NULL,
	Payload STRING(MAX),
	UpdatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (Id)`, t.Events)},
		{kind: "stream", name: t.Stream, ddl: fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s OPTIONS (retention_period = '%s')`, t.Stream, t.Events, retention)},
	}); err != nil {
		return err
	}
	return createMissing(ctx, deliveriesDSN, []object{
		{kind: "table", name: t.Deliveries, ddl: fmt.Sprintf(`CREATE TABLE %s (
	Id INT64 NOT NULL,
	Seq INT64 NOT NULL,
	DeliveryId STRING(36) NOT NULL,
	RunnerId STRING(MAX),
	PartitionToken STRING(MAX),
	CommitTimestamp TIMESTAMP,
	DeliveredAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (Id, Seq, DeliveryId)`, t.Deliveries)},
	})
}

type object struct {
	kind string // "table" or "stream"
	name string
	ddl  string
}

func createMissing(ctx context.Context, dsn string, objs []object) error {
	client, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return err
	}
	defer client.Close()

	var ddl []string
	for _, o := range objs {
		exists, err := objectExists(ctx, client, o)
		if err != nil {
			return err
		}
		if !exists {
			ddl = append(ddl, o.ddl)
		}
	}
	if len(ddl) == 0 {
		return nil
	}

	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer admin.Close()
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: dsn, Statements: ddl})
	if err != nil {
		return fmt.Errorf("failed to create soak schema: %w", err)
	}
	return op.Wait(ctx)
}

func objectExists(ctx context.Context, client *spanner.Client, o object) (bool, error) {
	sql := `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @name`
	if o.kind == "stream" {
		sql = `SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @name`
	}
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: map[string]any{"name": o.name}})
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	return err == nil, err
}

// keyID is the primary key of key number i out of n. Keys are spread evenly
// over the positive INT64 range, in order, so Spanner can split the table
// anywhere, and a run of consecutive keys covers one contiguous key range.
func keyID(i, n int) int64 {
	return int64(i) * (math.MaxInt64 / int64(n))
}
