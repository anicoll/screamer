package partitionstorage

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/anicoll/screamer"
)

func (s *SpannerTestSuite) claim(ctx context.Context, storage *testStorage, runner string, limit int) []*screamer.PartitionMetadata {
	ps, err := storage.ClaimPartitions(ctx, screamer.ClaimRequest{RunnerID: runner, TTL: 30 * time.Second, Limit: limit, ParentGating: true})
	s.Require().NoError(err)
	return ps
}

// expireLease moves a partition's lease expiry into the past, as if its owner
// had stopped renewing it 30 seconds ago.
func (s *SpannerTestSuite) expireLease(ctx context.Context, storage *testStorage, token string) {
	_, err := storage.client.Apply(ctx, []*spanner.Mutation{spanner.UpdateMap(storage.tableName, map[string]interface{}{
		columnPartitionToken: token,
		columnLeaseExpiresAt: time.Now().Add(-time.Second),
	})})
	s.Require().NoError(err)
}

func (s *SpannerTestSuite) readPartition(ctx context.Context, storage *testStorage, token string) *screamer.PartitionMetadata {
	row, err := storage.client.Single().ReadRow(ctx, storage.tableName, spanner.Key{token},
		[]string{columnPartitionToken, columnState, columnWatermark, columnOwner, columnEpoch, columnLeaseExpiresAt})
	s.Require().NoError(err)
	p := new(screamer.PartitionMetadata)
	s.Require().NoError(row.ToStruct(p))
	return p
}

func (s *SpannerTestSuite) TestLeases() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "Leases")
	defer cleanup()

	start := time.Now().Truncate(time.Microsecond).UTC()
	s.insertPartition(ctx, storage, "p1", nil, screamer.StateCreated, start, start)

	// A first claim takes the lease at epoch 1 and schedules the partition.
	ps := s.claim(ctx, storage, "a", 10)
	s.Require().Len(ps, 1)
	s.Equal("a", *ps[0].Owner)
	s.EqualValues(1, *ps[0].Epoch)
	s.Equal(screamer.StateScheduled, ps[0].State)
	stored := s.readPartition(ctx, storage, "p1")
	s.Equal(screamer.StateScheduled, stored.State)
	s.Equal("a", *stored.Owner)
	s.EqualValues(1, *stored.Epoch)
	s.WithinDuration(time.Now().Add(30*time.Second), *stored.LeaseExpiresAt, 5*time.Second)

	// A held lease cannot be claimed.
	s.Empty(s.claim(ctx, storage, "b", 10))

	// Renewing moves the watermark forward; a wrong epoch is rejected.
	lost, err := storage.RenewLeases(ctx, "a", 30*time.Second, []screamer.Lease{
		{PartitionToken: "p1", Epoch: 1, Watermark: start.Add(5 * time.Second)},
	})
	s.Require().NoError(err)
	s.Empty(lost)
	s.Equal(start.Add(5*time.Second), s.readPartition(ctx, storage, "p1").Watermark)
	lost, err = storage.RenewLeases(ctx, "a", 30*time.Second, []screamer.Lease{{PartitionToken: "p1", Epoch: 7}})
	s.Require().NoError(err)
	s.Equal([]string{"p1"}, lost)

	// Once the lease expires, another runner claims it at the next epoch and
	// resumes from the last checkpoint.
	s.expireLease(ctx, storage, "p1")
	ps = s.claim(ctx, storage, "b", 10)
	s.Require().Len(ps, 1)
	s.Equal("b", *ps[0].Owner)
	s.EqualValues(2, *ps[0].Epoch)
	s.Equal(start.Add(5*time.Second), ps[0].Watermark)

	// The old owner is fenced: nothing it renews or releases is written.
	lost, err = storage.RenewLeases(ctx, "a", 30*time.Second, []screamer.Lease{
		{PartitionToken: "p1", Epoch: 1, Watermark: start.Add(9 * time.Second)},
	})
	s.Require().NoError(err)
	s.Equal([]string{"p1"}, lost)
	s.Require().NoError(storage.ReleaseLeases(ctx, "a", []screamer.Lease{{PartitionToken: "p1", Epoch: 1, Watermark: start.Add(9 * time.Second)}}))
	stored = s.readPartition(ctx, storage, "p1")
	s.Equal(start.Add(5*time.Second), stored.Watermark)
	s.Equal("b", *stored.Owner)

	// A watermark never moves backwards.
	_, err = storage.RenewLeases(ctx, "b", 30*time.Second, []screamer.Lease{{PartitionToken: "p1", Epoch: 2, Watermark: start.Add(time.Second)}})
	s.Require().NoError(err)
	s.Equal(start.Add(5*time.Second), s.readPartition(ctx, storage, "p1").Watermark)

	// Releasing writes the watermark and frees the partition at once.
	s.Require().NoError(storage.ReleaseLeases(ctx, "b", []screamer.Lease{{PartitionToken: "p1", Epoch: 2, Watermark: start.Add(7 * time.Second)}}))
	stored = s.readPartition(ctx, storage, "p1")
	s.Nil(stored.Owner)
	s.Nil(stored.LeaseExpiresAt)
	ps = s.claim(ctx, storage, "c", 10)
	s.Require().Len(ps, 1)
	s.EqualValues(3, *ps[0].Epoch)
	s.Equal(start.Add(7*time.Second), ps[0].Watermark)

	// A finished partition is never claimed, its lease cannot be renewed, and a
	// late UpdateToRunning does not bring it back.
	s.Require().NoError(storage.UpdateToFinished(ctx, ps[0], "c"))
	s.Require().NoError(storage.UpdateToRunning(ctx, ps[0]))
	s.Equal(screamer.StateFinished, s.getPartitionState(ctx, storage, "p1"))
	s.expireLease(ctx, storage, "p1")
	s.Empty(s.claim(ctx, storage, "d", 10))
	lost, err = storage.RenewLeases(ctx, "c", 30*time.Second, []screamer.Lease{{PartitionToken: "p1", Epoch: 3}})
	s.Require().NoError(err)
	s.Equal([]string{"p1"}, lost)
}

// Claiming takes expired leases first, then ready CREATED partitions, up to
// the limit, and honours parent gating.
func (s *SpannerTestSuite) TestClaimPartitionsOrderAndGating() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ClaimOrder")
	defer cleanup()

	start := time.Now().Add(-time.Hour).Truncate(time.Microsecond).UTC()
	// "old" was being read by a runner that did not use leases: SCHEDULED with no lease.
	s.insertPartition(ctx, storage, "old", nil, screamer.StateRunning, start, start)
	s.insertPartition(ctx, storage, "parent", nil, screamer.StateCreated, start, start)
	s.insertPartition(ctx, storage, "child", []string{"parent"}, screamer.StateCreated, start.Add(time.Minute), start.Add(time.Minute))

	ps := s.claim(ctx, storage, "a", 1)
	s.Equal([]string{"old"}, tokens(ps), "a partition with no lease is claimed first")
	s.EqualValues(1, *ps[0].Epoch)
	s.Equal(screamer.StateRunning, s.getPartitionState(ctx, storage, "old"), "a reclaimed partition keeps its state")

	ps = s.claim(ctx, storage, "a", 10)
	s.Equal([]string{"parent"}, tokens(ps), "the child waits for its parent")

	s.Require().NoError(storage.UpdateToFinished(ctx, ps[0], "a"))
	ps = s.claim(ctx, storage, "a", 10)
	s.Equal([]string{"child"}, tokens(ps))
}

func (s *SpannerTestSuite) TestClaimPartitionsWithoutParentGating() {
	ctx := context.Background()
	storage, cleanup := s.setupStorage(ctx, "ClaimUngated")
	defer cleanup()

	start := time.Now().Add(-time.Hour).Truncate(time.Microsecond).UTC()
	s.insertPartition(ctx, storage, "early", nil, screamer.StateCreated, start, start)
	s.insertPartition(ctx, storage, "late", []string{"missing"}, screamer.StateCreated, start.Add(time.Minute), start.Add(time.Minute))

	ps, err := storage.ClaimPartitions(ctx, screamer.ClaimRequest{RunnerID: "a", TTL: time.Minute, Limit: 10, MinWatermark: start.Add(30 * time.Second)})
	s.Require().NoError(err)
	s.Equal([]string{"late"}, tokens(ps))
}

// A metadata table created before leases gets the lease columns from
// RunMigrations, keeps its rows, and works with leases straight away. Before
// that, claiming fails with an error that says what to do.
func (s *SpannerTestSuite) TestRunMigrationsAddsLeaseColumns() {
	ctx := context.Background()
	const table = "PreLeaseTable"

	admin, err := database.NewDatabaseAdminClient(ctx)
	s.Require().NoError(err)
	defer admin.Close()
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: s.dsn,
		Statements: []string{fmt.Sprintf(`CREATE TABLE %s (
			PartitionToken STRING(MAX) NOT NULL,
			ParentTokens ARRAY<STRING(MAX)> NOT NULL,
			StartTimestamp TIMESTAMP NOT NULL,
			EndTimestamp TIMESTAMP NOT NULL,
			HeartbeatMillis INT64 NOT NULL,
			State STRING(MAX) NOT NULL,
			Watermark TIMESTAMP NOT NULL,
			CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ScheduledAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
			RunningAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
			FinishedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
		) PRIMARY KEY (PartitionToken)`, table)},
	})
	s.Require().NoError(err)
	s.Require().NoError(op.Wait(ctx))

	client, err := spanner.NewClient(ctx, s.dsn)
	s.Require().NoError(err)
	defer client.Close()
	storage := &testStorage{SpannerPartitionStorage: NewSpanner(client, table), t: s.T()}
	defer storage.cleanup(ctx)

	start := time.Now().Truncate(time.Microsecond).UTC()
	s.insertPartition(ctx, storage, "p1", nil, screamer.StateRunning, start, start)

	_, err = storage.ClaimPartitions(ctx, screamer.ClaimRequest{RunnerID: "a", TTL: time.Minute, Limit: 10, ParentGating: true})
	s.Require().Error(err)
	s.ErrorContains(err, "RunMigrations")

	s.Require().NoError(storage.RunMigrations(ctx))
	s.Require().NoError(storage.RunMigrations(ctx), "running migrations again changes nothing")

	p, err := storage.GetUnfinishedMinWatermarkPartition(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(p)
	s.Equal("p1", p.PartitionToken)
	s.Nil(p.Owner)

	ps := s.claim(ctx, storage, "a", 10)
	s.Equal([]string{"p1"}, tokens(ps))
}
