package soak

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/internal/helper"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SoakTestSuite runs the soak tool end to end against the Spanner emulator.
// The emulator never splits partitions, so this checks the tool itself, not
// Screamer at scale: that is what a soak on real Spanner is for.
type SoakTestSuite struct {
	suite.Suite
	ctx       context.Context
	container testcontainers.Container
	instance  string
	dbs       int
}

func TestSoakTestSuite(t *testing.T) {
	suite.Run(t, new(SoakTestSuite))
}

func (s *SoakTestSuite) SetupSuite() {
	s.ctx = context.Background()
	var err error
	s.container, err = helper.NewTestContainer(s.ctx, "gcr.io/cloud-spanner-emulator/emulator", nil,
		[]string{"9010/tcp"}, wait.ForLog("gRPC server listening at"))
	s.Require().NoError(err)
	port, err := s.container.MappedPort(s.ctx, "9010")
	s.Require().NoError(err)
	host, err := s.container.Host(s.ctx)
	s.Require().NoError(err)
	os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%s", host, port.Port()))
	s.instance, err = helper.CreateInstance(s.ctx, "soak-project", "soak-instance")
	s.Require().NoError(err)
}

func (s *SoakTestSuite) TearDownSuite() {
	if s.container != nil {
		_ = s.container.Terminate(s.ctx)
	}
}

// newDB returns a fresh database with the soak schema, and a client for it.
func (s *SoakTestSuite) newDB() (string, *spanner.Client) {
	s.dbs++
	dsn, err := helper.CreateDatabase(s.ctx, s.instance, fmt.Sprintf("soak-%d", s.dbs))
	s.Require().NoError(err)
	s.Require().NoError(Setup(s.ctx, dsn, dsn, DefaultTables, "1d"))
	s.Require().NoError(Setup(s.ctx, dsn, dsn, DefaultTables, "1d"), "setup must be safe to run twice")
	client, err := spanner.NewClient(s.ctx, dsn)
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	return dsn, client
}

// Two runners read a stream while the writer runs. One is stopped and
// replaced halfway, so its partitions move. Every write must be delivered.
func (s *SoakTestSuite) TestEndToEnd() {
	dsn, client := s.newDB()
	start := time.Now()

	runner := func(ctx context.Context, id string) error {
		return RunRunner(ctx, RunnerConfig{
			StreamDSN: dsn, MetadataDSN: dsn, DeliveriesDSN: dsn,
			Tables:     DefaultTables,
			RunnerID:   id,
			Start:      start,
			StatsEvery: 5 * time.Second,
			Options: []screamer.Option{
				screamer.WithHeartbeatInterval(time.Second),
				screamer.WithLeaseTTL(3 * time.Second),
				screamer.WithParkInterval(2 * time.Second),
				screamer.WithParkIdleAfter(3 * time.Second),
			},
		})
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	run := func(id string) context.CancelFunc {
		ctx, cancel := context.WithCancel(s.ctx)
		wg.Go(func() { errs <- runner(ctx, id) })
		return cancel
	}
	stopA := run("runner-a")
	stopB := run("runner-b")

	w, err := NewWriter(client, DefaultTables.Events, WriterConfig{
		Keys: 40, HotFraction: 0.25, HotRate: 10,
		BurstEvery: 8 * time.Second, BurstFor: 3 * time.Second, BurstRate: 20,
		PayloadBytes: 16, Workers: 4, Batch: 5, Shards: 1,
		Duration: 20 * time.Second,
	})
	s.Require().NoError(err)
	writeDone := make(chan error, 1)
	go func() { writeDone <- w.Run(s.ctx) }()

	time.Sleep(10 * time.Second)
	stopA() // a clean stop, like a rollout
	stopC := run("runner-c")

	s.Require().NoError(<-writeDone)
	s.Require().Positive(w.Writes())
	// Give the last writes time to arrive, through a park interval or two.
	asOf := time.Now()
	s.Eventually(func() bool {
		rep, err := Check(s.ctx, client, client, DefaultTables, asOf)
		return err == nil && rep.Missing == 0
	}, 30*time.Second, time.Second)
	stopB()
	stopC()
	wg.Wait()
	close(errs)
	for err := range errs {
		s.NoError(err)
	}

	rep, err := Check(s.ctx, client, client, DefaultTables, asOf)
	s.Require().NoError(err)
	s.T().Log(rep.String())
	s.True(rep.OK(), rep.String())
	s.Equal(w.Writes(), rep.Expected)
	s.Equal(rep.Expected, rep.Delivered)
	s.Positive(rep.LagMax)
}

// Check finds the problems it is there to find.
func (s *SoakTestSuite) TestCheckFindsProblems() {
	_, client := s.newDB()
	t := DefaultTables
	apply := func(ms ...*spanner.Mutation) {
		_, err := client.Apply(s.ctx, ms)
		s.Require().NoError(err)
	}
	event := func(id, seq int64) *spanner.Mutation {
		return spanner.InsertOrUpdate(t.Events, []string{"Id", "Seq", "UpdatedAt"}, []any{id, seq, spanner.CommitTimestamp})
	}
	n := 0
	deliver := func(id, seq int64) {
		n++
		// Each delivery commits on its own, so each gets a later DeliveredAt.
		apply(spanner.Insert(t.Deliveries,
			[]string{"Id", "Seq", "DeliveryId", "RunnerId", "CommitTimestamp", "DeliveredAt"},
			[]any{id, seq, fmt.Sprint(n), "test", time.Now().Add(-time.Second), spanner.CommitTimestamp}))
	}

	apply(event(1, 3), event(2, 2), event(3, 2), event(4, 1))
	asOf := time.Now()
	apply(event(4, 2)) // after asOf: not expected yet

	deliver(1, 1)
	deliver(1, 3) // 1/2 is missing
	deliver(2, 2) // 2/2 before 2/1: out of order
	deliver(2, 1)
	deliver(3, 1)
	deliver(3, 2)
	deliver(3, 1) // a redelivery is a duplicate, not a reordering
	// Key 4's only expected write, 4/1, is never delivered; 4/2 is not expected.
	deliver(4, 2)

	rep, err := Check(s.ctx, client, client, t, asOf)
	s.Require().NoError(err)
	s.T().Log(rep.String())
	s.Equal(4, rep.Keys)
	s.EqualValues(3+2+2+1, rep.Expected)
	s.EqualValues(2, rep.Missing)
	s.ElementsMatch([]Write{{ID: 1, Seq: 2}, {ID: 4, Seq: 1}}, rep.MissingSamples)
	s.EqualValues(1, rep.OrderViolations)
	s.Equal(Write{ID: 2, Seq: 1}, rep.ReorderedSamples[0].Write)
	s.EqualValues(1, rep.Duplicates)
	s.False(rep.OK())
}
