// Package simulator provides an in-process simulation of a Cloud Spanner change
// stream, for testing screamer without a Spanner instance.
//
// The Spanner emulator only ever returns a single partition that never splits,
// so it cannot exercise partition scaling. The simulator generates a partition
// topology up front (initial partitions, then splits and merges over time) and
// a stream of writes to a key space, then serves change stream queries for that
// topology through the screamer.ChangeStreamReader interface.
//
// Reads are driven by the wall clock (time.Now, timers), so tests normally run
// inside a testing/synctest bubble, where time is virtual and a ten-minute
// simulation finishes in milliseconds.
package simulator

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config describes the simulated change stream.
type Config struct {
	// Seed makes the generated topology and writes reproducible.
	Seed int64
	// Start is the beginning of the simulated time range. Default: time.Now().
	Start time.Time
	// Horizon is the length of the simulated time range. Topology events and
	// writes are generated in [Start, Start+Horizon). After that, partitions that
	// are still open only return heartbeats. Default: 10 minutes.
	Horizon time.Duration

	// InitialPartitions is the number of partitions returned by the root query. Default: 1.
	InitialPartitions int
	// KeySpace is the number of distinct primary keys. Default: 10,000.
	KeySpace int
	// MeanPartitionLifetime is the mean time before a partition splits or merges
	// (exponentially distributed). 0 disables splits and merges.
	MeanPartitionLifetime time.Duration
	// MergeFraction is the fraction of partition ends that are merges with the
	// neighbouring partition, between 0 and 1. The rest are splits.
	MergeFraction float64
	// MinPartitions and MaxPartitions bound the number of live partitions.
	// A merge is not done below MinPartitions, a split is not done at or above
	// MaxPartitions; the partition is then replaced by a single child instead.
	// Defaults: 1 and unlimited.
	MinPartitions int
	MaxPartitions int

	// WritesPerSecond is the total write rate across all keys.
	WritesPerSecond float64
	// HotKeyFraction of the key space receives HotWriteShare of the writes.
	// This models the common case where most partitions are idle. With both 0,
	// writes are uniform.
	HotKeyFraction float64
	HotWriteShare  float64

	// QueryLatency is the delay before a query returns its first row.
	QueryLatency time.Duration
	// MaxConcurrentStreams limits how many queries can be open at once; further
	// queries block until one closes. This models running out of gRPC streams
	// on the client's channels. 0 means unlimited.
	MaxConcurrentStreams int

	// ReadFailureRate is the probability that a query fails part way through with
	// codes.Unavailable, like a dropped stream. The failure comes at a random point
	// in the first ReadFailureWithin of the query (default 30s). 0 disables it.
	ReadFailureRate   float64
	ReadFailureWithin time.Duration
}

// Write is one generated write, the ground truth that consumers are checked against.
type Write struct {
	Key             int64
	Seq             int64 // 1-based, per key, in commit timestamp order
	CommitTimestamp time.Time
	Partition       string
}

// Partition is one generated partition.
type Partition struct {
	Token   string
	Parents []string
	// Children are the partitions that take over this partition's key range
	// when it ends. Empty if the partition does not end within the horizon.
	Children []string
	// KeyLow and KeyHigh are the key range [KeyLow, KeyHigh).
	KeyLow, KeyHigh int64
	Start           time.Time
	// End is when the partition splits or merges; zero if it does not end within the horizon.
	End time.Time

	writes []*Write // in commit timestamp order
}

// Stats are counters about how the simulator was queried.
type Stats struct {
	Queries          int64
	OpenStreams      int64
	MaxOpenStreams   int64
	BlockedOnStreams int64 // queries that had to wait for a free stream
	// PartitionsQueried is the number of distinct partition tokens that were queried.
	PartitionsQueried int
	// InjectedFailures is the number of queries failed by ReadFailureRate.
	InjectedFailures int64
}

// Simulator is a simulated change stream. It implements screamer.ChangeStreamReader.
type Simulator struct {
	cfg        Config
	partitions map[string]*Partition
	initial    []*Partition
	writes     []*Write
	splits     int
	merges     int

	streams chan struct{}

	mu      sync.Mutex
	stats   Stats
	queried map[string]struct{}
	faults  *rand.Rand // guarded by mu
}

var _ screamer.ChangeStreamReader = (*Simulator)(nil)

// New generates the topology and writes described by cfg.
func New(cfg Config) *Simulator {
	if cfg.Start.IsZero() {
		cfg.Start = time.Now()
	}
	if cfg.Horizon == 0 {
		cfg.Horizon = 10 * time.Minute
	}
	if cfg.InitialPartitions <= 0 {
		cfg.InitialPartitions = 1
	}
	if cfg.KeySpace <= 0 {
		cfg.KeySpace = 10_000
	}
	if cfg.KeySpace < cfg.InitialPartitions {
		cfg.KeySpace = cfg.InitialPartitions
	}
	if cfg.MinPartitions <= 0 {
		cfg.MinPartitions = 1
	}
	if cfg.MaxPartitions <= 0 {
		cfg.MaxPartitions = math.MaxInt
	}

	s := &Simulator{
		cfg:        cfg,
		partitions: make(map[string]*Partition),
		queried:    make(map[string]struct{}),
	}
	if cfg.ReadFailureWithin <= 0 {
		cfg.ReadFailureWithin = 30 * time.Second
		s.cfg.ReadFailureWithin = cfg.ReadFailureWithin
	}
	s.faults = rand.New(rand.NewSource(cfg.Seed + 1))
	if cfg.MaxConcurrentStreams > 0 {
		s.streams = make(chan struct{}, cfg.MaxConcurrentStreams)
	}
	s.generate(rand.New(rand.NewSource(cfg.Seed)))
	return s
}

// Partitions returns every generated partition, keyed by token.
func (s *Simulator) Partitions() map[string]*Partition { return s.partitions }

// Writes returns every generated write in commit timestamp order.
func (s *Simulator) Writes() []*Write { return s.writes }

// Splits and Merges return how many of each topology event were generated.
func (s *Simulator) Splits() int { return s.splits }
func (s *Simulator) Merges() int { return s.merges }

// Stats returns a snapshot of the query counters.
func (s *Simulator) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.PartitionsQueried = len(s.queried)
	return st
}

// LiveAt returns the partitions that cover the key space at time t.
func (s *Simulator) LiveAt(t time.Time) []*Partition {
	var live []*Partition
	for _, p := range s.partitions {
		if !t.Before(p.Start) && (p.End.IsZero() || t.Before(p.End)) {
			live = append(live, p)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].KeyLow < live[j].KeyLow })
	return live
}

// generate builds the topology and assigns writes to partitions.
func (s *Simulator) generate(rng *rand.Rand) {
	cfg := s.cfg
	horizonEnd := cfg.Start.Add(cfg.Horizon)
	next := 0
	newToken := func() string {
		next++
		return fmt.Sprintf("sim-%06d", next)
	}

	// Writes, in time order, with a per-key sequence number.
	s.writes = s.generateWrites(rng, horizonEnd)

	// Initial partitions split the key space evenly.
	live := make([]*Partition, 0, cfg.InitialPartitions) // sorted by KeyLow
	step := float64(cfg.KeySpace) / float64(cfg.InitialPartitions)
	for i := 0; i < cfg.InitialPartitions; i++ {
		p := &Partition{
			Token:   newToken(),
			Parents: []string{},
			KeyLow:  int64(math.Round(float64(i) * step)),
			KeyHigh: int64(math.Round(float64(i+1) * step)),
			Start:   cfg.Start,
		}
		live = append(live, p)
		s.partitions[p.Token] = p
	}
	s.initial = append([]*Partition(nil), live...)

	// Each live partition gets a planned end time.
	planned := map[*Partition]time.Time{}
	plan := func(p *Partition) {
		if cfg.MeanPartitionLifetime <= 0 {
			return
		}
		d := time.Duration(rng.ExpFloat64() * float64(cfg.MeanPartitionLifetime))
		if d <= 0 {
			d = time.Nanosecond
		}
		planned[p] = p.Start.Add(d)
	}
	for _, p := range live {
		plan(p)
	}

	wi := 0
	assignUntil := func(t time.Time) {
		for wi < len(s.writes) && s.writes[wi].CommitTimestamp.Before(t) {
			w := s.writes[wi]
			idx := sort.Search(len(live), func(i int) bool { return live[i].KeyHigh > w.Key })
			p := live[idx]
			w.Partition = p.Token
			p.writes = append(p.writes, w)
			wi++
		}
	}

	for {
		// Next topology event: the live partition with the earliest planned end.
		var victim *Partition
		var at time.Time
		for _, p := range live {
			if end, ok := planned[p]; ok && (victim == nil || end.Before(at)) {
				victim, at = p, end
			}
		}
		if victim == nil || !at.Before(horizonEnd) {
			break
		}
		assignUntil(at)

		idx := 0
		for i, p := range live {
			if p == victim {
				idx = i
				break
			}
		}

		end := func(p *Partition, children ...*Partition) {
			p.End = at
			for _, c := range children {
				p.Children = append(p.Children, c.Token)
			}
			delete(planned, p)
		}

		switch {
		case rng.Float64() < cfg.MergeFraction && len(live) > cfg.MinPartitions:
			// Merge with the right neighbour, or the left one at the end of the key space.
			l, r := idx, idx+1
			if r >= len(live) {
				l, r = idx-1, idx
			}
			a, b := live[l], live[r]
			c := &Partition{Token: newToken(), Parents: []string{a.Token, b.Token}, KeyLow: a.KeyLow, KeyHigh: b.KeyHigh, Start: at}
			end(a, c)
			end(b, c)
			s.partitions[c.Token] = c
			live = append(live[:l], append([]*Partition{c}, live[r+1:]...)...)
			plan(c)
			s.merges++
		case len(live) < cfg.MaxPartitions && victim.KeyHigh-victim.KeyLow >= 2:
			mid := victim.KeyLow + (victim.KeyHigh-victim.KeyLow)/2
			c1 := &Partition{Token: newToken(), Parents: []string{victim.Token}, KeyLow: victim.KeyLow, KeyHigh: mid, Start: at}
			c2 := &Partition{Token: newToken(), Parents: []string{victim.Token}, KeyLow: mid, KeyHigh: victim.KeyHigh, Start: at}
			end(victim, c1, c2)
			s.partitions[c1.Token], s.partitions[c2.Token] = c1, c2
			live = append(live[:idx], append([]*Partition{c1, c2}, live[idx+1:]...)...)
			plan(c1)
			plan(c2)
			s.splits++
		default:
			// Replace with a single child covering the same range.
			c := &Partition{Token: newToken(), Parents: []string{victim.Token}, KeyLow: victim.KeyLow, KeyHigh: victim.KeyHigh, Start: at}
			end(victim, c)
			s.partitions[c.Token] = c
			live[idx] = c
			plan(c)
		}
	}
	assignUntil(horizonEnd)
}

func (s *Simulator) generateWrites(rng *rand.Rand, horizonEnd time.Time) []*Write {
	cfg := s.cfg
	if cfg.WritesPerSecond <= 0 {
		return nil
	}
	hotKeys := int64(float64(cfg.KeySpace) * cfg.HotKeyFraction)
	pickKey := func() int64 {
		if hotKeys > 0 && rng.Float64() < cfg.HotWriteShare {
			// Hot keys are spread through the key space so that a few partitions are hot.
			stride := int64(cfg.KeySpace) / hotKeys
			return rng.Int63n(hotKeys) * stride
		}
		return rng.Int63n(int64(cfg.KeySpace))
	}

	var writes []*Write
	seq := map[int64]int64{}
	t := cfg.Start
	for {
		gap := time.Duration(rng.ExpFloat64() / cfg.WritesPerSecond * float64(time.Second))
		if gap <= 0 {
			gap = time.Nanosecond
		}
		t = t.Add(gap)
		if !t.Before(horizonEnd) {
			break
		}
		k := pickKey()
		seq[k]++
		writes = append(writes, &Write{Key: k, Seq: seq[k], CommitTimestamp: t})
	}
	return writes
}

// Read implements screamer.ChangeStreamReader.
func (s *Simulator) Read(ctx context.Context, p *screamer.PartitionMetadata, fn func([]*screamer.ChangeRecord) error) error {
	if err := s.openStream(ctx, p.PartitionToken); err != nil {
		return err
	}
	defer s.closeStream()

	if err := sleep(ctx, s.cfg.QueryLatency); err != nil {
		return err
	}

	if p.IsRootPartition() {
		return s.readRoot(p, fn)
	}
	var failAt time.Time
	if d, ok := s.plannedFailure(); ok {
		failAt = time.Now().Add(d)
	}

	part, ok := s.partitions[p.PartitionToken]
	if !ok {
		return fmt.Errorf("simulator: unknown partition token %q", p.PartitionToken)
	}
	start := p.Watermark
	if start.Before(part.Start) {
		return fmt.Errorf("simulator: start %s is before partition %s starts at %s", start, part.Token, part.Start)
	}

	stop := p.EndTimestamp
	endsHere := false
	if !part.End.IsZero() && !part.End.After(stop) {
		stop, endsHere = part.End, true
	}

	hb := time.Duration(p.HeartbeatMillis) * time.Millisecond
	if hb <= 0 {
		hb = time.Second
	}

	i := sort.Search(len(part.writes), func(i int) bool { return !part.writes[i].CommitTimestamp.Before(start) })
	lastRow := time.Now()
	for {
		now := time.Now()
		limit := now
		if limit.After(stop) {
			limit = stop
		}

		var batch []*screamer.RawDataChangeRecord
		for i < len(part.writes) && !part.writes[i].CommitTimestamp.After(limit) && part.writes[i].CommitTimestamp.Before(stop) {
			batch = append(batch, toRecord(part.writes[i]))
			i++
		}
		switch {
		case len(batch) > 0:
			if err := fn([]*screamer.ChangeRecord{{DataChangeRecords: batch}}); err != nil {
				return err
			}
			lastRow = now
		case now.Sub(lastRow) >= hb && now.Before(stop):
			if err := fn([]*screamer.ChangeRecord{{HeartbeatRecords: []*screamer.HeartbeatRecord{{Timestamp: now}}}}); err != nil {
				return err
			}
			lastRow = now
		}

		if !failAt.IsZero() && !now.Before(failAt) {
			s.mu.Lock()
			s.stats.InjectedFailures++
			s.mu.Unlock()
			return status.Error(codes.Unavailable, "simulator: injected stream failure")
		}

		if !now.Before(stop) {
			if endsHere && len(part.Children) > 0 {
				cpr := &screamer.ChildPartitionsRecord{StartTimestamp: part.End, RecordSequence: "00000001"}
				for _, c := range part.Children {
					cpr.ChildPartitions = append(cpr.ChildPartitions, &screamer.ChildPartition{
						Token:                 c,
						ParentPartitionTokens: s.partitions[c].Parents,
					})
				}
				return fn([]*screamer.ChangeRecord{{ChildPartitionsRecords: []*screamer.ChildPartitionsRecord{cpr}}})
			}
			return nil
		}

		// Sleep until the next write, the next heartbeat, or the end of the query.
		wake := lastRow.Add(hb)
		if i < len(part.writes) && part.writes[i].CommitTimestamp.Before(wake) {
			wake = part.writes[i].CommitTimestamp
		}
		if stop.Before(wake) {
			wake = stop
		}
		if !failAt.IsZero() && failAt.Before(wake) {
			wake = failAt
		}
		if err := sleep(ctx, time.Until(wake)); err != nil {
			return err
		}
	}
}

// plannedFailure decides whether the next query fails, and after how long.
func (s *Simulator) plannedFailure() (time.Duration, bool) {
	if s.cfg.ReadFailureRate <= 0 {
		return 0, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.faults.Float64() >= s.cfg.ReadFailureRate {
		return 0, false
	}
	return time.Duration(s.faults.Int63n(int64(s.cfg.ReadFailureWithin))), true
}

// readRoot returns the partitions live at the query's start timestamp, as Spanner does
// for a query without a partition token.
func (s *Simulator) readRoot(p *screamer.PartitionMetadata, fn func([]*screamer.ChangeRecord) error) error {
	at := p.Watermark
	if at.Before(s.cfg.Start) {
		return fmt.Errorf("simulator: start %s is before the simulation starts at %s", at, s.cfg.Start)
	}
	cpr := &screamer.ChildPartitionsRecord{StartTimestamp: at, RecordSequence: "00000001"}
	for _, c := range s.LiveAt(at) {
		cpr.ChildPartitions = append(cpr.ChildPartitions, &screamer.ChildPartition{Token: c.Token, ParentPartitionTokens: []string{}})
	}
	return fn([]*screamer.ChangeRecord{{ChildPartitionsRecords: []*screamer.ChildPartitionsRecord{cpr}}})
}

func (s *Simulator) openStream(ctx context.Context, token string) error {
	if s.streams != nil {
		select {
		case s.streams <- struct{}{}:
		default:
			s.mu.Lock()
			s.stats.BlockedOnStreams++
			s.mu.Unlock()
			select {
			case s.streams <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Queries++
	s.stats.OpenStreams++
	if s.stats.OpenStreams > s.stats.MaxOpenStreams {
		s.stats.MaxOpenStreams = s.stats.OpenStreams
	}
	s.queried[token] = struct{}{}
	return nil
}

func (s *Simulator) closeStream() {
	s.mu.Lock()
	s.stats.OpenStreams--
	s.mu.Unlock()
	if s.streams != nil {
		<-s.streams
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const (
	// TableName is the table name on every simulated data change record.
	TableName = "SimTable"
	// KeyColumn and SeqColumn are the column names on simulated records:
	// the primary key and the per-key write sequence number.
	KeyColumn = "id"
	SeqColumn = "seq"
)

func toRecord(w *Write) *screamer.RawDataChangeRecord {
	return &screamer.RawDataChangeRecord{
		CommitTimestamp:                      w.CommitTimestamp,
		RecordSequence:                       "00000000",
		ServerTransactionID:                  fmt.Sprintf("tx-%d-%d", w.Key, w.Seq),
		IsLastRecordInTransactionInPartition: true,
		TableName:                            TableName,
		ColumnTypes: []*screamer.RawColumnType{
			{Name: KeyColumn, Type: jsonValue(map[string]interface{}{"code": "INT64"}), IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: SeqColumn, Type: jsonValue(map[string]interface{}{"code": "INT64"}), OrdinalPosition: 2},
		},
		Mods: []*screamer.RawMod{{
			Keys:      jsonValue(map[string]interface{}{KeyColumn: fmt.Sprint(w.Key)}),
			NewValues: jsonValue(map[string]interface{}{SeqColumn: fmt.Sprint(w.Seq)}),
			OldValues: spanner.NullJSON{},
		}},
		ModType:                         screamer.ModType_UPDATE,
		ValueCaptureType:                "NEW_VALUES",
		NumberOfRecordsInTransaction:    1,
		NumberOfPartitionsInTransaction: 1,
	}
}

func jsonValue(v map[string]interface{}) spanner.NullJSON {
	return spanner.NullJSON{Value: v, Valid: true}
}
