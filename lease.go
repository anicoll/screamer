package screamer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// PartitionLeaser is an optional extension of PartitionStorage that gives each
// partition a time-limited lease and an epoch. Both built-in storages implement
// it.
//
// Without leases, a runner owns a partition for as long as it refreshes its
// Runner row, and another runner takes over a partition whose runner has not
// refreshed for 3 seconds. A pause longer than that (a GC pause, a slow
// metadata write, a network blip) gives the partition two owners, and nothing
// stops the old one writing an older watermark over the new owner's.
//
// With leases, a runner owns a partition until its lease expires (30 seconds by
// default, see WithLeaseTTL), and it renews every lease it holds when it writes
// checkpoints. Every claim increments the partition's epoch, and every
// watermark write is conditional on the writer still holding the lease at that
// epoch. A runner that comes back from a long pause finds its writes rejected,
// stops reading those partitions, and writes nothing.
//
// Leases are on by default (see WithLeases). Subscribe returns an error if they
// are on and the PartitionStorage does not implement this interface.
type PartitionLeaser interface {
	// ClaimPartitions leases up to req.Limit partitions to req.RunnerID for
	// req.TTL and returns them with their new Owner, Epoch and LeaseExpiresAt.
	//
	// It claims CREATED partitions that are ready to read (with req.ParentGating,
	// the same rule as ParentGatedScheduler; otherwise StartTimestamp at or after
	// req.MinWatermark), marking them SCHEDULED, and SCHEDULED or RUNNING
	// partitions whose lease has expired or that have no lease. Each claim must
	// increment the partition's epoch, and two runners must never both claim
	// the same partition at the same epoch.
	ClaimPartitions(ctx context.Context, req ClaimRequest) ([]*PartitionMetadata, error)

	// RenewLeases extends every lease in leases that runnerID still holds at the
	// given epoch to ttl from now and, in the same write, moves each one's
	// watermark forward to the given watermark (never backwards; the zero time
	// leaves it as it is). It returns the tokens of the leases runnerID no
	// longer holds: another runner claimed them, or they finished. Nothing is
	// written for those.
	RenewLeases(ctx context.Context, runnerID string, ttl time.Duration, leases []Lease) (lost []string, err error)

	// ReleaseLeases ends the leases runnerID still holds at the given epochs,
	// after moving each watermark forward as RenewLeases does, so that another
	// runner can claim them straight away.
	ReleaseLeases(ctx context.Context, runnerID string, leases []Lease) error
}

// ClaimRequest says which partitions ClaimPartitions may claim, and for whom.
type ClaimRequest struct {
	// RunnerID is the runner to lease the partitions to.
	RunnerID string
	// TTL is how long the lease lasts unless it is renewed.
	TTL time.Duration
	// Limit is the most partitions to claim.
	Limit int
	// ParentGating claims a CREATED partition only once all its parents have
	// finished. When false, MinWatermark is used instead.
	ParentGating bool
	// MinWatermark is the lowest StartTimestamp of a CREATED partition that may
	// be claimed when ParentGating is false.
	MinWatermark time.Time
	// Parked also claims PARKED partitions whose NextReadAt has passed (see
	// PartitionParker). Their state stays PARKED until UpdateToRunning.
	Parked bool
	// Exclude lists partitions not to claim: ones this runner just handed to
	// other runners to even out the load (see WithFairShare).
	Exclude []string
}

// Lease identifies a lease held by a runner, with the watermark to write when
// it is renewed or released.
type Lease struct {
	PartitionToken string
	Epoch          int64
	// Watermark is written if it is later than the stored one. The zero time
	// writes nothing.
	Watermark time.Time
}

const (
	// defaultLeaseTTL is how long a lease lasts without renewal.
	defaultLeaseTTL = 30 * time.Second
	// minLeaseTTL keeps a lease long enough to be renewed a few times over.
	minLeaseTTL = 3 * time.Second
)

// errLeaseLost is the cause given to a partition's context when its lease was
// claimed by another runner.
var errLeaseLost = errors.New("partition lease lost to another runner")

// heldLease is a lease this runner holds on a partition it is reading.
type heldLease struct {
	epoch       int64
	cancel      context.CancelCauseFunc
	lastData    time.Time // when the partition last delivered data, or was claimed
	handingOver bool      // being handed to another runner (see rebalance)
}

// leaseSet tracks the leases this runner holds.
type leaseSet struct {
	mu   sync.Mutex
	held map[string]heldLease

	lost      atomic.Int64 // leases found taken by another runner
	takeovers atomic.Int64 // partitions claimed after another runner's lease expired
}

func newLeaseSet() *leaseSet {
	return &leaseSet{held: make(map[string]heldLease)}
}

func (l *leaseSet) add(token string, epoch int64, cancel context.CancelCauseFunc) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held[token] = heldLease{epoch: epoch, cancel: cancel, lastData: time.Now()}
}

func (l *leaseSet) remove(token string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.held, token)
}

func (l *leaseSet) epoch(token string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.held[token]
	return h.epoch, ok
}

func (l *leaseSet) count() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.held)
}

func (l *leaseSet) lostCount() int64 {
	if l == nil {
		return 0
	}
	return l.lost.Load()
}

func (l *leaseSet) takeoverCount() int64 {
	if l == nil {
		return 0
	}
	return l.takeovers.Load()
}

// leases returns every held lease, with the watermark from pending where there is one.
func (l *leaseSet) leases(pending map[string]time.Time) []Lease {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Lease, 0, len(l.held))
	for token, h := range l.held {
		out = append(out, Lease{PartitionToken: token, Epoch: h.epoch, Watermark: pending[token]})
	}
	return out
}

// drop forgets the given leases and stops reading their partitions.
func (l *leaseSet) drop(tokens []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, token := range tokens {
		h, ok := l.held[token]
		if !ok {
			continue
		}
		delete(l.held, token)
		h.cancel(errLeaseLost)
		l.lost.Add(1)
	}
}

// epochOf returns a partition's epoch, treating a missing one as 0.
func epochOf(p *PartitionMetadata) int64 {
	if p.Epoch == nil {
		return 0
	}
	return *p.Epoch
}

// renewInterval is how often every held lease is renewed: a third of the TTL,
// so two renewals in a row can fail before a lease lapses.
func (s *Subscriber) renewInterval() time.Duration {
	return s.leaseTTL / 3
}

// claimPartitions claims the next partitions to read.
func (s *Subscriber) claimPartitions(ctx context.Context, minWatermark time.Time, limit int) ([]*PartitionMetadata, error) {
	ps, err := s.leaser.ClaimPartitions(ctx, ClaimRequest{
		RunnerID:     s.runnerID,
		TTL:          s.leaseTTL,
		Limit:        limit,
		ParentGating: s.parentGating,
		MinWatermark: minWatermark,
		Parked:       s.parker != nil,
		Exclude:      s.fair.recentlyHandedOver(),
	})
	if err != nil {
		return nil, err
	}
	for _, p := range ps {
		// Epoch 1 is a first claim. Anything later was claimed before, by a
		// runner whose lease has since expired or that released it.
		if epochOf(p) > 1 {
			s.leases.takeovers.Add(1)
		}
	}
	return ps, nil
}

// leasedContext returns the context to read p with while its lease is held.
// The lease is forgotten when release is called, unless the subscriber is
// stopping: then the final flush releases it.
func (s *Subscriber) leasedContext(ctx context.Context, p *PartitionMetadata) (context.Context, func()) {
	if s.leaser == nil {
		return ctx, func() {}
	}
	pctx, cancel := context.WithCancelCause(ctx)
	s.leases.add(p.PartitionToken, epochOf(p), cancel)
	return pctx, func() {
		if ctx.Err() == nil {
			s.leases.remove(p.PartitionToken)
		}
		cancel(nil)
	}
}

// renewLeases writes the given watermarks under this runner's leases and, when
// all is true, renews every other lease it holds as well. Partitions whose
// lease was lost stop being read. Watermarks that could not be written are
// returned to the checkpointer, except for lost partitions.
func (s *Subscriber) renewLeases(ctx context.Context, ws []PartitionWatermark, all bool) error {
	pending := make(map[string]time.Time, len(ws))
	for _, w := range ws {
		pending[w.PartitionToken] = w.Watermark
	}
	var leases []Lease
	if all {
		leases = s.leases.leases(pending)
	} else {
		leases = make([]Lease, 0, len(ws))
		for _, w := range ws {
			if epoch, ok := s.leases.epoch(w.PartitionToken); ok {
				leases = append(leases, Lease{PartitionToken: w.PartitionToken, Epoch: epoch, Watermark: w.Watermark})
			}
		}
	}
	if len(leases) == 0 {
		return nil
	}
	lost, err := s.leaser.RenewLeases(ctx, s.runnerID, s.leaseTTL, leases)
	if err != nil {
		s.checkpoints.restore(ws)
		return fmt.Errorf("failed to renew %d partition leases: %w", len(leases), err)
	}
	if len(lost) > 0 {
		log.Warn().Str("runner_id", s.runnerID).Strs("partition_tokens", lost).
			Msg("another runner holds these partitions now; stopped reading them")
		s.leases.drop(lost)
	}
	return nil
}

// releaseLeases hands back every lease this runner holds, with its last
// watermark, so other runners can claim the partitions without waiting for the
// leases to expire.
func (s *Subscriber) releaseLeases(ctx context.Context) error {
	ws := s.checkpoints.take()
	pending := make(map[string]time.Time, len(ws))
	for _, w := range ws {
		pending[w.PartitionToken] = w.Watermark
	}
	leases := s.leases.leases(pending)
	if len(leases) == 0 {
		return nil
	}
	if err := s.leaser.ReleaseLeases(ctx, s.runnerID, leases); err != nil {
		return fmt.Errorf("failed to release %d partition leases: %w", len(leases), err)
	}
	log.Info().Str("runner_id", s.runnerID).Int("partitions", len(leases)).Msg("released partition leases")
	return nil
}
