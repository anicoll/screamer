package screamer

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PartitionCounter is an optional extension of PartitionStorage. When the storage
// implements it, the subscriber publishes cluster-wide scaling metrics
// (unfinished partitions and desired runners). Both built-in storages implement it.
type PartitionCounter interface {
	// CountUnfinishedPartitions returns the number of partitions that are not FINISHED.
	CountUnfinishedPartitions(ctx context.Context) (int, error)
}

// TargetUtilization is the share of each runner's WithMaxConcurrentPartitions that
// DesiredRunners plans to use, leaving headroom for partitions that split.
const TargetUtilization = 0.75

// clusterStatsInterval is how often the subscriber reads cluster-wide stats from
// the partition storage.
const clusterStatsInterval = 15 * time.Second

// ClusterMetrics are cluster-wide figures read from the partition storage. Every
// runner computes the same values, so dashboards and autoscalers should take the
// max across runners rather than the sum.
type ClusterMetrics struct {
	// UpdatedAt is when these figures were last read. Zero until the first read.
	UpdatedAt time.Time
	// UnfinishedPartitions is the number of partitions not yet FINISHED, or -1 if the
	// partition storage does not implement PartitionCounter.
	UnfinishedPartitions int
	// LiveRunners is the number of runners that refreshed recently.
	LiveRunners int
	// DesiredRunners is how many runners are needed to serve the cluster's
	// Demand at TargetUtilization:
	// ceil(Demand / (MaxConcurrentPartitions × TargetUtilization)).
	// It is 0 when it cannot be computed: no partition limit is set, or the
	// storage does not count partitions.
	DesiredRunners int
	// WatermarkLag is how far the oldest unfinished partition's watermark is behind
	// the current time. When it keeps growing, the runners are not keeping up.
	WatermarkLag time.Duration
	// PartitionsByState counts unfinished partitions by state, when the storage
	// implements PartitionStateCounter: CREATED ones wait to be read, SCHEDULED
	// and RUNNING ones stream, PARKED ones are idle.
	PartitionsByState map[State]int
	// Demand is the number of query slots the cluster needs: one per partition
	// that streams or waits to, plus the share of a slot that each parked
	// partition's catch-up reads use. DesiredRunners is computed from it. Equal
	// to UnfinishedPartitions when the storage does not count by state.
	Demand int
}

// DesiredRunners returns ceil(unfinished / (maxConcurrentPartitions × TargetUtilization)),
// at least 1, or 0 if maxConcurrentPartitions is 0 (no limit) or unfinished is unknown (< 0).
func DesiredRunners(unfinished, maxConcurrentPartitions int) int {
	if maxConcurrentPartitions <= 0 || unfinished < 0 {
		return 0
	}
	n := int(math.Ceil(float64(unfinished) / (float64(maxConcurrentPartitions) * TargetUtilization)))
	if n < 1 {
		return 1
	}
	return n
}

type clusterStats struct {
	mu sync.RWMutex
	m  ClusterMetrics
}

// GetClusterMetrics returns the most recent cluster-wide metrics.
func (s *Subscriber) GetClusterMetrics() ClusterMetrics {
	s.cluster.mu.RLock()
	defer s.cluster.mu.RUnlock()
	return s.cluster.m
}

// refreshClusterStats reads cluster-wide figures from the partition storage.
// Errors are logged and leave the previous figures in place: metrics must never
// stop the subscriber.
func (s *Subscriber) refreshClusterStats(ctx context.Context) {
	m := ClusterMetrics{UnfinishedPartitions: -1}

	if pc, ok := s.partitionStorage.(PartitionCounter); ok {
		n, err := pc.CountUnfinishedPartitions(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("failed to count unfinished partitions for metrics")
			return
		}
		m.UnfinishedPartitions = n
	}

	runners, err := s.partitionStorage.GetActiveRunnerCount(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to count live runners for metrics")
		return
	}
	m.LiveRunners = runners

	p, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to read min watermark for metrics")
		return
	}
	m.UpdatedAt = time.Now()
	if p != nil {
		if lag := m.UpdatedAt.Sub(p.Watermark); lag > 0 {
			m.WatermarkLag = lag
		}
	}
	m.Demand = m.UnfinishedPartitions
	if sc, ok := s.partitionStorage.(PartitionStateCounter); ok {
		counts, err := sc.CountPartitionsByState(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("failed to count partitions by state for metrics")
			return
		}
		m.PartitionsByState = copyCounts(counts)
		m.Demand = s.demand(counts)
		if s.useFairShare && s.leaser != nil {
			s.fair.target.Store(int64(fairShareTarget(counts[StateCreated]+counts[StateScheduled]+counts[StateRunning], m.LiveRunners)))
		}
	}
	m.DesiredRunners = DesiredRunners(m.Demand, s.maxConcurrentPartitions)

	s.cluster.mu.Lock()
	s.cluster.m = m
	s.cluster.mu.Unlock()

	s.rebalance()
}

// clusterStatsLoop refreshes cluster metrics until ctx is done or done is closed.
func (s *Subscriber) clusterStatsLoop(ctx context.Context, done <-chan struct{}) error {
	s.refreshClusterStats(ctx)
	ticker := time.NewTicker(clusterStatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.refreshClusterStats(ctx)
		case <-done:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// registerMetrics creates the OpenTelemetry instruments. They are observed on the
// meter provider's schedule, from values the subscriber already keeps.
func (s *Subscriber) registerMetrics(mp metric.MeterProvider) (metric.Registration, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter("github.com/anicoll/screamer")

	active, err := meter.Int64ObservableGauge("screamer.partitions.active",
		metric.WithDescription("Partitions this runner is currently reading."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	capacity, err := meter.Int64ObservableGauge("screamer.partitions.capacity",
		metric.WithDescription("This runner's WithMaxConcurrentPartitions limit (0 = unlimited)."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	retries, err := meter.Int64ObservableCounter("screamer.partition.retries",
		metric.WithDescription("Partition reads retried after a failure on this runner."), metric.WithUnit("{retry}"))
	if err != nil {
		return nil, err
	}
	unfinished, err := meter.Int64ObservableGauge("screamer.partitions.unfinished",
		metric.WithDescription("Cluster-wide partitions not yet finished. Same on every runner: aggregate with max."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	live, err := meter.Int64ObservableGauge("screamer.runners.live",
		metric.WithDescription("Runners that refreshed recently. Same on every runner: aggregate with max."), metric.WithUnit("{runner}"))
	if err != nil {
		return nil, err
	}
	desired, err := meter.Int64ObservableGauge("screamer.runners.desired",
		metric.WithDescription("Runners needed to read every unfinished partition at 75% of WithMaxConcurrentPartitions. Scale on max() of this."), metric.WithUnit("{runner}"))
	if err != nil {
		return nil, err
	}
	lag, err := meter.Float64ObservableGauge("screamer.watermark.lag",
		metric.WithDescription("How far the oldest unfinished partition's watermark is behind now. Same on every runner: aggregate with max."), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	leasesHeld, err := meter.Int64ObservableGauge("screamer.leases.held",
		metric.WithDescription("Partition leases this runner holds."), metric.WithUnit("{lease}"))
	if err != nil {
		return nil, err
	}
	leasesLost, err := meter.Int64ObservableCounter("screamer.leases.lost",
		metric.WithDescription("Leases this runner found taken by another runner, which stopped it reading those partitions. Should stay near zero."), metric.WithUnit("{lease}"))
	if err != nil {
		return nil, err
	}
	takeovers, err := meter.Int64ObservableCounter("screamer.leases.takeovers",
		metric.WithDescription("Partitions this runner claimed after another runner's lease expired or was released. Spikes mean runners died or stopped."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}

	parks, err := meter.Int64ObservableCounter("screamer.partitions.parks",
		metric.WithDescription("Times this runner parked an idle partition, including parking it again after a catch-up read found nothing."), metric.WithUnit("{park}"))
	if err != nil {
		return nil, err
	}

	byState, err := meter.Int64ObservableGauge("screamer.partitions.by_state",
		metric.WithDescription("Cluster-wide unfinished partitions by state (CREATED waits, SCHEDULED and RUNNING stream, PARKED is idle). Same on every runner: aggregate with max."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	handovers, err := meter.Int64ObservableCounter("screamer.partitions.handovers",
		metric.WithDescription("Partitions this runner handed to other runners because it held more than its fair share."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}
	share, err := meter.Int64ObservableGauge("screamer.partitions.fair_share",
		metric.WithDescription("The most partitions this runner aims to hold. 0 means no limit."), metric.WithUnit("{partition}"))
	if err != nil {
		return nil, err
	}

	attrs := metric.WithAttributes(
		attribute.String("stream", s.streamName),
		attribute.String("runner_id", s.runnerID),
	)
	clusterAttrs := metric.WithAttributes(attribute.String("stream", s.streamName))

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(active, int64(s.getRunningPartitionCount()), attrs)
		o.ObserveInt64(capacity, int64(s.maxConcurrentPartitions), attrs)
		o.ObserveInt64(retries, s.partitionRetries.Load(), attrs)
		if s.leaser != nil {
			o.ObserveInt64(leasesHeld, int64(s.leases.count()), attrs)
			o.ObserveInt64(leasesLost, s.leases.lostCount(), attrs)
			o.ObserveInt64(takeovers, s.leases.takeoverCount(), attrs)
		}
		if s.parker != nil {
			o.ObserveInt64(parks, s.parked.Load(), attrs)
		}
		if s.leaser != nil && s.useFairShare {
			o.ObserveInt64(handovers, s.fair.handovers.Load(), attrs)
			o.ObserveInt64(share, s.fair.target.Load(), attrs)
		}

		c := s.GetClusterMetrics()
		if c.UpdatedAt.IsZero() {
			return nil
		}
		if c.UnfinishedPartitions >= 0 {
			o.ObserveInt64(unfinished, int64(c.UnfinishedPartitions), clusterAttrs)
		}
		o.ObserveInt64(live, int64(c.LiveRunners), clusterAttrs)
		if c.DesiredRunners > 0 {
			o.ObserveInt64(desired, int64(c.DesiredRunners), clusterAttrs)
		}
		o.ObserveFloat64(lag, c.WatermarkLag.Seconds(), clusterAttrs)
		for state, n := range c.PartitionsByState {
			o.ObserveInt64(byState, int64(n), metric.WithAttributes(attribute.String("stream", s.streamName), attribute.String("state", string(state))))
		}
		return nil
	}, active, capacity, retries, unfinished, live, desired, lag, leasesHeld, leasesLost, takeovers, parks, byState, handovers, share)
}
