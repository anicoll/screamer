package soak

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
)

// Report is what Check found.
type Report struct {
	// AsOf is the time the writes were read at: writes committed after it are
	// not expected yet.
	AsOf time.Time `json:"as_of"`
	// Keys is how many keys had been written by AsOf.
	Keys int `json:"keys"`
	// Expected is the number of writes committed by AsOf.
	Expected int64 `json:"expected"`
	// Delivered is how many of those were delivered at least once.
	Delivered int64 `json:"delivered"`
	// Missing is how many were never delivered.
	Missing int64 `json:"missing"`
	// Duplicates is how many extra deliveries there were: at-least-once
	// redelivery after a restart, a handover or a nack.
	Duplicates int64 `json:"duplicates"`
	// OrderViolations is how many writes were first delivered after a later
	// write to the same key had been.
	OrderViolations int64 `json:"order_violations"`
	// Lag is the time from a write's commit to its first delivery.
	LagP50 time.Duration `json:"lag_p50"`
	LagP99 time.Duration `json:"lag_p99"`
	LagMax time.Duration `json:"lag_max"`
	// Samples are a few of the problems found, to start debugging from.
	MissingSamples   []Write          `json:"missing_samples,omitempty"`
	ReorderedSamples []ReorderedWrite `json:"reordered_samples,omitempty"`
}

// OK says whether every expected write was delivered, in order.
func (r Report) OK() bool { return r.Missing == 0 && r.OrderViolations == 0 }

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "as of %s: %d keys, %d writes expected, %d delivered, %d missing, %d duplicate deliveries, %d out of order\n",
		r.AsOf.Format(time.RFC3339), r.Keys, r.Expected, r.Delivered, r.Missing, r.Duplicates, r.OrderViolations)
	fmt.Fprintf(&b, "delivery lag: p50 %s, p99 %s, max %s\n", r.LagP50, r.LagP99, r.LagMax)
	for _, w := range r.MissingSamples {
		fmt.Fprintf(&b, "  missing: id=%d seq=%d\n", w.ID, w.Seq)
	}
	for _, w := range r.ReorderedSamples {
		fmt.Fprintf(&b, "  out of order: id=%d seq=%d first delivered at %s, after seq %d at %s\n",
			w.ID, w.Seq, w.DeliveredAt.Format(time.RFC3339Nano), w.AfterSeq, w.AfterDeliveredAt.Format(time.RFC3339Nano))
	}
	return b.String()
}

// Write is one write to one key.
type Write struct {
	ID  int64 `json:"id"`
	Seq int64 `json:"seq"`
}

// ReorderedWrite is a write first delivered after a later write to its key.
type ReorderedWrite struct {
	Write
	DeliveredAt      time.Time `json:"delivered_at"`
	AfterSeq         int64     `json:"after_seq"`
	AfterDeliveredAt time.Time `json:"after_delivered_at"`
}

const maxSamples = 20

// Check compares the deliveries with the writes committed by asOf.
//
// Every key's writes are numbered 1, 2, 3... so the events table read at asOf
// says exactly which writes to expect. Deliveries are ordered by the commit
// timestamp of the row that recorded them, which Spanner keeps consistent
// across runners and hosts, so a reordering between two runners is caught
// as well as one within a runner.
//
// The runners must have started reading before the writer's first write (or
// from a start timestamp before it), or the writes before they started are
// reported missing.
func Check(ctx context.Context, eventsClient, deliveriesClient *spanner.Client, t Tables, asOf time.Time) (Report, error) {
	rep := Report{AsOf: asOf}
	expected := make(map[int64]int64)
	iter := eventsClient.Single().WithTimestampBound(spanner.ReadTimestamp(asOf)).
		Query(ctx, spanner.Statement{SQL: fmt.Sprintf("SELECT Id, Seq FROM %s", t.Events)})
	if err := iter.Do(func(r *spanner.Row) error {
		var id, seq int64
		if err := r.Columns(&id, &seq); err != nil {
			return err
		}
		expected[id] = seq
		rep.Expected += seq
		return nil
	}); err != nil {
		return rep, fmt.Errorf("failed to read writes as of %s: %w", asOf.Format(time.RFC3339), err)
	}
	rep.Keys = len(expected)

	lags := newHistogram()
	var k keyDeliveries
	finish := func() {
		if k.seqs == nil {
			return
		}
		k.evaluate(expected[k.id], &rep, lags)
		delete(expected, k.id)
	}
	iter = deliveriesClient.Single().Query(ctx, spanner.Statement{SQL: fmt.Sprintf(
		"SELECT Id, Seq, DeliveredAt, CommitTimestamp FROM %s ORDER BY Id, Seq", t.Deliveries)})
	if err := iter.Do(func(r *spanner.Row) error {
		var id, seq int64
		var at time.Time
		var commit spanner.NullTime
		if err := r.Columns(&id, &seq, &at, &commit); err != nil {
			return err
		}
		if k.seqs == nil || id != k.id {
			finish()
			k = keyDeliveries{id: id, seqs: map[int64]*seqDelivery{}}
		}
		k.add(seq, at, commit.Time)
		return nil
	}); err != nil {
		return rep, fmt.Errorf("failed to read deliveries: %w", err)
	}
	finish()

	// Keys written but never delivered at all.
	ids := make([]int64, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		n := expected[id]
		rep.Missing += n
		for seq := int64(1); seq <= n && len(rep.MissingSamples) < maxSamples; seq++ {
			rep.MissingSamples = append(rep.MissingSamples, Write{ID: id, Seq: seq})
		}
	}
	rep.LagP50, rep.LagP99, rep.LagMax = lags.quantile(0.5), lags.quantile(0.99), lags.max
	return rep, nil
}

// keyDeliveries gathers the deliveries of one key.
type keyDeliveries struct {
	id   int64
	seqs map[int64]*seqDelivery
}

type seqDelivery struct {
	first  time.Time // when it was first delivered
	commit time.Time
	count  int64
}

func (k *keyDeliveries) add(seq int64, at, commit time.Time) {
	d, ok := k.seqs[seq]
	if !ok {
		k.seqs[seq] = &seqDelivery{first: at, commit: commit, count: 1}
		return
	}
	d.count++
	if at.Before(d.first) {
		d.first = at
	}
}

// evaluate adds this key's figures to rep, for writes 1 to expected.
func (k *keyDeliveries) evaluate(expected int64, rep *Report, lags *histogram) {
	// Walk the writes from the newest back, keeping the earliest first
	// delivery of any later write: a write first delivered after that was
	// delivered out of order. Two writes recorded in the same flush share a
	// timestamp, which is not a reordering.
	var laterSeq int64
	var laterAt time.Time
	for seq := expected; seq >= 1; seq-- {
		d, ok := k.seqs[seq]
		if !ok {
			rep.Missing++
			if len(rep.MissingSamples) < maxSamples {
				rep.MissingSamples = append(rep.MissingSamples, Write{ID: k.id, Seq: seq})
			}
			continue
		}
		rep.Delivered++
		rep.Duplicates += d.count - 1
		if !d.commit.IsZero() {
			lags.add(d.first.Sub(d.commit))
		}
		if laterSeq != 0 && d.first.After(laterAt) {
			rep.OrderViolations++
			if len(rep.ReorderedSamples) < maxSamples {
				rep.ReorderedSamples = append(rep.ReorderedSamples, ReorderedWrite{
					Write: Write{ID: k.id, Seq: seq}, DeliveredAt: d.first, AfterSeq: laterSeq, AfterDeliveredAt: laterAt,
				})
			}
		}
		if laterSeq == 0 || d.first.Before(laterAt) {
			laterSeq, laterAt = seq, d.first
		}
	}
}

// histogram counts durations in buckets that grow by 10%, so quantiles are
// within 10% without keeping every value.
type histogram struct {
	counts map[int]int64
	n      int64
	max    time.Duration
}

func newHistogram() *histogram { return &histogram{counts: map[int]int64{}} }

const histogramBase = 1.1

func (h *histogram) add(d time.Duration) {
	d = max(d, time.Millisecond)
	h.counts[int(math.Log(float64(d)/float64(time.Millisecond))/math.Log(histogramBase))]++
	h.n++
	h.max = max(h.max, d)
}

// quantile returns the upper bound of the bucket holding quantile q.
func (h *histogram) quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	buckets := make([]int, 0, len(h.counts))
	for b := range h.counts {
		buckets = append(buckets, b)
	}
	slices.Sort(buckets)
	target := int64(math.Ceil(q * float64(h.n)))
	var seen int64
	for _, b := range buckets {
		seen += h.counts[b]
		if seen >= target {
			return min(time.Duration(math.Pow(histogramBase, float64(b+1))*float64(time.Millisecond)), h.max)
		}
	}
	return h.max
}
