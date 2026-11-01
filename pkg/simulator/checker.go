package simulator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Checker is a screamer.Consumer that records what was delivered and checks it
// against the simulator's ground truth.
type Checker struct {
	sim *Simulator

	mu         sync.Mutex
	delivered  map[writeID]int // delivery count per write
	lastSeq    map[int64]int64 // highest seq delivered so far, per key
	violations []OrderViolation
	parseErrs  int
}

type writeID struct{ key, seq int64 }

// OrderViolation is a write delivered after a later write for the same key.
type OrderViolation struct {
	Key       int64
	Seq       int64 // the write delivered late
	AfterSeq  int64 // the later write that was delivered before it
	Partition string
}

// Report summarises a run.
type Report struct {
	// Expected is the number of writes with a commit timestamp before the cutoff.
	Expected int
	// Delivered is the number of those writes delivered at least once.
	Delivered int
	// Missing is Expected minus Delivered.
	Missing int
	// Duplicates is the number of extra deliveries (at-least-once redelivery).
	Duplicates int
	// OrderViolations is the number of writes delivered after a later write for the same key.
	OrderViolations int
	// PartitionsWithMissing is the number of partitions that had at least one missing write.
	PartitionsWithMissing int
	ParseErrors           int
}

func (r Report) String() string {
	return fmt.Sprintf("expected=%d delivered=%d missing=%d (in %d partitions) duplicates=%d order_violations=%d parse_errors=%d",
		r.Expected, r.Delivered, r.Missing, r.PartitionsWithMissing, r.Duplicates, r.OrderViolations, r.ParseErrors)
}

// NewChecker returns a Checker for sim.
func NewChecker(sim *Simulator) *Checker {
	return &Checker{
		sim:       sim,
		delivered: make(map[writeID]int),
		lastSeq:   make(map[int64]int64),
	}
}

type deliveredRecord struct {
	PartitionToken string `json:"partition_token"`
	Mods           []struct {
		Keys      map[string]string `json:"keys"`
		NewValues map[string]string `json:"new_values"`
	} `json:"mods"`
}

// Consume implements screamer.Consumer. It is safe for concurrent use.
func (c *Checker) Consume(change []byte) error {
	var rec deliveredRecord
	err := json.Unmarshal(change, &rec)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.parseErrs++
		return nil
	}
	for _, m := range rec.Mods {
		key, err1 := strconv.ParseInt(m.Keys[KeyColumn], 10, 64)
		seq, err2 := strconv.ParseInt(m.NewValues[SeqColumn], 10, 64)
		if err1 != nil || err2 != nil {
			c.parseErrs++
			continue
		}
		id := writeID{key, seq}
		c.delivered[id]++
		if c.delivered[id] > 1 {
			continue // redelivery, not an ordering problem
		}
		if last := c.lastSeq[key]; seq < last {
			c.violations = append(c.violations, OrderViolation{Key: key, Seq: seq, AfterSeq: last, Partition: rec.PartitionToken})
		} else {
			c.lastSeq[key] = seq
		}
	}
	return nil
}

// Violations returns the ordering violations seen so far.
func (c *Checker) Violations() []OrderViolation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]OrderViolation(nil), c.violations...)
}

// Report compares deliveries with every write committed before cutoff.
func (c *Checker) Report(cutoff time.Time) Report {
	c.mu.Lock()
	defer c.mu.Unlock()

	r := Report{OrderViolations: len(c.violations), ParseErrors: c.parseErrs}
	missingIn := map[string]struct{}{}
	for _, w := range c.sim.writes {
		if !w.CommitTimestamp.Before(cutoff) {
			break
		}
		r.Expected++
		if n := c.delivered[writeID{w.Key, w.Seq}]; n > 0 {
			r.Delivered++
			r.Duplicates += n - 1
		} else {
			missingIn[w.Partition] = struct{}{}
		}
	}
	r.Missing = r.Expected - r.Delivered
	r.PartitionsWithMissing = len(missingIn)
	return r
}
