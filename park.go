package screamer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// PartitionParker is an optional extension of PartitionStorage that parks idle
// partitions. Both built-in storages implement it.
//
// Most partitions of a large change stream carry no writes most of the time,
// but each one still holds a query open, and a runner can only hold so many.
// A partition that has delivered nothing but heartbeats for a while (see
// WithParkIdleAfter) is parked instead: its query is closed, its lease is
// released, and it waits for a park interval (see WithParkInterval). Then any
// runner claims it and reads it in a short catch-up query that ends at the
// current time. If that finds changes, the runner goes back to streaming the
// partition. If not, it parks it again.
//
// Parking needs leases (see WithLeases). It is on by default (see
// WithParking). Subscribe returns an error if it is on and the PartitionStorage
// does not implement this interface.
type PartitionParker interface {
	// ParkPartition ends runnerID's lease on the partition at lease.Epoch,
	// moves its watermark forward to lease.Watermark, sets its state to PARKED
	// and its NextReadAt to after from now. It reports false, and writes
	// nothing, if runnerID no longer holds the lease.
	//
	// ClaimPartitions must then claim the partition, with its state still
	// PARKED, once NextReadAt has passed and when ClaimRequest.Parked is set.
	ParkPartition(ctx context.Context, runnerID string, lease Lease, after time.Duration) (bool, error)
}

const (
	// defaultParkInterval is how long a parked partition waits between
	// catch-up reads.
	defaultParkInterval = 10 * time.Second
	// defaultParkIdleAfter is how long a streaming partition delivers nothing
	// but heartbeats before it is parked.
	defaultParkIdleAfter = time.Minute
)

// errIdle stops a streaming read whose partition has been idle for parkIdleAfter.
var errIdle = errors.New("partition idle")

// readOutcome is how a read of a partition ended.
type readOutcome int

const (
	// readFinished: the partition ended, and is now FINISHED.
	readFinished readOutcome = iota
	// readIdle: a streaming read was stopped because the partition was idle.
	readIdle
	// readSliceEmpty: a catch-up read found no changes.
	readSliceEmpty
	// readSliceData: a catch-up read found changes, so the partition should be
	// streamed again.
	readSliceData
)

// sliceEnd returns the end of a catch-up read of p starting now. It is the
// current time, unless p's watermark is already there (the runner's clock may
// be behind Spanner's), and never after p's own end. The second result is false
// when the read reaches p's end, so it is an ordinary final read.
func sliceEnd(p *PartitionMetadata, now time.Time) (time.Time, bool) {
	end := now
	if !end.After(p.Watermark) {
		end = p.Watermark.Add(time.Second)
	}
	if !end.Before(p.EndTimestamp) {
		return p.EndTimestamp, false
	}
	return end, true
}

// park releases the lease on p and parks it until the next catch-up read.
// A failure is logged, not returned: the lease then expires and another runner
// claims the partition from its last checkpoint.
func (s *Subscriber) park(ctx context.Context, p *PartitionMetadata) {
	epoch, ok := s.leases.epoch(p.PartitionToken)
	if !ok {
		return // lost meanwhile; the new owner has it
	}
	// Stop renewing and checkpointing it first: from here the park call writes
	// its last watermark.
	s.leases.remove(p.PartitionToken)
	s.checkpoints.forget(p.PartitionToken)

	held, err := s.parker.ParkPartition(ctx, s.runnerID, Lease{PartitionToken: p.PartitionToken, Epoch: epoch, Watermark: p.Watermark}, s.parkInterval)
	switch {
	case err != nil:
		log.Warn().Err(err).Str("partition_token", p.PartitionToken).
			Msg("failed to park partition; another runner will take it over when its lease expires")
	case !held:
		s.leases.lost.Add(1)
		log.Warn().Str("partition_token", p.PartitionToken).Msg("could not park partition: another runner holds it now")
	default:
		s.parked.Add(1)
		log.Debug().Str("partition_token", p.PartitionToken).Time("watermark", p.Watermark).Msg("parked idle partition")
	}
}

// parkingCheck validates the parking configuration at Subscribe.
func (s *Subscriber) parkingCheck() error {
	if !s.useParking || s.leaser == nil {
		return nil
	}
	pk, ok := s.partitionStorage.(PartitionParker)
	if !ok {
		return fmt.Errorf("screamer: parking is on but %T does not implement screamer.PartitionParker; implement it, or turn parking off with WithParking(false) (see MIGRATION.md)", s.partitionStorage)
	}
	s.parker = pk
	return nil
}
