package screamer

import "time"

// PartitionMetadata represents metadata for a change stream partition, including its state, timing, and parent/child relationships.
type PartitionMetadata struct {
	PartitionToken  string     `spanner:"PartitionToken" json:"partition_token"`
	ParentTokens    []string   `spanner:"ParentTokens" json:"parent_tokens"`
	StartTimestamp  time.Time  `spanner:"StartTimestamp" json:"start_timestamp"`
	EndTimestamp    time.Time  `spanner:"EndTimestamp" json:"end_timestamp"`
	HeartbeatMillis int64      `spanner:"HeartbeatMillis" json:"heartbeat_millis"`
	State           State      `spanner:"State" json:"state"`
	Watermark       time.Time  `spanner:"Watermark" json:"watermark"`
	CreatedAt       time.Time  `spanner:"CreatedAt" json:"created_at"`
	ScheduledAt     *time.Time `spanner:"ScheduledAt" json:"scheduled_at,omitempty"`
	RunningAt       *time.Time `spanner:"RunningAt" json:"running_at,omitempty"`
	FinishedAt      *time.Time `spanner:"FinishedAt" json:"finished_at,omitempty"`

	// Owner is the runner holding the partition's lease, if any. Only set when
	// leases are on (see PartitionLeaser).
	Owner *string `spanner:"Owner" json:"owner,omitempty"`
	// Epoch counts the times the partition has been claimed. Each claim
	// increments it, and a lease is only valid at the epoch it was claimed at.
	Epoch *int64 `spanner:"Epoch" json:"epoch,omitempty"`
	// LeaseExpiresAt is when the owner's lease lapses unless it is renewed.
	LeaseExpiresAt *time.Time `spanner:"LeaseExpiresAt" json:"lease_expires_at,omitempty"`
	// NextReadAt is when a PARKED partition is next read.
	NextReadAt *time.Time `spanner:"NextReadAt" json:"next_read_at,omitempty"`
}

// State represents the state of a partition in the change stream lifecycle.
type State string

const (
	// StateCreated indicates the partition is newly created and not yet scheduled.
	StateCreated   State = "CREATED"
	// StateScheduled indicates the partition is scheduled for processing.
	StateScheduled State = "SCHEDULED"
	// StateRunning indicates the partition is currently being processed.
	StateRunning   State = "RUNNING"
	// StateParked indicates the partition is idle and waits, with no query
	// open, until its NextReadAt (see PartitionParker).
	StateParked State = "PARKED"
	// StateFinished indicates the partition has been fully processed.
	StateFinished  State = "FINISHED"
)

const (
	// RootPartitionToken is the token value for the root partition.
	RootPartitionToken = "Parent0"
)

// IsRootPartition returns true if this partition is the root partition.
func (p *PartitionMetadata) IsRootPartition() bool {
	return p.PartitionToken == RootPartitionToken
}
