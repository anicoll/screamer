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
}

// State represents the state of a partition in the change stream lifecycle.
type State string

const (
	// StateCreated indicates the partition is newly created and not yet scheduled.
	StateCreated State = "CREATED"
	// StateScheduled indicates the partition is scheduled for processing.
	StateScheduled State = "SCHEDULED"
	// StateRunning indicates the partition is currently being processed.
	StateRunning State = "RUNNING"
	// StateFinished indicates the partition has been fully processed.
	StateFinished State = "FINISHED"
)

const (
	// RootPartitionToken is the token value for the root partition.
	RootPartitionToken = "Parent0"
)

// IsRootPartition returns true if this partition is the root partition.
func (p *PartitionMetadata) IsRootPartition() bool {
	return p.PartitionToken == RootPartitionToken
}
