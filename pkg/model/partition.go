package model

import "time"

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

type State string

const (
	StateCreated   State = "CREATED"
	StateScheduled State = "SCHEDULED"
	StateRunning   State = "RUNNING"
	StateFinished  State = "FINISHED"
)

const (
	RootPartitionToken = "Parent0"
)

// IsRootPartition returns true if this is root partition.
func (p *PartitionMetadata) IsRootPartition() bool {
	return p.PartitionToken == RootPartitionToken
}
