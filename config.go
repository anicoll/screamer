package screamer

import "time"

type Config struct {
	DSN               string
	Stream            string
	MetadataTable     *string
	Start             *time.Time
	End               *time.Time
	HeartbeatInterval *time.Duration
	PartitionDSN      *string
	Priority          int32
	// MaxConcurrentPartitions limits partitions read at once by this runner (0 = unlimited).
	MaxConcurrentPartitions int
}
