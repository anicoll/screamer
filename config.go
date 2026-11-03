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
	// LeaseTTL is how long a partition lease lasts without renewal (0 = default, 30s).
	LeaseTTL time.Duration
	// DisableParking streams every partition instead of parking idle ones.
	DisableParking bool
	// ParkInterval is how long a parked partition waits between catch-up reads (0 = default, 10s).
	ParkInterval time.Duration
	// ParkIdleAfter is how long a partition delivers only heartbeats before it is parked (0 = default, 1m).
	ParkIdleAfter time.Duration
}
