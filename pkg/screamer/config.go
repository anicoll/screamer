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
}
