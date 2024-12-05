package screamer

import "github.com/anicoll/screamer/pkg/model"

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume might be called from multiple goroutines and must be re-entrant safe.
type Consumer interface {
	Consume(change *model.DataChangeRecord) error
}

// ConsumerFunc type is an adapter to allow the use of ordinary functions as Consumer.
type ConsumerFunc func(*model.DataChangeRecord) error

// Consume calls f(change).
func (f ConsumerFunc) Consume(change *model.DataChangeRecord) error {
	return f(change)
}
