package screamer

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume might be called from multiple goroutines and must be re-entrant safe.
type Consumer interface {
	Consume(change []byte) error
}

// ConsumerFunc type is an adapter to allow the use of ordinary functions as Consumer.
// consumes json.Marshal(DataChangeRecord)
type ConsumerFunc func([]byte) error

// Consume calls f(change).
// consumes json.Marshal(DataChangeRecord)
func (f ConsumerFunc) Consume(change []byte) error {
	return f(change)
}
