package screamer

// Consumer is the interface to consume the DataChangeRecord.
//
// Consume might be called from multiple goroutines and must be re-entrant safe.
type Consumer interface {
	// Consume processes a marshaled DataChangeRecord.
	Consume(change []byte) error
}

// ConsumerFunc type is an adapter to allow the use of ordinary functions as Consumer.
// The function receives json.Marshal(DataChangeRecord).
type ConsumerFunc func([]byte) error

// Consume calls f(change) for ConsumerFunc, allowing function types to satisfy the Consumer interface.
// The input is json.Marshal(DataChangeRecord.
func (f ConsumerFunc) Consume(change []byte) error {
	return f(change)
}
