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

// AckFunc acknowledges or nacks a record.
// Passing nil signals successful processing; passing a non-nil error nacks the record,
// which stops partition processing and surfaces the error to the subscriber.
type AckFunc func(error)

// ConsumerWithAck is the interface to consume DataChangeRecords with explicit per-record acknowledgment.
//
// Consume is called for each record. The ack function must be called exactly once when
// processing is complete — either with nil to ack, or with an error to nack.
// Consume itself may return immediately (async processing is allowed); the subscriber will
// not advance to the next batch until every ack from the current batch has been received.
//
// If Consume returns a non-nil error the subscriber stops immediately without waiting for ack.
//
// Consume might be called from multiple goroutines and must be re-entrant safe.
type ConsumerWithAck interface {
	Consume(change []byte, ack AckFunc) error
}

// ConsumerFuncWithAck is an adapter to allow the use of ordinary functions as ConsumerWithAck.
type ConsumerFuncWithAck func([]byte, AckFunc) error

// Consume calls f(change, ack) for ConsumerFuncWithAck.
func (f ConsumerFuncWithAck) Consume(change []byte, ack AckFunc) error {
	return f(change, ack)
}
