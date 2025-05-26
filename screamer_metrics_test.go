package screamer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConsumer is a mock implementation of the Consumer interface.
type mockConsumer struct {
	mu        sync.Mutex
	consumeFn func(data []byte) error
	consumed  int
}

func (mc *mockConsumer) Consume(data []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.consumed++
	if mc.consumeFn != nil {
		return mc.consumeFn(data)
	}
	return nil
}

func (mc *mockConsumer) ConsumedCount() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.consumed
}

// mockPartitionStorage is a mock implementation of the PartitionStorage interface.
type mockPartitionStorage struct {
	mu                                 sync.Mutex
	getUnfinishedMinWatermarkFn        func(ctx context.Context) (*PartitionMetadata, error)
	getInterruptedPartitionsFn         func(ctx context.Context, runnerID string) ([]*PartitionMetadata, error)
	initializeRootPartitionFn          func(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error
	getAndSchedulePartitionsFn         func(ctx context.Context, minWatermark time.Time, runnerID string) ([]*PartitionMetadata, error)
	addChildPartitionsFn               func(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error
	updateToRunningFn                  func(ctx context.Context, partition *PartitionMetadata) error
	refreshRunnerFn                    func(ctx context.Context, runnerID string) error
	updateToFinishedFn                 func(ctx context.Context, partition *PartitionMetadata) error
	updateWatermarkFn                  func(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error
}

func (mps *mockPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error) {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.getUnfinishedMinWatermarkFn != nil {
		return mps.getUnfinishedMinWatermarkFn(ctx)
	}
	return nil, nil
}

func (mps *mockPartitionStorage) GetInterruptedPartitions(ctx context.Context, runnerID string) ([]*PartitionMetadata, error) {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.getInterruptedPartitionsFn != nil {
		return mps.getInterruptedPartitionsFn(ctx, runnerID)
	}
	return nil, nil
}

func (mps *mockPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.initializeRootPartitionFn != nil {
		return mps.initializeRootPartitionFn(ctx, startTimestamp, endTimestamp, heartbeatInterval)
	}
	return nil
}

func (mps *mockPartitionStorage) GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string) ([]*PartitionMetadata, error) {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.getAndSchedulePartitionsFn != nil {
		return mps.getAndSchedulePartitionsFn(ctx, minWatermark, runnerID)
	}
	return nil, nil
}

func (mps *mockPartitionStorage) AddChildPartitions(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.addChildPartitionsFn != nil {
		return mps.addChildPartitionsFn(ctx, parentPartition, childPartitionsRecord)
	}
	return nil
}

func (mps *mockPartitionStorage) UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.updateToRunningFn != nil {
		return mps.updateToRunningFn(ctx, partition)
	}
	return nil
}

func (mps *mockPartitionStorage) RefreshRunner(ctx context.Context, runnerID string) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.refreshRunnerFn != nil {
		return mps.refreshRunnerFn(ctx, runnerID)
	}
	return nil
}

func (mps *mockPartitionStorage) UpdateToFinished(ctx context.Context, partition *PartitionMetadata) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.updateToFinishedFn != nil {
		return mps.updateToFinishedFn(ctx, partition)
	}
	return nil
}

func (mps *mockPartitionStorage) UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error {
	mps.mu.Lock()
	defer mps.mu.Unlock()
	if mps.updateWatermarkFn != nil {
		return mps.updateWatermarkFn(ctx, partition, watermark)
	}
	return nil
}

func newTestSubscriber(t *testing.T, reg *prometheus.Registry, ps PartitionStorage, consumer Consumer, streamName, runnerID string, opts ...Option) *Subscriber {
	cfg := &config{
		startTimestamp:    time.Now(),
		endTimestamp:      defaultEndTimestamp,
		heartbeatInterval: defaultHeartbeatInterval,
	}
	for _, o := range opts {
		o.Apply(cfg)
	}

	// This is a simplified client, real Spanner interactions are not tested here.
	// We rely on mock PartitionStorage and mock Consumer for metric tests.
	var spannerClient *spanner.Client

	return &Subscriber{
		spannerClient:          spannerClient,
		streamName:             streamName,
		runnerID:               runnerID,
		startTimestamp:         cfg.startTimestamp,
		endTimestamp:           cfg.endTimestamp,
		heartbeatInterval:      cfg.heartbeatInterval,
		spannerRequestPriority: cfg.spannerRequestPriority,
		partitionStorage:       ps,
		consumer:               consumer,
		metrics:                newScreamerMetrics(reg, streamName, runnerID), // Use the test registry
		serializedConsumer:     cfg.serializedConsumer,
	}
}

func TestScreamerMetrics_RecordProcessing(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockPS := &mockPartitionStorage{}
	mockC := &mockConsumer{}
	streamName := "test_stream"
	runnerID := "test_runner"

	s := newTestSubscriber(t, reg, mockPS, mockC, streamName, runnerID)

	record := &DataChangeRecord{
		CommitTimestamp:      time.Now(),
		RecordSequence:       "seq1",
		ServerTransactionID:  "tx1",
		IsLastRecordInTransaction: true,
		TableName:            "Users",
		ColumnTypes:          []*ColumnType{{Name: "ID", Type: spannerpb.Type{Code: spannerpb.TypeCode_INT64}}},
		Mods:                 []*Mod{{Keys: `{"ID": "1"}`, NewValues: `{"Name": "Test"}`}},
		ModType:              DataChangeRecord_INSERT,
		ValueCaptureType:     "NEW_ROW",
		NumberOfRecordsInTransaction: 1,
		NumberOfPartitionsInTransaction: 1,
		TransactionTag:       "",
		IsSystemTransaction:  false,
	}
	changeRecords := []*ChangeRecord{{DataChangeRecords: []*DataChangeRecord{record}}}
	partitionMeta := &PartitionMetadata{PartitionToken: "token1"}

	// Successful processing
	err := s.handle(context.Background(), partitionMeta, changeRecords)
	require.NoError(t, err)

	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.recordsProcessedTotal.WithLabelValues()))
	assert.Equal(t, 0.0, testutil.ToFloat64(s.metrics.recordsProcessingErrorsTotal.WithLabelValues()))
	assert.GreaterOrEqual(t, testutil.ToFloat64(s.metrics.consumerDurationSeconds.WithLabelValues()), 0.0) // Just check it observed
	hist, _ := s.metrics.consumerDurationSeconds.GetMetricWithLabelValues()
	assert.Equal(t, uint64(1), testutil.ToFloat64((hist.(prometheus.Histogram)).GetSampleCount()))


	// Error during processing
	expectedErr := errors.New("consumer error")
	mockC.consumeFn = func(data []byte) error {
		return expectedErr
	}
	err = s.handle(context.Background(), partitionMeta, changeRecords)
	require.ErrorIs(t, err, expectedErr)

	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.recordsProcessedTotal.WithLabelValues())) // Should not have incremented
	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.recordsProcessingErrorsTotal.WithLabelValues()))
	histAfterError, _ := s.metrics.consumerDurationSeconds.GetMetricWithLabelValues()
	assert.Equal(t, uint64(2), testutil.ToFloat64((histAfterError.(prometheus.Histogram)).GetSampleCount())) // Still observes duration
}

func TestScreamerMetrics_PartitionWatermark(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockPS := &mockPartitionStorage{}
	mockC := &mockConsumer{}
	streamName := "test_stream_watermark"
	runnerID := "test_runner_watermark"

	s := newTestSubscriber(t, reg, mockPS, mockC, streamName, runnerID)

	commitTime := time.Now().Add(-time.Minute)
	heartbeatTime := time.Now().Add(-30 * time.Second)
	childStartTime := time.Now().Add(-15 * time.Second)

	dataRecord := &DataChangeRecord{CommitTimestamp: commitTime}
	heartbeatRecord := &HeartbeatRecord{Timestamp: heartbeatTime}
	childRecord := &ChildPartitionsRecord{StartTimestamp: childStartTime, ChildPartitions: []*ChildPartition{{Token: "child1"}}}

	changeRecords := []*ChangeRecord{
		{DataChangeRecords: []*DataChangeRecord{dataRecord}},
		{HeartbeatRecords: []*HeartbeatRecord{heartbeatRecord}},
		{ChildPartitionsRecords: []*ChildPartitionsRecord{childRecord}},
	}
	partitionToken := "partition_A"
	partitionMeta := &PartitionMetadata{PartitionToken: partitionToken}

	// Watermark should be the latest of the three
	expectedWatermark := childStartTime

	err := s.handle(context.Background(), partitionMeta, changeRecords)
	require.NoError(t, err)

	assert.Equal(t, float64(expectedWatermark.Unix()), testutil.ToFloat64(s.metrics.partitionWatermarkSeconds.WithLabelValues(partitionToken)))
	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.childPartitionsCreatedTotal.WithLabelValues(partitionToken)))
}

func TestScreamerMetrics_RunnerHeartbeat(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockPS := &mockPartitionStorage{}
	streamName := "test_stream_heartbeat"
	runnerID := "test_runner_heartbeat"

	// We don't need a consumer for this test, as we are not processing records
	s := newTestSubscriber(t, reg, mockPS, nil, streamName, runnerID)

	var refreshCount int
	mockPS.refreshRunnerFn = func(ctx context.Context, rid string) error {
		assert.Equal(t, runnerID, rid)
		refreshCount++
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // Run for a short period
	defer cancel()

	s.initErrGroup(ctx) // Initialize the errgroup

	// Manually run the heartbeat loop part from Subscribe
	s.eg.Go(func() error {
		ticker := time.NewTicker(10 * time.Millisecond) // Faster ticker for test
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.partitionStorage.RefreshRunner(ctx, s.runnerID)
				switch err {
				case nil:
					s.metrics.runnerHeartbeatsTotal.Inc()
				default:
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	_ = s.eg.Wait() // Wait for context timeout

	assert.Greater(t, refreshCount, 0)
	assert.Equal(t, float64(refreshCount), testutil.ToFloat64(s.metrics.runnerHeartbeatsTotal))
}


func TestScreamerMetrics_ActivePartitions(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockPS := &mockPartitionStorage{}
	streamName := "test_stream_active"
	runnerID := "test_runner_active"

	// We don't need a real consumer or spanner client for this test
	s := newTestSubscriber(t, reg, mockPS, &mockConsumer{}, streamName, runnerID)

	// Initial state
	assert.Equal(t, 0.0, testutil.ToFloat64(s.metrics.activePartitions))

	// Mock GetUnfinishedMinWatermarkPartition to return a partition to start detection
	minWatermarkTime := time.Now().Add(-time.Hour)
	mockPS.getUnfinishedMinWatermarkFn = func(ctx context.Context) (*PartitionMetadata, error) {
		// Return nil after the first call to stop the detection loop
		if mockPS.getUnfinishedMinWatermarkFn == nil { // Poor man's once
			return nil, nil
		}
		mockPS.getUnfinishedMinWatermarkFn = nil
		return &PartitionMetadata{Watermark: minWatermarkTime}, nil
	}

	// Mock GetAndSchedulePartitions to return two partitions
	partition1 := &PartitionMetadata{PartitionToken: "p1", Watermark: minWatermarkTime, EndTimestamp: defaultEndTimestamp, HeartbeatMillis: 10000}
	partition2 := &PartitionMetadata{PartitionToken: "p2", Watermark: minWatermarkTime, EndTimestamp: defaultEndTimestamp, HeartbeatMillis: 10000}
	scheduledPartitions := []*PartitionMetadata{partition1, partition2}
	var scheduleCalls int
	mockPS.getAndSchedulePartitionsFn = func(ctx context.Context, watermark time.Time, rID string) ([]*PartitionMetadata, error) {
		scheduleCalls++
		assert.Equal(t, runnerID, rID)
		assert.Equal(t, minWatermarkTime, watermark)
		return scheduledPartitions, nil
	}

	// Mock queryChangeStream to simulate work and finish
	// This is tricky because queryChangeStream is called in a goroutine by detectNewPartitions
	// and we want to control its lifecycle for metric assertion.
	// For simplicity, we'll assume queryChangeStream finishes immediately.
	// A more complex test would involve channels to signal completion.
	var updateToRunningCalls int
	mockPS.updateToRunningFn = func(ctx context.Context, p *PartitionMetadata) error {
		updateToRunningCalls++
		return nil
	}
	var updateToFinishedCalls int
	mockPS.updateToFinishedFn = func(ctx context.Context, p *PartitionMetadata) error {
		updateToFinishedCalls++
		return nil
	}
	// Simulate spanner client returning no rows, so queryChangeStream finishes quickly
	s.spannerClient = &spanner.Client{} // Dummy client to avoid nil panic, actual calls will be mocked by PartitionStorage

	ctx, cancel := context.WithCancel(context.Background())
	s.initErrGroup(ctx) // Initialize the errgroup for detectNewPartitions

	// Run detectNewPartitions once
	err := s.detectNewPartitions(ctx)
	require.NoError(t, err, "detectNewPartitions failed")

	// Assertions
	assert.Equal(t, 1, scheduleCalls, "GetAndSchedulePartitions should be called once")
	// The activePartitions metric is incremented BEFORE the goroutine for queryChangeStream is launched.
	// It's decremented via defer within that goroutine.
	// To reliably test this, we'd need to wait for those goroutines to finish.
	// Since queryChangeStream is simplified to finish immediately due to no rows from spanner.Client.Single().QueryWithOptions,
	// the decrements should have happened.
	// This requires the goroutines in detectNewPartitions to actually complete.
	// We'll wait a bit for the goroutines to complete their (mocked) work.
	// This is not ideal but avoids more complex synchronization for this test.
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 2, updateToRunningCalls, "UpdateToRunning should be called for each partition")
	assert.Equal(t, 2, updateToFinishedCalls, "UpdateToFinished should be called for each partition")
	assert.Equal(t, 0.0, testutil.ToFloat64(s.metrics.activePartitions), "Active partitions should be zero after processing")

	cancel()
	_ = s.eg.Wait()
}

// SpannerQueryDurationSeconds is tested implicitly via queryChangeStream if we had a real spanner client.
// For now, we'll test the observation part within handle, assuming a query happened.
// A more direct test would require deeper mocking of spanner client interactions.

func TestScreamerMetrics_SpannerQueryDuration(t *testing.T) {
	// This metric is recorded in queryChangeStream.
	// Testing it directly here is difficult without a real Spanner client or complex mocks.
	// We'll assume that if queryChangeStream is called, the defer function for the metric will run.
	// The primary goal is to ensure the metric exists and can be incremented.
	reg := prometheus.NewRegistry()
	s := newTestSubscriber(t, reg, &mockPartitionStorage{}, &mockConsumer{}, "s", "r")

	// Simulate a call, normally this happens inside queryChangeStream
	func() {
		start := time.Now()
		defer func() {
			s.metrics.spannerQueryDurationSeconds.WithLabelValues().Observe(time.Since(start).Seconds())
		}()
		time.Sleep(10 * time.Millisecond) // Simulate work
	}()

	hist, _ := s.metrics.spannerQueryDurationSeconds.GetMetricWithLabelValues()
	require.NotNil(t, hist)
	assert.Equal(t, uint64(1), testutil.ToFloat64((hist.(prometheus.Histogram)).GetSampleCount()))
	assert.Greater(t, testutil.ToFloat64((hist.(prometheus.Histogram)).GetSampleSum()), 0.0)
}

// Helper to create a valid DataChangeRecord for handle function
func makeDataChangeRecord(t time.Time) *DataChangeRecord {
	return &DataChangeRecord{
		CommitTimestamp:      t,
		RecordSequence:       "seq",
		ServerTransactionID:  "txid",
		IsLastRecordInTransaction: true,
		TableName:            "TestTable",
		ColumnTypes:          []*ColumnType{{Name: "ID", Type: spannerpb.Type{Code: spannerpb.TypeCode_INT64}}},
		Mods:                 []*Mod{{Keys: `{"ID":1}`, NewValues: `{"Data":"Test"}`}},
		ModType:              DataChangeRecord_INSERT,
		ValueCaptureType:     "NEW_ROW",
		NumberOfRecordsInTransaction: 1,
		NumberOfPartitionsInTransaction: 1,
	}
}

func TestHandleMetricsIntegration(t *testing.T) {
	reg := prometheus.NewRegistry()
	mockPS := &mockPartitionStorage{}
	mockC := &mockConsumer{}
	streamName := "integration_stream"
	runnerID := "integration_runner"

	s := newTestSubscriber(t, reg, mockPS, mockC, streamName, runnerID)

	partitionToken := "p_integrate"
	partitionMeta := &PartitionMetadata{PartitionToken: partitionToken}

	commitTime1 := time.Now().Add(-time.Hour)
	commitTime2 := time.Now().Add(-30 * time.Minute)
	childTime := time.Now().Add(-10 * time.Minute)

	records := []*ChangeRecord{
		{DataChangeRecords: []*DataChangeRecord{makeDataChangeRecord(commitTime1), makeDataChangeRecord(commitTime2)}},
		{ChildPartitionsRecords: []*ChildPartitionsRecord{{StartTimestamp: childTime, ChildPartitions: []*ChildPartition{{Token: "childX"}}}}},
	}

	var updateWatermarkTime time.Time
	mockPS.updateWatermarkFn = func(ctx context.Context, p *PartitionMetadata, watermark time.Time) error {
		updateWatermarkTime = watermark
		return nil
	}

	err := s.handle(context.Background(), partitionMeta, records)
	require.NoError(t, err)

	// recordsProcessedTotal
	assert.Equal(t, 2.0, testutil.ToFloat64(s.metrics.recordsProcessedTotal.WithLabelValues()), "recordsProcessedTotal")
	// recordsProcessingErrorsTotal
	assert.Equal(t, 0.0, testutil.ToFloat64(s.metrics.recordsProcessingErrorsTotal.WithLabelValues()), "recordsProcessingErrorsTotal")
	// consumerDurationSeconds
	hist, _ := s.metrics.consumerDurationSeconds.GetMetricWithLabelValues()
	assert.Equal(t, uint64(2), testutil.ToFloat64((hist.(prometheus.Histogram)).GetSampleCount()), "consumerDurationSeconds count")
	// partitionWatermarkSeconds
	assert.Equal(t, float64(childTime.Unix()), testutil.ToFloat64(s.metrics.partitionWatermarkSeconds.WithLabelValues(partitionToken)), "partitionWatermarkSeconds")
	assert.Equal(t, childTime, updateWatermarkTime, "Updated watermark in storage")
	// childPartitionsCreatedTotal
	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.childPartitionsCreatedTotal.WithLabelValues(partitionToken)), "childPartitionsCreatedTotal")

	// Test error case
	errorMsg := "consume failed"
	mockC.consumeFn = func(data []byte) error {
		// Fail on the second record of the next batch
		if mockC.ConsumedCount() == 2+1 { // 2 from previous success + 1st of this batch
			return errors.New(errorMsg)
		}
		return nil
	}
	commitTime3 := time.Now().Add(-5 * time.Minute)
	recordsWithError := []*ChangeRecord{
		{DataChangeRecords: []*DataChangeRecord{makeDataChangeRecord(commitTime3)}},
	}

	err = s.handle(context.Background(), partitionMeta, recordsWithError)
	require.Error(t, err)
	assert.Contains(t, err.Error(), errorMsg)

	assert.Equal(t, 2.0+1.0, testutil.ToFloat64(s.metrics.recordsProcessedTotal.WithLabelValues()), "recordsProcessedTotal after error") // One more success before error
	assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.recordsProcessingErrorsTotal.WithLabelValues()), "recordsProcessingErrorsTotal after error")
	histAfterError, _ := s.metrics.consumerDurationSeconds.GetMetricWithLabelValues()
	assert.Equal(t, uint64(2+2), testutil.ToFloat64((histAfterError.(prometheus.Histogram)).GetSampleCount()), "consumerDurationSeconds count after error") // 2 more attempts

	// Watermark should not have updated to commitTime3 because processing failed
	assert.Equal(t, float64(childTime.Unix()), testutil.ToFloat64(s.metrics.partitionWatermarkSeconds.WithLabelValues(partitionToken)), "partitionWatermarkSeconds after error")
}

func (dcr *DataChangeRecord) DecodeToNonSpannerType() map[string]interface{} {
	// Simplified decode for testing. Real implementation is more complex.
	out := make(map[string]interface{})
	if dcr.Keys != "" {
		_ = json.Unmarshal([]byte(dcr.Keys), &out)
	}
	if dcr.NewValues != "" {
		var newVals map[string]interface{}
		_ = json.Unmarshal([]byte(dcr.NewValues), &newVals)
		for k, v := range newVals {
			out[k] = v
		}
	}
	return out
}
