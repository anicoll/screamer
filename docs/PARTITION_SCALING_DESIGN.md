# Partition Scaling Design

## Problem Statement

### Issue
When a Spanner instance grows to a certain size, it produces more partitions than a single Screamer instance can handle efficiently (approximately 500+ partitions). The current implementation attempts to read all available partitions concurrently, causing:

1. Resource exhaustion (memory, CPU, goroutines)
2. Service instability and crash loops
3. Falling behind in event tracking
4. Potential SDK limitations being exceeded

### Root Cause Analysis

**Current Behavior (screamer.go:222-249)**:
```go
func (s *Subscriber) detectNewPartitions(ctx context.Context) error {
    // ...
    partitions, err := s.partitionStorage.GetAndSchedulePartitions(ctx, minWatermarkPartition.Watermark, s.runnerID)
    // ...
    for _, p := range partitions {
        s.eg.Go(func() error {
            return s.queryChangeStream(ctx, p)
        })
    }
}
```

**Issues Identified**:
1. `GetAndSchedulePartitions` retrieves ALL `StateCreated` partitions without limit
2. No concurrency control - spawns unlimited goroutines
3. Single runner tries to handle all partitions
4. No work distribution strategy across multiple runners

**Current Distribution Mechanism**:
- Distributed locking exists via `PartitionToRunner` and `Runner` tables
- Stale partition reassignment works (`GetInterruptedPartitions`)
- **BUT**: First runner to call `GetAndSchedulePartitions` claims ALL available partitions

## Solution Design

### Goals

1. **Limit concurrent partitions per runner** - Prevent resource exhaustion
2. **Enable horizontal scaling** - Distribute work across multiple runners
3. **Maintain backward compatibility** - No breaking changes to API
4. **Fair work distribution** - Balance load across runners
5. **Handle dynamic partitions** - Support partition splits/merges
6. **Graceful degradation** - Work well with 1 or N runners

### Proposed Architecture

#### 1. Concurrency Limiting (Per-Runner)

**Add configuration option**:
```go
// config.go
type config struct {
    // ... existing fields
    maxConcurrentPartitions int
}

// options.go
type withMaxConcurrentPartitions int

func (o withMaxConcurrentPartitions) Apply(c *config) {
    c.maxConcurrentPartitions = int(o)
}

func WithMaxConcurrentPartitions(max int) Option {
    return withMaxConcurrentPartitions(max)
}
```

**Default values**:
- Default: 100 concurrent partitions per runner
- Configurable via option
- 0 = unlimited (current behavior, for backward compatibility)

#### 2. Partition Work Distribution Strategy

**Modify `GetAndSchedulePartitions` to support batching**:

```go
// PartitionStorage interface addition
GetAndSchedulePartitions(ctx context.Context, minWatermark time.Time, runnerID string, limit int) ([]*PartitionMetadata, error)
```

**Implementation in spanner.go (line 247)**:
```sql
SELECT * FROM {tableName}
WHERE State = @state
  AND StartTimestamp >= @minWatermark
ORDER BY StartTimestamp ASC
LIMIT @limit  -- Add limit parameter
FOR UPDATE
```

**Changes to detectNewPartitions logic**:

```go
func (s *Subscriber) detectNewPartitions(ctx context.Context) error {
    // Calculate available capacity
    availableSlots := s.getAvailablePartitionSlots()
    if availableSlots <= 0 {
        return nil // At capacity, skip this round
    }

    // Fetch only what we can handle
    partitions, err := s.partitionStorage.GetAndSchedulePartitions(
        ctx,
        minWatermarkPartition.Watermark,
        s.runnerID,
        availableSlots,
    )
    // ... rest of logic
}

func (s *Subscriber) getAvailablePartitionSlots() int {
    if s.maxConcurrentPartitions == 0 {
        return 100 // Default batch size
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    currentlyRunning := s.getRunningPartitionCount()
    available := s.maxConcurrentPartitions - currentlyRunning

    if available <= 0 {
        return 0
    }
    return available
}
```

#### 3. Partition Tracking

**Add partition tracking to Subscriber**:

```go
type Subscriber struct {
    // ... existing fields
    activePartitions   map[string]struct{} // Track running partitions
    partitionCountMu   sync.RWMutex
}

func (s *Subscriber) markPartitionActive(token string) {
    s.partitionCountMu.Lock()
    defer s.partitionCountMu.Unlock()
    s.activePartitions[token] = struct{}{}
}

func (s *Subscriber) markPartitionInactive(token string) {
    s.partitionCountMu.Lock()
    defer s.partitionCountMu.Unlock()
    delete(s.activePartitions, token)
}

func (s *Subscriber) getRunningPartitionCount() int {
    s.partitionCountMu.RLock()
    defer s.partitionCountMu.RUnlock()
    return len(s.activePartitions)
}
```

**Update queryChangeStream to track lifecycle**:

```go
func (s *Subscriber) queryChangeStream(ctx context.Context, p *PartitionMetadata) error {
    s.markPartitionActive(p.PartitionToken)
    defer s.markPartitionInactive(p.PartitionToken)

    // ... existing logic
}
```

#### 4. Fair Distribution Across Runners

**Problem**: First runner grabs all partitions

**Solution**: Randomized selection or round-robin via SQL

**Option A: Randomized Fetch (Simple)**
```sql
-- Add random ordering to prevent thundering herd
SELECT * FROM {tableName}
WHERE State = @state
  AND StartTimestamp >= @minWatermark
ORDER BY FARM_FINGERPRINT(CAST(CURRENT_TIMESTAMP() AS STRING) || PartitionToken), StartTimestamp ASC
LIMIT @limit
FOR UPDATE
```

**Option B: Preferred - Fair Distribution via Modulo**
```sql
-- Distribute based on hash of partition token
WITH RankedPartitions AS (
  SELECT *,
    MOD(FARM_FINGERPRINT(PartitionToken), @totalRunners) as runner_hash
  FROM {tableName}
  WHERE State = @state
    AND StartTimestamp >= @minWatermark
)
SELECT * EXCEPT(runner_hash)
FROM RankedPartitions
WHERE runner_hash = MOD(FARM_FINGERPRINT(@runnerID), @totalRunners)
ORDER BY StartTimestamp ASC
LIMIT @limit
FOR UPDATE
```

**Trade-off**: Option A is simpler, Option B provides better distribution but requires active runner count.

**Recommended**: Start with Option A for simplicity.

#### 5. Dynamic Runner Count

**Enhance Runner table to track active runners**:

```go
// New method in PartitionStorage interface
GetActiveRunnerCount(ctx context.Context) (int, error)
```

**Implementation**:
```sql
SELECT COUNT(*) FROM Runner
WHERE UpdatedAt > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND)
```

Used for metrics and optional fair distribution (Option B above).

### Implementation Plan

#### Phase 1: Concurrency Limiting (Critical - Fixes immediate issue)

1. Add `maxConcurrentPartitions` configuration option
2. Add partition tracking (`activePartitions` map)
3. Modify `detectNewPartitions` to calculate available slots
4. Update `queryChangeStream` to track partition lifecycle
5. Add limit parameter to `GetAndSchedulePartitions` interface and implementation
6. **Testing**: Verify single runner respects concurrency limit

**Files to modify**:
- `config.go` - Add config field
- `options.go` - Add option constructor
- `screamer.go` - Add tracking, modify detectNewPartitions, queryChangeStream
- `partitionstorage/spanner.go` - Add limit to SQL query
- `screamer_test.go` - Add tests for concurrency limiting

#### Phase 2: Fair Distribution (Important - Enables scaling)

1. Implement randomized partition selection (Option A)
2. Add `GetActiveRunnerCount` method
3. Add metrics/logging for partition distribution
4. **Testing**: Verify multiple runners share work fairly

**Files to modify**:
- `partitionstorage/spanner.go` - Update query with randomization
- `screamer.go` - Add metrics/logging
- `screamer_test.go` - Add multi-runner tests

#### Phase 3: Observability & Tuning (Nice to have)

1. Add metrics for:
   - Active partition count per runner
   - Partition processing rate
   - Runner health/liveness
   - Queue depth (available partitions)
2. Add health check endpoint
3. Add partition distribution histograms
4. Tune default values based on production data

**Files to modify**:
- New: `metrics.go` - Prometheus/OpenTelemetry metrics
- `screamer.go` - Instrument with metrics

### Configuration Examples

**Single runner (current behavior)**:
```go
subscriber := screamer.NewSubscriber(
    client,
    streamName,
    runnerID,
    partitionStorage,
    screamer.WithMaxConcurrentPartitions(0), // Unlimited
)
```

**Single runner with limit**:
```go
subscriber := screamer.NewSubscriber(
    client,
    streamName,
    runnerID,
    partitionStorage,
    screamer.WithMaxConcurrentPartitions(100), // Limit to 100
)
```

**Multiple runners (horizontal scaling)**:
```go
// Runner 1
subscriber1 := screamer.NewSubscriber(
    client,
    "stream-1",
    "runner-1",
    partitionStorage,
    screamer.WithMaxConcurrentPartitions(100),
)

// Runner 2
subscriber2 := screamer.NewSubscriber(
    client,
    "stream-1",
    "runner-2",
    partitionStorage,
    screamer.WithMaxConcurrentPartitions(100),
)
```

With 500 partitions and 5 runners at 100 partitions each = full coverage

### Backward Compatibility

1. Default `maxConcurrentPartitions = 0` maintains unlimited behavior
2. No changes to existing API signatures (only additions)
3. Existing partition storage implementations continue to work
4. Single runner deployments unaffected

### Testing Strategy

#### Unit Tests

1. **Concurrency limiting**:
   - Verify partition count never exceeds limit
   - Test partition tracking (add/remove)
   - Test available slot calculation

2. **Distribution**:
   - Verify GetAndSchedulePartitions respects limit parameter
   - Test randomization in partition selection

#### Integration Tests

1. **Single runner with limit**:
   - Create 200 partitions, limit to 50
   - Verify only 50 processed concurrently
   - Verify all eventually processed

2. **Multi-runner**:
   - 2 runners, 100 partitions
   - Verify work distribution
   - Verify no duplicate processing

3. **Failover**:
   - Kill runner mid-processing
   - Verify stale partitions reassigned
   - Verify limit respected after reassignment

4. **Dynamic partitions**:
   - Test partition splits during processing
   - Verify new children partitions scheduled fairly

### Performance Considerations

#### Memory Impact

- Partition tracking map: `O(n)` where n = concurrent partitions
- With 100 partitions: ~100 * (16 bytes pointer + 32 bytes string) ≈ 5KB
- Negligible impact

#### Database Impact

- Randomized query adds `FARM_FINGERPRINT` computation
- Minimal CPU impact, same query plan
- No additional round trips

#### Throughput Impact

- Positive: Prevents runner overload
- Positive: Better resource utilization across runners
- Neutral: Fair distribution adds minor query overhead
- Net: **Improved** overall throughput at scale

### Migration Path

#### For existing deployments with <100 partitions
- No action required
- Optional: Set explicit limit for safety

#### For existing deployments with >100 partitions (hitting issue)
1. Set `WithMaxConcurrentPartitions(100)` on existing runner
2. Monitor partition processing rate
3. If backlog grows, add additional runners
4. Each runner automatically shares work

#### Recommended defaults by scale

| Partition Count | Recommended Setup |
|----------------|------------------|
| < 50 | 1 runner, no limit (default) |
| 50-100 | 1 runner, limit=100 |
| 100-300 | 2-3 runners, limit=100 each |
| 300-500 | 3-5 runners, limit=100 each |
| 500+ | 5-10 runners, limit=100 each |

### Monitoring & Alerts

**Key metrics to track**:
1. `screamer_active_partitions` (gauge) - per runner
2. `screamer_partition_capacity_used` (gauge) - percentage
3. `screamer_partition_processing_rate` (counter) - partitions/sec
4. `screamer_available_partitions` (gauge) - in CREATED state
5. `screamer_active_runners` (gauge) - runner count

**Recommended alerts**:
1. Active runners < 2 (for HA deployments)
2. Available partitions > 100 for >5 minutes (backlog)
3. Capacity used > 90% for >5 minutes (scale up)
4. Partition processing rate = 0 for >1 minute (stuck)

### Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Uneven distribution | Some runners idle while others overloaded | Use randomized selection (Phase 2) |
| Default limit too low | Underutilization | Set default to 100 (based on issue report) |
| Default limit too high | Still crashes | Users can tune via option |
| Breaking changes | Breaks existing deployments | Maintain backward compatibility |
| Race conditions in tracking | Inaccurate count | Use proper mutex locking |

### Future Enhancements

1. **Adaptive concurrency**: Automatically adjust limit based on resource usage
2. **Priority partitions**: Process certain partitions first
3. **Partition affinity**: Sticky assignment of partitions to runners
4. **Backpressure**: Slow down if consumer can't keep up
5. **Graceful shutdown**: Drain partitions before exit

### Open Questions

1. **Default limit value**: 100 seems reasonable based on issue (~500 crashes). Should this be configurable via environment variable?
   - Recommendation: Start with 100, make configurable

2. **Batch size for GetAndSchedulePartitions**: Should we fetch exactly `availableSlots` or fetch more for lookahead?
   - Recommendation: Fetch exactly `availableSlots` for simplicity

3. **Metrics library**: Prometheus? OpenTelemetry? Both? None (just logging)?
   - Recommendation: Start with structured logging (Phase 1), add metrics (Phase 3)

4. **Health checks**: Should subscriber expose health check method?
   - Recommendation: Yes, add in Phase 3

## Summary

This design solves the partition scaling issue through:

1. **Concurrency limiting** - Prevents single runner from overloading
2. **Fair distribution** - Enables horizontal scaling across runners
3. **Backward compatibility** - No breaking changes
4. **Incremental rollout** - Can be adopted gradually

**Immediate fix**: Phase 1 can be implemented quickly to stop crash loops by limiting concurrent partitions per runner.

**Long-term scaling**: Phase 2 enables true horizontal scaling by distributing work fairly across multiple runners.

**Expected outcome**: A deployment with 500 partitions can run 5 runners at 100 partitions each instead of 1 runner trying to handle all 500.
