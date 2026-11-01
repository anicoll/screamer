# Performance notes

This covers where Screamer spends CPU and memory, what was changed, and what is still worth measuring on a real Spanner instance. The numbers come from Go benchmarks and from CPU and heap profiles of simulator runs (`pkg/simulator`) on a 4-core machine. The simulator replaces Spanner and gRPC, so it shows Screamer's own costs, not the Spanner client's network or stream buffers.

To reproduce:

```sh
go test -run XXX -bench Record -benchmem .                         # per-record path
go test -run TestBaseline_FiveThousandPartitions \
  -cpuprofile cpu.out -memprofile mem.out ./pkg/simulator          # 5,000 partitions
go tool pprof -top -cum pkg/simulator.test cpu.out
```

## 1. The per-record path (fixed)

Every data change record used to be decoded twice and encoded once. First, the Spanner client decoded the `ChangeRecord` column with reflection and unmarshalled every JSON value into `map[string]interface{}`: each mod's `keys`, `new_values` and `old_values`, plus each column's type. Then Screamer copied that into `DataChangeRecord` and `json.Marshal`ed it again for the consumer. That cost about 52µs and 24KB of garbage per record, which caps a core at around 20,000 records a second before the consumer does any work.

Now:

- **`decode.go`** walks the protobuf values of the column directly, using the field names in the row's type metadata. JSON values stay as the text Spanner sent. If a row doesn't have the expected shape, it falls back to the client's `row.Columns`.
- **`encode.go`** writes the consumer's JSON with `append` calls into a single buffer, copies the mods' JSON text verbatim, and caches the handful of distinct column-type strings.

10-column UPDATE, one mod (`BenchmarkRecord*`):

| | time/record | allocated/record | allocations/record |
| --- | ---: | ---: | ---: |
| Before: `row.Columns` + `json.Marshal` | 49–54 µs | 24.0 KB | 320 |
| After | 4.3–4.6 µs | 3.3 KB | 22 |
| Decode only: client vs fast | 37 µs → 2.1 µs | 20.1 KB → 1.5 KB | 276 → 21 |
| Encode only: `encoding/json` vs fast | 11.6 µs → 1.9 µs | 3.9 KB → 1.8 KB | 44 → 1 |

That is about 11x less CPU and 7x less garbage per record. The remaining allocation is mostly the output buffer, which the consumer owns.

**Output compatibility.**
- The record envelope is byte-for-byte what `encoding/json` produced. `TestEncodeDataChangeRecord_ByteIdenticalForNullJSON` checks this, and `FuzzAppendString` and `FuzzAppendTime` check string escaping and timestamps against `encoding/json`.
- Inside `keys`, `new_values` and `old_values`, object keys now keep Spanner's order, and strings keep Spanner's escaping, instead of being sorted and HTML-escaped. The JSON values are identical (`TestDecodeChangeRecordColumn_MatchesClientDecoder`).
- Empty and NULL mods are still left out.

## 2. Scheduling burst (fixed)

With no `WithMaxConcurrentPartitions`, detection scheduled at most 100 partitions per one-second tick. A runner starting with 5,000 partitions, or a burst of splits, took 50 seconds to open them all. Until then, the last partitions were not read at all. Detection now keeps scheduling in batches of 100 until nothing is ready, up to 100 transactions per tick. `TestScheduling_UnlimitedOpensEveryPartitionAtOnce` opens all 2,000 partitions within 3 seconds; the old code had opened 100 by then.

## 3. Smaller fixes

- **In-memory storage:** `GetUnfinishedMinWatermarkPartition` copied and sorted every partition each second, which was O(n log n) and accounted for 5% of all allocations in the 5,000-partition run. It is now a single pass with no allocation.
- **Checkpoints:** each flush allocated a fresh map sized for every partition. It now reuses the previous map, which more than halves its allocations.

## 4. Memory per partition

At 5,000 partitions on one runner, Screamer itself holds about **1.6 KB of heap and 9 KB of goroutine stack per partition**, roughly 55 MB in total. The stack is the goroutine that holds each partition's query open for its whole life.

The simulator does not include the Spanner client's per-stream state (gRPC flow-control windows, `RowIterator` buffers), which is likely larger and is worth measuring on a real instance. Parking idle partitions (on by default, see the README) removes both costs for partitions with no traffic, which in most deployments is most of them: a parked partition has no goroutine and no query. In the simulator, 200 mostly idle partitions settle at under 50 open queries.

## 5. Metadata table load

These are the queries each runner sends to the metadata table:

| Query | Every | Cost | Note |
| --- | --- | --- | --- |
| `GetUnfinishedMinWatermarkPartition` | 1 s | Index range on `Watermark`, `LIMIT 1` | Cheap. |
| `GetAndScheduleReadyPartitions` | 1 s | Read-write transaction. Scans every row with `State = CREATED`, with a correlated parent lookup per row. | There is no index on `State`, so it scans the whole table, including up to a day of FINISHED rows kept by the row deletion policy. Cost grows with runners × table size. |
| `GetInterruptedPartitions` | 10 s | Read-write transaction joining `Runner`, `PartitionToRunner` and the metadata table. | Runs even when no runner is stale. |
| `CountUnfinishedPartitions` | 15 s | Full scan. | Metrics only. |
| `RefreshRunner` | 2 s | One-row write. | |
| `UpdateWatermarks` | 1 s | Read-write transaction, up to 500 DML statements. | Was one write per batch per partition before batched checkpoints. |

Recommendations, in order:

1. **Index `State`.** Done: `RunMigrations` creates `<table>_State_idx ON <table>(State, Watermark)`, and every query that skipped finished rows with `State != FINISHED` now asks for `State IN UNNEST(@unfinished)` so it can use the index. Scheduling, claiming, counting and the missing-parent check read only unfinished rows. The query plans still need checking on a real instance: the emulator does not show them.
2. **Don't scan for CREATED rows when nothing new was added.** The runner that adds child partitions knows they exist. A cheap "any CREATED rows?" check, or scheduling right after `AddChildPartitions`, would save a read-write transaction per runner per second when nothing has split.
3. **Lease-based ownership.** Done: with leases (the default), `GetInterruptedPartitions` and its three-table join no longer run. Claiming an expired lease is part of the one claim transaction a second, and lease renewal rides on the checkpoint write.

## 6. In-memory storage at long uptimes

`InmemoryPartitionStorage` used to keep FINISHED partitions forever, so memory and the per-second scans grew with every split for the life of the process. It now removes them an hour after they finish (`WithFinishedRetention`), as the Spanner storage does after a day. That is safe with parent gating on: a missing parent counts as finished once nothing is behind the child.
