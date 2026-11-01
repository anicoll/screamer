# Migration guide

## Faster record decoding and encoding

Change records are now decoded without reflection, and the JSON delivered to consumers is written directly. That's about 11x less CPU and 7x less garbage per record (see [docs/PERFORMANCE.md](docs/PERFORMANCE.md)). Nothing to change in your code.

One visible difference: inside `mods[].keys`, `mods[].new_values` and `mods[].old_values`, object keys now keep the order Spanner sent them in, and strings keep Spanner's escaping. Before, `encoding/json` sorted keys alphabetically and escaped `<`, `>` and `&` as `\u003c` and friends. The JSON values are the same, so a consumer that parses the JSON is unaffected. One that compares the raw bytes of a mod with an earlier delivery will see a difference. Everything outside the mods is byte-for-byte the same as before.

## Parent gating and batched checkpoints

### What changed

- **Parent gating is on by default.** A child partition is read only once every one of its parents has finished, so changes to each key stay in commit order across splits and merges. Before, a child could start while another parent of a merge was still behind, and the old `StartTimestamp >= min watermark` check rarely held one back.
- **Watermarks are written in batches**, once a second by default, instead of after every batch of records.

### Do I need to do anything?

**If you use `partitionstorage.NewSpanner` or `partitionstorage.NewInmemory`, no.** Both implement the new interfaces. No schema change is needed.

**If you have your own `PartitionStorage`**, `Subscribe` now returns this error at start:

```
screamer: parent gating is on but *yourpkg.Storage does not implement screamer.ParentGatedScheduler; ...
```

Choose one:

1. Implement `screamer.ParentGatedScheduler`:

   ```go
   // GetAndScheduleReadyPartitions finds CREATED partitions whose parents have all
   // FINISHED, marks up to limit of them SCHEDULED for runnerID and returns them.
   GetAndScheduleReadyPartitions(ctx context.Context, runnerID string, limit int) ([]*screamer.PartitionMetadata, error)
   ```

   A parent with no row may not have been discovered yet (the other parent of a merge can report the child first), so count a missing parent as finished only once no unfinished partition has a watermark before the child's `StartTimestamp`. See `pkg/partitionstorage/spanner.go` for the SQL and `inmemory.go` for a plain Go version.

2. Or keep the old behaviour with `screamer.WithParentGating(false)`. Changes to the same key can then be delivered out of order around merges.

Optionally, implement `screamer.WatermarkBatchUpdater` to write each flush in one call:

```go
// UpdateWatermarks persists the given watermarks. It must never move a watermark
// backwards, and must ignore partitions it does not hold.
UpdateWatermarks(ctx context.Context, watermarks []screamer.PartitionWatermark) error
```

Without it, each flush calls your `UpdateWatermark` once per partition that moved, which is still at most one write per partition per second.

### Behaviour to be aware of

- After a crash or a takeover, a partition resumes from its last written watermark, so up to one checkpoint interval (1s by default) of changes can be delivered again. Use `WithCheckpointInterval(0)` to write after every batch, as before.
- With parent gating, a merged child starts only after its slowest parent finishes. While a runner catches up on a backlog, children of merges wait for that parent rather than racing ahead.
