# Migration guide

## Parking idle partitions

### What changed

A partition that delivers nothing but heartbeats for a minute is now parked: its query is closed, and it is read every 10 seconds in a short catch-up query until it has changes again (see [Parking idle partitions](README.md#parking-idle-partitions)). A change to a partition that had been idle can therefore arrive up to 10 seconds later than before. `WithParking(false)` or `--park=false` keeps every partition streaming. Partitions also have a new state, `PARKED`.

### Do I need to do anything?

**If you use `partitionstorage.NewSpanner`**, the metadata table needs one more nullable column. `RunMigrations` adds it; otherwise apply:

```sql
ALTER TABLE <metadata table> ADD COLUMN NextReadAt TIMESTAMP;
```

**If you use `partitionstorage.NewInmemory`**, nothing to do.

**If you have your own `PartitionStorage`**, implement `screamer.PartitionParker` (`ParkPartition`), and have `ClaimPartitions` also claim `PARKED` partitions whose `NextReadAt` has passed when `ClaimRequest.Parked` is set. Treat `PARKED` as unfinished everywhere you filter on state. Or turn parking off with `screamer.WithParking(false)`.

If anything reads the metadata table directly, for example a dashboard counting partitions by state, add `PARKED` to it.

## Partition leases

### What changed

Runners now own partitions through leases with fencing (see [Partition leases](README.md#partition-leases) in the README):

- A runner that dies is taken over after its lease expires, **30 seconds** by default, not 3 seconds after its last `Runner` refresh. A runner that stops cleanly hands its partitions over within about a second. Tune with `WithLeaseTTL(d)` or `--lease-ttl`.
- A runner that comes back from a pause longer than the TTL cannot write over the new owner's watermarks. It stops reading those partitions.
- The checkpoint interval is capped at a third of the lease TTL, so leases are always renewed in time.
- `PartitionToRunner` is no longer written. You can drop it once every runner runs this release.
- `InmemoryPartitionStorage` now removes finished partitions an hour after they finish, so long-running processes don't grow without bound. `NewInmemory(partitionstorage.WithFinishedRetention(0))` keeps them all, as before.

### Do I need to do anything?

**If you use `partitionstorage.NewSpanner`**, the metadata table needs three new nullable columns and an index. `RunMigrations` adds them (the CLI calls it at start). If you manage the schema yourself, apply:

```sql
ALTER TABLE <metadata table> ADD COLUMN Owner STRING(MAX);
ALTER TABLE <metadata table> ADD COLUMN Epoch INT64;
ALTER TABLE <metadata table> ADD COLUMN LeaseExpiresAt TIMESTAMP;
ALTER TABLE <metadata table> ADD COLUMN NextReadAt TIMESTAMP;
CREATE INDEX <metadata table>_State_idx ON <metadata table>(State, Watermark)
  STORING (ParentTokens, StartTimestamp, Owner, Epoch, LeaseExpiresAt);
```

Until the columns exist, `Subscribe` fails with an error naming the missing column and pointing here.

**If you use `partitionstorage.NewInmemory`**, nothing to do.

**If you have your own `PartitionStorage`**, `Subscribe` now returns:

```
screamer: leases are on but *yourpkg.Storage does not implement screamer.PartitionLeaser; ...
```

Implement `screamer.PartitionLeaser` (`ClaimPartitions`, `RenewLeases`, `ReleaseLeases`; the interface documents what each must guarantee, and `pkg/partitionstorage/inmemory.go` is a plain Go version), or keep the old behaviour with `screamer.WithLeases(false)`.

### Rolling out

Runners from before this release don't use leases, and runners from this release treat a partition with no lease as free to claim. While both run at once, the same partition can be read by an old and a new runner, so some changes are delivered twice. To avoid that, stop every old runner before starting new ones. Either way nothing is lost, and the duplicates stop once the old runners are gone.

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
