# Soak testing on real Spanner

The emulator only ever has one partition, which never splits. It also sends no heartbeats while a stream is idle. The simulator (`pkg/simulator`) models splits, merges and idle partitions, but only real Spanner shows:

- how partitions actually split and merge under load;
- real latencies, which set how leases, renewals and parking behave;
- which query plans the metadata queries get, and what they cost.

`cmd/soak` runs Screamer against a real database for hours or days and checks the one thing that matters: every write is delivered, in order per key.

## How it works

| Command | What it does |
| --- | --- |
| `soak setup` | Creates the `SoakEvents` table, the `SoakStream` change stream on it, and the `SoakDeliveries` table, where any is missing. |
| `soak write` | Updates many keys. Each write sets a key's `Seq` to one more than before, so every key's writes are numbered 1, 2, 3... |
| `soak run` | One runner. It reads the stream with Screamer and writes every delivered write to `SoakDeliveries` before it acknowledges it. A runner killed mid-write leaves nothing acknowledged that was not recorded. |
| `soak check` | Compares the deliveries with the writes, and reports missing, duplicate and out-of-order deliveries, and delivery lag. It exits 1 if anything is missing or out of order. |
| `soak drill` | Runs several `soak run` processes. Every so often it restarts one (SIGTERM), kills one (SIGKILL), freezes one (SIGSTOP for longer than the lease TTL), or adds or removes one. |

**The writes to expect.** The events table holds each key's latest `Seq`, so `check` reads it at a past time (`--settle`, 2 minutes by default) and expects writes 1 to `Seq` for each key.

**Ordering.** Deliveries are ordered by the commit timestamp of the row that recorded them, and Spanner keeps those consistent across hosts. A write delivered by one runner after a later write to the same key was delivered by another runner is caught, without trusting anyone's clock.

**The write pattern.** The writer has hot keys, one contiguous key range that is written all the time, and cold keys, which are written only in bursts (by default 2 minutes in every 10). Between bursts, partitions holding only cold keys are idle and park. In a burst they all wake at once, and the load can make Spanner split them.

## What you need

- A Spanner instance. The smallest, 100 processing units, is enough to start with. If partitions don't split, raise `--keys`, `--payload-bytes` and the rates before you add capacity.
- Credentials the Go client can find, such as `gcloud auth application-default login` or a service account. `setup` needs `roles/spanner.databaseAdmin`. The other commands need `roles/spanner.databaseUser`, and the runners also need it for their first `RunMigrations`.
- Optionally, a second database for `--deliveries-dsn`. Recording deliveries roughly doubles the write load, and a separate database keeps that load off the instance under test.

```sh
gcloud spanner instances create screamer-soak --config=regional-us-central1 \
  --processing-units=100 --description="Screamer soak"
gcloud spanner databases create soak --instance=screamer-soak

export SOAK_DSN=projects/$PROJECT/instances/screamer-soak/databases/soak
go build -o soak ./cmd/soak   # or: make build, which writes .bin/soak
./soak setup
```

Each command reads `--dsn` from `SOAK_DSN`, and also takes `SOAK_METADATA_DSN` and `SOAK_DELIVERIES_DSN`.

## Running a soak

Start the runners before the writer. If you don't, pass `--start` with a time before the writer's first write. Otherwise the writes from before the runners started are reported missing.

```sh
# Terminal 1: two to four runners, one drill action every 5 minutes.
./soak drill --min-runners 2 --max-runners 4 --every 5m -- --max-concurrent-partitions 100

# Terminal 2: the load.
./soak write --keys 20000 --hot-rate 200 --burst-rate 1000

# Terminal 3: check every 10 minutes.
./soak check --watch 10m
```

The writer and runners log JSON lines to stderr. Each runner logs `soak runner stats` every 30 seconds. That line has its deliveries, its delivery lag, Screamer's partition metrics, partitions by state, live and desired runners, demand, and watermark lag.

To stop, press Ctrl-C. The drill stops every runner cleanly, and the writer can be started again later: it carries on from the sequence numbers in the table.

### Scenarios

Run these roughly in order. Each builds on the one before.

1. **Baseline**: one `soak run`, no partition limit, `--burst-rate 0`, for an hour. This checks the setup works and gives a delivery lag to compare against.
2. **Splits and parking**: the default write pattern, one runner, for a few hours. Watch `partitions_by_state`. Partitions should split during bursts and park between them. `check` should stay clean.
3. **Over the limit**: set `--max-concurrent-partitions` well below the number of unfinished partitions. Parking should keep every partition read. Run the same load again with `--park=false` to see the difference.
4. **Drills**: `soak drill` for 24 hours or more, with a lease TTL of 30s (the default) and `--freeze-for 45s`.
5. **Kubernetes**: run `soak run` as a Deployment with the KEDA setup from the README. Then try `kubectl rollout restart`, `kubectl delete pod`, deleting a node, and scaling by hand.

### What passing looks like

- `check` reports **0 missing and 0 out of order**, every time. Anything else is a bug, and the report lists sample keys and sequence numbers to start from.
- Duplicates come only from restarts, kills, freezes and handovers. Each such event redelivers at most about one checkpoint interval of that partition's writes.
- On a steady load, delivery lag p99 stays within the park interval plus a few seconds. During a burst it can rise, but it should come back down.
- `watermark_lag` doesn't keep growing. If it does, the runners are not keeping up. Check whether `desired_runners` asked for more.
- Leases are lost (`partition_metrics.LeasesLost`) only after freezes.

## Checking the metadata queries

The queries Screamer runs against its metadata table should read only unfinished partitions: claiming, counting and gating should all use the `<table>_State_idx` index. Finished partitions are kept for a day, so on a busy stream a scan that reads them costs far more. Query statistics show what each query actually read:

```sql
SELECT text, execution_count, avg_latency_seconds, avg_rows_scanned
FROM SPANNER_SYS.QUERY_STATS_TOP_HOUR
WHERE text LIKE '%SoakMetadata%'
ORDER BY avg_rows_scanned DESC;
```

`avg_rows_scanned` should stay close to the number of unfinished partitions. If a query scans the whole table, open its plan in the console's Query insights. Then add or change an index in `pkg/partitionstorage/migration.go`.

## Costs and clean-up

The instance costs money for as long as it exists, whether or not anything is running. Delete it when you're done:

```sh
gcloud spanner instances delete screamer-soak
```

`SoakDeliveries` grows by one row per delivery. `check` reads all of it, so each check takes a little longer than the last. To start over, drop and recreate the tables, or use new table names (`--events-table`, `--stream`, `--deliveries-table`, `--metadata-table`).
