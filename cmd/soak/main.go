// Command soak runs Screamer against a real Spanner database and checks that
// every write is delivered, in order per key. See docs/SOAK.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/internal/soak"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app().Run(ctx, os.Args); err != nil {
		var exit cli.ExitCoder
		if errors.As(err, &exit) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(exit.ExitCode())
		}
		log.Fatal().Err(err).Msg("soak failed")
	}
}

func app() *cli.Command {
	return &cli.Command{
		Name:  "soak",
		Usage: "soak test Screamer against a real Spanner database",
		Commands: []*cli.Command{
			setupCommand(),
			writeCommand(),
			runCommand(),
			checkCommand(),
			drillCommand(),
		},
	}
}

// Flags every subcommand takes.
func dbFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "dsn", Sources: cli.EnvVars("SOAK_DSN"), Required: true,
			Usage: "database with the events table and change stream: projects/P/instances/I/databases/D"},
		&cli.StringFlag{Name: "metadata-dsn", Sources: cli.EnvVars("SOAK_METADATA_DSN"),
			Usage: "database for Screamer's partition metadata (default: --dsn)"},
		&cli.StringFlag{Name: "deliveries-dsn", Sources: cli.EnvVars("SOAK_DELIVERIES_DSN"),
			Usage: "database for the deliveries table (default: --dsn)"},
		&cli.StringFlag{Name: "events-table", Value: soak.DefaultTables.Events},
		&cli.StringFlag{Name: "stream", Value: soak.DefaultTables.Stream},
		&cli.StringFlag{Name: "deliveries-table", Value: soak.DefaultTables.Deliveries},
		&cli.StringFlag{Name: "metadata-table", Value: soak.DefaultTables.Metadata},
	}
}

type dsns struct{ stream, metadata, deliveries string }

func dbConfig(c *cli.Command) (dsns, soak.Tables) {
	d := dsns{stream: c.String("dsn"), metadata: c.String("metadata-dsn"), deliveries: c.String("deliveries-dsn")}
	if d.metadata == "" {
		d.metadata = d.stream
	}
	if d.deliveries == "" {
		d.deliveries = d.stream
	}
	return d, soak.Tables{
		Events:     c.String("events-table"),
		Stream:     c.String("stream"),
		Deliveries: c.String("deliveries-table"),
		Metadata:   c.String("metadata-table"),
	}
}

func setupCommand() *cli.Command {
	return &cli.Command{
		Name:  "setup",
		Usage: "create the events table, change stream and deliveries table, where missing",
		Flags: append(dbFlags(),
			&cli.StringFlag{Name: "retention", Value: "1d", Usage: "change stream retention period"},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			d, t := dbConfig(c)
			if err := soak.Setup(ctx, d.stream, d.deliveries, t, c.String("retention")); err != nil {
				return err
			}
			log.Info().Str("events", t.Events).Str("stream", t.Stream).Str("deliveries", t.Deliveries).Msg("soak schema ready")
			return nil
		},
	}
}

func writeCommand() *cli.Command {
	def := soak.DefaultWriterConfig
	return &cli.Command{
		Name:  "write",
		Usage: "write a steady load to hot keys, and bursts to the rest",
		Flags: append(dbFlags(),
			&cli.IntFlag{Name: "keys", Value: def.Keys, Usage: "how many rows to update"},
			&cli.FloatFlag{Name: "hot-fraction", Value: def.HotFraction, Usage: "share of keys, one contiguous key range, written all the time"},
			&cli.FloatFlag{Name: "hot-rate", Value: def.HotRate, Usage: "writes per second to the hot keys"},
			&cli.DurationFlag{Name: "burst-every", Value: def.BurstEvery, Usage: "how often a burst on the other keys starts"},
			&cli.DurationFlag{Name: "burst-for", Value: def.BurstFor, Usage: "how long a burst lasts"},
			&cli.FloatFlag{Name: "burst-rate", Value: def.BurstRate, Usage: "writes per second to the other keys during a burst (0: no bursts)"},
			&cli.IntFlag{Name: "payload-bytes", Value: def.PayloadBytes, Usage: "bytes of padding per row"},
			&cli.IntFlag{Name: "workers", Value: def.Workers, Usage: "concurrent writes"},
			&cli.IntFlag{Name: "batch", Value: def.Batch, Usage: "most rows per commit"},
			&cli.IntFlag{Name: "shard", Value: 0, Usage: "this writer's shard, when several writers share the keys"},
			&cli.IntFlag{Name: "shards", Value: 1, Usage: "how many writers share the keys"},
			&cli.DurationFlag{Name: "duration", Usage: "stop after this long (default: until stopped)"},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			d, t := dbConfig(c)
			client, err := spanner.NewClient(ctx, d.stream)
			if err != nil {
				return err
			}
			defer client.Close()
			w, err := soak.NewWriter(client, t.Events, soak.WriterConfig{
				Keys:         int(c.Int("keys")),
				HotFraction:  c.Float("hot-fraction"),
				HotRate:      c.Float("hot-rate"),
				BurstEvery:   c.Duration("burst-every"),
				BurstFor:     c.Duration("burst-for"),
				BurstRate:    c.Float("burst-rate"),
				PayloadBytes: int(c.Int("payload-bytes")),
				Workers:      int(c.Int("workers")),
				Batch:        int(c.Int("batch")),
				Shard:        int(c.Int("shard")),
				Shards:       int(c.Int("shards")),
				Duration:     c.Duration("duration"),
			})
			if err != nil {
				return err
			}
			return w.Run(ctx)
		},
	}
}

// runFlags are the runner's Screamer settings, as in the screamer command.
func runFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "runner-id", Usage: "runner name (default: random)"},
		&cli.StringFlag{Name: "start", Usage: "RFC 3339 time a new stream starts reading from (default: now). Start runners before the writer, or set this before its first write."},
		&cli.IntFlag{Name: "max-concurrent-partitions", Usage: "partitions this runner reads at once (0: unlimited)"},
		&cli.DurationFlag{Name: "heartbeat-interval", Usage: "change stream heartbeat interval (0: Screamer's default)"},
		&cli.DurationFlag{Name: "lease-ttl", Usage: "partition lease TTL (0: Screamer's default)"},
		&cli.BoolFlag{Name: "park", Value: true, Usage: "park idle partitions"},
		&cli.DurationFlag{Name: "park-interval", Usage: "time between a parked partition's catch-up reads (0: default)"},
		&cli.DurationFlag{Name: "park-idle-after", Usage: "idle time before a partition parks (0: default)"},
		&cli.BoolFlag{Name: "fair-share", Value: true, Usage: "even out partitions across runners"},
		&cli.DurationFlag{Name: "stats-every", Value: 30 * time.Second, Usage: "how often to log runner stats"},
		&cli.StringFlag{Name: "log-level", Value: "info"},
	}
}

func runCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "read the change stream with Screamer and record every delivery",
		Flags: append(dbFlags(), runFlags()...),
		Action: func(ctx context.Context, c *cli.Command) error {
			d, t := dbConfig(c)
			cfg := soak.RunnerConfig{
				StreamDSN:               d.stream,
				MetadataDSN:             d.metadata,
				DeliveriesDSN:           d.deliveries,
				Tables:                  t,
				RunnerID:                c.String("runner-id"),
				MaxConcurrentPartitions: int(c.Int("max-concurrent-partitions")),
				StatsEvery:              c.Duration("stats-every"),
			}
			if s := c.String("start"); s != "" {
				start, err := time.Parse(time.RFC3339Nano, s)
				if err != nil {
					return fmt.Errorf("bad --start: %w", err)
				}
				cfg.Start = start
			}
			opts := []screamer.Option{
				screamer.WithLogLevel(c.String("log-level")),
				screamer.WithParking(c.Bool("park")),
				screamer.WithFairShare(c.Bool("fair-share")),
			}
			if v := c.Duration("heartbeat-interval"); v > 0 {
				opts = append(opts, screamer.WithHeartbeatInterval(v))
			}
			if v := c.Duration("lease-ttl"); v > 0 {
				opts = append(opts, screamer.WithLeaseTTL(v))
			}
			if v := c.Duration("park-interval"); v > 0 {
				opts = append(opts, screamer.WithParkInterval(v))
			}
			if v := c.Duration("park-idle-after"); v > 0 {
				opts = append(opts, screamer.WithParkIdleAfter(v))
			}
			cfg.Options = opts
			return soak.RunRunner(ctx, cfg)
		},
	}
}

func checkCommand() *cli.Command {
	return &cli.Command{
		Name:  "check",
		Usage: "compare deliveries with writes; exits 1 if any write is missing or out of order",
		Flags: append(dbFlags(),
			&cli.DurationFlag{Name: "settle", Value: 2 * time.Minute,
				Usage: "only expect writes committed at least this long ago; give it more than the park interval plus a lease TTL"},
			&cli.DurationFlag{Name: "watch", Usage: "check again at this interval until stopped, instead of once"},
			&cli.BoolFlag{Name: "json", Usage: "print the report as JSON"},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			d, t := dbConfig(c)
			events, err := spanner.NewClient(ctx, d.stream)
			if err != nil {
				return err
			}
			defer events.Close()
			deliveries := events
			if d.deliveries != d.stream {
				if deliveries, err = spanner.NewClient(ctx, d.deliveries); err != nil {
					return err
				}
				defer deliveries.Close()
			}
			failed := false
			for {
				rep, err := soak.Check(ctx, events, deliveries, t, time.Now().Add(-c.Duration("settle")))
				if err != nil {
					return err
				}
				if c.Bool("json") {
					b, _ := json.Marshal(rep)
					fmt.Println(string(b))
				} else {
					fmt.Print(rep.String())
				}
				failed = failed || !rep.OK()
				if c.Duration("watch") <= 0 || sleep(ctx, c.Duration("watch")) != nil {
					break
				}
			}
			if failed {
				return cli.Exit("soak check failed: writes missing or out of order", 1)
			}
			return nil
		},
	}
}

func drillCommand() *cli.Command {
	var names []string
	for _, a := range soak.AllActions {
		names = append(names, string(a))
	}
	return &cli.Command{
		Name:      "drill",
		Usage:     "run several runners as child processes, and keep restarting, killing, freezing and scaling them",
		ArgsUsage: "-- [run flags]",
		Description: "Everything after -- is passed to each 'soak run' child, for example:\n" +
			"  soak drill --min-runners 2 --max-runners 4 --every 5m -- --dsn $DSN --max-concurrent-partitions 100",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "min-runners", Value: 2},
			&cli.IntFlag{Name: "max-runners", Value: 4},
			&cli.DurationFlag{Name: "every", Value: 5 * time.Minute, Usage: "time between actions"},
			&cli.StringFlag{Name: "actions", Value: strings.Join(names, ","), Usage: "comma-separated actions to pick from"},
			&cli.DurationFlag{Name: "freeze-for", Value: 45 * time.Second, Usage: "how long a frozen runner stays frozen; longer than the lease TTL"},
			&cli.DurationFlag{Name: "stop-timeout", Value: 30 * time.Second, Usage: "how long a stopped runner has to exit before it is killed"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			exe, err := os.Executable()
			if err != nil {
				return err
			}
			var actions []soak.Action
			for _, s := range strings.Split(c.String("actions"), ",") {
				a, err := soak.ParseAction(strings.TrimSpace(s))
				if err != nil {
					return err
				}
				actions = append(actions, a)
			}
			return soak.Drill(ctx, soak.DrillConfig{
				Command:     append([]string{exe, "run"}, c.Args().Slice()...),
				MinRunners:  int(c.Int("min-runners")),
				MaxRunners:  int(c.Int("max-runners")),
				Every:       c.Duration("every"),
				Actions:     actions,
				FreezeFor:   c.Duration("freeze-for"),
				StopTimeout: c.Duration("stop-timeout"),
			})
		},
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
