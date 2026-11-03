package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/signal"
	"github.com/google/uuid"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

var defaultHeartbeatInterval time.Duration = 3 * time.Second

func ScreamerCommand() *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:     "dsn",
			Sources:  cli.EnvVars("DSN"),
			Required: true,
			Value:    "",
		},
		&cli.StringFlag{
			Name:     "stream",
			Sources:  cli.EnvVars("STREAM"),
			Required: true,
			Value:    "",
		},
		&cli.StringFlag{
			Name:     "metadata-table",
			Sources:  cli.EnvVars("METADATA_TABLE"),
			Required: false,
			Value:    "",
		},
		&cli.StringFlag{
			Name:        "start",
			Sources:     cli.EnvVars("START"),
			Required:    false,
			Value:       "",
			DefaultText: "Start timestamp with RFC3339 format, default: current timestamp",
		},
		&cli.StringFlag{
			Name:        "end",
			Sources:     cli.EnvVars("END"),
			Required:    false,
			Value:       "",
			DefaultText: "End timestamp with RFC3339 format default: indefinite",
		},
		&cli.DurationFlag{
			Name:     "heartbeat-interval",
			Sources:  cli.EnvVars("HEARTBEAT_INTERVAL"),
			Required: false,
			Value:    defaultHeartbeatInterval,
		},
		&cli.IntFlag{
			Name:     "max-concurrent-partitions",
			Sources:  cli.EnvVars("MAX_CONCURRENT_PARTITIONS"),
			Required: false,
			Value:    0,
			Usage:    "Maximum partitions this runner reads at once (0 = unlimited). Also sizes the gRPC channel pool for change stream reads.",
		},
		&cli.DurationFlag{
			Name:     "lease-ttl",
			Sources:  cli.EnvVars("LEASE_TTL"),
			Required: false,
			Value:    0,
			Usage:    "How long a partition lease lasts without renewal (0 = default, 30s). A runner that dies is taken over after this long.",
		},
		&cli.BoolFlag{
			Name:    "park",
			Sources: cli.EnvVars("PARK"),
			Value:   true,
			Usage:   "Park idle partitions and read them in short catch-up queries, so a runner can look after more partitions than it streams at once. --park=false streams every partition.",
		},
		&cli.DurationFlag{
			Name:    "park-interval",
			Sources: cli.EnvVars("PARK_INTERVAL"),
			Value:   0,
			Usage:   "How long a parked partition waits between catch-up reads, and so the most extra delay for a change to an idle partition (0 = default, 10s).",
		},
		&cli.DurationFlag{
			Name:    "park-idle-after",
			Sources: cli.EnvVars("PARK_IDLE_AFTER"),
			Value:   0,
			Usage:   "How long a partition delivers only heartbeats before it is parked (0 = default, 1m).",
		},
		&cli.BoolFlag{
			Name:    "fair-share",
			Sources: cli.EnvVars("FAIR_SHARE"),
			Value:   true,
			Usage:   "Even out streaming partitions across runners: a runner holding more than its share hands its quietest partitions to others. --fair-share=false lets each runner keep what it claims.",
		},
		&cli.StringFlag{
			Name:        "partition-dsn",
			Sources:     cli.EnvVars("PARTITION_DSN"),
			Required:    false,
			Value:       "",
			DefaultText: "Database dsn for use by the partition metadata table. If not provided, the main dsn will be used.",
		},
	}
	return &cli.Command{
		Name:  "screamer",
		Flags: flags,
		Action: func(ctx context.Context, c *cli.Command) error {
			cfg, err := buildConfig(c)
			if err != nil {
				return err
			}

			eg, ctx := errgroup.WithContext(ctx)

			eg.Go(func() error {
				return signal.SignalHandler(ctx)
			})

			eg.Go(func() error {
				return run(ctx, cfg)
			})

			if err := eg.Wait(); err != nil {
				if errors.Is(err, signal.ErrSignal) {
					return nil
				}
				return err
			}
			return nil
		},
	}
}

func buildConfig(c *cli.Command) (*screamer.Config, error) {
	cfg := &screamer.Config{}
	cfg.DSN = c.String("dsn")
	cfg.Stream = c.String("stream")

	dur := c.Duration("heartbeat-interval")
	cfg.HeartbeatInterval = &dur

	cfg.MaxConcurrentPartitions = int(c.Int("max-concurrent-partitions"))
	cfg.LeaseTTL = c.Duration("lease-ttl")
	cfg.DisableParking = !c.Bool("park")
	cfg.DisableFairShare = !c.Bool("fair-share")
	cfg.ParkInterval = c.Duration("park-interval")
	cfg.ParkIdleAfter = c.Duration("park-idle-after")
	cfg.MetadataTable = nillableString(c, "metadata-table")
	cfg.PartitionDSN = nillableString(c, "partition-dsn")
	if cfg.PartitionDSN == nil {
		cfg.PartitionDSN = &cfg.DSN
	}

	if endTime := nillableString(c, "end"); endTime != nil {
		t, err := time.Parse(time.RFC3339, *endTime)
		if err != nil {
			return nil, err
		}
		cfg.End = &t
	}
	if startTime := nillableString(c, "start"); startTime != nil {
		t, err := time.Parse(time.RFC3339, *startTime)
		if err != nil {
			return nil, err
		}
		cfg.Start = &t
	}

	return cfg, nil
}

func nillableString(c *cli.Command, str string) *string {
	s := c.String(str)
	if c.IsSet(str) {
		return &s
	}
	return nil
}

type jsonOutputConsumer struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *jsonOutputConsumer) Consume(change []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.out.Write(change)
	return err
}

func run(ctx context.Context, cfg *screamer.Config) error {
	eg, ctx := errgroup.WithContext(ctx)
	// Change stream reads get their own client, with a channel pool sized for the
	// partition limit, so open streams cannot starve metadata writes.
	spannerClient, err := screamer.NewChangeStreamClient(ctx, cfg.DSN, cfg.MaxConcurrentPartitions)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	defer spannerClient.Close()
	runnerID := uuid.NewString()

	var partitionStorage screamer.PartitionStorage
	if *cfg.MetadataTable == "" {
		partitionStorage = partitionstorage.NewInmemory()
	} else {
		partitionSpannerClient, err := spanner.NewClient(ctx, *cfg.PartitionDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		ps := partitionstorage.NewSpanner(partitionSpannerClient, *cfg.MetadataTable)
		if err := ps.RunMigrations(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		if err := ps.RegisterRunner(ctx, runnerID); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		eg.Go(func() error {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					if err := ps.RefreshRunner(ctx, runnerID); err != nil {
						return err
					}
				}
			}
		})
		partitionStorage = ps
	}

	options := []screamer.Option{}
	if cfg.Start != nil && !cfg.Start.IsZero() {
		options = append(options, screamer.WithStartTimestamp(*cfg.Start))
	}
	if cfg.End != nil && !cfg.End.IsZero() {
		options = append(options, screamer.WithEndTimestamp(*cfg.End))
	}
	if *cfg.HeartbeatInterval != 0 {
		options = append(options, screamer.WithHeartbeatInterval(*cfg.HeartbeatInterval))
	}
	if cfg.MaxConcurrentPartitions > 0 {
		options = append(options, screamer.WithMaxConcurrentPartitions(cfg.MaxConcurrentPartitions))
	}
	if cfg.LeaseTTL > 0 {
		options = append(options, screamer.WithLeaseTTL(cfg.LeaseTTL))
	}
	if cfg.DisableFairShare {
		options = append(options, screamer.WithFairShare(false))
	}
	if cfg.DisableParking {
		options = append(options, screamer.WithParking(false))
	}
	if cfg.ParkInterval > 0 {
		options = append(options, screamer.WithParkInterval(cfg.ParkInterval))
	}
	if cfg.ParkIdleAfter > 0 {
		options = append(options, screamer.WithParkIdleAfter(cfg.ParkIdleAfter))
	}
	if cfg.Priority != int32(spannerpb.RequestOptions_PRIORITY_UNSPECIFIED) {
		options = append(options, screamer.WithSpannerRequestPriotiry(spannerpb.RequestOptions_Priority(cfg.Priority)))
	}

	subscriber := screamer.NewSubscriber(spannerClient, cfg.Stream, runnerID, partitionStorage, options...)
	consumer := &jsonOutputConsumer{out: os.Stdout}

	eg.Go(func() error {
		return subscriber.Subscribe(ctx, consumer)
	})

	return eg.Wait()
}
