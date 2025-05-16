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
	spannerClient, err := spanner.NewClient(ctx, cfg.DSN)
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
						fmt.Fprintln(os.Stderr, err)
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
