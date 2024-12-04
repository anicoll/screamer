package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/anicoll/screamer/pkg/model"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/scream"
	"github.com/anicoll/screamer/pkg/signal"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var defaultHeartbeatInterval time.Duration = 10

func ScreamCommand() *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:     "dsn",
			EnvVars:  []string{"DSN"},
			Required: true,
			Value:    "",
		},
		&cli.StringFlag{
			Name:     "stream",
			EnvVars:  []string{"STREAM"},
			Required: true,
			Value:    "",
		},
		&cli.StringFlag{
			Name:     "metadata-table",
			EnvVars:  []string{"METADATA_TABLE"},
			Required: false,
			Value:    "",
		},
		&cli.StringFlag{
			Name:        "start",
			EnvVars:     []string{"START"},
			Required:    false,
			Value:       "",
			DefaultText: "Start timestamp with RFC3339 format (default: current timestamp)",
		},
		&cli.StringFlag{
			Name:        "end",
			EnvVars:     []string{"END"},
			Required:    false,
			Value:       "",
			DefaultText: "End timestamp with RFC3339 format (default: indefinite)",
		},
		&cli.DurationFlag{
			Name:     "heartbeat-interval",
			EnvVars:  []string{"HEARTBEAT_INTERVAL"},
			Required: false,
			Value:    10,
		},
		&cli.StringFlag{
			Name:        "partition-dsn",
			EnvVars:     []string{"PARTITION_DSN"},
			Required:    false,
			Value:       "",
			DefaultText: "Database dsn for use by the partition metadata table. If not provided, the main dsn will be used.",
		},
	}
	return &cli.Command{
		Name:  "sop-pipeline",
		Flags: flags,
		Action: func(c *cli.Context) error {
			cfg := buildConfig(c)

			eg, ctx := errgroup.WithContext(c.Context)

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

func buildConfig(c *cli.Context) *scream.Config {
	cfg := &scream.Config{}
	cfg.DSN = c.String("dsn")

	if c.IsSet("heartbeat-interval") {
		dur := c.Duration("heartbeat-interval")
		cfg.HeartbeatInterval = &dur
	} else {
		cfg.HeartbeatInterval = &defaultHeartbeatInterval
	}

	cfg.MetadataTable = nillableString(c, "metadata-table")
	cfg.PartitionDSN = nillableString(c, "partition-dsn")
	if cfg.PartitionDSN == nil {
		cfg.PartitionDSN = &cfg.DSN
	}

	cfg.End = nillableTimestamp(c, "end")
	cfg.Start = nillableTimestamp(c, "start")

	return cfg
}

func nillableString(c *cli.Context, str string) *string {
	s := c.String(str)
	if c.IsSet(str) {
		return &s
	}
	return nil
}

func nillableTimestamp(c *cli.Context, str string) *time.Time {
	s := c.Timestamp(str)
	if c.IsSet(str) {
		return s
	}
	return nil
}

type jsonOutputConsumer struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *jsonOutputConsumer) Consume(change *model.DataChangeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.out).Encode(change)
}

func run(ctx context.Context, cfg *scream.Config) error {
	spannerClient, err := spanner.NewClient(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	defer spannerClient.Close()

	var partitionStorage scream.PartitionStorage
	if *cfg.MetadataTable == "" {
		partitionStorage = partitionstorage.NewInmemory()
	} else {
		partitionSpannerClient, err := spanner.NewClient(ctx, *cfg.PartitionDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		ps := partitionstorage.NewSpanner(partitionSpannerClient, *cfg.MetadataTable)
		if err := ps.CreateTableIfNotExists(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		partitionStorage = ps
	}

	options := []scream.Option{}
	if cfg.Start != nil && !cfg.Start.IsZero() {
		options = append(options, scream.WithStartTimestamp(*cfg.Start))
	}
	if cfg.End != nil && !cfg.End.IsZero() {
		options = append(options, scream.WithEndTimestamp(*cfg.End))
	}
	if *cfg.HeartbeatInterval != 0 {
		options = append(options, scream.WithHeartbeatInterval(*cfg.HeartbeatInterval))
	}
	if cfg.Priority != int32(spannerpb.RequestOptions_PRIORITY_UNSPECIFIED) {
		options = append(options, scream.WithSpannerRequestPriotiry(spannerpb.RequestOptions_Priority(cfg.Priority)))
	}

	subscriber := scream.NewSubscriber(spannerClient, cfg.Stream, partitionStorage, options...)
	consumer := &jsonOutputConsumer{out: os.Stdout}

	return subscriber.Subscribe(ctx, consumer)
}
