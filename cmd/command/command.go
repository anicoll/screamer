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
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var defaultHeartbeatInterval time.Duration = 3 * time.Second

func ScreamerCommand() *cli.Command {
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
			DefaultText: "Start timestamp with RFC3339 format, default: current timestamp",
		},
		&cli.StringFlag{
			Name:        "end",
			EnvVars:     []string{"END"},
			Required:    false,
			Value:       "",
			DefaultText: "End timestamp with RFC3339 format default: indefinite",
		},
		&cli.DurationFlag{
			Name:     "heartbeat-interval",
			EnvVars:  []string{"HEARTBEAT_INTERVAL"},
			Required: false,
			Value:    defaultHeartbeatInterval,
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
		Name:  "screamer",
		Flags: flags,
		Action: func(c *cli.Context) error {
			cfg, err := buildConfig(c)
			if err != nil {
				return err
			}

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

func buildConfig(c *cli.Context) (*screamer.Config, error) {
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

func nillableString(c *cli.Context, str string) *string {
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
	spannerClient, err := spanner.NewClient(ctx, cfg.DSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	defer spannerClient.Close()

	var partitionStorage screamer.PartitionStorage
	if *cfg.MetadataTable == "" {
		// partitionStorage = partitionstorage.NewInmemory()
	} else {
		partitionSpannerClient, err := spanner.NewClient(ctx, *cfg.PartitionDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
		ps := partitionstorage.NewSpanner(partitionSpannerClient, *cfg.MetadataTable)
		if err := ps.CreateTableIfNotExists(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return err
		}
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

	subscriber := screamer.NewSubscriber(spannerClient, cfg.Stream, partitionStorage, options...)
	consumer := &jsonOutputConsumer{out: os.Stdout}

	return subscriber.Subscribe(ctx, consumer)
}
