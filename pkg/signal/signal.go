package signal

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var ErrSignal error = errors.New("received the quit signal")

func SignalHandler(ctx context.Context) error {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Default().Print("signalHandler: upstream context called cancel()")
	case <-sigint:
		log.Default().Print("signalHandler: os signal received")
		return ErrSignal
	}
	return nil
}
