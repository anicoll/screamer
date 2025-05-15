package main

import (
	"context"
	"log"
	"os"

	"github.com/anicoll/screamer/cmd/command"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Commands: []*cli.Command{
			command.ScreamerCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
