package screamer

import (
	"log"
	"os"

	"github.com/anicoll/screamer/cmd/command"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			command.ScreamerCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
