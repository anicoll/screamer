package partitionstorage

import (
	"context"
	"os"
	"strconv"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewTestContainer(ctx context.Context, image string, envVars map[string]string, ports []string, waitfor wait.Strategy, cmdArgs ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		SkipReaper:   true,
		Image:        image,
		Env:          envVars,
		ExposedPorts: ports,
		WaitingFor:   waitfor,
		Cmd:          cmdArgs,
	}

	// picks up local test env to clean up containers
	if skipReaper := os.Getenv("SKIP_REAPER"); skipReaper != "" {
		shouldSkipReaper, err := strconv.ParseBool(skipReaper)
		if err != nil {
			return nil, err
		}
		req.SkipReaper = shouldSkipReaper
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}
