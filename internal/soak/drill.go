package soak

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

// DrillConfig says how a drill treats its runners.
type DrillConfig struct {
	// Command starts one runner. The drill appends --runner-id.
	Command []string
	// MinRunners and MaxRunners bound how many runners are up at once. The
	// drill starts with MinRunners.
	MinRunners, MaxRunners int
	// Every is how often the drill does something to a runner.
	Every time.Duration
	// Actions are the actions to pick from, at random. Default: all of them.
	Actions []Action
	// FreezeFor is how long a frozen runner stays frozen. Make it longer than
	// the lease TTL, so its leases are taken over while it is frozen.
	FreezeFor time.Duration
	// StopTimeout is how long a stopped runner has to release its leases
	// before it is killed.
	StopTimeout time.Duration
}

// Action is something a drill does to the runners.
type Action string

const (
	// ActionRestart stops a runner cleanly (SIGTERM) and starts a new one, like a rollout.
	ActionRestart Action = "restart"
	// ActionKill kills a runner (SIGKILL) and starts a new one, like a crash or a lost node.
	ActionKill Action = "kill"
	// ActionFreeze stops a runner (SIGSTOP) for FreezeFor and then resumes it,
	// like a long GC pause or a suspended VM. Its leases expire meanwhile, and
	// it must not overwrite the new owners' progress when it wakes.
	ActionFreeze Action = "freeze"
	// ActionScaleUp starts one more runner.
	ActionScaleUp Action = "scale-up"
	// ActionScaleDown stops a runner cleanly without replacing it.
	ActionScaleDown Action = "scale-down"
)

// AllActions are every action a drill can take.
var AllActions = []Action{ActionRestart, ActionKill, ActionFreeze, ActionScaleUp, ActionScaleDown}

// ParseAction checks an action's name.
func ParseAction(s string) (Action, error) {
	for _, a := range AllActions {
		if string(a) == s {
			return a, nil
		}
	}
	return "", fmt.Errorf("unknown drill action %q; want one of %v", s, AllActions)
}

type drillRunner struct {
	id     string
	cmd    *exec.Cmd
	done   chan struct{}
	frozen bool // guarded by the drill's mutex
}

// Drill runs runners as child processes until ctx ends, and every so often
// restarts, kills, freezes, adds or removes one. At the end it stops every
// runner cleanly.
func Drill(ctx context.Context, cfg DrillConfig) error {
	if len(cfg.Command) == 0 {
		return fmt.Errorf("drill needs a runner command")
	}
	if cfg.MinRunners < 1 || cfg.MaxRunners < cfg.MinRunners {
		return fmt.Errorf("need 1 <= min-runners <= max-runners")
	}
	if len(cfg.Actions) == 0 {
		cfg.Actions = AllActions
	}
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = 30 * time.Second
	}

	var (
		mu      sync.Mutex
		runners []*drillRunner
		next    int
		frozen  sync.WaitGroup
	)
	start := func() error {
		next++
		id := fmt.Sprintf("drill-%d-%d", os.Getpid(), next)
		args := append(append([]string{}, cfg.Command[1:]...), "--runner-id", id)
		cmd := exec.Command(cfg.Command[0], args...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start runner: %w", err)
		}
		r := &drillRunner{id: id, cmd: cmd, done: make(chan struct{})}
		go func() {
			err := cmd.Wait()
			log.Info().Str("runner_id", id).AnErr("exit", err).Msg("drill: runner exited")
			close(r.done)
		}()
		runners = append(runners, r)
		log.Info().Str("runner_id", id).Int("pid", cmd.Process.Pid).Msg("drill: started runner")
		return nil
	}
	stop := func(r *drillRunner) {
		_ = r.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-r.done:
		case <-time.After(cfg.StopTimeout):
			log.Warn().Str("runner_id", r.id).Msg("drill: runner did not stop in time; killing it")
			_ = r.cmd.Process.Kill()
			<-r.done
		}
	}
	// pick removes and returns a random live runner.
	pick := func() *drillRunner {
		i := rand.IntN(len(runners))
		r := runners[i]
		runners = append(runners[:i], runners[i+1:]...)
		return r
	}

	mu.Lock()
	for range cfg.MinRunners {
		if err := start(); err != nil {
			mu.Unlock()
			return err
		}
	}
	mu.Unlock()

	ticker := time.NewTicker(cfg.Every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			frozen.Wait()
			mu.Lock()
			defer mu.Unlock()
			var wg sync.WaitGroup
			for _, r := range runners {
				wg.Go(func() { stop(r) })
			}
			wg.Wait()
			return nil
		case <-ticker.C:
		}

		mu.Lock()
		// Replace runners that died on their own: that is a finding, and the
		// drill carries on with the configured number of runners.
		live := runners[:0]
		for _, r := range runners {
			select {
			case <-r.done:
				log.Error().Str("runner_id", r.id).Msg("drill: runner exited on its own")
			default:
				live = append(live, r)
			}
		}
		runners = live
		for len(runners) < cfg.MinRunners {
			if err := start(); err != nil {
				log.Error().Err(err).Msg("drill: failed to replace runner")
				break
			}
		}

		action := cfg.Actions[rand.IntN(len(cfg.Actions))]
		switch {
		case action == ActionScaleUp && len(runners) >= cfg.MaxRunners,
			action == ActionScaleDown && len(runners) <= cfg.MinRunners:
			action = ActionRestart
		}
		log.Info().Str("action", string(action)).Int("runners", len(runners)).Msg("drill: action")
		switch action {
		case ActionRestart:
			r := pick()
			stop(r)
			_ = start()
		case ActionKill:
			r := pick()
			_ = r.cmd.Process.Kill()
			<-r.done
			_ = start()
		case ActionFreeze:
			var thawed []*drillRunner
			for _, r := range runners {
				if !r.frozen {
					thawed = append(thawed, r)
				}
			}
			if len(thawed) == 0 {
				break
			}
			r := thawed[rand.IntN(len(thawed))]
			if err := freeze(r.cmd.Process); err != nil {
				log.Warn().Err(err).Str("runner_id", r.id).Msg("drill: could not freeze runner")
				break
			}
			r.frozen = true
			log.Info().Str("runner_id", r.id).Dur("for", cfg.FreezeFor).Msg("drill: froze runner")
			frozen.Go(func() {
				select {
				case <-time.After(cfg.FreezeFor):
				case <-ctx.Done():
				}
				mu.Lock()
				r.frozen = false
				mu.Unlock()
				_ = thaw(r.cmd.Process)
				log.Info().Str("runner_id", r.id).Msg("drill: thawed runner")
			})
		case ActionScaleUp:
			_ = start()
		case ActionScaleDown:
			stop(pick())
		}
		mu.Unlock()
	}
}
