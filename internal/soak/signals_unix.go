//go:build unix

package soak

import (
	"os"
	"syscall"
)

func freeze(p *os.Process) error { return p.Signal(syscall.SIGSTOP) }
func thaw(p *os.Process) error   { return p.Signal(syscall.SIGCONT) }
