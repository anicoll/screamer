//go:build !unix

package soak

import (
	"errors"
	"os"
)

var errNoFreeze = errors.New("freezing a process needs SIGSTOP, which this OS does not have")

func freeze(*os.Process) error { return errNoFreeze }
func thaw(*os.Process) error   { return errNoFreeze }
