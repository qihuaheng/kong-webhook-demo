//go:build !linux

package store

import (
	"syscall"
	"time"
)

// TCP_USER_TIMEOUT is Linux-only; keepalives and per-operation timeouts
// still bound hangs elsewhere.
func setTCPUserTimeout(time.Duration) func(network, address string, c syscall.RawConn) error {
	return nil
}
