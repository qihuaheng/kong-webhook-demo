//go:build linux

package store

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func setTCPUserTimeout(d time.Duration) func(network, address string, c syscall.RawConn) error {
	return func(_, _ string, c syscall.RawConn) error {
		var serr error
		err := c.Control(func(fd uintptr) {
			serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(d.Milliseconds()))
		})
		if err != nil {
			return err
		}
		return serr
	}
}
