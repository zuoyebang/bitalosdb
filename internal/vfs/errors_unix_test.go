//go:build darwin || dragonfly || freebsd || linux || openbsd
// +build darwin dragonfly freebsd linux openbsd

package vfs

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestIsNoSpaceError(t *testing.T) {
	err := errors.WithStack(unix.ENOSPC)
	require.True(t, IsNoSpaceError(err))
}
