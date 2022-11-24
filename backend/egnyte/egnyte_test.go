// Test Box filesystem interface
package egnyte_test

import (
	"testing"

	"github.com/rclone/rclone/backend/egnyte"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "egnyte:",
		NilObject:  (*egnyte.Object)(nil),
	})
}
