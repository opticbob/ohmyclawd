//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the child in its own process group so cancellation
// kills the whole tree (claude spawns helpers) rather than just the leader.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
