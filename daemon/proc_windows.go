//go:build windows

package main

import "os/exec"

// setProcessGroup is a no-op on Windows: exec.CommandContext's default
// Cancel (Process.Kill) applies, and WaitDelay bounds any orphaned pipes.
func setProcessGroup(cmd *exec.Cmd) {}
