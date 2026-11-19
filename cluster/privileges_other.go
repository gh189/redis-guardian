//go:build !unix

package cluster

import "os/exec"

func dropPrivileges(*exec.Cmd) {}

func GiveToRedis(string) error { return nil }
