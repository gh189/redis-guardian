//go:build unix

package cluster

import (
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
)

func dropPrivileges(cmd *exec.Cmd) {
	uid, gid, ok := redisUser()
	if !ok {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: uid, Gid: gid},
	}
}

func GiveToRedis(path string) error {
	uid, gid, ok := redisUser()
	if !ok {
		return nil
	}
	return os.Chown(path, int(uid), int(gid))
}

func redisUser() (uid, gid uint32, ok bool) {
	if os.Geteuid() != 0 {
		return 0, 0, false
	}
	u, err := user.Lookup("redis")
	if err != nil {
		return 0, 0, false
	}
	uid64, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid64, _ := strconv.ParseUint(u.Gid, 10, 32)
	return uint32(uid64), uint32(gid64), true
}
