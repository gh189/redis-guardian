package cluster

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type Supervisor struct {
	Bin     string
	Args    []string
	DataDir string
	Gate    func(ctx context.Context)

	mu  sync.Mutex
	cmd *exec.Cmd
}

func (s *Supervisor) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if s.Gate != nil {
			s.Gate(ctx)
		}
		if ctx.Err() != nil {
			return
		}
		s.wipe()

		cmd := exec.Command(s.Bin, s.Args...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		dropPrivileges(cmd)
		if err := cmd.Start(); err != nil {
			slog.Error("failed to start redis-server", "err", err)
		} else {
			slog.Info("redis-server started", "pid", cmd.Process.Pid)
			s.setCmd(cmd)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				slog.Warn("redis-server exited", "err", err)
			case <-ctx.Done():
				cmd.Process.Signal(syscall.SIGTERM)
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					cmd.Process.Kill()
					<-done
				}
				slog.Info("redis-server stopped")
			}
			s.setCmd(nil)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func (s *Supervisor) wipe() {
	stale, _ := filepath.Glob(filepath.Join(s.DataDir, "*.rdb"))
	stale = append(stale, filepath.Join(s.DataDir, "nodes.conf"), filepath.Join(s.DataDir, "appendonlydir"),
		filepath.Join(s.DataDir, "backup"), filepath.Join(s.DataDir, "restore"))
	for _, f := range stale {
		if err := os.RemoveAll(f); err != nil {
			slog.Warn("cannot remove stale redis file", "file", f, "err", err)
		}
	}
}

func (s *Supervisor) setCmd(cmd *exec.Cmd) {
	s.mu.Lock()
	s.cmd = cmd
	s.mu.Unlock()
}

func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd != nil
}

func (s *Supervisor) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		s.cmd.Process.Kill()
	}
}

type Instance struct {
	cmd    *exec.Cmd
	exited chan struct{}
}

func StartInstance(bin string, args []string) (*Instance, error) {
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	dropPrivileges(cmd)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	in := &Instance{cmd: cmd, exited: make(chan struct{})}
	go func() {
		cmd.Wait()
		close(in.exited)
	}()
	return in, nil
}

func (in *Instance) Exited() bool {
	select {
	case <-in.exited:
		return true
	default:
		return false
	}
}

func (in *Instance) Stop() {
	in.cmd.Process.Kill()
	<-in.exited
}
