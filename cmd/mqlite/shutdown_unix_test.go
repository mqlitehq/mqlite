//go:build darwin || linux

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

type signalBrokerProcess struct {
	cmd     *exec.Cmd
	addr    string
	dbPath  string
	lines   chan string
	done    chan struct{}
	waitErr error
	logMu   sync.Mutex
	log     strings.Builder
}

func startSignalBroker(t *testing.T) *signalBrokerProcess {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "broker.db")
	cmd := exec.Command(mqliteBin, "serve", "--addr", addr)
	cmd.Env = append(os.Environ(),
		"MQLITE_DB=file:"+dbPath,
		"MQLITE_SYNC=FULL",
		"MQLITE_TOKENS=off",
		"MQLITE_MONITOR_TOKENS=",
		"MQLITE_METRICS=off",
		"MQLITE_UI=off",
		"MQLITE_CORS=off",
		"MQLITE_DLQ_RETENTION=off",
		"NO_COLOR=1",
	)
	cmd.Stdout = io.Discard
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &signalBrokerProcess{
		cmd: cmd, addr: addr, dbPath: dbPath,
		lines: make(chan string, 32), done: make(chan struct{}),
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			p.logMu.Lock()
			p.log.WriteString(line)
			p.log.WriteByte('\n')
			p.logMu.Unlock()
			select {
			case p.lines <- line:
			default:
			}
		}
		close(p.lines)
	}()
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Kill()
		<-p.done
	})
	p.waitReady(t)
	return p
}

func (p *signalBrokerProcess) logs() string {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	return p.log.String()
}

func (p *signalBrokerProcess) waitReady(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				select {
				case <-p.done:
					t.Fatalf("broker exited before ready: %v\n%s", p.waitErr, p.logs())
				default:
					t.Fatalf("broker stderr closed before ready\n%s", p.logs())
				}
			}
			if strings.Contains(line, "ready") {
				return
			}
		case <-p.done:
			t.Fatalf("broker exited before ready: %v\n%s", p.waitErr, p.logs())
		case <-timer.C:
			t.Fatalf("broker did not become ready\n%s", p.logs())
		}
	}
}

func (p *signalBrokerProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.waitErr
	case <-time.After(timeout):
		t.Fatalf("broker did not exit within %s\n%s", timeout, p.logs())
		return nil
	}
}

func (p *signalBrokerProcess) waitListenerClosed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", p.addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		select {
		case <-p.done:
			t.Fatalf("broker exited before the forced second signal: %v\n%s", p.waitErr, p.logs())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener remained open after the first signal\n%s", p.logs())
		}
		timer := time.NewTimer(10 * time.Millisecond)
		<-timer.C
	}
}

func holdActiveRequest(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	request := fmt.Sprintf(
		"POST %s HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 1024\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
		wire.PathSend, addr,
	)
	if _, err := io.WriteString(conn, request); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		t.Fatalf("read 100-continue status: %v", err)
	}
	if !strings.Contains(status, " 100 ") {
		_ = conn.Close()
		t.Fatalf("unexpected interim response %q", strings.TrimSpace(status))
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			t.Fatalf("read 100-continue headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	return conn
}

func TestBrokerSingleSignalDrainsAndPreservesAcknowledgedMessage(t *testing.T) {
	p := startSignalBroker(t)
	remoteEnv := append(os.Environ(),
		"MQLITE_ENDPOINT=http://"+p.addr,
		"MQLITE_TOKEN=",
	)
	create := exec.Command(mqliteBin, "create-queue", "shutdown-acked")
	create.Env = remoteEnv
	if output, err := create.CombinedOutput(); err != nil {
		t.Fatalf("create queue before shutdown: %v: %s", err, output)
	}
	send := exec.Command(mqliteBin, "send", "shutdown-acked", "persisted")
	send.Env = remoteEnv
	if output, err := send.CombinedOutput(); err != nil {
		t.Fatalf("send before shutdown: %v: %s", err, output)
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := p.wait(t, 10*time.Second); err != nil {
		t.Fatalf("graceful broker exit: %v\n%s", err, p.logs())
	}

	eng, err := engine.Open(context.Background(), engine.Options{
		DB: "file:" + p.dbPath, DisableBackground: true, Synchronous: "FULL",
	})
	if err != nil {
		t.Fatalf("reopen after graceful shutdown: %v", err)
	}
	defer eng.Close()
	msgs, err := eng.Peek(context.Background(), "shutdown-acked", engine.PeekOptions{Max: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || string(msgs[0].Body) != "persisted" {
		t.Fatalf("acknowledged message after restart = %+v", msgs)
	}
}

func TestBrokerSecondSignalForcesBlockedShutdown(t *testing.T) {
	p := startSignalBroker(t)
	conn := holdActiveRequest(t, p.addr)
	defer conn.Close()

	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	// Shutdown closes the listener only after serveSignalContext has restored the
	// default signal behavior; this is the deterministic barrier for signal two.
	p.waitListenerClosed(t)
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	err := p.wait(t, 5*time.Second)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("second signal exit = %v, want signal termination\n%s", err, p.logs())
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGINT {
		t.Fatalf("second signal status = %v, want SIGINT\n%s", exitErr.Sys(), p.logs())
	}
}
