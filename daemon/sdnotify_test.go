// Copyright 2016 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package daemon

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func listenUnixgram(t *testing.T, path string) *net.UnixConn {
	t.Helper()
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readDatagrams reads datagrams from conn until the timeout expires and
// returns what it got.
func readDatagrams(t *testing.T, conn *net.UnixConn, timeout time.Duration) []string {
	t.Helper()
	var got []string
	deadline := time.Now().Add(timeout)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(time.Until(deadline))); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return got
			}
			t.Fatal(err)
		}
		got = append(got, string(buf[:n]))
	}
}

func TestSdNotify(t *testing.T) {
	testDir := t.TempDir()

	notifySocket := testDir + "/notify-socket.sock"
	listenUnixgram(t, notifySocket)

	tests := []struct {
		unsetEnv  bool
		envSocket string
		state     string

		wsent       bool
		werr        bool
		wenvCleared bool
	}{
		// (true, nil) - notification supported, data has been sent
		{false, notifySocket, "STATUS=TestSdNotify test message #0", true, false, false},
		// successful send with unsetEnv cleans up the environment
		{true, notifySocket, "STATUS=TestSdNotify test message #1", true, false, true},
		// (false, err) - notification supported, but failure happened;
		// a failed send must not clean up the environment
		{true, testDir + "/missing.sock", "STATUS=TestSdNotify test message #2", false, true, false},
		// (false, nil) - notification not supported
		{true, "", "STATUS=TestSdNotify test message #3", false, false, false},
	}

	for i, tt := range tests {
		t.Setenv(notifySocketEnv, tt.envSocket)
		sent, err := SdNotify(tt.unsetEnv, tt.state)

		if sent != tt.wsent {
			t.Errorf("#%d: expected send result %t, got %t", i, tt.wsent, sent)
		}
		if tt.werr && err == nil {
			t.Errorf("#%d: want non-nil err, got nil", i)
		} else if !tt.werr && err != nil {
			t.Errorf("#%d: want nil err, got %v", i, err)
		}
		if tt.envSocket != "" {
			if cleared := os.Getenv(notifySocketEnv) == ""; cleared != tt.wenvCleared {
				t.Errorf("#%d: environment variable cleared = %t, want %t", i, cleared, tt.wenvCleared)
			}
		}
	}
}

func TestSdNotifyInvalidState(t *testing.T) {
	testDir := t.TempDir()
	notifySocket := testDir + "/notify-socket.sock"
	conn := listenUnixgram(t, notifySocket)
	t.Setenv(notifySocketEnv, notifySocket)

	invalid := []string{
		"",                       // empty
		"\n",                     // no assignments
		"READY",                  // missing value
		"ready=1",                // lowercase key
		"1READY=1",               // leading digit
		"READY=1\nnot an assign", // garbage after valid field
	}
	for _, state := range invalid {
		sent, err := SdNotify(false, state)
		if err == nil {
			t.Errorf("SdNotify(false, %q): want validation error, got nil", state)
		}
		if sent {
			t.Errorf("SdNotify(false, %q): want sent=false, got true", state)
		}
	}

	// Nothing may have been written to the socket.
	if got := readDatagrams(t, conn, 100*time.Millisecond); len(got) != 0 {
		t.Errorf("invalid states were sent anyway: %q", got)
	}
}

func TestSdNotifyMissingSocket(t *testing.T) {
	missing := t.TempDir() + "/missing.sock"
	t.Setenv(notifySocketEnv, missing)

	sent, err := SdNotify(false, SdNotifyReady)
	if sent || err == nil {
		t.Fatalf("SdNotify to missing socket = (%t, %v), want (false, err)", sent, err)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error %v does not match fs.ErrNotExist", err)
	}
	var ne *NotifyError
	if !errors.As(err, &ne) {
		t.Fatalf("error %v is not a *NotifyError", err)
	}
	if ne.Addr != missing {
		t.Errorf("NotifyError.Addr = %q, want %q", ne.Addr, missing)
	}
	if !strings.Contains(err.Error(), missing) {
		t.Errorf("error %q does not mention the socket address", err)
	}
}

func TestSdNotifySocketPermissionDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, permission checks are bypassed")
	}
	testDir := t.TempDir()
	notifySocket := testDir + "/notify-socket.sock"
	listenUnixgram(t, notifySocket)
	if err := os.Chmod(testDir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(testDir, 0o700) })
	t.Setenv(notifySocketEnv, notifySocket)

	sent, err := SdNotify(false, SdNotifyReady)
	if sent || err == nil {
		t.Fatalf("SdNotify to unreadable socket = (%t, %v), want (false, err)", sent, err)
	}
	if !errors.Is(err, fs.ErrPermission) {
		t.Errorf("error %v does not match fs.ErrPermission", err)
	}
}

// fakeConn simulates a socket that truncates writes.
type fakeConn struct {
	n int
}

func (f fakeConn) Write(b []byte) (int, error) { return f.n, nil }
func (f fakeConn) Close() error                { return nil }

func TestSdNotifyShortWrite(t *testing.T) {
	t.Setenv(notifySocketEnv, t.TempDir()+"/notify-socket.sock")

	orig := dialUnixgram
	t.Cleanup(func() { dialUnixgram = orig })
	dialUnixgram = func(addr *net.UnixAddr) (unixgramConn, error) {
		return fakeConn{n: len(SdNotifyReady) - 1}, nil
	}

	sent, err := SdNotify(false, SdNotifyReady)
	if sent || err == nil {
		t.Fatalf("SdNotify with truncating socket = (%t, %v), want (false, err)", sent, err)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("error %v does not match io.ErrShortWrite", err)
	}
	var ne *NotifyError
	if errors.As(err, &ne) && ne.Addr == "" {
		t.Error("NotifyError does not carry the socket address")
	}
}

func TestNotifierUnsetEnvironmentOnlyOnSuccess(t *testing.T) {
	testDir := t.TempDir()
	notifySocket := testDir + "/notify-socket.sock"
	t.Setenv(notifySocketEnv, notifySocket)
	t.Setenv("OTHER_COMPONENT_VAR", "keepme")

	n := NewNotifier(true)

	// The socket does not exist yet: the send fails and the environment
	// must be left untouched.
	if sent, err := n.Notify(SdNotifyReady); sent || err == nil {
		t.Fatalf("Notify to missing socket = (%t, %v), want (false, err)", sent, err)
	}
	if got := os.Getenv(notifySocketEnv); got != notifySocket {
		t.Errorf("NOTIFY_SOCKET = %q after failed send, want %q", got, notifySocket)
	}

	// Once the socket appears, the same Notifier must succeed and only
	// then clean up NOTIFY_SOCKET.
	listenUnixgram(t, notifySocket)
	if sent, err := n.Notify(SdNotifyReady); !sent || err != nil {
		t.Fatalf("Notify after socket appeared = (%t, %v), want (true, nil)", sent, err)
	}
	if got := os.Getenv(notifySocketEnv); got != "" {
		t.Errorf("NOTIFY_SOCKET = %q after successful send, want unset", got)
	}
	if got := os.Getenv("OTHER_COMPONENT_VAR"); got != "keepme" {
		t.Errorf("OTHER_COMPONENT_VAR = %q, want %q", got, "keepme")
	}
}

func TestNotifierConcurrentIdempotent(t *testing.T) {
	testDir := t.TempDir()
	notifySocket := testDir + "/notify-socket.sock"
	conn := listenUnixgram(t, notifySocket)
	t.Setenv(notifySocketEnv, notifySocket)

	n := NewNotifier(false)

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sent, err := n.Ready()
			if !sent {
				errs[i] = fmt.Errorf("Ready() = (%t, %v), want (true, nil)", sent, err)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	// Concurrent sends of the same state collapse into a single datagram.
	got := readDatagrams(t, conn, 300*time.Millisecond)
	if len(got) != 1 || got[0] != SdNotifyReady {
		t.Fatalf("received %q, want exactly one %q", got, SdNotifyReady)
	}

	// The watchdog keepalive is exempt from deduplication.
	for i := 0; i < 2; i++ {
		if sent, err := n.Notify(SdNotifyWatchdog); !sent || err != nil {
			t.Fatalf("Notify(WATCHDOG) = (%t, %v), want (true, nil)", sent, err)
		}
	}
	got = readDatagrams(t, conn, 300*time.Millisecond)
	if len(got) != 2 || got[0] != SdNotifyWatchdog || got[1] != SdNotifyWatchdog {
		t.Fatalf("received %q, want two %q", got, SdNotifyWatchdog)
	}
}

func TestNotifierLifecycleStates(t *testing.T) {
	testDir := t.TempDir()
	notifySocket := testDir + "/notify-socket.sock"
	conn := listenUnixgram(t, notifySocket)
	t.Setenv(notifySocketEnv, notifySocket)

	n := NewNotifier(false)

	if sent, err := n.Reloading(); !sent || err != nil {
		t.Fatalf("Reloading() = (%t, %v), want (true, nil)", sent, err)
	}
	if sent, err := n.ReloadDone(); !sent || err != nil {
		t.Fatalf("ReloadDone() = (%t, %v), want (true, nil)", sent, err)
	}
	if sent, err := n.Stopping(); !sent || err != nil {
		t.Fatalf("Stopping() = (%t, %v), want (true, nil)", sent, err)
	}

	got := readDatagrams(t, conn, 300*time.Millisecond)
	if len(got) != 3 {
		t.Fatalf("received %q, want 3 datagrams", got)
	}
	if !strings.HasPrefix(got[0], SdNotifyReloading+"\nMONOTONIC_USEC=") {
		t.Errorf("reload notification %q does not carry MONOTONIC_USEC", got[0])
	}
	if got[1] != SdNotifyReady {
		t.Errorf("reload-done notification = %q, want %q", got[1], SdNotifyReady)
	}
	if got[2] != SdNotifyStopping {
		t.Errorf("stopping notification = %q, want %q", got[2], SdNotifyStopping)
	}
}

func TestNotifierNotEnabled(t *testing.T) {
	os.Unsetenv(notifySocketEnv)
	n := NewNotifier(false)
	if n.Enabled() {
		t.Error("Enabled() = true without NOTIFY_SOCKET")
	}
	sent, err := n.Ready()
	if sent || err != nil {
		t.Errorf("Ready() without NOTIFY_SOCKET = (%t, %v), want (false, nil)", sent, err)
	}
}
