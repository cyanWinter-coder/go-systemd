// Copyright 2026 RedHat, Inc.
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

//go:build unix

package activation

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestFilesWithErrorValidation checks that the socket activation environment
// is validated instead of being used as-is.
func TestFB579FilesWithErrorValidation(t *testing.T) {
	pid := strconv.Itoa(os.Getpid())

	tests := []struct {
		name    string
		pid     string
		fds     string
		wantErr string
	}{
		{name: "not activated", pid: "", fds: "", wantErr: ""},
		{name: "fds without pid", pid: "", fds: "2", wantErr: "LISTEN_PID is not set"},
		{name: "invalid pid", pid: "notanumber", fds: "1", wantErr: "invalid LISTEN_PID"},
		{name: "foreign pid", pid: "1", fds: "1", wantErr: "does not match current process"},
		{name: "pid without fds", pid: pid, fds: "", wantErr: "LISTEN_FDS is not set"},
		{name: "invalid fds", pid: pid, fds: "x", wantErr: "invalid LISTEN_FDS"},
		{name: "negative fds", pid: pid, fds: "-1", wantErr: "negative LISTEN_FDS"},
		{name: "zero fds", pid: pid, fds: "0", wantErr: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LISTEN_PID", tt.pid)
			t.Setenv("LISTEN_FDS", tt.fds)
			t.Setenv("LISTEN_FDNAMES", "")

			files, err := FilesWithError(false)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(files) != 0 {
					t.Fatalf("unexpected files: %v", files)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q, got nil (files: %v)", tt.wantErr, files)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
			if files != nil {
				t.Fatalf("files must not be returned on error, got %v", files)
			}
		})
	}
}

// TestFilesKeepsLegacyBehaviour makes sure the legacy Files API still returns
// nil instead of an error for a mismatched environment.
func TestFB579FilesKeepsLegacyBehaviour(t *testing.T) {
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "1")

	if files := Files(false); files != nil {
		t.Fatalf("Files() = %v, want nil for a foreign LISTEN_PID", files)
	}
}

// TestFilesUnsetEnvOnlyTouchesListenVars verifies that unsetting the socket
// activation environment leaves every other variable alone.
func TestFB579FilesUnsetEnvOnlyTouchesListenVars(t *testing.T) {
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "1")
	t.Setenv("LISTEN_FDNAMES", "a")
	t.Setenv("ACTIVATION_TEST_KEEP", "keep-me")

	Files(true)

	if got := os.Getenv("ACTIVATION_TEST_KEEP"); got != "keep-me" {
		t.Fatalf("unrelated environment variable modified: %q", got)
	}
	for _, key := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		if _, ok := os.LookupEnv(key); ok {
			t.Fatalf("%s was not unset", key)
		}
	}
}

// TestFilesNoUnsetEnvKeepsEnv verifies the opposite: unsetEnv=false leaves the
// environment intact.
func TestFB579FilesNoUnsetEnvKeepsEnv(t *testing.T) {
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "1")
	t.Setenv("LISTEN_FDNAMES", "a")

	Files(false)

	for _, key := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		if _, ok := os.LookupEnv(key); !ok {
			t.Fatalf("%s was unexpectedly unset", key)
		}
	}
}

// TestNamesForFiles exercises the named-listener validation.
func TestFB579NamesForFiles(t *testing.T) {
	tests := []struct {
		name    string
		names   []string
		n       int
		want    []string
		wantErr string
	}{
		{name: "no fds", names: nil, n: 0},
		{name: "no names", names: nil, n: 1, wantErr: "LISTEN_FDNAMES is not set"},
		{name: "too few names", names: []string{"a"}, n: 2, wantErr: "provides 1 name(s) for 2 file descriptor(s)"},
		{name: "empty name", names: []string{"a", ""}, n: 2, wantErr: "missing LISTEN_FDNAMES name for fd 4 (index 1)"},
		{name: "duplicate name", names: []string{"dup", "dup"}, n: 2, wantErr: `duplicate LISTEN_FDNAMES name "dup" for fd 3 (index 0) and fd 4 (index 1)`},
		{name: "extra names ignored", names: []string{"a", "b", "c"}, n: 2, want: []string{"a", "b"}},
		{name: "valid", names: []string{"a", "b"}, n: 2, want: []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := namesForFiles(tt.names, tt.n)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got error %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("namesForFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

// fb579FakeListener counts Close calls without touching real descriptors.
type fb579FakeListener struct {
	addr   net.Addr
	mu     sync.Mutex
	closes int
}

func (l *fb579FakeListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }

func (l *fb579FakeListener) Addr() net.Addr { return l.addr }

func (l *fb579FakeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closes++
	return nil
}

// TestOnceCloseListener verifies that repeated Close calls release the
// listener once and return nil.
func TestFB579OnceCloseListener(t *testing.T) {
	f := &fb579FakeListener{addr: &net.TCPAddr{}}
	l := &onceCloseListener{Listener: f}

	if err := l.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}

	f.mu.Lock()
	closes := f.closes
	f.mu.Unlock()
	if closes != 1 {
		t.Fatalf("underlying listener closed %d times, want 1", closes)
	}
}

// TestListenersFromFilesReleasesInherited checks that wrapping a descriptor
// releases the inherited file and that closing the returned listener frees
// the socket exactly once.
func TestFB579ListenersFromFilesReleasesInherited(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	f, err := ln.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	// The original listener is no longer needed; the duplicated descriptor
	// is what socket activation would pass on.
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	listeners, err := listenersFromFiles([]*os.File{f})
	if err != nil {
		t.Fatalf("listenersFromFiles() error: %v", err)
	}
	if len(listeners) != 1 || listeners[0] == nil {
		t.Fatalf("expected one listener, got %v", listeners)
	}

	// The inherited os.File must have been released right after wrapping.
	if _, err := f.Stat(); err == nil {
		t.Fatal("inherited file descriptor still open after wrapping")
	}

	// The listener must still hold the socket.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dialing wrapped listener: %v", err)
	}
	_ = conn.Close()

	if err := listeners[0].Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := listeners[0].Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}

	// Closing the listener must have released the socket, so the address can
	// be bound again.
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("address %s still in use after Close: %v", addr, err)
	}
	_ = again.Close()
}

// TestTLSListenersDoubleClose checks that a TLS listener wrapping an
// activated listener can also be closed repeatedly.
func TestFB579TLSListenersDoubleClose(t *testing.T) {
	f := &fb579FakeListener{addr: &net.TCPAddr{}}
	l := tls.NewListener(&onceCloseListener{Listener: f}, &tls.Config{})

	if err := l.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
}

// TestListenersFromFilesCleanupOnError verifies that when one descriptor
// cannot be wrapped, every descriptor is released, including the ones that
// were already turned into listeners.
func TestFB579ListenersFromFilesCleanupOnError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	tcpFile, err := ln.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close() //nolint:errcheck // test cleanup

	// The first descriptor is wrapped successfully, the second is not a
	// socket at all.
	files := []*os.File{tcpFile, r}
	listeners, err := listenersFromFiles(files)
	if err == nil {
		for _, l := range listeners {
			if l != nil {
				_ = l.Close()
			}
		}
		t.Fatal("expected an error for a non-socket descriptor")
	}
	if !strings.Contains(err.Error(), "fd 4 (index 1)") {
		t.Fatalf("error %q does not identify the failing descriptor", err)
	}

	for i, f := range files {
		if _, err := f.Stat(); err == nil {
			t.Fatalf("file %d was not closed on error", i)
		}
	}

	// Both the listener created before the failure and the port it held must
	// have been released.
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("address %s still in use after failed wrapping: %v", addr, err)
	}
	_ = again.Close()
}

// TestPacketConnsFromFilesCleanupOnError is the packet connection analogue of
// TestListenersFromFilesCleanupOnError.
func TestFB579PacketConnsFromFilesCleanupOnError(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close() //nolint:errcheck // test cleanup

	packetErrFile, err := os.CreateTemp(t.TempDir(), "not-a-socket")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packetErrFile.Close() })

	files := []*os.File{r, packetErrFile}
	conns, err := packetConnsFromFiles(files)
	if err == nil {
		for _, c := range conns {
			if c != nil {
				_ = c.Close()
			}
		}
		t.Fatal("expected an error for a non-socket descriptor")
	}

	for i, f := range files {
		if _, err := f.Stat(); err == nil {
			t.Fatalf("file %d was not closed on error", i)
		}
	}
}

// TestPacketConnsLeavesGapAndReleasesTCP checks that a stream socket in the
// packet connection list becomes a nil gap and its descriptor is released.
func TestFB579PacketConnsLeavesGapAndReleasesTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lnFile, err := ln.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // test cleanup

	conns, err := packetConnsFromFiles([]*os.File{lnFile})
	if err != nil {
		t.Fatalf("packetConnsFromFiles() error: %v", err)
	}
	if len(conns) != 1 || conns[0] != nil {
		t.Fatalf("expected a nil gap, got %v", conns)
	}
	if _, err := lnFile.Stat(); err == nil {
		t.Fatal("stream socket descriptor not released")
	}
}

// TestListenersFromFilesLeavesGapForDatagram checks that a datagram socket in
// a listener list becomes a nil gap while the neighbouring stream socket is
// still wrapped.
func TestFB579ListenersFromFilesLeavesGapForDatagram(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	udpFile, err := udp.File()
	if err != nil {
		t.Fatal(err)
	}
	_ = udp.Close()

	tcp := fb579TcpFiles(t, 1)

	listeners, err := listenersFromFiles([]*os.File{udpFile, tcp[0]})
	if err != nil {
		t.Fatalf("listenersFromFiles() error: %v", err)
	}
	defer func() {
		for _, l := range listeners {
			if l != nil {
				_ = l.Close()
			}
		}
	}()

	if len(listeners) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(listeners))
	}
	if listeners[0] != nil {
		t.Fatalf("datagram socket should leave a nil gap, got %T", listeners[0])
	}
	if listeners[1] == nil {
		t.Fatal("stream socket should have been wrapped")
	}
	if _, err := udpFile.Stat(); err == nil {
		t.Fatal("datagram socket descriptor not released")
	}
}

// TestListenersUnixCloseKeepsPath checks that a unix socket activated
// listener can be closed without unlinking the socket path, so that systemd
// can keep handing the same socket to a restarted service.
func TestFB579ListenersUnixCloseKeepsPath(t *testing.T) {
	path := t.TempDir() + "/activated.sock"
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ul := ln.(*net.UnixListener)
	// systemd owns the path; the activated listener must not unlink it. This
	// mirrors the unsubscribe-on-close behaviour of a socket unit.
	ul.SetUnlinkOnClose(false)
	f, err := ul.File()
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()

	listeners, err := listenersFromFiles([]*os.File{f})
	if err != nil {
		t.Fatalf("listenersFromFiles() error: %v", err)
	}
	if len(listeners) != 1 || listeners[0] == nil {
		t.Fatalf("expected one listener, got %v", listeners)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket path missing before Close: %v", err)
	}

	if err := listeners[0].Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := listeners[0].Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket path removed by Close: %v", err)
	}
}

// TestListenersHelperProcess is run as a child process by the tests below. It
// exercises the public API with descriptors inherited from the parent.
func TestFB579ListenersHelperProcess(t *testing.T) {
	mode := os.Getenv("ACTIVATION_TEST_HELPER")
	if mode == "" {
		t.Skip("helper process only")
	}

	if os.Getenv("ACTIVATION_TEST_FIX_PID") != "" {
		_ = os.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
	}

	var err error
	switch mode {
	case "listeners":
		var listeners []net.Listener
		listeners, err = Listeners()
		if err == nil {
			defer func() {
				for _, l := range listeners {
					if l != nil {
						_ = l.Close()
					}
				}
			}()
			fmt.Printf("OK: %d\n", len(listeners))
		}
	case "named":
		var named map[string][]net.Listener
		named, err = ListenersWithNames()
		if err == nil {
			defer func() {
				for _, ll := range named {
					for _, l := range ll {
						_ = l.Close()
					}
				}
			}()
			keys := make([]string, 0, len(named))
			for k := range named {
				keys = append(keys, k)
			}
			fmt.Printf("OK: %d %s\n", len(named), strings.Join(keys, ","))
		}
	case "packetconns":
		var conns []net.PacketConn
		conns, err = PacketConns()
		if err == nil {
			defer func() {
				for _, c := range conns {
					if c != nil {
						_ = c.Close()
					}
				}
			}()
			fmt.Printf("OK: %d\n", len(conns))
		}
	case "doubleclose":
		var listeners []net.Listener
		listeners, err = Listeners()
		if err == nil {
			for _, l := range listeners {
				if l == nil {
					continue
				}
				if cerr := l.Close(); cerr != nil {
					fmt.Printf("ERR: first close: %v\n", cerr)
					os.Exit(0)
				}
				if cerr := l.Close(); cerr != nil {
					fmt.Printf("ERR: second close: %v\n", cerr)
					os.Exit(0)
				}
			}
			fmt.Printf("OK: %d\n", len(listeners))
		}
	case "tlsgap":
		var listeners []net.Listener
		listeners, err = TLSListeners(&tls.Config{})
		if err == nil {
			defer func() {
				for _, l := range listeners {
					if l != nil {
						_ = l.Close()
					}
				}
			}()
			fmt.Printf("OK: %d\n", len(listeners))
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		os.Exit(2)
	}

	if err != nil {
		fmt.Printf("ERR: %v\n", err)
		os.Exit(0)
	}
	os.Exit(0)
}

// fb579RunHelper starts the helper process with the given files as inherited
// descriptors (starting at fd 3) and environment. Unless the caller supplies
// its own LISTEN_PID, the helper rewrites it to its own pid.
func fb579RunHelper(t *testing.T, mode string, files []*os.File, env ...string) string {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestFB579ListenersHelperProcess$", "-test.v=false")
	cmd.ExtraFiles = files
	cmd.Env = append(os.Environ(), "ACTIVATION_TEST_HELPER="+mode)
	if !fb579HasEnv(env, "LISTEN_PID=") {
		cmd.Env = append(cmd.Env, "ACTIVATION_TEST_FIX_PID=1")
	}
	cmd.Env = append(cmd.Env, env...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, out)
	}
	return string(out)
}

// fb579HasEnv reports whether env contains an assignment for the given prefix.
func fb579HasEnv(env []string, prefix string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

// fb579TcpFiles returns n file objects for listening TCP sockets.
func fb579TcpFiles(t *testing.T, n int) []*os.File {
	t.Helper()

	files := make([]*os.File, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		f, err := ln.(*net.TCPListener).File()
		if err != nil {
			t.Fatal(err)
		}
		_ = ln.Close()
		t.Cleanup(func() { _ = f.Close() })
		files = append(files, f)
	}
	return files
}

func TestFB579ListenersRejectsForeignPID(t *testing.T) {
	out := fb579RunHelper(t, "listeners", fb579TcpFiles(t, 1),
		"LISTEN_PID=1", "LISTEN_FDS=1", "LISTEN_FDNAMES=socket")
	if !strings.Contains(out, "LISTEN_PID") || !strings.Contains(out, "does not match current process") {
		t.Fatalf("helper output %q does not report the PID mismatch", out)
	}
}

func TestFB579ListenersRejectsMissingPID(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestFB579ListenersHelperProcess$", "-test.v=false")
	cmd.ExtraFiles = fb579TcpFiles(t, 1)
	cmd.Env = append(os.Environ(),
		"ACTIVATION_TEST_HELPER=listeners",
		"LISTEN_PID=", "LISTEN_FDS=1",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "LISTEN_PID is not set") {
		t.Fatalf("helper output %q does not report the missing PID", out)
	}
}

func TestFB579ListenersNamedDuplicate(t *testing.T) {
	out := fb579RunHelper(t, "named", fb579TcpFiles(t, 2),
		"LISTEN_FDS=2", "LISTEN_FDNAMES=dup:dup")
	if !strings.Contains(out, `duplicate LISTEN_FDNAMES name "dup"`) ||
		!strings.Contains(out, "fd 3 (index 0)") || !strings.Contains(out, "fd 4 (index 1)") {
		t.Fatalf("helper output %q does not locate the duplicate name", out)
	}
}

func TestFB579ListenersNamedMissing(t *testing.T) {
	out := fb579RunHelper(t, "named", fb579TcpFiles(t, 2),
		"LISTEN_FDS=2", "LISTEN_FDNAMES=only-one")
	if !strings.Contains(out, "LISTEN_FDNAMES provides 1 name(s) for 2 file descriptor(s)") {
		t.Fatalf("helper output %q does not report the missing name", out)
	}
}

func TestFB579ListenersNamedEmpty(t *testing.T) {
	out := fb579RunHelper(t, "named", fb579TcpFiles(t, 2),
		"LISTEN_FDS=2", "LISTEN_FDNAMES=first:")
	if !strings.Contains(out, "missing LISTEN_FDNAMES name for fd 4 (index 1)") {
		t.Fatalf("helper output %q does not report the empty name", out)
	}
}

func TestFB579ListenersNamedGrouping(t *testing.T) {
	out := fb579RunHelper(t, "named", fb579TcpFiles(t, 2),
		"LISTEN_FDS=2", "LISTEN_FDNAMES=http:metrics")
	// Map iteration order is unspecified; check the count and both names.
	if !strings.Contains(out, "OK: 2 ") ||
		!strings.Contains(out, "http") || !strings.Contains(out, "metrics") {
		t.Fatalf("helper output %q does not contain the grouped listeners", out)
	}
}

func TestFB579ListenersNonSocket(t *testing.T) {
	regular, err := os.CreateTemp(t.TempDir(), "regular")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regular.Close() })

	out := fb579RunHelper(t, "listeners", []*os.File{regular},
		"LISTEN_FDS=1", "LISTEN_FDNAMES=regular")
	if !strings.Contains(out, "fd 3 (index 0) is not a usable socket") {
		t.Fatalf("helper output %q does not identify the non-socket descriptor", out)
	}
}

func TestFB579PacketConnsMixedGap(t *testing.T) {
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	udpFile, err := u.File()
	if err != nil {
		t.Fatal(err)
	}
	_ = u.Close()
	t.Cleanup(func() { _ = udpFile.Close() })

	tcp := fb579TcpFiles(t, 1)
	out := fb579RunHelper(t, "packetconns", []*os.File{udpFile, tcp[0]},
		"LISTEN_FDS=2", "LISTEN_FDNAMES=udp:tcp")
	if !strings.Contains(out, "OK: 2") {
		t.Fatalf("helper output %q does not report two packet connections", out)
	}
}

// TestListenersPublicDoubleClose exercises repeated Close through the public
// Listeners constructor in an inherited-fd helper process.
func TestFB579ListenersPublicDoubleClose(t *testing.T) {
	out := fb579RunHelper(t, "doubleclose", fb579TcpFiles(t, 2),
		"LISTEN_FDS=2", "LISTEN_FDNAMES=a:b")
	if strings.Contains(out, "ERR:") {
		t.Fatalf("repeated Close failed: %s", out)
	}
	if !strings.Contains(out, "OK: 2") {
		t.Fatalf("helper output %q does not report successful double close", out)
	}
}

// TestTLSListenersWithDatagramGap ensures TLSListeners tolerates the nil gaps
// produced by datagram descriptors instead of panicking on l.Addr().
func TestFB579TLSListenersWithDatagramGap(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	udpFile, err := udp.File()
	if err != nil {
		t.Fatal(err)
	}
	_ = udp.Close()
	t.Cleanup(func() { _ = udpFile.Close() })

	tcp := fb579TcpFiles(t, 1)
	out := fb579RunHelper(t, "tlsgap", []*os.File{udpFile, tcp[0]},
		"LISTEN_FDS=2", "LISTEN_FDNAMES=udp:http")
	if strings.Contains(out, "panic") || strings.Contains(out, "ERR:") {
		t.Fatalf("TLSListeners did not tolerate nil gap: %s", out)
	}
	if !strings.Contains(out, "OK: 2") {
		t.Fatalf("helper output %q does not report the mixed set", out)
	}
}

// TestIsTCPListenerNilSafe locks the nil-gap guard.
func TestFB579IsTCPListenerNilSafe(t *testing.T) {
	if isTCPListener(nil) {
		t.Fatal("nil listener must not be classified as TCP")
	}
	if isTCPListener(&onceCloseListener{Listener: &fb579FakeListener{addr: &net.UnixAddr{Name: "x", Net: "unix"}}}) {
		t.Fatal("unix listener must not be classified as TCP")
	}
	if !isTCPListener(&onceCloseListener{Listener: &fb579FakeListener{addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}}}) {
		t.Fatal("tcp listener should be classified as TCP")
	}
}
