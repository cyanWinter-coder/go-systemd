// Copyright 2015 CoreOS, Inc.
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

package activation

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync"
)

// Listeners returns a slice containing a net.Listener for each matching socket type
// passed to this process.
//
// The order of the file descriptors is preserved in the returned slice.
// Nil values are used to fill any gaps. For example if systemd were to return file descriptors
// corresponding with "udp, tcp, tcp", then the slice would contain {nil, net.Listener, net.Listener}
//
// A missing or inconsistent socket activation environment is reported as an
// error instead of silently adopting unrelated descriptors.
//
// Stream sockets are wrapped into listeners. A descriptor that is a socket of
// another type (for example a datagram socket) keeps producing the documented
// nil gap. If a descriptor that ought to be a listener cannot be wrapped, all
// descriptors received from systemd — listeners already created, the failing
// descriptor and descriptors not processed yet — are closed and the error is
// returned, so a failed startup does not keep holding the ports.
//
// The returned listeners can be closed more than once: only the first Close
// releases the underlying descriptor, later calls are no-ops returning nil.
func Listeners() ([]net.Listener, error) {
	files, _, err := filesFromEnv(true)
	if err != nil {
		return nil, err
	}
	return listenersFromFiles(files)
}

// ListenersWithNames maps a listener name to a set of net.Listener instances.
//
// Descriptors are grouped by their LISTEN_FDNAMES entry, preserving fd order
// inside each group. Every inherited descriptor must have a non-empty, unique
// name; a missing, short or duplicate name list is reported as an error that
// identifies the offending descriptor position and systemd fd number. On such
// an error all inherited descriptors are released.
func ListenersWithNames() (map[string][]net.Listener, error) {
	files, rawNames, err := filesFromEnv(true)
	if err != nil {
		return nil, err
	}

	names, err := namesForFiles(rawNames, len(files))
	if err != nil {
		closeFiles(files)
		return nil, err
	}

	listeners, err := listenersFromFiles(files)
	if err != nil {
		return nil, err
	}

	listenersWithNames := make(map[string][]net.Listener, len(listeners))
	for i, l := range listeners {
		// Non-listener socket types leave a nil gap, as in Listeners.
		if l == nil {
			continue
		}
		listenersWithNames[names[i]] = append(listenersWithNames[names[i]], l)
	}
	return listenersWithNames, nil
}

// TLSListeners returns a slice containing a net.Listener for each matching TCP socket type
// passed to this process.
// It uses default Listeners func and forces TCP sockets handlers to use TLS based on tlsConfig.
func TLSListeners(tlsConfig *tls.Config) ([]net.Listener, error) {
	listeners, err := Listeners()
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		for i, l := range listeners {
			// Activate TLS only for TCP sockets. Nil gaps (e.g. datagram
			// sockets in the inherited set) are left untouched.
			if isTCPListener(l) {
				listeners[i] = tls.NewListener(l, tlsConfig)
			}
		}
	}

	return listeners, nil
}

// TLSListenersWithNames maps a listener name to a net.Listener with
// the associated TLS configuration.
func TLSListenersWithNames(tlsConfig *tls.Config) (map[string][]net.Listener, error) {
	listeners, err := ListenersWithNames()

	if listeners == nil || err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		for _, ll := range listeners {
			// Activate TLS only for TCP sockets
			for i, l := range ll {
				if isTCPListener(l) {
					ll[i] = tls.NewListener(l, tlsConfig)
				}
			}
		}
	}

	return listeners, err
}

// isTCPListener reports whether l is a non-nil listener bound to a TCP
// network. It tolerates the nil gaps produced for non-stream descriptors.
func isTCPListener(l net.Listener) bool {
	return l != nil && l.Addr() != nil && l.Addr().Network() == "tcp"
}

// listenersFromFiles wraps the stream sockets in files into listeners.
//
// The returned slice is parallel to files: sockets that net.FileListener
// cannot turn into a listener because they are not stream sockets leave a nil
// gap (as documented for datagram sockets). Every inherited os.File is closed
// once wrapping has been attempted, because net.FileListener duplicates the
// descriptor for the returned listener. On a genuine wrapping error all
// listeners created so far and every not-yet-released file are closed.
func listenersFromFiles(files []*os.File) ([]net.Listener, error) {
	listeners := make([]net.Listener, len(files))
	released := make([]bool, len(files))

	for i, f := range files {
		l, err := net.FileListener(f)
		if err != nil {
			// Distinguish an expected socket-type mismatch (which keeps the
			// historical nil gap) from a descriptor that can never be a
			// listener or a broken stream socket.
			kind, kindErr := socketType(f)
			if kindErr != nil {
				closeListenersAndFiles(listeners, files, released)
				return nil, fmt.Errorf("activation: fd %d (index %d) is not a usable socket: %w",
					listenFdsStart+i, i, err)
			}
			if kind != sockStream {
				// A socket of another type (e.g. SOCK_DGRAM) cannot become a
				// net.Listener. Release the inherited descriptor and keep the
				// nil gap in the result slice.
				_ = f.Close()
				released[i] = true
				continue
			}

			closeListenersAndFiles(listeners, files, released)
			return nil, fmt.Errorf("activation: wrapping fd %d (index %d) as a listener failed: %w",
				listenFdsStart+i, i, err)
		}

		// The listener owns an independent duplicate of the descriptor, so
		// the inherited one is released immediately. From now on closing the
		// listener is the only release path for the socket.
		_ = f.Close()
		released[i] = true
		listeners[i] = &onceCloseListener{Listener: l}
	}

	return listeners, nil
}

// onceCloseListener wraps a net.Listener created from an inherited descriptor
// so that its resources are released exactly once. Repeated calls to Close do
// not touch the underlying listener again and return nil after a successful
// first close, instead of the usual "use of closed network connection" error.
//
// All other net.Listener methods are promoted from the wrapped listener.
// Callers that need the concrete implementation (for example
// *net.TCPListener) can retrieve it with Unwrap.
type onceCloseListener struct {
	net.Listener

	once     sync.Once
	closeErr error
}

func (l *onceCloseListener) Close() error {
	l.once.Do(func() {
		l.closeErr = l.Listener.Close()
	})
	if l.closeErr != nil {
		// Preserve the first close error for every caller while making sure
		// the underlying Close ran only once.
		return l.closeErr
	}
	return nil
}

// Unwrap returns the listener wrapped by l.
func (l *onceCloseListener) Unwrap() net.Listener {
	return l.Listener
}

// namesForFiles validates that the raw LISTEN_FDNAMES entries supply one
// non-empty, unique name for each of the n inherited descriptors and returns
// the n names in fd order. Extra names are accepted and ignored, matching the
// protocol's colon-separated list.
func namesForFiles(rawNames []string, n int) ([]string, error) {
	if n == 0 {
		return nil, nil
	}
	if len(rawNames) == 0 {
		return nil, fmt.Errorf("activation: LISTEN_FDNAMES is not set but %d named file descriptor(s) were received (fds %d-%d)",
			n, listenFdsStart, listenFdsStart+n-1)
	}
	if len(rawNames) < n {
		return nil, fmt.Errorf("activation: LISTEN_FDNAMES provides %d name(s) for %d file descriptor(s) starting at fd %d; first missing name is fd %d (index %d)",
			len(rawNames), n, listenFdsStart, listenFdsStart+len(rawNames), len(rawNames))
	}

	seen := make(map[string]int, n)
	names := make([]string, n)
	for i := 0; i < n; i++ {
		name := rawNames[i]
		if name == "" {
			return nil, fmt.Errorf("activation: missing LISTEN_FDNAMES name for fd %d (index %d)", listenFdsStart+i, i)
		}
		if prev, ok := seen[name]; ok {
			return nil, fmt.Errorf("activation: duplicate LISTEN_FDNAMES name %q for fd %d (index %d) and fd %d (index %d)",
				name, listenFdsStart+prev, prev, listenFdsStart+i, i)
		}
		seen[name] = i
		names[i] = name
	}
	return names, nil
}

// closeFiles closes every file, ignoring errors. It is used on error paths
// where some files may already have been closed.
func closeFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

// closeListenersAndFiles releases every descriptor received from systemd on a
// fatal conversion error.
//
// Listeners created so far are closed first (their Close is idempotent), then
// every inherited file that has not been released yet. Errors are ignored so
// that one failed close cannot prevent releasing the remaining descriptors.
func closeListenersAndFiles(listeners []net.Listener, files []*os.File, released []bool) {
	for _, l := range listeners {
		if l != nil {
			_ = l.Close()
		}
	}
	for i, f := range files {
		if f != nil && (i >= len(released) || !released[i]) {
			_ = f.Close()
		}
	}
}
