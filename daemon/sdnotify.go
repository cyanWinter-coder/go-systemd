// Copyright 2014 Docker, Inc.
// Copyright 2015-2018 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Package daemon provides a Go implementation of the sd_notify protocol.
// It can be used to inform systemd of service start-up completion, watchdog
// events, and other status changes.
//
// https://www.freedesktop.org/software/systemd/man/sd_notify.html#Description
package daemon

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

const (
	// SdNotifyReady tells the service manager that service startup is finished
	// or the service finished loading its configuration.
	SdNotifyReady = "READY=1"

	// SdNotifyStopping tells the service manager that the service is beginning
	// its shutdown.
	SdNotifyStopping = "STOPPING=1"

	// SdNotifyReloading tells the service manager that this service is
	// reloading its configuration. Note that you must call SdNotifyReady when
	// it completed reloading.
	SdNotifyReloading = "RELOADING=1"

	// SdNotifyWatchdog tells the service manager to update the watchdog
	// timestamp for the service.
	SdNotifyWatchdog = "WATCHDOG=1"
)

// notifySocketEnv is the name of the environment variable holding the
// address of the notification socket passed by the service manager.
const notifySocketEnv = "NOTIFY_SOCKET"

// NotifyError describes a failed attempt to deliver a notification to the
// service manager. It records the operation that failed, the NOTIFY_SOCKET
// address the notification was sent to, and the state that was being sent.
//
// The wrapped cause can be inspected with errors.Is, e.g.:
//
//	errors.Is(err, fs.ErrNotExist)   // the socket does not exist (yet)
//	errors.Is(err, fs.ErrPermission) // no permission to write to the socket
//	errors.Is(err, io.ErrShortWrite) // the datagram was truncated on write
type NotifyError struct {
	Op    string // "dial" or "write"
	Addr  string // NOTIFY_SOCKET address the notification was sent to
	State string // state that was being sent
	Err   error  // underlying cause
}

func (e *NotifyError) Error() string {
	return fmt.Sprintf("sd_notify: %s state %q to %q: %v", e.Op, e.State, e.Addr, e.Err)
}

func (e *NotifyError) Unwrap() error { return e.Err }

// validateState checks that state is a well-formed sd_notify message: one or
// more KEY=value assignments separated by newlines, with keys consisting of
// uppercase letters, digits (not leading) and underscores. Blank lines are
// tolerated so states may carry a trailing newline.
func validateState(state string) error {
	if state == "" {
		return errors.New("sd_notify: state is empty")
	}
	fields := 0
	for _, field := range strings.Split(state, "\n") {
		if field == "" {
			continue
		}
		key, _, ok := strings.Cut(field, "=")
		if !ok {
			return fmt.Errorf("sd_notify: invalid state field %q: want KEY=value", field)
		}
		if !validStateKey(key) {
			return fmt.Errorf("sd_notify: invalid variable name %q in state %q", key, state)
		}
		fields++
	}
	if fields == 0 {
		return fmt.Errorf("sd_notify: state %q contains no assignments", state)
	}
	return nil
}

func validStateKey(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// unixgramConn abstracts *net.UnixConn so tests can stub the socket.
type unixgramConn interface {
	Write(b []byte) (int, error)
	Close() error
}

// dialUnixgram opens a connection to the notification socket. It is a
// variable so tests can substitute a fake.
var dialUnixgram = func(addr *net.UnixAddr) (unixgramConn, error) {
	return net.DialUnix(addr.Net, nil, addr)
}

// sendState delivers state to addr as a single datagram.
func sendState(addr *net.UnixAddr, state string) error {
	conn, err := dialUnixgram(addr)
	if err != nil {
		return &NotifyError{Op: "dial", Addr: addr.Name, State: state, Err: err}
	}
	defer conn.Close()

	n, err := conn.Write([]byte(state))
	if err != nil {
		return &NotifyError{Op: "write", Addr: addr.Name, State: state, Err: err}
	}
	if n != len(state) {
		return &NotifyError{Op: "write", Addr: addr.Name, State: state, Err: io.ErrShortWrite}
	}
	return nil
}

// Notifier reports service state changes to the service manager over the
// socket named by NOTIFY_SOCKET. The socket address is captured once at
// construction, so later changes to the process environment by other
// components do not affect it.
//
// A Notifier is safe for concurrent use. Notifications are serialized, and
// re-sending the state that was just delivered is a no-op (with the
// exception of SdNotifyWatchdog, which is a keepalive and always sent), so
// concurrent sends of the same state are idempotent. A failed send leaves
// the Notifier's internal state untouched: the last delivered state is not
// updated and the environment is not cleaned up, so a later retry behaves
// exactly as if the failed send had never happened.
type Notifier struct {
	mu sync.Mutex

	addr     net.UnixAddr
	enabled  bool
	unsetEnv bool

	envUnset     bool   // NOTIFY_SOCKET has been cleaned up
	lastState    string // last successfully delivered state
	hasLastState bool
}

// NewNotifier returns a Notifier using the current value of NOTIFY_SOCKET.
// If unsetEnvironment is true, NOTIFY_SOCKET is removed from the process
// environment after the first successful notification — and only then, so a
// failed send does not alter process state. No other environment variables
// are touched.
func NewNotifier(unsetEnvironment bool) *Notifier {
	name := os.Getenv(notifySocketEnv)
	return &Notifier{
		addr:     net.UnixAddr{Name: name, Net: "unixgram"},
		enabled:  name != "",
		unsetEnv: unsetEnvironment,
	}
}

// Enabled reports whether NOTIFY_SOCKET was set when the Notifier was
// created, i.e. whether notifications will be sent at all.
func (n *Notifier) Enabled() bool { return n.enabled }

// Notify sends state to the service manager. The state is validated before
// anything is sent; an invalid state is an error and no datagram is
// written.
//
// It returns one of the following:
// (false, nil) - notification not supported (i.e. NOTIFY_SOCKET is unset)
// (false, err) - notification supported, but failure happened (e.g. error
// connecting to NOTIFY_SOCKET or while sending data)
// (true, nil) - notification supported, data has been sent
func (n *Notifier) Notify(state string) (bool, error) {
	if err := validateState(state); err != nil {
		return false, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.enabled {
		return false, nil
	}

	// Idempotency: re-delivering the state we just sent has no effect on
	// the service manager, so skip it. The watchdog keepalive is exempt:
	// its repetitions are the whole point.
	if n.hasLastState && n.lastState == state && state != SdNotifyWatchdog {
		return true, nil
	}

	if err := sendState(&n.addr, state); err != nil {
		return false, err
	}

	// Only after a successful send may we mutate process state.
	if n.unsetEnv && !n.envUnset {
		if err := os.Unsetenv(notifySocketEnv); err != nil {
			return false, fmt.Errorf("sd_notify: unset %s: %w", notifySocketEnv, err)
		}
		n.envUnset = true
	}
	n.lastState, n.hasLastState = state, true

	return true, nil
}

// Ready tells the service manager that service startup is finished.
func (n *Notifier) Ready() (bool, error) {
	return n.Notify(SdNotifyReady)
}

// Reloading tells the service manager that the service is reloading its
// configuration. The notification carries a MONOTONIC_USEC timestamp (when
// the monotonic clock is available) so the service manager can synchronize
// reload cycles. Call ReloadDone once the reload has completed.
func (n *Notifier) Reloading() (bool, error) {
	state := SdNotifyReloading
	if m := SdNotifyMonotonicUsec(); m != "" {
		state += "\n" + m
	}
	return n.Notify(state)
}

// ReloadDone tells the service manager that a reload announced with
// Reloading has completed.
func (n *Notifier) ReloadDone() (bool, error) {
	return n.Notify(SdNotifyReady)
}

// Stopping tells the service manager that the service is beginning its
// shutdown.
func (n *Notifier) Stopping() (bool, error) {
	return n.Notify(SdNotifyStopping)
}

// SdNotify sends a message to the init daemon. It is common to ignore the error.
// If `unsetEnvironment` is true, the environment variable `NOTIFY_SOCKET`
// will be unset after the notification has been successfully sent.
//
// The state is validated before anything is sent; an invalid state is
// reported as an error and no datagram is written.
//
// It returns one of the following:
// (false, nil) - notification not supported (i.e. NOTIFY_SOCKET is unset)
// (false, err) - notification supported, but failure happened (e.g. error connecting to NOTIFY_SOCKET or while sending data)
// (true, nil) - notification supported, data has been sent
func SdNotify(unsetEnvironment bool, state string) (bool, error) {
	return NewNotifier(unsetEnvironment).Notify(state)
}
