// Copyright 2025
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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultMinWatchdogInterval is the smallest heartbeat interval used by
	// NewWatchdogHeartbeat. It prevents an almost-zero WATCHDOG_USEC value from
	// turning the heartbeat loop into a busy loop.
	DefaultMinWatchdogInterval = time.Second

	// DefaultMaxWatchdogInterval is the largest heartbeat interval used by
	// NewWatchdogHeartbeat. It keeps keepalives reasonably frequent even when
	// the service manager allows a very long watchdog timeout.
	DefaultMaxWatchdogInterval = time.Minute
)

// Common watchdog heartbeat errors.
var (
	// ErrWatchdogAlreadyRunning is returned when Run or Start is called twice
	// on the same heartbeat before the previous run has exited.
	ErrWatchdogAlreadyRunning = errors.New("daemon: watchdog heartbeat already running")

	// ErrWatchdogStopped is returned when a terminal heartbeat is started
	// again after Stop.
	ErrWatchdogStopped = errors.New("daemon: watchdog heartbeat is stopped")

	// ErrWatchdogNotifyUnavailable means that watchdog is enabled, but
	// NOTIFY_SOCKET is not set, so no keepalive can be delivered.
	ErrWatchdogNotifyUnavailable = errors.New("daemon: notification socket is not available")

	errInvalidIntervalBounds = errors.New("daemon: watchdog interval minimum must not be greater than maximum")
)

// HeartbeatSender sends one WATCHDOG=1 keepalive. It must not retry internally:
// returning an error terminates the heartbeat loop.
type HeartbeatSender func() error

// WatchdogOption configures a WatchdogHeartbeat.
type WatchdogOption func(*watchdogConfig)

type watchdogConfig struct {
	timeout          time.Duration
	timeoutSet       bool
	unsetEnvironment bool
	minInterval      time.Duration
	maxInterval      time.Duration
	sender           HeartbeatSender
	clock            heartbeatClock
}

// WithWatchdogTimeout supplies WATCHDOG_USEC directly instead of reading the
// environment. Zero or a negative duration disables the heartbeat.
func WithWatchdogTimeout(timeout time.Duration) WatchdogOption {
	return func(cfg *watchdogConfig) {
		cfg.timeout = timeout
		cfg.timeoutSet = true
	}
}

// WithWatchdogIntervalBounds replaces the default lower and upper interval
// bounds. A zero bound leaves the corresponding default in place.
func WithWatchdogIntervalBounds(minimum, maximum time.Duration) WatchdogOption {
	return func(cfg *watchdogConfig) {
		cfg.minInterval = minimum
		cfg.maxInterval = maximum
	}
}

// WithHeartbeatSender replaces the systemd notification sender. It is useful
// in tests and for integrating another keepalive transport.
func WithHeartbeatSender(sender HeartbeatSender) WatchdogOption {
	return func(cfg *watchdogConfig) { cfg.sender = sender }
}

// WithUnsetWatchdogEnvironment removes WATCHDOG_USEC and WATCHDOG_PID after
// they have been read.
func WithUnsetWatchdogEnvironment() WatchdogOption {
	return func(cfg *watchdogConfig) { cfg.unsetEnvironment = true }
}

type heartbeatState uint8

const (
	heartbeatNew heartbeatState = iota
	heartbeatRunning
	heartbeatStopping
	heartbeatStopped
)

// WatchdogHeartbeat sends watchdog keepalives at half of the configured
// WATCHDOG_USEC timeout, clamped to the configured bounds.
//
// A WatchdogHeartbeat must not be copied after first use. A zero value is a
// disabled, no-op heartbeat.
type WatchdogHeartbeat struct {
	interval time.Duration
	send     HeartbeatSender
	clock    heartbeatClock

	stopCh chan struct{}

	mu     sync.Mutex
	state  heartbeatState
	doneCh chan struct{}
	err    error
}

// Watchdog is an alias kept for callers that prefer the shorter name.
type Watchdog = WatchdogHeartbeat

// NewWatchdogHeartbeat reads watchdog configuration from the environment and
// returns a heartbeat controller.
//
// If watchdog is not enabled for the current process, the returned heartbeat
// is disabled: Enabled and Interval return false and zero, and Run, Start and
// Stop are safe no-ops. Configuration/parse errors are returned unchanged.
func NewWatchdogHeartbeat(unsetEnvironment bool, options ...WatchdogOption) (*WatchdogHeartbeat, error) {
	cfg := watchdogConfig{
		unsetEnvironment: unsetEnvironment,
		minInterval:      DefaultMinWatchdogInterval,
		maxInterval:      DefaultMaxWatchdogInterval,
		clock:            realHeartbeatClock{},
	}
	for _, option := range options {
		option(&cfg)
	}

	if cfg.minInterval <= 0 {
		cfg.minInterval = DefaultMinWatchdogInterval
	}
	if cfg.maxInterval <= 0 {
		cfg.maxInterval = DefaultMaxWatchdogInterval
	}
	if cfg.minInterval > cfg.maxInterval {
		return nil, errInvalidIntervalBounds
	}

	timeout := cfg.timeout
	if !cfg.timeoutSet {
		enabledTimeout, err := SdWatchdogEnabled(cfg.unsetEnvironment)
		if err != nil {
			return nil, err
		}
		timeout = enabledTimeout
	}
	if timeout <= 0 {
		return &WatchdogHeartbeat{clock: cfg.clock}, nil
	}

	sender := cfg.sender
	if sender == nil {
		sender = sendSystemdWatchdog
	}

	return &WatchdogHeartbeat{
		interval: ClampWatchdogInterval(timeout/2, cfg.minInterval, cfg.maxInterval),
		send:     sender,
		clock:    cfg.clock,
	}, nil
}

// NewWatchdog is the short-name equivalent of NewWatchdogHeartbeat with
// default options.
func NewWatchdog(unsetEnvironment bool) (*WatchdogHeartbeat, error) {
	return NewWatchdogHeartbeat(unsetEnvironment)
}

// SdWatchdogHeartbeatInterval returns the clamped keepalive interval derived
// from WATCHDOG_USEC. It returns (0, nil) when watchdog is disabled for this
// process.
func SdWatchdogHeartbeatInterval(unsetEnvironment bool) (time.Duration, error) {
	timeout, err := SdWatchdogEnabled(unsetEnvironment)
	if err != nil {
		return 0, err
	}
	if timeout == 0 {
		return 0, nil
	}
	return ClampWatchdogInterval(timeout/2, DefaultMinWatchdogInterval, DefaultMaxWatchdogInterval), nil
}

// ClampWatchdogInterval clamps interval to [minimum, maximum]. Non-positive
// bounds are ignored.
func ClampWatchdogInterval(interval, minimum, maximum time.Duration) time.Duration {
	if minimum > 0 && interval < minimum {
		interval = minimum
	}
	if maximum > 0 && interval > maximum {
		interval = maximum
	}
	return interval
}

// Enabled reports whether watchdog keepalives should be sent.
func (h *WatchdogHeartbeat) Enabled() bool {
	return h != nil && h.send != nil && h.interval > 0
}

// Interval returns the clamped keepalive interval. It returns zero when the
// heartbeat is disabled.
func (h *WatchdogHeartbeat) Interval() time.Duration {
	if h == nil || !h.Enabled() {
		return 0
	}
	return h.interval
}

// Run executes the heartbeat loop until Stop is called, ctx is canceled, or a
// keepalive fails. The first send error is returned and no further send is
// attempted. Cancellation and explicit stop are normal termination.
func (h *WatchdogHeartbeat) Run(ctx context.Context) error {
	started, err := h.begin()
	if err != nil {
		return err
	}
	if !started {
		return nil
	}

	err = h.loop(ctx)
	h.finish(err)
	return err
}

// Start runs Run in a background goroutine. Use Wait or Err to observe a send
// failure, and Stop to request graceful termination.
func (h *WatchdogHeartbeat) Start(ctx context.Context) error {
	started, err := h.begin()
	if err != nil {
		return err
	}
	if !started {
		return nil
	}

	go func() {
		err := h.loop(ctx)
		h.finish(err)
	}()
	return nil
}

// Wait blocks until a running heartbeat loop exits and returns its terminal
// error. Stop termination returns nil; cancellation returns the context error;
// a send failure returns the wrapped sender error. It returns nil if the
// heartbeat was never started.
func (h *WatchdogHeartbeat) Wait() error {
	if h == nil {
		return nil
	}

	h.mu.Lock()
	done := h.doneCh
	h.mu.Unlock()

	if done != nil {
		<-done
	}

	h.mu.Lock()
	err := h.err
	h.mu.Unlock()
	return err
}

// Err returns the terminal loop error: wrapped sender error, context
// cancellation/deadline error, or nil after Stop.
func (h *WatchdogHeartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// Stop terminates the heartbeat loop and releases its timer. It waits for a
// running loop to exit. Calling Stop repeatedly, before Start/Run, or on a
// disabled heartbeat is safe and does not panic or deadlock.
func (h *WatchdogHeartbeat) Stop() {
	if h == nil {
		return
	}

	h.mu.Lock()
	switch h.state {
	case heartbeatStopped:
		h.mu.Unlock()
		return
	case heartbeatNew:
		if h.stopCh == nil {
			h.stopCh = make(chan struct{})
		}
		close(h.stopCh)
		h.state = heartbeatStopped
		h.mu.Unlock()
		return
	case heartbeatRunning, heartbeatStopping:
		if h.state == heartbeatRunning {
			close(h.stopCh)
			h.state = heartbeatStopping
		}
		done := h.doneCh
		h.mu.Unlock()
		<-done
	}
}

// Close implements io.Closer. It has the same idempotent behavior as Stop.
func (h *WatchdogHeartbeat) Close() error {
	h.Stop()
	return nil
}

func (h *WatchdogHeartbeat) begin() (bool, error) {
	if h == nil || !h.Enabled() {
		return false, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.state {
	case heartbeatStopped:
		return false, ErrWatchdogStopped
	case heartbeatRunning, heartbeatStopping:
		return false, ErrWatchdogAlreadyRunning
	}

	if h.stopCh == nil {
		h.stopCh = make(chan struct{})
	}
	if h.clock == nil {
		h.clock = realHeartbeatClock{}
	}
	h.doneCh = make(chan struct{})
	h.state = heartbeatRunning
	return true, nil
}

func (h *WatchdogHeartbeat) finish(err error) {
	h.mu.Lock()
	h.state = heartbeatStopped
	h.err = err
	if h.doneCh != nil {
		close(h.doneCh)
	}
	h.mu.Unlock()
}

func (h *WatchdogHeartbeat) loop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		// Create a fresh timer on every iteration. After suspend/resume or a
		// clock adjustment this establishes a new deadline from the time the
		// previous cycle completed; stale ticks are never replayed.
		timer := h.clock.NewTimer(h.interval)

		select {
		case <-h.stopCh:
			stopHeartbeatTimer(timer)
			return nil
		case <-ctx.Done():
			stopHeartbeatTimer(timer)
			return ctx.Err()
		case <-timer.C():
		}

		// Stop and timer may become ready together. Prefer stop so Stop's
		// guarantee also covers a heartbeat whose deadline just fired.
		select {
		case <-h.stopCh:
			return nil
		default:
		}

		if err := h.send(); err != nil {
			return fmt.Errorf("daemon: watchdog heartbeat failed: %w", err)
		}
	}
}

func stopHeartbeatTimer(timer heartbeatTimer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		// The timer may already have fired. Drain its one-slot channel so the
		// stale event cannot be observed if the implementation is reused. The
		// current loop does not reuse timers, but draining keeps the contract
		// explicit.
		select {
		case <-timer.C():
		default:
		}
	}
}

func sendSystemdWatchdog() error {
	ok, err := SdNotify(false, SdNotifyWatchdog)
	if err != nil {
		return err
	}
	if !ok {
		return ErrWatchdogNotifyUnavailable
	}
	return nil
}

// heartbeatTimer is the subset of *time.Timer used by the loop.
type heartbeatTimer interface {
	C() <-chan time.Time
	Stop() bool
}

// heartbeatClock abstracts timer creation for deterministic tests. Every wait
// creates a fresh timer; implementations therefore have no periodic backlog.
type heartbeatClock interface {
	NewTimer(time.Duration) heartbeatTimer
}

type realHeartbeatClock struct{}

func (realHeartbeatClock) NewTimer(d time.Duration) heartbeatTimer {
	return realHeartbeatTimer{Timer: time.NewTimer(d)}
}

type realHeartbeatTimer struct {
	*time.Timer
}

func (t realHeartbeatTimer) C() <-chan time.Time { return t.Timer.C }
