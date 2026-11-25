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
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClampWatchdogInterval(t *testing.T) {
	tests := []struct {
		name             string
		interval         time.Duration
		minimum, maximum time.Duration
		want             time.Duration
	}{
		{"within bounds", 5 * time.Second, time.Second, time.Minute, 5 * time.Second},
		{"below minimum", time.Millisecond, time.Second, time.Minute, time.Second},
		{"above maximum", time.Hour, time.Second, time.Minute, time.Minute},
		{"zero bounds ignored", time.Hour, 0, 0, time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClampWatchdogInterval(tt.interval, tt.minimum, tt.maximum); got != tt.want {
				t.Fatalf("ClampWatchdogInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewWatchdogHeartbeatEnvironment(t *testing.T) {
	matchingPID := strconv.Itoa(os.Getpid())

	tests := []struct {
		name         string
		usec         string
		pid          string
		wantEnabled  bool
		wantInterval time.Duration
		wantErr      bool
	}{
		{
			name:         "half interval",
			usec:         "10000000",
			pid:          matchingPID,
			wantEnabled:  true,
			wantInterval: 5 * time.Second,
		},
		{
			name:         "minimum clamp",
			usec:         "100",
			pid:          matchingPID,
			wantEnabled:  true,
			wantInterval: DefaultMinWatchdogInterval,
		},
		{
			name:         "maximum clamp",
			usec:         strconv.FormatInt(int64(10*time.Minute/time.Microsecond), 10),
			pid:          matchingPID,
			wantEnabled:  true,
			wantInterval: DefaultMaxWatchdogInterval,
		},
		{
			name: "missing usec returns zero",
			pid:  matchingPID,
		},
		{
			name: "pid mismatch returns zero",
			usec: "10000000",
			pid:  "0",
		},
		{
			name:    "invalid usec",
			usec:    "later",
			pid:     matchingPID,
			wantErr: true,
		},
		{
			name:    "invalid pid",
			usec:    "10000000",
			pid:     "not-a-pid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WATCHDOG_USEC", tt.usec)
			t.Setenv("WATCHDOG_PID", tt.pid)

			hb, err := NewWatchdogHeartbeat(false,
				WithHeartbeatSender(func() error { return nil }),
			)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewWatchdogHeartbeat() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewWatchdogHeartbeat() error = %v", err)
			}
			if hb.Enabled() != tt.wantEnabled {
				t.Fatalf("Enabled() = %v, want %v", hb.Enabled(), tt.wantEnabled)
			}
			if got := hb.Interval(); got != tt.wantInterval {
				t.Fatalf("Interval() = %v, want %v", got, tt.wantInterval)
			}
		})
	}
}

func TestNewWatchdogHeartbeatCustomBounds(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		minimum time.Duration
		maximum time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "minimum", timeout: 10 * time.Millisecond, minimum: 100 * time.Millisecond, maximum: time.Second, want: 100 * time.Millisecond},
		{name: "maximum", timeout: time.Second, minimum: time.Millisecond, maximum: 400 * time.Millisecond, want: 400 * time.Millisecond},
		{name: "within", timeout: 10 * time.Second, minimum: time.Second, maximum: time.Minute, want: 5 * time.Second},
		{name: "invalid", minimum: time.Minute, maximum: time.Second, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hb, err := NewWatchdogHeartbeat(false,
				WithWatchdogTimeout(tt.timeout),
				WithWatchdogIntervalBounds(tt.minimum, tt.maximum),
				WithHeartbeatSender(func() error { return nil }),
			)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewWatchdogHeartbeat() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewWatchdogHeartbeat() error = %v", err)
			}
			if hb.Interval() != tt.want {
				t.Fatalf("Interval() = %v, want %v", hb.Interval(), tt.want)
			}
		})
	}
}

func TestWithWatchdogTimeoutZeroDoesNotReadEnvironment(t *testing.T) {
	t.Setenv("WATCHDOG_USEC", "not-a-duration")
	t.Setenv("WATCHDOG_PID", "not-a-pid")

	hb, err := NewWatchdogHeartbeat(false, WithWatchdogTimeout(0))
	if err != nil {
		t.Fatalf("NewWatchdogHeartbeat() error = %v", err)
	}
	if hb.Enabled() || hb.Interval() != 0 {
		t.Fatalf("heartbeat = enabled:%v interval:%v, want disabled zero", hb.Enabled(), hb.Interval())
	}
}

func TestSdWatchdogHeartbeatIntervalDisabledZero(t *testing.T) {
	t.Setenv("WATCHDOG_USEC", "")
	t.Setenv("WATCHDOG_PID", "")

	interval, err := SdWatchdogHeartbeatInterval(false)
	if err != nil {
		t.Fatalf("SdWatchdogHeartbeatInterval() error = %v", err)
	}
	if interval != 0 {
		t.Fatalf("interval = %v, want zero", interval)
	}
}

func TestWatchdogHeartbeatSendFailureStops(t *testing.T) {
	clock := newFakeHeartbeatClock()
	var calls atomic.Int32
	sendErr := errors.New("send failed")
	hb := newTestHeartbeat(time.Second, clock, func() error {
		calls.Add(1)
		return sendErr
	})

	runErr := make(chan error, 1)
	go func() {
		runErr <- hb.Run(context.Background())
	}()

	timer := clock.waitForTimerCount(t, 1)[0]
	timer.fire()

	select {
	case err := <-runErr:
		if !errors.Is(err, sendErr) {
			t.Fatalf("Run() error = %v, want %v", err, sendErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after send failure")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("send calls = %d, want exactly 1", got)
	}
	if timers := clock.waitForTimerCount(t, 1); len(timers) != 1 {
		t.Fatalf("timer count = %d, want no replacement after failure", len(timers))
	}

	// Stopping after the terminal error must remain safe and idempotent.
	hb.Stop()
	hb.Stop()
}

func TestWatchdogHeartbeatStartReportsSendFailure(t *testing.T) {
	clock := newFakeHeartbeatClock()
	sendErr := errors.New("background send failed")
	hb := newTestHeartbeat(time.Second, clock, func() error { return sendErr })

	if err := hb.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	clock.waitForTimerCount(t, 1)[0].fire()

	waitEventually(t, func() bool { return hb.Err() != nil }, "background terminal error")
	if err := hb.Wait(); !errors.Is(err, sendErr) {
		t.Fatalf("Wait() error = %v, want %v", err, sendErr)
	}
	hb.Stop()
}

func TestWatchdogHeartbeatConcurrentStopIdempotent(t *testing.T) {
	clock := newFakeHeartbeatClock()
	hb := newTestHeartbeat(10*time.Second, clock, func() error { return nil })

	runErr := make(chan error, 1)
	go func() { runErr <- hb.Run(context.Background()) }()
	clock.waitForTimerCount(t, 1)

	var stoppers sync.WaitGroup
	for range 16 {
		stoppers.Add(1)
		go func() {
			defer stoppers.Done()
			hb.Stop()
		}()
	}
	stoppers.Wait()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Stop did not terminate Run")
	}
}

func TestWatchdogHeartbeatStopIdempotentAndReleasesTimer(t *testing.T) {
	clock := newFakeHeartbeatClock()
	var calls atomic.Int32
	hb := newTestHeartbeat(10*time.Second, clock, func() error {
		calls.Add(1)
		return nil
	})

	runErr := make(chan error, 1)
	go func() {
		runErr <- hb.Run(context.Background())
	}()

	timer := clock.waitForTimerCount(t, 1)[0]
	hb.Stop()
	hb.Stop()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() after Stop error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not terminate Run")
	}
	if !timer.wasStopped() {
		t.Fatal("Stop did not stop the active timer")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("send calls = %d, want 0", got)
	}
}

func TestWatchdogHeartbeatStopBeforeRun(t *testing.T) {
	clock := newFakeHeartbeatClock()
	hb := newTestHeartbeat(time.Second, clock, func() error { return nil })

	hb.Stop()
	hb.Stop()

	if err := hb.Run(context.Background()); !errors.Is(err, ErrWatchdogStopped) {
		t.Fatalf("Run() after Stop error = %v, want %v", err, ErrWatchdogStopped)
	}
}

func TestWatchdogHeartbeatContextCancelReleasesTimer(t *testing.T) {
	clock := newFakeHeartbeatClock()
	hb := newTestHeartbeat(10*time.Second, clock, func() error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- hb.Run(ctx)
	}()

	timer := clock.waitForTimerCount(t, 1)[0]
	cancel()

	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not terminate Run")
	}
	if !timer.wasStopped() {
		t.Fatal("context cancellation did not stop the active timer")
	}
	hb.Stop()
}

func TestWatchdogHeartbeatDoesNotReplayMissedBeats(t *testing.T) {
	clock := newFakeHeartbeatClock()
	sent := make(chan struct{}, 8)
	hb := newTestHeartbeat(10*time.Second, clock, func() error {
		sent <- struct{}{}
		return nil
	})

	runErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		runErr <- hb.Run(ctx)
	}()

	first := clock.waitForTimerCount(t, 1)[0]
	if first.duration != 10*time.Second {
		t.Fatalf("first timer duration = %v, want 10s", first.duration)
	}

	// Simulate a very long suspend and wall-clock adjustment before the timer
	// deadline is handled. There must be no burst of catch-up heartbeats.
	clock.advance(2 * time.Hour)
	first.fire()
	waitSignal(t, sent, "first heartbeat")

	second := clock.waitForTimerCount(t, 2)[1]
	if second.duration != 10*time.Second {
		t.Fatalf("second timer duration = %v, want a fresh 10s interval", second.duration)
	}

	// Jump again, then deliver only one expiration.
	clock.advance(-time.Hour)
	second.fire()
	waitSignal(t, sent, "second heartbeat")

	select {
	case <-sent:
		t.Fatal("unexpected catch-up heartbeat")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not stop heartbeat")
	}
}

func TestWatchdogHeartbeatStartWaitAndStop(t *testing.T) {
	clock := newFakeHeartbeatClock()
	var calls atomic.Int32
	hb := newTestHeartbeat(time.Second, clock, func() error {
		calls.Add(1)
		return nil
	})

	if err := hb.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	timer := clock.waitForTimerCount(t, 1)[0]
	timer.fire()
	waitEventually(t, func() bool { return calls.Load() == 1 }, "one background heartbeat")

	hb.Stop()
	if err := hb.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if err := hb.Start(t.Context()); !errors.Is(err, ErrWatchdogStopped) {
		t.Fatalf("second Start() after Stop error = %v, want %v", err, ErrWatchdogStopped)
	}
}

func TestDisabledWatchdogHeartbeatZeroValue(t *testing.T) {
	var hb WatchdogHeartbeat
	if hb.Enabled() {
		t.Fatal("zero WatchdogHeartbeat should be disabled")
	}
	if hb.Interval() != 0 {
		t.Fatalf("Interval() = %v, want zero", hb.Interval())
	}
	if err := hb.Run(context.Background()); err != nil {
		t.Fatalf("zero value Run() error = %v, want nil", err)
	}
	if err := hb.Start(context.Background()); err != nil {
		t.Fatalf("zero value Start() error = %v, want nil", err)
	}
	if err := hb.Wait(); err != nil {
		t.Fatalf("zero value Wait() error = %v, want nil", err)
	}
	hb.Stop()
}

func newTestHeartbeat(interval time.Duration, clock heartbeatClock, send HeartbeatSender) *WatchdogHeartbeat {
	return &WatchdogHeartbeat{
		interval: interval,
		send:     send,
		clock:    clock,
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitEventually(t *testing.T, condition func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type fakeHeartbeatClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeHeartbeatTimer
}

func newFakeHeartbeatClock() *fakeHeartbeatClock {
	return &fakeHeartbeatClock{now: time.Unix(100, 0)}
}

func (c *fakeHeartbeatClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeHeartbeatClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeHeartbeatClock) NewTimer(d time.Duration) heartbeatTimer {
	timer := &fakeHeartbeatTimer{
		clock:    c,
		duration: d,
		ch:       make(chan time.Time, 1),
	}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	return timer
}

func (c *fakeHeartbeatClock) waitForTimerCount(t *testing.T, count int) []*fakeHeartbeatTimer {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		timers := append([]*fakeHeartbeatTimer(nil), c.timers...)
		c.mu.Unlock()
		if len(timers) >= count {
			return timers
		}
		time.Sleep(time.Millisecond)
	}
	c.mu.Lock()
	got := len(c.timers)
	c.mu.Unlock()
	t.Fatalf("timed out waiting for %d timers, got %d", count, got)
	return nil
}

type fakeHeartbeatTimer struct {
	clock    *fakeHeartbeatClock
	duration time.Duration
	ch       chan time.Time

	mu      sync.Mutex
	stopped bool
	fired   bool
}

func (t *fakeHeartbeatTimer) C() <-chan time.Time { return t.ch }

func (t *fakeHeartbeatTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (t *fakeHeartbeatTimer) wasStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (t *fakeHeartbeatTimer) fire() {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return
	}
	t.fired = true
	t.mu.Unlock()

	select {
	case t.ch <- t.clock.Now():
	default:
		panic("fake heartbeat timer channel already has an event")
	}
}
