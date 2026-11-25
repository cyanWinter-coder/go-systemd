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

package dbus

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
)

// testEvent is a namedEvent used to exercise the generic event bus without
// needing a live D-Bus connection.
type testEvent struct {
	unit  string
	value int
}

func (e *testEvent) eventUnit() string { return e.unit }

func newTestBus(t *testing.T) (*eventBus[*testEvent], *sync.WaitGroup) {
	t.Helper()
	bus := newEventBus[*testEvent]()
	var wg sync.WaitGroup
	t.Cleanup(func() {
		// Close first so a fatal assertion elsewhere still shuts everything
		// down; every goroutine started by the bus must exit afterwards.
		bus.close()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("event bus goroutines did not exit after close")
		}
	})
	return bus, &wg
}

func waitFor(t *testing.T, ch <-chan *testEvent) *testEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

// TestRingFIFOAndDropOldest checks that a full ring evicts the oldest event,
// counts the drop, and keeps FIFO delivery order.
func TestRingFIFOAndDropOldest(t *testing.T) {
	r := newRing[*testEvent](2, false)

	r.Push(&testEvent{unit: "a.service", value: 1})
	r.Push(&testEvent{unit: "b.service", value: 2})
	r.Push(&testEvent{unit: "c.service", value: 3}) // drops a.service

	if got := r.drops(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}

	first, ok := r.Pop()
	if !ok || first.value != 2 {
		t.Fatalf("first = %v ok=%v, want value 2", first, ok)
	}
	second, ok := r.Pop()
	if !ok || second.value != 3 {
		t.Fatalf("second = %v ok=%v, want value 3", second, ok)
	}
}

// TestRingCoalesceNewestWins checks that a newer event for the same unit
// replaces the pending older one in place, with no drop counted and global
// delivery order preserved.
func TestRingCoalesceNewestWins(t *testing.T) {
	r := newRing[*testEvent](4, true)

	r.Push(&testEvent{unit: "a.service", value: 1})
	r.Push(&testEvent{unit: "b.service", value: 2})
	r.Push(&testEvent{unit: "a.service", value: 3}) // replaces a in place

	if got := r.drops(); got != 0 {
		t.Fatalf("drops = %d, want 0 for coalesce", got)
	}

	first, _ := r.Pop()
	if first.unit != "a.service" || first.value != 3 {
		t.Fatalf("coalesced event = %+v, want a.service/3", first)
	}
	second, _ := r.Pop()
	if second.unit != "b.service" || second.value != 2 {
		t.Fatalf("second event = %+v, want b.service/2", second)
	}
}

// TestRingCloseDiscardsAndWakes checks that after Close, Pop reports shutdown
// immediately (buffered events are discarded, since an unsubscribed consumer
// must not receive them) and that Close is idempotent.
func TestRingCloseDiscardsAndWakes(t *testing.T) {
	r := newRing[*testEvent](2, false)
	r.Push(&testEvent{unit: "a.service"})
	r.Close()
	r.Close() // idempotent

	if _, ok := r.Pop(); ok {
		t.Fatal("Pop must return false after Close even with buffered events")
	}
	// Pushes after close are discarded.
	r.Push(&testEvent{unit: "a.service"})
	if r.drops() != 0 {
		t.Fatal("post-close pushes must not count as drops")
	}
}

// TestEventBusMultiSubscriberFanOut checks that one published event reaches
// every independent subscriber.
func TestEventBusMultiSubscriberFanOut(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx := context.Background()

	sub1, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 4}, wg)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 4}, wg)
	if err != nil {
		t.Fatal(err)
	}

	bus.publish(&testEvent{unit: "a.service", value: 7})

	if ev := waitFor(t, sub1.Events()); ev.value != 7 {
		t.Fatalf("sub1 got %d, want 7", ev.value)
	}
	if ev := waitFor(t, sub2.Events()); ev.value != 7 {
		t.Fatalf("sub2 got %d, want 7", ev.value)
	}

	bus.close()
}

// TestEventBusSlowConsumerDropsWithoutBlocking proves that a subscriber that
// stops reading does not block publish (the dispatch path) nor affect another
// subscriber, and that its drops are counted and reported.
func TestEventBusSlowConsumerDropsWithoutBlocking(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx := context.Background()

	slow, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 2}, wg)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 32}, wg)
	if err != nil {
		t.Fatal(err)
	}

	const published = 20

	// Collect everything the fast subscriber receives concurrently, so its
	// delivery goroutine is never blocked.
	var fastMu sync.Mutex
	fastValues := make([]int, 0, published)
	fastCount := func() int {
		fastMu.Lock()
		defer fastMu.Unlock()
		return len(fastValues)
	}
	fastErrs := make(chan error, 8)
	var fastWg sync.WaitGroup
	fastWg.Add(1)
	go func() {
		defer fastWg.Done()
		for {
			select {
			case ev, ok := <-fast.Events():
				if !ok {
					return
				}
				fastMu.Lock()
				fastValues = append(fastValues, ev.value)
				fastMu.Unlock()
			case err := <-fast.Err():
				fastErrs <- err
			}
		}
	}()

	// Publish far more than the slow subscriber can hold. Publish must return
	// promptly every time.
	done := make(chan struct{})
	go func() {
		for i := range published {
			bus.publish(&testEvent{unit: "a.service", value: i})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a slow consumer")
	}

	// The fast subscriber receives the full stream in order with no drops.
	waitUntil(t, func() bool { return fastCount() == published })
	fastMu.Lock()
	got := append([]int(nil), fastValues...)
	fastMu.Unlock()
	for i, v := range got {
		if v != i {
			t.Fatalf("fast subscriber out of order at %d: got %d, want %d", i, v, i)
		}
	}
	select {
	case err := <-fastErrs:
		t.Fatalf("fast subscriber unexpectedly got error: %v", err)
	default:
	}

	// Closing the bus ends every subscription and lets the collectors exit.
	bus.close()
	fastWg.Wait()

	// Drain the slow subscriber until it closes and tally what was reported.
	// Drops still pending when the bus closes are discarded with the buffer,
	// so assert the accounting invariant rather than an exact total.
	var dropped uint64
	gotEvents := 0
	eventsOpen, errsOpen := true, true
	deadline := time.After(2 * time.Second)
	for eventsOpen || errsOpen {
		select {
		case _, ok := <-slow.Events():
			if !ok {
				eventsOpen = false
				continue
			}
			gotEvents++
		case err, ok := <-slow.Err():
			if !ok {
				errsOpen = false
				continue
			}
			var de *DroppedEventsError
			if errors.As(err, &de) {
				dropped += de.Count
			}
		case <-deadline:
			t.Fatal("slow subscriber never finished after bus close")
		}
	}
	if dropped == 0 {
		t.Fatal("slow subscriber should have reported at least one drop")
	}
	if gotEvents+int(dropped) > published {
		t.Fatalf("delivered %d + dropped %d exceeds %d published", gotEvents, dropped, published)
	}
	if gotEvents >= published {
		t.Fatalf("slow subscriber delivered %d of %d; it should have dropped some", gotEvents, published)
	}
}

// waitUntil polls cond until it becomes true or the test deadline elapses.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("condition not met before deadline")
		}
	}
}

// TestEventBusUnsubscribeStopsDelivery checks that Close on a subscription is
// idempotent and no further events are delivered to it afterwards.
func TestEventBusUnsubscribeStopsDelivery(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx := context.Background()

	sub, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 4}, wg)
	if err != nil {
		t.Fatal(err)
	}

	sub.Close()
	sub.Close() // idempotent

	// Channels must be closed by the delivery goroutine.
	deadline := time.After(2 * time.Second)
	eventsClosed, errsClosed := false, false
	for !eventsClosed || !errsClosed {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				eventsClosed = true
			}
		case _, ok := <-sub.Err():
			if !ok {
				errsClosed = true
			}
		case <-deadline:
			t.Fatal("subscription channels were not closed after unsubscribe")
		}
	}

	// Publishing after unsubscribe must not deliver or panic.
	bus.publish(&testEvent{unit: "a.service"})

	bus.mu.RLock()
	if len(bus.subs) != 0 {
		t.Fatalf("subs = %d, want 0 after unsubscribe", len(bus.subs))
	}
	bus.mu.RUnlock()

	bus.close()
}

// TestEventBusContextCancelUnsubscribes checks that canceling the context ends
// the subscription and closes its channels.
func TestEventBusContextCancelUnsubscribes(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx, cancel := context.WithCancel(context.Background())

	sub, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 4}, wg)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("events channel should be closed on context cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events channel not closed after context cancel")
	}

	bus.close()
}

// TestEventBusFilterOnlyAffectsMatchingUnit checks that a filter only suppresses
// non-matching units for its own subscription.
func TestEventBusFilterOnlyAffectsMatchingUnit(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx := context.Background()

	filtered, err := bus.subscribe(ctx, SubscribeOptions{
		Buffer:     8,
		FilterUnit: func(unit string) bool { return unit == "keep.service" },
	}, wg)
	if err != nil {
		t.Fatal(err)
	}
	all, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 8}, wg)
	if err != nil {
		t.Fatal(err)
	}

	bus.publish(&testEvent{unit: "drop.service"})
	bus.publish(&testEvent{unit: "keep.service"})

	if ev := waitFor(t, filtered.Events()); ev.unit != "keep.service" {
		t.Fatalf("filtered sub got %q, want keep.service", ev.unit)
	}
	select {
	case ev := <-filtered.Events():
		t.Fatalf("filtered sub unexpectedly received %q", ev.unit)
	default:
	}

	// The unfiltered subscriber still sees both units.
	first := waitFor(t, all.Events())
	second := waitFor(t, all.Events())
	if first.unit != "drop.service" || second.unit != "keep.service" {
		t.Fatalf("unfiltered sub got %q, %q", first.unit, second.unit)
	}

	bus.close()
}

// TestEventBusRejectsSubscribeAfterClose checks shutdown wakes waiters and no
// new subscriptions are accepted.
func TestEventBusRejectsSubscribeAfterClose(t *testing.T) {
	bus, wg := newTestBus(t)
	bus.close()
	bus.close() // idempotent

	if _, err := bus.subscribe(context.Background(), SubscribeOptions{Buffer: 4}, wg); !errors.Is(err, errConnClosed) {
		t.Fatalf("err = %v, want errConnClosed", err)
	}
}

// TestEventBusConcurrentSubscribePublishClose hammers the bus from many
// goroutines publishing while subscriptions come and go. It mainly exists to
// run under -race and to assert that channel closing never races a send.
func TestEventBusConcurrentSubscribePublishClose(t *testing.T) {
	bus, wg := newTestBus(t)
	ctx := context.Background()

	var producers sync.WaitGroup
	for p := range 4 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := range 200 {
				bus.publish(&testEvent{
					unit:  "unit-" + strconv.Itoa(p%8) + ".service",
					value: i,
				})
				bus.publishErr(errors.New("transient fetch error"))
			}
		}()
	}

	var subscribers sync.WaitGroup
	for range 6 {
		subscribers.Add(1)
		go func() {
			defer subscribers.Done()
			sub, err := bus.subscribe(ctx, SubscribeOptions{Buffer: 4}, wg)
			if err != nil {
				return
			}
			done := make(chan struct{})
			go func() {
				events, errs := sub.Events(), sub.Err()
				for {
					select {
					case _, ok := <-events:
						if !ok {
							events = nil
						}
					case _, ok := <-errs:
						if !ok {
							errs = nil
						}
					case <-done:
						return
					}
					if events == nil && errs == nil {
						return
					}
				}
			}()
			time.Sleep(time.Millisecond)
			close(done)
			sub.Close()
		}()
	}

	subscribers.Wait()
	producers.Wait()
	bus.close()
}

// TestIgnoreListBoundedAndSwept verifies the cap, expiry semantics and periodic
// cleanup of the spurious-signal ignore list.
func TestIgnoreListBoundedAndSwept(t *testing.T) {
	now := int64(0)
	list := newIgnoreList()
	list.now = func() int64 { return now }

	// Expired entries are reported as not ignored.
	list.add("/old", time.Millisecond)
	now = 2 * int64(time.Millisecond)
	if list.ignored("/old") {
		t.Fatal("expired entry should no longer be ignored")
	}

	// Live entries are ignored.
	list.add("/live", 10*time.Millisecond)
	if !list.ignored("/live") {
		t.Fatal("unexpired entry should be ignored")
	}

	// Sweep removes the expired entry.
	list.sweep()
	list.mu.Lock()
	if _, ok := list.entries["/old"]; ok {
		t.Fatal("sweep did not remove expired entry")
	}
	list.mu.Unlock()

	// The list refuses to grow past MaxIgnoreEntries.
	now = 0
	for i := range MaxIgnoreEntries {
		list.add(dbus.ObjectPath("/unit/"+strconv.Itoa(i)), time.Hour)
	}
	list.add("/overflow", time.Hour)
	list.mu.Lock()
	if len(list.entries) > MaxIgnoreEntries {
		t.Fatalf("ignore list grew to %d, cap is %d", len(list.entries), MaxIgnoreEntries)
	}
	if _, ok := list.entries["/overflow"]; ok {
		t.Fatal("overflow insertion should be rejected when the list is full")
	}
	list.mu.Unlock()
}

// TestIgnoreListSweeperGoroutineExits checks the background sweeper terminates
// when its stop channel closes.
func TestIgnoreListSweeperGoroutineExits(t *testing.T) {
	list := newIgnoreList()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	list.run(stop, &wg)
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ignore sweeper did not exit")
	}
}
