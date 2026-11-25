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
	"log"
	"sync"
)

// DropPolicy decides what happens when a subscriber's event buffer is full.
type DropPolicy int

const (
	// DropOldest evicts the oldest buffered event to make room for a new one.
	// Per-unit coalescing (see SubscribeOptions) still applies first, so a
	// newer state for the same unit always replaces the pending older one.
	DropOldest DropPolicy = iota
)

// DefaultSubscriptionBuffer is the capacity used for a subscription when
// SubscribeOptions.Buffer is left at zero.
const DefaultSubscriptionBuffer = 64

// MaxIgnoreEntries bounds the number of entries in the spurious-signal ignore
// list; see the ignoreList discussion and updateIgnore in subscription.go.
const MaxIgnoreEntries = 4096

// SubscribeOptions configures a unit event subscription. The zero value is
// valid and selects the documented defaults.
type SubscribeOptions struct {
	// Buffer is the maximum number of events buffered for the subscriber
	// before the DropPolicy kicks in. Zero means DefaultSubscriptionBuffer.
	Buffer int
	// Policy selects how overflow is handled. Only DropOldest is supported.
	Policy DropPolicy
	// Coalesce, when true, replaces a pending event for the same unit with the
	// newer one instead of buffering a second event, guaranteeing that an old
	// state can never overwrite a newer state for that unit.
	Coalesce bool
	// FilterUnit, when non-nil, only delivers events for units for which it
	// returns true. It never blocks event dispatch: it is evaluated inline on
	// the non-blocking signal path, so it must be cheap.
	FilterUnit func(unit string) bool
}

// DroppedEventsError is reported to a subscriber when one or more events had
// to be discarded because the subscriber was not consuming fast enough. The
// events are dropped on behalf of that subscriber only; other subscribers are
// unaffected and the D-Bus dispatch path is never blocked.
type DroppedEventsError struct {
	// Count is the number of events discarded since the previous report.
	Count uint64
}

func (e *DroppedEventsError) Error() string {
	return "dropped unit events due to slow consumer"
}

// UnitSubscription is a handle to one subscriber's event stream. Events are
// delivered on the channel returned by Events; errors (including drop reports)
// are delivered on the channel returned by Err. Both channels are closed when
// the subscription ends, either via Close, context cancellation, or Conn.Close.
type UnitSubscription[T namedEvent] struct {
	// eventsOut/errsOut are the caller-facing read-only views; events/errs
	// are the send sides used by the delivery goroutine. For non-legacy
	// subscriptions closeEvents/closeErrs are the owned bidirectional
	// channels that get closed on shutdown; legacy adapters leave them nil
	// because the caller owns the channels.
	eventsOut   <-chan T
	errsOut     <-chan error
	events      chan<- T
	errs        chan<- error
	closeEvents chan T
	closeErrs   chan error

	ring      *ring[T]
	opts      SubscribeOptions
	legacy    bool // legacy adapters own their channels, so don't close them
	closeOnce sync.Once
	done      chan struct{}
}

// Events returns the channel on which unit events are delivered.
func (s *UnitSubscription[T]) Events() <-chan T { return s.eventsOut }

// Err returns the channel on which errors, including DroppedEventsError
// notifications, are delivered.
func (s *UnitSubscription[T]) Err() <-chan error { return s.errsOut }

// Dropped returns the number of events discarded for this subscriber since the
// previous call. It is an alternative to consuming DroppedEventsError values
// from the error channel.
func (s *UnitSubscription[T]) Dropped() uint64 { return s.ring.drops() }

// Close terminates the subscription. It is idempotent: calling Close more than
// once, or after the connection has closed, is safe. No further events are
// delivered after Close returns; in-flight buffered events are discarded.
func (s *UnitSubscription[T]) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.ring.Close()
	})
}

// deliver pushes one event into the subscription's bounded buffer, honoring its
// filter. It is called from the shared publish path and must never block.
func (s *UnitSubscription[T]) deliver(event T) {
	if s.opts.FilterUnit != nil && !s.opts.FilterUnit(event.eventUnit()) {
		return
	}
	s.ring.Push(event)
}

// reportDrops flushes the ring's drop counter to the error channel without
// blocking; a full error channel is logged and the count is still reset.
func (s *UnitSubscription[T]) reportDrops() {
	dropped := s.ring.drops()
	if dropped == 0 {
		return
	}
	select {
	case s.errs <- &DroppedEventsError{Count: dropped}:
	default:
		log.Printf("full error channel while reporting %d dropped unit events\n", dropped)
	}
}

// run is the subscription's delivery goroutine. It moves events from the
// bounded ring to the caller-facing channel. A slow consumer only fills its
// own ring (where the drop policy applies); it cannot stall any other
// subscriber or the signal dispatch goroutine. It returns when the ring is
// shut down (Close, context cancel, or bus close); startWorkers removes the
// subscription from the bus before closing its channels.
func (s *UnitSubscription[T]) run(ctx context.Context) {
	for {
		event, ok := s.ring.Pop()
		if !ok {
			return
		}

		// Report any drops accumulated while this event was buffered.
		s.reportDrops()

		select {
		case s.events <- event:
		case <-ctx.Done():
			return
		case <-s.done:
			return
		}
	}
}

// finish closes the caller-facing channels. It must run only after the
// subscription has been removed from the bus, otherwise a concurrent publish
// could send on an about-to-be-closed error channel.
func (s *UnitSubscription[T]) finish() {
	// Flush any final drop count before closing the error channel.
	s.reportDrops()
	if s.legacy {
		return
	}
	close(s.closeEvents)
	close(s.closeErrs)
}

// eventBus is a fan-out point for unit events. publish is called by the signal
// dispatch goroutine and must be non-blocking: every subscriber accepts events
// into its own bounded buffer or drops them locally.
type eventBus[T namedEvent] struct {
	mu     sync.RWMutex
	subs   map[*UnitSubscription[T]]struct{}
	closed bool
}

func newEventBus[T namedEvent]() *eventBus[T] {
	return &eventBus[T]{subs: make(map[*UnitSubscription[T]]struct{})}
}

var errConnClosed = errors.New("dbus connection is closed")

// subscribe registers a new subscriber and starts its delivery goroutine. The
// returned subscription is removed from the bus when it ends.
func (b *eventBus[T]) subscribe(ctx context.Context, opts SubscribeOptions, wg *sync.WaitGroup) (*UnitSubscription[T], error) {
	if opts.Buffer <= 0 {
		opts.Buffer = DefaultSubscriptionBuffer
	}

	eventsCh := make(chan T, opts.Buffer)
	errsCh := make(chan error, opts.Buffer)
	sub := &UnitSubscription[T]{
		eventsOut:   eventsCh,
		errsOut:     errsCh,
		events:      eventsCh,
		errs:        errsCh,
		closeEvents: eventsCh,
		closeErrs:   errsCh,
		ring:        newRing[T](opts.Buffer, opts.Coalesce),
		opts:        opts,
		done:        make(chan struct{}),
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errConnClosed
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	b.startWorkers(ctx, sub, wg)
	return sub, nil
}

// startWorkers launches the delivery goroutine and the context watcher for a
// subscription. Both are tracked by wg so Close can wait on a clean shutdown.
func (b *eventBus[T]) startWorkers(ctx context.Context, sub *UnitSubscription[T], wg *sync.WaitGroup) {
	wg.Add(2)
	go func() {
		defer wg.Done()
		sub.run(ctx)
		// Detach first so no further publish can reach the subscription,
		// then close its channels. Closing while still registered would race
		// a publishErr send on the error channel.
		b.remove(sub)
		sub.finish()
	}()

	// An externally canceled context tears down the subscription; otherwise
	// the watcher exits when the subscription ends on its own.
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			sub.Close()
		case <-sub.done:
		}
	}()
}

// subscribeLegacy adapts a caller-owned channel pair to the bus. The caller
// keeps ownership of the channels (they are not closed here), preserving the
// historical SetSubStateSubscriber/SetPropertiesSubscriber contract.
func (b *eventBus[T]) subscribeLegacy(ctx context.Context, updateCh chan<- T, errCh chan<- error, coalesce bool, wg *sync.WaitGroup) (*UnitSubscription[T], error) {
	sub := &UnitSubscription[T]{
		events: updateCh,
		errs:   errCh,
		ring:   newRing[T](1, coalesce),
		opts:   SubscribeOptions{Buffer: 1, Policy: DropOldest, Coalesce: coalesce},
		legacy: true,
		done:   make(chan struct{}),
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errConnClosed
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	b.startWorkers(ctx, sub, wg)
	return sub, nil
}

func (b *eventBus[T]) remove(sub *UnitSubscription[T]) {
	b.mu.Lock()
	delete(b.subs, sub)
	b.mu.Unlock()
}

// publish fans one event out to every current subscriber. It never blocks:
// each subscriber's ring either buffers the event or drops its oldest event
// locally.
func (b *eventBus[T]) publish(event T) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for sub := range b.subs {
		sub.deliver(event)
	}
}

// publishErr delivers an error to every current subscriber's error channel
// without blocking. It is used for failures that are not tied to a single
// subscriber (for example a property RPC failure on the shared fetch path).
func (b *eventBus[T]) publishErr(err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for sub := range b.subs {
		select {
		case sub.errs <- err:
		default:
			log.Printf("full error channel while reporting: %s\n", err)
		}
	}
}

// close terminates all current subscriptions and rejects future ones. It is
// idempotent.
func (b *eventBus[T]) close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subs := make([]*UnitSubscription[T], 0, len(b.subs))
	for sub := range b.subs {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		sub.Close()
	}
}
