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

import "sync"

// namedEvent is implemented by the events carried by an eventBus. The event's
// unit name is used to keep per-unit ordering and to apply per-unit filters.
type namedEvent interface {
	eventUnit() string
}

// ring is a bounded FIFO buffer of events with optional per-unit coalescing.
// Push never blocks: when the buffer is full the oldest event is dropped and
// the drop is counted. When coalescing is enabled, a new event for a unit that
// already has a pending event replaces it in place, so an older state can never
// be delivered after a newer one for the same unit.
//
// ring is safe for concurrent use between producing goroutines (Push) and the
// owning subscription's single delivery goroutine (Pop).
type ring[T namedEvent] struct {
	mu        sync.Mutex
	cond      *sync.Cond
	events    []T
	head      int // index of the oldest event
	size      int // number of buffered events
	coalesce  bool
	closed    bool
	dropCount uint64
}

func newRing[T namedEvent](capacity int, coalesce bool) *ring[T] {
	if capacity < 1 {
		capacity = 1
	}
	r := &ring[T]{
		events:   make([]T, capacity),
		coalesce: coalesce,
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Push appends event. When coalescing is enabled and an event for the same unit
// is already buffered, event replaces it in place and no drop is counted.
// Otherwise, if the buffer is full, the oldest event is evicted and one drop
// is counted. Push always returns promptly and never blocks on a consumer.
func (r *ring[T]) Push(event T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	if r.coalesce {
		unit := event.eventUnit()
		for i := range r.size {
			idx := (r.head + i) % len(r.events)
			if r.events[idx].eventUnit() == unit {
				// Newer state wins: replace in place, preserving the
				// event's position in the global delivery order.
				r.events[idx] = event
				return
			}
		}
	}

	if r.size == len(r.events) {
		// Buffer full: discard the oldest event and reuse its slot.
		r.events[r.head] = *new(T)
		r.head = (r.head + 1) % len(r.events)
		r.size--
		r.dropCount++
	}

	idx := (r.head + r.size) % len(r.events)
	r.events[idx] = event
	r.size++
	r.cond.Signal()
}

// Pop removes and returns the oldest event. If the buffer is empty it blocks
// until an event is pushed or the ring is closed. It returns false when the
// ring is closed; any events still buffered at that point are discarded (see
// Close), so Pop never reports data after shutdown.
func (r *ring[T]) Pop() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.size == 0 && !r.closed {
		r.cond.Wait()
	}

	if r.closed {
		return *new(T), false
	}

	event := r.events[r.head]
	r.events[r.head] = *new(T)
	r.head = (r.head + 1) % len(r.events)
	r.size--
	return event, true
}

// Close wakes any goroutine blocked in Pop, discards buffered events and
// causes further Pushes to be discarded. It is idempotent.
func (r *ring[T]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	// An unsubscribed subscriber must not receive buffered events, so the
	// pending contents are dropped rather than drained.
	r.events = make([]T, len(r.events))
	r.head = 0
	r.size = 0
	r.cond.Broadcast()
}

// drops returns and resets the number of events evicted since the last call.
func (r *ring[T]) drops() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	dropped := r.dropCount
	r.dropCount = 0
	return dropped
}
