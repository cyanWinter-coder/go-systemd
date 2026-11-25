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
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	cleanIgnoreInterval = int64(10 * time.Second)
	ignoreInterval      = int64(30 * time.Millisecond)
)

// Subscribe sets up this connection to subscribe to all systemd dbus events.
// This is required before calling SubscribeUnits. When the connection closes
// systemd will automatically stop sending signals so there is no need to
// explicitly call Unsubscribe().
func (c *Conn) Subscribe() error {
	c.sigconn.BusObject().Call("org.freedesktop.DBus.AddMatch", 0,
		"type='signal',interface='org.freedesktop.systemd1.Manager',member='UnitNew'")
	c.sigconn.BusObject().Call("org.freedesktop.DBus.AddMatch", 0,
		"type='signal',interface='org.freedesktop.DBus.Properties',member='PropertiesChanged'")

	return c.sigobj.Call("org.freedesktop.systemd1.Manager.Subscribe", 0).Store()
}

// Unsubscribe this connection from systemd dbus events.
func (c *Conn) Unsubscribe() error {
	return c.sigobj.Call("org.freedesktop.systemd1.Manager.Unsubscribe", 0).Store()
}

// hasSubscribers reports whether the bus currently has any subscriber.
func (b *eventBus[T]) hasSubscribers() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs) > 0
}

// dispatch consumes the raw D-Bus signal channel. Its only jobs are to
// classify signals and hand work off: property events are published directly
// (non-blocking) and substate fetches are queued to the fetch shards. It never
// performs a blocking RPC or a blocking channel send, so a slow consumer cannot
// stall signal delivery for any other unit. The goroutine exits when the signal
// channel is closed by Conn.Close.
func (c *Conn) dispatch() {
	ch := make(chan *dbus.Signal, signalBuffer)
	c.sigconn.Signal(ch)

	c.closeWG.Add(1)
	go func() {
		defer c.closeWG.Done()
		for signal := range ch {
			c.handleSignal(signal)
		}
	}()
}

func (c *Conn) handleSignal(signal *dbus.Signal) {
	if signal.Name == "org.freedesktop.systemd1.Manager.JobRemoved" {
		c.jobComplete(signal)
	}

	subStateSubs := c.subStateBus.hasSubscribers()
	propertiesSubs := c.propertiesBus.hasSubscribers()
	if !subStateSubs && !propertiesSubs {
		return
	}

	switch signal.Name {
	case "org.freedesktop.systemd1.Manager.JobRemoved":
		// JobRemoved only drives substate refreshes; nothing to do if nobody
		// subscribes to those.
		if !subStateSubs {
			return
		}
		unitName, ok := signal.Body[2].(string)
		if !ok {
			return
		}
		// The path resolution and property fetch happen off the dispatch
		// goroutine in the fetch shards.
		c.enqueueFetch(fetchRequest{unitName: unitName})

	case "org.freedesktop.systemd1.Manager.UnitNew":
		if !subStateSubs {
			return
		}
		unitPath, ok := signal.Body[1].(dbus.ObjectPath)
		if !ok || unitPath == "" {
			return
		}
		c.enqueueFetch(fetchRequest{unitPath: unitPath})

	case "org.freedesktop.DBus.Properties.PropertiesChanged":
		iface, ok := signal.Body[0].(string)
		if !ok || iface != "org.freedesktop.systemd1.Unit" {
			return
		}
		unitPath := signal.Path
		if propertiesSubs && len(signal.Body) >= 2 {
			if changed, ok := signal.Body[1].(map[string]dbus.Variant); ok {
				c.propertiesBus.publish(&PropertiesUpdate{
					UnitName: unitName(unitPath),
					Changed:  changed,
				})
			}
		}
		// PropertiesChanged carries the new SubState directly, but the
		// historical behavior also refreshed the full unit status, so keep the
		// substate fetch for that signal when anyone wants substates.
		if subStateSubs {
			c.enqueueFetch(fetchRequest{unitPath: unitPath})
		}
	}
}

// Deprecated: use SubscribeUnitsContext instead.
func (c *Conn) SubscribeUnits(interval time.Duration) (<-chan map[string]*UnitStatus, <-chan error) {
	return c.SubscribeUnitsContext(context.Background(), interval)
}

// SubscribeUnitsContext returns two unbuffered channels which will receive all changed units every
// interval.  Deleted units are sent as nil.
func (c *Conn) SubscribeUnitsContext(ctx context.Context, interval time.Duration) (<-chan map[string]*UnitStatus, <-chan error) {
	return c.SubscribeUnitsCustomContext(ctx, interval, 0, func(u1, u2 *UnitStatus) bool { return *u1 != *u2 }, nil)
}

// Deprecated: use SubscribeUnitsCustomContext instead.
func (c *Conn) SubscribeUnitsCustom(interval time.Duration, buffer int, isChanged func(*UnitStatus, *UnitStatus) bool, filterUnit func(string) bool) (<-chan map[string]*UnitStatus, <-chan error) {
	return c.SubscribeUnitsCustomContext(context.Background(), interval, buffer, isChanged, filterUnit)
}

// SubscribeUnitsCustomContext is like [Conn.SubscribeUnitsContext] but lets you specify the buffer
// size of the channels, the comparison function for detecting changes and a filter
// function for cutting down on the noise that your channel receives.
func (c *Conn) SubscribeUnitsCustomContext(ctx context.Context, interval time.Duration, buffer int, isChanged func(*UnitStatus, *UnitStatus) bool, filterUnit func(string) bool) (<-chan map[string]*UnitStatus, <-chan error) {
	old := make(map[string]*UnitStatus)
	statusChan := make(chan map[string]*UnitStatus, buffer)
	errChan := make(chan error, buffer)

	go func() {
		defer close(statusChan)
		defer close(errChan)
		for {
			timerChan := time.After(interval)

			units, err := c.ListUnitsContext(ctx)
			if err == nil {
				cur := make(map[string]*UnitStatus)
				for i := range units {
					if filterUnit != nil && filterUnit(units[i].Name) {
						continue
					}
					cur[units[i].Name] = &units[i]
				}

				// add all new or changed units
				changed := make(map[string]*UnitStatus)
				for n, u := range cur {
					if oldU, ok := old[n]; !ok || isChanged(oldU, u) {
						changed[n] = u
					}
					delete(old, n)
				}

				// add all deleted units
				for oldN := range old {
					changed[oldN] = nil
				}

				old = cur

				if len(changed) != 0 {
					select {
					case statusChan <- changed:
					case <-ctx.Done():
						return
					}
				}
			} else {
				select {
				case errChan <- err:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-timerChan:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return statusChan, errChan
}

type SubStateUpdate struct {
	UnitName string
	SubState string
}

// eventUnit implements namedEvent so SubStateUpdate can be carried by eventBus.
func (u *SubStateUpdate) eventUnit() string { return u.UnitName }

// SubscribeSubStates registers a subscriber for unit substate changes. Events
// are buffered per subscriber up to opts.Buffer; when the consumer falls
// behind, the oldest buffered event is discarded (counted and reported through
// UnitSubscription.Err) rather than blocking D-Bus signal dispatch. Coalescing
// is always enabled for substate events: a pending event for the same unit is
// replaced by the newer state, so an old state can never overwrite a newer one.
//
// The subscription ends when Close is called on the returned handle, when ctx
// is canceled, or when the Conn is closed; in each case the subscription's
// channels are closed. Subscribing repeatedly is supported and each call
// returns an independent subscription.
func (c *Conn) SubscribeSubStates(ctx context.Context, opts SubscribeOptions) (*UnitSubscription[*SubStateUpdate], error) {
	opts.Coalesce = true
	return c.subStateBus.subscribe(ctx, opts, &c.closeWG)
}

// SetSubStateSubscriber writes to updateCh when any unit's substate changes.
// Although this writes to updateCh on every state change, the reported state
// may be more recent than the change that generated it (due to an unavoidable
// race in the systemd dbus interface).  That is, this method provides a good
// way to keep a current view of all units' states, but is not guaranteed to
// show every state transition they go through.  Furthermore, state changes will
// only be written to the channel with non-blocking writes.  If updateCh is
// full, it attempts to write an error to errCh; if errCh is full, the error
// passes silently.
//
// Calling this method again replaces the previous subscriber; passing a nil
// update channel removes it. The provided channels are owned by the caller and
// are not closed by this package. Prefer SubscribeSubStates for new code: it
// supports multiple concurrent subscribers, bounded buffering with drop
// accounting and explicit unsubscribe.
func (c *Conn) SetSubStateSubscriber(updateCh chan<- *SubStateUpdate, errCh chan<- error) {
	if c == nil {
		msg := errors.New("nil receiver")
		select {
		case errCh <- msg:
		default:
			log.Printf("full error channel while reporting: %s\n", msg)
		}
		return
	}

	c.legacyMu.Lock()
	defer c.legacyMu.Unlock()

	if c.legacySubState != nil {
		c.legacySubState.Close()
		c.legacySubState = nil
	}

	if updateCh == nil {
		return
	}

	sub, err := c.subStateBus.subscribeLegacy(c.bgCtx, updateCh, errCh, true, &c.closeWG)
	if err != nil {
		select {
		case errCh <- err:
		default:
			log.Printf("full error channel while reporting: %s\n", err)
		}
		return
	}
	c.legacySubState = sub
}

// updateIgnore marks the unit's path as temporarily ignored when its
// LoadState is "not-found", to suppress the spurious UnitNew/UnitRemoved signal
// pair systemd emits for properties requests on unloaded units.
//
// The ignore functions work around a wart in the systemd dbus interface.
// Requesting the properties of an unloaded unit will cause systemd to send a
// pair of UnitNew/UnitRemoved signals.  Because we need to get a unit's
// properties on UnitNew (as that's the only indication of a new unit coming up
// for the first time), we would enter an infinite loop if we did not attempt
// to detect and ignore these spurious signals.  The signal themselves are
// indistinguishable from relevant ones, so we (somewhat hackishly) ignore an
// unloaded unit's signals for a short time after requesting its properties.
// This means that we will miss e.g. a transient unit being restarted
// *immediately* upon failure and also a transient unit being started
// immediately after requesting its status (with systemctl status, for example,
// because this causes a UnitNew signal to be sent which then causes us to fetch
// the properties).
func (c *Conn) updateIgnore(path dbus.ObjectPath, info map[string]any) {
	loadState, ok := info["LoadState"].(string)
	if !ok {
		return
	}

	// unit is unloaded - it will trigger bad systemd dbus behavior
	if loadState == "not-found" {
		c.ignores.add(path, time.Duration(ignoreInterval))
	}
}

// PropertiesUpdate holds a map of a unit's changed properties
type PropertiesUpdate struct {
	UnitName string
	Changed  map[string]dbus.Variant
}

// eventUnit implements namedEvent so PropertiesUpdate can be carried by eventBus.
func (u *PropertiesUpdate) eventUnit() string { return u.UnitName }

// SubscribeProperties registers a subscriber for raw unit property change
// signals. Every property change reported by systemd is offered to the
// subscriber; with Coalesce disabled (the default) no transition is collapsed,
// matching the historical SetPropertiesSubscriber behavior. As with
// SubscribeSubStates, a slow consumer causes the oldest events to be dropped
// and counted instead of blocking dispatch, and the subscription ends on Close,
// context cancellation or Conn.Close.
func (c *Conn) SubscribeProperties(ctx context.Context, opts SubscribeOptions) (*UnitSubscription[*PropertiesUpdate], error) {
	return c.propertiesBus.subscribe(ctx, opts, &c.closeWG)
}

// SetPropertiesSubscriber writes to updateCh when any unit's properties
// change. Every property change reported by systemd will be sent; that is, no
// transitions will be "missed" (as they might be with SetSubStateSubscriber).
// However, state changes will only be written to the channel with non-blocking
// writes.  If updateCh is full, it attempts to write an error to errCh; if
// errCh is full, the error passes silently.
//
// Calling this method again replaces the previous subscriber; passing a nil
// update channel removes it. The provided channels are owned by the caller and
// are not closed by this package. Prefer SubscribeProperties for new code.
func (c *Conn) SetPropertiesSubscriber(updateCh chan<- *PropertiesUpdate, errCh chan<- error) {
	c.legacyMu.Lock()
	defer c.legacyMu.Unlock()

	if c.legacyProps != nil {
		c.legacyProps.Close()
		c.legacyProps = nil
	}

	if updateCh == nil {
		return
	}

	sub, err := c.propertiesBus.subscribeLegacy(c.bgCtx, updateCh, errCh, false, &c.closeWG)
	if err != nil {
		select {
		case errCh <- err:
		default:
			log.Printf("full error channel while reporting: %s\n", err)
		}
		return
	}
	c.legacyProps = sub
}
