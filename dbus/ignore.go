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
	"sync"
	"time"

	"github.com/godbus/dbus/v5"
)

// ignoreList bounds the workaround map that suppresses the spurious
// UnitNew/UnitRemoved signals systemd emits when properties are requested for
// an unloaded unit. Entries carry an expiry timestamp; a background sweeper
// removes stale ones periodically, and insertion refuses to grow the map beyond
// MaxIgnoreEntries so it cannot consume unbounded memory on a busy host.
//
// The full background for this workaround is documented at updateIgnore in
// subscription.go.
type ignoreList struct {
	mu      sync.Mutex
	entries map[dbus.ObjectPath]int64 // unix nano expiry
	now     func() int64
}

func newIgnoreList() *ignoreList {
	return &ignoreList{
		entries: make(map[dbus.ObjectPath]int64),
		now:     func() int64 { return time.Now().UnixNano() },
	}
}

// ignored reports whether signals for path are currently suppressed.
func (l *ignoreList) ignored(path dbus.ObjectPath) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	expiry, ok := l.entries[path]
	return ok && expiry >= l.now()
}

// add suppresses path until the given TTL elapses. If the map is already at
// MaxIgnoreEntries, expired entries are swept first; if it is still full the
// new entry is skipped rather than evicting a live suppression.
func (l *ignoreList) add(path dbus.ObjectPath, ttl time.Duration) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) >= MaxIgnoreEntries {
		for p, expiry := range l.entries {
			if expiry < now {
				delete(l.entries, p)
			}
		}
		if len(l.entries) >= MaxIgnoreEntries {
			return
		}
	}

	l.entries[path] = now + int64(ttl)
}

// sweep removes all expired entries.
func (l *ignoreList) sweep() {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for p, expiry := range l.entries {
		if expiry < now {
			delete(l.entries, p)
		}
	}
}

// run sweeps the list on a ticker until stop is closed.
func (l *ignoreList) run(stop <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Duration(cleanIgnoreInterval))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				l.sweep()
			case <-stop:
				return
			}
		}
	}()
}
