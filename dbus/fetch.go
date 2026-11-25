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
	"hash/fnv"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	// fetchShards is the number of worker goroutines fetching unit properties.
	// A unit always maps to the same shard, so its requests are processed in
	// signal order; a slow unit only stalls its own shard, never the signal
	// dispatch goroutine or other units' shards.
	fetchShards = 4
	// fetchQueueSize bounds the number of pending requests per shard. A unit
	// with a request already queued is coalesced (see enqueue), so the queue
	// depth tracks distinct busy units rather than signal volume.
	fetchQueueSize = 256
	// fetchTimeout bounds each property RPC so an unresponsive bus cannot pin
	// a worker forever during shutdown.
	fetchTimeout = 5 * time.Second
)

// fetchRequest asks a worker to resolve a unit's current substate. When
// unitName is set the request was triggered by JobRemoved and the unit's path
// must first be resolved via Manager.GetUnit, mirroring the historical
// dispatch-time behavior.
type fetchRequest struct {
	unitPath dbus.ObjectPath
	unitName string
}

type fetchShard struct {
	conn *Conn
	ch   chan fetchRequest
	mu   sync.Mutex
	// inFlight dedupes requests for a unit that is already queued; the newest
	// signal for a unit is still represented by the single pending request, so
	// no state transition ordering is lost within the shard.
	inFlight map[dbus.ObjectPath]struct{}
}

func newFetchShards(c *Conn) []*fetchShard {
	shards := make([]*fetchShard, fetchShards)
	for i := range shards {
		shards[i] = &fetchShard{
			conn:     c,
			ch:       make(chan fetchRequest, fetchQueueSize),
			inFlight: make(map[dbus.ObjectPath]struct{}),
		}
	}
	return shards
}

// shardFor deterministically assigns a unit path to a worker.
func shardFor(path dbus.ObjectPath) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return int(h.Sum32() % fetchShards)
}

// enqueue hands a request to its shard without blocking the dispatch goroutine.
// If the shard queue is full the request is dropped and reported to substate
// subscribers; a unit already queued is skipped (deduped).
func (c *Conn) enqueueFetch(req fetchRequest) {
	key := req.unitPath
	if key == "" {
		// JobRemoved: derive a stable key/path from the unit name.
		key = unitPath(req.unitName)
		req.unitPath = key
	}
	shard := c.fetchShards[shardFor(key)]

	shard.mu.Lock()
	if _, queued := shard.inFlight[key]; queued {
		shard.mu.Unlock()
		return
	}
	select {
	case shard.ch <- req:
		shard.inFlight[key] = struct{}{}
		shard.mu.Unlock()
	default:
		shard.mu.Unlock()
		c.subStateBus.publishErr(errors.New("fetch queue full, dropped unit update for " + req.unitName))
	}
}

func (s *fetchShard) run(stop <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case req := <-s.ch:
				s.mu.Lock()
				delete(s.inFlight, req.unitPath)
				s.mu.Unlock()
				s.conn.processFetch(req)
			case <-stop:
				return
			}
		}
	}()
}

// processFetch performs the (potentially slow) property RPC for one unit and
// publishes the resulting substate update. It never runs on the signal
// dispatch goroutine.
func (c *Conn) processFetch(req fetchRequest) {
	unitPath := req.unitPath

	if req.unitName != "" {
		// JobRemoved: resolve the current path, matching the old behavior of
		// calling Manager.GetUnit before reading properties.
		ctx, cancel := context.WithTimeout(c.bgCtx, fetchTimeout)
		var resolved dbus.ObjectPath
		err := c.sysobj.CallWithContext(ctx, "org.freedesktop.systemd1.Manager.GetUnit", 0, req.unitName).Store(&resolved)
		cancel()
		if err != nil || resolved == "" {
			return
		}
		unitPath = resolved
	}

	if c.ignores.ignored(unitPath) {
		return
	}

	ctx, cancel := context.WithTimeout(c.bgCtx, fetchTimeout)
	info, err := c.GetUnitPathPropertiesContext(ctx, unitPath)
	cancel()
	if err != nil {
		c.subStateBus.publishErr(err)
		return
	}
	c.updateIgnore(unitPath, info)

	name, ok := info["Id"].(string)
	if !ok {
		c.subStateBus.publishErr(errors.New("failed to cast info.Id"))
		return
	}
	substate, ok := info["SubState"].(string)
	if !ok {
		c.subStateBus.publishErr(errors.New("failed to cast info.SubState"))
		return
	}

	c.subStateBus.publish(&SubStateUpdate{UnitName: name, SubState: substate})
}
