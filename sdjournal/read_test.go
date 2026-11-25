// Copyright 2025 CoreOS, Inc.
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

package sdjournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/coreos/go-systemd/v22/journal"
)

// constFormatter makes tests independent of whether an entry carries a
// MESSAGE field, which simpleMessageFormatter requires.
func constFormatter(s string) func(entry *JournalEntry) (string, error) {
	return func(entry *JournalEntry) (string, error) {
		return s, nil
	}
}

func TestMatchValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		match Match
		valid bool
	}{
		{"plain field", Match{Field: "MESSAGE", Value: "x"}, true},
		{"trusted field", Match{Field: "_SYSTEMD_UNIT", Value: "x.service"}, true},
		{"digit in field", Match{Field: "FIELD2", Value: "x"}, true},
		{"empty field", Match{Field: "", Value: "x"}, false},
		{"lowercase field", Match{Field: "message", Value: "x"}, false},
		{"space in field", Match{Field: "BAD FIELD", Value: "x"}, false},
		{"leading digit", Match{Field: "2FIELD", Value: "x"}, false},
		{"field too long", Match{Field: string(make([]byte, 65)), Value: "x"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.match.Validate()
			if tc.valid && err != nil {
				t.Fatalf("expected match %q to be valid, got: %v", tc.match.String(), err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("expected match %q to be invalid", tc.match.String())
			}
		})
	}
}

func TestNewJournalReaderInvalidMatch(t *testing.T) {
	// Invalid filter expressions must fail while opening, before any
	// entry can be read.
	r, err := NewJournalReader(JournalReaderConfig{
		Formatter: constFormatter("x"),
		Matches:   []Match{{Field: "BAD FIELD", Value: "x"}},
	})
	if err == nil {
		_ = r.Close()
		t.Fatal("expected an error for an invalid match, got nil")
	}
	if r != nil {
		t.Fatalf("expected a nil reader on failure, got %#v", r)
	}
}

func TestNewJournalReaderMalformedCursor(t *testing.T) {
	// sd_journal_seek_cursor rejects syntactically invalid cursors with
	// EINVAL.
	_, err := NewJournalReader(JournalReaderConfig{
		Cursor:    "not-a-cursor",
		Formatter: constFormatter("x"),
	})
	if err == nil {
		t.Fatal("expected an error for a malformed cursor, got nil")
	}
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("expected EINVAL for malformed cursor, got: %v", err)
	}
}

// wellFormedButUnknownCursor parses correctly but cannot match any
// entry: the boot id and sequence number do not exist in any real
// journal. It mirrors the state of a cursor whose entry was rotated
// away.
const wellFormedButUnknownCursor = "s=11111111111111111111111111111111;i=999999999;b=22222222222222222222222222222222;m=1;t=1;x=1"

func TestNewJournalReaderStaleCursor(t *testing.T) {
	_, err := NewJournalReader(JournalReaderConfig{
		Cursor:    wellFormedButUnknownCursor,
		Formatter: constFormatter("x"),
	})
	if err == nil {
		t.Fatal("expected an error for a rotated-away cursor, got nil")
	}
	if !errors.Is(err, ErrCursorUnavailable) {
		t.Fatalf("expected ErrCursorUnavailable, got: %v", err)
	}

	var cue *CursorUnavailableError
	if !errors.As(err, &cue) {
		t.Fatalf("expected a CursorUnavailableError, got %T: %v", err, err)
	}
	if cue.Cursor != wellFormedButUnknownCursor {
		t.Fatalf("unexpected cursor in error: %q", cue.Cursor)
	}
	if cue.Earliest == "" {
		t.Fatal("expected the earliest available cursor on a non-empty journal")
	}

	// The reported earliest cursor must let the caller reopen the
	// reader and resume immediately.
	r, err := NewJournalReader(JournalReaderConfig{
		Cursor:    cue.Earliest,
		Formatter: constFormatter("ok\n"),
	})
	if err != nil {
		t.Fatalf("failed to reopen reader at earliest cursor: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 32)
	deadline := time.Now().Add(3 * time.Second)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			return
		}
		if rerr != nil && rerr != io.EOF {
			t.Fatalf("read after recovery failed: %v", rerr)
		}
		if time.Now().After(deadline) {
			t.Fatal("no entry readable after repositioning to the earliest cursor")
		}
	}
}

func TestNewJournalReaderStaleCursorEmptyJournal(t *testing.T) {
	// On an empty journal no cursor can resolve and no earliest cursor
	// exists.
	_, err := NewJournalReader(JournalReaderConfig{
		Path:      t.TempDir(),
		Cursor:    wellFormedButUnknownCursor,
		Formatter: constFormatter("x"),
	})
	var cue *CursorUnavailableError
	if !errors.As(err, &cue) {
		t.Fatalf("expected CursorUnavailableError, got: %v", err)
	}
	if cue.Earliest != "" {
		t.Fatalf("expected empty earliest cursor on an empty journal, got %q", cue.Earliest)
	}
}

func TestJournalReaderCloseIdempotent(t *testing.T) {
	r, err := NewJournalReader(JournalReaderConfig{
		Since:     -time.Second,
		Formatter: constFormatter("x"),
	})
	if err != nil {
		t.Fatalf("Error opening journal: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close must be idempotent, got: %v", err)
	}
	if _, err := r.Read(make([]byte, 8)); !errors.Is(err, ErrReadClosed) {
		t.Fatalf("Read after Close must return ErrReadClosed, got: %v", err)
	}
	if err := r.Rewind(); !errors.Is(err, ErrReadClosed) {
		t.Fatalf("Rewind after Close must return ErrReadClosed, got: %v", err)
	}
}

func TestFollowWithContextDeadline(t *testing.T) {
	// A quiet, uniquely matched reader reaches the tail immediately.
	field := "SDJTESTFOLLOWDEADLINE"
	value := uniqueValue(t)
	r := newMatchReader(t, field, value)

	start := time.Now()
	result := r.FollowWithContext(context.Background(), time.After(400*time.Millisecond), io.Discard)
	elapsed := time.Since(start)

	if result.Reason != FollowUntilExceeded {
		t.Fatalf("expected FollowUntilExceeded, got reason %d err %v", result.Reason, result.Err)
	}
	if result.Err != nil {
		t.Fatalf("deadline is a clean end, got error: %v", result.Err)
	}
	// The follow must actually block on the deadline instead of
	// returning early or spinning through it.
	if elapsed < 300*time.Millisecond {
		t.Fatalf("Follow returned after %s, before the deadline", elapsed)
	}
}

func TestFollowWithContextContextCancel(t *testing.T) {
	field := "SDJTESTFOLLOWCANCEL"
	value := uniqueValue(t)
	r := newMatchReader(t, field, value)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan FollowResult, 1)
	go func() {
		done <- r.FollowWithContext(ctx, nil, io.Discard)
	}()

	// Give the follow loop time to park inside the wait, then cancel.
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case result := <-done:
		if result.Reason != FollowInterrupted {
			t.Fatalf("expected FollowInterrupted, got %d", result.Reason)
		}
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", result.Err)
		}
		// Cancellation must be noticed well before the next full wait
		// iteration would run; a generous bound tolerates scheduler
		// jitter while still failing on a polling-based implementation
		// that ignores ctx.
		if time.Since(start) > 2*time.Second {
			t.Fatalf("cancel took %s to take effect", time.Since(start))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Follow did not return after context cancellation")
	}
}

func TestFollowCloseUnblocks(t *testing.T) {
	field := "SDJTESTFOLLOWCLOSE"
	value := uniqueValue(t)
	r := newMatchReader(t, field, value)

	done := make(chan FollowResult, 1)
	go func() {
		done <- r.FollowWithContext(context.Background(), nil, io.Discard)
	}()

	// Park the follow inside the bounded wait, then close from outside.
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	if err := r.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// The sleeping sd_journal_wait (500ms here) bounds how long Close
	// blocks while joining the waiter.
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Close blocked for %s while a wait was in flight", time.Since(start))
	}

	select {
	case result := <-done:
		if result.Reason != FollowReaderClosed {
			t.Fatalf("expected FollowReaderClosed, got reason %d err %v", result.Reason, result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("Follow did not return after Close")
	}

	// Repeated close stays idempotent.
	if err := r.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestFollowFiltersMatches(t *testing.T) {
	field := "SDJTESTFILTER"
	value := uniqueValue(t)
	// Use a formatter that preserves the MESSAGE field so the sent
	// contents are observable in the writer output.
	formatter := func(entry *JournalEntry) (string, error) {
		return entry.Fields["MESSAGE"] + "\n", nil
	}
	r, err := NewJournalReader(JournalReaderConfig{
		Matches:     []Match{{Field: field, Value: value}},
		Formatter:   formatter,
		WaitTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Error opening journal: %v", err)
	}
	defer r.Close()

	var out bytes.Buffer
	deadline := time.After(2 * time.Second)
	done := make(chan FollowResult, 1)
	go func() {
		done <- r.FollowWithContext(context.Background(), deadline, &out)
	}()

	// Give the follower time to reach the tail.
	time.Sleep(300 * time.Millisecond)

	want := "matching-entry-" + value
	if err := journal.Send(want, journal.PriInfo, map[string]string{field: value}); err != nil {
		t.Fatalf("failed to write matching entry: %v", err)
	}
	if err := journal.Send("must-not-appear", journal.PriInfo, map[string]string{field: value + "-other"}); err != nil {
		t.Fatalf("failed to write non-matching entry: %v", err)
	}

	result := <-done
	if result.Reason != FollowUntilExceeded {
		t.Fatalf("expected FollowUntilExceeded, got reason %d err %v", result.Reason, result.Err)
	}
	body := out.String()
	if !bytes.Contains(out.Bytes(), []byte(want)) {
		t.Fatalf("matching entry was not delivered, output: %q", body)
	}
	if bytes.Contains(out.Bytes(), []byte("must-not-appear")) {
		t.Fatalf("non-matching entry leaked through the filter, output: %q", body)
	}
}

func TestFollowLegacyWrapper(t *testing.T) {
	field := "SDJTESTLEGACY"
	value := uniqueValue(t)
	r := newMatchReader(t, field, value)

	if err := r.Follow(time.After(300*time.Millisecond), io.Discard); !errors.Is(err, ErrExpired) {
		t.Fatalf("legacy Follow must return ErrExpired on deadline, got: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := r.Follow(nil, io.Discard); !errors.Is(err, ErrReadClosed) {
		t.Fatalf("legacy Follow after Close must return ErrReadClosed, got: %v", err)
	}
}

func TestReadReturnsEOFAtTail(t *testing.T) {
	field := "SDJTESTEOF"
	value := uniqueValue(t)
	r := newMatchReader(t, field, value)
	defer r.Close()

	buf := make([]byte, 16)
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := r.Read(buf)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("never reached io.EOF at the tail")
		}
	}
}

// newMatchReader opens a reader filtered to a unique field/value pair,
// which guarantees no pre-existing entries match and the first wait
// happens at the tail.
func newMatchReader(t *testing.T, field, value string) *JournalReader {
	t.Helper()
	r, err := NewJournalReader(JournalReaderConfig{
		Matches:     []Match{{Field: field, Value: value}},
		Formatter:   constFormatter("entry\n"),
		WaitTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Error opening journal: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func uniqueValue(t *testing.T) string {
	t.Helper()
	return time.Now().Format("20060102T150405.000000000") + "-" + t.Name()
}
