// Copyright 2015 Red Hat, Inc.
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

package sdjournal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ErrExpired gets returned when the Follow function runs into the
// specified timeout.
var ErrExpired = errors.New("Timeout expired")

// ErrCursorUnavailable is returned when a cursor passed to
// NewJournalReader cannot be located in the current journal, typically
// because the entry it pointed at has been rotated away (for example
// after a machine restart). Use errors.Is to check for it and type
// assert the error to CursorUnavailableError to retrieve the cursor of
// the oldest entry still available, which can be used to reopen the
// reader and resume consumption without data loss beyond the rotated
// entries.
var ErrCursorUnavailable = errors.New("sdjournal: cursor is not available in the current journal")

// ErrReadClosed is returned when Read or Follow is used after the
// JournalReader has been closed.
var ErrReadClosed = errors.New("sdjournal: journal reader is closed")

// defaultWaitTimeout is the maximum time the internal journal wait
// sleeps in sd_journal_wait before rechecking for cancellation. It
// bounds how long Close can block while a wait is in flight and costs
// no CPU while sleeping, unlike a polling loop.
const defaultWaitTimeout = time.Second

// CursorUnavailableError wraps ErrCursorUnavailable. It carries the
// cursor that failed to resolve and the cursor of the oldest entry
// currently available in the journal, which the caller can use to
// reposition a new reader.
type CursorUnavailableError struct {
	// Cursor is the cursor that could not be located.
	Cursor string
	// Earliest is the cursor of the oldest available entry, or the
	// empty string when the journal contains no entries at all.
	Earliest string
}

func (e *CursorUnavailableError) Error() string {
	if e.Earliest == "" {
		return fmt.Sprintf("sdjournal: cursor %q is not available and the journal is empty", e.Cursor)
	}
	return fmt.Sprintf("sdjournal: cursor %q is not available; earliest available cursor is %q", e.Cursor, e.Earliest)
}

// Unwrap exposes ErrCursorUnavailable for errors.Is.
func (e *CursorUnavailableError) Unwrap() error {
	return ErrCursorUnavailable
}

// FollowEndReason describes why FollowWithContext stopped.
type FollowEndReason int

const (
	// FollowUntilExceeded means the until channel fired. This is a
	// normal end condition, not a read error.
	FollowUntilExceeded FollowEndReason = iota
	// FollowReaderClosed means the reader was closed while following.
	FollowReaderClosed
	// FollowInterrupted means following stopped because of the context
	// or an I/O error; see FollowResult.Err.
	FollowInterrupted
)

// String returns a human readable description of the end reason.
func (r FollowEndReason) String() string {
	switch r {
	case FollowUntilExceeded:
		return "until deadline exceeded"
	case FollowReaderClosed:
		return "reader closed"
	case FollowInterrupted:
		return "interrupted"
	default:
		return fmt.Sprintf("unknown follow end reason %d", int(r))
	}
}

// FollowResult is the outcome of FollowWithContext. Err is only set
// when Reason is FollowInterrupted.
type FollowResult struct {
	Reason FollowEndReason
	Err    error
}

// JournalReaderConfig represents options to drive the behavior of a JournalReader.
type JournalReaderConfig struct {
	// The Since, NumFromTail and Cursor options are mutually exclusive and
	// determine where the reading begins within the journal. The order in which
	// options are written is exactly the order of precedence.
	Since       time.Duration // start relative to a Duration from now
	NumFromTail uint64        // start relative to the tail
	Cursor      string        // start relative to the cursor

	// Show only journal entries whose fields match the supplied values. If
	// the array is empty, entries will not be filtered. Invalid match
	// expressions are rejected when the reader is opened.
	Matches []Match

	// If not empty, the journal instance will point to a journal residing
	// in this directory. The supplied path may be relative or absolute.
	Path string

	// If not nil, Formatter will be used to translate the resulting entries
	// into strings. If not set, the default format (timestamp and message field)
	// will be used. If Formatter returns an error, Read will stop and return the error.
	Formatter func(entry *JournalEntry) (string, error)

	// WaitTimeout caps how long the internal sd_journal_wait call sleeps
	// while following the journal. It is the granularity at which
	// cancellation and Close are noticed, and therefore bounds the time
	// Close may block while a wait is in flight. Leaving it zero selects
	// a one second default. It must be positive; sdjournal.IndefiniteWait
	// is accepted but then Close cannot complete until the journal next
	// changes.
	WaitTimeout time.Duration
}

// JournalReader is an io.ReadCloser which provides a simple interface for iterating through the
// systemd journal. A JournalReader is not safe for concurrent use by multiple goroutines, except
// that Close may be called from another goroutine to unblock a running FollowWithContext.
type JournalReader struct {
	journal   *Journal
	msgReader *strings.Reader
	formatter func(entry *JournalEntry) (string, error)

	waitTimeout time.Duration

	// mu guards closed and is held by Close while in-flight reads and
	// the wait goroutine are draining, so the underlying journal handle
	// is never released while a C call still uses it.
	mu     sync.Mutex
	closed bool

	// closeCh is closed once when Close runs.
	closeCh chan struct{}

	// readWg tracks Read calls currently inside the C library.
	readWg sync.WaitGroup

	// The waiter goroutine, started lazily by Follow. eventCh buffers a
	// single sd_journal_wait status; doneCh is closed when the goroutine
	// exits.
	startOnce     sync.Once
	waiterMu      sync.Mutex
	waiterRunning bool
	eventCh       chan int
	doneCh        chan struct{}
}

// NewJournalReader creates a new JournalReader with configuration options that are similar to the
// systemd journalctl tool's iteration and filtering features.
//
// When config.Cursor points at an entry that no longer exists, the
// returned error wraps ErrCursorUnavailable and a CursorUnavailableError
// carrying the cursor of the oldest available entry; malformed cursors
// are returned as a plain error wrapping syscall.EINVAL. Invalid match
// expressions are rejected here, at open time.
func NewJournalReader(config JournalReaderConfig) (*JournalReader, error) {
	// use simpleMessageFormatter as default formatter.
	if config.Formatter == nil {
		config.Formatter = simpleMessageFormatter
	}

	waitTimeout := config.WaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = defaultWaitTimeout
	}

	r := &JournalReader{
		formatter:   config.Formatter,
		waitTimeout: waitTimeout,
		closeCh:     make(chan struct{}),
		eventCh:     make(chan int, 1),
		doneCh:      make(chan struct{}),
	}

	// Open the journal
	var err error
	if config.Path != "" {
		r.journal, err = NewJournalFromDir(config.Path)
	} else {
		r.journal, err = NewJournal()
	}
	if err != nil {
		return nil, err
	}

	// Add any supplied matches. Validation failures and C-side rejections
	// both surface while opening, before the reader is returned.
	for _, m := range config.Matches {
		if err := m.Validate(); err != nil {
			_ = r.journal.Close()
			return nil, err
		}
		if err := r.journal.AddMatch(m.String()); err != nil {
			_ = r.journal.Close()
			return nil, fmt.Errorf("sdjournal: invalid match %q: %w", m.String(), err)
		}
	}

	// Set the start position based on options
	if config.Since != 0 {
		// Start based on a relative time
		start := time.Now().Add(config.Since)
		if err := r.journal.SeekRealtimeUsec(uint64(start.UnixNano() / 1000)); err != nil {
			_ = r.journal.Close()
			return nil, err
		}
	} else if config.NumFromTail != 0 {
		// Start based on a number of lines before the tail
		if err := r.journal.SeekTail(); err != nil {
			_ = r.journal.Close()
			return nil, err
		}

		// Move the read pointer into position near the tail. Go one further than
		// the option so that the initial cursor advancement positions us at the
		// correct starting point.
		skip, err := r.journal.PreviousSkip(config.NumFromTail + 1)
		if err != nil {
			_ = r.journal.Close()
			return nil, err
		}
		// If we skipped fewer lines than expected, we have reached journal start.
		// Thus, we seek to head so that next invocation can read the first line.
		if skip != config.NumFromTail+1 {
			if err := r.journal.SeekHead(); err != nil {
				_ = r.journal.Close()
				return nil, err
			}
		}
	} else if config.Cursor != "" {
		// Start based on a custom cursor. sd_journal_seek_cursor only
		// fails on malformed cursors; a cursor whose entry was rotated
		// away is accepted silently and repositions to the closest
		// entry, so verify explicitly with Next/TestCursor.
		if err := r.journal.SeekCursor(config.Cursor); err != nil {
			_ = r.journal.Close()
			return nil, err
		}

		n, err := r.journal.Next()
		if err != nil {
			_ = r.journal.Close()
			return nil, err
		}

		stale := false
		if n == 0 {
			// The journal holds no entries, so no cursor can resolve.
			stale = true
		} else if err := r.journal.TestCursor(config.Cursor); err != nil {
			if !errors.Is(err, ErrNoTestCursor) {
				_ = r.journal.Close()
				return nil, err
			}
			stale = true
		}

		if stale {
			earliest, _ := r.earliestCursor()
			_ = r.journal.Close()
			return nil, &CursorUnavailableError{
				Cursor:   config.Cursor,
				Earliest: earliest,
			}
		}

		// Reposition before the sought entry so that, matching the
		// previous behavior, the first Read call returns the entry the
		// cursor points at rather than the one following it.
		if err := r.journal.SeekCursor(config.Cursor); err != nil {
			_ = r.journal.Close()
			return nil, err
		}
	}

	return r, nil
}

// earliestCursor returns the cursor of the oldest entry currently
// available in the journal, or the empty string if the journal is
// empty. It temporarily moves the journal read pointer.
func (r *JournalReader) earliestCursor() (string, error) {
	if err := r.journal.SeekHead(); err != nil {
		return "", err
	}
	n, err := r.journal.Next()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	return r.journal.GetCursor()
}

// Read reads entries from the journal. Read follows the Reader interface so
// it must be able to read a specific amount of bytes. Journald on the other
// hand only allows us to read full entries of arbitrary size (without byte
// granularity). JournalReader is therefore internally buffering entries that
// don't fit in the read buffer. Callers should keep calling until 0 and/or an
// error is returned. At the tail of the journal Read returns io.EOF; use
// FollowWithContext to wait for new entries without polling.
func (r *JournalReader) Read(b []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, ErrReadClosed
	}
	// Add while holding mu so Close can never reach Wait while an Add
	// that observed closed == false is still in flight.
	r.readWg.Add(1)
	r.mu.Unlock()
	defer r.readWg.Done()

	if r.msgReader == nil {
		// Advance the journal cursor. It has to be called at least one time
		// before reading
		c, err := r.journal.Next()
		// An unexpected error
		if err != nil {
			return 0, err
		}

		// EOF detection
		if c == 0 {
			return 0, io.EOF
		}

		entry, err := r.journal.GetEntry()
		if err != nil {
			return 0, err
		}

		// Build a message
		msg, err := r.formatter(entry)
		if err != nil {
			return 0, err
		}
		r.msgReader = strings.NewReader(msg)
	}

	// Copy and return the message
	sz, err := r.msgReader.Read(b)
	if err == io.EOF {
		// The current entry has been fully read. Don't propagate this
		// EOF, so the next entry can be read at the next Read()
		// iteration.
		r.msgReader = nil
		return sz, nil
	}
	if err != nil {
		return sz, err
	}
	if r.msgReader.Len() == 0 {
		r.msgReader = nil
	}

	return sz, nil
}

// Close closes the JournalReader's handle to the journal. It is safe to
// call Close multiple times and concurrently with a running
// FollowWithContext: the follow loop returns immediately with
// FollowReaderClosed, while Close blocks until in-flight journal calls
// drain and then releases the underlying handle, at most WaitTimeout
// when a wait is sleeping.
func (r *JournalReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.closeCh)

	started := r.waiterStarted()
	r.mu.Unlock()

	// Wait for the waiter goroutine and any in-flight Read to leave the
	// C library before freeing the handle.
	if started {
		<-r.doneCh
	}
	r.readWg.Wait()
	return r.journal.Close()
}

// Rewind attempts to rewind the JournalReader to the first entry.
func (r *JournalReader) Rewind() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrReadClosed
	}
	r.mu.Unlock()

	r.msgReader = nil
	return r.journal.SeekHead()
}

// waitLoop is the single goroutine that blocks in sd_journal_wait. The
// C call sleeps in poll(2) for at most waitTimeout, which keeps CPU
// usage at zero while idle, and the bounded timeout guarantees Close
// cannot stall forever.
func (r *JournalReader) waitLoop() {
	defer close(r.doneCh)
	for {
		status := r.journal.Wait(r.waitTimeout)
		select {
		case r.eventCh <- status:
		case <-r.closeCh:
			return
		}
	}
}

func (r *JournalReader) ensureWaiter() {
	r.startOnce.Do(func() {
		r.waiterMu.Lock()
		r.waiterRunning = true
		r.waiterMu.Unlock()
		go r.waitLoop()
	})
}

func (r *JournalReader) waiterStarted() bool {
	r.waiterMu.Lock()
	defer r.waiterMu.Unlock()
	return r.waiterRunning
}

// FollowWithContext synchronously follows the JournalReader, writing
// each new journal entry to writer. It processes entries already
// available and then blocks without polling until new entries arrive,
// the until channel fires, ctx is canceled, or the reader is closed.
//
// A deadline on until ends the follow cleanly with
// FollowUntilExceeded rather than a read error. Context cancellation
// and I/O failures report FollowInterrupted with Err set; closing the
// reader reports FollowReaderClosed immediately.
func (r *JournalReader) FollowWithContext(ctx context.Context, until <-chan time.Time, writer io.Writer) FollowResult {
	if ctx == nil {
		ctx = context.Background()
	}
	msg := make([]byte, 64*1<<(10))

	for {
		c, err := r.Read(msg)
		if err != nil && err != io.EOF {
			if errors.Is(err, ErrReadClosed) {
				return FollowResult{Reason: FollowReaderClosed}
			}
			return FollowResult{Reason: FollowInterrupted, Err: err}
		}

		// Honor the deadline and cancellation even while a large
		// backlog is being drained; non-blocking so entries keep
		// flowing when none of them has fired.
		select {
		case <-until:
			return FollowResult{Reason: FollowUntilExceeded}
		case <-r.closeCh:
			return FollowResult{Reason: FollowReaderClosed}
		case <-ctx.Done():
			return FollowResult{Reason: FollowInterrupted, Err: ctx.Err()}
		default:
		}

		if c > 0 {
			if _, err := writer.Write(msg[:c]); err != nil {
				return FollowResult{Reason: FollowInterrupted, Err: err}
			}
			continue
		}

		// At the tail: block for journal changes, the deadline,
		// cancellation, or Close.
		r.ensureWaiter()
		select {
		case <-until:
			return FollowResult{Reason: FollowUntilExceeded}
		case <-r.closeCh:
			return FollowResult{Reason: FollowReaderClosed}
		case <-ctx.Done():
			return FollowResult{Reason: FollowInterrupted, Err: ctx.Err()}
		case status := <-r.eventCh:
			switch status {
			case SD_JOURNAL_NOP:
				// The journal did not change within WaitTimeout; loop
				// back and block on the next event. No busy polling:
				// eventCh stays empty until sd_journal_wait returns.
			case SD_JOURNAL_APPEND, SD_JOURNAL_INVALIDATE:
				// New entries or a rotated journal; re-run Read.
			default:
				if status < 0 {
					return FollowResult{
						Reason: FollowInterrupted,
						Err:    fmt.Errorf("sdjournal: journal wait failed: %w", syscall.Errno(-status)),
					}
				}
				log.Printf("received unknown event: %d\n", status)
			}
		}
	}
}

// Follow synchronously follows the JournalReader, writing each new journal entry to writer. The
// follow will continue until a single time.Time is received on the until channel.
//
// It is a compatibility wrapper around FollowWithContext: a fired
// deadline returns ErrExpired, a closed reader returns ErrReadClosed,
// and any other failure is returned as the underlying error. New code
// should prefer FollowWithContext, which reports an explicit end
// reason.
func (r *JournalReader) Follow(until <-chan time.Time, writer io.Writer) error {
	result := r.FollowWithContext(context.Background(), until, writer)
	switch result.Reason {
	case FollowUntilExceeded:
		return ErrExpired
	case FollowReaderClosed:
		return ErrReadClosed
	default:
		return result.Err
	}
}

// Validate checks that the match describes a legal journal field
// expression. sd_journal_add_match rejects the same expressions with
// EINVAL; validating up front makes the failure deterministic and
// keeps it in the open phase of NewJournalReader.
func (m Match) Validate() error {
	if m.Field == "" {
		return errors.New("sdjournal: match has an empty field name")
	}
	if len(m.Field) > 64 {
		return fmt.Errorf("sdjournal: match field %q is longer than 64 characters", m.Field)
	}
	for i, ch := range m.Field {
		switch {
		case ch >= 'A' && ch <= 'Z':
		case ch == '_':
		case ch >= '0' && ch <= '9':
			if i == 0 {
				return fmt.Errorf("sdjournal: match field %q must not start with a digit", m.Field)
			}
		default:
			return fmt.Errorf("sdjournal: match field %q contains invalid character %q", m.Field, ch)
		}
	}
	return nil
}

// simpleMessageFormatter is the default formatter.
// It returns a string representing the current journal entry in a simple format which
// includes the entry timestamp and MESSAGE field.
func simpleMessageFormatter(entry *JournalEntry) (string, error) {
	msg, ok := entry.Fields["MESSAGE"]
	if !ok {
		return "", errors.New("no MESSAGE field present in journal entry")
	}

	usec := entry.RealtimeTimestamp
	timestamp := time.Unix(0, int64(usec)*int64(time.Microsecond))

	return fmt.Sprintf("%s %s\n", timestamp, msg), nil
}
