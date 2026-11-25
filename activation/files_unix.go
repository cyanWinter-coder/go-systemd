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

//go:build unix

// Package activation implements primitives for systemd socket activation.
package activation

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// maxListenFds is a generous sanity limit for the number of descriptors
// advertised in LISTEN_FDS. Production services pass dozens of sockets, and
// even large hosts stay below a million. It only guards against a broken or
// hostile environment making us allocate an enormous slice; it is not a
// protocol limit.
const maxListenFds = 1 << 20

// Files returns a slice containing an `os.File` object for each
// file descriptor passed to this process via systemd fd-passing protocol.
//
// The order of the file descriptors is preserved in the returned slice.
// `unsetEnv` is typically set to `true` in order to avoid clashes in
// fd usage and to avoid leaking environment flags to child processes. Only
// the LISTEN_PID, LISTEN_FDS and LISTEN_FDNAMES variables are removed; every
// other environment variable is left untouched.
//
// Files silently returns nil when the socket activation environment is absent
// or invalid, preserving the historical behaviour. Callers that need to tell
// "not socket activated" apart from a malformed environment should use
// [FilesWithError], and the Listeners family reports validation errors
// directly.
func Files(unsetEnv bool) []*os.File {
	files, _, err := filesFromEnv(unsetEnv)
	if err != nil {
		return nil
	}
	return files
}

// FilesWithError is like [Files], but reports a malformed socket activation
// environment as an error instead of silently returning nil.
//
// When neither LISTEN_PID nor LISTEN_FDS is set the process was not socket
// activated, so a nil slice and a nil error are returned. Any other
// inconsistency — a missing LISTEN_PID/LISTEN_FDS, a non-numeric value, a
// negative count, or a LISTEN_PID that does not identify the current process
// — is reported as an error before any inherited descriptor is used. This
// prevents adopting file descriptors that belong to a different process.
func FilesWithError(unsetEnv bool) ([]*os.File, error) {
	files, _, err := filesFromEnv(unsetEnv)
	return files, err
}

// filesFromEnv parses and validates the systemd socket activation environment
// and wraps the inherited descriptors as os.File values.
//
// The returned names slice holds the raw entries of LISTEN_FDNAMES; it is nil
// when the variable is absent or empty. Unlike the file names exposed through
// os.File.Name it contains no generated "LISTEN_FD_n" fallbacks, allowing
// callers to detect missing names.
func filesFromEnv(unsetEnv bool) (files []*os.File, names []string, err error) {
	if unsetEnv {
		defer unsetListenEnv()
	}

	pidStr := os.Getenv("LISTEN_PID")
	fdsStr := os.Getenv("LISTEN_FDS")

	// Neither variable is set: the process was not socket activated by
	// systemd. This is not an error, so Files, Listeners and friends keep
	// returning empty results for ordinary (non-activated) processes.
	if pidStr == "" && fdsStr == "" {
		return nil, nil, nil
	}

	if pidStr == "" {
		return nil, nil, fmt.Errorf("activation: LISTEN_PID is not set; refusing to adopt LISTEN_FDS=%q without verifying the owner", fdsStr)
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return nil, nil, fmt.Errorf("activation: invalid LISTEN_PID %q: %w", pidStr, err)
	}
	if pid != os.Getpid() {
		// Adopting the descriptors in this case would touch fds that the
		// protocol handed to a different process (or stale environment from
		// an earlier generation), so stop before touching any fd.
		return nil, nil, fmt.Errorf("activation: LISTEN_PID mismatch: LISTEN_PID %d does not match current process pid %d", pid, os.Getpid())
	}

	if fdsStr == "" {
		return nil, nil, fmt.Errorf("activation: LISTEN_FDS is not set for LISTEN_PID %d", pid)
	}
	nfds, err := strconv.Atoi(fdsStr)
	if err != nil {
		return nil, nil, fmt.Errorf("activation: invalid LISTEN_FDS %q: %w", fdsStr, err)
	}
	if nfds < 0 {
		return nil, nil, fmt.Errorf("activation: invalid negative LISTEN_FDS value %d", nfds)
	}
	if nfds == 0 {
		return nil, nil, nil
	}
	if nfds > maxListenFds {
		return nil, nil, fmt.Errorf("activation: LISTEN_FDS count %d exceeds supported maximum %d", nfds, maxListenFds)
	}

	names = splitNames(os.Getenv("LISTEN_FDNAMES"))

	files = make([]*os.File, 0, nfds)
	for fd := listenFdsStart; fd < listenFdsStart+nfds; fd++ {
		syscall.CloseOnExec(fd)
		name := "LISTEN_FD_" + strconv.Itoa(fd)
		offset := fd - listenFdsStart
		if offset < len(names) && names[offset] != "" {
			name = names[offset]
		}
		files = append(files, os.NewFile(uintptr(fd), name))
	}

	return files, names, nil
}

// splitNames parses LISTEN_FDNAMES. An empty value yields a nil slice so that
// callers can tell that no names were supplied at all.
func splitNames(env string) []string {
	if env == "" {
		return nil
	}
	return strings.Split(env, ":")
}

// unsetListenEnv removes exactly the variables defined by the socket
// activation protocol. It never modifies any other environment variable.
func unsetListenEnv() {
	// Unsetenv on unix never returns an error for these names.
	_ = os.Unsetenv("LISTEN_PID")
	_ = os.Unsetenv("LISTEN_FDS")
	_ = os.Unsetenv("LISTEN_FDNAMES")
}

// socketType reports the SO_TYPE of the socket behind f. It is used to tell
// an expected nil gap (a datagram socket passed to Listeners, or a stream
// socket passed to PacketConns) apart from a genuinely unusable descriptor.
func socketType(f *os.File) (int, error) {
	return syscall.GetsockoptInt(int(f.Fd()), syscall.SOL_SOCKET, syscall.SO_TYPE)
}
