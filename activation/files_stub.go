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

//go:build !unix

package activation

import "os"

// Socket activation through the systemd fd-passing protocol is only
// implemented on unix systems. On other platforms the environment is never
// consulted and no descriptors are created, which keeps Files, Listeners and
// PacketConns working (returning empty results) for portable callers.

// Files always returns nil on non-unix platforms.
func Files(unsetEnv bool) []*os.File {
	return nil
}

// FilesWithError always returns an empty result on non-unix platforms: the
// systemd fd-passing protocol is not available there.
func FilesWithError(unsetEnv bool) ([]*os.File, error) {
	return nil, nil
}

// filesFromEnv stands in for the unix implementation.
func filesFromEnv(unsetEnv bool) ([]*os.File, []string, error) {
	return nil, nil, nil
}

// socketType stands in for the unix implementation. It is only reachable from
// descriptor conversion paths, which never have any descriptors here.
func socketType(f *os.File) (int, error) {
	return sockOther, nil
}
