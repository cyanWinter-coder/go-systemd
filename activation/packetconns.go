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

package activation

import (
	"fmt"
	"net"
	"os"
)

// PacketConns returns a slice containing a net.PacketConn for each matching socket type
// passed to this process.
//
// The order of the file descriptors is preserved in the returned slice.
// Nil values are used to fill any gaps. For example if systemd were to return file descriptors
// corresponding with "udp, tcp, udp", then the slice would contain {net.PacketConn, nil, net.PacketConn}
//
// A missing or inconsistent socket activation environment is reported as an
// error. If a descriptor that ought to be a packet connection cannot be
// wrapped, all descriptors received from systemd — connections already
// created, the failing descriptor and descriptors not processed yet — are
// closed and the error is returned.
func PacketConns() ([]net.PacketConn, error) {
	files, _, err := filesFromEnv(true)
	if err != nil {
		return nil, err
	}
	return packetConnsFromFiles(files)
}

// packetConnsFromFiles wraps the datagram sockets in files into packet
// connections.
//
// The returned slice is parallel to files: sockets that
// net.FilePacketConn cannot turn into a packet connection because they are not
// datagram sockets leave a nil gap (as documented for stream sockets). Every
// inherited os.File is closed once wrapping has been attempted. On a genuine
// wrapping error all connections created so far and every not-yet-released
// file are closed.
func packetConnsFromFiles(files []*os.File) ([]net.PacketConn, error) {
	conns := make([]net.PacketConn, len(files))
	released := make([]bool, len(files))

	for i, f := range files {
		pc, err := net.FilePacketConn(f)
		if err != nil {
			kind, kindErr := socketType(f)
			if kindErr != nil {
				closePacketConnsAndFiles(conns, files, released)
				return nil, fmt.Errorf("activation: fd %d (index %d) is not a usable socket: %w",
					listenFdsStart+i, i, err)
			}
			if kind != sockDgram {
				// A socket of another type (e.g. SOCK_STREAM) cannot become a
				// net.PacketConn. Release the inherited descriptor and keep
				// the nil gap.
				_ = f.Close()
				released[i] = true
				continue
			}

			closePacketConnsAndFiles(conns, files, released)
			return nil, fmt.Errorf("activation: cannot wrap datagram socket fd %d (index %d) as a packet connection: %w",
				listenFdsStart+i, i, err)
		}

		conns[i] = pc

		// The connection owns an independent duplicate of the descriptor; the
		// inherited one is released immediately.
		_ = f.Close()
		released[i] = true
	}

	return conns, nil
}

// closePacketConnsAndFiles releases every descriptor received from systemd on
// a fatal conversion error. Errors are ignored so that one failed close cannot
// prevent releasing the remaining descriptors.
func closePacketConnsAndFiles(conns []net.PacketConn, files []*os.File, released []bool) {
	for _, c := range conns {
		if c != nil {
			_ = c.Close()
		}
	}
	for i, f := range files {
		if f != nil && (i >= len(released) || !released[i]) {
			_ = f.Close()
		}
	}
}
