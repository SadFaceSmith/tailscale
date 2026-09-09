// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

//go:build !ts_omit_serve && !ts_omit_usermetrics

package ipnlocal

import (
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/netmap"
	"tailscale.com/util/usermetric"
	"tailscale.com/wgengine/magicsock"
)

func counterValue(m *usermetric.MultiLabelMap[serveLabels], svc string, path magicsock.Path) int64 {
	v, _ := m.Get(serveLabels{Service: svc, Path: path}).(*expvar.Int)
	if v == nil {
		return -1
	}
	return v.Value()
}

func TestServiceMeteredConn(t *testing.T) {
	b := newTestBackend(t)

	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	wrapped := b.meteredConnForService(serverSide, tailcfg.ServiceName("svc:foo"), netip.AddrPort{}).(*serviceMeteredConn)

	// Reuse the connection to check that path changes create separate series.
	for _, path := range []magicsock.Path{
		magicsock.PathDERP,
		magicsock.PathDirectIPv4,
		magicsock.PathDirectIPv6,
		magicsock.PathPeerRelayIPv4,
		magicsock.PathPeerRelayIPv6,
		"unknown",
	} {
		t.Run(string(path), func(t *testing.T) {
			wrapped.peerPath = func() magicsock.Path {
				if path == "unknown" {
					return ""
				}
				return path
			}

			const inboundPayload = "hello from client"
			writeDone := make(chan struct{})
			go func() {
				clientSide.Write([]byte(inboundPayload))
				close(writeDone)
			}()
			buf := make([]byte, len(inboundPayload))
			if _, err := io.ReadFull(wrapped, buf); err != nil {
				t.Fatalf("read: %v", err)
			}
			<-writeDone
			if got := counterValue(b.metrics.serveBytesInbound, "svc:foo", path); got != int64(len(inboundPayload)) {
				t.Errorf("inbound = %d; want %d", got, len(inboundPayload))
			}

			// Deliberately a different length than inboundPayload so a backwards
			// inbound/outbound wiring can't pass.
			const outboundPayload = "hello from the server side"
			writeDone = make(chan struct{})
			go func() {
				wrapped.Write([]byte(outboundPayload))
				close(writeDone)
			}()
			buf = make([]byte, len(outboundPayload))
			if _, err := io.ReadFull(clientSide, buf); err != nil {
				t.Fatalf("read: %v", err)
			}
			<-writeDone
			if got := counterValue(b.metrics.serveBytesOutbound, "svc:foo", path); got != int64(len(outboundPayload)) {
				t.Errorf("outbound = %d; want %d", got, len(outboundPayload))
			}
		})
	}

	w := httptest.NewRecorder()
	b.UserMetricsRegistry().Handler(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, metric := range []string{"inbound", "outbound"} {
		want := fmt.Sprintf(`tailscaled_serve_%s_bytes_total{service="svc:foo",path="direct_ipv4"}`, metric)
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
}

func TestServiceMeteredConnLabelKeepsPrefix(t *testing.T) {
	b := newTestBackend(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	wrapped := b.meteredConnForService(c1, tailcfg.ServiceName("svc:my-app"), netip.AddrPort{})
	go c2.Write([]byte("x"))
	io.ReadFull(wrapped, make([]byte, 1))
	if v := counterValue(b.metrics.serveBytesInbound, "svc:my-app", "unknown"); v != 1 {
		t.Errorf("inbound for service=\"svc:my-app\" = %d; want 1", v)
	}
	if v := counterValue(b.metrics.serveBytesInbound, "my-app", "unknown"); v != -1 {
		t.Errorf("counter unexpectedly present under prefix-stripped name; got %d", v)
	}
}

func TestServiceMeteredConnPeerPath(t *testing.T) {
	b := newTestBackend(t)
	srcAddr := netip.MustParseAddrPort("100.64.0.2:12345")
	node := &tailcfg.Node{
		ID:        2,
		Key:       key.NewNode().Public(),
		DiscoKey:  key.NewDisco().Public(),
		HomeDERP:  1,
		Addresses: []netip.Prefix{netip.PrefixFrom(srcAddr.Addr(), 32)},
	}
	b.currentNode().SetNetMap(&netmap.NetworkMap{
		SelfNode: (&tailcfg.Node{
			ID:        1,
			Addresses: []netip.Prefix{netip.MustParsePrefix("100.64.0.1/32")},
		}).View(),
		Peers: []tailcfg.NodeView{node.View()},
	})
	b.MagicConn().UpsertPeer(node.View())

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	wrapped := b.meteredConnForService(c1, "svc:foo", srcAddr).(*serviceMeteredConn)
	if got := wrapped.labels().Path; got != magicsock.PathDERP {
		t.Errorf("path = %q; want %q", got, magicsock.PathDERP)
	}
	b.MagicConn().RemovePeer(node.ID)
	if got := wrapped.labels().Path; got != "unknown" {
		t.Errorf("path after peer removal = %q; want unknown", got)
	}
	if got := b.meteredConnForService(c1, "", srcAddr); got != c1 {
		t.Error("non-Service connection was wrapped")
	}
	for _, addr := range []string{"100.64.0.1:12345", "100.64.0.3:12345"} {
		wrapped := b.meteredConnForService(c1, "svc:foo", netip.MustParseAddrPort(addr)).(*serviceMeteredConn)
		if got := wrapped.labels().Path; got != "unknown" {
			t.Errorf("path for %s = %q; want unknown", addr, got)
		}
	}
}

type partialServiceConn struct {
	net.Conn
	n int
}

func (c partialServiceConn) Read([]byte) (int, error)  { return c.n, io.EOF }
func (c partialServiceConn) Write([]byte) (int, error) { return c.n, io.ErrShortWrite }

func TestServiceMeteredConnPartialIO(t *testing.T) {
	b := newTestBackend(t)
	for _, n := range []int{0, 3} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			svc := tailcfg.ServiceName(fmt.Sprintf("svc:partial-%d", n))
			wrapped := b.meteredConnForService(partialServiceConn{n: n}, svc, netip.AddrPort{}).(*serviceMeteredConn)
			var pathCalls int
			wrapped.peerPath = func() magicsock.Path {
				pathCalls++
				return magicsock.PathDirectIPv6
			}
			buf := make([]byte, 10)
			if got, err := wrapped.Read(buf); got != n || err != io.EOF {
				t.Errorf("Read = %d, %v; want %d, EOF", got, err, n)
			}
			if got, err := wrapped.Write(buf); got != n || err != io.ErrShortWrite {
				t.Errorf("Write = %d, %v; want %d, ErrShortWrite", got, err, n)
			}
			want, wantCalls := int64(n), 2
			if n == 0 {
				want, wantCalls = -1, 0
			}
			for _, m := range []*usermetric.MultiLabelMap[serveLabels]{b.metrics.serveBytesInbound, b.metrics.serveBytesOutbound} {
				if got := counterValue(m, svc.String(), magicsock.PathDirectIPv6); got != want {
					t.Errorf("counter = %d; want %d", got, want)
				}
			}
			if pathCalls != wantCalls {
				t.Errorf("path lookups = %d; want %d", pathCalls, wantCalls)
			}
		})
	}
}
