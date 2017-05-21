package storage

import (
	"net"
	"net/http"
	"net/rpc"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) Storage {
	return &client{addr: addr}
}

// Serve as a backend based on the given configuration
func ServeBack(b *BackConfig) error {
	l, e := net.Listen("tcp", b.Addr)
	if e != nil {
		b.Ready <- false
		return e
	}
	server := rpc.NewServer()
	e = server.RegisterName("Storage", b.Store)
	if e != nil {
		b.Ready <- false
		return e
	}

	if b.Ready != nil {
		b.Ready <- true
	}
	return http.Serve(l, server)
}
