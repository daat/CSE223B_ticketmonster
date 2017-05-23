package storage

import (
	"net/rpc"
)

type client struct {
	addr string
}

// Creates an RPC client that connects to addr.
func NewClient(addr string) Storage {
	return &client{addr: addr}
}

func NewBackupClient(addr string) CommandStorage {
	return &client{addr: addr}
}

// KeyList interface
func (self *client) ListGet(key string, list *List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil
	// perform the call
	e = conn.Call("Storage.ListGet", key, list)
	if e != nil {
		conn.Close()
		return e
	}
	if list.L == nil {
		list.L = []string{}
	}
	// close the connection
	return conn.Close()
}

func (self *client) ListAppend(kv *KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.ListAppend", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}


func (self *client) ListKeys(p *Pattern, list *List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil
	// perform the call
	e = conn.Call("Storage.ListKeys", p, list)
	if e != nil {
		conn.Close()
		return e
	}

	if list.L == nil {
		list.L = []string{}
	}

	// close the connection
	return conn.Close()
}

func (self *client) Clock(atLeast uint64, ret *uint64) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.Clock", atLeast, ret)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) StartServing(id int, clock *uint64) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("CommandStorage.StartServing", id, clock)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}
