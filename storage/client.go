package storage

import (
	"net/rpc"
)

type client struct {
	addr string
}

//var _ Storage = new(client)

// KeyString interface
func (self *client) Get(key string, value *string) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("Storage.Get", key, value)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *client) Set(kv *KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("Storage.Set", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()

}

func (self *client) Keys(p *Pattern, list *List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("Storage.Keys", p, list)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()

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

func (self *client) ListRemove(kv *KeyValue, n *int) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Storage.ListRemove", kv, n)
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

func (self *client) GetTicket(useless bool, succ *bool) error {
	// do nothing
	return nil
}
