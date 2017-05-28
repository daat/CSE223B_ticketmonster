package storage

import (
	"net/rpc"
)

type backup_client struct {
	addr string
}

func NewBackupClient(addr string) CommandStorage {
	return &backup_client{addr: addr}
}

// KeyList interface
func (self *backup_client) ListGet(key string, list *List) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	list.L = nil
	// perform the call
	e = conn.Call("CommandStorage.ListGet", key, list)
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

func (self *backup_client) ListAppend(kv *KeyValue, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("CommandStorage.ListAppend", kv, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *backup_client) Clock(atLeast uint64, ret *uint64) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("CommandStorage.Clock", atLeast, ret)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *backup_client) StartServing(id int, clock *uint64) error {
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
