package main

import (
	"net/rpc"
	"storage"
)

type client struct {
	addr string
}

// KeyList interface
func (self *client) ListGet(key string, list *trib.List) error {
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

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error {
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
