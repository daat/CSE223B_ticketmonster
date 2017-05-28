package storage

import (
	"net"
    "fmt"
)

type primary_client struct {
	addr string
    conn net.Conn
    mux ConnMux
}

// Creates an RPC client that connects to addr.
func NewPrimaryClient(addr string) Storage {
	cli := primary_client{addr: addr}
    e :=cli.init()
    if e != nil {
        fmt.Println(e)
        return nil
    }
    return &cli
}

func (self *primary_client) init() error {
    conn, e := net.Dial("tcp", self.addr)
    if e != nil {
        return e
    }
    self.conn = conn
    self.mux = ConnMux{Conn: self.conn}
    self.mux.Init()
    return nil
}

// KeyList interface
func (self *primary_client) ListGet(key string, list *List) error {
	return nil
}

func (self *primary_client) ListAppend(kv *KeyValue, succ *bool) error {

	// perform the call

    req := Request{OP: "ListAppend", KV: kv}
    res := self.mux.Handle(&req)
    if res.Err != "" {
        return fmt.Errorf(res.Err)
    }
    *succ = res.Succ
	return nil
}
