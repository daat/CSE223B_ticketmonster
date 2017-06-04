package storage

import (
    "net"
    "fmt"
)

type backup_client struct {
    addr string
    conn net.Conn
    mux ConnMux
    connected bool
}

func NewBackupClient(addr string) CommandStorage {
    cli := backup_client{addr: addr}
    e :=cli.init()
    if e != nil {
        fmt.Println(e)
    }
    return &cli
}

func (self *backup_client) init() error {
    conn, e := net.Dial("tcp", self.addr)
    if e != nil {
        return e
    }
    self.conn = conn
    self.mux = ConnMux{Conn: self.conn}
    self.mux.Init()
    self.connected = true
    return nil
}

// KeyList interface
func (self *backup_client) ListGet(key string, list *List) error {
    if !self.connected {
        e := self.init()
        if e != nil {
            return e
        }
    }
    req := Request{OP: "ListGet", KV: &KeyValue{Key: key, Value: ""}}
    res := self.mux.Handle(&req)
    if res.Err != "" {
        return fmt.Errorf(res.Err)
    }
    list.L = res.L.L
	return nil
}

func (self *backup_client) ListAppend(kv *KeyValue, succ *bool) error {
    if !self.connected {
        e := self.init()
        if e != nil {
            return e
        }
    }
    req := Request{OP: "ListAppend", KV: kv}
    res := self.mux.Handle(&req)
    if res.Err != "" {
        return fmt.Errorf(res.Err)
    }
    *succ = res.Succ
	return nil
}

func (self *backup_client) StartServing(id int, clock *uint64) error {
    if !self.connected {
        e := self.init()
        if e != nil {
            return e
        }
    }
    req := Request{OP: "StartServing", KV: &KeyValue{Key: fmt.Sprintf("%v", id), Value: ""}}
    res := self.mux.Handle(&req)
    if res.Err != "" {
        return fmt.Errorf(res.Err)
    }
    fmt.Sscanf(res.L.L[0], "%v", clock)
	return nil
}
