package ticket

import (
	"net"
    "encoding/gob"
	"ticketmonster/storage"
    "fmt"
)

type Frontier struct {
	addr string
}


func NewFrontier(addr string) Window{
	return &Frontier{addr: addr}
}

func (self *Frontier) BuyTicket(in *BuyInfo, succ *bool) error{
    req := storage.Request{OP: "BuyTicket", KV: &storage.KeyValue{Key: in.Uid, Value: fmt.Sprintf("%v", in.N)}}
    conn, e := net.Dial("tcp", self.addr)
    if e != nil {
        conn.Close()
        return e
    }
    dec := gob.NewDecoder(conn)
    enc := gob.NewEncoder(conn)
    e = enc.Encode(req)
    if e != nil {
        return e
    }
    res := storage.Response{}
    e = dec.Decode(&res)
    if e != nil {
        conn.Close()
        return e
    }
    if res.Err != "" {
        conn.Close()
        return fmt.Errorf(res.Err)
    }
    *succ = res.Succ
    return conn.Close()
}

func (self *Frontier) GetLeftTickets(useless bool, n *int) error{
    req := storage.Request{OP: "GetLeftTickets"}
    conn, e := net.Dial("tcp", self.addr)
    if e != nil {
        return e
    }
    dec := gob.NewDecoder(conn)
    enc := gob.NewEncoder(conn)
    e = enc.Encode(req)
    if e != nil {
        conn.Close()
        return e
    }
    res := storage.Response{}
    e = dec.Decode(&res)
    if e != nil {
        conn.Close()
        return e
    }
    if res.Err != "" {
        conn.Close()
        return fmt.Errorf(res.Err)
    }
    fmt.Sscanf(res.L.L[0], "%v", n)
    return conn.Close()
}

func (self *Frontier) GetAllTickets(useless bool, ret *storage.List) error{
    req := storage.Request{OP: "GetAllTickets"}
    conn, e := net.Dial("tcp", self.addr)
    if e != nil {
        return e
    }
    dec := gob.NewDecoder(conn)
    enc := gob.NewEncoder(conn)
    e = enc.Encode(req)
    if e != nil {
        conn.Close()
        return e
    }
    res := storage.Response{}
    res.L.L = nil
    e = dec.Decode(&res)
    if e != nil {
        conn.Close()
        return e
    }
    if res.Err != "" {
        conn.Close()
        return fmt.Errorf(res.Err)
    }
    if res.L.L == nil {
        res.L.L = []string{}
    }
    ret.L = res.L.L
    return conn.Close()
}



