package storage

import (
    "encoding/gob"
    "sync"
    "net"
    "fmt"
)

type Request struct {
    Seq uint64
    OP string
    KV *KeyValue
}

type ReqRes struct {
    Req *Request
    Res chan Response
}

type Response struct {
    Seq uint64
    Succ bool
    L List
    Err string
}

type Handler interface {
    Handle(req *Request) *Response
}

type ConnDemux struct {
    Conn net.Conn
    Server Handler
}

func (self *ConnDemux) Serve() {
    dec := gob.NewDecoder(self.Conn)
    enc := gob.NewEncoder(self.Conn)
    for {
        req := Request{}
        e := dec.Decode(&req)
        // fmt.Printf("decode:: %v\n", e)
        if e != nil {
            // go func(err error, Seq uint64) {
            //     res := Response{Err: err.Error(), Seq: Seq}
            //     e = enc.Encode(res)
            //     if e != nil {
            //         fmt.Printf("deencode:: %v\n", e)
            //     }
            // }(e, req.Seq)
            fmt.Println("connection close")
            self.Conn.Close()
            return
        } else {
            go func(r *Request) {
                res := self.Server.Handle(r)
                res.Seq = r.Seq
                e = enc.Encode(res)
                if e != nil {
                    fmt.Printf("encode:: %v\n", e)
                }
            }(&req)
        }
    }
}

type ConnMux struct {
    Conn net.Conn
    queue chan ReqRes
    Seq uint64
    chs map[uint64]chan Response
    lock sync.Mutex
}

func (self *ConnMux) Init() error {
    self.queue = make(chan ReqRes, 2048)
    self.chs = make(map[uint64]chan Response)
    go self.sender()
    go self.receiver()
    return nil
}

func (self *ConnMux) sender() {
    enc := gob.NewEncoder(self.Conn)
    for {
        req_res := <-self.queue
        req_res.Req.Seq = self.Seq
        self.lock.Lock()
        self.chs[self.Seq] = req_res.Res
        self.lock.Unlock()
        go func(req *Request){
            e := enc.Encode(req)
            // fmt.Printf("send: %v\n", req.Seq)
            if e != nil {
                self.lock.Lock()
                self.chs[req.Seq] <- Response{Err: e.Error()}
                delete(self.chs, req.Seq)
                self.lock.Unlock()
            }
        }(req_res.Req)
        self.Seq++
    }
}

func (self *ConnMux) receiver() {
    dec := gob.NewDecoder(self.Conn)
    for {
        res := Response{}
        res.L.L = nil
        e := dec.Decode(&res)
        // fmt.Printf("recv: %v", e)
        if e != nil {
            self.lock.Lock()
            res.Err = e.Error()
            for k, v := range self.chs {
                v <- res
                delete(self.chs, k)
            }
            self.lock.Unlock()
        }
        if res.L.L == nil {
            res.L.L = []string{}
        }
        self.lock.Lock()
        self.chs[res.Seq] <- res
        delete(self.chs, res.Seq)
        self.lock.Unlock()
    }
}

func (self *ConnMux) Handle(req *Request) Response {
    ch := make(chan Response)
    rr := ReqRes{Req: req, Res: ch}
    self.queue <- rr
    return <-ch
}
