package storage

import (
	"net"
    "fmt"
    "strings"
)

type BackupBackend struct {
    store Store
    primary *PrimaryBackend
    this int
}

func (self *BackupBackend) Handle(req *Request) *Response {
    res := Response{}
    var e error
    if req.OP == "ListAppend" {
        e = self.ListAppend(req.KV, &res.Succ)
    } else if req.OP == "ListGet" {
        e = self.ListGet(req.KV.Key, &res.L)
    } else if req.OP == "StartServing" {
        var id int
        var clock uint64
        fmt.Sscanf(req.KV.Key, "%d", &id)
        e = self.StartServing(id, &clock)
        res.L.L = []string{fmt.Sprintf("%v", clock)}
    } else {
        e = fmt.Errorf("no this operation")
    }
    if e != nil {
        res.Err = e.Error()
    }
    return &res
}

func (self *BackupBackend) export(addr string) {
    l, ee := net.Listen("tcp", addr)
	if ee != nil {
        fmt.Println(ee)
        return
	}
	for {
        conn, e := l.Accept()
        if e != nil {
            fmt.Println(e)
            return
        }
        demux := ConnDemux{Conn: conn, Server: self}
        go demux.Serve()
    }
}

func (self *BackupBackend) Clock(atLeast uint64, ret *uint64) error {
    return self.store.Clock(atLeast, ret)
}

// Get the list.
func (self *BackupBackend) ListGet(key string, list *List) error {
    return self.store.ListGet(key, list)
}

// Append a string to the list. Set succ to true when no error.
func (self *BackupBackend) ListAppend(kv *KeyValue, succ *bool) error {
    id := self.primary.getID(kv.Key)
    self.primary.statusLock.Lock()
    if !self.primary.alive[id] {
        self.primary.statusLock.Unlock()
        return fmt.Errorf("it's dead")
    }
    self.primary.statusLock.Unlock()

    arr := strings.SplitN(kv.Value, ",", 2)
    var clock uint64
    fmt.Sscanf(arr[0], "%25d", &clock)
    self.store.Clock(clock+1, &clock)
    return self.store.ListAppend(kv, succ)
}

func (self *BackupBackend) StartServing(id int, clock *uint64) error {
    self.primary.statusLock.Lock()
    self.primary.alive[id] = true
    fmt.Printf("%d: %d alive\n", self.this, id)

    self.primary.clients[id] = NewBackupClient(self.primary.bc.BackupAddrs[id])

    if id == (self.this - 1) % len(self.primary.alive) {
        self.primary.moveToPrimary[id] = true
    } else if id == (self.this + 1) % len(self.primary.alive) {
        self.primary.moveToBackup[id] = true
    }

    self.primary.statusLock.Unlock()
    self.store.Clock(0, clock)
    go func() {
        /*migration*/
        if id == (self.this - 1) % len(self.primary.alive) {
            self.primary.replicate(id, false)
            self.primary.moveToPrimary[id] = false
        } else if id == (self.this + 1) % len(self.primary.alive) {
            self.primary.moveToBackup[id] = false
        }
    }()
    return nil
}
