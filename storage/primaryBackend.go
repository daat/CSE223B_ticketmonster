package storage

import (
	"net"
    "fmt"
    "sync"
    "strings"
    "strconv"
    "sort"
)

type PrimaryBackend struct {
    bc *BackConfig
    clients []CommandStorage
    alive []bool
    moveToPrimary []bool
    moveToBackup []bool
    store Store
    backup *BackupBackend
    this int
    statusLock sync.Mutex
}

func (self *PrimaryBackend) getID(key string) int {
    bin_name := Unescape(strings.SplitN(key, "::", 2)[0])
    id, e := strconv.Atoi(bin_name)
    if e != nil || id < 0 || id >= len(self.alive) {
        id = Hash(bin_name) % len(self.alive)
    }
    return id
}

func (self *PrimaryBackend) export(addr string) {
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

// Serve as a backend based on the given configuration
func (self *PrimaryBackend) Serve(b *BackConfig) error {
    self.store = *NewStore()
    self.this = b.This
    self.backup = &BackupBackend{store: self.store, primary: self, this: self.this}

	go self.export(b.PrimaryAddrs[b.This])
    e := self.backup.export(b.BackupAddrs[b.This])
    if e != nil {
        return e
    }
    self.alive = make([]bool, len(b.BackupAddrs))
    self.moveToPrimary = make([]bool, len(b.BackupAddrs))
    self.moveToBackup = make([]bool, len(b.BackupAddrs))
    self.clients = make([]CommandStorage, 0, len(b.BackupAddrs))
    var clock uint64
    for i, v := range b.BackupAddrs {
		cl := NewBackupClient(v)
		self.clients = append(self.clients, cl)
        if i != self.this {
            if cl.StartServing(self.this, &clock) == nil {
                self.store.Clock(clock+1, &clock)
                self.statusLock.Lock()
                self.alive[i] = true
                self.statusLock.Unlock()
            }
        }
	}
    self.statusLock.Lock()
    self.alive[self.this] = true
    self.statusLock.Unlock()
    fmt.Printf("start %d\n", self.this)
    return nil
}

func (self *PrimaryBackend) Handle(req *Request) *Response {
    res := Response{}
    var e error
    if req.OP == "ListAppend" {
        e = self.ListAppend(req.KV, &res.Succ)
    } else {
        e = fmt.Errorf("no this operation")
    }
    if e != nil {
        res.Err = e.Error()
    }
    return &res
}

func (self *PrimaryBackend) Clock(atLeast uint64, ret *uint64) error {
    return self.store.Clock(atLeast, ret)
}

// Get the list.
func (self *PrimaryBackend) ListGet(key string, list *List) error {
    self.statusLock.Lock()
    if !self.alive[self.this] {
        self.statusLock.Unlock()
        return fmt.Errorf("not ready")
    }
    self.statusLock.Unlock()

    var l1, l2 List
    e := self.store.ListGet(key, &l1)
    if e != nil {
        return e
    }

    now := (self.this + 1) % len(self.clients)
    for now != self.this {

        if !self.alive[now] {
            now = (now + 1) % len(self.clients)
            e = fmt.Errorf("%d: %d not alive", self.this, now)
            continue
        }

        e = self.clients[now].ListGet(key, &l2)
        if e != nil {
            self.statusLock.Lock()
            self.alive[now] = false
            self.statusLock.Unlock()
            now = (now + 1) % len(self.clients)
            go func() {
                /*migration*/
            }()
        } else {
            break
        }
    }

    if e != nil {
        return e
    }

    sort.Strings(l1.L)
    sort.Strings(l2.L)
    if len(l1.L) == 0 {
        list.L = l2.L
    } else if len(l2.L) == 0 {
        list.L = l1.L
    } else {
        var mc, clk uint64
        arr := strings.SplitN(l1.L[0], ",", 2)
        fmt.Sscanf(arr[0], "%25d", &mc)
        logs := make([]string, 0, len(l1.L))
        for _, log := range l2.L {
            arr = strings.SplitN(log, ",", 2)
            fmt.Sscanf(arr[0], "%25d", &clk)
            if clk >= mc {
                break
            }
            logs = append(logs, arr[1])
        }
        for _, log := range l1.L {
            arr = strings.SplitN(log, ",", 2)
            logs = append(logs, arr[1])
        }
        list.L = logs
    }

    return nil
}

// Append a string to the list. Set succ to true when no error.
func (self *PrimaryBackend) ListAppend(kv *KeyValue, succ *bool) error {
    // fmt.Printf("%d: ListAppend\n", self.this)
    self.statusLock.Lock()
    if !self.alive[self.this] {
        self.statusLock.Unlock()
        return fmt.Errorf("not ready")
    }
    self.statusLock.Unlock()

    id := self.getID(kv.Key)
    if id != self.this {
        self.statusLock.Lock()
        if self.alive[id] {
            if self.moveToPrimary[id] {
                // The original primary is up, ask client to go back
                self.statusLock.Unlock()
                return fmt.Errorf("prev alive")
            } else {
                // the original primary is down
                // fmt.Printf("%d: %d primary not alive\n", self.this, id)
                self.alive[id] = false

                go func() {
                    /*migration*/
                }()
            }
        }
        self.statusLock.Unlock()
    }


    var clock uint64
    self.store.Clock(clock, &clock)
    kv.Value = fmt.Sprintf("%25d,%s", clock, kv.Value)

    var e error

    now := (self.this + 1) % len(self.clients)
    for now != self.this {
        // fmt.Printf("%d: backup%d_ListAppend\n", self.this, now)
        if !self.alive[now] {
            e = fmt.Errorf("%d: %d not alive", self.this, now)
            // fmt.Println(e)
            now = (now + 1) % len(self.clients)
            continue
        }
        e = self.clients[now].ListAppend(kv, succ)
        // fmt.Printf("%d: %v\n", self.this, e)
        if e != nil {
            self.statusLock.Lock()
            e = fmt.Errorf("%d: %d %v", self.this, now, e)
            // fmt.Println(e)
            self.alive[now] = false
            self.statusLock.Unlock()
            now = (now + 1) % len(self.clients)
            go func() {
                /*migration*/
            }()
        } else {
            break
        }

    }

    if e != nil {
        return e
    }
    return self.store.ListAppend(kv, succ)
}
