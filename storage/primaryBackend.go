package storage

import (
	"net"
	"net/http"
	"net/rpc"
    "fmt"
    "sync"
    "strings"
    "strconv"
)

type PrimaryBackend struct {
    bc *BackConfig
    clients []CommandStorage
    alive []bool
    migrating []bool
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

func (self *PrimaryBackend) export(addr string) error {
    l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	server := rpc.NewServer()
	e = server.RegisterName("Storage", *self)
	if e != nil {
		return e
	}

	go http.Serve(l, server)
    return nil
}

// Serve as a backend based on the given configuration
func (self *PrimaryBackend) Serve(b *BackConfig) error {
    self.store = *NewStore()
    self.this = b.This
    self.backup = &BackupBackend{store: self.store, primary: self, this: self.this}
	e := self.export(b.PrimaryAddrs[b.This])
    if e != nil {
        return e
    }
    e = self.backup.export(b.BackupAddrs[b.This])
    if e != nil {
        return e
    }
    self.alive = make([]bool, len(b.BackupAddrs))
    self.migrating = make([]bool, len(b.BackupAddrs))
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
    return nil
}

func (self *PrimaryBackend) Clock(atLeast uint64, ret *uint64) error {
    return self.store.Clock(atLeast, ret)
}

// Get the list.
func (self *PrimaryBackend) ListGet(key string, list *List) error {

    return nil
}

// Append a string to the list. Set succ to true when no error.
func (self *PrimaryBackend) ListAppend(kv *KeyValue, succ *bool) error {
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
            if self.migrating[id] {
                // The original primary is up, ask client to go back
                self.statusLock.Unlock()
                return fmt.Errorf("prev alive")
            } else {
                // the original primary is down
                self.alive[id] = false
                self.statusLock.Unlock()
                go func() {
                    /*migration*/
                }()
            }
        }
    }


    var clock uint64
    self.store.Clock(clock, &clock)
    kv.Value = fmt.Sprintf("%25d,%s", clock, kv.Value)
    e := self.store.ListAppend(kv, succ)
    if e != nil {
        return e
    }

    now := (self.this + 1) % len(self.clients)

    for now != self.this {

        if !self.alive[now] {
            now = (now + 1) & len(self.clients)
            continue
        }

        e = self.clients[now].ListAppend(kv, succ)
        if e != nil {
            self.statusLock.Lock()
            self.alive[now] = false
            self.statusLock.Unlock()
            go func() {
                /*migration*/
            }()
        }
        break
    }


    if e != nil {
        rn := 0
        self.store.ListRemove(kv, &rn)
        return e
    }
    return nil
}
