package storage

import (
	"net"
	"net/http"
	"net/rpc"
    "fmt"
    "strings"
)

type BackupBackend struct {
    store Store
    primary *PrimaryBackend
    this int
}

func (self *BackupBackend) export(addr string) error {
    l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	server := rpc.NewServer()
	e = server.RegisterName("CommandStorage", self)
	if e != nil {
		return e
	}

	go http.Serve(l, server)
    return nil
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
    // fmt.Printf("%d: %d alive\n", self.this, id)
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
            self.primary.moveToPrimary[id] = false
        } else if id == (self.this + 1) % len(self.primary.alive) {
            self.primary.moveToBackup[id] = false
        }
    }()
    return nil
}
