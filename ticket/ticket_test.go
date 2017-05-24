package ticket_test

import (
	"testing"
	"ticketmonster/storage"
)


func NewBinClient(backs []string) storage.BinStorage {
	bc := &storage.binStorageClient{backs: backs}
	bc.Init()
	return bc
}

// Makes a front end that talks to backend
func NewFront(backs []string, id string) ticketserver {
	s := NewBinClient(backs)
	ts = &ticketserver{bc: s, ticketserver_id: id}
	ts.Init(1000) // initialize tickets
	return ts
}


func TestTicket(t *testing.T) {
    n := 3
    primaryAddrs := make([]string, n)
    backupAddrs := make([]string, n)
    for i := 0; i < n; i++ {
        primaryAddrs[i] = fmt.Sprintf("localhost:%d", 15000+i)
        backupAddrs[i] = fmt.Sprintf("localhost:%d", 16000+i)
    }
    backs := make([]storage.PrimaryBackend, n)
    var e error
    for i := 0; i < n; i++ {
        e = backs[i].Serve(&storage.BackConfig{PrimaryAddrs: primaryAddrs, BackupAddrs: backupAddrs, This: i})
        if e != nil {
            debug.PrintStack()
			t.Fatal(e)
        }
    }

	tserver := NewFront(primaryAddrs, "0")

	CheckServerConcur(t, tserver)
}