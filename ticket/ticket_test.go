package ticket_test

import (
	"testing"
	"runtime/debug"
	"fmt"

	"ticketmonster/storage"
	"ticketmonster/ticket"
)


func NewBinClient(backs []string) storage.BinStorage {
	bc := &storage.BinStorageClient{Backs: backs}
	bc.Init()
	return bc
}

// Makes a front end that talks to backend
func NewFront(backs []string, id string) ticket.TicketServer {
	s := NewBinClient(backs)
	ts := ticket.TicketServer{Bc: s, Ticketserver_id: id}
	out_port := fmt.Sprintf("localhost:%d", 17000)
	ts.Init(1000, out_port) // initialize tickets: tickets, out_port
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