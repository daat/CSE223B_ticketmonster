package ticket_test

import (
	"testing"
	"runtime/debug"
	"fmt"

	"ticketmonster/storage"
	"ticketmonster/ticket"
)


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

    ret := new(ticket.TicketServerConfig)
    ret.Backs = primaryAddrs
    ret.OutAddr = "localhost:17000"
    ret.InAddrs = []string{"localhost:17001"}
    ret.This = 0
    ret.Id = "0"

	tserver := ticket.NewTicketServer(ret)
    tserver.InitPool()
    tserver.Init(10000)
    

	CheckServerConcur(t, &tserver)
}
