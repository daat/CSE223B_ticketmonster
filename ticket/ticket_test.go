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

	tserver := ticket.NewTicketServer(primaryAddrs, "0", "localhost:17000")

	CheckServerConcur(t, tserver)
}