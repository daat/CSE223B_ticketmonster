package ticket_test

import (
	"testing"
	"runtime/debug"
	"fmt"
    "strconv"
    "time"

	"ticketmonster/storage"
	"ticketmonster/ticket"
)


func TestHeartBeat(t *testing.T) {
    n := 3
    primaryAddrs := make([]string, n)
    backupAddrs := make([]string, n)
    tserver_out := make([]string, n)
    tserver_in := make([]string, n)
    for i := 0; i < n; i++ {
        primaryAddrs[i] = fmt.Sprintf("localhost:%d", 15000+i)
        backupAddrs[i] = fmt.Sprintf("localhost:%d", 16000+i)
        tserver_out[i] = fmt.Sprintf("localhost:%d", 17000+i)
        tserver_in[i] = fmt.Sprintf("localhost:%d", 18000+i)
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

    run := func(i int) {
        if i > len(tserver_out) {
            fmt.Errorf("index out of range: %d", i)
        }
        ret := new(ticket.TicketServerConfig)
        ret.Backs = primaryAddrs
        ret.OutAddr = tserver_out[i]
        ret.InAddrs = tserver_in
        ret.This = i
        ret.Id = strconv.Itoa(i)
        ts := ticket.NewTicketServer(ret)
        
        ts.InitPool()
        ts.Init()

        fmt.Printf("ticket server serving on %s\n", tserver_out[i])
    }

    for i := 0; i < n; i++ {
        go run(i)
    }

	time.Sleep(time.Minute * 2)
}
