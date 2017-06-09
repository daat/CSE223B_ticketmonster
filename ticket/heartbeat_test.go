package ticket_test

import (
	"testing"
	"runtime/debug"
	"fmt"
    "strconv"
    "time"
    "log"

	"ticketmonster/storage"
	"ticketmonster/ticket"
)


func TestHeartBeat(t *testing.T) {
    n := 3
    servers := make([]*ticket.TicketServer, 0, n)
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

    /*run := func(i int, poolready chan bool) {
        fmt.Printf("boot server %d\n", i)
        if i > len(tserver_out) {
            fmt.Errorf("index out of range: %d", i)
        }
        ret := new(ticket.TicketServerConfig)
        ret.Backs = primaryAddrs
        ret.OutAddr = tserver_out[i]
        ret.InAddrs = tserver_in
        ret.This = i
        ret.Id = strconv.Itoa(i)
        c := make(chan bool)
        ret.Ready = c
        ts := ticket.NewTicketServer(ret)
        b := <-c
        log.Printf("r\n")
        if b {
            log.Printf("ticket server serving on %s", ret.OutAddr)
            servers = append(servers, &ts)
            if poolready!=nil{
                poolready <- true
            }
        } else {
            log.Printf("ticket server on %s init failed", ret.OutAddr)
        }

    }


    for i := 0; i < n; i++ {
        go run(i, nil)
    }

    //CheckServerConcur(t, servers[0])

    time.Sleep(time.Minute * 1)

    var l storage.List

    servers[0].GetAllTickets(true, &l)

    if len(l.L) == 0 {
        fmt.Printf("len 0 \n")
    }

    for i, v := range l.L{
        fmt.Printf("%d: %s\n", i, v)
    }
    */


    run := func(ind int, ready chan bool) {
        ts := ticket.NewTicketServer(&ticket.TicketServerConfig{
            Backs: primaryAddrs,
            OutAddr: tserver_out[ind],
            InAddrs: tserver_in,
            This:  ind,
            Id:    strconv.Itoa(ind),
            Ready: ready,
        })

        servers = append(servers, &ts)
        log.Printf("ticket server 0 serving")
    }

    poolready := make(chan bool)
    go run(0, poolready)
    if <-poolready{
        log.Printf("pool ready")
    }

    ready1 := make(chan bool)
    for i := 1; i < n; i++ {
        go run(i, ready1)
    }
    for i := 1; i < n; i++ {
        <- ready1
    }
    log.Printf("ticket server all serving")

    time.Sleep(time.Second * 2)

    var l storage.List

    servers[0].GetAllTickets(true, &l)

    if len(l.L) == 0 {
        fmt.Printf("len 0 \n")
    }

    for i, v := range l.L{
        fmt.Printf("%d: %s\n", i, v)
    }

    ss := 0
    servers[0].GetTotalSale(true, &ss)
    fmt.Printf("Sell: %d \n", ss)
}
