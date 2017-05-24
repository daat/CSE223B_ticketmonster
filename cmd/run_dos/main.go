package main

import (
	"ticketmonster"
    "ticketmonster/ticket"
    "ticketmonster/storage"
    "ticketmonster/client"
    "time"
    "fmt"
)

func main() {
    n := 1000
    rc, _ := ticketmonster.LoadRC("bins.rc")
    var front client.Front
    front.Init(rc.TicketServers)

    ch := make(chan int64, n)
    for i := 0; i < n; i++ {
        go func() {
            var succ bool
            t := time.Now().UnixNano()
            e := front.BuyTicket(true, &succ)
            if e!= nil {
                fmt.Errorf("%v %v\n", e, i)
            }
            ch <- (time.Now().UnixNano() - t)
        }()
    }
    for i := 0; i < n; i++ {
        fmt.Println(<- ch)
    }
}
