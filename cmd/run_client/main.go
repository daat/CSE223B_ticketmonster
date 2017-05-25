package main

import (
	"ticketmonster"
    "ticketmonster/ticket"
    "ticketmonster/client"
    "time"
    "fmt"
)

func main() {
    n := 1000
    rc, _ := ticketmonster.LoadRC("bins.rc")
    var front client.Front
    front.Init(rc.TicketServers)
    info := &ticket.BuyInfo{Uid: "uuu", N: 1}
    ch := make(chan int64, n)
    for i := 0; i < n; i++ {
        go func() {
            var succ bool
            info.Uid = fmt.Sprintf("uuu%d", i)
            t := time.Now().UnixNano()
            e := front.BuyTicket(info, &succ)
            if e!= nil {
                fmt.Println(e)
            }
            ch <- (time.Now().UnixNano() - t)
        }()
    }
    for i := 0; i < n; i++ {
        fmt.Println(<- ch)
    }
}
