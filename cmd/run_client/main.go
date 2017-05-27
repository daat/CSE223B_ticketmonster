package main

import (
	"ticketmonster"
    "ticketmonster/ticket"
    "ticketmonster/client"
    "ticketmonster/storage"
    "time"
    "fmt"
)

func main() {
    n := 1000
    rc, _ := ticketmonster.LoadRC("bins.rc")
    var front client.Front
    front.Init(rc.TicketServers)
    bc := storage.NewBinClient(rc.PrimaryBacks)
    bin := bc.Bin("0")
    info := &ticket.BuyInfo{Uid: "uuu", N: 1}
    ch := make(chan int64, n)
    // t := time.NewTicker(time.Millisecond)
    for i := 0; i < n; i++ {
        go func(uid int) {
            var succ bool
            info.Uid = fmt.Sprintf("uuu%d", uid)
            kv := storage.KeyValue{Key: fmt.Sprintf("uuu%d", uid), Value: "buy 1"}
            now := time.Now().UnixNano()
            // e := front.BuyTicket(info, &succ)
            e := bin.ListAppend(&kv, &succ)
            if e!= nil {
                fmt.Println(e)
            }
            ch <- (time.Now().UnixNano() - now)
        }(i)
        // <-t.C
    }
    for i := 0; i < n; i++ {
        fmt.Println(<- ch)
    }
}
