package main

import (
	"ticketmonster"
    "ticketmonster/ticket"
    "ticketmonster/client"
    "time"
    "fmt"
    "net"
    "strconv"
    "os"
    "bufio"
)

func main() {
    n := 1000
    wait := true
    rc, _ := ticketmonster.LoadRC("bins.rc")
    var front client.Front
    front.Init(rc.TicketServers_out)
    id := -1
    if wait {
        l, ee := net.Listen("tcp", ":17898")
    	if ee != nil {
            fmt.Println(ee)
            return
    	}
        conn, e := l.Accept()
        if e != nil {
            fmt.Println(e)
            return
        }
        buffer := make([]byte, 256)
    	n, e := conn.Read(buffer)
        if e != nil {
            fmt.Println(e)
            return
        }
        id, _ = strconv.Atoi(string(buffer[:n]))

    }
    f, _ := os.Create(fmt.Sprintf("output_%v.txt", id))
    w := bufio.NewWriter(f)

    ch := make(chan int64, n)
    for i := 0; i < n; i++ {
        go func(uid int) {
            var succ bool
            info := &ticket.BuyInfo{Uid: fmt.Sprintf("uuu%d", uid), N: 1}
            now := time.Now().UnixNano()
            e := front.BuyTicket(info, &succ)
            if e!= nil {
                fmt.Println(e)
            }
            ch <- (time.Now().UnixNano() - now)
        }(i)
    }
    s := ""
    for i := 0; i < n; i++ {
        t := fmt.Sprintf("%v\n",<-ch)
        s = s + t
    }
    w.WriteString(s)
    w.Flush()
    f.Close()
}
