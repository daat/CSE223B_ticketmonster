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
    n := 15000
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

    exit_ch := make(chan bool)
    wait_ch := make(chan bool, 500)
    ch := make(chan int64, 500)

    go func() {
        f, _ := os.Create(fmt.Sprintf("output_%v.txt", id))
        w := bufio.NewWriter(f)
        lines := make([]string, 0, n)
        for i := 0; i < n; i++ {
            t := fmt.Sprintf("%v\n",<-ch)
            <-wait_ch
            lines = append(lines, t)
        }
        for i := 0; i < n; i++ {
            w.WriteString(lines[i])
        }
        w.Flush()
        f.Close()
        exit_ch <- true
    }()

    for i := 0; i < n; i++ {
        go func(uid int) {
            wait_ch <- true
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
    <- exit_ch
}
