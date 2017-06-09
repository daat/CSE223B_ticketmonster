package client

import (
    "fmt"
    "math/rand"
    "ticketmonster/ticket"
    "ticketmonster/storage"
)

type Front struct {
    servers []string
    clients []ticket.Window
}

func (self *Front) Init(addrs []string) {
    self.servers = addrs
    self.clients = make([]ticket.Window, len(self.servers))
    for i, s := range self.servers {
        self.clients[i] = ticket.NewFrontier(s)
    }
}

// redirect

func (self *Front) BuyTicket(in *ticket.BuyInfo, succ *bool) error {
    if len(self.clients) == 0 {
        return fmt.Errorf("no ticket servers")
    }

    // n := rand.Int()%len(self.clients)
    ns := []int{0,0,1,2}
    n := ns[rand.Int()%len(ns)]


    err := self.clients[n].BuyTicket(in, succ)
    if err == nil{
        // buy succeed
        return nil
    }

    flag := false
    var l storage.List
    err = self.clients[n].GetAllTickets(true, &l)
    if err != nil {
        return err
    }
    i := (n + 1) % len(self.clients)
    for i != n {
        v := l.L[i]
        if v == "-" || v == "0" {
            i = (i + 1) % len(self.clients)
            continue
        }
        err = self.clients[i].BuyTicket(in, succ)
        if err == nil{
            flag = true
            break
        }
        i = (i + 1) % len(self.clients)
    }

    if flag {
        return nil
    } else {
        return fmt.Errorf("no ticket left")
    }
}
