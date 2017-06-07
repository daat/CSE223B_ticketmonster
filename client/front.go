package client

import (
    "ticketmonster/ticket"
    "fmt"
    "math/rand"
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

func (self *Front) BuyTicket(in *ticket.BuyInfo, succ *bool) error {
    if len(self.clients) == 0 {
        return fmt.Errorf("no ticket servers")
    }
    return self.clients[rand.Int()%len(self.clients)].BuyTicket(in, succ)
}
