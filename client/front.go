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
    _ = rand.Int()%len(self.clients)
    return self.clients[0].BuyTicket(in, succ)
}
