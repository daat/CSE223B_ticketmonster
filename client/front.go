package client

import (
    "ticketmonster/ticket"
)

type Front struct {
    servers []string
    clients []ticket.Window
}

func (self *Front) Init(addrs []string) {
    self.servers = self.addrs
    self.clients = make([]ticket.Window, len(self.servers))
    for i, s := range self.servers {
        self.clients[i] = ticket.NewFrontier(s)
    }
}
