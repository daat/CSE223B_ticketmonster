package ticket

import (
	"net/rpc"
)

type Frontier struct {
	addr string
}


func NewFrontier(addr string) Window{
	return &Frontier{addr: addr}
}

func (self *Frontier) BuyTicket(in *BuyInfo, succ *bool) error{
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("TICKET.BuyTicket", in, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (self *Frontier) GetLeftTickets(useless bool, n *int) error{
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}
	// perform the call
	e = conn.Call("TICKET.GetLeftTickets", useless, n)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}
