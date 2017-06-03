package ticket


import (
	"sync"
	"strconv"
	"strings"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"ticketmonster/storage"
)

type TicketServerConfig struct {
	// The addresses of back-ends
	Backs []string

	// The address for outside connection
	OutAddr string

	// The inside addresses of TicketServers
	InAddrs []string

	// The index of this ticketserver
	This int

	// TicketServer Id ('0', '1', '2')
	Id string
}

type TicketServer struct{
	tc *TicketServerConfig
	Bc storage.BinStorage
	addr string

	// related variables
	tlock sync.Mutex
	ticket_counter int
	current_sale int
}

// TODO:
// Read user bought ticket

// Makes a front end that talks to backend
func NewTicketServer(config *TicketServerConfig) TicketServer {
	s := storage.NewBinClient(config.Backs)
	ts := TicketServer{Bc: s, tc: config}
	return ts
}


func (self *TicketServer) Init(n int) error {
	self.tlock.Lock()
	self.ticket_counter = n
	self.current_sale = 0
	self.tlock.Unlock()

	// start rpc server for client connection
	l, e := net.Listen("tcp", self.tc.OutAddr)
	if e != nil {
		return e
	}
	
	server := rpc.NewServer()
	e = server.RegisterName("Window", self)
	if e != nil {
		return e
	}
	go http.Serve(l, server)
	go self.UpdateTicketCounter()
	return nil
}



func (self *TicketServer) BuyTicket(in *BuyInfo, succ *bool) error {
	self.tlock.Lock()
	defer self.tlock.Unlock()

	if in.N>self.ticket_counter {
		return fmt.Errorf("do not have %q ticket left", in.N)
	}

	self.ticket_counter -= in.N
	self.current_sale += in.N

	e := self.WriteToLog(in.Uid, strconv.Itoa(in.N))
	if e != nil {
		return e
	}

	return nil
}

func (self *TicketServer) WriteToLog(uid string, n string) error {
	bin := self.Bc.Bin(self.tc.Id)
	var succ bool
	succ = false
	bin.ListAppend(&storage.KeyValue{Key: uid, Value: n}, &succ)
	if succ == false{
		return fmt.Errorf("WriteToLog failed %q", uid)
	}
	return nil
}

func (self *TicketServer) GetLeftTickets(useless bool, n *int) error {
	self.tlock.Lock()
	*n = self.ticket_counter
	self.tlock.Unlock()
	return nil
}


func (self *TicketServer) UpdateTicketCounter() {
	tick_chan := time.NewTicker(time.Minute*2).C // freq to be adjust
	for {
		select {
		case <- tick_chan:
			self.tlock.Lock()
			c := self.current_sale 
			t := self.ticket_counter
			self.current_sale = 0
			self.tlock.Unlock()

			bin := self.Bc.Bin("TICKETPOOL")
			var l storage.List
			if t < c {
				e := bin.AccessPool(&storage.KeyValue{Key: "GET", Value: strconv.Itoa(c/2)}, &l) 
				if e!=nil {
					continue
				}
				
				// update ticket counter
				ret := strings.Split(l.L[0], ",")
				n,_ := strconv.Atoi(ret[2])
				self.tlock.Lock()
				self.ticket_counter += n
				self.tlock.Unlock()
			
			} else if t > c {
				e := bin.AccessPool(&storage.KeyValue{Key: "PUT", Value: strconv.Itoa(t/2)}, &l)
				if e!=nil {
					continue
				}

				// update ticket counter
				ret := strings.Split(l.L[0], ",")
				n,_ := strconv.Atoi(ret[2])
				self.tlock.Lock()
				self.ticket_counter -= n
				self.tlock.Unlock()

			}
		}
	}
}
