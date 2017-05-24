package ticket


import (
	"sync"
	"strconv"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"ticketmonster/storage"
)

type TicketServer struct{
	Bc storage.BinStorage
	Ticketserver_id string // '0', '1', '2' for now

	// related variables
	tlock sync.Mutex
	ticket_counter int
	current_sale int
}

// TODO:
// Read user bought ticket

func (self *TicketServer) Init(n int, addr string){
	self.tlock.Lock()
	self.ticket_counter = n
	self.current_sale = 0
	self.tlock.Unlock()

	// start rpc server for client connection
	l, e := net.Listen("tcp", addr)
	if e != nil {
		fmt.Errorf("TicketServer Listen error")
		return 
	}
	
	server := rpc.NewServer()
	e = server.RegisterName("Window", self)
	if e != nil {
		fmt.Errorf("TicketServer RPC fail")
		return 
	}
	go http.Serve(l, server)
	
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
	bin := self.Bc.Bin(self.Ticketserver_id)
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

/*

func (self *TicketServer) ListenForToken(token_chan chan bool, exit chan bool){
	for {
		c, err := self.listener.Accept()
		if err != nil {
			// handle error (and then for example indicate acceptor is down)
			exit <- true
			break
		}
		// receive token

		token_chan <- true
		c.Close()
	}
	return 
}

func (self *TicketServer) MonitorSale() error {
	token_chan := make(chan bool)
	listen_exit := make(chan bool)

	go self.ListenForToken(token_chan, listen_exit)

	tick_chan := time.NewTicker(time.Millisecond * 100).C // freq to be adjust

	for {
		select {
		case <- listen_exit:
			exit <- true
			return nil
		case <- token_chan:
			self.tlock.Lock()
			c := current_sale 
			t := ticket_counter
			current_sale = 0
			self.tlock.Unlock()

			if t < c {
				self.GetFromPool(c/2) // can change this 
			} else if t > c {
				self.PutToPool(c/2)
			}
		}
		case <- tick_chan:
			self.tlock.Lock()
			current_sale = 0
			self.tlock.Unlock()
	}
}

func (self *TicketServer) GetFromPool(n int) error {
	bin := self.Bc.Bin("TicketPool")
	v := bin.ReadTicket() // func to read public pool tickets
	getticket := 0
	if n <= v {
		getticket = n
	} else {
		getticket = v
	}

	self.tlock.Lock()
	ticket_counter = ticket_counter + getticket
	//write to log
	self.tlock.Unlock()

	bin.WriteBackTicket() // func to write back to public pool tickets
}

func (self *TicketServer) PutToPool(n int) error {
	bin := self.Bc.Bin("TicketPool")

	self.tlock.Lock()
	ticket_counter = ticket_counter - n
	//write to log
	self.tlock.Unlock()

	bin.WriteBackTicket() // func to write back to public pool tickets
}
*/
