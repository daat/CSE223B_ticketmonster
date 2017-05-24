package ticket


import (
	"sync"
	"storage"
)

type ticketserver struct{
	bc BinStorage
	ticketserver_id string // '0', '1', '2' for now

	// related variables
	tlock sync.Mutex
	ticket_counter int
	current_sale int
}


func (self *ticketserver) Init(n int){
	self.tlock.Lock()
	defer self.tlock.Unlock()
	ticket_counter = n
	current_sale = 0
}


func (self *ticketserver) BuyTicket(uid string, n int) error {
	self.tlock.Lock()
	defer self.tlock.Unlock()

	if n>ticket_counter {
		return fmt.Errorf("do not have %q ticket left", n)
	}

	ticket_counter = ticket_counter - n
	current_sale = current_sale + n

	e := self.WriteToLog(uid, strconv.Itoa(n))
	if e != nil {
		return e
	}

	return nil
}

func (self *ticketserver) WriteToLog(uid string, n string) error {
	bin := self.bc.Bin(self.ticketserver_id)
	var succ bool
	succ = false
	bin.ListAppend(&storage.KeyValue{Key: uid, Value: str}, &succ)
	if succ == false{
		return fmt.Errorf("WriteToLog failed %q %q ", uid, n)
	}
}

func (self *ticketserver) GetLeftTickets(*n int) int{
	self.tlock.Lock()
	n = ticket_counter
	self.tlock.Unlock()
	return n
}

/*
func (self *ticketserver) GetTickets(uid string) int {
	bin := self.bc.Bin(self.ticketserver_id)
	
}

func (self *ticketserver) ListenForToken(token_chan chan bool, exit chan bool){
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

func (self *ticketserver) MonitorSale() error {
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

func (self *ticketserver) GetFromPool(n int) error {
	bin := self.bc.Bin("TicketPool")
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

func (self *ticketserver) PutToPool(n int) error {
	bin := self.bc.Bin("TicketPool")

	self.tlock.Lock()
	ticket_counter = ticket_counter - n
	//write to log
	self.tlock.Unlock()

	bin.WriteBackTicket() // func to write back to public pool tickets
}
*/
