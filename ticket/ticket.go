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

const init_tickets = 10000

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
	listener    net.Listener
	my_iaddr string

	// record other ticektserver state
	ts_counts_map map[string]int
	ts_states_map map[string]bool

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

	self.GetFromPool(init_tickets)
	//fmt.Printf("Init: %v\n", self.ticket_counter)

	self.ts_counts_map = make(map[string]int)
	self.ts_states_map = make(map[string]bool)
	for _, v := range self.tc.Backs {
		self.ts_counts_map[v] = 0
		self.ts_states_map[v] = true
	}

	// start listening for inside connection
	self.my_iaddr = self.tc.InAddrs[self.tc.This]
	l, e := net.Listen("tcp", self.my_iaddr)
	if e != nil {
		return e
	}
	self.listener = l

	// start rpc server for client connection
	l, e = net.Listen("tcp", self.tc.OutAddr)
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

func (self *TicketServer) InitPool() {
	bin := self.Bc.Bin(self.tc.Id)
	var succ bool
	succ = false
	bin.ListAppend(&storage.KeyValue{Key: "TICKETPOOL", Value: "PUT,20000,20000"}, &succ)
	if succ == false{
		fmt.Printf("InitPool fail\n")
	}

	var l storage.List
	e := bin.AccessPool(&storage.KeyValue{Key: "TICKETPOOL", Value: "GET,0"}, &l)
	if e!=nil {
		// 
	}

	// update ticket counter
	fmt.Printf("%s\n", l.L[0])
	ret := strings.Split(l.L[0], ",")
	total,_ := strconv.Atoi(ret[3])
	fmt.Printf("Pool: %d\n", total)
}



func (self *TicketServer) BuyTicket(in *BuyInfo, succ *bool) error {
	self.tlock.Lock()
	defer self.tlock.Unlock()

	if in.N>self.ticket_counter {
		return fmt.Errorf("do not have %q ticket left", in.N)
	}

	self.ticket_counter -= in.N
	self.current_sale += in.N

    //fmt.Printf("%v, Buy %v, %v left\n", time.Now(), in.N, self.ticket_counter)

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

/*
func (self *TicketServer) HeartBeat(exit chan bool){
	listen_exit := make(chan bool)
	go self.listen_func(listen_exit)

	t := time.NewTicker(time.Second) // freq to be adjust

	for {
		select {
		case <-listen_exit:
			exit <- true
			return
		default:
			for _, v := range self.tc.InAddrs {
				conn, e := net.Dial("tcp", v)
				if e != nil {
					// ticket server v fail, do recovery
					// ...
					continue
				}
				self.ts_states_map[v] = false
				conn.Write([]byte("beep"))
				// wait for 0.5 second
				time.Sleep(250 * time.Millisecond)
				if self.ts_states_map[v] == false {
					// ticket server v fail, do recovery
					// ...
				}

				conn.Close()
			}
			<-t.C
		}

	}
}
*/

func (self *TicketServer) listen_func(exit chan bool) {
	for {
		conn, err := self.listener.Accept()
		if err != nil {
			// handle error (and then for example indicate acceptor is down)
			exit <- true
			break
		}

    	buffer := make([]byte, 256)
    	n, err := conn.Read(buffer)
    	if err!=nil{
    		conn.Close()
    		continue
    	}

    	if string(buffer[:n]) == "beep" {
    		words := fmt.Sprintf("%s,%d", self.tc.Id, self.ticket_counter)
    		conn.Write([]byte(words))
    	} else {
    		info := strings.Split(string(buffer[:n]), ",")
    		if len(info)!=2 {
    			conn.Close()
    			continue
    		}

    		num,_ := strconv.Atoi(info[1])
    		self.ts_counts_map[info[0]] = num
    		self.ts_states_map[info[0]] = true

    	}

		conn.Close()
	}
}


func (self *TicketServer) UpdateTicketCounter() {
	tick_chan := time.NewTicker(time.Second * 30).C // freq to be adjust

	for {
		select {
		case <- tick_chan:
			self.tlock.Lock()
			c := self.current_sale
			t := self.ticket_counter
			self.current_sale = 0
			self.tlock.Unlock()

			//fmt.Printf("%v, Updating: Current counter %d sale %d\n", time.Now(), t, c)
			if t==0 && c==0 {
				e := self.GetFromPool(init_tickets)
				if e!=nil {
					continue
				}

			} else if t < c {
				e := self.GetFromPool(c/2)
				if e!=nil {
					continue
				}

			} else if t > c {
				e := self.PutToPool(t/2)
				if e!=nil {
					continue
				}

			}
		}
	}
}

func (self *TicketServer) GetFromPool(n int) error {
	bin := self.Bc.Bin(self.tc.Id)
	var l storage.List

	e := bin.AccessPool(&storage.KeyValue{Key: "TICKETPOOL", Value: "GET,"+strconv.Itoa(n)}, &l)
	if e!=nil {
		return e
	}

	// update ticket counter
	ret := strings.Split(l.L[0], ",")
	num,_ := strconv.Atoi(ret[2])

	self.tlock.Lock()
	self.ticket_counter += num
	// fmt.Printf("%v, %d, %d\n", time.Now(), num, self.ticket_counter)
	self.tlock.Unlock()

	return nil
}

func (self *TicketServer) PutToPool(n int) error {
	bin := self.Bc.Bin(self.tc.Id)
	var l storage.List

	e := bin.AccessPool(&storage.KeyValue{Key: "TICKETPOOL", Value: "PUT,"+strconv.Itoa(n)}, &l)
	if e!=nil {
		return e
	}

	// update ticket counter
	ret := strings.Split(l.L[0], ",")
	num,_ := strconv.Atoi(ret[2])
	self.tlock.Lock()
	self.ticket_counter -= num
	self.tlock.Unlock()

	return nil
}
