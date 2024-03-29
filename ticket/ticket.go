package ticket


import (
	"sync"
	"strconv"
	"strings"
	"fmt"
	"net"
	"encoding/gob"
	"time"
	"ticketmonster/storage"
)

const init_tickets = 3000
const TOTAL = 30000

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

	// Send a value when the ticketserver is ready
	// The distributed service should be ready to serve
	// when all of the ticketservers is ready.
	Ready chan<- bool
}

type TicketServer struct{
	tc *TicketServerConfig
	Bc storage.BinStorage
	listener    net.Listener
	my_iaddr string

	// record other ticektserver state
	ts_counts []int
	ts_states []int
	ts_sales  []int
	ts_total  []int

	// related variables
	tlock sync.Mutex
	ticket_counter int
	current_sale int
	total_sale int
}

// TODO:
// Read user bought ticket

// Makes a front end that talks to backend
func NewTicketServer(config *TicketServerConfig) TicketServer {
	s := storage.NewBinClient(config.Backs)
	ts := TicketServer{Bc: s, tc: config}
	if config.Id == "0" {
		ts.InitPool()
	}
	ts.Init()
	if config.Ready != nil {
		config.Ready <- true
	}
	return ts
}


func (self *TicketServer) Init() error {
	self.tlock.Lock()
	self.ticket_counter = 0
	self.current_sale = 0
	self.total_sale = 0
	self.tlock.Unlock()

	self.GetFromPool(init_tickets)
	fmt.Printf("%s Init: %v\n", self.tc.Id, self.ticket_counter)

	self.ts_counts = make([]int, len(self.tc.InAddrs))
	self.ts_states = make([]int, len(self.tc.InAddrs))
	self.ts_sales  = make([]int, len(self.tc.InAddrs))
	self.ts_total  = make([]int, len(self.tc.InAddrs))
	for i,_ := range self.tc.InAddrs {
		self.ts_counts[i] = 0
		self.ts_states[i] = -1
		self.ts_sales[i]  = 0
		self.ts_total[i] = 0
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

	// server := rpc.NewServer()
	// e = server.RegisterName("Window", self)
	// if e != nil {
	// 	return e
	// }

    // rpc.RegisterName("Window", self)
    go func() {
        for {
            conn, err := l.Accept()
            if err != nil {
                fmt.Println(err)
                continue
            }
            go func(ts *TicketServer, c net.Conn) {
                dec := gob.NewDecoder(c)
                enc := gob.NewEncoder(c)
                req := storage.Request{}
                res := storage.Response{}
                e := dec.Decode(&req)
                if e != nil {
                    return
                }
                if req.OP == "BuyTicket" {
                    var n int
                    fmt.Sscanf(req.KV.Value, "%d", &n)
                    e = ts.BuyTicket(&BuyInfo{Uid: req.KV.Key, N: n}, &res.Succ)
                } else if req.OP == "GetLeftTickets" {
                    var n int
                    e = ts.GetLeftTickets(true, &n)
                    res.L.L = []string{fmt.Sprintf("%v", n)}
                } else if req.OP == "GetAllTickets" {
                    e = ts.GetAllTickets(true, &res.L)
                } else {
                    e = fmt.Errorf("no such operation")
                }
                if e != nil {
                    res.Err = e.Error()
                }
                e = enc.Encode(res)
                if e != nil {
                    fmt.Println(e)
                }
                c.Close()
            }(self, conn)
        }
    }()
	// go http.Serve(l, server)
	go self.UpdateTicketCounter()
	go self.HeartBeat(nil)
	return nil
}

func (self *TicketServer) InitPool() error{
	bin := self.Bc.Bin("0")
	if bin==nil{
		return fmt.Errorf("initpool nil bin\n")
	}
	var succ bool
	succ = false
	bin.ListAppend(&storage.KeyValue{Key: "TICKETPOOL", Value: "PUT,30000,30000"}, &succ)
	if succ == false{
		return fmt.Errorf("InitPool fail\n")
	}
	return nil
}

// BuyTicket
// server log - Key: "LOG", Value: n,new_total
// user log - Key: uid, Value: n
func (self *TicketServer) BuyTicket(in *BuyInfo, succ *bool) error {
	self.tlock.Lock()
	defer self.tlock.Unlock()

	if in.N>self.ticket_counter {
		return fmt.Errorf("do not have %q ticket left", in.N)
	}

	self.ticket_counter -= in.N
	self.current_sale += in.N
	self.total_sale += in.N

    //fmt.Printf("%v, Buy %v, %v left\n", time.Now(), in.N, self.ticket_counter)
	e := self.WriteToLog(self.tc.Id, strconv.Itoa(self.ticket_counter))
	if e != nil {
		return e
	}
	e = self.WriteToLog(in.Uid, strconv.Itoa(in.N))
	if e != nil {
		return e
	}

	return nil
}

func (self *TicketServer) WriteToLog(key string, n string) error {
	bin := self.Bc.Bin(key)
	var succ bool
	succ = false
	bin.ListAppend(&storage.KeyValue{Key: "LOG", Value: n}, &succ)
	if succ == false{
		return fmt.Errorf("%q WriteToLog failed ", key)
	}
	return nil
}

func (self *TicketServer) GetLeftTickets(useless bool, n *int) error {
	self.tlock.Lock()
	*n = self.ticket_counter
	self.tlock.Unlock()
	return nil
}

func (self *TicketServer) GetAllTickets(useless bool, ret *storage.List) error {
	l := make([]string, 0, len(self.tc.InAddrs))
	for i, n := range self.ts_counts {
		if i == self.tc.This {
			l = append(l, "-")
		} else if self.ts_states[i] == 1 {
			l = append(l, strconv.Itoa(n))
		} else {
			l = append(l, "0")
		}
	}
	ret.L = l
	return nil
}

func (self *TicketServer) GetTotalSale(useless bool, n *int) error {
	self.tlock.Lock()
	*n = self.total_sale
	self.tlock.Unlock()
	return nil
}


// server states
// 1 - live
// 0 - to be recovered
// -1 - server dead

func (self *TicketServer) HeartBeat(exit chan bool){
	listen_exit := make(chan bool)
	go self.listen_func(listen_exit)

	t := time.NewTicker(time.Second*1) // freq to be adjust

	for {
		select {
		case <-listen_exit:
			exit <- true
			return
		default:
			higher_reply := false
			fmt.Printf("server %s: current tickets %d, total sale %d\n", self.tc.Id, self.ticket_counter, self.total_sale)
			for i, v := range self.tc.InAddrs {
				if i == self.tc.This{
					continue
				}
				//fmt.Printf("%s dialing %d\n", self.tc.Id, i)
				conn, e := net.DialTimeout("tcp", v, time.Second)
				if e != nil {
					if self.ts_states[i] == 1{
						self.ts_states[i] = 0
					}
					continue
				}
				if i > self.tc.This {
					higher_reply = true
				}
				go self.handle(conn, i)
			}
			if higher_reply == false {
				for i:=self.tc.This+1; i<len(self.tc.InAddrs); i++{
					if self.ts_states[i] == 0{
						fmt.Printf("%s DoRecovery %d", self.tc.Id, i)
						go self.DoRecovery(i)
					}
				}
			}
			<-t.C
		}

	}
}

// server log - Key: "LOG", Value: n,new_total
func (self *TicketServer) DoRecovery(i int) {
	bin := self.Bc.Bin(strconv.Itoa(i))
	//ListGet(key string, ret *List)
	var l storage.List
	e := bin.ListGet("LOG", &l)
	if e!=nil{
		return
	}

	// get last log total
	if len(l.L)==0 {
		return
	}
	last_log := l.L[len(l.L)-1]
	total,_ := strconv.Atoi(last_log)

	// put the failed server tickets to pool
	e = self.PutToPool(total)
	if e!= nil{
		// recovery not succeed
		// wait till next heartbeat to redo
		return
	}

	// write to failed server log
	succ := false
	bin.ListAppend(&storage.KeyValue{Key: "LOG", Value: "0,0"}, &succ)
	if succ!= true {
		return
	}

	// change server i state to dead
	self.ts_states[i] = -1

	return
}

func (self *TicketServer) listen_func(exit chan bool) {
	for {
		conn, err := self.listener.Accept()
		if err != nil {
			// handle error (and then for example indicate acceptor is down)
			exit <- true
			break
		}

    	words := fmt.Sprintf("%s,%d,%d,%d", self.tc.Id, self.ticket_counter, self.current_sale, self.total_sale)
    	conn.Write([]byte(words))

		conn.Close()
	}
}

func (self *TicketServer) handle(conn net.Conn, i int){
	buffer := make([]byte, 256)
	conn.SetReadDeadline(time.Now().Add(time.Microsecond * 10))
	n, err := conn.Read(buffer)
	if err!=nil{
		// ticket server v might fail
		// until next hearbeat to do recovery
		conn.Close()
		return
	}

	info := strings.Split(string(buffer[:n]), ",")
	if len(info)!=4 {
		conn.Close()
		return
	}
	id, _ := strconv.Atoi(info[0])
	if id != i {
		return
	}
	num,_ := strconv.Atoi(info[1])
	s,_ := strconv.Atoi(info[2])
	t,_ := strconv.Atoi(info[3])
	self.ts_counts[i] = num
	self.ts_states[i] = 1
	self.ts_sales[i] = s
	self.ts_total[i] = t

	conn.Close()

	//fmt.Printf("%v, server %d count: %d\n",time.Now(), i, num)
}


func (self *TicketServer) UpdateTicketCounter() {
	tick_chan := time.NewTicker(time.Second * 1).C // freq to be adjust

	for {
		select {
		case <- tick_chan:
			self.tlock.Lock()
			c := self.current_sale
			t := self.ticket_counter
			mytotal := self.total_sale
			self.current_sale = 0
			self.tlock.Unlock()

			//fmt.Printf("%v, Updating: Current counter %d sale %d\n", time.Now(), t, c)
			if t==0 && c==0 {
				e := self.GetFromPool(init_tickets)
				if e!=nil {
					continue
				}

			} else if 4*c > init_tickets {
				// count for share
				sum := 0
				for i,v := range self.ts_sales {
					if i==self.tc.This {
						sum += c
					} else {
						sum += v
					}
				}
				estimate_total := TOTAL
				for i,v := range self.ts_total {
					if i==self.tc.This {
						estimate_total -= mytotal
					} else {
						estimate_total -= v
					}
				}
				share := 0
				if c > 8*sum/10 {
					share = 3*c
				} else {
					if estimate_total < 5000 {
						share = estimate_total * c / sum
					} else if estimate_total < 15000 {
						share = estimate_total * c / sum /2
					} else {
						share = estimate_total *c / sum /4
					}

				}
				// get tickets
				e := self.GetFromPool(share)
				if e!=nil {
					continue
				}

			} else if 6*c < init_tickets {
				if t <= init_tickets{
					continue
				}
				e := self.PutToPool(c)
				if e!=nil {
					continue
				}

			}
		}
	}
}

func (self *TicketServer) GetFromPool(n int) error {
	bin := self.Bc.Bin("0") // fixed bin name for all pool access
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
    t := self.ticket_counter
    self.tlock.Unlock()
	self.WriteToLog(self.tc.Id, strconv.Itoa(t))

	return nil
}

func (self *TicketServer) PutToPool(n int) error {
	bin := self.Bc.Bin("0")
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
    t := self.ticket_counter
    self.tlock.Unlock()
	self.WriteToLog(self.tc.Id, strconv.Itoa(t))


	return nil
}

func (self *TicketServer) GetPoolTickets() int {
	bin := self.Bc.Bin("0")
	var l storage.List

	e := bin.AccessPool(&storage.KeyValue{Key: "TICKETPOOL", Value: "PUT,0"}, &l)
	if e!=nil {
		return 0
	}

	// update ticket counter
	ret := strings.Split(l.L[0], ",")
	n_pool, _ := strconv.Atoi(ret[3])

	return n_pool
}
