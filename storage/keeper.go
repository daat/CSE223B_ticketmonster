package storage

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	FOLLOWER = iota
	LEADER
)

type keeper struct {
	kc          *KeeperConfig
	keeperStore Storage
	listener    net.Listener
	state       int

	clients_map   map[string]Storage
	clients       []Storage
	clients_alive []bool
}

func (self *keeper) Init() {
	self.clients = make([]Storage, 0, len(self.kc.Backs))
	self.clients_alive = make([]bool, len(self.kc.Backs))
	self.clients_map = make(map[string]Storage)
	for i, v := range self.kc.Backs {
		cl := NewClient(v)
		self.clients_map[v] = cl
		self.clients = append(self.clients, cl)

		// check if the backend is alive
		var value string // add
		if cl.Get("whatever", &value) == nil {
			self.clients_alive[i] = true
			// var succ bool
			// self.clients[ind].Set(&KeyValue{Key: "complete", Value: "true"}, &succ)
		} else {
			self.clients_alive[i] = false
		}
	}
	name := "KEEPERLOG"
	n := Hash(name) % len(self.kc.Backs)
	self.keeperStore = &mid_client{bin_name: name, clients: self.clients, n: n}
}

func (self *keeper) HeartBeat(exit chan bool) {
	listen_exit := make(chan bool)
	go self.listen_func(listen_exit)

	myid := self.kc.This
	higherkeeper := make([]string, len(self.kc.Addrs)-self.kc.This-1)
	idx := 0
	for i := myid + 1; i < len(self.kc.Addrs); i++ {
		higherkeeper[idx] = self.kc.Addrs[i]
		idx++
	}

	//fmt.Printf("%v beeep! %v\n", myid, len(higherkeeper))
	count := 0
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-listen_exit:
			exit <- true
			return
		default:
			higher_reply := false
			//fmt.Printf("election! %v\n", count)
			count++
			for _, v := range higherkeeper {
				res := self.check_keeper(v)
				if res == true {
					higher_reply = true
					//fmt.Printf("%v is FOLLOWER\n", myid)
					self.state = FOLLOWER
					break
				}
			}
			if !higher_reply {
				// leader now
				self.state = LEADER
				//fmt.Printf("I AM LEADER. %v\n", myid)
				// check keeper log redo all operation
				self.checklog()
				// do migration ~~~
				self.update_clk()

			}
			<-t.C
		}

	}
}

func (self *keeper) listen_func(exit chan bool) {
	for {
		c, err := self.listener.Accept()
		if err != nil {
			// handle error (and then for example indicate acceptor is down)
			exit <- true
			break
		}
		c.Close()
	}
}

func (self *keeper) check_keeper(addr string) bool {
	conn, e := net.Dial("tcp", addr)
	if e != nil {
		return false
	}
	defer conn.Close()
	// fmt.Printf("%v check %v alive\n", self.kc.This, addr)
	return true
}

func (self *keeper) update_clk() error {
	var max_clk uint64
	max_clk = 0

	for i, store := range self.clients {
		var t uint64
		e := store.Clock(max_clk, &t)
		self.check_alive(i, e)

		if t > max_clk {
			max_clk = t
		}
	}

	for i, store := range self.clients {
		var t uint64
		e := store.Clock(max_clk, &t)
		self.check_alive(i, e)
	}
	return nil
}

/* TODO:
consider keeper dies when doing checklog
-> change logging (not in function after_up, down)
-> do it before and after calling the function
*/

func (self *keeper) checklog() {
	var l List
	e := self.keeperStore.ListGet("Log", &l)
	if e != nil {
		return
	}
	for _, s := range l.L {
		i := strings.Index(s, "@")
		if i == -1 {
			continue
		}
		op := s[:i]
		ind, err := strconv.Atoi(s[i+1:])
		if err != nil {
			// error~~~
		}
		if op == "after_up" {
			go self.after_up(ind, true)

		} else if op == "after_down" {
			go self.after_down(ind, true)
		}
	}

}

func (self *keeper) check_alive(ind int, e error) {

	if e != nil {
		if self.clients_alive[ind] {
			// it's down
			self.clients_alive[ind] = false
			go self.after_down(ind, false)

		}
	} else {
		if !self.clients_alive[ind] {
			// it's up
			self.clients_alive[ind] = true // ???
			go self.after_up(ind, false)

		}
	}
}

func (self *keeper) after_up(ind int, redo bool) {
	v := "after_up@" + strconv.Itoa(ind)
	// write to log
	if !redo {
		var succ bool
		succ = false
		e := self.keeperStore.ListAppend(&KeyValue{Key: "Log", Value: v}, &succ)
		if e != nil {
			// log failed
		}
	}

	n_backs := len(self.clients)
	next := (ind + 1) % n_backs
	for next != ind {
		if self.clients_alive[next] {
			break
		}
		next = (next + 1) % n_backs
	}
	// no alive back-end
	if next == ind {
		return
	}
	prev := (ind - 1) % n_backs
	for prev != ind {
		if self.clients_alive[prev] {
			break
		}
		prev = (prev - 1) % n_backs
	}

	pattern := Pattern{Prefix: "", Suffix: ""}
	var keys List
	if self.clients[prev].ListKeys(&pattern, &keys) == nil {
		for _, key := range keys.L {
			arr := strings.Split(key, "::")
			if len(arr) > 1 {
				n := Hash(Unescape(arr[0])) % n_backs
				first := n
				for !self.clients_alive[first] {
					first = (first + 1) % n_backs
				}
				// the key-value pair's primary server is prev
				if first == prev {
					self.replicate(key, prev, ind)
				}
			} else {
			}

		}
	}

	if self.clients[next].ListKeys(&pattern, &keys) == nil {
		for _, key := range keys.L {
			arr := strings.Split(key, "::")
			if len(arr) > 1 {
				n := Hash(Unescape(arr[0])) % len(self.clients)
				first := n
				for !self.clients_alive[first] {
					first = (first + 1) % n_backs
				}
				// the key-value pair's primary server is ind
				if first == ind {
					self.replicate(key, next, ind)
				}
			} else {
			}

		}
	}
	// var succ bool
	// self.clients[ind].Set(&KeyValue{Key: "complete", Value: "true"}, &succ)

	// remove keeper-op from log
	n := 0
	e := self.keeperStore.ListRemove(&KeyValue{Key: "Log", Value: v}, &n)
	if e != nil {
		// ???!!!
	}
}

func (self *keeper) after_down(ind int, redo bool) {
	v := "after_down@" + strconv.Itoa(ind)
	if !redo {
		// write to log
		var succ bool
		succ = false
		e := self.keeperStore.ListAppend(&KeyValue{Key: "Log", Value: v}, &succ)
		if e != nil {
			// log failed
		}
	}

	n_backs := len(self.clients)
	next := (ind + 1) % n_backs
	for next != ind {
		if self.clients_alive[next] {
			break
		}
		next = (next + 1) % n_backs
	}
	// no alive back-end
	if next == ind {
		return
	}
	prev := (ind - 1) % n_backs
	for prev != ind {
		if self.clients_alive[prev] {
			break
		}
		prev = (prev - 1) % n_backs
	}
	// only one alive back-end
	if prev == next {
		return
	}
	next_next := (next + 1) % n_backs
	for next_next != ind {
		if self.clients_alive[next_next] {
			break
		}
		next_next = (next_next + 1) % n_backs
	}

	pattern := Pattern{Prefix: "", Suffix: ""}
	var keys List
	if self.clients[prev].ListKeys(&pattern, &keys) == nil {
		for _, key := range keys.L {
			arr := strings.Split(key, "::")
			if len(arr) > 1 {
				n := Hash(Unescape(arr[0])) % n_backs
				first := n
				for !self.clients_alive[first] {
					first = (first + 1) % n_backs
				}
				// the key-value pair's primary server is prev
				if first == prev {
					self.replicate(key, prev, next)
				}
			} else {
			}

		}
	}

	if self.clients[next].ListKeys(&pattern, &keys) == nil {
		for _, key := range keys.L {
			arr := strings.Split(key, "::")
			if len(arr) > 1 {
				n := Hash(Unescape(arr[0])) % len(self.clients)
				first := n
				for !self.clients_alive[first] && first != ind {
					first = (first + 1) % n_backs
				}
				// the key-value pair's primary server is ind
				if first == ind {
					self.replicate(key, next, next_next)
				}
			} else {
			}

		}
	}

	// remove keeper-op from log
	n := 0
	e := self.keeperStore.ListRemove(&KeyValue{Key: "Log", Value: v}, &n)
	if e != nil {
		// ???!!!
	}
}

func (self *keeper) replicate(key string, src int, dest int) {
	var plist List
	var list List
	var kv KeyValue
	var succ bool
	var tclock uint64
	var max_clock uint64 = 0
	self.clients[dest].ListGet(key, &plist)
	for _, log := range plist.L {
		arr := strings.SplitN(log, ",", 3)
		fmt.Sscanf(arr[0], "%25d", &tclock)
		if tclock > max_clock {
			max_clock = tclock
		}
	}
	if self.clients[src].ListGet(key, &list) == nil {
		for _, log := range list.L {
			arr := strings.SplitN(log, ",", 3)
			fmt.Sscanf(arr[0], "%25d", &tclock)
			if tclock > max_clock {
				kv.Key = key
				kv.Value = log
				self.clients[dest].ListAppend(&kv, &succ)
			}
		}
	}
}
