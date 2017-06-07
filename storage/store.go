// Package store provides a simple in-memory key value store.
package storage

import (
	"container/list"
	"log"
	"math"
	"sync"
	"strings"
	"strconv"
	"fmt"
)

var Logging bool

type strList []string

// In-memory storage implementation. All calls always returns nil.
type Store struct {
	clock uint64

	lists map[string]*list.List

	clockLock sync.Mutex
	strLock   sync.Mutex
	listLock  sync.Mutex
}

var _ Storage = new(Store)

func NewStoreId(id int) *Store {
	return &Store{
		lists: make(map[string]*list.List),
	}
}

func NewStore() *Store {
	//return NewStorageId(0)

	store := NewStoreId(0)
    var clock uint64 = 1
    store.Clock(clock, &clock) //start from 1
	return store

}

func newList() *list.List {
    return list.New()
}

func (self *Store) Clock(atLeast uint64, ret *uint64) error {
	self.clockLock.Lock()
	defer self.clockLock.Unlock()

	if self.clock < atLeast {
		self.clock = atLeast
	}

	*ret = self.clock

	if self.clock < math.MaxUint64 {
		self.clock++
	}

	if Logging {
		log.Printf("Clock(%d) => %d", atLeast, *ret)
	}

	return nil
}

func (self *Store) ListKeys(p *Pattern, r *List) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	ret := make([]string, 0, len(self.lists))
	for k := range self.lists {
		if p.Match(k) {
			ret = append(ret, k)
		}
	}

	r.L = ret

	if Logging {
		log.Printf("ListKeys(%q, %q) => %d", p.Prefix, p.Suffix, len(r.L))
		for i, s := range r.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *Store) ListGet(key string, ret *List) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	if lst, found := self.lists[key]; !found {
		ret.L = []string{}
	} else {
		ret.L = make([]string, 0, lst.Len())
		for i := lst.Front(); i != nil; i = i.Next() {
			ret.L = append(ret.L, i.Value.(string))
		}
	}

	if Logging {
		log.Printf("ListGet(%q) => %d", key, len(ret.L))
		for i, s := range ret.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *Store) ListAppend(kv *KeyValue, succ *bool) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	lst, found := self.lists[kv.Key]
	if !found {
		lst = list.New()
		self.lists[kv.Key] = lst
	}

	lst.PushBack(kv.Value)

	*succ = true

	if Logging {
		log.Printf("ListAppend(%q, %q)", kv.Key, kv.Value)
	}

	return nil
}

func (self *Store) ListRemove(kv *KeyValue, n *int) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	*n = 0

	lst, found := self.lists[kv.Key]
	if !found {
		return nil
	}

	i := lst.Front()
	for i != nil {
		if i.Value.(string) == kv.Value {
			hold := i
			i = i.Next()
			lst.Remove(hold)
			*n++
			continue
		}

		i = i.Next()
	}

	if lst.Len() == 0 {
		delete(self.lists, kv.Key)
	}

	if Logging {
		log.Printf("ListRemove(%q, %q) => %d", kv.Key, kv.Value, *n)
	}

	return nil
}

// public pool kv pair
// key: TICKETPOOL
// value: clock,(PUT/GET),number

// TICKETPOOL List log: clock, (PUT/GET), number, total
func (self *Store) AccessPool(kv *KeyValue, list *List) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	// if kv.Key != "TICKETPOOL" {
	// 	return fmt.Errorf("AccessPoolDenied: not TICKETPOOL")
	// }

	lst, found := self.lists[kv.Key]
	if !found {
		lst = newList()
		self.lists[kv.Key] = lst
	}

    // get last log
    total := 0
    var mc, c uint64
    for i := lst.Front(); i != nil; i = i.Next() {
        arr := strings.Split(i.Value.(string), ",")
        fmt.Sscanf(arr[0], "%25d", &c)
        if c > mc {
            mc = c
            total, _ = strconv.Atoi(arr[3])
        }
    }

	access := strings.Split(kv.Value, ",")
    clock := access[0]
	op := access[1]
	num,_ := strconv.Atoi(access[2])
	if op != "PUT" && op !="GET"{
		return fmt.Errorf("AccessPoolDenied: wrong operation: %s", access[1])
	}

	var s string
	if op == "PUT" {
		s = fmt.Sprintf("%s,%d", kv.Value, total+num)
	} else if total > num {
		s = fmt.Sprintf("%s,%d", kv.Value, total-num)
	} else {
        s = fmt.Sprintf("%s,%s,%d,%d", clock, op, total, 0)
    }
	lst.PushBack(s)

	list.L = []string{s}

	if Logging {
		log.Printf("AccessPool(%q)", s)
	}

	return nil

}

func (self *Store) RecoverTicketServer(server_i int, n int) error {
	self.listLock.Lock()
	defer self.listLock.Unlock()

	// get recover log
	lst, found := self.lists["RECOVERLOG"]
	if !found {
		lst = newList()
		self.lists["RECOVERLOG"] = lst
	}

	

	return nil
}

