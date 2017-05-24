// Package store provides a simple in-memory key value store.
package storage

import (
	"container/list"
	"log"
	"math"
	"sync"
	//"fmt"
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

	return store

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
