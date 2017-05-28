package storage

import (
	"strings"
)

type Stat struct {
    n int
    users []string
    counts []int
}

type KeyValue struct {
	Key   string
	Value string
}

type Pattern struct {
	Prefix string
	Suffix string
}

func (p *Pattern) Match(k string) bool {
	ret := strings.HasPrefix(k, p.Prefix)
	ret = ret && strings.HasSuffix(k, p.Suffix)
	return ret
}

type List struct {
	L []string
}

func KV(k, v string) *KeyValue { return &KeyValue{k, v} }

type Storage interface {
    ListGet(key string, list *List) error
	// Append a string to the list. Set succ to true when no error.
	ListAppend(kv *KeyValue, succ *bool) error

}

type CommandStorage interface {
    // Returns an auto-incrementing clock. The returned value of each call will
    // be unique, no smaller than atLeast, and strictly larger than the value
    // returned last time, unless it was math.MaxUint64.
    Clock(atLeast uint64, ret *uint64) error

    StartServing(id int, clock *uint64) error

    Storage
}

// Key-Storage interface
type BinStorage interface {
	// Fetch a storage based on the given bin name.
	Bin(name string) Storage
}
