package storage

import (
	"hash/fnv"
	"sort"
)

type binStorageClient struct {
	backs       []string                // addr of all back-end
	clients_map map[string]Storage // map from client_addr to client storage
	clients     []Storage
}

type mid_client struct {
	bin_name string
	n        int
	clients  []Storage
}

func (self *binStorageClient) Init() {
	// initialize map
	self.clients_map = make(map[string]Storage)
	self.clients = make([]Storage, 0, len(self.backs))
	// initialize RPC client to Storage
	for _, v := range self.backs {
		cl := NewClient(v)
		self.clients_map[v] = cl
		self.clients = append(self.clients, cl)
	}
}

func (self *binStorageClient) Bin(name string) Storage {
	// n := Hash(name) % len(self.backs)
	// mid_cl := self.New_mid_client(name, n)
	mid_cl := self.New_mid_client(name)
	return mid_cl
}

func (self *binStorageClient) New_mid_client(name string) Storage {
	n := Hash(name) % len(self.backs)
	return &mid_client{bin_name: name, clients: self.clients, n: n}
}

func Hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (self *mid_client) ListGet(key string, list *List) error {
	logs, e := self.getLogs(key, "list")

	if e != nil {
		return e
	}

	// convert logs to list
	sort.Strings(logs.L)
    list = &logs
	return nil
}

func (self *mid_client) ListAppend(kv *KeyValue, succ *bool) error {
	var kv2 KeyValue
	kv2.Key = genKey(self.bin_name, kv.Key, "list")
	kv2.Value = kv.Value

	bs := self
	n_backs := len(bs.clients)
	now := self.n

	var e error

	e = bs.clients[now].ListAppend(&kv2, succ)
    if e != nil {
    	for now != self.n {
			e = bs.clients[now].ListAppend(&kv2, succ)
    		if e == nil {
    			break
    		}
    		now = (now + 1) % n_backs
    	}
    }
    if e != nil {
        return e
    }

    return nil
}

func (self *mid_client) Clock(atLeast uint64, ret *uint64) error {
	return nil
}

func (self *mid_client) getLogs(key string, vtype string) (List, error) {
	encoded_key := genKey(self.bin_name, key, vtype)

	bs := self
	n_backs := len(bs.clients)
	now := self.n
	var logs, logs2 List
	e := bs.clients[now].ListGet(encoded_key, &logs)
	if e != nil {
		now = (now + 1) % n_backs
		for now != self.n {
			if e == nil {
				e = bs.clients[now].ListGet(encoded_key, &logs)
			}
			if e == nil {
				break
			}
			now = (now + 1) % n_backs
		}
	}

	if e != nil {
		return logs, e
	}

	now = (now + 1) % n_backs
	for now != self.n {
		if e == nil {
			e = bs.clients[now].ListGet(encoded_key, &logs2)
		}
		if e == nil {
			break
		}
		now = (now + 1) % n_backs
	}

	if len(logs2.L) > len(logs.L) {
		return logs2, nil
	}
	return logs, nil
}

func genKey(bin_name, key, vtype string) string {
	nbin_name := Escape(bin_name)
	nkey := Escape(key)
	var ret string
	if vtype == "list" {
		ret = nbin_name + "::list_" + nkey
	} else if vtype == "value" {
		ret = nbin_name + "::value_" + nkey
	}
	return ret
}
