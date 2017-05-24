package storage

import (
	"hash/fnv"
    "strconv"
)

type BinStorageClient struct {
	Backs       []string                // addr of all back-end
	clients_map map[string]Storage // map from client_addr to client storage
	clients     []Storage
}

type mid_client struct {
	bin_name string
	n        int
	clients  []Storage
}

func (self *BinStorageClient) Init() {
	// initialize map
	self.clients_map = make(map[string]Storage)
	self.clients = make([]Storage, 0, len(self.Backs))
	// initialize RPC client to Storage
	for _, v := range self.Backs {
		cl := NewPrimaryClient(v)
		self.clients_map[v] = cl
		self.clients = append(self.clients, cl)
	}
}

func (self *BinStorageClient) Bin(name string) Storage {
	// n := Hash(name) % len(self.Backs)
	// mid_cl := self.New_mid_client(name, n)
	mid_cl := self.New_mid_client(name)
	return mid_cl
}

func (self *BinStorageClient) New_mid_client(name string) Storage {
	n := Hash(name) % len(self.Backs)

    v, e := strconv.Atoi(name) // fix a backend
    if e == nil {
        n = v % len(self.Backs)
    }
	return &mid_client{bin_name: name, clients: self.clients, n: n}
}

func Hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (self *mid_client) ListGet(key string, list *List) error {
	encoded_key := genKey(self.bin_name, key)

	bs := self
	n_backs := len(bs.clients)
	now := self.n

	var e error

	e = bs.clients[now].ListGet(encoded_key, list)
    if e != nil {
        now = (now + 1) % n_backs
    	for now != self.n {
			e = bs.clients[now].ListGet(encoded_key, list)
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

func (self *mid_client) ListAppend(kv *KeyValue, succ *bool) error {
	var kv2 KeyValue
	kv2.Key = genKey(self.bin_name, kv.Key)
	kv2.Value = kv.Value

	bs := self
	n_backs := len(bs.clients)
	now := self.n

	var e error

	e = bs.clients[now].ListAppend(&kv2, succ)
    if e != nil {
        now = (now + 1) % n_backs
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

func genKey(bin_name, key string) string {
	nbin_name := Escape(bin_name)
	nkey := Escape(key)

	return nbin_name + "::" + nkey
}
