package storage

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
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

// mid_client implements Storage interface

func (self *mid_client) Get(key string, value *string) error {
	logs, e := self.getLogs(key, "value")
	if e != nil {
		return e
	}

	logs_to_value(&logs, value)

	return nil
}

func (self *mid_client) Set(kv *KeyValue, succ *bool) error {
	kv.Key = genKey(self.bin_name, kv.Key, "value")

	primary, _, _, e := self.action(kv, "A")

	if e != nil || primary < 0 {
		return e
	}

	*succ = true

	return nil
}

func (self *mid_client) Keys(p *Pattern, list *List) error {
	return self.getKeys(p, list, "value")
}

func (self *mid_client) ListGet(key string, list *List) error {
	logs, e := self.getLogs(key, "list")

	if e != nil {
		return e
	}

	// convert logs to list
	sort.Strings(logs.L)
	slist := make([]string, 0, len(logs.L))
	for _, log := range logs.L {
		arr := strings.SplitN(log, ",", 3)
		val := arr[2]
		if arr[1] == "A" {
			slist = append(slist, val)
		} else if arr[1] == "R" {
			new_list := make([]string, 0, len(slist))
			for _, s := range slist {
				if s != val {
					new_list = append(new_list, val)
				}
			}
			slist = new_list
		}
	}
	list.L = slist

	return nil
}

func (self *mid_client) ListAppend(kv *KeyValue, succ *bool) error {
	var kv2 KeyValue
	kv2.Key = genKey(self.bin_name, kv.Key, "list")
	kv2.Value = kv.Value

	primary, _, _, e := self.action(&kv2, "A")
	if e != nil || primary < 0 {
		return e
	}

	*succ = true

	return nil
}

func (self *mid_client) ListRemove(kv *KeyValue, n *int) error {
	var kv2 KeyValue
	kv2.Key = genKey(self.bin_name, kv.Key, "list")
	kv2.Value = kv.Value

	var logs List
	bs := self
	primary, backup, curr_log, e := self.action(&kv2, "R")
	if e != nil || primary < 0 {
		return e
	}
	e = self.check_back(primary)
	if e == nil {
		e = bs.clients[primary].ListGet(kv2.Key, &logs)
	}
	if e != nil {
		if backup >= 0 {
			e = self.check_back(backup)
			if e != nil || bs.clients[backup].ListGet(kv2.Key, &logs) != nil {
				return nil
			}
		}
	}

	var clock uint64
	var lclock uint64
	arr := strings.SplitN(curr_log, ",", 3)
	fmt.Sscanf(arr[0], "%25d", &clock)

	sort.Strings(logs.L)
	counts := make(map[string]int)
	for _, log := range logs.L {
		arr = strings.SplitN(log, ",", 3)
		fmt.Sscanf(arr[0], "%25d", &lclock)
		val := arr[2]
		if lclock >= clock {
			break
		}
		if arr[1] == "A" {
			counts[val] += 1
		} else if arr[1] == "R" {
			counts[val] = 0
		}
	}
	*n = counts[kv.Value]

	return nil
}

func (self *mid_client) ListKeys(p *Pattern, list *List) error {
	return self.getKeys(p, list, "list")
}

func (self *mid_client) getClock(atLeast uint64, ret *uint64) (int, int, error) {
	bs := self
	n_backs := len(bs.clients)
	primary := -1
	backup := -1
	now := self.n
	var clock1, clock2 uint64
	e := self.check_back(now)
	if e == nil {
		e = bs.clients[now].Clock(atLeast, &clock1)
	}
	if e != nil {
		now = (now + 1) % n_backs
		for now != self.n {
			e = self.check_back(now)
			if e == nil {
				e = bs.clients[now].Clock(atLeast, &clock1)
			}
			if e == nil {
				break
			}
			now = (now + 1) % n_backs
		}
	}
	if e != nil {
		return primary, backup, e
	}
	primary = now
	*ret = clock1
	now = (now + 1) % n_backs
	for now != self.n {
		e = self.check_back(now)
		if e == nil {
			e = bs.clients[now].Clock(atLeast, &clock2)
		}
		if e == nil {
			backup = now
			if clock1 > clock2 {
				bs.clients[now].Clock(clock1, &clock2)
				*ret = clock1
			} else if clock1 < clock2 {
				bs.clients[primary].Clock(clock2, &clock1)
				*ret = clock2
			}
			break
		}
		now = (now + 1) % n_backs
	}

	return primary, backup, nil
}

func (self *mid_client) Clock(atLeast uint64, ret *uint64) error {
	_, _, e := self.getClock(atLeast, ret)
	return e
}

func logs_to_value(logs *List, value *string) {
	sort.Strings(logs.L)
	if len(logs.L) == 0 {
		*value = ""
	} else {
		log := logs.L[len(logs.L)-1]
		arr := strings.SplitN(log, ",", 3)
		*value = arr[2]
	}
}

func (self *mid_client) getLogs(key string, vtype string) (List, error) {
	encoded_key := genKey(self.bin_name, key, vtype)

	bs := self
	n_backs := len(bs.clients)
	now := self.n
	var logs, logs2 List

	e := self.check_back(now)
	if e == nil {
		e = bs.clients[now].ListGet(encoded_key, &logs)
	}
	if e != nil {
		now = (now + 1) % n_backs
		for now != self.n {
			e = self.check_back(now)
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
		e = self.check_back(now)
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

func (self *mid_client) getKeys(p *Pattern, list *List, vtype string) error {
	nbin_name := Escape(self.bin_name)
	nbin_type := ""
	if vtype == "list" {
		nbin_type = nbin_name + "::list_"
	} else if vtype == "value" {
		nbin_type = nbin_name + "::value_"
	}
	p.Prefix = nbin_type + p.Prefix
	p.Suffix = Escape(p.Suffix)

	bs := self
	n_backs := len(bs.clients)
	now := self.n
	e := self.check_back(now)
	primary := -1
	backup := -1
	var list1, list2, logs List
	if e == nil {
		e = bs.clients[now].ListKeys(p, &list1)
	}
	if e != nil {
		now = (now + 1) % n_backs
		for now != self.n {
			e = self.check_back(now)
			if e == nil {
				e = bs.clients[now].ListKeys(p, &list1)
			}
			if e == nil {
				break
			}
			now = (now + 1) % n_backs
		}
	}
	if e != nil {
		return e
	}
	primary = now

	keys := list1.L
	now = (now + 1) % n_backs
	for now != self.n {
		e = self.check_back(now)
		if e == nil {
			e = bs.clients[now].ListKeys(p, &list2)
		}
		if e == nil {
			backup = now
			break
		}
		now = (now + 1) % n_backs
	}
	if len(list2.L) > len(list1.L) {
		keys = list2.L
	}

	ret := make([]string, 0, len(keys))
	var is_key bool

	for _, key := range keys {
		is_key = false
		e = self.clients[primary].ListGet(key, &logs)
		if e != nil {
			if backup < 0 {
				return e
			}
			e = self.clients[backup].ListGet(key, &logs)
			if e != nil {
				return e
			}
		}
		if vtype == "value" {
			var value string = ""
			logs_to_value(&logs, &value)
			if value != "" {
				is_key = true
			}
		} else if vtype == "list" {
			sort.Strings(logs.L)
			total_count := 0
			counts := make(map[string]int)
			for _, log := range logs.L {
				arr := strings.SplitN(log, ",", 3)
				val := arr[2]
				if arr[1] == "A" {
					counts[val] += 1
					total_count += 1
				} else if arr[1] == "R" {
					total_count -= counts[val]
					counts[val] = 0
				}
			}
			if total_count > 0 {
				is_key = true
			}
		}
		if is_key {
			nkey := strings.TrimPrefix(key, nbin_type)
			nkey = Unescape(nkey)
			ret = append(ret, nkey)
		}
	}
	list.L = ret
	return e
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

func (self *mid_client) action(kv *KeyValue, op string) (int, int, string, error) {
	var kv2 KeyValue
	kv2.Key = kv.Key
	kv2.Value = kv.Value

	primary := -1
	backup := -1

	bs := self
	n_backs := len(bs.clients)
	now := self.n

	var clock uint64
	var succ bool
	var e, e2 error

	primary, backup, e = self.getClock(0, &clock)
	now = primary
	if e != nil {
		return -1, -1, "", e
	}
	e = self.check_back(now)
	if e == nil {
		kv2.Value = fmt.Sprintf("%25d,%s,%s", clock, op, kv.Value)
		e = bs.clients[now].ListAppend(&kv2, &succ)
	}
	if e != nil {
		// backup becomes primary
		if backup >= 0 {
			now = backup
			e = bs.clients[now].ListAppend(&kv2, &succ)
		}
		// backup is also down
		if backup < 0 || e != nil {
			return -1, -1, "", e
		}
		primary = backup
	}

	now = (now + 1) % n_backs
	for now != self.n {
		e2 = self.check_back(now)
		if e2 == nil {
			e2 = bs.clients[now].ListAppend(&kv2, &succ)
		}
		if e2 == nil {
			backup = now
			break
		}
		now = (now + 1) % n_backs
	}
	return primary, backup, kv2.Value, nil
}

func (self *mid_client) check_back(ind int) error {
	// var value string
	// e := self.bin_storage.clients[ind].Get("complete", &value)
	// if e != nil {
	//     return e
	// }
	// if value != "true" {
	//     return fmt.Errorf("not complete")
	// }
	return nil
}
func (self *mid_client) GetTicket(useless bool, succ *bool) error {

	return nil
}
