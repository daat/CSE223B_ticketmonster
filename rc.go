package ticketmonster

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var DefaultRCPath = "bins.rc"

type RC struct {
	PrimaryBacks   []string
    BackupBacks []string
	TicketServers []string
}

func (self *RC) BackCount() int {
	return len(self.PrimaryBacks)
}

func LoadRC(p string) (*RC, error) {
	fin, e := os.Open(p)
	if e != nil {
		return nil, e
	}
	defer fin.Close()

	ret := new(RC)
	e = json.NewDecoder(fin).Decode(ret)
	if e != nil {
		return nil, e
	}

	return ret, nil
}

func (self *RC) marshal() []byte {
	b, e := json.MarshalIndent(self, "", "    ")
	if e != nil {
		panic(e)
	}

	return b
}

func (self *RC) Save(p string) error {
	b := self.marshal()

	fout, e := os.Create(p)
	if e != nil {
		return e
	}

	_, e = fout.Write(b)
	if e != nil {
		return e
	}

	_, e = fmt.Fprintln(fout)
	if e != nil {
		return e
	}

	return fout.Close()
}

func (self *RC) String() string {
	b := self.marshal()
	return string(b)
}
