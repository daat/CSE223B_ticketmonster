package storage

import (
	"net"
)

func NewBinClient(backs []string) BinStorage {
	bc := &binStorageClient{backs: backs}
	bc.Init()
	return bc
}

func ServeKeeper(kc *KeeperConfig) error {
	l, e := net.Listen("tcp", kc.Addrs[kc.This])
	if e != nil {
		return e
	}
	k := &keeper{kc: kc, listener: l}
	k.Init()
	if kc.Ready != nil {
		kc.Ready <- true
	}
	/*t := time.NewTicker(time.Second)
	for {
		// update clk with all back-end
		k.update_clk()
		<-t.C
	}*/
	go k.HeartBeat(nil)
	return nil
}
