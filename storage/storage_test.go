package storage_test

import (
	"testing"
    "runtime/debug"
	"ticketmonster/storage"
    "fmt"
    "time"
)

func TestBackend(t *testing.T) {
    ne := func(e error) {
        if e != nil {
            debug.PrintStack()
            t.Fatal(e)
        }
    }

    as := func(cond bool) {
        if !cond {
            debug.PrintStack()
            t.Fatal("assertion failed")
        }
    }
    n := 3
    primaryAddrs := make([]string, n)
    backupAddrs := make([]string, n)
    for i := 0; i < n; i++ {
        primaryAddrs[i] = fmt.Sprintf("localhost:%d", 15000+i)
        backupAddrs[i] = fmt.Sprintf("localhost:%d", 16000+i)
    }
    backs := make([]storage.PrimaryBackend, n)
    for i := 0; i < n; i++ {
        ne(backs[i].Serve(&storage.BackConfig{PrimaryAddrs: primaryAddrs, BackupAddrs: backupAddrs, This: i}))
    }

    time.Sleep(200 * time.Millisecond)

    bs := storage.BinStorageClient{Backs: primaryAddrs}
    bs.Init()
    bin := bs.Bin("0")
    kv := storage.KeyValue{Key: "uuu", Value: "buy 2"}
    var succ bool
    var list storage.List
    // bin.ListGet(kv.Key, &list)
    ne(bin.ListAppend(&kv, &succ))
    bin.ListGet(kv.Key, &list)
    as(len(list.L) == 1)
    as(kv.Value == list.L[0])
}
