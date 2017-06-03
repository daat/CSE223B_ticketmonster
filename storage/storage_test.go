package storage_test

import (
	"testing"
    "runtime/debug"
	"ticketmonster/storage"
    "fmt"
    "time"
    "strings"
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

    time.Sleep(100 * time.Millisecond)

    bs := storage.BinStorageClient{Backs: primaryAddrs}
    bs.Init()
    bin := bs.Bin("0")
    kv := storage.KeyValue{Key: "uuu", Value: "buy 2"}
    var succ bool
    var list storage.List

    // append one get one
    ne(bin.ListAppend(&kv, &succ))
    ne(bin.ListGet(kv.Key, &list))
    as(len(list.L) == 1)
    as(kv.Value == list.L[0])

    // append one to existing list
    ne(bin.ListAppend(&kv, &succ))
    ne(bin.ListGet(kv.Key, &list))
    as(len(list.L) == 2)
    as(kv.Value == list.L[0])
    as(kv.Value == list.L[1])

    // get empty list
    ne(bin.ListGet("empty", &list))
    as(len(list.L) == 0)

    // init pool
    kv = storage.KeyValue{Key: "TICKETPOOL", Value: "PUT,1000,1000"}
    ne(bin.ListAppend(&kv, &succ))

    // get pool
    kv = storage.KeyValue{Key: "TICKETPOOL", Value: "GET,100"}
    ne(bin.AccessPool(&kv, &list))
    as(strings.Split(list.L[0], ",")[3] == "900")

}
