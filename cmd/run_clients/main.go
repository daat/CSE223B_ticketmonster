package main

import (
    "fmt"
    "net"
)

func main() {
    prefix := "169.228.66"
    suffices := []string{"159", "160"}
    port := 17898
    for i, s := range suffices {
        conn, e := net.Dial("tcp", fmt.Sprintf("%v.%v:%v", prefix, s, port))
        if e != nil {
            fmt.Println(e)
            return
        }
        conn.Write([]byte(fmt.Sprintf("%v", i)))
    }
}
