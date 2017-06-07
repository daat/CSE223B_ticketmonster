package main

import (
	"flag"
	"fmt"
	"log"
	//"strconv"
	"time"

	"ticketmonster"
    "ticketmonster/ticket"
    //"ticketmonster/local"
)

var (
	frc = flag.String("rc", ticketmonster.DefaultRCPath, "bin storage config file")
)


func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()


	rc, e := ticketmonster.LoadRC("bins.rc")
	if e!= nil{
		log.Printf("%v", e)
		return 
	}

	run := func(i int, ready chan bool) {
		if i > len(rc.PrimaryBacks) {
			noError(fmt.Errorf("back-end index out of range: %d", i))
		}

		tsConfig := rc.TSConfig(i)
		tsConfig.Ready = ready
		_ = ticket.NewTicketServer(tsConfig)
		log.Printf("ticket server serving on %s", rc.TicketServers_out[i])
	}
	
	n := rc.TicketServerCount()

	poolready := make(chan bool)
    go run(0, poolready)
    if <-poolready{
        log.Printf("pool ready")
    }

    ready1 := make(chan bool)
    for i := 1; i < n; i++ {
        go run(i, ready1)
    }
    for i := 1; i < n; i++ {
        <- ready1
    }

	
	for {
		time.Sleep(time.Second * 30)
        log.Print("ticket server running \n")
        
    }
}
