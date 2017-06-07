package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"ticketmonster"
    "ticketmonster/ticket"
    "ticketmonster/local"
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
	servers := make([]*ticket.TicketServer, 0, rc.TicketServerCount())
	run := func(i int, ready chan bool) {
		if i > len(rc.PrimaryBacks) {
			noError(fmt.Errorf("ticket server index out of range: %d", i))
		}

		tsConfig := rc.TSConfig(i)
		tsConfig.Ready = ready
		ts := ticket.NewTicketServer(tsConfig)

		servers = append(servers, &ts)
		log.Printf("ticket server serving on %s", rc.TicketServers_out[i])
	}
	
	/*
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
    */

    args := flag.Args()

	n := 0
	myId := 0
	if len(args) == 0 {
		// scan for addresses on this machine
		for i, b := range rc.TicketServers_out {
			if local.Check(b) {
				if i == 0 {
					poolready := make(chan bool)
				    go run(0, poolready)
				    if <-poolready{
				        log.Printf("server %d ready, pool ready", i)
				    }
				    if n==0 {
				    	myId = i
				    }
				    n++
				} else {
					ready1 := make(chan bool)
					go run(i, ready1)
					if <- ready1 {
						log.Printf("server %d ready", i)
					}
					if n==0 {
				    	myId = i
				    }
					n++
				}
			}
		}

		if n == 0 {
			log.Fatal("no back-end found for this host")
		}
	} else {
		// scan for indices for the addresses
		for _, a := range args {
			i, e := strconv.Atoi(a)
			noError(e)
			ready1 := make(chan bool)
			run(i, ready1)
			if <- ready1 {
				log.Printf("server %d ready", i)
			}
			if n==0 {
				myId = i
			}
			n++
		}
	}

	for {
		time.Sleep(time.Second * 30)
		var num int
		servers[0].GetLeftTickets(true, &num)
		fmt.Printf("ticketserver %d tickets %d\n", myId, num)
	}
	
}
