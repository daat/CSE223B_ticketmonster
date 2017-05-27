package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"

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


	rc, _ := ticketmonster.LoadRC("bins.rc")

	run := func(i int) {
		if i > len(rc.PrimaryBacks) {
			noError(fmt.Errorf("back-end index out of range: %d", i))
		}
		ts := ticket.NewTicketServer(rc.PrimaryBacks, strconv.Itoa(i), rc.TicketServers[i])
		noError(ts.Init(100000))
		log.Printf("ticket server serving on %s", rc.TicketServers[i])
	}

	args := flag.Args()

	n := 0
	if len(args) == 0 {
		// scan for addresses on this machine
		for i, b := range rc.TicketServers {
			if local.Check(b) {
				go run(i)
				n++
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
			go run(i)
			n++
		}
	}

	if n > 0 {
		select {}
	}
}