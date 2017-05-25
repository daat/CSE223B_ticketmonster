// Tribbler back-end launcher.
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
    "ticketmonster"
    "ticketmonster/storage"
)

var (
	frc       = flag.String("rc", "bins.rc", "bin storage config file")
)

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	rc, e := ticketmonster.LoadRC(*frc)
	noError(e)

	run := func(i int) {
		if i > len(rc.PrimaryBacks) {
			noError(fmt.Errorf("back-end index out of range: %d", i))
		}

        var back storage.PrimaryBackend
		noError(back.Serve(&storage.BackConfig{PrimaryAddrs: rc.PrimaryBacks, BackupAddrs: rc.BackupBacks, This: i}))
	}

	args := flag.Args()

	n := 0
	if len(args) == 0 {
		// scan for addresses on this machine
		for i, b := range rc.PrimaryBacks {
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
