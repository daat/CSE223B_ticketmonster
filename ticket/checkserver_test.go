package ticket_test

import (
	"runtime"
	"runtime/debug"
	"fmt"
	//"strconv"
	"testing"

	"ticketmonster/ticket"
)



func CheckServerConcur(t *testing.T, ts ticket.TicketServer) {
	runtime.GOMAXPROCS(2)

	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}
	/*
	er := func(e error) {
		if e == nil {
			debug.PrintStack()
			t.Fatal()
		}
	}
	

	as := func(cond bool) {
		if !cond {
			debug.PrintStack()
			t.Fatal()
		}
	}
	*/


	p := func(th, n int, done chan<- bool) {
		for i := 1; i <= n; i++ {
			ne(ts.BuyTicket("u123", n))
		}
		done <- true
	}

	nconcur := 5
	done := make(chan bool, nconcur)
	for i := 0; i < nconcur; i++ {
		go p(i, 10, done)
	}

	for i := 0; i < nconcur; i++ {
		<-done
	}

	//as(ts.GetLeftTickets() == 725) 
	v := ts.GetLeftTickets()
	fmt.Printf("Ticket left %v\n", v)

}