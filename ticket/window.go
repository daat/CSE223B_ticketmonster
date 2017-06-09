package ticket

import (
	"ticketmonster/storage"
)

type BuyInfo struct{
	Uid string
	N int
}

type Window interface{
	// let client 'uid' to buy 'n' ticket
	BuyTicket(in *BuyInfo, succ *bool) error

	// get current ticket server left tickets
	GetLeftTickets(useless bool, n *int) error

	// get current left tickets from all ticket server
	GetAllTickets(useless bool, ret *storage.List) error


}

