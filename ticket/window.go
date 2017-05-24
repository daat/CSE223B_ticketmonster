package ticket


type BuyInfo struct{
	Uid string
	N int
}

type Window interface{
	// let client 'uid' to buy 'n' ticket
	BuyTicket(in *BuyInfo, succ *bool) error

	// get current ticket server left tickets
	GetLeftTickets(useless bool, n *int) error
}