package main

import "TicketX/clerk"

func main() {
	ck := clerk.NewClerk()
	clerk.TPut(ck, "a", "1")
	clerk.TGet(ck, "a")
}
