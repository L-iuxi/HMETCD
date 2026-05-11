package main

import (
	"TicketX/clerk"
	"time"
)

func main() {

	ck := clerk.NewClerk()

	go clerk.TWatch(ck, "a")

	time.Sleep(time.Second)

	clerk.TPut(ck, "a", "1")

	time.Sleep(time.Second)

	clerk.TPut(ck, "a", "2")

	time.Sleep(time.Second)

	clerk.TPut(ck, "a", "3")

	time.Sleep(time.Second)
}
