package main

import (
	"fmt"
	"time"
)

var NumEmails = 1000000000
var WorkPerEmail = 10

func main() {

	startTime := time.Now()

	fmt.Println("----- Requesting All Emails")
	fmt.Println("----- Requesting Latest Emails")

	go getAllEmails(startTime)
	getLastEmail(startTime)

	time.Sleep(10000 * time.Millisecond)

}

func getEmail() {
	x := 1
	for i := 1; i < WorkPerEmail; i++ {
		x = x * i
		x = x / i
	}
}

func getAllEmails(startTime time.Time) {

	for i := 0; i < NumEmails; i++ {
		getEmail()
	}

	elapedTime := time.Now().Sub(startTime)

	fmt.Println("----- Got All Emails in: ", elapedTime)

}

func getLastEmail(startTime time.Time) {

	getEmail()

	elapedTime := time.Now().Sub(startTime)

	fmt.Println("----- Got Latest Emails in: ", elapedTime)

}
