package main

import (
	"log"
	"net"
	"strconv"
	"time"
)

type Ack struct {
	ackVal    int
	createdAt time.Time
}

const TIMEOUT = 3000

var Acker = make(map[int]*Ack)
var conn1, conn2 *net.UDPConn
var Err error

var SpoutIp string
var SinkIp string

func launchAcker() {

	for {
		currTime := time.Now()
		for messageId, ack := range Acker {

			if currTime.Sub(ack.createdAt) >= TIMEOUT*time.Millisecond {
				//send fail message to spout
				messageFail := "messageFail " + strconv.Itoa(messageId)
				conn1.Write([]byte(messageFail))
				log.Printf("send messageId %d: Fail\n", messageId)

				//send abort message to sink
				messageAbort := "messageAbort " + strconv.Itoa(messageId)
				conn2.Write([]byte(messageAbort))
				log.Printf("send messageId %d: Abort\n", messageId)

				//delete item in the map
				delete(Acker, messageId)
			}
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
	}
	conn1.Close()
	conn2.Close()
}

func handleAck(messageId int, ackVal int) {
	if _, ok := Acker[messageId]; !ok {
		Acker[messageId] = &Ack{
			ackVal:    ackVal,
			createdAt: time.Now(),
		}
	} else {
		Acker[messageId].ackVal ^= ackVal
		if Acker[messageId].ackVal == 0 {
			// send success message to spout
			messageSuccess := "messageSuccess " + strconv.Itoa(messageId)
			conn1.Write([]byte(messageSuccess))
			log.Printf("send messageId %d: Success\n", messageId)

			// send commit message to sink
			messageCommit := "messageCommit " + strconv.Itoa(messageId)
			conn2.Write([]byte(messageCommit))
			log.Printf("send messageId %d: Commit\n", messageId)

			// delete item in the map
			delete(Acker, messageId)
		}
	}
}
