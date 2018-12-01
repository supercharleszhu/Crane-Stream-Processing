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
	currTime := time.Now()
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	spoutAddr := &net.UDPAddr{IP: net.ParseIP(SpoutIp), Port: UDPPORT}
	sinkAddr := &net.UDPAddr{IP: net.ParseIP(SinkIp), Port: UDPPORT}
	conn1, Err = net.DialUDP("udp", monitorAddr, spoutAddr)
	conn2, Err = net.DialUDP("udp", monitorAddr, sinkAddr)
	checkErr(Err)
	defer conn1.Close()
	defer conn2.Close()

	for {
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
