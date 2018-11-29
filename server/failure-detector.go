package main

import (
	"log"
	"net"
	"net/rpc"
	"time"

	"../shared"
)

const UDPPORT = 6001

/*SWIM fault detection:
	1. Send the udp packet to the 3 peers every 500ms so that we can detect the error
	2. If the server receive the udp packet, send back a message to the source
    3. If the server doesn't receive the udp packet, adding the error counters.
	4. If the error counter exceeds the threshold (3), send failing message to peer nodes
*/
func launchFailureDetection(done chan bool) {
	for {
		for i := 0; i < 3; i++ {
			if peerList[i].Status == 1 {
				UDPSender(peerList[i], time.Now())
			}
		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
	done <- true
}

func UDPSender(receiver shared.Member, tNow time.Time) {
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	dstAddr := &net.UDPAddr{IP: net.ParseIP(receiver.Ip), Port: UDPPORT}

	conn, err := net.DialUDP("udp", monitorAddr, dstAddr)
	if err != nil {
		log.Printf("UDP Sender dial fail caused by: %s \n", err)
	}
	defer conn.Close()

	conn.Write([]byte("hi"))
	// log.Printf("UDP sender send \"hi\" to <%s> ", conn.RemoteAddr())

	// Ping back from UDP listener
	data := make([]byte, 16)
	_, err = conn.Read(data)

	if err != nil {
		// Cannot hear back from receiver, which means receiver may fail
		memberList[receiver.Id].UnresponseCount++
		if memberList[receiver.Id].UnresponseCount > 3 {
			log.Printf("ID: <%d> fail!", receiver.Id)
			memberList[receiver.Id].Status = 0
			log.Printf("ID: <%d> fail!", receiver.Id)
			memberList[receiver.Id].TimeStamp = tNow
			log.Printf("ID: <%d> fail!", receiver.Id)
			updatePeerList()
			log.Printf("ID: <%d> fail!", receiver.Id)

			//gossip update to live peers with multiple goroutine
			channel := make(chan *rpc.Call, NUMOFPEER)
			for _, member := range peerList {
				log.Printf("ID: <%d> fail!", receiver.Id)
				args := &shared.GossipMsg{Msg: "fail", Id: receiver.Id, Ip: receiver.Ip, TimeStamp: memberList[receiver.Id].TimeStamp}
				if member.Status == 1 {
					SendGossipAsync(args, member.Ip, channel)
				} else {
					channel <- &rpc.Call{}
				}
			}
			for i := 0; i < NUMOFPEER; i++ {
				gCall := <-channel
				checkErr(gCall.Error)
			}
		}
	} else {
		memberList[receiver.Id].UnresponseCount = 0
	}
}

func UDPReceiver(done chan bool) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: UDPPORT})
	checkErr(err)

	data := make([]byte, 16)
	for {
		n, remoteAddr, err := listener.ReadFromUDP(data)
		checkErr(err)
		str := data[:n]
		_, err = listener.WriteToUDP([]byte(str), remoteAddr)
		checkErr(err)
	}
	done <- true
}
