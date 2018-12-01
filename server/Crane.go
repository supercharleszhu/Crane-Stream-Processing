package main

import (
	"bufio"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"../shared"
)

var Cache map[int]interface{}
var MasterIp string
var standByMasterIp string

var workerIP = make([]string, 0)
var AckerIp string
var message = make(map[int]string)

const CRANEPORT = 5001

type App interface {
	join(message string)
	transform(message string)
	mergeCache(messageId int)
	getAckVal() int
	setAckVal(ackVal int)
	getMessageId() int
	setMessageId(id int)
}

var currApp App
var currAppName string

func sendAck(messageId int, ackVal int) {
	messageAck := "ack " + strconv.Itoa(messageId) + " " + strconv.Itoa(ackVal)
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	ackerAddr := &net.UDPAddr{IP: net.ParseIP(MasterIp), Port: UDPPORT}
	conn, Err := net.DialUDP("udp", monitorAddr, ackerAddr)
	defer conn.Close()
	checkErr(Err)
	conn.Write([]byte(messageAck))
	log.Printf("send messageId %d: Ack\n", messageId)

}

// App Configuration. Modify this if new app is added
func startApp(appName string) {
	currAppName = appName
	if appName == "wordCount" {
		currApp = &wordCount{
			result:    map[string]int{},
			messageId: 0,
			ackVal:    0,
			counter:   0,
		}
	}
}

func sendMessageSink(ackVal int, messageId int, message string) {
	messageSink := "join " + strconv.Itoa(messageId) + " " + strconv.Itoa(ackVal) + " " + message
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	sinkAddr := &net.UDPAddr{IP: net.ParseIP(SinkIp), Port: UDPPORT}
	conn, Err := net.DialUDP("udp", monitorAddr, sinkAddr)
	defer conn.Close()
	checkErr(Err)
	conn.Write([]byte(messageSink))
	log.Printf("send messageId %d: Ack\n", messageId)
}

func sendMessageWorker(task string, ackVal int, messageId int, message string, workerIp string) {
	//TODO: send message to worker
	messageWorker := task + " " + strconv.Itoa(messageId) + " " + strconv.Itoa(ackVal) + " " + message
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	workerAddr := &net.UDPAddr{IP: net.ParseIP(workerIp), Port: UDPPORT}
	conn, Err := net.DialUDP("udp", monitorAddr, workerAddr)
	defer conn.Close()
	checkErr(Err)
	conn.Write([]byte(messageWorker))
	log.Printf("send messageId %d: Ack\n", messageId)
}

func parseMessage(rawMessage string) {
	messageArr := strings.Fields(rawMessage)
	task := messageArr[0]
	messageId, err := strconv.Atoi(messageArr[1])
	ackVal, err := strconv.Atoi(messageArr[2])
	data := messageArr[3]
	if err != nil {
		log.Println("parseMessage: Fail!", err)
	}
	currApp.setMessageId(messageId)
	currApp.setAckVal(ackVal)
	if task == "join" {
		currApp.join(data)
	} else {
		currApp.transform(data)
	}
}

// start application
func (r *Crane) StartApp(args *shared.App, reply *shared.WriteAck) error {

	// Fetch the demo-data from sdfs to local dir
	req := &shared.SDFSMsg{Type: "get", LocalFileName: "data", SDFSFileName: "demo-data", TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+RPCPORT)
	checkErr(err)
	reply = &shared.WriteAck{}
	err = client.Call("SDFS.GetReq", req, reply)

	// the VM with smallest ID serves as master
	// client, err = rpc.Dial("tcp", MasterIp+":"+RPCPORT)
	// if err != nil {
	// 	log.Printf("Start %s fails", args.AppName)
	// 	return nil
	// }
	// log.Printf("Start %s succeeds\n", args.AppName)

	// Assign roles
	assignRoles()

	// Tell all nodes which application is running
	sendAppName(args.AppName)

	// access the file
	absPath, _ := filepath.Abs("./duplication/data")
	file, err := os.Open(absPath)
	checkErr(err)
	defer file.Close()
	br := bufio.NewReader(file)

	// start sending data stream
	line := 0
	for _, ip := range workerIP {
		n, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		// set up id and random number
		line++
		ackVal := int(rand.Int31n(255))

		// Send data to worker (task messageID ackVal data)
		// TODO: implement transform logic
		sendMessageWorker("transform", ackVal, ackVal, string(n), ip)

		// Record the data into message map
		message[line] = string(n)

		// send ack to Acker (ack messageID ackVal)
		sendAck(line, ackVal)

	}

	// // TODO: when UDPReceiver receives an ack on a message, the message will be removed in map

	// send all message in the message map again to worker, if the message is not empty
	for len(message) != 0 {
		for line, data := range message {
			// connWorker, err := net.Dial("udp", workerIP[0]+":"+"8888")
			// if err != nil {
			// 	fmt.Println(err)
			// }
			// defer connWorker.Close()

			ackVal := int(rand.Int31n(255))

			// connWorker.Write([]byte(data + " " + strconv.Itoa(ackVal) + "\n"))
			sendMessageWorker("transform", ackVal, line, data, workerIP[0])

			// // Record the data into message map
			// message[line] = string(n)

			// send message to Acker
			// connMaster.Write([]byte("ack " + strconv.Itoa(line) + " " + strconv.Itoa(ackVal)))
			sendAck(line, ackVal)
		}
	}

	// Answer back to the client CLI
	reply.Finish = true
	return nil
}

func sendAppName(appName string) {

	// specify the app name
	message := "start " + appName
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}

	// Send to master
	ackerAddr := &net.UDPAddr{IP: net.ParseIP(MasterIp), Port: UDPPORT}
	conn, err := net.DialUDP("udp", monitorAddr, ackerAddr)
	checkErr(err)
	// defer conn.Close()
	conn.Write([]byte(message))
	conn.Close()
	log.Printf("send start %s to master\n", appName)

	// Send to Sink
	sinkAddr := &net.UDPAddr{IP: net.ParseIP(SinkIp), Port: UDPPORT}
	conn, Err := net.DialUDP("udp", monitorAddr, sinkAddr)
	// defer conn.Close()
	checkErr(Err)
	conn.Write([]byte(message))
	conn.Close()
	log.Printf("send start %s to sink\n", appName)

	// Send to workers
	for _, ip := range workerIP {
		workerAddr := &net.UDPAddr{IP: net.ParseIP(ip), Port: UDPPORT}
		conn, Err := net.DialUDP("udp", monitorAddr, workerAddr)
		checkErr(Err)
		conn.Write([]byte(message))
		conn.Close()
		log.Printf("send start %s to workers\n", appName)

	}
}

func assignRoles() {

	counter := 0
	for _, member := range memberList {
		if counter == 0 {
			SpoutIp = member.Ip
		} else if counter == 1 {
			MasterIp = member.Ip
			AckerIp = member.Ip
		} else if counter == 2 {
			standByMasterIp = member.Ip
		} else if counter == len(memberList)-1 {
			SinkIp = member.Ip
		} else {
			workerIP = append(workerIP, member.Ip)
		}

		counter++
	}

}

func deleteMessage(id string) {
	ID, err := strconv.Atoi(id)
	checkErr(err)
	delete(message, ID)
}

// func (r *Crane) MasterStart(args *shared.App, reply *shared.EmptyReq) error {
//
// }
