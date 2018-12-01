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
var StopApp = false
var MasterIp string
var standByMasterIp string

var workerIP = make([]string, 0)
var AckerIp string
var message = make(map[int]string)

const CRANEPORT = 5001

var Period = 10000 * time.Millisecond // millisecond
var SendPeriod = 200 * time.Millisecond
var Ticker = time.NewTicker(Period)

// Crane RPC server
type Crane int

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

		time.Sleep(SendPeriod)
		// set up id and random number
		line++
		ackVal := int(rand.Int31n(255))

		// Send data to worker (task messageID ackVal data)
		// TODO: implement transform logic
		sendMessageWorker("transform", ackVal, line, string(n), ip)

		// Record the data into message map
		message[line] = string(n)

		// send ack to Acker (ack messageID ackVal)
		sendAck(line, ackVal)

	}

	// // TODO: when UDPReceiver receives an ack on a message, the message will be removed in map

	// send all message in the message map again to worker
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

	// Answer back to the client CLI
	reply.Finish = true
	return nil
}

//func (r *Crane) StartApp(args *shared.App, reply *shared.WriteAck) error {
//
//	// the VM with smallest ID serves as master
//	client, err := rpc.Dial("tcp", memberList[1].Ip+":"+RPCPORT)
//	if err != nil {
//		fmt.Printf("Start %s fails", args.AppName)
//		return nil
//	}
//	fmt.Printf("Start %s succeeds", args.AppName)
//	err = client.Call("Crane.MasterStart", args, &shared.EmptyReq{})
//	checkErr(err)
//
//	// start emitting data streaming
//
//	reply.Finish = true
//	return nil
//}

func (r *Crane) RecStopApp(args *shared.CraneMsg, reply *shared.WriteAck) {
	StopApp = true
	reply.Finish = true
	return
}

func (r *Crane) StopApp(args *shared.CraneMsg, reply *shared.WriteAck) {
	//broadcast the stop command
	channel := make(chan *rpc.Call, NUMOFVM)
	for _, member := range memberList {
		if member.Status == 1 && member.Ip != MasterIp && member.Id != ID {
			sendCommandAsync(args, member.Ip, channel)
		} else {
			channel <- &rpc.Call{}
		}
	}
	for i := 0; i < NUMOFVM; i++ {
		gCall := <-channel
		checkErr(gCall.Error)
	}
}

func sendCommandAsync(args *shared.CraneMsg, ip string, channel chan *rpc.Call) {
	client, err := rpc.Dial("tcp", ip+":"+RPCPORT)
	if err != nil {
		channel <- &rpc.Call{}
		return
	}
	log.Printf("broadcasting Command to start/stop app: %s\n", args.Command)
	reply := &shared.WriteAck{}
	if args.Command == "start" {
		gCall := client.Go("Crane.RecStartApp", args, reply, channel)
		checkErr(gCall.Error)
	} else {
		gCall := client.Go("Crane.RecStopApp", args, reply, channel)
		checkErr(gCall.Error)
	}
}

//App Part of Crane
type App interface {
	join(message string)
	transform(message string)
	mergeCache(messageId int)
	getAckVal() int
	setAckVal(ackVal int)
	getMessageId() int
	setMessageId(id int)
	writeToSDFS()
}

var currApp App
var currAppName string

func CraneTimer() {
	for t := range Ticker.C {
		if currApp != nil {
			currApp.writeToSDFS()
			log.Printf("App %s write to SDFS at %s\n", currAppName, t.String())
		}
		if StopApp {
			currApp = nil
			log.Printf("App %s stopped at %s\n", currAppName, t.String())
		}
	}
}

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
	Cache = make(map[int]interface{}) //truncate the cache
	StopApp = false
	currAppName = appName
	if appName == "wordCount" {
		currApp = &wordCount{
			result:    map[string]int{},
			messageId: 0,
			ackVal:    0,
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
	MasterIp = memberList[1].Ip
	AckerIp = memberList[1].Ip
	SpoutIp = memberList[0].Ip
	standByMasterIp = memberList[2].Ip
	SinkIp = memberList[len(memberList)-1].Ip

	for _, member := range memberList {
		if member.Ip != MasterIp && member.Ip != SpoutIp && member.Ip != standByMasterIp && member.Ip != SinkIp {
			workerIP = append(workerIP, member.Ip)
		}
	}
}

// func (r *Crane) MasterStart(args *shared.App, reply *shared.EmptyReq) error {
//
// }
