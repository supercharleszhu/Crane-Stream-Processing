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
var StopApp = true
var Restart = false
var MasterIp string
var standByMasterIp string

var workerIP = make([]string, 0)
var message = make(map[int]string)

const CRANEPORT = 5001

var Period = 10000 * time.Millisecond // millisecond
var SendPeriod = 100 * time.Millisecond
var Ticker = time.NewTicker(Period)

// Crane RPC server
type Crane int

// start application
func (r *Crane) StartApp(args *shared.App, reply *shared.WriteAck) error {
	StopApp = false
	// Fetch the demo-data from sdfs to local dir
	Ticker = time.NewTicker(args.Period)
	SendPeriod = args.SendPeriod
	req := &shared.SDFSMsg{Type: "get", LocalFileName: "data", SDFSFileName: "demo-data", TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+RPCPORT)
	checkErr(err)
	reply = &shared.WriteAck{}
	err = client.Call("SDFS.GetReq", req, reply)

	currAppName = args.AppName
	sendAppName(args.AppName)

	// access the file
	absPath, _ := filepath.Abs("./duplication/data")
	file, err := os.Open(absPath)
	checkErr(err)
	defer file.Close()
	br := bufio.NewReader(file)

	// start sending data stream
	line := 0

	counter := 0
	for {

		n, _, err := br.ReadLine()

		if err == io.EOF {
			break
		}
		if Restart {
			log.Println("Restart the App, aborting the current goroutine...")
			Restart = false
			return nil
		}
		if len(n) == 0 {
			continue
		}

		counter = (counter + 1) % len(workerIP)
		ip := workerIP[counter]

		//log.Println(string(n))

		time.Sleep(SendPeriod)
		// set up id and random number
		line++
		ackVal := int(rand.Int31())

		// Send data to worker (task messageID ackVal data)
		// TODO: implement transform logic
		sendMessageWorker("transform", ackVal, line, string(n), ip)

		// Record the data into message map
		message[line] = string(n)

		// send ack to Acker (ack messageID ackVal)
		sendAck(line, ackVal)

	}

	time.Sleep(TIMEOUT * time.Millisecond)

	// // TODO: when UDPReceiver receives an ack on a message, the message will be removed in map

	// send all message in the message map again to worker, if the message is not empty
	for len(message) != 0 {
		for line, data := range message {
			// connWorker, err := net.Dial("udp", workerIP[0]+":"+"8888")
			// if err != nil {
			// 	fmt.Println(err)
			// }
			// defer connWorker.Close()
			ackVal := int(rand.Int31())

			// connWorker.Write([]byte(data + " " + strconv.Itoa(ackVal) + "\n"))
			counter = (counter + 1) % len(workerIP)
			sendMessageWorker("transform", ackVal, line, data, workerIP[counter])
			sendAck(line, ackVal)

			time.Sleep(SendPeriod)
			// // Record the data into message map
			// message[line] = string(n)

			// send message to Acker
			// connMaster.Write([]byte("ack " + strconv.Itoa(line) + " " + strconv.Itoa(ackVal)))
		}
		time.Sleep(TIMEOUT * time.Millisecond)
	}

	// Answer back to the client CLI
	crane := new(Crane)
	newargs := &shared.CraneMsg{
		AppName: currAppName,
	}
	res := &shared.WriteAck{}
	crane.StopApp(newargs, res)

	reply.Finish = true
	return nil
}

func (r *Crane) RecStopApp(args *shared.CraneMsg, reply *shared.WriteAck) error {
	StopApp = true
	reply.Finish = true
	return nil
}

func (r *Crane) StopApp(args *shared.CraneMsg, reply *shared.WriteAck) error {
	//broadcast the stop command
	channel := make(chan *rpc.Call, NUMOFVM)
	for _, member := range memberList {
		if member.Status == 1 && member.Ip != MasterIp && member.Id != ID {
			sendStopAsync(args, member.Ip, channel)
		} else {
			channel <- &rpc.Call{}
		}
	}
	for i := 0; i < NUMOFVM; i++ {
		gCall := <-channel
		checkErr(gCall.Error)
	}
	reply.Finish = true
	return nil
}

func sendStopAsync(args *shared.CraneMsg, ip string, channel chan *rpc.Call) {
	client, err := rpc.Dial("tcp", ip+":"+RPCPORT)
	if err != nil {
		channel <- &rpc.Call{}
		return
	}
	log.Printf("broadcasting Command to stop app\n")
	reply := &shared.WriteAck{}
	gCall := client.Go("Crane.RecStopApp", args, reply, channel)
	checkErr(gCall.Error)
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
		if currApp != nil && SELFIP == SinkIp {
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
	log.Printf("send messageId %d to %s: Ack\n", messageId, MasterIp)

}

// App Configuration. Modify this if new app is added
func startApp(appName string) {
	assignRoles()
	Cache = make(map[int]interface{}) //truncate the cache
	Acker = make(map[int]*Ack)        //truncate the Acker cache
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
	log.Printf("send messageId %d to %s: join\n", messageId, SinkIp)
}

func sendMessageWorker(task string, ackVal int, messageId int, message string, workerIp string) {
	//TODO: send message to worker
	messageWorker := task + " " + strconv.Itoa(messageId) + " " + strconv.Itoa(ackVal) + " " + message
	log.Println("messageWorker: " + messageWorker)
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	workerAddr := &net.UDPAddr{IP: net.ParseIP(workerIp), Port: UDPPORT}
	conn, Err := net.DialUDP("udp", monitorAddr, workerAddr)
	defer conn.Close()
	checkErr(Err)
	conn.Write([]byte(messageWorker))
	log.Printf("send messageId %d: %s to %s\n", messageId, task, workerIP)
}

func parseMessage(rawMessage string) {
	messageArr := strings.Fields(rawMessage)
	task := messageArr[0]
	messageId, err := strconv.Atoi(messageArr[1])
	ackVal, err := strconv.Atoi(messageArr[2])
	data := strings.Join(messageArr[3:], " ")
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

func AbortCache(messageId int) {
	delete(Cache, messageId)
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

	// Send to stand by master
	ackerAddr = &net.UDPAddr{IP: net.ParseIP(standByMasterIp), Port: UDPPORT}
	conn, err = net.DialUDP("udp", monitorAddr, ackerAddr)
	checkErr(err)
	// defer conn.Close()
	conn.Write([]byte(message))
	conn.Close()
	log.Printf("send start %s to standby master\n", appName)

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
	aliveIp := make([]string, 0)
	counter := 0
	for _, member := range memberList {
		if member.Status == 1 {
			aliveIp = append(aliveIp, member.Ip)
			counter++
		}
	}
	if counter < 5 {
		log.Printf("not enough worker\n")
		return
	}

	newSpoutIp := aliveIp[0]
	newSinkIp := aliveIp[counter-1]
	MasterIp = aliveIp[1]
	standByMasterIp = aliveIp[2]
	workerIP = aliveIp[3 : counter-1]
	if SELFIP == aliveIp[0] && SpoutIp != newSpoutIp {
		crane := new(Crane)
		SpoutIp = newSpoutIp
		args := &shared.App{
			AppName:    currAppName,
			Period:     Period,
			SendPeriod: SendPeriod,
		}
		log.Println("new spout: restarting app!")
		res := &shared.WriteAck{}
		if StopApp == false {
			go crane.StartApp(args, res)
		}
	}
	SpoutIp = newSpoutIp
	if SELFIP == SpoutIp && SinkIp != newSinkIp {
		crane := new(Crane)
		SinkIp = newSinkIp
		args := &shared.App{
			AppName:    currAppName,
			Period:     Period,
			SendPeriod: SendPeriod,
		}
		res := &shared.WriteAck{}
		if StopApp == false {
			Restart = true
			time.Sleep(TIMEOUT * time.Millisecond)
			message = make(map[int]string)
			go crane.StartApp(args, res)
		}
	}
	SinkIp = newSinkIp
	monitorAddr := &net.UDPAddr{IP: net.ParseIP(SELFIP), Port: 0}
	spoutAddr := &net.UDPAddr{IP: net.ParseIP(SpoutIp), Port: UDPPORT}
	sinkAddr := &net.UDPAddr{IP: net.ParseIP(SinkIp), Port: UDPPORT}
	conn1, Err = net.DialUDP("udp", monitorAddr, spoutAddr)
	conn2, Err = net.DialUDP("udp", monitorAddr, sinkAddr)
	checkErr(Err)
	log.Println("Spout Ip" + SpoutIp)
	log.Println("Master Ip" + MasterIp)
	log.Println("standbyMaster IP" + standByMasterIp)
	for i := 0; i < len(workerIP); i++ {
		log.Println("worker IP " + strconv.Itoa(i) + workerIP[i])
	}
	log.Println("Sink IP" + SinkIp)
}

func deleteMessage(id string) {
	ID, err := strconv.Atoi(id)
	checkErr(err)
	delete(message, ID)
}

// func (r *Crane) MasterStart(args *shared.App, reply *shared.EmptyReq) error {
//
// }
