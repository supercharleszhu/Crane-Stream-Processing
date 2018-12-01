package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"../shared"
)

const IPFILE = "iptable.config"
const PORT = "6000"
const LOGFILE = "output.log"
const NUMOFVM = 10

var SELFIP string
var ID int // the index of VM
var memberList [NUMOFVM]shared.Member

const Period = 10000 * time.Millisecond // millisecond
const SendPeriod = 200 * time.Millisecond

// get the pattern in the distributed grep command
func getPattern() string {
	var pattern string
	fmt.Print("Enter Regular Expression: ")
	fmt.Scanln(&pattern)
	return pattern
}

// grep the log file from all the virtual machines
func grep(pattern string) [NUMOFVM]shared.LogReply {

	file, err := os.Open(IPFILE)
	checkErr(err)
	defer file.Close()
	var hostConfig string

	scanner := bufio.NewScanner(file)
	channel := make(chan *rpc.Call, NUMOFVM)
	reply := [NUMOFVM]shared.LogReply{}
	counter := 0
	// grep from every VM one by one
	for scanner.Scan() && counter < NUMOFVM {
		hostConfig = scanner.Text()
		// create the LogRequest from client
		req := &shared.LogRequest{RegEx: pattern, Filename: LOGFILE}

		// get the ip address of a VM
		ip := strings.Split(hostConfig, " ")[1]

		// try to connect with specified VM
		client, err := rpc.Dial("tcp", ip+":"+PORT)
		if err != nil {
			// log.Println("Connection:", err)
			reply[counter] = shared.LogReply{Result: "", Count: -1}
			channel <- &rpc.Call{}
			counter++
			continue
		}
		// fmt.Println(ip + " : connetction sucess...")
		// call the grep function on server
		reply[counter] = shared.LogReply{Result: "", Count: 0}
		grepCall := client.Go("GrepLog.FindLog", req, &reply[counter], channel)
		checkErr(grepCall.Error)
		counter++
	}

	//waiting for other threads
	for i := 0; i < NUMOFVM; i++ {
		grepCall := <-channel
		// fmt.Println(i, "th"+" pointer poped")
		checkErr(grepCall.Error)
	}

	// print out answers
	for i := 0; i < NUMOFVM; i++ {
		fmt.Print(reply[i].Result)
	}

	for i := 0; i < NUMOFVM; i++ {
		fmt.Println("total line from VM" + strconv.Itoa(i+1) + ": " + strconv.Itoa(reply[i].Count))
	}
	file.Seek(0, 0)
	return reply
}

// send the join or leave message to the local server
func joinOrLeave(command string) {
	req := &shared.GossipMsg{
		Ip:        SELFIP,
		Id:        ID,
		TimeStamp: time.Now(),
		Msg:       command,
	}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.GossipRpl{}
	fmt.Println(SELFIP + " : connection success...")
	err = client.Call("Gossip.RecGossip", req, reply)
	checkErr(err)
}

// request  the membership list from the local server
func showMembershipList() {
	req := &shared.EmptyReq{}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.MembershipRpl{}
	err = client.Call("Memlst.RplMemlst", req, reply)
	fmt.Printf("%s\n", string(reply.MemberList))
}

// call put function in local server
func put(localFileName string, sdfsFileName string) {
	t1 := time.Now()
	_, err := os.Stat("./duplication/" + localFileName)
	if err != nil && !os.IsExist(err) {
		fmt.Println(err.Error())
		return
	}
	req := &shared.SDFSMsg{Type: "put", LocalFileName: localFileName, SDFSFileName: sdfsFileName, TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("SDFS.PutReq", req, reply)
	fmt.Printf("Successful write: %t, time: %f s\n", reply.Finish, time.Since(t1).Seconds())
}

// call get function in local server
func get(sdfsFileName string, localFileName string) {
	t1 := time.Now()
	req := &shared.SDFSMsg{Type: "get", LocalFileName: localFileName, SDFSFileName: sdfsFileName, TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("SDFS.GetReq", req, reply)
	if !reply.Finish {
		fmt.Printf("The file is not available.\n")
	} else {
		fmt.Printf("Successful get: %t, time: %f s\n", reply.Finish, time.Since(t1).Seconds())
	}
}

// call get function in local server
func getVersions(sdfsFileName string, numVersions string, localFileName string) {
	t1 := time.Now()
	req := &shared.SDFSMsg{
		Type:          "get",
		LocalFileName: localFileName,
		SDFSFileName:  sdfsFileName,
		TimeStamp:     time.Now(),
		NumVersions:   numVersions,
	}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("SDFS.GetReqVersion", req, reply)
	fmt.Printf("Successful get-versions: %t, time: %f s\n", reply.Finish, time.Since(t1).Seconds())
}

//sdfsDelete
func sdfsDelete(sdfsFileName string) {
	req := &shared.SDFSMsg{Type: "get", LocalFileName: "", SDFSFileName: sdfsFileName, TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("SDFS.DelReq", req, reply)
	fmt.Printf("Successful sdfsDelete: %t\n", reply.Finish)
}

func listDupNode(sdfsFileName string) {
	req := &shared.SDFSMsg{Type: "ls", LocalFileName: "", SDFSFileName: sdfsFileName, TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.DupList{}
	err = client.Call("SDFS.DupNodes", req, reply)

	fmt.Println("\"" + sdfsFileName + "\"" + " should be stored in:")
	for _, node := range reply.Nodes {
		fmt.Printf(" %d", node)
	}
	fmt.Printf("\n")
}

func listLocalFile() {
	req := &shared.SDFSMsg{Type: "store", LocalFileName: "", SDFSFileName: "", TimeStamp: time.Now()}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.SFileList{}
	err = client.Call("SDFS.FileList", req, reply)

	fmt.Println("All files currently stored: ")
	for filename, timestampList := range reply.SFile {
		for _, timestamp := range timestampList {
			fmt.Println(filename + "." + timestamp.Format(shared.TIMEFMT))
		}
	}
}

// request the peerlist from the local server
func showPrList() {
	req := &shared.EmptyReq{}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.MembershipRpl{}
	err = client.Call("Memlst.RplPrList", req, reply)
	fmt.Printf("%s\n", string(reply.MemberList))
}

// send start request to local server
func start(appName string) {
	req := &shared.App{
		AppName:    appName,
		Period:     Period,
		SendPeriod: SendPeriod,
	}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("Crane.StartApp", req, reply)
	fmt.Printf("Start %s: %t ", appName, reply.Finish)
}

func startWithConfig(appName string, period string, sendPeriod string) {
	sendP, err := strconv.Atoi(sendPeriod)
	p, err := strconv.Atoi(period)
	sendPDuration := time.Duration(sendP) * time.Millisecond
	pDuration := time.Duration(p) * time.Millisecond
	checkErr(err)
	req := &shared.App{
		AppName:    appName,
		Period:     pDuration,
		SendPeriod: sendPDuration,
	}
	client, err := rpc.Dial("tcp", SELFIP+":"+PORT)
	checkErr(err)
	reply := &shared.WriteAck{}
	err = client.Call("Crane.StartApp", req, reply)
	fmt.Printf("Start %s: %t ", appName, reply.Finish)
}

func init() {
	// initializing the membership list...
	SELFIP = getInternalIP()
	log.Println("Ip address: " + SELFIP)
	file, err := os.Open(IPFILE)
	checkErr(err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 0
	fmt.Println("initializing memberlist...")
	for scanner.Scan() && i < NUMOFVM {
		ip := strings.Split(scanner.Text(), " ")[1]
		if ip == SELFIP {
			ID = i
			fmt.Println("ID of the VM: ", i)
		}
		memberList[i] = shared.Member{}
		memberList[i].Ip = ip
		memberList[i].Id = i
		memberList[i].TimeStamp = time.Now()
		memberList[i].Status = 0
		i += 1
	}
}

func main() {

	showCommand()

	for {
		fmt.Print(">")
		inputReader := bufio.NewReader(os.Stdin)
		command, err := inputReader.ReadString('\n')
		checkErr(err)
		cmdArr := strings.Fields(command)
		if len(cmdArr) == 0 {
			continue
		} else if cmdArr[0] == "start" && len(cmdArr) == 3 {
			put(cmdArr[2], "demo-data")
			start(cmdArr[1])
		} else if cmdArr[0] == "start" && len(cmdArr) == 5 {
			put(cmdArr[2], "demo-data")
			startWithConfig(cmdArr[1], cmdArr[3], cmdArr[4])
		} else if cmdArr[0] == "put" && len(cmdArr) == 3 {
			put(cmdArr[1], cmdArr[2])
		} else if cmdArr[0] == "get" && len(cmdArr) == 3 {
			get(cmdArr[1], cmdArr[2])
		} else if cmdArr[0] == "get-versions" && len(cmdArr) == 4 {
			getVersions(cmdArr[1], cmdArr[2], cmdArr[3])
		} else if cmdArr[0] == "delete" && len(cmdArr) == 2 {
			sdfsDelete(cmdArr[1])
		} else if cmdArr[0] == "join" || cmdArr[0] == "leave" {
			joinOrLeave(cmdArr[0])
		} else if cmdArr[0] == "ls" && len(cmdArr) == 2 {
			listDupNode(cmdArr[1])
		} else if cmdArr[0] == "store" {
			listLocalFile()
		} else if cmdArr[0] == "join" {
			joinOrLeave(cmdArr[0])
		} else if cmdArr[0] == "memberlist" {
			showMembershipList()
		} else if cmdArr[0] == "peerlist" {
			showPrList()
		} else if cmdArr[0] == "q" {
			break
		} else if cmdArr[0] == "grep" {
			pattern := getPattern()
			grep(pattern)
		} else if cmdArr[0] == "showID" {
			showID()
		}
	}
}

func checkErr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func getInternalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func showCommand() {

	fmt.Println("SDFS command line interface\n" +
		"Please enter the following command:\n" +
		"======================================================\n" +
		"start appName localfilename period sendPeriod" +
		"put localfilename sdfsfilename\n" +
		"get sdfsfilename localfilename\n" +
		"delete sdfsfilename\n" +
		"ls sdfsfilename\n" +
		"store\n" +
		"join\n" +
		"get-versions sdfsFileName numVersion LocalFileName\n" +
		"memberlist: show membership list\n" +
		"peerlist: show peer list\n" +
		"showID: show ID\n" +
		"q: quit\n" +
		"======================================================")
}

//request the ID of itself from server
func showID() {
	fmt.Printf("Host %d, IP <%s>\n", ID, SELFIP)
}
