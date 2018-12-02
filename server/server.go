package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
	"time"

	"../shared"
)

var appName int

const NUMOFPEER = 3
const NUMOFVM = 10
const IPFILE = "iptable.config"
const RPCPORT = "6000"
const HTTPPORT = "8000"
const W = 4
const TIMEFMT = "2006-01-02 15:04:05"

var SELFIP string
var ID int // the index of VM
var peerList [NUMOFPEER]shared.Member
var memberList [NUMOFVM]shared.Member

// Read IP file and initialize the memberList
func init() {
	rand.Seed(time.Now().UTC().UnixNano())
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
		memberList[i].UnresponseCount = 0
		i += 1
	}
	sfile = make(map[string][]time.Time)
	initializePeerList()
	printMemberList()

	// TODO delete all sdfsfile
	deleteAllSfile()
}

func main() {

	// SWIM failure detection with UDP
	done := make(chan bool, 2)
	go CraneTimer()
	go launchFailureDetection(done)
	go UDPReceiver(done)
	go SwimReceiver()
	go handleHTTP()
	go launchAcker()
	log.Printf("Start UDP")

	dst, err := os.OpenFile("test.txt", os.O_APPEND|os.O_WRONLY, 0644)
	file, err := os.Open("iptable.config")
	io.Copy(dst, file)
	io.Copy(dst, file)
	file.Close()
	dst.Close()

	// Listen to RPC call
	log.Print("Start Listening to port " + RPCPORT + "...")
	listener, err := net.Listen("tcp", SELFIP+":"+RPCPORT)
	checkErr(err)
	grepLog := new(GrepLog)
	gossip := new(Gossip)
	memlst := new(Memlst)
	sdfs := new(SDFS)
	crane := new(Crane)
	//registering new servers...
	server := rpc.NewServer()
	server.RegisterName("GrepLog", grepLog)
	server.RegisterName("Gossip", gossip)
	server.RegisterName("Memlst", memlst)
	server.RegisterName("SDFS", sdfs)
	server.RegisterName("Crane", crane)
	server.Accept(listener)

	<-done
	<-done
}
