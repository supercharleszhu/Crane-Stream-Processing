package main

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

const UDPPORT = 8888
const TCPPORT = "9000"
const IP = "172.22.154.172"

var writeBuf = make([]byte, 2000000)
var data bytes.Buffer // buffer which stores tcp socket message
var readBuf = make([]byte, 2000000)

func tcpListenClient(done chan bool) {
	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		fmt.Println(err)
		return
	}

	for {

		conn, err := listener.Accept()
		defer conn.Close()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleClient(conn)
	}

	done <- true
}

func handleClient(conn net.Conn) {

	for {
		n, err := conn.Read(writeBuf)
		if err != nil {
			fmt.Println(err)
			return
		}
		// data = string(b[:n])
		data.Write(writeBuf[:n]) // write
		fmt.Println("Remain in Buffer:")
		fmt.Println(data.String())
	}
}

func udpListenClient(done chan bool) {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(IP), Port: UDPPORT})
	checkErr(err)

	for {
		n, _, err := listener.ReadFromUDP(writeBuf)
		checkErr(err)
		data.Write(writeBuf[:n])
		fmt.Println("Remain in Buffer:")
		fmt.Println(data.String())
	}
	fmt.Println("quit udp server loop")
	done <- true
}

func consumeBuf(done chan bool) {

	ticker := time.NewTicker(100 * time.Millisecond)
	for _ = range ticker.C {
		n, _ := data.Read(readBuf)
		// fmt.Println("read bytes:")
		if n != 0 {
			fmt.Println(n)
			fmt.Println("Remain in Buffer:")
			fmt.Println(data.String())
			fmt.Println("Read from Buffer:")
			fmt.Println(string(readBuf[:n]))
		}
	}

	done <- true
}

func listenSpark(done chan bool) {
	for {
		server, err := net.Listen("tcp", ":9000")
		if err != nil {
			fmt.Println(err)
			return
		}

		conn, err := server.Accept()
		checkErr(err)
		handleSpark(conn)
		conn.Close()
		server.Close()
	}
	fmt.Println("break from listenSpark")
	done <- true
}

func handleSpark(c net.Conn) {
	for {
		// c.Write([]byte(data))

		n, _ := data.Read(readBuf)
		if n == 0 {
			break
		}

		c.Write(readBuf[:n])
		fmt.Println(string(readBuf[:n]))

		// fmt.Println(data.String())
		// fmt.Println(string(readBuf[:n]))
	}
	fmt.Println("break from handleSpark")
}

func main() {

	done := make(chan bool, 2)

	// go tcpListenClient(done)
	go listenSpark(done)
	go udpListenClient(done)
	// go consumeBuf(done)

	<-done
	<-done
}

func checkErr(e error) {
	if e != nil {
		fmt.Println(e)
	}
}
