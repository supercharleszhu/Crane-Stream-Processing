package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"strconv"
)

func main() {
	// tcpSend()
	udpSend()
}

func udpSend() {

	// Open log file
	absPath, _ := filepath.Abs("../demo-data")
	file, err := os.Open(absPath)
	checkErr(err)
	defer file.Close()

	br := bufio.NewReader(file)

	// Connect to master
	conn, err := net.Dial("udp", "172.22.154.172:8888")
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()

	// Set timer
	ticker := time.NewTicker(20 * time.Millisecond)
	// counter := 0

	// Start sending stream data
	for _ = range ticker.C {
		n, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		conn.Write([]byte(string(n) + "\n"))
		fmt.Print(string(n) + "\n")
		// conn.Write([]byte("hello world " + strconv.Itoa(counter) + "\n"))
		// fmt.Println("hello world" + strconv.Itoa(counter))
		// counter += 1
	}
}

func tcpSend() {
	conn, err := net.Dial("tcp", "172.22.154.172:8888")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(1000 * time.Millisecond)
	counter := 0
	for _ = range ticker.C {
		conn.Write([]byte("hello world " + strconv.Itoa(counter) + "\n"))
		fmt.Println("hello world" + strconv.Itoa(counter))
		counter += 1
	}
}

func checkErr(e error) {
	if e != nil {
		fmt.Println(e)
	}
}
