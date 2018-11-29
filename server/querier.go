package main

import (
	"bufio"
	"bytes"
	"os"
	"regexp"
	"strconv"

	"../shared"
)

type GrepLog int // Grep text from log

func (r *GrepLog) FindLog(args *shared.LogRequest, reply *shared.LogReply) error {
	// t1 := time.Now()
	totalCount := 0
	file, err := os.Open("output.log")
	checkErr(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	line := 1
	count := 0
	var buffer bytes.Buffer
	for scanner.Scan() {
		// Regex matching
		contained, err := regexp.MatchString(args.RegEx, scanner.Text())
		checkErr(err)

		// If match, write this line into the buffer
		if contained {
			buffer.WriteString("From VM: ")
			buffer.WriteString(strconv.Itoa(ID))
			buffer.WriteString(": ")
			buffer.WriteString(scanner.Text())
			buffer.WriteString(". At line: ")
			buffer.WriteString(strconv.Itoa(line))
			buffer.WriteString("\n")
			count++
		}
		line++
	}

	//Write result into the response struct
	reply.Result += buffer.String()
	totalCount += count
	// fmt.Println("totalCount: ", totalCount)
	reply.Count = totalCount

	//output and reset the file pointer
	file.Seek(0, 0)
	// t2 := time.Since(t1)
	// fmt.Println("time: ", t2)
	return nil
}
