package main

import (
	"log"
	"strconv"
	"strings"
)

var Cache map[int]interface{}
var MasterIp string

type App interface {
	join(message string)
	transform(message string)
	getAckVal() int
	setAckVal(ackVal int)
	getMessageId() int
	setMessageId(id int)
}

var currApp App
var currAppName string

func sendAck(messageId int) {
	//TODO send ack to acker

}

// App Configuration. Modify this if new app is added
func startApp(appName string) {
	currAppName = appName
	if appName == "wordCount" {
		currApp = &wordCount{
			result:    map[string]int{},
			messageId: 0,
			ackVal:    0,
		}
	}
}

func sendMessageSink(message string) {
	//TODO: send message to sink

}

func sendMessageWorker(message string) {
	//TODO: send message to sink
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
