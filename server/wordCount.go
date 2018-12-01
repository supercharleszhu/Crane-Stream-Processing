package main

import (
	"log"
	"strconv"
	"strings"
)

type wordCount struct {
	result    map[string]int
	messageId int
	ackVal    int
}

func (w wordCount) mergeCache(messageId int) {
	for word, count := range Cache[messageId].(map[string]int) {
		if _, ok := w.result[word]; !ok {
			w.result[word] = count
		} else {
			w.result[word] += count
		}
	}
}

func (w *wordCount) join(data string) {
	tuple := strings.Fields(data)
	if len(tuple) != 2 {
		log.Printf("data format error! messageID: %d\n", w.messageId)
		return
	}
	word := tuple[0]
	count, err := strconv.Atoi(tuple[1])
	if err != nil {
		log.Printf("data format error! messageID: %d\n", w.messageId)
		return
	}

	//write data into Cache
	if temp, ok := Cache[w.messageId]; !ok {
		Cache[w.messageId] = &map[string]int{
			word: count,
		}
	} else {
		tempMap := temp.(map[string]int)
		if _, ok := tempMap[word]; !ok {
			tempMap[word] = count
		} else {
			tempMap[word] += count
		}
	}
	sendAck(w.messageId)

}
func (w *wordCount) transform(data string) {
	//TODO: implement it!
	sendAck(w.messageId)
}

func (w *wordCount) getMessageId() int {
	return w.messageId
}
func (w *wordCount) setMessageId(id int) {
	w.messageId = id
}
func (w *wordCount) getAckVal() int {
	return w.ackVal
}
func (w *wordCount) setAckVal(ackVal int) {
	w.ackVal = ackVal
}
