package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"../shared"
)

type twitter struct {
	result    map[string]int
	messageId int
	ackVal    int
}

func (w *twitter) mergeCache(messageId int) {
	log.Printf("merging Cache.....Cache messageId: %d length %d", messageId, len(Cache[messageId].(map[string]int)))
	for word, count := range Cache[messageId].(map[string]int) {
		if _, ok := w.result[word]; !ok {
			w.result[word] = count
		} else {
			w.result[word] += count
		}
	}
	Cache[messageId] = map[string]int{}
}

func (w *twitter) join(data string) {
	tuple := strings.Fields(data)
	if len(tuple) != 1 {
		log.Printf("data format error! messageID: %d\n", w.messageId)
		return
	}
	word := tuple[0]

	//write data into Cache
	if temp, ok := Cache[w.messageId]; !ok {
		Cache[w.messageId] = map[string]int{
			word: 1,
		}
	} else {
		tempMap := temp.(map[string]int)
		if _, ok := tempMap[word]; !ok {
			tempMap[word] = 1
		} else {
			tempMap[word] += 1
		}
	}
	log.Printf("Join: writing to Cache[%d], length %d\n", w.messageId, len(Cache[w.messageId].(map[string]int)))

	//sendAck
	sendAck(w.messageId, w.ackVal)
}
func (w *twitter) transform(data string) {
	words := strings.Fields(data)
	if len(words) == 2 {
		ackVal := int(rand.Int31())
		w.ackVal ^= ackVal
		sendMessageSink(ackVal, w.messageId, words[1])
	}
	sendAck(w.messageId, w.ackVal)
}

func (w *twitter) getMessageId() int {
	return w.messageId
}
func (w *twitter) setMessageId(id int) {
	w.messageId = id
}
func (w *twitter) getAckVal() int {
	return w.ackVal
}
func (w *twitter) setAckVal(ackVal int) {
	w.ackVal = ackVal
}

func (w *twitter) writeToSDFS() {
	log.Println("Writing to SDFS")

	//1. sorting
	pl := make(PairList, len(w.result))
	log.Printf("Length of current result: %d\n", len(w.result))
	i := 0

	for k, v := range w.result {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))

	//2. create temp file
	destFile, err := os.Create("./duplication/tempfile")
	if err != nil {
		log.Println("os.Create: ", err)
	}
	fmt.Fprintf(destFile, "Result of wordcount: \n")
	if i >= 5 {
		for i := 0; i < 5; i++ {
			fmt.Fprintf(destFile, "%s: %d\n", pl[i].Key, pl[i].Value)
		}
	}
	destFile.Close()

	//3. write into SDFS!
	args := &shared.SDFSMsg{
		Type:          "put",
		LocalFileName: "tempfile",
		SDFSFileName:  "wordcount_result",
		TimeStamp:     time.Now(),
	}
	res := &shared.WriteAck{}
	sdfs := new(SDFS)
	sdfs.PutReq(args, res)

}
