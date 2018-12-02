package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"../shared"
)

var sfile map[string][]time.Time
var masterFile = make(map[string]bool)

type Memlst int
type SDFS int // HTTP file transfer

// put file to corresponding VMs
// Should duplication to four VMs
func (r *SDFS) PutReq(args *shared.SDFSMsg, reply *shared.WriteAck) error {

	done := make(chan bool, W)

	// Find live nodes and send file to them
	nodes := findDupNodes(args.SDFSFileName)

	for _, i := range nodes {
		go sendFile(memberList[i].Ip, args, done)
	}
	// Wait until all four replication write finish
	for j := 0; j < W; j++ {
		res := <-done
		if res == false {
			reply.Finish = false
			return nil
		}
	}
	reply.Finish = true
	return nil
}

func sendFile(IP string, args *shared.SDFSMsg, done chan bool) {
	// Create form file, the first arg is attribute name
	// the second arg is filename

	// Read from file and get info
	srcFile, err := os.Open("./duplication/" + args.LocalFileName)
	fi, _ := srcFile.Stat()
	if err != nil {
		done <- false
		log.Println("os.Open: ", err)
	}
	defer srcFile.Close()

	// buffer for storing multipart data and create writer
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	// create file
	hFileName := strconv.Itoa(int(hash(args.SDFSFileName)))
	log.Printf("sendFile: %s, hFileName: %s\n", args.SDFSFileName+"."+args.TimeStamp.Format(TIMEFMT), hFileName)
	_, err = writer.CreateFormFile("duplication", args.SDFSFileName+"."+args.TimeStamp.Format(TIMEFMT))
	if err != nil {
		log.Println("createFormFile: ", err)
	}
	contentType := writer.FormDataContentType()

	// Read file
	nmulti := buf.Len()
	multi := make([]byte, nmulti)
	_, _ = buf.Read(multi)

	// io.Copy
	// _, err = io.Copy(formFile, srcFile)
	// if err != nil {
	// 	log.Println("io.Copy: ", err)
	// }

	// Read last boundary
	writer.Close()
	nboundary := buf.Len()
	lastBoundary := make([]byte, nboundary)
	_, _ = buf.Read(lastBoundary)

	// calculate content length
	totalSize := int64(nmulti) + fi.Size() + int64(nboundary)

	// use pipe to pass request
	rd, wr := io.Pipe()
	defer rd.Close()

	go func() {
		defer wr.Close()

		//write multipart
		_, _ = wr.Write(multi)

		//write file
		buf := make([]byte, 10000)
		for {
			n, err := srcFile.Read(buf)
			if err != nil {
				break
			}
			_, _ = wr.Write(buf[:n])
		}
		//write boundary
		_, _ = wr.Write(lastBoundary)
	}()

	request, err := http.NewRequest("POST", "http://"+IP+":"+HTTPPORT+"/duplication", rd)
	request.Header.Add("Content-Type", contentType)
	request.ContentLength = totalSize
	checkErr(err)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		log.Printf("Post File Fail IP: %s\n", IP)
	} else {
		defer response.Body.Close()

		log.Println("Send file to " + IP + ": " + response.Status)
	}
	done <- true
}

// get file from sdfs
func (r *SDFS) GetReq(args *shared.SDFSMsg, reply *shared.WriteAck) error {
	nodes := findDupNodes(args.SDFSFileName)
	var resp *http.Response
	var err error
	for _, destID := range nodes {
		//log.Printf("The hashVal of sdfsFile: %s, %d;  Destination ID: %d\n", args.SDFSFileName, hashVal, destID)
		resp, err = http.Get("http://" + memberList[destID].Ip + ":" + HTTPPORT + "/duplication/" + args.SDFSFileName)
		if err != nil || resp.StatusCode >= 400 {
			log.Println("get file from " + memberList[destID].Ip + ": " + resp.Status)
			continue
		} else {
			log.Println("get file from " + memberList[destID].Ip + ": " + resp.Status)
			break
		}
	}

	// TODO: cannot find sdfsfilename
	if resp.StatusCode == 404 {
		reply.Finish = false
		return nil
	}

	destFile, err := os.Create("./duplication/" + args.LocalFileName)
	defer destFile.Close()
	if err != nil || resp == nil {
		log.Println("os.Create: ", err)
		return nil
	}
	_, err = io.Copy(destFile, resp.Body)
	if err != nil {
		log.Fatal("io.Copy: ", err)
	}
	reply.Finish = true
	return nil
}

// get all the latest num-versions versions of the file
func (r *SDFS) GetReqVersion(args *shared.SDFSMsg, reply *shared.WriteAck) error {
	nodes := findDupNodes(args.SDFSFileName)
	var resp *http.Response
	var err error
	for _, destID := range nodes {
		client := &http.Client{}
		url := "http://" + memberList[destID].Ip + ":" + HTTPPORT + "/duplication/" + args.SDFSFileName
		req, err := http.NewRequest("GET", url, nil)
		req.Header.Add("versions", args.NumVersions)
		resp, err = client.Do(req)
		if err != nil || resp.StatusCode >= 400 {
			continue
		} else {
			break
		}
	}
	destFile, err := os.Create("./duplication/" + args.LocalFileName)
	defer destFile.Close()
	defer resp.Body.Close()
	if err != nil || resp == nil {
		log.Println("os.Create: ", err)
		return nil
	}
	_, err = io.Copy(destFile, resp.Body)
	if err != nil {
		log.Fatal("io.Copy: ", err)
	}
	reply.Finish = true
	return nil
}

// delete sdfsfile
func (r *SDFS) DelReq(args *shared.SDFSMsg, reply *shared.WriteAck) error {
	done := make(chan bool, W)

	nodes := findDupNodes(args.SDFSFileName)

	for _, i := range nodes {
		go deleteFile(memberList[i].Ip, args, done)
	}
	// Wait until all four replication delete finish
	for j := 0; j < W; j++ {
		res := <-done
		if res == false {
			reply.Finish = false
			return nil
		}
	}
	reply.Finish = true
	return nil
}
func deleteFile(IP string, args *shared.SDFSMsg, done chan bool) {
	//hFileName := strconv.Itoa(int(hash(args.SDFSFileName)))
	log.Printf("deleteFile: hFileName: %s, SDFSFileName: %s\n", args.SDFSFileName+"."+args.TimeStamp.Format(TIMEFMT), args.SDFSFileName)
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", "http://"+IP+":"+HTTPPORT+"/duplication/"+args.SDFSFileName, nil)
	checkErr(err)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		done <- false
	}
	done <- true
}

// Listen to 80 port and receive http tranferred file
func recFile(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)

	// get form file
	formFile, header, err := r.FormFile("duplication")
	if err != nil {
		log.Println("r.FormFile: ", err)
	}
	defer formFile.Close()

	filemeta := strings.Split(header.Filename, ".")
	sFileName := filemeta[0]
	hFileName := strconv.Itoa(int(hash(sFileName)))
	if ID == findMasterNode(hFileName) {
		masterFile[sFileName] = true
	}

	// save to local file
	log.Println("r.URL.Path", r.URL.Path)
	log.Println("header.Filename (sFileName.timestamp)", header.Filename)

	destFile, err := os.Create("." + r.URL.Path + "/" + hFileName + "." + filemeta[1])
	if err != nil {
		log.Println("os.Create: ", err)
	}
	defer destFile.Close()

	// Read form file and write to local file
	_, err = io.Copy(destFile, formFile)
	if err != nil {
		log.Println("io.Copy: ", err)
	}
	// Update sfile list
	timestamp, _ := time.Parse(TIMEFMT, filemeta[1])
	if _, ok := sfile[filemeta[0]]; ok {
		if !find(sfile[filemeta[0]], timestamp) {
			sfile[filemeta[0]] = append(sfile[filemeta[0]], timestamp)
		}
	} else {
		sfile[filemeta[0]] = []time.Time{timestamp}
	}
	w.WriteHeader(200)
}
func find(times []time.Time, timestamp time.Time) bool {
	for _, t := range times {
		if t.Equal(timestamp) {
			return true
		}
	}
	return false
}

// return or delete required file
func httpResponse(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)

	// get version number
	if numVersions := r.Header.Get("versions"); numVersions != "" {
		log.Println("getNumVersions: r.URL.Path", r.URL.Path[1:])
		sFileName := r.URL.Path[13:]
		urlPart := strings.Split(r.URL.Path[1:], "/")
		if _, ok := sfile[sFileName]; !ok {
			http.NotFound(w, r)
			log.Printf("File %s not found!\n", urlPart[1])
			return
		}
		tStamps := sfile[sFileName]
		sort.Slice(tStamps, func(i, j int) bool {
			return tStamps[i].After(tStamps[j])
		})
		nums, err := strconv.Atoi(numVersions)

		//if invalid number of versions, return all the versions!
		if nums <= 0 || nums > len(tStamps) {
			nums = len(tStamps)
		}
		checkErr(err)
		hFileName := strconv.Itoa(int(hash(sFileName)))
		log.Printf("GetVersions: finding files %s, hash value %s \n", r.URL.Path[13:], hFileName)
		for i := 0; i < nums; i++ {
			io.WriteString(w, tStamps[i].Format(TIMEFMT)+"\n")
			file, err := os.Open("./duplication/" + hFileName + "." + tStamps[i].Format(TIMEFMT))
			checkErr(err)
			io.Copy(w, file)
			io.WriteString(w, "\n")
			file.Close()
		}
	} else if r.Method == "GET" {
		sFileName := r.URL.Path[13:]
		hFileName := strconv.Itoa(int(hash(sFileName)))
		log.Println("r.URL.Path", r.URL.Path[1:])
		if _, ok := sfile[sFileName]; !ok {
			http.NotFound(w, r)
			log.Printf("File %s not found!\n", sFileName)
			return
		}
		tStamps := sfile[r.URL.Path[13:]] // /duplication/
		sort.Slice(tStamps, func(i, j int) bool {
			return tStamps[i].After(tStamps[j])
		})
		log.Printf("HTTP: Serving file %s\n", hFileName+"."+tStamps[0].Format(TIMEFMT))

		// serve local file
		http.ServeFile(w, r, "./duplication/"+hFileName+"."+tStamps[0].Format(TIMEFMT))
	} else if r.Method == "DELETE" {
		log.Println("r.URL.Path", r.URL.Path[1:])
		sFileName := r.URL.Path[13:]
		hFileName := strconv.Itoa(int(hash(sFileName)))
		log.Println("Deleting sFileName  ", sFileName)
		urlPart := strings.Split(r.URL.Path[1:], "/")
		if _, ok := sfile[sFileName]; !ok {
			http.NotFound(w, r)
			log.Printf("File %s not found!\n", urlPart[1])
			return
		}
		tStamps := sfile[sFileName]
		sort.Slice(tStamps, func(i, j int) bool {
			return tStamps[i].After(tStamps[j])
		})
		for _, t := range tStamps {
			log.Printf("HTTP: Deleting file %s\n", hFileName+"."+t.Format(TIMEFMT))
			err := os.Remove("./duplication/" + hFileName + "." + t.Format(TIMEFMT))
			checkErr(err)
		}
		// remove from map
		delete(sfile, sFileName)
		if _, ok := masterFile[sFileName]; ok {
			delete(masterFile, sFileName)
		}
	}

}

// find all duplication nodes for specified sdfsFileName
// return duplication nodes to client
func (r *SDFS) DupNodes(args *shared.SDFSMsg, reply *shared.DupList) error {
	nodes := findDupNodes(args.SDFSFileName)
	reply.Nodes = nodes
	return nil
}
func findDupNodes(sdfsFilename string) []int {
	// Calculate the hash value of sdfsFileName
	node := FNV32a(sdfsFilename)
	var nodeArr []int
	counter := 0
	for i := 0; i < NUMOFVM; i++ {
		if counter == W {
			break
		}
		if memberList[(node+i)%NUMOFVM].Status == 1 {
			nodeArr = append(nodeArr, (node+i)%NUMOFVM)
			counter += 1
		}
	}
	return nodeArr
}

// find the Master node ID of given hashed file name
func findMasterNode(hFilename string) int {
	val, err := strconv.Atoi(hFilename)
	checkErr(err)
	node := val % 10
	for i := 0; i < NUMOFVM; i++ {
		if memberList[(node+i)%NUMOFVM].Status == 1 {
			log.Printf("master node of file %s: %d\n", hFilename, (node+i)%NUMOFVM)
			return (node + i) % NUMOFVM
		}
	}
	return -1
}

// return SFileList to client
func (r *SDFS) FileList(args *shared.SDFSMsg, reply *shared.SFileList) error {
	reply.SFile = sfile
	log.Println("list sdfs file.")
	return nil
}

//Update live peers in peerList
func updatePeerList() {
	t1 := time.Now()
	i := 1
	counter := 0
	var oldPeerList [NUMOFPEER]int
	for k := 0; k < NUMOFPEER; k++ {
		oldPeerList[k] = peerList[k].Id
	}
	log.Println("oldPeerList: ", oldPeerList)
	log.Printf("Updating peerlist...\n")
	for counter < NUMOFPEER && i != 0 {
		if memberList[(ID+i)%NUMOFVM].Status == 1 {
			peerList[counter] = memberList[(ID+i)%NUMOFVM]
			jsString, err := json.Marshal(peerList[counter])
			checkErr(err)
			fmt.Println("Peer " + strconv.Itoa(counter) + ":" + string(jsString))
			counter++
		}
		i = (i + 1) % NUMOFVM
	}
	// Unset peer will be set as offline peer
	counter++
	for ; counter < NUMOFPEER; counter++ {
		peerList[counter] = shared.Member{Status: 0}
	}

	// TODO: Minor Optimization
	// Delete all replica on outdated peer
	for i := 0; i < NUMOFPEER; i++ {
		found := false
		for j := 0; j < NUMOFPEER; j++ {
			if peerList[j].Id == oldPeerList[i] {
				found = true
				break
			}
		}
		if !found {
			deleteAllReplica(memberList[oldPeerList[i]].Ip)
		}
	}

	// Send files to new peer
	for i := 0; i < NUMOFPEER; i++ {
		found := false
		for j := 0; j < NUMOFPEER; j++ {
			if oldPeerList[j] == peerList[i].Id {
				found = true
				break
			}
		}
		if !found {
			sendAllReplica(peerList[i].Ip)
		}
	}

	// update masterFiles:
	// 1. delete all the old master files
	for sFileName, _ := range masterFile {
		hFileName := strconv.Itoa(int(hash(sFileName)))
		if ID != findMasterNode(hFileName) {
			delete(masterFile, sFileName)
			// TODO: when master rejoin
			for _, t := range sfile[sFileName] {
				sendReplica(memberList[findMasterNode(hFileName)].Ip, sFileName, t)
			}

			// file in last peer is not deleted
			client := &http.Client{}
			log.Printf("deleteReplica: %s\n", sFileName)
			req, err := http.NewRequest("DELETE", "http://"+peerList[2].Ip+":"+HTTPPORT+"/duplication/"+sFileName, nil)
			checkErr(err)
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode >= 400 {
				log.Printf("deleteReplica: fail!\n")
			}
		}
	}

	// 2. send all new masterfiles to peers
	for sFileName, tStamps := range sfile {
		hFileName := strconv.Itoa(int(hash(sFileName)))
		if ID == findMasterNode(hFileName) {
			if _, ok := masterFile[sFileName]; !ok {
				log.Printf("new master file: %s\n", sFileName)
				masterFile[sFileName] = true
				for i := 0; i < NUMOFPEER; i++ {
					for _, t := range tStamps {
						sendReplica(peerList[i].Ip, sFileName, t)
					}
				}
			}
		}
	}
	log.Printf("Data Replication: %f s", time.Since(t1).Seconds())
}

//delete replica when updating peerList
func deleteAllReplica(destIP string) {
	//1. find all the replicas that it is in charge of
	for sFileName, _ := range sfile {
		hFileName := strconv.Itoa(int(hash(sFileName)))
		if ID == findMasterNode(hFileName) {
			client := &http.Client{}
			log.Printf("deleteReplica: %s\n", sFileName)
			req, err := http.NewRequest("DELETE", "http://"+destIP+":"+HTTPPORT+"/duplication/"+sFileName, nil)
			checkErr(err)
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode >= 400 {
				log.Printf("deleteReplica: fail!\n")
			}
		}
	}
}

//send replica when updating peerList
func sendAllReplica(destIP string) {
	for sFileName, tStamps := range sfile {
		hFileName := strconv.Itoa(int(hash(sFileName)))
		if ID == findMasterNode(hFileName) {
			log.Printf("sendReplica: %s\n", sFileName)
			for _, t := range tStamps {
				sendReplica(destIP, sFileName, t)
			}
		}
	}
}
func sendReplica(IP string, sFileName string, timeStamp time.Time) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	formFile, err := writer.CreateFormFile("duplication", sFileName+"."+timeStamp.Format(TIMEFMT))
	if err != nil {
		log.Println("createFormFile: ", err)
	}

	// Read from file and write to form
	hFileName := strconv.Itoa(int(hash(sFileName)))
	fileName := hFileName + "." + timeStamp.Format(TIMEFMT)
	srcFile, err := os.Open("./duplication/" + fileName)
	if err != nil {
		log.Println("os.Open: ", err)
	}
	defer srcFile.Close()
	_, err = io.Copy(formFile, srcFile)
	if err != nil {
		log.Println("io.Copy: ", err)
	}

	// Send the form
	contentType := writer.FormDataContentType()
	writer.Close()
	resp, err := http.Post("http://"+IP+":"+HTTPPORT+"/duplication", contentType, buf)
	if err != nil {
		log.Println("http.Post: ", err)
	}
	log.Println("SendReplica: Send file to " + IP + ": " + resp.Status)
}

// calculate hash of sdfsfilename
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func FNV32a(text string) int {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum32()) % 10
}

// Listen to port 80 for HTTP file transfer
func handleHTTP() {
	http.HandleFunc("/duplication", recFile)
	http.HandleFunc("/duplication/", httpResponse)
	log.Printf("Start Listening to port %s for HTTP", HTTPPORT)
	err := http.ListenAndServe(SELFIP+":"+HTTPPORT, nil)
	checkErr(err)
}

// Delete all sdfs file when server is (re)join.
func deleteAllSfile() {
	dir, _ := ioutil.ReadDir("./duplication")
	pattern := `d.*`
	for _, f := range dir {
		match, _ := regexp.MatchString(pattern, f.Name())
		if !match {
			os.RemoveAll(path.Join([]string{"./duplication", f.Name()}...))
		}
	}
}
