package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"../shared"
)

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

// reply the membership list of self host
func (r *Memlst) RplMemlst(args *shared.EmptyReq, reply *shared.MembershipRpl) error {
	var rpl []shared.Member
	for _, member := range memberList {
		if member.Status == 1 {
			rpl = append(rpl, member)
		}
	}

	jsons, err := json.Marshal(rpl)
	checkErr(err)
	reply.MemberList = jsons
	return nil
}

//reply the peer list
func (r *Memlst) RplPrList(args *shared.EmptyReq, reply *shared.MembershipRpl) error {
	var rpl []shared.Member
	for _, member := range peerList {
		if member.Status == 1 {
			rpl = append(rpl, member)
		}
	}

	jsons, err := json.Marshal(rpl)
	checkErr(err)
	reply.MemberList = jsons
	return nil
}

// Initialize peer list with next three host in membership list
func initializePeerList() {
	for i := 0; i < NUMOFPEER; i++ {
		peerList[i] = memberList[(ID+i+1)%NUMOFVM]
	}
}

//Print all the members alive
func printMemberList() {
	i := 0
	for i < NUMOFVM {
		jsString, err := json.Marshal(memberList[i])
		checkErr(err)
		if memberList[i].Status == 1 {
			fmt.Println(string(jsString))
		}
		i += 1
	}
}
